package main

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"
)

type syncBuf struct {
	mu     sync.Mutex
	writes [][]byte
	at     []time.Time
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.writes = append(b.writes, append([]byte(nil), p...))
	b.at = append(b.at, time.Now())
	return len(p), nil
}

func (b *syncBuf) snapshot() ([][]byte, []time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]byte(nil), b.writes...), append([]time.Time(nil), b.at...)
}

// frame builds one access unit: AUD + a slice NAL with the given byte.
func frame(tag byte) []byte {
	return []byte{0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x41, tag, tag, tag}
}

func TestPacerSplitsBurstIntoSpacedAccessUnits(t *testing.T) {
	out := &syncBuf{}
	p := newAUPacer(out)
	defer p.Close()

	var burst []byte
	for i := 0; i < 6; i++ {
		burst = append(burst, frame(byte(0x10+i))...)
	}
	// One chunk carries six frames (a relay burst), split at odd places.
	p.Write(burst[:7])
	p.Write(burst[7:])
	p.Write(frame(0x99)) // closes the sixth access unit

	time.Sleep(600 * time.Millisecond)
	writes, at := out.snapshot()
	if len(writes) != 6 {
		t.Fatalf("got %d access units, want 6", len(writes))
	}
	for i, w := range writes {
		if !bytes.Equal(w, frame(byte(0x10+i))) {
			t.Fatalf("access unit %d = %x", i, w)
		}
	}
	for i := 1; i < len(at); i++ {
		if gap := at[i].Sub(at[i-1]); gap < 20*time.Millisecond {
			t.Fatalf("access units %d and %d written %s apart; want paced", i-1, i, gap)
		}
	}
}

func TestPacerDropsInvalidNALUnits(t *testing.T) {
	au := []byte{0, 0, 0, 1, 0x09, 0xf0,
		0, 0, 1, 0x80, 0x7e, 0x7e, // forbidden bit set: framing residue
		0, 0, 0, 1, 0x00, 0xff, // type 0
		0, 0, 0, 1, 0x65, 0xaa, 0xbb}
	got := cleanAccessUnit(au)
	want := []byte{0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x65, 0xaa, 0xbb}
	if !bytes.Equal(got, want) {
		t.Fatalf("clean = %x, want %x", got, want)
	}
}

func TestPacerPassesThroughStreamWithoutAUD(t *testing.T) {
	out := &syncBuf{}
	p := newAUPacer(out)
	defer p.Close()
	chunk := bytes.Repeat([]byte{0, 0, 0, 1, 0x41, 1, 2, 3}, 1024)
	for i := 0; i < 70; i++ { // > pacerNoAUDLimit
		p.Write(chunk)
	}
	writes, _ := out.snapshot()
	if len(writes) == 0 {
		t.Fatal("no pass-through output for a stream without AUD")
	}
}

func TestPacerDropsTrailingC0Residue(t *testing.T) {
	// Seen on a Wyze Cam OG: a P slice, zero padding, then "00 00 01 c0 00".
	au := []byte{0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x41, 0x9a, 0x11, 0x22,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xc0, 0x00}
	got := cleanAccessUnit(au)
	want := []byte{0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x41, 0x9a, 0x11, 0x22}
	if !bytes.Equal(got, want) {
		t.Fatalf("clean = %x, want %x", got, want)
	}
}

func TestReescapeNALEscapesZeroRuns(t *testing.T) {
	cases := []struct{ in, want []byte }{
		{[]byte{0x41, 1, 0, 0, 0, 0, 0x35}, []byte{0x41, 1, 0, 0, 3, 0, 0, 0x35}},
		{[]byte{0x41, 0, 0, 0, 0, 0, 0}, []byte{0x41, 0, 0, 3, 0, 0, 3, 0, 0, 3}},
		{[]byte{0x41, 0, 0, 3, 1}, []byte{0x41, 0, 0, 3, 1}}, // already escaped
		{[]byte{0x41, 5, 6}, []byte{0x41, 5, 6}},
		{[]byte{0x41, 0, 0, 1}, []byte{0x41, 0, 0, 3, 1}},
	}
	for _, c := range cases {
		if got := reescapeNAL(c.in); !bytes.Equal(got, c.want) {
			t.Errorf("reescapeNAL(%x) = %x, want %x", c.in, got, c.want)
		}
	}
}

// TestPacerCleanFile rewrites an Annex B file with cleanAccessUnit when
// PACER_IN/PACER_OUT are set (manual check against a hardware decoder).
func TestPacerCleanFile(t *testing.T) {
	in, out := os.Getenv("PACER_IN"), os.Getenv("PACER_OUT")
	if in == "" || out == "" {
		t.Skip("PACER_IN/PACER_OUT not set")
	}
	d, err := os.ReadFile(in)
	if err != nil {
		t.Fatal(err)
	}
	var res []byte
	start := findAUD(d, 0)
	for start >= 0 {
		next := findAUD(d, start+4)
		end := next
		if end < 0 {
			end = len(d)
		}
		res = append(res, cleanAccessUnit(d[start:end])...)
		start = next
	}
	if err := os.WriteFile(out, res, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPacerMovesParameterSetsToTheirFrame(t *testing.T) {
	out := &syncBuf{}
	p := newAUPacer(out)
	defer p.Close()
	sps := []byte{0, 0, 0, 1, 0x67, 0x64, 0x00, 0x29}
	pps := []byte{0, 0, 0, 1, 0x68, 0xee, 0x3c}
	aud := []byte{0, 0, 0, 1, 0x09, 0xf0}
	idr := []byte{0, 0, 0, 1, 0x65, 0x88, 0x80}
	pslice := []byte{0, 0, 0, 1, 0x41, 0x9a, 0x01}
	cat := func(parts ...[]byte) []byte {
		var b []byte
		for _, x := range parts {
			b = append(b, x...)
		}
		return b
	}
	// Camera order: SPS PPS AUD IDR | AUD P | SPS PPS AUD IDR | AUD ...
	p.Write(cat(sps, pps, aud, idr, aud, pslice, sps, pps, aud, idr, aud, pslice))
	time.Sleep(500 * time.Millisecond)
	writes, _ := out.snapshot()
	want := [][]byte{
		cat(aud, sps, pps, idr),
		cat(aud, pslice),
		cat(aud, sps, pps, idr),
	}
	if len(writes) != len(want) {
		t.Fatalf("got %d access units, want %d", len(writes), len(want))
	}
	for i := range want {
		if !bytes.Equal(writes[i], want[i]) {
			t.Fatalf("access unit %d = %x, want %x", i, writes[i], want[i])
		}
	}
}
