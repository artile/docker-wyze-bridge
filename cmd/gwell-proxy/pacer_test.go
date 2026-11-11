package main

import (
	"bytes"
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
