package main

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

type timedWriter struct {
	mu     sync.Mutex
	writes [][]byte
	at     []time.Time
}

func (w *timedWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, append([]byte(nil), b...))
	w.at = append(w.at, time.Now())
	return len(b), nil
}

func (w *timedWriter) snapshot() ([][]byte, []time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.writes...), append([]time.Time(nil), w.at...)
}

func TestPacerSpacesABurstAndKeepsNALOrder(t *testing.T) {
	var w timedWriter
	p := newAUPacer(&w)
	defer p.Close()
	au := func(slice byte) []byte { return []byte{0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x41, slice} }
	idr := []byte{0, 0, 0, 1, 0x67, 0x64, 0, 0, 0, 1, 0x68, 0xee, 0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x65, 0x88}
	var in []byte
	in = append(in, au(1)...)
	in = append(in, au(2)...)
	in = append(in, idr...) // SPS/PPS before the AUD, as Wyze cameras send them
	in = append(in, au(3)...)
	in = append(in, au(4)...) // stays open: no next unit yet
	if _, err := p.Write(in); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if writes, _ := w.snapshot(); len(writes) == 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	writes, at := w.snapshot()
	want := [][]byte{au(1), au(2), idr, au(3)}
	if len(writes) != len(want) {
		t.Fatalf("%d writes, want %d", len(writes), len(want))
	}
	for i := range want {
		if !bytes.Equal(writes[i], want[i]) {
			t.Fatalf("write %d = % x, want % x", i, writes[i], want[i])
		}
	}
	for i := 1; i < len(at); i++ {
		if gap := at[i].Sub(at[i-1]); gap < pacerMinInterval {
			t.Fatalf("writes %d and %d only %s apart", i-1, i, gap)
		}
	}
}
