package main

import (
	"io"
	"log"
	"sync"
	"time"
)

// auPacer writes H.264 to ffmpeg one access unit at a time, at the
// camera's frame interval (-pace).
//
// A Gwell relay delivers frames in bursts (KCP recovers a lost segment,
// then hands over everything behind it at once). ffmpeg stamps raw H.264
// with the wallclock time it reads a frame, so a burst gets equal or
// near-equal timestamps: browsers drop those frames, and mediamtx's
// LL-HLS parts change length all the time.
//
// The pacer takes clean Annex B (behind the nalEscaper) and cuts it into
// access units where H.264 7.4.1.2.3 says a new one starts: at the first
// AUD, SPS, PPS or SEI after a slice. It never reorders or changes a NAL
// unit. A queue absorbs the bursts and one access unit goes out per
// measured frame interval; when the queue grows the pacer catches up, so
// latency stays bounded.
type auPacer struct {
	out io.Writer

	mu      sync.Mutex
	pending []byte // the open access unit
	sawVCL  bool   // pending holds a slice

	q        chan []byte
	interval time.Duration // EWMA of the access-unit arrival interval
	lastIn   time.Time
	done     chan struct{}
	once     sync.Once
	dropped  int
}

const (
	pacerQueueCap      = 256
	pacerTargetDepth   = 3  // access units kept to absorb a burst
	pacerMaxDepth      = 40 // above this the queue is flushed at once
	pacerMinInterval   = 20 * time.Millisecond
	pacerMaxInterval   = 200 * time.Millisecond
	pacerStartInterval = 50 * time.Millisecond
)

func newAUPacer(out io.Writer) *auPacer {
	p := &auPacer{
		out:      out,
		q:        make(chan []byte, pacerQueueCap),
		interval: pacerStartInterval,
		done:     make(chan struct{}),
	}
	go p.run()
	return p
}

// Write takes Annex B bytes made of whole NAL units with 4-byte start
// codes (what nalEscaper writes). It never blocks on ffmpeg.
func (p *auPacer) Write(b []byte) (int, error) {
	select {
	case <-p.done:
		return 0, io.ErrClosedPipe
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, nal := range splitStartCodes(b) {
		t := nal[4] & 0x1f
		vcl := t >= 1 && t <= 5
		if !vcl && p.sawVCL && (t == 6 || t == 7 || t == 8 || t == 9) {
			p.enqueue(p.pending)
			p.pending = nil
			p.sawVCL = false
		}
		p.pending = append(p.pending, nal...)
		if vcl {
			p.sawVCL = true
		}
	}
	return len(b), nil
}

// splitStartCodes splits b at 00 00 00 01 into units that keep their
// start code. b must begin with a start code.
func splitStartCodes(b []byte) [][]byte {
	var out [][]byte
	start := -1
	for i := 0; i+4 < len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 0 && b[i+3] == 1 {
			if start >= 0 {
				out = append(out, b[start:i])
			}
			start = i
			i += 3
		}
	}
	if start >= 0 {
		out = append(out, b[start:])
	}
	return out
}

func (p *auPacer) enqueue(au []byte) {
	if len(au) == 0 {
		return
	}
	now := time.Now()
	if !p.lastIn.IsZero() {
		d := now.Sub(p.lastIn)
		if d > 2*time.Second {
			d = p.interval // a gap, not a frame interval
		}
		// Slow EWMA: bursts average out to the camera frame rate.
		p.interval = time.Duration(0.97*float64(p.interval) + 0.03*float64(d))
	}
	p.lastIn = now
	select {
	case p.q <- au:
	default:
		p.dropped++
		if p.dropped%100 == 1 {
			log.Printf("[pacer] queue full, dropped %d access units", p.dropped)
		}
	}
}

func (p *auPacer) currentInterval() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return min(max(p.interval, pacerMinInterval), pacerMaxInterval)
}

func (p *auPacer) run() {
	next := time.Now()
	for {
		var au []byte
		select {
		case <-p.done:
			return
		case au = <-p.q:
		}
		depth := len(p.q)
		iv := p.currentInterval()
		switch {
		case depth > pacerMaxDepth:
			iv = 0
		case depth > pacerTargetDepth:
			iv = iv * 3 / 4 // catch up, up to a third faster
		}
		if wait := time.Until(next); wait > 0 {
			select {
			case <-p.done:
				return
			case <-time.After(wait):
			}
		}
		if _, err := p.out.Write(au); err != nil {
			return
		}
		// Schedule from the real write time: a late timer must not send
		// the next frame back to back.
		next = time.Now().Add(iv)
	}
}

// Close stops the pacer. Queued access units are dropped.
func (p *auPacer) Close() {
	p.once.Do(func() { close(p.done) })
}
