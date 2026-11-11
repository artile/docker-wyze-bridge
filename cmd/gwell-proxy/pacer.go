package main

import (
	"bytes"
	"io"
	"log"
	"sync"
	"time"
)

// auPacer sits between the P2P session and the ffmpeg publisher.
//
// A Gwell relay delivers video in bursts: several frames arrive within a
// millisecond, then nothing for a while. ffmpeg stamps raw H.264 input
// with the wallclock time it reads each frame, so a burst gets the same
// (or non-increasing) timestamps. RTSP and go2rtc pass them on, and a
// browser's Media Source Extensions reject HLS segments with
// non-increasing DTS (hls.js: bufferAppendError).
//
// The pacer splits the byte stream into access units at the AUD NAL
// (type 9, which Wyze Gwell cameras send before every frame), drops NAL
// units that cannot be valid H.264 (forbidden_zero_bit set, or type 0:
// residue of the IoTVideo framing), and writes one access unit at a time
// at the measured frame interval. A small queue absorbs the bursts; a
// growing queue makes the pacer catch up, so latency stays bounded.
//
// When the stream has no AUD NAL units the pacer passes bytes through.
type auPacer struct {
	out io.Writer

	mu      sync.Mutex
	pending []byte
	sawAUD  bool
	scanned int // bytes inspected while looking for the first AUD

	q        chan []byte
	interval time.Duration // EWMA of the access-unit arrival interval
	lastIn   time.Time
	done     chan struct{}
	once     sync.Once
	dropped  int
}

const (
	pacerQueueCap      = 512
	pacerTargetDepth   = 4 // access units kept to absorb a burst
	pacerMaxDepth      = 45
	pacerMinInterval   = 15 * time.Millisecond
	pacerMaxInterval   = 200 * time.Millisecond
	pacerStartInterval = 66 * time.Millisecond // 15 fps
	pacerNoAUDLimit    = 512 * 1024
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

// Write accepts raw Annex B bytes in any chunking. It never blocks on
// ffmpeg; it only fails after Close.
func (p *auPacer) Write(b []byte) (int, error) {
	select {
	case <-p.done:
		return 0, io.ErrClosedPipe
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = append(p.pending, b...)

	if !p.sawAUD {
		if idx := findAUD(p.pending, 0); idx >= 0 {
			p.sawAUD = true
			p.pending = p.pending[idx:]
		} else {
			p.scanned += len(b)
			if p.scanned > pacerNoAUDLimit {
				// No AUD in this stream: pass through unchanged.
				out := p.pending
				p.pending = nil
				p.mu.Unlock()
				_, err := p.out.Write(out)
				p.mu.Lock()
				return len(b), err
			}
			// Keep only a tail that can still hold a start code.
			if len(p.pending) > 8 {
				p.pending = append([]byte(nil), p.pending[len(p.pending)-8:]...)
			}
			return len(b), nil
		}
	}

	// Emit every complete access unit: from one AUD to the next.
	for {
		next := findAUD(p.pending, 4)
		if next < 0 {
			break
		}
		au := cleanAccessUnit(p.pending[:next])
		p.pending = p.pending[next:]
		p.enqueue(au)
	}
	return len(b), nil
}

func (p *auPacer) enqueue(au []byte) {
	if len(au) == 0 {
		return
	}
	now := time.Now()
	if !p.lastIn.IsZero() {
		d := now.Sub(p.lastIn)
		if d > 2*time.Second {
			d = p.interval // a gap (reconnect), not a frame interval
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
	iv := p.interval
	if iv < pacerMinInterval {
		iv = pacerMinInterval
	}
	if iv > pacerMaxInterval {
		iv = pacerMaxInterval
	}
	return iv
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
			iv = 0 // far behind: flush
		case depth > pacerTargetDepth:
			// Catch up gently: up to 25 % faster than the camera.
			iv = iv * 3 / 4
		}
		now := time.Now()
		if next.Before(now) || depth == 0 && next.Sub(now) > pacerMaxInterval {
			next = now
		}
		if wait := next.Sub(now); wait > 0 {
			select {
			case <-p.done:
				return
			case <-time.After(wait):
			}
		}
		if _, err := p.out.Write(au); err != nil {
			return
		}
		next = next.Add(iv)
	}
}

// Close stops the pacer goroutine. Queued access units are dropped.
func (p *auPacer) Close() {
	p.once.Do(func() { close(p.done) })
}

// findAUD returns the index of the first Annex B start code (3 or 4
// bytes) followed by an AUD NAL (type 9) at or after from, or -1.
func findAUD(b []byte, from int) int {
	for i := from; i+3 < len(b); i++ {
		if b[i] != 0 || b[i+1] != 0 {
			continue
		}
		if b[i+2] == 1 && b[i+3]&0x1f == 9 && b[i+3]&0x80 == 0 {
			if i > 0 && b[i-1] == 0 {
				return i - 1
			}
			return i
		}
	}
	return -1
}

// cleanAccessUnit rewrites one access unit with 4-byte start codes and
// without NAL units that cannot be valid H.264.
func cleanAccessUnit(au []byte) []byte {
	var out bytes.Buffer
	out.Grow(len(au))
	start := -1 // index of the first NAL byte
	emit := func(end int) {
		if start < 0 || start >= end {
			return
		}
		nal := au[start:end]
		// Trim trailing zero bytes (they belong to the next start code).
		for len(nal) > 0 && nal[len(nal)-1] == 0 {
			nal = nal[:len(nal)-1]
		}
		if len(nal) == 0 || nal[0]&0x80 != 0 || nal[0]&0x1f == 0 {
			return
		}
		out.Write([]byte{0, 0, 0, 1})
		out.Write(nal)
	}
	for i := 0; i+2 < len(au); i++ {
		if au[i] == 0 && au[i+1] == 0 && au[i+2] == 1 {
			emit(i)
			start = i + 3
			i += 2
		}
	}
	emit(len(au))
	return out.Bytes()
}
