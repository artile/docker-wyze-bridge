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
// residue of the IoTVideo framing), re-escapes zero runs inside NAL
// units (see reescapeNAL), and writes one access unit at a time
// at the measured frame interval. A small queue absorbs the bursts; a
// growing queue makes the pacer catch up, so latency stays bounded.
//
// When the stream has no AUD NAL units the pacer passes bytes through.
type auPacer struct {
	out io.Writer

	mu      sync.Mutex
	pending []byte
	sawAUD  bool
	scanned int      // bytes inspected while looking for the first AUD
	carry   [][]byte // SPS/PPS/SEI that belong to the next access unit

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
			// Parameter sets sent before the first AUD belong to the
			// first access unit: keep them.
			for _, n := range splitNALs(p.pending[:idx]) {
				if isParamOrSEI(n) {
					p.carry = append(p.carry, n)
				}
			}
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
			return len(b), nil
		}
	}

	// Emit every complete access unit: from one AUD to the next.
	for {
		next := findAUD(p.pending, 4)
		if next < 0 {
			break
		}
		au := p.assemble(splitNALs(p.pending[:next]))
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
		// Schedule from the real write time: a late timer must not make
		// the next frame go out back to back.
		next = time.Now().Add(iv)
	}
}

// Close stops the pacer goroutine. Queued access units are dropped.
func (p *auPacer) Close() {
	p.once.Do(func() { close(p.done) })
}

// assemble builds one access unit in decoding order. Wyze cameras send
// SPS/PPS (and SEI) before the AUD of the frame they belong to, so a cut
// at the AUD leaves them at the tail of the previous access unit. They
// are moved to the start of the next one, right after its AUD.
func (p *auPacer) assemble(nals [][]byte) []byte {
	lastVCL := -1
	for i, n := range nals {
		if t := n[0] & 0x1f; t >= 1 && t <= 5 {
			lastVCL = i
		}
	}
	if lastVCL < 0 {
		// No picture: everything is carried to the next access unit.
		for _, n := range nals {
			if n[0]&0x1f != 9 {
				p.carry = append(p.carry, n)
			}
		}
		return nil
	}
	var body, tail [][]byte
	for i, n := range nals {
		if i > lastVCL && isParamOrSEI(n) {
			tail = append(tail, n)
			continue
		}
		body = append(body, n)
	}
	out := make([][]byte, 0, len(body)+len(p.carry))
	if len(body) > 0 && body[0][0]&0x1f == 9 {
		out = append(out, body[0])
		body = body[1:]
	}
	out = append(out, p.carry...)
	out = append(out, body...)
	p.carry = tail
	return joinNALs(out)
}

func isParamOrSEI(n []byte) bool {
	t := n[0] & 0x1f
	return t == 6 || t == 7 || t == 8
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

// reescapeNAL removes and re-inserts emulation prevention bytes.
//
// Wyze Gwell cameras send slice NAL units with runs of zero bytes that
// are not escaped ("00 00 00" inside a NAL is illegal in Annex B).
// Software decoders ignore it; Apple VideoToolbox (Chrome and Safari on
// macOS/iOS) rejects every such frame (kVTVideoDecoderBadDataErr). This
// is the same rewrite ffmpeg's CBS-based bitstream filters do.
func reescapeNAL(nal []byte) []byte {
	// Fast path: nothing to escape.
	need := false
	for i := 2; i < len(nal); i++ {
		if nal[i-2] == 0 && nal[i-1] == 0 && nal[i] <= 3 {
			need = true
			break
		}
	}
	if !need && nal[len(nal)-1] != 0 {
		return nal
	}
	// Unescape: drop 0x03 that follows two zero bytes.
	rbsp := make([]byte, 0, len(nal))
	zeros := 0
	for _, c := range nal {
		if zeros >= 2 && c == 3 {
			zeros = 0
			continue
		}
		rbsp = append(rbsp, c)
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	// Escape: insert 0x03 before any byte <= 3 that follows two zeros,
	// and after a final zero byte.
	out := make([]byte, 0, len(rbsp)+len(rbsp)/64+2)
	zeros = 0
	for _, c := range rbsp {
		if zeros >= 2 && c <= 3 {
			out = append(out, 3)
			zeros = 0
		}
		out = append(out, c)
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	if out[len(out)-1] == 0 {
		out = append(out, 3)
	}
	return out
}

// cleanAccessUnit rewrites one access unit with 4-byte start codes and
// without NAL units that cannot be valid H.264.
func cleanAccessUnit(au []byte) []byte {
	return joinNALs(splitNALs(au))
}

func joinNALs(nals [][]byte) []byte {
	var out bytes.Buffer
	for _, n := range nals {
		out.Write([]byte{0, 0, 0, 1})
		out.Write(n)
	}
	return out.Bytes()
}

// splitNALs splits Annex B bytes into NAL units, drops units that cannot
// be valid H.264 and re-escapes the rest.
func splitNALs(au []byte) [][]byte {
	var nals [][]byte
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
		nals = append(nals, reescapeNAL(nal))
	}
	for i := 0; i+2 < len(au); i++ {
		if au[i] == 0 && au[i+1] == 0 && au[i+2] == 1 {
			emit(i)
			start = i + 3
			i += 2
		}
	}
	emit(len(au))
	return nals
}
