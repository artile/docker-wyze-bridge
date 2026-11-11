package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// demandHub turns camera sessions on and off by viewer demand (-on-demand).
//
// A Gwell camera session costs relay bandwidth (about 1 Mbit/s) and the
// camera serves one client at a time: while the proxy holds a session,
// the Wyze app gets no picture. With -on-demand a camera stays idle until
// somebody asks for its stream.
//
// Demand is an open HTTP request: GET /demand/<stream> holds the
// response open, and the camera streams while at least one such request
// is open. A media server's on-demand hook fits this directly, e.g.
// mediamtx:
//
//	paths:
//	  "~^[a-z0-9_]+$":
//	    runOnDemand: wget -q -O /dev/null http://wyze-bridge:18564/demand/$MTX_PATH
//	    runOnDemandCloseAfter: 60s
//
// mediamtx runs the command when the first reader arrives and kills it
// when the last reader has been gone for runOnDemandCloseAfter. The
// session then ends after -demand-grace.
//
// GET /health answers with the state of every camera as JSON.
type demandHub struct {
	mu   sync.Mutex
	cams map[string]*camDemand // by stream name
	cond *sync.Cond
}

type camDemand struct {
	camera    string
	stream    string
	viewers   int
	idleSince time.Time // when viewers dropped to 0; zero while watched
	streaming bool
	lastData  time.Time
	sessions  int
}

func newDemandHub() *demandHub {
	h := &demandHub{cams: map[string]*camDemand{}}
	h.cond = sync.NewCond(&h.mu)
	return h
}

// register adds a camera under its stream name.
func (h *demandHub) register(cameraID, stream string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.cams[stream]; !ok {
		h.cams[stream] = &camDemand{camera: cameraID, stream: stream, idleSince: time.Now()}
	}
}

// acquire counts one viewer; it returns false for an unknown stream.
func (h *demandHub) acquire(stream string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.cams[stream]
	if !ok {
		return false
	}
	c.viewers++
	c.idleSince = time.Time{}
	h.cond.Broadcast()
	return true
}

func (h *demandHub) release(stream string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.cams[stream]
	if !ok || c.viewers == 0 {
		return
	}
	c.viewers--
	if c.viewers == 0 {
		c.idleSince = time.Now()
	}
	h.cond.Broadcast()
}

// waitDemand blocks until the stream has a viewer or stop closes. It
// returns false on stop.
func (h *demandHub) waitDemand(stream string, stop <-chan struct{}) bool {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-stop:
			h.mu.Lock()
			h.cond.Broadcast()
			h.mu.Unlock()
		case <-done:
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		select {
		case <-stop:
			return false
		default:
		}
		if c := h.cams[stream]; c != nil && c.viewers > 0 {
			return true
		}
		h.cond.Wait()
	}
}

// idleFor returns how long the stream has had no viewer (0 while watched).
func (h *demandHub) idleFor(stream string) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.cams[stream]
	if c == nil || c.idleSince.IsZero() {
		return 0
	}
	return time.Since(c.idleSince)
}

// setStreaming records the session state for /health.
func (h *demandHub) setStreaming(stream string, on bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c := h.cams[stream]; c != nil {
		if on && !c.streaming {
			c.sessions++
		}
		c.streaming = on
	}
}

func (h *demandHub) setLastData(stream string, t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c := h.cams[stream]; c != nil {
		c.lastData = t
	}
}

type camHealth struct {
	Camera       string   `json:"camera"`
	Stream       string   `json:"stream"`
	Viewers      int      `json:"viewers"`
	Streaming    bool     `json:"streaming"`
	LastDataAgeS *float64 `json:"lastDataAgeSeconds"`
	Sessions     int      `json:"sessions"`
}

func (h *demandHub) health() []camHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]camHealth, 0, len(h.cams))
	for _, c := range h.cams {
		ch := camHealth{Camera: c.camera, Stream: c.stream, Viewers: c.viewers, Streaming: c.streaming, Sessions: c.sessions}
		if !c.lastData.IsZero() {
			age := time.Since(c.lastData).Seconds()
			ch.LastDataAgeS = &age
		}
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Stream < out[j].Stream })
	return out
}

// handler serves GET /demand/<stream> and GET /health.
func (h *demandHub) handler(onDemand bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"onDemand": onDemand,
			"cameras":  h.health(),
		})
	})
	mux.HandleFunc("/demand/", func(w http.ResponseWriter, r *http.Request) {
		stream := strings.TrimPrefix(r.URL.Path, "/demand/")
		if !h.acquire(stream) {
			http.Error(w, "unknown stream", http.StatusNotFound)
			return
		}
		defer h.release(stream)
		log.Printf("[demand] %s: viewer in (%s)", stream, r.RemoteAddr)
		defer log.Printf("[demand] %s: viewer out (%s)", stream, r.RemoteAddr)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		// A byte now and then keeps idle-timeout clients (busybox wget)
		// and proxies from closing the request.
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			if _, err := w.Write([]byte("\n")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
			}
		}
	})
	return mux
}
