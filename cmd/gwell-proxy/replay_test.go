package main

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/IDisposable/docker-wyze-bridge/internal/gwell/upstream/stream"
)

// TestReplayIntoFFmpeg feeds a raw camera dump through the escaper (and
// the pacer with GWELL_REPLAY_PACE=1) into the real ffmpeg publisher, in
// bursts like the relay's. Manual: it needs ffmpeg and an RTSP server.
//
//	GWELL_REPLAY=dump.h264 GWELL_REPLAY_RTSP=host:port go test -run Replay ./cmd/gwell-proxy/
func TestReplayIntoFFmpeg(t *testing.T) {
	file, target := os.Getenv("GWELL_REPLAY"), os.Getenv("GWELL_REPLAY_RTSP")
	if file == "" || target == "" {
		t.Skip("set GWELL_REPLAY and GWELL_REPLAY_RTSP")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	host, portS, _ := strings2(target)
	port, _ := strconv.Atoi(portS)
	ff, err := stream.StartFFmpegPublisher("replay", host, port, "warning", os.Getenv("GWELL_REPLAY_RATE"))
	if err != nil {
		t.Fatal(err)
	}
	defer ff.Close()
	var sink = ff
	var w interface{ Write([]byte) (int, error) } = sink
	if os.Getenv("GWELL_REPLAY_PACE") == "1" {
		p := newAUPacer(sink)
		defer p.Close()
		w = p
	}
	e := newNALEscaper(w)
	r := rand.New(rand.NewSource(1))
	// The dump's own byte rate (GWELL_REPLAY_BPS, default 200 KB/s),
	// delivered in bursts.
	bps := 200000
	if v, err := strconv.Atoi(os.Getenv("GWELL_REPLAY_BPS")); err == nil && v > 0 {
		bps = v
	}
	for off := 0; off < len(data); {
		burst := 2000 + r.Intn(bps/5)
		end := min(off+burst, len(data))
		if _, err := e.Write(data[off:end]); err != nil {
			t.Fatal(err)
		}
		off = end
		time.Sleep(time.Duration(burst) * time.Second / time.Duration(bps))
	}
	time.Sleep(2 * time.Second)
}

func strings2(hostport string) (string, string, bool) {
	for i := len(hostport) - 1; i >= 0; i-- {
		if hostport[i] == ':' {
			return hostport[:i], hostport[i+1:], true
		}
	}
	return hostport, "", false
}
