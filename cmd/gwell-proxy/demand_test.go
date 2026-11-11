package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDemandRequestStartsAndStopsTheCamera(t *testing.T) {
	h := newDemandHub()
	h.register("GW_GC1_TEST", "office")
	srv := httptest.NewServer(h.handler(true))
	defer srv.Close()

	if h.idleFor("office") == 0 {
		t.Fatal("a new camera must start idle")
	}

	// Unknown stream: 404, no demand.
	resp, err := http.Get(srv.URL + "/demand/nope")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown stream: %d", resp.StatusCode)
	}

	// A viewer arrives: waitDemand returns.
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/demand/office", nil)
	got := make(chan bool, 1)
	go func() { got <- h.waitDemand("office", make(chan struct{})) }()
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("waitDemand returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitDemand did not see the viewer")
	}
	if h.idleFor("office") != 0 {
		t.Fatal("watched camera reported idle")
	}
	var health struct {
		OnDemand bool        `json:"onDemand"`
		Cameras  []camHealth `json:"cameras"`
	}
	hr, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = json.NewDecoder(hr.Body).Decode(&health)
	hr.Body.Close()
	if !health.OnDemand || len(health.Cameras) != 1 || health.Cameras[0].Viewers != 1 {
		t.Fatalf("health = %+v", health)
	}

	// The viewer leaves: the camera goes idle.
	cancel()
	resp.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for h.idleFor("office") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("camera still watched after the request closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWaitDemandReturnsOnStop(t *testing.T) {
	h := newDemandHub()
	h.register("GW_GC1_TEST", "office")
	stop := make(chan struct{})
	got := make(chan bool, 1)
	go func() { got <- h.waitDemand("office", stop) }()
	close(stop)
	select {
	case ok := <-got:
		if ok {
			t.Fatal("waitDemand returned true on stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitDemand ignored stop")
	}
}
