package grpcnet

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
)

// A peer that was down long enough for the dial backoff to grow must be
// reachable again within seconds of returning. The metadata server learns
// of the return from the peer's own heartbeats and places new chunks on it
// at once; a client still waiting out a long backoff fails those writes
// fast, and the chunks commit one copy short (real-mode transient blip).
func TestCallerReconnectsSoonAfterLongOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a 20 s outage")
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	caller := NewCaller(map[iface.NodeID]string{"n": addr}, time.Second)
	defer caller.Close()
	call := []iface.Call{{To: "n", Kind: "ping"}}
	// Keep calling through the outage, as clients reading from a dead
	// replica do, so the backoff keeps growing.
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if r := caller.Do(context.Background(), call); r[0].Err == nil {
			t.Fatal("call succeeded with the peer down")
		}
	}

	lis, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port %s taken during the outage: %v", addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop := runtime.NewLoop()
	go loop.Run(ctx)
	tr := New(addr, nil, loop, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer tr.Close()
	tr.Serve("n", "ping", func(_ iface.Message, respond iface.Responder) { respond([]byte("pong"), nil) }, iface.ServeOpts{Concurrent: true})
	s := grpc.NewServer()
	tr.Register(s)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	back := time.Now()
	for caller.Do(context.Background(), call)[0].Err != nil {
		if time.Since(back) > 4*time.Second {
			t.Fatalf("peer back for %v and still unreachable: the dial backoff is not capped", time.Since(back).Round(time.Millisecond))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("reachable %v after the peer returned", time.Since(back).Round(time.Millisecond))
}
