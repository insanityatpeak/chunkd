package grpcnet

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/insanityatpeak/chunkd/internal/core/heartbeat"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
)

type proc struct {
	addr  string
	loop  *runtime.Loop
	clock *runtime.Clock
	net   *Transport
	deps  heartbeat.Deps
}

// start runs one process with its own loop, transport and gRPC server on a
// random localhost port.
func start(t *testing.T, ctx context.Context, peers map[iface.NodeID]string) *proc {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	loop := runtime.NewLoop()
	p := &proc{addr: lis.Addr().String(), loop: loop, clock: runtime.NewClock(loop)}
	p.net = New(p.addr, peers, loop, log)
	p.deps = heartbeat.Deps{Clock: p.clock, Net: p.net, Rand: runtime.NewRand(), Log: log}
	s := grpc.NewServer()
	p.net.Register(s)
	go func() { _ = s.Serve(lis) }()
	go loop.Run(ctx)
	t.Cleanup(func() { s.Stop(); p.net.Close() })
	return p
}

// The same core heartbeat code the sim runs, over real gRPC and wall time.
// The meta process has no static peer table: it learns the node's address
// from the envelope, which is what lets it send pongs back.
func TestHeartbeatOverGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	meta := start(t, ctx, nil)
	node := start(t, ctx, map[iface.NodeID]string{"meta-1": meta.addr})

	tr := heartbeat.NewTracker(meta.deps, "meta-1")
	s := heartbeat.NewSender(node.deps, "node-1", "meta-1", 50*time.Millisecond)
	meta.loop.Do(tr.Start)
	node.loop.Do(s.Start)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var pings, acked uint64
		meta.loop.Do(func() {
			if ps := tr.Peers(); len(ps) == 1 {
				pings = ps[0].Pings
			}
		})
		node.loop.Do(func() { acked = s.Stats().Acked })
		if pings >= 5 && acked >= 5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("fewer than 5 heartbeats round-tripped within 5s")
}

func TestSendToUnknownNodeIsDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := start(t, ctx, nil)
	p.net.Send("nobody", iface.Message{From: "x", Kind: "k"}) // must not panic or block
}
