package grpcnet

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/iface/ifacetest"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

type proc struct {
	addr  string
	loop  *runtime.Loop
	clock *runtime.Clock
	net   *Transport
	rng   iface.Rand
	log   *slog.Logger
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
	p := &proc{addr: lis.Addr().String(), loop: loop, clock: runtime.NewClock(loop), rng: runtime.NewRand(), log: log}
	p.net = New(p.addr, peers, loop, log)
	s := grpc.NewServer(grpc.MaxRecvMsgSize(MaxUnary), grpc.MaxSendMsgSize(MaxUnary))
	p.net.Register(s)
	go func() { _ = s.Serve(lis) }()
	go loop.Run(ctx)
	t.Cleanup(func() { s.Stop(); p.net.Close() })
	return p
}

// The same core node and metadata code the sim runs, over real gRPC and
// wall time. The metadata process has no static peer table: it learns the
// node's address from the envelope, which is how heartbeat acks get back.
func TestNodeAndMetaOverGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mp := start(t, ctx, nil)
	np := start(t, ctx, map[iface.NodeID]string{"meta": mp.addr})

	var srv *meta.Server
	mp.loop.Do(func() {
		var err error
		srv, err = meta.NewServer(ctx, meta.Deps{Clock: mp.clock, Net: mp.net, Store: sim.NewMetaStore(), Rand: mp.rng, Log: mp.log}, meta.DefaultConfig("meta"))
		if err != nil {
			t.Error(err)
			return
		}
		srv.Start()
	})
	cfg := node.DefaultConfig("n1", "meta", "r1")
	cfg.Heartbeat, cfg.Addr = 50*time.Millisecond, np.addr
	n := node.New(node.Deps{Clock: np.clock, Net: np.net, Store: sim.NewBlockStore(), Rand: np.rng, Log: np.log}, cfg)
	np.loop.Do(n.Start)

	waitFor(t, func() bool {
		var ok bool
		mp.loop.Do(func() { ok = srv.Cluster().Alive("n1", mp.clock.Now(), time.Second) })
		return ok
	}, "node registered with meta")

	// A 5 MiB chunk goes through the streaming path; the node's incremental
	// block report must reach meta.
	data := make([]byte, 5<<20)
	for i := range data {
		data[i] = byte(i)
	}
	id := iface.ChunkID(sha256.Sum256(data))
	caller := NewCaller(nil, 5*time.Second)
	defer caller.Close()
	r := caller.Do(ctx, []iface.Call{{To: "n1", Addr: np.addr, Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: data})}})
	if r[0].Err != nil {
		t.Fatal(r[0].Err)
	}
	waitFor(t, func() bool {
		var locs []iface.NodeID
		mp.loop.Do(func() { locs = srv.Cluster().Locations(id) })
		return len(locs) == 1
	}, "location reported")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSendToUnknownNodeIsDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := start(t, ctx, nil)
	p.net.Send("nobody", iface.Message{From: "x", Kind: "k"}) // must not panic or block
}

type realEnv struct {
	t      *testing.T
	ctx    context.Context
	procs  map[iface.NodeID]*proc
	caller *Caller
}

func (e *realEnv) proc(id iface.NodeID) *proc {
	if p, ok := e.procs[id]; ok {
		return p
	}
	p := start(e.t, e.ctx, nil)
	e.procs[id] = p
	return p
}

func (e *realEnv) Server(id iface.NodeID) iface.Transport { return e.proc(id).net }
func (e *realEnv) Addr(id iface.NodeID) string            { return e.proc(id).addr }
func (e *realEnv) Clock(id iface.NodeID) iface.Clock      { return e.proc(id).clock }
func (e *realEnv) Caller() iface.Caller                   { return e.caller }

func TestRPCConformance(t *testing.T) {
	ifacetest.RPC(t, func(t *testing.T) ifacetest.RPCEnv {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		c := NewCaller(nil, time.Second)
		t.Cleanup(c.Close)
		return &realEnv{t: t, ctx: ctx, procs: map[iface.NodeID]*proc{}, caller: c}
	})
}
