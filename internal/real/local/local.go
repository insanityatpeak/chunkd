// Package local starts a real-mode cluster inside one process: real gRPC on
// loopback, real disk stores and WAL in a directory, wall clock. Tests use
// it to run the same scenarios as the sim against the production code path.
package local

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/blockstore"
	"github.com/insanityatpeak/chunkd/internal/real/grpcnet"
	"github.com/insanityatpeak/chunkd/internal/real/metastore"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
)

// MetaProc is one metadata server process.
type MetaProc struct {
	ID   iface.NodeID
	Addr string
	Loop *runtime.Loop
	Srv  *meta.Server
	stop func()
}

// Cluster is a running in-process real cluster.
type Cluster struct {
	cancel  context.CancelFunc
	closers []func()
	// Metas are the metadata servers. Meta, MetaLoop and MetaAddr are the
	// first one, which is the only one in a single-server cluster.
	Metas    []*MetaProc
	Meta     *meta.Server
	MetaLoop *runtime.Loop
	MetaAddr string
	caller   *grpcnet.Caller
	Client   *client.Direct
}

type proc struct {
	addr  string
	loop  *runtime.Loop
	clock *runtime.Clock
	net   *grpcnet.Transport
	stop  func()
}

// Start runs 1 metadata server and n storage nodes (racks r1..r3) under dir,
// and waits until every node has heartbeated and reported.
func Start(dir string, n int, cfg meta.Config) (*Cluster, error) {
	return StartGroup(dir, 1, n, cfg)
}

// StartGroup runs a group of metas metadata servers and n storage nodes
// under dir. The nodes heartbeat and report to every metadata server.
func StartGroup(dir string, metas, n int, cfg meta.Config) (*Cluster, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{cancel: cancel}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Listeners first: every process needs every metadata address.
	lis := make([]net.Listener, metas)
	ids := make([]iface.NodeID, metas)
	addrs := map[iface.NodeID]string{}
	for i := range lis {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			c.Close()
			return nil, err
		}
		lis[i], ids[i] = l, iface.NodeID(fmt.Sprintf("meta-%d", i+1))
		addrs[ids[i]] = l.Addr().String()
	}

	serve := func(l net.Listener, peers map[iface.NodeID]string) *proc {
		loop := runtime.NewLoop()
		p := &proc{addr: l.Addr().String(), loop: loop, clock: runtime.NewClock(loop)}
		p.net = grpcnet.New(p.addr, peers, loop, log)
		s := grpc.NewServer(grpc.MaxRecvMsgSize(grpcnet.MaxUnary), grpc.MaxSendMsgSize(grpcnet.MaxUnary))
		p.net.Register(s)
		go func() { _ = s.Serve(l) }()
		go loop.Run(ctx)
		p.stop = func() { s.Stop(); p.net.Close() }
		c.closers = append(c.closers, p.stop)
		return p
	}

	for i := range lis {
		mp := serve(lis[i], addrs)
		dirName := "meta"
		if metas > 1 {
			dirName = string(ids[i])
		}
		ms, err := metastore.Open(filepath.Join(dir, dirName))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.closers = append(c.closers, func() { ms.Close() })
		mc := cfg
		mc.ID = ids[i]
		if metas > 1 {
			mc.Peers = ids
		}
		var srv *meta.Server
		var srvErr error
		mp.loop.Do(func() {
			srv, srvErr = meta.NewServer(ctx, meta.Deps{Clock: mp.clock, Net: mp.net, Store: ms, Rand: runtime.NewRand(), Log: log}, mc)
			if srvErr == nil {
				srv.Start()
			}
		})
		if srvErr != nil {
			c.Close()
			return nil, srvErr
		}
		c.Metas = append(c.Metas, &MetaProc{ID: ids[i], Addr: mp.addr, Loop: mp.loop, Srv: srv, stop: func() {
			mp.loop.Do(srv.Stop)
			mp.stop()
		}})
	}
	c.Meta, c.MetaLoop, c.MetaAddr = c.Metas[0].Srv, c.Metas[0].Loop, c.Metas[0].Addr

	for i := 1; i <= n; i++ {
		np := serve(mustListen(c), addrs)
		bs, err := blockstore.Open(filepath.Join(dir, fmt.Sprintf("node-%d", i)))
		if err != nil {
			c.Close()
			return nil, err
		}
		ncfg := node.DefaultConfig(iface.NodeID(fmt.Sprintf("node-%d", i)), ids[0], fmt.Sprintf("r%d", (i-1)%3+1))
		ncfg.Addr, ncfg.Heartbeat, ncfg.Metas = np.addr, 100*time.Millisecond, ids
		peers := grpcnet.NewCaller(nil, 30*time.Second)
		c.closers = append(c.closers, peers.Close)
		nd, err := node.New(node.Deps{Clock: np.clock, Net: np.net, Async: peers.Async(np.loop), Store: bs, Rand: runtime.NewRand(), Log: log}, ncfg)
		if err != nil {
			c.Close()
			return nil, err
		}
		np.loop.Do(nd.Start)
	}

	c.caller = grpcnet.NewCaller(addrs, 30*time.Second)
	c.closers = append(c.closers, c.caller.Close)
	opts := client.Options{}
	for _, m := range c.Metas {
		opts.Peers = append(opts.Peers, client.MetaPeer{ID: m.ID, Addr: m.Addr})
	}
	if metas > 1 {
		opts.Rand = runtime.NewRand()
	}
	c.Client = client.New(c.caller, opts)

	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := c.Client.Cluster(ctx, 0)
		alive := 0
		for _, nd := range info.Nodes {
			if nd.Alive {
				alive++
			}
		}
		if err == nil && alive == n {
			return c, nil
		}
		if time.Now().After(deadline) {
			c.Close()
			return nil, fmt.Errorf("local cluster: %d of %d nodes alive after 15s (%v)", alive, n, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func mustListen(c *Cluster) net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err) // loopback with an ephemeral port cannot run out in a test
	}
	return l
}

// Leader returns the metadata server that is a ready leader, or nil.
func (c *Cluster) Leader() *MetaProc {
	for _, m := range c.Metas {
		var ready bool
		m.Loop.Do(func() { ready = m.Srv.Raft().Ready })
		if ready {
			return m
		}
	}
	return nil
}

// KillMeta stops a metadata server the way a crash would: its port closes and
// it sends nothing more. Its log stays on disk.
func (c *Cluster) KillMeta(m *MetaProc) { m.stop() }

// Close stops every process.
func (c *Cluster) Close() {
	c.cancel()
	for i := len(c.closers) - 1; i >= 0; i-- {
		c.closers[i]()
	}
}
