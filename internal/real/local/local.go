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
	"sync"
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
	"github.com/insanityatpeak/chunkd/internal/real/server"
)

// MetaProc is one metadata server process.
type MetaProc struct {
	ID   iface.NodeID
	Addr string
	Loop *runtime.Loop
	Srv  *meta.Server
	// Dir holds its WAL and snapshots.
	Dir  string
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

	ctx   context.Context
	log   *slog.Logger
	cfg   meta.Config
	ids   []iface.NodeID
	addrs map[iface.NodeID]string
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
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &Cluster{cancel: cancel, ctx: ctx, log: log, cfg: cfg}

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
	c.ids, c.addrs = ids, addrs
	serve := func(l net.Listener, peers map[iface.NodeID]string) *proc { return c.serve(l, peers) }

	for i := range lis {
		dirName := "meta"
		if metas > 1 {
			dirName = string(ids[i])
		}
		m := &MetaProc{ID: ids[i], Dir: filepath.Join(dir, dirName)}
		if err := c.startMeta(m, lis[i], false); err != nil {
			c.Close()
			return nil, err
		}
		c.Metas = append(c.Metas, m)
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

// serve runs a process's event loop and gRPC server on l.
func (c *Cluster) serve(l net.Listener, peers map[iface.NodeID]string) *proc {
	loop := runtime.NewLoop()
	p := &proc{addr: l.Addr().String(), loop: loop, clock: runtime.NewClock(loop)}
	p.net = grpcnet.New(p.addr, peers, loop, c.log)
	s := grpc.NewServer(grpc.MaxRecvMsgSize(grpcnet.MaxUnary), grpc.MaxSendMsgSize(grpcnet.MaxUnary))
	p.net.Register(s)
	go func() { _ = s.Serve(l) }()
	go loop.Run(c.ctx)
	var once sync.Once
	p.stop = func() { once.Do(func() { s.Stop(); p.net.Close() }) }
	c.closers = append(c.closers, p.stop)
	return p
}

// startMeta runs metadata process m on l over its directory, which must not
// be open elsewhere. With join, an empty directory joins the running group
// as chunkd-meta -join does (server.MetaIdentity).
func (c *Cluster) startMeta(m *MetaProc, l net.Listener, join bool) error {
	ms, err := metastore.Open(m.Dir)
	if err != nil {
		l.Close()
		return err
	}
	var closeStore sync.Once
	storeClose := func() { closeStore.Do(func() { ms.Close() }) }
	c.closers = append(c.closers, storeClose)
	mc := c.cfg
	mc.ID = m.ID
	if len(c.ids) > 1 {
		mc.Peers = c.ids
	}
	if err := server.MetaIdentity(c.ctx, ms, &mc, c.addrs, join, c.log); err != nil {
		storeClose()
		l.Close()
		return err
	}
	mp := c.serve(l, c.addrs)
	var srv *meta.Server
	mp.loop.Do(func() {
		srv, err = meta.NewServer(c.ctx, meta.Deps{Clock: mp.clock, Net: mp.net, Store: ms, Rand: runtime.NewRand(), Log: c.log}, mc)
		if err == nil {
			srv.Start()
		}
	})
	if err != nil {
		mp.stop()
		storeClose()
		return err
	}
	m.Addr, m.Loop, m.Srv = mp.addr, mp.loop, srv
	m.stop = func() {
		mp.loop.Do(srv.Stop)
		mp.stop()
		storeClose()
	}
	return nil
}

// RestartMeta starts a killed metadata server again on its address over its
// directory; with join, over an emptied directory, it joins as the member a
// replace added (ADR-0031). It fails as chunkd-meta would, e.g. with
// metastore.ErrCorrupt.
func (c *Cluster) RestartMeta(m *MetaProc, join bool) error {
	l, err := net.Listen("tcp", m.Addr)
	if err != nil {
		return err
	}
	return c.startMeta(m, l, join)
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
// it sends nothing more. Its log stays on disk, closed.
func (c *Cluster) KillMeta(m *MetaProc) { m.stop() }

// Close stops every process.
func (c *Cluster) Close() {
	c.cancel()
	for i := len(c.closers) - 1; i >= 0; i-- {
		c.closers[i]()
	}
}
