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

// Cluster is a running in-process real cluster.
type Cluster struct {
	cancel  context.CancelFunc
	closers []func()
	Meta    *meta.Server
	MetaLoop *runtime.Loop
	MetaAddr string
	caller  *grpcnet.Caller
	Client  *client.Direct
}

type proc struct {
	addr  string
	loop  *runtime.Loop
	clock *runtime.Clock
	net   *grpcnet.Transport
}

// Start runs 1 metadata server and n storage nodes (racks r1..r3) under dir,
// and waits until every node has heartbeated and reported.
func Start(dir string, n int, cfg meta.Config) (*Cluster, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{cancel: cancel}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	start := func(peers map[iface.NodeID]string) (*proc, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		loop := runtime.NewLoop()
		p := &proc{addr: lis.Addr().String(), loop: loop, clock: runtime.NewClock(loop)}
		p.net = grpcnet.New(p.addr, peers, loop, log)
		s := grpc.NewServer(grpc.MaxRecvMsgSize(grpcnet.MaxUnary), grpc.MaxSendMsgSize(grpcnet.MaxUnary))
		p.net.Register(s)
		go func() { _ = s.Serve(lis) }()
		go loop.Run(ctx)
		c.closers = append(c.closers, s.Stop, p.net.Close)
		return p, nil
	}

	mp, err := start(nil)
	if err != nil {
		c.Close()
		return nil, err
	}
	ms, err := metastore.Open(filepath.Join(dir, "meta"))
	if err != nil {
		c.Close()
		return nil, err
	}
	c.closers = append(c.closers, func() { ms.Close() })
	cfg.ID = "meta-1"
	var srvErr error
	mp.loop.Do(func() {
		c.Meta, srvErr = meta.NewServer(ctx, meta.Deps{Clock: mp.clock, Net: mp.net, Store: ms, Rand: runtime.NewRand(), Log: log}, cfg)
		if srvErr == nil {
			c.Meta.Start()
		}
	})
	if srvErr != nil {
		c.Close()
		return nil, srvErr
	}
	c.MetaLoop, c.MetaAddr = mp.loop, mp.addr

	for i := 1; i <= n; i++ {
		np, err := start(map[iface.NodeID]string{"meta-1": mp.addr})
		if err != nil {
			c.Close()
			return nil, err
		}
		bs, err := blockstore.Open(filepath.Join(dir, fmt.Sprintf("node-%d", i)))
		if err != nil {
			c.Close()
			return nil, err
		}
		ncfg := node.DefaultConfig(iface.NodeID(fmt.Sprintf("node-%d", i)), "meta-1", fmt.Sprintf("r%d", (i-1)%3+1))
		ncfg.Addr, ncfg.Heartbeat = np.addr, 100*time.Millisecond
		peers := grpcnet.NewCaller(nil, 30*time.Second)
		c.closers = append(c.closers, peers.Close)
		nd := node.New(node.Deps{Clock: np.clock, Net: np.net, Async: peers.Async(np.loop), Store: bs, Rand: runtime.NewRand(), Log: log}, ncfg)
		np.loop.Do(nd.Start)
	}

	c.caller = grpcnet.NewCaller(map[iface.NodeID]string{"meta-1": mp.addr}, 30*time.Second)
	c.closers = append(c.closers, c.caller.Close)
	c.Client = client.New(c.caller, client.Options{Meta: "meta-1", MetaAddr: mp.addr})

	deadline := time.Now().Add(10 * time.Second)
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
			return nil, fmt.Errorf("local cluster: %d of %d nodes alive after 10s (%v)", alive, n, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Close stops every process.
func (c *Cluster) Close() {
	c.cancel()
	for i := len(c.closers) - 1; i >= 0; i-- {
		c.closers[i]()
	}
}
