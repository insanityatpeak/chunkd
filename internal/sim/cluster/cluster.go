// Package cluster assembles an in-process simulated cluster from the same
// core components the real binaries run, wired to sim implementations. The
// WASM build, the chaos tests and the round-trip tests all drive it.
package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/obs"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// MetaID is the metadata server's node ID.
const MetaID iface.NodeID = "meta-1"

// Config sizes the cluster and its network.
type Config struct {
	Nodes  int
	Racks  int
	Meta   meta.Config
	Faults sim.Faults
	// CallTimeout bounds each client RPC in simulated time.
	CallTimeout time.Duration
}

// DefaultConfig is 1 metadata server and 5 storage nodes over 3 racks, on a
// LAN with 1-40 ms latency, 1 Gbit/s links and 1% loss.
func DefaultConfig() Config {
	return Config{
		Nodes:       5,
		Racks:       3,
		Meta:        meta.DefaultConfig(MetaID),
		Faults:      sim.Faults{DropRate: 0.01, DupRate: 0.005, MinDelay: time.Millisecond, MaxDelay: 40 * time.Millisecond, BytesPerSec: 125 << 20},
		CallTimeout: 10 * time.Second,
	}
}

// Cluster is a running simulation. Like the sim, it is single-threaded.
type Cluster struct {
	seed      uint64
	cfg       Config
	clock     *sim.Clock
	net       *sim.Net
	rng       iface.Rand
	log       io.Writer
	metaStore *sim.MetaStore
	meta      *meta.Server
	nodes     []*Node
}

// Node is one simulated storage node.
type Node struct {
	*node.Node
	Rack  string
	Store *sim.BlockStore
}

// New builds and starts a cluster whose every choice derives from seed. Logs
// go to w; pass io.Discard to silence them.
func New(seed uint64, cfg Config, w io.Writer) *Cluster {
	c := &Cluster{seed: seed, cfg: cfg, clock: sim.NewClock(), rng: sim.NewRand(seed), log: w, metaStore: sim.NewMetaStore()}
	c.net = sim.NewNet(c.clock, c.rng, cfg.Faults)
	if err := c.startMeta(); err != nil {
		panic(err) // an empty in-memory log cannot fail to recover
	}
	for i := 1; i <= cfg.Nodes; i++ {
		id := iface.NodeID(fmt.Sprintf("node-%d", i))
		rack := fmt.Sprintf("r%d", (i-1)%max(cfg.Racks, 1)+1)
		store := sim.NewBlockStore()
		n := node.New(node.Deps{Clock: c.clock, Net: c.net, Store: store, Rand: c.rng, Log: c.logger(id)}, node.DefaultConfig(id, MetaID, rack))
		n.Start()
		c.nodes = append(c.nodes, &Node{Node: n, Rack: rack, Store: store})
	}
	return c
}

func (c *Cluster) logger(id iface.NodeID) *slog.Logger {
	return obs.NewLogger(c.log, string(id), slog.LevelInfo)
}

func (c *Cluster) startMeta() error {
	srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: c.clock, Net: c.net, Store: c.metaStore, Rand: c.rng, Log: c.logger(MetaID)}, c.cfg.Meta)
	if err != nil {
		return err
	}
	srv.Start()
	c.meta = srv
	return nil
}

// RestartMeta replaces the metadata server with a fresh one recovered from
// the same log, as after a process crash. Locations come back through block
// reports.
func (c *Cluster) RestartMeta() error { return c.startMeta() }

// Tick advances simulated time by d, running every event due in that window.
func (c *Cluster) Tick(d time.Duration) { c.clock.Advance(d) }

// Now returns simulated time.
func (c *Cluster) Now() iface.Instant { return c.clock.Now() }

// Net exposes the network for fault injection.
func (c *Cluster) Net() *sim.Net { return c.net }

// Meta exposes the metadata server. Only touch it between Ticks.
func (c *Cluster) Meta() *meta.Server { return c.meta }

// Nodes returns the storage nodes in ID order.
func (c *Cluster) Nodes() []*Node { return c.nodes }

// Seed returns the seed the cluster was built from.
func (c *Cluster) Seed() uint64 { return c.seed }

// Config returns the configuration.
func (c *Cluster) Config() Config { return c.cfg }

// NewCaller returns a client endpoint on the sim network.
func (c *Cluster) NewCaller(id iface.NodeID) *sim.Caller { return c.net.NewCaller(id, c.cfg.CallTimeout) }
