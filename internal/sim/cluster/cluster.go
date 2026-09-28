// Package cluster assembles an in-process simulated cluster from core
// components wired to sim implementations. The WASM build and chaos tests
// both drive it.
package cluster

import (
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/heartbeat"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/obs"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// Config sizes the cluster and its network.
type Config struct {
	Nodes     int
	Heartbeat time.Duration
	DeadAfter time.Duration
	Faults    sim.Faults
}

// DefaultConfig is the hello-world cluster: 1 metadata server, 3 storage
// nodes, 1 s heartbeats over a slightly lossy LAN.
func DefaultConfig() Config {
	return Config{
		Nodes:     3,
		Heartbeat: time.Second,
		DeadAfter: 3 * time.Second,
		Faults:    sim.Faults{DropRate: 0.02, DupRate: 0.01, MinDelay: time.Millisecond, MaxDelay: 40 * time.Millisecond},
	}
}

const metaID iface.NodeID = "meta-1"

// Cluster is a running simulation. It is single-threaded like the sim itself.
type Cluster struct {
	seed    uint64
	cfg     Config
	clock   *sim.Clock
	net     *sim.Net
	tracker *heartbeat.Tracker
	nodes   []*node
}

type node struct {
	id     iface.NodeID
	sender *heartbeat.Sender
}

// New builds and starts a cluster whose every choice derives from seed. Logs
// go to w; pass io.Discard to silence them.
func New(seed uint64, cfg Config, w io.Writer) *Cluster {
	clock := sim.NewClock()
	rng := sim.NewRand(seed)
	net := sim.NewNet(clock, rng, cfg.Faults)
	c := &Cluster{seed: seed, cfg: cfg, clock: clock, net: net}

	deps := func(id iface.NodeID) heartbeat.Deps {
		return heartbeat.Deps{Clock: clock, Net: net, Rand: rng, Log: obs.NewLogger(w, string(id), slog.LevelInfo)}
	}
	c.tracker = heartbeat.NewTracker(deps(metaID), metaID)
	c.tracker.Start()
	for i := 1; i <= cfg.Nodes; i++ {
		id := iface.NodeID(fmt.Sprintf("node-%d", i))
		s := heartbeat.NewSender(deps(id), id, metaID, cfg.Heartbeat)
		s.Start()
		c.nodes = append(c.nodes, &node{id: id, sender: s})
	}
	return c
}

// Tick advances simulated time by d, running every event due in that window.
func (c *Cluster) Tick(d time.Duration) { c.clock.Advance(d) }

// State is a JSON-friendly snapshot for the dashboard and for replay tests.
type State struct {
	Seed  uint64       `json:"seed"`
	NowMs int64        `json:"nowMs"`
	Meta  MetaState    `json:"meta"`
	Nodes []NodeState  `json:"nodes"`
	Net   sim.NetStats `json:"net"`
}

// MetaState is the metadata server's view.
type MetaState struct {
	ID    iface.NodeID `json:"id"`
	Peers []PeerView   `json:"peers"`
}

// PeerView is one node as the metadata server sees it.
type PeerView struct {
	heartbeat.PeerState
	Alive bool `json:"alive"`
}

// NodeState is one storage node's own counters.
type NodeState struct {
	ID iface.NodeID `json:"id"`
	heartbeat.SenderStats
}

// State returns the current snapshot.
func (c *Cluster) State() State {
	s := State{Seed: c.seed, NowMs: int64(c.clock.Now()) / int64(time.Millisecond), Net: c.net.Stats()}
	s.Meta.ID = metaID
	for _, p := range c.tracker.Peers() {
		s.Meta.Peers = append(s.Meta.Peers, PeerView{PeerState: p, Alive: c.tracker.Alive(p.ID, c.cfg.DeadAfter)})
	}
	for _, n := range c.nodes {
		s.Nodes = append(s.Nodes, NodeState{ID: n.id, SenderStats: n.sender.Stats()})
	}
	return s
}

// Net exposes the network for fault injection in tests and the dashboard.
func (c *Cluster) Net() *sim.Net { return c.net }
