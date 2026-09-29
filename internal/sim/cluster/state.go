package cluster

import (
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// State is a JSON-friendly snapshot for the dashboard and replay tests. The
// health, copies and events fields have the gateway's /cluster shape, built
// by the same meta.ClusterView, so one UI renders both.
type State struct {
	Seed     uint64              `json:"seed"`
	NowMs    int64               `json:"nowMs"`
	Meta     MetaView            `json:"meta"`
	Nodes    []NodeView          `json:"nodes"`
	Files    []FileView          `json:"files"`
	Net      sim.NetStats        `json:"net"`
	Health   client.Health       `json:"health"`
	Copies   []client.RepairCopy `json:"copies"`
	Events   []client.Event      `json:"events"`
	EventSeq uint64              `json:"eventSeq"`
}

// MetaView is the metadata server's summary.
type MetaView struct {
	ID      iface.NodeID `json:"id"`
	Applied uint64       `json:"applied"`
	Pending int          `json:"pendingUploads"`
}

// NodeView is one storage node, as the metadata server sees it plus the
// node's own counters.
type NodeView struct {
	client.NodeInfo
	Crashed    bool   `json:"crashed"`
	Heartbeats uint64 `json:"heartbeats"`
	Acks       uint64 `json:"acks"`
}

// FileView is one committed file and its replication state.
type FileView struct {
	Path            string `json:"path"`
	Version         uint64 `json:"version"`
	Size            int64  `json:"size"`
	Chunks          int    `json:"chunks"`
	UnderReplicated int    `json:"underReplicated"`
	MinLive         int    `json:"minLive"`
}

// State returns the current snapshot with every retained event.
func (c *Cluster) State() State { return c.StateSince(0) }

// StateSince returns the current snapshot with the events after seq.
func (c *Cluster) StateSince(seq uint64) State {
	now := c.clock.Now()
	view := client.ClusterFromProto(c.meta.ClusterView(seq))
	s := State{Seed: c.seed, NowMs: int64(now) / int64(time.Millisecond), Net: c.net.Stats(), Files: []FileView{},
		Meta:   MetaView{ID: MetaID, Applied: uint64(c.meta.Applied()), Pending: c.meta.State().PendingUploads()},
		Health: view.Health, Copies: view.Copies, Events: view.Events, EventSeq: view.EventSeq}
	known := map[string]client.NodeInfo{}
	for _, n := range view.Nodes {
		known[n.ID] = n
	}
	for _, n := range c.nodes {
		info, ok := known[string(n.ID())]
		if !ok {
			info = client.NodeInfo{ID: string(n.ID()), State: "unknown"}
		}
		info.Rack = n.Rack
		s.Nodes = append(s.Nodes, NodeView{NodeInfo: info, Crashed: c.net.Crashed(n.ID()), Heartbeats: n.Stats().Heartbeats, Acks: n.Stats().Acks})
	}
	health := map[string]client.FileHealth{}
	for _, f := range view.FileHealth {
		health[f.Path] = f
	}
	for _, e := range c.meta.State().List("/") {
		h := health[e.Path]
		s.Files = append(s.Files, FileView{Path: e.Path, Version: e.V, Size: e.Size, Chunks: len(e.Chunks), UnderReplicated: h.UnderReplicated, MinLive: h.MinLive})
	}
	return s
}
