package cluster

import (
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// State is a JSON-friendly snapshot for the dashboard and replay tests.
type State struct {
	Seed  uint64       `json:"seed"`
	NowMs int64        `json:"nowMs"`
	Meta  MetaView     `json:"meta"`
	Nodes []NodeView   `json:"nodes"`
	Files []FileView   `json:"files"`
	Net   sim.NetStats `json:"net"`
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
	ID         iface.NodeID `json:"id"`
	Rack       string       `json:"rack"`
	Alive      bool         `json:"alive"`
	Crashed    bool         `json:"crashed"`
	UsedBytes  int64        `json:"usedBytes"`
	Chunks     int64        `json:"chunks"`
	Heartbeats uint64       `json:"heartbeats"`
	Acks       uint64       `json:"acks"`
}

// FileView is one committed file.
type FileView struct {
	Path    string `json:"path"`
	Version uint64 `json:"version"`
	Size    int64  `json:"size"`
	Chunks  int    `json:"chunks"`
}

// State returns the current snapshot.
func (c *Cluster) State() State {
	now := c.clock.Now()
	s := State{Seed: c.seed, NowMs: int64(now) / int64(time.Millisecond), Net: c.net.Stats(), Files: []FileView{},
		Meta: MetaView{ID: MetaID, Applied: uint64(c.meta.Applied()), Pending: c.meta.State().PendingUploads()}}
	for _, n := range c.nodes {
		v := NodeView{ID: n.ID(), Rack: n.Rack, Crashed: c.net.Crashed(n.ID()), Heartbeats: n.Stats().Heartbeats, Acks: n.Stats().Acks}
		if ns, ok := c.meta.Cluster().Node(n.ID()); ok {
			v.Alive = c.meta.Cluster().Alive(n.ID())
			v.UsedBytes, v.Chunks = ns.Used, ns.Chunks
		}
		s.Nodes = append(s.Nodes, v)
	}
	for _, e := range c.meta.State().List("/") {
		s.Files = append(s.Files, FileView{Path: e.Path, Version: e.V, Size: e.Size, Chunks: len(e.Chunks)})
	}
	return s
}
