package cluster

import (
	"slices"
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
	Metas    []MetaPeerView      `json:"metas"`
	Nodes    []NodeView          `json:"nodes"`
	Files    []FileView          `json:"files"`
	Net      sim.NetStats        `json:"net"`
	Health   client.Health       `json:"health"`
	Copies   []client.RepairCopy `json:"copies"`
	Events   []client.Event      `json:"events"`
	EventSeq uint64              `json:"eventSeq"`
	// Reads are the scripted client calls' results (kind "read" or
	// "write"), newest last, at most 100; their seq is separate from the
	// metadata server's.
	Reads []client.Event `json:"reads"`
	// Dedup, GC and deleted files, as in the gateway's /cluster.
	ReferencedBytes int64                `json:"referencedBytes"`
	DistinctBytes   int64                `json:"distinctBytes"`
	Epoch           uint64               `json:"epoch"`
	GC              client.GCStats       `json:"gc"`
	Deleted         []client.DeletedFile `json:"deleted"`
}

// MetaView is the peer the dashboard reads: the leader, or during an
// election the live peer that has applied the most.
type MetaView struct {
	ID      iface.NodeID `json:"id"`
	Applied uint64       `json:"applied"`
	Pending int          `json:"pendingUploads"`
}

// MetaPeerView is one metadata peer: its Raft view plus injected faults.
type MetaPeerView struct {
	ID iface.NodeID `json:"id"`
	// State is the Raft role (leader, follower, candidate, pre-candidate),
	// or down or frozen while that fault holds.
	State string `json:"state"`
	// Leader: leads with a quorum and has applied its term's first entry,
	// so it serves. A cut-off leader keeps its role but not this.
	Leader  bool   `json:"leader"`
	CutOff  bool   `json:"cutOff"`
	Term    uint64 `json:"term"`
	Commit  uint64 `json:"commit"`
	Applied uint64 `json:"applied"`
}

// NodeView is one storage node, as the metadata server sees it plus the
// node's own counters.
type NodeView struct {
	client.NodeInfo
	Crashed bool `json:"crashed"`
	// Injected faults, so the dashboard can show and toggle them.
	Frozen      bool   `json:"frozen"`
	SlowMs      int64  `json:"slowMs"`
	Partitioned bool   `json:"partitioned"`
	Heartbeats  uint64 `json:"heartbeats"`
	Acks        uint64 `json:"acks"`
}

// FileView is one committed file and its replication state.
type FileView struct {
	Path            string            `json:"path"`
	Version         uint64            `json:"version"`
	Size            int64             `json:"size"`
	Chunks          int               `json:"chunks"`
	UnderReplicated int               `json:"underReplicated"`
	MinLive         int               `json:"minLive"`
	Redundancy      client.Redundancy `json:"redundancy,omitempty"`
}

// State returns the current snapshot with every retained event.
func (c *Cluster) State() State { return c.StateSince(0) }

// StateSince returns the current snapshot with the events after seq.
func (c *Cluster) StateSince(seq uint64) State {
	now := c.clock.Now()
	srv := c.Meta()
	// The merged log replaces the peer's own events; ask it for none.
	view := client.ClusterFromProto(srv.ClusterView(srv.EventSeq()))
	events, eventSeq := c.eventsSince(seq)
	s := State{Seed: c.seed, NowMs: int64(now) / int64(time.Millisecond), Net: c.net.Stats(), Files: []FileView{},
		Meta:   MetaView{ID: c.metaID(srv), Applied: uint64(srv.Applied()), Pending: srv.State().PendingUploads()},
		Health: view.Health, Copies: view.Copies, Events: events, EventSeq: eventSeq, Reads: slices.Clone(c.readLog),
		ReferencedBytes: view.ReferencedBytes, DistinctBytes: view.DistinctBytes, Epoch: view.Epoch, GC: view.GC, Deleted: view.Deleted}
	if s.Reads == nil {
		s.Reads = []client.Event{}
	}
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
		s.Nodes = append(s.Nodes, NodeView{NodeInfo: info, Crashed: c.net.Crashed(n.ID()), Frozen: c.net.Frozen(n.ID()),
			SlowMs: int64(c.net.Slow(n.ID()) / time.Millisecond), Partitioned: c.nodeCut(n.ID()),
			Heartbeats: n.Stats().Heartbeats, Acks: n.Stats().Acks})
	}
	health := map[string]client.FileHealth{}
	for _, f := range view.FileHealth {
		health[f.Path] = f
	}
	for _, p := range c.metas {
		st := p.srv.Raft()
		v := MetaPeerView{ID: p.id, State: st.Role, CutOff: c.MetaCut(p.id), Term: st.Term, Commit: st.Commit, Applied: st.Applied}
		switch {
		case c.net.Crashed(p.id):
			v.State = "down"
		case c.net.Frozen(p.id):
			v.State = "frozen"
		default:
			v.Leader = st.Ready
		}
		s.Metas = append(s.Metas, v)
	}
	for _, e := range srv.State().List("/") {
		h := health[e.Path]
		s.Files = append(s.Files, FileView{Path: e.Path, Version: e.V, Size: e.Size, Chunks: len(e.Chunks), UnderReplicated: h.UnderReplicated, MinLive: h.MinLive,
			Redundancy: h.Redundancy})
	}
	return s
}

// nodeCut reports whether node id is cut off from every metadata peer.
func (c *Cluster) nodeCut(id iface.NodeID) bool {
	for _, m := range c.MetaIDs() {
		if !c.net.Blocked(id, m) {
			return false
		}
	}
	return true
}
