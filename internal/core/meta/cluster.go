package meta

import (
	"cmp"
	"maps"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/core/placement"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// NodeState is what the metadata server knows about a storage node. None of
// it is durable.
type NodeState struct {
	ID       iface.NodeID
	Rack     string
	Addr     string
	Used     int64
	Chunks   int64
	Draining bool
	State    detector.State
	LastSeen iface.Instant
	// DeadSince is when the detector declared the node dead; repair waits
	// a delay from here before replacing its replicas.
	DeadSince iface.Instant
	// reported is false until a full block report arrives; until then the
	// node's locations are unknown, not empty.
	reported bool
}

// Cluster tracks nodes and chunk locations, rebuilt from heartbeats and
// block reports. As in GFS and HDFS, locations are never persisted: nodes are
// the source of truth for what they hold, and a restarted metadata server
// asks for full reports instead of trusting a stale copy.
type Cluster struct {
	det     *detector.Detector
	nodes   map[iface.NodeID]*NodeState
	byNode  map[iface.NodeID]map[iface.ChunkID]struct{}
	byChunk map[iface.ChunkID]map[iface.NodeID]struct{}
}

// NewCluster returns an empty view whose liveness comes from a detector with
// the given thresholds.
func NewCluster(cfg detector.Config) *Cluster {
	return &Cluster{
		det:     detector.New(cfg),
		nodes:   map[iface.NodeID]*NodeState{},
		byNode:  map[iface.NodeID]map[iface.ChunkID]struct{}{},
		byChunk: map[iface.ChunkID]map[iface.NodeID]struct{}{},
	}
}

// Heartbeat records a heartbeat. It returns the detector's transition, if
// any, and whether the node must send a full block report.
func (c *Cluster) Heartbeat(hb NodeState, b detector.Beat, now iface.Instant) (tr detector.Transition, changed, needFullReport bool) {
	n := c.nodes[hb.ID]
	if n == nil {
		n = &NodeState{ID: hb.ID}
		c.nodes[hb.ID] = n
	}
	n.Rack, n.Addr, n.Used, n.Chunks, n.Draining = hb.Rack, hb.Addr, hb.Used, hb.Chunks, hb.Draining
	tr, changed = c.det.Observe(hb.ID, b, now)
	if changed {
		c.apply(tr)
	}
	n.LastSeen, _ = c.det.LastSeen(hb.ID)
	return tr, changed, !n.reported
}

// Tick applies heartbeat timeouts and returns the transitions.
func (c *Cluster) Tick(now iface.Instant) []detector.Transition {
	trs := c.det.Tick(now)
	for _, tr := range trs {
		c.apply(tr)
	}
	return trs
}

// apply keeps the node table in step with the detector. Locations are
// never dropped on a transition, only replaced by the node's next full
// report: dropping a restarted node's locations made its chunks look lost
// for the round trip before that report and started needless repairs
// (bugs-found #4). A dead or restarted node must send a full report before
// it is trusted again, and until it is alive its copies do not count
// toward commit.
func (c *Cluster) apply(tr detector.Transition) {
	n := c.nodes[tr.Node]
	if n == nil {
		return
	}
	n.State = tr.To
	if tr.To == detector.Dead {
		n.DeadSince = tr.At
	}
	if tr.To == detector.Dead || tr.Restarted {
		n.reported = false
	}
}

// FullReport replaces the node's known chunks.
func (c *Cluster) FullReport(id iface.NodeID, chunks []iface.ChunkID) {
	for ch := range c.byNode[id] {
		c.drop(id, ch)
	}
	c.byNode[id] = map[iface.ChunkID]struct{}{}
	for _, ch := range chunks {
		c.add(id, ch)
	}
	if n := c.nodes[id]; n != nil {
		n.reported = true
	}
}

// Received records that a node stored a chunk (incremental block report).
func (c *Cluster) Received(id iface.NodeID, chunks []iface.ChunkID) {
	if c.byNode[id] == nil {
		c.byNode[id] = map[iface.ChunkID]struct{}{}
	}
	for _, ch := range chunks {
		c.add(id, ch)
	}
}

func (c *Cluster) add(id iface.NodeID, ch iface.ChunkID) {
	c.byNode[id][ch] = struct{}{}
	if c.byChunk[ch] == nil {
		c.byChunk[ch] = map[iface.NodeID]struct{}{}
	}
	c.byChunk[ch][id] = struct{}{}
}

func (c *Cluster) drop(id iface.NodeID, ch iface.ChunkID) {
	delete(c.byNode[id], ch)
	if m := c.byChunk[ch]; m != nil {
		delete(m, id)
		if len(m) == 0 {
			delete(c.byChunk, ch)
		}
	}
}

// Alive reports whether the detector considers the node alive: it takes
// new replicas and its copies count toward commit.
func (c *Cluster) Alive(id iface.NodeID) bool { return c.det.State(id) == detector.Alive }

// Readable reports whether reads may be sent to the node (alive or suspect).
func (c *Cluster) Readable(id iface.NodeID) bool {
	s := c.det.State(id)
	return s == detector.Alive || s == detector.Suspect
}

func (c *Cluster) state(id iface.NodeID) detector.State { return c.det.State(id) }

// Detector exposes the detector's thresholds and counters.
func (c *Cluster) Detector() *detector.Detector { return c.det }

// Locations returns the nodes reported to hold ch, sorted by ID. It includes
// suspect and dead nodes; callers filter with Alive or Readable.
func (c *Cluster) Locations(ch iface.ChunkID) []iface.NodeID {
	return slices.Sorted(maps.Keys(c.byChunk[ch]))
}

// Node returns one node's state.
func (c *Cluster) Node(id iface.NodeID) (NodeState, bool) {
	n := c.nodes[id]
	if n == nil {
		return NodeState{}, false
	}
	return *n, true
}

// Nodes returns every known node sorted by ID.
func (c *Cluster) Nodes() []NodeState {
	out := make([]NodeState, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, *n)
	}
	slices.SortFunc(out, func(a, b NodeState) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// PlacementView converts the node table for placement.Place.
// Suspect nodes are excluded: a node that may be dying takes no new data.
func (c *Cluster) PlacementView() []placement.Node {
	var out []placement.Node
	for _, n := range c.Nodes() {
		out = append(out, placement.Node{ID: n.ID, Rack: n.Rack, Used: n.Used, Alive: c.Alive(n.ID), Draining: n.Draining})
	}
	return out
}
