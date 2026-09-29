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
	// Integrity counters from the node's heartbeats.
	Corrupt               uint64
	ScrubDone, ScrubTotal int64
	ScrubPasses           uint64
	State                 detector.State
	LastSeen              iface.Instant
	// DeadSince is when the detector declared the node dead; repair waits
	// a delay from here before replacing its replicas.
	DeadSince iface.Instant
	// Incarnation is the node process's, from its heartbeats.
	Incarnation uint64
	// reported is false until a full block report arrives; until then the
	// node's locations are unknown, not empty.
	reported bool
	// ledger orders this incarnation's block reports.
	ledger ledger
}

// ledger orders one node incarnation's block reports, which the network
// may deliver out of order. A full report with seq S lists exactly the
// store changes whose reports carry seq < S (the node takes the seq and
// lists its store under a lock that excludes puts and deletes).
type ledger struct {
	inc uint64
	// full is the seq of the newest full report applied; anything at or
	// below it is already reflected and is ignored.
	full uint64
	// last holds, for chunks changed by incremental reports newer than
	// full, the seq of the latest change (an add or a delete).
	last map[iface.ChunkID]uint64
	// seen is the highest seq applied: the fence for GC deletes. Every
	// store change numbered at or below it happened before the server's
	// current view was formed.
	seen uint64
}

// Report is one block report: Full lists every chunk the node holds in
// Added; otherwise Added and Deleted are changes since the node's last
// report.
type Report struct {
	Incarnation, Seq uint64
	Full             bool
	Added, Deleted   []iface.ChunkID
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
	n.Corrupt, n.ScrubDone, n.ScrubTotal, n.ScrubPasses = hb.Corrupt, hb.ScrubDone, hb.ScrubTotal, hb.ScrubPasses
	n.Incarnation = b.Incarnation
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

// Report applies a block report and returns the location changes it made.
// Reports from another incarnation than the node's latest heartbeat are
// ignored (ok false): a killed process's late reports describe a disk the
// new process reports itself.
//
// Without the ordering, a full report listed just before a put finished but
// delivered after that put's incremental report dropped the new chunk, and
// repair copied it again; a delete overtaken by an older full report
// re-added a copy that no longer existed (bugs-found #7).
func (c *Cluster) Report(id iface.NodeID, r Report) (added, removed []iface.ChunkID, ok bool) {
	n := c.nodes[id]
	if n == nil || r.Incarnation != n.Incarnation {
		return nil, nil, false
	}
	l := &n.ledger
	if l.inc != r.Incarnation {
		*l = ledger{inc: r.Incarnation}
	}
	l.seen = max(l.seen, r.Seq)
	if r.Seq <= l.full {
		return nil, nil, true
	}
	if c.byNode[id] == nil {
		c.byNode[id] = map[iface.ChunkID]struct{}{}
	}
	// newer: an incremental change after seq already decided ch.
	newer := func(ch iface.ChunkID, seq uint64) bool {
		last, ok := l.last[ch]
		return ok && last > seq
	}
	put := func(ch iface.ChunkID) {
		if _, has := c.byNode[id][ch]; !has {
			c.add(id, ch)
			added = append(added, ch)
		}
	}
	del := func(ch iface.ChunkID) {
		if _, has := c.byNode[id][ch]; has {
			c.drop(id, ch)
			removed = append(removed, ch)
		}
	}
	if !r.Full {
		if l.last == nil {
			l.last = map[iface.ChunkID]uint64{}
		}
		for _, ch := range r.Added {
			if !newer(ch, r.Seq) {
				l.last[ch] = r.Seq
				put(ch)
			}
		}
		for _, ch := range r.Deleted {
			if !newer(ch, r.Seq) {
				l.last[ch] = r.Seq
				del(ch)
			}
		}
		return added, removed, true
	}
	listed := make(map[iface.ChunkID]struct{}, len(r.Added))
	for _, ch := range r.Added {
		listed[ch] = struct{}{}
		if !newer(ch, r.Seq) {
			put(ch)
		}
	}
	for ch := range c.byNode[id] {
		if _, ok := listed[ch]; !ok && !newer(ch, r.Seq) {
			del(ch)
		}
	}
	l.full = r.Seq
	for ch, seq := range l.last {
		if seq <= r.Seq {
			delete(l.last, ch)
		}
	}
	n.reported = true
	return added, removed, true
}

// Reported reports whether the node's locations are confirmed by a full
// report since it joined, restarted or died.
func (c *Cluster) Reported(id iface.NodeID) bool {
	n := c.nodes[id]
	return n != nil && n.reported
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

// Fence returns the incarnation and highest applied report seq of a node
// whose locations are confirmed, for a conditional GC delete.
func (c *Cluster) Fence(id iface.NodeID) (inc, seq uint64, ok bool) {
	n := c.nodes[id]
	if n == nil || !n.reported {
		return 0, 0, false
	}
	return n.ledger.inc, n.ledger.seen, true
}

// Located returns every chunk with at least one reported copy, sorted.
func (c *Cluster) Located() []iface.ChunkID {
	return slices.SortedFunc(maps.Keys(c.byChunk), func(a, b iface.ChunkID) int { return slices.Compare(a[:], b[:]) })
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
