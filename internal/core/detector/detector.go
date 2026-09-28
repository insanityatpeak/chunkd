// Package detector decides which storage nodes are alive from their
// heartbeats. It is a pure state machine over logical time: the owner feeds
// it heartbeats and periodic ticks and acts on the transitions it returns.
package detector

import (
	"cmp"
	"maps"
	"slices"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// State is the detector's opinion of one node.
type State int

const (
	// Alive nodes take new replicas and count toward commit.
	Alive State = iota + 1
	// Suspect nodes missed beats, or are recovering and have not yet sent
	// enough on-time beats. They serve reads but take no new replicas.
	Suspect
	// Dead nodes are silent past DeadAfter. Their replicas no longer count.
	Dead
)

func (s State) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	}
	return "unknown"
}

// Config sets the detector's thresholds.
type Config struct {
	// Interval is the nodes' heartbeat period.
	Interval time.Duration
	// SuspectAfter and DeadAfter are silences that trigger each state.
	SuspectAfter time.Duration
	DeadAfter    time.Duration
	// OnTime is the largest gap between two beats that counts toward
	// recovery; RecoverBeats consecutive on-time beats make a node alive.
	OnTime       time.Duration
	RecoverBeats int
	// TickEvery is how often the owner calls Tick. A gap above 2×TickEvery
	// between ticks means the owner itself stalled (GC pause, slow fsync).
	TickEvery time.Duration
}

// DefaultConfig: 1 s beats, suspect after 3 missed, dead after 10 s, 3 on-time
// beats to recover.
func DefaultConfig() Config {
	return Config{
		Interval:     time.Second,
		SuspectAfter: 3 * time.Second,
		DeadAfter:    10 * time.Second,
		OnTime:       1500 * time.Millisecond,
		RecoverBeats: 3,
		TickEvery:    500 * time.Millisecond,
	}
}

// Beat identifies one heartbeat. Incarnation changes each time the node
// process starts, so Seq restarting from 1 is not mistaken for a replay.
type Beat struct {
	Incarnation uint64
	Seq         uint64
}

// Transition is a state change.
type Transition struct {
	Node iface.NodeID
	From State // 0 for a node seen for the first time
	To   State
	At   iface.Instant
	// Restarted is set when the beat came from a new incarnation.
	Restarted bool
}

type node struct {
	state       State
	lastSeen    iface.Instant
	incarnation uint64
	seq         uint64
	streak      int // consecutive on-time beats while suspect
}

// Detector tracks every node that has ever sent a heartbeat. Not safe for
// concurrent use; it lives on the metadata server's event loop.
type Detector struct {
	cfg      Config
	nodes    map[iface.NodeID]*node
	lastTick iface.Instant
	ticked   bool
	stalls   uint64
}

// New returns an empty detector.
func New(cfg Config) *Detector {
	return &Detector{cfg: cfg, nodes: map[iface.NodeID]*node{}}
}

// Config returns the thresholds.
func (d *Detector) Config() Config { return d.cfg }

// Observe records a heartbeat and returns the resulting transition, if any.
// Duplicated and reordered beats (same incarnation, seq not newer) are
// ignored: counting a duplicate toward recovery would let a flapping node
// look healthy.
func (d *Detector) Observe(id iface.NodeID, b Beat, now iface.Instant) (Transition, bool) {
	n := d.nodes[id]
	if n == nil {
		d.nodes[id] = &node{state: Alive, lastSeen: now, incarnation: b.Incarnation, seq: b.Seq}
		return Transition{Node: id, To: Alive, At: now}, true
	}
	restarted := b.Incarnation != n.incarnation
	if !restarted && b.Seq <= n.seq {
		return Transition{}, false
	}
	gap := now.Sub(n.lastSeen)
	n.lastSeen, n.incarnation, n.seq = now, b.Incarnation, b.Seq
	switch n.state {
	case Alive:
		if !restarted {
			return Transition{}, false
		}
		// A restart the detector never noticed still loses the node's
		// memory; it has to earn alive again.
		n.state, n.streak = Suspect, 1
		return Transition{Node: id, From: Alive, To: Suspect, At: now, Restarted: true}, true
	case Suspect:
		if gap <= d.cfg.OnTime && !restarted {
			n.streak++
		} else {
			n.streak = 1
		}
		if n.streak >= d.cfg.RecoverBeats {
			n.state = Alive
			return Transition{Node: id, From: Suspect, To: Alive, At: now}, true
		}
		if restarted {
			return Transition{Node: id, From: Suspect, To: Suspect, At: now, Restarted: true}, true
		}
		return Transition{}, false
	default: // Dead: back to suspect, then earn alive with on-time beats.
		n.state, n.streak = Suspect, 1
		return Transition{Node: id, From: Dead, To: Suspect, At: now, Restarted: restarted}, true
	}
}

// Tick applies silence timeouts and returns transitions sorted by node ID.
func (d *Detector) Tick(now iface.Instant) []Transition {
	if d.ticked {
		// Time the owner spent stalled is time it was not listening. Shift
		// every node's last beat forward by the excess so a pause of the
		// metadata server does not declare the whole cluster dead.
		if gap := now.Sub(d.lastTick); gap > 2*d.cfg.TickEvery {
			d.stalls++
			for _, n := range d.nodes {
				n.lastSeen = n.lastSeen.Add(gap - d.cfg.TickEvery)
				if n.lastSeen > now {
					n.lastSeen = now
				}
			}
		}
	}
	d.lastTick, d.ticked = now, true
	var out []Transition
	for _, id := range slices.Sorted(maps.Keys(d.nodes)) {
		n := d.nodes[id]
		silent := now.Sub(n.lastSeen)
		switch {
		case n.state != Dead && silent > d.cfg.DeadAfter:
			out = append(out, Transition{Node: id, From: n.state, To: Dead, At: now})
			n.state, n.streak = Dead, 0
		case n.state == Alive && silent > d.cfg.SuspectAfter:
			out = append(out, Transition{Node: id, From: Alive, To: Suspect, At: now})
			n.state, n.streak = Suspect, 0
		}
	}
	return out
}

// State returns a node's state, or 0 if it was never seen.
func (d *Detector) State(id iface.NodeID) State {
	if n := d.nodes[id]; n != nil {
		return n.state
	}
	return 0
}

// LastSeen returns when the node's latest beat arrived (shifted by stalls).
func (d *Detector) LastSeen(id iface.NodeID) (iface.Instant, bool) {
	n := d.nodes[id]
	if n == nil {
		return 0, false
	}
	return n.lastSeen, true
}

// Stalls counts ticks that detected an owner stall.
func (d *Detector) Stalls() uint64 { return d.stalls }

// Nodes returns every tracked node ID, sorted.
func (d *Detector) Nodes() []iface.NodeID {
	ids := slices.Collect(maps.Keys(d.nodes))
	slices.SortFunc(ids, cmp.Compare)
	return ids
}
