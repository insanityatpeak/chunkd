// Package chaos runs randomized fault schedules against a cluster and checks
// global invariants afterwards. A scenario is a pure function of its seed:
// the sim runs it on the fake clock, and the real-mode runner drives the same
// schedule against processes.
package chaos

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// Kind names a fault action.
type Kind string

const (
	Kill    Kind = "kill"    // stop the process; the disk survives
	Restart Kind = "restart" // start a killed node over its disk
	Wipe    Kind = "wipe"    // replace a killed node's disk with an empty one
	Freeze  Kind = "freeze"  // pause the process (SIGSTOP, long GC)
	Thaw    Kind = "thaw"
	Slow    Kind = "slow"    // add Delay to every message to or from Node
	Fast    Kind = "fast"    // clear Slow
	Lossy   Kind = "lossy"   // raise message drop and duplicate rates cluster-wide
	Clean   Kind = "clean"   // restore the base network
	Corrupt Kind = "corrupt" // flip a byte in Count chunks on Node's disk (bit rot)

	// Metadata-group faults. The victim is the leader when the fault fires,
	// so the schedule stays a pure function of the seed. An end fault acts on
	// the peer its start fault hit, and does nothing if that one never fired.
	KillLeader   Kind = "kill-leader"   // stop the leader's process; the log survives
	ReviveLeader Kind = "revive-leader" // start the killed leader over its log
	FreezeLeader Kind = "freeze-leader" // pause the leader: it wakes as a stale leader
	ThawLeader   Kind = "thaw-leader"
	CutLeader    Kind = "cut-leader"  // partition {leader} from the other peers
	HealLeader   Kind = "heal-leader" // rejoin it

	// Membership faults (ADR-0020, ADR-0021), drawn from a stream of their
	// own (Shape.Admin) so the faults above keep their schedule.
	AddNode Kind = "add-node" // start node-(N+1), empty, on the next rack in turn
	Drain   Kind = "drain"    // drain Node through the metadata leader
	Undrain Kind = "undrain"  // return Node to service
	// KillLeaderMidMove waits, up to 15 s, for a balance copy in flight, then
	// kills the leader; a ReviveLeader ends it.
	KillLeaderMidMove Kind = "kill-leader-mid-rebalance"
)

// metaKind reports whether k acts on the metadata group.
func metaKind(k Kind) bool {
	switch k {
	case KillLeader, ReviveLeader, FreezeLeader, ThawLeader, CutLeader, HealLeader, KillLeaderMidMove:
		return true
	}
	return false
}

// Fault is one scheduled action.
type Fault struct {
	At    time.Duration
	Kind  Kind
	Node  iface.NodeID
	Delay time.Duration // Slow
	Drop  float64       // Lossy
	Dup   float64       // Lossy
	Count int           // Corrupt: chunks to rot
	Pick  uint64        // Corrupt: first chunk, as an index into the node's chunks in ID order
}

func (f Fault) String() string {
	s := fmt.Sprintf("%6.1fs %-7s", f.At.Seconds(), f.Kind)
	switch f.Kind {
	case Slow:
		s += fmt.Sprintf(" %s +%v", f.Node, f.Delay)
	case Lossy:
		s += fmt.Sprintf(" drop %.1f%% dup %.1f%%", f.Drop*100, f.Dup*100)
	case Clean, KillLeader, ReviveLeader, FreezeLeader, ThawLeader, CutLeader, HealLeader, AddNode, KillLeaderMidMove:
	case Corrupt:
		s += fmt.Sprintf(" %s ×%d from #%d", f.Node, f.Count, f.Pick%1000)
	default:
		s += " " + string(f.Node)
	}
	return s
}

// OpKind is a workload operation.
type OpKind string

const (
	Put    OpKind = "put"
	Get    OpKind = "get"
	Delete OpKind = "delete"
)

// Op is one client operation.
type Op struct {
	At   time.Duration
	Kind OpKind
	Path string
	Size int64 // Put
	// Minority sends the op from a client pinned to the peer a CutLeader
	// fault has partitioned off, when one is: the client on the small side.
	Minority bool
}

// Scenario is a fault schedule plus a workload over a fixed cluster shape.
type Scenario struct {
	Name   string // set for hand-written scenarios
	Seed   uint64
	Nodes  int
	Metas  int // metadata peers; 0 or 1 is a single server
	Length time.Duration
	Faults []Fault
	Ops    []Op
	// NoRepair asserts the faults are transient: zero repair copies.
	NoRepair bool
	// WantCorrupt is how many rotted copies the cluster must find (by
	// reads or the scrubber) before the run counts as settled.
	WantCorrupt int
}

func (s Scenario) String() string {
	var b strings.Builder
	if s.Name != "" {
		b.WriteString(s.Name + ", ")
	}
	fmt.Fprintf(&b, "seed %d: %d nodes, ", s.Seed, s.Nodes)
	if s.Metas > 1 {
		fmt.Fprintf(&b, "%d metas, ", s.Metas)
	}
	fmt.Fprintf(&b, "%v, %d ops\n", s.Length, len(s.Ops))
	for _, f := range s.Faults {
		b.WriteString("  " + f.String() + "\n")
	}
	return b.String()
}

// Shape bounds what Generate may produce.
type Shape struct {
	Nodes    int
	Metas    int // 3 adds metadata-group faults; below that the schedule is the node-only one
	Length   time.Duration
	Ops      int
	Paths    int
	MaxSize  int64
	Episodes int // fault episodes, each an impairment and its end
	// Admin is the most membership episodes (add a node, drain, drain and
	// kill, add and kill the leader mid-rebalance); 0 draws none.
	Admin int
}

// DefaultShape is a 5-node cluster, 2 simulated minutes, 40 operations on
// 12 paths of up to 512 KiB, up to 6 fault episodes and up to 2 membership
// episodes.
func DefaultShape() Shape {
	return Shape{Nodes: 5, Length: 2 * time.Minute, Ops: 40, Paths: 12, MaxSize: 512 << 10, Episodes: 6, Admin: 2}
}

// MetaShape is DefaultShape over a 3-peer metadata group, with room for the
// leader faults.
func MetaShape() Shape {
	sh := DefaultShape()
	sh.Metas, sh.Episodes = 3, 8
	return sh
}

// elections is the episode kind for repeated leader kills; it has no Fault
// kind of its own.
const elections Kind = "elections"

type episode struct {
	kind       Kind
	node       iface.NodeID
	start, end time.Duration
	downs      [][2]time.Duration // elections: each kill and its revive
}

// Generate builds a scenario from seed. Safety rules keep every invariant
// achievable, so a violation is a bug rather than an impossible schedule:
//
//   - at most 2 nodes killed or frozen at once (RF 3 leaves one copy);
//   - at most one wipe per scenario (commit at 2 of 3 survives one lost
//     disk; two lost disks can destroy an acknowledged chunk by design);
//   - one lossy window at a time;
//   - every impairment ends 10 s before the scenario does;
//   - with a metadata group, at most one peer is down, frozen or cut off at
//     once (a majority of 3 always stands), so leader faults never overlap;
//   - rot (applied by the runner) only hits a chunk that keeps 2 intact
//     copies on other nodes this scenario never wipes: rot on the last
//     copies is detected loudly by design (TestAllReplicasCorrupt), not
//     survivable.
func Generate(seed uint64, sh Shape) Scenario {
	rng := sim.NewRand(seed ^ 0x9e3779b97f4a7c15) // independent of the cluster's stream
	s := Scenario{Seed: seed, Nodes: sh.Nodes, Length: sh.Length}
	if sh.Metas > 1 {
		s.Metas = sh.Metas
	}
	span := func(lo, hi time.Duration) time.Duration { return lo + time.Duration(rng.IntN(int(hi-lo)+1)) }
	node := func() iface.NodeID { return iface.NodeID(fmt.Sprintf("node-%d", 1+rng.IntN(sh.Nodes))) }
	last := sh.Length - 10*time.Second

	var eps []episode
	overlapping := func(start, end time.Duration, match func(episode) bool) int {
		n := 0
		for _, e := range eps {
			if e.start < end && start < e.end && match(e) {
				n++
			}
		}
		return n
	}
	down := func(e episode) bool { return e.kind == Kill || e.kind == Freeze }
	wiped := false
	var cuts []episode
	for range 1 + rng.IntN(sh.Episodes) {
		start := span(5*time.Second, last-5*time.Second)
		var e episode
		kinds := 12 // shapes without a group draw exactly as before
		if s.Metas >= 3 {
			kinds = 18
		}
		switch k := rng.IntN(kinds); {
		case k < 4:
			e = episode{kind: Kill, node: node(), start: start, end: min(start+span(time.Second, time.Minute), last)}
		case k < 6:
			e = episode{kind: Freeze, node: node(), start: start, end: min(start+span(time.Second, 30*time.Second), last)}
		case k < 8:
			e = episode{kind: Slow, node: node(), start: start, end: min(start+span(5*time.Second, 40*time.Second), last)}
		case k < 10:
			e = episode{kind: Lossy, start: start, end: min(start+span(5*time.Second, 20*time.Second), last)}
		case k < 12:
			// Instantaneous; the scrubber or a reader finds it later.
			e = episode{kind: Corrupt, node: node(), start: start, end: start}
		case k < 14:
			e = episode{kind: KillLeader, start: start, end: min(start+span(3*time.Second, 30*time.Second), last)}
		case k < 15:
			e = episode{kind: FreezeLeader, start: start, end: min(start+span(2*time.Second, 20*time.Second), last)}
		case k < 17:
			e = episode{kind: CutLeader, start: start, end: min(start+span(5*time.Second, 30*time.Second), last)}
		default:
			// Repeated elections: 2-3 leader kills, each revived before the next.
			e = episode{kind: elections, start: start, end: start}
			for t, n := start, 2+rng.IntN(2); n > 0; n-- {
				down := span(2*time.Second, 5*time.Second)
				if t+down > last {
					break
				}
				e.downs = append(e.downs, [2]time.Duration{t, t + down})
				e.end = t + down
				t += down + span(8*time.Second, 14*time.Second)
			}
		}
		sameNode := func(o episode) bool { return e.node != "" && o.node == e.node }
		if overlapping(e.start, e.end, sameNode) > 0 {
			continue
		}
		if down(e) && overlapping(e.start, e.end, down) >= 2 {
			continue
		}
		isMeta := func(o episode) bool { return metaKind(o.kind) || o.kind == elections }
		if isMeta(e) && overlapping(e.start, e.end, isMeta) > 0 {
			continue
		}
		if e.kind == Lossy && overlapping(e.start, e.end, func(o episode) bool { return o.kind == Lossy }) > 0 {
			continue
		}
		eps = append(eps, e)
		switch e.kind {
		case Kill:
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: Kill, Node: e.node})
			if !wiped && rng.IntN(4) == 0 {
				wiped = true
				s.Faults = append(s.Faults, Fault{At: e.end, Kind: Wipe, Node: e.node})
			}
			s.Faults = append(s.Faults, Fault{At: e.end, Kind: Restart, Node: e.node})
		case Freeze:
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: Freeze, Node: e.node}, Fault{At: e.end, Kind: Thaw, Node: e.node})
		case Slow:
			d := span(200*time.Millisecond, 2*time.Second)
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: Slow, Node: e.node, Delay: d}, Fault{At: e.end, Kind: Fast, Node: e.node})
		case Lossy:
			drop, dup := float64(rng.IntN(51))/1000, float64(rng.IntN(51))/1000
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: Lossy, Drop: drop, Dup: dup}, Fault{At: e.end, Kind: Clean})
		case Corrupt:
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: Corrupt, Node: e.node, Count: 1 + rng.IntN(3), Pick: rng.Uint64()})
		case KillLeader:
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: KillLeader}, Fault{At: e.end, Kind: ReviveLeader})
		case FreezeLeader:
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: FreezeLeader}, Fault{At: e.end, Kind: ThawLeader})
		case CutLeader:
			s.Faults = append(s.Faults, Fault{At: e.start, Kind: CutLeader}, Fault{At: e.end, Kind: HealLeader})
			cuts = append(cuts, e)
		case elections:
			for _, d := range e.downs {
				s.Faults = append(s.Faults, Fault{At: d[0], Kind: KillLeader}, Fault{At: d[1], Kind: ReviveLeader})
			}
		}
	}
	s.Faults = append(s.Faults, admin(seed, sh, &eps, overlapping, down)...)
	// Stable: a wipe must stay before the restart scheduled at the same time.
	slices.SortStableFunc(s.Faults, func(a, b Fault) int { return cmp.Compare(a.At, b.At) })

	for range sh.Ops {
		op := Op{At: span(2*time.Second, sh.Length), Path: fmt.Sprintf("/c/%02d", rng.IntN(sh.Paths))}
		switch k := rng.IntN(20); {
		case k < 10:
			op.Kind, op.Size = Put, 1+int64(rng.IntN(int(sh.MaxSize)))
		case k < 17:
			op.Kind = Get
		default:
			op.Kind = Delete
		}
		s.Ops = append(s.Ops, op)
	}
	slices.SortStableFunc(s.Ops, func(a, b Op) int { return cmp.Compare(a.At, b.At) })
	// Half the ops issued while a leader is cut off come from the client on its side.
	for i, op := range s.Ops {
		if slices.ContainsFunc(cuts, func(e episode) bool { return op.At >= e.start && op.At < e.end }) {
			s.Ops[i].Minority = rng.IntN(2) == 0
		}
	}
	return s
}

// draining marks an admin episode's drain window, for the one-drain rule.
const draining Kind = "draining"

// admin draws up to sh.Admin membership episodes from a stream of its own,
// so a shape's other faults and its ops are what they were without them. It
// keeps Generate's rules, counting the episodes in eps:
//
//   - at most one node added per scenario (5 -> 6), and one drain at a time;
//   - every drain ends in an undrain 10 s before the end: a node left
//     draining keeps its copies on top of RF copies elsewhere;
//   - a drained node that is also killed (the drain target dies) counts
//     toward the 2-down limit and is never impaired by another episode;
//   - a leader kill mid-rebalance is a metadata fault: no other overlaps it.
func admin(seed uint64, sh Shape, eps *[]episode, overlapping func(time.Duration, time.Duration, func(episode) bool) int, down func(episode) bool) []Fault {
	if sh.Admin <= 0 {
		return nil
	}
	rng := sim.NewRand(seed ^ 0xad31_5eed_c0ff_ee11)
	span := func(lo, hi time.Duration) time.Duration { return lo + time.Duration(rng.IntN(int(hi-lo)+1)) }
	node := func() iface.NodeID { return iface.NodeID(fmt.Sprintf("node-%d", 1+rng.IntN(sh.Nodes))) }
	last := sh.Length - 10*time.Second
	isDrain := func(o episode) bool { return o.kind == draining }
	isMeta := func(o episode) bool { return metaKind(o.kind) || o.kind == elections }
	kinds := 3
	if sh.Metas >= 3 {
		kinds = 4
	}
	added := false
	var out []Fault
	for range rng.IntN(sh.Admin + 1) {
		start := span(5*time.Second, last-20*time.Second)
		switch rng.IntN(kinds) {
		case 0:
			if added {
				continue
			}
			added = true
			*eps = append(*eps, episode{kind: AddNode, start: start, end: start})
			out = append(out, Fault{At: start, Kind: AddNode})
		case 1:
			x, end := node(), start+span(5*time.Second, 40*time.Second)
			if end > last || overlapping(start, end, isDrain) > 0 {
				continue
			}
			*eps = append(*eps, episode{kind: draining, node: x, start: start, end: end})
			out = append(out, Fault{At: start, Kind: Drain, Node: x}, Fault{At: end, Kind: Undrain, Node: x})
		case 2:
			// The drain target dies mid-evacuation and comes back.
			x := node()
			kill := start + span(500*time.Millisecond, 3*time.Second)
			restart := kill + span(time.Second, 30*time.Second)
			end := restart + span(time.Second, 10*time.Second)
			k := episode{kind: Kill, node: x, start: kill, end: restart}
			sameNode := func(o episode) bool { return o.node == x }
			if end > last || overlapping(start, end, isDrain) > 0 || overlapping(start, end, sameNode) > 0 || overlapping(kill, restart, down) >= 2 {
				continue
			}
			*eps = append(*eps, episode{kind: draining, node: x, start: start, end: end}, k)
			out = append(out, Fault{At: start, Kind: Drain, Node: x}, Fault{At: kill, Kind: Kill, Node: x},
				Fault{At: restart, Kind: Restart, Node: x}, Fault{At: end, Kind: Undrain, Node: x})
		default:
			// A node joins and the leader dies while its moves are copying.
			end := start + 15*time.Second + span(3*time.Second, 12*time.Second)
			if added || end > last || overlapping(start, end, isMeta) > 0 {
				continue
			}
			added = true
			*eps = append(*eps, episode{kind: KillLeaderMidMove, start: start, end: end})
			out = append(out, Fault{At: start, Kind: AddNode}, Fault{At: start, Kind: KillLeaderMidMove}, Fault{At: end, Kind: ReviveLeader})
		}
	}
	return out
}
