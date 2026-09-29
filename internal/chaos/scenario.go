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
	Slow    Kind = "slow"  // add Delay to every message to or from Node
	Fast    Kind = "fast"  // clear Slow
	Lossy   Kind = "lossy" // raise message drop and duplicate rates cluster-wide
	Clean   Kind = "clean" // restore the base network
)

// Fault is one scheduled action.
type Fault struct {
	At    time.Duration
	Kind  Kind
	Node  iface.NodeID
	Delay time.Duration // Slow
	Drop  float64       // Lossy
	Dup   float64       // Lossy
}

func (f Fault) String() string {
	s := fmt.Sprintf("%6.1fs %-7s", f.At.Seconds(), f.Kind)
	switch f.Kind {
	case Slow:
		s += fmt.Sprintf(" %s +%v", f.Node, f.Delay)
	case Lossy:
		s += fmt.Sprintf(" drop %.1f%% dup %.1f%%", f.Drop*100, f.Dup*100)
	case Clean:
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
}

// Scenario is a fault schedule plus a workload over a fixed cluster shape.
type Scenario struct {
	Name   string // set for hand-written scenarios
	Seed   uint64
	Nodes  int
	Length time.Duration
	Faults []Fault
	Ops    []Op
	// NoRepair asserts the faults are transient: zero repair copies.
	NoRepair bool
}

func (s Scenario) String() string {
	var b strings.Builder
	if s.Name != "" {
		b.WriteString(s.Name + ", ")
	}
	fmt.Fprintf(&b, "seed %d: %d nodes, %v, %d ops\n", s.Seed, s.Nodes, s.Length, len(s.Ops))
	for _, f := range s.Faults {
		b.WriteString("  " + f.String() + "\n")
	}
	return b.String()
}

// Shape bounds what Generate may produce.
type Shape struct {
	Nodes    int
	Length   time.Duration
	Ops      int
	Paths    int
	MaxSize  int64
	Episodes int // fault episodes, each an impairment and its end
}

// DefaultShape is a 5-node cluster, 2 simulated minutes, 40 operations on
// 12 paths of up to 512 KiB, and up to 6 fault episodes.
func DefaultShape() Shape {
	return Shape{Nodes: 5, Length: 2 * time.Minute, Ops: 40, Paths: 12, MaxSize: 512 << 10, Episodes: 6}
}

type episode struct {
	kind       Kind
	node       iface.NodeID
	start, end time.Duration
}

// Generate builds a scenario from seed. Safety rules keep every invariant
// achievable, so a violation is a bug rather than an impossible schedule:
//
//   - at most 2 nodes killed or frozen at once (RF 3 leaves one copy);
//   - at most one wipe per scenario (commit at 2 of 3 survives one lost
//     disk; two lost disks can destroy an acknowledged chunk by design);
//   - one lossy window at a time;
//   - every impairment ends 10 s before the scenario does.
func Generate(seed uint64, sh Shape) Scenario {
	rng := sim.NewRand(seed ^ 0x9e3779b97f4a7c15) // independent of the cluster's stream
	s := Scenario{Seed: seed, Nodes: sh.Nodes, Length: sh.Length}
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
	for range 1 + rng.IntN(sh.Episodes) {
		start := span(5*time.Second, last-5*time.Second)
		var e episode
		switch k := rng.IntN(10); {
		case k < 4:
			e = episode{kind: Kill, node: node(), start: start, end: min(start+span(time.Second, time.Minute), last)}
		case k < 6:
			e = episode{kind: Freeze, node: node(), start: start, end: min(start+span(time.Second, 30*time.Second), last)}
		case k < 8:
			e = episode{kind: Slow, node: node(), start: start, end: min(start+span(5*time.Second, 40*time.Second), last)}
		default:
			e = episode{kind: Lossy, start: start, end: min(start+span(5*time.Second, 20*time.Second), last)}
		}
		sameNode := func(o episode) bool { return e.node != "" && o.node == e.node }
		if overlapping(e.start, e.end, sameNode) > 0 {
			continue
		}
		if down(e) && overlapping(e.start, e.end, down) >= 2 {
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
		}
	}
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
	return s
}
