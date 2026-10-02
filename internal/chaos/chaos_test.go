package chaos

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim/cluster"
)

var seeds = flag.Int("seeds", 20, "random chaos seeds to run")

func TestChaosSeeds(t *testing.T) {
	n := *seeds
	if testing.Short() {
		n = min(n, 5)
	}
	var deleted uint64
	for seed := uint64(1); seed <= uint64(n); seed++ {
		r := Run(Generate(seed, DefaultShape()), io.Discard)
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		deleted += r.GC.Deleted
		if r.History == 0 {
			t.Fatalf("seed %d: no history was checked", seed)
		}
	}
	// Overwrites and deletes in the workload leave garbage; a run where GC
	// never deleted anything did not exercise the orphan check.
	if n >= 5 && deleted == 0 {
		t.Fatalf("%d seeds, no GC delete: the workload made no garbage", n)
	}
	t.Logf("%d seeds: GC deleted %d unreferenced copies", n, deleted)
}

// Over a 3-peer metadata group, with leader kills, freezes, partitions (a
// client on each side) and repeated elections. CI runs 500 of these through
// the chaos command.
func TestChaosMetaSeeds(t *testing.T) {
	n := min(*seeds, 10)
	if testing.Short() {
		n = min(n, 3)
	}
	faults := 0
	for seed := uint64(1); seed <= uint64(n); seed++ {
		r := Run(Generate(seed, MetaShape()), io.Discard)
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		faults += r.LeaderFaults
	}
	if faults == 0 {
		t.Fatalf("%d seeds hit no leader", n)
	}
}

// Seed 260 ends with the 30 s epoch proposal committing at the very instant
// of the agreement check: one follower has not yet heard the new commit
// index. Peers must be compared at one applied index, not one instant.
func TestChaosMetaAgreeAcrossEpochTick(t *testing.T) {
	if r := Run(Generate(260, MetaShape()), io.Discard); r.Err != nil {
		t.Fatal(r.Err)
	}
}

func TestSameSeedSameTraceMetaGroup(t *testing.T) {
	a := Run(Generate(12, MetaShape()), io.Discard)
	b := Run(Generate(12, MetaShape()), io.Discard)
	if a.Trace != b.Trace || a.LeaderFaults != b.LeaderFaults || a.LeaderFaults == 0 {
		t.Fatalf("seed 12 replayed differently: %s/%d vs %s/%d", a.Trace, a.LeaderFaults, b.Trace, b.LeaderFaults)
	}
	if !strings.Contains(Replay(Generate(12, MetaShape())), "--metas=3") {
		t.Fatal("the replay command drops --metas")
	}
}

func TestSameSeedSameTrace(t *testing.T) {
	for _, seed := range []uint64{3, 17} {
		a := Run(Generate(seed, DefaultShape()), io.Discard)
		b := Run(Generate(seed, DefaultShape()), io.Discard)
		if a.Trace != b.Trace || a.Repair != b.Repair {
			t.Fatalf("seed %d replayed differently: %s vs %s", seed, a.Trace, b.Trace)
		}
	}
	if Run(Generate(3, DefaultShape()), io.Discard).Trace == Run(Generate(4, DefaultShape()), io.Discard).Trace {
		t.Fatal("seeds 3 and 4 produced the same trace")
	}
}

func TestGenerateRespectsSafetyRules(t *testing.T) {
	for seed := uint64(0); seed < 2000; seed++ {
		s := Generate(seed, DefaultShape())
		down := map[string]bool{}
		wipes := 0
		for _, f := range s.Faults {
			switch f.Kind {
			case Kill, Freeze:
				down[string(f.Node)] = true
			case Restart, Thaw:
				delete(down, string(f.Node))
			case Wipe:
				wipes++
			}
			if len(down) > 2 {
				t.Fatalf("seed %d: %d nodes down at %v\n%s", seed, len(down), f.At, s)
			}
			if f.At > s.Length-10*time.Second {
				t.Fatalf("seed %d: fault at %v, after the heal deadline", seed, f.At)
			}
		}
		if len(down) != 0 || wipes > 1 {
			t.Fatalf("seed %d: unhealed %v, %d wipes\n%s", seed, down, wipes, s)
		}
	}
}

// With a metadata group: at most one peer impaired at a time, every leader
// fault ended 10 s before the end, and each new kind drawn by some seed.
func TestGenerateMetaSafetyRules(t *testing.T) {
	ends := map[Kind]Kind{KillLeader: ReviveLeader, KillLeaderMidMove: ReviveLeader, FreezeLeader: ThawLeader, CutLeader: HealLeader}
	seen := map[Kind]int{}
	minority := 0
	for seed := uint64(0); seed < 2000; seed++ {
		s := Generate(seed, MetaShape())
		if s.Metas != 3 {
			t.Fatalf("seed %d: %d metas", seed, s.Metas)
		}
		var open Kind
		kills := 0
		for _, f := range s.Faults {
			seen[f.Kind]++
			if !metaKind(f.Kind) {
				continue
			}
			if f.At > s.Length-10*time.Second {
				t.Fatalf("seed %d: %v after the heal deadline\n%s", seed, f, s)
			}
			if end, start := ends[f.Kind]; start {
				if open != "" {
					t.Fatalf("seed %d: %v while %s is open\n%s", seed, f, open, s)
				}
				open = end
				if f.Kind == KillLeader {
					kills++
				}
				continue
			}
			if f.Kind != open {
				t.Fatalf("seed %d: %v closes %q\n%s", seed, f, open, s)
			}
			open = ""
		}
		if open != "" {
			t.Fatalf("seed %d: %s never came\n%s", seed, open, s)
		}
		if kills >= 2 {
			seen[elections]++
		}
		for _, op := range s.Ops {
			if op.Minority {
				minority++
			}
		}
	}
	for _, k := range []Kind{KillLeader, FreezeLeader, CutLeader, elections} {
		if seen[k] == 0 {
			t.Errorf("no seed drew %s", k)
		}
	}
	if minority == 0 {
		t.Error("no op ran on the minority side")
	}
}

// Shapes without a metadata group must generate what they did before the
// group's faults existed: those seeds' traces are pinned elsewhere.
func TestGenerateWithoutGroupUnchanged(t *testing.T) {
	for seed := uint64(0); seed < 500; seed++ {
		sh := DefaultShape()
		a := Generate(seed, sh)
		sh.Metas = 1
		b := Generate(seed, sh)
		if a.String() != b.String() || a.Metas != 0 || b.Metas != 0 {
			t.Fatalf("seed %d: Metas 1 changed the schedule\n%s\n%s", seed, a, b)
		}
		for _, f := range a.Faults {
			if metaKind(f.Kind) {
				t.Fatalf("seed %d: leader fault without a group: %v", seed, f)
			}
		}
	}
}

// The checker must not be vacuous: three disks lost at once destroys every
// chunk they held, and a run that does this has to fail.
func TestCheckerCatchesLoss(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 5, Length: time.Minute}
	for i := range 6 {
		s.Ops = append(s.Ops, Op{At: time.Second, Kind: Put, Path: fmt.Sprintf("/x/%d", i), Size: 200 << 10})
	}
	for _, n := range []iface.NodeID{"node-1", "node-2", "node-3"} {
		s.Faults = append(s.Faults, Fault{At: 20 * time.Second, Kind: Kill, Node: n})
	}
	for _, n := range []iface.NodeID{"node-1", "node-2", "node-3"} {
		s.Faults = append(s.Faults, Fault{At: 21 * time.Second, Kind: Wipe, Node: n}, Fault{At: 21 * time.Second, Kind: Restart, Node: n})
	}
	r := Run(s, io.Discard)
	if r.Err == nil {
		t.Fatal("three wiped disks reported no violation")
	}
	if !strings.Contains(r.Err.Error(), "replay: go run ./tools/task chaos --seed=1") {
		t.Fatalf("failure does not print the replay command:\n%v", r.Err)
	}
}

// The rot checks must not be vacuous: rot is visible on disk until found,
// and a run with rot finds and replaces all of it.
func TestCheckerSeesRot(t *testing.T) {
	c := cluster.New(3, Config(5), io.Discard)
	c.Tick(3 * time.Second)
	if _, _, err := c.UploadRandom("/r", 600<<10); err != nil {
		t.Fatal(err)
	}
	c.Settle(time.Minute)
	if errs := rotOnDisk(c); len(errs) != 0 {
		t.Fatalf("clean cluster: %v", errs)
	}
	rotted := c.RotNode("node-1", 5, 0, func(iface.ChunkID) bool { return true })
	if len(rotted) == 0 {
		t.Fatal("node-1 holds nothing to rot")
	}
	if errs := rotOnDisk(c); len(errs) != len(rotted) {
		t.Fatalf("checker saw %d of %d rotted chunks", len(errs), len(rotted))
	}
	if errs := scrubbed(c); len(errs) != 0 {
		t.Fatalf("after scrubbing: %v", errs)
	}
	if n := c.Nodes()[0].Stats().Corrupt; n != uint64(len(rotted)) {
		t.Fatalf("node-1 quarantined %d of %d", n, len(rotted))
	}
}

// Bugs-found #17: after a metadata failover the new leader's counters start
// below the old one's; last minus first underflowed to 2^64 - 60.
func TestLeaderDeltasAcrossFailover(t *testing.T) {
	h := func(repaired, found uint64) client.Health {
		return client.Health{RepairCompleted: repaired, CorruptReplicas: found}
	}
	d := leaderDeltas{prev: h(50, 2), leader: "meta-1"}
	for _, p := range []struct {
		leader string
		h      client.Health
	}{
		{"meta-1", h(53, 3)}, // +3, +1
		{"", h(0, 0)},        // election: nothing
		{"meta-2", h(0, 0)},  // new process
		{"meta-2", h(4, 1)},  // +4, +1
		{"meta-2", h(4, 1)},
	} {
		d.add(p.leader, p.h)
	}
	if d.repaired != 7 || d.found != 2 {
		t.Fatalf("repaired %d, found %d; want 7 and 2", d.repaired, d.found)
	}
}

// Membership episodes come from a stream of their own: with them on, every
// other fault and every op is what it was with them off, in the same order.
func TestAdminKeepsBaseSchedule(t *testing.T) {
	for _, base := range []Shape{DefaultShape(), MetaShape()} {
		for seed := uint64(0); seed < 1000; seed++ {
			off := base
			off.Admin = 0
			a, b := Generate(seed, off), Generate(seed, base)
			if fmt.Sprint(a.Ops) != fmt.Sprint(b.Ops) {
				t.Fatalf("seed %d: membership faults changed the ops", seed)
			}
			i := 0
			for _, f := range b.Faults {
				if i < len(a.Faults) && f == a.Faults[i] {
					i++
				}
			}
			if i != len(a.Faults) {
				t.Fatalf("seed %d: the base faults are not a subsequence of the faults with membership on\n%s\n%s", seed, a, b)
			}
		}
	}
}

// Every drain is undone on the same node before the heal deadline, one
// drain at a time, at most one node is added, and each episode kind is drawn
// by some seed.
func TestGenerateAdminRules(t *testing.T) {
	seen := map[string]int{}
	for _, sh := range []Shape{DefaultShape(), MetaShape()} {
		for seed := uint64(0); seed < 2000; seed++ {
			s := Generate(seed, sh)
			var drained iface.NodeID
			adds := 0
			for _, f := range s.Faults {
				switch f.Kind {
				case AddNode:
					adds++
					seen["add-node"]++
				case KillLeaderMidMove:
					seen[string(f.Kind)]++
				case Drain:
					if drained != "" {
						t.Fatalf("seed %d: drain %s while %s drains\n%s", seed, f.Node, drained, s)
					}
					drained = f.Node
					seen["drain"]++
				case Undrain:
					if f.Node != drained {
						t.Fatalf("seed %d: undrain %s, draining %q\n%s", seed, f.Node, drained, s)
					}
					drained = ""
				case Kill:
					if f.Node == drained {
						seen["kill-drain-target"]++
					}
				}
			}
			if drained != "" || adds > 1 {
				t.Fatalf("seed %d: %q left draining, %d nodes added\n%s", seed, drained, adds, s)
			}
		}
	}
	for _, k := range []string{"add-node", "drain", "kill-drain-target", string(KillLeaderMidMove)} {
		if seen[k] == 0 {
			t.Errorf("no seed drew %s", k)
		}
	}
}
