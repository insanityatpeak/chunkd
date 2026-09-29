package chaos

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

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
	}
	// Overwrites and deletes in the workload leave garbage; a run where GC
	// never deleted anything did not exercise the orphan check.
	if n >= 5 && deleted == 0 {
		t.Fatalf("%d seeds, no GC delete: the workload made no garbage", n)
	}
	t.Logf("%d seeds: GC deleted %d unreferenced copies", n, deleted)
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
