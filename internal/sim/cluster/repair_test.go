package cluster

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// loaded returns a cluster holding 50 files of 1 B to 8 MiB (about 200 MiB),
// every chunk at full replication.
func loaded(t *testing.T, seed uint64) *Cluster {
	t.Helper()
	c := New(seed, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	for i := range 50 {
		size := 1 + int64(uint64(i+1)*2654435761%(8<<20))
		if _, _, err := c.UploadRandom(fmt.Sprintf("/data/%02d", i), size); err != nil {
			t.Fatalf("seed %d: upload %d: %v", seed, i, err)
		}
	}
	// Uploads may commit at 2 of 3; repair tops those up at once.
	if _, ok := c.Settle(time.Minute); !ok {
		t.Fatalf("seed %d: %d chunks under-replicated before any fault", seed, c.UnderReplicated())
	}
	return c
}

func TestKillNodeRestoresRF(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		c := loaded(t, seed)
		victim := iface.NodeID("node-3")
		bytes := c.BytesOn(victim)
		bound := c.RepairBound(bytes)
		c.KillNode(victim)
		// Let the detector notice before measuring from here.
		c.Tick(c.Config().Meta.Detector.SuspectAfter + time.Second)
		took, ok := c.Settle(2 * bound)
		took += c.Config().Meta.Detector.SuspectAfter + time.Second
		if !ok || took > bound {
			t.Fatalf("seed %d: RF restored in %v (ok=%v), bound %v for %d MiB", seed, took, ok, bound, bytes>>20)
		}
		if err := c.AssertInvariants(); err != nil {
			t.Fatal(err)
		}
		st := c.Meta().Repair().Stats()
		t.Logf("seed %d: %d MiB on %s, RF 3 after %v (bound %v), %d copies", seed, bytes>>20, victim, took, bound, st.Completed)
	}
}

func TestTransientBlipNoRepair(t *testing.T) {
	for _, down := range []time.Duration{2 * time.Second, 15 * time.Second, 25 * time.Second} {
		t.Run(fmt.Sprint(down), func(t *testing.T) {
			c := loaded(t, 1)
			before := c.Meta().Repair().Stats().Dispatched
			c.KillNode("node-2")
			c.Tick(down)
			c.RestartNode("node-2")
			c.Tick(time.Minute)
			st := c.Meta().Repair().Stats()
			if st.Dispatched != before {
				t.Fatalf("node down %v (dead after 10 s, repair delay 20 s): %d repair copies", down, st.Dispatched-before)
			}
			if n := c.UnderReplicated(); n != 0 {
				t.Fatalf("%d chunks under-replicated after the node returned", n)
			}
			if err := c.AssertInvariants(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRepairThrottle(t *testing.T) {
	c := loaded(t, 2)
	cfg := c.Config().Meta.Repair
	bytes := c.BytesOn("node-1") + c.BytesOn("node-2")
	c.KillNode("node-1")
	c.KillNode("node-2")
	c.Tick(10 * time.Second)
	if _, ok := c.Settle(2 * c.RepairBound(bytes)); !ok {
		t.Fatalf("%d chunks still under-replicated", c.UnderReplicated())
	}
	st := c.Meta().Repair().Stats()
	if st.PeakInFlight > cfg.MaxInFlight || st.PeakPerSource > cfg.PerSource || st.PeakPerTarget > cfg.PerTarget {
		t.Fatalf("limits exceeded: %+v (limits %d total, %d per source, %d per target)", st, cfg.MaxInFlight, cfg.PerSource, cfg.PerTarget)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d copies, %d MiB, peaks %d/%d/%d", st.Completed, st.Bytes>>20, st.PeakInFlight, st.PeakPerSource, st.PeakPerTarget)
}
