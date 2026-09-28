package cluster

import (
	"encoding/json"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// run drives a cluster in fixed 50 ms steps, the same way the browser worker
// does, and returns the encoded state after every step.
func run(seed uint64, steps int) []string {
	c := New(seed, DefaultConfig(), io.Discard)
	out := make([]string, steps)
	for i := range out {
		c.Tick(50 * time.Millisecond)
		b, _ := json.Marshal(c.State())
		out[i] = string(b)
	}
	return out
}

func TestSameSeedSameStateSequence(t *testing.T) {
	a := run(42, 400)
	if !slices.Equal(a, run(42, 400)) {
		t.Fatal("seed 42 produced two different state sequences")
	}
	if slices.Equal(a, run(43, 400)) {
		t.Fatal("seeds 42 and 43 produced identical sequences")
	}
}

func TestHeartbeatsAdvance(t *testing.T) {
	c := New(1, DefaultConfig(), io.Discard)
	c.Tick(10 * time.Second)
	s := c.State()
	if len(s.Meta.Peers) != 3 {
		t.Fatalf("meta sees %d peers, want 3", len(s.Meta.Peers))
	}
	for _, p := range s.Meta.Peers {
		// 10 pings at 1 s intervals, 2% drop: 8 is a loose floor.
		if p.Pings < 8 || !p.Alive {
			t.Errorf("%s: pings=%d alive=%v, want >= 8 and alive", p.ID, p.Pings, p.Alive)
		}
	}
	for _, n := range s.Nodes {
		if n.Sent < 9 || n.Acked == 0 {
			t.Errorf("%s: sent=%d acked=%d", n.ID, n.Sent, n.Acked)
		}
	}
}

func TestPartitionedNodeGoesDeadAndRecovers(t *testing.T) {
	c := New(7, DefaultConfig(), io.Discard)
	c.Tick(5 * time.Second)
	c.Net().Partition([]iface.NodeID{"node-2"}, []iface.NodeID{metaID})

	alive := func() map[iface.NodeID]bool {
		m := map[iface.NodeID]bool{}
		for _, p := range c.State().Meta.Peers {
			m[p.ID] = p.Alive
		}
		return m
	}

	c.Tick(5 * time.Second) // > DeadAfter
	if a := alive(); a["node-2"] || !a["node-1"] || !a["node-3"] {
		t.Fatalf("after partition alive=%v, want only node-2 dead", a)
	}
	c.Net().Heal()
	c.Tick(3 * time.Second)
	if a := alive(); !a["node-2"] {
		t.Fatalf("after heal alive=%v, want node-2 alive", a)
	}
}
