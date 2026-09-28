package cluster

import (
	"encoding/json"
	"io"
	"slices"
	"testing"
	"time"
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

func TestNodesJoin(t *testing.T) {
	c := New(1, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	s := c.State()
	if len(s.Nodes) != 5 {
		t.Fatalf("%d nodes, want 5", len(s.Nodes))
	}
	for _, n := range s.Nodes {
		if !n.Alive || n.Heartbeats < 2 || n.Acks == 0 {
			t.Errorf("%s: %+v", n.ID, n)
		}
	}
}
