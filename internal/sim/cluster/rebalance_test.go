package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/rebalance"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// unbalanced returns the nodes outside their band of the rack-feasible
// target, computed as the balancer computes it from the located bytes of
// every live file's chunks.
func (c *Cluster) unbalanced(band int64) []string {
	var nodes []rebalance.Node
	for _, n := range c.Meta().Cluster().Nodes() {
		if c.Meta().Cluster().Alive(n.ID) && !n.Draining {
			nodes = append(nodes, rebalance.Node{ID: n.ID, Rack: n.Rack, Used: c.BytesOn(n.ID)})
		}
	}
	var distinct, largest int64
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, ch := range e.Chunks {
			if !seen[ch] {
				seen[ch] = true
				ci, _ := c.Meta().State().Chunk(ch)
				distinct += ci.Size
				largest = max(largest, ci.Size)
			}
		}
	}
	target := rebalance.Targets(nodes, distinct, c.cfg.Meta.Replicas)
	var out []string
	for _, n := range nodes {
		b := max(target[n.ID]*band/100, 2*largest)
		if d := n.Used - target[n.ID]; d > b || d < -b {
			out = append(out, fmt.Sprintf("%s used %d MiB, target %d MiB", n.ID, n.Used>>20, target[n.ID]>>20))
		}
	}
	return out
}

// A node that lost its disk comes back empty after repair restored RF on
// the others. The balancer refills it with moves, under the repair throttle,
// until every node is within the band, and no chunk drops below RF.
func TestWipedNodeIsRefilled(t *testing.T) {
	c := loaded(t, 3)
	c.KillNode("node-1")
	c.Tick(c.cfg.Meta.Detector.DeadAfter + c.cfg.Meta.Repair.Delay + time.Second)
	if _, ok := c.Settle(2 * c.RepairBound(c.BytesOn("node-2"))); !ok {
		t.Fatalf("RF not restored after node-1 died: %d short", c.UnderReplicated())
	}
	c.WipeNode("node-1")
	c.RestartNode("node-1")
	moved0 := c.Meta().Repair().Stats().MovedBytes
	for range 600 {
		c.Tick(500 * time.Millisecond)
		if n := c.UnderReplicated(); n != 0 {
			t.Fatalf("at %v: %d chunks below RF while balancing", c.Now(), n)
		}
		if c.BytesOn("node-1") > 0 && len(c.unbalanced(10)) == 0 && c.OverReplicated() == 0 {
			break
		}
	}
	if off := c.unbalanced(10); len(off) != 0 {
		t.Fatalf("not balanced after 5 min: %v", off)
	}
	moved := c.Meta().Repair().Stats().MovedBytes - moved0
	t.Logf("refilled node-1 with %d MiB of moves by %v", moved>>20, c.Now())
	if moved == 0 || c.BytesOn("node-1") == 0 {
		t.Fatal("nothing moved onto the returning node")
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}
