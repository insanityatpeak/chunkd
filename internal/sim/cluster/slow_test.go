package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
)

// A gray node, alive to the detector but 2 s slow per message, is judged
// slow within seconds; puts then place nothing on it while its rack has
// fast nodes, and once it recovers it is judged fast again (ADR-0035).
func TestGrayNodeIsPlacedLast(t *testing.T) {
	for _, policy := range []client.Redundancy{client.Replicated, client.EC42} {
		name := "replicated"
		if policy != client.Replicated {
			name = string(policy)
		}
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Nodes = 7 // node-1 shares rack r1 with node-4 and node-7
			cfg.Faults.DropRate = 0
			c := New(5, cfg, io.Discard)
			c.Tick(3 * time.Second)
			slowNodes := func() []string {
				var out []string
				for _, n := range c.Meta().Cluster().Nodes() {
					if n.Slow {
						out = append(out, string(n.ID))
					}
				}
				return out
			}
			if got := slowNodes(); len(got) != 0 {
				t.Fatalf("healthy cluster: slow %v", got)
			}
			c.Net().SetSlow("node-1", 2*time.Second)
			start := c.Now()
			for !slices.Contains(slowNodes(), "node-1") {
				if c.Now().Sub(start) > 30*time.Second {
					t.Fatal("node-1 not judged slow after 30 s")
				}
				c.Tick(time.Second)
			}
			t.Logf("node-1 judged slow %v after the fault", c.Now().Sub(start))
			if got := slowNodes(); len(got) != 1 {
				t.Fatalf("slow %v, want only node-1", got)
			}
			for i := range 20 {
				p := fmt.Sprintf("/f%02d", i)
				data := c.RandomData(p, 256<<10)
				m, err := c.Client().Put(context.Background(), p, bytes.NewReader(data), int64(len(data)), client.PutOptions{Redundancy: policy})
				if err != nil {
					t.Fatal(err)
				}
				for _, ch := range m.Chunk {
					placed := slices.Clone(ch.Replicas)
					for _, s := range ch.Shards {
						placed = append(placed, s.Replicas...)
					}
					if slices.Contains(placed, "node-1") {
						t.Fatalf("%s chunk %d placed on the slow node: %v", p, ch.Index, placed)
					}
				}
			}
			c.Net().SetSlow("node-1", 0)
			start = c.Now()
			for slices.Contains(slowNodes(), "node-1") {
				if c.Now().Sub(start) > time.Minute {
					t.Fatal("node-1 still slow a minute after it recovered")
				}
				c.Tick(time.Second)
			}
			t.Logf("node-1 judged fast %v after it recovered", c.Now().Sub(start))
			if _, ok := c.Settle(time.Minute); !ok {
				t.Fatal("did not settle")
			}
			if err := c.AssertInvariants(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
