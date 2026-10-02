package cluster

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// TestECBenchmark is docs/benchmarks/ec.md. The same 20 files (1 B to
// 8 MiB, about 70 MiB) go to a 7-node cluster replicated, then to another
// erasure-coded. It records the bytes stored per logical byte, then kills
// node-3 and records the bytes repair read and wrote and the time from the
// kill to every chunk or stripe whole again.
func TestECBenchmark(t *testing.T) {
	for _, policy := range []client.Redundancy{client.Replicated, client.EC42} {
		name := "replicate:3"
		if policy == client.EC42 {
			name = "ec:4+2"
		}
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Nodes = ec.TotalShards + 1
			c := New(5, cfg, io.Discard)
			c.Tick(3 * time.Second)
			var logical int64
			for i := range 20 {
				size := 1 + int64(uint64(i+1)*2654435761%(8<<20))
				path := fmt.Sprintf("/bench/%02d", i)
				if _, _, err := c.Session().UploadAs(path, c.RandomData(path, size), policy); err != nil {
					t.Fatal(err)
				}
				logical += size
			}
			if _, ok := c.Settle(time.Minute); !ok {
				t.Fatalf("%d short before any fault", c.UnderReplicated())
			}
			// Uploads may leave a surplus copy until the next scan trims it.
			for range 600 {
				if c.OverReplicated() == 0 && len(c.Meta().Repair().InFlight()) == 0 {
					break
				}
				c.Tick(100 * time.Millisecond)
			}
			stored := int64(0)
			for _, n := range c.Nodes() {
				stored += c.BytesOn(n.ID())
			}
			overhead := float64(stored) / float64(logical)

			victim := iface.NodeID("node-3")
			lost := c.BytesOn(victim)
			before := c.Meta().Repair().Stats()
			start := c.Now()
			c.KillNode(victim)
			c.Tick(c.Config().Meta.Detector.DeadAfter)
			if _, ok := c.Settle(2 * c.RepairBound(4*lost)); !ok {
				t.Fatalf("%d short after killing %s", c.UnderReplicated(), victim)
			}
			took := c.Now().Sub(start)
			st := c.Meta().Repair().Stats()
			written := int64(st.Bytes - before.Bytes)
			read := written // a copy reads what it writes
			if policy == client.EC42 {
				read = int64(st.RebuildRead - before.RebuildRead)
			}
			t.Logf("%s: %.1f MiB logical, %.1f MiB stored (%.2f×); %s held %.1f MiB, repair read %.1f MiB and wrote %.1f MiB in %d copies (%d rebuilds), whole again %v after the kill",
				name, mib(logical), mib(stored), overhead, victim, mib(lost), mib(read), mib(written),
				st.Completed-before.Completed, st.Rebuilds-before.Rebuilds, took.Round(100*time.Millisecond))

			switch policy {
			case client.Replicated:
				if overhead < 2.99 || overhead > 3.01 || read != lost {
					t.Fatalf("replicated: %.2f× stored, read %d for %d lost", overhead, read, lost)
				}
			case client.EC42:
				if overhead < 1.5 || overhead > 1.51 || read != ec.DataShards*written || written != lost {
					t.Fatalf("EC: %.2f× stored, read %d and wrote %d for %d lost", overhead, read, written, lost)
				}
			}
			if err := c.AssertInvariants(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func mib(b int64) float64 { return float64(b) / (1 << 20) }
