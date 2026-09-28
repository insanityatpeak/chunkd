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
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// hedgedReadBound is the p99 Get latency we claim with one bad node: the
// hedge ceiling (500 ms) plus a metadata round trip and one chunk read on
// this network (2×40 ms each, plus 8 ms to move 1 MiB), with slack.
const hedgedReadBound = 750 * time.Millisecond

// readP99 uploads 30 one-chunk files, applies fault to node-1, then reads
// every file 3 times and returns the p99 Get latency and how many reads
// hedged.
func readP99(t *testing.T, seed uint64, noHedge bool, fault func(c *Cluster)) (time.Duration, int) {
	t.Helper()
	cfg := DefaultConfig()
	// No loss: a lost metadata call costs a full call timeout, which would
	// measure retries instead of hedging.
	cfg.Faults.DropRate = 0
	c := New(seed, cfg, io.Discard)
	c.Tick(3 * time.Second)
	var paths []string
	for i := range 30 {
		p := fmt.Sprintf("/f%02d", i)
		if _, _, err := c.UploadRandom(p, 1<<20); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	caller := c.NewCaller("reader")
	cl := client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep, NoHedge: noHedge})
	fault(c)

	var lat []time.Duration
	hedged := 0
	for range 3 {
		for _, p := range paths {
			start := c.Now()
			var buf bytes.Buffer
			m, err := cl.Get(context.Background(), p, &buf)
			if err != nil {
				t.Fatalf("seed %d: get %s: %v", seed, p, err)
			}
			lat = append(lat, c.Now().Sub(start))
			for _, ch := range m.Chunk {
				if ch.Hedged {
					hedged++
				}
			}
		}
	}
	slices.Sort(lat)
	return lat[len(lat)*99/100], hedged
}

func TestSlowNodeHedgedRead(t *testing.T) {
	faults := []struct {
		name  string
		fault func(c *Cluster)
	}{
		// Frozen: no answer at all. The detector notices after 3 s and
		// orders the node last; hedging covers the reads before that.
		{"frozen", func(c *Cluster) { c.Net().Freeze("node-1") }},
		// Gray: 2 s added to every message, heartbeats included. Beats stay
		// regular, so the detector keeps the node alive; only the client's
		// health score and hedging protect the reader.
		{"gray", func(c *Cluster) { c.Net().SetSlow("node-1", 2*time.Second) }},
	}
	for _, f := range faults {
		t.Run(f.name, func(t *testing.T) {
			for seed := uint64(1); seed <= 3; seed++ {
				p99, hedged := readP99(t, seed, false, f.fault)
				if p99 > hedgedReadBound {
					t.Fatalf("seed %d: hedged p99 %v > %v", seed, p99, hedgedReadBound)
				}
				if hedged == 0 {
					t.Fatalf("seed %d: no read hedged; node-1 never first choice?", seed)
				}
				base, _ := readP99(t, seed, true, f.fault)
				if base < 2*time.Second {
					t.Fatalf("seed %d: unhedged p99 %v; the fault is not being exercised", seed, base)
				}
				t.Logf("seed %d: p99 hedged %v (%d hedges), unhedged %v", seed, p99, hedged, base)
			}
		})
	}
}

func TestGrayNodeStaysAlive(t *testing.T) {
	c := New(1, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	c.Net().SetSlow("node-1", 2*time.Second)
	c.Tick(30 * time.Second)
	if !c.Meta().Cluster().Alive(iface.NodeID("node-1")) {
		t.Fatal("slow node declared not alive; gray failure should be invisible to heartbeats")
	}
}
