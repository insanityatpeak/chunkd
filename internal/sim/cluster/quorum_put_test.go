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

// With one gray node (2 s added to every message) a put returns once two
// replicas stored each chunk and the grace period has passed, instead of
// waiting out the slow third (ADR-0029). The slow copy still lands, so the
// cluster ends with no chunk short of its replicas and nothing acknowledged
// is lost.
func TestPutNotHeldBySlowReplica(t *testing.T) {
	const putBound = 750 * time.Millisecond // 2 round trips of 2x40 ms, a chunk's transfer, the grace period, slack
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig()
		cfg.Faults.DropRate = 0
		c := New(seed, cfg, io.Discard)
		c.Tick(3 * time.Second)
		c.Net().SetSlow("node-1", 2*time.Second)
		caller := c.NewCaller("writer")
		put := func(grace time.Duration, p string) time.Duration {
			cl := client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep, PutGrace: grace})
			data := c.RandomData(p, 1<<20)
			t0 := c.Now()
			if _, err := cl.Put(context.Background(), p, bytes.NewReader(data), int64(len(data)), client.PutOptions{}); err != nil {
				t.Fatalf("seed %d: put %s: %v", seed, p, err)
			}
			return c.Now().Sub(t0)
		}
		var quorum, all []time.Duration
		for i := range 12 {
			quorum = append(quorum, put(0, fmt.Sprintf("/q/%02d", i)))
			all = append(all, put(-1, fmt.Sprintf("/a/%02d", i)))
		}
		slices.Sort(quorum)
		slices.Sort(all)
		if worst := quorum[len(quorum)-1]; worst > putBound {
			t.Fatalf("seed %d: slowest put %v > %v with one slow replica", seed, worst, putBound)
		}
		if worst := all[len(all)-1]; worst < 2*time.Second {
			t.Fatalf("seed %d: wait-for-all slowest put %v; the fault is not being exercised", seed, worst)
		}
		if _, ok := c.Settle(2 * time.Minute); !ok {
			t.Fatalf("seed %d: chunks still under-replicated after the slow copies should have landed", seed)
		}
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}
