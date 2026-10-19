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
	"github.com/insanityatpeak/chunkd/internal/core/ec"
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

// The same for erasure-coded puts on 7 nodes: a put returns once 5 of 6
// shards are stored and the grace has passed (ADR-0031), and the slow
// node's shards still land.
func TestECPutNotHeldBySlowShard(t *testing.T) {
	const putBound = 750 * time.Millisecond
	for seed := uint64(1); seed <= 3; seed++ {
		c := slowECCluster(seed, 2*time.Second)
		put := ecPutter(t, c, seed)
		var quorum, all []time.Duration
		for i := range 12 {
			quorum = append(quorum, put(0, fmt.Sprintf("/q/%02d", i)))
			all = append(all, put(-1, fmt.Sprintf("/a/%02d", i)))
		}
		slices.Sort(quorum)
		slices.Sort(all)
		if worst := quorum[len(quorum)-1]; worst > putBound {
			t.Fatalf("seed %d: slowest EC put %v > %v with one slow node", seed, worst, putBound)
		}
		if worst := all[len(all)-1]; worst < 2*time.Second {
			t.Fatalf("seed %d: wait-for-all slowest EC put %v; the fault is not being exercised", seed, worst)
		}
		before := c.Meta().Repair().Stats().Rebuilds
		if _, ok := c.Settle(2 * time.Minute); !ok {
			t.Fatalf("seed %d: stripes still short after the slow shards should have landed", seed)
		}
		if n := c.Meta().Repair().Stats().Rebuilds - before; n != 0 {
			t.Fatalf("seed %d: %d rebuilds for shards that were only slow", seed, n)
		}
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

// A node that never answers holds no EC put: its shards are left pending,
// commit takes the other 5, and repair rebuilds the missing shard elsewhere
// once the node is declared dead.
func TestECPutShardNeverLands(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		c := slowECCluster(seed, time.Hour)
		put := ecPutter(t, c, seed)
		for i := range 8 {
			if d := put(0, fmt.Sprintf("/n/%02d", i)); d > 750*time.Millisecond {
				t.Fatalf("seed %d: EC put took %v with one node silent", seed, d)
			}
		}
		if _, ok := c.Settle(2 * time.Minute); !ok {
			t.Fatalf("seed %d: %d stripes short after repair", seed, c.UnderReplicated())
		}
		if c.Meta().Repair().Stats().Rebuilds == 0 {
			t.Fatalf("seed %d: no shard rebuilt; the fault is not being exercised", seed)
		}
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

// slowECCluster is 7 nodes with no message loss and node-1 adding d to
// every message.
func slowECCluster(seed uint64, d time.Duration) *Cluster {
	cfg := DefaultConfig()
	cfg.Nodes = ec.TotalShards + 1
	cfg.Faults.DropRate = 0
	c := New(seed, cfg, io.Discard)
	c.Tick(3 * time.Second)
	c.Net().SetSlow("node-1", d)
	return c
}

// ecPutter returns a put of one 4 MiB EC file with the given PutGrace,
// reporting how long it took.
func ecPutter(t *testing.T, c *Cluster, seed uint64) func(time.Duration, string) time.Duration {
	caller := c.NewCaller("writer")
	return func(grace time.Duration, p string) time.Duration {
		cl := client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep, PutGrace: grace})
		data := c.RandomData(p, 4<<20)
		t0 := c.Now()
		if _, err := cl.Put(context.Background(), p, bytes.NewReader(data), int64(len(data)), client.PutOptions{Redundancy: client.EC42}); err != nil {
			t.Fatalf("seed %d: put %s: %v", seed, p, err)
		}
		return c.Now().Sub(t0)
	}
}
