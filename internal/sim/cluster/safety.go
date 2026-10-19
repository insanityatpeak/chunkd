package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// SafetyOpts tune WatchSafety. Zero fields take the defaults.
type SafetyOpts struct {
	// Every is the simulated time between probes (default 100 ms).
	Every time.Duration
	// PhantomGrace is how long the leader may count a copy that a running
	// node's disk lacks (default two full-report intervals, a heartbeat and
	// 5 s): a lost incremental report leaves one until the next full report,
	// and that report may be lost too.
	PhantomGrace time.Duration
}

// safety is the state of the per-step watcher.
type safety struct {
	opts SafetyOpts
	next iface.Instant
	// debt is each block's copies (a stripe's shards) lost to wipes and rot
	// since it was last at its target; the floor drops by that much.
	debt map[iface.ChunkID]int
	// since is when each phantom copy was first seen, under leader.
	since  map[copyKey]iface.Instant
	leader iface.NodeID
	// flagged blocks and copies are reported once.
	flagged map[copyKey]bool
	errs    []error
}

// WatchSafety checks two invariants every opts.Every of simulated time, after
// whichever event crosses that mark, for the rest of the run:
//
//  1. Copy floor: every block of a live file keeps min(target-1, commit
//     minimum) intact copies on disk (running or not), less those lost to
//     WipeNode or RotNode since it was last at target. A chunk's target is
//     Replicas copies, a stripe's 6 shards with a copy each. Deletes of any
//     kind (trim, GC, drain, balance) never excuse a drop.
//  2. No lasting phantom: while one leader is up and reachable, it never
//     counts a copy on a running, reachable node whose disk has none for
//     longer than PhantomGrace.
//
// Probes only read, so a watched run replays like an unwatched one. A
// violation fails AssertInvariants.
func (c *Cluster) WatchSafety(opts SafetyOpts) {
	if opts.Every <= 0 {
		opts.Every = 100 * time.Millisecond
	}
	if opts.PhantomGrace <= 0 {
		nc := node.DefaultConfig("", "", "")
		opts.PhantomGrace = 2*nc.FullReport + nc.Heartbeat + 5*time.Second
	}
	c.safety = &safety{opts: opts, debt: map[iface.ChunkID]int{}, since: map[copyKey]iface.Instant{}, flagged: map[copyKey]bool{}}
	c.clock.Observe(c.probe)
}

func (c *Cluster) probe() {
	s := c.safety
	now := c.clock.Now()
	if now < s.next {
		return
	}
	s.next = now.Add(s.opts.Every)
	c.checkFloor(now)
	c.checkPhantoms(now)
}

// lost records that node's copy of ch is gone to a wipe or rot.
func (c *Cluster) lost(ch iface.ChunkID) {
	if c.safety == nil {
		return
	}
	key := ch
	if ref, ok := c.Meta().State().ShardOf(ch); ok {
		key = ref.Stripe
	}
	c.safety.debt[key]++
}

// intactCopies counts the disks holding ch with bytes that hash to it.
func (c *Cluster) intactCopies(ch iface.ChunkID) int {
	n := 0
	for _, nd := range c.nodes {
		if _, ok := nd.Store.Holds(ch); ok {
			n++
		}
	}
	return n
}

func (c *Cluster) checkFloor(now iface.Instant) {
	s := c.safety
	st := c.Meta().State()
	seen := map[iface.ChunkID]bool{}
	for _, e := range st.List("/") {
		for _, id := range e.Chunks {
			if seen[id] {
				continue
			}
			seen[id] = true
			have, target, commit := c.intactCopies(id), c.cfg.Meta.Replicas, c.cfg.Meta.MinReplicas
			shards, stripe := st.Stripe(id)
			if stripe {
				have, target, commit = 0, ec.TotalShards, ec.CommitShards
				for _, sh := range shards {
					if c.intactCopies(sh) > 0 {
						have++
					}
				}
			}
			if have >= target {
				delete(s.debt, id)
				continue
			}
			floor := min(target-1, commit) - s.debt[id]
			if key := (copyKey{chunk: id}); have < floor && !s.flagged[key] {
				s.flagged[key] = true
				what := "intact copies on disk"
				if stripe {
					what = "shards with an intact copy on disk"
				}
				s.errs = append(s.errs, fmt.Errorf("at t=%v %s chunk %s has %d %s, floor %d (target %d, %d lost to wipes or rot)",
					now, e.Path, id.String()[:12], have, what, floor, target, s.debt[id]))
			}
		}
	}
}

func (c *Cluster) checkPhantoms(now iface.Instant) {
	s := c.safety
	leader := c.MetaLeader()
	if leader == "" || c.net.Frozen(leader) || c.MetaCut(leader) {
		leader = ""
	}
	if leader != s.leader {
		clear(s.since)
		s.leader = leader
	}
	if leader == "" {
		return
	}
	cl := c.MetaPeer(leader).Cluster()
	still := map[copyKey]bool{}
	for _, nd := range c.nodes {
		id := nd.ID()
		if c.net.Crashed(id) || c.net.Frozen(id) || c.nodeCut(id) {
			continue
		}
		for _, ch := range cl.ChunksOn(id) {
			if present, _ := nd.Store.Holds(ch); present {
				continue
			}
			key := copyKey{node: id, chunk: ch}
			still[key] = true
			at, ok := s.since[key]
			if !ok {
				s.since[key] = now
				continue
			}
			if now.Sub(at) > s.opts.PhantomGrace && !s.flagged[key] {
				s.flagged[key] = true
				s.errs = append(s.errs, fmt.Errorf("at t=%v leader %s has counted %s's copy of chunk %s for %v, but its disk has none",
					now, leader, id, ch.String()[:12], now.Sub(at)))
			}
		}
	}
	for key := range s.since {
		if !still[key] {
			delete(s.since, key)
		}
	}
}

// wiped records the copies a disk replacement destroys.
func (c *Cluster) wiped(store *sim.BlockStore) {
	if c.safety == nil {
		return
	}
	_ = store.List(context.Background(), func(ch iface.ChunkID) error {
		if _, ok := store.Holds(ch); ok {
			c.lost(ch)
		}
		return nil
	})
}
