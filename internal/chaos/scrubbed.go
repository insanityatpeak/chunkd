package chaos

import (
	"context"
	"fmt"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim/cluster"
)

// scrubbed checks that rot nobody read is still found: after a full scrub
// pass on every running node, begun after replication settled, no running
// node's disk holds a corrupt chunk, and replication settles again.
func scrubbed(c *cluster.Cluster) []error {
	want := map[iface.NodeID]uint64{}
	for _, n := range c.Nodes() {
		// +2: the pass in progress may already have passed a rotten chunk.
		want[n.ID()] = n.Scrub().Passes + 2
	}
	limit := c.Now().Add(3*ScrubPass + time.Minute)
	for {
		done := true
		for _, n := range c.Nodes() {
			if !c.Net().Crashed(n.ID()) && n.Scrub().Passes < want[n.ID()] {
				done = false
			}
		}
		if done {
			break
		}
		if c.Now() > limit {
			return []error{fmt.Errorf("scrub passes did not complete within %v", 3*ScrubPass+time.Minute)}
		}
		c.Tick(time.Second)
	}
	errs := rotOnDisk(c)
	// Found rot is replaced like any lost copy: no delay, one copy each.
	if _, ok := c.Settle(time.Minute); !ok {
		errs = append(errs, fmt.Errorf("replication not restored a minute after the scrub: %d under, %d over", c.UnderReplicated(), c.OverReplicated()))
	}
	return errs
}

// rotOnDisk lists every corrupt chunk on a running node's disk.
func rotOnDisk(c *cluster.Cluster) []error {
	var errs []error
	for _, n := range c.Nodes() {
		if c.Net().Crashed(n.ID()) {
			continue
		}
		_ = n.Store.List(context.Background(), func(ch iface.ChunkID) error {
			if !c.Intact(n.ID(), ch) {
				errs = append(errs, fmt.Errorf("%s holds corrupt chunk %s after two full scrub passes", n.ID(), ch.String()[:12]))
			}
			return nil
		})
	}
	return errs
}
