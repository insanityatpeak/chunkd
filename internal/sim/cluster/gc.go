package cluster

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// GCSettle is how long, after the last delete, overwrite or abandoned
// upload, every unreferenced copy is gone: retention, then the grace
// period, then two more sweeps (one to see the orphan, one for a re-send
// after a lost delete or answer), plus the lease for an abandoned upload.
func (c *Cluster) GCSettle() time.Duration {
	m := c.cfg.Meta
	retain := time.Duration(max(m.RetainEpochs, m.LeaseEpochs)+1) * m.EpochEvery
	return retain + m.GCGrace + 2*m.EpochEvery + 5*time.Second
}

// Stored lists every chunk copy on every node's disk, quarantined copies
// excluded.
func (c *Cluster) Stored() map[iface.ChunkID][]iface.NodeID {
	out := map[iface.ChunkID][]iface.NodeID{}
	for _, n := range c.nodes {
		n.Store.List(context.Background(), func(id iface.ChunkID) error {
			out[id] = append(out[id], n.ID())
			return nil
		})
	}
	return out
}

// AssertCollected checks the GC invariants once GCSettle has passed with
// no new garbage:
//
//  1. No orphan: every copy on disk belongs to a marked chunk (referenced
//     by a retained version or claimed by a pending upload).
//  2. Refcounts and claim counts equal a recount from first principles.
func (c *Cluster) AssertCollected() error {
	st := c.Meta().State()
	var errs []error
	orphans := 0
	for id, nodes := range c.Stored() {
		if !st.Marked(id) {
			orphans++
			if orphans <= 5 {
				errs = append(errs, fmt.Errorf("orphan chunk %s survives GC on %v", hex.EncodeToString(id[:6]), nodes))
			}
		}
	}
	if orphans > 5 {
		errs = append(errs, fmt.Errorf("%d orphan chunks in all", orphans))
	}
	if d := st.Reconcile(); !d.Empty() {
		errs = append(errs, fmt.Errorf("refcount drift on %d chunks, claim drift on %d", len(d.Refcounts), len(d.Claims)))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("seed %d: %w", c.seed, err)
	}
	return nil
}
