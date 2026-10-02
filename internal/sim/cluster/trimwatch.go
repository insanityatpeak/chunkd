package cluster

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// nodeNet is the network as one storage node sees it. It hands every
// message to the node unchanged and, after a trim delete has run, checks
// the trim was safe (checkTrim).
type nodeNet struct {
	*sim.Net
	c *Cluster
}

func (t nodeNet) Listen(id iface.NodeID, h iface.Handler) {
	t.Net.Listen(id, func(m iface.Message) {
		var cmd chunkdv1.DeleteReplica
		if m.Kind != wire.KindDeleteReplica || wire.Decode(m.Body, &cmd) != nil || cmd.GetGc() {
			h(m)
			return
		}
		ch, err := wire.ChunkID(cmd.GetChunkId())
		if err != nil {
			h(m)
			return
		}
		store := t.c.node(id).Store
		_, before := store.Get(context.Background(), ch)
		h(m)
		if _, after := store.Get(context.Background(), ch); before == nil && after != nil {
			decided, ok := t.c.trimSent[trimKey{id, ch}]
			if !ok {
				decided = t.c.clock.Now()
			}
			t.c.checkTrim(id, ch, decided)
		}
	})
}

// trimGrace is how long after a node is killed or wiped the leader may
// still count its copies: the dead timeout plus two heartbeats of detector
// lag. A trim decided in that window may leave a chunk short, and the
// failure, not the trim, is to blame.
func (c *Cluster) trimGrace() time.Duration {
	return c.cfg.Meta.Detector.DeadAfter + 2*time.Second
}

// checkTrim is the trim-safety invariant (ADR-0020), checked the moment a
// trim removed node's copy of ch: a referenced chunk keeps at least RF
// intact copies on running nodes, a referenced shard at least one. Exempt
// are chunks with a rotted copy, on disk or since quarantined (the leader
// may have counted it before a read or the scrubber found it), and chunks
// one of whose holders was killed or wiped later than trimGrace before the
// trim was decided (sent). A violation fails AssertInvariants.
func (c *Cluster) checkTrim(node iface.NodeID, ch iface.ChunkID, decided iface.Instant) {
	want, ok := c.copyTarget(ch)
	if !ok {
		return
	}
	now := c.clock.Now()
	// A failure from trimGrace before the decision on is to blame, not the
	// trim: a frozen victim may carry the delete out long after it was sent.
	recent := func(at iface.Instant, ok bool) bool { return ok && decided.Sub(at) <= c.trimGrace() }
	intact := 0
	for _, n := range c.nodes {
		_, err := n.Store.Get(context.Background(), ch)
		if (err == nil && !c.Intact(n.ID(), ch)) || slices.Contains(n.Store.Quarantined(), ch) {
			return
		}
		killed, k := c.killedAt[n.ID()]
		wiped, w := c.wipedAt[n.ID()]
		if (err == nil && recent(killed, k)) || recent(wiped, w) {
			return
		}
		if n.ID() != node && !c.net.Crashed(n.ID()) && err == nil {
			intact++
		}
	}
	if intact < want {
		c.badTrims = append(c.badTrims, fmt.Errorf("a trim on %s at t=%v left chunk %s with %d intact copies on running nodes, want >= %d",
			node, now, ch.String()[:12], intact, want))
	}
}

// trimKey names a trim delete by its victim and chunk.
type trimKey struct {
	node  iface.NodeID
	chunk iface.ChunkID
}

// metaNet is the network as a metadata peer sees it: it records when each
// trim delete was sent, the moment the leader decided it was safe.
type metaNet struct {
	*sim.Net
	c *Cluster
}

func (t metaNet) Send(to iface.NodeID, m iface.Message) {
	var cmd chunkdv1.DeleteReplica
	if m.Kind == wire.KindDeleteReplica && wire.Decode(m.Body, &cmd) == nil && !cmd.GetGc() {
		if ch, err := wire.ChunkID(cmd.GetChunkId()); err == nil {
			t.c.trimSent[trimKey{to, ch}] = t.c.clock.Now()
		}
	}
	t.Net.Send(to, m)
}
