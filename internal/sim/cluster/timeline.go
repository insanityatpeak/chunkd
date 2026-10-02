package cluster

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// timeline is the dashboard's event log across metadata failovers. Each
// peer keeps its own ring with its own sequence, and the peer the dashboard
// reads (Meta) changes with the leader.
//   - Raft events come from every peer: an election plays out on peers the
//     dashboard is not reading yet. Every peer logs "term N: X leads", so
//     each of those is kept once.
//   - Other events follow Meta. On a switch the log takes the new peer's
//     events from the moment it stopped reading the old one.
//
// It is pulled after every Tick and before a peer is killed, never by the
// page, so a seed yields the same log at any playback speed.
type timeline struct {
	src      *meta.Server // a restarted peer is a new server, with a new sequence
	srcSeq   uint64       // last event pulled from src
	pulledAt iface.Instant
	raftSeq  map[*meta.Server]uint64 // last event each peer was scanned to for raft events
	seen     map[seenKey]bool        // raft events kept
	events   []client.Event          // newest last, at most meta.EventRing
	seq      uint64
}

func (c *Cluster) pullEvents() {
	tl := &c.timeline
	if tl.raftSeq == nil {
		tl.raftSeq, tl.seen = map[*meta.Server]uint64{}, map[seenKey]bool{}
	}
	var fresh []meta.Event
	keepRaft := func(e meta.Event) {
		k := seenKey{node: e.Node, text: e.Text}
		if !strings.HasPrefix(e.Text, "term ") {
			k.at = e.At // logged by one peer only
		}
		if !tl.seen[k] {
			tl.seen[k] = true
			fresh = append(fresh, e)
		}
	}
	for _, p := range c.metas {
		evs, latest := p.srv.Events(tl.raftSeq[p.srv])
		for _, e := range evs {
			if e.Kind == "raft" {
				keepRaft(e)
			}
		}
		tl.raftSeq[p.srv] = latest
	}

	srv := c.Meta()
	cutoff := iface.Instant(-1) // first pull: everything
	if srv != tl.src {
		if tl.src != nil {
			cutoff = tl.pulledAt
		}
		tl.src, tl.srcSeq = srv, 0
	}
	evs, latest := srv.Events(tl.srcSeq)
	for _, e := range evs {
		if e.Kind != "raft" && e.At > cutoff {
			fresh = append(fresh, e)
		}
	}
	tl.srcSeq, tl.pulledAt = latest, c.clock.Now()

	slices.SortStableFunc(fresh, func(a, b meta.Event) int { return cmp.Compare(a.At, b.At) })
	for _, e := range fresh {
		tl.seq++
		tl.events = append(tl.events, client.Event{Seq: tl.seq, AtMs: int64(e.At) / int64(time.Millisecond), Kind: e.Kind,
			Node: string(e.Node), Text: e.Text})
	}
	if n := len(tl.events) - meta.EventRing; n > 0 {
		tl.events = tl.events[n:]
	}
}

// eventsSince returns the merged events with Seq > after and the latest seq.
func (c *Cluster) eventsSince(after uint64) ([]client.Event, uint64) {
	tl := &c.timeline
	out := []client.Event{}
	for _, e := range tl.events {
		if e.Seq > after {
			out = append(out, e)
		}
	}
	return out, tl.seq
}

// seenKey identifies a raft event across peers: every peer logs the same
// "term N: …" text when it learns of a term or leader.
type seenKey struct {
	node iface.NodeID
	text string
	at   iface.Instant
}
