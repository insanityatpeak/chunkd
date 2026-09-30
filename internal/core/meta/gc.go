package meta

import (
	"bytes"
	"maps"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// gcState is the leader's soft state for the sweep. It belongs to one term
// and starts empty when a peer becomes leader: at worst a grace period
// restarts, which only delays deletes. What is authorized (a GC delete that
// may be sent, and with which fence) is not here: it is replicated state,
// State.GCPending, so the next leader resends it unchanged.
type gcState struct {
	// orphanSince is when each located chunk was first seen unmarked.
	orphanSince map[iface.ChunkID]iface.Instant
	// sent is when this leader last sent each pending delete.
	sent map[iface.ChunkID]map[iface.NodeID]iface.Instant
	// acked are pending deletes the node has answered (deleted or kept),
	// waiting for a GCDone to be logged.
	acked map[iface.ChunkID]map[iface.NodeID]bool
	stats GCStats
}

// GCStats counts the sweep's work in this leader's term.
type GCStats struct {
	Orphans uint64 `json:"orphans"` // located chunks unmarked at the last sweep
	Sent    uint64 `json:"sent"`    // GC deletes sent, re-sends included
	Deleted uint64 `json:"deleted"` // copies nodes confirmed deleted
	Kept    uint64 `json:"kept"`    // deletes refused: the chunk was re-written
}

func newGCState() gcState {
	return gcState{orphanSince: map[iface.ChunkID]iface.Instant{}, sent: map[iface.ChunkID]map[iface.NodeID]iface.Instant{},
		acked: map[iface.ChunkID]map[iface.NodeID]bool{}}
}

// GC returns the sweep's counters.
func (s *Server) GC() GCStats { return s.gc.stats }

// gcPending: a GC delete of the copy of id on n is authorized and the node
// has not answered it, so the copy may vanish at any moment.
func (s *Server) gcPending(id iface.ChunkID, n iface.NodeID) bool {
	_, ok := s.state.GCPending(id, n)
	return ok && !s.gc.acked[id][n]
}

// collect is the sweep. The mark set is State.Marked, a pure function of
// the log: referenced by a committed or retained version, or claimed by a
// pending upload. A located copy is deleted only after its chunk has been
// unmarked for GCGrace, and only on the fence of this server's view of the
// node, so a copy an upload re-wrote after the decision is kept (ADR-0016).
//
// The decision is logged before anything is sent (ADR-0019): a GCIntent
// commits, and only then do the deletes go out. A deposed leader cannot
// commit, so it can never authorize a delete; and applying the intent re-checks
// the mark in log order, so a claim logged first protects the chunk.
func (s *Server) collect() {
	now := s.d.Clock.Now()
	s.flushGCDone()
	located := s.cluster.Located()
	still := make(map[iface.ChunkID]bool, len(located))
	var orphans uint64
	var targets []*chunkdv1.GCTarget
	for _, id := range located {
		still[id] = true
		if s.state.Marked(id) {
			delete(s.gc.orphanSince, id)
			continue
		}
		orphans++
		since, ok := s.gc.orphanSince[id]
		if !ok {
			s.gc.orphanSince[id] = now
			continue
		}
		if now.Sub(since) < s.cfg.GCGrace {
			continue
		}
		for _, n := range s.cluster.Locations(id) {
			if _, pending := s.state.GCPending(id, n); pending || !s.cluster.Alive(n) {
				continue
			}
			inc, seq, ok := s.cluster.Fence(n)
			if !ok {
				continue
			}
			targets = append(targets, &chunkdv1.GCTarget{ChunkId: id[:], Node: string(n), FenceIncarnation: inc, FenceSeq: seq})
		}
	}
	for id := range s.gc.orphanSince {
		if !still[id] {
			delete(s.gc.orphanSince, id)
		}
	}
	s.gc.stats.Orphans = orphans
	if len(targets) > 0 {
		s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_GcIntent{GcIntent: &chunkdv1.GCIntentOp{Targets: targets}}}, func(res Result, err error) {
			if err != nil {
				s.d.Log.Warn("gc intent", "err", err)
				return
			}
			if res.Intents > 0 {
				s.event("gc", "", "deleting %d unreferenced copies", res.Intents)
			}
			s.sendGC(targets)
		})
	}
	// A lost delete or a lost answer leaves a pending entry behind; send it
	// again, with its original fence. This is also how a new leader takes over
	// the previous one's deletes. The answer, delete or keep, ends it.
	for _, t := range s.state.GCPendingAll() {
		if s.gc.acked[t.Chunk][t.Node] || !s.cluster.Alive(t.Node) {
			continue
		}
		if at, sent := s.gc.sent[t.Chunk][t.Node]; sent && now.Sub(at) < s.cfg.EpochEvery {
			continue
		}
		s.sendGCDelete(t)
	}
}

// sendGC sends the deletes of every target the applied intent authorized.
func (s *Server) sendGC(targets []*chunkdv1.GCTarget) {
	for _, tg := range targets {
		id, err := wire.ChunkID(tg.GetChunkId())
		if err != nil {
			continue
		}
		t, ok := s.state.GCPending(id, iface.NodeID(tg.GetNode()))
		if !ok || t.Incarnation != tg.GetFenceIncarnation() || t.Seq != tg.GetFenceSeq() {
			continue // not authorized (marked at apply time), or authorized on an older fence
		}
		if _, sent := s.gc.sent[id][t.Node]; !sent {
			s.sendGCDelete(t)
		}
	}
}

func (s *Server) sendGCDelete(t GCTarget) {
	if s.gc.sent[t.Chunk] == nil {
		s.gc.sent[t.Chunk] = map[iface.NodeID]iface.Instant{}
	}
	s.gc.sent[t.Chunk][t.Node] = s.d.Clock.Now()
	s.gc.stats.Sent++
	s.d.Net.Send(t.Node, iface.Message{From: s.cfg.ID, Kind: wire.KindDeleteReplica,
		Body: wire.Marshal(&chunkdv1.DeleteReplica{ChunkId: t.Chunk[:], Gc: true, FenceIncarnation: t.Incarnation, FenceSeq: t.Seq, Term: s.raft.Status().Term})})
}

// gcAcked notes that a node answered a pending delete: the ordered block
// report says the copy was deleted or kept. Only the leader keeps this; the
// answer is logged in a batch (flushGCDone) so the delete stops being pending
// for every peer.
func (s *Server) gcAcked(id iface.ChunkID, n iface.NodeID, kept bool) {
	if !s.leading || !s.gcPending(id, n) {
		return
	}
	if s.gc.acked[id] == nil {
		s.gc.acked[id] = map[iface.NodeID]bool{}
	}
	s.gc.acked[id][n] = true
	if kept {
		s.gc.stats.Kept++
	} else {
		s.gc.stats.Deleted++
	}
}

// flushGCDone logs the answers received since the last flush. Until it
// applies the delete stays pending in the replicated state, and a new leader
// would resend it, which is harmless: an absent copy is confirmed absent.
func (s *Server) flushGCDone() {
	if len(s.gc.acked) == 0 {
		return
	}
	var targets []*chunkdv1.GCTarget
	batch := map[iface.ChunkID][]iface.NodeID{}
	for _, id := range slices.SortedFunc(maps.Keys(s.gc.acked), func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) }) {
		for _, n := range slices.Sorted(maps.Keys(s.gc.acked[id])) {
			targets = append(targets, &chunkdv1.GCTarget{ChunkId: id[:], Node: string(n)})
			batch[id] = append(batch[id], n)
		}
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_GcDone{GcDone: &chunkdv1.GCDoneOp{Targets: targets}}}, func(_ Result, err error) {
		if err != nil {
			s.d.Log.Warn("gc done", "err", err)
			return
		}
		for id, ns := range batch {
			for _, n := range ns {
				delete(s.gc.acked[id], n)
				delete(s.gc.sent[id], n)
			}
			if len(s.gc.acked[id]) == 0 {
				delete(s.gc.acked, id)
			}
			if len(s.gc.sent[id]) == 0 {
				delete(s.gc.sent, id)
			}
		}
	})
}
