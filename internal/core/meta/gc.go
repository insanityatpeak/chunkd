package meta

import (
	"bytes"
	"maps"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// gcState is the sweep's soft state. None of it is durable: a restarted
// server starts every grace period again, which only delays deletes.
type gcState struct {
	// orphanSince is when each located chunk was first seen unmarked.
	orphanSince map[iface.ChunkID]iface.Instant
	// pending holds GC deletes sent and not yet answered, with the fence
	// they carried. Re-sends reuse that fence, never a newer one: a newer
	// fence would allow deleting a copy re-written after the first send.
	pending map[iface.ChunkID]map[iface.NodeID]gcDelete
	stats   GCStats
}

type gcDelete struct {
	inc, seq uint64
	sent     iface.Instant
}

// GCStats counts the sweep's work since the server started.
type GCStats struct {
	Orphans uint64 `json:"orphans"` // located chunks unmarked at the last sweep
	Sent    uint64 `json:"sent"`    // GC deletes sent, re-sends included
	Deleted uint64 `json:"deleted"` // copies nodes confirmed deleted
	Kept    uint64 `json:"kept"`    // deletes refused: the chunk was re-written
}

func newGCState() gcState {
	return gcState{orphanSince: map[iface.ChunkID]iface.Instant{}, pending: map[iface.ChunkID]map[iface.NodeID]gcDelete{}}
}

// GC returns the sweep's counters.
func (s *Server) GC() GCStats { return s.gc.stats }

func (s *Server) gcPending(id iface.ChunkID, n iface.NodeID) bool {
	_, ok := s.gc.pending[id][n]
	return ok
}

// collect is the sweep. The mark set is State.Marked, a pure function of
// the log: referenced by a committed or retained version, or claimed by a
// pending upload. A located copy is deleted only after its chunk has been
// unmarked for GCGrace, and only on the fence of this server's view of the
// node, so a copy an upload re-wrote after the decision is kept (ADR-0016).
func (s *Server) collect() {
	now := s.d.Clock.Now()
	located := s.cluster.Located()
	still := make(map[iface.ChunkID]bool, len(located))
	var orphans, sent uint64
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
			if s.gcPending(id, n) || !s.cluster.Alive(n) {
				continue
			}
			inc, seq, ok := s.cluster.Fence(n)
			if !ok {
				continue
			}
			s.sendGCDelete(id, n, gcDelete{inc: inc, seq: seq, sent: now})
			sent++
		}
	}
	for id := range s.gc.orphanSince {
		if !still[id] {
			delete(s.gc.orphanSince, id)
		}
	}
	// A lost delete or a lost answer leaves an entry behind; send it again
	// with its original fence. The answer, delete or keep, clears it.
	for _, id := range slices.SortedFunc(maps.Keys(s.gc.pending), func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) }) {
		for _, n := range slices.Sorted(maps.Keys(s.gc.pending[id])) {
			if d := s.gc.pending[id][n]; now.Sub(d.sent) >= s.cfg.EpochEvery && s.cluster.Alive(n) {
				d.sent = now
				s.sendGCDelete(id, n, d)
			}
		}
	}
	s.gc.stats.Orphans = orphans
	if sent > 0 {
		s.event("gc", "", "deleting %d unreferenced copies", sent)
	}
}

func (s *Server) sendGCDelete(id iface.ChunkID, n iface.NodeID, d gcDelete) {
	if s.gc.pending[id] == nil {
		s.gc.pending[id] = map[iface.NodeID]gcDelete{}
	}
	s.gc.pending[id][n] = d
	s.gc.stats.Sent++
	s.d.Net.Send(n, iface.Message{From: s.cfg.ID, Kind: wire.KindDeleteReplica,
		Body: wire.Marshal(&chunkdv1.DeleteReplica{ChunkId: id[:], Gc: true, FenceIncarnation: d.inc, FenceSeq: d.seq})})
}

// gcAcked clears a pending delete once the node's ordered report says the
// copy was deleted or kept.
func (s *Server) gcAcked(id iface.ChunkID, n iface.NodeID, kept bool) {
	if !s.gcPending(id, n) {
		return
	}
	delete(s.gc.pending[id], n)
	if len(s.gc.pending[id]) == 0 {
		delete(s.gc.pending, id)
	}
	if kept {
		s.gc.stats.Kept++
	} else {
		s.gc.stats.Deleted++
	}
}
