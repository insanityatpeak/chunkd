package meta

import (
	"bytes"
	"maps"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// trimState is the leader's soft state for trims; it starts empty on every
// term. What is authorized lives in State.TrimPending, so a new leader
// resends it unchanged.
type trimState struct {
	// sent is when this leader last sent each pending trim.
	sent map[iface.ChunkID]iface.Instant
	// ids are the scheduler's trim IDs, for the timeline.
	ids map[iface.ChunkID]uint64
	// acked are pending trims whose copy is gone, waiting for a TrimDone.
	acked map[iface.ChunkID]iface.NodeID
	// flushing: a TrimDone is in flight; the next flush waits for it.
	flushing bool
	// batch are trims decided in this turn of the loop, proposed together.
	batch []repair.Trim
	stats TrimStats
}

// TrimStats counts this leader's trims.
type TrimStats struct {
	Proposed uint64 `json:"proposed"` // trims proposed to the log
	Sent     uint64 `json:"sent"`     // deletes sent after commit, re-sends included
	Done     uint64 `json:"done"`     // copies the nodes confirmed removed
}

func newTrimState() trimState {
	return trimState{sent: map[iface.ChunkID]iface.Instant{}, ids: map[iface.ChunkID]uint64{}, acked: map[iface.ChunkID]iface.NodeID{}}
}

// Trims returns this leader's trim counters.
func (s *Server) Trims() TrimStats { return s.trims.stats }

// trimPending: an authorized trim of the copy of id on n is unanswered, so
// the copy may vanish at any moment and must not count.
func (s *Server) trimPending(id iface.ChunkID, n iface.NodeID) bool {
	p, ok := s.state.TrimPending(id)
	return ok && p == n && s.trims.acked[id] != n
}

// sendTrim queues the scheduler's decision for the log; the trims decided in
// one turn of the loop (a scan) go out as one intent. The delete is sent
// only once the intent has committed (ADR-0020): a deposed leader cannot
// commit, so it can never remove a copy, and the log admits one pending trim
// per chunk, so two leaders deciding on views that missed each other cannot
// both remove one.
func (s *Server) sendTrim(t repair.Trim) {
	s.trims.stats.Proposed++
	if len(s.trims.batch) == 0 {
		s.d.Clock.AfterFunc(0, s.proposeTrims)
	}
	s.trims.batch = append(s.trims.batch, t)
}

func (s *Server) proposeTrims() {
	batch := s.trims.batch
	s.trims.batch = nil
	if len(batch) == 0 {
		return
	}
	var targets []*chunkdv1.TrimTarget
	for _, t := range batch {
		targets = append(targets, &chunkdv1.TrimTarget{ChunkId: t.Chunk[:], Node: string(t.Node)})
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_TrimIntent{TrimIntent: &chunkdv1.TrimIntentOp{Targets: targets}}}, func(_ Result, err error) {
		if err != nil {
			s.d.Log.Warn("trim intent", "trims", len(batch), "err", err)
			return
		}
		for _, t := range batch {
			// Not authorized if another trim of the chunk was pending at apply time.
			if p, ok := s.state.TrimPending(t.Chunk); !s.leading || !ok || p != t.Node {
				continue
			}
			if _, sent := s.trims.sent[t.Chunk]; !sent {
				if !s.trimSafe(t.Chunk, t.Node) {
					s.trims.ids[t.Chunk] = t.ID // resendTrims sends it once repair has caught up
					continue
				}
				s.trimSent(t)
				s.d.Log.Info("trim replica", "trim", t.ID, "chunk", t.Chunk.String()[:12], "node", t.Node)
				s.sendTrimDelete(t.Chunk, t.Node, t.ID)
			}
		}
	})
}

func (s *Server) sendTrimDelete(id iface.ChunkID, n iface.NodeID, trimID uint64) {
	s.trims.sent[id] = s.d.Clock.Now()
	if trimID != 0 {
		s.trims.ids[id] = trimID
	}
	s.trims.stats.Sent++
	s.d.Net.Send(n, iface.Message{From: s.cfg.ID, Kind: wire.KindDeleteReplica,
		Body: wire.Marshal(&chunkdv1.DeleteReplica{TrimId: trimID, ChunkId: id[:], Term: s.raft.Status().Term})})
}

// trimAcked notes that n's copy of id, under a pending trim, is gone: the
// node reported it deleted (an absent chunk is reported deleted too).
func (s *Server) trimAcked(id iface.ChunkID, n iface.NodeID) {
	if !s.leading || !s.trimPending(id, n) {
		return
	}
	s.trims.acked[id] = n
	s.trims.stats.Done++
	s.trimmed(id, n, s.trims.ids[id])
}

// flushTrimDone logs the answers received since the last flush. Until it
// applies, the trim stays pending and a new leader resends it: harmless, the
// node answers an absent copy as deleted.
func (s *Server) flushTrimDone() {
	if len(s.trims.acked) == 0 || s.trims.flushing {
		return
	}
	s.trims.flushing = true
	batch := maps.Clone(s.trims.acked)
	var targets []*chunkdv1.TrimTarget
	for _, id := range slices.SortedFunc(maps.Keys(batch), func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) }) {
		targets = append(targets, &chunkdv1.TrimTarget{ChunkId: id[:], Node: string(batch[id])})
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_TrimDone{TrimDone: &chunkdv1.TrimDoneOp{Targets: targets}}}, func(_ Result, err error) {
		s.trims.flushing = false
		if err != nil {
			s.d.Log.Warn("trim done", "err", err)
			return
		}
		for id, n := range batch {
			if s.trims.acked[id] == n {
				delete(s.trims.acked, id)
				delete(s.trims.sent, id)
				delete(s.trims.ids, id)
			}
		}
	})
}

// resendTrims re-sends pending trims that went unanswered for a copy
// timeout: the command or the answer was lost, or the previous leader sent it.
// A copy no longer located at its node needs no delete: the trim ends.
// SIMPLIFIED: a pending trim on a node that never returns stays pending and
// holds off further trims of that chunk. HDFS drops a dead DataNode's
// pending deletions when it removes the node; decommission (ADR-0021) is
// where an operator would end them here.
func (s *Server) resendTrims() {
	if !s.leading {
		return
	}
	now := s.d.Clock.Now()
	for _, t := range s.state.TrimPendingAll() {
		if s.trims.acked[t.Chunk] == t.Node {
			continue
		}
		if !slices.Contains(s.cluster.Locations(t.Chunk), t.Node) {
			s.trimAcked(t.Chunk, t.Node)
			continue
		}
		if !s.cluster.Alive(t.Node) {
			continue
		}
		if at, sent := s.trims.sent[t.Chunk]; sent && now.Sub(at) < s.cfg.Repair.CopyTimeout {
			continue
		}
		if !s.trimSafe(t.Chunk, t.Node) {
			continue
		}
		s.sendTrimDelete(t.Chunk, t.Node, 0)
	}
	s.flushTrimDone()
}

// trimSafe: deleting n's copy of id now leaves the chunk RF confirmed, alive
// copies on other nodes that are not leaving and not being deleted. A trim
// is authorized against the holders when it is decided, but its delete may
// go out much later (the victim was down, or a new leader resends it); a
// holder that died since must not be counted. Until repair has replaced
// it, the delete waits: the pending copy does not count as a holder, so
// repair tops the chunk up first.
func (s *Server) trimSafe(id iface.ChunkID, n iface.NodeID) bool {
	if ci, ok := s.state.Chunk(id); !ok || ci.Refcount == 0 {
		return true
	}
	others := 0
	for _, o := range s.cluster.Locations(id) {
		if o != n && s.cluster.Alive(o) && s.cluster.Reported(o) && !s.state.Leaving(o) && !s.gcPending(id, o) && !s.trimPending(id, o) {
			others++
		}
	}
	return others >= s.cfg.Replicas
}
