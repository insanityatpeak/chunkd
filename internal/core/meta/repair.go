package meta

import (
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/placement"
	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// repairView adapts the server's state and node table to repair.View.
type repairView struct{ s *Server }

var _ repair.View = repairView{}

// Want: every chunk a committed or retained version references, until hard
// delete drops the last reference. Pending uploads are the client's to
// finish.
func (v repairView) Want(id iface.ChunkID) (int64, bool) {
	ci, ok := v.s.state.Chunk(id)
	return ci.Size, ok && ci.Refcount > 0
}

func (v repairView) Chunks(fn func(iface.ChunkID, int64)) {
	v.s.state.Chunks(func(id iface.ChunkID, ci ChunkInfo) {
		if ci.Refcount > 0 {
			fn(id, ci.Size)
		}
	})
}

func (v repairView) Holders(id iface.ChunkID) []repair.Holder {
	var out []repair.Holder
	for _, n := range v.s.cluster.Locations(id) {
		// A copy with a GC delete or a trim in flight may vanish: counting it
		// could trim a good copy or skip a needed one.
		if v.s.gcPending(id, n) || v.s.trimPending(id, n) {
			continue
		}
		ns, _ := v.s.cluster.Node(n)
		out = append(out, repair.Holder{Node: n, State: ns.State, DeadSince: ns.DeadSince,
			Confirmed: v.s.cluster.Reported(n), Rack: ns.Rack, Used: ns.Used})
	}
	return out
}

// Target uses the upload placement rules for one replica, with existing
// holders and busy targets removed from the candidate set.
// SIMPLIFIED: rack spread is not considered against the surviving replicas'
// racks. HDFS's BlockPlacementPolicy chooses repair targets with the
// existing replicas' racks as input.
func (v repairView) Target(size int64, exclude []iface.NodeID) (iface.NodeID, bool) {
	nodes := slices.DeleteFunc(v.s.cluster.PlacementView(), func(n placement.Node) bool { return slices.Contains(exclude, n.ID) })
	pl, err := placement.Place(nodes, []int64{size}, 1, 1, v.s.d.Rand)
	if err != nil {
		return "", false
	}
	return pl[0][0], true
}

// sendCopy tells the target to pull the chunk from the source.
func (s *Server) sendCopy(c repair.Copy) {
	src, _ := s.cluster.Node(c.Source)
	s.copyStarted(c)
	s.d.Log.Info("repair copy", "copy", c.ID, "chunk", c.Chunk.String()[:12], "from", c.Source, "to", c.Target, "bytes", c.Size)
	s.d.Net.Send(c.Target, iface.Message{From: s.cfg.ID, Kind: wire.KindReplicate,
		Body: wire.Marshal(&chunkdv1.ReplicateChunk{CopyId: c.ID, ChunkId: c.Chunk[:], Source: string(c.Source), SourceAddr: src.Addr, Term: s.raft.Status().Term})})
}

// corrupted handles copies a node quarantined after they failed
// verification: they no longer count, and their chunks are re-checked now,
// not at the next scan and without the repair delay (nothing died).
func (s *Server) corrupted(node iface.NodeID, corrupt, removed []iface.ChunkID) {
	var lost []iface.ChunkID
	for _, id := range corrupt {
		if slices.Contains(removed, id) {
			s.corruptReplicas++
			s.event("corrupt", node, "chunk %s failed verification: copy quarantined", id.String()[:12])
			lost = append(lost, id)
		}
	}
	s.repair.Recheck(lost)
}

// suspect handles a client's report that node served bytes for a chunk
// that did not match its hash. The client is not trusted to remove a
// replica (a bad gateway or network path would condemn good copies), so
// the node re-checks, and a mismatch comes back as an ordinary corrupt
// report.
// SIMPLIFIED: hints are not rate-limited; each costs the node one re-read.
// HDFS DataNodes throttle client-reported bad blocks the same way scans
// are throttled.
func (s *Server) suspect(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.SuspectRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	id, err := wire.ChunkID(req.GetChunkId())
	if err != nil {
		respond(nil, err)
		return
	}
	node := iface.NodeID(req.GetNode())
	if _, known := s.cluster.Node(node); known {
		s.event("corrupt", node, "client saw bad bytes for chunk %s: node asked to re-check", id.String()[:12])
		s.d.Net.Send(node, iface.Message{From: s.cfg.ID, Kind: wire.KindVerifyChunk, Body: wire.Marshal(&chunkdv1.VerifyChunk{ChunkId: id[:], Term: s.raft.Status().Term})})
	}
	respond(wire.Marshal(&chunkdv1.SuspectResponse{}), nil)
}

// verifyReport is where Phase 3 checks a returning node's copies against
// the scrubber's record (stale or corrupt replicas). Today a reported copy
// is trusted until a reader rejects its hash.
func (s *Server) verifyReport(iface.NodeID, []iface.ChunkID) {}

// Repair exposes the repair scheduler. Loop-owned.
func (s *Server) Repair() *repair.Scheduler { return s.repair }

// Nodes lists every known node that is not draining, with its liveness.
func (v repairView) Nodes() []repair.Member {
	var out []repair.Member
	for _, n := range v.s.cluster.Nodes() {
		if !n.Draining {
			out = append(out, repair.Member{ID: n.ID, Rack: n.Rack, State: n.State, DeadSince: n.DeadSince, Confirmed: v.s.cluster.Reported(n.ID)})
		}
	}
	return out
}
