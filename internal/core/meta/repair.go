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

// Want: every chunk a committed version references, until GC (Phase 4)
// decrements its refcount. Pending uploads are the client's to finish.
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
	s.d.Log.Info("repair copy", "copy", c.ID, "chunk", c.Chunk.String()[:12], "from", c.Source, "to", c.Target, "bytes", c.Size)
	s.d.Net.Send(c.Target, iface.Message{From: s.cfg.ID, Kind: wire.KindReplicate,
		Body: wire.Marshal(&chunkdv1.ReplicateChunk{CopyId: c.ID, ChunkId: c.Chunk[:], Source: string(c.Source), SourceAddr: src.Addr})})
}

// sendTrim tells a node to drop its copy of an over-replicated chunk.
func (s *Server) sendTrim(t repair.Trim) {
	s.d.Log.Info("trim replica", "trim", t.ID, "chunk", t.Chunk.String()[:12], "node", t.Node)
	s.d.Net.Send(t.Node, iface.Message{From: s.cfg.ID, Kind: wire.KindDeleteReplica,
		Body: wire.Marshal(&chunkdv1.DeleteReplica{TrimId: t.ID, ChunkId: t.Chunk[:]})})
}

// verifyReport is where Phase 3 checks a returning node's copies against
// the scrubber's record (stale or corrupt replicas). Today a reported copy
// is trusted until a reader rejects its hash.
func (s *Server) verifyReport(iface.NodeID, []iface.ChunkID) {}

// Repair exposes the repair scheduler. Loop-owned.
func (s *Server) Repair() *repair.Scheduler { return s.repair }
