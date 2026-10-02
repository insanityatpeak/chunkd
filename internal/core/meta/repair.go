package meta

import (
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/placement"
	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// repairView adapts the server's state and node table to repair.View.
type repairView struct{ s *Server }

var _ repair.View = repairView{}

// Want: every chunk a committed or retained version references, and every
// shard of a referenced stripe, until hard delete drops the last reference.
// Pending uploads are the client's to finish.
func (v repairView) Want(id iface.ChunkID) (repair.Block, bool) {
	return v.s.wanted(id)
}

// Chunks walks records in ID order; a stripe yields its shards in index
// order. A stripe record has no copies of its own.
func (v repairView) Chunks(fn func(iface.ChunkID, repair.Block)) {
	v.s.state.Chunks(func(id iface.ChunkID, ci ChunkInfo) {
		switch {
		case ci.Refcount == 0:
		case ci.Shards == nil:
			fn(id, repair.Block{Size: ci.Size, Target: v.s.cfg.Replicas})
		default:
			for j, sh := range ci.Shards {
				fn(sh, shardBlock(id, j, ci))
			}
		}
	})
}

// wanted describes a block repair should keep at its target.
func (s *Server) wanted(id iface.ChunkID) (repair.Block, bool) {
	b, ok := s.state.Block(id)
	if !ok {
		return repair.Block{}, false
	}
	if !b.Shard {
		ci, _ := s.state.Chunk(id)
		return repair.Block{Size: b.Size, Target: s.cfg.Replicas}, ci.Refcount > 0
	}
	ci, ok := s.state.Chunk(b.Ref.Stripe)
	if !ok || ci.Refcount == 0 {
		return repair.Block{}, false
	}
	return shardBlock(b.Ref.Stripe, b.Ref.Index, ci), true
}

func shardBlock(stripe iface.ChunkID, j int, ci ChunkInfo) repair.Block {
	return repair.Block{Size: ec.BlockSize(ci.Size), Target: 1,
		Stripe: &repair.Stripe{ID: stripe, Index: j, ChunkSize: ci.Size, Shards: ci.Shards}}
}

func (v repairView) Holders(id iface.ChunkID) []repair.Holder {
	var out []repair.Holder
	for _, n := range v.s.cluster.Locations(id) {
		// A copy with a GC delete or a trim in flight may vanish: counting it
		// could trim a good copy or skip a needed one.
		if v.s.gcPending(id, n) || v.s.trimPending(id, n) {
			continue
		}
		// A decommissioned node may be switched off at any moment: its copy is
		// neither counted nor used as a source.
		admin := v.s.state.NodeAdmin(n)
		if admin == chunkdv1.NodeAdmin_NODE_ADMIN_DECOMMISSIONED {
			continue
		}
		ns, _ := v.s.cluster.Node(n)
		out = append(out, repair.Holder{Node: n, State: ns.State, DeadSince: ns.DeadSince,
			Confirmed: v.s.cluster.Reported(n), Rack: ns.Rack, Used: ns.Used, Leaving: admin == chunkdv1.NodeAdmin_NODE_ADMIN_DRAINING})
	}
	return out
}

// Target uses the upload placement rules for one replica, with existing
// holders and busy targets removed from the candidate set.
// SIMPLIFIED: rack spread is not considered against the surviving replicas'
// racks. HDFS's BlockPlacementPolicy chooses repair targets with the
// existing replicas' racks as input.
func (v repairView) Target(size int64, exclude []iface.NodeID) (iface.NodeID, bool) {
	nodes := slices.DeleteFunc(v.s.cluster.PlacementView(v.s.state.Leaving), func(n placement.Node) bool { return slices.Contains(exclude, n.ID) })
	pl, err := placement.Place(nodes, []int64{size}, 1, 1, v.s.d.Rand)
	if err != nil {
		return "", false
	}
	return pl[0][0], true
}

// sendCopy tells the target to pull the chunk from the source.
func (s *Server) sendCopy(c repair.Copy) {
	s.copyStarted(c)
	if r := c.Rebuild; r != nil {
		cmd := &chunkdv1.RebuildShard{CopyId: c.ID, ShardId: c.Chunk[:], Index: int32(r.Stripe.Index), LogicalId: r.Stripe.ID[:],
			ChunkSize: r.Stripe.ChunkSize, Term: s.raft.Status().Term}
		for _, src := range r.Sources {
			n, _ := s.cluster.Node(src.Node)
			cmd.Sources = append(cmd.Sources, &chunkdv1.ShardSource{Index: int32(src.Index), ShardId: src.Shard[:], Node: string(src.Node), Addr: n.Addr})
		}
		s.d.Log.Info("repair rebuild", "copy", c.ID, "shard", c.Chunk.String()[:12], "index", r.Stripe.Index, "to", c.Target, "sources", len(r.Sources))
		s.d.Net.Send(c.Target, iface.Message{From: s.cfg.ID, Kind: wire.KindRebuildShard, Body: wire.Marshal(cmd)})
		return
	}
	src, _ := s.cluster.Node(c.Source)
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
		if !v.s.state.Leaving(n.ID) {
			out = append(out, repair.Member{ID: n.ID, Rack: n.Rack, State: n.State, DeadSince: n.DeadSince, Confirmed: v.s.cluster.Reported(n.ID)})
		}
	}
	return out
}
