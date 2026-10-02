package meta

import (
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// nodeAdmin drains, undrains or decommissions a storage node (ADR-0021).
// The state is logged, so the drain outlives this leader; a repeat of the
// current state applies nothing, so a retried command is safe.
func (s *Server) nodeAdmin(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.NodeAdminRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	n, to := iface.NodeID(req.GetNode()), req.GetState()
	if _, known := s.cluster.Node(n); !known {
		respond(nil, iface.Errorf(iface.CodeNotFound, "no storage node %q has reported", n))
		return
	}
	if to == chunkdv1.NodeAdmin_NODE_ADMIN_DECOMMISSIONED && s.state.NodeAdmin(n) != to {
		if short := s.shortElsewhere(n); short > 0 {
			respond(nil, iface.Errorf(iface.CodeConflict, "%s cannot be decommissioned yet: %d of its chunks have fewer than %d confirmed copies on other nodes", n, short, s.cfg.Replicas))
			return
		}
	}
	resp := &chunkdv1.NodeAdminResponse{State: to}
	if to == chunkdv1.NodeAdmin_NODE_ADMIN_DRAINING {
		resp.Warning = s.drainWarning(n)
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_NodeAdmin{NodeAdmin: &chunkdv1.NodeAdminOp{Node: string(n), State: to}}}, func(_ Result, err error) {
		if err == nil {
			s.event("node", n, "%s by the operator", AdminName(to))
			// Placement, holders and targets changed: look again now.
			s.repair.Scan()
		}
		wire.Respond(respond, resp, err)
	})
}

// shortElsewhere counts the referenced blocks on n that do not have their
// target (RF, or 1 for a shard) of confirmed, alive copies on other
// non-leaving nodes: what decommissioning n now would leave short.
// SIMPLIFIED: checked against the leader's location map, which is soft state;
// a holder that dies after the check is a failure like any other, repaired
// from the copies that remain. HDFS checks its block map the same way.
func (s *Server) shortElsewhere(n iface.NodeID) int {
	short := 0
	for _, id := range s.cluster.ChunksOn(n) {
		b, ok := s.wanted(id)
		if !ok {
			continue
		}
		others := 0
		for _, o := range s.cluster.Locations(id) {
			if o != n && s.cluster.Alive(o) && s.cluster.Reported(o) && !s.state.Leaving(o) && !s.gcPending(id, o) && !s.trimPending(id, o) {
				others++
			}
		}
		if others < b.Target {
			short++
		}
	}
	return short
}

// drainWarning says what draining n gives up, if anything: the last node of
// its rack takes rack spread with it.
func (s *Server) drainWarning(n iface.NodeID) string {
	ns, _ := s.cluster.Node(n)
	for _, o := range s.cluster.Nodes() {
		if o.ID != n && o.Rack == ns.Rack && !s.state.Leaving(o.ID) {
			return ""
		}
	}
	return "last active node on rack " + ns.Rack + ": its chunks will span fewer racks"
}
