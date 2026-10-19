package meta

import (
	"context"
	"errors"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/consensus"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// group is the metadata group's slots in order: Peers, or this server alone.
func (s *Server) group() []iface.NodeID {
	if len(s.cfg.Peers) == 0 {
		return []iface.NodeID{s.cfg.ID}
	}
	return s.cfg.Peers
}

// slotVoters returns the voters in peer's slot, lowest generation first, and
// the slot's index; ok is false if peer is not a member's name.
func (s *Server) slotVoters(peer iface.NodeID) (ids []uint64, slot int, ok bool) {
	g := s.group()
	slot = slices.Index(g, peer) + 1
	if slot == 0 {
		return nil, 0, false
	}
	for _, id := range s.raft.Status().Voters {
		if RaftSlot(id, len(g)) == slot {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, slot, true
}

// peer returns the leader's view of voter id.
func (s *Server) peer(id uint64) (consensus.Peer, bool) {
	i := slices.IndexFunc(s.raft.Peers(), func(p consensus.Peer) bool { return p.ID == id })
	if i < 0 {
		return consensus.Peer{}, false
	}
	return s.raft.Peers()[i], true
}

// replacePeer swaps the voter in a slot for the slot's next generation:
// it logs the old ID as retired, removes it, then adds the next one, one
// single-server change at a time (ADR-0032). Removal comes first because the
// old peer is usually down: added first, the new voter would make four with
// two down, and the group would lose its quorum. The logged retirement is
// what makes a repeat safe: the next generation is added only while the
// slot is empty and its latest retired ID is the one the caller named, so
// no ID is ever added twice.
// SIMPLIFIED: etcd gives each member a random ID and keeps the member list
// in its log, so a group can also grow, shrink or move a member to a new
// address; here the names and addresses are fixed by the -peers list.
func (s *Server) replacePeer(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.ReplacePeerRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	name, old := iface.NodeID(req.GetPeer()), req.GetRaftId()
	ids, slot, ok := s.slotVoters(name)
	if !ok {
		respond(nil, iface.Errorf(iface.CodeNotFound, "no metadata peer %q", name))
		return
	}
	n := len(s.group())
	if old == 0 || RaftSlot(old, n) != slot {
		respond(nil, iface.Errorf(iface.CodeInvalid, "raft ID %d is not in %s's slot %d", old, name, slot))
		return
	}
	next := old + uint64(n)
	resp := &chunkdv1.ReplacePeerResponse{Removed: old, Added: next}
	add := func() {
		s.raft.ChangeMembership(true, next, func(err error) {
			if err == nil {
				s.event("raft", name, "raft ID %d added in place of %d; start %s with an empty store in join mode", next, old, name)
			}
			wire.Respond(respond, resp, err)
		})
	}
	remove := func() {
		s.raft.ChangeMembership(false, old, func(err error) {
			if err != nil {
				respond(nil, err)
				return
			}
			s.event("raft", name, "raft ID %d removed", old)
			add()
		})
	}
	switch {
	case slices.Equal(ids, []uint64{next}):
		respond(wire.Marshal(resp), nil) // already done
	case len(ids) == 0 && s.latestRetired(slot) == old:
		add()
	case slices.Equal(ids, []uint64{old}):
		if old == s.raft.Status().ID {
			respond(nil, iface.Errorf(iface.CodeConflict, "%s is the leader; replace it from a group another peer leads", name))
			return
		}
		if p, ok := s.peer(old); ok && p.Heard && p.HeardAgo < s.cfg.ElectionTimeout && !req.GetForce() {
			respond(nil, iface.Errorf(iface.CodeConflict, "%s answered %v ago; replace a live peer only with force", name, p.HeardAgo))
			return
		}
		if slices.Contains(s.state.Retired(), old) {
			remove()
			return
		}
		s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_RetirePeer{RetirePeer: &chunkdv1.RetirePeerOp{RaftId: old}}}, func(_ Result, err error) {
			if err != nil {
				respond(nil, err)
				return
			}
			remove()
		})
	default:
		respond(nil, iface.Errorf(iface.CodeConflict, "%s's slot holds raft IDs %v and last retired %d, not %d", name, ids, s.latestRetired(slot), old))
	}
}

// latestRetired is the highest retired Raft ID in slot, or 0.
func (s *Server) latestRetired(slot int) uint64 {
	var top uint64
	for _, id := range s.state.Retired() {
		if RaftSlot(id, len(s.group())) == slot {
			top = max(top, id)
		}
	}
	return top
}

// join tells a peer started in join mode its Raft ID: the voter in its
// slot, as long as that voter has never acknowledged an entry. One that has
// held a log, and now asks with an empty store, lost it; answering would let
// it vote again with no memory of its votes, so it must be replaced again.
// SIMPLIFIED: a voter can grant a vote before its first append, which the
// match index does not show; etcd's random member IDs make every restart
// with an empty store a new member.
func (s *Server) join(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.JoinRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	name := iface.NodeID(req.GetPeer())
	ids, _, ok := s.slotVoters(name)
	switch {
	case !ok:
		respond(nil, iface.Errorf(iface.CodeNotFound, "no metadata peer %q", name))
	case len(ids) == 0:
		respond(nil, iface.Errorf(iface.CodeNotFound, "%s's slot has no voter; run chunkd meta replace %s", name, name))
	default:
		id := ids[len(ids)-1]
		if p, ok := s.peer(id); ok && p.Match > 0 {
			respond(nil, iface.Errorf(iface.CodeConflict, "raft ID %d has already acknowledged log entries, so %s lost its log; run chunkd meta replace %s again", id, name, name))
			return
		}
		respond(wire.Marshal(&chunkdv1.JoinResponse{RaftId: id}), nil)
	}
}

// AskJoin asks the other peers, all at once, which Raft ID the group added for
// self. Only the leader answers; a refusal from it (CodeConflict: self lost a
// log the group relied on) wins over the followers' not-leader answers. Call
// it before NewServer, never from a handler or timer.
func AskJoin(ctx context.Context, caller iface.Caller, self iface.NodeID, peers []iface.NodeID) (uint64, error) {
	var calls []iface.Call
	for _, p := range peers {
		if p != self {
			calls = append(calls, iface.Call{To: p, Kind: wire.KindJoin, Body: wire.Marshal(&chunkdv1.JoinRequest{Peer: string(self)})})
		}
	}
	if len(calls) == 0 {
		return 0, iface.Errorf(iface.CodeInvalid, "%s has no other peers to join", self)
	}
	var errs []error
	for _, r := range caller.Do(ctx, calls) {
		if r.Err != nil {
			if c := iface.CodeOf(r.Err); c == iface.CodeConflict || c == iface.CodeNotFound {
				return 0, r.Err
			}
			errs = append(errs, r.Err)
			continue
		}
		var resp chunkdv1.JoinResponse
		if err := wire.Decode(r.Body, &resp); err != nil {
			return 0, err
		}
		return resp.GetRaftId(), nil
	}
	return 0, iface.Errorf(iface.CodeUnavailable, "no leader answered: %v", errors.Join(errs...))
}
