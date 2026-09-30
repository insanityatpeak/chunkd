// Package consensus runs one peer of a Raft group on the metadata server's
// event loop, on top of etcd's raft state machine (RawNode).
//
// The library is a pure state machine: it never reads a clock, spawns a
// goroutine or touches disk. This package supplies those from iface, so the
// sim replays a run from its seed. One thing the library does itself is draw
// election timeouts from crypto/rand, which no seed controls. So followers
// are never ticked; a seeded timer here calls Campaign instead, and the two
// protections the library builds on its own election timer (the vote lease
// and the leader's quorum check) are done here, from the injected clock.
package consensus

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// FSM is the replicated state machine. Every method runs on the node's
// event loop.
type FSM interface {
	// Apply applies the committed entry at index. Apply must be a pure
	// function of the entries before it: every peer sees the same ones. Its
	// result goes to the callback of the Propose that made the entry, if
	// this peer proposed it and is still waiting.
	Apply(index uint64, data []byte) any
	// Snapshot encodes the state after the last Apply.
	Snapshot() []byte
	// Restore replaces the state with a snapshot.
	Restore(data []byte) error
}

// Deps are the environment a peer runs in.
type Deps struct {
	Clock iface.Clock
	Net   iface.Transport
	Store iface.MetaStore
	Rand  iface.Rand
	Log   *slog.Logger
}

// Config sets identity and timing.
type Config struct {
	// ID is this peer's Raft ID (non-zero). Node names peers on the
	// transport; it maps every ID to its NodeID, including peers that join
	// later.
	ID   uint64
	Node func(id uint64) iface.NodeID
	// Peers are the initial voters, used only when the store is empty. A
	// peer that will join a running group starts with none and is added by
	// ChangeMembership on the leader.
	Peers []uint64
	// TickEvery is the leader's heartbeat interval and the granularity of
	// every timer here.
	TickEvery time.Duration
	// ElectionTimeout is the minimum silence from a leader before a follower
	// campaigns; the actual wait is uniform in [ElectionTimeout, 2×). It is
	// also the vote lease and the leader's quorum window.
	ElectionTimeout time.Duration
	// RequestTimeout bounds how long a proposal or read waits for its
	// outcome. On expiry the callback gets CodeUnavailable: the entry may
	// still commit, so callers retry idempotently.
	RequestTimeout time.Duration
	// SnapshotEvery is the number of applied entries between FSM snapshots
	// and log compactions; Trailing is how many entries before the snapshot
	// stay in memory so a briefly lagging follower catches up from the log
	// instead of a snapshot.
	SnapshotEvery int
	Trailing      int
}

// DefaultConfig is a 100 ms heartbeat, a 1 s election timeout and a
// snapshot every 1,000 entries.
// SIMPLIFIED: etcd defaults to a 100 ms heartbeat and a 1 s election
// timeout too; Consul and CockroachDB use several seconds across regions.
func DefaultConfig(id uint64, node func(uint64) iface.NodeID, peers []uint64) Config {
	return Config{ID: id, Node: node, Peers: peers, TickEvery: 100 * time.Millisecond, ElectionTimeout: time.Second,
		RequestTimeout: 5 * time.Second, SnapshotEvery: 1000, Trailing: 1000}
}

// Status is a peer's view of the group.
type Status struct {
	ID         uint64
	Term       uint64
	Leader     uint64 // 0 if unknown
	LeaderNode iface.NodeID
	Commit     uint64
	Applied    uint64
	Voters     []uint64
	// IsLeader: this peer is the leader and heard from a quorum within the
	// election timeout. A leader cut off from its quorum reports false and
	// must not send commands, although the library still calls it leader
	// until it sees a higher term.
	IsLeader bool
	// Ready: IsLeader, and the FSM has applied an entry of this term, so it
	// holds everything committed by earlier leaders. Serve nothing before it.
	Ready bool
}

type pending struct {
	cb      func(any, error)
	expires iface.Instant
	// data is the proposed payload. An applied entry answers the callback
	// only if its payload matches: ids restart at 1 with every incarnation,
	// so an entry the previous incarnation logged could otherwise deliver its
	// result to a new proposal that reused the id.
	data []byte
}

type readReq struct {
	cb      func(error)
	expires iface.Instant
}

type readWait struct {
	index uint64
	cb    func(error)
}

// Node is one Raft peer. Not safe for concurrent use: every method runs on
// the owner's event loop.
type Node struct {
	d   Deps
	cfg Config
	fsm FSM
	rn  *raft.RawNode
	mem *raft.MemoryStorage

	term        uint64 // last term seen in a Ready
	applied     uint64
	appliedTerm uint64
	voters      []uint64
	confState   pb.ConfState
	sinceSnap   int

	proposals map[uint64]*pending
	nextProp  uint64
	confWait  map[uint64]*pending // by the peer a membership change adds or removes
	reads     map[uint64]*readReq
	nextRead  uint64
	waiting   []readWait // reads answered by the library, waiting for apply

	lastHeard  iface.Instant // last leader contact, or vote granted
	electionAt time.Duration // this round's randomized wait
	seen       map[uint64]iface.Instant
	wasLeader  bool

	changed  func(Status)
	last     Status
	timer    iface.Timer
	busy     bool
	deferred []func()
	stopped  bool
}

// New recovers the peer from its store: the snapshot into the FSM, the log
// and hard state into the library. An empty store with Peers set bootstraps
// a new group; every initial peer must be given the same list.
func New(ctx context.Context, d Deps, cfg Config, fsm FSM) (*Node, error) {
	if d.Clock == nil || d.Net == nil || d.Store == nil || d.Rand == nil || d.Log == nil || fsm == nil {
		panic("consensus: missing dependency")
	}
	if cfg.ID == 0 || cfg.Node == nil || cfg.TickEvery <= 0 || cfg.ElectionTimeout < 2*cfg.TickEvery || cfg.SnapshotEvery < 1 || cfg.Trailing < 0 {
		return nil, fmt.Errorf("consensus: bad config %+v", cfg)
	}
	n := &Node{d: d, cfg: cfg, fsm: fsm, mem: raft.NewMemoryStorage(), voters: slices.Clone(cfg.Peers),
		proposals: map[uint64]*pending{}, confWait: map[uint64]*pending{}, reads: map[uint64]*readReq{}, seen: map[uint64]iface.Instant{}}
	snapAt, blob, err := d.Store.LoadSnapshot(ctx)
	at := uint64(snapAt)
	if err != nil {
		return nil, err
	}
	if at > 0 {
		var snap pb.Snapshot
		if err := proto.Unmarshal(blob, &snap); err != nil {
			return nil, fmt.Errorf("consensus: snapshot at %d: %w", at, err)
		}
		if snap.GetMetadata().GetIndex() != at {
			return nil, fmt.Errorf("consensus: snapshot metadata index %d, store says %d", snap.GetMetadata().GetIndex(), at)
		}
		if err := n.mem.ApplySnapshot(&snap); err != nil {
			return nil, err
		}
		if err := fsm.Restore(snap.GetData()); err != nil {
			return nil, fmt.Errorf("consensus: restore snapshot at %d: %w", at, err)
		}
		n.applied, n.appliedTerm = at, snap.GetMetadata().GetTerm()
		n.confState = *snap.GetMetadata().GetConfState()
		n.voters = slices.Clone(n.confState.Voters)
	}
	var ents []*pb.Entry
	err = d.Store.Replay(ctx, snapAt+1, func(i iface.Index, b []byte) error {
		e := &pb.Entry{}
		if err := proto.Unmarshal(b, e); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if e.GetIndex() != uint64(i) {
			return fmt.Errorf("entry %d holds raft index %d", i, e.GetIndex())
		}
		ents = append(ents, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := n.mem.Append(ents); err != nil {
		return nil, err
	}
	hsb, err := d.Store.State(ctx)
	if err != nil {
		return nil, err
	}
	hs := &pb.HardState{}
	if hsb != nil {
		if err := proto.Unmarshal(hsb, hs); err != nil {
			return nil, fmt.Errorf("consensus: hard state: %w", err)
		}
		if err := n.mem.SetHardState(hs); err != nil {
			return nil, err
		}
	}

	n.rn, err = raft.NewRawNode(&raft.Config{ID: cfg.ID, ElectionTick: 10, HeartbeatTick: 1, Storage: snapshotter{n.mem, n}, Applied: at,
		MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, PreVote: true, DisableProposalForwarding: true, Logger: raftLog{d.Log}})
	if err != nil {
		return nil, err
	}
	if at == 0 && len(ents) == 0 && raft.IsEmptyHardState(hs) && len(cfg.Peers) > 0 {
		peers := make([]raft.Peer, len(cfg.Peers))
		for i, id := range cfg.Peers {
			peers[i] = raft.Peer{ID: id}
		}
		if err := n.rn.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	n.lastHeard = d.Clock.Now()
	if len(n.voters) == 1 {
		// A group of one never waits for anyone: draw nothing from the
		// shared RNG, so adding consensus leaves a single server's run
		// exactly as it was.
		n.electionAt = cfg.ElectionTimeout
	} else {
		n.rearm()
	}
	n.term = n.rn.BasicStatus().GetTerm()
	n.process()
	return n, nil
}

// OnChange registers f, called when the leader, term or readiness changes.
func (n *Node) OnChange(f func(Status)) { n.changed = f }

// Start begins the timers. A group of one has nobody to wait for and leads
// at once.
func (n *Node) Start() {
	n.timer = n.d.Clock.AfterFunc(n.cfg.TickEvery, n.tick)
	if len(n.voters) == 1 && n.voters[0] == n.cfg.ID {
		_ = n.rn.Campaign()
		n.process()
	}
}

// Stop cancels the timers and fails every waiting caller. The peer must not
// be used afterwards; a restart builds a new Node on the same store.
func (n *Node) Stop() {
	n.stopped = true
	if n.timer != nil {
		n.timer.Stop()
	}
	n.failAll(iface.Errorf(iface.CodeUnavailable, "stopped"))
	n.flush()
}

func (n *Node) rearm() {
	n.electionAt = n.cfg.ElectionTimeout + time.Duration(n.d.Rand.IntN(int(n.cfg.ElectionTimeout)))
}

func (n *Node) tick() {
	if n.stopped {
		return
	}
	n.timer = n.d.Clock.AfterFunc(n.cfg.TickEvery, n.tick)
	now := n.d.Clock.Now()
	if n.rn.BasicStatus().RaftState == raft.StateLeader {
		n.rn.Tick()
	} else if now.Sub(n.lastHeard) >= n.electionAt {
		n.lastHeard = now
		n.rearm()
		_ = n.rn.Campaign()
	}
	n.expire(now)
	n.process()
}

// expire fails proposals and reads that waited past RequestTimeout.
func (n *Node) expire(now iface.Instant) {
	for id, p := range n.proposals {
		if now >= p.expires {
			delete(n.proposals, id)
			n.later(func() {
				p.cb(nil, iface.Errorf(iface.CodeUnavailable, "no commit within %v; the entry may still commit", n.cfg.RequestTimeout))
			})
		}
	}
	for id, p := range n.confWait {
		if now >= p.expires {
			delete(n.confWait, id)
			n.later(func() {
				p.cb(nil, iface.Errorf(iface.CodeUnavailable, "membership change not applied within %v", n.cfg.RequestTimeout))
			})
		}
	}
	for id, r := range n.reads {
		if now >= r.expires {
			delete(n.reads, id)
			n.later(func() {
				r.cb(iface.Errorf(iface.CodeUnavailable, "no quorum confirmed the leader within %v", n.cfg.RequestTimeout))
			})
		}
	}
}

// Receive handles a message from another peer.
func (n *Node) Receive(from iface.NodeID, body []byte) {
	if n.stopped {
		return
	}
	var m pb.Message
	if err := proto.Unmarshal(body, &m); err != nil {
		n.d.Log.Warn("bad raft message", "from", from, "err", err)
		return
	}
	now := n.d.Clock.Now()
	st := n.rn.BasicStatus()
	switch m.GetType() {
	case pb.MsgVote, pb.MsgPreVote:
		// The vote lease: while a leader is heard from, or this peer is a
		// leader with its quorum, vote requests are dropped, so a peer that
		// lost contact cannot depose a working leader.
		if n.leaseHeld(now, st) {
			return
		}
	case pb.MsgApp, pb.MsgHeartbeat, pb.MsgSnap:
		// Contact from a leader of the current or a newer term. One of an
		// older term is a deposed leader and must not delay elections.
		if m.GetTerm() >= st.GetTerm() {
			n.lastHeard = now
		}
	case pb.MsgAppResp, pb.MsgHeartbeatResp:
		n.seen[m.GetFrom()] = now
	}
	if err := n.rn.Step(&m); err != nil && !errors.Is(err, raft.ErrStepPeerNotFound) {
		n.d.Log.Debug("raft step", "from", from, "type", m.GetType().String(), "err", err)
	}
	n.process()
}

func (n *Node) leaseHeld(now iface.Instant, st raft.BasicStatus) bool {
	if st.RaftState == raft.StateLeader {
		return n.quorumActive(now)
	}
	return st.Lead != raft.None && now.Sub(n.lastHeard) < n.cfg.ElectionTimeout
}

// quorumActive: this peer heard from a majority of voters, itself included,
// within the election timeout. The library's CheckQuorum does this from its
// own tick counter, which is off here (see the package comment).
func (n *Node) quorumActive(now iface.Instant) bool {
	active := 0
	for _, id := range n.voters {
		if id == n.cfg.ID || now.Sub(n.seen[id]) < n.cfg.ElectionTimeout && n.seen[id] != 0 {
			active++
		}
	}
	return active > len(n.voters)/2
}

// Status returns the current view.
func (n *Node) Status() Status {
	now := n.d.Clock.Now()
	st := n.rn.BasicStatus()
	s := Status{ID: n.cfg.ID, Term: st.GetTerm(), Leader: st.Lead, Commit: st.GetCommit(), Applied: n.applied, Voters: slices.Clone(n.voters)}
	if s.Leader != raft.None {
		s.LeaderNode = n.cfg.Node(s.Leader)
	}
	s.IsLeader = st.RaftState == raft.StateLeader && n.quorumActive(now)
	s.Ready = s.IsLeader && n.appliedTerm == st.GetTerm()
	return s
}

// NotLeader is the error for a request that needs the leader: CodeNotLeader
// with the leader's node ID as its message, empty if none is known.
func (s Status) NotLeader() error {
	return &iface.Error{Code: iface.CodeNotLeader, Msg: string(s.LeaderNode)}
}

func (n *Node) notLeader() error { return n.Status().NotLeader() }

// frame prefixes a proposal with the id its callback is registered under.
func frame(id uint64, data []byte) []byte {
	return append(binary.LittleEndian.AppendUint64(make([]byte, 0, 8+len(data)), id), data...)
}

// Propose replicates data. cb gets the FSM's result once the entry commits
// and applies, or an error: CodeNotLeader if this peer is not a ready
// leader, CodeUnavailable if it lost leadership or timed out first. In both
// of the last two cases the entry may still commit, so callers retry with an
// idempotent request.
func (n *Node) Propose(data []byte, cb func(any, error)) {
	if !n.Status().Ready {
		cb(nil, n.notLeader())
		return
	}
	n.nextProp++
	id := n.nextProp
	if err := n.rn.Propose(frame(id, data)); err != nil {
		cb(nil, iface.Errorf(iface.CodeUnavailable, "proposal dropped: %v", err))
		return
	}
	n.proposals[id] = &pending{cb: cb, expires: n.d.Clock.Now().Add(n.cfg.RequestTimeout), data: data}
	n.process()
}

// ReadIndex calls cb(nil) once this peer, still the leader by a quorum's
// confirmation, has applied everything committed when the read began: a
// read of the FSM after it is linearizable. No clock is consulted (a lease
// would be), so a paused leader cannot serve a stale read.
func (n *Node) ReadIndex(cb func(error)) {
	if !n.Status().Ready {
		cb(n.notLeader())
		return
	}
	n.nextRead++
	id := n.nextRead
	n.reads[id] = &readReq{cb: cb, expires: n.d.Clock.Now().Add(n.cfg.RequestTimeout)}
	n.rn.ReadIndex(binary.LittleEndian.AppendUint64(nil, id))
	n.process()
}

// ChangeMembership adds or removes one voter. cb runs when the change has
// applied on this peer. One change at a time: the library drops a proposal
// while an earlier change is uncommitted.
// SIMPLIFIED: single-server changes only. Joint consensus (Raft paper §6)
// changes several members at once; etcd and CockroachDB use it for
// replacing a rack, and single-server steps for everything else.
func (n *Node) ChangeMembership(add bool, id uint64, cb func(error)) {
	if !n.Status().Ready {
		cb(n.notLeader())
		return
	}
	t := pb.ConfChangeRemoveNode
	if add {
		t = pb.ConfChangeAddNode
	}
	if err := n.rn.ProposeConfChange(&pb.ConfChange{Type: t.Enum(), NodeId: &id}); err != nil {
		cb(iface.Errorf(iface.CodeUnavailable, "membership change dropped: %v", err))
		return
	}
	n.confWait[id] = &pending{cb: func(_ any, err error) { cb(err) }, expires: n.d.Clock.Now().Add(n.cfg.RequestTimeout)}
	n.process()
}

// later queues f to run after the current Ready has been advanced, so a
// callback that proposes again never runs mid-batch.
func (n *Node) later(f func()) { n.deferred = append(n.deferred, f) }

func (n *Node) flush() {
	for len(n.deferred) > 0 {
		f := n.deferred[0]
		n.deferred = n.deferred[1:]
		f()
	}
}

// process drains the library: persist, send, apply, advance. It re-enters
// itself through callbacks that propose, so a flag keeps one batch at a time.
func (n *Node) process() {
	if n.busy {
		return
	}
	n.busy = true
	defer func() { n.busy = false }()
	for {
		for n.rn.HasReady() {
			n.handleReady(n.rn.Ready())
		}
		if len(n.deferred) == 0 {
			break
		}
		n.flush()
	}
	n.notify()
}

func (n *Node) handleReady(rd raft.Ready) {
	ctx := context.Background()
	if rd.SoftState != nil && rd.RaftState != raft.StateLeader && n.wasLeader {
		n.failAll(iface.Errorf(iface.CodeUnavailable, "leadership lost; the entry may still commit"))
	}
	if rd.SoftState != nil {
		n.wasLeader = rd.RaftState == raft.StateLeader
		if n.wasLeader {
			clear(n.seen)
			n.lastHeard = n.d.Clock.Now()
		}
	}

	var hs []byte
	if !raft.IsEmptyHardState(rd.HardState) {
		hs = wire.Marshal(rd.HardState)
		n.term = rd.HardState.GetTerm()
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		n.installSnapshot(ctx, rd.Snapshot, hs)
	}
	if len(rd.Entries) > 0 || hs != nil {
		first := n.lastIndex() + 1
		var raw [][]byte
		if len(rd.Entries) > 0 {
			first = rd.Entries[0].GetIndex()
			for _, e := range rd.Entries {
				raw = append(raw, wire.Marshal(e))
			}
		}
		// A peer that cannot persist what it acknowledged must not go on:
		// a lost vote or entry breaks Raft's safety, not just its liveness.
		if err := n.d.Store.Save(ctx, iface.Index(first), raw, hs); err != nil {
			panic(fmt.Sprintf("consensus: persist: %v", err))
		}
		if hs != nil {
			must(n.mem.SetHardState(rd.HardState))
		}
		must(n.mem.Append(rd.Entries))
	}

	for _, m := range rd.Messages {
		n.send(m)
	}
	for _, e := range rd.CommittedEntries {
		n.applyEntry(e)
	}
	n.maybeSnapshot(ctx)
	for _, rs := range rd.ReadStates {
		n.readConfirmed(rs)
	}
	n.releaseReads()
	n.rn.Advance(rd)
}

func (n *Node) lastIndex() uint64 {
	i, err := n.mem.LastIndex()
	must(err)
	return i
}

// installSnapshot replaces the log and the FSM with a snapshot from the
// leader. Any entries after it in the store are stale (the library dropped
// its own log), so they are truncated right after.
func (n *Node) installSnapshot(ctx context.Context, snap *pb.Snapshot, hs []byte) {
	at := snap.GetMetadata().GetIndex()
	if err := n.d.Store.SaveSnapshot(ctx, iface.Index(at), wire.Marshal(snap)); err != nil {
		panic(fmt.Sprintf("consensus: persist snapshot: %v", err))
	}
	if err := n.d.Store.Save(ctx, iface.Index(at+1), nil, hs); err != nil {
		panic(fmt.Sprintf("consensus: persist: %v", err))
	}
	must(n.mem.ApplySnapshot(snap))
	if err := n.fsm.Restore(snap.GetData()); err != nil {
		panic(fmt.Sprintf("consensus: restore snapshot at %d: %v", at, err))
	}
	n.applied, n.appliedTerm = at, snap.GetMetadata().GetTerm()
	n.confState = *snap.GetMetadata().GetConfState()
	n.voters = slices.Clone(n.confState.Voters)
	n.sinceSnap = 0
}

func (n *Node) send(m *pb.Message) {
	switch m.GetType() {
	case pb.MsgVoteResp:
		if !m.GetReject() {
			// Granting a vote is contact: give the candidate time to win. A
			// granted pre-vote is not, or the real vote that follows it would
			// meet this peer's own vote lease.
			n.lastHeard = n.d.Clock.Now()
		}
	case pb.MsgSnap:
		// Fire and forget: a lost snapshot makes the follower's next
		// rejection trigger another. Without this the leader would wait on
		// it for good.
		defer n.rn.ReportSnapshot(m.GetTo(), raft.SnapshotFinish)
	}
	n.d.Net.Send(n.cfg.Node(m.GetTo()), iface.Message{From: n.cfg.Node(n.cfg.ID), Kind: wire.KindRaft, Body: wire.Marshal(m)})
}

func (n *Node) applyEntry(e *pb.Entry) {
	switch e.GetType() {
	case pb.EntryNormal:
		if len(e.GetData()) >= 8 {
			id := binary.LittleEndian.Uint64(e.GetData())
			res := n.fsm.Apply(e.GetIndex(), e.GetData()[8:])
			if p, ok := n.proposals[id]; ok && bytes.Equal(p.data, e.GetData()[8:]) {
				delete(n.proposals, id)
				n.later(func() { p.cb(res, nil) })
			}
			n.sinceSnap++
		}
		// An empty entry is a new leader's no-op: it commits the entries of
		// earlier terms, and applying it marks the leader ready.
	case pb.EntryConfChange:
		var cc pb.ConfChange
		must(proto.Unmarshal(e.GetData(), &cc))
		n.confState = *n.rn.ApplyConfChange(&cc)
		n.voters = slices.Clone(n.confState.Voters)
		if p, ok := n.confWait[cc.GetNodeId()]; ok {
			delete(n.confWait, cc.GetNodeId())
			n.later(func() { p.cb(nil, nil) })
		}
		n.sinceSnap++
	default:
		panic(fmt.Sprintf("consensus: entry type %v at %d", e.GetType(), e.GetIndex()))
	}
	n.applied, n.appliedTerm = e.GetIndex(), e.GetTerm()
}

// maybeSnapshot snapshots the FSM at the applied index once SnapshotEvery
// entries have applied. The store drops its entries up to it; the trailing
// ones stay in memory only, so after a restart a lagging follower gets the
// snapshot instead.
func (n *Node) maybeSnapshot(ctx context.Context) {
	if n.sinceSnap < n.cfg.SnapshotEvery {
		return
	}
	snap, err := n.mem.CreateSnapshot(n.applied, &n.confState, n.fsm.Snapshot())
	if err != nil {
		n.d.Log.Error("snapshot", "at", n.applied, "err", err)
		return
	}
	if err := n.d.Store.SaveSnapshot(ctx, iface.Index(n.applied), wire.Marshal(snap)); err != nil {
		panic(fmt.Sprintf("consensus: persist snapshot: %v", err))
	}
	n.sinceSnap = 0
	if first, _ := n.mem.FirstIndex(); n.applied > uint64(n.cfg.Trailing) && n.applied-uint64(n.cfg.Trailing) >= first {
		must(n.mem.Compact(n.applied - uint64(n.cfg.Trailing)))
	}
}

// readConfirmed handles a ReadIndex the library has confirmed with a quorum.
func (n *Node) readConfirmed(rs raft.ReadState) {
	if len(rs.RequestCtx) != 8 {
		return
	}
	id := binary.LittleEndian.Uint64(rs.RequestCtx)
	r, ok := n.reads[id]
	if !ok {
		return
	}
	delete(n.reads, id)
	n.waiting = append(n.waiting, readWait{index: rs.Index, cb: r.cb})
}

func (n *Node) releaseReads() {
	keep := n.waiting[:0]
	for _, w := range n.waiting {
		if w.index <= n.applied {
			n.later(func() { w.cb(nil) })
		} else {
			keep = append(keep, w)
		}
	}
	n.waiting = keep
}

// failAll ends every waiting caller with err.
func (n *Node) failAll(err error) {
	for id, p := range n.proposals {
		delete(n.proposals, id)
		n.later(func() { p.cb(nil, err) })
	}
	for id, p := range n.confWait {
		delete(n.confWait, id)
		n.later(func() { p.cb(nil, err) })
	}
	for id, r := range n.reads {
		delete(n.reads, id)
		n.later(func() { r.cb(err) })
	}
	for _, w := range n.waiting {
		n.later(func() { w.cb(err) })
	}
	n.waiting = nil
}

func (n *Node) notify() {
	s := n.Status()
	if s.Term == n.last.Term && s.Leader == n.last.Leader && s.IsLeader == n.last.IsLeader && s.Ready == n.last.Ready {
		return
	}
	n.last = s
	if n.changed != nil {
		n.changed(s)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// raftLog routes the library's logging to slog at debug level.
type raftLog struct{ l *slog.Logger }

func (r raftLog) Debug(v ...any)              { r.l.Debug(fmt.Sprint(v...)) }
func (r raftLog) Debugf(f string, v ...any)   { r.l.Debug(fmt.Sprintf(f, v...)) }
func (r raftLog) Info(v ...any)               { r.l.Debug(fmt.Sprint(v...)) }
func (r raftLog) Infof(f string, v ...any)    { r.l.Debug(fmt.Sprintf(f, v...)) }
func (r raftLog) Warning(v ...any)            { r.l.Warn(fmt.Sprint(v...)) }
func (r raftLog) Warningf(f string, v ...any) { r.l.Warn(fmt.Sprintf(f, v...)) }
func (r raftLog) Error(v ...any)              { r.l.Error(fmt.Sprint(v...)) }
func (r raftLog) Errorf(f string, v ...any)   { r.l.Error(fmt.Sprintf(f, v...)) }
func (r raftLog) Fatal(v ...any)              { panic(fmt.Sprint(v...)) }
func (r raftLog) Fatalf(f string, v ...any)   { panic(fmt.Sprintf(f, v...)) }
func (r raftLog) Panic(v ...any)              { panic(fmt.Sprint(v...)) }
func (r raftLog) Panicf(f string, v ...any)   { panic(fmt.Sprintf(f, v...)) }

// snapshotter is the library's view of the log. Its Snapshot is the one the
// leader sends a follower that fell behind the compacted log. The stored
// snapshot can predate a membership change, and the library refuses a
// snapshot whose membership lacks the receiver, so a peer added after it
// would never catch up: when the membership moved on, a fresh snapshot of
// the applied state is cut instead.
type snapshotter struct {
	*raft.MemoryStorage
	n *Node
}

func (s snapshotter) Snapshot() (*pb.Snapshot, error) {
	snap, err := s.MemoryStorage.Snapshot()
	if err != nil || proto.Equal(snap.GetMetadata().GetConfState(), &s.n.confState) {
		return snap, err
	}
	return s.n.mem.CreateSnapshot(s.n.applied, &s.n.confState, s.n.fsm.Snapshot())
}
