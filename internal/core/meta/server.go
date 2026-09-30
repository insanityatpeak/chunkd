package meta

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/consensus"
	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/core/placement"
	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// Deps are the environment the metadata server runs in.
type Deps struct {
	Clock iface.Clock
	Net   iface.Transport
	Store iface.MetaStore
	Rand  iface.Rand
	Log   *slog.Logger
}

// Config sets replication and timing.
type Config struct {
	ID iface.NodeID
	// Peers are the metadata group's members, this server included; Raft ID i
	// is Peers[i-1]. Empty means a group of one.
	Peers         []iface.NodeID
	Replicas      int
	MinReplicas   int
	ChunkSize     int
	Detector      detector.Config
	Repair        repair.Config
	SnapshotEvery int
	// Consensus timing (see consensus.Config).
	TickEvery, ElectionTimeout, RequestTimeout time.Duration
	// Trailing is how many log entries before a snapshot stay in memory for
	// followers that are only a little behind.
	Trailing int
	// EpochEvery is how often the server logs an AdvanceEpoch. Retired
	// versions are dropped RetainEpochs epochs after retirement, so undelete
	// works for between (RetainEpochs-1)×EpochEvery and RetainEpochs×EpochEvery.
	EpochEvery   time.Duration
	RetainEpochs int
	// LeaseEpochs: a pending upload with no claim for this many epochs is
	// aborted, so one idle for less than (LeaseEpochs-1)×EpochEvery never
	// is. GCGrace: an unreferenced copy is deleted only after it has been
	// unreferenced this long (ADR-0016). The lease outlasts the grace plus
	// two sweeps, so a stall GC notices never costs the upload its lease.
	LeaseEpochs int
	GCGrace     time.Duration
}

// DefaultConfig is N=3, commit at 2 (ADR-0007), 4 MiB chunks (ADR-0005),
// suspect after 3 s and dead after 10 s of silence (ADR-0010), 30 s epochs
// with 3 of retention (ADR-0016).
// SIMPLIFIED: demo-scale retention (60–90 s). S3 versioning keeps
// noncurrent versions until a lifecycle rule expires them, typically days.
func DefaultConfig(id iface.NodeID) Config {
	rc := consensus.DefaultConfig(1, nil, nil)
	return Config{ID: id, Replicas: 3, MinReplicas: 2, ChunkSize: chunk.DefaultSize, Detector: detector.DefaultConfig(), Repair: repair.DefaultConfig(), SnapshotEvery: 1000,
		TickEvery: rc.TickEvery, ElectionTimeout: rc.ElectionTimeout, RequestTimeout: rc.RequestTimeout, Trailing: rc.Trailing,
		EpochEvery: 30 * time.Second, RetainEpochs: 3, LeaseEpochs: 6, GCGrace: time.Minute}
}

// Server is one metadata peer. Everything runs on its event loop. The
// namespace (State) is the Raft FSM: every mutation is proposed, and takes
// effect on every peer when the log applies it. Chunk locations, node
// liveness and the repair and GC schedulers are soft state each peer keeps
// for itself; only the leader acts on them.
type Server struct {
	d       Deps
	cfg     Config
	state   *State
	cluster *Cluster
	repair  *repair.Scheduler
	raft    *consensus.Node
	// leading: this peer is a ready leader (as of the last change), and the
	// soft state below belongs to its term.
	leading bool
	stopped bool
	// seenTerm and seenLeader dedupe the timeline's election events.
	seenTerm, seenLeader uint64
	events               events
	// corruptReplicas counts copies removed after failing verification.
	corruptReplicas uint64
	// dedupSkipped counts chunk bytes clients did not send because a claim
	// found them present. Not durable: it restarts at 0.
	dedupSkipped uint64
	gc           gcState
	// drift is the latest reconciliation's result.
	drift Drift
}

// NewServer recovers durable state: the latest snapshot, then every WAL
// entry after it.
func NewServer(ctx context.Context, d Deps, cfg Config) (*Server, error) {
	if d.Clock == nil || d.Net == nil || d.Store == nil || d.Rand == nil || d.Log == nil {
		panic("meta: missing dependency")
	}
	if cfg.EpochEvery <= 0 || cfg.RetainEpochs < 1 {
		return nil, fmt.Errorf("meta: epoch every %v, retain %d epochs: both must be positive", cfg.EpochEvery, cfg.RetainEpochs)
	}
	peers := cfg.Peers
	if len(peers) == 0 {
		peers = []iface.NodeID{cfg.ID}
	}
	self := slices.Index(peers, cfg.ID) + 1
	if self == 0 {
		return nil, fmt.Errorf("meta: %s is not among its peers %v", cfg.ID, peers)
	}
	s := &Server{d: d, cfg: cfg, state: New(), cluster: NewCluster(cfg.Detector), gc: newGCState()}
	rc := cfg.Repair
	rc.Replicas = cfg.Replicas
	s.repair = repair.New(rc, d.Clock, repairView{s}, repair.Sender{Copy: s.sendCopy, Trim: s.sendTrim, Done: s.copyDone, Trimmed: s.trimmed})
	s.repair.SetActive(false)
	ids := make([]uint64, len(peers))
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	cc := consensus.DefaultConfig(uint64(self), func(id uint64) iface.NodeID { return peers[id-1] }, ids)
	cc.TickEvery, cc.ElectionTimeout, cc.RequestTimeout = cfg.TickEvery, cfg.ElectionTimeout, cfg.RequestTimeout
	cc.SnapshotEvery, cc.Trailing = cfg.SnapshotEvery, cfg.Trailing
	// Recovery: the snapshot restores the state, then every committed entry
	// after it applies through fsm.Apply before New returns.
	raft, err := consensus.New(ctx, consensus.Deps{Clock: d.Clock, Net: d.Net, Store: d.Store, Rand: d.Rand, Log: d.Log}, cc, fsm{s})
	if err != nil {
		return nil, err
	}
	s.raft = raft
	s.raft.OnChange(s.leadership)
	st := s.raft.Status()
	d.Log.Info("metadata recovered", "applied", st.Applied, "term", st.Term, "files", len(s.state.List("/")))
	return s, nil
}

// applied is what the FSM returns for one entry: the op's result, or the
// error that rejected it, identically on every peer.
type applied struct {
	res Result
	err error
}

// fsm adapts State to consensus.FSM.
type fsm struct{ s *Server }

func (f fsm) Apply(_ uint64, data []byte) any {
	var op chunkdv1.Op
	if err := proto.Unmarshal(data, &op); err != nil {
		return applied{err: iface.Errorf(iface.CodeInternal, "undecodable log entry: %v", err)}
	}
	res, err := f.s.state.Apply(&op)
	return applied{res, err}
}

func (f fsm) Snapshot() []byte { return f.s.state.Snapshot() }

func (f fsm) Restore(data []byte) error {
	st, err := Restore(data)
	if err != nil {
		return err
	}
	f.s.state = st
	return nil
}

// leadership runs when the peer's role changes. Everything only a leader may
// do (repair, GC, the epoch timer) hangs off s.leading, and its soft state
// starts empty on every new term: a deposed leader's queue and pending
// deletes describe a view that is no longer authoritative.
func (s *Server) leadership(st consensus.Status) {
	if len(s.cfg.Peers) > 1 && (st.Term != s.seenTerm || st.Leader != s.seenLeader) {
		s.seenTerm, s.seenLeader = st.Term, st.Leader
		s.event("raft", st.LeaderNode, "term %d, leader %q", st.Term, string(st.LeaderNode))
	}
	if st.Ready == s.leading {
		return
	}
	s.leading = st.Ready
	if st.Ready {
		s.gc = newGCState()
		s.repair.SetActive(true)
		return
	}
	s.repair.SetActive(false)
}

// Stop ends this peer's timers and consensus activity. A restart builds a new
// Server on the same store; the old one must not keep writing to it.
func (s *Server) Stop() {
	s.stopped = true
	s.leading = false
	s.repair.SetActive(false)
	s.raft.Stop()
}

// Raft returns the peer's view of the metadata group.
func (s *Server) Raft() consensus.Status { return s.raft.Status() }

// Start registers handlers.
func (s *Server) Start() {
	s.d.Net.Listen(s.cfg.ID, s.handle)
	for kind, h := range map[string]iface.RPCHandler{
		wire.KindBegin:    s.begin,
		wire.KindClaim:    s.claim,
		wire.KindCommit:   s.commit,
		wire.KindAbort:    s.abort,
		wire.KindDelete:   s.delete,
		wire.KindUndelete: s.undelete,
		wire.KindStat:     s.stat,
		wire.KindList:     s.list,
		wire.KindLog:      s.log,
		wire.KindCluster:  s.clusterInfo,
		wire.KindSuspect:  s.suspect,
	} {
		s.d.Net.Serve(s.cfg.ID, kind, h, iface.ServeOpts{})
	}
	s.d.Clock.AfterFunc(s.cfg.Detector.TickEvery, s.tick)
	s.d.Clock.AfterFunc(s.cfg.EpochEvery, s.advanceEpoch)
	s.repair.Start()
	s.raft.Start()
}

// advanceEpoch logs the next epoch. The timer only proposes it; what the
// epoch drops is decided by applying the logged op, identically everywhere.
func (s *Server) advanceEpoch() {
	if s.stopped {
		return
	}
	s.d.Clock.AfterFunc(s.cfg.EpochEvery, s.advanceEpoch)
	// Only the leader proposes; every peer applies. A follower's timer is
	// idle, and a leader elected between ticks proposes at its next one.
	if !s.leading {
		return
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_AdvanceEpoch{AdvanceEpoch: &chunkdv1.AdvanceEpochOp{
		RetainEpochs: uint32(s.cfg.RetainEpochs), LeaseEpochs: uint32(s.cfg.LeaseEpochs)}}}, func(res Result, err error) {
		if err != nil {
			s.d.Log.Error("advance epoch", "err", err)
			return
		}
		if res.Dropped > 0 {
			s.event("gc", "", "epoch %d: %d versions past retention dropped", s.state.Epoch(), res.Dropped)
		}
		if res.Expired > 0 {
			s.event("gc", "", "epoch %d: %d idle uploads expired, their claims released", s.state.Epoch(), res.Expired)
		}
		// Only if still the leader that proposed it: a deposed one's view of
		// what is orphaned is not authoritative.
		if s.leading {
			s.collect()
			s.reconcile()
		}
	})
}

// propose validates op against the applied state, replicates it, and calls
// done with its result once every peer's log has it applied here. Not the
// leader: CodeNotLeader, checked first, since a stale peer's validation
// verdict means nothing. Unavailable means the outcome is unknown (the
// entry may still commit), so callers retry with idempotent requests.
func (s *Server) propose(op *chunkdv1.Op, done func(Result, error)) {
	if st := s.raft.Status(); !st.Ready {
		done(Result{}, st.NotLeader())
		return
	}
	// Fails fast on what can never apply. Ops proposed together can still
	// invalidate each other; Apply rejects the loser identically everywhere.
	if err := s.state.Validate(op); err != nil {
		done(Result{}, err)
		return
	}
	s.raft.Propose(wire.Marshal(op), func(v any, err error) {
		if err != nil {
			done(Result{}, err)
			return
		}
		a := v.(applied)
		done(a.res, a.err)
	})
}

// requireLeader answers a request that only the leader may serve.
func (s *Server) requireLeader(respond iface.Responder) bool {
	if st := s.raft.Status(); !st.Ready {
		respond(nil, st.NotLeader())
		return false
	}
	return true
}

// linearize runs f once this peer, still the leader by a quorum's
// confirmation, has applied everything committed when the read began
// (read-index). No clock is consulted: a paused leader cannot serve a stale
// read.
func (s *Server) linearize(respond iface.Responder, f func()) {
	s.raft.ReadIndex(func(err error) {
		if err != nil {
			respond(nil, err)
			return
		}
		f()
	})
}

// reconcile recounts refcounts and claims each epoch. Drift is alarmed on
// (metric, error log, timeline) and never auto-corrected: it means an op
// applied wrongly, and rewriting counts would hide the bug and could let GC
// delete a referenced chunk.
func (s *Server) reconcile() {
	s.drift = s.state.Reconcile()
	if s.drift.Empty() {
		return
	}
	s.d.Log.Error("refcount drift", "chunks", len(s.drift.Refcounts), "claims", len(s.drift.Claims))
	s.event("gc", "", "refcount drift on %d chunks, claim drift on %d: GC should be stopped and the log inspected", len(s.drift.Refcounts), len(s.drift.Claims))
}

// Drift returns the latest reconciliation result.
func (s *Server) Drift() Drift { return s.drift }

func (s *Server) tick() {
	if s.stopped {
		return
	}
	trs := s.cluster.Tick(s.d.Clock.Now())
	for _, tr := range trs {
		s.transition(tr)
	}
	if len(trs) > 0 {
		s.repair.Scan()
	}
	s.d.Clock.AfterFunc(s.cfg.Detector.TickEvery, s.tick)
}

func (s *Server) transition(tr detector.Transition) {
	s.nodeEvent(tr)
	if tr.From == 0 {
		s.d.Log.Info("node joined", "node", tr.Node)
		return
	}
	s.d.Log.Info("node state", "node", tr.Node, "from", tr.From.String(), "to", tr.To.String(), "restarted", tr.Restarted)
}

// DedupSkipped returns the chunk bytes clients skipped sending since start.
func (s *Server) DedupSkipped() uint64 { return s.dedupSkipped }

// State exposes the state machine for tests and invariants. Loop-owned.
func (s *Server) State() *State { return s.state }

// Cluster exposes the node view. Loop-owned.
func (s *Server) Cluster() *Cluster { return s.cluster }

// Applied returns the index of the last applied log entry.
func (s *Server) Applied() iface.Index { return iface.Index(s.raft.Status().Applied) }

func (s *Server) handle(m iface.Message) {
	if s.stopped {
		return
	}
	switch m.Kind {
	case wire.KindRaft:
		s.raft.Receive(m.From, m.Body)
	case wire.KindHeartbeat:
		var hb chunkdv1.Heartbeat
		if err := wire.Decode(m.Body, &hb); err != nil {
			s.d.Log.Warn("bad heartbeat", "from", m.From, "err", err)
			return
		}
		id := iface.NodeID(hb.GetNode())
		tr, changed, need := s.cluster.Heartbeat(NodeState{ID: id, Rack: hb.GetRack(), Addr: hb.GetAddr(), Used: hb.GetUsedBytes(),
			Chunks: hb.GetChunkCount(), Draining: hb.GetDraining(), Corrupt: hb.GetCorrupt(), ScrubDone: hb.GetScrubDone(),
			ScrubTotal: hb.GetScrubTotal(), ScrubPasses: hb.GetScrubPasses()}, detector.Beat{Incarnation: hb.GetIncarnation(), Seq: hb.GetSeq()}, s.d.Clock.Now())
		if changed {
			s.transition(tr)
			s.repair.Scan()
		}
		s.d.Net.Send(m.From, iface.Message{From: s.cfg.ID, Kind: wire.KindHeartbeatAck,
			Body: wire.Marshal(&chunkdv1.HeartbeatAck{Seq: hb.GetSeq(), NeedFullReport: need})})
	case wire.KindBlockReport:
		var r chunkdv1.BlockReport
		if err := wire.Decode(m.Body, &r); err != nil {
			s.d.Log.Warn("bad block report", "from", m.From, "err", err)
			return
		}
		ids := make([]iface.ChunkID, 0, len(r.GetChunkIds()))
		for _, raw := range r.GetChunkIds() {
			if id, err := wire.ChunkID(raw); err == nil {
				ids = append(ids, id)
			}
		}
		node := iface.NodeID(r.GetNode())
		var deleted []iface.ChunkID
		for _, raw := range r.GetDeletedIds() {
			if id, err := wire.ChunkID(raw); err == nil {
				deleted = append(deleted, id)
			}
		}
		var corrupt []iface.ChunkID
		for _, raw := range r.GetCorruptIds() {
			if id, err := wire.ChunkID(raw); err == nil {
				corrupt = append(corrupt, id)
			}
		}
		if r.GetFull() {
			s.verifyReport(node, ids)
		}
		// A quarantined copy is gone as far as locations go: same path as a
		// delete, so report ordering applies to it too.
		added, removed, ok := s.cluster.Report(node, Report{Incarnation: r.GetIncarnation(), Seq: r.GetSeq(), Full: r.GetFull(), Added: ids,
			Deleted: append(deleted, corrupt...)})
		if !ok {
			return
		}
		if len(corrupt) > 0 {
			s.corrupted(node, corrupt, removed)
		}
		for _, id := range deleted {
			s.gcAcked(id, node, false)
		}
		for _, raw := range r.GetKeptIds() {
			if id, err := wire.ChunkID(raw); err == nil {
				s.gcAcked(id, node, true)
			}
		}
		// Only changes the report actually made: a reordered, older report
		// must not complete a copy or a trim.
		s.repair.Removed(node, removed)
		s.repair.Reported(node, added)
		if r.GetFull() {
			// A full report can restore replicas a scan counted as missing.
			s.repair.Scan()
		}
	case wire.KindReplicateFailed:
		var f chunkdv1.ReplicateFailed
		if err := wire.Decode(m.Body, &f); err != nil {
			return
		}
		id, err := wire.ChunkID(f.GetChunkId())
		if err != nil {
			return
		}
		s.d.Log.Warn("repair copy failed", "copy", f.GetCopyId(), "node", f.GetNode(), "err", f.GetError())
		s.repair.Failed(f.GetCopyId(), id)
	}
}

func (s *Server) replica(id iface.NodeID) *chunkdv1.Replica {
	n, _ := s.cluster.Node(id)
	return &chunkdv1.Replica{Node: string(id), Addr: n.Addr, Suspect: n.State == detector.Suspect}
}

func (s *Server) begin(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.BeginUploadRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	if req.GetSize() < 0 {
		respond(nil, iface.Errorf(iface.CodeInvalid, "negative size"))
		return
	}
	n := chunk.Count(req.GetSize(), s.cfg.ChunkSize)
	sizes := make([]int64, n)
	for i := range sizes {
		sizes[i] = chunk.SizeOf(req.GetSize(), s.cfg.ChunkSize, i)
	}
	pl, err := placement.Place(s.cluster.PlacementView(), sizes, s.cfg.Replicas, s.cfg.MinReplicas, s.d.Rand)
	if err != nil {
		respond(nil, iface.Errorf(iface.CodeUnavailable, "%v (need %d)", err, s.cfg.MinReplicas))
		return
	}
	op := &chunkdv1.BeginUploadOp{Path: req.GetPath(), ExpectedVersion: req.GetExpectedVersion(), Size: req.GetSize(), ChunkSize: int32(s.cfg.ChunkSize),
		Claims: true, LastWriterWins: req.GetLastWriterWins()}
	resp := &chunkdv1.BeginUploadResponse{ChunkSize: int32(s.cfg.ChunkSize), MinReplicas: int32(s.cfg.MinReplicas)}
	for _, nodes := range pl {
		r := &chunkdv1.Replicas{}
		cp := &chunkdv1.ChunkPlacement{}
		for _, id := range nodes {
			r.Nodes = append(r.Nodes, string(id))
			cp.Replicas = append(cp.Replicas, s.replica(id))
		}
		op.Placement = append(op.Placement, r)
		resp.Placement = append(resp.Placement, cp)
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_Begin{Begin: op}}, func(res Result, err error) {
		resp.UploadId = res.UploadID
		wire.Respond(respond, resp, err)
	})
}

// claim logs that an upload will reference these chunks, then tells the
// client which already have MinReplicas copies on alive nodes so it can skip
// sending them. The claim is logged before any copy is written, so GC marks
// the chunk for the whole life of the upload (ADR-0015).
// SIMPLIFIED: dedup is global. Cross-user dedup lets an uploader probe
// whether content exists; Dropbox moved to per-user dedup after that was
// shown in 2011, and a multi-tenant chunkd would salt chunk IDs per tenant.
func (s *Server) claim(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.ClaimChunksRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_Claim{Claim: &chunkdv1.ClaimChunksOp{UploadId: req.GetUploadId(), Claims: req.GetClaims()}}}, func(_ Result, err error) {
		if err != nil {
			respond(nil, err)
			return
		}
		u, ok := s.state.Upload(req.GetUploadId())
		if !ok {
			// Committed or aborted between applying the claim and answering it.
			respond(nil, iface.Errorf(iface.CodeNotFound, "upload %d", req.GetUploadId()))
			return
		}
		resp := &chunkdv1.ClaimChunksResponse{}
		for _, cl := range req.GetClaims() {
			id, _ := wire.ChunkID(cl.GetId())
			live := s.liveLocations(id)
			present := len(live) >= s.cfg.MinReplicas
			loc := &chunkdv1.ChunkPlacement{}
			if present {
				for _, n := range live {
					loc.Replicas = append(loc.Replicas, s.replica(n))
				}
				s.dedupSkipped += uint64(chunk.SizeOf(u.Size, u.ChunkSize, int(cl.GetIndex())))
			}
			resp.Present = append(resp.Present, present)
			resp.Locations = append(resp.Locations, loc)
		}
		wire.Respond(respond, resp, nil)
	})
}

// commit publishes a version once every chunk has at least MinReplicas
// reported locations on live nodes. Locations come from the nodes' own
// block reports, not from the client's word.
func (s *Server) commit(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.CommitUploadRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	// A retried or duplicated commit of an upload that already committed
	// succeeds with the same version: the client cannot tell a lost response
	// from a failed commit, so commit must be idempotent.
	if c, ok := s.state.CommittedUpload(req.GetUploadId()); ok {
		respond(wire.Marshal(&chunkdv1.CommitUploadResponse{Version: c.Version}), nil)
		return
	}
	op := &chunkdv1.Op{Op: &chunkdv1.Op_Commit{Commit: &chunkdv1.CommitUploadOp{UploadId: req.GetUploadId(), ChunkIds: req.GetChunkIds(), Sha256: req.GetSha256()}}}
	if err := s.state.Validate(op); err != nil {
		respond(nil, err)
		return
	}
	for i, raw := range req.GetChunkIds() {
		id, _ := wire.ChunkID(raw)
		if got := len(s.liveLocations(id)); got < s.cfg.MinReplicas {
			// Retry, not failure: incremental reports may still be in flight.
			respond(nil, iface.Errorf(iface.CodeRetry, "chunk %d has %d of %d required replicas reported", i, got, s.cfg.MinReplicas))
			return
		}
	}
	s.propose(op, func(res Result, err error) {
		if err != nil {
			// A retry racing its own first attempt: the first applied, so
			// this one found the upload gone. Same answer as the fast path.
			if c, ok := s.state.CommittedUpload(req.GetUploadId()); ok && iface.CodeOf(err) != iface.CodeNotLeader && iface.CodeOf(err) != iface.CodeUnavailable {
				respond(wire.Marshal(&chunkdv1.CommitUploadResponse{Version: c.Version}), nil)
				return
			}
			respond(nil, err)
			return
		}
		ids := make([]iface.ChunkID, 0, len(req.GetChunkIds()))
		for _, raw := range req.GetChunkIds() {
			if id, err := wire.ChunkID(raw); err == nil {
				ids = append(ids, id)
			}
		}
		s.repair.Fresh(ids)
		respond(wire.Marshal(&chunkdv1.CommitUploadResponse{Version: res.Version}), nil)
	})
}

// liveLocations are replicas that count toward commit: alive nodes only. A
// suspect node may already be dead, and an acknowledged upload promises
// MinReplicas copies on nodes believed alive.
// A copy with a GC delete in flight does not count either: it may be gone
// by the time the version is read.
func (s *Server) liveLocations(id iface.ChunkID) []iface.NodeID {
	return slices.DeleteFunc(s.cluster.Locations(id), func(n iface.NodeID) bool { return !s.cluster.Alive(n) || s.gcPending(id, n) })
}

// readLocations are replicas a reader may try: alive first, then suspect.
func (s *Server) readLocations(id iface.ChunkID) []iface.NodeID {
	locs := slices.DeleteFunc(s.cluster.Locations(id), func(n iface.NodeID) bool { return !s.cluster.Readable(n) })
	slices.SortStableFunc(locs, func(a, b iface.NodeID) int {
		return cmp.Compare(s.cluster.state(a), s.cluster.state(b))
	})
	return locs
}

func (s *Server) abort(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.AbortUploadRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_Abort{Abort: &chunkdv1.AbortUploadOp{UploadId: req.GetUploadId()}}}, func(_ Result, err error) {
		wire.Respond(respond, &chunkdv1.AbortUploadResponse{}, err)
	})
}

func (s *Server) delete(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.DeleteRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	// Idempotent for a known version: if that exact version was already
	// tombstoned, this is a retry of our own delete.
	e := req.GetExpectedVersion()
	if e != 0 {
		if v, ok := s.state.Tombstoned(req.GetPath(), e); ok {
			respond(wire.Marshal(&chunkdv1.DeleteResponse{Version: v}), nil)
			return
		}
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_Delete{Delete: &chunkdv1.DeleteOp{Path: req.GetPath(), ExpectedVersion: e}}}, func(res Result, err error) {
		if err != nil && e != 0 && notOutcomeUnknown(err) {
			// A retry racing its own first attempt.
			if v, ok := s.state.Tombstoned(req.GetPath(), e); ok {
				respond(wire.Marshal(&chunkdv1.DeleteResponse{Version: v}), nil)
				return
			}
		}
		wire.Respond(respond, &chunkdv1.DeleteResponse{Version: res.Version}, err)
	})
}

// notOutcomeUnknown: err is the verdict of an applied op, not a failure to
// learn one. After NotLeader or Unavailable the op may or may not have applied.
func notOutcomeUnknown(err error) bool {
	c := iface.CodeOf(err)
	return c != iface.CodeNotLeader && c != iface.CodeUnavailable
}

// undelete restores a retained version as the newest one. Like delete it is
// conditional on the live version, and a retry finds its own result.
func (s *Server) undelete(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.UndeleteRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	if req.GetVersion() == 0 {
		respond(nil, iface.Errorf(iface.CodeInvalid, "undelete needs a version"))
		return
	}
	if v, ok := s.state.Restored(req.GetPath(), req.GetVersion(), req.GetExpectedVersion()); ok {
		respond(wire.Marshal(&chunkdv1.UndeleteResponse{Version: v}), nil)
		return
	}
	s.propose(&chunkdv1.Op{Op: &chunkdv1.Op_Undelete{Undelete: &chunkdv1.UndeleteOp{Path: req.GetPath(), Version: req.GetVersion(), ExpectedVersion: req.GetExpectedVersion()}}}, func(res Result, err error) {
		if err != nil && notOutcomeUnknown(err) {
			if v, ok := s.state.Restored(req.GetPath(), req.GetVersion(), req.GetExpectedVersion()); ok {
				respond(wire.Marshal(&chunkdv1.UndeleteResponse{Version: v}), nil)
				return
			}
		}
		wire.Respond(respond, &chunkdv1.UndeleteResponse{Version: res.Version}, err)
	})
}

func (s *Server) stat(m iface.Message, respond iface.Responder) {
	var req chunkdv1.StatRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	s.linearize(respond, func() { s.statNow(&req, respond) })
}

func (s *Server) statNow(req *chunkdv1.StatRequest, respond iface.Responder) {
	v, err := s.state.StatVersion(req.GetPath(), req.GetVersion())
	if err != nil {
		respond(nil, err)
		return
	}
	resp := &chunkdv1.StatResponse{Path: req.GetPath(), Version: v.V, Size: v.Size, Sha256: v.SHA256[:], ChunkSize: int32(v.ChunkSize)}
	for i, id := range v.Chunks {
		loc := &chunkdv1.ChunkLocation{Id: id[:], Size: chunk.SizeOf(v.Size, v.ChunkSize, i)}
		for _, n := range s.readLocations(id) {
			loc.Replicas = append(loc.Replicas, s.replica(n))
		}
		resp.Chunks = append(resp.Chunks, loc)
	}
	respond(wire.Marshal(resp), nil)
}

func (s *Server) list(m iface.Message, respond iface.Responder) {
	var req chunkdv1.ListRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	s.linearize(respond, func() {
		resp := &chunkdv1.ListResponse{}
		for _, e := range s.state.List(req.GetPrefix()) {
			resp.Files = append(resp.Files, &chunkdv1.FileInfo{Path: e.Path, Version: e.V, Size: e.Size, Sha256: e.SHA256[:], ChunkCount: int32(len(e.Chunks))})
		}
		respond(wire.Marshal(resp), nil)
	})
}

func (s *Server) log(m iface.Message, respond iface.Responder) {
	var req chunkdv1.LogRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	s.linearize(respond, func() {
		vs, err := s.state.Log(req.GetPath())
		if err != nil {
			respond(nil, err)
			return
		}
		resp := &chunkdv1.LogResponse{Epoch: s.state.Epoch()}
		for _, v := range vs {
			vi := &chunkdv1.VersionInfo{Version: v.V, Size: v.Size, ChunkCount: int32(len(v.Chunks)), Tombstone: v.Tombstone, Retired: v.Retired}
			if v.Retired {
				vi.ExpiresEpoch = v.RetiredAt + uint64(s.cfg.RetainEpochs)
			}
			if !v.Tombstone {
				vi.Sha256 = v.SHA256[:]
			}
			resp.Versions = append(resp.Versions, vi)
		}
		respond(wire.Marshal(resp), nil)
	})
}

// clusterInfo serves the dashboard's view. Leader only, without a read-index
// round: it is polled about once a second and shows soft state (liveness,
// locations, this peer's timeline) that is not linearizable anyway.
func (s *Server) clusterInfo(m iface.Message, respond iface.Responder) {
	if !s.requireLeader(respond) {
		return
	}
	var req chunkdv1.ClusterRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	respond(wire.Marshal(s.ClusterView(req.GetEventsAfter())), nil)
}

// ClusterView is everything the dashboard shows: nodes, replication health
// per chunk and per file, copies in flight, and events after eventsAfter.
// The sim reads it directly; real mode reads it through the cluster RPC.
// O(chunks); called about once a second.
func (s *Server) ClusterView(eventsAfter uint64) *chunkdv1.ClusterResponse {
	now := s.d.Clock.Now()
	ms := func(t iface.Instant) int64 { return int64(t.Sub(0) / time.Millisecond) }
	resp := &chunkdv1.ClusterResponse{NowMs: ms(now)}
	for _, n := range s.cluster.Nodes() {
		resp.Nodes = append(resp.Nodes, &chunkdv1.NodeInfo{Id: string(n.ID), Rack: n.Rack, Addr: n.Addr, UsedBytes: n.Used,
			ChunkCount: n.Chunks, Alive: s.cluster.Alive(n.ID), Draining: n.Draining, State: n.State.String(),
			HeartbeatAgeMs: int64(now.Sub(n.LastSeen) / time.Millisecond), Corrupt: n.Corrupt, ScrubDone: n.ScrubDone,
			ScrubTotal: n.ScrubTotal, ScrubPasses: n.ScrubPasses})
	}
	for _, e := range s.state.List("/") {
		resp.Files++
		resp.LogicalBytes += e.Size
		fh := &chunkdv1.FileHealth{Path: e.Path, Chunks: int32(len(e.Chunks)), MinLive: int32(s.cfg.Replicas)}
		for _, id := range e.Chunks {
			live := int32(len(s.liveLocations(id)))
			fh.MinLive = min(fh.MinLive, live)
			if live < int32(s.cfg.Replicas) {
				fh.UnderReplicated++
			}
		}
		resp.FileHealth = append(resp.FileHealth, fh)
	}
	for _, e := range s.state.Deleted("/") {
		resp.Deleted = append(resp.Deleted, &chunkdv1.DeletedFile{Path: e.Path, Version: e.V, Size: e.Size,
			ExpiresEpoch: e.RetiredAt + uint64(s.cfg.RetainEpochs)})
	}
	resp.ReferencedBytes, resp.DistinctBytes = s.state.Dedup()
	resp.Epoch = s.state.Epoch()
	resp.Gc = &chunkdv1.GCStats{Orphans: s.gc.stats.Orphans, Sent: s.gc.stats.Sent, Deleted: s.gc.stats.Deleted, Kept: s.gc.stats.Kept,
		Drift: uint64(len(s.drift.Refcounts) + len(s.drift.Claims)), RetainEpochs: uint32(s.cfg.RetainEpochs),
		EpochEveryMs: int64(s.cfg.EpochEvery / time.Millisecond)}
	h := s.Health()
	resp.Health = &chunkdv1.ClusterHealth{Chunks: int64(h.Chunks), UnderReplicated: int64(h.UnderReplicated), OverReplicated: int64(h.OverReplicated),
		Lost: int64(h.Lost), RepairQueued: int64(h.Repair.Queued), RepairInFlight: int64(h.Repair.InFlight), RepairWaiting: int64(h.Repair.Waiting),
		RepairCompleted: h.Repair.Completed, RepairBytes: h.Repair.Bytes, RepairTrimmed: h.Repair.Trimmed, RepairTimedOut: h.Repair.TimedOut,
		RepairFailed: h.Repair.Failed, DetectorStalls: h.DetectorStalls, CorruptReplicas: h.CorruptReplicas}
	for _, n := range h.Replicas {
		resp.Health.Replicas = append(resp.Health.Replicas, int64(n))
	}
	for _, c := range s.repair.InFlight() {
		resp.Copies = append(resp.Copies, &chunkdv1.RepairCopy{Id: c.ID, ChunkId: c.Chunk[:], Source: string(c.Source), Target: string(c.Target),
			Bytes: c.Size, StartedMs: ms(c.Started)})
	}
	evs, latest := s.Events(eventsAfter)
	resp.EventSeq = latest
	for _, e := range evs {
		resp.Events = append(resp.Events, &chunkdv1.Event{Seq: e.Seq, AtMs: ms(e.At), Kind: e.Kind, Node: string(e.Node), Text: e.Text})
	}
	return resp
}
