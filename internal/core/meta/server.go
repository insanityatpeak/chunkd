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
	ID            iface.NodeID
	Replicas      int
	MinReplicas   int
	ChunkSize     int
	Detector      detector.Config
	Repair        repair.Config
	SnapshotEvery int
	// EpochEvery is how often the server logs an AdvanceEpoch. Retired
	// versions are dropped RetainEpochs epochs after retirement, so undelete
	// works for between (RetainEpochs-1)×EpochEvery and RetainEpochs×EpochEvery.
	EpochEvery   time.Duration
	RetainEpochs int
	// LeaseEpochs: a pending upload with no claim for this many epochs is
	// aborted. GCGrace: an unreferenced copy is deleted only after it has
	// been unreferenced this long (ADR-0016).
	LeaseEpochs int
	GCGrace     time.Duration
}

// DefaultConfig is N=3, commit at 2 (ADR-0007), 4 MiB chunks (ADR-0005),
// suspect after 3 s and dead after 10 s of silence (ADR-0010), 30 s epochs
// with 3 of retention (ADR-0016).
// SIMPLIFIED: demo-scale retention (60–90 s). S3 versioning keeps
// noncurrent versions until a lifecycle rule expires them, typically days.
func DefaultConfig(id iface.NodeID) Config {
	return Config{ID: id, Replicas: 3, MinReplicas: 2, ChunkSize: chunk.DefaultSize, Detector: detector.DefaultConfig(), Repair: repair.DefaultConfig(), SnapshotEvery: 1000,
		EpochEvery: 30 * time.Second, RetainEpochs: 3, LeaseEpochs: 4, GCGrace: time.Minute}
}

// Server is the metadata server. Everything runs on its event loop.
type Server struct {
	d         Deps
	cfg       Config
	state     *State
	cluster   *Cluster
	repair    *repair.Scheduler
	applied   iface.Index
	sinceSnap int
	events    events
	// corruptReplicas counts copies removed after failing verification.
	corruptReplicas uint64
	// dedupSkipped counts chunk bytes clients did not send because a claim
	// found them present. Not durable: it restarts at 0.
	dedupSkipped uint64
	gc           gcState
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
	at, snap, err := d.Store.LoadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	state, err := Restore(snap)
	if err != nil {
		return nil, fmt.Errorf("restore snapshot at %d: %w", at, err)
	}
	s := &Server{d: d, cfg: cfg, state: state, cluster: NewCluster(cfg.Detector), applied: at, gc: newGCState()}
	rc := cfg.Repair
	rc.Replicas = cfg.Replicas
	s.repair = repair.New(rc, d.Clock, repairView{s}, repair.Sender{Copy: s.sendCopy, Trim: s.sendTrim, Done: s.copyDone, Trimmed: s.trimmed})
	err = d.Store.Replay(ctx, at+1, func(i iface.Index, b []byte) error {
		var op chunkdv1.Op
		if err := proto.Unmarshal(b, &op); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		s.state.Apply(&op)
		s.applied = i
		return nil
	})
	if err != nil {
		return nil, err
	}
	d.Log.Info("metadata recovered", "snapshot", at, "applied", s.applied, "files", len(s.state.List("/")))
	return s, nil
}

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
}

// advanceEpoch logs the next epoch. The timer only proposes it; what the
// epoch drops is decided by applying the logged op, identically everywhere.
func (s *Server) advanceEpoch() {
	s.d.Clock.AfterFunc(s.cfg.EpochEvery, s.advanceEpoch)
	res, err := s.apply(&chunkdv1.Op{Op: &chunkdv1.Op_AdvanceEpoch{AdvanceEpoch: &chunkdv1.AdvanceEpochOp{
		RetainEpochs: uint32(s.cfg.RetainEpochs), LeaseEpochs: uint32(s.cfg.LeaseEpochs)}}})
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
	s.collect()
}

func (s *Server) tick() {
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
func (s *Server) Applied() iface.Index { return s.applied }

func (s *Server) handle(m iface.Message) {
	switch m.Kind {
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

// apply validates, logs (fsync) and applies one op.
func (s *Server) apply(op *chunkdv1.Op) (Result, error) {
	if err := s.state.Validate(op); err != nil {
		return Result{}, err
	}
	idx, err := s.d.Store.Append(context.Background(), wire.Marshal(op))
	if err != nil {
		return Result{}, iface.Errorf(iface.CodeUnavailable, "log append: %v", err)
	}
	res := s.state.Apply(op)
	s.applied = idx
	if s.sinceSnap++; s.sinceSnap >= s.cfg.SnapshotEvery {
		s.sinceSnap = 0
		if err := s.d.Store.SaveSnapshot(context.Background(), s.applied, s.state.Snapshot()); err != nil {
			// Not fatal: the WAL still holds every op.
			s.d.Log.Error("snapshot", "at", s.applied, "err", err)
		}
	}
	return res, nil
}

func (s *Server) replica(id iface.NodeID) *chunkdv1.Replica {
	n, _ := s.cluster.Node(id)
	return &chunkdv1.Replica{Node: string(id), Addr: n.Addr, Suspect: n.State == detector.Suspect}
}

func (s *Server) begin(m iface.Message, respond iface.Responder) {
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
	res, err := s.apply(&chunkdv1.Op{Op: &chunkdv1.Op_Begin{Begin: op}})
	resp.UploadId = res.UploadID
	wire.Respond(respond, resp, err)
}

// claim logs that an upload will reference these chunks, then tells the
// client which already have MinReplicas copies on alive nodes so it can skip
// sending them. The claim is logged before any copy is written, so GC marks
// the chunk for the whole life of the upload (ADR-0015).
// SIMPLIFIED: dedup is global. Cross-user dedup lets an uploader probe
// whether content exists; Dropbox moved to per-user dedup after that was
// shown in 2011, and a multi-tenant chunkd would salt chunk IDs per tenant.
func (s *Server) claim(m iface.Message, respond iface.Responder) {
	var req chunkdv1.ClaimChunksRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	if _, err := s.apply(&chunkdv1.Op{Op: &chunkdv1.Op_Claim{Claim: &chunkdv1.ClaimChunksOp{UploadId: req.GetUploadId(), Claims: req.GetClaims()}}}); err != nil {
		respond(nil, err)
		return
	}
	u, _ := s.state.Upload(req.GetUploadId())
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
}

// commit publishes a version once every chunk has at least MinReplicas
// reported locations on live nodes. Locations come from the nodes' own
// block reports, not from the client's word.
func (s *Server) commit(m iface.Message, respond iface.Responder) {
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
	res, err := s.apply(op)
	if err == nil {
		ids := make([]iface.ChunkID, 0, len(req.GetChunkIds()))
		for _, raw := range req.GetChunkIds() {
			if id, err := wire.ChunkID(raw); err == nil {
				ids = append(ids, id)
			}
		}
		s.repair.Fresh(ids)
	}
	wire.Respond(respond, &chunkdv1.CommitUploadResponse{Version: res.Version}, err)
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
	var req chunkdv1.AbortUploadRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	_, err := s.apply(&chunkdv1.Op{Op: &chunkdv1.Op_Abort{Abort: &chunkdv1.AbortUploadOp{UploadId: req.GetUploadId()}}})
	wire.Respond(respond, &chunkdv1.AbortUploadResponse{}, err)
}

func (s *Server) delete(m iface.Message, respond iface.Responder) {
	var req chunkdv1.DeleteRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	// Idempotent for a known version: if that exact version was already
	// tombstoned, this is a retry of our own delete.
	if e := req.GetExpectedVersion(); e != 0 {
		if v, ok := s.state.Tombstoned(req.GetPath(), e); ok {
			respond(wire.Marshal(&chunkdv1.DeleteResponse{Version: v}), nil)
			return
		}
	}
	res, err := s.apply(&chunkdv1.Op{Op: &chunkdv1.Op_Delete{Delete: &chunkdv1.DeleteOp{Path: req.GetPath(), ExpectedVersion: req.GetExpectedVersion()}}})
	wire.Respond(respond, &chunkdv1.DeleteResponse{Version: res.Version}, err)
}

// undelete restores a retained version as the newest one. Like delete it is
// conditional on the live version, and a retry finds its own result.
func (s *Server) undelete(m iface.Message, respond iface.Responder) {
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
	res, err := s.apply(&chunkdv1.Op{Op: &chunkdv1.Op_Undelete{Undelete: &chunkdv1.UndeleteOp{Path: req.GetPath(), Version: req.GetVersion(), ExpectedVersion: req.GetExpectedVersion()}}})
	wire.Respond(respond, &chunkdv1.UndeleteResponse{Version: res.Version}, err)
}

func (s *Server) stat(m iface.Message, respond iface.Responder) {
	var req chunkdv1.StatRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
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
	resp := &chunkdv1.ListResponse{}
	for _, e := range s.state.List(req.GetPrefix()) {
		resp.Files = append(resp.Files, &chunkdv1.FileInfo{Path: e.Path, Version: e.V, Size: e.Size, Sha256: e.SHA256[:], ChunkCount: int32(len(e.Chunks))})
	}
	respond(wire.Marshal(resp), nil)
}

func (s *Server) log(m iface.Message, respond iface.Responder) {
	var req chunkdv1.LogRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
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
}

func (s *Server) clusterInfo(m iface.Message, respond iface.Responder) {
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
