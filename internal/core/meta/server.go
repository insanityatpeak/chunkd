package meta

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/placement"
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
	DeadAfter     time.Duration
	SnapshotEvery int
}

// DefaultConfig is N=3, commit at 2 (ADR-0007), 4 MiB chunks (ADR-0005).
func DefaultConfig(id iface.NodeID) Config {
	return Config{ID: id, Replicas: 3, MinReplicas: 2, ChunkSize: chunk.DefaultSize, DeadAfter: 5 * time.Second, SnapshotEvery: 1000}
}

// Server is the metadata server. Everything runs on its event loop.
type Server struct {
	d         Deps
	cfg       Config
	state     *State
	cluster   *Cluster
	applied   iface.Index
	sinceSnap int
}

// NewServer recovers durable state: the latest snapshot, then every WAL
// entry after it.
func NewServer(ctx context.Context, d Deps, cfg Config) (*Server, error) {
	if d.Clock == nil || d.Net == nil || d.Store == nil || d.Rand == nil || d.Log == nil {
		panic("meta: missing dependency")
	}
	at, snap, err := d.Store.LoadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	state, err := Restore(snap)
	if err != nil {
		return nil, fmt.Errorf("restore snapshot at %d: %w", at, err)
	}
	s := &Server{d: d, cfg: cfg, state: state, cluster: NewCluster(), applied: at}
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
		wire.KindBegin:   s.begin,
		wire.KindCommit:  s.commit,
		wire.KindAbort:   s.abort,
		wire.KindDelete:  s.delete,
		wire.KindStat:    s.stat,
		wire.KindList:    s.list,
		wire.KindCluster: s.clusterInfo,
	} {
		s.d.Net.Serve(s.cfg.ID, kind, h, iface.ServeOpts{})
	}
}

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
		if _, known := s.cluster.Node(id); !known {
			s.d.Log.Info("node joined", "node", id, "rack", hb.GetRack())
		}
		need := s.cluster.Heartbeat(NodeState{ID: id, Rack: hb.GetRack(), Addr: hb.GetAddr(), Used: hb.GetUsedBytes(),
			Chunks: hb.GetChunkCount(), Draining: hb.GetDraining()}, s.d.Clock.Now())
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
		if r.GetFull() {
			s.cluster.FullReport(iface.NodeID(r.GetNode()), ids)
		} else {
			s.cluster.Received(iface.NodeID(r.GetNode()), ids)
		}
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
	return &chunkdv1.Replica{Node: string(id), Addr: n.Addr}
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
	pl, err := placement.Place(s.cluster.PlacementView(s.d.Clock.Now(), s.cfg.DeadAfter), sizes, s.cfg.Replicas, s.cfg.MinReplicas, s.d.Rand)
	if err != nil {
		respond(nil, iface.Errorf(iface.CodeUnavailable, "%v (need %d)", err, s.cfg.MinReplicas))
		return
	}
	op := &chunkdv1.BeginUploadOp{Path: req.GetPath(), ExpectedVersion: req.GetExpectedVersion(), Size: req.GetSize(), ChunkSize: int32(s.cfg.ChunkSize)}
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

// commit publishes a version once every chunk has at least MinReplicas
// reported locations on live nodes. Locations come from the nodes' own
// block reports, not from the client's word.
func (s *Server) commit(m iface.Message, respond iface.Responder) {
	var req chunkdv1.CommitUploadRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
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
	wire.Respond(respond, &chunkdv1.CommitUploadResponse{Version: res.Version}, err)
}

func (s *Server) liveLocations(id iface.ChunkID) []iface.NodeID {
	var out []iface.NodeID
	for _, n := range s.cluster.Locations(id) {
		if s.cluster.Alive(n, s.d.Clock.Now(), s.cfg.DeadAfter) {
			out = append(out, n)
		}
	}
	return out
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
	res, err := s.apply(&chunkdv1.Op{Op: &chunkdv1.Op_Delete{Delete: &chunkdv1.DeleteOp{Path: req.GetPath(), ExpectedVersion: req.GetExpectedVersion()}}})
	wire.Respond(respond, &chunkdv1.DeleteResponse{Version: res.Version}, err)
}

func (s *Server) stat(m iface.Message, respond iface.Responder) {
	var req chunkdv1.StatRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	v, err := s.state.Stat(req.GetPath())
	if err != nil {
		respond(nil, err)
		return
	}
	resp := &chunkdv1.StatResponse{Path: req.GetPath(), Version: v.V, Size: v.Size, Sha256: v.SHA256[:], ChunkSize: int32(v.ChunkSize)}
	for i, id := range v.Chunks {
		loc := &chunkdv1.ChunkLocation{Id: id[:], Size: chunk.SizeOf(v.Size, v.ChunkSize, i)}
		for _, n := range s.liveLocations(id) {
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

func (s *Server) clusterInfo(_ iface.Message, respond iface.Responder) {
	resp := &chunkdv1.ClusterResponse{}
	for _, n := range s.cluster.Nodes() {
		resp.Nodes = append(resp.Nodes, &chunkdv1.NodeInfo{Id: string(n.ID), Rack: n.Rack, Addr: n.Addr, UsedBytes: n.Used,
			ChunkCount: n.Chunks, Alive: s.cluster.Alive(n.ID, s.d.Clock.Now(), s.cfg.DeadAfter), Draining: n.Draining})
	}
	for _, e := range s.state.List("/") {
		resp.Files++
		resp.LogicalBytes += e.Size
	}
	respond(wire.Marshal(resp), nil)
}
