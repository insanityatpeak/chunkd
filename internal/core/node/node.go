// Package node is the storage node: it stores and serves chunks by hash,
// heartbeats to the metadata server, and reports what it holds. It knows
// nothing about files.
package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/scrub"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// Deps are the environment a node runs in.
type Deps struct {
	Clock iface.Clock
	Net   iface.Transport
	// Async fetches chunks from peers for repair.
	Async iface.AsyncCaller
	Store iface.BlockStore
	Rand  iface.Rand
	Log   *slog.Logger
}

// Config identifies the node and sets its timers.
type Config struct {
	ID iface.NodeID
	// Metas are every metadata peer. Heartbeats and block reports go to all of
	// them, so a follower already knows every location when it becomes leader;
	// commands are honoured only from the current leader (see fenced).
	Metas      []iface.NodeID
	Rack       string
	Addr       string // advertised to clients for chunk transfers; empty in sim
	Heartbeat  time.Duration
	FullReport time.Duration
	Scrub      scrub.Config
}

// DefaultConfig fills timers: 1 s heartbeats, full block report every 30 s,
// the default scrub rate and pass interval.
func DefaultConfig(id, meta iface.NodeID, rack string) Config {
	return Config{ID: id, Metas: []iface.NodeID{meta}, Rack: rack, Heartbeat: time.Second, FullReport: 30 * time.Second, Scrub: scrub.DefaultConfig()}
}

// Stats counts a node's control traffic. Loop-owned.
type Stats struct {
	Heartbeats   uint64 `json:"heartbeats"`
	Acks         uint64 `json:"acks"`
	FullReports  uint64 `json:"fullReports"`
	RepairCopies uint64 `json:"repairCopies"`
	RepairFailed uint64 `json:"repairFailed"`
	Trimmed      uint64 `json:"trimmed"`
	// GC deletes carried out, and refused because the chunk was written
	// after the delete's fence.
	GCDeleted uint64 `json:"gcDeleted"`
	GCKept    uint64 `json:"gcKept"`
	// Corrupt counts chunks that failed verification and were quarantined.
	Corrupt uint64 `json:"corrupt"`
	// Fenced counts commands refused because their term was below the highest
	// this node has seen: a deposed leader still talking.
	Fenced uint64 `json:"fenced"`
}

// Node is one storage node.
type Node struct {
	d     Deps
	cfg   Config
	stats Stats
	// incarnation changes on every start, so the metadata server can tell a
	// restarted node (seq back at 1) from replayed old heartbeats.
	incarnation uint64
	stopped     bool
	// maxTerm is the highest metadata term this node has obeyed or heard a
	// leader announce, persisted with the chunks. A command below it comes
	// from a deposed leader. Loop-owned: only handle and heartbeatAck touch it.
	maxTerm uint64

	// reportMu orders store changes against full reports: puts and deletes
	// hold it shared while they change the store and take a seq; a full
	// report holds it exclusively while it takes a seq and lists the store.
	// So a full report with seq S reflects exactly the changes numbered
	// below S, which lets the metadata server order reports the network
	// reorders (meta.Cluster.Report).
	reportMu  sync.RWMutex
	reportSeq atomic.Uint64
	// corrupt is counted by concurrent read handlers, so not in stats.
	corrupt atomic.Uint64
	scrub   *scrub.Scrubber

	// chunkMu orders writes of a chunk against a GC delete of it: the delete
	// checks lastWrite and removes the file under the same lock, so a write
	// cannot land in between. Striped so puts of other chunks stay parallel.
	chunkMu [64]sync.Mutex
	// lastWrite is the seq of the latest write of each chunk in this
	// incarnation. SIMPLIFIED: never pruned; 40 bytes per chunk written.
	lastWriteMu sync.Mutex
	lastWrite   map[iface.ChunkID]uint64
}

// write stores a chunk through f and records the change's seq as the
// chunk's latest write.
func (n *Node) write(id iface.ChunkID, f func() error) (uint64, error) {
	mu := &n.chunkMu[id[0]%64]
	mu.Lock()
	defer mu.Unlock()
	seq, err := n.change(f)
	if err == nil {
		n.lastWriteMu.Lock()
		n.lastWrite[id] = seq
		n.lastWriteMu.Unlock()
	}
	return seq, err
}

// change runs a store mutation and returns the seq for its report.
func (n *Node) change(f func() error) (uint64, error) {
	n.reportMu.RLock()
	defer n.reportMu.RUnlock()
	if err := f(); err != nil {
		return 0, err
	}
	return n.reportSeq.Add(1), nil
}

// report sends an incremental block report.
func (n *Node) report(seq uint64, added, deleted, corrupt []iface.ChunkID) {
	r := &chunkdv1.BlockReport{Node: string(n.cfg.ID), Incarnation: n.incarnation, Seq: seq}
	for _, id := range added {
		r.ChunkIds = append(r.ChunkIds, id[:])
	}
	for _, id := range deleted {
		r.DeletedIds = append(r.DeletedIds, id[:])
	}
	for _, id := range corrupt {
		r.CorruptIds = append(r.CorruptIds, id[:])
	}
	n.toMetas(wire.KindBlockReport, wire.Marshal(r))
}

// toMetas sends one message to every metadata peer.
func (n *Node) toMetas(kind string, body []byte) {
	for _, m := range n.cfg.Metas {
		n.d.Net.Send(m, iface.Message{From: n.cfg.ID, Kind: kind, Body: body})
	}
}

// fenced reports whether a command carrying term must be refused: the node
// has already heard from a leader of a later term, so this one was deposed.
// A newer term is recorded first, durably, so the refusal outlives a restart;
// if it cannot be recorded the command is refused too.
func (n *Node) fenced(kind string, term uint64) bool {
	if term < n.maxTerm {
		n.stats.Fenced++
		n.d.Log.Warn("refused a command from a deposed leader", "kind", kind, "term", term, "current", n.maxTerm)
		return true
	}
	return term > n.maxTerm && !n.raiseTerm(term)
}

func (n *Node) raiseTerm(term uint64) bool {
	if err := n.d.Store.SaveTerm(context.Background(), term); err != nil {
		n.d.Log.Error("cannot record the metadata term", "term", term, "err", err)
		return false
	}
	n.maxTerm = term
	return true
}

// Stop halts timers and ignores further messages, like a killed process.
// The sim uses it to replace a node with a fresh incarnation on restart.
func (n *Node) Stop() {
	n.stopped = true
	n.scrub.Stop()
}

// New returns a stopped node, with the fencing term it recorded before it
// last stopped. A node that cannot read its own disk does not run.
func New(d Deps, cfg Config) (*Node, error) {
	if d.Clock == nil || d.Net == nil || d.Async == nil || d.Store == nil || d.Rand == nil || d.Log == nil {
		panic("node: missing dependency")
	}
	if len(cfg.Metas) == 0 {
		return nil, fmt.Errorf("node %s: no metadata peers", cfg.ID)
	}
	term, err := d.Store.Term(context.Background())
	if err != nil {
		return nil, fmt.Errorf("node %s: read term: %w", cfg.ID, err)
	}
	n := &Node{d: d, cfg: cfg, incarnation: d.Rand.Uint64(), lastWrite: map[iface.ChunkID]uint64{}, maxTerm: term}
	n.scrub = scrub.New(cfg.Scrub, d.Clock, d.Store, n.quarantine)
	return n, nil
}

// Scrub returns the scrubber's counters and pass progress. Loop-owned.
func (n *Node) Scrub() scrub.Stats { return n.scrub.Stats() }

// Start registers handlers and schedules timers. The first heartbeat and
// report are jittered so nodes started together do not arrive in lockstep.
func (n *Node) Start() {
	n.d.Net.Serve(n.cfg.ID, wire.KindPutChunk, n.putChunk, iface.ServeOpts{Concurrent: true})
	n.d.Net.Serve(n.cfg.ID, wire.KindGetChunk, n.getChunk, iface.ServeOpts{Concurrent: true})
	n.d.Net.Listen(n.cfg.ID, n.handle)
	n.d.Clock.AfterFunc(time.Duration(n.d.Rand.IntN(int(n.cfg.Heartbeat))), n.heartbeat)
	n.d.Clock.AfterFunc(n.cfg.FullReport+time.Duration(n.d.Rand.IntN(int(n.cfg.Heartbeat))), n.periodicReport)
	// First pass after one full report interval, jittered by up to a
	// second, so a restarted cluster reports before it scrubs.
	n.scrub.Start(n.cfg.FullReport + time.Duration(n.d.Rand.IntN(int(time.Second))))
}

// Stats returns control-traffic counters.
func (n *Node) Stats() Stats {
	s := n.stats
	s.Corrupt = n.corrupt.Load()
	return s
}

// ID returns the node's ID.
func (n *Node) ID() iface.NodeID { return n.cfg.ID }

// putChunk verifies and stores a chunk, then tells the metadata server. It
// runs concurrently in real mode and touches only the store and the network.
func (n *Node) putChunk(m iface.Message, respond iface.Responder) {
	var req chunkdv1.PutChunkRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	id, err := wire.ChunkID(req.GetId())
	if err != nil {
		respond(nil, err)
		return
	}
	// The store rejects data that does not hash to id, so a corrupted
	// transfer is never acknowledged.
	seq, err := n.write(id, func() error { return n.d.Store.Put(context.Background(), id, req.GetData()) })
	if err != nil {
		respond(nil, err)
		return
	}
	// Incremental block report before the ack: by the time the client
	// commits, the report is usually already in (if not, commit retries).
	n.report(seq, []iface.ChunkID{id}, nil, nil)
	respond(wire.Marshal(&chunkdv1.PutChunkResponse{}), nil)
}

func (n *Node) getChunk(m iface.Message, respond iface.Responder) {
	var req chunkdv1.GetChunkRequest
	if err := wire.Decode(m.Body, &req); err != nil {
		respond(nil, err)
		return
	}
	id, err := wire.ChunkID(req.GetId())
	if err != nil {
		respond(nil, err)
		return
	}
	data, err := n.d.Store.Get(context.Background(), id)
	if err != nil {
		respond(nil, iface.AsError(err, iface.CodeInternal))
		return
	}
	// Verified here as well as by the reader: the node is the one place
	// that can quarantine the file and tell the metadata server, and a
	// reader (or a gateway in between) is not trusted to report replicas.
	if sha256.Sum256(data) != id {
		n.quarantine(id)
		respond(nil, iface.Errorf(iface.CodeCorrupt, "%s: chunk %s failed verification and was quarantined", n.cfg.ID, id.String()[:12]))
		return
	}
	respond(wire.Marshal(&chunkdv1.GetChunkResponse{Data: data}), nil)
}

// verifyChunk re-reads one chunk on the metadata server's request (a
// client saw bad bytes from here) and quarantines it if it fails.
func (n *Node) verifyChunk(m iface.Message) {
	var cmd chunkdv1.VerifyChunk
	if err := wire.Decode(m.Body, &cmd); err != nil || n.fenced(m.Kind, cmd.GetTerm()) {
		return
	}
	id, err := wire.ChunkID(cmd.GetChunkId())
	if err != nil {
		return
	}
	data, err := n.d.Store.Get(context.Background(), id)
	if err != nil {
		return // gone already, or unreadable: the next full report says so
	}
	if sha256.Sum256(data) != id {
		n.quarantine(id)
	}
}

// quarantine moves a chunk that failed verification aside and reports it,
// so the metadata server stops counting it and repair replaces it. Safe
// from concurrent handlers.
func (n *Node) quarantine(id iface.ChunkID) {
	seq, err := n.change(func() error { return n.d.Store.Quarantine(context.Background(), id) })
	if err != nil {
		n.d.Log.Error("quarantine", "chunk", id.String()[:12], "err", err)
		return
	}
	n.corrupt.Add(1)
	n.d.Log.Warn("corrupt chunk quarantined", "chunk", id.String()[:12])
	n.report(seq, nil, nil, []iface.ChunkID{id})
}

func (n *Node) handle(m iface.Message) {
	if n.stopped {
		return
	}
	switch m.Kind {
	case wire.KindHeartbeatAck:
		n.heartbeatAck(m)
	case wire.KindReplicate:
		n.replicate(m)
	case wire.KindDeleteReplica:
		n.deleteReplica(m)
	case wire.KindVerifyChunk:
		n.verifyChunk(m)
	}
}

// deleteReplica drops an over-replicated copy and confirms it, also when
// the copy is already gone, so a trim retried after a lost confirmation
// completes (repair.retryTrim).
func (n *Node) deleteReplica(m iface.Message) {
	var cmd chunkdv1.DeleteReplica
	if err := wire.Decode(m.Body, &cmd); err != nil || n.fenced(m.Kind, cmd.GetTerm()) {
		return
	}
	id, err := wire.ChunkID(cmd.GetChunkId())
	if err != nil {
		return
	}
	if cmd.GetGc() {
		n.gcDelete(id, cmd.GetFenceIncarnation(), cmd.GetFenceSeq())
		return
	}
	seq, err := n.change(func() error {
		if err := n.d.Store.Delete(context.Background(), id); err != nil && !errors.Is(err, iface.ErrNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		n.d.Log.Error("trim delete", "chunk", id.String()[:12], "err", err)
		return
	}
	n.stats.Trimmed++
	n.report(seq, nil, []iface.ChunkID{id}, nil)
}

// gcDelete removes an unreferenced copy unless this node wrote the chunk
// after the metadata server's view (a change numbered above fenceSeq) or is
// not the incarnation the server saw: then an upload may have claimed and
// re-written it since the server decided. Either way the answer goes back
// through the ordered report path, so the server knows the delete is over.
func (n *Node) gcDelete(id iface.ChunkID, fenceInc, fenceSeq uint64) {
	mu := &n.chunkMu[id[0]%64]
	mu.Lock()
	defer mu.Unlock()
	n.lastWriteMu.Lock()
	keep := fenceInc != n.incarnation || n.lastWrite[id] > fenceSeq
	n.lastWriteMu.Unlock()
	seq, err := n.change(func() error {
		if keep {
			return nil
		}
		if err := n.d.Store.Delete(context.Background(), id); err != nil && !errors.Is(err, iface.ErrNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		n.d.Log.Error("gc delete", "chunk", id.String()[:12], "err", err)
		return
	}
	r := &chunkdv1.BlockReport{Node: string(n.cfg.ID), Incarnation: n.incarnation, Seq: seq}
	if keep {
		n.stats.GCKept++
		r.KeptIds = [][]byte{id[:]}
	} else {
		n.stats.GCDeleted++
		r.DeletedIds = [][]byte{id[:]}
	}
	n.toMetas(wire.KindBlockReport, wire.Marshal(r))
}

// replicate pulls one chunk from a peer and stores it. Put verifies the
// hash, so a corrupt source is never copied; success is reported the same
// way as a client write, with an incremental block report.
// SIMPLIFIED: the store write runs on the event loop. HDFS DataNodes run
// transfers on dedicated threads; here a 4 MiB write per copy (at most two
// at a time, ADR-0011) is small next to the 3 s suspect timeout.
func (n *Node) replicate(m iface.Message) {
	var cmd chunkdv1.ReplicateChunk
	if err := wire.Decode(m.Body, &cmd); err != nil {
		n.d.Log.Warn("bad replicate command", "err", err)
		return
	}
	if n.fenced(m.Kind, cmd.GetTerm()) {
		return
	}
	id, err := wire.ChunkID(cmd.GetChunkId())
	if err != nil {
		return
	}
	fail := func(err error) {
		n.stats.RepairFailed++
		n.d.Log.Warn("repair copy failed", "copy", cmd.GetCopyId(), "chunk", id.String()[:12], "source", cmd.GetSource(), "err", err)
		n.toMetas(wire.KindReplicateFailed,
			wire.Marshal(&chunkdv1.ReplicateFailed{CopyId: cmd.GetCopyId(), ChunkId: id[:], Node: string(n.cfg.ID), Error: err.Error()}))
	}
	call := iface.Call{To: iface.NodeID(cmd.GetSource()), Addr: cmd.GetSourceAddr(), Kind: wire.KindGetChunk, Body: wire.Marshal(&chunkdv1.GetChunkRequest{Id: id[:]})}
	n.d.Async.Go(call, func(r iface.Result) {
		if n.stopped {
			return
		}
		if r.Err != nil {
			fail(r.Err)
			return
		}
		var resp chunkdv1.GetChunkResponse
		if err := wire.Decode(r.Body, &resp); err != nil {
			fail(err)
			return
		}
		seq, err := n.write(id, func() error { return n.d.Store.Put(context.Background(), id, resp.GetData()) })
		if err != nil {
			fail(err)
			return
		}
		n.stats.RepairCopies++
		n.report(seq, []iface.ChunkID{id}, nil, nil)
	})
}

func (n *Node) heartbeatAck(m iface.Message) {
	var ack chunkdv1.HeartbeatAck
	if err := wire.Decode(m.Body, &ack); err != nil {
		n.d.Log.Warn("bad heartbeat ack", "err", err)
		return
	}
	n.stats.Acks++
	// The leader's ack announces the current term, so a deposed leader's
	// commands are refused from the first heartbeat after an election, not
	// only after the new leader's first command.
	if ack.GetLeader() && ack.GetTerm() > n.maxTerm {
		n.raiseTerm(ack.GetTerm())
	}
	if ack.GetNeedFullReport() {
		// Only the peer that asked: the others already have this node's state.
		n.fullReport(m.From)
	}
}

func (n *Node) heartbeat() {
	if n.stopped {
		return
	}
	u, err := n.d.Store.Usage(context.Background())
	if err != nil {
		n.d.Log.Error("usage", "err", err)
	}
	n.stats.Heartbeats++
	sc := n.scrub.Stats()
	hb := &chunkdv1.Heartbeat{Node: string(n.cfg.ID), Rack: n.cfg.Rack, Addr: n.cfg.Addr,
		UsedBytes: u.Bytes, ChunkCount: u.Chunks, Seq: n.stats.Heartbeats, Incarnation: n.incarnation,
		Corrupt: n.corrupt.Load(), ScrubDone: int64(sc.Done), ScrubTotal: int64(sc.Total), ScrubPasses: sc.Passes}
	n.toMetas(wire.KindHeartbeat, wire.Marshal(hb))
	n.d.Clock.AfterFunc(n.cfg.Heartbeat, n.heartbeat)
}

func (n *Node) periodicReport() {
	if n.stopped {
		return
	}
	n.fullReport()
	n.d.Clock.AfterFunc(n.cfg.FullReport, n.periodicReport)
}

// fullReport sends every chunk the store holds to to, or to every metadata
// peer if none is named. Periodic full reports repair any incremental report
// the network dropped.
// SIMPLIFIED: one message for the whole report. HDFS splits reports per
// storage volume and rate-limits them so a cluster restart does not flood
// the NameNode.
// SIMPLIFIED: listing blocks puts and deletes for its duration. HDFS
// DataNodes snapshot the replica map in memory instead of scanning disk.
func (n *Node) fullReport(to ...iface.NodeID) {
	var ids [][]byte
	n.reportMu.Lock()
	seq := n.reportSeq.Add(1)
	err := n.d.Store.List(context.Background(), func(id iface.ChunkID) error {
		ids = append(ids, append([]byte(nil), id[:]...))
		return nil
	})
	n.reportMu.Unlock()
	if err != nil {
		n.d.Log.Error("list chunks", "err", err)
		return
	}
	n.stats.FullReports++
	body := wire.Marshal(&chunkdv1.BlockReport{Node: string(n.cfg.ID), Full: true, ChunkIds: ids, Incarnation: n.incarnation, Seq: seq})
	if len(to) == 0 {
		to = n.cfg.Metas
	}
	for _, m := range to {
		n.d.Net.Send(m, iface.Message{From: n.cfg.ID, Kind: wire.KindBlockReport, Body: body})
	}
}

// Config returns the node's configuration.
func (n *Node) Config() Config { return n.cfg }
