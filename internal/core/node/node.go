// Package node is the storage node: it stores and serves chunks by hash,
// heartbeats to the metadata server, and reports what it holds. It knows
// nothing about files.
package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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
	ID         iface.NodeID
	Meta       iface.NodeID
	Rack       string
	Addr       string // advertised to clients for chunk transfers; empty in sim
	Heartbeat  time.Duration
	FullReport time.Duration
	Draining   bool
}

// DefaultConfig fills timers: 1 s heartbeats, full block report every 30 s.
func DefaultConfig(id, meta iface.NodeID, rack string) Config {
	return Config{ID: id, Meta: meta, Rack: rack, Heartbeat: time.Second, FullReport: 30 * time.Second}
}

// Stats counts a node's control traffic. Loop-owned.
type Stats struct {
	Heartbeats   uint64 `json:"heartbeats"`
	Acks         uint64 `json:"acks"`
	FullReports  uint64 `json:"fullReports"`
	RepairCopies uint64 `json:"repairCopies"`
	RepairFailed uint64 `json:"repairFailed"`
	Trimmed      uint64 `json:"trimmed"`
	// Corrupt counts chunks that failed verification and were quarantined.
	Corrupt uint64 `json:"corrupt"`
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
	n.d.Net.Send(n.cfg.Meta, iface.Message{From: n.cfg.ID, Kind: wire.KindBlockReport, Body: wire.Marshal(r)})
}

// Stop halts timers and ignores further messages, like a killed process.
// The sim uses it to replace a node with a fresh incarnation on restart.
func (n *Node) Stop() { n.stopped = true }

// New returns a stopped node.
func New(d Deps, cfg Config) *Node {
	if d.Clock == nil || d.Net == nil || d.Async == nil || d.Store == nil || d.Rand == nil || d.Log == nil {
		panic("node: missing dependency")
	}
	return &Node{d: d, cfg: cfg, incarnation: d.Rand.Uint64()}
}

// Start registers handlers and schedules timers. The first heartbeat and
// report are jittered so nodes started together do not arrive in lockstep.
func (n *Node) Start() {
	n.d.Net.Serve(n.cfg.ID, wire.KindPutChunk, n.putChunk, iface.ServeOpts{Concurrent: true})
	n.d.Net.Serve(n.cfg.ID, wire.KindGetChunk, n.getChunk, iface.ServeOpts{Concurrent: true})
	n.d.Net.Listen(n.cfg.ID, n.handle)
	n.d.Clock.AfterFunc(time.Duration(n.d.Rand.IntN(int(n.cfg.Heartbeat))), n.heartbeat)
	n.d.Clock.AfterFunc(n.cfg.FullReport+time.Duration(n.d.Rand.IntN(int(n.cfg.Heartbeat))), n.periodicReport)
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
	seq, err := n.change(func() error { return n.d.Store.Put(context.Background(), id, req.GetData()) })
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
	}
}

// deleteReplica drops an over-replicated copy and confirms it, also when
// the copy is already gone, so a trim retried after a lost confirmation
// completes (repair.retryTrim).
func (n *Node) deleteReplica(m iface.Message) {
	var cmd chunkdv1.DeleteReplica
	if err := wire.Decode(m.Body, &cmd); err != nil {
		return
	}
	id, err := wire.ChunkID(cmd.GetChunkId())
	if err != nil {
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
	id, err := wire.ChunkID(cmd.GetChunkId())
	if err != nil {
		return
	}
	fail := func(err error) {
		n.stats.RepairFailed++
		n.d.Log.Warn("repair copy failed", "copy", cmd.GetCopyId(), "chunk", id.String()[:12], "source", cmd.GetSource(), "err", err)
		n.d.Net.Send(n.cfg.Meta, iface.Message{From: n.cfg.ID, Kind: wire.KindReplicateFailed,
			Body: wire.Marshal(&chunkdv1.ReplicateFailed{CopyId: cmd.GetCopyId(), ChunkId: id[:], Node: string(n.cfg.ID), Error: err.Error()})})
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
		seq, err := n.change(func() error { return n.d.Store.Put(context.Background(), id, resp.GetData()) })
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
	if ack.GetNeedFullReport() {
		n.fullReport()
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
	hb := &chunkdv1.Heartbeat{Node: string(n.cfg.ID), Rack: n.cfg.Rack, Addr: n.cfg.Addr,
		UsedBytes: u.Bytes, ChunkCount: u.Chunks, Draining: n.cfg.Draining, Seq: n.stats.Heartbeats, Incarnation: n.incarnation}
	n.d.Net.Send(n.cfg.Meta, iface.Message{From: n.cfg.ID, Kind: wire.KindHeartbeat, Body: wire.Marshal(hb)})
	n.d.Clock.AfterFunc(n.cfg.Heartbeat, n.heartbeat)
}

func (n *Node) periodicReport() {
	if n.stopped {
		return
	}
	n.fullReport()
	n.d.Clock.AfterFunc(n.cfg.FullReport, n.periodicReport)
}

// fullReport sends every chunk the store holds. Periodic full reports repair
// any incremental report the network dropped.
// SIMPLIFIED: one message for the whole report. HDFS splits reports per
// storage volume and rate-limits them so a cluster restart does not flood
// the NameNode.
// SIMPLIFIED: listing blocks puts and deletes for its duration. HDFS
// DataNodes snapshot the replica map in memory instead of scanning disk.
func (n *Node) fullReport() {
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
	n.d.Net.Send(n.cfg.Meta, iface.Message{From: n.cfg.ID, Kind: wire.KindBlockReport,
		Body: wire.Marshal(&chunkdv1.BlockReport{Node: string(n.cfg.ID), Full: true, ChunkIds: ids, Incarnation: n.incarnation, Seq: seq})})
}
