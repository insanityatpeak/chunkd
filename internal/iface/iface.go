// Package iface defines the seams between core logic and its environment.
// Core packages depend only on these interfaces; internal/real and
// internal/sim provide the implementations.
package iface

import (
	"context"
	"encoding/hex"
	"errors"
	"time"
)

// NodeID names a process in the cluster (metadata server, storage node, gateway).
type NodeID string

// Instant is logical time in nanoseconds since the cluster clock started.
// It orders local events only; correctness never depends on comparing
// Instants across nodes.
type Instant int64

// Add returns i shifted by d.
func (i Instant) Add(d time.Duration) Instant { return i + Instant(d) }

// Sub returns the duration between i and j.
func (i Instant) Sub(j Instant) time.Duration { return time.Duration(i - j) }

// Message is the unit of communication between nodes. Body holds an encoded
// protobuf; Kind names its type so handlers can dispatch without reflection.
type Message struct {
	From  NodeID
	To    NodeID
	Kind  string
	ReqID uint64 // correlates a response with its request
	Body  []byte
	// Err is set on RPC responses that failed.
	Err *Error
}

// Handler receives messages addressed to one node. It always runs on that
// node's event loop, so handlers never need locks.
type Handler func(Message)

// Responder completes an RPC. Call it exactly once; later calls are ignored.
type Responder func(body []byte, err error)

// RPCHandler serves one request. It may respond later, e.g. after a WAL
// fsync, by keeping respond.
type RPCHandler func(m Message, respond Responder)

// ServeOpts configures an RPC handler.
type ServeOpts struct {
	// Concurrent handlers run on the transport's goroutines in real mode
	// instead of the owner's event loop. They must be safe for concurrent
	// use and must not touch loop-owned state. Chunk transfers use this so
	// disk I/O never stalls heartbeats.
	Concurrent bool
}

// Transport delivers messages between nodes. Send is fire-and-forget: a
// message may be lost, delayed, duplicated or reordered, and callers must
// tolerate all four. Serve registers request handlers that Callers reach.
type Transport interface {
	Send(to NodeID, m Message)
	Listen(id NodeID, h Handler)
	Serve(id NodeID, kind string, h RPCHandler, opts ServeOpts)
}

// Call is one request in a batch.
type Call struct {
	To   NodeID
	Addr string // where To listens in real mode; ignored by the sim
	Kind string
	Body []byte
}

// Result is the outcome of one Call. Err is an *Error; transport failures
// and timeouts are CodeUnavailable.
type Result struct {
	Body []byte
	Err  error
	// Latency is the time from send to response. With Pending set it is a
	// lower bound: Hedge returned before this call finished.
	Latency time.Duration
	Pending bool
}

// HedgeResult is the outcome of Caller.Hedge.
type HedgeResult struct {
	// Winner indexes the accepted result, or is -1 if none was accepted.
	Winner int
	// Results has one entry per call; only the first Launched were sent.
	Results  []Result
	Launched int
}

// Caller issues requests from outside the event loops (clients, CLI,
// gateway). In the sim, Do and Hedge advance the clock until they finish,
// so they must never be called from inside a handler or timer.
type Caller interface {
	// Do runs a batch concurrently; results are in call order.
	Do(ctx context.Context, calls []Call) []Result
	// Hedge sends calls[0] at once and calls[i] after another `after`
	// passes with no accepted answer, or at once when every call sent so far
	// has failed or been rejected. It returns at the first result accept
	// takes; accept runs on the caller's goroutine.
	Hedge(ctx context.Context, calls []Call, after time.Duration, accept func(i int, r Result) bool) HedgeResult
	// Gather is Hedge for need answers out of many: it sends calls[:first]
	// at once, the next call at once when one fails or is rejected, and the
	// next after another `after` passes with no new accepted answer. It
	// returns once need results are accepted or every call sent has settled
	// and none is left; accept runs on the caller's goroutine.
	Gather(ctx context.Context, calls []Call, first, need int, after time.Duration, accept func(i int, r Result) bool) GatherResult
}

// GatherResult is the outcome of Caller.Gather.
type GatherResult struct {
	// Accepted indexes the accepted results, in the order they arrived.
	Accepted []int
	// Results has one entry per call; only the first Launched were sent. A
	// call still running when Gather returned is Pending.
	Results  []Result
	Launched int
}

// AsyncCaller issues requests from inside an event loop, where blocking
// Caller methods are forbidden. cb runs exactly once, on the owner's loop,
// with the response or a CodeUnavailable error after the caller's timeout.
type AsyncCaller interface {
	Go(c Call, cb func(Result))
}

// Timer is a pending callback scheduled on a Clock.
type Timer interface {
	// Stop cancels the timer and reports whether it had not yet fired.
	Stop() bool
}

// Clock supplies local time and timers. Callbacks run on the event loop that
// owns the clock.
type Clock interface {
	Now() Instant
	AfterFunc(d time.Duration, f func()) Timer
}

// ChunkID is the SHA-256 of a chunk's contents.
type ChunkID [32]byte

func (c ChunkID) String() string { return hex.EncodeToString(c[:]) }

// ErrNotFound is returned by stores when a key does not exist.
var ErrNotFound = errors.New("not found")

// Usage is how much a BlockStore holds.
type Usage struct {
	Chunks int64
	Bytes  int64
}

// BlockStore holds chunk bytes addressed by content hash. Put rejects data
// whose SHA-256 is not id (CodeInvalid) and is idempotent: storing a chunk
// that is already present and intact succeeds without rewriting it.
type BlockStore interface {
	Put(ctx context.Context, id ChunkID, data []byte) error
	Get(ctx context.Context, id ChunkID) ([]byte, error)
	Delete(ctx context.Context, id ChunkID) error
	// List calls fn for every stored chunk; iteration order is unspecified.
	List(ctx context.Context, fn func(ChunkID) error) error
	// Quarantine moves a chunk whose bytes failed verification out of Get,
	// List and Usage, keeping the bytes for inspection. A later Put of the
	// intact chunk stores it again. Quarantining a missing chunk succeeds.
	Quarantine(ctx context.Context, id ChunkID) error
	Usage(ctx context.Context) (Usage, error)
	// Term returns the highest metadata term SaveTerm recorded, or 0. It is
	// the node's fencing memory and lives on the node's disk, so it survives
	// a restart exactly as the chunks do (and is lost with a wiped disk).
	Term(ctx context.Context) (uint64, error)
	// SaveTerm records term durably. The caller only ever raises it.
	SaveTerm(ctx context.Context, term uint64) error
}

// Index is a position in the metadata log. The first entry has index 1.
type Index uint64

// MetaStore is the durable metadata log of one consensus peer: entries, a
// small state record (the consensus hard state: term, vote, commit) and a
// snapshot. All three are opaque bytes to the store.
type MetaStore interface {
	// Save writes entries at indexes first, first+1, … replacing every stored
	// entry at or after first (a follower's conflicting tail), then state if
	// it is non-nil. With no entries it truncates the log to first-1. All of
	// it is durable, in one sync, when Save returns. first must be above the
	// snapshot index and at most the last index + 1 (CodeInvalid otherwise).
	Save(ctx context.Context, first Index, entries [][]byte, state []byte) error
	// State returns the last state saved, or nil.
	State(ctx context.Context) ([]byte, error)
	// Replay calls fn for every entry with index >= from, in order.
	Replay(ctx context.Context, from Index, fn func(Index, []byte) error) error
	// SaveSnapshot records data as covering every entry up to and including
	// at, and drops those entries. Later entries are kept; if at is past the
	// last entry the log is empty and continues at at+1.
	SaveSnapshot(ctx context.Context, at Index, data []byte) error
	// LoadSnapshot returns the latest snapshot, or index 0 and nil data if none.
	LoadSnapshot(ctx context.Context) (Index, []byte, error)
}

// Rand is the only source of randomness in core code. The sim seeds it so a
// run replays exactly from its seed.
type Rand interface {
	Uint64() uint64
	IntN(n int) int
	Shuffle(n int, swap func(i, j int))
}
