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
}

// Caller issues requests from outside the event loops (clients, CLI,
// gateway). Calls in one batch run concurrently; results are in call order.
// In the sim, Do advances the clock until every call finishes or times out,
// so it must never be called from inside a handler or timer.
type Caller interface {
	Do(ctx context.Context, calls []Call) []Result
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
	Usage(ctx context.Context) (Usage, error)
}

// Index is a position in the metadata log. The first entry has index 1.
type Index uint64

// MetaStore is the durable, ordered metadata log plus snapshots. An entry is
// durable once Append returns without error.
type MetaStore interface {
	Append(ctx context.Context, entry []byte) (Index, error)
	// Replay calls fn for every entry with index >= from, in order.
	Replay(ctx context.Context, from Index, fn func(Index, []byte) error) error
	// SaveSnapshot records state covering all entries up to and including at.
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
