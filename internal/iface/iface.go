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
	ReqID uint64 // correlates a response with its request; 0 for one-way messages
	Body  []byte
}

// Handler receives messages addressed to one node. It always runs on that
// node's event loop, so handlers never need locks.
type Handler func(Message)

// Transport delivers messages between nodes. Send is fire-and-forget: a
// message may be lost, delayed, duplicated or reordered, and callers must
// tolerate all four.
type Transport interface {
	Send(to NodeID, m Message)
	Listen(id NodeID, h Handler)
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

// BlockStore holds chunk bytes addressed by content hash.
type BlockStore interface {
	Put(ctx context.Context, id ChunkID, data []byte) error
	Get(ctx context.Context, id ChunkID) ([]byte, error)
	Delete(ctx context.Context, id ChunkID) error
	// List calls fn for every stored chunk; iteration order is unspecified.
	List(ctx context.Context, fn func(ChunkID) error) error
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
