// Package ec erasure-codes one chunk into Reed-Solomon shards: 4 data shards
// and 2 parity shards, any 4 of which rebuild the chunk (ADR-0022).
package ec

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/klauspost/reedsolomon"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

const (
	// DataShards is k in RS(k, m).
	DataShards = 4
	// ParityShards is m: the number of lost shards a chunk survives.
	ParityShards = 2
	// TotalShards is k + m: the nodes one chunk's stripe spans.
	TotalShards = DataShards + ParityShards
	// CommitShards are the shards a commit needs on alive nodes: one loss of
	// margin above DataShards, as 2 of 3 copies is for replication.
	CommitShards = DataShards + 1
)

// ErrUnrecoverable means more than ParityShards shards of a chunk are missing.
var ErrUnrecoverable = errors.New("ec: fewer than 4 of 6 shards available")

// Codec encodes and decodes RS(4,2) stripes. Safe for concurrent use.
type Codec struct {
	enc reedsolomon.Encoder
}

// New returns an RS(4,2) codec.
func New() *Codec {
	// One goroutine: the sim's event loop and the browser are single-threaded,
	// and a chunk is small enough that fan-out only adds scheduling noise.
	enc, err := reedsolomon.New(DataShards, ParityShards, reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		panic(err) // fixed, valid parameters
	}
	return &Codec{enc: enc}
}

// ShardSize returns the length of each shard of a chunk of size bytes. The
// last data shard is zero-padded up to it.
func ShardSize(size int) int {
	return (size + DataShards - 1) / DataShards
}

// Split returns the 6 shards of data: indexes 0-3 hold data, 4-5 parity.
// Every shard is a fresh buffer; data is not retained.
func (c *Codec) Split(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("ec: empty chunk")
	}
	per := ShardSize(len(data))
	buf := make([]byte, per*TotalShards)
	copy(buf, data)
	shards := make([][]byte, TotalShards)
	for i := range shards {
		shards[i] = buf[i*per : (i+1)*per : (i+1)*per]
	}
	if err := c.enc.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

// Join returns the size-byte chunk from shards, where a nil entry is a
// missing shard. Missing data shards are rebuilt in place from parity.
func (c *Codec) Join(shards [][]byte, size int) ([]byte, error) {
	if err := c.check(shards, size); err != nil {
		return nil, err
	}
	if missing(shards[:DataShards]) > 0 {
		if err := c.enc.ReconstructData(shards); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnrecoverable, err)
		}
	}
	out := make([]byte, 0, size)
	for _, s := range shards[:DataShards] {
		out = append(out, s...)
	}
	return out[:size], nil
}

// Rebuild returns shard i of a chunk of size bytes, computed from the other
// shards present in shards. It reads exactly DataShards shards' worth of
// bytes, the repair cost ADR-0023 compares with replication.
func (c *Codec) Rebuild(shards [][]byte, size, i int) ([]byte, error) {
	if i < 0 || i >= TotalShards {
		return nil, fmt.Errorf("ec: shard index %d out of range", i)
	}
	if err := c.check(shards, size); err != nil {
		return nil, err
	}
	work := make([][]byte, TotalShards)
	used := 0
	for j, s := range shards {
		if j != i && s != nil && used < DataShards {
			work[j] = s
			used++
		}
	}
	if err := c.enc.Reconstruct(work); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnrecoverable, err)
	}
	return work[i], nil
}

// HeaderSize prefixes every shard block: the shard index, then the logical
// ID of its chunk. Equal payloads (two zero quarters of one chunk, a quarter
// two chunks share) thus get distinct block IDs, one per stripe slot.
const HeaderSize = 1 + len(iface.ChunkID{})

// LogicalID names the stripe of the chunk whose content hash is data. It
// differs from data, so a chunk stored replicated and the same bytes stored
// as a stripe never share a record (ADR-0022).
func LogicalID(data iface.ChunkID) iface.ChunkID {
	return sha256.Sum256(append([]byte("chunkd-ec-4+2:"), data[:]...))
}

// Shard is one stored block of a stripe.
type Shard struct {
	Index int
	ID    iface.ChunkID // sha256(Block): the block store's key
	Block []byte        // header + payload
}

// Encode splits the chunk data (content hash data) into its 6 shard blocks.
func (c *Codec) Encode(data []byte) ([]Shard, error) {
	payloads, err := c.Split(data)
	if err != nil {
		return nil, err
	}
	logical := LogicalID(sha256.Sum256(data))
	out := make([]Shard, TotalShards)
	for i, p := range payloads {
		b := Block(logical, i, p)
		out[i] = Shard{Index: i, ID: sha256.Sum256(b), Block: b}
	}
	return out, nil
}

// Block wraps a shard payload with its header.
func Block(logical iface.ChunkID, i int, payload []byte) []byte {
	b := make([]byte, HeaderSize+len(payload))
	b[0] = byte(i)
	copy(b[1:HeaderSize], logical[:])
	copy(b[HeaderSize:], payload)
	return b
}

// Payload returns a shard block's payload after checking that its header
// names slot i of stripe logical: a block from another slot hashes fine but
// would decode to the wrong bytes.
func Payload(block []byte, logical iface.ChunkID, i int) ([]byte, error) {
	if len(block) < HeaderSize || int(block[0]) != i || !bytes.Equal(block[1:HeaderSize], logical[:]) {
		return nil, fmt.Errorf("ec: block is not shard %d of stripe %s", i, logical)
	}
	return block[HeaderSize:], nil
}

// BlockSize returns the stored size of each shard of a size-byte chunk.
func BlockSize(size int64) int64 { return int64(HeaderSize + ShardSize(int(size))) }

func (c *Codec) check(shards [][]byte, size int) error {
	if len(shards) != TotalShards {
		return fmt.Errorf("ec: %d shards, want %d", len(shards), TotalShards)
	}
	per := ShardSize(size)
	for j, s := range shards {
		if s != nil && len(s) != per {
			return fmt.Errorf("ec: shard %d is %d bytes, want %d", j, len(s), per)
		}
	}
	if n := missing(shards); n > ParityShards {
		return fmt.Errorf("%w: %d missing", ErrUnrecoverable, n)
	}
	return nil
}

func missing(shards [][]byte) int {
	n := 0
	for _, s := range shards {
		if s == nil {
			n++
		}
	}
	return n
}
