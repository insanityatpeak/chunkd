// Package chunk splits a byte stream into fixed-size, content-addressed
// chunks without buffering the whole stream.
package chunk

import (
	"crypto/sha256"
	"errors"
	"hash"
	"io"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// DefaultSize is 4 MiB; see ADR-0005 for the comparison with GFS's 64 MiB.
const DefaultSize = 4 << 20

// Chunk is one piece of a stream. Data is owned by the caller.
type Chunk struct {
	Index int
	ID    iface.ChunkID
	Data  []byte
}

// ID returns the content address of data.
func ID(data []byte) iface.ChunkID { return sha256.Sum256(data) }

// Count returns how many chunks a stream of size bytes splits into. An empty
// stream has zero chunks.
func Count(size int64, chunkSize int) int {
	if size <= 0 {
		return 0
	}
	return int((size + int64(chunkSize) - 1) / int64(chunkSize))
}

// SizeOf returns the length of chunk i in a stream of size bytes.
func SizeOf(size int64, chunkSize, i int) int64 {
	if rest := size - int64(i)*int64(chunkSize); rest < int64(chunkSize) {
		return rest
	}
	return int64(chunkSize)
}

// Splitter reads a stream chunk by chunk and hashes the whole stream as it
// goes, so the file-level SHA-256 is known once the last chunk is read.
type Splitter struct {
	r     io.Reader
	size  int
	next  int
	total int64
	file  hash.Hash
	done  bool
}

// NewSplitter returns a splitter producing chunks of size bytes (the last may
// be shorter). size must be positive.
func NewSplitter(r io.Reader, size int) *Splitter {
	if size <= 0 {
		panic("chunk: size must be positive")
	}
	return &Splitter{r: r, size: size, file: sha256.New()}
}

// Next returns the next chunk, or io.EOF after the last one. Each chunk gets a
// fresh buffer: senders may still hold the previous one.
func (s *Splitter) Next() (Chunk, error) {
	if s.done {
		return Chunk{}, io.EOF
	}
	buf := make([]byte, s.size)
	n, err := io.ReadFull(s.r, buf)
	switch {
	case errors.Is(err, io.EOF): // zero bytes: stream ended on a chunk boundary
		s.done = true
		return Chunk{}, io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF): // short last chunk
		s.done = true
	case err != nil:
		return Chunk{}, err
	}
	buf = buf[:n]
	s.file.Write(buf)
	s.total += int64(n)
	c := Chunk{Index: s.next, ID: ID(buf), Data: buf}
	s.next++
	return c, nil
}

// Sum returns the SHA-256 of every byte read so far.
func (s *Splitter) Sum() [32]byte {
	var out [32]byte
	s.file.Sum(out[:0])
	return out
}

// Total returns the number of bytes read so far.
func (s *Splitter) Total() int64 { return s.total }
