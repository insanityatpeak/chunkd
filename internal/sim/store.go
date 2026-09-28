package sim

import (
	"bytes"
	"context"
	"slices"
	"sync"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// BlockStore is an in-memory iface.BlockStore. It copies on Put and Get so
// callers cannot alias stored bytes.
type BlockStore struct {
	mu     sync.Mutex
	chunks map[iface.ChunkID][]byte
}

var _ iface.BlockStore = (*BlockStore)(nil)

// NewBlockStore returns an empty store.
func NewBlockStore() *BlockStore { return &BlockStore{chunks: map[iface.ChunkID][]byte{}} }

func (s *BlockStore) Put(_ context.Context, id iface.ChunkID, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks[id] = bytes.Clone(data)
	return nil
}

func (s *BlockStore) Get(_ context.Context, id iface.ChunkID) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.chunks[id]
	if !ok {
		return nil, iface.ErrNotFound
	}
	return bytes.Clone(b), nil
}

func (s *BlockStore) Delete(_ context.Context, id iface.ChunkID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.chunks, id)
	return nil
}

// List visits chunks in ascending ID order; Go map order is randomized and
// would break seed replay.
func (s *BlockStore) List(_ context.Context, fn func(iface.ChunkID) error) error {
	s.mu.Lock()
	ids := make([]iface.ChunkID, 0, len(s.chunks))
	for id := range s.chunks {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	slices.SortFunc(ids, func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) })
	for _, id := range ids {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// MetaStore is an in-memory iface.MetaStore.
// SIMPLIFIED: entries are never truncated after a snapshot. HDFS and etcd
// compact the log up to the snapshot index to bound disk and replay time.
type MetaStore struct {
	mu      sync.Mutex
	entries [][]byte
	snapAt  iface.Index
	snap    []byte
}

var _ iface.MetaStore = (*MetaStore)(nil)

// NewMetaStore returns an empty log.
func NewMetaStore() *MetaStore { return &MetaStore{} }

func (s *MetaStore) Append(_ context.Context, entry []byte) (iface.Index, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, bytes.Clone(entry))
	return iface.Index(len(s.entries)), nil
}

func (s *MetaStore) Replay(_ context.Context, from iface.Index, fn func(iface.Index, []byte) error) error {
	s.mu.Lock()
	entries := slices.Clone(s.entries)
	s.mu.Unlock()
	if from < 1 {
		from = 1
	}
	for i := int(from) - 1; i < len(entries); i++ {
		if err := fn(iface.Index(i+1), bytes.Clone(entries[i])); err != nil {
			return err
		}
	}
	return nil
}

func (s *MetaStore) SaveSnapshot(_ context.Context, at iface.Index, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapAt, s.snap = at, bytes.Clone(data)
	return nil
}

func (s *MetaStore) LoadSnapshot(_ context.Context) (iface.Index, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapAt, bytes.Clone(s.snap), nil
}
