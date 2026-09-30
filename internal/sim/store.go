package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"
	"sync"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// BlockStore is an in-memory iface.BlockStore. It copies on Put and Get so
// callers cannot alias stored bytes.
type BlockStore struct {
	mu          sync.Mutex
	chunks      map[iface.ChunkID][]byte
	quarantined map[iface.ChunkID][]byte
	term        uint64
}

var _ iface.BlockStore = (*BlockStore)(nil)

// NewBlockStore returns an empty store.
func NewBlockStore() *BlockStore {
	return &BlockStore{chunks: map[iface.ChunkID][]byte{}, quarantined: map[iface.ChunkID][]byte{}}
}

func (s *BlockStore) Put(_ context.Context, id iface.ChunkID, data []byte) error {
	if sha256.Sum256(data) != id {
		return iface.Errorf(iface.CodeInvalid, "data does not hash to %s", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Keep an intact copy; replace a missing or rotted one.
	if old, ok := s.chunks[id]; !ok || sha256.Sum256(old) != id {
		s.chunks[id] = bytes.Clone(data)
	}
	return nil
}

// Corrupt flips a byte of a stored chunk, simulating bit rot. It reports
// whether the chunk existed.
func (s *BlockStore) Corrupt(id iface.ChunkID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.chunks[id]
	if ok && len(b) > 0 {
		b[0] ^= 0xff
	}
	return ok
}

func (s *BlockStore) Term(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, nil
}

func (s *BlockStore) SaveTerm(_ context.Context, term uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	return nil
}

func (s *BlockStore) Usage(context.Context) (iface.Usage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := iface.Usage{Chunks: int64(len(s.chunks))}
	for _, b := range s.chunks {
		u.Bytes += int64(len(b))
	}
	return u, nil
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

func (s *BlockStore) Quarantine(_ context.Context, id iface.ChunkID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.chunks[id]; ok {
		s.quarantined[id] = b
		delete(s.chunks, id)
	}
	return nil
}

// Quarantined returns the IDs of quarantined chunks in ascending order.
func (s *BlockStore) Quarantined() []iface.ChunkID {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]iface.ChunkID, 0, len(s.quarantined))
	for id := range s.quarantined {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) })
	return ids
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

// MetaStore is an in-memory iface.MetaStore. Everything saved survives a
// simulated restart, which reuses the same MetaStore.
type MetaStore struct {
	mu      sync.Mutex
	base    iface.Index // index of the entry before entries[0]: the snapshot's
	entries [][]byte
	state   []byte
	snap    []byte
}

var _ iface.MetaStore = (*MetaStore)(nil)

// NewMetaStore returns an empty log.
func NewMetaStore() *MetaStore { return &MetaStore{} }

func (s *MetaStore) Save(_ context.Context, first iface.Index, entries [][]byte, state []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if first <= s.base || first > s.base+iface.Index(len(s.entries))+1 {
		return iface.Errorf(iface.CodeInvalid, "save at %d: log holds %d..%d", first, s.base+1, s.base+iface.Index(len(s.entries)))
	}
	s.entries = s.entries[:first-s.base-1]
	for _, e := range entries {
		s.entries = append(s.entries, bytes.Clone(e))
	}
	if state != nil {
		s.state = bytes.Clone(state)
	}
	return nil
}

func (s *MetaStore) State(context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.state), nil
}

func (s *MetaStore) Replay(_ context.Context, from iface.Index, fn func(iface.Index, []byte) error) error {
	s.mu.Lock()
	base, entries := s.base, slices.Clone(s.entries)
	s.mu.Unlock()
	for k, e := range entries {
		if i := base + iface.Index(k+1); i >= from {
			if err := fn(i, bytes.Clone(e)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *MetaStore) SaveSnapshot(_ context.Context, at iface.Index, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if at <= s.base {
		return nil
	}
	if keep := at - s.base; int(keep) < len(s.entries) {
		s.entries = slices.Clone(s.entries[keep:])
	} else {
		s.entries = nil
	}
	s.base, s.snap = at, bytes.Clone(data)
	return nil
}

func (s *MetaStore) LoadSnapshot(context.Context) (iface.Index, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base, bytes.Clone(s.snap), nil
}
