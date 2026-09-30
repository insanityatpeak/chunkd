// Package metastore is the real-mode iface.MetaStore: an append-only WAL for
// log entries and the consensus state, and bbolt for snapshots.
//
// WAL layout: 16-byte header ("CHWAL002" + base index, little endian), then
// one record per Save: [len u32][crc32c u32][payload], payload = [first u64]
// [count u32][has state u8], count × ([len u32][entry]), then the state.
// Nothing is rewritten in place: a record drops every earlier entry at or
// after its first index, then appends its own, and the last state wins
// (etcd's WAL works the same way). One record per Save means a crash can
// only tear the last record, however the disk orders the unsynced writes.
package metastore

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/fsutil"
)

const (
	walName     = "meta.wal"
	dbName      = "snapshot.db"
	magic       = "CHWAL002"
	headerSize  = 16
	recHeader   = 8
	batchHeader = 13 // first u64, count u32, has state u8
	maxRecord   = 64 << 20
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	bucket     = []byte("snapshot")
	keyIndex   = []byte("index")
	keyData    = []byte("data")

	// ErrCorrupt means a record before the tail failed its checksum or
	// breaks the log's order. A torn tail is expected after a crash and
	// repaired; corruption in the middle is not, because truncating there
	// would silently drop acknowledged entries.
	ErrCorrupt = errors.New("metastore: WAL corrupt before tail")
)

// span locates one entry's or state's bytes in the WAL.
type span struct {
	off int64
	n   int
}

// Store implements iface.MetaStore. Safe for concurrent use.
type Store struct {
	mu      sync.Mutex
	dir     string
	wal     *os.File
	base    iface.Index // the snapshot index the WAL starts after
	entries []span      // entries[k] has index base+k+1
	state   *span
	end     int64
	db      *bolt.DB
}

var _ iface.MetaStore = (*Store)(nil)

// Open opens or creates the store in dir, repairing a torn WAL tail.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, dbName), 0o644, nil)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, db: db}
	if err := s.openWAL(); err != nil {
		if s.wal != nil {
			s.wal.Close()
		}
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) openWAL() error {
	path := filepath.Join(s.dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	at, _, err := s.loadSnapshot()
	if err != nil {
		f.Close()
		return err
	}
	if fi.Size() < headerSize {
		// New, or crashed while writing the header: start empty at the
		// snapshot index so indexes stay continuous.
		if err := writeHeader(f, at); err != nil {
			f.Close()
			return err
		}
		if err := fsutil.SyncDir(s.dir); err != nil {
			f.Close()
			return err
		}
	}
	s.wal = f
	if err := s.scan(); err != nil {
		return err
	}
	// A crash between committing a snapshot and rewriting the WAL leaves
	// entries the snapshot covers: finish the rewrite.
	if at > s.base {
		return s.compact(at)
	}
	return nil
}

func writeHeader(f *os.File, base iface.Index) error {
	var h [headerSize]byte
	copy(h[:], magic)
	binary.LittleEndian.PutUint64(h[8:], uint64(base))
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.WriteAt(h[:], 0); err != nil {
		return err
	}
	return f.Sync()
}

// scan reads every record, truncating a torn tail.
func (s *Store) scan() error {
	var h [headerSize]byte
	if _, err := s.wal.ReadAt(h[:], 0); err != nil {
		return err
	}
	if string(h[:8]) != magic {
		return fmt.Errorf("metastore: %s: bad magic %q (an older WAL format; start with an empty data directory)", walName, h[:8])
	}
	s.base = iface.Index(binary.LittleEndian.Uint64(h[8:]))
	fi, err := s.wal.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	s.entries, s.state = s.entries[:0], nil
	off := int64(headerSize)
	r := bufio.NewReaderSize(io.NewSectionReader(s.wal, off, size-off), 1<<20)
	for off < size {
		payload, n, ok, err := readRecord(r, size-off)
		if err != nil {
			return err
		}
		if !ok {
			if last := off+recHeader+int64(n) >= size; !last {
				return fmt.Errorf("%w at offset %d", ErrCorrupt, off)
			}
			// Torn write: the crash hit mid-record. Everything before it was
			// fsynced and acknowledged; this record never was.
			if err := s.wal.Truncate(off); err != nil {
				return err
			}
			if err := s.wal.Sync(); err != nil {
				return err
			}
			break
		}
		if err := s.index(payload, off+recHeader); err != nil {
			return fmt.Errorf("%w at offset %d: %v", ErrCorrupt, off, err)
		}
		off += recHeader + int64(n)
	}
	s.end = off
	return nil
}

// index applies one record, whose payload starts at file offset at, to the
// in-memory view of the log.
func (s *Store) index(p []byte, at int64) error {
	if len(p) < batchHeader {
		return fmt.Errorf("record of %d bytes", len(p))
	}
	first := iface.Index(binary.LittleEndian.Uint64(p))
	count := int(binary.LittleEndian.Uint32(p[8:]))
	hasState := p[12] == 1
	if first <= s.base || first > s.last()+1 {
		return fmt.Errorf("record at index %d, log holds %d..%d", first, s.base+1, s.last())
	}
	var add []span
	pos := batchHeader
	for range count {
		if pos+4 > len(p) {
			return fmt.Errorf("entry header past the record")
		}
		n := int(binary.LittleEndian.Uint32(p[pos:]))
		pos += 4
		if pos+n > len(p) {
			return fmt.Errorf("entry of %d bytes past the record", n)
		}
		add = append(add, span{off: at + int64(pos), n: n})
		pos += n
	}
	s.entries = append(s.entries[:first-s.base-1], add...)
	if hasState {
		s.state = &span{off: at + int64(pos), n: len(p) - pos}
	}
	return nil
}

func (s *Store) last() iface.Index { return s.base + iface.Index(len(s.entries)) }

// readRecord reads one record. ok is false if it is truncated or fails its
// checksum; n is the length the header claims (0 if the header is torn).
func readRecord(r io.Reader, remaining int64) (payload []byte, n uint32, ok bool, err error) {
	if remaining < recHeader {
		return nil, 0, false, nil
	}
	var hdr [recHeader]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, false, err
	}
	n = binary.LittleEndian.Uint32(hdr[:4])
	if n > maxRecord || int64(n) > remaining-recHeader {
		return nil, n, false, nil
	}
	payload = make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, n, false, err
	}
	return payload, n, crc32.Checksum(payload, castagnoli) == binary.LittleEndian.Uint32(hdr[4:]), nil
}

// encode builds one record: the record header, then the payload.
func encode(first iface.Index, entries [][]byte, state []byte) []byte {
	n := batchHeader + len(state)
	for _, e := range entries {
		n += 4 + len(e)
	}
	buf := make([]byte, recHeader, recHeader+n)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(first))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))
	if state != nil {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e)))
		buf = append(buf, e...)
	}
	buf = append(buf, state...)
	binary.LittleEndian.PutUint32(buf[:4], uint32(n))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(buf[recHeader:], castagnoli))
	return buf
}

// Save writes entries and state as one record and one fsync.
func (s *Store) Save(_ context.Context, first iface.Index, entries [][]byte, state []byte) error {
	n := batchHeader + len(state)
	for _, e := range entries {
		n += 4 + len(e)
	}
	if n > maxRecord {
		return iface.Errorf(iface.CodeInvalid, "save of %d bytes exceeds %d", n, maxRecord)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if first <= s.base || first > s.last()+1 {
		return iface.Errorf(iface.CodeInvalid, "save at %d: log holds %d..%d", first, s.base+1, s.last())
	}
	buf := encode(first, entries, state)
	if _, err := s.wal.WriteAt(buf, s.end); err != nil {
		return err
	}
	// SIMPLIFIED: one fsync per Save. The consensus layer already batches a
	// round's entries into one Save; etcd also pipelines fsyncs with sends.
	if err := s.wal.Sync(); err != nil {
		return err
	}
	// Validated above, so indexing cannot fail.
	_ = s.index(buf[recHeader:], s.end+recHeader)
	s.end += int64(len(buf))
	return nil
}

func (s *Store) read(sp span) ([]byte, error) {
	b := make([]byte, sp.n)
	_, err := s.wal.ReadAt(b, sp.off)
	return b, err
}

// State returns the last state saved, or nil.
func (s *Store) State(context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return nil, nil
	}
	return s.read(*s.state)
}

// Replay calls fn for each entry with index >= from. Entries covered by the
// snapshot are gone; callers load the snapshot first.
func (s *Store) Replay(_ context.Context, from iface.Index, fn func(iface.Index, []byte) error) error {
	s.mu.Lock()
	var idx []iface.Index
	var data [][]byte
	for k, sp := range s.entries {
		i := s.base + iface.Index(k+1)
		if i < from {
			continue
		}
		b, err := s.read(sp)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		idx, data = append(idx, i), append(data, b)
	}
	s.mu.Unlock()
	for k := range idx {
		if err := fn(idx[k], data[k]); err != nil {
			return err
		}
	}
	return nil
}

// SaveSnapshot stores data as covering every entry up to at, then drops those
// entries from the WAL. The snapshot is committed first: a crash between the
// two steps leaves extra WAL entries that Open drops, never a gap. A
// snapshot at or below the current one is ignored.
func (s *Store) SaveSnapshot(_ context.Context, at iface.Index, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if at <= s.base {
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		var idx [8]byte
		binary.LittleEndian.PutUint64(idx[:], uint64(at))
		if err := b.Put(keyIndex, idx[:]); err != nil {
			return err
		}
		return b.Put(keyData, data)
	})
	if err != nil {
		return err
	}
	return s.compact(at)
}

// compact rewrites the WAL as base at, the entries after at and the state:
// temp file, fsync, rename over the old WAL, fsync the directory.
func (s *Store) compact(at iface.Index) error {
	var keep [][]byte
	for k := int(at - s.base); k < len(s.entries); k++ {
		b, err := s.read(s.entries[k])
		if err != nil {
			return err
		}
		keep = append(keep, b)
	}
	var state []byte
	if s.state != nil {
		b, err := s.read(*s.state)
		if err != nil {
			return err
		}
		state = b
	}
	tmpPath := filepath.Join(s.dir, walName+".tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeHeader(tmp, at); err != nil {
		tmp.Close()
		return err
	}
	if len(keep) > 0 || state != nil {
		if _, err := tmp.WriteAt(encode(at+1, keep, state), headerSize); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()
	// Windows cannot rename over an open file.
	if err := s.wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(s.dir, walName)); err != nil {
		return err
	}
	if err := fsutil.SyncDir(s.dir); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, walName), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	s.wal = f
	return s.scan()
}

// LoadSnapshot returns the latest snapshot, or 0 and nil if there is none.
func (s *Store) LoadSnapshot(context.Context) (iface.Index, []byte, error) {
	return s.loadSnapshot()
}

func (s *Store) loadSnapshot() (iface.Index, []byte, error) {
	var at iface.Index
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		if v := b.Get(keyIndex); len(v) == 8 {
			at = iface.Index(binary.LittleEndian.Uint64(v))
		}
		if v := b.Get(keyData); v != nil {
			data = append([]byte(nil), v...)
		}
		return nil
	})
	return at, data, err
}

// LastIndex returns the index of the last entry (or the snapshot base).
func (s *Store) LastIndex() iface.Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last()
}

// Close closes the WAL and the snapshot database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.wal.Close(), s.db.Close())
}
