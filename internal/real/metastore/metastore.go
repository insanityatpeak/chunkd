// Package metastore is the real-mode iface.MetaStore: an append-only WAL for
// ops and bbolt for snapshots.
//
// WAL layout: 16-byte header ("CHWAL001" + base index, little endian), then
// records of [len u32][crc32c u32][payload]. Entry k in the file has index
// base+k. A record is durable once Append's fsync returns.
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
	walName    = "meta.wal"
	dbName     = "snapshot.db"
	magic      = "CHWAL001"
	headerSize = 16
	recHeader  = 8
	maxRecord  = 64 << 20
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	bucket     = []byte("snapshot")
	keyIndex   = []byte("index")
	keyData    = []byte("data")

	// ErrCorrupt means a record before the tail failed its checksum. A torn
	// tail is expected after a crash and repaired; corruption in the middle
	// is not, because truncating there would silently drop acknowledged ops.
	ErrCorrupt = errors.New("metastore: WAL corrupt before tail")
)

// Store implements iface.MetaStore. Safe for concurrent use.
type Store struct {
	mu      sync.Mutex
	dir     string
	wal     *os.File
	base    iface.Index
	offsets []int64 // file offset of each record
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
	if fi.Size() < headerSize {
		// New, or crashed while writing the header: start empty at the
		// snapshot index so indexes stay continuous.
		at, _, err := s.loadSnapshot()
		if err != nil {
			f.Close()
			return err
		}
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
	return s.scan()
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
		return fmt.Errorf("metastore: %s: bad magic", walName)
	}
	s.base = iface.Index(binary.LittleEndian.Uint64(h[8:]))
	fi, err := s.wal.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	s.offsets = s.offsets[:0]
	off := int64(headerSize)
	r := bufio.NewReaderSize(io.NewSectionReader(s.wal, off, size-off), 1<<20)
	for off < size {
		n, ok, err := readRecord(r, size-off)
		if err != nil {
			return err
		}
		if !ok {
			last := off+recHeader+int64(n) >= size
			if !last {
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
		s.offsets = append(s.offsets, off)
		off += recHeader + int64(n)
	}
	s.end = off
	return nil
}

// readRecord reads one record. ok is false if it is truncated or fails its
// checksum; n is the length the header claims (0 if the header is torn).
func readRecord(r io.Reader, remaining int64) (n uint32, ok bool, err error) {
	if remaining < recHeader {
		return 0, false, nil
	}
	var hdr [recHeader]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, false, err
	}
	n = binary.LittleEndian.Uint32(hdr[:4])
	if n > maxRecord || int64(n) > remaining-recHeader {
		return n, false, nil
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return n, false, err
	}
	return n, crc32.Checksum(payload, castagnoli) == binary.LittleEndian.Uint32(hdr[4:]), nil
}

// Append writes entry and fsyncs before returning its index.
func (s *Store) Append(_ context.Context, entry []byte) (iface.Index, error) {
	if len(entry) > maxRecord {
		return 0, iface.Errorf(iface.CodeInvalid, "entry of %d bytes exceeds %d", len(entry), maxRecord)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]byte, recHeader+len(entry))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(entry)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(entry, castagnoli))
	copy(buf[recHeader:], entry)
	if _, err := s.wal.WriteAt(buf, s.end); err != nil {
		return 0, err
	}
	// SIMPLIFIED: one fsync per op. etcd and HDFS's edit log batch
	// concurrent appends into one fsync (group commit).
	if err := s.wal.Sync(); err != nil {
		return 0, err
	}
	s.offsets = append(s.offsets, s.end)
	s.end += int64(len(buf))
	return s.base + iface.Index(len(s.offsets)), nil
}

// Replay calls fn for each WAL entry with index >= from. Entries covered by
// the snapshot are gone; callers load the snapshot first.
func (s *Store) Replay(_ context.Context, from iface.Index, fn func(iface.Index, []byte) error) error {
	s.mu.Lock()
	offsets := append([]int64(nil), s.offsets...)
	base := s.base
	s.mu.Unlock()
	for k, off := range offsets {
		idx := base + iface.Index(k+1)
		if idx < from {
			continue
		}
		var hdr [recHeader]byte
		if _, err := s.wal.ReadAt(hdr[:], off); err != nil {
			return err
		}
		payload := make([]byte, binary.LittleEndian.Uint32(hdr[:4]))
		if _, err := s.wal.ReadAt(payload, off+recHeader); err != nil {
			return err
		}
		if err := fn(idx, payload); err != nil {
			return err
		}
	}
	return nil
}

// SaveSnapshot stores data as covering every entry up to at, then drops those
// entries from the WAL. The snapshot is committed first: a crash between the
// two steps leaves extra WAL entries that replay skips, never a gap.
func (s *Store) SaveSnapshot(_ context.Context, at iface.Index, data []byte) error {
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

// compact rewrites the WAL keeping entries after at: temp file, fsync,
// rename over the old WAL, fsync the directory.
func (s *Store) compact(at iface.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if at <= s.base {
		return nil
	}
	tmpPath := filepath.Join(s.dir, walName+".tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	newBase := min(at, s.base+iface.Index(len(s.offsets)))
	if err := writeHeader(tmp, newBase); err != nil {
		tmp.Close()
		return err
	}
	keepFrom := int(newBase - s.base)
	if keepFrom < len(s.offsets) {
		start := s.offsets[keepFrom]
		if _, err := io.Copy(io.NewOffsetWriter(tmp, headerSize), io.NewSectionReader(s.wal, start, s.end-start)); err != nil {
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
		data = append([]byte(nil), b.Get(keyData)...)
		return nil
	})
	return at, data, err
}

// LastIndex returns the index of the last entry (or the snapshot base).
func (s *Store) LastIndex() iface.Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base + iface.Index(len(s.offsets))
}

// Close closes the WAL and the snapshot database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.wal.Close(), s.db.Close())
}
