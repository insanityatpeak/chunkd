// Package blockstore is the real-mode iface.BlockStore: one file per chunk at
// root/ab/cd/<sha256 hex>, written atomically.
package blockstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/fsutil"
)

const (
	tmpDir        = "tmp"
	quarantineDir = "quarantine"
)

// Store implements iface.BlockStore on a local directory. Safe for
// concurrent use.
// SIMPLIFIED: one hash over the whole chunk; HDFS keeps a CRC per 512-byte
// block in a sidecar file so reads can verify partial ranges. Chunks are
// read whole here, and at 4 MiB one SHA-256 is enough.
type Store struct {
	root  string
	mu    sync.Mutex // serialises the final rename and usage counters
	usage iface.Usage
}

var _ iface.BlockStore = (*Store)(nil)

// Open prepares root, removes temp files left by a crash, and counts usage.
func Open(root string) (*Store, error) {
	// A crash between write and rename leaves a temp file; it was never
	// acknowledged, so deleting it is safe.
	if err := os.RemoveAll(filepath.Join(root, tmpDir)); err != nil {
		return nil, err
	}
	for _, d := range []string{tmpDir, quarantineDir} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	s := &Store{root: filepath.Clean(root)}
	err := s.walk(func(_ iface.ChunkID, path string) error {
		fi, err := os.Stat(path)
		if err != nil {
			return err
		}
		s.usage.Chunks++
		s.usage.Bytes += fi.Size()
		return nil
	})
	return s, err
}

func (s *Store) path(id iface.ChunkID) string {
	h := id.String()
	return filepath.Join(s.root, h[0:2], h[2:4], h)
}

// Put stores data under id: write a temp file, fsync it, rename it into
// place, fsync the directory. The rename is the commit point; a crash before
// it leaves only a temp file, a crash after it leaves a complete chunk.
func (s *Store) Put(_ context.Context, id iface.ChunkID, data []byte) error {
	if sha256.Sum256(data) != id {
		return iface.Errorf(iface.CodeInvalid, "data does not hash to %s", id)
	}
	final := s.path(id)
	if intact(final, id) {
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, tmpDir), "put-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil { // data durable before the name points at it
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := s.mkdirs(filepath.Dir(final)); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, statErr := os.Stat(final)
	if statErr == nil && intact(final, id) {
		return nil // a concurrent Put of the same chunk won
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return err
	}
	if err := fsutil.SyncDir(filepath.Dir(final)); err != nil { // the rename itself durable
		return err
	}
	// Replacing a rotted copy changes nothing: the counters already hold
	// len(data) for this id, since a chunk's size is fixed by its hash.
	if statErr != nil {
		s.usage.Chunks++
		s.usage.Bytes += int64(len(data))
	}
	return nil
}

// mkdirs creates the fan-out directories, fsyncing each parent that gained a
// new entry so the directories survive a crash too.
func (s *Store) mkdirs(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := fsutil.SyncDir(filepath.Dir(dir)); err != nil {
		return err
	}
	return fsutil.SyncDir(s.root)
}

// intact reports whether path exists and hashes to id.
func intact(path string, id iface.ChunkID) bool {
	b, err := os.ReadFile(path)
	return err == nil && sha256.Sum256(b) == id
}

// Get returns the stored bytes. It does not verify them: readers do.
func (s *Store) Get(_ context.Context, id iface.ChunkID) ([]byte, error) {
	b, err := os.ReadFile(s.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, iface.ErrNotFound
	}
	return b, err
}

// Quarantine renames a chunk into quarantine/<hash>, replacing an older
// quarantined copy of the same chunk.
// SIMPLIFIED: quarantined files are never removed. HDFS deletes corrupt
// replicas once the NameNode has a good copy elsewhere; GC (Phase 4) will
// age them out here.
func (s *Store) Quarantine(_ context.Context, id iface.ChunkID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.path(id)
	fi, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Rename(p, filepath.Join(s.root, quarantineDir, id.String())); err != nil {
		return err
	}
	if err := fsutil.SyncDir(filepath.Dir(p)); err != nil {
		return err
	}
	if err := fsutil.SyncDir(filepath.Join(s.root, quarantineDir)); err != nil {
		return err
	}
	s.usage.Chunks--
	s.usage.Bytes -= fi.Size()
	return nil
}

// Delete removes a chunk; deleting a missing chunk succeeds.
func (s *Store) Delete(_ context.Context, id iface.ChunkID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.path(id)
	fi, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil {
		return err
	}
	s.usage.Chunks--
	s.usage.Bytes -= fi.Size()
	return fsutil.SyncDir(filepath.Dir(p))
}

// List visits chunks in ascending ID order (the fan-out is hex, so lexical
// directory order is byte order).
func (s *Store) List(_ context.Context, fn func(iface.ChunkID) error) error {
	return s.walk(func(id iface.ChunkID, _ string) error { return fn(id) })
}

func (s *Store) walk(fn func(iface.ChunkID, string) error) error {
	return filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if (d.Name() == tmpDir || d.Name() == quarantineDir) && filepath.Dir(path) == s.root {
				return filepath.SkipDir
			}
			return nil
		}
		raw, err := hex.DecodeString(d.Name())
		if err != nil || len(raw) != len(iface.ChunkID{}) {
			return nil // not a chunk file
		}
		var id iface.ChunkID
		copy(id[:], raw)
		if s.path(id) != path {
			return nil // right name, wrong directory
		}
		return fn(id, path)
	})
}

// Usage returns counters maintained on Put and Delete.
func (s *Store) Usage(context.Context) (iface.Usage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usage, nil
}

// Rot flips one byte in each of n chunk files under root, simulating bit
// rot for fault injection. Chunks are taken in ID order from index
// pick % count, so a seed picks the same files. It edits files in place and
// never opens the store: Open would delete a running node's temp files.
// The node only notices when it next reads the chunk (a client read, a
// repair copy, or the scrubber).
func Rot(root string, n int, pick uint64) ([]iface.ChunkID, error) {
	s := &Store{root: filepath.Clean(root)}
	var ids []iface.ChunkID
	if err := s.walk(func(id iface.ChunkID, _ string) error { ids = append(ids, id); return nil }); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	slices.SortFunc(ids, func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) })
	var rotted []iface.ChunkID
	for i := range min(n, len(ids)) {
		id := ids[(pick+uint64(i))%uint64(len(ids))]
		if err := flipByte(s.path(id)); err != nil {
			return rotted, err
		}
		rotted = append(rotted, id)
	}
	return rotted, nil
}

func flipByte(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		return err
	}
	b := make([]byte, 1)
	off := fi.Size() / 2
	if _, err := f.ReadAt(b, off); err != nil {
		return err
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		return err
	}
	return f.Sync()
}
