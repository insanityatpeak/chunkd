package metastore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/iface/ifacetest"
)

var ctx = context.Background()

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func entries(t *testing.T, s *Store) []string {
	t.Helper()
	var out []string
	err := s.Replay(ctx, 0, func(i iface.Index, b []byte) error {
		out = append(out, fmt.Sprintf("%d:%s", i, b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func save(t *testing.T, s *Store, first iface.Index, state string, es ...string) {
	t.Helper()
	var bs [][]byte
	for _, e := range es {
		bs = append(bs, []byte(e))
	}
	var st []byte
	if state != "" {
		st = []byte(state)
	}
	if err := s.Save(ctx, first, bs, st); err != nil {
		t.Fatal(err)
	}
}

func TestConformance(t *testing.T) {
	ifacetest.MetaStore(t, func(t *testing.T) (iface.MetaStore, func() iface.MetaStore) {
		dir := t.TempDir()
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		cur := s
		t.Cleanup(func() { cur.Close() })
		return s, func() iface.MetaStore {
			cur.Close()
			n, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			cur = n
			return n
		}
	})
}

// Crash mid-write: the last record, a batch of three entries plus a state,
// is cut at every possible byte. Recovery keeps every earlier record whole
// and none of the torn one, and accepts saves after it.
func TestTornTailRecovers(t *testing.T) {
	base := t.TempDir()
	s := open(t, base)
	save(t, s, 1, "hs1", "first", "second")
	save(t, s, 3, "hs2", "third", "fourth", "fifth")
	s.Close()
	full, err := os.ReadFile(filepath.Join(base, walName))
	if err != nil {
		t.Fatal(err)
	}
	lastLen := len(encode(3, [][]byte{[]byte("third"), []byte("fourth"), []byte("fifth")}, []byte("hs2")))
	for cut := 1; cut <= lastLen; cut++ {
		t.Run(fmt.Sprintf("cut %d bytes", cut), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, walName), full[:len(full)-cut], 0o644); err != nil {
				t.Fatal(err)
			}
			s := open(t, dir)
			if got, want := entries(t, s), []string{"1:first", "2:second"}; !slices.Equal(got, want) {
				t.Fatalf("replay = %v, want %v", got, want)
			}
			if st, _ := s.State(ctx); string(st) != "hs1" {
				t.Fatalf("state = %q, want hs1", st)
			}
			save(t, s, 3, "", "after")
			if got := entries(t, s); !slices.Equal(got, []string{"1:first", "2:second", "3:after"}) {
				t.Fatalf("replay after repair = %v", got)
			}
		})
	}
}

func TestBadChecksumOnLastRecordIsTorn(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	save(t, s, 1, "", "ok")
	save(t, s, 2, "", "bad")
	s.Close()
	path := filepath.Join(dir, walName)
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff // sector of the last record never reached disk
	os.WriteFile(path, b, 0o644)

	if got := entries(t, open(t, dir)); !slices.Equal(got, []string{"1:ok"}) {
		t.Fatalf("replay = %v, want only the first record", got)
	}
}

func TestCorruptionBeforeTailIsAnError(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	save(t, s, 1, "", "one")
	save(t, s, 2, "", "two")
	s.Close()
	path := filepath.Join(dir, walName)
	b, _ := os.ReadFile(path)
	b[headerSize+recHeader+batchHeader+4] ^= 0xff // first entry's first byte
	os.WriteFile(path, b, 0o644)

	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt (never truncate acknowledged entries)", err)
	}
}

func TestSnapshotShrinksWAL(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	save(t, s, 1, "hs", "a", "b", "c", "d", "e")
	walBefore, _ := os.Stat(filepath.Join(dir, walName))
	if err := s.SaveSnapshot(ctx, 3, []byte("state@3")); err != nil {
		t.Fatal(err)
	}
	walAfter, _ := os.Stat(filepath.Join(dir, walName))
	if walAfter.Size() >= walBefore.Size() {
		t.Fatalf("WAL did not shrink: %d -> %d bytes", walBefore.Size(), walAfter.Size())
	}
	if got := entries(t, s); !slices.Equal(got, []string{"4:d", "5:e"}) {
		t.Fatalf("entries after snapshot = %v", got)
	}
}

func TestLostWALRestartsAtTheSnapshotIndex(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	save(t, s, 1, "", "a", "b")
	if err := s.SaveSnapshot(ctx, 2, []byte("s")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	os.Remove(filepath.Join(dir, walName))
	s = open(t, dir)
	if err := s.Save(ctx, 1, [][]byte{[]byte("x")}, nil); iface.CodeOf(err) != iface.CodeInvalid {
		t.Fatalf("save at 1 below the snapshot: %v, want CodeInvalid", err)
	}
	save(t, s, 3, "", "c")
	if got := entries(t, s); !slices.Equal(got, []string{"3:c"}) {
		t.Fatalf("entries = %v", got)
	}
}

// A crash after the snapshot commits and before the WAL is rewritten
// leaves covered entries in the WAL; Open drops them.
func TestCrashBetweenSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	save(t, s, 1, "hs", "a", "b", "c")
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		b.Put(keyIndex, binary.LittleEndian.AppendUint64(nil, 2))
		return b.Put(keyData, []byte("snap@2"))
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	s = open(t, dir)
	if got := entries(t, s); !slices.Equal(got, []string{"3:c"}) {
		t.Fatalf("entries = %v, want only those after the snapshot", got)
	}
	if st, _ := s.State(ctx); string(st) != "hs" {
		t.Fatalf("state = %q, want hs", st)
	}
	save(t, s, 4, "", "d")
}

func TestOldFormatIsRefused(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, walName), append([]byte("CHWAL001"), make([]byte, 8)...), 0o644)
	if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "older WAL format") {
		t.Fatalf("Open err = %v, want the older-format error", err)
	}
}
