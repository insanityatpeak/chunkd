package metastore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

var ctx = context.Background()

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func entries(t *testing.T, s *Store, from iface.Index) []string {
	t.Helper()
	var out []string
	err := s.Replay(ctx, from, func(i iface.Index, b []byte) error {
		out = append(out, fmt.Sprintf("%d:%s", i, b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func appendAll(t *testing.T, s *Store, es ...string) {
	t.Helper()
	for _, e := range es {
		if _, err := s.Append(ctx, []byte(e)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAppendReplayAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	appendAll(t, s, "a", "b", "c")
	s.Close()

	s = open(t, dir)
	defer s.Close()
	if got, want := entries(t, s, 1), []string{"1:a", "2:b", "3:c"}; !slices.Equal(got, want) {
		t.Fatalf("replay = %v, want %v", got, want)
	}
	if got := entries(t, s, 3); !slices.Equal(got, []string{"3:c"}) {
		t.Fatalf("replay from 3 = %v", got)
	}
	if i, _ := s.Append(ctx, []byte("d")); i != 4 {
		t.Fatalf("next index %d, want 4", i)
	}
}

// Crash mid-write: the last record is cut at every possible byte. Recovery
// must keep every complete record and accept new appends after them.
func TestTornTailRecovers(t *testing.T) {
	base := t.TempDir()
	s := open(t, base)
	appendAll(t, s, "first", "second", "third")
	s.Close()
	full, err := os.ReadFile(filepath.Join(base, walName))
	if err != nil {
		t.Fatal(err)
	}
	thirdLen := recHeader + len("third")
	for cut := 1; cut <= thirdLen; cut++ {
		t.Run(fmt.Sprintf("cut %d bytes", cut), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, walName), full[:len(full)-cut], 0o644); err != nil {
				t.Fatal(err)
			}
			s := open(t, dir)
			defer s.Close()
			if got, want := entries(t, s, 1), []string{"1:first", "2:second"}; !slices.Equal(got, want) {
				t.Fatalf("replay = %v, want %v", got, want)
			}
			if i, _ := s.Append(ctx, []byte("after")); i != 3 {
				t.Fatalf("append after repair got index %d, want 3", i)
			}
			if got := entries(t, s, 3); !slices.Equal(got, []string{"3:after"}) {
				t.Fatalf("replay after repair = %v", got)
			}
		})
	}
}

func TestBadChecksumOnLastRecordIsTorn(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	appendAll(t, s, "ok", "bad")
	s.Close()
	path := filepath.Join(dir, walName)
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff // sector of the last record never reached disk
	os.WriteFile(path, b, 0o644)

	s = open(t, dir)
	defer s.Close()
	if got := entries(t, s, 1); !slices.Equal(got, []string{"1:ok"}) {
		t.Fatalf("replay = %v, want only the first record", got)
	}
}

func TestCorruptionBeforeTailIsAnError(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	appendAll(t, s, "one", "two", "three")
	s.Close()
	path := filepath.Join(dir, walName)
	b, _ := os.ReadFile(path)
	b[headerSize+recHeader] ^= 0xff // first payload byte
	os.WriteFile(path, b, 0o644)

	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt (never truncate acknowledged ops)", err)
	}
}

func TestSnapshotTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	appendAll(t, s, "a", "b", "c", "d", "e")
	walBefore, _ := os.Stat(filepath.Join(dir, walName))
	if err := s.SaveSnapshot(ctx, 3, []byte("state@3")); err != nil {
		t.Fatal(err)
	}
	walAfter, _ := os.Stat(filepath.Join(dir, walName))
	if walAfter.Size() >= walBefore.Size() {
		t.Fatalf("WAL did not shrink: %d -> %d bytes", walBefore.Size(), walAfter.Size())
	}
	appendAll(t, s, "f")
	s.Close()

	s = open(t, dir)
	defer s.Close()
	at, snap, err := s.LoadSnapshot(ctx)
	if err != nil || at != 3 || string(snap) != "state@3" {
		t.Fatalf("snapshot = %d %q %v", at, snap, err)
	}
	if got, want := entries(t, s, at+1), []string{"4:d", "5:e", "6:f"}; !slices.Equal(got, want) {
		t.Fatalf("replay after snapshot = %v, want %v", got, want)
	}
}

func TestSnapshotAtHeadThenReopenKeepsIndexes(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	appendAll(t, s, "a", "b")
	if err := s.SaveSnapshot(ctx, 2, []byte("s")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Losing the WAL entirely (e.g. deleted after the snapshot) restarts it
	// at the snapshot index, not at 1.
	os.Remove(filepath.Join(dir, walName))
	s = open(t, dir)
	defer s.Close()
	if i, _ := s.Append(ctx, []byte("c")); i != 3 {
		t.Fatalf("index after snapshot-only restart = %d, want 3", i)
	}
}

func TestEmptyStore(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()
	at, snap, err := s.LoadSnapshot(ctx)
	if err != nil || at != 0 || len(snap) != 0 {
		t.Fatalf("empty snapshot = %d %q %v", at, snap, err)
	}
	if got := entries(t, s, 1); len(got) != 0 {
		t.Fatalf("empty replay = %v", got)
	}
}
