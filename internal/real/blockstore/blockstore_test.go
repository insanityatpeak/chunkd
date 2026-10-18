package blockstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/iface/ifacetest"
)

func open(t *testing.T, root string) *Store {
	t.Helper()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	ifacetest.BlockStore(t, func(t *testing.T) iface.BlockStore { return open(t, t.TempDir()) })
}

func TestLayout(t *testing.T) {
	root := t.TempDir()
	s := open(t, root)
	data := []byte("hello")
	id := iface.ChunkID(sha256.Sum256(data))
	if err := s.Put(context.Background(), id, data); err != nil {
		t.Fatal(err)
	}
	h := id.String()
	if _, err := os.Stat(filepath.Join(root, h[:2], h[2:4], h)); err != nil {
		t.Fatalf("chunk not at root/ab/cd/<hash>: %v", err)
	}
}

func TestPutRepairsRottedCopy(t *testing.T) {
	root := t.TempDir()
	s := open(t, root)
	ctx := context.Background()
	data := []byte("precious bytes")
	id := iface.ChunkID(sha256.Sum256(data))
	_ = s.Put(ctx, id, data)
	h := id.String()
	os.WriteFile(filepath.Join(root, h[:2], h[2:4], h), []byte("rotten"), 0o644)

	if err := s.Put(ctx, id, data); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, id)
	if !bytes.Equal(got, data) {
		t.Fatalf("after re-put Get = %q, want the original", got)
	}
	if u, _ := s.Usage(ctx); u != (iface.Usage{Chunks: 1, Bytes: int64(len(data))}) {
		t.Fatalf("usage = %+v", u)
	}
}

func TestReopenRecountsAndCleansTemp(t *testing.T) {
	root := t.TempDir()
	s := open(t, root)
	ctx := context.Background()
	for _, d := range []string{"a", "bb", "ccc"} {
		_ = s.Put(ctx, sha256.Sum256([]byte(d)), []byte(d))
	}
	// A crash mid-Put leaves a temp file behind.
	os.WriteFile(filepath.Join(root, tmpDir, "put-crashed"), []byte("partial"), 0o644)

	s = open(t, root)
	if u, _ := s.Usage(ctx); u != (iface.Usage{Chunks: 3, Bytes: 6}) {
		t.Fatalf("usage after reopen = %+v, want 3 chunks 6 bytes", u)
	}
	if entries, _ := os.ReadDir(filepath.Join(root, tmpDir)); len(entries) != 0 {
		t.Fatalf("temp files survived reopen: %v", entries)
	}
}

func TestQuarantineKeepsBytesOutOfCounts(t *testing.T) {
	root := t.TempDir()
	s := open(t, root)
	ctx := context.Background()
	data := []byte("soon rotten")
	id := iface.ChunkID(sha256.Sum256(data))
	_ = s.Put(ctx, id, data)
	h := id.String()
	os.WriteFile(filepath.Join(root, h[:2], h[2:4], h), []byte("rotten bytes"), 0o644)
	if err := s.Quarantine(ctx, id); err != nil {
		t.Fatal(err)
	}
	// Kept for inspection, exactly as found.
	if got, err := os.ReadFile(filepath.Join(root, quarantineDir, h)); err != nil || string(got) != "rotten bytes" {
		t.Fatalf("quarantined file = %q, %v", got, err)
	}
	s = open(t, root)
	if u, _ := s.Usage(ctx); u != (iface.Usage{}) {
		t.Fatalf("usage after reopen counts quarantine: %+v", u)
	}
}

func TestQuarantineCapEvictsOldest(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	s, err := Open(root, WithQuarantineCap(30))
	if err != nil {
		t.Fatal(err)
	}
	var ids []iface.ChunkID
	for _, d := range []string{"0123456789", "abcdefghij", "ABCDEFGHIJ", "klmnopqrst", "KLMNOPQRST"} {
		id := iface.ChunkID(sha256.Sum256([]byte(d)))
		if err := s.Put(ctx, id, []byte(d)); err != nil {
			t.Fatal(err)
		}
		if err := s.Quarantine(ctx, id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	held := func() []string {
		entries, _ := os.ReadDir(filepath.Join(root, quarantineDir))
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return names
	}
	want := []string{ids[2].String(), ids[3].String(), ids[4].String()}
	slices.Sort(want)
	if got := held(); !slices.Equal(got, want) {
		t.Fatalf("quarantine holds %v, want the 3 newest %v", got, want)
	}
	if u, _ := s.Usage(ctx); u != (iface.Usage{}) {
		t.Fatalf("usage = %+v after quarantining everything", u)
	}

	// A smaller cap on reopen trims at once, oldest first.
	if _, err := Open(root, WithQuarantineCap(10)); err != nil {
		t.Fatal(err)
	}
	if got := held(); !slices.Equal(got, []string{ids[4].String()}) {
		t.Fatalf("after reopen with a 10-byte cap quarantine holds %v, want only the newest", got)
	}
}

func TestRotFlipsChosenChunks(t *testing.T) {
	root := t.TempDir()
	s := open(t, root)
	ctx := context.Background()
	for _, d := range []string{"one", "two", "three"} {
		_ = s.Put(ctx, sha256.Sum256([]byte(d)), []byte(d))
	}
	rotted, err := Rot(root, 2, 7)
	if err != nil || len(rotted) != 2 {
		t.Fatalf("Rot = %v, %v", rotted, err)
	}
	again, _ := Rot(t.TempDir(), 2, 7) // empty store: nothing to do
	if len(again) != 0 {
		t.Fatalf("Rot on an empty store = %v", again)
	}
	bad := 0
	_ = s.List(ctx, func(id iface.ChunkID) error {
		b, _ := s.Get(ctx, id)
		if sha256.Sum256(b) != id {
			bad++
		}
		return nil
	})
	if bad != 2 {
		t.Fatalf("%d chunks fail verification, want 2", bad)
	}
}

// The fencing term is on the node's disk: it survives a restart.
func TestTermSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	s := open(t, root)
	if err := s.SaveTerm(context.Background(), 41); err != nil {
		t.Fatal(err)
	}
	if got, err := open(t, root).Term(context.Background()); err != nil || got != 41 {
		t.Fatalf("Term after reopen = %d, %v; want 41", got, err)
	}
	// A truncated file is an error, not a silent 0 that would readmit a
	// deposed leader.
	if err := os.WriteFile(filepath.Join(root, termFile), []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := open(t, root).Term(context.Background()); err == nil {
		t.Fatal("a corrupt term file read as valid")
	}
}
