package blockstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
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
