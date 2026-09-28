package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/iface/ifacetest"
)

func TestBlockStore(t *testing.T) {
	ctx := context.Background()
	s := NewBlockStore()
	data := []byte("hello")
	id := iface.ChunkID(sha256.Sum256(data))

	if _, err := s.Get(ctx, id); !errors.Is(err, iface.ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
	if err := s.Put(ctx, id, data); err != nil {
		t.Fatal(err)
	}
	data[0] = 'X' // store must not alias the caller's buffer
	got, err := s.Get(ctx, id)
	if err != nil || string(got) != "hello" {
		t.Fatalf("Get = %q, %v; want hello", got, err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, id); !errors.Is(err, iface.ErrNotFound) {
		t.Fatalf("Get after Delete: err = %v, want ErrNotFound", err)
	}
}

func TestBlockStoreListIsSorted(t *testing.T) {
	ctx := context.Background()
	s := NewBlockStore()
	var want []iface.ChunkID
	for _, b := range []byte{9, 3, 7, 1} {
		id := iface.ChunkID(sha256.Sum256([]byte{b}))
		_ = s.Put(ctx, id, []byte{b})
		want = append(want, id)
	}
	slices.SortFunc(want, func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) })
	var got []iface.ChunkID
	_ = s.List(ctx, func(id iface.ChunkID) error { got = append(got, id); return nil })
	if !slices.Equal(got, want) {
		t.Fatalf("List order = %v, want %v", got, want)
	}
}

func TestMetaStore(t *testing.T) {
	ctx := context.Background()
	s := NewMetaStore()
	for i, e := range []string{"a", "b", "c"} {
		idx, err := s.Append(ctx, []byte(e))
		if err != nil || idx != iface.Index(i+1) {
			t.Fatalf("Append(%s) = %d, %v; want %d", e, idx, err, i+1)
		}
	}

	tests := []struct {
		from iface.Index
		want []string
	}{
		{0, []string{"a", "b", "c"}},
		{1, []string{"a", "b", "c"}},
		{3, []string{"c"}},
		{4, nil},
	}
	for _, tt := range tests {
		var got []string
		_ = s.Replay(ctx, tt.from, func(_ iface.Index, b []byte) error { got = append(got, string(b)); return nil })
		if !slices.Equal(got, tt.want) {
			t.Errorf("Replay(%d) = %v, want %v", tt.from, got, tt.want)
		}
	}

	if at, snap, _ := s.LoadSnapshot(ctx); at != 0 || snap != nil {
		t.Fatalf("empty snapshot = %d %q, want 0 nil", at, snap)
	}
	_ = s.SaveSnapshot(ctx, 2, []byte("state"))
	if at, snap, _ := s.LoadSnapshot(ctx); at != 2 || string(snap) != "state" {
		t.Fatalf("snapshot = %d %q, want 2 state", at, snap)
	}
}

func TestBlockStoreConformance(t *testing.T) {
	ifacetest.BlockStore(t, func(*testing.T) iface.BlockStore { return NewBlockStore() })
}
