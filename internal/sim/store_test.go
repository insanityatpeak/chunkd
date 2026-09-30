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

func TestMetaStoreConformance(t *testing.T) {
	ifacetest.MetaStore(t, func(*testing.T) (iface.MetaStore, func() iface.MetaStore) {
		s := NewMetaStore()
		return s, func() iface.MetaStore { return s }
	})
}

func TestBlockStoreConformance(t *testing.T) {
	ifacetest.BlockStore(t, func(*testing.T) iface.BlockStore { return NewBlockStore() })
}
