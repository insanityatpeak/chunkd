// Package ifacetest holds conformance suites that every implementation of an
// iface seam must pass, so sim and real behave the same where core can see.
package ifacetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

func chunkOf(n int, seed byte) ([]byte, iface.ChunkID) {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*31)
	}
	return b, sha256.Sum256(b)
}

// BlockStore runs the BlockStore conformance suite. open returns a fresh,
// empty store for each subtest.
func BlockStore(t *testing.T, open func(t *testing.T) iface.BlockStore) {
	ctx := context.Background()

	t.Run("put get round trip", func(t *testing.T) {
		s := open(t)
		for _, n := range []int{0, 1, 4<<20 - 1, 4 << 20} {
			data, id := chunkOf(n, byte(n))
			if err := s.Put(ctx, id, data); err != nil {
				t.Fatalf("Put(%d bytes): %v", n, err)
			}
			got, err := s.Get(ctx, id)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("Get(%d bytes): %d bytes, %v", n, len(got), err)
			}
		}
	})

	t.Run("get missing", func(t *testing.T) {
		_, id := chunkOf(3, 1)
		if _, err := open(t).Get(ctx, id); !errors.Is(err, iface.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("wrong id rejected", func(t *testing.T) {
		s := open(t)
		data, _ := chunkOf(10, 1)
		_, other := chunkOf(10, 2)
		if err := s.Put(ctx, other, data); iface.CodeOf(err) != iface.CodeInvalid {
			t.Fatalf("err = %v, want CodeInvalid", err)
		}
		if _, err := s.Get(ctx, other); !errors.Is(err, iface.ErrNotFound) {
			t.Fatal("rejected chunk was stored")
		}
	})

	t.Run("put is idempotent", func(t *testing.T) {
		s := open(t)
		data, id := chunkOf(100, 7)
		for range 3 {
			if err := s.Put(ctx, id, data); err != nil {
				t.Fatal(err)
			}
		}
		if u, _ := s.Usage(ctx); u != (iface.Usage{Chunks: 1, Bytes: 100}) {
			t.Fatalf("usage after 3 identical puts = %+v", u)
		}
	})

	t.Run("caller buffers not aliased", func(t *testing.T) {
		s := open(t)
		data, id := chunkOf(16, 3)
		orig := bytes.Clone(data)
		_ = s.Put(ctx, id, data)
		data[0] ^= 0xff
		got, _ := s.Get(ctx, id)
		got[1] ^= 0xff
		again, _ := s.Get(ctx, id)
		if !bytes.Equal(again, orig) {
			t.Fatal("store shares memory with a caller")
		}
	})

	t.Run("delete", func(t *testing.T) {
		s := open(t)
		data, id := chunkOf(50, 4)
		_ = s.Put(ctx, id, data)
		for range 2 { // second delete of a missing chunk also succeeds
			if err := s.Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Get(ctx, id); !errors.Is(err, iface.ErrNotFound) {
			t.Fatalf("Get after Delete: %v", err)
		}
		if u, _ := s.Usage(ctx); u != (iface.Usage{}) {
			t.Fatalf("usage after delete = %+v", u)
		}
	})

	t.Run("list sorted and complete", func(t *testing.T) {
		s := open(t)
		var want []iface.ChunkID
		var bytesTotal int64
		for i := range 20 {
			data, id := chunkOf(i+1, byte(i))
			_ = s.Put(ctx, id, data)
			want = append(want, id)
			bytesTotal += int64(i + 1)
		}
		slices.SortFunc(want, func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) })
		var got []iface.ChunkID
		if err := s.List(ctx, func(id iface.ChunkID) error { got = append(got, id); return nil }); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("List returned %d ids (sorted=%v), want %d", len(got), slices.IsSortedFunc(got, func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) }), len(want))
		}
		if u, _ := s.Usage(ctx); u != (iface.Usage{Chunks: 20, Bytes: bytesTotal}) {
			t.Fatalf("usage = %+v, want 20 chunks %d bytes", u, bytesTotal)
		}
		stop := errors.New("stop")
		n := 0
		if err := s.List(ctx, func(iface.ChunkID) error { n++; return stop }); !errors.Is(err, stop) || n != 1 {
			t.Fatalf("List did not stop on callback error: n=%d err=%v", n, err)
		}
	})

	t.Run("concurrent puts", func(t *testing.T) {
		s := open(t)
		var wg sync.WaitGroup
		errs := make(chan error, 64)
		for i := range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data, id := chunkOf(1000, byte(i%8)) // 8 distinct chunks, each put 4 times
				if err := s.Put(ctx, id, data); err != nil {
					errs <- fmt.Errorf("put %d: %w", i, err)
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if u, _ := s.Usage(ctx); u != (iface.Usage{Chunks: 8, Bytes: 8000}) {
			t.Fatalf("usage = %+v, want 8 chunks 8000 bytes", u)
		}
	})
}
