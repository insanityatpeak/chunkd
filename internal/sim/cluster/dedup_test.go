package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
)

// storedBytes sums what every node's block store holds: physical bytes,
// replicas included.
func storedBytes(t *testing.T, c *Cluster) int64 {
	t.Helper()
	var total int64
	for _, n := range c.Nodes() {
		u, err := n.Store.Usage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		total += u.Bytes
	}
	return total
}

// TestDedupSavings uploads a 20 MiB base and 10 copies with one small
// in-place edit each, then one copy with a single byte inserted at the
// front. Fixed-size chunking stores an in-place edit as one new chunk, but
// the insert shifts every boundary and shares nothing with the base.
func TestDedupSavings(t *testing.T) {
	const base, edits = 20 << 20, 10
	c := New(1, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	chunk := int64(c.Config().Meta.ChunkSize)
	rf := int64(c.Config().Meta.Replicas)
	orig := c.RandomData("/base", base)
	put := func(path string, data []byte) client.Manifest {
		t.Helper()
		m, err := c.Client().Put(context.Background(), path, bytes.NewReader(data), int64(len(data)), client.PutOptions{})
		if err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		c.Tick(time.Second) // block reports make the chunks claimable as present
		return m
	}
	put("/base", orig)
	for i := range edits {
		v := bytes.Clone(orig)
		// One 64-byte edit inside chunk i%5 changes that chunk only.
		off := int64(i%5)*chunk + 1000 + int64(i)
		copy(v[off:], fmt.Sprintf("edit %02d %058d", i, i))
		m := put(fmt.Sprintf("/edit-%02d", i), v)
		deduped := 0
		for _, ch := range m.Chunk {
			if ch.Deduped {
				deduped++
			}
		}
		if deduped != 4 {
			t.Fatalf("edit %d: %d of 5 chunks deduped, want 4", i, deduped)
		}
	}
	if _, ok := c.Settle(time.Minute); !ok {
		t.Fatal("did not settle")
	}
	logical := int64(base * (1 + edits))
	wantUnique := int64(base) + edits*chunk
	ref, uniq := c.Meta().State().Dedup()
	if ref != logical || uniq != wantUnique {
		t.Fatalf("referenced %d, unique %d; want %d, %d", ref, uniq, logical, wantUnique)
	}
	if got := storedBytes(t, c); got != rf*wantUnique {
		t.Fatalf("nodes store %d bytes, want %d (RF %d × %d distinct)", got, rf*wantUnique, rf, wantUnique)
	}
	if got, want := c.Meta().DedupSkipped(), uint64(edits*(base-chunk)); got != want {
		t.Fatalf("clients skipped %d bytes, want %d", got, want)
	}
	t.Logf("in-place edits: %d MiB logical, %d MiB distinct, ratio %.2f", logical>>20, uniq>>20, float64(logical)/float64(uniq))

	shifted := append([]byte{0}, orig...)
	m := put("/insert", shifted)
	for _, ch := range m.Chunk {
		if ch.Deduped {
			t.Fatalf("chunk %d of the shifted copy deduped; fixed-size chunks should share nothing after an insert", ch.Index)
		}
	}
	_, after := c.Meta().State().Dedup()
	t.Logf("1-byte insert at offset 0: %d MiB new distinct bytes for a %d MiB file", (after-uniq)>>20, len(shifted)>>20)
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}
