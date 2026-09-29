package cluster

import (
	"bytes"
	"encoding/hex"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

func chunkID(t *testing.T, hexID string) iface.ChunkID {
	t.Helper()
	raw, err := hex.DecodeString(hexID)
	if err != nil || len(raw) != len(iface.ChunkID{}) {
		t.Fatalf("bad chunk id %q", hexID)
	}
	return iface.ChunkID(raw)
}

// TestNodeQuarantinesCorruptChunkOnRead: a node serving a chunk whose bytes
// rotted answers CodeCorrupt instead of the bytes, quarantines the file, and
// the client reads the chunk from another replica.
func TestNodeQuarantinesCorruptChunkOnRead(t *testing.T) {
	c := New(11, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	m, data, err := c.UploadRandom("/f", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	c.Settle(time.Minute)
	st, err := c.Client().Stat(t.Context(), "/f")
	if err != nil {
		t.Fatal(err)
	}
	ref := st.Chunk[0]
	id := chunkID(t, ref.ID)
	// A fresh client scores every node 0 and tries replicas in the metadata
	// server's order, so the first listed replica is read first.
	bad := iface.NodeID(ref.Replicas[0])
	if !c.node(bad).Store.Corrupt(id) {
		t.Fatalf("%s does not hold chunk %s", bad, ref.ID[:12])
	}

	got, dm, err := c.Download(m.Path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download: %v", err)
	}
	if !slices.Contains(dm.Chunk[0].Rejected, string(bad)) || dm.Chunk[0].ServedBy == string(bad) {
		t.Fatalf("chunk 0 served by %s, rejected %v; want %s rejected", dm.Chunk[0].ServedBy, dm.Chunk[0].Rejected, bad)
	}
	if q := c.node(bad).Store.Quarantined(); !slices.Equal(q, []iface.ChunkID{id}) {
		t.Fatalf("quarantined on %s: %v", bad, q)
	}
	if n := c.node(bad).Stats().Corrupt; n != 1 {
		t.Fatalf("corrupt count %d", n)
	}
}
