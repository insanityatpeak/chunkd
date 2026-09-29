package cluster

import (
	"bytes"
	"encoding/hex"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// corruptionBound is how long RF may take to return after a corrupt copy is
// detected: no death and no delay, so one copy (4 MiB at 40 MiB/s), one
// copy timeout for a lost message, and slack.
const corruptionBound = 100*time.Millisecond + 10*time.Second + 5*time.Second

// oneChunk uploads a 1 MiB file at full replication and returns its chunk
// and replicas in the order a fresh client reads them.
func oneChunk(t *testing.T, seed uint64) (*Cluster, []byte, iface.ChunkID, []iface.NodeID) {
	t.Helper()
	c := New(seed, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	_, data, err := c.UploadRandom("/f", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Settle(time.Minute); !ok {
		t.Fatal("not at RF before corruption")
	}
	st, err := c.Client().Stat(t.Context(), "/f")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := hex.DecodeString(st.Chunk[0].ID)
	var reps []iface.NodeID
	for _, r := range st.Chunk[0].Replicas {
		reps = append(reps, iface.NodeID(r))
	}
	if len(reps) != 3 {
		t.Fatalf("replicas %v", reps)
	}
	return c, data, iface.ChunkID(raw), reps
}

// A fresh client scores every node 0 and tries replicas in the metadata
// server's order, so corrupting the first k listed replicas makes the read
// meet them first.
func corruptFirst(t *testing.T, c *Cluster, id iface.ChunkID, reps []iface.NodeID, k int) {
	t.Helper()
	for _, n := range reps[:k] {
		if !c.node(n).Store.Corrupt(id) {
			t.Fatalf("%s does not hold the chunk", n)
		}
	}
}

func restoredWithin(t *testing.T, c *Cluster, bound time.Duration) time.Duration {
	t.Helper()
	took, ok := c.Settle(2 * bound)
	if !ok || took > bound {
		t.Fatalf("RF restored after %v (ok=%v), bound %v", took, ok, bound)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
	return took
}

func TestCorruptChunkDetectedOnRead(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		c, data, id, reps := oneChunk(t, seed)
		bad := reps[0]
		corruptFirst(t, c, id, reps, 1)

		got, m, err := c.Download("/f")
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("seed %d: download: %v", seed, err)
		}
		if ch := m.Chunk[0]; !slices.Contains(ch.Rejected, string(bad)) || ch.ServedBy == string(bad) {
			t.Fatalf("seed %d: served by %s, rejected %v; want %s rejected", seed, ch.ServedBy, ch.Rejected, bad)
		}
		if q := c.node(bad).Store.Quarantined(); !slices.Equal(q, []iface.ChunkID{id}) {
			t.Fatalf("seed %d: quarantined on %s: %v", seed, bad, q)
		}
		took := restoredWithin(t, c, corruptionBound)
		t.Logf("seed %d: %s quarantined, RF 3 after %v", seed, bad, took)
	}
}

func TestTwoReplicasCorrupt(t *testing.T) {
	c, data, id, reps := oneChunk(t, 4)
	corruptFirst(t, c, id, reps, 2)
	got, _, err := c.Download("/f")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download from the one good copy: %v", err)
	}
	for _, n := range reps[:2] {
		if len(c.node(n).Store.Quarantined()) != 1 {
			t.Fatalf("%s did not quarantine", n)
		}
	}
	// One live copy left skips the repair delay; two copies from it.
	restoredWithin(t, c, corruptionBound+5*time.Second)
	if st := c.Meta().Repair().Stats(); st.Completed < 2 {
		t.Fatalf("%d copies, want at least 2", st.Completed)
	}
}

func TestAllReplicasCorrupt(t *testing.T) {
	c, _, id, reps := oneChunk(t, 5)
	corruptFirst(t, c, id, reps, 3)
	_, _, err := c.Download("/f")
	if iface.CodeOf(err) != iface.CodeCorrupt {
		t.Fatalf("download error %v (code %v), want corrupt", err, iface.CodeOf(err))
	}
	t.Logf("error: %v", err)
	c.Tick(5 * time.Second)
	// No copy left anywhere to repair from: counted as lost, not copied.
	if h := c.Meta().Health(); h.Lost != 1 || h.Repair.Completed != 0 {
		t.Fatalf("health %+v", h)
	}
}

// TestSuspectHintMakesNodeRecheck: a client's hint about bad bytes makes the
// node re-check; only the node's own check removes a copy.
func TestSuspectHintMakesNodeRecheck(t *testing.T) {
	c, _, id, reps := oneChunk(t, 6)
	caller := c.NewCaller("client-9")
	hint := func(node iface.NodeID) {
		r := caller.Do(t.Context(), []iface.Call{{To: MetaID, Kind: wire.KindSuspect,
			Body: wire.Marshal(&chunkdv1.SuspectRequest{ChunkId: id[:], Node: string(node)})}})
		if r[0].Err != nil {
			t.Fatal(r[0].Err)
		}
		c.Tick(time.Second)
	}
	// An intact copy survives a hint: the path was at fault, not the disk.
	hint(reps[0])
	if len(c.node(reps[0]).Store.Quarantined()) != 0 || len(c.Meta().Cluster().Locations(id)) != 3 {
		t.Fatal("intact copy removed on a client's word")
	}
	// A rotten copy that nobody has read yet is found by the re-check.
	corruptFirst(t, c, id, reps[1:], 1)
	hint(reps[1])
	if len(c.node(reps[1]).Store.Quarantined()) != 1 {
		t.Fatal("rotten copy not quarantined after the hint")
	}
	restoredWithin(t, c, corruptionBound)
}
