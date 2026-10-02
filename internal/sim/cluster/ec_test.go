package cluster

import (
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// ecCluster is the default cluster with a sixth node: a stripe needs 6.
func ecCluster(t *testing.T, seed uint64) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Nodes = ec.TotalShards
	c := New(seed, cfg, io.Discard)
	c.Tick(3 * time.Second)
	return c
}

func TestECRoundTrip(t *testing.T) {
	c := ecCluster(t, 1)
	const size = 9 << 20 // chunks of 4, 4 and 1 MiB
	m, _, err := c.Session().UploadAs("/ec", c.RandomData("/ec", size), client.EC42)
	if err != nil {
		t.Fatal(err)
	}
	if m.Redundancy != client.EC42 || len(m.Chunk) != 3 {
		t.Fatalf("manifest: %q, %d chunks", m.Redundancy, len(m.Chunk))
	}
	for i, ch := range m.Chunk {
		nodes := map[string]bool{}
		for _, s := range ch.Shards {
			for _, n := range s.Replicas {
				nodes[n] = true
			}
		}
		if len(ch.Shards) != ec.TotalShards || len(nodes) < ec.CommitShards || len(ch.Replicas) != 0 {
			t.Fatalf("chunk %d: %d shards on %d nodes, %d replicas", i, len(ch.Shards), len(nodes), len(ch.Replicas))
		}
	}
	_, got, err := c.Download("/ec")
	if err != nil {
		t.Fatal(err)
	}
	if got.Redundancy != client.EC42 || slices.ContainsFunc(got.Chunk, func(r client.ChunkRef) bool { return r.Decoded }) {
		t.Fatalf("healthy read: %q, decoded %v", got.Redundancy, got.Chunk)
	}
	var stored int64
	for _, n := range c.Nodes() {
		stored += c.BytesOn(n.ID())
	}
	// 1.5× plus one header per shard, against 3× for replication.
	if want := int64(size)*3/2 + 18*int64(ec.HeaderSize); stored > want {
		t.Fatalf("%d bytes stored for %d, want at most %d", stored, size, want)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestECReadSurvivesTwoLostNodesNotThree(t *testing.T) {
	c := ecCluster(t, 2)
	m, _, err := c.Session().UploadAs("/ec", c.RandomData("/ec", 5<<20), client.EC42)
	if err != nil {
		t.Fatal(err)
	}
	// Two data shards of chunk 0 go: the read decodes from parity, at once
	// (the nodes time out) and once the detector has declared them dead.
	shards := m.Chunk[0].Shards
	for _, j := range []int{0, 1} {
		c.KillNode(iface.NodeID(shards[j].Replicas[0]))
	}
	for _, wait := range []time.Duration{0, c.Config().Meta.Detector.DeadAfter + 2*time.Second} {
		c.Tick(wait)
		_, got, err := c.Download("/ec")
		if err != nil {
			t.Fatalf("after %v with 2 shards lost: %v", wait, err)
		}
		if !got.Chunk[0].Decoded {
			t.Fatalf("after %v: chunk 0 read without decoding", wait)
		}
	}
	// A third: 3 of 6 left, fewer than 4. The read fails and says why.
	c.KillNode(iface.NodeID(shards[4].Replicas[0]))
	c.Tick(c.Config().Meta.Detector.DeadAfter + 2*time.Second)
	_, _, err = c.Download("/ec")
	if iface.CodeOf(err) != iface.CodeUnavailable || !strings.Contains(err.Error(), "3 of 6 shards readable") {
		t.Fatalf("read with 3 shards lost: %v", err)
	}
}

// A rotted data shard fails the client's hash check: the read decodes from
// the others and reports the node, which is asked to re-check its copy.
func TestECReadDecodesAroundARottedShard(t *testing.T) {
	c := ecCluster(t, 3)
	m, _, err := c.Session().UploadAs("/ec", c.RandomData("/ec", 1<<20), client.EC42)
	if err != nil {
		t.Fatal(err)
	}
	sh := m.Chunk[0].Shards[2]
	if err := c.CorruptReplica(iface.NodeID(sh.Replicas[0]), sh.ID); err != nil {
		t.Fatal(err)
	}
	_, got, err := c.Download("/ec")
	if err != nil {
		t.Fatal(err)
	}
	if r := got.Chunk[0]; !r.Decoded || !slices.Equal(r.Shards[2].Rejected, sh.Replicas) {
		t.Fatalf("decoded %v, shard 2 rejected by %v", r.Decoded, r.Shards[2].Rejected)
	}
}
