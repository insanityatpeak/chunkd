package cluster

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/history/check"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// newMetaGroup is a cluster with three metadata peers and the small chunks
// that keep an upload a few dozen simulated milliseconds long, past the first
// election.
func newMetaGroup(t *testing.T, seed uint64) *Cluster {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Metas = 3
	cfg.Meta.ChunkSize = 256 << 10
	c := New(seed, cfg, io.Discard)
	c.Tick(6 * time.Second)
	if c.MetaLeader() == "" {
		t.Fatalf("seed %d: no metadata leader after 6 s", seed)
	}
	return c
}

func mustAgree(t *testing.T, c *Cluster) {
	t.Helper()
	c.Tick(10 * time.Second)
	if err := c.AssertMetaAgree(); err != nil {
		t.Fatal(err)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestMetaGroupRoundTripAndAgreement(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		c := newMetaGroup(t, seed)
		for i := range 5 {
			if _, _, err := c.UploadRandom(fmt.Sprintf("/f%d", i), int64(1+i)<<20); err != nil {
				t.Fatalf("seed %d: upload %d: %v", seed, i, err)
			}
		}
		if err := c.Delete("/f0"); err != nil {
			t.Fatal(err)
		}
		mustAgree(t, c)
	}
}

// TestKillLeaderMidUpload: the leader dies at a different moment of an
// upload for each seed (begin, claims, chunk writes, commit). The client
// finds the new leader and retries; the upload completes, every acknowledged
// file reads back with its hash, and the surviving peers hold the same state.
func TestKillLeaderMidUpload(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			c := newMetaGroup(t, seed)
			if _, _, err := c.UploadRandom("/before", 1<<20); err != nil {
				t.Fatal(err)
			}
			old := c.MetaLeader()
			c.AfterFunc(time.Duration(5+30*seed)*time.Millisecond, func() { c.KillMeta(old) })
			if _, _, err := c.UploadRandom("/during", 2<<20); err != nil {
				t.Fatalf("upload with the leader killed: %v", err)
			}
			c.Tick(3 * time.Second)
			if nl := c.MetaLeader(); nl == "" || nl == old {
				t.Fatalf("leader %q after killing %s", nl, old)
			}
			if _, _, err := c.UploadRandom("/after", 1<<20); err != nil {
				t.Fatalf("upload under the new leader: %v", err)
			}
			mustAgree(t, c)
		})
	}
}

// Cut off from both other peers, the old leader reports itself deposed and
// refuses requests; a write sent to it before it noticed never commits; and
// after the partition heals it follows the new leader with nothing extra.
func TestMinorityLeaderRejectsAndStaleWriteNeverCommits(t *testing.T) {
	c := newMetaGroup(t, 3)
	if _, _, err := c.UploadRandom("/before", 1<<20); err != nil {
		t.Fatal(err)
	}
	old := c.MetaLeader()
	var rest []iface.NodeID
	for _, id := range c.MetaIDs() {
		if id != old {
			rest = append(rest, id)
		}
	}
	c.Net().Partition([]iface.NodeID{old}, rest)

	// Immediately: the old leader has not yet noticed, so it takes the
	// proposal into its log, and nothing can commit it.
	probe := c.NewCaller("probe")
	begin := wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/stale", Size: 4})
	r := probe.Do(context.Background(), []iface.Call{{To: old, Kind: wire.KindBegin, Body: begin}})
	if r[0].Err == nil {
		t.Fatal("the cut-off leader acknowledged a write")
	}
	// Later: it knows, and says so.
	c.Tick(3 * time.Second)
	r = probe.Do(context.Background(), []iface.Call{{To: old, Kind: wire.KindBegin, Body: begin}})
	if iface.CodeOf(r[0].Err) != iface.CodeNotLeader {
		t.Fatalf("the deposed leader answered %v, want NotLeader", r[0].Err)
	}
	if nl := c.MetaLeader(); nl == "" || nl == old {
		t.Fatalf("no new leader on the majority side: %q", nl)
	}
	if _, _, err := c.UploadRandom("/majority", 1<<20); err != nil {
		t.Fatalf("the majority side cannot write: %v", err)
	}

	c.Net().Heal()
	mustAgree(t, c)
	for _, id := range c.MetaIDs() {
		st := c.MetaPeer(id).State()
		if n := st.PendingUploads(); n != 0 {
			t.Fatalf("%s holds %d pending uploads: the stale write took effect", id, n)
		}
		if _, err := st.Stat("/stale"); err == nil {
			t.Fatalf("%s has the stale file", id)
		}
	}
}

// A restarted follower catches up; so does a whole group restarted at once,
// from its snapshots and logs.
func TestMetaGroupRestarts(t *testing.T) {
	c := newMetaGroup(t, 5)
	for i := range 3 {
		if _, _, err := c.UploadRandom(fmt.Sprintf("/a%d", i), 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	var follower iface.NodeID
	for _, id := range c.MetaIDs() {
		if id != c.MetaLeader() {
			follower = id
		}
	}
	c.KillMeta(follower)
	for i := range 3 {
		if _, _, err := c.UploadRandom(fmt.Sprintf("/b%d", i), 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.ReviveMeta(follower); err != nil {
		t.Fatal(err)
	}
	mustAgree(t, c)

	for _, id := range c.MetaIDs() {
		c.KillMeta(id)
	}
	c.Tick(2 * time.Second)
	for _, id := range c.MetaIDs() {
		if err := c.ReviveMeta(id); err != nil {
			t.Fatal(err)
		}
	}
	c.Tick(8 * time.Second)
	if _, _, err := c.UploadRandom("/c", 1<<20); err != nil {
		t.Fatalf("upload after a full restart: %v", err)
	}
	mustAgree(t, c)
}

// A client pinned to a deposed leader never sees stale data: its reads are
// refused (NotLeader or Unavailable) rather than served from the old log.
// The recorded history passes the checker. The same history with one read
// served locally by the deposed leader, as a leader without a read-index
// check would serve it, fails: that is the mutation the checker must catch.
func TestPinnedClientOnDeposedLeaderIsLinearizable(t *testing.T) {
	c := newMetaGroup(t, 7)
	rec := c.RecordHistory()
	old := c.MetaLeader()
	var rest []iface.NodeID
	for _, id := range c.MetaIDs() {
		if id != old {
			rest = append(rest, id)
		}
	}
	pin := c.Pinned(old)
	if _, _, err := pin.Upload("/x", c.RandomData("/x", 1<<20)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pin.Download("/x"); err != nil {
		t.Fatal(err)
	}

	c.Net().Partition([]iface.NodeID{old}, rest)
	// Before the old leader notices: a write it can never commit.
	if _, _, err := pin.Upload("/x", c.RandomData("/x-lost", 1<<20)); err == nil {
		t.Fatal("the cut-off leader acknowledged a write")
	}
	c.Tick(3 * time.Second)
	if _, _, err := c.Upload("/x", c.RandomData("/x-new", 1<<20)); err != nil {
		t.Fatalf("majority write: %v", err)
	}
	if data, _, err := pin.Download("/x"); err == nil {
		t.Fatalf("the deposed leader served a read of %d bytes", len(data))
	}
	c.Net().Heal()
	c.Tick(10 * time.Second)
	if _, _, err := pin.Download("/x"); err == nil {
		t.Fatal("a follower served the pinned client's read")
	}
	if _, _, err := c.Download("/x"); err != nil {
		t.Fatal(err)
	}

	ops := rec.Ops()
	if res := check.Check(ops); !res.OK {
		t.Fatal(res)
	}

	// Mutation: the old leader answers from its own state, which stopped at
	// the first version.
	st, err := c.MetaPeer(old).State().StatVersion("/x", 1)
	if err != nil {
		t.Fatal(err)
	}
	now := int64(c.Now())
	rec.Read(pin.id, "/x", now, now+1, st.V, st.SHA256, nil)
	if res := check.Check(rec.Ops()); res.OK {
		t.Fatal("the checker accepted a read served from a deposed leader's stale state")
	}
}
