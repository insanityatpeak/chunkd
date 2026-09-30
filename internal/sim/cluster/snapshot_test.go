package cluster

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// TestLogBoundedUnder10kOps: 10,000 client operations (puts of small files
// over 200 paths, reads, deletes) against the default snapshot interval. No
// peer's durable log ever holds much more than one interval of entries, and a
// follower down for most of the run comes back to find its missing entries
// compacted away, so it catches up from the leader's snapshot.
func TestLogBoundedUnder10kOps(t *testing.T) {
	if testing.Short() {
		t.Skip("10,000 operations")
	}
	cfg := DefaultConfig()
	cfg.Metas = 3
	cfg.Meta.ChunkSize = 64 << 10
	c := New(11, cfg, io.Discard)
	c.Tick(6 * time.Second)
	if c.MetaLeader() == "" {
		t.Fatal("no metadata leader after 6 s")
	}
	var lag iface.NodeID
	for _, id := range c.MetaIDs() {
		if id != c.MetaLeader() {
			lag = id
		}
	}
	held := func(id iface.NodeID) int {
		n := 0
		_ = c.metaPeer(id).store.Replay(context.Background(), 0, func(iface.Index, []byte) error { n++; return nil })
		return n
	}
	limit := 2 * cfg.Meta.SnapshotEvery
	peak := 0
	const ops = 10_000
	failed := 0
	for i := range ops {
		switch i {
		case 500:
			c.KillMeta(lag)
		case 8000:
			if err := c.ReviveMeta(lag); err != nil {
				t.Fatal(err)
			}
		}
		path := fmt.Sprintf("/s/%03d", i%200)
		var err error
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5:
			_, _, err = c.UploadRandom(path, 1+int64(i%4096))
		case 6, 7, 8:
			_, _, err = c.Download(path)
		default:
			err = c.Delete(path)
		}
		if err != nil && iface.CodeOf(err) != iface.CodeNotFound {
			failed++
		}
		if i%250 == 249 {
			for _, id := range c.MetaIDs() {
				if c.net.Crashed(id) {
					continue
				}
				n := held(id)
				peak = max(peak, n)
				if n > limit {
					t.Fatalf("op %d: %s holds %d log entries, bound %d (snapshot every %d)", i, id, n, limit, cfg.Meta.SnapshotEvery)
				}
			}
		}
	}
	leader := c.MetaLeader()
	applied := c.MetaPeer(leader).Raft().Applied
	if applied < uint64(5*cfg.Meta.SnapshotEvery) {
		t.Fatalf("%d ops applied only %d entries: too few to compact several times", ops, applied)
	}
	if failed > ops/100 {
		t.Fatalf("%d of %d operations failed", failed, ops)
	}
	mustAgree(t, c)
	if n := c.MetaPeer(lag).Raft().SnapshotsInstalled; n == 0 {
		t.Fatalf("%s caught up without a snapshot: the entries it missed were never compacted", lag)
	}
	if a, b := c.MetaPeer(lag).Raft().Applied, c.MetaPeer(leader).Raft().Applied; a != b {
		t.Fatalf("%s applied %d, leader %d", lag, a, b)
	}
	t.Logf("%d ops, %d entries applied, peak durable log %d entries (bound %d), %d failed", ops, applied, peak, limit, failed)
}
