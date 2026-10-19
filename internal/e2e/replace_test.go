package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/local"
	"github.com/insanityatpeak/chunkd/internal/real/metastore"
)

// TestReplaceMetaPeerReal: over real gRPC and disk, a follower's WAL is
// corrupted before its tail, so it refuses to start (metastore.ErrCorrupt).
// The operator's replace swaps its Raft ID, its directory is emptied and it
// starts with -join: it records its new ID, catches up, and the group then
// survives losing another peer (ADR-0031).
func TestReplaceMetaPeerReal(t *testing.T) {
	if testing.Short() {
		t.Skip("starts three metadata servers and restarts one")
	}
	ctx := context.Background()
	cfg := meta.DefaultConfig("")
	cfg.ChunkSize = 256 << 10
	cfg.TickEvery, cfg.ElectionTimeout = 50*time.Millisecond, 600*time.Millisecond
	c, err := local.StartGroup(t.TempDir(), 3, 3, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	put := func(path string) {
		t.Helper()
		data := bytes.Repeat([]byte(path), 64<<10)
		if _, err := c.Client.Put(ctx, path, bytes.NewReader(data), int64(len(data)), client.PutOptions{Overwrite: true}); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		var got bytes.Buffer
		if _, err := c.Client.Get(ctx, path, &got); err != nil || sha256.Sum256(got.Bytes()) != sha256.Sum256(data) {
			t.Fatalf("get %s: %v", path, err)
		}
	}
	leader := waitLeader(t, c)
	for i := range 5 {
		put(fmt.Sprintf("/a%d", i))
	}
	var victim, other *local.MetaProc
	for _, m := range c.Metas {
		switch {
		case m == leader:
		case victim == nil:
			victim = m
		default:
			other = m
		}
	}

	c.KillMeta(victim)
	corruptWAL(t, filepath.Join(victim.Dir, "meta.wal"))
	if err := c.RestartMeta(victim, false); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("restart over a corrupt WAL: %v, want ErrCorrupt", err)
	}
	put("/while-down")
	time.Sleep(2 * cfg.ElectionTimeout) // the leader stops hearing from it
	// Generation 0's Raft ID is the slot: meta-1..3 are 1..3.
	res, err := c.Client.ReplacePeer(ctx, string(victim.ID), uint64(slotOf(victim)), false)
	if err != nil || res.Added != res.Removed+3 {
		t.Fatalf("replace: %+v, %v", res, err)
	}
	if err := os.RemoveAll(victim.Dir); err != nil {
		t.Fatal(err)
	}
	if err := c.RestartMeta(victim, true); err != nil {
		t.Fatalf("join: %v", err)
	}
	waitCaughtUp(t, c, victim)

	c.KillMeta(other)
	for i := range 3 {
		put(fmt.Sprintf("/b%d", i))
	}
	waitCaughtUp(t, c, victim)
}

func slotOf(m *local.MetaProc) int {
	var n int
	fmt.Sscanf(string(m.ID), "meta-%d", &n)
	return n
}

func waitLeader(t *testing.T, c *local.Cluster) *local.MetaProc {
	t.Helper()
	for range 100 {
		if l := c.Leader(); l != nil {
			return l
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

// waitCaughtUp waits until m has applied everything the leader has and holds
// the same state.
func waitCaughtUp(t *testing.T, c *local.Cluster, m *local.MetaProc) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		l := c.Leader()
		if l != nil && l != m {
			var la, ma iface.Index
			var ls, mss []byte
			l.Loop.Do(func() { la, ls = l.Srv.Applied(), l.Srv.State().Snapshot() })
			m.Loop.Do(func() { ma, mss = m.Srv.Applied(), m.Srv.State().Snapshot() })
			if la == ma && bytes.Equal(ls, mss) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s applied %d, leader %s %d", m.ID, ma, l.ID, la)
			}
		} else if time.Now().After(deadline) {
			t.Fatal("no leader other than the joined peer")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// corruptWAL flips a byte inside the WAL's first record, well before its tail.
func corruptWAL(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 512 {
		t.Fatalf("WAL only %d bytes", len(b))
	}
	b[40] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
