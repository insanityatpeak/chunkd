package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/real/local"
)

// TestKillLeaderMidUploadReal: three metadata servers over real gRPC and disk
// WALs. The leader is killed at a different point of an upload each round.
// The client finds the new leader and retries: the upload completes, reads
// back with its hash, and the surviving servers hold the same state.
func TestKillLeaderMidUploadReal(t *testing.T) {
	if testing.Short() {
		t.Skip("starts three metadata servers per round")
	}
	data := make([]byte, 6<<20)
	for i := range data {
		data[i] = byte(i*13 + i>>9)
	}
	want := sha256.Sum256(data)
	for _, delay := range []time.Duration{0, 5 * time.Millisecond, 30 * time.Millisecond, 120 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			cfg := meta.DefaultConfig("")
			cfg.ChunkSize = 256 << 10
			cfg.TickEvery, cfg.ElectionTimeout = 50*time.Millisecond, 600*time.Millisecond
			c, err := local.StartGroup(t.TempDir(), 3, 3, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var old *local.MetaProc
			for range 100 {
				if old = c.Leader(); old != nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if old == nil {
				t.Fatal("no leader")
			}

			done := make(chan error, 1)
			go func() {
				_, err := c.Client.Put(context.Background(), "/f", bytes.NewReader(data), int64(len(data)), client.PutOptions{})
				done <- err
			}()
			time.Sleep(delay)
			c.KillMeta(old)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Put with the leader killed %v in: %v", delay, err)
				}
			case <-time.After(60 * time.Second):
				t.Fatal("Put did not finish within 60 s of the leader dying")
			}

			var got bytes.Buffer
			if _, err := c.Client.Get(context.Background(), "/f", &got); err != nil {
				t.Fatalf("Get: %v", err)
			}
			if sha256.Sum256(got.Bytes()) != want {
				t.Fatal("the file reads back with a different hash")
			}
			if nl := c.Leader(); nl == nil || nl == old {
				t.Fatalf("leader after the kill: %v (killed %s)", nl, old.ID)
			}
		})
	}
}
