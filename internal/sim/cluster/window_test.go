package cluster

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// With the same latency on every link, a 16-chunk put with Window 4 makes a
// quarter of the claim round trips and overlaps the chunks' puts (ADR-0034).
// The bound only checks that the window helps; the ratio is logged.
func TestWindowOverlapsChunks(t *testing.T) {
	const chunks, size = 16, 64 << 10
	for _, policy := range []client.Redundancy{client.Replicated, client.EC42} {
		name := "replicated"
		if policy != client.Replicated {
			name = string(policy)
		}
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Nodes = ec.TotalShards + 1
			cfg.Faults.DropRate = 0
			cfg.Meta.ChunkSize = size
			c := New(1, cfg, io.Discard)
			c.Tick(3 * time.Second)
			caller := c.NewCaller("writer")
			put := func(window int, p string) time.Duration {
				cl := client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep, Window: window})
				data := c.RandomData(p, chunks*size)
				t0 := c.Now()
				m, err := cl.Put(context.Background(), p, bytes.NewReader(data), int64(len(data)), client.PutOptions{Redundancy: policy})
				d := c.Now().Sub(t0)
				if err != nil || m.Chunks != chunks {
					t.Fatalf("put %s with window %d: %d chunks, %v", p, window, m.Chunks, err)
				}
				for i, ref := range m.Chunk {
					if ref.Index != i {
						t.Fatalf("window %d: manifest entry %d is chunk %d", window, i, ref.Index)
					}
				}
				if got, _, err := c.Download(p); err != nil || !bytes.Equal(got, data) {
					t.Fatalf("read back %s: %d bytes, %v", p, len(got), err)
				}
				return d
			}
			serial, windowed := put(1, "/w1"), put(4, "/w4")
			t.Logf("16 chunks: window 1 %v, window 4 %v (%.2fx)", serial, windowed, float64(serial)/float64(windowed))
			if 2*windowed > serial {
				t.Fatalf("window 4 took %v, window 1 %v: the window does not overlap chunks", windowed, serial)
			}
			if _, ok := c.Settle(time.Minute); !ok {
				t.Fatal("did not settle")
			}
			if err := c.AssertInvariants(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A chunk short of its minimum fails the whole wave: the put returns the
// error, aborts, and nothing becomes visible.
func TestWindowFailedChunkAbortsUpload(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Faults.DropRate = 0
	cfg.Meta.ChunkSize = 64 << 10
	c := New(2, cfg, io.Discard)
	c.Tick(3 * time.Second)
	// Before the detector notices, placement still names the dead nodes, so
	// some chunk of the file has two of its three replicas on them.
	nodes := c.Nodes()
	for _, n := range nodes[:3] {
		c.KillNode(n.ID())
	}
	caller := c.NewCaller("writer")
	cl := client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep, Window: 4})
	data := c.RandomData("/f", 16*64<<10)
	_, err := cl.Put(context.Background(), "/f", bytes.NewReader(data), int64(len(data)), client.PutOptions{})
	if err == nil || !strings.Contains(err.Error(), "required replicas stored") {
		t.Fatalf("put with 3 of 5 nodes dead: %v, want a short chunk", err)
	}
	if _, err := c.Client().Stat(context.Background(), "/f"); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("stat after the failed put: %v, want not found", err)
	}
}

// A chunk repeated within one wave is put once and the repeats are deduped:
// the wave's single claim cannot find the first copy present yet.
func TestWindowDedupsRepeatsInWave(t *testing.T) {
	const size = 64 << 10
	for _, policy := range []client.Redundancy{client.Replicated, client.EC42} {
		t.Run(string(policy), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Nodes = ec.TotalShards + 1
			cfg.Faults.DropRate = 0
			cfg.Meta.ChunkSize = size
			c := New(3, cfg, io.Discard)
			c.Tick(3 * time.Second)
			caller := c.NewCaller("writer")
			cl := client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep, Window: 4})
			data := bytes.Repeat(c.RandomData("/r", size), 8)
			m, err := cl.Put(context.Background(), "/r", bytes.NewReader(data), int64(len(data)), client.PutOptions{Redundancy: policy})
			if err != nil {
				t.Fatal(err)
			}
			if m.Chunk[0].Deduped {
				t.Fatal("chunk 0 deduped: nothing held it before the put")
			}
			for _, ref := range m.Chunk[1:4] {
				if !ref.Deduped || ref.ID != m.Chunk[0].ID {
					t.Fatalf("chunk %d: deduped %v, id %s; want a dedup of chunk 0 (%s)", ref.Index, ref.Deduped, ref.ID, m.Chunk[0].ID)
				}
			}
			if got, _, err := c.Download("/r"); err != nil || !bytes.Equal(got, data) {
				t.Fatalf("read back: %d bytes, %v", len(got), err)
			}
			if _, ok := c.Settle(time.Minute); !ok {
				t.Fatal("did not settle")
			}
			if err := c.AssertInvariants(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
