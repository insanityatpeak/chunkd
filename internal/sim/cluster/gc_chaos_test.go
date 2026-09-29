package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// GC chaos scenarios. Each drives the upload protocol by hand, so it can
// stall between writing chunks and committing, over the default lossy sim
// network, then checks the usual invariants and, after GCSettle, that no
// orphan survives and no counts drifted.

const gcChunk = 64 << 10

// putCreate fails if the path exists.
var putCreate = client.PutOptions{}

func gcCluster(seed uint64) *Cluster {
	cfg := DefaultConfig()
	cfg.Meta.ChunkSize = gcChunk
	c := New(seed, cfg, io.Discard)
	c.Tick(3 * time.Second)
	return c
}

// rpc calls the metadata server, retrying while the sim network drops the
// request or the response (every metadata RPC is safe to repeat).
func rpc(t *testing.T, c *Cluster, caller *sim.Caller, kind string, req []byte) ([]byte, error) {
	t.Helper()
	for range 20 {
		r := caller.Do(context.Background(), []iface.Call{{To: MetaID, Kind: kind, Body: req}})
		if code := iface.CodeOf(r[0].Err); code != iface.CodeUnavailable && code != iface.CodeRetry {
			return r[0].Body, r[0].Err
		}
		c.Tick(200 * time.Millisecond)
	}
	t.Fatalf("%s: metadata server unreachable", kind)
	return nil, nil
}

type rawUpload struct {
	id   uint64
	ids  [][]byte
	sum  [32]byte
	data []byte
}

// stage begins an upload of data at path and claims and writes every chunk
// the cluster lacks, without committing.
func stage(t *testing.T, c *Cluster, caller *sim.Caller, path string, data []byte, expected uint64) (rawUpload, error) {
	t.Helper()
	body, err := rpc(t, c, caller, wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: path, Size: int64(len(data)), ExpectedVersion: expected}))
	if err != nil {
		return rawUpload{}, err
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	u := rawUpload{id: begin.GetUploadId(), sum: sha256.Sum256(data), data: data}
	for i, pl := range begin.GetPlacement() {
		part := data[i*gcChunk : min((i+1)*gcChunk, len(data))]
		id := sha256.Sum256(part)
		u.ids = append(u.ids, id[:])
		body, err := rpc(t, c, caller, wire.KindClaim, wire.Marshal(&chunkdv1.ClaimChunksRequest{UploadId: u.id, Claims: []*chunkdv1.ChunkClaim{{Index: int32(i), Id: id[:]}}}))
		if err != nil {
			return rawUpload{}, err
		}
		var claim chunkdv1.ClaimChunksResponse
		wire.Decode(body, &claim)
		if claim.GetPresent()[0] {
			continue
		}
		var calls []iface.Call
		for _, r := range pl.GetReplicas() {
			calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: part})})
		}
		// Twice: puts are idempotent, and one dropped put should not cost
		// the upload a replica.
		caller.Do(context.Background(), calls)
		caller.Do(context.Background(), calls)
	}
	return u, nil
}

// commit retries while block reports are in flight.
func commit(t *testing.T, c *Cluster, caller *sim.Caller, u rawUpload) (uint64, error) {
	t.Helper()
	req := wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: u.id, ChunkIds: u.ids, Sha256: u.sum[:]})
	for range 100 {
		r := caller.Do(context.Background(), []iface.Call{{To: MetaID, Kind: wire.KindCommit, Body: req}})
		if code := iface.CodeOf(r[0].Err); code != iface.CodeUnavailable && code != iface.CodeRetry {
			var resp chunkdv1.CommitUploadResponse
			wire.Decode(r[0].Body, &resp)
			return resp.GetVersion(), r[0].Err
		}
		c.Tick(300 * time.Millisecond)
	}
	return 0, fmt.Errorf("commit of upload %d never succeeded", u.id)
}

func payload(seed uint64, tag string, size int) []byte {
	b := make([]byte, size)
	r := sim.NewRand(seed ^ uint64(len(tag))*0x9e3779b97f4a7c15)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	copy(b, tag)
	return b
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func readBack(t *testing.T, c *Cluster, path string, want []byte) {
	t.Helper()
	got, _, err := c.Download(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("seed %d: %s reads back %d bytes, %v; want the committed %d", c.Seed(), path, len(got), err, len(want))
	}
}

func settleGC(t *testing.T, c *Cluster) {
	t.Helper()
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
	c.Tick(c.GCSettle())
	if err := c.AssertCollected(); err != nil {
		t.Fatal(err)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

const gcSeeds = 25

// TestChaosConcurrentWriters: two writers race on one path, round after
// round, both expecting the same live version. Exactly one commit wins each
// round; the loser's chunks become garbage and are collected.
func TestChaosConcurrentWriters(t *testing.T) {
	for seed := uint64(1); seed <= gcSeeds; seed++ {
		c := gcCluster(seed)
		a, b := c.NewCaller("writer-a"), c.NewCaller("writer-b")
		var live uint64
		var want []byte
		for round := range 4 {
			da := payload(seed, fmt.Sprintf("a%d", round), 3*gcChunk)
			db := payload(seed, fmt.Sprintf("b%d", round), 2*gcChunk+100)
			ua, erra := stage(t, c, a, "/hot", da, live)
			ub, errb := stage(t, c, b, "/hot", db, live)
			if erra != nil || errb != nil {
				t.Fatalf("seed %d round %d: begin: %v, %v", seed, round, erra, errb)
			}
			va, erra := commit(t, c, a, ua)
			vb, errb := commit(t, c, b, ub)
			if (erra == nil) == (errb == nil) {
				t.Fatalf("seed %d round %d: commits returned %v and %v; exactly one must win", seed, round, erra, errb)
			}
			if erra == nil {
				live, want = va, da
				if iface.CodeOf(errb) != iface.CodeConflict {
					t.Fatalf("seed %d: loser got %v, want conflict", seed, errb)
				}
			} else {
				live, want = vb, db
				if iface.CodeOf(erra) != iface.CodeConflict {
					t.Fatalf("seed %d: loser got %v, want conflict", seed, erra)
				}
			}
			readBack(t, c, "/hot", want)
		}
		settleGC(t, c)
		readBack(t, c, "/hot", want)
	}
}

// TestChaosDeleteWhileUploadingSameChunk: /b's upload claims chunks /a
// already holds, so dedup skips writing them. Then /a is deleted and its
// versions drop past retention while /b is still uncommitted. /b's claims
// keep the shared chunks marked: its commit succeeds and reads back.
func TestChaosDeleteWhileUploadingSameChunk(t *testing.T) {
	for seed := uint64(1); seed <= gcSeeds; seed++ {
		c := gcCluster(seed)
		data := payload(seed, "shared", 4*gcChunk)
		if _, err := c.Client().Put(context.Background(), "/a", bytes.NewReader(data), int64(len(data)), putCreate); err != nil {
			t.Fatal(err)
		}
		c.Tick(2 * time.Second)
		w := c.NewCaller("writer")
		u, err := stage(t, c, w, "/b", data, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Delete("/a"); err != nil {
			t.Fatal(err)
		}
		// Past retention (at most 90 s) and one sweep, inside the lease.
		c.Tick(100 * time.Second)
		if log, _ := c.Meta().State().Log("/a"); len(log) != 1 || !log[0].Tombstone {
			t.Fatalf("seed %d: /a still retains %d versions; the test needs its data version dropped", seed, len(log))
		}
		if _, err := commit(t, c, w, u); err != nil {
			t.Fatalf("seed %d: commit after the shared chunks lost their other reference: %v", seed, err)
		}
		readBack(t, c, "/b", data)
		settleGC(t, c)
		readBack(t, c, "/b", data)
	}
}

// TestChaosGCDuringSlowUpload: chunks written, then a stall longer than the
// grace period and two sweeps before commit.
func TestChaosGCDuringSlowUpload(t *testing.T) {
	for seed := uint64(1); seed <= gcSeeds; seed++ {
		c := gcCluster(seed)
		w := c.NewCaller("slow")
		data := payload(seed, "slow", 5*gcChunk+7)
		u, err := stage(t, c, w, "/slow", data, 0)
		if err != nil {
			t.Fatal(err)
		}
		mc := c.Config().Meta
		stall := mc.GCGrace + 2*mc.EpochEvery
		if lease := time.Duration(mc.LeaseEpochs-1) * mc.EpochEvery; stall >= lease {
			t.Fatalf("config: a %v stall can outlive the %v minimum lease", stall, lease)
		}
		c.Tick(stall)
		if _, err := commit(t, c, w, u); err != nil {
			t.Fatalf("seed %d: commit after a %v stall: %v", seed, stall, err)
		}
		readBack(t, c, "/slow", data)
		if gc := c.Meta().GC(); gc.Sent != 0 {
			t.Fatalf("seed %d: GC sent %d deletes with only live and in-flight data", seed, gc.Sent)
		}
		settleGC(t, c)
	}
}

// TestChaosNodeReturnsWithDeletedChunks: a node is down while a file is
// deleted and collected everywhere else. It comes back with its old copies,
// which are orphans by then. The sweep's deletes to it are lost, the same
// content is uploaded again (re-writing some chunks on that node), and the
// next sweep re-sends the lost deletes with their original fences: the
// re-written copies must survive them.
func TestChaosNodeReturnsWithDeletedChunks(t *testing.T) {
	var kept uint64
	for seed := uint64(1); seed <= gcSeeds; seed++ {
		c := gcCluster(seed)
		data := payload(seed, "old", 4*gcChunk)
		m, err := c.Client().Put(context.Background(), "/a", bytes.NewReader(data), int64(len(data)), putCreate)
		if err != nil {
			t.Fatal(err)
		}
		c.Tick(2 * time.Second)
		down := iface.NodeID(m.Chunk[0].Replicas[0])
		c.KillNode(down)
		if err := c.Delete("/a"); err != nil {
			t.Fatal(err)
		}
		c.Tick(c.GCSettle())
		c.RestartNode(down)
		c.Tick(5 * time.Second) // its full report arrives
		// Lose the sweep's deletes to the returning node.
		sent := c.Meta().GC().Sent
		c.Net().Block(MetaID, down)
		for c.Meta().GC().Sent == sent {
			c.Tick(time.Second)
		}
		c.Net().Unblock(MetaID, down)
		again, err := c.Client().Put(context.Background(), "/again", bytes.NewReader(data), int64(len(data)), putCreate)
		if err != nil {
			t.Fatalf("seed %d: re-upload: %v", seed, err)
		}
		readBack(t, c, "/again", data)
		settleGC(t, c)
		readBack(t, c, "/again", data)
		// No fault removed anything since: every copy a node acknowledged
		// writing for /again must still be on its disk, unless repair
		// trimmed it (the kept copy on the returning node makes a fourth).
		// A GC delete that removed one ignored the fence.
		trimmed := map[string]bool{}
		evs, _ := c.Meta().Events(0)
		for _, e := range evs {
			if e.Kind == "trim" && strings.Contains(e.Text, " done: chunk ") {
				trimmed[string(e.Node)+" "+e.Text[strings.Index(e.Text, "chunk ")+6:][:12]] = true
			}
		}
		stored := c.Stored()
		for _, ch := range again.Chunk {
			var id iface.ChunkID
			copy(id[:], mustHex(t, ch.ID))
			for _, n := range ch.Replicas {
				if !slices.Contains(stored[id], iface.NodeID(n)) && !trimmed[n+" "+ch.ID[:12]] {
					t.Fatalf("seed %d: %s acknowledged chunk %d of /again and no longer holds it", seed, n, ch.Index)
				}
			}
		}
		id := sha256.Sum256(data[:chunk.SizeOf(int64(len(data)), gcChunk, 0)])
		if n := len(c.Meta().Cluster().Locations(id)); n != c.Config().Meta.Replicas {
			t.Fatalf("seed %d: re-uploaded chunk has %d copies after GC, want %d", seed, n, c.Config().Meta.Replicas)
		}
		kept += c.Meta().GC().Kept
	}
	// The race this scenario exists for: a GC delete aimed at an old copy
	// arrives after the re-upload wrote the chunk again, and is refused.
	if kept == 0 {
		t.Fatal("no seed refused a GC delete: the fence was never exercised")
	}
	t.Logf("%d seeds: %d GC deletes refused by the fence", gcSeeds, kept)
}
