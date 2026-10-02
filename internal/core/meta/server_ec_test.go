package meta_test

import (
	"context"
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// beginEC opens an erasure-coded upload of size bytes.
func (e *env) beginEC(t *testing.T, path string, size int) (*chunkdv1.BeginUploadResponse, error) {
	t.Helper()
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: path, Size: int64(size), Redundancy: chunkdv1.Redundancy_REDUNDANCY_EC_4_2}))
	if err != nil {
		return nil, err
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	return &begin, nil
}

// putStripes claims every chunk of data as a stripe and writes its shards,
// shard i to placement replica i, except the shard indexes in skip. It
// returns the stripe IDs and how many chunks the claim found present.
func (e *env) putStripes(t *testing.T, begin *chunkdv1.BeginUploadResponse, data []byte, skip ...int) ([][]byte, int) {
	t.Helper()
	codec := ec.New()
	var ids [][]byte
	present := 0
	for i, pl := range begin.GetPlacement() {
		part := data[i*4 : min((i+1)*4, len(data))]
		shards, err := codec.Encode(part)
		if err != nil {
			t.Fatal(err)
		}
		logical := ec.LogicalID(sha256.Sum256(part))
		ids = append(ids, logical[:])
		cl := &chunkdv1.ChunkClaim{Index: int32(i), Id: logical[:]}
		for _, s := range shards {
			cl.Shards = append(cl.Shards, s.ID[:])
		}
		body, err := e.rpc(t, "meta", wire.KindClaim, wire.Marshal(&chunkdv1.ClaimChunksRequest{UploadId: begin.GetUploadId(), Claims: []*chunkdv1.ChunkClaim{cl}}))
		if err != nil {
			t.Fatal(err)
		}
		var claim chunkdv1.ClaimChunksResponse
		wire.Decode(body, &claim)
		if claim.GetPresent()[0] {
			present++
			continue
		}
		var calls []iface.Call
		for j, r := range pl.GetReplicas() {
			if !slices.Contains(skip, j) {
				calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: shards[j].ID[:], Data: shards[j].Block})})
			}
		}
		e.caller.Do(context.Background(), calls)
	}
	return ids, present
}

func (e *env) commitWithRetry(t *testing.T, upload uint64, ids [][]byte, data []byte) (uint64, error) {
	t.Helper()
	sum := sha256.Sum256(data)
	req := wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: upload, ChunkIds: ids, Sha256: sum[:]})
	var body []byte
	var err error
	for range 20 {
		if body, err = e.rpc(t, "meta", wire.KindCommit, req); iface.CodeOf(err) != iface.CodeRetry {
			break
		}
		e.clock.Advance(100 * time.Millisecond)
	}
	if err != nil {
		return 0, err
	}
	var resp chunkdv1.CommitUploadResponse
	wire.Decode(body, &resp)
	return resp.GetVersion(), nil
}

func TestECBeginNeedsSixNodes(t *testing.T) {
	e := newEnv(t, 5)
	if _, err := e.beginEC(t, "/f", 10); iface.CodeOf(err) != iface.CodeUnavailable {
		t.Fatalf("EC begin on 5 nodes: err = %v, want unavailable", err)
	}
	// Replicated uploads are unaffected.
	if v, err := e.upload(t, "/g", []byte("0123")); err != nil || v != 1 {
		t.Fatalf("replicated upload = v%d, %v", v, err)
	}
}

func TestECCommitNeedsFiveShards(t *testing.T) {
	e := newEnv(t, 6)
	data := []byte("0123456789")
	begin, err := e.beginEC(t, "/f", len(data))
	if err != nil {
		t.Fatal(err)
	}
	if begin.GetRedundancy() != chunkdv1.Redundancy_REDUNDANCY_EC_4_2 || begin.GetMinReplicas() != ec.CommitShards {
		t.Fatalf("begin = %v, min %d", begin.GetRedundancy(), begin.GetMinReplicas())
	}
	for i, pl := range begin.GetPlacement() {
		nodes := map[string]bool{}
		for _, r := range pl.GetReplicas() {
			nodes[r.GetNode()] = true
		}
		if len(nodes) != ec.TotalShards {
			t.Fatalf("chunk %d placed on %d distinct nodes", i, len(nodes))
		}
	}

	ids, _ := e.putStripes(t, begin, data, 1, 4) // 4 of 6 shards
	if _, err := e.commitWithRetry(t, begin.GetUploadId(), ids, data); iface.CodeOf(err) != iface.CodeRetry {
		t.Fatalf("commit with 4 shards: err = %v, want retry", err)
	}
	e.putStripes(t, begin, data, 4) // shard 1 arrives: 5 of 6
	if v, err := e.commitWithRetry(t, begin.GetUploadId(), ids, data); err != nil || v != 1 {
		t.Fatalf("commit with 5 shards = v%d, %v", v, err)
	}

	st, err := e.stat(t, "/f")
	if err != nil {
		t.Fatal(err)
	}
	if st.GetRedundancy() != chunkdv1.Redundancy_REDUNDANCY_EC_4_2 || len(st.GetChunks()) != 3 {
		t.Fatalf("stat: %v, %d chunks", st.GetRedundancy(), len(st.GetChunks()))
	}
	for i, c := range st.GetChunks() {
		if len(c.GetReplicas()) != 0 || len(c.GetShards()) != ec.TotalShards {
			t.Fatalf("chunk %d: %d replicas, %d shards", i, len(c.GetReplicas()), len(c.GetShards()))
		}
		for j, sh := range c.GetShards() {
			want := 1
			if j == 4 {
				want = 0 // never written; repair rebuilds it (ADR-0023)
			}
			if len(sh.GetReplicas()) != want || (want == 1 && sh.GetReplicas()[0].GetNode() != begin.GetPlacement()[i].GetReplicas()[j].GetNode()) {
				t.Fatalf("chunk %d shard %d: replicas %v", i, j, sh.GetReplicas())
			}
		}
	}
}

func TestECDedupAgainstAStripe(t *testing.T) {
	e := newEnv(t, 6)
	data := []byte("abcdefgh")
	for i, path := range []string{"/a", "/b"} {
		begin, err := e.beginEC(t, path, len(data))
		if err != nil {
			t.Fatal(err)
		}
		ids, present := e.putStripes(t, begin, data)
		if want := i * 2; present != want {
			t.Fatalf("%s: %d chunks present, want %d", path, present, want)
		}
		if _, err := e.commitWithRetry(t, begin.GetUploadId(), ids, data); err != nil {
			t.Fatal(err)
		}
	}
	// The same bytes uploaded replicated are stored apart (ADR-0022).
	if _, skipped, err := e.uploadCounting(t, "/r", data); err != nil || skipped != 0 {
		t.Fatalf("replicated upload of EC content: skipped %d, %v", skipped, err)
	}
}
