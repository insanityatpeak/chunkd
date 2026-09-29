package meta_test

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// staged is an upload whose chunks are claimed and written but not committed.
type staged struct {
	id  uint64
	ids [][]byte
	sum [32]byte
}

func (e *env) stage(t *testing.T, path string, data []byte) staged {
	t.Helper()
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: path, Size: int64(len(data))}))
	if err != nil {
		t.Fatal(err)
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	st := staged{id: begin.GetUploadId(), sum: sha256.Sum256(data)}
	for i, pl := range begin.GetPlacement() {
		part := data[i*4 : min((i+1)*4, len(data))]
		id := sha256.Sum256(part)
		st.ids = append(st.ids, id[:])
		e.claim(t, st.id, i, id[:])
		var calls []iface.Call
		for _, r := range pl.GetReplicas() {
			calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: part})})
		}
		e.caller.Do(context.Background(), calls)
	}
	return st
}

func (e *env) commitStaged(t *testing.T, st staged) (uint64, error) {
	t.Helper()
	body, err := e.rpc(t, "meta", wire.KindCommit, wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: st.id, ChunkIds: st.ids, Sha256: st.sum[:]}))
	var resp chunkdv1.CommitUploadResponse
	wire.Decode(body, &resp)
	return resp.GetVersion(), err
}

func (e *env) copies(raw []byte) int {
	id, _ := wire.ChunkID(raw)
	return len(e.srv.Cluster().Locations(id))
}

func TestGCCollectsAbortedUpload(t *testing.T) {
	e := newEnv(t, 3)
	st := e.stage(t, "/f", []byte("abcdefgh"))
	e.clock.Advance(time.Second)
	if _, err := e.rpc(t, "meta", wire.KindAbort, wire.Marshal(&chunkdv1.AbortUploadRequest{UploadId: st.id})); err != nil {
		t.Fatal(err)
	}
	// First sweep sees the orphans, the one after the grace deletes them.
	e.clock.Advance(2*time.Minute + 5*time.Second)
	for i, raw := range st.ids {
		if n := e.copies(raw); n != 0 {
			t.Fatalf("chunk %d: %d copies left after GC", i, n)
		}
	}
	if gc := e.srv.GC(); gc.Deleted != 6 || gc.Kept != 0 {
		t.Fatalf("gc stats %+v, want 6 deleted", gc)
	}
}

// TestGCSparesInflightUpload: an upload stalled between writing its chunks
// and committing, for longer than a GC cycle and the grace period, still
// commits with every copy in place. Its claims mark the chunks.
func TestGCSparesInflightUpload(t *testing.T) {
	e := newEnv(t, 3)
	st := e.stage(t, "/f", []byte("abcdefgh"))
	e.clock.Advance(75 * time.Second)
	if v, err := e.commitStaged(t, st); err != nil || v != 1 {
		t.Fatalf("commit after stalling: v%d, %v", v, err)
	}
	for i, raw := range st.ids {
		if n := e.copies(raw); n != 3 {
			t.Fatalf("chunk %d: %d copies, want 3", i, n)
		}
	}
	if gc := e.srv.GC(); gc.Sent != 0 {
		t.Fatalf("gc sent %d deletes for a live upload", gc.Sent)
	}
}

// TestUploadLeaseExpires: a client that stops mid-upload loses its lease;
// the upload is aborted, a late commit fails, and its chunks are collected.
func TestUploadLeaseExpires(t *testing.T) {
	e := newEnv(t, 3)
	st := e.stage(t, "/f", []byte("abcd"))
	e.clock.Advance(4 * time.Minute)
	if _, err := e.commitStaged(t, st); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("commit after the lease ran out: %v, want not found", err)
	}
	if _, ok := e.srv.State().Upload(st.id); ok {
		t.Fatal("expired upload still pending")
	}
	e.clock.Advance(2 * time.Minute)
	if n := e.copies(st.ids[0]); n != 0 {
		t.Fatalf("%d copies of an expired upload's chunk remain", n)
	}
}

func TestGCReclaimsDroppedVersions(t *testing.T) {
	e := newEnv(t, 3)
	if _, err := e.upload(t, "/keep", []byte("shared!!")); err != nil {
		t.Fatal(err)
	}
	// /gone shares its first chunk with /keep and has one of its own.
	if _, err := e.upload(t, "/gone", []byte("sharmine")); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(time.Second)
	if _, err := e.rpc(t, "meta", wire.KindDelete, wire.Marshal(&chunkdv1.DeleteRequest{Path: "/gone", ExpectedVersion: 1})); err != nil {
		t.Fatal(err)
	}
	shared, own := sha256.Sum256([]byte("shar")), sha256.Sum256([]byte("mine"))
	// Retention (3 epochs) then grace (1 min) then one more sweep.
	e.clock.Advance(3*time.Minute + 5*time.Second)
	if n := e.copies(own[:]); n != 0 {
		t.Fatalf("dropped version's own chunk: %d copies left", n)
	}
	if n := e.copies(shared[:]); n != 3 {
		t.Fatalf("chunk still referenced by /keep: %d copies, want 3", n)
	}
	if _, err := e.stat(t, "/keep"); err != nil {
		t.Fatal(err)
	}
}
