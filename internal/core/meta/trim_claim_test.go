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

// diskCopies counts the nodes whose disk holds id.
func (e *env) diskCopies(id iface.ChunkID) (n int) {
	for _, d := range e.disks {
		if _, err := d.Get(context.Background(), id); err == nil {
			n++
		}
	}
	return n
}

// A trim must not delete a copy of a chunk a pending upload has claimed: that
// upload's dedup may have counted the copy, and its commit would then rest on
// one that is deleted a moment later. The surplus copy is trimmed once the
// claim is gone.
func TestTrimSkipsChunkWithPendingClaim(t *testing.T) {
	e := newEnv(t, 5)
	data := []byte("abcd")
	id := iface.ChunkID(sha256.Sum256(data))
	if _, err := e.upload(t, "/a", data); err != nil {
		t.Fatal(err)
	}
	if got := e.diskCopies(id); got != 3 {
		t.Fatalf("%d copies after the upload, want 3", got)
	}
	// A fourth copy the cluster did not place: surplus, so repair trims one.
	for _, d := range e.disks {
		if _, err := d.Get(context.Background(), id); err != nil {
			if err := d.Put(context.Background(), id, data); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	// A second upload claims the chunk and stays pending.
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/b", Size: int64(len(data))}))
	if err != nil {
		t.Fatal(err)
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	e.claim(t, begin.GetUploadId(), 0, id[:])

	e.clock.Advance(90 * time.Second)
	if got := e.diskCopies(id); got != 4 {
		t.Fatalf("%d copies while the chunk is claimed, want 4: a trim ran under a pending claim", got)
	}
	if _, err := e.rpc(t, "meta", wire.KindAbort, wire.Marshal(&chunkdv1.AbortUploadRequest{UploadId: begin.GetUploadId()})); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(90 * time.Second)
	if got := e.diskCopies(id); got != 3 {
		t.Fatalf("%d copies after the claim ended, want the surplus trimmed to 3", got)
	}
}
