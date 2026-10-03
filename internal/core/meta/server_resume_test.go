package meta_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func (e *env) uploadStatus(t *testing.T, id uint64) (*chunkdv1.UploadStatusResponse, error) {
	t.Helper()
	body, err := e.rpc(t, "meta", wire.KindUploadStatus, wire.Marshal(&chunkdv1.UploadStatusRequest{UploadId: id}))
	if err != nil {
		return nil, err
	}
	var resp chunkdv1.UploadStatusResponse
	wire.Decode(body, &resp)
	return &resp, nil
}

func TestUploadStatus(t *testing.T) {
	e := newEnv(t, 3)
	data := []byte("0123456789") // chunks of 4, 4 and 2 bytes
	sum := sha256.Sum256(data)
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/f", Size: int64(len(data)), Sha256: sum[:]}))
	if err != nil {
		t.Fatal(err)
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	// Chunk 0 claimed and stored; chunk 1 claimed only (the client died
	// between claim and put).
	id0, id1 := sha256.Sum256(data[0:4]), sha256.Sum256(data[4:8])
	e.claim(t, begin.GetUploadId(), 0, id0[:])
	var calls []iface.Call
	for _, r := range begin.GetPlacement()[0].GetReplicas() {
		calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id0[:], Data: data[0:4]})})
	}
	e.caller.Do(context.Background(), calls)
	e.claim(t, begin.GetUploadId(), 1, id1[:])
	e.clock.Advance(time.Second) // block reports arrive

	st, err := e.uploadStatus(t, begin.GetUploadId())
	if err != nil {
		t.Fatal(err)
	}
	if st.GetPath() != "/f" || st.GetSize() != 10 || !bytes.Equal(st.GetSha256(), sum[:]) || len(st.GetBegin().GetPlacement()) != 3 || st.GetCommittedVersion() != 0 {
		t.Fatalf("status %v", st)
	}
	if c := st.GetClaims(); len(c) != 2 || !c[0].GetPresent() || c[1].GetPresent() || !bytes.Equal(c[1].GetId(), id1[:]) {
		t.Fatalf("claims %v", c)
	}
	if st.GetLeaseExpiresEpoch() <= st.GetEpoch() {
		t.Fatalf("lease expires at epoch %d, now %d", st.GetLeaseExpiresEpoch(), st.GetEpoch())
	}

	if _, err := e.uploadStatus(t, 99); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("unknown upload: %v", err)
	}
	// A committed upload reports the version it created.
	v, err := e.upload(t, "/g", []byte("abcd"))
	if err != nil {
		t.Fatal(err)
	}
	st, err = e.uploadStatus(t, begin.GetUploadId()+1)
	if err != nil || st.GetCommittedVersion() != v || st.GetPath() != "/g" {
		t.Fatalf("committed upload status %v, %v", st, err)
	}
}
