package client

import (
	"context"
	"encoding/hex"
	"io"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// BeginResumable opens an upload of size bytes whose whole-file SHA-256 is
// sum, to be filled with Append from any process (ADR-0024). Versions are
// checked as in Put.
func (c *Direct) BeginResumable(ctx context.Context, path string, size int64, sum [32]byte, opts PutOptions) (UploadInfo, error) {
	expected, err := c.expectedVersion(ctx, path, opts)
	if err != nil {
		return UploadInfo{}, err
	}
	var begin chunkdv1.BeginUploadResponse
	if err := c.meta(ctx, wire.KindBegin, &chunkdv1.BeginUploadRequest{Path: path, ExpectedVersion: expected, Size: size, LastWriterWins: opts.LastWriterWins,
		RequestId: c.requestID(), Redundancy: opts.Redundancy.proto(), Sha256: sum[:], Quota: quotaOf(ctx)}, &begin); err != nil {
		return UploadInfo{}, err
	}
	if begin.GetRedundancy() != opts.Redundancy.proto() {
		var ignored chunkdv1.AbortUploadResponse
		_ = c.meta(ctx, wire.KindAbort, &chunkdv1.AbortUploadRequest{UploadId: begin.GetUploadId()}, &ignored)
		return UploadInfo{}, iface.Errorf(iface.CodeInvalid, "asked for %q, the metadata server opened a %v upload", opts.Redundancy, begin.GetRedundancy())
	}
	return c.UploadStatus(ctx, begin.GetUploadId())
}

// UploadStatus reports a resumable upload's progress, or the version it
// created once committed. An upload whose lease ran out is not found.
func (c *Direct) UploadStatus(ctx context.Context, id uint64) (UploadInfo, error) {
	st, err := c.status(ctx, id)
	if err != nil {
		return UploadInfo{}, err
	}
	return uploadInfo(st), nil
}

func (c *Direct) status(ctx context.Context, id uint64) (*chunkdv1.UploadStatusResponse, error) {
	var st chunkdv1.UploadStatusResponse
	if err := c.meta(ctx, wire.KindUploadStatus, &chunkdv1.UploadStatusRequest{UploadId: id}, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func uploadInfo(st *chunkdv1.UploadStatusResponse) UploadInfo {
	b := st.GetBegin()
	info := UploadInfo{ID: st.GetUploadId(), Path: st.GetPath(), Size: st.GetSize(), SHA256: hex.EncodeToString(st.GetSha256()),
		ChunkSize: int(b.GetChunkSize()), Redundancy: redundancy(b.GetRedundancy()), Version: st.GetCommittedVersion(),
		LeaseExpiresEpoch: st.GetLeaseExpiresEpoch(), Epoch: st.GetEpoch()}
	if info.Version != 0 {
		return info
	}
	stored, claimed := leadingRuns(st.GetClaims())
	info.Offset = min(int64(stored)*int64(info.ChunkSize), info.Size)
	info.Claimed = min(int64(claimed)*int64(info.ChunkSize), info.Size)
	return info
}

// leadingRuns counts the chunks from index 0 on that are claimed and
// stored, and those that are claimed.
func leadingRuns(claims []*chunkdv1.ClaimStatus) (stored, claimed int) {
	storedRun := true
	for _, cl := range claims { // sorted by index
		if int(cl.GetIndex()) != claimed {
			break
		}
		claimed++
		if storedRun = storedRun && cl.GetPresent(); storedRun {
			stored++
		}
	}
	return stored, claimed
}

// Append stores n bytes of r at offset, which must be a chunk boundary no
// further than the claimed run. n is whole chunks, or reaches the end of
// the file. Once the end is reached it commits with the declared SHA-256,
// and the returned UploadInfo carries the version. Chunks the cluster
// already holds are claimed but not sent, so appending again from a lower
// offset costs only claims.
func (c *Direct) Append(ctx context.Context, id uint64, offset int64, r io.Reader, n int64) (UploadInfo, error) {
	st, err := c.status(ctx, id)
	if err != nil {
		return UploadInfo{}, err
	}
	info := uploadInfo(st)
	if info.Version != 0 {
		return info, nil // a previous attempt committed; its response was lost
	}
	cs := int64(info.ChunkSize)
	switch {
	case offset < 0 || offset%cs != 0 || offset > info.Claimed:
		return info, iface.Errorf(iface.CodeConflict, "upload %d: offset %d, want a %d-byte chunk boundary no further than %d (stored up to %d)", id, offset, cs, info.Claimed, info.Offset)
	case n < 0 || offset+n > info.Size:
		return info, iface.Errorf(iface.CodeInvalid, "upload %d: %d bytes at %d run past its %d bytes", id, n, offset, info.Size)
	case n%cs != 0 && offset+n != info.Size:
		return info, iface.Errorf(iface.CodeInvalid, "upload %d: %d bytes is not whole %d-byte chunks and does not end the file", id, n, cs)
	}
	ids := make([][]byte, chunk.Count(info.Size, info.ChunkSize))
	for _, cl := range st.GetClaims() {
		ids[cl.GetIndex()] = cl.GetId()
	}
	split := chunk.NewSplitter(io.LimitReader(r, n), info.ChunkSize)
	base := int(offset / cs)
	for {
		ch, err := split.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return info, err
		}
		ch.Index += base
		cid, _, err := c.storeChunk(ctx, st.GetBegin(), ch)
		if err != nil {
			return info, err
		}
		ids[ch.Index] = cid[:]
		info.Offset = min(int64(ch.Index+1)*cs, info.Size)
		info.Claimed = max(info.Claimed, info.Offset)
	}
	if split.Total() != n {
		return info, iface.Errorf(iface.CodeInvalid, "upload %d: read %d bytes, expected %d", id, split.Total(), n)
	}
	if offset+n < info.Size {
		return info, nil
	}
	// Only a single pass sees every byte, so only it can check the
	// declaration before committing; otherwise reads check it (ADR-0024).
	if sum := split.Sum(); offset == 0 && hex.EncodeToString(sum[:]) != info.SHA256 {
		return info, iface.Errorf(iface.CodeInvalid, "upload %d: the file hashes to %x, %s was declared", id, sum, info.SHA256)
	}
	for i, cid := range ids {
		if cid == nil {
			return info, iface.Errorf(iface.CodeConflict, "upload %d: chunk %d was never claimed", id, i)
		}
	}
	if info.Version, err = c.commit(ctx, &chunkdv1.CommitUploadRequest{UploadId: id, ChunkIds: ids, Sha256: st.GetSha256()}); err != nil {
		return info, err
	}
	return info, nil
}
