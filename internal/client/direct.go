package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// Options configure a direct client.
type Options struct {
	Meta     iface.NodeID
	MetaAddr string // real mode only
	// CommitTimeout bounds commit retries while block reports arrive. It
	// exceeds the 30 s full-report interval so a lost incremental report is
	// recovered by the next full one.
	CommitTimeout time.Duration
	// Sleep waits between retries: time.Sleep in real mode, advancing the
	// simulated clock in sim.
	Sleep func(time.Duration)
	// NoHedge reads one replica at a time, moving on only after an error or
	// timeout. Tests use it as the baseline hedging is measured against.
	NoHedge bool
}

// Direct talks to the metadata server and storage nodes itself.
type Direct struct {
	caller iface.Caller
	opts   Options
	health *health
}

var _ API = (*Direct)(nil)

// New returns a direct client over caller.
func New(caller iface.Caller, opts Options) *Direct {
	if opts.CommitTimeout == 0 {
		opts.CommitTimeout = 45 * time.Second
	}
	if opts.Sleep == nil {
		opts.Sleep = time.Sleep
	}
	return &Direct{caller: caller, opts: opts, health: newHealth()}
}

// meta calls the metadata server, retrying while it is unreachable. Every
// metadata RPC is safe to repeat: reads trivially, commit and delete by
// design, and a repeated begin only leaves an extra pending upload that GC
// reclaims (Phase 4).
func (c *Direct) meta(ctx context.Context, kind string, req, resp proto.Message) error {
	body := wire.Marshal(req)
	backoff := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		r := c.caller.Do(ctx, []iface.Call{{To: c.opts.Meta, Addr: c.opts.MetaAddr, Kind: kind, Body: body}})
		if r[0].Err == nil {
			return wire.Decode(r[0].Body, resp)
		}
		if iface.CodeOf(r[0].Err) != iface.CodeUnavailable || attempt == metaAttempts || ctx.Err() != nil {
			return r[0].Err
		}
		c.opts.Sleep(backoff)
		backoff = min(backoff*2, 2*time.Second)
	}
}

const metaAttempts = 6

// Put uploads r (exactly size bytes) as a new version of path.
//
// Protocol: Begin (placement per chunk) → for each chunk, put to every
// placed replica in parallel → Commit, retried while block reports are in
// flight. Any failure aborts the upload so nothing becomes visible.
// SIMPLIFIED: chunks go one at a time. HDFS keeps a window of packets in
// flight so disk and network overlap.
func (c *Direct) Put(ctx context.Context, path string, r io.Reader, size int64, opts PutOptions) (Manifest, error) {
	expected := opts.ExpectedVersion
	if opts.Overwrite {
		expected = 0
		st, err := c.Stat(ctx, path)
		switch {
		case err == nil:
			expected = st.Version
		case iface.CodeOf(err) != iface.CodeNotFound:
			return Manifest{}, err
		}
	}
	var begin chunkdv1.BeginUploadResponse
	if err := c.meta(ctx, wire.KindBegin, &chunkdv1.BeginUploadRequest{Path: path, ExpectedVersion: expected, Size: size}, &begin); err != nil {
		return Manifest{}, err
	}
	m, err := c.upload(ctx, path, r, size, &begin)
	if err != nil {
		// Best effort: an upload left pending is invisible anyway and is
		// reclaimed by GC (Phase 4).
		var ignored chunkdv1.AbortUploadResponse
		_ = c.meta(ctx, wire.KindAbort, &chunkdv1.AbortUploadRequest{UploadId: begin.GetUploadId()}, &ignored)
		return Manifest{}, err
	}
	return m, nil
}

func (c *Direct) upload(ctx context.Context, path string, r io.Reader, size int64, begin *chunkdv1.BeginUploadResponse) (Manifest, error) {
	chunkSize := int(begin.GetChunkSize())
	split := chunk.NewSplitter(r, chunkSize)
	m := Manifest{FileInfo: FileInfo{Path: path, Size: size}, ChunkSize: chunkSize}
	var ids [][]byte
	for {
		ch, err := split.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Manifest{}, err
		}
		if ch.Index >= len(begin.GetPlacement()) {
			return Manifest{}, iface.Errorf(iface.CodeInvalid, "input longer than the declared %d bytes", size)
		}
		ref, err := c.putChunk(ctx, ch, begin.GetPlacement()[ch.Index], int(begin.GetMinReplicas()))
		if err != nil {
			return Manifest{}, err
		}
		m.Chunk = append(m.Chunk, ref)
		ids = append(ids, ch.ID[:])
	}
	if split.Total() != size {
		return Manifest{}, iface.Errorf(iface.CodeInvalid, "read %d bytes, declared %d", split.Total(), size)
	}
	sum := split.Sum()
	req := &chunkdv1.CommitUploadRequest{UploadId: begin.GetUploadId(), ChunkIds: ids, Sha256: sum[:]}
	var resp chunkdv1.CommitUploadResponse
	backoff := 50 * time.Millisecond
	for waited := time.Duration(0); ; {
		err := c.meta(ctx, wire.KindCommit, req, &resp)
		if err == nil {
			break
		}
		if code := iface.CodeOf(err); (code != iface.CodeRetry && code != iface.CodeUnavailable) || waited >= c.opts.CommitTimeout {
			return Manifest{}, err
		}
		c.opts.Sleep(backoff)
		waited += backoff
		backoff = min(backoff*2, 2*time.Second)
	}
	m.Version, m.SHA256, m.Chunks = resp.GetVersion(), hex.EncodeToString(sum[:]), len(ids)
	return m, nil
}

// putChunk sends one chunk to all its replicas at once and fails if fewer
// than min acknowledge.
func (c *Direct) putChunk(ctx context.Context, ch chunk.Chunk, pl *chunkdv1.ChunkPlacement, min int) (ChunkRef, error) {
	body := wire.Marshal(&chunkdv1.PutChunkRequest{Id: ch.ID[:], Data: ch.Data})
	calls := make([]iface.Call, len(pl.GetReplicas()))
	for i, rep := range pl.GetReplicas() {
		calls[i] = iface.Call{To: iface.NodeID(rep.GetNode()), Addr: rep.GetAddr(), Kind: wire.KindPutChunk, Body: body}
	}
	ref := ChunkRef{Index: ch.Index, ID: ch.ID.String(), Size: int64(len(ch.Data))}
	var errs []error
	// Two rounds: a put lost in transit is retried once (puts are
	// idempotent), so one dropped message does not cost a replica.
	for round := 0; round < 2 && len(calls) > 0; round++ {
		errs = errs[:0]
		var failed []iface.Call
		for i, res := range c.caller.Do(ctx, calls) {
			if res.Err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", calls[i].To, res.Err))
				failed = append(failed, calls[i])
				continue
			}
			ref.Replicas = append(ref.Replicas, string(calls[i].To))
		}
		calls = failed
	}
	if len(ref.Replicas) < min {
		return ChunkRef{}, iface.Errorf(iface.CodeUnavailable, "chunk %d: %d of %d required replicas stored: %v", ch.Index, len(ref.Replicas), min, errors.Join(errs...))
	}
	return ref, nil
}

// Stat returns the live version of path and where its chunks are.
func (c *Direct) Stat(ctx context.Context, path string) (Manifest, error) {
	var st chunkdv1.StatResponse
	if err := c.meta(ctx, wire.KindStat, &chunkdv1.StatRequest{Path: path}, &st); err != nil {
		return Manifest{}, err
	}
	m := manifest(&st)
	for i, loc := range st.GetChunks() {
		m.Chunk = append(m.Chunk, chunkRef(i, loc))
	}
	return m, nil
}

func manifest(st *chunkdv1.StatResponse) Manifest {
	return Manifest{
		FileInfo:  FileInfo{Path: st.GetPath(), Version: st.GetVersion(), Size: st.GetSize(), SHA256: hex.EncodeToString(st.GetSha256()), Chunks: len(st.GetChunks())},
		ChunkSize: int(st.GetChunkSize()),
	}
}

func chunkRef(i int, loc *chunkdv1.ChunkLocation) ChunkRef {
	ref := ChunkRef{Index: i, ID: hex.EncodeToString(loc.GetId()), Size: loc.GetSize()}
	for _, r := range loc.GetReplicas() {
		ref.Replicas = append(ref.Replicas, r.GetNode())
	}
	return ref
}

// Get downloads path, verifying each chunk against its ID and the file
// against its recorded SHA-256. A replica with bad data is skipped and the
// next one tried.
func (c *Direct) Get(ctx context.Context, path string, w io.Writer) (Manifest, error) {
	var st chunkdv1.StatResponse
	if err := c.meta(ctx, wire.KindStat, &chunkdv1.StatRequest{Path: path}, &st); err != nil {
		return Manifest{}, err
	}
	m := manifest(&st)
	if mw, ok := w.(ManifestWriter); ok {
		full := m
		for i, loc := range st.GetChunks() {
			full.Chunk = append(full.Chunk, chunkRef(i, loc))
		}
		mw.SetManifest(full)
	}
	file := sha256.New()
	for i, loc := range st.GetChunks() {
		ref := chunkRef(i, loc)
		data, err := c.fetch(ctx, loc, &ref)
		if err != nil {
			return m, err
		}
		file.Write(data)
		if _, err := w.Write(data); err != nil {
			return m, err
		}
		m.Chunk = append(m.Chunk, ref)
	}
	if !bytes.Equal(file.Sum(nil), st.GetSha256()) {
		return m, iface.Errorf(iface.CodeInternal, "%s: file SHA-256 does not match the committed version", path)
	}
	return m, nil
}

// fetch reads one chunk, hedged: the best-scored replica first, the next one
// after the recent p95 read latency, or at once if a replica fails or
// returns data that does not match the chunk hash.
func (c *Direct) fetch(ctx context.Context, loc *chunkdv1.ChunkLocation, ref *ChunkRef) ([]byte, error) {
	var errs []error
	// Two passes: a replica that timed out may answer the second time.
	for pass := 0; pass < 2; pass++ {
		if data, ok := c.fetchPass(ctx, loc, ref, &errs); ok {
			return data, nil
		}
	}
	return nil, iface.Errorf(iface.CodeUnavailable, "chunk %d (%s): no intact replica among %v: %v", ref.Index, ref.ID[:12], ref.Replicas, errors.Join(errs...))
}

func (c *Direct) fetchPass(ctx context.Context, loc *chunkdv1.ChunkLocation, ref *ChunkRef, errs *[]error) ([]byte, bool) {
	reps := c.health.order(loc.GetReplicas())
	if len(reps) == 0 {
		return nil, false
	}
	body := wire.Marshal(&chunkdv1.GetChunkRequest{Id: loc.GetId()})
	calls := make([]iface.Call, len(reps))
	for i, r := range reps {
		calls[i] = iface.Call{To: iface.NodeID(r.GetNode()), Addr: r.GetAddr(), Kind: wire.KindGetChunk, Body: body}
	}
	after := c.health.hedgeDelay()
	if c.opts.NoHedge {
		after = noHedge
	}
	var data []byte
	rejected := make([]bool, len(reps))
	h := c.caller.Hedge(ctx, calls, after, func(i int, r iface.Result) bool {
		var resp chunkdv1.GetChunkResponse
		if err := wire.Decode(r.Body, &resp); err != nil {
			rejected[i] = true
			*errs = append(*errs, fmt.Errorf("%s: %w", reps[i].GetNode(), err))
			return false
		}
		if sum := sha256.Sum256(resp.GetData()); !bytes.Equal(sum[:], loc.GetId()) {
			rejected[i] = true
			if !slices.Contains(ref.Rejected, reps[i].GetNode()) {
				ref.Rejected = append(ref.Rejected, reps[i].GetNode())
			}
			*errs = append(*errs, fmt.Errorf("%s: data does not match chunk hash", reps[i].GetNode()))
			return false
		}
		data = resp.GetData()
		return true
	})
	for i := range h.Launched {
		r := h.Results[i]
		c.health.observe(reps[i].GetNode(), r, i == h.Winner)
		if r.Err != nil && !r.Pending {
			*errs = append(*errs, fmt.Errorf("%s: %w", reps[i].GetNode(), r.Err))
		}
	}
	if h.Winner < 0 {
		return nil, false
	}
	ref.ServedBy = reps[h.Winner].GetNode()
	ref.Hedged = h.Launched > 1
	return data, true
}

// noHedge is a hedge delay no read reaches.
const noHedge = time.Duration(1) << 60

// List returns live files under prefix.
func (c *Direct) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	var resp chunkdv1.ListResponse
	if err := c.meta(ctx, wire.KindList, &chunkdv1.ListRequest{Prefix: prefix}, &resp); err != nil {
		return nil, err
	}
	out := make([]FileInfo, 0, len(resp.GetFiles()))
	for _, f := range resp.GetFiles() {
		out = append(out, FileInfo{Path: f.GetPath(), Version: f.GetVersion(), Size: f.GetSize(), SHA256: hex.EncodeToString(f.GetSha256()), Chunks: int(f.GetChunkCount())})
	}
	return out, nil
}

// Delete tombstones path; expectedVersion 0 deletes whatever is live. The
// live version is resolved first so the delete itself is always
// conditional, which is what makes a retried delete safe.
func (c *Direct) Delete(ctx context.Context, path string, expectedVersion uint64) (uint64, error) {
	if expectedVersion == 0 {
		m, err := c.Stat(ctx, path)
		if err != nil {
			return 0, err
		}
		expectedVersion = m.Version
	}
	var resp chunkdv1.DeleteResponse
	err := c.meta(ctx, wire.KindDelete, &chunkdv1.DeleteRequest{Path: path, ExpectedVersion: expectedVersion}, &resp)
	return resp.GetVersion(), err
}

// Cluster returns the metadata server's view of the nodes.
func (c *Direct) Cluster(ctx context.Context) (Cluster, error) {
	var resp chunkdv1.ClusterResponse
	if err := c.meta(ctx, wire.KindCluster, &chunkdv1.ClusterRequest{}, &resp); err != nil {
		return Cluster{}, err
	}
	out := Cluster{Files: resp.GetFiles(), LogicalBytes: resp.GetLogicalBytes()}
	for _, n := range resp.GetNodes() {
		out.Nodes = append(out.Nodes, NodeInfo{ID: n.GetId(), Rack: n.GetRack(), Alive: n.GetAlive(), Draining: n.GetDraining(), UsedBytes: n.GetUsedBytes(), Chunks: n.GetChunkCount()})
	}
	return out, nil
}
