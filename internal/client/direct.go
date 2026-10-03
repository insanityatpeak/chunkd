package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// MetaPeer is one metadata server the client may talk to.
type MetaPeer struct {
	ID   iface.NodeID
	Addr string // real mode only
}

// Options configure a direct client.
type Options struct {
	// Meta and MetaAddr name a single metadata server. Ignored if Peers is set.
	Meta     iface.NodeID
	MetaAddr string // real mode only
	// Peers are the members of a metadata group. The client remembers which
	// one led last and follows a follower's hint to the leader.
	Peers []MetaPeer
	// Rand supplies the request ID of each upload's Begin, kept across retries
	// so a retry after a lost response or a leader change opens one upload.
	// Nil: Begin carries no ID and a retry may leave an extra pending upload,
	// which GC reclaims.
	Rand iface.Rand
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
	// PutGrace is how long a put waits for the remaining replicas once min_replicas
	// have stored a chunk: 0 uses DefaultPutGrace, negative waits for all of them.
	PutGrace time.Duration
}

// DefaultPutGrace lets a healthy third replica finish, so a healthy put still
// stores every copy, without letting a slow one hold the put.
const DefaultPutGrace = 50 * time.Millisecond

// Direct talks to the metadata server and storage nodes itself.
type Direct struct {
	caller iface.Caller
	opts   Options
	health *health
	codec  *ec.Codec
	peers  []MetaPeer
	// leader indexes the peer that answered last, or the one a follower named.
	leader atomic.Int32
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
	peers := opts.Peers
	if len(peers) == 0 {
		peers = []MetaPeer{{ID: opts.Meta, Addr: opts.MetaAddr}}
	}
	return &Direct{caller: caller, opts: opts, health: newHealth(), codec: ec.New(), peers: peers}
}

// meta calls the metadata service, finding its leader and riding out
// failures. Every metadata RPC is safe to repeat: reads trivially, claim,
// commit, delete and undelete by design, and begin through its request ID
// (Put sets one), so a retry after a lost response or a leader change never
// duplicates or half-applies anything.
//
// A follower answers CodeNotLeader naming the leader: the client goes there
// at once. With no hint (an election is running) or an unreachable peer it
// tries the next one after a pause, for about 11 s with three peers, roughly
// four election timeouts.
func (c *Direct) meta(ctx context.Context, kind string, req, resp proto.Message) error {
	body := wire.Marshal(req)
	attempts := metaAttempts
	if len(c.peers) > 1 {
		attempts = groupAttempts
	}
	backoff := 100 * time.Millisecond
	hops := 0
	for attempt := 1; ; attempt++ {
		i := int(c.leader.Load()) % len(c.peers)
		r := c.caller.Do(ctx, []iface.Call{{To: c.peers[i].ID, Addr: c.peers[i].Addr, Kind: kind, Body: body}})
		err := r[0].Err
		if err == nil {
			return wire.Decode(r[0].Body, resp)
		}
		code := iface.CodeOf(err)
		if (code != iface.CodeUnavailable && code != iface.CodeNotLeader) || attempt == attempts || ctx.Err() != nil {
			return err
		}
		// Follow a hint without waiting, but not round and round: stale
		// hints can point at each other.
		if j := c.peerIndex(err); code == iface.CodeNotLeader && j >= 0 && j != i && hops < len(c.peers) {
			c.leader.Store(int32(j))
			hops++
			continue
		}
		hops = 0
		c.leader.Store(int32((i + 1) % len(c.peers)))
		c.opts.Sleep(backoff)
		backoff = min(backoff*2, 2*time.Second)
	}
}

// peerIndex returns the peer a NotLeader error names, or -1.
func (c *Direct) peerIndex(err error) int {
	var e *iface.Error
	if !errors.As(err, &e) || e.Code != iface.CodeNotLeader || e.Msg == "" {
		return -1
	}
	return slices.IndexFunc(c.peers, func(p MetaPeer) bool { return string(p.ID) == e.Msg })
}

// requestID returns a fresh 16-byte ID for one upload's Begin, or nil.
func (c *Direct) requestID() []byte {
	if c.opts.Rand == nil {
		return nil
	}
	b := binary.LittleEndian.AppendUint64(nil, c.opts.Rand.Uint64())
	return binary.LittleEndian.AppendUint64(b, c.opts.Rand.Uint64())
}

const (
	metaAttempts  = 6
	groupAttempts = 10
)

// expectedVersion is the live version a write replaces: opts.ExpectedVersion,
// or with Overwrite whatever is live now (0 if nothing).
func (c *Direct) expectedVersion(ctx context.Context, path string, opts PutOptions) (uint64, error) {
	if !opts.Overwrite || opts.LastWriterWins {
		return opts.ExpectedVersion, nil
	}
	st, err := c.Stat(ctx, path)
	switch {
	case err == nil:
		return st.Version, nil
	case iface.CodeOf(err) == iface.CodeNotFound:
		return 0, nil
	}
	return 0, err
}

// Put uploads r (exactly size bytes) as a new version of path.
//
// Protocol: Begin (placement per chunk) → for each chunk, Claim it, then
// put it to every placed replica in parallel unless the claim found it
// present → Commit, retried while block reports are in flight. Any failure
// aborts the upload so nothing becomes visible.
// SIMPLIFIED: chunks go one at a time, with one claim round trip each. HDFS
// keeps a window of packets in flight so disk and network overlap.
func (c *Direct) Put(ctx context.Context, path string, r io.Reader, size int64, opts PutOptions) (Manifest, error) {
	expected, err := c.expectedVersion(ctx, path, opts)
	if err != nil {
		return Manifest{}, err
	}
	var begin chunkdv1.BeginUploadResponse
	if err := c.meta(ctx, wire.KindBegin, &chunkdv1.BeginUploadRequest{Path: path, ExpectedVersion: expected, Size: size, LastWriterWins: opts.LastWriterWins, RequestId: c.requestID(), Redundancy: opts.Redundancy.proto(), Quota: quotaOf(ctx)}, &begin); err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if begin.GetRedundancy() != opts.Redundancy.proto() {
		// A server that predates EC ignores the field and places copies.
		err = iface.Errorf(iface.CodeInvalid, "asked for %q, the metadata server opened a %v upload", opts.Redundancy, begin.GetRedundancy())
	} else {
		m, err = c.upload(ctx, path, r, size, &begin)
	}
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
	m := Manifest{FileInfo: FileInfo{Path: path, Size: size, Redundancy: redundancy(begin.GetRedundancy())}, ChunkSize: chunkSize}
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
		id, ref, err := c.storeChunk(ctx, begin, ch)
		if err != nil {
			return Manifest{}, err
		}
		m.Chunk = append(m.Chunk, ref)
		ids = append(ids, id[:])
	}
	if split.Total() != size {
		return Manifest{}, iface.Errorf(iface.CodeInvalid, "read %d bytes, declared %d", split.Total(), size)
	}
	sum := split.Sum()
	version, err := c.commit(ctx, &chunkdv1.CommitUploadRequest{UploadId: begin.GetUploadId(), ChunkIds: ids, Sha256: sum[:]})
	if err != nil {
		return Manifest{}, err
	}
	m.Version, m.SHA256, m.Chunks = version, hex.EncodeToString(sum[:]), len(ids)
	return m, nil
}

// storeChunk claims chunk ch of an upload and stores it, unless the claim
// finds it present. It returns the ID the commit names: the chunk's, or
// its stripe's.
func (c *Direct) storeChunk(ctx context.Context, begin *chunkdv1.BeginUploadResponse, ch chunk.Chunk) (iface.ChunkID, ChunkRef, error) {
	stripes := begin.GetRedundancy() == chunkdv1.Redundancy_REDUNDANCY_EC_4_2
	id := ch.ID
	cl := &chunkdv1.ChunkClaim{Index: int32(ch.Index), Id: id[:]}
	var shards []ec.Shard
	if stripes {
		// The claim names the stripe and its shards, so it is encoded first.
		var err error
		if shards, err = c.codec.Encode(ch.Data); err != nil {
			return id, ChunkRef{}, err
		}
		id = ec.LogicalID(ch.ID)
		cl.Id = id[:]
		for _, s := range shards {
			cl.Shards = append(cl.Shards, s.ID[:])
		}
	}
	var claim chunkdv1.ClaimChunksResponse
	if err := c.meta(ctx, wire.KindClaim, &chunkdv1.ClaimChunksRequest{UploadId: begin.GetUploadId(), Claims: []*chunkdv1.ChunkClaim{cl}}, &claim); err != nil {
		return id, ChunkRef{}, err
	}
	switch {
	case len(claim.GetPresent()) == 1 && claim.GetPresent()[0]:
		ref := ChunkRef{Index: ch.Index, ID: id.String(), Size: int64(len(ch.Data)), Deduped: true}
		for _, r := range claim.GetLocations()[0].GetReplicas() {
			ref.Replicas = append(ref.Replicas, r.GetNode())
		}
		return id, ref, nil
	case stripes:
		ref, err := c.putStripe(ctx, ch, shards, begin.GetPlacement()[ch.Index], int(begin.GetMinReplicas()))
		return id, ref, err
	default:
		ref, err := c.putChunk(ctx, ch, begin.GetPlacement()[ch.Index], int(begin.GetMinReplicas()))
		return id, ref, err
	}
}

// commit publishes an upload, retried while block reports are in flight or
// the metadata group elects a leader.
func (c *Direct) commit(ctx context.Context, req *chunkdv1.CommitUploadRequest) (uint64, error) {
	var resp chunkdv1.CommitUploadResponse
	backoff := 50 * time.Millisecond
	for waited := time.Duration(0); ; {
		err := c.meta(ctx, wire.KindCommit, req, &resp)
		if err == nil {
			return resp.GetVersion(), nil
		}
		if code := iface.CodeOf(err); (code != iface.CodeRetry && code != iface.CodeUnavailable) || waited >= c.opts.CommitTimeout {
			return 0, err
		}
		c.opts.Sleep(backoff)
		waited += backoff
		backoff = min(backoff*2, 2*time.Second)
	}
}

// putChunk sends one chunk to all its replicas at once and returns once min
// have stored it and PutGrace has passed for the rest: a replica that is slow
// no longer sets the put's latency. Replicas still in flight keep going; if one
// never lands, repair tops the chunk up from the others.
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
		for i, res := range c.putRound(ctx, calls, min-len(ref.Replicas)) {
			switch {
			case res.Pending:
				// Still in flight: not a failure, and not retried.
			case res.Err != nil:
				errs = append(errs, fmt.Errorf("%s: %w", calls[i].To, res.Err))
				failed = append(failed, calls[i])
			default:
				ref.Replicas = append(ref.Replicas, string(calls[i].To))
			}
		}
		calls = failed
	}
	if len(ref.Replicas) < min {
		return ChunkRef{}, iface.Errorf(iface.CodeUnavailable, "chunk %d: %d of %d required replicas stored: %v", ch.Index, len(ref.Replicas), min, errors.Join(errs...))
	}
	return ref, nil
}

// putRound sends calls and waits for need of them plus the grace period; a
// negative PutGrace waits for every call, as before ADR-0029.
func (c *Direct) putRound(ctx context.Context, calls []iface.Call, need int) []iface.Result {
	if c.opts.PutGrace < 0 {
		return c.caller.Do(ctx, calls)
	}
	grace := c.opts.PutGrace
	if grace == 0 {
		grace = DefaultPutGrace
	}
	return c.caller.Quorum(ctx, calls, max(need, 1), grace)
}

// Stat returns the live version of path and where its chunks are.
func (c *Direct) Stat(ctx context.Context, path string) (Manifest, error) {
	return c.StatVersion(ctx, path, 0)
}

// StatVersion returns one version of path; 0 is the live one.
func (c *Direct) StatVersion(ctx context.Context, path string, version uint64) (Manifest, error) {
	var st chunkdv1.StatResponse
	if err := c.meta(ctx, wire.KindStat, &chunkdv1.StatRequest{Path: path, Version: version}, &st); err != nil {
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
		FileInfo: FileInfo{Path: st.GetPath(), Version: st.GetVersion(), Size: st.GetSize(), SHA256: hex.EncodeToString(st.GetSha256()), Chunks: len(st.GetChunks()),
			Redundancy: redundancy(st.GetRedundancy())},
		ChunkSize: int(st.GetChunkSize()),
	}
}

func chunkRef(i int, loc *chunkdv1.ChunkLocation) ChunkRef {
	ref := ChunkRef{Index: i, ID: hex.EncodeToString(loc.GetId()), Size: loc.GetSize()}
	for _, r := range loc.GetReplicas() {
		ref.Replicas = append(ref.Replicas, r.GetNode())
	}
	for j, sl := range loc.GetShards() {
		sr := ShardRef{Index: j, ID: hex.EncodeToString(sl.GetId()), Replicas: []string{}}
		for _, r := range sl.GetReplicas() {
			sr.Replicas = append(sr.Replicas, r.GetNode())
		}
		ref.Shards = append(ref.Shards, sr)
	}
	return ref
}

// Get downloads path, verifying each chunk against its ID and the file
// against its recorded SHA-256. A replica with bad data is skipped and the
// next one tried.
func (c *Direct) Get(ctx context.Context, path string, w io.Writer) (Manifest, error) {
	return c.GetVersion(ctx, path, 0, w)
}

// Log returns every retained version of path.
func (c *Direct) Log(ctx context.Context, path string) ([]VersionInfo, error) {
	var resp chunkdv1.LogResponse
	if err := c.meta(ctx, wire.KindLog, &chunkdv1.LogRequest{Path: path}, &resp); err != nil {
		return nil, err
	}
	out := make([]VersionInfo, 0, len(resp.GetVersions()))
	for _, v := range resp.GetVersions() {
		vi := VersionInfo{Version: v.GetVersion(), Size: v.GetSize(), Chunks: int(v.GetChunkCount()), Deleted: v.GetTombstone(), Retired: v.GetRetired(), ExpiresEpoch: v.GetExpiresEpoch(),
			Redundancy: redundancy(v.GetRedundancy())}
		if !vi.Deleted {
			vi.SHA256 = hex.EncodeToString(v.GetSha256())
		}
		out = append(out, vi)
	}
	return out, nil
}

// GetVersion downloads one version of path; 0 is the live one.
func (c *Direct) GetVersion(ctx context.Context, path string, version uint64, w io.Writer) (Manifest, error) {
	var st chunkdv1.StatResponse
	if err := c.meta(ctx, wire.KindStat, &chunkdv1.StatRequest{Path: path, Version: version}, &st); err != nil {
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
		var data []byte
		var err error
		if len(loc.GetShards()) > 0 {
			data, err = c.fetchStripe(ctx, loc, &ref)
		} else {
			data, err = c.fetch(ctx, loc, &ref)
		}
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
	gone := map[string]bool{} // replicas that answered not_found
	// Two passes: a replica that timed out may answer the second time.
	for pass := 0; pass < 2; pass++ {
		if data, ok := c.fetchPass(ctx, loc, ref, &errs, gone); ok {
			return data, nil
		}
	}
	// Every copy failed verification, here or already on its node (a
	// replica quarantined by another reader or a repair answers not_found):
	// retrying cannot help, so say so plainly.
	if len(ref.Rejected) > 0 && !slices.ContainsFunc(ref.Replicas, func(n string) bool { return !gone[n] && !slices.Contains(ref.Rejected, n) }) {
		return nil, iface.Errorf(iface.CodeCorrupt, "chunk %d (%s): every replica %v failed verification; the data is lost: %v", ref.Index, ref.ID[:12], ref.Replicas, errors.Join(errs...))
	}
	return nil, iface.Errorf(iface.CodeUnavailable, "chunk %d (%s): no intact replica among %v: %v", ref.Index, ref.ID[:12], ref.Replicas, errors.Join(errs...))
}

func (c *Direct) fetchPass(ctx context.Context, loc *chunkdv1.ChunkLocation, ref *ChunkRef, errs *[]error, gone map[string]bool) ([]byte, bool) {
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
	var mismatch []string // nodes whose bytes failed this client's check
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
			mismatch = append(mismatch, reps[i].GetNode())
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
			switch iface.CodeOf(r.Err) {
			case iface.CodeCorrupt:
				if !slices.Contains(ref.Rejected, reps[i].GetNode()) {
					ref.Rejected = append(ref.Rejected, reps[i].GetNode())
				}
			case iface.CodeNotFound:
				gone[reps[i].GetNode()] = true
			}
		}
	}
	for _, node := range mismatch {
		c.suspect(ctx, loc.GetId(), node)
	}
	if h.Winner < 0 {
		return nil, false
	}
	ref.ServedBy = reps[h.Winner].GetNode()
	ref.Hedged = h.Launched > 1
	return data, true
}

// suspect tells the metadata server that node sent bytes failing the hash.
// Best effort: the read has already moved on, and the node's own re-check,
// not this client, decides whether the copy is bad.
// One attempt, no retries, so a slow metadata server never delays reads.
func (c *Direct) suspect(ctx context.Context, chunkID []byte, node string) {
	body := wire.Marshal(&chunkdv1.SuspectRequest{ChunkId: chunkID, Node: node})
	p := c.peers[int(c.leader.Load())%len(c.peers)]
	c.caller.Do(ctx, []iface.Call{{To: p.ID, Addr: p.Addr, Kind: wire.KindSuspect, Body: body}})
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
		out = append(out, FileInfo{Path: f.GetPath(), Version: f.GetVersion(), Size: f.GetSize(), SHA256: hex.EncodeToString(f.GetSha256()), Chunks: int(f.GetChunkCount()),
			Redundancy: redundancy(f.GetRedundancy())})
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

// Undelete restores a retained version. Both the version and the live
// version it replaces are resolved first, so the call is conditional and a
// retry finds its own result.
func (c *Direct) Undelete(ctx context.Context, path string, version uint64) (uint64, error) {
	if version == 0 {
		log, err := c.Log(ctx, path)
		if err != nil {
			return 0, err
		}
		for _, v := range slices.Backward(log) {
			if !v.Deleted {
				version = v.Version
				break
			}
		}
		if version == 0 {
			return 0, iface.Errorf(iface.CodeNotFound, "%s has no retained version to restore", path)
		}
	}
	var expected uint64
	switch m, err := c.Stat(ctx, path); {
	case err == nil:
		expected = m.Version
	case iface.CodeOf(err) != iface.CodeNotFound:
		return 0, err
	}
	var resp chunkdv1.UndeleteResponse
	err := c.meta(ctx, wire.KindUndelete, &chunkdv1.UndeleteRequest{Path: path, Version: version, ExpectedVersion: expected, Quota: quotaOf(ctx)}, &resp)
	return resp.GetVersion(), err
}

// Cluster returns the metadata server's view of the cluster.
func (c *Direct) Cluster(ctx context.Context, eventsAfter uint64) (Cluster, error) {
	var resp chunkdv1.ClusterResponse
	if err := c.meta(ctx, wire.KindCluster, &chunkdv1.ClusterRequest{EventsAfter: eventsAfter}, &resp); err != nil {
		return Cluster{}, err
	}
	return ClusterFromProto(&resp), nil
}

// ClusterFromProto converts the metadata server's cluster view. Slices are
// never nil, so JSON carries [] rather than null.
func ClusterFromProto(resp *chunkdv1.ClusterResponse) Cluster {
	out := Cluster{NowMs: resp.GetNowMs(), Files: resp.GetFiles(), LogicalBytes: resp.GetLogicalBytes(), EventSeq: resp.GetEventSeq(),
		Nodes: []NodeInfo{}, FileHealth: []FileHealth{}, Copies: []RepairCopy{}, Events: []Event{}, Deleted: []DeletedFile{},
		ReferencedBytes: resp.GetReferencedBytes(), DistinctBytes: resp.GetDistinctBytes(), Epoch: resp.GetEpoch(),
		MetaLeader: resp.GetMetaLeader(), MetaTerm: resp.GetMetaTerm(), MetaPeer: resp.GetMetaPeer(), MetaRole: resp.GetMetaRole(),
		MetaCommit: resp.GetMetaCommit(), MetaApplied: resp.GetMetaApplied(), MetaPeers: []MetaPeerInfo{}}
	for _, p := range resp.GetMetaPeers() {
		out.MetaPeers = append(out.MetaPeers, MetaPeerInfo{ID: p.GetId(), Match: p.GetMatch(), HeardAgoMs: p.GetHeardAgoMs()})
	}
	if g := resp.GetGc(); g != nil {
		out.GC = GCStats{Orphans: g.GetOrphans(), Sent: g.GetSent(), Deleted: g.GetDeleted(), Kept: g.GetKept(), Drift: g.GetDrift(),
			RetainEpochs: g.GetRetainEpochs(), EpochEveryMs: g.GetEpochEveryMs()}
	}
	for _, d := range resp.GetDeleted() {
		out.Deleted = append(out.Deleted, DeletedFile{Path: d.GetPath(), Version: d.GetVersion(), Size: d.GetSize(), ExpiresEpoch: d.GetExpiresEpoch()})
	}
	for _, n := range resp.GetNodes() {
		out.Nodes = append(out.Nodes, NodeInfo{ID: n.GetId(), Rack: n.GetRack(), Alive: n.GetAlive(), State: n.GetState(), Draining: n.GetDraining(), Admin: n.GetAdmin(),
			UsedBytes: n.GetUsedBytes(), Chunks: n.GetChunkCount(), HeartbeatAgeMs: n.GetHeartbeatAgeMs(),
			Corrupt: n.GetCorrupt(), ScrubDone: n.GetScrubDone(), ScrubTotal: n.GetScrubTotal(), ScrubPasses: n.GetScrubPasses(),
			BalanceUsed: n.GetBalanceUsed(), BalanceTarget: n.GetBalanceTarget(), BalanceBand: n.GetBalanceBand()})
	}
	h := resp.GetHealth()
	out.Health = Health{Chunks: h.GetChunks(), UnderReplicated: h.GetUnderReplicated(), OverReplicated: h.GetOverReplicated(), Lost: h.GetLost(),
		Replicas: h.GetReplicas(), RepairQueued: h.GetRepairQueued(), RepairInFlight: h.GetRepairInFlight(), RepairWaiting: h.GetRepairWaiting(),
		RepairCompleted: h.GetRepairCompleted(), RepairBytes: h.GetRepairBytes(), RepairTrimmed: h.GetRepairTrimmed(),
		RepairTimedOut: h.GetRepairTimedOut(), RepairFailed: h.GetRepairFailed(), DetectorStalls: h.GetDetectorStalls(),
		CorruptReplicas: h.GetCorruptReplicas(), RepairEvacuated: h.GetRepairEvacuated(), RepairMoved: h.GetRepairMoved()}
	for _, f := range resp.GetFileHealth() {
		out.FileHealth = append(out.FileHealth, FileHealth{Path: f.GetPath(), Chunks: int(f.GetChunks()),
			UnderReplicated: int(f.GetUnderReplicated()), MinLive: int(f.GetMinLive()), Redundancy: redundancy(f.GetRedundancy())})
	}
	for _, c := range resp.GetCopies() {
		out.Copies = append(out.Copies, RepairCopy{ID: c.GetId(), Chunk: hex.EncodeToString(c.GetChunkId()), Source: c.GetSource(),
			Target: c.GetTarget(), Bytes: c.GetBytes(), StartedMs: c.GetStartedMs(), RebuildFrom: c.GetRebuildFrom()})
	}
	for _, e := range resp.GetEvents() {
		out.Events = append(out.Events, Event{Seq: e.GetSeq(), AtMs: e.GetAtMs(), Kind: e.GetKind(), Node: e.GetNode(), Text: e.GetText()})
	}
	return out
}

// adminStates maps the API's state names to their wire values.
var adminStates = map[string]chunkdv1.NodeAdmin{
	"active":         chunkdv1.NodeAdmin_NODE_ADMIN_ACTIVE,
	"draining":       chunkdv1.NodeAdmin_NODE_ADMIN_DRAINING,
	"decommissioned": chunkdv1.NodeAdmin_NODE_ADMIN_DECOMMISSIONED,
}

// SetRetention keeps the retired versions of path for epochs GC epochs
// instead of the cluster default; 0 restores the default (ADR-0026).
func (c *Direct) SetRetention(ctx context.Context, path string, epochs uint32) error {
	var resp chunkdv1.SetRetentionResponse
	return c.meta(ctx, wire.KindSetRetention, &chunkdv1.SetRetentionRequest{Path: path, RetainEpochs: epochs}, &resp)
}

// NodeAdmin asks the metadata leader to change a node's admin state. A
// repeat of the state the node is in succeeds and changes nothing.
func (c *Direct) NodeAdmin(ctx context.Context, node, state string) (NodeAdminResult, error) {
	to, ok := adminStates[state]
	if !ok {
		return NodeAdminResult{}, iface.Errorf(iface.CodeInvalid, "unknown node state %q: want draining, active or decommissioned", state)
	}
	var resp chunkdv1.NodeAdminResponse
	if err := c.meta(ctx, wire.KindNodeAdmin, &chunkdv1.NodeAdminRequest{Node: node, State: to}, &resp); err != nil {
		return NodeAdminResult{}, err
	}
	return NodeAdminResult{Node: node, Admin: state, Warning: resp.GetWarning()}, nil
}
