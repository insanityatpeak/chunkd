package meta_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

type env struct {
	clock  *sim.Clock
	net    *sim.Net
	rng    iface.Rand
	store  *sim.MetaStore
	srv    *meta.Server
	caller *sim.Caller
	log    *slog.Logger
	disks  []*sim.BlockStore // each node's disk, in node order
}

func newEnv(t *testing.T, nodes int) *env {
	t.Helper()
	e := &env{clock: sim.NewClock(), store: sim.NewMetaStore(), rng: sim.NewRand(1), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	e.net = sim.NewNet(e.clock, e.rng, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})
	e.startMeta(t)
	for i := 1; i <= nodes; i++ {
		id := iface.NodeID(fmt.Sprintf("n%d", i))
		disk := sim.NewBlockStore()
		e.disks = append(e.disks, disk)
		n, err := node.New(node.Deps{Clock: e.clock, Net: e.net, Async: e.net.AsyncCaller(id, 10*time.Second), Store: disk, Rand: e.rng, Log: e.log},
			node.DefaultConfig(id, "meta", fmt.Sprintf("r%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
	}
	e.caller = e.net.NewCaller("client", 10*time.Second)
	e.clock.Advance(3 * time.Second) // heartbeats and first reports arrive
	return e
}

func (e *env) startMeta(t *testing.T) {
	t.Helper()
	if e.srv != nil {
		e.srv.Stop() // a restart replaces the process; the old one must not share its store
	}
	cfg := meta.DefaultConfig("meta")
	cfg.ChunkSize = 4
	srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: e.clock, Net: e.net, Store: e.store, Rand: e.rng, Log: e.log}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	e.srv = srv
}

func (e *env) rpc(t *testing.T, to iface.NodeID, kind string, body []byte) ([]byte, error) {
	t.Helper()
	r := e.caller.Do(context.Background(), []iface.Call{{To: to, Kind: kind, Body: body}})
	return r[0].Body, r[0].Err
}

// upload runs the protocol by hand: begin, claim and put every chunk the
// cluster lacks, commit with retries.
func (e *env) upload(t *testing.T, path string, data []byte) (uint64, error) {
	v, _, err := e.uploadCounting(t, path, data)
	return v, err
}

// uploadCounting is upload that also returns how many chunks were skipped
// because a claim found them present.
func (e *env) uploadCounting(t *testing.T, path string, data []byte) (uint64, int, error) {
	t.Helper()
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: path, Size: int64(len(data))}))
	if err != nil {
		return 0, 0, err
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	var ids [][]byte
	skipped := 0
	for i, pl := range begin.GetPlacement() {
		part := data[i*4 : min((i+1)*4, len(data))]
		id := sha256.Sum256(part)
		ids = append(ids, id[:])
		body, err := e.rpc(t, "meta", wire.KindClaim, wire.Marshal(&chunkdv1.ClaimChunksRequest{UploadId: begin.GetUploadId(),
			Claims: []*chunkdv1.ChunkClaim{{Index: int32(i), Id: id[:]}}}))
		if err != nil {
			return 0, 0, err
		}
		var claim chunkdv1.ClaimChunksResponse
		wire.Decode(body, &claim)
		if claim.GetPresent()[0] {
			skipped++
			continue
		}
		var calls []iface.Call
		for _, r := range pl.GetReplicas() {
			calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: part})})
		}
		e.caller.Do(context.Background(), calls)
	}
	sum := sha256.Sum256(data)
	commit := wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: begin.GetUploadId(), ChunkIds: ids, Sha256: sum[:]})
	for range 20 {
		body, err = e.rpc(t, "meta", wire.KindCommit, commit)
		if iface.CodeOf(err) != iface.CodeRetry {
			break
		}
		e.clock.Advance(100 * time.Millisecond)
	}
	if err != nil {
		return 0, skipped, err
	}
	var resp chunkdv1.CommitUploadResponse
	wire.Decode(body, &resp)
	return resp.GetVersion(), skipped, nil
}

// claim claims chunk i of an upload as id.
func (e *env) claim(t *testing.T, upload uint64, i int, id []byte) {
	t.Helper()
	if _, err := e.rpc(t, "meta", wire.KindClaim, wire.Marshal(&chunkdv1.ClaimChunksRequest{UploadId: upload,
		Claims: []*chunkdv1.ChunkClaim{{Index: int32(i), Id: id}}})); err != nil {
		t.Fatal(err)
	}
}

func (e *env) stat(t *testing.T, path string) (*chunkdv1.StatResponse, error) {
	t.Helper()
	body, err := e.rpc(t, "meta", wire.KindStat, wire.Marshal(&chunkdv1.StatRequest{Path: path}))
	if err != nil {
		return nil, err
	}
	var resp chunkdv1.StatResponse
	wire.Decode(body, &resp)
	return &resp, nil
}

func TestUploadCommitsWithReportedReplicas(t *testing.T) {
	e := newEnv(t, 3)
	v, err := e.upload(t, "/f", []byte("0123456789"))
	if err != nil || v != 1 {
		t.Fatalf("upload = v%d, %v", v, err)
	}
	st, err := e.stat(t, "/f")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.GetChunks()) != 3 {
		t.Fatalf("%d chunks, want 3", len(st.GetChunks()))
	}
	for i, c := range st.GetChunks() {
		if len(c.GetReplicas()) != 3 {
			t.Fatalf("chunk %d has %d replicas, want 3", i, len(c.GetReplicas()))
		}
	}
}

func TestCommitRequiresMinReplicas(t *testing.T) {
	e := newEnv(t, 3)
	// Begin sees 3 live nodes; then two crash before the data is written.
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/f", Size: 4}))
	if err != nil {
		t.Fatal(err)
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	e.net.Crash("n2")
	e.net.Crash("n3")
	data := []byte("abcd")
	id := sha256.Sum256(data)
	e.claim(t, begin.GetUploadId(), 0, id[:])
	var calls []iface.Call
	for _, r := range begin.GetPlacement()[0].GetReplicas() {
		calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: data})})
	}
	e.caller.Do(context.Background(), calls)
	sum := sha256.Sum256(data)
	_, err = e.rpc(t, "meta", wire.KindCommit, wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: begin.GetUploadId(), ChunkIds: [][]byte{id[:]}, Sha256: sum[:]}))
	if iface.CodeOf(err) != iface.CodeRetry {
		t.Fatalf("commit with 1 replica: err = %v, want retry", err)
	}
	if _, err := e.stat(t, "/f"); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("version visible after failed commit: %v", err)
	}
	// With only one node alive, new uploads cannot even begin.
	e.clock.Advance(10 * time.Second)
	if _, err := e.upload(t, "/g", data); iface.CodeOf(err) != iface.CodeUnavailable {
		t.Fatalf("begin with 1 live node: err = %v, want unavailable", err)
	}
}

func TestRestartRecoversStateAndLocations(t *testing.T) {
	e := newEnv(t, 3)
	for i := range 5 {
		if _, err := e.upload(t, fmt.Sprintf("/f%d", i), []byte(fmt.Sprintf("file number %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	before := e.srv.State().Snapshot()

	// Restart the metadata server on the same log. Locations are not
	// durable: the restarted server asks nodes for full reports through
	// heartbeat acks and rebuilds them.
	e.startMeta(t)
	if got := e.srv.State().Snapshot(); string(got) != string(before) {
		t.Fatal("recovered state differs from state before restart")
	}
	if st, _ := e.stat(t, "/f0"); len(st.GetChunks()[0].GetReplicas()) != 0 {
		t.Fatal("restarted server knew locations before any block report")
	}
	e.clock.Advance(3 * time.Second)
	st, err := e.stat(t, "/f0")
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range st.GetChunks() {
		if len(c.GetReplicas()) != 3 {
			t.Fatalf("chunk %d: %d replicas after reports, want 3", i, len(c.GetReplicas()))
		}
	}
}

func TestSnapshotDuringOperationRecovers(t *testing.T) {
	e := newEnv(t, 3)
	cfg := meta.DefaultConfig("meta")
	cfg.ChunkSize, cfg.SnapshotEvery = 4, 5
	e.srv.Stop() // the first server's consensus node must not share the store
	srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: e.clock, Net: e.net, Store: e.store, Rand: e.rng, Log: e.log}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	e.srv = srv
	e.clock.Advance(3 * time.Second) // a new server knows no nodes until they heartbeat
	for i := range 4 {               // 12 ops (begin, claim, commit): snapshots at 5 and 10, two entries after
		if _, err := e.upload(t, fmt.Sprintf("/s%d", i), []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
	// Raft numbers its own entries too (the bootstrap membership, the leader's
	// no-op), so assert the shape: a snapshot, and a log tail after it.
	if at, _, _ := e.store.LoadSnapshot(context.Background()); at == 0 || at >= e.srv.Applied() {
		t.Fatalf("snapshot at %d with %d applied: want a snapshot and entries after it", at, e.srv.Applied())
	}
	before := e.srv.State().Snapshot()
	e.startMeta(t)
	if string(e.srv.State().Snapshot()) != string(before) {
		t.Fatal("snapshot + WAL tail did not recover the same state")
	}
}

// Regression: a duplicated or retried commit used to fail with not_found
// after the first copy committed, so the client reported failure for a file
// that was in fact visible.
func TestCommitAndDeleteAreIdempotent(t *testing.T) {
	e := newEnv(t, 3)
	body, _ := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/f", Size: 4}))
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	data := []byte("abcd")
	id := sha256.Sum256(data)
	e.claim(t, begin.GetUploadId(), 0, id[:])
	var calls []iface.Call
	for _, r := range begin.GetPlacement()[0].GetReplicas() {
		calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: data})})
	}
	e.caller.Do(context.Background(), calls)
	e.clock.Advance(100 * time.Millisecond)
	commit := wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: begin.GetUploadId(), ChunkIds: [][]byte{id[:]}, Sha256: id[:]})
	for i := range 3 {
		body, err := e.rpc(t, "meta", wire.KindCommit, commit)
		var resp chunkdv1.CommitUploadResponse
		wire.Decode(body, &resp)
		if err != nil || resp.GetVersion() != 1 {
			t.Fatalf("commit attempt %d = v%d, %v; want v1 every time", i, resp.GetVersion(), err)
		}
	}
	del := wire.Marshal(&chunkdv1.DeleteRequest{Path: "/f", ExpectedVersion: 1})
	for i := range 2 {
		body, err := e.rpc(t, "meta", wire.KindDelete, del)
		var resp chunkdv1.DeleteResponse
		wire.Decode(body, &resp)
		if err != nil || resp.GetVersion() != 2 {
			t.Fatalf("delete attempt %d = v%d, %v; want tombstone v2 every time", i, resp.GetVersion(), err)
		}
	}
	// The committed-upload index survives a restart (rebuilt from versions).
	e.startMeta(t)
	if _, err := e.rpc(t, "meta", wire.KindCommit, commit); err != nil {
		t.Fatalf("commit retry after restart: %v", err)
	}
}

func TestDedupSkipsPresentChunks(t *testing.T) {
	e := newEnv(t, 3)
	data := []byte("same bytes, twice")
	if _, n, err := e.uploadCounting(t, "/a", data); err != nil || n != 0 {
		t.Fatalf("first upload: skipped %d, err %v; want 0, nil", n, err)
	}
	e.clock.Advance(time.Second) // reports for /a arrive
	v, n, err := e.uploadCounting(t, "/b", data)
	if want := (len(data) + 3) / 4; err != nil || n != want || v != 1 {
		t.Fatalf("second upload: v%d, skipped %d, err %v; want v1, %d, nil", v, n, err, want)
	}
	if got := e.srv.DedupSkipped(); got != uint64(len(data)) {
		t.Fatalf("skipped %d bytes, want %d", got, len(data))
	}
	if ref, uniq := e.srv.State().Dedup(); ref != 2*int64(len(data)) || uniq != int64(len(data)) {
		t.Fatalf("referenced %d, unique %d; want %d, %d", ref, uniq, 2*len(data), len(data))
	}
}

func TestCommitWithoutClaimRejected(t *testing.T) {
	e := newEnv(t, 3)
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/a", Size: 4}))
	if err != nil {
		t.Fatal(err)
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	id := sha256.Sum256([]byte("abcd"))
	sum := sha256.Sum256([]byte("abcd"))
	_, err = e.rpc(t, "meta", wire.KindCommit, wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: begin.GetUploadId(), ChunkIds: [][]byte{id[:]}, Sha256: sum[:]}))
	if iface.CodeOf(err) != iface.CodeInvalid {
		t.Fatalf("commit of an unclaimed chunk: err = %v, want invalid", err)
	}
}

// TestCASConflict: two writers begin against the same version, write their
// chunks, then commit. Exactly one wins; the loser gets a conflict and its
// content is never visible.
func TestCASConflict(t *testing.T) {
	e := newEnv(t, 3)
	type writer struct {
		begin chunkdv1.BeginUploadResponse
		data  []byte
		id    [32]byte
	}
	ws := []*writer{{data: []byte("AAAA")}, {data: []byte("BBBB")}}
	for _, w := range ws {
		body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/f", Size: 4}))
		if err != nil {
			t.Fatal(err)
		}
		wire.Decode(body, &w.begin)
		w.id = sha256.Sum256(w.data)
		e.claim(t, w.begin.GetUploadId(), 0, w.id[:])
		var calls []iface.Call
		for _, r := range w.begin.GetPlacement()[0].GetReplicas() {
			calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: w.id[:], Data: w.data})})
		}
		e.caller.Do(context.Background(), calls)
	}
	e.clock.Advance(time.Second)
	var won, conflicts int
	var winner []byte
	for _, w := range ws {
		_, err := e.rpc(t, "meta", wire.KindCommit, wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: w.begin.GetUploadId(), ChunkIds: [][]byte{w.id[:]}, Sha256: w.id[:]}))
		switch iface.CodeOf(err) {
		case iface.CodeUnknown:
			won++
			winner = w.id[:]
		case iface.CodeConflict:
			conflicts++
		default:
			t.Fatalf("commit: %v", err)
		}
	}
	if won != 1 || conflicts != 1 {
		t.Fatalf("%d commits won and %d conflicted, want 1 and 1", won, conflicts)
	}
	st, err := e.stat(t, "/f")
	if err != nil || st.GetVersion() != 1 || string(st.GetSha256()) != string(winner) {
		t.Fatalf("stat after the race: %+v, %v", st, err)
	}
}

// Three metadata peers: only the leader proposes epochs and serves requests,
// a follower names the leader, every peer applies the same log, and a new
// leader takes over when the old one dies.
func TestMetadataGroupRedirectsReplicatesAndFailsOver(t *testing.T) {
	clock := sim.NewClock()
	rng := sim.NewRand(3)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	net := sim.NewNet(clock, rng, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})
	peers := []iface.NodeID{"meta-1", "meta-2", "meta-3"}
	srvs := map[iface.NodeID]*meta.Server{}
	for _, id := range peers {
		cfg := meta.DefaultConfig(id)
		cfg.Peers = peers
		srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: clock, Net: net, Store: sim.NewMetaStore(), Rand: rng, Log: log}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		srv.Start()
		srvs[id] = srv
	}
	caller := net.NewCaller("client", 10*time.Second)
	leader := func(alive ...iface.NodeID) iface.NodeID {
		t.Helper()
		var l iface.NodeID
		for _, id := range alive {
			if srvs[id].Raft().Ready {
				if l != "" {
					t.Fatalf("two ready leaders: %s and %s", l, id)
				}
				l = id
			}
		}
		if l == "" {
			t.Fatal("no ready leader")
		}
		return l
	}
	clock.Advance(5 * time.Second)
	l := leader(peers...)

	for _, id := range peers {
		r := caller.Do(context.Background(), []iface.Call{{To: id, Kind: wire.KindList, Body: wire.Marshal(&chunkdv1.ListRequest{Prefix: "/"})}})
		switch {
		case id == l && r[0].Err != nil:
			t.Fatalf("leader %s: %v", id, r[0].Err)
		case id != l && (iface.CodeOf(r[0].Err) != iface.CodeNotLeader || r[0].Err.(*iface.Error).Msg != string(l)):
			t.Fatalf("follower %s: %v, want NotLeader naming %s", id, r[0].Err, l)
		}
	}

	// One epoch per EpochEvery, not one per peer.
	clock.Advance(35 * time.Second)
	for _, id := range peers {
		if e := srvs[id].State().Epoch(); e != 1 {
			t.Fatalf("%s at epoch %d after one epoch interval, want 1", id, e)
		}
	}

	srvs[l].Stop()
	net.Crash(l)
	var rest []iface.NodeID
	for _, id := range peers {
		if id != l {
			rest = append(rest, id)
		}
	}
	clock.Advance(10 * time.Second)
	nl := leader(rest...)
	if nl == l {
		t.Fatal("the dead peer still leads")
	}
	clock.Advance(35 * time.Second)
	for _, id := range rest {
		if e := srvs[id].State().Epoch(); e != 2 {
			t.Fatalf("%s at epoch %d after failover and another interval, want 2", id, e)
		}
	}
	if string(srvs[rest[0]].State().Snapshot()) != string(srvs[rest[1]].State().Snapshot()) {
		t.Fatal("surviving peers' states differ")
	}
}

// The leader's heartbeat acks announce its term: after a few seconds every
// node has it on its disk, so a deposed leader's commands are refused.
func TestLeaderAnnouncesItsTermToNodes(t *testing.T) {
	e := newEnv(t, 3)
	want := e.srv.Raft().Term
	if want == 0 {
		t.Fatal("the leader has no term")
	}
	for i, d := range e.disks {
		if got, _ := d.Term(context.Background()); got != want {
			t.Fatalf("node %d recorded term %d, leader is at %d", i+1, got, want)
		}
	}
}

// A retried Begin (a lost response, a leader change) opens one upload.
func TestBeginRetryReturnsTheSameUpload(t *testing.T) {
	e := newEnv(t, 3)
	begin := func(id string) *chunkdv1.BeginUploadResponse {
		t.Helper()
		body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/f", Size: 12, RequestId: []byte(id)}))
		if err != nil {
			t.Fatal(err)
		}
		var resp chunkdv1.BeginUploadResponse
		if err := wire.Decode(body, &resp); err != nil {
			t.Fatal(err)
		}
		return &resp
	}
	first, again := begin("req-1"), begin("req-1")
	if first.GetUploadId() != again.GetUploadId() || e.srv.State().PendingUploads() != 1 {
		t.Fatalf("uploads %d and %d, %d pending; want one upload", first.GetUploadId(), again.GetUploadId(), e.srv.State().PendingUploads())
	}
	if len(first.GetPlacement()) != len(again.GetPlacement()) || len(first.GetPlacement()) == 0 {
		t.Fatalf("placements differ in length: %d vs %d", len(first.GetPlacement()), len(again.GetPlacement()))
	}
	for i := range first.GetPlacement() {
		for j, r := range first.GetPlacement()[i].GetReplicas() {
			if again.GetPlacement()[i].GetReplicas()[j].GetNode() != r.GetNode() {
				t.Fatalf("chunk %d replica %d moved between attempts", i, j)
			}
		}
	}
	if other := begin("req-2"); other.GetUploadId() == first.GetUploadId() {
		t.Fatal("a different request shared the upload")
	}
	// Without a request ID nothing is deduplicated.
	a, _ := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/g", Size: 4}))
	b, _ := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/h", Size: 4}))
	if string(a) == string(b) {
		t.Fatal("two anonymous begins answered identically")
	}
}
