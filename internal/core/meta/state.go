// Package meta holds the metadata state machine and the metadata server.
//
// State is the durable part: files, versions, chunk refcounts and pending
// uploads. It changes only by applying logged Ops, and applying an op is a
// pure function of the state before it, so replaying the log (and, from
// Phase 5, Raft) rebuilds identical state. Chunk locations are not here: they
// live in Cluster and are rebuilt from block reports.
package meta

import (
	"bytes"
	"cmp"
	"maps"
	"path"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// Version is one immutable version of a file.
type Version struct {
	V         uint64
	UploadID  uint64 // 0 for tombstones
	Size      int64
	SHA256    [32]byte
	Chunks    []iface.ChunkID
	ChunkSize int
	Tombstone bool
	// Retired: superseded at epoch RetiredAt. It still holds its chunks, so
	// undelete can restore it, until hard delete drops it.
	Retired   bool
	RetiredAt uint64
	// RestoredFrom is the version an undelete copied; 0 otherwise.
	RestoredFrom uint64
	// Redundancy: under EC, Chunks holds stripe logical IDs (ADR-0022).
	Redundancy chunkdv1.Redundancy
}

// File is every version of a path, oldest first. The newest entry is never
// dropped, even if it is a tombstone, so version numbers are never reused.
type File struct {
	Path     string
	Versions []Version
	// Retain overrides how many GC epochs a retired version is kept; 0 uses
	// the cluster's (ADR-0026).
	Retain uint32
}

// Upload is a pending version: invisible to Stat and List until committed.
type Upload struct {
	ID        uint64
	Path      string
	Expected  uint64
	Size      int64
	ChunkSize int
	Placement [][]iface.NodeID
	// Claims: every chunk must be claimed before commit (ADR-0015). False
	// only for uploads begun before claims existed.
	Claims  bool
	Claimed map[int]iface.ChunkID
	// Redundancy: under EC, Placement lists 6 nodes per chunk (shard i on
	// node i), Claimed holds stripe logical IDs and Shards their shard IDs.
	Redundancy chunkdv1.Redundancy
	Shards     map[int][]iface.ChunkID
	// LWW commits over whatever version is live instead of comparing.
	LWW bool
	// Touched is the epoch of the begin or the latest claim. The upload
	// expires LeaseEpochs after it (AdvanceEpoch).
	Touched uint64
	// RequestID is the client's key for its Begin: a repeat returns this upload.
	RequestID string
	// SHA256 is the file hash a resumable upload declared at Begin; Commit
	// must carry it (ADR-0024). Nil: not declared.
	SHA256 []byte
}

// GCTarget is a GC delete the log has authorized and no node has answered:
// delete Chunk on Node unless it wrote the chunk after the fence.
type GCTarget struct {
	Chunk            iface.ChunkID
	Node             iface.NodeID
	Incarnation, Seq uint64
}

// ChunkInfo is the durable record of a chunk. Refcount counts references
// from committed versions; hard delete decrements it.
type ChunkInfo struct {
	Refcount uint64
	Size     int64
	// Shards are a stripe's 6 shard block IDs, data first; nil for a
	// replicated chunk. Size is then the encoded chunk's size.
	Shards []iface.ChunkID
}

// ShardRef locates a shard block within its stripe.
type ShardRef struct {
	Stripe iface.ChunkID
	Index  int
	// Size is the stored block's size: header plus payload.
	Size int64
}

// stripe is a stripe known to a record or a pending claim. holds counts
// them: the stripe's shards stay marked while it is above zero.
type stripe struct {
	shards []iface.ChunkID
	size   int64
	holds  int
}

// State is the durable metadata state.
type State struct {
	files        map[string]*File
	chunks       map[iface.ChunkID]*ChunkInfo
	uploads      map[uint64]*Upload
	lastUploadID uint64
	// claimed counts pending-upload claims per chunk: the GC mark set beyond
	// refcounts. Derived from uploads, so not stored separately.
	claimed map[iface.ChunkID]int
	// committed indexes versions by the upload that created them; rebuilt
	// from files on restore, so it is not stored separately.
	committed map[uint64]Committed
	// epoch is logical GC time, advanced only by logged AdvanceEpoch ops, so
	// retention never depends on any machine's clock.
	epoch uint64
	// requests maps a Begin's request ID to its pending upload; derived from
	// uploads, so not stored separately.
	requests map[string]uint64
	// gcPending are the deletes a committed GCIntent authorized and no GCDone
	// has cleared. Replicated, so any leader can resend them with their
	// original fence.
	gcPending map[iface.ChunkID]map[iface.NodeID]GCTarget
	// trimPending are the surplus copies a committed TrimIntent authorized
	// deleting and no TrimDone has cleared: at most one per chunk, so leaders
	// never trim two copies of a chunk on views that missed each other.
	trimPending map[iface.ChunkID]iface.NodeID
	// nodeAdmin is each storage node's operator-set state; absent means active
	// (ADR-0021). Logged, so a new leader re-derives every drain from it.
	nodeAdmin map[iface.NodeID]chunkdv1.NodeAdmin
	// stripes and shardOf index every stripe a record or pending claim names,
	// and each of its shards; derived from chunks and uploads.
	stripes map[iface.ChunkID]*stripe
	shardOf map[iface.ChunkID]ShardRef
	// nsBytes is each namespace's quota usage: live versions plus pending
	// reservations. Derived, so rebuilt on restore, not stored.
	nsBytes map[string]int64
}

// Committed locates the version an upload produced.
type Committed struct {
	Path    string
	Version uint64
}

// Result reports what an applied op produced.
type Result struct {
	UploadID uint64
	Version  uint64
	// Dropped counts versions hard-deleted by an AdvanceEpoch, Expired the
	// uploads whose lease ran out.
	Dropped, Expired int
	// Intents counts the GC targets a GCIntent added; the rest were marked
	// or already pending.
	Intents int
	// Trims counts the targets a TrimIntent added; the rest had a trim pending.
	Trims int
}

// New returns empty state.
func New() *State {
	return &State{files: map[string]*File{}, chunks: map[iface.ChunkID]*ChunkInfo{}, uploads: map[uint64]*Upload{}, committed: map[uint64]Committed{}, claimed: map[iface.ChunkID]int{},
		requests: map[string]uint64{}, gcPending: map[iface.ChunkID]map[iface.NodeID]GCTarget{}, trimPending: map[iface.ChunkID]iface.NodeID{}, nodeAdmin: map[iface.NodeID]chunkdv1.NodeAdmin{},
		stripes: map[iface.ChunkID]*stripe{}, shardOf: map[iface.ChunkID]ShardRef{}, nsBytes: map[string]int64{}}
}

// ValidPath reports whether p is an absolute, clean path to a file.
func ValidPath(p string) bool {
	return strings.HasPrefix(p, "/") && p != "/" && path.Clean(p) == p && !strings.ContainsRune(p, 0)
}

// Namespace is the first segment of a path: what an API key owns and what a
// quota limits (ADR-0025).
func Namespace(p string) string {
	rest := strings.TrimPrefix(p, "/")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i]
	}
	return rest
}

// NamespaceBytes is the logical size of the live versions under ns plus the
// size every pending upload there has reserved. Retired versions kept for
// undelete are not counted. It reads a counter that every op moving a live
// version or a reservation keeps; recountNamespaces is the oracle (ADR-0028).
// SIMPLIFIED: logical bytes, not stored bytes; S3-compatible stores keep a
// running usage counter per bucket. An overwrite counts the old and the new
// version until the commit.
func (s *State) NamespaceBytes(ns string) int64 { return s.nsBytes[ns] }

// addNS moves ns's usage by d; a namespace at zero is forgotten.
func (s *State) addNS(ns string, d int64) {
	if n := s.nsBytes[ns] + d; n != 0 {
		s.nsBytes[ns] = n
	} else {
		delete(s.nsBytes, ns)
	}
}

// recountNamespaces recomputes every namespace's usage from the files and
// uploads: what the counter must equal.
func (s *State) recountNamespaces() map[string]int64 {
	out := map[string]int64{}
	add := func(ns string, n int64) {
		if out[ns] += n; out[ns] == 0 {
			delete(out, ns)
		}
	}
	for p, f := range s.files {
		if len(f.Versions) == 0 {
			continue
		}
		if v := f.Versions[len(f.Versions)-1]; !v.Tombstone {
			add(Namespace(p), v.Size)
		}
	}
	for _, u := range s.uploads {
		add(Namespace(u.Path), u.Size)
	}
	return out
}

// overQuota refuses growing ns by add bytes past limit (0: unlimited).
func (s *State) overQuota(path string, add, limit int64) error {
	if limit <= 0 {
		return nil
	}
	if used := s.NamespaceBytes(Namespace(path)); used+add > limit {
		return iface.Errorf(iface.CodeQuota, "namespace %q holds %d of %d bytes, %d more would pass its quota", Namespace(path), used, limit, add)
	}
	return nil
}

func (s *State) lastVersion(p string) uint64 {
	f := s.files[p]
	if f == nil || len(f.Versions) == 0 {
		return 0
	}
	return f.Versions[len(f.Versions)-1].V
}

// liveVersion is the committed, non-deleted version of p, or 0.
func (s *State) liveVersion(p string) uint64 {
	f := s.files[p]
	if f == nil || len(f.Versions) == 0 || f.Versions[len(f.Versions)-1].Tombstone {
		return 0
	}
	return f.Versions[len(f.Versions)-1].V
}

// Validate reports whether op would apply cleanly. Apply runs it again, so
// a logged op that stopped being valid is rejected, not applied.
func (s *State) Validate(op *chunkdv1.Op) error {
	switch o := op.GetOp().(type) {
	case *chunkdv1.Op_Begin:
		b := o.Begin
		// A repeat of a pending Begin is answered with that upload, whatever
		// the state of the path has become since: nothing is applied.
		if _, dup := s.requests[string(b.GetRequestId())]; dup && len(b.GetRequestId()) > 0 {
			return nil
		}
		if !ValidPath(b.GetPath()) {
			return iface.Errorf(iface.CodeInvalid, "bad path %q", b.GetPath())
		}
		if b.GetSize() < 0 || b.GetChunkSize() <= 0 {
			return iface.Errorf(iface.CodeInvalid, "bad size %d or chunk size %d", b.GetSize(), b.GetChunkSize())
		}
		if n := len(b.GetSha256()); n != 0 && n != 32 {
			return iface.Errorf(iface.CodeInvalid, "declared file sha256 of %d bytes", n)
		}
		if want := chunk.Count(b.GetSize(), int(b.GetChunkSize())); len(b.GetPlacement()) != want {
			return iface.Errorf(iface.CodeInvalid, "placement for %d chunks, want %d", len(b.GetPlacement()), want)
		}
		for i, r := range b.GetPlacement() {
			if len(r.GetNodes()) == 0 {
				return iface.Errorf(iface.CodeInvalid, "chunk %d has no replicas", i)
			}
		}
		switch b.GetRedundancy() {
		case chunkdv1.Redundancy_REDUNDANCY_REPLICATED:
		case chunkdv1.Redundancy_REDUNDANCY_EC_4_2:
			// Claims carry the shard IDs, so an EC upload without them could
			// commit stripes no record describes.
			if !b.GetClaims() {
				return iface.Errorf(iface.CodeInvalid, "an erasure-coded upload must claim its chunks")
			}
			for i, r := range b.GetPlacement() {
				if n := r.GetNodes(); len(n) != ec.TotalShards || len(slices.Compact(slices.Sorted(slices.Values(n)))) != ec.TotalShards {
					return iface.Errorf(iface.CodeInvalid, "chunk %d: an erasure-coded chunk needs %d distinct nodes, got %v", i, ec.TotalShards, n)
				}
			}
		default:
			return iface.Errorf(iface.CodeInvalid, "unknown redundancy %d", b.GetRedundancy())
		}
		if live := s.liveVersion(b.GetPath()); live != b.GetExpectedVersion() && !b.GetLastWriterWins() {
			return iface.Errorf(iface.CodeConflict, "%s is at version %d, expected %d", b.GetPath(), live, b.GetExpectedVersion())
		}
		if err := s.overQuota(b.GetPath(), b.GetSize(), b.GetQuota()); err != nil {
			return err
		}
	case *chunkdv1.Op_Commit:
		c := o.Commit
		u := s.uploads[c.GetUploadId()]
		if u == nil {
			return iface.Errorf(iface.CodeNotFound, "upload %d", c.GetUploadId())
		}
		if len(c.GetChunkIds()) != len(u.Placement) {
			return iface.Errorf(iface.CodeInvalid, "%d chunk ids, upload has %d chunks", len(c.GetChunkIds()), len(u.Placement))
		}
		for _, id := range c.GetChunkIds() {
			if len(id) != len(iface.ChunkID{}) {
				return iface.Errorf(iface.CodeInvalid, "chunk id of %d bytes", len(id))
			}
		}
		if len(c.GetSha256()) != 32 {
			return iface.Errorf(iface.CodeInvalid, "file sha256 of %d bytes", len(c.GetSha256()))
		}
		if len(u.SHA256) > 0 && !bytes.Equal(u.SHA256, c.GetSha256()) {
			return iface.Errorf(iface.CodeInvalid, "file sha256 %x differs from the %x upload %d declared", c.GetSha256(), u.SHA256, u.ID)
		}
		if u.Claims {
			for i, raw := range c.GetChunkIds() {
				if id, ok := u.Claimed[i]; !ok || !bytes.Equal(id[:], raw) {
					return iface.Errorf(iface.CodeInvalid, "chunk %d was not claimed by upload %d", i, u.ID)
				}
			}
		}
		// Rechecked here: another writer may have committed since Begin.
		if live := s.liveVersion(u.Path); live != u.Expected && !u.LWW {
			return iface.Errorf(iface.CodeConflict, "%s moved to version %d since upload began at %d", u.Path, live, u.Expected)
		}
	case *chunkdv1.Op_Claim:
		c := o.Claim
		u := s.uploads[c.GetUploadId()]
		if u == nil {
			return iface.Errorf(iface.CodeNotFound, "upload %d", c.GetUploadId())
		}
		batch := map[iface.ChunkID][]iface.ChunkID{}
		for _, cl := range c.GetClaims() {
			i := int(cl.GetIndex())
			if i < 0 || i >= len(u.Placement) {
				return iface.Errorf(iface.CodeInvalid, "claim for chunk %d, upload has %d chunks", i, len(u.Placement))
			}
			if len(cl.GetId()) != len(iface.ChunkID{}) {
				return iface.Errorf(iface.CodeInvalid, "chunk id of %d bytes", len(cl.GetId()))
			}
			// Re-claiming the same chunk is a retry; a different one is a bug.
			if id, ok := u.Claimed[i]; ok && !bytes.Equal(id[:], cl.GetId()) {
				return iface.Errorf(iface.CodeInvalid, "chunk %d of upload %d already claimed as %s", i, u.ID, id)
			}
			if err := s.validShards(u, cl, batch); err != nil {
				return err
			}
		}
	case *chunkdv1.Op_Abort:
		if s.uploads[o.Abort.GetUploadId()] == nil {
			return iface.Errorf(iface.CodeNotFound, "upload %d", o.Abort.GetUploadId())
		}
	case *chunkdv1.Op_Delete:
		d := o.Delete
		live := s.liveVersion(d.GetPath())
		if live == 0 {
			return iface.Errorf(iface.CodeNotFound, "%s", d.GetPath())
		}
		if d.GetExpectedVersion() != 0 && d.GetExpectedVersion() != live {
			return iface.Errorf(iface.CodeConflict, "%s is at version %d, expected %d", d.GetPath(), live, d.GetExpectedVersion())
		}
	case *chunkdv1.Op_Undelete:
		u := o.Undelete
		if _, err := s.StatVersion(u.GetPath(), u.GetVersion()); err != nil || u.GetVersion() == 0 {
			return iface.Errorf(iface.CodeNotFound, "%s has no retained version %d to restore", u.GetPath(), u.GetVersion())
		}
		if live := s.liveVersion(u.GetPath()); live != u.GetExpectedVersion() {
			return iface.Errorf(iface.CodeConflict, "%s is at version %d, expected %d", u.GetPath(), live, u.GetExpectedVersion())
		}
		src, _ := s.StatVersion(u.GetPath(), u.GetVersion())
		if err := s.overQuota(u.GetPath(), src.Size, u.GetQuota()); err != nil {
			return err
		}
	case *chunkdv1.Op_AdvanceEpoch:
		if o.AdvanceEpoch.GetRetainEpochs() == 0 {
			return iface.Errorf(iface.CodeInvalid, "retain_epochs must be at least 1")
		}
	case *chunkdv1.Op_GcIntent:
		return validTargets(o.GcIntent.GetTargets())
	case *chunkdv1.Op_GcDone:
		return validTargets(o.GcDone.GetTargets())
	case *chunkdv1.Op_TrimIntent:
		return validTrims(o.TrimIntent.GetTargets())
	case *chunkdv1.Op_TrimDone:
		return validTrims(o.TrimDone.GetTargets())
	case *chunkdv1.Op_SetRetention:
		if s.files[o.SetRetention.GetPath()] == nil {
			return iface.Errorf(iface.CodeNotFound, "%s", o.SetRetention.GetPath())
		}
	case *chunkdv1.Op_NodeAdmin:
		a := o.NodeAdmin
		if a.GetNode() == "" {
			return iface.Errorf(iface.CodeInvalid, "node admin with no node")
		}
		cur, to := s.NodeAdmin(iface.NodeID(a.GetNode())), a.GetState()
		switch {
		case to == cur:
		case to == chunkdv1.NodeAdmin_NODE_ADMIN_DECOMMISSIONED && cur != chunkdv1.NodeAdmin_NODE_ADMIN_DRAINING:
			return iface.Errorf(iface.CodeConflict, "%s is %s: drain it before decommissioning", a.GetNode(), AdminName(cur))
		case to == chunkdv1.NodeAdmin_NODE_ADMIN_DRAINING && cur == chunkdv1.NodeAdmin_NODE_ADMIN_DECOMMISSIONED:
			return iface.Errorf(iface.CodeConflict, "%s is decommissioned: undrain it first", a.GetNode())
		case AdminName(to) == "":
			return iface.Errorf(iface.CodeInvalid, "unknown node admin state %d", to)
		}
	default:
		return iface.Errorf(iface.CodeInvalid, "empty op")
	}
	return nil
}

func validTargets(ts []*chunkdv1.GCTarget) error {
	if len(ts) == 0 {
		return iface.Errorf(iface.CodeInvalid, "no targets")
	}
	for _, t := range ts {
		if len(t.GetChunkId()) != len(iface.ChunkID{}) || t.GetNode() == "" {
			return iface.Errorf(iface.CodeInvalid, "bad gc target: chunk id of %d bytes, node %q", len(t.GetChunkId()), t.GetNode())
		}
	}
	return nil
}

func validTrims(ts []*chunkdv1.TrimTarget) error {
	if len(ts) == 0 {
		return iface.Errorf(iface.CodeInvalid, "no targets")
	}
	for _, t := range ts {
		if len(t.GetChunkId()) != len(iface.ChunkID{}) || t.GetNode() == "" {
			return iface.Errorf(iface.CodeInvalid, "bad trim target: chunk id of %d bytes, node %q", len(t.GetChunkId()), t.GetNode())
		}
	}
	return nil
}

// validShards checks a claim's shard list against its upload's policy and
// every stripe already known, including earlier claims in the same op
// (batch): one logical ID names one set of shards, and one shard one slot.
func (s *State) validShards(u *Upload, cl *chunkdv1.ChunkClaim, batch map[iface.ChunkID][]iface.ChunkID) error {
	var id iface.ChunkID
	copy(id[:], cl.GetId())
	rec := s.chunks[id]
	if u.Redundancy != chunkdv1.Redundancy_REDUNDANCY_EC_4_2 {
		if len(cl.GetShards()) != 0 || s.stripes[id] != nil {
			return iface.Errorf(iface.CodeInvalid, "replicated upload %d claims stripe %s", u.ID, id)
		}
		return nil
	}
	if rec != nil && rec.Shards == nil {
		return iface.Errorf(iface.CodeInvalid, "erasure-coded upload %d claims replicated chunk %s", u.ID, id)
	}
	shards, err := shardIDs(cl.GetShards())
	if err != nil {
		return err
	}
	known := batch[id]
	if st := s.stripes[id]; st != nil {
		known = st.shards
	}
	if known != nil && !slices.Equal(known, shards) {
		return iface.Errorf(iface.CodeInvalid, "stripe %s claimed with shards that differ from its record", id)
	}
	for j, sh := range shards {
		if ref, ok := s.shardOf[sh]; ok && (ref.Stripe != id || ref.Index != j) {
			return iface.Errorf(iface.CodeInvalid, "shard %s is slot %d of stripe %s, claimed as slot %d of %s", sh, ref.Index, ref.Stripe, j, id)
		}
	}
	batch[id] = shards
	return nil
}

func rawIDs(ids []iface.ChunkID) [][]byte {
	var out [][]byte
	for _, id := range ids {
		out = append(out, slices.Clone(id[:]))
	}
	return out
}

func shardIDs(raw [][]byte) ([]iface.ChunkID, error) {
	if len(raw) != ec.TotalShards {
		return nil, iface.Errorf(iface.CodeInvalid, "%d shard ids, want %d", len(raw), ec.TotalShards)
	}
	out := make([]iface.ChunkID, len(raw))
	for j, r := range raw {
		if len(r) != len(iface.ChunkID{}) {
			return nil, iface.Errorf(iface.CodeInvalid, "shard id of %d bytes", len(r))
		}
		copy(out[j][:], r)
		if slices.Contains(out[:j], out[j]) {
			return nil, iface.Errorf(iface.CodeInvalid, "shard %d repeats an earlier shard id", j)
		}
	}
	return out, nil
}

// hold adds a reference to stripe id, indexing it and its shards on the
// first. size is the encoded chunk's size.
func (s *State) hold(id iface.ChunkID, shards []iface.ChunkID, size int64) {
	st := s.stripes[id]
	if st == nil {
		st = &stripe{shards: shards, size: size}
		s.stripes[id] = st
		for j, sh := range shards {
			s.shardOf[sh] = ShardRef{Stripe: id, Index: j, Size: ec.BlockSize(size)}
		}
	}
	st.holds++
}

// release drops a reference to stripe id. At zero its shards are unmarked.
func (s *State) release(id iface.ChunkID) {
	st := s.stripes[id]
	if st.holds--; st.holds > 0 {
		return
	}
	for _, sh := range st.shards {
		delete(s.shardOf, sh)
	}
	delete(s.stripes, id)
}

// ShardOf reports whether id is a shard of a recorded or claimed stripe.
func (s *State) ShardOf(id iface.ChunkID) (ShardRef, bool) {
	r, ok := s.shardOf[id]
	return r, ok
}

// Stripe returns the shard IDs of a recorded or claimed stripe, data first.
func (s *State) Stripe(id iface.ChunkID) ([]iface.ChunkID, bool) {
	st := s.stripes[id]
	if st == nil {
		return nil, false
	}
	return slices.Clone(st.shards), true
}

// BlockInfo is what the metadata knows about one stored block.
type BlockInfo struct {
	Size int64
	// Shard is set for a shard of a stripe; Ref then locates it.
	Shard bool
	Ref   ShardRef
}

// Block describes a block a node may store: a replicated chunk with a
// record, or a shard of a recorded or claimed stripe. A block that is both
// (a file chunk whose bytes equal a shard's) is reported as the chunk, the
// stronger target.
func (s *State) Block(id iface.ChunkID) (BlockInfo, bool) {
	if c := s.chunks[id]; c != nil && c.Shards == nil {
		return BlockInfo{Size: c.Size}, true
	}
	if r, ok := s.shardOf[id]; ok {
		return BlockInfo{Size: r.Size, Shard: true, Ref: r}, true
	}
	return BlockInfo{}, false
}

// Apply applies op, or rejects it with Validate's error and leaves the state
// unchanged. The leader validates before proposing, but ops proposed
// together can invalidate each other (two commits on one expected version):
// the loser is logged and rejected here, identically on every replica.
func (s *State) Apply(op *chunkdv1.Op) (Result, error) {
	if err := s.Validate(op); err != nil {
		return Result{}, err
	}
	return s.apply(op), nil
}

func (s *State) apply(op *chunkdv1.Op) Result {
	switch o := op.GetOp().(type) {
	case *chunkdv1.Op_Begin:
		b := o.Begin
		if id, dup := s.requests[string(b.GetRequestId())]; dup && len(b.GetRequestId()) > 0 {
			return Result{UploadID: id}
		}
		s.lastUploadID++
		u := &Upload{ID: s.lastUploadID, Path: b.GetPath(), Expected: b.GetExpectedVersion(), Size: b.GetSize(), ChunkSize: int(b.GetChunkSize()), Claims: b.GetClaims(), Claimed: map[int]iface.ChunkID{}, LWW: b.GetLastWriterWins(), Touched: s.epoch,
			Redundancy: b.GetRedundancy(), Shards: map[int][]iface.ChunkID{},
			RequestID: string(b.GetRequestId()), SHA256: bytes.Clone(b.GetSha256())}
		if u.RequestID != "" {
			s.requests[u.RequestID] = u.ID
		}
		for _, r := range b.GetPlacement() {
			nodes := make([]iface.NodeID, len(r.GetNodes()))
			for i, n := range r.GetNodes() {
				nodes[i] = iface.NodeID(n)
			}
			u.Placement = append(u.Placement, nodes)
		}
		s.uploads[u.ID] = u
		s.addNS(Namespace(u.Path), u.Size)
		return Result{UploadID: u.ID}
	case *chunkdv1.Op_Commit:
		c := o.Commit
		u := s.uploads[c.GetUploadId()]
		v := Version{V: s.lastVersion(u.Path) + 1, UploadID: u.ID, Size: u.Size, ChunkSize: u.ChunkSize, Redundancy: u.Redundancy}
		copy(v.SHA256[:], c.GetSha256())
		for i, raw := range c.GetChunkIds() {
			var id iface.ChunkID
			copy(id[:], raw)
			v.Chunks = append(v.Chunks, id)
			ci := s.chunks[id]
			if ci == nil {
				ci = &ChunkInfo{Size: chunk.SizeOf(u.Size, u.ChunkSize, i), Shards: u.Shards[i]}
				s.chunks[id] = ci
				if ci.Shards != nil {
					s.hold(id, ci.Shards, ci.Size)
				}
			}
			ci.Refcount++
		}
		s.appendVersion(u.Path, v)
		s.committed[u.ID] = Committed{Path: u.Path, Version: v.V}
		s.dropUpload(u)
		return Result{UploadID: u.ID, Version: v.V}
	case *chunkdv1.Op_Claim:
		u := s.uploads[o.Claim.GetUploadId()]
		u.Touched = s.epoch
		for _, cl := range o.Claim.GetClaims() {
			s.claim(u, int(cl.GetIndex()), cl.GetId(), cl.GetShards())
		}
		return Result{UploadID: u.ID}
	case *chunkdv1.Op_Abort:
		s.dropUpload(s.uploads[o.Abort.GetUploadId()])
		return Result{UploadID: o.Abort.GetUploadId()}
	case *chunkdv1.Op_Delete:
		p := o.Delete.GetPath()
		v := Version{V: s.lastVersion(p) + 1, Tombstone: true}
		s.appendVersion(p, v)
		return Result{Version: v.V}
	case *chunkdv1.Op_Undelete:
		u := o.Undelete
		src, _ := s.StatVersion(u.GetPath(), u.GetVersion())
		v := Version{V: s.lastVersion(u.GetPath()) + 1, Size: src.Size, SHA256: src.SHA256, Chunks: slices.Clone(src.Chunks), ChunkSize: src.ChunkSize, RestoredFrom: src.V, Redundancy: src.Redundancy}
		// src is retained, so it still holds a reference to every chunk: the
		// records exist and none can have been collected.
		for _, id := range v.Chunks {
			s.chunks[id].Refcount++
		}
		s.appendVersion(u.GetPath(), v)
		return Result{Version: v.V}
	case *chunkdv1.Op_AdvanceEpoch:
		s.epoch++
		return Result{Dropped: s.hardDelete(uint64(o.AdvanceEpoch.GetRetainEpochs())), Expired: s.expire(uint64(o.AdvanceEpoch.GetLeaseEpochs()))}
	case *chunkdv1.Op_GcIntent:
		var added int
		for _, t := range o.GcIntent.GetTargets() {
			var id iface.ChunkID
			copy(id[:], t.GetChunkId())
			n := iface.NodeID(t.GetNode())
			// Marked here, in log order: a claim applied before this intent
			// protects the chunk, one applied after is protected by the fence.
			if _, pending := s.gcPending[id][n]; pending || s.Marked(id) {
				continue
			}
			if s.gcPending[id] == nil {
				s.gcPending[id] = map[iface.NodeID]GCTarget{}
			}
			s.gcPending[id][n] = GCTarget{Chunk: id, Node: n, Incarnation: t.GetFenceIncarnation(), Seq: t.GetFenceSeq()}
			added++
		}
		return Result{Intents: added}
	case *chunkdv1.Op_GcDone:
		for _, t := range o.GcDone.GetTargets() {
			var id iface.ChunkID
			copy(id[:], t.GetChunkId())
			delete(s.gcPending[id], iface.NodeID(t.GetNode()))
			if len(s.gcPending[id]) == 0 {
				delete(s.gcPending, id)
			}
		}
		return Result{}
	case *chunkdv1.Op_TrimIntent:
		var added int
		for _, t := range o.TrimIntent.GetTargets() {
			var id iface.ChunkID
			copy(id[:], t.GetChunkId())
			// In log order: an intent from another leader (or an earlier one of
			// this leader's) already holds the chunk.
			if _, busy := s.trimPending[id]; busy {
				continue
			}
			s.trimPending[id] = iface.NodeID(t.GetNode())
			added++
		}
		return Result{Trims: added}
	case *chunkdv1.Op_TrimDone:
		for _, t := range o.TrimDone.GetTargets() {
			var id iface.ChunkID
			copy(id[:], t.GetChunkId())
			if s.trimPending[id] == iface.NodeID(t.GetNode()) {
				delete(s.trimPending, id)
			}
		}
		return Result{}
	case *chunkdv1.Op_SetRetention:
		s.files[o.SetRetention.GetPath()].Retain = o.SetRetention.GetRetainEpochs()
		return Result{}
	case *chunkdv1.Op_NodeAdmin:
		n, to := iface.NodeID(o.NodeAdmin.GetNode()), o.NodeAdmin.GetState()
		if to == chunkdv1.NodeAdmin_NODE_ADMIN_ACTIVE {
			delete(s.nodeAdmin, n)
		} else {
			s.nodeAdmin[n] = to
		}
		return Result{}
	}
	panic("unreachable")
}

// NodeAdmin returns n's operator-set state; active if never set.
func (s *State) NodeAdmin(n iface.NodeID) chunkdv1.NodeAdmin { return s.nodeAdmin[n] }

// Leaving reports whether n is draining or decommissioned: it takes no new
// data and its copies do not count toward RF.
func (s *State) Leaving(n iface.NodeID) bool {
	return s.nodeAdmin[n] != chunkdv1.NodeAdmin_NODE_ADMIN_ACTIVE
}

// AdminName is the lower-case name of an admin state, or "" if unknown.
func AdminName(a chunkdv1.NodeAdmin) string {
	switch a {
	case chunkdv1.NodeAdmin_NODE_ADMIN_ACTIVE:
		return "active"
	case chunkdv1.NodeAdmin_NODE_ADMIN_DRAINING:
		return "draining"
	case chunkdv1.NodeAdmin_NODE_ADMIN_DECOMMISSIONED:
		return "decommissioned"
	}
	return ""
}

// TrimPending returns the node whose copy of id an authorized, unanswered
// trim is removing.
func (s *State) TrimPending(id iface.ChunkID) (iface.NodeID, bool) {
	n, ok := s.trimPending[id]
	return n, ok
}

// TrimTarget is a trim the log has authorized: delete Chunk on Node.
type TrimTarget struct {
	Chunk iface.ChunkID
	Node  iface.NodeID
}

// TrimPendingAll returns every pending trim, ordered by chunk.
func (s *State) TrimPendingAll() []TrimTarget {
	var out []TrimTarget
	for _, id := range slices.SortedFunc(maps.Keys(s.trimPending), func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) }) {
		out = append(out, TrimTarget{Chunk: id, Node: s.trimPending[id]})
	}
	return out
}

// UploadByRequest returns the pending upload a Begin with this request ID
// opened.
func (s *State) UploadByRequest(rid []byte) (uint64, bool) {
	if len(rid) == 0 {
		return 0, false
	}
	id, ok := s.requests[string(rid)]
	return id, ok
}

// GCPending returns the authorized, unanswered GC delete of id on n.
func (s *State) GCPending(id iface.ChunkID, n iface.NodeID) (GCTarget, bool) {
	t, ok := s.gcPending[id][n]
	return t, ok
}

// GCPendingAll returns every pending GC delete, ordered by chunk then node.
func (s *State) GCPendingAll() []GCTarget {
	var out []GCTarget
	for _, id := range slices.SortedFunc(maps.Keys(s.gcPending), func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) }) {
		for _, n := range slices.Sorted(maps.Keys(s.gcPending[id])) {
			out = append(out, s.gcPending[id][n])
		}
	}
	return out
}

func (s *State) claim(u *Upload, i int, raw []byte, rawShards [][]byte) {
	if _, ok := u.Claimed[i]; ok {
		return
	}
	var id iface.ChunkID
	copy(id[:], raw)
	u.Claimed[i] = id
	s.claimed[id]++
	if len(rawShards) > 0 {
		shards, _ := shardIDs(rawShards) // validated
		u.Shards[i] = shards
		s.hold(id, shards, chunk.SizeOf(u.Size, u.ChunkSize, i))
	}
}

// dropUpload ends a pending upload and releases its claims. On commit the
// refcounts taken just before keep the chunks marked.
func (s *State) dropUpload(u *Upload) {
	for i, id := range u.Claimed {
		if s.claimed[id]--; s.claimed[id] == 0 {
			delete(s.claimed, id)
		}
		if u.Shards[i] != nil {
			s.release(id)
		}
	}
	if u.RequestID != "" {
		delete(s.requests, u.RequestID)
	}
	delete(s.uploads, u.ID)
	s.addNS(Namespace(u.Path), -u.Size)
}

// Claimed reports whether a pending upload has claimed chunk id.
func (s *State) Claimed(id iface.ChunkID) bool { return s.claimed[id] > 0 }

// Dedup returns the bytes committed versions reference (each reference
// counted) and the bytes of distinct referenced chunks: what one copy of
// each costs. The difference is what dedup saves per replica.
func (s *State) Dedup() (referenced, unique int64) {
	for _, c := range s.chunks {
		if c.Refcount > 0 {
			referenced += c.Size * int64(c.Refcount)
			unique += c.Size
		}
	}
	return referenced, unique
}

// appendVersion adds v as the newest version of p and retires the one it
// supersedes (data or tombstone) at the current epoch.
func (s *State) appendVersion(p string, v Version) {
	f := s.files[p]
	if f == nil {
		f = &File{Path: p}
		s.files[p] = f
	}
	if n := len(f.Versions); n > 0 {
		f.Versions[n-1].Retired, f.Versions[n-1].RetiredAt = true, s.epoch
		if !f.Versions[n-1].Tombstone {
			s.addNS(Namespace(p), -f.Versions[n-1].Size)
		}
	}
	if !v.Tombstone {
		s.addNS(Namespace(p), v.Size)
	}
	f.Versions = append(f.Versions, v)
}

// RetainFor is how many epochs a retired version of p is kept: its own
// setting, else def.
func (s *State) RetainFor(p string, def uint64) uint64 {
	if f := s.files[p]; f != nil && f.Retain > 0 {
		return uint64(f.Retain)
	}
	return def
}

// hardDelete drops versions retired at least retain epochs ago, except the
// newest version of each path, and releases their chunk references.
func (s *State) hardDelete(retain uint64) int {
	dropped := 0
	for _, f := range s.files {
		retain := s.RetainFor(f.Path, retain)
		keep := f.Versions[:0]
		for i, v := range f.Versions {
			if i == len(f.Versions)-1 || !v.Retired || v.RetiredAt+retain > s.epoch {
				keep = append(keep, v)
				continue
			}
			dropped++
			delete(s.committed, v.UploadID)
			for _, id := range v.Chunks {
				s.unref(id)
			}
		}
		clear(f.Versions[len(keep):])
		f.Versions = keep
	}
	return dropped
}

// expire aborts uploads idle for lease epochs: a client that died
// mid-upload stops pinning its chunks, and GC can collect them.
// SIMPLIFIED: one lease per upload, renewed by claims. HDFS has a soft limit
// (1 min, another writer may take over) and a hard limit (1 h, the NameNode
// closes the file).
func (s *State) expire(lease uint64) int {
	if lease == 0 {
		return 0
	}
	n := 0
	for _, id := range slices.Sorted(maps.Keys(s.uploads)) {
		if u := s.uploads[id]; u.Touched+lease <= s.epoch {
			s.dropUpload(u)
			n++
		}
	}
	return n
}

// Marked reports whether GC must keep every copy of id: a committed or
// retained version references it, a pending upload has claimed it, or it is
// a shard of a stripe one of those names.
func (s *State) Marked(id iface.ChunkID) bool {
	_, shard := s.shardOf[id]
	return s.chunks[id] != nil || s.claimed[id] > 0 || shard
}

// unref drops one reference to id. At zero the record goes: the chunk (or
// the stripe's shards) is unmarked, and GC removes its copies once the
// grace period has passed.
func (s *State) unref(id iface.ChunkID) {
	ci := s.chunks[id]
	if ci.Refcount--; ci.Refcount == 0 {
		delete(s.chunks, id)
		if ci.Shards != nil {
			s.release(id)
		}
	}
}

// Epoch returns the current GC epoch.
func (s *State) Epoch() uint64 { return s.epoch }

// Restored reports whether the newest version of p is an undelete of
// version from that replaced live version prev (0: the path was deleted),
// and returns it: a retried undelete finds its own result here.
func (s *State) Restored(p string, from, prev uint64) (uint64, bool) {
	f := s.files[p]
	if f == nil || len(f.Versions) < 2 {
		return 0, false
	}
	last, before := f.Versions[len(f.Versions)-1], f.Versions[len(f.Versions)-2]
	was := before.V
	if before.Tombstone {
		was = 0
	}
	if last.RestoredFrom != from || was != prev {
		return 0, false
	}
	return last.V, true
}

// Stat returns the live version of p.
func (s *State) Stat(p string) (Version, error) {
	if s.liveVersion(p) == 0 {
		return Version{}, iface.Errorf(iface.CodeNotFound, "%s", p)
	}
	vs := s.files[p].Versions
	return vs[len(vs)-1], nil
}

// StatVersion returns version v of p; 0 is the live version. A tombstone
// is not a readable version.
func (s *State) StatVersion(p string, v uint64) (Version, error) {
	if v == 0 {
		return s.Stat(p)
	}
	if f := s.files[p]; f != nil {
		if i, ok := slices.BinarySearchFunc(f.Versions, v, func(x Version, v uint64) int { return cmp.Compare(x.V, v) }); ok {
			if f.Versions[i].Tombstone {
				return Version{}, iface.Errorf(iface.CodeNotFound, "%s version %d is a delete marker", p, v)
			}
			return f.Versions[i], nil
		}
	}
	return Version{}, iface.Errorf(iface.CodeNotFound, "%s version %d", p, v)
}

// Log returns every retained version of p, tombstones included, oldest
// first.
func (s *State) Log(p string) ([]Version, error) {
	f := s.files[p]
	if f == nil || len(f.Versions) == 0 {
		return nil, iface.Errorf(iface.CodeNotFound, "%s", p)
	}
	return slices.Clone(f.Versions), nil
}

// Entry is one line of a listing.
type Entry struct {
	Path string
	Version
}

// List returns live files under prefix, sorted by path. Pending uploads are
// never listed.
func (s *State) List(prefix string) []Entry {
	var out []Entry
	for p := range s.files {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		if v, err := s.Stat(p); err == nil {
			out = append(out, Entry{Path: p, Version: v})
		}
	}
	slices.SortFunc(out, func(a, b Entry) int { return cmp.Compare(a.Path, b.Path) })
	return out
}

// Deleted lists paths under prefix whose newest version is a tombstone,
// each with the newest real version it retains: what undelete restores.
func (s *State) Deleted(prefix string) []Entry {
	var out []Entry
	for p, f := range s.files {
		if !strings.HasPrefix(p, prefix) || len(f.Versions) == 0 || !f.Versions[len(f.Versions)-1].Tombstone {
			continue
		}
		for _, v := range slices.Backward(f.Versions) {
			if !v.Tombstone {
				out = append(out, Entry{Path: p, Version: v})
				break
			}
		}
	}
	slices.SortFunc(out, func(a, b Entry) int { return cmp.Compare(a.Path, b.Path) })
	return out
}

// CommittedUpload reports the version an already-committed upload created.
func (s *State) CommittedUpload(id uint64) (Committed, bool) {
	c, ok := s.committed[id]
	return c, ok
}

// Tombstoned reports whether the last version of p is a tombstone that
// replaced version prev, and returns the tombstone's version.
func (s *State) Tombstoned(p string, prev uint64) (uint64, bool) {
	f := s.files[p]
	if f == nil || len(f.Versions) < 2 {
		return 0, false
	}
	last, before := f.Versions[len(f.Versions)-1], f.Versions[len(f.Versions)-2]
	if !last.Tombstone || before.V != prev {
		return 0, false
	}
	return last.V, true
}

// Upload returns a pending upload.
func (s *State) Upload(id uint64) (*Upload, bool) {
	u, ok := s.uploads[id]
	return u, ok
}

// Chunk returns the durable record of a chunk.
func (s *State) Chunk(id iface.ChunkID) (ChunkInfo, bool) {
	c, ok := s.chunks[id]
	if !ok {
		return ChunkInfo{}, false
	}
	return *c, true
}

// Chunks calls fn for every chunk record, in chunk ID order.
func (s *State) Chunks(fn func(iface.ChunkID, ChunkInfo)) {
	for _, id := range slices.SortedFunc(maps.Keys(s.chunks), func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) }) {
		fn(id, *s.chunks[id])
	}
}

// PendingUploads reports how many uploads are pending; used by invariants.
func (s *State) PendingUploads() int { return len(s.uploads) }

// Snapshot encodes the state deterministically (sorted, deterministic proto).
func (s *State) Snapshot() []byte {
	snap := &chunkdv1.MetaSnapshot{LastUploadId: s.lastUploadID, Epoch: s.epoch}
	for _, p := range slices.Sorted(maps.Keys(s.files)) {
		f := s.files[p]
		rec := &chunkdv1.FileRecord{Path: p, RetainEpochs: f.Retain}
		for _, v := range f.Versions {
			fv := &chunkdv1.FileVersion{Version: v.V, UploadId: v.UploadID, Size: v.Size, ChunkSize: int32(v.ChunkSize), State: chunkdv1.VersionState_VERSION_STATE_COMMITTED,
				Retired: v.Retired, RetiredAt: v.RetiredAt, RestoredFrom: v.RestoredFrom, Redundancy: v.Redundancy}
			if v.Tombstone {
				fv.State = chunkdv1.VersionState_VERSION_STATE_TOMBSTONE
			} else {
				fv.Sha256 = v.SHA256[:]
			}
			for _, id := range v.Chunks {
				fv.ChunkIds = append(fv.ChunkIds, id[:])
			}
			rec.Versions = append(rec.Versions, fv)
		}
		snap.Files = append(snap.Files, rec)
	}
	ids := slices.SortedFunc(maps.Keys(s.chunks), func(a, b iface.ChunkID) int { return slices.Compare(a[:], b[:]) })
	for _, id := range ids {
		c := s.chunks[id]
		snap.Chunks = append(snap.Chunks, &chunkdv1.ChunkRecord{Id: id[:], Refcount: c.Refcount, Size: c.Size, Shards: rawIDs(c.Shards)})
	}
	for _, id := range slices.Sorted(maps.Keys(s.uploads)) {
		u := s.uploads[id]
		b := &chunkdv1.BeginUploadOp{Path: u.Path, ExpectedVersion: u.Expected, Size: u.Size, ChunkSize: int32(u.ChunkSize), Claims: u.Claims, LastWriterWins: u.LWW, Redundancy: u.Redundancy}
		if u.RequestID != "" {
			b.RequestId = []byte(u.RequestID)
		}
		b.Sha256 = u.SHA256
		for _, r := range u.Placement {
			rep := &chunkdv1.Replicas{}
			for _, n := range r {
				rep.Nodes = append(rep.Nodes, string(n))
			}
			b.Placement = append(b.Placement, rep)
		}
		rec := &chunkdv1.UploadRecord{Id: id, Begin: b, TouchedEpoch: u.Touched}
		for _, i := range slices.Sorted(maps.Keys(u.Claimed)) {
			cid := u.Claimed[i]
			rec.Claims = append(rec.Claims, &chunkdv1.ChunkClaim{Index: int32(i), Id: cid[:], Shards: rawIDs(u.Shards[i])})
		}
		snap.Uploads = append(snap.Uploads, rec)
	}
	for _, t := range s.GCPendingAll() {
		snap.GcPending = append(snap.GcPending, &chunkdv1.GCTarget{ChunkId: t.Chunk[:], Node: string(t.Node), FenceIncarnation: t.Incarnation, FenceSeq: t.Seq})
	}
	for _, t := range s.TrimPendingAll() {
		snap.TrimPending = append(snap.TrimPending, &chunkdv1.TrimTarget{ChunkId: t.Chunk[:], Node: string(t.Node)})
	}
	for _, n := range slices.Sorted(maps.Keys(s.nodeAdmin)) {
		snap.NodeAdmin = append(snap.NodeAdmin, &chunkdv1.NodeAdminOp{Node: string(n), State: s.nodeAdmin[n]})
	}
	out, err := proto.MarshalOptions{Deterministic: true}.Marshal(snap)
	if err != nil {
		panic(err)
	}
	return out
}

// Restore replaces the state with a decoded snapshot.
func Restore(data []byte) (*State, error) {
	s := New()
	if len(data) == 0 {
		return s, nil
	}
	var snap chunkdv1.MetaSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	s.lastUploadID, s.epoch = snap.GetLastUploadId(), snap.GetEpoch()
	for _, rec := range snap.GetFiles() {
		f := &File{Path: rec.GetPath(), Retain: rec.GetRetainEpochs()}
		for _, fv := range rec.GetVersions() {
			v := Version{V: fv.GetVersion(), UploadID: fv.GetUploadId(), Size: fv.GetSize(), ChunkSize: int(fv.GetChunkSize()), Tombstone: fv.GetState() == chunkdv1.VersionState_VERSION_STATE_TOMBSTONE,
				Retired: fv.GetRetired(), RetiredAt: fv.GetRetiredAt(), RestoredFrom: fv.GetRestoredFrom(), Redundancy: fv.GetRedundancy()}
			if v.UploadID != 0 {
				s.committed[v.UploadID] = Committed{Path: rec.GetPath(), Version: v.V}
			}
			copy(v.SHA256[:], fv.GetSha256())
			for _, raw := range fv.GetChunkIds() {
				var id iface.ChunkID
				copy(id[:], raw)
				v.Chunks = append(v.Chunks, id)
			}
			f.Versions = append(f.Versions, v)
		}
		s.files[f.Path] = f
	}
	for _, c := range snap.GetChunks() {
		var id iface.ChunkID
		copy(id[:], c.GetId())
		ci := &ChunkInfo{Refcount: c.GetRefcount(), Size: c.GetSize()}
		if len(c.GetShards()) > 0 {
			shards, err := shardIDs(c.GetShards())
			if err != nil {
				return nil, err
			}
			ci.Shards = shards
			s.hold(id, shards, ci.Size)
		}
		s.chunks[id] = ci
	}
	for _, u := range snap.GetUploads() {
		b := u.GetBegin()
		up := &Upload{ID: u.GetId(), Path: b.GetPath(), Expected: b.GetExpectedVersion(), Size: b.GetSize(), ChunkSize: int(b.GetChunkSize()), Claims: b.GetClaims(), Claimed: map[int]iface.ChunkID{}, LWW: b.GetLastWriterWins(), Touched: u.GetTouchedEpoch(),
			Redundancy: b.GetRedundancy(), Shards: map[int][]iface.ChunkID{},
			RequestID: string(b.GetRequestId()), SHA256: bytes.Clone(b.GetSha256())}
		if up.RequestID != "" {
			s.requests[up.RequestID] = up.ID
		}
		for _, r := range b.GetPlacement() {
			var nodes []iface.NodeID
			for _, n := range r.GetNodes() {
				nodes = append(nodes, iface.NodeID(n))
			}
			up.Placement = append(up.Placement, nodes)
		}
		s.uploads[up.ID] = up
		for _, cl := range u.GetClaims() {
			s.claim(up, int(cl.GetIndex()), cl.GetId(), cl.GetShards())
		}
	}
	for _, t := range snap.GetGcPending() {
		var id iface.ChunkID
		copy(id[:], t.GetChunkId())
		n := iface.NodeID(t.GetNode())
		if s.gcPending[id] == nil {
			s.gcPending[id] = map[iface.NodeID]GCTarget{}
		}
		s.gcPending[id][n] = GCTarget{Chunk: id, Node: n, Incarnation: t.GetFenceIncarnation(), Seq: t.GetFenceSeq()}
	}
	for _, t := range snap.GetTrimPending() {
		var id iface.ChunkID
		copy(id[:], t.GetChunkId())
		s.trimPending[id] = iface.NodeID(t.GetNode())
	}
	for _, a := range snap.GetNodeAdmin() {
		s.nodeAdmin[iface.NodeID(a.GetNode())] = a.GetState()
	}
	s.nsBytes = s.recountNamespaces()
	return s, nil
}
