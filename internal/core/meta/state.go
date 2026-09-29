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
}

// File is every version of a path, oldest first. The newest entry is never
// dropped, even if it is a tombstone, so version numbers are never reused.
type File struct {
	Path     string
	Versions []Version
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
	// LWW commits over whatever version is live instead of comparing.
	LWW bool
}

// ChunkInfo is the durable record of a chunk. Refcount counts references
// from committed versions; hard delete decrements it.
type ChunkInfo struct {
	Refcount uint64
	Size     int64
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
	// Dropped counts versions hard-deleted by an AdvanceEpoch.
	Dropped int
}

// New returns empty state.
func New() *State {
	return &State{files: map[string]*File{}, chunks: map[iface.ChunkID]*ChunkInfo{}, uploads: map[uint64]*Upload{}, committed: map[uint64]Committed{}, claimed: map[iface.ChunkID]int{}}
}

// ValidPath reports whether p is an absolute, clean path to a file.
func ValidPath(p string) bool {
	return strings.HasPrefix(p, "/") && p != "/" && path.Clean(p) == p && !strings.ContainsRune(p, 0)
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

// Validate reports whether op would apply cleanly. The server validates
// before logging, so the log only ever holds ops that apply.
func (s *State) Validate(op *chunkdv1.Op) error {
	switch o := op.GetOp().(type) {
	case *chunkdv1.Op_Begin:
		b := o.Begin
		if !ValidPath(b.GetPath()) {
			return iface.Errorf(iface.CodeInvalid, "bad path %q", b.GetPath())
		}
		if b.GetSize() < 0 || b.GetChunkSize() <= 0 {
			return iface.Errorf(iface.CodeInvalid, "bad size %d or chunk size %d", b.GetSize(), b.GetChunkSize())
		}
		if want := chunk.Count(b.GetSize(), int(b.GetChunkSize())); len(b.GetPlacement()) != want {
			return iface.Errorf(iface.CodeInvalid, "placement for %d chunks, want %d", len(b.GetPlacement()), want)
		}
		for i, r := range b.GetPlacement() {
			if len(r.GetNodes()) == 0 {
				return iface.Errorf(iface.CodeInvalid, "chunk %d has no replicas", i)
			}
		}
		if live := s.liveVersion(b.GetPath()); live != b.GetExpectedVersion() && !b.GetLastWriterWins() {
			return iface.Errorf(iface.CodeConflict, "%s is at version %d, expected %d", b.GetPath(), live, b.GetExpectedVersion())
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
	case *chunkdv1.Op_AdvanceEpoch:
		if o.AdvanceEpoch.GetRetainEpochs() == 0 {
			return iface.Errorf(iface.CodeInvalid, "retain_epochs must be at least 1")
		}
	default:
		return iface.Errorf(iface.CodeInvalid, "empty op")
	}
	return nil
}

// Apply applies a validated op. An op that fails validation here means the
// log and the state have diverged, which is unrecoverable: it panics.
func (s *State) Apply(op *chunkdv1.Op) Result {
	if err := s.Validate(op); err != nil {
		panic("meta: applying invalid op: " + err.Error())
	}
	switch o := op.GetOp().(type) {
	case *chunkdv1.Op_Begin:
		b := o.Begin
		s.lastUploadID++
		u := &Upload{ID: s.lastUploadID, Path: b.GetPath(), Expected: b.GetExpectedVersion(), Size: b.GetSize(), ChunkSize: int(b.GetChunkSize()), Claims: b.GetClaims(), Claimed: map[int]iface.ChunkID{}, LWW: b.GetLastWriterWins()}
		for _, r := range b.GetPlacement() {
			nodes := make([]iface.NodeID, len(r.GetNodes()))
			for i, n := range r.GetNodes() {
				nodes[i] = iface.NodeID(n)
			}
			u.Placement = append(u.Placement, nodes)
		}
		s.uploads[u.ID] = u
		return Result{UploadID: u.ID}
	case *chunkdv1.Op_Commit:
		c := o.Commit
		u := s.uploads[c.GetUploadId()]
		v := Version{V: s.lastVersion(u.Path) + 1, UploadID: u.ID, Size: u.Size, ChunkSize: u.ChunkSize}
		copy(v.SHA256[:], c.GetSha256())
		for i, raw := range c.GetChunkIds() {
			var id iface.ChunkID
			copy(id[:], raw)
			v.Chunks = append(v.Chunks, id)
			ci := s.chunks[id]
			if ci == nil {
				ci = &ChunkInfo{Size: chunk.SizeOf(u.Size, u.ChunkSize, i)}
				s.chunks[id] = ci
			}
			ci.Refcount++
		}
		s.appendVersion(u.Path, v)
		s.committed[u.ID] = Committed{Path: u.Path, Version: v.V}
		s.dropUpload(u)
		return Result{UploadID: u.ID, Version: v.V}
	case *chunkdv1.Op_Claim:
		u := s.uploads[o.Claim.GetUploadId()]
		for _, cl := range o.Claim.GetClaims() {
			s.claim(u, int(cl.GetIndex()), cl.GetId())
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
		v := Version{V: s.lastVersion(u.GetPath()) + 1, Size: src.Size, SHA256: src.SHA256, Chunks: slices.Clone(src.Chunks), ChunkSize: src.ChunkSize, RestoredFrom: src.V}
		// src is retained, so it still holds a reference to every chunk: the
		// records exist and none can have been collected.
		for _, id := range v.Chunks {
			s.chunks[id].Refcount++
		}
		s.appendVersion(u.GetPath(), v)
		return Result{Version: v.V}
	case *chunkdv1.Op_AdvanceEpoch:
		s.epoch++
		return Result{Dropped: s.hardDelete(uint64(o.AdvanceEpoch.GetRetainEpochs()))}
	}
	panic("unreachable")
}

func (s *State) claim(u *Upload, i int, raw []byte) {
	if _, ok := u.Claimed[i]; ok {
		return
	}
	var id iface.ChunkID
	copy(id[:], raw)
	u.Claimed[i] = id
	s.claimed[id]++
}

// dropUpload ends a pending upload and releases its claims. On commit the
// refcounts taken just before keep the chunks marked.
func (s *State) dropUpload(u *Upload) {
	for _, id := range u.Claimed {
		if s.claimed[id]--; s.claimed[id] == 0 {
			delete(s.claimed, id)
		}
	}
	delete(s.uploads, u.ID)
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
	}
	f.Versions = append(f.Versions, v)
}

// hardDelete drops versions retired at least retain epochs ago, except the
// newest version of each path, and releases their chunk references.
func (s *State) hardDelete(retain uint64) int {
	dropped := 0
	for _, f := range s.files {
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

// unref drops one reference to id. At zero the record goes: the chunk is
// unmarked, and GC removes its copies once the grace period has passed.
func (s *State) unref(id iface.ChunkID) {
	ci := s.chunks[id]
	if ci.Refcount--; ci.Refcount == 0 {
		delete(s.chunks, id)
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
		rec := &chunkdv1.FileRecord{Path: p}
		for _, v := range f.Versions {
			fv := &chunkdv1.FileVersion{Version: v.V, UploadId: v.UploadID, Size: v.Size, ChunkSize: int32(v.ChunkSize), State: chunkdv1.VersionState_VERSION_STATE_COMMITTED,
				Retired: v.Retired, RetiredAt: v.RetiredAt, RestoredFrom: v.RestoredFrom}
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
		snap.Chunks = append(snap.Chunks, &chunkdv1.ChunkRecord{Id: id[:], Refcount: c.Refcount, Size: c.Size})
	}
	for _, id := range slices.Sorted(maps.Keys(s.uploads)) {
		u := s.uploads[id]
		b := &chunkdv1.BeginUploadOp{Path: u.Path, ExpectedVersion: u.Expected, Size: u.Size, ChunkSize: int32(u.ChunkSize), Claims: u.Claims, LastWriterWins: u.LWW}
		for _, r := range u.Placement {
			rep := &chunkdv1.Replicas{}
			for _, n := range r {
				rep.Nodes = append(rep.Nodes, string(n))
			}
			b.Placement = append(b.Placement, rep)
		}
		rec := &chunkdv1.UploadRecord{Id: id, Begin: b}
		for _, i := range slices.Sorted(maps.Keys(u.Claimed)) {
			cid := u.Claimed[i]
			rec.Claims = append(rec.Claims, &chunkdv1.ChunkClaim{Index: int32(i), Id: cid[:]})
		}
		snap.Uploads = append(snap.Uploads, rec)
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
		f := &File{Path: rec.GetPath()}
		for _, fv := range rec.GetVersions() {
			v := Version{V: fv.GetVersion(), UploadID: fv.GetUploadId(), Size: fv.GetSize(), ChunkSize: int(fv.GetChunkSize()), Tombstone: fv.GetState() == chunkdv1.VersionState_VERSION_STATE_TOMBSTONE,
				Retired: fv.GetRetired(), RetiredAt: fv.GetRetiredAt(), RestoredFrom: fv.GetRestoredFrom()}
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
		s.chunks[id] = &ChunkInfo{Refcount: c.GetRefcount(), Size: c.GetSize()}
	}
	for _, u := range snap.GetUploads() {
		b := u.GetBegin()
		up := &Upload{ID: u.GetId(), Path: b.GetPath(), Expected: b.GetExpectedVersion(), Size: b.GetSize(), ChunkSize: int(b.GetChunkSize()), Claims: b.GetClaims(), Claimed: map[int]iface.ChunkID{}, LWW: b.GetLastWriterWins()}
		for _, r := range b.GetPlacement() {
			var nodes []iface.NodeID
			for _, n := range r.GetNodes() {
				nodes = append(nodes, iface.NodeID(n))
			}
			up.Placement = append(up.Placement, nodes)
		}
		s.uploads[up.ID] = up
		for _, cl := range u.GetClaims() {
			s.claim(up, int(cl.GetIndex()), cl.GetId())
		}
	}
	return s, nil
}
