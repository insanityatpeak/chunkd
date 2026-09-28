// Package meta holds the metadata state machine and the metadata server.
//
// State is the durable part: files, versions, chunk refcounts and pending
// uploads. It changes only by applying logged Ops, and applying an op is a
// pure function of the state before it, so replaying the log (and, from
// Phase 5, Raft) rebuilds identical state. Chunk locations are not here: they
// live in Cluster and are rebuilt from block reports.
package meta

import (
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
}

// File is every version of a path, oldest first.
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
}

// ChunkInfo is the durable record of a chunk. Refcount counts committed
// versions that reference it; Phase 4 GC decrements it.
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
	// committed indexes versions by the upload that created them; rebuilt
	// from files on restore, so it is not stored separately.
	committed map[uint64]Committed
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
}

// New returns empty state.
func New() *State {
	return &State{files: map[string]*File{}, chunks: map[iface.ChunkID]*ChunkInfo{}, uploads: map[uint64]*Upload{}, committed: map[uint64]Committed{}}
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
		if live := s.liveVersion(b.GetPath()); live != b.GetExpectedVersion() {
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
		// Rechecked here: another writer may have committed since Begin.
		if live := s.liveVersion(u.Path); live != u.Expected {
			return iface.Errorf(iface.CodeConflict, "%s moved to version %d since upload began at %d", u.Path, live, u.Expected)
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
		u := &Upload{ID: s.lastUploadID, Path: b.GetPath(), Expected: b.GetExpectedVersion(), Size: b.GetSize(), ChunkSize: int(b.GetChunkSize())}
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
		delete(s.uploads, u.ID)
		return Result{UploadID: u.ID, Version: v.V}
	case *chunkdv1.Op_Abort:
		delete(s.uploads, o.Abort.GetUploadId())
		return Result{UploadID: o.Abort.GetUploadId()}
	case *chunkdv1.Op_Delete:
		p := o.Delete.GetPath()
		v := Version{V: s.lastVersion(p) + 1, Tombstone: true}
		s.appendVersion(p, v)
		return Result{Version: v.V}
	}
	panic("unreachable")
}

func (s *State) appendVersion(p string, v Version) {
	f := s.files[p]
	if f == nil {
		f = &File{Path: p}
		s.files[p] = f
	}
	f.Versions = append(f.Versions, v)
}

// Stat returns the live version of p.
func (s *State) Stat(p string) (Version, error) {
	if s.liveVersion(p) == 0 {
		return Version{}, iface.Errorf(iface.CodeNotFound, "%s", p)
	}
	vs := s.files[p].Versions
	return vs[len(vs)-1], nil
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

// PendingUploads reports how many uploads are pending; used by invariants.
func (s *State) PendingUploads() int { return len(s.uploads) }

// Snapshot encodes the state deterministically (sorted, deterministic proto).
func (s *State) Snapshot() []byte {
	snap := &chunkdv1.MetaSnapshot{LastUploadId: s.lastUploadID}
	for _, p := range slices.Sorted(maps.Keys(s.files)) {
		f := s.files[p]
		rec := &chunkdv1.FileRecord{Path: p}
		for _, v := range f.Versions {
			fv := &chunkdv1.FileVersion{Version: v.V, UploadId: v.UploadID, Size: v.Size, ChunkSize: int32(v.ChunkSize), State: chunkdv1.VersionState_VERSION_STATE_COMMITTED}
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
		b := &chunkdv1.BeginUploadOp{Path: u.Path, ExpectedVersion: u.Expected, Size: u.Size, ChunkSize: int32(u.ChunkSize)}
		for _, r := range u.Placement {
			rep := &chunkdv1.Replicas{}
			for _, n := range r {
				rep.Nodes = append(rep.Nodes, string(n))
			}
			b.Placement = append(b.Placement, rep)
		}
		snap.Uploads = append(snap.Uploads, &chunkdv1.UploadRecord{Id: id, Begin: b})
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
	s.lastUploadID = snap.GetLastUploadId()
	for _, rec := range snap.GetFiles() {
		f := &File{Path: rec.GetPath()}
		for _, fv := range rec.GetVersions() {
			v := Version{V: fv.GetVersion(), UploadID: fv.GetUploadId(), Size: fv.GetSize(), ChunkSize: int(fv.GetChunkSize()), Tombstone: fv.GetState() == chunkdv1.VersionState_VERSION_STATE_TOMBSTONE}
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
		up := &Upload{ID: u.GetId(), Path: b.GetPath(), Expected: b.GetExpectedVersion(), Size: b.GetSize(), ChunkSize: int(b.GetChunkSize())}
		for _, r := range b.GetPlacement() {
			var nodes []iface.NodeID
			for _, n := range r.GetNodes() {
				nodes = append(nodes, iface.NodeID(n))
			}
			up.Placement = append(up.Placement, nodes)
		}
		s.uploads[up.ID] = up
	}
	return s, nil
}
