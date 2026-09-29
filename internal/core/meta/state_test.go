package meta

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func begin(path string, expected uint64, size int64) *chunkdv1.Op {
	n := int((size + 3) / 4)
	pl := make([]*chunkdv1.Replicas, n)
	for i := range pl {
		pl[i] = &chunkdv1.Replicas{Nodes: []string{"n1", "n2", "n3"}}
	}
	return &chunkdv1.Op{Op: &chunkdv1.Op_Begin{Begin: &chunkdv1.BeginUploadOp{Path: path, ExpectedVersion: expected, Size: size, ChunkSize: 4, Placement: pl}}}
}

func commit(id uint64, chunks int, tag byte) *chunkdv1.Op {
	var ids [][]byte
	for i := range chunks {
		h := sha256.Sum256([]byte{tag, byte(i)})
		ids = append(ids, h[:])
	}
	sum := sha256.Sum256([]byte{tag})
	return &chunkdv1.Op{Op: &chunkdv1.Op_Commit{Commit: &chunkdv1.CommitUploadOp{UploadId: id, ChunkIds: ids, Sha256: sum[:]}}}
}

func abort(id uint64) *chunkdv1.Op {
	return &chunkdv1.Op{Op: &chunkdv1.Op_Abort{Abort: &chunkdv1.AbortUploadOp{UploadId: id}}}
}

func del(path string, expected uint64) *chunkdv1.Op {
	return &chunkdv1.Op{Op: &chunkdv1.Op_Delete{Delete: &chunkdv1.DeleteOp{Path: path, ExpectedVersion: expected}}}
}

// step is one op and the outcome it must have.
type step struct {
	op   *chunkdv1.Op
	code iface.Code // CodeUnknown means success
	res  Result
}

func run(t *testing.T, s *State, steps []step) {
	t.Helper()
	for i, st := range steps {
		err := s.Validate(st.op)
		if iface.CodeOf(err) != st.code || (st.code != iface.CodeUnknown) != (err != nil) {
			t.Fatalf("step %d: err = %v, want code %v", i, err, st.code)
		}
		if err != nil {
			continue
		}
		if got := s.Apply(st.op); got != st.res {
			t.Fatalf("step %d: result %+v, want %+v", i, got, st.res)
		}
	}
}

func TestStateOps(t *testing.T) {
	ok := iface.CodeUnknown
	tests := []struct {
		name     string
		steps    []step
		wantLive map[string]uint64 // path -> live version; absent = not found
	}{
		{"create then read", []step{
			{begin("/a", 0, 10), ok, Result{UploadID: 1}},
			{commit(1, 3, 'a'), ok, Result{UploadID: 1, Version: 1}},
		}, map[string]uint64{"/a": 1}},
		{"overwrite needs the live version", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
			{begin("/a", 0, 4), iface.CodeConflict, Result{}},
			{begin("/a", 1, 4), ok, Result{UploadID: 2}},
			{commit(2, 1, 'b'), ok, Result{UploadID: 2, Version: 2}},
		}, map[string]uint64{"/a": 2}},
		{"racing writers: second commit loses", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{begin("/a", 0, 4), ok, Result{UploadID: 2}},
			{commit(2, 1, 'b'), ok, Result{UploadID: 2, Version: 1}},
			{commit(1, 1, 'a'), iface.CodeConflict, Result{}},
		}, map[string]uint64{"/a": 1}},
		{"abort leaves nothing visible", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{abort(1), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), iface.CodeNotFound, Result{}},
		}, nil},
		{"delete tombstones; recreate continues numbering", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
			{del("/a", 2), iface.CodeConflict, Result{}},
			{del("/a", 1), ok, Result{Version: 2}},
			{del("/a", 0), iface.CodeNotFound, Result{}},
			{begin("/a", 0, 4), ok, Result{UploadID: 2}},
			{commit(2, 1, 'b'), ok, Result{UploadID: 2, Version: 3}},
		}, map[string]uint64{"/a": 3}},
		{"empty file has zero chunks", []step{
			{begin("/empty", 0, 0), ok, Result{UploadID: 1}},
			{commit(1, 0, 'e'), ok, Result{UploadID: 1, Version: 1}},
		}, map[string]uint64{"/empty": 1}},
		{"invalid requests", []step{
			{begin("relative", 0, 4), iface.CodeInvalid, Result{}},
			{begin("/a/../b", 0, 4), iface.CodeInvalid, Result{}},
			{begin("/a", 0, -1), iface.CodeInvalid, Result{}},
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 2, 'a'), iface.CodeInvalid, Result{}}, // wrong chunk count
			{&chunkdv1.Op{}, iface.CodeInvalid, Result{}},
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			run(t, s, tt.steps)
			for _, p := range []string{"/a", "/empty"} {
				v, err := s.Stat(p)
				want, exists := tt.wantLive[p]
				if exists != (err == nil) || (exists && v.V != want) {
					t.Fatalf("Stat(%s) = v%d, %v; want exists=%v v%d", p, v.V, err, exists, want)
				}
			}
			if len(s.List("/")) != len(tt.wantLive) {
				t.Fatalf("List = %v, want %d files", s.List("/"), len(tt.wantLive))
			}
		})
	}
}

func TestUncommittedInvisible(t *testing.T) {
	s := New()
	run(t, s, []step{
		{begin("/done", 0, 4), iface.CodeUnknown, Result{UploadID: 1}},
		{commit(1, 1, 'd'), iface.CodeUnknown, Result{UploadID: 1, Version: 1}},
		{begin("/pending", 0, 4), iface.CodeUnknown, Result{UploadID: 2}},
		{begin("/done", 1, 4), iface.CodeUnknown, Result{UploadID: 3}}, // pending overwrite
	})
	if _, err := s.Stat("/pending"); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("Stat(/pending) err = %v, want not found", err)
	}
	if v, _ := s.Stat("/done"); v.V != 1 {
		t.Fatalf("Stat(/done) = v%d, want v1 while v2 is pending", v.V)
	}
	var paths []string
	for _, e := range s.List("/") {
		paths = append(paths, e.Path)
	}
	if !slices.Equal(paths, []string{"/done"}) {
		t.Fatalf("List = %v, want only /done", paths)
	}
}

func TestRefcounts(t *testing.T) {
	s := New()
	run(t, s, []step{
		{begin("/a", 0, 8), iface.CodeUnknown, Result{UploadID: 1}},
		{commit(1, 2, 'x'), iface.CodeUnknown, Result{UploadID: 1, Version: 1}},
		{begin("/b", 0, 8), iface.CodeUnknown, Result{UploadID: 2}},
		{commit(2, 2, 'x'), iface.CodeUnknown, Result{UploadID: 2, Version: 1}}, // same content
	})
	id := iface.ChunkID(sha256.Sum256([]byte{'x', 0}))
	if c, ok := s.Chunk(id); !ok || c.Refcount != 2 || c.Size != 4 {
		t.Fatalf("chunk = %+v %v, want refcount 2 size 4", c, ok)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := New()
	run(t, s, []step{
		{begin("/a", 0, 10), iface.CodeUnknown, Result{UploadID: 1}},
		{commit(1, 3, 'a'), iface.CodeUnknown, Result{UploadID: 1, Version: 1}},
		{begin("/b", 0, 4), iface.CodeUnknown, Result{UploadID: 2}},
		{commit(2, 1, 'b'), iface.CodeUnknown, Result{UploadID: 2, Version: 1}},
		{del("/b", 1), iface.CodeUnknown, Result{Version: 2}},
		{begin("/c", 0, 4), iface.CodeUnknown, Result{UploadID: 3}}, // pending survives snapshot
	})
	snap := s.Snapshot()
	r, err := Restore(snap)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Snapshot(), snap) {
		t.Fatal("restore then snapshot changed the bytes")
	}
	// The restored state must accept the same next ops with the same results.
	for _, st := range []*State{s, r} {
		run(t, st, []step{
			{commit(3, 1, 'c'), iface.CodeUnknown, Result{UploadID: 3, Version: 1}},
			{begin("/d", 0, 4), iface.CodeUnknown, Result{UploadID: 4}},
		})
	}
	if !bytes.Equal(r.Snapshot(), s.Snapshot()) {
		t.Fatal("states diverged after identical ops")
	}
}

func TestApplyInvalidPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Apply of an invalid op did not panic")
		}
	}()
	New().Apply(abort(99))
}

func beginClaims(path string, expected uint64, size int64) *chunkdv1.Op {
	op := begin(path, expected, size)
	op.GetBegin().Claims = true
	return op
}

// claimOp claims chunk i of the file commit(id, _, tag) will commit.
func claimOp(id uint64, tag byte, idx ...int) *chunkdv1.Op {
	c := &chunkdv1.ClaimChunksOp{UploadId: id}
	for _, i := range idx {
		h := sha256.Sum256([]byte{tag, byte(i)})
		c.Claims = append(c.Claims, &chunkdv1.ChunkClaim{Index: int32(i), Id: h[:]})
	}
	return &chunkdv1.Op{Op: &chunkdv1.Op_Claim{Claim: c}}
}

func TestClaims(t *testing.T) {
	ok := iface.CodeUnknown
	tests := []struct {
		name        string
		steps       []step
		wantClaimed int // chunks still claimed by pending uploads
	}{
		{"commit needs every chunk claimed", []step{
			{beginClaims("/a", 0, 8), ok, Result{UploadID: 1}},
			{claimOp(1, 'a', 0), ok, Result{UploadID: 1}},
			{commit(1, 2, 'a'), iface.CodeInvalid, Result{}},
			{claimOp(1, 'a', 1), ok, Result{UploadID: 1}},
			{commit(1, 2, 'a'), ok, Result{UploadID: 1, Version: 1}},
		}, 0},
		{"a claim for other content does not cover the chunk", []step{
			{beginClaims("/a", 0, 4), ok, Result{UploadID: 1}},
			{claimOp(1, 'x', 0), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), iface.CodeInvalid, Result{}},
		}, 1},
		{"re-claim is a retry, a different id is rejected", []step{
			{beginClaims("/a", 0, 4), ok, Result{UploadID: 1}},
			{claimOp(1, 'a', 0), ok, Result{UploadID: 1}},
			{claimOp(1, 'a', 0), ok, Result{UploadID: 1}},
			{claimOp(1, 'b', 0), iface.CodeInvalid, Result{}},
		}, 1},
		{"claims outside the upload are rejected", []step{
			{beginClaims("/a", 0, 4), ok, Result{UploadID: 1}},
			{claimOp(1, 'a', 1), iface.CodeInvalid, Result{}},
			{claimOp(9, 'a', 0), iface.CodeNotFound, Result{}},
		}, 0},
		{"abort releases claims", []step{
			{beginClaims("/a", 0, 8), ok, Result{UploadID: 1}},
			{claimOp(1, 'a', 0, 1), ok, Result{UploadID: 1}},
			{abort(1), ok, Result{UploadID: 1}},
		}, 0},
		{"two uploads claiming one chunk both hold it", []step{
			{beginClaims("/a", 0, 4), ok, Result{UploadID: 1}},
			{beginClaims("/b", 0, 4), ok, Result{UploadID: 2}},
			{claimOp(1, 'a', 0), ok, Result{UploadID: 1}},
			{claimOp(2, 'a', 0), ok, Result{UploadID: 2}},
			{abort(1), ok, Result{UploadID: 1}},
		}, 1},
		{"uploads from before claims commit without them", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			run(t, s, tt.steps)
			if len(s.claimed) != tt.wantClaimed {
				t.Fatalf("%d chunks claimed, want %d", len(s.claimed), tt.wantClaimed)
			}
			r, err := Restore(s.Snapshot())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(r.Snapshot(), s.Snapshot()) || len(r.claimed) != len(s.claimed) {
				t.Fatal("claims did not survive a snapshot")
			}
		})
	}
}

func TestDedupAccounting(t *testing.T) {
	s := New()
	// Two files with identical content (tag 'a', 2 chunks of 4 bytes) and
	// one with its own: 3 files × 8 bytes referenced, 16 distinct.
	run(t, s, []step{
		{beginClaims("/a", 0, 8), iface.CodeUnknown, Result{UploadID: 1}},
		{claimOp(1, 'a', 0, 1), iface.CodeUnknown, Result{UploadID: 1}},
		{commit(1, 2, 'a'), iface.CodeUnknown, Result{UploadID: 1, Version: 1}},
		{beginClaims("/b", 0, 8), iface.CodeUnknown, Result{UploadID: 2}},
		{claimOp(2, 'a', 0, 1), iface.CodeUnknown, Result{UploadID: 2}},
		{commit(2, 2, 'a'), iface.CodeUnknown, Result{UploadID: 2, Version: 1}},
		{beginClaims("/c", 0, 8), iface.CodeUnknown, Result{UploadID: 3}},
		{claimOp(3, 'c', 0, 1), iface.CodeUnknown, Result{UploadID: 3}},
		{commit(3, 2, 'c'), iface.CodeUnknown, Result{UploadID: 3, Version: 1}},
	})
	if ref, uniq := s.Dedup(); ref != 24 || uniq != 16 {
		t.Fatalf("referenced %d, unique %d; want 24, 16", ref, uniq)
	}
}

func beginLWW(path string, size int64) *chunkdv1.Op {
	op := begin(path, 0, size)
	op.GetBegin().LastWriterWins = true
	return op
}

func TestLastWriterWins(t *testing.T) {
	ok := iface.CodeUnknown
	s := New()
	run(t, s, []step{
		{begin("/a", 0, 4), ok, Result{UploadID: 1}},
		{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
		// Both begin against v1; with CAS the second commit would conflict.
		{beginLWW("/a", 4), ok, Result{UploadID: 2}},
		{beginLWW("/a", 4), ok, Result{UploadID: 3}},
		{commit(2, 1, 'b'), ok, Result{UploadID: 2, Version: 2}},
		{commit(3, 1, 'c'), ok, Result{UploadID: 3, Version: 3}},
		// A CAS writer that began before them still loses.
		{begin("/a", 3, 4), ok, Result{UploadID: 4}},
		{beginLWW("/a", 4), ok, Result{UploadID: 5}},
		{commit(5, 1, 'd'), ok, Result{UploadID: 5, Version: 4}},
		{commit(4, 1, 'e'), iface.CodeConflict, Result{}},
	})
	r, err := Restore(s.Snapshot())
	if err != nil || !bytes.Equal(r.Snapshot(), s.Snapshot()) {
		t.Fatalf("LWW flag did not survive a snapshot: %v", err)
	}
	if u, _ := r.Upload(4); u.LWW {
		t.Fatal("CAS upload restored as LWW")
	}
}

func TestVersionHistory(t *testing.T) {
	ok := iface.CodeUnknown
	s := New()
	run(t, s, []step{
		{begin("/a", 0, 4), ok, Result{UploadID: 1}},
		{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
		{begin("/a", 1, 8), ok, Result{UploadID: 2}},
		{commit(2, 2, 'b'), ok, Result{UploadID: 2, Version: 2}},
		{del("/a", 2), ok, Result{Version: 3}},
	})
	tests := []struct {
		v        uint64
		code     iface.Code
		wantSize int64
	}{
		{0, iface.CodeNotFound, 0}, // live: deleted
		{1, ok, 4},
		{2, ok, 8},
		{3, iface.CodeNotFound, 0}, // the tombstone itself
		{4, iface.CodeNotFound, 0},
	}
	for _, tt := range tests {
		got, err := s.StatVersion("/a", tt.v)
		if iface.CodeOf(err) != tt.code || (err == nil && got.Size != tt.wantSize) {
			t.Errorf("version %d: %+v, %v; want code %v size %d", tt.v, got, err, tt.code, tt.wantSize)
		}
	}
	log, err := s.Log("/a")
	if err != nil || len(log) != 3 || !log[2].Tombstone || log[0].V != 1 {
		t.Fatalf("log = %+v, %v", log, err)
	}
	if _, err := s.Log("/missing"); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("log of a missing path: %v", err)
	}
}

func undel(path string, version, expected uint64) *chunkdv1.Op {
	return &chunkdv1.Op{Op: &chunkdv1.Op_Undelete{Undelete: &chunkdv1.UndeleteOp{Path: path, Version: version, ExpectedVersion: expected}}}
}

func advance(retain uint32) *chunkdv1.Op {
	return &chunkdv1.Op{Op: &chunkdv1.Op_AdvanceEpoch{AdvanceEpoch: &chunkdv1.AdvanceEpochOp{RetainEpochs: retain}}}
}

func chunkOf(tag byte, i int) iface.ChunkID { return sha256.Sum256([]byte{tag, byte(i)}) }

func TestRetention(t *testing.T) {
	ok := iface.CodeUnknown
	tests := []struct {
		name      string
		steps     []step
		versions  []uint64 // retained versions of /a
		refs      map[iface.ChunkID]uint64
		wantEpoch uint64
	}{
		{"superseded version kept for the window, then dropped", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
			{begin("/a", 1, 4), ok, Result{UploadID: 2}},
			{commit(2, 1, 'b'), ok, Result{UploadID: 2, Version: 2}}, // v1 retired at epoch 0
			{advance(2), ok, Result{}},                               // epoch 1: 0+2 > 1, kept
			{advance(2), ok, Result{Dropped: 1}},                     // epoch 2: dropped
		}, []uint64{2}, map[iface.ChunkID]uint64{chunkOf('a', 0): 0, chunkOf('b', 0): 1}, 2},
		{"undelete inside the window", []step{
			{begin("/a", 0, 8), ok, Result{UploadID: 1}},
			{commit(1, 2, 'a'), ok, Result{UploadID: 1, Version: 1}},
			{del("/a", 1), ok, Result{Version: 2}},
			{advance(2), ok, Result{}},
			{undel("/a", 1, 1), iface.CodeConflict, Result{}}, // the path is deleted: expect 0
			{undel("/a", 2, 0), iface.CodeNotFound, Result{}}, // a tombstone cannot be restored
			{undel("/a", 1, 0), ok, Result{Version: 3}},
			{advance(2), ok, Result{Dropped: 1}}, // v1 retired at 0 goes; v3 holds its chunks
			{advance(2), ok, Result{Dropped: 1}}, // the tombstone, retired at 1 by v3
		}, []uint64{3}, map[iface.ChunkID]uint64{chunkOf('a', 0): 1, chunkOf('a', 1): 1}, 3},
		{"undelete after the window fails", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
			{del("/a", 1), ok, Result{Version: 2}},
			{advance(1), ok, Result{Dropped: 1}},
			{undel("/a", 1, 0), iface.CodeNotFound, Result{}},
		}, []uint64{2}, map[iface.ChunkID]uint64{chunkOf('a', 0): 0}, 1},
		{"version numbers are not reused after everything is dropped", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
			{del("/a", 1), ok, Result{Version: 2}},
			{advance(1), ok, Result{Dropped: 1}},
			{advance(1), ok, Result{}}, // the tombstone is newest: kept
			{begin("/a", 0, 4), ok, Result{UploadID: 2}},
			{commit(2, 1, 'b'), ok, Result{UploadID: 2, Version: 3}},
		}, []uint64{2, 3}, nil, 2},
		{"a chunk shared with another file outlives one reference", []step{
			{begin("/a", 0, 4), ok, Result{UploadID: 1}},
			{commit(1, 1, 'x'), ok, Result{UploadID: 1, Version: 1}},
			{begin("/b", 0, 4), ok, Result{UploadID: 2}},
			{commit(2, 1, 'x'), ok, Result{UploadID: 2, Version: 1}},
			{del("/a", 1), ok, Result{Version: 2}},
			{advance(1), ok, Result{Dropped: 1}},
		}, []uint64{2}, map[iface.ChunkID]uint64{chunkOf('x', 0): 1}, 1},
		{"retention of 0 epochs is rejected", []step{
			{advance(0), iface.CodeInvalid, Result{}},
		}, nil, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			run(t, s, tt.steps)
			var got []uint64
			if log, err := s.Log("/a"); err == nil {
				for _, v := range log {
					got = append(got, v.V)
				}
			}
			if !slices.Equal(got, tt.versions) {
				t.Fatalf("retained versions %v, want %v", got, tt.versions)
			}
			for id, want := range tt.refs {
				ci, _ := s.Chunk(id)
				if ci.Refcount != want {
					t.Errorf("chunk %s: refcount %d, want %d", id.String()[:8], ci.Refcount, want)
				}
				if _, ok := s.Chunk(id); ok != (want > 0) {
					t.Errorf("chunk %s: record present %v, want %v", id.String()[:8], ok, want > 0)
				}
			}
			if s.Epoch() != tt.wantEpoch {
				t.Fatalf("epoch %d, want %d", s.Epoch(), tt.wantEpoch)
			}
			r, err := Restore(s.Snapshot())
			if err != nil || !bytes.Equal(r.Snapshot(), s.Snapshot()) {
				t.Fatalf("retention state did not survive a snapshot: %v", err)
			}
		})
	}
}

func TestUndeleteRetryFindsItsResult(t *testing.T) {
	s := New()
	run(t, s, []step{
		{begin("/a", 0, 4), iface.CodeUnknown, Result{UploadID: 1}},
		{commit(1, 1, 'a'), iface.CodeUnknown, Result{UploadID: 1, Version: 1}},
		{del("/a", 1), iface.CodeUnknown, Result{Version: 2}},
		{undel("/a", 1, 0), iface.CodeUnknown, Result{Version: 3}},
	})
	if v, ok := s.Restored("/a", 1, 0); !ok || v != 3 {
		t.Fatalf("Restored = v%d, %v; want v3, true", v, ok)
	}
	if _, ok := s.Restored("/a", 1, 1); ok {
		t.Fatal("an undelete that expected v1 live matched a restore of a deleted path")
	}
}
