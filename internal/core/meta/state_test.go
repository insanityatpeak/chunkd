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
