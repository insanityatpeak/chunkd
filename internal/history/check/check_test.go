package check

import (
	"path/filepath"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/history"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

func h(b byte) [32]byte { return [32]byte{b} }

// hist builds a history from calls made in the order given, at the times
// they carry.
type hist struct{ r history.Recorder }

func (x *hist) put(c int, call, ret int64, v uint64, b byte, err error) {
	x.r.Put(c, "/a", h(b), call, ret, v, err)
}
func (x *hist) del(c int, call, ret int64, v uint64, err error) {
	x.r.Delete(c, "/a", call, ret, v, err)
}
func (x *hist) read(c int, call, ret int64, v uint64, b byte, err error) {
	x.r.Read(c, "/a", call, ret, v, h(b), err)
}

var (
	errAmbig    = iface.Errorf(iface.CodeUnavailable, "timed out")
	errConflict = iface.Errorf(iface.CodeConflict, "stale")
	errMissing  = iface.Errorf(iface.CodeNotFound, "/a")
)

func TestModel(t *testing.T) {
	tests := []struct {
		name  string
		build func(x *hist)
		want  bool
	}{
		{"sequential put and read", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.read(1, 3, 4, 1, 1, nil)
		}, true},
		{"read before any write", func(x *hist) { x.read(1, 1, 2, 0, 0, errMissing) }, true},
		{"stale read: old version after a newer put returned", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.put(1, 3, 4, 2, 2, nil)
			x.read(2, 5, 6, 1, 1, nil)
		}, false},
		{"lost acked write: not found after ok put", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.read(2, 3, 4, 0, 0, errMissing)
		}, false},
		{"version gap: put returns v3 on an empty path", func(x *hist) { x.put(1, 1, 2, 3, 1, nil) }, false},
		{"version repeat: two puts both v1", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.put(2, 3, 4, 1, 2, nil)
		}, false},
		{"read returns bytes never written at that version", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.read(2, 3, 4, 1, 9, nil)
		}, false},
		{"ambiguous put that did not apply", func(x *hist) {
			x.put(1, 1, 2, 0, 1, errAmbig)
			x.read(2, 3, 4, 0, 0, errMissing)
		}, true},
		{"ambiguous put that applied, seen late", func(x *hist) {
			x.put(1, 1, 2, 0, 1, errAmbig)
			x.read(2, 3, 4, 0, 0, errMissing)
			x.read(2, 5, 6, 1, 1, nil)
		}, true},
		{"ambiguous put applied, then it must stay applied", func(x *hist) {
			x.put(1, 1, 2, 0, 1, errAmbig)
			x.read(2, 3, 4, 1, 1, nil)
			x.read(2, 5, 6, 0, 0, errMissing)
		}, false},
		{"ambiguous put shifts the next version", func(x *hist) {
			x.put(1, 1, 2, 0, 1, errAmbig)
			x.put(2, 3, 4, 2, 2, nil)
		}, true},
		{"overlapping puts, either order", func(x *hist) {
			x.put(1, 1, 10, 2, 1, nil)
			x.put(2, 2, 9, 1, 2, nil)
			x.read(3, 11, 12, 2, 1, nil)
		}, true},
		{"overlapping puts, reader saw the loser", func(x *hist) {
			x.put(1, 1, 10, 2, 1, nil)
			x.put(2, 2, 9, 1, 2, nil)
			x.read(3, 11, 12, 1, 2, nil)
		}, false},
		{"put conflict changes nothing", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.put(2, 3, 4, 0, 2, errConflict)
			x.read(1, 5, 6, 1, 1, nil)
		}, true},
		{"delete then read not found", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.del(1, 3, 4, 2, nil)
			x.read(2, 5, 6, 0, 0, errMissing)
		}, true},
		{"read of a deleted file", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.del(1, 3, 4, 2, nil)
			x.read(2, 5, 6, 1, 1, nil)
		}, false},
		{"put after delete continues the version count", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.del(1, 3, 4, 2, nil)
			x.put(1, 5, 6, 3, 2, nil)
		}, true},
		{"put after delete restarts at v1", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.del(1, 3, 4, 2, nil)
			x.put(1, 5, 6, 1, 2, nil)
		}, false},
		{"delete ok on an absent path", func(x *hist) { x.del(1, 1, 2, 1, nil) }, false},
		{"delete not found on a live path", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.del(2, 3, 4, 0, errMissing)
		}, false},
		{"ambiguous delete either way", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.del(1, 3, 4, 0, errAmbig)
			x.read(2, 5, 6, 0, 0, errMissing)
		}, true},
		{"failed reads are dropped", func(x *hist) {
			x.put(1, 1, 2, 1, 1, nil)
			x.read(2, 3, 4, 0, 0, errAmbig)
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var x hist
			tc.build(&x)
			if got := Check(x.r.Ops()); got.OK != tc.want || got.Unknown {
				t.Fatalf("%s, want linearizable=%v", got, tc.want)
			}
		})
	}
}

func TestListDecomposed(t *testing.T) {
	var r history.Recorder
	r.Put(1, "/d/a", h(1), 1, 2, 1, nil)
	r.Put(1, "/d/b", h(2), 3, 4, 1, nil)
	// A listing after both puts that omits /d/b is a lost write.
	r.List(2, "/d/", 5, 6, []history.Entry{{Path: "/d/a", Version: 1, Hash: h(1)}}, nil)
	if Check(r.Ops()).OK {
		t.Fatal("a listing missing an acknowledged file passed")
	}
	var ok history.Recorder
	ok.Put(1, "/d/a", h(1), 1, 2, 1, nil)
	ok.Put(1, "/d/b", h(2), 3, 4, 1, nil)
	ok.List(2, "/d/", 5, 6, []history.Entry{{Path: "/d/a", Version: 1, Hash: h(1)}, {Path: "/d/b", Version: 1, Hash: h(2)}}, nil)
	ok.List(2, "/other/", 7, 8, nil, nil)
	if res := Check(ok.Ops()); !res.OK {
		t.Fatal(res)
	}
}

func TestPathsAreIndependent(t *testing.T) {
	var r history.Recorder
	r.Put(1, "/a", h(1), 1, 2, 1, nil)
	r.Put(1, "/b", h(2), 3, 4, 1, nil)
	r.Read(2, "/a", 5, 6, 1, h(1), nil)
	if res := Check(r.Ops()); !res.OK {
		t.Fatal(res)
	}
}

func TestWriteHTML(t *testing.T) {
	var x hist
	x.put(1, 1, 2, 1, 1, nil)
	x.read(2, 3, 4, 0, 0, errMissing)
	res := Check(x.r.Ops())
	if res.OK {
		t.Fatal("expected a violation")
	}
	if err := res.WriteHTML(filepath.Join(t.TempDir(), "h.html")); err != nil {
		t.Fatal(err)
	}
}
