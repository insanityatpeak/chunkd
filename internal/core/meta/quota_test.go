package meta

import (
	"maps"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func withQuota(op *chunkdv1.Op, limit int64) *chunkdv1.Op {
	switch o := op.GetOp().(type) {
	case *chunkdv1.Op_Begin:
		o.Begin.Quota = limit
	case *chunkdv1.Op_Undelete:
		o.Undelete.Quota = limit
	}
	return op
}

func TestNamespace(t *testing.T) {
	for p, want := range map[string]string{"/alice/a/b": "alice", "/alice": "alice", "/bob/x": "bob"} {
		if got := Namespace(p); got != want {
			t.Errorf("Namespace(%q) = %q, want %q", p, got, want)
		}
	}
}

// Reservations count at Begin, so uploads that each fit alone cannot
// together pass the quota; an abort or commit changes what is charged.
func TestQuotaReservesAtBegin(t *testing.T) {
	ok, over := iface.CodeUnknown, iface.CodeQuota
	s := New()
	run(t, s, []step{
		{withQuota(begin("/a/one", 0, 60), 100), ok, Result{UploadID: 1}},
		{withQuota(begin("/a/two", 0, 60), 100), over, Result{}}, // 60 reserved + 60 > 100
		{withQuota(begin("/b/two", 0, 60), 100), ok, Result{UploadID: 2}},
		{abort(1), ok, Result{UploadID: 1}},
		{withQuota(begin("/a/two", 0, 100), 100), ok, Result{UploadID: 3}}, // the reservation was released
		{commit(3, 25, 'a'), ok, Result{UploadID: 3, Version: 1}},
		{withQuota(begin("/a/three", 0, 1), 100), over, Result{}},        // now charged as a live version
		{withQuota(begin("/a/three", 0, 1), 0), ok, Result{UploadID: 4}}, // no limit set
	})
	if got := s.NamespaceBytes("a"); got != 101 {
		t.Fatalf("namespace a holds %d, want 101", got)
	}
}

func TestQuotaFreedByDeleteAndChargedByUndelete(t *testing.T) {
	ok, over := iface.CodeUnknown, iface.CodeQuota
	s := New()
	run(t, s, []step{
		{withQuota(begin("/a/f", 0, 80), 100), ok, Result{UploadID: 1}},
		{commit(1, 20, 'a'), ok, Result{UploadID: 1, Version: 1}},
		{del("/a/f", 1), ok, Result{Version: 2}},
		{withQuota(begin("/a/g", 0, 80), 100), ok, Result{UploadID: 2}}, // the delete freed 80
		{commit(2, 20, 'b'), ok, Result{UploadID: 2, Version: 1}},
		{withQuota(undel("/a/f", 1, 0), 100), over, Result{}}, // restoring 80 more would pass 100
		{withQuota(undel("/a/f", 1, 0), 200), ok, Result{Version: 3}},
	})
}

// A snapshot restores the uploads, so the reservations survive with them.
func TestQuotaSurvivesSnapshot(t *testing.T) {
	s := New()
	if _, err := s.Apply(withQuota(begin("/a/one", 0, 60), 100)); err != nil {
		t.Fatal(err)
	}
	r, err := Restore(s.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(withQuota(begin("/a/two", 0, 60), 100)); iface.CodeOf(err) != iface.CodeQuota {
		t.Fatalf("after restore: %v", err)
	}
}

func retention(path string, epochs uint32) *chunkdv1.Op {
	return &chunkdv1.Op{Op: &chunkdv1.Op_SetRetention{SetRetention: &chunkdv1.SetRetentionOp{Path: path, RetainEpochs: epochs}}}
}

func twoVersions(path string, uid uint64, tag byte) []step {
	ok := iface.CodeUnknown
	return []step{
		{begin(path, 0, 4), ok, Result{UploadID: uid}},
		{commit(uid, 1, tag), ok, Result{UploadID: uid, Version: 1}},
		{begin(path, 1, 4), ok, Result{UploadID: uid + 1}},
		{commit(uid+1, 1, tag+1), ok, Result{UploadID: uid + 1, Version: 2}},
	}
}

// A path with its own retention keeps a retired version past the cluster's.
func TestRetentionPerPath(t *testing.T) {
	ok := iface.CodeUnknown
	s := New()
	run(t, s, twoVersions("/a", 1, 'a'))
	run(t, s, twoVersions("/b", 3, 'c'))
	run(t, s, []step{
		{retention("/a", 3), ok, Result{}},
		{retention("/missing", 3), iface.CodeNotFound, Result{}},
		{advance(1), ok, Result{Dropped: 1}},
	})
	if got := len(s.files["/b"].Versions); got != 1 {
		t.Fatalf("/b kept %d versions on the default retention of 1 epoch", got)
	}
	if got := len(s.files["/a"].Versions); got != 2 {
		t.Fatalf("/a kept %d versions, want its retired one still held", got)
	}
	run(t, s, []step{{advance(1), ok, Result{}}, {advance(1), ok, Result{Dropped: 1}}})
	if got := len(s.files["/a"].Versions); got != 1 {
		t.Fatalf("/a kept %d versions after its 3 epochs", got)
	}
	if got := s.RetainFor("/a", 1); got != 3 {
		t.Fatalf("RetainFor = %d", got)
	}
}

func TestRetentionSurvivesSnapshot(t *testing.T) {
	s := New()
	run(t, s, twoVersions("/a", 1, 'a'))
	run(t, s, []step{{retention("/a", 5), iface.CodeUnknown, Result{}}})
	r, err := Restore(s.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if got := r.RetainFor("/a", 1); got != 5 {
		t.Fatalf("after restore RetainFor = %d", got)
	}
}

// The counter must equal a recount after every op, through Delete, Undelete,
// overwrites, lease expiry and a snapshot round trip (ADR-0028).
func TestQuotaCounterMatchesRecount(t *testing.T) {
	for seed := range uint64(40) {
		rng := &lcg{seed*2 + 1}
		s := New()
		paths := []string{"/a/x", "/a/y", "/b/x", "/b/y/z", "/c"}
		for i := range 300 {
			var op *chunkdv1.Op
			switch k := rng.IntN(8); k {
			case 0, 1, 2:
				p := paths[rng.IntN(len(paths))]
				op = begin(p, 0, int64(rng.IntN(40)))
				if o := op.GetBegin(); rng.IntN(2) == 0 {
					o.ExpectedVersion = s.liveVersion(p)
				} else {
					o.LastWriterWins = true
				}
			case 3, 4:
				if ids := slices.Sorted(maps.Keys(s.uploads)); len(ids) > 0 {
					u := s.uploads[ids[rng.IntN(len(ids))]]
					op = commit(u.ID, len(u.Placement), byte(rng.IntN(5)))
				}
			case 5:
				if ids := slices.Sorted(maps.Keys(s.uploads)); len(ids) > 0 {
					op = abort(ids[rng.IntN(len(ids))])
				}
			case 6:
				p := paths[rng.IntN(len(paths))]
				if rng.IntN(2) == 0 {
					op = del(p, s.liveVersion(p))
				} else if f := s.files[p]; f != nil {
					op = undel(p, f.Versions[rng.IntN(len(f.Versions))].V, s.liveVersion(p))
				}
			default:
				op = &chunkdv1.Op{Op: &chunkdv1.Op_AdvanceEpoch{AdvanceEpoch: &chunkdv1.AdvanceEpochOp{RetainEpochs: 1, LeaseEpochs: 2}}}
			}
			if op == nil {
				continue
			}
			_, _ = s.Apply(op) // refusals leave the state alone
			if got, want := s.nsBytes, s.recountNamespaces(); !maps.Equal(got, want) {
				t.Fatalf("seed %d op %d: counter %v, recount %v", seed, i, got, want)
			}
			if i%50 == 0 {
				r, err := Restore(s.Snapshot())
				if err != nil {
					t.Fatal(err)
				}
				if !maps.Equal(r.nsBytes, s.nsBytes) {
					t.Fatalf("seed %d op %d: restored %v, live %v", seed, i, r.nsBytes, s.nsBytes)
				}
			}
		}
		if d := s.Reconcile(); !d.Empty() {
			t.Fatalf("seed %d: drift %+v", seed, d)
		}
	}
}

func TestReconcileAlarmsQuotaDrift(t *testing.T) {
	s := New()
	run(t, s, []step{
		{begin("/a/f", 0, 8), iface.CodeUnknown, Result{UploadID: 1}},
		{commit(1, 2, 'a'), iface.CodeUnknown, Result{UploadID: 1, Version: 1}},
	})
	s.nsBytes["a"]++
	s.nsBytes["ghost"] = 5
	d := s.Reconcile()
	if !slices.Equal(d.Namespaces, []string{"a", "ghost"}) {
		t.Fatalf("drift = %+v", d)
	}
	if s.nsBytes["a"] != 9 {
		t.Fatal("reconcile corrected the counter")
	}
}

// lcg is a seeded generator local to the test; core code takes randomness
// from iface, and a test needs only a fixed sequence.
type lcg struct{ x uint64 }

func (l *lcg) IntN(n int) int {
	l.x = l.x*6364136223846793005 + 1442695040888963407
	return int(l.x >> 33 % uint64(n))
}
