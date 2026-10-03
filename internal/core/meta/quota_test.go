package meta

import (
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
