package meta

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

func TestDeclaredHashBindsCommit(t *testing.T) {
	ok := iface.CodeUnknown
	declared := sha256.Sum256([]byte{'a'}) // what commit(_, _, 'a') carries
	withHash := func(h []byte) step {
		op := beginClaims("/a", 0, 4)
		op.GetBegin().Sha256 = h
		return step{op, ok, Result{UploadID: 1}}
	}
	short := withHash(declared[:5])
	short.code, short.res = iface.CodeInvalid, Result{}

	s := New()
	run(t, s, []step{short, withHash(declared[:]), {claimOp(1, 'a', 0), ok, Result{UploadID: 1}}})
	if u, _ := s.Upload(1); !bytes.Equal(u.SHA256, declared[:]) {
		t.Fatalf("upload hash %x", u.SHA256)
	}
	r, err := Restore(s.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if u, _ := r.Upload(1); !bytes.Equal(u.SHA256, declared[:]) || !bytes.Equal(r.Snapshot(), s.Snapshot()) {
		t.Fatal("declared hash did not survive a snapshot")
	}
	for _, st := range []*State{s, r} {
		other := commit(1, 1, 'a')
		other.GetCommit().Sha256 = make([]byte, 32)
		run(t, st, []step{
			{other, iface.CodeInvalid, Result{}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
		})
	}
}
