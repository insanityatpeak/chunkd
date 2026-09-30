package ifacetest

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// MetaStore runs the MetaStore conformance suite. open returns a fresh,
// empty store and a reopen function that simulates a restart: it returns
// the same store as recovered from durable storage.
func MetaStore(t *testing.T, open func(t *testing.T) (s iface.MetaStore, reopen func() iface.MetaStore)) {
	ctx := context.Background()
	es := func(xs ...string) [][]byte {
		out := make([][]byte, len(xs))
		for i, x := range xs {
			out[i] = []byte(x)
		}
		return out
	}
	save := func(t *testing.T, s iface.MetaStore, first iface.Index, entries [][]byte, state string) {
		t.Helper()
		var st []byte
		if state != "" {
			st = []byte(state)
		}
		if err := s.Save(ctx, first, entries, st); err != nil {
			t.Fatalf("Save(%d, %d entries): %v", first, len(entries), err)
		}
	}
	// check verifies the whole durable view, before and after a restart, and
	// returns the restarted store.
	check := func(t *testing.T, s iface.MetaStore, reopen func() iface.MetaStore, snapAt iface.Index, snap, state string, want ...string) iface.MetaStore {
		t.Helper()
		for round := range 2 {
			st := s
			if round == 1 {
				st = reopen()
			}
			var got []string
			if err := st.Replay(ctx, 0, func(i iface.Index, b []byte) error { got = append(got, fmt.Sprintf("%d:%s", i, b)); return nil }); err != nil {
				t.Fatalf("round %d: Replay: %v", round, err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("round %d: entries = %v, want %v", round, got, want)
			}
			at, data, err := st.LoadSnapshot(ctx)
			if err != nil || at != snapAt || string(data) != snap {
				t.Fatalf("round %d: snapshot = %d %q %v, want %d %q", round, at, data, err, snapAt, snap)
			}
			if gs, err := st.State(ctx); err != nil || string(gs) != state {
				t.Fatalf("round %d: state = %q %v, want %q", round, gs, err, state)
			}
			s = st
		}
		return s
	}

	t.Run("empty", func(t *testing.T) {
		s, reopen := open(t)
		s = check(t, s, reopen, 0, "", "")
		if st, _ := s.State(ctx); st != nil {
			t.Fatalf("empty state = %q, want nil", st)
		}
	})

	t.Run("save and replay from an index", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a", "b", "c"), "hs1")
		s = check(t, s, reopen, 0, "", "hs1", "1:a", "2:b", "3:c")
		var got []string
		_ = s.Replay(ctx, 3, func(i iface.Index, b []byte) error { got = append(got, string(b)); return nil })
		if !slices.Equal(got, []string{"c"}) {
			t.Fatalf("Replay(3) = %v", got)
		}
	})

	t.Run("a later save replaces the tail", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a", "b", "c"), "")
		save(t, s, 2, es("x"), "")
		s = check(t, s, reopen, 0, "", "", "1:a", "2:x")
		save(t, s, 3, es("y", "z"), "")
		check(t, s, reopen, 0, "", "", "1:a", "2:x", "3:y", "4:z")
	})

	t.Run("save with no entries truncates", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a", "b", "c"), "")
		save(t, s, 2, nil, "hs")
		s = check(t, s, reopen, 0, "", "hs", "1:a")
		save(t, s, 2, es("b2"), "")
		check(t, s, reopen, 0, "", "hs", "1:a", "2:b2")
	})

	t.Run("the last state wins and nil keeps it", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a"), "hs1")
		save(t, s, 2, es("b"), "")
		s = check(t, s, reopen, 0, "", "hs1", "1:a", "2:b")
		save(t, s, 3, nil, "hs2")
		check(t, s, reopen, 0, "", "hs2", "1:a", "2:b")
	})

	t.Run("a gap is rejected and changes nothing", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a"), "hs")
		if err := s.Save(ctx, 3, es("c"), []byte("other")); iface.CodeOf(err) != iface.CodeInvalid {
			t.Fatalf("Save past the end: %v, want CodeInvalid", err)
		}
		if err := s.Save(ctx, 0, es("z"), nil); iface.CodeOf(err) != iface.CodeInvalid {
			t.Fatalf("Save at index 0: %v, want CodeInvalid", err)
		}
		check(t, s, reopen, 0, "", "hs", "1:a")
	})

	t.Run("snapshot drops covered entries and keeps later ones", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a", "b", "c", "d"), "hs")
		if err := s.SaveSnapshot(ctx, 2, []byte("snap@2")); err != nil {
			t.Fatal(err)
		}
		s = check(t, s, reopen, 2, "snap@2", "hs", "3:c", "4:d")
		if err := s.Save(ctx, 2, es("z"), nil); iface.CodeOf(err) != iface.CodeInvalid {
			t.Fatalf("Save at the snapshot index: %v, want CodeInvalid", err)
		}
		save(t, s, 4, es("d2", "e"), "")
		check(t, s, reopen, 2, "snap@2", "hs", "3:c", "4:d2", "5:e")
	})

	t.Run("snapshot past the end empties the log", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a", "b"), "hs")
		if err := s.SaveSnapshot(ctx, 10, []byte("snap@10")); err != nil {
			t.Fatal(err)
		}
		s = check(t, s, reopen, 10, "snap@10", "hs")
		save(t, s, 11, es("k"), "")
		check(t, s, reopen, 10, "snap@10", "hs", "11:k")
	})

	t.Run("a newer snapshot replaces the older", func(t *testing.T) {
		s, reopen := open(t)
		save(t, s, 1, es("a", "b", "c", "d"), "")
		_ = s.SaveSnapshot(ctx, 1, []byte("s1"))
		_ = s.SaveSnapshot(ctx, 3, []byte("s3"))
		check(t, s, reopen, 3, "s3", "", "4:d")
	})
}
