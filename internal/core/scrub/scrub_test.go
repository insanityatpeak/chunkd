package scrub

import (
	"context"
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

const mib = 1 << 20

func fill(t *testing.T, s *sim.BlockStore, n int, size int) []iface.ChunkID {
	t.Helper()
	var ids []iface.ChunkID
	for i := range n {
		b := make([]byte, size)
		b[0], b[1] = byte(i), byte(i>>8)
		id := iface.ChunkID(sha256.Sum256(b))
		if err := s.Put(context.Background(), id, b); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestScrubber(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, clock *sim.Clock, store *sim.BlockStore, sc *Scrubber, found *[]iface.ChunkID)
	}{
		{"rate is capped: 10 MiB at 1 MiB/s takes 10 s", func(t *testing.T, clock *sim.Clock, store *sim.BlockStore, sc *Scrubber, _ *[]iface.ChunkID) {
			fill(t, store, 10, mib)
			sc.Start(0)
			clock.Advance(9 * time.Second)
			if st := sc.Stats(); st.Passes != 0 || st.Bytes > 10*mib {
				t.Fatalf("after 9 s: %+v", st)
			}
			clock.Advance(1500 * time.Millisecond)
			if st := sc.Stats(); st.Passes != 1 || st.Bytes != 10*mib || st.LastPass != 10*time.Second {
				t.Fatalf("after 10.5 s: %+v", st)
			}
		}},
		{"corrupt chunk found once per pass", func(t *testing.T, clock *sim.Clock, store *sim.BlockStore, sc *Scrubber, found *[]iface.ChunkID) {
			ids := fill(t, store, 5, mib)
			store.Corrupt(ids[3])
			sc.Start(0)
			clock.Advance(6 * time.Second)
			if !slices.Equal(*found, []iface.ChunkID{ids[3]}) || sc.Stats().Corrupt != 1 {
				t.Fatalf("found %v, stats %+v", *found, sc.Stats())
			}
		}},
		{"next pass starts one interval after the last start", func(t *testing.T, clock *sim.Clock, store *sim.BlockStore, sc *Scrubber, found *[]iface.ChunkID) {
			ids := fill(t, store, 2, mib)
			sc.Start(0)
			clock.Advance(59 * time.Second)
			store.Corrupt(ids[0]) // after the first pass read it
			if len(*found) != 0 || sc.Stats().Passes != 1 {
				t.Fatalf("first pass: found %v, %+v", *found, sc.Stats())
			}
			clock.Advance(2 * time.Second) // second pass starts at 60 s
			if len(*found) != 1 {
				t.Fatalf("second pass did not find the corruption: %+v", sc.Stats())
			}
		}},
		{"a pass longer than the interval is followed at once", func(t *testing.T, clock *sim.Clock, store *sim.BlockStore, sc *Scrubber, _ *[]iface.ChunkID) {
			fill(t, store, 70, mib) // 70 s per pass against a 60 s interval
			sc.Start(0)
			clock.Advance(141 * time.Second)
			if st := sc.Stats(); st.Passes != 2 {
				t.Fatalf("passes %d after 141 s, want 2", st.Passes)
			}
		}},
		{"deleted mid-pass: skipped; added mid-pass: next pass", func(t *testing.T, clock *sim.Clock, store *sim.BlockStore, sc *Scrubber, _ *[]iface.ChunkID) {
			ids := fill(t, store, 4, mib)
			sc.Start(0)
			clock.Advance(500 * time.Millisecond)
			_ = store.Delete(context.Background(), ids[3])
			fill(t, store, 6, 1000)
			clock.Advance(10 * time.Second)
			if st := sc.Stats(); st.Passes != 1 || st.Chunks != 3 || st.Total != 4 {
				t.Fatalf("first pass %+v, want 3 of 4 verified", st)
			}
			clock.Advance(60 * time.Second)
			if st := sc.Stats(); st.Total != 9 {
				t.Fatalf("second pass saw %d chunks, want 9", st.Total)
			}
		}},
		{"stop halts", func(t *testing.T, clock *sim.Clock, store *sim.BlockStore, sc *Scrubber, _ *[]iface.ChunkID) {
			fill(t, store, 10, mib)
			sc.Start(0)
			clock.Advance(2500 * time.Millisecond)
			sc.Stop()
			before := sc.Stats().Chunks
			clock.Advance(time.Minute)
			if sc.Stats().Chunks != before {
				t.Fatal("scrubbed after Stop")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock, store := sim.NewClock(), sim.NewBlockStore()
			var found []iface.ChunkID
			sc := New(Config{BytesPerSec: mib, Pass: time.Minute}, clock, store, func(id iface.ChunkID) { found = append(found, id) })
			tc.run(t, clock, store, sc, &found)
		})
	}
}
