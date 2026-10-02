package repair

import (
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// ecSetup: 8 nodes, one stripe on n01-n06, started 5 s in.
func ecSetup(t *testing.T) (*world, *harness, []iface.ChunkID) {
	w := newWorld(8)
	shards := w.addStripe(10, 1, 2, 3, 4, 5, 6)
	h := newHarness(t, unthrottled(), w, 10*time.Millisecond)
	h.s.Start()
	h.clock.Advance(5 * time.Second)
	return w, h, shards
}

func TestShardRebuild(t *testing.T) {
	t.Run("one shard lost waits out the delay, then rebuilds from 4", func(t *testing.T) {
		w, h, shards := ecSetup(t)
		w.kill(node(3), h.clock.Now())
		h.s.Scan()
		h.clock.Advance(19 * time.Second)
		if len(h.copies) != 0 {
			t.Fatalf("%d copies inside the delay with 5 of 6 shards left", len(h.copies))
		}
		h.clock.Advance(2 * time.Second)
		if len(h.copies) != 1 {
			t.Fatalf("%d copies after the delay, want 1", len(h.copies))
		}
		c := h.copies[0]
		if c.Rebuild == nil || c.Chunk != shards[2] || c.Source != "" || c.Rebuild.Stripe.Index != 2 {
			t.Fatalf("copy %+v, want a rebuild of shard 2", c)
		}
		if len(c.Rebuild.Sources) != 5 || slices.ContainsFunc(c.Rebuild.Sources, func(s ShardSource) bool { return s.Index == 2 }) {
			t.Fatalf("sources %+v: want the 5 other shards", c.Rebuild.Sources)
		}
		// Never onto a node holding another shard of the stripe (n03 is dead).
		if c.Target != node(7) && c.Target != node(8) {
			t.Fatalf("rebuilt onto %s", c.Target)
		}
		st := h.s.Stats()
		if st.Rebuilds != 1 || st.RebuildRead != 4<<20 || st.Completed != 1 {
			t.Fatalf("stats %+v: want 1 rebuild reading 4 MiB", st)
		}
	})

	t.Run("a stripe down to 4 rebuilds at once", func(t *testing.T) {
		w, h, _ := ecSetup(t)
		w.kill(node(1), h.clock.Now())
		w.kill(node(5), h.clock.Now())
		h.s.Scan()
		h.clock.Advance(time.Second)
		// Both are assessed with no margin left, so neither waits.
		if st := h.s.Stats(); st.Rebuilds != 2 || st.Completed != 2 {
			t.Fatalf("stats %+v: want both rebuilt at once", st)
		}
	})

	t.Run("three lost is counted, never sent", func(t *testing.T) {
		w, h, _ := ecSetup(t)
		for _, n := range []int{1, 2, 6} {
			w.kill(node(n), h.clock.Now())
		}
		h.s.Scan()
		h.clock.Advance(time.Minute)
		if st := h.s.Stats(); st.Lost != 3 || st.Dispatched != 0 {
			t.Fatalf("stats %+v", st)
		}
	})

	t.Run("a shard never written is rebuilt after the upload grace", func(t *testing.T) {
		w := newWorld(8)
		shards := w.addStripe(10, 1, 2, 3, 4, 5, 0) // committed at 5 of 6
		h := newHarness(t, unthrottled(), w, 10*time.Millisecond)
		h.s.Fresh(shards)
		h.s.Scan()
		h.clock.Advance(h.s.cfg.UploadGrace - time.Second)
		if len(h.copies) != 0 {
			t.Fatal("rebuilt inside the upload grace")
		}
		h.clock.Advance(2 * time.Second)
		if st := h.s.Stats(); st.Rebuilds != 1 {
			t.Fatalf("stats %+v", st)
		}
	})

	t.Run("each of 4 sources holds a source slot", func(t *testing.T) {
		w, h, _ := ecSetup(t)
		h.copyTime = -1
		w.kill(node(3), h.clock.Now())
		h.s.Scan()
		h.clock.Advance(21 * time.Second)
		c := h.copies[0]
		for _, s := range c.Rebuild.Sources[:ec.DataShards] {
			if h.s.src[s.Node] != 1 {
				t.Fatalf("source %s holds %d slots", s.Node, h.s.src[s.Node])
			}
		}
		if fallback := c.Rebuild.Sources[ec.DataShards].Node; h.s.src[fallback] != 0 {
			t.Fatalf("fallback %s holds a slot", fallback)
		}
		h.s.Failed(c.ID, c.Chunk)
		// The failure frees every slot; the retry takes them again.
		if st := h.s.Stats(); st.Failed != 1 || st.Rebuilds != 2 {
			t.Fatalf("stats %+v", st)
		}
	})

	t.Run("a busy source makes way for a free sibling", func(t *testing.T) {
		w, h, _ := ecSetup(t)
		h.s.src[node(1)] = h.s.cfg.PerSource
		w.kill(node(3), h.clock.Now())
		h.s.Scan()
		h.clock.Advance(21 * time.Second)
		srcs := h.copies[0].Rebuild.Sources
		if slices.ContainsFunc(srcs[:ec.DataShards], func(s ShardSource) bool { return s.Node == node(1) }) {
			t.Fatalf("busy n01 among the first 4 sources: %+v", srcs)
		}
	})
}

func TestShardOnLeavingNodeIsCopiedNotRebuilt(t *testing.T) {
	w, h, shards := ecSetup(t)
	w.leaving = map[iface.NodeID]bool{node(4): true}
	h.s.Scan()
	h.clock.Advance(time.Second)
	if len(h.copies) != 1 {
		t.Fatalf("%d copies, want 1", len(h.copies))
	}
	c := h.copies[0]
	if c.Rebuild != nil || c.Chunk != shards[3] || c.Source != node(4) || c.Class != Drain || (c.Target != node(7) && c.Target != node(8)) {
		t.Fatalf("copy %+v: want shard 3 drained from n04 to n07 or n08", c)
	}
}

func TestExtraShardCopyIsTrimmed(t *testing.T) {
	w, h, shards := ecSetup(t)
	w.holders[shards[0]] = append(w.holders[shards[0]], node(7))
	var trims []Trim
	h.s.send.Trim = func(tr Trim) { trims = append(trims, tr) }
	h.s.Scan()
	if len(trims) != 1 || trims[0].Chunk != shards[0] {
		t.Fatalf("trims %+v, want one of shard 0", trims)
	}
}

// Two shards of one stripe on one node (left by a copy GC was deleting):
// a rebuild reading both must stay within that node's source slots
// (bugs-found #26).
func TestRebuildCountsSlotsPerNode(t *testing.T) {
	w := newWorld(8)
	w.addStripe(10, 1, 2, 2, 4, 5, 6) // n02 holds shards 1 and 2
	h := newHarness(t, unthrottled(), w, -1)
	h.s.src[node(2)] = 1
	w.kill(node(1), 0)
	h.s.Scan()
	h.clock.Advance(21 * time.Second) // 5 shards left: the delay applies
	if len(h.copies) != 1 {
		t.Fatalf("%d copies, want 1", len(h.copies))
	}
	n2 := 0
	for _, s := range h.copies[0].Rebuild.Sources[:ec.DataShards] {
		if s.Node == node(2) {
			n2++
		}
	}
	if n2 != 1 || h.s.src[node(2)] > h.s.cfg.PerSource {
		t.Fatalf("n02 is %d of the first 4 sources, %d slots held", n2, h.s.src[node(2)])
	}
}
