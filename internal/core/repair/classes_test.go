package repair

import (
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// applyTrims makes the harness carry out trims: the copy leaves the world and
// the node reports it gone.
func (h *harness) applyTrims() {
	h.s.send.Trim = func(t Trim) {
		h.trims = append(h.trims, t)
		h.clock.AfterFunc(5*time.Millisecond, func() {
			h.w.holders[t.Chunk] = slices.DeleteFunc(h.w.holders[t.Chunk], func(n iface.NodeID) bool { return n == t.Node })
			h.s.Removed(t.Node, []iface.ChunkID{t.Chunk})
		})
	}
}

func (w *world) member(ns ...int) {
	if w.members == nil {
		w.members = map[iface.NodeID]bool{}
	}
	for _, n := range ns {
		w.members[node(n)] = true
	}
}

// A draining holder's copy still makes RF, so its chunks are drain class:
// copied off from any holder, the leaving copy included, and never trimmed.
func TestDrainCopiesOffAndNeverTrimsTheLeavingCopy(t *testing.T) {
	w := newWorld(5)
	w.leaving = map[iface.NodeID]bool{node(1): true}
	for i := range 6 {
		w.add(chunk(i), 1, 1, 2, 3)
	}
	h := newHarness(t, unthrottled(), w, 10*time.Millisecond)
	h.applyTrims()
	h.s.Scan()
	h.clock.Advance(5 * time.Second)
	for _, c := range h.copies {
		if c.Class != Drain || c.Target == node(1) {
			t.Fatalf("copy %+v: want drain class onto a non-leaving node", c)
		}
	}
	if len(h.copies) != 6 {
		t.Fatalf("%d evacuation copies, want 6", len(h.copies))
	}
	for _, tr := range h.trims {
		if tr.Node == node(1) {
			t.Fatalf("trimmed the leaving node's copy of %v", tr.Chunk)
		}
	}
	if st := h.s.Stats(); st.Evacuated != 6 {
		t.Fatalf("evacuated %d, want 6", st.Evacuated)
	}
}

// Repair goes first, and drain and balance never hold more than Background
// slots: a failure arriving mid-drain finds slots at once.
func TestRepairBeforeDrainAndBackgroundCap(t *testing.T) {
	w := newWorld(20)
	w.leaving = map[iface.NodeID]bool{node(1): true}
	for i := range 20 {
		w.add(chunk(i), 1, 1, 2+i%9, 11+i%9) // drain: n01 leaving
	}
	for i := 100; i < 104; i++ {
		w.add(chunk(i), 1, 2+i%9, 11+(i+1)%9) // repair: one copy missing
	}
	cfg := unthrottled()
	cfg.PerSource, cfg.PerTarget = 100, 100
	h := newHarness(t, cfg, w, -1)
	h.s.Scan()
	var drain, repair int
	for _, c := range h.copies {
		if c.Class == Repair {
			repair++
			if drain > 0 {
				t.Fatalf("repair copy dispatched after a drain copy: %v", h.copies)
			}
		} else {
			drain++
		}
	}
	if repair != 4 || drain != cfg.Background {
		t.Fatalf("dispatched %d repair and %d drain copies; want 4 and %d", repair, drain, cfg.Background)
	}
}

// A node added empty draws balance moves. Each move's second half trims its
// own source, holders stay at RF, and moving stops once n04 is within the
// band of its share (18 MiB of 72; band: two chunks).
func TestBalanceMovesOntoNewNode(t *testing.T) {
	w := newWorld(4)
	w.member(1, 2, 3, 4)
	for i := range 24 {
		w.add(chunk(i), 1<<20, 1, 2, 3) // n04 is new and empty; one rack
	}
	h := newHarness(t, unthrottled(), w, 10*time.Millisecond)
	h.applyTrims()
	for range 20 {
		h.s.Scan()
		h.clock.Advance(time.Second)
	}
	st := h.s.Stats()
	if st.MovedBytes < 16<<20 || st.MovedBytes > 18<<20 {
		t.Fatalf("moved %d MiB, want 16..18 (n04's share of 72, within 2 MiB)", st.MovedBytes>>20)
	}
	for _, c := range h.copies {
		if c.Class != Balance || c.Target != node(4) {
			t.Fatalf("copy %+v: want balance onto n04", c)
		}
	}
	for i, tr := range h.trims {
		if tr.Node != h.copies[i].Source {
			t.Fatalf("trim %d removed %s, the move's source was %s", i, tr.Node, h.copies[i].Source)
		}
	}
	for id, hs := range w.holders {
		if len(hs) != 3 {
			t.Fatalf("chunk %v has %d copies", id, len(hs))
		}
	}
}

// Balance waits while anything needs repair.
func TestBalanceWaitsForRepair(t *testing.T) {
	w := newWorld(4)
	w.member(1, 2, 3, 4)
	for i := range 12 {
		w.add(chunk(i), 1<<20, 1, 2, 3)
	}
	w.add(chunk(99), 1<<20, 1) // below RF, one copy: repaired at once
	cfg := unthrottled()
	cfg.MaxInFlight = 1
	h := newHarness(t, cfg, w, -1)
	h.s.Scan()
	if len(h.copies) != 1 || h.copies[0].Class != Repair {
		t.Fatalf("first copies %+v, want one repair", h.copies)
	}
	if st := h.s.Stats(); st.Queued != 0 {
		t.Fatalf("balance planned while repair in flight: %d queued", st.Queued)
	}
}

// While a node may still return (suspect, unconfirmed, or dead inside the
// delay), targets computed without it are wrong: no balance is planned.
func TestBalanceWaitsForSettledMembership(t *testing.T) {
	for name, unsettle := range map[string]func(w *world){
		"suspect":     func(w *world) { w.state[node(5)] = detector.Suspect },
		"unconfirmed": func(w *world) { w.unconfirmed = map[iface.NodeID]bool{node(5): true} },
		"dead":        func(w *world) { w.kill(node(5), 0) },
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(5)
			w.member(1, 2, 3, 4, 5)
			for i := range 24 {
				w.add(chunk(i), 1<<20, 1, 2, 3) // n04 empty: unbalanced
			}
			unsettle(w)
			h := newHarness(t, unthrottled(), w, -1)
			h.s.Scan()
			if len(h.copies) != 0 {
				t.Fatalf("planned %d moves with n05 %s", len(h.copies), name)
			}
		})
	}
	// Dead past the delay: gone, and balancing resumes without it.
	w := newWorld(5)
	w.member(1, 2, 3, 4, 5)
	for i := range 24 {
		w.add(chunk(i), 1<<20, 1, 2, 3)
	}
	w.kill(node(5), 0)
	h := newHarness(t, unthrottled(), w, -1)
	h.clock.Advance(DefaultConfig().Delay)
	h.s.Scan()
	if len(h.copies) == 0 {
		t.Fatal("no moves once the dead node's delay was over")
	}
}
