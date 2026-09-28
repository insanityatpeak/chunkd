package repair

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// world is a fake View: chunk sizes, who holds what, and node states.
type world struct {
	sizes       map[iface.ChunkID]int64
	holders     map[iface.ChunkID][]iface.NodeID
	state       map[iface.NodeID]detector.State
	deadSince   map[iface.NodeID]iface.Instant
	unconfirmed map[iface.NodeID]bool
}

func newWorld(nodes int) *world {
	w := &world{sizes: map[iface.ChunkID]int64{}, holders: map[iface.ChunkID][]iface.NodeID{},
		state: map[iface.NodeID]detector.State{}, deadSince: map[iface.NodeID]iface.Instant{}}
	for i := 1; i <= nodes; i++ {
		w.state[node(i)] = detector.Alive
	}
	return w
}

func node(i int) iface.NodeID   { return iface.NodeID(fmt.Sprintf("n%02d", i)) }
func chunk(i int) iface.ChunkID { return iface.ChunkID{byte(i >> 8), byte(i)} }

func (w *world) add(id iface.ChunkID, size int64, holders ...int) {
	w.sizes[id] = size
	for _, h := range holders {
		w.holders[id] = append(w.holders[id], node(h))
	}
}

func (w *world) kill(n iface.NodeID, at iface.Instant) {
	w.state[n], w.deadSince[n] = detector.Dead, at
}

func (w *world) Want(id iface.ChunkID) (int64, bool) {
	s, ok := w.sizes[id]
	return s, ok
}

func (w *world) Chunks(fn func(iface.ChunkID, int64)) {
	ids := slices.SortedFunc(maps.Keys(w.sizes), func(a, b iface.ChunkID) int { return cmp.Compare(a.String(), b.String()) })
	for _, id := range ids {
		fn(id, w.sizes[id])
	}
}

func (w *world) Holders(id iface.ChunkID) []Holder {
	var out []Holder
	for _, n := range w.holders[id] {
		out = append(out, Holder{Node: n, State: w.state[n], DeadSince: w.deadSince[n], Confirmed: !w.unconfirmed[n], Rack: "r1"})
	}
	return out
}

// Target: least-loaded alive node not excluded, by node ID on ties.
func (w *world) Target(_ int64, exclude []iface.NodeID) (iface.NodeID, bool) {
	load := map[iface.NodeID]int{}
	for _, hs := range w.holders {
		for _, h := range hs {
			load[h]++
		}
	}
	var best iface.NodeID
	for _, n := range slices.Sorted(maps.Keys(w.state)) {
		if w.state[n] != detector.Alive || slices.Contains(exclude, n) {
			continue
		}
		if best == "" || load[n] < load[best] {
			best = n
		}
	}
	return best, best != ""
}

// harness runs a scheduler over a world on a sim clock. Each dispatched copy
// lands on its target after copyTime unless copyTime is negative (never).
type harness struct {
	t        *testing.T
	clock    *sim.Clock
	w        *world
	s        *Scheduler
	copyTime time.Duration
	order    []iface.ChunkID
	copies   []Copy
	maxIn    int
	maxSrc   map[iface.NodeID]int
	maxDst   map[iface.NodeID]int
}

func newHarness(t *testing.T, cfg Config, w *world, copyTime time.Duration) *harness {
	h := &harness{t: t, clock: sim.NewClock(), w: w, copyTime: copyTime, maxSrc: map[iface.NodeID]int{}, maxDst: map[iface.NodeID]int{}}
	h.s = New(cfg, h.clock, w, Sender{Copy: h.send, Trim: func(Trim) {}})
	return h
}

func (h *harness) send(c Copy) {
	h.order = append(h.order, c.Chunk)
	h.copies = append(h.copies, c)
	in := h.s.InFlight()
	h.maxIn = max(h.maxIn, len(in))
	src, dst := map[iface.NodeID]int{}, map[iface.NodeID]int{}
	for _, f := range in {
		src[f.Source]++
		dst[f.Target]++
	}
	for n, v := range src {
		h.maxSrc[n] = max(h.maxSrc[n], v)
	}
	for n, v := range dst {
		h.maxDst[n] = max(h.maxDst[n], v)
	}
	if h.copyTime < 0 {
		return
	}
	h.clock.AfterFunc(h.copyTime, func() {
		h.w.holders[c.Chunk] = append(h.w.holders[c.Chunk], c.Target)
		h.s.Reported(c.Target, []iface.ChunkID{c.Chunk})
	})
}

func unthrottled() Config {
	cfg := DefaultConfig()
	cfg.BytesPerSec = 0
	return cfg
}

func TestPriorityOrder(t *testing.T) {
	w := newWorld(6)
	// Missing replicas with no dead holder to excuse them: repair at once.
	w.add(chunk(1), 1, 1, 2) // live 2
	w.add(chunk(2), 1, 3)    // live 1: first
	w.add(chunk(3), 1, 4, 5) // live 2
	w.add(chunk(4), 1, 1, 2, 3)
	cfg := unthrottled()
	cfg.MaxInFlight = 1
	h := newHarness(t, cfg, w, 10*time.Millisecond)
	h.s.Scan()
	h.clock.Advance(time.Second)
	// chunk 2 goes first (1 live); its second copy re-queues behind the
	// chunks that were already waiting at 2 live.
	want := []iface.ChunkID{chunk(2), chunk(1), chunk(3), chunk(2)}
	if !slices.Equal(h.order, want) {
		t.Fatalf("order %v, want %v", h.order, want)
	}
	if st := h.s.Stats(); st.Completed != 4 || st.Queued != 0 || st.InFlight != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestRepairThrottle(t *testing.T) {
	const chunks, size = 100, 4 << 20
	w := newWorld(10)
	w.kill(node(10), 0)
	for i := range chunks {
		// Each chunk: two live replicas spread over n01..n09, one on the dead n10.
		w.add(chunk(i), size, 1+i%9, 1+(i+1)%9, 10)
	}
	cfg := DefaultConfig() // 8 in flight, 2 per node, 40 MiB/s, 4 MiB burst
	h := newHarness(t, cfg, w, 300*time.Millisecond)
	h.clock.Advance(cfg.Delay)
	h.s.Scan()
	h.clock.Advance(time.Minute)

	if st := h.s.Stats(); st.Completed != chunks || st.Queued != 0 || st.InFlight != 0 {
		t.Fatalf("not all restored: %+v", st)
	}
	if h.maxIn > cfg.MaxInFlight {
		t.Fatalf("%d copies in flight, limit %d", h.maxIn, cfg.MaxInFlight)
	}
	for n, v := range h.maxSrc {
		if v > cfg.PerSource {
			t.Fatalf("%s sourced %d copies at once, limit %d", n, v, cfg.PerSource)
		}
	}
	for n, v := range h.maxDst {
		if v > cfg.PerTarget {
			t.Fatalf("%s received %d copies at once, limit %d", n, v, cfg.PerTarget)
		}
	}
	// Byte ceiling: by any instant t after the first dispatch, at most
	// burst + rate × t bytes have been dispatched.
	start := h.copies[0].Started
	var sent int64
	for _, c := range h.copies {
		sent += c.Size
		allowed := cfg.Burst + int64(float64(cfg.BytesPerSec)*c.Started.Sub(start).Seconds()) + 1
		// The debt model lets one take overshoot the balance by < 1 chunk.
		if sent > allowed+size {
			t.Fatalf("at +%v dispatched %d bytes, ceiling %d", c.Started.Sub(start), sent, allowed)
		}
	}
	// 400 MiB at 40 MiB/s is 10 s; slots must not be what limits it.
	last := h.copies[len(h.copies)-1].Started.Sub(start)
	if last < 9*time.Second || last > 11*time.Second {
		t.Fatalf("last copy dispatched at +%v, want about 10 s", last)
	}
}

func TestRepairDelay(t *testing.T) {
	setup := func() (*world, *harness) {
		w := newWorld(5)
		for i := range 10 {
			w.add(chunk(i), 1, 1, 2, 3)
		}
		h := newHarness(t, unthrottled(), w, 10*time.Millisecond)
		h.s.Start()
		h.clock.Advance(5 * time.Second)
		w.kill(node(3), h.clock.Now())
		h.s.Scan()
		return w, h
	}

	t.Run("no copy before the delay, all after", func(t *testing.T) {
		_, h := setup()
		h.clock.Advance(19 * time.Second)
		if len(h.copies) != 0 {
			t.Fatalf("%d copies started inside the delay", len(h.copies))
		}
		if st := h.s.Stats(); st.Waiting != 10 {
			t.Fatalf("waiting = %d, want 10", st.Waiting)
		}
		h.clock.Advance(2 * time.Second)
		if st := h.s.Stats(); st.Completed != 10 {
			t.Fatalf("after the delay: %+v", st)
		}
		if first := h.copies[0].Started.Sub(5 * iface.Instant(time.Second)); first != 20*time.Second {
			t.Fatalf("first copy %v after death, want exactly the 20 s delay", first)
		}
	})

	t.Run("node back inside the delay cancels everything", func(t *testing.T) {
		w, h := setup()
		h.clock.Advance(15 * time.Second)
		w.state[node(3)] = detector.Suspect // beating again, not yet alive
		h.s.Scan()
		h.clock.Advance(time.Minute)
		if len(h.copies) != 0 {
			t.Fatalf("%d copies for a node that came back", len(h.copies))
		}
	})

	t.Run("queued work is cancelled at dispatch when the node returns", func(t *testing.T) {
		w, h := setup()
		h.s.cfg.MaxInFlight = 1
		h.copyTime = 5 * time.Second // slow copies keep the rest queued
		h.clock.Advance(21 * time.Second)
		if st := h.s.Stats(); st.InFlight != 1 || st.Queued != 9 {
			t.Fatalf("setup: %+v", st)
		}
		w.state[node(3)] = detector.Alive
		h.clock.Advance(time.Minute)
		if st := h.s.Stats(); st.Dispatched != 1 || st.Cancelled != 9 {
			t.Fatalf("returning node: %+v", st)
		}
	})

	t.Run("last copy is repaired without waiting", func(t *testing.T) {
		w, h := setup()
		w.kill(node(2), h.clock.Now())
		h.s.Scan()
		h.clock.Advance(time.Second)
		// One live copy left: the second copy is made at once, despite two
		// holders dead for under a second. Back at 2 live, the third copy
		// falls under the normal delay again.
		if st := h.s.Stats(); st.Completed != 10 {
			t.Fatalf("last-copy chunks waited: %+v", st)
		}
		h.clock.Advance(20 * time.Second)
		if st := h.s.Stats(); st.Completed != 20 {
			t.Fatalf("third copies after the delay: %+v", st)
		}
	})
}

func TestTimeoutAndFailureRequeue(t *testing.T) {
	w := newWorld(5)
	w.add(chunk(1), 1, 1, 2)
	cfg := unthrottled()
	h := newHarness(t, cfg, w, -1) // targets never report
	h.s.Scan()
	h.clock.Advance(cfg.CopyTimeout + time.Millisecond)
	if st := h.s.Stats(); st.TimedOut != 1 || st.Dispatched != 2 || st.InFlight != 1 {
		t.Fatalf("after timeout: %+v", st)
	}
	c := h.s.InFlight()[0]
	h.s.Failed(c.ID+100, c.Chunk) // wrong ID: a stale failure is ignored
	h.s.Failed(c.ID, c.Chunk)
	if st := h.s.Stats(); st.Failed != 1 || st.Dispatched != 3 {
		t.Fatalf("after failure: %+v", st)
	}
	// A report from a node that is not the target does not complete it.
	if tgt := h.s.InFlight()[0].Target; tgt == node(5) {
		t.Fatalf("test assumes the target is not n05, got %s", tgt)
	}
	h.s.Reported(node(5), []iface.ChunkID{chunk(1)})
	if h.s.Stats().Completed != 0 {
		t.Fatal("completed by a report from the wrong node")
	}
}

func TestLostChunkIsCountedNotCopied(t *testing.T) {
	w := newWorld(3)
	w.add(chunk(1), 1, 1, 2)
	w.kill(node(1), 0)
	w.kill(node(2), 0)
	h := newHarness(t, unthrottled(), w, time.Millisecond)
	h.s.Scan()
	h.clock.Advance(time.Minute)
	if st := h.s.Stats(); st.Lost != 1 || st.Dispatched != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestSourcePrefersAliveOverSuspect(t *testing.T) {
	w := newWorld(5)
	w.add(chunk(1), 1, 1, 2)
	w.state[node(1)] = detector.Suspect
	h := newHarness(t, unthrottled(), w, time.Millisecond)
	h.s.Scan()
	if len(h.copies) != 1 || h.copies[0].Source != node(2) {
		t.Fatalf("copies %+v", h.copies)
	}
}

func TestBucket(t *testing.T) {
	const mib = 1 << 20
	b := NewBucket(10*mib, 4*mib)
	sec := func(s float64) iface.Instant { return iface.Instant(s * float64(time.Second)) }
	steps := []struct {
		at   iface.Instant
		n    int64
		want bool
	}{
		{0, 4 * mib, true},         // full burst
		{0, 1, false},              // empty
		{sec(0.1), mib, true},      // 1 MiB refilled
		{sec(0.1), mib, false},     // spent
		{sec(0.6), 8 * mib, true},  // 4 MiB (the burst) is enough to go into debt
		{sec(0.7), mib, false},     // paying off 4 MiB of debt
		{sec(1.1), 4 * mib, false}, // still short: 1 MiB
		{sec(1.4), 4 * mib, true},  // 4 MiB after 0.8 s
		{sec(100), 4 * mib, true},  // capped at the burst, no overflow
		{sec(100), 1, false},
	}
	for i, s := range steps {
		if got := b.Take(s.n, s.at); got != s.want {
			t.Fatalf("step %d: Take(%d) at %v = %v", i, s.n, s.at, got)
		}
	}
	b = NewBucket(10*mib, 4*mib)
	b.Take(4*mib, 0)
	if at := b.ReadyAt(2*mib, 0); at.Sub(0) < 200*time.Millisecond || at.Sub(0) > 201*time.Millisecond {
		t.Fatalf("ReadyAt = %v, want 200 ms", at.Sub(0))
	}
	if !b.Take(2*mib, b.ReadyAt(2*mib, 0)) {
		t.Fatal("Take failed at ReadyAt")
	}
}

func TestVictim(t *testing.T) {
	h := func(n, rack string, used int64) Holder {
		return Holder{Node: iface.NodeID(n), Rack: rack, Used: used}
	}
	tests := []struct {
		name    string
		holders []Holder
		want    iface.NodeID
	}{
		{"rack with two copies loses one", []Holder{h("a", "r1", 9), h("b", "r2", 5), h("c", "r2", 1), h("d", "r3", 9)}, "b"},
		{"all racks distinct: most used", []Holder{h("a", "r1", 1), h("b", "r2", 7), h("c", "r3", 3), h("d", "r4", 2)}, "b"},
		{"tie on rack and use: highest ID", []Holder{h("a", "r1", 1), h("b", "r2", 1), h("c", "r3", 1), h("d", "r4", 1)}, "d"},
		{"one rack: most used", []Holder{h("a", "r1", 1), h("b", "r1", 2), h("c", "r1", 3), h("d", "r1", 0)}, "c"},
		{"order does not matter", []Holder{h("d", "r3", 9), h("c", "r2", 1), h("b", "r2", 5), h("a", "r1", 9)}, "b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Victim(tc.holders); got != tc.want {
				t.Fatalf("victim %s, want %s", got, tc.want)
			}
		})
	}
}

func TestTrimSafety(t *testing.T) {
	type check struct {
		name  string
		setup func(w *world)
		trims int
		busy  bool // a copy of the chunk is in flight
	}
	for _, tc := range []check{
		{"4 confirmed alive: trim one", func(w *world) {}, 1, false},
		{"suspect holder does not count", func(w *world) { w.state[node(4)] = detector.Suspect }, 0, false},
		{"unconfirmed holder does not count", func(w *world) { w.unconfirmed = map[iface.NodeID]bool{node(4): true} }, 0, false},
		{"not while a copy is in flight", func(w *world) {}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(6)
			w.add(chunk(1), 1, 1, 2, 3, 4)
			tc.setup(w)
			var trims []Trim
			s := New(unthrottled(), sim.NewClock(), w, Sender{Copy: func(Copy) {}, Trim: func(tr Trim) { trims = append(trims, tr) }})
			if tc.busy {
				s.inflight[chunk(1)] = &Copy{Chunk: chunk(1)}
			}
			s.Scan()
			s.Scan() // a second scan must not trim again while the first is pending
			if len(trims) != tc.trims {
				t.Fatalf("trims %+v, want %d", trims, tc.trims)
			}
			if tc.trims == 0 {
				return
			}
			s.Removed(node(9), []iface.ChunkID{chunk(1)}) // wrong node: ignored
			if s.Stats().Trimmed != 0 {
				t.Fatal("trim completed by the wrong node")
			}
			s.Removed(trims[0].Node, []iface.ChunkID{chunk(1)})
			if s.Stats().Trimmed != 1 {
				t.Fatal("trim not completed")
			}
		})
	}
}
