package consensus

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// kv is a log-recording FSM: what it applied, in order.
type kv struct{ log []string }

func (f *kv) Apply(i uint64, d []byte) any { f.log = append(f.log, string(d)); return i }
func (f *kv) Snapshot() []byte             { return []byte(strings.Join(f.log, "\x00")) }
func (f *kv) Restore(b []byte) error {
	f.log = nil
	if len(b) > 0 {
		f.log = strings.Split(string(b), "\x00")
	}
	return nil
}

func name(id uint64) iface.NodeID { return iface.NodeID(fmt.Sprintf("meta-%d", id)) }

type group struct {
	t      *testing.T
	clock  *sim.Clock
	net    *sim.Net
	rng    iface.Rand
	stores map[uint64]*sim.MetaStore
	fsms   map[uint64]*kv
	nodes  map[uint64]*Node
	mut    func(*Config)
}

func newGroup(t *testing.T, seed uint64, faults sim.Faults, mut func(*Config), ids ...uint64) *group {
	t.Helper()
	clock := sim.NewClock()
	rng := sim.NewRand(seed)
	g := &group{t: t, clock: clock, net: sim.NewNet(clock, rng, faults), rng: rng, stores: map[uint64]*sim.MetaStore{},
		fsms: map[uint64]*kv{}, nodes: map[uint64]*Node{}, mut: mut}
	for _, id := range ids {
		g.start(id, ids)
	}
	return g
}

// start builds (or rebuilds after a stop) peer id on its store.
func (g *group) start(id uint64, peers []uint64) *Node {
	g.t.Helper()
	if g.stores[id] == nil {
		g.stores[id] = sim.NewMetaStore()
	}
	g.fsms[id] = &kv{}
	cfg := DefaultConfig(id, name, peers)
	cfg.SnapshotEvery, cfg.Trailing = 50, 10
	if g.mut != nil {
		g.mut(&cfg)
	}
	d := Deps{Clock: g.clock, Net: g.net, Store: g.stores[id], Rand: g.rng, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	n, err := New(context.Background(), d, cfg, g.fsms[id])
	if err != nil {
		g.t.Fatal(err)
	}
	g.nodes[id] = n
	g.net.Listen(name(id), func(m iface.Message) {
		if m.Kind == wire.KindRaft && g.nodes[id] != nil {
			g.nodes[id].Receive(m.From, m.Body)
		}
	})
	g.net.Restart(name(id))
	n.Start()
	return n
}

// kill stops peer id and cuts it off; its store survives.
func (g *group) kill(id uint64) {
	g.nodes[id].Stop()
	g.nodes[id] = nil
	g.net.Crash(name(id))
}

func (g *group) run(d time.Duration) { g.clock.Advance(d) }

func (g *group) leader() *Node {
	var l *Node
	for _, n := range g.nodes {
		if n != nil && n.Status().Ready {
			if l != nil {
				g.t.Fatalf("two ready leaders: %d and %d", l.cfg.ID, n.cfg.ID)
			}
			l = n
		}
	}
	return l
}

func (g *group) waitLeader() *Node {
	g.t.Helper()
	for range 100 {
		g.run(100 * time.Millisecond)
		if l := g.leader(); l != nil {
			return l
		}
	}
	g.t.Fatal("no leader after 10 s")
	return nil
}

type result struct {
	done bool
	val  any
	err  error
}

func (g *group) propose(n *Node, data string) *result {
	r := &result{}
	n.Propose([]byte(data), func(v any, err error) { r.done, r.val, r.err = true, v, err })
	return r
}

func logsEqual(t *testing.T, g *group, ids ...uint64) {
	t.Helper()
	for _, id := range ids[1:] {
		if !slices.Equal(g.fsms[ids[0]].log, g.fsms[id].log) {
			t.Fatalf("peer %d applied %d entries, peer %d applied %d; first difference at %d",
				ids[0], len(g.fsms[ids[0]].log), id, len(g.fsms[id].log), firstDiff(g.fsms[ids[0]].log, g.fsms[id].log))
		}
	}
}

func firstDiff(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func TestElectsOneLeader(t *testing.T) {
	g := newGroup(t, 1, sim.Faults{}, nil, 1, 2, 3)
	l := g.waitLeader()
	g.run(3 * time.Second)
	if g.leader() != l {
		t.Fatal("leadership moved with no fault")
	}
	st := l.Status()
	for id, n := range g.nodes {
		s := n.Status()
		if s.Leader != st.ID || s.Term != st.Term {
			t.Fatalf("peer %d sees leader %d term %d, want %d term %d", id, s.Leader, s.Term, st.ID, st.Term)
		}
	}
}

func TestReplicatesInOrder(t *testing.T) {
	g := newGroup(t, 2, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}, nil, 1, 2, 3)
	l := g.waitLeader()
	var rs []*result
	for i := range 20 {
		rs = append(rs, g.propose(l, fmt.Sprintf("e%d", i)))
	}
	g.run(time.Second)
	prev := uint64(0)
	for i, r := range rs {
		if !r.done || r.err != nil {
			t.Fatalf("proposal %d: %+v", i, r)
		}
		if idx := r.val.(uint64); idx <= prev {
			t.Fatalf("proposal %d applied at %d, after %d", i, idx, prev)
		} else {
			prev = idx
		}
	}
	logsEqual(t, g, 1, 2, 3)
	if len(g.fsms[1].log) != 20 || g.fsms[1].log[7] != "e7" {
		t.Fatalf("log = %v", g.fsms[1].log)
	}
}

func TestFollowerRefusesAndNamesTheLeader(t *testing.T) {
	g := newGroup(t, 3, sim.Faults{}, nil, 1, 2, 3)
	l := g.waitLeader()
	var f *Node
	for _, n := range g.nodes {
		if n != l {
			f = n
		}
	}
	r := g.propose(f, "x")
	if !r.done || iface.CodeOf(r.err) != iface.CodeNotLeader || r.err.(*iface.Error).Msg != string(name(l.cfg.ID)) {
		t.Fatalf("propose on a follower: %+v, want NotLeader naming %s", r, name(l.cfg.ID))
	}
	var rerr error
	called := false
	f.ReadIndex(func(err error) { called, rerr = true, err })
	if !called || iface.CodeOf(rerr) != iface.CodeNotLeader {
		t.Fatalf("ReadIndex on a follower: called=%v err=%v", called, rerr)
	}
}

func TestLeaderKilledAndRestarted(t *testing.T) {
	g := newGroup(t, 4, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}, nil, 1, 2, 3)
	l := g.waitLeader()
	for i := range 5 {
		g.propose(l, fmt.Sprintf("a%d", i))
	}
	g.run(time.Second)
	old := l.cfg.ID
	g.kill(old)
	l = g.waitLeader()
	if l.cfg.ID == old {
		t.Fatal("dead peer still leads")
	}
	r := g.propose(l, "b")
	g.run(time.Second)
	if !r.done || r.err != nil {
		t.Fatalf("proposal after failover: %+v", r)
	}
	g.start(old, []uint64{1, 2, 3})
	g.run(3 * time.Second)
	logsEqual(t, g, 1, 2, 3)
	if n := len(g.fsms[old].log); n != 6 {
		t.Fatalf("restarted peer applied %d entries, want 6", n)
	}
}

// The dashboard's group view and timeline: the leader reports every voter's
// match index and last contact, a follower only itself, and a follower that
// stops hearing from the leader reports the loss once before campaigning.
func TestPeersAndLeaderLost(t *testing.T) {
	g := newGroup(t, 6, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}, nil, 1, 2, 3)
	type loss struct {
		by, leader iface.NodeID
		silent     time.Duration
	}
	var lost []loss
	for id, n := range g.nodes {
		n.OnLeaderLost(func(leader iface.NodeID, silent time.Duration) { lost = append(lost, loss{name(id), leader, silent}) })
	}
	l := g.waitLeader()
	for i := range 3 {
		g.propose(l, fmt.Sprintf("a%d", i))
	}
	g.run(time.Second)
	if len(lost) != 0 {
		t.Fatalf("leader lost reported with a working leader: %+v", lost)
	}
	if st := l.Status(); st.Role != "leader" {
		t.Fatalf("leader's role %q", st.Role)
	}
	peers := l.Peers()
	if len(peers) != 3 {
		t.Fatalf("leader sees %d peers, want 3", len(peers))
	}
	for _, p := range peers {
		if p.Match != l.lastIndex() || !p.Heard || p.HeardAgo >= time.Second {
			t.Errorf("leader's view of %s: %+v, want match %d heard recently", p.Node, p, l.lastIndex())
		}
	}
	for _, n := range g.nodes {
		if n != l {
			if ps := n.Peers(); len(ps) != 1 || ps[0].ID != n.cfg.ID {
				t.Errorf("follower %d tracks %+v, want only itself", n.cfg.ID, ps)
			}
			if r := n.Status().Role; r != "follower" {
				t.Errorf("follower %d role %q", n.cfg.ID, r)
			}
		}
	}

	old := l.cfg.ID
	g.kill(old)
	nl := g.waitLeader()
	g.run(5 * time.Second)
	if len(lost) == 0 || len(lost) > 2 {
		t.Fatalf("leader loss reported %d times: %+v", len(lost), lost)
	}
	for _, x := range lost {
		if x.leader != name(old) || x.silent < time.Second || x.by == name(old) {
			t.Errorf("bad report %+v", x)
		}
	}
	for _, p := range nl.Peers() {
		if p.ID == old && p.Heard && p.HeardAgo < 5*time.Second {
			t.Errorf("new leader heard from the dead peer %v ago", p.HeardAgo)
		}
	}
}

// The deposed leader accepts a proposal into its own log while cut off. It
// must never commit, and once healed it must adopt the new leader's log.
func TestStaleLeaderCannotCommit(t *testing.T) {
	g := newGroup(t, 5, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}, nil, 1, 2, 3)
	old := g.waitLeader()
	g.propose(old, "before")
	g.run(time.Second)
	var others []iface.NodeID
	for id := range g.nodes {
		if id != old.cfg.ID {
			others = append(others, name(id))
		}
	}
	g.net.Partition([]iface.NodeID{name(old.cfg.ID)}, others)
	stale := g.propose(old, "stale")
	g.run(4 * time.Second)
	if old.Status().IsLeader {
		t.Fatal("cut-off leader still reports IsLeader")
	}
	var l *Node
	for _, n := range g.nodes {
		if n != old && n.Status().Ready {
			l = n
		}
	}
	if l == nil {
		t.Fatal("majority side elected nobody")
	}
	fresh := g.propose(l, "fresh")
	g.run(time.Second)
	if !fresh.done || fresh.err != nil {
		t.Fatalf("majority-side proposal: %+v", fresh)
	}
	if slices.Contains(g.fsms[l.cfg.ID].log, "stale") {
		t.Fatal("the stale leader's entry committed")
	}
	g.net.Heal()
	g.run(6 * time.Second)
	if !stale.done || stale.err == nil {
		t.Fatalf("stale proposal: %+v, want an error", stale)
	}
	logsEqual(t, g, 1, 2, 3)
	if !slices.Equal(g.fsms[1].log, []string{"before", "fresh"}) {
		t.Fatalf("log = %v, want [before fresh]", g.fsms[1].log)
	}
}

func TestMinorityRejectsWritesAndReads(t *testing.T) {
	g := newGroup(t, 6, sim.Faults{}, nil, 1, 2, 3)
	l := g.waitLeader()
	var others []iface.NodeID
	for id := range g.nodes {
		if id != l.cfg.ID {
			others = append(others, name(id))
		}
	}
	g.net.Partition([]iface.NodeID{name(l.cfg.ID)}, others)
	g.run(2 * time.Second)
	if r := g.propose(l, "x"); !r.done || iface.CodeOf(r.err) != iface.CodeNotLeader {
		t.Fatalf("write on the minority side: %+v, want NotLeader", r)
	}
	var err error
	l.ReadIndex(func(e error) { err = e })
	if iface.CodeOf(err) != iface.CodeNotLeader {
		t.Fatalf("read on the minority side: %v, want NotLeader", err)
	}
}

// A read confirmed by ReadIndex sees every write acknowledged before it.
func TestReadIndexSeesAcknowledgedWrites(t *testing.T) {
	g := newGroup(t, 7, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}, nil, 1, 2, 3)
	l := g.waitLeader()
	for i := range 10 {
		r := g.propose(l, fmt.Sprintf("w%d", i))
		for range 100 {
			if r.done {
				break
			}
			g.run(10 * time.Millisecond)
		}
		if !r.done || r.err != nil {
			t.Fatalf("write %d: %+v", i, r)
		}
		seen, done := -1, false
		l.ReadIndex(func(err error) {
			if err != nil {
				t.Errorf("read %d: %v", i, err)
			}
			seen, done = len(g.fsms[l.cfg.ID].log), true
		})
		for range 100 {
			if done {
				break
			}
			g.run(10 * time.Millisecond)
		}
		if !done || seen < i+1 {
			t.Fatalf("read after write %d done=%v saw %d entries", i, done, seen)
		}
	}
}

// The log stays bounded, a peer restarted from its snapshot resumes, and a
// follower that missed the compacted entries catches up from a snapshot.
func TestSnapshotBoundsLogAndCatchesUp(t *testing.T) {
	g := newGroup(t, 8, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond}, nil, 1, 2, 3)
	l := g.waitLeader()
	lag := uint64(1)
	if l.cfg.ID == lag {
		lag = 2
	}
	var others []iface.NodeID
	for id := range g.nodes {
		if id != lag {
			others = append(others, name(id))
		}
	}
	g.net.Partition([]iface.NodeID{name(lag)}, others)
	const n = 1000
	for i := range n {
		g.propose(l, fmt.Sprintf("e%04d", i))
		if i%10 == 9 {
			g.run(100 * time.Millisecond)
		}
	}
	g.run(2 * time.Second)
	for id, st := range g.stores {
		if id == lag {
			continue
		}
		var held int
		_ = st.Replay(context.Background(), 0, func(iface.Index, []byte) error { held++; return nil })
		if held > 2*50 {
			t.Fatalf("peer %d holds %d log entries after %d proposals with a snapshot every 50", id, held, n)
		}
	}
	g.net.Heal()
	g.run(5 * time.Second)
	logsEqual(t, g, 1, 2, 3)
	if len(g.fsms[lag].log) != n {
		t.Fatalf("lagging peer applied %d of %d", len(g.fsms[lag].log), n)
	}
	if g.nodes[lag].Status().SnapshotsInstalled == 0 {
		t.Fatal("lagging peer caught up without a snapshot")
	}
	// Restart every peer: each recovers from its snapshot plus tail.
	for id := range g.nodes {
		g.kill(id)
	}
	for id := range g.nodes {
		g.start(id, []uint64{1, 2, 3})
	}
	l = g.waitLeader()
	g.run(2 * time.Second)
	logsEqual(t, g, 1, 2, 3)
	if len(g.fsms[1].log) != n {
		t.Fatalf("after a full restart peers applied %d of %d", len(g.fsms[1].log), n)
	}
	if r := g.propose(l, "after"); true {
		g.run(time.Second)
		if !r.done || r.err != nil {
			t.Fatalf("proposal after full restart: %+v", r)
		}
	}
}

func TestAddAndRemoveVoter(t *testing.T) {
	g := newGroup(t, 9, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond}, nil, 1, 2, 3)
	l := g.waitLeader()
	for i := range 120 {
		g.propose(l, fmt.Sprintf("e%d", i))
	}
	g.run(2 * time.Second)
	g.start(4, nil)
	var err error
	done := false
	l.ChangeMembership(true, 4, func(e error) { err, done = e, true })
	g.run(3 * time.Second)
	if !done || err != nil {
		t.Fatalf("add voter: done=%v err=%v", done, err)
	}
	logsEqual(t, g, 1, 2, 3, 4)
	if v := l.Status().Voters; len(v) != 4 {
		t.Fatalf("voters = %v", v)
	}
	// Replace a dead voter: 3 is removed while down.
	dead := uint64(3)
	if l.cfg.ID == dead {
		dead = 2
	}
	g.kill(dead)
	g.run(time.Second)
	done, err = false, nil
	g.leader().ChangeMembership(false, dead, func(e error) { err, done = e, true })
	g.run(3 * time.Second)
	if !done || err != nil {
		t.Fatalf("remove voter: done=%v err=%v", done, err)
	}
	if v := g.leader().Status().Voters; len(v) != 3 || slices.Contains(v, dead) {
		t.Fatalf("voters = %v", v)
	}
	r := g.propose(g.leader(), "after")
	g.run(time.Second)
	if !r.done || r.err != nil {
		t.Fatalf("proposal after replacing a voter: %+v", r)
	}
}

// A peer that lost contact campaigns forever but cannot win, and cannot push
// the group into a new term when the link heals.
func TestIsolatedFollowerDoesNotDisruptTheLeader(t *testing.T) {
	g := newGroup(t, 10, sim.Faults{}, nil, 1, 2, 3)
	l := g.waitLeader()
	var f uint64
	for id := range g.nodes {
		if id != l.cfg.ID {
			f = id
		}
	}
	term := l.Status().Term
	var rest []iface.NodeID
	for id := range g.nodes {
		if id != f {
			rest = append(rest, name(id))
		}
	}
	g.net.Partition([]iface.NodeID{name(f)}, rest)
	g.run(20 * time.Second)
	g.net.Heal()
	g.run(5 * time.Second)
	if g.leader() != l || l.Status().Term != term {
		t.Fatalf("leader or term changed: leader %v term %d, was %d", g.leader() == l, l.Status().Term, term)
	}
	if g.nodes[f].Status().Leader != l.cfg.ID {
		t.Fatal("the isolated peer did not rejoin")
	}
}

// Same seed, same outcome, message loss and duplication included.
func TestReplaysFromSeed(t *testing.T) {
	run := func() string {
		g := newGroup(t, 11, sim.Faults{DropRate: 0.05, DupRate: 0.02, MinDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond}, nil, 1, 2, 3)
		for i := range 40 {
			if l := g.leader(); l != nil {
				g.propose(l, fmt.Sprintf("e%d", i))
			}
			g.run(300 * time.Millisecond)
		}
		g.run(3 * time.Second)
		l := g.leader()
		return fmt.Sprintf("leader %d term %d log %v", l.cfg.ID, l.Status().Term, g.fsms[l.cfg.ID].log)
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("runs differ:\n%s\n%s", a, b)
	}
}

// Random partitions, crashes and lossy links while clients propose: every
// acknowledged proposal survives, in one order, on every peer.
func TestChaosAckedProposalsSurvive(t *testing.T) {
	for seed := uint64(100); seed < 130; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			g := newGroup(t, seed, sim.Faults{DropRate: 0.03, DupRate: 0.02, MinDelay: time.Millisecond, MaxDelay: 30 * time.Millisecond}, nil, 1, 2, 3)
			rng := sim.NewRand(seed * 7)
			var acked []string
			var results []*result
			var vals []string
			downs := map[uint64]bool{}
			for step := range 60 {
				switch rng.IntN(6) {
				case 0:
					a, b := uint64(1+rng.IntN(3)), uint64(1+rng.IntN(3))
					if a != b {
						g.net.Partition([]iface.NodeID{name(a)}, []iface.NodeID{name(b)})
					}
				case 1:
					g.net.Heal()
				case 2:
					if id := uint64(1 + rng.IntN(3)); g.nodes[id] != nil && len(downs) == 0 {
						g.kill(id)
						downs[id] = true
					}
				case 3:
					for id := range downs {
						g.start(id, []uint64{1, 2, 3})
						delete(downs, id)
					}
				}
				for _, n := range g.nodes {
					if n != nil && n.Status().Ready {
						v := fmt.Sprintf("s%d-%d", seed, step)
						results, vals = append(results, g.propose(n, v)), append(vals, v)
						break
					}
				}
				g.run(time.Duration(200+rng.IntN(600)) * time.Millisecond)
			}
			g.net.Heal()
			for id := range downs {
				g.start(id, []uint64{1, 2, 3})
			}
			g.run(15 * time.Second)
			for i, r := range results {
				if r.done && r.err == nil {
					acked = append(acked, vals[i])
				}
			}
			logsEqual(t, g, 1, 2, 3)
			for _, v := range acked {
				if !slices.Contains(g.fsms[1].log, v) {
					t.Fatalf("acknowledged %s is missing from the log %v", v, g.fsms[1].log)
				}
			}
			seen := map[string]bool{}
			for _, v := range g.fsms[1].log {
				if seen[v] {
					t.Fatalf("%s applied twice", v)
				}
				seen[v] = true
			}
		})
	}
}
