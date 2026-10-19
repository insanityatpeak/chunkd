package meta_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func TestRaftSlot(t *testing.T) {
	for _, tc := range []struct {
		id   uint64
		n    int
		slot int
	}{{1, 3, 1}, {3, 3, 3}, {4, 3, 1}, {5, 3, 2}, {8, 3, 2}, {1, 1, 1}, {7, 1, 1}} {
		if got := meta.RaftSlot(tc.id, tc.n); got != tc.slot {
			t.Errorf("RaftSlot(%d, %d) = %d, want %d", tc.id, tc.n, got, tc.slot)
		}
	}
}

// A peer may start under any generation of its own slot and no other's.
func TestRaftIDMustMatchSlot(t *testing.T) {
	peers := []iface.NodeID{"meta-1", "meta-2", "meta-3"}
	start := func(id iface.NodeID, raftID uint64) error {
		clock := sim.NewClock()
		rng := sim.NewRand(1)
		cfg := meta.DefaultConfig(id)
		cfg.Peers, cfg.RaftID, cfg.Join = peers, raftID, raftID > 3
		_, err := meta.NewServer(context.Background(), meta.Deps{Clock: clock, Net: sim.NewNet(clock, rng, sim.Faults{MaxDelay: time.Millisecond}),
			Store: sim.NewMetaStore(), Rand: rng, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, cfg)
		return err
	}
	for _, ok := range []struct {
		id  iface.NodeID
		raw uint64
	}{{"meta-2", 0}, {"meta-2", 2}, {"meta-2", 5}, {"meta-3", 9}} {
		if err := start(ok.id, ok.raw); err != nil {
			t.Errorf("%s as raft ID %d: %v", ok.id, ok.raw, err)
		}
	}
	if err := start("meta-2", 4); err == nil || !strings.Contains(err.Error(), "belongs to slot 1") {
		t.Errorf("meta-2 as raft ID 4: err = %v, want a slot mismatch", err)
	}
}

// replace sends a ReplacePeerRequest to the leader l.
func (g *groupEnv) replace(t *testing.T, c iface.Caller, l, peer iface.NodeID, raftID uint64, force bool) (*chunkdv1.ReplacePeerResponse, error) {
	t.Helper()
	body := wire.Marshal(&chunkdv1.ReplacePeerRequest{Peer: string(peer), RaftId: raftID, Force: force})
	r := c.Do(context.Background(), []iface.Call{{To: l, Kind: wire.KindReplacePeer, Body: body}})[0]
	if r.Err != nil {
		return nil, r.Err
	}
	var resp chunkdv1.ReplacePeerResponse
	if err := wire.Decode(r.Body, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp, nil
}

// startJoined starts peer over an empty store as raft ID id, in join mode.
func (g *groupEnv) startJoined(t *testing.T, peer iface.NodeID, id uint64) {
	t.Helper()
	cfg := meta.DefaultConfig(peer)
	cfg.Peers, cfg.RaftID, cfg.Join = g.peers, id, true
	srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: g.clock, Net: g.net, Store: sim.NewMetaStore(), Rand: sim.NewRand(9),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	g.srvs[peer] = srv
}

// A peer that lost its log is replaced by its slot's next generation: added,
// then the old ID removed. It joins over an empty store, catches up, and the
// group survives losing another peer again.
func TestReplacePeer(t *testing.T) {
	g := newGroupEnv(t, 3)
	g.clock.Advance(5 * time.Second)
	l := g.leader(t, g.peers...)
	var victim, other iface.NodeID
	for _, p := range g.peers {
		switch {
		case p == l:
		case victim == "":
			victim = p
		default:
			other = p
		}
	}
	slot := uint64(meta.RaftSlot(uint64(slicesIndex(g.peers, victim)+1), 3))
	g.srvs[victim].Stop()
	g.net.Crash(victim)
	g.clock.Advance(70 * time.Second) // epochs commit without it
	l = g.leader(t, l, other)
	if other == l {
		other = g.peers[3-slicesIndex(g.peers, victim)-slicesIndex(g.peers, l)]
	}

	op := g.net.NewCaller("operator", 10*time.Second)
	resp, err := g.replace(t, op, l, victim, slot, false)
	if err != nil || resp.GetRemoved() != slot || resp.GetAdded() != slot+3 {
		t.Fatalf("replace: %v, %v", resp, err)
	}
	if again, err := g.replace(t, op, l, victim, slot, false); err != nil || again.GetAdded() != slot+3 {
		t.Fatalf("repeated replace: %v, %v", again, err)
	}
	if v := g.srvs[l].Raft().Voters; len(v) != 3 || !containsID(v, slot+3) || containsID(v, slot) {
		t.Fatalf("voters %v", v)
	}

	g.net.Restart(victim)
	id, err := meta.AskJoin(context.Background(), g.net.NewCaller(victim, 10*time.Second), victim, g.peers)
	if err != nil || id != slot+3 {
		t.Fatalf("join: %d, %v", id, err)
	}
	g.startJoined(t, victim, id)
	g.clock.Advance(10 * time.Second)
	if a, b := g.srvs[l].Raft().Applied, g.srvs[victim].Raft().Applied; b == 0 || a != b {
		t.Fatalf("joined peer applied %d, leader %d", b, a)
	}
	if !bytes.Equal(g.srvs[l].State().Snapshot(), g.srvs[victim].State().Snapshot()) {
		t.Fatal("the joined peer's state differs from the leader's")
	}
	// Having acked entries, the new member may not join again over an empty store.
	if _, err := meta.AskJoin(context.Background(), op, victim, g.peers); iface.CodeOf(err) != iface.CodeConflict {
		t.Fatalf("second join over an empty store: %v, want a conflict", err)
	}

	g.srvs[other].Stop()
	g.net.Crash(other)
	before := g.srvs[victim].Raft().Applied
	g.clock.Advance(70 * time.Second)
	if !g.srvs[l].Raft().Ready || g.srvs[victim].Raft().Applied <= before {
		t.Fatalf("with %s down the group stopped: leader ready %v, applied %d -> %d", other, g.srvs[l].Raft().Ready, before, g.srvs[victim].Raft().Applied)
	}
}

func TestReplacePeerRefusals(t *testing.T) {
	g := newGroupEnv(t, 3)
	g.clock.Advance(5 * time.Second)
	l := g.leader(t, g.peers...)
	lid := uint64(slicesIndex(g.peers, l) + 1)
	live := g.peers[0]
	if live == l {
		live = g.peers[1]
	}
	lv := uint64(slicesIndex(g.peers, live) + 1)
	op := g.net.NewCaller("operator", 10*time.Second)
	for _, tc := range []struct {
		name string
		peer iface.NodeID
		id   uint64
		code iface.Code
	}{
		{"live peer", live, lv, iface.CodeConflict},
		{"the leader", l, lid, iface.CodeConflict},
		{"wrong slot", live, lv%3 + 1, iface.CodeInvalid},
		{"stale raft ID", live, lv + 6, iface.CodeConflict},
		{"unknown peer", "meta-9", 1, iface.CodeNotFound},
	} {
		if _, err := g.replace(t, op, l, tc.peer, tc.id, false); iface.CodeOf(err) != tc.code {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.code)
		}
	}
	if v := g.srvs[l].Raft().Voters; len(v) != 3 {
		t.Fatalf("a refused replace changed the voters: %v", v)
	}
	if resp, err := g.replace(t, op, l, live, lv, true); err != nil || resp.GetAdded() != lv+3 {
		t.Fatalf("forced replace of a live peer: %v, %v", resp, err)
	}
}

func slicesIndex(s []iface.NodeID, v iface.NodeID) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

func containsID(s []uint64, v uint64) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
