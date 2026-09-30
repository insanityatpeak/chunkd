package meta_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// groupEnv is three metadata peers and storage nodes that heartbeat and
// report to all of them.
type groupEnv struct {
	clock *sim.Clock
	net   *sim.Net
	peers []iface.NodeID
	srvs  map[iface.NodeID]*meta.Server
	disks []*sim.BlockStore
}

func newGroupEnv(t *testing.T, nodes int) *groupEnv {
	t.Helper()
	clock := sim.NewClock()
	rng := sim.NewRand(4)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	g := &groupEnv{clock: clock, peers: []iface.NodeID{"meta-1", "meta-2", "meta-3"}, srvs: map[iface.NodeID]*meta.Server{},
		net: sim.NewNet(clock, rng, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})}
	for _, id := range g.peers {
		cfg := meta.DefaultConfig(id)
		cfg.Peers = g.peers
		srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: clock, Net: g.net, Store: sim.NewMetaStore(), Rand: rng, Log: log}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		srv.Start()
		g.srvs[id] = srv
	}
	for i := 1; i <= nodes; i++ {
		id := iface.NodeID(fmt.Sprintf("n%d", i))
		disk := sim.NewBlockStore()
		g.disks = append(g.disks, disk)
		cfg := node.DefaultConfig(id, g.peers[0], fmt.Sprintf("r%d", i))
		cfg.Metas = g.peers
		n, err := node.New(node.Deps{Clock: clock, Net: g.net, Async: g.net.AsyncCaller(id, 10*time.Second), Store: disk, Rand: rng, Log: log}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
	}
	return g
}

func (g *groupEnv) leader(t *testing.T, among ...iface.NodeID) iface.NodeID {
	t.Helper()
	var l iface.NodeID
	for _, id := range among {
		if g.srvs[id].Raft().Ready {
			if l != "" {
				t.Fatalf("two ready leaders: %s and %s", l, id)
			}
			l = id
		}
	}
	if l == "" {
		t.Fatal("no ready leader")
	}
	return l
}

func (g *groupEnv) has(id iface.ChunkID) int {
	n := 0
	for _, d := range g.disks {
		if _, err := d.Get(context.Background(), id); err == nil {
			n++
		}
	}
	return n
}

// A leader cut off from its peers must not authorize or send a GC delete,
// though its nodes still talk to it. The peers elect another, which logs the
// intent under its own term and finishes the job; every peer ends with the
// same state, with nothing left pending.
func TestDeposedLeaderSendsNoGCDeletes(t *testing.T) {
	g := newGroupEnv(t, 3)
	g.clock.Advance(5 * time.Second)
	old := g.leader(t, g.peers...)

	data := []byte("an orphan: written to every disk, referenced by nothing")
	orphan := iface.ChunkID(sha256.Sum256(data))
	for _, d := range g.disks {
		if err := d.Put(context.Background(), orphan, data); err != nil {
			t.Fatal(err)
		}
	}

	g.clock.Advance(40 * time.Second) // full reports arrive; the old leader's first epochs pass
	var rest []iface.NodeID
	for _, id := range g.peers {
		if id != old {
			rest = append(rest, id)
		}
	}
	g.net.Partition([]iface.NodeID{old}, rest)
	sentBefore := g.srvs[old].GC().Sent

	g.clock.Advance(250 * time.Second)
	nl := g.leader(t, rest...)
	if g.srvs[old].Raft().Ready {
		t.Fatal("the cut-off leader still thinks it leads")
	}
	if got := g.srvs[old].GC().Sent; got != sentBefore {
		t.Fatalf("the deposed leader sent %d GC deletes", got-sentBefore)
	}
	if n := g.has(orphan); n != 0 {
		t.Fatalf("the new leader left the orphan on %d disks after 250 s", n)
	}
	term := g.srvs[nl].Raft().Term
	for i, d := range g.disks {
		if got, _ := d.Term(context.Background()); got != term {
			t.Fatalf("node %d fenced at term %d, the new leader is at %d", i+1, got, term)
		}
	}

	g.net.Heal()
	g.clock.Advance(45 * time.Second)
	for _, id := range g.peers {
		if p := g.srvs[id].State().GCPendingAll(); len(p) != 0 {
			t.Fatalf("%s still has %d pending GC deletes", id, len(p))
		}
	}
	base := string(g.srvs[nl].State().Snapshot())
	for _, id := range g.peers {
		if string(g.srvs[id].State().Snapshot()) != base {
			t.Fatalf("%s's state differs from the leader's", id)
		}
	}
}
