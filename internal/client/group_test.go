package client_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

const chunkSize = 64 << 10

// group is three metadata peers and three storage nodes on a sim network.
type group struct {
	clock  *sim.Clock
	net    *sim.Net
	rng    iface.Rand
	peers  []iface.NodeID
	srvs   map[iface.NodeID]*meta.Server
	caller *sim.Caller
}

func newGroup(t *testing.T, seed uint64) *group {
	t.Helper()
	clock := sim.NewClock()
	rng := sim.NewRand(seed)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	g := &group{clock: clock, rng: rng, peers: []iface.NodeID{"meta-1", "meta-2", "meta-3"}, srvs: map[iface.NodeID]*meta.Server{},
		net: sim.NewNet(clock, rng, sim.Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})}
	for _, id := range g.peers {
		cfg := meta.DefaultConfig(id)
		cfg.Peers, cfg.ChunkSize = g.peers, chunkSize
		srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: clock, Net: g.net, Store: sim.NewMetaStore(), Rand: rng, Log: log}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		srv.Start()
		g.srvs[id] = srv
	}
	for i := 1; i <= 3; i++ {
		id := iface.NodeID(fmt.Sprintf("n%d", i))
		cfg := node.DefaultConfig(id, g.peers[0], fmt.Sprintf("r%d", i))
		cfg.Metas = g.peers
		n, err := node.New(node.Deps{Clock: clock, Net: g.net, Async: g.net.AsyncCaller(id, 10*time.Second), Store: sim.NewBlockStore(), Rand: rng, Log: log}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
	}
	g.caller = g.net.NewCaller("client", 10*time.Second)
	clock.Advance(6 * time.Second) // an election, heartbeats, first reports
	return g
}

func (g *group) leader(t *testing.T) iface.NodeID {
	t.Helper()
	for _, id := range g.peers {
		if g.srvs[id].Raft().Ready {
			return id
		}
	}
	t.Fatal("no ready leader")
	return ""
}

func (g *group) client(order ...iface.NodeID) (*client.Direct, *int) {
	var peers []client.MetaPeer
	for _, id := range order {
		peers = append(peers, client.MetaPeer{ID: id})
	}
	sleeps := new(int)
	return client.New(g.caller, client.Options{Peers: peers, Rand: g.rng, Sleep: func(d time.Duration) { *sleeps++; g.caller.Sleep(d) }}), sleeps
}

// A client that starts at a follower goes straight to the leader the
// follower names, without waiting.
func TestFollowerHintIsFollowed(t *testing.T) {
	g := newGroup(t, 1)
	l := g.leader(t)
	var order []iface.NodeID
	for _, id := range g.peers {
		if id != l {
			order = append(order, id)
		}
	}
	order = append(order, l)
	c, sleeps := g.client(order...)
	if _, err := c.List(context.Background(), "/"); err != nil {
		t.Fatalf("List through a follower: %v", err)
	}
	if *sleeps != 0 {
		t.Fatalf("the client slept %d times before reaching the leader the follower named", *sleeps)
	}
	if _, err := c.Cluster(context.Background(), 0); err != nil {
		t.Fatalf("Cluster: %v", err)
	}
}

// The leader dies while an upload is in flight, at every millisecond for
// the first 40 ms (the begin and first claims) and every 40 ms up to 380 ms
// (chunk puts and the commit). The client retries against
// the new leader, the upload completes exactly once, reads back with its hash
// intact, and no pending upload is left behind by a retried Begin.
func TestPutSurvivesTheLeaderDying(t *testing.T) {
	data := make([]byte, 20*chunkSize)
	sim.NewRand(9).Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
	for i := range data {
		data[i] = byte(i*7 + i>>8)
	}
	want := sha256.Sum256(data)
	interrupted, tried := 0, 0
	defer func() {
		// A kill after the Put returned would prove nothing.
		if interrupted < tried/2 {
			t.Errorf("the leader died mid-upload in only %d of %d runs", interrupted, tried)
		}
	}()
	var kills []time.Duration
	for ms := 1; ms <= 40; ms++ { // every millisecond through the begin and the first claims
		kills = append(kills, time.Duration(ms)*time.Millisecond)
	}
	for ms := 60; ms <= 380; ms += 40 {
		kills = append(kills, time.Duration(ms)*time.Millisecond)
	}
	for _, kill := range kills {
		t.Run(kill.String(), func(t *testing.T) {
			g := newGroup(t, 2)
			old := g.leader(t)
			order := []iface.NodeID{old}
			for _, id := range g.peers {
				if id != old {
					order = append(order, id)
				}
			}
			c, _ := g.client(order...)
			putting := true
			g.clock.AfterFunc(kill, func() {
				if putting {
					interrupted++
				}
				g.srvs[old].Stop()
				g.net.Crash(old)
			})
			tried++
			m, err := c.Put(context.Background(), "/f", bytes.NewReader(data), int64(len(data)), client.PutOptions{})
			putting = false
			if err != nil {
				t.Fatalf("Put with the leader killed %v in: %v", kill, err)
			}
			if m.Version != 1 {
				t.Fatalf("version %d, want 1", m.Version)
			}
			var got bytes.Buffer
			if _, err := c.Get(context.Background(), "/f", &got); err != nil {
				t.Fatalf("Get: %v", err)
			}
			if sha256.Sum256(got.Bytes()) != want {
				t.Fatal("the file reads back with a different hash")
			}
			g.clock.Advance(2 * time.Second)
			for _, id := range g.peers {
				if id == old {
					continue
				}
				if n := g.srvs[id].State().PendingUploads(); n != 0 {
					t.Fatalf("%s holds %d pending uploads after the Put succeeded", id, n)
				}
				if files := g.srvs[id].State().List("/"); len(files) != 1 {
					t.Fatalf("%s lists %d files, want 1", id, len(files))
				}
			}
		})
	}
}
