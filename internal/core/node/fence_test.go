package node_test

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// group is one storage node and a set of metadata peers that record what
// they receive.
type group struct {
	t     *testing.T
	clock *sim.Clock
	net   *sim.Net
	rng   iface.Rand
	store *sim.BlockStore
	metas []iface.NodeID
	got   map[iface.NodeID]map[string][]iface.Message
	n     *node.Node
}

func newGroup(t *testing.T, metas ...iface.NodeID) *group {
	t.Helper()
	clock := sim.NewClock()
	rng := sim.NewRand(1)
	g := &group{t: t, clock: clock, rng: rng, store: sim.NewBlockStore(), metas: metas, got: map[iface.NodeID]map[string][]iface.Message{},
		net: sim.NewNet(clock, rng, sim.Faults{MinDelay: time.Millisecond, MaxDelay: time.Millisecond})}
	for _, m := range metas {
		g.got[m] = map[string][]iface.Message{}
		g.net.Listen(m, func(msg iface.Message) { g.got[m][msg.Kind] = append(g.got[m][msg.Kind], msg) })
	}
	g.start()
	return g
}

// start runs a fresh node process on the group's disk.
func (g *group) start() {
	g.t.Helper()
	cfg := node.DefaultConfig("n1", g.metas[0], "r1")
	cfg.Metas = g.metas
	n, err := node.New(node.Deps{Clock: g.clock, Net: g.net, Async: g.net.AsyncCaller("n1", time.Second), Store: g.store, Rand: g.rng,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, cfg)
	if err != nil {
		g.t.Fatal(err)
	}
	g.n = n
	n.Start()
}

func (g *group) send(from iface.NodeID, kind string, body []byte) {
	g.net.Send("n1", iface.Message{From: from, Kind: kind, Body: body})
	g.clock.Advance(10 * time.Millisecond)
}

func (g *group) ack(from iface.NodeID, leader bool, term uint64) {
	g.send(from, wire.KindHeartbeatAck, wire.Marshal(&chunkdv1.HeartbeatAck{Leader: leader, Term: term}))
}

func (g *group) term() uint64 {
	g.t.Helper()
	term, err := g.store.Term(context.Background())
	if err != nil {
		g.t.Fatal(err)
	}
	return term
}

func (g *group) put(data []byte) iface.ChunkID {
	g.t.Helper()
	id := iface.ChunkID(sha256.Sum256(data))
	if err := g.store.Put(context.Background(), id, data); err != nil {
		g.t.Fatal(err)
	}
	return id
}

func (g *group) has(id iface.ChunkID) bool {
	_, err := g.store.Get(context.Background(), id)
	return err == nil
}

func (g *group) trim(from iface.NodeID, id iface.ChunkID, term uint64) {
	g.send(from, wire.KindDeleteReplica, wire.Marshal(&chunkdv1.DeleteReplica{TrimId: 1, ChunkId: id[:], Term: term}))
}

func TestHeartbeatsAndReportsGoToEveryMeta(t *testing.T) {
	g := newGroup(t, "m1", "m2", "m3")
	g.clock.Advance(3 * time.Second)
	for _, m := range g.metas {
		if n := len(g.got[m][wire.KindHeartbeat]); n < 2 {
			t.Fatalf("%s got %d heartbeats in 3 s, want at least 2", m, n)
		}
	}
	// An incremental report (here a client write) reaches all of them, with
	// the same sequence number: each peer orders reports on its own.
	caller := g.net.NewCaller("client", time.Second)
	data := []byte("chunk")
	id := sha256.Sum256(data)
	if r := caller.Do(context.Background(), []iface.Call{{To: "n1", Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: data})}}); r[0].Err != nil {
		t.Fatal(r[0].Err)
	}
	var seqs []uint64
	for _, m := range g.metas {
		rs := g.got[m][wire.KindBlockReport]
		if len(rs) == 0 {
			t.Fatalf("%s got no block report", m)
		}
		var r chunkdv1.BlockReport
		wire.Decode(rs[len(rs)-1].Body, &r)
		seqs = append(seqs, r.GetSeq())
	}
	if seqs[0] != seqs[1] || seqs[1] != seqs[2] {
		t.Fatalf("report seqs differ across peers: %v", seqs)
	}

	// A peer that asks for a full report is the only one sent it.
	count := func(m iface.NodeID) int {
		n := 0
		for _, msg := range g.got[m][wire.KindBlockReport] {
			var r chunkdv1.BlockReport
			wire.Decode(msg.Body, &r)
			if r.GetFull() {
				n++
			}
		}
		return n
	}
	before := map[iface.NodeID]int{"m1": count("m1"), "m2": count("m2"), "m3": count("m3")}
	g.send("m2", wire.KindHeartbeatAck, wire.Marshal(&chunkdv1.HeartbeatAck{NeedFullReport: true}))
	if count("m2") != before["m2"]+1 || count("m1") != before["m1"] || count("m3") != before["m3"] {
		t.Fatalf("full reports after m2 asked: m1 %d→%d, m2 %d→%d, m3 %d→%d; want only m2 to gain one",
			before["m1"], count("m1"), before["m2"], count("m2"), before["m3"], count("m3"))
	}
}

// A command carries the sending leader's term. Once the node has heard a
// later term (from a leader's ack or a command), everything below it is
// refused, whatever the command.
func TestStaleTermCommandsAreRefused(t *testing.T) {
	g := newGroup(t, "m1", "m2")
	id := g.put([]byte("victim"))

	g.ack("m1", true, 5)
	if g.term() != 5 {
		t.Fatalf("recorded term %d after a leader's ack of 5", g.term())
	}
	// Only a leader's word counts: a follower or a candidate can carry any term.
	g.ack("m2", false, 99)
	if g.term() != 5 {
		t.Fatalf("a non-leader's ack moved the term to %d", g.term())
	}

	g.trim("m2", id, 4) // the deposed leader
	if !g.has(id) || g.n.Stats().Fenced != 1 {
		t.Fatalf("a command of term 4 after 5: chunk present %v, fenced %d; want present and 1", g.has(id), g.n.Stats().Fenced)
	}
	// The same command with no term at all (a peer that predates fencing) too.
	g.trim("m2", id, 0)
	if !g.has(id) || g.n.Stats().Fenced != 2 {
		t.Fatalf("an unfenced command: chunk present %v, fenced %d", g.has(id), g.n.Stats().Fenced)
	}
	g.trim("m1", id, 5)
	if g.has(id) {
		t.Fatal("the current leader's command was refused")
	}

	// A newer term in a command raises the fence for everything after it.
	id2 := g.put([]byte("second"))
	g.trim("m1", id2, 6)
	if g.term() != 6 || g.has(id2) {
		t.Fatalf("term %d, chunk present %v after a term-6 command", g.term(), g.has(id2))
	}
	id3 := g.put([]byte("third"))
	g.trim("m2", id3, 5)
	if !g.has(id3) {
		t.Fatal("a term-5 command got through after term 6")
	}

	// Repair copies and verifications are fenced the same way; a refused
	// copy does not even try to fetch (a failed fetch would report back).
	g.send("m2", wire.KindReplicate, wire.Marshal(&chunkdv1.ReplicateChunk{CopyId: 9, ChunkId: id[:], Source: "ghost", Term: 5}))
	g.clock.Advance(2 * time.Second)
	if n := len(g.got["m1"][wire.KindReplicateFailed]) + len(g.got["m2"][wire.KindReplicateFailed]); n != 0 {
		t.Fatalf("a refused copy still ran: %d failure reports", n)
	}
	g.send("m1", wire.KindReplicate, wire.Marshal(&chunkdv1.ReplicateChunk{CopyId: 10, ChunkId: id[:], Source: "ghost", Term: 6}))
	g.clock.Advance(2 * time.Second)
	if len(g.got["m1"][wire.KindReplicateFailed]) == 0 {
		t.Fatal("the current leader's copy command was not carried out")
	}
}

// The fence is on the node's disk: a restart does not readmit a deposed leader.
func TestFenceSurvivesRestart(t *testing.T) {
	g := newGroup(t, "m1", "m2")
	g.ack("m1", true, 7)
	g.n.Stop()
	g.start()
	id := g.put([]byte("victim"))
	g.trim("m2", id, 6)
	if !g.has(id) || g.n.Stats().Fenced != 1 {
		t.Fatalf("after a restart a term-6 command got through: chunk present %v, fenced %d", g.has(id), g.n.Stats().Fenced)
	}
	// A wiped disk forgets it, and takes the chunks with it.
	g.store = sim.NewBlockStore()
	g.n.Stop()
	g.start()
	id = g.put([]byte("victim"))
	g.trim("m2", id, 6)
	if g.has(id) {
		t.Fatal("a node on a wiped disk kept a fence it cannot have")
	}
}
