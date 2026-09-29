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

// TestGCDeleteFence: a GC delete removes the copy only if the node has not
// written the chunk since the metadata server's view (fence_seq) and is the
// incarnation the server saw. Every answer goes back as a block report.
func TestGCDeleteFence(t *testing.T) {
	clock := sim.NewClock()
	rng := sim.NewRand(1)
	net := sim.NewNet(clock, rng, sim.Faults{MinDelay: time.Millisecond, MaxDelay: time.Millisecond})
	var reports []*chunkdv1.BlockReport
	net.Listen("meta", func(m iface.Message) {
		if m.Kind == wire.KindBlockReport {
			var r chunkdv1.BlockReport
			if err := wire.Decode(m.Body, &r); err == nil && !r.GetFull() {
				reports = append(reports, &r)
			}
		}
	})
	store := sim.NewBlockStore()
	n := node.New(node.Deps{Clock: clock, Net: net, Async: net.AsyncCaller("n1", time.Second), Store: store, Rand: rng, Log: slog.New(slog.NewTextHandler(io.Discard, nil))},
		node.DefaultConfig("n1", "meta", "r1"))
	n.Start()
	caller := net.NewCaller("client", time.Second)
	data := []byte("chunk")
	id := sha256.Sum256(data)
	put := func() uint64 {
		t.Helper()
		r := caller.Do(context.Background(), []iface.Call{{To: "n1", Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: data})}})
		if r[0].Err != nil {
			t.Fatal(r[0].Err)
		}
		last := reports[len(reports)-1]
		return last.GetSeq()
	}
	del := func(inc, seq uint64) *chunkdv1.BlockReport {
		t.Helper()
		before := len(reports)
		net.Send("n1", iface.Message{From: "meta", Kind: wire.KindDeleteReplica, Body: wire.Marshal(&chunkdv1.DeleteReplica{ChunkId: id[:], Gc: true, FenceIncarnation: inc, FenceSeq: seq})})
		clock.Advance(10 * time.Millisecond)
		if len(reports) != before+1 {
			t.Fatalf("%d reports after a GC delete, want 1", len(reports)-before)
		}
		return reports[len(reports)-1]
	}
	has := func() bool {
		_, err := store.Get(context.Background(), id)
		return err == nil
	}

	seq := put()
	inc := reports[len(reports)-1].GetIncarnation()
	tests := []struct {
		name      string
		inc, seq  uint64
		wantKept  bool
		wantStore bool
	}{
		{"written after the fence: kept", inc, seq - 1, true, true},
		{"another incarnation: kept", inc + 1, seq, true, true},
		{"written at or before the fence: deleted", inc, seq, false, false},
		{"already gone: still answered as deleted", inc, seq, false, false},
	}
	for _, tt := range tests {
		r := del(tt.inc, tt.seq)
		kept := len(r.GetKeptIds()) == 1 && len(r.GetDeletedIds()) == 0
		if kept != tt.wantKept || has() != tt.wantStore {
			t.Fatalf("%s: kept %v, stored %v; want %v, %v", tt.name, kept, has(), tt.wantKept, tt.wantStore)
		}
	}
	// A re-write after a delete moves the chunk past any earlier fence.
	if again := put(); again <= seq {
		t.Fatalf("re-write seq %d not above %d", again, seq)
	}
	if r := del(inc, seq); len(r.GetKeptIds()) != 1 || !has() {
		t.Fatal("delete with the old fence removed a re-written copy")
	}
	if st := n.Stats(); st.GCDeleted != 2 || st.GCKept != 3 {
		t.Fatalf("stats %+v, want 2 deleted, 3 kept", st)
	}
}
