package meta_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// dropFirstReport is a node's network that loses its first incremental
// block report.
type dropFirstReport struct {
	*sim.Net
	dropped bool
}

func (d *dropFirstReport) Send(to iface.NodeID, m iface.Message) {
	var r chunkdv1.BlockReport
	if !d.dropped && m.Kind == wire.KindBlockReport && wire.Decode(m.Body, &r) == nil && !r.GetFull() {
		d.dropped = true
		return
	}
	d.Net.Send(to, m)
}

// Every node loses the incremental report of its copy of the one chunk, so
// the commit has no reported copy to count. Each node's next heartbeats
// list the report as unconfirmed, the server says it never got it, and the
// node sends it again: the commit goes through within seconds, not at the
// next full report 30 s later (ADR-0033).
func TestLostIncrementalReportIsResent(t *testing.T) {
	e := newEnv(t, 0)
	var nodes []*node.Node
	for i := 1; i <= 3; i++ {
		id := iface.NodeID(fmt.Sprintf("n%d", i))
		cfg := node.DefaultConfig(id, "meta", fmt.Sprintf("r%d", i))
		n, err := node.New(node.Deps{Clock: e.clock, Net: &dropFirstReport{Net: e.net}, Async: e.net.AsyncCaller(id, 10*time.Second),
			Store: sim.NewBlockStore(), Rand: e.rng, Log: e.log}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
		nodes = append(nodes, n)
	}
	e.clock.Advance(3 * time.Second) // heartbeats and the first full reports
	data := []byte("abcd")           // one chunk: the env's chunks are 4 bytes
	body, err := e.rpc(t, "meta", wire.KindBegin, wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/f", Size: int64(len(data))}))
	if err != nil {
		t.Fatal(err)
	}
	var begin chunkdv1.BeginUploadResponse
	wire.Decode(body, &begin)
	id := sha256.Sum256(data)
	if _, err := e.rpc(t, "meta", wire.KindClaim, wire.Marshal(&chunkdv1.ClaimChunksRequest{UploadId: begin.GetUploadId(),
		Claims: []*chunkdv1.ChunkClaim{{Index: 0, Id: id[:]}}})); err != nil {
		t.Fatal(err)
	}
	var calls []iface.Call
	for _, r := range begin.GetPlacement()[0].GetReplicas() {
		calls = append(calls, iface.Call{To: iface.NodeID(r.GetNode()), Kind: wire.KindPutChunk, Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: id[:], Data: data})})
	}
	e.caller.Do(context.Background(), calls)
	start := e.clock.Now()
	commit := wire.Marshal(&chunkdv1.CommitUploadRequest{UploadId: begin.GetUploadId(), ChunkIds: [][]byte{id[:]}, Sha256: id[:]})
	for {
		if _, err = e.rpc(t, "meta", wire.KindCommit, commit); iface.CodeOf(err) != iface.CodeRetry {
			break
		}
		if e.clock.Now().Sub(start) > 20*time.Second {
			break
		}
		e.clock.Advance(100 * time.Millisecond)
	}
	took := e.clock.Now().Sub(start)
	if err != nil {
		t.Fatalf("commit after %v: %v", took, err)
	}
	if took > 5*time.Second {
		t.Fatalf("commit took %v with lost incremental reports, want a few heartbeats", took)
	}
	resent := uint64(0)
	for _, n := range nodes {
		resent += n.Stats().ReportsResent
	}
	if resent == 0 {
		t.Fatal("no report was resent")
	}
	t.Logf("committed %v after the writes; %d reports resent", took, resent)
}
