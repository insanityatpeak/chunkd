package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// rebuildShard recomputes a lost shard (ADR-0023): it reads sources, the
// first ec.DataShards in parallel and the next one each time a read fails
// or does not verify, or every rebuildHedge while one is outstanding,
// decodes once ec.DataShards have verified, and stores
// the result only if it hashes to the shard ID. Completion is the
// incremental block report; failure is a ReplicateFailed.
// SIMPLIFIED: the decode runs on the event loop. RS(4,2) rebuilds a 1 MiB
// shard in about a millisecond natively; HDFS runs reconstruction on an
// ECWorker thread pool.
func (n *Node) rebuildShard(m iface.Message) {
	var cmd chunkdv1.RebuildShard
	if err := wire.Decode(m.Body, &cmd); err != nil {
		n.d.Log.Warn("bad rebuild command", "err", err)
		return
	}
	if n.fenced(m.Kind, cmd.GetTerm()) {
		return
	}
	id, err := wire.ChunkID(cmd.GetShardId())
	if err != nil {
		return
	}
	fail := func(err error) {
		n.stats.RepairFailed++
		n.d.Log.Warn("shard rebuild failed", "copy", cmd.GetCopyId(), "shard", id.String()[:12], "err", err)
		n.toMetas(wire.KindReplicateFailed,
			wire.Marshal(&chunkdv1.ReplicateFailed{CopyId: cmd.GetCopyId(), ChunkId: id[:], Node: string(n.cfg.ID), Error: err.Error()}))
	}
	logical, err := wire.ChunkID(cmd.GetLogicalId())
	idx, size := int(cmd.GetIndex()), int(cmd.GetChunkSize())
	if err != nil || idx < 0 || idx >= ec.TotalShards || size <= 0 {
		fail(fmt.Errorf("bad rebuild of shard %d of a %d-byte chunk: %v", idx, size, err))
		return
	}
	if n.codec == nil {
		n.codec = ec.New()
	}
	r := &rebuild{n: n, id: id, logical: logical, idx: idx, size: size, sources: cmd.GetSources(),
		shards: make([][]byte, ec.TotalShards), fail: fail}
	r.more()
	r.n.d.Clock.AfterFunc(rebuildHedge, r.hedge)
}

// rebuildHedge is how long a rebuild waits on its first reads before it also
// asks the next source. A lost message costs the call timeout (10 s), the
// scheduler's whole copy timeout; a shard read takes milliseconds.
const rebuildHedge = 2 * time.Second

// rebuild is one shard rebuild in progress. Loop-owned.
type rebuild struct {
	n           *Node
	id, logical iface.ChunkID
	idx, size   int
	sources     []*chunkdv1.ShardSource
	shards      [][]byte
	have, next  int
	pending     int
	hedges      int // extra reads started because the first were slow
	done        bool
	errs        []error
	fail        func(error)
}

// more starts reads until enough are verified or in flight, and gives up
// once nothing is in flight and the sources are exhausted.
func (r *rebuild) more() {
	for r.have+r.pending < ec.DataShards+r.hedges && r.next < len(r.sources) {
		src := r.sources[r.next]
		r.next++
		j := int(src.GetIndex())
		if j < 0 || j >= ec.TotalShards || j == r.idx || r.shards[j] != nil {
			r.errs = append(r.errs, fmt.Errorf("source for shard %d skipped", j))
			continue
		}
		r.pending++
		call := iface.Call{To: iface.NodeID(src.GetNode()), Addr: src.GetAddr(), Kind: wire.KindGetChunk,
			Body: wire.Marshal(&chunkdv1.GetChunkRequest{Id: src.GetShardId()})}
		r.n.d.Async.Go(call, func(res iface.Result) { r.read(src, j, res) })
	}
	if !r.done && r.pending == 0 && r.have < ec.DataShards {
		r.done = true
		r.fail(fmt.Errorf("%d of %d shards read: %w", r.have, ec.DataShards, errors.Join(r.errs...)))
	}
}

func (r *rebuild) read(src *chunkdv1.ShardSource, j int, res iface.Result) {
	r.pending--
	if r.done || r.n.stopped {
		return
	}
	payload, err := r.verify(src, j, res)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("shard %d from %s: %w", j, src.GetNode(), err))
	} else if r.shards[j] == nil {
		r.shards[j] = payload
		r.have++
	}
	if r.have == ec.DataShards {
		r.done = true
		r.finish()
		return
	}
	r.more()
}

func (r *rebuild) verify(src *chunkdv1.ShardSource, j int, res iface.Result) ([]byte, error) {
	if res.Err != nil {
		return nil, res.Err
	}
	var resp chunkdv1.GetChunkResponse
	if err := wire.Decode(res.Body, &resp); err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(resp.GetData()); !bytes.Equal(sum[:], src.GetShardId()) {
		return nil, errors.New("data does not match the shard hash")
	}
	return ec.Payload(resp.GetData(), r.logical, j)
}

// finish decodes, checks the block hashes to the shard ID (sources from
// another stripe or a wrong size would not) and stores it.
func (r *rebuild) finish() {
	payload, err := r.n.codec.Rebuild(r.shards, r.size, r.idx)
	if err != nil {
		r.fail(err)
		return
	}
	block := ec.Block(r.logical, r.idx, payload)
	if sha256.Sum256(block) != r.id {
		r.fail(errors.New("rebuilt shard does not match its ID"))
		return
	}
	seq, err := r.n.write(r.id, func() error { return r.n.d.Store.Put(context.Background(), r.id, block) })
	if err != nil {
		r.fail(err)
		return
	}
	r.n.stats.RepairCopies++
	r.n.report(seq, []iface.ChunkID{r.id}, nil, nil)
}

// hedge starts one more read while the rebuild is still waiting, and again
// every rebuildHedge until it ends or the sources run out.
func (r *rebuild) hedge() {
	if r.done || r.n.stopped || r.next >= len(r.sources) {
		return
	}
	r.hedges++
	r.more()
	r.n.d.Clock.AfterFunc(rebuildHedge, r.hedge)
}
