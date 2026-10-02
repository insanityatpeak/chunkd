package client

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// putStripe encodes one chunk and sends shard i to placement node i, all in
// one batch, retrying failed puts once. It fails if fewer than min shards
// are stored (ADR-0022).
// SIMPLIFIED: shards go in one parallel batch. HDFS-EC streams cells to the
// shard writers in a pipeline as the client produces them.
func (c *Direct) putStripe(ctx context.Context, ch chunk.Chunk, shards []ec.Shard, pl *chunkdv1.ChunkPlacement, min int) (ChunkRef, error) {
	reps := pl.GetReplicas()
	if len(reps) != len(shards) {
		return ChunkRef{}, iface.Errorf(iface.CodeInternal, "chunk %d: %d nodes placed for %d shards", ch.Index, len(reps), len(shards))
	}
	logical := ec.LogicalID(ch.ID)
	ref := ChunkRef{Index: ch.Index, ID: logical.String(), Size: int64(len(ch.Data))}
	todo := make([]int, len(shards))
	for i := range todo {
		todo[i] = i
		ref.Shards = append(ref.Shards, ShardRef{Index: i, ID: shards[i].ID.String(), Replicas: []string{}})
	}
	var errs []error
	stored := 0
	for round := 0; round < 2 && len(todo) > 0; round++ {
		errs = errs[:0]
		calls := make([]iface.Call, len(todo))
		for k, i := range todo {
			calls[k] = iface.Call{To: iface.NodeID(reps[i].GetNode()), Addr: reps[i].GetAddr(), Kind: wire.KindPutChunk,
				Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: shards[i].ID[:], Data: shards[i].Block})}
		}
		var failed []int
		for k, res := range c.caller.Do(ctx, calls) {
			i := todo[k]
			if res.Err != nil {
				errs = append(errs, fmt.Errorf("shard %d on %s: %w", i, calls[k].To, res.Err))
				failed = append(failed, i)
				continue
			}
			ref.Shards[i].Replicas = []string{string(calls[k].To)}
			stored++
		}
		todo = failed
	}
	if stored < min {
		return ChunkRef{}, iface.Errorf(iface.CodeUnavailable, "chunk %d: %d of %d required shards stored: %v", ch.Index, stored, min, errors.Join(errs...))
	}
	return ref, nil
}

// fetchStripe reads one erasure-coded chunk: any 4 shards, data shards on
// alive nodes first, then parity, then shards only a suspect node holds.
// A shard that fails, is missing or does not verify is replaced by the next
// one in that order; once none is left, a shard that timed out is asked
// again (a lost message is not a lost shard). Decoding is needed only when
// a data shard is not used. The result is checked against the stripe ID (ADR-0022).
// SIMPLIFIED: no hedging within a stripe: a slow shard costs its timeout
// before the next is asked for. HDFS-EC's striped reader hedges per cell.
func (c *Direct) fetchStripe(ctx context.Context, loc *chunkdv1.ChunkLocation, ref *ChunkRef) ([]byte, error) {
	logical, err := wire.ChunkID(loc.GetId())
	if err != nil {
		return nil, err
	}
	locs := loc.GetShards()
	if len(locs) != ec.TotalShards {
		return nil, iface.Errorf(iface.CodeInternal, "chunk %d: %d shard locations, want %d", ref.Index, len(locs), ec.TotalShards)
	}
	size := int(loc.GetSize())
	payloads := make([][]byte, ec.TotalShards)
	tries := make([]map[string]int, ec.TotalShards)
	for j := range tries {
		tries[j] = map[string]int{}
	}
	var errs []error
	have := 0
	for have < ec.DataShards {
		calls, idx := c.nextShards(locs, payloads, tries, ec.DataShards-have)
		if len(calls) == 0 {
			break
		}
		var mismatch []iface.Call
		for k, res := range c.caller.Do(ctx, calls) {
			j, node := idx[k], string(calls[k].To)
			data, err := shardPayload(res, locs[j].GetId(), logical, j)
			c.health.observe(node, res, err == nil)
			sr := &ref.Shards[j]
			if err != nil {
				errs = append(errs, fmt.Errorf("shard %d on %s: %w", j, node, err))
				if code := iface.CodeOf(err); code != iface.CodeUnavailable && code != iface.CodeRetry {
					tries[j][node] = shardTries // not worth asking again
				}
				if errors.Is(err, errShardMismatch) || iface.CodeOf(err) == iface.CodeCorrupt {
					sr.Rejected = append(sr.Rejected, node)
				}
				if errors.Is(err, errShardMismatch) {
					mismatch = append(mismatch, calls[k])
				}
				continue
			}
			payloads[j], sr.ServedBy = data, node
			have++
		}
		for _, call := range mismatch {
			var req chunkdv1.GetChunkRequest
			wire.Decode(call.Body, &req)
			c.suspect(ctx, req.GetId(), string(call.To))
		}
	}
	if have < ec.DataShards {
		return nil, iface.Errorf(iface.CodeUnavailable, "chunk %d (stripe %s): %d of %d shards readable, %d needed: %v: %v",
			ref.Index, ref.ID[:12], have, ec.TotalShards, ec.DataShards, ec.ErrUnrecoverable, errors.Join(errs...))
	}
	ref.Decoded = slices.ContainsFunc(payloads[:ec.DataShards], func(p []byte) bool { return p == nil })
	data, err := c.codec.Join(payloads, size)
	if err != nil {
		return nil, iface.Errorf(iface.CodeInternal, "chunk %d: %v", ref.Index, err)
	}
	// The shards each verified, so a mismatch here means they were encoded
	// from other bytes: a client that claimed the wrong shards (ADR-0022).
	if ec.LogicalID(sha256.Sum256(data)) != logical {
		return nil, iface.Errorf(iface.CodeCorrupt, "chunk %d: decoded bytes do not match stripe %s", ref.Index, ref.ID[:12])
	}
	return data, nil
}

// shardTries bounds the reads of one shard replica: a timeout or an
// unavailable node may answer the second time, as in fetch.
const shardTries = 2

// nextShards picks up to n shards to read next and, for each, its best
// replica with tries left: shards not yet asked before retries, then alive
// data shards, alive parity, then suspect ones.
func (c *Direct) nextShards(locs []*chunkdv1.ShardLocation, payloads [][]byte, tries []map[string]int, n int) ([]iface.Call, []int) {
	type pick struct {
		j       int
		rep     *chunkdv1.Replica
		suspect bool
		tries   int
	}
	var picks []pick
	for j, sl := range locs {
		if payloads[j] != nil {
			continue
		}
		for _, r := range c.health.order(sl.GetReplicas()) {
			if t := tries[j][r.GetNode()]; t < shardTries {
				picks = append(picks, pick{j, r, r.GetSuspect(), t})
				break
			}
		}
	}
	// Stable: within each group, data shards (lower indexes) first.
	slices.SortStableFunc(picks, func(a, b pick) int {
		return cmp.Or(cmp.Compare(a.tries, b.tries), cmp.Compare(btoi(a.suspect), btoi(b.suspect)))
	})
	var calls []iface.Call
	var idx []int
	for _, p := range picks[:min(n, len(picks))] {
		tries[p.j][p.rep.GetNode()]++
		calls = append(calls, iface.Call{To: iface.NodeID(p.rep.GetNode()), Addr: p.rep.GetAddr(), Kind: wire.KindGetChunk,
			Body: wire.Marshal(&chunkdv1.GetChunkRequest{Id: locs[p.j].GetId()})})
		idx = append(idx, p.j)
	}
	return calls, idx
}

var errShardMismatch = errors.New("data does not match shard hash")

// shardPayload checks a GetChunk result for shard j of stripe logical and
// returns its payload.
func shardPayload(res iface.Result, id []byte, logical iface.ChunkID, j int) ([]byte, error) {
	if res.Err != nil {
		return nil, res.Err
	}
	var resp chunkdv1.GetChunkResponse
	if err := wire.Decode(res.Body, &resp); err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(resp.GetData()); !bytes.Equal(sum[:], id) {
		return nil, errShardMismatch
	}
	return ec.Payload(resp.GetData(), logical, j)
}

func (r Redundancy) proto() chunkdv1.Redundancy {
	if r == EC42 {
		return chunkdv1.Redundancy_REDUNDANCY_EC_4_2
	}
	return chunkdv1.Redundancy_REDUNDANCY_REPLICATED
}

func redundancy(r chunkdv1.Redundancy) Redundancy {
	if r == chunkdv1.Redundancy_REDUNDANCY_EC_4_2 {
		return EC42
	}
	return Replicated
}

// ParseRedundancy reads a policy name: "replicated" (or "") or "ec-4+2".
func ParseRedundancy(s string) (Redundancy, error) {
	switch s {
	case "", "replicated":
		return Replicated, nil
	case string(EC42):
		return EC42, nil
	}
	return "", iface.Errorf(iface.CodeInvalid, "unknown redundancy %q: want replicated or %s", s, EC42)
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
