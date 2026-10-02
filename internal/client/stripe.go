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
// alive nodes first, then parity, then shards only a suspect node holds. A
// shard that fails or does not verify is replaced at once by the next in
// that order, and one still outstanding after the hedge delay (the recent
// p95 read) brings in the next too, as fetch hedges a replica. A second pass
// asks again the shards that only timed out: a lost message is not a lost
// shard. Decoding is needed only when a data shard is not used. The result
// is checked against the stripe ID (ADR-0022).
func (c *Direct) fetchStripe(ctx context.Context, loc *chunkdv1.ChunkLocation, ref *ChunkRef) ([]byte, error) {
	logical, err := wire.ChunkID(loc.GetId())
	if err != nil {
		return nil, err
	}
	locs := loc.GetShards()
	if len(locs) != ec.TotalShards {
		return nil, iface.Errorf(iface.CodeInternal, "chunk %d: %d shard locations, want %d", ref.Index, len(locs), ec.TotalShards)
	}
	after := c.health.hedgeDelay()
	if c.opts.NoHedge {
		after = noHedge
	}
	payloads := make([][]byte, ec.TotalShards)
	have := 0
	var errs []error
	retry := map[int]bool{} // shards whose every read failed transiently
	for pass := 0; pass < 2 && have < ec.DataShards; pass++ {
		calls, idx := c.shardCalls(locs, payloads, retry, pass)
		if len(calls) == 0 {
			break
		}
		clear(retry)
		var mismatch []iface.Call
		g := c.caller.Gather(ctx, calls, ec.DataShards-have, ec.DataShards-have, after, func(k int, res iface.Result) bool {
			j := idx[k]
			if payloads[j] != nil {
				return false // another replica of this shard won
			}
			data, err := shardPayload(res, locs[j].GetId(), logical, j)
			if err != nil {
				errs = append(errs, fmt.Errorf("shard %d on %s: %w", j, calls[k].To, err))
				ref.Shards[j].Rejected = append(ref.Shards[j].Rejected, string(calls[k].To))
				if errors.Is(err, errShardMismatch) {
					mismatch = append(mismatch, calls[k])
				}
				return false
			}
			payloads[j], ref.Shards[j].ServedBy = data, string(calls[k].To)
			have++
			return true
		})
		for k := range g.Launched {
			r, j := g.Results[k], idx[k]
			c.health.observe(string(calls[k].To), r, slices.Contains(g.Accepted, k))
			if r.Err == nil || r.Pending {
				continue
			}
			errs = append(errs, fmt.Errorf("shard %d on %s: %w", j, calls[k].To, r.Err))
			switch iface.CodeOf(r.Err) {
			case iface.CodeUnavailable, iface.CodeRetry:
				retry[j] = true
			case iface.CodeCorrupt:
				ref.Shards[j].Rejected = append(ref.Shards[j].Rejected, string(calls[k].To))
			}
		}
		ref.Hedged = ref.Hedged || g.Launched > ec.DataShards
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
	data, err := c.codec.Join(payloads, int(loc.GetSize()))
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

// shardCalls lists the reads of one pass in preference order. The first
// pass asks every missing shard's replicas: alive data shards, alive parity,
// then shards only a suspect node holds, a shard's other replicas last. The
// second asks only the shards in retry, those whose reads timed out.
func (c *Direct) shardCalls(locs []*chunkdv1.ShardLocation, payloads [][]byte, retry map[int]bool, pass int) ([]iface.Call, []int) {
	type pick struct {
		j, rank int
		rep     *chunkdv1.Replica
	}
	var picks []pick
	for j, sl := range locs {
		if payloads[j] != nil || (pass > 0 && !retry[j]) {
			continue
		}
		for n, r := range c.health.order(sl.GetReplicas()) {
			rank := 0
			switch {
			case n > 0:
				rank = 2
			case r.GetSuspect():
				rank = 1
			}
			picks = append(picks, pick{j, rank, r})
		}
	}
	// Stable: within a rank, data shards (lower indexes) first.
	slices.SortStableFunc(picks, func(a, b pick) int { return cmp.Compare(a.rank, b.rank) })
	calls := make([]iface.Call, len(picks))
	idx := make([]int, len(picks))
	for k, p := range picks {
		calls[k] = iface.Call{To: iface.NodeID(p.rep.GetNode()), Addr: p.rep.GetAddr(), Kind: wire.KindGetChunk,
			Body: wire.Marshal(&chunkdv1.GetChunkRequest{Id: locs[p.j].GetId()})}
		idx[k] = p.j
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
