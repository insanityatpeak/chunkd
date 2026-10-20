package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/insanityatpeak/chunkd/internal/core/chunk"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// DefaultWindow is how many chunks an upload claims and puts at once: 16 MiB
// of 4 MiB chunks in flight (24 MiB for EC 4+2). Past it a client on one
// 1 Gbit/s uplink gains little (put-window.csv, ADR-0034).
const DefaultWindow = 4

func (c *Direct) window() int {
	if c.opts.Window > 0 {
		return c.opts.Window
	}
	return DefaultWindow
}

// nextWave reads up to n chunks; it returns io.EOF only once none is left.
func nextWave(split *chunk.Splitter, n int) ([]chunk.Chunk, error) {
	var wave []chunk.Chunk
	for len(wave) < n {
		ch, err := split.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		wave = append(wave, ch)
	}
	if len(wave) == 0 {
		return nil, io.EOF
	}
	return wave, nil
}

// storeWave claims chs, chunks of one upload, in one request and stores those
// the claim does not find present, every chunk's puts in one batch
// (ADR-0034). It returns, in chs order, the ID each names in the commit (the
// chunk's, or its stripe's) and its ref. It fails at the first chunk, by
// index, with fewer than min copies or shards stored.
func (c *Direct) storeWave(ctx context.Context, begin *chunkdv1.BeginUploadResponse, chs []chunk.Chunk) ([]iface.ChunkID, []ChunkRef, error) {
	stripes := begin.GetRedundancy() == chunkdv1.Redundancy_REDUNDANCY_EC_4_2
	ids := make([]iface.ChunkID, len(chs))
	shards := make([][]ec.Shard, len(chs))
	req := &chunkdv1.ClaimChunksRequest{UploadId: begin.GetUploadId()}
	for k, ch := range chs {
		ids[k] = ch.ID
		cl := &chunkdv1.ChunkClaim{Index: int32(ch.Index), Id: ch.ID[:]}
		if stripes {
			// The claim names the stripe and its shards, so it is encoded first.
			var err error
			if shards[k], err = c.codec.Encode(ch.Data); err != nil {
				return nil, nil, err
			}
			ids[k] = ec.LogicalID(ch.ID)
			cl.Id = ids[k][:]
			for _, s := range shards[k] {
				cl.Shards = append(cl.Shards, s.ID[:])
			}
		}
		req.Claims = append(req.Claims, cl)
	}
	var claim chunkdv1.ClaimChunksResponse
	if err := c.meta(ctx, wire.KindClaim, req, &claim); err != nil {
		return nil, nil, err
	}
	refs := make([]ChunkRef, len(chs))
	var puts []*put
	min := int(begin.GetMinReplicas())
	for k, ch := range chs {
		if k < len(claim.GetPresent()) && claim.GetPresent()[k] {
			ref := ChunkRef{Index: ch.Index, ID: ids[k].String(), Size: int64(len(ch.Data)), Deduped: true}
			for _, r := range claim.GetLocations()[k].GetReplicas() {
				ref.Replicas = append(ref.Replicas, r.GetNode())
			}
			refs[k] = ref
			continue
		}
		pl := begin.GetPlacement()[ch.Index]
		var p *put
		if stripes {
			var err error
			if p, err = stripePut(ch, shards[k], pl, min); err != nil {
				return nil, nil, err
			}
		} else {
			p = chunkPut(ch, pl, min)
		}
		p.k = k
		puts = append(puts, p)
	}
	c.putWave(ctx, puts)
	for _, p := range puts {
		if p.stored < p.min {
			return nil, nil, p.failure()
		}
		refs[p.k] = p.ref
	}
	return ids, refs, nil
}

// put is one chunk's writes in a wave: a copy to each placed replica, or
// shard i to placement node i of a stripe.
type put struct {
	k, index int // in the wave, in the file
	stripe   bool
	calls    []iface.Call // one per target
	todo     []int        // targets the next round sends
	min      int
	stored   int
	ref      ChunkRef
	errs     []error // the last round's failures
}

func chunkPut(ch chunk.Chunk, pl *chunkdv1.ChunkPlacement, min int) *put {
	body := wire.Marshal(&chunkdv1.PutChunkRequest{Id: ch.ID[:], Data: ch.Data})
	p := &put{index: ch.Index, min: min, ref: ChunkRef{Index: ch.Index, ID: ch.ID.String(), Size: int64(len(ch.Data))}}
	for i, rep := range pl.GetReplicas() {
		p.calls = append(p.calls, iface.Call{To: iface.NodeID(rep.GetNode()), Addr: rep.GetAddr(), Kind: wire.KindPutChunk, Body: body})
		p.todo = append(p.todo, i)
	}
	return p
}

// stripePut sends shard i of an encoded chunk to placement node i. A stripe
// needs min shards stored (ADR-0022); one still in flight is left to land late
// or to repair (ADR-0031).
// SIMPLIFIED: shards go in one parallel batch. HDFS-EC streams cells to the
// shard writers in a pipeline as the client produces them.
func stripePut(ch chunk.Chunk, shards []ec.Shard, pl *chunkdv1.ChunkPlacement, min int) (*put, error) {
	reps := pl.GetReplicas()
	if len(reps) != len(shards) {
		return nil, iface.Errorf(iface.CodeInternal, "chunk %d: %d nodes placed for %d shards", ch.Index, len(reps), len(shards))
	}
	p := &put{index: ch.Index, stripe: true, min: min, ref: ChunkRef{Index: ch.Index, ID: ec.LogicalID(ch.ID).String(), Size: int64(len(ch.Data))}}
	for i, s := range shards {
		p.calls = append(p.calls, iface.Call{To: iface.NodeID(reps[i].GetNode()), Addr: reps[i].GetAddr(), Kind: wire.KindPutChunk,
			Body: wire.Marshal(&chunkdv1.PutChunkRequest{Id: s.ID[:], Data: s.Block})})
		p.todo = append(p.todo, i)
		p.ref.Shards = append(p.ref.Shards, ShardRef{Index: i, ID: s.ID.String(), Replicas: []string{}})
	}
	return p, nil
}

// land records target i's result and reports whether to send it again.
func (p *put) land(i int, res iface.Result) bool {
	to := string(p.calls[i].To)
	switch {
	case res.Pending:
		// Still in flight: not a failure, and not retried.
		return false
	case res.Err != nil && p.stripe:
		p.errs = append(p.errs, fmt.Errorf("shard %d on %s: %w", i, to, res.Err))
		return true
	case res.Err != nil:
		p.errs = append(p.errs, fmt.Errorf("%s: %w", to, res.Err))
		return true
	}
	p.stored++
	if p.stripe {
		p.ref.Shards[i].Replicas = []string{to}
	} else {
		p.ref.Replicas = append(p.ref.Replicas, to)
	}
	return false
}

func (p *put) failure() error {
	what := "replicas"
	if p.stripe {
		what = "shards"
	}
	return iface.Errorf(iface.CodeUnavailable, "chunk %d: %d of %d required %s stored: %v", p.index, p.stored, p.min, what, errors.Join(p.errs...))
}

// putWave sends every put's targets in one batch and returns once each put
// has min stored and PutGrace has passed for the rest, or has settled: a slow
// target no longer sets the wave's latency. Two rounds: a put lost in transit
// is retried once (puts are idempotent), so one dropped message does not cost
// a copy. Targets still in flight keep going; if one never lands, repair tops
// the chunk up from the others.
func (c *Direct) putWave(ctx context.Context, puts []*put) {
	for round := 0; round < 2; round++ {
		var sent []*put
		var groups []iface.QuorumGroup
		for _, p := range puts {
			if len(p.todo) == 0 {
				continue
			}
			p.errs = p.errs[:0]
			g := iface.QuorumGroup{Need: max(p.min-p.stored, 1)}
			for _, i := range p.todo {
				g.Calls = append(g.Calls, p.calls[i])
			}
			sent, groups = append(sent, p), append(groups, g)
		}
		if len(sent) == 0 {
			return
		}
		for j, res := range c.putRound(ctx, groups) {
			p := sent[j]
			todo := p.todo
			p.todo = nil
			for k, r := range res {
				if p.land(todo[k], r) {
					p.todo = append(p.todo, todo[k])
				}
			}
		}
	}
}

// putRound sends one round of a wave; a negative PutGrace waits for every
// call, as before ADR-0029.
func (c *Direct) putRound(ctx context.Context, groups []iface.QuorumGroup) [][]iface.Result {
	if c.opts.PutGrace < 0 {
		var calls []iface.Call
		for _, g := range groups {
			calls = append(calls, g.Calls...)
		}
		flat := c.caller.Do(ctx, calls)
		out := make([][]iface.Result, len(groups))
		for j, g := range groups {
			out[j], flat = flat[:len(g.Calls)], flat[len(g.Calls):]
		}
		return out
	}
	grace := c.opts.PutGrace
	if grace == 0 {
		grace = DefaultPutGrace
	}
	return c.caller.Quorums(ctx, groups, grace)
}
