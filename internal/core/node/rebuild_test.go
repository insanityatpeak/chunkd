package node_test

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// stripeSources serves shard j of a fresh stripe from node sj, with the
// bytes bad[j] returns in place of the real block when set.
func stripeSources(t *testing.T, g *group, bad map[int][]byte) ([]ec.Shard, iface.ChunkID, []byte) {
	t.Helper()
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i * 7)
	}
	shards, err := ec.New().Encode(data)
	if err != nil {
		t.Fatal(err)
	}
	for j, s := range shards {
		body := s.Block
		if b, ok := bad[j]; ok {
			body = b
		}
		g.net.Serve(iface.NodeID(fmt.Sprintf("s%d", j)), wire.KindGetChunk, func(_ iface.Message, respond iface.Responder) {
			if body == nil {
				respond(nil, iface.Errorf(iface.CodeNotFound, "gone"))
				return
			}
			respond(wire.Marshal(&chunkdv1.GetChunkResponse{Data: body}), nil)
		}, iface.ServeOpts{})
	}
	return shards, ec.LogicalID(sha256.Sum256(data)), data
}

func rebuildCmd(shards []ec.Shard, logical iface.ChunkID, idx int, order ...int) []byte {
	cmd := &chunkdv1.RebuildShard{CopyId: 7, ShardId: shards[idx].ID[:], Index: int32(idx), LogicalId: logical[:], ChunkSize: 1000, Term: 1}
	for _, j := range order {
		cmd.Sources = append(cmd.Sources, &chunkdv1.ShardSource{Index: int32(j), ShardId: shards[j].ID[:], Node: fmt.Sprintf("s%d", j)})
	}
	return wire.Marshal(cmd)
}

func TestRebuildShard(t *testing.T) {
	t.Run("decodes from the first 4", func(t *testing.T) {
		g := newGroup(t, "m1")
		shards, logical, _ := stripeSources(t, g, nil)
		g.send("m1", wire.KindRebuildShard, rebuildCmd(shards, logical, 1, 0, 2, 4, 5, 3))
		g.clock.Advance(time.Second)
		if !g.has(shards[1].ID) || len(g.got["m1"][wire.KindReplicateFailed]) != 0 {
			t.Fatalf("shard stored %v, failures %d", g.has(shards[1].ID), len(g.got["m1"][wire.KindReplicateFailed]))
		}
	})

	t.Run("a bad and a missing source fall back to the rest", func(t *testing.T) {
		g := newGroup(t, "m1")
		rotted := []byte("not a shard")
		shards, logical, _ := stripeSources(t, g, map[int][]byte{0: rotted, 4: nil})
		g.send("m1", wire.KindRebuildShard, rebuildCmd(shards, logical, 3, 0, 1, 2, 4, 5))
		g.clock.Advance(time.Second)
		// 1, 2 and 5 verify; 0 and 4 do not: three are not enough.
		if g.has(shards[3].ID) || len(g.got["m1"][wire.KindReplicateFailed]) != 1 {
			t.Fatalf("with 3 good sources: stored %v, failures %d", g.has(shards[3].ID), len(g.got["m1"][wire.KindReplicateFailed]))
		}
		g2 := newGroup(t, "m1")
		shards, logical, _ = stripeSources(t, g2, map[int][]byte{0: rotted})
		g2.send("m1", wire.KindRebuildShard, rebuildCmd(shards, logical, 3, 0, 1, 2, 4, 5))
		g2.clock.Advance(time.Second)
		if !g2.has(shards[3].ID) {
			t.Fatal("not rebuilt from the fallback source")
		}
	})

	t.Run("a shard of another slot is refused", func(t *testing.T) {
		g := newGroup(t, "m1")
		shards, logical, _ := stripeSources(t, g, nil)
		// Shard 2's block offered as shard 0: the hash is fine, the header is not.
		cmd := &chunkdv1.RebuildShard{}
		wire.Decode(rebuildCmd(shards, logical, 5, 0, 1, 2, 3), cmd)
		cmd.Sources[0].ShardId = shards[2].ID[:]
		cmd.Sources[0].Node = "s2"
		g.send("m1", wire.KindRebuildShard, wire.Marshal(cmd))
		g.clock.Advance(time.Second)
		if g.has(shards[5].ID) || len(g.got["m1"][wire.KindReplicateFailed]) != 1 {
			t.Fatalf("stored %v, failures %d", g.has(shards[5].ID), len(g.got["m1"][wire.KindReplicateFailed]))
		}
	})
}
