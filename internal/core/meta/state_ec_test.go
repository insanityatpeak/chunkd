package meta

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

var ecNodes = []string{"n1", "n2", "n3", "n4", "n5", "n6"}

func beginEC(path string, expected uint64, size int64, nodes ...string) *chunkdv1.Op {
	op := beginClaims(path, expected, size)
	b := op.GetBegin()
	b.Redundancy = chunkdv1.Redundancy_REDUNDANCY_EC_4_2
	if nodes == nil {
		nodes = ecNodes
	}
	for _, r := range b.Placement {
		r.Nodes = nodes
	}
	return op
}

// shardOf names shard j of the stripe commit(_, _, tag) commits as chunk i.
func shardOf(tag byte, i, j int) iface.ChunkID { return sha256.Sum256([]byte{tag, byte(i), 's', byte(j)}) }

// claimEC claims chunk i as the stripe chunkOf(tag, i) with shards
// shardOf(shardTag, i, 0..5).
func claimEC(id uint64, tag, shardTag byte, idx ...int) *chunkdv1.Op {
	op := claimOp(id, tag, idx...)
	for _, cl := range op.GetClaim().GetClaims() {
		for j := range ec.TotalShards {
			sh := shardOf(shardTag, int(cl.GetIndex()), j)
			cl.Shards = append(cl.Shards, sh[:])
		}
	}
	return op
}

func TestECBeginAndClaimValidation(t *testing.T) {
	ok, bad := iface.CodeUnknown, iface.CodeInvalid
	noClaims := beginEC("/a", 0, 4)
	noClaims.GetBegin().Claims = false
	unknown := beginEC("/a", 0, 4)
	unknown.GetBegin().Redundancy = 7
	shortClaim := claimEC(1, 'a', 'a', 0)
	shortClaim.GetClaim().Claims[0].Shards = shortClaim.GetClaim().Claims[0].Shards[:5]
	repeatShard := claimEC(1, 'a', 'a', 0)
	repeatShard.GetClaim().Claims[0].Shards[5] = repeatShard.GetClaim().Claims[0].Shards[0]
	tests := []struct {
		name  string
		steps []step
	}{
		{"begin needs 6 distinct nodes and claims", []step{
			{beginEC("/a", 0, 4, "n1", "n2", "n3", "n4", "n5"), bad, Result{}},
			{beginEC("/a", 0, 4, "n1", "n2", "n3", "n4", "n5", "n1"), bad, Result{}},
			{noClaims, bad, Result{}},
			{unknown, bad, Result{}},
			{beginEC("/a", 0, 4), ok, Result{UploadID: 1}},
		}},
		{"an EC claim carries 6 distinct shard ids", []step{
			{beginEC("/a", 0, 4), ok, Result{UploadID: 1}},
			{claimOp(1, 'a', 0), bad, Result{}},
			{shortClaim, bad, Result{}},
			{repeatShard, bad, Result{}},
			{claimEC(1, 'a', 'a', 0), ok, Result{UploadID: 1}},
			{claimEC(1, 'a', 'a', 0), ok, Result{UploadID: 1}}, // retry
		}},
		{"a replicated claim carries no shards", []step{
			{beginClaims("/a", 0, 4), ok, Result{UploadID: 1}},
			{claimEC(1, 'a', 'a', 0), bad, Result{}},
		}},
		{"one stripe id names one shard set", []step{
			{beginEC("/a", 0, 4), ok, Result{UploadID: 1}},
			{beginEC("/b", 0, 4), ok, Result{UploadID: 2}},
			{claimEC(1, 'x', 'a', 0), ok, Result{UploadID: 1}},
			{claimEC(2, 'x', 'b', 0), bad, Result{}},
			{claimEC(2, 'x', 'a', 0), ok, Result{UploadID: 2}},
		}},
		{"one claim op cannot give a stripe two shard sets", []step{
			{beginEC("/a", 0, 4), ok, Result{UploadID: 1}},
			{beginEC("/b", 0, 4), ok, Result{UploadID: 2}},
			{&chunkdv1.Op{Op: &chunkdv1.Op_Claim{Claim: &chunkdv1.ClaimChunksOp{UploadId: 1, Claims: append(
				claimEC(1, 'x', 'a', 0).GetClaim().GetClaims(), claimEC(1, 'x', 'b', 0).GetClaim().GetClaims()...)}}}, bad, Result{}},
		}},
		{"a shard keeps its slot across stripes", []step{
			{beginEC("/a", 0, 8), ok, Result{UploadID: 1}},
			{claimEC(1, 'x', 'a', 0), ok, Result{UploadID: 1}},
			// Stripe y reuses stripe x's shard ids (shardTag 'a', chunk 0) as chunk 1.
			{func() *chunkdv1.Op {
				op := claimEC(1, 'y', 'a', 1)
				for j, sh := range op.GetClaim().Claims[0].Shards {
					id := shardOf('a', 0, j)
					copy(sh, id[:])
				}
				return op
			}(), bad, Result{}},
		}},
		{"policies never share a record", []step{
			{beginClaims("/r", 0, 4), ok, Result{UploadID: 1}},
			{claimOp(1, 'a', 0), ok, Result{UploadID: 1}},
			{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
			{beginEC("/e", 0, 4), ok, Result{UploadID: 2}},
			{claimEC(2, 'a', 'a', 0), bad, Result{}}, // chunkOf('a', 0) is a replicated record
			{claimEC(2, 'b', 'b', 0), ok, Result{UploadID: 2}},
			{beginClaims("/r2", 0, 4), ok, Result{UploadID: 3}},
			{claimOp(3, 'b', 0), bad, Result{}}, // chunkOf('b', 0) is a claimed stripe
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { run(t, New(), tt.steps) })
	}
}

func TestECStripeLifecycle(t *testing.T) {
	ok := iface.CodeUnknown
	s := New()
	run(t, s, []step{
		{beginEC("/a", 0, 6), ok, Result{UploadID: 1}}, // chunks of 4 and 2 bytes
		{claimEC(1, 'a', 'a', 0, 1), ok, Result{UploadID: 1}},
	})
	marked := func(want bool) {
		t.Helper()
		if d := s.Reconcile(); !d.Empty() {
			t.Fatalf("drift %+v", d)
		}
		for i := range 2 {
			for j := range ec.TotalShards {
				if s.Marked(shardOf('a', i, j)) != want {
					t.Fatalf("chunk %d shard %d: marked %v, want %v", i, j, !want, want)
				}
			}
		}
	}
	marked(true) // claimed only

	ref, found := s.ShardOf(shardOf('a', 1, 5))
	if !found || ref.Stripe != chunkOf('a', 1) || ref.Index != 5 || ref.Size != ec.BlockSize(2) {
		t.Fatalf("ShardOf = %+v, %v", ref, found)
	}
	if b, _ := s.Block(shardOf('a', 0, 2)); !b.Shard || b.Size != ec.BlockSize(4) {
		t.Fatalf("Block = %+v", b)
	}

	run(t, s, []step{
		{commit(1, 2, 'a'), ok, Result{UploadID: 1, Version: 1}},
		// A second file deduplicates against the stripe.
		{beginEC("/b", 0, 4), ok, Result{UploadID: 2}},
		{claimEC(2, 'a', 'a', 0), ok, Result{UploadID: 2}},
		{commit(2, 1, 'a'), ok, Result{UploadID: 2, Version: 1}},
	})
	marked(true)
	if v, _ := s.Stat("/a"); v.Redundancy != chunkdv1.Redundancy_REDUNDANCY_EC_4_2 {
		t.Fatalf("version redundancy %v", v.Redundancy)
	}
	if c, _ := s.Chunk(chunkOf('a', 0)); c.Refcount != 2 || len(c.Shards) != ec.TotalShards || c.Shards[3] != shardOf('a', 0, 3) {
		t.Fatalf("record %+v", c)
	}
	if st := s.stripes[chunkOf('a', 0)]; st.holds != 1 {
		t.Fatalf("stripe holds %d after its claims ended, want 1 (the record)", st.holds)
	}

	r, err := Restore(s.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Snapshot(), s.Snapshot()) || len(r.shardOf) != len(s.shardOf) || len(r.stripes) != len(s.stripes) {
		t.Fatal("stripes did not survive a snapshot")
	}

	// Deleting both files and letting retention pass unmarks every shard.
	run(t, s, []step{
		{del("/a", 1), ok, Result{Version: 2}},
		{del("/b", 1), ok, Result{Version: 2}},
		{advance(1), ok, Result{Dropped: 2}},
	})
	marked(false)
	if len(s.stripes) != 0 || len(s.shardOf) != 0 {
		t.Fatalf("%d stripes, %d shards indexed after collection", len(s.stripes), len(s.shardOf))
	}
}

func TestECAbortUnmarksClaimedShards(t *testing.T) {
	ok := iface.CodeUnknown
	s := New()
	run(t, s, []step{
		{beginEC("/a", 0, 4), ok, Result{UploadID: 1}},
		{claimEC(1, 'a', 'a', 0), ok, Result{UploadID: 1}},
		{abort(1), ok, Result{UploadID: 1}},
	})
	if s.Marked(shardOf('a', 0, 0)) || len(s.shardOf) != 0 {
		t.Fatal("aborted upload's shards still marked")
	}
}

func TestBlockPrefersReplicatedRecord(t *testing.T) {
	ok := iface.CodeUnknown
	s := New()
	run(t, s, []step{
		{beginEC("/e", 0, 4), ok, Result{UploadID: 1}},
		{claimEC(1, 'a', 'a', 0), ok, Result{UploadID: 1}},
		{commit(1, 1, 'a'), ok, Result{UploadID: 1, Version: 1}},
	})
	// A replicated file whose chunk's bytes are exactly shard 0's block.
	sh := shardOf('a', 0, 0)
	op := beginClaims("/r", 0, 4)
	run(t, s, []step{{op, ok, Result{UploadID: 2}}})
	claim := &chunkdv1.Op{Op: &chunkdv1.Op_Claim{Claim: &chunkdv1.ClaimChunksOp{UploadId: 2, Claims: []*chunkdv1.ChunkClaim{{Index: 0, Id: sh[:]}}}}}
	sum := sha256.Sum256(nil)
	cm := &chunkdv1.Op{Op: &chunkdv1.Op_Commit{Commit: &chunkdv1.CommitUploadOp{UploadId: 2, ChunkIds: [][]byte{sh[:]}, Sha256: sum[:]}}}
	run(t, s, []step{{claim, ok, Result{UploadID: 2}}, {cm, ok, Result{UploadID: 2, Version: 1}}})
	if b, _ := s.Block(sh); b.Shard || b.Size != 4 {
		t.Fatalf("Block = %+v, want the replicated chunk", b)
	}
}

func TestReconcileFindsStripeDrift(t *testing.T) {
	ok := iface.CodeUnknown
	s := New()
	run(t, s, []step{
		{beginEC("/a", 0, 4), ok, Result{UploadID: 1}},
		{claimEC(1, 'a', 'a', 0), ok, Result{UploadID: 1}},
	})
	s.stripes[chunkOf('a', 0)].holds++
	if d := s.Reconcile(); len(d.Stripes) != 1 || d.Stripes[0] != chunkOf('a', 0) {
		t.Fatalf("drift %+v, want the stripe", d)
	}
}
