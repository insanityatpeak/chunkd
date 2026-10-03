package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// secondClient is another client process: its own caller, nothing shared
// with c.Client() but the cluster.
func secondClient(c *Cluster) *client.Direct {
	caller := c.NewCaller("client-2")
	opts := client.Options{Meta: MetaID, Sleep: caller.Sleep}
	for _, id := range c.MetaIDs() {
		opts.Peers = append(opts.Peers, client.MetaPeer{ID: id})
	}
	return client.New(caller, opts)
}

// settled waits for incremental block reports so a status shows what was
// stored.
func settled(c *Cluster) { c.Tick(time.Second) }

func TestResumeAfterClientCrash(t *testing.T) {
	for _, policy := range []client.Redundancy{client.Replicated, client.EC42} {
		name := "replicated"
		if policy != client.Replicated {
			name = string(policy)
		}
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Nodes = 6
			c := New(3, cfg, io.Discard)
			c.Tick(3 * time.Second)
			ctx := context.Background()
			data := c.RandomData("/big", 14<<20) // chunks of 4, 4, 4 and 2 MiB
			sum := sha256.Sum256(data)

			first := c.Client()
			info, err := first.BeginResumable(ctx, "/big", int64(len(data)), sum, client.PutOptions{Redundancy: policy})
			if err != nil {
				t.Fatal(err)
			}
			if info, err = first.Append(ctx, info.ID, 0, bytes.NewReader(data[:8<<20]), 8<<20); err != nil || info.Offset != 8<<20 {
				t.Fatalf("first append: offset %d, %v", info.Offset, err)
			}
			// The first process dies here. Another one asks where the upload is.
			settled(c)
			second := secondClient(c)
			st, err := second.UploadStatus(ctx, info.ID)
			if err != nil {
				t.Fatal(err)
			}
			if st.Offset != 8<<20 || st.Size != int64(len(data)) || st.Path != "/big" || st.Redundancy != policy || st.Version != 0 {
				t.Fatalf("status %+v", st)
			}
			sent := c.Net().Stats().Bytes
			done, err := second.Append(ctx, st.ID, st.Offset, bytes.NewReader(data[st.Offset:]), int64(len(data))-st.Offset)
			if err != nil || done.Version != 1 {
				t.Fatalf("resume: version %d, %v", done.Version, err)
			}
			// Only the 6 MiB left crossed the network, times the redundancy.
			limit := int64(6<<20) * 3
			if policy == client.EC42 {
				limit /= 2 // 1.5×
			}
			if got := int64(c.Net().Stats().Bytes - sent); got > limit+limit/10 {
				t.Fatalf("resuming sent %d bytes, want about %d", got, limit)
			}
			got, _, err := c.Download("/big")
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("read back %d bytes, %v", len(got), err)
			}
			// Appending again after the commit reports the version.
			if again, err := second.Append(ctx, st.ID, 0, bytes.NewReader(data), int64(len(data))); err != nil || again.Version != 1 {
				t.Fatalf("append after commit: %+v, %v", again, err)
			}
		})
	}
}

func TestResumeRefusals(t *testing.T) {
	c := New(4, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	ctx := context.Background()
	cl := c.Client()
	data := c.RandomData("/r", 10<<20)
	sum := sha256.Sum256(data)
	info, err := cl.BeginResumable(ctx, "/r", int64(len(data)), sum, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Append(ctx, info.ID, 4<<20, bytes.NewReader(data[4<<20:8<<20]), 4<<20); iface.CodeOf(err) != iface.CodeConflict {
		t.Fatalf("append past the claimed run: %v", err)
	}
	if _, err := cl.Append(ctx, info.ID, 0, bytes.NewReader(data[:3]), 3); iface.CodeOf(err) != iface.CodeInvalid {
		t.Fatalf("append of a partial chunk mid-file: %v", err)
	}
	if _, err := cl.Append(ctx, info.ID, 0, bytes.NewReader(data[:4<<20]), 4<<20); err != nil {
		t.Fatal(err)
	}
	// Other bytes for chunk 0: the claim names the first ones.
	other := bytes.Clone(data[:4<<20])
	other[0] ^= 1
	if _, err := cl.Append(ctx, info.ID, 0, bytes.NewReader(other), 4<<20); iface.CodeOf(err) != iface.CodeInvalid {
		t.Fatalf("append of changed bytes: %v", err)
	}
	// Bytes that do not hash to the declaration: a single pass sees them all
	// and refuses to commit.
	wrong := bytes.Clone(data)
	wrong[len(wrong)-1] ^= 1
	single, err := cl.BeginResumable(ctx, "/single", int64(len(wrong)), sum, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Append(ctx, single.ID, 0, bytes.NewReader(wrong), int64(len(wrong))); iface.CodeOf(err) != iface.CodeInvalid {
		t.Fatalf("single-pass append of other bytes: %v", err)
	}
	// Split over appends nobody sees the whole file: it commits, and every
	// read refuses it rather than return bytes that are not the declared ones.
	if done, err := cl.Append(ctx, info.ID, 4<<20, bytes.NewReader(wrong[4<<20:]), int64(len(wrong))-4<<20); err != nil || done.Version != 1 {
		t.Fatalf("split append: %+v, %v", done, err)
	}
	if _, _, err := c.Download("/r"); iface.CodeOf(err) != iface.CodeInternal {
		t.Fatalf("read of a version whose bytes differ from its hash: %v", err)
	}

	// A lease that ran out: the upload is gone and says so.
	lost, err := cl.BeginResumable(ctx, "/lost", 1, sha256.Sum256([]byte{1}), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := c.Config().Meta
	c.Tick(time.Duration(m.LeaseEpochs+2) * m.EpochEvery)
	if _, err := cl.UploadStatus(ctx, lost.ID); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("expired upload: %v", err)
	}
}

// The upload's progress is in the log, so it survives the metadata leader.
func TestResumeAcrossLeaderFailover(t *testing.T) {
	c := newMetaGroup(t, 5)
	ctx := context.Background()
	data := c.RandomData("/f", 1<<20) // 4 chunks of 256 KiB
	sum := sha256.Sum256(data)
	cl := c.Client()
	info, err := cl.BeginResumable(ctx, "/f", int64(len(data)), sum, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Append(ctx, info.ID, 0, bytes.NewReader(data[:512<<10]), 512<<10); err != nil {
		t.Fatal(err)
	}
	settled(c)
	old := c.MetaLeader()
	c.KillMeta(old)
	c.Tick(3 * time.Second)
	if nl := c.MetaLeader(); nl == "" || nl == old {
		t.Fatalf("leader %q after killing %s", nl, old)
	}
	second := secondClient(c)
	st, err := second.UploadStatus(ctx, info.ID)
	if err != nil || st.Offset != 512<<10 {
		t.Fatalf("status under the new leader: %+v, %v", st, err)
	}
	if done, err := second.Append(ctx, st.ID, st.Offset, bytes.NewReader(data[st.Offset:]), int64(len(data))-st.Offset); err != nil || done.Version != 1 {
		t.Fatalf("resume: %+v, %v", done, err)
	}
	mustAgree(t, c)
}
