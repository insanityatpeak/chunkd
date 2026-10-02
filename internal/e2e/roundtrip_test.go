package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/local"
	"github.com/insanityatpeak/chunkd/internal/sim"
	"github.com/insanityatpeak/chunkd/internal/sim/cluster"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

const mib = 1 << 20

var sizes = []struct {
	name string
	n    int64
}{
	{"0 B", 0},
	{"1 B", 1},
	{"4 MiB - 1", 4*mib - 1},
	{"4 MiB", 4 * mib},
	{"4 MiB + 1", 4*mib + 1},
	{"37 MiB", 37 * mib},
}

func random(n int64, seed uint64) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(seed, uint64(n)))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// roundTrip uploads, stats and downloads every size through api.
func roundTrip(t *testing.T, api client.API) { roundTripAs(t, api, client.Replicated) }

// roundTripAs is roundTrip under a redundancy policy. Erasure-coded chunks
// must come back as 6 shards, none as replicas.
func roundTripAs(t *testing.T, api client.API, policy client.Redundancy) {
	ctx := context.Background()
	for i, sz := range sizes {
		t.Run(sz.name, func(t *testing.T) {
			data := random(sz.n, uint64(i))
			path := fmt.Sprintf("/rt/%d", i)
			m, err := api.Put(ctx, path, bytes.NewReader(data), sz.n, client.PutOptions{Redundancy: policy})
			if err != nil {
				t.Fatal(err)
			}
			want := sha256.Sum256(data)
			if m.Redundancy != policy {
				t.Fatalf("stored as %q, asked for %q", m.Redundancy, policy)
			}
			for _, ch := range m.Chunk {
				if ec := policy == client.EC42; ec != (len(ch.Shards) == 6) || ec != (len(ch.Replicas) == 0) {
					t.Fatalf("chunk %d: %d shards, %d replicas under %q", ch.Index, len(ch.Shards), len(ch.Replicas), policy)
				}
			}
			if m.Version != 1 || m.Size != sz.n || m.SHA256 != fmt.Sprintf("%x", want) {
				t.Fatalf("put manifest %+v", m.FileInfo)
			}
			if wantChunks := int((sz.n + 4*mib - 1) / (4 * mib)); m.Chunks != wantChunks {
				t.Fatalf("%d chunks, want %d", m.Chunks, wantChunks)
			}
			var buf bytes.Buffer
			if _, err := api.Get(ctx, path, &buf); err != nil {
				t.Fatal(err)
			}
			if got := sha256.Sum256(buf.Bytes()); got != want {
				t.Fatalf("read back sha256 %x, wrote %x", got, want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	t.Run("sim", func(t *testing.T) {
		c := cluster.New(1, cluster.DefaultConfig(), io.Discard)
		c.Tick(3 * time.Second)
		roundTrip(t, c.Client())
		if err := c.AssertInvariants(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("real", func(t *testing.T) {
		c, err := local.Start(t.TempDir(), 3, meta.DefaultConfig(""))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		roundTrip(t, c.Client)
	})
}

func TestCommitRequiresMinReplicas(t *testing.T) {
	c := cluster.New(2, cluster.DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	// Only node-1 stays reachable. The metadata server still believes the
	// others are alive (heartbeats have not timed out yet), so Begin places
	// on them; the puts fail, and the upload must fail loudly.
	for _, n := range c.Nodes()[1:] {
		c.Net().Crash(n.ID())
	}
	_, _, err := c.UploadRandom("/doomed", 6*mib)
	if iface.CodeOf(err) != iface.CodeUnavailable {
		t.Fatalf("upload with 1 reachable node: err = %v, want unavailable", err)
	}
	if _, err := c.Client().Stat(context.Background(), "/doomed"); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("failed upload visible: %v", err)
	}
	// Once heartbeats time out, uploads are refused at Begin.
	c.Tick(10 * time.Second)
	if _, _, err := c.UploadRandom("/doomed2", 10); iface.CodeOf(err) != iface.CodeUnavailable {
		t.Fatalf("begin with 1 live node: err = %v, want unavailable", err)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestUncommittedInvisible(t *testing.T) {
	c := cluster.New(3, cluster.DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	if _, _, err := c.UploadRandom("/v", 5*mib); err != nil {
		t.Fatal(err)
	}
	// Begin a second version and write its chunks, but never commit.
	caller := c.NewCaller("half-writer")
	r := caller.Do(context.Background(), []iface.Call{{To: cluster.MetaID, Kind: wire.KindBegin,
		Body: wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/v", ExpectedVersion: 1, Size: 3})}})
	if r[0].Err != nil {
		t.Fatal(r[0].Err)
	}
	r = caller.Do(context.Background(), []iface.Call{{To: cluster.MetaID, Kind: wire.KindBegin,
		Body: wire.Marshal(&chunkdv1.BeginUploadRequest{Path: "/new", Size: 3})}})
	if r[0].Err != nil {
		t.Fatal(r[0].Err)
	}
	ctx := context.Background()
	files, err := c.Client().List(ctx, "/")
	if err != nil || len(files) != 1 || files[0].Path != "/v" || files[0].Version != 1 {
		t.Fatalf("List = %+v, %v; want only /v v1", files, err)
	}
	if _, err := c.Client().Get(ctx, "/new", io.Discard); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("Get of pending-only path: %v, want not found", err)
	}
	if m, err := c.Client().Stat(ctx, "/v"); err != nil || m.Version != 1 {
		t.Fatalf("Stat(/v) = v%d, %v; want v1 while v2 is pending", m.Version, err)
	}
}

// Many uploads over a lossy, duplicating network. Every upload that reports
// success must stay readable; the failures must stay invisible.
func TestUploadsUnderMessageLoss(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		cfg := cluster.DefaultConfig()
		cfg.Faults = sim.Faults{DropRate: 0.05, DupRate: 0.05, MinDelay: time.Millisecond, MaxDelay: 60 * time.Millisecond, BytesPerSec: 125 * mib}
		cfg.Meta.ChunkSize = 256 << 10
		c := cluster.New(seed, cfg, io.Discard)
		c.Tick(3 * time.Second)
		for i := range 8 {
			path := fmt.Sprintf("/f%d", i%5)
			_, _, err := c.UploadRandom(path, int64(1+i*300_000))
			if err != nil && iface.CodeOf(err) != iface.CodeUnavailable {
				t.Fatalf("seed %d upload %d: unexpected error class: %v", seed, i, err)
			}
		}
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("replay with seed %d: %v", seed, err)
		}
	}
}

// TestRoundTripEC stores every size as RS(4,2) stripes over real gRPC and
// disk block stores: shard puts, gathered shard reads and decoding.
func TestRoundTripEC(t *testing.T) {
	c, err := local.Start(t.TempDir(), 6, meta.DefaultConfig(""))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	roundTripAs(t, c.Client, client.EC42)
}
