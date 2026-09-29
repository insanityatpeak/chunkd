// Command chunkd-node runs a storage node: it stores chunks by hash, serves
// them to clients, and heartbeats and reports to the metadata server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/core/scrub"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/blockstore"
	"github.com/insanityatpeak/chunkd/internal/real/grpcnet"
	"github.com/insanityatpeak/chunkd/internal/real/server"
)

func main() {
	id := flag.String("id", server.Env("CHUNKD_ID", "node-1"), "node ID")
	grpcAddr := flag.String("grpc", server.Env("CHUNKD_GRPC", ":7000"), "gRPC listen address")
	advertise := flag.String("advertise", server.Env("CHUNKD_ADVERTISE", "localhost:7001"), "gRPC address peers and clients dial")
	adminAddr := flag.String("admin", server.Env("CHUNKD_ADMIN", ":9000"), "HTTP address for /metrics and /healthz")
	metaID := flag.String("meta-id", server.Env("CHUNKD_META_ID", "meta-1"), "metadata server ID")
	metaAddr := flag.String("meta", server.Env("CHUNKD_META", "localhost:7000"), "metadata server gRPC address")
	rack := flag.String("rack", server.Env("CHUNKD_RACK", "r1"), "failure domain label")
	dataDir := flag.String("data", server.Env("CHUNKD_DATA", "data/node"), "chunk directory")
	defScrub := scrub.DefaultConfig()
	scrubPass := flag.Duration("scrub-pass", envDuration("CHUNKD_SCRUB_PASS", defScrub.Pass), "interval between scrub pass starts")
	scrubRate := flag.Int64("scrub-rate", envInt("CHUNKD_SCRUB_RATE", defScrub.BytesPerSec>>20), "scrub read cap, MiB/s")
	flag.Parse()

	store, err := blockstore.Open(*dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-node:", err)
		os.Exit(1)
	}
	cfg := node.DefaultConfig(iface.NodeID(*id), iface.NodeID(*metaID), *rack)
	cfg.Addr = *advertise
	cfg.Scrub = scrub.Config{BytesPerSec: *scrubRate << 20, Pass: *scrubPass}

	sc := server.Config{
		ID: cfg.ID, GRPCAddr: *grpcAddr, Advertise: *advertise, AdminAddr: *adminAddr,
		Peers: map[iface.NodeID]string{cfg.Meta: *metaAddr},
	}
	err = server.Run("node", sc, func(p *server.Process) error {
		// Repair pulls chunks from peers: a 4 MiB stream per copy.
		// Lives as long as the process.
		peers := grpcnet.NewCaller(nil, 30*time.Second)
		n := node.New(node.Deps{Clock: p.Clock, Net: p.Net, Async: peers.Async(p.Loop), Store: store, Rand: p.Rand, Log: p.Log}, cfg)
		n.Start()

		heartbeats := p.Metrics.Gauge("chunkd_node_heartbeats_sent", "Heartbeats sent to the metadata server.")
		chunks := p.Metrics.Gauge("chunkd_node_chunks", "Chunks stored.")
		used := p.Metrics.Gauge("chunkd_node_used_bytes", "Bytes stored.")
		scrubBytes := p.Metrics.Counter("chunkd_scrub_bytes_total", "Bytes re-read and verified by the scrubber; rate() gives the scrub rate.")
		scrubCorrupt := p.Metrics.Counter("chunkd_scrub_corrupt_total", "Chunks the scrubber found corrupt and quarantined.")
		corrupt := p.Metrics.Counter("chunkd_node_corrupt_total", "Chunks quarantined after failing verification, by reads and the scrubber.")
		lastPass := p.Metrics.Gauge("chunkd_scrub_last_pass_seconds", "Duration of the last complete scrub pass.")
		progress := p.Metrics.Gauge("chunkd_scrub_pass_progress", "Fraction of the current scrub pass done.")
		var refresh func()
		refresh = func() {
			heartbeats.Set(float64(n.Stats().Heartbeats))
			sc := n.Scrub()
			scrubBytes.Mirror(sc.Bytes)
			scrubCorrupt.Mirror(sc.Corrupt)
			corrupt.Mirror(n.Stats().Corrupt)
			lastPass.Set(sc.LastPass.Seconds())
			if sc.Total > 0 {
				progress.Set(float64(sc.Done) / float64(sc.Total))
			}
			if u, err := store.Usage(context.Background()); err == nil {
				chunks.Set(float64(u.Chunks))
				used.Set(float64(u.Bytes))
			}
			p.Clock.AfterFunc(time.Second, refresh)
		}
		refresh()
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-node:", err)
		os.Exit(1)
	}
}

func envDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return def
}

func envInt(key string, def int64) int64 {
	if n, err := strconv.ParseInt(os.Getenv(key), 10, 64); err == nil {
		return n
	}
	return def
}
