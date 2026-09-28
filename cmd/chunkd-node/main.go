// Command chunkd-node runs a storage node: it stores chunks by hash, serves
// them to clients, and heartbeats and reports to the metadata server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/blockstore"
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
	flag.Parse()

	store, err := blockstore.Open(*dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-node:", err)
		os.Exit(1)
	}
	cfg := node.DefaultConfig(iface.NodeID(*id), iface.NodeID(*metaID), *rack)
	cfg.Addr = *advertise

	sc := server.Config{
		ID: cfg.ID, GRPCAddr: *grpcAddr, Advertise: *advertise, AdminAddr: *adminAddr,
		Peers: map[iface.NodeID]string{cfg.Meta: *metaAddr},
	}
	err = server.Run("node", sc, func(p *server.Process) error {
		n := node.New(node.Deps{Clock: p.Clock, Net: p.Net, Store: store, Rand: p.Rand, Log: p.Log}, cfg)
		n.Start()

		heartbeats := p.Metrics.Gauge("chunkd_node_heartbeats_sent", "Heartbeats sent to the metadata server.")
		chunks := p.Metrics.Gauge("chunkd_node_chunks", "Chunks stored.")
		used := p.Metrics.Gauge("chunkd_node_used_bytes", "Bytes stored.")
		var refresh func()
		refresh = func() {
			heartbeats.Set(float64(n.Stats().Heartbeats))
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
