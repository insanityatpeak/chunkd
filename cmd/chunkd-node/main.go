// Command chunkd-node runs a storage node. In this phase it only heartbeats
// to the metadata server; chunk storage arrives with the data path.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/heartbeat"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/server"
)

func main() {
	id := flag.String("id", server.Env("CHUNKD_ID", "node-1"), "node ID")
	grpcAddr := flag.String("grpc", server.Env("CHUNKD_GRPC", ":7000"), "gRPC listen address")
	advertise := flag.String("advertise", server.Env("CHUNKD_ADVERTISE", "localhost:7001"), "gRPC address peers dial")
	adminAddr := flag.String("admin", server.Env("CHUNKD_ADMIN", ":9000"), "HTTP address for /metrics and /healthz")
	metaID := flag.String("meta-id", server.Env("CHUNKD_META_ID", "meta-1"), "metadata server ID")
	metaAddr := flag.String("meta", server.Env("CHUNKD_META", "localhost:7000"), "metadata server gRPC address")
	interval := flag.Duration("heartbeat", time.Second, "heartbeat interval")
	flag.Parse()

	cfg := server.Config{
		ID: iface.NodeID(*id), GRPCAddr: *grpcAddr, Advertise: *advertise, AdminAddr: *adminAddr,
		Peers: map[iface.NodeID]string{iface.NodeID(*metaID): *metaAddr},
	}
	err := server.Run("node", cfg, func(p *server.Process) error {
		s := heartbeat.NewSender(heartbeat.Deps{Clock: p.Clock, Net: p.Net, Rand: p.Rand, Log: p.Log}, p.ID, iface.NodeID(*metaID), *interval)
		s.Start()

		sent := p.Metrics.Gauge("chunkd_node_heartbeats_sent", "Heartbeats sent to the metadata server.")
		acked := p.Metrics.Gauge("chunkd_node_heartbeats_acked", "Heartbeat acknowledgements received.")
		var refresh func()
		refresh = func() {
			st := s.Stats()
			sent.Set(float64(st.Sent))
			acked.Set(float64(st.Acked))
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
