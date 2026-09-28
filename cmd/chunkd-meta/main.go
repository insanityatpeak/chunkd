// Command chunkd-meta runs a metadata server. In this phase it tracks storage
// node heartbeats; file metadata and Raft replication come in later phases.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/heartbeat"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/server"
)

func main() {
	id := flag.String("id", server.Env("CHUNKD_ID", "meta-1"), "node ID")
	grpcAddr := flag.String("grpc", server.Env("CHUNKD_GRPC", ":7000"), "gRPC listen address")
	advertise := flag.String("advertise", server.Env("CHUNKD_ADVERTISE", "localhost:7000"), "gRPC address peers dial")
	adminAddr := flag.String("admin", server.Env("CHUNKD_ADMIN", ":9000"), "HTTP address for /metrics, /healthz, /nodes")
	deadAfter := flag.Duration("dead-after", 3*time.Second, "heartbeat silence before a node counts as dead")
	flag.Parse()

	cfg := server.Config{ID: iface.NodeID(*id), GRPCAddr: *grpcAddr, Advertise: *advertise, AdminAddr: *adminAddr}
	err := server.Run("meta", cfg, func(p *server.Process) error {
		tr := heartbeat.NewTracker(heartbeat.Deps{Clock: p.Clock, Net: p.Net, Rand: p.Rand, Log: p.Log}, p.ID)
		tr.Start()

		known := p.Metrics.Gauge("chunkd_meta_nodes_known", "Storage nodes that have ever sent a heartbeat.")
		alive := p.Metrics.Gauge("chunkd_meta_nodes_alive", "Storage nodes heard from within dead-after.")
		var refresh func()
		refresh = func() {
			n := 0
			peers := tr.Peers()
			for _, pr := range peers {
				if tr.Alive(pr.ID, *deadAfter) {
					n++
				}
			}
			known.Set(float64(len(peers)))
			alive.Set(float64(n))
			p.Clock.AfterFunc(time.Second, refresh)
		}
		refresh()

		p.HTTP = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/nodes" {
				http.NotFound(w, r)
				return
			}
			type view struct {
				heartbeat.PeerState
				Alive bool `json:"alive"`
			}
			var out []view
			p.Loop.Do(func() {
				for _, pr := range tr.Peers() {
					out = append(out, view{pr, tr.Alive(pr.ID, *deadAfter)})
				}
			})
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		})
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-meta:", err)
		os.Exit(1)
	}
}
