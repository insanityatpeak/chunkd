// Command chunkd-meta runs the metadata server: namespace, versions, chunk
// placement, and the node view rebuilt from heartbeats and block reports.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/metastore"
	"github.com/insanityatpeak/chunkd/internal/real/server"
)

func main() {
	id := flag.String("id", server.Env("CHUNKD_ID", "meta-1"), "node ID")
	grpcAddr := flag.String("grpc", server.Env("CHUNKD_GRPC", ":7000"), "gRPC listen address")
	advertise := flag.String("advertise", server.Env("CHUNKD_ADVERTISE", "localhost:7000"), "gRPC address peers dial")
	adminAddr := flag.String("admin", server.Env("CHUNKD_ADMIN", ":9000"), "HTTP address for /metrics, /healthz, /nodes")
	dataDir := flag.String("data", server.Env("CHUNKD_DATA", "data/meta"), "directory for the WAL and snapshots")
	cfg := meta.DefaultConfig("")
	flag.IntVar(&cfg.Replicas, "replicas", cfg.Replicas, "replicas per chunk")
	flag.IntVar(&cfg.MinReplicas, "min-replicas", cfg.MinReplicas, "reported replicas required to commit")
	flag.IntVar(&cfg.ChunkSize, "chunk-size", cfg.ChunkSize, "chunk size in bytes")
	flag.DurationVar(&cfg.Detector.SuspectAfter, "suspect-after", cfg.Detector.SuspectAfter, "heartbeat silence before a node is suspect")
	flag.DurationVar(&cfg.Detector.DeadAfter, "dead-after", cfg.Detector.DeadAfter, "heartbeat silence before a node is dead")
	flag.Parse()
	cfg.ID = iface.NodeID(*id)

	store, err := metastore.Open(*dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-meta:", err)
		os.Exit(1)
	}
	defer store.Close()

	sc := server.Config{ID: cfg.ID, GRPCAddr: *grpcAddr, Advertise: *advertise, AdminAddr: *adminAddr}
	err = server.Run("meta", sc, func(p *server.Process) error {
		srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: p.Clock, Net: p.Net, Store: store, Rand: p.Rand, Log: p.Log}, cfg)
		if err != nil {
			return err
		}
		srv.Start()

		alive := p.Metrics.Gauge("chunkd_meta_nodes_alive", "Storage nodes the failure detector considers alive.")
		files := p.Metrics.Gauge("chunkd_meta_files", "Live files.")
		applied := p.Metrics.Gauge("chunkd_meta_applied_index", "Index of the last applied log entry.")
		var refresh func()
		refresh = func() {
			n := 0
			for _, ns := range srv.Cluster().Nodes() {
				if srv.Cluster().Alive(ns.ID) {
					n++
				}
			}
			alive.Set(float64(n))
			files.Set(float64(len(srv.State().List("/"))))
			applied.Set(float64(srv.Applied()))
			p.Clock.AfterFunc(time.Second, refresh)
		}
		refresh()

		p.HTTP = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/nodes" {
				http.NotFound(w, r)
				return
			}
			type view struct {
				meta.NodeState
				Alive bool `json:"alive"`
			}
			var out []view
			p.Loop.Do(func() {
				for _, ns := range srv.Cluster().Nodes() {
					out = append(out, view{ns, srv.Cluster().Alive(ns.ID)})
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
