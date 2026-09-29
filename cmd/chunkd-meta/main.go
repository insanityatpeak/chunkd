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
		nodes := p.Metrics.GaugeVec("chunkd_meta_nodes", "Storage nodes by failure-detector state.", "state")
		replicas := p.Metrics.GaugeVec("chunkd_meta_chunk_replicas", "Chunks by number of copies on alive nodes (replication-factor distribution).", "replicas")
		under := p.Metrics.Gauge("chunkd_meta_under_replicated_chunks", "Chunks with fewer copies on alive nodes than the replication factor.")
		lost := p.Metrics.Gauge("chunkd_meta_lost_chunks", "Chunks with no copy on an alive or suspect node.")
		queue := p.Metrics.Gauge("chunkd_repair_queue_length", "Chunks queued for a repair copy.")
		inflight := p.Metrics.Gauge("chunkd_repair_in_flight", "Repair copies in progress.")
		repairBytes := p.Metrics.Counter("chunkd_repair_bytes_total", "Bytes copied by repair; rate() gives repair bytes/sec.")
		repairCopies := p.Metrics.Counter("chunkd_repair_copies_total", "Repair copies completed.")
		trimmed := p.Metrics.Counter("chunkd_repair_trimmed_total", "Over-replicated copies removed.")
		var refresh func()
		refresh = func() {
			n := 0
			for _, ns := range srv.Cluster().Nodes() {
				if srv.Cluster().Alive(ns.ID) {
					n++
				}
			}
			alive.Set(float64(n))
			h := srv.Health()
			for _, st := range []string{"alive", "suspect", "dead"} {
				nodes.Set(st, float64(h.Nodes[st]))
			}
			for i, c := range h.Replicas {
				label := fmt.Sprint(i)
				if i == len(h.Replicas)-1 {
					label += "+"
				}
				replicas.Set(label, float64(c))
			}
			under.Set(float64(h.UnderReplicated))
			lost.Set(float64(h.Lost))
			queue.Set(float64(h.Repair.Queued))
			inflight.Set(float64(h.Repair.InFlight))
			repairBytes.Mirror(h.Repair.Bytes)
			repairCopies.Mirror(h.Repair.Completed)
			trimmed.Mirror(h.Repair.Trimmed)
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
