// Command chunkd-gateway is the client-facing HTTP entry point. In this phase
// it serves /healthz, which is healthy only while the metadata server answers
// a gRPC Ping.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/server"
	rpcv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/rpc/v1"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func main() {
	id := flag.String("id", server.Env("CHUNKD_ID", "gateway-1"), "gateway ID")
	httpAddr := flag.String("http", server.Env("CHUNKD_HTTP", ":8080"), "HTTP listen address")
	metaAddr := flag.String("meta", server.Env("CHUNKD_META", "localhost:7000"), "metadata server gRPC address")
	flag.Parse()

	conn, err := grpc.NewClient(*metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-gateway:", err)
		os.Exit(1)
	}
	defer conn.Close()
	ping := rpcv1.NewPingServiceClient(conn)

	cfg := server.Config{ID: iface.NodeID(*id), AdminAddr: *httpAddr}
	err = server.Run("gateway", cfg, func(p *server.Process) error {
		var seq atomic.Uint64 // health checks run on concurrent HTTP goroutines
		p.Healthy = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := ping.Ping(server.OutgoingContext(ctx), &chunkdv1.PingRequest{From: *id, Seq: seq.Add(1)}); err != nil {
				return fmt.Errorf("metadata server unreachable: %w", err)
			}
			return nil
		}
		p.HTTP = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			fmt.Fprintln(w, "chunkd gateway; see /healthz and /metrics")
		})
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-gateway:", err)
		os.Exit(1)
	}
}
