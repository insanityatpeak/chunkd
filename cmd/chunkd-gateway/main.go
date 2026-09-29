// Command chunkd-gateway is the client-facing HTTP entry point. It runs the
// same client library as the CLI, so data flows between the gateway and
// storage nodes directly; the metadata server is never on the data path.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/gateway"
	"github.com/insanityatpeak/chunkd/internal/real/grpcnet"
	"github.com/insanityatpeak/chunkd/internal/real/server"
)

func main() {
	id := flag.String("id", server.Env("CHUNKD_ID", "gateway-1"), "gateway ID")
	httpAddr := flag.String("http", server.Env("CHUNKD_HTTP", ":8080"), "HTTP listen address")
	metaID := flag.String("meta-id", server.Env("CHUNKD_META_ID", "meta-1"), "metadata server ID")
	metaAddr := flag.String("meta", server.Env("CHUNKD_META", "localhost:7000"), "metadata server gRPC address")
	uiDir := flag.String("ui", server.Env("CHUNKD_UI", ""), "serve the dashboard's static build from this directory at /")
	flag.Parse()

	caller := grpcnet.NewCaller(map[iface.NodeID]string{iface.NodeID(*metaID): *metaAddr}, 30*time.Second)
	defer caller.Close()
	api := client.New(caller, client.Options{Meta: iface.NodeID(*metaID), MetaAddr: *metaAddr})

	cfg := server.Config{ID: iface.NodeID(*id), AdminAddr: *httpAddr}
	err := server.Run("gateway", cfg, func(p *server.Process) error {
		p.Healthy = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := api.Cluster(ctx, 0); err != nil {
				return fmt.Errorf("metadata server unreachable: %w", err)
			}
			return nil
		}
		p.HTTP = gateway.Handler(api, p.Log)
		if *uiDir != "" {
			p.HTTP = gateway.WithUI(p.HTTP, *uiDir)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd-gateway:", err)
		os.Exit(1)
	}
}
