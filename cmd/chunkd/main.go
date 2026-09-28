// Command chunkd is the operator CLI.
//
//	chunkd ping <grpc-addr>   ping a metadata server or storage node
//	chunkd probe <http-url>   exit 0 if the URL returns 200 (container health checks)
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rpcv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/rpc/v1"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: chunkd ping <grpc-addr> | chunkd probe <http-url>")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "ping":
		err = ping(os.Args[2])
	case "probe":
		err = probe(os.Args[2])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd:", err)
		os.Exit(1)
	}
}

func ping(addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	resp, err := rpcv1.NewPingServiceClient(conn).Ping(ctx, &chunkdv1.PingRequest{From: "cli", Seq: 1})
	if err != nil {
		return err
	}
	fmt.Printf("pong from %s in %v\n", resp.GetFrom(), time.Since(start).Round(time.Microsecond))
	return nil
}

func probe(url string) error {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return nil
}
