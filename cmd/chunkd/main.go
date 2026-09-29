// Command chunkd is the operator CLI.
//
//	chunkd put <local-file> <path>          upload (overwrites; -expected N for compare-and-swap)
//	chunkd get <path> <local-file>          download and verify (-expect-sha256 to pin the content)
//	chunkd ls [prefix]                      list files
//	chunkd stat <path>                      version, hash and chunk placement
//	chunkd rm <path>                        delete
//	chunkd cluster [status]                 nodes, detector state and replication health
//	chunkd ping <grpc-addr>                 ping a process
//	chunkd probe <http-url>                 exit 0 if the URL returns 200 (health checks)
//
// By default it talks to the gateway (-gateway, $CHUNKD_GATEWAY). With -meta
// it talks gRPC to the metadata server and nodes directly.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/grpcnet"
	rpcv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/rpc/v1"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func main() {
	fs := flag.NewFlagSet("chunkd", flag.ExitOnError)
	gatewayURL := fs.String("gateway", envOr("CHUNKD_GATEWAY", "http://localhost:8080"), "gateway base URL")
	metaAddr := fs.String("meta", "", "metadata server gRPC address (direct mode, bypasses the gateway)")
	expected := fs.Uint64("expected", 0, "put/rm: expected live version (compare-and-swap); 0 with put means overwrite")
	create := fs.Bool("create", false, "put: fail if the path exists")
	expectSHA := fs.String("expect-sha256", "", "get: fail unless the content has this SHA-256")
	asJSON := fs.Bool("json", false, "print JSON")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: chunkd [flags] put|get|ls|stat|rm|cluster|ping|probe ...")
		fs.PrintDefaults()
	}
	// Flags may come before or after the command.
	var args []string
	rest := os.Args[1:]
	for len(rest) > 0 {
		fs.Parse(rest)
		rest = fs.Args()
		if len(rest) > 0 {
			args = append(args, rest[0])
			rest = rest[1:]
		}
	}
	if len(args) == 0 {
		fs.Usage()
		os.Exit(2)
	}

	var api client.API
	if *metaAddr != "" {
		caller := grpcnet.NewCaller(map[iface.NodeID]string{"meta-1": *metaAddr}, 30*time.Second)
		defer caller.Close()
		api = client.New(caller, client.Options{Meta: "meta-1", MetaAddr: *metaAddr})
	} else {
		api = httpclient.New(*gatewayURL)
	}
	ctx := context.Background()
	out := printer{json: *asJSON}

	var err error
	switch cmd := args[0]; {
	case cmd == "put" && len(args) == 3:
		opts := client.PutOptions{Overwrite: *expected == 0 && !*create, ExpectedVersion: *expected}
		err = put(ctx, api, args[1], args[2], opts, out)
	case cmd == "get" && len(args) == 3:
		err = get(ctx, api, args[1], args[2], *expectSHA, out)
	case cmd == "ls" && len(args) <= 2:
		prefix := "/"
		if len(args) == 2 {
			prefix = args[1]
		}
		var files []client.FileInfo
		if files, err = api.List(ctx, prefix); err == nil {
			out.list(files)
		}
	case cmd == "stat" && len(args) == 2:
		var m client.Manifest
		if m, err = api.Stat(ctx, args[1]); err == nil {
			out.manifest(m)
		}
	case cmd == "rm" && len(args) == 2:
		var v uint64
		if v, err = api.Delete(ctx, args[1], *expected); err == nil {
			fmt.Printf("deleted %s (tombstone v%d)\n", args[1], v)
		}
	case cmd == "cluster" && (len(args) == 1 || len(args) == 2 && args[1] == "status"):
		var c client.Cluster
		if c, err = api.Cluster(ctx, 0); err == nil {
			out.cluster(c)
		}
	case cmd == "ping" && len(args) == 2:
		err = ping(args[1])
	case cmd == "probe" && len(args) == 2:
		err = probe(args[1])
	default:
		fs.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd:", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func put(ctx context.Context, api client.API, local, path string, opts client.PutOptions, out printer) error {
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	start := time.Now()
	m, err := api.Put(ctx, path, f, fi.Size(), opts)
	if err != nil {
		return err
	}
	if out.json {
		out.print(m)
		return nil
	}
	fmt.Printf("put %s v%d  %s in %d chunks  %.1fs\nsha256 %s\n", m.Path, m.Version, human(m.Size), m.Chunks, time.Since(start).Seconds(), m.SHA256)
	return nil
}

// get writes to a temp file next to the target and renames it only after
// every check passes, so a failed download never leaves a plausible file.
func get(ctx context.Context, api client.API, path, local, expectSHA string, out printer) error {
	tmp, err := os.CreateTemp(filepath.Dir(local), ".chunkd-get-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	m, err := api.Get(ctx, path, io.MultiWriter(tmp, h))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if expectSHA != "" && !strings.EqualFold(got, expectSHA) {
		return fmt.Errorf("content sha256 %s, expected %s", got, expectSHA)
	}
	if err := os.Rename(tmp.Name(), local); err != nil {
		return err
	}
	if out.json {
		out.print(m)
		return nil
	}
	fmt.Printf("got %s v%d  %s  sha256 %s  verified\n", m.Path, m.Version, human(m.Size), got)
	return nil
}

type printer struct{ json bool }

func (p printer) print(v any) {
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	_ = e.Encode(v)
}

func (p printer) list(files []client.FileInfo) {
	if p.json {
		p.print(files)
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "PATH\tVERSION\tSIZE\tCHUNKS\tSHA256")
	for _, f := range files {
		fmt.Fprintf(w, "%s\tv%d\t%s\t%d\t%.16s\n", f.Path, f.Version, human(f.Size), f.Chunks, f.SHA256)
	}
	w.Flush()
}

func (p printer) manifest(m client.Manifest) {
	if p.json {
		p.print(m)
		return
	}
	fmt.Printf("%s v%d  %s  chunk size %s\nsha256 %s\n", m.Path, m.Version, human(m.Size), human(int64(m.ChunkSize)), m.SHA256)
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CHUNK\tID\tSIZE\tREPLICAS")
	for _, c := range m.Chunk {
		fmt.Fprintf(w, "%d\t%.16s\t%s\t%s\n", c.Index, c.ID, human(c.Size), strings.Join(c.Replicas, ","))
	}
	w.Flush()
}

func (p printer) cluster(c client.Cluster) {
	if p.json {
		p.print(c)
		return
	}
	fmt.Printf("%d files, %s logical\n\n", c.Files, human(c.LogicalBytes))
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tRACK\tSTATE\tLAST BEAT\tCHUNKS\tUSED")
	for _, n := range c.Nodes {
		state := n.State
		if n.Draining {
			state += ",draining"
		}
		age := (time.Duration(n.HeartbeatAgeMs) * time.Millisecond).Round(100 * time.Millisecond)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s ago\t%d\t%s\n", n.ID, n.Rack, state, age, n.Chunks, human(n.UsedBytes))
	}
	w.Flush()
	h := c.Health
	fmt.Printf("\nchunks %d: under-replicated %d, over-replicated %d, lost %d\n", h.Chunks, h.UnderReplicated, h.OverReplicated, h.Lost)
	var dist []string
	for i, n := range h.Replicas {
		label := fmt.Sprint(i)
		if i == len(h.Replicas)-1 {
			label += "+"
		}
		dist = append(dist, fmt.Sprintf("%s:%d", label, n))
	}
	fmt.Printf("copies per chunk on alive nodes  %s\n", strings.Join(dist, "  "))
	fmt.Printf("repair: %d queued, %d in flight, %d inside the delay; %d copies (%s), %d trimmed, %d timed out, %d failed\n",
		h.RepairQueued, h.RepairInFlight, h.RepairWaiting, h.RepairCompleted, human(int64(h.RepairBytes)), h.RepairTrimmed, h.RepairTimedOut, h.RepairFailed)
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
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
		return errors.New(url + ": " + resp.Status)
	}
	return nil
}
