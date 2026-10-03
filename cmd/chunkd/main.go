// Command chunkd is the operator CLI.
//
//	chunkd put <local-file> <path>          upload (overwrites; -expected N for compare-and-swap; -resume continues a broken put)
//	chunkd get <path> <local-file>          download and verify (-expect-sha256 to pin the content)
//	chunkd ls [prefix]                      list files
//	chunkd stat <path>                      version, hash and chunk placement
//	chunkd rm <path>                        delete
//	chunkd cluster [status]                 nodes, detector state and replication health
//	chunkd node drain|undrain|decommission <id>   retire a storage node (decommission -wait 10m polls until safe)
//	chunkd keygen -name alice [-quota N] [-admin]   a new API key and its entry for the gateway's keys file
//	chunkd diff|restore|retain|bench <path> <from> <to>          chunk-level difference of two versions (what a rewrite would send)
//	chunkd restore <path> <version>         make an old version the live one again
//	chunkd retain <path> <epochs>           keep the path's retired versions this many GC epochs (0: cluster default)
//	chunkd bench [-size MiB -count N]       put, get and remove random files, report MiB/s
//	chunkd completion bash|zsh|powershell   shell completion script
//	chunkd ping <grpc-addr>                 ping a process
//	chunkd probe <http-url>                 exit 0 if the URL returns 200 (health checks)
//	chunkd debug corrupt                    flip a byte in -n chunk files under -root (fault injection;
//	                                        run on the node's host or with docker compose exec)
//
// Exit codes: 0 ok, 1 other failure, 2 usage, 3 not found, 4 conflict, 5 quota exceeded,
// 6 denied, 7 unavailable, 8 corrupt.
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
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/blockstore"
	"github.com/insanityatpeak/chunkd/internal/real/grpcnet"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
	"github.com/insanityatpeak/chunkd/internal/real/server"
	rpcv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/rpc/v1"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

func main() {
	fs := flag.NewFlagSet("chunkd", flag.ExitOnError)
	gatewayURL := fs.String("gateway", envOr("CHUNKD_GATEWAY", "http://localhost:8080"), "gateway base URL")
	metaAddr := fs.String("meta", "", "metadata server gRPC address, or a group as id=host:port,... (direct mode, bypasses the gateway)")
	expected := fs.Uint64("expected", 0, "put/rm: expected live version (compare-and-swap); 0 with put means overwrite")
	create := fs.Bool("create", false, "put: fail if the path exists")
	lww := fs.Bool("lww", false, "put: last writer wins, no version check (a concurrent update is lost)")
	resume := fs.Bool("resume", false, "put: continue the upload a broken put left, if the file is unchanged")
	redundancy := fs.String("redundancy", "replicated", "put: replicated (3 copies) or ec-4+2 (4 data + 2 parity shards on 6 nodes, 1.5x storage)")
	version := fs.Uint64("version", 0, "get: download this version instead of the live one; undelete: the version to restore (default newest)")
	expectSHA := fs.String("expect-sha256", "", "get: fail unless the content has this SHA-256")
	apiKey := fs.String("key", envOr("CHUNKD_KEY", ""), "API key, when the gateway requires one")
	asJSON := fs.Bool("json", false, "print JSON")
	root := fs.String("root", envOr("CHUNKD_DATA", "data/node"), "debug corrupt: the node's chunk directory")
	rotN := fs.Int("n", 1, "debug corrupt: number of chunks")
	pick := fs.Uint64("pick", 0, "debug corrupt: first chunk, as an index into the chunks in ID order")
	wait := fs.Duration("wait", 0, "node decommission: keep retrying this long while the node's chunks are still short elsewhere")
	benchSize := fs.Int64("size", 16, "bench: MiB per file")
	benchCount := fs.Int("count", 3, "bench: files to put and get")
	keyName := fs.String("name", "", "keygen: the key's name; its namespace unless -namespace is set")
	keyNS := fs.String("namespace", "", "keygen: the path segment the key owns")
	keyQuota := fs.Int64("quota", 0, "keygen: byte quota of the namespace (0: unlimited)")
	keyAdmin := fs.Bool("admin", false, "keygen: an admin key (every path, node controls)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: chunkd [flags] put|get|log|ls|stat|rm|undelete|cluster|node|keygen|diff|restore|bench|completion|ping|probe|debug corrupt ...")
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
		os.Exit(exitUsage)
	}

	var api client.API
	if *metaAddr != "" {
		ids, addrs, err := server.ParseGroup(*metaAddr, "meta-1")
		if err != nil {
			fmt.Fprintln(os.Stderr, "chunkd:", err)
			os.Exit(2)
		}
		caller := grpcnet.NewCaller(addrs, 30*time.Second)
		defer caller.Close()
		opts := client.Options{Rand: runtime.NewRand()}
		for _, id := range ids {
			opts.Peers = append(opts.Peers, client.MetaPeer{ID: id, Addr: addrs[id]})
		}
		api = client.New(caller, opts)
	} else {
		hc := httpclient.New(*gatewayURL)
		hc.Key = *apiKey
		api = hc
	}
	ctx := context.Background()
	out := printer{json: *asJSON}

	var err error
	switch cmd := args[0]; {
	case cmd == "put" && len(args) == 3:
		opts := client.PutOptions{Overwrite: *expected == 0 && !*create, ExpectedVersion: *expected, LastWriterWins: *lww}
		if opts.Redundancy, err = client.ParseRedundancy(*redundancy); err == nil {
			err = putResumable(ctx, api, args[1], args[2], opts, *resume, out)
		}
	case cmd == "get" && len(args) == 3:
		err = get(ctx, api, args[1], args[2], *version, *expectSHA, out)
	case cmd == "undelete" && len(args) == 2:
		var v uint64
		if v, err = api.Undelete(ctx, args[1], *version); err == nil {
			fmt.Printf("restored %s as v%d\n", args[1], v)
		}
	case cmd == "log" && len(args) == 2:
		var vs []client.VersionInfo
		if vs, err = api.Log(ctx, args[1]); err == nil {
			out.log(vs)
		}
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
	case cmd == "node" && len(args) == 3:
		err = nodeAdmin(ctx, api, args[1], args[2], *wait, out)
	case cmd == "diff" && len(args) == 4:
		var from, to uint64
		if from, err = strconv.ParseUint(args[2], 10, 64); err == nil {
			if to, err = strconv.ParseUint(args[3], 10, 64); err == nil {
				var d Diff
				if d, err = diffVersions(ctx, api, args[1], from, to); err == nil {
					out.diff(d)
				}
			}
		}
	case cmd == "restore" && len(args) == 3:
		var v, restored uint64
		if v, err = strconv.ParseUint(args[2], 10, 64); err == nil {
			if restored, err = api.Undelete(ctx, args[1], v); err == nil {
				fmt.Printf("restored %s v%d as v%d\n", args[1], v, restored)
			}
		}
	case cmd == "retain" && len(args) == 3:
		var n uint64
		if n, err = strconv.ParseUint(args[2], 10, 32); err == nil {
			if err = api.SetRetention(ctx, args[1], uint32(n)); err == nil {
				fmt.Printf("%s keeps retired versions for %d epochs\n", args[1], n)
			}
		}
	case cmd == "bench" && len(args) == 1:
		opts := client.PutOptions{Overwrite: true}
		if opts.Redundancy, err = client.ParseRedundancy(*redundancy); err == nil {
			err = bench(ctx, api, *benchSize<<20, *benchCount, opts, out)
		}
	case cmd == "completion" && len(args) == 2:
		err = completion(args[1])
	case cmd == "keygen" && len(args) == 1:
		err = keygen(*keyName, *keyNS, *keyQuota, *keyAdmin)
	case cmd == "ping" && len(args) == 2:
		err = ping(args[1])
	case cmd == "probe" && len(args) == 2:
		err = probe(args[1])
	case cmd == "debug" && len(args) == 2 && args[1] == "corrupt":
		var ids []iface.ChunkID
		ids, err = blockstore.Rot(*root, *rotN, *pick)
		for _, id := range ids {
			fmt.Println(id)
		}
	default:
		fs.Usage()
		os.Exit(exitUsage)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "chunkd:", err)
		os.Exit(exitCode(err))
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// get writes to a temp file next to the target and renames it only after
// every check passes, so a failed download never leaves a plausible file.
func get(ctx context.Context, api client.API, path, local string, version uint64, expectSHA string, out printer) error {
	tmp, err := os.CreateTemp(filepath.Dir(local), ".chunkd-get-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	prog := newProgress()
	m, err := api.GetVersion(ctx, path, version, io.MultiWriter(tmp, h, prog))
	prog.done()
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

func (p printer) log(vs []client.VersionInfo) {
	if p.json {
		p.print(vs)
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "VERSION\tSIZE\tCHUNKS\tSHA256")
	for _, v := range vs {
		if v.Deleted {
			fmt.Fprintf(w, "v%d\t-\t-\tdeleted\n", v.Version)
			continue
		}
		fmt.Fprintf(w, "v%d\t%s\t%d\t%.16s\n", v.Version, human(v.Size), v.Chunks, v.SHA256)
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
		if n.Admin != "" && n.Admin != "active" {
			state += "," + n.Admin
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

// nodeAdmin drains, undrains or decommissions a storage node. Decommission is
// refused while any of the node's chunks lacks RF copies elsewhere; with
// wait it retries until allowed or the time is up.
func nodeAdmin(ctx context.Context, api client.API, action, node string, wait time.Duration, out printer) error {
	state, ok := map[string]string{"drain": "draining", "undrain": "active", "decommission": "decommissioned"}[action]
	if !ok {
		return fmt.Errorf("node %s: want drain, undrain or decommission", action)
	}
	deadline := time.Now().Add(wait)
	for {
		res, err := api.NodeAdmin(ctx, node, state)
		if err == nil {
			if out.json {
				out.print(res)
				return nil
			}
			fmt.Printf("%s is %s\n", node, res.Admin)
			if res.Warning != "" {
				fmt.Printf("warning: %s\n", res.Warning)
			}
			return nil
		}
		if iface.CodeOf(err) != iface.CodeConflict || state != "decommissioned" || time.Now().After(deadline) {
			return err
		}
		fmt.Fprintf(os.Stderr, "waiting: %v\n", err)
		time.Sleep(2 * time.Second)
	}
}
