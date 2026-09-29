// Command chunkd-demo breaks the compose cluster on purpose and narrates the
// recovery: upload a file, kill the node holding most of its replicas, print
// the failure detector's and the repair scheduler's timeline, re-download,
// verify the SHA-256, then restart the node and watch the extra copies go.
//
//	go run ./tools/task demo                 from a clone (uses the docker CLI)
//	docker compose run --rm demo             nothing but Docker (uses the Docker API socket)
//
// It exits non-zero if any step misses its deadline.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
)

func main() {
	gateway := flag.String("gateway", envOr("CHUNKD_GATEWAY", "http://localhost:8080"), "gateway URL")
	project := flag.String("project", envOr("COMPOSE_PROJECT_NAME", "chunkd"), "compose project name")
	socket := flag.String("docker", "/var/run/docker.sock", "Docker API socket; if absent, the docker CLI is used")
	size := flag.Int64("size", 20<<20, "bytes to upload")
	flag.Parse()

	d := &demo{api: httpclient.New(*gateway), docker: newDocker(*socket, *project), start: time.Now()}
	if err := d.run(*size); err != nil {
		fmt.Fprintf(os.Stderr, "\ndemo failed: %v\n", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type demo struct {
	api    *httpclient.Client
	docker docker
	start  time.Time
	killed time.Time
	seq    uint64
}

func (d *demo) say(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

// at prefixes a line with the time since the node was killed.
func (d *demo) at(format string, args ...any) {
	fmt.Printf("  +%5.1fs  %s\n", time.Since(d.killed).Seconds(), fmt.Sprintf(format, args...))
}

func (d *demo) cluster(ctx context.Context) (client.Cluster, error) {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return d.api.Cluster(c, d.seq)
}

func (d *demo) run(size int64) error {
	ctx := context.Background()
	d.say("chunkd demo: break a node, watch the cluster repair itself\n")

	// 1. Cluster up, every node alive.
	if err := d.until(ctx, 60*time.Second, "all nodes alive", func(c client.Cluster) bool {
		return len(c.Nodes) > 0 && !slices.ContainsFunc(c.Nodes, func(n client.NodeInfo) bool { return n.State != "alive" }) &&
			c.Health.UnderReplicated == 0 && c.Health.OverReplicated == 0
	}); err != nil {
		return err
	}

	// 2. Upload.
	data := make([]byte, size)
	_, _ = rand.Read(data)
	sum := sha256.Sum256(data)
	path := fmt.Sprintf("/demo/%s.bin", time.Now().Format("150405"))
	m, err := d.api.Put(ctx, path, bytes.NewReader(data), size, client.PutOptions{Overwrite: true})
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	d.say("uploaded %s: %d MiB, %d chunks, SHA-256 %s…", path, size>>20, m.Chunks, hex.EncodeToString(sum[:])[:16])
	st, err := d.api.Stat(ctx, path)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	held := map[string]int{}
	for _, ch := range st.Chunk {
		d.say("  chunk %d  %s…  on %s", ch.Index, ch.ID[:12], strings.Join(ch.Replicas, ", "))
		for _, n := range ch.Replicas {
			held[n]++
		}
	}

	// 3. Kill the node holding most replicas of this file.
	victim := ""
	for n, k := range held {
		if victim == "" || k > held[victim] || k == held[victim] && n > victim {
			victim = n
		}
	}
	c, err := d.cluster(ctx)
	if err != nil {
		return err
	}
	d.seq = c.EventSeq
	d.say("\nkilling %s (holds %d of this file's %d replicas, %d chunks in all)", victim, held[victim], len(st.Chunk)*3, nodeChunks(c, victim))
	if err := d.docker.kill(victim); err != nil {
		return fmt.Errorf("kill %s: %w", victim, err)
	}
	d.killed = time.Now()

	// 4. Detector, delay, repair: narrate until RF is back with the node still dead.
	base, baseBytes := c.Health.RepairCompleted, c.Health.RepairBytes
	lastLine := time.Time{}
	err = d.until(ctx, 90*time.Second, "replication restored", func(c client.Cluster) bool {
		for _, e := range c.Events {
			if e.Kind == "node" && e.Node == victim {
				d.at("%s %s", e.Node, e.Text)
				if strings.HasSuffix(e.Text, "→ dead") {
					d.at("repair waits 20 s in case %s is only rebooting", victim)
				}
			}
		}
		h := c.Health
		done := h.RepairCompleted - base
		if (h.RepairInFlight > 0 || done > 0 && h.UnderReplicated > 0) && time.Since(lastLine) >= time.Second {
			d.at("repair: %d chunks below 3 copies, %d in flight, %d done", h.UnderReplicated, h.RepairInFlight, done)
			lastLine = time.Now()
		}
		dead := slices.ContainsFunc(c.Nodes, func(n client.NodeInfo) bool { return n.ID == victim && n.State == "dead" })
		if dead && h.UnderReplicated == 0 && done > 0 {
			d.at("repair: %d copies done, %d MiB copied", done, (h.RepairBytes-baseBytes)>>20)
			return true
		}
		return false
	})
	if err != nil {
		return err
	}
	d.at("every chunk back at 3 copies, %s still down", victim)

	// 5. Read it back; the client checks every chunk and the whole file.
	var buf bytes.Buffer
	if _, err := d.api.Get(ctx, path, &buf); err != nil {
		return fmt.Errorf("download: %w", err)
	}
	got := sha256.Sum256(buf.Bytes())
	if got != sum {
		return fmt.Errorf("downloaded SHA-256 %x, uploaded %x", got, sum)
	}
	d.say("\ndownloaded %s with %s down: SHA-256 verified ✓ %s…", path, victim, hex.EncodeToString(got[:])[:16])

	// 6. Bring it back: its old copies are now extra, and get trimmed.
	c, err = d.cluster(ctx)
	if err != nil {
		return err
	}
	trimmed := c.Health.RepairTrimmed
	d.seq = c.EventSeq
	if err := d.docker.start(victim); err != nil {
		return fmt.Errorf("start %s: %w", victim, err)
	}
	d.say("\nrestarted %s over its old disk", victim)
	var back client.Cluster
	if err := d.until(ctx, 60*time.Second, "extra copies trimmed", func(c client.Cluster) bool {
		for _, e := range c.Events {
			if e.Kind == "node" && e.Node == victim {
				d.at("%s %s", e.Node, e.Text)
			}
		}
		back = c
		alive := slices.ContainsFunc(c.Nodes, func(n client.NodeInfo) bool { return n.ID == victim && n.State == "alive" })
		return alive && c.Health.OverReplicated == 0 && c.Health.UnderReplicated == 0
	}); err != nil {
		return err
	}
	d.at("%s alive; %d extra copies trimmed; all chunks at 3 copies", victim, back.Health.RepairTrimmed-trimmed)
	d.say("\ndone in %.0fs", time.Since(d.start).Seconds())
	return nil
}

func nodeChunks(c client.Cluster, id string) int64 {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n.Chunks
		}
	}
	return 0
}

// until polls the cluster twice a second, passing only new events to ok.
func (d *demo) until(ctx context.Context, limit time.Duration, what string, ok func(client.Cluster) bool) error {
	deadline := time.Now().Add(limit)
	for {
		c, err := d.cluster(ctx)
		if err == nil {
			if c.EventSeq < d.seq { // metadata server restarted: timeline began again
				d.seq = 0
			} else {
				d.seq = c.EventSeq
			}
			if ok(c) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("%s: not within %v: %w", what, limit, err)
			}
			return fmt.Errorf("%s: not within %v", what, limit)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
