package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/insanityatpeak/chunkd/internal/bench"
	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
)

const (
	pwSize   = 32 << 20 // 8 chunks of 4 MiB
	pwRounds = 3        // passes over every window, each starting one window later
	pwPerRun = 4        // timed puts per window, redundancy and round, after one warm-up
)

var pwWindows = []int{1, 2, 4, 8}

// runPutWindow times 32 MiB puts through the compose gateway at each upload
// window (ADR-0034). The window is the gateway's: each one recreates the
// gateway container with CHUNKD_WINDOW set. One host drifts by more than the
// windows differ (two back-to-back single passes differed 2 to 3x), so the
// windows are interleaved over rounds in rotated order and pooled. EC runs
// only when six or more nodes are alive and active (compose profile ec).
// Every put carries fresh random bytes, so none dedups. The gateway is left
// at the default window.
func runPutWindow(data, gateway string) error {
	if err := os.MkdirAll(data, 0o755); err != nil {
		return err
	}
	api := httpclient.New(gateway)
	ctx := context.Background()
	reds := []client.Redundancy{client.Replicated}
	cl, err := api.Cluster(ctx, 0)
	if err != nil {
		return err
	}
	active := 0
	for _, n := range cl.Nodes {
		if n.State == "alive" && n.Admin == "active" {
			active++
		}
	}
	if active >= 6 {
		reds = append(reds, client.EC42)
	} else {
		fmt.Printf("%d active nodes: EC skipped (needs 6; compose --profile full --profile ec)\n", active)
	}
	defer setWindow(0)

	ns := fmt.Sprintf("/bench-pw-%d/", time.Now().UnixMilli())
	body := make([]byte, pwSize)
	type cell struct {
		red client.Redundancy
		w   int
	}
	samples := map[cell][]time.Duration{}
	n := 0
	for r := range pwRounds {
		for k := range pwWindows {
			w := pwWindows[(r+k)%len(pwWindows)]
			if err := setWindow(w); err != nil {
				return err
			}
			for _, red := range reds {
				for i := range pwPerRun + 1 {
					for j := range body {
						body[j] = byte(rand.Uint32())
					}
					n++
					p := fmt.Sprintf("%s%04d", ns, n)
					start := time.Now()
					if _, err := api.Put(ctx, p, bytes.NewReader(body), pwSize, client.PutOptions{Overwrite: true, Redundancy: red}); err != nil {
						return fmt.Errorf("put %s: %w", p, err)
					}
					if i > 0 {
						samples[cell{red, w}] = append(samples[cell{red, w}], time.Since(start))
					}
				}
			}
			fmt.Printf("round %d window %d done\n", r+1, w)
		}
	}

	tab := &bench.Table{Header: []string{"redundancy", "window", "samples", "p50_ms", "p95_ms", "p99_ms", "mib_s_p50"}}
	for _, red := range reds {
		for _, w := range pwWindows {
			ds := samples[cell{red, w}]
			slices.Sort(ds)
			pct := func(p float64) float64 {
				return float64(ds[min(len(ds)-1, int(p*float64(len(ds))))].Microseconds()) / 1000
			}
			fmt.Printf("window %d %s: p50 %.0f ms\n", w, label(red), pct(0.50))
			tab.Add(label(red), w, len(ds), pct(0.50), pct(0.95), pct(0.99), float64(pwSize>>20)/(pct(0.50)/1000))
		}
	}
	if err := os.WriteFile(filepath.Join(data, "put-window-compose-hardware.txt"), []byte(bench.Hardware()+"\n"), 0o644); err != nil {
		return err
	}
	return tab.WriteCSV(filepath.Join(data, "put-window-compose.csv"))
}

// label names a redundancy as the sim's put-window.csv does.
func label(r client.Redundancy) string {
	if r == client.EC42 {
		return "ec:4+2"
	}
	return "replicate:3"
}

// setWindow recreates the gateway container with upload window w (0: the
// client default) and waits for it to report healthy.
func setWindow(w int) error {
	cmd := exec.Command("docker", "compose", "up", "-d", "--no-deps", "--wait", "gateway")
	cmd.Env = append(os.Environ(), "CHUNKD_WINDOW="+strconv.Itoa(w))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("gateway with window %d: %w: %s", w, err, bytes.TrimSpace(out))
	}
	return nil
}
