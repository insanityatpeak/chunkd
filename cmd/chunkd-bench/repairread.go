package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/insanityatpeak/chunkd/internal/bench"
	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
)

const (
	rrFiles    = 160     // files preloaded
	rrSize     = 4 << 20 // each; one chunk
	rrReaders  = 4       // concurrent readers
	rrBaseline = 30 * time.Second
	rrSteady   = 30 * time.Second
	rrLimit    = 3 * time.Minute // longest wait for a health condition
	// rrFloor is the detector (10 s) plus the repair delay (20 s): no copy
	// starts before it, so the time up to it is "detecting".
	rrFloor = 30 * time.Second
)

// sample is one read: when it finished, how long it took, whether it failed.
type sample struct {
	end time.Time
	d   time.Duration
	err bool
}

// runRepairRead measures what a repair does to foreground reads on the
// compose cluster (real containers, real disks, the Docker network): readers
// loop over preloaded files while node-3 is killed, repaired around and
// restarted afterwards. The cluster must be up (docker compose up -d --wait) and quiet.
// The phase "repaired" is the control: the same four nodes with no repair
// traffic, so the difference to "copying" is repair's cost to a reader.
func runRepairRead(data, gateway string) error {
	if err := os.MkdirAll(data, 0o755); err != nil {
		return err
	}
	api := httpclient.New(gateway)
	ctx := context.Background()
	ns := fmt.Sprintf("/bench-rr-%d/", time.Now().UnixMilli())

	fmt.Printf("preloading %d files of %d MiB\n", rrFiles, rrSize>>20)
	paths := make([]string, rrFiles)
	for i := range paths {
		paths[i] = fmt.Sprintf("%sf%02d", ns, i)
		body := make([]byte, rrSize)
		for j := range body {
			body[j] = byte(rand.Uint32())
		}
		if _, err := api.Put(ctx, paths[i], bytes.NewReader(body), int64(len(body)), client.PutOptions{Overwrite: true}); err != nil {
			return fmt.Errorf("preload %s: %w", paths[i], err)
		}
	}
	if err := waitHealth(api, func(h client.Health) bool { return h.UnderReplicated == 0 && h.OverReplicated == 0 }, rrLimit); err != nil {
		return fmt.Errorf("before the run: %w", err)
	}

	var mu sync.Mutex
	var samples []sample
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := range rrReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				p := paths[rng.IntN(len(paths))]
				start := time.Now()
				_, err := api.Get(ctx, p, io.Discard)
				s := sample{end: time.Now(), d: time.Since(start), err: err != nil}
				mu.Lock()
				samples = append(samples, s)
				mu.Unlock()
			}
		}()
	}

	type phase struct {
		name       string
		start, end time.Time
	}
	var phases []phase
	mark := func(name string) {
		now := time.Now()
		if n := len(phases); n > 0 {
			phases[n-1].end = now
		}
		phases = append(phases, phase{name: name, start: now})
		fmt.Printf("%s  %s\n", now.Format("15:04:05"), name)
	}
	dockerOK := func(args ...string) error {
		out, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("docker %v: %w: %s", args, err, bytes.TrimSpace(out))
		}
		return nil
	}
	fail := func(err error) error {
		close(stop)
		wg.Wait()
		_ = dockerOK("compose", "start", "node-3")
		return err
	}

	mark("baseline")
	time.Sleep(rrBaseline)

	mark("detecting")
	if err := dockerOK("compose", "kill", "node-3"); err != nil {
		return fail(err)
	}
	// Under-replicated from the kill; repair has finished once it clears.
	if err := waitHealth(api, func(h client.Health) bool { return h.UnderReplicated > 0 }, rrLimit); err != nil {
		return fail(fmt.Errorf("after the kill: %w", err))
	}
	time.Sleep(max(0, rrFloor-time.Since(phases[len(phases)-1].start)))
	mark("copying")
	if err := waitHealth(api, func(h client.Health) bool { return h.UnderReplicated == 0 }, rrLimit); err != nil {
		return fail(fmt.Errorf("repair: %w", err))
	}

	mark("repaired")
	time.Sleep(rrSteady)

	phases[len(phases)-1].end = time.Now()
	close(stop)
	wg.Wait()
	// Bring the node back so the cluster is whole; not measured.
	if err := dockerOK("compose", "start", "node-3"); err != nil {
		return err
	}
	if err := waitHealth(api, func(h client.Health) bool { return h.UnderReplicated == 0 && h.OverReplicated == 0 }, rrLimit); err != nil {
		return fmt.Errorf("after the return: %w", err)
	}

	tab := &bench.Table{Header: []string{"phase", "seconds", "reads", "failed", "p50_ms", "p99_ms", "mib_s"}}
	for _, ph := range phases {
		var ds []time.Duration
		failed := 0
		for _, s := range samples {
			if s.end.Before(ph.start) || !s.end.Before(ph.end) {
				continue
			}
			if s.err {
				failed++
				continue
			}
			ds = append(ds, s.d)
		}
		slices.Sort(ds)
		secs := ph.end.Sub(ph.start).Seconds()
		pct := func(p float64) float64 {
			if len(ds) == 0 {
				return 0
			}
			return float64(ds[min(len(ds)-1, int(p*float64(len(ds))))].Microseconds()) / 1000
		}
		tab.Add(ph.name, secs, len(ds), failed, pct(0.50), pct(0.99), float64(len(ds))*float64(rrSize>>20)/secs)
	}
	if err := os.WriteFile(filepath.Join(data, "repair-read-hardware.txt"), []byte(bench.Hardware()+"\n"), 0o644); err != nil {
		return err
	}
	return tab.WriteCSV(filepath.Join(data, "repair-read.csv"))
}

// waitHealth polls the cluster view once a second until ok holds.
func waitHealth(api *httpclient.Client, ok func(client.Health) bool, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	var last client.Health
	for {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cl, err := api.Cluster(c, 0)
		cancel()
		if err == nil {
			last = cl.Health
			if ok(last) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met after %v (under %d, over %d, last error %v)", limit, last.UnderReplicated, last.OverReplicated, err)
		}
		time.Sleep(time.Second)
	}
}
