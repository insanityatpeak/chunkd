// Command chunkd-chaos runs randomized fault scenarios and checks the
// cluster's invariants after each one.
//
//	chunkd-chaos -seeds 500            seeds 1..500 in parallel (sim)
//	chunkd-chaos -seed 63 -v           one seed, with its schedule and logs
//	chunkd-chaos -seeds 500 -metas 3   over a 3-peer metadata group, with leader faults
//	chunkd-chaos -mode real -short     the real-mode suite against docker compose
//
// Every failure prints the command that replays it.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/insanityatpeak/chunkd/internal/chaos"
	"github.com/insanityatpeak/chunkd/internal/chaos/compose"
)

// runReal runs the short suite against the compose cluster, one scenario at a
// time, and returns the exit code.
func runReal(gateway, project string, bound time.Duration) int {
	t := compose.New(gateway, project)
	logf := func(format string, args ...any) {
		fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
	}
	code := 0
	for _, s := range chaos.ShortSuite() {
		logf("start %s", s.Name)
		r := chaos.RunTarget(s, t, bound, logf)
		for _, f := range r.Skipped {
			logf("%s: skipped %v (sim only)", s.Name, f)
		}
		if r.Err != nil {
			fmt.Fprintln(os.Stderr, "FAIL", r.Err)
			code = 1
			continue
		}
		logf("ok %s: longest under-replication %v (bound %v), %d repair copies, ops %v",
			s.Name, r.LongestUnder.Round(time.Second), r.Bound, r.RepairCopies, r.Ops)
	}
	return code
}

func main() {
	seed := flag.Uint64("seed", 0, "run this one seed (0: run -seeds seeds)")
	seeds := flag.Int("seeds", 500, "number of seeds, starting at -from")
	from := flag.Uint64("from", 1, "first seed")
	parallel := flag.Int("parallel", runtime.NumCPU(), "scenarios run at once")
	verbose := flag.Bool("v", false, "print each schedule and, for -seed, the cluster logs")
	mode := flag.String("mode", "sim", "sim, or real (the compose cluster must be up)")
	short := flag.Bool("short", false, "real mode: the short suite CI runs on every push")
	gateway := flag.String("gateway", "http://localhost:8080", "real mode: gateway URL")
	project := flag.String("project", "chunkd", "real mode: compose project name")
	bound := flag.Duration("bound", 90*time.Second, "real mode: longest allowed under-replication, and settle time after quiet")
	metas := flag.Int("metas", 1, "sim mode: metadata peers; 3 adds leader kills, freezes, partitions and repeated elections")
	artifacts := flag.String("artifacts", "", "sim mode: directory for the visualization of a history that is not linearizable")
	flag.Parse()

	if *mode == "real" {
		if !*short {
			fmt.Fprintln(os.Stderr, "chunkd-chaos: real mode runs the -short suite only")
			os.Exit(2)
		}
		os.Exit(runReal(*gateway, *project, *bound))
	}

	shape := chaos.DefaultShape()
	if *metas > 1 {
		shape = chaos.MetaShape()
		shape.Metas = *metas
	}

	if *seed != 0 {
		s := chaos.Generate(*seed, shape)
		fmt.Print(s)
		var logs io.Writer = io.Discard
		if *verbose {
			logs = os.Stderr
		}
		r := chaos.RunOpts(s, logs, chaos.Options{Artifacts: *artifacts})
		if r.Err != nil {
			fmt.Fprintln(os.Stderr, r.Err)
			os.Exit(1)
		}
		fmt.Printf("ok: trace %s, RF restored %v after quiet (bound %v), %d repair copies, %d trims, %d leader faults, %d minority-side ops, ops %v\n",
			r.Trace, r.Restored, r.Bound, r.Repair.Completed, r.Repair.Trimmed, r.LeaderFaults, r.MinorityOps, r.Ops)
		return
	}

	start := time.Now()
	jobs := make(chan uint64)
	var mu sync.Mutex
	var failed []chaos.Report
	var worst time.Duration
	rotted, leaderFaults, minorityOps := 0, 0, 0
	var wg sync.WaitGroup
	for range max(*parallel, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sd := range jobs {
				s := chaos.Generate(sd, shape)
				r := chaos.RunOpts(s, io.Discard, chaos.Options{Artifacts: *artifacts})
				mu.Lock()
				if r.Err != nil {
					failed = append(failed, r)
				}
				worst = max(worst, r.Restored)
				rotted += r.Rotted
				leaderFaults += r.LeaderFaults
				minorityOps += r.MinorityOps
				if *verbose {
					fmt.Printf("seed %d: trace %s restored %v\n", sd, r.Trace, r.Restored)
				}
				mu.Unlock()
			}
		}()
	}
	for sd := *from; sd < *from+uint64(*seeds); sd++ {
		jobs <- sd
	}
	close(jobs)
	wg.Wait()

	summary := fmt.Sprintf("chaos: %d seeds from %d in %v, %d failed, slowest RF restore %v, %d copies rotted",
		*seeds, *from, time.Since(start).Round(time.Millisecond), len(failed), worst, rotted)
	if shape.Metas > 1 {
		summary += fmt.Sprintf(", %d leader faults, %d minority-side ops", leaderFaults, minorityOps)
	}
	fmt.Println(summary)
	for _, r := range failed {
		fmt.Fprintln(os.Stderr, r.Err)
	}
	if len(failed) > 0 {
		os.Exit(1)
	}
}
