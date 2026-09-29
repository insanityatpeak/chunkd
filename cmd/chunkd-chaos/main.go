// Command chunkd-chaos runs randomized fault scenarios and checks the
// cluster's invariants after each one.
//
//	chunkd-chaos -seeds 500            seeds 1..500 in parallel (sim)
//	chunkd-chaos -seed 63 -v           one seed, with its schedule and logs
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
)

func main() {
	seed := flag.Uint64("seed", 0, "run this one seed (0: run -seeds seeds)")
	seeds := flag.Int("seeds", 500, "number of seeds, starting at -from")
	from := flag.Uint64("from", 1, "first seed")
	parallel := flag.Int("parallel", runtime.NumCPU(), "scenarios run at once")
	verbose := flag.Bool("v", false, "print each schedule and, for -seed, the cluster logs")
	flag.Parse()

	if *seed != 0 {
		s := chaos.Generate(*seed, chaos.DefaultShape())
		fmt.Print(s)
		var logs io.Writer = io.Discard
		if *verbose {
			logs = os.Stderr
		}
		r := chaos.Run(s, logs)
		if r.Err != nil {
			fmt.Fprintln(os.Stderr, r.Err)
			os.Exit(1)
		}
		fmt.Printf("ok: trace %s, RF restored %v after quiet (bound %v), %d repair copies, %d trims, ops %v\n",
			r.Trace, r.Restored, r.Bound, r.Repair.Completed, r.Repair.Trimmed, r.Ops)
		return
	}

	start := time.Now()
	jobs := make(chan uint64)
	var mu sync.Mutex
	var failed []chaos.Report
	var worst time.Duration
	var wg sync.WaitGroup
	for range max(*parallel, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sd := range jobs {
				s := chaos.Generate(sd, chaos.DefaultShape())
				r := chaos.Run(s, io.Discard)
				mu.Lock()
				if r.Err != nil {
					failed = append(failed, r)
				}
				worst = max(worst, r.Restored)
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

	fmt.Printf("chaos: %d seeds from %d in %v, %d failed, slowest RF restore %v\n",
		*seeds, *from, time.Since(start).Round(time.Millisecond), len(failed), worst)
	for _, r := range failed {
		fmt.Fprintln(os.Stderr, r.Err)
	}
	if len(failed) > 0 {
		os.Exit(1)
	}
}
