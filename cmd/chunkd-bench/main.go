// Command chunkd-bench runs the benchmarks and writes docs/benchmarks:
// CSVs under data/, SVG charts under charts/, and results.md.
//
//	go run ./tools/task bench                  every stage
//	go run ./tools/task bench -stages=real     one stage: sim, meta, real, report
//
// sim runs the TestBench* tests in internal/sim/cluster (simulated time,
// exact on any machine). meta runs the quota Go benchmark. real starts a
// cluster in this process, over loopback gRPC and real disks, and measures
// wall-clock time; those numbers move with the machine and its load.
package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/insanityatpeak/chunkd/internal/bench"
	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/real/local"
)

func main() {
	out := flag.String("out", "docs/benchmarks", "output directory")
	stages := flag.String("stages", "sim,meta,real,report", "comma-separated stages")
	scratch := flag.String("dir", "", "directory for the real cluster's disks (default: the system temp dir)")
	flag.Parse()
	data := filepath.Join(*out, "data")
	for _, st := range strings.Split(*stages, ",") {
		var err error
		switch st {
		case "sim":
			err = runSim(data)
		case "meta":
			err = runMeta(data)
		case "real":
			err = runReal(data, *scratch)
		case "report":
			err = report(*out)
		default:
			err = fmt.Errorf("unknown stage %q", st)
		}
		if err != nil {
			log.Fatalf("%s: %v", st, err)
		}
	}
}

func runSim(data string) error {
	abs, err := filepath.Abs(data)
	if err != nil {
		return err
	}
	cmd := exec.Command("go", "test", "-count=1", "-timeout=30m", "-run", "^TestBench", "./internal/sim/cluster")
	cmd.Env = append(os.Environ(), bench.OutEnv+"="+abs)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

var benchLine = regexp.MustCompile(`^BenchmarkBegin/(\S+)/files=(\d+)-\d+\s+\d+\s+([\d.]+) ns/op`)

func runMeta(data string) error {
	cmd := exec.Command("go", "test", "-run", "^$", "-bench", "BenchmarkBegin", "-benchtime=200x", "./internal/core/meta")
	cmd.Stderr = os.Stderr
	b, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("%v\n%s", err, b)
	}
	tab := &bench.Table{Header: []string{"mode", "files", "us_per_begin"}}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if m := benchLine.FindStringSubmatch(sc.Text()); m != nil {
			files, _ := strconv.Atoi(m[2])
			ns, _ := strconv.ParseFloat(m[3], 64)
			tab.Add(m[1], files, ns/1000)
		}
	}
	if len(tab.Rows) == 0 {
		return fmt.Errorf("no benchmark lines in:\n%s", b)
	}
	return tab.WriteCSV(filepath.Join(data, "quota.csv"))
}

// stream is size bytes that differ from every other stream with another seed.
func stream(seed uint64, size int64) io.Reader {
	var s [32]byte
	for i := range 8 {
		s[i] = byte(seed >> (8 * i))
	}
	return io.LimitReader(rand.NewChaCha8(s), size)
}

func runReal(data, scratch string) error {
	if err := os.MkdirAll(data, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(data, "hardware.txt"), []byte(bench.Hardware()+"\n"), 0o644); err != nil {
		return err
	}
	ctx := context.Background()
	start := func() (*local.Cluster, string, error) {
		dir, err := os.MkdirTemp(scratch, "chunkd-bench-")
		if err != nil {
			return nil, "", err
		}
		c, err := local.Start(dir, 5, meta.DefaultConfig(""))
		return c, dir, err
	}

	thr := &bench.Table{Header: []string{"size_mib", "run", "put_s", "get_s", "put_mib_s", "get_mib_s"}}
	for _, sizeMiB := range []int64{1, 16, 128, 1024} {
		c, dir, err := start()
		if err != nil {
			return err
		}
		runs := 3
		if sizeMiB >= 1024 {
			runs = 1
		}
		size := sizeMiB << 20
		for r := range runs {
			path := fmt.Sprintf("/t/%d-%d", sizeMiB, r)
			t0 := time.Now()
			if _, err := c.Client.Put(ctx, path, stream(uint64(sizeMiB)<<8|uint64(r), size), size, client.PutOptions{}); err != nil {
				return fmt.Errorf("put %d MiB: %w", sizeMiB, err)
			}
			put := time.Since(t0)
			t0 = time.Now()
			if _, err := c.Client.Get(ctx, path, io.Discard); err != nil {
				return fmt.Errorf("get %d MiB: %w", sizeMiB, err)
			}
			get := time.Since(t0)
			thr.Add(int(sizeMiB), r+1, put.Seconds(), get.Seconds(), float64(sizeMiB)/put.Seconds(), float64(sizeMiB)/get.Seconds())
			fmt.Printf("real %5d MiB run %d: put %.1f MiB/s, get %.1f MiB/s\n", sizeMiB, r+1, float64(sizeMiB)/put.Seconds(), float64(sizeMiB)/get.Seconds())
		}
		c.Close()
		_ = os.RemoveAll(dir)
	}
	if err := thr.WriteCSV(filepath.Join(data, "real-throughput.csv")); err != nil {
		return err
	}

	// Latency: 1000 sequential operations on 256 KiB files, one client.
	c, dir, err := start()
	if err != nil {
		return err
	}
	defer func() { c.Close(); _ = os.RemoveAll(dir) }()
	const n, fsize = 1000, 256 << 10
	var put, get, stat []time.Duration
	for i := range n {
		t0 := time.Now()
		if _, err := c.Client.Put(ctx, fmt.Sprintf("/l/%04d", i), stream(uint64(i)+1<<32, fsize), fsize, client.PutOptions{}); err != nil {
			return err
		}
		put = append(put, time.Since(t0))
	}
	for i := range n {
		t0 := time.Now()
		if _, err := c.Client.Stat(ctx, fmt.Sprintf("/l/%04d", i)); err != nil {
			return err
		}
		stat = append(stat, time.Since(t0))
	}
	for i := range n {
		t0 := time.Now()
		if _, err := c.Client.Get(ctx, fmt.Sprintf("/l/%04d", i), io.Discard); err != nil {
			return err
		}
		get = append(get, time.Since(t0))
	}
	lat := &bench.Table{Header: []string{"op", "samples", "p50_ms", "p95_ms", "p99_ms", "max_ms"}}
	for _, op := range []struct {
		name string
		d    []time.Duration
	}{{"put", put}, {"get", get}, {"stat", stat}} {
		slices.Sort(op.d)
		at := func(p int) float64 {
			return float64(op.d[min(len(op.d)-1, len(op.d)*p/100)]) / float64(time.Millisecond)
		}
		lat.Add(op.name, len(op.d), at(50), at(95), at(99), float64(op.d[len(op.d)-1])/float64(time.Millisecond))
	}
	return lat.WriteCSV(filepath.Join(data, "real-latency.csv"))
}
