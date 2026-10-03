package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/bench"
	"github.com/insanityatpeak/chunkd/internal/client"
)

// The TestBench* tests produce the CSVs behind docs/benchmarks. They run
// only under `go run ./tools/task bench`, which sets bench.OutEnv. Time is
// simulated, so the same seed gives the same numbers on any machine.

func benchOut(t *testing.T, name string) string {
	t.Helper()
	dir := bench.OutDir()
	if dir == "" {
		t.Skip(bench.OutEnv + " not set")
	}
	return filepath.Join(dir, name)
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, len(sorted)*p/100)]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// uploadFiles stores n files of size bytes and waits for full replication.
func uploadFiles(t *testing.T, c *Cluster, n int, size int64) []string {
	t.Helper()
	c.Tick(3 * time.Second)
	var paths []string
	for i := range n {
		p := fmt.Sprintf("/b/%03d", i)
		if _, _, err := c.UploadRandom(p, size); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	if _, ok := c.Settle(time.Minute); !ok {
		t.Fatalf("%d chunks short before any fault", c.UnderReplicated())
	}
	return paths
}

// TestBenchRepairTime kills node-3 and records the time to RF 3 again, by
// distinct volume and repair rate limit. The detector (10 s) and the repair
// delay (20 s) set a 30 s floor; past_floor_s is what the copies add.
func TestBenchRepairTime(t *testing.T) {
	out := benchOut(t, "repair-time.csv")
	tab := &bench.Table{Header: []string{"volume_mib", "limit_mib_s", "lost_mib", "copies", "whole_after_kill_s", "past_floor_s", "mib_s_past_floor"}}
	for _, vol := range []int{40, 120, 240} {
		for _, limit := range []int64{5, 20, 80} {
			cfg := DefaultConfig()
			cfg.Meta.Repair.BytesPerSec = limit << 20
			c := New(11, cfg, io.Discard)
			uploadFiles(t, c, vol/4, 4<<20)
			lost := c.BytesOn("node-3")
			before := c.Meta().Repair().Stats()
			start := c.Now()
			c.KillNode("node-3")
			floor := cfg.Meta.Detector.DeadAfter + cfg.Meta.Repair.Delay
			c.Tick(floor - time.Second)
			if _, ok := c.Settle(2 * c.RepairBound(lost)); !ok {
				t.Fatalf("vol %d limit %d: %d short", vol, limit, c.UnderReplicated())
			}
			whole := c.Now().Sub(start)
			past := max(whole-floor, 0)
			st := c.Meta().Repair().Stats()
			rate := 0.0 // below one second the 250 ms polling dominates
			if past >= time.Second {
				rate = mib(lost) / past.Seconds()
			}
			tab.Add(vol, int(limit), mib(lost), int(st.Completed-before.Completed), whole.Seconds(), past.Seconds(), rate)
			if err := c.AssertInvariants(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tab.WriteCSV(out); err != nil {
		t.Fatal(err)
	}
}

// TestBenchRepairForeground reads 4 MiB files while repair copies, with the
// rate limit at 10 and 40 MiB/s (the default) and removed along with the
// slot limits. Baseline is the same reads with no fault. The sim gives every
// message its own transfer time and shares no capacity between messages, so
// repair cannot slow a read here: the table shows the cap holding (peak
// repair rate) and what it costs in repair time, not the effect on readers.
func TestBenchRepairForeground(t *testing.T) {
	out := benchOut(t, "repair-foreground.csv")
	tab := &bench.Table{Header: []string{"mode", "samples", "p50_ms", "p95_ms", "p99_ms", "max_ms", "copy_phase_s", "peak_repair_mib_s"}}
	modes := []struct {
		name  string
		limit int64
		slots int
	}{
		{"no-fault", 0, 0},
		{"limit-10-mib-s", 10 << 20, 8},
		{"limit-40-mib-s", 40 << 20, 8},
		{"unlimited", 1 << 40, 64},
	}
	seeds := []uint64{21, 22, 23}
	for _, m := range modes {
		var lat []time.Duration
		var phase time.Duration
		peak := 0.0
		for _, seed := range seeds {
			cfg := DefaultConfig()
			cfg.Faults.DropRate = 0 // a lost message costs a 10 s call timeout, which would swamp the p99
			if m.limit > 0 {
				cfg.Meta.Repair.BytesPerSec = m.limit
				cfg.Meta.Repair.MaxInFlight = m.slots
				cfg.Meta.Repair.PerSource = max(cfg.Meta.Repair.PerSource, m.slots/4)
				cfg.Meta.Repair.PerTarget = max(cfg.Meta.Repair.PerTarget, m.slots/4)
			}
			c := New(seed, cfg, io.Discard)
			paths := uploadFiles(t, c, 60, 4<<20)
			cl := c.Client()
			read := func(i int) {
				start := c.Now()
				if _, err := cl.Get(context.Background(), paths[i%len(paths)], io.Discard); err != nil {
					t.Fatalf("%s seed %d: %v", m.name, seed, err)
				}
				lat = append(lat, c.Now().Sub(start))
			}
			if m.limit == 0 {
				for i := range 300 {
					read(i)
				}
				continue
			}
			c.KillNode("node-3")
			c.Tick(cfg.Meta.Detector.DeadAfter + cfg.Meta.Repair.Delay)
			start := c.Now()
			mark, markBytes := start, c.Meta().Repair().Stats().Bytes
			for i := 0; c.UnderReplicated() > 0 && c.Now().Sub(start) < 5*time.Minute; i++ {
				read(i)
				// Repair traffic over windows of at least a second.
				if w := c.Now().Sub(mark); w >= time.Second {
					b := c.Meta().Repair().Stats().Bytes
					peak = max(peak, float64(b-markBytes)/(1<<20)/w.Seconds())
					mark, markBytes = c.Now(), b
				}
			}
			// The last, shorter window: an unlimited repair ends inside the first.
			if w := c.Now().Sub(mark); w >= 100*time.Millisecond {
				peak = max(peak, float64(c.Meta().Repair().Stats().Bytes-markBytes)/(1<<20)/w.Seconds())
			}
			phase += c.Now().Sub(start)
		}
		slices.Sort(lat)
		tab.Add(m.name, len(lat), ms(pct(lat, 50)), ms(pct(lat, 95)), ms(pct(lat, 99)), ms(lat[len(lat)-1]), phase.Seconds()/float64(len(seeds)), peak)
	}
	if err := tab.WriteCSV(out); err != nil {
		t.Fatal(err)
	}
}

// TestBenchLatency records put, get and stat latency on a LAN with 1% loss,
// healthy, with one gray node (2 s added to every message), and with 1% message
// loss (a lost call waits out the 10 s call timeout), hedged reads on and off.
// Every operation uses a new client, as a CLI invocation does.
func TestBenchLatency(t *testing.T) {
	out := benchOut(t, "latency.csv")
	tab := &bench.Table{Header: []string{"fault", "hedging", "op", "samples", "p50_ms", "p95_ms", "p99_ms"}}
	for _, fault := range []string{"none", "gray-node", "loss-1pct"} {
		for _, noHedge := range []bool{false, true} {
			var put, get, stat []time.Duration
			for _, seed := range []uint64{31, 32, 33} {
				cfg := DefaultConfig()
				if fault != "loss-1pct" {
					cfg.Faults.DropRate = 0
				}
				c := New(seed, cfg, io.Discard)
				c.Tick(3 * time.Second)
				caller := c.NewCaller("reader")
				// A new client per operation: a long-lived one learns which node is
				// slow from its first reads and avoids it, which hides the hedge.
				fresh := func() *client.Direct {
					return client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep, NoHedge: noHedge})
				}
				ctx := context.Background()
				var paths []string
				for i := range 40 {
					p := fmt.Sprintf("/l/%02d", i)
					if _, _, err := c.UploadRandom(p, 1<<20); err != nil {
						t.Fatal(err)
					}
					paths = append(paths, p)
				}
				if fault == "gray-node" {
					c.Net().SetSlow("node-1", 2*time.Second)
				}
				for round := range 3 {
					for _, p := range paths {
						t0 := c.Now()
						if _, err := fresh().Stat(ctx, p); err != nil {
							t.Fatal(err)
						}
						stat = append(stat, c.Now().Sub(t0))
						t0 = c.Now()
						if _, err := fresh().Get(ctx, p, io.Discard); err != nil {
							t.Fatal(err)
						}
						get = append(get, c.Now().Sub(t0))
					}
					for i := range 10 {
						p := fmt.Sprintf("/w/%d-%02d", round, i)
						data := c.RandomData(p, 1<<20)
						t0 := c.Now()
						if _, err := fresh().Put(ctx, p, bytes.NewReader(data), int64(len(data)), client.PutOptions{}); err != nil {
							t.Fatalf("%s seed %d: put: %v", fault, seed, err)
						}
						put = append(put, c.Now().Sub(t0))
					}
				}
			}
			h := "on"
			if noHedge {
				h = "off"
			}
			for _, op := range []struct {
				name string
				lat  []time.Duration
			}{{"put", put}, {"get", get}, {"stat", stat}} {
				slices.Sort(op.lat)
				tab.Add(fault, h, op.name, len(op.lat), ms(pct(op.lat, 50)), ms(pct(op.lat, 95)), ms(pct(op.lat, 99)))
			}
		}
	}
	if err := tab.WriteCSV(out); err != nil {
		t.Fatal(err)
	}
}

// TestBenchResume cuts a 32 MiB upload off at 25, 50, 75 and 90% (rounded
// down to a chunk) and records what the client sends to finish it: by
// resuming at the stored offset (ADR-0024), and by starting the same Put
// again with no resume.
func TestBenchResume(t *testing.T) {
	out := benchOut(t, "resume.csv")
	tab := &bench.Table{Header: []string{"redundancy", "cut_at_pct", "file_mib", "sent_before_cut_mib", "resume_finish_mib", "restart_finish_mib", "resume_total_mib", "restart_total_mib", "resume_body_mib", "restart_body_mib"}}
	const size = 32 << 20
	ctx := context.Background()
	for _, policy := range []client.Redundancy{client.Replicated, client.EC42} {
		for _, at := range []int{25, 50, 75, 90} {
			cfg := DefaultConfig()
			cfg.Nodes = 6
			cfg.Faults.DropRate = 0
			cut := int64(size) * int64(at) / 100 / (4 << 20) * (4 << 20)
			var before, resume, restart, resumeBody int64
			for _, restartRun := range []bool{false, true} {
				c := New(41, cfg, io.Discard)
				c.Tick(3 * time.Second)
				data := c.RandomData("/f", size)
				cl := c.Client()
				info, err := cl.BeginResumable(ctx, "/f", size, sha256.Sum256(data), client.PutOptions{Redundancy: policy})
				if err != nil {
					t.Fatal(err)
				}
				n0 := c.Net().Stats().Bytes
				if _, err = cl.Append(ctx, info.ID, 0, bytes.NewReader(data[:cut]), cut); err != nil {
					t.Fatal(err)
				}
				c.Tick(time.Second)
				n1 := c.Net().Stats().Bytes
				if restartRun {
					// A client without resume knows nothing of the upload.
					if _, err = cl.Put(ctx, "/f", bytes.NewReader(data), size, client.PutOptions{Redundancy: policy}); err != nil {
						t.Fatal(err)
					}
					restart = int64(c.Net().Stats().Bytes - n1)
					continue
				}
				st, err := cl.UploadStatus(ctx, info.ID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = cl.Append(ctx, st.ID, st.Offset, bytes.NewReader(data[st.Offset:]), size-st.Offset); err != nil {
					t.Fatal(err)
				}
				before, resume, resumeBody = int64(n1-n0), int64(c.Net().Stats().Bytes-n1), size-st.Offset
			}
			name := "replicate:3"
			if policy == client.EC42 {
				name = "ec:4+2"
			}
			tab.Add(name, at, mib(size), mib(before), mib(resume), mib(restart), mib(before+resume), mib(before+restart), mib(resumeBody), mib(size))
		}
	}
	if err := tab.WriteCSV(out); err != nil {
		t.Fatal(err)
	}
}
