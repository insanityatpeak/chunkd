package chaos

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/core/scrub"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim/cluster"
)

// ChunkSize keeps chaos runs fast: 64 KiB chunks give files of several
// chunks without moving megabytes per operation.
const ChunkSize = 64 << 10

// Report is the outcome of one run.
type Report struct {
	Seed uint64 `json:"seed"`
	// Trace hashes the final cluster state; two runs of one seed must match.
	Trace string `json:"trace"`
	// Restored is how long after the last fault ended and the workload
	// finished every chunk was back to exactly Replicas copies; Bound is
	// the documented limit.
	Restored time.Duration  `json:"restored"`
	Bound    time.Duration  `json:"bound"`
	Ops      map[string]int `json:"ops"` // "put ok", "get failed", ...
	Repair   repair.Stats   `json:"repair"`
	Rotted   int            `json:"rotted"` // chunk copies corrupted by Corrupt faults
	GC       meta.GCStats   `json:"gc"`
	Err      error          `json:"-"`
}

// ScrubPass is the scrub interval chaos runs use.
const ScrubPass = 20 * time.Second

// Replay is the command that reruns one seed.
func Replay(seed uint64) string { return fmt.Sprintf("go run ./tools/task chaos --seed=%d", seed) }

// Config returns the cluster configuration chaos runs use.
func Config(nodes int) cluster.Config {
	cfg := cluster.DefaultConfig()
	cfg.Nodes = nodes
	cfg.Meta.ChunkSize = ChunkSize
	// Short passes, so rot nobody reads is found within a run.
	cfg.Scrub = scrub.Config{BytesPerSec: 8 << 20, Pass: ScrubPass}
	return cfg
}

// Run executes s in the sim and checks every invariant. Logs go to w.
func Run(s Scenario, w io.Writer) Report {
	r := Report{Seed: s.Seed, Ops: map[string]int{}}
	cfg := Config(s.Nodes)
	c := cluster.New(s.Seed, cfg, w)
	base := c.Net().Faults()
	wipes := map[iface.NodeID]bool{} // nodes this scenario wipes at some point
	for _, f := range s.Faults {
		if f.Kind == Wipe {
			wipes[f.Node] = true
		}
	}
	c.Tick(3 * time.Second)

	apply := func(f Fault) {
		switch f.Kind {
		case Kill:
			c.KillNode(f.Node)
		case Restart:
			c.RestartNode(f.Node)
		case Wipe:
			c.WipeNode(f.Node)
		case Freeze:
			c.Net().Freeze(f.Node)
		case Thaw:
			c.Net().Thaw(f.Node)
		case Slow:
			c.Net().SetSlow(f.Node, f.Delay)
		case Fast:
			c.Net().SetSlow(f.Node, 0)
		case Lossy:
			lossy := base
			lossy.DropRate, lossy.DupRate = f.Drop, f.Dup
			c.Net().SetFaults(lossy)
		case Clean:
			c.Net().SetFaults(base)
		case Corrupt:
			rotted := c.RotNode(f.Node, f.Count, f.Pick, func(ch iface.ChunkID) bool {
				intact := 0
				for _, n := range c.Nodes() {
					if n.ID() != f.Node && !wipes[n.ID()] && c.Intact(n.ID(), ch) {
						intact++
					}
				}
				return intact >= 2
			})
			r.Rotted += len(rotted)
		}
	}
	do := func(op Op) {
		var err error
		switch op.Kind {
		case Put:
			_, _, err = c.UploadRandom(op.Path, op.Size)
		case Get:
			_, _, err = c.Download(op.Path)
		case Delete:
			err = c.Delete(op.Path)
		}
		outcome := "ok"
		switch {
		case iface.CodeOf(err) == iface.CodeNotFound:
			outcome = "not found"
		case err != nil:
			outcome = "failed"
		}
		r.Ops[string(op.Kind)+" "+outcome]++
	}

	// Faults are clock events, so they fire on time even while a client
	// operation is blocked advancing the clock. Ops run in order; one that
	// is due while another runs starts right after it.
	start := c.Now()
	for _, f := range s.Faults {
		c.AfterFunc(f.At, func() { apply(f) })
	}
	at := func(d time.Duration) {
		if now := c.Now().Sub(start); now < d {
			c.Tick(d - now)
		}
	}
	for _, op := range s.Ops {
		at(op.At)
		do(op)
	}
	// Quiet: the last fault has ended and the workload is done. Writes that
	// dedup against existing chunks over-replicate until the next scan
	// trims them, so restoration is measured from here, not from the heal.
	quiet := c.Now().Sub(start)
	if len(s.Faults) > 0 {
		quiet = max(quiet, s.Faults[len(s.Faults)-1].At)
	}

	// Measure restoration from the end of the last fault. The bound
	// assumes, pessimistically, that every live byte is copied once.
	var live int64
	for _, e := range c.Meta().State().List("/") {
		live += e.Size
	}
	r.Bound = c.RepairBound(live)
	var errs []error
	at(quiet)
	for {
		r.Restored = c.Now().Sub(start) - quiet
		if c.UnderReplicated() == 0 && c.OverReplicated() == 0 {
			break
		}
		if r.Restored > r.Bound {
			errs = append(errs, fmt.Errorf("replication not restored %v after faults and workload ended (bound %v): %d under, %d over",
				r.Restored, r.Bound, c.UnderReplicated(), c.OverReplicated()))
			break
		}
		c.Tick(250 * time.Millisecond)
	}
	if r.Rotted > 0 && len(errs) == 0 {
		errs = append(errs, scrubbed(c)...)
	}

	if err := c.AssertInvariants(); err != nil {
		errs = append(errs, err)
	}
	// Let retention, leases and two sweeps past the grace run out: every
	// copy of deleted, overwritten or abandoned data must then be gone, and
	// every file must still read back.
	if len(errs) == 0 {
		c.Tick(c.GCSettle())
		if err := c.AssertCollected(); err != nil {
			errs = append(errs, err)
		}
		if err := c.AssertInvariants(); err != nil {
			errs = append(errs, fmt.Errorf("after GC: %w", err))
		}
	}
	h := c.Meta().Health()
	if h.Lost > 0 {
		errs = append(errs, fmt.Errorf("%d chunks have no live copy", h.Lost))
	}
	r.Repair = h.Repair
	r.GC = c.Meta().GC()
	lim := cfg.Meta.Repair
	if r.Repair.PeakInFlight > lim.MaxInFlight || r.Repair.PeakPerSource > lim.PerSource || r.Repair.PeakPerTarget > lim.PerTarget {
		errs = append(errs, fmt.Errorf("repair limits exceeded: peaks %d/%d/%d, limits %d/%d/%d",
			r.Repair.PeakInFlight, r.Repair.PeakPerSource, r.Repair.PeakPerTarget, lim.MaxInFlight, lim.PerSource, lim.PerTarget))
	}

	st, _ := json.Marshal(struct {
		State  cluster.State
		Health any
		Now    int64
	}{c.State(), h, int64(c.Now())})
	sum := sha256.Sum256(st)
	r.Trace = hex.EncodeToString(sum[:8])
	if err := errors.Join(errs...); err != nil {
		r.Err = fmt.Errorf("chaos seed %d: %w\n%s\nreplay: %s", s.Seed, err, s, Replay(s.Seed))
	}
	return r
}
