package chaos

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/core/scrub"
	"github.com/insanityatpeak/chunkd/internal/history/check"
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
	// LeaderFaults counts leader kills, freezes and cuts that found a leader
	// to hit; MinorityOps, ops sent from the cut-off leader's side.
	LeaderFaults int          `json:"leaderFaults"`
	MinorityOps  int          `json:"minorityOps"`
	GC           meta.GCStats `json:"gc"`
	// History is how many recorded client operations the linearizability
	// check covered.
	History int   `json:"history"`
	Err     error `json:"-"`
}

// ScrubPass is the scrub interval chaos runs use.
const ScrubPass = 20 * time.Second

// Replay is the command that reruns one generated scenario.
func Replay(s Scenario) string {
	cmd := fmt.Sprintf("go run ./tools/task chaos --seed=%d", s.Seed)
	if s.Metas > 1 {
		cmd += fmt.Sprintf(" --metas=%d", s.Metas)
	}
	return cmd
}

// Config returns the cluster configuration chaos runs use.
func Config(nodes int) cluster.Config {
	cfg := cluster.DefaultConfig()
	cfg.Nodes = nodes
	cfg.Meta.ChunkSize = ChunkSize
	// Short passes, so rot nobody reads is found within a run.
	cfg.Scrub = scrub.Config{BytesPerSec: 8 << 20, Pass: ScrubPass}
	return cfg
}

// Options tune a run.
type Options struct {
	// Artifacts is a directory that receives porcupine's HTML visualization
	// of a history that fails the linearizability check, as seed-N.html.
	Artifacts string
}

// Run executes s in the sim and checks every invariant. Logs go to w.
func Run(s Scenario, w io.Writer) Report { return RunOpts(s, w, Options{}) }

// RunOpts is Run with options.
func RunOpts(s Scenario, w io.Writer, opts Options) Report {
	r := Report{Seed: s.Seed, Ops: map[string]int{}}
	cfg := Config(s.Nodes)
	cfg.Metas = s.Metas
	c := cluster.New(s.Seed, cfg, w)
	rec := c.RecordHistory()
	base := c.Net().Faults()
	wipes := map[iface.NodeID]bool{} // nodes this scenario wipes at some point
	for _, f := range s.Faults {
		if f.Kind == Wipe {
			wipes[f.Node] = true
		}
	}
	if s.Metas >= 3 {
		c.Tick(6 * time.Second) // the group needs its first election
	} else {
		c.Tick(3 * time.Second)
	}

	var errs []error
	// held is the peer the running leader fault hit; cut is set while it is
	// partitioned off. Leader faults never overlap, so one slot is enough.
	var held, cut iface.NodeID
	others := func(id iface.NodeID) []iface.NodeID {
		var rest []iface.NodeID
		for _, p := range c.MetaIDs() {
			if p != id {
				rest = append(rest, p)
			}
		}
		return rest
	}
	// hit picks the current leader as the victim; "" during an election.
	hit := func() iface.NodeID {
		if held == "" {
			if held = c.MetaLeader(); held != "" {
				r.LeaderFaults++
			}
			return held
		}
		return ""
	}
	apply := func(f Fault) {
		switch f.Kind {
		case KillLeader:
			if id := hit(); id != "" {
				c.KillMeta(id)
			}
		case ReviveLeader:
			if held != "" {
				if err := c.ReviveMeta(held); err != nil {
					errs = append(errs, fmt.Errorf("revive %s: %w", held, err))
				}
				held = ""
			}
		case FreezeLeader:
			if id := hit(); id != "" {
				c.Net().Freeze(id)
			}
		case ThawLeader:
			if held != "" {
				c.Net().Thaw(held)
				held = ""
			}
		case CutLeader:
			if id := hit(); id != "" {
				c.Net().Partition([]iface.NodeID{id}, others(id))
				cut = id
			}
		case HealLeader:
			if cut != "" {
				for _, p := range others(cut) {
					c.Net().Unblock(cut, p)
					c.Net().Unblock(p, cut)
				}
				held, cut = "", ""
			}
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
		sess := c.Session()
		if op.Minority && cut != "" {
			sess = c.Pinned(cut)
			r.MinorityOps++
		}
		switch op.Kind {
		case Put:
			_, _, err = sess.UploadRandom(op.Path, op.Size)
		case Get:
			_, _, err = sess.Download(op.Path)
		case Delete:
			err = sess.Delete(op.Path)
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
	// Quiet for 10 s, every peer must hold byte-identical state.
	if s.Metas >= 3 {
		c.Tick(10 * time.Second)
		if err := c.AssertMetaAgree(); err != nil {
			errs = append(errs, err)
		}
	}
	// Every client call of the run, the final read-back included, must fit one
	// sequential history of the namespace.
	res := check.Check(rec.Ops())
	r.History = res.Ops
	if err := res.Err(); err != nil {
		if opts.Artifacts != "" {
			art := filepath.Join(opts.Artifacts, fmt.Sprintf("seed-%d.html", s.Seed))
			if werr := writeArtifact(art, res); werr != nil {
				err = fmt.Errorf("%w (visualization not written: %v)", err, werr)
			} else {
				err = fmt.Errorf("%w (visualization: %s)", err, art)
			}
		}
		errs = append(errs, err)
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
		r.Err = fmt.Errorf("chaos seed %d: %w\n%s\nreplay: %s", s.Seed, err, s, Replay(s))
	}
	return r
}

func writeArtifact(path string, res check.Result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return res.WriteHTML(path)
}
