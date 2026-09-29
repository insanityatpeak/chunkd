package chaos

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// ErrUnsupported is returned by a Target for faults it cannot inject.
var ErrUnsupported = errors.New("fault not supported by this target")

// Target is a cluster the real-mode runner drives: separate processes on a
// wall clock. Apply returns ErrUnsupported for faults the target cannot
// inject; the runner skips them and says so.
type Target interface {
	Apply(f Fault) error
	Put(path string, data []byte) error
	Get(path string) ([]byte, error)
	Delete(path string) error
	Health() (client.Health, error)
}

// Data is the content a scenario writes to path: a pure function of the
// seed, the path and the size, so a real run can be compared to its sim run.
func Data(seed uint64, path string, size int64) []byte {
	h := fnv.New64a()
	h.Write([]byte(path))
	r := rand.New(rand.NewPCG(seed, h.Sum64()))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// RealReport is the outcome of a run against a Target.
type RealReport struct {
	Name    string
	Skipped []Fault // faults the target could not inject
	// LongestUnder is the longest stretch with any chunk under-replicated;
	// it must stay within Bound.
	LongestUnder time.Duration
	Bound        time.Duration
	RepairCopies uint64 // during this scenario
	Ops          map[string]int
	Err          error
}

// RunTarget runs s against t on the wall clock. Faults fire on their own
// schedule while operations run. bound limits both the longest
// under-replicated stretch and the time to settle after quiet.
func RunTarget(s Scenario, t Target, bound time.Duration, logf func(string, ...any)) RealReport {
	r := RealReport{Name: s.Name, Bound: bound, Ops: map[string]int{}}
	h0, err := t.Health()
	if err != nil {
		r.Err = fmt.Errorf("%s: health before start: %w", s.Name, err)
		return r
	}

	// Poll replication health once a second for the whole run.
	var mu sync.Mutex
	var underSince time.Time
	var polls, lost int
	var last client.Health
	stop := make(chan struct{})
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			h, err := t.Health()
			now := time.Now()
			mu.Lock()
			if err == nil {
				polls++
				last = h
				switch {
				case h.UnderReplicated > 0 && underSince.IsZero():
					underSince = now
				case h.UnderReplicated == 0 && !underSince.IsZero():
					r.LongestUnder = max(r.LongestUnder, now.Sub(underSince))
					underSince = time.Time{}
				}
				if !underSince.IsZero() {
					r.LongestUnder = max(r.LongestUnder, now.Sub(underSince))
				}
				lost = max(lost, int(h.Lost))
			}
			mu.Unlock()
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()

	start := time.Now()
	faultsDone := make(chan struct{})
	go func() {
		defer close(faultsDone)
		for _, f := range s.Faults {
			time.Sleep(time.Until(start.Add(f.At)))
			err := t.Apply(f)
			switch {
			case errors.Is(err, ErrUnsupported):
				mu.Lock()
				r.Skipped = append(r.Skipped, f)
				mu.Unlock()
			case err != nil:
				logf("%s: %v failed: %v", s.Name, f, err)
			default:
				logf("%s: %v", s.Name, f)
			}
		}
	}()

	type ack struct {
		hashes [][32]byte
		gone   bool
	}
	acked := map[string]*ack{}
	for _, op := range s.Ops {
		time.Sleep(time.Until(start.Add(op.At)))
		var err error
		switch op.Kind {
		case Put:
			data := Data(s.Seed, op.Path, op.Size)
			err = t.Put(op.Path, data)
			sum := sha256.Sum256(data)
			switch a := acked[op.Path]; {
			case err == nil:
				acked[op.Path] = &ack{hashes: [][32]byte{sum}}
			case a != nil:
				a.hashes = append(a.hashes, sum)
			}
		case Get:
			_, err = t.Get(op.Path)
		case Delete:
			err = t.Delete(op.Path)
			switch a := acked[op.Path]; {
			case err == nil:
				delete(acked, op.Path)
			case a != nil:
				a.gone = true
			}
		}
		outcome := "ok"
		switch {
		case iface.CodeOf(err) == iface.CodeNotFound:
			outcome = "not found"
		case err != nil:
			outcome = "failed"
			logf("%s: %s %s: %v", s.Name, op.Kind, op.Path, err)
		}
		r.Ops[string(op.Kind)+" "+outcome]++
	}
	<-faultsDone

	var errs []error
	quiet := time.Now()
	for {
		mu.Lock()
		h := last
		mu.Unlock()
		if h.UnderReplicated == 0 && h.OverReplicated == 0 && time.Since(quiet) > 2*time.Second {
			break
		}
		if time.Since(quiet) > bound {
			errs = append(errs, fmt.Errorf("replication not settled %v after quiet: %d under, %d over", bound, h.UnderReplicated, h.OverReplicated))
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	close(stop)
	<-polled

	for _, p := range slices.Sorted(maps.Keys(acked)) {
		a := acked[p]
		data, err := t.Get(p)
		if err != nil {
			if !(a.gone && iface.CodeOf(err) == iface.CodeNotFound) {
				errs = append(errs, fmt.Errorf("acknowledged %s unreadable: %w", p, err))
			}
			continue
		}
		if !slices.Contains(a.hashes, sha256.Sum256(data)) {
			errs = append(errs, fmt.Errorf("acknowledged %s reads back different content", p))
		}
	}
	if lost > 0 {
		errs = append(errs, fmt.Errorf("%d chunks had no live copy", lost))
	}
	if r.LongestUnder > bound {
		errs = append(errs, fmt.Errorf("chunks under-replicated for %v, bound %v", r.LongestUnder, bound))
	}
	r.RepairCopies = last.RepairCompleted - h0.RepairCompleted
	if s.NoRepair && r.RepairCopies > 0 {
		errs = append(errs, fmt.Errorf("%d repair copies for a transient fault", r.RepairCopies))
	}
	if polls == 0 {
		errs = append(errs, errors.New("health was never readable"))
	}
	if err := errors.Join(errs...); err != nil {
		r.Err = fmt.Errorf("%s: %w\n%s", s.Name, err, s)
	}
	return r
}

// ShortSuite is the real-mode suite CI runs on every push. Timings assume
// the default detector (dead after 10 s) and repair delay (20 s).
func ShortSuite() []Scenario {
	preload := func(n int) []Op {
		var ops []Op
		for i := range n {
			ops = append(ops, Op{At: time.Duration(i) * 200 * time.Millisecond, Kind: Put,
				Path: fmt.Sprintf("/chaos/f%02d", i), Size: 1 + int64(i+1)*(700<<10)})
		}
		return ops
	}
	reads := func(from, to time.Duration, n int) []Op {
		var ops []Op
		for t := from; t < to; t += time.Second {
			ops = append(ops, Op{At: t, Kind: Get, Path: fmt.Sprintf("/chaos/f%02d", int(t/time.Second)%n)})
		}
		return ops
	}
	return []Scenario{
		{
			// TestKillNodeRestoresRF, real mode: the node stays down well
			// past dead + delay, so repair must restore RF 3 on the other
			// four nodes; on return the extra copies are trimmed.
			Name: "kill-node-restores-rf", Seed: 101, Nodes: 5, Length: 100 * time.Second,
			Ops:    append(preload(12), reads(10*time.Second, 90*time.Second, 12)...),
			Faults: []Fault{{At: 8 * time.Second, Kind: Kill, Node: "node-3"}, {At: 85 * time.Second, Kind: Restart, Node: "node-3"}},
		},
		{
			// TestTransientBlipNoRepair, real mode: restarted inside the
			// repair delay, so zero copies.
			Name: "transient-blip-no-repair", Seed: 102, Nodes: 5, Length: 45 * time.Second, NoRepair: true,
			Ops:    append(preload(6), reads(8*time.Second, 40*time.Second, 6)...),
			Faults: []Fault{{At: 6 * time.Second, Kind: Kill, Node: "node-2"}, {At: 21 * time.Second, Kind: Restart, Node: "node-2"}},
		},
		{
			// A paused process (docker pause) serves nothing while reads
			// continue; hedging covers them, the detector marks it suspect
			// then dead, and it returns without data loss.
			Name: "freeze-node", Seed: 103, Nodes: 5, Length: 40 * time.Second,
			Ops:    append(preload(6), reads(8*time.Second, 35*time.Second, 6)...),
			Faults: []Fault{{At: 6 * time.Second, Kind: Freeze, Node: "node-1"}, {At: 20 * time.Second, Kind: Thaw, Node: "node-1"}},
		},
	}
}
