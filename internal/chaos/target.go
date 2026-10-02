package chaos

import (
	"cmp"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/history"
	"github.com/insanityatpeak/chunkd/internal/history/check"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// ErrUnsupported is returned by a Target for faults it cannot inject.
var ErrUnsupported = errors.New("fault not supported by this target")

// Target is a cluster the real-mode runner drives: separate processes on a
// wall clock. Apply returns ErrUnsupported for faults the target cannot
// inject; the runner skips them and says so. Put and Delete return the
// version they created, Get the version it read, for the history check.
type Target interface {
	Apply(f Fault) error
	Put(path string, data []byte) (uint64, error)
	Get(path string) ([]byte, uint64, error)
	Delete(path string) (uint64, error)
	Cluster() (client.Cluster, error)
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
	// Leaders is each metadata leader the health poll saw, in order.
	Leaders []string
	// History is how many recorded operations the linearizability check covered.
	History int
	Err     error
}

// RunTarget runs s against t on the wall clock. Faults fire on their own
// schedule while operations run. bound limits both the longest
// under-replicated stretch and the time to settle after quiet. Every path
// lives under a namespace of its own for this run, so the recorded history
// starts from empty paths even on a cluster earlier scenarios wrote to; the
// content written is still Data(seed, the scenario's path, size).
func RunTarget(s Scenario, t Target, bound time.Duration, logf func(string, ...any), opts Options) RealReport {
	r := RealReport{Name: s.Name, Bound: bound, Ops: map[string]int{}}
	c0, err := t.Cluster()
	h0 := c0.Health
	if err != nil {
		r.Err = fmt.Errorf("%s: health before start: %w", s.Name, err)
		return r
	}
	ns := fmt.Sprintf("/run-%d/%s", time.Now().UnixMilli(), s.Name)
	rec := &history.Recorder{}
	now := func() int64 { return time.Now().UnixNano() }

	// Poll replication health once a second for the whole run.
	var mu sync.Mutex
	var underSince time.Time
	var polls, lost int
	var last client.Health
	counts := leaderDeltas{prev: h0, leader: c0.MetaLeader}
	allAlive := false
	stop := make(chan struct{})
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			cl, err := t.Cluster()
			h := cl.Health
			now := time.Now()
			mu.Lock()
			if err == nil {
				polls++
				last = h
				counts.add(cl.MetaLeader, h)
				if l := cl.MetaLeader; l != "" && (len(r.Leaders) == 0 || r.Leaders[len(r.Leaders)-1] != l) {
					r.Leaders = append(r.Leaders, l)
				}
				allAlive = !slices.ContainsFunc(cl.Nodes, func(n client.NodeInfo) bool { return n.State != "alive" })
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

	var errs []error
	killedLeader := false
	start := time.Now()
	faultsDone := make(chan struct{})
	go func() {
		defer close(faultsDone)
		for _, f := range s.Faults {
			time.Sleep(time.Until(start.Add(f.At)))
			err := t.Apply(f)
			mu.Lock()
			switch {
			case errors.Is(err, ErrUnsupported):
				r.Skipped = append(r.Skipped, f)
			case err != nil:
				errs = append(errs, fmt.Errorf("%v: %w", f, err))
			default:
				killedLeader = killedLeader || f.Kind == KillLeader
				logf("%s: %v", s.Name, f)
			}
			mu.Unlock()
		}
	}()

	type ack struct {
		hashes [][32]byte
		gone   bool
	}
	acked := map[string]*ack{}
	written := map[string][][32]byte{} // every content sent to a path
	for _, op := range s.Ops {
		time.Sleep(time.Until(start.Add(op.At)))
		p := ns + op.Path
		var err error
		call := now()
		switch op.Kind {
		case Put:
			data := Data(s.Seed, op.Path, op.Size)
			sum := sha256.Sum256(data)
			written[p] = append(written[p], sum)
			var v uint64
			v, err = t.Put(p, data)
			rec.Put(1, p, sum, call, now(), v, err)
			switch a := acked[p]; {
			case err == nil:
				acked[p] = &ack{hashes: [][32]byte{sum}}
			case a != nil:
				a.hashes = append(a.hashes, sum)
			}
		case Get:
			var data []byte
			var v uint64
			data, v, err = t.Get(p)
			rec.Read(1, p, call, now(), v, sha256.Sum256(data), err)
			// Invariant: a read never returns bytes nobody wrote there.
			if err == nil && !slices.Contains(written[p], sha256.Sum256(data)) {
				mu.Lock()
				errs = append(errs, fmt.Errorf("a read of %s at %v returned %d bytes never written there", op.Path, op.At, len(data)))
				mu.Unlock()
			}
		case Delete:
			var v uint64
			v, err = t.Delete(p)
			rec.Delete(1, p, call, now(), v, err)
			switch a := acked[p]; {
			case err == nil:
				delete(acked, p)
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

	quiet := time.Now()
	for {
		mu.Lock()
		h, alive, seen := last, allAlive, int(counts.found)
		mu.Unlock()
		// Settled also means every node is alive: a returning node is
		// suspect first, and until then its extra copies do not count as
		// over-replication, so the next scenario would start mid-reconcile.
		if h.UnderReplicated == 0 && h.OverReplicated == 0 && alive && seen >= s.WantCorrupt && time.Since(quiet) > 2*time.Second {
			break
		}
		if time.Since(quiet) > bound {
			errs = append(errs, fmt.Errorf("replication not settled %v after quiet: %d under, %d over, all nodes alive %v, %d of %d rotted copies found",
				bound, h.UnderReplicated, h.OverReplicated, alive, seen, s.WantCorrupt))
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	close(stop)
	<-polled

	for _, p := range slices.Sorted(maps.Keys(acked)) {
		a := acked[p]
		call := now()
		data, v, err := t.Get(p)
		rec.Read(1, p, call, now(), v, sha256.Sum256(data), err)
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
	r.RepairCopies = counts.repaired
	if s.NoRepair && r.RepairCopies > 0 {
		errs = append(errs, fmt.Errorf("%d repair copies for a transient fault", r.RepairCopies))
	}
	if polls == 0 {
		errs = append(errs, errors.New("health was never readable"))
	}
	if killedLeader && len(r.Leaders) < 2 {
		errs = append(errs, fmt.Errorf("the metadata leader was killed but no failover was seen (leaders %v)", r.Leaders))
	}
	res := check.Check(rec.Ops())
	r.History = res.Ops
	if err := res.Err(); err != nil {
		if opts.Artifacts != "" {
			art := filepath.Join(opts.Artifacts, s.Name+".html")
			if werr := writeArtifact(art, res); werr != nil {
				err = fmt.Errorf("%w (visualization not written: %v)", err, werr)
			} else {
				err = fmt.Errorf("%w (visualization: %s)", err, art)
			}
		}
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		r.Err = fmt.Errorf("%s: %w\n%s", s.Name, err, s)
	}
	return r
}

// ShortSuite is the real-mode suite CI runs on every push. Timings assume
// the default detector (dead after 10 s) and repair delay (20 s).
func ShortSuite() []Scenario {
	// n files of 0.5 to 4 MiB (+1 byte, so chunk boundaries vary), 200 ms apart.
	preload := func(n int) []Op {
		var ops []Op
		for i := range n {
			ops = append(ops, Op{At: time.Duration(i) * 200 * time.Millisecond, Kind: Put,
				Path: fmt.Sprintf("/chaos/f%02d", i), Size: 1 + int64(i%8+1)*(512<<10)})
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
			// TestKillNodeRestoresRF, real mode: 50 files (about 115 MiB),
			// then the node stays down well past dead + delay, so repair
			// must restore RF 3 on the other four nodes; on return the
			// extra copies are trimmed.
			Name: "kill-node-restores-rf", Seed: 101, Nodes: 5, Length: 120 * time.Second,
			Ops:    append(preload(50), reads(20*time.Second, 110*time.Second, 50)...),
			Faults: []Fault{{At: 20 * time.Second, Kind: Kill, Node: "node-3"}, {At: 105 * time.Second, Kind: Restart, Node: "node-3"}},
		},
		{
			// TestTransientBlipNoRepair, real mode: restarted inside the
			// repair delay, so zero copies.
			Name: "transient-blip-no-repair", Seed: 102, Nodes: 5, Length: 45 * time.Second, NoRepair: true,
			Ops:    append(preload(6), reads(8*time.Second, 40*time.Second, 6)...),
			Faults: []Fault{{At: 6 * time.Second, Kind: Kill, Node: "node-2"}, {At: 21 * time.Second, Kind: Restart, Node: "node-2"}},
		},
		{
			// TestCorruptChunkDetectedOnRead and TestScrubberFindsCorruption,
			// real mode: flip bytes in 4 chunk files on node-2's volume.
			// Reads or the scrubber (30 s passes in compose) find all 4,
			// quarantine them, and repair restores RF; reads never return
			// bad bytes.
			Name: "corrupt-replicas", Seed: 104, Nodes: 5, Length: 45 * time.Second, WantCorrupt: 4,
			Ops:    append(preload(6), reads(8*time.Second, 40*time.Second, 6)...),
			Faults: []Fault{{At: 6 * time.Second, Kind: Corrupt, Node: "node-2", Count: 4, Pick: 104}},
		},
		{
			// A paused process (docker pause) serves nothing while reads
			// continue; hedging covers them, the detector marks it suspect
			// then dead, and it returns without data loss.
			Name: "freeze-node", Seed: 103, Nodes: 5, Length: 40 * time.Second,
			Ops:    append(preload(6), reads(8*time.Second, 35*time.Second, 6)...),
			Faults: []Fault{{At: 6 * time.Second, Kind: Freeze, Node: "node-1"}, {At: 20 * time.Second, Kind: Thaw, Node: "node-1"}},
		},
		{
			// TestKillLeaderMidUpload, real mode: the metadata leader (found
			// from the cluster view when the fault fires) is killed while
			// writes go on every 2 s, then restarted over its log 20 s later.
			// The gateway's client finds the new leader; every acknowledged
			// upload reads back, the history is linearizable, and a failover
			// must have been seen.
			Name: "kill-meta-leader", Seed: 105, Nodes: 5, Length: 60 * time.Second,
			Ops:    sorted(append(append(preload(6), reads(8*time.Second, 55*time.Second, 6)...), writes(10*time.Second, 50*time.Second)...)),
			Faults: []Fault{{At: 15 * time.Second, Kind: KillLeader}, {At: 35 * time.Second, Kind: ReviveLeader}},
		},
	}
}

// writes puts a file of 256 KiB and up every 2 s in [from, to).
func writes(from, to time.Duration) []Op {
	var ops []Op
	for t, i := from, 0; t < to; t, i = t+2*time.Second, i+1 {
		ops = append(ops, Op{At: t, Kind: Put, Path: fmt.Sprintf("/chaos/w%02d", i), Size: 256<<10 + int64(i)*4099})
	}
	return ops
}

// sorted orders ops by time; RunTarget issues them in slice order.
func sorted(ops []Op) []Op {
	slices.SortStableFunc(ops, func(a, b Op) int { return cmp.Compare(a.At, b.At) })
	return ops
}

// leaderDeltas adds up repair and corruption counts across leader changes.
// Repair copies are those a chunk below RF caused: drain copies and balance
// moves, which a transient fault may follow from an earlier scenario, are
// left out.
// The counters are the answering leader's, since its process started: they
// restart with a new leader, so only deltas within one leader count. Copies
// in the second before a new leader's first poll are lost.
type leaderDeltas struct {
	prev            client.Health
	leader          string
	repaired, found uint64
}

func (d *leaderDeltas) add(leader string, h client.Health) {
	repairs := func(h client.Health) uint64 { return h.RepairCompleted - h.RepairEvacuated - h.RepairMoved }
	if leader == d.leader && repairs(h) >= repairs(d.prev) && h.CorruptReplicas >= d.prev.CorruptReplicas {
		d.repaired += repairs(h) - repairs(d.prev)
		d.found += h.CorruptReplicas - d.prev.CorruptReplicas
	}
	d.prev, d.leader = h, leader
}
