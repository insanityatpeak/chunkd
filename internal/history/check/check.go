// Package check decides whether a recorded history is linearizable against a
// versioned key-value model of the namespace, using porcupine. Only tests and
// the chaos tool import it.
//
// Model, per path: {live, ver, hash}. ver counts every version ever created,
// tombstones included, and is never reused, so a successful put or delete
// must return exactly ver+1. A put that failed ambiguously either did not
// apply or applied as ver+1. Ambiguous operations return at +inf: they may
// take effect at any point after their call.
//
// SIMPLIFIED: a List is decomposed into one read per path it could have
// mentioned, all over the list's own interval. That checks each path's entry
// against the model but not that the listing was one atomic snapshot across
// paths. Jepsen's set and bank checkers verify snapshot atomicity directly.
//
// Sim clients block, so their calls never overlap; concurrency in the sim
// comes from ambiguous operations. Real-mode histories overlap for real.
package check

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/insanityatpeak/chunkd/internal/history"
)

type state struct {
	Live bool
	Ver  uint64
	Hash [32]byte
}

type input struct {
	Kind history.Kind
	Path string
	Hash [32]byte // put: content written
}

type output struct {
	Outcome history.Outcome
	Version uint64
	Hash    [32]byte // read: content returned
}

func step(st, in, out any) []any {
	s, i, o := st.(state), in.(input), out.(output)
	same := []any{s}
	switch i.Kind {
	case history.Put:
		applied := state{Live: true, Ver: s.Ver + 1, Hash: i.Hash}
		switch o.Outcome {
		case history.OK:
			if o.Version == s.Ver+1 {
				return []any{applied}
			}
		case history.Conflict:
			return same
		case history.Ambiguous:
			return []any{s, applied}
		}
	case history.Delete:
		applied := state{Live: false, Ver: s.Ver + 1}
		switch o.Outcome {
		case history.OK:
			if s.Live && o.Version == s.Ver+1 {
				return []any{applied}
			}
		case history.NotFound:
			if !s.Live {
				return same
			}
		case history.Conflict:
			return same
		case history.Ambiguous:
			if s.Live {
				return []any{s, applied}
			}
			return same
		}
	case history.Read:
		switch o.Outcome {
		case history.OK:
			if s.Live && s.Ver == o.Version && s.Hash == o.Hash {
				return same
			}
		case history.NotFound:
			if !s.Live {
				return same
			}
		}
	}
	return nil
}

func describeOp(in, out any) string {
	i, o := in.(input), out.(output)
	switch i.Kind {
	case history.Put:
		return fmt.Sprintf("put %x -> %s", i.Hash[:4], describeOut(o, false))
	case history.Read:
		return "read -> " + describeOut(o, true)
	}
	return "delete -> " + describeOut(o, false)
}

func describeOut(o output, hash bool) string {
	switch {
	case o.Outcome != history.OK:
		return o.Outcome.String()
	case hash:
		return fmt.Sprintf("v%d %x", o.Version, o.Hash[:4])
	}
	return fmt.Sprintf("v%d", o.Version)
}

// Model is the porcupine model of one path; Check partitions a history by path.
func Model() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
			by := map[string][]porcupine.Operation{}
			for _, op := range h {
				p := op.Input.(input).Path
				by[p] = append(by[p], op)
			}
			out := make([][]porcupine.Operation, 0, len(by))
			for _, p := range slices.Sorted(maps.Keys(by)) {
				out = append(out, by[p])
			}
			return out
		},
		Init:              func() []any { return []any{state{}} },
		Step:              step,
		Equal:             func(a, b any) bool { return a.(state) == b.(state) },
		DescribeOperation: describeOp,
		DescribeState: func(st any) string {
			s := st.(state)
			if !s.Live {
				return fmt.Sprintf("absent (last v%d)", s.Ver)
			}
			return fmt.Sprintf("v%d %x", s.Ver, s.Hash[:4])
		},
	}
	return nm.ToModel()
}

// Result is the outcome of one check.
type Result struct {
	// OK: the history is linearizable.
	OK bool
	// Unknown: the search hit its timeout without a verdict; treat as a failure.
	Unknown bool
	Ops     int
	info    porcupine.LinearizationInfo
	model   porcupine.Model
}

func (r Result) String() string {
	switch {
	case r.OK:
		return fmt.Sprintf("linearizable (%d ops)", r.Ops)
	case r.Unknown:
		return fmt.Sprintf("linearizability check timed out (%d ops)", r.Ops)
	}
	return fmt.Sprintf("history of %d ops is not linearizable", r.Ops)
}

// Err is nil for a linearizable history.
func (r Result) Err() error {
	if r.OK {
		return nil
	}
	return fmt.Errorf("%s", r)
}

// WriteHTML writes porcupine's visualization of the history to path.
func (r Result) WriteHTML(path string) error {
	return porcupine.VisualizePath(r.model, r.info, path)
}

// Timeout bounds one check. Histories from a chaos run have tens of
// operations per path, far inside it.
const Timeout = 30 * time.Second

// Check runs the linearizability check over ops.
func Check(ops []history.Op) Result {
	h := operations(ops)
	m := Model()
	res, info := porcupine.CheckOperationsVerbose(m, h, Timeout)
	return Result{OK: res == porcupine.Ok, Unknown: res == porcupine.Unknown, Ops: len(h), info: info, model: m}
}

// operations converts ops for porcupine: ambiguous operations return after
// every observed time, and each list becomes one read per candidate path.
func operations(ops []history.Op) []porcupine.Operation {
	var last int64
	paths := map[string]bool{}
	for _, o := range ops {
		last = max(last, o.Call, o.Return)
		if o.Kind != history.List {
			paths[o.Path] = true
		}
		for _, e := range o.Listed {
			paths[e.Path] = true
		}
	}
	inf := last + 1
	var out []porcupine.Operation
	for _, o := range ops {
		ret := o.Return
		if o.Outcome == history.Ambiguous {
			ret = inf
		}
		if o.Kind != history.List {
			out = append(out, porcupine.Operation{ClientId: o.Client, Call: o.Call, Return: ret,
				Input:  input{Kind: o.Kind, Path: o.Path, Hash: hashOf(o)},
				Output: output{Outcome: o.Outcome, Version: o.Version, Hash: readHash(o)}})
			continue
		}
		listed := map[string]history.Entry{}
		for _, e := range o.Listed {
			listed[e.Path] = e
		}
		for _, p := range slices.Sorted(maps.Keys(paths)) {
			if !strings.HasPrefix(p, o.Path) {
				continue
			}
			in := input{Kind: history.Read, Path: p}
			out2 := output{Outcome: history.NotFound}
			if e, ok := listed[p]; ok {
				out2 = output{Outcome: history.OK, Version: e.Version, Hash: e.Hash}
			}
			out = append(out, porcupine.Operation{ClientId: o.Client, Call: o.Call, Return: ret, Input: in, Output: out2})
		}
	}
	return out
}

func hashOf(o history.Op) [32]byte {
	if o.Kind == history.Put {
		return o.Hash
	}
	return [32]byte{}
}

func readHash(o history.Op) [32]byte {
	if o.Kind == history.Read {
		return o.Hash
	}
	return [32]byte{}
}
