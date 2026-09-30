// Package history records the client-visible operations of a run so a checker
// can decide whether they could have happened on one linearizable store. It
// holds data only: the checker (and porcupine with it) lives in history/check,
// which only tests and the chaos tool import, so the WASM build does not grow.
package history

import (
	"slices"
	"sync"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Kind is the operation a client invoked.
type Kind uint8

const (
	Put Kind = iota + 1
	Delete
	Read
	List
)

func (k Kind) String() string {
	switch k {
	case Put:
		return "put"
	case Delete:
		return "delete"
	case Read:
		return "read"
	case List:
		return "list"
	}
	return "?"
}

// Outcome is what the client learned.
type Outcome uint8

const (
	OK Outcome = iota + 1
	// Conflict: a compare-and-swap on the version failed; nothing applied.
	Conflict
	// NotFound: the path had no live version.
	NotFound
	// Ambiguous: the call failed in a way that may or may not have applied
	// (timeout, lost response, unavailable metadata). The checker lets it
	// have taken effect at any time after its call.
	Ambiguous
)

func (o Outcome) String() string {
	switch o {
	case OK:
		return "ok"
	case Conflict:
		return "conflict"
	case NotFound:
		return "notfound"
	case Ambiguous:
		return "ambiguous"
	}
	return "?"
}

// Entry is one file a List returned.
type Entry struct {
	Path    string
	Version uint64
	Hash    [32]byte
}

// Op is one completed client call. Times are simulated (or wall-clock)
// nanoseconds from one clock.
type Op struct {
	Client       int
	Kind         Kind
	Path         string // List: the prefix
	Call, Return int64
	Outcome      Outcome
	// Version is the version a put or delete created, or a read observed.
	Version uint64
	// Hash is the content a put wrote (known even when the outcome is
	// ambiguous) or a read returned.
	Hash   [32]byte
	Listed []Entry // List, OK
}

// Recorder collects ops from any number of clients.
type Recorder struct {
	mu  sync.Mutex
	ops []Op
}

// Classify maps a call's error to what the client can conclude from it.
func Classify(err error) Outcome {
	if err == nil {
		return OK
	}
	switch iface.CodeOf(err) {
	case iface.CodeConflict:
		return Conflict
	case iface.CodeNotFound:
		return NotFound
	}
	return Ambiguous
}

func (r *Recorder) add(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// Put records a put of content with hash. A put never reports NotFound; if it
// does, the result is ambiguous.
func (r *Recorder) Put(client int, path string, hash [32]byte, call, ret int64, version uint64, err error) {
	o := Classify(err)
	if o == NotFound {
		o = Ambiguous
	}
	r.add(Op{Client: client, Kind: Put, Path: path, Call: call, Return: ret, Outcome: o, Version: version, Hash: hash})
}

// Delete records a delete of path.
func (r *Recorder) Delete(client int, path string, call, ret int64, version uint64, err error) {
	r.add(Op{Client: client, Kind: Delete, Path: path, Call: call, Return: ret, Outcome: Classify(err), Version: version})
}

// Read records a read. A read that failed for any reason other than the path
// not existing tells the checker nothing about the store and is dropped.
func (r *Recorder) Read(client int, path string, call, ret int64, version uint64, hash [32]byte, err error) {
	o := Classify(err)
	if o != OK && o != NotFound {
		return
	}
	r.add(Op{Client: client, Kind: Read, Path: path, Call: call, Return: ret, Outcome: o, Version: version, Hash: hash})
}

// List records a listing; a failed one is dropped, like a failed read.
func (r *Recorder) List(client int, prefix string, call, ret int64, entries []Entry, err error) {
	if err != nil {
		return
	}
	r.add(Op{Client: client, Kind: List, Path: prefix, Call: call, Return: ret, Outcome: OK, Listed: slices.Clone(entries)})
}

// Ops returns a copy of what was recorded, in the order recorded.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ops)
}
