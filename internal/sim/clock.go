// Package sim provides deterministic, in-memory implementations of the iface
// seams. Everything in a sim run is driven by one Clock on one goroutine, so a
// run is a pure function of its seed and the sequence of Advance calls.
package sim

import (
	"container/heap"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Clock is a manually advanced clock and the sim's event queue. Events fire in
// (deadline, insertion order), which makes ties deterministic. It is not safe
// for concurrent use: the sim is single-threaded by design.
type Clock struct {
	now iface.Instant
	seq uint64
	q   eventQueue
}

var _ iface.Clock = (*Clock)(nil)

// NewClock returns a clock at Instant 0 with no pending events.
func NewClock() *Clock { return &Clock{} }

// Now returns the current simulated time.
func (c *Clock) Now() iface.Instant { return c.now }

// AfterFunc schedules f to run once the clock reaches Now()+d. A negative d is
// treated as zero; f still runs from Advance, never inline.
func (c *Clock) AfterFunc(d time.Duration, f func()) iface.Timer {
	if d < 0 {
		d = 0
	}
	c.seq++
	e := &event{at: c.now.Add(d), seq: c.seq, f: f, clock: c}
	heap.Push(&c.q, e)
	return e
}

// Advance moves time forward by d, firing every event due at or before the
// new time, including events scheduled by callbacks during the advance.
func (c *Clock) Advance(d time.Duration) {
	target := c.now.Add(d)
	for len(c.q) > 0 && c.q[0].at <= target {
		c.fireNext()
	}
	c.now = target
}

// Step jumps to the next pending event and fires it. It reports false if no
// event is pending.
func (c *Clock) Step() bool {
	if len(c.q) == 0 {
		return false
	}
	c.fireNext()
	return true
}

// Pending returns the number of scheduled events that have not fired.
func (c *Clock) Pending() int { return len(c.q) }

func (c *Clock) fireNext() {
	e := heap.Pop(&c.q).(*event)
	c.now = e.at
	e.f()
}

type event struct {
	at    iface.Instant
	seq   uint64
	f     func()
	idx   int // heap index; -1 once fired or stopped
	clock *Clock
}

func (e *event) Stop() bool {
	if e.idx < 0 {
		return false
	}
	heap.Remove(&e.clock.q, e.idx)
	return true
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].idx = i
	q[j].idx = j
}
func (q *eventQueue) Push(x any) {
	e := x.(*event)
	e.idx = len(*q)
	*q = append(*q, e)
}
func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.idx = -1
	*q = old[:n-1]
	return e
}
