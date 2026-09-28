// Package runtime provides the real-mode event loop, wall clock and random
// source. Core components run on a Loop exactly as they run on the sim
// clock: one goroutine, no locks, callbacks in arrival order.
package runtime

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Loop runs posted functions one at a time on a single goroutine.
type Loop struct {
	q chan func()
}

// NewLoop returns a loop with a buffered queue; call Run to start it.
func NewLoop() *Loop { return &Loop{q: make(chan func(), 4096)} }

// Run executes posted functions until ctx is done.
func (l *Loop) Run(ctx context.Context) {
	for {
		select {
		case f := <-l.q:
			f()
		case <-ctx.Done():
			return
		}
	}
}

// Post queues f. It blocks if the queue is full, which applies backpressure
// to the network goroutines feeding it.
func (l *Loop) Post(f func()) { l.q <- f }

// Do runs f on the loop and waits for it. Use it to read core state from
// other goroutines (HTTP handlers, metrics).
func (l *Loop) Do(f func()) {
	done := make(chan struct{})
	l.Post(func() { f(); close(done) })
	<-done
}

// Clock is the wall clock with callbacks delivered through a Loop.
// Instants are monotonic time since the clock was created.
type Clock struct {
	loop  *Loop
	start time.Time
}

var _ iface.Clock = (*Clock)(nil)

// NewClock returns a clock whose timers fire on loop.
func NewClock(loop *Loop) *Clock { return &Clock{loop: loop, start: time.Now()} }

// Now returns monotonic time since the clock started.
func (c *Clock) Now() iface.Instant { return iface.Instant(time.Since(c.start)) }

// AfterFunc runs f on the loop after d.
func (c *Clock) AfterFunc(d time.Duration, f func()) iface.Timer {
	return timer{time.AfterFunc(d, func() { c.loop.Post(f) })}
}

// timer.Stop returns false once the callback has been queued, even if the
// loop has not run it yet; same contract as time.Timer.
type timer struct{ t *time.Timer }

func (t timer) Stop() bool { return t.t.Stop() }

// NewRand returns a randomly seeded generator. It is not safe for concurrent
// use; only the loop goroutine may call it.
func NewRand() iface.Rand { return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) }
