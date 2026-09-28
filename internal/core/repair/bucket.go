package repair

import (
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Bucket is a token bucket over logical time. Tokens are bytes; they refill
// at Rate per second up to Burst. A take may exceed the balance once it is at
// least min(n, Burst), leaving a debt: a chunk larger than the burst still
// goes, and the long-run rate stays at Rate.
//
// Integer arithmetic only (the level is kept in byte-nanoseconds), so a sim
// run replays identically on every platform.
type Bucket struct {
	rate  int64 // bytes per second; <= 0 means unlimited
	burst int64
	level int64 // bytes × 1e9
	at    iface.Instant
	init  bool
}

// NewBucket returns a full bucket.
func NewBucket(rate, burst int64) *Bucket {
	return &Bucket{rate: rate, burst: burst}
}

const nano = int64(time.Second)

func (b *Bucket) refill(now iface.Instant) {
	if !b.init {
		b.level, b.at, b.init = b.burst*nano, now, true
		return
	}
	if now <= b.at {
		return
	}
	full := b.burst * nano
	// Cap elapsed before multiplying so rate × elapsed cannot overflow.
	toFull := (full - b.level + b.rate - 1) / b.rate
	if elapsed := int64(now - b.at); elapsed >= toFull {
		b.level = full
	} else {
		b.level += b.rate * elapsed
	}
	b.at = now
}

// Take spends n bytes if the bucket allows it now.
func (b *Bucket) Take(n int64, now iface.Instant) bool {
	if b.rate <= 0 {
		return true
	}
	b.refill(now)
	if b.level < min(n, b.burst)*nano {
		return false
	}
	b.level -= n * nano
	return true
}

// ReadyAt is the earliest instant at which Take(n) can succeed.
func (b *Bucket) ReadyAt(n int64, now iface.Instant) iface.Instant {
	if b.rate <= 0 {
		return now
	}
	b.refill(now)
	need := min(n, b.burst)*nano - b.level
	if need <= 0 {
		return now
	}
	return now + iface.Instant((need+b.rate-1)/b.rate)
}
