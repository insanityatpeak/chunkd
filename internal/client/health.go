package client

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// Hedging bounds. Below hedgeMin a hedge doubles load for no gain; above
// hedgeMax a stuck replica costs more than a second read does.
const (
	hedgeMin     = 20 * time.Millisecond
	hedgeMax     = 500 * time.Millisecond
	hedgeDefault = 100 * time.Millisecond
	// hedgeWarmup samples are needed before the p95 estimate is trusted.
	hedgeWarmup = 16
	ewmaAlpha   = 0.2
	errDecay    = 0.8
)

// health scores storage nodes by the chunk reads this client has seen. It
// is advisory and local: it orders replicas and sets the hedge delay, and
// never affects placement or repair. It catches gray failures (slow but
// heartbeating nodes) that the metadata server's detector cannot see.
type health struct {
	mu     sync.Mutex
	nodes  map[string]*nodeHealth
	recent [128]time.Duration // successful read latencies, ring buffer
	n      int
}

type nodeHealth struct {
	ewma time.Duration
	errs float64 // decaying error count
}

func newHealth() *health { return &health{nodes: map[string]*nodeHealth{}} }

// observe records one read. A pending result's latency is a lower bound, so
// it moves the node's score but not the p95 of healthy reads.
func (h *health) observe(node string, r iface.Result, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	nh := h.nodes[node]
	if nh == nil {
		nh = &nodeHealth{ewma: r.Latency}
		h.nodes[node] = nh
	}
	if r.Pending || ok {
		nh.ewma = time.Duration((1-ewmaAlpha)*float64(nh.ewma) + ewmaAlpha*float64(r.Latency))
	}
	if !ok && !r.Pending {
		nh.errs = nh.errs*errDecay + 1
		return
	}
	nh.errs *= errDecay
	if ok {
		h.recent[h.n%len(h.recent)] = r.Latency
		h.n++
	}
}

// score is lower for better nodes. Unknown nodes score 0 so they get tried
// and measured.
func (h *health) score(node string) float64 {
	nh := h.nodes[node]
	if nh == nil {
		return 0
	}
	return float64(nh.ewma) * (1 + nh.errs)
}

// order returns replicas with alive ones before suspect ones, each group by
// score. The sort is stable, so ties keep the metadata server's order.
func (h *health) order(reps []*chunkdv1.Replica) []*chunkdv1.Replica {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := slices.Clone(reps)
	slices.SortStableFunc(out, func(a, b *chunkdv1.Replica) int {
		if a.GetSuspect() != b.GetSuspect() {
			if a.GetSuspect() {
				return 1
			}
			return -1
		}
		return cmp.Compare(h.score(a.GetNode()), h.score(b.GetNode()))
	})
	return out
}

// hedgeDelay is the p95 of recent successful reads, clamped.
func (h *health) hedgeDelay() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.n < hedgeWarmup {
		return hedgeDefault
	}
	s := slices.Clone(h.recent[:min(h.n, len(h.recent))])
	slices.Sort(s)
	return min(max(s[len(s)*95/100], hedgeMin), hedgeMax)
}
