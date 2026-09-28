package obs

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// SIMPLIFIED: unlabelled counters and gauges only. Production systems use
// prometheus/client_golang for labels, histograms and exposition negotiation;
// this keeps the WASM build small until labels are needed.

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a value that can go up and down.
type Gauge struct{ bits atomic.Uint64 }

func (g *Gauge) Set(v float64)  { g.bits.Store(math.Float64bits(v)) }
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

type metric struct {
	help, kind string
	value      func() string
}

// Registry names metrics and renders them in Prometheus text format.
type Registry struct {
	mu      sync.Mutex
	metrics map[string]metric
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{metrics: map[string]metric{}} }

// Counter registers and returns a counter. Registering a name twice panics:
// it is a wiring bug, not a runtime condition.
func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{}
	r.add(name, metric{help, "counter", func() string { return fmt.Sprint(c.Value()) }})
	return c
}

// Gauge registers and returns a gauge.
func (r *Registry) Gauge(name, help string) *Gauge {
	g := &Gauge{}
	r.add(name, metric{help, "gauge", func() string { return fmt.Sprint(g.Value()) }})
	return g
}

func (r *Registry) add(name string, m metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.metrics[name]; dup {
		panic("obs: duplicate metric " + name)
	}
	r.metrics[name] = m
}

// WriteText writes every metric, sorted by name, in Prometheus text format 0.0.4.
func (r *Registry) WriteText(w io.Writer) error {
	r.mu.Lock()
	names := make([]string, 0, len(r.metrics))
	for n := range r.metrics {
		names = append(names, n)
	}
	sort.Strings(names)
	ms := make([]metric, len(names))
	for i, n := range names {
		ms[i] = r.metrics[n]
	}
	r.mu.Unlock()
	for i, n := range names {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", n, ms[i].help, n, ms[i].kind, n, ms[i].value()); err != nil {
			return err
		}
	}
	return nil
}
