package obs

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// SIMPLIFIED: counters, gauges and one-label gauge families only.
// Production systems use prometheus/client_golang for arbitrary labels,
// histograms and exposition negotiation; this keeps the WASM build small.

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

// Mirror sets the counter to a monotonic total kept elsewhere, e.g. by a
// component on an event loop that the scrape goroutine must not touch.
func (c *Counter) Mirror(total uint64) { c.v.Store(total) }

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
	vecs    map[string]*GaugeVec
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{metrics: map[string]metric{}, vecs: map[string]*GaugeVec{}}
}

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

// GaugeVec is a gauge family with one label, e.g. nodes{state="alive"}.
type GaugeVec struct {
	label string
	mu    sync.Mutex
	vals  map[string]float64
}

// Set sets the gauge for one label value.
func (v *GaugeVec) Set(labelValue string, x float64) {
	v.mu.Lock()
	v.vals[labelValue] = x
	v.mu.Unlock()
}

// GaugeVec registers a one-label gauge family.
func (r *Registry) GaugeVec(name, help, label string) *GaugeVec {
	v := &GaugeVec{label: label, vals: map[string]float64{}}
	r.add(name, metric{help, "gauge", nil})
	r.mu.Lock()
	r.vecs[name] = v
	r.mu.Unlock()
	return v
}

func (v *GaugeVec) lines(name string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	keys := make([]string, 0, len(v.vals))
	for k := range v.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s{%s=%q} %v\n", name, v.label, k, v.vals[k])
	}
	return b.String()
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
	vecs := make([]*GaugeVec, len(names))
	for i, n := range names {
		ms[i], vecs[i] = r.metrics[n], r.vecs[n]
	}
	r.mu.Unlock()
	for i, n := range names {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", n, ms[i].help, n, ms[i].kind); err != nil {
			return err
		}
		var err error
		if vecs[i] != nil {
			_, err = io.WriteString(w, vecs[i].lines(n))
		} else {
			_, err = fmt.Fprintf(w, "%s %s\n", n, ms[i].value())
		}
		if err != nil {
			return err
		}
	}
	return nil
}
