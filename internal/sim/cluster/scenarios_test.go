package cluster

import (
	"io"
	"strings"
	"testing"
	"time"
)

// play starts a scenario the way the dashboard does (right after the
// cluster starts) and runs it in the browser worker's 50 ms steps.
func play(t *testing.T, seed uint64, name string, d time.Duration) *Cluster {
	t.Helper()
	c := New(seed, DefaultConfig(), io.Discard)
	if err := c.RunScenario(name); err != nil {
		t.Fatal(err)
	}
	for end := c.Now().Add(d); c.Now() < end; {
		c.Tick(50 * time.Millisecond)
	}
	return c
}

func eventsText(s State) string {
	var b strings.Builder
	for _, e := range s.Events {
		b.WriteString(e.Node + " " + e.Text + "\n")
	}
	for _, e := range s.Reads {
		b.WriteString(e.Node + " " + e.Text + "\n")
	}
	return b.String()
}

func TestScenarios(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  time.Duration
		want []string // substrings the timeline must contain
		then func(t *testing.T, c *Cluster, s State)
	}{
		{"kill-node", 150 * time.Second, []string{"node-3 alive → suspect", "node-3 suspect → dead", " completed after", "node-3 dead → suspect (restarted)", " done: chunk"}, nil},
		{"corrupt-chunk", 90 * time.Second, []string{"node-2 chunk", "failed verification"}, func(t *testing.T, c *Cluster, s State) {
			if s.Health.CorruptReplicas != 3 {
				t.Fatalf("%d corrupt copies found, want 3", s.Health.CorruptReplicas)
			}
		}},
		{"rack-loss", 90 * time.Second, []string{"node-1 suspect → dead", "node-4 suspect → dead", " completed after"}, func(t *testing.T, c *Cluster, s State) {
			if s.Health.Lost != 0 {
				t.Fatalf("%d chunks lost to one rack", s.Health.Lost)
			}
		}},
		{"slow-node", 75 * time.Second, []string{"client read /demo/", "hedged chunk"}, func(t *testing.T, c *Cluster, s State) {
			if len(s.Reads) != 14 || strings.Contains(eventsText(s), "failed") {
				t.Fatalf("%d reads:\n%s", len(s.Reads), eventsText(s))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := play(t, 7, tc.name, tc.run)
			if _, ok := c.Settle(time.Minute); !ok {
				t.Fatalf("not settled: %d under, %d over", c.UnderReplicated(), c.OverReplicated())
			}
			s := c.State()
			text := eventsText(s)
			for _, w := range tc.want {
				if !strings.Contains(text, w) {
					t.Fatalf("timeline lacks %q:\n%s", w, text)
				}
			}
			if tc.then != nil {
				tc.then(t, c, s)
			}
			if err := c.AssertInvariants(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
