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
	c := New(seed, DashboardConfig(), io.Discard)
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
		{"kill-leader", 100 * time.Second, []string{"no contact from leader", "starting an election", "election, no leader yet", " leads", "write /demo/during-election.bin", "write /demo/after-return.bin"}, func(t *testing.T, c *Cluster, s State) {
			if strings.Contains(eventsText(s), "failed") {
				t.Fatalf("a scripted call failed:\n%s", eventsText(s))
			}
			var leaders int
			var term uint64
			for _, m := range s.Metas {
				if m.State == "down" || m.CutOff {
					t.Errorf("%s still %s, cut off %v", m.ID, m.State, m.CutOff)
				}
				if m.Leader {
					leaders++
				}
				term = max(term, m.Term)
			}
			if leaders != 1 || term < 3 {
				t.Fatalf("%d leaders, highest term %d; want one leader after a second election: %+v", leaders, term, s.Metas)
			}
			if err := c.AssertMetaAgree(); err != nil {
				t.Fatal(err)
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

// TestScenarioGC plays the gc scenario: the edit sends only the changed
// chunk, the deleted file is restorable until its version expires, and
// then the sweep leaves exactly the referenced chunks on disk.
func TestScenarioGC(t *testing.T) {
	c := play(t, 7, "gc", 60*time.Second)
	s := c.State()
	if len(s.Deleted) != 1 || s.Deleted[0].Path != "/demo/file-7.bin" || s.Deleted[0].ExpiresEpoch != 3 {
		t.Fatalf("deleted at 60 s = %+v, want /demo/file-7.bin expiring at epoch 3", s.Deleted)
	}
	if s.ReferencedBytes <= s.DistinctBytes {
		t.Fatalf("referenced %d, distinct %d: the edit shares a chunk with the retained version", s.ReferencedBytes, s.DistinctBytes)
	}
	for end := c.Now().Add(120 * time.Second); c.Now() < end; {
		c.Tick(50 * time.Millisecond)
	}
	// A lost delete or ack is resent by the next sweep (60 s apart).
	for end := c.Now().Add(120 * time.Second); c.Now() < end && c.State().GC.Deleted < 9; {
		c.Tick(50 * time.Millisecond)
	}
	s = c.State()
	text := eventsText(s)
	for _, w := range []string{"1 of 2 chunks already stored, 1 sent", "delete /demo/file-7.bin", "epoch 3: 2 versions past retention dropped", "deleting 9 unreferenced copies"} {
		if !strings.Contains(text, w) {
			t.Fatalf("timeline lacks %q:\n%s", w, text)
		}
	}
	if len(s.Deleted) != 0 || s.GC.Deleted != 9 || s.ReferencedBytes != s.DistinctBytes {
		t.Fatalf("after GC: deleted %+v, %d copies collected (want 9), referenced %d, distinct %d", s.Deleted, s.GC.Deleted, s.ReferencedBytes, s.DistinctBytes)
	}
	if err := c.AssertCollected(); err != nil {
		t.Fatal(err)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}
