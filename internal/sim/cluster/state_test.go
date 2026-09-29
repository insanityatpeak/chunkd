package cluster

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
)

// TestDashboardTimeline is the dashboard's "kill node-3" script: the state
// shows the detector's transitions, files going under-replicated, copies in
// flight, RF restored, and the trims after the node returns.
func TestDashboardTimeline(t *testing.T) {
	c := New(5, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	seq := c.State().EventSeq
	var timeline []client.Event
	sawCopies, sawUnder := false, false
	step := func(d time.Duration) {
		for end := c.Now().Add(d); c.Now() < end; {
			c.Tick(500 * time.Millisecond)
			s := c.StateSince(seq)
			timeline = append(timeline, s.Events...)
			seq = s.EventSeq
			sawCopies = sawCopies || len(s.Copies) > 0
			for _, f := range s.Files {
				sawUnder = sawUnder || f.UnderReplicated > 0
			}
		}
	}

	if err := c.ScriptKillNode("node-3", 90*time.Second); err != nil {
		t.Fatal(err)
	}
	step(150 * time.Second)

	s := c.State()
	if s.Health.UnderReplicated != 0 || s.Health.OverReplicated != 0 || len(s.Copies) != 0 {
		t.Fatalf("not settled: %+v, %d copies", s.Health, len(s.Copies))
	}
	for _, f := range s.Files {
		if f.MinLive != 3 || f.UnderReplicated != 0 {
			t.Errorf("%s: min live %d, %d under-replicated", f.Path, f.MinLive, f.UnderReplicated)
		}
	}
	if !sawUnder || !sawCopies {
		t.Errorf("never saw an under-replicated file (%v) or a copy in flight (%v)", sawUnder, sawCopies)
	}
	// The timeline must tell the story in order.
	want := []string{"node-3 alive → suspect", "node-3 suspect → dead", " started: chunk", " completed after", "node-3 dead → suspect (restarted)", "node-3 suspect → alive", " done: chunk"}
	i := 0
	for _, e := range timeline {
		if i < len(want) && strings.Contains(e.Node+" "+e.Text, want[i]) {
			i++
		}
	}
	if i < len(want) {
		var lines []string
		for _, e := range timeline {
			lines = append(lines, fmt.Sprintf("%6d %s %s", e.AtMs, e.Node, e.Text))
		}
		t.Fatalf("timeline stops matching at %q:\n%s", want[i], strings.Join(lines, "\n"))
	}
	if !slices.IsSortedFunc(timeline, func(a, b client.Event) int { return int(a.Seq) - int(b.Seq) }) {
		t.Error("events out of order or repeated")
	}
	if got := c.StateSince(s.EventSeq).Events; len(got) != 0 {
		t.Errorf("%d events after the latest seq", len(got))
	}
}
