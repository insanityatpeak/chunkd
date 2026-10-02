package cluster

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
)

var update = flag.Bool("update", false, "rewrite web/e2e/golden files")

// goldens are the shareable scenarios and how much simulated time each
// golden timeline covers. The Playwright replay test waits for the page to
// pass it, then compares.
var goldens = []struct {
	name  string
	until time.Duration
}{{"kill-node", 120 * time.Second}, {"corrupt-chunk", 120 * time.Second}, {"gc", 200 * time.Second}, {"kill-leader", 120 * time.Second}}

// Golden is a scenario's timeline as the dashboard must show it.
type Golden struct {
	Seed     uint64         `json:"seed"`
	Scenario string         `json:"scenario"`
	UntilMs  int64          `json:"untilMs"`
	Events   []client.Event `json:"events"`
	Reads    []client.Event `json:"reads"`
}

// TestScenarioGolden runs the dashboard's shareable scenarios exactly as the
// browser worker does (start, scenario, then 50 ms ticks) and checks the
// timeline against web/e2e/golden. The browser test replays the same URLs;
// if the two ever differ, a shared link no longer replays. Regenerate with
// go test ./internal/sim/cluster -run TestScenarioGolden -update.
func TestScenarioGolden(t *testing.T) {
	for _, gd := range goldens {
		name := gd.name
		t.Run(name, func(t *testing.T) {
			c := play(t, 7, name, gd.until)
			s := c.State()
			until := int64(gd.until / time.Millisecond)
			keep := func(es []client.Event) []client.Event {
				return slices.DeleteFunc(slices.Clone(es), func(e client.Event) bool { return e.AtMs > until })
			}
			g := Golden{Seed: 7, Scenario: name, UntilMs: until, Events: keep(s.Events), Reads: keep(s.Reads)}
			got, _ := json.MarshalIndent(g, "", "  ")
			path := filepath.Join("..", "..", "..", "web", "e2e", "golden", name+".json")
			if *update {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(got, '\n'), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (generate with -update)", err)
			}
			var w Golden
			if err := json.Unmarshal(want, &w); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(w.Events, g.Events) || !slices.Equal(w.Reads, g.Reads) {
				t.Fatalf("%s: timeline differs from %s; a shared link would not replay what the page shows. Regenerate with -update if the change is intended.", name, path)
			}
		})
	}
}
