package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

func safetyErr(c *Cluster) error { return errors.Join(c.safety.errs...) }

// Two copies vanish from disk with no failure to blame, as a wrong trim or GC
// delete would leave them: the floor (2) is broken within one probe.
func TestWatchSafetyFlagsUnexcusedLoss(t *testing.T) {
	c, _, id, reps := oneChunk(t, 1)
	c.WatchSafety(SafetyOpts{})
	for _, n := range reps[:2] {
		_ = c.node(n).Store.Delete(context.Background(), id)
	}
	c.Tick(time.Second)
	if err := safetyErr(c); err == nil || !strings.Contains(err.Error(), "1 intact copies on disk, floor 2") {
		t.Fatalf("err = %v, want a floor violation", err)
	}
	if err := c.AssertInvariants(); err == nil || !strings.Contains(err.Error(), "floor 2") {
		t.Fatalf("AssertInvariants = %v, want the floor violation", err)
	}
}

// Copies lost to wipes and rot lower the floor; once repair is back at RF the
// debt is cleared.
func TestWatchSafetyExcusesWipeAndRot(t *testing.T) {
	c, _, id, reps := oneChunk(t, 1)
	c.WatchSafety(SafetyOpts{})
	c.KillNode(reps[0])
	c.WipeNode(reps[0])
	if got := c.RotNode(reps[1], 1, 0, func(ch iface.ChunkID) bool { return ch == id }); len(got) != 1 {
		t.Fatalf("rotted %v", got)
	}
	c.Tick(time.Second)
	if err := safetyErr(c); err != nil {
		t.Fatalf("a loss to faults was flagged: %v", err)
	}
	c.RestartNode(reps[0])
	// The rotted copy counts to the leader until a scrub pass (10 min) finds it.
	c.Tick(11 * time.Minute)
	if _, ok := c.Settle(3 * time.Minute); !ok {
		t.Fatal("RF not restored")
	}
	c.Tick(5 * time.Second)
	if n := c.intactCopies(id); n != 3 {
		t.Fatalf("%d intact copies after the scrub pass, want 3", n)
	}
	if d := c.safety.debt[id]; d != 0 {
		t.Fatalf("debt %d after RF returned, want 0", d)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

// A copy deleted behind the node's back is a phantom until the next full
// report: within the default grace that is allowed, past a short one it is not.
func TestWatchSafetyFlagsLastingPhantom(t *testing.T) {
	for _, tc := range []struct {
		name  string
		grace time.Duration
		flag  bool
	}{{"short grace", time.Second, true}, {"default grace", 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, id, reps := oneChunk(t, 1)
			c.WatchSafety(SafetyOpts{PhantomGrace: tc.grace})
			_ = c.node(reps[0]).Store.Delete(context.Background(), id)
			if tc.flag {
				c.Tick(3 * time.Second)
			} else {
				c.Tick(75 * time.Second)
			}
			err := safetyErr(c)
			if got := err != nil && strings.Contains(err.Error(), string(reps[0])+"'s copy"); got != tc.flag {
				t.Fatalf("flagged %v, want %v: %v", got, tc.flag, err)
			}
		})
	}
}

// Probes only read: a watched run ends in the same state as an unwatched one.
func TestWatchSafetyKeepsReplay(t *testing.T) {
	run := func(watch bool) string {
		c, _, _, reps := oneChunk(t, 3)
		if watch {
			c.WatchSafety(SafetyOpts{})
		}
		c.KillNode(reps[0])
		c.Tick(40 * time.Second)
		c.RestartNode(reps[0])
		c.Tick(40 * time.Second)
		b, _ := json.Marshal(c.State())
		return fmt.Sprintf("%s %d", b, c.Now())
	}
	if a, b := run(false), run(true); a != b {
		t.Fatal("WatchSafety changed the run")
	}
}
