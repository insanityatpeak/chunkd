package sim

import (
	"slices"
	"testing"
	"time"
)

func TestClockFiresInDeadlineThenInsertionOrder(t *testing.T) {
	c := NewClock()
	var got []string
	rec := func(s string) func() { return func() { got = append(got, s) } }
	c.AfterFunc(20*time.Millisecond, rec("c"))
	c.AfterFunc(10*time.Millisecond, rec("a"))
	c.AfterFunc(10*time.Millisecond, rec("b"))
	c.AfterFunc(30*time.Millisecond, rec("late"))

	c.Advance(25 * time.Millisecond)

	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	if c.Now() != 25_000_000 {
		t.Fatalf("Now = %d, want 25ms", c.Now())
	}
	if c.Pending() != 1 {
		t.Fatalf("Pending = %d, want 1", c.Pending())
	}
}

func TestClockCallbackSeesItsDeadline(t *testing.T) {
	c := NewClock()
	var at time.Duration
	c.AfterFunc(7*time.Millisecond, func() { at = time.Duration(c.Now()) })
	c.Advance(time.Second)
	if at != 7*time.Millisecond {
		t.Fatalf("callback Now = %v, want 7ms", at)
	}
}

func TestClockEventsScheduledDuringAdvance(t *testing.T) {
	tests := []struct {
		name    string
		delay   time.Duration
		advance time.Duration
		fired   bool
	}{
		{"within window", 5 * time.Millisecond, 20 * time.Millisecond, true},
		{"zero delay", 0, 10 * time.Millisecond, true},
		{"past window", 15 * time.Millisecond, 20 * time.Millisecond, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClock()
			fired := false
			c.AfterFunc(10*time.Millisecond, func() {
				c.AfterFunc(tt.delay, func() { fired = true })
			})
			c.Advance(tt.advance)
			if fired != tt.fired {
				t.Fatalf("fired = %v, want %v", fired, tt.fired)
			}
		})
	}
}

func TestTimerStop(t *testing.T) {
	c := NewClock()
	fired := false
	tm := c.AfterFunc(time.Millisecond, func() { fired = true })
	if !tm.Stop() {
		t.Fatal("first Stop = false, want true")
	}
	if tm.Stop() {
		t.Fatal("second Stop = true, want false")
	}
	c.Advance(time.Second)
	if fired {
		t.Fatal("stopped timer fired")
	}

	done := c.AfterFunc(time.Millisecond, func() {})
	c.Advance(time.Second)
	if done.Stop() {
		t.Fatal("Stop after firing = true, want false")
	}
}

func TestStepJumpsToNextEvent(t *testing.T) {
	c := NewClock()
	c.AfterFunc(time.Hour, func() {})
	if !c.Step() {
		t.Fatal("Step = false with a pending event")
	}
	if c.Now() != 3_600_000_000_000 {
		t.Fatalf("Now = %d, want 1h", c.Now())
	}
	if c.Step() {
		t.Fatal("Step = true with no pending events")
	}
}
