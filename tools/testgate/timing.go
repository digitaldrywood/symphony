package main

import (
	"strconv"
	"strings"
	"time"
)

type testTiming struct {
	Test           string    `json:"test"`
	Outcome        string    `json:"outcome"`
	Elapsed        float64   `json:"elapsed_seconds"`
	Wall           float64   `json:"wall_seconds"`
	Queued         float64   `json:"queued_seconds"`
	FixtureOpens   int       `json:"fixture_opens,omitempty"`
	FixtureElapsed float64   `json:"fixture_open_seconds,omitempty"`
	Started        time.Time `json:"started,omitempty"`
	LastProgress   time.Time `json:"last_progress,omitempty"`

	paused time.Time
}

func (r *packageResult) recordTiming(event testEvent) {
	if event.Test == "" {
		if event.Action == "pass" || event.Action == "fail" || event.Action == "skip" {
			for _, timing := range r.timings {
				if timing.Outcome == "run" || timing.Outcome == "pause" || timing.Outcome == "cont" {
					timing.finish(event.Time)
				}
			}
		}
		return
	}
	if r.timings == nil {
		r.timings = make(map[string]*testTiming)
	}
	timing := r.timings[event.Test]
	if timing == nil {
		timing = &testTiming{Test: event.Test}
		r.timings[event.Test] = timing
	}
	switch event.Action {
	case "run":
		timing.Started = event.Time
	case "pause":
		timing.paused = event.Time
	case "cont":
		timing.resume(event.Time)
	case "pass", "fail", "skip":
		timing.Elapsed = event.Elapsed
		timing.finish(event.Time)
	case "output":
		_, duration, ok := strings.Cut(event.Output, "hub_fixture_open_seconds=")
		if ok {
			seconds, err := strconv.ParseFloat(strings.TrimSpace(duration), 64)
			if err == nil && seconds >= 0 {
				timing.FixtureOpens++
				timing.FixtureElapsed += seconds
			}
		}
		return
	default:
		return
	}
	timing.Outcome = event.Action
	timing.LastProgress = event.Time
}

func (t *testTiming) resume(at time.Time) {
	if !t.paused.IsZero() && !at.IsZero() {
		t.Queued += at.Sub(t.paused).Seconds()
		t.paused = time.Time{}
	}
}

func (t *testTiming) finish(at time.Time) {
	t.resume(at)
	if !t.Started.IsZero() && !at.IsZero() {
		t.Wall = at.Sub(t.Started).Seconds()
	}
}
