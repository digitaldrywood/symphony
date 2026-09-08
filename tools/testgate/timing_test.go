package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTestTiming(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	event := func(action string, seconds int) testEvent {
		return testEvent{Time: start.Add(time.Duration(seconds) * time.Second), Action: action, Test: "TestFixture"}
	}
	tests := []struct {
		name                           string
		events                         []testEvent
		outcome                        string
		wall, queued, elapsed, fixture float64
		opens                          int
	}{
		{
			name: "parallel queue and fixture work",
			events: []testEvent{
				event("run", 0), event("pause", 1), event("cont", 11),
				{Action: "output", Test: "TestFixture", Output: "    fixture_test.go:1: hub_fixture_open_seconds=2.5\n"},
				{Action: "output", Test: "TestFixture", Output: "    fixture_test.go:1: hub_fixture_open_seconds=0.5\n"},
				{Time: start.Add(15 * time.Second), Action: "pass", Test: "TestFixture", Elapsed: 5},
			},
			outcome: "pass", wall: 15, queued: 10, elapsed: 5, fixture: 3, opens: 2,
		},
		{
			name:    "package timeout retains queued test",
			events:  []testEvent{event("run", 0), event("pause", 1), {Time: start.Add(20 * time.Second), Action: "fail"}},
			outcome: "pause", wall: 20, queued: 19,
		},
		{
			name:    "package timeout retains active test",
			events:  []testEvent{event("run", 0), event("pause", 1), event("cont", 11), {Time: start.Add(20 * time.Second), Action: "fail"}},
			outcome: "cont", wall: 20, queued: 10,
		},
		{
			name:    "sequential failure",
			events:  []testEvent{event("run", 0), {Time: start.Add(3 * time.Second), Action: "fail", Test: "TestFixture", Elapsed: 3}},
			outcome: "fail", wall: 3, elapsed: 3,
		},
		{
			name:    "skip without timestamps",
			events:  []testEvent{{Action: "run", Test: "TestFixture"}, {Action: "skip", Test: "TestFixture"}},
			outcome: "skip",
		},
		{
			name:    "ignore malformed fixture output",
			events:  []testEvent{event("run", 0), {Action: "output", Test: "TestFixture", Output: "hub_fixture_open_seconds=invalid"}, {Action: "output", Test: "TestFixture", Output: "hub_fixture_open_seconds=-1"}, event("pass", 1)},
			outcome: "pass", wall: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := packageResult{Outcome: "running", testTimeouts: make(map[string]bool)}
			for _, e := range tt.events {
				result.apply(e)
			}
			result.finalize()
			result.finalize()
			if len(result.Tests) != 1 {
				t.Fatalf("timings = %v", result.Tests)
			}
			got := result.Tests[0]
			if got.Outcome != tt.outcome || got.Wall != tt.wall || got.Queued != tt.queued || got.Elapsed != tt.elapsed || got.FixtureElapsed != tt.fixture || got.FixtureOpens != tt.opens {
				t.Fatalf("timing = %+v, want outcome=%s wall=%v queued=%v elapsed=%v fixture=%v opens=%d", got, tt.outcome, tt.wall, tt.queued, tt.elapsed, tt.fixture, tt.opens)
			}
		})
	}
}

func TestRaceGateDetectsFixtureRace(t *testing.T) {
	t.Parallel()
	binary := filepath.Join(t.TempDir(), "testgate.exe")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build testgate: %v\n%s", err, output)
	}
	for _, tt := range []struct {
		name    string
		race    bool
		outcome string
	}{
		{name: "ordinary", outcome: "pass"},
		{name: "race detector", race: true, outcome: "fail"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			files := map[string]string{
				"go.mod": "module example.com/fixture\n\ngo 1.26\n",
				"fixture_test.go": `package fixture
import "testing"
func TestFixture(t *testing.T) {
 t.Parallel()
 done := make(chan struct{})
 value := 0
 go func() { value = 1; close(done) }()
 value = 2
 <-done
 t.Logf("value=%d", value)
 t.Log("hub_fixture_open_seconds=1.25")
}
`,
			}
			for name, content := range files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			outputDir := filepath.Join(dir, "evidence")
			args := []string{"-output", outputDir, "-parallel=2", "-timeout=30s"}
			if tt.race {
				args = append(args, "-race")
			}
			args = append(args, "./...")
			command := exec.CommandContext(t.Context(), binary, args...)
			command.Dir = dir
			output, err := command.CombinedOutput()
			if (err != nil) != tt.race {
				t.Fatalf("gate error = %v, race=%v\n%s", err, tt.race, output)
			}
			data, err := os.ReadFile(filepath.Join(outputDir, "summary.json"))
			if err != nil {
				t.Fatal(err)
			}
			var summary gateSummary
			if err := json.Unmarshal(data, &summary); err != nil {
				t.Fatal(err)
			}
			if summary.Race != tt.race || len(summary.Packages) != 1 || summary.Packages[0].Outcome != tt.outcome {
				t.Fatalf("summary = %s", data)
			}
			result := summary.Packages[0]
			if len(result.Tests) != 1 || result.Tests[0].FixtureOpens != 1 || result.Tests[0].FixtureElapsed != 1.25 {
				t.Fatalf("fixture timing = %s", data)
			}
			if tt.race && !strings.Contains(string(output), "WARNING: DATA RACE") {
				t.Fatalf("missing race evidence: %s", output)
			}
		})
	}
}
