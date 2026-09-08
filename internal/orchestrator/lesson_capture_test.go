package orchestrator

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/lessons"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
)

func TestReworkLessonRequiresEvidence(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"tracker_state_observed", "stranded_active_recovery", "backend_capacity_pause", "ci_not_green", "session_duration_exceeded", "merge_conflicts"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "lessons.md")
			o := &Orchestrator{cfg: Config{Lessons: LessonCaptureConfig{Enabled: true, Path: path}}}
			o.captureReworkLesson(connector.Issue{ID: "one"}, time.Now(), reason)
			entries, err := lessons.ReadAll(path)
			if err != nil || len(entries) != 0 {
				t.Fatalf("evidence-free capture = %v, %v; want no entries", entries, err)
			}
		})
	}
}

func TestReworkLessonOmitsBoilerplate(t *testing.T) {
	t.Parallel()
	entry := reworkLessonEntry("project", connector.Issue{ID: "one"}, time.Now(), "ci_not_green")
	if strings.TrimSpace(entry.Hypothesis) != "" || strings.TrimSpace(entry.Hint) != "" {
		t.Fatalf("capture invents hypothesis or hint: %#v", entry)
	}
}

func TestReworkLessonConcreteEvidence(t *testing.T) {
	t.Parallel()
	checks := &connector.PullRequest{
		Checks: []connector.PullRequestCheck{
			{Name: "test", Conclusion: "failure", FailureDetail: "worker_test.go: assertion: want released slot"},
			{Name: "passing", Conclusion: "success", FailureDetail: "must not appear"},
			{Name: "neutral", Conclusion: "neutral", FailureDetail: "must not appear"},
			{Name: "missing", Conclusion: "missing", FailureDetail: "must not appear"},
		},
		RequiredCheckFailures: []connector.PullRequestCheck{{Name: "test", Conclusion: "failure"}, {Name: "lint", Conclusion: "failure"}},
	}
	review := &connector.PullRequest{CodexReviewState: "COMMENTED", UnresolvedReviewThreads: []connector.PullRequestReviewThread{
		{Path: "worker.go", Line: 42, Body: "Release the slot after cancellation."},
		{Path: "later.go", Body: "later thread must not appear"},
	}}
	for _, tt := range []struct {
		name, reason, kind string
		pr                 *connector.PullRequest
		evidence           reworkLessonEvidence
		want               []string
	}{
		{name: "CI assertions", reason: "ci_not_green", kind: "ci_failure", pr: checks, want: []string{"failed checks: test, lint", "worker_test.go: assertion: want released slot"}},
		{name: "commented transition", reason: "tracker_state_observed", kind: "rework_transition", pr: review, want: []string{"worker.go:42", "Release the slot after cancellation."}},
		{name: "review request", reason: string(AutoPromoteReasonUnresolvedReviewThreads), kind: "changes_requested", pr: review, want: []string{"worker.go:42", "Release the slot"}},
		{name: "duration ignores unrelated CI", reason: "session_duration_exceeded", kind: "session_duration_exceeded", pr: checks, evidence: reworkLessonEvidence{LastCommand: "go test ./internal/runner -race"}, want: []string{"last command: go test ./internal/runner -race"}},
		{name: "conflicts", reason: "merge_conflicts", kind: "merge_conflict", evidence: reworkLessonEvidence{ConflictPaths: []string{"worker.go", "go.mod", "worker.go", " "}}, want: []string{"conflicting paths: worker.go, go.mod"}},
		{name: "merge fallback", reason: mergeFallbackRequiresReworkReason, kind: "merge_conflict", evidence: reworkLessonEvidence{ConflictPaths: []string{"worker.go"}}, want: []string{"conflicting paths: worker.go"}},
		{name: "validator with evidence", reason: string(AutoPromoteReasonValidatorRework), kind: "validator_rework", pr: review, want: []string{"Release the slot"}},
		{name: "artifact with evidence", reason: string(AutoPromoteReasonArtifactStatusRework), kind: "artifact_rework", pr: review, want: []string{"Release the slot"}},
		{name: "workpad with evidence", reason: string(AutoPromoteReasonWorkpadStatusInvalid), kind: "invalid_workpad_status", pr: review, want: []string{"Release the slot"}},
		{name: "missing signal with review evidence", reason: mergeWorkerRequiredChecksMissingReason, kind: "ci_signal_missing", pr: review, want: []string{"Release the slot"}},
		{name: "operator with review evidence", reason: "operator_rework", kind: "operator_rework", pr: review, want: []string{"Release the slot"}},
		{name: "neutral check alone", reason: "tracker_state_observed", pr: &connector.PullRequest{Checks: []connector.PullRequestCheck{{Name: "optional", Conclusion: "neutral"}}}},
		{name: "state alone", reason: "tracker_state_observed", pr: &connector.PullRequest{CodexReviewState: "COMMENTED"}},
		{name: "thread location alone", reason: "tracker_state_observed", pr: &connector.PullRequest{UnresolvedReviewThreads: []connector.PullRequestReviewThread{{Path: "worker.go", Line: 42}}}},
		{name: "recovery ignores stale evidence", reason: "stranded_active_recovery", pr: checks},
		{name: "capacity ignores stale evidence", reason: "backend_capacity_pause", pr: review},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "lessons.md")
			o := &Orchestrator{cfg: Config{Lessons: LessonCaptureConfig{Enabled: true, Path: path}}}
			issue := connector.Issue{ID: "one", Identifier: "repo#1", PullRequest: tt.pr}
			at := time.Now()
			o.captureReworkLesson(issue, at, tt.reason, tt.evidence)
			o.captureReworkLesson(issue, at, tt.reason, tt.evidence)
			entries, err := lessons.ReadAll(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(tt.want) == 0 {
				if len(entries) != 0 {
					t.Fatalf("unexpected capture: %v", entries)
				}
				return
			}
			if len(entries) != 1 {
				t.Fatalf("entries=%d, want one deduplicated capture", len(entries))
			}
			for _, want := range append(tt.want, "**Failure kind:** "+tt.kind) {
				if !strings.Contains(entries[0], want) {
					t.Errorf("missing %q in %s", want, entries[0])
				}
			}
			for _, unwanted := range []string{"Hypothesis", "Hint for next time", "must not appear"} {
				if strings.Contains(entries[0], unwanted) {
					t.Errorf("unexpected %q in %s", unwanted, entries[0])
				}
			}
		})
	}
}

func TestReworkLessonProjectIsolation(t *testing.T) {
	t.Parallel()
	at := time.Now()
	issue := connector.Issue{ID: "same-id", PullRequest: &connector.PullRequest{Checks: []connector.PullRequestCheck{{Name: "test", Conclusion: "failure"}}}}
	paths := []string{filepath.Join(t.TempDir(), "lessons.md"), filepath.Join(t.TempDir(), "lessons.md")}
	for index, path := range paths {
		o := &Orchestrator{cfg: Config{Project: scheduler.ProjectCandidate{ID: fmt.Sprintf("project-%d", index)}, Lessons: LessonCaptureConfig{Enabled: true, Path: path}}}
		o.captureReworkLesson(issue, at, "ci_not_green")
		entries, err := lessons.ReadAll(path)
		if err != nil || len(entries) != 1 || !strings.Contains(entries[0], fmt.Sprintf("rework|project-%d|", index)) {
			t.Fatalf("project capture = %v, %v", entries, err)
		}
	}
}

func TestSessionBrakeLessonUsesLastCommand(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"", "go test ./internal/runner -race"} {
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "lessons.md")
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress", "Rework"}, Lessons: LessonCaptureConfig{Enabled: true, Path: path}})
			o := &Orchestrator{cfg: cfg, connector: &workflowMetricsConnector{}}
			state := newState(cfg)
			running := Running{Issue: connector.Issue{ID: "one", State: "In Progress"}, LastCommand: command}
			completion := runpkg.Completion{IssueID: "one", CompletedAt: time.Now(), Err: &runpkg.SessionBrakeError{Reason: runpkg.SessionBrakeReasonDuration, Resumable: true}}
			if !o.handleSessionBrake(t.Context(), &state, completion, running) {
				t.Fatal("session brake was not handled")
			}
			entries, err := lessons.ReadAll(path)
			if err != nil {
				t.Fatal(err)
			}
			if command == "" {
				if len(entries) != 0 {
					t.Fatalf("capture without command = %v", entries)
				}
			} else if len(entries) != 1 || !strings.Contains(entries[0], command) {
				t.Fatalf("capture missing last command: %v", entries)
			}
		})
	}
}

func TestMergeReworkLessonUsesPrecheckPaths(t *testing.T) {
	t.Parallel()
	for _, paths := range [][]string{nil, {"README.md", "internal/worker.go"}} {
		t.Run(fmt.Sprint(paths), func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "lessons.md")
			cfg := normalizeConfig(Config{Lessons: LessonCaptureConfig{Enabled: true, Path: path}})
			issue := connector.Issue{ID: "one", State: "Merging"}
			o := &Orchestrator{cfg: cfg, connector: &workflowMetricsConnector{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			state := newState(cfg)
			completion := runpkg.Completion{IssueID: "one", CompletedAt: time.Now(), Result: runpkg.RunResult{MergePrecheck: &runpkg.MergePrecheck{Status: "conflict", ConflictPaths: paths}}}
			o.reworkMergeWorkerResult(t.Context(), &state, completion, Running{Issue: issue}, issue, mergeFallbackRequiresReworkReason, nil, "")
			entries, err := lessons.ReadAll(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(paths) == 0 {
				if len(entries) != 0 {
					t.Fatalf("capture without paths = %v", entries)
				}
			} else if len(entries) != 1 || !strings.Contains(entries[0], "conflicting paths: README.md, internal/worker.go") {
				t.Fatalf("capture missing conflict paths: %v", entries)
			}
		})
	}
}
