package orchestrator

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/lessons"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
)

const reworkTransitionFailureKind = "rework_transition"

type reworkLessonEvidence struct {
	LastCommand   string
	ConflictPaths []string
}

func (o *Orchestrator) captureReworkLesson(issue connector.Issue, at time.Time, reason string, evidence ...reworkLessonEvidence) {
	if o == nil || !o.cfg.Lessons.Enabled || strings.TrimSpace(o.cfg.Lessons.Path) == "" {
		return
	}
	if at.IsZero() {
		if o.now != nil {
			at = o.now()
		} else {
			at = time.Now()
		}
	}
	entry := reworkLessonEntry(o.workflowMetricsProjectID(), issue, at.UTC(), reason, evidence...)
	if len(entry.Evidence) == 0 {
		return
	}
	appended, err := lessons.AppendUnique(o.cfg.Lessons.Path, entry, lessons.AppendOptions{
		Date:       at.UTC(),
		MaxEntries: o.cfg.Lessons.MaxEntries,
	})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("rework lesson capture failed", "project_id", o.workflowMetricsProjectID(), "issue_id", issue.ID, "identifier", issue.Identifier, "reason", reason, "error", err)
		}
		return
	}
	if appended && o.logger != nil {
		o.logger.Info("rework lesson captured", "project_id", o.workflowMetricsProjectID(), "issue_id", issue.ID, "identifier", issue.Identifier, "failure_kind", entry.FailureKind)
	}
}

func reworkLessonEntry(projectID string, issue connector.Issue, at time.Time, reason string, evidence ...reworkLessonEvidence) lessons.Entry {
	checks := reworkFailedChecks(issue.PullRequest)
	names := make([]string, 0, len(checks))
	for _, check := range checks {
		names = append(names, check.Name)
	}
	kind := reworkFailureKind(reason, issue.PullRequest, names)
	entry := lessons.Entry{
		IssueNumber: reworkIssueNumber(issue),
		IssueRef:    strings.TrimSpace(issue.Identifier),
		PullRequest: reworkPullRequestRef(issue),
		Title:       issue.Title,
		FailureKind: kind,
		Symptom:     reworkLessonSymptom(issue, reason),
		CaptureKey:  reworkLessonCaptureKey(projectID, issue, at),
	}
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "stranded_active_recovery", "backend_capacity_pause":
		return entry
	}
	var signals reworkLessonEvidence
	if len(evidence) > 0 {
		signals = evidence[0]
	}
	switch kind {
	case "session_duration_exceeded":
		if command := strings.TrimSpace(signals.LastCommand); command != "" {
			entry.Evidence = append(entry.Evidence, "last command: "+command)
		}
	case "merge_conflict":
		if paths := uniqueStrings(signals.ConflictPaths); len(paths) > 0 {
			entry.Evidence = append(entry.Evidence, "conflicting paths: "+strings.Join(paths, ", "))
		}
	default:
		if len(names) > 0 {
			entry.Evidence = append(entry.Evidence, "failed checks: "+strings.Join(names, ", "))
		}
		for _, check := range checks {
			if detail := strings.TrimSpace(check.FailureDetail); detail != "" {
				entry.Evidence = append(entry.Evidence, check.Name+": "+detail)
				break
			}
		}
		if review := reworkReviewSummary(issue.PullRequest); review != "" {
			entry.Evidence = append(entry.Evidence, "review: "+review)
		}
	}
	for index, signal := range entry.Evidence {
		entry.Evidence[index] = runtimeoutput.Truncate(strings.Join(strings.Fields(signal), " "), 1024).Value
	}
	return entry
}

func reworkFailedChecks(pr *connector.PullRequest) []connector.PullRequestCheck {
	if pr == nil {
		return nil
	}
	checks := append([]connector.PullRequestCheck{}, pr.Checks...)
	checks = append(checks, pr.RequiredCheckFailures...)
	checks = append(checks, pr.TransientFailedChecks...)
	checks = append(checks, pr.SlowChecks...)
	failures := []connector.PullRequestCheck{}
	seen := map[string]int{}
	for _, check := range checks {
		check.Name = strings.TrimSpace(check.Name)
		if check.Name == "" {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(check.Conclusion)) {
		case "failure", "failed", "error", "timed_out", "startup_failure", "action_required":
		default:
			continue
		}
		key := strings.ToLower(check.Name)
		if index, ok := seen[key]; ok {
			if failures[index].FailureDetail == "" {
				failures[index].FailureDetail = check.FailureDetail
			}
			continue
		}
		seen[key] = len(failures)
		failures = append(failures, check)
	}
	return failures
}

func reworkIssueNumber(issue connector.Issue) string {
	if issue.Number <= 0 {
		return ""
	}
	return strconv.Itoa(issue.Number)
}

func reworkFailureKind(reason string, pullRequest *connector.PullRequest, failedChecks []string) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	switch reason {
	case "session_duration_exceeded":
		return reason
	case mergeFallbackRequiresReworkReason, "merge_conflict":
		return "merge_conflict"
	case string(AutoPromoteReasonCINotGreen):
		return "ci_failure"
	case mergeWorkerRequiredChecksMissingReason, mergeWorkerFastPathNotReadyReason:
		return "ci_signal_missing"
	case string(AutoPromoteReasonP1Findings), string(AutoPromoteReasonUnresolvedReviewThreads), "plan_review_decision":
		return "changes_requested"
	case string(AutoPromoteReasonValidatorRework), string(AutoPromoteReasonValidatorScoreBelowThreshold), string(AutoPromoteReasonValidatorBlockedSeverity):
		return "validator_rework"
	case string(AutoPromoteReasonArtifactStatusRework):
		return "artifact_rework"
	case string(AutoPromoteReasonMergeConflicts):
		return "merge_conflict"
	case string(AutoPromoteReasonWorkpadStatusInvalid):
		return "invalid_workpad_status"
	}
	if len(failedChecks) > 0 {
		return "ci_failure"
	}
	if pullRequest != nil {
		reviewState := strings.ToUpper(strings.TrimSpace(pullRequest.CodexReviewState))
		if reviewState == "CHANGES_REQUESTED" || reviewState == "REQUESTED_CHANGES" || reviewState == "P1" {
			return "changes_requested"
		}
	}
	if reason == "" || reason == "tracker_state_observed" {
		return reworkTransitionFailureKind
	}
	return strings.ReplaceAll(reason, " ", "_")
}

func reworkLessonSymptom(issue connector.Issue, reason string) string {
	transition := reworkIssueRef(issue) + " was observed entering Rework"
	if sourceState := displayStateName(issue.State); sourceState != "" {
		transition = fmt.Sprintf("%s entered Rework from %s", reworkIssueRef(issue), sourceState)
	}
	parts := []string{transition}
	if reason = strings.TrimSpace(reason); reason != "" {
		parts = append(parts, "reason: "+reason)
	}
	return strings.Join(parts, "; ")
}

func reworkReviewSummary(pullRequest *connector.PullRequest) string {
	if pullRequest == nil {
		return ""
	}
	for _, thread := range pullRequest.UnresolvedReviewThreads {
		if body := strings.TrimSpace(thread.Body); body != "" {
			if location := pullRequestReviewThreadLocation(thread); location != "" {
				return location + ": " + body
			}
			return body
		}
	}
	for _, finding := range pullRequest.CodexReviewFindings {
		if body := strings.TrimSpace(finding.Body); body != "" {
			parts := []string{}
			if state := strings.TrimSpace(pullRequest.CodexReviewState); state != "" {
				parts = append(parts, state)
			}
			if path := strings.TrimSpace(finding.Path); path != "" {
				parts = append(parts, path)
			}
			return strings.Join(append(parts, body), ": ")
		}
	}
	return ""
}

func reworkPullRequestRef(issue connector.Issue) string {
	if issue.PullRequest != nil {
		if url := strings.TrimSpace(issue.PullRequest.URL); url != "" {
			return url
		}
		if issue.PullRequest.Number > 0 {
			return fmt.Sprintf("PR #%d", issue.PullRequest.Number)
		}
	}
	if issue.PRNumber != nil && *issue.PRNumber > 0 {
		return fmt.Sprintf("PR #%d", *issue.PRNumber)
	}
	return ""
}

func reworkIssueRef(issue connector.Issue) string {
	if identifier := strings.TrimSpace(issue.Identifier); identifier != "" {
		return identifier
	}
	if issueID := strings.TrimSpace(issue.ID); issueID != "" {
		return issueID
	}
	return "issue"
}

func reworkLessonCaptureKey(projectID string, issue connector.Issue, at time.Time) string {
	return strings.Join([]string{
		"rework",
		strings.TrimSpace(projectID),
		reworkIssueRef(issue),
		at.UTC().Format(time.RFC3339Nano),
	}, "|")
}
