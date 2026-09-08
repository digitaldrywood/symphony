package orchestrator

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func (o *Orchestrator) retryTransientPullRequestChecks(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	now time.Time,
	reason string,
) bool {
	rerunner, ok := o.connector.(connector.PullRequestCheckRerunner)
	if !ok || state == nil || issue.PullRequest == nil {
		return false
	}
	checks := transientPullRequestChecks(issue.PullRequest)
	if len(checks) == 0 {
		return false
	}
	limit := 0
	if o.cfg.AutoPromote.Gate.TransientCIRetryLimit != nil {
		limit = *o.cfg.AutoPromote.Gate.TransientCIRetryLimit
	}
	if limit <= 0 {
		return false
	}

	retryable := make([]connector.PullRequestCheck, 0, len(checks))
	for _, check := range checks {
		key := transientCheckRetryKey(issue, check)
		if key == "" {
			continue
		}
		if state.TransientCheckRetries[key].Attempts >= limit {
			continue
		}
		retryable = append(retryable, check)
	}
	if len(retryable) == 0 {
		return false
	}
	if err := rerunner.RerunPullRequestChecks(ctx, issue, retryable); err != nil {
		if connector.IsRetryable(err) {
			if o.logger != nil {
				o.logger.Info("transient ci retry deferred", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
			}
			return true
		}
		if o.logger != nil {
			o.logger.Warn("transient ci retry failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		return false
	}

	for _, check := range retryable {
		key := transientCheckRetryKey(issue, check)
		entry := state.TransientCheckRetries[key]
		entry.IssueID = issue.ID
		entry.HeadSHA = transientCheckRetryHeadSHA(issue)
		entry.CheckName = strings.TrimSpace(check.Name)
		entry.CheckID = check.ID
		entry.WorkflowRunID = check.WorkflowRunID
		entry.Attempts++
		entry.RetriedAt = now
		state.TransientCheckRetries[key] = entry
	}
	body := transientCIRetryComment(issue, retryable, limit, reason)
	if err := o.connector.CreateComment(ctx, issue.ID, body); err != nil && o.logger != nil {
		o.logger.Warn("transient ci retry comment failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "transient_ci_retry",
		Message: "reran transient CI checks for " + issueLabel(issue) + ": " + transientCheckNames(retryable),
	})
	o.recordWorkflowReviewAction(ctx, issue, "ci_rerun", reason, now, workflowLaneMetadata{})
	return true
}

func transientPullRequestChecks(pullRequest *connector.PullRequest) []connector.PullRequestCheck {
	if pullRequest == nil || !staleMergingCIRed(pullRequest.CIStatus) {
		return nil
	}
	checks := make([]connector.PullRequestCheck, 0, len(pullRequest.TransientFailedChecks))
	for _, check := range pullRequest.TransientFailedChecks {
		if strings.TrimSpace(check.Name) == "" && check.ID <= 0 && check.WorkflowRunID <= 0 {
			continue
		}
		checks = append(checks, check)
	}
	return checks
}

func transientCheckRetryKey(issue connector.Issue, check connector.PullRequestCheck) string {
	issueID := strings.TrimSpace(issue.ID)
	headSHA := transientCheckRetryHeadSHA(issue)
	if issueID == "" || headSHA == "" {
		return ""
	}
	var identity string
	switch {
	case check.WorkflowRunID > 0:
		identity = "run:" + strconv.FormatInt(check.WorkflowRunID, 10)
	case check.ID > 0:
		identity = "check:" + strconv.FormatInt(check.ID, 10)
	default:
		identity = "name:" + strings.ToLower(strings.TrimSpace(check.Name))
	}
	if identity == "name:" {
		return ""
	}
	return issueID + ":" + headSHA + ":" + identity
}

func transientCheckRetryHeadSHA(issue connector.Issue) string {
	if issue.PullRequest != nil {
		if headSHA := strings.TrimSpace(issue.PullRequest.HeadSHA); headSHA != "" {
			return headSHA
		}
	}
	return strings.TrimSpace(issue.BranchName)
}

func transientCIRetryComment(issue connector.Issue, checks []connector.PullRequestCheck, limit int, reason string) string {
	var b strings.Builder
	b.WriteString("Transient CI failure detected; reran failed check")
	if len(checks) != 1 {
		b.WriteString("s")
	}
	b.WriteString(" before treating CI as a hard failure.")
	if strings.TrimSpace(reason) != "" {
		b.WriteString("\n\nReason: ")
		b.WriteString(strings.TrimSpace(reason))
	}
	b.WriteString("\n\nRerun limit: ")
	b.WriteString(strconv.Itoa(limit))
	b.WriteString("\nChecks:")
	for _, check := range checks {
		b.WriteString("\n- ")
		name := strings.TrimSpace(check.Name)
		if name == "" {
			name = "check run"
		}
		b.WriteString(name)
		if check.WorkflowRunID > 0 {
			b.WriteString(" (workflow run " + strconv.FormatInt(check.WorkflowRunID, 10) + ")")
		} else if check.ID > 0 {
			b.WriteString(" (check run " + strconv.FormatInt(check.ID, 10) + ")")
		}
		if url := strings.TrimSpace(check.DetailsURL); url != "" {
			b.WriteString(" ")
			b.WriteString(url)
		}
	}
	return b.String()
}

func transientCheckNames(checks []connector.PullRequestCheck) string {
	names := make([]string, 0, len(checks))
	for _, check := range checks {
		if name := strings.TrimSpace(check.Name); name != "" {
			names = append(names, name)
		}
	}
	return strings.Join(names, ", ")
}
