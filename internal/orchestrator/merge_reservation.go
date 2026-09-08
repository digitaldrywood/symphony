package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/store"
)

const mergeReservationMetadataKey = "merge_reservation"

type mergeReservation struct {
	IssueID        string    `json:"issue_id"`
	Repository     string    `json:"repository"`
	HeadSHA        string    `json:"head_sha"`
	BaseSHA        string    `json:"base_sha"`
	StartedAt      time.Time `json:"started_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	ReleasedReason string    `json:"released_reason,omitempty"`
	RefreshHeadSHA string    `json:"refresh_head_sha,omitempty"`
}

func reserveMergeCandidate(state *State, issue connector.Issue, now time.Time) mergeReservation {
	repository := mergeWorkerRepositoryKey(issue)
	if !mergeWorkerIssue(issue) || repository == "" || issue.PullRequest == nil || issue.PullRequest.HeadSHA == "" {
		return mergeReservation{}
	}
	if state.mergeReservations == nil {
		state.mergeReservations = map[string]mergeReservation{}
	}
	if reservation := state.mergeReservations[repository]; reservation.IssueID == issue.ID {
		return reservation
	}
	reservation := mergeReservation{
		IssueID: issue.ID, Repository: repository,
		HeadSHA: strings.TrimSpace(issue.PullRequest.HeadSHA), BaseSHA: strings.TrimSpace(issue.PullRequest.BaseSHA),
		StartedAt: now.UTC(), ExpiresAt: now.Add(mergeWorkerCurrentHeadCIWaitTimeout).UTC(),
	}
	state.mergeReservations[repository] = reservation
	return reservation
}

func mergeReservationBlocks(state *State, issue connector.Issue, now time.Time) (mergeReservation, bool) {
	if state == nil || !mergeWorkerIssue(issue) {
		return mergeReservation{}, false
	}
	for _, running := range state.Running {
		if mergeWorkerIssue(running.Issue) && running.Issue.ID != issue.ID && mergeWorkerRepositoryKey(running.Issue) == mergeWorkerRepositoryKey(issue) {
			return mergeReservation{IssueID: running.Issue.ID, Repository: mergeWorkerRepositoryKey(issue)}, true
		}
	}
	reservation := state.mergeReservations[mergeWorkerRepositoryKey(issue)]
	return reservation, reservation.IssueID != "" && reservation.IssueID != issue.ID &&
		reservation.ReleasedReason == "" && now.Before(reservation.ExpiresAt)
}

func mergeFairnessBlocks(state *State, stickyID string, issue connector.Issue, now time.Time) bool {
	if stickyID == "" || stickyID == issue.ID || !mergeWorkerIssue(issue) {
		return false
	}
	if reservation := state.mergeReservations[mergeWorkerRepositoryKey(issue)]; reservation.IssueID != "" && reservation.ReleasedReason == "" && now.Before(reservation.ExpiresAt) {
		return false
	}
	if running, ok := state.Running[stickyID]; ok {
		return mergeWorkerRepositoryKey(running.Issue) == mergeWorkerRepositoryKey(issue)
	}
	if retry, ok := state.Retry[stickyID]; ok {
		return mergeWorkerRepositoryKey(retry.Issue) == mergeWorkerRepositoryKey(issue)
	}
	return false
}

func reconcileMergeReservations(state *State, issues []connector.Issue, cfg Config, now time.Time) []mergeReservation {
	current := make(map[string]connector.Issue, len(issues))
	for _, issue := range issues {
		current[issue.ID] = issue
	}
	var released []mergeReservation
	for repository, reservation := range state.mergeReservations {
		if reservation.ReleasedReason != "" {
			continue
		}
		reason := ""
		issue, present := current[reservation.IssueID]
		if !present {
			if completed, ok := state.Completed[reservation.IssueID]; ok {
				issue, present = completed.Issue, true
			}
		}
		_, running := state.Running[reservation.IssueID]
		switch {
		case !now.Before(reservation.ExpiresAt):
			reason = "expired"
		case present && workspaceIssueTerminal(issue, cfg.TerminalStates):
			reason = "completed"
		case present && (!mergeWorkerIssue(issue) || issue.Closed):
			reason = "withdrawn"
		case present && issue.PullRequest != nil && !pullRequestHydrationBlocksProgress(issue.PullRequest):
			pr := issue.PullRequest
			switch {
			case normalizePullRequestState(pr.State) != "open" || pr.Draft:
				reason = "withdrawn"
			case mergeWorkerRepositoryKey(issue) != repository:
				reason = "repository_changed"
			case strings.TrimSpace(pr.HeadSHA) != reservation.HeadSHA && !running:
				reason = "head_changed"
			case mergeWorkerCIFailed(pr):
				reason = "required_checks_failed"
			case strings.EqualFold(pr.MergeableState, "dirty"):
				reason = "conflict"
			case pr.MergeQueueEntry != nil:
				reason = "native_queue"
			default:
				if _, revoked := mergeApprovalLabelRevoked(issue, cfg); revoked {
					reason = "approval_withdrawn"
				} else if _, revoked := mergeCITriggerLabelRevoked(issue, cfg); revoked {
					reason = "ci_trigger_withdrawn"
				}
			}
		}
		if reason != "" {
			reservation.ReleasedReason = reason
			state.mergeReservations[repository] = reservation
			released = append(released, reservation)
			if retry := state.Retry[issue.ID]; reason == "required_checks_failed" && retry.Wait.Kind == retryWaitCurrentHeadCI && !running {
				delete(state.Retry, issue.ID)
			}
		}
	}
	return released
}

func mergeWorkerCIFailed(pr *connector.PullRequest) bool {
	if pr == nil || pullRequestHydrationBlocksProgress(pr) {
		return false
	}
	if staleMergingCIRed(pr.CIStatus) {
		return true
	}
	for _, check := range pr.RequiredCheckFailures {
		if !autoPromoteCheckPending(check) && autoPromoteCheckFailed(check) {
			return true
		}
	}
	return false
}

func (o *Orchestrator) reconcileMergeReservations(state *State, issues []connector.Issue, now time.Time) {
	for _, reservation := range reconcileMergeReservations(state, issues, o.cfg, now) {
		if o.logger != nil {
			issue := connector.Issue{ID: reservation.IssueID}
			for _, current := range issues {
				if current.ID == reservation.IssueID {
					issue = current
					break
				}
			}
			o.logger.Info("merge_reservation_released", mergeWorkerLogAttrs(issue,
				"repository", reservation.Repository, "reason", reservation.ReleasedReason,
				"reserved_head_sha", reservation.HeadSHA, "reserved_base_sha", reservation.BaseSHA,
				"expires_at", reservation.ExpiresAt, "prior_validation_invalidated", reservation.ReleasedReason == "head_changed")...)
		}
	}
}

func (o *Orchestrator) recordMergeReservationWait(state *State, issue connector.Issue, now time.Time) mergeReservation {
	reservation := reserveMergeCandidate(state, issue, now)
	if reservation.IssueID == "" {
		return reservation
	}
	if o.logger != nil {
		o.logger.Info("merge_reservation_wait", mergeWorkerLogAttrs(issue,
			"reserved_head_sha", reservation.HeadSHA, "reserved_base_sha", reservation.BaseSHA,
			"expires_at", reservation.ExpiresAt,
			"prior_validation_invalidated", reservation.HeadSHA != issue.PullRequest.HeadSHA)...)
	}
	reservation.HeadSHA = strings.TrimSpace(issue.PullRequest.HeadSHA)
	reservation.BaseSHA = strings.TrimSpace(issue.PullRequest.BaseSHA)
	reservation.RefreshHeadSHA = ""
	state.mergeReservations[reservation.Repository] = reservation
	return reservation
}

func (o *Orchestrator) recoverMergeReservations(state *State, attempts []store.WorkAttempt, issues []connector.Issue, now time.Time) {
	current := make(map[string]connector.Issue, len(issues))
	for _, issue := range issues {
		current[issue.ID] = issue
	}
	latest := latestStoreTerminalAttemptsByIssue(attempts)
	for _, attempt := range latest {
		var metadata struct {
			Reservation mergeReservation `json:"merge_reservation"`
		}
		if json.Unmarshal([]byte(attempt.WorkerMetadataJSON), &metadata) != nil {
			continue
		}
		reservation := metadata.Reservation
		if reservation.IssueID != attempt.IssueID || reservation.Repository == "" || reservation.HeadSHA == "" ||
			reservation.StartedAt.IsZero() || reservation.StartedAt.After(now) || reservation.ReleasedReason != "" || !now.Before(reservation.ExpiresAt) ||
			reservation.ExpiresAt.After(reservation.StartedAt.Add(mergeWorkerCurrentHeadCIWaitTimeout)) ||
			attempt.Phase != "waiting" || attempt.TerminalState != store.WorkAttemptTerminalSuccess {
			continue
		}
		issue := current[reservation.IssueID]
		if issue.ID != reservation.IssueID || mergeWorkerRepositoryKey(issue) != reservation.Repository || issue.PullRequest == nil || pullRequestHydrationBlocksProgress(issue.PullRequest) {
			continue
		}
		validation := State{mergeReservations: map[string]mergeReservation{reservation.Repository: reservation}}
		o.reconcileMergeReservations(&validation, []connector.Issue{issue}, now)
		if validation.mergeReservations[reservation.Repository].ReleasedReason != "" {
			continue
		}
		if state.mergeReservations == nil {
			state.mergeReservations = map[string]mergeReservation{}
		}
		if existing := state.mergeReservations[reservation.Repository]; existing.IssueID != "" && existing.ReleasedReason == "" && now.Before(existing.ExpiresAt) && !reservation.StartedAt.Before(existing.StartedAt) {
			continue
		}
		state.mergeReservations[reservation.Repository] = reservation
		state.Retry[issue.ID] = Retry{Issue: cloneIssue(issue), Attempt: attempt.AttemptNumber,
			DueAt: now, WorkerHost: attempt.WorkerHost, Error: mergeWorkerCurrentHeadCIWaitReason(issue),
			Wait: RetryWait{Kind: retryWaitCurrentHeadCI, StartedAt: reservation.StartedAt}}
		state.MergeTimings[issue.ID] = MergeTiming{CIWaitStartedAt: reservation.StartedAt, CIWaitHeadSHA: reservation.HeadSHA}
		if o.logger != nil {
			o.logger.Info("merge_reservation_restored", mergeWorkerLogAttrs(issue, "expires_at", reservation.ExpiresAt)...)
		}
	}
}

func (o *Orchestrator) restoreDurableMergeReservations(ctx context.Context, state *State, issues []connector.Issue, now time.Time) {
	if o.workAttempts == nil {
		return
	}
	if state.mergeRecoveryChecked == nil {
		state.mergeRecoveryChecked = map[string]bool{}
	}
	for _, issue := range issues {
		if !mergeWorkerIssue(issue) || state.mergeRecoveryChecked[issue.ID] || issue.PullRequest == nil || pullRequestHydrationBlocksProgress(issue.PullRequest) {
			continue
		}
		if reservation := state.mergeReservations[mergeWorkerRepositoryKey(issue)]; reservation.IssueID == issue.ID {
			state.mergeRecoveryChecked[issue.ID] = true
			continue
		}
		attempts, err := o.workAttempts.ListRecentTerminalWorkAttempts(ctx, store.WorkAttemptHistoryQuery{
			ProjectID: o.cfg.Project.ID, IssueID: issue.ID, Limit: 1,
		})
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("merge_reservation_recovery_failed", "issue_id", issue.ID, "error", err)
			}
			continue
		}
		state.mergeRecoveryChecked[issue.ID] = true
		o.recoverMergeReservations(state, attempts, []connector.Issue{issue}, now)
	}
}
