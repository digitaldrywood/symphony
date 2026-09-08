package orchestrator

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/securityaudit"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workpad"
)

const (
	implementProgressMetadataKey       = "completion_progress"
	implementProgressOutcomeNoProgress = "no_progress"
	implementProgressReasonNonDiff     = "verifiable_non_diff_progress"
	implementProgressReasonMixed       = "workspace_diff_and_verifiable_non_diff_progress"
	implementOperationalCompletion     = "operational_completion"
	implementDependencyDeferralReason  = "dependency_deferral"
	implementMergedCompletionReason    = "merged_pull_request_completion"
	noProgressLimitReason              = "no_progress_limit"
	strandedUnpushedWorkReason         = "stranded_unpushed_work"
	workpadBlockedUnactionedReason     = "workpad_blocked_unactioned"
	workpadBlockedUnactionedLimit      = 2
	workspaceHeadUnavailableReason     = "workspace_head_unavailable_for_unpushed_check"
	pullRequestHeadUnavailableReason   = "pull_request_head_unavailable_for_unpushed_check"
	unpushedRemoteTruthUnavailable     = "unpushed_remote_truth_unavailable_without_pull_request"
)

type implementCompletionProgressDecision struct {
	Issue                  connector.Issue
	Outcome                store.WorkAttemptTerminalState
	Reason                 string
	CurrentSignature       autoPromoteReworkSignature
	PreviousSignature      autoPromoteReworkSignature
	PreviousSignatureFound bool
	FailedChecksAdded      []string
	FailedChecksRemoved    []string
	WorkspaceDiffStats     DiffStats
	ConsecutiveNoProgress  int
	WorkpadStatus          string
	SecurityAudit          securityaudit.Evaluation
	HumanAction            string
	TrackerState           string
	ConsecutiveHumanAction int
	NoProgressLimit        int
	BlockReason            string
	Block                  bool
	Warning                string
	DependencyDeferral     bool
	DependencyBlockers     []implementDependencyBlocker
	RejectedBlockerRefs    []string
	ProgressKinds          []string
	CompletionKind         string
}

type implementProgressRecord struct {
	Outcome                string                            `json:"outcome"`
	Reason                 string                            `json:"reason"`
	CurrentSignature       implementProgressSignatureRecord  `json:"current_signature"`
	PreviousSignature      *implementProgressSignatureRecord `json:"previous_signature,omitempty"`
	PreviousHeadSHA        string                            `json:"previous_head_sha,omitempty"`
	CurrentHeadSHA         string                            `json:"current_head_sha,omitempty"`
	FailedChecksAdded      []string                          `json:"failed_checks_added,omitempty"`
	FailedChecksRemoved    []string                          `json:"failed_checks_removed,omitempty"`
	WorkspaceDiffStats     implementProgressDiffStats        `json:"workspace_diffstat"`
	ConsecutiveNoProgress  int                               `json:"consecutive_no_progress,omitempty"`
	WorkpadStatus          string                            `json:"workpad_status,omitempty"`
	HumanAction            string                            `json:"human_action,omitempty"`
	TrackerState           string                            `json:"tracker_state,omitempty"`
	ConsecutiveHumanAction int                               `json:"consecutive_human_action,omitempty"`
	NoProgressLimit        int                               `json:"no_progress_limit,omitempty"`
	BlockReason            string                            `json:"block_reason,omitempty"`
	Warning                string                            `json:"warning,omitempty"`
	DependencyDeferral     bool                              `json:"dependency_deferral,omitempty"`
	DependencyBlockers     []implementDependencyBlocker      `json:"dependency_blockers,omitempty"`
	RejectedBlockerRefs    []string                          `json:"rejected_blocker_refs,omitempty"`
	ProgressKinds          []string                          `json:"progress_kinds,omitempty"`
	CompletionKind         string                            `json:"completion_kind,omitempty"`
	DispatchLoopStart      dispatchLoopStartRecord           `json:"-"`
}

type implementProgressArtifactSnapshot struct {
	TrackerState       string
	NativeBlockers     []string
	WorkpadRead        bool
	WorkpadStatus      string
	WorkpadReason      string
	WorkpadFields      map[string]string
	WorkpadReceiptHash string
	CompletionKind     string
}

type implementDependencyBlocker struct {
	ID         string `json:"id,omitempty"`
	Identifier string `json:"identifier"`
	State      string `json:"state,omitempty"`
}

type implementProgressSignatureRecord struct {
	PRNumber     int64    `json:"pr_number,omitempty"`
	HeadSHA      string   `json:"head_sha,omitempty"`
	FailedChecks []string `json:"failed_checks,omitempty"`
}

type implementProgressDiffStats struct {
	FilesChanged                   int      `json:"files_changed"`
	AddedLines                     int      `json:"added_lines"`
	RemovedLines                   int      `json:"removed_lines"`
	UnpushedCommits                int      `json:"unpushed_commits,omitempty"`
	UnpushedCommitRefs             []string `json:"unpushed_commit_refs,omitempty"`
	TrackedPaths                   []string `json:"tracked_paths,omitempty"`
	UntrackedPaths                 []string `json:"untracked_paths,omitempty"`
	CommitsNotInPullRequest        []string `json:"commits_not_in_pull_request,omitempty"`
	PullRequestComparisonAvailable bool     `json:"pull_request_comparison_available,omitempty"`
	RecoveryStateExpected          bool     `json:"recovery_state_expected,omitempty"`
	RecoveryStateAvailable         bool     `json:"recovery_state_available,omitempty"`
	HeadSHA                        string   `json:"head_sha,omitempty"`
	Fingerprint                    string   `json:"fingerprint,omitempty"`
	Status                         string   `json:"status,omitempty"`
}

func (o *Orchestrator) evaluateImplementCompletionProgress(
	ctx context.Context,
	running Running,
	finalState string,
	pullRequestUpdated bool,
) implementCompletionProgressDecision {
	cfg := normalizeAutoPromoteConfig(o.cfg.AutoPromote)
	decision := implementCompletionProgressDecision{
		Issue:              cloneIssue(running.Issue),
		Outcome:            store.WorkAttemptTerminalSuccess,
		Reason:             "success_without_progress_check",
		WorkspaceDiffStats: running.DiffStats,
		NoProgressLimit:    cfg.NoProgressLimit,
	}
	if strings.TrimSpace(finalState) != FinalStateCompleted {
		decision.Reason = "final_state_not_completed"
		return decision
	}
	if !implementProgressLinkedPullRequest(running.Issue) {
		issue, workpadCurrent := o.refreshImplementCompletionIssue(ctx, running.Issue)
		decision.Issue = issue
		decision.TrackerState = strings.TrimSpace(issue.State)
		decision.ProgressKinds = implementProgressArtifactKinds(
			running.DispatchProgress,
			implementProgressArtifactSnapshotFromIssue(issue, workpadCurrent),
		)
		if workpadCurrent {
			decision.WorkpadStatus, decision.HumanAction = implementProgressBlockedHumanAction(issue)
			blockers, rejected, deferred := o.evaluateImplementDependencyDeferral(ctx, issue)
			decision.DependencyBlockers = blockers
			decision.RejectedBlockerRefs = rejected
			if deferred {
				decision.Reason = implementDependencyDeferralReason
				decision.DependencyDeferral = true
				decision.WorkpadStatus = workpad.StatusBlocked
				return decision
			}
		}
		if pullRequestUpdated {
			decision.Reason = "pull_request_created_or_updated"
			decision.ProgressKinds = []string{"pull_request"}
			return decision
		}
		operationalCompletionCandidate := false
		if workpadCurrent && running.DispatchProgress.CompletionKind == workpad.CompletionOperational {
			_, operationalCompletionCandidate = operationalCompletionFromIssue(issue)
			if operationalCompletionCandidate && implementProgressOperationalWorkspaceClean(running.DiffStats) {
				decision.WorkpadStatus = workpad.StatusComplete
				decision.Reason = implementOperationalCompletion
				decision.ProgressKinds = []string{"operational_completion"}
				decision.CompletionKind = workpad.CompletionOperational
				return decision
			}
		}
		if workpadCurrent && implementProgressMergedCompletionCandidate(issue, running.DiffStats) &&
			implementProgressLinkedPullRequest(issue) && issue.PullRequest == nil {
			hydrator, ok := o.connector.(connector.PullRequestHydrator)
			if !ok {
				decision.Warning = "pull request hydrator unavailable"
				o.warnImplementProgressHydration(issue, decision.Warning, nil)
			} else {
				hydrated, err := hydrator.HydratePullRequest(ctx, issue)
				if err != nil {
					decision.Warning = err.Error()
					o.warnImplementProgressHydration(issue, "pull request hydration failed", err)
				} else {
					issue = hydrated
					decision.Issue = issue
				}
			}
		}
		if workpadCurrent && implementProgressMergedCompletion(issue, running.DiffStats) {
			decision.WorkpadStatus = workpad.StatusComplete
			decision.CurrentSignature = autoPromoteReworkSignatureFromIssue(issue, staleMergedPullRequestSummaryFromIssue(issue))
			decision.Reason = implementMergedCompletionReason
			return decision
		}
		attempts, err := o.recentImplementCompletionAttempts(ctx, issue, running)
		if err != nil {
			decision.Reason = "attempt_history_lookup_failed"
			decision.Warning = err.Error()
			if o.logger != nil {
				o.logger.Warn(
					"implement worker progress history lookup failed",
					"issue_id", running.Issue.ID,
					"identifier", running.Issue.Identifier,
					"error", err,
				)
			}
			return decision
		}
		if decision.HumanAction != "" {
			decision.ConsecutiveHumanAction = 1 + consecutiveImplementBlockedHumanActionAttempts(
				attempts,
				decision.HumanAction,
				decision.TrackerState,
			)
			if decision.ConsecutiveHumanAction >= workpadBlockedUnactionedLimit {
				decision.Outcome = store.WorkAttemptTerminalNoProgress
				decision.Reason = workpadBlockedUnactionedReason
				decision.BlockReason = workpadBlockedUnactionedReason
				decision.Block = true
				return decision
			}
		}
		if operationalCompletionCandidate && (running.DiffStats.UnpushedCommits > 0 || running.DiffStats.CommitsAhead > 0) {
			decision.Outcome = store.WorkAttemptTerminalNoProgress
			decision.Reason = strandedUnpushedWorkReason
			decision.ConsecutiveNoProgress = 1 + consecutiveImplementStrandedWorkAttempts(attempts, autoPromoteReworkSignature{})
			decision.Block = decision.NoProgressLimit > 0 && decision.ConsecutiveNoProgress >= decision.NoProgressLimit
			if decision.Block {
				decision.BlockReason = strandedUnpushedWorkReason
			}
			return decision
		}
		if stranded, deferReason := implementProgressUnpushedClassification(running.DiffStats, nil); deferReason != "" {
			decision.Reason = deferReason
			return decision
		} else if stranded {
			decision.Outcome = store.WorkAttemptTerminalNoProgress
			decision.Reason = strandedUnpushedWorkReason
			decision.ConsecutiveNoProgress = 1 + consecutiveImplementStrandedWorkAttempts(attempts, autoPromoteReworkSignature{})
			decision.Block = decision.NoProgressLimit > 0 && decision.ConsecutiveNoProgress >= decision.NoProgressLimit
			if decision.Block {
				decision.BlockReason = strandedUnpushedWorkReason
			}
			return decision
		}
		if !diffStatsPresent(running.DiffStats) {
			decision.Reason = "workspace_diffstat_unavailable_without_pull_request"
			return decision
		}
		if !implementProgressDiffStatsClean(running.DiffStats) {
			if strings.TrimSpace(running.DiffStats.Fingerprint) == "" {
				decision.Reason = "workspace_diff_fingerprint_unavailable_without_pull_request"
				return decision
			}
			matchingAttempts := consecutiveImplementSameNoPRDiffAttempts(
				attempts,
				running.DiffStats,
				decision.WorkpadStatus,
				decision.HumanAction,
			)
			if matchingAttempts == 0 {
				decision.ProgressKinds = append([]string{"workspace_diff"}, decision.ProgressKinds...)
				decision.Reason = "workspace_diff_present_without_pull_request"
				if len(decision.ProgressKinds) > 1 {
					decision.Reason = implementProgressReasonMixed
				}
				return decision
			}
			if len(decision.ProgressKinds) > 0 {
				decision.Reason = implementProgressReasonNonDiff
				return decision
			}
			decision.Outcome = store.WorkAttemptTerminalNoProgress
			decision.Reason = "unchanged_workspace_diff_without_pull_request"
			decision.ConsecutiveNoProgress = 1 + matchingAttempts
			decision.Block = decision.NoProgressLimit > 0 && decision.ConsecutiveNoProgress >= decision.NoProgressLimit
			if decision.Block {
				decision.BlockReason = noProgressLimitReason
			}
			return decision
		}
		if len(decision.ProgressKinds) > 0 {
			decision.Reason = implementProgressReasonNonDiff
			return decision
		}
		decision.Outcome = store.WorkAttemptTerminalNoProgress
		decision.Reason = "completed_clean_diff_without_pull_request"
		decision.ConsecutiveNoProgress = 1 + consecutiveImplementNoProgressAttempts(attempts, autoPromoteReworkSignature{})
		decision.Block = decision.NoProgressLimit > 0 && decision.ConsecutiveNoProgress >= decision.NoProgressLimit
		if decision.Block {
			decision.BlockReason = noProgressLimitReason
		}
		return decision
	}
	hydrator, ok := o.connector.(connector.PullRequestHydrator)
	if !ok {
		decision.Reason = "pull_request_hydrator_unavailable"
		decision.Warning = "pull request hydrator unavailable"
		o.warnImplementProgressHydration(running.Issue, decision.Warning, nil)
		return decision
	}
	issue, err := hydrator.HydratePullRequest(ctx, running.Issue)
	if err != nil {
		decision.Reason = "pull_request_hydration_failed"
		decision.Warning = err.Error()
		o.warnImplementProgressHydration(running.Issue, "pull request hydration failed", err)
		return decision
	}
	decision.Issue = issue
	if reason := implementProgressHydrationUnavailableReason(issue.PullRequest); reason != "" {
		decision.Reason = "pull_request_hydration_unavailable"
		decision.Warning = reason
		o.warnImplementProgressHydration(issue, reason, nil)
		return decision
	}
	var workpadCurrent bool
	if pullRequestMerged(issue.PullRequest) {
		var refreshed connector.Issue
		refreshed, workpadCurrent = o.refreshImplementCompletionIssue(ctx, issue)
		decision.Issue = refreshed
		decision.TrackerState = strings.TrimSpace(refreshed.State)
		if workpadCurrent && implementProgressMergedCompletion(refreshed, running.DiffStats) {
			decision.WorkpadStatus = workpad.StatusComplete
			decision.CurrentSignature = autoPromoteReworkSignatureFromIssue(refreshed, staleMergedPullRequestSummaryFromIssue(refreshed))
			decision.Reason = implementMergedCompletionReason
			return decision
		}
		issue = refreshed
	} else {
		var refreshed connector.Issue
		refreshed, workpadCurrent = o.refreshImplementCompletionIssue(ctx, issue)
		issue = refreshed
	}
	decision.Issue = issue
	decision.TrackerState = strings.TrimSpace(issue.State)
	decision.WorkspaceDiffStats = implementProgressReconcilePullRequestEvidence(running.DiffStats, issue.PullRequest)
	if workpadCurrent {
		decision.WorkpadStatus = implementProgressArtifactSnapshotFromIssue(issue, true).WorkpadStatus
		_, decision.HumanAction = implementProgressBlockedHumanAction(issue)
	}
	decision.SecurityAudit = o.securityAuditEvaluation(ctx, issue)
	if workpadCurrent && !pullRequestMerged(issue.PullRequest) {
		blockers, rejected, deferred := o.evaluateImplementDependencyDeferral(ctx, issue)
		decision.DependencyBlockers = blockers
		decision.RejectedBlockerRefs = rejected
		if deferred {
			decision.Reason = implementDependencyDeferralReason
			decision.DependencyDeferral = true
			decision.WorkpadStatus = workpad.StatusBlocked
			return decision
		}
	}
	decision.ProgressKinds = implementProgressArtifactKinds(
		running.DispatchProgress,
		implementProgressArtifactSnapshotFromIssue(issue, workpadCurrent),
	)
	signature := autoPromoteReworkSignatureFromIssue(issue, AutoPromoteSummaryFromIssue(issue))
	decision.CurrentSignature = signature
	if !implementProgressSignatureUsable(signature) {
		decision.Reason = "pull_request_signature_incomplete"
		return decision
	}

	attempts, err := o.recentImplementCompletionAttempts(ctx, issue, running)
	if err != nil {
		decision.Reason = "attempt_history_lookup_failed"
		decision.Warning = err.Error()
		if o.logger != nil {
			o.logger.Warn(
				"implement worker progress history lookup failed",
				"issue_id", issue.ID,
				"identifier", issue.Identifier,
				"error", err,
			)
		}
		return decision
	}
	previous, ok := latestImplementProgressSignature(attempts)
	if decision.WorkpadStatus == workpad.StatusBlocked {
		decision.Outcome = store.WorkAttemptTerminalNoProgress
		decision.Reason = "workpad_blocked"
		decision.ConsecutiveNoProgress = 1 + consecutiveImplementNoProgressAttempts(attempts, signature)
		decision.Block = decision.NoProgressLimit > 0 && decision.ConsecutiveNoProgress >= decision.NoProgressLimit
		if decision.Block {
			decision.BlockReason = noProgressLimitReason
		}
		if decision.HumanAction != "" {
			decision.ConsecutiveHumanAction = 1 + consecutiveImplementBlockedHumanActionAttempts(attempts, decision.HumanAction, decision.TrackerState)
			decision.Block = decision.ConsecutiveHumanAction >= workpadBlockedUnactionedLimit
			if decision.Block {
				decision.Reason = workpadBlockedUnactionedReason
				decision.BlockReason = workpadBlockedUnactionedReason
			}
		}
		return decision
	}
	if ok {
		decision.PreviousSignature = previous
		decision.PreviousSignatureFound = true
		decision.FailedChecksAdded, decision.FailedChecksRemoved = implementProgressFailedCheckDelta(previous.FailedChecks, signature.FailedChecks)
	}
	if stranded, deferReason := implementProgressUnpushedClassification(decision.WorkspaceDiffStats, issue.PullRequest); deferReason != "" {
		decision.Reason = deferReason
		return decision
	} else if stranded {
		decision.Outcome = store.WorkAttemptTerminalNoProgress
		decision.Reason = strandedUnpushedWorkReason
		decision.ConsecutiveNoProgress = 1 + consecutiveImplementStrandedWorkAttempts(attempts, signature)
		decision.Block = decision.NoProgressLimit > 0 && decision.ConsecutiveNoProgress >= decision.NoProgressLimit
		if decision.Block {
			decision.BlockReason = strandedUnpushedWorkReason
		}
		return decision
	}
	if !ok {
		decision.Reason = "first_completed_attempt"
		return decision
	}
	if !implementProgressSignatureEqual(previous, signature) {
		decision.Reason = "signature_changed"
		decision.ProgressKinds = append([]string{"pull_request"}, decision.ProgressKinds...)
		return decision
	}
	if !implementProgressDiffStatsClean(decision.WorkspaceDiffStats) {
		if !diffStatsPresent(decision.WorkspaceDiffStats) {
			decision.Reason = "workspace_diffstat_unavailable"
			return decision
		}
		decision.ProgressKinds = append([]string{"workspace_diff"}, decision.ProgressKinds...)
		decision.Reason = "workspace_diff_present"
		if len(decision.ProgressKinds) > 1 {
			decision.Reason = implementProgressReasonMixed
		}
		return decision
	}
	if len(decision.ProgressKinds) > 0 {
		decision.Reason = implementProgressReasonNonDiff
		return decision
	}

	decision.Outcome = store.WorkAttemptTerminalNoProgress
	decision.Reason = "unchanged_signature_clean_diff"
	decision.ConsecutiveNoProgress = 1 + consecutiveImplementNoProgressAttempts(attempts, signature)
	decision.Block = decision.NoProgressLimit > 0 && decision.ConsecutiveNoProgress >= decision.NoProgressLimit
	if decision.Block {
		decision.BlockReason = noProgressLimitReason
	}
	return decision
}

func (o *Orchestrator) implementProgressDispatchArtifactSnapshot(ctx context.Context, issue connector.Issue) implementProgressArtifactSnapshot {
	snapshot := implementProgressArtifactSnapshotFromIssue(issue, false)
	if o == nil || o.connector == nil {
		return snapshot
	}
	reader, ok := o.connector.(connector.IssueCommentReader)
	if !ok {
		return snapshot
	}
	comments, err := reader.FetchIssueComments(ctx, issue)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("implement progress dispatch artifact snapshot failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		return snapshot
	}
	issue.Comments = comments
	return implementProgressArtifactSnapshotFromIssue(issue, true)
}

func implementProgressArtifactSnapshotFromIssue(issue connector.Issue, workpadRead bool) implementProgressArtifactSnapshot {
	snapshot := implementProgressArtifactSnapshot{
		TrackerState:   normalizeState(issue.State),
		NativeBlockers: implementProgressBlockedRefIdentifiers(issue.BlockedBy),
		WorkpadRead:    workpadRead,
	}
	if kind, found, err := workpad.CompletionAuthorizationFromIssueBody(issue.Description); err == nil && found {
		snapshot.CompletionKind = kind
	}
	if !workpadRead {
		return snapshot
	}
	signal, ok := autoPromoteIssueWorkpadSignal(issue)
	if !ok || signal == nil || signal.Invalid != nil || signal.Source != workpad.SourceStructured {
		return snapshot
	}
	snapshot.WorkpadStatus = strings.TrimSpace(signal.Status)
	snapshot.WorkpadReason = strings.TrimSpace(signal.ReasonCode)
	snapshot.WorkpadFields = cloneStringMap(signal.Fields)
	if snapshot.WorkpadStatus == workpad.StatusComplete {
		snapshot.WorkpadReceiptHash = artifactCompletionReceiptHash(issue.Comments)
	}
	return snapshot
}

func implementProgressArtifactKinds(previous, current implementProgressArtifactSnapshot) []string {
	kinds := make([]string, 0, 5)
	if previous.TrackerState != "" && current.TrackerState != "" && previous.TrackerState != current.TrackerState {
		kinds = append(kinds, "tracker_state_transition")
	}
	if implementProgressHasNewString(current.NativeBlockers, previous.NativeBlockers) {
		kinds = append(kinds, "linked_blocker")
	}
	if !previous.WorkpadRead || !current.WorkpadRead {
		return kinds
	}
	if current.WorkpadReason != "" && current.WorkpadReason != previous.WorkpadReason {
		kinds = append(kinds, "workpad_predicate")
	}
	if strings.TrimSpace(current.WorkpadFields[workpad.FieldCompletionKind]) != workpad.CompletionOperational &&
		current.WorkpadStatus == workpad.StatusComplete && current.WorkpadReceiptHash != "" &&
		current.WorkpadReceiptHash != previous.WorkpadReceiptHash {
		kinds = append(kinds, "artifact_receipt")
	}
	if implementProgressFieldsAdvanced(previous.WorkpadFields, current.WorkpadFields) {
		kinds = append(kinds, "audit_artifact")
	}
	return kinds
}

func implementProgressBlockedRefIdentifiers(refs []connector.BlockedRef) []string {
	identifiers := make([]string, 0, len(refs))
	for _, ref := range refs {
		identifier := strings.TrimSpace(ref.Identifier)
		if identifier == "" {
			identifier = strings.TrimSpace(ref.ID)
		}
		if identifier != "" {
			identifiers = append(identifiers, identifier)
		}
	}
	slices.Sort(identifiers)
	return slices.Compact(identifiers)
}

func implementProgressHasNewString(current, previous []string) bool {
	for _, value := range current {
		if !slices.Contains(previous, value) {
			return true
		}
	}
	return false
}

func implementProgressFieldsAdvanced(previous, current map[string]string) bool {
	for name, value := range current {
		if name == workpad.FieldCompletionKind || name == workpad.FieldCompletionEvidence {
			continue
		}
		if previousValue, ok := previous[name]; !ok || strings.TrimSpace(previousValue) != strings.TrimSpace(value) {
			return true
		}
	}
	return false
}

func (o *Orchestrator) recentImplementCompletionAttempts(
	ctx context.Context,
	issue connector.Issue,
	running Running,
) ([]store.WorkAttempt, error) {
	if o == nil || o.workAttempts == nil {
		return nil, nil
	}
	limit := normalizeAutoPromoteConfig(o.cfg.AutoPromote).NoProgressLimit + 1
	if limit < 20 {
		limit = 20
	}
	return o.workAttempts.ListRecentTerminalWorkAttempts(ctx, store.WorkAttemptHistoryQuery{
		ProjectID:  strings.TrimSpace(o.cfg.Project.ID),
		IssueID:    strings.TrimSpace(issue.ID),
		Identifier: strings.TrimSpace(issue.Identifier),
		IssueURL:   strings.TrimSpace(issue.URL),
		WorkerType: workAttemptWorkerType(issue, running.Mode),
		Limit:      limit,
	})
}

func (o *Orchestrator) refreshImplementCompletionIssue(ctx context.Context, issue connector.Issue) (connector.Issue, bool) {
	if o == nil || o.connector == nil || strings.TrimSpace(issue.ID) == "" {
		return cloneIssue(issue), false
	}
	issues, err := o.connector.FetchIssueStatesByIDs(ctx, []string{issue.ID})
	if err != nil {
		o.warnImplementProgressRefresh(issue, "fetch tracker state failed", err)
		return cloneIssue(issue), false
	}
	refreshed := connector.Issue{}
	hydratedPullRequest := issue.PullRequest
	hydratedPRNumber := issue.PRNumber
	hydratedPRRepository := issue.PRRepository
	for _, candidate := range issues {
		if strings.TrimSpace(candidate.ID) == strings.TrimSpace(issue.ID) {
			refreshed = mergeIssueTrackerFields(issue, candidate)
			break
		}
	}
	if strings.TrimSpace(refreshed.ID) == "" {
		o.warnImplementProgressRefresh(issue, "tracker issue was not returned", nil)
		return cloneIssue(issue), false
	}
	if hydratedPullRequest != nil {
		refreshed.PullRequest = hydratedPullRequest
		refreshed.PRNumber = hydratedPRNumber
		refreshed.PRRepository = hydratedPRRepository
	}
	reader, ok := o.connector.(connector.IssueCommentReader)
	if !ok {
		o.warnImplementProgressRefresh(refreshed, "issue comment reader unavailable", nil)
		return refreshed, false
	}
	comments, err := reader.FetchIssueComments(ctx, refreshed)
	if err != nil {
		o.warnImplementProgressRefresh(refreshed, "fetch workpad comments failed", err)
		return refreshed, false
	}
	refreshed.Comments = comments
	refreshed.WorkpadSignal = nil
	if signal, ok := autoPromoteIssueWorkpadSignal(refreshed); ok {
		refreshed.WorkpadSignal = signal
	}
	return refreshed, true
}

func implementProgressBlockedHumanAction(issue connector.Issue) (string, string) {
	signal, ok := autoPromoteIssueWorkpadSignal(issue)
	if !ok || signal == nil || signal.Invalid != nil || signal.Source != workpad.SourceStructured {
		return "", ""
	}
	if strings.TrimSpace(signal.Status) != workpad.StatusBlocked {
		return "", ""
	}
	humanAction := strings.TrimSpace(signal.HumanAction)
	if humanAction == "" {
		for _, blocker := range signal.Blockers {
			if blocker.Owner == workpad.BlockerOwnerHuman {
				humanAction = firstNonBlank(blocker.Reason, blocker.Ref, blocker.Identifier)
				if humanAction != "" {
					break
				}
			}
		}
	}
	if humanAction == "" {
		return "", ""
	}
	return workpad.StatusBlocked, humanAction
}

func implementProgressMergedCompletion(issue connector.Issue, diffStats DiffStats) bool {
	if !implementProgressMergedCompletionCandidate(issue, diffStats) {
		return false
	}
	pullRequest := issue.PullRequest
	if workAttemptPRNumber(issue) == nil || !pullRequestMerged(pullRequest) ||
		pullRequestHydrationBlocksProgress(pullRequest) || strings.TrimSpace(pullRequest.HeadSHA) == "" {
		return false
	}
	if pullRequest.CheckRunCount+pullRequest.StatusContextCount == 0 ||
		len(pullRequest.RunningChecks) > 0 || len(pullRequest.UnstartedChecks) > 0 ||
		len(pullRequest.RequiredCheckFailures) > 0 {
		return false
	}
	return mergeWorkerCIGreen(pullRequest.CIStatus)
}

func implementProgressMergedCompletionCandidate(issue connector.Issue, diffStats DiffStats) bool {
	if diffStats.UnpushedCommits > 0 || !implementProgressDiffStatsClean(diffStats) {
		return false
	}
	signal, ok := autoPromoteIssueWorkpadSignal(issue)
	if !ok || signal == nil || signal.Invalid != nil || signal.Source != workpad.SourceStructured ||
		strings.TrimSpace(signal.Status) != workpad.StatusComplete ||
		strings.TrimSpace(signal.HumanAction) != "" || len(signal.Blockers) > 0 {
		return false
	}
	return true
}

func latestImplementProgressSignature(attempts []store.WorkAttempt) (autoPromoteReworkSignature, bool) {
	for _, attempt := range attempts {
		record, ok := implementProgressRecordFromAttempt(attempt)
		if !ok {
			continue
		}
		signature := record.CurrentSignature.signature()
		if implementProgressSignatureUsable(signature) {
			return signature, true
		}
	}
	return autoPromoteReworkSignature{}, false
}

func consecutiveImplementNoProgressAttempts(attempts []store.WorkAttempt, current autoPromoteReworkSignature) int {
	count := 0
	for _, attempt := range attempts {
		record, ok := implementProgressRecordFromAttempt(attempt)
		if !ok || !implementProgressRecordMatchesNoProgress(record, current) {
			return count
		}
		count++
	}
	return count
}

func consecutiveImplementSameNoPRDiffAttempts(
	attempts []store.WorkAttempt,
	current DiffStats,
	workpadStatus string,
	humanAction string,
) int {
	count := 0
	for _, attempt := range attempts {
		record, ok := implementProgressRecordFromAttempt(attempt)
		if !ok || implementProgressSignatureUsable(record.CurrentSignature.signature()) ||
			!implementProgressDiffFingerprintEqual(record.WorkspaceDiffStats.Fingerprint, current.Fingerprint) ||
			!implementProgressWorkpadEqual(record, workpadStatus, humanAction) {
			return count
		}
		count++
	}
	return count
}

func consecutiveImplementStrandedWorkAttempts(attempts []store.WorkAttempt, current autoPromoteReworkSignature) int {
	count := 0
	for _, attempt := range attempts {
		record, ok := implementProgressRecordFromAttempt(attempt)
		if !ok || !implementProgressSignatureEqual(record.CurrentSignature.signature(), current) || !implementProgressRecordedStrandedEvidence(record.WorkspaceDiffStats) {
			return count
		}
		count++
	}
	return count
}

func implementProgressWorkpadEqual(record implementProgressRecord, workpadStatus string, humanAction string) bool {
	return strings.TrimSpace(record.WorkpadStatus) == strings.TrimSpace(workpadStatus) &&
		strings.TrimSpace(record.HumanAction) == strings.TrimSpace(humanAction)
}

func consecutiveImplementBlockedHumanActionAttempts(attempts []store.WorkAttempt, humanAction string, trackerState string) int {
	humanAction = strings.TrimSpace(humanAction)
	trackerState = normalizeState(trackerState)
	if humanAction == "" || trackerState == "" {
		return 0
	}
	count := 0
	for _, attempt := range attempts {
		record, ok := implementProgressRecordFromAttempt(attempt)
		if !ok || strings.TrimSpace(record.WorkpadStatus) != workpad.StatusBlocked ||
			strings.TrimSpace(record.HumanAction) != humanAction ||
			normalizeState(record.TrackerState) != trackerState {
			return count
		}
		count++
	}
	return count
}

func implementProgressDiffFingerprintEqual(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && left == right
}

func implementProgressRecordMatchesNoProgress(record implementProgressRecord, current autoPromoteReworkSignature) bool {
	if !implementProgressSignatureEqual(record.CurrentSignature.signature(), current) {
		return false
	}
	if strings.TrimSpace(record.Outcome) == implementProgressOutcomeNoProgress {
		return true
	}
	return !implementProgressSignatureUsable(current) &&
		strings.TrimSpace(record.Outcome) == string(store.WorkAttemptTerminalSuccess) &&
		strings.TrimSpace(record.Reason) == "no_linked_pull_request" &&
		record.WorkspaceDiffStats.Status != "" &&
		record.WorkspaceDiffStats.FilesChanged == 0 &&
		record.WorkspaceDiffStats.AddedLines == 0 &&
		record.WorkspaceDiffStats.RemovedLines == 0
}

func implementProgressRecordFromAttempt(attempt store.WorkAttempt) (implementProgressRecord, bool) {
	if attempt.TerminalState != store.WorkAttemptTerminalSuccess &&
		attempt.TerminalState != store.WorkAttemptTerminalNoProgress {
		return implementProgressRecord{}, false
	}
	return implementProgressRecordFromAnyAttempt(attempt)
}

func implementProgressRecordFromAnyAttempt(attempt store.WorkAttempt) (implementProgressRecord, bool) {
	var root struct {
		CompletionProgress implementProgressRecord `json:"completion_progress"`
		DispatchLoopStart  dispatchLoopStartRecord `json:"dispatch_loop_start"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(attempt.WorkerMetadataJSON)), &root); err != nil {
		return implementProgressRecord{}, false
	}
	record := root.CompletionProgress
	if strings.TrimSpace(record.Outcome) == "" {
		return implementProgressRecord{}, false
	}
	record.DispatchLoopStart = root.DispatchLoopStart
	return record, true
}

func implementCompletionProgressMetadata(decision implementCompletionProgressDecision) map[string]any {
	record := implementProgressRecord{
		Outcome:                string(decision.Outcome),
		Reason:                 decision.Reason,
		CurrentSignature:       implementProgressSignatureRecordFromSignature(decision.CurrentSignature),
		PreviousHeadSHA:        decision.PreviousSignature.HeadSHA,
		CurrentHeadSHA:         decision.CurrentSignature.HeadSHA,
		FailedChecksAdded:      append([]string(nil), decision.FailedChecksAdded...),
		FailedChecksRemoved:    append([]string(nil), decision.FailedChecksRemoved...),
		WorkspaceDiffStats:     implementProgressDiffStatsFromDiffStats(decision.WorkspaceDiffStats),
		ConsecutiveNoProgress:  decision.ConsecutiveNoProgress,
		WorkpadStatus:          strings.TrimSpace(decision.WorkpadStatus),
		HumanAction:            strings.TrimSpace(decision.HumanAction),
		TrackerState:           strings.TrimSpace(decision.TrackerState),
		ConsecutiveHumanAction: decision.ConsecutiveHumanAction,
		NoProgressLimit:        decision.NoProgressLimit,
		BlockReason:            strings.TrimSpace(decision.BlockReason),
		Warning:                strings.TrimSpace(decision.Warning),
		DependencyDeferral:     decision.DependencyDeferral,
		DependencyBlockers:     append([]implementDependencyBlocker(nil), decision.DependencyBlockers...),
		RejectedBlockerRefs:    append([]string(nil), decision.RejectedBlockerRefs...),
		ProgressKinds:          append([]string(nil), decision.ProgressKinds...),
		CompletionKind:         strings.TrimSpace(decision.CompletionKind),
	}
	if decision.PreviousSignatureFound {
		previous := implementProgressSignatureRecordFromSignature(decision.PreviousSignature)
		record.PreviousSignature = &previous
	}
	return map[string]any{implementProgressMetadataKey: record}
}

func implementProgressSignatureRecordFromSignature(signature autoPromoteReworkSignature) implementProgressSignatureRecord {
	return implementProgressSignatureRecord{
		PRNumber:     signature.PRNumber,
		HeadSHA:      strings.TrimSpace(signature.HeadSHA),
		FailedChecks: append([]string(nil), signature.FailedChecks...),
	}
}

func (r implementProgressSignatureRecord) signature() autoPromoteReworkSignature {
	return autoPromoteReworkSignature{
		PRNumber:     r.PRNumber,
		HeadSHA:      strings.TrimSpace(r.HeadSHA),
		FailedChecks: autoPromoteCanonicalChecks(r.FailedChecks),
	}
}

func implementProgressDiffStatsFromDiffStats(diffStats DiffStats) implementProgressDiffStats {
	return implementProgressDiffStats{
		FilesChanged:                   diffStats.FilesChanged,
		AddedLines:                     diffStats.AddedLines,
		RemovedLines:                   diffStats.RemovedLines,
		UnpushedCommits:                diffStats.UnpushedCommits,
		UnpushedCommitRefs:             append([]string(nil), diffStats.UnpushedCommitRefs...),
		TrackedPaths:                   append([]string(nil), diffStats.TrackedPaths...),
		UntrackedPaths:                 append([]string(nil), diffStats.UntrackedPaths...),
		CommitsNotInPullRequest:        append([]string(nil), diffStats.CommitsNotInPullRequest...),
		PullRequestComparisonAvailable: diffStats.PullRequestComparisonAvailable,
		RecoveryStateExpected:          diffStats.RecoveryStateExpected,
		RecoveryStateAvailable:         diffStats.RecoveryStateAvailable,
		HeadSHA:                        strings.TrimSpace(diffStats.HeadSHA),
		Fingerprint:                    strings.TrimSpace(diffStats.Fingerprint),
		Status:                         strings.TrimSpace(diffStats.Status),
	}
}

func implementProgressRecordedStrandedEvidence(diffStats implementProgressDiffStats) bool {
	return diffStats.UnpushedCommits > 0 || len(diffStats.TrackedPaths) > 0 || len(diffStats.UntrackedPaths) > 0 || len(diffStats.CommitsNotInPullRequest) > 0
}

func implementProgressSignatureUsable(signature autoPromoteReworkSignature) bool {
	return signature.PRNumber > 0 && strings.TrimSpace(signature.HeadSHA) != ""
}

func implementProgressSignatureEqual(left autoPromoteReworkSignature, right autoPromoteReworkSignature) bool {
	return left.PRNumber == right.PRNumber &&
		strings.TrimSpace(left.HeadSHA) == strings.TrimSpace(right.HeadSHA) &&
		slices.Equal(autoPromoteCanonicalChecks(left.FailedChecks), autoPromoteCanonicalChecks(right.FailedChecks))
}

func implementProgressDiffStatsClean(diffStats DiffStats) bool {
	return diffStatsPresent(diffStats) &&
		diffStats.FilesChanged == 0 &&
		diffStats.AddedLines == 0 &&
		diffStats.RemovedLines == 0
}

func implementProgressOperationalWorkspaceClean(diffStats DiffStats) bool {
	return implementProgressDiffStatsClean(diffStats) &&
		diffStats.UnpushedCommits == 0 &&
		diffStats.CommitsAhead == 0 &&
		len(diffStats.TrackedPaths) == 0 &&
		len(diffStats.UntrackedPaths) == 0 &&
		len(diffStats.CommitsNotInPullRequest) == 0
}

func implementProgressReconcilePullRequestEvidence(diffStats DiffStats, pullRequest *connector.PullRequest) DiffStats {
	if pullRequest == nil || strings.TrimSpace(diffStats.HeadSHA) == "" ||
		strings.TrimSpace(diffStats.HeadSHA) != strings.TrimSpace(pullRequest.HeadSHA) {
		return diffStats
	}
	diffStats.UnpushedCommits = 0
	diffStats.UnpushedCommitRefs = nil
	diffStats.CommitsNotInPullRequest = nil
	diffStats.PullRequestComparisonAvailable = true
	return diffStats
}

func implementProgressUnpushedClassification(diffStats DiffStats, pullRequest *connector.PullRequest) (bool, string) {
	if pullRequest != nil {
		if len(diffStats.TrackedPaths) > 0 || len(diffStats.UntrackedPaths) > 0 {
			return true, ""
		}
		if diffStats.PullRequestComparisonAvailable {
			return len(diffStats.CommitsNotInPullRequest) > 0, ""
		}
	}
	if diffStats.UnpushedCommits <= 0 {
		return false, ""
	}
	if pullRequest == nil {
		if !implementProgressDiffStatsClean(diffStats) {
			return true, ""
		}
		return false, unpushedRemoteTruthUnavailable
	}
	workspaceHead := strings.TrimSpace(diffStats.HeadSHA)
	if workspaceHead == "" {
		return false, workspaceHeadUnavailableReason
	}
	pullRequestHead := strings.TrimSpace(pullRequest.HeadSHA)
	if pullRequestHead == "" {
		return false, pullRequestHeadUnavailableReason
	}
	if workspaceHead == pullRequestHead {
		return false, ""
	}
	return true, ""
}

func implementProgressLinkedPullRequest(issue connector.Issue) bool {
	return workAttemptPRNumber(issue) != nil
}

func implementProgressHydrationUnavailableReason(pullRequest *connector.PullRequest) string {
	reasons := make([]string, 0, 2)
	if reason := pullRequestHydrationUnavailableReason(pullRequest); reason != "" {
		reasons = append(reasons, reason)
	}
	if reason := pullRequestHydrationDegradedReason(pullRequest); reason != "" {
		reasons = append(reasons, reason)
	}
	return strings.Join(reasons, "; ")
}

func implementProgressFailedCheckDelta(previous []string, current []string) ([]string, []string) {
	previous = autoPromoteCanonicalChecks(previous)
	current = autoPromoteCanonicalChecks(current)
	added := make([]string, 0)
	removed := make([]string, 0)
	for _, check := range current {
		if !slices.Contains(previous, check) {
			added = append(added, check)
		}
	}
	for _, check := range previous {
		if !slices.Contains(current, check) {
			removed = append(removed, check)
		}
	}
	return added, removed
}

func (o *Orchestrator) warnImplementProgressHydration(issue connector.Issue, message string, err error) {
	if o == nil || o.logger == nil {
		return
	}
	attrs := []any{
		"issue_id", issue.ID,
		"identifier", issue.Identifier,
		"reason", strings.TrimSpace(message),
	}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	o.logger.Warn("implement worker progress check failed open", attrs...)
}

func (o *Orchestrator) warnImplementProgressRefresh(issue connector.Issue, message string, err error) {
	if o == nil || o.logger == nil {
		return
	}
	attrs := []any{
		"issue_id", issue.ID,
		"identifier", issue.Identifier,
		"reason", strings.TrimSpace(message),
	}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	o.logger.Warn("implement worker tracker refresh failed open", attrs...)
}

func (o *Orchestrator) blockImplementProgress(
	ctx context.Context,
	state *State,
	running Running,
	decision implementCompletionProgressDecision,
	blockedAt time.Time,
) bool {
	issue := cloneIssue(decision.Issue)
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" {
		return false
	}
	blockReason := strings.TrimSpace(decision.BlockReason)
	if blockReason == "" {
		blockReason = noProgressLimitReason
	}
	predicate := blockedRecoveryPredicateFingerprintChange
	switch blockReason {
	case strandedUnpushedWorkReason:
		predicate = blockedRecoveryPredicateOncePerFingerprint
	case noProgressLimitReason, dispatchLoopDetectedReason:
		predicate = blockedRecoveryPredicateManaged
	}
	if strings.TrimSpace(decision.HumanAction) != "" || blockReason == workpadBlockedUnactionedReason {
		predicate = blockedRecoveryPredicateManaged
	}
	metadata := o.newBlockedRecoveryMetadata(
		ctx,
		issue,
		RunModeImplement,
		blockReason,
		predicate,
		autoPromoteReworkState,
		decision.WorkspaceDiffStats,
	)
	if predicate == blockedRecoveryPredicateManaged {
		metadata.BlockedRecovery.Owner = blockedRecoveryOwnerHuman
	}
	if err := o.updateIssueStateByIDWithMetadata(ctx, state, issueID, issue, blockedStatusState, blockedAt, blockReason, metadata, laneMutationRevokeWorker); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"no progress limit state transition failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"target_state", blockedStatusState,
				"error", err,
			)
		}
		return false
	}
	issue.State = blockedStatusState
	stageUpdatedAt := blockedAt.UTC()
	issue.StageUpdatedAt = &stageUpdatedAt
	if o.connector != nil {
		if err := o.connector.CreateComment(ctx, issueID, implementProgressBlockComment(issue, decision)); err != nil && o.logger != nil {
			o.logger.Warn("no progress limit comment failed", "issue_id", issueID, "identifier", issue.Identifier, "error", err)
		}
	}
	if err := o.abandonClaim(ctx, issueID); err != nil && o.logger != nil {
		o.logger.Warn("no progress limit claim release failed", "issue_id", issueID, "identifier", issue.Identifier, "error", err)
	}
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
	delete(state.PriorAttempts, issueID)
	if completed, ok := state.Completed[issueID]; ok {
		completed.Issue = issue
		state.Completed[issueID] = completed
	}
	if state.Blocked == nil {
		state.Blocked = map[string]Blocked{}
	}
	state.Blocked[issueID] = Blocked{
		Issue:          issue,
		Reason:         blockReason,
		RecoveryReason: implementProgressRecoveryReason(decision),
		RecoveryTarget: "Rework",
		BlockedAt:      blockedAt,
		Source:         BlockedSourceProjectStatus,
		Recovery:       metadata.BlockedRecovery,
	}
	eventName := "implement_worker_no_progress_limit"
	if blockReason == dispatchLoopDetectedReason {
		eventName = "implement_worker_dispatch_loop_detected"
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      blockedAt,
		Event:   eventName,
		Message: "parked " + issueLabel(issue) + " after implement progress breaker " + blockReason,
	})
	telemetry.LogLifecycle(o.logger, slog.LevelError, telemetry.LifecycleSafetyControl, eventName, o.runningLifecycleCorrelation(issue, running),
		"block_reason", blockReason,
		"consecutive_no_progress", decision.ConsecutiveNoProgress,
		"no_progress_limit", decision.NoProgressLimit,
	)
	return true
}

func (o *Orchestrator) finishImplementDependencyDeferral(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	completedAt time.Time,
) {
	if !o.issueHasStickyBlockReason(ctx, state, issue) {
		sourceState, _ := o.dispatchTimelineTransitionContext(ctx, issue)
		target := dependencyWaitTarget(issue, sourceState)
		if normalizeState(issue.State) != normalizeState(target) {
			if err := o.updateIssueState(ctx, state, issue, target, completedAt, "dependency_wait", laneMutationPreserveOwnership); err != nil && o.logger != nil {
				o.logger.Warn("dependency wait lane update failed", "issue_id", issue.ID, "error", err)
			}
		}
	}
	if err := o.abandonClaim(ctx, issue.ID); err != nil && o.logger != nil {
		o.logger.Warn("implement dependency deferral claim release failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
	}
	o.releaseClaim(state, issue.ID)
	repository := mergeWorkerRepositoryKey(issue)
	if reservation := state.mergeReservations[repository]; reservation.IssueID == issue.ID {
		delete(state.mergeReservations, repository)
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      completedAt,
		Event:   "implement_dependency_deferred",
		Message: "deferred " + issueLabel(issue) + " while declared dependencies remain unresolved",
	})
}

func implementProgressRecoveryReason(decision implementCompletionProgressDecision) string {
	if humanAction := strings.TrimSpace(decision.HumanAction); humanAction != "" {
		return humanAction
	}
	if implementProgressStrandedEvidence(decision.WorkspaceDiffStats) {
		return "validate and push the stranded workspace commits, then open or update the pull request and move the issue to Rework"
	}
	return "inspect the linked PR and worker logs, then move the issue back to Rework or Todo after a human confirms the next action"
}

func implementProgressBlockComment(issue connector.Issue, decision implementCompletionProgressDecision) string {
	var b strings.Builder
	if strings.TrimSpace(decision.BlockReason) == dispatchLoopDetectedReason {
		b.WriteString("Routed this issue to Blocked: loop detected after ")
		b.WriteString(strconv.Itoa(decision.ConsecutiveNoProgress))
		b.WriteString(" dispatches without lane, diff, commit, or pull request advancement.")
	} else if implementProgressStrandedEvidence(decision.WorkspaceDiffStats) {
		b.WriteString("Routed this issue to Blocked because the implement worker completed repeatedly with work produced but stranded unpushed in the workspace.")
	} else {
		b.WriteString("Routed this issue to Blocked because the implement worker completed repeatedly without deliverable progress.")
	}
	b.WriteString("\n\n- reason: ")
	blockReason := strings.TrimSpace(decision.BlockReason)
	if blockReason == "" {
		blockReason = noProgressLimitReason
	}
	b.WriteString(blockReason)
	if decision.NoProgressLimit > 0 {
		b.WriteString("\n- no_progress_limit: ")
		b.WriteString(strconv.Itoa(decision.NoProgressLimit))
	}
	if decision.ConsecutiveNoProgress > 0 {
		b.WriteString("\n- consecutive_no_progress_attempts: ")
		b.WriteString(strconv.Itoa(decision.ConsecutiveNoProgress))
	}
	if decision.ConsecutiveHumanAction > 0 {
		b.WriteString("\n- consecutive_human_action_attempts: ")
		b.WriteString(strconv.Itoa(decision.ConsecutiveHumanAction))
	}
	if issue.PullRequest != nil {
		if url := strings.TrimSpace(issue.PullRequest.URL); url != "" {
			b.WriteString("\n- pull request: ")
			b.WriteString(url)
		}
	}
	if decision.CurrentSignature.PRNumber > 0 {
		b.WriteString("\n- pr_number: ")
		b.WriteString(strconv.FormatInt(decision.CurrentSignature.PRNumber, 10))
	}
	if headSHA := strings.TrimSpace(decision.CurrentSignature.HeadSHA); headSHA != "" {
		b.WriteString("\n- current_head_sha: ")
		b.WriteString(headSHA)
	}
	if previousHeadSHA := strings.TrimSpace(decision.PreviousSignature.HeadSHA); previousHeadSHA != "" {
		b.WriteString("\n- previous_head_sha: ")
		b.WriteString(previousHeadSHA)
	}
	if failedChecks := strings.Join(decision.CurrentSignature.FailedChecks, ", "); failedChecks != "" {
		b.WriteString("\n- failed_checks: ")
		b.WriteString(failedChecks)
	}
	if added := strings.Join(decision.FailedChecksAdded, ", "); added != "" {
		b.WriteString("\n- failed_checks_added: ")
		b.WriteString(added)
	}
	if removed := strings.Join(decision.FailedChecksRemoved, ", "); removed != "" {
		b.WriteString("\n- failed_checks_removed: ")
		b.WriteString(removed)
	}
	if blockReason == dispatchLoopDetectedReason {
		appendDispatchLoopWorkspaceEvidence(&b, decision)
	} else {
		b.WriteString("\n- workspace_diffstat: ")
		if diffStatsPresent(decision.WorkspaceDiffStats) {
			b.WriteString(strconv.Itoa(decision.WorkspaceDiffStats.FilesChanged))
			b.WriteString(" files, +")
			b.WriteString(strconv.Itoa(decision.WorkspaceDiffStats.AddedLines))
			b.WriteString("/-")
			b.WriteString(strconv.Itoa(decision.WorkspaceDiffStats.RemovedLines))
			if status := strings.TrimSpace(decision.WorkspaceDiffStats.Status); status != "" {
				b.WriteString(" (")
				b.WriteString(status)
				b.WriteString(")")
			}
		} else {
			b.WriteString("unavailable")
		}
		if decision.WorkspaceDiffStats.UnpushedCommits > 0 {
			b.WriteString("\n- unpushed_commits: ")
			b.WriteString(strconv.Itoa(decision.WorkspaceDiffStats.UnpushedCommits))
		}
		appendStrandedWorkEvidence(&b, decision.WorkspaceDiffStats)
	}
	if humanAction := strings.TrimSpace(decision.HumanAction); humanAction != "" {
		b.WriteString("\n\nHuman action requested by the Workpad:\n\n")
		for line := range strings.SplitSeq(humanAction, "\n") {
			b.WriteString("> ")
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func implementProgressStrandedEvidence(diffStats DiffStats) bool {
	return diffStats.UnpushedCommits > 0 || len(diffStats.TrackedPaths) > 0 || len(diffStats.UntrackedPaths) > 0 || len(diffStats.CommitsNotInPullRequest) > 0
}

func appendStrandedWorkEvidence(b *strings.Builder, diffStats DiffStats) {
	appendQuotedEvidence(b, "tracked_paths", diffStats.TrackedPaths)
	appendQuotedEvidence(b, "untracked_paths", diffStats.UntrackedPaths)
	appendQuotedEvidence(b, "commits_not_in_pull_request", diffStats.CommitsNotInPullRequest)
	if len(diffStats.CommitsNotInPullRequest) == 0 {
		appendQuotedEvidence(b, "unpushed_commit_refs", diffStats.UnpushedCommitRefs)
	}
}

func appendQuotedEvidence(b *strings.Builder, label string, values []string) {
	if len(values) == 0 {
		return
	}
	b.WriteString("\n- ")
	b.WriteString(label)
	b.WriteString(": ")
	for index, value := range values {
		if index > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(value))
	}
}

func appendDispatchLoopWorkspaceEvidence(b *strings.Builder, decision implementCompletionProgressDecision) {
	diffStats := decision.WorkspaceDiffStats
	if !diffStatsPresent(diffStats) {
		b.WriteString("\n- workspace evidence: unavailable")
		return
	}
	b.WriteString("\n- evidence: workspace unchanged across ")
	b.WriteString(strconv.Itoa(decision.ConsecutiveNoProgress))
	b.WriteString(" attempts")
	if headSHA := strings.TrimSpace(diffStats.HeadSHA); headSHA != "" {
		b.WriteString(" — head `")
		b.WriteString(headSHA)
		b.WriteString("`")
		if fingerprint := strings.TrimSpace(diffStats.Fingerprint); fingerprint != "" {
			b.WriteString(" and diff fingerprint `")
			b.WriteString(fingerprint)
			b.WriteString("`")
		}
	} else if fingerprint := strings.TrimSpace(diffStats.Fingerprint); fingerprint != "" {
		b.WriteString(" — diff fingerprint `")
		b.WriteString(fingerprint)
		b.WriteString("`")
	}
	b.WriteString(" identical since attempt 1")

	if diffStats.FilesChanged == 0 && diffStats.AddedLines == 0 && diffStats.RemovedLines == 0 && diffStats.UnpushedCommits == 0 {
		return
	}
	b.WriteString("\n- carried stale work: carrying ")
	b.WriteString(strconv.Itoa(diffStats.FilesChanged))
	b.WriteString(" changed file")
	if diffStats.FilesChanged != 1 {
		b.WriteString("s")
	}
	b.WriteString(" (+")
	b.WriteString(strconv.Itoa(diffStats.AddedLines))
	b.WriteString("/-")
	b.WriteString(strconv.Itoa(diffStats.RemovedLines))
	b.WriteString("), ")
	b.WriteString(strconv.Itoa(diffStats.UnpushedCommits))
	b.WriteString(" unpushed commit")
	if diffStats.UnpushedCommits != 1 {
		b.WriteString("s")
	}
	b.WriteString(", unchanged since attempt 1")
}
