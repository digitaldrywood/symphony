package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workpad"
)

const dispatchLoopDetectedReason = "dispatch_loop_detected"

const dispatchLoopStartMetadataKey = "dispatch_loop_start"

const dispatchLoopMinimumDispatches = 2

func dispatchLoopBlockMessage(decision implementCompletionProgressDecision) string {
	return fmt.Sprintf(
		"loop detected after %d dispatches without lane, diff, commit, or pull request advancement",
		decision.ConsecutiveNoProgress,
	)
}

type dispatchLoopFingerprint struct {
	Lane            string   `json:"lane,omitempty"`
	PRNumber        int64    `json:"pr_number,omitempty"`
	PRHeadSHA       string   `json:"pr_head_sha,omitempty"`
	FailedChecks    []string `json:"failed_checks,omitempty"`
	FilesChanged    int      `json:"files_changed"`
	AddedLines      int      `json:"added_lines"`
	RemovedLines    int      `json:"removed_lines"`
	UnpushedCommits int      `json:"unpushed_commits,omitempty"`
	WorkspaceHead   string   `json:"workspace_head,omitempty"`
	DiffFingerprint string   `json:"diff_fingerprint,omitempty"`
	DiffStatus      string   `json:"diff_status,omitempty"`
}

type dispatchLoopStartRecord struct {
	Fingerprint            dispatchLoopFingerprint `json:"fingerprint"`
	Captured               bool                    `json:"captured"`
	Persisted              bool                    `json:"persisted"`
	LaneAvailable          bool                    `json:"lane_available"`
	PullRequestAvailable   bool                    `json:"pull_request_available"`
	WorkspaceDiffAvailable bool                    `json:"workspace_diff_available"`
	WorkspaceHeadAvailable bool                    `json:"workspace_head_available"`
}

type dispatchLoopCompletionEvidence struct {
	LaneAvailable          bool
	PullRequestAvailable   bool
	WorkspaceDiffAvailable bool
	WorkspaceHeadAvailable bool
}

func (o *Orchestrator) evaluateDispatchLoopProgress(
	ctx context.Context,
	running Running,
	decision implementCompletionProgressDecision,
) implementCompletionProgressDecision {
	if decision.NoProgressLimit <= 0 || dispatchLoopPreservesSpecificDecision(decision) {
		return decision
	}

	dispatchLane := normalizeState(firstNonBlank(running.DispatchSourceState, running.Issue.State))
	currentLane := normalizeState(firstNonBlank(decision.TrackerState, decision.Issue.State))
	decision.TrackerState = strings.TrimSpace(firstNonBlank(decision.TrackerState, decision.Issue.State))
	if dispatchLane == "" || currentLane == "" || dispatchLane != currentLane {
		return resetDispatchLoopDecision(decision)
	}

	attempts, err := o.recentImplementCompletionAttempts(ctx, decision.Issue, running)
	if err != nil {
		if decision.Warning == "" {
			decision.Warning = err.Error()
		}
		return decision
	}

	current := dispatchLoopFingerprintFromDecision(decision, currentLane)
	withinAttemptAdvanced, withinAttemptComplete := dispatchLoopWithinAttemptProgress(
		running.DispatchLoopStart,
		current,
		dispatchLoopCompletionEvidenceFromDecision(decision, current),
	)
	if withinAttemptAdvanced || !withinAttemptComplete {
		return resetDispatchLoopDecision(decision)
	}
	previous, found := latestDispatchLoopFingerprint(attempts, currentLane)
	if dispatchLoopCurrentAdvanced(decision, current, previous, found) {
		return resetDispatchLoopDecision(decision)
	}

	decision.ConsecutiveNoProgress = 1 + consecutiveDispatchLoopAttempts(attempts, current)
	if decision.BlockReason == noProgressLimitReason {
		decision.Block = false
		decision.BlockReason = ""
	}
	if decision.ConsecutiveNoProgress < dispatchLoopMinimumDispatches || decision.ConsecutiveNoProgress < decision.NoProgressLimit {
		return decision
	}

	decision.Outcome = store.WorkAttemptTerminalNoProgress
	decision.Reason = dispatchLoopDetectedReason
	decision.BlockReason = dispatchLoopDetectedReason
	decision.Block = true
	decision.DependencyDeferral = false
	return decision
}

func dispatchLoopPreservesSpecificDecision(decision implementCompletionProgressDecision) bool {
	if decision.DependencyDeferral {
		return true
	}
	switch strings.TrimSpace(decision.Reason) {
	case strandedUnpushedWorkReason, workpadBlockedUnactionedReason:
		return true
	default:
		return decision.Block && decision.BlockReason != "" && decision.BlockReason != noProgressLimitReason
	}
}

func resetDispatchLoopDecision(decision implementCompletionProgressDecision) implementCompletionProgressDecision {
	decision.ConsecutiveNoProgress = 0
	if decision.BlockReason == noProgressLimitReason {
		decision.Block = false
		decision.BlockReason = ""
	}
	return decision
}

func dispatchLoopCurrentAdvanced(
	decision implementCompletionProgressDecision,
	current dispatchLoopFingerprint,
	previous dispatchLoopFingerprint,
	found bool,
) bool {
	switch strings.TrimSpace(decision.Reason) {
	case "pull_request_created_or_updated":
		return implementProgressSignatureUsable(decision.CurrentSignature)
	case implementMergedCompletionReason:
		return true
	case implementOperationalCompletion:
		return strings.TrimSpace(decision.CompletionKind) == workpad.CompletionOperational
	case "pull_request_hydrator_unavailable", "pull_request_hydration_failed", "pull_request_hydration_unavailable",
		"attempt_history_lookup_failed", "workspace_diffstat_unavailable", "workspace_diffstat_unavailable_without_pull_request",
		"workspace_diff_fingerprint_unavailable_without_pull_request", workspaceHeadUnavailableReason,
		pullRequestHeadUnavailableReason, unpushedRemoteTruthUnavailable:
		return true
	}
	if found {
		return !dispatchLoopFingerprintEqual(previous, current)
	}
	return false
}

func latestDispatchLoopFingerprint(attempts []store.WorkAttempt, currentLane string) (dispatchLoopFingerprint, bool) {
	for _, attempt := range attempts {
		record, ok := implementProgressRecordFromAnyAttempt(attempt)
		if !ok {
			continue
		}
		return dispatchLoopFingerprintFromRecord(attempt, record, currentLane), true
	}
	return dispatchLoopFingerprint{}, false
}

func consecutiveDispatchLoopAttempts(attempts []store.WorkAttempt, current dispatchLoopFingerprint) int {
	count := 0
	for _, attempt := range attempts {
		record, ok := implementProgressRecordFromAnyAttempt(attempt)
		if !ok {
			return count
		}
		fingerprint := dispatchLoopFingerprintFromRecord(attempt, record, current.Lane)
		if !dispatchLoopFingerprintEqual(fingerprint, current) || dispatchLoopRecordBreaksSequence(attempt, record, fingerprint) {
			return count
		}
		count++
	}
	return count
}

func dispatchLoopRecordBreaksSequence(attempt store.WorkAttempt, record implementProgressRecord, completion dispatchLoopFingerprint) bool {
	advanced, complete := dispatchLoopWithinAttemptProgress(
		record.DispatchLoopStart,
		completion,
		dispatchLoopCompletionEvidenceFromRecord(attempt, record, completion),
	)
	if advanced || !complete {
		return true
	}
	if record.ConsecutiveNoProgress > 0 {
		return false
	}
	for _, kind := range record.ProgressKinds {
		switch strings.TrimSpace(kind) {
		case "tracker_state_transition", "workspace_diff":
			return true
		case "pull_request":
			if implementProgressSignatureUsable(record.CurrentSignature.signature()) {
				return true
			}
		}
	}
	switch strings.TrimSpace(record.Reason) {
	case "pull_request_created_or_updated":
		return implementProgressSignatureUsable(record.CurrentSignature.signature())
	case "signature_changed", implementMergedCompletionReason:
		return true
	case implementOperationalCompletion:
		return strings.TrimSpace(record.CompletionKind) == workpad.CompletionOperational
	default:
		return false
	}
}

func newDispatchLoopStartRecord(issue connector.Issue, mode string) dispatchLoopStartRecord {
	if strings.TrimSpace(mode) != RunModeImplement {
		return dispatchLoopStartRecord{}
	}
	signature := autoPromoteReworkSignatureFromIssue(issue, AutoPromoteSummaryFromIssue(issue))
	lane := normalizeState(issue.State)
	return dispatchLoopStartRecord{
		Fingerprint:          dispatchLoopFingerprintFromValues(lane, signature, implementProgressDiffStats{}),
		LaneAvailable:        lane != "",
		PullRequestAvailable: dispatchLoopPullRequestEvidenceAvailable(workAttemptPRNumber(issue), signature, ""),
	}
}

func dispatchLoopStartRecordFromSnapshot(
	running Running,
	snapshot runpkg.DispatchLoopStartSnapshot,
) dispatchLoopStartRecord {
	record := running.DispatchLoopStart
	diff := implementProgressDiffStatsFromDiffStats(snapshot.DiffStats)
	record.Fingerprint = dispatchLoopFingerprintFromValues(
		normalizeState(firstNonBlank(running.DispatchSourceState, running.Issue.State)),
		record.Fingerprint.signature(),
		diff,
	)
	record.Captured = true
	record.Persisted = true
	record.WorkspaceDiffAvailable = snapshot.WorkspaceDiffAvailable
	record.WorkspaceHeadAvailable = snapshot.WorkspaceHeadAvailable
	return record
}

func (f dispatchLoopFingerprint) signature() autoPromoteReworkSignature {
	return autoPromoteReworkSignature{
		PRNumber:     f.PRNumber,
		HeadSHA:      strings.TrimSpace(f.PRHeadSHA),
		FailedChecks: append([]string(nil), f.FailedChecks...),
	}
}

func dispatchLoopWithinAttemptProgress(
	start dispatchLoopStartRecord,
	completion dispatchLoopFingerprint,
	evidence dispatchLoopCompletionEvidence,
) (bool, bool) {
	if !start.Captured || !start.Persisted {
		return false, false
	}
	if start.LaneAvailable && evidence.LaneAvailable && start.Fingerprint.Lane != completion.Lane {
		return true, true
	}
	if start.PullRequestAvailable && evidence.PullRequestAvailable &&
		(start.Fingerprint.PRNumber != completion.PRNumber || start.Fingerprint.PRHeadSHA != completion.PRHeadSHA) {
		return true, true
	}
	if start.WorkspaceDiffAvailable && evidence.WorkspaceDiffAvailable &&
		!dispatchLoopWorkspaceDiffEqual(start.Fingerprint, completion) {
		return true, true
	}
	if start.WorkspaceHeadAvailable && evidence.WorkspaceHeadAvailable && start.Fingerprint.WorkspaceHead != completion.WorkspaceHead {
		return true, true
	}
	complete := start.LaneAvailable && evidence.LaneAvailable &&
		start.PullRequestAvailable && evidence.PullRequestAvailable &&
		start.WorkspaceDiffAvailable && evidence.WorkspaceDiffAvailable &&
		start.WorkspaceHeadAvailable && evidence.WorkspaceHeadAvailable
	return false, complete
}

func dispatchLoopCompletionEvidenceFromDecision(
	decision implementCompletionProgressDecision,
	fingerprint dispatchLoopFingerprint,
) dispatchLoopCompletionEvidence {
	return dispatchLoopCompletionEvidence{
		LaneAvailable:          fingerprint.Lane != "",
		PullRequestAvailable:   dispatchLoopPullRequestEvidenceAvailable(workAttemptPRNumber(decision.Issue), decision.CurrentSignature, decision.Reason),
		WorkspaceDiffAvailable: diffStatsPresent(decision.WorkspaceDiffStats),
		WorkspaceHeadAvailable: strings.TrimSpace(decision.WorkspaceDiffStats.HeadSHA) != "",
	}
}

func dispatchLoopCompletionEvidenceFromRecord(
	attempt store.WorkAttempt,
	record implementProgressRecord,
	fingerprint dispatchLoopFingerprint,
) dispatchLoopCompletionEvidence {
	return dispatchLoopCompletionEvidence{
		LaneAvailable:          fingerprint.Lane != "",
		PullRequestAvailable:   dispatchLoopPullRequestEvidenceAvailable(attempt.PRNumber, record.CurrentSignature.signature(), record.Reason),
		WorkspaceDiffAvailable: implementProgressDiffStatsPresent(record.WorkspaceDiffStats),
		WorkspaceHeadAvailable: strings.TrimSpace(record.WorkspaceDiffStats.HeadSHA) != "",
	}
}

func dispatchLoopPullRequestEvidenceAvailable(number *int64, signature autoPromoteReworkSignature, reason string) bool {
	switch strings.TrimSpace(reason) {
	case "pull_request_hydrator_unavailable", "pull_request_hydration_failed", "pull_request_hydration_unavailable":
		return false
	}
	if number == nil && signature.PRNumber == 0 {
		return true
	}
	return implementProgressSignatureUsable(signature)
}

func dispatchLoopWorkspaceDiffEqual(left dispatchLoopFingerprint, right dispatchLoopFingerprint) bool {
	return left.FilesChanged == right.FilesChanged &&
		left.AddedLines == right.AddedLines &&
		left.RemovedLines == right.RemovedLines &&
		left.UnpushedCommits == right.UnpushedCommits &&
		left.DiffFingerprint == right.DiffFingerprint &&
		left.DiffStatus == right.DiffStatus
}

func implementProgressDiffStatsPresent(diff implementProgressDiffStats) bool {
	return diff.FilesChanged != 0 || diff.AddedLines != 0 || diff.RemovedLines != 0 ||
		diff.UnpushedCommits != 0 || len(diff.UnpushedCommitRefs) != 0 || len(diff.TrackedPaths) != 0 || len(diff.UntrackedPaths) != 0 ||
		len(diff.CommitsNotInPullRequest) != 0 || diff.PullRequestComparisonAvailable || diff.RecoveryStateExpected || diff.RecoveryStateAvailable || strings.TrimSpace(diff.HeadSHA) != "" ||
		strings.TrimSpace(diff.Fingerprint) != "" || strings.TrimSpace(diff.Status) != ""
}

func dispatchLoopFingerprintFromDecision(decision implementCompletionProgressDecision, lane string) dispatchLoopFingerprint {
	diff := implementProgressDiffStatsFromDiffStats(decision.WorkspaceDiffStats)
	return dispatchLoopFingerprintFromValues(lane, decision.CurrentSignature, diff)
}

func dispatchLoopFingerprintFromRecord(attempt store.WorkAttempt, record implementProgressRecord, fallbackLane string) dispatchLoopFingerprint {
	lane := normalizeState(firstNonBlank(record.TrackerState, attempt.Lane, fallbackLane))
	return dispatchLoopFingerprintFromValues(lane, record.CurrentSignature.signature(), record.WorkspaceDiffStats)
}

func dispatchLoopFingerprintFromValues(lane string, signature autoPromoteReworkSignature, diff implementProgressDiffStats) dispatchLoopFingerprint {
	diffStatus := strings.TrimSpace(diff.Status)
	if diffStatus == "" && diff.FilesChanged == 0 && diff.AddedLines == 0 && diff.RemovedLines == 0 &&
		diff.UnpushedCommits == 0 && strings.TrimSpace(diff.HeadSHA) == "" && strings.TrimSpace(diff.Fingerprint) == "" {
		diffStatus = "clean"
	}
	return dispatchLoopFingerprint{
		Lane:            normalizeState(lane),
		PRNumber:        signature.PRNumber,
		PRHeadSHA:       strings.TrimSpace(signature.HeadSHA),
		FailedChecks:    autoPromoteCanonicalChecks(signature.FailedChecks),
		FilesChanged:    diff.FilesChanged,
		AddedLines:      diff.AddedLines,
		RemovedLines:    diff.RemovedLines,
		UnpushedCommits: diff.UnpushedCommits,
		WorkspaceHead:   strings.TrimSpace(diff.HeadSHA),
		DiffFingerprint: strings.TrimSpace(diff.Fingerprint),
		DiffStatus:      diffStatus,
	}
}

func dispatchLoopFingerprintEqual(left dispatchLoopFingerprint, right dispatchLoopFingerprint) bool {
	return left.PRNumber == right.PRNumber &&
		left.PRHeadSHA == right.PRHeadSHA &&
		slices.Equal(left.FailedChecks, right.FailedChecks) &&
		left.FilesChanged == right.FilesChanged &&
		left.AddedLines == right.AddedLines &&
		left.RemovedLines == right.RemovedLines &&
		left.UnpushedCommits == right.UnpushedCommits &&
		left.WorkspaceHead == right.WorkspaceHead &&
		left.DiffFingerprint == right.DiffFingerprint &&
		left.DiffStatus == right.DiffStatus
}
