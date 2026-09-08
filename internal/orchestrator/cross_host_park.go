package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/provenance"
	"github.com/digitaldrywood/detent/internal/store"
)

const trackerRecoveryParkPrefix = "## Detent recovery park\n\n```detent-park\n"

type trackerRecoveryPark struct {
	Schema           int    `json:"schema"`
	Owner            string `json:"owner"`
	Cause            string `json:"cause"`
	CauseFingerprint string `json:"cause_fingerprint"`
}

func protectedRecoveryPark(park workflowLaneBlockedRecoveryMetadata) bool {
	if strings.EqualFold(strings.TrimSpace(park.Owner), blockedRecoveryOwnerHuman) ||
		strings.EqualFold(strings.TrimSpace(park.Owner), blockedRecoveryOwnerOperator) {
		return true
	}
	switch strings.TrimSpace(park.Cause) {
	case spendProgressReason, dispatchLoopDetectedReason, noProgressLimitReason:
		return true
	default:
		return false
	}
}

func (o *Orchestrator) publishTrackerRecoveryPark(ctx context.Context, issueID, targetState string, metadata workflowLaneMetadata) {
	park := metadata.BlockedRecovery
	if normalizeState(targetState) != normalizeState(blockedStatusState) || park == nil || !protectedRecoveryPark(*park) {
		return
	}
	data, err := json.Marshal(trackerRecoveryPark{Schema: 1, Owner: park.Owner, Cause: park.Cause, CauseFingerprint: park.CauseFingerprint})
	if err == nil {
		err = o.connector.CreateComment(ctx, issueID, trackerRecoveryParkPrefix+string(data)+"\n```\n\nDependency reconciliation must preserve this recovery park.")
	}
	if err != nil && o.logger != nil {
		o.logger.Warn("publish tracker recovery park failed", "issue_id", issueID, "error", err)
	}
}

func (o *Orchestrator) trackerRecoveryParkHold(ctx context.Context, issue connector.Issue) string {
	comments := issue.Comments
	if reader, ok := o.connector.(connector.IssueCommentReader); ok {
		var err error
		comments, err = reader.FetchIssueComments(ctx, issue)
		if err != nil {
			return "tracker_recovery_park_unavailable"
		}
	}
	for _, comment := range comments {
		if !comment.AuthorAuthorized {
			continue
		}
		if comment.CreatedAt != nil && !blockedEntryMatchesCurrent(issue, *comment.CreatedAt) {
			continue
		}
		body := strings.TrimSpace(comment.Body)
		if raw, ok := strings.CutPrefix(body, trackerRecoveryParkPrefix); ok {
			data, _, complete := strings.Cut(raw, "\n```")
			var park trackerRecoveryPark
			if !complete || json.Unmarshal([]byte(data), &park) != nil || park.Schema != 1 {
				return "tracker_recovery_park_unavailable"
			}
			if protectedRecoveryPark(workflowLaneBlockedRecoveryMetadata{Owner: park.Owner, Cause: park.Cause}) {
				return "tracker_recovery_park"
			}
		}
		if !strings.HasPrefix(body, "Routed this issue to Blocked") {
			continue
		}
		var park workflowLaneBlockedRecoveryMetadata
		for line := range strings.SplitSeq(body, "\n") {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "- reason: "); ok {
				park.Cause = strings.TrimSpace(value)
			}
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "- owner: "); ok {
				park.Owner = strings.TrimSpace(value)
			}
		}
		if protectedRecoveryPark(park) {
			return "tracker_recovery_park"
		}
	}
	return ""
}

func (o *Orchestrator) retainUnacknowledgedRecoveryParks(ctx context.Context, state *State, issues []connector.Issue) {
	for _, issue := range issues {
		if !stateIn(issue.State, o.cfg.ActiveStates) {
			continue
		}
		if _, running := state.Running[issue.ID]; running {
			continue
		}
		timeline, ok := o.issueWorkflowTimeline(ctx, issue)
		if !ok {
			if _, available := o.workflowMetrics.(WorkflowMetricsTimelineReader); available {
				state.Blocked[issue.ID] = Blocked{Issue: cloneIssue(issue), Source: BlockedSourceProjectStatus, Reason: "recovery_park_history_unavailable"}
			}
			continue
		}
		var park *workflowLaneBlockedRecoveryMetadata
		var parkedAt time.Time
		for _, event := range timeline.Events {
			if event.PhaseType != store.WorkflowPhaseTypeLane || event.Status != "entered" || !recoveryParkEventMatchesIssue(event, issue) {
				continue
			}
			metadata, _ := workflowLaneMetadataFromJSON(event.MetadataJSON)
			if normalizeState(event.PhaseName) == normalizeState(blockedStatusState) {
				candidate := metadata.BlockedRecovery
				if candidate == nil && protectedRecoveryPark(workflowLaneBlockedRecoveryMetadata{Cause: event.Reason}) {
					candidate = &workflowLaneBlockedRecoveryMetadata{Owner: blockedRecoveryOwnerHuman, Cause: event.Reason}
				}
				if candidate != nil && protectedRecoveryPark(*candidate) {
					park = candidate
					parkedAt = workflowLaneTransitionAt(event)
				}
				continue
			}
			if park != nil && recoveryParkAcknowledged(event, metadata, *park) {
				park = nil
			}
		}
		if park == nil {
			if blocked, ok := state.Blocked[issue.ID]; ok && (blocked.RecoveryReason == "park_acknowledgement_required" || blocked.Reason == "recovery_park_history_unavailable") {
				delete(state.Blocked, issue.ID)
			}
			continue
		}
		state.Blocked[issue.ID] = Blocked{
			Issue: cloneIssue(issue), Source: BlockedSourceProjectStatus, BlockedAt: parkedAt,
			Reason: park.Cause, Recovery: park, RecoveryAction: "hold", RecoveryReason: "park_acknowledgement_required",
			RecoveryRemedy: fmt.Sprintf("Use an explicit Detent work-item move to acknowledge the %s park before retrying.", park.Cause),
		}
		o.recordBlockedRecoveryDecision(ctx, state, issue, "hold", "park_acknowledgement_required", park, park.CauseFingerprint)
	}
}

func recoveryParkEventMatchesIssue(event store.WorkflowPhaseEvent, issue connector.Issue) bool {
	if event.IssueID != "" && issue.ID != "" {
		return event.IssueID == issue.ID
	}
	if event.Identifier != "" && issue.Identifier != "" {
		return strings.EqualFold(event.Identifier, issue.Identifier)
	}
	return event.IssueURL != "" && event.IssueURL == issue.URL
}

func recoveryParkAcknowledged(event store.WorkflowPhaseEvent, metadata workflowLaneMetadata, park workflowLaneBlockedRecoveryMetadata) bool {
	if metadata.Provenance.Initiator == provenance.InitiatorHuman && metadata.Provenance.Basis == provenance.BasisAuthenticatedHuman {
		return true
	}
	switch event.Reason {
	case "kanban_move", "kanban_move_field":
		return true
	}
	if metadata.Provenance.Initiator != provenance.InitiatorDetentInstance {
		return false
	}
	switch event.Reason {
	case workflowActionCauseBlockedRecovery:
		return park.Owner == blockedRecoveryOwnerOrchestrator
	case workflowActionBlockedReadyPRReconciliation:
		return blockedReadyPullRequestRecoverableCause(park)
	case "obsolete_artifact_spend_recovery":
		return park.Cause == spendProgressReason
	default:
		return false
	}
}
