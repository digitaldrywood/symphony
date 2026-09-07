package orchestrator

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workpad"
)

type deferredCandidate struct {
	issue              connector.Issue
	record             implementProgressRecord
	blocked            bool
	historyUnavailable bool
	detail             string
}

func (o *Orchestrator) evaluateImplementDependencyDeferral(
	ctx context.Context,
	issue connector.Issue,
) ([]implementDependencyBlocker, []string, bool) {
	signal, ok := autoPromoteIssueWorkpadSignal(issue)
	if !ok || signal == nil || signal.Source != workpad.SourceStructured {
		return nil, nil, false
	}
	if signal.Invalid != nil {
		rejected := []string{strings.TrimSpace(signal.Invalid.Message)}
		o.warnRejectedImplementDependencyRefs(issue, rejected, nil)
		return nil, rejected, false
	}
	if strings.TrimSpace(signal.Status) != workpad.StatusBlocked ||
		strings.TrimSpace(signal.HumanAction) != "" ||
		len(signal.Blockers) == 0 {
		return nil, nil, false
	}
	resolver, ok := o.connector.(connector.IssueReferenceResolver)
	if !ok {
		rejected := implementDependencyIdentifiers(signal.Blockers)
		o.warnRejectedImplementDependencyRefs(issue, rejected, errors.New("issue reference resolver unavailable"))
		return nil, rejected, false
	}
	identifiers := implementDependencyIdentifiers(signal.Blockers)
	resolved, err := resolver.FetchIssueStatesByIdentifiers(ctx, identifiers)
	if err != nil {
		o.warnRejectedImplementDependencyRefs(issue, identifiers, err)
		return nil, identifiers, false
	}
	byIdentifier := make(map[string]connector.Issue, len(resolved))
	for _, blocker := range resolved {
		identifier := normalizedIssueIdentifier(blocker.Identifier)
		if identifier != "" && strings.TrimSpace(blocker.ID) != "" {
			byIdentifier[identifier] = blocker
		}
	}
	self := normalizedIssueIdentifier(issue.Identifier)
	blockers := make([]implementDependencyBlocker, 0, len(identifiers))
	rejected := make([]string, 0)
	unresolved := false
	for _, identifier := range identifiers {
		key := normalizedIssueIdentifier(identifier)
		blocker, found := byIdentifier[key]
		if key == "" || key == self || !found {
			rejected = append(rejected, identifier)
			continue
		}
		blockers = append(blockers, implementDependencyBlocker{
			ID:         strings.TrimSpace(blocker.ID),
			Identifier: strings.TrimSpace(blocker.Identifier),
			State:      strings.TrimSpace(blocker.State),
		})
		if !implementDependencyTerminal(blocker, o.cfg.TerminalStates) {
			unresolved = true
		}
	}
	if len(rejected) > 0 {
		o.warnRejectedImplementDependencyRefs(issue, rejected, nil)
		return blockers, rejected, false
	}
	return blockers, nil, len(blockers) > 0 && unresolved
}

func (o *Orchestrator) filterImplementDependencyDeferrals(
	ctx context.Context,
	issues []connector.Issue,
) []connector.Issue {
	if o == nil || o.workAttempts == nil || len(issues) == 0 {
		return issues
	}
	candidates := make(map[string]*deferredCandidate)
	identifiers := make([]string, 0)
	seenIdentifiers := make(map[string]struct{})
	for _, issue := range issues {
		attempts, err := o.recentImplementCompletionAttempts(ctx, issue, Running{Issue: issue, Mode: runpkg.RunModeImplement})
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("implement dependency deferral history lookup failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
			}
			candidates[issue.ID] = &deferredCandidate{issue: issue, blocked: true, historyUnavailable: true, detail: "dependency deferral history unavailable: " + err.Error()}
			continue
		}
		if len(attempts) == 0 {
			continue
		}
		record, ok := implementProgressRecordFromAttempt(attempts[0])
		if !ok || !record.DependencyDeferral || record.Reason != implementDependencyDeferralReason || len(record.DependencyBlockers) == 0 {
			continue
		}
		candidate := &deferredCandidate{issue: issue, record: record, blocked: true, detail: "dependency resolution unavailable: no blocker identifiers"}
		candidates[issue.ID] = candidate
		for _, blocker := range record.DependencyBlockers {
			identifier := strings.TrimSpace(blocker.Identifier)
			key := normalizedIssueIdentifier(identifier)
			if key == "" {
				continue
			}
			if _, ok := seenIdentifiers[key]; ok {
				continue
			}
			seenIdentifiers[key] = struct{}{}
			identifiers = append(identifiers, identifier)
		}
	}
	if len(candidates) == 0 {
		return issues
	}
	if len(identifiers) == 0 {
		return o.removeDeferredImplementCandidates(ctx, issues, candidates)
	}
	resolver, ok := o.connector.(connector.IssueReferenceResolver)
	if !ok {
		o.markImplementDependencyDeferralRefreshUnavailable(candidates, errors.New("issue reference resolver unavailable"))
		return o.removeDeferredImplementCandidates(ctx, issues, candidates)
	}
	resolved, err := resolver.FetchIssueStatesByIdentifiers(ctx, identifiers)
	if err != nil {
		o.markImplementDependencyDeferralRefreshUnavailable(candidates, err)
		return o.removeDeferredImplementCandidates(ctx, issues, candidates)
	}
	byIdentifier := make(map[string]connector.Issue, len(resolved))
	for _, issue := range resolved {
		if identifier := normalizedIssueIdentifier(issue.Identifier); identifier != "" {
			byIdentifier[identifier] = issue
		}
	}
	for _, candidate := range candidates {
		if candidate.historyUnavailable {
			continue
		}
		var details []string
		for _, blocker := range candidate.record.DependencyBlockers {
			resolvedIssue, found := byIdentifier[normalizedIssueIdentifier(blocker.Identifier)]
			if !found {
				label := strings.TrimSpace(blocker.Identifier)
				if label == "" {
					label = "unknown dependency"
				}
				details = append(details, "dependency resolution unavailable: missing blocker result for "+label)
				continue
			}
			if implementDependencyTerminal(resolvedIssue, o.cfg.TerminalStates) {
				continue
			}
			if connector.HumanOwned(resolvedIssue) {
				details = append(details, humanDependencyWaitReason([]connector.BlockedRef{{Identifier: blocker.Identifier, HumanOwned: true}}))
			} else {
				details = append(details, "waiting on dependency "+blocker.Identifier)
			}
		}
		candidate.blocked = len(details) > 0
		candidate.detail = strings.Join(details, "; ")
		if o.logger != nil {
			o.logger.Debug(
				"implement dependency deferral evaluated",
				"issue_id", candidate.issue.ID,
				"identifier", candidate.issue.Identifier,
				"blocked", candidate.blocked,
				"blocker_refs", implementDependencyBlockerLabels(candidate.record.DependencyBlockers),
			)
		}
	}
	return o.removeDeferredImplementCandidates(ctx, issues, candidates)
}

func (o *Orchestrator) removeDeferredImplementCandidates(
	ctx context.Context,
	issues []connector.Issue,
	candidates map[string]*deferredCandidate,
) []connector.Issue {
	filtered := make([]connector.Issue, 0, len(issues))
	for _, issue := range issues {
		candidate, ok := candidates[issue.ID]
		if ok && candidate.blocked {
			o.recordSchedulerDecision(ctx, nil, time.Now(), dispatchPlanDecision{Issue: issue, SkipDetail: candidate.detail}, string(store.SchedulerDecisionResultSkipped), dispatchSkipBlockedByDependency)
			continue
		}
		filtered = append(filtered, issue)
	}
	return filtered
}

func implementDependencyIdentifiers(blockers []workpad.Blocker) []string {
	identifiers := make([]string, 0, len(blockers))
	seen := make(map[string]struct{}, len(blockers))
	for _, blocker := range blockers {
		identifier := strings.TrimSpace(blocker.Identifier)
		key := normalizedIssueIdentifier(identifier)
		if key == "" {
			identifier = strings.TrimSpace(blocker.Ref)
			key = strings.ToLower(identifier)
		}
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		identifiers = append(identifiers, identifier)
	}
	return identifiers
}

func implementDependencyTerminal(issue connector.Issue, terminalStates []string) bool {
	if connector.HumanOwned(issue) {
		return connector.HumanPrerequisiteReady(issue)
	}
	return issue.Closed || stateIn(issue.State, terminalStates) || pullRequestMerged(issue.PullRequest)
}

func implementDependencyBlockerLabels(blockers []implementDependencyBlocker) string {
	labels := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if identifier := strings.TrimSpace(blocker.Identifier); identifier != "" {
			labels = append(labels, identifier)
		}
	}
	return strings.Join(labels, ",")
}

func (o *Orchestrator) warnRejectedImplementDependencyRefs(issue connector.Issue, refs []string, err error) {
	if o == nil || o.logger == nil || len(refs) == 0 {
		return
	}
	attrs := []any{
		"issue_id", strings.TrimSpace(issue.ID),
		"identifier", strings.TrimSpace(issue.Identifier),
		"rejected_refs", strings.Join(refs, ","),
	}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	o.logger.Warn("implement dependency deferral rejected", attrs...)
}

func (o *Orchestrator) markImplementDependencyDeferralRefreshUnavailable(candidates map[string]*deferredCandidate, err error) {
	for _, candidate := range candidates {
		if candidate.historyUnavailable {
			continue
		}
		candidate.detail = "dependency resolution unavailable for " + implementDependencyBlockerLabels(candidate.record.DependencyBlockers) + ": " + err.Error()
		if o.logger == nil {
			continue
		}
		o.logger.Warn(
			"implement dependency deferral refresh failed",
			"issue_id", candidate.issue.ID,
			"identifier", candidate.issue.Identifier,
			"blocker_refs", implementDependencyBlockerLabels(candidate.record.DependencyBlockers),
			"error", err,
		)
	}
}
