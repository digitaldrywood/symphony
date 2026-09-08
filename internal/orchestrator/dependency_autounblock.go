package orchestrator

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/dependencyline"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workpad"
)

const (
	DependencyReadinessTerminal         = workflowconfig.DependencyReadinessTerminal
	DependencyReadinessTerminalOrMerged = workflowconfig.DependencyReadinessTerminalOrMerged
)

type DependencyAutoUnblockConfig struct {
	Enabled      bool
	SourceStates []string
	TargetState  string
	Readiness    string
}

type BlockerAutoPromoteConfig struct {
	Enabled       bool
	SourceStates  []string
	BlockerStates []string
	TargetState   string
}

type dependencyBlocker struct {
	Ref      connector.BlockedRef
	Issue    connector.Issue
	Resolved bool
}

var (
	dependencyIssueRefPattern = regexp.MustCompile(`(?:([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+))?#(\d+)`)
	dependencyIssueURLPattern = regexp.MustCompile(`https?://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/issues/(\d+)`)
)

func normalizeDependencySource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case workflowconfig.DependencySourceNativeOnly:
		return workflowconfig.DependencySourceNativeOnly
	default:
		return workflowconfig.DependencySourceMerged
	}
}

func normalizeDependencyAutoUnblockConfig(cfg DependencyAutoUnblockConfig) DependencyAutoUnblockConfig {
	cfg.SourceStates = normalizedStates(defaultStringSlice(cfg.SourceStates, []string{blockedStatusState}))
	cfg.TargetState = strings.TrimSpace(defaultString(cfg.TargetState, "Todo"))
	cfg.Readiness = strings.ToLower(strings.TrimSpace(defaultString(cfg.Readiness, DependencyReadinessTerminalOrMerged)))
	return cfg
}

func normalizeBlockerAutoPromoteConfig(
	cfg BlockerAutoPromoteConfig,
	activeStates []string,
	autoUnblock DependencyAutoUnblockConfig,
) BlockerAutoPromoteConfig {
	sourceDefaults := mergeStateLists(activeStates, normalizeDependencyAutoUnblockConfig(autoUnblock).SourceStates)
	cfg.SourceStates = normalizedStates(defaultStringSlice(cfg.SourceStates, sourceDefaults))
	cfg.BlockerStates = normalizedStates(defaultStringSlice(cfg.BlockerStates, []string{"Backlog", "Blocked", "Human Review"}))
	cfg.TargetState = strings.TrimSpace(defaultString(cfg.TargetState, "Todo"))
	return cfg
}

func mergeStateLists(left []string, right []string) []string {
	out := make([]string, 0, len(left)+len(right))
	seen := map[string]struct{}{}
	for _, state := range append(append([]string{}, left...), right...) {
		key := normalizeState(state)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, displayStateName(state))
	}
	return out
}

func (o *Orchestrator) autoUnblockDependencyIssues(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
	now time.Time,
) map[string]struct{} {
	cfg := normalizeDependencyAutoUnblockConfig(o.cfg.DependencyAutoUnblock)
	if !cfg.Enabled {
		return nil
	}

	transitioned := map[string]struct{}{}
	for _, issue := range issuesInStates(issues, cfg.SourceStates) {
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" {
			continue
		}
		hydrated, ok := o.hydrateDependencyAutoUnblockIssue(ctx, issue, cfg.SourceStates)
		if !ok || connector.NonExecutableReason(hydrated) != "" {
			continue
		}
		hydrated, workpadRefs, workpadCurrent := o.issueWithCurrentWorkpadDependencyRefs(ctx, hydrated)
		if dependencyAutoUnblockWorkpadHold(hydrated, workpadCurrent) {
			o.logDependencyAutoUnblockDecision(hydrated, "hold", "workpad_status_not_blocked", nil, "")
			continue
		}
		if reason := o.blockedCauseHoldReason(hydrated, state, nil, cfg, workpadCurrent); reason != "" {
			o.logDependencyAutoUnblockDecision(hydrated, "hold", reason, nil, "")
			continue
		}
		if o.currentBlockedOperatorStop(ctx, state, hydrated) {
			o.logDependencyAutoUnblockDecision(hydrated, "hold", "operator_stop", nil, "")
			continue
		}
		if park, ok := o.currentBlockedRecoveryPark(ctx, state, hydrated); ok && strings.TrimSpace(park.Cause) != "" {
			o.logDependencyAutoUnblockDecision(hydrated, "hold", "blocked_cause_recovery", nil, "")
			continue
		}
		if stickyReason := o.issueStickyBlockReason(ctx, state, hydrated); stickyReason != "" {
			o.logDependencyAutoUnblockDecision(hydrated, "hold", stickyReason, nil, "")
			continue
		}
		if len(hydrated.BlockedBy) == 0 {
			continue
		}
		if reason := o.trackerRecoveryParkHold(ctx, hydrated); reason != "" {
			o.logDependencyAutoUnblockDecision(hydrated, "hold", reason, nil, "")
			continue
		}
		o.logDependencyProseOnly(hydrated)
		blockers := o.resolveDependencyBlockers(ctx, hydrated)
		workpadBlockers := dependencyBlockersMatchingRefs(blockers, workpadRefs)
		if reason := o.blockedCauseHoldReason(hydrated, state, workpadBlockers, cfg, workpadCurrent); reason != "" {
			o.logDependencyAutoUnblockDecision(hydrated, "hold", reason, blockers, "")
			continue
		}
		if !dependencyBlockersReady(blockers, cfg, o.cfg.TerminalStates) {
			o.logDependencyAutoUnblockDecision(hydrated, "skip", "blockers_not_ready", blockers, "")
			continue
		}
		blockerSignature := dependencyAutoUnblockBlockerSignature(blockers, cfg, o.cfg.TerminalStates)
		if o.dependencyAutoUnblockAlreadyConsumed(ctx, state, hydrated, blockerSignature) {
			action := "skip"
			reason := "blocker_set_already_consumed"
			if dependencyBlockersTerminal(blockers, o.cfg.TerminalStates) {
				action = "error"
				reason = "terminal_blocker_set_already_consumed"
			}
			o.logDependencyAutoUnblockDecision(hydrated, action, reason, blockers, "")
			continue
		}
		targetState := dependencyAutoUnblockTargetState(state, hydrated, cfg.TargetState)
		if !o.applyDependencyAutoUnblock(ctx, state, hydrated, blockers, blockerSignature, targetState, now) {
			o.logDependencyAutoUnblockDecision(hydrated, "skip", "transition_failed", blockers, targetState)
			continue
		}
		o.logDependencyAutoUnblockDecision(hydrated, "transition", "blockers_ready", blockers, targetState)
		transitioned[issueID] = struct{}{}
	}
	if len(transitioned) == 0 {
		return nil
	}
	return transitioned
}

func dependencyAutoUnblockWorkpadHold(issue connector.Issue, current bool) bool {
	signal := issue.WorkpadSignal
	return current && signal != nil && signal.Invalid == nil && signal.Source == workpad.SourceStructured &&
		strings.TrimSpace(signal.Status) != "" && strings.TrimSpace(signal.Status) != workpad.StatusBlocked
}

func (o *Orchestrator) autoPromoteBlockerIssues(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
	now time.Time,
) map[string]struct{} {
	cfg := normalizeBlockerAutoPromoteConfig(o.cfg.BlockerAutoPromote, o.cfg.ActiveStates, o.cfg.DependencyAutoUnblock)
	if !cfg.Enabled {
		return nil
	}

	remaining := o.blockerAutoPromoteCapacity(state, cfg.TargetState)
	if remaining <= 0 {
		return nil
	}

	transitioned := map[string]struct{}{}
	seenBlockers := map[string]struct{}{}
	for _, dependent := range issuesInStates(issues, cfg.SourceStates) {
		if remaining <= 0 {
			break
		}
		dependent = o.issueWithDependencyRefs(dependent)
		if len(dependent.BlockedBy) == 0 {
			continue
		}
		for _, blocker := range o.resolveDependencyBlockers(ctx, dependent) {
			if remaining <= 0 {
				break
			}
			if !blockerAutoPromoteEligible(dependent, blocker, cfg, o.cfg.TerminalStates) {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(firstNonBlank(blocker.Issue.ID, blocker.Issue.Identifier)))
			if key == "" {
				continue
			}
			if _, ok := seenBlockers[key]; ok {
				continue
			}
			seenBlockers[key] = struct{}{}
			if !o.applyBlockerAutoPromote(ctx, state, dependent, blocker.Issue, cfg.TargetState, now) {
				continue
			}
			transitioned[blocker.Issue.ID] = struct{}{}
			remaining--
		}
	}
	if len(transitioned) == 0 {
		return nil
	}
	return transitioned
}

func (o *Orchestrator) blockerAutoPromoteCapacity(state *State, targetState string) int {
	if state == nil {
		return 0
	}
	available := o.dispatchPlanner().availableSlots(state)
	stats := o.projectStateSlotStats(connector.Issue{State: targetState}, state)
	if stats.available < available {
		available = stats.available
	}
	if available < 0 {
		return 0
	}
	return available
}

func blockerAutoPromoteEligible(
	dependent connector.Issue,
	blocker dependencyBlocker,
	cfg BlockerAutoPromoteConfig,
	terminalStates []string,
) bool {
	if !blocker.Resolved {
		return false
	}
	issue := blocker.Issue
	if strings.TrimSpace(issue.ID) == "" || connector.NonExecutableReason(issue) != "" {
		return false
	}
	if !stateIn(issue.State, cfg.BlockerStates) {
		return false
	}
	if stateIn(issue.State, terminalStates) || issue.Closed || pullRequestMerged(issue.PullRequest) || pullRequestOpen(issue.PullRequest) {
		return false
	}
	if normalizeState(issue.State) == normalizeState(cfg.TargetState) {
		return false
	}
	return sameDependencyRepository(dependent.Identifier, issue.Identifier)
}

func sameDependencyRepository(leftIdentifier string, rightIdentifier string) bool {
	leftRepo := dependencyIssueRepo(leftIdentifier)
	rightRepo := dependencyIssueRepo(rightIdentifier)
	return leftRepo != "" && rightRepo != "" && strings.EqualFold(leftRepo, rightRepo)
}

func (o *Orchestrator) applyBlockerAutoPromote(
	ctx context.Context,
	state *State,
	dependent connector.Issue,
	blocker connector.Issue,
	targetState string,
	now time.Time,
) bool {
	if connector.NonExecutableReason(blocker) != "" || o.issueHasStickyBlockReason(ctx, state, blocker) || o.currentBlockedOperatorStop(ctx, state, blocker) {
		return false
	}
	if park, ok := o.currentBlockedRecoveryPark(ctx, state, blocker); ok && strings.TrimSpace(park.Cause) != "" {
		return false
	}
	issueID := strings.TrimSpace(blocker.ID)
	if err := o.updateIssueStateByID(ctx, state, issueID, blocker, targetState, now, "blocker_auto_promote", laneMutationPreserveOwnership); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"blocker auto-promote transition failed",
				"issue_id", issueID,
				"identifier", blocker.Identifier,
				"dependent_identifier", dependent.Identifier,
				"from_state", blocker.State,
				"target_state", targetState,
				"error", err,
			)
		}
		return false
	}

	body := blockerAutoPromoteComment(dependent, blocker, targetState)
	if err := o.connector.CreateComment(ctx, issueID, body); err != nil && o.logger != nil {
		o.logger.Warn(
			"blocker auto-promote comment failed",
			"issue_id", issueID,
			"identifier", blocker.Identifier,
			"dependent_identifier", dependent.Identifier,
			"target_state", targetState,
			"error", err,
		)
	}

	delete(state.Blocked, issueID)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "blocker_auto_promote_transition",
		Message: "auto-promoted blocker " + issueLabel(blocker) + " from " + blocker.State + " to " + targetState + " for " + issueLabel(dependent),
	})
	if o.logger != nil {
		o.logger.Info(
			"blocker auto-promote transition",
			"issue_id", issueID,
			"identifier", blocker.Identifier,
			"dependent_identifier", dependent.Identifier,
			"from_state", blocker.State,
			"target_state", targetState,
		)
	}
	return true
}

func blockerAutoPromoteComment(dependent connector.Issue, blocker connector.Issue, targetState string) string {
	var b strings.Builder
	b.WriteString("Dependency blocker queued.")
	if strings.TrimSpace(blocker.State) != "" && strings.TrimSpace(targetState) != "" {
		b.WriteString(" Moved this blocker from ")
		b.WriteString(strings.TrimSpace(blocker.State))
		b.WriteString(" to ")
		b.WriteString(strings.TrimSpace(targetState))
		b.WriteString(".")
	}
	b.WriteString("\n\nDependent issue: ")
	b.WriteString(issueLabel(dependent))
	if url := strings.TrimSpace(dependent.URL); url != "" {
		b.WriteString(" ")
		b.WriteString(url)
	}
	return b.String()
}

func (o *Orchestrator) hydrateDependencyAutoUnblockIssue(
	ctx context.Context,
	issue connector.Issue,
	sourceStates []string,
) (connector.Issue, bool) {
	issue = o.issueWithDependencyRefs(issue)
	if strings.TrimSpace(issue.Identifier) == "" {
		return issue, stateIn(issue.State, sourceStates)
	}
	resolver, ok := o.connector.(connector.IssueReferenceResolver)
	if !ok {
		return issue, stateIn(issue.State, sourceStates)
	}
	issues, err := resolver.FetchIssueStatesByIdentifiers(ctx, []string{issue.Identifier})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("hydrate dependency auto-unblock issue failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		return connector.Issue{}, false
	}
	for _, hydrated := range issues {
		if !sameIssueIdentity(issue, hydrated) {
			continue
		}
		previousBlockedBy := append([]connector.BlockedRef(nil), issue.BlockedBy...)
		merged := mergeIssueTrackerFields(issue, hydrated)
		merged.BlockedBy = mergeDependencyBlockedRefs(merged.BlockedBy, previousBlockedBy)
		merged = o.issueWithDependencyRefs(merged)
		return merged, stateIn(merged.State, sourceStates)
	}
	return issue, stateIn(issue.State, sourceStates)
}

func (o *Orchestrator) issueWithDependencyRefs(issue connector.Issue) connector.Issue {
	if normalizeDependencySource(o.cfg.DependencySource) == workflowconfig.DependencySourceNativeOnly {
		issue.BlockedBy = dependencyBlockedRefsWithoutSelf(issue.BlockedBy, issue.Identifier)
		return issue
	}
	return issueWithTextDependencyRefs(issue)
}

func (o *Orchestrator) issueWithCurrentWorkpadDependencyRefs(
	ctx context.Context,
	issue connector.Issue,
) (connector.Issue, []connector.BlockedRef, bool) {
	current := o.workpadSignalMatchesCurrentBlockedEntry(ctx, issue)
	if !current || issue.WorkpadSignal == nil || strings.TrimSpace(issue.WorkpadSignal.Status) != workpad.StatusBlocked ||
		len(issue.WorkpadSignal.Blockers) == 0 {
		return issue, nil, current
	}
	refs := workpadDependencyRefs(issue)
	issue.BlockedBy = mergeDependencyBlockedRefs(issue.BlockedBy, refs)
	issue.BlockedBy = dependencyBlockedRefsWithoutSelf(issue.BlockedBy, issue.Identifier)
	refs = dependencyBlockedRefsWithoutSelf(refs, issue.Identifier)
	return issue, refs, true
}

func (o *Orchestrator) workpadSignalMatchesCurrentBlockedEntry(ctx context.Context, issue connector.Issue) bool {
	signal := issue.WorkpadSignal
	if signal == nil || signal.Invalid != nil || signal.Source != workpad.SourceStructured {
		return false
	}
	commentAt, ok := workpadSignalCommentTime(issue, signal)
	if !ok {
		return true
	}
	timeline, ok := o.issueWorkflowTimeline(ctx, issue)
	if !ok {
		return true
	}
	currentIndex := -1
	for index := len(timeline.Events) - 1; index >= 0; index-- {
		event := timeline.Events[index]
		if event.PhaseType != store.WorkflowPhaseTypeLane || !strings.EqualFold(strings.TrimSpace(event.Status), "entered") {
			continue
		}
		if normalizeState(event.PhaseName) != normalizeState(blockedStatusState) || !workflowLaneEntryMatchesCurrent(issue, event) {
			return false
		}
		currentIndex = index
		break
	}
	if currentIndex < 0 {
		return true
	}
	for index := currentIndex - 1; index >= 0; index-- {
		event := timeline.Events[index]
		if event.PhaseType != store.WorkflowPhaseTypeLane || !strings.EqualFold(strings.TrimSpace(event.Status), "entered") {
			continue
		}
		return !commentAt.Before(event.StartedAt.Add(-reworkBreakerStageUpdateSkew))
	}
	return true
}

func workpadSignalCommentTime(issue connector.Issue, signal *workpad.Signal) (time.Time, bool) {
	if signal == nil {
		return time.Time{}, false
	}
	commentURL := strings.TrimSpace(signal.CommentURL)
	if commentURL == "" {
		if signal.RecordedAt != nil && !signal.RecordedAt.IsZero() {
			return signal.RecordedAt.UTC(), true
		}
		return time.Time{}, false
	}
	for index := len(issue.Comments) - 1; index >= 0; index-- {
		comment := issue.Comments[index]
		if strings.TrimSpace(comment.URL) != commentURL {
			continue
		}
		if comment.UpdatedAt != nil && !comment.UpdatedAt.IsZero() {
			return comment.UpdatedAt.UTC(), true
		}
		if comment.CreatedAt != nil && !comment.CreatedAt.IsZero() {
			return comment.CreatedAt.UTC(), true
		}
		return time.Time{}, false
	}
	if signal.RecordedAt != nil && !signal.RecordedAt.IsZero() {
		return signal.RecordedAt.UTC(), true
	}
	return time.Time{}, false
}

func workpadDependencyRefs(issue connector.Issue) []connector.BlockedRef {
	signal := issue.WorkpadSignal
	if signal == nil {
		return nil
	}
	repo := dependencyIssueRepo(issue.Identifier)
	refs := make([]connector.BlockedRef, 0, len(signal.Blockers))
	for _, blocker := range signal.Blockers {
		if blocker.Predicate != nil {
			if blocker.Predicate.Type != workpad.PredicateIssueState || len(blocker.Predicate.States) > 0 {
				continue
			}
		}
		identifier := strings.TrimSpace(blocker.Identifier)
		if identifier == "" && blocker.Predicate != nil {
			identifier = strings.TrimSpace(blocker.Predicate.Identifier)
		}
		if identifier == "" {
			parsed, err := workpad.ParseRef(blocker.Ref, repo)
			if err != nil {
				continue
			}
			identifier = parsed
		}
		refs = append(refs, connector.BlockedRef{
			Identifier: identifier,
			Source:     connector.BlockedRefSourceWorkpad,
		})
	}
	return refs
}

func issueWithTextDependencyRefs(issue connector.Issue) connector.Issue {
	issue.BlockedBy = mergeDependencyBlockedRefs(issue.BlockedBy, dependencyRefsFromIssueText(issue))
	issue.BlockedBy = dependencyBlockedRefsWithoutSelf(issue.BlockedBy, issue.Identifier)
	return issue
}

func mergeDependencyBlockedRefs(existing []connector.BlockedRef, incoming []connector.BlockedRef) []connector.BlockedRef {
	if len(existing) == 0 && len(incoming) == 0 {
		return nil
	}
	merged := make([]connector.BlockedRef, 0, len(existing)+len(incoming))
	seen := map[string]struct{}{}
	appendRefs := func(refs []connector.BlockedRef) {
		for _, ref := range refs {
			key := strings.ToLower(strings.TrimSpace(ref.Identifier))
			if key == "" {
				key = strings.ToLower(strings.TrimSpace(ref.ID))
			}
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			if strings.TrimSpace(ref.Source) == "" {
				ref.Source = connector.BlockedRefSourceProse
			}
			seen[key] = struct{}{}
			merged = append(merged, ref)
		}
	}
	appendRefs(existing)
	appendRefs(incoming)
	return merged
}

func dependencyBlockedRefsWithoutSelf(refs []connector.BlockedRef, identifier string) []connector.BlockedRef {
	self := strings.ToLower(strings.TrimSpace(identifier))
	if self == "" || len(refs) == 0 {
		return refs
	}
	filtered := refs[:0]
	for _, ref := range refs {
		if strings.ToLower(strings.TrimSpace(ref.Identifier)) == self {
			continue
		}
		filtered = append(filtered, ref)
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func dependencyRefsFromIssueText(issue connector.Issue) []connector.BlockedRef {
	repo := dependencyIssueRepo(issue.Identifier)
	refs := []connector.BlockedRef{}
	seen := map[string]struct{}{}
	appendRefs := func(incoming []connector.BlockedRef) {
		for _, ref := range incoming {
			key := strings.ToLower(strings.TrimSpace(ref.Identifier))
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			refs = append(refs, ref)
		}
	}
	appendRefs(dependencyLineRefs(issue.Description, repo))
	appendRefs(dependencyRefsInText(dependencyMarkdownSectionText(issue.Description, "Blockers"), repo))
	appendRefs(dependencyReasonRefs(issue.BlockerReason, repo))
	return refs
}

func dependencyLineRefs(body string, repo string) []connector.BlockedRef {
	refs := []connector.BlockedRef{}
	for _, line := range strings.FieldsFunc(body, func(r rune) bool {
		return r == '\n' || r == '\r'
	}) {
		text, ok := dependencyline.Match(line)
		if !ok {
			continue
		}
		refs = append(refs, dependencyRefsInText(text, repo)...)
	}
	return refs
}

func dependencyRefsInText(text string, repo string) []connector.BlockedRef {
	refs := []connector.BlockedRef{}
	for _, identifier := range dependencyIssueIdentifiersInText(text, repo) {
		refs = append(refs, connector.BlockedRef{Identifier: identifier, Source: connector.BlockedRefSourceProse})
	}
	return refs
}

func dependencyReasonRefs(reason string, repo string) []connector.BlockedRef {
	refs := dependencyLineRefs(reason, repo)
	if len(refs) > 0 {
		return refs
	}
	if !dependencyReasonMentionsWaiting(reason) {
		return nil
	}
	return dependencyRefsInText(reason, repo)
}

func dependencyReasonMentionsWaiting(reason string) bool {
	reason = strings.ToLower(strings.Join(strings.Fields(reason), " "))
	for _, phrase := range []string{
		"waiting on",
		"waited on",
		"blocked by",
		"depends on",
		"dependency",
		"blocker",
	} {
		if strings.Contains(reason, phrase) {
			return true
		}
	}
	return false
}

func dependencyIssueIdentifiersInText(text string, repo string) []string {
	refs := []string{}
	seen := map[string]struct{}{}
	add := func(refRepo string, number string) {
		identifier := dependencyBlockerIdentifier(refRepo, number, repo)
		if identifier == "" {
			return
		}
		key := strings.ToLower(identifier)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		refs = append(refs, identifier)
	}
	for _, matches := range dependencyIssueURLPattern.FindAllStringSubmatch(text, -1) {
		if len(matches) == 3 {
			add(matches[1], matches[2])
		}
	}
	for _, matches := range dependencyIssueRefPattern.FindAllStringSubmatch(text, -1) {
		if len(matches) == 3 {
			add(matches[1], matches[2])
		}
	}
	return refs
}

func dependencyIssueRepo(identifier string) string {
	identifier = strings.TrimSpace(identifier)
	index := strings.LastIndex(identifier, "#")
	if index <= 0 {
		return ""
	}
	return strings.TrimSpace(identifier[:index])
}

func dependencyBlockerIdentifier(refRepo string, number string, repo string) string {
	if strings.TrimSpace(number) == "" {
		return ""
	}
	refRepo = strings.TrimSpace(refRepo)
	if refRepo == "" {
		if repo == "" {
			return "#" + strings.TrimSpace(number)
		}
		refRepo = repo
	}
	return refRepo + "#" + strings.TrimSpace(number)
}

func dependencyMarkdownSectionText(body string, title string) string {
	want := dependencyNormalizeSectionTitle(title)
	inSection := false
	lines := []string{}
	for line := range strings.SplitSeq(body, "\n") {
		heading, ok := dependencyMarkdownHeadingTitle(line)
		if ok {
			if inSection {
				break
			}
			inSection = dependencyNormalizeSectionTitle(heading) == want
			continue
		}
		if inSection {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func dependencyMarkdownHeadingTitle(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || line[0] != '#' {
		return "", false
	}
	index := 0
	for index < len(line) && line[index] == '#' {
		index++
	}
	if index > 6 || index == len(line) {
		return "", false
	}
	if line[index] != ' ' && line[index] != '\t' {
		return "", false
	}
	return strings.Trim(strings.TrimSpace(line[index:]), "# \t"), true
}

func dependencyNormalizeSectionTitle(title string) string {
	return strings.ToLower(strings.Join(strings.Fields(title), " "))
}

func sameIssueIdentity(left connector.Issue, right connector.Issue) bool {
	leftID := strings.TrimSpace(left.ID)
	rightID := strings.TrimSpace(right.ID)
	if leftID != "" && rightID != "" && leftID == rightID {
		return true
	}
	leftIdentifier := strings.ToLower(strings.TrimSpace(left.Identifier))
	rightIdentifier := strings.ToLower(strings.TrimSpace(right.Identifier))
	return leftIdentifier != "" && leftIdentifier == rightIdentifier
}

func (o *Orchestrator) resolveDependencyBlockers(ctx context.Context, issue connector.Issue) []dependencyBlocker {
	blockers := make([]dependencyBlocker, 0, len(issue.BlockedBy))
	identifiers := make([]string, 0, len(issue.BlockedBy))
	seen := map[string]struct{}{}
	for _, ref := range issue.BlockedBy {
		ref.Identifier = strings.TrimSpace(ref.Identifier)
		ref.ID = strings.TrimSpace(ref.ID)
		ref.State = strings.TrimSpace(ref.State)
		if ref.Identifier == "" && ref.ID == "" {
			continue
		}
		blockers = append(blockers, dependencyBlocker{Ref: ref})
		if ref.Identifier == "" {
			continue
		}
		key := strings.ToLower(ref.Identifier)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		identifiers = append(identifiers, ref.Identifier)
	}

	resolver, ok := o.connector.(connector.IssueReferenceResolver)
	if !ok || len(identifiers) == 0 {
		return blockers
	}
	issues, err := resolver.FetchIssueStatesByIdentifiers(ctx, identifiers)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("resolve dependency blockers failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		return blockers
	}

	byIdentifier := make(map[string]connector.Issue, len(issues))
	for _, blocker := range issues {
		identifier := strings.ToLower(strings.TrimSpace(blocker.Identifier))
		if identifier == "" {
			continue
		}
		byIdentifier[identifier] = blocker
	}
	for index := range blockers {
		identifier := strings.ToLower(strings.TrimSpace(blockers[index].Ref.Identifier))
		blocker, ok := byIdentifier[identifier]
		if !ok {
			continue
		}
		blockers[index].Ref.HumanOwned = connector.HumanOwned(blocker)
		blockers[index].Ref.HumanCompletionReady = connector.HumanOwned(blocker) && connector.HumanPrerequisiteReady(blocker)
		blockers[index].Issue = blocker
		blockers[index].Resolved = true
		blockers[index].Ref.ID = firstNonBlank(blocker.ID, blockers[index].Ref.ID)
		blockers[index].Ref.Identifier = firstNonBlank(blocker.Identifier, blockers[index].Ref.Identifier)
		blockers[index].Ref.State = firstNonBlank(blocker.State, blockers[index].Ref.State)
		blockers[index].Ref.TrackerState = connector.BlockedRefTrackerStateOpen
		if blocker.Closed {
			blockers[index].Ref.TrackerState = connector.BlockedRefTrackerStateClosed
		}
	}
	return blockers
}

func dependencyResolvedBlockerRefs(blockers []dependencyBlocker) []connector.BlockedRef {
	refs := make([]connector.BlockedRef, 0, len(blockers))
	for _, blocker := range blockers {
		refs = append(refs, blocker.Ref)
	}
	return refs
}

func dependencyBlockersReady(blockers []dependencyBlocker, cfg DependencyAutoUnblockConfig, terminalStates []string) bool {
	if len(blockers) == 0 {
		return false
	}
	for _, blocker := range blockers {
		if !dependencyBlockerReady(blocker, cfg, terminalStates) {
			return false
		}
	}
	return true
}

func dependencyBlockersMatchingRefs(blockers []dependencyBlocker, refs []connector.BlockedRef) []dependencyBlocker {
	wanted := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		key := strings.ToLower(strings.TrimSpace(firstNonBlank(ref.Identifier, ref.ID)))
		if key != "" {
			wanted[key] = struct{}{}
		}
	}
	filtered := make([]dependencyBlocker, 0, len(wanted))
	for _, blocker := range blockers {
		key := dependencyAutoUnblockBlockerKey(blocker)
		if _, ok := wanted[key]; ok {
			filtered = append(filtered, blocker)
		}
	}
	return filtered
}

func dependencyBlockersNotReady(blockers []dependencyBlocker, cfg DependencyAutoUnblockConfig, terminalStates []string) []dependencyBlocker {
	filtered := make([]dependencyBlocker, 0, len(blockers))
	for _, blocker := range blockers {
		if !dependencyBlockerReady(blocker, cfg, terminalStates) {
			filtered = append(filtered, blocker)
		}
	}
	return filtered
}

func dependencyBlockersTerminal(blockers []dependencyBlocker, terminalStates []string) bool {
	if len(blockers) == 0 {
		return false
	}
	for _, blocker := range blockers {
		if blocker.Resolved {
			if !blocker.Issue.Closed && !stateIn(blocker.Issue.State, terminalStates) {
				return false
			}
			continue
		}
		if !stateIn(blocker.Ref.State, terminalStates) {
			return false
		}
	}
	return true
}

func dependencyBlockerReady(blocker dependencyBlocker, cfg DependencyAutoUnblockConfig, terminalStates []string) bool {
	if blocker.Resolved && connector.HumanOwned(blocker.Issue) {
		return connector.HumanPrerequisiteReady(blocker.Issue)
	}
	if blocker.Ref.HumanOwned {
		return blocker.Ref.HumanCompletionReady
	}
	if blocker.Resolved {
		if blocker.Issue.Closed || stateIn(blocker.Issue.State, terminalStates) {
			return true
		}
		if cfg.Readiness == DependencyReadinessTerminalOrMerged && pullRequestMerged(blocker.Issue.PullRequest) {
			return true
		}
		return false
	}
	if strings.TrimSpace(blocker.Ref.State) == "" {
		return false
	}
	return stateIn(blocker.Ref.State, terminalStates)
}

func pullRequestMerged(pullRequest *connector.PullRequest) bool {
	return pullRequest != nil && normalizePullRequestState(pullRequest.State) == "merged"
}

func pullRequestOpen(pullRequest *connector.PullRequest) bool {
	return pullRequest != nil && normalizePullRequestState(pullRequest.State) == "open"
}

func dependencyAutoUnblockTargetState(state *State, issue connector.Issue, configuredTarget string) string {
	if dependencyAutoUnblockStarted(state, issue) {
		return autoPromoteReworkState
	}
	return strings.TrimSpace(defaultString(configuredTarget, "Todo"))
}

func dependencyAutoUnblockStarted(state *State, issue connector.Issue) bool {
	if dependencyAutoUnblockStartedSignal(issue) {
		return true
	}
	if state == nil {
		return false
	}
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" {
		return false
	}
	if _, ok := state.Running[issueID]; ok {
		return true
	}
	if _, ok := state.Claimed[issueID]; ok {
		return true
	}
	if _, ok := state.Retry[issueID]; ok {
		return true
	}
	if _, ok := state.Completed[issueID]; ok {
		return true
	}
	if blocked, ok := state.Blocked[issueID]; ok {
		return dependencyAutoUnblockStartedSignal(blocked.Issue)
	}
	return false
}

func dependencyAutoUnblockStartedSignal(issue connector.Issue) bool {
	if strings.TrimSpace(issue.BranchName) != "" {
		return true
	}
	if issue.PRNumber != nil && *issue.PRNumber > 0 {
		return true
	}
	if issue.PullRequest == nil {
		return false
	}
	return issue.PullRequest.Number > 0 ||
		strings.TrimSpace(issue.PullRequest.URL) != "" ||
		strings.TrimSpace(issue.PullRequest.BranchName) != ""
}

func (o *Orchestrator) applyDependencyAutoUnblock(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	blockers []dependencyBlocker,
	blockerSet string,
	targetState string,
	now time.Time,
) bool {
	issueID := strings.TrimSpace(issue.ID)
	metadata := workflowLaneMetadata{
		DependencyAutoUnblock: &workflowLaneDependencyAutoUnblockMetadata{
			BlockerSet: strings.TrimSpace(blockerSet),
			Blockers:   dependencyAutoUnblockBlockerLabels(blockers),
		},
	}
	if err := o.updateIssueStateByIDStrictWithMetadata(ctx, state, issueID, issue, targetState, now, "dependency_auto_unblock", metadata, laneMutationPreserveOwnership); err != nil {
		if o.logger != nil {
			o.logger.Warn("dependency auto-unblock transition failed", "issue_id", issueID, "identifier", issue.Identifier, "from_state", issue.State, "target_state", targetState, "error", err)
		}
		return false
	}

	body := dependencyAutoUnblockComment(issue.State, targetState, blockers)
	if err := o.connector.CreateComment(ctx, issueID, body); err != nil && o.logger != nil {
		o.logger.Warn("dependency auto-unblock comment failed", "issue_id", issueID, "identifier", issue.Identifier, "target_state", targetState, "error", err)
	}

	delete(state.Blocked, issueID)
	if state.DependencyAutoUnblocks == nil {
		state.DependencyAutoUnblocks = map[string]DependencyAutoUnblockRecord{}
	}
	state.DependencyAutoUnblocks[issueID] = DependencyAutoUnblockRecord{
		BlockerSet:  strings.TrimSpace(blockerSet),
		UnblockedAt: now,
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "dependency_auto_unblock_transition",
		Message: "auto-unblocked " + issueLabel(issue) + " from " + issue.State + " to " + targetState,
	})
	if o.logger != nil {
		o.logger.Info("dependency auto-unblock transition", "issue_id", issueID, "identifier", issue.Identifier, "from_state", issue.State, "target_state", targetState)
	}
	return true
}

func (o *Orchestrator) logDependencyAutoUnblockDecision(
	issue connector.Issue,
	action string,
	reason string,
	blockers []dependencyBlocker,
	targetState string,
) {
	if o.logger == nil {
		return
	}
	attrs := []any{
		"issue_id", strings.TrimSpace(issue.ID),
		"identifier", issue.Identifier,
		"action", action,
		"reason", reason,
		"blockers", dependencyAutoUnblockBlockerLabels(blockers),
		"blocker_sources", dependencyAutoUnblockBlockerSourceLabels(blockers),
	}
	if targetState != "" {
		attrs = append(attrs, "target_state", targetState)
	}
	if action == "error" {
		o.logger.Warn("dependency auto-unblock decision", attrs...)
		return
	}
	o.logger.Info("dependency auto-unblock decision", attrs...)
}

func (o *Orchestrator) logDependencyProseOnly(issue connector.Issue) {
	if o.logger == nil || !dependencyRefsProseOnly(issue.BlockedBy) {
		return
	}
	o.logger.Warn(
		"dependency_prose_only",
		"issue_id", strings.TrimSpace(issue.ID),
		"identifier", issue.Identifier,
		"blockers", dependencyRefLabels(issue.BlockedBy),
	)
}

func dependencyRefsProseOnly(refs []connector.BlockedRef) bool {
	if len(refs) == 0 {
		return false
	}
	for _, ref := range refs {
		if dependencyBlockedRefSource(ref) != connector.BlockedRefSourceProse {
			return false
		}
	}
	return true
}

func dependencyRefLabels(refs []connector.BlockedRef) []string {
	labels := make([]string, 0, len(refs))
	for _, ref := range refs {
		label := firstNonBlank(ref.Identifier, ref.ID)
		if label != "" {
			labels = append(labels, label)
		}
	}
	labels = uniqueStrings(labels)
	slices.Sort(labels)
	return labels
}

func (o *Orchestrator) issueHasStickyBlockReason(ctx context.Context, state *State, issue connector.Issue) bool {
	return o.issueStickyBlockReason(ctx, state, issue) != ""
}

func (o *Orchestrator) issueStickyBlockReason(ctx context.Context, state *State, issue connector.Issue) string {
	issueID := strings.TrimSpace(issue.ID)
	if state != nil && issueID != "" {
		if blocked, ok := state.Blocked[issueID]; ok &&
			blockedEntryMatchesCurrent(issue, blocked.BlockedAt) &&
			stickyBlockReason(blocked.Reason) {
			return strings.TrimSpace(blocked.Reason)
		}
	}
	entry, ok := o.latestWorkflowLaneEntry(ctx, issue)
	if !ok {
		if stickyBlockReason(issue.BlockerReason) {
			return strings.TrimSpace(issue.BlockerReason)
		}
		return ""
	}
	if normalizeState(entry.Event.PhaseName) != normalizeState(blockedStatusState) ||
		!workflowLaneEntryMatchesCurrent(issue, entry.Event) {
		return ""
	}
	if stickyBlockReason(entry.Event.Reason) {
		return strings.TrimSpace(entry.Event.Reason)
	}
	if stickyBlockReason(issue.BlockerReason) {
		return strings.TrimSpace(issue.BlockerReason)
	}
	return ""
}

func blockedEntryMatchesCurrent(issue connector.Issue, enteredAt time.Time) bool {
	if issue.StageUpdatedAt == nil || issue.StageUpdatedAt.IsZero() || enteredAt.IsZero() {
		return true
	}
	return !enteredAt.Before(issue.StageUpdatedAt.Add(-reworkBreakerStageUpdateSkew))
}

func stickyBlockReason(reason string) bool {
	reason = strings.ToLower(strings.TrimSpace(reason))
	if strings.HasPrefix(reason, tokenCeilingBlockedReasonPrefix) ||
		strings.HasPrefix(reason, repeatedFailureBlockedReasonPrefix) ||
		strings.HasPrefix(reason, lifetimeLimitBlockedReasonPrefix) ||
		strings.HasPrefix(reason, artifactGateConvergenceBlockedReasonPrefix) ||
		strings.HasPrefix(reason, mergeWorkerRetryExhaustedReason) {
		return true
	}
	switch reason {
	case "rework_limit",
		string(AutoPromoteReasonMergeRevocationLimit),
		noProgressLimitReason,
		dispatchLoopDetectedReason,
		lifetimeLimitReason,
		spendProgressReason,
		repeatedFailureCircuitBreakerCause,
		"workpad_blocked_unactioned",
		"circuit_breaker",
		"token_ceiling_circuit_breaker",
		deliverableConfigurationFailureCause,
		budgetProjectionCeilingFailureCause,
		artifactGateConvergenceReason,
		terminalAttemptRetryLimitCause,
		workspacePreparationRetryLimitCause:
		return true
	default:
		return false
	}
}

func (o *Orchestrator) dependencyAutoUnblockAlreadyConsumed(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	blockerSet string,
) bool {
	blockerSet = strings.TrimSpace(blockerSet)
	if blockerSet == "" {
		return false
	}
	issueID := strings.TrimSpace(issue.ID)
	if state != nil && issueID != "" {
		if record, ok := state.DependencyAutoUnblocks[issueID]; ok &&
			record.BlockerSet == blockerSet &&
			dependencyAutoUnblockConsumptionCurrent(issue, record.UnblockedAt) {
			return true
		}
	}
	return o.workflowTimelineHasDependencyAutoUnblock(ctx, issue, blockerSet)
}

func (o *Orchestrator) workflowTimelineHasDependencyAutoUnblock(
	ctx context.Context,
	issue connector.Issue,
	blockerSet string,
) bool {
	timeline, ok := o.issueWorkflowTimeline(ctx, issue)
	if !ok {
		return false
	}
	for _, event := range timeline.Events {
		if event.PhaseType != store.WorkflowPhaseTypeLane ||
			!strings.EqualFold(strings.TrimSpace(event.Status), "entered") ||
			!strings.EqualFold(strings.TrimSpace(event.Reason), "dependency_auto_unblock") ||
			!dependencyAutoUnblockConsumptionCurrent(issue, event.StartedAt) {
			continue
		}
		metadata, ok := workflowLaneMetadataFromJSON(event.MetadataJSON)
		if !ok || metadata.DependencyAutoUnblock == nil {
			continue
		}
		if strings.TrimSpace(metadata.DependencyAutoUnblock.BlockerSet) == blockerSet {
			return true
		}
	}
	return false
}

func dependencyAutoUnblockConsumptionCurrent(issue connector.Issue, unblockedAt time.Time) bool {
	if issue.StageUpdatedAt == nil || issue.StageUpdatedAt.IsZero() || unblockedAt.IsZero() {
		return true
	}
	return !unblockedAt.Before(issue.StageUpdatedAt.UTC())
}

type workflowTimelineMetadataMatch struct {
	Event    store.WorkflowPhaseEvent
	Metadata workflowLaneMetadata
}

func (o *Orchestrator) workflowTimelineLaneActionSignature(
	ctx context.Context,
	issue connector.Issue,
	reason string,
	action string,
	signature string,
) (workflowTimelineMetadataMatch, bool) {
	return o.workflowTimelineLaneMetadataMatch(ctx, issue, reason, func(metadata workflowLaneMetadata) bool {
		return workflowLaneMetadataHasActionSignature(metadata, action, signature)
	})
}

func (o *Orchestrator) workflowTimelineActionSignature(
	ctx context.Context,
	issue connector.Issue,
	action string,
	signature string,
) (workflowTimelineMetadataMatch, bool) {
	timeline, ok := o.issueWorkflowTimeline(ctx, issue)
	if !ok {
		return workflowTimelineMetadataMatch{}, false
	}
	for _, event := range timeline.Events {
		metadata, ok := workflowLaneMetadataFromJSON(event.MetadataJSON)
		if !ok {
			continue
		}
		if workflowLaneMetadataHasActionSignature(metadata, action, signature) {
			return workflowTimelineMetadataMatch{Event: event, Metadata: metadata}, true
		}
	}
	return workflowTimelineMetadataMatch{}, false
}

func (o *Orchestrator) latestWorkflowLaneEntry(
	ctx context.Context,
	issue connector.Issue,
) (workflowTimelineMetadataMatch, bool) {
	timeline, ok := o.issueWorkflowTimeline(ctx, issue)
	if !ok {
		return workflowTimelineMetadataMatch{}, false
	}
	for index := len(timeline.Events) - 1; index >= 0; index-- {
		event := timeline.Events[index]
		if event.PhaseType != store.WorkflowPhaseTypeLane {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(event.Status), "entered") {
			continue
		}
		metadata, _ := workflowLaneMetadataFromJSON(event.MetadataJSON)
		return workflowTimelineMetadataMatch{Event: event, Metadata: metadata}, true
	}
	return workflowTimelineMetadataMatch{}, false
}

func (o *Orchestrator) workflowTimelineLaneMetadataMatch(
	ctx context.Context,
	issue connector.Issue,
	reason string,
	matches func(workflowLaneMetadata) bool,
) (workflowTimelineMetadataMatch, bool) {
	timeline, ok := o.issueWorkflowTimeline(ctx, issue)
	if !ok {
		return workflowTimelineMetadataMatch{}, false
	}
	reason = strings.TrimSpace(reason)
	for _, event := range timeline.Events {
		if event.PhaseType != store.WorkflowPhaseTypeLane {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(event.Status), "entered") {
			continue
		}
		if reason != "" && !strings.EqualFold(strings.TrimSpace(event.Reason), reason) {
			continue
		}
		metadata, ok := workflowLaneMetadataFromJSON(event.MetadataJSON)
		if !ok {
			continue
		}
		if matches(metadata) {
			return workflowTimelineMetadataMatch{Event: event, Metadata: metadata}, true
		}
	}
	return workflowTimelineMetadataMatch{}, false
}

func (o *Orchestrator) latestWorkflowLaneReason(ctx context.Context, issue connector.Issue, stateName string) (string, bool) {
	timeline, ok := o.issueWorkflowTimeline(ctx, issue)
	if !ok {
		return "", false
	}
	stateName = normalizeState(stateName)
	for index := len(timeline.Events) - 1; index >= 0; index-- {
		event := timeline.Events[index]
		if event.PhaseType != store.WorkflowPhaseTypeLane {
			continue
		}
		if normalizeState(event.PhaseName) != stateName {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(event.Status), "entered") {
			continue
		}
		return strings.TrimSpace(event.Reason), true
	}
	return "", false
}

func (o *Orchestrator) issueWorkflowTimeline(ctx context.Context, issue connector.Issue) (store.WorkflowTimeline, bool) {
	reader, ok := o.workflowMetrics.(WorkflowMetricsTimelineReader)
	if !ok || reader == nil {
		return store.WorkflowTimeline{}, false
	}
	timeline, err := reader.IssueWorkflowTimeline(ctx, store.IssueIdentity{
		ProjectID:  o.workflowMetricsProjectID(),
		IssueID:    issue.ID,
		Identifier: issue.Identifier,
		IssueURL:   issue.URL,
	})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("workflow timeline read failed", "issue_id", strings.TrimSpace(issue.ID), "identifier", issue.Identifier, "error", err)
		}
		return store.WorkflowTimeline{}, false
	}
	return timeline, true
}

func dependencyAutoUnblockBlockerSet(blockers []dependencyBlocker) string {
	keys := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		key := dependencyAutoUnblockBlockerKey(blocker)
		if key != "" {
			keys = append(keys, key)
		}
	}
	keys = uniqueStrings(keys)
	slices.Sort(keys)
	return strings.Join(keys, "\n")
}

func dependencyAutoUnblockBlockerSignature(
	blockers []dependencyBlocker,
	cfg DependencyAutoUnblockConfig,
	terminalStates []string,
) string {
	keys := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		key := dependencyAutoUnblockBlockerKey(blocker)
		if key == "" {
			continue
		}
		state := blocker.Ref.State
		closed := false
		pullRequestState := ""
		stateUpdatedAt := ""
		if blocker.Resolved {
			state = firstNonBlank(blocker.Issue.State, state)
			closed = blocker.Issue.Closed
			if blocker.Issue.StageUpdatedAt != nil && !blocker.Issue.StageUpdatedAt.IsZero() {
				stateUpdatedAt = blocker.Issue.StageUpdatedAt.UTC().Format(time.RFC3339Nano)
			}
			if blocker.Issue.PullRequest != nil {
				pullRequestState = blocker.Issue.PullRequest.State
			}
		}
		keys = append(keys, fmt.Sprintf(
			"%s|resolved=%t|ready=%t|closed=%t|state=%s|state_updated_at=%s|pull_request=%s",
			key,
			blocker.Resolved,
			dependencyBlockerReady(blocker, cfg, terminalStates),
			closed,
			normalizeState(state),
			stateUpdatedAt,
			normalizePullRequestState(pullRequestState),
		))
	}
	keys = uniqueStrings(keys)
	slices.Sort(keys)
	return strings.Join(keys, "\n")
}

func dependencyAutoUnblockBlockerKey(blocker dependencyBlocker) string {
	key := firstNonBlank(blocker.Ref.Identifier, blocker.Ref.ID, blocker.Issue.Identifier, blocker.Issue.ID)
	return strings.ToLower(strings.TrimSpace(key))
}

func dependencyAutoUnblockBlockerLabels(blockers []dependencyBlocker) []string {
	labels := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if label := dependencyBlockerLabel(blocker); label != "" {
			labels = append(labels, label)
		}
	}
	labels = uniqueStrings(labels)
	slices.Sort(labels)
	return labels
}

func dependencyAutoUnblockBlockerSourceLabels(blockers []dependencyBlocker) []string {
	labels := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		label := dependencyBlockerLabel(blocker)
		if label == "" {
			continue
		}
		labels = append(labels, label+" source="+dependencyBlockedRefSource(blocker.Ref))
	}
	labels = uniqueStrings(labels)
	slices.Sort(labels)
	return labels
}

func dependencyBlockedRefSource(ref connector.BlockedRef) string {
	switch strings.ToLower(strings.TrimSpace(ref.Source)) {
	case connector.BlockedRefSourceNative:
		return connector.BlockedRefSourceNative
	case connector.BlockedRefSourceWorkpad:
		return connector.BlockedRefSourceWorkpad
	case "", connector.BlockedRefSourceProse:
		return connector.BlockedRefSourceProse
	default:
		return "unknown"
	}
}

func dependencyAutoUnblockComment(sourceState string, targetState string, blockers []dependencyBlocker) string {
	var b strings.Builder
	b.WriteString("Dependency blockers cleared.")
	if strings.TrimSpace(sourceState) != "" && strings.TrimSpace(targetState) != "" {
		b.WriteString(" Moved this issue from ")
		b.WriteString(strings.TrimSpace(sourceState))
		b.WriteString(" to ")
		b.WriteString(strings.TrimSpace(targetState))
		b.WriteString(".")
	}
	b.WriteString("\n\nCleared dependencies:")
	for _, blocker := range blockers {
		b.WriteString("\n- ")
		b.WriteString(dependencyBlockerLabel(blocker))
		if state := dependencyBlockerState(blocker); state != "" {
			b.WriteString(" (state: ")
			b.WriteString(state)
			b.WriteString(")")
		}
		if pullRequest := dependencyBlockerPullRequest(blocker); pullRequest != nil && pullRequestMerged(pullRequest) {
			b.WriteString(fmt.Sprintf(" (merged PR: #%d)", pullRequest.Number))
		}
	}
	return b.String()
}

func dependencyBlockerLabel(blocker dependencyBlocker) string {
	if blocker.Resolved {
		if identifier := strings.TrimSpace(blocker.Issue.Identifier); identifier != "" {
			return identifier
		}
		if id := strings.TrimSpace(blocker.Issue.ID); id != "" {
			return id
		}
	}
	if identifier := strings.TrimSpace(blocker.Ref.Identifier); identifier != "" {
		return identifier
	}
	if id := strings.TrimSpace(blocker.Ref.ID); id != "" {
		return id
	}
	return "unknown dependency"
}

func dependencyBlockerState(blocker dependencyBlocker) string {
	if blocker.Resolved {
		return strings.TrimSpace(blocker.Issue.State)
	}
	return strings.TrimSpace(blocker.Ref.State)
}

func dependencyBlockerPullRequest(blocker dependencyBlocker) *connector.PullRequest {
	if !blocker.Resolved {
		return nil
	}
	return blocker.Issue.PullRequest
}

func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func defaultStringSlice(values []string, fallback []string) []string {
	if len(values) == 0 {
		return append([]string(nil), fallback...)
	}
	return append([]string(nil), values...)
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
