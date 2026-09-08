package web

import (
	"context"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	admissionmodel "github.com/digitaldrywood/detent/internal/admission/model"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const (
	budgetHistoryWindowDays                = 7
	budgetSpendQueryFailedReason           = "budget spend query failed"
	dailySpendRegressionThresholdPercent   = 90.0
	dailySpendRegressionMinimumBaselineUSD = 1.0
	dailySpendRegressionMinimumElapsed     = 6 * time.Hour
)

type configuredBudget struct {
	ProjectID      string
	PerDayMaxUSD   float64
	PerIssueMaxUSD float64
}

type snapshotEnrichmentCache struct {
	mu       sync.Mutex
	cond     *sync.Cond
	cached   bool
	raw      telemetry.Snapshot
	enriched telemetry.Snapshot
	loading  bool
	pending  telemetry.Snapshot
	revision int64
	loadedAt time.Time
}

type spendRegressionMonitor struct {
	mu         sync.Mutex
	warnedDate string
}

func newSpendRegressionMonitor() *spendRegressionMonitor {
	return &spendRegressionMonitor{}
}

func newSnapshotEnrichmentCache() *snapshotEnrichmentCache {
	cache := &snapshotEnrichmentCache{}
	cache.cond = sync.NewCond(&cache.mu)
	return cache
}

func (s *Server) cachedEnrichedSnapshot(ctx context.Context, snapshot telemetry.Snapshot) telemetry.Snapshot {
	revision, err := workflowHistoryRevision(ctx, s.store)
	if err != nil {
		return s.enrichSnapshot(ctx, snapshot)
	}
	return s.snapshots.enrichVersion(ctx, snapshot, revision, s.now(), s.enrichSnapshot)
}

func (c *snapshotEnrichmentCache) get(snapshot telemetry.Snapshot) (telemetry.Snapshot, bool) {
	enriched, cached, _, _ := c.lookup(snapshot)
	return enriched, cached
}

func (c *snapshotEnrichmentCache) lookup(snapshot telemetry.Snapshot) (telemetry.Snapshot, bool, time.Time, bool) {
	if c == nil {
		return telemetry.Snapshot{}, false, time.Time{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	lastCompletedAt := time.Time{}
	if c.cached {
		lastCompletedAt = c.raw.GeneratedAt
	}
	if !c.cached || !sameSnapshotForEnrichment(c.raw, snapshot) {
		return telemetry.Snapshot{}, false, lastCompletedAt, c.loading
	}
	return c.enriched, true, lastCompletedAt, c.loading
}

func sameSnapshotForEnrichment(left telemetry.Snapshot, right telemetry.Snapshot) bool {
	if left.Seq != right.Seq {
		return false
	}
	if !left.GeneratedAt.Equal(right.GeneratedAt) {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func newerEnrichmentSnapshot(left, right telemetry.Snapshot) bool {
	return left.Seq > right.Seq && left.Project.ID == right.Project.ID && !left.GeneratedAt.Before(right.GeneratedAt)
}

func (c *snapshotEnrichmentCache) enrich(ctx context.Context, snapshot telemetry.Snapshot, enrich func(context.Context, telemetry.Snapshot) telemetry.Snapshot) telemetry.Snapshot {
	return c.enrichVersion(ctx, snapshot, 0, time.Now(), enrich)
}

func (c *snapshotEnrichmentCache) enrichVersion(ctx context.Context, snapshot telemetry.Snapshot, revision int64, now time.Time, enrich func(context.Context, telemetry.Snapshot) telemetry.Snapshot) telemetry.Snapshot {
	if c == nil {
		return enrich(ctx, snapshot)
	}

	c.mu.Lock()
	if !c.loading || newerEnrichmentSnapshot(snapshot, c.pending) {
		c.pending = snapshot
	}
	for {
		if ctx != nil && ctx.Err() != nil {
			c.mu.Unlock()
			return snapshot
		}
		if newerEnrichmentSnapshot(c.pending, snapshot) {
			snapshot = c.pending
		}

		if c.cached && c.revision == revision && workflowHistoryFresh(c.loadedAt, now) && (sameSnapshotForEnrichment(c.raw, snapshot) || newerEnrichmentSnapshot(c.raw, snapshot)) {
			enriched := c.enriched
			c.mu.Unlock()
			return enriched
		}
		if !c.loading {
			c.loading = true
			break
		}
		c.cond.Wait()
	}
	c.mu.Unlock()

	enriched := enrich(ctx, snapshot)

	if ctx != nil && ctx.Err() != nil {
		c.mu.Lock()
		c.loading = false
		c.cond.Broadcast()
		c.mu.Unlock()
		return enriched
	}

	c.mu.Lock()
	c.revision = revision
	c.loadedAt = now
	c.raw = snapshot
	c.enriched = enriched
	c.cached = true
	c.loading = false
	c.cond.Broadcast()
	c.mu.Unlock()

	return enriched
}

func (s *Server) enrichSnapshot(ctx context.Context, snapshot telemetry.Snapshot) telemetry.Snapshot {
	snapshot.AdmissionProposals = s.snapshotAdmissionProposals(ctx, snapshot.GeneratedAt)
	snapshot = s.snapshotParkSummaries(ctx, snapshot)
	if cycleTime, ok := s.snapshotCycleTime(ctx); ok {
		snapshot.CycleTime = cycleTime
	}
	snapshot.WorkflowMetrics = s.snapshotWorkflowMetrics(ctx, snapshot)
	snapshot.Concurrency = s.snapshotConcurrency(ctx, snapshot)

	budget, ok := s.snapshotBudget(ctx, snapshot.GeneratedAt)
	if !ok {
		return snapshot
	}
	if budget.DegradedReason != "" {
		snapshot.Budget = degradedSnapshotBudget(snapshot.Budget, budget)
		return snapshot
	}

	budget.ProjectedCostUSD = snapshot.Budget.ProjectedCostUSD
	budget.Refusals = append([]telemetry.BudgetRefusal(nil), snapshot.Budget.Refusals...)
	snapshot.Budget = budget
	return snapshot
}

func (s *Server) snapshotParkSummaries(ctx context.Context, snapshot telemetry.Snapshot) telemetry.Snapshot {
	if s.store == nil || len(snapshot.BoardIssues) == 0 {
		return snapshot
	}
	reader, ok := s.store.(store.ParkSummaryStore)
	if !ok {
		return snapshot
	}
	identities := make([]store.IssueIdentity, 0, len(snapshot.BoardIssues))
	for _, issue := range snapshot.BoardIssues {
		identities = append(identities, boardIssueParkIdentity(issue))
	}
	summaries, err := reader.IssueParkSummaries(ctx, identities)
	if err != nil {
		s.logger.WarnContext(ctx, "issue park summary query failed", slog.Any("error", err))
		return snapshot
	}
	snapshot.BoardIssues = append([]telemetry.Issue(nil), snapshot.BoardIssues...)
	for index := range snapshot.BoardIssues {
		if summary, ok := summaries[identities[index]]; ok {
			snapshot.BoardIssues[index].ParkSummary = parkSummaryTelemetry(summary)
		}
	}
	return snapshot
}

func boardIssueParkIdentity(issue telemetry.Issue) store.IssueIdentity {
	return store.IssueIdentity{
		ProjectID:  strings.TrimSpace(issue.ProjectID),
		IssueID:    strings.TrimSpace(issue.ID),
		Identifier: strings.TrimSpace(issue.Identifier),
		IssueURL:   strings.TrimSpace(issue.URL),
	}
}

func parkSummaryTelemetry(summary store.ParkSummary) telemetry.ParkSummary {
	causes := make([]telemetry.ParkCauseSummary, 0, len(summary.Causes))
	for _, cause := range summary.Causes {
		causes = append(causes, telemetry.ParkCauseSummary{Cause: cause.Cause, Count: cause.Count, FirstAt: cause.FirstAt, LastAt: cause.LastAt})
	}
	return telemetry.ParkSummary{
		AttemptCount:             summary.AttemptCount,
		ParkCount:                summary.ParkCount,
		AcknowledgedParkSequence: summary.AcknowledgedParkSequence,
		AcknowledgedAt:           summary.AcknowledgedAt,
		Causes:                   causes,
		Tokens: telemetry.ParkTokenTotals{
			InputTokens:           summary.Tokens.InputTokens,
			CachedInputTokens:     summary.Tokens.CachedInputTokens,
			OutputTokens:          summary.Tokens.OutputTokens,
			ReasoningOutputTokens: summary.Tokens.ReasoningOutputTokens,
		},
	}
}

func (s *Server) snapshotAdmissionProposals(ctx context.Context, observedAt time.Time) []telemetry.AdmissionProposal {
	if s.store == nil {
		return nil
	}
	reader, ok := s.store.(store.AdmissionProposalDecisionReader)
	if !ok {
		return nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	proposals, err := reader.AdmissionProposalsAwaitingDecision(ctx, "", observedAt)
	if err != nil {
		s.logger.WarnContext(ctx, "admission proposals awaiting decision query failed", slog.Any("error", err))
		return nil
	}
	out := make([]telemetry.AdmissionProposal, 0, len(proposals))
	for _, proposal := range proposals {
		out = append(out, admissionProposalTelemetry(proposal))
	}
	return out
}

func admissionProposalTelemetry(proposal admissionmodel.Proposal) telemetry.AdmissionProposal {
	return telemetry.AdmissionProposal{
		ID:              proposal.ID,
		ProjectID:       proposal.ProjectID,
		IssueID:         proposal.IssueID,
		IssueIdentifier: proposal.IssueIdentifier,
		IssueURL:        proposal.IssueURL,
		Confidence:      proposal.Confidence,
		CreatedAt:       proposal.CreatedAt,
		ExpiresAt:       proposal.ExpiresAt,
	}
}

func (s *Server) snapshotBudget(ctx context.Context, now time.Time) (telemetry.Budget, bool) {
	projects := s.configuredBudgets()
	projectIDs := s.configuredProjectIDs()
	if len(projectIDs) == 0 {
		return telemetry.Budget{}, false
	}
	enabled := len(projects) > 0
	if now.IsZero() {
		now = time.Now().UTC().Truncate(time.Second)
	}
	now = now.UTC()
	periodStart, periodEnd := dailyBudgetPeriod(now)
	queryFrom := periodStart.AddDate(0, 0, -(budgetHistoryWindowDays - 1))

	events, err := s.store.BudgetCostEvents(ctx, store.BudgetCostQuery{
		ProjectIDs: projectIDs,
		From:       queryFrom,
		To:         periodEnd,
	})
	if err != nil {
		s.logger.Warn("budget spend query failed", slog.Any("error", err))
		return telemetry.Budget{
			Enabled:        enabled,
			DegradedReason: budgetSpendQueryFailedReason,
			PerDayMaxUSD:   positiveFloatPtr(totalDailyBudgetCap(projects)),
			PerIssueMaxUSD: issueBudgetCap(projects),
			PeriodStart:    periodStart,
			PeriodEnd:      periodEnd,
		}, true
	}

	points, currentSpend := currentBudgetSpendPoints(events, periodStart, periodEnd)
	budget := telemetry.Budget{
		Enabled:           enabled,
		PerDayMaxUSD:      positiveFloatPtr(totalDailyBudgetCap(projects)),
		PerIssueMaxUSD:    issueBudgetCap(projects),
		CurrentSpendUSD:   currentSpend,
		ProjectedSpendUSD: projectedBudgetSpend(periodStart, periodEnd, now, currentSpend),
		PeriodStart:       periodStart,
		PeriodEnd:         periodEnd,
		SpendPoints:       points,
		Days:              budgetSpendDays(events),
	}
	budget.SpendRegression = budgetSpendRegression(budget, now)
	s.logSpendRegression(budget.SpendRegression)
	return budget, true
}

func budgetSpendRegression(budget telemetry.Budget, now time.Time) *telemetry.SpendRegression {
	periodStart := budget.PeriodStart.UTC()
	if periodStart.IsZero() || now.UTC().Sub(periodStart) < dailySpendRegressionMinimumElapsed {
		return nil
	}
	previousDate := periodStart.AddDate(0, 0, -1).Format("2006-01-02")
	previousSpend := 0.0
	for _, day := range budget.Days {
		if day.Date == previousDate {
			previousSpend = day.SpendUSD
			break
		}
	}
	if previousSpend < dailySpendRegressionMinimumBaselineUSD {
		return nil
	}
	projectedSpend := max(0, budget.ProjectedSpendUSD)
	dropPercent := (previousSpend - projectedSpend) / previousSpend * 100
	if dropPercent < dailySpendRegressionThresholdPercent {
		return nil
	}
	return &telemetry.SpendRegression{
		Date:              periodStart.Format("2006-01-02"),
		PreviousSpendUSD:  previousSpend,
		ProjectedSpendUSD: projectedSpend,
		DropPercent:       dropPercent,
		ThresholdPercent:  dailySpendRegressionThresholdPercent,
	}
}

func (s *Server) logSpendRegression(regression *telemetry.SpendRegression) {
	if s == nil || s.logger == nil || s.spendRegressions == nil || regression == nil {
		return
	}
	s.spendRegressions.mu.Lock()
	if s.spendRegressions.warnedDate == regression.Date {
		s.spendRegressions.mu.Unlock()
		return
	}
	s.spendRegressions.warnedDate = regression.Date
	s.spendRegressions.mu.Unlock()
	s.logger.Warn("fleet daily spend regression",
		slog.String("event", "fleet_daily_spend_regression"),
		slog.String("date", regression.Date),
		slog.Float64("previous_spend_usd", regression.PreviousSpendUSD),
		slog.Float64("projected_spend_usd", regression.ProjectedSpendUSD),
		slog.Float64("drop_percent", regression.DropPercent),
		slog.Float64("threshold_percent", regression.ThresholdPercent),
	)
}

func degradedSnapshotBudget(snapshotBudget telemetry.Budget, degradedBudget telemetry.Budget) telemetry.Budget {
	if !snapshotBudget.Enabled {
		snapshotBudget.Enabled = degradedBudget.Enabled
	}
	if snapshotBudget.PerDayMaxUSD == nil {
		snapshotBudget.PerDayMaxUSD = degradedBudget.PerDayMaxUSD
	}
	if snapshotBudget.PerIssueMaxUSD == nil {
		snapshotBudget.PerIssueMaxUSD = degradedBudget.PerIssueMaxUSD
	}
	if snapshotBudget.PeriodStart.IsZero() {
		snapshotBudget.PeriodStart = degradedBudget.PeriodStart
	}
	if snapshotBudget.PeriodEnd.IsZero() {
		snapshotBudget.PeriodEnd = degradedBudget.PeriodEnd
	}
	snapshotBudget.DegradedReason = degradedBudget.DegradedReason
	return snapshotBudget
}

func (s *Server) configuredBudgets() []configuredBudget {
	if s.registry == nil {
		return nil
	}

	projects := s.registry.List()
	budgets := make([]configuredBudget, 0, len(projects))
	for _, project := range projects {
		if project == nil {
			continue
		}
		workflow := project.Workflow()
		cfg := workflow.Config.Budget
		if !cfg.Enabled {
			continue
		}
		projectID := strings.TrimSpace(string(project.ID()))
		if projectID == "" {
			continue
		}
		budgets = append(budgets, configuredBudget{
			ProjectID:      projectID,
			PerDayMaxUSD:   cfg.PerDayMaxUSD,
			PerIssueMaxUSD: cfg.PerIssueMaxUSD,
		})
	}
	return budgets
}

func (s *Server) configuredProjectIDs() []string {
	if s == nil || s.registry == nil {
		return nil
	}
	projects := s.registry.List()
	ids := make([]string, 0, len(projects))
	for _, project := range projects {
		if project == nil {
			continue
		}
		if projectID := strings.TrimSpace(string(project.ID())); projectID != "" {
			ids = append(ids, projectID)
		}
	}
	return ids
}

func dailyBudgetPeriod(now time.Time) (time.Time, time.Time) {
	year, month, day := now.UTC().Date()
	start := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 0, 1)
}

func totalDailyBudgetCap(projects []configuredBudget) float64 {
	total := 0.0
	for _, project := range projects {
		if project.PerDayMaxUSD > 0 {
			total += project.PerDayMaxUSD
		}
	}
	return total
}

func issueBudgetCap(projects []configuredBudget) *float64 {
	if len(projects) != 1 || projects[0].PerIssueMaxUSD <= 0 {
		return nil
	}
	value := projects[0].PerIssueMaxUSD
	return &value
}

func currentBudgetSpendPoints(events []store.BudgetCostEvent, periodStart time.Time, periodEnd time.Time) ([]telemetry.BudgetSpendPoint, float64) {
	periodEvents := make([]store.BudgetCostEvent, 0, len(events))
	for _, event := range events {
		at := event.At.UTC()
		if at.IsZero() || at.Before(periodStart) || !at.Before(periodEnd) {
			continue
		}
		periodEvents = append(periodEvents, store.BudgetCostEvent{
			ProjectID: event.ProjectID,
			At:        at,
			CostUSD:   event.CostUSD,
		})
	}
	sort.SliceStable(periodEvents, func(i, j int) bool {
		return periodEvents[i].At.Before(periodEvents[j].At)
	})

	points := make([]telemetry.BudgetSpendPoint, 0, len(periodEvents))
	total := 0.0
	for _, event := range periodEvents {
		if event.CostUSD <= 0 {
			continue
		}
		total += event.CostUSD
		points = append(points, telemetry.BudgetSpendPoint{
			At:       event.At,
			SpendUSD: total,
		})
	}
	return points, total
}

func budgetSpendDays(events []store.BudgetCostEvent) []telemetry.BudgetDay {
	byDay := map[string]float64{}
	for _, event := range events {
		if event.CostUSD <= 0 || event.At.IsZero() {
			continue
		}
		day := event.At.UTC().Format("2006-01-02")
		byDay[day] += event.CostUSD
	}
	if len(byDay) == 0 {
		return nil
	}

	days := make([]string, 0, len(byDay))
	for day := range byDay {
		days = append(days, day)
	}
	sort.Strings(days)
	if len(days) > budgetHistoryWindowDays {
		days = days[len(days)-budgetHistoryWindowDays:]
	}

	out := make([]telemetry.BudgetDay, 0, len(days))
	for _, day := range days {
		out = append(out, telemetry.BudgetDay{
			Date:     day,
			SpendUSD: byDay[day],
		})
	}
	return out
}

func projectedBudgetSpend(periodStart time.Time, periodEnd time.Time, now time.Time, currentSpend float64) float64 {
	if currentSpend <= 0 {
		return 0
	}
	if periodStart.IsZero() || !periodEnd.After(periodStart) {
		return currentSpend
	}
	elapsed := now.Sub(periodStart).Seconds()
	if elapsed <= 0 {
		return currentSpend
	}
	total := periodEnd.Sub(periodStart).Seconds()
	if total <= 0 {
		return currentSpend
	}
	projected := currentSpend * total / elapsed
	if projected < currentSpend {
		return currentSpend
	}
	return projected
}

func positiveFloatPtr(value float64) *float64 {
	if value <= 0 {
		return nil
	}
	return &value
}
