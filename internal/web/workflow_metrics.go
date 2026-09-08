package web

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const maxWorkflowOldestCards = 8

func (s *Server) snapshotWorkflowMetrics(ctx context.Context, snapshot telemetry.Snapshot) telemetry.WorkflowMetrics {
	if s.store == nil {
		return telemetry.WorkflowMetrics{
			RuntimeStore: telemetry.RuntimeStoreEvidence{
				Backend: "none",
				Status:  "not_configured",
			},
		}
	}

	now := snapshot.GeneratedAt
	if now.IsZero() {
		now = time.Now()
	}

	out := s.workflowHistory.get(ctx, s.store, strings.TrimSpace(snapshot.Project.ID), now, s.now, s.loadWorkflowHistory)
	out.OldestCards = workflowOldestCards(snapshot)
	out.ActiveBottleneck = workflowActiveBottleneck(snapshot, now)
	return out
}

func (s *Server) loadWorkflowHistory(ctx context.Context, projectID string, now time.Time) telemetry.WorkflowMetrics {
	runtimeEvidence, err := s.store.RuntimeEvidence(ctx, store.RuntimeEvidenceQuery{ProjectID: projectID})
	if err != nil {
		s.logger.Warn("runtime store evidence query failed", slog.Any("error", err))
		return telemetry.WorkflowMetrics{
			DegradedReason: "runtime store evidence query failed",
			RuntimeStore: telemetry.RuntimeStoreEvidence{
				Status: "degraded",
			},
		}
	}
	windows := []struct {
		label    string
		duration time.Duration
	}{
		{label: "24h", duration: 24 * time.Hour},
		{label: "7d", duration: 7 * 24 * time.Hour},
		{label: "30d", duration: 30 * 24 * time.Hour},
	}

	out := telemetry.WorkflowMetrics{
		Available:    true,
		RuntimeStore: runtimeStoreEvidenceFromStore(runtimeEvidence),
	}
	for _, window := range windows {
		from := now.Add(-window.duration)
		previousFrom := from.Add(-window.duration)
		report, err := s.store.WorkflowMetricsReport(ctx, store.WorkflowMetricsQuery{
			ProjectID: projectID,
			From:      from,
			To:        now,
		})
		if err != nil {
			s.logger.Warn("workflow metrics report failed", slog.Any("error", err))
			return telemetry.WorkflowMetrics{DegradedReason: "workflow metrics query failed", RuntimeStore: out.RuntimeStore}
		}
		previousReport, err := s.store.WorkflowMetricsReport(ctx, store.WorkflowMetricsQuery{
			ProjectID: projectID,
			From:      previousFrom,
			To:        from,
		})
		if err != nil {
			s.logger.Warn("workflow metrics previous report failed", slog.Any("error", err))
			return telemetry.WorkflowMetrics{DegradedReason: "workflow metrics query failed", RuntimeStore: out.RuntimeStore}
		}

		lanes := workflowPhaseMetricsFromStore(report.Lanes)
		workflowAttachLaneComparisons(lanes, workflowPhaseMetricsFromStore(previousReport.Lanes), window.label, previousFrom, from)
		workflowMarkBottleneckLane(lanes)
		out.Windows = append(out.Windows, telemetry.WorkflowMetricsWindow{
			Label:      window.label,
			From:       from,
			To:         now,
			Lanes:      lanes,
			SubPhases:  workflowPhaseMetricsFromStore(report.SubPhases),
			LaneTrends: workflowLaneTrendsFromStore(report.LaneTrends),
		})
	}
	return out
}

func runtimeStoreEvidenceFromStore(evidence store.RuntimeEvidence) telemetry.RuntimeStoreEvidence {
	out := telemetry.RuntimeStoreEvidence{
		Backend:             string(evidence.Backend),
		Status:              "degraded",
		Healthy:             evidence.Healthy,
		Path:                strings.TrimSpace(evidence.Path),
		MigrationStatus:     strings.TrimSpace(evidence.MigrationStatus),
		MigrationVersion:    evidence.MigrationVersion,
		WorkflowPhaseEvents: runtimeWorkflowPhaseEventsFromStore(evidence.WorkflowPhaseEvents),
	}
	if evidence.Healthy {
		out.Status = "healthy"
	}
	out.Tables = make([]telemetry.RuntimeStoreTableEvidence, 0, len(evidence.Tables))
	for _, table := range evidence.Tables {
		out.Tables = append(out.Tables, telemetry.RuntimeStoreTableEvidence{
			Name:     strings.TrimSpace(table.Name),
			Scope:    strings.TrimSpace(table.Scope),
			RowCount: table.RowCount,
		})
	}
	return out
}

func runtimeWorkflowPhaseEventsFromStore(evidence store.RuntimeWorkflowPhaseEventEvidence) telemetry.RuntimeStoreWorkflowPhaseEvents {
	return telemetry.RuntimeStoreWorkflowPhaseEvents{
		RowCount:         evidence.RowCount,
		OldestFinishedAt: evidence.OldestFinishedAt,
		NewestFinishedAt: evidence.NewestFinishedAt,
	}
}

func workflowPhaseMetricsFromStore(metrics []store.WorkflowPhaseMetric) []telemetry.WorkflowPhaseMetric {
	out := make([]telemetry.WorkflowPhaseMetric, 0, len(metrics))
	for _, metric := range metrics {
		out = append(out, telemetry.WorkflowPhaseMetric{
			ProjectID:             metric.ProjectID,
			PhaseType:             metric.PhaseType,
			PhaseName:             metric.PhaseName,
			Count:                 metric.Count,
			TotalSeconds:          metric.TotalSeconds,
			AverageSeconds:        metric.AverageSeconds,
			P50Seconds:            metric.P50Seconds,
			P90Seconds:            metric.P90Seconds,
			P95Seconds:            metric.P95Seconds,
			InputTokens:           metric.InputTokens,
			CachedInputTokens:     metric.CachedInputTokens,
			OutputTokens:          metric.OutputTokens,
			ReasoningOutputTokens: metric.ReasoningOutputTokens,
			TotalTokens:           metric.TotalTokens,
			Turns:                 metric.Turns,
			EndpointFamily:        metric.EndpointFamily,
			ActiveSeconds:         metric.ActiveSeconds,
			WaitSeconds:           metric.WaitSeconds,
			ActivePercent:         metric.ActivePercent,
			Representatives:       workflowRepresentativeRunsFromStore(metric.Representatives),
		})
	}
	return out
}

func workflowRepresentativeRunsFromStore(representatives []store.WorkflowRepresentativeRun) []telemetry.WorkflowRepresentativeRun {
	out := make([]telemetry.WorkflowRepresentativeRun, 0, len(representatives))
	for _, representative := range representatives {
		out = append(out, telemetry.WorkflowRepresentativeRun{
			RunID:      representative.RunID,
			SessionID:  representative.SessionID,
			IssueID:    representative.IssueID,
			Identifier: representative.Identifier,
			IssueURL:   representative.IssueURL,
			FinishedAt: representative.FinishedAt,
		})
	}
	return out
}

func workflowLaneTrendsFromStore(trends []store.WorkflowLaneTrend) []telemetry.WorkflowLaneTrend {
	out := make([]telemetry.WorkflowLaneTrend, 0, len(trends))
	for _, trend := range trends {
		points := make([]telemetry.WorkflowLaneTrendPoint, 0, len(trend.Points))
		for _, point := range trend.Points {
			points = append(points, telemetry.WorkflowLaneTrendPoint{
				Label:          point.Label,
				BucketEnd:      point.BucketEnd,
				Count:          point.Count,
				AverageSeconds: point.AverageSeconds,
			})
		}
		out = append(out, telemetry.WorkflowLaneTrend{
			ProjectID:  trend.ProjectID,
			PhaseName:  trend.PhaseName,
			Points:     points,
			TotalCount: trend.TotalCount,
		})
	}
	return out
}

func workflowAttachLaneComparisons(lanes []telemetry.WorkflowPhaseMetric, previous []telemetry.WorkflowPhaseMetric, label string, previousFrom time.Time, previousTo time.Time) {
	previousByKey := make(map[string]telemetry.WorkflowPhaseMetric, len(previous))
	for _, metric := range previous {
		previousByKey[workflowLaneMetricKey(metric)] = metric
	}
	comparisonLabel := label + " vs previous " + label
	for i := range lanes {
		previousMetric, ok := previousByKey[workflowLaneMetricKey(lanes[i])]
		comparison := telemetry.WorkflowMetricComparison{
			Label:        comparisonLabel,
			PreviousFrom: previousFrom,
			PreviousTo:   previousTo,
			Direction:    "insufficient_history",
		}
		if ok && previousMetric.Count > 0 {
			comparison.PreviousCount = previousMetric.Count
			comparison.PreviousAverageSeconds = previousMetric.AverageSeconds
			comparison.DeltaSeconds = lanes[i].AverageSeconds - previousMetric.AverageSeconds
			comparison.DeltaPercent = workflowMetricDeltaPercent(lanes[i].AverageSeconds, previousMetric.AverageSeconds)
			comparison.Direction = workflowMetricTrendDirection(comparison.DeltaSeconds)
		}
		lanes[i].Comparison = &comparison
	}
}

func workflowLaneMetricKey(metric telemetry.WorkflowPhaseMetric) string {
	return strings.Join([]string{
		strings.TrimSpace(metric.ProjectID),
		strings.TrimSpace(metric.PhaseType),
		strings.TrimSpace(metric.PhaseName),
	}, "\x00")
}

func workflowMetricDeltaPercent(currentAverage int64, previousAverage int64) float64 {
	if previousAverage <= 0 {
		return 0
	}
	return float64(currentAverage-previousAverage) / float64(previousAverage) * 100
}

func workflowMetricTrendDirection(deltaSeconds int64) string {
	switch {
	case deltaSeconds < 0:
		return "faster"
	case deltaSeconds > 0:
		return "slower"
	default:
		return "unchanged"
	}
}

func workflowMarkBottleneckLane(lanes []telemetry.WorkflowPhaseMetric) {
	best := -1
	for i := range lanes {
		lanes[i].Bottleneck = false
		if lanes[i].Count == 0 || lanes[i].AverageSeconds <= 0 {
			continue
		}
		if best < 0 || workflowLaneBottleneckLess(lanes[best], lanes[i]) {
			best = i
		}
	}
	if best >= 0 {
		lanes[best].Bottleneck = true
	}
}

func workflowLaneBottleneckLess(a telemetry.WorkflowPhaseMetric, b telemetry.WorkflowPhaseMetric) bool {
	if a.AverageSeconds != b.AverageSeconds {
		return a.AverageSeconds < b.AverageSeconds
	}
	if a.P95Seconds != b.P95Seconds {
		return a.P95Seconds < b.P95Seconds
	}
	if a.P90Seconds != b.P90Seconds {
		return a.P90Seconds < b.P90Seconds
	}
	if a.Count != b.Count {
		return a.Count < b.Count
	}
	return a.PhaseName > b.PhaseName
}

func workflowOldestCards(snapshot telemetry.Snapshot) []telemetry.WorkflowLaneAge {
	cards := workflowLaneAgeCards(snapshot)
	sort.SliceStable(cards, func(i int, j int) bool {
		if cards[i].AgeSeconds == cards[j].AgeSeconds {
			return cards[i].Identifier < cards[j].Identifier
		}
		return cards[i].AgeSeconds > cards[j].AgeSeconds
	})
	if len(cards) > maxWorkflowOldestCards {
		return append([]telemetry.WorkflowLaneAge(nil), cards[:maxWorkflowOldestCards]...)
	}
	return cards
}

func workflowLaneAgeCards(snapshot telemetry.Snapshot) []telemetry.WorkflowLaneAge {
	var issues []telemetry.Issue
	issues = append(issues, snapshot.BoardIssues...)
	issues = append(issues, snapshot.Pipeline...)
	for _, row := range snapshot.Running {
		issues = append(issues, row.Issue)
	}
	for _, row := range snapshot.Queue {
		issues = append(issues, row.Issue)
	}
	for _, row := range snapshot.Blocked {
		issues = append(issues, row.Issue)
	}
	for _, row := range snapshot.Completed {
		issues = append(issues, row.Issue)
	}

	seen := map[string]struct{}{}
	out := make([]telemetry.WorkflowLaneAge, 0, len(issues))
	for _, issue := range issues {
		key := strings.TrimSpace(issue.ProjectID) + "\x00" + strings.TrimSpace(issue.ID) + "\x00" + strings.TrimSpace(issue.State)
		if strings.TrimSpace(issue.ID) == "" {
			key = strings.TrimSpace(issue.ProjectID) + "\x00" + strings.TrimSpace(issue.Identifier) + "\x00" + strings.TrimSpace(issue.State)
		}
		if key == "\x00\x00" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, telemetry.WorkflowLaneAge{
			ProjectID:     issue.ProjectID,
			IssueID:       issue.ID,
			Identifier:    issue.Identifier,
			URL:           issue.URL,
			Title:         issue.Title,
			State:         issue.State,
			EnteredAt:     issue.CurrentLaneEnteredAt,
			AgeSeconds:    issue.CurrentLaneAgeSeconds,
			BottleneckKey: workflowIssueBottleneckKey(issue),
		})
	}
	return out
}

func workflowActiveBottleneck(snapshot telemetry.Snapshot, now time.Time) telemetry.WorkflowBottleneck {
	if backoff := workflowRESTBackoffBottleneck(snapshot.RateLimits, now); !backoff.IsZero() {
		return backoff
	}
	if ci := workflowCIBottleneck(snapshot); !ci.IsZero() {
		return ci
	}
	if len(snapshot.Running) > 0 {
		var seconds int64
		var issue telemetry.Issue
		for _, running := range snapshot.Running {
			runtimeSeconds := int64(running.RuntimeSeconds)
			if runtimeSeconds > seconds {
				seconds = runtimeSeconds
				issue = running.Issue
			}
		}
		return telemetry.WorkflowBottleneck{
			Kind:       "ai_active",
			Label:      "AI active",
			Detail:     "agent sessions currently running",
			ProjectID:  issue.ProjectID,
			IssueID:    issue.ID,
			Identifier: issue.Identifier,
			Seconds:    seconds,
			Count:      len(snapshot.Running),
		}
	}
	if mergeQueue := workflowMergeQueueBottleneck(snapshot); !mergeQueue.IsZero() {
		return mergeQueue
	}
	oldest := workflowOldestCards(snapshot)
	if len(oldest) == 0 {
		return telemetry.WorkflowBottleneck{}
	}
	card := oldest[0]
	return telemetry.WorkflowBottleneck{
		Kind:       "lane_age",
		Label:      "Oldest lane",
		Detail:     strings.TrimSpace(card.State),
		ProjectID:  card.ProjectID,
		IssueID:    card.IssueID,
		Identifier: card.Identifier,
		Seconds:    card.AgeSeconds,
		Count:      len(oldest),
	}
}

func workflowRESTBackoffBottleneck(limits *telemetry.RateLimits, now time.Time) telemetry.WorkflowBottleneck {
	if limits == nil || limits.RESTUsage == nil || limits.RESTUsage.BackoffUntil == nil {
		return telemetry.WorkflowBottleneck{}
	}
	until := *limits.RESTUsage.BackoffUntil
	if !until.After(now) {
		return telemetry.WorkflowBottleneck{}
	}
	return telemetry.WorkflowBottleneck{
		Kind:    "rate_limited",
		Label:   "Rate limited",
		Detail:  workflowRESTBackoffFamily(limits.RESTUsage.Contributors),
		Seconds: int64(until.Sub(now) / time.Second),
		Until:   &until,
	}
}

func workflowRESTBackoffFamily(contributors []telemetry.RESTUsageContributor) string {
	for _, contributor := range contributors {
		if contributor.RateLimited && strings.TrimSpace(contributor.EndpointFamily) != "" {
			return contributor.EndpointFamily
		}
	}
	return "GitHub REST backoff"
}

func workflowCIBottleneck(snapshot telemetry.Snapshot) telemetry.WorkflowBottleneck {
	var selected telemetry.Issue
	var seconds int64
	count := 0
	visit := func(issue telemetry.Issue) {
		if issue.PullRequest == nil || !workflowCIWaiting(issue.PullRequest) {
			return
		}
		count++
		wait := issue.PullRequest.CIQueueSeconds + issue.PullRequest.CIDurationSeconds
		if wait < issue.CurrentLaneAgeSeconds {
			wait = issue.CurrentLaneAgeSeconds
		}
		if wait > seconds {
			seconds = wait
			selected = issue
		}
	}
	for _, issue := range snapshot.Pipeline {
		visit(issue)
	}
	for _, issue := range snapshot.BoardIssues {
		visit(issue)
	}
	if count == 0 {
		return telemetry.WorkflowBottleneck{}
	}
	return telemetry.WorkflowBottleneck{
		Kind:       "ci_wait",
		Label:      "CI wait",
		Detail:     "pull requests waiting on checks",
		ProjectID:  selected.ProjectID,
		IssueID:    selected.ID,
		Identifier: selected.Identifier,
		Seconds:    seconds,
		Count:      count,
	}
}

func workflowCIWaiting(pr *telemetry.PullRequest) bool {
	status := strings.ToLower(strings.TrimSpace(pr.CIStatus))
	if status == "" {
		return len(pr.RunningChecks) > 0 || len(pr.UnstartedChecks) > 0
	}
	switch status {
	case "pending", "queued", "in_progress", "waiting", "expected":
		return true
	default:
		return false
	}
}

func workflowMergeQueueBottleneck(snapshot telemetry.Snapshot) telemetry.WorkflowBottleneck {
	var selected telemetry.Issue
	var seconds int64
	count := 0
	visit := func(issue telemetry.Issue) {
		if !strings.EqualFold(strings.TrimSpace(issue.State), "Merging") {
			return
		}
		count++
		if issue.CurrentLaneAgeSeconds > seconds {
			seconds = issue.CurrentLaneAgeSeconds
			selected = issue
		}
	}
	for _, issue := range snapshot.Pipeline {
		visit(issue)
	}
	for _, issue := range snapshot.BoardIssues {
		visit(issue)
	}
	if count == 0 {
		return telemetry.WorkflowBottleneck{}
	}
	return telemetry.WorkflowBottleneck{
		Kind:       "merge_queue",
		Label:      "Merge queue",
		Detail:     "issues waiting or actively merging",
		ProjectID:  selected.ProjectID,
		IssueID:    selected.ID,
		Identifier: selected.Identifier,
		Seconds:    seconds,
		Count:      count,
	}
}

func workflowIssueBottleneckKey(issue telemetry.Issue) string {
	if issue.PullRequest != nil && workflowCIWaiting(issue.PullRequest) {
		return "ci_wait"
	}
	if strings.EqualFold(strings.TrimSpace(issue.State), "Merging") {
		return "merge_queue"
	}
	if issue.CurrentLaneAgeSeconds > 0 {
		return "lane_age"
	}
	return ""
}
