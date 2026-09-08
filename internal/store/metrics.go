package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/store/sqlc"
)

func (s *sqliteStore) RecordWorkflowPhaseEvent(ctx context.Context, attrs WorkflowPhaseEvent) (int64, error) {
	projectID := strings.TrimSpace(attrs.ProjectID)
	if projectID == "" {
		return 0, errors.New("project_id is required")
	}
	phaseType := strings.TrimSpace(string(attrs.PhaseType))
	if phaseType == "" {
		return 0, errors.New("phase_type is required")
	}
	phaseName := strings.TrimSpace(attrs.PhaseName)
	if phaseName == "" {
		return 0, errors.New("phase_name is required")
	}
	startedAt, err := requiredTimestamp("started_at", attrs.StartedAt)
	if err != nil {
		return 0, err
	}
	finishedAt, err := optionalTimestamp("finished_at", attrs.FinishedAt)
	if err != nil {
		return 0, err
	}

	eventAt := attrs.StartedAt
	if !attrs.FinishedAt.IsZero() {
		eventAt = attrs.FinishedAt
	}
	metadataJSON := strings.TrimSpace(attrs.MetadataJSON)
	if metadataJSON == "" {
		metadataJSON = "{}"
	}

	event, err := s.queries.CreateWorkflowPhaseEvent(ctx, sqlc.CreateWorkflowPhaseEventParams{
		ProjectID:             projectID,
		RunID:                 nullPositiveInt64(attrs.RunID),
		SessionID:             nullPositiveInt64(attrs.SessionID),
		IssueID:               nullString(attrs.IssueID),
		Identifier:            nullString(attrs.Identifier),
		IssueURL:              nullString(attrs.IssueURL),
		PrNumber:              nullOptionalInt64(attrs.PRNumber),
		PhaseType:             phaseType,
		PhaseName:             phaseName,
		PreviousPhaseName:     nullString(attrs.PreviousPhaseName),
		Reason:                nullString(attrs.Reason),
		Status:                nullString(attrs.Status),
		StartedAt:             startedAt,
		FinishedAt:            finishedAt,
		DurationSeconds:       workflowEventDuration(attrs),
		EventDay:              eventAt.UTC().Format("2006-01-02"),
		CommandName:           nullString(attrs.CommandName),
		ExitCode:              nullOptionalInt64(attrs.ExitCode),
		Turns:                 nonNegative(attrs.Turns),
		InputTokens:           nonNegative(attrs.InputTokens),
		CachedInputTokens:     nullNonNegativeInt64(attrs.CachedInputTokens),
		OutputTokens:          nonNegative(attrs.OutputTokens),
		ReasoningOutputTokens: nullNonNegativeInt64(attrs.ReasoningOutputTokens),
		TotalTokens:           nonNegative(attrs.TotalTokens),
		ModelContextWindow:    nullOptionalInt64(attrs.ModelContextWindow),
		EndpointFamily:        nullString(attrs.EndpointFamily),
		MetadataJson:          metadataJSON,
	})
	if err != nil {
		return 0, fmt.Errorf("recording workflow phase event: %w", err)
	}
	return event.ID, nil
}

func (s *sqliteStore) UpdateWorkflowPhaseEventMetadata(ctx context.Context, eventID int64, metadataJSON string) error {
	if eventID <= 0 {
		return errors.New("workflow phase event id is required")
	}
	metadataJSON = strings.TrimSpace(metadataJSON)
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE workflow_phase_events
SET metadata_json = ?
WHERE id = ?`, metadataJSON, eventID)
	if err != nil {
		return fmt.Errorf("updating workflow phase event metadata: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("reading updated workflow phase event count: %w", err)
	}
	if updated == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *sqliteStore) ProvenanceAttributionTrustBoundary(ctx context.Context) (time.Time, error) {
	value, err := s.queries.ProvenanceAttributionTrustBoundary(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("reading provenance attribution trust boundary: %w", err)
	}
	boundary, err := parseTimestamp("trustworthy_since", value)
	if err != nil {
		return time.Time{}, err
	}
	return boundary.UTC(), nil
}

func (s *sqliteStore) WorkflowMetricsReport(ctx context.Context, query WorkflowMetricsQuery) (WorkflowMetricsReport, error) {
	from, err := optionalTimestamp("from", query.From)
	if err != nil {
		return WorkflowMetricsReport{}, err
	}
	to, err := optionalTimestamp("to", query.To)
	if err != nil {
		return WorkflowMetricsReport{}, err
	}
	if from.Valid && to.Valid && from.String >= to.String {
		return WorkflowMetricsReport{}, errors.New("from must be before to")
	}

	rows, err := s.queries.WorkflowPhaseDurationRows(ctx, sqlc.WorkflowPhaseDurationRowsParams{
		ProjectID: nullString(query.ProjectID),
		FromTime:  from,
		ToTime:    to,
	})
	if err != nil {
		return WorkflowMetricsReport{}, fmt.Errorf("reading workflow metrics report: %w", err)
	}

	metricRows, err := workflowMetricRowsFromEvents(rows)
	if err != nil {
		return WorkflowMetricsReport{}, err
	}

	flowRows := make([]workflowMetricRow, 0, len(metricRows))
	for _, row := range metricRows {
		if row.event.PhaseType == WorkflowPhaseTypeLane {
			flowRows = append(flowRows, row)
		}
	}

	flowFrom, flowTo, ok := workflowLaneFlowBounds(flowRows)
	if !ok {
		return workflowMetricsReport(metricRows, flowRows, query.From, query.To), nil
	}
	flowFromTime, err := optionalTimestamp("flow_from", flowFrom)
	if err != nil {
		return WorkflowMetricsReport{}, err
	}
	flowToTime, err := optionalTimestamp("flow_to", flowTo)
	if err != nil {
		return WorkflowMetricsReport{}, err
	}
	activeRows, err := s.queries.WorkflowPhaseFlowRows(ctx, sqlc.WorkflowPhaseFlowRowsParams{
		ProjectID: nullString(query.ProjectID),
		FromTime:  flowFromTime,
		ToTime:    flowToTime,
	})
	if err != nil {
		return WorkflowMetricsReport{}, fmt.Errorf("reading workflow flow metrics: %w", err)
	}
	activeMetricRows, err := workflowMetricRowsFromEvents(activeRows)
	if err != nil {
		return WorkflowMetricsReport{}, err
	}
	flowRows = append(flowRows, activeMetricRows...)

	return workflowMetricsReport(metricRows, flowRows, query.From, query.To), nil
}

func (s *sqliteStore) IssueWorkflowTimeline(ctx context.Context, identity IssueIdentity) (WorkflowTimeline, error) {
	identity = normalizeIssueIdentity(identity)
	if identity.ProjectID == "" {
		return WorkflowTimeline{}, ErrProjectRequired
	}
	if identity.IssueID == "" && identity.Identifier == "" && identity.IssueURL == "" {
		return WorkflowTimeline{Events: []WorkflowPhaseEvent{}}, nil
	}

	rows, err := s.queries.IssueWorkflowTimelineRows(ctx, sqlc.IssueWorkflowTimelineRowsParams{
		ProjectID:  identity.ProjectID,
		IssueID:    nullString(identity.IssueID),
		Identifier: nullString(identity.Identifier),
		IssueURL:   nullString(identity.IssueURL),
	})
	if err != nil {
		return WorkflowTimeline{}, fmt.Errorf("reading workflow timeline: %w", err)
	}

	timeline := WorkflowTimeline{Events: make([]WorkflowPhaseEvent, 0, len(rows))}
	for _, row := range rows {
		event, err := workflowPhaseEventFromRow(row)
		if err != nil {
			return WorkflowTimeline{}, err
		}
		timeline.Events = append(timeline.Events, event)
	}
	return timeline, nil
}

type workflowMetricRow struct {
	event WorkflowPhaseEvent
}

type workflowMetricBucket struct {
	projectID             string
	phaseType             string
	phaseName             string
	endpointFamily        string
	durations             []int64
	inputTokens           int64
	cachedInputTokens     int64
	outputTokens          int64
	reasoningOutputTokens int64
	totalTokens           int64
	turns                 int64
}

type workflowLaneFlow struct {
	activeSeconds int64
	waitSeconds   int64
}

const maxWorkflowRepresentativeRuns = 3

type workflowInterval struct {
	startedAt  time.Time
	finishedAt time.Time
}

type workflowLaneTrendBucket struct {
	totalSeconds int64
	count        int64
	label        string
	end          time.Time
}

type workflowLaneTrendAccumulator struct {
	projectID  string
	phaseName  string
	buckets    []workflowLaneTrendBucket
	totalCount int64
}

func workflowLaneFlowBounds(rows []workflowMetricRow) (time.Time, time.Time, bool) {
	var from time.Time
	var to time.Time
	for _, row := range rows {
		event := row.event
		if event.PhaseType != WorkflowPhaseTypeLane || !workflowEventHasInterval(event) {
			continue
		}
		if from.IsZero() || event.StartedAt.Before(from) {
			from = event.StartedAt
		}
		if to.IsZero() || event.FinishedAt.After(to) {
			to = event.FinishedAt
		}
	}
	return from, to, !from.IsZero() && from.Before(to)
}

func workflowMetricRowsFromEvents(rows []sqlc.WorkflowPhaseEvent) ([]workflowMetricRow, error) {
	metricRows := make([]workflowMetricRow, 0, len(rows))
	for _, row := range rows {
		event, err := workflowPhaseEventFromRow(row)
		if err != nil {
			return nil, err
		}
		metricRows = append(metricRows, workflowMetricRow{event: event})
	}
	return metricRows, nil
}

func workflowMetricsReport(rows []workflowMetricRow, flowRows []workflowMetricRow, from time.Time, to time.Time) WorkflowMetricsReport {
	buckets := map[string]*workflowMetricBucket{}
	for _, row := range rows {
		event := row.event
		if event.DurationSeconds < 0 {
			continue
		}
		key := workflowMetricKey(event)
		bucket, ok := buckets[key]
		if !ok {
			bucket = &workflowMetricBucket{
				projectID:      event.ProjectID,
				phaseType:      string(event.PhaseType),
				phaseName:      event.PhaseName,
				endpointFamily: event.EndpointFamily,
			}
			buckets[key] = bucket
		}
		bucket.durations = append(bucket.durations, event.DurationSeconds)
		bucket.inputTokens += event.InputTokens
		bucket.cachedInputTokens += event.CachedInputTokens
		bucket.outputTokens += event.OutputTokens
		bucket.reasoningOutputTokens += event.ReasoningOutputTokens
		bucket.totalTokens += event.TotalTokens
		bucket.turns += event.Turns
	}

	index := newWorkflowEventIndex(flowRows)
	laneFlows := workflowLaneFlowsIndexed(flowRows, index)
	laneRepresentatives := workflowLaneRepresentativeRunsIndexed(rows, index)
	metrics := make([]WorkflowPhaseMetric, 0, len(buckets))
	for _, bucket := range buckets {
		metric := workflowPhaseMetricFromBucket(bucket)
		if flow, ok := laneFlows[workflowMetricBucketKey(bucket)]; ok {
			metric.ActiveSeconds = flow.activeSeconds
			metric.WaitSeconds = flow.waitSeconds
			metric.ActivePercent = workflowPercent(flow.activeSeconds, flow.activeSeconds+flow.waitSeconds)
		}
		if representatives := laneRepresentatives[workflowMetricBucketKey(bucket)]; len(representatives) > 0 {
			metric.Representatives = representatives
		}
		metrics = append(metrics, metric)
	}
	sort.SliceStable(metrics, func(i, j int) bool {
		if metrics[i].ProjectID != metrics[j].ProjectID {
			return metrics[i].ProjectID < metrics[j].ProjectID
		}
		if metrics[i].PhaseType != metrics[j].PhaseType {
			return metrics[i].PhaseType < metrics[j].PhaseType
		}
		if metrics[i].PhaseName != metrics[j].PhaseName {
			return metrics[i].PhaseName < metrics[j].PhaseName
		}
		return metrics[i].EndpointFamily < metrics[j].EndpointFamily
	})

	report := WorkflowMetricsReport{
		Lanes:      []WorkflowPhaseMetric{},
		SubPhases:  []WorkflowPhaseMetric{},
		LaneTrends: workflowLaneTrends(rows, from, to),
	}
	for _, metric := range metrics {
		if metric.PhaseType == string(WorkflowPhaseTypeLane) {
			report.Lanes = append(report.Lanes, metric)
			continue
		}
		report.SubPhases = append(report.SubPhases, metric)
	}
	return report
}

func workflowMetricKey(event WorkflowPhaseEvent) string {
	return workflowMetricKeyFromParts(event.ProjectID, string(event.PhaseType), event.PhaseName, event.EndpointFamily)
}

func workflowMetricBucketKey(bucket *workflowMetricBucket) string {
	return workflowMetricKeyFromParts(bucket.projectID, bucket.phaseType, bucket.phaseName, bucket.endpointFamily)
}

func workflowMetricKeyFromParts(projectID string, phaseType string, phaseName string, endpointFamily string) string {
	parts := []string{
		projectID,
		phaseType,
		phaseName,
	}
	if endpointFamily != "" {
		parts = append(parts, endpointFamily)
	}
	return strings.Join(parts, "\x00")
}

func workflowLaneFlowsIndexed(rows []workflowMetricRow, index workflowEventIndex) map[string]workflowLaneFlow {
	flows := map[string]*workflowLaneFlow{}
	for _, row := range rows {
		lane := row.event
		if lane.PhaseType != WorkflowPhaseTypeLane || lane.DurationSeconds < 0 {
			continue
		}
		key := workflowMetricKey(lane)
		flow, ok := flows[key]
		if !ok {
			flow = &workflowLaneFlow{}
			flows[key] = flow
		}

		activeIntervals := []workflowInterval{}
		if workflowEventHasInterval(lane) {
			for _, candidate := range index.matching(lane) {
				activeEvent := index.events[candidate]
				if overlap, ok := workflowEventOverlap(lane, activeEvent); ok {
					activeIntervals = append(activeIntervals, overlap)
				}
			}
		}
		activeSeconds := workflowMergedIntervalSeconds(activeIntervals)
		if activeSeconds > lane.DurationSeconds {
			activeSeconds = lane.DurationSeconds
		}
		if activeSeconds < 0 {
			activeSeconds = 0
		}
		flow.activeSeconds += activeSeconds
		flow.waitSeconds += lane.DurationSeconds - activeSeconds
	}

	out := make(map[string]workflowLaneFlow, len(flows))
	for key, flow := range flows {
		out[key] = *flow
	}
	return out
}

func workflowLaneRepresentativeRunsIndexed(rows []workflowMetricRow, index workflowEventIndex) map[string][]WorkflowRepresentativeRun {
	laneEvents := make([]WorkflowPhaseEvent, 0, len(rows))
	for _, row := range rows {
		event := row.event
		if event.PhaseType == WorkflowPhaseTypeLane && event.DurationSeconds >= 0 {
			laneEvents = append(laneEvents, event)
		}
	}
	sort.SliceStable(laneEvents, func(i int, j int) bool {
		if laneEvents[i].FinishedAt.Equal(laneEvents[j].FinishedAt) {
			return laneEvents[i].ID > laneEvents[j].ID
		}
		return laneEvents[i].FinishedAt.After(laneEvents[j].FinishedAt)
	})

	out := map[string][]WorkflowRepresentativeRun{}
	seen := map[string]map[string]struct{}{}
	fallbacks := map[string][]WorkflowRepresentativeRun{}
	for _, lane := range laneEvents {
		key := workflowMetricKey(lane)
		for _, candidate := range index.matching(lane) {
			activeEvent := index.events[candidate]
			if len(out[key]) >= maxWorkflowRepresentativeRuns {
				break
			}
			if _, ok := workflowEventOverlap(lane, activeEvent); !ok {
				continue
			}
			representative := workflowRepresentativeRunFromEvents(lane, activeEvent)
			workflowAppendRepresentative(out, seen, key, representative)
		}
		fallbacks[key] = append(fallbacks[key], workflowRepresentativeRunFromEvents(lane, lane))
	}
	for key, representatives := range fallbacks {
		for _, representative := range representatives {
			if len(out[key]) >= maxWorkflowRepresentativeRuns {
				break
			}
			workflowAppendRepresentative(out, seen, key, representative)
		}
	}
	return out
}

func workflowRepresentativeRunFromEvents(lane WorkflowPhaseEvent, event WorkflowPhaseEvent) WorkflowRepresentativeRun {
	run := WorkflowRepresentativeRun{
		RunID:      event.RunID,
		SessionID:  event.SessionID,
		IssueID:    firstWorkflowMetricNonEmpty(event.IssueID, lane.IssueID),
		Identifier: firstWorkflowMetricNonEmpty(event.Identifier, lane.Identifier),
		IssueURL:   firstWorkflowMetricNonEmpty(event.IssueURL, lane.IssueURL),
		FinishedAt: event.FinishedAt,
	}
	if run.FinishedAt.IsZero() {
		run.FinishedAt = lane.FinishedAt
	}
	return run
}

func workflowAppendRepresentative(out map[string][]WorkflowRepresentativeRun, seen map[string]map[string]struct{}, key string, representative WorkflowRepresentativeRun) bool {
	representativeKey := workflowRepresentativeRunKey(representative)
	if representativeKey == "" {
		return false
	}
	if seen[key] == nil {
		seen[key] = map[string]struct{}{}
	}
	if _, ok := seen[key][representativeKey]; ok {
		return false
	}
	seen[key][representativeKey] = struct{}{}
	out[key] = append(out[key], representative)
	return true
}

func workflowRepresentativeRunKey(run WorkflowRepresentativeRun) string {
	parts := []string{
		strconv.FormatInt(run.RunID, 10),
		strconv.FormatInt(run.SessionID, 10),
		strings.TrimSpace(run.IssueID),
		strings.TrimSpace(run.Identifier),
		strings.TrimSpace(run.IssueURL),
	}
	if !run.FinishedAt.IsZero() {
		parts = append(parts, run.FinishedAt.UTC().Format(time.RFC3339Nano))
	}
	key := strings.Join(parts, "\x00")
	if strings.Trim(key, "\x00") == "0\x000" {
		return ""
	}
	return key
}

func firstWorkflowMetricNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func workflowLaneTrends(rows []workflowMetricRow, from time.Time, to time.Time) []WorkflowLaneTrend {
	from = from.UTC()
	to = to.UTC()
	if from.IsZero() || to.IsZero() || !from.Before(to) {
		return []WorkflowLaneTrend{}
	}

	const bucketCount = 8
	window := to.Sub(from)
	bucketDuration := window / bucketCount
	if bucketDuration <= 0 {
		return []WorkflowLaneTrend{}
	}

	accumulators := map[string]*workflowLaneTrendAccumulator{}
	for _, row := range rows {
		event := row.event
		if event.PhaseType != WorkflowPhaseTypeLane || !workflowLaneIsTracked(event.PhaseName) || event.DurationSeconds < 0 {
			continue
		}
		if event.FinishedAt.Before(from) || !event.FinishedAt.Before(to) {
			continue
		}
		key := strings.Join([]string{event.ProjectID, event.PhaseName}, "\x00")
		accumulator, ok := accumulators[key]
		if !ok {
			accumulator = &workflowLaneTrendAccumulator{
				projectID: event.ProjectID,
				phaseName: event.PhaseName,
				buckets:   workflowLaneTrendBuckets(from, bucketDuration, bucketCount, window),
			}
			accumulators[key] = accumulator
		}
		index := int(event.FinishedAt.Sub(from) / bucketDuration)
		if index < 0 {
			continue
		}
		if index >= bucketCount {
			index = bucketCount - 1
		}
		accumulator.buckets[index].totalSeconds += event.DurationSeconds
		accumulator.buckets[index].count++
		accumulator.totalCount++
	}

	trends := make([]WorkflowLaneTrend, 0, len(accumulators))
	for _, accumulator := range accumulators {
		points := make([]WorkflowLaneTrendPoint, 0, len(accumulator.buckets))
		for _, bucket := range accumulator.buckets {
			averageSeconds := int64(0)
			if bucket.count > 0 {
				averageSeconds = bucket.totalSeconds / bucket.count
			}
			points = append(points, WorkflowLaneTrendPoint{
				Label:          bucket.label,
				BucketEnd:      bucket.end,
				Count:          bucket.count,
				AverageSeconds: averageSeconds,
			})
		}
		trends = append(trends, WorkflowLaneTrend{
			ProjectID:  accumulator.projectID,
			PhaseName:  accumulator.phaseName,
			Points:     points,
			TotalCount: accumulator.totalCount,
		})
	}
	sort.SliceStable(trends, func(i int, j int) bool {
		if trends[i].ProjectID != trends[j].ProjectID {
			return trends[i].ProjectID < trends[j].ProjectID
		}
		return workflowTrackedLaneRank(trends[i].PhaseName) < workflowTrackedLaneRank(trends[j].PhaseName)
	})
	return trends
}

func workflowLaneTrendBuckets(from time.Time, bucketDuration time.Duration, bucketCount int, window time.Duration) []workflowLaneTrendBucket {
	buckets := make([]workflowLaneTrendBucket, 0, bucketCount)
	for i := range bucketCount {
		bucketStart := from.Add(time.Duration(i) * bucketDuration)
		bucketEnd := bucketStart.Add(bucketDuration)
		buckets = append(buckets, workflowLaneTrendBucket{
			label: workflowLaneTrendBucketLabel(bucketEnd, window),
			end:   bucketEnd,
		})
	}
	return buckets
}

func workflowLaneTrendBucketLabel(bucketEnd time.Time, window time.Duration) string {
	switch {
	case window <= 48*time.Hour:
		return bucketEnd.Format("15:04")
	case window <= 14*24*time.Hour:
		return bucketEnd.Format("Jan 2 15:04")
	default:
		return bucketEnd.Format("Jan 2")
	}
}

func workflowLaneIsTracked(phaseName string) bool {
	return workflowTrackedLaneRank(phaseName) >= 0
}

func workflowTrackedLaneRank(phaseName string) int {
	switch strings.ToLower(strings.TrimSpace(phaseName)) {
	case "in progress":
		return 0
	case "human review":
		return 1
	case "merging":
		return 2
	case "rework":
		return 3
	default:
		return -1
	}
}

func workflowPhaseTypeIsActive(phaseType WorkflowPhaseType) bool {
	switch phaseType {
	case WorkflowPhaseTypeAgentSession, WorkflowPhaseTypeLocalCheck, WorkflowPhaseTypeCI:
		return true
	default:
		return false
	}
}

func workflowEventHasInterval(event WorkflowPhaseEvent) bool {
	return !event.StartedAt.IsZero() && !event.FinishedAt.IsZero() && event.StartedAt.Before(event.FinishedAt)
}

func workflowEventOverlap(lane WorkflowPhaseEvent, event WorkflowPhaseEvent) (workflowInterval, bool) {
	startedAt := lane.StartedAt
	if event.StartedAt.After(startedAt) {
		startedAt = event.StartedAt
	}
	finishedAt := lane.FinishedAt
	if event.FinishedAt.Before(finishedAt) {
		finishedAt = event.FinishedAt
	}
	if !startedAt.Before(finishedAt) {
		return workflowInterval{}, false
	}
	return workflowInterval{startedAt: startedAt, finishedAt: finishedAt}, true
}

func workflowMergedIntervalSeconds(intervals []workflowInterval) int64 {
	if len(intervals) == 0 {
		return 0
	}
	sort.Slice(intervals, func(i int, j int) bool {
		if intervals[i].startedAt.Equal(intervals[j].startedAt) {
			return intervals[i].finishedAt.Before(intervals[j].finishedAt)
		}
		return intervals[i].startedAt.Before(intervals[j].startedAt)
	})

	total := int64(0)
	current := intervals[0]
	for _, interval := range intervals[1:] {
		if interval.startedAt.After(current.finishedAt) {
			total += int64(current.finishedAt.Sub(current.startedAt) / time.Second)
			current = interval
			continue
		}
		if interval.finishedAt.After(current.finishedAt) {
			current.finishedAt = interval.finishedAt
		}
	}
	total += int64(current.finishedAt.Sub(current.startedAt) / time.Second)
	return total
}

func workflowPercent(part int64, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func workflowPhaseMetricFromBucket(bucket *workflowMetricBucket) WorkflowPhaseMetric {
	sort.Slice(bucket.durations, func(i, j int) bool {
		return bucket.durations[i] < bucket.durations[j]
	})

	total := int64(0)
	for _, duration := range bucket.durations {
		total += duration
	}
	count := int64(len(bucket.durations))
	average := int64(0)
	if count > 0 {
		average = total / count
	}

	return WorkflowPhaseMetric{
		ProjectID:             bucket.projectID,
		PhaseType:             bucket.phaseType,
		PhaseName:             bucket.phaseName,
		Count:                 count,
		TotalSeconds:          total,
		AverageSeconds:        average,
		P50Seconds:            percentileSeconds(bucket.durations, 0.50),
		P90Seconds:            percentileSeconds(bucket.durations, 0.90),
		P95Seconds:            percentileSeconds(bucket.durations, 0.95),
		InputTokens:           bucket.inputTokens,
		CachedInputTokens:     bucket.cachedInputTokens,
		OutputTokens:          bucket.outputTokens,
		ReasoningOutputTokens: bucket.reasoningOutputTokens,
		TotalTokens:           bucket.totalTokens,
		Turns:                 bucket.turns,
		EndpointFamily:        bucket.endpointFamily,
	}
}

func percentileSeconds(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	if percentile <= 0 {
		return values[0]
	}
	if percentile >= 1 {
		return values[len(values)-1]
	}
	index := int(percentile*float64(len(values))+0.999999999) - 1
	if index < 0 {
		return values[0]
	}
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func workflowEventDuration(attrs WorkflowPhaseEvent) int64 {
	if attrs.DurationSeconds > 0 {
		return attrs.DurationSeconds
	}
	if attrs.StartedAt.IsZero() || attrs.FinishedAt.IsZero() || attrs.FinishedAt.Before(attrs.StartedAt) {
		return 0
	}
	return int64(attrs.FinishedAt.Sub(attrs.StartedAt) / time.Second)
}

func optionalTimestamp(name string, value time.Time) (sql.NullString, error) {
	if value.IsZero() {
		return sql.NullString{}, nil
	}
	text, err := requiredTimestamp(name, value)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: text, Valid: true}, nil
}

func workflowPhaseEventFromRow(row sqlc.WorkflowPhaseEvent) (WorkflowPhaseEvent, error) {
	startedAt, err := parseTimestamp("started_at", row.StartedAt)
	if err != nil {
		return WorkflowPhaseEvent{}, err
	}
	var finishedAt time.Time
	if row.FinishedAt.Valid {
		finishedAt, err = parseTimestamp("finished_at", row.FinishedAt.String)
		if err != nil {
			return WorkflowPhaseEvent{}, err
		}
	}

	event := WorkflowPhaseEvent{
		ID:                    row.ID,
		ProjectID:             strings.TrimSpace(row.ProjectID),
		IssueID:               row.IssueID.String,
		Identifier:            row.Identifier.String,
		IssueURL:              row.IssueURL.String,
		PhaseType:             WorkflowPhaseType(row.PhaseType),
		PhaseName:             row.PhaseName,
		PreviousPhaseName:     row.PreviousPhaseName.String,
		Reason:                row.Reason.String,
		Status:                row.Status.String,
		StartedAt:             startedAt.UTC(),
		FinishedAt:            finishedAt.UTC(),
		DurationSeconds:       nonNegative(row.DurationSeconds),
		CommandName:           row.CommandName.String,
		Turns:                 nonNegative(row.Turns),
		InputTokens:           nonNegative(row.InputTokens),
		CachedInputTokens:     nonNegative(row.CachedInputTokens.Int64),
		OutputTokens:          nonNegative(row.OutputTokens),
		ReasoningOutputTokens: nonNegative(row.ReasoningOutputTokens.Int64),
		TotalTokens:           nonNegative(row.TotalTokens),
		EndpointFamily:        row.EndpointFamily.String,
		MetadataJSON:          row.MetadataJson,
	}
	if row.RunID.Valid {
		event.RunID = row.RunID.Int64
	}
	if row.SessionID.Valid {
		event.SessionID = row.SessionID.Int64
	}
	if row.PrNumber.Valid {
		value := row.PrNumber.Int64
		event.PRNumber = &value
	}
	if row.ExitCode.Valid {
		value := row.ExitCode.Int64
		event.ExitCode = &value
	}
	if row.ModelContextWindow.Valid {
		value := row.ModelContextWindow.Int64
		event.ModelContextWindow = &value
	}
	return event, nil
}

func (s *sqliteStore) WorkflowHistoryRevision(ctx context.Context) (int64, error) {
	var revision int64
	if err := s.db.QueryRowContext(ctx, "SELECT revision FROM workflow_history_revision WHERE id = 1").Scan(&revision); err != nil {
		return 0, fmt.Errorf("reading workflow history revision: %w", err)
	}
	return revision, nil
}
