package store

import (
	"sort"
	"strings"
)

func referenceWorkflowLaneFlows(rows []workflowMetricRow) map[string]workflowLaneFlow {
	activeEvents := make([]WorkflowPhaseEvent, 0, len(rows))
	for _, row := range rows {
		event := row.event
		if workflowPhaseTypeIsActive(event.PhaseType) && workflowEventHasInterval(event) {
			activeEvents = append(activeEvents, event)
		}
	}

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
			for _, activeEvent := range activeEvents {
				if !workflowEventsShareIssue(lane, activeEvent) {
					continue
				}
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

func referenceWorkflowLaneRepresentativeRuns(rows []workflowMetricRow, flowRows []workflowMetricRow) map[string][]WorkflowRepresentativeRun {
	activeEvents := make([]WorkflowPhaseEvent, 0, len(flowRows))
	for _, row := range flowRows {
		event := row.event
		if workflowPhaseTypeIsActive(event.PhaseType) && workflowEventHasInterval(event) {
			activeEvents = append(activeEvents, event)
		}
	}
	sort.SliceStable(activeEvents, func(i int, j int) bool {
		if activeEvents[i].FinishedAt.Equal(activeEvents[j].FinishedAt) {
			return activeEvents[i].ID > activeEvents[j].ID
		}
		return activeEvents[i].FinishedAt.After(activeEvents[j].FinishedAt)
	})

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
		for _, activeEvent := range activeEvents {
			if len(out[key]) >= maxWorkflowRepresentativeRuns {
				break
			}
			if !workflowEventsShareIssue(lane, activeEvent) {
				continue
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

func workflowEventsShareIssue(lane WorkflowPhaseEvent, event WorkflowPhaseEvent) bool {
	if strings.TrimSpace(lane.ProjectID) != strings.TrimSpace(event.ProjectID) {
		return false
	}
	if workflowNonEmptyEqual(lane.IssueID, event.IssueID) {
		return true
	}
	if workflowNonEmptyEqual(lane.Identifier, event.Identifier) {
		return true
	}
	if workflowNonEmptyEqual(lane.IssueURL, event.IssueURL) {
		return true
	}
	if lane.PRNumber != nil && event.PRNumber != nil && *lane.PRNumber == *event.PRNumber {
		return true
	}
	return false
}

func workflowNonEmptyEqual(a string, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	return a != "" && b != "" && a == b
}
