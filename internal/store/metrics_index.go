package store

import (
	"slices"
	"sort"
	"strconv"
	"strings"
)

type workflowEventKey struct {
	project string
	kind    int
	value   string
}

type workflowEventIndex struct {
	events     []WorkflowPhaseEvent
	identities map[workflowEventKey][]int
}

func workflowEventKeys(event WorkflowPhaseEvent) [4]workflowEventKey {
	project := strings.TrimSpace(event.ProjectID)
	keys := [4]workflowEventKey{
		{project, 0, strings.TrimSpace(event.IssueID)},
		{project, 1, strings.TrimSpace(event.Identifier)},
		{project, 2, strings.TrimSpace(event.IssueURL)},
		{project, 3, ""},
	}
	if event.PRNumber != nil {
		keys[3].value = strconv.FormatInt(*event.PRNumber, 10)
	}
	return keys
}

func newWorkflowEventIndex(rows []workflowMetricRow) workflowEventIndex {
	index := workflowEventIndex{identities: make(map[workflowEventKey][]int), events: make([]WorkflowPhaseEvent, 0, len(rows))}
	for _, row := range rows {
		if workflowPhaseTypeIsActive(row.event.PhaseType) && workflowEventHasInterval(row.event) {
			index.events = append(index.events, row.event)
		}
	}
	sort.SliceStable(index.events, func(i, j int) bool {
		if index.events[i].FinishedAt.Equal(index.events[j].FinishedAt) {
			return index.events[i].ID > index.events[j].ID
		}
		return index.events[i].FinishedAt.After(index.events[j].FinishedAt)
	})
	for i, event := range index.events {
		for _, key := range workflowEventKeys(event) {
			if key.value != "" {
				index.identities[key] = append(index.identities[key], i)
			}
		}
	}
	return index
}

func (index workflowEventIndex) matching(event WorkflowPhaseEvent) []int {
	var candidates []int
	for _, key := range workflowEventKeys(event) {
		if key.value != "" {
			candidates = append(candidates, index.identities[key]...)
		}
	}
	sort.Ints(candidates)
	return slices.Compact(candidates)
}
