package github

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
)

type checkRunTelemetrySummary struct {
	QueueSeconds    int64
	DurationSeconds int64
	SlowChecks      []connector.PullRequestCheck
	RunningChecks   []string
	UnstartedCount  int
	UnstartedChecks []connector.PullRequestCheck
}

type checkRunContextResult struct {
	Run     restCheckRun
	Pending bool
	Settled bool
}

func (c *Connector) fetchPullRequestCI(ctx context.Context, repo pullRequestRepo, sha string) (pullRequestCI, error) {
	checkRuns, err := fetchRESTCheckRuns(ctx, c.client, restCommitCheckRunsPath(repo, sha))
	if err != nil {
		return pullRequestCI{}, fmt.Errorf("fetch github check runs: %w", err)
	}
	effectiveRuns := effectiveCheckRuns(checkRuns)
	workflowRuns, workflowRunErr := fetchRESTWorkflowRunsForCheckRuns(ctx, c.client, repo, effectiveRuns)
	if workflowRunErr != nil {
		if pullRequestHydrationThrottleError(workflowRunErr) {
			return pullRequestCI{}, fmt.Errorf("fetch github workflow runs: %w", workflowRunErr)
		}
		workflowRuns = nil
	}
	statuses, err := fetchRESTList[restCommitStatus](ctx, c.client, restCommitStatusesPath(repo, sha))
	if err != nil {
		return pullRequestCI{}, fmt.Errorf("fetch github commit statuses: %w", err)
	}
	now := time.Now()
	if c.now != nil {
		now = c.now()
	}
	telemetry := checkRunTelemetry(effectiveRuns, workflowRuns, now, c.unstartedThreshold)
	staleSuccessfulChecks := staleSuccessfulCheckRuns(checkRuns)
	c.logStaleSuccessfulCheckRuns(ctx, repo, sha, staleSuccessfulChecks)
	requiredFailures := requiredStatusCheckFailures(checkRuns, statuses, c.requiredChecks)
	state := combinedCIState(checkRunsState(checkRuns), commitStatusesState(statuses))
	state = combinedCIState(requiredStatusCheckState(requiredFailures), state)
	transientFailures, err := c.transientCheckRunFailures(ctx, repo, checkRuns)
	if err != nil {
		return pullRequestCI{}, err
	}
	return pullRequestCI{
		State:                 state,
		Checks:                pullRequestCheckInventory(checkRuns, statuses),
		CheckRunCount:         len(checkRuns),
		StatusContextCount:    len(statuses),
		CIQueueSeconds:        telemetry.QueueSeconds,
		CIDurationSeconds:     telemetry.DurationSeconds,
		SlowChecks:            telemetry.SlowChecks,
		RunningChecks:         telemetry.RunningChecks,
		UnstartedCheckCount:   telemetry.UnstartedCount,
		UnstartedChecks:       telemetry.UnstartedChecks,
		StaleSuccessfulChecks: staleSuccessfulChecks,
		RequiredFailures:      requiredFailures,
		TransientFailures:     transientFailures,
	}, nil
}

func pullRequestCheckInventory(checkRuns []restCheckRun, statuses []restCommitStatus) []connector.PullRequestCheck {
	checks := make([]connector.PullRequestCheck, 0, len(checkRuns)+len(statuses))
	seen := map[string]struct{}{}
	for _, group := range groupedCheckRuns(checkRuns) {
		checkRun := latestCheckRun(group)
		name := strings.TrimSpace(checkRun.Name)
		key := strings.ToLower(name)
		if key == "" {
			continue
		}
		seen[key] = struct{}{}
		checks = append(checks, connector.PullRequestCheck{
			FailureDetail: checkRun.FailureDetail,
			ID:            checkRun.ID,
			WorkflowRunID: checkRunWorkflowRunID(checkRun),
			Name:          name,
			Status:        normalizedCheckRunStatus(checkRun),
			Conclusion:    strings.ToLower(strings.TrimSpace(checkRun.Conclusion)),
			DetailsURL:    firstNonBlank(checkRun.DetailsURL, checkRun.HTMLURL),
		})
	}
	for _, status := range statuses {
		name := strings.TrimSpace(status.Context)
		key := strings.ToLower(name)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		checks = append(checks, connector.PullRequestCheck{
			Name:       name,
			Status:     strings.ToLower(strings.TrimSpace(status.State)),
			Conclusion: strings.ToLower(strings.TrimSpace(status.State)),
		})
	}
	return checks
}

func (c *Connector) transientCheckRunFailures(ctx context.Context, repo pullRequestRepo, checkRuns []restCheckRun) ([]connector.PullRequestCheck, error) {
	failures := make([]connector.PullRequestCheck, 0)
	for _, checkRun := range effectiveCheckRuns(checkRuns) {
		if !completedFailedCheckRun(checkRun) {
			continue
		}
		text := checkRunTransientText(checkRun)
		detail := firstNonBlank(checkRun.Output.Text, checkRun.Output.Summary, checkRun.Output.Title)
		if checkRun.ID > 0 {
			annotations, err := fetchRESTCheckRunAnnotations(ctx, c.client, restCheckRunAnnotationsPath(repo, checkRun.ID))
			if err != nil {
				if pullRequestHydrationThrottleError(err) {
					return nil, fmt.Errorf("fetch github check run annotations: %w", err)
				}
				if c.logger != nil {
					c.logger.DebugContext(ctx, "fetch github check run annotations failed", "check_run_id", checkRun.ID, "check_run_name", checkRun.Name, "error", err)
				}
			} else {
				text = strings.TrimSpace(text + "\n" + checkRunAnnotationTransientText(annotations))
				for _, annotation := range annotations {
					if strings.EqualFold(annotation.AnnotationLevel, "failure") && strings.TrimSpace(annotation.Message) != "" {
						detail = strings.TrimSpace(annotation.Path + ": " + annotation.Message)
						break
					}
				}
			}
		}
		for index := range checkRuns {
			if checkRuns[index].ID == checkRun.ID && checkRuns[index].Name == checkRun.Name {
				checkRuns[index].FailureDetail = runtimeoutput.Truncate(detail, 2048).Value
			}
		}
		if !transientCheckFailureText(text) && !transientCheckConclusion(checkRun.Conclusion) {
			continue
		}
		failures = append(failures, connector.PullRequestCheck{
			ID:            checkRun.ID,
			WorkflowRunID: checkRunWorkflowRunID(checkRun),
			Name:          strings.TrimSpace(checkRun.Name),
			Status:        strings.ToLower(strings.TrimSpace(checkRun.Status)),
			Conclusion:    strings.ToLower(strings.TrimSpace(checkRun.Conclusion)),
			DetailsURL:    firstNonBlank(checkRun.DetailsURL, checkRun.HTMLURL),
		})
	}
	return failures, nil
}

func pullRequestHydrationThrottleError(err error) bool {
	return errors.Is(err, ErrRateLimited) || restFanoutOrReserveDeferred(err)
}

func (c *Connector) logStaleSuccessfulCheckRuns(ctx context.Context, repo pullRequestRepo, sha string, checks []connector.PullRequestCheck) {
	if c == nil || c.logger == nil || len(checks) == 0 {
		return
	}
	c.logger.WarnContext(ctx, "github check runs reported stale successful status; treating completed successful check-runs as passed",
		"repository", pullRequestRepoName(repo),
		"head_sha", strings.TrimSpace(sha),
		"reason", "stale_successful_check_run",
		"checks", strings.Join(pullRequestCheckNames(checks), ", "),
		"action", "normalize_success",
	)
}

func completedFailedCheckRun(checkRun restCheckRun) bool {
	if normalizedCheckRunStatus(checkRun) != "completed" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(checkRun.Conclusion)) {
	case "", "success", "skipped", "neutral", "cancelled", "canceled":
		return false
	default:
		return true
	}
}

func staleSuccessfulCheckRuns(checkRuns []restCheckRun) []connector.PullRequestCheck {
	checks := make([]connector.PullRequestCheck, 0)
	for _, checkRun := range checkRuns {
		if !staleSuccessfulCheckRun(checkRun) {
			continue
		}
		checks = append(checks, connector.PullRequestCheck{
			ID:            checkRun.ID,
			WorkflowRunID: checkRunWorkflowRunID(checkRun),
			Name:          strings.TrimSpace(checkRun.Name),
			Status:        strings.ToLower(strings.TrimSpace(checkRun.Status)),
			Conclusion:    strings.ToLower(strings.TrimSpace(checkRun.Conclusion)),
			DetailsURL:    firstNonBlank(checkRun.DetailsURL, checkRun.HTMLURL),
		})
	}
	return checks
}

func staleSuccessfulCheckRun(checkRun restCheckRun) bool {
	status := strings.ToLower(strings.TrimSpace(checkRun.Status))
	return status != "" &&
		status != "completed" &&
		strings.ToLower(strings.TrimSpace(checkRun.Conclusion)) == "success" &&
		checkRun.CompletedAt != nil
}

func normalizedCheckRunStatus(checkRun restCheckRun) string {
	status := strings.ToLower(strings.TrimSpace(checkRun.Status))
	conclusion := strings.ToLower(strings.TrimSpace(checkRun.Conclusion))
	if status != "" && status != "completed" && conclusion != "" && checkRun.CompletedAt != nil {
		return "completed"
	}
	return status
}

func checkRunTransientText(checkRun restCheckRun) string {
	return strings.Join([]string{
		checkRun.Name,
		checkRun.Conclusion,
		checkRun.Output.Title,
		checkRun.Output.Summary,
		checkRun.Output.Text,
	}, "\n")
}

func checkRunAnnotationTransientText(annotations []restCheckRunAnnotation) string {
	parts := make([]string, 0, len(annotations)*3)
	for _, annotation := range annotations {
		parts = append(parts, annotation.Path, annotation.Message, annotation.RawDetails)
	}
	return strings.Join(parts, "\n")
}

func transientCheckConclusion(conclusion string) bool {
	switch strings.ToLower(strings.TrimSpace(conclusion)) {
	case "timed_out", "startup_failure":
		return true
	default:
		return false
	}
}

func transientCheckFailureText(text string) bool {
	text = strings.ToLower(strings.Join(strings.Fields(text), " "))
	if text == "" {
		return false
	}
	for _, phrase := range []string{
		"signal: killed",
		"compile: signal: killed",
		"out of memory",
		"oom",
		"oom-kill",
		"oom killed",
		"exit code 137",
		"killed process",
		"runner lost communication",
		"the hosted runner",
		"operation was canceled by the runner",
		"no space left on device",
	} {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func checkRunsState(checkRuns []restCheckRun) string {
	if len(checkRuns) == 0 {
		return ""
	}
	pending := false
	failed := false
	for _, group := range groupedCheckRuns(checkRuns) {
		result := selectCheckRunContext(group)
		if result.Pending {
			pending = true
			continue
		}
		if !result.Settled {
			continue
		}
		conclusion := strings.ToLower(strings.TrimSpace(result.Run.Conclusion))
		switch conclusion {
		case "success", "neutral":
		case "":
			pending = true
		default:
			failed = true
		}
	}
	if failed {
		return "failure"
	}
	if pending {
		return "pending"
	}
	return "success"
}

func requiredStatusCheckFailures(checkRuns []restCheckRun, statuses []restCommitStatus, required []string) []connector.PullRequestCheck {
	required = normalizeRequiredStatusChecks(required)
	if len(required) == 0 {
		return nil
	}

	checkRunsByName := make(map[string]checkRunContextResult, len(checkRuns))
	for _, group := range groupedCheckRuns(checkRuns) {
		name := strings.TrimSpace(group[0].Name)
		if name == "" {
			continue
		}
		checkRunsByName[name] = selectCheckRunContext(group)
	}
	statusesByContext := latestCommitStatusesByContext(statuses)

	failures := make([]connector.PullRequestCheck, 0, len(required))
	for _, name := range required {
		if result, ok := checkRunsByName[name]; ok {
			if result.Pending || result.Settled {
				if failure, failed := requiredCheckRunFailure(name, result.Run); failed {
					failures = append(failures, failure)
				}
				continue
			}
			failures = append(failures, connector.PullRequestCheck{
				Name:   name,
				Status: "pending",
			})
			continue
		}
		if status, ok := statusesByContext[name]; ok {
			if failure, failed := requiredCommitStatusFailure(name, status); failed {
				failures = append(failures, failure)
			}
			continue
		}
		failures = append(failures, connector.PullRequestCheck{
			Name:       name,
			Status:     "missing",
			Conclusion: "missing",
		})
	}
	if len(failures) == 0 {
		return nil
	}
	return failures
}

func requiredCheckRunFailure(name string, checkRun restCheckRun) (connector.PullRequestCheck, bool) {
	status := normalizedCheckRunStatus(checkRun)
	conclusion := strings.ToLower(strings.TrimSpace(checkRun.Conclusion))
	if ignoredCheckRunConclusion(conclusion) {
		return connector.PullRequestCheck{}, false
	}
	if status != "" && status != "completed" {
		return connector.PullRequestCheck{
			ID:            checkRun.ID,
			WorkflowRunID: checkRunWorkflowRunID(checkRun),
			Name:          name,
			Status:        status,
			Conclusion:    conclusion,
			DetailsURL:    firstNonBlank(checkRun.DetailsURL, checkRun.HTMLURL),
		}, true
	}
	if conclusion == "success" {
		return connector.PullRequestCheck{}, false
	}
	if conclusion == "" {
		conclusion = "missing"
	}
	return connector.PullRequestCheck{
		ID:            checkRun.ID,
		WorkflowRunID: checkRunWorkflowRunID(checkRun),
		Name:          name,
		Status:        firstNonBlank(status, "completed"),
		Conclusion:    conclusion,
		DetailsURL:    firstNonBlank(checkRun.DetailsURL, checkRun.HTMLURL),
	}, true
}

func requiredCommitStatusFailure(name string, status restCommitStatus) (connector.PullRequestCheck, bool) {
	state := strings.ToLower(strings.TrimSpace(status.State))
	if state == "success" {
		return connector.PullRequestCheck{}, false
	}
	if state == "" {
		state = "pending"
	}
	return connector.PullRequestCheck{
		Name:       name,
		Status:     state,
		Conclusion: state,
	}, true
}

func requiredStatusCheckState(failures []connector.PullRequestCheck) string {
	if len(failures) == 0 {
		return ""
	}
	pending := false
	failed := false
	for _, failure := range failures {
		status := strings.ToLower(strings.TrimSpace(failure.Status))
		conclusion := strings.ToLower(strings.TrimSpace(failure.Conclusion))
		switch {
		case requiredStatusCheckPending(status, conclusion):
			pending = true
		default:
			failed = true
		}
	}
	if failed {
		return "failure"
	}
	if pending {
		return "pending"
	}
	return ""
}

func requiredStatusCheckPending(status string, conclusion string) bool {
	switch conclusion {
	case "missing", "":
		return true
	}
	switch status {
	case "missing", "pending", "queued", "waiting", "in_progress", "in progress", "requested", "expected":
		return true
	default:
		return false
	}
}

func checkRunTelemetry(checkRuns []restCheckRun, workflowRuns []restWorkflowRun, now time.Time, unstartedThreshold time.Duration) checkRunTelemetrySummary {
	var queueCreatedAt *time.Time
	var queueStartedAt *time.Time
	var checkStartedAt *time.Time
	var completedAt *time.Time
	hasRunning := false
	slowChecks := make([]connector.PullRequestCheck, 0, len(checkRuns))
	runningChecks := make([]string, 0, len(checkRuns))
	unstartedChecks := make([]connector.PullRequestCheck, 0, len(checkRuns))

	for _, run := range workflowRuns {
		queueCreatedAt = earliestGitHubTime(queueCreatedAt, run.CreatedAt)
		queueStartedAt = earliestGitHubTime(queueStartedAt, run.RunStartedAt)
	}

	for _, checkRun := range effectiveCheckRuns(checkRuns) {
		queueCreatedAt = earliestGitHubTime(queueCreatedAt, checkRun.CreatedAt)
		queueStartedAt = earliestGitHubTime(queueStartedAt, checkRun.StartedAt)
		checkStartedAt = earliestGitHubTime(checkStartedAt, checkRun.StartedAt)
		completedAt = latestGitHubTime(completedAt, checkRun.CompletedAt)

		name := strings.TrimSpace(checkRun.Name)
		status := normalizedCheckRunStatus(checkRun)
		conclusion := strings.ToLower(strings.TrimSpace(checkRun.Conclusion))
		if queueSeconds, unstarted := unstartedCheckRunQueueSeconds(checkRun, now, unstartedThreshold); unstarted {
			hasRunning = true
			unstartedChecks = append(unstartedChecks, connector.PullRequestCheck{
				ID:            checkRun.ID,
				WorkflowRunID: checkRunWorkflowRunID(checkRun),
				Name:          name,
				Status:        status,
				Conclusion:    conclusion,
				DetailsURL:    firstNonBlank(checkRun.DetailsURL, checkRun.HTMLURL),
				QueueSeconds:  queueSeconds,
			})
			continue
		}
		if (status != "" && status != "completed") || conclusion == "" {
			hasRunning = true
			runningChecks = append(runningChecks, name)
			continue
		}
		if name == "" || checkRun.StartedAt == nil || checkRun.CompletedAt == nil || checkRun.CompletedAt.Before(*checkRun.StartedAt) {
			continue
		}
		var queueSeconds int64
		if checkRun.CreatedAt != nil && !checkRun.StartedAt.Before(*checkRun.CreatedAt) {
			queueSeconds = int64(checkRun.StartedAt.Sub(*checkRun.CreatedAt) / time.Second)
		}
		slowChecks = append(slowChecks, connector.PullRequestCheck{
			Name:            name,
			Status:          status,
			Conclusion:      conclusion,
			QueueSeconds:    queueSeconds,
			DurationSeconds: int64(checkRun.CompletedAt.Sub(*checkRun.StartedAt) / time.Second),
		})
	}

	sort.SliceStable(slowChecks, func(i, j int) bool {
		if slowChecks[i].DurationSeconds != slowChecks[j].DurationSeconds {
			return slowChecks[i].DurationSeconds > slowChecks[j].DurationSeconds
		}
		return slowChecks[i].Name < slowChecks[j].Name
	})
	if len(slowChecks) > pullRequestSlowCheckLimit {
		slowChecks = slowChecks[:pullRequestSlowCheckLimit]
	}
	runningChecks = uniqueNonBlank(runningChecks)
	sort.Strings(runningChecks)
	if len(runningChecks) > pullRequestRunningCheckLimit {
		runningChecks = runningChecks[:pullRequestRunningCheckLimit]
	}
	sort.SliceStable(unstartedChecks, func(i, j int) bool {
		if unstartedChecks[i].QueueSeconds != unstartedChecks[j].QueueSeconds {
			return unstartedChecks[i].QueueSeconds > unstartedChecks[j].QueueSeconds
		}
		return unstartedChecks[i].Name < unstartedChecks[j].Name
	})
	unstartedCount := len(unstartedChecks)
	if len(unstartedChecks) > pullRequestUnstartedCheckLimit {
		unstartedChecks = unstartedChecks[:pullRequestUnstartedCheckLimit]
	}
	var durationSeconds int64
	if !hasRunning && checkStartedAt != nil && completedAt != nil && !completedAt.Before(*checkStartedAt) {
		durationSeconds = int64(completedAt.Sub(*checkStartedAt) / time.Second)
	}
	var queueSeconds int64
	if queueCreatedAt != nil && queueStartedAt != nil && !queueStartedAt.Before(*queueCreatedAt) {
		queueSeconds = int64(queueStartedAt.Sub(*queueCreatedAt) / time.Second)
	}
	return checkRunTelemetrySummary{
		QueueSeconds:    queueSeconds,
		DurationSeconds: durationSeconds,
		SlowChecks:      slowChecks,
		RunningChecks:   runningChecks,
		UnstartedCount:  unstartedCount,
		UnstartedChecks: unstartedChecks,
	}
}

func unstartedCheckRunQueueSeconds(checkRun restCheckRun, now time.Time, threshold time.Duration) (int64, bool) {
	if threshold <= 0 || normalizedCheckRunStatus(checkRun) != "queued" || checkRun.StartedAt != nil || checkRun.CreatedAt == nil {
		return 0, false
	}
	age := now.Sub(*checkRun.CreatedAt)
	if age < threshold {
		return 0, false
	}
	return int64(age / time.Second), true
}

func groupedCheckRuns(checkRuns []restCheckRun) [][]restCheckRun {
	groups := make([][]restCheckRun, 0, len(checkRuns))
	groupIndexes := make(map[string]int, len(checkRuns))
	for _, checkRun := range checkRuns {
		name := strings.TrimSpace(checkRun.Name)
		if name == "" {
			groups = append(groups, []restCheckRun{checkRun})
			continue
		}
		index, ok := groupIndexes[name]
		if !ok {
			groupIndexes[name] = len(groups)
			groups = append(groups, []restCheckRun{checkRun})
			continue
		}
		groups[index] = append(groups[index], checkRun)
	}
	return groups
}

func effectiveCheckRuns(checkRuns []restCheckRun) []restCheckRun {
	effective := make([]restCheckRun, 0, len(checkRuns))
	for _, group := range groupedCheckRuns(checkRuns) {
		result := selectCheckRunContext(group)
		if result.Pending || result.Settled {
			effective = append(effective, result.Run)
		}
	}
	return effective
}

func selectCheckRunContext(checkRuns []restCheckRun) checkRunContextResult {
	if len(checkRuns) == 0 {
		return checkRunContextResult{}
	}
	latest := latestCheckRun(checkRuns)
	status := normalizedCheckRunStatus(latest)
	conclusion := strings.ToLower(strings.TrimSpace(latest.Conclusion))
	if (status != "" && status != "completed") || conclusion == "" {
		return checkRunContextResult{Run: latest, Pending: true}
	}
	if ignoredCheckRunConclusion(conclusion) {
		return checkRunContextResult{}
	}
	return checkRunContextResult{Run: latest, Settled: true}
}

func latestCheckRun(checkRuns []restCheckRun) restCheckRun {
	if len(checkRuns) == 0 {
		return restCheckRun{}
	}
	latest := checkRuns[0]
	for _, checkRun := range checkRuns[1:] {
		if restCheckRunAfter(checkRun, latest) {
			latest = checkRun
		}
	}
	return latest
}

func ignoredCheckRunConclusion(conclusion string) bool {
	switch strings.ToLower(strings.TrimSpace(conclusion)) {
	case "cancelled", "canceled", "skipped":
		return true
	default:
		return false
	}
}

func restCheckRunAfter(left restCheckRun, right restCheckRun) bool {
	leftAt := checkRunOrderTime(left)
	rightAt := checkRunOrderTime(right)
	switch {
	case leftAt != nil && rightAt != nil && !leftAt.Equal(*rightAt):
		return leftAt.After(*rightAt)
	case leftAt != nil && rightAt == nil:
		return true
	case leftAt == nil && rightAt != nil:
		return false
	default:
		return left.ID > right.ID
	}
}

func checkRunOrderTime(checkRun restCheckRun) *time.Time {
	for _, at := range []*time.Time{checkRun.CreatedAt, checkRun.StartedAt, checkRun.CompletedAt} {
		if at != nil {
			return at
		}
	}
	return nil
}

func earliestGitHubTime(current *time.Time, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.Before(*current) {
		value := *candidate
		return &value
	}
	return current
}

func latestGitHubTime(current *time.Time, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.After(*current) {
		value := *candidate
		return &value
	}
	return current
}

func commitStatusesState(statuses []restCommitStatus) string {
	if len(statuses) == 0 {
		return ""
	}
	latestByContext := latestCommitStatusesByContext(statuses)
	pending := false
	failed := false
	for _, status := range latestByContext {
		switch strings.ToLower(strings.TrimSpace(status.State)) {
		case "success":
		case "pending":
			pending = true
		case "":
			pending = true
		default:
			failed = true
		}
	}
	if failed {
		return "failure"
	}
	if pending {
		return "pending"
	}
	return "success"
}

func latestCommitStatusesByContext(statuses []restCommitStatus) map[string]restCommitStatus {
	latestByContext := map[string]restCommitStatus{}
	for index, status := range statuses {
		context := strings.TrimSpace(status.Context)
		if context == "" {
			context = strconv.Itoa(index)
		}
		previous, ok := latestByContext[context]
		if !ok || restCommitStatusAfter(status, previous) {
			latestByContext[context] = status
		}
	}
	return latestByContext
}

func restCommitStatusAfter(left restCommitStatus, right restCommitStatus) bool {
	if left.CreatedAt == nil {
		return false
	}
	if right.CreatedAt == nil {
		return true
	}
	return left.CreatedAt.After(*right.CreatedAt)
}

func normalizeRequiredStatusChecks(checks []string) []string {
	normalized := make([]string, 0, len(checks))
	seen := make(map[string]struct{}, len(checks))
	for _, check := range checks {
		check = strings.TrimSpace(check)
		if check == "" {
			continue
		}
		if _, ok := seen[check]; ok {
			continue
		}
		seen[check] = struct{}{}
		normalized = append(normalized, check)
	}
	return normalized
}

func combinedCIState(checkRuns string, statuses string) string {
	states := []string{checkRuns, statuses}
	hasSuccess := false
	hasPending := false
	hasFailure := false
	for _, state := range states {
		switch strings.ToLower(strings.TrimSpace(state)) {
		case "failure", "failed", "error":
			hasFailure = true
		case "pending", "expected", "queued", "waiting", "in_progress", "in progress":
			hasPending = true
		case "success", "green", "pass", "passed":
			hasSuccess = true
		}
	}
	if hasFailure {
		return "failure"
	}
	if hasPending {
		return "pending"
	}
	if hasSuccess {
		return "success"
	}
	return ""
}

func normalizePullRequestCIStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "success", "green", "pass", "passed":
		return "pass"
	case "failure", "failed", "error", "red":
		return "fail"
	case "pending", "expected", "queued", "waiting", "in_progress", "in progress":
		return "pending"
	default:
		return ""
	}
}
