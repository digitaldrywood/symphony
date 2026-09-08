package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/digitaldrywood/detent/internal/citrigger"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/reviewseverity"
	"github.com/digitaldrywood/detent/internal/securityaudit"
)

type issuePullRequestCandidate struct {
	Index             int
	Identifier        string
	BranchPrefix      string
	PullRequestNumber int
	PullRequestRepo   pullRequestRepo
}

type pullRequestKey struct {
	Repo   pullRequestRepo
	Number int
}

type pullRequestDiffFingerprintCacheKey struct {
	Repository string
	Number     int
	HeadSHA    string
	BaseSHA    string
}

type pullRequestRepo struct {
	Owner string
	Name  string
}

const (
	linkedPullRequestHydrationConcurrency     = 8
	linkedPullRequestHydrationRequestEstimate = 6
	codexReviewSummaryMarker                  = "<!-- codex-pull-request-review-summary -->"
	codexReviewSummaryHeading                 = "## Codex Review Summary"
	codexReviewSummaryTableHeader             = "| Review | Status | Commit | Review trigger |"
	codexReviewSummaryTableSeparator          = "| --- | --- | --- | --- |"
	minimumCodexReviewCommitPrefixLength      = 7
)

const pullRequestReviewThreadsQuery = `
query DetentGitHubPullRequestReviewThreads($owner: String!, $name: String!, $number: Int!, $after: String) {
	repository(owner: $owner, name: $name) {
		pullRequest(number: $number) {
			headRefOid
			reviewThreads(first: 100, after: $after) {
				pageInfo { hasNextPage endCursor }
				nodes { isResolved isOutdated path line originalLine comments(first: 1) { nodes { body } } }
			}
		}
	}
	rateLimit { limit used remaining cost resetAt }
}`

var (
	codexReviewSummaryStatusPattern = regexp.MustCompile(`^✅\s+\*\*Completed\*\*\s+<relative-time datetime="([^"]+)">([^<]+)</relative-time>$`)
	codexReviewSummaryCommitPattern = regexp.MustCompile("^`([0-9a-fA-F]{7,40})`$")
)

type linkedPullRequestHydration struct {
	repo        pullRequestRepo
	number      int
	pullRequest pullRequestNode
	state       pullRequestHydrationState
}

func (c *Connector) attachPullRequests(ctx context.Context, issues []connector.Issue) error {
	return c.attachPullRequestsWithCache(ctx, issues, true)
}

func (c *Connector) attachFreshPullRequests(ctx context.Context, issues []connector.Issue) error {
	return c.attachPullRequestsWithCache(ctx, issues, false)
}

func (c *Connector) attachPullRequestsWithCache(ctx context.Context, issues []connector.Issue, useStatusCache bool) error {
	if c.usesLabelStatus() {
		if err := c.attachLabelIssuePullRequestReferences(ctx, issues); err != nil {
			return err
		}
	}
	byRepo := make(map[pullRequestRepo][]issuePullRequestCandidate)
	for index, issue := range issues {
		repo, ok := pullRequestRepoFromIdentifier(issue.Identifier)
		if !ok {
			continue
		}
		branchPrefix := detentIssueBranchPrefix(issue.Identifier)
		pullRequestNumber := 0
		linkedPullRequestRepo := repo
		if issue.PRNumber != nil {
			pullRequestNumber = *issue.PRNumber
		}
		if owner, name, ok := splitRepositoryName(issue.PRRepository); ok {
			linkedPullRequestRepo = pullRequestRepo{Owner: owner, Name: name}
		}
		if normalizeStateName(issue.State) == normalizeStateName("Blocked") && pullRequestNumber <= 0 && !statusLabelConflictIssue(issue) {
			branchPrefix = ""
		}
		if branchPrefix == "" && pullRequestNumber <= 0 {
			continue
		}
		identifier := strings.TrimSpace(issue.Identifier)
		if identifier == "" {
			identifier = strings.TrimSpace(issue.ID)
		}
		byRepo[repo] = append(byRepo[repo], issuePullRequestCandidate{
			Index:             index,
			Identifier:        identifier,
			BranchPrefix:      branchPrefix,
			PullRequestNumber: pullRequestNumber,
			PullRequestRepo:   linkedPullRequestRepo,
		})
	}
	if len(byRepo) == 0 {
		return nil
	}

	repos := make([]pullRequestRepo, 0, len(byRepo))
	for repo := range byRepo {
		repos = append(repos, repo)
	}
	sort.Slice(repos, func(i, j int) bool {
		left := repos[i].Owner + "/" + repos[i].Name
		right := repos[j].Owner + "/" + repos[j].Name
		return left < right
	})

	for _, repo := range repos {
		candidates := c.rotatePullRequestHydrationCandidates(repo, byRepo[repo])
		nextCursor := ""
		branchFirst := firstPullRequestCandidateNeedsBranchHydration(issues, candidates)
		if branchFirst {
			var err error
			nextCursor, err = c.attachBranchPullRequests(ctx, repo, issues, candidates, useStatusCache)
			if err != nil {
				return err
			}
		}

		linkedCursor, err := c.attachLinkedPullRequests(ctx, repo, issues, candidates, useStatusCache)
		if err != nil {
			return err
		}
		if nextCursor == "" {
			nextCursor = linkedCursor
		}
		if !branchFirst {
			branchCursor, err := c.attachBranchPullRequests(ctx, repo, issues, candidates, useStatusCache)
			if err != nil {
				return err
			}
			if nextCursor == "" {
				nextCursor = branchCursor
			}
		}
		c.setPullRequestHydrationCursor(repo, nextCursor)
	}
	return nil
}

func (c *Connector) attachBranchPullRequests(
	ctx context.Context,
	repo pullRequestRepo,
	issues []connector.Issue,
	candidates []issuePullRequestCandidate,
	useStatusCache bool,
) (string, error) {
	if !hasUnattachedBranchPullRequestCandidates(issues, candidates) {
		return "", nil
	}
	if state, ok := c.currentPullRequestHydrationState(repo); ok {
		c.logPullRequestHydrationSkip(ctx, repo, state, "shared_backoff")
		markPullRequestHydrationUnavailableForCandidates(issues, candidates, repo, state)
		return firstUnattachedBranchPullRequestCandidate(issues, candidates), nil
	}
	pullRequests, err := c.fetchRepositoryPullRequests(ctx, repo)
	if err != nil {
		if state := c.pullRequestHydrationStateForError(repo, err); state.Reason != "" {
			cursor := ""
			if restFanoutOrReserveDeferred(err) {
				cursor = firstUnattachedBranchPullRequestCandidate(issues, candidates)
			}
			markPullRequestHydrationUnavailableForCandidates(issues, candidates, repo, state)
			return cursor, nil
		}
		return "", err
	}
	return c.attachMatchingPullRequests(ctx, repo, issues, candidates, pullRequests, useStatusCache)
}

func firstPullRequestCandidateNeedsBranchHydration(issues []connector.Issue, candidates []issuePullRequestCandidate) bool {
	if len(candidates) == 0 {
		return false
	}
	candidate := candidates[0]
	return issues[candidate.Index].PullRequest == nil &&
		candidate.PullRequestNumber <= 0 &&
		strings.TrimSpace(candidate.BranchPrefix) != ""
}

func firstUnattachedBranchPullRequestCandidate(issues []connector.Issue, candidates []issuePullRequestCandidate) string {
	for _, candidate := range candidates {
		if issues[candidate.Index].PullRequest == nil &&
			candidate.PullRequestNumber <= 0 &&
			strings.TrimSpace(candidate.BranchPrefix) != "" {
			return candidate.Identifier
		}
	}
	for _, candidate := range candidates {
		if issues[candidate.Index].PullRequest == nil && strings.TrimSpace(candidate.BranchPrefix) != "" {
			return candidate.Identifier
		}
	}
	return ""
}

func (c *Connector) attachPullRequestMergeStates(ctx context.Context, issues []connector.Issue) error {
	byRepo := make(map[pullRequestRepo][]issuePullRequestCandidate)
	for index, issue := range issues {
		if issue.PullRequest != nil {
			continue
		}
		repo, ok := pullRequestRepoFromIdentifier(issue.Identifier)
		if !ok {
			continue
		}
		branchPrefix := detentIssueBranchPrefix(issue.Identifier)
		if branchPrefix == "" {
			continue
		}
		byRepo[repo] = append(byRepo[repo], issuePullRequestCandidate{
			Index:        index,
			Identifier:   strings.TrimSpace(issue.Identifier),
			BranchPrefix: branchPrefix,
		})
	}
	if len(byRepo) == 0 {
		return nil
	}

	repos := make([]pullRequestRepo, 0, len(byRepo))
	for repo := range byRepo {
		repos = append(repos, repo)
	}
	sort.Slice(repos, func(i, j int) bool {
		left := repos[i].Owner + "/" + repos[i].Name
		right := repos[j].Owner + "/" + repos[j].Name
		return left < right
	})

	for _, repo := range repos {
		pullRequests, err := c.fetchRepositoryPullRequests(ctx, repo)
		if err != nil {
			if restFanoutOrReserveDeferred(err) {
				continue
			}
			return err
		}
		attachMatchingPullRequestMergeStates(repo, issues, byRepo[repo], pullRequests)
	}
	return nil
}

func attachMatchingPullRequestMergeStates(
	repo pullRequestRepo,
	issues []connector.Issue,
	candidates []issuePullRequestCandidate,
	pullRequests []pullRequestNode,
) {
	for _, pullRequest := range pullRequests {
		if normalizeStateName(pullRequest.State) != "merged" {
			continue
		}
		branchName := strings.TrimSpace(pullRequest.HeadRefName)
		if branchName == "" {
			continue
		}
		for _, candidate := range candidates {
			if issues[candidate.Index].PullRequest != nil {
				continue
			}
			if !branchMatchesIssuePrefix(branchName, candidate.BranchPrefix) {
				continue
			}
			issues[candidate.Index].PRSource = "detent_branch"
			issues[candidate.Index].PullRequest = &connector.PullRequest{
				Number:     pullRequest.Number,
				URL:        strings.TrimSpace(pullRequest.URL),
				BranchName: branchName,
				State:      strings.ToUpper(strings.TrimSpace(pullRequest.State)),
				ActivityAt: cloneGitHubTime(pullRequest.ActivityAt),
			}
			if issues[candidate.Index].PRNumber == nil && pullRequest.Number > 0 {
				number := pullRequest.Number
				issues[candidate.Index].PRNumber = &number
			}
			if issues[candidate.Index].PRRepository == "" {
				issues[candidate.Index].PRRepository = pullRequestRepoName(repo)
			}
		}
	}
}

func (c *Connector) fetchRepositoryPullRequests(ctx context.Context, repo pullRequestRepo) ([]pullRequestNode, error) {
	pullRequests := []pullRequestNode{}
	for page := 1; page <= pullRequestsPageLimit; page++ {
		pagePullRequests, err := c.fetchRepositoryPullRequestsPage(ctx, repo, page)
		if err != nil {
			return nil, err
		}
		pullRequests = append(pullRequests, pagePullRequests...)
		if len(pagePullRequests) < pullRequestsPageSize {
			break
		}
	}
	return pullRequests, nil
}

func (c *Connector) fetchRepositoryPullRequest(ctx context.Context, repo pullRequestRepo, number int) (pullRequestNode, error) {
	var response restPullRequest
	if err := c.client.REST(ctx, http.MethodGet, restPullRequestPath(repo, number), nil, &response); err != nil {
		return pullRequestNode{}, fmt.Errorf("fetch github pull request: %w", err)
	}
	return pullRequestNodeFromREST(response), nil
}

func (c *Connector) HydratePullRequest(ctx context.Context, issue connector.Issue) (connector.Issue, error) {
	repo, number, ok := hydratedPullRequestRef(issue)
	if !ok {
		return issue, nil
	}
	pullRequest, err := c.fetchRepositoryPullRequest(ctx, repo, number)
	if err != nil {
		if state := c.pullRequestHydrationStateForError(repo, err); state.Reason != "" {
			attachPullRequestHydrationUnavailableToIssue(&issue, repo, number, state)
			return issue, nil
		}
		return issue, fmt.Errorf("hydrate github pull request: %w", err)
	}
	if err := c.populatePullRequestStatus(ctx, repo, &pullRequest, false); err != nil {
		if state := c.pullRequestHydrationStateForError(repo, err); state.Reason != "" {
			applyPullRequestHydrationUnavailableState(&pullRequest, state)
		} else {
			return issue, fmt.Errorf("hydrate github pull request status: %w", err)
		}
	}
	attachPullRequestToIssue(&issue, repo, pullRequest)
	return issue, nil
}

func (c *Connector) LookupPullRequestByHead(
	ctx context.Context,
	repository string,
	branch string,
	headSHA string,
) (connector.PullRequest, bool, error) {
	repo, ok := pullRequestRepoFromName(repository)
	branch = strings.TrimSpace(branch)
	headSHA = strings.TrimSpace(headSHA)
	if !ok || branch == "" || headSHA == "" {
		return connector.PullRequest{}, false, errors.New("lookup github pull request by head: invalid repository, branch, or head SHA")
	}

	var response []restPullRequest
	if err := c.client.REST(ctx, http.MethodGet, restPullRequestsByHeadPath(repo, branch), nil, &response); err != nil {
		return connector.PullRequest{}, false, fmt.Errorf("lookup github pull request by head: %w", err)
	}
	for _, candidate := range response {
		pullRequest := pullRequestNodeFromREST(candidate)
		if strings.TrimSpace(pullRequest.HeadRefName) != branch || strings.TrimSpace(pullRequest.HeadSHA) != headSHA {
			continue
		}
		issue := connector.Issue{}
		attachPullRequestToIssue(&issue, repo, pullRequest)
		return *issue.PullRequest, true, nil
	}
	return connector.PullRequest{}, false, nil
}

func (c *Connector) PullRequestDiffFingerprint(ctx context.Context, issue connector.Issue) (string, error) {
	repo, number, ok := hydratedPullRequestRef(issue)
	if !ok || issue.PullRequest == nil {
		return "", errors.New("pull request diff fingerprint requires a linked pull request")
	}
	key := pullRequestDiffFingerprintCacheKey{
		Repository: pullRequestRepoName(repo),
		Number:     number,
		HeadSHA:    strings.TrimSpace(issue.PullRequest.HeadSHA),
		BaseSHA:    strings.TrimSpace(issue.PullRequest.BaseSHA),
	}
	if key.Repository == "" || key.Number <= 0 || key.HeadSHA == "" || key.BaseSHA == "" {
		return "", errors.New("pull request diff fingerprint requires current head and base OIDs")
	}
	if fingerprint := c.cachedPullRequestDiffFingerprint(key); fingerprint != "" {
		return fingerprint, nil
	}
	files, err := fetchRESTList[restPullRequestFile](ctx, c.client, restPullRequestFilesPath(repo, number))
	if err != nil {
		return "", fmt.Errorf("fetch github pull request files: %w", err)
	}
	fingerprint := pullRequestDiffFingerprint(files)
	c.cachePullRequestDiffFingerprint(key, fingerprint)
	return fingerprint, nil
}

func (c *Connector) SecurityAuditSnapshot(ctx context.Context, issue connector.Issue, maxDiffBytes int) (securityaudit.Snapshot, error) {
	repo, number, ok := hydratedPullRequestRef(issue)
	if !ok {
		return securityaudit.Snapshot{}, errors.New("security audit snapshot requires a linked pull request")
	}
	if maxDiffBytes <= 0 {
		maxDiffBytes = securityaudit.DefaultMaxDiffBytes
	}

	var before restPullRequest
	if err := c.client.REST(ctx, http.MethodGet, restPullRequestPath(repo, number), nil, &before); err != nil {
		return securityaudit.Snapshot{}, fmt.Errorf("fetch security audit pull request metadata: %w", err)
	}
	diff, truncated, err := c.client.RESTText(ctx, restPullRequestPath(repo, number), "application/vnd.github.diff", maxDiffBytes)
	if err != nil {
		return securityaudit.Snapshot{}, fmt.Errorf("fetch security audit pull request diff: %w", err)
	}
	var after restPullRequest
	if err := c.client.REST(ctx, http.MethodGet, restPullRequestPath(repo, number), nil, &after); err != nil {
		return securityaudit.Snapshot{}, fmt.Errorf("refresh security audit pull request metadata: %w", err)
	}
	if strings.TrimSpace(before.Base.SHA) == "" || strings.TrimSpace(before.Head.SHA) == "" ||
		strings.TrimSpace(before.Base.SHA) != strings.TrimSpace(after.Base.SHA) ||
		strings.TrimSpace(before.Head.SHA) != strings.TrimSpace(after.Head.SHA) {
		return securityaudit.Snapshot{}, errors.New("security audit pull request head changed while collecting textual diff")
	}
	return securityaudit.Snapshot{
		ProjectID:        "",
		IssueID:          strings.TrimSpace(issue.ID),
		Identifier:       strings.TrimSpace(issue.Identifier),
		IssueURL:         strings.TrimSpace(issue.URL),
		IssueTitle:       issue.Title,
		IssueDescription: issue.Description,
		Repository:       pullRequestRepoName(repo),
		PRNumber:         number,
		PRTitle:          before.Title,
		PRBody:           restStringValue(before.Body),
		BaseSHA:          strings.TrimSpace(before.Base.SHA),
		HeadSHA:          strings.TrimSpace(before.Head.SHA),
		Diff:             diff,
		DiffTruncated:    truncated,
	}, nil
}

func pullRequestDiffFingerprint(files []restPullRequestFile) string {
	files = append([]restPullRequestFile(nil), files...)
	sort.Slice(files, func(i, j int) bool {
		left := files[i]
		right := files[j]
		if left.Filename != right.Filename {
			return left.Filename < right.Filename
		}
		if left.PreviousFilename != right.PreviousFilename {
			return left.PreviousFilename < right.PreviousFilename
		}
		if left.Status != right.Status {
			return left.Status < right.Status
		}
		return left.SHA < right.SHA
	})
	hash := sha256.New()
	for _, file := range files {
		for _, value := range []string{file.Filename, file.PreviousFilename, file.Status, file.SHA} {
			_, _ = hash.Write([]byte(value))
			_, _ = hash.Write([]byte{0})
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *Connector) cachedPullRequestDiffFingerprint(key pullRequestDiffFingerprintCacheKey) string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	fingerprint := c.prDiffFingerprints[key]
	c.mu.RUnlock()
	return fingerprint
}

func (c *Connector) cachePullRequestDiffFingerprint(key pullRequestDiffFingerprintCacheKey, fingerprint string) {
	if c == nil || strings.TrimSpace(fingerprint) == "" {
		return
	}
	c.mu.Lock()
	if c.prDiffFingerprints == nil {
		c.prDiffFingerprints = map[pullRequestDiffFingerprintCacheKey]string{}
	}
	c.prDiffFingerprints[key] = strings.TrimSpace(fingerprint)
	c.mu.Unlock()
}

func (c *Connector) MergePullRequest(ctx context.Context, repository string, number int, headSHA string, mergeMethod string) error {
	repo, ok := pullRequestRepoFromName(repository)
	if !ok || number <= 0 {
		return fmt.Errorf("merge github pull request: invalid pull request %s#%d", strings.TrimSpace(repository), number)
	}
	mergeMethod = strings.ToLower(strings.TrimSpace(mergeMethod))
	if mergeMethod == "" {
		mergeMethod = "squash"
	}
	switch mergeMethod {
	case "squash", "merge", "rebase":
	default:
		return errors.New("merge github pull request: merge method must be one of squash, merge, rebase")
	}
	body := map[string]string{
		"merge_method": mergeMethod,
	}
	if headSHA = strings.TrimSpace(headSHA); headSHA != "" {
		body["sha"] = headSHA
	}
	var response restPullRequestMergeResponse
	if err := c.client.REST(ctx, http.MethodPut, restPullRequestMergePath(repo, number), body, &response); err != nil {
		var status *StatusError
		if errors.As(err, &status) && status.StatusCode == http.StatusMethodNotAllowed {
			message := strings.ToLower(status.Body)
			if strings.Contains(message, "head branch is out of date") || strings.Contains(message, "base branch was modified") {
				return fmt.Errorf("merge github pull request: %w: %w", connector.ErrPullRequestBaseOutOfDate, err)
			}
		}
		return fmt.Errorf("merge github pull request: %w", err)
	}
	if !response.Merged {
		message := strings.TrimSpace(response.Message)
		if message == "" {
			message = "github did not merge pull request"
		}
		return fmt.Errorf("merge github pull request: %s", message)
	}
	return nil
}

func (c *Connector) RerunPullRequestChecks(ctx context.Context, issue connector.Issue, checks []connector.PullRequestCheck) error {
	repo, _, ok := hydratedPullRequestRef(issue)
	if !ok {
		return errors.New("rerun github pull request checks: missing pull request repository")
	}
	seenRuns := map[int64]struct{}{}
	seenChecks := map[int64]struct{}{}
	var errs []error
	for _, check := range checks {
		if check.WorkflowRunID > 0 {
			if _, ok := seenRuns[check.WorkflowRunID]; ok {
				continue
			}
			seenRuns[check.WorkflowRunID] = struct{}{}
			if err := c.client.REST(ctx, http.MethodPost, restWorkflowRunRerunFailedJobsPath(repo, check.WorkflowRunID), nil, nil); err != nil {
				errs = append(errs, fmt.Errorf("rerun workflow run %d: %w", check.WorkflowRunID, err))
			}
			continue
		}
		if check.ID <= 0 {
			continue
		}
		if _, ok := seenChecks[check.ID]; ok {
			continue
		}
		seenChecks[check.ID] = struct{}{}
		if err := c.client.REST(ctx, http.MethodPost, restCheckRunRerequestPath(repo, check.ID), nil, nil); err != nil {
			errs = append(errs, fmt.Errorf("rerequest check run %d: %w", check.ID, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("rerun github pull request checks: %w", errors.Join(errs...))
	}
	return nil
}

func (c *Connector) ReapplyPullRequestLabel(ctx context.Context, repository string, number int, labelName string, stagger time.Duration) error {
	repo, ok := pullRequestRepoFromName(repository)
	labelName = strings.TrimSpace(labelName)
	if !ok || number <= 0 || labelName == "" {
		return fmt.Errorf("reapply github pull request label: invalid pull request %s#%d or label", strings.TrimSpace(repository), number)
	}
	return citrigger.Reapply(ctx, citrigger.Options{
		CoordinationDir: c.triggerLabelDir,
		Repository:      repository,
		Stagger:         stagger,
	}, citrigger.Dependencies{}, func(ctx context.Context) error {
		ref := issueRef{Owner: repo.Owner, Name: repo.Name, Number: number}
		issue, err := c.fetchRESTIssue(ctx, ref)
		if err != nil {
			return fmt.Errorf("fetch github pull request labels: %w", err)
		}
		if stringSliceContainsFold(labelNames(issue.Labels), labelName) {
			if err := c.client.REST(ctx, http.MethodDelete, restIssueLabelPath(ref, labelName), nil, nil); err != nil {
				return fmt.Errorf("remove github pull request label: %w", err)
			}
		}
		var response []label
		if err := c.client.REST(ctx, http.MethodPost, restIssueLabelsPath(ref), map[string]any{"labels": []string{labelName}}, &response); err != nil {
			return fmt.Errorf("add github pull request label: %w", err)
		}
		if !stringSliceContainsFold(labelNames(nodeConnection[label]{Nodes: response}), labelName) {
			return errors.New("add github pull request label: response did not include label")
		}
		return nil
	})
}

func hydratedPullRequestRef(issue connector.Issue) (pullRequestRepo, int, bool) {
	number := 0
	if issue.PullRequest != nil && issue.PullRequest.Number > 0 {
		number = issue.PullRequest.Number
	}
	if number <= 0 && issue.PRNumber != nil {
		number = *issue.PRNumber
	}
	if number <= 0 {
		return pullRequestRepo{}, 0, false
	}
	if repo, ok := pullRequestRepoFromName(issue.PRRepository); ok {
		return repo, number, true
	}
	if repo, ok := pullRequestRepoFromIdentifier(issue.Identifier); ok {
		return repo, number, true
	}
	return pullRequestRepo{}, 0, false
}

func (c *Connector) fetchRepositoryPullRequestsPage(
	ctx context.Context,
	repo pullRequestRepo,
	page int,
) ([]pullRequestNode, error) {
	var response []restPullRequest
	if err := c.client.REST(ctx, http.MethodGet, restPullRequestsPath(repo, page), nil, &response); err != nil {
		return nil, fmt.Errorf("fetch github pull requests: %w", err)
	}
	pullRequests := make([]pullRequestNode, 0, len(response))
	for _, pullRequest := range response {
		pullRequests = append(pullRequests, pullRequestNodeFromREST(pullRequest))
	}
	return pullRequests, nil
}

func (c *Connector) attachLinkedPullRequests(
	ctx context.Context,
	repo pullRequestRepo,
	issues []connector.Issue,
	candidates []issuePullRequestCandidate,
	useStatusCache bool,
) (string, error) {
	hydrations := make([]linkedPullRequestHydration, 0, len(candidates))
	hydrationByKey := make(map[pullRequestKey]int, len(candidates))
	hydrationByCandidate := make(map[int]int, len(candidates))
	for _, candidate := range candidates {
		if issues[candidate.Index].PullRequest != nil || candidate.PullRequestNumber <= 0 {
			continue
		}
		pullRequestRepo := candidate.PullRequestRepo
		if strings.TrimSpace(pullRequestRepo.Owner) == "" || strings.TrimSpace(pullRequestRepo.Name) == "" {
			pullRequestRepo = repo
		}
		if state, ok := c.currentPullRequestHydrationState(pullRequestRepo); ok {
			c.logPullRequestHydrationSkip(ctx, pullRequestRepo, state, "linked_pull_request")
			attachPullRequestHydrationUnavailableToIssue(&issues[candidate.Index], pullRequestRepo, candidate.PullRequestNumber, state)
			continue
		}
		key := pullRequestKey{Repo: pullRequestRepo, Number: candidate.PullRequestNumber}
		hydrationIndex, ok := hydrationByKey[key]
		if !ok {
			hydrationIndex = len(hydrations)
			hydrationByKey[key] = hydrationIndex
			hydrations = append(hydrations, linkedPullRequestHydration{
				repo:   pullRequestRepo,
				number: candidate.PullRequestNumber,
			})
		}
		hydrationByCandidate[candidate.Index] = hydrationIndex
	}

	concurrentStart := 0
	if c.linkedPullRequestHydrationUsesFiniteFanoutCap() && len(hydrations) > 0 {
		if err := c.hydrateLinkedPullRequest(ctx, &hydrations[0], useStatusCache); err != nil {
			return "", err
		}
		concurrentStart = 1
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(c.linkedPullRequestHydrationLimit())
	for index := concurrentStart; index < len(hydrations); index++ {
		group.Go(func() error {
			return c.hydrateLinkedPullRequest(groupCtx, &hydrations[index], useStatusCache)
		})
	}
	if err := group.Wait(); err != nil {
		return "", err
	}

	nextCursor := ""
	for _, candidate := range candidates {
		hydrationIndex, ok := hydrationByCandidate[candidate.Index]
		if !ok {
			continue
		}
		hydration := hydrations[hydrationIndex]
		if hydration.state.Reason != "" {
			if pullRequestHydrationBudgetDeferred(hydration.state.Reason) && nextCursor == "" {
				nextCursor = candidate.Identifier
			}
			if hydration.pullRequest.Number <= 0 {
				attachPullRequestHydrationUnavailableToIssue(&issues[candidate.Index], hydration.repo, candidate.PullRequestNumber, hydration.state)
				continue
			}
		}
		attachPullRequestToIssue(&issues[candidate.Index], hydration.repo, hydration.pullRequest)
	}
	return nextCursor, nil
}

func (c *Connector) hydrateLinkedPullRequest(ctx context.Context, hydration *linkedPullRequestHydration, useStatusCache bool) error {
	pullRequest, err := c.fetchRepositoryPullRequest(ctx, hydration.repo, hydration.number)
	if err != nil {
		state := c.pullRequestHydrationStateForError(hydration.repo, err)
		if state.Reason == "" {
			return err
		}
		hydration.state = state
		return nil
	}
	if err := c.populatePullRequestStatus(ctx, hydration.repo, &pullRequest, useStatusCache); err != nil {
		state := c.pullRequestHydrationStateForError(hydration.repo, err)
		if state.Reason == "" {
			return err
		}
		hydration.state = state
		applyPullRequestHydrationUnavailableState(&pullRequest, state)
	}
	hydration.pullRequest = pullRequest
	return nil
}

func (c *Connector) linkedPullRequestHydrationUsesFiniteFanoutCap() bool {
	return c != nil && c.client != nil && c.client.restPolicy.FanoutMaxRequests > 0
}

func (c *Connector) linkedPullRequestHydrationLimit() int {
	limit := linkedPullRequestHydrationConcurrency
	if c == nil || c.client == nil {
		return limit
	}
	if fanoutLimit := c.client.restPolicy.FanoutMaxRequests; fanoutLimit > 0 {
		limit = int(fanoutLimit) / linkedPullRequestHydrationRequestEstimate
		if limit < 1 {
			return 1
		}
		if limit > linkedPullRequestHydrationConcurrency {
			return linkedPullRequestHydrationConcurrency
		}
	}
	return limit
}

func (c *Connector) attachMatchingPullRequests(
	ctx context.Context,
	repo pullRequestRepo,
	issues []connector.Issue,
	candidates []issuePullRequestCandidate,
	pullRequests []pullRequestNode,
	useStatusCache bool,
) (string, error) {
	hydrated := map[int]pullRequestNode{}
	for _, candidate := range candidates {
		for _, pullRequest := range pullRequests {
			if issues[candidate.Index].PullRequest != nil {
				break
			}
			branchName := strings.TrimSpace(pullRequest.HeadRefName)
			if branchName == "" {
				continue
			}
			if !branchMatchesIssuePrefix(branchName, candidate.BranchPrefix) {
				continue
			}
			issues[candidate.Index].PRSource = "detent_branch"

			hydratedPullRequest, ok := hydrated[pullRequest.Number]
			if !ok {
				var err error
				hydratedPullRequest, err = c.fetchRepositoryPullRequest(ctx, repo, pullRequest.Number)
				if err != nil {
					if state := c.pullRequestHydrationStateForError(repo, err); state.Reason != "" {
						applyPullRequestHydrationUnavailableState(&pullRequest, state)
						hydrated[pullRequest.Number] = pullRequest
						attachPullRequestToIssue(&issues[candidate.Index], repo, pullRequest)
						markPullRequestHydrationUnavailableForCandidates(issues, candidates, repo, state)
						if restFanoutOrReserveDeferred(err) {
							return candidate.Identifier, nil
						}
						return "", nil
					}
					return "", err
				}
				if err := c.populatePullRequestStatus(ctx, repo, &hydratedPullRequest, useStatusCache); err != nil {
					if state := c.pullRequestHydrationStateForError(repo, err); state.Reason != "" {
						applyPullRequestHydrationUnavailableState(&hydratedPullRequest, state)
						hydrated[pullRequest.Number] = hydratedPullRequest
						attachPullRequestToIssue(&issues[candidate.Index], repo, hydratedPullRequest)
						markPullRequestHydrationUnavailableForCandidates(issues, candidates, repo, state)
						if restFanoutOrReserveDeferred(err) {
							return candidate.Identifier, nil
						}
						return "", nil
					} else {
						return "", err
					}
				}
				hydrated[pullRequest.Number] = hydratedPullRequest
			}
			attachPullRequestToIssue(&issues[candidate.Index], repo, hydratedPullRequest)
			break
		}
	}
	return "", nil
}

func (c *Connector) rotatePullRequestHydrationCandidates(repo pullRequestRepo, candidates []issuePullRequestCandidate) []issuePullRequestCandidate {
	if c == nil || len(candidates) < 2 {
		return candidates
	}
	key := pullRequestRepoName(repo)
	c.mu.RLock()
	cursor := c.prHydrationCursor[key]
	c.mu.RUnlock()
	if cursor == "" {
		return candidates
	}
	for index, candidate := range candidates {
		if candidate.Identifier != cursor || index == 0 {
			continue
		}
		rotated := make([]issuePullRequestCandidate, 0, len(candidates))
		rotated = append(rotated, candidates[index:]...)
		rotated = append(rotated, candidates[:index]...)
		return rotated
	}
	return candidates
}

func (c *Connector) setPullRequestHydrationCursor(repo pullRequestRepo, identifier string) {
	if c == nil {
		return
	}
	key := pullRequestRepoName(repo)
	if key == "" {
		return
	}
	c.mu.Lock()
	if identifier == "" {
		delete(c.prHydrationCursor, key)
	} else {
		if c.prHydrationCursor == nil {
			c.prHydrationCursor = make(map[string]string)
		}
		c.prHydrationCursor[key] = identifier
	}
	c.mu.Unlock()
}

func hasUnattachedBranchPullRequestCandidates(issues []connector.Issue, candidates []issuePullRequestCandidate) bool {
	for _, candidate := range candidates {
		if issues[candidate.Index].PullRequest == nil && strings.TrimSpace(candidate.BranchPrefix) != "" {
			return true
		}
	}
	return false
}

func pullRequestNodeFromREST(pullRequest restPullRequest) pullRequestNode {
	return pullRequestNode{
		NodeID:         pullRequest.NodeID,
		Number:         pullRequest.Number,
		URL:            pullRequest.HTMLURL,
		State:          restPullRequestState(pullRequest),
		MergeableState: strings.ToLower(strings.TrimSpace(pullRequest.MergeableState)),
		Draft:          pullRequest.Draft,
		Labels:         labelNames(nodeConnection[label]{Nodes: pullRequest.Labels}),
		ActivityAt:     cloneGitHubTime(pullRequest.UpdatedAt),
		HeadRefName:    pullRequest.Head.Ref,
		BaseRefName:    pullRequest.Base.Ref,
		HeadSHA:        pullRequest.Head.SHA,
		BaseSHA:        pullRequest.Base.SHA,
	}
}

func attachPullRequestToIssue(issue *connector.Issue, repo pullRequestRepo, pullRequest pullRequestNode) {
	issue.PullRequest = &connector.PullRequest{
		NodeID:                       strings.TrimSpace(pullRequest.NodeID),
		Number:                       pullRequest.Number,
		URL:                          strings.TrimSpace(pullRequest.URL),
		BranchName:                   strings.TrimSpace(pullRequest.HeadRefName),
		BaseRef:                      strings.TrimSpace(pullRequest.BaseRefName),
		State:                        strings.ToUpper(strings.TrimSpace(pullRequest.State)),
		MergeableState:               strings.ToLower(strings.TrimSpace(pullRequest.MergeableState)),
		Draft:                        pullRequest.Draft,
		Labels:                       append([]string{}, pullRequest.Labels...),
		ActivityAt:                   cloneGitHubTime(pullRequest.ActivityAt),
		HeadSHA:                      strings.TrimSpace(pullRequest.HeadSHA),
		BaseSHA:                      strings.TrimSpace(pullRequest.BaseSHA),
		HydrationUnavailableReason:   strings.TrimSpace(pullRequest.HydrationUnavailableReason),
		HydrationDegradedReason:      strings.TrimSpace(pullRequest.HydrationDegradedReason),
		HydrationNextRetryAt:         cloneGitHubTime(pullRequest.HydrationNextRetryAt),
		CIStatus:                     normalizePullRequestCIStatus(pullRequestCIState(pullRequest)),
		Checks:                       append([]connector.PullRequestCheck(nil), pullRequest.CI.Checks...),
		CheckRunCount:                pullRequest.CI.CheckRunCount,
		StatusContextCount:           pullRequest.CI.StatusContextCount,
		CIQueueSeconds:               pullRequest.CI.CIQueueSeconds,
		CIDurationSeconds:            pullRequest.CI.CIDurationSeconds,
		SlowChecks:                   append([]connector.PullRequestCheck(nil), pullRequest.CI.SlowChecks...),
		RunningChecks:                append([]string(nil), pullRequest.CI.RunningChecks...),
		UnstartedCheckCount:          pullRequest.CI.UnstartedCheckCount,
		UnstartedChecks:              append([]connector.PullRequestCheck(nil), pullRequest.CI.UnstartedChecks...),
		StaleSuccessfulChecks:        append([]connector.PullRequestCheck(nil), pullRequest.CI.StaleSuccessfulChecks...),
		RequiredCheckFailures:        append([]connector.PullRequestCheck(nil), pullRequest.CI.RequiredFailures...),
		TransientFailedChecks:        append([]connector.PullRequestCheck(nil), pullRequest.CI.TransientFailures...),
		UnresolvedReviewThreads:      append([]connector.PullRequestReviewThread(nil), pullRequest.UnresolvedReviewThreads...),
		CodexReviewState:             pullRequestCodexReviewState(pullRequest),
		CodexReviewSource:            pullRequestCodexReviewSource(pullRequest),
		CodexReviewAPIState:          pullRequestCodexReviewAPIState(pullRequest),
		CodexReviewBodySeverity:      pullRequestCodexReviewBodySeverity(pullRequest),
		CodexReviewSubmittedAt:       pullRequestCodexReviewSubmittedAt(pullRequest),
		CodexReviewFindings:          pullRequestCodexReviewFindings(pullRequest),
		LatestCodexReviewState:       pullRequestLatestCodexReviewState(pullRequest),
		LatestCodexReviewCommitSHA:   pullRequestLatestCodexReviewCommitSHA(pullRequest),
		LatestCodexReviewSubmittedAt: pullRequestLatestCodexReviewSubmittedAt(pullRequest),
	}
	if issue.PRNumber == nil && pullRequest.Number > 0 {
		number := pullRequest.Number
		issue.PRNumber = &number
	}
	if issue.PRRepository == "" {
		issue.PRRepository = pullRequestRepoName(repo)
	}
}

func markPullRequestHydrationUnavailableForCandidates(
	issues []connector.Issue,
	candidates []issuePullRequestCandidate,
	defaultRepo pullRequestRepo,
	state pullRequestHydrationState,
) {
	for _, candidate := range candidates {
		if issues[candidate.Index].PullRequest != nil {
			continue
		}
		repo := candidate.PullRequestRepo
		if strings.TrimSpace(repo.Owner) == "" || strings.TrimSpace(repo.Name) == "" {
			repo = defaultRepo
		}
		attachPullRequestHydrationUnavailableToIssue(&issues[candidate.Index], repo, candidate.PullRequestNumber, state)
	}
}

func attachPullRequestHydrationUnavailableToIssue(issue *connector.Issue, repo pullRequestRepo, number int, state pullRequestHydrationState) {
	if strings.TrimSpace(state.Reason) == "" {
		return
	}
	if issue.PullRequest == nil {
		issue.PullRequest = &connector.PullRequest{}
	}
	if number > 0 {
		issue.PullRequest.Number = number
	}
	issue.PullRequest.HydrationUnavailableReason = strings.TrimSpace(state.Reason)
	issue.PullRequest.HydrationNextRetryAt = cloneGitHubTime(state.NextRetryAt)
	if issue.PRNumber == nil && number > 0 {
		issue.PRNumber = &number
	}
	if issue.PRRepository == "" {
		issue.PRRepository = pullRequestRepoName(repo)
	}
}

func applyPullRequestHydrationUnavailableState(pullRequest *pullRequestNode, state pullRequestHydrationState) {
	if pullRequest == nil || strings.TrimSpace(state.Reason) == "" {
		return
	}
	pullRequest.HydrationUnavailableReason = strings.TrimSpace(state.Reason)
	pullRequest.HydrationNextRetryAt = cloneGitHubTime(state.NextRetryAt)
}

func (c *Connector) currentPullRequestHydrationState(repo pullRequestRepo) (pullRequestHydrationState, bool) {
	if c == nil || c.prHydration == nil {
		return pullRequestHydrationState{}, false
	}
	return c.prHydration.Current(repo)
}

func (c *Connector) pullRequestHydrationStateForError(repo pullRequestRepo, err error) pullRequestHydrationState {
	switch {
	case errors.Is(err, ErrRESTFanoutDeferred):
		return pullRequestHydrationState{Reason: connector.PullRequestHydrationReasonRESTFanoutDeferred}
	case errors.Is(err, ErrRESTBudgetReserved):
		return pullRequestHydrationState{Reason: connector.PullRequestHydrationReasonRESTBudgetReserved}
	case errors.Is(err, ErrRateLimited):
		var statusErr *StatusError
		if errors.As(err, &statusErr) {
			switch statusErr.RateLimitKind {
			case restRateLimitKindSecondaryThrottled:
				return c.tripPullRequestHydrationCircuit(repo, statusErr.RetryAfter)
			case restRateLimitKindPrimaryExhausted:
				return newPullRequestHydrationState(
					connector.PullRequestHydrationReasonPrimaryExhausted,
					c.pullRequestHydrationRetryAt(statusErr),
				)
			}
		}
		return pullRequestHydrationState{Reason: connector.PullRequestHydrationReasonRateLimited}
	default:
		return pullRequestHydrationState{}
	}
}

func restFanoutOrReserveDeferred(err error) bool {
	return errors.Is(err, ErrRESTFanoutDeferred) || errors.Is(err, ErrRESTBudgetReserved)
}

func pullRequestHydrationBudgetDeferred(reason string) bool {
	return reason == connector.PullRequestHydrationReasonRESTFanoutDeferred || reason == connector.PullRequestHydrationReasonRESTBudgetReserved
}

func (c *Connector) tripPullRequestHydrationCircuit(repo pullRequestRepo, retryAfter time.Duration) pullRequestHydrationState {
	reason := connector.PullRequestHydrationReasonSecondaryThrottled
	if c == nil || c.prHydration == nil {
		return pullRequestHydrationState{Reason: reason}
	}
	state := c.prHydration.Trip(repo, reason, retryAfter)
	if strings.TrimSpace(state.Reason) == "" {
		return pullRequestHydrationState{Reason: reason}
	}
	return state
}

func (c *Connector) pullRequestHydrationRetryAt(statusErr *StatusError) time.Time {
	if statusErr == nil {
		return time.Time{}
	}
	now := time.Now()
	if c != nil && c.prHydration != nil && c.prHydration.now != nil {
		now = c.prHydration.now()
	}
	if statusErr.RetryAfter > 0 {
		return now.Add(statusErr.RetryAfter)
	}
	if statusErr.ResetAt.After(now) {
		return statusErr.ResetAt
	}
	return time.Time{}
}

func pullRequestRepoName(repo pullRequestRepo) string {
	owner := strings.TrimSpace(repo.Owner)
	name := strings.TrimSpace(repo.Name)
	if owner == "" || name == "" {
		return ""
	}
	return owner + "/" + name
}

func samePullRequestRepo(left pullRequestRepo, right pullRequestRepo) bool {
	return strings.EqualFold(left.Owner, right.Owner) && strings.EqualFold(left.Name, right.Name)
}

func (c *Connector) populatePullRequestStatus(ctx context.Context, repo pullRequestRepo, pullRequest *pullRequestNode, useStatusCache bool) error {
	previousStatus, hadPreviousStatus := pullRequestStatus{}, false
	if c.pullRequests != nil {
		previousStatus, hadPreviousStatus = c.pullRequests.Peek(repo, pullRequest.Number, pullRequest.HeadSHA)
	}
	if useStatusCache && c.pullRequests != nil {
		if status, ok := c.pullRequests.Get(repo, pullRequest.Number, pullRequest.HeadSHA); ok {
			c.logPullRequestCache(ctx, repo, pullRequest, true, false, "")
			applyPullRequestStatus(pullRequest, status)
			return nil
		}
		c.logPullRequestCache(ctx, repo, pullRequest, false, false, "")
	}

	status := pullRequestStatus{}
	checksUnavailable := false
	if strings.TrimSpace(pullRequest.HeadSHA) != "" {
		c.deletePullRequestCIConditionalEntries(repo, pullRequest.HeadSHA)
		ci, err := c.fetchPullRequestCI(ctx, repo, pullRequest.HeadSHA)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				pullRequest.HydrationUnavailableReason = connector.PullRequestHydrationReasonChecksUnavailable
				checksUnavailable = true
			} else {
				state := c.pullRequestHydrationStateForError(repo, err)
				if c.applyCachedPullRequestStatusAfterThrottle(ctx, repo, pullRequest, state) {
					return nil
				}
				return err
			}
		} else {
			status.ci = ci
			if hadPreviousStatus {
				c.logPullRequestCheckReconciliation(ctx, repo, pullRequest, previousStatus.ci, ci)
			}
		}
	}
	reviews, err := c.fetchPullRequestReviews(ctx, repo, pullRequest.Number, pullRequest.HeadSHA)
	if err != nil {
		state := c.pullRequestHydrationStateForError(repo, err)
		if c.applyCachedPullRequestStatusAfterThrottle(ctx, repo, pullRequest, state) {
			return nil
		}
		return err
	}
	status.reviews = reviews
	if c.pullRequests != nil && !checksUnavailable {
		c.pullRequests.Set(repo, pullRequest.Number, pullRequest.HeadSHA, status)
		c.logPullRequestCache(ctx, repo, pullRequest, false, false, "stored")
	}
	applyPullRequestStatus(pullRequest, status)
	return nil
}

func (c *Connector) deletePullRequestCIConditionalEntries(repo pullRequestRepo, headSHA string) {
	if c == nil || c.client == nil {
		return
	}
	c.client.deleteRESTConditionalEntriesForEndpoint(http.MethodGet, restCommitCheckRunsPath(repo, headSHA))
	c.client.deleteRESTConditionalEntriesForEndpoint(http.MethodGet, restCommitStatusesPath(repo, headSHA))
}

func (c *Connector) logPullRequestCheckReconciliation(ctx context.Context, repo pullRequestRepo, pullRequest *pullRequestNode, previous pullRequestCI, current pullRequestCI) {
	if c == nil || c.logger == nil || pullRequest == nil {
		return
	}
	currentNames := make(map[string]struct{}, len(current.Checks))
	for _, check := range current.Checks {
		if name := strings.TrimSpace(check.Name); name != "" {
			currentNames[name] = struct{}{}
		}
	}
	removedChecks := make([]string, 0)
	for _, check := range previous.Checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		if _, ok := currentNames[name]; !ok {
			removedChecks = append(removedChecks, name)
		}
	}
	removedChecks = uniqueNonBlank(removedChecks)
	if len(removedChecks) == 0 {
		return
	}
	removedNames := make(map[string]struct{}, len(removedChecks))
	for _, name := range removedChecks {
		removedNames[name] = struct{}{}
	}
	removedRunningChecks := make([]string, 0)
	for _, name := range previous.RunningChecks {
		if _, ok := removedNames[name]; ok {
			removedRunningChecks = append(removedRunningChecks, name)
		}
	}
	c.logger.InfoContext(ctx, "github pull request checks reconciled",
		"endpoint_family", "check runs",
		"request_purpose", "reconcile_current_head_checks",
		"repository", pullRequestRepoName(repo),
		"pr_number", pullRequest.Number,
		"head_sha", strings.TrimSpace(pullRequest.HeadSHA),
		"authoritative_check_count", len(current.Checks),
		"removed_checks", removedChecks,
		"removed_running_checks", uniqueNonBlank(removedRunningChecks),
	)
}

func (c *Connector) applyCachedPullRequestStatusAfterThrottle(ctx context.Context, repo pullRequestRepo, pullRequest *pullRequestNode, state pullRequestHydrationState) bool {
	if c.pullRequests == nil || pullRequest == nil {
		return false
	}
	if strings.TrimSpace(state.Reason) == "" {
		return false
	}
	status, ok := c.pullRequests.Get(repo, pullRequest.Number, pullRequest.HeadSHA)
	if !ok {
		return false
	}
	c.logPullRequestCache(ctx, repo, pullRequest, true, true, state.Reason)
	applyPullRequestStatus(pullRequest, status)
	pullRequest.HydrationDegradedReason = connector.PullRequestHydrationReasonStaleCachedPullData
	pullRequest.HydrationNextRetryAt = cloneGitHubTime(state.NextRetryAt)
	return true
}

func (c *Connector) logPullRequestHydrationSkip(ctx context.Context, repo pullRequestRepo, state pullRequestHydrationState, purpose string) {
	if c == nil || c.logger == nil {
		return
	}
	c.logger.DebugContext(ctx, "github pull request hydration skipped",
		"endpoint_family", "pull requests",
		"request_purpose", "hydrate_pull_request",
		"repository", pullRequestRepoName(repo),
		"cache_hit", true,
		"avoidable_request", true,
		"backoff_reason", strings.TrimSpace(state.Reason),
		"purpose", strings.TrimSpace(purpose),
		"retry_at", state.NextRetryAt,
	)
}

func (c *Connector) logPullRequestCache(ctx context.Context, repo pullRequestRepo, pullRequest *pullRequestNode, hit bool, staleFallback bool, reason string) {
	if c == nil || c.logger == nil || pullRequest == nil {
		return
	}
	c.logger.DebugContext(ctx, "github pull request status cache",
		"endpoint_family", "pull_request_status_cache",
		"request_purpose", "hydrate_pull_request_status",
		"repository", pullRequestRepoName(repo),
		"pr_number", pullRequest.Number,
		"head_sha_known", strings.TrimSpace(pullRequest.HeadSHA) != "",
		"cache_hit", hit,
		"avoidable_request", hit,
		"stale_fallback", staleFallback,
		"backoff_reason", strings.TrimSpace(reason),
	)
}

func applyPullRequestStatus(pullRequest *pullRequestNode, status pullRequestStatus) {
	pullRequest.CI = clonePullRequestCI(status.ci)
	pullRequest.Commits = nodeConnection[pullRequestCommit]{Nodes: []pullRequestCommit{{
		Commit: commitNode{StatusCheckRollup: &statusCheckRollup{State: status.ci.State}},
	}}}
	pullRequest.LatestReviews = nodeConnection[pullRequestReview]{Nodes: clonePullRequestReviews(status.reviews.CurrentHead)}
	pullRequest.CodexReviews = clonePullRequestCodexReviews(status.reviews)
}

func (c *Connector) HydratePullRequestReviewThreads(ctx context.Context, issue connector.Issue) (connector.Issue, error) {
	repo, number, ok := hydratedPullRequestRef(issue)
	if !ok || issue.PullRequest == nil || normalizeStateName(issue.PullRequest.State) != "open" {
		return issue, nil
	}
	threads, err := c.fetchPullRequestReviewThreads(ctx, repo, number, issue.PullRequest.HeadSHA)
	if err != nil {
		if state := c.pullRequestHydrationStateForError(repo, err); state.Reason != "" {
			issue = issueWithPullRequestReviewThreads(issue, nil)
			issue.PullRequest.HydrationUnavailableReason = state.Reason
			issue.PullRequest.HydrationNextRetryAt = cloneGitHubTime(state.NextRetryAt)
			return issue, nil
		}
		return issue, err
	}
	issue = issueWithPullRequestReviewThreads(issue, threads)
	return issue, nil
}

func issueWithPullRequestReviewThreads(issue connector.Issue, threads []connector.PullRequestReviewThread) connector.Issue {
	if issue.PullRequest == nil {
		return issue
	}
	pullRequest := *issue.PullRequest
	pullRequest.UnresolvedReviewThreads = append([]connector.PullRequestReviewThread(nil), threads...)
	issue.PullRequest = &pullRequest
	return issue
}

func (c *Connector) fetchPullRequestReviewThreads(
	ctx context.Context,
	repo pullRequestRepo,
	number int,
	headSHA string,
) ([]connector.PullRequestReviewThread, error) {
	headSHA = strings.TrimSpace(headSHA)
	if !validPullRequestRepo(repo) || number <= 0 || headSHA == "" {
		return nil, ErrInvalidResponse
	}
	var after *string
	threads := []connector.PullRequestReviewThread{}
	for {
		var response struct {
			Repository *struct {
				PullRequest *struct {
					HeadSHA       string                                  `json:"headRefOid"`
					ReviewThreads nodeConnection[pullRequestReviewThread] `json:"reviewThreads"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.client.GraphQLWithType(ctx, graphQLQueryReviewThreads, pullRequestReviewThreadsQuery, map[string]any{
			"owner":  repo.Owner,
			"name":   repo.Name,
			"number": number,
			"after":  after,
		}, &response); err != nil {
			return nil, fmt.Errorf("fetch github pull request review threads: %w", err)
		}
		if response.Repository == nil || response.Repository.PullRequest == nil {
			return nil, ErrInvalidResponse
		}
		if strings.TrimSpace(response.Repository.PullRequest.HeadSHA) != headSHA {
			return nil, fmt.Errorf("%w: pull request head changed while fetching review threads", ErrInvalidResponse)
		}
		connection := response.Repository.PullRequest.ReviewThreads
		for _, thread := range connection.Nodes {
			if thread.IsResolved {
				continue
			}
			line := thread.Line
			if line <= 0 {
				line = thread.OriginalLine
			}
			body := ""
			if len(thread.Comments.Nodes) > 0 {
				body = thread.Comments.Nodes[0].Body
			}
			threads = append(threads, connector.PullRequestReviewThread{
				Body: body,
				Path: strings.TrimSpace(thread.Path),
				Line: line,
			})
		}
		if !connection.PageInfo.HasNextPage {
			return threads, nil
		}
		cursor := strings.TrimSpace(connection.PageInfo.EndCursor)
		if cursor == "" {
			return nil, ErrInvalidResponse
		}
		after = &cursor
	}
}

func (c *Connector) fetchPullRequestReviews(ctx context.Context, repo pullRequestRepo, number int, headSHA string) (pullRequestCodexReviews, error) {
	response, err := fetchRESTList[restReview](ctx, c.client, restPullRequestReviewsPath(repo, number))
	if err != nil {
		return pullRequestCodexReviews{}, fmt.Errorf("fetch github pull request reviews: %w", err)
	}
	reviews := pullRequestCodexReviews{}
	if review, ok := latestCodexReview(response, headSHA); ok {
		reviews.CurrentHead = []pullRequestReview{review}
	}
	if review, ok := latestCodexReview(response, ""); ok {
		reviews.Latest = []pullRequestReview{review}
	}
	if len(reviews.CurrentHead) > 0 {
		return reviews, nil
	}
	comments, err := fetchRESTList[restComment](ctx, c.client, restIssueCommentsListPath(issueRef{
		Owner:  repo.Owner,
		Name:   repo.Name,
		Number: number,
	}))
	if err != nil {
		return pullRequestCodexReviews{}, fmt.Errorf("fetch github pull request review summary comments: %w", err)
	}
	if review, ok := latestCodexSummaryReview(comments, response, headSHA); ok {
		reviews.CurrentHead = []pullRequestReview{review}
		if len(reviews.Latest) == 0 {
			reviews.Latest = []pullRequestReview{review}
		}
	}
	return reviews, nil
}

type pullRequestReference struct {
	Number     int
	Repository string
	State      string
	UpdatedAt  *time.Time
}

func firstPullRequestReference(pullRequests nodeConnection[pullRequest]) (pullRequestReference, bool) {
	var fallback pullRequestReference
	fallbackOK := false
	var open pullRequestReference
	openOK := false
	var merged pullRequestReference
	mergedOK := false
	for _, pullRequest := range pullRequests.Nodes {
		if pullRequest.Number <= 0 {
			continue
		}
		ref := pullRequestReferenceFromNode(pullRequest)
		if !fallbackOK {
			fallback = ref
			fallbackOK = true
		}
		switch normalizeStateName(pullRequest.State) {
		case "open":
			if !openOK || pullRequestReferenceAfter(ref, open) {
				open = ref
				openOK = true
			}
		case "merged":
			if !mergedOK || pullRequestReferenceAfter(ref, merged) {
				merged = ref
				mergedOK = true
			}
		}
	}
	if openOK {
		return open, true
	}
	if mergedOK {
		return merged, true
	}
	return fallback, fallbackOK
}

func mergedPullRequestReference(pullRequests nodeConnection[pullRequest]) (pullRequestReference, bool) {
	var merged pullRequestReference
	mergedOK := false
	for _, pullRequest := range pullRequests.Nodes {
		if pullRequest.Number <= 0 || normalizeStateName(pullRequest.State) != "merged" {
			continue
		}
		ref := pullRequestReferenceFromNode(pullRequest)
		if !mergedOK || pullRequestReferenceAfter(ref, merged) {
			merged = ref
			mergedOK = true
		}
	}
	return merged, mergedOK
}

func pullRequestReferenceFromNode(pullRequest pullRequest) pullRequestReference {
	return pullRequestReference{
		Number:     pullRequest.Number,
		Repository: strings.TrimSpace(pullRequest.Repository.NameWithOwner),
		State:      strings.ToUpper(strings.TrimSpace(pullRequest.State)),
		UpdatedAt:  parseGitHubTime(pullRequest.UpdatedAt),
	}
}

func pullRequestReferenceAfter(left, right pullRequestReference) bool {
	if left.UpdatedAt != nil && right.UpdatedAt != nil && !left.UpdatedAt.Equal(*right.UpdatedAt) {
		return left.UpdatedAt.After(*right.UpdatedAt)
	}
	if left.UpdatedAt != nil && right.UpdatedAt == nil {
		return true
	}
	if left.UpdatedAt == nil && right.UpdatedAt != nil {
		return false
	}
	return left.Number > right.Number
}

func pullRequestCIState(pullRequest pullRequestNode) string {
	for _, commit := range pullRequest.Commits.Nodes {
		if commit.Commit.StatusCheckRollup != nil {
			return commit.Commit.StatusCheckRollup.State
		}
	}
	return ""
}

func restPullRequestState(pullRequest restPullRequest) string {
	if pullRequest.MergedAt != nil && strings.TrimSpace(*pullRequest.MergedAt) != "" {
		return "MERGED"
	}
	return strings.ToUpper(strings.TrimSpace(pullRequest.State))
}

func latestCodexReview(reviews []restReview, headSHA string) (pullRequestReview, bool) {
	headSHA = strings.TrimSpace(headSHA)
	var latest pullRequestReview
	found := false
	for _, review := range reviews {
		if !codexReviewAuthor(review.User) || strings.EqualFold(strings.TrimSpace(review.State), "DISMISSED") {
			continue
		}
		if headSHA != "" && !strings.EqualFold(strings.TrimSpace(review.CommitID), headSHA) {
			continue
		}
		candidate := pullRequestReview{
			Body:        review.Body,
			URL:         review.HTMLURL,
			State:       review.State,
			Source:      connector.PullRequestReviewSourceFormal,
			Author:      review.User,
			CommitID:    review.CommitID,
			SubmittedAt: review.SubmittedAt,
		}
		if !found || pullRequestReviewAfter(candidate, latest) {
			latest = candidate
			found = true
		}
	}
	return latest, found
}

type codexReviewSummary struct {
	commitPrefix string
	completedAt  time.Time
}

func latestCodexSummaryReview(comments []restComment, formalReviews []restReview, headSHA string) (pullRequestReview, bool) {
	comment, ok := latestTrustedCodexSummaryComment(comments)
	if !ok {
		return pullRequestReview{}, false
	}
	summary, ok := parseCodexReviewSummary(comment.Body)
	if !ok || !codexReviewSummaryMatchesHead(summary.commitPrefix, headSHA, formalReviews) {
		return pullRequestReview{}, false
	}
	if comment.CreatedAt != nil && summary.completedAt.Before(*comment.CreatedAt) {
		return pullRequestReview{}, false
	}
	if comment.UpdatedAt == nil || summary.completedAt.After(comment.UpdatedAt.Add(time.Second)) {
		return pullRequestReview{}, false
	}
	completedAt := summary.completedAt
	return pullRequestReview{
		Body:        comment.Body,
		URL:         comment.HTMLURL,
		State:       "COMMENTED",
		Source:      connector.PullRequestReviewSourceSummaryComment,
		Author:      comment.User,
		CommitID:    strings.ToLower(strings.TrimSpace(headSHA)),
		SubmittedAt: &completedAt,
	}, true
}

func latestTrustedCodexSummaryComment(comments []restComment) (restComment, bool) {
	var latest restComment
	found := false
	for _, comment := range comments {
		if !trustedCodexSummaryAuthor(comment.User) || !strings.Contains(comment.Body, codexReviewSummaryMarker) {
			continue
		}
		if !found || restCommentAfter(comment, latest) {
			latest = comment
			found = true
		}
	}
	return latest, found
}

func trustedCodexSummaryAuthor(author *actor) bool {
	if author == nil || !strings.EqualFold(strings.TrimSpace(actorType(author)), "Bot") {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(author.Login)) {
	case "chatgpt-codex-connector", "chatgpt-codex-connector[bot]":
		return true
	default:
		return false
	}
}

func restCommentAfter(left restComment, right restComment) bool {
	leftAt := firstNonNilTime(left.UpdatedAt, left.CreatedAt)
	rightAt := firstNonNilTime(right.UpdatedAt, right.CreatedAt)
	if leftAt == nil {
		return rightAt == nil && left.ID > right.ID
	}
	if rightAt == nil {
		return true
	}
	if leftAt.Equal(*rightAt) {
		return left.ID > right.ID
	}
	return leftAt.After(*rightAt)
}

func firstNonNilTime(values ...*time.Time) *time.Time {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func parseCodexReviewSummary(body string) (codexReviewSummary, bool) {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != codexReviewSummaryMarker || strings.Count(body, codexReviewSummaryMarker) != 1 {
		return codexReviewSummary{}, false
	}
	headingIndex := -1
	headerIndex := -1
	for index, line := range lines {
		switch strings.TrimSpace(line) {
		case codexReviewSummaryHeading:
			if headingIndex >= 0 {
				return codexReviewSummary{}, false
			}
			headingIndex = index
		case codexReviewSummaryTableHeader:
			if headerIndex >= 0 {
				return codexReviewSummary{}, false
			}
			headerIndex = index
		}
	}
	if headingIndex <= 0 || headerIndex <= headingIndex || headerIndex+2 >= len(lines) || strings.TrimSpace(lines[headerIndex+1]) != codexReviewSummaryTableSeparator {
		return codexReviewSummary{}, false
	}

	var summary codexReviewSummary
	found := false
	for _, line := range lines[headerIndex+2:] {
		line = strings.TrimSpace(line)
		if line == "" {
			if found {
				break
			}
			continue
		}
		if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
			break
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 4 {
			return codexReviewSummary{}, false
		}
		if strings.TrimSpace(cells[0]) != "📝 **Code Review**" {
			continue
		}
		if found {
			return codexReviewSummary{}, false
		}
		statusMatches := codexReviewSummaryStatusPattern.FindStringSubmatch(strings.TrimSpace(cells[1]))
		commitMatches := codexReviewSummaryCommitPattern.FindStringSubmatch(strings.TrimSpace(cells[2]))
		if len(statusMatches) != 3 || len(commitMatches) != 2 {
			return codexReviewSummary{}, false
		}
		completedAt, err := time.Parse(time.RFC3339Nano, statusMatches[1])
		if err != nil {
			return codexReviewSummary{}, false
		}
		displayedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(statusMatches[2]))
		if err != nil || !displayedAt.Equal(completedAt) {
			return codexReviewSummary{}, false
		}
		summary = codexReviewSummary{
			commitPrefix: strings.ToLower(commitMatches[1]),
			completedAt:  completedAt,
		}
		found = true
	}
	return summary, found
}

func codexReviewSummaryMatchesHead(prefix string, headSHA string, formalReviews []restReview) bool {
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	if len(prefix) < minimumCodexReviewCommitPrefixLength || !validFullGitSHA(headSHA) || !strings.HasPrefix(headSHA, prefix) {
		return false
	}
	matches := map[string]struct{}{headSHA: {}}
	for _, review := range formalReviews {
		commitID := strings.ToLower(strings.TrimSpace(review.CommitID))
		if validFullGitSHA(commitID) && strings.HasPrefix(commitID, prefix) {
			matches[commitID] = struct{}{}
		}
	}
	return len(matches) == 1
}

func validFullGitSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func codexReviewAuthor(author *actor) bool {
	if author == nil {
		return false
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(author.Login)), "codex")
}

func pullRequestReviewAfter(left pullRequestReview, right pullRequestReview) bool {
	if left.SubmittedAt == nil {
		return right.SubmittedAt == nil
	}
	if right.SubmittedAt == nil {
		return true
	}
	return left.SubmittedAt.After(*right.SubmittedAt)
}

func pullRequestCodexReviewState(pullRequest pullRequestNode) string {
	return pullRequestCodexReviewStateFromReviews(pullRequest.LatestReviews.Nodes)
}

func pullRequestCodexReviewSource(pullRequest pullRequestNode) string {
	for _, review := range pullRequest.LatestReviews.Nodes {
		if source := strings.TrimSpace(review.Source); source != "" {
			return source
		}
	}
	return ""
}

func pullRequestLatestCodexReviewState(pullRequest pullRequestNode) string {
	return pullRequestCodexReviewStateFromReviews(pullRequest.CodexReviews.Latest)
}

func pullRequestCodexReviewStateFromReviews(reviews []pullRequestReview) string {
	reviewState, bodySeverity := pullRequestCodexReviewStateInputsFromReviews(reviews)
	if bodySeverity != "" {
		return bodySeverity
	}
	return reviewState
}

func pullRequestCodexReviewAPIState(pullRequest pullRequestNode) string {
	return pullRequestCodexReviewAPIStateFromReviews(pullRequest.LatestReviews.Nodes)
}

func pullRequestCodexReviewBodySeverity(pullRequest pullRequestNode) string {
	return pullRequestCodexReviewBodySeverityFromReviews(pullRequest.LatestReviews.Nodes)
}

func pullRequestCodexReviewAPIStateFromReviews(reviews []pullRequestReview) string {
	reviewState, _ := pullRequestCodexReviewStateInputsFromReviews(reviews)
	return reviewState
}

func pullRequestCodexReviewBodySeverityFromReviews(reviews []pullRequestReview) string {
	_, bodySeverity := pullRequestCodexReviewStateInputsFromReviews(reviews)
	return bodySeverity
}

func pullRequestCodexReviewStateInputsFromReviews(reviews []pullRequestReview) (string, string) {
	bodySeverity := ""
	reviewState := ""
	for _, review := range reviews {
		switch reviewBodySeverity(review.Body) {
		case "P1":
			bodySeverity = "P1"
		case "P2":
			if bodySeverity == "" {
				bodySeverity = "P2"
			}
		}
		if state := strings.ToUpper(strings.TrimSpace(review.State)); state != "" {
			reviewState = state
		}
	}
	return reviewState, bodySeverity
}

func pullRequestCodexReviewSubmittedAt(pullRequest pullRequestNode) *time.Time {
	return pullRequestCodexReviewSubmittedAtFromReviews(pullRequest.LatestReviews.Nodes)
}

func pullRequestLatestCodexReviewSubmittedAt(pullRequest pullRequestNode) *time.Time {
	return pullRequestCodexReviewSubmittedAtFromReviews(pullRequest.CodexReviews.Latest)
}

func pullRequestCodexReviewSubmittedAtFromReviews(reviews []pullRequestReview) *time.Time {
	var latest *time.Time
	for _, review := range reviews {
		if review.SubmittedAt == nil {
			continue
		}
		if latest == nil || review.SubmittedAt.After(*latest) {
			value := *review.SubmittedAt
			latest = &value
		}
	}
	return latest
}

func pullRequestLatestCodexReviewCommitSHA(pullRequest pullRequestNode) string {
	for _, review := range pullRequest.CodexReviews.Latest {
		if commitID := strings.TrimSpace(review.CommitID); commitID != "" {
			return commitID
		}
	}
	return ""
}

func pullRequestCodexReviewFindings(pullRequest pullRequestNode) []connector.PullRequestFinding {
	findings := []connector.PullRequestFinding{}
	for _, review := range pullRequest.LatestReviews.Nodes {
		if !containsReviewSeverity(review.Body, "P1") {
			continue
		}
		findings = append(findings, connector.PullRequestFinding{
			Body: strings.TrimSpace(review.Body),
			URL:  strings.TrimSpace(review.URL),
		})
	}
	return findings
}

func containsReviewSeverity(body string, severity string) bool {
	return reviewseverity.Contains(body, severity)
}

func reviewBodySeverity(body string) string {
	return reviewseverity.BodySeverity(body)
}

func pullRequestCheckNames(checks []connector.PullRequestCheck) []string {
	names := make([]string, 0, len(checks))
	for _, check := range checks {
		names = append(names, check.Name)
	}
	return uniqueNonBlank(names)
}
