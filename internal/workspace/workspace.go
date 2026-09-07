package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	commandshell "github.com/digitaldrywood/detent/internal/shell"
)

const (
	KindLocalGit   = "local_git"
	KindFilesystem = "filesystem"
)

const defaultHookTimeout = time.Minute
const failedWorkspacePreservationTimeout = time.Minute
const workspaceCommandWaitDelay = time.Second
const hookOutputTailBytes = 16 * 1024
const workerScratchRelativePath = ".detent/tmp"
const quarantineTimestampFormat = "20060102T150405.000000000Z"
const quarantineAccumulationWarningThreshold = 5

var (
	ErrHookFailed         = errors.New("workspace hook failed")
	ErrMissingWorkspace   = errors.New("workspace missing")
	ErrUnsafePath         = errors.New("unsafe workspace path")
	ErrUnsupportedBackend = errors.New("unsupported workspace backend")
	ErrWorktreeInvariant  = errors.New("workspace worktree invariant failed")
)

var unsafeKeyPattern = regexp.MustCompile(`[^A-Za-z0-9._-]`)

var sourceOperationLocks = struct {
	sync.Mutex
	bySource map[string]*sourceOperationLock
}{bySource: make(map[string]*sourceOperationLock)}

type sourceOperationLock struct {
	permit chan struct{}
}

type workspaceProcessScanner func(context.Context, string) ([]int, error)

func scanOwnedWorkspaceProcessIDs(ctx context.Context, path string, scan workspaceProcessScanner) ([]int, error) {
	pids, err := scan(ctx, path)
	if err != nil {
		return nil, err
	}
	owned := make([]int, 0, len(pids))
	seen := make(map[int]struct{}, len(pids))
	for _, pid := range pids {
		if pid <= 0 || pid == os.Getpid() {
			continue
		}
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		owned = append(owned, pid)
	}
	return owned, nil
}

type Backend interface {
	Create(context.Context, Issue) (Info, error)
	Cleanup(context.Context, string) error
	BeforeRun(context.Context, Info, Issue) error
	AfterRun(context.Context, Info, Issue)
	DiffStat(context.Context, Info, Issue) (DiffStat, error)
}

type RecoveryStateProvider interface {
	RecoveryState(context.Context, Info, Issue) (RecoveryState, error)
}

type DeliverableStateProvider interface {
	DeliverableState(context.Context, Info, Issue) (DeliverableState, error)
}

type ArtifactEvidenceProvider interface {
	ArtifactEvidence(context.Context, Info, Issue) (ArtifactEvidence, error)
}

type BranchHoldProvider interface {
	BranchHold(context.Context, Issue) (BranchHold, bool, error)
}

type BranchHold struct {
	Branch string
	Path   string
}

type IssueRecoveryStateProvider interface {
	IssueRecoveryState(context.Context, Issue) (RecoveryState, error)
}

type RecoveryState struct {
	DiffStat                       DiffStat
	BaseFingerprint                string
	HeadSHA                        string
	WorkspaceFingerprint           string
	UnpushedCommits                int
	UnpushedCommitRefs             []string
	TrackedPaths                   []string
	UntrackedPaths                 []string
	CommitsNotInPullRequest        []string
	PullRequestComparisonAvailable bool
}

type ArtifactEvidence struct {
	Available   bool
	Files       int
	Fingerprint string
}

type DeliverableState struct {
	CommitsAhead       int
	Remote             string
	RemoteRef          string
	LocalHeadSHA       string
	RemoteHeadSHA      string
	RemoteBranchExists bool
}

type MergePreparer interface {
	PrepareMerge(context.Context, Info, Issue, MergePrepareOptions) (MergePrepareResult, error)
}

type MergePrepareOptions struct {
	VerifyResolution   bool
	ValidationCommand  string
	ExpectedRemoteHead string
	Remote             string
	TargetBranch       string
}

type MergePrepareResult struct {
	HeadSHA     string
	Status      MergePrepareStatus
	DiffStat    DiffStat
	Message     string
	HeadChanged bool
}

type MergePrepareStatus string

const (
	MergePrepareStatusClean    MergePrepareStatus = "clean"
	MergePrepareStatusConflict MergePrepareStatus = "conflict"
	MergePrepareStatusDirty    MergePrepareStatus = "dirty"
)

type CleanupResult struct {
	Path      string
	Worktrees int
	Branches  int
	Processes int
}

type IssueCleaner interface {
	CleanupIssue(context.Context, Issue) (CleanupResult, error)
}

type CleanupFailure struct {
	Path  string
	Error string
}

type ReconcileResult struct {
	Removed           int
	ActiveSkipped     int
	PreservedSkipped  int
	RegisteredSkipped int
	UnownedSkipped    int
	CompletedPaths    []string
	Failures          []CleanupFailure
}

type ResidualReconciler interface {
	ReconcileResiduals(context.Context, []Issue) (ReconcileResult, error)
}

type Issue struct {
	ProjectID          string
	ID                 string
	Identifier         string
	BranchName         string
	BaseRef            string
	PullRequestHeadSHA string
}

type Info struct {
	Path    string
	Key     string
	Branch  string
	Created bool
}

type Hooks struct {
	Shell        string
	AfterCreate  string
	BeforeRun    string
	AfterRun     string
	BeforeRemove string
	Timeout      time.Duration
}

type LocalGitOptions struct {
	Root       string
	SourceRoot string
	OutputRoot string
	AutoBranch bool
	Hooks      Hooks
	Logger     *slog.Logger
}

type LocalGit struct {
	root               string
	sourceRoot         string
	autoBranch         bool
	hooks              Hooks
	logger             *slog.Logger
	createMu           sync.Mutex
	removeOwnedPath    func(string, string) error
	scanWorkspacePaths workspaceProcessScanner
}

type PathError struct {
	Path   string
	Root   string
	Reason string
}

func (e *PathError) Error() string {
	return fmt.Sprintf("%s: %s is not safe under %s: %s", ErrUnsafePath, e.Path, e.Root, e.Reason)
}

func (e *PathError) Unwrap() error {
	return ErrUnsafePath
}

type HookError struct {
	Hook     string
	Command  string
	Dir      string
	ExitCode int
	LogPath  string
	Output   string
	Err      error
}

func (e *HookError) Error() string {
	parts := []string{}
	if e.Command != "" {
		parts = append(parts, fmt.Sprintf("command %q", e.Command))
	}
	if e.Dir != "" {
		parts = append(parts, fmt.Sprintf("working directory %q", e.Dir))
	}
	if e.ExitCode >= 0 {
		parts = append(parts, fmt.Sprintf("exit status %d", e.ExitCode))
	}
	if e.LogPath != "" {
		parts = append(parts, fmt.Sprintf("hook log %q", e.LogPath))
	}

	detail := ""
	if len(parts) > 0 {
		detail = " (" + strings.Join(parts, "; ") + ")"
	}

	output := hookOutputTail(e.Output, hookOutputTailBytes)
	if output != "" {
		detail += fmt.Sprintf("\noutput (last %d KiB):\n%s", hookOutputTailBytes/1024, output)
	}

	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v%s", ErrHookFailed, e.Hook, e.Err, detail)
	}
	return fmt.Sprintf("%s: %s exited with status %d%s", ErrHookFailed, e.Hook, e.ExitCode, detail)
}

func (e *HookError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrHookFailed
}

func (e *HookError) Is(target error) bool {
	return target == ErrHookFailed
}

type CommandError struct {
	Command  string
	Args     []string
	ExitCode int
	Output   string
	Err      error
}

type workspaceRemovalError struct {
	path        string
	remediation string
	err         error
}

type worktreeCreationError struct {
	err error
}

type BranchHeldError struct {
	Branch string
	Path   string
}

func (e *BranchHeldError) Error() string {
	if e == nil {
		return "branch held by another worktree"
	}
	return fmt.Sprintf("branch %q is held by worktree at %q", e.Branch, e.Path)
}

func (e *worktreeCreationError) Error() string {
	return e.err.Error()
}

func (e *worktreeCreationError) Unwrap() error {
	return e.err
}

func (e *workspaceRemovalError) Error() string {
	return fmt.Sprintf("remove workspace path %q: %v; remediation: %s", e.path, e.err, e.remediation)
}

func (e *workspaceRemovalError) Unwrap() error {
	return e.err
}

func (e *workspaceRemovalError) WorkspacePath() string {
	return e.path
}

func (e *workspaceRemovalError) Remediation() string {
	return e.remediation
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("%s %s failed: %v", e.Command, strings.Join(e.Args, " "), e.Err)
}

func (e *CommandError) Unwrap() error {
	return e.Err
}

func IsMissingWorkspaceError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrMissingWorkspace) {
		return true
	}

	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		return false
	}
	output := strings.ToLower(commandErr.Output)
	return strings.Contains(output, "cannot change to") && strings.Contains(output, "no such file or directory")
}

func NewBackend(kind string, opts LocalGitOptions) (Backend, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case KindLocalGit:
		return NewLocalGit(opts)
	case KindFilesystem:
		return NewFilesystem(FilesystemOptions{
			Root:       opts.Root,
			SourceRoot: opts.SourceRoot,
			OutputRoot: opts.OutputRoot,
			Hooks:      opts.Hooks,
			Logger:     opts.Logger,
		})
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedBackend, kind)
	}
}

func NewLocalGit(opts LocalGitOptions) (*LocalGit, error) {
	root, err := prepareRoot(opts.Root)
	if err != nil {
		return nil, err
	}

	sourceRoot, err := canonicalExistingPath(opts.SourceRoot)
	if err != nil {
		return nil, fmt.Errorf("source root: %w", err)
	}

	hooks := opts.Hooks
	if hooks.Timeout == 0 {
		hooks.Timeout = defaultHookTimeout
	}
	if hooks.Timeout < 0 {
		return nil, errors.New("hooks timeout must be greater than or equal to 0")
	}
	hooks.Shell = commandshell.Normalize(hooks.Shell)

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &LocalGit{
		root:               root,
		sourceRoot:         sourceRoot,
		autoBranch:         opts.AutoBranch,
		hooks:              hooks,
		logger:             logger,
		removeOwnedPath:    removeWorkspacePath,
		scanWorkspacePaths: workspaceProcessIDs,
	}, nil
}

func newSourceOperationLock() *sourceOperationLock {
	lock := &sourceOperationLock{permit: make(chan struct{}, 1)}
	lock.permit <- struct{}{}
	return lock
}

func sourceOperationLockFor(sourceRoot string) *sourceOperationLock {
	key := filepath.Clean(sourceRoot)
	sourceOperationLocks.Lock()
	defer sourceOperationLocks.Unlock()
	lock, ok := sourceOperationLocks.bySource[key]
	if !ok {
		lock = newSourceOperationLock()
		sourceOperationLocks.bySource[key] = lock
	}
	return lock
}

func (l *sourceOperationLock) acquire(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.permit:
		return func() {
			l.permit <- struct{}{}
		}, nil
	}
}

func (l *LocalGit) acquireSourceOperation(ctx context.Context) (func(), error) {
	key := l.sourceRoot
	if key != "" {
		commonDir, err := gitCommonDir(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("source git common dir: %w", err)
		}
		key = commonDir
	}
	return sourceOperationLockFor(key).acquire(ctx)
}

func SafeKey(identifier string) string {
	key := unsafeKeyPattern.ReplaceAllString(strings.TrimSpace(identifier), "_")
	if key == "" || key == "." || key == ".." || key == ".detent" {
		return "issue"
	}
	return key
}

func issueKey(issue Issue) string {
	identifierKey := SafeKey(issue.Identifier)
	projectKey := SafeKey(issue.ProjectID)
	if strings.TrimSpace(issue.ProjectID) == "" {
		return identifierKey
	}
	return projectKey + "-" + identifierKey + "-" + issueKeyDigest(issue)
}

func issueKeyDigest(issue Issue) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(issue.ProjectID) + "\x00" + strings.TrimSpace(issue.Identifier)))
	return hex.EncodeToString(sum[:])[:12]
}

func (l *LocalGit) Create(ctx context.Context, issue Issue) (Info, error) {
	info, err := l.infoForIssue(issue)
	if err != nil {
		return Info{}, err
	}

	created, err := l.createWorktree(ctx, info.Path, info.Branch)
	if err != nil {
		var creationErr *worktreeCreationError
		if errors.As(err, &creationErr) {
			err = l.preserveFailedWorkspace(ctx, info.Path, err)
		}
		return Info{}, err
	}
	info.Created = created
	if err := ensureGitInfoExcludes(ctx, info.Path, detentHandoffDiffExcludes); err != nil {
		if created {
			err = l.preserveFailedWorkspace(ctx, info.Path, err)
		}
		return Info{}, err
	}

	if created {
		if err := l.validateCreatedWorktree(ctx, info.Path); err != nil {
			return Info{}, l.preserveFailedWorkspace(ctx, info.Path, err)
		}
		if err := l.runHook(ctx, "after_create", l.hooks.AfterCreate, info, issue); err != nil {
			return Info{}, l.preserveFailedWorkspace(ctx, info.Path, err)
		}
	}

	return info, nil
}

func (l *LocalGit) createWorktree(ctx context.Context, path string, branch string) (bool, error) {
	l.createMu.Lock()
	defer l.createMu.Unlock()
	return l.ensureWorktree(ctx, path, branch)
}

func (l *LocalGit) Cleanup(ctx context.Context, identifier string) error {
	_, err := l.CleanupIssue(ctx, Issue{Identifier: identifier})
	return err
}

func (l *LocalGit) CleanupIssue(ctx context.Context, issue Issue) (CleanupResult, error) {
	info, err := l.infoForIssue(issue)
	if err != nil {
		return CleanupResult{}, err
	}

	return l.cleanupWorkspace(ctx, info, issue)
}

func (l *LocalGit) cleanupWorkspace(ctx context.Context, info Info, issue Issue) (CleanupResult, error) {
	result := CleanupResult{Path: info.Path}
	if err := l.checkWorkspaceCleanup(ctx, info); err != nil {
		return result, err
	}
	exists, isDir, err := pathExists(info.Path)
	if err != nil {
		return result, err
	}
	if !exists {
		_, pruneErr := l.runGit(ctx, "worktree", "prune")
		if pruneErr != nil {
			return result, pruneErr
		}
		branchRemoved, branchErr := l.deleteBranch(ctx, info.Branch)
		if branchErr != nil {
			return result, branchErr
		}
		if branchRemoved {
			result.Branches = 1
		}
		if err := l.removeOwnershipRecord(info.Path); err != nil {
			return result, err
		}
		return result, nil
	}
	result.Processes = reapWorkspaceProcesses(ctx, info.Path, l.logger)
	if isDir && l.isSourceWorktree(ctx, info.Path) {
		if err := l.runHook(ctx, "before_remove", l.hooks.BeforeRemove, info, issue); err != nil {
			l.logger.Warn("workspace before_remove hook failed", slog.String("path", info.Path), slog.Any("error", err))
		}
	}

	if err := l.beginWorkspaceCleanup(ctx, info, issue, isDir); err != nil {
		return result, err
	}
	if err := l.removePath(ctx, info.Path); err != nil {
		return result, err
	}
	result.Worktrees = 1
	if _, err := l.runGit(ctx, "worktree", "prune"); err != nil {
		return result, err
	}
	branchRemoved, err := l.deleteBranch(ctx, info.Branch)
	if err != nil {
		return result, err
	}
	if branchRemoved {
		result.Branches = 1
	}
	if err := l.removeOwnershipRecord(info.Path); err != nil {
		return result, err
	}
	return result, nil
}

func (l *LocalGit) BeforeRun(ctx context.Context, info Info, issue Issue) error {
	normalized, err := l.normalizeInfo(info, issue)
	if err != nil {
		return err
	}
	return l.runHook(ctx, "before_run", l.hooks.BeforeRun, normalized, issue)
}

func (l *LocalGit) AfterRun(ctx context.Context, info Info, issue Issue) {
	normalized, err := l.normalizeInfo(info, issue)
	if err != nil {
		l.logger.Warn("workspace after_run path validation failed", slog.String("path", info.Path), slog.Any("error", err))
		return
	}
	if err := l.runHook(ctx, "after_run", l.hooks.AfterRun, normalized, issue); err != nil {
		l.logger.Warn("workspace after_run hook failed", slog.String("path", normalized.Path), slog.Any("error", err))
	}
}

func (l *LocalGit) infoForIssue(issue Issue) (Info, error) {
	key := issueKey(issue)
	path, err := l.workspacePath(key)
	if err != nil {
		return Info{}, err
	}

	return Info{
		Path:   path,
		Key:    key,
		Branch: l.branchName(issue, key),
	}, nil
}

func (l *LocalGit) IssueRecoveryState(ctx context.Context, issue Issue) (RecoveryState, error) {
	info, err := l.infoForIssue(issue)
	if err != nil {
		return RecoveryState{}, err
	}
	return l.RecoveryState(ctx, info, issue)
}

func (l *LocalGit) BranchHold(ctx context.Context, issue Issue) (BranchHold, bool, error) {
	info, err := l.infoForIssue(issue)
	if err != nil {
		return BranchHold{}, false, err
	}
	if strings.TrimSpace(info.Branch) == "" {
		return BranchHold{}, false, nil
	}
	release, err := l.acquireSourceOperation(ctx)
	if err != nil {
		return BranchHold{}, false, err
	}
	defer release()
	holder, held, err := l.branchWorktreePath(ctx, info.Branch, info.Path)
	if err != nil || !held {
		return BranchHold{}, held, err
	}
	return BranchHold{Branch: info.Branch, Path: holder}, true, nil
}

func (l *LocalGit) normalizeInfo(info Info, issue Issue) (Info, error) {
	key := info.Key
	if key == "" {
		key = issueKey(issue)
	}
	path := info.Path
	if path == "" {
		var err error
		path, err = l.workspacePath(key)
		if err != nil {
			return Info{}, err
		}
	} else {
		var err error
		path, err = validateWorkspacePath(l.root, path)
		if err != nil {
			return Info{}, err
		}
	}
	branch := info.Branch
	if branch == "" {
		branch = l.branchName(issue, key)
	}

	info.Path = path
	info.Key = key
	info.Branch = branch
	return info, nil
}

func (l *LocalGit) workspacePath(key string) (string, error) {
	return validateWorkspacePath(l.root, filepath.Join(l.root, key))
}

func (l *LocalGit) branchName(issue Issue, key string) string {
	if !l.autoBranch {
		return ""
	}
	if strings.TrimSpace(issue.BranchName) != "" {
		return strings.TrimSpace(issue.BranchName)
	}
	return "detent/" + strings.ToLower(key)
}

func (l *LocalGit) ensureWorktree(ctx context.Context, path string, branch string) (bool, error) {
	if _, err := l.runGit(ctx, "worktree", "prune"); err != nil {
		return false, fmt.Errorf("prune stale worktree registrations during preparation: %w", withCommandOutput(err))
	}
	exists, isDir, err := pathExists(path)
	if err != nil {
		return false, err
	}
	if exists {
		if isDir {
			if l.isSourceWorktree(ctx, path) {
				matches, current, err := l.workspaceOnExpectedBranch(ctx, path, branch)
				if err != nil {
					return false, err
				}
				if matches {
					return false, nil
				}
				if err := l.recoverStaleSourceWorktree(ctx, path, current, branch); err != nil {
					return false, err
				}
				exists = false
			} else if removed, err := l.removeDanglingSourceWorktree(ctx, path); err != nil {
				return false, err
			} else if removed {
				exists = false
			} else if l.isGitWorkspace(ctx, path) {
				return false, fmt.Errorf("workspace path is a git worktree not managed by source: %s", path)
			} else {
				empty, err := dirIsEmpty(path)
				if err != nil {
					return false, err
				}
				if !empty {
					return false, fmt.Errorf("workspace path exists but is not a git worktree: %s", path)
				}
			}
		}
		if exists {
			if err := removeWorkspacePath(l.root, path); err != nil {
				return false, fmt.Errorf("remove stale workspace path: %w", err)
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, fmt.Errorf("create workspace parent: %w", err)
	}

	if l.autoBranch {
		if err := l.addBranchedWorktree(ctx, path, branch); err != nil {
			return false, &worktreeCreationError{err: err}
		}
		return true, nil
	}

	err = l.addWorktreeWithPrune(ctx, func() error {
		_, addErr := l.runGit(ctx, "worktree", "add", "--detach", path, "HEAD")
		return addErr
	})
	if err != nil {
		return false, &worktreeCreationError{err: err}
	}
	return true, nil
}

func (l *LocalGit) validateCreatedWorktree(ctx context.Context, path string) error {
	sourceCommon, sourceErr := gitCommonDir(ctx, l.sourceRoot)
	workspaceCommon, workspaceErr := gitCommonDirWithinRoot(ctx, path)
	registered, registrationErr := l.sourceWorktreeRegistered(ctx, path)
	if sourceErr == nil && workspaceErr == nil && registrationErr == nil && workspaceCommon == sourceCommon && registered {
		return nil
	}

	sourceObservation := fmt.Sprintf("%q", sourceCommon)
	if sourceErr != nil {
		sourceObservation = fmt.Sprintf("unavailable (%v)", sourceErr)
	}
	workspaceObservation := fmt.Sprintf("%q", workspaceCommon)
	if workspaceErr != nil {
		workspaceObservation = fmt.Sprintf("unavailable (%v)", workspaceErr)
	}
	registrationObservation := strconv.FormatBool(registered)
	if registrationErr != nil {
		registrationObservation = fmt.Sprintf("unavailable (%v)", registrationErr)
	}
	return fmt.Errorf(
		"%w: path %q; workspace common dir: %s; source common dir: %s; registered with source: %s",
		ErrWorktreeInvariant,
		path,
		workspaceObservation,
		sourceObservation,
		registrationObservation,
	)
}

func (l *LocalGit) sourceWorktreeRegistered(ctx context.Context, path string) (bool, error) {
	output, err := l.runGit(ctx, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return false, withCommandOutput(err)
	}
	want, err := canonicalExistingPath(path)
	if err != nil {
		return false, fmt.Errorf("resolve workspace registration path: %w", err)
	}
	for _, field := range strings.Split(output, "\x00") {
		listed, ok := strings.CutPrefix(field, "worktree ")
		if !ok {
			continue
		}
		listed, err = canonicalExistingPath(listed)
		if err == nil && listed == want {
			return true, nil
		}
	}
	return false, nil
}

func (l *LocalGit) addBranchedWorktree(ctx context.Context, path string, branch string) error {
	return l.addWorktreeWithPrune(ctx, func() error {
		exists, err := l.branchExists(ctx, branch)
		if err != nil {
			return err
		}
		if exists {
			if holder, held, holdErr := l.branchWorktreePath(ctx, branch, path); holdErr != nil {
				return holdErr
			} else if held {
				return &BranchHeldError{Branch: branch, Path: holder}
			}
			_, err = l.runGit(ctx, "worktree", "add", path, branch)
			return err
		}

		baseRef, err := l.newBranchBaseRef(ctx)
		if err != nil {
			return err
		}
		_, err = l.runGit(ctx, "worktree", "add", "-b", branch, path, baseRef)
		return err
	})
}

func (l *LocalGit) branchWorktreePath(ctx context.Context, branch string, targetPath string) (string, bool, error) {
	output, err := l.runGit(ctx, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return "", false, withCommandOutput(err)
	}
	wantRef := "refs/heads/" + strings.TrimSpace(branch)
	targetPath = filepath.Clean(targetPath)
	var worktreePath string
	for _, field := range strings.Split(output, "\x00") {
		switch {
		case strings.HasPrefix(field, "worktree "):
			worktreePath = filepath.Clean(strings.TrimPrefix(field, "worktree "))
		case strings.HasPrefix(field, "branch "):
			if strings.TrimSpace(strings.TrimPrefix(field, "branch ")) == wantRef && worktreePath != targetPath {
				return worktreePath, true, nil
			}
		}
	}
	return "", false, nil
}

func (l *LocalGit) addWorktreeWithPrune(ctx context.Context, add func() error) error {
	return runWorktreeAddWithPrune(func() error {
		_, err := l.runGit(ctx, "worktree", "prune")
		return err
	}, add)
}

func runWorktreeAddWithPrune(prune func() error, add func() error) error {
	if err := prune(); err != nil {
		return fmt.Errorf("prune stale worktree registrations before add: %w", withCommandOutput(err))
	}

	err := add()
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.ExitCode != 128 {
		return withCommandOutput(err)
	}
	if pruneErr := prune(); pruneErr != nil {
		return errors.Join(
			withCommandOutput(err),
			fmt.Errorf("prune stale worktree registrations before retry: %w", withCommandOutput(pruneErr)),
		)
	}
	return withCommandOutput(add())
}

func withCommandOutput(err error) error {
	if err == nil {
		return nil
	}
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		return err
	}
	output := strings.TrimSpace(commandErr.Output)
	if output == "" {
		return err
	}
	return fmt.Errorf("%w\n%s", err, output)
}

func (l *LocalGit) newBranchBaseRef(ctx context.Context) (string, error) {
	remotes, err := l.runGit(ctx, "remote")
	if err != nil {
		return "", fmt.Errorf("list git remotes: %w", err)
	}
	if !stringListContains(remotes, defaultGitRemote) {
		return "HEAD", nil
	}

	branch, err := remoteDefaultBranch(ctx, l.sourceRoot, defaultGitRemote)
	if err != nil {
		return "", fmt.Errorf("resolve %s default branch: %w", defaultGitRemote, err)
	}
	remoteRef := defaultGitRemote + "/" + branch
	refspec := "+refs/heads/" + branch + ":refs/remotes/" + remoteRef
	if _, err := l.runGit(ctx, "fetch", defaultGitRemote, refspec); err != nil {
		return "", fmt.Errorf("fetch remote default branch %s: %w", remoteRef, err)
	}
	return remoteRef, nil
}

func stringListContains(list string, want string) bool {
	for _, value := range strings.Fields(list) {
		if value == want {
			return true
		}
	}
	return false
}

func (l *LocalGit) workspaceOnExpectedBranch(ctx context.Context, path string, branch string) (bool, string, error) {
	branch = strings.TrimSpace(branch)
	if !l.autoBranch || branch == "" {
		return true, "", nil
	}

	output, err := runGitAt(ctx, path, "branch", "--show-current")
	if err != nil {
		return false, "", fmt.Errorf("inspect workspace branch: %w", err)
	}
	current := strings.TrimSpace(output)
	return current == branch, current, nil
}

func (l *LocalGit) recoverStaleSourceWorktree(ctx context.Context, path string, currentBranch string, expectedBranch string) error {
	dirty, err := l.worktreeHasChanges(ctx, path)
	if err != nil {
		return fmt.Errorf("inspect stale workspace changes for branch %q, want %q: %w", currentBranch, expectedBranch, err)
	}
	unreferencedDetachedHead, err := l.unreferencedDetachedHead(ctx, path, currentBranch)
	if err != nil {
		return fmt.Errorf("inspect detached workspace HEAD for branch %q, want %q: %w", currentBranch, expectedBranch, err)
	}
	if dirty || unreferencedDetachedHead {
		quarantinePath, err := l.quarantineWorktree(ctx, path)
		if err != nil {
			reason := "has uncommitted changes"
			if unreferencedDetachedHead {
				reason = "has a detached HEAD commit that is not reachable from a ref"
			}
			return fmt.Errorf("workspace path is on branch %q, want %q and %s; preserve it by moving or cleaning %s: %w", currentBranch, expectedBranch, reason, path, err)
		}
		quarantineCount, countErr := l.quarantineWorkspaceCount(path)
		if countErr != nil {
			l.logger.Warn(
				"workspace quarantine count failed",
				slog.String("path", path),
				slog.String("quarantine_path", quarantinePath),
				slog.Any("error", countErr),
			)
		}
		l.logger.Warn(
			"quarantined stale workspace",
			slog.String("path", path),
			slog.String("quarantine_path", quarantinePath),
			slog.Int("quarantine_count", quarantineCount),
			slog.String("current_branch", currentBranch),
			slog.String("expected_branch", expectedBranch),
			slog.Bool("dirty", dirty),
			slog.Bool("unreferenced_detached_head", unreferencedDetachedHead),
		)
		if quarantineCount > quarantineAccumulationWarningThreshold {
			l.logger.Error(
				"workspace quarantine accumulation exceeded threshold",
				slog.String("path", path),
				slog.String("quarantine_path", quarantinePath),
				slog.Int("quarantine_count", quarantineCount),
				slog.Int("threshold", quarantineAccumulationWarningThreshold),
			)
		}
		return nil
	}

	if err := l.removeCleanWorktree(ctx, path); err != nil {
		return fmt.Errorf("recover stale clean workspace on branch %q, want %q: %w", currentBranch, expectedBranch, err)
	}
	l.logger.Info(
		"removed stale clean workspace",
		slog.String("path", path),
		slog.String("current_branch", currentBranch),
		slog.String("expected_branch", expectedBranch),
		slog.Bool("dirty", false),
	)
	return nil
}

func (l *LocalGit) worktreeHasChanges(ctx context.Context, path string) (bool, error) {
	output, err := runGitAt(ctx, path, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return false, fmt.Errorf("inspect workspace status: %w", err)
	}
	return strings.TrimSpace(output) != "", nil
}

func (l *LocalGit) unreferencedDetachedHead(ctx context.Context, path string, currentBranch string) (bool, error) {
	if strings.TrimSpace(currentBranch) != "" {
		return false, nil
	}
	output, err := runGitAt(ctx, path, "for-each-ref", "--contains", "HEAD", "--format=%(refname)")
	if err != nil {
		return false, fmt.Errorf("inspect refs containing HEAD: %w", err)
	}
	return strings.TrimSpace(output) == "", nil
}

func (l *LocalGit) removeCleanWorktree(ctx context.Context, path string) error {
	_, err := l.runGit(ctx, "worktree", "remove", path)
	if err == nil {
		return nil
	}
	removeErr := fmt.Errorf("remove stale clean worktree: %w", err)
	if cleanupErr := removeWorkspacePath(l.root, path); cleanupErr != nil {
		return errors.Join(removeErr, fmt.Errorf("remove partially recovered workspace: %w", cleanupErr))
	}
	if _, pruneErr := l.runGit(ctx, "worktree", "prune"); pruneErr != nil {
		return errors.Join(removeErr, fmt.Errorf("prune partially recovered workspace registration: %w", withCommandOutput(pruneErr)))
	}
	l.logger.Warn("removed partially recovered clean workspace", slog.String("path", path), slog.Any("git_remove_error", err))
	return nil
}

func (l *LocalGit) quarantineWorktree(ctx context.Context, path string) (string, error) {
	metadata, err := inspectGitMetadata(ctx, path)
	if err != nil {
		return "", fmt.Errorf("inspect stale worktree before quarantine: %w", err)
	}
	sourceCommon, err := gitCommonDir(ctx, l.sourceRoot)
	if err != nil {
		return "", fmt.Errorf("inspect source before quarantine: %w", err)
	}
	if metadata.commonDir != sourceCommon {
		return "", fmt.Errorf("refusing to quarantine worktree not managed by source: %s", path)
	}
	quarantinePath, err := l.nextQuarantinePath(path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(quarantinePath), 0o700); err != nil {
		return "", fmt.Errorf("create quarantine parent: %w", err)
	}
	if _, err := l.runGit(ctx, "worktree", "move", path, quarantinePath); err != nil {
		return "", fmt.Errorf("move stale worktree to quarantine: %w", err)
	}
	if err := relocateWorktreeAdmin(metadata, quarantinePath); err != nil {
		if _, rollbackErr := l.runGit(ctx, "worktree", "move", quarantinePath, path); rollbackErr != nil {
			return "", errors.Join(fmt.Errorf("release quarantined worktree admin name: %w", err), fmt.Errorf("restore stale worktree path: %w", rollbackErr))
		}
		return "", fmt.Errorf("release quarantined worktree admin name: %w", err)
	}
	return quarantinePath, nil
}

func (l *LocalGit) preserveFailedWorkspace(ctx context.Context, path string, cause error) error {
	preservationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failedWorkspacePreservationTimeout)
	defer cancel()

	quarantinePath, preserved, err := l.quarantineFailedWorkspace(preservationCtx, path)
	if err != nil {
		l.logger.Warn(
			"failed to quarantine workspace failure state",
			slog.String("path", path),
			slog.String("quarantine_path", quarantinePath),
			slog.Bool("preserved", preserved),
			slog.Any("cause", cause),
			slog.Any("error", err),
		)
		if preserved {
			return errors.Join(cause, fmt.Errorf("workspace quarantined at %q but failed to release its branch: %w", quarantinePath, err))
		}
		return errors.Join(cause, fmt.Errorf("quarantine failed workspace %q: %w", path, err))
	}
	if !preserved {
		return cause
	}

	quarantineCount, countErr := l.quarantineWorkspaceCount(path)
	if countErr != nil {
		l.logger.Warn(
			"workspace quarantine count failed",
			slog.String("path", path),
			slog.String("quarantine_path", quarantinePath),
			slog.Any("error", countErr),
		)
	}
	l.logger.Warn(
		"quarantined failed workspace",
		slog.String("path", path),
		slog.String("quarantine_path", quarantinePath),
		slog.Int("quarantine_count", quarantineCount),
		slog.Any("cause", cause),
	)
	if quarantineCount > quarantineAccumulationWarningThreshold {
		l.logger.Error(
			"workspace quarantine accumulation exceeded threshold",
			slog.String("path", path),
			slog.String("quarantine_path", quarantinePath),
			slog.Int("quarantine_count", quarantineCount),
			slog.Int("threshold", quarantineAccumulationWarningThreshold),
		)
	}
	return fmt.Errorf("%w; workspace quarantined at %q", cause, quarantinePath)
}

func (l *LocalGit) quarantineFailedWorkspace(ctx context.Context, path string) (string, bool, error) {
	exists, isDir, err := pathExists(path)
	if err != nil || !exists {
		return "", false, err
	}
	linkedWorktree := false
	if isDir {
		_, linkedWorktree, err = readLinkedWorktreeGitDir(path)
		if err != nil {
			return "", false, fmt.Errorf("inspect failed workspace linkage: %w", err)
		}
	}
	if isDir && linkedWorktree {
		quarantinePath, err := l.quarantineWorktree(ctx, path)
		if err != nil {
			return "", false, err
		}
		if err := detachWorktreeHead(ctx, quarantinePath); err != nil {
			return quarantinePath, true, err
		}
		return quarantinePath, true, nil
	}

	path, err = validateWorkspacePath(l.root, path)
	if err != nil {
		return "", false, err
	}
	quarantinePath, err := l.nextQuarantinePath(path)
	if err != nil {
		return "", false, err
	}
	if err := os.MkdirAll(filepath.Dir(quarantinePath), 0o700); err != nil {
		return "", false, fmt.Errorf("create quarantine parent: %w", err)
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		return "", false, fmt.Errorf("move failed workspace to quarantine: %w", err)
	}
	return quarantinePath, true, nil
}

func detachWorktreeHead(ctx context.Context, path string) error {
	output, err := runGitAt(ctx, path, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return fmt.Errorf("resolve quarantined worktree HEAD: %w", withCommandOutput(err))
	}
	head := strings.TrimSpace(output)
	if head == "" {
		return errors.New("resolve quarantined worktree HEAD: empty revision")
	}
	if _, err := runGitAt(ctx, path, "update-ref", "--no-deref", "HEAD", head); err != nil {
		return fmt.Errorf("detach quarantined worktree HEAD: %w", withCommandOutput(err))
	}
	return nil
}

func (l *LocalGit) removeDanglingSourceWorktree(ctx context.Context, path string) (bool, error) {
	gitDir, ok, err := readLinkedWorktreeGitDir(path)
	if err != nil || !ok {
		return false, err
	}
	sourceCommon, err := gitCommonDir(ctx, l.sourceRoot)
	if err != nil {
		return false, fmt.Errorf("inspect source for dangling workspace: %w", err)
	}
	adminRoot := filepath.Join(sourceCommon, "worktrees")
	if filepath.Dir(gitDir) != adminRoot {
		return false, nil
	}
	exists, _, err := pathExists(gitDir)
	if err != nil {
		return false, fmt.Errorf("inspect linked worktree admin directory: %w", err)
	}
	if exists {
		return false, nil
	}
	if err := removeWorkspacePath(l.root, path); err != nil {
		return false, fmt.Errorf("remove workspace with dangling gitdir: %w", err)
	}
	l.logger.Warn("removed workspace with dangling gitdir", slog.String("path", path), slog.String("git_dir", gitDir))
	return true, nil
}

func relocateWorktreeAdmin(metadata gitMetadata, workspacePath string) error {
	adminRoot := filepath.Join(metadata.commonDir, "worktrees")
	if filepath.Dir(metadata.gitDir) != adminRoot {
		return fmt.Errorf("worktree admin directory %s is not directly under %s", metadata.gitDir, adminRoot)
	}
	info, err := os.Lstat(metadata.gitDir)
	if err != nil {
		return fmt.Errorf("inspect worktree admin directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("worktree admin path is not a directory: %s", metadata.gitDir)
	}

	newGitDir, err := nextWorktreeAdminPath(adminRoot, filepath.Base(workspacePath))
	if err != nil {
		return err
	}
	gitFilePath := filepath.Join(workspacePath, ".git")
	linkedGitDir, ok, err := readLinkedWorktreeGitDir(workspacePath)
	if err != nil {
		return err
	}
	if !ok || linkedGitDir != metadata.gitDir {
		return fmt.Errorf("worktree gitdir pointer changed during quarantine: got %s, want %s", linkedGitDir, metadata.gitDir)
	}
	if err := os.Rename(metadata.gitDir, newGitDir); err != nil {
		return fmt.Errorf("rename worktree admin directory: %w", err)
	}
	if err := writeLinkedWorktreeGitDir(gitFilePath, newGitDir); err != nil {
		if rollbackErr := os.Rename(newGitDir, metadata.gitDir); rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("restore worktree admin directory: %w", rollbackErr))
		}
		return err
	}
	return nil
}

func nextWorktreeAdminPath(adminRoot string, workspaceBase string) (string, error) {
	base := SafeKey(workspaceBase)
	for i := range 100 {
		name := base
		if i > 0 {
			name += strconv.Itoa(i)
		}
		candidate := filepath.Join(adminRoot, name)
		exists, _, err := pathExists(candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
	return "", errors.New("exhausted worktree admin path attempts")
}

func readLinkedWorktreeGitDir(workspacePath string) (string, bool, error) {
	path := filepath.Join(workspacePath, ".git")
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("inspect linked worktree gitdir: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false, fmt.Errorf("read linked worktree gitdir: %w", err)
	}
	value, ok := strings.CutPrefix(strings.TrimSpace(string(data)), "gitdir: ")
	if !ok || strings.TrimSpace(value) == "" {
		return "", false, nil
	}
	gitDir := strings.TrimSpace(value)
	if !filepath.IsAbs(gitDir) {
		gitDir = filepath.Join(workspacePath, gitDir)
	}
	return filepath.Clean(gitDir), true, nil
}

func writeLinkedWorktreeGitDir(path string, gitDir string) (err error) {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect linked worktree gitdir file: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".git.detent-*")
	if err != nil {
		return fmt.Errorf("create linked worktree gitdir file: %w", err)
	}
	tempPath := temp.Name()
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, temp.Close())
		}
		if cleanupErr := os.Remove(tempPath); cleanupErr != nil && !errors.Is(cleanupErr, fs.ErrNotExist) {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if err := temp.Chmod(info.Mode().Perm()); err != nil {
		return fmt.Errorf("set linked worktree gitdir file mode: %w", err)
	}
	if _, err := fmt.Fprintf(temp, "gitdir: %s\n", gitDir); err != nil {
		return fmt.Errorf("write linked worktree gitdir file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close linked worktree gitdir file: %w", err)
	}
	closed = true
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace linked worktree gitdir file: %w", err)
	}
	return nil
}

func (l *LocalGit) nextQuarantinePath(path string) (string, error) {
	parent := filepath.Join(l.root, ".detent", "quarantine")
	base := SafeKey(filepath.Base(path))
	stamp := time.Now().UTC().Format(quarantineTimestampFormat)
	for i := range 100 {
		name := base + "-" + stamp
		if i > 0 {
			name += fmt.Sprintf("-%d", i)
		}
		candidate, err := validateWorkspacePath(l.root, filepath.Join(parent, name))
		if err != nil {
			return "", err
		}
		exists, _, err := pathExists(candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
	return "", errors.New("exhausted quarantine workspace path attempts")
}

func (l *LocalGit) quarantineWorkspaceCount(path string) (int, error) {
	parent := filepath.Join(l.root, ".detent", "quarantine")
	entries, err := os.ReadDir(parent)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read quarantine directory: %w", err)
	}
	base := SafeKey(filepath.Base(path))
	count := 0
	for _, entry := range entries {
		if entry.IsDir() && quarantineEntryMatchesWorkspace(base, entry.Name()) {
			count++
		}
	}
	return count, nil
}

func quarantineEntryMatchesWorkspace(base string, name string) bool {
	suffix, ok := strings.CutPrefix(name, base+"-")
	if !ok {
		return false
	}
	stampEnd := strings.IndexByte(suffix, 'Z')
	if stampEnd < 0 {
		return false
	}
	stampEnd++
	if _, err := time.Parse(quarantineTimestampFormat, suffix[:stampEnd]); err != nil {
		return false
	}
	collision := suffix[stampEnd:]
	if collision == "" {
		return true
	}
	value, err := strconv.Atoi(strings.TrimPrefix(collision, "-"))
	return err == nil && strings.HasPrefix(collision, "-") && value > 0
}

func (l *LocalGit) branchExists(ctx context.Context, branch string) (bool, error) {
	_, err := l.runGit(ctx, "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	if err == nil {
		return true, nil
	}

	var cmdErr *CommandError
	if errors.As(err, &cmdErr) && cmdErr.ExitCode == 1 {
		return false, nil
	}
	return false, err
}

func (l *LocalGit) deleteBranch(ctx context.Context, branch string) (bool, error) {
	branch = strings.TrimSpace(branch)
	if !l.autoBranch || branch == "" || !strings.HasPrefix(branch, "detent/") {
		return false, nil
	}
	exists, err := l.branchExists(ctx, branch)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if err := l.checkCleanupBranch(ctx, branch); err != nil {
		return false, err
	}
	_, err = l.runGit(ctx, "branch", "-D", branch)
	return err == nil, err
}

func (l *LocalGit) isGitWorkspace(ctx context.Context, path string) bool {
	output, err := runGitAtWithEnv(ctx, path, gitDiscoveryBoundary(path), "rev-parse", "--is-inside-work-tree")
	return err == nil && strings.TrimSpace(output) == "true"
}

func (l *LocalGit) isSourceWorktree(ctx context.Context, path string) bool {
	workspaceCommon, err := gitCommonDirWithinRoot(ctx, path)
	if err != nil {
		return false
	}
	sourceCommon, err := gitCommonDir(ctx, l.sourceRoot)
	if err != nil {
		return false
	}
	return workspaceCommon == sourceCommon
}

func (l *LocalGit) removePath(ctx context.Context, path string) error {
	exists, _, err := pathExists(path)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	sourceWorktree := l.isSourceWorktree(ctx, path)
	if _, err := l.runGit(ctx, "worktree", "remove", "--force", path); err != nil {
		if sourceWorktree {
			return l.retrySourceWorktreeRemoveAfterPermissionRemediation(ctx, path, err)
		}
		if l.isGitWorkspace(ctx, path) {
			return fmt.Errorf("refusing to remove git workspace not managed by source: %s", path)
		}
		if owned, ownershipErr := l.cleanupOwnershipRecorded(ctx, path); ownershipErr != nil {
			return ownershipErr
		} else if !owned {
			return fmt.Errorf("refusing to remove unregistered workspace without Detent ownership evidence: %s", path)
		}
		if err := l.removeOwnedPath(l.root, path); err != nil {
			return &workspaceRemovalError{path: path, remediation: workspaceRemovalRemediation, err: err}
		}
		return nil
	}
	if exists, _, err := pathExists(path); err != nil {
		return err
	} else if exists {
		if !sourceWorktree {
			return fmt.Errorf("refusing to remove residual workspace without pre-removal source ownership: %s", path)
		}
		if err := l.removeOwnedPath(l.root, path); err != nil {
			return &workspaceRemovalError{path: path, remediation: workspaceRemovalRemediation, err: err}
		}
	}
	return nil
}

const workspaceRemovalRemediation = "ensure the Detent user owns the workspace, chmod writable directories inside it, then remove it or rerun Detent cleanup"

func (l *LocalGit) retrySourceWorktreeRemoveAfterPermissionRemediation(ctx context.Context, path string, removeErr error) error {
	path, chmodErr := remediateWorkspacePathPermissions(l.root, path)
	if chmodErr != nil {
		return &workspaceRemovalError{
			path:        path,
			remediation: workspaceRemovalRemediation,
			err:         errors.Join(removeErr, fmt.Errorf("remediate workspace permissions: %w", chmodErr)),
		}
	}
	if !l.isSourceWorktree(ctx, path) {
		if retryErr := l.removeOwnedPath(l.root, path); retryErr != nil {
			return &workspaceRemovalError{
				path:        path,
				remediation: workspaceRemovalRemediation,
				err:         errors.Join(removeErr, retryErr),
			}
		}
		return nil
	}
	if _, retryErr := l.runGit(ctx, "worktree", "remove", "--force", path); retryErr != nil {
		return &workspaceRemovalError{
			path:        path,
			remediation: workspaceRemovalRemediation,
			err:         retryErr,
		}
	}
	if exists, _, retryErr := pathExists(path); retryErr != nil {
		return &workspaceRemovalError{path: path, remediation: workspaceRemovalRemediation, err: retryErr}
	} else if exists {
		if retryErr := l.removeOwnedPath(l.root, path); retryErr != nil {
			return &workspaceRemovalError{path: path, remediation: workspaceRemovalRemediation, err: retryErr}
		}
	}
	return nil
}

func removeWorkspacePath(root string, path string) error {
	path, err := validateWorkspacePath(root, path)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		if !errors.Is(err, fs.ErrPermission) {
			return err
		}
		if chmodErr := makeWorkspaceTreeRemovable(path); chmodErr != nil {
			return &workspaceRemovalError{
				path:        path,
				remediation: workspaceRemovalRemediation,
				err:         errors.Join(err, fmt.Errorf("remediate workspace permissions: %w", chmodErr)),
			}
		}
		if retryErr := os.RemoveAll(path); retryErr != nil {
			return &workspaceRemovalError{
				path:        path,
				remediation: workspaceRemovalRemediation,
				err:         retryErr,
			}
		}
	}
	if exists, _, err := pathExists(path); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("workspace path still exists after removal: %s", path)
	}
	return nil
}

func PrepareWorkerScratch(ctx context.Context, workspacePath string) (scratchPath string, err error) {
	workspacePath, err = canonicalExistingPath(workspacePath)
	if errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("%w: resolve worker workspace: %w", ErrMissingWorkspace, err)
	}
	if err != nil {
		return "", fmt.Errorf("resolve worker workspace: %w", err)
	}
	if err := ensureWorkerScratchExcluded(ctx, workspacePath); err != nil {
		return "", err
	}
	scratchPath = filepath.Join(workspacePath, filepath.FromSlash(workerScratchRelativePath))
	if err := removeWorkspacePath(workspacePath, scratchPath); err != nil {
		return "", fmt.Errorf("remove stale worker scratch: %w", err)
	}

	root, err := os.OpenRoot(workspacePath)
	if err != nil {
		return "", fmt.Errorf("open worker workspace: %w", err)
	}
	defer func() {
		err = errors.Join(err, root.Close())
	}()

	if err := root.MkdirAll(workerScratchRelativePath, 0o700); err != nil {
		return "", fmt.Errorf("create worker scratch: %w", err)
	}
	return scratchPath, nil
}

func ensureWorkerScratchExcluded(ctx context.Context, workspacePath string) error {
	_, err := os.Lstat(filepath.Join(workspacePath, ".git"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat worker workspace git metadata: %w", err)
	}
	if err := ensureGitInfoExcludes(ctx, workspacePath, detentHandoffDiffExcludes); err != nil {
		return fmt.Errorf("exclude worker scratch from git: %w", err)
	}
	return nil
}

func CleanupWorkerScratch(workspacePath string) error {
	workspacePath, err := canonicalExistingPath(workspacePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve worker workspace: %w", err)
	}
	scratchPath := filepath.Join(workspacePath, filepath.FromSlash(workerScratchRelativePath))
	if err := removeWorkspacePath(workspacePath, scratchPath); err != nil {
		return fmt.Errorf("remove worker scratch: %w", err)
	}
	return nil
}

func CleanupOwnedPath(root string, path string) error {
	if strings.TrimSpace(root) == "" || strings.TrimSpace(path) == "" {
		return nil
	}
	root, err := canonicalExistingPath(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve cleanup root: %w", err)
	}
	if err := removeWorkspacePath(root, path); err != nil {
		return fmt.Errorf("remove owned path: %w", err)
	}
	return nil
}

func remediateWorkspacePathPermissions(root string, path string) (string, error) {
	validated, err := validateWorkspacePath(root, path)
	if err != nil {
		return path, err
	}
	return validated, makeWorkspaceTreeRemovable(validated)
}

func makeWorkspaceTreeRemovable(path string) error {
	return filepath.WalkDir(path, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect %s: %w", path, err)
		}
		mode := info.Mode().Perm()
		switch {
		case entry.IsDir():
			mode |= 0o700
		case runtime.GOOS == "windows":
			mode |= 0o600
		default:
			return nil
		}
		if err := os.Chmod(path, mode); err != nil { // #nosec G122 -- the path is confined by validateWorkspacePath and symlinks are skipped before chmod.
			return fmt.Errorf("chmod %s: %w", path, err)
		}
		return nil
	})
}

func (l *LocalGit) runHook(ctx context.Context, name string, command string, info Info, issue Issue) error {
	if strings.TrimSpace(command) == "" {
		return nil
	}
	release, err := l.acquireSourceOperation(ctx)
	if err != nil {
		return err
	}
	defer release()

	timeout := l.hooks.Timeout
	if timeout == 0 {
		timeout = defaultHookTimeout
	}

	hookCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		hookCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	cmd := commandshell.Command(hookCtx, command, l.hooks.Shell)
	cmd.Dir = info.Path
	cmd.Env = hookEnv(info, issue)
	cmd.WaitDelay = workspaceCommandWaitDelay

	l.logger.Info(
		"running workspace hook",
		slog.String("hook", name),
		slog.String("path", info.Path),
		slog.String("command", command),
	)

	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	if errors.Is(err, exec.ErrWaitDelay) && hookCtx.Err() == nil {
		return nil
	}

	exitCode := -1
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exitCode = exitErr.ExitCode()
	}
	if hookCtx.Err() != nil {
		err = hookCtx.Err()
	}

	logPath, logErr := l.writeHookLog(name, command, info, exitCode, err, output)
	hookErr := &HookError{
		Hook:     name,
		Command:  command,
		Dir:      info.Path,
		ExitCode: exitCode,
		LogPath:  logPath,
		Output:   string(output),
		Err:      err,
	}
	if logErr != nil {
		l.logger.Warn(
			"workspace hook log write failed",
			slog.String("hook", name),
			slog.String("path", info.Path),
			slog.String("command", command),
			slog.Any("error", logErr),
		)
	}
	l.logger.Warn(
		"workspace hook failed",
		slog.String("hook", name),
		slog.String("path", info.Path),
		slog.String("command", command),
		slog.Int("exit_code", exitCode),
		slog.String("log_path", logPath),
		slog.String("output_tail", hookOutputTail(string(output), hookOutputTailBytes)),
		slog.Any("error", err),
	)
	return hookErr
}

func (l *LocalGit) writeHookLog(
	name string,
	command string,
	info Info,
	exitCode int,
	err error,
	output []byte,
) (string, error) {
	root := strings.TrimSpace(l.root)
	if root == "" {
		root = strings.TrimSpace(info.Path)
	}
	if root == "" {
		return "", errors.New("workspace hook log root is empty")
	}
	dir := filepath.Join(root, ".detent", "hook-logs", SafeKey(info.Key))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, hookLogFileName(name, time.Now().UTC(), os.Getpid()))

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "hook: %s\n", name)
	fmt.Fprintf(&buf, "command: %s\n", command)
	fmt.Fprintf(&buf, "working_directory: %s\n", info.Path)
	fmt.Fprintf(&buf, "exit_status: %d\n", exitCode)
	if err != nil {
		fmt.Fprintf(&buf, "error: %s\n", err)
	}
	fmt.Fprint(&buf, "\noutput:\n")
	fmt.Fprint(&buf, string(output))
	if len(output) > 0 && output[len(output)-1] != '\n' {
		fmt.Fprint(&buf, "\n")
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func hookLogFileName(name string, at time.Time, pid int) string {
	return at.Format("20060102T150405.000000000Z") + "-" + SafeKey(name) + fmt.Sprintf("-%d.log", pid)
}

func hookOutputTail(output string, limit int) string {
	if limit <= 0 || output == "" {
		return ""
	}
	if len(output) <= limit {
		return output
	}
	return "[truncated to last " + strconv.Itoa(limit/1024) + " KiB]\n" + output[len(output)-limit:]
}

func (l *LocalGit) runGit(ctx context.Context, args ...string) (string, error) {
	return runGitAt(ctx, l.sourceRoot, args...)
}

func runGitAt(ctx context.Context, dir string, args ...string) (string, error) {
	return runGitAtWithEnv(ctx, dir, nil, args...)
}

func runGitAtWithEnv(ctx context.Context, dir string, env []string, args ...string) (string, error) {
	gitArgs := append([]string{"git", "-C", dir}, args...)
	cmd := exec.CommandContext(ctx, "git")
	cmd.Args = gitArgs
	cmd.WaitDelay = workspaceCommandWaitDelay
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	output, err := cmd.CombinedOutput()
	if err == nil {
		return string(output), nil
	}

	exitCode := -1
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exitCode = exitErr.ExitCode()
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}

	return "", &CommandError{
		Command:  "git",
		Args:     gitArgs[1:],
		ExitCode: exitCode,
		Output:   string(output),
		Err:      err,
	}
}

func gitDiscoveryBoundary(dir string) []string {
	return []string{"GIT_CEILING_DIRECTORIES=" + filepath.Dir(dir)}
}

func gitCommonDir(ctx context.Context, dir string) (string, error) {
	return gitCommonDirWithEnv(ctx, dir, nil)
}

func gitCommonDirWithinRoot(ctx context.Context, dir string) (string, error) {
	return gitCommonDirWithEnv(ctx, dir, gitDiscoveryBoundary(dir))
}

func gitCommonDirWithEnv(ctx context.Context, dir string, env []string) (string, error) {
	output, err := runGitAtWithEnv(ctx, dir, env, "rev-parse", "--git-common-dir")
	if err != nil {
		return "", err
	}
	commonDir := strings.TrimSpace(output)
	if commonDir == "" {
		return "", errors.New("git common dir is empty")
	}
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(dir, commonDir)
	}
	return canonicalExistingPath(commonDir)
}

func GitMetadataWritableRoots(ctx context.Context, workspacePath string) ([]string, error) {
	workspaceRoot, err := canonicalExistingPath(workspacePath)
	if err != nil {
		return nil, fmt.Errorf("workspace path: %w", err)
	}
	if _, err := os.Lstat(filepath.Join(workspaceRoot, ".git")); err != nil {
		return nil, fmt.Errorf("workspace git metadata: %w", err)
	}
	metadata, err := inspectGitMetadata(ctx, workspaceRoot)
	if err != nil {
		return nil, err
	}

	roots := []string{}
	seen := map[string]struct{}{}
	addRoot := func(path string) {
		if path == "" || path == metadata.commonDir || pathWithin(workspaceRoot, path) || !pathWithin(metadata.commonDir, path) {
			return
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		roots = append(roots, path)
	}

	addRoot(metadata.gitDir)
	addRoot(metadata.objectsDir)
	if !strings.HasPrefix(metadata.headRef, "refs/heads/") {
		return roots, nil
	}

	refDir, err := canonicalExistingDir(filepath.Dir(filepath.Join(metadata.commonDir, filepath.FromSlash(metadata.headRef))))
	if err != nil {
		return nil, fmt.Errorf("git branch ref dir: %w", err)
	}
	addRoot(refDir)

	logDir, err := canonicalExistingDir(filepath.Dir(filepath.Join(metadata.commonDir, "logs", filepath.FromSlash(metadata.headRef))))
	if err != nil {
		return nil, fmt.Errorf("git branch log dir: %w", err)
	}
	addRoot(logDir)

	return roots, nil
}

type gitMetadata struct {
	commonDir  string
	gitDir     string
	objectsDir string
	headRef    string
}

func inspectGitMetadata(ctx context.Context, dir string) (gitMetadata, error) {
	output, err := runGitAtWithEnv(
		ctx,
		dir,
		gitDiscoveryBoundary(dir),
		"rev-parse",
		"--path-format=absolute",
		"--git-common-dir",
		"--git-dir",
		"--git-path", "objects",
		"--symbolic-full-name", "HEAD",
	)
	if err != nil {
		return gitMetadata{}, fmt.Errorf("inspect git metadata: %w", err)
	}
	fields := strings.Split(strings.TrimSpace(output), "\n")
	if len(fields) != 4 {
		return gitMetadata{}, fmt.Errorf("inspect git metadata: expected 4 fields, got %d", len(fields))
	}

	paths := make([]string, 3)
	for i, field := range fields[:3] {
		path, err := canonicalExistingPath(strings.TrimSpace(field))
		if err != nil {
			return gitMetadata{}, fmt.Errorf("inspect git metadata field %d: %w", i+1, err)
		}
		paths[i] = path
	}
	headRef := strings.TrimSpace(fields[3])
	if headRef == "" {
		return gitMetadata{}, errors.New("inspect git metadata: head ref is empty")
	}

	return gitMetadata{
		commonDir:  paths[0],
		gitDir:     paths[1],
		objectsDir: paths[2],
		headRef:    headRef,
	}, nil
}

func canonicalExistingDir(path string) (string, error) {
	for {
		canonical, err := canonicalExistingPath(path)
		if err == nil {
			return canonical, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "", err
		}
		path = parent
	}
}

func pathWithin(root string, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel))
}

func hookEnv(info Info, issue Issue) []string {
	env := append([]string{}, os.Environ()...)
	values := []struct {
		key   string
		value string
	}{
		{"DETENT_WORKSPACE", info.Path},
		{"DETENT_WORKSPACE_KEY", info.Key},
		{"DETENT_BRANCH", info.Branch},
		{"DETENT_ISSUE_ID", issue.ID},
		{"DETENT_ISSUE_IDENTIFIER", issue.Identifier},
		{"WORKSPACE", info.Path},
		{"WORKSPACE_KEY", info.Key},
		{"BRANCH", info.Branch},
		{"ISSUE_ID", issue.ID},
		{"ISSUE_IDENTIFIER", issue.Identifier},
	}
	for _, value := range values {
		env = append(env, value.key+"="+value.value)
	}
	return env
}

func prepareRoot(path string) (string, error) {
	expanded, err := expandPath(path)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(expanded) == "" {
		return "", errors.New("workspace root is required")
	}
	if err := os.MkdirAll(expanded, 0o700); err != nil {
		return "", fmt.Errorf("create workspace root: %w", err)
	}
	return canonicalExistingPath(expanded)
}

func canonicalExistingPath(path string) (string, error) {
	expanded, err := expandPath(path)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(expanded) == "" {
		return "", errors.New("path is required")
	}
	abs, err := filepath.Abs(expanded)
	if err != nil {
		return "", fmt.Errorf("absolute path: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("canonicalize %s: %w", abs, err)
	}
	return filepath.Clean(canonical), nil
}

func expandPath(path string) (string, error) {
	if path == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		return home, nil
	}
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		return filepath.Join(home, strings.TrimPrefix(path, "~/")), nil
	}
	return path, nil
}

func validateWorkspacePath(root string, path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("absolute workspace path: %w", err)
	}
	clean := filepath.Clean(abs)

	if info, err := os.Lstat(clean); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", &PathError{Path: clean, Root: root, Reason: "workspace path is a symlink"}
		}
		canonical, err := filepath.EvalSymlinks(clean)
		if err != nil {
			return "", fmt.Errorf("canonicalize workspace path: %w", err)
		}
		clean = filepath.Clean(canonical)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("inspect workspace path: %w", err)
	}

	if clean == root {
		return "", &PathError{Path: clean, Root: root, Reason: "workspace path equals root"}
	}

	rel, err := filepath.Rel(root, clean)
	if err != nil {
		return "", fmt.Errorf("relative workspace path: %w", err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", &PathError{Path: clean, Root: root, Reason: "workspace path escapes root"}
	}

	return clean, nil
}

func pathExists(path string) (bool, bool, error) {
	info, err := os.Lstat(path)
	if err == nil {
		return true, info.IsDir(), nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, false, nil
	}
	return false, false, fmt.Errorf("inspect path: %w", err)
}

func dirIsEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, fmt.Errorf("read workspace directory: %w", err)
	}
	return len(entries) == 0, nil
}
