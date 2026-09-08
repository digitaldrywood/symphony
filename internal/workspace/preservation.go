package workspace

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var ErrWorkspacePreserved = errors.New("workspace retained for recovery")

type Preservation struct {
	LocalChangesVerified bool              `json:"local_changes_verified"`
	Delivery             *DeliverableState `json:"delivery,omitempty"`
	DeliveryError        string            `json:"delivery_error,omitempty"`
	Path                 string            `json:"path"`
	Branch               string            `json:"branch,omitempty"`
	Preserved            bool              `json:"preserved"`
	Files                int               `json:"files,omitempty"`
	HeadSHA              string            `json:"head_sha,omitempty"`
	UnpushedCommits      int               `json:"unpushed_commits,omitempty"`
	TrackedPaths         []string          `json:"tracked_paths,omitempty"`
	UntrackedPaths       []string          `json:"untracked_paths,omitempty"`
}

type IssuePreserver interface {
	PreserveIssue(context.Context, Issue) (Preservation, error)
}

func (l *LocalGit) PreserveIssue(ctx context.Context, issue Issue) (Preservation, error) {
	info, err := l.infoForIssue(issue)
	if err != nil {
		return Preservation{}, err
	}
	result := Preservation{Path: info.Path, Branch: info.Branch}
	exists, isDir, err := pathExists(info.Path)
	if err != nil {
		return result, err
	}
	if !exists {
		return result, ErrMissingWorkspace
	}
	if err := l.recordCleanupOwnership(ctx, info, issue, isDir); err != nil {
		return result, err
	}
	record, err := l.readOwnershipRecord(cleanupOwnershipRecordRelativePath(info.Path))
	if err != nil {
		return result, err
	}
	record.Preserve = true
	if err := l.writeOwnershipRecord(record); err != nil {
		return result, fmt.Errorf("retain workspace ownership: %w", err)
	}
	result.Preserved = true
	recovery, err := l.RecoveryState(ctx, info, issue)
	if err != nil {
		return result, fmt.Errorf("inspect retained workspace: %w", err)
	}
	result.HeadSHA = recovery.HeadSHA
	result.UnpushedCommits, err = retainedGitCommitCount(ctx, info.Path)
	if err != nil {
		return result, err
	}
	result.TrackedPaths = recovery.TrackedPaths
	result.UntrackedPaths = recovery.UntrackedPaths
	result.LocalChangesVerified = true
	delivery, err := gitDeliveryState(ctx, info.Path, info.Branch, issue.BaseRef)
	if err != nil {
		result.DeliveryError = err.Error()
	} else {
		result.Delivery = &delivery
	}
	return result, nil
}

func (l *LocalGit) checkWorkspaceCleanup(ctx context.Context, info Info) error {
	recordPath := cleanupOwnershipRecordRelativePath(info.Path)
	record, err := l.readOwnershipRecord(recordPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w at %s: read workspace retention: %w", ErrWorkspacePreserved, info.Path, err)
	}
	if err == nil && !l.validOwnershipRecord(ctx, recordPath, record) {
		return fmt.Errorf("%w: invalid workspace retention record: %s", ErrWorkspacePreserved, info.Path)
	}
	if err := l.checkCleanupBranch(ctx, info.Branch); err != nil {
		return fmt.Errorf("%w at %s: %w", ErrWorkspacePreserved, info.Path, err)
	}
	exists, isDir, err := pathExists(info.Path)
	if err != nil {
		return fmt.Errorf("%w at %s: %w", ErrWorkspacePreserved, info.Path, err)
	}
	if !exists && !record.Preserve {
		return nil
	}
	if !isDir || !l.isSourceWorktree(ctx, info.Path) {
		if exists && record.CleanupStarted && !record.Preserve && !l.isGitWorkspace(ctx, info.Path) {
			return nil
		}
		return fmt.Errorf("%w at %s: worktree registration is unavailable or not managed by source", ErrWorkspacePreserved, info.Path)
	}
	changed, err := l.worktreeHasChanges(ctx, info.Path)
	if err != nil {
		return fmt.Errorf("%w at %s: %w", ErrWorkspacePreserved, info.Path, err)
	}
	unpushed, err := retainedGitCommitCount(ctx, info.Path)
	if err != nil {
		return fmt.Errorf("%w at %s: %w", ErrWorkspacePreserved, info.Path, err)
	}
	if unpushed > 0 || changed {
		return fmt.Errorf("%w at %s: unpushed commits or uncommitted files remain", ErrWorkspacePreserved, info.Path)
	}
	return nil
}

func (l *LocalGit) checkCleanupBranch(ctx context.Context, branch string) error {
	if !l.autoBranch || !strings.HasPrefix(branch, "detent/") {
		return nil
	}
	exists, err := l.branchExists(ctx, branch)
	if err != nil || !exists {
		return err
	}
	output, err := l.runGit(ctx, "rev-list", "--count", "refs/heads/"+branch, "--not", "--remotes")
	if err != nil {
		return fmt.Errorf("inspect cleanup branch: %w", err)
	}
	if strings.TrimSpace(output) != "0" {
		return fmt.Errorf("%w: branch %s contains commits absent from remote refs", ErrWorkspacePreserved, branch)
	}
	return nil
}

func (l *LocalGit) beginWorkspaceCleanup(ctx context.Context, info Info, issue Issue, isDir bool) error {
	if err := l.checkWorkspaceCleanup(ctx, info); err != nil {
		return err
	}
	if err := l.recordCleanupOwnership(ctx, info, issue, isDir); err != nil {
		return err
	}
	record, err := l.readOwnershipRecord(cleanupOwnershipRecordRelativePath(info.Path))
	if err != nil {
		return err
	}
	record.CleanupStarted = true
	return l.writeOwnershipRecord(record)
}

func retainedGitCommitCount(ctx context.Context, path string) (int, error) {
	output, err := runGitAt(ctx, path, "rev-list", "--count", "HEAD", "--not", "--remotes")
	if err != nil {
		return 0, fmt.Errorf("inspect retained commits: %w", err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return 0, fmt.Errorf("parse retained commit count: %w", err)
	}
	return count, nil
}

func (f *Filesystem) PreserveIssue(ctx context.Context, issue Issue) (Preservation, error) {
	info, err := f.infoForIssue(issue)
	if err != nil {
		return Preservation{}, err
	}
	result := Preservation{Path: info.Path}
	exists, isDir, err := pathExists(info.Path)
	if err != nil {
		return result, err
	}
	if !exists {
		return result, ErrMissingWorkspace
	}
	if !isDir {
		return result, fmt.Errorf("filesystem workspace is not a directory: %s", info.Path)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	root, err := os.OpenRoot(f.root)
	if err != nil {
		return result, err
	}
	defer f.closeRoot("preservation", root)
	if err := root.MkdirAll(filesystemRetentionPath(info), 0o700); err != nil {
		return result, fmt.Errorf("retain filesystem workspace: %w", err)
	}
	result.Preserved = true
	stat, err := f.DiffStat(ctx, info, issue)
	if err != nil {
		return result, fmt.Errorf("inspect retained filesystem workspace: %w", err)
	}
	result.Files = stat.Files
	result.LocalChangesVerified = true
	return result, nil
}

func (f *Filesystem) checkPreservedWorkspace(info Info) error {
	root, err := os.OpenRoot(f.root)
	if err != nil {
		return err
	}
	defer f.closeRoot("preservation", root)
	if _, err := root.Lstat(filesystemRetentionPath(info)); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read filesystem workspace retention: %w", err)
	}
	return fmt.Errorf("%w at %s: filesystem output requires explicit disposition", ErrWorkspacePreserved, info.Path)
}

func filesystemRetentionPath(info Info) string {
	return filepath.Join(info.Key, ".detent", "retained")
}
