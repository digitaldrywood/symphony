package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/digitaldrywood/detent/internal/procgroup"
	commandshell "github.com/digitaldrywood/detent/internal/shell"
)

const defaultGitRemote = "origin"

var ErrMergeResolutionInvalid = errors.New("merge resolution is invalid")

func (l *LocalGit) PrepareMerge(
	ctx context.Context,
	info Info,
	issue Issue,
	opts MergePrepareOptions,
) (MergePrepareResult, error) {
	normalized, err := l.normalizeInfo(info, issue)
	if err != nil {
		return MergePrepareResult{}, err
	}
	if opts.VerifyResolution {
		return l.prepareResolvedMerge(ctx, normalized, issue, opts)
	}
	release, err := l.acquireSourceOperation(ctx)
	if err != nil {
		return MergePrepareResult{}, fmt.Errorf("wait for source repository operation: %w", err)
	}
	defer release()
	remote := strings.TrimSpace(opts.Remote)
	if remote == "" {
		remote = defaultGitRemote
	}
	targetBranch := strings.TrimSpace(opts.TargetBranch)
	if targetBranch == "" {
		targetBranch, err = remoteDefaultBranch(ctx, normalized.Path, remote)
		if err != nil {
			return MergePrepareResult{}, fmt.Errorf("resolve remote default branch: %w", err)
		}
	}
	targetRef := remote + "/" + targetBranch
	fetchRefspec := "+refs/heads/" + targetBranch + ":refs/remotes/" + targetRef

	if _, err := runGitAt(ctx, normalized.Path, "fetch", remote, fetchRefspec); err != nil {
		return MergePrepareResult{}, errors.Join(
			fmt.Errorf("git fetch %s %s: %w", remote, fetchRefspec, err),
			abortRebaseIfInProgress(ctx, normalized.Path),
		)
	}
	if _, err := runGitAt(ctx, normalized.Path, "rebase", targetRef); err != nil {
		abortErr := abortRebaseIfInProgress(ctx, normalized.Path)
		if abortErr != nil {
			return MergePrepareResult{}, errors.Join(
				fmt.Errorf("git rebase %s: %w", targetRef, err),
				abortErr,
			)
		}
		return MergePrepareResult{
			Status:  MergePrepareStatusConflict,
			Message: commandErrorOutput(err),
		}, nil
	}

	diffStat, err := l.DiffStat(ctx, normalized, issue)
	if err != nil {
		return MergePrepareResult{}, errors.Join(
			fmt.Errorf("workspace diff stat after rebase: %w", err),
			abortRebaseIfInProgress(ctx, normalized.Path),
		)
	}
	if diffStat != (DiffStat{}) {
		return MergePrepareResult{Status: MergePrepareStatusDirty, DiffStat: diffStat}, nil
	}

	branch := strings.TrimSpace(normalized.Branch)
	if branch == "" {
		return MergePrepareResult{}, errors.New("workspace branch is required for merge fast-path push")
	}
	remoteHead, remoteBranchExists, err := remoteBranchHead(ctx, normalized.Path, remote, branch)
	if err != nil {
		return MergePrepareResult{}, errors.Join(
			fmt.Errorf("inspect remote branch %s/%s: %w", remote, branch, err),
			abortRebaseIfInProgress(ctx, normalized.Path),
		)
	}
	localHead, err := runGitAt(ctx, normalized.Path, "rev-parse", "HEAD")
	if err != nil {
		return MergePrepareResult{}, errors.Join(
			fmt.Errorf("inspect local branch head: %w", err),
			abortRebaseIfInProgress(ctx, normalized.Path),
		)
	}
	headChanged := !remoteBranchExists || !strings.EqualFold(strings.TrimSpace(localHead), strings.TrimSpace(remoteHead))
	pushArgs := []string{"push"}
	if remoteBranchExists {
		pushArgs = append(pushArgs, "--force-with-lease=refs/heads/"+branch+":"+remoteHead)
	}
	pushArgs = append(pushArgs, remote, "HEAD:"+branch)
	if _, err := runGitAt(ctx, normalized.Path, pushArgs...); err != nil {
		return MergePrepareResult{}, errors.Join(
			fmt.Errorf("git %s: %w", strings.Join(pushArgs, " "), err),
			abortRebaseIfInProgress(ctx, normalized.Path),
		)
	}
	return MergePrepareResult{Status: MergePrepareStatusClean, DiffStat: diffStat, HeadChanged: headChanged}, nil
}

func remoteDefaultBranch(ctx context.Context, workspacePath string, remote string) (string, error) {
	output, err := runGitAt(ctx, workspacePath, "ls-remote", "--symref", remote, "HEAD")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "ref:" || fields[2] != "HEAD" {
			continue
		}
		branch := strings.TrimPrefix(fields[1], "refs/heads/")
		if branch != fields[1] && strings.TrimSpace(branch) != "" {
			return branch, nil
		}
	}
	return "", fmt.Errorf("remote %s HEAD is not a branch", remote)
}

func remoteDefaultBranchHead(ctx context.Context, workspacePath string, remote string) (string, string, error) {
	branch, err := remoteDefaultBranch(ctx, workspacePath, remote)
	if err != nil {
		return "", "", err
	}
	head, exists, err := remoteBranchHead(ctx, workspacePath, remote, branch)
	if err != nil {
		return "", "", err
	}
	if !exists {
		return "", "", fmt.Errorf("remote %s default branch %s is missing", remote, branch)
	}
	return branch, head, nil
}

func remoteBranchHead(ctx context.Context, workspacePath string, remote string, branch string) (string, bool, error) {
	ref := "refs/heads/" + branch
	output, err := runGitAt(ctx, workspacePath, "ls-remote", remote, ref)
	if err != nil {
		return "", false, err
	}
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[1] != ref {
			continue
		}
		return fields[0], true, nil
	}
	return "", false, nil
}

func abortRebaseIfInProgress(ctx context.Context, workspacePath string) error {
	inProgress, err := rebaseInProgress(ctx, workspacePath)
	if err != nil {
		return err
	}
	if !inProgress {
		return nil
	}
	if _, err := runGitAt(ctx, workspacePath, "rebase", "--abort"); err != nil {
		return fmt.Errorf("git rebase --abort: %w", err)
	}
	return nil
}

func rebaseInProgress(ctx context.Context, workspacePath string) (bool, error) {
	for _, gitPath := range []string{"rebase-merge", "rebase-apply"} {
		path, err := gitPathFor(ctx, workspacePath, gitPath)
		if err != nil {
			return false, err
		}
		if _, err := os.Stat(path); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

func gitPathFor(ctx context.Context, workspacePath string, gitPath string) (string, error) {
	output, err := runGitAt(ctx, workspacePath, "rev-parse", "--git-path", gitPath)
	if err != nil {
		return "", err
	}
	path := strings.TrimSpace(output)
	if path == "" {
		return "", errors.New("git path is empty")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(workspacePath, path)
	}
	return filepath.Clean(path), nil
}

func commandErrorOutput(err error) string {
	var commandErr *CommandError
	if errors.As(err, &commandErr) {
		return strings.TrimSpace(commandErr.Output)
	}
	return strings.TrimSpace(err.Error())
}

func (l *LocalGit) prepareResolvedMerge(ctx context.Context, info Info, issue Issue, opts MergePrepareOptions) (MergePrepareResult, error) {
	remote := strings.TrimSpace(opts.Remote)
	if remote == "" {
		remote = defaultGitRemote
	}
	target := strings.TrimSpace(opts.TargetBranch)
	if target == "" {
		var err error
		target, err = remoteDefaultBranch(ctx, info.Path, remote)
		if err != nil {
			return MergePrepareResult{}, err
		}
	}
	head, base, remoteHead, err := l.resolvedMergeHeads(ctx, info, issue, remote, target)
	if err != nil {
		return MergePrepareResult{}, err
	}
	if remoteHead != strings.TrimSpace(opts.ExpectedRemoteHead) && remoteHead != head {
		return MergePrepareResult{}, fmt.Errorf("%w: remote branch changed before merge-fallback validation", ErrMergeResolutionInvalid)
	}
	if err := l.validateMergeResolution(ctx, info, issue, opts.ValidationCommand); err != nil {
		return MergePrepareResult{}, err
	}
	currentHead, currentBase, currentRemote, err := l.resolvedMergeHeads(ctx, info, issue, remote, target)
	if err != nil {
		return MergePrepareResult{}, err
	}
	if currentHead != head || currentBase != base || currentRemote != remoteHead {
		return MergePrepareResult{}, fmt.Errorf("%w: local head, target base, or remote branch changed during merge-fallback validation", ErrMergeResolutionInvalid)
	}
	if _, err := runGitAt(ctx, info.Path, "push", "--force-with-lease=refs/heads/"+info.Branch+":"+remoteHead, remote, head+":refs/heads/"+info.Branch); err != nil {
		return MergePrepareResult{}, fmt.Errorf("push validated merge resolution: %w", err)
	}
	return MergePrepareResult{Status: MergePrepareStatusClean, HeadSHA: head, HeadChanged: remoteHead != head}, nil
}

func (l *LocalGit) resolvedMergeHeads(ctx context.Context, info Info, issue Issue, remote, target string) (string, string, string, error) {
	release, err := l.acquireSourceOperation(ctx)
	if err != nil {
		return "", "", "", err
	}
	defer release()
	branch, err := runGitAt(ctx, info.Path, "symbolic-ref", "--short", "HEAD")
	if err != nil {
		return "", "", "", err
	}
	if strings.TrimSpace(info.Branch) == "" || strings.TrimSpace(branch) != info.Branch {
		return "", "", "", fmt.Errorf("%w: merge resolution is not on the owned workspace branch", ErrMergeResolutionInvalid)
	}
	inProgress, err := rebaseInProgress(ctx, info.Path)
	if err != nil {
		return "", "", "", err
	}
	if inProgress {
		return "", "", "", fmt.Errorf("%w: merge resolution still has a rebase in progress", ErrMergeResolutionInvalid)
	}
	diff, err := l.DiffStat(ctx, info, issue)
	if err != nil {
		return "", "", "", err
	}
	if diff != (DiffStat{}) {
		return "", "", "", fmt.Errorf("%w: merge resolution workspace is not source-clean", ErrMergeResolutionInvalid)
	}
	ref := "refs/remotes/" + remote + "/" + target
	if _, err := runGitAt(ctx, info.Path, "fetch", remote, "+refs/heads/"+target+":"+ref); err != nil {
		return "", "", "", err
	}
	base, err := runGitAt(ctx, info.Path, "rev-parse", ref)
	if err != nil {
		return "", "", "", err
	}
	head, err := runGitAt(ctx, info.Path, "rev-parse", "HEAD")
	if err != nil {
		return "", "", "", err
	}
	if _, err := runGitAt(ctx, info.Path, "merge-base", "--is-ancestor", strings.TrimSpace(base), strings.TrimSpace(head)); err != nil {
		var commandErr *CommandError
		if errors.As(err, &commandErr) && commandErr.ExitCode == 1 {
			err = errors.Join(ErrMergeResolutionInvalid, err)
		}
		return "", "", "", fmt.Errorf("merge resolution does not contain the current target branch: %w", err)
	}
	remoteHead, exists, err := remoteBranchHead(ctx, info.Path, remote, info.Branch)
	if err != nil {
		return "", "", "", err
	}
	if !exists {
		return "", "", "", fmt.Errorf("%w: merge resolution remote branch is missing", ErrMergeResolutionInvalid)
	}
	return strings.TrimSpace(head), strings.TrimSpace(base), remoteHead, nil
}

func (l *LocalGit) validateMergeResolution(ctx context.Context, info Info, issue Issue, command string) (err error) {
	if strings.TrimSpace(command) == "" {
		return nil
	}
	scratch, err := PrepareWorkerScratch(ctx, info.Path)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, CleanupWorkerScratch(info.Path))
	}()
	cmd := commandshell.Command(ctx, command, l.hooks.Shell)
	cmd.Dir = info.Path
	cmd.Env = hookEnv(info, issue)
	cmd.WaitDelay = workspaceCommandWaitDelay
	procgroup.SetTempDir(cmd, scratch)
	procgroup.Configure(ctx, cmd)
	l.logger.Info("validating merge resolution", "workspace_path", info.Path, "command", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("merge resolution gate failed: %w: %s", errors.Join(ErrMergeResolutionInvalid, ctx.Err(), err), output)
	}
	return nil
}
