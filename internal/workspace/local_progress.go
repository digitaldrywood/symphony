package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

const (
	localProgressTimeout    = 5 * time.Second
	localProgressPatchLimit = 8 << 20
)

type LocalProgressProvider interface {
	LocalProgress(context.Context, Info, Issue) (LocalProgress, error)
}

type LocalProgress struct {
	HeadSHA            string `json:"head_sha"`
	CommitFingerprint  string `json:"commit_fingerprint"`
	TrackedFingerprint string `json:"tracked_fingerprint"`
}

func (l *LocalGit) LocalProgress(ctx context.Context, info Info, issue Issue) (LocalProgress, error) {
	ctx, cancel := context.WithTimeout(ctx, localProgressTimeout)
	defer cancel()
	normalized, err := l.normalizeInfo(info, issue)
	if err != nil {
		return LocalProgress{}, err
	}
	head, err := runGitAt(ctx, normalized.Path, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return LocalProgress{}, err
	}
	head = strings.TrimSpace(head)
	base, err := l.localProgressBase(ctx, normalized.Path, issue)
	if err != nil {
		return LocalProgress{}, err
	}
	ancestor, err := runGitAt(ctx, normalized.Path, "merge-base", base, head)
	if err != nil {
		return LocalProgress{}, err
	}
	committed, err := gitProgressPatch(ctx, normalized.Path, strings.TrimSpace(ancestor), head)
	if err != nil {
		return LocalProgress{}, err
	}
	tracked, err := gitProgressPatch(ctx, normalized.Path, head)
	if err != nil {
		return LocalProgress{}, err
	}
	verifiedHead, err := runGitAt(ctx, normalized.Path, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return LocalProgress{}, err
	}
	if strings.TrimSpace(verifiedHead) != head {
		return LocalProgress{}, errors.New("workspace head changed during progress observation")
	}
	return LocalProgress{HeadSHA: head, CommitFingerprint: committed, TrackedFingerprint: tracked}, nil
}

func (l *LocalGit) localProgressBase(ctx context.Context, path string, issue Issue) (string, error) {
	if branch := strings.TrimSpace(issue.ProgressBaseRef); branch != "" {
		for _, ref := range []string{"refs/remotes/origin/" + branch, "refs/heads/" + branch} {
			if base, err := runGitAt(ctx, path, "rev-parse", "--verify", ref+"^{commit}"); err == nil {
				return strings.TrimSpace(base), nil
			}
		}
		return "", fmt.Errorf("local progress base branch %q is unavailable", branch)
	}
	if ref := strings.TrimSpace(issue.BaseRef); ref != "" {
		base, err := runGitAt(ctx, path, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}")
		return strings.TrimSpace(base), err
	}
	if base, err := l.runGit(ctx, "rev-parse", "--verify", "refs/remotes/origin/HEAD^{commit}"); err == nil {
		return strings.TrimSpace(base), nil
	}
	base, err := l.runGit(ctx, "rev-parse", "--verify", "HEAD")
	return strings.TrimSpace(base), err
}

func gitProgressPatch(ctx context.Context, path string, revisions ...string) (string, error) {
	return gitProgressPatchBounded(ctx, path, localProgressPatchLimit, revisions...)
}

func gitProgressPatchBounded(ctx context.Context, path string, limit int64, revisions ...string) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	args := []string{"-C", path, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--no-color", "--no-relative", "--diff-algorithm=myers", "--binary", "--full-index", "--unified=0", "--src-prefix=a/", "--dst-prefix=b/"}
	args = append(args, revisions...)
	args = append(args, "--", ".")
	for _, exclude := range detentHandoffDiffExcludes {
		args = append(args, ":(exclude)"+exclude)
	}
	diff := exec.CommandContext(ctx, "git")
	diff.Args = append(diff.Args, args...)
	diff.WaitDelay = workspaceCommandWaitDelay
	var diffError bytes.Buffer
	diff.Stderr = &diffError
	patch, err := diff.StdoutPipe()
	if err != nil {
		return "", err
	}
	if err := diff.Start(); err != nil {
		return "", err
	}
	identity := exec.CommandContext(ctx, "git", "patch-id", "--verbatim")
	identity.Dir = path
	identity.WaitDelay = workspaceCommandWaitDelay
	bounded := &io.LimitedReader{R: patch, N: limit + 1}
	identity.Stdin = bounded
	output, patchErr := identity.Output()
	if bounded.N == 0 {
		patchErr = errors.Join(patchErr, errors.New("local progress patch exceeds observation limit"))
	}
	if patchErr != nil {
		cancel()
	}
	closeErr := patch.Close()
	diffErr := diff.Wait()
	if err := errors.Join(patchErr, closeErr, diffErr); err != nil {
		return "", fmt.Errorf("read local progress patch: %w: %s", err, strings.TrimSpace(diffError.String()))
	}
	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return "", nil
	}
	return fields[0], nil
}
