package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/digitaldrywood/detent/internal/procgroup"
)

var ErrCheckpointUnsafe = errors.New("checkpoint could not be safely published")

var checkpointSecretPattern = regexp.MustCompile(`-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|AKIA[A-Z0-9]{16}|sk-proj-[A-Za-z0-9_-]{30,})`)

type CheckpointSelection struct {
	HeadSHA  string   `json:"head_sha"`
	Paths    []string `json:"paths"`
	Reviewed bool     `json:"reviewed"`
}

type CheckpointPlan struct {
	Info      Info   `json:"workspace"`
	BaseSHA   string `json:"base_sha"`
	RemoteURL string `json:"-"`
	Journal   string `json:"-"`
}

type CheckpointRecord struct {
	Schema        int      `json:"schema"`
	Reason        string   `json:"reason"`
	Status        string   `json:"status"`
	WorkspacePath string   `json:"workspace_path"`
	Branch        string   `json:"branch"`
	HeadSHA       string   `json:"head_sha,omitempty"`
	Paths         []string `json:"paths,omitempty"`
	Published     bool     `json:"published"`
	PullRequest   string   `json:"pull_request,omitempty"`
	Detail        string   `json:"detail"`
}

type CheckpointBackend interface {
	PrepareCheckpoint(context.Context, Info, Issue) (CheckpointPlan, error)
	Checkpoint(context.Context, CheckpointPlan, CheckpointSelection, func(context.Context) error, procgroup.Environment) (string, error)
}

func (l *LocalGit) PrepareCheckpoint(ctx context.Context, info Info, issue Issue) (CheckpointPlan, error) {
	expected, err := l.infoForIssue(issue)
	if err != nil {
		return CheckpointPlan{}, err
	}
	automatic := issue
	automatic.BranchName = ""
	if !l.autoBranch || expected.Branch == "" || info.Path != expected.Path || info.Branch != l.branchName(automatic, expected.Key) {
		return CheckpointPlan{}, fmt.Errorf("%w: workspace is not an automatically owned issue branch", ErrCheckpointUnsafe)
	}
	if !l.isSourceWorktree(ctx, info.Path) {
		return CheckpointPlan{}, fmt.Errorf("%w: workspace ownership is unavailable", ErrCheckpointUnsafe)
	}
	if err := l.recordCleanupOwnership(ctx, info, issue, true); err != nil {
		return CheckpointPlan{}, err
	}
	record, err := l.readOwnershipRecord(cleanupOwnershipRecordRelativePath(info.Path))
	if err != nil {
		return CheckpointPlan{}, err
	}
	record.Preserve = true
	if err := l.writeOwnershipRecord(record); err != nil {
		return CheckpointPlan{}, err
	}
	gitDir, err := runGitAt(ctx, info.Path, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return CheckpointPlan{}, err
	}
	plan := CheckpointPlan{Info: info, Journal: filepath.Join(strings.TrimSpace(gitDir), "detent-checkpoint.json")}
	if err := WriteCheckpointRecord(plan, CheckpointRecord{Reason: "session_started", Status: "active", Detail: "Worker may have local work. No checkpoint handshake or publication has been verified; inspect the retained workspace after an interrupted session."}); err != nil {
		return plan, err
	}
	remote, err := runGitAt(ctx, info.Path, "remote", "get-url", "--push", defaultGitRemote)
	if err != nil {
		return plan, err
	}
	plan.RemoteURL = strings.TrimSpace(remote)
	base := strings.TrimSpace(issue.BaseRef)
	if base == "" {
		_, base, err = remoteDefaultBranchHead(ctx, info.Path, defaultGitRemote)
		if err != nil {
			return plan, err
		}
	}
	baseSHA, err := runGitAt(ctx, info.Path, "merge-base", "HEAD", base)
	if err != nil {
		return plan, err
	}
	plan.BaseSHA = strings.TrimSpace(baseSHA)
	return plan, nil
}

func WriteCheckpointRecord(plan CheckpointPlan, record CheckpointRecord) (returnErr error) {
	if plan.Journal == "" {
		return errors.New("checkpoint journal is unavailable")
	}
	record.Schema = 1
	record.WorkspacePath = plan.Info.Path
	record.Branch = plan.Info.Branch
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(plan.Journal), "detent-checkpoint-*.json")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, err)
		}
	}()
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	if err := errors.Join(writeErr, syncErr, file.Close()); err != nil {
		return err
	}
	return os.Rename(file.Name(), plan.Journal)
}

func (l *LocalGit) Checkpoint(ctx context.Context, plan CheckpointPlan, selection CheckpointSelection, validate func(context.Context) error, environment procgroup.Environment) (string, error) {
	if validate == nil || !selection.Reviewed || selection.HeadSHA == "" || plan.RemoteURL == "" || plan.BaseSHA == "" {
		return "", fmt.Errorf("%w: explicit reviewed selection and ownership validation are required", ErrCheckpointUnsafe)
	}
	release, err := l.acquireSourceOperation(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	git := checkpointGit(plan.Info.Path, environment)
	if err := checkpointGuard(ctx, git, plan, selection.HeadSHA, validate); err != nil {
		return "", err
	}
	paths := slices.Clone(selection.Paths)
	slices.Sort(paths)
	paths = slices.Compact(paths)
	for _, path := range paths {
		if !checkpointPathAllowed(path) {
			return "", fmt.Errorf("%w: excluded path %q", ErrCheckpointUnsafe, path)
		}
		stat, err := os.Lstat(filepath.Join(plan.Info.Path, filepath.FromSlash(path)))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if err == nil && !stat.Mode().IsRegular() {
			return "", fmt.Errorf("%w: selection must name regular files", ErrCheckpointUnsafe)
		}
	}
	if err := checkpointHistory(ctx, git, plan.BaseSHA, selection.HeadSHA, paths); err != nil {
		return "", err
	}
	if len(paths) > 0 {
		changed, err := git(ctx, append([]string{"diff", "--name-only", "--no-renames", "-z", "HEAD", "--"}, paths...)...)
		if err != nil {
			return "", err
		}
		untracked, err := git(ctx, append([]string{"ls-files", "--others", "--exclude-standard", "-z", "--"}, paths...)...)
		if err != nil {
			return "", err
		}
		dirty := strings.Split(strings.TrimSuffix(changed+untracked, "\x00"), "\x00")
		if changed+untracked != "" {
			if _, err := git(ctx, append([]string{"add", "--"}, dirty...)...); err != nil {
				return "", err
			}
		}
		diff, err := git(ctx, append([]string{"diff", "--cached", "--name-only", "-z", "HEAD", "--"}, paths...)...)
		if err != nil {
			return "", err
		}
		if diff != "" {
			for _, path := range strings.Split(strings.TrimSuffix(diff, "\x00"), "\x00") {
				if err := checkpointBlob(ctx, git, ":", path); err != nil {
					return "", err
				}
			}
			if err := checkpointGuard(ctx, git, plan, selection.HeadSHA, validate); err != nil {
				return "", err
			}
			if _, err := git(ctx, append([]string{"commit", "--only", "-m", "chore(recovery): checkpoint intended issue work", "--"}, dirty...)...); err != nil {
				return "", err
			}
		}
	}
	head, err := git(ctx, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	head = strings.TrimSpace(head)
	if err := checkpointHistory(ctx, git, plan.BaseSHA, head, paths); err != nil {
		return head, err
	}
	if head == plan.BaseSHA {
		return head, nil
	}
	remoteHead, err := checkpointRemoteHead(ctx, git, plan)
	if err != nil {
		return head, err
	}
	if remoteHead != "" && remoteHead != head {
		if _, err := git(ctx, "merge-base", "--is-ancestor", remoteHead, head); err != nil {
			return head, fmt.Errorf("%w: remote branch advanced; retain local work for reconciliation", ErrCheckpointUnsafe)
		}
	}
	if err := checkpointGuard(ctx, git, plan, head, validate); err != nil {
		return head, err
	}
	if remoteHead != head {
		if _, err := git(ctx, "-c", "push.followTags=false", "push", "--recurse-submodules=no", "--force-with-lease=refs/heads/"+plan.Info.Branch+":"+remoteHead, plan.RemoteURL, head+":refs/heads/"+plan.Info.Branch); err != nil {
			return head, err
		}
	}
	verified, err := checkpointRemoteHead(ctx, git, plan)
	if err != nil || verified != head {
		return head, errors.Join(fmt.Errorf("%w: remote checkpoint could not be verified", ErrCheckpointUnsafe), err)
	}
	return head, nil
}

type checkpointGitRunner func(context.Context, ...string) (string, error)

func checkpointGit(path string, environment procgroup.Environment) checkpointGitRunner {
	env := []string{"GIT_LITERAL_PATHSPECS=1", "GIT_TERMINAL_PROMPT=0"}
	for key, value := range environment.Variables {
		env = append(env, key+"="+value)
	}
	return func(ctx context.Context, args ...string) (string, error) {
		return runGitAtWithEnv(ctx, path, env, args...)
	}
}

func checkpointGuard(ctx context.Context, git checkpointGitRunner, plan CheckpointPlan, head string, validate func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validate(ctx); err != nil {
		return err
	}
	branch, err := git(ctx, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(branch) != plan.Info.Branch {
		return errors.Join(fmt.Errorf("%w: owned branch changed", ErrCheckpointUnsafe), err)
	}
	current, err := git(ctx, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(current) != head {
		return errors.Join(fmt.Errorf("%w: reviewed head changed", ErrCheckpointUnsafe), err)
	}
	return nil
}

func checkpointRemoteHead(ctx context.Context, git checkpointGitRunner, plan CheckpointPlan) (string, error) {
	output, err := git(ctx, "ls-remote", "--heads", plan.RemoteURL, "refs/heads/"+plan.Info.Branch)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(output)
	if len(fields) == 0 {
		return "", nil
	}
	if len(fields) != 2 || fields[1] != "refs/heads/"+plan.Info.Branch {
		return "", fmt.Errorf("%w: unexpected remote ref", ErrCheckpointUnsafe)
	}
	return fields[0], nil
}

func checkpointHistory(ctx context.Context, git checkpointGitRunner, base, head string, paths []string) error {
	if _, err := git(ctx, "merge-base", "--is-ancestor", base, head); err != nil {
		return fmt.Errorf("%w: checkpoint base changed", ErrCheckpointUnsafe)
	}
	commits, err := git(ctx, "rev-list", base+".."+head)
	if err != nil {
		return err
	}
	for _, commit := range strings.Fields(commits) {
		changed, err := git(ctx, "diff-tree", "--root", "--no-commit-id", "--name-only", "--no-renames", "-r", "-m", "-z", commit)
		if err != nil {
			return err
		}
		for _, path := range strings.Split(strings.TrimSuffix(changed, "\x00"), "\x00") {
			if path == "" {
				continue
			}
			if !checkpointPathAllowed(path) || !slices.Contains(paths, path) {
				return fmt.Errorf("%w: commit contains an unapproved path %q", ErrCheckpointUnsafe, path)
			}
			if err := checkpointBlob(ctx, git, commit+":", path); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkpointBlob(ctx context.Context, git checkpointGitRunner, prefix, path string) error {
	args := []string{"ls-tree", "-z", strings.TrimSuffix(prefix, ":"), "--", path}
	if prefix == ":" {
		args = []string{"ls-files", "--stage", "-z", "--", path}
	}
	entry, err := git(ctx, args...)
	if err != nil {
		return err
	}
	if entry == "" {
		return nil
	}
	header, name, ok := strings.Cut(strings.TrimSuffix(entry, "\x00"), "\t")
	fields := strings.Fields(header)
	if !ok || name != path || len(fields) != 3 || (fields[0] != "100644" && fields[0] != "100755") {
		return fmt.Errorf("%w: selected history must contain regular files", ErrCheckpointUnsafe)
	}
	blob, err := git(ctx, "show", prefix+path)
	if err != nil {
		return err
	}
	if checkpointSecretPattern.MatchString(blob) {
		return fmt.Errorf("%w: possible credential in selected content", ErrCheckpointUnsafe)
	}
	return nil
}

func checkpointPathAllowed(path string) bool {
	if path == "" || !filepath.IsLocal(path) || filepath.ToSlash(filepath.Clean(path)) != path || strings.ContainsAny(path, "\\\x00\r\n") {
		return false
	}
	for _, part := range strings.Split(strings.ToLower(path), "/") {
		if part == ".git" || part == ".env" || strings.HasPrefix(part, ".env.") || part == ".ssh" || part == "node_modules" || part == "tmp" || part == "secrets" || part == "credentials.json" || strings.HasSuffix(part, ".pem") || strings.HasSuffix(part, ".key") {
			return false
		}
	}
	return true
}
