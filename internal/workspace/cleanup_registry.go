package workspace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	cleanupOwnershipSchema      = 1
	cleanupOwnershipRegistryDir = ".detent/cleanup-ownership"
)

type cleanupOwnershipRecord struct {
	Schema          int    `json:"schema"`
	Path            string `json:"path"`
	Key             string `json:"key"`
	ProjectID       string `json:"project_id,omitempty"`
	IssueID         string `json:"issue_id,omitempty"`
	Identifier      string `json:"identifier,omitempty"`
	Branch          string `json:"branch,omitempty"`
	SourceCommonDir string `json:"source_common_dir"`
	Preserve        bool   `json:"preserve,omitempty"`
	CleanupStarted  bool   `json:"cleanup_started,omitempty"`
}

func (l *LocalGit) recordCleanupOwnership(ctx context.Context, info Info, issue Issue, isDir bool) error {
	if isDir && !l.isSourceWorktree(ctx, info.Path) && l.isGitWorkspace(ctx, info.Path) {
		return fmt.Errorf("refusing to record cleanup ownership for git workspace not managed by source: %s", info.Path)
	}
	path, err := validateWorkspacePath(l.root, info.Path)
	if err != nil {
		return err
	}
	sourceCommonDir, err := gitCommonDir(ctx, l.sourceRoot)
	if err != nil {
		return fmt.Errorf("resolve cleanup ownership source: %w", err)
	}
	record := cleanupOwnershipRecord{
		Schema:          cleanupOwnershipSchema,
		Path:            path,
		Key:             info.Key,
		ProjectID:       strings.TrimSpace(issue.ProjectID),
		IssueID:         strings.TrimSpace(issue.ID),
		Identifier:      strings.TrimSpace(issue.Identifier),
		Branch:          strings.TrimSpace(info.Branch),
		SourceCommonDir: sourceCommonDir,
	}
	if record.Key == "" {
		record.Key = filepath.Base(path)
	}
	if record.Key != filepath.Base(path) {
		return fmt.Errorf("cleanup ownership key %q does not match workspace path %q", record.Key, path)
	}
	if record.Identifier != "" && issueKey(issue) != record.Key {
		return fmt.Errorf("cleanup ownership issue does not match workspace key %q", record.Key)
	}
	previous, err := l.readOwnershipRecord(cleanupOwnershipRecordRelativePath(path))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("read previous cleanup ownership: %w", err)
	}
	if l.validOwnershipRecord(ctx, cleanupOwnershipRecordRelativePath(path), previous) {
		record.Preserve = previous.Preserve
	}
	if err := l.writeOwnershipRecord(record); err != nil {
		return fmt.Errorf("record cleanup ownership: %w", err)
	}
	return nil
}

func (l *LocalGit) writeOwnershipRecord(record cleanupOwnershipRecord) (returnErr error) {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	root, err := os.OpenRoot(l.root)
	if err != nil {
		return err
	}
	defer func() {
		returnErr = errors.Join(returnErr, root.Close())
	}()
	if err := root.MkdirAll(cleanupOwnershipRegistryDir, 0o700); err != nil {
		return err
	}
	recordPath := cleanupOwnershipRecordRelativePath(record.Path)
	temporaryPath, err := cleanupOwnershipTemporaryPath(recordPath)
	if err != nil {
		return err
	}
	file, err := root.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			returnErr = errors.Join(returnErr, file.Close())
		}
		if cleanupErr := root.Remove(temporaryPath); cleanupErr != nil && !errors.Is(cleanupErr, fs.ErrNotExist) {
			returnErr = errors.Join(returnErr, cleanupErr)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	closed = true
	if err := root.Rename(temporaryPath, recordPath); err != nil {
		return err
	}
	return nil
}

func cleanupOwnershipTemporaryPath(recordPath string) (string, error) {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return recordPath + ".tmp-" + hex.EncodeToString(suffix[:]), nil
}

func cleanupOwnershipRecordRelativePath(path string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(path)))
	return filepath.Join(cleanupOwnershipRegistryDir, hex.EncodeToString(sum[:])[:24]+".json")
}

func (l *LocalGit) removeOwnershipRecord(path string) (returnErr error) {
	root, err := os.OpenRoot(l.root)
	if err != nil {
		return err
	}
	defer func() {
		returnErr = errors.Join(returnErr, root.Close())
	}()
	err = root.Remove(cleanupOwnershipRecordRelativePath(path))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove cleanup ownership record: %w", err)
	}
	return nil
}

func (l *LocalGit) cleanupOwnershipRecorded(ctx context.Context, path string) (bool, error) {
	record, err := l.readOwnershipRecord(cleanupOwnershipRecordRelativePath(path))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return l.validOwnershipRecord(ctx, cleanupOwnershipRecordRelativePath(path), record), nil
}

func (l *LocalGit) readOwnershipRecord(relativePath string) (_ cleanupOwnershipRecord, returnErr error) {
	root, err := os.OpenRoot(l.root)
	if err != nil {
		return cleanupOwnershipRecord{}, err
	}
	defer func() {
		returnErr = errors.Join(returnErr, root.Close())
	}()
	info, err := root.Lstat(relativePath)
	if err != nil {
		return cleanupOwnershipRecord{}, err
	}
	if !info.Mode().IsRegular() {
		return cleanupOwnershipRecord{}, fmt.Errorf("cleanup ownership record is not a regular file: %s", relativePath)
	}
	data, err := root.ReadFile(relativePath)
	if err != nil {
		return cleanupOwnershipRecord{}, err
	}
	var decoded cleanupOwnershipRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		return cleanupOwnershipRecord{}, err
	}
	return decoded, nil
}

func (l *LocalGit) validOwnershipRecord(ctx context.Context, relativePath string, record cleanupOwnershipRecord) bool {
	if record.Schema != cleanupOwnershipSchema || record.Path == "" || record.Key == "" || record.SourceCommonDir == "" {
		return false
	}
	path, err := validateWorkspacePath(l.root, record.Path)
	if err != nil || path != filepath.Clean(record.Path) || filepath.Base(path) != record.Key {
		return false
	}
	if cleanupOwnershipRecordRelativePath(path) != relativePath {
		return false
	}
	if record.Identifier != "" && issueKey(Issue{ProjectID: record.ProjectID, Identifier: record.Identifier}) != record.Key {
		return false
	}
	sourceCommonDir, err := gitCommonDir(ctx, l.sourceRoot)
	return err == nil && sourceCommonDir == filepath.Clean(record.SourceCommonDir)
}

func (l *LocalGit) ReconcileResiduals(ctx context.Context, activeIssues []Issue) (ReconcileResult, error) {
	entries, err := os.ReadDir(filepath.Join(l.root, cleanupOwnershipRegistryDir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return ReconcileResult{}, fmt.Errorf("read cleanup ownership registry: %w", err)
	}
	activeKeys := make(map[string]struct{}, len(activeIssues))
	for _, issue := range activeIssues {
		activeKeys[issueKey(issue)] = struct{}{}
	}
	result := ReconcileResult{}
	seen := map[string]bool{}
	var records []cleanupOwnershipRecord
	for _, entry := range entries {
		relativePath := filepath.Join(cleanupOwnershipRegistryDir, entry.Name())
		record, readErr := l.readOwnershipRecord(relativePath)
		if readErr != nil || !l.validOwnershipRecord(ctx, relativePath, record) {
			result.UnownedSkipped++
			continue
		}
		seen[record.Path] = true
		records = append(records, record)
	}
	unrecorded, err := l.unrecordedWorkspaces(ctx, seen)
	if err != nil {
		return result, err
	}
	records = append(records, unrecorded...)
	var reconcileErrors []error
	for _, record := range records {
		if _, active := activeKeys[record.Key]; active {
			result.ActiveSkipped++
			continue
		}
		if record.Preserve {
			result.PreservedSkipped++
			continue
		}
		removed, reconcileErr := l.reconcileWorkspace(ctx, record, seen[record.Path], &result)
		if reconcileErr != nil {
			if errors.Is(reconcileErr, ErrWorkspacePreserved) {
				result.PreservedSkipped++
			}
			result.Failures = append(result.Failures, CleanupFailure{Path: record.Path, Error: reconcileErr.Error()})
			reconcileErrors = append(reconcileErrors, reconcileErr)
			continue
		}
		if removed {
			result.CompletedPaths = append(result.CompletedPaths, record.Path)
		}
	}
	return result, errors.Join(reconcileErrors...)
}

func (l *LocalGit) reconcileWorkspace(ctx context.Context, record cleanupOwnershipRecord, recorded bool, result *ReconcileResult) (bool, error) {
	if !recorded && record.Branch != "detent/"+strings.ToLower(record.Key) {
		result.UnownedSkipped++
		return false, nil
	}
	exists, _, err := pathExists(record.Path)
	if err != nil {
		return false, err
	}
	if exists {
		registered, err := l.sourceWorktreeRegistered(ctx, record.Path)
		if err != nil {
			return false, err
		}
		if recorded && registered {
			result.RegisteredSkipped++
			return false, nil
		}
		pids, err := scanOwnedWorkspaceProcessIDs(ctx, record.Path, l.scanWorkspacePaths)
		if err != nil {
			return false, err
		}
		if len(pids) > 0 {
			result.ActiveSkipped++
			return false, nil
		}
	}
	info := Info{Path: record.Path, Key: record.Key, Branch: record.Branch}
	issue := Issue{ProjectID: record.ProjectID, ID: record.IssueID, Identifier: record.Identifier}
	cleaned, err := l.cleanupWorkspace(ctx, info, issue)
	result.Removed += cleaned.Worktrees
	return err == nil, err
}

func (l *LocalGit) unrecordedWorkspaces(ctx context.Context, recorded map[string]bool) ([]cleanupOwnershipRecord, error) {
	output, err := l.runGit(ctx, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return nil, fmt.Errorf("discover unrecorded workspaces: %w", err)
	}
	var records []cleanupOwnershipRecord
	for index, entry := range strings.Split(output, "\x00\x00") {
		if index == 0 {
			continue
		}
		var path, branch string
		for _, field := range strings.Split(entry, "\x00") {
			if value, ok := strings.CutPrefix(field, "worktree "); ok {
				path = value
			}
			if value, ok := strings.CutPrefix(field, "branch refs/heads/"); ok {
				branch = value
			}
		}
		if path == "" || recorded[path] || filepath.Dir(path) != l.root {
			continue
		}
		path, err := validateWorkspacePath(l.root, path)
		if err != nil {
			return nil, err
		}
		exists, _, err := pathExists(path)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		records = append(records, cleanupOwnershipRecord{Path: path, Key: filepath.Base(path), Branch: branch})
	}
	return records, nil
}
