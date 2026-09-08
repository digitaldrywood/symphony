package lessons

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/digitaldrywood/detent/internal/runtimeoutput"
)

const (
	DefaultPath       = ".detent/lessons.md"
	DefaultMaxEntries = 50
	MaxFileBytes      = 16 * 1024
	MaxEntryBytes     = 4 * 1024
)

type Entry struct {
	IssueNumber string
	IssueRef    string
	PullRequest string
	Title       string
	FailureKind string
	Symptom     string
	Hypothesis  string
	Hint        string
	CaptureKey  string
	Evidence    []string
}

type AppendOptions struct {
	Date       time.Time
	MaxEntries int
}

type FailureKindPattern struct {
	FailureKind string
	Count       int
	Examples    []string
}

type CaptureStats struct {
	Count          int
	LastCapturedAt *time.Time
}

var appendMu sync.Mutex

func Append(path string, entry Entry, opts AppendOptions) error {
	appendMu.Lock()
	defer appendMu.Unlock()

	return appendEntry(path, entry, opts)
}

func AppendUnique(path string, entry Entry, opts AppendOptions) (bool, error) {
	appendMu.Lock()
	defer appendMu.Unlock()

	captureKey := strings.TrimSpace(entry.CaptureKey)
	if captureKey == "" {
		return false, errors.New("lesson capture key is required")
	}
	entries, err := ReadAll(path)
	if err != nil {
		return false, err
	}
	for _, existing := range entries {
		if renderedEntryField(existing, "Capture key") == captureKey {
			return false, nil
		}
	}
	return true, appendEntryTo(path, entry, opts, entries)
}

func CaptureSummary(path string) (CaptureStats, error) {
	entries, err := ReadAll(path)
	if err != nil {
		return CaptureStats{}, err
	}
	stats := CaptureStats{Count: len(entries)}
	for _, entry := range entries {
		capturedAt, ok := renderedEntryCapturedAt(entry)
		if !ok || stats.LastCapturedAt != nil && !capturedAt.After(*stats.LastCapturedAt) {
			continue
		}
		value := capturedAt
		stats.LastCapturedAt = &value
	}
	return stats, nil
}

func appendEntry(path string, entry Entry, opts AppendOptions) error {
	entries, err := ReadAll(path)
	if err != nil {
		return err
	}
	return appendEntryTo(path, entry, opts, entries)
}

func appendEntryTo(path string, entry Entry, opts AppendOptions, entries []string) error {
	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxEntries
	}

	date := opts.Date
	if date.IsZero() {
		date = time.Now().UTC()
	}

	rendered := renderEntry(entry, date)
	entries = append([]string{rendered}, entries...)
	truncated := len(entries) > maxEntries
	if len(entries) > maxEntries {
		entries = entries[:maxEntries]
	}

	return writeEntries(path, entries, truncated)
}

func Recent(path string, count int) ([]string, error) {
	if count <= 0 {
		return []string{}, nil
	}

	entries, err := ReadAll(path)
	if err != nil {
		return nil, err
	}
	if len(entries) > count {
		entries = entries[:count]
	}

	return entries, nil
}

func FailureKindPatterns(path string, threshold int) ([]FailureKindPattern, error) {
	if threshold <= 0 {
		threshold = 1
	}

	entries, err := ReadAll(path)
	if err != nil {
		return nil, err
	}

	patternsByKind := map[string]*FailureKindPattern{}
	keys := []string{}
	for _, entry := range entries {
		failureKind := renderedEntryField(entry, "Failure kind")
		if failureKind == "" || strings.HasPrefix(failureKind, "<") {
			continue
		}
		key := strings.ToLower(failureKind)
		pattern, ok := patternsByKind[key]
		if !ok {
			pattern = &FailureKindPattern{FailureKind: failureKind}
			patternsByKind[key] = pattern
			keys = append(keys, key)
		}
		pattern.Count++
		if example := renderedEntryHeading(entry); example != "" && len(pattern.Examples) < 3 {
			pattern.Examples = append(pattern.Examples, example)
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		left := patternsByKind[keys[i]]
		right := patternsByKind[keys[j]]
		if left.Count != right.Count {
			return left.Count > right.Count
		}
		return left.FailureKind < right.FailureKind
	})

	patterns := make([]FailureKindPattern, 0, len(keys))
	for _, key := range keys {
		pattern, ok := patternsByKind[key]
		if !ok || pattern == nil {
			continue
		}
		if pattern.Count < threshold {
			continue
		}
		patterns = append(patterns, *pattern)
	}
	return patterns, nil
}

func ReadAll(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}

	return parseEntries(string(content)), nil
}

func renderEntry(entry Entry, date time.Time) string {
	lines := []string{
		"## " + date.Format("2006-01-02") + " - " + issueRef(entry) + " - \"" + headingTitle(entry) + "\"",
		"- **Captured at:** " + date.UTC().Format(time.RFC3339Nano),
		"- **Failure kind:** " + field(entry.FailureKind, "<unknown>"),
		"- **Issue:** " + field(entry.IssueRef, issueRef(entry)),
		"- **Pull request:** " + field(entry.PullRequest, "<unavailable>"),
		"- **Symptom:** " + field(entry.Symptom, "<unavailable>"),
	}
	for _, evidence := range entry.Evidence {
		if value := field(evidence, ""); value != "" {
			lines = append(lines, "- **Evidence:** "+value)
		}
	}
	if value := field(entry.Hypothesis, ""); value != "" {
		lines = append(lines, "- **Hypothesis (Detent):** "+value)
	}
	if value := field(entry.Hint, ""); value != "" {
		lines = append(lines, "- **Hint for next time:** "+value)
	}
	if captureKey := strings.TrimSpace(entry.CaptureKey); captureKey != "" {
		lines = append(lines, "- **Capture key:** "+field(captureKey, ""))
	}

	return strings.Join(lines, "\n")
}

func issueRef(entry Entry) string {
	if strings.TrimSpace(entry.IssueNumber) != "" {
		return "issue #" + strings.TrimLeft(strings.TrimSpace(entry.IssueNumber), "#")
	}
	if strings.TrimSpace(entry.IssueRef) != "" {
		return strings.TrimSpace(entry.IssueRef)
	}
	return "issue <unknown>"
}

func headingTitle(entry Entry) string {
	return strings.ReplaceAll(field(entry.Title, "Untitled"), `"`, `\"`)
}

func field(value string, fallback string) string {
	normalized := strings.Join(strings.Fields(value), " ")
	if normalized == "" {
		return fallback
	}
	return normalized
}

func parseEntries(content string) []string {
	normalized := strings.ReplaceAll(content, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	entries := make([]string, 0)
	var current []string

	for _, line := range lines {
		if strings.HasPrefix(line, "## ") {
			if len(current) > 0 {
				entries = append(entries, strings.TrimSpace(strings.Join(current, "\n")))
			}
			current = []string{line}
			continue
		}
		if len(current) > 0 {
			current = append(current, line)
		}
	}
	if len(current) > 0 {
		entries = append(entries, strings.TrimSpace(strings.Join(current, "\n")))
	}

	return entries
}

func renderedEntryField(entry string, label string) string {
	prefix := "- **" + label + ":** "
	for _, line := range strings.Split(entry, "\n") {
		line = strings.TrimSpace(line)
		value, ok := strings.CutPrefix(line, prefix)
		if ok {
			return field(value, "")
		}
	}
	return ""
}

func renderedEntryHeading(entry string) string {
	for _, line := range strings.Split(entry, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "## ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "## "))
		}
	}
	return ""
}

func renderedEntryCapturedAt(entry string) (time.Time, bool) {
	if value := renderedEntryField(entry, "Captured at"); value != "" {
		capturedAt, err := time.Parse(time.RFC3339Nano, value)
		if err == nil {
			return capturedAt.UTC(), true
		}
	}
	heading := renderedEntryHeading(entry)
	if len(heading) < len("2006-01-02") {
		return time.Time{}, false
	}
	capturedAt, err := time.Parse("2006-01-02", heading[:len("2006-01-02")])
	if err != nil {
		return time.Time{}, false
	}
	return capturedAt.UTC(), true
}

func writeEntries(path string, entries []string, truncated bool) error {
	const marker = "> [truncated: older lessons omitted]\n\n"
	size := len(marker)
	for index, entry := range entries {
		entry = runtimeoutput.Truncate(entry, MaxEntryBytes).Value
		if size+len(entry)+2 > MaxFileBytes {
			entries = entries[:index]
			truncated = true
			break
		}
		entries[index] = entry
		size += len(entry) + 2
	}
	content := strings.Join(entries, "\n\n") + "\n"
	if truncated {
		content = marker + content
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	return os.WriteFile(path, []byte(content), 0o600)
}
