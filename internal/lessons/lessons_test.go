package lessons

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestReadAllAndRecentTreatMissingFileAsEmpty(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), ".detent", "lessons.md")

	entries, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("ReadAll() len = %d, want 0", len(entries))
	}

	recent, err := Recent(path, 3)
	if err != nil {
		t.Fatalf("Recent() error = %v", err)
	}
	if len(recent) != 0 {
		t.Fatalf("Recent() len = %d, want 0", len(recent))
	}
}

func TestAppendStoresNewestFirstAndCapsEntries(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), ".detent", "lessons.md")

	for index := 1; index <= 4; index++ {
		err := Append(path, Entry{
			IssueNumber: strconv.Itoa(index),
			Title:       "Issue " + strconv.Itoa(index),
			FailureKind: "kind " + strconv.Itoa(index),
			Symptom:     "symptom " + strconv.Itoa(index),
			Hypothesis:  "hypothesis " + strconv.Itoa(index),
			Hint:        "hint " + strconv.Itoa(index),
		}, AppendOptions{
			Date:       time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC),
			MaxEntries: 3,
		})
		if err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}

	entries, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries len = %d, want 3", len(entries))
	}
	if !strings.Contains(entries[0], "issue #4") || !strings.Contains(entries[2], "issue #2") {
		t.Fatalf("entries not newest first with oldest capped: %#v", entries)
	}
	if strings.Contains(strings.Join(entries, "\n"), "issue #1") {
		t.Fatalf("oldest entry was not capped: %#v", entries)
	}

	recent, err := Recent(path, 2)
	if err != nil {
		t.Fatalf("Recent() error = %v", err)
	}
	if len(recent) != 2 || !strings.Contains(recent[0], "issue #4") || !strings.Contains(recent[1], "issue #3") {
		t.Fatalf("Recent() = %#v, want issue #4 then issue #3", recent)
	}
}

func TestFailureKindPatternsGroupsRepeatedKinds(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), ".detent", "lessons.md")
	entries := []Entry{
		{IssueNumber: "1", Title: "First ceiling", FailureKind: "token_ceiling_exceeded", Symptom: "hit ceiling"},
		{IssueNumber: "2", Title: "Transient CI", FailureKind: "transient_ci", Symptom: "flake"},
		{IssueNumber: "3", Title: "Second ceiling", FailureKind: "token_ceiling_exceeded", Symptom: "hit ceiling again"},
	}
	for index, entry := range entries {
		if err := Append(path, entry, AppendOptions{Date: time.Date(2026, 6, 1+index, 0, 0, 0, 0, time.UTC)}); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}

	patterns, err := FailureKindPatterns(path, 2)
	if err != nil {
		t.Fatalf("FailureKindPatterns() error = %v", err)
	}
	if len(patterns) != 1 {
		t.Fatalf("patterns len = %d, want 1: %#v", len(patterns), patterns)
	}
	pattern := patterns[0]
	if pattern.FailureKind != "token_ceiling_exceeded" || pattern.Count != 2 {
		t.Fatalf("pattern = %#v, want token ceiling count 2", pattern)
	}
	if len(pattern.Examples) != 2 || !strings.Contains(pattern.Examples[0], "Second ceiling") || !strings.Contains(pattern.Examples[1], "First ceiling") {
		t.Fatalf("examples = %#v, want newest token-ceiling lessons", pattern.Examples)
	}
}

func TestAppendRendersFallbacksAndEscapesTitleQuotes(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), ".detent", "lessons.md")

	err := Append(path, Entry{
		IssueRef:    "issue MT-9",
		Title:       `Needs "quotes" escaped`,
		FailureKind: "",
		Symptom:     "  command\nfailed\tbefore diff  ",
		Hypothesis:  "",
		Hint:        "",
	}, AppendOptions{Date: time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	entries, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	entry := entries[0]
	for _, want := range []string{
		`## 2026-05-22 - issue MT-9 - "Needs \"quotes\" escaped"`,
		"- **Captured at:** 2026-05-22T00:00:00Z",
		"- **Failure kind:** <unknown>",
		"- **Issue:** issue MT-9",
		"- **Pull request:** <unavailable>",
		"- **Symptom:** command failed before diff",
	} {
		if !strings.Contains(entry, want) {
			t.Fatalf("entry missing %q:\n%s", want, entry)
		}
	}
}

func TestAppendRoundTripsStructuredFields(t *testing.T) {
	t.Parallel()

	capturedAt := time.Date(2026, 7, 17, 14, 15, 16, 123, time.UTC)
	tests := []struct {
		name  string
		entry Entry
		want  map[string]string
	}{
		{
			name: "issue and pull request context",
			entry: Entry{
				IssueRef:    "digitaldrywood/detent#1397",
				PullRequest: "https://github.com/digitaldrywood/detent/pull/1401",
				Title:       "Capture rework",
				FailureKind: "ci_failure",
				Symptom:     "checks failed: test, lint",
				Hypothesis:  "the change was not ready",
				Hint:        "run the gate locally",
				CaptureKey:  "rework|detent|1397|2026-07-17T14:15:16.000000123Z",
			},
			want: map[string]string{
				"Captured at":         capturedAt.Format(time.RFC3339Nano),
				"Failure kind":        "ci_failure",
				"Issue":               "digitaldrywood/detent#1397",
				"Pull request":        "https://github.com/digitaldrywood/detent/pull/1401",
				"Symptom":             "checks failed: test, lint",
				"Hypothesis (Detent)": "the change was not ready",
				"Hint for next time":  "run the gate locally",
				"Capture key":         "rework|detent|1397|2026-07-17T14:15:16.000000123Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "lessons.md")
			if err := Append(path, tt.entry, AppendOptions{Date: capturedAt}); err != nil {
				t.Fatalf("Append() error = %v", err)
			}
			entries, err := ReadAll(path)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			if len(entries) != 1 {
				t.Fatalf("ReadAll() len = %d, want 1", len(entries))
			}
			for label, want := range tt.want {
				if got := renderedEntryField(entries[0], label); got != want {
					t.Errorf("renderedEntryField(%q) = %q, want %q", label, got, want)
				}
			}
		})
	}
}

func TestAppendUniqueAndCaptureSummary(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "lessons.md")
	firstAt := time.Date(2026, 7, 16, 9, 30, 0, 0, time.FixedZone("CDT", -5*60*60))
	secondAt := firstAt.Add(2 * time.Hour)
	tests := []struct {
		name       string
		captureKey string
		capturedAt time.Time
		wantAppend bool
	}{
		{name: "first capture", captureKey: "rework|detent|1397|first", capturedAt: firstAt, wantAppend: true},
		{name: "duplicate capture", captureKey: "rework|detent|1397|first", capturedAt: firstAt, wantAppend: false},
		{name: "second capture", captureKey: "rework|detent|1397|second", capturedAt: secondAt, wantAppend: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appended, err := AppendUnique(path, Entry{
				IssueRef:    "digitaldrywood/detent#1397",
				FailureKind: "ci_failure",
				CaptureKey:  tt.captureKey,
			}, AppendOptions{Date: tt.capturedAt})
			if err != nil {
				t.Fatalf("AppendUnique() error = %v", err)
			}
			if appended != tt.wantAppend {
				t.Fatalf("AppendUnique() = %t, want %t", appended, tt.wantAppend)
			}
		})
	}

	summary, err := CaptureSummary(path)
	if err != nil {
		t.Fatalf("CaptureSummary() error = %v", err)
	}
	if summary.Count != 2 || summary.LastCapturedAt == nil || !summary.LastCapturedAt.Equal(secondAt.UTC()) {
		t.Fatalf("CaptureSummary() = %#v, want count 2 last %v", summary, secondAt.UTC())
	}
}

func TestReadAllReturnsReadErrors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	_, err := ReadAll(root)
	if err == nil {
		t.Fatal("ReadAll() error = nil, want error")
	}
	_, err = Recent(root, 1)
	if err == nil {
		t.Fatal("Recent() error = nil, want error")
	}
	if err := Append(root, Entry{Title: "cannot append"}, AppendOptions{}); err == nil {
		t.Fatal("Append() error = nil, want error")
	}
}

func TestAppendBoundsRetainedContent(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		count      int
		maxEntries int
		body       string
		wantCount  int
	}{
		{name: "entry count", count: 5, maxEntries: 3, body: "specific failure", wantCount: 3},
		{name: "default count", count: 60, body: "specific failure", wantCount: DefaultMaxEntries},
		{name: "byte limit", count: 10, maxEntries: 100, body: strings.Repeat("failure ", 400), wantCount: 4},
		{name: "oversized unicode", count: 1, body: strings.Repeat("界", MaxFileBytes), wantCount: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "lessons.md")
			for index := range tt.count {
				entry := Entry{IssueNumber: strconv.Itoa(index), Evidence: []string{tt.body}, CaptureKey: "key-" + strconv.Itoa(index)}
				if err := Append(path, entry, AppendOptions{MaxEntries: tt.maxEntries}); err != nil {
					t.Fatal(err)
				}
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > MaxFileBytes || !utf8.Valid(data) || !strings.Contains(string(data), "truncated") {
				t.Fatalf("retained file: bytes=%d valid=%t marker=%t", len(data), utf8.Valid(data), strings.Contains(string(data), "truncated"))
			}
			entries, err := ReadAll(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != tt.wantCount || !strings.Contains(entries[0], "issue #"+strconv.Itoa(tt.count-1)) {
				t.Fatalf("retained entries = %d, want %d and newest first", len(entries), tt.wantCount)
			}
			for _, entry := range entries {
				if len(entry) > MaxEntryBytes {
					t.Fatalf("entry bytes = %d", len(entry))
				}
			}
			appended, err := AppendUnique(path, Entry{CaptureKey: "key-" + strconv.Itoa(tt.count-1)}, AppendOptions{})
			if err != nil || appended {
				t.Fatalf("retained newest deduplication = %t, %v", appended, err)
			}
		})
	}
}

func TestAppendOmitsEmptyAdvice(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, hypothesis, hint string
		want                   bool
	}{
		{name: "absent"},
		{name: "whitespace", hypothesis: " \n ", hint: "\t"},
		{name: "explicit advice", hypothesis: "The assertion lacks an await", hint: "Wait for the event", want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			text := renderEntry(Entry{Hypothesis: tt.hypothesis, Hint: tt.hint}, time.Now())
			for _, label := range []string{"Hypothesis (Detent)", "Hint for next time"} {
				if strings.Contains(text, label) != tt.want {
					t.Fatalf("unexpected %s in %s", label, text)
				}
			}
		})
	}
}
