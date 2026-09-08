package detent_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type requiredStatusCheck struct {
	name     string
	budget   string
	jobStart string
	jobEnd   string
	markers  []string
}

var requiredPRStatusChecks = []requiredStatusCheck{
	{
		name:     "Lint",
		budget:   "2m",
		jobStart: "  lint:",
		jobEnd:   "  verify:",
		markers:  []string{"name: Lint"},
	},
	{
		name:     "Verify (ubuntu-latest)",
		budget:   "30m",
		jobStart: "  verify:",
		jobEnd:   "  test-cover:",
		markers:  []string{"name: Verify (ubuntu-latest)", "runs-on: ubuntu-latest", "timeout-minutes: 30"},
	},
	{
		name:     "Test Coverage",
		budget:   "4m",
		jobStart: "  test-cover:",
		jobEnd:   "  browser-visual:",
		markers:  []string{"name: Test Coverage", "make test-cover-packages"},
	},
	{
		name:     "Browser Visual",
		budget:   "15m",
		jobStart: "  browser-visual:",
		jobEnd:   "  portability-verify:",
		markers:  []string{"name: Browser Visual", "timeout-minutes: 15", "Run full browser visual gate", "Run browser smoke gate"},
	},
	{
		name:     "Portability Verify (macos-latest)",
		budget:   "8m",
		jobStart: "  portability-verify:",
		jobEnd:   "  windows-core:",
		markers:  []string{"name: Portability Verify (${{ matrix.os }})", "os: [macos-latest, windows-latest]", "go build ./...", "go vet ./...", "go test ./..."},
	},
	{
		name:     "Portability Verify (windows-latest)",
		budget:   "45m",
		jobStart: "  portability-verify:",
		jobEnd:   "  windows-core:",
		markers:  []string{"name: Portability Verify (${{ matrix.os }})", "os: [macos-latest, windows-latest]", "go build ./...", "go vet ./...", "go test ./..."},
	},
	{
		name:     "Windows Core",
		budget:   "4m",
		jobStart: "  windows-core:",
		jobEnd:   "  installer-smoke:",
		markers:  []string{"name: Windows Core"},
	},
	{
		name:     "Installer Smoke (ubuntu-latest)",
		budget:   "6m",
		jobStart: "  installer-smoke:",
		jobEnd:   "  goreleaser-snapshot:",
		markers:  []string{"name: Installer Smoke (${{ matrix.os }})", "os: [ubuntu-latest, windows-latest]"},
	},
	{
		name:     "Installer Smoke (windows-latest)",
		budget:   "6m",
		jobStart: "  installer-smoke:",
		jobEnd:   "  goreleaser-snapshot:",
		markers:  []string{"name: Installer Smoke (${{ matrix.os }})", "os: [ubuntu-latest, windows-latest]"},
	},
	{
		name:     "GoReleaser Snapshot",
		budget:   "15m",
		jobStart: "  goreleaser-snapshot:",
		jobEnd:   "",
		markers:  []string{"name: GoReleaser Snapshot", "timeout-minutes: 15", "args: release --snapshot --clean", "MINISIGN_KEY_FILE: ${{ runner.temp }}/detent-minisign.key"},
	},
}

func TestCIConcurrencyKeepsMainPushRuns(t *testing.T) {
	t.Parallel()

	workflow := readNormalizedFile(t, ".github/workflows/ci.yml")
	concurrency := workflowBetween(t, workflow, "concurrency:\n", "\njobs:")
	for _, want := range []string{
		"group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && format('pr-{0}', github.event.pull_request.number) || github.run_id }}",
		"cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
	} {
		if !strings.Contains(concurrency, want) {
			t.Fatalf("CI concurrency missing %q", want)
		}
	}
}

func TestSnapshotBudgetPreservesReleaseWork(t *testing.T) {
	t.Parallel()

	config := readNormalizedFile(t, ".goreleaser.yaml")
	for _, tt := range []struct {
		name    string
		start   string
		end     string
		markers []string
	}{
		{"hooks", "before:", "\nbuilds:", []string{"go mod download", "go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0", "make generate", "go test ./..."}},
		{"targets", "builds:", "\narchives:", []string{"CGO_ENABLED=0", "-trimpath", "- darwin", "- linux", "- windows", "- amd64", "- arm64"}},
		{"archives", "archives:", "\nbrews:", []string{"- tar.gz", "goos: windows", "- zip", "- README.md", "- LICENSE", "- docs/**/*", "- scripts/hub-smoke.py"}},
		{"packages", "nfpms:", "\nscoops:", []string{"- deb", "- rpm"}},
		{"checksums", "checksum:", "\nsigns:", []string{"algorithm: sha256"}},
		{"signing", "signs:", "\nsnapshot:", []string{"artifacts: checksum", "cmd: minisign", "${artifact}.minisig", "{{ .Env.MINISIGN_KEY_FILE }}"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			section := workflowBetween(t, config, tt.start, tt.end)
			for _, marker := range tt.markers {
				if !strings.Contains(section, marker) {
					t.Errorf("release configuration missing %q", marker)
				}
			}
		})
	}
}

func TestMakeTestTargetsIsolateAPIToken(t *testing.T) {
	t.Parallel()

	makefile := readNormalizedFile(t, "Makefile")
	if !strings.Contains(makefile, "GO_TEST := env -u DETENT_API_TOKEN go test") {
		t.Fatal("Makefile Go test command must remove inherited DETENT_API_TOKEN")
	}

	tests := []struct {
		name string
		want string
	}{
		{name: "test", want: "$(GO_TEST) ./..."},
		{name: "test-race", want: "$(GO_TEST) -race $$packages"},
		{name: "test-race-hub", want: "env -u DETENT_API_TOKEN go run ./tools/testgate -race"},
		{name: "test-cover", want: "$(GO_TEST) -coverprofile=$(COVERPROFILE_RAW) ./..."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(makefile, tt.want) {
				t.Fatalf("Makefile missing %q", tt.want)
			}
		})
	}
}

func TestGolangCILintUsesRepositoryPinnedVersion(t *testing.T) {
	t.Parallel()

	version := strings.TrimSpace(readNormalizedFile(t, ".golangci-version"))
	if !strings.HasPrefix(version, "v2.") {
		t.Fatalf("golangci-lint version = %q, want a v2 release", version)
	}

	makefile := readNormalizedFile(t, "Makefile")
	for _, want := range []string{
		"GOLANGCI_LINT_VERSION_FILE := .golangci-version",
		"GOLANGCI_LINT_VERSION := $(shell cat $(GOLANGCI_LINT_VERSION_FILE))",
		"GOLANGCI_LINT_TOOLCHAIN := $(shell awk '/^toolchain / { print $$2 }' go.mod)",
		"GOLANGCI_LINT_DIR := $(CURDIR)/tmp/tools/golangci-lint/$(GOLANGCI_LINT_VERSION)/$(GOLANGCI_LINT_TOOLCHAIN)",
		"lint: $(GOLANGCI_LINT)",
		`GOTOOLCHAIN="$(GOLANGCI_LINT_TOOLCHAIN)" "$(GOLANGCI_LINT)" run --timeout=5m`,
		`GOTOOLCHAIN="$(GOLANGCI_LINT_TOOLCHAIN)" GOBIN="$(GOLANGCI_LINT_DIR)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)`,
		"setup: $(GOLANGCI_LINT)",
	} {
		if !strings.Contains(makefile, want) {
			t.Fatalf("Makefile missing pinned golangci-lint contract %q", want)
		}
	}
	if strings.Contains(makefile, "cmd/golangci-lint@"+version) {
		t.Fatal("Makefile must read the golangci-lint version from .golangci-version")
	}

	workflow := workflowBetween(t, readNormalizedFile(t, ".github/workflows/ci.yml"), "  lint:", "\n  verify:")
	for _, want := range []string{
		"path: tmp/tools/golangci-lint",
		"hashFiles('.golangci-version')",
		"run: make lint",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("CI lint job missing pinned golangci-lint contract %q", want)
		}
	}
	for _, forbidden := range []string{"~/go/bin/golangci-lint", "cmd/golangci-lint@"} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("CI lint job bypasses the Makefile pin with %q", forbidden)
		}
	}
}

func TestMakeLintIgnoresAmbientBinary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Makefile tooling requires a POSIX shell")
	}
	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Fatal(err)
	}
	for _, cached := range []bool{false, true} {
		name := "install"
		if cached {
			name = "cached"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			write := func(path, content string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join(root, "Makefile"), readNormalizedFile(t, "Makefile"))
			write(filepath.Join(root, ".golangci-version"), "v2.9.0\n")
			write(filepath.Join(root, "go.mod"), "module example.com/test\n\ngo 1.26\n\ntoolchain go1.26.6\n")
			bin := filepath.Join(root, "bin")
			write(filepath.Join(bin, "golangci-lint"), "#!/bin/sh\necho incompatible ambient linter >&2\nexit 99\n")
			linter := "#!/bin/sh\nprintf 'pinned:%s:%s\\n' \"$GOTOOLCHAIN\" \"$*\"\n"
			fixture := filepath.Join(root, "pinned-linter")
			write(fixture, linter)
			write(filepath.Join(bin, "go"), "#!/bin/sh\nset -eu\n[ \"$GOTOOLCHAIN\" = go1.26.6 ]\n[ \"$*\" = 'install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.9.0' ]\ncp pinned-linter \"$GOBIN/golangci-lint\"\n")
			if cached {
				cachedPath := filepath.Join(root, "tmp/tools/golangci-lint/v2.9.0/go1.26.6/golangci-lint")
				write(cachedPath, linter)
				old := time.Unix(1, 0)
				if err := os.Chtimes(cachedPath, old, old); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.CommandContext(t.Context(), makePath, "lint")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("make lint: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "pinned:go1.26.6:run --timeout=5m") {
				t.Fatalf("make lint did not invoke the pinned toolchain: %s", output)
			}
			if installed := strings.Contains(string(output), "go install"); installed == cached {
				t.Fatalf("make lint installed = %v, cached = %v: %s", installed, cached, output)
			}
		})
	}
}

func TestMainProtectionDocumentationMatchesWorkflow(t *testing.T) {
	t.Parallel()

	workflow := readNormalizedFile(t, ".github/workflows/ci.yml")
	docs := readNormalizedFile(t, "docs/execution-seams.md")
	protection := workflowBetween(t, docs, "### Main Branch Protection\n", "\n## Still Git/PR Coupled")

	for _, want := range []string{
		"`required_status_checks.strict: true`",
		"must not report success from a path- or event-dependent no-op",
		"`gate.required_status_checks`",
		"`cancel-in-progress: ${{ github.event_name == 'pull_request' }}`",
		"`Browser Visual`",
	} {
		if !strings.Contains(protection, want) {
			t.Fatalf("main branch protection docs missing %q", want)
		}
	}

	for _, check := range requiredPRStatusChecks {
		if !strings.Contains(protection, "- `"+check.name+"` - budget: `"+check.budget+"`") {
			t.Fatalf("main branch protection docs missing required check %q", check.name)
		}

		job := workflowBetween(t, workflow, check.jobStart, check.jobEnd)
		for _, marker := range check.markers {
			if !strings.Contains(job, marker) {
				t.Fatalf("workflow job for required check %q missing %q", check.name, marker)
			}
		}
	}
}

func TestRequiredChecksDoNotUseEventDependentGreenNoops(t *testing.T) {
	t.Parallel()

	workflow := readNormalizedFile(t, ".github/workflows/ci.yml")
	for _, check := range requiredPRStatusChecks {
		job := workflowBetween(t, workflow, check.jobStart, check.jobEnd)
		for _, forbidden := range []string{
			"github.event_name",
			"EVENT_NAME",
			"pull_request",
			"steps.policy.outputs",
			"Skip ",
			" skipped:",
		} {
			if strings.Contains(job, forbidden) {
				t.Fatalf("required check %q contains green no-op marker %q", check.name, forbidden)
			}
		}
	}
}

func TestPortabilityStressRunsOutsidePullRequestGate(t *testing.T) {
	t.Parallel()

	requiredWorkflow := readNormalizedFile(t, ".github/workflows/ci.yml")
	requiredJob := workflowBetween(t, requiredWorkflow, "  portability-verify:", "\n  windows-core:")
	for _, forbidden := range []string{"go test -race", "-count=10", "-count=20"} {
		if strings.Contains(requiredJob, forbidden) {
			t.Fatalf("required portability job contains heavy coverage %q", forbidden)
		}
	}

	stressWorkflow := readNormalizedFile(t, ".github/workflows/portability-stress.yml")
	for _, want := range []string{
		"schedule:",
		"workflow_dispatch:",
		"timeout-minutes: 45",
		"os: [macos-latest, windows-latest]",
		"go test ./internal/orchestrator -run '^TestLocalSQLiteArtifactLifecycleEndToEnd$' -count=20",
		"go test -race ./internal/cli ./internal/runner ./tools/checklock -count=10 -timeout=30m",
		"go test -race ./...",
	} {
		if !strings.Contains(stressWorkflow, want) {
			t.Fatalf("portability stress workflow missing %q", want)
		}
	}
	if strings.Contains(stressWorkflow, "pull_request:") {
		t.Fatal("portability stress workflow must not run for every pull request")
	}
}

func TestInstallerSmokeUsesAuthenticatedReleaseVersion(t *testing.T) {
	t.Parallel()

	workflow := readNormalizedFile(t, ".github/workflows/ci.yml")
	job := workflowBetween(t, workflow, "  installer-smoke:", "\n  goreleaser-snapshot:")

	for _, want := range []string{
		"name: Resolve release installer version",
		"GH_TOKEN: ${{ github.token }}",
		"bash ./scripts/resolve-release-tag.sh",
		"DETENT_VERSION=$tag",
		"$GITHUB_ENV",
	} {
		if !strings.Contains(job, want) {
			t.Fatalf("installer-smoke job missing %q", want)
		}
	}

	linux := workflowBetween(t, job, "      - name: Smoke release installer\n        if: runner.os == 'Linux'", "      - name: Smoke release installer\n        if: runner.os == 'Windows'")
	for _, want := range []string{
		"2>&1",
		"falling back to go install",
		"Release installer fell back to go install",
		"exit 1",
		"Verified checksum for detent_",
	} {
		if !strings.Contains(linux, want) {
			t.Fatalf("Linux installer smoke step missing %q", want)
		}
	}

	windows := workflowBetween(t, job, "      - name: Smoke release installer\n        if: runner.os == 'Windows'", "")
	for _, want := range []string{
		"falling back to go install",
		"Release installer fell back to go install",
		"Verified checksum for detent_.*_windows_.*\\.zip",
	} {
		if !strings.Contains(windows, want) {
			t.Fatalf("Windows installer smoke step missing %q", want)
		}
	}
}

func TestBrowserVisualGateCoversBoardInteractions(t *testing.T) {
	t.Parallel()

	workflowRaw, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("ReadFile(.github/workflows/ci.yml) error = %v", err)
	}
	workflow := strings.ReplaceAll(string(workflowRaw), "\r\n", "\n")
	visualJob := workflowBetween(t, workflow, "  browser-visual:", "\n  portability-verify:")
	for _, want := range []string{
		"npm run test:visual",
		"tmp/detent --help",
		"go.mod|go.sum",
		"name: Upload browser visual evidence",
		"tmp/playwright-evidence",
		"name: Upload browser visual failure artifacts",
		"tmp/playwright-report",
		"tmp/playwright-results",
	} {
		if !strings.Contains(visualJob, want) {
			t.Fatalf("browser visual job missing %q", want)
		}
	}

	visualSpecRaw, err := os.ReadFile("tests/visual/layout.spec.js")
	if err != nil {
		t.Fatalf("ReadFile(tests/visual/layout.spec.js) error = %v", err)
	}
	visualSpec := strings.ReplaceAll(string(visualSpecRaw), "\r\n", "\n")
	for _, want := range []string{
		`test("board card opens the detail sheet"`,
		`[data-detail-sheet]`,
		`test("board lane picker hides and restores lanes"`,
		`test("board applies snapshot updates without reload"`,
	} {
		if !strings.Contains(visualSpec, want) {
			t.Fatalf("browser visual spec missing %q", want)
		}
	}
}

func readNormalizedFile(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return strings.ReplaceAll(string(raw), "\r\n", "\n")
}

func workflowBetween(t *testing.T, content string, startMarker string, endMarker string) string {
	t.Helper()

	start := strings.Index(content, startMarker)
	if start == -1 {
		t.Fatalf("workflow missing marker %q", startMarker)
	}
	section := content[start:]
	if endMarker == "" {
		return section
	}
	end := strings.Index(section[len(startMarker):], endMarker)
	if end == -1 {
		t.Fatalf("workflow missing end marker %q after %q", endMarker, startMarker)
	}
	return section[:len(startMarker)+end]
}
