package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type testEvent struct {
	Time        time.Time `json:"Time"`
	Action      string    `json:"Action"`
	Package     string    `json:"Package"`
	ImportPath  string    `json:"ImportPath"`
	FailedBuild string    `json:"FailedBuild"`
	Test        string    `json:"Test"`
	Output      string    `json:"Output"`
	Elapsed     float64   `json:"Elapsed"`
}

type packageResult struct {
	Package        string        `json:"package"`
	Outcome        string        `json:"outcome"`
	Classification string        `json:"classification,omitempty"`
	Elapsed        float64       `json:"elapsed_seconds,omitempty"`
	EvidenceFile   string        `json:"evidence_file"`
	Tests          []*testTiming `json:"tests,omitempty"`

	packageTimeout bool
	testTimeouts   map[string]bool
	timings        map[string]*testTiming
}

type gateSummary struct {
	Schema             int             `json:"schema"`
	PackageParallelism int             `json:"package_parallelism"`
	TestParallelism    int             `json:"test_parallelism"`
	PackageTimeout     string          `json:"package_timeout"`
	Race               bool            `json:"race"`
	Packages           []packageResult `json:"packages"`
}

type evidenceCollector struct {
	dir        string
	combined   io.Writer
	console    io.Writer
	files      map[string]*os.File
	results    map[string]*packageResult
	buildLines map[string][][]byte
	closeErrs  []error
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("testgate", flag.ContinueOnError)
	flags.SetOutput(stderr)

	outputDir := flags.String("output", filepath.Join("tmp", "windows-test-evidence"), "test evidence output directory")
	testParallelism := flags.Int("parallel", 4, "maximum parallel tests within one package")
	packageTimeout := flags.Duration("timeout", 10*time.Minute, "timeout for each test package")
	race := flags.Bool("race", false, "enable the race detector")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*outputDir) == "" {
		fmt.Fprintln(stderr, "-output is required")
		return 2
	}
	if *testParallelism <= 0 {
		fmt.Fprintln(stderr, "-parallel must be positive")
		return 2
	}
	if *packageTimeout <= 0 {
		fmt.Fprintln(stderr, "-timeout must be positive")
		return 2
	}
	packages := flags.Args()
	if len(packages) == 0 {
		packages = []string{"./..."}
	}

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fmt.Fprintf(stderr, "create evidence directory: %v\n", err)
		return 1
	}
	combinedPath := filepath.Join(*outputDir, "combined.jsonl")
	combined, err := os.Create(combinedPath)
	if err != nil {
		fmt.Fprintf(stderr, "create combined evidence: %v\n", err)
		return 1
	}

	collector := newEvidenceCollector(*outputDir, combined, stdout)
	commandErr := runGoTest(ctx, packages, *testParallelism, *packageTimeout, *race, collector)
	closeErr := errors.Join(collector.close(), combined.Close())
	summary := collector.summary(*testParallelism, *packageTimeout)
	summary.Race = *race
	summaryErr := writeSummary(*outputDir, summary)
	if commandErr != nil {
		fmt.Fprintf(stderr, "Test gate failed: %v\n", commandErr)
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "close test evidence: %v\n", closeErr)
	}
	if summaryErr != nil {
		fmt.Fprintf(stderr, "write test summary: %v\n", summaryErr)
	}
	if commandErr != nil || closeErr != nil || summaryErr != nil {
		return 1
	}
	return 0
}

func runGoTest(ctx context.Context, packages []string, parallel int, timeout time.Duration, race bool, collector *evidenceCollector) error {
	cmd := exec.CommandContext(ctx, "go", "test")
	cmd.Args = append(cmd.Args, goTestArgs(packages, parallel, timeout, race)...)
	output, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("capture go test output: %w", err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start go test: %w", err)
	}

	scanErr := collector.collect(output)
	waitErr := cmd.Wait()
	if scanErr != nil {
		return errors.Join(fmt.Errorf("collect go test evidence: %w", scanErr), waitErr)
	}
	return waitErr
}

func goTestArgs(packages []string, parallel int, timeout time.Duration, race bool) []string {
	args := []string{
		"-json",
		"-p=1",
		"-parallel=" + strconv.Itoa(parallel),
		"-timeout=" + timeout.String(),
	}
	if race {
		args = append(args, "-race", "-count=1")
	}
	return append(args, packages...)
}

func newEvidenceCollector(dir string, combined, console io.Writer) *evidenceCollector {
	return &evidenceCollector{
		dir:        dir,
		combined:   combined,
		console:    console,
		files:      make(map[string]*os.File),
		results:    make(map[string]*packageResult),
		buildLines: make(map[string][][]byte),
	}
}

func (c *evidenceCollector) collect(input io.Reader) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if err := c.collectLine(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (c *evidenceCollector) collectLine(line []byte) error {
	for _, writer := range []io.Writer{c.combined, c.console} {
		if _, err := writer.Write(append(line, '\n')); err != nil {
			return err
		}
	}

	var event testEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return nil
	}
	if event.ImportPath != "" {
		c.buildLines[event.ImportPath] = append(c.buildLines[event.ImportPath], line)
		return nil
	}
	if event.Package == "" {
		return nil
	}
	result := c.result(event.Package)
	file, err := c.file(result)
	if err != nil {
		return err
	}
	if err := c.flushBuildLines(event.FailedBuild, file); err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	result.apply(event)
	return nil
}

func (c *evidenceCollector) flushBuildLines(importPath string, file io.Writer) error {
	if importPath == "" {
		return nil
	}
	for _, line := range c.buildLines[importPath] {
		if _, err := file.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	delete(c.buildLines, importPath)
	return nil
}

func (c *evidenceCollector) result(packagePath string) *packageResult {
	if result, ok := c.results[packagePath]; ok {
		return result
	}
	result := &packageResult{
		Package:      packagePath,
		Outcome:      "running",
		EvidenceFile: packageEvidenceName(packagePath),
		testTimeouts: make(map[string]bool),
	}
	c.results[packagePath] = result
	return result
}

func (c *evidenceCollector) file(result *packageResult) (*os.File, error) {
	if file, ok := c.files[result.Package]; ok {
		return file, nil
	}
	file, err := os.Create(filepath.Join(c.dir, result.EvidenceFile))
	if err != nil {
		return nil, fmt.Errorf("create %s evidence: %w", result.Package, err)
	}
	c.files[result.Package] = file
	return file, nil
}

func (c *evidenceCollector) close() error {
	for _, file := range c.files {
		if err := file.Close(); err != nil {
			c.closeErrs = append(c.closeErrs, err)
		}
	}
	return errors.Join(c.closeErrs...)
}

func (c *evidenceCollector) summary(parallel int, timeout time.Duration) gateSummary {
	packages := make([]packageResult, 0, len(c.results))
	for _, result := range c.results {
		result.finalize()
		packages = append(packages, *result)
	}
	sort.Slice(packages, func(i, j int) bool {
		return packages[i].Package < packages[j].Package
	})
	return gateSummary{
		Schema:             1,
		PackageParallelism: 1,
		TestParallelism:    parallel,
		PackageTimeout:     timeout.String(),
		Packages:           packages,
	}
}

func (r *packageResult) apply(event testEvent) {
	r.recordTiming(event)
	if (event.Action == "pass" || event.Action == "skip") && event.Test == "" {
		r.Outcome = event.Action
		r.Elapsed = event.Elapsed
	}
	if event.Action == "fail" {
		if event.Test == "" {
			r.Outcome = "fail"
			r.Elapsed = event.Elapsed
		} else if operationTimeoutOutput(event.Output) {
			r.testTimeouts[event.Test] = true
		}
	}
	if packageTimeoutOutput(event.Output) {
		r.packageTimeout = true
	}
	if event.Test != "" && operationTimeoutOutput(event.Output) {
		r.testTimeouts[event.Test] = true
	}
}

func (r *packageResult) finalize() {
	r.Tests = nil
	for _, timing := range r.timings {
		r.Tests = append(r.Tests, timing)
	}
	sort.Slice(r.Tests, func(i, j int) bool { return r.Tests[i].Test < r.Tests[j].Test })
	if r.Outcome != "fail" {
		return
	}
	switch {
	case r.packageTimeout:
		r.Classification = "package_timeout"
	case len(r.testTimeouts) > 0:
		r.Classification = "operation_timeout"
	default:
		r.Classification = "assertion"
	}
}

func packageTimeoutOutput(output string) bool {
	return strings.Contains(strings.ToLower(output), "panic: test timed out after")
}

func operationTimeoutOutput(output string) bool {
	output = strings.ToLower(output)
	for _, marker := range []string{
		"context deadline exceeded",
		"deadline exceeded",
		"i/o timeout",
		"operation timed out",
		"stream stalled",
		"timed out waiting",
		"timeout waiting",
	} {
		if strings.Contains(output, marker) {
			return true
		}
	}
	return false
}

func packageEvidenceName(packagePath string) string {
	name := strings.TrimPrefix(packagePath, "github.com/digitaldrywood/detent/")
	name = strings.Trim(name, "/")
	replacer := strings.NewReplacer("/", "__", "\\", "__", ":", "_", " ", "_")
	name = replacer.Replace(name)
	if name == "" {
		name = "root"
	}
	return name + ".jsonl"
}

func writeSummary(dir string, summary gateSummary) error {
	file, err := os.Create(filepath.Join(dir, "summary.json"))
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(summary)
	return errors.Join(encodeErr, file.Close())
}
