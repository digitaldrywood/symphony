//go:build unix

package claudecode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/runner"
)

func TestRunTurnCancellationKillsChildProcessGroup(t *testing.T) {
	t.Parallel()

	pidPath := filepath.Join(t.TempDir(), "child.pid")
	backend := newTestBackend(t, Options{
		CommandFactory: func(ctx context.Context) *exec.Cmd {
			script := "sleep 3600 & printf '%s\n' \"$!\" > " + shellQuote(pidPath) + "; wait"
			return exec.CommandContext(ctx, "sh", "-c", script)
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	workspace := t.TempDir()
	errCh := make(chan error, 1)
	go func() {
		_, err := backend.RunTurn(ctx, runner.AgentTurnRequest{
			Workspace: workspace,
			Prompt:    "cancel",
			Model:     "fable",
		}, nil)
		errCh <- err
	}()

	pid := waitForPIDFile(t, pidPath)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunTurn() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunTurn() did not return after context cancellation")
	}
	waitForProcessExit(t, pid)
}

func TestRunTurnDetectsExitedParentWithInheritedStdout(t *testing.T) {
	t.Parallel()

	pidPath := filepath.Join(t.TempDir(), "child.pid")
	backend := newTestBackend(t, Options{
		CommandFactory: func(ctx context.Context) *exec.Cmd {
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestClaudeCodeDeadParentHelper", "--")
			cmd.Env = append(os.Environ(),
				"CLAUDECODE_DEAD_PARENT_HELPER=1",
				"CLAUDECODE_CHILD_PID_PATH="+pidPath,
			)
			return cmd
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	workspace := t.TempDir()
	errCh := make(chan error, 1)
	go func() {
		_, err := backend.RunTurn(ctx, runner.AgentTurnRequest{
			Workspace: workspace,
			Prompt:    "detect dead parent",
			Model:     "fable",
		}, nil)
		errCh <- err
	}()

	pid := waitForPIDFile(t, pidPath)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrMissingResult) {
			t.Fatalf("RunTurn() error = %v, want ErrMissingResult", err)
		}
		if !strings.Contains(err.Error(), "process exited: exit status 9") {
			t.Fatalf("RunTurn() error = %q, want provider exit status", err)
		}
	case <-time.After(time.Second):
		cancel()
		<-errCh
		t.Fatal("RunTurn() did not detect the exited provider parent")
	}
	waitForProcessExit(t, pid)
}

func TestClaudeCodeDeadParentHelper(t *testing.T) {
	if os.Getenv("CLAUDECODE_DEAD_PARENT_HELPER") != "1" {
		return
	}

	cmd := exec.CommandContext(t.Context(), "sleep", "3600")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		os.Exit(2)
	}
	if err := os.WriteFile(os.Getenv("CLAUDECODE_CHILD_PID_PATH"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		os.Exit(3)
	}
	fmt.Fprintln(os.Stdout, `{"type":"system","subtype":"init","session_id":"session-dead-parent","model":"fable"}`)
	os.Exit(9)
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()

	deadline := time.After(time.Second)
	var lastErr error
	var lastRaw string
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			lastRaw = string(raw)
			pid, parseErr := strconv.Atoi(strings.TrimSpace(lastRaw))
			if parseErr == nil && pid > 0 {
				return pid
			}
			if parseErr != nil {
				lastErr = parseErr
			} else {
				lastErr = errors.New("pid is not positive")
			}
		} else {
			lastErr = err
		}

		select {
		case <-deadline:
			if lastRaw != "" {
				t.Fatalf("timed out waiting for parseable pid file, last value %q: %v", lastRaw, lastErr)
			}
			t.Fatalf("timed out waiting for pid file: %v", lastErr)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		if !processAlive(pid) {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("process %d is still alive", pid)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestWaitAndCleanupPreservesFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		exitCode   int
		cleanupErr error
	}{
		{name: "successful exit and cleanup"},
		{name: "successful exit with cleanup failure", cleanupErr: syscall.EPERM},
		{name: "failed exit", exitCode: 9},
		{name: "failed exit and cleanup", exitCode: 9, cleanupErr: syscall.EPERM},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := exec.CommandContext(t.Context(), "sh", "-c", "exit "+strconv.Itoa(tt.exitCode))
			if err := cmd.Start(); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			cleanupCalls := 0
			waitErr := waitAndCleanupWith(cmd, cmd.Process.Pid, func(groupID int) error {
				cleanupCalls++
				if groupID != cmd.Process.Pid {
					t.Errorf("cleanup group = %d, want %d", groupID, cmd.Process.Pid)
				}
				if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != tt.exitCode {
					t.Errorf("process state at cleanup = %v, want exit status %d", cmd.ProcessState, tt.exitCode)
				}
				return tt.cleanupErr
			})
			if cleanupCalls != 1 {
				t.Fatalf("cleanup calls = %d, want 1", cleanupCalls)
			}
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) != (tt.exitCode != 0) {
				t.Errorf("wait error = %v, want exit error = %t", waitErr, tt.exitCode != 0)
			}
			if tt.cleanupErr != nil {
				if !errors.Is(waitErr, tt.cleanupErr) {
					t.Errorf("wait error = %v, want cleanup error %v", waitErr, tt.cleanupErr)
				}
				want := fmt.Sprintf("clean up claude process group %d after exit status %d", cmd.Process.Pid, tt.exitCode)
				if waitErr == nil || !strings.Contains(waitErr.Error(), want) {
					t.Errorf("wait error = %v, want diagnostic %q", waitErr, want)
				}
			}
			state := turnState{sawResult: true, resultSubtype: "success"}
			err := finalTurnError(t.Context(), state, waitErr, "")
			wantFailure := tt.exitCode != 0 || tt.cleanupErr != nil
			if errors.Is(err, ErrTurnFailed) != wantFailure {
				t.Errorf("finalTurnError() = %v, want turn failure = %t", err, wantFailure)
			}
			if !wantFailure && err != nil {
				t.Errorf("finalTurnError() = %v, want nil", err)
			}
		})
	}
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
