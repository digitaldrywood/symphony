package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

const validationLockPollInterval = 100 * time.Millisecond

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("checklock", flag.ContinueOnError)
	flags.SetOutput(stderr)

	lockPath := flags.String("lock", "", "validation lock path")
	waitTimeout := flags.Duration("wait-timeout", 15*time.Minute, "maximum time to wait for another validation gate")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *lockPath == "" {
		fmt.Fprintln(stderr, "-lock is required")
		return 2
	}
	if *waitTimeout <= 0 {
		fmt.Fprintln(stderr, "-wait-timeout must be positive")
		return 2
	}
	command := flags.Args()
	if len(command) == 0 {
		fmt.Fprintln(stderr, "command is required after --")
		return 2
	}

	waitCtx, cancel := context.WithTimeout(ctx, *waitTimeout)
	defer cancel()

	lock, waited, err := acquireValidationLock(waitCtx, *lockPath, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "acquire validation lock: %v\n", err)
		return 1
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintf(stderr, "validation canceled before command start: %v\n", errors.Join(err, lock.Close()))
		return 1
	}
	if waited {
		fmt.Fprintln(stderr, "validation gate acquired shared lock; starting validation (wait timeout no longer applies)")
	}

	commandPath, commandPathErr := exec.LookPath(command[0])
	if commandPathErr != nil {
		fmt.Fprintf(stderr, "resolve validation command: %v\n", commandPathErr)
		if err := lock.Close(); err != nil {
			fmt.Fprintf(stderr, "release validation lock: %v\n", err)
		}
		return 1
	}
	cmd := &exec.Cmd{
		Path:   commandPath,
		Args:   command,
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
	commandErr := cmd.Run()
	closeErr := lock.Close()
	if closeErr != nil {
		fmt.Fprintf(stderr, "release validation lock: %v\n", closeErr)
	}
	if commandErr != nil {
		var exitErr *exec.ExitError
		if errors.As(commandErr, &exitErr) {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(stderr, "run validation command: %v\n", commandErr)
		return 1
	}
	if closeErr != nil {
		return 1
	}
	return 0
}
