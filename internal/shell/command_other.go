//go:build !windows

package shell

import "os/exec"

func configureCommand(_ *exec.Cmd, _ string) {}
