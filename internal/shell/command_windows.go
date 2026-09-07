package shell

import (
	"os/exec"
	"syscall"
)

func configureCommand(cmd *exec.Cmd, goos string) {
	if goos != "windows" || shellBase(cmd.Args[0]) != "cmd" {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CmdLine: syscall.EscapeArg(cmd.Args[0]) + ` /S /C "` + cmd.Args[len(cmd.Args)-1] + `"`,
	}
}
