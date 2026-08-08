//go:build unix

package tools

import (
	"os/exec"
	"syscall"
)

type unixToolProcessGuard struct{}

func newToolProcessGuard() toolProcessGuard {
	return &unixToolProcessGuard{}
}

func (g *unixToolProcessGuard) prepare(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func (g *unixToolProcessGuard) afterStart(cmd *exec.Cmd) error {
	return nil
}

func (g *unixToolProcessGuard) terminate(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	// Kill the whole process group so descendant processes do not keep the
	// inherited stdout/stderr pipes alive after the parent is canceled.
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

func (g *unixToolProcessGuard) close() error {
	return nil
}
