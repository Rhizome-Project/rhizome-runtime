//go:build !windows

package main

import (
	"fmt"
	"os/exec"
	"syscall"
)

type shellCommandProcessHandle struct {
	pid int
}

func configureShellCommandProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func attachShellCommandProcessTree(cmd *exec.Cmd) (*shellCommandProcessHandle, error) {
	if cmd == nil || cmd.Process == nil {
		return nil, fmt.Errorf("shell process not started")
	}
	return &shellCommandProcessHandle{pid: cmd.Process.Pid}, nil
}

func fallbackShellCommandProcessHandle(cmd *exec.Cmd) *shellCommandProcessHandle {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return &shellCommandProcessHandle{pid: cmd.Process.Pid}
}

func (h *shellCommandProcessHandle) terminate() error {
	if h == nil {
		return nil
	}
	return killShellCommandProcessTreeByPID(h.pid)
}

func (h *shellCommandProcessHandle) release() {}

func killShellCommandProcessTreeByPID(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err == nil || err == syscall.ESRCH {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}

func cleanupShellCommandWorkdirProcesses(workdir string) (string, error) {
	return "", nil
}
