//go:build !windows

package main

import (
	"io"
	"os/exec"
	"syscall"
)

func startDetachedProcess(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
	cmd := exec.Command(executablePath, args...)
	cmd.Dir = workdir
	cmd.Env = env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	go func() {
		_ = cmd.Wait()
	}()
	return pid, nil
}

func releaseManagedAgentProcessResources(pid int) {}

func managedAgentProcessTreeKnown(pid int) bool { return false }

func cleanupManagedAgentWorkdirProcesses(workdir string) (string, error) {
	return "", nil
}

func processExists(pid int) (bool, error) {
	process, err := syscall.Getpgid(pid)
	if err == nil && process >= 0 {
		return true, nil
	}
	if err == syscall.ESRCH {
		return false, nil
	}
	return false, err
}

func killProcess(pid int) error {
	if err := signalManagedAgentProcess(pid, syscall.SIGTERM); err != nil {
		return err
	}
	if err := waitForProcessExit(pid, managedAgentStopExitTimeout); err == nil {
		return nil
	}
	return signalManagedAgentProcess(pid, syscall.SIGKILL)
}

func signalManagedAgentProcess(pid int, signal syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-pid, signal); err == nil {
		return nil
	}
	if err := syscall.Kill(pid, signal); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
