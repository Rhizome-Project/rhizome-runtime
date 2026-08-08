//go:build windows

package tools

import (
	"fmt"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsToolProcessGuard struct {
	job windows.Handle
}

func newToolProcessGuard() toolProcessGuard {
	return &windowsToolProcessGuard{}
}

func (g *windowsToolProcessGuard) prepare(cmd *exec.Cmd) {}

func (g *windowsToolProcessGuard) afterStart(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return fmt.Errorf("create job object: %w", err)
	}

	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		windows.CloseHandle(job)
		return fmt.Errorf("set job object info: %w", err)
	}

	processHandle, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		windows.CloseHandle(job)
		return fmt.Errorf("open process for job attach: %w", err)
	}
	defer windows.CloseHandle(processHandle)

	if err := windows.AssignProcessToJobObject(job, processHandle); err != nil {
		windows.CloseHandle(job)
		return fmt.Errorf("assign process to job object: %w", err)
	}

	g.job = job
	return nil
}

func (g *windowsToolProcessGuard) terminate(cmd *exec.Cmd) error {
	if g.job != 0 {
		if err := windows.TerminateJobObject(g.job, 1); err == nil {
			return nil
		}
	}
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}

func (g *windowsToolProcessGuard) close() error {
	if g.job != 0 {
		err := windows.CloseHandle(g.job)
		g.job = 0
		return err
	}
	return nil
}
