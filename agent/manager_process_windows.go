//go:build windows

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

const procThreadAttributeJobList = 0x0002000D

var managedAgentWindowsJobs = struct {
	mu   sync.Mutex
	jobs map[int]windows.Handle
}{jobs: make(map[int]windows.Handle)}

func startDetachedProcess(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
	job, err := newManagedAgentWindowsJob(false)
	if err != nil {
		return 0, err
	}
	jobAssigned := false
	defer func() {
		if !jobAssigned {
			_ = windows.CloseHandle(job)
		}
	}()

	stdoutHandle, closeStdout, err := inheritableManagedAgentWriterHandle(stdout)
	if err != nil {
		return 0, fmt.Errorf("prepare managed agent stdout: %w", err)
	}
	defer closeStdout()
	stderrHandle, closeStderr, err := inheritableManagedAgentWriterHandle(stderr)
	if err != nil {
		return 0, fmt.Errorf("prepare managed agent stderr: %w", err)
	}
	defer closeStderr()
	stdinHandle, _ := windows.GetStdHandle(windows.STD_INPUT_HANDLE)
	if stdinHandle == windows.InvalidHandle {
		stdinHandle = 0
	}
	handleList := []windows.Handle{stdoutHandle, stderrHandle}
	if stdinHandle != 0 {
		handleList = append(handleList, stdinHandle)
	}

	attrs, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return 0, fmt.Errorf("create managed agent startup attributes: %w", err)
	}
	defer attrs.Delete()
	jobList := []windows.Handle{job}
	if err := attrs.Update(procThreadAttributeJobList, unsafe.Pointer(&jobList[0]), unsafe.Sizeof(jobList[0])*uintptr(len(jobList))); err != nil {
		return 0, fmt.Errorf("attach managed agent job at process creation: %w", err)
	}
	if err := attrs.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&handleList[0]), unsafe.Sizeof(handleList[0])*uintptr(len(handleList))); err != nil {
		return 0, fmt.Errorf("attach managed agent stdio handles: %w", err)
	}

	appName, err := windows.UTF16PtrFromString(executablePath)
	if err != nil {
		return 0, fmt.Errorf("encode managed agent executable path: %w", err)
	}
	commandLine, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(append([]string{executablePath}, args...)))
	if err != nil {
		return 0, fmt.Errorf("encode managed agent command line: %w", err)
	}
	var cwd *uint16
	if strings.TrimSpace(workdir) != "" {
		cwd, err = windows.UTF16PtrFromString(workdir)
		if err != nil {
			return 0, fmt.Errorf("encode managed agent workdir: %w", err)
		}
	}
	envBlock, err := windowsEnvironmentBlock(env)
	if err != nil {
		return 0, fmt.Errorf("encode managed agent environment: %w", err)
	}
	var envPtr *uint16
	if len(envBlock) > 0 {
		envPtr = &envBlock[0]
	}
	startup := windows.StartupInfoEx{
		StartupInfo: windows.StartupInfo{
			Cb:        uint32(unsafe.Sizeof(windows.StartupInfoEx{})),
			Flags:     windows.STARTF_USESTDHANDLES,
			StdInput:  stdinHandle,
			StdOutput: stdoutHandle,
			StdErr:    stderrHandle,
		},
		ProcThreadAttributeList: attrs.List(),
	}
	var pi windows.ProcessInformation
	creationFlags := uint32(windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_UNICODE_ENVIRONMENT | windows.EXTENDED_STARTUPINFO_PRESENT)
	if err := windows.CreateProcess(appName, commandLine, nil, nil, true, creationFlags, envPtr, cwd, &startup.StartupInfo, &pi); err != nil {
		return 0, err
	}
	defer windows.CloseHandle(pi.Process)
	defer windows.CloseHandle(pi.Thread)
	pid := int(pi.ProcessId)
	rememberManagedAgentWindowsJob(pid, job)
	jobAssigned = true
	return pid, nil
}

func inheritableManagedAgentWriterHandle(writer io.Writer) (windows.Handle, func(), error) {
	file, ok := writer.(*os.File)
	if !ok || file == nil {
		return 0, func() {}, fmt.Errorf("writer is %T, want *os.File", writer)
	}
	currentProcess := windows.CurrentProcess()
	var duplicated windows.Handle
	if err := windows.DuplicateHandle(currentProcess, windows.Handle(file.Fd()), currentProcess, &duplicated, 0, true, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return 0, func() {}, err
	}
	return duplicated, func() { _ = windows.CloseHandle(duplicated) }, nil
}

func windowsEnvironmentBlock(env []string) ([]uint16, error) {
	if env == nil {
		env = os.Environ()
	}
	block := make([]uint16, 0, 4096)
	for _, item := range env {
		if strings.ContainsRune(item, 0) {
			return nil, fmt.Errorf("environment contains NUL")
		}
		block = append(block, utf16.Encode([]rune(item))...)
		block = append(block, 0)
	}
	block = append(block, 0)
	return block, nil
}

func newManagedAgentWindowsJob(killOnClose bool) (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("create managed agent job object: %w", err)
	}
	if killOnClose {
		info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
		info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
		if _, err := windows.SetInformationJobObject(
			job,
			windows.JobObjectExtendedLimitInformation,
			uintptr(unsafe.Pointer(&info)),
			uint32(unsafe.Sizeof(info)),
		); err != nil {
			_ = windows.CloseHandle(job)
			return 0, fmt.Errorf("configure managed agent job object: %w", err)
		}
	}
	return job, nil
}

func rememberManagedAgentWindowsJob(pid int, job windows.Handle) {
	if pid <= 0 || job == 0 {
		return
	}
	managedAgentWindowsJobs.mu.Lock()
	defer managedAgentWindowsJobs.mu.Unlock()
	managedAgentWindowsJobs.jobs[pid] = job
}

func takeManagedAgentWindowsJob(pid int) (windows.Handle, bool) {
	if pid <= 0 {
		return 0, false
	}
	managedAgentWindowsJobs.mu.Lock()
	defer managedAgentWindowsJobs.mu.Unlock()
	job, ok := managedAgentWindowsJobs.jobs[pid]
	if ok {
		delete(managedAgentWindowsJobs.jobs, pid)
	}
	return job, ok
}

func hasManagedAgentWindowsJob(pid int) bool {
	if pid <= 0 {
		return false
	}
	managedAgentWindowsJobs.mu.Lock()
	defer managedAgentWindowsJobs.mu.Unlock()
	_, ok := managedAgentWindowsJobs.jobs[pid]
	return ok
}

func managedAgentProcessTreeKnown(pid int) bool {
	return hasManagedAgentWindowsJob(pid)
}

func releaseManagedAgentProcessResources(pid int) {
	if job, ok := takeManagedAgentWindowsJob(pid); ok {
		_ = windows.CloseHandle(job)
	}
}

func cleanupManagedAgentWorkdirProcesses(workdir string) (string, error) {
	return cleanupWindowsCommandLineProcessesForRoots(managedAgentWorkdirProcessCleanupRoots(workdir))
}

func processExists(pid int) (bool, error) {
	cmd := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH")
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	line := strings.TrimSpace(string(bytes.TrimSpace(out)))
	if line == "" || strings.Contains(line, "No tasks are running") {
		return false, nil
	}
	return strings.Contains(line, fmt.Sprintf(",\"%d\"", pid)), nil
}

func windowsManagedAgentKillAllowed(pid int) (bool, string) {
	commandLine, err := processCommandLine(pid)
	if err != nil {
		return false, fmt.Sprintf("command-line inspection failed: %v", err)
	}
	if strings.TrimSpace(commandLine) == "" {
		return false, "empty command line"
	}
	if !managedAgentDaemonCommandLineLooksKillable(commandLine) {
		return false, "process is not a rhizome-bot daemon with managed-agent flags"
	}
	return true, ""
}

func killProcess(pid int) error {
	knownManagedJob := hasManagedAgentWindowsJob(pid)
	var job windows.Handle
	if knownManagedJob {
		if knownJob, ok := takeManagedAgentWindowsJob(pid); ok {
			job = knownJob
			defer windows.CloseHandle(job)
		}
	}
	if !knownManagedJob {
		if ok, reason := windowsManagedAgentKillAllowed(pid); !ok {
			return fmt.Errorf("refusing to kill pid %d: %s", pid, reason)
		}
	}
	cmd := exec.Command("taskkill", "/PID", fmt.Sprintf("%d", pid), "/T", "/F")
	var taskkillErr error
	if out, err := cmd.CombinedOutput(); err != nil {
		trimmed := strings.TrimSpace(string(out))
		lower := strings.ToLower(trimmed)
		if strings.Contains(lower, "not found") || strings.Contains(lower, "not running") || strings.Contains(lower, "no tasks are running") {
			taskkillErr = nil
		} else {
			taskkillErr = fmt.Errorf("taskkill process tree: %w: %s", err, trimmed)
		}
	}
	if job != 0 {
		if err := windows.TerminateJobObject(job, 1); err != nil && taskkillErr != nil {
			return fmt.Errorf("%v; terminate managed job object: %w", taskkillErr, err)
		}
		return nil
	}
	if taskkillErr != nil {
		process, openErr := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid))
		if openErr != nil {
			return taskkillErr
		}
		defer windows.CloseHandle(process)
		if terminateErr := windows.TerminateProcess(process, 1); terminateErr != nil {
			return terminateErr
		}
	}
	releaseManagedAgentProcessResources(pid)
	return nil
}
