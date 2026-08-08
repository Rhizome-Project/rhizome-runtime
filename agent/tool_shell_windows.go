//go:build windows

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

type shellCommandProcessHandle struct {
	pid int
	job windows.Handle
}

var windowsCommandLineCleanupMu sync.Mutex

func configureShellCommandProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
}

func attachShellCommandProcessTree(cmd *exec.Cmd) (*shellCommandProcessHandle, error) {
	if cmd == nil || cmd.Process == nil {
		return nil, fmt.Errorf("shell process not started")
	}
	job, err := newManagedAgentWindowsJob(true)
	if err != nil {
		return nil, err
	}
	pid := cmd.Process.Pid
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("open shell process for job assignment: %w", err)
	}
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		_ = windows.CloseHandle(process)
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("assign shell process to job object: %w", err)
	}
	_ = windows.CloseHandle(process)
	return &shellCommandProcessHandle{pid: pid, job: job}, nil
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
	if h.job != 0 {
		job := h.job
		h.job = 0
		if err := windows.CloseHandle(job); err == nil {
			return nil
		}
	}
	return killShellCommandProcessTreeByPID(h.pid)
}

func (h *shellCommandProcessHandle) release() {
	if h == nil || h.job == 0 {
		return
	}
	job := h.job
	h.job = 0
	_ = windows.CloseHandle(job)
}

func killShellCommandProcessTreeByPID(pid int) error {
	if pid <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "taskkill", "/PID", fmt.Sprintf("%d", pid), "/T", "/F")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return fmt.Errorf("taskkill shell process tree timed out: %w", ctx.Err())
	}
	if err == nil {
		return nil
	}
	trimmed := strings.TrimSpace(string(out))
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, "not found") || strings.Contains(lower, "not running") || strings.Contains(lower, "no tasks are running") {
		return nil
	}
	process, openErr := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid))
	if openErr != nil {
		return fmt.Errorf("taskkill shell process tree: %w: %s", err, trimmed)
	}
	defer windows.CloseHandle(process)
	return windows.TerminateProcess(process, 1)
}

func cleanupShellCommandWorkdirProcesses(workdir string) (string, error) {
	return cleanupWindowsCommandLineProcessesForRoots(shellWorkdirProcessCleanupRoots(workdir))
}

func cleanupWindowsCommandLineProcessesForRoots(roots []string) (string, error) {
	if len(roots) == 0 {
		return "", nil
	}
	rootsJSON, err := json.Marshal(roots)
	if err != nil {
		return "", fmt.Errorf("encode workdir cleanup roots: %w", err)
	}
	script := `
$rootsJson = [Environment]::GetEnvironmentVariable('RHIZOME_AGENT_SHELL_CLEANUP_ROOTS_JSON')
if ([string]::IsNullOrWhiteSpace($rootsJson)) { exit 0 }
$parsedRoots = ConvertFrom-Json -InputObject $rootsJson
$roots = @()
foreach ($rootItem in @($parsedRoots)) {
  if ($null -eq $rootItem) { continue }
  if ($rootItem -is [string]) {
    $roots += [string]$rootItem
    continue
  }
  $valueProperty = $rootItem.PSObject.Properties['value']
  if ($null -ne $valueProperty) {
    foreach ($nestedRoot in @($valueProperty.Value)) {
      if (-not [string]::IsNullOrWhiteSpace([string]$nestedRoot)) {
        $roots += [string]$nestedRoot
      }
    }
    continue
  }
  $roots += [string]$rootItem
}
if ($roots.Count -eq 0) { exit 0 }
$processes = @(Get-CimInstance Win32_Process)
$denyNames = @(
  'explorer.exe',
  'codex.exe',
  'code.exe',
  'powershell.exe',
  'pwsh.exe',
  'cmd.exe',
  'conhost.exe',
  'wt.exe',
  'windows_terminal.exe',
  'windowsterminal.exe',
  'openconsole.exe',
  'rhizome-bot.exe'
)
function Test-RhizomeCleanupDeniedProcess($p) {
  if ($null -eq $p) { return $true }
  if ($p.ProcessId -eq $PID -or $p.ProcessId -le 4) { return $true }
  $name = ([string]$p.Name).ToLowerInvariant()
  return $denyNames -contains $name
}
$targetByPid = @{}
foreach ($root in $roots) {
  if ([string]::IsNullOrWhiteSpace([string]$root)) { continue }
  try {
    $needle = [System.IO.Path]::GetFullPath([string]$root).TrimEnd('\','/')
  } catch {
    $errors += ("invalid cleanup root {0}: {1}" -f ([string]$root), $_.Exception.Message)
    continue
  }
  $patterns = @("*$needle\*", "*$needle/*")
  foreach ($p in $processes) {
    if ($p.ProcessId -eq $PID) { continue }
    if (Test-RhizomeCleanupDeniedProcess $p) { continue }
    $cmdLine = [string]$p.CommandLine
    $exePath = [string]$p.ExecutablePath
    if ($cmdLine -like $patterns[0] -or $cmdLine -like $patterns[1] -or $exePath -like $patterns[0] -or $exePath -like $patterns[1]) {
      $targetByPid[[string]$p.ProcessId] = $p
    }
  }
}
$changed = $true
while ($changed) {
  $changed = $false
  foreach ($p in $processes) {
    if ($p.ProcessId -eq $PID) { continue }
    if (Test-RhizomeCleanupDeniedProcess $p) { continue }
    if ($targetByPid.ContainsKey([string]$p.ProcessId)) { continue }
    if ($p.ParentProcessId -and $targetByPid.ContainsKey([string]$p.ParentProcessId)) {
      $targetByPid[[string]$p.ProcessId] = $p
      $changed = $true
    }
  }
}
$targets = @($targetByPid.Values)
$killed = @()
$errors = @()
$skipped = @()
foreach ($p in $targets) {
  if (Test-RhizomeCleanupDeniedProcess $p) {
    $skipped += [int]$p.ProcessId
    continue
  }
  try {
    Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
    $killed += [int]$p.ProcessId
  } catch {
    $errors += ("{0}: {1}" -f $p.ProcessId, $_.Exception.Message)
  }
}
if ($targets.Count -gt 0 -or $errors.Count -gt 0) {
  [pscustomobject]@{
    eligible = $true
    roots = $roots
    matched_pids = @($targets | ForEach-Object { [int]$_.ProcessId })
    killed_pids = $killed
    skipped_pids = $skipped
    errors = $errors
  } | ConvertTo-Json -Compress
}
`
	windowsCommandLineCleanupMu.Lock()
	defer windowsCommandLineCleanupMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	cmd.Env = append(os.Environ(), "RHIZOME_AGENT_SHELL_CLEANUP_ROOTS_JSON="+string(rootsJSON))
	out, err := cmd.CombinedOutput()
	trimmed := strings.TrimSpace(string(out))
	if ctx.Err() != nil {
		return trimmed, fmt.Errorf("workdir process cleanup timed out: %w", ctx.Err())
	}
	if err != nil {
		if trimmed == "" {
			return "", fmt.Errorf("workdir process cleanup failed: %w", err)
		}
		return trimmed, fmt.Errorf("workdir process cleanup failed: %w", err)
	}
	return trimmed, nil
}
