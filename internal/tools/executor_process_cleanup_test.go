package tools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExecutorCallTimeoutKillsSpawnedDescendants(t *testing.T) {
	executor := NewExecutor(t.TempDir())
	workspaceID := "ws-desc-timeout"
	toolID := "tool-desc-timeout"
	pidFile := filepath.Join(t.TempDir(), "timeout-child.pid")
	deployDescendantSpawnerTool(t, executor, workspaceID, toolID)

	type outcome struct {
		result *CallResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := executor.Call(context.Background(), CallInput{
			WorkspaceID: workspaceID,
			ToolID:      toolID,
			Arguments: map[string]any{
				"pid_file":        pidFile,
				"child_sleep_sec": 30,
				"sleep_sec":       30,
				"spawn_child":     true,
			},
			TimeoutSec: 5,
		})
		done <- outcome{result: result, err: err}
	}()

	pid := waitForPIDFile(t, pidFile)
	out := <-done
	if out.err != nil {
		t.Fatalf("expected timeout result, got error: %v", out.err)
	}
	if out.result == nil || !out.result.TimedOut || out.result.ExitCode != -1 {
		t.Fatalf("expected timed out result, got %+v", out.result)
	}
	waitForProcessExit(t, pid, 5*time.Second)
}

func TestExecutorCallCancelKillsSpawnedDescendants(t *testing.T) {
	executor := NewExecutor(t.TempDir())
	workspaceID := "ws-desc-cancel"
	toolID := "tool-desc-cancel"
	pidFile := filepath.Join(t.TempDir(), "cancel-child.pid")
	deployDescendantSpawnerTool(t, executor, workspaceID, toolID)

	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		result *CallResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := executor.Call(ctx, CallInput{
			WorkspaceID: workspaceID,
			ToolID:      toolID,
			Arguments: map[string]any{
				"pid_file":        pidFile,
				"child_sleep_sec": 30,
				"sleep_sec":       30,
				"spawn_child":     true,
			},
		})
		done <- outcome{result: result, err: err}
	}()

	pid := waitForPIDFile(t, pidFile)
	cancel()
	out := <-done
	if out.result != nil {
		t.Fatalf("expected nil result on cancel, got %+v", out.result)
	}
	if !errors.Is(out.err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", out.err)
	}
	waitForProcessExit(t, pid, 5*time.Second)
}

func deployDescendantSpawnerTool(t *testing.T, executor *Executor, workspaceID, toolID string) {
	t.Helper()
	installNativeTestTool(t, executor, workspaceID, toolID)
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for child pid file %s", path)
	return 0
}

func waitForProcessExit(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		alive, err := isProcessAlive(pid)
		if err != nil {
			t.Fatalf("check child process %d: %v", pid, err)
		}
		if !alive {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("process %d stayed alive after tool.call termination window", pid)
}
