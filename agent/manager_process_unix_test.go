//go:build !windows

package main

import (
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestManagedAgentIgnoreSIGTERMHelper(t *testing.T) {
	if os.Getenv("RHIZOME_AGENT_IGNORE_SIGTERM_HELPER") != "1" {
		return
	}
	signalIgnored := make(chan os.Signal, 1)
	signal.Notify(signalIgnored, syscall.SIGTERM)
	for range signalIgnored {
	}
}

func TestStopManagedAgentEscalatesSIGTERMToSIGKILLOnUnix(t *testing.T) {
	if testing.Short() {
		t.Skip("process signal integration test is skipped in short mode")
	}
	workdir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error: %v", err)
	}
	rhizomeBot := filepath.Join(workdir, "rhizome-bot")
	raw, err := os.ReadFile(executable)
	if err != nil {
		t.Fatalf("read test executable: %v", err)
	}
	if err := os.WriteFile(rhizomeBot, raw, 0o755); err != nil {
		t.Fatalf("copy test executable: %v", err)
	}
	stdout, err := os.OpenFile(filepath.Join(workdir, "agent.out.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open stdout: %v", err)
	}
	defer stdout.Close()
	stderr, err := os.OpenFile(filepath.Join(workdir, "agent.err.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open stderr: %v", err)
	}
	defer stderr.Close()

	record := ManagedAgentRecord{AgentID: "lyrica", Workdir: workdir}
	pid, err := startDetachedProcess(
		rhizomeBot,
		[]string{"-test.run=TestManagedAgentIgnoreSIGTERMHelper", "daemon", "--workdir", workdir, "--agent-id", "lyrica"},
		workdir,
		append(os.Environ(), "RHIZOME_AGENT_IGNORE_SIGTERM_HELPER=1"),
		stdout,
		stderr,
	)
	if err != nil {
		t.Fatalf("start SIGTERM-ignoring helper: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: pid, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()
	managedAgentStopExitTimeout = 40 * time.Millisecond
	managedAgentProcessExitPollGap = 5 * time.Millisecond

	if err := StopManagedAgent(record); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected process state to be removed after SIGKILL escalation, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(managedAgentStopRequestPath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected stop request to be cleared after SIGKILL escalation, stat err=%v", statErr)
	}
	if ok, err := processExists(pid); err != nil {
		t.Fatalf("processExists(%d) error: %v", pid, err)
	} else if ok {
		t.Fatalf("expected pid %d to exit after SIGKILL escalation", pid)
	}
}
