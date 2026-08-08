package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestManagedAgentProcessTreeHelper(t *testing.T) {
	if os.Getenv("RHIZOME_AGENT_PROCESS_TREE_HELPER") != "1" {
		return
	}
	marker := os.Getenv("RHIZOME_AGENT_PROCESS_TREE_MARKER")
	executable, err := os.Executable()
	if err != nil {
		os.Exit(2)
	}
	cmd := exec.Command(executable, "-test.run=TestManagedAgentProcessTreeChild")
	cmd.Env = append(os.Environ(), "RHIZOME_AGENT_PROCESS_TREE_CHILD=1", "RHIZOME_AGENT_PROCESS_TREE_MARKER="+marker)
	if err := cmd.Start(); err != nil {
		os.Exit(2)
	}
	_ = cmd.Process.Release()
	time.Sleep(30 * time.Second)
	os.Exit(0)
}

func TestOpenProcessLogRestrictsExistingFileOnPOSIX(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}
	logDir := filepath.Join(t.TempDir(), agentProcessLogDirname)
	if err := os.Mkdir(logDir, 0o755); err != nil {
		t.Fatalf("seed log directory: %v", err)
	}
	path := filepath.Join(logDir, "agent.out.log")
	if err := os.WriteFile(path, []byte("existing\n"), 0o644); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	file, err := openProcessLog(path)
	if err != nil {
		t.Fatalf("open process log: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close process log: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat process log: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("process log mode = %o, want 600", got)
	}
	info, err = os.Stat(logDir)
	if err != nil {
		t.Fatalf("stat process log directory: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("process log directory mode = %o, want 700", got)
	}
}

func TestManagedAgentProcessTreeChild(t *testing.T) {
	if os.Getenv("RHIZOME_AGENT_PROCESS_TREE_CHILD") != "1" {
		return
	}
	marker := os.Getenv("RHIZOME_AGENT_PROCESS_TREE_MARKER")
	time.Sleep(1200 * time.Millisecond)
	_ = os.WriteFile(marker, []byte("escaped child"), 0o600)
	os.Exit(0)
}

func TestManagedAgentProcessCleanupRemovesPidlessState(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     0,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	changed, err := CleanupStaleManagedAgentProcessState(record)
	if err != nil {
		t.Fatalf("CleanupStaleManagedAgentProcessState() error: %v", err)
	}
	if !changed {
		t.Fatal("expected stale state to be removed")
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected process state to be removed, stat err=%v", err)
	}
	if got := ManagedAgentRuntimeStatus(record); got != "stopped" {
		t.Fatalf("expected stopped status after cleanup, got %q", got)
	}
}

func TestInspectManagedAgentProcessTreatsLookupErrorsAsUnknownNotStale(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	defer func() { managedAgentProcessExistsFunc = origExists }()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, errors.New("ps unavailable")
	}

	status := InspectManagedAgentProcess(record)
	if status.State != "unknown" {
		t.Fatalf("expected unknown state, got %+v", status)
	}
	if status.Stale {
		t.Fatalf("expected transient inspection failure to stay non-stale, got %+v", status)
	}
	if status.Running {
		t.Fatalf("expected transient inspection failure to not report running, got %+v", status)
	}
	if status.PID != 1234 {
		t.Fatalf("expected pid to remain visible, got %+v", status)
	}
}

func TestInspectManagedAgentProcessTreatsPidReuseAsStale(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		if pid != 1234 {
			t.Fatalf("expected lookup on pid 1234, got %d", pid)
		}
		return true, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		if pid != 1234 || gotRecord.AgentID != "lyrica" {
			t.Fatalf("unexpected identity check pid=%d record=%+v", pid, gotRecord)
		}
		return false, nil
	}

	status := InspectManagedAgentProcess(record)
	if status.State != "stale" || !status.Stale || status.Running {
		t.Fatalf("expected pid reuse to report stale, got %+v", status)
	}
}

func TestInspectManagedAgentProcessTreatsExecutableHashDriftAsStale(t *testing.T) {
	workdir := t.TempDir()
	executable := filepath.Join(workdir, "rhizome-bot-test.exe")
	if err := os.WriteFile(executable, []byte("original binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(original executable) error: %v", err)
	}
	digest, err := managerFileSHA256(executable)
	if err != nil {
		t.Fatalf("managerFileSHA256() error: %v", err)
	}
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:                 1234,
		Executable:          executable,
		ExecutableSHA256:    digest,
		Mode:                string(RuntimeModeDaemon),
		Workdir:             workdir,
		Args:                managedAgentDaemonArgs(record),
		ArgsDigest:          managedAgentArgsDigest(managedAgentDaemonArgs(record)),
		RuntimeConfigDigest: managedAgentRuntimeConfigDigest(managedAgentStartRuntimeConfig(record)),
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}
	if err := os.WriteFile(executable, []byte("replacement binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(replacement executable) error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 1234, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	status := InspectManagedAgentProcess(record)
	if status.State != "stale" || !status.Stale || status.Running {
		t.Fatalf("expected executable hash drift to report stale, got %+v", status)
	}
	if !containsTrimmedString(status.DriftReasons, "executable_hash_drift") {
		t.Fatalf("expected executable hash drift reason, got %+v", status.DriftReasons)
	}
}

func TestInspectManagedAgentProcessTreatsLaunchArgsDriftAsStale(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	executable := filepath.Join(workdir, "rhizome-bot.exe")
	if err := os.WriteFile(executable, []byte("binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(executable) error: %v", err)
	}
	digest, err := managerFileSHA256(executable)
	if err != nil {
		t.Fatalf("managerFileSHA256() error: %v", err)
	}
	staleArgs := []string{"daemon", "--workdir", workdir, "--workspace-id", "old-workspace", "--agent-id", "lyrica"}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:                 1234,
		Executable:          executable,
		ExecutableSHA256:    digest,
		Mode:                string(RuntimeModeDaemon),
		Workdir:             workdir,
		Args:                staleArgs,
		ArgsDigest:          managedAgentArgsDigest(staleArgs),
		RuntimeConfigDigest: managedAgentRuntimeConfigDigest(managedAgentStartRuntimeConfig(record)),
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 1234, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	status := InspectManagedAgentProcess(record)
	if status.State != "stale" || !status.Stale || status.Running {
		t.Fatalf("expected launch args drift to report stale, got %+v", status)
	}
	if !containsTrimmedString(status.DriftReasons, "launch_args_drift") {
		t.Fatalf("expected launch args drift reason, got %+v", status.DriftReasons)
	}
}

func TestInspectManagedAgentProcessTreatsRuntimeConfigDriftAsStale(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	executable := filepath.Join(workdir, "rhizome-bot.exe")
	if err := os.WriteFile(executable, []byte("binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(executable) error: %v", err)
	}
	digest, err := managerFileSHA256(executable)
	if err != nil {
		t.Fatalf("managerFileSHA256() error: %v", err)
	}
	staleConfig := managedAgentStartRuntimeConfig(record)
	staleConfig.WorkspaceID = "old-workspace"
	args := managedAgentDaemonArgs(record)
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:                 1234,
		Executable:          executable,
		ExecutableSHA256:    digest,
		Mode:                string(RuntimeModeDaemon),
		Workdir:             workdir,
		Args:                args,
		ArgsDigest:          managedAgentArgsDigest(args),
		RuntimeConfigDigest: managedAgentRuntimeConfigDigest(staleConfig),
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 1234, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	status := InspectManagedAgentProcess(record)
	if status.State != "stale" || !status.Stale || status.Running {
		t.Fatalf("expected runtime config drift to report stale, got %+v", status)
	}
	if !containsTrimmedString(status.DriftReasons, "runtime_config_drift") {
		t.Fatalf("expected runtime config drift reason, got %+v", status.DriftReasons)
	}
}

func TestInspectManagedAgentProcessIgnoresPostStartRuntimeProfileMaterialization(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	capabilities := []string{"tool.call", "local.shell"}
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			Capabilities: capabilities,
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:   "codex",
			Title:        "Codex",
			ChannelType:  providerChannelBridge,
			Driver:       llmBackendCodex,
			GroupID:      "codex",
			DefaultModel: "gpt-5.4",
			Enabled:      true,
			CreatedAt:    "2026-05-27T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:          "alpha",
		DisplayName:      "Alpha",
		WorkspaceID:      "rhizome-main",
		Workdir:          workdir,
		OwnerUserID:      "owner-1",
		ProviderID:       "codex",
		GroupID:          "codex",
		LLMBackend:       llmBackendCodex,
		Model:            "gpt-5.4",
		CoordinationMode: CoordinationModeTrustFirst,
		Role:             "coordinator",
	}
	if err := SaveAgentProcessState(workdir, trustedAgentProcessStateForTest(t, record, 1234)); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		Mode:                     string(RuntimeModeDaemon),
		ProtocolVersion:          defaultProtocolVersion,
		ProviderID:               "codex",
		GroupID:                  "codex",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4",
		RealLLMPilot:             true,
		CoordinationMode:         CoordinationModeTrustFirst,
		RPCEndpoint:              "https://rhizome.example.test/rpc",
		HostURL:                  "https://rhizome.example.test",
		WorkspaceID:              "rhizome-main",
		WorkspaceName:            "Rhizome Main Clean Run",
		WorkspacePassword:        "runtime-password",
		AgentID:                  "alpha",
		DisplayName:              "Alpha",
		AgentToken:               "runtime-token",
		OwnerUserID:              "owner-1",
		Role:                     "coordinator",
		Capabilities:             capabilities,
		MaxProviderRetryAttempts: 2,
		ProviderCallTimeoutSec:   180,
		RegisteredExecutor: RegisteredExecutorIdentity{
			AgentID:         "alpha",
			WorkspaceID:     "rhizome-main",
			DisplayName:     "Alpha",
			OwnerUserID:     "owner-1",
			Role:            "coordinator",
			Status:          "ACTIVE",
			ProtocolVersion: defaultProtocolVersion,
			Capabilities:    capabilities,
			ConfirmedAt:     "2026-05-27T00:00:01Z",
		},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 1234, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	status := InspectManagedAgentProcess(record)
	if status.State != "running" || !status.Running || status.Stale {
		t.Fatalf("expected post-start runtime profile materialization to remain running, got %+v", status)
	}
	if containsTrimmedString(status.DriftReasons, "runtime_config_drift") {
		t.Fatalf("post-start runtime profile materialization must not report runtime_config_drift, got %+v", status.DriftReasons)
	}
}

func TestInspectManagedAgentProcessWithSnapshotCommandLineErrorIsUnknown(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	status := InspectManagedAgentProcessWithSnapshot(record, map[int]managedAgentProcessProbe{
		1234: {
			PID:            1234,
			Exists:         true,
			CommandLineErr: errors.New("command-line inspection unavailable"),
		},
	})
	if status.State != "unknown" {
		t.Fatalf("expected command-line inspection error to report unknown, got %+v", status)
	}
	if status.Running || status.Stale {
		t.Fatalf("expected unknown status to avoid running/stale claims, got %+v", status)
	}
}

func TestInspectManagedAgentProcessWithSnapshotEmptyCommandLineIsUnknown(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	status := InspectManagedAgentProcessWithSnapshot(record, map[int]managedAgentProcessProbe{
		1234: {
			PID:         1234,
			Exists:      true,
			CommandLine: "  ",
		},
	})
	if status.State != "unknown" {
		t.Fatalf("expected empty command-line snapshot to report unknown, got %+v", status)
	}
	if status.Running || status.Stale {
		t.Fatalf("expected unknown status to avoid running/stale claims, got %+v", status)
	}
}

func TestInspectManagedAgentProcessWithLegacyMissingProvenanceIsUnknown(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	args := managedAgentDaemonArgs(record)
	status := InspectManagedAgentProcessWithSnapshot(record, map[int]managedAgentProcessProbe{
		1234: {
			PID:         1234,
			Exists:      true,
			CommandLine: "rhizome-bot " + strings.Join(args, " "),
		},
	})
	if status.State != "unknown" || status.Running || status.Stale {
		t.Fatalf("expected legacy process state without provenance to report unknown, got %+v", status)
	}
	for _, want := range []string{"missing_executable_identity", "missing_launch_args_identity", "missing_runtime_config_identity"} {
		if !containsTrimmedString(status.DriftReasons, want) {
			t.Fatalf("expected missing provenance reason %q, got %+v", want, status.DriftReasons)
		}
	}
}

func TestStopManagedAgentRemovesStateAfterSuccessfulExit(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		switch callCount {
		case 1:
			return true, nil
		default:
			return false, nil
		}
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	if err := StopManagedAgent(record); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	if killedPID != 0 {
		t.Fatalf("expected graceful stop without force kill, got killed pid %d", killedPID)
	}
	if _, err := os.Stat(managedAgentStopRequestPath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected graceful stop request to be cleared, stat err=%v", err)
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected process state to be removed, stat err=%v", err)
	}
}

func TestStopManagedAgentClearsStateBeforeReturningCleanupErrorAfterExit(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origCleanup := managedAgentCleanupWorkdirFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentCleanupWorkdirFunc = origCleanup
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		return callCount == 1, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentCleanupWorkdirFunc = func(gotWorkdir string) (string, error) {
		return "", errors.New("cleanup warning")
	}
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	err := StopManagedAgent(record)
	if err == nil || !strings.Contains(err.Error(), "cleanup warning") {
		t.Fatalf("expected cleanup warning after confirmed exit, got %v", err)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected process state to be removed before cleanup warning, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(managedAgentStopRequestPath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected stop request to be cleared before cleanup warning, stat err=%v", statErr)
	}
	status := InspectManagedAgentProcess(record)
	if status.State != "stopped" || status.Running {
		t.Fatalf("expected cleanup warning not to leave process active, got %+v", status)
	}
}

func TestStopManagedAgentDropsMismatchedLivePIDWithoutKill(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origCleanup := managedAgentCleanupWorkdirFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentCleanupWorkdirFunc = origCleanup
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		if pid != 1234 {
			t.Fatalf("expected lookup on pid 1234, got %d", pid)
		}
		return true, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		if pid != 1234 {
			t.Fatalf("expected identity check on pid 1234, got %d", pid)
		}
		return false, nil
	}
	managedAgentKillProcessFunc = func(pid int) error {
		t.Fatalf("mismatched live pid must not be killed, got %d", pid)
		return nil
	}
	managedAgentCleanupWorkdirFunc = func(gotWorkdir string) (string, error) {
		t.Fatalf("mismatched live pid must not trigger residual cleanup, got %q", gotWorkdir)
		return "", nil
	}

	if err := StopManagedAgent(record); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected process state to be removed, stat err=%v", err)
	}
}

func TestStopManagedAgentRequestsGracefulStopBeforeForceKill(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origCleanup := managedAgentCleanupWorkdirFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	t.Setenv("RHIZOME_MANAGED_AGENT_ALLOW_FORCE_STOP", "")
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentCleanupWorkdirFunc = origCleanup
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		return callCount == 1, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentKillProcessFunc = func(pid int) error {
		t.Fatalf("graceful stop path must not force-kill pid %d", pid)
		return nil
	}
	managedAgentCleanupWorkdirFunc = func(gotWorkdir string) (string, error) {
		if gotWorkdir != workdir {
			t.Fatalf("expected cleanup workdir %q, got %q", workdir, gotWorkdir)
		}
		return "", nil
	}
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	if err := StopManagedAgent(record); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	if _, err := os.Stat(managedAgentStopRequestPath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected graceful stop request to be cleared, stat err=%v", err)
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected process state to be removed, stat err=%v", err)
	}
}

func TestCleanupStaleManagedAgentProcessStateDropsMismatchedLivePID(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return true, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return false, nil
	}

	changed, err := CleanupStaleManagedAgentProcessState(record)
	if err != nil {
		t.Fatalf("CleanupStaleManagedAgentProcessState() error: %v", err)
	}
	if !changed {
		t.Fatal("expected mismatched live pid state to be removed")
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected process state to be removed, stat err=%v", err)
	}
}

func TestManagedAgentCommandLineMatchesRecord(t *testing.T) {
	workdir := filepath.Join(t.TempDir(), "agent work")
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	commandLine := `"C:\Tools\rhizome-bot.exe" daemon --workdir "` + workdir + `" --workspace-id rhizome-main --agent-id lyrica`
	state := AgentProcessState{PID: 1234, Workdir: workdir}

	if !managedAgentCommandLineMatchesRecord(commandLine, state, record) {
		t.Fatalf("expected managed daemon command line to match record")
	}
	if managedAgentCommandLineMatchesRecord(strings.Replace(commandLine, "--agent-id lyrica", "--agent-id beta", 1), state, record) {
		t.Fatalf("expected wrong agent id to be rejected")
	}
	if managedAgentCommandLineMatchesRecord(`C:\Windows\explorer.exe`, state, record) {
		t.Fatalf("expected non-daemon process to be rejected")
	}
	if managedAgentDaemonCommandLineLooksKillable(`"C:\Users\developer\AppData\Local\Programs\Microsoft VS Code\Code.exe" --workdir "` + workdir + `" --agent-id lyrica`) {
		t.Fatalf("expected app process with coincidental flags to be rejected")
	}
}

func TestStopManagedAgentRunsWorkdirCleanupAfterSuccessfulExit(t *testing.T) {
	workdir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workdir, "project-checkouts", "demo"), 0o755); err != nil {
		t.Fatalf("mkdir project checkout: %v", err)
	}
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origCleanup := managedAgentCleanupWorkdirFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentCleanupWorkdirFunc = origCleanup
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		switch callCount {
		case 1:
			return true, nil
		default:
			return false, nil
		}
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentKillProcessFunc = func(pid int) error {
		if pid != 1234 {
			t.Fatalf("expected kill on pid 1234, got %d", pid)
		}
		return nil
	}
	cleanupCalled := false
	managedAgentCleanupWorkdirFunc = func(gotWorkdir string) (string, error) {
		cleanupCalled = true
		if gotWorkdir != workdir {
			t.Fatalf("expected cleanup workdir %q, got %q", workdir, gotWorkdir)
		}
		return "matched_pids=[]", nil
	}
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	if err := StopManagedAgent(record); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	if !cleanupCalled {
		t.Fatal("expected stop to run managed workdir residual cleanup")
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected process state to be removed, stat err=%v", err)
	}
}

func TestStopManagedAgentTerminatesDetachedProcessTree(t *testing.T) {
	if testing.Short() {
		t.Skip("process-tree integration test is skipped in short mode")
	}
	origMatches := managedAgentProcessMatchesFunc
	managedAgentProcessMatchesFunc = func(int, AgentProcessState, ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	defer func() { managedAgentProcessMatchesFunc = origMatches }()

	workdir := t.TempDir()
	marker := filepath.Join(workdir, "escaped-child.txt")
	stdout, err := os.OpenFile(filepath.Join(workdir, "tree.out.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open stdout: %v", err)
	}
	defer stdout.Close()
	stderr, err := os.OpenFile(filepath.Join(workdir, "tree.err.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open stderr: %v", err)
	}
	defer stderr.Close()

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error: %v", err)
	}
	pid, err := startDetachedProcess(
		executable,
		[]string{"-test.run=TestManagedAgentProcessTreeHelper"},
		workdir,
		append(os.Environ(), "RHIZOME_AGENT_PROCESS_TREE_HELPER=1", "RHIZOME_AGENT_PROCESS_TREE_MARKER="+marker),
		stdout,
		stderr,
	)
	if err != nil {
		t.Fatalf("start detached process tree: %v", err)
	}
	record := ManagedAgentRecord{AgentID: "tree-agent", Workdir: workdir}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: pid, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	if err := StopManagedAgent(record); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("expected process-tree stop to prevent escaped child marker, stat err=%v", err)
	}
}

func TestStopManagedAgentForceKillsAfterGracefulTimeoutByDefault(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	killedPID := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return killedPID == 0, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	if err := StopManagedAgent(record); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	if killedPID != 1234 {
		t.Fatalf("expected force stop on pid 1234 after graceful timeout, got %d", killedPID)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected process state to be removed after force stop, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(managedAgentStopRequestPath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected graceful stop request to be cleared after force stop, stat err=%v", statErr)
	}
}

func TestStopManagedAgentCanRefuseForceKillWhenExplicitlyDisabled(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	t.Setenv("RHIZOME_MANAGED_AGENT_ALLOW_FORCE_STOP", "0")
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return true, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentKillProcessFunc = func(pid int) error {
		t.Fatalf("force stop should be disabled, got kill pid %d", pid)
		return nil
	}
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	err := StopManagedAgent(record)
	if err == nil {
		t.Fatal("expected stop timeout error")
	}
	if !strings.Contains(err.Error(), "graceful stop requested") || !strings.Contains(err.Error(), "force stop disabled") {
		t.Fatalf("expected explicit graceful stop timeout refusal, got %v", err)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); statErr != nil {
		t.Fatalf("expected process state to remain on disabled force stop, stat err=%v", statErr)
	}
}

func TestStartManagedAgentRollsBackStartedProcessWhenStateSaveFails(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	startedPID := 4321
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		return startedPID, nil
	}
	managedAgentSaveStateFunc = func(workdir string, state AgentProcessState) error {
		return errors.New("disk full")
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	_, err := startManagedAgentWithoutPreflightForTest(record)
	if err == nil {
		t.Fatal("expected save-state failure")
	}
	if killedPID != startedPID {
		t.Fatalf("expected rollback kill on pid %d, got %d", startedPID, killedPID)
	}
	if !strings.Contains(err.Error(), "persist started agent lyrica state after starting pid 4321") || !strings.Contains(err.Error(), "rolled back started process") {
		t.Fatalf("expected explicit rollback error, got %v", err)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected no persisted process state after rollback, stat err=%v", statErr)
	}
}

func TestStartManagedAgentRollsBackWhenStartedProcessDiesBeforeProof(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "beta",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	origProofTimeout := managedAgentStartProofTimeout
	origProofGap := managedAgentStartProofPollGap
	origStopTimeout := managedAgentStopExitTimeout
	origExitGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
		managedAgentStartProofTimeout = origProofTimeout
		managedAgentStartProofPollGap = origProofGap
		managedAgentStopExitTimeout = origStopTimeout
		managedAgentProcessExitPollGap = origExitGap
	}()

	startedPID := 8844
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return false, nil }
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		return startedPID, nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStartProofTimeout = 5 * time.Millisecond
	managedAgentStartProofPollGap = time.Millisecond
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = time.Millisecond

	_, err := startManagedAgentWithoutPreflightForTest(record)
	if err == nil {
		t.Fatal("expected readiness proof failure")
	}
	if !strings.Contains(err.Error(), "failed readiness proof") {
		t.Fatalf("expected explicit readiness proof error, got %v", err)
	}
	if killedPID != startedPID {
		t.Fatalf("expected rollback kill on pid %d, got %d", startedPID, killedPID)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected failed start proof to remove process state, stat err=%v", statErr)
	}
}

func TestCleanupManagedAgentRuntimeIdentityLeaseBeforeStartReclaimsDeadLease(t *testing.T) {
	root := t.TempDir()
	workdir := filepath.Join(root, "beta")
	cfg := RuntimeConfig{
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		AgentID:     "beta",
		DisplayName: "Beta",
		Mode:        RuntimeModeDaemon,
	}
	cfg.ApplyDefaults()

	leaseRoot := filepath.Join(managedAgentIdentityLeaseRootPath(workdir), runtimeIdentityLeaseDirname)
	leasePath := runtimeIdentityLeasePathUnderRoot(leaseRoot, cfg.WorkspaceID, cfg.AgentID)
	if err := os.MkdirAll(filepath.Dir(leasePath), 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	stale := runtimeIdentityLeaseInfo{
		PID:         28444,
		StartedAt:   now,
		LastSeenAt:  now,
		Mode:        string(RuntimeModeDaemon),
		Workdir:     workdir,
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.AgentID,
		DisplayName: cfg.DisplayName,
	}
	raw, err := json.MarshalIndent(stale, "", "  ")
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	if err := os.WriteFile(leasePath, raw, 0o600); err != nil {
		t.Fatalf("WriteFile(lease) error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	defer func() { managedAgentProcessExistsFunc = origExists }()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return false, nil }

	changed, err := cleanupManagedAgentRuntimeIdentityLeaseBeforeStart(cfg)
	if err != nil {
		t.Fatalf("cleanupManagedAgentRuntimeIdentityLeaseBeforeStart() error = %v", err)
	}
	if !changed {
		t.Fatal("expected dead lease to be reclaimed")
	}
	if pathExists(leasePath) {
		t.Fatalf("expected lease path to be archived before start: %s", leasePath)
	}
	matches, globErr := filepath.Glob(leasePath + ".stale-*")
	if globErr != nil {
		t.Fatalf("Glob() error: %v", globErr)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived dead lease, got %v", matches)
	}
}

func TestStartManagedAgentPersistsProcessProvenance(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		return 4321, nil
	}
	var capturedState AgentProcessState
	managedAgentSaveStateFunc = func(workdir string, state AgentProcessState) error {
		capturedState = state
		return nil
	}

	state, err := startManagedAgentWithoutPreflightForTest(record)
	if err != nil {
		t.Fatalf("startManagedAgentWithoutPreflightForTest() error: %v", err)
	}
	if state.PID != 4321 || capturedState.PID != 4321 {
		t.Fatalf("expected persisted pid 4321, state=%+v captured=%+v", state, capturedState)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error: %v", err)
	}
	executableHash, err := managerFileSHA256(executable)
	if err != nil {
		t.Fatalf("managerFileSHA256() error: %v", err)
	}
	if capturedState.Executable != executable || capturedState.ExecutableSHA256 != executableHash {
		t.Fatalf("expected executable provenance %s/%s, got %+v", executable, executableHash, capturedState)
	}
	wantArgs := managedAgentDaemonArgs(record)
	if strings.Join(capturedState.Args, "\x00") != strings.Join(wantArgs, "\x00") || capturedState.ArgsDigest != managedAgentArgsDigest(wantArgs) {
		t.Fatalf("expected launch args provenance %v, got %+v", wantArgs, capturedState)
	}
	if capturedState.RuntimeConfigDigest != managedAgentRuntimeConfigDigest(managedAgentStartRuntimeConfig(record)) {
		t.Fatalf("expected runtime config digest provenance, got %+v", capturedState)
	}
}

func argsHaveFlagValue(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func argsHaveFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

func TestAppendManagedAgentRuntimeConfigArgsToolLoopCompaction(t *testing.T) {
	base := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "rhizome-main",
		AgentID:     "lyrica",
		Workdir:     t.TempDir(),
	}

	// Default off: no --tool-loop-compaction arg is emitted, preserving stock
	// behavior for managed launches when unset.
	off := base
	if got := appendManagedAgentRuntimeConfigArgs([]string{"daemon"}, off); argsHaveFlag(got, "--tool-loop-compaction") {
		t.Fatalf("expected no --tool-loop-compaction arg when disabled, got %v", got)
	}

	// Explicit on: the flag propagates to the spawned daemon command line.
	on := base
	on.ToolLoopCompaction = true
	if got := appendManagedAgentRuntimeConfigArgs([]string{"daemon"}, on); !argsHaveFlagValue(got, "--tool-loop-compaction", "true") {
		t.Fatalf("expected --tool-loop-compaction true arg when enabled, got %v", got)
	}
}

func TestStartManagedAgentWithPreflightUsesInstalledExecutable(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")

	current := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-current", "same binary")
	installed := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-installed", "same binary")
	installCleanManagerSubstrateStubsForTest(t, current, installed)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	var capturedExecutable string
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		capturedExecutable = executablePath
		return 4321, nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState

	state, err := StartManagedAgentWithPreflight(record)
	if err != nil {
		t.Fatalf("StartManagedAgentWithPreflight() error: %v", err)
	}
	if capturedExecutable != installed || state.Executable != installed {
		t.Fatalf("expected managed child to launch installed executable %q, captured=%q state=%+v", installed, capturedExecutable, state)
	}
	raw, err := os.ReadFile(managedRunSubstrateAdmissionReceiptPath(workdir))
	if err != nil {
		t.Fatalf("expected managed run preflight receipt: %v", err)
	}
	var receipt managedRunSubstrateAdmissionReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatalf("decode managed run preflight receipt: %v", err)
	}
	if receipt.Admission != "admitted" || receipt.ChildExecutablePath != installed {
		t.Fatalf("expected admitted receipt with installed executable %q, got %+v", installed, receipt)
	}
}

func TestStartManagedAgentWithOptionsThreadsResumeWaiver(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{AgentID: "lyrica", Workdir: workdir}
	childExecutable := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")

	origAdmit := admitManagedRunStartFunc
	origAdmitWithOptions := admitManagedRunStartWithOptionsFunc
	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		admitManagedRunStartFunc = origAdmit
		admitManagedRunStartWithOptionsFunc = origAdmitWithOptions
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()

	admitManagedRunStartFunc = func(record ManagedAgentRecord) (managedRunPreflightResult, error) {
		t.Fatalf("expected StartManagedAgentWithOptions to use options admission path")
		return managedRunPreflightResult{}, nil
	}
	var captured managedRunPreflightOptions
	admitManagedRunStartWithOptionsFunc = func(record ManagedAgentRecord, options managedRunPreflightOptions) (managedRunPreflightResult, error) {
		captured = options
		return managedRunPreflightResult{ChildExecutablePath: childExecutable}, nil
	}
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return pid == 4321, nil }
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		if executablePath != childExecutable {
			t.Fatalf("expected child executable %q, got %q", childExecutable, executablePath)
		}
		return 4321, nil
	}
	managedAgentSaveStateFunc = func(workdir string, state AgentProcessState) error { return nil }

	_, err := StartManagedAgentWithOptions(record, managedRunPreflightOptions{
		ResumeContinuationWaiver: managedRunFullResumeContinuationWaiver(),
	})
	if err != nil {
		t.Fatalf("StartManagedAgentWithOptions() error: %v", err)
	}
	waiver := captured.resumeContinuationWaiver()
	if !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
		t.Fatalf("expected resume waivers to reach admission, got %+v", captured)
	}
}

func TestRestartManagedAgentWithOptionsThreadsResumeWaiver(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{AgentID: "lyrica", Workdir: workdir}
	childExecutable := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")

	origAdmit := admitManagedRunStartFunc
	origAdmitWithOptions := admitManagedRunStartWithOptionsFunc
	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		admitManagedRunStartFunc = origAdmit
		admitManagedRunStartWithOptionsFunc = origAdmitWithOptions
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()

	admitManagedRunStartFunc = func(record ManagedAgentRecord) (managedRunPreflightResult, error) {
		t.Fatalf("expected RestartManagedAgentWithOptions to use options admission path")
		return managedRunPreflightResult{}, nil
	}
	var captured managedRunPreflightOptions
	admitManagedRunStartWithOptionsFunc = func(record ManagedAgentRecord, options managedRunPreflightOptions) (managedRunPreflightResult, error) {
		captured = options
		return managedRunPreflightResult{ChildExecutablePath: childExecutable}, nil
	}
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return pid == 4321, nil }
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		if executablePath != childExecutable {
			t.Fatalf("expected child executable %q, got %q", childExecutable, executablePath)
		}
		return 4321, nil
	}
	managedAgentSaveStateFunc = func(workdir string, state AgentProcessState) error { return nil }

	_, err := RestartManagedAgentWithOptions(record, managedRunPreflightOptions{
		ResumeContinuationWaiver: managedRunFullResumeContinuationWaiver(),
	})
	if err != nil {
		t.Fatalf("RestartManagedAgentWithOptions() error: %v", err)
	}
	waiver := captured.resumeContinuationWaiver()
	if !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
		t.Fatalf("expected resume waivers to reach restart admission, got %+v", captured)
	}
}

func TestStartManagedAgentRequiresManagedSubstrateAdmission(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	current := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-current", "current binary")
	installed := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-installed", "stale installed binary")
	installCleanManagerSubstrateStubsForTest(t, current, installed)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}

	origStart := managedAgentStartProcessFunc
	defer func() { managedAgentStartProcessFunc = origStart }()
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		t.Fatalf("StartManagedAgent must not launch without admitted substrate")
		return 0, nil
	}

	_, err := StartManagedAgent(record)
	if err == nil {
		t.Fatal("expected public StartManagedAgent to fail closed on substrate mismatch")
	}
	if !strings.Contains(err.Error(), "managed run substrate admission blocked") || !strings.Contains(err.Error(), "installed_executable_hash_mismatch") {
		t.Fatalf("expected managed substrate admission error, got %v", err)
	}
	raw, readErr := os.ReadFile(managedRunSubstrateAdmissionReceiptPath(workdir))
	if readErr != nil {
		t.Fatalf("expected rejected managed run preflight receipt: %v", readErr)
	}
	var receipt managedRunSubstrateAdmissionReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatalf("decode managed run preflight receipt: %v", err)
	}
	if receipt.Admission != "blocked" {
		t.Fatalf("expected blocked admission receipt, got %+v", receipt)
	}
}

func TestStartManagedAgentRejectsUnknownExistingProcessState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 1234, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 1234, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		t.Fatalf("StartManagedAgent must not launch over unknown process provenance")
		return 0, nil
	}

	_, err := startManagedAgentWithoutPreflightForTest(record)
	if err == nil {
		t.Fatal("expected unknown process provenance to block managed start")
	}
	if !strings.Contains(err.Error(), "untrusted managed process state unknown") || !strings.Contains(err.Error(), "missing_executable_identity") {
		t.Fatalf("expected explicit unknown provenance error, got %v", err)
	}
}

func TestStartManagedAgentRejectsLiveProvenanceStaleProcess(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	executable := filepath.Join(workdir, "rhizome-bot.exe")
	if err := os.WriteFile(executable, []byte("original binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(original executable) error: %v", err)
	}
	digest, err := managerFileSHA256(executable)
	if err != nil {
		t.Fatalf("managerFileSHA256() error: %v", err)
	}
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		Workdir:     workdir,
	}
	args := managedAgentDaemonArgs(record)
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:                 1234,
		Executable:          executable,
		ExecutableSHA256:    digest,
		Workdir:             workdir,
		Args:                args,
		ArgsDigest:          managedAgentArgsDigest(args),
		RuntimeConfigDigest: managedAgentRuntimeConfigDigest(managedAgentStartRuntimeConfig(record)),
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}
	if err := os.WriteFile(executable, []byte("replacement binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(replacement executable) error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 1234, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		t.Fatalf("StartManagedAgent must not launch over live stale process provenance")
		return 0, nil
	}

	_, err = startManagedAgentWithoutPreflightForTest(record)
	if err == nil {
		t.Fatal("expected stale process provenance to block managed start")
	}
	if !strings.Contains(err.Error(), "untrusted managed process state stale") || !strings.Contains(err.Error(), "executable_hash_drift") {
		t.Fatalf("expected explicit stale provenance error, got %v", err)
	}
}

func TestStartManagedAgentPassesBoundedRealPilotArgsForRealProvider(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:   "codex",
			Title:        "Codex",
			ChannelType:  providerChannelBridge,
			Driver:       llmBackendCodex,
			GroupID:      "codex",
			DefaultModel: "gpt-5.4",
			Enabled:      true,
			CreatedAt:    "2026-04-27T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "alpha",
		Workdir:     workdir,
		ProviderID:  "codex",
		WorkspaceID: "rhizome-main",
		LLMBackend:  llmBackendCodex,
		Model:       "gpt-5.4",
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		ProviderID:      "codex",
		LLMBackend:      llmBackendCodex,
		Model:           "gpt-5.4",
		WorkspaceID:     "rhizome-main",
		AgentID:         "alpha",
		AgentToken:      "local-token",
		OwnerUserID:     "owner-1",
		BudgetAccountID: "",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	var capturedArgs []string
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		capturedArgs = append([]string(nil), args...)
		return 4321, nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState

	if _, err := startManagedAgentWithoutPreflightForTest(record); err != nil {
		t.Fatalf("startManagedAgentWithoutPreflightForTest() error: %v", err)
	}
	if !containsArg(capturedArgs, "--real-llm-pilot") {
		t.Fatalf("expected managed real provider start to include --real-llm-pilot, got %v", capturedArgs)
	}
	if got := argValue(capturedArgs, "--provider-id"); got != "codex" {
		t.Fatalf("expected managed start to pass provider id, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--agent-id"); got != "alpha" {
		t.Fatalf("expected managed start to pass agent id, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--workspace-id"); got != "rhizome-main" {
		t.Fatalf("expected managed start to pass workspace id, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--provider-call-timeout-sec"); got != "1080" {
		t.Fatalf("expected managed real pilot provider timeout, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--planner-cycle-timeout-sec"); got != "1080" {
		t.Fatalf("expected managed real pilot planner cycle timeout, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-account-id"); got != "pilot-agent-alpha" {
		t.Fatalf("expected stable per-agent budget account, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-hard-limit-micros"); got != "100000000000" {
		t.Fatalf("expected managed real pilot hard limit override, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-reserve-micros"); got != "200000000" {
		t.Fatalf("expected managed real pilot reserve override, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-micros-per-token"); got != "1" {
		t.Fatalf("expected managed real pilot micros-per-token override, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--max-provider-retry-attempts"); got != "1" {
		t.Fatalf("expected managed real pilot retry bound override, got %q in %v", got, capturedArgs)
	}
	profile := LoadLocalRuntimeProfile(workdir)
	if profile.BudgetAccountID != "pilot-agent-alpha" || profile.BudgetHardLimitMicros != managedRealPilotDefaultBudgetHardLimitMicros || profile.BudgetReserveMicros != managedRealPilotDefaultBudgetReserveMicros || profile.BudgetMicrosPerToken != managedRealPilotDefaultBudgetMicrosPerToken {
		t.Fatalf("expected managed start to persist budget defaults in local runtime profile, got account=%q hard=%d reserve=%d price=%d", profile.BudgetAccountID, profile.BudgetHardLimitMicros, profile.BudgetReserveMicros, profile.BudgetMicrosPerToken)
	}
	if profile.ProviderCallTimeoutSec != int(managedRealPilotDefaultProviderCallTimeout/time.Second) || profile.MaxProviderRetryAttempts != realLLMPilotMaxProviderRetryAttempts || profile.MaxToolLoopIterations != defaultToolLoopIterations {
		t.Fatalf("expected managed start to persist pilot bounds in local runtime profile, got timeout=%d retries=%d iterations=%d", profile.ProviderCallTimeoutSec, profile.MaxProviderRetryAttempts, profile.MaxToolLoopIterations)
	}
}

func TestStartManagedAgentTrustFirstKeepsProviderTimeoutAndBudgetPilotBounds(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_COORDINATION_MODE", CoordinationModeStrict)

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:   "codex",
			Title:        "Codex",
			ChannelType:  providerChannelBridge,
			Driver:       llmBackendCodex,
			GroupID:      "codex",
			DefaultModel: "gpt-5.4",
			Enabled:      true,
			CreatedAt:    "2026-05-08T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		ProviderID:               "codex",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4",
		CoordinationMode:         CoordinationModeTrustFirst,
		ProviderCallTimeoutSec:   int(managedRealPilotDefaultProviderCallTimeout / time.Second),
		MaxProviderRetryAttempts: 1,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	record := ManagedAgentRecord{
		AgentID:          "alpha",
		Workdir:          workdir,
		ProviderID:       "codex",
		WorkspaceID:      "rhizome-main",
		LLMBackend:       llmBackendCodex,
		Model:            "gpt-5.4",
		CoordinationMode: CoordinationModeTrustFirst,
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	var capturedArgs []string
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		capturedArgs = append([]string(nil), args...)
		return 4321, nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState

	if _, err := startManagedAgentWithoutPreflightForTest(record); err != nil {
		t.Fatalf("startManagedAgentWithoutPreflightForTest() error: %v", err)
	}
	if !containsArg(capturedArgs, "--real-llm-pilot") {
		t.Fatalf("expected managed real provider start to include --real-llm-pilot, got %v", capturedArgs)
	}
	if got := argValue(capturedArgs, "--coordination-mode"); got != CoordinationModeTrustFirst {
		t.Fatalf("expected trust-first coordination mode arg, got %q in %v", got, capturedArgs)
	}
	expectedProviderTimeout := fmt.Sprintf("%d", int(managedRealPilotDefaultProviderCallTimeout/time.Second))
	if got := argValue(capturedArgs, "--provider-call-timeout-sec"); got != expectedProviderTimeout {
		t.Fatalf("expected trust-first managed launch to keep provider timeout, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--planner-cycle-timeout-sec"); got != expectedProviderTimeout {
		t.Fatalf("expected trust-first managed launch to pass planner cycle timeout, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-account-id"); got != "pilot-agent-alpha" {
		t.Fatalf("expected trust-first managed launch to pass budget account, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-hard-limit-micros"); got != "100000000000" {
		t.Fatalf("expected trust-first managed launch to pass hard budget limit, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-reserve-micros"); got != "200000000" {
		t.Fatalf("expected trust-first managed launch to pass reserve budget, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--budget-micros-per-token"); got != "1" {
		t.Fatalf("expected trust-first managed launch to pass token price, got %q in %v", got, capturedArgs)
	}
}

func TestManagedRealPilotStartRaisesStaleBudgetReserveProfile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_COORDINATION_MODE", CoordinationModeStrict)

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		ProviderID:               "codex",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4-mini",
		CoordinationMode:         CoordinationModeTrustFirst,
		BudgetHardLimitMicros:    1_000_000_000,
		BudgetReserveMicros:      500_000,
		ProviderCallTimeoutSec:   int(managedRealPilotDefaultProviderCallTimeout / time.Second),
		MaxProviderRetryAttempts: 1,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	record := ManagedAgentRecord{
		AgentID:          "alpha",
		Workdir:          workdir,
		ProviderID:       "codex",
		WorkspaceID:      "rhizome-main",
		LLMBackend:       llmBackendCodex,
		Model:            "gpt-5.4-mini",
		CoordinationMode: CoordinationModeTrustFirst,
	}

	args := managedAgentDaemonArgs(record)
	if got := argValue(args, "--budget-reserve-micros"); got != "200000000" {
		t.Fatalf("expected stale managed reserve profile to be raised, got %q in %v", got, args)
	}
	if got := argValue(args, "--budget-hard-limit-micros"); got != "100000000000" {
		t.Fatalf("expected stale managed hard-limit profile to be raised, got %q in %v", got, args)
	}
}

func TestStartManagedAgentPassesRegistryProviderWhenRuntimeProfileMissing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:   "codex-bridge",
			Title:        "Codex Bridge",
			ChannelType:  providerChannelBridge,
			Driver:       llmBackendCodex,
			GroupID:      "codex",
			DefaultModel: "gpt-5.4",
			Enabled:      true,
			CreatedAt:    "2026-05-04T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	root := t.TempDir()
	workdir := filepath.Join(root, "alpha")
	record := ManagedAgentRecord{
		AgentID:     "alpha",
		DisplayName: "Alpha",
		Workdir:     workdir,
		ProviderID:  "codex-bridge",
		WorkspaceID: "rhizome-main",
		LLMBackend:  llmBackendCodex,
		Model:       "gpt-5.4",
		Role:        "strategist",
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	var capturedArgs []string
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		capturedArgs = append([]string(nil), args...)
		return 4321, nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState

	if _, err := startManagedAgentWithoutPreflightForTest(record); err != nil {
		t.Fatalf("startManagedAgentWithoutPreflightForTest() error: %v", err)
	}
	if got := argValue(capturedArgs, "--provider-id"); got != "codex-bridge" {
		t.Fatalf("expected registry provider to override deterministic launch fallback, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--llm-backend"); got != llmBackendCodex {
		t.Fatalf("expected registry backend to be passed, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--agent-id"); got != "alpha" {
		t.Fatalf("expected registry agent id to be passed, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--role"); got != "strategist" {
		t.Fatalf("expected registry role to be passed, got %q in %v", got, capturedArgs)
	}
}

func TestStartManagedAgentMaterializesResolvedAnatomy(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:       "sigma",
		DisplayName:   "Sigma",
		Workdir:       workdir,
		WorkspaceID:   "rhizome-main",
		Role:          "generalist",
		AnatomyPreset: "ui-critic",
		LLMBackend:    llmBackendFake,
		Model:         "normal_complete",
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	var capturedArgs []string
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		capturedArgs = append([]string(nil), args...)
		return 4321, nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState

	if _, err := startManagedAgentWithoutPreflightForTest(record); err != nil {
		t.Fatalf("startManagedAgentWithoutPreflightForTest() error: %v", err)
	}
	if got := argValue(capturedArgs, "--agent-id"); got != "sigma" {
		t.Fatalf("expected launch args to preserve agent id, got %q in %v", got, capturedArgs)
	}
	anatomy, err := ReadAgentAnatomyConfig(workdir, DefaultAgentProfile("sigma", "Sigma", "generalist"))
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig() error: %v", err)
	}
	if anatomy.Preset != "ui_ux_reality_critic" {
		t.Fatalf("expected materialized UI anatomy, got %+v", anatomy)
	}
	if _, ok := findTestHeartbeat(anatomy, "visual_product_audit"); !ok {
		t.Fatalf("expected visual audit heartbeat in materialized anatomy")
	}
	if !pathExists(filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_visual_probe", installedToolBundleManifestName)) {
		t.Fatalf("expected UI anatomy to materialize browser_visual_probe bundle into agent workdir")
	}
	if !pathExists(filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_session", installedToolBundleManifestName)) {
		t.Fatalf("expected UI anatomy to materialize browser_session bundle into agent workdir")
	}
}

func TestMaterializeManagedAgentAnatomyRejectsDigestMismatch(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	_, err := MaterializeManagedAgentAnatomy(ManagedAgentRecord{
		AgentID:       "alpha",
		Workdir:       workdir,
		WorkspaceID:   "rhizome-main",
		Role:          "strategist",
		AnatomyDigest: "not-the-real-digest",
	}, RuntimeConfig{AgentID: "alpha", Workdir: workdir, WorkspaceID: "rhizome-main", Role: "strategist"})
	if err == nil {
		t.Fatal("expected digest mismatch")
	}
	if !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("expected digest mismatch error, got %v", err)
	}
}

func TestMaterializeManagedAgentAnatomyPresetDoesNotMergeRoleDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	result, err := MaterializeManagedAgentAnatomy(ManagedAgentRecord{
		AgentID:       "alpha",
		DisplayName:   "Alpha",
		Workdir:       workdir,
		WorkspaceID:   "rhizome-main",
		Role:          "strategist",
		AnatomyPreset: "generalist",
	}, RuntimeConfig{AgentID: "alpha", DisplayName: "Alpha", Workdir: workdir, WorkspaceID: "rhizome-main", Role: "strategist"})
	if err != nil {
		t.Fatalf("MaterializeManagedAgentAnatomy() error: %v", err)
	}
	if result.Preset != "generalist" {
		t.Fatalf("expected generalist preset, got %+v", result)
	}
	anatomy, err := ReadAgentAnatomyConfig(workdir, DefaultAgentProfile("alpha", "Alpha", "strategist"))
	if err != nil {
		t.Fatal(err)
	}
	if anatomy.Preset != "generalist" {
		t.Fatalf("expected saved anatomy preset to remain generalist, got %q", anatomy.Preset)
	}
	if _, ok := findTestHeartbeat(anatomy, "project_role_initiative"); ok {
		t.Fatalf("explicit generalist preset must not inherit strategist heartbeat: %+v", anatomy.Heartbeats)
	}
}

func TestManagedAgentStartRuntimeConfigPrefersRegistryIdentityOverStaleLocalProfile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			Capabilities: []string{"tool.call", "project.git"},
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		ProviderID:  "codex",
		LLMBackend:  llmBackendCodex,
		Model:       "gpt-5.4",
		WorkspaceID: "rhizome-main",
		AgentID:     "alpha",
		Role:        "generalist",
		Capabilities: []string{
			"tool.call",
			"legacy.local",
		},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cfg := managedAgentStartRuntimeConfig(ManagedAgentRecord{
		AgentID:     "alpha",
		DisplayName: "Alpha",
		Workdir:     workdir,
		ProviderID:  "codex-bridge",
		WorkspaceID: "rhizome-main",
		LLMBackend:  llmBackendCodex,
		Model:       "gpt-5.4",
		Role:        "strategist",
	})
	if cfg.ProviderID != "codex-bridge" {
		t.Fatalf("expected registry provider to override stale local profile, got %q", cfg.ProviderID)
	}
	if cfg.Role != "strategist" {
		t.Fatalf("expected registry role to override stale local profile, got %q", cfg.Role)
	}
	if got := strings.Join(cfg.Capabilities, ","); got != "tool.call,project.git" {
		t.Fatalf("expected registry capabilities to override stale local profile, got %+v", cfg.Capabilities)
	}
}

func TestManagedAgentStartRuntimeConfigFallsBackFromStaleCodexFullModel(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		ProviderID:    "codex",
		ModelOverride: "gpt-5.4",
		LLMBackend:    llmBackendCodex,
		Model:         "gpt-5.4",
		WorkspaceID:   "rhizome-main",
		AgentID:       "alpha",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cfg := managedAgentStartRuntimeConfig(ManagedAgentRecord{
		AgentID:       "alpha",
		Workdir:       workdir,
		ProviderID:    "codex",
		ModelOverride: "gpt-5.4",
		LLMBackend:    llmBackendCodex,
		Model:         "gpt-5.4",
		WorkspaceID:   "rhizome-main",
	})
	if cfg.ModelOverride != defaultModel || cfg.Model != defaultModel {
		t.Fatalf("expected stale codex full model to fall back to %q, got model_override=%q model=%q", defaultModel, cfg.ModelOverride, cfg.Model)
	}
}

func TestManagedAgentStartRuntimeConfigUsesTrustFirstRegistryDefaultWhenRecordUnset(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			CoordinationMode: CoordinationModeTrustFirst,
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	cfg := managedAgentStartRuntimeConfig(ManagedAgentRecord{
		AgentID: "alpha",
		Workdir: workdir,
	})
	if cfg.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected blank record to inherit trust-first manager default, got %q", cfg.CoordinationMode)
	}
}

func TestManagedAgentStartRuntimeConfigTrustFirstRegistryDefaultOverridesStaleLocalStrict(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			CoordinationMode: CoordinationModeTrustFirst,
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		CoordinationMode: CoordinationModeStrict,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cfg := managedAgentStartRuntimeConfig(ManagedAgentRecord{
		AgentID: "alpha",
		Workdir: workdir,
	})
	if cfg.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected trust-first registry default to override stale local strict profile, got %q", cfg.CoordinationMode)
	}
}

func TestManagedAgentStartRuntimeConfigIgnoresAmbientCoordinationMode(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_COORDINATION_MODE", CoordinationModeStrict)

	workdir := t.TempDir()
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			CoordinationMode: CoordinationModeTrustFirst,
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	cfg := managedAgentStartRuntimeConfig(ManagedAgentRecord{
		AgentID: "alpha",
		Workdir: workdir,
	})
	if cfg.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected managed registry/default coordination mode to ignore ambient env override, got %q", cfg.CoordinationMode)
	}
}

func TestStartManagedAgentDoesNotPassRealPilotArgsForFakeBackend(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:    "fake-agent",
		Workdir:    workdir,
		LLMBackend: llmBackendFake,
		Model:      "normal_complete",
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		LLMBackend: llmBackendFake,
		Model:      "normal_complete",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	var capturedArgs []string
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		capturedArgs = append([]string(nil), args...)
		return 4321, nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState

	if _, err := startManagedAgentWithoutPreflightForTest(record); err != nil {
		t.Fatalf("startManagedAgentWithoutPreflightForTest() error: %v", err)
	}
	if got := argValue(capturedArgs, "--llm-backend"); got != llmBackendFake {
		t.Fatalf("expected fake backend start to pass backend, got %q in %v", got, capturedArgs)
	}
	if got := argValue(capturedArgs, "--agent-id"); got != "fake-agent" {
		t.Fatalf("expected fake backend start to pass agent id, got %q in %v", got, capturedArgs)
	}
	if containsArg(capturedArgs, "--real-llm-pilot") {
		t.Fatalf("expected fake backend start to omit --real-llm-pilot, got %v", capturedArgs)
	}
}

func TestRestartManagedAgentSurfacesReplacementStartFailureExplicitly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		switch callCount {
		case 1, 2:
			return true, nil
		default:
			return false, nil
		}
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		return 0, errors.New("spawn failed")
	}
	managedAgentSaveStateFunc = SaveAgentProcessState
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	_, err := restartManagedAgentWithoutPreflightForTest(record)
	if err == nil {
		t.Fatal("expected restart replacement start failure")
	}
	if killedPID != 0 {
		t.Fatalf("expected restart to stop existing pid gracefully without force kill, got %d", killedPID)
	}
	if !strings.Contains(err.Error(), "restart agent lyrica stopped current process but failed to start replacement") || !strings.Contains(err.Error(), "spawn failed") {
		t.Fatalf("expected explicit restart replacement failure, got %v", err)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected process state to stay absent after failed replacement start, stat err=%v", statErr)
	}
}

func TestRestartManagedAgentRunsCleanStopBeforeReplacementPreflight(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origAdmit := admitManagedRunStartFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		admitManagedRunStartFunc = origAdmit
	}()

	order := []string{}
	existsCalls := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		existsCalls++
		order = append(order, fmt.Sprintf("exists:%d", existsCalls))
		return existsCalls == 1, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		order = append(order, "matches")
		return true, nil
	}
	admitManagedRunStartFunc = func(gotRecord ManagedAgentRecord) (managedRunPreflightResult, error) {
		order = append(order, "preflight")
		return managedRunPreflightResult{}, &managedRunPreflightBlockedError{Reasons: []string{"remote_active_claim:task-project"}}
	}

	_, err := RestartManagedAgentWithPreflight(record)
	if err == nil {
		t.Fatal("expected restart preflight block after clean stop")
	}
	if !strings.Contains(err.Error(), "stopped current process but preflight blocked replacement") || !strings.Contains(err.Error(), "remote_active_claim:task-project") {
		t.Fatalf("expected explicit post-stop preflight failure, got %v", err)
	}
	if got := strings.Join(order, ","); got != "exists:1,matches,exists:2,preflight" {
		t.Fatalf("expected cleanup/stop before preflight, got %s", got)
	}
	if _, statErr := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected process state to stay absent after post-stop preflight block, stat err=%v", statErr)
	}
}

func TestTailManagedAgentLogsReturnsLastLines(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent.out.log"), []byte("one\ntwo\nthree\nfour\n"), 0o644); err != nil {
		t.Fatalf("write stdout log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent.err.log"), []byte("err-one\nerr-two\n"), 0o644); err != nil {
		t.Fatalf("write stderr log: %v", err)
	}

	tail, err := TailManagedAgentLogs(record, 2)
	if err != nil {
		t.Fatalf("TailManagedAgentLogs() error: %v", err)
	}
	if len(tail.Stdout) != 2 || tail.Stdout[0] != "three" || tail.Stdout[1] != "four" {
		t.Fatalf("unexpected stdout tail: %+v", tail.Stdout)
	}
	if len(tail.Stderr) != 2 || tail.Stderr[0] != "err-one" || tail.Stderr[1] != "err-two" {
		t.Fatalf("unexpected stderr tail: %+v", tail.Stderr)
	}
}

func TestTailManagedAgentLogsPreservesUTF8(t *testing.T) {
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	}
	want := "Опубликованы канонические доказательства"
	if err := os.WriteFile(filepath.Join(workdir, "agent.out.log"), []byte("prefix\n"+want+"\n"), 0o644); err != nil {
		t.Fatalf("write stdout log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent.err.log"), nil, 0o644); err != nil {
		t.Fatalf("write stderr log: %v", err)
	}

	tail, err := TailManagedAgentLogs(record, 1)
	if err != nil {
		t.Fatalf("TailManagedAgentLogs() error: %v", err)
	}
	if len(tail.Stdout) != 1 || tail.Stdout[0] != want {
		t.Fatalf("expected UTF-8 log tail %q, got %+v", want, tail.Stdout)
	}
}

func TestManagedAgentWorkdirProcessCleanupRootsIncludeBrowserSessions(t *testing.T) {
	workdir := t.TempDir()
	sessionRoot := filepath.Join(workdir, ".runtime-config", "browser-sessions")
	if err := os.MkdirAll(sessionRoot, 0o755); err != nil {
		t.Fatalf("mkdir browser sessions root: %v", err)
	}

	roots := managedAgentWorkdirProcessCleanupRoots(workdir)
	if !containsCleanPath(roots, workdir) {
		t.Fatalf("expected cleanup roots to include workdir when browser sessions exist, got %v", roots)
	}
	if !containsCleanPath(roots, sessionRoot) {
		t.Fatalf("expected cleanup roots to include browser session root, got %v", roots)
	}
}

func TestStopManagedAgentRemovesBrowserSessionStateWithoutProcess(t *testing.T) {
	workdir := t.TempDir()
	sessionDir := filepath.Join(workdir, ".runtime-config", "browser-sessions", "default")
	if err := os.MkdirAll(filepath.Join(sessionDir, "profile"), 0o755); err != nil {
		t.Fatalf("mkdir browser session profile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "session.json"), []byte(`{"session_id":"default","pid":4242}`), 0o644); err != nil {
		t.Fatalf("write browser session state: %v", err)
	}

	origCleanup := managedAgentCleanupWorkdirFunc
	defer func() { managedAgentCleanupWorkdirFunc = origCleanup }()

	cleanupCalled := false
	managedAgentCleanupWorkdirFunc = func(gotWorkdir string) (string, error) {
		cleanupCalled = true
		if filepath.Clean(gotWorkdir) != filepath.Clean(workdir) {
			t.Fatalf("cleanup workdir = %q, want %q", gotWorkdir, workdir)
		}
		return "", nil
	}

	if err := StopManagedAgent(ManagedAgentRecord{AgentID: "sigma", Workdir: workdir}); err != nil {
		t.Fatalf("StopManagedAgent() error: %v", err)
	}
	if !cleanupCalled {
		t.Fatalf("expected stop to run workdir process cleanup before pruning browser session state")
	}
	if _, err := os.Stat(filepath.Join(workdir, ".runtime-config", "browser-sessions")); !os.IsNotExist(err) {
		t.Fatalf("expected browser session state root to be removed, stat err=%v", err)
	}
}

func containsCleanPath(paths []string, want string) bool {
	want = filepath.Clean(want)
	for _, path := range paths {
		if filepath.Clean(path) == want {
			return true
		}
	}
	return false
}

func TestBuildManagedAgentProcessEnvUsesAllowlistAndLocalScratch(t *testing.T) {
	home := t.TempDir()
	allowedPath := filepath.Join(t.TempDir(), "tools")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "developer")
	t.Setenv("PATH", allowedPath)
	t.Setenv("OPENAI_API_KEY", "openai-secret")
	t.Setenv("HTTPS_PROXY", "http://proxy.internal")
	t.Setenv("RHIZOME_TOKEN", "do-not-leak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "also-do-not-leak")

	workdir := t.TempDir()
	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     workdir,
		OwnerUserID: "developer",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}

	if got[managedAgentEnvFlag] != "1" {
		t.Fatalf("expected managed env flag, got %+v", got)
	}
	if !pathListContains(filepath.SplitList(got["PATH"]), allowedPath) || got["OPENAI_API_KEY"] != "openai-secret" {
		t.Fatalf("expected allowed env keys to survive, got %+v", got)
	}
	if got["HTTPS_PROXY"] != "http://proxy.internal" {
		t.Fatalf("expected proxy env to survive, got %+v", got)
	}
	if _, ok := got["RHIZOME_TOKEN"]; ok {
		t.Fatalf("expected RHIZOME_TOKEN to be stripped, got %+v", got)
	}
	if _, ok := got["AWS_SECRET_ACCESS_KEY"]; ok {
		t.Fatalf("expected unrelated host secret to be stripped, got %+v", got)
	}
	for _, key := range []string{"TMP", "TEMP", "TMPDIR"} {
		if got[key] != filepath.Join(workdir, ".tmp") {
			t.Fatalf("expected %s to point at workdir scratch, got %+v", key, got)
		}
	}
	if got["GOCACHE"] != filepath.Join(workdir, ".cache", "go-build") {
		t.Fatalf("expected GOCACHE to point at managed go build cache, got %+v", got)
	}
	// TA-01: GOMODCACHE/GOPATH must be explicit per-agent writable paths, not derived from the
	// repointed empty managed HOME (which caused cold/unwritable module caches and silent build
	// failures).
	if got["GOMODCACHE"] != filepath.Join(workdir, ".cache", "go-mod") {
		t.Fatalf("expected GOMODCACHE to point at managed go module cache, got %+v", got)
	}
	if got["GOPATH"] != filepath.Join(workdir, ".cache", "go-path") {
		t.Fatalf("expected GOPATH to point at managed go path, got %+v", got)
	}
	if got["LOCALAPPDATA"] != filepath.Join(workdir, ".local-data", "LocalAppData") || got["LocalAppData"] != filepath.Join(workdir, ".local-data", "LocalAppData") {
		t.Fatalf("expected LOCALAPPDATA variants to point at managed local app data, got %+v", got)
	}
	if got["APPDATA"] != filepath.Join(workdir, ".local-data", "AppData", "Roaming") {
		t.Fatalf("expected APPDATA to point at managed roaming app data, got %+v", got)
	}
}

func TestBuildManagedAgentProcessEnvMaterializesProviderRegistry(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:   "codex",
			Title:        "Codex",
			ChannelType:  providerChannelBridge,
			Driver:       llmBackendCodex,
			GroupID:      "codex",
			DefaultModel: "gpt-5.4",
			Enabled:      true,
			CreatedAt:    "2026-04-27T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	workdir := t.TempDir()
	if _, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:    "alpha",
		Workdir:    workdir,
		ProviderID: "codex",
	}); err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	registryPath := filepath.Join(managedAgentConfigRootPath(workdir), providerRegistryFilename)
	registry, err := loadProviderRegistryFromDisk(registryPath)
	if err != nil {
		t.Fatalf("load materialized provider registry: %v", err)
	}
	provider, ok := findProviderRecordInRegistry(registry, "codex")
	if !ok {
		t.Fatalf("expected codex provider in materialized registry, got %+v", registry)
	}
	if provider.Driver != llmBackendCodex || provider.GroupID != "codex" || provider.DefaultModel != "gpt-5.4" {
		t.Fatalf("materialized provider changed shape: %+v", provider)
	}
}

func TestBuildManagedAgentProcessEnvOmitsSharedOpenAIKeyForPartnerOwner(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "developer")
	t.Setenv("PATH", "C:\\tools")
	t.Setenv("OPENAI_API_KEY", "openai-secret")

	workdir := t.TempDir()
	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "partner",
		Workdir:     workdir,
		OwnerUserID: "partner-owner",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}
	if _, ok := got["OPENAI_API_KEY"]; ok {
		t.Fatalf("expected partner managed env to omit shared OPENAI_API_KEY, got %+v", got)
	}
}

func TestBuildManagedAgentProcessEnvOmitsSharedProxyEnvForPartnerOwner(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "developer")
	t.Setenv("HTTP_PROXY", "http://proxy.internal")
	t.Setenv("HTTPS_PROXY", "https://proxy.internal")
	t.Setenv("NO_PROXY", "localhost,127.0.0.1")
	t.Setenv("ALL_PROXY", "socks5://proxy.internal")

	workdir := t.TempDir()
	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "partner",
		Workdir:     workdir,
		OwnerUserID: "partner-owner",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY"} {
		if _, ok := got[key]; ok {
			t.Fatalf("expected partner managed env to omit %s, got %+v", key, got)
		}
	}
}

func TestBuildManagedAgentProcessEnvOmitsSharedSSLCertEnvForPartnerOwner(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "developer")
	t.Setenv("SSL_CERT_FILE", filepath.Join(home, "certs", "ca.pem"))
	t.Setenv("SSL_CERT_DIR", filepath.Join(home, "certs"))

	workdir := t.TempDir()
	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "partner",
		Workdir:     workdir,
		OwnerUserID: "partner-owner",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}
	for _, key := range []string{"SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if _, ok := got[key]; ok {
			t.Fatalf("expected partner managed env to omit %s, got %+v", key, got)
		}
	}
}

func TestBuildManagedAgentProcessEnvCarriesManagerOwnedShellDecision(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	trustedEnv, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     t.TempDir(),
		OwnerUserID: "developer",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(trusted) error: %v", err)
	}
	partnerEnv, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "partner",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(partner) error: %v", err)
	}

	trusted := map[string]string{}
	for _, entry := range trustedEnv {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed trusted env entry %q", entry)
		}
		trusted[key] = value
	}
	partner := map[string]string{}
	for _, entry := range partnerEnv {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed partner env entry %q", entry)
		}
		partner[key] = value
	}

	if trusted[managedAgentAllowLocalShellFlag] != "1" {
		t.Fatalf("expected trusted managed env to carry shell entitlement, got %+v", trusted)
	}
	if trusted["RHIZOME_OWNER_USER_ID"] != "developer" {
		t.Fatalf("expected trusted managed env to carry the trusted owner reference, got %+v", trusted)
	}
	if partner["RHIZOME_OWNER_USER_ID"] != "developer" {
		t.Fatalf("expected partner managed env to carry the trusted owner reference, got %+v", partner)
	}
	if _, ok := partner[managedAgentAllowLocalShellFlag]; ok {
		t.Fatalf("expected partner managed env to omit shell entitlement, got %+v", partner)
	}
}

func TestBuildManagedAgentProcessEnvCarriesManagerOwnedMutationDecision(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	trustedEnv, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     t.TempDir(),
		OwnerUserID: "developer",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(trusted) error: %v", err)
	}
	partnerEnv, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "partner",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(partner) error: %v", err)
	}

	trusted := map[string]string{}
	for _, entry := range trustedEnv {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed trusted env entry %q", entry)
		}
		trusted[key] = value
	}
	partner := map[string]string{}
	for _, entry := range partnerEnv {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed partner env entry %q", entry)
		}
		partner[key] = value
	}

	if trusted[managedAgentAllowLocalMutationFlag] != "1" {
		t.Fatalf("expected trusted managed env to carry local mutation entitlement, got %+v", trusted)
	}
	if _, ok := partner[managedAgentAllowLocalMutationFlag]; ok {
		t.Fatalf("expected partner managed env to omit local mutation entitlement, got %+v", partner)
	}
}

func TestBuildManagedAgentProcessEnvTrustFirstGrantsLocalExecutionToPartner(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:          "partner",
		Workdir:          t.TempDir(),
		OwnerUserID:      "partner-owner",
		CoordinationMode: CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(trust-first partner) error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}
	if got[managedAgentAllowLocalShellFlag] != "1" || got[managedAgentAllowLocalMutationFlag] != "1" {
		t.Fatalf("expected trust-first partner env to carry local execution entitlements, got %+v", got)
	}
	if got["RHIZOME_AGENT_CODEX_SANDBOX"] != "danger-full-access" || got["RHIZOME_COORDINATION_MODE"] != CoordinationModeTrustFirst {
		t.Fatalf("expected trust-first env markers, got %+v", got)
	}
	if got[codexExecVersionPinEnvFlag] != defaultManagedCodexCLIVersionPin {
		t.Fatalf("expected managed codex version pin %q, got %+v", defaultManagedCodexCLIVersionPin, got)
	}
}

func TestBuildManagedAgentProcessEnvTrustsLocalOwnerAliasWhenGlobalOwnerIsCanonical(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{
		OwnerUserID: "user-live",
		HostURL:     "http://127.0.0.1:8420",
		WorkspaceID: "rhizome-main",
	}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			OwnerUserID: "developer",
			HostURL:     "http://127.0.0.1:8420",
			WorkspaceID: "rhizome-main",
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "beta",
		Workdir:     t.TempDir(),
		OwnerUserID: "developer",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(alias owner) error: %v", err)
	}
	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}

	if got[managedAgentAllowLocalShellFlag] != "1" || got[managedAgentAllowLocalMutationFlag] != "1" {
		t.Fatalf("expected local owner alias to keep trusted local entitlements, got %+v", got)
	}
	if got["RHIZOME_OWNER_USER_ID"] != "user-live" {
		t.Fatalf("expected env to keep canonical owner reference for registration, got %+v", got)
	}
	if got[managedAgentTrustedOwnerUserIDsFlag] != "user-live,developer" {
		t.Fatalf("expected env to carry canonical and local owner references, got %+v", got)
	}
}

func TestBuildManagedAgentProcessEnvPartitionsRuntimeConfigAndCodexHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("CODEX_CLI_PATH", filepath.Join(home, "shared-codex"))
	t.Setenv("CUSTOM_CLI_PATH", filepath.Join(home, "shared-custom-codex"))
	t.Setenv("RHIZOME_OWNER_USER_ID", "developer")

	workdir := t.TempDir()
	trustedEnv, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "trusted",
		Workdir:     workdir,
		OwnerUserID: "developer",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(trusted) error: %v", err)
	}
	partnerEnv, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "partner",
		Workdir:     workdir,
		OwnerUserID: "partner-owner",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv(partner) error: %v", err)
	}

	trusted := map[string]string{}
	for _, entry := range trustedEnv {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed trusted env entry %q", entry)
		}
		trusted[key] = value
	}
	partner := map[string]string{}
	for _, entry := range partnerEnv {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed partner env entry %q", entry)
		}
		partner[key] = value
	}

	wantConfigRoot := filepath.Join(workdir, ".runtime-config")
	if trusted[managedAgentConfigRootFlag] != wantConfigRoot {
		t.Fatalf("trusted RHIZOME_AGENT_CONFIG_ROOT = %q, want %q", trusted[managedAgentConfigRootFlag], wantConfigRoot)
	}
	if partner[managedAgentConfigRootFlag] != wantConfigRoot {
		t.Fatalf("partner RHIZOME_AGENT_CONFIG_ROOT = %q, want %q", partner[managedAgentConfigRootFlag], wantConfigRoot)
	}
	wantIdentityRoot := filepath.Join(filepath.Dir(workdir), ".runtime-identity")
	if trusted[managedAgentIdentityLeaseRootFlag] != wantIdentityRoot {
		t.Fatalf("trusted RHIZOME_AGENT_IDENTITY_LEASE_ROOT = %q, want %q", trusted[managedAgentIdentityLeaseRootFlag], wantIdentityRoot)
	}
	if partner[managedAgentIdentityLeaseRootFlag] != wantIdentityRoot {
		t.Fatalf("partner RHIZOME_AGENT_IDENTITY_LEASE_ROOT = %q, want %q", partner[managedAgentIdentityLeaseRootFlag], wantIdentityRoot)
	}
	if trusted["HOME"] != home || trusted["USERPROFILE"] != home {
		t.Fatalf("expected trusted managed env to keep shared home roots, got %+v", trusted)
	}

	wantManagedHome := filepath.Join(workdir, ".home")
	if partner["HOME"] != wantManagedHome || partner["USERPROFILE"] != wantManagedHome {
		t.Fatalf("expected partner managed env to use isolated home roots, got %+v", partner)
	}

	wantCodexHome := filepath.Join(workdir, ".codex-home")
	if _, ok := trusted[managedAgentCodexHomeFlag]; ok {
		t.Fatalf("expected trusted managed env to omit RHIZOME_AGENT_CODEX_HOME, got %+v", trusted)
	}
	if partner[managedAgentCodexHomeFlag] != wantCodexHome {
		t.Fatalf("partner RHIZOME_AGENT_CODEX_HOME = %q, want %q", partner[managedAgentCodexHomeFlag], wantCodexHome)
	}
	if partner["CODEX_HOME"] != wantCodexHome {
		t.Fatalf("partner CODEX_HOME = %q, want %q", partner["CODEX_HOME"], wantCodexHome)
	}
	if trusted["CODEX_CLI_PATH"] == "" || trusted["CUSTOM_CLI_PATH"] == "" {
		t.Fatalf("expected trusted managed env to retain explicit codex executable overrides, got %+v", trusted)
	}
	if _, ok := partner["CODEX_CLI_PATH"]; ok {
		t.Fatalf("expected partner managed env to omit shared CODEX_CLI_PATH, got %+v", partner)
	}
	if _, ok := partner["CUSTOM_CLI_PATH"]; ok {
		t.Fatalf("expected partner managed env to omit shared CUSTOM_CLI_PATH, got %+v", partner)
	}
}

func TestBuildManagedAgentProcessEnvUsesManagedHomeForTrustedBridgeProvider(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	sharedCodex := filepath.Join(home, "shared-codex")
	if runtime.GOOS == "windows" {
		sharedCodex += ".exe"
	}
	if err := os.WriteFile(sharedCodex, []byte("stub"), 0o755); err != nil {
		t.Fatalf("WriteFile(sharedCodex) error: %v", err)
	}
	t.Setenv("CODEX_CLI_PATH", sharedCodex)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}
	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-managed-home",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "group-codex-managed-home",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			UseManagedHome: true,
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	workdir := t.TempDir()
	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "trusted",
		Workdir:     workdir,
		OwnerUserID: "developer",
		ProviderID:  "codex-managed-home",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}

	wantManagedHome := filepath.Join(workdir, ".home")
	wantCodexHome := filepath.Join(workdir, ".codex-home")
	if got["HOME"] != wantManagedHome || got["USERPROFILE"] != wantManagedHome {
		t.Fatalf("expected trusted provider-managed home roots, got %+v", got)
	}
	if got[managedAgentCodexHomeFlag] != wantCodexHome || got["CODEX_HOME"] != wantCodexHome {
		t.Fatalf("expected trusted provider-managed codex home, got %+v", got)
	}
	if got["CODEX_CLI_PATH"] != sharedCodex || got["CUSTOM_CLI_PATH"] != sharedCodex {
		t.Fatalf("expected provider-managed home to retain deterministic codex executable overrides, got %+v", got)
	}
}

func TestBuildManagedAgentProcessEnvSeedsManagedCodexAuth(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}
	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-managed-auth",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "group-codex-managed-auth",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			UseManagedHome: true,
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	sharedCodexHome := filepath.Join(home, codexConfigDir)
	if err := os.MkdirAll(sharedCodexHome, 0o755); err != nil {
		t.Fatalf("MkdirAll(sharedCodexHome) error: %v", err)
	}
	authJSON := []byte(`{"auth_mode":"chatgpt","tokens":{"access_token":"token"}}`)
	if err := os.WriteFile(filepath.Join(sharedCodexHome, "auth.json"), authJSON, 0o600); err != nil {
		t.Fatalf("WriteFile(shared auth) error: %v", err)
	}

	workdir := t.TempDir()
	if _, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "trusted",
		Workdir:     workdir,
		OwnerUserID: "developer",
		ProviderID:  "codex-managed-auth",
	}); err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(workdir, ".codex-home", "auth.json"))
	if err != nil {
		t.Fatalf("ReadFile(managed auth) error: %v", err)
	}
	if string(got) != string(authJSON) {
		t.Fatalf("managed auth was not copied from shared codex home")
	}
}

func TestBuildManagedAgentProcessEnvUsesCanonicalCodexExecutableForTrustedManagedHomeWithoutEnvOverride(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("CODEX_CLI_PATH", "")
	t.Setenv("CUSTOM_CLI_PATH", "")
	t.Setenv("PATH", "")
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}
	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-managed-home-canonical",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "group-codex-managed-home-canonical",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			UseManagedHome: true,
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	canonicalCodex := filepath.Join(home, ".local", "bin", name)
	if err := os.MkdirAll(filepath.Dir(canonicalCodex), 0o755); err != nil {
		t.Fatalf("MkdirAll(canonicalCodex) error: %v", err)
	}
	if err := os.WriteFile(canonicalCodex, []byte("stub"), 0o755); err != nil {
		t.Fatalf("WriteFile(canonicalCodex) error: %v", err)
	}

	workdir := t.TempDir()
	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "trusted",
		Workdir:     workdir,
		OwnerUserID: "developer",
		ProviderID:  "codex-managed-home-canonical",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}

	if got["CODEX_CLI_PATH"] != canonicalCodex || got["CUSTOM_CLI_PATH"] != canonicalCodex {
		t.Fatalf("expected trusted managed home to resolve canonical codex executable, got %+v", got)
	}
}

func TestBuildManagedAgentProcessEnvTreatsGlobalOwnerIdentityAsTrusted(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("PATH", "")
	t.Setenv("CODEX_CLI_PATH", "")
	t.Setenv("CUSTOM_CLI_PATH", "")
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{
		OwnerUserID:       "user-live",
		HostURL:           "https://rhizome.test",
		WorkspaceID:       "rhizome-main",
		WorkspacePassword: "test-workspace-password",
	}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			OwnerUserID:       "developer",
			HostURL:           "https://rhizome.test",
			WorkspaceID:       "rhizome-main",
			WorkspacePassword: "test-workspace-password",
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-global-owner",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "group-codex-global-owner",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			UseManagedHome: true,
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	canonicalCodex := filepath.Join(home, ".local", "bin", name)
	if err := os.MkdirAll(filepath.Dir(canonicalCodex), 0o755); err != nil {
		t.Fatalf("MkdirAll(canonicalCodex) error: %v", err)
	}
	if err := os.WriteFile(canonicalCodex, []byte("stub"), 0o755); err != nil {
		t.Fatalf("WriteFile(canonicalCodex) error: %v", err)
	}

	env, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:     "observer",
		Workdir:     t.TempDir(),
		OwnerUserID: "user-live",
		ProviderID:  "codex-global-owner",
	})
	if err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error: %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", entry)
		}
		got[key] = value
	}

	if got[managedAgentAllowLocalShellFlag] != "1" || got[managedAgentAllowLocalMutationFlag] != "1" {
		t.Fatalf("expected live owner identity to keep trusted local permissions, got %+v", got)
	}
	if got["CODEX_CLI_PATH"] != canonicalCodex || got["CUSTOM_CLI_PATH"] != canonicalCodex {
		t.Fatalf("expected trusted live owner identity to inject codex executable, got %+v", got)
	}
}

func TestBuildManagedAgentProcessEnvRefreshesStaleManagedCodexAuth(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("PATH", "")
	t.Setenv("CODEX_CLI_PATH", "")
	t.Setenv("CUSTOM_CLI_PATH", "")
	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-managed-refresh",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "group-codex-managed-refresh",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			UseManagedHome: true,
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	sourceAuth := `{"auth_mode":"chatgpt","tokens":{"access_token":"fresh-token"}}`
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o755); err != nil {
		t.Fatalf("MkdirAll(source codex) error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(sourceAuth), 0o600); err != nil {
		t.Fatalf("WriteFile(source auth) error: %v", err)
	}

	workdir := t.TempDir()
	targetHome := managedAgentCodexHomePath(workdir)
	if err := os.MkdirAll(targetHome, 0o755); err != nil {
		t.Fatalf("MkdirAll(target codex) error: %v", err)
	}
	staleAuth := `{"auth_mode":"chatgpt","tokens":{"access_token":"stale-token"}}`
	if err := os.WriteFile(filepath.Join(targetHome, "auth.json"), []byte(staleAuth), 0o600); err != nil {
		t.Fatalf("WriteFile(stale auth) error: %v", err)
	}

	if _, err := buildManagedAgentProcessEnv(ManagedAgentRecord{
		AgentID:    "refresh",
		Workdir:    workdir,
		ProviderID: "codex-managed-refresh",
	}); err != nil {
		t.Fatalf("buildManagedAgentProcessEnv() error = %v", err)
	}

	got, err := os.ReadFile(filepath.Join(targetHome, "auth.json"))
	if err != nil {
		t.Fatalf("ReadFile(target auth) error: %v", err)
	}
	if string(got) != sourceAuth {
		t.Fatalf("expected managed auth to refresh from shared source, got %s", string(got))
	}
}

func TestStartManagedAgentRejectsDisabledProvider(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-disabled",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex-disabled",
			Enabled:     false,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	_, err := startManagedAgentWithoutPreflightForTest(ManagedAgentRecord{
		AgentID:    "lyrica",
		Workdir:    t.TempDir(),
		ProviderID: "codex-disabled",
	})
	if !errors.Is(err, errProviderDisabled) {
		t.Fatalf("expected disabled provider error, got %v", err)
	}
}
