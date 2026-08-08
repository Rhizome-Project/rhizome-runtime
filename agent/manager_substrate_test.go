package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func installCleanManagerSubstrateStubsForTest(t *testing.T, current, installed string) {
	t.Helper()
	repoRoot := t.TempDir()
	origExecutable := managerSubstrateExecutableFunc
	origInstalled := managerSubstrateInstalledExecutableFunc
	origRepoRoot := managerSubstrateRepoRootFunc
	origCommand := managerSubstrateCommandOutputFunc
	origBuild := managerSubstrateRuntimeBuildFunc
	t.Cleanup(func() {
		managerSubstrateExecutableFunc = origExecutable
		managerSubstrateInstalledExecutableFunc = origInstalled
		managerSubstrateRepoRootFunc = origRepoRoot
		managerSubstrateCommandOutputFunc = origCommand
		managerSubstrateRuntimeBuildFunc = origBuild
	})
	managerSubstrateExecutableFunc = func() (string, error) { return current, nil }
	managerSubstrateInstalledExecutableFunc = func() string { return installed }
	managerSubstrateRepoRootFunc = func() string { return repoRoot }
	managerSubstrateCommandOutputFunc = func(_ string, _ string, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "rev-parse HEAD"):
			return "abc123\n", nil
		case strings.Contains(joined, "rev-parse --abbrev-ref HEAD"):
			return "main\n", nil
		case strings.Contains(joined, "status --porcelain"):
			return "", nil
		default:
			return "", errors.New("unexpected command")
		}
	}
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "abc123"}
	}
}

func writeExecutableForSubstrateTest(t *testing.T, dir, name, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
		name += ".exe"
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error: %v", path, err)
	}
	return path
}

func writeProviderSmokeScript(t *testing.T, dir, name string) string {
	t.Helper()
	if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
		name += ".cmd"
	}
	path := filepath.Join(dir, name)
	body := "#!/bin/sh\necho provider-smoke-ok\n"
	if runtime.GOOS == "windows" {
		body = "@echo off\r\necho provider-smoke-ok\r\nexit /b 0\r\n"
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error: %v", path, err)
	}
	return path
}

func writeProviderCodexModelSmokeScript(t *testing.T, dir, name, unsupportedModel string) string {
	t.Helper()
	if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
		name += ".cmd"
	}
	path := filepath.Join(dir, name)
	body := fmt.Sprintf(`#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "%s" ]; then
    echo "unsupported model %s"
    exit 1
  fi
done
echo provider-smoke-ok
exit 0
`, unsupportedModel, unsupportedModel)
	if runtime.GOOS == "windows" {
		body = fmt.Sprintf(`@echo off
:loop
if "%%~1"=="" goto ok
if "%%~1"=="%s" (
  echo unsupported model %s
  exit /b 1
)
shift
goto loop
:ok
echo provider-smoke-ok
exit /b 0
`, unsupportedModel, unsupportedModel)
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error: %v", path, err)
	}
	return path
}

func writeProviderCodexTransientModelSmokeScript(t *testing.T, dir, name string) string {
	t.Helper()
	if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
		name += ".cmd"
	}
	path := filepath.Join(dir, name)
	body := `#!/bin/sh
count_file="$(dirname "$0")/codex-smoke-count.txt"
count=0
if [ -f "$count_file" ]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
if [ "$count" -eq 1 ]; then
  echo "temporary gateway timeout"
  exit 1
fi
echo provider-smoke-ok
exit 0
`
	if runtime.GOOS == "windows" {
		body = `@echo off
set "COUNT_FILE=%~dp0codex-smoke-count.txt"
if exist "%COUNT_FILE%" (
  set /p COUNT=<"%COUNT_FILE%"
) else (
  set COUNT=0
)
set /a COUNT=%COUNT%+1
> "%COUNT_FILE%" echo %COUNT%
if "%COUNT%"=="1" (
  echo temporary gateway timeout
  exit /b 1
)
echo provider-smoke-ok
exit /b 0
`
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error: %v", path, err)
	}
	return path
}

func TestManagerProviderCodexModelSmokeCachedRetriesTransientFailure(t *testing.T) {
	dir := t.TempDir()
	executable := writeProviderCodexTransientModelSmokeScript(t, dir, "codex")
	origDelay := managerProviderModelSmokeRetryDelay
	managerProviderModelSmokeRetryDelay = 0
	t.Cleanup(func() {
		managerProviderModelSmokeRetryDelay = origDelay
	})

	cache := map[string]managerProviderSmokeResult{}
	err := managerProviderCodexModelSmokeCached(
		ManagedAgentRecord{AgentID: "delta", Workdir: t.TempDir()},
		executable,
		nil,
		os.Environ(),
		"gpt-5.4-mini",
		cache,
	)
	if err != nil {
		t.Fatalf("expected transient smoke retry to pass, got %v", err)
	}
	raw, readErr := os.ReadFile(filepath.Join(dir, "codex-smoke-count.txt"))
	if readErr != nil {
		t.Fatalf("expected smoke counter file: %v", readErr)
	}
	if got := strings.TrimSpace(string(raw)); got != "2" {
		t.Fatalf("expected two smoke attempts, got %q", got)
	}
	if cached := cache["codex\x00"+executable+"\x00\x00gpt-5.4-mini"]; cached.Status != "passed" {
		t.Fatalf("expected successful smoke to be cached as passed, got %+v", cached)
	}
}

func TestManagerProviderCodexModelSmokeRetryableKeepsModelAdmissionFailClosed(t *testing.T) {
	for _, msg := range []string{
		"codex bridge model smoke failed for model gpt-5.4: exit status 1: unsupported model gpt-5.4",
		"codex bridge model smoke failed for model gpt-5.5: exit status 1: requires a newer version of Codex",
		"codex bridge model smoke failed for model x: exit status 1: you do not have access to model x",
	} {
		if managerProviderCodexModelSmokeRetryable(fmt.Errorf("%s", msg)) {
			t.Fatalf("expected deterministic model admission failure to stay fail-closed: %s", msg)
		}
	}
	if !managerProviderCodexModelSmokeRetryable(fmt.Errorf("codex bridge model smoke timed out for model gpt-5.4-mini")) {
		t.Fatalf("expected timeout smoke failure to be retryable")
	}
}

func TestManagerWebCurrentSubstrateFailsOnExecutableHashMismatchAndDirtyRepo(t *testing.T) {
	setManagerWebTestHome(t)

	current := filepath.Join(t.TempDir(), "rhizome-bot-current.exe")
	installed := filepath.Join(t.TempDir(), "rhizome-bot-installed.exe")
	if err := os.WriteFile(current, []byte("current binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(current) error: %v", err)
	}
	if err := os.WriteFile(installed, []byte("installed binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(installed) error: %v", err)
	}
	repoRoot := t.TempDir()

	origExecutable := managerSubstrateExecutableFunc
	origInstalled := managerSubstrateInstalledExecutableFunc
	origRepoRoot := managerSubstrateRepoRootFunc
	origCommand := managerSubstrateCommandOutputFunc
	origBuild := managerSubstrateRuntimeBuildFunc
	defer func() {
		managerSubstrateExecutableFunc = origExecutable
		managerSubstrateInstalledExecutableFunc = origInstalled
		managerSubstrateRepoRootFunc = origRepoRoot
		managerSubstrateCommandOutputFunc = origCommand
		managerSubstrateRuntimeBuildFunc = origBuild
	}()
	managerSubstrateExecutableFunc = func() (string, error) { return current, nil }
	managerSubstrateInstalledExecutableFunc = func() string { return installed }
	managerSubstrateRepoRootFunc = func() string { return repoRoot }
	managerSubstrateCommandOutputFunc = func(_ string, _ string, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "rev-parse HEAD"):
			return "abc123\n", nil
		case strings.Contains(joined, "rev-parse --abbrev-ref HEAD"):
			return "main\n", nil
		case strings.Contains(joined, "status --porcelain"):
			return " M agent/manager_substrate.go\n", nil
		default:
			return "", errors.New("unexpected command")
		}
	}
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "abc123"}
	}

	status := managerWebCurrentSubstrate(BotRegistry{}, ProviderRegistry{}, nil)
	if status.Status != "fail" {
		t.Fatalf("expected substrate fail, got %+v", status)
	}
	for _, want := range []string{"installed_executable_hash_mismatch", "repository_dirty:1"} {
		if !containsTrimmedString(status.Reasons, want) {
			t.Fatalf("expected substrate reason %q, got %+v", want, status.Reasons)
		}
	}
	if status.Diagnostics.Status != "fail" || status.Diagnostics.ReasonCount < 2 {
		t.Fatalf("expected failing diagnostics summary, got %+v", status.Diagnostics)
	}
}

func TestManagerWebCurrentSubstrateBlocksMissingBuildRevision(t *testing.T) {
	setManagerWebTestHome(t)
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	origBuild := managerSubstrateRuntimeBuildFunc
	t.Cleanup(func() { managerSubstrateRuntimeBuildFunc = origBuild })
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true}
	}

	status := managerWebCurrentSubstrate(BotRegistry{}, ProviderRegistry{}, nil)
	if status.Status != "fail" || !containsTrimmedString(status.Reasons, "build_revision_missing") {
		t.Fatalf("expected missing build revision to block substrate readiness, got %+v", status)
	}
}

func TestManagerWebCurrentSubstrateAllowsSubmoduleBuildRevision(t *testing.T) {
	setManagerWebTestHome(t)
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	repoRoot := t.TempDir()

	origExecutable := managerSubstrateExecutableFunc
	origInstalled := managerSubstrateInstalledExecutableFunc
	origRepoRoot := managerSubstrateRepoRootFunc
	origCommand := managerSubstrateCommandOutputFunc
	origBuild := managerSubstrateRuntimeBuildFunc
	t.Cleanup(func() {
		managerSubstrateExecutableFunc = origExecutable
		managerSubstrateInstalledExecutableFunc = origInstalled
		managerSubstrateRepoRootFunc = origRepoRoot
		managerSubstrateCommandOutputFunc = origCommand
		managerSubstrateRuntimeBuildFunc = origBuild
	})
	managerSubstrateExecutableFunc = func() (string, error) { return bin, nil }
	managerSubstrateInstalledExecutableFunc = func() string { return bin }
	managerSubstrateRepoRootFunc = func() string { return repoRoot }
	managerSubstrateCommandOutputFunc = func(_ string, _ string, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "rev-parse HEAD"):
			return "root-rev\n", nil
		case strings.Contains(joined, "rev-parse --abbrev-ref HEAD"):
			return "main\n", nil
		case strings.Contains(joined, "status --porcelain"):
			return "", nil
		case strings.Contains(joined, "ls-tree -r HEAD"):
			return "160000 commit bot-rev\tagent\n", nil
		case strings.Contains(joined, "submodule status --recursive"):
			return " bot-rev agent (heads/main)\n", nil
		default:
			return "", errors.New("unexpected command")
		}
	}
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "bot-rev"}
	}

	status := managerWebCurrentSubstrate(BotRegistry{}, ProviderRegistry{}, nil)
	if containsTrimmedString(status.Reasons, "build_revision_mismatch") {
		t.Fatalf("expected submodule bot build revision to satisfy substrate identity, got %+v", status)
	}
}

func TestManagedRunPreflightBlocksHashMismatchAndPersistsReceipt(t *testing.T) {
	setManagerWebTestHome(t)
	current := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-current", "current binary")
	installed := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-installed", "installed binary")
	installCleanManagerSubstrateStubsForTest(t, current, installed)
	workdir := t.TempDir()

	_, err := admitManagedRunStart(ManagedAgentRecord{
		AgentID: "lyrica",
		Workdir: workdir,
	})
	if !isManagedRunPreflightBlockedError(err) {
		t.Fatalf("expected managed preflight blocker, got %v", err)
	}
	if !strings.Contains(err.Error(), "installed_executable_hash_mismatch") {
		t.Fatalf("expected hash mismatch blocker, got %v", err)
	}
	raw, readErr := os.ReadFile(managedRunSubstrateAdmissionReceiptPath(workdir))
	if readErr != nil {
		t.Fatalf("expected substrate admission receipt: %v", readErr)
	}
	var receipt managedRunSubstrateAdmissionReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatalf("decode receipt: %v", err)
	}
	if receipt.Contract != managedRunSubstrateAdmissionContract || receipt.Admission != "blocked" {
		t.Fatalf("expected blocked substrate admission receipt, got %+v", receipt)
	}
	if !containsTrimmedString(receipt.Reasons, "installed_executable_hash_mismatch") {
		t.Fatalf("expected receipt to preserve hash mismatch reason, got %+v", receipt.Reasons)
	}
}

func TestManagedRunPreflightBlocksCleanStopResidual(t *testing.T) {
	setManagerWebTestHome(t)
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	workdir := t.TempDir()
	if err := requestManagedAgentGracefulStop(workdir); err != nil {
		t.Fatalf("requestManagedAgentGracefulStop() error: %v", err)
	}

	_, err := ManagedRunPreflight(ManagedAgentRecord{AgentID: "lyrica", Workdir: workdir})
	if !isManagedRunPreflightBlockedError(err) || !strings.Contains(err.Error(), "clean_stop:blocked") {
		t.Fatalf("expected clean-stop residual to block managed preflight, got %v", err)
	}
}

func TestManagedRunPreflightFixtureWaivesOnlyCleanStopResidual(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv(managedRunFixtureAllowCleanStopEnv, "1")
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	workdir := t.TempDir()
	if err := requestManagedAgentGracefulStop(workdir); err != nil {
		t.Fatalf("requestManagedAgentGracefulStop() error: %v", err)
	}

	result, err := ManagedRunPreflight(ManagedAgentRecord{AgentID: "lyrica", Workdir: workdir})
	if err != nil {
		t.Fatalf("expected fixture clean-stop waiver to admit, got %v; receipt=%+v", err, result.Receipt)
	}
	if result.Receipt.Admission != "admitted" {
		t.Fatalf("expected admitted receipt, got %+v", result.Receipt)
	}
	if containsTrimmedString(result.Receipt.Reasons, "clean_stop:blocked") {
		t.Fatalf("expected clean-stop reason to be waived from admission reasons, got %+v", result.Receipt.Reasons)
	}
	if !containsTrimmedString(result.Receipt.WaivedReasons, "clean_stop:blocked") {
		t.Fatalf("expected receipt to preserve waived clean-stop evidence, got %+v", result.Receipt.WaivedReasons)
	}
	if !containsTrimmedString(result.Receipt.Substrate.Reasons, "clean_stop:blocked") {
		t.Fatalf("expected raw substrate evidence to keep clean-stop failure, got %+v", result.Receipt.Substrate.Reasons)
	}
}

func TestManagedRunPreflightFixtureWaiverKeepsOtherBlockersFailClosed(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv(managedRunFixtureAllowCleanStopEnv, "1")
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	current := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-current", "current binary")
	installed := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-installed", "installed binary")
	installCleanManagerSubstrateStubsForTest(t, current, installed)
	workdir := t.TempDir()
	if err := requestManagedAgentGracefulStop(workdir); err != nil {
		t.Fatalf("requestManagedAgentGracefulStop() error: %v", err)
	}

	result, err := ManagedRunPreflight(ManagedAgentRecord{AgentID: "lyrica", Workdir: workdir})
	if !isManagedRunPreflightBlockedError(err) {
		t.Fatalf("expected non-clean-stop blocker to stay fail-closed, got %v; receipt=%+v", err, result.Receipt)
	}
	if !strings.Contains(err.Error(), "installed_executable_hash_mismatch") {
		t.Fatalf("expected hash mismatch blocker, got %v", err)
	}
	if strings.Contains(err.Error(), "clean_stop:blocked") {
		t.Fatalf("expected waived clean-stop blocker to stay out of admission error, got %v", err)
	}
	if !containsTrimmedString(result.Receipt.WaivedReasons, "clean_stop:blocked") {
		t.Fatalf("expected receipt to preserve waived clean-stop evidence, got %+v", result.Receipt.WaivedReasons)
	}
	if !containsTrimmedString(result.Receipt.Reasons, "installed_executable_hash_mismatch") {
		t.Fatalf("expected receipt to preserve hash mismatch admission blocker, got %+v", result.Receipt.Reasons)
	}
}

func TestManagedRunPreflightResumeWaivesOnlyLocalDirtyProjectCheckout(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	workdir := t.TempDir()
	checkout := filepath.Join(workdir, "project-checkouts", "dirty-checkout")
	if err := os.MkdirAll(checkout, 0o755); err != nil {
		t.Fatalf("MkdirAll(checkout) error: %v", err)
	}
	managerSubstrateCommandOutputFunc = func(dir string, _ string, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "rev-parse --show-toplevel"):
			return strings.TrimSpace(dir) + "\n", nil
		case strings.Contains(joined, "rev-parse HEAD"):
			return "abc123\n", nil
		case strings.Contains(joined, "rev-parse --abbrev-ref HEAD"):
			return "main\n", nil
		case strings.Contains(joined, "status --porcelain"):
			if filepath.Clean(dir) == filepath.Clean(checkout) {
				return " M dirty.txt\n", nil
			}
			return "", nil
		default:
			return "", errors.New("unexpected command")
		}
	}
	record := ManagedAgentRecord{AgentID: "theta", Workdir: workdir, WorkspaceID: "ws-clean"}

	blocked, err := managedRunPreflightWithOptions(record, managedRunPreflightOptions{})
	if !isManagedRunPreflightBlockedError(err) || !strings.Contains(err.Error(), "clean_stop:blocked") {
		t.Fatalf("expected dirty checkout to block without resume waiver, got %v; receipt=%+v", err, blocked.Receipt)
	}

	result, err := managedRunPreflightWithOptions(record, managedRunPreflightOptions{
		ResumeContinuationWaiver: managedRunResumeContinuationWaiver{AllowDirtyProjectCheckout: true},
	})
	if err != nil {
		t.Fatalf("expected resume dirty-checkout waiver to admit, got %v; receipt=%+v", err, result.Receipt)
	}
	if result.Receipt.Admission != "admitted" {
		t.Fatalf("expected admitted receipt, got %+v", result.Receipt)
	}
	if containsTrimmedString(result.Receipt.Reasons, "clean_stop:blocked") {
		t.Fatalf("expected clean-stop reason to be waived from admission reasons, got %+v", result.Receipt.Reasons)
	}
	if !containsTrimmedString(result.Receipt.WaivedReasons, "clean_stop:blocked") {
		t.Fatalf("expected receipt to preserve waived clean-stop evidence, got %+v", result.Receipt.WaivedReasons)
	}
	if !containsTrimmedString(result.Receipt.Substrate.Reasons, "clean_stop:blocked") {
		t.Fatalf("expected raw substrate reasons to keep clean-stop evidence, got %+v", result.Receipt.Substrate.Reasons)
	}
	if !strings.Contains(result.Receipt.Substrate.CleanStop.Reason, "theta:local_dirty_project_checkout:dirty-checkout") {
		t.Fatalf("expected raw clean-stop reason to name dirty checkout, got %+v", result.Receipt.Substrate.CleanStop)
	}
}

func TestManagedRunPreflightResumeDirtyCheckoutWaiverKeepsMixedCleanStopFailClosed(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "theta:local_dirty_project_checkout:dirty-checkout; theta:stop_request_residual",
	}
	reasons, waived := managedRunApplyResumeDirtyProjectCheckoutWaiver([]string{"clean_stop:blocked"}, cleanStop, true)
	if !containsTrimmedString(reasons, "clean_stop:blocked") || len(waived) != 0 {
		t.Fatalf("expected mixed clean-stop reasons to stay fail-closed, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightRetriesTransientRepositoryDeadline(t *testing.T) {
	t.Setenv(managedRunFixtureAllowCleanStopEnv, "1")
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)

	oldDelay := managerSubstrateRepositoryCommandRetryDelay
	managerSubstrateRepositoryCommandRetryDelay = 0
	t.Cleanup(func() { managerSubstrateRepositoryCommandRetryDelay = oldDelay })

	var statusAttempts int
	managerSubstrateCommandOutputFunc = func(_ string, _ string, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "rev-parse HEAD"):
			return "abc123\n", nil
		case strings.Contains(joined, "rev-parse --abbrev-ref HEAD"):
			return "main\n", nil
		case strings.Contains(joined, "status --porcelain"):
			statusAttempts++
			if statusAttempts == 1 {
				return "", context.DeadlineExceeded
			}
			return "", nil
		default:
			return "", errors.New("unexpected command")
		}
	}

	result, err := managedRunPreflightWithOptions(ManagedAgentRecord{AgentID: "eta", Workdir: t.TempDir()}, managedRunPreflightOptions{})
	if err != nil {
		t.Fatalf("expected transient repository deadline to be retried and admitted, got %v; receipt=%+v", err, result.Receipt)
	}
	if statusAttempts != 2 {
		t.Fatalf("expected one repository status retry, got attempts=%d", statusAttempts)
	}
	if result.Receipt.Admission != "admitted" {
		t.Fatalf("expected admitted receipt after retry, got %+v", result.Receipt)
	}
}

func TestManagedRunPreflightResumeWaivesPatchQueueContinuation(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "alpha:remote_patch_queue_live:queue-1/item-1:PROPOSED; beta:remote_patch_queue_claim:queue-2/item-2:CLAIMED; gamma:remote_patch_queue_live_claimed_by:queue-3/item-3:CLAIMED:kappa; theta:local_dirty_project_checkout:dirty-checkout",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunResumeContinuationWaiver{
		AllowDirtyProjectCheckout: true,
		AllowLivePatchQueue:       true,
	})
	if len(reasons) != 0 || !containsTrimmedString(waived, "clean_stop:blocked") {
		t.Fatalf("expected resume patch queue continuation to be waived, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumeWaivesRecoverableAgentRequestContinuation(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "epsilon:remote_open_agent_request:areq-timeout:TIMEOUT; zeta:remote_open_agent_request:areq-pending:PENDING; theta:local_dirty_project_checkout:dirty-checkout",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunResumeContinuationWaiver{
		AllowDirtyProjectCheckout: true,
		AllowAgentRequests:        true,
	})
	if len(reasons) != 0 || !containsTrimmedString(waived, "clean_stop:blocked") {
		t.Fatalf("expected recoverable agent requests to be waived for resume, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumeWaivesLiveProjectBranchContinuation(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "zeta:remote_project_branch_live:branch-1:READY_FOR_REVIEW:gamma:task=task-project:claim=task-project; alpha:remote_project_branch_live:branch-2:ACTIVE:delta:task=task-build; theta:local_dirty_project_checkout:dirty-checkout",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunResumeContinuationWaiver{
		AllowDirtyProjectCheckout: true,
		AllowLiveProjectBranches:  true,
	})
	if len(reasons) != 0 || !containsTrimmedString(waived, "clean_stop:blocked") {
		t.Fatalf("expected live project branch continuation to be waived, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumeWaivesRemoteProjectCheckoutContinuation(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "alpha:remote_active_project_checkout:projcheckout-1; beta:remote_dirty_project_checkout:projcheckout-2; theta:local_dirty_project_checkout:dirty-checkout",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunResumeContinuationWaiver{
		AllowDirtyProjectCheckout: true,
	})
	if len(reasons) != 0 || !containsTrimmedString(waived, "clean_stop:blocked") {
		t.Fatalf("expected remote project checkout continuation to be waived, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumeWaivesPendingRequestResumeContinuation(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "delta:remote_pending_trigger:request_resume; kappa:remote_pending_trigger:request_resume; theta:local_dirty_project_checkout:dirty-checkout",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunResumeContinuationWaiver{
		AllowDirtyProjectCheckout:  true,
		AllowPendingResumeTriggers: true,
	})
	if len(reasons) != 0 || !containsTrimmedString(waived, "clean_stop:blocked") {
		t.Fatalf("expected pending request_resume continuation to be waived, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumeProjectBranchWaiverKeepsUnknownStatusFailClosed(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "zeta:remote_project_branch_live:branch-1:RUNNING:gamma:task=task-project:claim=task-project",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunFullResumeContinuationWaiver())
	if !containsTrimmedString(reasons, "clean_stop:blocked") || len(waived) != 0 {
		t.Fatalf("expected unknown live project branch status to stay fail-closed, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumePendingTriggerWaiverKeepsOtherTriggersFailClosed(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "kappa:remote_pending_trigger:runtime_switch_task",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunFullResumeContinuationWaiver())
	if !containsTrimmedString(reasons, "clean_stop:blocked") || len(waived) != 0 {
		t.Fatalf("expected non-request_resume pending trigger to stay fail-closed, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumeAgentRequestWaiverKeepsProcessingFailClosed(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "epsilon:remote_open_agent_request:areq-processing:PROCESSING",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunFullResumeContinuationWaiver())
	if !containsTrimmedString(reasons, "clean_stop:blocked") || len(waived) != 0 {
		t.Fatalf("expected processing agent request to stay fail-closed, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightResumePatchQueueWaiverKeepsOtherResidualsFailClosed(t *testing.T) {
	cleanStop := managerCleanStopReadiness{
		Status: "blocked",
		Reason: "alpha:remote_patch_queue_live:queue-1/item-1:PROPOSED; theta:remote_active_session:sess-1",
	}
	reasons, waived := managedRunApplyResumeContinuationWaiver([]string{"clean_stop:blocked"}, cleanStop, managedRunFullResumeContinuationWaiver())
	if !containsTrimmedString(reasons, "clean_stop:blocked") || len(waived) != 0 {
		t.Fatalf("expected mixed non-continuation residual to stay fail-closed, reasons=%+v waived=%+v", reasons, waived)
	}
}

func TestManagedRunPreflightOptionsResumeContinuationWaiverObjectMergesLegacyFields(t *testing.T) {
	options := managedRunPreflightOptions{
		ResumeContinuationWaiver:        managedRunResumeContinuationWaiver{AllowDirtyProjectCheckout: true, AllowLiveProjectBranches: true, AllowPendingResumeTriggers: true},
		AllowResumeLivePatchQueue:       true,
		AllowResumeAgentRequests:        true,
		AllowResumeDirtyProjectCheckout: false,
	}
	waiver := options.resumeContinuationWaiver()
	if !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
		t.Fatalf("expected resume continuation waiver object to merge all compatible inputs, got %+v", waiver)
	}
	if !options.hasOverrides() {
		t.Fatalf("resume continuation waiver should count as an admission override")
	}
}

func TestManagedRunPreflightWaivesRepositoryDirtyOnlyForExactAllowlist(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv(managedRunFixtureAllowCleanStopEnv, "1")
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "abc123", VCSModified: true}
	}
	managerSubstrateCommandOutputFunc = func(_ string, _ string, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "rev-parse HEAD"):
			return "abc123\n", nil
		case strings.Contains(joined, "rev-parse --abbrev-ref HEAD"):
			return "main\n", nil
		case strings.Contains(joined, "status --porcelain"):
			return " M runs/signal01-rq/README.md\n?? runs/signal01-rq/postmortems/round-09.md\n", nil
		default:
			return "", errors.New("unexpected command")
		}
	}
	record := ManagedAgentRecord{AgentID: "lyrica", Workdir: t.TempDir()}

	result, err := managedRunPreflightWithOptions(record, managedRunPreflightOptions{
		RepositoryDirtyPathAllowlist: []string{
			"runs/signal01-rq/README.md",
			"runs/signal01-rq/postmortems/round-09.md",
		},
	})
	if err != nil {
		t.Fatalf("expected exact dirty allowlist to admit, got %v; receipt=%+v", err, result.Receipt)
	}
	if containsTrimmedString(result.Receipt.Reasons, "repository_dirty:2") {
		t.Fatalf("expected dirty reason to be waived from admission reasons, got %+v", result.Receipt.Reasons)
	}
	if containsTrimmedString(result.Receipt.Reasons, "build_vcs_modified") {
		t.Fatalf("expected dirty-build reason to be waived with exact dirty allowlist, got %+v", result.Receipt.Reasons)
	}
	if !containsTrimmedString(result.Receipt.WaivedReasons, "repository_dirty:2") {
		t.Fatalf("expected receipt to preserve waived dirty reason, got %+v", result.Receipt.WaivedReasons)
	}
	if !containsTrimmedString(result.Receipt.WaivedReasons, "build_vcs_modified") {
		t.Fatalf("expected receipt to preserve waived dirty-build reason, got %+v", result.Receipt.WaivedReasons)
	}
	if !containsTrimmedString(result.Receipt.Substrate.Reasons, "repository_dirty:2") {
		t.Fatalf("expected raw substrate evidence to keep dirty failure, got %+v", result.Receipt.Substrate.Reasons)
	}
	if !containsTrimmedString(result.Receipt.Substrate.Reasons, "build_vcs_modified") {
		t.Fatalf("expected raw substrate evidence to keep dirty-build failure, got %+v", result.Receipt.Substrate.Reasons)
	}

	result, err = managedRunPreflightWithOptions(record, managedRunPreflightOptions{
		RepositoryDirtyPathAllowlist: []string{"runs/signal01-rq/README.md"},
	})
	if !isManagedRunPreflightBlockedError(err) {
		t.Fatalf("expected incomplete dirty allowlist to stay blocked, got %v; receipt=%+v", err, result.Receipt)
	}
	if !containsTrimmedString(result.Receipt.Reasons, "repository_dirty:2") {
		t.Fatalf("expected dirty blocker to stay in admission reasons, got %+v", result.Receipt.Reasons)
	}
	if !containsTrimmedString(result.Receipt.Reasons, "build_vcs_modified") {
		t.Fatalf("expected dirty-build blocker to stay in admission reasons, got %+v", result.Receipt.Reasons)
	}
}

func TestManagedRunPreflightMaterializesToolBundlesBeforeReadiness(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	prependFakeBrowserToPath(t)
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	workdir := t.TempDir()

	result, err := ManagedRunPreflight(ManagedAgentRecord{
		AgentID:     "visual-agent",
		Workdir:     workdir,
		WorkspaceID: "ws-visual",
		ProviderID:  "fake",
		LLMBackend:  llmBackendFake,
		ToolBundles: []string{"browser_visual_probe"},
	})
	if err != nil {
		t.Fatalf("expected materialized browser probe to pass preflight, got %v; receipt=%+v", err, result.Receipt)
	}
	manifestPath := filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_visual_probe", installedToolBundleManifestName)
	if !pathExists(manifestPath) {
		t.Fatalf("expected preflight to materialize browser_visual_probe before readiness check")
	}
	if len(result.Receipt.Substrate.ToolBundles) != 1 || result.Receipt.Substrate.ToolBundles[0].Status != "ready" {
		t.Fatalf("expected materialized tool bundle readiness, got %+v", result.Receipt.Substrate.ToolBundles)
	}
	if result.Receipt.Substrate.Browser.Status != "ready" {
		t.Fatalf("expected browser readiness from materialized visual probe, got %+v", result.Receipt.Substrate.Browser)
	}
}

func TestManagerCleanStopReadinessBlocksRemoteResidualsWithoutClaimingRequests(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		calls[req.Method]++
		switch req.Method {
		case "workspace.sessions.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"sessions": []map[string]any{}})
		case "workspace.tasks.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"tasks": []map[string]any{{
				"task_id":    "task-project",
				"status":     "PENDING",
				"project_id": "project-1",
			}, {
				"task_id":        "task-blocked-claim-only",
				"status":         "RUNNING",
				"claim_agent_id": "agent-clean",
				"claim_status":   "BLOCKED",
			}}})
		case "project.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"projects": []map[string]any{
				{"project_id": "project-1", "workspace_id": "ws-clean", "status": "ACTIVE", "title": "Project 1"},
				{"project_id": "project-2", "workspace_id": "ws-clean", "status": "ACTIVE", "title": "Project 2"},
			}})
		case "project.coordination.get":
			params, _ := req.Params.(map[string]any)
			projectID, _ := params["project_id"].(string)
			if projectID == "project-2" {
				writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"coordination": map[string]any{
					"project": map[string]any{"project_id": "project-2"},
					"patch_queue_items": []map[string]any{{
						"queue_id": "queue-2",
						"item_id":  "item-2",
						"state":    "PROPOSED",
					}},
					"checkouts": []map[string]any{{
						"checkout_id": "checkout-2",
						"agent_id":    "other-agent",
						"status":      "ACTIVE",
						"dirty_state": "dirty",
					}},
					"branches": []map[string]any{{
						"branch_id":      "branch-2",
						"branch_name":    "agent/other/project-1/stale",
						"agent_id":       "other-agent",
						"status":         "READY_FOR_REVIEW",
						"active_task_id": "task-other",
					}, {
						"branch_id":   "branch-ready-terminal-lineage",
						"branch_name": "agent/other/project-1/review-ready",
						"agent_id":    "other-agent",
						"status":      "READY_FOR_REVIEW",
					}},
				}})
				return
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"coordination": map[string]any{
				"project": map[string]any{"project_id": "project-1"},
				"patch_queue_items": []map[string]any{{
					"queue_id":    "queue-1",
					"item_id":     "item-1",
					"state":       "CLAIMED",
					"claimed_by":  "agent-clean",
					"claim_token": "claim-token",
				}},
				"checkouts": []map[string]any{{
					"checkout_id":     "checkout-1",
					"agent_id":        "agent-clean",
					"status":          "ACTIVE",
					"dirty_state":     "dirty",
					"active_task_id":  "task-project",
					"active_claim_id": "task-project",
				}},
				"branches": []map[string]any{{
					"branch_id":       "branch-1",
					"branch_name":     "agent/agent-clean/project-1/live",
					"agent_id":        "agent-clean",
					"status":          "READY_FOR_REVIEW",
					"active_task_id":  "task-project",
					"active_claim_id": "task-project",
				}},
			}})
		case "agent.request.open.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"requests": []map[string]any{{
				"request_id":   "req-1",
				"workspace_id": "ws-clean",
				"to_agent_id":  "agent-clean",
				"status":       "PENDING",
				"method":       "delegate_task",
			}, {
				"request_id":   "req-timeout",
				"workspace_id": "ws-clean",
				"to_agent_id":  "agent-clean",
				"status":       "TIMEOUT",
				"method":       "delegate_task",
			}}})
		case "agent.state.get":
			raw, _ := json.Marshal(RuntimeScratchState{ActiveTaskID: "task-project", ActiveSessionID: "sess-1", ActiveRunID: "run-1"})
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"value": string(raw)})
		case "workspace.events.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"items": []map[string]any{}, "count": 0})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-clean",
		WorkspaceID: "ws-clean",
		AgentID:     "agent-clean",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerCleanStopReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-clean",
		Workdir:     workdir,
		WorkspaceID: "ws-clean",
	}}})
	for _, method := range []string{"agent.request.open.list", "project.list", "project.coordination.get"} {
		if calls[method] == 0 {
			t.Fatalf("expected clean-stop readiness to call %s, calls=%+v", method, calls)
		}
	}
	for _, want := range []string{"remote_open_agent_request:req-1:PENDING", "remote_active_claim:task-blocked-claim-only", "remote_patch_queue_claim:queue-1/item-1:CLAIMED", "remote_patch_queue_live:queue-2/item-2:PROPOSED", "remote_dirty_project_checkout:checkout-1", "remote_dirty_project_checkout:checkout-2", "remote_active_project_checkout:checkout-1", "remote_project_branch_live:branch-1:READY_FOR_REVIEW:agent-clean:task=task-project:claim=task-project", "remote_project_branch_live:branch-2:READY_FOR_REVIEW:other-agent:task=task-other", "remote_scratch_active_binding:task=task-project,session=sess-1,run=run-1"} {
		if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, want) {
			t.Fatalf("expected clean-stop blocker %q, got %+v", want, readiness)
		}
	}
	if strings.Contains(readiness.Reason, "branch-ready-terminal-lineage") {
		t.Fatalf("review-ready branch with no active task/claim refs should not block clean stop: %+v", readiness)
	}
	if strings.Contains(readiness.Reason, "remote_open_agent_request:req-timeout:TIMEOUT") {
		t.Fatalf("timed-out agent request should not block clean stop: %+v", readiness)
	}
}

func TestManagerCleanStopReadinessReportsFutureProjectRootTaskWithoutCoordinationGet(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		calls[req.Method]++
		switch req.Method {
		case "workspace.sessions.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"sessions": []map[string]any{}})
		case "workspace.tasks.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"tasks": []map[string]any{{
				"task_id":               "task-signal01-rq-root-run23",
				"title":                 "Signal-01 rq root bootstrap",
				"description":           "Operator root task should create the project before it owns a project_id.",
				"status":                "PENDING",
				"task_kind":             "COORDINATION",
				"project_lane":          "strategy",
				"project_id":            "signal01-rq-run23",
				"requires_project_gate": false,
			}}})
		case "project.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"projects": []map[string]any{}})
		case "agent.request.open.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"requests": []map[string]any{}})
		case "agent.state.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"value": "{}"})
		case "project.coordination.get":
			t.Fatalf("future bootstrap project_id must not call project.coordination.get")
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-clean",
		WorkspaceID: "ws-clean",
		AgentID:     "agent-clean",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerCleanStopReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-clean",
		Workdir:     workdir,
		WorkspaceID: "ws-clean",
	}}})
	if calls["project.coordination.get"] != 0 {
		t.Fatalf("future bootstrap project id should not be coordinated, calls=%+v", calls)
	}
	want := "remote_bootstrap_task_future_project_id:task-signal01-rq-root-run23:signal01-rq-run23"
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, want) {
		t.Fatalf("expected clean-stop future-project blocker %q, got %+v", want, readiness)
	}
}

func TestManagerCleanStopReadinessReportsUnknownProjectTaskWithoutCoordinationGet(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		calls[req.Method]++
		switch req.Method {
		case "workspace.sessions.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"sessions": []map[string]any{}})
		case "workspace.tasks.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"tasks": []map[string]any{{
				"task_id":               "task-signal01-rq-parser-run23",
				"title":                 "Signal-01 rq parser lane",
				"description":           "Implementation task referring to a project row that should already exist.",
				"status":                "PENDING",
				"task_kind":             "EXECUTION",
				"project_lane":          "implementation",
				"project_id":            "signal01-rq-run23",
				"requires_project_gate": true,
			}}})
		case "project.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"projects": []map[string]any{}})
		case "agent.request.open.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"requests": []map[string]any{}})
		case "agent.state.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"value": "{}"})
		case "project.coordination.get":
			t.Fatalf("unknown project task must not call project.coordination.get")
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-clean",
		WorkspaceID: "ws-clean",
		AgentID:     "agent-clean",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerCleanStopReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-clean",
		Workdir:     workdir,
		WorkspaceID: "ws-clean",
	}}})
	if calls["project.coordination.get"] != 0 {
		t.Fatalf("unknown project id should not be coordinated, calls=%+v", calls)
	}
	want := "remote_task_unknown_project_id:task-signal01-rq-parser-run23:signal01-rq-run23"
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, want) {
		t.Fatalf("expected clean-stop unknown-project blocker %q, got %+v", want, readiness)
	}
}

func TestManagerCleanStopReadinessBlocksUnreconciledPatchQueueIntegrationAdmission(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")

	now := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "workspace.sessions.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"sessions": []map[string]any{}})
		case "workspace.tasks.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"tasks": []map[string]any{}})
		case "project.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"projects": []map[string]any{
				{"project_id": "project-1", "workspace_id": "ws-clean", "status": "ACTIVE", "title": "Project 1"},
			}})
		case "project.coordination.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"coordination": map[string]any{
				"project": map[string]any{"project_id": "project-1"},
				"patch_queue_items": []map[string]any{{
					"queue_id":   "queue-a",
					"item_id":    "item-a",
					"project_id": "project-1",
					"state":      "ACCEPTED",
					"head_sha":   strings.Repeat("a", 40),
				}},
			}})
		case "workspace.events.list":
			params, _ := req.Params.(map[string]any)
			eventType, _ := params["event_type"].(string)
			if eventType == managerPatchQueueIntegrationAdmittedEventType {
				writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"items": []map[string]any{{
					"event_id":     "evt-admitted",
					"workspace_id": "ws-clean",
					"event_type":   managerPatchQueueIntegrationAdmittedEventType,
					"entity_type":  managerPatchQueueIntegrationEntityType,
					"entity_id":    "queue-a/item-a",
					"payload_json": `{"project_id":"project-1","target_branch":"main"}`,
					"created_at":   now.Format(time.RFC3339Nano),
					"ingest_seq":   10,
				}}, "count": 1})
				return
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"items": []map[string]any{}, "count": 0})
		case "agent.request.open.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"requests": []map[string]any{}})
		case "agent.state.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"value": `{}`})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-clean",
		WorkspaceID: "ws-clean",
		AgentID:     "agent-clean",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerCleanStopReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-clean",
		Workdir:     workdir,
		WorkspaceID: "ws-clean",
	}}})
	want := "remote_patch_queue_integration_unreconciled:project-1:queue-a/item-a:main"
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, want) {
		t.Fatalf("expected integration reconcile blocker %q, got %+v", want, readiness)
	}
}

func TestManagerCleanStopReadinessAcceptsReconciledPatchQueueIntegrationAdmission(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")

	now := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "workspace.sessions.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"sessions": []map[string]any{}})
		case "workspace.tasks.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"tasks": []map[string]any{}})
		case "project.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"projects": []map[string]any{
				{"project_id": "project-1", "workspace_id": "ws-clean", "status": "ACTIVE", "title": "Project 1"},
			}})
		case "project.coordination.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"coordination": map[string]any{
				"project": map[string]any{"project_id": "project-1"},
				"patch_queue_items": []map[string]any{{
					"queue_id":   "queue-a",
					"item_id":    "item-a",
					"project_id": "project-1",
					"state":      "ACCEPTED",
					"head_sha":   strings.Repeat("a", 40),
				}},
			}})
		case "workspace.events.list":
			params, _ := req.Params.(map[string]any)
			eventType, _ := params["event_type"].(string)
			switch eventType {
			case managerPatchQueueIntegrationAdmittedEventType:
				writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"items": []map[string]any{{
					"event_id":     "evt-admitted",
					"workspace_id": "ws-clean",
					"event_type":   managerPatchQueueIntegrationAdmittedEventType,
					"entity_type":  managerPatchQueueIntegrationEntityType,
					"entity_id":    "queue-a/item-a",
					"payload_json": `{"project_id":"project-1","target_branch":"main"}`,
					"created_at":   now.Format(time.RFC3339Nano),
					"ingest_seq":   10,
				}}, "count": 1})
			case managerPatchQueueIntegratedEventType:
				writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"items": []map[string]any{{
					"event_id":     "evt-integrated",
					"workspace_id": "ws-clean",
					"event_type":   managerPatchQueueIntegratedEventType,
					"entity_type":  managerPatchQueueIntegrationEntityType,
					"entity_id":    "queue-a/item-a",
					"created_at":   now.Add(time.Second).Format(time.RFC3339Nano),
					"ingest_seq":   11,
				}}, "count": 1})
			default:
				writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"items": []map[string]any{}, "count": 0})
			}
		case "agent.request.open.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"requests": []map[string]any{}})
		case "agent.state.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"value": `{}`})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-clean",
		WorkspaceID: "ws-clean",
		AgentID:     "agent-clean",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerCleanStopReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-clean",
		Workdir:     workdir,
		WorkspaceID: "ws-clean",
	}}})
	if readiness.Status != "ready" || strings.Contains(readiness.Reason, "remote_patch_queue_integration_unreconciled") {
		t.Fatalf("expected reconciled admission to pass clean-stop readiness, got %+v", readiness)
	}
}

func TestManagerCleanStopReadinessIgnoresIntegratedPatchQueueItems(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "workspace.sessions.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"sessions": []map[string]any{}})
		case "workspace.tasks.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"tasks": []map[string]any{}})
		case "project.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"projects": []map[string]any{
				{"project_id": "project-1", "workspace_id": "ws-clean", "status": "ACTIVE", "title": "Project 1"},
			}})
		case "project.coordination.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"coordination": map[string]any{
				"project": map[string]any{"project_id": "project-1"},
				"patch_queue_items": []map[string]any{{
					"queue_id":   "queue-a",
					"item_id":    "item-a",
					"project_id": "project-1",
					"state":      "INTEGRATED",
					"head_sha":   strings.Repeat("a", 40),
				}},
			}})
		case "workspace.events.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"items": []map[string]any{}, "count": 0})
		case "agent.request.open.list":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"requests": []map[string]any{}})
		case "agent.state.get":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"value": `{}`})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-clean",
		WorkspaceID: "ws-clean",
		AgentID:     "agent-clean",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerCleanStopReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-clean",
		Workdir:     workdir,
		WorkspaceID: "ws-clean",
	}}})
	if readiness.Status != "ready" || strings.Contains(readiness.Reason, "remote_patch_queue_live") {
		t.Fatalf("expected INTEGRATED patch queue item to pass clean-stop readiness, got %+v", readiness)
	}
}

func TestManagerCleanStopReadinessBlocksLocalDirtyProjectCheckout(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	workdir := t.TempDir()
	checkout := filepath.Join(workdir, "project-checkouts", "dirty-checkout")
	if err := os.MkdirAll(checkout, 0o755); err != nil {
		t.Fatalf("MkdirAll(checkout) error: %v", err)
	}
	gitOutput(t, checkout, "init")
	if err := os.WriteFile(filepath.Join(checkout, "dirty.txt"), []byte("dirty"), 0o644); err != nil {
		t.Fatalf("WriteFile(dirty) error: %v", err)
	}

	readiness := managerCleanStopReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-clean",
		Workdir:     workdir,
		WorkspaceID: "ws-clean",
	}}})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "local_dirty_project_checkout:dirty-checkout") {
		t.Fatalf("expected local dirty checkout blocker, got %+v", readiness)
	}
}

func TestManagerCleanStopReadinessChecksRemoteRosterInParallel(t *testing.T) {
	setManagerWebTestHome(t)
	origRemote := managerRemoteCleanStopBlockersFunc
	t.Cleanup(func() { managerRemoteCleanStopBlockersFunc = origRemote })

	var active int32
	var maxActive int32
	managerRemoteCleanStopBlockersFunc = func(record ManagedAgentRecord) ([]string, []string) {
		current := atomic.AddInt32(&active, 1)
		for {
			previous := atomic.LoadInt32(&maxActive)
			if current <= previous || atomic.CompareAndSwapInt32(&maxActive, previous, current) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return []string{record.AgentID + ":remote_active_session:sess-" + record.AgentID}, []string{record.AgentID}
	}

	registry := BotRegistry{Agents: []ManagedAgentRecord{
		{AgentID: "alpha", Workdir: t.TempDir()},
		{AgentID: "beta", Workdir: t.TempDir()},
		{AgentID: "zeta", Workdir: t.TempDir()},
	}}
	readiness := managerCleanStopReadinessForRoster(registry)
	if atomic.LoadInt32(&maxActive) < 2 {
		t.Fatalf("expected remote clean-stop checks to overlap, max concurrency=%d", maxActive)
	}
	if readiness.Status != "blocked" {
		t.Fatalf("expected fake remote blockers to keep clean-stop blocked, got %+v", readiness)
	}
	for _, want := range []string{
		"alpha:remote_active_session:sess-alpha",
		"beta:remote_active_session:sess-beta",
		"zeta:remote_active_session:sess-zeta",
	} {
		if !strings.Contains(readiness.Reason, want) {
			t.Fatalf("expected blocker %q to be preserved, got %+v", want, readiness)
		}
	}
}

func TestManagerCleanStopReadinessTreatsTransientCheckFailuresAsDegraded(t *testing.T) {
	readiness := managerCleanStopReadinessFromBlockers([]string{
		"alpha:remote_active_session_check_failed:context deadline exceeded",
		"beta:remote_project_coordination_check_failed:project-1:The operation has timed out.",
	}, []string{"alpha", "beta"})
	if readiness.Status != "degraded" {
		t.Fatalf("expected transient check failures to be degraded, got %+v", readiness)
	}
	if !strings.Contains(readiness.Reason, "remote_active_session_check_failed") || !strings.Contains(readiness.Reason, "remote_project_coordination_check_failed") {
		t.Fatalf("expected degraded reason to retain transient probe evidence, got %+v", readiness)
	}
}

func TestManagerCleanStopReadinessKeepsHardBlockersFailClosed(t *testing.T) {
	readiness := managerCleanStopReadinessFromBlockers([]string{
		"alpha:remote_active_session_check_failed:context deadline exceeded",
		"alpha:remote_active_claim:task-1",
	}, []string{"alpha"})
	if readiness.Status != "blocked" {
		t.Fatalf("expected hard residual to keep clean_stop blocked, got %+v", readiness)
	}
	if !strings.Contains(readiness.Reason, "remote_active_claim:task-1") {
		t.Fatalf("expected hard blocker in reason, got %+v", readiness)
	}
	if strings.Contains(readiness.Reason, "remote_active_session_check_failed") {
		t.Fatalf("transient probe failure should not pollute blocked clean_stop reason, got %+v", readiness)
	}
}

func TestManagerRPCContractTransientErrorRecognizesDeadlines(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		context.Canceled,
		errors.New("The operation has timed out."),
		errors.New("context deadline exceeded while reading response"),
	} {
		if !managerRPCContractTransientError(err) {
			t.Fatalf("expected %q to be classified transient", err)
		}
	}
	if managerRPCContractTransientError(errors.New("sqlite syntax error near SELECTT")) {
		t.Fatalf("sqlite syntax error must remain non-transient")
	}
}

func TestManagerSubstrateRPCWithRetryRetriesTransientDeadline(t *testing.T) {
	origRetryDelay := managerRPCContractSmokeRetryDelay
	t.Cleanup(func() {
		managerRPCContractSmokeRetryDelay = origRetryDelay
	})
	managerRPCContractSmokeRetryDelay = 0
	attempts := 0
	err := managerSubstrateRPCWithRetry(context.Background(), "test.deadline", func(context.Context) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil {
		t.Fatalf("managerSubstrateRPCWithRetry() error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected retry helper to make 2 attempts, got %d", attempts)
	}
}

func TestManagerRPCContractReadinessPassesRemoteMethodMatrix(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	origBuild := managerSubstrateRuntimeBuildFunc
	origRepository := managerSubstrateRepositoryFunc
	t.Cleanup(func() {
		managerSubstrateRuntimeBuildFunc = origBuild
		managerSubstrateRepositoryFunc = origRepository
	})
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "rev-matrix"}
	}
	managerSubstrateRepositoryFunc = func() managerRepositoryIdentity {
		return managerRepositoryIdentity{Root: t.TempDir(), Head: "rev-matrix", Branch: "main"}
	}
	required := managerRPCContractRequiredMethods()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-rpc-matrix" {
			t.Fatalf("Authorization = %q", got)
		}
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "rpc.methods.list":
			methods := make([]map[string]string, 0, len(required))
			for _, method := range required {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"count": len(methods), "methods": methods})
		case "rpc.describe":
			params, _ := req.Params.(map[string]any)
			method, _ := params["method"].(string)
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"method": method, "description": "schema", "params": managerRPCContractTestParamsSchema(method)})
		case "runtime.build.info":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"vcs_revision": "rev-matrix", "go_version": "go1.26.1", "binary_sha256": "deadbeef"})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-rpc-matrix",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-rpc-matrix",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-rpc-matrix",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-matrix",
	}}})
	if readiness.Status != "ready" || len(readiness.Agents) != 1 || readiness.Agents[0].Status != "ready" {
		t.Fatalf("expected RPC contract readiness, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessRetriesTransientRateLimit(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	origBuild := managerSubstrateRuntimeBuildFunc
	origRepository := managerSubstrateRepositoryFunc
	origRetryDelay := managerRPCContractSmokeRetryDelay
	t.Cleanup(func() {
		managerSubstrateRuntimeBuildFunc = origBuild
		managerSubstrateRepositoryFunc = origRepository
		managerRPCContractSmokeRetryDelay = origRetryDelay
	})
	managerRPCContractSmokeRetryDelay = time.Millisecond
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "rev-retry"}
	}
	managerSubstrateRepositoryFunc = func() managerRepositoryIdentity {
		return managerRepositoryIdentity{Root: t.TempDir(), Head: "rev-retry", Branch: "main"}
	}
	required := managerRPCContractRequiredMethods()
	var rateLimitedDescribeAttempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "rpc.methods.list":
			methods := make([]map[string]string, 0, len(required))
			for _, method := range required {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"count": len(methods), "methods": methods})
		case "rpc.describe":
			params, _ := req.Params.(map[string]any)
			method, _ := params["method"].(string)
			if method == "project.governance.predicates.check" && atomic.AddInt32(&rateLimitedDescribeAttempts, 1) == 1 {
				http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"method": method, "description": "schema", "params": managerRPCContractTestParamsSchema(method)})
		case "runtime.build.info":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"vcs_revision": "rev-retry", "go_version": "go1.26.1", "binary_sha256": "deadbeef"})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-rpc-retry",
		WorkspaceID: "ws-rpc-retry",
		AgentID:     "agent-rpc-retry",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-rpc-retry",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-retry",
	}}})
	if readiness.Status != "ready" || len(readiness.Agents) != 1 || readiness.Agents[0].Status != "ready" {
		t.Fatalf("expected transient rate limit to retry and pass readiness, got %+v", readiness)
	}
	if atomic.LoadInt32(&rateLimitedDescribeAttempts) != 2 {
		t.Fatalf("expected exactly one retry for rate-limited describe, attempts=%d", rateLimitedDescribeAttempts)
	}
}

func TestManagerRPCContractReadinessRetriesSlowDescribeWithinSmokeBudget(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	origBuild := managerSubstrateRuntimeBuildFunc
	origRepository := managerSubstrateRepositoryFunc
	origSmokeTimeout := managerRPCContractSmokeTimeout
	origCallTimeout := managerRPCContractCallTimeout
	origRetryDelay := managerRPCContractSmokeRetryDelay
	t.Cleanup(func() {
		managerSubstrateRuntimeBuildFunc = origBuild
		managerSubstrateRepositoryFunc = origRepository
		managerRPCContractSmokeTimeout = origSmokeTimeout
		managerRPCContractCallTimeout = origCallTimeout
		managerRPCContractSmokeRetryDelay = origRetryDelay
	})
	managerRPCContractSmokeTimeout = 2 * time.Second
	managerRPCContractCallTimeout = 50 * time.Millisecond
	managerRPCContractSmokeRetryDelay = time.Millisecond
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "rev-slow-describe"}
	}
	managerSubstrateRepositoryFunc = func() managerRepositoryIdentity {
		return managerRepositoryIdentity{Root: t.TempDir(), Head: "rev-slow-describe", Branch: "main"}
	}
	required := managerRPCContractRequiredMethods()
	var slowDescribeAttempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "rpc.methods.list":
			methods := make([]map[string]string, 0, len(required))
			for _, method := range required {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"count": len(methods), "methods": methods})
		case "rpc.describe":
			params, _ := req.Params.(map[string]any)
			method, _ := params["method"].(string)
			if method == "project.patch_queue.rollback_record" && atomic.AddInt32(&slowDescribeAttempts, 1) == 1 {
				time.Sleep(150 * time.Millisecond)
				return
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"method": method, "description": "schema", "params": managerRPCContractTestParamsSchema(method)})
		case "runtime.build.info":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"vcs_revision": "rev-slow-describe", "go_version": "go1.26.1", "binary_sha256": "deadbeef"})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-rpc-slow-describe",
		WorkspaceID: "ws-rpc-slow-describe",
		AgentID:     "agent-rpc-slow-describe",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-rpc-slow-describe",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-slow-describe",
	}}})
	if readiness.Status != "ready" || len(readiness.Agents) != 1 || readiness.Agents[0].Status != "ready" {
		t.Fatalf("expected slow describe retry to pass readiness, got %+v", readiness)
	}
	if atomic.LoadInt32(&slowDescribeAttempts) != 2 {
		t.Fatalf("expected exactly one retry for slow describe, attempts=%d", slowDescribeAttempts)
	}
}

func TestManagerRPCContractDefaultSmokeBudgetFitsDashboardGate(t *testing.T) {
	if managerRPCContractSmokeTimeout >= 70*time.Second {
		t.Fatalf("default RPC smoke timeout must fit inside the 70s dashboard substrate gate, got %s", managerRPCContractSmokeTimeout)
	}
	if managerRPCContractCallTimeout <= 0 || managerRPCContractCallTimeout >= managerRPCContractSmokeTimeout {
		t.Fatalf("default per-call timeout must be positive and below total smoke budget, call=%s total=%s", managerRPCContractCallTimeout, managerRPCContractSmokeTimeout)
	}
}

func runManagerRPCContractBuildParityReadiness(t *testing.T, localRev string, remoteBuildInfo map[string]any) managerRPCContractReadiness {
	return runManagerRPCContractBuildParityReadinessWithServerHead(t, localRev, localRev, remoteBuildInfo)
}

func runManagerRPCContractBuildParityReadinessWithServerHead(t *testing.T, localBuildRev, localServerRev string, remoteBuildInfo map[string]any) managerRPCContractReadiness {
	t.Helper()
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	origBuild := managerSubstrateRuntimeBuildFunc
	origRepository := managerSubstrateRepositoryFunc
	t.Cleanup(func() {
		managerSubstrateRuntimeBuildFunc = origBuild
		managerSubstrateRepositoryFunc = origRepository
	})
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: localBuildRev}
	}
	managerSubstrateRepositoryFunc = func() managerRepositoryIdentity {
		return managerRepositoryIdentity{Root: t.TempDir(), Head: localServerRev, Branch: "main"}
	}
	required := managerRPCContractRequiredMethods()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "rpc.methods.list":
			methods := make([]map[string]string, 0, len(required))
			for _, method := range required {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"count": len(methods), "methods": methods})
		case "rpc.describe":
			params, _ := req.Params.(map[string]any)
			method, _ := params["method"].(string)
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"method": method, "description": "schema", "params": managerRPCContractTestParamsSchema(method)})
		case "runtime.build.info":
			writeManagerRPCContractTestResult(t, w, req.ID, remoteBuildInfo)
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-rpc-matrix",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-rpc-matrix",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	return managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-rpc-matrix",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-matrix",
	}}})
}

func TestManagerRPCContractReadinessBlocksRemoteBuildRevisionDrift(t *testing.T) {
	readiness := runManagerRPCContractBuildParityReadiness(t, "rev-local", map[string]any{
		"vcs_revision": "rev-remote",
		"go_version":   "go1.26.1",
	})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "does not match local server checkout revision") {
		t.Fatalf("expected remote build revision drift to block readiness, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessBlocksRemoteModifiedBuild(t *testing.T) {
	readiness := runManagerRPCContractBuildParityReadiness(t, "rev-local", map[string]any{
		"vcs_revision": "rev-local",
		"vcs_modified": true,
		"go_version":   "go1.26.1",
	})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "vcs-modified") {
		t.Fatalf("expected remote vcs-modified build to block readiness, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessBlocksMissingRemoteBuildRevision(t *testing.T) {
	readiness := runManagerRPCContractBuildParityReadiness(t, "rev-local", map[string]any{
		"go_version": "go1.26.1",
	})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "no vcs_revision") {
		t.Fatalf("expected missing remote vcs_revision to block readiness, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessAllowsMatchingRemoteBuild(t *testing.T) {
	readiness := runManagerRPCContractBuildParityReadiness(t, "rev-shared", map[string]any{
		"vcs_revision": "rev-shared",
		"go_version":   "go1.26.1",
	})
	if readiness.Status != "ready" || len(readiness.Agents) != 1 || readiness.Agents[0].Status != "ready" {
		t.Fatalf("expected matching remote build to pass readiness, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessAllowsRemoteServerBuildWithSubmoduleBot(t *testing.T) {
	readiness := runManagerRPCContractBuildParityReadinessWithServerHead(t, "bot-rev", "root-rev", map[string]any{
		"vcs_revision": "root-rev",
		"go_version":   "go1.26.1",
	})
	if readiness.Status != "ready" || len(readiness.Agents) != 1 || readiness.Agents[0].Status != "ready" {
		t.Fatalf("expected remote server/root parity to pass with submodule bot build, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessBlocksMissingRequiredMethod(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	required := managerRPCContractRequiredMethods()
	missingMethod := "project.patch_queue.integration_record"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "rpc.methods.list":
			methods := []map[string]string{}
			for _, method := range required {
				if method == missingMethod {
					continue
				}
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"count": len(methods), "methods": methods})
		case "rpc.describe":
			params, _ := req.Params.(map[string]any)
			method, _ := params["method"].(string)
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"method": method, "description": "schema", "params": managerRPCContractTestParamsSchema(method)})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-rpc-matrix",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-rpc-matrix",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-rpc-matrix",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-matrix",
	}}})
	if readiness.Status != "blocked" || !containsTrimmedString(readiness.MissingMethods, missingMethod) {
		t.Fatalf("expected missing %s to block RPC contract readiness, got %+v", missingMethod, readiness)
	}
}

func TestManagerRPCContractReadinessBlocksSkippedProofWithoutOverride(t *testing.T) {
	setManagerWebTestHome(t)

	workdirMissingToken := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdirMissingToken, LocalRuntimeProfile{
		RPCEndpoint: "https://rhizome.example/rpc",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-missing-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile(missing token) error: %v", err)
	}
	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-missing-token",
		Workdir:     workdirMissingToken,
		WorkspaceID: "ws-rpc-matrix",
	}}})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "rhizome token unavailable") {
		t.Fatalf("expected missing token to block RPC contract proof, got %+v", readiness)
	}

	workdirLocalEndpoint := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdirLocalEndpoint, LocalRuntimeProfile{
		RPCEndpoint: "http://127.0.0.1:51955/rpc",
		AgentToken:  "token-local-rpc",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-local-endpoint",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile(local endpoint) error: %v", err)
	}
	readiness = managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-local-endpoint",
		Workdir:     workdirLocalEndpoint,
		WorkspaceID: "ws-rpc-matrix",
	}}})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "local RPC endpoint smoke disabled") {
		t.Fatalf("expected local endpoint skip to block RPC contract proof, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessAllowsExplicitLocalOnlyOverride(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: "http://127.0.0.1:51955/rpc",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-local-only",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-local-only",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-matrix",
	}}})
	if readiness.Status != "not_required" || len(readiness.Agents) != 1 || !strings.Contains(readiness.Agents[0].Reason, "explicit local-only") {
		t.Fatalf("expected explicit local-only override to be recorded, got %+v", readiness)
	}
}

func writeManagerRPCContractTestResult(t *testing.T, w http.ResponseWriter, id string, result any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	}); err != nil {
		t.Fatalf("encode rpc response: %v", err)
	}
}

func managerRPCContractTestParamsSchema(method string) map[string]any {
	out := map[string]any{}
	requirement, ok := managerRPCContractSchemaRequirements[method]
	if !ok {
		return out
	}
	for name, paramRequirement := range requirement.Params {
		param := map[string]any{
			"type":     "string",
			"required": paramRequirement.Required,
		}
		if name == "arguments" || strings.HasSuffix(name, "_evidence") {
			param["type"] = "object"
		}
		if len(paramRequirement.Enum) > 0 {
			enum := make([]any, 0, len(paramRequirement.Enum))
			for _, value := range paramRequirement.Enum {
				enum = append(enum, value)
			}
			param["enum"] = enum
		}
		out[name] = param
	}
	return out
}

func TestManagerRPCContractReadinessBlocksStaleTaskFrontierDecisionSchema(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	required := managerRPCContractRequiredMethods()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "rpc.methods.list":
			methods := make([]map[string]string, 0, len(required))
			for _, method := range required {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"count": len(methods), "methods": methods})
		case "rpc.describe":
			params, _ := req.Params.(map[string]any)
			method, _ := params["method"].(string)
			schema := managerRPCContractTestParamsSchema(method)
			if method == "agent.task_frontier.decision" {
				decision, _ := schema["decision_state"].(map[string]any)
				decision["enum"] = []any{"selected", "declined", "model_failed", "hydration_failed"}
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"method": method, "description": "schema", "params": schema})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-rpc-matrix",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-rpc-matrix",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-rpc-matrix",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-matrix",
	}}})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "admission_failed") {
		t.Fatalf("expected stale task frontier decision schema to block readiness, got %+v", readiness)
	}
}

func TestManagerRPCContractReadinessBlocksServerGrownRequiredParam(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")
	required := managerRPCContractRequiredMethods()
	grownMethod := "project.patch_queue.claim"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "rpc.methods.list":
			methods := make([]map[string]string, 0, len(required))
			for _, method := range required {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"count": len(methods), "methods": methods})
		case "rpc.describe":
			params, _ := req.Params.(map[string]any)
			method, _ := params["method"].(string)
			schema := managerRPCContractTestParamsSchema(method)
			if method == grownMethod {
				// Server has grown a new required param the bot does not know to send.
				schema["new_required_field"] = map[string]any{"type": "string", "required": true}
			}
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"method": method, "description": "schema", "params": schema})
		case "runtime.build.info":
			writeManagerRPCContractTestResult(t, w, req.ID, map[string]any{"vcs_revision": "rev", "go_version": "go1.26.1"})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint: server.URL,
		AgentToken:  "token-rpc-matrix",
		WorkspaceID: "ws-rpc-matrix",
		AgentID:     "agent-rpc-matrix",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	readiness := managerRPCContractReadinessForRoster(BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "agent-rpc-matrix",
		Workdir:     workdir,
		WorkspaceID: "ws-rpc-matrix",
	}}})
	if readiness.Status != "blocked" || !strings.Contains(readiness.Reason, "server contract grew beyond the bot's known schema") {
		t.Fatalf("expected server-grown required param to block readiness, got %+v", readiness)
	}
}

func TestManagerProviderReadinessForRosterClassifiesRoutes(t *testing.T) {
	setManagerWebTestHome(t)
	readyExecutable := writeProviderSmokeScript(t, t.TempDir(), "codex-ready")
	modelFailExecutable := writeProviderCodexModelSmokeScript(t, t.TempDir(), "codex-model-fail", "unsupported-model")
	pathDir := t.TempDir()
	pathExecutableName := "codex-path-test"
	pathExecutable := writeProviderSmokeScript(t, pathDir, pathExecutableName)
	defaultCodexExecutable := writeProviderSmokeScript(t, pathDir, "codex")
	t.Setenv("PATH", pathDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CODEX_CLI_PATH", defaultCodexExecutable)
	t.Setenv("CUSTOM_CLI_PATH", defaultCodexExecutable)
	t.Setenv("RHIZOME_AGENT_PROVIDER_API_READY_API_KEY", "api-ready-key")
	t.Setenv("RHIZOME_AGENT_PROVIDER_API_NO_BASE_API_KEY", "api-no-base-key")

	registry := BotRegistry{Agents: []ManagedAgentRecord{
		{AgentID: "bridge-missing", ProviderID: "codex-missing", Workdir: filepath.Join(t.TempDir(), "bridge-missing"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
		{AgentID: "bridge-ready", ProviderID: "codex-ready", Workdir: filepath.Join(t.TempDir(), "bridge-ready"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
		{AgentID: "bridge-model-fail", ProviderID: "codex-model-fail", ModelOverride: "unsupported-model", Workdir: filepath.Join(t.TempDir(), "bridge-model-fail"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
		{AgentID: "bridge-path", ProviderID: "codex-path", Workdir: filepath.Join(t.TempDir(), "bridge-path"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
		{AgentID: "bridge-default", ProviderID: "codex-default", Workdir: filepath.Join(t.TempDir(), "bridge-default"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
		{AgentID: "api-missing", ProviderID: "api-missing", Workdir: filepath.Join(t.TempDir(), "api-missing"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
		{AgentID: "api-ready", ProviderID: "api-ready", Workdir: filepath.Join(t.TempDir(), "api-ready"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
		{AgentID: "api-no-base", ProviderID: "api-no-base", Workdir: filepath.Join(t.TempDir(), "api-no-base"), WorkspaceID: "ws-1", CoordinationMode: CoordinationModeTrustFirst},
	}}
	providers := ProviderRegistry{Providers: []ProviderRecord{
		{
			ProviderID:  "codex-missing",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			Enabled:     true,
			Bridge:      ProviderBridgeConfig{Executable: filepath.Join(t.TempDir(), "missing-codex.exe")},
		},
		{
			ProviderID:  "codex-ready",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			Enabled:     true,
			Bridge:      ProviderBridgeConfig{Executable: readyExecutable},
		},
		{
			ProviderID:   "codex-model-fail",
			ChannelType:  providerChannelBridge,
			Driver:       llmBackendCodex,
			DefaultModel: "unsupported-model",
			Enabled:      true,
			Bridge:       ProviderBridgeConfig{Executable: modelFailExecutable},
		},
		{
			ProviderID:  "codex-path",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			Enabled:     true,
			Bridge:      ProviderBridgeConfig{Executable: pathExecutableName},
		},
		{
			ProviderID:  "codex-default",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			Enabled:     true,
		},
		{
			ProviderID:  "api-missing",
			ChannelType: providerChannelAPI,
			Driver:      providerDriverOpenAICompatible,
			Enabled:     true,
			API:         ProviderAPIConfig{BaseURL: "https://api.example.test/v1"},
		},
		{
			ProviderID:  "api-ready",
			ChannelType: providerChannelAPI,
			Driver:      providerDriverOpenAICompatible,
			Enabled:     true,
			API:         ProviderAPIConfig{BaseURL: "https://api.example.test/v1"},
		},
		{
			ProviderID:  "api-no-base",
			ChannelType: providerChannelAPI,
			Driver:      providerDriverOpenAICompatible,
			Enabled:     true,
		},
	}}

	rows := managerProviderReadinessForRoster(registry, providers, nil)
	byAgent := map[string]managerProviderRouteReadiness{}
	for _, row := range rows {
		byAgent[row.AgentID] = row
	}
	if byAgent["bridge-missing"].Status != "blocked" || !strings.Contains(byAgent["bridge-missing"].Reason, "does not exist") {
		t.Fatalf("expected missing bridge provider blocked, got %+v", byAgent["bridge-missing"])
	}
	if byAgent["bridge-ready"].Status != "ready" || byAgent["bridge-ready"].Executable != readyExecutable {
		t.Fatalf("expected ready bridge provider, got %+v", byAgent["bridge-ready"])
	}
	if byAgent["bridge-ready"].SmokeStatus != "passed" {
		t.Fatalf("expected ready bridge provider to pass managed child-env smoke, got %+v", byAgent["bridge-ready"])
	}
	if byAgent["bridge-model-fail"].Status != "blocked" || byAgent["bridge-model-fail"].SmokeStatus != "blocked" || !strings.Contains(byAgent["bridge-model-fail"].Reason, "codex bridge model smoke failed") {
		t.Fatalf("expected model-incompatible codex provider blocked by model smoke, got %+v", byAgent["bridge-model-fail"])
	}
	if byAgent["bridge-path"].Status != "ready" || byAgent["bridge-path"].Executable != pathExecutable {
		t.Fatalf("expected PATH bridge provider to resolve, got %+v", byAgent["bridge-path"])
	}
	if byAgent["bridge-default"].Status != "ready" || byAgent["bridge-default"].SmokeStatus != "passed" {
		t.Fatalf("expected default bridge provider to pass via managed child PATH, got %+v", byAgent["bridge-default"])
	}
	if byAgent["api-missing"].Status != "blocked" || !strings.Contains(byAgent["api-missing"].Reason, "api key") {
		t.Fatalf("expected API provider without credential blocked, got %+v", byAgent["api-missing"])
	}
	if byAgent["api-ready"].Status != "blocked" || byAgent["api-ready"].SmokeStatus != "blocked" || !strings.Contains(byAgent["api-ready"].Reason, "executable API smoke") {
		t.Fatalf("expected API provider without executable proof blocked despite credential metadata, got %+v", byAgent["api-ready"])
	}
	if byAgent["api-no-base"].Status != "blocked" || !strings.Contains(byAgent["api-no-base"].Reason, "base url") {
		t.Fatalf("expected OpenAI-compatible API provider without base URL blocked, got %+v", byAgent["api-no-base"])
	}
}

func TestManagerProviderReadinessForOverviewSkipsManagedSmoke(t *testing.T) {
	setManagerWebTestHome(t)
	modelFailExecutable := writeProviderCodexModelSmokeScript(t, t.TempDir(), "codex-model-fail", "unsupported-model")
	registry := BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:          "bridge-model-fail",
		ProviderID:       "codex-model-fail",
		ModelOverride:    "unsupported-model",
		Workdir:          filepath.Join(t.TempDir(), "bridge-model-fail"),
		WorkspaceID:      "ws-1",
		CoordinationMode: CoordinationModeTrustFirst,
	}}}
	providers := ProviderRegistry{Providers: []ProviderRecord{{
		ProviderID:   "codex-model-fail",
		ChannelType:  providerChannelBridge,
		Driver:       llmBackendCodex,
		DefaultModel: "unsupported-model",
		Enabled:      true,
		Bridge:       ProviderBridgeConfig{Executable: modelFailExecutable},
	}}}

	rows := managerProviderReadinessForRosterWithOptions(registry, providers, nil, managerSubstrateOptions{SkipProviderManagedSmoke: true})
	if len(rows) != 1 {
		t.Fatalf("expected one provider readiness row, got %+v", rows)
	}
	row := rows[0]
	if row.Status != "ready" || row.SmokeStatus != "not_checked" {
		t.Fatalf("overview provider readiness should not run fail-close model smoke, got %+v", row)
	}
	if strings.Contains(row.Reason, "model smoke") || strings.Contains(row.SmokeReason, "unsupported-model") {
		t.Fatalf("overview row appears to have executed model smoke: %+v", row)
	}
}

func TestManagerBrowserReadinessBlocksVisualProbeWithoutExecutableSmoke(t *testing.T) {
	setManagerWebTestHome(t)
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_visual_probe"), InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "browser_visual_probe",
		Description:      "Capture screenshots",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.0.0",
		CapabilitySuites: []string{"browser_read_only", "screenshot_capture"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_session"), InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "browser_session",
		Description:      "Run browser sessions",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.0.0",
		CapabilitySuites: []string{"browser_unrestricted", "console_read"},
	})
	registry := BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "visual-agent",
		Workdir:     workdir,
		WorkspaceID: "ws-1",
		ToolBundles: []string{"browser_visual_probe"},
	}}}

	rows := managerToolBundleReadinessForRoster(registry)
	if len(rows) != 1 || rows[0].Status != "ready" {
		t.Fatalf("expected installed bundle itself to be ready before browser smoke gate, got %+v", rows)
	}
	browser := managerBrowserReadinessFromToolBundles(rows)
	if browser.Status != "blocked" || !strings.Contains(browser.Reason, "smoke missing") {
		t.Fatalf("expected browser readiness to block without visual-probe healthcheck smoke, got %+v", browser)
	}
}

func TestManagerToolBundleReadinessBlocksMissingBrowserBundle(t *testing.T) {
	setManagerWebTestHome(t)
	registry := BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "visual-agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		ToolBundles: []string{"browser_visual_probe"},
	}}}

	rows := managerToolBundleReadinessForRoster(registry)
	if len(rows) != 1 {
		t.Fatalf("expected one tool bundle readiness row, got %+v", rows)
	}
	if rows[0].Status != "blocked" || !strings.Contains(rows[0].Reason, "browser_visual_probe:missing") {
		t.Fatalf("expected missing browser bundle to block, got %+v", rows[0])
	}
	browser := managerBrowserReadinessFromToolBundles(rows)
	if browser.Status != "blocked" || !strings.Contains(browser.Reason, "visual-agent") {
		t.Fatalf("expected browser readiness blocked by missing bundle, got %+v", browser)
	}
}

func TestManagerToolBundleReadinessBlocksUnreadableAnatomy(t *testing.T) {
	setManagerWebTestHome(t)
	registry := BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "bad-anatomy-agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AnatomyPath: filepath.Join(t.TempDir(), "missing-anatomy.json"),
	}}}

	rows := managerToolBundleReadinessForRoster(registry)
	if len(rows) != 1 {
		t.Fatalf("expected one tool bundle readiness row, got %+v", rows)
	}
	if rows[0].Status != "blocked" || !strings.Contains(rows[0].Reason, "anatomy_unreadable") {
		t.Fatalf("expected unreadable anatomy to block substrate readiness, got %+v", rows[0])
	}
}

func TestManagerToolBundleReadinessAcceptsInstalledBrowserVisualProbe(t *testing.T) {
	setManagerWebTestHome(t)
	workdir := t.TempDir()
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER", "1")
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_visual_probe"), InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "browser_visual_probe",
		Description:      "Capture screenshots",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.0.0",
		CapabilitySuites: []string{"browser_read_only", "screenshot_capture"},
		Healthcheck: &InstalledToolBundleHealthcheck{
			Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHealthcheckHelper", "--"},
			TimeoutSeconds: 5,
		},
		ArtifactContracts: []InstalledToolBundleArtifactContract{{
			Name: "probe_report", Type: "application/json", Path: "probe-report.json", Required: true,
		}},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_session"), InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "browser_session",
		Description:      "Run browser sessions",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.0.0",
		CapabilitySuites: []string{"browser_unrestricted", "console_read"},
	})
	registry := BotRegistry{Agents: []ManagedAgentRecord{{
		AgentID:     "visual-agent",
		Workdir:     workdir,
		WorkspaceID: "ws-1",
		ToolBundles: []string{"browser_visual_probe"},
	}}}

	rows := managerToolBundleReadinessForRoster(registry)
	if len(rows) != 1 {
		t.Fatalf("expected one tool bundle readiness row, got %+v", rows)
	}
	if rows[0].Status != "ready" {
		t.Fatalf("expected installed browser_visual_probe ready, got %+v", rows[0])
	}
	browser := managerBrowserReadinessFromToolBundles(rows)
	if browser.Status != "ready" || !containsTrimmedString(browser.Agents, "visual-agent") {
		t.Fatalf("expected browser readiness ready, got %+v", browser)
	}
}
