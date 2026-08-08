package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestShellToolExecuteSurfacesTimeoutExplicitly(t *testing.T) {
	origTimeout := shellToolTimeoutBudget
	origRunner := shellToolRunner
	shellToolTimeoutBudget = 5 * time.Millisecond
	shellToolRunner = func(ctx context.Context, _ *exec.Cmd) ([]byte, error) {
		<-ctx.Done()
		return []byte("partial output"), ctx.Err()
	}
	t.Cleanup(func() {
		shellToolTimeoutBudget = origTimeout
		shellToolRunner = origRunner
	})

	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": "go test ./..."})
	if result == nil || !result.IsError {
		t.Fatalf("expected timeout result, got %+v", result)
	}
	if !strings.Contains(result.Output, "partial output") || !strings.Contains(result.Output, "command timed out after 5ms") {
		t.Fatalf("expected explicit timeout output, got %q", result.Output)
	}
}

func TestShellToolDefaultRunnerTimeoutReturnsPromptly(t *testing.T) {
	origTimeout := shellToolTimeoutBudget
	origKillWait := shellToolKillWaitBudget
	shellToolTimeoutBudget = 100 * time.Millisecond
	shellToolKillWaitBudget = time.Second
	t.Cleanup(func() {
		shellToolTimeoutBudget = origTimeout
		shellToolKillWaitBudget = origKillWait
	})

	started := time.Now()
	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": shellLongSleepCommand()})
	elapsed := time.Since(started)
	if result == nil || !result.IsError {
		t.Fatalf("expected timeout result, got %+v", result)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("expected shell timeout to return promptly, elapsed=%s output=%q", elapsed, result.Output)
	}
	if !strings.Contains(result.Output, "command timed out after 100ms") {
		t.Fatalf("expected explicit timeout output, got %q", result.Output)
	}
}

func TestShellToolDefaultRunnerTimeoutKillsBackgroundChildWithOpenOutput(t *testing.T) {
	origTimeout := shellToolTimeoutBudget
	origKillWait := shellToolKillWaitBudget
	shellToolTimeoutBudget = 1500 * time.Millisecond
	shellToolKillWaitBudget = time.Second
	t.Cleanup(func() {
		shellToolTimeoutBudget = origTimeout
		shellToolKillWaitBudget = origKillWait
	})

	started := time.Now()
	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": shellBackgroundChildCommand()})
	elapsed := time.Since(started)
	if result == nil || !result.IsError {
		t.Fatalf("expected timeout result for background child, got %+v", result)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("expected background child timeout to return promptly, elapsed=%s output=%q", elapsed, result.Output)
	}
	if !strings.Contains(result.Output, "spawned child") {
		t.Fatalf("expected partial child output, got %q", result.Output)
	}
	if !strings.Contains(result.Output, "process tree kill attempted") {
		t.Fatalf("expected cleanup telemetry, got %q", result.Output)
	}
}

func TestBoundedShellOutputBufferDrainsBeyondLimit(t *testing.T) {
	buf := newBoundedShellOutputBuffer(4)
	if n, err := buf.Write([]byte("abcdef")); err != nil || n != 6 {
		t.Fatalf("Write() = %d, %v; want full drain", n, err)
	}
	output := string(buf.Bytes())
	if !strings.Contains(output, "abcd") || !strings.Contains(output, "2 bytes omitted") {
		t.Fatalf("expected bounded output with truncation notice, got %q", output)
	}
}

func TestShellWorkdirProcessCleanupEligibleOnlyForProjectCheckouts(t *testing.T) {
	checkout := filepath.Join(t.TempDir(), "project-checkouts", "demo")
	if !shellWorkdirProcessCleanupEligible(checkout) {
		t.Fatalf("expected project-checkouts path to be cleanup eligible")
	}
	agentRoot := t.TempDir()
	if shellWorkdirProcessCleanupEligible(agentRoot) {
		t.Fatalf("expected agent root path to be cleanup skipped")
	}
	nearMiss := filepath.Join(t.TempDir(), "project-checkouts-old", "demo")
	if shellWorkdirProcessCleanupEligible(nearMiss) {
		t.Fatalf("expected near-miss segment to be cleanup skipped")
	}
}

func TestShellWorkdirProcessCleanupRootsIncludeAgentProjectCheckouts(t *testing.T) {
	agentRoot := t.TempDir()
	projectCheckouts := filepath.Join(agentRoot, "project-checkouts")
	if err := os.MkdirAll(projectCheckouts, 0o755); err != nil {
		t.Fatalf("mkdir project-checkouts: %v", err)
	}
	roots := shellWorkdirProcessCleanupRoots(agentRoot)
	if len(roots) != 1 || roots[0] != filepath.Clean(projectCheckouts) {
		t.Fatalf("expected agent project-checkouts cleanup root, got %v", roots)
	}
}

func TestShellToolEnvProvidesUniqueSmokePortHint(t *testing.T) {
	agentRoot := t.TempDir()
	env := shellToolEnv(agentRoot, "theta")
	values := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	port := values["RHIZOME_SMOKE_PORT_HINT"]
	if port == "" {
		t.Fatalf("expected RHIZOME_SMOKE_PORT_HINT in shell env")
	}
	if port == "3000" || port == "4173" || port == "5173" {
		t.Fatalf("expected non-default smoke port hint, got %s", port)
	}
	if values["RHIZOME_AGENT_ID"] != "theta" {
		t.Fatalf("expected agent id env, got %q", values["RHIZOME_AGENT_ID"])
	}
	if filepath.Clean(values["RHIZOME_SHELL_WORKDIR"]) != filepath.Clean(agentRoot) {
		t.Fatalf("expected shell workdir env, got %q want %q", values["RHIZOME_SHELL_WORKDIR"], agentRoot)
	}
}

func TestShellToolEnvRedirectsPackageCachesToAgentRoot(t *testing.T) {
	agentRoot := t.TempDir()
	checkout := filepath.Join(agentRoot, "project-checkouts", "review-1")
	if err := os.MkdirAll(checkout, 0o755); err != nil {
		t.Fatalf("mkdir checkout: %v", err)
	}
	env := shellToolEnv(checkout, "theta")
	values := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	wantCacheRoot := filepath.Join(agentRoot, ".cache")
	if filepath.Clean(values["XDG_CACHE_HOME"]) != filepath.Clean(wantCacheRoot) {
		t.Fatalf("expected XDG cache under agent root, got %q want %q", values["XDG_CACHE_HOME"], wantCacheRoot)
	}
	if filepath.Clean(values["npm_config_cache"]) != filepath.Clean(filepath.Join(wantCacheRoot, "npm")) {
		t.Fatalf("expected npm cache under agent root, got %q", values["npm_config_cache"])
	}
	if filepath.Clean(values["PLAYWRIGHT_BROWSERS_PATH"]) != filepath.Clean(filepath.Join(wantCacheRoot, "ms-playwright")) {
		t.Fatalf("expected playwright cache under agent root, got %q", values["PLAYWRIGHT_BROWSERS_PATH"])
	}
	if filepath.Clean(values["GOCACHE"]) != filepath.Clean(filepath.Join(wantCacheRoot, "go-build")) {
		t.Fatalf("expected go build cache under agent root, got %q", values["GOCACHE"])
	}
	wantLocalAppData := filepath.Join(agentRoot, ".local-data", "LocalAppData")
	if filepath.Clean(values["LOCALAPPDATA"]) != filepath.Clean(wantLocalAppData) || filepath.Clean(values["LocalAppData"]) != filepath.Clean(wantLocalAppData) {
		t.Fatalf("expected LOCALAPPDATA variants under agent root, got LOCALAPPDATA=%q LocalAppData=%q", values["LOCALAPPDATA"], values["LocalAppData"])
	}
	if filepath.Clean(values["APPDATA"]) != filepath.Clean(filepath.Join(agentRoot, ".local-data", "AppData", "Roaming")) {
		t.Fatalf("expected APPDATA under agent root, got %q", values["APPDATA"])
	}
	if filepath.Clean(values["RHIZOME_SHELL_WORKDIR"]) != filepath.Clean(checkout) {
		t.Fatalf("expected execution workdir to remain checkout, got %q", values["RHIZOME_SHELL_WORKDIR"])
	}
}

func TestShellToolDiskBackpressureBlocksInstallAndCleansGeneratedArtifacts(t *testing.T) {
	agentRoot := t.TempDir()
	staleModules := filepath.Join(agentRoot, "project-checkouts", "old-review", "node_modules", "vite")
	if err := os.MkdirAll(staleModules, 0o755); err != nil {
		t.Fatalf("mkdir stale node_modules: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleModules, "index.js"), []byte("generated\n"), 0o644); err != nil {
		t.Fatalf("write stale module: %v", err)
	}

	origRunner := shellToolRunner
	origFree := shellToolFreeDiskBytes
	origMin := shellToolDiskMinFreeBytes
	ran := false
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		ran = true
		return []byte("should not run"), nil
	}
	shellToolFreeDiskBytes = func(string) (uint64, error) {
		return 10 * 1024 * 1024, nil
	}
	shellToolDiskMinFreeBytes = 1024 * 1024 * 1024
	t.Cleanup(func() {
		shellToolRunner = origRunner
		shellToolFreeDiskBytes = origFree
		shellToolDiskMinFreeBytes = origMin
	})

	result := NewShellTool(agentRoot).Execute(context.Background(), map[string]any{"command": "npm.cmd install"})
	if result == nil || !result.IsError {
		t.Fatalf("expected disk backpressure error, got %+v", result)
	}
	if ran {
		t.Fatal("shell runner executed despite disk backpressure")
	}
	if !strings.Contains(result.Output, "shell disk backpressure") || !strings.Contains(result.Output, "generated-artifact cleanup attempted") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
	if _, err := os.Stat(staleModules); !os.IsNotExist(err) {
		t.Fatalf("expected stale generated dependency tree to be removed, stat err=%v", err)
	}
}

func TestShellToolDescriptionRequiresIsolatedBrowserSmoke(t *testing.T) {
	description := NewShellTool(t.TempDir()).Description()
	for _, want := range []string{"RHIZOME_SMOKE_PORT_HINT", "3000", "4173", "5173", "target product"} {
		if !strings.Contains(description, want) {
			t.Fatalf("shell description missing %q: %s", want, description)
		}
	}
}

func TestFormatShellToolErrorGuidesStaleLocalhostAndPlaywrightRecovery(t *testing.T) {
	output := formatShellToolError(`{"title":"Purple Deception Live Console","url":"http://127.0.0.1:4173"}`, "browserType.launchPersistentContext: Pass userDataDir parameter")
	for _, want := range []string{"stale local preview", "RHIZOME_SMOKE_PORT_HINT", "launchPersistentContext(userDataDir, options)"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected guidance %q in output:\n%s", want, output)
		}
	}
}

func TestFormatShellToolErrorGuidesMissingPlaywrightCoreRecovery(t *testing.T) {
	output := formatShellToolError("Error: Cannot find module 'playwright-core'", "exit status 1")
	for _, want := range []string{"Missing Playwright tooling is self-remediable", "browser-tooling", "npm.cmd install playwright-core --no-save", "NODE_PATH", "Do not mark visual acceptance blocked"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected guidance %q in output:\n%s", want, output)
		}
	}
}

func TestFormatShellToolErrorGuidesStartProcessRedirectionRecovery(t *testing.T) {
	output := formatShellToolError("Start-Process : This command cannot be run because RedirectStandardOutput and RedirectStandardError are the same.", "exit status 1")
	for _, want := range []string{"Start-Process", "distinct stdout/stderr files", "finally block"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected guidance %q in output:\n%s", want, output)
		}
	}
}

func TestRecordShellWorkdirCleanupRetriesForDelayedChildren(t *testing.T) {
	origRunner := shellToolWorkdirCleanupRunner
	origPasses := shellToolPostRunCleanupPasses
	origGap := shellToolPostRunCleanupGap
	t.Cleanup(func() {
		shellToolWorkdirCleanupRunner = origRunner
		shellToolPostRunCleanupPasses = origPasses
		shellToolPostRunCleanupGap = origGap
	})

	checkout := filepath.Join(t.TempDir(), "project-checkouts", "demo")
	if err := os.MkdirAll(checkout, 0o755); err != nil {
		t.Fatalf("mkdir checkout: %v", err)
	}
	calls := 0
	shellToolPostRunCleanupPasses = 3
	shellToolPostRunCleanupGap = time.Millisecond
	shellToolWorkdirCleanupRunner = func(workdir string) (string, error) {
		calls++
		if filepath.Clean(workdir) != filepath.Clean(checkout) {
			t.Fatalf("cleanup called with %q, want %q", workdir, checkout)
		}
		if calls == 2 {
			return `{"killed_pids":[1234]}`, nil
		}
		return "", nil
	}

	output := newBoundedShellOutputBuffer(shellToolOutputLimitBytes)
	recordShellWorkdirCleanup(output, checkout)
	if calls != 3 {
		t.Fatalf("expected three cleanup passes, got %d", calls)
	}
	if got := string(output.Bytes()); !strings.Contains(got, `"killed_pids":[1234]`) {
		t.Fatalf("expected delayed cleanup note, got %q", got)
	}
}

func TestShellCommandForWindowsPowerShellSetsUTF8Encoding(t *testing.T) {
	name, args := shellCommandForGOOS("windows", "Get-Content .\\agent.err.log")
	if name != "powershell" {
		t.Fatalf("expected powershell, got %q", name)
	}
	if len(args) == 0 {
		t.Fatal("expected powershell arguments")
	}
	command := args[len(args)-1]
	for _, want := range []string{"[Console]::InputEncoding", "[Console]::OutputEncoding", "$OutputEncoding", "Get-Content .\\agent.err.log"} {
		if !strings.Contains(command, want) {
			t.Fatalf("powershell command missing %q: %s", want, command)
		}
	}
}

func TestShellCommandForExplicitWindowsPowerShellStillSetsUTF8Encoding(t *testing.T) {
	name, args := shellCommandForGOOS("windows", "powershell -NoProfile -Command Get-Content .\\agent.err.log")
	if name != "powershell" {
		t.Fatalf("expected outer powershell, got %q", name)
	}
	command := args[len(args)-1]
	for _, want := range []string{"[Console]::OutputEncoding", "powershell -NoProfile -EncodedCommand "} {
		if !strings.Contains(command, want) {
			t.Fatalf("explicit powershell command missing %q: %s", want, command)
		}
	}
	decoded := decodeEncodedCommandFromShellText(t, command)
	if !strings.Contains(decoded, windowsPowerShellUTF8Prologue()+"Get-Content .\\agent.err.log") {
		t.Fatalf("expected inner encoded command to receive UTF-8 prologue, decoded=%q command=%s", decoded, command)
	}
}

func TestShellCommandForExplicitWindowsEncodedPowerShellInjectsUTF8(t *testing.T) {
	encoded := encodePowerShellEncodedCommand("Get-Content .\\agent.err.log")
	name, args := shellCommandForGOOS("windows", "powershell -NoProfile -EncodedCommand "+encoded)
	if name != "powershell" {
		t.Fatalf("expected outer powershell, got %q", name)
	}
	command := args[len(args)-1]
	if !strings.Contains(command, "[Console]::OutputEncoding") {
		t.Fatalf("outer powershell command missing UTF-8 prologue: %s", command)
	}
	decoded := decodeEncodedCommandFromShellText(t, command)
	if !strings.Contains(decoded, windowsPowerShellUTF8Prologue()+"Get-Content .\\agent.err.log") {
		t.Fatalf("expected inner encoded command to receive UTF-8 prologue, decoded=%q", decoded)
	}
}

func TestShellCommandForExplicitWindowsPowerShellCommandExecutesCleanly(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("execution semantics test is Windows-specific")
	}
	name, args := shellCommandForGOOS("windows", `powershell -NoProfile -Command "Write-Output 'hello'"`)
	out, err := exec.Command(name, args...).CombinedOutput()
	output := string(out)
	if err != nil {
		t.Fatalf("explicit powershell command failed: %v\n%s", err, output)
	}
	if !strings.Contains(output, "hello") {
		t.Fatalf("expected command output, got %q", output)
	}
	if strings.Contains(output, "UTF8Encoding :") || strings.Contains(output, "not recognized") {
		t.Fatalf("expected clean UTF-8 prologue execution, got %q", output)
	}
}

func TestWindowsPowerShellCommandNormalizesStartProcessPackageManagers(t *testing.T) {
	command := windowsPowerShellCommandWithUTF8(`$p = Start-Process -FilePath npm -ArgumentList 'run','dev' -PassThru; Start-Process -FilePath "npx" -ArgumentList '-y','serve'; Start-Process -FilePath 'pnpm.ps1' -ArgumentList 'test'; Start-Process yarn -ArgumentList 'test'; Start-Process -FilePath npm.cmd -ArgumentList 'ci'; npm run build`)
	for _, want := range []string{"Start-Process -FilePath npm.cmd", `Start-Process -FilePath "npx.cmd"`, "Start-Process -FilePath 'pnpm.cmd'", "Start-Process yarn.cmd", "Start-Process -FilePath npm.cmd -ArgumentList 'ci'", "npm run build"} {
		if !strings.Contains(command, want) {
			t.Fatalf("expected normalized command to contain %q, got %s", want, command)
		}
	}
	for _, forbidden := range []string{"-FilePath npm -ArgumentList", `-FilePath "npx"`, "-FilePath 'pnpm.ps1'", "Start-Process yarn -ArgumentList"} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("expected command to avoid %q, got %s", forbidden, command)
		}
	}
}

func TestWindowsPowerShellCommandNormalizesLegacyCompatibility(t *testing.T) {
	command := windowsPowerShellCommandWithUTF8(`Set-Location project && npm run build; Set-Content README.md -Encoding utf8NoBOM -Value ok`)
	if strings.Contains(command, "&&") {
		t.Fatalf("expected PowerShell && to be normalized, got %q", command)
	}
	if !strings.Contains(command, `Set-Location project ; if (-not $?) { exit 1 }; npm run build`) {
		t.Fatalf("expected guarded PowerShell sequencing, got %q", command)
	}
	if strings.Contains(command, "utf8NoBOM") || !strings.Contains(command, "-Encoding utf8") {
		t.Fatalf("expected legacy utf8NoBOM encoding to be normalized, got %q", command)
	}
}

func TestWindowsPowerShellCommandDoesNotRewriteAndAndInsideQuotedPayloads(t *testing.T) {
	command := windowsPowerShellCommandWithUTF8("$script = @'\nif (server && !server.killed) { cleanup(); }\n'@; Set-Content smoke.mjs $script; Set-Location project && npm run build; Write-Output \"literal && stays\"")
	if !strings.Contains(command, "server && !server.killed") {
		t.Fatalf("expected JS payload && to be preserved, got %q", command)
	}
	if !strings.Contains(command, "literal && stays") {
		t.Fatalf("expected quoted PowerShell string && to be preserved, got %q", command)
	}
	if strings.Contains(command, "server ; if (-not $?)") || strings.Contains(command, "literal ; if (-not $?)") {
		t.Fatalf("quoted payload was corrupted by && compatibility rewrite: %q", command)
	}
	if !strings.Contains(command, "Set-Location project ; if (-not $?) { exit 1 }; npm run build") {
		t.Fatalf("expected command-level && to remain guarded, got %q", command)
	}
}

func TestExplicitWindowsPowerShellCommandNormalizesStartProcessPackageManagers(t *testing.T) {
	command := windowsPowerShellCommandWithUTF8(`powershell -NoProfile -Command "Start-Process -FilePath npm -ArgumentList 'run','dev'"`)
	decoded := decodeEncodedCommandFromShellText(t, command)
	if !strings.Contains(decoded, "Start-Process -FilePath npm.cmd") {
		t.Fatalf("expected inner encoded command to normalize npm.cmd, decoded=%q command=%s", decoded, command)
	}
}

func TestShellCommandForWindowsStartProcessUsesPowerShellAndNormalizesPackageManager(t *testing.T) {
	name, args := shellCommandForGOOS("windows", `Start-Process -FilePath npm -ArgumentList 'run','dev'`)
	if name != "powershell" {
		t.Fatalf("expected Start-Process command to use powershell, got %q args=%v", name, args)
	}
	if len(args) == 0 {
		t.Fatal("expected powershell arguments")
	}
	command := args[len(args)-1]
	if !strings.Contains(command, "Start-Process -FilePath npm.cmd") {
		t.Fatalf("expected package manager to be normalized through dispatcher path, got %s", command)
	}
	if strings.Contains(command, "-FilePath npm -ArgumentList") {
		t.Fatalf("expected dispatcher command to avoid npm.ps1 association risk, got %s", command)
	}
}

func decodeEncodedCommandFromShellText(t *testing.T, command string) string {
	t.Helper()
	parts := strings.Fields(command)
	for i, part := range parts {
		if strings.EqualFold(part, "-EncodedCommand") && i+1 < len(parts) {
			decoded, ok := decodePowerShellEncodedCommand(parts[i+1])
			if !ok {
				t.Fatalf("encoded command did not decode: %s", command)
			}
			return decoded
		}
	}
	t.Fatalf("rewritten command missing encoded payload: %s", command)
	return ""
}

func shellLongSleepCommand() string {
	if runtime.GOOS == "windows" {
		return "powershell -NoProfile -NonInteractive -Command Start-Sleep -Seconds 10"
	}
	return "sleep 10"
}

func shellBackgroundChildCommand() string {
	if runtime.GOOS == "windows" {
		return "echo spawned child & start /b cmd /c \"ping -n 10 127.0.0.1 > nul\" & ping -n 10 127.0.0.1 > nul"
	}
	return "(sleep 10) & echo spawned child"
}

func TestShellToolExecuteSurfacesCancellationExplicitly(t *testing.T) {
	origRunner := shellToolRunner
	shellToolRunner = func(ctx context.Context, _ *exec.Cmd) ([]byte, error) {
		<-ctx.Done()
		return []byte("partial output"), ctx.Err()
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := NewShellTool(t.TempDir()).Execute(ctx, map[string]any{"command": "go test ./..."})
	if result == nil || !result.IsError {
		t.Fatalf("expected canceled result, got %+v", result)
	}
	if !strings.Contains(result.Output, "partial output") || !strings.Contains(result.Output, "command canceled") {
		t.Fatalf("expected explicit canceled output, got %q", result.Output)
	}
}

func TestShellToolExecutePreservesNonContextExecFailure(t *testing.T) {
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		return []byte("stderr output"), errors.New("exit status 1")
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": "go test ./..."})
	if result == nil || !result.IsError {
		t.Fatalf("expected exec failure result, got %+v", result)
	}
	if !strings.Contains(result.Output, "stderr output") || !strings.Contains(result.Output, "exit status 1") {
		t.Fatalf("expected generic exec failure output, got %q", result.Output)
	}
}

func TestShellToolBlocksDirtyReviewCheckoutMutation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepoWithBranch(t, "agent-gamma-feature", "src/App.tsx", "export default function App() { return null }\n")
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "review-candidate")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "agent-gamma-feature")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-review",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "validation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-review",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-iota",
				"local_path":     checkoutPath,
				"checkout_kind":  "review",
				"branch_name":    "agent-gamma-feature",
				"active_task_id": "task-review",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("mutated\n"), 0o644); err != nil {
			return nil, err
		}
		return []byte("smoke mutated candidate"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-review"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "mutate-review"})
	if result == nil || !result.IsError {
		t.Fatalf("expected dirty review checkout shell to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "shell dirtied read-only or foreign project checkout") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestShellToolAllowsGeneratedDependencyArtifactsInReviewCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepoWithBranch(t, "agent-gamma-feature", "src/App.tsx", "export default function App() { return null }\n")
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "review-candidate")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "agent-gamma-feature")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "VALIDATION",
			Tasks: []map[string]any{{
				"task_id":      "task-review",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "validation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-review",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-theta",
				"local_path":     checkoutPath,
				"checkout_kind":  "review",
				"branch_name":    "agent-gamma-feature",
				"active_task_id": "task-review",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.MkdirAll(filepath.Join(checkoutPath, "node_modules", "vite"), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(checkoutPath, "node_modules", "vite", "index.js"), []byte("generated dependency\n"), 0o644); err != nil {
			return nil, err
		}
		return []byte("dependency install completed"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-theta",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-review"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "npm.cmd install"})
	if result == nil || result.IsError {
		t.Fatalf("expected generated dependency artifacts to be ignored in review checkout, got %+v", result)
	}
	if !strings.Contains(result.Output, "dependency install completed") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestShellToolDoesNotRunWhenProjectCheckoutPreScanFails(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir := t.TempDir()
	brokenCheckout := filepath.Join(workdir, "project-checkouts", "broken")
	if err := os.MkdirAll(brokenCheckout, 0o755); err != nil {
		t.Fatalf("mkdir broken checkout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brokenCheckout, ".git"), []byte("not a valid gitdir\n"), 0o644); err != nil {
		t.Fatalf("write broken git marker: %v", err)
	}

	ran := false
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		ran = true
		return []byte("should not run"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient("http://127.0.0.1:1", "token"),
		"ws",
		"agent-theta",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-review"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "echo should-not-run"})
	if result == nil || !result.IsError {
		t.Fatalf("expected pre-scan failure, got %+v", result)
	}
	if ran {
		t.Fatal("shell runner executed despite project checkout pre-scan failure")
	}
	if !strings.Contains(result.Output, "(command not run)") || !strings.Contains(result.Output, "could not safely scan project checkout") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestGitStatusPorcelainIgnoresGeneratedVerificationArtifacts(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepoWithBranch(t, "agent-gamma-feature", "src/App.tsx", "export default function App() { return null }\n")

	generatedCases := map[string]string{
		filepath.Join("node_modules", "vite", "index.js"):             "generated dependency\n",
		filepath.Join(".tmp", "chrome-profile", "DevToolsActivePort"): "generated browser profile\n",
		filepath.Join(".vite", "deps", "cache.js"):                    "generated vite cache\n",
		filepath.Join("dist", "assets", "index.js"):                   "generated build output\n",
		"tsconfig.tsbuildinfo":                                        "generated ts cache\n",
	}
	for rel, body := range generatedCases {
		t.Run("generated_"+filepath.ToSlash(rel), func(t *testing.T) {
			checkoutPath := filepath.Join(t.TempDir(), "checkout")
			runGitNoDir(t, "clone", remote, checkoutPath)
			runGit(t, checkoutPath, "checkout", "agent-gamma-feature")
			if err := os.MkdirAll(filepath.Dir(filepath.Join(checkoutPath, rel)), 0o755); err != nil {
				t.Fatalf("mkdir generated path: %v", err)
			}
			if err := os.WriteFile(filepath.Join(checkoutPath, rel), []byte(body), 0o644); err != nil {
				t.Fatalf("write generated path: %v", err)
			}
			status, err := gitStatusPorcelain(context.Background(), checkoutPath)
			if err != nil {
				t.Fatalf("gitStatusPorcelain: %v", err)
			}
			if strings.TrimSpace(status) != "" {
				t.Fatalf("expected generated artifact %s to be ignored, status=%q", rel, status)
			}
		})
	}

	productCases := map[string]string{
		"package-lock.json":                     "{}\n",
		filepath.Join("src", "App.tsx"):         "mutated source\n",
		filepath.Join("tests", "smoke.test.ts"): "test mutation\n",
	}
	for rel, body := range productCases {
		t.Run("product_"+filepath.ToSlash(rel), func(t *testing.T) {
			checkoutPath := filepath.Join(t.TempDir(), "checkout")
			runGitNoDir(t, "clone", remote, checkoutPath)
			runGit(t, checkoutPath, "checkout", "agent-gamma-feature")
			if err := os.MkdirAll(filepath.Dir(filepath.Join(checkoutPath, rel)), 0o755); err != nil {
				t.Fatalf("mkdir product path: %v", err)
			}
			if err := os.WriteFile(filepath.Join(checkoutPath, rel), []byte(body), 0o644); err != nil {
				t.Fatalf("write product path: %v", err)
			}
			status, err := gitStatusPorcelain(context.Background(), checkoutPath)
			if err != nil {
				t.Fatalf("gitStatusPorcelain: %v", err)
			}
			if !strings.Contains(filepath.ToSlash(status), filepath.ToSlash(rel)) {
				t.Fatalf("expected product path %s to remain visible, status=%q", rel, status)
			}
		})
	}
}

func TestShellToolBlocksProjectCheckoutMutationWithoutTaskBinding(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "ambient-validation")
	runGitNoDir(t, "clone", remote, checkoutPath)
	if err := os.MkdirAll(filepath.Join(checkoutPath, "src"), 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("ambient mutation\n"), 0o644); err != nil {
			return nil, err
		}
		return []byte("ambient shell mutated checkout"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient("http://127.0.0.1:1", "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "ambient-mutate"})
	if result == nil || !result.IsError {
		t.Fatalf("expected taskless checkout mutation to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "without active project/task binding") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestShellToolBlocksAlreadyDirtyProjectCheckoutWithoutTaskBinding(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepoWithBranch(t, "ambient-feature", "src/App.tsx", "export default function App() { return null }\n")
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "ambient-validation")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "ambient-feature")
	if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("dirty before\n"), 0o644); err != nil {
		t.Fatalf("dirty checkout before shell: %v", err)
	}

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("dirty after\n"), 0o644); err != nil {
			return nil, err
		}
		return []byte("ambient shell changed already dirty checkout"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient("http://127.0.0.1:1", "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "ambient-dirty-mutate"})
	if result == nil || !result.IsError {
		t.Fatalf("expected already-dirty taskless checkout shell to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "without active project/task binding") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestShellToolBlocksCleanHeadMutationInReviewCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepoWithBranch(t, "agent-gamma-feature", "src/App.tsx", "export default function App() { return null }\n")
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "review-candidate")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "agent-gamma-feature")
	runGit(t, checkoutPath, "config", "user.name", "Rhizome Test")
	runGit(t, checkoutPath, "config", "user.email", "rhizome-test@example.invalid")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-review",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "validation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-review",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-iota",
				"local_path":     checkoutPath,
				"checkout_kind":  "review",
				"branch_name":    "agent-gamma-feature",
				"active_task_id": "task-review",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("committed mutation\n"), 0o644); err != nil {
			return nil, err
		}
		runGit(t, checkoutPath, "add", "src/App.tsx")
		runGit(t, checkoutPath, "commit", "-m", "Mutate review checkout")
		return []byte("committed review mutation"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-review"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "commit-review"})
	if result == nil || !result.IsError {
		t.Fatalf("expected clean committed review checkout mutation to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "shell dirtied read-only or foreign project checkout") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestShellToolBlocksAlreadyDirtyReviewCheckoutMutation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepoWithBranch(t, "agent-gamma-feature", "src/App.tsx", "export default function App() { return null }\n")
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "review-candidate")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "agent-gamma-feature")
	if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("dirty before\n"), 0o644); err != nil {
		t.Fatalf("dirty checkout before shell: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-review",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "validation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-review",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-iota",
				"local_path":     checkoutPath,
				"checkout_kind":  "review",
				"branch_name":    "agent-gamma-feature",
				"active_task_id": "task-review",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("dirty after\n"), 0o644); err != nil {
			return nil, err
		}
		return []byte("review shell changed already dirty checkout"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-review"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "review-dirty-mutate"})
	if result == nil || !result.IsError {
		t.Fatalf("expected already-dirty review checkout shell to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "shell dirtied read-only or foreign project checkout") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestShellToolAllowsOwnedImplementationCheckoutMutation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "owned-branch")
	branchName := "agent-iota-owned-task"
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-b", branchName)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["src/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-impl",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-owned",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-iota",
				"local_path":     checkoutPath,
				"checkout_kind":  "clone",
				"branch_name":    branchName,
				"active_task_id": "task-impl",
				"status":         "ACTIVE",
			}},
			Branches: []map[string]any{{
				"branch_id":        "branch-owned",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-owned",
				"agent_id":         "agent-iota",
				"active_task_id":   "task-impl",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"write_scope_json": `{"paths":["src/**"]}`,
				"status":           "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.MkdirAll(filepath.Join(checkoutPath, "src"), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(checkoutPath, "src", "App.tsx"), []byte("owned mutation\n"), 0o644); err != nil {
			return nil, err
		}
		return []byte("implementation mutation"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-impl"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "mutate-owned"})
	if result == nil || result.IsError {
		t.Fatalf("expected owned implementation checkout shell to pass, got %+v", result)
	}
}

func TestShellToolAllowsOwnedCheckoutWhenUnrelatedCheckoutWasAlreadyDirty(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	ownedPath := filepath.Join(workdir, "project-checkouts", "owned-branch")
	foreignPath := filepath.Join(workdir, "project-checkouts", "stale-foreign")
	branchName := "agent-delta-owned-task"
	runGitNoDir(t, "clone", remote, ownedPath)
	runGit(t, ownedPath, "checkout", "-b", branchName)
	runGitNoDir(t, "clone", remote, foreignPath)
	if err := os.WriteFile(filepath.Join(foreignPath, "README.md"), []byte("dirty before shell\n"), 0o644); err != nil {
		t.Fatalf("dirty unrelated checkout before shell: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["src/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-impl",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-owned",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-delta",
				"local_path":     ownedPath,
				"checkout_kind":  "clone",
				"branch_name":    branchName,
				"active_task_id": "task-impl",
				"status":         "ACTIVE",
			}},
			Branches: []map[string]any{{
				"branch_id":        "branch-owned",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-owned",
				"agent_id":         "agent-delta",
				"active_task_id":   "task-impl",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"write_scope_json": `{"paths":["src/**"]}`,
				"status":           "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		if err := os.MkdirAll(filepath.Join(ownedPath, "src"), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(ownedPath, "src", "App.tsx"), []byte("owned mutation\n"), 0o644); err != nil {
			return nil, err
		}
		return []byte("implementation mutation with stale foreign checkout present"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-impl"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "mutate-owned", "workdir": ownedPath})
	if result == nil || result.IsError {
		t.Fatalf("expected stale unrelated checkout not to block owned implementation shell, got %+v", result)
	}
}

func TestShellToolDefaultsToActiveClaimCheckoutWorkdir(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	stalePath := filepath.Join(workdir, "project-checkouts", "stale-vendored")
	for _, path := range []string{activePath, stalePath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir checkout path: %v", err)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "file:///tmp/lua51-subset.git",
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-eval",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-active",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-delta",
				"local_path":     activePath,
				"checkout_kind":  "clone",
				"branch_name":    "agent-delta-eval",
				"active_task_id": "task-eval",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	var gotDir string
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, cmd *exec.Cmd) ([]byte, error) {
		gotDir = cmd.Dir
		return []byte("cwd captured"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				ProjectID:       "project-subpixel",
				TaskID:          "task-eval",
				ClaimRepoID:     "projrepo-1",
				ClaimCheckoutID: "checkout-active",
				ClaimBranchID:   "branch-active",
			}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "echo cwd"})
	if result == nil || result.IsError {
		t.Fatalf("expected active claim checkout default workdir, got %+v", result)
	}
	if !sameProjectLocalPath(gotDir, activePath) {
		t.Fatalf("expected shell dir %s, got %s", activePath, gotDir)
	}
}

func TestShellToolBlocksForeignCheckoutWorkdirDuringActiveClaim(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	stalePath := filepath.Join(workdir, "project-checkouts", "stale-vendored")
	for _, path := range []string{activePath, stalePath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir checkout path: %v", err)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "file:///tmp/lua51-subset.git",
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-eval",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-active",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-delta",
				"local_path":     activePath,
				"checkout_kind":  "clone",
				"branch_name":    "agent-delta-eval",
				"active_task_id": "task-eval",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	ran := false
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		ran = true
		return []byte("should not run"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				ProjectID:       "project-subpixel",
				TaskID:          "task-eval",
				ClaimRepoID:     "projrepo-1",
				ClaimCheckoutID: "checkout-active",
				ClaimBranchID:   "branch-active",
			}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "echo stale", "workdir": stalePath})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign checkout workdir to fail, got %+v", result)
	}
	if ran {
		t.Fatal("shell runner executed despite foreign checkout workdir")
	}
	if !strings.Contains(result.Output, "bound to active claim checkout") || !strings.Contains(result.Output, "stale sibling project-checkouts") {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestShellToolCoercesAgentRootWorkdirDuringActiveClaim(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	if err := os.MkdirAll(activePath, 0o755); err != nil {
		t.Fatalf("mkdir active checkout: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "file:///tmp/lua51-subset.git",
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-eval",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-active",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-delta",
				"local_path":     activePath,
				"checkout_kind":  "clone",
				"branch_name":    "agent-delta-eval",
				"active_task_id": "task-eval",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	var gotDir string
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, cmd *exec.Cmd) ([]byte, error) {
		gotDir = cmd.Dir
		return []byte("ran in claim checkout"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				ProjectID:       "project-subpixel",
				TaskID:          "task-eval",
				ClaimRepoID:     "projrepo-1",
				ClaimCheckoutID: "checkout-active",
				ClaimBranchID:   "branch-active",
			}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{"command": "git status", "workdir": "."})
	if result == nil || result.IsError {
		t.Fatalf("expected agent root workdir to be coerced to active checkout, got %+v", result)
	}
	if !sameProjectLocalPath(gotDir, activePath) {
		t.Fatalf("expected shell dir %s, got %s", activePath, gotDir)
	}
}

func TestShellToolCoercesMissingWorkdirDuringActiveClaim(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	if err := os.MkdirAll(activePath, 0o755); err != nil {
		t.Fatalf("mkdir active checkout: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "file:///tmp/lua51-subset.git",
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-eval",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-active",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-delta",
				"local_path":     activePath,
				"checkout_kind":  "clone",
				"branch_name":    "agent-delta-eval",
				"active_task_id": "task-eval",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	var gotDir string
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, cmd *exec.Cmd) ([]byte, error) {
		gotDir = cmd.Dir
		return []byte("ran in claim checkout"), nil
	}
	t.Cleanup(func() { shellToolRunner = origRunner })

	tool := NewShellTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				ProjectID:       "project-subpixel",
				TaskID:          "task-eval",
				ClaimRepoID:     "projrepo-1",
				ClaimCheckoutID: "checkout-active",
				ClaimBranchID:   "branch-active",
			}
		},
	)
	missing := filepath.Join(workdir, "project-checkouts", "activeclaim-typo")
	result := tool.Execute(context.Background(), map[string]any{"command": "git status", "workdir": missing})
	if result == nil || result.IsError {
		t.Fatalf("expected missing workdir to be coerced to active checkout, got %+v", result)
	}
	if !sameProjectLocalPath(gotDir, activePath) {
		t.Fatalf("expected shell dir %s, got %s", activePath, gotDir)
	}
}

func TestShellToolFailureGuidanceTreatsShellAsTrustedExecution(t *testing.T) {
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		return []byte("'tsc' is not recognized as an internal or external command"), errors.New("exit status 1")
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": "npm run build"})
	if result == nil || !result.IsError {
		t.Fatalf("expected exec failure result, got %+v", result)
	}
	for _, want := range []string{
		"Shell is trusted local execution",
		"adjust the command or workdir/cwd",
		"retry when useful",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected shell guidance to contain %q, got %q", want, result.Output)
		}
	}
}

func TestShellToolFailureGuidanceMentionsWindowsBrowserPaths(t *testing.T) {
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		return []byte("NO_BROWSER_FOUND"), errors.New("exit status 1")
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": "where chrome || echo NO_BROWSER_FOUND"})
	if result == nil || !result.IsError {
		t.Fatalf("expected exec failure result, got %+v", result)
	}
	for _, want := range []string{
		"PATH-only probes",
		"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
		"C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected browser guidance to contain %q, got %q", want, result.Output)
		}
	}
}

func TestShellToolFailureGuidanceMentionsChromeUserDataDir(t *testing.T) {
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		return []byte("DevTools remote debugging requires a non-default data directory. Specify this using --user-data-dir."), errors.New("exit status 1")
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": "chrome.exe --remote-debugging-port=9222"})
	if result == nil || !result.IsError {
		t.Fatalf("expected exec failure result, got %+v", result)
	}
	for _, want := range []string{
		"self-remediable",
		"--user-data-dir=<workdir>\\.tmp\\chrome-profile-<run_id>",
		"unique `--remote-debugging-port`",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected CDP guidance to contain %q, got %q", want, result.Output)
		}
	}
}

func TestShellToolFailureGuidanceMentionsRgAccessDeniedFallbacks(t *testing.T) {
	origRunner := shellToolRunner
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		return []byte("C:\\Program Files\\WindowsApps\\OpenAI.Codex\\resources\\rg.exe: Access is denied."), errors.New("exit status 1")
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{"command": "rg browser"})
	if result == nil || !result.IsError {
		t.Fatalf("expected exec failure result, got %+v", result)
	}
	for _, want := range []string{
		"git grep",
		"Select-String",
		"findstr",
		"direct file reads",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rg fallback guidance to contain %q, got %q", want, result.Output)
		}
	}
}

func TestShellCommandForWindowsUsesPowerShellForPowerShellSyntax(t *testing.T) {
	command := "$ErrorActionPreference='Stop'; $p='C:\\Users\\developer\\Desktop\\agents\\epsilon\\project-checkouts\\demo'; Set-Location $p; if (Test-Path 'README.md') { Get-Content 'README.md' }"
	name, args := shellCommandForGOOS("windows", command)
	if name != "powershell" {
		t.Fatalf("expected powershell for PowerShell-shaped command, got %q args=%v", name, args)
	}
	joined := strings.Join(args, "\x00")
	for _, want := range []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", command} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected PowerShell args to contain %q, got %v", want, args)
		}
	}
}

func TestShellCommandForWindowsUsesPowerShellForGetChildItem(t *testing.T) {
	name, args := shellCommandForGOOS("windows", "Get-ChildItem -Force")
	if name != "powershell" {
		t.Fatalf("expected powershell for Get-ChildItem, got %q args=%v", name, args)
	}
}

func TestShellCommandForWindowsUsesPowerShellForSemicolonSeparatedCommands(t *testing.T) {
	name, args := shellCommandForGOOS("windows", "git status --short; git log -1 --oneline")
	if name != "powershell" {
		t.Fatalf("expected powershell for semicolon-separated command, got %q args=%v", name, args)
	}
}

func TestShellCommandForWindowsKeepsCmdForSemicolonInsideNodePayload(t *testing.T) {
	command := `node -e "if (server && !server.killed) { console.log('a;b'); }"`
	name, args := shellCommandForGOOS("windows", command)
	if name != "cmd" {
		t.Fatalf("expected cmd for semicolon inside quoted node payload, got %q args=%v", name, args)
	}
	if len(args) != 2 || args[0] != "/C" || args[1] != command {
		t.Fatalf("unexpected cmd args: %v", args)
	}
}

func TestShellCommandForWindowsKeepsCmdForCmdSyntax(t *testing.T) {
	name, args := shellCommandForGOOS("windows", "cd project && npm run build")
	if name != "cmd" {
		t.Fatalf("expected cmd for cmd-shaped command, got %q args=%v", name, args)
	}
	if len(args) != 2 || args[0] != "/C" || args[1] != "cd project && npm run build" {
		t.Fatalf("unexpected cmd args: %v", args)
	}
}

func TestShellCommandForWindowsHonorsExplicitPowerShellInvocation(t *testing.T) {
	name, args := shellCommandForGOOS("windows", "powershell -NoProfile -Command Get-ChildItem")
	if name != "powershell" {
		t.Fatalf("expected explicit powershell invocation to use UTF-8 outer powershell, got %q args=%v", name, args)
	}
	if len(args) == 0 || !strings.Contains(args[len(args)-1], "[Console]::OutputEncoding") || !strings.Contains(args[len(args)-1], "powershell -NoProfile -EncodedCommand ") {
		t.Fatalf("unexpected explicit invocation args: %v", args)
	}
}

func TestShellToolExecutesInsideRequestedRelativeWorkdir(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "project-checkouts", "demo")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested workdir: %v", err)
	}
	wantDir, err := filepath.EvalSymlinks(nested)
	if err != nil {
		t.Fatalf("resolve nested workdir: %v", err)
	}

	origRunner := shellToolRunner
	var gotDir string
	shellToolRunner = func(_ context.Context, cmd *exec.Cmd) ([]byte, error) {
		gotDir = cmd.Dir
		return []byte("ok"), nil
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(root).Execute(context.Background(), map[string]any{
		"command": "python3 --version",
		"workdir": filepath.Join("project-checkouts", "demo"),
	})
	if result == nil || result.IsError {
		t.Fatalf("expected shell success, got %+v", result)
	}
	if gotDir != wantDir {
		t.Fatalf("expected shell dir %q, got %q", wantDir, gotDir)
	}
}

func TestShellToolExecutesInsideRequestedAbsoluteCWD(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	nested := filepath.Join(outside, "external-project")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested workdir: %v", err)
	}
	wantDir, err := filepath.EvalSymlinks(nested)
	if err != nil {
		t.Fatalf("resolve nested workdir: %v", err)
	}

	origRunner := shellToolRunner
	var gotDir string
	shellToolRunner = func(_ context.Context, cmd *exec.Cmd) ([]byte, error) {
		gotDir = cmd.Dir
		return []byte("ok"), nil
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(root).Execute(context.Background(), map[string]any{
		"command": "python3 --version",
		"cwd":     nested,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected shell success, got %+v", result)
	}
	if gotDir != wantDir {
		t.Fatalf("expected shell dir %q, got %q", wantDir, gotDir)
	}
}

func TestShellToolAllowsRequestedWorkdirOutsideAgentWorkspace(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	wantDir, err := filepath.EvalSymlinks(outside)
	if err != nil {
		t.Fatalf("resolve outside workdir: %v", err)
	}

	origRunner := shellToolRunner
	var gotDir string
	shellToolRunner = func(_ context.Context, cmd *exec.Cmd) ([]byte, error) {
		gotDir = cmd.Dir
		return []byte("ok"), nil
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	result := NewShellTool(root).Execute(context.Background(), map[string]any{
		"command": "python3 --version",
		"workdir": outside,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected outside workdir to succeed, got %+v", result)
	}
	if gotDir != wantDir {
		t.Fatalf("expected shell dir %q, got %q", wantDir, gotDir)
	}
}

func TestShellToolAllowsTrustedShellSyntaxBeforeRunner(t *testing.T) {
	origRunner := shellToolRunner
	ran := false
	var gotCommand string
	shellToolRunner = func(_ context.Context, _ *exec.Cmd) ([]byte, error) {
		ran = true
		return []byte("trusted shell syntax accepted"), nil
	}
	t.Cleanup(func() {
		shellToolRunner = origRunner
	})

	commands := []string{
		"cd project && npm run build",
		"echo hi > out.txt",
		"cmd /C dir",
		"powershell -Command Get-ChildItem",
		"echo hi | findstr hi",
		"RHIZOME_PATCH_QUEUE_CLAIM_TOKEN=token rhizome approval patch-queue-enable --workspace-id ws --project-id project --queue-id queue --item-id item --actor operator-1",
	}
	for _, command := range commands {
		ran = false
		gotCommand = command
		result := NewShellTool(t.TempDir()).Execute(context.Background(), map[string]any{
			"command": command,
		})
		if result == nil || result.IsError {
			t.Fatalf("expected trusted shell result for %q, got %+v", command, result)
		}
		if !ran {
			t.Fatalf("expected shell runner to be called for trusted command %q", gotCommand)
		}
	}
}
