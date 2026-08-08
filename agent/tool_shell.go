package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
)

var (
	shellToolTimeoutBudget        = 60 * time.Second
	shellToolKillWaitBudget       = 5 * time.Second
	shellToolPostRunScanBudget    = 5 * time.Second
	shellToolPostRunCleanupPasses = 3
	shellToolPostRunCleanupGap    = 250 * time.Millisecond
	shellToolOutputLimitBytes     = 1024 * 1024
	shellToolDiskMinFreeBytes     = uint64(1024 * 1024 * 1024)
	shellToolRunner               = runShellToolCommand
	shellToolWorkdirCleanupRunner = cleanupShellCommandWorkdirProcesses
	shellToolFreeDiskBytes        = diskFreeBytes
)

var (
	powerShellCommandFlagRe        = regexp.MustCompile(`(?i)(^|\s)(-command|-c)\s+`)
	powerShellEncodedCommandFlagRe = regexp.MustCompile(`(?i)(^|\s)(-encodedcommand|-enc|-e)\s+([A-Za-z0-9+/=]+)`)
)

// ShellTool executes shell commands within the agent's workdir.
type ShellTool struct {
	workdir        string
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewShellTool(workdir string) *ShellTool {
	return &ShellTool{workdir: workdir}
}

func (t *ShellTool) WithProjectCheckoutGuard(client *RhizomeClient, workspaceID, agentID string, runtimeBinding func() AgentRuntimeBinding) *ShellTool {
	t.client = client
	t.workspaceID = strings.TrimSpace(workspaceID)
	t.agentID = strings.TrimSpace(agentID)
	t.runtimeBinding = runtimeBinding
	return t
}

func (t *ShellTool) Name() string { return "shell" }

func (t *ShellTool) Description() string {
	return "Execute a trusted local shell command. By default it runs inside the agent workspace; optional workdir/cwd may be relative to the agent workspace or an absolute directory anywhere on the host. On Windows this auto-selects PowerShell for PowerShell-shaped commands and otherwise uses cmd.exe. Normal shell syntax is allowed, including cd, pipes, redirects, control operators, nested shells, package managers, git commands, and file creation/editing. Background children are treated as part of one bounded command; for dev-server/browser smoke, start, poll, test, and clean up in the same command. Use RHIZOME_SMOKE_PORT_HINT or another unique high port, not default/shared ports such as 3000, 4173, or 5173, and assert the loaded page matches the target product before accepting browser evidence. Returns stdout+stderr. Timeout: 60s."
}

func (t *ShellTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"command": map[string]any{
				"type":        "string",
				"description": "Shell command to execute",
			},
			"workdir": map[string]any{
				"type":        "string",
				"description": "Optional working directory. Relative paths resolve under the agent workspace; absolute paths may point anywhere on the host.",
			},
			"cwd": map[string]any{
				"type":        "string",
				"description": "Alias for workdir.",
			},
		},
		"required": []string{"command"},
	}
}

func (t *ShellTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	command, _ := args["command"].(string)
	if command == "" {
		return &ToolResult{Output: "command is required", IsError: true}
	}

	workdir, err := t.resolveExecutionWorkdir(ctx, args)
	if err != nil {
		return &ToolResult{Output: formatShellToolError("", err.Error()), IsError: true}
	}
	if err := t.preflightDiskBackpressure(command, workdir); err != nil {
		return &ToolResult{Output: formatShellToolError("(command not run)", err.Error()), IsError: true}
	}

	ctx, cancel := context.WithTimeout(ctx, shellToolTimeoutBudget)
	defer cancel()

	beforeCheckoutStatus, beforeCheckoutScanErrors := t.projectCheckoutStatusSnapshot(ctx)
	if checkoutMutationErr := t.projectCheckoutScanFailureError(beforeCheckoutScanErrors); checkoutMutationErr != nil {
		return &ToolResult{Output: appendShellCheckoutMutationError("(command not run)", checkoutMutationErr), IsError: true}
	}
	name, shellArgs := shellCommandForGOOS(runtime.GOOS, command)
	cmd := exec.Command(name, shellArgs...)
	cmd.Dir = workdir
	cmd.Env = shellToolEnv(workdir, t.agentID)

	out, err := shellToolRunner(ctx, cmd)
	output := string(out)
	scanCtx, scanCancel := context.WithTimeout(context.Background(), shellToolPostRunScanBudget)
	checkoutMutationErr := t.validateProjectCheckoutShellMutation(scanCtx, beforeCheckoutStatus, beforeCheckoutScanErrors, workdir)
	scanCancel()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return &ToolResult{Output: appendShellCheckoutMutationError(formatShellToolError(output, fmt.Sprintf("command timed out after %s", shellToolTimeoutBudget)), checkoutMutationErr), IsError: true}
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return &ToolResult{Output: appendShellCheckoutMutationError(formatShellToolError(output, "command canceled"), checkoutMutationErr), IsError: true}
		}
		output = formatShellToolError(output, fmt.Sprintf("%v", err))
		return &ToolResult{Output: appendShellCheckoutMutationError(output, checkoutMutationErr), IsError: true}
	}
	if checkoutMutationErr != nil {
		return &ToolResult{Output: appendShellCheckoutMutationError(firstNonEmpty(output, "(no output)"), checkoutMutationErr), IsError: true}
	}
	if output == "" {
		output = "(no output)"
	}
	return &ToolResult{Output: output}
}

func appendShellCheckoutMutationError(output string, err error) string {
	if err == nil {
		return output
	}
	return strings.TrimRight(output, "\r\n") + "\nerror: " + err.Error()
}

func (t *ShellTool) projectCheckoutScanFailureError(scanErrors []string) error {
	if t == nil || t.client == nil || t.workspaceID == "" || t.agentID == "" || t.runtimeBinding == nil {
		return nil
	}
	if scanErrors := uniqueNonEmptyStrings(scanErrors); len(scanErrors) > 0 {
		return fmt.Errorf("shell could not safely scan project checkout(s) for generated-artifact-aware mutation guard: %s. Dependency install and smoke setup may write generated artifacts, but if checkout scan cannot be prepared, use a fresh validation checkout or a writable temp clone outside project-checkouts before continuing", strings.Join(scanErrors, "; "))
	}
	return nil
}

type projectCheckoutShellState struct {
	Branch string
	Head   string
	Status string
}

func (s projectCheckoutShellState) fingerprint() string {
	return strings.Join([]string{strings.TrimSpace(s.Branch), strings.TrimSpace(s.Head), strings.TrimSpace(s.Status)}, "\x00")
}

func (t *ShellTool) projectCheckoutStatusSnapshot(ctx context.Context) (map[string]projectCheckoutShellState, []string) {
	if t == nil || strings.TrimSpace(t.workdir) == "" {
		return nil, nil
	}
	roots := projectCheckoutGitRoots(t.workdir)
	if len(roots) == 0 {
		return nil, nil
	}
	out := make(map[string]projectCheckoutShellState, len(roots))
	var scanErrors []string
	for _, root := range roots {
		status, err := gitStatusPorcelain(ctx, root)
		if err != nil {
			scanErrors = append(scanErrors, fmt.Sprintf("%s: git status failed after generated-artifact exclude setup: %v", root, err))
			continue
		}
		branch, err := currentGitBranch(ctx, root)
		if err != nil {
			scanErrors = append(scanErrors, fmt.Sprintf("%s: read current branch failed: %v", root, err))
			continue
		}
		head, err := gitRevParse(ctx, root, "HEAD")
		if err != nil {
			scanErrors = append(scanErrors, fmt.Sprintf("%s: read HEAD failed: %v", root, err))
			continue
		}
		out[filepath.Clean(root)] = projectCheckoutShellState{
			Branch: branch,
			Head:   head,
			Status: status,
		}
	}
	return out, scanErrors
}

func (t *ShellTool) validateProjectCheckoutShellMutation(ctx context.Context, before map[string]projectCheckoutShellState, beforeScanErrors []string, executionWorkdir string) error {
	if t == nil || t.client == nil || t.workspaceID == "" || t.agentID == "" || t.runtimeBinding == nil {
		return nil
	}
	after, afterScanErrors := t.projectCheckoutStatusSnapshot(ctx)
	if err := t.projectCheckoutScanFailureError(append(beforeScanErrors, afterScanErrors...)); err != nil {
		return err
	}
	if len(after) == 0 {
		return nil
	}
	var changed []string
	for root, state := range after {
		if strings.TrimSpace(state.fingerprint()) == "" {
			continue
		}
		if before != nil && before[root].fingerprint() == state.fingerprint() {
			continue
		}
		changed = append(changed, root)
	}
	binding := t.runtimeBinding()
	projectID := strings.TrimSpace(binding.ProjectID)
	taskID := strings.TrimSpace(binding.TaskID)
	includeStableDirtyRoots := projectID == "" || taskID == "" || !shellWorkdirUnderAnyCheckoutRoot(executionWorkdir, after)
	dirtyRoots := dirtyProjectCheckoutRootsChangedOrFocused(before, after, executionWorkdir, includeStableDirtyRoots)
	suspect := uniqueTrimmedCSVStrings(append(changed, dirtyRoots...))
	if projectID == "" || taskID == "" {
		if len(suspect) == 0 {
			return nil
		}
		return fmt.Errorf("shell dirtied project checkout(s) without active project/task binding: %s. Ambient and unclaimed shell usage may inspect or test, but must not mutate project-checkouts; create or claim a concrete task before editing product files", strings.Join(suspect, "; "))
	}
	if len(suspect) == 0 {
		return nil
	}
	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return fmt.Errorf("shell dirtied project checkout(s) but could not verify ownership for project %s: %v", projectID, err)
	}
	var disallowed []string
	for _, root := range suspect {
		if _, _, err := projectCheckoutWriteAuthority(ctx, coordination, root, t.agentID, taskID, "shell"); err != nil {
			disallowed = append(disallowed, fmt.Sprintf("%s (%v)", root, err))
		}
	}
	if len(disallowed) > 0 {
		return fmt.Errorf("shell dirtied read-only or foreign project checkout(s): %s. Review/validation checkouts are disposable and read-only; create or claim a revision implementation task and materialize an owned branch before editing product files", strings.Join(disallowed, "; "))
	}
	return nil
}

func dirtyProjectCheckoutRoots(snapshot map[string]projectCheckoutShellState) []string {
	if len(snapshot) == 0 {
		return nil
	}
	var roots []string
	for root, state := range snapshot {
		if strings.TrimSpace(state.Status) != "" {
			roots = append(roots, root)
		}
	}
	return roots
}

func dirtyProjectCheckoutRootsChangedOrFocused(before, after map[string]projectCheckoutShellState, executionWorkdir string, includeStableDirtyRoots bool) []string {
	if len(after) == 0 {
		return nil
	}
	var roots []string
	for root, state := range after {
		if strings.TrimSpace(state.Status) == "" {
			continue
		}
		if includeStableDirtyRoots {
			roots = append(roots, root)
			continue
		}
		beforeState, hadBefore := before[root]
		if !hadBefore || strings.TrimSpace(beforeState.Status) == "" || beforeState.fingerprint() != state.fingerprint() || shellWorkdirUnderCheckoutRoot(executionWorkdir, root) {
			roots = append(roots, root)
		}
	}
	return roots
}

func shellWorkdirUnderAnyCheckoutRoot(workdir string, snapshot map[string]projectCheckoutShellState) bool {
	for root := range snapshot {
		if shellWorkdirUnderCheckoutRoot(workdir, root) {
			return true
		}
	}
	return false
}

func shellWorkdirUnderCheckoutRoot(workdir, root string) bool {
	workdir = strings.TrimSpace(workdir)
	root = strings.TrimSpace(root)
	if workdir == "" || root == "" {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(workdir))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func projectCheckoutGitRoots(workdir string) []string {
	root := workspacePathPolicyRoot(workdir)
	if root == "" {
		return nil
	}
	projectCheckouts := filepath.Join(root, "project-checkouts")
	if info, err := os.Stat(projectCheckouts); err != nil || !info.IsDir() {
		return nil
	}
	var roots []string
	_ = filepath.WalkDir(projectCheckouts, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil {
			return nil
		}
		name := entry.Name()
		if name == ".git" {
			roots = append(roots, filepath.Clean(filepath.Dir(path)))
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			switch strings.ToLower(name) {
			case ".cache", ".tmp", "dist", "node_modules":
				return filepath.SkipDir
			}
		}
		return nil
	})
	return dedupeShellCleanupRoots(roots)
}

func gitStatusPorcelain(ctx context.Context, localPath string) (string, error) {
	if err := ensureGitGeneratedArtifactExcludes(ctx, localPath); err != nil {
		return "", err
	}
	out, err := gitCombined(ctx, localPath, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return "", err
	}
	lines := gitOutputNonDiagnosticLines(out)
	return strings.Join(lines, "\n"), nil
}

func shellToolEnv(workdir, agentID string) []string {
	env := appendKnownBrowserDirsToEnvPath(os.Environ())
	if trimmed := strings.TrimSpace(workdir); trimmed != "" {
		env = append(env, "RHIZOME_SHELL_WORKDIR="+trimmed)
	}
	if trimmed := strings.TrimSpace(agentID); trimmed != "" {
		env = append(env, "RHIZOME_AGENT_ID="+trimmed)
	}
	if port := rhizomeShellSmokePortHint(workdir, agentID); port != "" {
		env = append(env, "RHIZOME_SMOKE_PORT_HINT="+port)
	}
	if cacheRoot := shellToolCacheRoot(workdir); cacheRoot != "" {
		_ = os.MkdirAll(cacheRoot, 0o755)
		npmCache := filepath.Join(cacheRoot, "npm")
		playwrightBrowsers := filepath.Join(cacheRoot, "ms-playwright")
		pnpmHome := filepath.Join(cacheRoot, "pnpm-home")
		pnpmStore := filepath.Join(cacheRoot, "pnpm-store")
		goCache := filepath.Join(cacheRoot, "go-build")
		localAppData := filepath.Join(shellToolAgentRootForWorkdir(workdir), ".local-data", "LocalAppData")
		appData := filepath.Join(shellToolAgentRootForWorkdir(workdir), ".local-data", "AppData", "Roaming")
		for _, dir := range []string{npmCache, playwrightBrowsers, pnpmHome, pnpmStore, goCache, localAppData, appData} {
			_ = os.MkdirAll(dir, 0o755)
		}
		env = append(env,
			"XDG_CACHE_HOME="+cacheRoot,
			"npm_config_cache="+npmCache,
			"NPM_CONFIG_CACHE="+npmCache,
			"PLAYWRIGHT_BROWSERS_PATH="+playwrightBrowsers,
			"PNPM_HOME="+pnpmHome,
			"PNPM_STORE_DIR="+pnpmStore,
			"npm_config_store_dir="+pnpmStore,
			"GOCACHE="+goCache,
			"LOCALAPPDATA="+localAppData,
			"LocalAppData="+localAppData,
			"APPDATA="+appData,
		)
	}
	return env
}

func shellToolCacheRoot(workdir string) string {
	root := shellToolAgentRootForWorkdir(workdir)
	if root == "" {
		return ""
	}
	return filepath.Join(root, ".cache")
}

func shellToolAgentRootForWorkdir(workdir string) string {
	root := workspacePathPolicyRoot(workdir)
	if root == "" {
		return ""
	}
	for cur := root; cur != "" && cur != filepath.Dir(cur); cur = filepath.Dir(cur) {
		name := strings.ToLower(filepath.Base(cur))
		switch name {
		case "project-checkouts", "project-integration", "validation-checkouts", "review-checkouts":
			parent := filepath.Dir(cur)
			if parent != "" && parent != cur {
				return filepath.Clean(parent)
			}
		}
	}
	return root
}

func (t *ShellTool) preflightDiskBackpressure(command, workdir string) error {
	if !shellToolDiskPressureCommand(command) || shellToolFreeDiskBytes == nil || shellToolDiskMinFreeBytes == 0 {
		return nil
	}
	free, err := shellToolFreeDiskBytes(workdir)
	if err != nil || free >= shellToolDiskMinFreeBytes {
		return nil
	}
	root := shellToolAgentRootForWorkdir(firstNonEmpty(t.workdir, workdir))
	if root == "" {
		root = shellToolAgentRootForWorkdir(workdir)
	}
	cleanupNote := "no eligible agent root found"
	if root != "" {
		if note, cleanupErr := shellToolCleanupGeneratedArtifacts(root, workdir); cleanupErr == nil && strings.TrimSpace(note) != "" {
			cleanupNote = note
		} else if cleanupErr != nil {
			cleanupNote = cleanupErr.Error()
		}
	}
	if freeAfter, err := shellToolFreeDiskBytes(workdir); err == nil {
		free = freeAfter
	}
	if free < shellToolDiskMinFreeBytes {
		return fmt.Errorf("shell disk backpressure: only %s free before a dependency/build/browser command; generated-artifact cleanup attempted (%s), but free space is still below %s. Stop the run or reclaim agent node_modules/dist/tmp/npm-cache artifacts before retrying", humanBytes(free), cleanupNote, humanBytes(shellToolDiskMinFreeBytes))
	}
	return nil
}

func shellToolDiskPressureCommand(command string) bool {
	normalized := strings.ToLower(strings.TrimSpace(command))
	if normalized == "" {
		return false
	}
	return containsAnySignal(normalized, []string{
		"npm install",
		"npm.cmd install",
		"npm ci",
		"npm.cmd ci",
		"pnpm install",
		"pnpm.cmd install",
		"yarn install",
		"yarn.cmd install",
		"npx ",
		"npx.cmd ",
		"playwright",
		"vite build",
		"npm run build",
		"npm.cmd run build",
		"npm test",
		"npm.cmd test",
	})
}

func shellToolCleanupGeneratedArtifacts(agentRoot, preserveWorkdir string) (string, error) {
	root := workspacePathPolicyRoot(agentRoot)
	if root == "" {
		return "", fmt.Errorf("invalid agent root for cleanup")
	}
	preserve := workspacePathPolicyRoot(preserveWorkdir)
	if strings.EqualFold(filepath.Clean(preserve), filepath.Clean(root)) {
		preserve = ""
	}
	targets := []string{
		filepath.Join(root, ".tmp"),
		filepath.Join(root, "tmp"),
		filepath.Join(root, ".codex-home", ".tmp"),
		filepath.Join(root, ".codex-home", "tmp"),
		filepath.Join(root, ".home", "npm-cache"),
		filepath.Join(root, ".cache", "npm"),
		filepath.Join(root, ".cache", "pnpm-store"),
		filepath.Join(root, ".cache", "pnpm-home"),
		filepath.Join(root, ".cache", "ms-playwright"),
		filepath.Join(root, "validation-checkouts"),
		filepath.Join(root, "review-checkouts"),
	}
	projectCheckouts := filepath.Join(root, "project-checkouts")
	if entries, err := os.ReadDir(projectCheckouts); err == nil {
		for _, entry := range entries {
			if entry == nil || !entry.IsDir() {
				continue
			}
			checkout := filepath.Join(projectCheckouts, entry.Name())
			targets = append(targets,
				filepath.Join(checkout, "node_modules"),
				filepath.Join(checkout, "dist"),
				filepath.Join(checkout, ".vite"),
			)
		}
	}
	removed := 0
	for _, target := range uniqueTrimmedCSVStrings(targets) {
		clean := workspacePathPolicyRoot(target)
		if clean == "" || clean == root || !pathWithinRoot(clean, root) || shellToolCleanupPathOverlaps(clean, preserve) {
			continue
		}
		if _, err := os.Stat(clean); err != nil {
			continue
		}
		if err := os.RemoveAll(clean); err != nil {
			return fmt.Sprintf("removed=%d", removed), fmt.Errorf("generated-artifact cleanup failed for %s: %w", clean, err)
		}
		removed++
	}
	return fmt.Sprintf("removed=%d root=%s", removed, root), nil
}

func shellToolCleanupPathOverlaps(target, preserve string) bool {
	if strings.TrimSpace(target) == "" || strings.TrimSpace(preserve) == "" {
		return false
	}
	target = filepath.Clean(target)
	preserve = filepath.Clean(preserve)
	return pathWithinRoot(target, preserve) || pathWithinRoot(preserve, target)
}

func pathWithinRoot(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	if strings.EqualFold(path, root) {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".."
}

func humanBytes(v uint64) string {
	const gb = 1024 * 1024 * 1024
	const mb = 1024 * 1024
	if v >= gb {
		return fmt.Sprintf("%.1fGB", float64(v)/float64(gb))
	}
	if v >= mb {
		return fmt.Sprintf("%.1fMB", float64(v)/float64(mb))
	}
	return fmt.Sprintf("%dB", v)
}

func rhizomeShellSmokePortHint(workdir, agentID string) string {
	seed := strings.TrimSpace(workdir) + "\x00" + strings.TrimSpace(agentID)
	if strings.Trim(seed, "\x00 \t\r\n") == "" {
		return ""
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(filepath.ToSlash(seed))))
	port := 43000 + int(h.Sum32()%18000)
	return fmt.Sprintf("%d", port)
}

func runShellToolCommand(ctx context.Context, cmd *exec.Cmd) ([]byte, error) {
	output := newBoundedShellOutputBuffer(shellToolOutputLimitBytes)
	cmd.Stdout = output
	cmd.Stderr = output
	configureShellCommandProcess(cmd)

	if err := ctx.Err(); err != nil {
		return output.Bytes(), err
	}
	if err := cmd.Start(); err != nil {
		return output.Bytes(), err
	}

	processHandle, err := attachShellCommandProcessTree(cmd)
	if err != nil {
		output.WriteString(fmt.Sprintf("[shell containment] process tree containment degraded: %v; timeout cleanup will use pid fallback\n", err))
		processHandle = fallbackShellCommandProcessHandle(cmd)
	}
	if processHandle != nil {
		defer processHandle.release()
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case err := <-waitCh:
		recordShellWorkdirCleanup(output, cmd.Dir)
		return output.Bytes(), err
	case <-ctx.Done():
		output.WriteString(fmt.Sprintf("\n[shell cleanup] context ended; process tree kill attempted; wait grace %s\n", shellToolKillWaitBudget))
		var killErr error
		if processHandle != nil {
			killErr = processHandle.terminate()
		} else if cmd.Process != nil {
			killErr = killShellCommandProcessTreeByPID(cmd.Process.Pid)
		}
		recordShellWorkdirCleanup(output, cmd.Dir)
		select {
		case waitErr := <-waitCh:
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[shell cleanup] process tree kill error: %v\n", killErr))
			}
			if waitErr != nil {
				return output.Bytes(), waitErr
			}
			return output.Bytes(), ctx.Err()
		case <-time.After(shellToolKillWaitBudget):
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[shell cleanup] process tree kill error: %v\n", killErr))
			}
			output.WriteString(fmt.Sprintf("[shell cleanup] process wait grace expired after %s\n", shellToolKillWaitBudget))
			return output.Bytes(), ctx.Err()
		}
	}
}

func recordShellWorkdirCleanup(output *boundedShellOutputBuffer, workdir string) {
	if !shellWorkdirProcessCleanupEligible(workdir) {
		return
	}
	passes := shellToolPostRunCleanupPasses
	if passes < 1 {
		passes = 1
	}
	var notes []string
	var errors []string
	for i := 0; i < passes; i++ {
		note, err := shellToolWorkdirCleanupRunner(workdir)
		if strings.TrimSpace(note) != "" {
			notes = append(notes, strings.TrimSpace(note))
		}
		if err != nil {
			errors = append(errors, err.Error())
		}
		if i+1 < passes && shellToolPostRunCleanupGap > 0 {
			time.Sleep(shellToolPostRunCleanupGap)
		}
	}
	for _, note := range uniqueNonEmptyStrings(notes) {
		output.WriteString(fmt.Sprintf("[shell cleanup] workdir cleanup: %s\n", note))
	}
	for _, msg := range uniqueNonEmptyStrings(errors) {
		output.WriteString(fmt.Sprintf("[shell cleanup] workdir cleanup error: %s\n", msg))
	}
}

func shellWorkdirProcessCleanupEligible(workdir string) bool {
	return len(shellWorkdirProcessCleanupRoots(workdir)) > 0
}

func shellWorkdirProcessCleanupRoots(workdir string) []string {
	cleaned := strings.TrimSpace(workdir)
	if cleaned == "" {
		return nil
	}
	cleaned = filepath.Clean(cleaned)
	var roots []string
	for _, segment := range strings.Split(filepath.ToSlash(cleaned), "/") {
		if strings.EqualFold(segment, "project-checkouts") {
			roots = append(roots, cleaned)
			break
		}
	}
	agentProjectCheckouts := filepath.Join(cleaned, "project-checkouts")
	if info, err := os.Stat(agentProjectCheckouts); err == nil && info.IsDir() {
		roots = append(roots, agentProjectCheckouts)
	}
	return dedupeShellCleanupRoots(roots)
}

func dedupeShellCleanupRoots(roots []string) []string {
	if len(roots) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(roots))
	out := make([]string, 0, len(roots))
	for _, root := range roots {
		cleaned := filepath.Clean(strings.TrimSpace(root))
		if cleaned == "" {
			continue
		}
		key := strings.ToLower(filepath.ToSlash(cleaned))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, cleaned)
	}
	return out
}

func waitForShellCommandExit(cmd *exec.Cmd, timeout time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()
	select {
	case err := <-waitCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("process wait grace expired after %s", timeout)
	}
}

type boundedShellOutputBuffer struct {
	mu      sync.Mutex
	limit   int
	data    []byte
	dropped int64
}

func newBoundedShellOutputBuffer(limit int) *boundedShellOutputBuffer {
	if limit <= 0 {
		limit = 1024 * 1024
	}
	return &boundedShellOutputBuffer{limit: limit}
}

func (b *boundedShellOutputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) < b.limit {
		remaining := b.limit - len(b.data)
		n := len(p)
		if n > remaining {
			n = remaining
		}
		b.data = append(b.data, p[:n]...)
		if n < len(p) {
			b.dropped += int64(len(p) - n)
		}
	} else {
		b.dropped += int64(len(p))
	}
	return len(p), nil
}

func (b *boundedShellOutputBuffer) WriteString(s string) {
	_, _ = b.Write([]byte(s))
}

func (b *boundedShellOutputBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]byte(nil), b.data...)
	if b.dropped > 0 {
		out = append(out, []byte(fmt.Sprintf("\n[shell output truncated: %d bytes omitted]\n", b.dropped))...)
	}
	return out
}

func formatShellToolError(output, message string) string {
	output = strings.TrimRight(output, "\r\n")
	message = strings.TrimSpace(message)
	guidance := "Shell is trusted local execution. Inspect the error/output, adjust the command or workdir/cwd, and retry when useful; record a blocker/tension only when a concrete dependency, executable, permission, or environment capability is missing."
	combined := strings.ToLower(output + "\n" + message)
	if strings.Contains(combined, "python: not found") || strings.Contains(combined, "python: command not found") {
		guidance += " If the failed command used python and the host is Unix-like, retry once with python3 using the same workdir/cwd before declaring the check blocked."
	}
	if strings.Contains(combined, "no_browser_found") || strings.Contains(combined, "chrome") || strings.Contains(combined, "msedge") || strings.Contains(combined, "firefox") {
		guidance += " For browser/UI validation on Windows, do not stop at PATH-only probes such as `where chrome`; also check common install paths like `C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe` and `C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe`, or invoke a discovered browser by absolute path."
	}
	if strings.Contains(combined, "devtools remote debugging requires a non-default data directory") ||
		strings.Contains(combined, "--user-data-dir") && strings.Contains(combined, "remote debugging") {
		guidance += " Chrome/Edge CDP launch is self-remediable: retry once with a fresh temporary profile such as `--user-data-dir=<workdir>\\.tmp\\chrome-profile-<run_id>` plus a unique `--remote-debugging-port`, then close that process after the smoke check. If raw CDP remains brittle on Windows, run the smoke from a writable temp clone with `npm.cmd`, `playwright-core --no-save`, and an absolute Chrome/Edge executable path instead of mutating a review checkout."
	}
	if strings.Contains(combined, "cannot find module 'playwright-core'") ||
		strings.Contains(combined, `cannot find module "playwright-core"`) ||
		strings.Contains(combined, "err_module_not_found") && strings.Contains(combined, "playwright-core") {
		guidance += " Missing Playwright tooling is self-remediable for visual QA: create an agent-local temp tooling directory outside `project-checkouts` (for example `$env:RHIZOME_SHELL_WORKDIR\\.tmp\\browser-tooling`), run `npm.cmd init -y` there if needed, run `npm.cmd install playwright-core --no-save`, set `NODE_PATH` to that directory's `node_modules`, then rerun the screenshot smoke from the candidate checkout using an absolute Chrome/Edge executable path. Do not mark visual acceptance blocked merely because the reviewed product does not vendor Playwright."
	}
	if strings.Contains(combined, "launchpersistentcontext") && strings.Contains(combined, "userdata") {
		guidance += " Playwright persistent context arguments are self-remediable: call `browserType.launchPersistentContext(userDataDir, options)` with the user data directory as the first positional argument, or use `browserType.launch(options)` for a throwaway browser smoke."
	}
	if strings.Contains(combined, "start-process") && (strings.Contains(combined, "redirectstandardoutput") || strings.Contains(combined, "redirectstandarderror") || strings.Contains(combined, "cannot be run because")) {
		guidance += " Windows `Start-Process` redirection failures are self-remediable: create the log directory first, use distinct stdout/stderr files, or avoid `Start-Process` for browser smoke by running one bounded PowerShell command that starts the dev server, polls the exact high-port URL, runs the smoke, and stops the owned process in a finally block."
	}
	if strings.Contains(combined, "enoent") && strings.Contains(combined, "project-checkout") {
		guidance += " Re-check the exact `project_checkout_materialize` output path before retrying; do not guess singular/plural checkout paths. Prefer the returned absolute checkout path as shell cwd."
	}
	if strings.Contains(combined, "localhost") || strings.Contains(combined, "127.0.0.1") || strings.Contains(combined, "purple deception") || strings.Contains(combined, "wrong product") {
		guidance += " Browser smoke can be fooled by stale local preview servers. Start the target checkout on a unique high port, preferably `$env:RHIZOME_SMOKE_PORT_HINT`, with strict-port behavior; probe that exact URL; assert title/body/product markers match the expected app; and treat any unrelated app content as failed validation, not success."
	}
	if strings.Contains(combined, "npm.ps1") || strings.Contains(combined, "npx.ps1") || strings.Contains(combined, "pnpm.ps1") || strings.Contains(combined, "yarn.ps1") {
		guidance += " On Windows, do not use `Start-Process -FilePath npm/npx/pnpm/yarn`; use `npm.cmd`, `npx.cmd`, `pnpm.cmd`, or `yarn.cmd` so PowerShell does not launch the package-manager `.ps1` script through file association."
	}
	if strings.Contains(combined, "rg.exe") && strings.Contains(combined, "access is denied") ||
		strings.Contains(combined, "rg.exe") && strings.Contains(combined, "permission denied") {
		guidance += " If bundled `rg.exe` is blocked by WindowsApps permissions, search with `git grep`, PowerShell `Select-String`, `findstr`, or direct file reads instead of treating repository search as unavailable."
	}
	if output == "" {
		return "error: " + message + "\n" + guidance
	}
	return output + "\nerror: " + message + "\n" + guidance
}

func (t *ShellTool) resolveExecutionWorkdir(ctx context.Context, args map[string]any) (string, error) {
	target := strings.TrimSpace(t.workdir)
	requested := strings.TrimSpace(stringMapArg(args, "workdir"))
	if requested == "" {
		requested = strings.TrimSpace(stringMapArg(args, "cwd"))
	}
	if requested == "" {
		if claimWorkdir, ok, err := t.activeClaimCheckoutWorkdir(ctx); err != nil {
			return "", err
		} else if ok {
			return claimWorkdir, nil
		}
		if target == "" {
			return "", nil
		}
		resolved, err := filepath.Abs(target)
		if err != nil {
			return "", fmt.Errorf("resolve shell workdir: %w", err)
		}
		return resolved, nil
	}
	if claimWorkdir, ok, err := t.activeClaimCheckoutWorkdir(ctx); err != nil {
		return "", err
	} else if ok {
		resolved, err := trustedShellWorkdir(target, requested)
		if err != nil {
			return claimWorkdir, nil
		}
		if shellWorkdirUnderCheckoutRoot(resolved, claimWorkdir) {
			return resolved, nil
		}
		if pathUnderProjectCheckouts(t.workdir, resolved) {
			if err := t.validateClaimCheckoutWorkdir(ctx, resolved); err != nil {
				return "", err
			}
		}
		return claimWorkdir, nil
	}
	return trustedShellWorkdir(target, requested)
}

func (t *ShellTool) activeClaimCheckoutWorkdir(ctx context.Context) (string, bool, error) {
	checkout, ok, err := t.activeClaimCheckout(ctx)
	if err != nil || !ok {
		return "", ok, err
	}
	resolved, err := trustedShellWorkdir("", checkout.LocalPath)
	if err != nil {
		return "", true, fmt.Errorf("shell cannot use active claim checkout %s at %s: %w", strings.TrimSpace(checkout.CheckoutID), strings.TrimSpace(checkout.LocalPath), err)
	}
	return resolved, true, nil
}

func (t *ShellTool) validateClaimCheckoutWorkdir(ctx context.Context, workdir string) error {
	checkout, ok, err := t.activeClaimCheckout(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	resolvedClaim, err := trustedShellWorkdir("", checkout.LocalPath)
	if err != nil {
		return fmt.Errorf("shell cannot verify active claim checkout %s at %s: %w", strings.TrimSpace(checkout.CheckoutID), strings.TrimSpace(checkout.LocalPath), err)
	}
	if shellWorkdirUnderCheckoutRoot(workdir, resolvedClaim) {
		return nil
	}
	return fmt.Errorf("shell refuses to run in %s because current task %s is bound to active claim checkout %s at %s. Omit workdir/cwd or use the active claim checkout path; do not inspect agent root, temp roots, or stale sibling project-checkouts for this claimed task", workdir, strings.TrimSpace(t.runtimeBinding().TaskID), strings.TrimSpace(checkout.CheckoutID), resolvedClaim)
}

func (t *ShellTool) activeClaimCheckout(ctx context.Context) (ProjectCheckoutRecord, bool, error) {
	if t == nil || t.client == nil || t.workspaceID == "" || t.agentID == "" || t.runtimeBinding == nil {
		return ProjectCheckoutRecord{}, false, nil
	}
	return activeRuntimeClaimCheckout(ctx, t.client, t.workspaceID, t.agentID, t.runtimeBinding())
}

func activeRuntimeClaimCheckout(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, binding AgentRuntimeBinding) (ProjectCheckoutRecord, bool, error) {
	if client == nil || strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(agentID) == "" {
		return ProjectCheckoutRecord{}, false, nil
	}
	projectID := strings.TrimSpace(binding.ProjectID)
	taskID := strings.TrimSpace(binding.TaskID)
	checkoutID := strings.TrimSpace(binding.ClaimCheckoutID)
	if projectID == "" || taskID == "" || checkoutID == "" {
		return ProjectCheckoutRecord{}, false, nil
	}
	coordination, err := client.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		return ProjectCheckoutRecord{}, true, fmt.Errorf("could not resolve active claim checkout %s for project %s: %v", checkoutID, projectID, err)
	}
	for _, checkout := range coordination.Checkouts {
		if strings.TrimSpace(checkout.CheckoutID) != checkoutID {
			continue
		}
		if strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) {
			return ProjectCheckoutRecord{}, true, fmt.Errorf("active claim checkout %s belongs to agent %s, not %s", checkoutID, strings.TrimSpace(checkout.AgentID), strings.TrimSpace(agentID))
		}
		if strings.TrimSpace(checkout.ActiveTaskID) != taskID {
			return ProjectCheckoutRecord{}, true, fmt.Errorf("active claim checkout %s is active for task %s, not %s", checkoutID, strings.TrimSpace(checkout.ActiveTaskID), taskID)
		}
		if repoID := strings.TrimSpace(binding.ClaimRepoID); repoID != "" && strings.TrimSpace(checkout.RepoID) != repoID {
			return ProjectCheckoutRecord{}, true, fmt.Errorf("active claim checkout %s belongs to repo %s, not %s", checkoutID, strings.TrimSpace(checkout.RepoID), repoID)
		}
		if !projectCheckoutWriteStatusActive(checkout.Status) {
			return ProjectCheckoutRecord{}, true, fmt.Errorf("active claim checkout %s status is %s", checkoutID, strings.TrimSpace(checkout.Status))
		}
		if strings.TrimSpace(checkout.LocalPath) == "" {
			return ProjectCheckoutRecord{}, true, fmt.Errorf("active claim checkout %s local_path is empty", checkoutID)
		}
		return checkout, true, nil
	}
	return ProjectCheckoutRecord{}, true, fmt.Errorf("could not find active claim checkout %s for task %s in current project coordination", checkoutID, taskID)
}

func trustedShellWorkdir(workdir, requested string) (string, error) {
	var candidate string
	var err error
	if filepath.IsAbs(requested) {
		candidate = filepath.Clean(requested)
	} else {
		base := strings.TrimSpace(workdir)
		if base == "" {
			base = "."
		}
		candidate = filepath.Join(base, requested)
		candidate, err = filepath.Abs(candidate)
		if err != nil {
			return "", fmt.Errorf("resolve shell workdir: %w", err)
		}
	}

	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve shell workdir: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("stat shell workdir: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("shell workdir is not a directory: %s", requested)
	}
	return resolved, nil
}

func stringMapArg(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return value
}

func shellCommandForGOOS(goos, command string) (string, []string) {
	switch goos {
	case "windows":
		if shouldRunWindowsCommandWithPowerShell(command) {
			return "powershell", []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", windowsPowerShellCommandWithUTF8(command)}
		}
		return "cmd", []string{"/C", command}
	default:
		return "sh", []string{"-c", command}
	}
}

func windowsPowerShellUTF8Prologue() string {
	return "$__rhizomeUtf8 = New-Object System.Text.UTF8Encoding $false; [Console]::InputEncoding = $__rhizomeUtf8; [Console]::OutputEncoding = $__rhizomeUtf8; $OutputEncoding = $__rhizomeUtf8; "
}

func windowsPowerShellCommandWithUTF8(command string) string {
	command = strings.TrimSpace(command)
	if isExplicitWindowsPowerShellCommand(command) {
		command = injectPowerShellUTF8PrologueIntoExplicitCommand(command)
	} else {
		command = normalizeWindowsPowerShellCommand(command)
	}
	return windowsPowerShellUTF8Prologue() + command
}

func normalizeWindowsPowerShellCommand(command string) string {
	command = strings.TrimSpace(command)
	command = normalizeWindowsPowerShellCompatibility(command)
	lowerCommand := strings.ToLower(command)
	if !strings.Contains(lowerCommand, "start-process") {
		return command
	}
	var b strings.Builder
	lastWritten := 0
	searchFrom := 0
	for {
		idx := strings.Index(lowerCommand[searchFrom:], "start-process")
		if idx < 0 {
			break
		}
		start := searchFrom + idx
		afterCommand := start + len("start-process")
		if afterCommand < len(command) && !isASCIISpace(command[afterCommand]) {
			searchFrom = afterCommand
			continue
		}
		pos := skipASCIISpaces(command, afterCommand)
		if hasPowerShellWordAt(lowerCommand, pos, "-filepath") {
			pos = skipASCIISpaces(command, pos+len("-filepath"))
		}
		_, tokenEnd, innerStart, innerEnd, ok := readPowerShellSimpleToken(command, pos)
		if !ok {
			searchFrom = afterCommand
			continue
		}
		normalizedInner, changed := normalizePowerShellPackageManagerToken(command[innerStart:innerEnd])
		if changed {
			b.WriteString(command[lastWritten:innerStart])
			b.WriteString(normalizedInner)
			lastWritten = innerEnd
		}
		searchFrom = tokenEnd
	}
	if lastWritten == 0 {
		return command
	}
	b.WriteString(command[lastWritten:])
	return b.String()
}

func normalizeWindowsPowerShellCompatibility(command string) string {
	if strings.Contains(command, "&&") {
		command = replacePowerShellAndAndOutsideQuotedText(command)
	}
	command = strings.ReplaceAll(command, "-Encoding utf8NoBOM", "-Encoding utf8")
	command = strings.ReplaceAll(command, "-Encoding UTF8NoBOM", "-Encoding utf8")
	return command
}

func replacePowerShellAndAndOutsideQuotedText(command string) string {
	if !strings.Contains(command, "&&") {
		return command
	}
	var b strings.Builder
	changed := false
	for i := 0; i < len(command); {
		if i+1 < len(command) && command[i] == '@' && (command[i+1] == '\'' || command[i+1] == '"') {
			end := consumePowerShellHereString(command, i, command[i+1])
			b.WriteString(command[i:end])
			i = end
			continue
		}
		switch command[i] {
		case '\'':
			end := consumePowerShellSingleQuotedString(command, i)
			b.WriteString(command[i:end])
			i = end
			continue
		case '"':
			end := consumePowerShellDoubleQuotedString(command, i)
			b.WriteString(command[i:end])
			i = end
			continue
		case '`':
			if i+1 < len(command) {
				b.WriteString(command[i : i+2])
				i += 2
				continue
			}
		case '&':
			if i+1 < len(command) && command[i+1] == '&' {
				b.WriteString("; if (-not $?) { exit 1 };")
				i += 2
				changed = true
				continue
			}
		}
		b.WriteByte(command[i])
		i++
	}
	if !changed {
		return command
	}
	return b.String()
}

func consumePowerShellSingleQuotedString(s string, start int) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] != '\'' {
			continue
		}
		if i+1 < len(s) && s[i+1] == '\'' {
			i++
			continue
		}
		return i + 1
	}
	return len(s)
}

func consumePowerShellDoubleQuotedString(s string, start int) int {
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '`':
			if i+1 < len(s) {
				i++
			}
		case '"':
			return i + 1
		}
	}
	return len(s)
}

func consumePowerShellHereString(s string, start int, quote byte) int {
	endMarker := string([]byte{quote, '@'})
	searchFrom := start + 2
	for {
		idx := strings.Index(s[searchFrom:], endMarker)
		if idx < 0 {
			return len(s)
		}
		end := searchFrom + idx
		if end == 0 || s[end-1] == '\n' || s[end-1] == '\r' {
			return end + len(endMarker)
		}
		searchFrom = end + len(endMarker)
	}
}

func normalizePowerShellPackageManagerToken(token string) (string, bool) {
	lower := strings.ToLower(token)
	if strings.HasSuffix(lower, ".cmd") || strings.Contains(lower, "\\") || strings.Contains(lower, "/") {
		return token, false
	}
	base := token
	lowerBase := lower
	if strings.HasSuffix(lower, ".ps1") {
		base = token[:len(token)-len(".ps1")]
		lowerBase = lower[:len(lower)-len(".ps1")]
	}
	switch lowerBase {
	case "npm", "npx", "pnpm", "yarn":
		return base + ".cmd", true
	default:
		return token, false
	}
}

func hasPowerShellWordAt(lowerCommand string, pos int, word string) bool {
	if pos < 0 || pos+len(word) > len(lowerCommand) || lowerCommand[pos:pos+len(word)] != word {
		return false
	}
	end := pos + len(word)
	return end == len(lowerCommand) || isASCIISpace(lowerCommand[end]) || lowerCommand[end] == ';'
}

func skipASCIISpaces(s string, pos int) int {
	for pos < len(s) && isASCIISpace(s[pos]) {
		pos++
	}
	return pos
}

func isASCIISpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
}

func readPowerShellSimpleToken(s string, pos int) (tokenStart, tokenEnd, innerStart, innerEnd int, ok bool) {
	if pos >= len(s) {
		return 0, 0, 0, 0, false
	}
	if s[pos] == '\'' || s[pos] == '"' {
		quote := s[pos]
		end := pos + 1
		for end < len(s) && s[end] != quote {
			end++
		}
		if end >= len(s) {
			return 0, 0, 0, 0, false
		}
		return pos, end + 1, pos + 1, end, true
	}
	end := pos
	for end < len(s) && !isASCIISpace(s[end]) && s[end] != ';' {
		end++
	}
	if end == pos {
		return 0, 0, 0, 0, false
	}
	return pos, end, pos, end, true
}

func isExplicitWindowsPowerShellCommand(command string) bool {
	lower := strings.ToLower(strings.TrimSpace(command))
	for _, prefix := range []string{"powershell ", "powershell.exe ", "pwsh ", "pwsh.exe "} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func injectPowerShellUTF8PrologueIntoExplicitCommand(command string) string {
	if strings.Contains(command, "$__rhizomeUtf8") {
		return command
	}
	prologue := windowsPowerShellUTF8Prologue()
	if loc := powerShellCommandFlagRe.FindStringIndex(command); loc != nil {
		prefix := strings.TrimRight(command[:loc[0]], " \t\r\n")
		script := stripPowerShellCommandQuotes(strings.TrimLeft(command[loc[1]:], " \t\r\n"))
		script = normalizeWindowsPowerShellCommand(script)
		return strings.TrimSpace(prefix) + " -EncodedCommand " + encodePowerShellEncodedCommand(prologue+script)
	}
	if loc := powerShellEncodedCommandFlagRe.FindStringSubmatchIndex(command); loc != nil && len(loc) >= 8 {
		rawStart, rawEnd := loc[6], loc[7]
		if decoded, ok := decodePowerShellEncodedCommand(command[rawStart:rawEnd]); ok {
			decoded = normalizeWindowsPowerShellCommand(decoded)
			return command[:rawStart] + encodePowerShellEncodedCommand(prologue+decoded) + command[rawEnd:]
		}
	}
	return command
}

func stripPowerShellCommandQuotes(script string) string {
	script = strings.TrimSpace(script)
	if len(script) < 2 {
		return script
	}
	first := script[0]
	last := script[len(script)-1]
	if first == last && (first == '"' || first == '\'') {
		return script[1 : len(script)-1]
	}
	return script
}

func decodePowerShellEncodedCommand(raw string) (string, bool) {
	data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil || len(data) == 0 || len(data)%2 != 0 {
		return "", false
	}
	words := make([]uint16, len(data)/2)
	for i := range words {
		words[i] = binary.LittleEndian.Uint16(data[i*2:])
	}
	return string(utf16.Decode(words)), true
}

func encodePowerShellEncodedCommand(command string) string {
	words := utf16.Encode([]rune(command))
	data := make([]byte, len(words)*2)
	for i, word := range words {
		binary.LittleEndian.PutUint16(data[i*2:], word)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func shouldRunWindowsCommandWithPowerShell(command string) bool {
	trimmed := strings.TrimSpace(command)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)
	for _, prefix := range []string{"cmd ", "cmd.exe "} {
		if strings.HasPrefix(lower, prefix) {
			return false
		}
	}
	for _, prefix := range []string{"powershell ", "powershell.exe ", "pwsh ", "pwsh.exe "} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	if containsPowerShellStatementSeparatorOutsideQuotedText(trimmed) {
		return true
	}
	markers := []string{
		"$erroractionpreference",
		"$env:",
		" set-location ",
		" get-childitem ",
		" gci ",
		" get-content ",
		" test-path ",
		" write-output ",
		" start-process ",
		" new-item ",
		" remove-item ",
		" copy-item ",
		" move-item ",
		" select-string ",
		" convertto-json ",
		" out-file ",
		" join-path ",
		" -erroraction ",
		"@(",
	}
	padded := " " + lower + " "
	for _, marker := range markers {
		if strings.Contains(padded, marker) || strings.HasPrefix(lower, strings.TrimSpace(marker)+" ") {
			return true
		}
	}
	return false
}

func containsPowerShellStatementSeparatorOutsideQuotedText(command string) bool {
	for i := 0; i < len(command); {
		if i+1 < len(command) && command[i] == '@' && (command[i+1] == '\'' || command[i+1] == '"') {
			i = consumePowerShellHereString(command, i, command[i+1])
			continue
		}
		switch command[i] {
		case '\'':
			i = consumePowerShellSingleQuotedString(command, i)
			continue
		case '"':
			i = consumePowerShellDoubleQuotedString(command, i)
			continue
		case '`':
			if i+1 < len(command) {
				i += 2
				continue
			}
		case ';':
			return true
		}
		i++
	}
	return false
}
