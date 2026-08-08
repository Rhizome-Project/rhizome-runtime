package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Executor manages deployed tools and executes them in subprocess sandboxes.
type Executor struct {
	toolsDir       string // base directory for deployed tool scripts
	mutationPolicy ExecutorMutationPolicy
}

const DirectExecutorMutationDeniedReason = "program_b.raw_executor_disabled"

type ExecutorMutationPolicy struct {
	RequireAuthority bool
	DisableDirect    bool
	Authority        ExecutorMutationAuthorityContext
	RecordDeny       func(ExecutorMutationDenyRecord)
}

type ExecutorMutationAuthorityContext struct {
	RepoLeaseID      string
	LeaseTerm        int64
	PatchQueueID     string
	PatchQueueItemID string
	OperationID      string
}

type ExecutorMutationDenyRecord struct {
	ToolID         string
	WorkspaceID    string
	Runtime        string
	ReasonCode     string
	MissingContext []string
}

// DefaultCallTimeout is the canonical single-host budget for one tool.call
// request unless the caller asks for a shorter timeout.
const DefaultCallTimeout = 5 * time.Minute

var toolEnvAllowlist = []string{
	"PATH",
	"PATHEXT",
	"SYSTEMROOT",
	"SystemRoot",
	"WINDIR",
	"ComSpec",
	"TMP",
	"TEMP",
	"HOME",
	"USERPROFILE",
	"LOCALAPPDATA",
	"APPDATA",
	"ProgramData",
	"LANG",
	"LC_ALL",
	"LC_CTYPE",
	"TERM",
	"TZ",
}

// NewExecutor creates a new Executor.
func NewExecutor(workspaceRoot string) *Executor {
	return &Executor{
		toolsDir: filepath.Join(workspaceRoot, "tools"),
	}
}

func (e *Executor) SetMutationPolicy(policy ExecutorMutationPolicy) {
	if e == nil {
		return
	}
	e.mutationPolicy = policy
}

// DeployInput describes a tool to deploy.
type DeployInput struct {
	ToolID      string `json:"tool_id"`
	WorkspaceID string `json:"workspace_id"`
	Runtime     string `json:"runtime"` // python, bash, node
	SourceCode  string `json:"source_code"`
	EntryPoint  string `json:"entry_point"` // filename
	DeployedBy  string `json:"deployed_by"`
}

// Deploy saves a tool script to disk.
func (e *Executor) Deploy(input DeployInput) (string, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id is required")
	}
	toolID := strings.TrimSpace(input.ToolID)
	if toolID == "" {
		return "", errors.New("tool_id is required")
	}
	if strings.TrimSpace(input.SourceCode) == "" {
		return "", errors.New("source_code is required")
	}

	runtime := strings.TrimSpace(input.Runtime)
	if runtime == "" {
		runtime = "python"
	}
	runtime = strings.ToLower(runtime)

	entryPoint := strings.TrimSpace(input.EntryPoint)
	if entryPoint == "" {
		switch runtime {
		case "python":
			entryPoint = "main.py"
		case "bash":
			entryPoint = "main.sh"
		case "node":
			entryPoint = "main.js"
		default:
			entryPoint = "main"
		}
	}
	entryPoint, err := normalizeEntryPoint(entryPoint)
	if err != nil {
		return "", err
	}

	dir := filepath.Join(e.toolsDir, sanitize(workspaceID), sanitize(toolID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create tool dir: %w", err)
	}

	scriptPath := filepath.Join(dir, entryPoint)
	var previousScript []byte
	scriptExisted := false
	if raw, err := os.ReadFile(scriptPath); err == nil {
		previousScript = raw
		scriptExisted = true
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read existing tool script for rollback: %w", err)
	}
	if err := os.WriteFile(scriptPath, []byte(input.SourceCode), 0o755); err != nil {
		return "", fmt.Errorf("write tool script: %w", err)
	}

	// Write metadata
	meta := map[string]string{
		"tool_id":      toolID,
		"workspace_id": workspaceID,
		"runtime":      runtime,
		"entry_point":  entryPoint,
		"deployed_by":  input.DeployedBy,
		"deployed_at":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "tool.json"), metaJSON, 0o644); err != nil {
		if scriptExisted {
			_ = os.WriteFile(scriptPath, previousScript, 0o755)
		} else {
			_ = os.Remove(scriptPath)
		}
		return "", fmt.Errorf("write tool metadata: %w", err)
	}

	return scriptPath, nil
}

// CallInput describes a tool call.
type CallInput struct {
	ToolID      string         `json:"tool_id"`
	WorkspaceID string         `json:"workspace_id"`
	Arguments   map[string]any `json:"arguments"`
	TimeoutSec  int            `json:"timeout_sec"`
}

// CallResult is the result of a tool execution.
type CallResult struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
	TimedOut bool   `json:"timed_out"`
}

// EffectiveCallTimeout resolves the effective local tool execution budget from
// the requested timeout and the earlier parent request deadline, if any.
func EffectiveCallTimeout(ctx context.Context, requestedSec int) time.Duration {
	timeout := DefaultCallTimeout
	if requestedSec > 0 {
		timeout = time.Duration(requestedSec) * time.Second
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}
	return timeout
}

// Call executes a deployed tool as a subprocess.
func (e *Executor) Call(ctx context.Context, input CallInput) (*CallResult, error) {
	toolID := strings.TrimSpace(input.ToolID)
	if toolID == "" {
		return nil, errors.New("tool_id is required")
	}

	dir := filepath.Join(e.toolsDir, sanitize(input.WorkspaceID), sanitize(toolID))

	// Read metadata
	metaPath := filepath.Join(dir, "tool.json")
	metaRaw, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("tool not found: %w", err)
	}

	var meta map[string]string
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		return nil, fmt.Errorf("invalid tool metadata: %w", err)
	}

	runtime := meta["runtime"]
	entryPoint, err := normalizeEntryPoint(meta["entry_point"])
	if err != nil {
		return nil, fmt.Errorf("invalid tool entry point: %w", err)
	}
	if denied := e.guardProgramBExecutorCall(input, runtime); denied != nil {
		return denied, nil
	}
	scriptPath := filepath.Join(dir, entryPoint)
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve tool dir: %w", err)
	}
	scriptPath, err = filepath.Abs(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("resolve tool script: %w", err)
	}

	// Determine command
	var cmd *exec.Cmd
	timeout := EffectiveCallTimeout(ctx, input.TimeoutSec)
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	processGuard := newToolProcessGuard()

	switch runtime {
	case "python":
		pythonCmd, err := resolveRuntimeCommand("python")
		if err != nil {
			return nil, err
		}
		cmd = exec.Command(pythonCmd, scriptPath)
	case "bash":
		bashCmd, err := resolveRuntimeCommand("bash")
		if err != nil {
			return nil, err
		}
		cmd = exec.Command(bashCmd, scriptPath)
	case "node":
		nodeCmd, err := resolveRuntimeCommand("node")
		if err != nil {
			return nil, err
		}
		cmd = exec.Command(nodeCmd, scriptPath)
	default:
		cmd = exec.Command(scriptPath)
	}
	processGuard.prepare(cmd)

	cmd.Dir = dir

	// Pass arguments as JSON on stdin
	argsJSON, _ := json.Marshal(input.Arguments)
	cmd.Stdin = bytes.NewReader(argsJSON)

	stdout := &limitWriter{limit: 4 * 1024 * 1024}
	stderr := &limitWriter{limit: 4 * 1024 * 1024}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	// Tools get a minimal runtime env plus explicit identifiers instead of the
	// full host environment, which may contain unrelated secrets.
	cmd.Env = buildToolEnv(toolID, input.WorkspaceID)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start tool: %w", err)
	}
	defer func() { _ = processGuard.close() }()
	if err := processGuard.afterStart(cmd); err != nil {
		_ = processGuard.terminate(cmd)
		_ = cmd.Wait()
		return nil, fmt.Errorf("attach tool process cleanup: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case err = <-waitCh:
	case <-execCtx.Done():
		_ = processGuard.terminate(cmd)
		err = <-waitCh
	}
	result := &CallResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if err != nil {
		if execCtx.Err() == context.DeadlineExceeded {
			result.TimedOut = true
			result.ExitCode = -1
			return result, nil
		}
		if execCtx.Err() == context.Canceled {
			return nil, execCtx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
			return result, nil
		}
		return nil, fmt.Errorf("execute tool: %w", err)
	}

	result.ExitCode = 0
	return result, nil
}

func (e *Executor) guardProgramBExecutorCall(input CallInput, runtime string) *CallResult {
	if e == nil {
		return nil
	}
	policy := e.mutationPolicy
	if !policy.DisableDirect && !policy.RequireAuthority {
		return nil
	}
	missing := policy.MissingContext()
	record := ExecutorMutationDenyRecord{
		ToolID:         strings.TrimSpace(input.ToolID),
		WorkspaceID:    strings.TrimSpace(input.WorkspaceID),
		Runtime:        strings.TrimSpace(runtime),
		ReasonCode:     DirectExecutorMutationDeniedReason,
		MissingContext: missing,
	}
	if policy.RecordDeny != nil {
		policy.RecordDeny(record)
	}
	message := DirectExecutorMutationDeniedReason + ": raw subprocess executor is disabled in guarded Program B mode; use repo authority patch flow"
	return &CallResult{
		Stderr:   message,
		ExitCode: 126,
	}
}

func (p ExecutorMutationPolicy) MissingContext() []string {
	if !p.RequireAuthority {
		return nil
	}
	var missing []string
	if strings.TrimSpace(p.Authority.RepoLeaseID) == "" {
		missing = append(missing, "repo_lease_id")
	}
	if p.Authority.LeaseTerm <= 0 {
		missing = append(missing, "lease_term")
	}
	if strings.TrimSpace(p.Authority.PatchQueueID) == "" {
		missing = append(missing, "patch_queue_id")
	}
	if strings.TrimSpace(p.Authority.PatchQueueItemID) == "" {
		missing = append(missing, "patch_queue_item_id")
	}
	if strings.TrimSpace(p.Authority.OperationID) == "" {
		missing = append(missing, "operation_id")
	}
	return missing
}

func buildToolEnv(toolID, workspaceID string) []string {
	env := []string{
		"TOOL_ID=" + toolID,
		"WORKSPACE_ID=" + workspaceID,
	}
	for _, key := range toolEnvAllowlist {
		value, ok := os.LookupEnv(key)
		if !ok || strings.TrimSpace(value) == "" {
			continue
		}
		env = append(env, key+"="+value)
	}
	return env
}

// Undeploy removes a deployed tool.
func (e *Executor) Undeploy(workspaceID, toolID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	toolID = strings.TrimSpace(toolID)
	if toolID == "" {
		return errors.New("tool_id is required")
	}
	dir := filepath.Join(e.toolsDir, sanitize(workspaceID), sanitize(toolID))
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove tool: %w", err)
	}
	return nil
}

// IsDeployed reports whether a tool currently has deployment metadata on disk.
func (e *Executor) IsDeployed(workspaceID, toolID string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return false, errors.New("workspace_id is required")
	}
	toolID = strings.TrimSpace(toolID)
	if toolID == "" {
		return false, errors.New("tool_id is required")
	}

	metaPath := filepath.Join(e.toolsDir, sanitize(workspaceID), sanitize(toolID), "tool.json")
	info, err := os.Stat(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat tool metadata: %w", err)
	}
	return !info.IsDir(), nil
}

type limitWriter struct {
	buf   bytes.Buffer
	limit int
	wrote int
}

func (w *limitWriter) Write(p []byte) (n int, err error) {
	if w.wrote >= w.limit {
		return len(p), nil
	}
	allow := w.limit - w.wrote
	if len(p) > allow {
		w.buf.Write(p[:allow])
		w.wrote += len(p)
		return len(p), nil
	}
	w.buf.Write(p)
	w.wrote += len(p)
	return len(p), nil
}

func (w *limitWriter) String() string {
	return w.buf.String()
}

// ListDeployed lists deployed tools for a workspace.
func (e *Executor) ListDeployed(workspaceID string) ([]map[string]string, error) {
	dir := filepath.Join(e.toolsDir, sanitize(workspaceID))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list tools dir: %w", err)
	}

	var tools []map[string]string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaPath := filepath.Join(dir, entry.Name(), "tool.json")
		metaRaw, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		var meta map[string]string
		if err := json.Unmarshal(metaRaw, &meta); err != nil {
			continue
		}
		tools = append(tools, meta)
	}
	return tools, nil
}

func sanitize(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "/", "-")
	s = strings.ReplaceAll(s, "\\", "-")
	s = strings.ReplaceAll(s, "..", "")
	if s == "" {
		return "default"
	}
	return s
}

func normalizeEntryPoint(entryPoint string) (string, error) {
	entryPoint = strings.TrimSpace(entryPoint)
	if entryPoint == "" {
		return "", errors.New("entry_point is required")
	}
	if filepath.IsAbs(entryPoint) {
		return "", errors.New("entry_point must be relative")
	}
	clean := filepath.Clean(entryPoint)
	if clean == "." || clean == string(filepath.Separator) {
		return "", errors.New("entry_point must be a file name")
	}
	if clean != filepath.Base(clean) {
		return "", errors.New("entry_point must not contain path separators")
	}
	return clean, nil
}

func resolveRuntimeCommand(runtime string) (string, error) {
	var candidates []string
	switch runtime {
	case "python":
		if env := strings.TrimSpace(os.Getenv("RHIZOME_TOOL_PYTHON")); env != "" {
			candidates = append(candidates, env)
		}
		if env := strings.TrimSpace(os.Getenv("RHIZOME_EXECUTOR_PYTHON")); env != "" {
			candidates = append(candidates, env)
		}
		candidates = append(candidates, "python3", "python")
	case "bash":
		if env := strings.TrimSpace(os.Getenv("RHIZOME_TOOL_BASH")); env != "" {
			candidates = append(candidates, env)
		}
		candidates = append(candidates, "bash")
	case "node":
		if env := strings.TrimSpace(os.Getenv("RHIZOME_TOOL_NODE")); env != "" {
			candidates = append(candidates, env)
		}
		candidates = append(candidates, "node")
	default:
		return "", fmt.Errorf("unsupported runtime: %s", runtime)
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if path, err := exec.LookPath(candidate); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("runtime executable not found for %s", runtime)
}
