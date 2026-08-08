package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/transport/rpc"
)

const (
	nodeScriptName               = "_rhizome_node.py"
	maxTransientNodeRetries      = 1
	nodeRetryBackoffBase         = 200 * time.Millisecond
	projectionSweepMaxWorkspaces = 16
	projectionSweepBatchSize     = 64
	projectionProcessingStaleAge = 2 * time.Minute
	rawExecutorDisabledReason    = "program_b.raw_executor_disabled"
)

var errMissingNodeScript = errors.New("node script is missing")

type RuntimeInvoker interface {
	RunNode(ctx context.Context, req rpc.NodeRunRequest) (rpc.ExecutorRunNodeResponse, error)
}

type RunnerConfig struct {
	WorkspaceRoot      string
	MaxNodesPerTick    int
	NodeTimeoutSec     int
	ActorID            string
	DisableRawExecutor bool
}

type Runner struct {
	store   *sqlite.Store
	runtime RuntimeInvoker
	cfg     RunnerConfig

	startupRecoveryMu       sync.Mutex
	startupRecoveryComplete bool
}

type nodeRunFailure struct {
	ErrorCode     string
	ErrorMessage  string
	TerminalState string
	Retryable     bool
}

func NewRunner(store *sqlite.Store, runtime RuntimeInvoker, cfg RunnerConfig) (*Runner, error) {
	if store == nil {
		return nil, errors.New("store is required")
	}
	if runtime == nil {
		return nil, errors.New("runtime invoker is required")
	}

	workspaceRoot := strings.TrimSpace(cfg.WorkspaceRoot)
	if workspaceRoot == "" {
		workspaceRoot = "data/workspace"
	}
	if cfg.MaxNodesPerTick <= 0 {
		cfg.MaxNodesPerTick = 10
	}
	if cfg.NodeTimeoutSec <= 0 {
		cfg.NodeTimeoutSec = 120
	}
	if strings.TrimSpace(cfg.ActorID) == "" {
		cfg.ActorID = "daemon"
	}
	cfg.WorkspaceRoot = workspaceRoot

	return &Runner{
		store:   store,
		runtime: runtime,
		cfg:     cfg,
	}, nil
}

func (r *Runner) RunLoop(ctx context.Context, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if _, err := r.RunOnce(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Runner) RunOnce(ctx context.Context) (int, error) {
	if err := r.ensureStartupRecovery(ctx); err != nil {
		return 0, err
	}
	r.bestEffortReconcileProjectionBacklog(ctx)

	nodes, err := r.store.ClaimExecutableNodes(ctx, r.cfg.MaxNodesPerTick, r.cfg.ActorID)
	if err != nil {
		return 0, err
	}
	if len(nodes) == 0 {
		return 0, nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(nodes))
	for _, node := range nodes {
		wg.Add(1)
		go func(n sqlite.ExecutableNode) {
			defer wg.Done()
			if err := r.runNode(ctx, n); err != nil {
				fmt.Fprintf(os.Stderr, "daemon: node execution failed %s/%s: %v\n", n.TaskID, n.NodeID, err)
				errCh <- err
			}
		}(node)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return len(nodes), err
		}
	}

	return len(nodes), nil
}

func (r *Runner) bestEffortReconcileProjectionBacklog(ctx context.Context) {
	reclaimed, err := r.store.ReclaimStaleMemoryProjectionProcessing(ctx, projectionProcessingStaleAge, projectionSweepBatchSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "daemon: reclaim stale memory projection processing failed: %v\n", err)
	} else if reclaimed > 0 {
		fmt.Fprintf(os.Stderr, "daemon: reclaimed %d stale memory projection processing row(s)\n", reclaimed)
	}
	workspaces, err := r.store.ListMemoryProjectionPendingWorkspaces(ctx, projectionSweepMaxWorkspaces)
	if err != nil {
		fmt.Fprintf(os.Stderr, "daemon: list memory projection backlog failed: %v\n", err)
		return
	}
	for _, workspaceID := range workspaces {
		result, err := r.store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, projectionSweepBatchSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "daemon: reconcile memory projection workspace %s failed after processed=%d failed=%d: %v\n", workspaceID, result.Processed, result.Failed, err)
			continue
		}
		if result.Processed > 0 {
			fmt.Fprintf(os.Stderr, "daemon: reconciled %d memory projection row(s) for %s\n", result.Processed, workspaceID)
		}
	}
}

func (r *Runner) ensureStartupRecovery(ctx context.Context) error {
	r.startupRecoveryMu.Lock()
	defer r.startupRecoveryMu.Unlock()

	if r.startupRecoveryComplete {
		return nil
	}

	reclaimed, err := r.store.ReclaimExecutableNodesAfterRestart(ctx, r.cfg.ActorID)
	if err != nil {
		return fmt.Errorf("reclaim executable nodes after restart: %w", err)
	}
	if len(reclaimed) > 0 {
		fmt.Fprintf(os.Stderr, "daemon: reclaimed %d running node(s) after restart\n", len(reclaimed))
	}
	r.startupRecoveryComplete = true
	return nil
}

func (r *Runner) runNode(ctx context.Context, node sqlite.ExecutableNode) error {
	actor := strings.TrimSpace(r.cfg.ActorID)
	traceID := fmt.Sprintf("tr-%d", time.Now().UTC().UnixNano())
	runtimeProfile := runtimeProfileForNodeType(node.NodeType)
	operation := r.newNodeExecutionOperationLedger(node, actor, traceID, runtimeProfile)
	if _, err := r.store.UpdateTaskStatusFromNodesWithWorkspaceAuthority(ctx, node.WorkspaceID, node.TaskID, actor, "node_execution_claimed"); err != nil {
		return fmt.Errorf("set task running for node %s/%s: %w", node.TaskID, node.NodeID, err)
	}
	if err := r.recordNodeExecutionOperationLedger(ctx, operation, "ACTIVE", "RUNNING", false, nil); err != nil {
		return fmt.Errorf("record executor active operation ledger %s/%s: %w", node.TaskID, node.NodeID, err)
	}
	ledgerCtx := context.WithoutCancel(ctx)
	if r.cfg.DisableRawExecutor {
		failure := nodeRunFailure{
			ErrorCode:     rawExecutorDisabledReason,
			ErrorMessage:  "raw executor.run_node is disabled in guarded Program B mode; use repo authority patch flow",
			TerminalState: "policy_denied",
		}
		if err := r.finishFailed(ctx, node, actor, failure, traceID); err != nil {
			return err
		}
		status, outcome := nodeExecutionTerminalStateForFailure(failure)
		_ = r.recordNodeExecutionOperationLedger(ledgerCtx, operation, status, outcome, true, nodeExecutionFailureResult(traceID, failure))
		return nil
	}

	scriptRef, err := r.ensureNodeScript(node)
	if err != nil {
		failure := classifyNodePreparationFailure(err)
		if err := r.finishFailed(ctx, node, actor, failure, traceID); err != nil {
			return err
		}
		status, outcome := nodeExecutionTerminalStateForFailure(failure)
		_ = r.recordNodeExecutionOperationLedger(ledgerCtx, operation, status, outcome, true, nodeExecutionFailureResult(traceID, failure))
		return nil
	}
	operation.requestDetails["script_ref"] = scriptRef

	if err := r.addAuditEvent(ctx, "node_execution_started", node.TaskID, node.NodeID, actor, map[string]any{
		"task_id":         node.TaskID,
		"node_id":         node.NodeID,
		"runtime_profile": runtimeProfile,
		"script_ref":      scriptRef,
		"trace_id":        traceID,
	}); err != nil {
		return err
	}

	req := rpc.NodeRunRequest{
		TaskID:         node.TaskID,
		NodeID:         node.NodeID,
		RuntimeProfile: runtimeProfile,
		ScriptRef:      scriptRef,
		TimeoutSec:     r.cfg.NodeTimeoutSec,
		Env:            map[string]string{},
		CPUs:           "1.0",
		Memory:         "512m",
		TraceID:        traceID,
	}

	resp, failure, runErr := r.runNodeWithRetry(ctx, node, req, traceID)
	if runErr != nil {
		return runErr
	}
	if failure != nil {
		if err := r.finishFailed(ctx, node, actor, *failure, traceID); err != nil {
			return err
		}
		status, outcome := nodeExecutionTerminalStateForFailure(*failure)
		_ = r.recordNodeExecutionOperationLedger(ledgerCtx, operation, status, outcome, true, nodeExecutionFailureResult(traceID, *failure))
		return nil
	}

	if resp.Error != nil {
		failure := classifyNodeResponseFailure(*resp.Error)
		if err := r.finishFailed(ctx, node, actor, failure, traceID); err != nil {
			return err
		}
		status, outcome := nodeExecutionTerminalStateForFailure(failure)
		_ = r.recordNodeExecutionOperationLedger(ledgerCtx, operation, status, outcome, true, nodeExecutionFailureResult(traceID, failure))
		return nil
	}

	if _, err := r.store.SetNodeStatusAndUpdateTaskStatusWithWorkspaceAuthority(ctx, node.WorkspaceID, sqlite.NodeStatusUpdateInput{
		TaskID:    node.TaskID,
		NodeID:    node.NodeID,
		NewStatus: model.NodeStatusResolved,
		Reason:    "executor_success",
		ActorID:   actor,
	}, "node_execution_success"); err != nil {
		return fmt.Errorf("set node resolved %s/%s: %w", node.TaskID, node.NodeID, err)
	}

	var durationSec float64
	var artifacts int
	var exitCode int
	if resp.Result != nil {
		durationSec = resp.Result.DurationSec
		artifacts = len(resp.Result.Artifacts)
		exitCode = resp.Result.ExitCode
	}

	if err := r.addAuditEvent(ctx, "node_execution_succeeded", node.TaskID, node.NodeID, actor, map[string]any{
		"task_id":      node.TaskID,
		"node_id":      node.NodeID,
		"trace_id":     traceID,
		"duration_sec": durationSec,
		"exit_code":    exitCode,
		"artifacts":    artifacts,
	}); err != nil {
		return err
	}
	_ = r.recordNodeExecutionOperationLedger(ledgerCtx, operation, "COMPLETED", "COMPLETED", true, nodeExecutionSuccessResult(traceID, resp.Result))

	return nil
}

type nodeExecutionOperationLedgerContext struct {
	operationID        string
	operationKey       string
	operationName      string
	createdAt          time.Time
	workspaceID        string
	taskID             string
	binding            map[string]any
	capabilitySnapshot map[string]any
	requestDetails     map[string]any
	fence              map[string]any
}

func (r *Runner) newNodeExecutionOperationLedger(node sqlite.ExecutableNode, actor, traceID, runtimeProfile string) nodeExecutionOperationLedgerContext {
	started := time.Now().UTC()
	requestDetails := map[string]any{
		"task_id":               strings.TrimSpace(node.TaskID),
		"node_id":               strings.TrimSpace(node.NodeID),
		"node_type":             strings.TrimSpace(node.NodeType),
		"runtime_profile":       strings.TrimSpace(runtimeProfile),
		"trace_id":              strings.TrimSpace(traceID),
		"timeout_sec":           r.cfg.NodeTimeoutSec,
		"execution_kind":        "executor_run_node",
		"transport":             "runtime_invoker",
		"raw_executor_disabled": r.cfg.DisableRawExecutor,
		"script_ref":            "",
	}
	requestHashInput := map[string]any{
		"workspace_id":    strings.TrimSpace(node.WorkspaceID),
		"task_id":         strings.TrimSpace(node.TaskID),
		"node_id":         strings.TrimSpace(node.NodeID),
		"runtime_profile": strings.TrimSpace(runtimeProfile),
		"trace_id":        strings.TrimSpace(traceID),
		"timeout_sec":     r.cfg.NodeTimeoutSec,
	}
	operationKey := "sha256:" + stableMapSHA256(requestHashInput)
	operationID := nodeExecutionOperationID(operationKey, traceID)
	return nodeExecutionOperationLedgerContext{
		operationID:   operationID,
		operationKey:  operationKey,
		operationName: "executor.run_node:" + strings.TrimSpace(node.TaskID) + "/" + strings.TrimSpace(node.NodeID),
		createdAt:     started,
		workspaceID:   strings.TrimSpace(node.WorkspaceID),
		taskID:        strings.TrimSpace(node.TaskID),
		binding: map[string]any{
			"workspace_id":            strings.TrimSpace(node.WorkspaceID),
			"principal_type":          "daemon",
			"principal_id":            strings.TrimSpace(actor),
			"agent_id":                "",
			"owner_user_id":           "",
			"task_id":                 strings.TrimSpace(node.TaskID),
			"session_id":              "",
			"run_id":                  operationID,
			"claim_agent_id":          "",
			"claim_status_at_start":   "",
			"session_status_at_start": "",
			"authority_scope":         "workspace",
			"authority_term":          "",
			"claim_term":              "",
			"session_term":            "",
		},
		capabilitySnapshot: map[string]any{
			"snapshot_id":                    "executor-run-node-ledger-snapshot",
			"snapshot_schema":                "operation_ledger.v1",
			"surface_id":                     "executor.run_node",
			"surface_status_at_start":        executorSurfaceStatus(r.cfg.DisableRawExecutor),
			"requested_capability":           "executor.run_node",
			"policy_epoch":                   "",
			"policy_verdict_at_start":        firstString(map[bool]string{true: "DENY", false: "ALLOW"}[r.cfg.DisableRawExecutor], "ALLOW"),
			"disabled_reason_codes_at_start": disabledExecutorReasons(r.cfg.DisableRawExecutor),
		},
		requestDetails: requestDetails,
		fence: map[string]any{
			"expected_status_before_terminal": "running",
			"expected_run_id":                 operationID,
			"completion_token":                operationID,
			"canonical_mutation_allowed":      false,
			"canonical_mutation_reason":       "executor.run_node records runtime execution outcome; repository mutations require repo-authority patch flow",
		},
	}
}

func nodeExecutionOperationID(operationKey, traceID string) string {
	key := strings.TrimPrefix(strings.TrimSpace(operationKey), "sha256:")
	if len(key) > 16 {
		key = key[:16]
	}
	if key == "" {
		key = "unhashed"
	}
	traceSuffix := strings.TrimPrefix(strings.TrimSpace(traceID), "tr-")
	if traceSuffix == "" {
		traceSuffix = "notrace"
	}
	return "executorrun_" + key + "_" + traceSuffix
}

func executorSurfaceStatus(disabled bool) string {
	if disabled {
		return "disabled_guarded"
	}
	return "enabled"
}

func disabledExecutorReasons(disabled bool) []string {
	if disabled {
		return []string{rawExecutorDisabledReason}
	}
	return []string{}
}

func stableMapSHA256(value map[string]any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func nodeExecutionSuccessResult(traceID string, result *rpc.ExecutorRunNodeResult) map[string]any {
	out := map[string]any{
		"trace_id": strings.TrimSpace(traceID),
		"status":   "SUCCESS",
	}
	if result == nil {
		return out
	}
	out["exit_code"] = result.ExitCode
	out["duration_sec"] = result.DurationSec
	out["artifact_count"] = len(result.Artifacts)
	out["executor_trace_id"] = strings.TrimSpace(result.TraceID)
	return out
}

func nodeExecutionFailureResult(traceID string, failure nodeRunFailure) map[string]any {
	status := "FAILED"
	if strings.EqualFold(strings.TrimSpace(failure.ErrorCode), "executor_timeout") ||
		strings.EqualFold(strings.TrimSpace(failure.TerminalState), "timeout") {
		status = "TIMED_OUT"
	}
	return map[string]any{
		"trace_id":       strings.TrimSpace(traceID),
		"status":         status,
		"error_code":     strings.TrimSpace(failure.ErrorCode),
		"error_message":  strings.TrimSpace(failure.ErrorMessage),
		"terminal_state": strings.TrimSpace(failure.TerminalState),
		"retryable":      failure.Retryable,
		"is_error":       true,
	}
}

func nodeExecutionTerminalStateForFailure(failure nodeRunFailure) (string, string) {
	if strings.EqualFold(strings.TrimSpace(failure.ErrorCode), "executor_timeout") ||
		strings.EqualFold(strings.TrimSpace(failure.TerminalState), "timeout") {
		return "TIMED_OUT", "TIMED_OUT"
	}
	return "FAILED", "FAILED"
}

func (r *Runner) recordNodeExecutionOperationLedger(ctx context.Context, op nodeExecutionOperationLedgerContext, runStatus, outcome string, terminal bool, result map[string]any) error {
	now := time.Now().UTC()
	payload := map[string]any{
		"schema":              "operation_ledger.v1",
		"operation_id":        op.operationID,
		"operation_key":       op.operationKey,
		"operation_kind":      "executor_run_node",
		"operation_name":      op.operationName,
		"status":              operationLedgerStatus(runStatus),
		"terminal":            terminal,
		"created_at":          op.createdAt.Format(time.RFC3339Nano),
		"started_at":          op.createdAt.Format(time.RFC3339Nano),
		"updated_at":          now.Format(time.RFC3339Nano),
		"timeout_sec":         r.cfg.NodeTimeoutSec,
		"attempt":             1,
		"binding":             op.binding,
		"capability_snapshot": op.capabilitySnapshot,
		"request": map[string]any{
			"request_hash":      op.operationKey,
			"idempotency_scope": "workspace",
			"redaction_policy":  "secrets-redacted-v1",
			"summary":           op.operationName,
			"details":           op.requestDetails,
		},
		"fence": op.fence,
		"causality": map[string]any{
			"source":      "daemon",
			"parent_refs": []string{},
		},
	}
	if terminal {
		payload["terminal_at"] = now.Format(time.RFC3339Nano)
	}
	if result != nil {
		payload["result"] = result
	}
	summary := "executor.run_node " + operationLedgerStatus(runStatus) + ": " + op.operationName
	if result != nil {
		if message, _ := result["error_message"].(string); strings.TrimSpace(message) != "" {
			summary = strings.TrimSpace(message)
		}
	}
	if _, _, err := r.store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		RunID:       op.operationID,
		WorkspaceID: op.workspaceID,
		TaskID:      op.taskID,
		Title:       "Executor node: " + op.operationName,
		Summary:     summary,
		Status:      runStatus,
		Outcome:     outcome,
		Verification: map[string]any{
			"operation_ledger": payload,
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: executor operation ledger degraded workspace=%s task=%s operation=%s: %v\n", op.workspaceID, op.taskID, op.operationID, err)
		return err
	}
	return nil
}

func operationLedgerStatus(runStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(runStatus)) {
	case "ACTIVE":
		return "running"
	case "COMPLETED":
		return "completed"
	case "FAILED":
		return "failed"
	case "TIMED_OUT":
		return "timed_out"
	case "CANCELLED":
		return "cancelled"
	default:
		return strings.ToLower(strings.TrimSpace(runStatus))
	}
}

func firstString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func (r *Runner) finishFailed(ctx context.Context, node sqlite.ExecutableNode, actor string, failure nodeRunFailure, traceID string) error {
	if _, err := r.store.SetNodeStatusAndUpdateTaskStatusWithWorkspaceAuthority(ctx, node.WorkspaceID, sqlite.NodeStatusUpdateInput{
		TaskID:    node.TaskID,
		NodeID:    node.NodeID,
		NewStatus: model.NodeStatusFailed,
		Reason:    strings.TrimSpace(failure.ErrorCode),
		ActorID:   actor,
	}, failure.ErrorCode); err != nil {
		return fmt.Errorf("set node failed %s/%s: %w", node.TaskID, node.NodeID, err)
	}

	if err := r.addAuditEvent(ctx, "node_execution_failed", node.TaskID, node.NodeID, actor, map[string]any{
		"task_id":        node.TaskID,
		"node_id":        node.NodeID,
		"trace_id":       traceID,
		"error_code":     strings.TrimSpace(failure.ErrorCode),
		"error_message":  strings.TrimSpace(failure.ErrorMessage),
		"terminal_state": strings.TrimSpace(failure.TerminalState),
		"retryable":      failure.Retryable,
	}); err != nil {
		return err
	}

	return nil
}

func (r *Runner) ensureNodeScript(node sqlite.ExecutableNode) (string, error) {
	sharedDir := filepath.Join(r.cfg.WorkspaceRoot, "shared", node.TaskID, node.NodeID)
	hostScriptPath := filepath.Join(sharedDir, nodeScriptName)
	if _, err := os.Stat(hostScriptPath); err == nil {
		return "/workspace/shared/" + nodeScriptName, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return "", errMissingNodeScript
	} else {
		return "", fmt.Errorf("stat node script: %w", err)
	}
}

func (r *Runner) addAuditEvent(ctx context.Context, eventType, taskID, nodeID, actor string, payload map[string]any) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal audit payload %s: %w", eventType, err)
	}
	if err := r.store.AddAuditEvent(ctx, sqlite.AuditEventInput{
		EventType:   eventType,
		EntityType:  "node",
		EntityID:    fmt.Sprintf("%s/%s", taskID, nodeID),
		ActorID:     strings.TrimSpace(actor),
		PayloadJSON: string(payloadJSON),
	}); err != nil {
		return fmt.Errorf("add audit event %s: %w", eventType, err)
	}
	return nil
}

func runtimeProfileForNodeType(nodeType string) string {
	v := strings.ToLower(strings.TrimSpace(nodeType))
	if strings.Contains(v, "browser") || strings.Contains(v, "playwright") {
		return "browser_automation"
	}
	return "compute"
}

func (r *Runner) runNodeWithRetry(ctx context.Context, node sqlite.ExecutableNode, req rpc.NodeRunRequest, traceID string) (rpc.ExecutorRunNodeResponse, *nodeRunFailure, error) {
	attempts := 1 + maxTransientNodeRetries
	for attempt := 1; attempt <= attempts; attempt++ {
		runCtx, cancel := context.WithTimeout(ctx, time.Duration(r.cfg.NodeTimeoutSec)*time.Second)
		resp, err := r.runtime.RunNode(runCtx, req)
		cancel()
		if err == nil {
			return resp, nil, nil
		}

		failure := classifyNodeRunFailure(err)
		if !failure.Retryable || attempt == attempts {
			return rpc.ExecutorRunNodeResponse{}, &failure, nil
		}
		if err := r.addAuditEvent(ctx, "node_execution_retrying", node.TaskID, node.NodeID, strings.TrimSpace(r.cfg.ActorID), map[string]any{
			"task_id":        node.TaskID,
			"node_id":        node.NodeID,
			"trace_id":       traceID,
			"attempt":        attempt,
			"max_attempts":   attempts,
			"error_code":     failure.ErrorCode,
			"error_message":  failure.ErrorMessage,
			"terminal_state": failure.TerminalState,
		}); err != nil {
			return rpc.ExecutorRunNodeResponse{}, nil, err
		}
		if err := sleepWithContext(ctx, nodeRetryBackoffBase*time.Duration(attempt)); err != nil {
			return rpc.ExecutorRunNodeResponse{}, nil, err
		}
	}
	return rpc.ExecutorRunNodeResponse{}, &nodeRunFailure{
		ErrorCode:     "executor_runtime_error",
		ErrorMessage:  "node execution failed after bounded retries",
		TerminalState: "runtime_error",
	}, nil
}

func classifyNodePreparationFailure(err error) nodeRunFailure {
	if errors.Is(err, errMissingNodeScript) {
		return nodeRunFailure{
			ErrorCode:     "executor_missing_script",
			ErrorMessage:  "node script is missing",
			TerminalState: "missing_script",
		}
	}
	return nodeRunFailure{
		ErrorCode:     "executor_node_preparation_failed",
		ErrorMessage:  strings.TrimSpace(err.Error()),
		TerminalState: "preparation_failed",
	}
}

func classifyNodeRunFailure(err error) nodeRunFailure {
	message := strings.TrimSpace(err.Error())
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return nodeRunFailure{
			ErrorCode:     "executor_timeout",
			ErrorMessage:  message,
			TerminalState: "timeout",
			Retryable:     true,
		}
	case errors.Is(err, rpc.ErrRuntimeClientCommand):
		return nodeRunFailure{
			ErrorCode:     "executor_runner_crash",
			ErrorMessage:  message,
			TerminalState: "runner_crash",
			Retryable:     true,
		}
	case errors.Is(err, rpc.ErrRuntimeClientOutput):
		errorCode := "executor_invalid_payload"
		terminalState := "invalid_payload"
		if strings.Contains(strings.ToLower(message), "no json-rpc response found") {
			errorCode = "executor_missing_payload"
			terminalState = "missing_payload"
		}
		return nodeRunFailure{
			ErrorCode:     errorCode,
			ErrorMessage:  message,
			TerminalState: terminalState,
		}
	case errors.Is(err, rpc.ErrInvalidRuntimeEnvelope),
		errors.Is(err, rpc.ErrInvalidRuntimeResult),
		errors.Is(err, rpc.ErrInvalidRuntimeError),
		errors.Is(err, rpc.ErrInvalidJSON):
		return nodeRunFailure{
			ErrorCode:     "executor_invalid_payload",
			ErrorMessage:  message,
			TerminalState: "invalid_payload",
		}
	default:
		return nodeRunFailure{
			ErrorCode:     "executor_runtime_error",
			ErrorMessage:  message,
			TerminalState: "runtime_error",
		}
	}
}

func classifyNodeResponseFailure(respErr rpc.ExecutorRunNodeError) nodeRunFailure {
	errorCode := rpc.MapExecutorRuntimeErrorCode(respErr.Code, respErr.Message)
	errorMessage := strings.TrimSpace(respErr.Details.ErrorMessage)
	if errorMessage == "" {
		errorMessage = strings.TrimSpace(respErr.Message)
	}
	terminalState := "runtime_error"
	switch errorCode {
	case "executor_timeout":
		terminalState = "timeout"
	case "state_transition_invalid":
		terminalState = "invalid_payload"
	}
	return nodeRunFailure{
		ErrorCode:     errorCode,
		ErrorMessage:  errorMessage,
		TerminalState: terminalState,
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
