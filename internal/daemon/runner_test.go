package daemon_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/daemon"
	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/transport/rpc"
)

type fakeRuntime struct {
	resp  rpc.ExecutorRunNodeResponse
	err   error
	calls int
}

func (f *fakeRuntime) RunNode(_ context.Context, _ rpc.NodeRunRequest) (rpc.ExecutorRunNodeResponse, error) {
	f.calls++
	return f.resp, f.err
}

type echoTraceRuntime struct {
	resp  rpc.ExecutorRunNodeResponse
	err   error
	calls int
}

func (f *echoTraceRuntime) RunNode(_ context.Context, req rpc.NodeRunRequest) (rpc.ExecutorRunNodeResponse, error) {
	f.calls++
	resp := f.resp
	if resp.Result != nil {
		result := *resp.Result
		if result.TraceID == "" {
			result.TraceID = req.TraceID
		}
		resp.Result = &result
	}
	return resp, f.err
}

func TestRunnerRunOnce_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-ok", "node-1")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-ok", "node-1")

	rt := &fakeRuntime{
		resp: rpc.ExecutorRunNodeResponse{
			JSONRPC: "2.0",
			ID:      "req-1",
			Result: &rpc.ExecutorRunNodeResult{
				Status:      "SUCCESS",
				ExitCode:    0,
				DurationSec: 0.5,
			},
		},
	}

	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1, got %d", processed)
	}
	if rt.calls != 1 {
		t.Fatalf("expected runtime calls=1, got %d", rt.calls)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-daemon-ok")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("expected task status %s, got %s", model.TaskStatusResolved, status.Status)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != model.NodeStatusResolved {
		t.Fatalf("expected node status %s, got %+v", model.NodeStatusResolved, status.Nodes)
	}
}

func TestRunnerRunOnce_RuntimeFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-fail", "node-1")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-fail", "node-1")

	rt := &fakeRuntime{
		resp: rpc.ExecutorRunNodeResponse{
			JSONRPC: "2.0",
			ID:      "req-2",
			Error: &rpc.ExecutorRunNodeError{
				Code:    -32018,
				Message: "executor_runtime_error",
				Details: rpc.ExecutorRunNodeErrorDetail{
					Status:       "FAILED",
					ExitCode:     1,
					DurationSec:  1.2,
					ErrorMessage: "script failed",
				},
			},
		},
	}

	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1, got %d", processed)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-daemon-fail")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Status != model.TaskStatusFailed {
		t.Fatalf("expected task status %s, got %s", model.TaskStatusFailed, status.Status)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != model.NodeStatusFailed {
		t.Fatalf("expected node status %s, got %+v", model.NodeStatusFailed, status.Nodes)
	}
	if status.Nodes[0].LastError == nil || *status.Nodes[0].LastError != "executor_runtime_error" {
		t.Fatalf("expected last_error executor_runtime_error, got %v", status.Nodes[0].LastError)
	}
}

func TestRunnerRunOnceRecordsExecutorOperationLedgerSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-ledger-ok", "node-1")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-ledger-ok", "node-1")

	rt := &echoTraceRuntime{
		resp: rpc.ExecutorRunNodeResponse{
			JSONRPC: "2.0",
			ID:      "req-ledger-ok",
			Result: &rpc.ExecutorRunNodeResult{
				Status:      "SUCCESS",
				ExitCode:    0,
				DurationSec: 0.25,
				Artifacts: []rpc.ExecutorArtifactRef{{
					Path:        "artifact.txt",
					SizeBytes:   12,
					ContentType: "text/plain",
				}},
			},
		},
	}
	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	if processed, err := runner.RunOnce(ctx); err != nil || processed != 1 {
		t.Fatalf("run once processed=%d err=%v", processed, err)
	}

	run := latestExecutionRunWithPrefix(t, ctx, store, "ws-task-daemon-ledger-ok", "executorrun_")
	if run.Status != "COMPLETED" || run.Outcome != "COMPLETED" {
		t.Fatalf("unexpected execution run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromExecutionRun(t, run)
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "executor_run_node" {
		t.Fatalf("operation_kind = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "execution_kind"); got != "executor_run_node" {
		t.Fatalf("execution_kind = %q", got)
	}
	if got := stringLedgerField(t, details, "script_ref"); got == "" {
		t.Fatalf("script_ref should be recorded in executor ledger details")
	}
	snapshot := nestedLedgerMap(t, ledger, "capability_snapshot")
	if got := stringLedgerField(t, snapshot, "surface_status_at_start"); got != "enabled" {
		t.Fatalf("surface_status_at_start = %q", got)
	}
	if got := stringLedgerField(t, snapshot, "policy_verdict_at_start"); got != "ALLOW" {
		t.Fatalf("policy_verdict_at_start = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	assertExecutorLedgerTraceCorrelation(t, run, ledger, details, resultLedger)
	if got := stringLedgerField(t, resultLedger, "executor_trace_id"); got != stringLedgerField(t, details, "trace_id") {
		t.Fatalf("executor_trace_id = %q, want request trace_id", got)
	}
	if got := stringLedgerField(t, resultLedger, "status"); got != "SUCCESS" {
		t.Fatalf("result status = %q", got)
	}
	if count, ok := resultLedger["artifact_count"].(float64); !ok || int(count) != 1 {
		t.Fatalf("artifact_count = %T %+v, want 1", resultLedger["artifact_count"], resultLedger["artifact_count"])
	}
}

func TestRunnerRunOnceRecordsExecutorOperationLedgerFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-ledger-fail", "node-1")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-ledger-fail", "node-1")

	rt := &fakeRuntime{
		resp: rpc.ExecutorRunNodeResponse{
			JSONRPC: "2.0",
			ID:      "req-ledger-fail",
			Error: &rpc.ExecutorRunNodeError{
				Code:    -32018,
				Message: "executor_runtime_error",
				Details: rpc.ExecutorRunNodeErrorDetail{
					Status:       "FAILED",
					ExitCode:     1,
					DurationSec:  1.2,
					ErrorMessage: "script failed",
				},
			},
		},
	}
	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	if processed, err := runner.RunOnce(ctx); err != nil || processed != 1 {
		t.Fatalf("run once processed=%d err=%v", processed, err)
	}

	run := latestExecutionRunWithPrefix(t, ctx, store, "ws-task-daemon-ledger-fail", "executorrun_")
	if run.Status != "FAILED" || run.Outcome != "FAILED" {
		t.Fatalf("unexpected execution run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromExecutionRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "failed" {
		t.Fatalf("ledger status = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	assertExecutorLedgerTraceCorrelation(t, run, ledger, details, resultLedger)
	if got := stringLedgerField(t, resultLedger, "error_code"); got != "executor_runtime_error" {
		t.Fatalf("error_code = %q", got)
	}
	if isError, ok := resultLedger["is_error"].(bool); !ok || !isError {
		t.Fatalf("is_error = %+v, want true", resultLedger["is_error"])
	}
}

func TestRunnerRunOnceRecordsExecutorOperationLedgerTimeout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-ledger-timeout", "node-1")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-ledger-timeout", "node-1")

	rt := &fakeRuntime{err: context.DeadlineExceeded}
	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  1,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	if processed, err := runner.RunOnce(ctx); err != nil || processed != 1 {
		t.Fatalf("run once processed=%d err=%v", processed, err)
	}
	if rt.calls != 2 {
		t.Fatalf("expected timeout retry then terminal failure, got %d runtime calls", rt.calls)
	}

	run := latestExecutionRunWithPrefix(t, ctx, store, "ws-task-daemon-ledger-timeout", "executorrun_")
	if run.Status != "TIMED_OUT" || run.Outcome != "TIMED_OUT" {
		t.Fatalf("unexpected timeout execution run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromExecutionRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "timed_out" {
		t.Fatalf("ledger status = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	assertExecutorLedgerTraceCorrelation(t, run, ledger, details, resultLedger)
	if got := stringLedgerField(t, resultLedger, "error_code"); got != "executor_timeout" {
		t.Fatalf("error_code = %q", got)
	}
	if got := stringLedgerField(t, resultLedger, "terminal_state"); got != "timeout" {
		t.Fatalf("terminal_state = %q", got)
	}
}

func TestRunnerRunOnce_ReclaimsRunningNodeAfterRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-reclaim", "node-1")
	claimed, err := store.ClaimExecutableNodes(ctx, 5, "daemon-before-crash")
	if err != nil {
		t.Fatalf("claim executable nodes before restart: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed node before restart, got %+v", claimed)
	}

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-reclaim", "node-1")

	rt := &fakeRuntime{
		resp: rpc.ExecutorRunNodeResponse{
			JSONRPC: "2.0",
			ID:      "req-reclaim",
			Result: &rpc.ExecutorRunNodeResult{
				Status:      "SUCCESS",
				ExitCode:    0,
				DurationSec: 0.25,
			},
		},
	}

	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-after-restart",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once with reclaim: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1 after startup reclaim, got %d", processed)
	}
	if rt.calls != 1 {
		t.Fatalf("expected runtime calls=1 after startup reclaim, got %d", rt.calls)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-daemon-reclaim")
	if err != nil {
		t.Fatalf("get reclaimed task status: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("expected reclaimed task to resolve, got %+v", status)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != model.NodeStatusResolved || status.Nodes[0].AttemptCount != 2 {
		t.Fatalf("expected reclaimed node to resolve on second attempt, got %+v", status.Nodes)
	}
}

func TestRunnerRunOnce_MissingScriptFailsNodeWithoutKillingTick(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-missing", "node-1")
	createSingleNodeTask(t, ctx, store, "task-daemon-present", "node-1")

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-present", "node-1")

	rt := &fakeRuntime{
		resp: rpc.ExecutorRunNodeResponse{
			JSONRPC: "2.0",
			ID:      "req-mixed",
			Result: &rpc.ExecutorRunNodeResult{
				Status:      "SUCCESS",
				ExitCode:    0,
				DurationSec: 0.2,
			},
		},
	}

	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 10,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected processed=2, got %d", processed)
	}
	if rt.calls != 1 {
		t.Fatalf("expected runtime calls=1 for scripted node only, got %d", rt.calls)
	}

	missingStatus, err := store.GetTaskStatus(ctx, "", "task-daemon-missing")
	if err != nil {
		t.Fatalf("get missing task status: %v", err)
	}
	if missingStatus.Status != model.TaskStatusFailed {
		t.Fatalf("expected missing task failed, got %s", missingStatus.Status)
	}
	if missingStatus.Nodes[0].LastError == nil || *missingStatus.Nodes[0].LastError != "executor_missing_script" {
		t.Fatalf("expected missing script reason code, got %v", missingStatus.Nodes[0].LastError)
	}
	run := latestExecutionRunWithPrefix(t, ctx, store, "ws-task-daemon-missing", "executorrun_")
	if run.Status != "FAILED" || run.Outcome != "FAILED" {
		t.Fatalf("unexpected missing-script execution run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromExecutionRun(t, run)
	if got := stringLedgerField(t, ledger, "status"); got != "failed" {
		t.Fatalf("ledger status = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	assertExecutorLedgerTraceCorrelation(t, run, ledger, details, resultLedger)
	if got := stringLedgerField(t, details, "script_ref"); got != "" {
		t.Fatalf("missing-script ledger should keep empty script_ref, got %q", got)
	}
	if got := stringLedgerField(t, resultLedger, "error_code"); got != "executor_missing_script" {
		t.Fatalf("error_code = %q", got)
	}
	if got := stringLedgerField(t, resultLedger, "terminal_state"); got != "missing_script" {
		t.Fatalf("terminal_state = %q", got)
	}

	presentStatus, err := store.GetTaskStatus(ctx, "", "task-daemon-present")
	if err != nil {
		t.Fatalf("get present task status: %v", err)
	}
	if presentStatus.Status != model.TaskStatusResolved {
		t.Fatalf("expected scripted task resolved, got %s", presentStatus.Status)
	}
}

func TestRunnerRunOnce_ReconcilesPendingMemoryProjectionBacklogWithoutNodes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)

	const workspaceID = "ws-daemon-projection-sweep"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Daemon Projection Sweep",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimRunnerWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Projection lag should self-heal",
		Body:        "Daemon ticks should reconcile pending memory projection rows even without executable nodes.",
		Summary:     "daemon projection sweep",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE memory_projection_outbox
		    SET status = ?, attempt_count = 0, last_error = '', available_at = ?, started_at = NULL, completed_at = NULL, updated_at = ?
		  WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`,
		"PENDING",
		now,
		now,
		workspaceID,
		"WORKSPACE_MEMORY",
		record.MemoryID,
	); err != nil {
		t.Fatalf("reset memory projection outbox row to pending: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges WHERE workspace_id = ? AND (from_memory_id = ? OR to_memory_id = ?)`, workspaceID, nodeID, nodeID); err != nil {
		t.Fatalf("delete projection edges: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID); err != nil {
		t.Fatalf("delete projection node: %v", err)
	}
	if workspaces, err := store.ListMemoryProjectionPendingWorkspaces(ctx, 10); err != nil {
		t.Fatalf("list pending memory projection workspaces before daemon sweep: %v", err)
	} else if len(workspaces) != 1 || workspaces[0] != workspaceID {
		t.Fatalf("expected pending memory projection backlog for %s before daemon sweep, got %+v", workspaceID, workspaces)
	}
	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID); err == nil {
		t.Fatalf("expected projection node to be absent before daemon sweep")
	}

	runner, err := daemon.NewRunner(store, &fakeRuntime{}, daemon.RunnerConfig{
		WorkspaceRoot:   filepath.Join(t.TempDir(), "workspace"),
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 0 {
		t.Fatalf("expected no executable nodes, got processed=%d", processed)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get reconciled projection node: %v", err)
	}
	if detail.Node.OriginKind != "workspace_memory" || detail.Node.OriginID != record.MemoryID {
		t.Fatalf("unexpected reconciled projection node %+v", detail.Node)
	}

	workspaces, err := store.ListMemoryProjectionPendingWorkspaces(ctx, 10)
	if err != nil {
		t.Fatalf("list pending memory projection workspaces: %v", err)
	}
	if len(workspaces) != 0 {
		t.Fatalf("expected daemon sweep to clear pending projection backlog, got %+v", workspaces)
	}
}

func TestRunnerRunOnce_ReclaimsStaleProcessingProjectionBacklogWithoutNodes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)

	const workspaceID = "ws-daemon-projection-reclaim"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Daemon Projection Reclaim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimRunnerWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Projection processing reclaim should self-heal",
		Body:        "Daemon ticks should reclaim stale processing projection rows even without executable nodes.",
		Summary:     "daemon projection reclaim",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	staleStartedAt := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE memory_projection_outbox
		    SET status = ?, attempt_count = 1, last_error = '', available_at = ?, started_at = ?, completed_at = NULL, updated_at = ?
		  WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`,
		"PROCESSING",
		staleStartedAt,
		staleStartedAt,
		staleStartedAt,
		workspaceID,
		"WORKSPACE_MEMORY",
		record.MemoryID,
	); err != nil {
		t.Fatalf("reset memory projection outbox row to stale processing: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges WHERE workspace_id = ? AND (from_memory_id = ? OR to_memory_id = ?)`, workspaceID, nodeID, nodeID); err != nil {
		t.Fatalf("delete projection edges: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID); err != nil {
		t.Fatalf("delete projection node: %v", err)
	}
	if workspaces, err := store.ListMemoryProjectionPendingWorkspaces(ctx, 10); err != nil {
		t.Fatalf("list pending memory projection workspaces before daemon reclaim sweep: %v", err)
	} else if len(workspaces) != 0 {
		t.Fatalf("expected stale processing backlog to be invisible before reclaim, got %+v", workspaces)
	}

	runner, err := daemon.NewRunner(store, &fakeRuntime{}, daemon.RunnerConfig{
		WorkspaceRoot:   filepath.Join(t.TempDir(), "workspace"),
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 0 {
		t.Fatalf("expected no executable nodes, got processed=%d", processed)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get reconciled projection node: %v", err)
	}
	if detail.Node.OriginKind != "workspace_memory" || detail.Node.OriginID != record.MemoryID {
		t.Fatalf("unexpected reconciled projection node %+v", detail.Node)
	}

	workspaces, err := store.ListMemoryProjectionPendingWorkspaces(ctx, 10)
	if err != nil {
		t.Fatalf("list pending memory projection workspaces: %v", err)
	}
	if len(workspaces) != 0 {
		t.Fatalf("expected daemon reclaim sweep to clear projection backlog, got %+v", workspaces)
	}
}

func TestRunnerRunOnce_RetriesTransientRunnerCrashOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-retry", "node-1")

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-retry", "node-1")

	call := 0
	runtime := &retryRuntime{calls: &call}

	runner, err := daemon.NewRunner(store, runtime, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 5,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1, got %d", processed)
	}
	if call != 2 {
		t.Fatalf("expected exactly one retry, got %d calls", call)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-daemon-retry")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("expected task resolved after retry, got %s", status.Status)
	}
}

func TestRunnerRunOnce_IsolatesBadNodeWithinClaimedBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-bad", "node-1")
	createSingleNodeTask(t, ctx, store, "task-daemon-retry", "node-1")
	createSingleNodeTask(t, ctx, store, "task-daemon-ok", "node-1")

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-bad", "node-1")
	writeNodeScript(t, workspaceRoot, "task-daemon-retry", "node-1")
	writeNodeScript(t, workspaceRoot, "task-daemon-ok", "node-1")

	rt := &scriptedRuntime{
		plans: map[string][]scriptedRuntimeOutcome{
			"task-daemon-bad/node-1": {{
				resp: rpc.ExecutorRunNodeResponse{
					JSONRPC: "2.0",
					ID:      "req-bad",
					Error: &rpc.ExecutorRunNodeError{
						Code:    -32018,
						Message: "executor_runtime_error",
						Details: rpc.ExecutorRunNodeErrorDetail{
							Status:       "FAILED",
							ExitCode:     1,
							DurationSec:  1.1,
							ErrorMessage: "bad node exploded",
						},
					},
				},
			}},
			"task-daemon-retry/node-1": {
				{err: errors.Join(rpc.ErrRuntimeClientCommand, errors.New("bridge exited"))},
				{resp: rpc.ExecutorRunNodeResponse{
					JSONRPC: "2.0",
					ID:      "req-retry-batch",
					Result: &rpc.ExecutorRunNodeResult{
						Status:      "SUCCESS",
						ExitCode:    0,
						DurationSec: 0.2,
					},
				}},
			},
			"task-daemon-ok/node-1": {{
				resp: rpc.ExecutorRunNodeResponse{
					JSONRPC: "2.0",
					ID:      "req-ok",
					Result: &rpc.ExecutorRunNodeResult{
						Status:      "SUCCESS",
						ExitCode:    0,
						DurationSec: 0.1,
					},
				},
			}},
		},
		calls: map[string]int{},
	}

	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 10,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 3 {
		t.Fatalf("expected processed=3, got %d", processed)
	}
	if got := rt.CallCount("task-daemon-bad/node-1"); got != 1 {
		t.Fatalf("expected bad node to run once, got %d", got)
	}
	if got := rt.CallCount("task-daemon-retry/node-1"); got != 2 {
		t.Fatalf("expected retry node to run twice, got %d", got)
	}
	if got := rt.CallCount("task-daemon-ok/node-1"); got != 1 {
		t.Fatalf("expected healthy sibling to run once, got %d", got)
	}

	badStatus, err := store.GetTaskStatus(ctx, "", "task-daemon-bad")
	if err != nil {
		t.Fatalf("get bad task status: %v", err)
	}
	if badStatus.Status != model.TaskStatusFailed {
		t.Fatalf("expected bad task failed, got %s", badStatus.Status)
	}
	if len(badStatus.Nodes) != 1 || badStatus.Nodes[0].Status != model.NodeStatusFailed {
		t.Fatalf("expected failed bad node, got %+v", badStatus.Nodes)
	}
	if badStatus.Nodes[0].LastError == nil || *badStatus.Nodes[0].LastError != "executor_runtime_error" {
		t.Fatalf("expected bad node reason executor_runtime_error, got %v", badStatus.Nodes[0].LastError)
	}

	retryStatus, err := store.GetTaskStatus(ctx, "", "task-daemon-retry")
	if err != nil {
		t.Fatalf("get retry task status: %v", err)
	}
	if retryStatus.Status != model.TaskStatusResolved {
		t.Fatalf("expected retry task resolved, got %s", retryStatus.Status)
	}
	if len(retryStatus.Nodes) != 1 || retryStatus.Nodes[0].Status != model.NodeStatusResolved {
		t.Fatalf("expected resolved retry node, got %+v", retryStatus.Nodes)
	}

	okStatus, err := store.GetTaskStatus(ctx, "", "task-daemon-ok")
	if err != nil {
		t.Fatalf("get healthy task status: %v", err)
	}
	if okStatus.Status != model.TaskStatusResolved {
		t.Fatalf("expected healthy task resolved, got %s", okStatus.Status)
	}
	if len(okStatus.Nodes) != 1 || okStatus.Nodes[0].Status != model.NodeStatusResolved {
		t.Fatalf("expected resolved healthy node, got %+v", okStatus.Nodes)
	}
}

func TestRunnerRunOnce_IsolatesFailureWithinSingleTaskBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createTaskWithNodes(t, ctx, store, "task-daemon-mixed", "node-bad", "node-ok")

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-mixed", "node-bad")
	writeNodeScript(t, workspaceRoot, "task-daemon-mixed", "node-ok")

	rt := &scriptedRuntime{
		plans: map[string][]scriptedRuntimeOutcome{
			"task-daemon-mixed/node-bad": {{
				resp: rpc.ExecutorRunNodeResponse{
					JSONRPC: "2.0",
					ID:      "req-mixed-bad",
					Error: &rpc.ExecutorRunNodeError{
						Code:    -32018,
						Message: "executor_runtime_error",
						Details: rpc.ExecutorRunNodeErrorDetail{
							Status:       "FAILED",
							ExitCode:     1,
							DurationSec:  0.9,
							ErrorMessage: "mixed batch node failed",
						},
					},
				},
			}},
			"task-daemon-mixed/node-ok": {{
				resp: rpc.ExecutorRunNodeResponse{
					JSONRPC: "2.0",
					ID:      "req-mixed-ok",
					Result: &rpc.ExecutorRunNodeResult{
						Status:      "SUCCESS",
						ExitCode:    0,
						DurationSec: 0.1,
					},
				},
			}},
		},
		calls: map[string]int{},
	}

	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 10,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected processed=2, got %d", processed)
	}
	if got := rt.CallCount("task-daemon-mixed/node-bad"); got != 1 {
		t.Fatalf("expected bad node to run once, got %d", got)
	}
	if got := rt.CallCount("task-daemon-mixed/node-ok"); got != 1 {
		t.Fatalf("expected healthy sibling node to run once, got %d", got)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-daemon-mixed")
	if err != nil {
		t.Fatalf("get mixed task status: %v", err)
	}
	if status.Status != model.TaskStatusFailed {
		t.Fatalf("expected mixed task failed after one node failure, got %s", status.Status)
	}
	if len(status.Nodes) != 2 {
		t.Fatalf("expected 2 nodes in mixed task, got %d", len(status.Nodes))
	}

	var badNodeErr *string
	var badNodeStatus string
	var okNodeStatus string
	for _, node := range status.Nodes {
		switch node.NodeID {
		case "node-bad":
			badNodeStatus = node.Status
			badNodeErr = node.LastError
		case "node-ok":
			okNodeStatus = node.Status
		}
	}
	if badNodeStatus != model.NodeStatusFailed {
		t.Fatalf("expected node-bad failed, got %s", badNodeStatus)
	}
	if badNodeErr == nil || *badNodeErr != "executor_runtime_error" {
		t.Fatalf("expected node-bad reason executor_runtime_error, got %v", badNodeErr)
	}
	if okNodeStatus != model.NodeStatusResolved {
		t.Fatalf("expected node-ok resolved, got %s", okNodeStatus)
	}
}

func TestRunnerRunOnce_SkipsAmbiguousWorkspaceTaskAndProcessesAuthorizedTask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-authorized", "node-1")
	createSingleNodeTask(t, ctx, store, "task-daemon-ambiguous", "node-1")

	secondaryWorkspaceID := "ws-task-daemon-ambiguous-shadow"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: secondaryWorkspaceID,
		Title:       "Ambiguous Runner Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create secondary workspace: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: secondaryWorkspaceID,
		TaskID:      "task-daemon-ambiguous",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach ambiguous task to secondary workspace: %v", err)
	}
	claimRunnerWorkspaceAuthority(t, ctx, store, secondaryWorkspaceID)

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-authorized", "node-1")
	writeNodeScript(t, workspaceRoot, "task-daemon-ambiguous", "node-1")

	rt := &scriptedRuntime{
		plans: map[string][]scriptedRuntimeOutcome{
			"task-daemon-authorized/node-1": {{
				resp: rpc.ExecutorRunNodeResponse{
					JSONRPC: "2.0",
					ID:      "req-authorized",
					Result: &rpc.ExecutorRunNodeResult{
						Status:      "SUCCESS",
						ExitCode:    0,
						DurationSec: 0.1,
					},
				},
			}},
		},
		calls: map[string]int{},
	}

	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:   workspaceRoot,
		MaxNodesPerTick: 10,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	processed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1 with ambiguous task skipped, got %d", processed)
	}
	if got := rt.CallCount("task-daemon-authorized/node-1"); got != 1 {
		t.Fatalf("expected authorized node to run once, got %d", got)
	}
	if got := rt.CallCount("task-daemon-ambiguous/node-1"); got != 0 {
		t.Fatalf("expected ambiguous node to be skipped, got %d calls", got)
	}

	authorizedStatus, err := store.GetTaskStatus(ctx, "", "task-daemon-authorized")
	if err != nil {
		t.Fatalf("get authorized task status: %v", err)
	}
	if authorizedStatus.Status != model.TaskStatusResolved {
		t.Fatalf("expected authorized task resolved, got %s", authorizedStatus.Status)
	}

	ambiguousStatus, err := store.GetTaskStatus(ctx, "", "task-daemon-ambiguous")
	if err != nil {
		t.Fatalf("get ambiguous task status: %v", err)
	}
	if ambiguousStatus.Status != model.TaskStatusPending {
		t.Fatalf("expected ambiguous task to remain pending, got %s", ambiguousStatus.Status)
	}
	if len(ambiguousStatus.Nodes) != 1 || ambiguousStatus.Nodes[0].Status != model.NodeStatusPending {
		t.Fatalf("expected ambiguous node to remain pending, got %+v", ambiguousStatus.Nodes)
	}
}

type retryRuntime struct {
	calls *int
}

func (r *retryRuntime) RunNode(_ context.Context, _ rpc.NodeRunRequest) (rpc.ExecutorRunNodeResponse, error) {
	*r.calls = *r.calls + 1
	if *r.calls == 1 {
		return rpc.ExecutorRunNodeResponse{}, errors.Join(rpc.ErrRuntimeClientCommand, errors.New("bridge exited"))
	}
	return rpc.ExecutorRunNodeResponse{
		JSONRPC: "2.0",
		ID:      "req-retry",
		Result: &rpc.ExecutorRunNodeResult{
			Status:      "SUCCESS",
			ExitCode:    0,
			DurationSec: 0.1,
		},
	}, nil
}

type scriptedRuntimeOutcome struct {
	resp rpc.ExecutorRunNodeResponse
	err  error
}

type scriptedRuntime struct {
	mu    sync.Mutex
	plans map[string][]scriptedRuntimeOutcome
	calls map[string]int
}

func (r *scriptedRuntime) RunNode(_ context.Context, req rpc.NodeRunRequest) (rpc.ExecutorRunNodeResponse, error) {
	key := req.TaskID + "/" + req.NodeID

	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls[key]++
	plan := r.plans[key]
	if len(plan) == 0 {
		return rpc.ExecutorRunNodeResponse{}, errors.New("unexpected node run without scripted outcome")
	}
	outcome := plan[0]
	r.plans[key] = plan[1:]
	return outcome.resp, outcome.err
}

func (r *scriptedRuntime) CallCount(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[key]
}

func newTestStore(t *testing.T) *sqlite.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "rhizome-daemon-test.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.ApplyMigrations(context.Background()); err != nil {
		_ = store.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func createSingleNodeTask(t *testing.T, ctx context.Context, store *sqlite.Store, taskID, nodeID string) {
	t.Helper()
	createTaskWithNodes(t, ctx, store, taskID, nodeID)
}

func createTaskWithNodes(t *testing.T, ctx context.Context, store *sqlite.Store, taskID string, nodeIDs ...string) {
	t.Helper()

	nodes := make([]dag.NodeSpec, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodes = append(nodes, dag.NodeSpec{NodeID: nodeID, Type: "compute"})
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: nodes})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}

	workspaceID := "ws-" + taskID
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runner Test Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	claimRunnerWorkspaceAuthority(t, ctx, store, workspaceID)
}

func claimRunnerWorkspaceAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.WorkspaceAuthorityRecord {
	t.Helper()

	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	now := time.Now().UTC()
	record, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           workspaceID,
		Scope:                 "workspace",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-runner-" + workspaceID,
		Term:                  1,
		LeaseExpiresAt:        now.Add(time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       1,
		AppliedWatermark:      1,
		ActorType:             "system",
		ActorID:               "runner-tests",
		ReferenceAt:           now.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	return record
}

func latestExecutionRunWithPrefix(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, prefix string) sqlite.ExecutionRunRecord {
	t.Helper()
	runs, err := store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("list execution runs: %v", err)
	}
	for _, run := range runs {
		if strings.HasPrefix(run.RunID, prefix) {
			return run
		}
	}
	t.Fatalf("expected execution run with prefix %q, got %+v", prefix, runs)
	return sqlite.ExecutionRunRecord{}
}

func operationLedgerFromExecutionRun(t *testing.T, run sqlite.ExecutionRunRecord) map[string]any {
	t.Helper()
	raw, ok := run.VerificationJSON["operation_ledger"]
	if !ok {
		t.Fatalf("missing operation_ledger in %+v", run.VerificationJSON)
	}
	ledger, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("operation_ledger has type %T, want map[string]any", raw)
	}
	return ledger
}

func nestedLedgerMap(t *testing.T, values map[string]any, key string) map[string]any {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing nested field %q in %+v", key, values)
	}
	nested, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("nested field %q has type %T, want map[string]any", key, raw)
	}
	return nested
}

func stringLedgerField(t *testing.T, values map[string]any, key string) string {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing field %q in %+v", key, values)
	}
	got, ok := raw.(string)
	if !ok {
		t.Fatalf("field %q has type %T, want string", key, raw)
	}
	return got
}

func assertExecutorLedgerTraceCorrelation(t *testing.T, run sqlite.ExecutionRunRecord, ledger, details, result map[string]any) {
	t.Helper()
	operationID := stringLedgerField(t, ledger, "operation_id")
	if operationID != run.RunID {
		t.Fatalf("ledger operation_id = %q, want run id %q", operationID, run.RunID)
	}
	operationKey := stringLedgerField(t, ledger, "operation_key")
	digest := strings.TrimPrefix(operationKey, "sha256:")
	if len(digest) < 16 {
		t.Fatalf("operation_key digest %q should include at least 16 hex chars", operationKey)
	}
	wantPrefix := "executorrun_" + digest[:16] + "_"
	if !strings.HasPrefix(operationID, wantPrefix) {
		t.Fatalf("operation_id %q should include operation key digest prefix %q", operationID, wantPrefix)
	}
	traceID := stringLedgerField(t, details, "trace_id")
	if got := stringLedgerField(t, result, "trace_id"); got != traceID {
		t.Fatalf("result trace_id = %q, want request trace_id %q", got, traceID)
	}
	traceSuffix := strings.TrimPrefix(traceID, "tr-")
	if traceSuffix == "" || !strings.HasSuffix(operationID, "_"+traceSuffix) {
		t.Fatalf("operation_id %q should include trace suffix from %q", operationID, traceID)
	}
	if !strings.HasPrefix(operationID, "executorrun_") {
		t.Fatalf("operation_id %q should have executorrun_ prefix", operationID)
	}
}

func writeNodeScript(t *testing.T, workspaceRoot, taskID, nodeID string) {
	t.Helper()

	dir := filepath.Join(workspaceRoot, "shared", taskID, nodeID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	script := filepath.Join(dir, "_rhizome_node.py")
	if err := os.WriteFile(script, []byte("print('ok')\n"), 0o644); err != nil {
		t.Fatalf("write node script: %v", err)
	}
}
