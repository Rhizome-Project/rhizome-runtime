package daemon_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/daemon"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRunnerRunOnceDeniesRawExecutorRunNodeWhenProgramBGuarded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-b2-3-deny", "node-1")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	writeNodeScript(t, workspaceRoot, "task-daemon-b2-3-deny", "node-1")

	rt := &fakeRuntime{}
	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:      workspaceRoot,
		MaxNodesPerTick:    5,
		NodeTimeoutSec:     30,
		ActorID:            "daemon-test",
		DisableRawExecutor: true,
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
	if rt.calls != 0 {
		t.Fatalf("raw executor guard should deny before runtime RunNode, got %d calls", rt.calls)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-daemon-b2-3-deny")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Status != model.TaskStatusFailed {
		t.Fatalf("expected task failed, got %s", status.Status)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != model.NodeStatusFailed {
		t.Fatalf("expected node failed, got %+v", status.Nodes)
	}
	if status.Nodes[0].LastError == nil || *status.Nodes[0].LastError != "program_b.raw_executor_disabled" {
		t.Fatalf("expected raw executor disabled last_error, got %v", status.Nodes[0].LastError)
	}

	events, err := store.ListAuditEvents(ctx, sqlite.AuditEventFilter{
		EventType:  "node_execution_failed",
		EntityType: "node",
		EntityID:   "task-daemon-b2-3-deny/node-1",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 1 || !strings.Contains(events[0].PayloadJSON, "program_b.raw_executor_disabled") {
		t.Fatalf("expected visible raw executor deny audit event, got %+v", events)
	}

	run := latestExecutionRunWithPrefix(t, ctx, store, "ws-task-daemon-b2-3-deny", "executorrun_")
	if run.Status != "FAILED" || run.Outcome != "FAILED" {
		t.Fatalf("unexpected raw executor ledger state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromExecutionRun(t, run)
	snapshot := nestedLedgerMap(t, ledger, "capability_snapshot")
	if got := stringLedgerField(t, snapshot, "surface_status_at_start"); got != "disabled_guarded" {
		t.Fatalf("surface_status_at_start = %q", got)
	}
	if got := stringLedgerField(t, snapshot, "policy_verdict_at_start"); got != "DENY" {
		t.Fatalf("policy_verdict_at_start = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if rawDisabled, ok := details["raw_executor_disabled"].(bool); !ok || !rawDisabled {
		t.Fatalf("expected raw_executor_disabled=true, got %+v", details["raw_executor_disabled"])
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	assertExecutorLedgerTraceCorrelation(t, run, ledger, details, resultLedger)
	if got := stringLedgerField(t, resultLedger, "error_code"); got != "program_b.raw_executor_disabled" {
		t.Fatalf("error_code = %q", got)
	}
	if got := stringLedgerField(t, resultLedger, "terminal_state"); got != "policy_denied" {
		t.Fatalf("terminal_state = %q", got)
	}
}

func TestRunnerRunOnceRawExecutorGuardDeniesBeforeNodeScriptPreparation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newTestStore(t)
	createSingleNodeTask(t, ctx, store, "task-daemon-b2-5-deny-missing-script", "node-1")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")

	rt := &fakeRuntime{}
	runner, err := daemon.NewRunner(store, rt, daemon.RunnerConfig{
		WorkspaceRoot:      workspaceRoot,
		MaxNodesPerTick:    5,
		NodeTimeoutSec:     30,
		ActorID:            "daemon-test",
		DisableRawExecutor: true,
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
	if rt.calls != 0 {
		t.Fatalf("raw executor guard should deny before runtime RunNode, got %d calls", rt.calls)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-daemon-b2-5-deny-missing-script")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Status != model.TaskStatusFailed {
		t.Fatalf("expected task failed, got %s", status.Status)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != model.NodeStatusFailed {
		t.Fatalf("expected node failed, got %+v", status.Nodes)
	}
	if status.Nodes[0].LastError == nil || *status.Nodes[0].LastError != "program_b.raw_executor_disabled" {
		t.Fatalf("expected raw executor disabled to win before missing script, got %v", status.Nodes[0].LastError)
	}

	events, err := store.ListAuditEvents(ctx, sqlite.AuditEventFilter{
		EventType:  "node_execution_failed",
		EntityType: "node",
		EntityID:   "task-daemon-b2-5-deny-missing-script/node-1",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 1 || !strings.Contains(events[0].PayloadJSON, "program_b.raw_executor_disabled") ||
		strings.Contains(events[0].PayloadJSON, "executor_missing_script") {
		t.Fatalf("expected raw executor deny audit event before script prep, got %+v", events)
	}
}
