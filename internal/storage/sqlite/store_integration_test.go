package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestBeginTxImmediateAcquiresWriteLock(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	store.DB().SetMaxOpenConns(2)
	ctx := context.Background()

	firstTx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin first immediate tx: %v", err)
	}
	defer firstTx.Rollback()

	secondCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	secondTx, err := store.BeginTxImmediate(secondCtx)
	if err == nil {
		_ = secondTx.Rollback()
		t.Fatal("expected second immediate tx to block/fail while first tx holds the write lock")
	}
}

func TestAT001_CreateTaskWithFiveNodeDAG(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: "node-1", Type: "generic"},
			{NodeID: "node-2", Type: "generic", DependsOn: []string{"node-1"}},
			{NodeID: "node-3", Type: "generic", DependsOn: []string{"node-1"}},
			{NodeID: "node-4", Type: "generic", DependsOn: []string{"node-2", "node-3"}},
			{NodeID: "node-5", Type: "generic", DependsOn: []string{"node-4"}},
		},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-at-001",
		OwnerUserID: "developer",
		Priority:    "high",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-at-001")
	if err != nil {
		t.Fatalf("get status: %v", err)
	}

	if status.Status != model.TaskStatusPending {
		t.Fatalf("expected task status %s, got %s", model.TaskStatusPending, status.Status)
	}
	if len(status.Nodes) != 5 {
		t.Fatalf("expected 5 nodes, got %d", len(status.Nodes))
	}
	if got := status.NodeCounts[model.NodeStatusPending]; got != 1 {
		t.Fatalf("expected 1 pending node, got %d", got)
	}
	if got := status.NodeCounts[model.NodeStatusBlocked]; got != 4 {
		t.Fatalf("expected 4 blocked nodes, got %d", got)
	}
}

func TestRecordClearingEntryRequiresFeatureFlag(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	err := store.RecordClearingEntry(ctx, sqlite.ClearingEntryInput{
		EntryID:        "cl-1",
		DebtorUserID:   "user-a",
		CreditorUserID: "user-b",
		ResourceKey:    "openai_req",
		Amount:         1,
	}, false)
	if !errors.Is(err, sqlite.ErrFeatureDisabled) {
		t.Fatalf("expected ErrFeatureDisabled, got %v", err)
	}
}

func TestApprovalPersistenceFlow(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: "node-1", Type: "generic"},
		},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-approval",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}

	if err := store.CreateResourceRequest(ctx, sqlite.ResourceRequestCreateInput{
		RequestID:        "rr-1",
		TaskID:           "task-approval",
		NodeID:           "node-1",
		OwnerUserID:      "developer",
		ResourceType:     "api_key",
		ServiceID:        "2captcha",
		EstimatedCostUSD: 0.05,
		Justification:    "captcha solve",
		IdempotencyKey:   "rr-1-key",
	}); err != nil {
		t.Fatalf("create resource request: %v", err)
	}

	if err := store.CreateApprovalRequest(ctx, sqlite.ApprovalCreateInput{
		ApprovalID: "ap-1",
		RequestID:  "rr-1",
		Status:     model.ApprovalStatusPendingOperator,
		TTLSec:     300,
	}); err != nil {
		t.Fatalf("create approval request: %v", err)
	}

	if err := store.AddApprovalEvent(ctx, sqlite.ApprovalEventInput{
		EventID:    "ev-1",
		ApprovalID: "ap-1",
		EventType:  "approval_created",
		ActorID:    "system",
	}); err != nil {
		t.Fatalf("add approval event: %v", err)
	}

	if err := store.DecideApproval(ctx, sqlite.ApprovalDecisionInput{
		ApprovalID:   "ap-1",
		NewStatus:    model.ApprovalStatusApproved,
		DecidedBy:    "operator-1",
		DecisionNote: "approved",
	}); err != nil {
		t.Fatalf("decide approval: %v", err)
	}
}

func TestAT002_AutoApproveCreatesSpendAndResumesNode(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-at-002", "node-a")

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-at-002",
		NodeID:    "node-a",
		NewStatus: model.NodeStatusAwaitingFunds,
		Reason:    "resource_request_pending",
		ActorID:   "system",
	}); err != nil {
		t.Fatalf("set node to awaiting funds: %v", err)
	}

	if err := store.CreateResourceRequest(ctx, sqlite.ResourceRequestCreateInput{
		RequestID:        "rr-at-002",
		TaskID:           "task-at-002",
		NodeID:           "node-a",
		OwnerUserID:      "developer",
		ResourceType:     "api_key",
		ServiceID:        "openai",
		EstimatedCostUSD: 0.5,
		Justification:    "model call",
		IdempotencyKey:   "rr-at-002-key",
		Decision:         "AUTO_APPROVE",
		DecisionReason:   "within_budget",
	}); err != nil {
		t.Fatalf("create resource request: %v", err)
	}

	if err := store.RecordSpendTransaction(ctx, sqlite.SpendTransactionInput{
		TxID:        "tx-at-002",
		OwnerUserID: "developer",
		TaskID:      "task-at-002",
		NodeID:      "node-a",
		ServiceID:   "openai",
		AmountUSD:   0.5,
	}); err != nil {
		t.Fatalf("record spend transaction: %v", err)
	}

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-at-002",
		NodeID:    "node-a",
		NewStatus: model.NodeStatusRunning,
		Reason:    "auto_approve",
		ActorID:   "system",
	}); err != nil {
		t.Fatalf("set node to running: %v", err)
	}

	entries, err := store.ListSpendTransactionsByTask(ctx, "task-at-002")
	if err != nil {
		t.Fatalf("list spend transactions: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 spend transaction, got %d", len(entries))
	}

	status, err := store.GetTaskStatus(ctx, "", "task-at-002")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Nodes[0].Status != model.NodeStatusRunning {
		t.Fatalf("expected node status %s, got %s", model.NodeStatusRunning, status.Nodes[0].Status)
	}
}

func TestAT003_ManualApproveResumesNode(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-at-003", "node-a")

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-at-003",
		NodeID:    "node-a",
		NewStatus: model.NodeStatusAwaitingFunds,
		Reason:    "approval_required",
		ActorID:   "system",
	}); err != nil {
		t.Fatalf("set node to awaiting funds: %v", err)
	}

	createApprovalBundle(t, ctx, store, "task-at-003", "node-a", "rr-at-003", "ap-at-003")

	if err := store.DecideApproval(ctx, sqlite.ApprovalDecisionInput{
		ApprovalID:   "ap-at-003",
		NewStatus:    model.ApprovalStatusApproved,
		DecidedBy:    "operator-1",
		DecisionNote: "approved",
	}); err != nil {
		t.Fatalf("decide approval approve: %v", err)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-at-003")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Nodes[0].Status != model.NodeStatusRunning {
		t.Fatalf("expected node status %s, got %s", model.NodeStatusRunning, status.Nodes[0].Status)
	}

	approvals, err := store.ListApprovalRequests(ctx, model.ApprovalStatusApproved)
	if err != nil {
		t.Fatalf("list approvals: %v", err)
	}
	if len(approvals) == 0 {
		t.Fatalf("expected approved approvals, got 0")
	}
}

func TestAT004_ManualRejectFailsNode(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-at-004", "node-a")

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-at-004",
		NodeID:    "node-a",
		NewStatus: model.NodeStatusAwaitingFunds,
		Reason:    "approval_required",
		ActorID:   "system",
	}); err != nil {
		t.Fatalf("set node to awaiting funds: %v", err)
	}

	createApprovalBundle(t, ctx, store, "task-at-004", "node-a", "rr-at-004", "ap-at-004")

	if err := store.DecideApproval(ctx, sqlite.ApprovalDecisionInput{
		ApprovalID:   "ap-at-004",
		NewStatus:    model.ApprovalStatusRejected,
		DecidedBy:    "operator-1",
		DecisionNote: "rejected",
	}); err != nil {
		t.Fatalf("decide approval reject: %v", err)
	}

	status, err := store.GetTaskStatus(ctx, "", "task-at-004")
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.Nodes[0].Status != model.NodeStatusFailed {
		t.Fatalf("expected node status %s, got %s", model.NodeStatusFailed, status.Nodes[0].Status)
	}
	if status.Nodes[0].LastError == nil || *status.Nodes[0].LastError != "approval_rejected" {
		t.Fatalf("expected node last_error approval_rejected, got %v", status.Nodes[0].LastError)
	}
}

func TestSpendTransactionIdempotentByTxID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-idempotent-spend", "node-a")

	input := sqlite.SpendTransactionInput{
		TxID:        "tx-fixed",
		OwnerUserID: "developer",
		TaskID:      "task-idempotent-spend",
		NodeID:      "node-a",
		ServiceID:   "openai",
		AmountUSD:   0.11,
	}

	if err := store.RecordSpendTransaction(ctx, input); err != nil {
		t.Fatalf("first spend insert failed: %v", err)
	}
	if err := store.RecordSpendTransaction(ctx, input); err != nil {
		t.Fatalf("second spend insert must be idempotent, got: %v", err)
	}

	entries, err := store.ListSpendTransactionsByTask(ctx, "task-idempotent-spend")
	if err != nil {
		t.Fatalf("list spend transactions: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 spend transaction after duplicate insert, got %d", len(entries))
	}
}

func TestApprovalDecisionIdempotentForSameStatus(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-idempotent-approval", "node-a")

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-idempotent-approval",
		NodeID:    "node-a",
		NewStatus: model.NodeStatusAwaitingFunds,
		Reason:    "approval_required",
		ActorID:   "system",
	}); err != nil {
		t.Fatalf("set node to awaiting funds: %v", err)
	}

	createApprovalBundle(
		t, ctx, store,
		"task-idempotent-approval", "node-a",
		"rr-idempotent-approval", "ap-idempotent-approval",
	)

	input := sqlite.ApprovalDecisionInput{
		ApprovalID:   "ap-idempotent-approval",
		NewStatus:    model.ApprovalStatusApproved,
		DecidedBy:    "operator-1",
		DecisionNote: "approved",
	}
	if err := store.DecideApproval(ctx, input); err != nil {
		t.Fatalf("first approval decision failed: %v", err)
	}
	if err := store.DecideApproval(ctx, input); err != nil {
		t.Fatalf("second approval decision must be idempotent, got: %v", err)
	}
}

func TestEvaluateAndRecordSpendAtomic_DailyBudgetExceeded(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-guard-daily", "node-a")

	if err := store.EvaluateAndRecordSpendAtomic(ctx, sqlite.SpendGuardedInput{
		TxID:             "tx-guard-daily-1",
		OwnerUserID:      "developer",
		TaskID:           "task-guard-daily",
		NodeID:           "node-a",
		ServiceID:        "openai",
		AmountUSD:        0.70,
		MaxDailySpendUSD: 1.00,
		MaxTaskSpendUSD:  2.00,
	}); err != nil {
		t.Fatalf("first guarded spend failed: %v", err)
	}

	err := store.EvaluateAndRecordSpendAtomic(ctx, sqlite.SpendGuardedInput{
		TxID:             "tx-guard-daily-2",
		OwnerUserID:      "developer",
		TaskID:           "task-guard-daily",
		NodeID:           "node-a",
		ServiceID:        "openai",
		AmountUSD:        0.40,
		MaxDailySpendUSD: 1.00,
		MaxTaskSpendUSD:  2.00,
	})
	if !errors.Is(err, sqlite.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}

	entries, err := store.ListSpendTransactionsByTask(ctx, "task-guard-daily")
	if err != nil {
		t.Fatalf("list spend transactions: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected only first spend persisted, got %d entries", len(entries))
	}
}

func TestEvaluateAndRecordSpendAtomic_TaskBudgetExceeded(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-guard-task", "node-a")

	if err := store.EvaluateAndRecordSpendAtomic(ctx, sqlite.SpendGuardedInput{
		TxID:             "tx-guard-task-1",
		OwnerUserID:      "developer",
		TaskID:           "task-guard-task",
		NodeID:           "node-a",
		ServiceID:        "openai",
		AmountUSD:        0.55,
		MaxDailySpendUSD: 5.00,
		MaxTaskSpendUSD:  0.75,
	}); err != nil {
		t.Fatalf("first guarded spend failed: %v", err)
	}

	err := store.EvaluateAndRecordSpendAtomic(ctx, sqlite.SpendGuardedInput{
		TxID:             "tx-guard-task-2",
		OwnerUserID:      "developer",
		TaskID:           "task-guard-task",
		NodeID:           "node-a",
		ServiceID:        "openai",
		AmountUSD:        0.30,
		MaxDailySpendUSD: 5.00,
		MaxTaskSpendUSD:  0.75,
	})
	if !errors.Is(err, sqlite.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestEvaluateAndRecordSpendAtomic_IdempotentTxID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-guard-idempotent", "node-a")

	input := sqlite.SpendGuardedInput{
		TxID:             "tx-guard-idem",
		OwnerUserID:      "developer",
		TaskID:           "task-guard-idempotent",
		NodeID:           "node-a",
		ServiceID:        "openai",
		AmountUSD:        0.33,
		MaxDailySpendUSD: 2.00,
		MaxTaskSpendUSD:  2.00,
	}

	if err := store.EvaluateAndRecordSpendAtomic(ctx, input); err != nil {
		t.Fatalf("first guarded spend failed: %v", err)
	}
	if err := store.EvaluateAndRecordSpendAtomic(ctx, input); err != nil {
		t.Fatalf("second guarded spend with same tx_id must be idempotent, got: %v", err)
	}

	entries, err := store.ListSpendTransactionsByTask(ctx, "task-guard-idempotent")
	if err != nil {
		t.Fatalf("list spend transactions: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 spend transaction after duplicate guarded insert, got %d", len(entries))
	}
}

func TestListExecutableNodes_RespectsDependencies(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: "node-root", Type: "compute"},
			{NodeID: "node-child", Type: "compute", DependsOn: []string{"node-root"}},
		},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-exec-ready",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}

	ready, err := store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes: %v", err)
	}
	if len(ready) != 1 || ready[0].NodeID != "node-root" {
		t.Fatalf("expected node-root ready first, got %+v", ready)
	}

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-exec-ready",
		NodeID:    "node-root",
		NewStatus: model.NodeStatusRunning,
		Reason:    "test_run",
		ActorID:   "tester",
	}); err != nil {
		t.Fatalf("set root running: %v", err)
	}
	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-exec-ready",
		NodeID:    "node-root",
		NewStatus: model.NodeStatusResolved,
		Reason:    "test_done",
		ActorID:   "tester",
	}); err != nil {
		t.Fatalf("set root resolved: %v", err)
	}

	ready, err = store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes after root resolved: %v", err)
	}
	if len(ready) != 1 || ready[0].NodeID != "node-child" {
		t.Fatalf("expected node-child ready after dependency resolve, got %+v", ready)
	}
}

func TestListExecutableNodes_ExcludesProjectGatedExecutionTasks(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-gated-exec-list"
		projectID   = "project-gated-exec-list"
		leadID      = "lead-project-gated-exec-list"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: "node-a", Type: "generic"},
		},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:              "task-project-gated-exec",
		OwnerUserID:         "developer",
		Priority:            "critical",
		TaskKind:            model.TaskKindExecution,
		TaskTemplate:        model.TaskTemplateGeneric,
		WorkspaceID:         workspaceID,
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create project-gated execution task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       "task-plain-exec",
		OwnerUserID:  "developer",
		Priority:     "normal",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create plain execution task: %v", err)
	}

	ready, err := store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes: %v", err)
	}
	if len(ready) != 1 || ready[0].TaskID != "task-plain-exec" {
		t.Fatalf("expected only plain execution task to be daemon-ready, got %+v", ready)
	}
}

func TestUpdateTaskStatusFromNodes(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	createSingleNodeTask(t, ctx, store, "task-status-rollup", "node-a")

	status, err := store.UpdateTaskStatusFromNodes(ctx, "task-status-rollup", "tester", "initial")
	if err != nil {
		t.Fatalf("update task status initial: %v", err)
	}
	if status != model.TaskStatusPending {
		t.Fatalf("expected %s, got %s", model.TaskStatusPending, status)
	}

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-status-rollup",
		NodeID:    "node-a",
		NewStatus: model.NodeStatusRunning,
		Reason:    "test_run",
		ActorID:   "tester",
	}); err != nil {
		t.Fatalf("set node running: %v", err)
	}
	status, err = store.UpdateTaskStatusFromNodes(ctx, "task-status-rollup", "tester", "running")
	if err != nil {
		t.Fatalf("update task status running: %v", err)
	}
	if status != model.TaskStatusRunning {
		t.Fatalf("expected %s, got %s", model.TaskStatusRunning, status)
	}

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TaskID:    "task-status-rollup",
		NodeID:    "node-a",
		NewStatus: model.NodeStatusResolved,
		Reason:    "test_done",
		ActorID:   "tester",
	}); err != nil {
		t.Fatalf("set node resolved: %v", err)
	}
	status, err = store.UpdateTaskStatusFromNodes(ctx, "task-status-rollup", "tester", "resolved")
	if err != nil {
		t.Fatalf("update task status resolved: %v", err)
	}
	if status != model.TaskStatusResolved {
		t.Fatalf("expected %s, got %s", model.TaskStatusResolved, status)
	}
}

func TestNewStoreAppliesConnectionPragmasToEachPooledHandle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	db := store.DB()
	db.SetMaxOpenConns(2)

	connA, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open first conn: %v", err)
	}
	defer connA.Close()

	connB, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open second conn: %v", err)
	}
	defer connB.Close()

	for name, conn := range map[string]*sql.Conn{"first": connA, "second": connB} {
		var foreignKeys int
		if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys;").Scan(&foreignKeys); err != nil {
			t.Fatalf("%s conn foreign_keys pragma: %v", name, err)
		}
		if foreignKeys != 1 {
			t.Fatalf("%s conn expected foreign_keys=1, got %d", name, foreignKeys)
		}

		var busyTimeout int
		if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
			t.Fatalf("%s conn busy_timeout pragma: %v", name, err)
		}
		if busyTimeout != 5000 {
			t.Fatalf("%s conn expected busy_timeout=5000, got %d", name, busyTimeout)
		}

		var journalMode string
		if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode;").Scan(&journalMode); err != nil {
			t.Fatalf("%s conn journal_mode pragma: %v", name, err)
		}
		if journalMode != "wal" {
			t.Fatalf("%s conn expected journal_mode=wal, got %q", name, journalMode)
		}
	}
}

func newTestStore(t *testing.T) *sqlite.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "rhizome-test.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
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

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: nodeID, Type: "generic"},
		},
	})
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
}

func createApprovalBundle(t *testing.T, ctx context.Context, store *sqlite.Store, taskID, nodeID, requestID, approvalID string) {
	t.Helper()

	if err := store.CreateResourceRequest(ctx, sqlite.ResourceRequestCreateInput{
		RequestID:        requestID,
		TaskID:           taskID,
		NodeID:           nodeID,
		OwnerUserID:      "developer",
		ResourceType:     "api_key",
		ServiceID:        "2captcha",
		EstimatedCostUSD: 0.05,
		Justification:    "captcha solve",
		IdempotencyKey:   requestID + "-key",
	}); err != nil {
		t.Fatalf("create resource request: %v", err)
	}

	if err := store.CreateApprovalRequest(ctx, sqlite.ApprovalCreateInput{
		ApprovalID: approvalID,
		RequestID:  requestID,
		Status:     model.ApprovalStatusPendingOperator,
		TTLSec:     300,
	}); err != nil {
		t.Fatalf("create approval request: %v", err)
	}
}

func seedRuntimeEventParents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, eventIDs ...string) {
	t.Helper()

	for idx, eventID := range eventIDs {
		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:     eventID,
			WorkspaceID: workspaceID,
			EventType:   "runtime.parent",
			EntityType:  "runtime_event_parent",
			EntityID:    eventID,
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"seed":"parent"}`,
			CreatedAt:   time.Date(2026, time.March, 22, 0, 0, idx, 0, time.UTC).Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("seed runtime parent %s: %v", eventID, err)
		}
	}
}
