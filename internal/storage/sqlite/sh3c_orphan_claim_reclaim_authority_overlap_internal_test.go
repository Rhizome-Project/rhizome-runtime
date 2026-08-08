package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestReclaimOrphanedClaimedTasksRejectsSuccessorLeaseAfterForceBreakReacquire(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh3c-orphan-claim-successor-lease"
		agentID     = "agent-sh3c-orphan-claim-successor-lease"
		taskID      = "task-sh3c-orphan-claim-successor-lease"
		nodeID      = "node-sh3c-orphan-claim-successor-lease"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Orphan Claim Successor Lease",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	initialAuthority := claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Orphan Claim Successor Lease Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim before authority rollover",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("age agent last_seen_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("age task claim timestamps: %v", err)
	}

	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)
	var beforeTaskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&beforeTaskStatus); err != nil {
		t.Fatalf("query task status before successor lease reject: %v", err)
	}

	var successorAuthority WorkspaceAuthorityRecord
	var hookCalled bool
	store.beforeLocalTaskClaimReclaimTxHook = func(ctx context.Context, candidate localTaskClaimReclaimCandidate) {
		if hookCalled {
			return
		}
		hookCalled = true
		if _, err := store.ForceBreakWorkspaceAuthority(ctx, ForceBreakWorkspaceAuthorityInput{
			WorkspaceID: workspaceID,
			ActorType:   "operator",
			ActorID:     "tests",
		}); err != nil {
			t.Fatalf("force-break workspace authority in orphan-claim reclaim hook: %v", err)
		}
		reclaimed, err := store.EnsureLocalWorkspaceAuthority(ctx, EnsureLocalWorkspaceAuthorityInput{
			WorkspaceID: workspaceID,
			ActorType:   "system",
			ActorID:     "local_reclaim_manager",
		})
		if err != nil {
			t.Fatalf("reacquire local workspace authority in orphan-claim reclaim hook: %v", err)
		}
		if reclaimed.Status.Authority == nil {
			t.Fatal("expected successor authority after orphan-claim reacquire hook")
		}
		successorAuthority = *reclaimed.Status.Authority
	}
	defer func() { store.beforeLocalTaskClaimReclaimTxHook = nil }()

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed tasks: %v", err)
	}
	if !hookCalled {
		t.Fatal("expected successor lease hook to run for orphan-claim reclaim")
	}
	if successorAuthority.LeaseToken == "" || successorAuthority.LeaseToken == initialAuthority.LeaseToken || successorAuthority.Term <= initialAuthority.Term {
		t.Fatalf("expected successor authority to advance beyond initial lease, initial=%+v successor=%+v", initialAuthority, successorAuthority)
	}
	if result.TaskClaimsReleased != 0 || result.Problems != 1 {
		t.Fatalf("unexpected orphan-claim reclaim result after successor lease rollover: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "PROBLEM" {
		t.Fatalf("unexpected orphan-claim reclaim items after successor lease rollover: %+v", result.Items)
	}
	if !strings.Contains(result.Items[0].Error, "authority") && !strings.Contains(result.Items[0].Error, "lease") && !strings.Contains(result.Items[0].Error, "stale") {
		t.Fatalf("expected stale authority-style error after orphan-claim successor lease rollover, got %+v", result.Items[0])
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after successor lease reject: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected task claim to stay CLAIMED after successor lease reject, got %q", claimStatus)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after successor lease reject: %v", err)
	}
	if taskStatus != beforeTaskStatus {
		t.Fatalf("expected task status to stay unchanged after successor lease reject, before=%q after=%q", beforeTaskStatus, taskStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no task.released after orphan-claim successor lease reject, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}

func TestReclaimOrphanedClaimedTasksSkipsClaimReleaseWhenAnotherSessionAppearsBeforeReclaimBoundary(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh3c-orphan-claim-active-session-boundary"
		claimAgentID = "agent-sh3c-orphan-claim-active-session-boundary-stale"
		liveAgentID  = "agent-sh3c-orphan-claim-active-session-boundary-live"
		sessionID    = "sess-sh3c-orphan-claim-active-session-boundary"
		taskID       = "task-sh3c-orphan-claim-active-session-boundary"
		nodeID       = "node-sh3c-orphan-claim-active-session-boundary"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Orphan Claim Active Session Boundary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{claimAgentID, liveAgentID} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     claimAgentID,
		Summary:     "Claim before live session reappears",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		claimAgentID,
	); err != nil {
		t.Fatalf("age claim agent last_seen_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("age task claim timestamps: %v", err)
	}

	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)
	var beforeTaskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&beforeTaskStatus); err != nil {
		t.Fatalf("query task status before active-session boundary skip: %v", err)
	}

	var hookCalled bool
	store.beforeLocalTaskClaimReclaimTxHook = func(ctx context.Context, candidate localTaskClaimReclaimCandidate) {
		if hookCalled {
			return
		}
		hookCalled = true
		if candidate.WorkspaceID != workspaceID || candidate.TaskID != taskID {
			t.Fatalf("unexpected reclaim candidate in boundary hook: %+v", candidate)
		}
		if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			AgentID:     liveAgentID,
			TaskID:      taskID,
			StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("insert receiptless foreign session in reclaim boundary hook: %v", err)
		}
		if err := store.TouchAgentActivity(ctx, workspaceID, liveAgentID); err != nil {
			t.Fatalf("touch live agent activity in reclaim boundary hook: %v", err)
		}
	}
	defer func() { store.beforeLocalTaskClaimReclaimTxHook = nil }()

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed tasks with boundary live session: %v", err)
	}
	if !hookCalled {
		t.Fatal("expected active-session boundary hook to run for orphan-claim reclaim")
	}
	if result.TaskClaimsReleased != 1 || result.SkippedActiveSession != 0 || result.Problems != 0 {
		t.Fatalf("unexpected orphan-claim boundary reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "RELEASED" {
		t.Fatalf("unexpected orphan-claim boundary reclaim items: %+v", result.Items)
	}
	if result.Items[0].ActiveSessionID != "" || result.Items[0].ActiveSessionAgentID != "" {
		t.Fatalf("expected receiptless foreign session to be ignored in boundary reclaim item, got %+v", result.Items[0])
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after boundary active-session skip: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("expected claim to release after boundary receiptless foreign-session reclaim, got %q", claimStatus)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after boundary active-session skip: %v", err)
	}
	if taskStatus != model.TaskStatusPending {
		t.Fatalf("expected task status to return to PENDING after boundary receiptless foreign-session reclaim, before=%q after=%q", beforeTaskStatus, taskStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents+1 {
		t.Fatalf("expected one task.released after boundary receiptless foreign-session reclaim, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}

func TestReclaimOrphanedClaimedTasksSkipsClaimReleaseWhenActiveExecutionAppearsBeforeReclaimBoundary(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh3c-orphan-claim-active-execution-boundary"
		claimAgentID = "agent-sh3c-orphan-claim-active-execution-boundary-stale"
		execAgentID  = "agent-sh3c-orphan-claim-active-execution-boundary-live"
		taskID       = "task-sh3c-orphan-claim-active-execution-boundary"
		nodeID       = "node-sh3c-orphan-claim-active-execution-boundary"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Orphan Claim Active Execution Boundary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{claimAgentID, execAgentID} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     claimAgentID,
		Summary:     "Claim before live execution reappears",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		claimAgentID,
	); err != nil {
		t.Fatalf("age claim agent last_seen_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("age task claim timestamps: %v", err)
	}

	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)

	var hookCalled bool
	store.beforeLocalTaskClaimReclaimTxHook = func(ctx context.Context, candidate localTaskClaimReclaimCandidate) {
		if hookCalled {
			return
		}
		hookCalled = true
		if candidate.WorkspaceID != workspaceID || candidate.TaskID != taskID {
			t.Fatalf("unexpected reclaim candidate in active-execution boundary hook: %+v", candidate)
		}
		if err := store.TouchAgentActivity(ctx, workspaceID, execAgentID); err != nil {
			t.Fatalf("touch execution agent activity in reclaim boundary hook: %v", err)
		}
		if _, err := store.ClaimNodeWithEvent(ctx, NodeClaimInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			NodeID:      nodeID,
			AgentID:     execAgentID,
			Summary:     "Live execution reappeared after reclaim listing",
		}); err != nil {
			t.Fatalf("claim node in reclaim boundary hook: %v", err)
		}
	}
	defer func() { store.beforeLocalTaskClaimReclaimTxHook = nil }()

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed tasks with boundary live execution: %v", err)
	}
	if !hookCalled {
		t.Fatal("expected active-execution boundary hook to run for orphan-claim reclaim")
	}
	if result.TaskClaimsReleased != 0 || result.SkippedActiveExecution != 1 || result.Problems != 0 {
		t.Fatalf("unexpected orphan-claim boundary reclaim result with live execution: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "SKIPPED_ACTIVE_EXECUTION" {
		t.Fatalf("unexpected orphan-claim boundary reclaim items with live execution: %+v", result.Items)
	}
	if result.Items[0].ActiveSessionID != "" || result.Items[0].ActiveSessionAgentID != "" {
		t.Fatalf("expected no active-session metadata when boundary live execution wins, got %+v", result.Items[0])
	}
	if !strings.Contains(result.Items[0].ActiveExecutionState, "claimed_node_claims:1") || !strings.Contains(result.Items[0].ActiveExecutionState, "active_nodes:1") {
		t.Fatalf("expected active execution evidence in reclaim item, got %+v", result.Items[0])
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after boundary active-execution skip: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected claim to remain claimed after boundary active-execution skip, got %q", claimStatus)
	}

	var nodeClaimAgentID, nodeClaimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status FROM node_claims WHERE workspace_id = ? AND task_id = ? AND node_id = ?`,
		workspaceID,
		taskID,
		nodeID,
	).Scan(&nodeClaimAgentID, &nodeClaimStatus); err != nil {
		t.Fatalf("query node claim after boundary active-execution skip: %v", err)
	}
	if nodeClaimAgentID != execAgentID || nodeClaimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected winning live execution node claim to remain claimed, agent=%q status=%q", nodeClaimAgentID, nodeClaimStatus)
	}

	var nodeStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status FROM dag_nodes WHERE task_id = ? AND node_id = ?`,
		taskID,
		nodeID,
	).Scan(&nodeStatus); err != nil {
		t.Fatalf("query dag node status after boundary active-execution skip: %v", err)
	}
	if nodeStatus != model.NodeStatusRunning {
		t.Fatalf("expected dag node to stay RUNNING after boundary active-execution skip, got %q", nodeStatus)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after boundary active-execution skip: %v", err)
	}
	if taskStatus != model.TaskStatusRunning {
		t.Fatalf("expected task to stay RUNNING after boundary active-execution skip, got %q", taskStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no task.released after boundary active-execution skip, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}
