package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestReclaimAbandonedSessionOwnershipRejectsSuccessorLeaseAfterForceBreakReacquire(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh3c-session-reclaim-successor-lease"
		agentID     = "agent-sh3c-session-reclaim-successor-lease"
		sessionID   = "sess-sh3c-session-reclaim-successor-lease"
		taskID      = "task-sh3c-session-reclaim-successor-lease"
		nodeID      = "node-sh3c-session-reclaim-successor-lease"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Session Reclaim Successor Lease",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	initialAuthority := claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Successor Lease Agent",
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before authority rollover",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Blocked before authority rollover",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
		UpdatedAt:   staleAt,
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync blocked session operator queue: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("age agent last_seen_at: %v", err)
	}

	var queueID string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+sessionID+":blocker",
	).Scan(&queueID); err != nil {
		t.Fatalf("lookup blocker queue: %v", err)
	}

	beforeSessionEndEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID)
	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)
	beforeQueueResolvedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID)

	var successorAuthority WorkspaceAuthorityRecord
	var hookCalled bool
	store.beforeLocalSessionReclaimTxHook = func(ctx context.Context, candidate localSessionReclaimCandidate) {
		if hookCalled {
			return
		}
		hookCalled = true
		if _, err := store.ForceBreakWorkspaceAuthority(ctx, ForceBreakWorkspaceAuthorityInput{
			WorkspaceID: workspaceID,
			ActorType:   "operator",
			ActorID:     "tests",
		}); err != nil {
			t.Fatalf("force-break workspace authority in reclaim hook: %v", err)
		}
		reclaimed, err := store.EnsureLocalWorkspaceAuthority(ctx, EnsureLocalWorkspaceAuthorityInput{
			WorkspaceID: workspaceID,
			ActorType:   "system",
			ActorID:     "local_reclaim_manager",
		})
		if err != nil {
			t.Fatalf("reacquire local workspace authority in reclaim hook: %v", err)
		}
		if reclaimed.Status.Authority == nil {
			t.Fatal("expected successor authority after reacquire hook")
		}
		successorAuthority = *reclaimed.Status.Authority
	}
	defer func() { store.beforeLocalSessionReclaimTxHook = nil }()

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership: %v", err)
	}
	if !hookCalled {
		t.Fatal("expected successor lease hook to run")
	}
	if successorAuthority.LeaseToken == "" || successorAuthority.LeaseToken == initialAuthority.LeaseToken || successorAuthority.Term <= initialAuthority.Term {
		t.Fatalf("expected successor authority to advance beyond initial lease, initial=%+v successor=%+v", initialAuthority, successorAuthority)
	}
	if result.SessionsEnded != 0 || result.TaskClaimsReleased != 0 || result.SessionQueuesResolved != 0 || result.Problems != 1 {
		t.Fatalf("unexpected reclaim result after successor lease rollover: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].SessionAction != "PROBLEM" {
		t.Fatalf("unexpected reclaim items after successor lease rollover: %+v", result.Items)
	}
	if !strings.Contains(result.Items[0].Error, "authority") && !strings.Contains(result.Items[0].Error, "lease") && !strings.Contains(result.Items[0].Error, "stale") {
		t.Fatalf("expected stale authority-style error after successor lease rollover, got %+v", result.Items[0])
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after successor lease reject: %v", err)
	}
	if state.Status != model.SessionStatusBlocked {
		t.Fatalf("expected blocked session to stay blocked after successor lease reject, got %+v", state)
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

	var queueStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status FROM operator_queue_items WHERE workspace_id = ? AND queue_id = ?`,
		workspaceID,
		queueID,
	).Scan(&queueStatus); err != nil {
		t.Fatalf("query operator queue after successor lease reject: %v", err)
	}
	if queueStatus != "OPEN" {
		t.Fatalf("expected blocker queue to stay OPEN after successor lease reject, got %q", queueStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID); got != beforeSessionEndEvents {
		t.Fatalf("expected no session.end after successor lease reject, before=%d after=%d", beforeSessionEndEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no task.released after successor lease reject, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID); got != beforeQueueResolvedEvents {
		t.Fatalf("expected no operator_queue.resolved after successor lease reject, before=%d after=%d", beforeQueueResolvedEvents, got)
	}
}

func TestReclaimAbandonedSessionOwnershipSkipsClaimReleaseWhenActiveExecutionAppearsBeforeReclaimBoundary(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh3c-session-reclaim-active-execution-boundary"
		staleAgentID = "agent-sh3c-session-reclaim-active-execution-boundary-stale"
		execAgentID  = "agent-sh3c-session-reclaim-active-execution-boundary-live"
		sessionID    = "sess-sh3c-session-reclaim-active-execution-boundary"
		taskID       = "task-sh3c-session-reclaim-active-execution-boundary"
		nodeID       = "node-sh3c-session-reclaim-active-execution-boundary"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Session Reclaim Active Execution Boundary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{staleAgentID, execAgentID} {
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, staleAgentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     staleAgentID,
		TaskID:      taskID,
		Summary:     "Started before reclaim boundary",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     staleAgentID,
		TaskID:      taskID,
		Summary:     "Blocked before reclaim boundary",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
		UpdatedAt:   staleAt,
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync blocked session operator queue: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		staleAgentID,
	); err != nil {
		t.Fatalf("age stale agent last_seen_at: %v", err)
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

	var queueID string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+sessionID+":blocker",
	).Scan(&queueID); err != nil {
		t.Fatalf("lookup blocker queue: %v", err)
	}

	beforeSessionEndEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID)
	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)
	beforeQueueResolvedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID)

	var hookCalled bool
	store.beforeLocalSessionReclaimTxHook = func(ctx context.Context, candidate localSessionReclaimCandidate) {
		if hookCalled {
			return
		}
		hookCalled = true
		if candidate.WorkspaceID != workspaceID || candidate.SessionID != sessionID {
			t.Fatalf("unexpected session reclaim candidate in boundary hook: %+v", candidate)
		}
		if _, err := store.ClaimNodeWithEvent(ctx, NodeClaimInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			NodeID:      nodeID,
			AgentID:     execAgentID,
			Summary:     "Live execution appeared after reclaim listing",
		}); err != nil {
			t.Fatalf("claim node in session reclaim boundary hook: %v", err)
		}
	}
	defer func() { store.beforeLocalSessionReclaimTxHook = nil }()

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership with boundary live execution: %v", err)
	}
	if !hookCalled {
		t.Fatal("expected active-execution boundary hook to run for session reclaim")
	}
	if result.SessionsEnded != 1 || result.SessionQueuesResolved != 1 || result.TaskClaimsReleased != 0 || result.Problems != 0 {
		t.Fatalf("unexpected session reclaim boundary result with live execution: %+v", result)
	}
	if len(result.Items) != 1 {
		t.Fatalf("unexpected session reclaim boundary items with live execution: %+v", result.Items)
	}
	item := result.Items[0]
	if item.SessionAction != "RECLAIMED" || item.QueueAction != "RESOLVED" || item.TaskAction != "SKIPPED_ACTIVE_EXECUTION" {
		t.Fatalf("unexpected session reclaim boundary item with live execution: %+v", item)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after boundary active-execution reclaim: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected stale session to end after boundary active-execution reclaim, got %+v", state)
	}

	var queueStatus, resolvedBy string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(resolved_by,'') FROM operator_queue_items WHERE workspace_id = ? AND queue_id = ?`,
		workspaceID,
		queueID,
	).Scan(&queueStatus, &resolvedBy); err != nil {
		t.Fatalf("query operator queue after boundary active-execution reclaim: %v", err)
	}
	if queueStatus != "RESOLVED" || resolvedBy != "local_reclaim_manager" {
		t.Fatalf("expected blocker queue to resolve under reclaim manager, status=%q resolved_by=%q", queueStatus, resolvedBy)
	}

	var claimOwner, claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimOwner, &claimStatus); err != nil {
		t.Fatalf("query task claim after boundary active-execution reclaim: %v", err)
	}
	if claimOwner != staleAgentID || claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected stale task claim to remain claimed for manual recovery, owner=%q status=%q", claimOwner, claimStatus)
	}

	var nodeClaimCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM node_claims WHERE workspace_id = ? AND task_id = ? AND node_id = ?`,
		workspaceID,
		taskID,
		nodeID,
	).Scan(&nodeClaimCount); err != nil {
		t.Fatalf("count node claims after boundary active-execution reclaim: %v", err)
	}
	if nodeClaimCount != 1 {
		t.Fatalf("expected exactly one live execution node claim after boundary reclaim, got %d", nodeClaimCount)
	}

	var nodeClaimOwner, nodeClaimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status FROM node_claims WHERE workspace_id = ? AND task_id = ? AND node_id = ?`,
		workspaceID,
		taskID,
		nodeID,
	).Scan(&nodeClaimOwner, &nodeClaimStatus); err != nil {
		t.Fatalf("query node claim after boundary active-execution reclaim: %v", err)
	}
	if nodeClaimOwner != execAgentID || nodeClaimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected live execution node claim to remain claimed, owner=%q status=%q", nodeClaimOwner, nodeClaimStatus)
	}

	var nodeStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status FROM dag_nodes WHERE task_id = ? AND node_id = ?`,
		taskID,
		nodeID,
	).Scan(&nodeStatus); err != nil {
		t.Fatalf("query dag node status after boundary active-execution reclaim: %v", err)
	}
	if nodeStatus != model.NodeStatusRunning {
		t.Fatalf("expected dag node to stay RUNNING after boundary active-execution reclaim, got %q", nodeStatus)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after boundary active-execution reclaim: %v", err)
	}
	if taskStatus != model.TaskStatusRunning {
		t.Fatalf("expected task to stay RUNNING after boundary active-execution reclaim, got %q", taskStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID); got != beforeSessionEndEvents+1 {
		t.Fatalf("expected one session.end after boundary active-execution reclaim, before=%d after=%d", beforeSessionEndEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID); got != beforeQueueResolvedEvents+1 {
		t.Fatalf("expected one operator_queue.resolved after boundary active-execution reclaim, before=%d after=%d", beforeQueueResolvedEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no task.released after boundary active-execution reclaim, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}

func TestReclaimAbandonedSessionOwnershipSkipsClaimReleaseWhenAnotherSessionAppearsBeforeReclaimBoundary(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh3c-session-reclaim-active-session-boundary"
		staleAgentID = "agent-sh3c-session-reclaim-active-session-boundary-stale"
		freshAgentID = "agent-sh3c-session-reclaim-active-session-boundary-fresh"
		staleSession = "sess-sh3c-session-reclaim-active-session-boundary-stale"
		freshSession = "sess-sh3c-session-reclaim-active-session-boundary-fresh"
		taskID       = "task-sh3c-session-reclaim-active-session-boundary"
		nodeID       = "node-sh3c-session-reclaim-active-session-boundary"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Session Reclaim Active Session Boundary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{staleAgentID, freshAgentID} {
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, staleAgentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   staleSession,
		AgentID:     staleAgentID,
		TaskID:      taskID,
		Summary:     "Started before reclaim boundary",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record stale session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   staleSession,
		AgentID:     staleAgentID,
		TaskID:      taskID,
		Summary:     "Blocked before reclaim boundary",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
		UpdatedAt:   staleAt,
	})
	if err != nil {
		t.Fatalf("record blocked stale session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync blocked stale session operator queue: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		staleAgentID,
	); err != nil {
		t.Fatalf("age stale agent last_seen_at: %v", err)
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

	var queueID string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+staleSession+":blocker",
	).Scan(&queueID); err != nil {
		t.Fatalf("lookup blocker queue: %v", err)
	}

	beforeSessionEndEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, staleSession)
	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)
	beforeQueueResolvedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID)

	var hookCalled bool
	store.beforeLocalSessionReclaimTxHook = func(ctx context.Context, candidate localSessionReclaimCandidate) {
		if hookCalled {
			return
		}
		hookCalled = true
		if candidate.WorkspaceID != workspaceID || candidate.SessionID != staleSession {
			t.Fatalf("unexpected session reclaim candidate in live-session boundary hook: %+v", candidate)
		}
		if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
			WorkspaceID: workspaceID,
			SessionID:   freshSession,
			AgentID:     freshAgentID,
			TaskID:      taskID,
			StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("insert receiptless foreign session in reclaim boundary hook: %v", err)
		}
		if err := store.TouchAgentActivity(ctx, workspaceID, freshAgentID); err != nil {
			t.Fatalf("touch fresh agent activity in reclaim boundary hook: %v", err)
		}
	}
	defer func() { store.beforeLocalSessionReclaimTxHook = nil }()

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership with boundary live session: %v", err)
	}
	if !hookCalled {
		t.Fatal("expected live-session boundary hook to run for session reclaim")
	}
	if result.SessionsEnded != 1 || result.SessionQueuesResolved != 1 || result.TaskClaimsReleased != 1 || result.Problems != 0 {
		t.Fatalf("unexpected session reclaim boundary result with live session: %+v", result)
	}
	if len(result.Items) != 1 {
		t.Fatalf("unexpected session reclaim boundary items with live session: %+v", result.Items)
	}
	item := result.Items[0]
	if item.SessionAction != "RECLAIMED" || item.QueueAction != "RESOLVED" || item.TaskAction != "RELEASED" {
		t.Fatalf("unexpected session reclaim boundary item with live session: %+v", item)
	}

	staleState, err := store.GetAgentSessionState(ctx, workspaceID, staleSession)
	if err != nil {
		t.Fatalf("get stale session state after boundary live-session reclaim: %v", err)
	}
	if staleState.Status != model.SessionStatusEnded {
		t.Fatalf("expected stale session to end after boundary live-session reclaim, got %+v", staleState)
	}
	freshState, err := store.GetAgentSessionState(ctx, workspaceID, freshSession)
	if err != nil {
		t.Fatalf("get fresh session state after boundary live-session reclaim: %v", err)
	}
	if !model.IsSessionStatusActive(freshState.Status) {
		t.Fatalf("expected fresh session to stay active after boundary live-session reclaim, got %+v", freshState)
	}

	var queueStatus, resolvedBy string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(resolved_by,'') FROM operator_queue_items WHERE workspace_id = ? AND queue_id = ?`,
		workspaceID,
		queueID,
	).Scan(&queueStatus, &resolvedBy); err != nil {
		t.Fatalf("query operator queue after boundary live-session reclaim: %v", err)
	}
	if queueStatus != "RESOLVED" || resolvedBy != "local_reclaim_manager" {
		t.Fatalf("expected blocker queue to resolve under reclaim manager, status=%q resolved_by=%q", queueStatus, resolvedBy)
	}

	var claimOwner, claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimOwner, &claimStatus); err != nil {
		t.Fatalf("query task claim after boundary live-session reclaim: %v", err)
	}
	if claimOwner != staleAgentID || claimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("expected stale task claim to release despite receiptless foreign session, owner=%q status=%q", claimOwner, claimStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, staleSession); got != beforeSessionEndEvents+1 {
		t.Fatalf("expected one session.end after boundary live-session reclaim, before=%d after=%d", beforeSessionEndEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID); got != beforeQueueResolvedEvents+1 {
		t.Fatalf("expected one operator_queue.resolved after boundary live-session reclaim, before=%d after=%d", beforeQueueResolvedEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents+1 {
		t.Fatalf("expected one task.released after boundary receiptless foreign-session reclaim, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}

func countRuntimeEventsByEntityAndTypeInternal(t *testing.T, ctx context.Context, store *Store, workspaceID, eventType, entityID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM runtime_events
		 WHERE workspace_id = ? AND event_type = ? AND entity_id = ?`,
		workspaceID,
		eventType,
		entityID,
	).Scan(&count); err != nil {
		t.Fatalf("count runtime events %s/%s: %v", eventType, entityID, err)
	}
	return count
}
