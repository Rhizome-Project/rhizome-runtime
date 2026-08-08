package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestReclaimAbandonedSessionOwnershipSkipsClaimReleaseWhenReviewerScarcitySaturated(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh4a-session-reclaim-reviewer-scarcity"
		staleAgentID = "agent-sh4a-session-reclaim-stale"
		sessionID    = "sess-sh4a-session-reclaim-stale"
		taskID       = "task-sh4a-session-reclaim-stale"
		nodeID       = "node-sh4a-session-reclaim-stale"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH4A Session Reclaim Reviewer Scarcity",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     staleAgentID,
		OwnerUserID: "developer",
		DisplayName: "Stale Session Agent",
	}); err != nil {
		t.Fatalf("register stale session agent: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "reviewer-c")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-c", "reviewer")

	liveAt := "2026-04-12T11:00:00Z"
	for _, tensionID := range []string{"tension-a", "tension-b", "tension-c"} {
		insertReviewerRouteLoadTension(t, ctx, store, workspaceID, tensionID, liveAt)
	}
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-a", "coal-a", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-a", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-a", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-b", "coal-b", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-b", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-b", "reviewer-b", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-c", "coal-c", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-c", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-c", "reviewer-c", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	scarcity, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("reviewer mesh scarcity snapshot: %v", err)
	}
	if scarcity.Status != "SATURATED" {
		t.Fatalf("expected saturated reviewer scarcity, got %+v", scarcity)
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
		Summary:     "Started before reviewer-scarcity reclaim",
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
		Summary:     "Blocked before reviewer-scarcity reclaim",
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

	var queueID, beforeTaskStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+sessionID+":blocker",
	).Scan(&queueID); err != nil {
		t.Fatalf("lookup blocker queue: %v", err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&beforeTaskStatus); err != nil {
		t.Fatalf("query task status before reclaim: %v", err)
	}

	beforeSessionEndEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID)
	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)
	beforeQueueResolvedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID)

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership: %v", err)
	}
	if result.SessionsEnded != 1 || result.SessionQueuesResolved != 1 || result.TaskClaimsReleased != 0 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result under reviewer scarcity saturation: %+v", result)
	}
	if len(result.Items) != 1 {
		t.Fatalf("unexpected reclaim items under reviewer scarcity saturation: %+v", result.Items)
	}
	item := result.Items[0]
	if item.SessionAction != "RECLAIMED" || item.QueueAction != "RESOLVED" || item.TaskAction != "SKIPPED_REVIEWER_SCARCITY" {
		t.Fatalf("unexpected reclaim item under reviewer scarcity saturation: %+v", item)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after reviewer-scarcity reclaim: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected stale session to end under reviewer-scarcity reclaim, got %+v", state)
	}

	var queueStatus, resolvedBy string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(resolved_by,'') FROM operator_queue_items WHERE workspace_id = ? AND queue_id = ?`,
		workspaceID,
		queueID,
	).Scan(&queueStatus, &resolvedBy); err != nil {
		t.Fatalf("query blocker queue after reviewer-scarcity reclaim: %v", err)
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
		t.Fatalf("query task claim after reviewer-scarcity reclaim: %v", err)
	}
	if claimOwner != staleAgentID || claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected task claim to remain claimed under reviewer-scarcity reclaim, owner=%q status=%q", claimOwner, claimStatus)
	}

	var afterTaskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&afterTaskStatus); err != nil {
		t.Fatalf("query task status after reviewer-scarcity reclaim: %v", err)
	}
	if afterTaskStatus != beforeTaskStatus {
		t.Fatalf("expected task status to stay unchanged under reviewer-scarcity reclaim, before=%q after=%q", beforeTaskStatus, afterTaskStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID); got != beforeSessionEndEvents+1 {
		t.Fatalf("expected one session.end under reviewer-scarcity reclaim, before=%d after=%d", beforeSessionEndEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "operator_queue.resolved", queueID); got != beforeQueueResolvedEvents+1 {
		t.Fatalf("expected one operator_queue.resolved under reviewer-scarcity reclaim, before=%d after=%d", beforeQueueResolvedEvents, got)
	}
	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no task.released under reviewer-scarcity reclaim, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}
