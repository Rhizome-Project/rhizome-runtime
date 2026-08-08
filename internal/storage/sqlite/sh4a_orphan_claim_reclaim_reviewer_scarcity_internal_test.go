package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestReclaimOrphanedClaimedTasksSkipsClaimReleaseWhenReviewerScarcitySaturated(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh4a-orphan-claim-reviewer-scarcity"
		staleAgentID = "agent-sh4a-orphan-claim-stale"
		taskID       = "task-sh4a-orphan-claim-stale"
		nodeID       = "node-sh4a-orphan-claim-stale"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH4A Orphan Claim Reviewer Scarcity",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     staleAgentID,
		OwnerUserID: "developer",
		DisplayName: "Stale Claimed Agent",
	}); err != nil {
		t.Fatalf("register stale orphan-claim agent: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "reviewer-c")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-c", "reviewer")

	liveAt := "2026-04-15T13:00:00Z"
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
	if _, err := store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     staleAgentID,
		Summary:     "Claim before saturated orphan reclaim",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		staleAgentID,
	); err != nil {
		t.Fatalf("age stale orphan-claim agent last_seen_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("age orphan-claim timestamps: %v", err)
	}

	var beforeTaskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&beforeTaskStatus); err != nil {
		t.Fatalf("query task status before orphan reclaim: %v", err)
	}
	if beforeTaskStatus != model.TaskStatusRunning {
		t.Fatalf("expected claimed task to stay in running state before orphan reclaim, got %q", beforeTaskStatus)
	}
	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID)

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed tasks: %v", err)
	}
	if result.TaskClaimsReleased != 0 || result.SkippedRecent != 0 || result.SkippedActiveSession != 0 || result.SkippedActiveExecution != 0 || result.Problems != 0 {
		t.Fatalf("unexpected orphan reclaim result under reviewer scarcity saturation: %+v", result)
	}
	if len(result.Items) != 1 {
		t.Fatalf("unexpected orphan reclaim items under reviewer scarcity saturation: %+v", result.Items)
	}
	item := result.Items[0]
	if item.TaskAction != "SKIPPED_REVIEWER_SCARCITY" {
		t.Fatalf("unexpected orphan reclaim item under reviewer scarcity saturation: %+v", item)
	}
	if item.ActiveSessionID != "" || item.ActiveSessionAgentID != "" || item.ActiveExecutionState != "" {
		t.Fatalf("expected no competing activity on orphan reclaim reviewer-scarcity path, got %+v", item)
	}
	if item.AgentID != staleAgentID || item.ClaimUpdatedAt == "" || item.AgentLastSeenAt == "" {
		t.Fatalf("expected stale orphan-claim context to be preserved, got %+v", item)
	}

	var claimOwner, claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimOwner, &claimStatus); err != nil {
		t.Fatalf("query task claim after reviewer-scarcity orphan reclaim: %v", err)
	}
	if claimOwner != staleAgentID || claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected task claim to remain claimed under reviewer-scarcity orphan reclaim, owner=%q status=%q", claimOwner, claimStatus)
	}

	var afterTaskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&afterTaskStatus); err != nil {
		t.Fatalf("query task status after reviewer-scarcity orphan reclaim: %v", err)
	}
	if afterTaskStatus != model.TaskStatusRunning || afterTaskStatus != beforeTaskStatus {
		t.Fatalf("expected task status to stay RUNNING under reviewer-scarcity orphan reclaim, before=%q after=%q", beforeTaskStatus, afterTaskStatus)
	}

	if got := countRuntimeEventsByEntityAndTypeInternal(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no task.released under reviewer-scarcity orphan reclaim, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}
