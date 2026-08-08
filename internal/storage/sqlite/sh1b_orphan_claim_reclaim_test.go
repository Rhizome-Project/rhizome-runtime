package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestReclaimOrphanedClaimedTasksReleasesStaleClaimWithoutSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-claim"
		agentID     = "agent-sh1b-orphan-claim"
		taskID      = "task-sh1b-orphan-claim"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Claim Reclaim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Orphan Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim without session before crash",
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

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed tasks: %v", err)
	}
	if result.TaskClaimsReleased != 1 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "RELEASED" {
		t.Fatalf("unexpected reclaim items: %+v", result.Items)
	}

	var claimStatus, claimSummary string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status, summary FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus, &claimSummary); err != nil {
		t.Fatalf("query task claim after orphan reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased || claimSummary != "Local ownership reclaim after stale task claim without session" {
		t.Fatalf("unexpected released task claim after orphan reclaim: status=%q summary=%q", claimStatus, claimSummary)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after orphan reclaim: %v", err)
	}
	if taskStatus != model.TaskStatusPending {
		t.Fatalf("expected task to return to PENDING after orphan reclaim, got %q", taskStatus)
	}

	releasedEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.released",
		EntityType:  "task",
		EntityID:    taskID,
		Limit:       10,
	})
	if releasedEvent.ActorType != "system" || releasedEvent.ActorID != "local_lease_manager" || releasedEvent.AgentID != agentID {
		t.Fatalf("unexpected orphan claim reclaim runtime event actor envelope: %+v", releasedEvent)
	}
	requireRuntimeEventAuthorityMetadataForSessionReclaim(t, store, ctx, workspaceID, "task.released", taskID, authority)
}

func TestReclaimOrphanedClaimedTasksSkipsRecentClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-claim-recent"
		agentID     = "agent-sh1b-orphan-claim-recent"
		taskID      = "task-sh1b-orphan-claim-recent"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Claim Recent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Recent Orphan Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Fresh orphan claim",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.TouchAgentActivity(ctx, workspaceID, agentID); err != nil {
		t.Fatalf("touch agent activity: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim recent orphan claimed task: %v", err)
	}
	if result.TaskClaimsReleased != 0 || result.SkippedRecent != 1 || result.Problems != 0 {
		t.Fatalf("unexpected recent orphan reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "SKIPPED_RECENT" {
		t.Fatalf("unexpected recent orphan reclaim items: %+v", result.Items)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after recent orphan reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected fresh orphan claim to remain claimed, got %q", claimStatus)
	}
}

func TestReclaimOrphanedClaimedTasksKeepsHeartbeatWindowClaimAlive(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-claim-heartbeat-window"
		agentID     = "agent-sh1b-orphan-claim-heartbeat-window"
		taskID      = "task-sh1b-orphan-claim-heartbeat-window"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Claim Heartbeat Window",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Heartbeat Window Orphan Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim should survive heartbeat-window orphan reclaim",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	heartbeatWindowAt := time.Now().UTC().Add(-(2*time.Minute + 15*time.Second)).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		heartbeatWindowAt,
		heartbeatWindowAt,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("age agent last_seen_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		heartbeatWindowAt,
		heartbeatWindowAt,
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("age task claim timestamps: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim heartbeat-window orphan claimed task: %v", err)
	}
	if result.TaskClaimsReleased != 0 || result.SkippedRecent != 1 || result.Problems != 0 {
		t.Fatalf("unexpected heartbeat-window orphan reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "SKIPPED_RECENT" {
		t.Fatalf("unexpected heartbeat-window orphan reclaim items: %+v", result.Items)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after heartbeat-window orphan reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected heartbeat-window orphan claim to stay CLAIMED, got %q", claimStatus)
	}
}

func TestReclaimOrphanedClaimedTasksSkipsWhenAnyActiveSessionExistsForTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh1b-orphan-claim-session"
		claimAgentID = "agent-sh1b-orphan-claim-session"
		otherAgentID = "agent-sh1b-orphan-claim-session-foreign"
		sessionID    = "sess-sh1b-orphan-claim-session"
		taskID       = "task-sh1b-orphan-claim-session"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Claim Active Session",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{claimAgentID, otherAgentID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     claimAgentID,
		Summary:     "Claim without session before crash",
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
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     otherAgentID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("insert receiptless foreign active session: %v", err)
	}
	if err := store.TouchAgentActivity(ctx, workspaceID, otherAgentID); err != nil {
		t.Fatalf("touch foreign agent activity: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphan claim with active session: %v", err)
	}
	if result.TaskClaimsReleased != 1 || result.SkippedActiveSession != 0 || result.Problems != 0 {
		t.Fatalf("unexpected orphan reclaim result with active session: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "RELEASED" {
		t.Fatalf("unexpected orphan reclaim items with active session: %+v", result.Items)
	}
	if result.Items[0].ActiveSessionID != "" || result.Items[0].ActiveSessionAgentID != "" {
		t.Fatalf("expected receiptless foreign session to be ignored in reclaim item, got %+v", result.Items[0])
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after active-session skip: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("expected claim to release when only receiptless foreign session exists, got %q", claimStatus)
	}
}

func TestReclaimOrphanedClaimedTasksReleasesStaleBlockedClaimWithoutPendingBlocker(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-blocked-claim"
		agentID     = "agent-sh1b-orphan-blocked-claim"
		taskID      = "task-sh1b-orphan-blocked-claim"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Blocked Claim Reclaim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Orphan Blocked Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim before stale block",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "Lost stale execution truth",
	}); err != nil {
		t.Fatalf("block task: %v", err)
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
		t.Fatalf("age blocked claim timestamps: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim stale blocked claim: %v", err)
	}
	if result.TaskClaimsReleased != 1 || result.Problems != 0 {
		t.Fatalf("unexpected blocked-claim reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "RELEASED" {
		t.Fatalf("unexpected blocked-claim reclaim items: %+v", result.Items)
	}

	var claimStatus, claimSummary string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status, summary FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus, &claimSummary); err != nil {
		t.Fatalf("query blocked claim after reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased || claimSummary != "Local ownership reclaim after stale task claim without session" {
		t.Fatalf("unexpected released blocked claim after reclaim: status=%q summary=%q", claimStatus, claimSummary)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after blocked-claim reclaim: %v", err)
	}
	if taskStatus != model.TaskStatusPending {
		t.Fatalf("expected blocked task to return to PENDING after reclaim, got %q", taskStatus)
	}

	releasedEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.released",
		EntityType:  "task",
		EntityID:    taskID,
		Limit:       10,
	})
	if releasedEvent.ActorType != "system" || releasedEvent.ActorID != "local_lease_manager" || releasedEvent.AgentID != agentID {
		t.Fatalf("unexpected blocked-claim reclaim runtime event actor envelope: %+v", releasedEvent)
	}
	requireRuntimeEventAuthorityMetadataForSessionReclaim(t, store, ctx, workspaceID, "task.released", taskID, authority)
}

func TestReclaimOrphanedClaimedTasksSkipsBlockedClaimBackedByPendingHumanAction(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-blocked-pending-action"
		agentID     = "agent-sh1b-orphan-blocked-pending-action"
		taskID      = "task-sh1b-orphan-blocked-pending-action"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Blocked Claim Pending Action",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Pending Action Blocked Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim before blocking action",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need operator decision",
		Blocking:    true,
	}); err != nil {
		t.Fatalf("create blocking action: %v", err)
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
		t.Fatalf("age blocked claim timestamps: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim blocked claim with pending action: %v", err)
	}
	if result.TaskClaimsReleased != 0 || result.Problems != 0 {
		t.Fatalf("unexpected pending-action reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "SKIPPED_PENDING_BLOCKER" {
		t.Fatalf("unexpected pending-action reclaim items: %+v", result.Items)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query blocked claim after pending-action skip: %v", err)
	}
	if claimStatus != model.TaskClaimStatusBlocked {
		t.Fatalf("expected blocked claim to remain BLOCKED with pending human action, got %q", claimStatus)
	}

	var blockerCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(1) FROM task_claim_blockers WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&blockerCount); err != nil {
		t.Fatalf("count blocker snapshots after pending-action skip: %v", err)
	}
	if blockerCount != 1 {
		t.Fatalf("expected blocker snapshot to remain while pending action exists, got %d", blockerCount)
	}
}

func TestReclaimOrphanedClaimedTasksIgnoresMissingAgentClaimAfterCascade(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-claim-missing-agent"
		agentID     = "agent-sh1b-orphan-claim-missing-agent"
		taskID      = "task-sh1b-orphan-claim-missing-agent"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Claim Missing Agent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Missing Agent Claim Owner",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim before agent row drift",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("age task claim timestamps: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`DELETE FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("delete agent row: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed tasks with missing agent row: %v", err)
	}
	if result.TaskClaimsReleased != 0 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result with missing agent row: %+v", result)
	}
	if len(result.Items) != 0 {
		t.Fatalf("unexpected reclaim items with missing agent row: %+v", result.Items)
	}

	var claimCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(1) FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimCount); err != nil {
		t.Fatalf("count task claims after missing-agent cascade: %v", err)
	}
	if claimCount != 0 {
		t.Fatalf("expected missing-agent cascade to remove orphan claim before reclaim, got %d row(s)", claimCount)
	}
	_ = authority
}

func TestReclaimOrphanedClaimedTasksSkipsWhenTaskHasLiveExecutionEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-claim-live-exec"
		agentID     = "agent-sh1b-orphan-claim-live-exec"
		taskID      = "task-sh1b-orphan-claim-live-exec"
		nodeID      = "node-sh1b-orphan-claim-live-exec"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Claim Live Execution",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Live Execution Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, nodeID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim without session before crash",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "Live execution without session",
	}); err != nil {
		t.Fatalf("claim node: %v", err)
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

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed task with live execution evidence: %v", err)
	}
	if result.TaskClaimsReleased != 0 || result.SkippedActiveExecution != 1 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result with live execution evidence: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "SKIPPED_ACTIVE_EXECUTION" {
		t.Fatalf("unexpected reclaim items with live execution evidence: %+v", result.Items)
	}
	if result.Items[0].ActiveExecutionState == "" {
		t.Fatalf("expected active execution evidence to be surfaced, got %+v", result.Items[0])
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after live-execution skip: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected claim to remain claimed when live execution exists, got %q", claimStatus)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after live-execution skip: %v", err)
	}
	if taskStatus != model.TaskStatusRunning {
		t.Fatalf("expected task to remain RUNNING when live execution exists, got %q", taskStatus)
	}
}

func TestReclaimOrphanedClaimedTasksReportsExpiredAuthorityWithoutMutation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-orphan-claim-expired"
		agentID     = "agent-sh1b-orphan-claim-expired"
		taskID      = "task-sh1b-orphan-claim-expired"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Orphan Claim Expired Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Expired Authority Orphan Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim before authority expired",
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
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_authority SET lease_expires_at = ?, updated_at = ? WHERE workspace_id = ? AND scope = ?`,
		time.Now().UTC().Add(-5*time.Minute).Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned claimed task with expired authority: %v", err)
	}
	if result.TaskClaimsReleased != 0 || result.Problems != 1 {
		t.Fatalf("unexpected expired-authority orphan reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].TaskAction != "PROBLEM" {
		t.Fatalf("expected orphan reclaim problem item for expired authority, got %+v", result.Items)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after expired-authority orphan reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected orphan claim to remain CLAIMED after expired-authority rejection, got %q", claimStatus)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after expired-authority orphan reclaim: %v", err)
	}
	if taskStatus != model.TaskStatusRunning {
		t.Fatalf("expected task to remain RUNNING after expired-authority rejection, got %q", taskStatus)
	}

	rejectEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		EntityID:    workspaceID + "/workspace",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list authority rejected runtime events: %v", err)
	}
	if len(rejectEvents) == 0 {
		t.Fatalf("expected expired authority orphan reclaim attempt to journal authority.rejected event")
	}
	if rejectEvents[0].ActorType != "system" || rejectEvents[0].ActorID == "" {
		t.Fatalf("unexpected authority rejected runtime event envelope: %+v", rejectEvents[0])
	}
}
