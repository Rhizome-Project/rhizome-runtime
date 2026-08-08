package sqlite_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestReclaimAbandonedSessionOwnershipEndsStaleSessionAndReleasesClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-session-reclaim"
		agentID     = "agent-sh1b-session-reclaim"
		sessionID   = "sess-sh1b-session-reclaim"
		taskID      = "task-sh1b-session-reclaim"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Session Reclaim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "SH1B Session Reclaim Agent",
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started work before crash",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
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
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("age task claim timestamps: %v", err)
	}

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership: %v", err)
	}
	if result.SessionsEnded != 1 || result.TaskClaimsReleased != 1 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].SessionAction != "RECLAIMED" || result.Items[0].TaskAction != "RELEASED" {
		t.Fatalf("unexpected reclaim items: %+v", result.Items)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after reclaim: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected ended session after reclaim, got %+v", state)
	}

	var claimStatus, claimSummary string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status, summary FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus, &claimSummary); err != nil {
		t.Fatalf("query task claim after reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased || claimSummary != "Local ownership reclaim after stale agent inactivity" {
		t.Fatalf("unexpected released task claim after reclaim: status=%q summary=%q", claimStatus, claimSummary)
	}

	var taskStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status after reclaim: %v", err)
	}
	if taskStatus != model.TaskStatusPending {
		t.Fatalf("expected task to return to PENDING after reclaim, got %q", taskStatus)
	}

	sessionEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventEnd,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	if sessionEvent.ActorType != "system" || sessionEvent.ActorID != "local_lease_manager" || sessionEvent.AgentID != agentID {
		t.Fatalf("unexpected session reclaim runtime event actor envelope: %+v", sessionEvent)
	}
	requireRuntimeEventAuthorityMetadataForSessionReclaim(t, store, ctx, workspaceID, model.SessionEventEnd, sessionID, authority)

	releasedEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.released",
		EntityType:  "task",
		EntityID:    taskID,
		Limit:       10,
	})
	if releasedEvent.ActorType != "system" || releasedEvent.ActorID != "local_lease_manager" || releasedEvent.AgentID != agentID {
		t.Fatalf("unexpected reclaimed task release runtime event actor envelope: %+v", releasedEvent)
	}
	requireRuntimeEventAuthorityMetadataForSessionReclaim(t, store, ctx, workspaceID, "task.released", taskID, authority)
}

func TestReclaimAbandonedSessionOwnershipReclaimsMissingAgentSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh4c-session-reclaim-missing-agent"
		agentID     = "agent-sh4c-session-reclaim-missing-agent"
		sessionID   = "sess-sh4c-session-reclaim-missing-agent"
		taskID      = "task-sh4c-session-reclaim-missing-agent"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH4C Missing Agent Session Reclaim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Missing Agent Session Reclaim",
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before missing-agent reclaim",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Blocked before missing-agent reclaim",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
		UpdatedAt:   staleAt,
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`DELETE FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("delete agent row: %v", err)
	}

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim missing-agent session ownership: %v", err)
	}
	if result.SessionsEnded != 1 || result.SessionQueuesResolved != 1 || result.TaskClaimsReleased != 0 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result: %+v", result)
	}
	if len(result.Items) != 1 {
		t.Fatalf("unexpected reclaim items: %+v", result.Items)
	}
	if item := result.Items[0]; item.SessionAction != "RECLAIMED" || item.QueueAction != "RESOLVED" || item.TaskAction != "NO_CLAIM" || item.AgentLastSeenAt != "" {
		t.Fatalf("unexpected reclaim item: %+v", item)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after reclaim: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected ended session after missing-agent reclaim, got %+v", state)
	}

	var claimCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimCount); err != nil {
		t.Fatalf("count task claims after reclaim: %v", err)
	}
	if claimCount != 0 {
		t.Fatalf("expected no task claim row after missing-agent cascade, got %d", claimCount)
	}

	var queueStatus, resolvedBy string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(resolved_by,'') FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+sessionID+":blocker",
	).Scan(&queueStatus, &resolvedBy); err != nil {
		t.Fatalf("query operator queue after reclaim: %v", err)
	}
	if queueStatus != "RESOLVED" || resolvedBy != "local_lease_manager" {
		t.Fatalf("unexpected operator queue after missing-agent reclaim: status=%q resolved_by=%q", queueStatus, resolvedBy)
	}

	sessionEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventEnd,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	if sessionEvent.ActorType != "system" || sessionEvent.ActorID != "local_lease_manager" || sessionEvent.AgentID != agentID {
		t.Fatalf("unexpected session reclaim runtime event actor envelope: %+v", sessionEvent)
	}
	requireRuntimeEventAuthorityMetadataForSessionReclaim(t, store, ctx, workspaceID, model.SessionEventEnd, sessionID, authority)
}

func requireRuntimeEventAuthorityMetadataForSessionReclaim(t *testing.T, store *sqlite.Store, ctx context.Context, workspaceID, eventType, entityID string, authority sqlite.WorkspaceAuthorityRecord) {
	t.Helper()

	var holderNodeID, leaseFingerprint string
	var authorityTerm int64
	if err := store.DB().QueryRowContext(ctx, `
		SELECT authority_holder_node_id, authority_term, authority_lease_token_fingerprint
		  FROM runtime_events
		 WHERE workspace_id = ? AND event_type = ? AND entity_id = ?
		 ORDER BY COALESCE(ingest_seq,0) DESC, created_at DESC, event_id DESC
		 LIMIT 1`,
		workspaceID,
		eventType,
		entityID,
	).Scan(&holderNodeID, &authorityTerm, &leaseFingerprint); err != nil {
		t.Fatalf("query %s authority metadata for %s: %v", eventType, entityID, err)
	}
	if holderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("%s authority holder = %s, want %s", eventType, holderNodeID, authority.HolderAuthorityNodeID)
	}
	if authorityTerm != authority.Term {
		t.Fatalf("%s authority term = %d, want %d", eventType, authorityTerm, authority.Term)
	}
	if leaseFingerprint == "" {
		t.Fatalf("%s authority lease token fingerprint is empty", eventType)
	}
}

func TestReclaimAbandonedSessionOwnershipSkipsRecentSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-session-reclaim-recent"
		agentID     = "agent-sh1b-session-reclaim-recent"
		sessionID   = "sess-sh1b-session-reclaim-recent"
		taskID      = "task-sh1b-session-reclaim-recent"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Session Reclaim Recent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Recent Agent",
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
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Fresh active session",
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	if err := store.TouchAgentActivity(ctx, workspaceID, agentID); err != nil {
		t.Fatalf("touch agent activity: %v", err)
	}

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim recent session ownership: %v", err)
	}
	if result.SessionsEnded != 0 || result.TaskClaimsReleased != 0 || result.SkippedRecent != 1 || result.Problems != 0 {
		t.Fatalf("unexpected recent reclaim result: %+v", result)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get recent session state: %v", err)
	}
	if state.Status != model.SessionStatusActive {
		t.Fatalf("expected recent session to stay active, got %+v", state)
	}
}

func TestReclaimAbandonedSessionOwnershipKeepsHeartbeatWindowSessionAlive(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-session-reclaim-heartbeat-window"
		agentID     = "agent-sh1b-session-reclaim-heartbeat-window"
		sessionID   = "sess-sh1b-session-reclaim-heartbeat-window"
		taskID      = "task-sh1b-session-reclaim-heartbeat-window"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Session Reclaim Heartbeat Window",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Heartbeat Window Agent",
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

	heartbeatWindowAt := time.Now().UTC().Add(-(2*time.Minute + 15*time.Second)).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Live session still inside heartbeat window",
		UpdatedAt:   heartbeatWindowAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		heartbeatWindowAt,
		heartbeatWindowAt,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("age agent last_seen_at: %v", err)
	}

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim heartbeat-window session ownership: %v", err)
	}
	if result.SessionsEnded != 0 || result.TaskClaimsReleased != 0 || result.SkippedRecent != 1 || result.Problems != 0 {
		t.Fatalf("unexpected heartbeat-window reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].SessionAction != "SKIPPED_RECENT" || result.Items[0].TaskAction != "SKIPPED_RECENT" {
		t.Fatalf("unexpected heartbeat-window reclaim items: %+v", result.Items)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get heartbeat-window session state: %v", err)
	}
	if state.Status != model.SessionStatusActive {
		t.Fatalf("expected heartbeat-window session to stay active, got %+v", state)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after heartbeat-window reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected heartbeat-window task claim to stay CLAIMED, got %q", claimStatus)
	}
}

func TestReclaimAbandonedSessionOwnershipSkipsRevivedSessionBeforeReclaimBoundary(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh3c-stale-lease-revived"
		agentID     = "agent-sh3c-stale-lease-revived"
		sessionID   = "sess-sh3c-stale-lease-revived"
		taskID      = "task-sh3c-stale-lease-revived"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Revived Session Reclaim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Revived Session Agent",
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before stale reclaim window",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Blocked before reconnect",
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

	queueKey := "session:" + sessionID + ":blocker"
	var queueID string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		queueKey,
	).Scan(&queueID); err != nil {
		t.Fatalf("lookup blocked operator queue: %v", err)
	}

	freshAt := time.Now().UTC().Format(time.RFC3339Nano)
	revivedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Blocked but alive again",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
		UpdatedAt:   freshAt,
	})
	if err != nil {
		t.Fatalf("record revived blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, revivedState); err != nil {
		t.Fatalf("sync revived blocked session operator queue: %v", err)
	}
	if err := store.TouchAgentActivity(ctx, workspaceID, agentID); err != nil {
		t.Fatalf("touch agent activity: %v", err)
	}

	beforeSessionEndEvents := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID)
	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "task.released", taskID)
	beforeQueueResolvedEvents := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "operator_queue.resolved", queueID)

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim revived session ownership: %v", err)
	}
	if result.SessionsEnded != 0 || result.TaskClaimsReleased != 0 || result.SessionQueuesResolved != 0 || result.SkippedRecent != 1 || result.Problems != 0 {
		t.Fatalf("unexpected revived reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].SessionAction != "SKIPPED_RECENT" || result.Items[0].TaskAction != "SKIPPED_RECENT" {
		t.Fatalf("unexpected revived reclaim items: %+v", result.Items)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get revived session state: %v", err)
	}
	if state.Status != model.SessionStatusBlocked {
		t.Fatalf("expected revived blocked session to stay blocked, got %+v", state)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after revived reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected task claim to stay CLAIMED after revived reclaim, got %q", claimStatus)
	}

	var queueStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status FROM operator_queue_items WHERE workspace_id = ? AND queue_id = ?`,
		workspaceID,
		queueID,
	).Scan(&queueStatus); err != nil {
		t.Fatalf("query operator queue after revived reclaim: %v", err)
	}
	if queueStatus != "OPEN" {
		t.Fatalf("expected blocked operator queue to stay OPEN after revived reclaim, got %q", queueStatus)
	}

	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID); got != beforeSessionEndEvents {
		t.Fatalf("expected no session.end after revived reclaim skip, before=%d after=%d", beforeSessionEndEvents, got)
	}
	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no task.released after revived reclaim skip, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "operator_queue.resolved", queueID); got != beforeQueueResolvedEvents {
		t.Fatalf("expected no operator_queue.resolved after revived reclaim skip, before=%d after=%d", beforeQueueResolvedEvents, got)
	}
}

func TestReclaimAbandonedSessionOwnershipResolvesSessionDerivedOperatorQueues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		queueType string
		apply     func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sessionID, agentID, taskID, updatedAt string) sqlite.AgentSessionStateRecord
	}{
		{
			queueType: "BLOCKER",
			apply: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sessionID, agentID, taskID, updatedAt string) sqlite.AgentSessionStateRecord {
				t.Helper()
				state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventBlocked,
					WorkspaceID: workspaceID,
					SessionID:   sessionID,
					AgentID:     agentID,
					TaskID:      taskID,
					Summary:     "Blocked before crash",
					BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
					UpdatedAt:   updatedAt,
				})
				if err != nil {
					t.Fatalf("record blocked session: %v", err)
				}
				return state
			},
		},
		{
			queueType: "DECISION",
			apply: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sessionID, agentID, taskID, updatedAt string) sqlite.AgentSessionStateRecord {
				t.Helper()
				state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
					EventType:          model.SessionEventDecisionNeeded,
					WorkspaceID:        workspaceID,
					SessionID:          sessionID,
					AgentID:            agentID,
					TaskID:             taskID,
					Summary:            "Decision needed before crash",
					DecisionNeededFrom: "developer",
					DecisionType:       "approval",
					UpdatedAt:          updatedAt,
				})
				if err != nil {
					t.Fatalf("record decision-needed session: %v", err)
				}
				return state
			},
		},
		{
			queueType: "HANDOFF",
			apply: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sessionID, agentID, taskID, updatedAt string) sqlite.AgentSessionStateRecord {
				t.Helper()
				if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
					WorkspaceID: workspaceID,
					AgentID:     "agent-target",
					OwnerUserID: "developer",
					DisplayName: "Target Agent",
				}); err != nil {
					t.Fatalf("register handoff target: %v", err)
				}
				state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventStatus,
					WorkspaceID: workspaceID,
					SessionID:   sessionID,
					AgentID:     agentID,
					TaskID:      taskID,
					Summary:     "Handoff pending before crash",
					Status:      model.SessionStatusHandoffPending,
					HandoffTo:   "agent-target",
					UpdatedAt:   updatedAt,
				})
				if err != nil {
					t.Fatalf("record handoff-pending session: %v", err)
				}
				return state
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.queueType, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()

			workspaceID := "ws-sh1b-session-reclaim-queue-" + strings.ToLower(tc.queueType)
			agentID := "agent-sh1b-session-reclaim-queue-" + strings.ToLower(tc.queueType)
			sessionID := "sess-sh1b-session-reclaim-queue-" + strings.ToLower(tc.queueType)
			taskID := "task-sh1b-session-reclaim-queue-" + strings.ToLower(tc.queueType)

			if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
				WorkspaceID: workspaceID,
				Title:       "SH1B Session Queue Reclaim " + tc.queueType,
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			authority := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
			if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
				WorkspaceID: workspaceID,
				AgentID:     agentID,
				OwnerUserID: "developer",
				DisplayName: "Queue Reclaim Agent",
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

			staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
			claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
			if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
				EventType:   model.SessionEventStart,
				WorkspaceID: workspaceID,
				SessionID:   sessionID,
				AgentID:     agentID,
				TaskID:      taskID,
				Summary:     "Started before crash",
				UpdatedAt:   staleAt,
			}); err != nil {
				t.Fatalf("record session start: %v", err)
			}
			state := tc.apply(t, ctx, store, workspaceID, sessionID, agentID, taskID, staleAt)
			syncResult, err := store.SyncOperatorQueueFromSessionState(ctx, state)
			if err != nil {
				t.Fatalf("sync operator queue from session state: %v", err)
			}
			if len(syncResult.Opened) != 1 {
				t.Fatalf("expected one opened operator queue, got %+v", syncResult)
			}

			queueKey := fmt.Sprintf("session:%s:%s", sessionID, strings.ToLower(tc.queueType))
			var queueID string
			if err := store.DB().QueryRowContext(ctx,
				`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
				workspaceID,
				queueKey,
			).Scan(&queueID); err != nil {
				t.Fatalf("lookup opened operator queue: %v", err)
			}

			if _, err := store.DB().ExecContext(ctx, `DELETE FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID); err != nil {
				t.Fatalf("remove task claim fixture for queue-only reclaim: %v", err)
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

			result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
				Scope:       "workspace",
				ActorType:   "system",
				ActorID:     "local_reclaim_manager",
				ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
			})
			if err != nil {
				t.Fatalf("reclaim abandoned session ownership: %v", err)
			}
			if result.SessionsEnded != 1 || result.SessionQueuesResolved != 1 || result.TaskClaimsReleased != 0 || result.Problems != 0 {
				t.Fatalf("unexpected reclaim result: %+v", result)
			}
			if len(result.Items) != 1 || result.Items[0].QueueAction != "RESOLVED" || result.Items[0].TaskAction != "NO_CLAIM" {
				t.Fatalf("unexpected reclaim items: %+v", result.Items)
			}

			var status, resolvedBy, resolution string
			if err := store.DB().QueryRowContext(ctx,
				`SELECT status, COALESCE(resolved_by,''), COALESCE(resolution,'') FROM operator_queue_items WHERE workspace_id = ? AND queue_id = ?`,
				workspaceID,
				queueID,
			).Scan(&status, &resolvedBy, &resolution); err != nil {
				t.Fatalf("query resolved operator queue: %v", err)
			}
			if status != "RESOLVED" || resolvedBy != "local_reclaim_manager" || resolution != "cleared_by_session_end" {
				t.Fatalf("unexpected resolved operator queue state: status=%q resolved_by=%q resolution=%q", status, resolvedBy, resolution)
			}

			queueEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.resolved",
				EntityType:  "operator_queue",
				EntityID:    queueID,
				Limit:       10,
			})
			if queueEvent.ActorType != "operator" || queueEvent.ActorID != "local_reclaim_manager" {
				t.Fatalf("unexpected operator queue resolve runtime event actor envelope: %+v", queueEvent)
			}
			requireRuntimeEventAuthorityMetadataForSessionReclaim(t, store, ctx, workspaceID, "operator_queue.resolved", queueID, authority)
		})
	}
}

func TestReclaimAbandonedSessionOwnershipLeavesCollidingManualQueueOpen(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-session-reclaim-colliding-queue"
		agentID     = "agent-sh1b-session-reclaim-colliding-queue"
		sessionID   = "sess-sh1b-session-reclaim-colliding-queue"
		taskID      = "task-sh1b-session-reclaim-colliding-queue"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Session Reclaim Colliding Queue",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Colliding Queue Agent",
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before crash",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID); err != nil {
		t.Fatalf("remove task claim fixture for colliding-queue reclaim: %v", err)
	}
	queueKey := "session:" + sessionID + ":blocker"
	queue := sqlite.OperatorQueueRecord{
		QueueID:     "opq-sh1b-colliding-manual",
		WorkspaceID: workspaceID,
		QueueKey:    queueKey,
		QueueType:   "BLOCKER",
		Status:      "OPEN",
		Title:       "Manual queue with colliding key",
		Summary:     "Should remain open after stale session reclaim",
		Urgency:     "NORMAL",
		SourceKind:  "manual",
		SourceID:    "tests",
		TaskID:      taskID,
		SessionID:   sessionID,
		AgentID:     agentID,
		CreatedAt:   staleAt,
		UpdatedAt:   staleAt,
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO operator_queue_items(
			queue_id, workspace_id, queue_key, queue_type, status, title, summary, details,
			payload_json, assigned_to, urgency, source_kind, source_id, task_id, session_id, agent_id,
			keep_session_active, due_at, resolution, resolved_at, resolved_by,
			escalation_count, last_escalated_at, last_escalated_by, escalation_reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, '', '', '', ?, ?, ?, ?, ?, ?, 0, NULL, '', NULL, NULL, 0, NULL, NULL, '', ?, ?)`,
		queue.QueueID,
		queue.WorkspaceID,
		queue.QueueKey,
		queue.QueueType,
		queue.Status,
		queue.Title,
		queue.Summary,
		queue.Urgency,
		queue.SourceKind,
		queue.SourceID,
		queue.TaskID,
		queue.SessionID,
		queue.AgentID,
		queue.CreatedAt,
		queue.UpdatedAt,
	); err != nil {
		t.Fatalf("insert colliding manual queue fixture: %v", err)
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

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership: %v", err)
	}
	if result.SessionsEnded != 1 || result.SessionQueuesResolved != 0 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].QueueAction != "NO_QUEUE" {
		t.Fatalf("unexpected reclaim items: %+v", result.Items)
	}

	record, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get colliding manual queue after reclaim: %v", err)
	}
	if record.Status != "OPEN" || record.ResolvedBy != nil {
		t.Fatalf("expected colliding manual queue to remain open, got %+v", record)
	}
	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "operator_queue.resolved", queue.QueueID); got != 0 {
		t.Fatalf("expected no operator_queue.resolved for colliding manual queue, got %d", got)
	}
}

func TestReclaimAbandonedSessionOwnershipReportsExpiredAuthorityWithoutMutation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-session-reclaim-expired"
		agentID     = "agent-sh1b-session-reclaim-expired"
		sessionID   = "sess-sh1b-session-reclaim-expired"
		taskID      = "task-sh1b-session-reclaim-expired"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Session Reclaim Expired",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Expired Authority Agent",
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before authority expired",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
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
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_authority SET lease_expires_at = ?, updated_at = ? WHERE workspace_id = ? AND scope = ?`,
		time.Now().UTC().Add(-5*time.Minute).Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim with expired authority: %v", err)
	}
	if result.SessionsEnded != 0 || result.TaskClaimsReleased != 0 || result.Problems != 1 {
		t.Fatalf("unexpected expired-authority reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].SessionAction != "PROBLEM" {
		t.Fatalf("expected reclaim problem item for expired authority, got %+v", result.Items)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after expired-authority reclaim: %v", err)
	}
	if state.Status != model.SessionStatusActive {
		t.Fatalf("expected session to remain active after expired-authority rejection, got %+v", state)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after expired-authority reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected task claim to remain CLAIMED after expired-authority rejection, got %q", claimStatus)
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
		t.Fatalf("expected expired authority reclaim attempt to journal authority.rejected event")
	}
	if rejectEvents[0].ActorType != "system" || rejectEvents[0].ActorID == "" {
		t.Fatalf("unexpected authority rejected runtime event envelope: %+v", rejectEvents[0])
	}
}

func TestReclaimAbandonedSessionOwnershipLeavesDifferentAgentClaimUntouched(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh1b-session-reclaim-other-claim-owner"
		sessionAgent = "agent-sh1b-session-reclaim-source"
		claimAgent   = "agent-sh1b-session-reclaim-claim-owner"
		sessionID    = "sess-sh1b-session-reclaim-other-claim-owner"
		taskID       = "task-sh1b-session-reclaim-other-claim-owner"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Session Reclaim Other Claim Owner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{sessionAgent, claimAgent} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: "SH1B Agent " + agentID,
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     sessionAgent,
		Summary:     "Stale source session",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     claimAgent,
		Summary:     "Claim owned by another active line",
	}); err != nil {
		t.Fatalf("claim task with other agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		sessionAgent,
	); err != nil {
		t.Fatalf("age source agent last_seen_at: %v", err)
	}
	if err := store.TouchAgentActivity(ctx, workspaceID, claimAgent); err != nil {
		t.Fatalf("touch claim agent activity: %v", err)
	}

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership: %v", err)
	}
	if result.SessionsEnded != 1 || result.TaskClaimsReleased != 0 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result: %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].SessionAction != "RECLAIMED" || result.Items[0].TaskAction != "NO_TASK" {
		t.Fatalf("unexpected reclaim items: %+v", result.Items)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after reclaim: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected stale session to end, got %+v", state)
	}

	var claimOwner, claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimOwner, &claimStatus); err != nil {
		t.Fatalf("query task claim after reclaim: %v", err)
	}
	if claimOwner != claimAgent || claimStatus != model.TaskClaimStatusClaimed {
		t.Fatalf("expected other agent claim to stay intact, got owner=%q status=%q", claimOwner, claimStatus)
	}
}

func TestReclaimAbandonedSessionOwnershipSkipsClaimReleaseWhenAnotherSessionIsActiveOnTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-sh3c-stale-lease-active-task-session"
		staleAgentID = "agent-sh3c-stale-lease-stale"
		freshAgentID = "agent-sh3c-stale-lease-fresh"
		staleSession = "sess-sh3c-stale-lease-stale"
		freshSession = "sess-sh3c-stale-lease-fresh"
		taskID       = "task-sh3c-stale-lease-active-task-session"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH3C Stale Lease Active Task Session",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{staleAgentID, freshAgentID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: "SH3C Agent " + agentID,
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, staleAgentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   staleSession,
		AgentID:     staleAgentID,
		TaskID:      taskID,
		Summary:     "Stale session before crash",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record stale session start: %v", err)
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

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		SessionID:   freshSession,
		AgentID:     freshAgentID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("insert receiptless foreign session fixture: %v", err)
	}
	if err := store.TouchAgentActivity(ctx, workspaceID, freshAgentID); err != nil {
		t.Fatalf("touch fresh agent activity: %v", err)
	}

	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "task.released", taskID)

	result, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim abandoned session ownership: %v", err)
	}
	if result.SessionsEnded != 1 || result.TaskClaimsReleased != 1 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result: %+v", result)
	}

	var staleItem *sqlite.LocalSessionOwnershipReclaimItem
	for i := range result.Items {
		if result.Items[i].SessionID == staleSession {
			staleItem = &result.Items[i]
			break
		}
	}
	if staleItem == nil {
		t.Fatalf("missing stale session reclaim item in %+v", result.Items)
	}
	if staleItem.SessionAction != "RECLAIMED" || staleItem.TaskAction != "RELEASED" {
		t.Fatalf("unexpected stale session reclaim item: %+v", *staleItem)
	}

	staleState, err := store.GetAgentSessionState(ctx, workspaceID, staleSession)
	if err != nil {
		t.Fatalf("get stale session state after reclaim: %v", err)
	}
	if staleState.Status != model.SessionStatusEnded {
		t.Fatalf("expected stale session to end, got %+v", staleState)
	}
	freshState, err := store.GetAgentSessionState(ctx, workspaceID, freshSession)
	if err != nil {
		t.Fatalf("get fresh session state after reclaim: %v", err)
	}
	if !model.IsSessionStatusActive(freshState.Status) {
		t.Fatalf("expected fresh session to stay active, got %+v", freshState)
	}

	var claimOwner, claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT agent_id, claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimOwner, &claimStatus); err != nil {
		t.Fatalf("query task claim after reclaim: %v", err)
	}
	if claimOwner != staleAgentID || claimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("expected stale claim to release despite receiptless foreign session, got owner=%q status=%q", claimOwner, claimStatus)
	}

	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents+1 {
		t.Fatalf("expected one task.released when only receiptless foreign session exists, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
}

func TestReclaimAbandonedSessionOwnershipIsIdempotentAfterInitialReclaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-session-reclaim-idempotent"
		agentID     = "agent-sh1b-session-reclaim-idempotent"
		sessionID   = "sess-sh1b-session-reclaim-idempotent"
		taskID      = "task-sh1b-session-reclaim-idempotent"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Session Reclaim Idempotent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Idempotent Agent",
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

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Stale session for idempotency",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Blocked before reclaim",
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

	first, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("first reclaim: %v", err)
	}
	if first.SessionsEnded != 1 || first.TaskClaimsReleased != 1 || first.Problems != 0 {
		t.Fatalf("unexpected first reclaim result: %+v", first)
	}

	beforeSessionEndEvents := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID)
	beforeTaskReleasedEvents := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "task.released", taskID)
	queueKey := "session:" + sessionID + ":blocker"
	var queueID string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		queueKey,
	).Scan(&queueID); err != nil {
		t.Fatalf("lookup blocked operator queue: %v", err)
	}
	beforeQueueResolvedEvents := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "operator_queue.resolved", queueID)

	second, err := store.ReclaimAbandonedSessionOwnership(ctx, sqlite.LocalSessionOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_reclaim_manager",
		ReferenceAt: time.Now().UTC().Add(30 * time.Second).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("second reclaim: %v", err)
	}
	if second.SessionsEnded != 0 || second.TaskClaimsReleased != 0 || second.SkippedRecent != 0 || second.Problems != 0 {
		t.Fatalf("unexpected second reclaim result: %+v", second)
	}
	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID); got != beforeSessionEndEvents {
		t.Fatalf("expected no duplicate session.end after second reclaim, before=%d after=%d", beforeSessionEndEvents, got)
	}
	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no duplicate task.released after second reclaim, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
	if got := countRuntimeEventsByEntityAndType(t, ctx, store, workspaceID, "operator_queue.resolved", queueID); got != beforeQueueResolvedEvents {
		t.Fatalf("expected no duplicate operator_queue.resolved after second reclaim, before=%d after=%d", beforeQueueResolvedEvents, got)
	}
}

func countRuntimeEventsByEntityAndType(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType, entityID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = ? AND entity_id = ?`,
		workspaceID,
		eventType,
		entityID,
	).Scan(&count); err != nil {
		t.Fatalf("count runtime events %s/%s: %v", eventType, entityID, err)
	}
	return count
}
