package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRunLocalAuthorityLeaseMaintenanceOnceReclaimsStaleSessionOwnership(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-authority-lease-reclaim"
		agentID     = "agent-authority-lease-reclaim"
		sessionID   = "sess-authority-lease-reclaim"
		taskID      = "task-authority-lease-reclaim"
	)

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Reclaim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Authority Lease Reclaim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Authority Lease Reclaim Task",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
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
		Summary:     "Claim before restart",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before restart",
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
		Summary:     "Blocked before restart",
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

	if err := runLocalAuthorityLeaseMaintenanceOnce(ctx, store, nil); err != nil {
		t.Fatalf("runLocalAuthorityLeaseMaintenanceOnce failed: %v", err)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after lease maintenance: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected lease maintenance reclaim to end stale session, got %+v", state)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after lease maintenance reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("expected lease maintenance reclaim to release task claim, got %q", claimStatus)
	}
	var queueStatus, resolvedBy string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(resolved_by,'') FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+sessionID+":blocker",
	).Scan(&queueStatus, &resolvedBy); err != nil {
		t.Fatalf("query operator queue after lease maintenance reclaim: %v", err)
	}
	if queueStatus != "RESOLVED" || resolvedBy != "local_lease_manager" {
		t.Fatalf("expected lease maintenance reclaim to resolve blocked operator queue, got status=%q resolved_by=%q", queueStatus, resolvedBy)
	}
}

func TestRunLocalAuthorityLeaseMaintenanceOnceIsIdempotentAfterReclaim(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-authority-lease-reclaim-idempotent"
		agentID     = "agent-authority-lease-reclaim-idempotent"
		sessionID   = "sess-authority-lease-reclaim-idempotent"
		taskID      = "task-authority-lease-reclaim-idempotent"
	)

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Reclaim Idempotent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Authority Lease Reclaim Idempotent Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Authority Lease Reclaim Idempotent Task",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
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
		Summary:     "Claim before restart",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before restart",
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
		Summary:     "Blocked before restart",
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

	if err := runLocalAuthorityLeaseMaintenanceOnce(ctx, store, nil); err != nil {
		t.Fatalf("first runLocalAuthorityLeaseMaintenanceOnce failed: %v", err)
	}

	beforeSessionEndEvents := mustCountRuntimeEvents(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID)
	beforeTaskReleasedEvents := mustCountRuntimeEvents(t, ctx, store, workspaceID, "task.released", taskID)
	var queueID string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT queue_id FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+sessionID+":blocker",
	).Scan(&queueID); err != nil {
		t.Fatalf("lookup blocked operator queue: %v", err)
	}
	beforeQueueResolvedEvents := mustCountRuntimeEvents(t, ctx, store, workspaceID, "operator_queue.resolved", queueID)

	if err := runLocalAuthorityLeaseMaintenanceOnce(ctx, store, nil); err != nil {
		t.Fatalf("second runLocalAuthorityLeaseMaintenanceOnce failed: %v", err)
	}

	if got := mustCountRuntimeEvents(t, ctx, store, workspaceID, model.SessionEventEnd, sessionID); got != beforeSessionEndEvents {
		t.Fatalf("expected no duplicate session.end event on second lease maintenance pass, before=%d after=%d", beforeSessionEndEvents, got)
	}
	if got := mustCountRuntimeEvents(t, ctx, store, workspaceID, "task.released", taskID); got != beforeTaskReleasedEvents {
		t.Fatalf("expected no duplicate task.released event on second lease maintenance pass, before=%d after=%d", beforeTaskReleasedEvents, got)
	}
	if got := mustCountRuntimeEvents(t, ctx, store, workspaceID, "operator_queue.resolved", queueID); got != beforeQueueResolvedEvents {
		t.Fatalf("expected no duplicate operator_queue.resolved event on second lease maintenance pass, before=%d after=%d", beforeQueueResolvedEvents, got)
	}
}

func TestRunLocalAuthorityLeaseMaintenanceOnceReclaimsStaleClaimWithoutSession(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-authority-lease-orphan-claim"
		agentID     = "agent-authority-lease-orphan-claim"
		taskID      = "task-authority-lease-orphan-claim"
	)

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Orphan Claim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Authority Lease Orphan Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Authority Lease Orphan Claim Task",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
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
		Summary:     "Claim before crash without session",
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

	if err := runLocalAuthorityLeaseMaintenanceOnce(ctx, store, nil); err != nil {
		t.Fatalf("runLocalAuthorityLeaseMaintenanceOnce failed: %v", err)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after lease maintenance orphan reclaim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("expected lease maintenance orphan reclaim to release task claim, got %q", claimStatus)
	}
}

func TestRunLocalAuthorityLeaseMaintenanceOnceIgnoresMissingAgentClaimAfterCascade(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-authority-lease-orphan-claim-missing-agent"
		agentID     = "agent-authority-lease-orphan-claim-missing-agent"
		taskID      = "task-authority-lease-orphan-claim-missing-agent"
	)

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Orphan Claim Missing Agent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Authority Lease Orphan Claim Missing Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Authority Lease Orphan Claim Missing Agent Task",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
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
		Summary:     "Claim before crash without session",
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

	if err := runLocalAuthorityLeaseMaintenanceOnce(ctx, store, nil); err != nil {
		t.Fatalf("runLocalAuthorityLeaseMaintenanceOnce failed: %v", err)
	}

	var claimCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(1) FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimCount); err != nil {
		t.Fatalf("count task claims after lease maintenance orphan reclaim with missing agent row: %v", err)
	}
	if claimCount != 0 {
		t.Fatalf("expected missing-agent cascade to remove orphan claim before lease reclaim, got %d row(s)", claimCount)
	}
}

func TestRunLocalAuthorityLeaseMaintenancePassRunsAuthorityHandoffWatchdog(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-authority-handoff-loop"
		projectID   = "project-authority-handoff-loop"
		agentID     = "theta"
		taskID      = "task-role-scope-authority-handoff-loop"
		updateID    = "update-authority-handoff-loop"
	)

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	now := time.Now().UTC()
	staleAt := now.Add(-20 * time.Minute).Format(time.RFC3339Nano)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Handoff Loop",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO projects(project_id, workspace_id, title, description, status, created_by, created_at, updated_at)
VALUES (?, ?, ?, '', 'ACTIVE', 'developer', ?, ?)`,
		projectID, workspaceID, projectID, staleAt, staleAt,
	); err != nil {
		t.Fatalf("insert project: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Theta",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "high",
		Title:       "Authority Handoff Carrier",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE tasks SET status = ?, task_kind = 'COORDINATION', task_template = 'generic', project_id = ?, requires_project_gate = 1, tags_json = '["project-role-scope"]', updated_at = ? WHERE task_id = ?`,
		model.TaskStatusRunning,
		projectID,
		staleAt,
		taskID,
	); err != nil {
		t.Fatalf("mark task as authority carrier: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, project_role_id, repo_id, checkout_id, branch_id, write_scope_json, updated_at)
VALUES (?, ?, ?, ?, 'previously admitted authority handoff', ?, ?, '', '', '', '', '', ?)`,
		taskID,
		workspaceID,
		agentID,
		model.TaskClaimStatusReleased,
		staleAt,
		staleAt,
		staleAt,
	); err != nil {
		t.Fatalf("insert released carrier claim: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"delegation_state": "claim_admitted",
		"request_id":       "req-" + updateID,
		"from_agent_id":    "alpha",
		"to_agent_id":      agentID,
		"task_id":          taskID,
		"request_kind":     "authority_transition",
		"coverage_state":   "authority_task_claim_admitted",
	})
	if err != nil {
		t.Fatalf("marshal handoff payload: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
VALUES (?, ?, ?, 'coordination', 'authority transition claim_admitted', ?, 0, ?)`,
		updateID,
		workspaceID,
		agentID,
		string(payload),
		staleAt,
	); err != nil {
		t.Fatalf("insert stale handoff signal: %v", err)
	}

	pass, err := runLocalAuthorityLeaseMaintenancePass(ctx, store)
	if err != nil {
		t.Fatalf("runLocalAuthorityLeaseMaintenancePass failed: %v", err)
	}
	if pass.AuthorityHandoff.Changed != 1 || pass.AuthorityHandoff.Problems != 0 {
		t.Fatalf("expected authority handoff watchdog to block one carrier, got %+v", pass.AuthorityHandoff)
	}
	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query authority handoff claim: %v", err)
	}
	if claimStatus != model.TaskClaimStatusBlocked {
		t.Fatalf("expected authority handoff watchdog to block carrier, got %q", claimStatus)
	}
}

func TestRunLocalAuthorityLeaseMaintenancePassResolvesTerminalSessionOperatorQueue(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-authority-lease-terminal-session-oq"
		agentID     = "agent-authority-lease-terminal-session-oq"
		sessionID   = "sess-authority-lease-terminal-session-oq"
	)

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Terminal Session Operator Queue",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Terminal Session Operator Queue Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "session starts before external gate opens",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	keepInactive := false
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventEnd,
		WorkspaceID:       workspaceID,
		SessionID:         sessionID,
		AgentID:           agentID,
		Summary:           "session ended before maintenance",
		KeepSessionActive: &keepInactive,
	}); err != nil {
		t.Fatalf("record session end: %v", err)
	}
	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "external_gate:credential_auth:lease-loop-terminal-session",
		QueueType:         "BLOCKER",
		Title:             "Complete credential authorization",
		Summary:           "session-owned external gate left open after stop",
		Urgency:           "NORMAL",
		SourceKind:        "session",
		SourceID:          sessionID,
		SessionID:         sessionID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert terminal session queue: %v", err)
	}

	pass, err := runLocalAuthorityLeaseMaintenancePass(ctx, store)
	if err != nil {
		t.Fatalf("runLocalAuthorityLeaseMaintenancePass failed: %v", err)
	}
	if pass.TerminalSessionOQ.Resolved != 1 || pass.TerminalSessionOQ.Problems != 0 {
		t.Fatalf("expected terminal session operator queue reconcile to resolve one queue, got %+v", pass.TerminalSessionOQ)
	}

	resolved, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved terminal session queue: %v", err)
	}
	if resolved.Status != "RESOLVED" || resolved.Resolution != "cleared_by_terminal_session_reconcile" {
		t.Fatalf("unexpected resolved terminal session queue: %+v", resolved)
	}
}

func mustCountRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType, entityID string) int {
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
