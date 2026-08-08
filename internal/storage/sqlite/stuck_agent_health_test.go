package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestCurrentStuckAgentSnapshotMarksDeadHeartbeatsAndStaleActivityDegraded(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-health"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Health",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-offline",
		DisplayName:       "Offline Agent",
	}); err != nil {
		t.Fatalf("register offline agent: %v", err)
	}

	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-offline-agent",
		AgentID:     "agent-offline",
		WorkspaceID: workspaceID,
		TaskID:      "task-offline-agent",
		StartedAt:   time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create offline-agent session: %v", err)
	}
	recentHeartbeatlessUpdateAt := time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
VALUES (?, ?, ?, ?, ?, ?, 0, ?)`,
		"upd-offline-heartbeatless",
		workspaceID,
		"agent-offline",
		model.SessionEventStatus,
		"recent status update without heartbeat",
		`{"session_id":"sess-offline-agent"}`,
		recentHeartbeatlessUpdateAt,
	); err != nil {
		t.Fatalf("insert offline-agent session update: %v", err)
	}

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-stale-activity",
		DisplayName:       "Stale Activity Agent",
	}); err != nil {
		t.Fatalf("register stale-activity agent: %v", err)
	}
	if err := store.RecordAgentHeartbeat(ctx, AgentHeartbeatInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-stale-activity",
		Status:      "active",
		Summary:     "alive but idle",
	}); err != nil {
		t.Fatalf("record stale-activity heartbeat: %v", err)
	}
	staleActivityAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-stale-activity",
		AgentID:     "agent-stale-activity",
		TaskID:      "",
		Summary:     "stale activity session",
		Status:      model.SessionStatusActive,
		UpdatedAt:   staleActivityAt,
	}); err != nil {
		t.Fatalf("record stale-activity session coordination: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded stuck-agent snapshot, got %+v", snapshot)
	}
	if snapshot.ActiveSessionCount != 2 {
		t.Fatalf("expected two active sessions, got %+v", snapshot)
	}
	if snapshot.DeadHeartbeatSessionCount != 1 {
		t.Fatalf("expected one dead-heartbeat session, got %+v", snapshot)
	}
	if snapshot.StaleActivitySessionCount != 1 {
		t.Fatalf("expected one stale-activity session, got %+v", snapshot)
	}
	if snapshot.OfflineAgentSessionCount != 1 {
		t.Fatalf("expected one offline-agent session, got %+v", snapshot)
	}
	if snapshot.MissingAgentSessionCount != 0 {
		t.Fatalf("expected zero missing-agent sessions in this scenario, got %+v", snapshot)
	}
	if snapshot.OldestAffectedStartedAt == "" {
		t.Fatalf("expected oldest affected started_at to be populated, got %+v", snapshot)
	}
	if snapshot.OldestStaleActivityAt == "" {
		t.Fatalf("expected oldest stale activity timestamp to be populated, got %+v", snapshot)
	}
	if snapshot.Message == "" || !strings.Contains(snapshot.Message, "dead_heartbeats=1") || !strings.Contains(snapshot.Message, "stale_activity=1") {
		t.Fatalf("expected stuck-agent message to mention dead heartbeats and stale activity, got %+v", snapshot)
	}
}

func TestCurrentStuckAgentSnapshotIncludesRecoveryManifestFromDurableState(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-recovery-manifest"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Recovery Manifest",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-recovery",
		DisplayName:       "Recovery Agent",
	}); err != nil {
		t.Fatalf("register recovery agent: %v", err)
	}

	sessionUpdatedAt := time.Now().UTC().Add(-15 * time.Minute).Format(time.RFC3339Nano)
	state, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-recovery",
		AgentID:     "agent-recovery",
		Summary:     "recovery candidate session",
		Status:      model.SessionStatusActive,
		UpdatedAt:   sessionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("record recovery coordination: %v", err)
	}
	if err := store.SyncExecutionRunFromSessionState(ctx, state); err != nil {
		t.Fatalf("sync recovery execution run: %v", err)
	}
	runID := sessionExecutionRunID(state.SessionID)
	if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      state.TaskID,
		SessionID:   state.SessionID,
		AgentID:     state.AgentID,
		Title:       "Open ledger operation",
		Summary:     "operation still running",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"schema":         "operation_ledger.v1",
				"operation_id":   "op-recovery-open-1",
				"operation_name": "fake-ledger-tool",
				"operation_kind": "tool_call",
				"status":         "running",
				"terminal":       false,
				"updated_at":     sessionUpdatedAt,
				"binding": map[string]any{
					"run_id":     runID,
					"task_id":    state.TaskID,
					"session_id": state.SessionID,
					"agent_id":   state.AgentID,
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert recovery execution run ledger: %v", err)
	}
	if _, err := store.RecordSessionCompactionSnapshot(ctx, SessionCompactionSnapshotInput{
		SessionID:           state.SessionID,
		WorkspaceID:         workspaceID,
		AgentID:             state.AgentID,
		TriggerKind:         "token_budget_exceeded",
		TokenBudget:         120,
		MessageCountBefore:  18,
		MessageCountAfter:   9,
		MessageTokensBefore: 110,
		MessageTokensAfter:  78,
		TotalInputTokens:    140,
		TotalOutputTokens:   22,
		SummaryText:         "compaction summary for recovery",
	}); err != nil {
		t.Fatalf("record recovery compaction snapshot: %v", err)
	}
	staleSeenAt := time.Now().UTC().Add(-12 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleSeenAt, workspaceID, state.AgentID); err != nil {
		t.Fatalf("force stale heartbeat: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded snapshot with recovery manifest, got %+v", snapshot)
	}
	if snapshot.RecoveryManifest == nil {
		t.Fatalf("expected recovery manifest to be populated, got %+v", snapshot)
	}
	if got := snapshot.RecoveryManifest.PriorOwner.SessionID; got != state.SessionID {
		t.Fatalf("expected prior owner session to match, got %q from %+v", got, snapshot.RecoveryManifest)
	}
	if got := snapshot.RecoveryManifest.PriorOwner.AgentID; got != state.AgentID {
		t.Fatalf("expected prior owner agent to match, got %q from %+v", got, snapshot.RecoveryManifest)
	}
	if snapshot.RecoveryManifest.Checkpoint == nil || snapshot.RecoveryManifest.Checkpoint.RunID == "" || snapshot.RecoveryManifest.Checkpoint.StepID == "" {
		t.Fatalf("expected checkpoint to include run and step, got %+v", snapshot.RecoveryManifest)
	}
	if len(snapshot.RecoveryManifest.OpenOps) == 0 {
		t.Fatalf("expected at least one open op, got %+v", snapshot.RecoveryManifest)
	}
	if got := snapshot.RecoveryManifest.OpenOps[0].OperationID; got != "op-recovery-open-1" {
		t.Fatalf("expected open op to come from operation ledger, got %+v", snapshot.RecoveryManifest.OpenOps)
	}
	if snapshot.RecoveryManifest.BudgetRemainder == nil || snapshot.RecoveryManifest.BudgetRemainder.RemainingTokens != 42 {
		t.Fatalf("expected budget remainder to be derived from compaction snapshot, got %+v", snapshot.RecoveryManifest)
	}
	if snapshot.RecoveryManifest.NextAction.Action != "resume_from_checkpoint" || snapshot.RecoveryManifest.NextAction.RequiresOperator {
		t.Fatalf("expected resume_from_checkpoint next action, got %+v", snapshot.RecoveryManifest.NextAction)
	}
	if !strings.Contains(snapshot.RecoveryManifest.PriorOwner.StuckReason, "offline_agent") {
		t.Fatalf("expected stuck reason to mention the stale heartbeat, got %+v", snapshot.RecoveryManifest.PriorOwner)
	}
}

func TestCurrentStuckAgentSnapshotRebuildsRecoveryManifestAfterRestart(t *testing.T) {
	ctx := context.Background()

	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "rhizome.db")

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.ApplyMigrations(ctx); err != nil {
		_ = store.Close()
		t.Fatalf("apply migrations: %v", err)
	}

	const workspaceID = "ws-stuck-agent-restart-readback"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Restart Readback",
		CreatedBy:   "developer",
	}); err != nil {
		_ = store.Close()
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-restart-readback",
		DisplayName:       "Restart Readback Agent",
	}); err != nil {
		_ = store.Close()
		t.Fatalf("register agent: %v", err)
	}

	sessionUpdatedAt := time.Now().UTC().Add(-20 * time.Minute).Format(time.RFC3339Nano)
	state, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-restart-readback",
		AgentID:     "agent-restart-readback",
		Summary:     "restart readback candidate session",
		Status:      model.SessionStatusActive,
		UpdatedAt:   sessionUpdatedAt,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("record session coordination: %v", err)
	}
	if err := store.SyncExecutionRunFromSessionState(ctx, state); err != nil {
		_ = store.Close()
		t.Fatalf("sync execution run: %v", err)
	}
	runID := sessionExecutionRunID(state.SessionID)
	if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      state.TaskID,
		SessionID:   state.SessionID,
		AgentID:     state.AgentID,
		Title:       "Restart readback operation",
		Summary:     "operation remains open across restart",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"schema":         "operation_ledger.v1",
				"operation_id":   "op-restart-readback-1",
				"operation_name": "restart-readback-tool",
				"operation_kind": "tool_call",
				"status":         "running",
				"terminal":       false,
				"updated_at":     sessionUpdatedAt,
				"binding": map[string]any{
					"run_id":     runID,
					"task_id":    state.TaskID,
					"session_id": state.SessionID,
					"agent_id":   state.AgentID,
				},
			},
		},
	}); err != nil {
		_ = store.Close()
		t.Fatalf("upsert open operation ledger: %v", err)
	}
	if _, err := store.RecordSessionCompactionSnapshot(ctx, SessionCompactionSnapshotInput{
		SessionID:           state.SessionID,
		WorkspaceID:         workspaceID,
		AgentID:             state.AgentID,
		TriggerKind:         "token_budget_exceeded",
		TokenBudget:         120,
		MessageCountBefore:  18,
		MessageCountAfter:   9,
		MessageTokensBefore: 110,
		MessageTokensAfter:  78,
		TotalInputTokens:    140,
		TotalOutputTokens:   22,
		SummaryText:         "restart readback compaction summary",
	}); err != nil {
		_ = store.Close()
		t.Fatalf("record compaction snapshot: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ? WHERE workspace_id = ? AND agent_id = ?`, time.Now().UTC().Add(-12*time.Minute).Format(time.RFC3339Nano), workspaceID, state.AgentID); err != nil {
		_ = store.Close()
		t.Fatalf("stale heartbeat update: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store before restart: %v", err)
	}

	reopened, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	snapshot := reopened.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded snapshot after restart, got %+v", snapshot)
	}
	if snapshot.RecoveryManifest == nil {
		t.Fatalf("expected recovery manifest after restart, got %+v", snapshot)
	}
	if got := snapshot.RecoveryManifest.PriorOwner.SessionID; got != state.SessionID {
		t.Fatalf("expected prior owner session to survive restart, got %q from %+v", got, snapshot.RecoveryManifest)
	}
	if snapshot.RecoveryManifest.Checkpoint == nil || snapshot.RecoveryManifest.Checkpoint.RunID != runID || snapshot.RecoveryManifest.Checkpoint.StepID == "" {
		t.Fatalf("expected durable checkpoint after restart, got %+v", snapshot.RecoveryManifest)
	}
	if len(snapshot.RecoveryManifest.OpenOps) != 1 {
		t.Fatalf("expected one open operation after restart, got %+v", snapshot.RecoveryManifest.OpenOps)
	}
	open := snapshot.RecoveryManifest.OpenOps[0]
	if open.OperationID != "op-restart-readback-1" {
		t.Fatalf("expected durable open operation id after restart, got %+v", open)
	}
	if open.RunID != runID || open.SessionID != state.SessionID || open.TaskID != state.TaskID {
		t.Fatalf("expected durable open operation binding after restart, got %+v", open)
	}
	if snapshot.RecoveryManifest.BudgetRemainder == nil || snapshot.RecoveryManifest.BudgetRemainder.RemainingTokens != 42 {
		t.Fatalf("expected compaction budget remainder after restart, got %+v", snapshot.RecoveryManifest.BudgetRemainder)
	}
	if snapshot.RecoveryManifest.NextAction.Action != string(StuckAgentRecoveryActionResumeFromCheckpoint) || snapshot.RecoveryManifest.NextAction.RequiresOperator {
		t.Fatalf("expected restart recovery to remain checkpoint-driven, got %+v", snapshot.RecoveryManifest.NextAction)
	}
}

func TestCurrentStuckAgentSnapshotUsesSessionExecutionFallbackWhenLedgerMissing(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-recovery-fallback"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Recovery Fallback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-recovery-fallback",
		DisplayName:       "Recovery Fallback Agent",
	}); err != nil {
		t.Fatalf("register recovery fallback agent: %v", err)
	}

	sessionUpdatedAt := time.Now().UTC().Add(-15 * time.Minute).Format(time.RFC3339Nano)
	state, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-recovery-fallback",
		AgentID:     "agent-recovery-fallback",
		Summary:     "recovery fallback candidate session",
		Status:      model.SessionStatusActive,
		UpdatedAt:   sessionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("record recovery fallback coordination: %v", err)
	}
	syncResult, err := store.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	if err != nil {
		t.Fatalf("sync recovery fallback execution run: %v", err)
	}
	staleSeenAt := time.Now().UTC().Add(-12 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleSeenAt, workspaceID, state.AgentID); err != nil {
		t.Fatalf("force stale fallback heartbeat: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "degraded" || snapshot.RecoveryManifest == nil {
		t.Fatalf("expected degraded snapshot with recovery manifest, got %+v", snapshot)
	}
	if got := snapshot.RecoveryManifest.Checkpoint.RunID; got != syncResult.Run.RunID {
		t.Fatalf("expected checkpoint run from session execution sync, got %q want %q", got, syncResult.Run.RunID)
	}
	if got := snapshot.RecoveryManifest.Checkpoint.StepID; got != syncResult.Step.StepID {
		t.Fatalf("expected checkpoint step from session execution sync, got %q want %q", got, syncResult.Step.StepID)
	}
	if len(snapshot.RecoveryManifest.OpenOps) != 1 {
		t.Fatalf("expected one fallback open run op, got %+v", snapshot.RecoveryManifest.OpenOps)
	}
	open := snapshot.RecoveryManifest.OpenOps[0]
	if open.OperationID != "" {
		t.Fatalf("expected ledger operation id to be empty for fallback open run, got %+v", open)
	}
	if open.RunID != syncResult.Run.RunID || open.LastStepID != syncResult.Step.StepID {
		t.Fatalf("expected fallback open op to reference durable run/step, got %+v run=%+v step=%+v", open, syncResult.Run, syncResult.Step)
	}
	if snapshot.RecoveryManifest.NextAction.Action != "resume_from_checkpoint" {
		t.Fatalf("expected resume_from_checkpoint fallback action, got %+v", snapshot.RecoveryManifest.NextAction)
	}
}

func TestCurrentStuckAgentSnapshotUsesOlderRunWhenLatestHasNoSteps(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-recovery-older-run"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Recovery Older Run",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-recovery-older-run",
		DisplayName:       "Recovery Older Run Agent",
	}); err != nil {
		t.Fatalf("register recovery older-run agent: %v", err)
	}

	sessionUpdatedAt := time.Now().UTC().Add(-15 * time.Minute).Format(time.RFC3339Nano)
	state, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-recovery-older-run",
		AgentID:     "agent-recovery-older-run",
		Summary:     "recovery older-run candidate session",
		Status:      model.SessionStatusActive,
		UpdatedAt:   sessionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("record recovery older-run coordination: %v", err)
	}
	olderSync, err := store.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	if err != nil {
		t.Fatalf("sync recovery older-run execution run: %v", err)
	}
	if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       "zz-newer-empty-restart-stub",
		WorkspaceID: workspaceID,
		TaskID:      state.TaskID,
		SessionID:   state.SessionID,
		AgentID:     state.AgentID,
		Title:       "Newer restart stub",
		Summary:     "newer run has not written a durable step yet",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("upsert newer empty restart stub: %v", err)
	}
	staleSeenAt := time.Now().UTC().Add(-12 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleSeenAt, workspaceID, state.AgentID); err != nil {
		t.Fatalf("force stale older-run heartbeat: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "degraded" || snapshot.RecoveryManifest == nil {
		t.Fatalf("expected degraded snapshot with recovery manifest, got %+v", snapshot)
	}
	if snapshot.RecoveryManifest.Checkpoint == nil {
		t.Fatalf("expected checkpoint from older durable run, got %+v", snapshot.RecoveryManifest)
	}
	if got := snapshot.RecoveryManifest.Checkpoint.RunID; got != olderSync.Run.RunID {
		t.Fatalf("expected checkpoint to use older run with steps, got %q want %q manifest=%+v", got, olderSync.Run.RunID, snapshot.RecoveryManifest)
	}
	if got := snapshot.RecoveryManifest.Checkpoint.StepID; got != olderSync.Step.StepID {
		t.Fatalf("expected checkpoint to use older durable step, got %q want %q manifest=%+v", got, olderSync.Step.StepID, snapshot.RecoveryManifest)
	}
	if snapshot.RecoveryManifest.NextAction.Action != "resume_from_checkpoint" {
		t.Fatalf("expected older durable checkpoint to preserve resume action, got %+v", snapshot.RecoveryManifest.NextAction)
	}
}

func TestCurrentStuckAgentSnapshotUsesCheckpointBeyondTenRuns(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-recovery-beyond-ten"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Recovery Beyond Ten Runs",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-recovery-beyond-ten",
		DisplayName:       "Recovery Beyond Ten Agent",
	}); err != nil {
		t.Fatalf("register recovery beyond-ten agent: %v", err)
	}

	sessionUpdatedAt := time.Now().UTC().Add(-15 * time.Minute).Format(time.RFC3339Nano)
	state, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-recovery-beyond-ten",
		AgentID:     "agent-recovery-beyond-ten",
		Summary:     "recovery beyond-ten candidate session",
		Status:      model.SessionStatusActive,
		UpdatedAt:   sessionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("record recovery beyond-ten coordination: %v", err)
	}
	olderSync, err := store.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	if err != nil {
		t.Fatalf("sync recovery beyond-ten execution run: %v", err)
	}
	for idx := 0; idx < 11; idx++ {
		if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
			RunID:       fmt.Sprintf("zz-newer-empty-restart-stub-%02d", idx),
			WorkspaceID: workspaceID,
			TaskID:      state.TaskID,
			SessionID:   state.SessionID,
			AgentID:     state.AgentID,
			Title:       "Newer restart stub",
			Summary:     "newer run has not written a durable step yet",
			Status:      "ACTIVE",
		}); err != nil {
			t.Fatalf("upsert newer empty restart stub %d: %v", idx, err)
		}
	}
	staleSeenAt := time.Now().UTC().Add(-12 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleSeenAt, workspaceID, state.AgentID); err != nil {
		t.Fatalf("force stale beyond-ten heartbeat: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "degraded" || snapshot.RecoveryManifest == nil {
		t.Fatalf("expected degraded snapshot with recovery manifest, got %+v", snapshot)
	}
	if snapshot.RecoveryManifest.Checkpoint == nil {
		t.Fatalf("expected checkpoint beyond ten newer stubs, got %+v", snapshot.RecoveryManifest)
	}
	if got := snapshot.RecoveryManifest.Checkpoint.RunID; got != olderSync.Run.RunID {
		t.Fatalf("expected checkpoint to use run beyond former ten-run cap, got %q want %q manifest=%+v", got, olderSync.Run.RunID, snapshot.RecoveryManifest)
	}
	if got := snapshot.RecoveryManifest.Checkpoint.StepID; got != olderSync.Step.StepID {
		t.Fatalf("expected checkpoint to use durable step beyond former ten-run cap, got %q want %q manifest=%+v", got, olderSync.Step.StepID, snapshot.RecoveryManifest)
	}
}

func TestCurrentStuckAgentSnapshotCountsMissingAgentAsDeadHeartbeat(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-health-missing"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Health Missing",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-missing-agent",
		AgentID:     "agent-missing",
		WorkspaceID: workspaceID,
		TaskID:      "task-missing-agent",
		StartedAt:   time.Now().UTC().Add(-90 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create missing-agent session: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded stuck-agent snapshot, got %+v", snapshot)
	}
	if snapshot.DeadHeartbeatSessionCount != 1 || snapshot.MissingAgentSessionCount != 1 || snapshot.OfflineAgentSessionCount != 0 {
		t.Fatalf("expected missing agent to count once as dead heartbeat without offline double-counting, got %+v", snapshot)
	}
	if snapshot.StaleActivitySessionCount != 1 {
		t.Fatalf("expected stale activity to be tracked alongside missing agent, got %+v", snapshot)
	}
}

func TestCurrentStuckAgentSnapshotIgnoresArchivedWorkspaceSessions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-health-archived"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Health Archived",
		CreatedBy:   "developer",
		Status:      model.WorkspaceStatusArchived,
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-offline",
		DisplayName:       "Offline Agent",
	}); err != nil {
		t.Fatalf("register offline agent: %v", err)
	}

	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-archived-agent",
		AgentID:     "agent-offline",
		WorkspaceID: workspaceID,
		TaskID:      "task-archived-agent",
		StartedAt:   time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create archived workspace session: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected archived workspace sessions to be ignored, got %+v", snapshot)
	}
	if snapshot.ActiveSessionCount != 0 {
		t.Fatalf("expected archived workspace sessions to be excluded, got %+v", snapshot)
	}
}

func TestCurrentStuckAgentSnapshotGrantsStartupGraceBeforeFirstHeartbeat(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-stuck-agent-health-startup-grace"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Stuck Agent Health Startup Grace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: DefaultWorkspacePassword,
		AgentID:           "agent-fresh",
		DisplayName:       "Fresh Agent",
	}); err != nil {
		t.Fatalf("register fresh agent: %v", err)
	}

	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-fresh-agent",
		AgentID:     "agent-fresh",
		WorkspaceID: workspaceID,
		TaskID:      "task-fresh-agent",
		StartedAt:   time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create fresh-agent session: %v", err)
	}

	snapshot := store.CurrentStuckAgentSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected startup grace to keep fresh session healthy, got %+v", snapshot)
	}
	if snapshot.ActiveSessionCount != 1 || snapshot.StartupGraceSessionCount != 1 {
		t.Fatalf("expected one startup-grace session, got %+v", snapshot)
	}
	if snapshot.RecoveryManifest != nil {
		t.Fatalf("expected no recovery manifest during startup grace, got %+v", snapshot.RecoveryManifest)
	}
	if snapshot.DeadHeartbeatSessionCount != 0 || snapshot.OfflineAgentSessionCount != 0 || snapshot.MissingAgentSessionCount != 0 || snapshot.StaleActivitySessionCount != 0 {
		t.Fatalf("expected startup grace not to count as dead/stale, got %+v", snapshot)
	}
}
