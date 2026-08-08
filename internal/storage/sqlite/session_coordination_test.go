package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordAgentSessionCoordinationAndListWorkspaceSessions(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session",
		Title:       "Session Coordination",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-session",
		AgentID:     "agent-session",
		OwnerUserID: "developer",
		DisplayName: "Session Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-session",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-session",
		TaskID:      "task-session",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-session",
		TaskID:      "task-session",
		AgentID:     "agent-session",
		Summary:     "claim before session start",
	}); err != nil {
		t.Fatalf("claim task before session start: %v", err)
	}

	startState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-session",
		SessionID:   "sess-1",
		AgentID:     "agent-session",
		TaskID:      "task-session",
		Summary:     "Taking ownership of the task",
		OwnerScope:  "task/session",
	})
	if err != nil {
		t.Fatalf("record start session coordination: %v", err)
	}
	if startState.Status != model.SessionStatusActive {
		t.Fatalf("expected ACTIVE session, got %+v", startState)
	}
	if startState.KeepSessionActive == nil || !*startState.KeepSessionActive {
		t.Fatalf("expected keep_session_active=true, got %+v", startState.KeepSessionActive)
	}

	keepFalse := false
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: "ws-session",
		SessionID:   "sess-1",
		AgentID:     "agent-session",
		Summary:     "Blocked waiting on bridge credential",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "auth", Detail: "Need bridge credential"},
		},
		KeepSessionActive: &keepFalse,
	})
	if err != nil {
		t.Fatalf("record blocked session coordination: %v", err)
	}
	if blockedState.Status != model.SessionStatusBlocked {
		t.Fatalf("expected BLOCKED session, got %+v", blockedState)
	}
	if blockedState.KeepSessionActive == nil || *blockedState.KeepSessionActive {
		t.Fatalf("expected keep_session_active=false, got %+v", blockedState.KeepSessionActive)
	}

	sessions, err := store.ListWorkspaceSessionStates(ctx, "ws-session", true, 10)
	if err != nil {
		t.Fatalf("list workspace session states: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected one active session state, got %+v", sessions)
	}
	got := sessions[0]
	if got.SessionID != "sess-1" || got.Status != model.SessionStatusBlocked {
		t.Fatalf("unexpected workspace session state: %+v", got)
	}
	if got.TaskID != "task-session" {
		t.Fatalf("expected task-session in session state, got %+v", got)
	}
	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "auth" {
		t.Fatalf("expected blocked_on payload, got %+v", got.BlockedOn)
	}
}

func TestTaskSessionStartRequiresLiveSameOwnerClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-session-live-claim"
		ownerUserID = "developer"
		agentID     = "agent-session-owner"
		peerID      = "agent-session-peer"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Live Claim",
		CreatedBy:   ownerUserID,
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agent := range []string{agentID, peerID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agent,
			OwnerUserID: ownerUserID,
			DisplayName: agent,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agent, err)
		}
	}

	for _, taskID := range []string{"task-no-claim", "task-peer-claim", "task-released-claim", "task-terminal", "task-same-claim", "task-pending-claimed"} {
		createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: "task-peer-claim", AgentID: peerID, Summary: "peer owns"}); err != nil {
		t.Fatalf("peer claim: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: "task-released-claim", AgentID: agentID, Summary: "claim before release"}); err != nil {
		t.Fatalf("released claim setup: %v", err)
	}
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{WorkspaceID: workspaceID, TaskID: "task-released-claim", AgentID: agentID, Reason: "release before session"}); err != nil {
		t.Fatalf("release setup claim: %v", err)
	}
	if err := store.CloseTask(ctx, sqlite.TaskCloseInput{WorkspaceID: workspaceID, TaskID: "task-terminal", ActorID: ownerUserID, Resolution: model.TaskStatusCancelled, Reason: "terminal before claim"}); err != nil {
		t.Fatalf("close terminal task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: "task-same-claim", AgentID: agentID, Summary: "same owner claim"}); err != nil {
		t.Fatalf("same owner claim: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: "task-pending-claimed", AgentID: agentID, Summary: "same owner claim with inconsistent task state"}); err != nil {
		t.Fatalf("pending claimed setup: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusPending, "task-pending-claimed"); err != nil {
		t.Fatalf("mark claimed task pending: %v", err)
	}

	for _, tc := range []struct {
		name    string
		taskID  string
		wantErr bool
	}{
		{name: "no claim", taskID: "task-no-claim", wantErr: true},
		{name: "peer claim", taskID: "task-peer-claim", wantErr: true},
		{name: "released claim", taskID: "task-released-claim", wantErr: true},
		{name: "terminal task", taskID: "task-terminal", wantErr: true},
		{name: "claimed pending task", taskID: "task-pending-claimed", wantErr: true},
		{name: "same owner claim", taskID: "task-same-claim"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
				EventType:   model.SessionEventStart,
				WorkspaceID: workspaceID,
				SessionID:   "sess-" + tc.taskID,
				AgentID:     agentID,
				TaskID:      tc.taskID,
				Summary:     "start task-bound work",
			})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "session start requires") {
					t.Fatalf("expected live-claim session start rejection, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("same-owner claimed task should start session: %v", err)
			}
		})
	}
}

func TestListWorkspaceSessionStatesActiveOnlyRequiresDurableStartReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID       = "ws-session-active-receipts"
		agentID           = "agent-session-active-receipts"
		receiptlessTask   = "task-receiptless-session"
		receiptBackedTask = "task-receipt-backed-session"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Active Session Receipts",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Active Receipt Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	for _, taskID := range []string{receiptlessTask, receiptBackedTask} {
		createWorkspaceTask(t, ctx, store, workspaceID, taskID)
		if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: taskID, AgentID: agentID, Summary: "claim before session"}); err != nil {
			t.Fatalf("claim task %s: %v", taskID, err)
		}
	}

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   "sess-receiptless-active",
		TaskID:      receiptlessTask,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create receiptless session: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   "sess-receipt-backed-active",
		TaskID:      receiptBackedTask,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create receipt-backed session: %v", err)
	}

	activeSessions, err := store.ListWorkspaceSessionStates(ctx, workspaceID, true, 10)
	if err != nil {
		t.Fatalf("list active session states: %v", err)
	}
	if len(activeSessions) != 1 || activeSessions[0].SessionID != "sess-receipt-backed-active" {
		t.Fatalf("expected only receipt-backed active session, got %+v", activeSessions)
	}
	allSessions, err := store.ListWorkspaceSessionStates(ctx, workspaceID, false, 10)
	if err != nil {
		t.Fatalf("list all session states: %v", err)
	}
	if len(allSessions) != 2 {
		t.Fatalf("all-session history should preserve receiptless evidence, got %+v", allSessions)
	}
}

func TestListWorkspaceSessionStatesActiveOnlyExcludesEndedSessions(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-ended",
		Title:       "Ended Sessions",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-ended")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-ended",
		AgentID:     "agent-ended",
		OwnerUserID: "developer",
		DisplayName: "Ended Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-ended",
		SessionID:   "sess-ended",
		AgentID:     "agent-ended",
		Summary:     "Started work",
	}); err != nil {
		t.Fatalf("record start session coordination: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: "ws-ended",
		SessionID:   "sess-ended",
		AgentID:     "agent-ended",
		Summary:     "Done and closing session",
	}); err != nil {
		t.Fatalf("record end session coordination: %v", err)
	}

	activeSessions, err := store.ListWorkspaceSessionStates(ctx, "ws-ended", true, 10)
	if err != nil {
		t.Fatalf("list active session states: %v", err)
	}
	if len(activeSessions) != 0 {
		t.Fatalf("expected no active sessions after end, got %+v", activeSessions)
	}

	allSessions, err := store.ListWorkspaceSessionStates(ctx, "ws-ended", false, 10)
	if err != nil {
		t.Fatalf("list all session states: %v", err)
	}
	if len(allSessions) != 1 || allSessions[0].Status != model.SessionStatusEnded {
		t.Fatalf("expected ended session in all-sessions view, got %+v", allSessions)
	}
	if allSessions[0].KeepSessionActive == nil || *allSessions[0].KeepSessionActive {
		t.Fatalf("expected ended session keep_session_active=false, got %+v", allSessions[0].KeepSessionActive)
	}
}

func TestListWorkspaceSessionStatesActiveOnlyIsLimitSafe(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-session-active-limit-safe"
		agentID     = "agent-session-active-limit-safe"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Active Limit Safe Sessions",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Active Limit Safe Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-old-active",
		AgentID:     agentID,
		Summary:     "Old but still active",
		UpdatedAt:   "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatalf("record old active session: %v", err)
	}
	for i := 0; i < 5; i++ {
		sessionID := fmt.Sprintf("sess-new-ended-%d", i)
		if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
			EventType:   model.SessionEventStart,
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			AgentID:     agentID,
			Summary:     "Newer terminal session",
			UpdatedAt:   fmt.Sprintf("2026-02-0%dT00:00:00Z", i+1),
		}); err != nil {
			t.Fatalf("record newer session start %d: %v", i, err)
		}
		if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
			EventType:   model.SessionEventEnd,
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			AgentID:     agentID,
			Summary:     "Ended newer session",
			UpdatedAt:   fmt.Sprintf("2026-02-0%dT00:10:00Z", i+1),
		}); err != nil {
			t.Fatalf("record newer session end %d: %v", i, err)
		}
	}
	activeSessions, err := store.ListWorkspaceSessionStates(ctx, workspaceID, true, 1)
	if err != nil {
		t.Fatalf("list active sessions: %v", err)
	}
	if len(activeSessions) != 1 || activeSessions[0].SessionID != "sess-old-active" {
		t.Fatalf("active-only limit hid old active session: %+v", activeSessions)
	}
}

func TestListWorkspaceSessionStatesIgnoresLateActiveUpdateAfterEnd(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ended-late-active"
	agentID := "agent-ended-late-active"
	sessionID := "sess-ended-late-active"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Ended Late Active Sessions",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Ended Late Active Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Started work",
	}); err != nil {
		t.Fatalf("record start session coordination: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Done and closing session",
	}); err != nil {
		t.Fatalf("record end session coordination: %v", err)
	}

	keepActive := true
	latePayload, err := json.Marshal(model.SessionCoordinationPayloadV1{
		SessionID:         sessionID,
		Status:            model.SessionStatusActive,
		Summary:           "Stale delayed active update",
		KeepSessionActive: &keepActive,
		UpdatedAt:         "2030-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal late payload: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"agent_update_late_active_after_end",
		workspaceID,
		agentID,
		model.SessionEventStatus,
		"Stale delayed active update",
		string(latePayload),
		0,
		"2030-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert late active update: %v", err)
	}

	activeSessions, err := store.ListWorkspaceSessionStates(ctx, workspaceID, true, 10)
	if err != nil {
		t.Fatalf("list active session states: %v", err)
	}
	if len(activeSessions) != 0 {
		t.Fatalf("expected no active sessions when base row is ended, got %+v", activeSessions)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected terminal base status to win over late active update, got %+v", state)
	}
}

func TestRecordAgentSessionCoordinationRejectsForeignOwnerUpdateWithoutRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session-owner-guard",
		Title:       "Session Owner Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session-owner-guard")
	for _, agentID := range []string{"agent-session-owner", "agent-session-foreign"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-session-owner-guard",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createSingleNodeTask(t, ctx, store, "task-session-owner-guard", "node-session-owner-guard")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-session-owner-guard",
		TaskID:      "task-session-owner-guard",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-session-owner-guard", "task-session-owner-guard", "agent-session-owner")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-session-owner-guard",
		SessionID:   "sess-owner-guard",
		AgentID:     "agent-session-owner",
		TaskID:      "task-session-owner-guard",
		Summary:     "Owner started work",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: "ws-session-owner-guard",
		SessionID:   "sess-owner-guard",
		AgentID:     "agent-session-foreign",
		TaskID:      "task-session-owner-guard",
		Summary:     "Foreign agent tries to overwrite",
	}); !errors.Is(err, sqlite.ErrSessionOwnershipMismatch) {
		t.Fatalf("expected ErrSessionOwnershipMismatch, got %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-session-owner-guard",
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    "sess-owner-guard",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no session.status runtime event after foreign update rejection, got %+v", events)
	}

	state, err := store.GetAgentSessionState(ctx, "ws-session-owner-guard", "sess-owner-guard")
	if err != nil {
		t.Fatalf("get agent session state: %v", err)
	}
	if state.AgentID != "agent-session-owner" || state.Status != model.SessionStatusActive || state.Summary != "Owner started work" {
		t.Fatalf("expected owner session state to remain unchanged, got %+v", state)
	}
}

func TestRecordAgentSessionCoordinationRejectsTaskBoundNonStartWithoutStartReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-session-non-start-receipt"
		agentID     = "agent-session-non-start-receipt"
		taskID      = "task-session-non-start-receipt"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Non Start Session Receipt",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Non Start Receipt Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim exists but no session start receipt",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: workspaceID,
		SessionID:   "sess-missing-start",
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "status cannot create task-bound session",
	}); err == nil || !strings.Contains(err.Error(), "requires durable session start receipt") {
		t.Fatalf("expected missing start receipt rejection, got %v", err)
	}
	if _, err := store.GetAgentSessionState(ctx, workspaceID, "sess-missing-start"); !errors.Is(err, sqlite.ErrSessionNotFound) {
		t.Fatalf("non-start update should not insert normal session row, got %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   "sess-receiptless-existing",
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create receiptless session row: %v", err)
	}
	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   "sess-receiptless-existing",
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "blocked update cannot use receiptless row",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "runtime", Detail: "missing start receipt"}},
	}); err == nil || !strings.Contains(err.Error(), "requires durable session start receipt") {
		t.Fatalf("expected receiptless existing row rejection, got %v", err)
	}
}

func TestRecordAgentSessionCoordinationRejectsCrossWorkspaceSessionSpoof(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session-a",
		Title:       "Session A",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace A: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session-b",
		Title:       "Session B",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace B: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session-b")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-session-a",
		AgentID:     "agent-session-a",
		OwnerUserID: "developer",
		DisplayName: "agent-session-a",
	}); err != nil {
		t.Fatalf("register agent A: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-session-b",
		AgentID:     "agent-session-b",
		OwnerUserID: "developer",
		DisplayName: "agent-session-b",
	}); err != nil {
		t.Fatalf("register agent B: %v", err)
	}

	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-session-a",
		SessionID:   "sess-cross-workspace",
		AgentID:     "agent-session-a",
		Summary:     "Workspace A owns this session",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: "ws-session-b",
		SessionID:   "sess-cross-workspace",
		AgentID:     "agent-session-b",
		Summary:     "Workspace B attempts to spoof session ownership",
	}); !errors.Is(err, sqlite.ErrSessionOwnershipMismatch) {
		t.Fatalf("expected ErrSessionOwnershipMismatch, got %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-session-b",
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    "sess-cross-workspace",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace B runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no workspace B runtime event after cross-workspace spoof rejection, got %+v", events)
	}

	state, err := store.GetAgentSessionState(ctx, "ws-session-a", "sess-cross-workspace")
	if err != nil {
		t.Fatalf("get workspace A session state: %v", err)
	}
	if state.AgentID != "agent-session-a" || state.Status != model.SessionStatusActive {
		t.Fatalf("expected workspace A session state to stay active, got %+v", state)
	}
}

func TestRecordAgentSessionCoordination_EndPreservesTerminalStatusInResult(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-terminal-ended",
		Title:       "Terminal Ended Sessions",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-terminal-ended")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-terminal-ended",
		AgentID:     "agent-terminal",
		OwnerUserID: "developer",
		DisplayName: "Terminal Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-terminal",
		AgentID:     "agent-terminal",
		WorkspaceID: "ws-terminal-ended",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         "sess-terminal",
		Status:            "FAILED",
		TotalInputTokens:  12,
		TotalOutputTokens: 6,
		CompletedAt:       time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("update agent session: %v", err)
	}

	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: "ws-terminal-ended",
		SessionID:   "sess-terminal",
		AgentID:     "agent-terminal",
		Summary:     "Failed and closed",
	})
	if err != nil {
		t.Fatalf("record end session coordination: %v", err)
	}
	if state.Status != "FAILED" {
		t.Fatalf("expected FAILED status to be preserved in returned state, got %+v", state)
	}
}

func TestTakeOverAgentSessionCreatesSuccessorAndEndsSource(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-takeover",
		Title:       "Session Takeover",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-takeover")
	for _, agentID := range []string{"agent-source", "agent-target"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-takeover",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-takeover",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-takeover",
		TaskID:      "task-takeover",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-takeover", "task-takeover", "agent-source")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-takeover",
		SessionID:   "sess-source",
		AgentID:     "agent-source",
		TaskID:      "task-takeover",
		Summary:     "Owning the transport rollout",
		OwnerScope:  "task/session",
		HandoffTo:   "agent-target",
	}); err != nil {
		t.Fatalf("record start session coordination: %v", err)
	}

	record, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     "ws-takeover",
		SessionID:       "sess-source",
		TakeoverAgentID: "agent-target",
		Summary:         "Need a fresh owner for the rollout",
	})
	if err != nil {
		t.Fatalf("take over agent session: %v", err)
	}
	if record.SourceState.Status != model.SessionStatusEnded {
		t.Fatalf("expected source session ended, got %+v", record.SourceState)
	}
	if record.SuccessorState.Status != model.SessionStatusActive || record.SuccessorState.AgentID != "agent-target" {
		t.Fatalf("expected successor session active for agent-target, got %+v", record.SuccessorState)
	}
	if record.SuccessorState.TaskID != "task-takeover" || record.SuccessorState.OwnerScope != "task/session" {
		t.Fatalf("expected successor session to inherit task and owner scope, got %+v", record.SuccessorState)
	}

	activeSessions, err := store.ListWorkspaceSessionStates(ctx, "ws-takeover", true, 10)
	if err != nil {
		t.Fatalf("list active session states: %v", err)
	}
	if len(activeSessions) != 1 || activeSessions[0].SessionID != record.SuccessorState.SessionID {
		t.Fatalf("expected only successor to stay active, got %+v", activeSessions)
	}

	allSessions, err := store.ListWorkspaceSessionStates(ctx, "ws-takeover", false, 10)
	if err != nil {
		t.Fatalf("list all session states: %v", err)
	}
	if len(allSessions) != 2 {
		t.Fatalf("expected two sessions after takeover, got %+v", allSessions)
	}
}

// TestTakeOverAgentSessionResolvesSourceOperatorQueues is the CA-17 regression:
// when the source session has an OPEN operator queue (e.g. a BLOCKER), the takeover
// must resolve it inside the takeover tx, not orphan it referencing the now-ended
// source session.
func TestTakeOverAgentSessionResolvesSourceOperatorQueues(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const ws = "ws-takeover-queue"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: ws, Title: "Takeover Queue", CreatedBy: "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, ws)
	for _, agentID := range []string{"agent-source", "agent-target"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: ws, AgentID: agentID, OwnerUserID: "developer", DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	// Source session starts, then goes BLOCKED -> creates an OPEN BLOCKER queue.
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType: model.SessionEventStart, WorkspaceID: ws, SessionID: "sess-source",
		AgentID: "agent-source", Summary: "owning the work", HandoffTo: "agent-target",
	}); err != nil {
		t.Fatalf("record start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType: model.SessionEventBlocked, WorkspaceID: ws, SessionID: "sess-source",
		AgentID: "agent-source", Summary: "blocked on operator", HandoffTo: "agent-target",
		BlockedOn: []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve"}},
	})
	if err != nil {
		t.Fatalf("record blocked: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync blocker queue: %v", err)
	}
	openBefore, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: ws, SessionID: "sess-source", Status: "OPEN",
	})
	if err != nil {
		t.Fatalf("list open queues before: %v", err)
	}
	if len(openBefore) != 1 || openBefore[0].QueueType != "BLOCKER" {
		t.Fatalf("precondition: expected one OPEN BLOCKER for the source session, got %+v", openBefore)
	}

	if _, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID: ws, SessionID: "sess-source", TakeoverAgentID: "agent-target",
		Summary: "fresh owner",
	}); err != nil {
		t.Fatalf("take over: %v", err)
	}

	openAfter, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: ws, SessionID: "sess-source", Status: "OPEN",
	})
	if err != nil {
		t.Fatalf("list open queues after: %v", err)
	}
	if len(openAfter) != 0 {
		t.Fatalf("CA-17: source session's operator queues must be resolved after takeover, still open: %+v", openAfter)
	}
}

func TestTakeOverAgentSessionRejectsUnassignedTakeoverAgent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-takeover-guard-store",
		Title:       "Store Takeover Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-takeover-guard-store")
	for _, agentID := range []string{"agent-source-store", "agent-target-store"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-takeover-guard-store",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-takeover-guard-store",
		SessionID:   "sess-takeover-guard-store",
		AgentID:     "agent-source-store",
		Summary:     "Source starts work without assigning a handoff",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	if _, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     "ws-takeover-guard-store",
		SessionID:       "sess-takeover-guard-store",
		TakeoverAgentID: "agent-target-store",
		Summary:         "Try to preempt without explicit handoff",
	}); !errors.Is(err, sqlite.ErrSessionTakeoverNotAuthorized) {
		t.Fatalf("expected ErrSessionTakeoverNotAuthorized, got %v", err)
	}

	state, err := store.GetAgentSessionState(ctx, "ws-takeover-guard-store", "sess-takeover-guard-store")
	if err != nil {
		t.Fatalf("get source session state: %v", err)
	}
	if state.AgentID != "agent-source-store" || state.Status != model.SessionStatusActive {
		t.Fatalf("expected source session to remain active and unchanged, got %+v", state)
	}
}

func TestTakeOverAgentSessionValidationErrorsUseSentinels(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID     = "ws-takeover-validation-sentinels"
		sourceAgentID   = "agent-takeover-source"
		targetAgentID   = "agent-takeover-target"
		sourceSessionID = "sess-takeover-source"
		existingSession = "sess-takeover-existing"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Store Takeover Validation Sentinels",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{sourceAgentID, targetAgentID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgentID,
		Summary:     "Source starts work and assigns a handoff",
		HandoffTo:   targetAgentID,
	}); err != nil {
		t.Fatalf("record source session start: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   existingSession,
		AgentID:     targetAgentID,
		Summary:     "Existing session blocks duplicate successor id",
	}); err != nil {
		t.Fatalf("record existing session start: %v", err)
	}

	cases := []struct {
		name  string
		input sqlite.AgentSessionTakeoverInput
		want  error
	}{
		{
			name: "same takeover agent",
			input: sqlite.AgentSessionTakeoverInput{
				WorkspaceID:     workspaceID,
				SessionID:       sourceSessionID,
				TakeoverAgentID: sourceAgentID,
				Summary:         "self takeover must be rejected",
			},
			want: sqlite.ErrSessionTakeoverAgentSame,
		},
		{
			name: "successor session same as source",
			input: sqlite.AgentSessionTakeoverInput{
				WorkspaceID:        workspaceID,
				SessionID:          sourceSessionID,
				TakeoverAgentID:    targetAgentID,
				SuccessorSessionID: sourceSessionID,
				Summary:            "same successor id must be rejected",
			},
			want: sqlite.ErrSessionTakeoverSuccessorSame,
		},
		{
			name: "successor already exists",
			input: sqlite.AgentSessionTakeoverInput{
				WorkspaceID:        workspaceID,
				SessionID:          sourceSessionID,
				TakeoverAgentID:    targetAgentID,
				SuccessorSessionID: existingSession,
				Summary:            "existing successor id must be rejected",
			},
			want: sqlite.ErrSessionTakeoverSuccessorExists,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.TakeOverAgentSession(ctx, tc.input); !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

func TestRecordAgentSessionCoordinationAppendsRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session-runtime-event",
		Title:       "Session Runtime Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session-runtime-event")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-session-runtime-event",
		AgentID:     "agent-session-runtime",
		OwnerUserID: "developer",
		DisplayName: "Session Runtime Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, "task-session-runtime", "node-session-runtime")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-session-runtime-event",
		TaskID:      "task-session-runtime",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-session-runtime-event", "task-session-runtime", "agent-session-runtime")
	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-session-runtime-event",
		SessionID:   "sess-runtime-event",
		AgentID:     "agent-session-runtime",
		TaskID:      "task-session-runtime",
		Summary:     "Starting projection path",
	})
	if err != nil {
		t.Fatalf("record session coordination: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-session-runtime-event",
		EventType:   "session.start",
		EntityType:  "agent_session",
		EntityID:    state.SessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 runtime event, got %+v", events)
	}
	if events[0].AgentID != "agent-session-runtime" || events[0].ActorID != "agent-session-runtime" {
		t.Fatalf("expected session runtime event actor/agent ids, got %+v", events[0])
	}
	if events[0].TaskID != "task-session-runtime" || events[0].SessionID != state.SessionID {
		t.Fatalf("expected task/session ids on runtime event, got %+v", events[0])
	}
}

func TestRecordAgentSessionCoordinationWithEventReturnsExactPersistedRowOnRepeatedStatus(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session-runtime-repeat",
		Title:       "Session Runtime Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session-runtime-repeat")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-session-runtime-repeat",
		AgentID:     "agent-session-runtime-repeat",
		OwnerUserID: "developer",
		DisplayName: "Session Runtime Repeat Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, "task-session-runtime-repeat", "node-session-runtime-repeat")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-session-runtime-repeat",
		TaskID:      "task-session-runtime-repeat",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-session-runtime-repeat", "task-session-runtime-repeat", "agent-session-runtime-repeat")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-session-runtime-repeat",
		SessionID:   "sess-runtime-repeat",
		AgentID:     "agent-session-runtime-repeat",
		TaskID:      "task-session-runtime-repeat",
		Summary:     "Starting repeated status path",
		UpdatedAt:   "2026-03-28T10:00:00Z",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: "ws-session-runtime-repeat",
		SessionID:   "sess-runtime-repeat",
		AgentID:     "agent-session-runtime-repeat",
		TaskID:      "task-session-runtime-repeat",
		Summary:     "First status update",
		UpdatedAt:   "2026-03-28T10:01:00Z",
	}); err != nil {
		t.Fatalf("record first status update: %v", err)
	}

	state, exactEvent, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: "ws-session-runtime-repeat",
		SessionID:   "sess-runtime-repeat",
		AgentID:     "agent-session-runtime-repeat",
		TaskID:      "task-session-runtime-repeat",
		Summary:     "Second status update",
		UpdatedAt:   "2026-03-28T10:02:00Z",
	})
	if err != nil {
		t.Fatalf("record second status update with event: %v", err)
	}
	if state.UpdateType != model.SessionEventStatus || state.Summary != "Second status update" {
		t.Fatalf("expected returned state to match second status update, got %+v", state)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-session-runtime-repeat",
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    "sess-runtime-repeat",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session status runtime events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected two session.status runtime events, got %+v", events)
	}
	if exactEvent != events[0] {
		t.Fatalf("expected RecordAgentSessionCoordinationWithEvent to return latest persisted row, returned=%+v persisted=%+v", exactEvent, events[0])
	}
	if exactEvent.EventID == events[1].EventID || exactEvent.IngestSeq <= events[1].IngestSeq {
		t.Fatalf("expected repeated status update to return newer runtime row, latest=%+v previous=%+v", exactEvent, events[1])
	}

	var payload sqlite.AgentSessionStateRecord
	decodeRuntimePayload(t, exactEvent.PayloadJSON, &payload)
	if payload.Summary != "Second status update" || payload.UpdateType != model.SessionEventStatus || payload.UpdatedAt != "2026-03-28T10:02:00Z" {
		t.Fatalf("unexpected exact runtime payload for repeated status update: %+v", payload)
	}
}

func TestRecordAgentSessionCoordinationPersistsRuntimeMetrics(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session-metrics",
		Title:       "Session Metrics",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session-metrics")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-session-metrics",
		AgentID:     "agent-session-metrics",
		OwnerUserID: "developer",
		DisplayName: "Session Metrics Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, "task-session-metrics", "node-session-metrics")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-session-metrics",
		TaskID:      "task-session-metrics",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-session-metrics", "task-session-metrics", "agent-session-metrics")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-session-metrics",
		SessionID:   "sess-session-metrics",
		AgentID:     "agent-session-metrics",
		TaskID:      "task-session-metrics",
		Summary:     "Starting metrics session",
		UpdatedAt:   "2026-03-28T10:00:00Z",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventStatus,
		WorkspaceID:       "ws-session-metrics",
		SessionID:         "sess-session-metrics",
		AgentID:           "agent-session-metrics",
		TaskID:            "task-session-metrics",
		Summary:           "Status with metrics",
		Iterations:        2,
		TotalInputTokens:  100,
		TotalOutputTokens: 40,
		ToolCalls:         3,
		UpdatedAt:         "2026-03-28T10:01:00Z",
	})
	if err != nil {
		t.Fatalf("record status metrics: %v", err)
	}
	if state.Iterations != 2 || state.TotalInputTokens != 100 || state.TotalOutputTokens != 40 || state.ToolCalls != 3 {
		t.Fatalf("state metrics after status = %+v", state)
	}

	state, err = store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventStatus,
		WorkspaceID:       "ws-session-metrics",
		SessionID:         "sess-session-metrics",
		AgentID:           "agent-session-metrics",
		TaskID:            "task-session-metrics",
		Summary:           "Lower repeated metrics",
		Iterations:        1,
		TotalInputTokens:  10,
		TotalOutputTokens: 4,
		ToolCalls:         1,
		UpdatedAt:         "2026-03-28T10:02:00Z",
	})
	if err != nil {
		t.Fatalf("record lower repeated status metrics: %v", err)
	}
	if state.Iterations != 2 || state.TotalInputTokens != 100 || state.TotalOutputTokens != 40 || state.ToolCalls != 3 {
		t.Fatalf("lower repeated status reduced metrics: %+v", state)
	}

	state, err = store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventEnd,
		WorkspaceID:       "ws-session-metrics",
		SessionID:         "sess-session-metrics",
		AgentID:           "agent-session-metrics",
		TaskID:            "task-session-metrics",
		Summary:           "Done with metrics",
		Iterations:        4,
		TotalInputTokens:  120,
		TotalOutputTokens: 70,
		ToolCalls:         5,
		UpdatedAt:         "2026-03-28T10:03:00Z",
	})
	if err != nil {
		t.Fatalf("record end metrics: %v", err)
	}
	if state.Status != "ENDED" || state.Iterations != 4 || state.TotalInputTokens != 120 || state.TotalOutputTokens != 70 || state.ToolCalls != 5 {
		t.Fatalf("state metrics after end = %+v", state)
	}
	record, err := store.GetAgentSession(ctx, "sess-session-metrics")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if record.Iterations != 4 || record.TotalInputTokens != 120 || record.TotalOutputTokens != 70 || record.ToolCalls != 5 {
		t.Fatalf("stored session metrics = %+v", record)
	}
}

func TestTakeOverAgentSessionAppendsRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-takeover-runtime-event",
		Title:       "Session Takeover Runtime Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-takeover-runtime-event")
	for _, agentID := range []string{"agent-source-runtime", "agent-target-runtime"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-takeover-runtime-event",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createSingleNodeTask(t, ctx, store, "task-takeover-runtime", "node-takeover-runtime")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-takeover-runtime-event",
		TaskID:      "task-takeover-runtime",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-takeover-runtime-event", "task-takeover-runtime", "agent-source-runtime")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-takeover-runtime-event",
		SessionID:   "sess-source-runtime",
		AgentID:     "agent-source-runtime",
		TaskID:      "task-takeover-runtime",
		Summary:     "Owning the rollout",
		HandoffTo:   "agent-target-runtime",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	if _, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     "ws-takeover-runtime-event",
		SessionID:       "sess-source-runtime",
		TakeoverAgentID: "agent-target-runtime",
		Summary:         "Fresh owner required",
	}); err != nil {
		t.Fatalf("take over agent session: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-takeover-runtime-event",
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    "sess-source-runtime",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list takeover runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 takeover runtime event, got %+v", events)
	}
	if events[0].AgentID != "agent-target-runtime" || events[0].ActorID != "agent-target-runtime" {
		t.Fatalf("expected takeover agent to be actor, got %+v", events[0])
	}
	if events[0].TaskID != "task-takeover-runtime" || events[0].SessionID != "sess-source-runtime" {
		t.Fatalf("expected task/session ids on takeover event, got %+v", events[0])
	}
}

func TestTakeOverAgentSessionWithEventReturnsExactPersistedRuntimeRow(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-takeover-runtime-with-event",
		Title:       "Session Takeover Runtime With Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-takeover-runtime-with-event")
	for _, agentID := range []string{"agent-source-runtime-with-event", "agent-target-runtime-with-event"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-takeover-runtime-with-event",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createSingleNodeTask(t, ctx, store, "task-takeover-runtime-with-event", "node-takeover-runtime-with-event")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-takeover-runtime-with-event",
		TaskID:      "task-takeover-runtime-with-event",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-takeover-runtime-with-event", "task-takeover-runtime-with-event", "agent-source-runtime-with-event")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-takeover-runtime-with-event",
		SessionID:   "sess-source-runtime-with-event",
		AgentID:     "agent-source-runtime-with-event",
		TaskID:      "task-takeover-runtime-with-event",
		Summary:     "Owning the rollout before exact-row takeover",
		HandoffTo:   "agent-target-runtime-with-event",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	record, exactEvent, err := store.TakeOverAgentSessionWithEvent(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     "ws-takeover-runtime-with-event",
		SessionID:       "sess-source-runtime-with-event",
		TakeoverAgentID: "agent-target-runtime-with-event",
		Summary:         "Fresh owner required for exact-row path",
	})
	if err != nil {
		t.Fatalf("take over agent session with event: %v", err)
	}

	persisted := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-takeover-runtime-with-event",
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    "sess-source-runtime-with-event",
	})
	if exactEvent != persisted {
		t.Fatalf("expected TakeOverAgentSessionWithEvent to return persisted runtime row, returned=%+v persisted=%+v", exactEvent, persisted)
	}
	if record.SourceState.SessionID != "sess-source-runtime-with-event" || record.SuccessorState.TaskID != "task-takeover-runtime-with-event" {
		t.Fatalf("unexpected takeover record returned with exact runtime row: %+v", record)
	}
}

func TestRecordAgentSessionCoordinationRejectsPostTakeoverStatusOnSourceSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-session-takeover-guard",
		Title:       "Session Takeover Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-session-takeover-guard")
	for _, agentID := range []string{"agent-source-guard", "agent-target-guard"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-session-takeover-guard",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createSingleNodeTask(t, ctx, store, "task-session-takeover-guard", "node-session-takeover-guard")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-session-takeover-guard",
		TaskID:      "task-session-takeover-guard",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, "ws-session-takeover-guard", "task-session-takeover-guard", "agent-source-guard")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-session-takeover-guard",
		SessionID:   "sess-source-guard",
		AgentID:     "agent-source-guard",
		TaskID:      "task-session-takeover-guard",
		Summary:     "Source owner starts work",
		HandoffTo:   "agent-target-guard",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	takeover, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     "ws-session-takeover-guard",
		SessionID:       "sess-source-guard",
		TakeoverAgentID: "agent-target-guard",
		Summary:         "Need a fresh owner",
	})
	if err != nil {
		t.Fatalf("take over agent session: %v", err)
	}

	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: "ws-session-takeover-guard",
		SessionID:   "sess-source-guard",
		AgentID:     "agent-source-guard",
		TaskID:      "task-session-takeover-guard",
		Summary:     "Stale source tries to keep writing after takeover",
	}); !errors.Is(err, sqlite.ErrSessionNotActive) {
		t.Fatalf("expected ErrSessionNotActive for stale post-takeover update, got %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-session-takeover-guard",
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    "sess-source-guard",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list source session status runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no new session.status event for source session after takeover, got %+v", events)
	}

	sourceState, err := store.GetAgentSessionState(ctx, "ws-session-takeover-guard", "sess-source-guard")
	if err != nil {
		t.Fatalf("get source session state: %v", err)
	}
	if sourceState.Status != model.SessionStatusEnded {
		t.Fatalf("expected source session to remain ended, got %+v", sourceState)
	}

	activeSessions, err := store.ListWorkspaceSessionStates(ctx, "ws-session-takeover-guard", true, 10)
	if err != nil {
		t.Fatalf("list active session states: %v", err)
	}
	if len(activeSessions) != 1 || activeSessions[0].SessionID != takeover.SuccessorState.SessionID {
		t.Fatalf("expected only successor session to remain active after stale update rejection, got %+v", activeSessions)
	}
}

func TestRecordAgentSessionCoordinationRejectsPostTakeoverEndOnSourceSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID     = "ws-session-takeover-end-guard"
		sourceAgent     = "agent-source-end-guard"
		targetAgent     = "agent-target-end-guard"
		taskID          = "task-session-takeover-end-guard"
		sourceSessionID = "sess-source-end-guard"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Takeover End Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{sourceAgent, targetAgent} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-session-takeover-end-guard")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, sourceAgent)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "Source owner starts work before takeover",
		HandoffTo:   targetAgent,
		UpdatedAt:   "2026-04-21T12:00:00Z",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	takeover, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     workspaceID,
		SessionID:       sourceSessionID,
		TakeoverAgentID: targetAgent,
		Summary:         "Fresh owner takes over",
		UpdatedAt:       "2026-04-21T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("take over agent session: %v", err)
	}
	if takeover.SourceState.Status != model.SessionStatusEnded {
		t.Fatalf("expected source session to be ended by takeover, got %+v", takeover.SourceState)
	}

	sourceBefore, err := store.GetAgentSessionState(ctx, workspaceID, sourceSessionID)
	if err != nil {
		t.Fatalf("get source session before stale end: %v", err)
	}
	beforeEndEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventEnd,
		EntityType:  "agent_session",
		EntityID:    sourceSessionID,
		Limit:       10,
	})
	beforeEndUpdates := countAgentSessionUpdatesByTypeForA310(t, ctx, store, workspaceID, sourceAgent, model.SessionEventEnd)

	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "Stale source tries to close again after takeover",
		UpdatedAt:   "2026-04-21T12:02:00Z",
	}); !errors.Is(err, sqlite.ErrSessionNotActive) {
		t.Fatalf("expected ErrSessionNotActive for stale post-takeover end, got %v", err)
	}

	sourceAfter, err := store.GetAgentSessionState(ctx, workspaceID, sourceSessionID)
	if err != nil {
		t.Fatalf("get source session after stale end: %v", err)
	}
	if sourceAfter.Status != sourceBefore.Status || sourceAfter.Summary != sourceBefore.Summary ||
		sourceAfter.UpdatedAt != sourceBefore.UpdatedAt || sourceAfter.CompletedAt != sourceBefore.CompletedAt {
		t.Fatalf("expected source session to remain unchanged after stale end reject, before=%+v after=%+v", sourceBefore, sourceAfter)
	}
	if afterEndEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventEnd,
		EntityType:  "agent_session",
		EntityID:    sourceSessionID,
		Limit:       10,
	}); afterEndEvents != beforeEndEvents {
		t.Fatalf("expected no session.end runtime event after stale end reject, before=%d after=%d", beforeEndEvents, afterEndEvents)
	}
	if afterEndUpdates := countAgentSessionUpdatesByTypeForA310(t, ctx, store, workspaceID, sourceAgent, model.SessionEventEnd); afterEndUpdates != beforeEndUpdates {
		t.Fatalf("expected no session.end agent_update after stale end reject, before=%d after=%d", beforeEndUpdates, afterEndUpdates)
	}

	activeSessions, err := store.ListWorkspaceSessionStates(ctx, workspaceID, true, 10)
	if err != nil {
		t.Fatalf("list active session states: %v", err)
	}
	if len(activeSessions) != 1 || activeSessions[0].SessionID != takeover.SuccessorState.SessionID {
		t.Fatalf("expected only successor session to remain active after stale end rejection, got %+v", activeSessions)
	}
}

func countAgentSessionUpdatesByTypeForA310(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, updateType string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agent_updates WHERE workspace_id = ? AND agent_id = ? AND update_type = ?`,
		workspaceID,
		agentID,
		updateType,
	).Scan(&count); err != nil {
		t.Fatalf("count agent_updates for %s/%s/%s: %v", workspaceID, agentID, updateType, err)
	}
	return count
}
