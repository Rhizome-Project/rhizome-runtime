package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordAgentSessionCoordinationWithEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-start-missing-authority"
		agentID     = "agent-d2a-session-start-missing-authority"
		sessionID   = "sess-d2a-session-start-missing-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Start Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Session Start Missing Authority",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	_, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Session start should fail closed without authority",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing workspace authority reject, got %+v", reject)
	}

	assertAgentSessionCount(t, ctx, store, workspaceID, 0)
	assertAgentUpdateCount(t, ctx, store, workspaceID, 0)
	assertNoSessionRuntimeEvents(t, ctx, store, workspaceID, sessionID, "start")
	if afterUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestTakeOverAgentSessionWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-takeover-stale-authority"
		sourceAgent = "agent-d2a-session-takeover-source"
		targetAgent = "agent-d2a-session-takeover-target"
		sessionID   = "sess-d2a-session-takeover-source"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Takeover Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{sourceAgent, targetAgent} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "tests",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	sourceState, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     sourceAgent,
		Summary:     "Source session ready for takeover",
		HandoffTo:   targetAgent,
	})
	if err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeTakeoverEvents := countSessionRuntimeEvents(t, ctx, store, workspaceID, sessionID, "takeover")
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2501")

	_, _, err = store.TakeOverAgentSessionWithEvent(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		TakeoverAgentID: targetAgent,
		Summary:         "Takeover should fail closed under stale authority",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale workspace authority reject, got %+v", reject)
	}

	afterSourceState, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("reload source session: %v", err)
	}
	if afterSourceState.Status != sourceState.Status || afterSourceState.UpdatedAt != sourceState.UpdatedAt {
		t.Fatalf("expected stale authority reject not to mutate source session, before=%+v after=%+v", sourceState, afterSourceState)
	}
	if got := countSessionRuntimeEvents(t, ctx, store, workspaceID, sessionID, "takeover"); got != beforeTakeoverEvents {
		t.Fatalf("expected no new session.takeover events after authority reject, before=%d after=%d", beforeTakeoverEvents, got)
	}
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestRecordAgentSessionBlockedWithEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-blocked-missing-authority"
		agentID     = "agent-d2a-session-blocked-missing-authority"
		sessionID   = "sess-d2a-session-blocked-missing-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Blocked Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Session Blocked Missing Authority",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	startState, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Session starts before missing-authority blocked update",
	})
	if err != nil {
		t.Fatalf("seed session start: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeBlockedEvents := countSessionRuntimeEvents(t, ctx, store, workspaceID, sessionID, "blocked")

	_, _, err = store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Blocked update should fail closed without authority",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing workspace authority reject, got %+v", reject)
	}

	afterState, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("reload session state: %v", err)
	}
	if afterState.Status != startState.Status || afterState.UpdatedAt != startState.UpdatedAt {
		t.Fatalf("expected missing-authority reject not to mutate session state, before=%+v after=%+v", startState, afterState)
	}
	if got := countSessionRuntimeEvents(t, ctx, store, workspaceID, sessionID, "blocked"); got != beforeBlockedEvents {
		t.Fatalf("expected no new session.blocked events after authority reject, before=%d after=%d", beforeBlockedEvents, got)
	}
	if afterUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func assertAgentSessionCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count agent_sessions rows: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d agent_sessions rows, got %d", want, got)
	}
}

func countSessionRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sessionID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "session." + eventType,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session.%s runtime events: %v", eventType, err)
	}
	return len(events)
}

func assertNoSessionRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sessionID, eventType string) {
	t.Helper()

	if got := countSessionRuntimeEvents(t, ctx, store, workspaceID, sessionID, eventType); got != 0 {
		t.Fatalf("expected no session.%s runtime events, got %d", eventType, got)
	}
}
