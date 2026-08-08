package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentSessionStartRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-start-missing-authority-rpc"
		agentID     = "agent-d2a-session-start-missing-authority-rpc"
		sessionID   = "sess-d2a-session-start-missing-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Start Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	result, rpcErr := callAgentSessionStartRaw(t, h, ctx, mustMarshalJSON(t, agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Session start should fail closed without authority",
		Status:      model.SessionStatusActive,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertAgentUpdateAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "agent.session.start")
	assertServerAgentUpdateCount(t, ctx, store, workspaceID, 0)
	assertNoServerAgentUpdateRuntimeEvents(t, ctx, store, workspaceID, "agent_session", sessionID, model.SessionEventStart)
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestAgentSessionTakeoverRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-takeover-stale-authority-rpc"
		sourceAgent = "agent-d2a-session-takeover-source-rpc"
		targetAgent = "agent-d2a-session-takeover-target-rpc"
		sessionID   = "sess-d2a-session-takeover-source-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Takeover Stale Authority RPC",
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
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     sourceAgent,
		Summary:     "Source session ready for takeover",
		HandoffTo:   targetAgent,
	}); err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2502")

	result, rpcErr := callAgentSessionTakeoverRaw(t, h, ctx, mustMarshalJSON(t, agentSessionTakeoverParams{
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		TakeoverAgentID: targetAgent,
		Summary:         "Takeover should fail closed under stale authority",
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	assertAgentUpdateAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "agent.session.takeover")
	assertNoServerAgentUpdateRuntimeEvents(t, ctx, store, workspaceID, "agent_session", sessionID, "session.takeover")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestAgentSessionBlockedRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-blocked-missing-authority-rpc"
		agentID     = "agent-d2a-session-blocked-missing-authority-rpc"
		sessionID   = "sess-d2a-session-blocked-missing-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Blocked Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Session starts before missing-authority blocked update",
	}); err != nil {
		t.Fatalf("seed session start: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	result, rpcErr := callAgentSessionBlockedRaw(t, h, ctx, mustMarshalJSON(t, agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Blocked update should fail closed without authority",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "need approval"}},
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertAgentUpdateAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "agent.session.blocked")
	assertNoServerAgentUpdateRuntimeEvents(t, ctx, store, workspaceID, "agent_session", sessionID, model.SessionEventBlocked)
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}
