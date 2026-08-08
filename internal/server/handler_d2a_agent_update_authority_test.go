package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentUpdatePostRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d2a-agent-update-missing-authority-rpc"
		agentID     = "agent-d2a-agent-update-missing-authority-rpc"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Update Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Agent Update Missing Authority RPC",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(agentUpdatePostParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "progress",
		Summary:       "Agent update should fail closed without authority",
		PayloadJSON:   `{"step":"missing-authority"}`,
		RequiresHuman: true,
	})
	if err != nil {
		t.Fatalf("marshal agent update params: %v", err)
	}

	result, rpcErr := h.agentUpdatePost(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertAgentUpdateAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "agent.update.post")
	assertServerAgentUpdateCount(t, ctx, store, workspaceID, 0)
	assertNoServerAgentUpdateRuntimeEvents(t, ctx, store, workspaceID, "agent_update", "", "agent_update.posted")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestAgentUpdatePostRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d2a-agent-update-stale-authority-rpc"
		agentID     = "agent-d2a-agent-update-stale-authority-rpc"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Update Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Agent Update Stale Authority RPC",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2402")

	raw, err := json.Marshal(agentUpdatePostParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "progress",
		Summary:       "Agent update should fail closed under stale authority",
		PayloadJSON:   `{"step":"stale-authority"}`,
		RequiresHuman: true,
	})
	if err != nil {
		t.Fatalf("marshal agent update params: %v", err)
	}

	result, rpcErr := h.agentUpdatePost(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	assertAgentUpdateAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "agent.update.post")
	assertServerAgentUpdateCount(t, ctx, store, workspaceID, 0)
	assertNoServerAgentUpdateRuntimeEvents(t, ctx, store, workspaceID, "agent_update", "", "agent_update.posted")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func assertAgentUpdateAuthorityRejectDetails(t *testing.T, rpcErr *RPCError, wantReject sqlite.AuthorityRejectCode, wantSurface string) {
	t.Helper()

	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(wantReject) || details["surface"] != wantSurface {
		t.Fatalf("unexpected authority reject details %+v", details)
	}
}

func assertServerAgentUpdateCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_updates WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count agent_updates rows: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d agent_updates rows, got %d", want, got)
	}
}

func assertNoServerAgentUpdateRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, entityType, entityID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  entityType,
		EntityID:    entityID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events: %v", eventType, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events, got %+v", eventType, events)
	}
}
