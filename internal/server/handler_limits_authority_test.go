package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentLimitsGetRejectsMismatchedAgentPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-limits-agent-principal-binding"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Limits Agent Principal Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register %s: %v", agentID, err)
		}
	}

	result, rpcErr := h.agentLimitsGet(ctx, mustJSONRaw(agentLimitsGetParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched limits principal to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no limits result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected limits mismatch error %+v", rpcErr)
	}
}

func TestLimitsReportRejectsMismatchedAgentPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-limits-report-principal-binding"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	seedLimitReportWorkspace(t, ctx, store, workspaceID, "agent-a", "grp-agent-a")
	seedLimitReportAgent(t, ctx, store, workspaceID, "agent-b", "grp-agent-b")

	result, rpcErr := h.limitsReport(ctx, mustJSONRaw(limitsReportParams{
		GroupID:         "grp-agent-b",
		AgentID:         "agent-b",
		DailyRemaining:  7,
		WeeklyRemaining: 70,
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched limits report principal to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no limits report result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected limits report mismatch error %+v", rpcErr)
	}
}

func TestLimitsReportRejectsCrossWorkspaceBudgetScope(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	ctxA := testAuthContext("ws-limits-report-scope-a", "agent", "agent-a")
	seedLimitReportWorkspace(t, ctxA, store, "ws-limits-report-scope-a", "agent-a", "grp-limits-scope-a")

	ctxB := testAuthContext("ws-limits-report-scope-b", "agent", "agent-a")
	seedLimitReportWorkspace(t, ctxB, store, "ws-limits-report-scope-b", "agent-a", "grp-limits-scope-b")

	result, rpcErr := h.limitsReport(ctxA, mustJSONRaw(limitsReportParams{
		GroupID:         "grp-limits-scope-b",
		AgentID:         "agent-a",
		DailyRemaining:  11,
		WeeklyRemaining: 110,
	}))
	if rpcErr == nil {
		t.Fatal("expected cross-workspace limits report to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no cross-workspace limits report result, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "budget scope mismatch: token identity does not match group_id" {
		t.Fatalf("unexpected cross-workspace limits report error %+v", rpcErr)
	}

	groupB, err := store.GetLimitGroup(ctxB, "ws-limits-report-scope-b", "grp-limits-scope-b")
	if err != nil {
		t.Fatalf("get untouched group-b: %v", err)
	}
	if groupB.DailyRemaining == 11 || groupB.WeeklyRemaining == 110 {
		t.Fatalf("expected rejected cross-workspace report not to mutate group-b, got %+v", groupB)
	}
}

func seedLimitReportWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, groupID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	seedLimitReportAgent(t, ctx, store, workspaceID, agentID, groupID)
}

func seedLimitReportAgent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, groupID string) {
	t.Helper()

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register %s/%s: %v", workspaceID, agentID, err)
	}
	if err := store.AssignAgentLimitGroup(ctx, workspaceID, agentID, groupID, groupID); err != nil {
		t.Fatalf("assign limit group %s/%s/%s: %v", workspaceID, agentID, groupID, err)
	}
}
