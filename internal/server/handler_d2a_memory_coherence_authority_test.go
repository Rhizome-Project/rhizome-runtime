package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryCoherenceSnapshotRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store, h, ctx, workspaceID, agentID, _ := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-coherence-missing-authority", "agent-handler-d2a-memory-coherence-missing-authority", "doc-handler-d2a-memory-coherence-missing-authority")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	result, rpcErr := callWorkspaceMemoryCoherenceSnapshotRaw(t, h, context.Background(), mustJSONRaw(workspaceMemoryCoherenceReportParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.memory.coherence.snapshot")
	assertNoHandlerMemoryCoherenceEvents(t, ctx, store, workspaceID)
}

func TestWorkspaceMemoryCoherenceSnapshotRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store, h, ctx, workspaceID, agentID, _ := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-coherence-stale-authority", "agent-handler-d2a-memory-coherence-stale-authority", "doc-handler-d2a-memory-coherence-stale-authority")
	current, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-4002")

	result, rpcErr := callWorkspaceMemoryCoherenceSnapshotRaw(t, h, context.Background(), mustJSONRaw(workspaceMemoryCoherenceReportParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.coherence.snapshot")
	assertNoHandlerMemoryCoherenceEvents(t, ctx, store, workspaceID)
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
}

func TestWorkspaceMemoryCoherenceReportRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store, h, ctx, workspaceID, agentID, _ := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-coherence-report-mismatch", "agent-handler-d2a-memory-coherence-report-mismatch", "doc-handler-d2a-memory-coherence-report-mismatch")

	result, rpcErr := callWorkspaceMemoryCoherenceReportRaw(t, h, testAuthContext("ws-other-memory-coherence", "human", "developer"), mustJSONRaw(workspaceMemoryCoherenceReportParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
	assertNoHandlerMemoryCoherenceEvents(t, ctx, store, workspaceID)
}

func TestWorkspaceMemoryCoherenceScopeRejectsAgentPrincipalMismatch(t *testing.T) {
	store, h, ctx, workspaceID, agentID, _ := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-coherence-scope-mismatch", "agent-handler-d2a-memory-coherence-scope-mismatch", "doc-handler-d2a-memory-coherence-scope-mismatch")

	result, rpcErr := callWorkspaceMemoryCoherenceScopeRaw(t, h, withTestAgentPrincipal(context.Background(), workspaceID, "agent-other"), mustJSONRaw(workspaceMemoryCoherenceScopeParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	}))
	if rpcErr == nil {
		t.Fatal("expected permission denied for agent principal mismatch")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
	assertNoHandlerMemoryCoherenceEvents(t, ctx, store, workspaceID)
}

func TestWorkspaceMemoryCoherenceSnapshotRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store, h, ctx, workspaceID, agentID, _ := seedHandlerOpenMemoryInvalidation(t, "ws-handler-d2a-memory-coherence-snapshot-mismatch", "agent-handler-d2a-memory-coherence-snapshot-mismatch", "doc-handler-d2a-memory-coherence-snapshot-mismatch")

	result, rpcErr := callWorkspaceMemoryCoherenceSnapshotRaw(t, h, testAuthContext("ws-other-memory-coherence", "human", "developer"), mustJSONRaw(workspaceMemoryCoherenceReportParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
	assertNoHandlerMemoryCoherenceEvents(t, ctx, store, workspaceID)
}

func assertNoHandlerMemoryCoherenceEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.coherence_snapshot",
		EntityType:  "memory_coherence",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory coherence runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no memory coherence runtime events, got %+v", events)
	}
}
