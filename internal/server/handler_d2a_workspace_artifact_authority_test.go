package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceArtifactWriteRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-d2a-artifact-write-missing-authority-rpc"
	ctx := testAuthContext(workspaceID, "agent", "agent-artifact")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Artifact Write Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-artifact",
		OwnerUserID: "tests",
		DisplayName: "Artifact Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceArtifactWriteParams{
		WorkspaceID: workspaceID,
		Title:       "Missing authority artifact",
		ArtifactRef: "artifact://missing-authority-rpc",
		Kind:        "reference",
		ContentType: "text/plain",
		CreatedBy:   "agent-artifact",
	})
	if err != nil {
		t.Fatalf("marshal workspace.artifact.write params: %v", err)
	}

	result, rpcErr := h.workspaceArtifactWrite(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing-authority reject, got %+v", result)
	}
	assertWorkspaceArtifactAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.artifact.write")
	assertServerWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoServerWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, "", "workspace_artifact.created")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to remain %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceArtifactWriteRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-d2a-artifact-write-stale-authority-rpc"
	ctx := testAuthContext(workspaceID, "agent", "agent-artifact")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Artifact Write Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-artifact",
		OwnerUserID: "tests",
		DisplayName: "Artifact Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2302")

	raw, err := json.Marshal(workspaceArtifactWriteParams{
		WorkspaceID: workspaceID,
		Title:       "Stale authority artifact",
		ArtifactRef: "artifact://stale-authority-rpc",
		Kind:        "reference",
		ContentType: "text/plain",
		CreatedBy:   "agent-artifact",
	})
	if err != nil {
		t.Fatalf("marshal workspace.artifact.write params: %v", err)
	}

	result, rpcErr := h.workspaceArtifactWrite(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	assertWorkspaceArtifactAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.artifact.write")
	assertServerWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoServerWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, "", "workspace_artifact.created")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func assertWorkspaceArtifactAuthorityRejectDetails(t *testing.T, rpcErr *RPCError, wantReject sqlite.AuthorityRejectCode, wantSurface string) {
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

func assertServerWorkspaceArtifactCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_artifacts WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count workspace_artifacts rows: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workspace_artifacts rows, got %d", want, got)
	}
}

func assertNoServerWorkspaceArtifactRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, entityID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "workspace_artifact",
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
