package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordWorkspaceArtifactWithEffectsRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-artifact-record-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Artifact Record Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	_, _, _, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		Title:       "Missing authority artifact",
		ArtifactRef: "artifact://missing-authority",
		Kind:        "reference",
		ContentType: "text/plain",
		CreatedBy:   "tests",
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

	assertWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, "", "workspace_artifact.created")
	if afterUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestRecordWorkspaceArtifactWithEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-artifact-record-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Artifact Record Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2301")

	_, _, _, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		Title:       "Stale authority artifact",
		ArtifactRef: "artifact://stale-authority",
		Kind:        "reference",
		ContentType: "text/plain",
		CreatedBy:   "tests",
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

	assertWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, "", "workspace_artifact.created")
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		t.Fatalf("load workspace updated_at: %v", err)
	}
	return updatedAt
}

func assertWorkspaceArtifactCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_artifacts WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count workspace_artifacts rows: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workspace_artifacts rows, got %d", want, got)
	}
}

func assertNoWorkspaceArtifactRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, entityID, eventType string) {
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
