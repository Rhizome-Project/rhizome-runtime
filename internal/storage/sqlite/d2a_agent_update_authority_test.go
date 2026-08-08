package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordAgentUpdateWithEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-agent-update-missing-authority"
		agentID     = "agent-d2a-agent-update-missing-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Update Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Agent Update Missing Authority",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	_, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:      "update-missing-authority",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "progress",
		Summary:       "Agent update should fail closed without authority",
		PayloadJSON:   `{"step":"missing-authority"}`,
		RequiresHuman: true,
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

	assertAgentUpdateCount(t, ctx, store, workspaceID, 0)
	assertNoAgentUpdateRuntimeEvents(t, ctx, store, workspaceID, "update-missing-authority")
	if afterUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestRecordAgentUpdateWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-agent-update-stale-authority"
		agentID     = "agent-d2a-agent-update-stale-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Update Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Agent Update Stale Authority",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2401")

	_, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:      "update-stale-authority",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "progress",
		Summary:       "Agent update should fail closed under stale authority",
		PayloadJSON:   `{"step":"stale-authority"}`,
		RequiresHuman: true,
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

	assertAgentUpdateCount(t, ctx, store, workspaceID, 0)
	assertNoAgentUpdateRuntimeEvents(t, ctx, store, workspaceID, "update-stale-authority")
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceArtifactAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func assertAgentUpdateCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_updates WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count agent_updates rows: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d agent_updates rows, got %d", want, got)
	}
}

func assertNoAgentUpdateRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, entityID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		EntityID:    entityID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_update.posted runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no agent_update.posted runtime events, got %+v", events)
	}
}
