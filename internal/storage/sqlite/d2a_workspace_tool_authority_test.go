package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRegisterWorkspaceToolRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-tool-register-missing-authority"
		toolID      = "tool-d2a-missing-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Tool Register Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		DisplayName: "Authority Missing Tool",
		OwnerUserID: "operator-a",
		Kind:        model.ToolKindOther,
		Status:      model.ToolStatusActive,
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	if _, err := store.GetWorkspaceTool(ctx, workspaceID, toolID); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected tool row to be absent after authority reject, got %v", err)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no workspace_tool.registered events after authority reject, got %d", got)
	}
}

func TestRemoveWorkspaceToolRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-tool-remove-stale-authority"
		toolID      = "tool-d2a-stale-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Tool Remove Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		DisplayName: "Authority Stale Tool",
		OwnerUserID: "operator-a",
		Kind:        model.ToolKindOther,
		Status:      model.ToolStatusActive,
	}); err != nil {
		t.Fatalf("register workspace tool: %v", err)
	}
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-3011")

	existed, err := store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		RemovedBy:   "operator-b",
	})
	if existed {
		t.Fatal("expected stale-authority remove to report no deletion")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}

	if _, err := store.GetWorkspaceTool(ctx, workspaceID, toolID); err != nil {
		t.Fatalf("expected tool row to remain after stale-authority reject: %v", err)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no workspace_tool.removed events after stale-authority reject, got %d", got)
	}
	assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
}

func TestWorkspaceToolLifecycleRuntimeEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-tool-authority-metadata"
		toolID      = "tool-d2a-authority-metadata"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Tool Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		DisplayName: "Authority Metadata Tool",
		OwnerUserID: "operator-a",
		Kind:        model.ToolKindIntegration,
		Status:      model.ToolStatusActive,
		Endpoint:    "mcp:notion",
	}); err != nil {
		t.Fatalf("register workspace tool: %v", err)
	}
	if _, err := store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		RemovedBy:   "operator-a",
	}); err != nil {
		t.Fatalf("remove workspace tool: %v", err)
	}

	registeredEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_tool.registered events: %v", err)
	}
	if len(registeredEvents) != 1 {
		t.Fatalf("expected one workspace_tool.registered event, got %d", len(registeredEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, registeredEvents[0], authority)

	removedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_tool.removed events: %v", err)
	}
	if len(removedEvents) != 1 {
		t.Fatalf("expected one workspace_tool.removed event, got %d", len(removedEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, removedEvents[0], authority)
}
