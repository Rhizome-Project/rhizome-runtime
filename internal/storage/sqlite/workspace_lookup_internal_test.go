package sqlite

import (
	"context"
	"errors"
	"testing"
)

func TestGetWorkspaceByTitleExactMatchAndAmbiguous(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: "ws-title-a",
		Title:       "Shared Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace a: %v", err)
	}

	workspace, err := store.GetWorkspaceByTitle(ctx, "shared workspace")
	if err != nil {
		t.Fatalf("get workspace by title: %v", err)
	}
	if workspace.WorkspaceID != "ws-title-a" {
		t.Fatalf("expected exact title lookup to return ws-title-a, got %+v", workspace)
	}

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: "ws-title-b",
		Title:       "Shared Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace b: %v", err)
	}

	if _, err := store.GetWorkspaceByTitle(ctx, "Shared Workspace"); !errors.Is(err, ErrWorkspaceRefAmbiguous) {
		t.Fatalf("expected ambiguous workspace title lookup, got %v", err)
	}
}

func TestGetWorkspaceByTitlePrefersSingleActiveMatch(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	for _, workspace := range []WorkspaceCreateInput{
		{
			WorkspaceID: "ws-title-archived",
			Title:       "Rhizome Main",
			CreatedBy:   "developer",
			Status:      "ARCHIVED",
		},
		{
			WorkspaceID: "ws-title-active",
			Title:       "Rhizome Main",
			CreatedBy:   "developer",
			Status:      "ACTIVE",
		},
	} {
		if err := store.CreateWorkspace(ctx, workspace); err != nil {
			t.Fatalf("create workspace %s: %v", workspace.WorkspaceID, err)
		}
	}

	workspace, err := store.GetWorkspaceByTitle(ctx, "rhizome main")
	if err != nil {
		t.Fatalf("get workspace by title: %v", err)
	}
	if workspace.WorkspaceID != "ws-title-active" {
		t.Fatalf("expected active workspace to win title lookup, got %+v", workspace)
	}
}
