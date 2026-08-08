package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceProjectRepositoryCLIUpsertAndList(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-project-repository-cli"
		projectID   = "project-repository-cli"
		repoID      = "repo-canonical"
		actorID     = "developer"
	)
	createCLITestWorkspace(t, workspaceID)

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Repository CLI",
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	upsertOut, err := captureStdout(t, func() error {
		return runWorkspaceProject([]string{
			"repository",
			"upsert",
			"--workspace-id", workspaceID,
			"--project-id", projectID,
			"--actor-id", actorID,
			"--repo-id", repoID,
			"--remote-url", "file:///tmp/project-repository-cli.git",
			"--remote-kind", "local",
			"--owner", "developer",
			"--name", "project-repository-cli",
			"--default-branch", "main",
			"--repo-status", "READY",
			"--canonical",
		})
	})
	if err != nil {
		t.Fatalf("workspace project repository upsert failed: %v", err)
	}
	var upsertPayload struct {
		RuntimeEventID string                         `json:"runtime_event_id"`
		Repository     sqlite.ProjectRepositoryRecord `json:"repository"`
	}
	if err := json.Unmarshal([]byte(upsertOut), &upsertPayload); err != nil {
		t.Fatalf("decode upsert output: %v; output=%q", err, upsertOut)
	}
	if upsertPayload.RuntimeEventID == "" {
		t.Fatalf("expected runtime event id in upsert output: %+v", upsertPayload)
	}
	if upsertPayload.Repository.RepoID != repoID || upsertPayload.Repository.RepoStatus != sqlite.ProjectRepositoryStatusReady || !upsertPayload.Repository.IsCanonical {
		t.Fatalf("unexpected repository payload: %+v", upsertPayload.Repository)
	}

	listOut, err := captureStdout(t, func() error {
		return runWorkspaceProject([]string{
			"repository",
			"list",
			"--workspace-id", workspaceID,
			"--project-id", projectID,
		})
	})
	if err != nil {
		t.Fatalf("workspace project repository list failed: %v", err)
	}
	var listPayload struct {
		Count        int                              `json:"count"`
		Repositories []sqlite.ProjectRepositoryRecord `json:"repositories"`
	}
	if err := json.Unmarshal([]byte(listOut), &listPayload); err != nil {
		t.Fatalf("decode list output: %v; output=%q", err, listOut)
	}
	if listPayload.Count != 1 || len(listPayload.Repositories) != 1 || listPayload.Repositories[0].RepoID != repoID {
		t.Fatalf("unexpected repository list payload: %+v", listPayload)
	}

	store, err = openStore()
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()
	profile, err := store.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project profile: %v", err)
	}
	if profile.RepoStatus != sqlite.ProjectRepositoryStatusReady || profile.RepoURL != "file:///tmp/project-repository-cli.git" {
		t.Fatalf("canonical repository did not update project profile: %+v", profile)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.repository.upserted",
		EntityType:  "project_repository",
		EntityID:    repoID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 || !strings.Contains(events[0].PayloadJSON, "cli.workspace.project.repository.upsert") {
		t.Fatalf("expected cli prompt context event, got %+v", events)
	}
}
