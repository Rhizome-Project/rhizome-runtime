package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectMutationsWithEventRecordPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-project-storage-evidence"
	const projectID = "project-storage-evidence"
	const actorID = "operator-a"
	seedProjectPromptContextWorkspace(t, ctx, store, workspaceID, actorID)

	project, createEvent, err := store.CreateProjectWithEvent(ctx, sqlite.ProjectCreateInput{
		ProjectID:   projectID,
		WorkspaceID: workspaceID,
		Title:       "Project Storage Evidence",
		Description: "Created from the authority-bearing project CRUD path.",
		CreatedBy:   actorID,
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"project.create",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "project.create",
	})
	if err != nil {
		t.Fatalf("create project with event: %v", err)
	}
	if project.ProjectID != projectID || project.CreatedBy != actorID || project.Status != "ACTIVE" {
		t.Fatalf("unexpected created project %+v", project)
	}
	if createEvent.EventType != "project.created" || createEvent.EntityType != "project" || createEvent.EntityID != projectID {
		t.Fatalf("unexpected create event %+v", createEvent)
	}
	assertProjectPromptContext(t, decodeProjectPromptPayload(t, createEvent.PayloadJSON), "project.create", workspaceID, "human", actorID, projectID, actorID)

	updatedDescription := "Updated from the same project CRUD evidence path."
	updated, updateEvent, err := store.UpdateProjectWithEvent(ctx, sqlite.ProjectUpdateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Storage Evidence Updated",
		Description: &updatedDescription,
		Status:      "ARCHIVED",
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"project.update",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "project.update",
	})
	if err != nil {
		t.Fatalf("update project with event: %v", err)
	}
	if updated.Title != "Project Storage Evidence Updated" || updated.Description != updatedDescription || updated.Status != "ARCHIVED" {
		t.Fatalf("unexpected updated project %+v", updated)
	}
	if updateEvent.EventType != "project.updated" || updateEvent.EntityType != "project" || updateEvent.EntityID != projectID {
		t.Fatalf("unexpected update event %+v", updateEvent)
	}
	assertProjectPromptContext(t, decodeProjectPromptPayload(t, updateEvent.PayloadJSON), "project.update", workspaceID, "human", actorID, projectID, actorID)

	deleteEvent, err := store.DeleteProjectWithEvent(ctx, sqlite.ProjectDeleteInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"project.delete",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "project.delete",
	})
	if err != nil {
		t.Fatalf("delete project with event: %v", err)
	}
	if deleteEvent.EventType != "project.deleted" || deleteEvent.EntityType != "project" || deleteEvent.EntityID != projectID {
		t.Fatalf("unexpected delete event %+v", deleteEvent)
	}
	assertProjectPromptContext(t, decodeProjectPromptPayload(t, deleteEvent.PayloadJSON), "project.delete", workspaceID, "human", actorID, projectID, actorID)
	if _, err := store.GetProject(ctx, workspaceID, projectID); err == nil {
		t.Fatal("expected deleted project to be gone")
	}
}

func TestProjectCreateWithEventRejectsForgedPromptPrincipal(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-project-storage-forged-principal"
	const projectID = "project-forged-principal"
	const actorID = "operator-a"
	seedProjectPromptContextWorkspace(t, ctx, store, workspaceID, actorID)

	_, _, err := store.CreateProjectWithEvent(ctx, sqlite.ProjectCreateInput{
		ProjectID:   projectID,
		WorkspaceID: workspaceID,
		Title:       "Forged Project",
		CreatedBy:   actorID,
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"project.create",
			"server_rpc",
			workspaceID,
			"human",
			"operator-b",
		),
		PromptContextSurface: "project.create",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged principal_id rejection, got %v", err)
	}
	if _, err := store.GetProject(ctx, workspaceID, projectID); err == nil {
		t.Fatal("forged prompt context create mutated project storage")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.created",
		EntityType:  "project",
		EntityID:    projectID,
	})
	if err != nil {
		t.Fatalf("list project.created events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("forged prompt context create recorded runtime events: %+v", events)
	}
}

func TestProjectDeleteWithEventRequiresPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-project-storage-missing-context"
	const projectID = "project-missing-context"
	const actorID = "operator-a"
	seedProjectPromptContextWorkspace(t, ctx, store, workspaceID, actorID)
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		ProjectID:   projectID,
		WorkspaceID: workspaceID,
		Title:       "Missing Prompt Context",
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("seed project: %v", err)
	}

	_, err := store.DeleteProjectWithEvent(ctx, sqlite.ProjectDeleteInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		ActorType:   "human",
	})
	if err == nil || !strings.Contains(err.Error(), "prompt_context_envelope") {
		t.Fatalf("expected missing prompt_context_envelope rejection, got %v", err)
	}
	if _, err := store.GetProject(ctx, workspaceID, projectID); err != nil {
		t.Fatalf("missing prompt context reject deleted project: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.deleted",
		EntityType:  "project",
		EntityID:    projectID,
	})
	if err != nil {
		t.Fatalf("list project.deleted events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("missing prompt context delete recorded runtime events: %+v", events)
	}
}

func TestProjectTaskCountsAndEventDeleteUnlinkStayWorkspaceScoped(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceA = "ws-project-delete-scope-a"
	const workspaceB = "ws-project-delete-scope-b"
	const projectID = "project-shared-delete-scope"
	const actorA = "operator-scope-a"
	const actorB = "operator-scope-b"
	const foreignTaskID = "task-foreign-project-link"
	seedProjectPromptContextWorkspace(t, ctx, store, workspaceA, actorA)
	seedProjectPromptContextWorkspace(t, ctx, store, workspaceB, actorB)

	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		ProjectID:   projectID,
		WorkspaceID: workspaceA,
		Title:       "Workspace A Project",
		CreatedBy:   actorA,
	}); err != nil {
		t.Fatalf("seed workspace A project: %v", err)
	}
	seedWorkspaceTaskWithProjectID(t, ctx, store, workspaceB, foreignTaskID, projectID)

	project, err := store.GetProject(ctx, workspaceA, projectID)
	if err != nil {
		t.Fatalf("get workspace A project: %v", err)
	}
	if project.TaskCount != 0 {
		t.Fatalf("project task count crossed workspace boundary: got %d, want 0", project.TaskCount)
	}
	projects, err := store.ListProjects(ctx, workspaceA)
	if err != nil {
		t.Fatalf("list workspace A projects: %v", err)
	}
	if len(projects) != 1 || projects[0].TaskCount != 0 {
		t.Fatalf("list projects crossed workspace boundary: %+v", projects)
	}

	deleteEvent, err := store.DeleteProjectWithEvent(ctx, sqlite.ProjectDeleteInput{
		WorkspaceID: workspaceA,
		ProjectID:   projectID,
		ActorID:     actorA,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"project.delete",
			"server_rpc",
			workspaceA,
			"human",
			actorA,
		),
	})
	if err != nil {
		t.Fatalf("delete workspace A project with event: %v", err)
	}
	payload := decodeProjectPromptPayload(t, deleteEvent.PayloadJSON)
	if got, ok := payload["task_count_before"].(float64); !ok || got != 0 {
		t.Fatalf("task_count_before = %v, want 0 in %+v", payload["task_count_before"], payload)
	}
	if got, ok := payload["unlinked_task_count"].(float64); !ok || got != 0 {
		t.Fatalf("unlinked_task_count = %v, want 0 in %+v", payload["unlinked_task_count"], payload)
	}
	requireWorkspaceTaskProjectID(t, ctx, store, workspaceB, foreignTaskID, projectID)
}

func TestLegacyProjectDeleteUnlinkStaysWorkspaceScoped(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceA = "ws-project-legacy-delete-scope-a"
	const workspaceB = "ws-project-legacy-delete-scope-b"
	const projectID = "project-legacy-delete-scope"
	const actorA = "operator-legacy-a"
	const actorB = "operator-legacy-b"
	const foreignTaskID = "task-foreign-legacy-project-link"
	seedProjectPromptContextWorkspace(t, ctx, store, workspaceA, actorA)
	seedProjectPromptContextWorkspace(t, ctx, store, workspaceB, actorB)

	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		ProjectID:   projectID,
		WorkspaceID: workspaceA,
		Title:       "Legacy Workspace A Project",
		CreatedBy:   actorA,
	}); err != nil {
		t.Fatalf("seed legacy workspace A project: %v", err)
	}
	seedWorkspaceTaskWithProjectID(t, ctx, store, workspaceB, foreignTaskID, projectID)

	if err := store.DeleteProject(ctx, workspaceA, projectID); err != nil {
		t.Fatalf("delete legacy workspace A project: %v", err)
	}
	requireWorkspaceTaskProjectID(t, ctx, store, workspaceB, foreignTaskID, projectID)
}

func seedProjectPromptContextWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "system",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("ensure local workspace authority for %s: %v", workspaceID, err)
	}
}

func seedWorkspaceTaskWithProjectID(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, projectID string) {
	t.Helper()
	createSingleNodeTask(t, ctx, store, taskID, taskID+"-node")
	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET project_id = ? WHERE task_id = ?`, projectID, taskID); err != nil {
		t.Fatalf("set task %s project_id to %s: %v", taskID, projectID, err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_tasks(workspace_id, task_id, linked_by, created_at)
VALUES (?, ?, 'scope-test', '2026-04-28T00:00:00Z')`, workspaceID, taskID); err != nil {
		t.Fatalf("seed legacy workspace task %s in workspace %s: %v", taskID, workspaceID, err)
	}
}

func requireWorkspaceTaskProjectID(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, wantProjectID string) {
	t.Helper()
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace %s tasks: %v", workspaceID, err)
	}
	for _, task := range tasks {
		if task.TaskID == taskID {
			if task.ProjectID != wantProjectID {
				t.Fatalf("task %s project_id = %q, want %q", taskID, task.ProjectID, wantProjectID)
			}
			return
		}
	}
	t.Fatalf("task %s not found in workspace %s tasks: %+v", taskID, workspaceID, tasks)
}

func decodeProjectPromptPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode project prompt payload: %v; payload=%q", err, payloadJSON)
	}
	return payload
}

func assertProjectPromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, projectID, actorID string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected project prompt_context_envelope in payload, got %+v", payload)
	}
	for key, want := range map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_project_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"project_id":                         projectID,
		"actor_id":                           actorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	} {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
}
