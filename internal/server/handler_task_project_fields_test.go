package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTaskSubmitPersistsProjectTaxonomy(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-task-submit-project-taxonomy"
		projectID   = "project-submit-taxonomy"
		taskID      = "task-submit-project-taxonomy"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")

	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	createServerProjectForTaskProjectFields(t, ctx, store, workspaceID, projectID)
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       "task-upstream",
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Upstream prerequisite",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
	})
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       "task-related",
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Related sibling context",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
	})

	raw, err := json.Marshal(map[string]any{
		"task_id":               taskID,
		"owner_user_id":         "developer",
		"priority":              "normal",
		"title":                 "Project lane implementation",
		"description":           "task.submit should preserve project linkage and taxonomy",
		"task_kind":             "EXECUTION",
		"task_template":         "generic",
		"workspace_id":          workspaceID,
		"project_id":            projectID,
		"project_lane":          " FrontEnd ",
		"requires_project_gate": true,
		"dependency_task_ids":   []string{"task-upstream"},
		"related_task_ids":      []string{"task-related"},
		"write_scope_hints":     []string{"ui/**"},
		"task_requirements": map[string]any{
			"required_work_modes": []string{"implementation"},
			"preferred_skills":    []string{"frontend", "visual-qa"},
			"preferred_tools":     []string{"browser"},
		},
	})
	if err != nil {
		t.Fatalf("marshal task.submit params: %v", err)
	}
	result, rpcErr := h.taskSubmit(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("taskSubmit rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected task.submit result type %T", result)
	}
	if payload["project_id"] != projectID || payload["project_lane"] != "frontend" || payload["requires_project_gate"] != true {
		t.Fatalf("task.submit result should echo project taxonomy, got %+v", payload)
	}
	statusPayload, ok := payload["task"].(sqlite.TaskStatus)
	if !ok {
		t.Fatalf("task.submit result should include normalized status readback, got %+v", payload)
	}
	if !strings.Contains(statusPayload.TaskRequirementsJSON, "required_work_modes") || strings.Join(statusPayload.WriteScopeHints, ",") != "ui/**" {
		t.Fatalf("task.submit status readback should include requirements and write scope, got %+v", statusPayload)
	}
	requirementsJSON, _ := payload["task_requirements_json"].(string)
	if !strings.Contains(requirementsJSON, "preferred_tools") {
		t.Fatalf("task.submit result should echo requirements json, got %+v", payload)
	}

	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status after submit: %v", err)
	}
	if status.ProjectID != projectID || status.TaskKind != "EXECUTION" || status.ProjectLane != "frontend" || !status.RequiresProjectGate {
		t.Fatalf("expected project taxonomy to persist, got %+v", status)
	}
	if !strings.Contains(status.TaskRequirementsJSON, "visual-qa") || strings.Join(status.WriteScopeHints, ",") != "ui/**" {
		t.Fatalf("expected task requirements and write scope hints to persist, got %+v", status)
	}
	links, err := store.ListWorkspaceTaskLinks(ctx, sqlite.WorkspaceTaskLinkFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list dependency links: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("expected dependency and related task links, got %+v", links)
	}
	linksByType := map[string]sqlite.WorkspaceTaskLinkRecord{}
	for _, link := range links {
		linksByType[link.LinkType] = link
	}
	if link := linksByType["BLOCKS"]; link.FromTaskID != "task-upstream" || link.ToTaskID != taskID {
		t.Fatalf("expected dependency_task_ids to create BLOCKS link, got %+v", links)
	}
	if link := linksByType["RELATES_TO"]; link.FromTaskID != "task-related" || link.ToTaskID != taskID {
		t.Fatalf("expected related_task_ids to create RELATES_TO link, got %+v", links)
	}
}

func TestTaskSubmitRejectsInvalidProjectAsInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-task-submit-invalid-project"
	ctx := testAuthContext(workspaceID, "human", "developer")
	createServerTaskClassWorkspace(t, ctx, store, workspaceID)

	raw, err := json.Marshal(map[string]any{
		"task_id":       "task-invalid-project",
		"owner_user_id": "developer",
		"title":         "Invalid project id",
		"workspace_id":  workspaceID,
		"project_id":    "project-does-not-exist",
	})
	if err != nil {
		t.Fatalf("marshal invalid project task.submit params: %v", err)
	}
	if _, rpcErr := h.taskSubmit(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for missing project, got %+v", rpcErr)
	}
}

func TestTaskProjectFieldsPutValidatesProjectScope(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-project-fields-put"
		projectID   = "project-fields-put"
		taskID      = "task-project-fields-put"
	)

	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	createServerProjectForTaskProjectFields(t, ctx, store, workspaceID, projectID)
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Set project fields later",
		Description:  "task.project_fields.put should validate project scope",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
	})

	missingProjectID := "missing-project-fields-put"
	rawMissing, err := json.Marshal(taskProjectFieldsPutParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ProjectID:   &missingProjectID,
		ActorID:     "operator-a",
	})
	if err != nil {
		t.Fatalf("marshal missing project update: %v", err)
	}
	if _, rpcErr := h.taskProjectFieldsPut(ctx, rawMissing); rpcErr == nil {
		t.Fatal("expected missing project update to be rejected")
	}

	claimServerProjectLeadForTaskProjectFields(t, ctx, store, workspaceID, projectID, "operator-a")
	projectLane := " Review "
	taskKind := "COORDINATION"
	requiresGate := true
	rawValid, err := json.Marshal(taskProjectFieldsPutParams{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		ProjectID:           stringPtr(projectID),
		TaskKind:            &taskKind,
		ProjectLane:         &projectLane,
		RequiresProjectGate: &requiresGate,
		ActorID:             "operator-a",
	})
	if err != nil {
		t.Fatalf("marshal valid project update: %v", err)
	}
	result, rpcErr := h.taskProjectFieldsPut(ctx, rawValid)
	if rpcErr != nil {
		t.Fatalf("taskProjectFieldsPut rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected task.project_fields.put result type %T", result)
	}
	status, ok := payload["task"].(sqlite.TaskStatus)
	if !ok {
		t.Fatalf("unexpected task.project_fields.put task payload type %T", payload["task"])
	}
	if status.ProjectID != projectID || status.TaskKind != "COORDINATION" || status.ProjectLane != "review" || !status.RequiresProjectGate {
		t.Fatalf("expected task.project_fields.put to persist taxonomy, got %+v", status)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.project_fields.updated",
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list project field runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one task.project_fields.updated event, got %+v", events)
	}
	if events[0].AuthorityHolderNodeID == "" || events[0].AuthorityTerm == 0 || events[0].AuthorityLeaseTokenFingerprint == "" {
		t.Fatalf("task.project_fields.updated should be authority-backed, got %+v", events[0])
	}
}

func TestTaskProjectFieldsPutBindsActorToAuthenticatedPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-project-fields-actor-binding"
		projectID   = "project-fields-actor-binding"
		taskID      = "task-project-fields-actor-binding"
		leadID      = "agent-project-fields-lead"
		otherID     = "agent-project-fields-other"
	)

	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	createServerProjectForTaskProjectFields(t, ctx, store, workspaceID, projectID)
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Actor-bound project fields",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
		ProjectID:    projectID,
		ProjectLane:  "coordination",
	})
	claimServerProjectLeadForTaskProjectFields(t, ctx, store, workspaceID, projectID, leadID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     otherID,
		OwnerUserID: "developer",
		DisplayName: otherID,
	}); err != nil {
		t.Fatalf("register non-lead agent: %v", err)
	}

	implementationLane := "implementation"
	spoofRaw, err := json.Marshal(taskProjectFieldsPutParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ProjectLane: &implementationLane,
		ActorID:     leadID,
	})
	if err != nil {
		t.Fatalf("marshal spoof update: %v", err)
	}
	if _, rpcErr := h.taskProjectFieldsPut(testAuthContext(workspaceID, "agent", otherID), spoofRaw); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected authenticated non-lead actor spoof to be rejected, got %+v", rpcErr)
	}

	leadRaw, err := json.Marshal(taskProjectFieldsPutParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ProjectLane: &implementationLane,
	})
	if err != nil {
		t.Fatalf("marshal lead update without actor_id: %v", err)
	}
	result, rpcErr := h.taskProjectFieldsPut(testAuthContext(workspaceID, "agent", leadID), leadRaw)
	if rpcErr != nil {
		t.Fatalf("authenticated lead without actor_id should update lane: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	status, ok := payload["task"].(sqlite.TaskStatus)
	if !ok || status.ProjectLane != "implementation" {
		t.Fatalf("expected authenticated lead update to persist implementation lane, got %+v", payload["task"])
	}
}

func TestTaskProjectFieldsSchemaContracts(t *testing.T) {
	submit := rpcMethodSchemas["task.submit"]
	for _, key := range []string{"project_id", "project_lane", "requires_project_gate", "dependency_task_ids", "related_task_ids", "write_scope_hints", "task_requirements"} {
		if _, ok := submit.Params[key]; !ok {
			t.Fatalf("task.submit schema must expose %s: %+v", key, submit.Params)
		}
	}
	if got := submit.Params["requires_project_gate"].Type; got != "boolean" {
		t.Fatalf("task.submit requires_project_gate should be boolean, got %+v", submit.Params["requires_project_gate"])
	}

	put, ok := rpcMethodSchemas["task.project_fields.put"]
	if !ok {
		t.Fatal("missing task.project_fields.put schema")
	}
	if !put.Params["workspace_id"].Required || !put.Params["task_id"].Required {
		t.Fatalf("task.project_fields.put must require workspace_id and task_id: %+v", put.Params)
	}
	if put.Params["project_id"].Required || put.Params["project_lane"].Required || put.Params["requires_project_gate"].Required {
		t.Fatalf("task.project_fields.put project taxonomy fields should remain patch-style optional: %+v", put.Params)
	}
}

func createServerProjectForTaskProjectFields(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID string) {
	t.Helper()
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create project %s: %v", projectID, err)
	}
}

func claimServerProjectLeadForTaskProjectFields(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, agentID string) {
	t.Helper()
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register project lead agent %s: %v", agentID, err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "test strategic lead for task.project_fields.put",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim project strategic lead %s: %v", agentID, err)
	}
}

func stringPtr(value string) *string {
	return &value
}
