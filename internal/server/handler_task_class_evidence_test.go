package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTaskClassPutRPCUpdatesAndClearsEvidence(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	createServerTaskClassWorkspace(t, ctx, store, "ws-task-class-put")
	createServerTaskClassTask(t, ctx, store, "ws-task-class-put", sqlite.TaskCreateInput{
		TaskID:       "task-class-put-rpc",
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Repair rollout",
		Description:  "Operator can update authored class evidence over RPC.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateBugfix,
	})

	rawPut, err := json.Marshal(taskClassPutParams{
		WorkspaceID:     "ws-task-class-put",
		TaskID:          "task-class-put-rpc",
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "operator-a",
	})
	if err != nil {
		t.Fatalf("marshal task.class.put params: %v", err)
	}
	result, rpcErr := h.taskClassPut(ctx, rawPut)
	if rpcErr != nil {
		t.Fatalf("taskClassPut rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected task.class.put result type %T", result)
	}
	status, ok := payload["task"].(sqlite.TaskStatus)
	if !ok {
		t.Fatalf("unexpected task.class.put task payload type %T", payload["task"])
	}
	if status.TaskClass != model.TaskClassIncident || status.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected task.class.put to persist incident evidence, got %+v", status)
	}

	rawClear, err := json.Marshal(taskClassPutParams{
		WorkspaceID:     "ws-task-class-put",
		TaskID:          "task-class-put-rpc",
		TaskClassSource: model.TaskClassSourceUnset,
		ActorID:         "operator-a",
	})
	if err != nil {
		t.Fatalf("marshal task.class.put clear params: %v", err)
	}
	result, rpcErr = h.taskClassPut(ctx, rawClear)
	if rpcErr != nil {
		t.Fatalf("taskClassPut clear rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected cleared task.class.put result type %T", result)
	}
	status, ok = payload["task"].(sqlite.TaskStatus)
	if !ok {
		t.Fatalf("unexpected cleared task.class.put task payload type %T", payload["task"])
	}
	if status.TaskClass != "" || status.TaskClassSource != model.TaskClassSourceUnset || status.TaskClassUpdatedAt != "" {
		t.Fatalf("expected cleared task.class.put evidence, got %+v", status)
	}
}

func TestTaskStatusReturnsExplicitTaskClassEvidence(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	createServerTaskClassWorkspace(t, ctx, store, "ws-task-status-class")
	createServerTaskClassTask(t, ctx, store, "ws-task-status-class", sqlite.TaskCreateInput{
		TaskID:          "task-status-class",
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Integration rollout",
		Description:     "Status RPC should surface authored class evidence.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateIntegration,
		TaskClass:       model.TaskClassIntegration,
		TaskClassSource: model.TaskClassSourceTemplateDefault,
	})

	raw, err := json.Marshal(taskStatusParams{
		WorkspaceID: "ws-task-status-class",
		TaskID:      "task-status-class",
	})
	if err != nil {
		t.Fatalf("marshal task.status params: %v", err)
	}
	result, rpcErr := h.taskStatus(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("taskStatus rpc error: %+v", rpcErr)
	}
	status, ok := result.(sqlite.TaskStatus)
	if !ok {
		t.Fatalf("unexpected task.status result type %T", result)
	}
	if status.TaskClass != model.TaskClassIntegration || status.TaskClassSource != model.TaskClassSourceTemplateDefault {
		t.Fatalf("expected task.status to surface explicit task class evidence, got %+v", status)
	}
}

func TestWorkspaceTasksListReturnsExplicitTaskClassEvidenceFields(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-task-list-class"
		taskID      = "task-list-class"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")

	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Bugfix rollout",
		Description:     "Workspace task listing should surface authored class evidence.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateBugfix,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})

	raw, err := json.Marshal(workspaceTasksListParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal workspace.tasks.list params: %v", err)
	}
	result, rpcErr := h.workspaceTasksList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceTasksList rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace.tasks.list result type %T", result)
	}
	tasks, ok := payload["tasks"].([]sqlite.WorkspaceTaskRecord)
	if !ok || len(tasks) != 1 {
		t.Fatalf("expected one workspace task record, got %+v", payload["tasks"])
	}
	taskMap := serverJSONMap(t, tasks[0])
	if taskMap["task_class"] != model.TaskClassIncident || taskMap["task_class_source"] != model.TaskClassSourceExplicit {
		t.Fatalf("expected workspace.tasks.list to surface task_class/task_class_source, got %+v", taskMap)
	}
}

func TestTaskSubmitPersistsExplicitTaskClassEvidence(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-task-submit-class"
		taskID      = "task-submit-class"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")

	createServerTaskClassWorkspace(t, ctx, store, workspaceID)

	raw, err := json.Marshal(map[string]any{
		"task_id":           taskID,
		"owner_user_id":     "developer",
		"priority":          "normal",
		"title":             "Deploy hotfix",
		"description":       "task.submit should preserve authored class evidence",
		"task_kind":         model.TaskKindExecution,
		"task_template":     model.TaskTemplateDeploy,
		"task_class":        model.TaskClassIncident,
		"task_class_source": model.TaskClassSourceExplicit,
		"workspace_id":      workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal task.submit params: %v", err)
	}
	if _, rpcErr := h.taskSubmit(ctx, raw); rpcErr != nil {
		t.Fatalf("taskSubmit rpc error: %+v", rpcErr)
	}

	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status after submit: %v", err)
	}
	if status.TaskClass != model.TaskClassIncident || status.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected task.submit to persist authored class evidence, got %+v", status)
	}
}

func TestTaskClassEvidenceSchemaContracts(t *testing.T) {
	submit := rpcMethodSchemas["task.submit"]
	if _, ok := submit.Params["task_class"]; !ok {
		t.Fatalf("task.submit schema must expose task_class: %+v", submit.Params)
	}
	if _, ok := submit.Params["task_class_source"]; !ok {
		t.Fatalf("task.submit schema must expose task_class_source: %+v", submit.Params)
	}

	put, ok := rpcMethodSchemas["task.class.put"]
	if !ok {
		t.Fatal("missing task.class.put schema")
	}
	if !put.Params["task_id"].Required {
		t.Fatalf("task.class.put must require task_id: %+v", put.Params["task_id"])
	}
	if got := put.Params["task_class"].Enum; len(got) != 4 || got[0] != model.TaskClassProof || got[3] != model.TaskClassIncident {
		t.Fatalf("task.class.put task_class enum drifted: %+v", got)
	}
	if got := put.Params["task_class_source"].Enum; len(got) != 3 || got[0] != model.TaskClassSourceExplicit || got[2] != model.TaskClassSourceUnset {
		t.Fatalf("task.class.put task_class_source enum drifted: %+v", got)
	}
}

func createServerTaskClassWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
}

func createServerTaskClassTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, input sqlite.TaskCreateInput) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + input.TaskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph for %s: %v", input.TaskID, err)
	}
	if err := store.CreateTaskWithGraph(ctx, input, graph); err != nil {
		t.Fatalf("create task %s: %v", input.TaskID, err)
	}
	if workspaceID == "" {
		return
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      input.TaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task %s: %v", input.TaskID, err)
	}
}

func serverJSONMap(t *testing.T, value any) map[string]any {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal value: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode value: %v", err)
	}
	return decoded
}
