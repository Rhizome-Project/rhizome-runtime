package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationCorridorAuthoritySurface(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-corridor-authority-rpc"
		taskID      = "task-corridor-authority-rpc"
	)

	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Repair rollout",
		Description:     "Explicit task-authored class evidence should stay visible before runtime activity.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateResearch,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})

	rawReport, err := json.Marshal(workspaceInstrumentationCorridorAuthorityParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal corridor authority report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorAuthorityReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorAuthorityReport rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected corridor authority report result type %T", result)
	}
	report, ok := payload["report"].(sqlite.CorridorAuthorityReport)
	if !ok {
		t.Fatalf("unexpected corridor authority report payload type %T", payload["report"])
	}
	if report.GeneratedAt == "" || report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor authority report generated_at to mirror time authority reference_at, report=%+v", report)
	}
	task := requireServerCorridorAuthorityTask(t, report.Tasks, taskID)
	if task.TaskClass != model.TaskClassIncident || task.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected explicit task-authored class evidence in corridor authority report, got %+v", task)
	}
	if task.VisibleInInstrumentation {
		t.Fatalf("expected corridor authority report to keep inactive authored task visible without instrumentation activity, got %+v", task)
	}
	if task.BasisState != "AUTHORED_FRESH" || !task.BasisAuthoritative {
		t.Fatalf("expected corridor authority report to keep fresh authoritative basis, got %+v", task)
	}

	rawTask, err := json.Marshal(workspaceInstrumentationCorridorAuthorityParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
	})
	if err != nil {
		t.Fatalf("marshal corridor authority task params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationCorridorAuthorityTask(ctx, rawTask)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorAuthorityTask rpc error: %+v", rpcErr)
	}
	taskPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected corridor authority task result type %T", result)
	}
	detail, ok := taskPayload["detail"].(sqlite.CorridorAuthorityTaskDetail)
	if !ok {
		t.Fatalf("unexpected corridor authority task detail type %T", taskPayload["detail"])
	}
	if detail.Task.TaskID != taskID || detail.Task.TaskClass != model.TaskClassIncident || detail.Task.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected corridor authority task detail to surface explicit authored class evidence, got %+v", detail.Task)
	}
	if len(detail.Clusters) != 0 {
		t.Fatalf("expected inactive authored task to keep empty proto-cluster linkage, got %+v", detail.Clusters)
	}
}

func TestWorkspaceInstrumentationCorridorAuthorityRejectsInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationCorridorAuthorityParams
	}{
		{name: "report", call: h.workspaceInstrumentationCorridorAuthorityReport, params: workspaceInstrumentationCorridorAuthorityParams{Limit: 20}},
		{name: "task", call: h.workspaceInstrumentationCorridorAuthorityTask, params: workspaceInstrumentationCorridorAuthorityParams{WorkspaceID: "ws-only"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params error, got %+v", rpcErr)
			}
		})
	}
}

func requireServerCorridorAuthorityTask(t *testing.T, items []sqlite.CorridorAuthorityTaskRecord, taskID string) sqlite.CorridorAuthorityTaskRecord {
	t.Helper()
	for _, item := range items {
		if item.TaskID == taskID {
			return item
		}
	}
	t.Fatalf("corridor authority task %s not found in %+v", taskID, items)
	return sqlite.CorridorAuthorityTaskRecord{}
}
