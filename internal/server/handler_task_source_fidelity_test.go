package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMergeTaskSubmitSourceDocKeysAddsDurableLineage(t *testing.T) {
	requirements := mergeTaskSubmitSourceDocKeys(map[string]any{
		"schema":              "task_requirements.v1",
		"required_work_modes": []any{"implementation"},
	}, []string{"run.clearpress.operator-spec"})

	keys, ok := requirements["source_doc_keys"].([]string)
	if !ok || len(keys) != 1 || keys[0] != "run.clearpress.operator-spec" {
		t.Fatalf("source_doc_keys not merged into task requirements: %#v", requirements)
	}
	if requirements["spec_fidelity_contract"] != "source_artifact_fidelity.v1" {
		t.Fatalf("missing spec_fidelity_contract: %#v", requirements)
	}
}

func TestTaskSubmitInheritsProjectSourceRefsIntoRequirements(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-task-submit-source-refs"
		projectID   = "project-submit-source-refs"
		taskID      = "task-submit-source-refs"
		sourceKey   = "run.clearpress.operator-spec"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	createServerProjectForTaskProjectFields(t, ctx, store, workspaceID, projectID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "project." + taskSubmitProjectDocKeySegment(projectID) + ".source_refs",
		Title:       "Source Refs",
		Content: "```rhizome_source_refs_v1\n" +
			"source_doc_keys:\n" +
			"- " + sourceKey + "\n" +
			"```",
		UpdatedBy: "lead-agent",
	}); err != nil {
		t.Fatalf("write project source refs: %v", err)
	}

	raw, err := json.Marshal(map[string]any{
		"task_id":               taskID,
		"owner_user_id":         "developer",
		"title":                 "Revision follow-up with source lineage",
		"description":           "Task should inherit project source refs into durable task_requirements_json.",
		"workspace_id":          workspaceID,
		"project_id":            projectID,
		"project_lane":          "implementation",
		"requires_project_gate": true,
		"task_requirements": map[string]any{
			"required_work_modes": []string{"implementation"},
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
	requirementsJSON, _ := payload["task_requirements_json"].(string)
	for _, want := range []string{sourceKey, "source_doc_keys", "source_artifact_fidelity.v1"} {
		if !strings.Contains(requirementsJSON, want) {
			t.Fatalf("task.submit result requirements missing %q: %s", want, requirementsJSON)
		}
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status after submit: %v", err)
	}
	for _, want := range []string{sourceKey, "source_doc_keys", "source_artifact_fidelity.v1"} {
		if !strings.Contains(status.TaskRequirementsJSON, want) {
			t.Fatalf("persisted task requirements missing %q: %s", want, status.TaskRequirementsJSON)
		}
	}
}
