package living_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMemoryGraphReadToolsExposeCompatibilityBoundaryContract(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-p6b-memory-shape-tools"
		agentID     = "agent-p6b-memory-shape-tools"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Tool graph stays compatibility-only",
		Body:        "Tool graph stays compatibility-only over canonical workspace memory.",
		Summary:     "Tool compatibility boundary",
		AgentID:     agentID,
		TaskID:      taskID,
		SourceKind:  "workspace_memory_write",
		SourceID:    "p6b-tool-test",
		Importance:  0.9,
		Confidence:  0.95,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.SyncMemoryGraphWorkspace(ctx, workspaceID); err != nil {
		t.Fatalf("sync memory graph workspace: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	expectedBoundary := sqlite.DefaultMemoryShapeBoundaryContract()

	listTool := living.NewWorkspaceMemoryGraphListReadTool(client, workspaceID, agentID)
	listOut, err := listTool.Execute(ctx, json.RawMessage(`{"task_id":"`+taskID+`","origin_kind":"workspace_memory"}`))
	if err != nil {
		t.Fatalf("memory_graph_list_read execute failed: %v", err)
	}
	var listPayload map[string]any
	if err := json.Unmarshal([]byte(listOut), &listPayload); err != nil {
		t.Fatalf("decode memory_graph_list_read output: %v", err)
	}
	listBoundary, ok := listPayload["boundary_contract"].(map[string]any)
	if !ok || listBoundary["canonical_shape"] != expectedBoundary.CanonicalShape || listBoundary["surface_authority"] != expectedBoundary.SurfaceAuthority || listBoundary["surface_role"] != expectedBoundary.SurfaceRole {
		t.Fatalf("expected list boundary contract, got %+v", listPayload)
	}
	listItems, ok := listPayload["items"].([]any)
	if !ok || len(listItems) != 1 {
		t.Fatalf("expected one list item, got %+v", listPayload)
	}
	listItem, ok := listItems[0].(map[string]any)
	if !ok || listItem["canonical_authority"] != "workspace_memory" || listItem["surface_authority"] != "compatibility_only" || listItem["compatibility_only"] != true {
		t.Fatalf("expected list item compatibility boundary, got %+v", listPayload)
	}

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	getTool := living.NewWorkspaceMemoryGraphGetReadTool(client, workspaceID)
	getOut, err := getTool.Execute(ctx, json.RawMessage(`{"memory_id":"`+nodeID+`"}`))
	if err != nil {
		t.Fatalf("memory_graph_get_read execute failed: %v", err)
	}
	var getPayload map[string]any
	if err := json.Unmarshal([]byte(getOut), &getPayload); err != nil {
		t.Fatalf("decode memory_graph_get_read output: %v", err)
	}
	getBoundary, ok := getPayload["boundary_contract"].(map[string]any)
	if !ok || getBoundary["canonical_shape"] != expectedBoundary.CanonicalShape || getBoundary["surface_authority"] != expectedBoundary.SurfaceAuthority || getBoundary["surface_role"] != expectedBoundary.SurfaceRole {
		t.Fatalf("expected get boundary contract, got %+v", getPayload)
	}
	getNode, ok := getPayload["node"].(map[string]any)
	if !ok || getNode["canonical_authority"] != "workspace_memory" || getNode["surface_authority"] != "compatibility_only" || getNode["compatibility_only"] != true {
		t.Fatalf("expected get node compatibility boundary, got %+v", getPayload)
	}

	searchTool := living.NewWorkspaceMemoryNodeSearchReadTool(client, workspaceID, agentID)
	searchOut, err := searchTool.Execute(ctx, json.RawMessage(`{"query":"compatibility-only over canonical workspace memory","origin_kind":"workspace_memory","task_id":"`+taskID+`"}`))
	if err != nil {
		t.Fatalf("memory_node_search_read execute failed: %v", err)
	}
	var searchPayload map[string]any
	if err := json.Unmarshal([]byte(searchOut), &searchPayload); err != nil {
		t.Fatalf("decode memory_node_search_read output: %v", err)
	}
	searchBoundary, ok := searchPayload["boundary_contract"].(map[string]any)
	if !ok || searchBoundary["canonical_shape"] != expectedBoundary.CanonicalShape || searchBoundary["surface_authority"] != expectedBoundary.SurfaceAuthority || searchBoundary["surface_role"] != expectedBoundary.SurfaceRole {
		t.Fatalf("expected search boundary contract, got %+v", searchPayload)
	}
	hits, ok := searchPayload["hits"].([]any)
	if !ok || len(hits) != 1 {
		t.Fatalf("expected one search hit, got %+v", searchPayload)
	}
	hit, ok := hits[0].(map[string]any)
	if !ok || hit["canonical_authority"] != "workspace_memory" || hit["surface_authority"] != "compatibility_only" || hit["compatibility_only"] != true {
		t.Fatalf("expected search hit compatibility boundary, got %+v", searchPayload)
	}
}
