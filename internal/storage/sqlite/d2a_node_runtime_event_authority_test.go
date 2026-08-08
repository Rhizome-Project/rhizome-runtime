package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestNodeLifecycleWithEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-node-authority-metadata"
		taskID      = "task-d2a-node-authority-metadata"
		agentID     = "agent-d2a-node-authority-metadata"
		nodeID      = "node-1"
	)

	seedD2ANodeWorkspace(t, ctx, store, workspaceID, taskID, agentID, nodeID)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claimed, err := store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "claim node with authority metadata",
	})
	if err != nil {
		t.Fatalf("claim node with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, claimed, authority)

	released, err := store.ReleaseNodeClaimWithEvent(ctx, sqlite.NodeReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Reason:      "release node with authority metadata",
	})
	if err != nil {
		t.Fatalf("release node with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, released, authority)

	reclaimed, err := store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "reclaim node before completion",
	})
	if err != nil {
		t.Fatalf("reclaim node with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, reclaimed, authority)

	completed, err := store.CompleteNodeClaimWithEvent(ctx, sqlite.NodeCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Summary:     "complete node with authority metadata",
	})
	if err != nil {
		t.Fatalf("complete node with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, completed, authority)

	for _, tc := range []struct {
		eventType string
	}{
		{eventType: "node.claimed"},
		{eventType: "node.released"},
		{eventType: "node.completed"},
	} {
		events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   tc.eventType,
			EntityType:  "dag_node",
			EntityID:    nodeID,
			TaskID:      taskID,
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("list %s runtime events: %v", tc.eventType, err)
		}
		if len(events) == 0 {
			t.Fatalf("expected at least one %s runtime event", tc.eventType)
		}
		assertRuntimeEventAuthorityMetadata(t, events[0], authority)
	}
}

func TestReleaseNodeClaimWithEventNoOpDoesNotAppendRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-node-release-noop"
		taskID      = "task-d2a-node-release-noop"
		agentID     = "agent-d2a-node-release-noop"
		nodeID      = "node-1"
	)

	seedD2ANodeWorkspace(t, ctx, store, workspaceID, taskID, agentID, nodeID)
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	event, err := store.ReleaseNodeClaimWithEvent(ctx, sqlite.NodeReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      nodeID,
		AgentID:     agentID,
		Reason:      "noop release should stay quiet",
	})
	if err != nil {
		t.Fatalf("release node with event: %v", err)
	}
	if event.EventID != "" {
		t.Fatalf("expected noop node release to return zero runtime event, got %+v", event)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "node.released",
		EntityType:  "dag_node",
		EntityID:    nodeID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list node.released runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no node.released runtime events for noop release, got %+v", events)
	}
}

func seedD2ANodeWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, nodeID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "tests",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task with graph: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "tests",
	}); err != nil {
		t.Fatalf("attach task to workspace: %v", err)
	}
}
