package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTaskClaimHistoryIsPreservedInRuntimeEventsAndCurrentTaskState(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-task-claim-history"
		agentID     = "agent-task-claim-history"
		taskID      = "task-claim-history"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createTaskClaimHistoryFixture(t, ctx, store, workspaceID, agentID, taskID)

	if _, rpcErr := h.agentTaskClaim(ctx, mustJSONRaw(agentTaskClaimParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "initial claim",
	})); rpcErr != nil {
		t.Fatalf("agentTaskClaim initial rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.agentTaskRelease(ctx, mustJSONRaw(agentTaskReleaseParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		Reason:      "waiting on dependency",
	})); rpcErr != nil {
		t.Fatalf("agentTaskRelease rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.agentTaskClaim(ctx, mustJSONRaw(agentTaskClaimParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "reclaimed after dependency",
	})); rpcErr != nil {
		t.Fatalf("agentTaskClaim replay rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.agentTaskBlock(ctx, mustJSONRaw(taskBlockParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		Reason:      "blocked by product decision",
	})); rpcErr != nil {
		t.Fatalf("agentTaskBlock rpc error: %+v", rpcErr)
	}

	workspaceCtx := testAuthContext(workspaceID, "human", "developer")
	listAny, rpcErr := h.workspaceTasksList(workspaceCtx, mustJSONRaw(workspaceTasksListParams{
		WorkspaceID: workspaceID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceTasksList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace.tasks.list result type %T", listAny)
	}
	tasks, ok := listPayload["tasks"].([]sqlite.WorkspaceTaskRecord)
	if !ok || len(tasks) != 1 {
		t.Fatalf("expected one workspace task record, got %+v", listPayload["tasks"])
	}
	taskMap := serverJSONMap(t, tasks[0])
	if taskMap["claim_status"] != model.TaskClaimStatusBlocked {
		t.Fatalf("expected latest task claim status to be BLOCKED, got %+v", taskMap["claim_status"])
	}
	if taskMap["claim_summary"] != "blocked by product decision" {
		t.Fatalf("expected latest task claim summary to reflect blocker reason, got %+v", taskMap["claim_summary"])
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	gotTypes := make([]string, 0, len(events))
	for _, event := range events {
		gotTypes = append(gotTypes, event.EventType)
	}
	wantTypes := []string{"task.blocked", "task.claimed", "task.released", "task.claimed"}
	if len(gotTypes) < len(wantTypes) || !reflect.DeepEqual(gotTypes[:len(wantTypes)], wantTypes) {
		t.Fatalf("unexpected task claim history order: got %v want %v", gotTypes, wantTypes)
	}
}

func createTaskClaimHistoryFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, taskID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Task Claim History Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Task Claim History",
		Description: "claim lock history regression fixture",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task to workspace: %v", err)
	}
}
