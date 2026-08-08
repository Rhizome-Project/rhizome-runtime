package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestResolveHumanActionWithQueueEffectsRejectsStaleHumanActionRevision(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-resolve-stale-revision"
		taskID      = "task-actions-resolve-stale-revision"
		agentID     = "agent-actions-resolve-stale-revision"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Resolve Stale Revision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Revision Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	actionID, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Need revision guard",
	})
	if err != nil {
		t.Fatalf("create action: %v", err)
	}
	staleAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get stale action: %v", err)
	}

	actionQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "action:" + actionID,
		QueueType:   "FOLLOW_UP",
		Title:       staleAction.Title,
		Urgency:     "NORMAL",
		AssignedTo:  staleAction.AssignedTo,
		SourceKind:  "human_action",
		SourceID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("create action queue: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE human_actions SET assigned_to = ?, revision = revision + 1 WHERE action_id = ?`,
		"reviewer-b",
		actionID,
	); err != nil {
		t.Fatalf("interleave human action revision: %v", err)
	}

	result, err := store.ResolveHumanActionWithQueueEffects(ctx, actionID, model.ActionStatusCompleted, "done", "developer", &sqlite.OperatorQueueResolveInput{
		WorkspaceID:             workspaceID,
		QueueID:                 actionQueue.QueueID,
		Status:                  "RESOLVED",
		ResolvedBy:              "developer",
		Resolution:              "done",
		RequireCurrentRevision:  actionQueue.Revision,
		RequireCurrentUpdatedAt: actionQueue.UpdatedAt,
	}, nil, nil, nil, staleAction)
	if err == nil {
		t.Fatalf("expected stale human_action revision reject, got result %+v", result)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "updated concurrently") {
		t.Fatalf("expected updated concurrently error, got %v", err)
	}

	currentAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get current action: %v", err)
	}
	if currentAction.Status != model.ActionStatusPending || currentAction.ResolvedAt != "" || currentAction.ResolutionComment != "" {
		t.Fatalf("action mutated after stale revision reject: %+v", currentAction)
	}
	if currentAction.Revision <= staleAction.Revision {
		t.Fatalf("expected interleaving row drift to advance revision, stale=%d current=%d", staleAction.Revision, currentAction.Revision)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get current action queue: %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("action queue status = %q, want OPEN after stale revision reject", currentQueue.Status)
	}
}

func TestResolveHumanActionRejectsStaleHumanActionRevision(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-direct-resolve-stale-revision"
		taskID      = "task-actions-direct-resolve-stale-revision"
		agentID     = "agent-actions-direct-resolve-stale-revision"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Direct Resolve Stale Revision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Direct Revision Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	actionID, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Need direct revision guard",
	})
	if err != nil {
		t.Fatalf("create action: %v", err)
	}
	staleAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get stale action: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE human_actions SET assigned_to = ?, revision = revision + 1 WHERE action_id = ?`,
		"reviewer-b",
		actionID,
	); err != nil {
		t.Fatalf("interleave human action revision: %v", err)
	}

	err = store.ResolveHumanAction(ctx, actionID, model.ActionStatusCompleted, "done", "developer", staleAction)
	if err == nil {
		t.Fatal("expected stale human_action revision reject")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "updated concurrently") {
		t.Fatalf("expected updated concurrently error, got %v", err)
	}

	currentAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get current action: %v", err)
	}
	if currentAction.Status != model.ActionStatusPending || currentAction.ResolvedAt != "" || currentAction.ResolutionComment != "" {
		t.Fatalf("action mutated after stale revision reject: %+v", currentAction)
	}
	if currentAction.Revision <= staleAction.Revision {
		t.Fatalf("expected interleaving row drift to advance revision, stale=%d current=%d", staleAction.Revision, currentAction.Revision)
	}
}
