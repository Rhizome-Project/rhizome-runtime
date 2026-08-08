package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestBlockingHumanActionsKeepTaskBlockedUntilLastOneResolves(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions"
		taskID      = "task-actions"
		agentID     = "agent-actions"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Agent",
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
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	actionA, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need approval A",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("create action A: %v", err)
	}
	actionB, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need approval B",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("create action B: %v", err)
	}

	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusBlocked)

	actionARecord, err := store.GetHumanAction(ctx, actionA)
	if err != nil {
		t.Fatalf("get action A: %v", err)
	}
	if err := store.ResolveHumanAction(ctx, actionA, "COMPLETED", "done", "developer", actionARecord); err != nil {
		t.Fatalf("resolve action A: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusBlocked)

	actionBRecord, err := store.GetHumanAction(ctx, actionB)
	if err != nil {
		t.Fatalf("get action B: %v", err)
	}
	if err := store.ResolveHumanAction(ctx, actionB, "COMPLETED", "done", "developer", actionBRecord); err != nil {
		t.Fatalf("resolve action B: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
}

func TestBlockingHumanActionCreatesAndRemovesSyntheticClaimForUnclaimedTask(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-synth"
		taskID      = "task-actions-synth"
		agentID     = "agent-actions-synth"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Synthetic",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Synthetic Agent",
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
		Title:       "Need synthetic blocker",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("create blocking action: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusBlocked)

	actionRecord, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get blocking action: %v", err)
	}
	if err := store.ResolveHumanAction(ctx, actionID, "COMPLETED", "done", "developer", actionRecord); err != nil {
		t.Fatalf("resolve blocking action: %v", err)
	}
	assertWorkspaceTaskClaimStatusNil(t, ctx, store, workspaceID, taskID)
}

func TestBlockingHumanActionRestoresReleasedClaimAfterResolve(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-release-restore"
		taskID      = "task-actions-release-restore"
		agentID     = "agent-actions-release-restore"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Release Restore",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Release Restore Agent",
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
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "waiting",
	}); err != nil {
		t.Fatalf("release task claim: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusReleased)

	actionID, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need release restore",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("create blocking action: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusBlocked)

	actionRecord, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get blocking action: %v", err)
	}
	if err := store.ResolveHumanAction(ctx, actionID, "COMPLETED", "done", "developer", actionRecord); err != nil {
		t.Fatalf("resolve blocking action: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusReleased)
}

func TestCreateHumanActionWithQueueEffectsRollsBackWhenLinkedSourceQueueLinkFails(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-queue-rollback"
		taskID      = "task-actions-create-queue-rollback"
		agentID     = "agent-actions-create-queue-rollback"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Queue Rollback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Queue Rollback Agent",
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
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	queueRecord := sqlite.OperatorQueueRecord{
		QueueID:           "opq-create-rollback",
		WorkspaceID:       workspaceID,
		QueueKey:          "tension_rebase_followup:create-rollback",
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase create rollback",
		Details:           "Pending rebase follow-up",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-create-rollback",
		TaskID:            taskID,
		AgentID:           "ghost-agent",
		KeepSessionActive: true,
	}
	payload := model.RebaseFollowupPayload{
		CoalitionID:       "coal-create-rollback",
		ForkTensionID:     "tens-fork-create-rollback",
		RepairTensionID:   "tens-repair-create-rollback",
		NextAction:        model.RebaseNextActionAttempt,
		ConflictSafeClass: "rebase_candidate",
	}
	result, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need create rollback",
		Blocking:    true,
	}, &queueRecord, &payload)
	if err == nil {
		t.Fatalf("expected linked source queue link failure, got result %+v", result)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no human actions after rollback, got %d", len(actions))
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)

	queueItems, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("list operator queue items: %v", err)
	}
	if len(queueItems) != 0 {
		t.Fatalf("expected no operator queue items after rollback, got %d", len(queueItems))
	}
}

func TestResolveHumanActionWithQueueEffectsRollsBackWhenLinkedSourceQueueResolutionFails(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-queue-rollback"
		taskID      = "task-actions-queue-rollback"
		agentID     = "agent-actions-queue-rollback"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Queue Rollback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Queue Rollback Agent",
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
		Title:       "Need queue rollback",
		Blocking:    false,
	})
	if err != nil {
		t.Fatalf("create action: %v", err)
	}

	actionQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "action:" + actionID,
		QueueType:   "FOLLOW_UP",
		Title:       "Need queue rollback",
		Urgency:     "NORMAL",
		SourceKind:  "human_action",
		SourceID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("create action queue: %v", err)
	}

	currentAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action(before): %v", err)
	}

	result, err := store.ResolveHumanActionWithQueueEffects(ctx, actionID, "COMPLETED", "done", "developer", &sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     actionQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "developer",
		Resolution:  "done",
	}, []sqlite.OperatorQueueResolveInput{{
		WorkspaceID: workspaceID,
		QueueKey:    "tension_rebase_followup:missing-linked-queue",
		Status:      "RESOLVED",
		ResolvedBy:  "developer",
		Resolution:  "missing_queue",
	}}, nil, nil, currentAction)
	if err == nil {
		t.Fatalf("expected linked source queue failure, got result %+v", result)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	if action.Status != model.ActionStatusPending {
		t.Fatalf("action status = %q, want %q after rollback", action.Status, model.ActionStatusPending)
	}
	if action.ResolvedAt != "" || action.ResolutionComment != "" {
		t.Fatalf("expected action resolution fields to roll back, got %+v", action)
	}

	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get action queue: %v", err)
	}
	if queue.Status != "OPEN" {
		t.Fatalf("action queue status = %q, want OPEN after rollback", queue.Status)
	}
	if queue.Resolution != "" {
		t.Fatalf("action queue resolution = %q, want empty after rollback", queue.Resolution)
	}
}

func TestCreateHumanActionWithQueueEffectsRollsBackWhenLinkedSourceQueueIsNotOpen(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-rollback"
		taskID      = "task-actions-create-rollback"
		agentID     = "agent-actions-create-rollback"
		queueKey    = "tension_rebase_followup:create-rollback"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Rollback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Rollback Agent",
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

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    queueKey,
		QueueType:   "FOLLOW_UP",
		Title:       "Attempt bounded overlap rebase",
		Urgency:     "HIGH",
		SourceKind:  "tension",
		SourceID:    "tens-repair-create-rollback",
		TaskID:      taskID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("create source queue: %v", err)
	}
	if sourceQueue.Revision != 1 {
		t.Fatalf("expected initial source queue revision 1, got %d", sourceQueue.Revision)
	}
	resolvedSourceQueue, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "developer",
		Resolution:  "preclosed",
	})
	if err != nil {
		t.Fatalf("resolve source queue precondition: %v", err)
	}

	result, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need create rollback",
		Blocking:    true,
	}, &resolvedSourceQueue, &model.RebaseFollowupPayload{
		RepairTensionID: "tens-repair-create-rollback",
		NextAction:      model.RebaseNextActionAttempt,
	})
	if err == nil {
		t.Fatalf("expected linked source queue precondition failure, got result %+v", result)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no persisted actions after rollback, got %+v", actions)
	}

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list operator queues: %v", err)
	}
	for _, item := range items {
		if item.SourceKind == "human_action" {
			t.Fatalf("unexpected human action queue after rollback: %+v", item)
		}
	}
	assertWorkspaceTaskClaimStatusNil(t, ctx, store, workspaceID, taskID)
}

func TestCreateHumanActionWithQueueEffectsRejectsStaleRetryPromotionSnapshot(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-stale-retry"
		taskID      = "task-actions-create-stale-retry"
		agentID     = "agent-actions-create-stale-retry"
		queueKey    = "tension_rebase_followup:create-stale-retry"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Stale Retry",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Stale Retry Agent",
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
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase stale retry snapshot",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       `{"repair_tension_id":"tens-repair-create-stale-retry","next_action":"attempt_rebase"}`,
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-stale-retry",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create source queue: %v", err)
	}

	stalePayload := model.RebaseFollowupPayload{
		RepairTensionID: "tens-repair-create-stale-retry",
		NextAction:      model.RebaseNextActionAttempt,
	}

	first, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need first stale retry guard pass",
		Blocking:    true,
	}, &sourceQueue, &stalePayload)
	if err != nil {
		t.Fatalf("first CreateHumanActionWithQueueEffects: %v", err)
	}
	if strings.TrimSpace(first.Action.ActionID) == "" {
		t.Fatalf("expected first action to persist, got %+v", first)
	}

	second, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need second stale retry guard pass",
		Blocking:    true,
	}, &sourceQueue, &stalePayload)
	if err == nil {
		t.Fatalf("expected stale retry promotion snapshot to fail, got %+v", second)
	}
	if !strings.Contains(err.Error(), "already linked to action") {
		t.Fatalf("expected active-link rejection, got %v", err)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected only one persisted action after stale retry rejection, got %+v", actions)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated source queue: %v", err)
	}
	var payload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(updatedSourceQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode updated source queue payload: %v", err)
	}
	if payload.ActionID != first.Action.ActionID {
		t.Fatalf("source queue action_id = %q, want %q", payload.ActionID, first.Action.ActionID)
	}
}

func TestCreateHumanActionWithQueueEffectsRejectsAssignedToDriftDuringRehydrate(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-assigned-drift"
		taskID      = "task-actions-create-assigned-drift"
		agentID     = "agent-actions-create-assigned-drift"
		queueKey    = "tension_rebase_followup:create-assigned-drift"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Assigned Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Assigned Drift Agent",
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

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase assigned-to drift",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       `{"repair_tension_id":"tens-repair-create-assigned-drift","next_action":"attempt_rebase"}`,
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-assigned-drift",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create source queue: %v", err)
	}

	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get stale source queue: %v", err)
	}
	stalePayload := model.RebaseFollowupPayload{
		RepairTensionID: "tens-repair-create-assigned-drift",
		NextAction:      model.RebaseNextActionAttempt,
	}

	if _, _, _, err := store.EscalateOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		EscalatedBy: "lead-b",
		AssignedTo:  "reviewer-b",
		Reason:      "handoff before create",
	}); err != nil {
		t.Fatalf("escalate source queue before stale create: %v", err)
	}

	result, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Need assigned drift guard",
		Blocking:    true,
	}, &staleQueue, &stalePayload)
	if err == nil {
		t.Fatalf("expected assigned_to drift to fail, got %+v", result)
	}
	if !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected updated concurrently on assigned_to drift, got %v", err)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no persisted action after assigned_to drift rejection, got %+v", actions)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get current source queue: %v", err)
	}
	if currentQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("current source queue assigned_to = %q, want reviewer-b", currentQueue.AssignedTo)
	}
}

func TestCreateHumanActionWithQueueEffectsRejectsTaskOrAgentMismatchAgainstLinkedSourceQueue(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-context-guard"
		taskID      = "task-actions-create-context-guard"
		agentID     = "agent-actions-create-context-guard"
		queueKey    = "tension_rebase_followup:create-context-guard"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Context Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Context Guard Agent",
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

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase task or agent mismatch guard",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       `{"repair_tension_id":"tens-repair-create-context-guard","next_action":"attempt_rebase"}`,
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-context-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create source queue: %v", err)
	}
	payload := model.RebaseFollowupPayload{
		RepairTensionID: "tens-repair-create-context-guard",
		NextAction:      model.RebaseNextActionAttempt,
	}

	testCases := []struct {
		name        string
		input       sqlite.HumanActionInput
		wantMessage string
	}{
		{
			name: "task mismatch",
			input: sqlite.HumanActionInput{
				WorkspaceID: workspaceID,
				TaskID:      "task-actions-create-context-other",
				AgentID:     agentID,
				AssignedTo:  "reviewer-a",
				Title:       "Need rebase task mismatch guard",
				Blocking:    true,
			},
			wantMessage: "source queue belongs to task " + taskID,
		},
		{
			name: "agent mismatch",
			input: sqlite.HumanActionInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				AgentID:     "agent-actions-create-context-other",
				AssignedTo:  "reviewer-a",
				Title:       "Need rebase agent mismatch guard",
				Blocking:    true,
			},
			wantMessage: "source queue belongs to agent " + agentID,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actionEventsBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list action.created events before rejected create: %v", err)
			}
			sourceUpdatesBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list source queue updates before rejected create: %v", err)
			}

			result, err := store.CreateHumanActionWithQueueEffects(ctx, tc.input, &sourceQueue, &payload)
			if err == nil {
				t.Fatalf("expected linked source context mismatch to fail, got %+v", result)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("expected linked source context mismatch %q, got %v", tc.wantMessage, err)
			}

			actions, err := store.ListHumanActions(ctx, workspaceID, "")
			if err != nil {
				t.Fatalf("list actions after rejected create: %v", err)
			}
			if len(actions) != 0 {
				t.Fatalf("expected no persisted action after linked source context rejection, got %+v", actions)
			}

			actionEventsAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list action.created events after rejected create: %v", err)
			}
			if len(actionEventsAfter) != len(actionEventsBefore) {
				t.Fatalf("expected no new action.created runtime events after linked source context rejection, before=%d after=%d", len(actionEventsBefore), len(actionEventsAfter))
			}
			sourceUpdatesAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list source queue updates after rejected create: %v", err)
			}
			if len(sourceUpdatesAfter) != len(sourceUpdatesBefore) {
				t.Fatalf("expected no new source queue updates after linked source context rejection, before=%d after=%d", len(sourceUpdatesBefore), len(sourceUpdatesAfter))
			}

			currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
			if err != nil {
				t.Fatalf("get source queue after rejected create: %v", err)
			}
			if currentQueue.UpdatedAt != sourceQueue.UpdatedAt {
				t.Fatalf("source queue updated_at after linked source context rejection = %q, want %q", currentQueue.UpdatedAt, sourceQueue.UpdatedAt)
			}
			if currentQueue.PayloadJSON != sourceQueue.PayloadJSON {
				t.Fatalf("source queue payload changed after linked source context rejection: %s", currentQueue.PayloadJSON)
			}
		})
	}
}

func TestCreateHumanActionWithQueueEffectsRejectsTaskOrAgentMismatchAgainstQueueOnlyLinkedSourceQueue(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID     = "ws-actions-create-tension-context-guard"
		taskID          = "task-actions-create-tension-context-guard"
		agentID         = "agent-actions-create-tension-context-guard"
		queueKey        = "tension_rebase_followup:create-tension-context-guard"
		repairTensionID = "tens-repair-create-tension-context-guard"
		forkTensionID   = "tens-fork-create-tension-context-guard"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Tension Context Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Tension Context Guard Agent",
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
	seedLinkedSourceQueueTensionContextForTest(t, ctx, store, workspaceID, repairTensionID, taskID, agentID)
	seedLinkedSourceQueueTensionContextForTest(t, ctx, store, workspaceID, forkTensionID, taskID, agentID)

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase queue-only task or agent mismatch guard",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       `{"repair_tension_id":"tens-repair-create-tension-context-guard","fork_tension_id":"tens-fork-create-tension-context-guard","next_action":"attempt_rebase"}`,
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairTensionID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create queue-only source queue: %v", err)
	}
	payload := model.RebaseFollowupPayload{
		RepairTensionID: repairTensionID,
		ForkTensionID:   forkTensionID,
		NextAction:      model.RebaseNextActionAttempt,
	}

	testCases := []struct {
		name        string
		input       sqlite.HumanActionInput
		wantMessage string
	}{
		{
			name: "task mismatch",
			input: sqlite.HumanActionInput{
				WorkspaceID: workspaceID,
				TaskID:      "task-actions-create-tension-context-other",
				AgentID:     agentID,
				AssignedTo:  "reviewer-a",
				Title:       "Need queue-only rebase task mismatch guard",
				Blocking:    true,
			},
			wantMessage: "source queue belongs to task " + taskID,
		},
		{
			name: "agent mismatch",
			input: sqlite.HumanActionInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				AgentID:     "agent-actions-create-tension-context-other",
				AssignedTo:  "reviewer-a",
				Title:       "Need queue-only rebase agent mismatch guard",
				Blocking:    true,
			},
			wantMessage: "source queue belongs to agent " + agentID,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actionEventsBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list queue-only action.created events before rejected create: %v", err)
			}
			sourceUpdatesBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list queue-only source queue updates before rejected create: %v", err)
			}

			result, err := store.CreateHumanActionWithQueueEffects(ctx, tc.input, &sourceQueue, &payload)
			if err == nil {
				t.Fatalf("expected queue-only linked source context mismatch to fail, got %+v", result)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("expected queue-only linked source context mismatch %q, got %v", tc.wantMessage, err)
			}

			actions, err := store.ListHumanActions(ctx, workspaceID, "")
			if err != nil {
				t.Fatalf("list queue-only actions after rejected create: %v", err)
			}
			if len(actions) != 0 {
				t.Fatalf("expected no persisted action after queue-only linked source context rejection, got %+v", actions)
			}

			actionEventsAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list queue-only action.created events after rejected create: %v", err)
			}
			if len(actionEventsAfter) != len(actionEventsBefore) {
				t.Fatalf("expected no new queue-only action.created runtime events after linked source context rejection, before=%d after=%d", len(actionEventsBefore), len(actionEventsAfter))
			}
			sourceUpdatesAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list queue-only source queue updates after rejected create: %v", err)
			}
			if len(sourceUpdatesAfter) != len(sourceUpdatesBefore) {
				t.Fatalf("expected no new queue-only source queue updates after linked source context rejection, before=%d after=%d", len(sourceUpdatesBefore), len(sourceUpdatesAfter))
			}

			currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
			if err != nil {
				t.Fatalf("get queue-only source queue after rejected create: %v", err)
			}
			if currentQueue.UpdatedAt != sourceQueue.UpdatedAt {
				t.Fatalf("queue-only source queue updated_at after linked source context rejection = %q, want %q", currentQueue.UpdatedAt, sourceQueue.UpdatedAt)
			}
			if currentQueue.PayloadJSON != sourceQueue.PayloadJSON {
				t.Fatalf("queue-only source queue payload changed after linked source context rejection: %s", currentQueue.PayloadJSON)
			}
		})
	}
}

func TestCreateHumanActionWithQueueEffectsRehydratesCurrentQueueLineageDuringRehydrate(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-lineage-drift"
		taskID      = "task-actions-create-lineage-drift"
		agentID     = "agent-actions-create-lineage-drift"
		queueKey    = "tension_rebase_followup:create-lineage-drift"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Lineage Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Lineage Drift Agent",
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

	sourceQueue, sourceQueueCreateEvent, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase lineage drift",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       `{"repair_tension_id":"tens-repair-create-lineage-drift","next_action":"attempt_rebase","root_cause_id":"root-old","provenance_group_id":"prov-old"}`,
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-lineage-drift",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create source queue: %v", err)
	}

	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get stale source queue: %v", err)
	}
	stalePayload := model.RebaseFollowupPayload{
		RepairTensionID:   "tens-repair-create-lineage-drift",
		NextAction:        model.RebaseNextActionAttempt,
		RootCauseID:       "root-old",
		ProvenanceGroupID: "prov-old",
	}

	freshPayload := stalePayload
	freshPayload.RootCauseID = "root-new"
	freshPayload.ProvenanceGroupID = "prov-new"
	freshPayload.ParentRefsJSON = []string{sourceQueueCreateEvent.EventID}
	freshPayload.Normalize()

	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:           sourceQueue.QueueID,
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase lineage drift",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       string(mustJSON(freshPayload)),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-lineage-drift",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	}); err != nil {
		t.Fatalf("update source queue lineage before stale create: %v", err)
	}
	refreshedQueueBeforeCreate, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get refreshed source queue before create: %v", err)
	}
	if refreshedQueueBeforeCreate.Revision != 2 {
		t.Fatalf("expected refreshed source queue revision 2 before create, got %d", refreshedQueueBeforeCreate.Revision)
	}

	actionEventsBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list action.created events before create rehydrate: %v", err)
	}
	sourceUpdatesBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list source queue updates before create rehydrate: %v", err)
	}

	result, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Need lineage drift guard",
		Blocking:    true,
	}, &staleQueue, &stalePayload)
	if err != nil {
		t.Fatalf("CreateHumanActionWithQueueEffects: %v", err)
	}
	if strings.TrimSpace(result.Action.ActionID) == "" {
		t.Fatalf("expected persisted action after lineage-only rehydrate, got %+v", result)
	}

	actionEventsAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list action.created events after create rehydrate: %v", err)
	}
	if len(actionEventsAfter) != len(actionEventsBefore)+1 {
		t.Fatalf("expected exactly one new action.created runtime event after lineage-only rehydrate, before=%d after=%d", len(actionEventsBefore), len(actionEventsAfter))
	}
	sourceUpdatesAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list source queue updates after create rehydrate: %v", err)
	}
	if len(sourceUpdatesAfter) != len(sourceUpdatesBefore)+1 {
		t.Fatalf("expected action link source queue update after lineage refresh baseline, before=%d after=%d", len(sourceUpdatesBefore), len(sourceUpdatesAfter))
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions after lineage-only rehydrate create: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one persisted action after lineage-only rehydrate create, got %+v", actions)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get current source queue: %v", err)
	}
	if currentQueue.UpdatedAt == staleQueue.UpdatedAt {
		t.Fatalf("expected source queue updated_at to change after lineage update")
	}
	if currentQueue.Revision != 3 {
		t.Fatalf("expected action-linked source queue revision 3 after create, got %d", currentQueue.Revision)
	}
	var currentPayload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(currentQueue.PayloadJSON), &currentPayload); err != nil {
		t.Fatalf("decode current source queue payload: %v", err)
	}
	if currentPayload.RootCauseID != "root-new" || currentPayload.ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected fresh lineage to remain authoritative after rehydrate create, got %+v", currentPayload)
	}
	if len(currentPayload.ParentRefsJSON) != 1 || currentPayload.ParentRefsJSON[0] != sourceQueueCreateEvent.EventID {
		t.Fatalf("expected fresh parent refs after lineage-only rehydrate create, got %+v", currentPayload.ParentRefsJSON)
	}
	if currentPayload.ActionID != result.Action.ActionID || strings.TrimSpace(currentPayload.ActionQueueKey) == "" {
		t.Fatalf("expected linked source queue after lineage-only rehydrate create, got %+v", currentPayload)
	}

	actionEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    result.Action.ActionID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list action.created runtime for lineage-only rehydrate create: %v", err)
	}
	if len(actionEvents) != 1 {
		t.Fatalf("expected action.created runtime event for lineage-only rehydrate create, got %+v", actionEvents)
	}
	if actionEvents[0].RootCauseID != "root-new" || actionEvents[0].ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected action.created runtime to use fresh lineage, got %+v", actionEvents[0])
	}
	var actionParentRefs []string
	if err := json.Unmarshal([]byte(actionEvents[0].ParentRefsJSON), &actionParentRefs); err != nil {
		t.Fatalf("decode action.created parent refs: %v", err)
	}
	if len(actionParentRefs) == 0 || !strings.Contains(strings.Join(actionParentRefs, ","), sourceQueueCreateEvent.EventID) {
		t.Fatalf("expected action.created parent refs to retain fresh lineage parent, got %+v", actionParentRefs)
	}
}

func TestCreateHumanActionWithQueueEffectsRollsBackWhenActionCreatedRuntimeEventAppendFails(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-runtime-rollback"
		taskID      = "task-actions-create-runtime-rollback"
		agentID     = "agent-actions-create-runtime-rollback"
		queueKey    = "tension_rebase_followup:create-runtime-rollback"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Create Runtime Rollback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Create Runtime Rollback Agent",
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

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Create runtime rollback guard",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       `{"repair_tension_id":"tens-repair-create-runtime-rollback","next_action":"attempt_rebase"}`,
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-runtime-rollback",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create source queue: %v", err)
	}
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get source queue before: %v", err)
	}

	actionEventsBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list action.created events before: %v", err)
	}
	sourceUpdatesBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list source queue updates before: %v", err)
	}

	result, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need runtime create rollback",
		Blocking:    true,
	}, &sourceQueue, &model.RebaseFollowupPayload{
		RepairTensionID: "tens-repair-create-runtime-rollback",
		NextAction:      model.RebaseNextActionAttempt,
	}, sqlite.RuntimeEventInput{
		TaskID: taskID + "-missing",
	})
	if err == nil {
		t.Fatalf("expected action.created runtime append failure, got %+v", result)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no persisted actions after rollback, got %+v", actions)
	}

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list operator queues: %v", err)
	}
	for _, item := range items {
		if item.SourceKind == "human_action" {
			t.Fatalf("unexpected human action queue after rollback: %+v", item)
		}
	}

	sourceQueueAfter, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get source queue after: %v", err)
	}
	if sourceQueueAfter.Status != sourceQueueBefore.Status || sourceQueueAfter.UpdatedAt != sourceQueueBefore.UpdatedAt || sourceQueueAfter.PayloadJSON != sourceQueueBefore.PayloadJSON {
		t.Fatalf("expected source queue to stay unchanged after rollback, before=%+v after=%+v", sourceQueueBefore, sourceQueueAfter)
	}

	actionEventsAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list action.created events after: %v", err)
	}
	if len(actionEventsAfter) != len(actionEventsBefore) {
		t.Fatalf("expected no new action.created runtime events after rollback, before=%d after=%d", len(actionEventsBefore), len(actionEventsAfter))
	}
	sourceUpdatesAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list source queue updates after: %v", err)
	}
	if len(sourceUpdatesAfter) != len(sourceUpdatesBefore) {
		t.Fatalf("expected no new source queue updates after rollback, before=%d after=%d", len(sourceUpdatesBefore), len(sourceUpdatesAfter))
	}

	assertWorkspaceTaskClaimStatusNil(t, ctx, store, workspaceID, taskID)
}

func TestCreateHumanActionWithRollbackFailureQueueEffectsRejectsStaleRetryPromotionSnapshot(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-rollback-stale-retry"
		taskID      = "task-actions-create-rollback-stale-retry"
		agentID     = "agent-actions-create-rollback-stale-retry"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "create-stale-retry"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Rollback Stale Retry",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Rollback Retry Agent",
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
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Rollback stale retry snapshot",
		Details:           "Pending rollback failure follow-up",
		PayloadJSON:       `{"kind":"rebase_rollback_failure","failure_scope":"run_verify","failure_trigger":"verifier_late_fail_run","task_id":"task-actions-create-rollback-stale-retry","agent_id":"agent-actions-create-rollback-stale-retry","repair_tension_id":"tens-repair-create-rollback-stale-retry"}`,
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-stale-retry",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rollback-failure queue: %v", err)
	}
	if sourceQueue.Revision != 1 {
		t.Fatalf("expected initial rollback-failure queue revision 1, got %d", sourceQueue.Revision)
	}

	stalePayload := model.RebaseRollbackFailurePayload{
		Kind:            model.RebaseRollbackFailureKind,
		FailureScope:    "run_verify",
		FailureTrigger:  "verifier_late_fail_run",
		TaskID:          taskID,
		AgentID:         agentID,
		RepairTensionID: "tens-repair-create-rollback-stale-retry",
	}

	first, err := store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need first rollback retry guard pass",
		Blocking:    true,
	}, &sourceQueue, &stalePayload)
	if err != nil {
		t.Fatalf("first CreateHumanActionWithRollbackFailureQueueEffects: %v", err)
	}
	if strings.TrimSpace(first.Action.ActionID) == "" {
		t.Fatalf("expected first rollback action to persist, got %+v", first)
	}

	second, err := store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need second rollback retry guard pass",
		Blocking:    true,
	}, &sourceQueue, &stalePayload)
	if err == nil {
		t.Fatalf("expected rollback stale retry promotion snapshot to fail, got %+v", second)
	}
	if !strings.Contains(err.Error(), "already linked to action") {
		t.Fatalf("expected active rollback link rejection, got %v", err)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected only one persisted rollback action after stale retry rejection, got %+v", actions)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated rollback-failure queue: %v", err)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(updatedSourceQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode updated rollback-failure queue payload: %v", err)
	}
	if payload.FollowupActionID != first.Action.ActionID {
		t.Fatalf("source queue followup_action_id = %q, want %q", payload.FollowupActionID, first.Action.ActionID)
	}
}

func TestCreateHumanActionWithRollbackFailureQueueEffectsRejectsTaskOrAgentMismatchAgainstLinkedSourceQueue(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-rollback-context-guard"
		taskID      = "task-actions-create-rollback-context-guard"
		agentID     = "agent-actions-create-rollback-context-guard"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "create-context-guard"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Rollback Context Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Rollback Context Guard Agent",
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

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Rollback task or agent mismatch guard",
		Details:           "Pending rollback failure follow-up",
		PayloadJSON:       `{"kind":"rebase_rollback_failure","failure_scope":"run_verify","failure_trigger":"verifier_late_fail_run","task_id":"task-actions-create-rollback-context-guard","agent_id":"agent-actions-create-rollback-context-guard","repair_tension_id":"tens-repair-create-rollback-context-guard"}`,
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-context-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rollback-failure queue: %v", err)
	}
	payload := model.RebaseRollbackFailurePayload{
		Kind:            model.RebaseRollbackFailureKind,
		FailureScope:    "run_verify",
		FailureTrigger:  "verifier_late_fail_run",
		TaskID:          taskID,
		AgentID:         agentID,
		RepairTensionID: "tens-repair-create-rollback-context-guard",
	}

	testCases := []struct {
		name        string
		input       sqlite.HumanActionInput
		wantMessage string
	}{
		{
			name: "task mismatch",
			input: sqlite.HumanActionInput{
				WorkspaceID: workspaceID,
				TaskID:      "task-actions-create-rollback-context-other",
				AgentID:     agentID,
				Title:       "Need rollback task mismatch guard",
				Blocking:    true,
			},
			wantMessage: "source queue belongs to task " + taskID,
		},
		{
			name: "agent mismatch",
			input: sqlite.HumanActionInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				AgentID:     "agent-actions-create-rollback-context-other",
				Title:       "Need rollback agent mismatch guard",
				Blocking:    true,
			},
			wantMessage: "source queue belongs to agent " + agentID,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actionEventsBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list rollback action.created events before rejected create: %v", err)
			}
			sourceUpdatesBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list rollback source queue updates before rejected create: %v", err)
			}

			result, err := store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, tc.input, &sourceQueue, &payload)
			if err == nil {
				t.Fatalf("expected linked rollback-failure source context mismatch to fail, got %+v", result)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("expected linked rollback-failure source context mismatch %q, got %v", tc.wantMessage, err)
			}

			actions, err := store.ListHumanActions(ctx, workspaceID, "")
			if err != nil {
				t.Fatalf("list rollback actions after rejected create: %v", err)
			}
			if len(actions) != 0 {
				t.Fatalf("expected no persisted rollback action after linked source context rejection, got %+v", actions)
			}

			actionEventsAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list rollback action.created events after rejected create: %v", err)
			}
			if len(actionEventsAfter) != len(actionEventsBefore) {
				t.Fatalf("expected no new rollback action.created runtime events after linked source context rejection, before=%d after=%d", len(actionEventsBefore), len(actionEventsAfter))
			}
			sourceUpdatesAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list rollback source queue updates after rejected create: %v", err)
			}
			if len(sourceUpdatesAfter) != len(sourceUpdatesBefore) {
				t.Fatalf("expected no new rollback source queue updates after linked source context rejection, before=%d after=%d", len(sourceUpdatesBefore), len(sourceUpdatesAfter))
			}

			currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
			if err != nil {
				t.Fatalf("get rollback source queue after rejected create: %v", err)
			}
			if currentQueue.UpdatedAt != sourceQueue.UpdatedAt {
				t.Fatalf("rollback source queue updated_at after linked source context rejection = %q, want %q", currentQueue.UpdatedAt, sourceQueue.UpdatedAt)
			}
			if currentQueue.PayloadJSON != sourceQueue.PayloadJSON {
				t.Fatalf("rollback source queue payload changed after linked source context rejection: %s", currentQueue.PayloadJSON)
			}
		})
	}
}

func seedLinkedSourceQueueTensionContextForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, taskID, agentID string) {
	t.Helper()

	taskIDsJSON, err := json.Marshal([]string{taskID})
	if err != nil {
		t.Fatalf("marshal tension task ids: %v", err)
	}
	agentIDsJSON, err := json.Marshal([]string{agentID})
	if err != nil {
		t.Fatalf("marshal tension agent ids: %v", err)
	}
	emptyJSON := "[]"
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions(
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json,
			segment_refs_json, agent_ids_json, constraint_refs_json, base_score, surface_score, evidence_count,
			last_seen_event_id, last_seen_at, last_detected_at, last_refreshed_at, stale_refresh_count,
			confirmed_by, archived_by, dismissed_reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(tension_id) DO UPDATE SET
			task_ids_json = excluded.task_ids_json,
			agent_ids_json = excluded.agent_ids_json,
			updated_at = excluded.updated_at
	`,
		tensionID,
		workspaceID,
		"",
		"repair",
		"ACTIVE",
		"PENDING",
		"Linked source queue context",
		"Tension-derived task and agent context",
		"queue",
		"operator_queue",
		string(taskIDsJSON),
		emptyJSON,
		emptyJSON,
		emptyJSON,
		emptyJSON,
		string(agentIDsJSON),
		emptyJSON,
		1,
		1,
		0,
		"",
		now,
		now,
		now,
		0,
		"",
		"",
		"",
		now,
		now,
	); err != nil {
		t.Fatalf("seed linked source queue tension context %s: %v", tensionID, err)
	}
}

func TestCreateHumanActionWithRollbackFailureQueueEffectsRehydratesCurrentQueueLineageDuringRehydrate(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-rollback-lineage-drift"
		taskID      = "task-actions-create-rollback-lineage-drift"
		agentID     = "agent-actions-create-rollback-lineage-drift"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "create-lineage-drift"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Rollback Lineage Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Rollback Lineage Drift Agent",
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

	sourceQueue, sourceQueueCreateEvent, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Rollback lineage drift",
		Details:           "Pending rollback failure follow-up",
		PayloadJSON:       `{"kind":"rebase_rollback_failure","failure_scope":"run_verify","failure_trigger":"verifier_late_fail_run","task_id":"task-actions-create-rollback-lineage-drift","agent_id":"agent-actions-create-rollback-lineage-drift","repair_tension_id":"tens-repair-create-rollback-lineage-drift","root_cause_id":"root-old","provenance_group_id":"prov-old"}`,
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-lineage-drift",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rollback-failure queue: %v", err)
	}

	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get stale rollback-failure queue: %v", err)
	}
	stalePayload := model.RebaseRollbackFailurePayload{
		Kind:              model.RebaseRollbackFailureKind,
		FailureScope:      "run_verify",
		FailureTrigger:    "verifier_late_fail_run",
		TaskID:            taskID,
		AgentID:           agentID,
		RepairTensionID:   "tens-repair-create-rollback-lineage-drift",
		RootCauseID:       "root-old",
		ProvenanceGroupID: "prov-old",
	}

	freshPayload := stalePayload
	freshPayload.RootCauseID = "root-new"
	freshPayload.ProvenanceGroupID = "prov-new"
	freshPayload.ParentRefsJSON = []string{sourceQueueCreateEvent.EventID}
	freshPayload.Normalize()

	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:           sourceQueue.QueueID,
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Rollback lineage drift",
		Details:           "Pending rollback failure follow-up",
		PayloadJSON:       string(mustJSON(freshPayload)),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-lineage-drift",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	}); err != nil {
		t.Fatalf("update rollback-failure lineage before stale create: %v", err)
	}
	refreshedQueueBeforeCreate, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get refreshed rollback-failure queue before create: %v", err)
	}
	if refreshedQueueBeforeCreate.Revision != 2 {
		t.Fatalf("expected refreshed rollback-failure queue revision 2 before create, got %d", refreshedQueueBeforeCreate.Revision)
	}

	actionEventsBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback action.created events before create rehydrate: %v", err)
	}
	sourceUpdatesBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback source queue updates before create rehydrate: %v", err)
	}

	result, err := store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Need rollback lineage drift guard",
		Blocking:    true,
	}, &staleQueue, &stalePayload)
	if err != nil {
		t.Fatalf("CreateHumanActionWithRollbackFailureQueueEffects: %v", err)
	}
	if strings.TrimSpace(result.Action.ActionID) == "" {
		t.Fatalf("expected persisted rollback action after lineage-only rehydrate, got %+v", result)
	}

	actionEventsAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback action.created events after create rehydrate: %v", err)
	}
	if len(actionEventsAfter) != len(actionEventsBefore)+1 {
		t.Fatalf("expected exactly one new rollback action.created runtime event after lineage-only rehydrate, before=%d after=%d", len(actionEventsBefore), len(actionEventsAfter))
	}
	sourceUpdatesAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback source queue updates after create rehydrate: %v", err)
	}
	if len(sourceUpdatesAfter) != len(sourceUpdatesBefore)+1 {
		t.Fatalf("expected rollback follow-up action link source queue update after lineage refresh baseline, before=%d after=%d", len(sourceUpdatesBefore), len(sourceUpdatesAfter))
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions after rollback lineage-only rehydrate create: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one persisted rollback action after lineage-only rehydrate create, got %+v", actions)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get current rollback-failure queue: %v", err)
	}
	if currentQueue.UpdatedAt == staleQueue.UpdatedAt {
		t.Fatalf("expected rollback-failure source queue updated_at to change after lineage update")
	}
	if currentQueue.Revision != 3 {
		t.Fatalf("expected rollback-failure source queue revision 3 after create, got %d", currentQueue.Revision)
	}
	var currentPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(currentQueue.PayloadJSON), &currentPayload); err != nil {
		t.Fatalf("decode current rollback-failure queue payload: %v", err)
	}
	if currentPayload.RootCauseID != "root-new" || currentPayload.ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected fresh rollback-failure lineage to remain authoritative after rehydrate create, got %+v", currentPayload)
	}
	if len(currentPayload.ParentRefsJSON) != 1 || currentPayload.ParentRefsJSON[0] != sourceQueueCreateEvent.EventID {
		t.Fatalf("expected fresh rollback-failure parent refs after lineage-only rehydrate create, got %+v", currentPayload.ParentRefsJSON)
	}
	if currentPayload.FollowupActionID != result.Action.ActionID || strings.TrimSpace(currentPayload.FollowupActionQueueKey) == "" || strings.TrimSpace(currentPayload.FollowupActionStatus) == "" {
		t.Fatalf("expected linked rollback-failure source queue after lineage-only rehydrate create, got %+v", currentPayload)
	}

	actionEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    result.Action.ActionID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list rollback action.created runtime for lineage-only rehydrate create: %v", err)
	}
	if len(actionEvents) != 1 {
		t.Fatalf("expected rollback action.created runtime event for lineage-only rehydrate create, got %+v", actionEvents)
	}
	if actionEvents[0].RootCauseID != "root-new" || actionEvents[0].ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected rollback action.created runtime to use fresh lineage, got %+v", actionEvents[0])
	}
	var actionParentRefs []string
	if err := json.Unmarshal([]byte(actionEvents[0].ParentRefsJSON), &actionParentRefs); err != nil {
		t.Fatalf("decode rollback action.created parent refs: %v", err)
	}
	if len(actionParentRefs) == 0 || !strings.Contains(strings.Join(actionParentRefs, ","), sourceQueueCreateEvent.EventID) {
		t.Fatalf("expected rollback action.created parent refs to retain fresh lineage parent, got %+v", actionParentRefs)
	}
}

func TestCreateHumanActionWithRollbackFailureQueueEffectsRollsBackWhenActionCreatedRuntimeEventAppendFails(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-actions-create-rollback-runtime-rollback"
		taskID      = "task-actions-create-rollback-runtime-rollback"
		agentID     = "agent-actions-create-rollback-runtime-rollback"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "create-runtime-rollback"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Actions Rollback Create Runtime Rollback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Actions Rollback Create Runtime Rollback Agent",
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

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Create rollback runtime rollback guard",
		Details:           "Pending rollback failure follow-up",
		PayloadJSON:       `{"kind":"rebase_rollback_failure","failure_scope":"run_verify","failure_trigger":"verifier_late_fail_run","task_id":"task-actions-create-rollback-runtime-rollback","agent_id":"agent-actions-create-rollback-runtime-rollback","repair_tension_id":"tens-repair-create-rollback-runtime-rollback"}`,
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-runtime-rollback",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rollback-failure queue: %v", err)
	}
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get rollback-failure queue before: %v", err)
	}

	actionEventsBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback action.created events before: %v", err)
	}
	sourceUpdatesBefore, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback source queue updates before: %v", err)
	}

	result, err := store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need rollback runtime create rollback",
		Blocking:    true,
	}, &sourceQueue, &model.RebaseRollbackFailurePayload{
		Kind:            model.RebaseRollbackFailureKind,
		FailureScope:    "run_verify",
		FailureTrigger:  "verifier_late_fail_run",
		TaskID:          taskID,
		AgentID:         agentID,
		RepairTensionID: "tens-repair-create-rollback-runtime-rollback",
	}, sqlite.RuntimeEventInput{
		TaskID: taskID + "-missing",
	})
	if err == nil {
		t.Fatalf("expected rollback-failure action.created runtime append failure, got %+v", result)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list rollback actions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no persisted rollback actions after rollback, got %+v", actions)
	}

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list operator queues: %v", err)
	}
	for _, item := range items {
		if item.SourceKind == "human_action" {
			t.Fatalf("unexpected rollback human action queue after rollback: %+v", item)
		}
	}

	sourceQueueAfter, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get rollback-failure queue after: %v", err)
	}
	if sourceQueueAfter.Status != sourceQueueBefore.Status || sourceQueueAfter.UpdatedAt != sourceQueueBefore.UpdatedAt || sourceQueueAfter.PayloadJSON != sourceQueueBefore.PayloadJSON {
		t.Fatalf("expected rollback-failure queue to stay unchanged after rollback, before=%+v after=%+v", sourceQueueBefore, sourceQueueAfter)
	}

	actionEventsAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback action.created events after: %v", err)
	}
	if len(actionEventsAfter) != len(actionEventsBefore) {
		t.Fatalf("expected no new rollback action.created runtime events after rollback, before=%d after=%d", len(actionEventsBefore), len(actionEventsAfter))
	}
	sourceUpdatesAfter, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rollback source queue updates after: %v", err)
	}
	if len(sourceUpdatesAfter) != len(sourceUpdatesBefore) {
		t.Fatalf("expected no new rollback source queue updates after rollback, before=%d after=%d", len(sourceUpdatesBefore), len(sourceUpdatesAfter))
	}

	assertWorkspaceTaskClaimStatusNil(t, ctx, store, workspaceID, taskID)
}

func TestNodeClaimRejectsBlockedNodeAndUnblocksDependentsOnCompletion(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-node-claims"
		taskID      = "task-node-claims"
		agentID     = "agent-node-claims"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Node Claims",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Node Claims Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: "node-1", Type: "generic"},
			{NodeID: "node-2", Type: "generic", DependsOn: []string{"node-1"}},
		},
	})
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

	err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      "node-2",
		AgentID:     agentID,
	})
	if !errors.Is(err, sqlite.ErrNodeNotPending) {
		t.Fatalf("expected ErrNodeNotPending for blocked node, got %v", err)
	}

	if err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      "node-1",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim node-1: %v", err)
	}
	if err := store.CompleteNodeClaim(ctx, sqlite.NodeCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		NodeID:      "node-1",
		AgentID:     agentID,
		Summary:     "done",
	}); err != nil {
		t.Fatalf("complete node-1 claim: %v", err)
	}

	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if len(status.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(status.Nodes))
	}

	nodeStatuses := map[string]string{}
	for _, node := range status.Nodes {
		nodeStatuses[node.NodeID] = node.Status
	}
	if nodeStatuses["node-1"] != model.NodeStatusResolved {
		t.Fatalf("expected node-1 resolved, got %s", nodeStatuses["node-1"])
	}
	if nodeStatuses["node-2"] != model.NodeStatusPending {
		t.Fatalf("expected node-2 pending after dependency resolution, got %s", nodeStatuses["node-2"])
	}
}

func assertWorkspaceTaskClaimStatus(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, want string) {
	t.Helper()
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID != taskID {
			continue
		}
		if task.ClaimStatus == nil {
			t.Fatalf("task %s claim status is nil, want %s", taskID, want)
		}
		if *task.ClaimStatus != want {
			t.Fatalf("task %s claim status = %s, want %s", taskID, *task.ClaimStatus, want)
		}
		return
	}
	t.Fatalf("task %s not found in workspace %s", taskID, workspaceID)
}

func assertWorkspaceTaskClaimStatusNil(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) {
	t.Helper()
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID != taskID {
			continue
		}
		if task.ClaimStatus != nil {
			t.Fatalf("task %s claim status = %s, want nil", taskID, *task.ClaimStatus)
		}
		return
	}
	t.Fatalf("task %s not found in workspace %s", taskID, workspaceID)
}
