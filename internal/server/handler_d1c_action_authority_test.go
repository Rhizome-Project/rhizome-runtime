package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestActionCreateRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-create-missing-authority"
		taskID      = "task-d1c-action-create-missing-authority"
		agentID     = "agent-d1c-action-create-missing-authority"
	)

	seedD1CActionCreateFixture(t, ctx, store, workspaceID, taskID, agentID, false)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	blocking := true
	raw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Missing authority action create",
		Description: "should fail closed before action queue materializes",
		Blocking:    &blocking,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}

	result, rpcErr := h.actionCreate(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "action.create" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if actions, err := store.ListHumanActions(ctx, workspaceID, ""); err != nil {
		t.Fatalf("list human actions after authority reject: %v", err)
	} else if len(actions) != 0 {
		t.Fatalf("expected no human actions after authority reject, got %+v", actions)
	}
	if items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list operator queue after authority reject: %v", err)
	} else if len(items) != 0 {
		t.Fatalf("expected no operator queue rows after authority reject, got %+v", items)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	}); err != nil {
		t.Fatalf("list action.created runtime events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no action.created runtime events after authority reject, got %+v", events)
	}
}

func TestActionCreateRejectsStaleWorkspaceAuthorityForLinkedQueueWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-create-stale-authority"
		taskID      = "task-d1c-action-create-stale-authority"
		agentID     = "agent-d1c-action-create-stale-authority"
		queueKey    = "tension_rebase_followup:tens-d1c-action-create-stale-authority"
	)

	current := seedD1CActionCreateFixture(t, ctx, store, workspaceID, taskID, agentID, true)
	sourceQueue := createD1CServerRebaseFollowupQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "d1c-action-create-stale-authority")
	beforeQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload source queue before authority transfer: %v", err)
	}
	beforeSourceUpdates := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1202")

	raw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}

	result, rpcErr := h.actionCreate(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "action.create" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if actions, err := store.ListHumanActions(ctx, workspaceID, ""); err != nil {
		t.Fatalf("list human actions after stale authority reject: %v", err)
	} else if len(actions) != 0 {
		t.Fatalf("expected no human actions after stale authority reject, got %+v", actions)
	}
	afterQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload source queue after stale authority reject: %v", err)
	}
	if afterQueue.PayloadJSON != beforeQueue.PayloadJSON || afterQueue.UpdatedAt != beforeQueue.UpdatedAt || afterQueue.AssignedTo != beforeQueue.AssignedTo {
		t.Fatalf("expected stale authority reject not to mutate source queue, before=%+v after=%+v", beforeQueue, afterQueue)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}); len(got) != len(beforeSourceUpdates) {
		t.Fatalf("expected no new source queue updates after stale authority reject, before=%v after=%v", beforeSourceUpdates, got)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	}); err != nil {
		t.Fatalf("list action.created runtime events after stale authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no action.created runtime events after stale authority reject, got %+v", events)
	}
}

func TestActionResolveRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-resolve-stale-authority"
		taskID      = "task-d1c-action-resolve-stale-authority"
		agentID     = "agent-d1c-action-resolve-stale-authority"
		queueKey    = "tension_rebase_followup:tens-d1c-action-resolve-stale-authority"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "d1c-action-resolve-stale-authority")
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload source queue before stale authority transfer: %v", err)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1404")

	raw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "should fail closed on stale authority",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}

	result, rpcErr := h.actionResolve(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "action.resolve" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("reload action after stale authority reject: %v", err)
	}
	if action.Status != humanActionStatusPending || action.ResolutionComment != "" || action.ResolvedAt != "" {
		t.Fatalf("expected stale authority reject not to resolve action, got %+v", action)
	}
	afterSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("reload source queue after stale authority reject: %v", err)
	}
	if afterSourceQueue.PayloadJSON != beforeSourceQueue.PayloadJSON || afterSourceQueue.UpdatedAt != beforeSourceQueue.UpdatedAt || afterSourceQueue.Status != beforeSourceQueue.Status {
		t.Fatalf("expected stale authority reject not to mutate source queue, before=%+v after=%+v", beforeSourceQueue, afterSourceQueue)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list action.resolved runtime events after stale authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no action.resolved runtime events after stale authority reject, got %+v", events)
	}
}

func seedD1CActionCreateFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID string, claimAuthority bool) sqlite.WorkspaceAuthorityRecord {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Action Create Authority Fixture",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	var current sqlite.WorkspaceAuthorityRecord
	if claimAuthority {
		current = claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	}
	for _, id := range []string{agentID, "reviewer-a"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     id,
			OwnerUserID: "tests",
			DisplayName: id,
		}); err != nil {
			t.Fatalf("register agent %s: %v", id, err)
		}
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
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
		t.Fatalf("attach task: %v", err)
	}
	return current
}

func createD1CServerRebaseFollowupQueueFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, queueKey, suffix string) sqlite.OperatorQueueRecord {
	t.Helper()

	payloadJSON, err := json.Marshal(model.RebaseFollowupPayload{
		CoalitionID:       "coal-" + suffix,
		ForkTensionID:     "tens-fork-" + suffix,
		RepairTensionID:   "tens-repair-" + suffix,
		NextAction:        model.RebaseNextActionAttempt,
		RebasePlanClass:   "trim_redundancy",
		ConflictSafeClass: "rebase_candidate",
		DecisionReason:    "admission_risk",
		RootCauseID:       "root-" + suffix,
		ProvenanceGroupID: "prov-" + suffix,
	})
	if err != nil {
		t.Fatalf("marshal rebase follow-up payload: %v", err)
	}
	record, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Seed D1C rebase queue",
		Details:           "Rebase authority fixture",
		PayloadJSON:       string(payloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-" + suffix,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}
	return record
}
