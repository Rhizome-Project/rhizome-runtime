package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCreateHumanActionRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-create-missing-authority"
		taskID      = "task-d1c-action-create-missing-authority"
		agentID     = "agent-d1c-action-create-missing-authority"
	)

	seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	_, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Missing authority human action",
		Description: "should fail closed before action write",
		Blocking:    true,
	})
	if err == nil {
		t.Fatal("expected missing authority reject on CreateHumanAction")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	assertHumanActionCount(t, ctx, store, workspaceID, 0)
	assertTaskClaimBlockerCount(t, ctx, store, workspaceID, taskID, 0)
	assertTaskClaimStatus(t, ctx, store, workspaceID, taskID, string(model.TaskClaimStatusClaimed))
	assertHumanActionRuntimeEventCount(t, ctx, store, workspaceID, 0)
}

func TestCreateHumanActionWithQueueEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-queue-stale-authority"
		taskID      = "task-d1c-action-queue-stale-authority"
		agentID     = "agent-d1c-action-queue-stale-authority"
		queueKey    = "tension_rebase_followup:tens-d1c-action-queue-stale-authority"
	)

	current := seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)
	queueRecord, payload := createD1CRebaseFollowupQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "d1c-action-queue-stale-authority")
	beforeActionCreated := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	})
	beforeSourceUpdates := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queueRecord.QueueID,
		Limit:       20,
	})
	beforeQueue := mustGetOperatorQueueItem(t, ctx, store, workspaceID, queueRecord.QueueID)
	transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1101")

	_, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Stale authority linked action",
		Description: "should fail closed before queue/runtime writes",
		Blocking:    true,
	}, &queueRecord, &payload)
	if err == nil {
		t.Fatal("expected stale authority reject on CreateHumanActionWithQueueEffects")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	assertHumanActionCount(t, ctx, store, workspaceID, 0)
	assertTaskClaimBlockerCount(t, ctx, store, workspaceID, taskID, 0)
	assertTaskClaimStatus(t, ctx, store, workspaceID, taskID, string(model.TaskClaimStatusClaimed))
	if items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list queue items after stale authority reject: %v", err)
	} else if len(items) != 1 || items[0].QueueID != queueRecord.QueueID {
		t.Fatalf("expected only seeded source queue after stale authority reject, got %+v", items)
	}
	afterQueue := mustGetOperatorQueueItem(t, ctx, store, workspaceID, queueRecord.QueueID)
	if afterQueue.PayloadJSON != beforeQueue.PayloadJSON || afterQueue.UpdatedAt != beforeQueue.UpdatedAt || afterQueue.AssignedTo != beforeQueue.AssignedTo {
		t.Fatalf("expected stale authority reject not to mutate source queue, before=%+v after=%+v", beforeQueue, afterQueue)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	}); got != beforeActionCreated {
		t.Fatalf("expected no new action.created rows after stale authority reject, before=%d after=%d", beforeActionCreated, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queueRecord.QueueID,
		Limit:       20,
	}); got != beforeSourceUpdates {
		t.Fatalf("expected no new source queue updates after stale authority reject, before=%d after=%d", beforeSourceUpdates, got)
	}
}

func TestCreateHumanActionWithRollbackFailureQueueEffectsRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-rollback-missing-authority"
		taskID      = "task-d1c-action-rollback-missing-authority"
		agentID     = "agent-d1c-action-rollback-missing-authority"
		queueKey    = "tension_rebase_followup:tens-d1c-action-rollback-missing-authority"
	)

	seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)
	queueRecord, payload := createD1CRollbackFailureQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "d1c-action-rollback-missing-authority")
	beforeActionCreated := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	})
	beforeSourceUpdates := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queueRecord.QueueID,
		Limit:       20,
	})
	beforeQueue := mustGetOperatorQueueItem(t, ctx, store, workspaceID, queueRecord.QueueID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	_, err := store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Missing authority rollback-followup action",
		Description: "should fail closed before rollback-followup writes",
		Blocking:    true,
	}, &queueRecord, &payload)
	if err == nil {
		t.Fatal("expected missing authority reject on CreateHumanActionWithRollbackFailureQueueEffects")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	assertHumanActionCount(t, ctx, store, workspaceID, 0)
	assertTaskClaimBlockerCount(t, ctx, store, workspaceID, taskID, 0)
	assertTaskClaimStatus(t, ctx, store, workspaceID, taskID, string(model.TaskClaimStatusClaimed))
	if items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
		t.Fatalf("list queue items after missing rollback authority reject: %v", err)
	} else if len(items) != 1 || items[0].QueueID != queueRecord.QueueID {
		t.Fatalf("expected only seeded rollback source queue after authority reject, got %+v", items)
	}
	afterQueue := mustGetOperatorQueueItem(t, ctx, store, workspaceID, queueRecord.QueueID)
	if afterQueue.PayloadJSON != beforeQueue.PayloadJSON || afterQueue.UpdatedAt != beforeQueue.UpdatedAt || afterQueue.AssignedTo != beforeQueue.AssignedTo {
		t.Fatalf("expected missing authority reject not to mutate rollback source queue, before=%+v after=%+v", beforeQueue, afterQueue)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	}); got != beforeActionCreated {
		t.Fatalf("expected no new action.created rows after missing authority reject, before=%d after=%d", beforeActionCreated, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queueRecord.QueueID,
		Limit:       20,
	}); got != beforeSourceUpdates {
		t.Fatalf("expected no new rollback source queue updates after authority reject, before=%d after=%d", beforeSourceUpdates, got)
	}
}

func TestResolveHumanActionRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-resolve-missing-authority"
		taskID      = "task-d1c-action-resolve-missing-authority"
		agentID     = "agent-d1c-action-resolve-missing-authority"
	)

	seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)
	actionID, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Missing authority action resolve",
		Description: "should fail closed before action resolve writes",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("seed blocking action: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	currentAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action(before resolve): %v", err)
	}
	err = store.ResolveHumanAction(ctx, actionID, "COMPLETED", "done", "reviewer-a", currentAction)
	if err == nil {
		t.Fatal("expected missing authority reject on ResolveHumanAction")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject code, got %+v", reject)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("reload action after authority reject: %v", err)
	}
	if action.Status != model.ActionStatusPending || action.ResolutionComment != "" || action.ResolvedAt != "" {
		t.Fatalf("expected missing authority reject not to resolve action, got %+v", action)
	}
	assertTaskClaimBlockerCount(t, ctx, store, workspaceID, taskID, 1)
	assertTaskClaimStatus(t, ctx, store, workspaceID, taskID, string(model.TaskClaimStatusBlocked))
}

func TestResolveHumanActionWithQueueEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-action-resolve-queue-stale-authority"
		taskID      = "task-d1c-action-resolve-queue-stale-authority"
		agentID     = "agent-d1c-action-resolve-queue-stale-authority"
		queueKey    = "tension_rebase_followup:tens-d1c-action-resolve-queue-stale-authority"
	)

	current := seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue, payload := createD1CRebaseFollowupQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "d1c-action-resolve-queue-stale-authority")
	created, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Stale authority action resolve",
		Description: "should fail closed before resolve queue/runtime writes",
		Blocking:    true,
	}, &sourceQueue, &payload)
	if err != nil {
		t.Fatalf("seed linked action: %v", err)
	}
	if created.ActionQueue == nil {
		t.Fatal("expected seeded action queue event")
	}
	if created.LinkedSourceQueue == nil {
		t.Fatal("expected seeded linked source queue event")
	}
	actionQueueBefore := created.ActionQueue.Record
	sourceQueueBefore := created.LinkedSourceQueue.Record
	beforeActionResolved := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    created.Action.ActionID,
		Limit:       20,
	})
	beforeActionQueueResolved := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	})
	beforeSourceQueueResolved := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	})
	transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1303")

	result, err := store.ResolveHumanActionWithQueueEffects(ctx, created.Action.ActionID, "COMPLETED", "done", "reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueueBefore.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "done",
			Summary:                 actionQueueBefore.Summary,
			Details:                 actionQueueBefore.Details,
			PayloadJSON:             actionQueueBefore.PayloadJSON,
			RequireCurrentUpdatedAt: actionQueueBefore.UpdatedAt,
		},
		[]sqlite.OperatorQueueResolveInput{{
			WorkspaceID:             workspaceID,
			QueueID:                 sourceQueueBefore.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "done",
			Summary:                 sourceQueueBefore.Summary,
			Details:                 sourceQueueBefore.Details,
			PayloadJSON:             sourceQueueBefore.PayloadJSON,
			RequireCurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		}},
		nil,
		&sqlite.RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "action.resolved",
			EntityType:  "human_action",
			EntityID:    created.Action.ActionID,
			ActorType:   "operator",
			ActorID:     "reviewer-a",
			AgentID:     agentID,
			TaskID:      taskID,
			PayloadJSON: `{"resolution":"COMPLETED"}`,
		},
		created.Action,
	)
	if err == nil {
		t.Fatalf("expected stale authority reject on ResolveHumanActionWithQueueEffects, got %+v", result)
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject code, got %+v", reject)
	}

	action, err := store.GetHumanAction(ctx, created.Action.ActionID)
	if err != nil {
		t.Fatalf("reload action after stale authority reject: %v", err)
	}
	if action.Status != model.ActionStatusPending || action.ResolutionComment != "" || action.ResolvedAt != "" {
		t.Fatalf("expected stale authority reject not to resolve action, got %+v", action)
	}
	assertTaskClaimBlockerCount(t, ctx, store, workspaceID, taskID, 1)
	assertTaskClaimStatus(t, ctx, store, workspaceID, taskID, string(model.TaskClaimStatusBlocked))
	actionQueueAfter := mustGetOperatorQueueItem(t, ctx, store, workspaceID, actionQueueBefore.QueueID)
	if actionQueueAfter.Status != actionQueueBefore.Status || actionQueueAfter.Resolution != actionQueueBefore.Resolution || actionQueueAfter.UpdatedAt != actionQueueBefore.UpdatedAt {
		t.Fatalf("expected stale authority reject not to mutate action queue, before=%+v after=%+v", actionQueueBefore, actionQueueAfter)
	}
	sourceQueueAfter := mustGetOperatorQueueItem(t, ctx, store, workspaceID, sourceQueueBefore.QueueID)
	if sourceQueueAfter.Status != sourceQueueBefore.Status || sourceQueueAfter.Resolution != sourceQueueBefore.Resolution || sourceQueueAfter.PayloadJSON != sourceQueueBefore.PayloadJSON || sourceQueueAfter.UpdatedAt != sourceQueueBefore.UpdatedAt {
		t.Fatalf("expected stale authority reject not to mutate linked source queue, before=%+v after=%+v", sourceQueueBefore, sourceQueueAfter)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    created.Action.ActionID,
		Limit:       20,
	}); got != beforeActionResolved {
		t.Fatalf("expected no new action.resolved rows after stale authority reject, before=%d after=%d", beforeActionResolved, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}); got != beforeActionQueueResolved {
		t.Fatalf("expected no new action queue resolved rows after stale authority reject, before=%d after=%d", beforeActionQueueResolved, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}); got != beforeSourceQueueResolved {
		t.Fatalf("expected no new source queue resolved rows after stale authority reject, before=%d after=%d", beforeSourceQueueResolved, got)
	}
}

func seedD1CActionAuthorityFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID string) sqlite.WorkspaceAuthorityRecord {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Action Authority Fixture",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
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
		Title:       taskID,
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "tests",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed D1C action authority fixture",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	return current
}

func createD1CRebaseFollowupQueueFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, queueKey, suffix string) (sqlite.OperatorQueueRecord, model.RebaseFollowupPayload) {
	t.Helper()

	payload := model.RebaseFollowupPayload{
		CoalitionID:       "coal-" + suffix,
		ForkTensionID:     "tens-fork-" + suffix,
		RepairTensionID:   "tens-repair-" + suffix,
		NextAction:        model.RebaseNextActionAttempt,
		RebasePlanClass:   "trim_redundancy",
		ConflictSafeClass: "rebase_candidate",
		DecisionReason:    "admission_risk",
		RootCauseID:       "root-" + suffix,
		ProvenanceGroupID: "prov-" + suffix,
	}
	payloadJSON, err := json.Marshal(payload)
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
	return record, payload
}

func createD1CRollbackFailureQueueFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, queueKey, suffix string) (sqlite.OperatorQueueRecord, model.RebaseRollbackFailurePayload) {
	t.Helper()

	payload := model.RebaseRollbackFailurePayload{
		Kind:            model.RebaseRollbackFailureKind,
		FailureScope:    "run_verify",
		FailureTrigger:  "verifier_late_fail_run",
		FailureMessage:  "Rollback path needs operator recovery.",
		TaskID:          taskID,
		AgentID:         agentID,
		RepairTensionID: "tens-repair-" + suffix,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal rollback failure payload: %v", err)
	}
	record, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Seed D1C rollback failure queue",
		Details:           "Rollback authority fixture",
		PayloadJSON:       string(payloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-failure-" + suffix,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rollback failure queue: %v", err)
	}
	return record, payload
}

func transferExternalWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, current sqlite.WorkspaceAuthorityRecord, peerNodeID string) {
	t.Helper()

	referenceAt := time.Now().UTC().Round(0)
	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head before transfer: %v", err)
	}
	commitWatermark := current.CommitWatermark + 1
	if journalHead > commitWatermark {
		commitWatermark = journalHead
	}
	appliedWatermark := current.AppliedWatermark + 1
	if appliedWatermark > commitWatermark {
		appliedWatermark = commitWatermark
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        "workspace",
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    "system",
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}

func assertHumanActionCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()
	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM human_actions WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count human actions: %v", err)
	}
	if got != want {
		t.Fatalf("human action count = %d, want %d", got, want)
	}
}

func assertTaskClaimBlockerCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string, want int) {
	t.Helper()
	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM task_claim_blockers WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&got); err != nil {
		t.Fatalf("count task claim blockers: %v", err)
	}
	if got != want {
		t.Fatalf("task claim blocker count = %d, want %d", got, want)
	}
}

func assertTaskClaimStatus(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, want string) {
	t.Helper()
	var got string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&got); err != nil {
		t.Fatalf("query task claim status: %v", err)
	}
	if got != want {
		t.Fatalf("task claim status = %q, want %q", got, want)
	}
}

func assertHumanActionRuntimeEventCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()
	got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	})
	if got != want {
		t.Fatalf("action.created runtime event count = %d, want %d", got, want)
	}
}

func countRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, filter sqlite.RuntimeEventFilter) int {
	t.Helper()
	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	return len(events)
}

func mustGetOperatorQueueItem(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueID string) sqlite.OperatorQueueRecord {
	t.Helper()
	record, err := store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
	if err != nil {
		t.Fatalf("get operator queue item %s: %v", queueID, err)
	}
	return record
}
