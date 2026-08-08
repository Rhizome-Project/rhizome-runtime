package sqlite_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRollbackFailureCreateContextsFailClosedWhenCurrentCarrierProofIsLost(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		run  func(t *testing.T, ctx context.Context, store *sqlite.Store)
	}{
		{
			name: "source-queue proof loss",
			run:  runRollbackFailureCreateSourceContextProofLoss,
		},
		{
			name: "action context proof loss",
			run:  runRollbackFailureCreateActionContextProofLoss,
		},
		{
			name: "repair context proof loss",
			run:  runRollbackFailureCreateRepairContextProofLoss,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()

			tc.run(t, ctx, store)
		})
	}
}

func runRollbackFailureCreateSourceContextProofLoss(t *testing.T, ctx context.Context, store *sqlite.Store) {
	const (
		workspaceID = "ws-rollback-create-source-proof-loss"
		queueKey    = "tension_rebase_followup:source-proof-loss"
		sourceID    = "evt-rollback-source-proof-loss"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Rollback Failure Create Source Context Regression",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	sourceQueue, _ := seedOpenLinkedRebaseFollowupQueue(t, ctx, store, workspaceID, queueKey, "", "", "", sourceID)
	breakQueueCarrierProof(t, ctx, store, workspaceID, sourceQueue.QueueID, "generic:source-proof-loss")

	_, _, err := store.UpsertRollbackFailureQueueItemWithCurrentCreateLineage(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "rebase_rollback_failure:source-proof-loss",
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Source carrier proof loss",
		Details:           "The store must not fall back to a generic create path once the current source queue can no longer be proved.",
		PayloadJSON:       rollbackFailurePayloadJSON("source-proof-loss", "", ""),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-create-source-proof-loss",
		KeepSessionActive: true,
	}, sourceQueue.QueueID, sourceQueue.QueueKey)
	if err == nil {
		t.Fatal("expected rollback-failure source-context create to fail when current source proof is lost")
	}
	if !strings.Contains(err.Error(), "current linked source queue") {
		t.Fatalf("expected source-context proof-loss error, got %v", err)
	}
	assertRollbackFailureQueueMissing(t, ctx, store, workspaceID, "rebase_rollback_failure:source-proof-loss")
}

func runRollbackFailureCreateActionContextProofLoss(t *testing.T, ctx context.Context, store *sqlite.Store) {
	const (
		workspaceID = "ws-rollback-create-action-proof-loss"
		taskID      = "task-rollback-create-action-proof-loss"
		agentID     = "agent-rollback-create-action-proof-loss"
		queueKey    = "tension_rebase_followup:action-proof-loss"
		repairID    = "repair-action-proof-loss"
	)

	seedRollbackFailureWorkspaceTaskAndAgent(t, ctx, store, workspaceID, taskID, agentID)

	sourceQueue, sourcePayload := seedOpenLinkedRebaseFollowupQueue(t, ctx, store, workspaceID, queueKey, taskID, agentID, repairID, "evt-rollback-action-proof-loss")
	actionResult, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Need rollback action proof",
		Blocking:    true,
	}, &sourceQueue, &sourcePayload)
	if err != nil {
		t.Fatalf("create linked human action: %v", err)
	}

	breakQueueCarrierProof(t, ctx, store, workspaceID, sourceQueue.QueueID, "generic:action-proof-loss")

	_, _, err = store.UpsertRollbackFailureQueueItemWithCurrentLinkedActionCreateContext(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "rebase_rollback_failure:action-proof-loss",
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Action carrier proof loss",
		Details:           "The store must not fall back to a generic create path once the action-linked current source queue is no longer current.",
		PayloadJSON:       rollbackFailurePayloadJSON("action-proof-loss", taskID, agentID),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-create-action-proof-loss",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	}, actionResult.Action.ActionID)
	if err == nil {
		t.Fatal("expected rollback-failure action-context create to fail when current source proof is lost")
	}
	if !strings.Contains(err.Error(), "current linked source queue for action context") {
		t.Fatalf("expected action-context proof-loss error, got %v", err)
	}
	assertRollbackFailureQueueMissing(t, ctx, store, workspaceID, "rebase_rollback_failure:action-proof-loss")
}

func runRollbackFailureCreateRepairContextProofLoss(t *testing.T, ctx context.Context, store *sqlite.Store) {
	const (
		workspaceID = "ws-rollback-create-repair-proof-loss"
		taskID      = "task-rollback-create-repair-proof-loss"
		agentID     = "agent-rollback-create-repair-proof-loss"
		queueKey    = "tension_rebase_followup:repair-proof-loss"
		repairID    = "repair-proof-loss"
	)

	seedRollbackFailureWorkspaceTaskAndAgent(t, ctx, store, workspaceID, taskID, agentID)

	sourceQueue, sourcePayload := seedOpenLinkedRebaseFollowupQueue(t, ctx, store, workspaceID, queueKey, taskID, agentID, repairID, "evt-rollback-repair-proof-loss")
	actionResult, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Need rollback repair proof",
		Blocking:    true,
	}, &sourceQueue, &sourcePayload)
	if err != nil {
		t.Fatalf("create linked repair human action: %v", err)
	}

	currentAction, err := store.GetHumanAction(ctx, actionResult.Action.ActionID)
	if err != nil {
		t.Fatalf("get linked human action before resolution: %v", err)
	}
	if err := store.ResolveHumanAction(ctx, actionResult.Action.ActionID, "COMPLETED", "repair carrier is no longer current", "developer", currentAction); err != nil {
		t.Fatalf("resolve linked human action to break repair proof: %v", err)
	}

	_, _, err = store.UpsertRollbackFailureQueueItemWithCurrentLinkedRepairCreateContext(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "rebase_rollback_failure:repair-proof-loss",
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Repair carrier proof loss",
		Details:           "The store must not fall back to a generic create path once the repair-linked carrier is no longer pending.",
		PayloadJSON:       rollbackFailurePayloadJSON("repair-proof-loss", taskID, agentID),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-create-repair-proof-loss",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	}, repairID)
	if err == nil {
		t.Fatal("expected rollback-failure repair-context create to fail when current repair carrier is no longer pending")
	}
	if !strings.Contains(err.Error(), "current pending linked rebase follow-up carrier for repair tension context") {
		t.Fatalf("expected repair-context proof-loss error, got %v", err)
	}
	assertRollbackFailureQueueMissing(t, ctx, store, workspaceID, "rebase_rollback_failure:repair-proof-loss")
}

func seedRollbackFailureWorkspaceTaskAndAgent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Rollback Failure Create Context Regression",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Rollback Failure Create Context Regression Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}},
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
}

func seedOpenLinkedRebaseFollowupQueue(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueKey, taskID, agentID, repairTensionID, sourceID string) (sqlite.OperatorQueueRecord, model.RebaseFollowupPayload) {
	t.Helper()

	payload := model.RebaseFollowupPayload{
		RepairTensionID:     repairTensionID,
		TaskID:              taskID,
		NextAction:          model.RebaseNextActionAttempt,
		RebaseWorkflowState: model.RebaseWorkflowStateInProgress,
		RebaseWorkflowStep:  model.RebaseWorkflowStepOperatorClaimed,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal linked follow-up payload: %v", err)
	}
	record, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Rollback failure create context regression seed",
		Details:           "Pending linked follow-up carrier",
		PayloadJSON:       string(payloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          sourceID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create linked follow-up queue: %v", err)
	}
	return record, payload
}

func breakQueueCarrierProof(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueID, replacementKey string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE operator_queue_items
   SET queue_key = ?, payload_json = ?, updated_at = ?, revision = revision + 1
 WHERE workspace_id = ? AND queue_id = ?`,
		replacementKey,
		`{"kind":"generic"}`,
		now,
		workspaceID,
		queueID,
	); err != nil {
		t.Fatalf("break queue carrier proof: %v", err)
	}
}

func rollbackFailurePayloadJSON(suffix, taskID, agentID string) string {
	payload := model.RebaseRollbackFailurePayload{
		Kind:           model.RebaseRollbackFailureKind,
		FailureScope:   "run_verify",
		FailureTrigger: "verifier_late_fail_run",
		FailureMessage: "rollback failure proof-loss regression",
		TaskID:         taskID,
		AgentID:        agentID,
		RepairTensionID: func() string {
			if suffix == "" {
				return ""
			}
			return "repair-" + suffix
		}(),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("marshal rollback-failure payload for %s: %v", suffix, err))
	}
	return string(raw)
}

func assertRollbackFailureQueueMissing(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueKey string) {
	t.Helper()

	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected no rollback-failure queue to be persisted for %s", queueKey)
	}
}
