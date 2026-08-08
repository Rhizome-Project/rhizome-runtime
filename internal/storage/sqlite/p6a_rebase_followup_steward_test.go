package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCreateHumanActionWithQueueEffectsRejectsRebaseFollowupWithoutActiveSteward(t *testing.T) {
	t.Parallel()

	scenario := newP6ARebaseStewardScenario(t, "ws-p6a-no-steward", "task-p6a-no-steward", "agent-p6a-no-steward")
	queue, payload := scenario.createRebaseFollowupQueue(t, "guarded-no-steward", "coal-p6a-no-steward", "tens-repair-p6a-no-steward")

	created, err := scenario.store.CreateHumanActionWithQueueEffects(scenario.ctx, sqlite.HumanActionInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     scenario.agentID,
		Title:       "Need guarded rebase review",
	}, &queue, &payload)
	if err == nil {
		t.Fatalf("expected missing steward guard, got %+v", created)
	}
	if !errors.Is(err, sqlite.ErrRebaseFollowupStewardRequired) {
		t.Fatalf("expected ErrRebaseFollowupStewardRequired, got %v", err)
	}

	actions, err := scenario.store.ListHumanActions(scenario.ctx, scenario.workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no persisted actions after missing steward rejection, got %+v", actions)
	}
}

func TestCreateHumanActionWithQueueEffectsRejectsRebaseFollowupWhenDifferentStewardOwnsCluster(t *testing.T) {
	t.Parallel()

	scenario := newP6ARebaseStewardScenario(t, "ws-p6a-steward-mismatch", "task-p6a-steward-mismatch", "agent-p6a-steward-mismatch")
	if err := scenario.store.RegisterAgent(scenario.ctx, sqlite.AgentRegisterInput{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-p6a-other-steward",
		OwnerUserID: "developer",
		DisplayName: "Other Steward",
	}); err != nil {
		t.Fatalf("register steward peer: %v", err)
	}
	if _, err := scenario.store.ElectClusterSteward(scenario.ctx, sqlite.ElectStewardInput{
		ClusterID:   "task:" + scenario.workspaceID + "/" + scenario.taskID,
		EpochID:     "epoch-p6a-steward-mismatch",
		CandidateID: "agent-p6a-other-steward",
		TTLSeconds:  300,
	}); err != nil {
		t.Fatalf("elect steward: %v", err)
	}

	queue, payload := scenario.createRebaseFollowupQueue(t, "guarded-steward-mismatch", "coal-p6a-steward-mismatch", "tens-repair-p6a-steward-mismatch")
	created, err := scenario.store.CreateHumanActionWithQueueEffects(scenario.ctx, sqlite.HumanActionInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     scenario.agentID,
		Title:       "Need steward-owned rebase review",
	}, &queue, &payload)
	if err == nil {
		t.Fatalf("expected steward mismatch guard, got %+v", created)
	}
	if !errors.Is(err, sqlite.ErrRebaseFollowupStewardMismatch) {
		t.Fatalf("expected ErrRebaseFollowupStewardMismatch, got %v", err)
	}
}

func TestCreateHumanActionWithQueueEffectsRejectsRebaseFollowupWhenQueueAgentIDMissing(t *testing.T) {
	t.Parallel()

	scenario := newP6ARebaseStewardScenario(t, "ws-p6a-steward-missing-agent", "task-p6a-steward-missing-agent", "agent-p6a-steward-missing-agent")
	if _, err := scenario.store.ElectClusterSteward(scenario.ctx, sqlite.ElectStewardInput{
		ClusterID:   "task:" + scenario.workspaceID + "/" + scenario.taskID,
		EpochID:     "epoch-p6a-steward-missing-agent",
		CandidateID: scenario.agentID,
		TTLSeconds:  300,
	}); err != nil {
		t.Fatalf("elect steward: %v", err)
	}

	queue, payload := scenario.createRebaseFollowupQueue(t, "guarded-missing-agent", "coal-p6a-steward-missing-agent", "tens-repair-p6a-steward-missing-agent")
	if _, err := scenario.store.DB().ExecContext(scenario.ctx, `
		UPDATE operator_queue_items
		   SET agent_id = ''
		 WHERE workspace_id = ? AND queue_id = ?
	`, scenario.workspaceID, queue.QueueID); err != nil {
		t.Fatalf("blank queue agent_id: %v", err)
	}
	queue.AgentID = ""

	created, err := scenario.store.CreateHumanActionWithQueueEffects(scenario.ctx, sqlite.HumanActionInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     scenario.agentID,
		Title:       "Guarded rebase should fail without queue agent authority",
	}, &queue, &payload)
	if err == nil {
		t.Fatalf("expected missing queue agent guard, got %+v", created)
	}
	if !errors.Is(err, sqlite.ErrRebaseFollowupStewardRequired) {
		t.Fatalf("expected ErrRebaseFollowupStewardRequired, got %v", err)
	}
}

func TestCreateHumanActionWithQueueEffectsRejectsRebaseFollowupWhenPayloadTaskMismatchesQueueTask(t *testing.T) {
	t.Parallel()

	scenario := newP6ARebaseStewardScenario(t, "ws-p6a-task-mismatch", "task-p6a-task-primary", "agent-p6a-task-primary")
	const secondaryTaskID = "task-p6a-task-secondary"
	createAttachedTaskForP6AStewardScenario(t, scenario, secondaryTaskID)
	if _, err := scenario.store.ElectClusterSteward(scenario.ctx, sqlite.ElectStewardInput{
		ClusterID:   "task:" + scenario.workspaceID + "/" + secondaryTaskID,
		EpochID:     "epoch-p6a-task-secondary",
		CandidateID: scenario.agentID,
		TTLSeconds:  300,
	}); err != nil {
		t.Fatalf("elect secondary steward: %v", err)
	}

	queue, payload := scenario.createRebaseFollowupQueue(t, "guarded-task-mismatch", "coal-p6a-task-mismatch", "tens-repair-p6a-task-mismatch")
	payload.TaskID = secondaryTaskID
	payload.TaskIDs = []string{secondaryTaskID}
	payload.Normalize()
	if _, err := scenario.store.DB().ExecContext(scenario.ctx, `
		UPDATE operator_queue_items
		   SET payload_json = ?
		 WHERE workspace_id = ? AND queue_id = ?
	`, string(mustJSON(payload)), scenario.workspaceID, queue.QueueID); err != nil {
		t.Fatalf("tamper queue payload task binding: %v", err)
	}

	created, err := scenario.store.CreateHumanActionWithQueueEffects(scenario.ctx, sqlite.HumanActionInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     scenario.agentID,
		Title:       "Guarded rebase should reject payload task mismatch",
	}, &queue, &payload)
	if err == nil {
		t.Fatalf("expected task-mismatch steward guard, got %+v", created)
	}
	if !errors.Is(err, sqlite.ErrRebaseFollowupStewardRequired) {
		t.Fatalf("expected ErrRebaseFollowupStewardRequired from canonical queue task binding, got %v", err)
	}
}

func TestCreateHumanActionWithQueueEffectsAllowsRebaseFollowupWithMatchingActiveSteward(t *testing.T) {
	t.Parallel()

	scenario := newP6ARebaseStewardScenario(t, "ws-p6a-steward-ok", "task-p6a-steward-ok", "agent-p6a-steward-ok")
	if _, err := scenario.store.ElectClusterSteward(scenario.ctx, sqlite.ElectStewardInput{
		ClusterID:   "task:" + scenario.workspaceID + "/" + scenario.taskID,
		EpochID:     "epoch-p6a-steward-ok",
		CandidateID: scenario.agentID,
		TTLSeconds:  300,
	}); err != nil {
		t.Fatalf("elect steward: %v", err)
	}

	queue, payload := scenario.createRebaseFollowupQueue(t, "guarded-steward-ok", "coal-p6a-steward-ok", "tens-repair-p6a-steward-ok")
	created, err := scenario.store.CreateHumanActionWithQueueEffects(scenario.ctx, sqlite.HumanActionInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     scenario.agentID,
		Title:       "Need bounded overlap rebase review",
	}, &queue, &payload)
	if err != nil {
		t.Fatalf("CreateHumanActionWithQueueEffects: %v", err)
	}
	if strings.TrimSpace(created.Action.ActionID) == "" {
		t.Fatalf("expected persisted action, got %+v", created)
	}

	updatedQueue, err := scenario.store.GetOperatorQueueItem(scenario.ctx, scenario.workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated queue: %v", err)
	}
	var updatedPayload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(updatedQueue.PayloadJSON), &updatedPayload); err != nil {
		t.Fatalf("decode updated queue payload: %v", err)
	}
	updatedPayload.Normalize()
	if updatedPayload.ActionID != created.Action.ActionID {
		t.Fatalf("updated queue action_id = %q, want %q", updatedPayload.ActionID, created.Action.ActionID)
	}
}

type p6aRebaseStewardScenario struct {
	store       *sqlite.Store
	ctx         context.Context
	workspaceID string
	taskID      string
	agentID     string
}

func newP6ARebaseStewardScenario(t *testing.T, workspaceID, taskID, agentID string) p6aRebaseStewardScenario {
	t.Helper()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P6A Rebase Steward Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "P6A Steward Agent",
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

	return p6aRebaseStewardScenario{
		store:       store,
		ctx:         ctx,
		workspaceID: workspaceID,
		taskID:      taskID,
		agentID:     agentID,
	}
}

func (s p6aRebaseStewardScenario) createRebaseFollowupQueue(t *testing.T, queueSuffix, coalitionID, repairTensionID string) (sqlite.OperatorQueueRecord, model.RebaseFollowupPayload) {
	t.Helper()

	payload := model.RebaseFollowupPayload{
		CoalitionID:          coalitionID,
		ForkTensionID:        "tens-fork-" + queueSuffix,
		RepairTensionID:      repairTensionID,
		StewardLeaseRequired: true,
		NextAction:           model.RebaseNextActionAttempt,
		RebasePlanClass:      "trim_redundancy",
		RebaseReason:         "lease_guard_required",
		ConflictSafeClass:    "rebase_candidate",
		TaskID:               s.taskID,
		TaskIDs:              []string{s.taskID},
	}
	payload.Normalize()

	record, _, err := s.store.UpsertOperatorQueueItemWithEvent(s.ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       s.workspaceID,
		QueueKey:          model.RebaseFollowupQueueKeyPrefix + queueSuffix,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Guarded rebase follow-up",
		Details:           "Coalition ID: " + coalitionID + "\nRepair tension: " + repairTensionID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(mustJSON(payload)),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairTensionID,
		TaskID:            s.taskID,
		AgentID:           s.agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}
	return record, payload
}

func createAttachedTaskForP6AStewardScenario(t *testing.T, scenario p6aRebaseStewardScenario, taskID string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate secondary graph: %v", err)
	}
	if err := scenario.store.CreateTaskWithGraph(scenario.ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create secondary task: %v", err)
	}
	if err := scenario.store.AttachTaskToWorkspace(scenario.ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach secondary task: %v", err)
	}
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
