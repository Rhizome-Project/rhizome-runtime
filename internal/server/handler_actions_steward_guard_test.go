package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestActionCreateRejectsGuardedRebaseFollowupWithoutActiveSteward(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-create-steward-guard"
		taskID      = "task-action-create-steward-guard"
		agentID     = "agent-action-create-steward-guard"
		repairID    = "tens-repair-action-create-steward-guard"
		coalitionID = "coal-action-create-steward-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	payloadJSON, err := json.Marshal(model.RebaseFollowupPayload{
		CoalitionID:          coalitionID,
		ForkTensionID:        "tens-fork-action-create-steward-guard",
		RepairTensionID:      repairID,
		StewardLeaseRequired: true,
		NextAction:           model.RebaseNextActionAttempt,
		RebasePlanClass:      "trim_redundancy",
		RebaseReason:         "lease_guard_required",
		ConflictSafeClass:    "rebase_candidate",
		TaskID:               taskID,
		TaskIDs:              []string{taskID},
	})
	if err != nil {
		t.Fatalf("marshal rebase payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          model.RebaseFollowupQueueKeyPrefix + repairID,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase follow-up",
		Details:           "Coalition ID: " + coalitionID + "\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(payloadJSON),
		AssignedTo:        "reviewer-a",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}

	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil {
		t.Fatal("expected actionCreate to reject guarded rebase follow-up without steward")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "active steward lease") {
		t.Fatalf("expected invalid params active steward lease guard, got %+v", rpcErr)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no persisted actions after steward guard rejection, got %+v", actions)
	}
}
