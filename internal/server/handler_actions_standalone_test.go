package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestStandaloneHumanActionResolveWithoutQueuePath(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-standalone-resolve"
		taskID      = "task-action-standalone-resolve"
		agentID     = "agent-action-standalone-resolve"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	actionID, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Standalone human action",
		Description: "Created without queue sidecar to exercise standalone graph blockers.",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("create standalone human action: %v", err)
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID); err == nil {
		t.Fatalf("expected standalone action queue alias to be absent")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected missing queue alias error, got %v", err)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Demo blocker cleared cleanly.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal standalone actionResolve params: %v", err)
	}
	resolveAny, rpcErr := h.actionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("standalone actionResolve rpc error: %+v", rpcErr)
	}
	resolveResp, ok := resolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected standalone actionResolve response type %T", resolveAny)
	}
	if got, _ := resolveResp["status"].(string); got != humanActionStatusCompleted {
		t.Fatalf("standalone actionResolve response status = %q, want %q", got, humanActionStatusCompleted)
	}

	resolvedAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get resolved standalone action: %v", err)
	}
	if resolvedAction.Status != humanActionStatusCompleted {
		t.Fatalf("standalone action status = %q, want %q", resolvedAction.Status, humanActionStatusCompleted)
	}

	resolveLive := nextEventOfType(t, ch, "action.resolved")
	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertValidEventTimestamp(t, resolveLive.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, resolveLive, resolveRuntime, "action.resolved")
}
