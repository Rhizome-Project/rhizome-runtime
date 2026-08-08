package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestHumanActionLifecycleMirrorsPersistedRuntimeEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-runtime"
		taskID      = "task-action-runtime"
		agentID     = "agent-action-runtime"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Approve deploy gate",
		Description: "Blocking approval before rollout.",
		Blocking:    boolPtr(true),
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	if got, _ := createResp["status"].(string); got != humanActionStatusPending {
		t.Fatalf("actionCreate response status = %q, want %q", got, humanActionStatusPending)
	}
	queueID := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID).QueueID

	createEvents := nextActionEventsOfTypes(t, ch, "action.created", "workspace.ops.updated")
	createLive := createEvents["action.created"]
	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertValidEventTimestamp(t, createLive.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, createLive, createRuntime, "action.created")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, createLive.PayloadJSON), createRuntime.PayloadJSON)
	assertHumanActionRuntimePromptContext(t, createRuntime, "action.create", workspaceID, "system", "server_rpc")
	queueCreateLive := createEvents["workspace.ops.updated"]
	queueCreateRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       1,
	})
	assertValidEventTimestamp(t, queueCreateLive.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, queueCreateLive, queueCreateRuntime, "workspace.ops.updated")

	chatRaw, err := json.Marshal(actionChatSendParams{
		ActionID: actionID,
		FromID:   "reviewer-a",
		Content:  "Please attach the latest rollout evidence.",
	})
	if err != nil {
		t.Fatalf("marshal actionChatSend params: %v", err)
	}
	chatAny, rpcErr := h.actionChatSend(ctx, chatRaw)
	if rpcErr != nil {
		t.Fatalf("actionChatSend rpc error: %+v", rpcErr)
	}
	chatResp, ok := chatAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionChatSend response type %T", chatAny)
	}
	messageID, ok := chatResp["message_id"].(string)
	if !ok || messageID == "" {
		t.Fatalf("unexpected actionChatSend response %+v", chatResp)
	}

	chatEvents := nextActionEventsOfTypes(t, ch, "action.chat", "agent.message")
	chatLive := chatEvents["action.chat"]
	chatRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.chat",
		EntityType:  "action_message",
		EntityID:    messageID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertValidEventTimestamp(t, chatLive.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, chatLive, chatRuntime, "action.chat")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, chatLive.PayloadJSON), chatRuntime.PayloadJSON)
	assertHumanActionRuntimePromptContext(t, chatRuntime, "action.chat.send", workspaceID, "system", "server_rpc")
	assertServerRuntimeEventAuthorityMetadata(t, chatRuntime, authority)
	chatMessageLive := chatEvents["agent.message"]
	chatMessageRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       1,
	})
	assertValidEventTimestamp(t, chatMessageLive.Timestamp)
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, chatMessageLive, chatMessageRuntime, "agent.message", agentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, chatMessageLive.PayloadJSON), chatMessageRuntime.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, chatMessageLive, "agent_message.sent")
	assertServerRuntimeEventAuthorityMetadata(t, chatMessageRuntime, authority)

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Evidence reviewed and approved.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	resolveAny, rpcErr := h.actionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	resolveResp, ok := resolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionResolve response type %T", resolveAny)
	}
	if got, _ := resolveResp["status"].(string); got != humanActionStatusCompleted {
		t.Fatalf("actionResolve response status = %q, want %q", got, humanActionStatusCompleted)
	}

	resolveEvents := nextActionEventsOfTypes(t, ch, "action.resolved", "workspace.ops.resolved", "agent.message")
	resolveLive := resolveEvents["action.resolved"]
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
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, resolveLive.PayloadJSON), resolveRuntime.PayloadJSON)
	assertHumanActionRuntimePromptContext(t, resolveRuntime, "action.resolve", workspaceID, "system", "server_rpc")
	resolveQueueLive := resolveEvents["workspace.ops.resolved"]
	resolveQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       1,
	})
	assertValidEventTimestamp(t, resolveQueueLive.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, resolveQueueLive, resolveQueueRuntime, "workspace.ops.resolved")
	resolveMessageLive := resolveEvents["agent.message"]
	resolveMessageRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       1,
	})
	assertValidEventTimestamp(t, resolveMessageLive.Timestamp)
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, resolveMessageLive, resolveMessageRuntime, "agent.message", agentID)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, resolveMessageLive.PayloadJSON), resolveMessageRuntime.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, resolveMessageLive, "agent_message.sent")
	assertServerRuntimeEventAuthorityMetadata(t, resolveMessageRuntime, authority)
}

func assertHumanActionRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantPrincipalType, wantPrincipalID string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected human action prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	assertHumanActionPromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertHumanActionPromptContextField(t, envelope, "context_kind", "authority_bearing_action_write")
	assertHumanActionPromptContextField(t, envelope, "surface", wantSurface)
	assertHumanActionPromptContextField(t, envelope, "origin", "server_rpc")
	assertHumanActionPromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertHumanActionPromptContextField(t, envelope, "principal_type", wantPrincipalType)
	assertHumanActionPromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertHumanActionPromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertHumanActionPromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertHumanActionPromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertHumanActionPromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertHumanActionPromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()
	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("human action prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("human action prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}

func TestActionResolveAllowsStandaloneHumanActionWithoutQueue(t *testing.T) {
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

func TestHumanActionQueueSyncRejectsImplicitReopenAfterResolution(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-queue-repeat"
		taskID      = "task-action-queue-repeat"
		agentID     = "agent-action-queue-repeat"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	actionID, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Repeat queue sync",
		Description: "Exercise fallback queue alias publish for repeated helper sync.",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("create human action: %v", err)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get human action: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	firstQueueRecord, firstQueueEvent, err := h.syncHumanActionOperatorQueue(ctx, action)
	if err != nil {
		t.Fatalf("first syncHumanActionOperatorQueue: %v", err)
	}
	if firstQueueEvent.EventID == "" {
		t.Fatalf("expected first queue sync to return runtime event, got %+v", firstQueueEvent)
	}
	h.publishOperatorQueueEventRecord(firstQueueEvent, "workspace.ops.updated", firstQueueRecord)

	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    firstQueueRecord.QueueID,
		Limit:       10,
	}
	firstUpdatedLive := nextEventOfType(t, ch, "workspace.ops.updated")
	firstUpdatedPersisted := mustRuntimeEvent(t, ctx, store, queueFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstUpdatedLive, firstUpdatedPersisted, "workspace.ops.updated")

	seenUpdatedEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	secondQueueRecord, secondQueueEvent, err := h.syncHumanActionOperatorQueue(ctx, action)
	if err != nil {
		t.Fatalf("second syncHumanActionOperatorQueue: %v", err)
	}
	if secondQueueEvent.EventID == "" {
		t.Fatalf("expected second queue sync to return runtime event, got %+v", secondQueueEvent)
	}
	h.publishOperatorQueueEventRecord(secondQueueEvent, "workspace.ops.updated", secondQueueRecord)

	secondUpdatedLive := nextEventOfType(t, ch, "workspace.ops.updated")
	secondUpdatedPersisted := mustNewRuntimeEvent(t, ctx, store, queueFilter, seenUpdatedEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondUpdatedLive, secondUpdatedPersisted, "workspace.ops.updated")
	if secondUpdatedPersisted.EventID == firstUpdatedPersisted.EventID || secondUpdatedPersisted.IngestSeq <= firstUpdatedPersisted.IngestSeq {
		t.Fatalf("expected repeated action queue update to mirror newly appended runtime row, got first=%+v second=%+v", firstUpdatedPersisted, secondUpdatedPersisted)
	}
	var updatedEnvelope sqlite.OperatorQueueRecord
	if err := json.Unmarshal([]byte(secondUpdatedLive.PayloadJSON), &updatedEnvelope); err != nil {
		t.Fatalf("decode repeated action update payload: %v", err)
	}
	if updatedEnvelope.QueueID != firstQueueRecord.QueueID || updatedEnvelope.SourceKind != "human_action" || updatedEnvelope.SourceID != actionID {
		t.Fatalf("unexpected repeated action update payload %+v", updatedEnvelope)
	}

	firstResolvedRecord, firstResolvedEvent, err := h.syncHumanActionResolution(ctx, action, "reviewer-a", "COMPLETED", "first queue resolution")
	if err != nil {
		t.Fatalf("first syncHumanActionResolution: %v", err)
	}
	if firstResolvedEvent.EventID == "" {
		t.Fatalf("expected first queue resolution to return runtime event, got %+v", firstResolvedEvent)
	}
	h.publishOperatorQueueEventRecord(firstResolvedEvent, "workspace.ops.resolved", firstResolvedRecord)

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    firstQueueRecord.QueueID,
		Limit:       10,
	}
	firstResolvedLive := nextEventOfType(t, ch, "workspace.ops.resolved")
	firstResolvedPersisted := mustRuntimeEvent(t, ctx, store, resolvedFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstResolvedLive, firstResolvedPersisted, "workspace.ops.resolved")

	if _, _, err := h.syncHumanActionOperatorQueue(ctx, action); err == nil {
		t.Fatal("expected syncHumanActionOperatorQueue to reject implicit reopen after terminal resolution")
	} else if !strings.Contains(err.Error(), "operator queue item is not open") {
		t.Fatalf("expected not-open error on implicit action queue reopen, got %v", err)
	}
	if _, _, err := h.syncHumanActionResolution(ctx, action, "reviewer-b", "FAILED", "second queue resolution"); err == nil {
		t.Fatal("expected syncHumanActionResolution to reject repeated terminal resolution")
	} else if !strings.Contains(err.Error(), "operator queue item is not open") {
		t.Fatalf("expected not-open error on repeated action queue resolution, got %v", err)
	}
}

func TestActionCreateLinksRebaseFollowupQueueToHumanAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-link"
		taskID      = "task-action-rebase-link"
		agentID     = "agent-action-rebase-link"
		queueKey    = "tension_rebase_followup:tens-repair-link"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-link",
		"fork_tension_id":     "tens-fork-link",
		"repair_tension_id":   "tens-repair-link",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	queueRecord, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-link",
		Details:           "Coalition ID: coal-link\nRepair tension: tens-repair-link\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-link",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     queueRecord.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	if got, _ := createResp["source_queue_id"].(string); got != queueRecord.QueueID {
		t.Fatalf("source_queue_id = %q, want %q", got, queueRecord.QueueID)
	}
	if got, _ := createResp["source_queue_key"].(string); got != queueKey {
		t.Fatalf("source_queue_key = %q, want %q", got, queueKey)
	}
	if got, _ := createResp["status"].(string); got != humanActionStatusPending {
		t.Fatalf("linked actionCreate response status = %q, want %q", got, humanActionStatusPending)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get human action: %v", err)
	}
	if action.Title != queueRecord.Title {
		t.Fatalf("action title = %q, want %q", action.Title, queueRecord.Title)
	}
	if action.Description != queueRecord.Details {
		t.Fatalf("action description = %q, want %q", action.Description, queueRecord.Details)
	}
	if action.AssignedTo != "reviewer-a" {
		t.Fatalf("action assigned_to = %q, want reviewer-a", action.AssignedTo)
	}
	if !action.Blocking {
		t.Fatalf("expected rebase-linked action to default blocking from keep_session_active")
	}
	if action.AgentID != agentID {
		t.Fatalf("action agent_id = %q, want %q", action.AgentID, agentID)
	}

	actionLive, sourceQueueLive := nextActionCreatedAndQueueUpdateForQueue(t, ch, queueRecord.QueueID)
	actionRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, actionLive, actionRuntime, "action.created")
	sourceQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queueRecord.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, sourceQueueLive, sourceQueueRuntime, "workspace.ops.updated")

	linkedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, queueRecord.QueueID, "")
	if err != nil {
		t.Fatalf("get linked queue: %v", err)
	}
	if !strings.Contains(linkedQueue.Summary, actionID) {
		t.Fatalf("linked queue summary did not reference action id: %q", linkedQueue.Summary)
	}
	if !strings.Contains(linkedQueue.Details, "Linked action: "+actionID) {
		t.Fatalf("linked queue details did not reference action id: %q", linkedQueue.Details)
	}
	linkedPayload := map[string]any{}
	if err := json.Unmarshal([]byte(linkedQueue.PayloadJSON), &linkedPayload); err != nil {
		t.Fatalf("decode linked queue payload: %v", err)
	}
	if got := actionCreateQueuePayloadString(linkedPayload, "action_id"); got != actionID {
		t.Fatalf("linked queue payload action_id = %q, want %q", got, actionID)
	}
	if got := actionCreateQueuePayloadString(linkedPayload, "action_queue_key"); got != "action:"+actionID {
		t.Fatalf("linked queue payload action_queue_key = %q, want action:%s", got, actionID)
	}
	if got := actionCreateQueuePayloadString(linkedPayload, "rebase_workflow_state"); got != "claimed" {
		t.Fatalf("linked queue payload rebase_workflow_state = %q, want claimed", got)
	}
	if got := actionCreateQueuePayloadString(linkedPayload, "rebase_workflow_step"); got != "await_action_resolution" {
		t.Fatalf("linked queue payload rebase_workflow_step = %q, want await_action_resolution", got)
	}
	if !strings.Contains(linkedQueue.Details, "Workflow state: claimed") {
		t.Fatalf("linked queue details did not surface workflow state: %q", linkedQueue.Details)
	}
	if !strings.Contains(linkedQueue.Details, "Workflow step: await_action_resolution") {
		t.Fatalf("linked queue details did not surface workflow step: %q", linkedQueue.Details)
	}

	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "already linked to action") {
		t.Fatalf("expected duplicate rebase action create to fail with invalid params, got %+v", rpcErr)
	}
}

func TestActionResolveResolvesLinkedRebaseFollowupQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve"
		taskID      = "task-action-rebase-resolve"
		agentID     = "agent-action-rebase-resolve"
		queueKey    = "tension_rebase_followup:tens-repair-resolve"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-resolve",
		"fork_tension_id":     "tens-fork-resolve",
		"repair_tension_id":   "tens-repair-resolve",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-resolve",
		Details:           "Coalition ID: coal-resolve\nRepair tension: tens-repair-resolve\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-resolve",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Rebase handled.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	resolveAny, rpcErr := h.actionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	resolveResp, ok := resolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionResolve response type %T", resolveAny)
	}

	resolveLive, queueLives := nextActionResolvedAndQueueResolutionsForQueues(t, ch, actionQueue.QueueID, sourceQueue.QueueID)
	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, resolveLive, resolveRuntime, "action.resolved")
	resolvePayload := decodeEventPayloadMap(t, resolveRuntime.PayloadJSON)
	if got, _ := resolvePayload["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("resolve runtime source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}
	if got, _ := resolvePayload["source_queue_key"].(string); got != sourceQueue.QueueKey {
		t.Fatalf("resolve runtime source_queue_key = %q, want %q", got, sourceQueue.QueueKey)
	}
	if got, _ := resolvePayload["workflow_state"].(string); got != rebaseWorkflowStateCompleted {
		t.Fatalf("resolve runtime workflow_state = %q, want %q", got, rebaseWorkflowStateCompleted)
	}
	if got, _ := resolvePayload["workflow_step"].(string); got != rebaseWorkflowStepActionResolved {
		t.Fatalf("resolve runtime workflow_step = %q, want %q", got, rebaseWorkflowStepActionResolved)
	}
	if got, _ := resolveResp["status"].(string); got != humanActionStatusCompleted {
		t.Fatalf("linked actionResolve response status = %q, want %q", got, humanActionStatusCompleted)
	}

	actionQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, queueLives[actionQueue.QueueID], actionQueueRuntime, "workspace.ops.resolved")
	sourceQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, queueLives[sourceQueue.QueueID], sourceQueueRuntime, "workspace.ops.resolved")

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved source queue: %v", err)
	}
	if updatedSourceQueue.Status != "RESOLVED" {
		t.Fatalf("source queue status = %q, want RESOLVED", updatedSourceQueue.Status)
	}
	if updatedSourceQueue.Resolution != "linked_action_completed:"+actionID {
		t.Fatalf("source queue resolution = %q, want linked_action_completed:%s", updatedSourceQueue.Resolution, actionID)
	}
	if updatedSourceQueue.ResolvedBy == nil || *updatedSourceQueue.ResolvedBy != "reviewer-a" {
		t.Fatalf("source queue resolved_by = %+v, want reviewer-a", updatedSourceQueue.ResolvedBy)
	}
	if !strings.Contains(updatedSourceQueue.PayloadJSON, "\"rebase_workflow_state\":\"completed\"") {
		t.Fatalf("resolved source queue payload should surface completed workflow state: %s", updatedSourceQueue.PayloadJSON)
	}
	if !strings.Contains(updatedSourceQueue.PayloadJSON, "\"rebase_workflow_step\":\"action_resolved\"") {
		t.Fatalf("resolved source queue payload should surface action_resolved workflow step: %s", updatedSourceQueue.PayloadJSON)
	}
}

func TestActionResolveRejectsSecondTerminalAttemptAfterPromotedRebaseCompletion(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-completed-second-resolve"
		taskID      = "task-action-rebase-completed-second-resolve"
		agentID     = "agent-action-rebase-completed-second-resolve"
		queueKey    = "tension_rebase_followup:tens-repair-completed-second-resolve"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-completed-second-resolve",
		"fork_tension_id":     "tens-fork-completed-second-resolve",
		"repair_tension_id":   "tens-repair-completed-second-resolve",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for completed second resolve guard",
		Details:           "Coalition ID: coal-completed-second-resolve\nRepair tension: tens-repair-completed-second-resolve\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-completed-second-resolve",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	firstResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Rebase landed cleanly.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal first actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, firstResolveRaw); rpcErr != nil {
		t.Fatalf("first actionResolve rpc error: %+v", rpcErr)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	secondResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "FAILED",
		Comment:    "Late conflicting failure should be rejected.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal second actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, secondResolveRaw); rpcErr == nil {
		t.Fatal("expected second terminal actionResolve attempt to fail after completion")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on second terminal resolve, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get source queue after rejected second resolve: %v", err)
	}
	if currentSourceQueue.Status != "RESOLVED" || currentSourceQueue.Resolution != "linked_action_completed:"+actionID {
		t.Fatalf("source queue changed after rejected second resolve: %+v", currentSourceQueue)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("second terminal resolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("second terminal resolve should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("second terminal resolve should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestActionCreateRejectsAssignedToOverrideForLinkedRebaseFollowupQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-create-holder-guard"
		taskID      = "task-action-rebase-create-holder-guard"
		agentID     = "agent-action-rebase-create-holder-guard"
		queueKey    = "tension_rebase_followup:tens-repair-create-holder-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-create-holder-guard",
		"fork_tension_id":     "tens-fork-create-holder-guard",
		"repair_tension_id":   "tens-repair-create-holder-guard",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-create-holder-guard",
		Details:           "Coalition ID: coal-create-holder-guard\nRepair tension: tens-repair-create-holder-guard\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-holder-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create guarded rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		AssignedTo:  "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-a") {
		t.Fatalf("expected assigned holder mismatch on actionCreate, got %+v", rpcErr)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after rejected create: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no human action after rejected create, got %+v", actions)
	}
}

func TestActionCreateRejectsTaskOrAgentMismatchForLinkedRebaseFollowupQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-create-context-guard"
		taskID      = "task-action-rebase-create-context-guard"
		agentID     = "agent-action-rebase-create-context-guard"
		queueKey    = "tension_rebase_followup:tens-repair-create-context-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-create-context-guard",
		"fork_tension_id":     "tens-fork-create-context-guard",
		"repair_tension_id":   "tens-repair-create-context-guard",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-create-context-guard",
		Details:           "Coalition ID: coal-create-context-guard\nRepair tension: tens-repair-create-context-guard\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-context-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create guarded rebase follow-up queue: %v", err)
	}

	testCases := []struct {
		name        string
		params      actionCreateParams
		wantMessage string
	}{
		{
			name: "task mismatch",
			params: actionCreateParams{
				WorkspaceID: workspaceID,
				QueueID:     sourceQueue.QueueID,
				TaskID:      "task-action-rebase-create-context-other",
			},
			wantMessage: "source queue belongs to task " + taskID,
		},
		{
			name: "agent mismatch",
			params: actionCreateParams{
				WorkspaceID: workspaceID,
				QueueID:     sourceQueue.QueueID,
				AgentID:     "agent-action-rebase-create-context-other",
			},
			wantMessage: "source queue belongs to agent " + agentID,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actionCreatedFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			}
			sourceUpdatedFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			}
			seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
			seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

			createRaw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal actionCreate params: %v", err)
			}
			if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, tc.wantMessage) {
				t.Fatalf("expected linked rebase context mismatch on actionCreate, got %+v", rpcErr)
			}

			if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated) {
				t.Fatalf("rejected linked rebase create should not append action.created rows, before=%v after=%v", seenActionCreated, got)
			}
			if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
				t.Fatalf("rejected linked rebase create should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
			}

			actions, err := store.ListHumanActions(ctx, workspaceID, "")
			if err != nil {
				t.Fatalf("list human actions after rejected create: %v", err)
			}
			if len(actions) != 0 {
				t.Fatalf("expected no human action after rejected linked rebase create, got %+v", actions)
			}

			currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
			if err != nil {
				t.Fatalf("get source queue after rejected linked rebase create: %v", err)
			}
			if currentSourceQueue.UpdatedAt != sourceQueue.UpdatedAt {
				t.Fatalf("source queue updated_at after rejected linked rebase create = %q, want %q", currentSourceQueue.UpdatedAt, sourceQueue.UpdatedAt)
			}
			if currentSourceQueue.PayloadJSON != sourceQueue.PayloadJSON {
				t.Fatalf("source queue payload changed after rejected linked rebase create: %s", currentSourceQueue.PayloadJSON)
			}
		})
	}
}

func TestActionCreateRejectsTaskOrAgentMismatchForQueueOnlyLinkedRebaseFollowupQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID     = "ws-action-rebase-create-tension-context-guard"
		taskID          = "task-action-rebase-create-tension-context-guard"
		agentID         = "agent-action-rebase-create-tension-context-guard"
		queueKey        = "tension_rebase_followup:tens-repair-create-tension-context-guard"
		repairTensionID = "tens-repair-create-tension-context-guard"
		forkTensionID   = "tens-fork-create-tension-context-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	seedActionCreateQueueTensionContextForTest(t, ctx, store, workspaceID, repairTensionID, taskID, agentID)
	seedActionCreateQueueTensionContextForTest(t, ctx, store, workspaceID, forkTensionID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-create-tension-context-guard",
		"fork_tension_id":     forkTensionID,
		"repair_tension_id":   repairTensionID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase tension-derived context guard",
		Details:           "Coalition ID: coal-create-tension-context-guard\nRepair tension: " + repairTensionID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairTensionID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create queue-only rebase follow-up queue: %v", err)
	}
	if strings.TrimSpace(sourceQueue.TaskID) != "" || strings.TrimSpace(sourceQueue.AgentID) != "" {
		t.Fatalf("expected queue-only rebase follow-up fixture, got %+v", sourceQueue)
	}

	testCases := []struct {
		name        string
		params      actionCreateParams
		wantMessage string
	}{
		{
			name: "task mismatch",
			params: actionCreateParams{
				WorkspaceID: workspaceID,
				QueueID:     sourceQueue.QueueID,
				TaskID:      "task-action-rebase-create-tension-context-other",
			},
			wantMessage: "source queue belongs to task " + taskID,
		},
		{
			name: "agent mismatch",
			params: actionCreateParams{
				WorkspaceID: workspaceID,
				QueueID:     sourceQueue.QueueID,
				AgentID:     "agent-action-rebase-create-tension-context-other",
			},
			wantMessage: "source queue belongs to agent " + agentID,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actionCreatedFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			}
			sourceUpdatedFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			}
			seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
			seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

			createRaw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal actionCreate params: %v", err)
			}
			if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, tc.wantMessage) {
				t.Fatalf("expected queue-only linked rebase context mismatch on actionCreate, got %+v", rpcErr)
			}

			if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated) {
				t.Fatalf("rejected queue-only linked rebase create should not append action.created rows, before=%v after=%v", seenActionCreated, got)
			}
			if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
				t.Fatalf("rejected queue-only linked rebase create should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
			}

			actions, err := store.ListHumanActions(ctx, workspaceID, "")
			if err != nil {
				t.Fatalf("list human actions after rejected queue-only create: %v", err)
			}
			if len(actions) != 0 {
				t.Fatalf("expected no human action after rejected queue-only linked rebase create, got %+v", actions)
			}

			currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
			if err != nil {
				t.Fatalf("get source queue after rejected queue-only linked rebase create: %v", err)
			}
			if currentSourceQueue.UpdatedAt != sourceQueue.UpdatedAt {
				t.Fatalf("queue-only source queue updated_at after rejected linked rebase create = %q, want %q", currentSourceQueue.UpdatedAt, sourceQueue.UpdatedAt)
			}
			if currentSourceQueue.PayloadJSON != sourceQueue.PayloadJSON {
				t.Fatalf("queue-only source queue payload changed after rejected linked rebase create: %s", currentSourceQueue.PayloadJSON)
			}
		})
	}
}

func TestActionCreateRejectsInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-create-interleaving-escalate"
		taskID      = "task-action-rebase-create-interleaving-escalate"
		agentID     = "agent-action-rebase-create-interleaving-escalate"
		queueKey    = "tension_rebase_followup:tens-repair-create-interleaving-escalate"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-create-interleaving-escalate",
		"fork_tension_id":     "tens-fork-create-interleaving-escalate",
		"repair_tension_id":   "tens-repair-create-interleaving-escalate",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for create interleaving escalate",
		Details:           "Coalition ID: coal-create-interleaving-escalate\nRepair tension: tens-repair-create-interleaving-escalate\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-interleaving-escalate",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create guarded rebase follow-up queue: %v", err)
	}

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionCreateQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionCreateQueueEffectsOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "create-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil {
		t.Fatal("expected actionCreate to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionCreate, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate create hook: %v", hookErr)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated) {
		t.Fatalf("failed interleaving actionCreate should not append action.created rows, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionCreate should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("interleaving queue handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after rejected interleaving create: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no human action after rejected interleaving create, got %+v", actions)
	}
	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list operator queue items after rejected interleaving create: %v", err)
	}
	if len(items) != 1 || items[0].QueueID != sourceQueue.QueueID {
		t.Fatalf("expected only original source queue after rejected interleaving create, got %+v", items)
	}

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueue.QueueID, err)
	}
	if currentSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("source queue assigned_to after interleaving create = %q, want reviewer-b", currentSourceQueue.AssignedTo)
	}
	payload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload after interleaving create: %v", err)
	}
	if payload.ActionID != "" || payload.ActionQueueKey != "" {
		t.Fatalf("interleaving create should not link source queue to action, got payload %+v", payload)
	}
}

func TestActionCreateRehydratesCurrentSourceQueueLineageDuringInterleavingUpdate(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-create-interleaving-lineage"
		taskID      = "task-action-rebase-create-interleaving-lineage"
		agentID     = "agent-action-rebase-create-interleaving-lineage"
		queueKey    = "tension_rebase_followup:tens-repair-create-interleaving-lineage"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(model.RebaseFollowupPayload{
		CoalitionID:       "coal-create-interleaving-lineage",
		ForkTensionID:     "tens-fork-create-interleaving-lineage",
		RepairTensionID:   "tens-repair-create-interleaving-lineage",
		NextAction:        model.RebaseNextActionAttempt,
		RebasePlanClass:   "trim_redundancy",
		ConflictSafeClass: "rebase_candidate",
		DecisionReason:    "admission_risk",
		RootCauseID:       "root-old",
		ProvenanceGroupID: "prov-old",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, sourceQueueCreateEvent, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase lineage-only drift",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-create-interleaving-lineage",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create guarded rebase follow-up queue: %v", err)
	}

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionCreateQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionCreateQueueEffectsOverride = nil
		current, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
		if err != nil {
			hookErr = fmt.Errorf("GetOperatorQueueItem(%s): %w", sourceQueue.QueueID, err)
			return
		}
		payload, err := actionCreateDecodeQueuePayload(current.PayloadJSON)
		if err != nil {
			hookErr = fmt.Errorf("decode source queue payload: %w", err)
			return
		}
		payload.RootCauseID = "root-new"
		payload.ProvenanceGroupID = "prov-new"
		payload.ParentRefsJSON = []string{sourceQueueCreateEvent.EventID}
		payload.Normalize()
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			hookErr = fmt.Errorf("marshal source queue payload: %w", err)
			return
		}
		if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			QueueID:           current.QueueID,
			WorkspaceID:       current.WorkspaceID,
			QueueKey:          current.QueueKey,
			QueueType:         current.QueueType,
			Title:             current.Title,
			Summary:           current.Summary,
			Details:           current.Details,
			PayloadJSON:       string(payloadJSON),
			AssignedTo:        current.AssignedTo,
			Urgency:           current.Urgency,
			SourceKind:        current.SourceKind,
			SourceID:          current.SourceID,
			TaskID:            current.TaskID,
			SessionID:         current.SessionID,
			AgentID:           current.AgentID,
			KeepSessionActive: current.KeepSessionActive,
		}); err != nil {
			hookErr = fmt.Errorf("update source queue lineage: %w", err)
		}
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	if hookErr != nil {
		t.Fatalf("interleaving lineage-only create hook: %v", hookErr)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated)+1 {
		t.Fatalf("lineage-only interleaving actionCreate should append exactly one action.created row, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+2 {
		t.Fatalf("lineage-only source queue refresh plus link should append exactly two source updated rows, before=%v after=%v", seenSourceUpdated, got)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after lineage-only interleaving create: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one human action after lineage-only interleaving create, got %+v", actions)
	}

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueue.QueueID, err)
	}
	payload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload after lineage-only interleaving create: %v", err)
	}
	if payload.RootCauseID != "root-new" || payload.ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected fresh source queue lineage after interleaving create rehydrate, got %+v", payload)
	}
	if len(payload.ParentRefsJSON) != 1 || payload.ParentRefsJSON[0] != sourceQueueCreateEvent.EventID {
		t.Fatalf("expected fresh parent refs after interleaving create rehydrate, got %+v", payload.ParentRefsJSON)
	}
	if payload.ActionID != actionID || strings.TrimSpace(payload.ActionQueueKey) == "" {
		t.Fatalf("lineage-only interleaving create should link source queue to action using fresh lineage, got payload %+v", payload)
	}

	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       1,
	})
	if createRuntime.RootCauseID != "root-new" || createRuntime.ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected action.created runtime to use fresh lineage after interleaving create, got %+v", createRuntime)
	}
	var actionParentRefs []string
	if err := json.Unmarshal([]byte(createRuntime.ParentRefsJSON), &actionParentRefs); err != nil {
		t.Fatalf("decode action.created parent refs: %v", err)
	}
	if len(actionParentRefs) == 0 || !strings.Contains(strings.Join(actionParentRefs, ","), sourceQueueCreateEvent.EventID) {
		t.Fatalf("expected action.created parent refs to retain fresh lineage parent, got %+v", actionParentRefs)
	}
}

func TestActionCreateRejectsRollbackFailureInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-create-interleaving-escalate"
		taskID      = "task-action-rollback-create-interleaving-escalate"
		agentID     = "agent-action-rollback-create-interleaving-escalate"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-create-interleaving-escalate"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-create-interleaving-escalate")

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionCreateQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionCreateQueueEffectsOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "rollback-create-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil {
		t.Fatal("expected rollback-failure actionCreate to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on rollback-failure interleaving actionCreate, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate rollback-failure create hook: %v", hookErr)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated) {
		t.Fatalf("failed rollback-failure interleaving actionCreate should not append action.created rows, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed rollback-failure interleaving actionCreate should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("rollback-failure queue handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after rejected rollback-failure interleaving create: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no human action after rejected rollback-failure interleaving create, got %+v", actions)
	}
	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list operator queue items after rejected rollback-failure interleaving create: %v", err)
	}
	if len(items) != 1 || items[0].QueueID != sourceQueue.QueueID {
		t.Fatalf("expected only original rollback-failure queue after rejected create, got %+v", items)
	}

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueue.QueueID, err)
	}
	if currentSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("rollback-failure source queue assigned_to after interleaving create = %q, want reviewer-b", currentSourceQueue.AssignedTo)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure source queue payload after interleaving create: %v", err)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" {
		t.Fatalf("interleaving rollback-failure create should not link source queue to action, got payload %+v", payload)
	}
}

func TestActionCreateRehydratesRollbackFailureLineageDuringInterleavingUpdate(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-create-interleaving-lineage"
		taskID      = "task-action-rollback-create-interleaving-lineage"
		agentID     = "agent-action-rollback-create-interleaving-lineage"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-create-interleaving-lineage"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-create-interleaving-lineage")
	sourceQueueCreateEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionCreateQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionCreateQueueEffectsOverride = nil
		current, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
		if err != nil {
			hookErr = fmt.Errorf("GetOperatorQueueItem(%s): %w", sourceQueue.QueueID, err)
			return
		}
		payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
		if err != nil {
			hookErr = fmt.Errorf("decode rollback-failure payload: %w", err)
			return
		}
		payload.RootCauseID = "root-new"
		payload.ProvenanceGroupID = "prov-new"
		payload.ParentRefsJSON = []string{sourceQueueCreateEvent.EventID}
		payload.Normalize()
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			hookErr = fmt.Errorf("marshal rollback-failure payload: %w", err)
			return
		}
		if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			QueueID:           current.QueueID,
			WorkspaceID:       current.WorkspaceID,
			QueueKey:          current.QueueKey,
			QueueType:         current.QueueType,
			Title:             current.Title,
			Summary:           current.Summary,
			Details:           current.Details,
			PayloadJSON:       string(payloadJSON),
			AssignedTo:        current.AssignedTo,
			Urgency:           current.Urgency,
			SourceKind:        current.SourceKind,
			SourceID:          current.SourceID,
			TaskID:            current.TaskID,
			SessionID:         current.SessionID,
			AgentID:           current.AgentID,
			KeepSessionActive: current.KeepSessionActive,
		}); err != nil {
			hookErr = fmt.Errorf("update rollback-failure source queue lineage: %w", err)
		}
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("rollback-failure actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected rollback-failure actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected rollback-failure actionCreate response %+v", createResp)
	}
	if hookErr != nil {
		t.Fatalf("interleaving rollback-failure lineage-only create hook: %v", hookErr)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated)+1 {
		t.Fatalf("rollback-failure lineage-only interleaving actionCreate should append exactly one action.created row, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+2 {
		t.Fatalf("rollback-failure lineage refresh plus link should append exactly two source updated rows, before=%v after=%v", seenSourceUpdated, got)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after rollback-failure lineage-only interleaving create: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one human action after rollback-failure lineage-only interleaving create, got %+v", actions)
	}

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueue.QueueID, err)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure source queue payload after lineage-only interleaving create: %v", err)
	}
	if payload.RootCauseID != "root-new" || payload.ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected fresh rollback-failure source queue lineage after interleaving create rehydrate, got %+v", payload)
	}
	if len(payload.ParentRefsJSON) != 1 || payload.ParentRefsJSON[0] != sourceQueueCreateEvent.EventID {
		t.Fatalf("expected fresh rollback-failure parent refs after interleaving create rehydrate, got %+v", payload.ParentRefsJSON)
	}
	if payload.FollowupActionID != actionID || strings.TrimSpace(payload.FollowupActionQueueKey) == "" || strings.TrimSpace(payload.FollowupActionStatus) == "" {
		t.Fatalf("rollback-failure interleaving create should link source queue to action using fresh lineage, got payload %+v", payload)
	}

	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       1,
	})
	if createRuntime.RootCauseID != "root-new" || createRuntime.ProvenanceGroupID != "prov-new" {
		t.Fatalf("expected rollback-failure action.created runtime to use fresh lineage after interleaving create, got %+v", createRuntime)
	}
	var actionParentRefs []string
	if err := json.Unmarshal([]byte(createRuntime.ParentRefsJSON), &actionParentRefs); err != nil {
		t.Fatalf("decode rollback-failure action.created parent refs: %v", err)
	}
	if len(actionParentRefs) == 0 || !strings.Contains(strings.Join(actionParentRefs, ","), sourceQueueCreateEvent.EventID) {
		t.Fatalf("expected rollback-failure action.created parent refs to retain fresh lineage parent, got %+v", actionParentRefs)
	}
}

func TestActionResolveRejectsNonHolderForLinkedRebaseFollowupQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-holder-guard"
		taskID      = "task-action-rebase-resolve-holder-guard"
		agentID     = "agent-action-rebase-resolve-holder-guard"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-holder-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-resolve-holder-guard",
		"fork_tension_id":     "tens-fork-resolve-holder-guard",
		"repair_tension_id":   "tens-repair-resolve-holder-guard",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-resolve-holder-guard",
		Details:           "Coalition ID: coal-resolve-holder-guard\nRepair tension: tens-repair-resolve-holder-guard\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-resolve-holder-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create guarded rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Trying to resolve without holder authority.",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-a") {
		t.Fatalf("expected holder mismatch on actionResolve, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestActionResolveKeepsActionPendingWhenActionQueueResolutionFails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-rollback"
		taskID      = "task-action-rebase-resolve-rollback"
		agentID     = "agent-action-rebase-resolve-rollback"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-rollback"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-resolve-rollback",
		"fork_tension_id":     "tens-fork-resolve-rollback",
		"repair_tension_id":   "tens-repair-resolve-rollback",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-resolve-rollback",
		Details:           "Coalition ID: coal-resolve-rollback\nRepair tension: tens-repair-resolve-rollback\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-resolve-rollback",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     actionQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "precondition_closed",
	}); err != nil {
		t.Fatalf("resolve action queue precondition: %v", err)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Rebase handled.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatalf("expected actionResolve to fail when linked action queue is already resolved")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "not open") {
		t.Fatalf("expected invalid params not open on resolved action queue, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.ResolvedAt != "" || action.ResolutionComment != "" {
		t.Fatalf("expected action resolution to roll back, got %+v", action)
	}
}

func TestActionResolveKeepsActionPendingWhenLinkedSourceQueueIsAlreadyResolved(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-source-resolve-rollback"
		taskID      = "task-action-rebase-source-resolve-rollback"
		agentID     = "agent-action-rebase-source-resolve-rollback"
		queueKey    = "tension_rebase_followup:tens-repair-source-resolve-rollback"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-source-resolve-rollback",
		"fork_tension_id":     "tens-fork-source-resolve-rollback",
		"repair_tension_id":   "tens-repair-source-resolve-rollback",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-source-resolve-rollback",
		Details:           "Coalition ID: coal-source-resolve-rollback\nRepair tension: tens-repair-source-resolve-rollback\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-source-resolve-rollback",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "precondition_closed",
	}); err != nil {
		t.Fatalf("resolve linked source queue precondition: %v", err)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Rebase handled.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatalf("expected actionResolve to fail when linked source queue is already resolved")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "not open") {
		t.Fatalf("expected invalid params not open on resolved linked source queue, got %+v", rpcErr)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.Status != "OPEN" {
		t.Fatalf("action queue status = %q, want OPEN after rollback", actionQueue.Status)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rollback", action.Status, humanActionStatusPending)
	}
	if action.ResolvedAt != "" || action.ResolutionComment != "" {
		t.Fatalf("expected action resolution to roll back, got %+v", action)
	}
}

func TestActionResolveRejectsInterleavingSourceQueueRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-interleaving-conflict"
		taskID      = "task-action-rebase-resolve-interleaving-conflict"
		agentID     = "agent-action-rebase-resolve-interleaving-conflict"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-interleaving-conflict"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-resolve-interleaving-conflict",
		"fork_tension_id":     "tens-fork-resolve-interleaving-conflict",
		"repair_tension_id":   "tens-repair-resolve-interleaving-conflict",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for resolve interleaving conflict",
		Details:           "Coalition ID: coal-resolve-interleaving-conflict\nRepair tension: tens-repair-resolve-interleaving-conflict\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-resolve-interleaving-conflict",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving resolve conflict.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "resolve-cas-loser")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved queue revision did not produce updated_at")
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Should lose to interleaving queue write.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail on interleaving source queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" {
		t.Fatalf("action mutated after rejected interleaving resolve: %+v", action)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed interleaving actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed interleaving actionResolve should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionResolveRejectsInterleavingHumanActionRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-human-action-row-interleaving-conflict"
		taskID      = "task-action-rebase-resolve-human-action-row-interleaving-conflict"
		agentID     = "agent-action-rebase-resolve-human-action-row-interleaving-conflict"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-human-action-row-interleaving-conflict"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-human-action-row-interleaving-conflict")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before human_action row interleaving resolve conflict.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}
	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current action queue): %v", err)
	}

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)

	var interleaved sqlite.HumanActionRecord
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		interleaved = interleaveHumanActionRevisionForTest(t, ctx, store, actionID, "reviewer-b", "resolve-row-cas-loser")
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving human_action row drift.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail on interleaving human_action row drift")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionResolve, got %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" {
		t.Fatalf("action mutated after rejected human_action-row interleaving resolve: %+v", action)
	}
	if action.Revision != interleaved.Revision || action.AssignedTo != interleaved.AssignedTo {
		t.Fatalf("action should keep winner row drift after rejected resolve, got %+v want revision=%d assigned_to=%q", action, interleaved.Revision, interleaved.AssignedTo)
	}
	latestActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, currentActionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest action queue): %v", err)
	}
	if latestActionQueue.Status != "OPEN" || latestActionQueue.UpdatedAt != currentActionQueue.UpdatedAt {
		t.Fatalf("action queue mutated after rejected human_action-row resolve: before=%+v after=%+v", currentActionQueue, latestActionQueue)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed human_action-row interleaving actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed human_action-row interleaving actionResolve should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionResolveRejectsInterleavingActionQueueRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-action-queue-interleaving-conflict"
		taskID      = "task-action-rebase-resolve-action-queue-interleaving-conflict"
		agentID     = "agent-action-rebase-resolve-action-queue-interleaving-conflict"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-action-queue-interleaving-conflict"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-action-queue-interleaving-conflict")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before action-queue interleaving resolve conflict.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, actionQueue.QueueID, "resolve-action-queue-cas-loser")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved action queue revision did not produce updated_at")
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving action queue write.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail on interleaving action queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving action-queue actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" {
		t.Fatalf("action mutated after rejected action-queue interleaving resolve: %+v", action)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed action-queue interleaving actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed action-queue interleaving actionResolve should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("failed action-queue interleaving actionResolve should only keep the winner's action-queue update, before=%v after=%v", seenActionQueueUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionResolveRejectsInterleavingResolvedWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-interleaving-resolved"
		taskID      = "task-action-rebase-resolve-interleaving-resolved"
		agentID     = "agent-action-rebase-resolve-interleaving-resolved"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-interleaving-resolved"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-interleaving-resolved")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		innerRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusCompleted,
			Comment:    "Concurrent winner should resolve before loser queue effects.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving actionResolve rpc error: %+v", rpcErr)
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving direct resolve winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail after interleaving action resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving resolve winner hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusCompleted {
		t.Fatalf("action status = %q, want %q after interleaving resolve winner", action.Status, humanActionStatusCompleted)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("interleaving direct resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved)+1 {
		t.Fatalf("interleaving direct resolve winner should append exactly one source queue resolved row, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("interleaving direct resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestActionResolveRejectsInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-interleaving-escalate"
		taskID      = "task-action-rebase-resolve-interleaving-escalate"
		agentID     = "agent-action-rebase-resolve-interleaving-escalate"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-interleaving-escalate"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-interleaving-escalate")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "resolve-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving queue handoff.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate resolve hook: %v", hookErr)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed interleaving actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed interleaving actionResolve should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionResolve should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("interleaving queue handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("interleaving queue handoff should append exactly one linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	assertLinkedRebaseActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, "reviewer-b")
}

func TestActionResolveFailedRejectsInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-failed-resolve-interleaving-escalate"
		taskID      = "task-action-failed-resolve-interleaving-escalate"
		agentID     = "agent-action-failed-resolve-interleaving-escalate"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, "failed-resolve-interleaving-escalate")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueueID, "lead-b", "reviewer-b", "failed-resolve-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose failed resolve to interleaving queue handoff.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected failed actionResolve to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on failed interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate failed resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("action mutated after rejected interleaving failed resolve: %+v", action)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed interleaving failed actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving failed actionResolve should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("interleaving failed-resolve handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("interleaving failed-resolve handoff should append exactly one linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	assertLinkedRebaseActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueueID, "reviewer-b")
}

func TestActionResolveFailedRejectsInterleavingUpsertWinnerOnStartedLinkedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-action-failed-resolve-interleaving-upsert-winner-started-rebase"
		taskID        = "task-action-failed-resolve-interleaving-upsert-winner-started-rebase"
		agentID       = "agent-action-failed-resolve-interleaving-upsert-winner-started-rebase"
		repairID      = "tens-repair-action-failed-resolve-interleaving-upsert-winner-started-rebase"
		winnerSummary = "winner note should beat stale failed resolve"
		winnerDetails = "winner workspace.ops.upsert should block stale action.resolve(FAILED) on started linked rebase carrier"
		winnerDueAt   = "2099-02-08T00:00:00Z"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	actionBefore, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action before interleaving manual edit): %v", err)
	}
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving manual edit): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var (
		hookErr error
		hookRan bool
	)
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		hookRan = true

		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          sourceQueueBefore.QueueID,
			QueueKey:         sourceQueueBefore.QueueKey,
			QueueType:        sourceQueueBefore.QueueType,
			Title:            sourceQueueBefore.Title,
			Summary:          winnerSummary,
			Details:          winnerDetails,
			AssignedTo:       sourceQueueBefore.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       sourceQueueBefore.SourceKind,
			SourceID:         sourceQueueBefore.SourceID,
			TaskID:           sourceQueueBefore.TaskID,
			AgentID:          sourceQueueBefore.AgentID,
			DueAt:            winnerDueAt,
			CurrentRevision:  sourceQueueBefore.Revision,
			CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose failed resolve to interleaving manual edit winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected failed actionResolve to fail after interleaving workspace.ops.upsert winner on started linked rebase")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on failed actionResolve loser after interleaving workspace.ops.upsert winner, got %+v", rpcErr)
	} else {
		msg := strings.ToLower(rpcErr.Message)
		if strings.Contains(msg, "human action was updated concurrently") || strings.Contains(msg, "operator queue item is not open") || strings.Contains(msg, "assigned to") {
			t.Fatalf("expected source-queue CAS conflict, not adjacent guard path, got %+v", rpcErr)
		}
	}
	if !hookRan {
		t.Fatal("expected interleaving workspace.ops.upsert hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving workspaceOpsUpsert failed resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("action mutated after rejected interleaving failed resolve loser: %+v", action)
	}
	if action.Revision != actionBefore.Revision {
		t.Fatalf("action revision changed after rejected interleaving failed resolve loser: before=%d after=%d", actionBefore.Revision, action.Revision)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after interleaving manual edit winner): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("source queue status after interleaving manual edit winner = %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != sourceQueueBefore.AssignedTo {
		t.Fatalf("source queue assigned_to after interleaving manual edit winner = %q, want %q", currentQueue.AssignedTo, sourceQueueBefore.AssignedTo)
	}
	if strings.TrimSpace(currentQueue.Resolution) != "" || derefString(currentQueue.ResolvedBy) != "" {
		t.Fatalf("source queue should remain open after stale failed resolve loser, got resolution=%q resolved_by=%q", currentQueue.Resolution, derefString(currentQueue.ResolvedBy))
	}
	if currentQueue.UpdatedAt == sourceQueueBefore.UpdatedAt {
		t.Fatalf("winning manual edit should advance source queue updated_at, before=%q after=%q", sourceQueueBefore.UpdatedAt, currentQueue.UpdatedAt)
	}
	if currentQueue.Revision != sourceQueueBefore.Revision+1 {
		t.Fatalf("winning manual edit should advance source queue revision exactly once, before=%d after=%d", sourceQueueBefore.Revision, currentQueue.Revision)
	}
	if currentQueue.Summary != winnerSummary || currentQueue.Details != winnerDetails {
		t.Fatalf("source queue should keep winner-owned manual edit text, got summary=%q details=%q", currentQueue.Summary, currentQueue.Details)
	}
	if currentQueue.Urgency != "CRITICAL" || derefString(currentQueue.DueAt) != winnerDueAt {
		t.Fatalf("source queue should keep winner-owned urgency/due_at, got urgency=%q due_at=%q", currentQueue.Urgency, derefString(currentQueue.DueAt))
	}
	if currentQueue.EscalationCount != sourceQueueBefore.EscalationCount {
		t.Fatalf("winning manual edit should not change source queue escalation_count, before=%d after=%d", sourceQueueBefore.EscalationCount, currentQueue.EscalationCount)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionQueueKey == "" || currentPayload.ActionStatus != humanActionStatusPending || currentPayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("source queue should keep pending linked action truth after stale failed resolve loser = %+v", currentPayload)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("source queue workflow after stale failed resolve loser = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}

	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue after interleaving manual edit winner): %v", err)
	}
	if currentActionQueue.Status != "OPEN" || currentActionQueue.AssignedTo != actionQueueBefore.AssignedTo || currentActionQueue.UpdatedAt != actionQueueBefore.UpdatedAt {
		t.Fatalf("action queue mutated after stale failed resolve loser: before=%+v after=%+v", actionQueueBefore, currentActionQueue)
	}
	if currentActionQueue.Revision != actionQueueBefore.Revision {
		t.Fatalf("action queue revision changed after stale failed resolve loser: before=%d after=%d", actionQueueBefore.Revision, currentActionQueue.Revision)
	}
	if strings.TrimSpace(currentActionQueue.Resolution) != "" || derefString(currentActionQueue.ResolvedBy) != "" {
		t.Fatalf("action queue should remain open after stale failed resolve loser, got resolution=%q resolved_by=%q", currentActionQueue.Resolution, derefString(currentActionQueue.ResolvedBy))
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed interleaving failed actionResolve loser should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed interleaving failed actionResolve loser should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving workspace.ops.upsert winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving workspace.ops.upsert winner should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed interleaving failed actionResolve loser should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionResolveRejectsInterleavingUpsertWinnerOnStartedLinkedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-action-resolve-interleaving-upsert-winner-started-rebase"
		taskID        = "task-action-resolve-interleaving-upsert-winner-started-rebase"
		agentID       = "agent-action-resolve-interleaving-upsert-winner-started-rebase"
		repairID      = "tens-repair-action-resolve-interleaving-upsert-winner-started-rebase"
		winnerSummary = "winner note should beat stale direct resolve"
		winnerDetails = "winner workspace.ops.upsert should block stale action.resolve on started linked rebase carrier"
		winnerDueAt   = "2099-02-01T00:00:00Z"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	actionBefore, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action before interleaving manual edit): %v", err)
	}
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving manual edit): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var (
		hookErr error
		hookRan bool
	)
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		hookRan = true

		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          sourceQueueBefore.QueueID,
			QueueKey:         sourceQueueBefore.QueueKey,
			QueueType:        sourceQueueBefore.QueueType,
			Title:            sourceQueueBefore.Title,
			Summary:          winnerSummary,
			Details:          winnerDetails,
			AssignedTo:       sourceQueueBefore.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       sourceQueueBefore.SourceKind,
			SourceID:         sourceQueueBefore.SourceID,
			TaskID:           sourceQueueBefore.TaskID,
			AgentID:          sourceQueueBefore.AgentID,
			DueAt:            winnerDueAt,
			CurrentRevision:  sourceQueueBefore.Revision,
			CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving manual edit winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail after interleaving workspace.ops.upsert winner on started linked rebase")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on actionResolve loser after interleaving workspace.ops.upsert winner, got %+v", rpcErr)
	} else {
		msg := strings.ToLower(rpcErr.Message)
		if strings.Contains(msg, "human action was updated concurrently") || strings.Contains(msg, "operator queue item is not open") || strings.Contains(msg, "assigned to") {
			t.Fatalf("expected source-queue CAS conflict, not adjacent guard path, got %+v", rpcErr)
		}
	}
	if !hookRan {
		t.Fatal("expected interleaving workspace.ops.upsert hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving workspaceOpsUpsert resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("action mutated after rejected interleaving resolve loser: %+v", action)
	}
	if action.Revision != actionBefore.Revision {
		t.Fatalf("action revision changed after rejected interleaving resolve loser: before=%d after=%d", actionBefore.Revision, action.Revision)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after interleaving manual edit winner): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("source queue status after interleaving manual edit winner = %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != sourceQueueBefore.AssignedTo {
		t.Fatalf("source queue assigned_to after interleaving manual edit winner = %q, want %q", currentQueue.AssignedTo, sourceQueueBefore.AssignedTo)
	}
	if strings.TrimSpace(currentQueue.Resolution) != "" || derefString(currentQueue.ResolvedBy) != "" {
		t.Fatalf("source queue should remain open after stale resolve loser, got resolution=%q resolved_by=%q", currentQueue.Resolution, derefString(currentQueue.ResolvedBy))
	}
	if currentQueue.UpdatedAt == sourceQueueBefore.UpdatedAt {
		t.Fatalf("winning manual edit should advance source queue updated_at, before=%q after=%q", sourceQueueBefore.UpdatedAt, currentQueue.UpdatedAt)
	}
	if currentQueue.Revision != sourceQueueBefore.Revision+1 {
		t.Fatalf("winning manual edit should advance source queue revision exactly once, before=%d after=%d", sourceQueueBefore.Revision, currentQueue.Revision)
	}
	if currentQueue.Summary != winnerSummary || currentQueue.Details != winnerDetails {
		t.Fatalf("source queue should keep winner-owned manual edit text, got summary=%q details=%q", currentQueue.Summary, currentQueue.Details)
	}
	if currentQueue.Urgency != "CRITICAL" || derefString(currentQueue.DueAt) != winnerDueAt {
		t.Fatalf("source queue should keep winner-owned urgency/due_at, got urgency=%q due_at=%q", currentQueue.Urgency, derefString(currentQueue.DueAt))
	}
	if currentQueue.EscalationCount != sourceQueueBefore.EscalationCount {
		t.Fatalf("winning manual edit should not change source queue escalation_count, before=%d after=%d", sourceQueueBefore.EscalationCount, currentQueue.EscalationCount)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionQueueKey == "" || currentPayload.ActionStatus != humanActionStatusPending || currentPayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("source queue should keep pending linked action truth after stale resolve loser = %+v", currentPayload)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("source queue workflow after stale resolve loser = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}

	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue after interleaving manual edit winner): %v", err)
	}
	if currentActionQueue.Status != "OPEN" || currentActionQueue.AssignedTo != actionQueueBefore.AssignedTo || currentActionQueue.UpdatedAt != actionQueueBefore.UpdatedAt {
		t.Fatalf("action queue mutated after stale resolve loser: before=%+v after=%+v", actionQueueBefore, currentActionQueue)
	}
	if currentActionQueue.Revision != actionQueueBefore.Revision {
		t.Fatalf("action queue revision changed after stale resolve loser: before=%d after=%d", actionQueueBefore.Revision, currentActionQueue.Revision)
	}
	if strings.TrimSpace(currentActionQueue.Resolution) != "" || derefString(currentActionQueue.ResolvedBy) != "" {
		t.Fatalf("action queue should remain open after stale resolve loser, got resolution=%q resolved_by=%q", currentActionQueue.Resolution, derefString(currentActionQueue.ResolvedBy))
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed interleaving actionResolve loser should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed interleaving actionResolve loser should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving workspace.ops.upsert winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving workspace.ops.upsert winner should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed interleaving actionResolve loser should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionResolveRejectsInterleavingEscalateWinnerOnStartedLinkedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-resolve-interleaving-escalate-winner-started-rebase"
		taskID      = "task-action-resolve-interleaving-escalate-winner-started-rebase"
		agentID     = "agent-action-resolve-interleaving-escalate-winner-started-rebase"
		repairID    = "tens-repair-action-resolve-interleaving-escalate-winner-started-rebase"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving handoff): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var (
		hookErr     error
		hookRan     bool
		winnerQueue sqlite.OperatorQueueRecord
	)
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		hookRan = true
		winnerQueue, hookErr = interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueueID, "lead-b", "reviewer-b", "completed-resolve-handoff-cas-winner")
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose completed resolve to interleaving queue handoff.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail on interleaving queue handoff for started linked rebase")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "human action was updated concurrently") {
		t.Fatalf("expected invalid params human-action updated concurrently on started-carrier interleaving actionResolve, got %+v", rpcErr)
	} else {
		msg := strings.ToLower(rpcErr.Message)
		if strings.Contains(msg, "assigned to") || strings.Contains(msg, "operator queue item is not open") || strings.Contains(msg, "operator queue item was updated concurrently") {
			t.Fatalf("expected human-action CAS conflict, not adjacent guard path, got %+v", rpcErr)
		}
	}
	if !hookRan {
		t.Fatal("expected interleaving workspaceOpsEscalate hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != "reviewer-b" || action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("action mutated unexpectedly after rejected started-carrier resolve loser: %+v", action)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after interleaving handoff): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("source queue status after interleaving handoff = %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != winnerQueue.AssignedTo || currentQueue.Urgency != winnerQueue.Urgency || currentQueue.UpdatedAt != winnerQueue.UpdatedAt || derefString(currentQueue.DueAt) != derefString(winnerQueue.DueAt) {
		t.Fatalf("started-carrier resolve loser mutated winner source queue state: got %+v want assigned_to=%q urgency=%q due_at=%q updated_at=%q", currentQueue, winnerQueue.AssignedTo, winnerQueue.Urgency, derefString(winnerQueue.DueAt), winnerQueue.UpdatedAt)
	}
	if currentQueue.EscalationCount != sourceQueueBefore.EscalationCount+1 {
		t.Fatalf("source queue escalation_count after interleaving handoff = %d, want %d", currentQueue.EscalationCount, sourceQueueBefore.EscalationCount+1)
	}
	if strings.TrimSpace(currentQueue.Resolution) != "" || derefString(currentQueue.ResolvedBy) != "" {
		t.Fatalf("source queue should remain open after started-carrier resolve loser, got resolution=%q resolved_by=%q", currentQueue.Resolution, derefString(currentQueue.ResolvedBy))
	}

	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionStatus != humanActionStatusPending || currentPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("source queue should keep winner-owned pending handoff truth after started-carrier resolve loser = %+v", currentPayload)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("source queue workflow after started-carrier resolve loser = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}

	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue after interleaving handoff): %v", err)
	}
	if currentActionQueue.Status != "OPEN" || currentActionQueue.AssignedTo != "reviewer-b" || currentActionQueue.UpdatedAt == actionQueueBefore.UpdatedAt {
		t.Fatalf("expected exactly one winning action-queue reassignment after started-carrier resolve loser, got %+v (before updated_at=%q)", currentActionQueue, actionQueueBefore.UpdatedAt)
	}
	if strings.TrimSpace(currentActionQueue.Resolution) != "" || derefString(currentActionQueue.ResolvedBy) != "" {
		t.Fatalf("action queue should remain open after started-carrier resolve loser, got resolution=%q resolved_by=%q", currentActionQueue.Resolution, derefString(currentActionQueue.ResolvedBy))
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed started-carrier interleaving actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed started-carrier interleaving actionResolve should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed started-carrier interleaving actionResolve should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("interleaving started-carrier handoff winner should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("interleaving started-carrier handoff winner should append exactly one linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed started-carrier interleaving actionResolve should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	assertLinkedRebaseActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueueID, "reviewer-b")
}

func TestActionResolveRejectsInterleavingPauseWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-resolve-interleaving-pause"
		taskID      = "task-action-rebase-resolve-interleaving-pause"
		agentID     = "agent-action-rebase-resolve-interleaving-pause"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-interleaving-pause"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-interleaving-pause")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving pause winner.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		innerRaw, err := json.Marshal(actionPauseParams{
			ActionID: actionID,
			PausedBy: "reviewer-a",
			Comment:  "Concurrent pause winner should move the carrier into restart-needed state first.",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving actionPause params: %w", err)
			return
		}
		if _, rpcErr := h.actionPause(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving actionPause rpc error: %+v", rpcErr)
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving action.pause winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected actionResolve to fail after interleaving action.pause winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving action.pause winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving action.pause winner hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("action mutated after rejected interleaving resolve-vs-pause winner: %+v", action)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused)+1 {
		t.Fatalf("interleaving action.pause winner should append exactly one action.paused row, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed resolve loser should not append action.resolved rows after interleaving action.pause winner, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving action.pause winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving action.pause winner should not append linked action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed resolve loser should not append linked action queue resolved rows after interleaving action.pause winner, before=%v after=%v", seenActionQueueResolved, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestActionResolveFailedRejectsInterleavingPauseWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-failed-resolve-interleaving-pause"
		taskID      = "task-action-rebase-failed-resolve-interleaving-pause"
		agentID     = "agent-action-rebase-failed-resolve-interleaving-pause"
		queueKey    = "tension_rebase_followup:tens-repair-failed-resolve-interleaving-pause"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-resolve-interleaving-pause")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving failed-resolve pause winner.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		innerRaw, err := json.Marshal(actionPauseParams{
			ActionID: actionID,
			PausedBy: "reviewer-a",
			Comment:  "Concurrent pause winner should rewind the carrier before failed resolve lands.",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving actionPause params: %w", err)
			return
		}
		if _, rpcErr := h.actionPause(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving actionPause rpc error: %+v", rpcErr)
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose failed resolve to interleaving action.pause winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected failed actionResolve to fail after interleaving action.pause winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving failed action.pause winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving action.pause failed resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("action mutated after rejected failed resolve vs interleaving action.pause winner: %+v", action)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused)+1 {
		t.Fatalf("interleaving action.pause winner should append exactly one action.paused row, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed resolve loser should not append action.resolved rows after interleaving action.pause winner, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving action.pause winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving action.pause winner should not append linked action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed resolve loser should not append linked action queue resolved rows after interleaving action.pause winner, before=%v after=%v", seenActionQueueResolved, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestActionResolveRejectsRollbackFailureInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-resolve-interleaving-escalate"
		taskID      = "task-action-rollback-resolve-interleaving-escalate"
		agentID     = "agent-action-rollback-resolve-interleaving-escalate"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-resolve-interleaving-escalate"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-interleaving-escalate")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "rollback-resolve-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving rollback-failure queue handoff.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected rollback-failure actionResolve to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on rollback-failure interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate rollback-failure resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("rollback-failure action mutated after rejected interleaving resolve: %+v", action)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed rollback-failure interleaving actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed rollback-failure interleaving actionResolve should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed rollback-failure interleaving actionResolve should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("rollback-failure interleaving queue handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("rollback-failure interleaving queue handoff should append exactly one linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	assertLinkedRollbackFailureActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, "reviewer-b")
}

func TestActionResolveRejectsRollbackFailureInterleavingUpsertWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-action-rollback-resolve-interleaving-upsert"
		taskID        = "task-action-rollback-resolve-interleaving-upsert"
		agentID       = "agent-action-rollback-resolve-interleaving-upsert"
		queueKey      = model.RebaseRollbackFailureQueueKeyPrefix + "repair-resolve-interleaving-upsert"
		winnerSummary = "winner manual edit should beat stale rollback-failure resolve"
		winnerDetails = "winner workspace.ops.upsert should block stale action.resolve on linked pending rollback-failure carrier"
		winnerDueAt   = "2099-07-01T00:00:00Z"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-interleaving-upsert")
	actionBefore, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action before interleaving manual edit): %v", err)
	}
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving manual edit): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var (
		hookErr error
		hookRan bool
	)
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		hookRan = true

		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          sourceQueueBefore.QueueID,
			QueueKey:         sourceQueueBefore.QueueKey,
			QueueType:        sourceQueueBefore.QueueType,
			Title:            sourceQueueBefore.Title,
			Summary:          winnerSummary,
			Details:          winnerDetails,
			AssignedTo:       sourceQueueBefore.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       sourceQueueBefore.SourceKind,
			SourceID:         sourceQueueBefore.SourceID,
			TaskID:           sourceQueueBefore.TaskID,
			AgentID:          sourceQueueBefore.AgentID,
			DueAt:            winnerDueAt,
			CurrentRevision:  sourceQueueBefore.Revision,
			CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving rollback-failure manual edit winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected rollback-failure actionResolve to fail after interleaving workspace.ops.upsert winner")
	} else {
		msg := strings.ToLower(rpcErr.Message)
		if rpcErr.Code != errCodeInvalidParams || !strings.Contains(msg, "updated concurrently") || !strings.Contains(rpcErr.Message, sourceQueueBefore.QueueID) {
			t.Fatalf("expected invalid params source-queue updated concurrently on rollback-failure interleaving actionResolve, got %+v", rpcErr)
		}
		if strings.Contains(msg, "human action was updated concurrently") ||
			strings.Contains(msg, "operator queue item is not open") ||
			strings.Contains(msg, "linked to pending action") ||
			strings.Contains(msg, "assigned to") {
			t.Fatalf("expected source-queue CAS conflict on rollback-failure interleaving actionResolve, not adjacent guard path, got %+v", rpcErr)
		}
	}
	if !hookRan {
		t.Fatal("expected interleaving workspace.ops.upsert hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving workspaceOpsUpsert rollback-failure resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != actionBefore.AssignedTo || action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("rollback-failure action mutated after rejected interleaving resolve loser: %+v", action)
	}
	if action.Revision != actionBefore.Revision {
		t.Fatalf("action revision changed after rejected interleaving resolve loser: before=%d after=%d", actionBefore.Revision, action.Revision)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after interleaving manual edit winner): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("rollback-failure source queue status after interleaving manual edit winner = %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != sourceQueueBefore.AssignedTo {
		t.Fatalf("rollback-failure source queue assigned_to after interleaving manual edit winner = %q, want %q", currentQueue.AssignedTo, sourceQueueBefore.AssignedTo)
	}
	if strings.TrimSpace(currentQueue.Resolution) != "" || derefString(currentQueue.ResolvedBy) != "" {
		t.Fatalf("rollback-failure source queue should remain open after stale resolve loser, got resolution=%q resolved_by=%q", currentQueue.Resolution, derefString(currentQueue.ResolvedBy))
	}
	if currentQueue.UpdatedAt == sourceQueueBefore.UpdatedAt {
		t.Fatalf("winning manual edit should advance rollback-failure source queue updated_at, before=%q after=%q", sourceQueueBefore.UpdatedAt, currentQueue.UpdatedAt)
	}
	if currentQueue.Revision != sourceQueueBefore.Revision+1 {
		t.Fatalf("winning manual edit should advance rollback-failure source queue revision exactly once, before=%d after=%d", sourceQueueBefore.Revision, currentQueue.Revision)
	}
	if currentQueue.Summary != winnerSummary || currentQueue.Details != winnerDetails {
		t.Fatalf("rollback-failure source queue should keep winner-owned manual edit text, got summary=%q details=%q", currentQueue.Summary, currentQueue.Details)
	}
	if currentQueue.Urgency != "CRITICAL" || derefString(currentQueue.DueAt) != winnerDueAt {
		t.Fatalf("rollback-failure source queue should keep winner-owned urgency/due_at, got urgency=%q due_at=%q", currentQueue.Urgency, derefString(currentQueue.DueAt))
	}
	if currentQueue.EscalationCount != sourceQueueBefore.EscalationCount {
		t.Fatalf("winning manual edit should not change rollback-failure source queue escalation_count, before=%d after=%d", sourceQueueBefore.EscalationCount, currentQueue.EscalationCount)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure source queue payload after stale resolve loser: %v", err)
	}
	if payload.FollowupActionID != actionID || payload.FollowupActionQueueKey != "action:"+actionID || payload.FollowupActionStatus != humanActionStatusPending {
		t.Fatalf("rollback-failure source queue should keep active pending followup truth after stale resolve loser = %+v", payload)
	}
	if payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("rollback-failure stale resolve loser should not mint failed lineage, got %+v", payload)
	}

	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue after interleaving manual edit winner): %v", err)
	}
	if currentActionQueue.Status != "OPEN" || currentActionQueue.AssignedTo != actionQueueBefore.AssignedTo || currentActionQueue.UpdatedAt != actionQueueBefore.UpdatedAt {
		t.Fatalf("action queue mutated after stale rollback-failure resolve loser: before=%+v after=%+v", actionQueueBefore, currentActionQueue)
	}
	if currentActionQueue.Revision != actionQueueBefore.Revision {
		t.Fatalf("action queue revision changed after stale rollback-failure resolve loser: before=%d after=%d", actionQueueBefore.Revision, currentActionQueue.Revision)
	}
	if strings.TrimSpace(currentActionQueue.Resolution) != "" || derefString(currentActionQueue.ResolvedBy) != "" {
		t.Fatalf("action queue should remain open after stale rollback-failure resolve loser, got resolution=%q resolved_by=%q", currentActionQueue.Resolution, derefString(currentActionQueue.ResolvedBy))
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed rollback-failure interleaving actionResolve loser should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed rollback-failure interleaving actionResolve loser should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving workspace.ops.upsert winner should append exactly one rollback-failure source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving workspace.ops.upsert winner should not append rollback-failure action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed rollback-failure interleaving actionResolve loser should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestActionResolveRejectsInterleavingEscalateWinnerOnStandaloneHumanActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-standalone-resolve-interleaving-escalate"
		taskID      = "task-action-standalone-resolve-interleaving-escalate"
		agentID     = "agent-action-standalone-resolve-interleaving-escalate"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Standalone resolve-vs-handoff authority race",
		Description: "Standalone human action should keep winner-owned handoff truth.",
		Blocking:    boolPtr(true),
	})
	if err != nil {
		t.Fatalf("marshal standalone actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("standalone actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected standalone actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected standalone actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	actionQueueEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenActionQueueEscalated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueEscalatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, actionQueue.QueueID, "lead-b", "reviewer-b", "standalone-resolve-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving standalone queue handoff.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal standalone actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected standalone actionResolve to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on standalone interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving standalone escalate resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != "reviewer-b" || action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("standalone action mutated unexpectedly after rejected interleaving resolve: %+v", action)
	}

	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", actionQueue.QueueID, err)
	}
	if currentActionQueue.Status != "OPEN" {
		t.Fatalf("standalone action queue status = %q, want OPEN", currentActionQueue.Status)
	}
	if currentActionQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("standalone action queue assigned_to = %q, want reviewer-b", currentActionQueue.AssignedTo)
	}
	if currentActionQueue.EscalationCount != actionQueue.EscalationCount+1 {
		t.Fatalf("standalone action queue escalation_count = %d, want %d", currentActionQueue.EscalationCount, actionQueue.EscalationCount+1)
	}
	if strings.TrimSpace(currentActionQueue.Resolution) != "" || derefString(currentActionQueue.ResolvedBy) != "" {
		t.Fatalf("standalone action queue should remain open after winning handoff, got resolution=%q resolved_by=%q", currentActionQueue.Resolution, derefString(currentActionQueue.ResolvedBy))
	}
	queuePayload, err := actionCreateDecodeQueuePayload(currentActionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode standalone action queue payload: %v", err)
	}
	if got := actionCreateQueuePayloadString(queuePayload, "action_assigned_to"); got != "reviewer-b" {
		t.Fatalf("standalone action queue payload action_assigned_to = %q, want reviewer-b", got)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed standalone interleaving actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed standalone interleaving actionResolve should not append queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueEscalatedFilter); len(got) != len(seenActionQueueEscalated)+1 {
		t.Fatalf("interleaving standalone queue handoff should append exactly one queue escalation row, before=%v after=%v", seenActionQueueEscalated, got)
	}
}

func TestActionResolveRejectsRollbackFailureInterleavingResolvedWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-resolve-interleaving-resolved"
		taskID      = "task-action-rollback-resolve-interleaving-resolved"
		agentID     = "agent-action-rollback-resolve-interleaving-resolved"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-resolve-interleaving-resolved"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "resolve-interleaving-resolved")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		innerRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusCompleted,
			Comment:    "Concurrent rollback-failure winner should resolve before loser queue effects.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal rollback-failure interleaving actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("rollback-failure interleaving actionResolve rpc error: %+v", rpcErr)
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Should lose to interleaving rollback-failure direct resolve winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected rollback-failure actionResolve to fail after interleaving action resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on rollback-failure interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("rollback-failure interleaving resolve winner hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusCompleted {
		t.Fatalf("rollback-failure action status = %q, want %q after interleaving resolve winner", action.Status, humanActionStatusCompleted)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("rollback-failure interleaving direct resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved)+1 {
		t.Fatalf("rollback-failure interleaving direct resolve winner should append exactly one source queue resolved row, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure interleaving direct resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestActionResolveFailedRejectsInterleavingResolvedWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-failed-resolve-interleaving-resolved"
		taskID      = "task-action-rebase-failed-resolve-interleaving-resolved"
		agentID     = "agent-action-rebase-failed-resolve-interleaving-resolved"
		queueKey    = "tension_rebase_followup:tens-repair-failed-resolve-interleaving-resolved"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-resolve-interleaving-resolved")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving failed direct resolve.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		innerRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "Concurrent linked rebase failed winner should resolve before loser queue effects.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal linked rebase failed interleaving actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("linked rebase failed interleaving actionResolve rpc error: %+v", rpcErr)
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose to interleaving linked rebase failed resolve winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal linked rebase failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected linked rebase failed actionResolve to fail after interleaving action resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on linked rebase failed interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("linked rebase failed interleaving resolve winner hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("linked rebase failed action status = %q, want %q after interleaving resolve winner", action.Status, humanActionStatusFailed)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("linked rebase failed interleaving direct resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("linked rebase failed interleaving direct resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("linked rebase failed interleaving direct resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked rebase source queue after interleaving failed resolve winner: %v", err)
	}
	if updatedSourceQueue.Status != "OPEN" {
		t.Fatalf("linked rebase source queue status = %q, want OPEN after interleaving failed resolve winner", updatedSourceQueue.Status)
	}
	payload, err := actionCreateDecodeQueuePayload(updatedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode linked rebase source queue payload after interleaving failed resolve winner: %v", err)
	}
	if payload.ActionID != "" || payload.ActionQueueKey != "" || payload.ActionStatus != "" {
		t.Fatalf("active linked rebase action should be cleared after interleaving failed resolve winner: %+v", payload)
	}
	if payload.LastFailedActionID != actionID {
		t.Fatalf("last_failed_action_id = %q, want %q after interleaving failed resolve winner", payload.LastFailedActionID, actionID)
	}
	if payload.LastFailedStatus != humanActionStatusFailed {
		t.Fatalf("last_failed_status = %q, want %q after interleaving failed resolve winner", payload.LastFailedStatus, humanActionStatusFailed)
	}
	if payload.RollbackReason != "linked_action_failed" {
		t.Fatalf("rollback_reason = %q, want linked_action_failed after interleaving failed resolve winner", payload.RollbackReason)
	}
	if payload.RebaseWorkflowState != rebaseWorkflowStateClaimed {
		t.Fatalf("payload workflow_state = %q, want %q after interleaving failed resolve winner", payload.RebaseWorkflowState, rebaseWorkflowStateClaimed)
	}
	if payload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("payload workflow_step = %q, want %q after interleaving failed resolve winner", payload.RebaseWorkflowStep, rebaseWorkflowStepAwaitRestart)
	}
}

func TestActionResolveFailedRejectsRollbackFailureInterleavingResolvedWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-resolve-interleaving-resolved"
		taskID      = "task-action-rollback-failed-resolve-interleaving-resolved"
		agentID     = "agent-action-rollback-failed-resolve-interleaving-resolved"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-failed-resolve-interleaving-resolved"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-resolve-interleaving-resolved")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		innerRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "Concurrent rollback-failure failed winner should resolve before loser queue effects.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal rollback-failure failed interleaving actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("rollback-failure failed interleaving actionResolve rpc error: %+v", rpcErr)
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose to interleaving rollback-failure failed resolve winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected rollback-failure failed actionResolve to fail after interleaving action resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on rollback-failure failed interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("rollback-failure failed interleaving resolve winner hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("rollback-failure failed action status = %q, want %q after interleaving resolve winner", action.Status, humanActionStatusFailed)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("rollback-failure failed interleaving direct resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("rollback-failure failed interleaving direct resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure failed interleaving direct resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}

	updatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get rollback-failure queue after interleaving failed resolve winner: %v", err)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(updatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after interleaving failed resolve winner: %v", err)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" {
		t.Fatalf("active followup link should be cleared after interleaving failed resolve winner: %+v", payload)
	}
	if payload.LastFailedFollowupActionID != actionID {
		t.Fatalf("last_failed_followup_action_id = %q, want %q after interleaving failed resolve winner", payload.LastFailedFollowupActionID, actionID)
	}
	if payload.LastFailedFollowupActionStatus != humanActionStatusFailed {
		t.Fatalf("last_failed_followup_action_status = %q, want %q after interleaving failed resolve winner", payload.LastFailedFollowupActionStatus, humanActionStatusFailed)
	}
}

func TestActionResolveFailedRejectsRollbackFailureInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-resolve-interleaving-escalate"
		taskID      = "task-action-rollback-failed-resolve-interleaving-escalate"
		agentID     = "agent-action-rollback-failed-resolve-interleaving-escalate"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-failed-resolve-interleaving-escalate"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-resolve-interleaving-escalate")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "rollback-failed-resolve-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose failed resolve to interleaving rollback-failure handoff.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected rollback-failure failed actionResolve to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on rollback-failure failed interleaving actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate rollback-failure failed resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("rollback-failure action mutated after rejected interleaving failed resolve: %+v", action)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed rollback-failure interleaving failed actionResolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed rollback-failure interleaving failed actionResolve should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("rollback-failure interleaving failed-resolve handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("rollback-failure interleaving failed-resolve handoff should append exactly one linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	assertLinkedRollbackFailureActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, "reviewer-b")
}

func TestActionResolveFailedRejectsRollbackFailureInterleavingUpsertWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-action-rollback-failed-resolve-interleaving-upsert"
		taskID        = "task-action-rollback-failed-resolve-interleaving-upsert"
		agentID       = "agent-action-rollback-failed-resolve-interleaving-upsert"
		queueKey      = model.RebaseRollbackFailureQueueKeyPrefix + "repair-failed-resolve-interleaving-upsert"
		winnerSummary = "winner manual edit should beat stale rollback-failure failed resolve"
		winnerDetails = "winner workspace.ops.upsert should block stale action.resolve(FAILED) on linked pending rollback-failure carrier"
		winnerDueAt   = "2099-08-01T00:00:00Z"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-resolve-interleaving-upsert")
	actionBefore, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action before interleaving manual edit): %v", err)
	}
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving manual edit): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var (
		hookErr error
		hookRan bool
	)
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		hookRan = true

		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          sourceQueueBefore.QueueID,
			QueueKey:         sourceQueueBefore.QueueKey,
			QueueType:        sourceQueueBefore.QueueType,
			Title:            sourceQueueBefore.Title,
			Summary:          winnerSummary,
			Details:          winnerDetails,
			AssignedTo:       sourceQueueBefore.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       sourceQueueBefore.SourceKind,
			SourceID:         sourceQueueBefore.SourceID,
			TaskID:           sourceQueueBefore.TaskID,
			AgentID:          sourceQueueBefore.AgentID,
			DueAt:            winnerDueAt,
			CurrentRevision:  sourceQueueBefore.Revision,
			CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose failed resolve to interleaving rollback-failure manual edit winner.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected rollback-failure failed actionResolve to fail after interleaving workspace.ops.upsert winner")
	} else {
		msg := strings.ToLower(rpcErr.Message)
		if rpcErr.Code != errCodeInvalidParams || !strings.Contains(msg, "updated concurrently") || !strings.Contains(rpcErr.Message, sourceQueueBefore.QueueID) {
			t.Fatalf("expected invalid params source-queue updated concurrently on rollback-failure failed interleaving actionResolve, got %+v", rpcErr)
		}
		if strings.Contains(msg, "human action was updated concurrently") ||
			strings.Contains(msg, "operator queue item is not open") ||
			strings.Contains(msg, "linked source queue") ||
			strings.Contains(msg, "already resolved") ||
			strings.Contains(msg, "linked to pending action") ||
			strings.Contains(msg, "assigned to") {
			t.Fatalf("expected source-queue CAS conflict on rollback-failure failed interleaving actionResolve, not adjacent guard path, got %+v", rpcErr)
		}
	}
	if !hookRan {
		t.Fatal("expected interleaving workspace.ops.upsert hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving workspaceOpsUpsert rollback-failure failed resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != actionBefore.AssignedTo || action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("rollback-failure action mutated after rejected interleaving failed resolve loser: %+v", action)
	}
	if action.Revision != actionBefore.Revision {
		t.Fatalf("action revision changed after rejected interleaving failed resolve loser: before=%d after=%d", actionBefore.Revision, action.Revision)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after interleaving manual edit winner): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("rollback-failure source queue status after interleaving failed resolve loser = %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != sourceQueueBefore.AssignedTo {
		t.Fatalf("rollback-failure source queue assigned_to after interleaving failed resolve loser = %q, want %q", currentQueue.AssignedTo, sourceQueueBefore.AssignedTo)
	}
	if strings.TrimSpace(currentQueue.Resolution) != "" || derefString(currentQueue.ResolvedBy) != "" {
		t.Fatalf("rollback-failure source queue should remain open after stale failed resolve loser, got resolution=%q resolved_by=%q", currentQueue.Resolution, derefString(currentQueue.ResolvedBy))
	}
	if currentQueue.UpdatedAt == sourceQueueBefore.UpdatedAt {
		t.Fatalf("winning manual edit should advance rollback-failure source queue updated_at, before=%q after=%q", sourceQueueBefore.UpdatedAt, currentQueue.UpdatedAt)
	}
	if currentQueue.Revision != sourceQueueBefore.Revision+1 {
		t.Fatalf("winning manual edit should advance rollback-failure source queue revision exactly once, before=%d after=%d", sourceQueueBefore.Revision, currentQueue.Revision)
	}
	if currentQueue.Summary != winnerSummary || currentQueue.Details != winnerDetails {
		t.Fatalf("rollback-failure source queue should keep winner-owned manual edit text, got summary=%q details=%q", currentQueue.Summary, currentQueue.Details)
	}
	if currentQueue.Urgency != "CRITICAL" || derefString(currentQueue.DueAt) != winnerDueAt {
		t.Fatalf("rollback-failure source queue should keep winner-owned urgency/due_at, got urgency=%q due_at=%q", currentQueue.Urgency, derefString(currentQueue.DueAt))
	}
	if currentQueue.EscalationCount != sourceQueueBefore.EscalationCount {
		t.Fatalf("winning manual edit should not change rollback-failure source queue escalation_count, before=%d after=%d", sourceQueueBefore.EscalationCount, currentQueue.EscalationCount)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure source queue payload after stale failed resolve loser: %v", err)
	}
	if payload.FollowupActionID != actionID || payload.FollowupActionQueueKey != "action:"+actionID || payload.FollowupActionStatus != humanActionStatusPending {
		t.Fatalf("rollback-failure source queue should keep active pending followup truth after stale failed resolve loser = %+v", payload)
	}
	if payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("rollback-failure stale failed resolve loser should not mint failed lineage, got %+v", payload)
	}

	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue after interleaving manual edit winner): %v", err)
	}
	if currentActionQueue.Status != "OPEN" || currentActionQueue.AssignedTo != actionQueueBefore.AssignedTo || currentActionQueue.UpdatedAt != actionQueueBefore.UpdatedAt {
		t.Fatalf("action queue mutated after stale rollback-failure failed resolve loser: before=%+v after=%+v", actionQueueBefore, currentActionQueue)
	}
	if currentActionQueue.Revision != actionQueueBefore.Revision {
		t.Fatalf("action queue revision changed after stale rollback-failure failed resolve loser: before=%d after=%d", actionQueueBefore.Revision, currentActionQueue.Revision)
	}
	if strings.TrimSpace(currentActionQueue.Resolution) != "" || derefString(currentActionQueue.ResolvedBy) != "" {
		t.Fatalf("action queue should remain open after stale rollback-failure failed resolve loser, got resolution=%q resolved_by=%q", currentActionQueue.Resolution, derefString(currentActionQueue.ResolvedBy))
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed rollback-failure interleaving failed actionResolve loser should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving workspace.ops.upsert winner should append exactly one rollback-failure source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("failed rollback-failure interleaving failed actionResolve loser should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving workspace.ops.upsert winner should not append rollback-failure action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("failed rollback-failure interleaving failed actionResolve loser should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestActionResolveFailedRejectsInterleavingActionQueueRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-resolve-action-queue-interleaving-conflict"
		taskID      = "task-action-rollback-failed-resolve-action-queue-interleaving-conflict"
		agentID     = "agent-action-rollback-failed-resolve-action-queue-interleaving-conflict"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-failed-resolve-action-queue-interleaving-conflict"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-resolve-action-queue-interleaving-conflict")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, actionQueue.QueueID, "failed-resolve-action-queue-cas-loser")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved action queue revision did not produce updated_at")
		}
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Should lose failed resolve to interleaving action queue write.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure failed actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil {
		t.Fatal("expected failed actionResolve to fail on interleaving action queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on failed action-queue actionResolve, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving action-queue failed resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.ResolvedAt != "" || action.ResolutionComment != "" || action.ResolvedBy != "" {
		t.Fatalf("rollback-failure action mutated after rejected action-queue failed resolve: %+v", action)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("failed action-queue failed resolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed action-queue failed resolve should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("failed action-queue failed resolve should only keep the winner's action-queue update, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestActionResolveFailedKeepsLinkedRebaseFollowupQueueOpenForRetry(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-failed-retry"
		taskID      = "task-action-rebase-failed-retry"
		agentID     = "agent-action-rebase-failed-retry"
		queueKey    = "tension_rebase_followup:tens-repair-failed-retry"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-failed-retry",
		"fork_tension_id":     "tens-fork-failed-retry",
		"repair_tension_id":   "tens-repair-failed-retry",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-failed-retry",
		Details:           "Coalition ID: coal-failed-retry\nRepair tension: tens-repair-failed-retry\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-failed-retry",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Starting rebase before verifier late fail.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "FAILED",
		Comment:    "Verifier late fail requires retry.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	resolveAny, rpcErr := h.actionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	resolveResp, ok := resolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionResolve response type %T", resolveAny)
	}
	if got, _ := resolveResp["status"].(string); got != humanActionStatusFailed {
		t.Fatalf("actionResolve response status = %q, want %q", got, humanActionStatusFailed)
	}
	if got, _ := resolveResp["workflow_state"].(string); got != rebaseWorkflowStateClaimed {
		t.Fatalf("actionResolve response workflow_state = %q, want %q", got, rebaseWorkflowStateClaimed)
	}
	if got, _ := resolveResp["workflow_step"].(string); got != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("actionResolve response workflow_step = %q, want %q", got, rebaseWorkflowStepAwaitRestart)
	}
	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	resolvePayload := decodeEventPayloadMap(t, resolveRuntime.PayloadJSON)
	if got, _ := resolvePayload["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("resolve runtime source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}
	if got, _ := resolvePayload["source_queue_key"].(string); got != sourceQueue.QueueKey {
		t.Fatalf("resolve runtime source_queue_key = %q, want %q", got, sourceQueue.QueueKey)
	}
	if got, _ := resolvePayload["workflow_state"].(string); got != rebaseWorkflowStateClaimed {
		t.Fatalf("resolve runtime workflow_state = %q, want %q", got, rebaseWorkflowStateClaimed)
	}
	if got, _ := resolvePayload["workflow_step"].(string); got != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("resolve runtime workflow_step = %q, want %q", got, rebaseWorkflowStepAwaitRestart)
	}
	if got, _ := resolvePayload["rollback_reason"].(string); got != "linked_action_failed" {
		t.Fatalf("resolve runtime rollback_reason = %q, want linked_action_failed", got)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q", action.Status, humanActionStatusFailed)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.Status != "RESOLVED" {
		t.Fatalf("action queue status = %q, want RESOLVED", actionQueue.Status)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated source queue: %v", err)
	}
	if updatedSourceQueue.Status != "OPEN" {
		t.Fatalf("source queue status = %q, want OPEN", updatedSourceQueue.Status)
	}
	if strings.Contains(updatedSourceQueue.PayloadJSON, "\"action_id\":\""+actionID+"\"") {
		t.Fatalf("source queue payload should clear active action link after failed resolution: %s", updatedSourceQueue.PayloadJSON)
	}
	payload, err := actionCreateDecodeQueuePayload(updatedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated source queue payload: %v", err)
	}
	if payload.LastFailedActionID != actionID {
		t.Fatalf("last_failed_action_id = %q, want %q", payload.LastFailedActionID, actionID)
	}
	if payload.LastFailedStatus != humanActionStatusFailed {
		t.Fatalf("last_failed_status = %q, want %q", payload.LastFailedStatus, humanActionStatusFailed)
	}
	if payload.RollbackReason != "linked_action_failed" {
		t.Fatalf("rollback_reason = %q, want linked_action_failed", payload.RollbackReason)
	}
	if payload.RebaseWorkflowState != rebaseWorkflowStateClaimed {
		t.Fatalf("payload workflow_state = %q, want %q", payload.RebaseWorkflowState, rebaseWorkflowStateClaimed)
	}
	if payload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("payload workflow_step = %q, want %q", payload.RebaseWorkflowStep, rebaseWorkflowStepAwaitRestart)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim status after failed rebase resolve: %v", err)
	}
	if claimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED while retry remains open", claimStatus)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryResp, ok := retryAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryAny)
	}
	retryActionID, ok := retryResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == actionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryResp)
	}
}

func TestActionResolveFailedRebaseFollowupUsesCurrentQueueEventLineageAfterMetadataOnlyUpdate(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-failed-lineage-refresh"
		taskID      = "task-action-rebase-failed-lineage-refresh"
		agentID     = "agent-action-rebase-failed-lineage-refresh"
		queueKey    = "tension_rebase_followup:tens-repair-failed-lineage-refresh"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-lineage-refresh")

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before metadata-only lineage refresh.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get current rebase source queue: %v", err)
	}
	refreshedQueue := advanceOpenOperatorQueueRevision(t, ctx, store, currentQueue, "failed-lineage-refresh")
	freshQueueUpdate := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Verifier late fail after metadata-only queue refresh.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	var actionParentRefs []string
	if err := json.Unmarshal([]byte(resolveRuntime.ParentRefsJSON), &actionParentRefs); err != nil {
		t.Fatalf("decode action.resolved parent refs: %v", err)
	}
	if len(actionParentRefs) == 0 || !strings.Contains(strings.Join(actionParentRefs, ","), strings.TrimSpace(freshQueueUpdate.EventID)) {
		t.Fatalf("expected action.resolved parent refs to include fresh metadata-only queue event %q, got %+v", freshQueueUpdate.EventID, actionParentRefs)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated source queue: %v", err)
	}
	if updatedSourceQueue.UpdatedAt == currentQueue.UpdatedAt {
		t.Fatalf("expected failed resolve to advance source queue revision beyond pre-refresh snapshot, current=%q latest=%q", currentQueue.UpdatedAt, updatedSourceQueue.UpdatedAt)
	}
	if !strings.Contains(updatedSourceQueue.Summary, "failed-lineage-refresh") {
		t.Fatalf("expected metadata-only summary marker to survive failed resolve, got %q", updatedSourceQueue.Summary)
	}
	payload, err := actionCreateDecodeQueuePayload(updatedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated source queue payload: %v", err)
	}
	if payload.LastFailedActionID != actionID {
		t.Fatalf("last_failed_action_id = %q, want %q", payload.LastFailedActionID, actionID)
	}
	if len(payload.ParentRefsJSON) == 0 || !strings.Contains(strings.Join(payload.ParentRefsJSON, ","), strings.TrimSpace(freshQueueUpdate.EventID)) {
		t.Fatalf("expected failed resolve payload lineage to include fresh metadata-only queue event %q, got %+v", freshQueueUpdate.EventID, payload.ParentRefsJSON)
	}
	if refreshedQueue.UpdatedAt == updatedSourceQueue.UpdatedAt {
		t.Fatalf("expected failed resolve to write a new source queue revision after metadata-only refresh, refreshed=%q latest=%q", refreshedQueue.UpdatedAt, updatedSourceQueue.UpdatedAt)
	}
}

func TestActionResolveRejectsSecondTerminalAttemptAfterPromotedRebaseRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-failed-second-resolve"
		taskID      = "task-action-rebase-failed-second-resolve"
		agentID     = "agent-action-rebase-failed-second-resolve"
		queueKey    = "tension_rebase_followup:tens-repair-failed-second-resolve"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-failed-second-resolve",
		"fork_tension_id":     "tens-fork-failed-second-resolve",
		"repair_tension_id":   "tens-repair-failed-second-resolve",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for failed second resolve guard",
		Details:           "Coalition ID: coal-failed-second-resolve\nRepair tension: tens-repair-failed-second-resolve\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-failed-second-resolve",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Starting before verifier late fail.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	firstResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "FAILED",
		Comment:    "Verifier late fail requires retry.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal first actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, firstResolveRaw); rpcErr != nil {
		t.Fatalf("first actionResolve rpc error: %+v", rpcErr)
	}

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	secondResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Late conflicting completion should be rejected.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal second actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, secondResolveRaw); rpcErr == nil {
		t.Fatal("expected second terminal actionResolve attempt to fail after rollback")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on second rollback resolve, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get source queue after rejected second rollback resolve: %v", err)
	}
	if currentSourceQueue.Status != "OPEN" {
		t.Fatalf("source queue status after rejected second rollback resolve = %q, want OPEN", currentSourceQueue.Status)
	}
	payload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload after rejected second rollback resolve: %v", err)
	}
	if payload.LastFailedActionID != actionID || payload.RebaseWorkflowState != rebaseWorkflowStateClaimed || payload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("source queue payload changed after rejected second rollback resolve: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("second rollback resolve should not append action.resolved rows, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("second rollback resolve should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionCreateLinksRollbackFailureQueueToHumanAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-link"
		taskID      = "task-action-rollback-link"
		agentID     = "agent-action-rollback-link"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-link"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-link")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	if got, _ := createResp["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}

	linkedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked rollback failure queue: %v", err)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(linkedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode linked rollback failure payload: %v", err)
	}
	if got := actionCreateQueuePayloadString(payload, "followup_action_id"); got != actionID {
		t.Fatalf("followup_action_id = %q, want %q", got, actionID)
	}
	if got := actionCreateQueuePayloadString(payload, "followup_action_queue_key"); got != "action:"+actionID {
		t.Fatalf("followup_action_queue_key = %q, want action:%s", got, actionID)
	}
	if got := actionCreateQueuePayloadString(payload, "followup_action_status"); got != humanActionStatusPending {
		t.Fatalf("followup_action_status = %q, want %q", got, humanActionStatusPending)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.Status != "OPEN" {
		t.Fatalf("action queue status = %q, want OPEN", actionQueue.Status)
	}

	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "already linked to action") {
		t.Fatalf("expected duplicate rollback failure actionCreate to fail with invalid params, got %+v", rpcErr)
	}
}

func TestActionCreateRejectsInterleavingWinnerOnRollbackFailureFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-create-interleaving-winner"
		taskID      = "task-action-rollback-create-interleaving-winner"
		agentID     = "agent-action-rollback-create-interleaving-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-create-interleaving-winner"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-create-interleaving-winner")

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionCreate params: %v", err)
	}

	var (
		hookErr        error
		winnerActionID string
	)
	h.beforeActionCreateQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionCreateQueueEffectsOverride = nil
		createAny, rpcErr := h.actionCreate(ctx, createRaw)
		if rpcErr != nil {
			hookErr = fmt.Errorf("interleaving rollback-failure actionCreate rpc error: %+v", rpcErr)
			return
		}
		createResp, ok := createAny.(map[string]any)
		if !ok {
			hookErr = fmt.Errorf("unexpected interleaving rollback-failure actionCreate response type %T", createAny)
			return
		}
		winnerActionID, _ = createResp["action_id"].(string)
		if strings.TrimSpace(winnerActionID) == "" {
			hookErr = fmt.Errorf("unexpected interleaving rollback-failure actionCreate response %+v", createResp)
		}
	}

	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil {
		t.Fatal("expected outer rollback-failure actionCreate to fail after interleaving winner linked the follow-up queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "already linked to action") {
		t.Fatalf("expected duplicate rollback-failure create to fail with already linked guidance, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving rollback-failure create hook: %v", hookErr)
	}
	if winnerActionID == "" {
		t.Fatalf("expected interleaving rollback-failure winner action id")
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated)+1 {
		t.Fatalf("interleaving rollback-failure create should append exactly one action.created row, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving rollback-failure create should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after interleaving rollback-failure create: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected exactly one human action after interleaving rollback-failure create, got %+v", actions)
	}

	linkedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked rollback failure queue: %v", err)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(linkedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode linked rollback failure payload after interleaving create: %v", err)
	}
	if got := actionCreateQueuePayloadString(payload, "followup_action_id"); got != winnerActionID {
		t.Fatalf("followup_action_id after interleaving create = %q, want %q", got, winnerActionID)
	}
	if got := actionCreateQueuePayloadString(payload, "followup_action_queue_key"); got != "action:"+winnerActionID {
		t.Fatalf("followup_action_queue_key after interleaving create = %q, want action:%s", got, winnerActionID)
	}
	if got := actionCreateQueuePayloadString(payload, "followup_action_status"); got != humanActionStatusPending {
		t.Fatalf("followup_action_status after interleaving create = %q, want %q", got, humanActionStatusPending)
	}
}

func TestActionCreateRejectsTaskOrAgentMismatchForLinkedRollbackFailureQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-create-context-guard"
		taskID      = "task-action-rollback-create-context-guard"
		agentID     = "agent-action-rollback-create-context-guard"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-create-context-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-create-context-guard")

	testCases := []struct {
		name        string
		params      actionCreateParams
		wantMessage string
	}{
		{
			name: "task mismatch",
			params: actionCreateParams{
				WorkspaceID: workspaceID,
				QueueID:     sourceQueue.QueueID,
				TaskID:      "task-action-rollback-create-context-other",
			},
			wantMessage: "source queue belongs to task " + taskID,
		},
		{
			name: "agent mismatch",
			params: actionCreateParams{
				WorkspaceID: workspaceID,
				QueueID:     sourceQueue.QueueID,
				AgentID:     "agent-action-rollback-create-context-other",
			},
			wantMessage: "source queue belongs to agent " + agentID,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actionCreatedFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "action.created",
				EntityType:  "human_action",
				Limit:       10,
			}
			sourceUpdatedFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "operator_queue.updated",
				EntityType:  "operator_queue",
				EntityID:    sourceQueue.QueueID,
				Limit:       10,
			}
			seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
			seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

			createRaw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal rollback-failure actionCreate params: %v", err)
			}
			if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, tc.wantMessage) {
				t.Fatalf("expected linked rollback-failure context mismatch on actionCreate, got %+v", rpcErr)
			}

			if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated) {
				t.Fatalf("rejected linked rollback-failure create should not append action.created rows, before=%v after=%v", seenActionCreated, got)
			}
			if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
				t.Fatalf("rejected linked rollback-failure create should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
			}

			actions, err := store.ListHumanActions(ctx, workspaceID, "")
			if err != nil {
				t.Fatalf("list human actions after rejected rollback-failure create: %v", err)
			}
			if len(actions) != 0 {
				t.Fatalf("expected no human action after rejected linked rollback-failure create, got %+v", actions)
			}

			currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
			if err != nil {
				t.Fatalf("get source queue after rejected linked rollback-failure create: %v", err)
			}
			if currentSourceQueue.UpdatedAt != sourceQueue.UpdatedAt {
				t.Fatalf("source queue updated_at after rejected linked rollback-failure create = %q, want %q", currentSourceQueue.UpdatedAt, sourceQueue.UpdatedAt)
			}
			if currentSourceQueue.PayloadJSON != sourceQueue.PayloadJSON {
				t.Fatalf("source queue payload changed after rejected linked rollback-failure create: %s", currentSourceQueue.PayloadJSON)
			}
		})
	}
}

func TestActionResolveResolvesLinkedRollbackFailureQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-resolve"
		taskID      = "task-action-rollback-resolve"
		agentID     = "agent-action-rollback-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-resolve"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-resolve")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Rollback repair complete.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	resolveAny, rpcErr := h.actionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	resolveResp, ok := resolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionResolve response type %T", resolveAny)
	}
	if got, _ := resolveResp["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}
	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	resolvePayload := decodeEventPayloadMap(t, resolveRuntime.PayloadJSON)
	if got, _ := resolvePayload["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("resolve runtime source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}
	if got, _ := resolvePayload["source_queue_key"].(string); got != sourceQueue.QueueKey {
		t.Fatalf("resolve runtime source_queue_key = %q, want %q", got, sourceQueue.QueueKey)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.Status != "RESOLVED" {
		t.Fatalf("action queue status = %q, want RESOLVED", actionQueue.Status)
	}

	resolvedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved rollback failure queue: %v", err)
	}
	if resolvedQueue.Status != "RESOLVED" {
		t.Fatalf("rollback failure queue status = %q, want RESOLVED", resolvedQueue.Status)
	}
	if resolvedQueue.Resolution != "followup_action_completed:"+actionID {
		t.Fatalf("rollback failure queue resolution = %q, want %q", resolvedQueue.Resolution, "followup_action_completed:"+actionID)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(resolvedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode resolved rollback failure payload: %v", err)
	}
	if payload.FollowupActionID != actionID {
		t.Fatalf("followup_action_id = %q, want %q", payload.FollowupActionID, actionID)
	}
	if payload.FollowupActionStatus != humanActionStatusCompleted {
		t.Fatalf("followup_action_status = %q, want %q", payload.FollowupActionStatus, humanActionStatusCompleted)
	}
}

func TestActionResolveRejectsNonHolderForLinkedRollbackFailureQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-resolve-holder-guard"
		taskID      = "task-action-rollback-resolve-holder-guard"
		agentID     = "agent-action-rollback-resolve-holder-guard"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-resolve-holder-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-resolve-holder-guard")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Trying to resolve rollback recovery without holder authority.",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-a") {
		t.Fatalf("expected holder mismatch on rollback-failure actionResolve, got %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("rollback-failure action status = %q, want %q", action.Status, humanActionStatusPending)
	}
}

func TestActionResolveFailedKeepsLinkedRollbackFailureQueueOpenForRetry(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-retry"
		taskID      = "task-action-rollback-failed-retry"
		agentID     = "agent-action-rollback-failed-retry"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-failed-retry"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-failed-retry")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Retry after rollback failure investigation.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	resolveAny, rpcErr := h.actionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	resolveResp, ok := resolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionResolve response type %T", resolveAny)
	}
	if got, _ := resolveResp["status"].(string); got != humanActionStatusFailed {
		t.Fatalf("actionResolve response status = %q, want %q", got, humanActionStatusFailed)
	}
	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	resolvePayload := decodeEventPayloadMap(t, resolveRuntime.PayloadJSON)
	if got, _ := resolvePayload["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("resolve runtime source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}
	if got, _ := resolvePayload["source_queue_key"].(string); got != sourceQueue.QueueKey {
		t.Fatalf("resolve runtime source_queue_key = %q, want %q", got, sourceQueue.QueueKey)
	}
	if got, _ := resolvePayload["rollback_reason"].(string); got != "followup_action_failed" {
		t.Fatalf("resolve runtime rollback_reason = %q, want followup_action_failed", got)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.Status != "RESOLVED" {
		t.Fatalf("action queue status = %q, want RESOLVED", actionQueue.Status)
	}

	updatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get rollback failure source queue: %v", err)
	}
	if updatedQueue.Status != "OPEN" {
		t.Fatalf("rollback failure queue status = %q, want OPEN", updatedQueue.Status)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(updatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback failure payload after failed resolve: %v", err)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" {
		t.Fatalf("active followup link should be cleared after failed resolve: %+v", payload)
	}
	if payload.LastFailedFollowupActionID != actionID {
		t.Fatalf("last_failed_followup_action_id = %q, want %q", payload.LastFailedFollowupActionID, actionID)
	}
	if payload.LastFailedFollowupActionStatus != humanActionStatusFailed {
		t.Fatalf("last_failed_followup_action_status = %q, want %q", payload.LastFailedFollowupActionStatus, humanActionStatusFailed)
	}

	retryAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryResp, ok := retryAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryAny)
	}
	retryActionID, ok := retryResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == actionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryResp)
	}

	retriedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get retried rollback failure queue: %v", err)
	}
	retriedPayload, err := actionCreateDecodeRollbackFailurePayload(retriedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode retried rollback failure payload: %v", err)
	}
	if retriedPayload.FollowupActionID != retryActionID {
		t.Fatalf("retried followup_action_id = %q, want %q", retriedPayload.FollowupActionID, retryActionID)
	}
}

func TestActionCreateRollbackFailureRetryUsesCurrentQueueEventLineageAfterMetadataOnlyInterleavingUpdate(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-retry-metadata-lineage"
		taskID      = "task-action-rollback-retry-metadata-lineage"
		agentID     = "agent-action-rollback-retry-metadata-lineage"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-retry-metadata-lineage"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-retry-metadata-lineage")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Retry after metadata-only lineage interleaving.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	updatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get rollback failure source queue after failed resolve: %v", err)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(updatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback failure payload after failed resolve: %v", err)
	}
	if payload.LastFailedFollowupActionID != actionID {
		t.Fatalf("last_failed_followup_action_id = %q, want %q", payload.LastFailedFollowupActionID, actionID)
	}

	var hookErr error
	var hookUpdateEventID string
	h.beforeActionCreateQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionCreateQueueEffectsOverride = nil
		current, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
		if err != nil {
			hookErr = fmt.Errorf("GetOperatorQueueItem(%s): %w", sourceQueue.QueueID, err)
			return
		}
		_, event, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			QueueID:           current.QueueID,
			WorkspaceID:       current.WorkspaceID,
			QueueKey:          current.QueueKey,
			QueueType:         current.QueueType,
			Title:             current.Title,
			Summary:           current.Summary + " | metadata-only-revision",
			Details:           current.Details,
			PayloadJSON:       current.PayloadJSON,
			AssignedTo:        current.AssignedTo,
			Urgency:           current.Urgency,
			SourceKind:        current.SourceKind,
			SourceID:          current.SourceID,
			TaskID:            current.TaskID,
			SessionID:         current.SessionID,
			AgentID:           current.AgentID,
			KeepSessionActive: current.KeepSessionActive,
		})
		if err != nil {
			hookErr = fmt.Errorf("metadata-only source queue update: %w", err)
			return
		}
		hookUpdateEventID = strings.TrimSpace(event.EventID)
	}

	retryAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("metadata-only interleaving hook: %v", hookErr)
	}
	retryResp, ok := retryAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryAny)
	}
	retryActionID, ok := retryResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == actionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryResp)
	}
	if hookUpdateEventID == "" {
		t.Fatal("expected metadata-only interleaving hook to record queue update event id")
	}

	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    retryActionID,
		Limit:       1,
	})
	var actionParentRefs []string
	if err := json.Unmarshal([]byte(createRuntime.ParentRefsJSON), &actionParentRefs); err != nil {
		t.Fatalf("decode retry action.created parent refs: %v", err)
	}
	if len(actionParentRefs) == 0 || !strings.Contains(strings.Join(actionParentRefs, ","), hookUpdateEventID) {
		t.Fatalf("expected retry action.created parent refs to include fresh metadata-only queue event %q, got %+v", hookUpdateEventID, actionParentRefs)
	}

	retriedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get retried rollback failure queue: %v", err)
	}
	retriedPayload, err := actionCreateDecodeRollbackFailurePayload(retriedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode retried rollback failure payload: %v", err)
	}
	if len(retriedPayload.ParentRefsJSON) == 0 || !strings.Contains(strings.Join(retriedPayload.ParentRefsJSON, ","), hookUpdateEventID) {
		t.Fatalf("expected retried rollback failure payload to include fresh metadata-only queue event %q, got %+v", hookUpdateEventID, retriedPayload.ParentRefsJSON)
	}
}

func TestActionResolveFailedRollbackFailureUsesCurrentQueueEventLineageAfterMetadataOnlyUpdate(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-lineage-refresh"
		taskID      = "task-action-rollback-failed-lineage-refresh"
		agentID     = "agent-action-rollback-failed-lineage-refresh"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-failed-lineage-refresh"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "failed-lineage-refresh")

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get current rollback-failure queue: %v", err)
	}
	refreshedQueue := advanceOpenOperatorQueueRevision(t, ctx, store, currentQueue, "rollback-failed-lineage-refresh")
	freshQueueUpdate := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "Retry after metadata-only rollback queue refresh.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	var actionParentRefs []string
	if err := json.Unmarshal([]byte(resolveRuntime.ParentRefsJSON), &actionParentRefs); err != nil {
		t.Fatalf("decode rollback-failure action.resolved parent refs: %v", err)
	}
	if len(actionParentRefs) == 0 || !strings.Contains(strings.Join(actionParentRefs, ","), strings.TrimSpace(freshQueueUpdate.EventID)) {
		t.Fatalf("expected rollback-failure action.resolved parent refs to include fresh metadata-only queue event %q, got %+v", freshQueueUpdate.EventID, actionParentRefs)
	}

	updatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated rollback-failure queue: %v", err)
	}
	if updatedQueue.UpdatedAt == currentQueue.UpdatedAt {
		t.Fatalf("expected failed resolve to advance rollback-failure queue revision beyond pre-refresh snapshot, current=%q latest=%q", currentQueue.UpdatedAt, updatedQueue.UpdatedAt)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(updatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated rollback-failure payload: %v", err)
	}
	if payload.LastFailedFollowupActionID != actionID {
		t.Fatalf("last_failed_followup_action_id = %q, want %q", payload.LastFailedFollowupActionID, actionID)
	}
	if len(payload.ParentRefsJSON) == 0 || !strings.Contains(strings.Join(payload.ParentRefsJSON, ","), strings.TrimSpace(freshQueueUpdate.EventID)) {
		t.Fatalf("expected rollback-failure failed resolve payload lineage to include fresh metadata-only queue event %q, got %+v", freshQueueUpdate.EventID, payload.ParentRefsJSON)
	}
	if refreshedQueue.UpdatedAt == updatedQueue.UpdatedAt {
		t.Fatalf("expected rollback-failure failed resolve to write a new queue revision after metadata-only refresh, refreshed=%q latest=%q", refreshedQueue.UpdatedAt, updatedQueue.UpdatedAt)
	}
}

func TestActionCreateRollbackFailureQueueRehydratesCurrentQueueRevisionBeforeLinking(t *testing.T) {
	store := newServerTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-create-stale-revision"
		taskID      = "task-action-rollback-create-stale-revision"
		agentID     = "agent-action-rollback-create-stale-revision"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-create-stale-revision"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-create-stale-revision")

	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get stale rollback failure queue: %v", err)
	}
	stalePayload, err := actionCreateDecodeRollbackFailurePayload(staleQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode stale rollback failure payload: %v", err)
	}
	_ = advanceOpenOperatorQueueRevision(t, ctx, store, staleQueue, "stale-create-guard")

	created, err := store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Need rollback stale create guard",
		Blocking:    true,
	}, &staleQueue, &stalePayload)
	if err != nil {
		t.Fatalf("CreateHumanActionWithRollbackFailureQueueEffects: %v", err)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one persisted action after rollback-failure create rehydrate, got %+v", actions)
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get latest rollback failure queue: %v", err)
	}
	if latestQueue.UpdatedAt == staleQueue.UpdatedAt {
		t.Fatalf("expected linked rollback-failure queue updated_at to advance beyond stale snapshot, stale=%q latest=%q", staleQueue.UpdatedAt, latestQueue.UpdatedAt)
	}
	if !strings.Contains(latestQueue.Summary, "stale-create-guard") {
		t.Fatalf("expected linked rollback-failure queue summary to preserve refreshed marker, got %q", latestQueue.Summary)
	}
	latestPayload, err := actionCreateDecodeRollbackFailurePayload(latestQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode latest rollback failure payload: %v", err)
	}
	if latestPayload.FollowupActionID != created.Action.ActionID {
		t.Fatalf("followup_action_id = %q, want %q after rollback-failure create rehydrate", latestPayload.FollowupActionID, created.Action.ActionID)
	}
	if latestPayload.LastFailedFollowupActionID != "" {
		t.Fatalf("rollback-failure create rehydrate should not create failed lineage, got %+v", latestPayload)
	}
}

func TestActionResolveCompletedRollbackFailureQueueRejectsStaleQueueRevision(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-resolve-stale-revision"
		taskID      = "task-action-rollback-resolve-stale-revision"
		agentID     = "agent-action-rollback-resolve-stale-revision"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-resolve-stale-revision"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-resolve-stale-revision")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get stale rollback failure queue: %v", err)
	}
	stalePayload, err := actionCreateDecodeRollbackFailurePayload(staleQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode stale rollback failure payload: %v", err)
	}
	refreshedQueue := advanceOpenOperatorQueueRevision(t, ctx, store, staleQueue, "stale-completed-guard")

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		actionID,
		humanActionStatusCompleted,
		"Rollback failure repaired.",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "Rollback failure repaired.",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentUpdatedAt: strings.TrimSpace(actionQueue.UpdatedAt),
		},
		[]sqlite.OperatorQueueResolveInput{
			linkedRollbackFailureSourceQueueResolutionInput(staleQueue, stalePayload, action, "reviewer-a", humanActionStatusCompleted),
		},
		nil,
		nil,
		action,
	)
	if err == nil {
		t.Fatalf("expected stale rollback-failure completed resolve snapshot to fail, got %+v", resolved)
	}
	if !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale revision guard, got %v", err)
	}

	pendingAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get pending action after rollback: %v", err)
	}
	if pendingAction.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after stale rollback-failure resolve rejection", pendingAction.Status, humanActionStatusPending)
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get latest rollback failure queue: %v", err)
	}
	if latestQueue.UpdatedAt != refreshedQueue.UpdatedAt {
		t.Fatalf("latest rollback queue updated_at = %q, want %q", latestQueue.UpdatedAt, refreshedQueue.UpdatedAt)
	}
	latestPayload, err := actionCreateDecodeRollbackFailurePayload(latestQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode latest rollback failure payload: %v", err)
	}
	if latestPayload.FollowupActionID != actionID {
		t.Fatalf("followup_action_id = %q, want %q after stale completed resolve rejection", latestPayload.FollowupActionID, actionID)
	}
	if latestPayload.LastFailedFollowupActionID != "" {
		t.Fatalf("stale completed resolve should not create failed lineage, got %+v", latestPayload)
	}
}

func TestActionResolveFailedRollbackFailureQueueRejectsStaleQueueRevision(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-stale-revision"
		taskID      = "task-action-rollback-failed-stale-revision"
		agentID     = "agent-action-rollback-failed-stale-revision"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-failed-stale-revision"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-failed-stale-revision")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get stale rollback failure queue: %v", err)
	}
	stalePayload, err := actionCreateDecodeRollbackFailurePayload(staleQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode stale rollback failure payload: %v", err)
	}
	refreshedQueue := advanceOpenOperatorQueueRevision(t, ctx, store, staleQueue, "stale-failed-guard")

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		actionID,
		humanActionStatusFailed,
		"Retry after stale queue conflict.",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "Retry after stale queue conflict.",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentUpdatedAt: strings.TrimSpace(actionQueue.UpdatedAt),
		},
		nil,
		[]sqlite.OperatorQueueUpsertInput{
			linkedRollbackFailureSourceQueueFailureUpsertInput(staleQueue, stalePayload, action, "reviewer-a", "Retry after stale queue conflict.", rebaseRuntimeLineage{}),
		},
		nil,
		action,
	)
	if err == nil {
		t.Fatalf("expected stale rollback-failure failed resolve snapshot to fail, got %+v", resolved)
	}
	if !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale revision guard, got %v", err)
	}

	pendingAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get pending action after rollback: %v", err)
	}
	if pendingAction.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after stale rollback-failure failed resolve rejection", pendingAction.Status, humanActionStatusPending)
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get latest rollback failure queue: %v", err)
	}
	if latestQueue.UpdatedAt != refreshedQueue.UpdatedAt {
		t.Fatalf("latest rollback queue updated_at = %q, want %q", latestQueue.UpdatedAt, refreshedQueue.UpdatedAt)
	}
	latestPayload, err := actionCreateDecodeRollbackFailurePayload(latestQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode latest rollback failure payload: %v", err)
	}
	if latestPayload.FollowupActionID != actionID {
		t.Fatalf("followup_action_id = %q, want %q after stale failed resolve rejection", latestPayload.FollowupActionID, actionID)
	}
	if latestPayload.LastFailedFollowupActionID != "" || latestPayload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("stale failed resolve should not drop active link or add failed lineage, got %+v", latestPayload)
	}
}

func TestActionResolveFailedRollbackFailureQueuePreservesCurrentLineageAgainstStalePreferredPayload(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-lineage-basis"
		taskID      = "task-action-rollback-failed-lineage-basis"
		agentID     = "agent-action-rollback-failed-lineage-basis"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-rollback-failed-lineage-basis"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-failed-lineage-basis")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get stale rollback failure queue: %v", err)
	}
	stalePayload, err := actionCreateDecodeRollbackFailurePayload(staleQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode stale rollback failure payload: %v", err)
	}
	stalePayload.RootCauseID = "root-stale-rollback-failed-lineage"
	stalePayload.ProvenanceGroupID = "prov-stale-rollback-failed-lineage"
	stalePayload.ParentRefsJSON = nil
	stalePayload.Normalize()

	queueCreateEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	})
	currentPayload, err := actionCreateDecodeRollbackFailurePayload(staleQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode current rollback failure payload: %v", err)
	}
	currentPayload.RootCauseID = "root-current-rollback-failed-lineage"
	currentPayload.ProvenanceGroupID = "prov-current-rollback-failed-lineage"
	currentPayload.ParentRefsJSON = []string{queueCreateEvent.EventID}
	currentPayload.Normalize()
	currentPayloadJSON, err := json.Marshal(currentPayload)
	if err != nil {
		t.Fatalf("marshal current rollback failure payload: %v", err)
	}
	currentQueue, currentQueueEvent, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             workspaceID,
		QueueID:                 staleQueue.QueueID,
		QueueKey:                staleQueue.QueueKey,
		QueueType:               staleQueue.QueueType,
		Title:                   staleQueue.Title,
		Summary:                 staleQueue.Summary,
		Details:                 staleQueue.Details,
		PayloadJSON:             string(currentPayloadJSON),
		AssignedTo:              staleQueue.AssignedTo,
		Urgency:                 staleQueue.Urgency,
		SourceKind:              staleQueue.SourceKind,
		SourceID:                staleQueue.SourceID,
		TaskID:                  staleQueue.TaskID,
		SessionID:               staleQueue.SessionID,
		AgentID:                 staleQueue.AgentID,
		KeepSessionActive:       staleQueue.KeepSessionActive,
		RequireCurrentStatus:    "OPEN",
		RequireCurrentUpdatedAt: strings.TrimSpace(staleQueue.UpdatedAt),
	})
	if err != nil {
		t.Fatalf("refresh rollback failure queue lineage basis: %v", err)
	}
	runtimeInput := actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		ActorType:   "operator",
		ActorID:     "reviewer-a",
		AgentID:     agentID,
		TaskID:      taskID,
		PayloadJSON: string(mustJSON(map[string]any{
			"action_id":       actionID,
			"resolution":      humanActionStatusFailed,
			"source_queue_id": currentQueue.QueueID,
		})),
	}, rebaseRollbackFailurePayloadLineage(stalePayload))

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		actionID,
		humanActionStatusFailed,
		"Retry after lineage basis guard.",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "Retry after lineage basis guard.",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentUpdatedAt: strings.TrimSpace(actionQueue.UpdatedAt),
		},
		nil,
		[]sqlite.OperatorQueueUpsertInput{
			linkedRollbackFailureSourceQueueFailureUpsertInput(currentQueue, stalePayload, action, "reviewer-a", "Retry after lineage basis guard.", rebaseRuntimeLineage{}),
		},
		&runtimeInput,
		action,
	)
	if err != nil {
		t.Fatalf("ResolveHumanActionWithQueueEffects: %v", err)
	}
	if resolved.ActionEvent == nil {
		t.Fatalf("expected action runtime event after failed resolve, got %+v", resolved)
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get latest rollback failure queue: %v", err)
	}
	latestPayload, err := actionCreateDecodeRollbackFailurePayload(latestQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode latest rollback failure payload: %v", err)
	}
	if latestPayload.RootCauseID != currentPayload.RootCauseID || latestPayload.ProvenanceGroupID != currentPayload.ProvenanceGroupID {
		t.Fatalf("failed resolve should preserve current rollback carrier root/provenance, got %+v", latestPayload)
	}
	if !reflect.DeepEqual(latestPayload.ParentRefsJSON, []string{currentQueueEvent.EventID}) {
		t.Fatalf("failed resolve should preserve current rollback carrier event-based parent refs, got %+v want %+v", latestPayload.ParentRefsJSON, []string{currentQueueEvent.EventID})
	}
	if latestPayload.LastFailedFollowupActionID != actionID || latestPayload.LastFailedFollowupActionStatus != humanActionStatusFailed {
		t.Fatalf("failed resolve should still stamp failed followup lineage, got %+v", latestPayload)
	}
	if resolved.ActionEvent.RootCauseID != currentPayload.RootCauseID || resolved.ActionEvent.ProvenanceGroupID != currentPayload.ProvenanceGroupID {
		t.Fatalf("action.resolved runtime lineage should use current rollback carrier basis, got %+v", resolved.ActionEvent)
	}
	if strings.TrimSpace(resolved.ActionEvent.ParentRefsJSON) == "" {
		t.Fatalf("action.resolved runtime event should keep parent refs after failed resolve, got %+v", resolved.ActionEvent)
	}
}

func TestActionResolveFailedLinkedRebaseQueueRejectsUnlinkedQueueRollbackUpsert(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID       = "ws-action-rebase-failed-unlinked-queue"
		taskID            = "task-action-rebase-failed-unlinked-queue"
		agentID           = "agent-action-rebase-failed-unlinked-queue"
		linkedQueueKey    = model.RebaseFollowupQueueKeyPrefix + "linked-rebase-failed-unlinked-queue"
		unrelatedQueueKey = model.RebaseFollowupQueueKeyPrefix + "unrelated-rebase-failed-unlinked-queue"
		idSuffix          = "rebase-failed-unlinked-queue"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, linkedQueueKey, idSuffix)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	unrelatedPayloadJSON, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-unrelated-" + idSuffix,
		"fork_tension_id":     "tens-fork-unrelated-" + idSuffix,
		"repair_tension_id":   "tens-repair-unrelated-" + idSuffix,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal unrelated queue payload: %v", err)
	}
	unrelatedQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          unrelatedQueueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt unrelated bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for unrelated path",
		Details:           "Coalition ID: unrelated\nRepair tension: unrelated\nNext action: attempt_rebase",
		PayloadJSON:       string(unrelatedPayloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-unrelated-" + idSuffix,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create unrelated rebase follow-up queue: %v", err)
	}
	unrelatedPayload, err := actionCreateDecodeQueuePayload(unrelatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode unrelated rebase follow-up payload: %v", err)
	}

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		actionID,
		humanActionStatusFailed,
		"Reject unrelated rollback carrier.",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "Reject unrelated rollback carrier.",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentUpdatedAt: strings.TrimSpace(actionQueue.UpdatedAt),
		},
		nil,
		[]sqlite.OperatorQueueUpsertInput{
			linkedActionSourceQueueFailureRollbackUpsertInput(unrelatedQueue, unrelatedPayload, action, "reviewer-a", humanActionStatusFailed, "Reject unrelated rollback carrier.", "", rebaseRuntimeLineage{}),
		},
		nil,
		action,
	)
	if err == nil {
		t.Fatalf("expected unlinked rebase failed resolve to fail, got %+v", resolved)
	}
	if !strings.Contains(err.Error(), "not linked to action") {
		t.Fatalf("expected unlinked rebase rollback guard, got %v", err)
	}

	pendingAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get pending action after rejected failed resolve: %v", err)
	}
	if pendingAction.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected failed resolve", pendingAction.Status, humanActionStatusPending)
	}

	latestSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked source queue after rejected failed resolve: %v", err)
	}
	latestSourcePayload, err := actionCreateDecodeQueuePayload(latestSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode linked source queue payload: %v", err)
	}
	if latestSourcePayload.ActionID != actionID {
		t.Fatalf("linked source queue action_id = %q, want %q after rejected failed resolve", latestSourcePayload.ActionID, actionID)
	}
	if latestSourcePayload.LastFailedActionID != "" {
		t.Fatalf("linked source queue should not record failed lineage after rejected resolve, got %+v", latestSourcePayload)
	}

	latestUnrelatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, unrelatedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get unrelated source queue after rejected failed resolve: %v", err)
	}
	if latestUnrelatedQueue.UpdatedAt != unrelatedQueue.UpdatedAt {
		t.Fatalf("unrelated queue updated_at = %q, want %q after rejected failed resolve", latestUnrelatedQueue.UpdatedAt, unrelatedQueue.UpdatedAt)
	}
	latestUnrelatedPayload, err := actionCreateDecodeQueuePayload(latestUnrelatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode unrelated queue payload after rejected failed resolve: %v", err)
	}
	if latestUnrelatedPayload.ActionID != "" || latestUnrelatedPayload.LastFailedActionID != "" {
		t.Fatalf("unrelated queue should remain unlinked after rejected failed resolve, got %+v", latestUnrelatedPayload)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim status after rejected failed resolve: %v", err)
	}
	if claimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED while pending blocking action remains", claimStatus)
	}
}

func TestActionResolveFailedRollbackFailureQueueRejectsUnlinkedQueueRollbackUpsert(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID       = "ws-action-rollback-failed-unlinked-queue"
		taskID            = "task-action-rollback-failed-unlinked-queue"
		agentID           = "agent-action-rollback-failed-unlinked-queue"
		linkedQueueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "linked-rollback-failed-unlinked-queue"
		unrelatedQueueKey = model.RebaseRollbackFailureQueueKeyPrefix + "unrelated-rollback-failed-unlinked-queue"
		idSuffix          = "rollback-failed-unlinked-queue"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, linkedQueueKey, idSuffix)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	unrelatedQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, unrelatedQueueKey, "tens-repair-unrelated-"+idSuffix)
	unrelatedPayload, err := actionCreateDecodeRollbackFailurePayload(unrelatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode unrelated rollback-failure payload: %v", err)
	}

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		actionID,
		humanActionStatusFailed,
		"Reject unrelated rollback-failure carrier.",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "Reject unrelated rollback-failure carrier.",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentUpdatedAt: strings.TrimSpace(actionQueue.UpdatedAt),
		},
		nil,
		[]sqlite.OperatorQueueUpsertInput{
			linkedRollbackFailureSourceQueueFailureUpsertInput(unrelatedQueue, unrelatedPayload, action, "reviewer-a", "Reject unrelated rollback-failure carrier.", rebaseRuntimeLineage{}),
		},
		nil,
		action,
	)
	if err == nil {
		t.Fatalf("expected unlinked rollback-failure failed resolve to fail, got %+v", resolved)
	}
	if !strings.Contains(err.Error(), "not linked to action") {
		t.Fatalf("expected unlinked rollback-failure guard, got %v", err)
	}

	pendingAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get pending action after rejected rollback-failure resolve: %v", err)
	}
	if pendingAction.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected rollback-failure resolve", pendingAction.Status, humanActionStatusPending)
	}

	latestSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked rollback-failure queue after rejected failed resolve: %v", err)
	}
	latestSourcePayload, err := actionCreateDecodeRollbackFailurePayload(latestSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode linked rollback-failure payload: %v", err)
	}
	if latestSourcePayload.FollowupActionID != actionID {
		t.Fatalf("linked rollback-failure queue followup_action_id = %q, want %q after rejected failed resolve", latestSourcePayload.FollowupActionID, actionID)
	}
	if latestSourcePayload.LastFailedFollowupActionID != "" {
		t.Fatalf("linked rollback-failure queue should not record failed lineage after rejected resolve, got %+v", latestSourcePayload)
	}

	latestUnrelatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, unrelatedQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get unrelated rollback-failure queue after rejected failed resolve: %v", err)
	}
	if latestUnrelatedQueue.UpdatedAt != unrelatedQueue.UpdatedAt {
		t.Fatalf("unrelated rollback-failure queue updated_at = %q, want %q after rejected failed resolve", latestUnrelatedQueue.UpdatedAt, unrelatedQueue.UpdatedAt)
	}
	latestUnrelatedPayload, err := actionCreateDecodeRollbackFailurePayload(latestUnrelatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode unrelated rollback-failure payload after rejected failed resolve: %v", err)
	}
	if latestUnrelatedPayload.FollowupActionID != "" || latestUnrelatedPayload.LastFailedFollowupActionID != "" {
		t.Fatalf("unrelated rollback-failure queue should remain unlinked after rejected failed resolve, got %+v", latestUnrelatedPayload)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim status after rejected rollback-failure resolve: %v", err)
	}
	if claimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED while pending blocking action remains", claimStatus)
	}
}

func TestActionResolveFailedLinkedRebaseQueueIgnoresMalformedRollbackPayloadAndUsesCanonicalState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-failed-malformed-payload"
		taskID      = "task-action-rebase-failed-malformed-payload"
		agentID     = "agent-action-rebase-failed-malformed-payload"
		queueKey    = model.RebaseFollowupQueueKeyPrefix + "linked-rebase-failed-malformed-payload"
		idSuffix    = "rebase-failed-malformed-payload"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, idSuffix)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked source queue: %v", err)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode linked source payload: %v", err)
	}
	badUpsert := linkedActionSourceQueueFailureRollbackUpsertInput(currentSourceQueue, sourcePayload, action, "reviewer-a", humanActionStatusFailed, "Reject malformed rollback payload.", "", rebaseRuntimeLineage{})
	badPayload, err := actionCreateDecodeQueuePayload(badUpsert.PayloadJSON)
	if err != nil {
		t.Fatalf("decode failed rollback upsert payload: %v", err)
	}
	badPayload.LastFailedActionID = ""
	badPayloadJSON, err := json.Marshal(badPayload)
	if err != nil {
		t.Fatalf("marshal malformed failed rollback payload: %v", err)
	}
	badUpsert.PayloadJSON = string(badPayloadJSON)

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		actionID,
		humanActionStatusFailed,
		"Reject malformed rollback payload.",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "Reject malformed rollback payload.",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentUpdatedAt: strings.TrimSpace(actionQueue.UpdatedAt),
		},
		nil,
		[]sqlite.OperatorQueueUpsertInput{badUpsert},
		nil,
		action,
	)
	if err != nil {
		t.Fatalf("malformed linked rebase failed resolve should be canonicalized in store: %v", err)
	}
	if resolved.Action.Status != humanActionStatusFailed {
		t.Fatalf("resolved action status = %q, want %q", resolved.Action.Status, humanActionStatusFailed)
	}
	resolvedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked source queue after canonicalized failed resolve: %v", err)
	}
	if resolvedSourceQueue.UpdatedAt == currentSourceQueue.UpdatedAt {
		t.Fatalf("expected canonicalized failed resolve to advance linked source queue revision, before=%q after=%q", currentSourceQueue.UpdatedAt, resolvedSourceQueue.UpdatedAt)
	}
	resolvedSourcePayload, err := actionCreateDecodeQueuePayload(resolvedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode canonicalized linked source payload: %v", err)
	}
	if resolvedSourcePayload.ActionID != "" {
		t.Fatalf("canonicalized linked source payload action_id = %q, want cleared action link", resolvedSourcePayload.ActionID)
	}
	if resolvedSourcePayload.LastFailedActionID != actionID {
		t.Fatalf("canonicalized linked source payload last_failed_action_id = %q, want %q", resolvedSourcePayload.LastFailedActionID, actionID)
	}
	if resolvedSourcePayload.RollbackReason != "linked_action_failed" {
		t.Fatalf("canonicalized linked source payload rollback_reason = %q, want linked_action_failed", resolvedSourcePayload.RollbackReason)
	}
	if resolvedSourcePayload.RebaseWorkflowState != rebaseWorkflowStateClaimed {
		t.Fatalf("canonicalized linked source payload workflow_state = %q, want %q", resolvedSourcePayload.RebaseWorkflowState, rebaseWorkflowStateClaimed)
	}
	if resolvedSourcePayload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("canonicalized linked source payload workflow_step = %q, want %q", resolvedSourcePayload.RebaseWorkflowStep, rebaseWorkflowStepAwaitRestart)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim status after canonicalized failed resolve: %v", err)
	}
	if claimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED after canonicalized failed resolve", claimStatus)
	}
}

func TestActionResolveFailedRollbackFailureQueueIgnoresMalformedRollbackPayloadAndUsesCanonicalState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-failed-malformed-payload"
		taskID      = "task-action-rollback-failed-malformed-payload"
		agentID     = "agent-action-rollback-failed-malformed-payload"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "linked-rollback-failed-malformed-payload"
		idSuffix    = "rollback-failed-malformed-payload"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, idSuffix)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked rollback-failure queue: %v", err)
	}
	sourcePayload, err := actionCreateDecodeRollbackFailurePayload(currentSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode linked rollback-failure payload: %v", err)
	}
	badUpsert := linkedRollbackFailureSourceQueueFailureUpsertInput(currentSourceQueue, sourcePayload, action, "reviewer-a", "Reject malformed rollback-failure payload.", rebaseRuntimeLineage{})
	badPayload, err := actionCreateDecodeRollbackFailurePayload(badUpsert.PayloadJSON)
	if err != nil {
		t.Fatalf("decode failed rollback-failure upsert payload: %v", err)
	}
	badPayload.LastFailedFollowupActionID = ""
	badPayloadJSON, err := json.Marshal(badPayload)
	if err != nil {
		t.Fatalf("marshal malformed rollback-failure payload: %v", err)
	}
	badUpsert.PayloadJSON = string(badPayloadJSON)

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		actionID,
		humanActionStatusFailed,
		"Reject malformed rollback-failure payload.",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 actionQueue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "Reject malformed rollback-failure payload.",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentUpdatedAt: strings.TrimSpace(actionQueue.UpdatedAt),
		},
		nil,
		[]sqlite.OperatorQueueUpsertInput{badUpsert},
		nil,
		action,
	)
	if err != nil {
		t.Fatalf("malformed linked rollback-failure failed resolve should be canonicalized in store: %v", err)
	}
	if resolved.Action.Status != humanActionStatusFailed {
		t.Fatalf("resolved action status = %q, want %q", resolved.Action.Status, humanActionStatusFailed)
	}
	resolvedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get linked rollback-failure queue after canonicalized failed resolve: %v", err)
	}
	if resolvedSourceQueue.UpdatedAt == currentSourceQueue.UpdatedAt {
		t.Fatalf("expected canonicalized failed rollback-failure resolve to advance queue revision, before=%q after=%q", currentSourceQueue.UpdatedAt, resolvedSourceQueue.UpdatedAt)
	}
	resolvedSourcePayload, err := actionCreateDecodeRollbackFailurePayload(resolvedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode canonicalized rollback-failure payload: %v", err)
	}
	if resolvedSourcePayload.FollowupActionID != "" {
		t.Fatalf("canonicalized rollback-failure payload followup_action_id = %q, want cleared active link", resolvedSourcePayload.FollowupActionID)
	}
	if resolvedSourcePayload.LastFailedFollowupActionID != actionID {
		t.Fatalf("canonicalized rollback-failure payload last_failed_followup_action_id = %q, want %q", resolvedSourcePayload.LastFailedFollowupActionID, actionID)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim status after canonicalized rollback-failure resolve: %v", err)
	}
	if claimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED after canonicalized rollback-failure resolve", claimStatus)
	}
}

func TestActionCreateRejectsQueueOnlyRollbackFailureQueueWithoutTaskContext(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rollback-queue-only"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-rollback-queue-only"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	sourceQueue := createQueueOnlyRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, queueKey, "entity-rollback-queue-only")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	if _, rpcErr := h.actionCreate(ctx, createRaw); rpcErr == nil {
		t.Fatal("expected queue-only rollback-failure promotion to fail")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "queue-only rollback-failure") || !strings.Contains(rpcErr.Message, "workspace.ops.resolve") {
		t.Fatalf("expected explicit queue-only rollback-failure guidance, got %+v", rpcErr)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after rejected queue-only rollback-failure promotion: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("expected no human action to be created, got %+v", actions)
	}
}

func TestSyncLinkedActionSourceQueueStartWithActionEventRollsBackOnRuntimeEventFailure(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-rollback"
		taskID      = "task-action-rebase-start-rollback"
		agentID     = "agent-action-rebase-start-rollback"
		queueKey    = "tension_rebase_followup:tens-repair-start-rollback"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-start-rollback",
		"fork_tension_id":     "tens-fork-start-rollback",
		"repair_tension_id":   "tens-repair-start-rollback",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for rollback guard",
		Details:           "Coalition ID: coal-start-rollback\nRepair tension: tens-repair-start-rollback\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-start-rollback",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	claimedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(claimed): %v", err)
	}
	claimedPayload, err := actionCreateDecodeQueuePayload(claimedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(claimed): %v", err)
	}

	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       10,
	}
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	seenActionEvents := snapshotRuntimeEventIDs(t, ctx, store, actionFilter)

	startRuntimeInput := actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		ActorType:   "operator",
		ActorID:     "reviewer-a",
		AgentID:     agentID,
		TaskID:      taskID + "-missing",
		PayloadJSON: string(mustJSON(map[string]any{
			"action_id":       actionID,
			"source_queue_id": sourceQueue.QueueID,
			"workflow_state":  "in_progress",
			"workflow_step":   "operator_claimed",
		})),
	}, rebaseFollowupPayloadLineage(claimedPayload))
	if _, err := h.syncLinkedActionSourceQueueStartWithActionEvent(ctx, claimedQueue, nil, claimedPayload, action, "reviewer-a", "Taking first rebase pass.", startRuntimeInput); err == nil {
		t.Fatal("expected runtime append failure to abort linked start queue sync")
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest): %v", err)
	}
	if latestQueue.UpdatedAt != claimedQueue.UpdatedAt {
		t.Fatalf("queue updated_at = %q, want %q after rollback", latestQueue.UpdatedAt, claimedQueue.UpdatedAt)
	}
	if latestQueue.PayloadJSON != claimedQueue.PayloadJSON || latestQueue.Summary != claimedQueue.Summary || latestQueue.Details != claimedQueue.Details || latestQueue.AssignedTo != claimedQueue.AssignedTo {
		t.Fatalf("expected claimed queue to stay unchanged after rollback, before=%+v after=%+v", claimedQueue, latestQueue)
	}
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, queueFilter, seenQueueEvents)
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, actionFilter, seenActionEvents)
}

func TestSyncLinkedActionSourceQueuePauseWithActionEventRollsBackOnRuntimeEventFailure(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-rollback"
		taskID      = "task-action-rebase-pause-rollback"
		agentID     = "agent-action-rebase-pause-rollback"
		queueKey    = "tension_rebase_followup:tens-repair-pause-rollback"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-pause-rollback",
		"fork_tension_id":     "tens-fork-pause-rollback",
		"repair_tension_id":   "tens-repair-pause-rollback",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for pause rollback guard",
		Details:           "Coalition ID: coal-pause-rollback\nRepair tension: tens-repair-pause-rollback\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-pause-rollback",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Taking first rebase pass.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	startedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(started): %v", err)
	}
	startedPayload, err := actionCreateDecodeQueuePayload(startedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(started): %v", err)
	}

	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       10,
	}
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	seenActionEvents := snapshotRuntimeEventIDs(t, ctx, store, actionFilter)

	pauseRuntimeInput := actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		ActorType:   "operator",
		ActorID:     "reviewer-a",
		AgentID:     agentID,
		TaskID:      taskID + "-missing",
		PayloadJSON: string(mustJSON(map[string]any{
			"action_id":       actionID,
			"source_queue_id": sourceQueue.QueueID,
			"workflow_state":  "claimed",
			"workflow_step":   "await_action_restart",
		})),
	}, rebaseFollowupPayloadLineage(startedPayload))
	if _, err := h.syncLinkedActionSourceQueuePauseWithActionEvent(ctx, startedQueue, nil, startedPayload, action, "reviewer-a", "Pause for fresh overlap evidence.", pauseRuntimeInput); err == nil {
		t.Fatal("expected runtime append failure to abort linked pause queue sync")
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest): %v", err)
	}
	if latestQueue.UpdatedAt != startedQueue.UpdatedAt {
		t.Fatalf("queue updated_at = %q, want %q after rollback", latestQueue.UpdatedAt, startedQueue.UpdatedAt)
	}
	if latestQueue.PayloadJSON != startedQueue.PayloadJSON || latestQueue.Summary != startedQueue.Summary || latestQueue.Details != startedQueue.Details || latestQueue.AssignedTo != startedQueue.AssignedTo {
		t.Fatalf("expected started queue to stay unchanged after rollback, before=%+v after=%+v", startedQueue, latestQueue)
	}
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, queueFilter, seenQueueEvents)
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, actionFilter, seenActionEvents)
}

func TestSyncLinkedActionSourceQueuePauseRejectsResolvedAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-resolved-action-guard"
		taskID      = "task-action-rebase-pause-resolved-action-guard"
		agentID     = "agent-action-rebase-pause-resolved-action-guard"
		queueKey    = "tension_rebase_followup:tens-repair-pause-resolved-action-guard"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-resolved-action-guard")
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before direct resolve guard.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	startedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(started): %v", err)
	}
	startedPayload, err := actionCreateDecodeQueuePayload(startedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(started): %v", err)
	}

	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	seenActionEvents := snapshotRuntimeEventIDs(t, ctx, store, actionFilter)

	if err := store.ResolveHumanAction(ctx, actionID, humanActionStatusCompleted, "resolved before pause sync", "reviewer-b", action); err != nil {
		t.Fatalf("ResolveHumanAction: %v", err)
	}

	pauseRuntimeInput := actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		ActorType:   "operator",
		ActorID:     "reviewer-a",
		AgentID:     agentID,
		TaskID:      taskID,
		PayloadJSON: string(mustJSON(map[string]any{
			"action_id":       actionID,
			"source_queue_id": sourceQueue.QueueID,
			"workflow_state":  linkedActionSourceQueuePausedState(),
			"workflow_step":   linkedActionSourceQueuePausedStep(),
		})),
	}, rebaseFollowupPayloadLineage(startedPayload))
	if _, err := h.syncLinkedActionSourceQueuePauseWithActionEvent(ctx, startedQueue, nil, startedPayload, action, "reviewer-a", "pause", pauseRuntimeInput); err == nil {
		t.Fatal("expected syncLinkedActionSourceQueuePauseWithActionEvent to reject resolved action")
	} else if !strings.Contains(strings.ToLower(err.Error()), "already resolved") {
		t.Fatalf("syncLinkedActionSourceQueuePauseWithActionEvent error = %v, want already resolved guard", err)
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest): %v", err)
	}
	if latestQueue.UpdatedAt != startedQueue.UpdatedAt {
		t.Fatalf("queue updated_at = %q, want %q after rejected resolved-action pause", latestQueue.UpdatedAt, startedQueue.UpdatedAt)
	}
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, queueFilter, seenQueueEvents)
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, actionFilter, seenActionEvents)
}

func TestResolveHumanActionWithQueueEffectsRollsBackWhenActionRuntimeEventAppendFails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-resolve-runtime-rollback"
		taskID      = "task-action-resolve-runtime-rollback"
		agentID     = "agent-action-resolve-runtime-rollback"
		queueKey    = "tension_rebase_followup:tens-repair-resolve-runtime-rollback"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-resolve-runtime-rollback",
		"fork_tension_id":     "tens-fork-resolve-runtime-rollback",
		"repair_tension_id":   "tens-repair-resolve-runtime-rollback",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for resolve rollback guard",
		Details:           "Coalition ID: coal-resolve-runtime-rollback\nRepair tension: tens-repair-resolve-runtime-rollback\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-resolve-runtime-rollback",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	claimedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(claimed): %v", err)
	}
	claimedPayload, err := actionCreateDecodeQueuePayload(claimedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(claimed): %v", err)
	}

	resolvedAction := action
	resolvedAction.Status = humanActionStatusCompleted
	resolvedAction.ResolvedBy = "reviewer-a"
	actionQueueInput := humanActionResolutionQueueInput(resolvedAction, &actionQueue, "reviewer-a", humanActionStatusCompleted, "Resolved after bounded rebase.")
	sourceQueueInputs := []sqlite.OperatorQueueResolveInput{
		linkedActionSourceQueueResolutionInput(claimedQueue, claimedPayload, resolvedAction, "reviewer-a", humanActionStatusCompleted),
	}
	resolveLineage := actionResolveLineageFromInputs([]actionCreateSourceQueue{{
		kind:    actionCreateSourceQueueKindRebaseFollowup,
		queue:   claimedQueue,
		payload: claimedPayload,
	}}, nil, nil, rebaseRuntimeLineage{})
	resolveRuntimeInput := actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		ActorType:   "operator",
		ActorID:     "reviewer-a",
		AgentID:     agentID,
		TaskID:      taskID + "-missing",
		PayloadJSON: string(mustJSON(map[string]any{
			"action_id":       actionID,
			"resolution":      humanActionStatusCompleted,
			"source_queue_id": sourceQueue.QueueID,
		})),
	}, resolveLineage)

	actionQueueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	sourceQueueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       10,
	}
	seenActionQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, actionQueueFilter)
	seenSourceQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sourceQueueFilter)
	seenActionEvents := snapshotRuntimeEventIDs(t, ctx, store, actionFilter)

	if _, err := store.ResolveHumanActionWithQueueEffects(ctx, actionID, humanActionStatusCompleted, "Resolved after bounded rebase.", "reviewer-a", &actionQueueInput, sourceQueueInputs, nil, &resolveRuntimeInput, action); err == nil {
		t.Fatal("expected runtime append failure to abort action resolve tx")
	}

	pendingAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(after): %v", err)
	}
	if pendingAction.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rollback", pendingAction.Status, humanActionStatusPending)
	}
	if pendingAction.ResolvedAt != "" || pendingAction.ResolvedBy != "" || pendingAction.ResolutionComment != "" {
		t.Fatalf("expected pending action without resolution metadata after rollback, got %+v", pendingAction)
	}

	latestActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue after): %v", err)
	}
	if latestActionQueue.Status != actionQueue.Status || latestActionQueue.UpdatedAt != actionQueue.UpdatedAt || latestActionQueue.PayloadJSON != actionQueue.PayloadJSON {
		t.Fatalf("expected action queue to stay unchanged after rollback, before=%+v after=%+v", actionQueue, latestActionQueue)
	}

	latestSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after): %v", err)
	}
	if latestSourceQueue.Status != claimedQueue.Status || latestSourceQueue.UpdatedAt != claimedQueue.UpdatedAt || latestSourceQueue.PayloadJSON != claimedQueue.PayloadJSON {
		t.Fatalf("expected source queue to stay unchanged after rollback, before=%+v after=%+v", claimedQueue, latestSourceQueue)
	}

	assertRuntimeEventSnapshotUnchanged(t, ctx, store, actionQueueFilter, seenActionQueueEvents)
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, sourceQueueFilter, seenSourceQueueEvents)
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, actionFilter, seenActionEvents)
}

func TestActionStartTransitionsLinkedRebaseFollowupQueueInProgress(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start"
		taskID      = "task-action-rebase-start"
		agentID     = "agent-action-rebase-start"
		queueKey    = "tension_rebase_followup:tens-repair-start"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-start",
		"fork_tension_id":     "tens-fork-start",
		"repair_tension_id":   "tens-repair-start",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-start",
		Details:           "Coalition ID: coal-start\nRepair tension: tens-repair-start\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-start",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Taking first rebase pass.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	startAny, rpcErr := h.actionStart(ctx, startRaw)
	if rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}
	startResp, ok := startAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionStart response type %T", startAny)
	}
	if got, _ := startResp["workflow_state"].(string); got != "in_progress" {
		t.Fatalf("workflow_state = %q, want in_progress", got)
	}
	if got, _ := startResp["workflow_step"].(string); got != "operator_claimed" {
		t.Fatalf("workflow_step = %q, want operator_claimed", got)
	}

	startLive, sourceQueueLive := nextActionStartedAndQueueUpdateForQueue(t, ch, sourceQueue.QueueID)
	startRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, startLive, startRuntime, "action.started")
	assertHumanActionRuntimePromptContext(t, startRuntime, "action.start", workspaceID, "system", "server_rpc")
	sourceQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, sourceQueueLive, sourceQueueRuntime, "workspace.ops.updated")
	if startRuntime.RootCauseID != sourceQueueRuntime.RootCauseID || startRuntime.ProvenanceGroupID != sourceQueueRuntime.ProvenanceGroupID {
		t.Fatalf("action.started lineage %+v does not match source queue event %+v", startRuntime, sourceQueueRuntime)
	}
	assertRuntimeEventParentRefsContain(t, startRuntime, sourceQueueRuntime.EventID)

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get started source queue: %v", err)
	}
	if updatedSourceQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("source queue assigned_to = %q, want reviewer-a", updatedSourceQueue.AssignedTo)
	}
	if !strings.Contains(updatedSourceQueue.Summary, "In progress") {
		t.Fatalf("source queue summary did not surface in-progress state: %q", updatedSourceQueue.Summary)
	}
	if !strings.Contains(updatedSourceQueue.Details, "Workflow state: in_progress") {
		t.Fatalf("source queue details did not surface workflow state: %q", updatedSourceQueue.Details)
	}
	if !strings.Contains(updatedSourceQueue.Details, "Workflow step: operator_claimed") {
		t.Fatalf("source queue details did not surface workflow step: %q", updatedSourceQueue.Details)
	}
	if !strings.Contains(updatedSourceQueue.Details, "Started by: reviewer-a") {
		t.Fatalf("source queue details did not surface started_by: %q", updatedSourceQueue.Details)
	}
	startedPayload := map[string]any{}
	if err := json.Unmarshal([]byte(updatedSourceQueue.PayloadJSON), &startedPayload); err != nil {
		t.Fatalf("decode started queue payload: %v", err)
	}
	if got := actionCreateQueuePayloadString(startedPayload, "rebase_workflow_state"); got != "in_progress" {
		t.Fatalf("started queue payload rebase_workflow_state = %q, want in_progress", got)
	}
	if got := actionCreateQueuePayloadString(startedPayload, "rebase_workflow_step"); got != "operator_claimed" {
		t.Fatalf("started queue payload rebase_workflow_step = %q, want operator_claimed", got)
	}
	if got := actionCreateQueuePayloadString(startedPayload, "action_started_by"); got != "reviewer-a" {
		t.Fatalf("started queue payload action_started_by = %q, want reviewer-a", got)
	}

	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "already in progress") {
		t.Fatalf("expected duplicate actionStart to fail with invalid params, got %+v", rpcErr)
	}
}

func TestActionStartRejectsNonHolderForLinkedRebaseFollowupQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-holder-guard"
		taskID      = "task-action-rebase-start-holder-guard"
		agentID     = "agent-action-rebase-start-holder-guard"
		queueKey    = "tension_rebase_followup:tens-repair-start-holder-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-start-holder-guard",
		"fork_tension_id":     "tens-fork-start-holder-guard",
		"repair_tension_id":   "tens-repair-start-holder-guard",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-start-holder-guard",
		Details:           "Coalition ID: coal-start-holder-guard\nRepair tension: tens-repair-start-holder-guard\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-start-holder-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create guarded rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-b",
		Comment:   "Trying to steal the linked rebase workflow.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-a") {
		t.Fatalf("expected holder mismatch on actionStart, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestActionStartRejectsResolvedLinkedRebaseFollowupQueueWithInvalidParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-resolved-source"
		taskID      = "task-action-rebase-start-resolved-source"
		agentID     = "agent-action-rebase-start-resolved-source"
		queueKey    = "tension_rebase_followup:tens-repair-start-resolved-source"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-start-resolved-source",
		"fork_tension_id":     "tens-fork-start-resolved-source",
		"repair_tension_id":   "tens-repair-start-resolved-source",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-start-resolved-source",
		Details:           "Coalition ID: coal-start-resolved-source\nRepair tension: tens-repair-start-resolved-source\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-start-resolved-source",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)

	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "precondition_closed",
	}); err != nil {
		t.Fatalf("resolve linked source queue precondition: %v", err)
	}

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should fail against a resolved linked source queue.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail when linked source queue is already resolved")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "not open") {
		t.Fatalf("expected invalid params not open on resolved linked source queue, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted) {
		t.Fatalf("failed actionStart should not append action.started rows, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed actionStart should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionStartRejectsInterleavingSourceQueueRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-interleaving-conflict"
		taskID      = "task-action-rebase-start-interleaving-conflict"
		agentID     = "agent-action-rebase-start-interleaving-conflict"
		queueKey    = "tension_rebase_followup:tens-repair-start-interleaving-conflict"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-start-interleaving-conflict",
		"fork_tension_id":     "tens-fork-start-interleaving-conflict",
		"repair_tension_id":   "tens-repair-start-interleaving-conflict",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for start interleaving conflict",
		Details:           "Coalition ID: coal-start-interleaving-conflict\nRepair tension: tens-repair-start-interleaving-conflict\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-start-interleaving-conflict",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionStartSyncOverride = func(ctx context.Context) {
		h.beforeActionStartSyncOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "start-cas-loser")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved queue revision did not produce updated_at")
		}
	}
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should lose to interleaving queue write.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail on interleaving source queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionStart, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving start hook: %v", hookErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted) {
		t.Fatalf("failed interleaving actionStart should not append action.started rows, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("failed interleaving actionStart should only keep the winner's queue update, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionStartRejectsInterleavingResolvedAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-interleaving-resolved"
		taskID      = "task-action-rebase-start-interleaving-resolved"
		agentID     = "agent-action-rebase-start-interleaving-resolved"
		queueKey    = "tension_rebase_followup:tens-repair-start-interleaving-resolved"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "start-interleaving-resolved")
	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	currentAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(before interleaving resolve): %v", err)
	}

	var hookErr error
	h.beforeActionStartSyncOverride = func(ctx context.Context) {
		h.beforeActionStartSyncOverride = nil
		hookErr = store.ResolveHumanAction(ctx, actionID, humanActionStatusCompleted, "Resolved before queue start sync.", "reviewer-b", currentAction)
	}
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should lose to interleaving action resolve.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail after interleaving action resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on interleaving actionStart, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusCompleted {
		t.Fatalf("action status = %q, want %q after rejected interleaving start", action.Status, humanActionStatusCompleted)
	}
	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest): %v", err)
	}
	if latestQueue.UpdatedAt != currentQueue.UpdatedAt {
		t.Fatalf("source queue updated_at = %q, want %q after rejected interleaving start", latestQueue.UpdatedAt, currentQueue.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted) {
		t.Fatalf("failed interleaving actionStart should not append action.started rows, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionStart should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionStartRejectsInterleavingHumanActionRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-interleaving-human-action-revision"
		taskID      = "task-action-rebase-start-interleaving-human-action-revision"
		agentID     = "agent-action-rebase-start-interleaving-human-action-revision"
		queueKey    = "tension_rebase_followup:tens-repair-start-interleaving-human-action-revision"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "start-interleaving-human-action-revision")
	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var interleaved sqlite.HumanActionRecord
	h.beforeActionStartSyncOverride = func(ctx context.Context) {
		h.beforeActionStartSyncOverride = nil
		interleaved = interleaveHumanActionRevisionForTest(t, ctx, store, actionID, "reviewer-b", "start-row-cas-loser")
	}
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should lose to interleaving human_action row drift.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail after interleaving human_action row drift")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionStart, got %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected interleaving start", action.Status, humanActionStatusPending)
	}
	if action.Revision != interleaved.Revision || action.AssignedTo != interleaved.AssignedTo {
		t.Fatalf("action should keep winner row drift after rejected start, got %+v want revision=%d assigned_to=%q", action, interleaved.Revision, interleaved.AssignedTo)
	}
	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest): %v", err)
	}
	if latestQueue.UpdatedAt != currentQueue.UpdatedAt {
		t.Fatalf("source queue updated_at = %q, want %q after rejected interleaving start", latestQueue.UpdatedAt, currentQueue.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted) {
		t.Fatalf("failed interleaving actionStart should not append action.started rows, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionStart should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestActionStartRejectsResolvedLinkedActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-resolved-action-queue"
		taskID      = "task-action-rebase-start-resolved-action-queue"
		agentID     = "agent-action-rebase-start-resolved-action-queue"
		queueKey    = "tension_rebase_followup:tens-repair-start-resolved-action-queue"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "start-resolved-action-queue")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     actionQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "closed_before_start",
	}); err != nil {
		t.Fatalf("resolve action queue precondition: %v", err)
	}

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should fail when linked action queue is already resolved.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail when linked action queue is already resolved")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "not open") {
		t.Fatalf("expected invalid params not open on resolved linked action queue, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted) {
		t.Fatalf("failed actionStart should not append action.started rows, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed actionStart should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionStartRejectsInterleavingActionQueueRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-action-queue-interleaving-conflict"
		taskID      = "task-action-rebase-start-action-queue-interleaving-conflict"
		agentID     = "agent-action-rebase-start-action-queue-interleaving-conflict"
		queueKey    = "tension_rebase_followup:tens-repair-start-action-queue-interleaving-conflict"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "start-action-queue-interleaving-conflict")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionStartSyncOverride = func(ctx context.Context) {
		h.beforeActionStartSyncOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, actionQueue.QueueID, "start-action-queue-cas-loser")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved action queue revision did not produce updated_at")
		}
	}
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should lose to interleaving action queue write.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail on interleaving action queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving action-queue actionStart, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving action-queue start hook: %v", hookErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted) {
		t.Fatalf("failed action-queue interleaving actionStart should not append action.started rows, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed action-queue interleaving actionStart should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("failed action-queue interleaving actionStart should only keep the winner's action-queue update, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestActionStartRejectsInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-interleaving-escalate"
		taskID      = "task-action-rebase-start-interleaving-escalate"
		agentID     = "agent-action-rebase-start-interleaving-escalate"
		queueKey    = "tension_rebase_followup:tens-repair-start-interleaving-escalate"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "start-interleaving-escalate")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionStartSyncOverride = func(ctx context.Context) {
		h.beforeActionStartSyncOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "start-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should lose to interleaving queue handoff.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionStart, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate start hook: %v", hookErr)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted) {
		t.Fatalf("failed interleaving actionStart should not append action.started rows, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionStart should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("interleaving queue handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("interleaving queue handoff should append exactly one linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	assertLinkedRebaseActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, "reviewer-b")
}

func TestActionStartRejectsInterleavingStartedWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-interleaving-started"
		taskID      = "task-action-rebase-start-interleaving-started"
		agentID     = "agent-action-rebase-start-interleaving-started"
		queueKey    = "tension_rebase_followup:tens-repair-start-interleaving-started"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "start-interleaving-started")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionStartedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionStarted := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionStartSyncOverride = func(ctx context.Context) {
		h.beforeActionStartSyncOverride = nil
		innerRaw, err := json.Marshal(actionStartParams{
			ActionID:  actionID,
			StartedBy: "reviewer-a",
			Comment:   "Concurrent start winner should claim the linked rebase carrier first.",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving actionStart params: %w", err)
			return
		}
		if _, rpcErr := h.actionStart(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving actionStart rpc error: %+v", rpcErr)
		}
	}
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Should lose to interleaving action.start winner.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr == nil {
		t.Fatal("expected actionStart to fail after interleaving action.start winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving action.start winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving action.start winner hook: %v", hookErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionStartedFilter); len(got) != len(seenActionStarted)+1 {
		t.Fatalf("interleaving action.start winner should append exactly one action.started row, before=%v after=%v", seenActionStarted, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving action.start winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving action.start winner should not append extra linked action queue update rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestSyncLinkedActionSourceQueueStartRejectsResolvedQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-guard"
		taskID      = "task-action-rebase-start-guard"
		agentID     = "agent-action-rebase-start-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queueRecord, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "tension_rebase_followup:guarded-start",
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase follow-up",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       `{"next_action":"attempt_rebase","task_id":"` + taskID + `","task_ids":["` + taskID + `"],"repair_tension_id":"tens-repair-guard","fork_tension_id":"tens-fork-guard","conflict_safe_class":"rebase_candidate","rebase_workflow_state":"claimed","rebase_workflow_step":"await_action_resolution"}`,
		SourceKind:        "tension",
		SourceID:          "tens-repair-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     queueRecord.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}

	staleQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, queueRecord.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem: %v", err)
	}
	stalePayload, err := actionCreateDecodeQueuePayload(staleQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload: %v", err)
	}

	if _, _, err := store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queueRecord.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "manual_guard",
	}); err != nil {
		t.Fatalf("ResolveOperatorQueueItemWithEvent: %v", err)
	}

	if _, _, err := h.syncLinkedActionSourceQueueStart(ctx, staleQueue, nil, stalePayload, action, "reviewer-a", "resume"); err == nil {
		t.Fatalf("expected syncLinkedActionSourceQueueStart to reject resolved queue")
	} else if !strings.Contains(err.Error(), "not open") {
		t.Fatalf("syncLinkedActionSourceQueueStart error = %v, want not open guard", err)
	}

	resolvedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, queueRecord.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(resolved): %v", err)
	}
	if resolvedQueue.Status != "RESOLVED" {
		t.Fatalf("resolved queue status = %q, want RESOLVED", resolvedQueue.Status)
	}
	if resolvedQueue.Resolution != "manual_guard" {
		t.Fatalf("resolved queue resolution = %q, want manual_guard", resolvedQueue.Resolution)
	}
}

func TestSyncLinkedActionSourceQueueStartRejectsResolvedAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-resolved-action-guard"
		taskID      = "task-action-rebase-start-resolved-action-guard"
		agentID     = "agent-action-rebase-start-resolved-action-guard"
		queueKey    = "tension_rebase_followup:tens-repair-start-resolved-action-guard"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "start-resolved-action-guard")
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	queueRecord, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem: %v", err)
	}
	queuePayload, err := actionCreateDecodeQueuePayload(queueRecord.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload: %v", err)
	}

	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	seenActionEvents := snapshotRuntimeEventIDs(t, ctx, store, actionFilter)

	if err := store.ResolveHumanAction(ctx, actionID, humanActionStatusCompleted, "resolved before sync", "reviewer-b", action); err != nil {
		t.Fatalf("ResolveHumanAction: %v", err)
	}

	startRuntimeInput := actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		ActorType:   "operator",
		ActorID:     "reviewer-a",
		AgentID:     agentID,
		TaskID:      taskID,
		PayloadJSON: string(mustJSON(map[string]any{
			"action_id":       actionID,
			"source_queue_id": sourceQueue.QueueID,
			"workflow_state":  rebaseWorkflowStateInProgress,
			"workflow_step":   rebaseWorkflowStepOperatorClaimed,
		})),
	}, rebaseFollowupPayloadLineage(queuePayload))
	if _, err := h.syncLinkedActionSourceQueueStartWithActionEvent(ctx, queueRecord, nil, queuePayload, action, "reviewer-a", "resume", startRuntimeInput); err == nil {
		t.Fatal("expected syncLinkedActionSourceQueueStartWithActionEvent to reject resolved action")
	} else if !strings.Contains(strings.ToLower(err.Error()), "already resolved") {
		t.Fatalf("syncLinkedActionSourceQueueStartWithActionEvent error = %v, want already resolved guard", err)
	}

	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest): %v", err)
	}
	if latestQueue.UpdatedAt != queueRecord.UpdatedAt {
		t.Fatalf("queue updated_at = %q, want %q after rejected resolved-action start", latestQueue.UpdatedAt, queueRecord.UpdatedAt)
	}
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, queueFilter, seenQueueEvents)
	assertRuntimeEventSnapshotUnchanged(t, ctx, store, actionFilter, seenActionEvents)
}

func TestSyncLinkedActionSourceQueueStartRejectsStaleQueueRevisionAfterRestart(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-start-stale-restart"
		taskID      = "task-action-rebase-start-stale-restart"
		agentID     = "agent-action-rebase-start-stale-restart"
		queueKey    = "tension_rebase_followup:tens-repair-start-stale-restart"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-start-stale-restart",
		"fork_tension_id":     "tens-fork-start-stale-restart",
		"repair_tension_id":   "tens-repair-start-stale-restart",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for stale restart guard",
		Details:           "Coalition ID: coal-start-stale-restart\nRepair tension: tens-repair-start-stale-restart\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-start-stale-restart",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}

	claimedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(claimed): %v", err)
	}
	claimedPayload, err := actionCreateDecodeQueuePayload(claimedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(claimed): %v", err)
	}

	startedQueue, _, err := h.syncLinkedActionSourceQueueStart(ctx, claimedQueue, nil, claimedPayload, action, "reviewer-a", "start once")
	if err != nil {
		t.Fatalf("syncLinkedActionSourceQueueStart(first): %v", err)
	}
	startedPayload, err := actionCreateDecodeQueuePayload(startedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(started): %v", err)
	}

	pausedQueue, _, err := h.syncLinkedActionSourceQueuePause(ctx, startedQueue, nil, startedPayload, action, "reviewer-a", "pause before restart")
	if err != nil {
		t.Fatalf("syncLinkedActionSourceQueuePause: %v", err)
	}
	pausedPayload, err := actionCreateDecodeQueuePayload(pausedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(paused): %v", err)
	}

	restartedQueue, _, err := h.syncLinkedActionSourceQueueStart(ctx, pausedQueue, nil, pausedPayload, action, "reviewer-a", "restart")
	if err != nil {
		t.Fatalf("syncLinkedActionSourceQueueStart(restart): %v", err)
	}
	if restartedQueue.UpdatedAt == pausedQueue.UpdatedAt {
		t.Fatalf("expected restart to advance queue updated_at, paused=%q restarted=%q", pausedQueue.UpdatedAt, restartedQueue.UpdatedAt)
	}

	if _, _, err := h.syncLinkedActionSourceQueueStart(ctx, pausedQueue, nil, pausedPayload, action, "reviewer-b", "stale restart"); err == nil {
		t.Fatal("expected stale restart snapshot to be rejected")
	} else if !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale revision guard, got %v", err)
	}
}

func TestSyncLinkedActionSourceQueuePauseRejectsStaleQueueRevisionAfterRestart(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-stale-restart"
		taskID      = "task-action-rebase-pause-stale-restart"
		agentID     = "agent-action-rebase-pause-stale-restart"
		queueKey    = "tension_rebase_followup:tens-repair-pause-stale-restart"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-pause-stale-restart",
		"fork_tension_id":     "tens-fork-pause-stale-restart",
		"repair_tension_id":   "tens-repair-pause-stale-restart",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for stale pause guard",
		Details:           "Coalition ID: coal-pause-stale-restart\nRepair tension: tens-repair-pause-stale-restart\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-pause-stale-restart",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}

	claimedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(claimed): %v", err)
	}
	claimedPayload, err := actionCreateDecodeQueuePayload(claimedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(claimed): %v", err)
	}

	startedQueue, _, err := h.syncLinkedActionSourceQueueStart(ctx, claimedQueue, nil, claimedPayload, action, "reviewer-a", "start once")
	if err != nil {
		t.Fatalf("syncLinkedActionSourceQueueStart(first): %v", err)
	}
	startedPayload, err := actionCreateDecodeQueuePayload(startedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(started): %v", err)
	}

	pausedQueue, _, err := h.syncLinkedActionSourceQueuePause(ctx, startedQueue, nil, startedPayload, action, "reviewer-a", "pause once")
	if err != nil {
		t.Fatalf("syncLinkedActionSourceQueuePause(first): %v", err)
	}
	pausedPayload, err := actionCreateDecodeQueuePayload(pausedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(paused): %v", err)
	}

	restartedQueue, _, err := h.syncLinkedActionSourceQueueStart(ctx, pausedQueue, nil, pausedPayload, action, "reviewer-a", "restart")
	if err != nil {
		t.Fatalf("syncLinkedActionSourceQueueStart(restart): %v", err)
	}
	if restartedQueue.UpdatedAt == startedQueue.UpdatedAt {
		t.Fatalf("expected restart to advance queue updated_at, started=%q restarted=%q", startedQueue.UpdatedAt, restartedQueue.UpdatedAt)
	}

	if _, _, err := h.syncLinkedActionSourceQueuePause(ctx, startedQueue, nil, startedPayload, action, "reviewer-b", "stale pause"); err == nil {
		t.Fatal("expected stale pause snapshot to be rejected")
	} else if !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale revision guard, got %v", err)
	}
}

func TestActionPauseReturnsLinkedRebaseFollowupQueueToClaimedState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause"
		taskID      = "task-action-rebase-pause"
		agentID     = "agent-action-rebase-pause"
		queueKey    = "tension_rebase_followup:tens-repair-pause"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-pause",
		"fork_tension_id":     "tens-fork-pause",
		"repair_tension_id":   "tens-repair-pause",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-pause",
		Details:           "Coalition ID: coal-pause\nRepair tension: tens-repair-pause\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-pause",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Taking first rebase pass.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Waiting on fresh overlap evidence.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	pauseAny, rpcErr := h.actionPause(ctx, pauseRaw)
	if rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}
	pauseResp, ok := pauseAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionPause response type %T", pauseAny)
	}
	if got, _ := pauseResp["workflow_state"].(string); got != "claimed" {
		t.Fatalf("workflow_state = %q, want claimed", got)
	}
	if got, _ := pauseResp["workflow_step"].(string); got != "await_action_restart" {
		t.Fatalf("workflow_step = %q, want await_action_restart", got)
	}

	pauseLive, sourceQueueLive := nextActionPausedAndQueueUpdateForQueue(t, ch, sourceQueue.QueueID)
	pauseRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, pauseLive, pauseRuntime, "action.paused")
	assertHumanActionRuntimePromptContext(t, pauseRuntime, "action.pause", workspaceID, "system", "server_rpc")
	sourceQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, sourceQueueLive, sourceQueueRuntime, "workspace.ops.updated")
	if pauseRuntime.RootCauseID != sourceQueueRuntime.RootCauseID || pauseRuntime.ProvenanceGroupID != sourceQueueRuntime.ProvenanceGroupID {
		t.Fatalf("action.paused lineage %+v does not match source queue event %+v", pauseRuntime, sourceQueueRuntime)
	}
	assertRuntimeEventParentRefsContain(t, pauseRuntime, sourceQueueRuntime.EventID)

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get paused source queue: %v", err)
	}
	if strings.Contains(updatedSourceQueue.Summary, "In progress") {
		t.Fatalf("source queue summary retained in-progress marker: %q", updatedSourceQueue.Summary)
	}
	if !strings.Contains(updatedSourceQueue.Summary, "Claimed") {
		t.Fatalf("source queue summary did not surface claimed marker: %q", updatedSourceQueue.Summary)
	}
	if !strings.Contains(updatedSourceQueue.Details, "Workflow state: claimed") {
		t.Fatalf("source queue details did not surface workflow state: %q", updatedSourceQueue.Details)
	}
	if !strings.Contains(updatedSourceQueue.Details, "Workflow step: await_action_restart") {
		t.Fatalf("source queue details did not surface workflow step: %q", updatedSourceQueue.Details)
	}
	if !strings.Contains(updatedSourceQueue.Details, "Paused by: reviewer-a") {
		t.Fatalf("source queue details did not surface paused_by: %q", updatedSourceQueue.Details)
	}
	if !strings.Contains(updatedSourceQueue.Details, "Pause comment: Waiting on fresh overlap evidence.") {
		t.Fatalf("source queue details did not surface pause comment: %q", updatedSourceQueue.Details)
	}
	pausedPayload := map[string]any{}
	if err := json.Unmarshal([]byte(updatedSourceQueue.PayloadJSON), &pausedPayload); err != nil {
		t.Fatalf("decode paused queue payload: %v", err)
	}
	if got := actionCreateQueuePayloadString(pausedPayload, "rebase_workflow_state"); got != "claimed" {
		t.Fatalf("paused queue payload rebase_workflow_state = %q, want claimed", got)
	}
	if got := actionCreateQueuePayloadString(pausedPayload, "rebase_workflow_step"); got != "await_action_restart" {
		t.Fatalf("paused queue payload rebase_workflow_step = %q, want await_action_restart", got)
	}
	if got := actionCreateQueuePayloadString(pausedPayload, "action_paused_by"); got != "reviewer-a" {
		t.Fatalf("paused queue payload action_paused_by = %q, want reviewer-a", got)
	}

	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "not in progress") {
		t.Fatalf("expected duplicate actionPause to fail with invalid params, got %+v", rpcErr)
	}
}

func TestActionPauseRejectsNonHolderForLinkedRebaseFollowupQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-holder-guard"
		taskID      = "task-action-rebase-pause-holder-guard"
		agentID     = "agent-action-rebase-pause-holder-guard"
		queueKey    = "tension_rebase_followup:tens-repair-pause-holder-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-pause-holder-guard",
		"fork_tension_id":     "tens-fork-pause-holder-guard",
		"repair_tension_id":   "tens-repair-pause-holder-guard",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-pause-holder-guard",
		Details:           "Coalition ID: coal-pause-holder-guard\nRepair tension: tens-repair-pause-holder-guard\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-pause-holder-guard",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create guarded rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Take the first rebase pass.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-b",
		Comment:  "Trying to pause without holding the workflow.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-a") {
		t.Fatalf("expected holder mismatch on actionPause, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionPauseRejectsResolvedLinkedRebaseFollowupQueueWithInvalidParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-resolved-source"
		taskID      = "task-action-rebase-pause-resolved-source"
		agentID     = "agent-action-rebase-pause-resolved-source"
		queueKey    = "tension_rebase_followup:tens-repair-pause-resolved-source"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-pause-resolved-source",
		"fork_tension_id":     "tens-fork-pause-resolved-source",
		"repair_tension_id":   "tens-repair-pause-resolved-source",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-pause-resolved-source",
		Details:           "Coalition ID: coal-pause-resolved-source\nRepair tension: tens-repair-pause-resolved-source\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-pause-resolved-source",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before forcing resolved precondition.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "precondition_closed",
	}); err != nil {
		t.Fatalf("resolve linked source queue precondition: %v", err)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should fail against a resolved linked source queue.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail when linked source queue is already resolved")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "not open") {
		t.Fatalf("expected invalid params not open on resolved linked source queue, got %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected pause", action.Status, humanActionStatusPending)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed actionPause should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionPauseRejectsInterleavingSourceQueueRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-interleaving-conflict"
		taskID      = "task-action-rebase-pause-interleaving-conflict"
		agentID     = "agent-action-rebase-pause-interleaving-conflict"
		queueKey    = "tension_rebase_followup:tens-repair-pause-interleaving-conflict"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-pause-interleaving-conflict",
		"fork_tension_id":     "tens-fork-pause-interleaving-conflict",
		"repair_tension_id":   "tens-repair-pause-interleaving-conflict",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for pause interleaving conflict",
		Details:           "Coalition ID: coal-pause-interleaving-conflict\nRepair tension: tens-repair-pause-interleaving-conflict\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-pause-interleaving-conflict",
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving pause conflict.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionPauseSyncOverride = func(ctx context.Context) {
		h.beforeActionPauseSyncOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "pause-cas-loser")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved queue revision did not produce updated_at")
		}
	}
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should lose to interleaving queue write.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail on interleaving source queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionPause, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving pause hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected interleaving pause", action.Status, humanActionStatusPending)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed interleaving actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("failed interleaving actionPause should only keep the winner's queue update, before=%v after=%v", seenSourceUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionPauseRejectsInterleavingResolvedAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-interleaving-resolved"
		taskID      = "task-action-rebase-pause-interleaving-resolved"
		agentID     = "agent-action-rebase-pause-interleaving-resolved"
		queueKey    = "tension_rebase_followup:tens-repair-pause-interleaving-resolved"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-interleaving-resolved")

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving direct resolve.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	currentAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(before interleaving resolve): %v", err)
	}

	var hookErr error
	h.beforeActionPauseSyncOverride = func(ctx context.Context) {
		h.beforeActionPauseSyncOverride = nil
		hookErr = store.ResolveHumanAction(ctx, actionID, humanActionStatusCompleted, "resolved before pause sync", "reviewer-b", currentAction)
	}
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should lose to interleaving direct resolve.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail after interleaving action resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") {
		t.Fatalf("expected invalid params already resolved on interleaving actionPause, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving resolve hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusCompleted {
		t.Fatalf("action status = %q, want %q after rejected interleaving pause", action.Status, humanActionStatusCompleted)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed interleaving actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionPause should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionPauseRejectsInterleavingHumanActionRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-interleaving-human-action-revision"
		taskID      = "task-action-rebase-pause-interleaving-human-action-revision"
		agentID     = "agent-action-rebase-pause-interleaving-human-action-revision"
		queueKey    = "tension_rebase_followup:tens-repair-pause-interleaving-human-action-revision"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-interleaving-human-action-revision")

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving human_action row drift.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}
	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var interleaved sqlite.HumanActionRecord
	h.beforeActionPauseSyncOverride = func(ctx context.Context) {
		h.beforeActionPauseSyncOverride = nil
		interleaved = interleaveHumanActionRevisionForTest(t, ctx, store, actionID, "reviewer-b", "pause-row-cas-loser")
	}
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should lose to interleaving human_action row drift.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail after interleaving human_action row drift")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionPause, got %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected interleaving pause", action.Status, humanActionStatusPending)
	}
	if action.Revision != interleaved.Revision || action.AssignedTo != interleaved.AssignedTo {
		t.Fatalf("action should keep winner row drift after rejected pause, got %+v want revision=%d assigned_to=%q", action, interleaved.Revision, interleaved.AssignedTo)
	}
	latestQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(latest): %v", err)
	}
	if latestQueue.UpdatedAt != currentQueue.UpdatedAt {
		t.Fatalf("source queue updated_at = %q, want %q after rejected interleaving pause", latestQueue.UpdatedAt, currentQueue.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed interleaving actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionPause should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestActionPauseRejectsResolvedLinkedActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-resolved-action-queue"
		taskID      = "task-action-rebase-pause-resolved-action-queue"
		agentID     = "agent-action-rebase-pause-resolved-action-queue"
		queueKey    = "tension_rebase_followup:tens-repair-pause-resolved-action-queue"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-resolved-action-queue")
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before resolved action-queue pause guard.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     actionQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "closed_before_pause",
	}); err != nil {
		t.Fatalf("resolve action queue precondition: %v", err)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should fail when linked action queue is already resolved.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail when linked action queue is already resolved")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "not open") {
		t.Fatalf("expected invalid params not open on resolved linked action queue, got %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected pause", action.Status, humanActionStatusPending)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed actionPause should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestActionPauseRejectsInterleavingFailedResolveWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-interleaving-failed-resolve"
		taskID      = "task-action-rebase-pause-interleaving-failed-resolve"
		agentID     = "agent-action-rebase-pause-interleaving-failed-resolve"
		queueKey    = "tension_rebase_followup:tens-repair-pause-interleaving-failed-resolve"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-interleaving-failed-resolve")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving failed resolve winner.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionPauseSyncOverride = func(ctx context.Context) {
		h.beforeActionPauseSyncOverride = nil
		innerRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "Concurrent failed resolve winner should beat stale pause.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving failed resolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving failed resolve rpc error: %+v", rpcErr)
		}
	}

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should lose to interleaving failed resolve winner.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail after interleaving failed resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || (!strings.Contains(strings.ToLower(rpcErr.Message), "already resolved") && !strings.Contains(strings.ToLower(rpcErr.Message), "not open") && !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently")) {
		t.Fatalf("expected fail-closed conflict on interleaving failed-resolve actionPause, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving failed resolve winner hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q after interleaving failed resolve winner", action.Status, humanActionStatusFailed)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed interleaving actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("interleaving failed resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving failed resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("interleaving failed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestActionPauseRejectsInterleavingActionQueueRevisionConflict(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-action-queue-interleaving-conflict"
		taskID      = "task-action-rebase-pause-action-queue-interleaving-conflict"
		agentID     = "agent-action-rebase-pause-action-queue-interleaving-conflict"
		queueKey    = "tension_rebase_followup:tens-repair-pause-action-queue-interleaving-conflict"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-action-queue-interleaving-conflict")
	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before action-queue interleaving pause conflict.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionPauseSyncOverride = func(ctx context.Context) {
		h.beforeActionPauseSyncOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, actionQueue.QueueID, "pause-action-queue-cas-loser")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved action queue revision did not produce updated_at")
		}
	}
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should lose to interleaving action queue write.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail on interleaving action queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving action-queue actionPause, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving action-queue pause hook: %v", hookErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after rejected action-queue pause", action.Status, humanActionStatusPending)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed action-queue interleaving actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed action-queue interleaving actionPause should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("failed action-queue interleaving actionPause should only keep the winner's action-queue update, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestActionPauseRejectsInterleavingEscalateWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-interleaving-escalate"
		taskID      = "task-action-rebase-pause-interleaving-escalate"
		agentID     = "agent-action-rebase-pause-interleaving-escalate"
		queueKey    = "tension_rebase_followup:tens-repair-pause-interleaving-escalate"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-interleaving-escalate")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving escalate handoff.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionPauseSyncOverride = func(ctx context.Context) {
		h.beforeActionPauseSyncOverride = nil
		if _, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "pause-handoff-cas-winner"); err != nil {
			hookErr = err
		}
	}
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should lose to interleaving queue handoff.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail on interleaving queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving actionPause, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate pause hook: %v", hookErr)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused) {
		t.Fatalf("failed interleaving actionPause should not append action.paused rows, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("failed interleaving actionPause should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("interleaving queue handoff should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("interleaving queue handoff should append exactly one linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	assertLinkedRebaseActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, "reviewer-b")
}

func TestActionPauseRejectsInterleavingPausedWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-rebase-pause-interleaving-paused"
		taskID      = "task-action-rebase-pause-interleaving-paused"
		agentID     = "agent-action-rebase-pause-interleaving-paused"
		queueKey    = "tension_rebase_followup:tens-repair-pause-interleaving-paused"
	)

	sourceQueue, actionID := createLinkedRebaseActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "pause-interleaving-paused")
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start before interleaving pause winner.",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       10,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionPauseSyncOverride = func(ctx context.Context) {
		h.beforeActionPauseSyncOverride = nil
		innerRaw, err := json.Marshal(actionPauseParams{
			ActionID: actionID,
			PausedBy: "reviewer-a",
			Comment:  "Concurrent pause winner should move the carrier into restart-needed state first.",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving actionPause params: %w", err)
			return
		}
		if _, rpcErr := h.actionPause(ctx, innerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving actionPause rpc error: %+v", rpcErr)
		}
	}
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Should lose to interleaving action.pause winner.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr == nil {
		t.Fatal("expected actionPause to fail after interleaving action.pause winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on interleaving action.pause winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving action.pause winner hook: %v", hookErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused)+1 {
		t.Fatalf("interleaving action.pause winner should append exactly one action.paused row, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving action.pause winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving action.pause winner should not append extra linked action queue update rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestBundleForkRebaseActionLifecycleBlackBox(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-blackbox"
		taskID      = "task-action-blackbox"
		agentID     = "agent-action-blackbox"
		tensionID   = "tension:bundle:blackbox"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	insertActionSourceTensionFixture(t, ctx, store, workspaceID, taskID, agentID, tensionID)

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "Task-linked bundle test")
	if err != nil {
		t.Fatalf("CreateCoalition: %v", err)
	}

	decision, err := store.EvaluateCoalitionBundle(ctx, workspaceID, coalition.CoalitionID, sqlite.BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}, "patch-ref-blackbox")
	if err != nil {
		t.Fatalf("EvaluateCoalitionBundle: %v", err)
	}
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if decision.NextAction != "attempt_rebase" {
		t.Fatalf("next action = %s, want attempt_rebase", decision.NextAction)
	}
	if decision.ConflictSafeClass != "rebase_candidate" {
		t.Fatalf("conflict-safe class = %s, want rebase_candidate", decision.ConflictSafeClass)
	}

	var forkTensionID, repairTensionID, repairTaskIDsJSON string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id, task_ids_json
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'repair'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&repairTensionID, &repairTaskIDsJSON); err != nil {
		t.Fatalf("query repair tension: %v", err)
	}
	if !strings.Contains(repairTaskIDsJSON, taskID) {
		t.Fatalf("repair tension task_ids_json = %s, want task %s", repairTaskIDsJSON, taskID)
	}
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'fork_candidate'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&forkTensionID); err != nil {
		t.Fatalf("query fork tension: %v", err)
	}

	sourceQueue := operatorQueueForSource(t, ctx, store, workspaceID, "tension", repairTensionID)
	if sourceQueue.TaskID != taskID {
		t.Fatalf("source queue task_id = %q, want %q", sourceQueue.TaskID, taskID)
	}
	if sourceQueue.AgentID != agentID {
		t.Fatalf("source queue agent_id = %q, want %q", sourceQueue.AgentID, agentID)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	if got, _ := createResp["status"].(string); got != humanActionStatusPending {
		t.Fatalf("black-box actionCreate status = %q, want %q", got, humanActionStatusPending)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
	createLive, sourceQueueLive := nextActionCreatedAndQueueUpdateForQueue(t, ch, sourceQueue.QueueID)
	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, createLive, createRuntime, "action.created")
	sourceQueueCreateRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, sourceQueueLive, sourceQueueCreateRuntime, "workspace.ops.updated")

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "First rebase pass.",
	})
	if err != nil {
		t.Fatalf("marshal first actionStart params: %v", err)
	}
	startAny, rpcErr := h.actionStart(ctx, startRaw)
	if rpcErr != nil {
		t.Fatalf("first actionStart rpc error: %+v", rpcErr)
	}
	startResp, ok := startAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first actionStart response type %T", startAny)
	}
	if got, _ := startResp["status"].(string); got != humanActionStatusPending {
		t.Fatalf("first actionStart status = %q, want %q", got, humanActionStatusPending)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	firstStartLive, firstStartQueueLive := nextActionStartedAndQueueUpdateForQueue(t, ch, sourceQueue.QueueID)
	firstStartRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, firstStartLive, firstStartRuntime, "action.started")
	firstStartQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, firstStartQueueLive, firstStartQueueRuntime, "workspace.ops.updated")

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Pause for fresh overlap evidence.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	pauseAny, rpcErr := h.actionPause(ctx, pauseRaw)
	if rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}
	pauseResp, ok := pauseAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionPause response type %T", pauseAny)
	}
	if got, _ := pauseResp["status"].(string); got != humanActionStatusPending {
		t.Fatalf("actionPause status = %q, want %q", got, humanActionStatusPending)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	pauseLive, pauseQueueLive := nextActionPausedAndQueueUpdateForQueue(t, ch, sourceQueue.QueueID)
	pauseRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, pauseLive, pauseRuntime, "action.paused")
	pauseQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, pauseQueueLive, pauseQueueRuntime, "workspace.ops.updated")

	restartRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Second rebase pass.",
	})
	if err != nil {
		t.Fatalf("marshal restart actionStart params: %v", err)
	}
	restartAny, rpcErr := h.actionStart(ctx, restartRaw)
	if rpcErr != nil {
		t.Fatalf("restart actionStart rpc error: %+v", rpcErr)
	}
	restartResp, ok := restartAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected restart actionStart response type %T", restartAny)
	}
	if got, _ := restartResp["status"].(string); got != humanActionStatusPending {
		t.Fatalf("restart actionStart status = %q, want %q", got, humanActionStatusPending)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	restartLive, restartQueueLive := nextActionStartedAndQueueUpdateForQueue(t, ch, sourceQueue.QueueID)
	restartRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, restartLive, restartRuntime, "action.started")
	restartQueueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, restartQueueLive, restartQueueRuntime, "workspace.ops.updated")

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: "COMPLETED",
		Comment:    "Rebase landed cleanly.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	resolveAny, rpcErr := h.actionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	resolveResp, ok := resolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionResolve response type %T", resolveAny)
	}
	if got, _ := resolveResp["status"].(string); got != humanActionStatusCompleted {
		t.Fatalf("black-box actionResolve status = %q, want %q", got, humanActionStatusCompleted)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	resolveLive, queueLives := nextActionResolvedAndQueueResolutionsForQueues(t, ch, actionQueue.QueueID, sourceQueue.QueueID)
	resolveRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		TaskID:      taskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, resolveLive, resolveRuntime, "action.resolved")

	sourceQueueResolvedRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, queueLives[sourceQueue.QueueID], sourceQueueResolvedRuntime, "workspace.ops.resolved")

	actionQueueResolvedRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, queueLives[actionQueue.QueueID], actionQueueResolvedRuntime, "workspace.ops.resolved")

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.Status != "COMPLETED" {
		t.Fatalf("action status = %q, want COMPLETED", action.Status)
	}

	resolvedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved source queue: %v", err)
	}
	if resolvedSourceQueue.Status != "RESOLVED" {
		t.Fatalf("source queue status = %q, want RESOLVED", resolvedSourceQueue.Status)
	}
	if resolvedSourceQueue.Resolution != "linked_action_completed:"+actionID {
		t.Fatalf("source queue resolution = %q, want linked_action_completed:%s", resolvedSourceQueue.Resolution, actionID)
	}
	if !strings.Contains(resolvedSourceQueue.PayloadJSON, "\"rebase_workflow_state\":\"completed\"") {
		t.Fatalf("resolved source queue payload should surface completed workflow state: %s", resolvedSourceQueue.PayloadJSON)
	}
	if !strings.Contains(resolvedSourceQueue.PayloadJSON, "\"rebase_workflow_step\":\"action_resolved\"") {
		t.Fatalf("resolved source queue payload should surface action_resolved workflow step: %s", resolvedSourceQueue.PayloadJSON)
	}
	if !strings.Contains(resolvedSourceQueue.PayloadJSON, "\"action_id\":\""+actionID+"\"") {
		t.Fatalf("resolved source queue payload should retain linked action id: %s", resolvedSourceQueue.PayloadJSON)
	}

	if strings.TrimSpace(forkTensionID) == "" || strings.TrimSpace(repairTensionID) == "" {
		t.Fatalf("expected fork and repair tensions, got fork=%q repair=%q", forkTensionID, repairTensionID)
	}
}

func TestBundleForkLateFailRetryLifecycleBlackBox(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-blackbox-late-fail-retry"
		taskID      = "task-action-blackbox-late-fail-retry"
		agentID     = "agent-action-blackbox-late-fail-retry"
		tensionID   = "tension:bundle:blackbox:late-fail-retry"
		runID       = "run-bundle-blackbox-late-fail-retry"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	insertActionSourceTensionFixture(t, ctx, store, workspaceID, taskID, agentID, tensionID)

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "Task-linked bundle retry test")
	if err != nil {
		t.Fatalf("CreateCoalition: %v", err)
	}

	decision, err := store.EvaluateCoalitionBundle(ctx, workspaceID, coalition.CoalitionID, sqlite.BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}, "patch-ref-blackbox-late-fail-retry")
	if err != nil {
		t.Fatalf("EvaluateCoalitionBundle: %v", err)
	}
	if decision.Decision != "FORK" || decision.NextAction != "attempt_rebase" || decision.ConflictSafeClass != "rebase_candidate" {
		t.Fatalf("unexpected bundle decision %+v", decision)
	}

	var repairTensionID string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'repair'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&repairTensionID); err != nil {
		t.Fatalf("query repair tension: %v", err)
	}
	sourceQueue := operatorQueueForSource(t, ctx, store, workspaceID, "tension", repairTensionID)
	if sourceQueue.TaskID != taskID || sourceQueue.AgentID != agentID {
		t.Fatalf("unexpected source queue %+v", sourceQueue)
	}

	firstCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal first actionCreate params: %v", err)
	}
	firstCreateAny, rpcErr := h.actionCreate(ctx, firstCreateRaw)
	if rpcErr != nil {
		t.Fatalf("first actionCreate rpc error: %+v", rpcErr)
	}
	firstCreateResp, ok := firstCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first actionCreate response type %T", firstCreateAny)
	}
	firstActionID, ok := firstCreateResp["action_id"].(string)
	if !ok || firstActionID == "" {
		t.Fatalf("unexpected first actionCreate response %+v", firstCreateResp)
	}

	firstStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  firstActionID,
		StartedBy: "reviewer-a",
		Comment:   "Initial rebase pass before verifier late fail.",
	})
	if err != nil {
		t.Fatalf("marshal first actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, firstStartRaw); rpcErr != nil {
		t.Fatalf("first actionStart rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, firstActionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)
	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier late fail on bundle-driven rebase",
		Summary:     "black-box bundle retry lifecycle",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueue.QueueID,
			"action_id":       firstActionID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, firstActionID, sourceQueue.QueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	var rollbackFailureCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM operator_queue_items
		 WHERE workspace_id = ? AND queue_key LIKE ?`,
		workspaceID,
		model.RebaseRollbackFailureQueueKeyPrefix+"%",
	).Scan(&rollbackFailureCount); err != nil {
		t.Fatalf("count rollback failure queues: %v", err)
	}
	if rollbackFailureCount != 0 {
		t.Fatalf("expected happy rollback path to avoid rollback-failure recovery queue, got %d", rollbackFailureCount)
	}

	retryCreateOverrideRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		AssignedTo:  "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate override params: %v", err)
	}
	if _, rpcErr := h.actionCreate(ctx, retryCreateOverrideRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-a") {
		t.Fatalf("expected reopened retry promotion to reject assigned holder override, got %+v", rpcErr)
	}

	retryCreateAny, rpcErr := h.actionCreate(ctx, firstCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == firstActionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Retry rebase after verifier rollback.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Retry rebase landed cleanly.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, retryResolveRaw); rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
	firstAction, err := store.GetHumanAction(ctx, firstActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(first): %v", err)
	}
	if firstAction.Status != humanActionStatusFailed {
		t.Fatalf("first action status = %q, want %q", firstAction.Status, humanActionStatusFailed)
	}
	retryAction, err := store.GetHumanAction(ctx, retryActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(retry): %v", err)
	}
	if retryAction.Status != humanActionStatusCompleted {
		t.Fatalf("retry action status = %q, want %q", retryAction.Status, humanActionStatusCompleted)
	}
	resolvedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved source queue: %v", err)
	}
	if resolvedSourceQueue.Status != "RESOLVED" {
		t.Fatalf("source queue status = %q, want RESOLVED", resolvedSourceQueue.Status)
	}
	if !strings.Contains(resolvedSourceQueue.PayloadJSON, "\"last_failed_action_id\":\""+firstActionID+"\"") {
		t.Fatalf("resolved source queue payload should preserve failed first attempt lineage: %s", resolvedSourceQueue.PayloadJSON)
	}
}

func TestActionCreateRejectsInterleavingRetryCreateWinnerOnReopenedFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-action-create-interleaving-retry-winner"
		taskID      = "task-action-create-interleaving-retry-winner"
		agentID     = "agent-action-create-interleaving-retry-winner"
		repairID    = "tens-repair-action-create-interleaving-retry-winner"
		runID       = "run-action-create-interleaving-retry-winner"
	)

	firstActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier late fail reopens retry path",
		Summary:     "seed reopened retry carrier before interleaving create race",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
			"action_id":       firstActionID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, firstActionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}

	var (
		hookErr        error
		winnerActionID string
	)
	h.beforeActionCreateQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionCreateQueueEffectsOverride = nil
		createAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
		if rpcErr != nil {
			hookErr = fmt.Errorf("interleaving retry actionCreate rpc error: %+v", rpcErr)
			return
		}
		createResp, ok := createAny.(map[string]any)
		if !ok {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response type %T", createAny)
			return
		}
		winnerActionID, _ = createResp["action_id"].(string)
		if strings.TrimSpace(winnerActionID) == "" {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response %+v", createResp)
		}
	}

	if _, rpcErr := h.actionCreate(ctx, retryCreateRaw); rpcErr == nil {
		t.Fatal("expected outer retry actionCreate to fail after interleaving winner linked the reopened source queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "already linked to action") {
		t.Fatalf("expected duplicate linked retry create to fail with already linked guidance, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving retry create hook: %v", hookErr)
	}
	if winnerActionID == "" || winnerActionID == firstActionID {
		t.Fatalf("unexpected winner action id %q after reopened retry interleaving", winnerActionID)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated)+1 {
		t.Fatalf("interleaving retry create should append exactly one action.created row, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving retry create should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("list human actions after interleaving retry create: %v", err)
	}
	if len(actions) != 2 {
		t.Fatalf("expected exactly two human actions after interleaving retry create, got %+v", actions)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, winnerActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	firstAction, err := store.GetHumanAction(ctx, firstActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(first): %v", err)
	}
	if firstAction.Status != humanActionStatusFailed {
		t.Fatalf("first action status = %q, want %q", firstAction.Status, humanActionStatusFailed)
	}
}

func assertBlackBoxRebaseWorkflowAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actionID, sourceQueueID, wantActionStatus, wantWorkflowState, wantWorkflowStep string) {
	t.Helper()

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != wantActionStatus {
		t.Fatalf("action status = %q, want %q", action.Status, wantActionStatus)
	}

	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	payload, err := actionCreateDecodeQueuePayload(queue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload: %v", err)
	}
	if got := actionCreateQueuePayloadString(payload, "rebase_workflow_state"); got != wantWorkflowState {
		t.Fatalf("workflow state = %q, want %q", got, wantWorkflowState)
	}
	if got := actionCreateQueuePayloadString(payload, "rebase_workflow_step"); got != wantWorkflowStep {
		t.Fatalf("workflow step = %q, want %q", got, wantWorkflowStep)
	}
}

func createActionFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Action Runtime Events",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Action Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "reviewer-a",
		OwnerUserID: "developer",
		DisplayName: "Reviewer A",
	}); err != nil {
		t.Fatalf("register reviewer agent: %v", err)
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
		t.Fatalf("create task with graph: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
}

func createRollbackFailureActionQueueFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, queueKey, repairTensionID string) sqlite.OperatorQueueRecord {
	t.Helper()

	payloadJSON, err := json.Marshal(model.RebaseRollbackFailurePayload{
		Kind:            model.RebaseRollbackFailureKind,
		FailureScope:    "run_verify",
		FailureTrigger:  "verifier_late_fail_run",
		FailureMessage:  "Rollback path needs operator recovery.",
		TaskID:          taskID,
		AgentID:         agentID,
		RepairTensionID: repairTensionID,
	})
	if err != nil {
		t.Fatalf("marshal rollback failure payload: %v", err)
	}
	record, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover rollback failure",
		Summary:           "Investigate rollback failure and decide retry path",
		Details:           "Failure scope: run_verify\nFailure trigger: verifier_late_fail_run\nRepair tension: " + repairTensionID,
		PayloadJSON:       string(payloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          "evt-rollback-failure-" + repairTensionID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rollback failure queue: %v", err)
	}
	return record
}

func createQueueOnlyRollbackFailureActionQueueFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueKey, entityID string) sqlite.OperatorQueueRecord {
	t.Helper()

	payloadJSON, err := json.Marshal(model.RebaseRollbackFailurePayload{
		Kind:           model.RebaseRollbackFailureKind,
		FailureScope:   "rsp_anomaly_list",
		FailureTrigger: "verifier_late_fail_queue_list",
		FailureMessage: "Queue-only anomaly recovery needs operator review.",
		EntityID:       entityID,
		SourceQueueKey: "tension_rebase_followup:entity-rollback-queue-only",
	})
	if err != nil {
		t.Fatalf("marshal queue-only rollback failure payload: %v", err)
	}
	record, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover queue-only rollback failure",
		Summary:           "Investigate rollback recovery and resolve or escalate the queue directly",
		Details:           "Failure scope: rsp_anomaly_list\nFailure trigger: verifier_late_fail_queue_list\nEntity: " + entityID,
		PayloadJSON:       string(payloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          entityID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create queue-only rollback failure queue: %v", err)
	}
	return record
}

func advanceOpenOperatorQueueRevision(t *testing.T, ctx context.Context, store *sqlite.Store, queue sqlite.OperatorQueueRecord, marker string) sqlite.OperatorQueueRecord {
	t.Helper()

	summary := strings.TrimSpace(queue.Summary)
	marker = strings.TrimSpace(marker)
	if marker != "" {
		if summary == "" {
			summary = marker
		} else {
			summary += " | " + marker
		}
	}

	updated, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:                 queue.QueueID,
		WorkspaceID:             queue.WorkspaceID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 summary,
		Details:                 queue.Details,
		PayloadJSON:             queue.PayloadJSON,
		AssignedTo:              queue.AssignedTo,
		Urgency:                 queue.Urgency,
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		TaskID:                  queue.TaskID,
		SessionID:               queue.SessionID,
		AgentID:                 queue.AgentID,
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   actionCreateOptionalString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	})
	if err != nil {
		t.Fatalf("advance open operator queue revision: %v", err)
	}
	if updated.UpdatedAt == queue.UpdatedAt {
		t.Fatalf("expected queue updated_at to advance, before=%q after=%q", queue.UpdatedAt, updated.UpdatedAt)
	}
	return updated
}

func insertActionSourceTensionFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, tensionID string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	taskIDs, err := json.Marshal([]string{taskID})
	if err != nil {
		t.Fatalf("marshal task ids: %v", err)
	}
	agentIDs, err := json.Marshal([]string{agentID})
	if err != nil {
		t.Fatalf("marshal agent ids: %v", err)
	}
	emptyList, err := json.Marshal([]string{})
	if err != nil {
		t.Fatalf("marshal empty list: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status,
			title, summary, anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json,
			artifact_refs_json, segment_refs_json, agent_ids_json, constraint_refs_json, base_score,
			surface_score, evidence_count, last_seen_event_id, last_seen_at, confirmed_by, archived_by,
			dismissed_reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		tensionID,
		workspaceID,
		"cluster-action-blackbox",
		"bridge",
		"ACTIVE",
		"CONFIRMED",
		"Task-linked overlap source tension",
		"Black-box rebase lifecycle source tension.",
		"task",
		taskID,
		string(taskIDs),
		string(emptyList),
		string(emptyList),
		string(emptyList),
		string(emptyList),
		string(agentIDs),
		string(emptyList),
		70,
		55,
		1,
		"event-action-blackbox",
		now,
		"developer",
		"",
		"",
		now,
		now,
	); err != nil {
		t.Fatalf("insert action source tension fixture: %v", err)
	}
}

func nextActionEventsOfTypes(t *testing.T, ch <-chan EventMessage, wantTypes ...string) map[string]EventMessage {
	t.Helper()

	want := make(map[string]struct{}, len(wantTypes))
	for _, wantType := range wantTypes {
		want[wantType] = struct{}{}
	}
	got := make(map[string]EventMessage, len(want))
	deadline := time.After(2 * time.Second)
	for len(got) < len(want) {
		select {
		case evt := <-ch:
			if _, ok := want[evt.Type]; ok {
				if _, seen := got[evt.Type]; !seen {
					got[evt.Type] = evt
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for live event types %v; got %+v", wantTypes, got)
		}
	}
	return got
}

func nextActionCreatedAndQueueUpdateForQueue(t *testing.T, ch <-chan EventMessage, queueID string) (EventMessage, EventMessage) {
	t.Helper()

	var actionLive EventMessage
	var queueLive EventMessage
	deadline := time.After(2 * time.Second)
	for actionLive.Type == "" || queueLive.Type == "" {
		select {
		case evt := <-ch:
			if evt.Type == "action.created" && actionLive.Type == "" {
				actionLive = evt
				continue
			}
			if evt.Type != "workspace.ops.updated" || queueLive.Type != "" {
				continue
			}
			var envelope sqlite.OperatorQueueRecord
			if err := json.Unmarshal([]byte(evt.PayloadJSON), &envelope); err != nil {
				continue
			}
			if envelope.QueueID == queueID {
				queueLive = evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for action.created and workspace.ops.updated for queue %s", queueID)
		}
	}
	return actionLive, queueLive
}

func nextActionStartedAndQueueUpdateForQueue(t *testing.T, ch <-chan EventMessage, queueID string) (EventMessage, EventMessage) {
	t.Helper()

	var actionLive EventMessage
	var queueLive EventMessage
	deadline := time.After(2 * time.Second)
	for actionLive.Type == "" || queueLive.Type == "" {
		select {
		case evt := <-ch:
			if evt.Type == "action.started" && actionLive.Type == "" {
				actionLive = evt
				continue
			}
			if evt.Type != "workspace.ops.updated" || queueLive.Type != "" {
				continue
			}
			var envelope sqlite.OperatorQueueRecord
			if err := json.Unmarshal([]byte(evt.PayloadJSON), &envelope); err != nil {
				continue
			}
			if envelope.QueueID == queueID {
				queueLive = evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for action.started and workspace.ops.updated for queue %s", queueID)
		}
	}
	return actionLive, queueLive
}

func nextActionPausedAndQueueUpdateForQueue(t *testing.T, ch <-chan EventMessage, queueID string) (EventMessage, EventMessage) {
	t.Helper()

	var actionLive EventMessage
	var queueLive EventMessage
	deadline := time.After(2 * time.Second)
	for actionLive.Type == "" || queueLive.Type == "" {
		select {
		case evt := <-ch:
			if evt.Type == "action.paused" && actionLive.Type == "" {
				actionLive = evt
				continue
			}
			if evt.Type != "workspace.ops.updated" || queueLive.Type != "" {
				continue
			}
			var envelope sqlite.OperatorQueueRecord
			if err := json.Unmarshal([]byte(evt.PayloadJSON), &envelope); err != nil {
				continue
			}
			if envelope.QueueID == queueID {
				queueLive = evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for action.paused and workspace.ops.updated for queue %s", queueID)
		}
	}
	return actionLive, queueLive
}

func nextActionResolvedAndQueueResolutionsForQueues(t *testing.T, ch <-chan EventMessage, queueIDs ...string) (EventMessage, map[string]EventMessage) {
	t.Helper()

	want := make(map[string]struct{}, len(queueIDs))
	for _, queueID := range queueIDs {
		queueID = strings.TrimSpace(queueID)
		if queueID == "" {
			continue
		}
		want[queueID] = struct{}{}
	}
	queueLives := make(map[string]EventMessage, len(want))
	var actionLive EventMessage
	deadline := time.After(2 * time.Second)
	for actionLive.Type == "" || len(queueLives) < len(want) {
		select {
		case evt := <-ch:
			if evt.Type == "action.resolved" && actionLive.Type == "" {
				actionLive = evt
				continue
			}
			if evt.Type != "workspace.ops.resolved" {
				continue
			}
			var envelope sqlite.OperatorQueueRecord
			if err := json.Unmarshal([]byte(evt.PayloadJSON), &envelope); err != nil {
				continue
			}
			if _, ok := want[envelope.QueueID]; !ok {
				continue
			}
			if _, seen := queueLives[envelope.QueueID]; !seen {
				queueLives[envelope.QueueID] = evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for action.resolved and workspace.ops.resolved for queues %v", queueIDs)
		}
	}
	return actionLive, queueLives
}

func operatorQueueForSource(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sourceKind, sourceID string) sqlite.OperatorQueueRecord {
	t.Helper()

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list operator queue items: %v", err)
	}
	filtered := make([]sqlite.OperatorQueueRecord, 0, len(items))
	for _, item := range items {
		if item.SourceKind == sourceKind && item.SourceID == sourceID {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) != 1 {
		t.Fatalf("expected one operator queue item for %s:%s, got %+v", sourceKind, sourceID, items)
	}
	return filtered[0]
}

func interleaveOperatorQueueRevisionForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueID, summaryMarker string) sqlite.OperatorQueueRecord {
	t.Helper()

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", queueID, err)
	}
	dueAt := ""
	if current.DueAt != nil {
		dueAt = strings.TrimSpace(*current.DueAt)
	}
	updated, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:              current.QueueID,
		WorkspaceID:          current.WorkspaceID,
		QueueKey:             current.QueueKey,
		QueueType:            current.QueueType,
		Title:                current.Title,
		Summary:              strings.TrimSpace(current.Summary + " | " + summaryMarker),
		Details:              current.Details,
		PayloadJSON:          current.PayloadJSON,
		AssignedTo:           current.AssignedTo,
		Urgency:              current.Urgency,
		SourceKind:           current.SourceKind,
		SourceID:             current.SourceID,
		TaskID:               current.TaskID,
		SessionID:            current.SessionID,
		AgentID:              current.AgentID,
		KeepSessionActive:    current.KeepSessionActive,
		DueAt:                dueAt,
		RequireCurrentStatus: "OPEN",
	})
	if err != nil {
		t.Fatalf("interleave queue revision for %s: %v", queueID, err)
	}
	if updated.UpdatedAt == current.UpdatedAt {
		t.Fatalf("expected interleaved queue revision to advance updated_at for %s", queueID)
	}
	return updated
}

func interleaveHumanActionRevisionForTest(t *testing.T, ctx context.Context, store *sqlite.Store, actionID, assignedTo, titleMarker string) sqlite.HumanActionRecord {
	t.Helper()

	current, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	nextAssignedTo := strings.TrimSpace(firstNonEmpty(assignedTo, current.AssignedTo))
	nextTitle := strings.TrimSpace(current.Title)
	if marker := strings.TrimSpace(titleMarker); marker != "" {
		nextTitle = strings.TrimSpace(current.Title + " | " + marker)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE human_actions SET assigned_to = ?, title = ?, revision = revision + 1 WHERE action_id = ?`,
		nextAssignedTo,
		nextTitle,
		actionID,
	); err != nil {
		t.Fatalf("interleave human action revision for %s: %v", actionID, err)
	}
	updated, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s) after interleave: %v", actionID, err)
	}
	if updated.Revision <= current.Revision {
		t.Fatalf("expected interleaved human action revision to advance, before=%d after=%d", current.Revision, updated.Revision)
	}
	return updated
}

func interleaveWorkspaceOpsEscalateForTest(t *testing.T, ctx context.Context, h *Handler, store *sqlite.Store, workspaceID, queueID, escalatedBy, assignedTo, reason string) (sqlite.OperatorQueueRecord, error) {
	t.Helper()

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
	if err != nil {
		return sqlite.OperatorQueueRecord{}, fmt.Errorf("GetOperatorQueueItem(%s): %w", queueID, err)
	}
	raw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          current.QueueID,
		EscalatedBy:      escalatedBy,
		Reason:           reason,
		AssignedTo:       assignedTo,
		CurrentUpdatedAt: current.UpdatedAt,
	})
	if err != nil {
		return sqlite.OperatorQueueRecord{}, fmt.Errorf("marshal workspaceOpsEscalate params: %w", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), raw); rpcErr != nil {
		return sqlite.OperatorQueueRecord{}, fmt.Errorf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}
	updated, err := store.GetOperatorQueueItem(ctx, workspaceID, current.QueueID, "")
	if err != nil {
		return sqlite.OperatorQueueRecord{}, fmt.Errorf("GetOperatorQueueItem(%s) after escalate: %w", current.QueueID, err)
	}
	return updated, nil
}

func createLinkedRebaseActionForTest(t *testing.T, ctx context.Context, h *Handler, store *sqlite.Store, workspaceID, taskID, agentID, queueKey, idSuffix string) (sqlite.OperatorQueueRecord, string) {
	t.Helper()

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-" + idSuffix,
		"fork_tension_id":     "tens-fork-" + idSuffix,
		"repair_tension_id":   "tens-repair-" + idSuffix,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for " + idSuffix,
		Details:           "Coalition ID: coal-" + idSuffix + "\nRepair tension: tens-repair-" + idSuffix + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-" + idSuffix,
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
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	return sourceQueue, actionID
}

func createLinkedRollbackFailureActionForTest(t *testing.T, ctx context.Context, h *Handler, store *sqlite.Store, workspaceID, taskID, agentID, queueKey, idSuffix string) (sqlite.OperatorQueueRecord, string) {
	t.Helper()

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-rollback-"+idSuffix)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("rollback-failure actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected rollback-failure actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected rollback-failure actionCreate response %+v", createResp)
	}
	return sourceQueue, actionID
}

func assertLinkedRebaseActionAuthorityHandoff(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actionID, sourceQueueID, wantAssignedTo string) {
	t.Helper()

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != wantAssignedTo {
		t.Fatalf("action assigned_to = %q, want %q", action.AssignedTo, wantAssignedTo)
	}

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.AssignedTo != wantAssignedTo {
		t.Fatalf("source queue assigned_to = %q, want %q", sourceQueue.AssignedTo, wantAssignedTo)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload: %v", err)
	}
	if sourcePayload.ActionAssignedTo != wantAssignedTo {
		t.Fatalf("source queue payload action_assigned_to = %q, want %q", sourcePayload.ActionAssignedTo, wantAssignedTo)
	}

	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action:%s): %v", actionID, err)
	}
	if actionQueue.AssignedTo != wantAssignedTo {
		t.Fatalf("action queue assigned_to = %q, want %q", actionQueue.AssignedTo, wantAssignedTo)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode action queue payload: %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != wantAssignedTo {
		t.Fatalf("action queue payload action_assigned_to = %q, want %q", actionQueuePayload.ActionAssignedTo, wantAssignedTo)
	}
}

func assertLinkedRollbackFailureActionAuthorityHandoff(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actionID, sourceQueueID, wantAssignedTo string) {
	t.Helper()

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.AssignedTo != wantAssignedTo {
		t.Fatalf("action assigned_to = %q, want %q", action.AssignedTo, wantAssignedTo)
	}

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.AssignedTo != wantAssignedTo {
		t.Fatalf("source queue assigned_to = %q, want %q", sourceQueue.AssignedTo, wantAssignedTo)
	}
	sourcePayload, err := actionCreateDecodeRollbackFailurePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure source queue payload: %v", err)
	}
	if sourcePayload.FollowupActionID != actionID {
		t.Fatalf("rollback-failure source queue followup_action_id = %q, want %q", sourcePayload.FollowupActionID, actionID)
	}
	if sourcePayload.FollowupActionStatus != humanActionStatusPending {
		t.Fatalf("rollback-failure source queue followup_action_status = %q, want %q", sourcePayload.FollowupActionStatus, humanActionStatusPending)
	}

	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action:%s): %v", actionID, err)
	}
	if actionQueue.AssignedTo != wantAssignedTo {
		t.Fatalf("action queue assigned_to = %q, want %q", actionQueue.AssignedTo, wantAssignedTo)
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func seedActionCreateQueueTensionContextForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, taskID, agentID string) {
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
		"Linked action create context",
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
		t.Fatalf("seed tension context %s: %v", tensionID, err)
	}
}
