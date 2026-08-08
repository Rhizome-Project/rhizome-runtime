package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/sessionmemory"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func nextEventOfType(t *testing.T, ch <-chan EventMessage, want string) EventMessage {
	t.Helper()
	for {
		evt := nextEvent(t, ch)
		if evt.Type == want {
			return evt
		}
	}
}

func TestAgentSessionTakeoverReturnsSuccessorState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-handler-takeover",
		PrincipalType: "agent",
		PrincipalID:   "agent-b",
	})
	ch := h.GetEventBus().Subscribe("ws-handler-takeover")
	defer h.GetEventBus().Unsubscribe("ws-handler-takeover", ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-takeover",
		Title:       "Handler Takeover",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, "ws-handler-takeover")
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-handler-takeover",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-handler-takeover",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-handler-takeover",
		TaskID:      "task-handler-takeover",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-handler-takeover",
		TaskID:      "task-handler-takeover",
		AgentID:     "agent-a",
		Summary:     "claim before takeover source session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-handler-source",
		AgentID:     "agent-a",
		WorkspaceID: "ws-handler-takeover",
		TaskID:      "task-handler-takeover",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: "ws-handler-takeover",
		SessionID:   "sess-handler-source",
		AgentID:     "agent-a",
		TaskID:      "task-handler-takeover",
		Summary:     "Working transport follow-up",
		OwnerScope:  "task/session",
		HandoffTo:   "agent-b",
	}); err != nil {
		t.Fatalf("record start session coordination: %v", err)
	}

	raw, err := json.Marshal(agentSessionTakeoverParams{
		WorkspaceID:     "ws-handler-takeover",
		SessionID:       "sess-handler-source",
		TakeoverAgentID: "agent-b",
		Summary:         "Shift ownership to agent-b",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentSessionTakeover(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentSessionTakeover rpc error: %+v", rpcErr)
	}

	payload := result.(map[string]any)
	if payload["status"] != "TAKEN_OVER" {
		t.Fatalf("expected TAKEN_OVER status, got %+v", payload)
	}
	successor, ok := payload["successor_state"].(sqlite.AgentSessionStateRecord)
	if !ok {
		t.Fatalf("unexpected successor state type %T", payload["successor_state"])
	}
	if successor.AgentID != "agent-b" || successor.Status != "ACTIVE" {
		t.Fatalf("unexpected successor state %+v", successor)
	}

	takeoverPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-handler-takeover",
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    "sess-handler-source",
	})
	sourceMemoryPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-handler-takeover",
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    sessionmemory.StateMemoryID("sess-handler-source"),
	})
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: takeoverPersisted, Type: "agent.session.takeover"},
		runtimeEventExpectation{Event: sourceMemoryPersisted, Type: "workspace.memory.recorded"},
	)
	for i, expectation := range ordered {
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}

	memory, err := store.GetWorkspaceMemory(ctx, "ws-handler-takeover", sessionmemory.StateMemoryID("sess-handler-source"))
	if err != nil {
		t.Fatalf("get source session memory: %v", err)
	}
	if memory.SourceKind != "session_event" || memory.SessionID != "sess-handler-source" {
		t.Fatalf("unexpected source session memory %+v", memory)
	}
	if !workspaceMemoryHasTag(memory.Tags, "ended") {
		t.Fatalf("expected source memory to be marked ended, got %+v", memory.Tags)
	}
	if _, err := store.GetWorkspaceMemory(ctx, "ws-handler-takeover", sessionmemory.StateMemoryID(successor.SessionID)); err == nil {
		t.Fatalf("expected no active successor start memory to persist")
	}
}

func TestAgentSessionEndRejectsPostTakeoverSourceWithoutUpdateSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID     = "ws-handler-session-stale-end"
		sourceAgent     = "agent-handler-source-stale-end"
		targetAgent     = "agent-handler-target-stale-end"
		taskID          = "task-handler-session-stale-end"
		sourceSessionID = "sess-handler-source-stale-end"
	)
	ctxSource := testAuthContext(workspaceID, "agent", sourceAgent)
	ctxTarget := testAuthContext(workspaceID, "agent", targetAgent)

	if err := store.CreateWorkspace(ctxSource, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Session Stale End",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctxSource, store, workspaceID)
	for _, agentID := range []string{sourceAgent, targetAgent} {
		if err := store.RegisterAgent(ctxSource, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-handler-session-stale-end", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctxSource, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctxSource, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctxSource, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     sourceAgent,
		Summary:     "claim before takeover source session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctxSource, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "source owns session before handoff",
		HandoffTo:   targetAgent,
		UpdatedAt:   "2026-04-21T13:00:00Z",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	result, rpcErr := h.agentSessionTakeover(ctxTarget, mustMarshalJSON(t, agentSessionTakeoverParams{
		WorkspaceID:     workspaceID,
		SessionID:       sourceSessionID,
		TakeoverAgentID: targetAgent,
		Summary:         "target takes over stale source session",
		UpdatedAt:       "2026-04-21T13:01:00Z",
	}))
	if rpcErr != nil {
		t.Fatalf("agentSessionTakeover rpc error: %+v", rpcErr)
	}
	takeoverPayload := result.(map[string]any)
	successor := takeoverPayload["successor_state"].(sqlite.AgentSessionStateRecord)

	sourceBefore, err := store.GetAgentSessionState(ctxSource, workspaceID, sourceSessionID)
	if err != nil {
		t.Fatalf("get source session before stale end: %v", err)
	}
	if sourceBefore.Status != model.SessionStatusEnded {
		t.Fatalf("expected takeover to end source session, got %+v", sourceBefore)
	}
	beforeAgentUpdates := countServerAgentUpdatesForA310(t, ctxSource, store, workspaceID)
	beforeEndEvents := countServerSessionRuntimeEventsForA310(t, ctxSource, store, workspaceID, sourceSessionID, model.SessionEventEnd)

	result, rpcErr = h.agentSessionEnd(ctxSource, mustMarshalJSON(t, agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "stale source tries to end after takeover",
		UpdatedAt:   "2026-04-21T13:02:00Z",
	}))
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for stale source session.end after takeover, got result=%+v rpcErr=%+v", result, rpcErr)
	}
	if result != nil {
		t.Fatalf("expected no result on stale source session.end rejection, got %+v", result)
	}

	sourceAfter, err := store.GetAgentSessionState(ctxSource, workspaceID, sourceSessionID)
	if err != nil {
		t.Fatalf("get source session after stale end: %v", err)
	}
	if sourceAfter.Status != sourceBefore.Status || sourceAfter.Summary != sourceBefore.Summary ||
		sourceAfter.UpdatedAt != sourceBefore.UpdatedAt || sourceAfter.CompletedAt != sourceBefore.CompletedAt {
		t.Fatalf("expected source session unchanged after stale end rejection, before=%+v after=%+v", sourceBefore, sourceAfter)
	}
	if afterAgentUpdates := countServerAgentUpdatesForA310(t, ctxSource, store, workspaceID); afterAgentUpdates != beforeAgentUpdates {
		t.Fatalf("expected no agent_updates side effects after stale session.end, before=%d after=%d", beforeAgentUpdates, afterAgentUpdates)
	}
	if afterEndEvents := countServerSessionRuntimeEventsForA310(t, ctxSource, store, workspaceID, sourceSessionID, model.SessionEventEnd); afterEndEvents != beforeEndEvents {
		t.Fatalf("expected no session.end runtime event after stale rejection, before=%d after=%d", beforeEndEvents, afterEndEvents)
	}
	activeSessions, err := store.ListWorkspaceSessionStates(ctxSource, workspaceID, true, 10)
	if err != nil {
		t.Fatalf("list active sessions: %v", err)
	}
	if len(activeSessions) != 1 || activeSessions[0].SessionID != successor.SessionID {
		t.Fatalf("expected only successor session active after stale end rejection, got %+v", activeSessions)
	}
}

func TestAgentSessionBlockedRejectsForeignOwnerWithoutQueueSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-session-owner-guard",
		Title:       "Handler Session Owner Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, "ws-handler-session-owner-guard")
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-handler-session-owner-guard",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-handler-session-owner-guard",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-handler-session-owner-guard",
		TaskID:      "task-handler-session-owner-guard",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-handler-session-owner-guard",
		TaskID:      "task-handler-session-owner-guard",
		AgentID:     "agent-a",
		Summary:     "claim before owner guard session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-handler-owner-guard",
		AgentID:     "agent-a",
		WorkspaceID: "ws-handler-session-owner-guard",
		TaskID:      "task-handler-session-owner-guard",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-handler-session-owner-guard",
		SessionID:   "sess-handler-owner-guard",
		AgentID:     "agent-a",
		TaskID:      "task-handler-session-owner-guard",
		Summary:     "agent-a owns the critical session",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	_, rpcErr := callAgentSessionBlockedRaw(t, h, ctx, mustMarshalJSON(t, agentSessionEventParams{
		WorkspaceID: "ws-handler-session-owner-guard",
		SessionID:   "sess-handler-owner-guard",
		AgentID:     "agent-b",
		TaskID:      "task-handler-session-owner-guard",
		Summary:     "agent-b tries to manufacture a blocker",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve takeover"}},
	}))
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for foreign owner session.blocked, got %+v", rpcErr)
	}

	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: "ws-handler-session-owner-guard",
		QueueType:   "BLOCKER",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocker queues: %v", err)
	}
	if len(queues) != 0 {
		t.Fatalf("expected no blocker queue after foreign session.blocked rejection, got %+v", queues)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-handler-session-owner-guard",
		EventType:   model.SessionEventBlocked,
		EntityType:  "agent_session",
		EntityID:    "sess-handler-owner-guard",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocked runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no session.blocked runtime event after foreign rejection, got %+v", events)
	}

	state, err := store.GetAgentSessionState(ctx, "ws-handler-session-owner-guard", "sess-handler-owner-guard")
	if err != nil {
		t.Fatalf("get session state: %v", err)
	}
	if state.AgentID != "agent-a" || state.Status != model.SessionStatusActive {
		t.Fatalf("expected session state to stay with agent-a and ACTIVE, got %+v", state)
	}
}

func TestAgentSessionTakeoverRejectsMissingExplicitHandoff(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-handler-takeover-guard",
		Title:       "Handler Takeover Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, "ws-handler-takeover-guard")
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-handler-takeover-guard",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-handler-takeover-guard",
		AgentID:     "agent-a",
		WorkspaceID: "ws-handler-takeover-guard",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: "ws-handler-takeover-guard",
		SessionID:   "sess-handler-takeover-guard",
		AgentID:     "agent-a",
		Summary:     "source owns the session without explicit handoff",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	_, rpcErr := callAgentSessionTakeoverRaw(t, h, ctx, mustMarshalJSON(t, agentSessionTakeoverParams{
		WorkspaceID:     "ws-handler-takeover-guard",
		SessionID:       "sess-handler-takeover-guard",
		TakeoverAgentID: "agent-b",
		Summary:         "agent-b tries to seize ownership",
	}))
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for takeover without explicit handoff, got %+v", rpcErr)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-handler-takeover-guard",
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    "sess-handler-takeover-guard",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list takeover runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no session.takeover runtime event after unauthorized takeover rejection, got %+v", events)
	}
}

func TestAgentSessionEventRPCPersistsAndArchivesSessionMemory(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-session-memory",
		PrincipalType: "agent",
		PrincipalID:   "agent-a",
	})

	const (
		workspaceID = "ws-session-memory"
		sessionID   = "sess-session-memory"
		taskID      = "task-session-memory"
	)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Memory",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
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
		AgentID:     "agent-a",
		Summary:     "claim before session memory start",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	runFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "session:" + sessionID,
		Limit:       10,
	}
	stepFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		Limit:       20,
	}

	startRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Start runtime work",
	})
	if err != nil {
		t.Fatalf("marshal start params: %v", err)
	}
	if _, rpcErr := h.agentSessionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("agentSessionStart rpc error: %+v", rpcErr)
	}
	startLive := nextEvent(t, ch)
	if startLive.Type != "agent.session.start" {
		t.Fatalf("expected agent.session.start event, got %+v", startLive)
	}
	startPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStart,
		EntityType:  "agent_session",
		EntityID:    sessionID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, startLive, startPersisted, "agent.session.start")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, startLive.PayloadJSON), startPersisted.PayloadJSON)
	startRunPersisted := mustRuntimeEvent(t, ctx, store, runFilter)
	startStepPersisted := mustRuntimeEvent(t, ctx, store, stepFilter)
	startDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get start execution detail: %v", err)
	}
	startStepRecord := mustExecutionStepFromDetail(t, startDetail, startStepPersisted.EntityID)
	startRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, startRunLive, startRunPersisted, "workspace.execution.run")
	var startRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(startRunLive.PayloadJSON), &startRunEnvelope); err != nil {
		t.Fatalf("decode start execution run payload: %v", err)
	}
	if startRunEnvelope.RunID != startDetail.Run.RunID || startRunEnvelope.Status != startDetail.Run.Status || startRunEnvelope.Summary != startDetail.Run.Summary {
		t.Fatalf("unexpected start execution run payload %+v / detail %+v", startRunEnvelope, startDetail.Run)
	}
	startStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, startStepLive, startStepPersisted, "workspace.execution.step")
	var startStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(startStepLive.PayloadJSON), &startStepEnvelope); err != nil {
		t.Fatalf("decode start execution step payload: %v", err)
	}
	if startStepEnvelope.StepID != startStepRecord.StepID || startStepEnvelope.Status != startStepRecord.Status || startStepEnvelope.Title != startStepRecord.Title {
		t.Fatalf("unexpected start execution step payload %+v / detail %+v", startStepEnvelope, startStepRecord)
	}
	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, stepFilter)

	blockedRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Waiting for human approval",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve rollout"}},
	})
	if err != nil {
		t.Fatalf("marshal blocked params: %v", err)
	}
	if _, rpcErr := h.agentSessionBlocked(ctx, blockedRaw); rpcErr != nil {
		t.Fatalf("agentSessionBlocked rpc error: %+v", rpcErr)
	}
	blockedPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventBlocked,
		EntityType:  "agent_session",
		EntityID:    sessionID,
	})
	blockedMemoryPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    sessionmemory.StateMemoryID(sessionID),
	})
	blockedClaimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
	})
	blockerQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "BLOCKER",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocker queue after blocked session event: %v", err)
	}
	if len(blockerQueues) != 1 {
		t.Fatalf("expected one blocker queue after blocked session event, got %+v", blockerQueues)
	}
	blockedQueuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
	})
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: blockedPersisted, Type: "agent.session.blocked"},
		runtimeEventExpectation{Event: blockedMemoryPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: blockedClaimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: blockedQueuePersisted, Type: "workspace.ops.updated"},
	)
	for i, expectation := range ordered {
		if strings.HasPrefix(expectation.Type, "workspace.ops.") || expectation.Type == "workspace.claim.written" {
			continue
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}
	blockedRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	blockedStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	blockedDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get blocked execution detail: %v", err)
	}
	blockedStepRecord := mustExecutionStepFromDetail(t, blockedDetail, blockedStepPersisted.EntityID)
	blockedRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, blockedRunLive, blockedRunPersisted, "workspace.execution.run")
	var blockedRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(blockedRunLive.PayloadJSON), &blockedRunEnvelope); err != nil {
		t.Fatalf("decode blocked execution run payload: %v", err)
	}
	if blockedRunEnvelope.RunID != blockedDetail.Run.RunID || blockedRunEnvelope.Status != blockedDetail.Run.Status || blockedRunEnvelope.Summary != blockedDetail.Run.Summary {
		t.Fatalf("unexpected blocked execution run payload %+v / detail %+v", blockedRunEnvelope, blockedDetail.Run)
	}
	blockedStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, blockedStepLive, blockedStepPersisted, "workspace.execution.step")
	var blockedStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(blockedStepLive.PayloadJSON), &blockedStepEnvelope); err != nil {
		t.Fatalf("decode blocked execution step payload: %v", err)
	}
	if blockedStepEnvelope.StepID != blockedStepRecord.StepID || blockedStepEnvelope.Status != blockedStepRecord.Status || blockedStepEnvelope.Title != blockedStepRecord.Title {
		t.Fatalf("unexpected blocked execution step payload %+v / detail %+v", blockedStepEnvelope, blockedStepRecord)
	}
	seenRunEvents = snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents = snapshotRuntimeEventIDs(t, ctx, store, stepFilter)

	memoryID := sessionmemory.StateMemoryID(sessionID)
	blockedMemory, err := store.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		t.Fatalf("get blocked memory: %v", err)
	}
	if blockedMemory.SourceKind != "session_event" || blockedMemory.ArchivedAt != nil {
		t.Fatalf("unexpected blocked memory %+v", blockedMemory)
	}
	if !workspaceMemoryHasTag(blockedMemory.Tags, "blocked") {
		t.Fatalf("expected blocked tag, got %+v", blockedMemory.Tags)
	}

	statusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Pending takeover by agent-b",
		Status:      model.SessionStatusHandoffPending,
		HandoffTo:   "agent-b",
	})
	if err != nil {
		t.Fatalf("marshal status params: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(ctx, statusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus rpc error: %+v", rpcErr)
	}
	statusPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    sessionID,
	})
	handoffMemoryPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    sessionmemory.StateMemoryID(sessionID),
	})
	handoffQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "HANDOFF",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list handoff queue after handoff-pending session status: %v", err)
	}
	if len(handoffQueues) != 1 {
		t.Fatalf("expected one handoff queue after handoff-pending session status, got %+v", handoffQueues)
	}
	handoffQueuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    handoffQueues[0].QueueID,
	})
	blockerResolvedPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
	})
	handoffClaimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
	})
	ordered, liveEvents = nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: statusPersisted, Type: "agent.session.status"},
		runtimeEventExpectation{Event: handoffMemoryPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: handoffClaimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: handoffQueuePersisted, Type: "workspace.ops.updated"},
		runtimeEventExpectation{Event: blockerResolvedPersisted, Type: "workspace.ops.resolved"},
	)
	for i, expectation := range ordered {
		if strings.HasPrefix(expectation.Type, "workspace.ops.") || expectation.Type == "workspace.claim.written" {
			continue
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}
	statusRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	statusStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	statusDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get status execution detail: %v", err)
	}
	statusStepRecord := mustExecutionStepFromDetail(t, statusDetail, statusStepPersisted.EntityID)
	statusRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, statusRunLive, statusRunPersisted, "workspace.execution.run")
	var statusRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(statusRunLive.PayloadJSON), &statusRunEnvelope); err != nil {
		t.Fatalf("decode status execution run payload: %v", err)
	}
	if statusRunEnvelope.RunID != statusDetail.Run.RunID || statusRunEnvelope.Status != statusDetail.Run.Status || statusRunEnvelope.Summary != statusDetail.Run.Summary {
		t.Fatalf("unexpected status execution run payload %+v / detail %+v", statusRunEnvelope, statusDetail.Run)
	}
	statusStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, statusStepLive, statusStepPersisted, "workspace.execution.step")
	var statusStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(statusStepLive.PayloadJSON), &statusStepEnvelope); err != nil {
		t.Fatalf("decode status execution step payload: %v", err)
	}
	if statusStepEnvelope.StepID != statusStepRecord.StepID || statusStepEnvelope.Status != statusStepRecord.Status || statusStepEnvelope.Title != statusStepRecord.Title {
		t.Fatalf("unexpected status execution step payload %+v / detail %+v", statusStepEnvelope, statusStepRecord)
	}
	seenRunEvents = snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents = snapshotRuntimeEventIDs(t, ctx, store, stepFilter)

	handoffMemory, err := store.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		t.Fatalf("get handoff memory: %v", err)
	}
	if !workspaceMemoryHasTag(handoffMemory.Tags, "handoff_pending") {
		t.Fatalf("expected handoff_pending tag, got %+v", handoffMemory.Tags)
	}

	keepaliveRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Back in active execution",
	})
	if err != nil {
		t.Fatalf("marshal keepalive params: %v", err)
	}
	if _, rpcErr := h.agentSessionKeepalive(ctx, keepaliveRaw); rpcErr != nil {
		t.Fatalf("agentSessionKeepalive rpc error: %+v", rpcErr)
	}
	keepalivePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventKeepalive,
		EntityType:  "agent_session",
		EntityID:    sessionID,
	})
	removedMemoryPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    sessionmemory.StateMemoryID(sessionID),
	})
	removedClaimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
	})
	blockerResolvedAfterKeepalivePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
	})
	seenBlockerResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
		Limit:       10,
	})
	handoffResolvedPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    handoffQueues[0].QueueID,
	})
	ordered, liveEvents = nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: keepalivePersisted, Type: "agent.session.keepalive"},
		runtimeEventExpectation{Event: removedMemoryPersisted, Type: "workspace.memory.removed"},
		runtimeEventExpectation{Event: removedClaimPersisted, Type: "workspace.claim.archived"},
		runtimeEventExpectation{Event: handoffResolvedPersisted, Type: "workspace.ops.resolved"},
	)
	for i, expectation := range ordered {
		if strings.HasPrefix(expectation.Type, "workspace.ops.") || expectation.Type == "workspace.claim.archived" {
			continue
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
		Limit:       10,
	}); len(got) != len(seenBlockerResolvedEvents) {
		t.Fatalf("expected keepalive not to append a second blocker resolve after terminal closure, before=%v after=%v latest=%+v", seenBlockerResolvedEvents, got, blockerResolvedAfterKeepalivePersisted)
	}
	keepaliveRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	keepaliveStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	keepaliveDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get keepalive execution detail: %v", err)
	}
	keepaliveStepRecord := mustExecutionStepFromDetail(t, keepaliveDetail, keepaliveStepPersisted.EntityID)
	keepaliveRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, keepaliveRunLive, keepaliveRunPersisted, "workspace.execution.run")
	var keepaliveRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(keepaliveRunLive.PayloadJSON), &keepaliveRunEnvelope); err != nil {
		t.Fatalf("decode keepalive execution run payload: %v", err)
	}
	if keepaliveRunEnvelope.RunID != keepaliveDetail.Run.RunID || keepaliveRunEnvelope.Status != keepaliveDetail.Run.Status || keepaliveRunEnvelope.Summary != keepaliveDetail.Run.Summary {
		t.Fatalf("unexpected keepalive execution run payload %+v / detail %+v", keepaliveRunEnvelope, keepaliveDetail.Run)
	}
	keepaliveStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, keepaliveStepLive, keepaliveStepPersisted, "workspace.execution.step")
	var keepaliveStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(keepaliveStepLive.PayloadJSON), &keepaliveStepEnvelope); err != nil {
		t.Fatalf("decode keepalive execution step payload: %v", err)
	}
	if keepaliveStepEnvelope.StepID != keepaliveStepRecord.StepID || keepaliveStepEnvelope.Status != keepaliveStepRecord.Status || keepaliveStepEnvelope.Title != keepaliveStepRecord.Title {
		t.Fatalf("unexpected keepalive execution step payload %+v / detail %+v", keepaliveStepEnvelope, keepaliveStepRecord)
	}

	archivedMemory, err := store.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		t.Fatalf("get archived memory: %v", err)
	}
	if archivedMemory.ArchivedAt == nil || archivedMemory.ArchivedReason != "session_session_keepalive" {
		t.Fatalf("expected archived keepalive memory, got %+v", archivedMemory)
	}
}

func TestAgentSessionBlockedSyncMirrorsNewPersistedQueueAndMemoryRowsOnRepeat(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-session-blocked-repeat",
		PrincipalType: "agent",
		PrincipalID:   "agent-a",
	})

	const (
		workspaceID = "ws-session-blocked-repeat"
		sessionID   = "sess-session-blocked-repeat"
		taskID      = "task-session-blocked-repeat"
	)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Repeated Session Blocked Mirrors",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
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
		AgentID:     "agent-a",
		Summary:     "claim before decision aliases session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	memoryID := sessionmemory.StateMemoryID(sessionID)
	memoryFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    memoryID,
		Limit:       10,
	}
	runFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "session:" + sessionID,
		Limit:       10,
	}
	stepFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		Limit:       20,
	}
	firstBlockedRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Waiting for rollout approval",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve rollout"}},
	})
	if err != nil {
		t.Fatalf("marshal first blocked params: %v", err)
	}
	if _, rpcErr := h.agentSessionBlocked(ctx, firstBlockedRaw); rpcErr != nil {
		t.Fatalf("agentSessionBlocked first rpc error: %+v", rpcErr)
	}
	statusFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventBlocked,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	}
	firstStatusPersisted := mustRuntimeEvent(t, ctx, store, statusFilter)
	firstMemoryPersisted := mustRuntimeEvent(t, ctx, store, memoryFilter)
	firstClaimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	})

	blockerQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "BLOCKER",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocker queues after first blocked event: %v", err)
	}
	if len(blockerQueues) != 1 {
		t.Fatalf("expected one blocker queue after first blocked event, got %+v", blockerQueues)
	}
	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
		Limit:       10,
	}
	firstQueuePersisted := mustRuntimeEvent(t, ctx, store, queueFilter)
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: firstStatusPersisted, Type: "agent.session.blocked"},
		runtimeEventExpectation{Event: firstMemoryPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: firstClaimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: firstQueuePersisted, Type: "workspace.ops.updated"},
	)
	for i, expectation := range ordered {
		if strings.HasPrefix(expectation.Type, "workspace.ops.") || expectation.Type == "workspace.claim.written" {
			continue
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}
	firstRunPersisted := mustRuntimeEvent(t, ctx, store, runFilter)
	firstStepPersisted := mustRuntimeEvent(t, ctx, store, stepFilter)
	firstDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get first blocked execution detail: %v", err)
	}
	firstStepRecord := mustExecutionStepFromDetail(t, firstDetail, firstStepPersisted.EntityID)
	firstRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, firstRunLive, firstRunPersisted, "workspace.execution.run")
	firstStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, firstStepLive, firstStepPersisted, "workspace.execution.step")
	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, stepFilter)
	var firstRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(firstRunLive.PayloadJSON), &firstRunEnvelope); err != nil {
		t.Fatalf("decode first blocked execution run payload: %v", err)
	}
	if firstRunEnvelope.RunID != firstDetail.Run.RunID || firstRunEnvelope.Status != firstDetail.Run.Status || firstRunEnvelope.Summary != firstDetail.Run.Summary {
		t.Fatalf("unexpected first blocked execution run payload %+v / detail %+v", firstRunEnvelope, firstDetail.Run)
	}
	var firstStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(firstStepLive.PayloadJSON), &firstStepEnvelope); err != nil {
		t.Fatalf("decode first blocked execution step payload: %v", err)
	}
	if firstStepEnvelope.StepID != firstStepRecord.StepID || firstStepEnvelope.Status != firstStepRecord.Status || firstStepEnvelope.Title != firstStepRecord.Title {
		t.Fatalf("unexpected first blocked execution step payload %+v / detail %+v", firstStepEnvelope, firstStepRecord)
	}

	seenStatusEvents := snapshotRuntimeEventIDs(t, ctx, store, statusFilter)
	seenMemoryEvents := snapshotRuntimeEventIDs(t, ctx, store, memoryFilter)
	seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	})
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	secondBlockedRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Still blocked on final approval",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve final rollout"}},
	})
	if err != nil {
		t.Fatalf("marshal second blocked params: %v", err)
	}
	if _, rpcErr := h.agentSessionBlocked(ctx, secondBlockedRaw); rpcErr != nil {
		t.Fatalf("agentSessionBlocked second rpc error: %+v", rpcErr)
	}
	secondStatusPersisted := mustNewRuntimeEvent(t, ctx, store, statusFilter, seenStatusEvents)
	secondMemoryPersisted := mustNewRuntimeEvent(t, ctx, store, memoryFilter, seenMemoryEvents)
	secondClaimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	}, seenClaimEvents)
	secondQueuePersisted := mustNewRuntimeEvent(t, ctx, store, queueFilter, seenQueueEvents)
	ordered, liveEvents = nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: secondStatusPersisted, Type: "agent.session.blocked"},
		runtimeEventExpectation{Event: secondMemoryPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: secondClaimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: secondQueuePersisted, Type: "workspace.ops.updated"},
	)
	for i, expectation := range ordered {
		if strings.HasPrefix(expectation.Type, "workspace.ops.") || expectation.Type == "workspace.claim.written" {
			continue
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}
	if secondMemoryPersisted.EventID == firstMemoryPersisted.EventID || secondMemoryPersisted.IngestSeq <= firstMemoryPersisted.IngestSeq {
		t.Fatalf("expected repeated session memory write to mirror the newly appended runtime row, got first=%+v second=%+v", firstMemoryPersisted, secondMemoryPersisted)
	}
	if secondStatusPersisted.EventID == firstStatusPersisted.EventID || secondStatusPersisted.IngestSeq <= firstStatusPersisted.IngestSeq {
		t.Fatalf("expected repeated session.blocked to mirror the newly appended runtime row, got first=%+v second=%+v", firstStatusPersisted, secondStatusPersisted)
	}
	if secondQueuePersisted.EventID == firstQueuePersisted.EventID || secondQueuePersisted.IngestSeq <= firstQueuePersisted.IngestSeq {
		t.Fatalf("expected repeated session queue sync to mirror the newly appended runtime row, got first=%+v second=%+v", firstQueuePersisted, secondQueuePersisted)
	}
	secondRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	secondStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	secondDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get second blocked execution detail: %v", err)
	}
	secondStepRecord := mustExecutionStepFromDetail(t, secondDetail, secondStepPersisted.EntityID)
	secondRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, secondRunLive, secondRunPersisted, "workspace.execution.run")
	secondStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, secondStepLive, secondStepPersisted, "workspace.execution.step")
	if secondRunPersisted.EventID == firstRunPersisted.EventID || secondRunPersisted.IngestSeq <= firstRunPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution run to mirror the newly appended runtime row, got first=%+v second=%+v", firstRunPersisted, secondRunPersisted)
	}
	if secondStepPersisted.EventID == firstStepPersisted.EventID || secondStepPersisted.IngestSeq <= firstStepPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution step to mirror the newly appended runtime row, got first=%+v second=%+v", firstStepPersisted, secondStepPersisted)
	}
	var secondRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(secondRunLive.PayloadJSON), &secondRunEnvelope); err != nil {
		t.Fatalf("decode second blocked execution run payload: %v", err)
	}
	if secondRunEnvelope.RunID != secondDetail.Run.RunID || secondRunEnvelope.Status != secondDetail.Run.Status || secondRunEnvelope.Summary != secondDetail.Run.Summary {
		t.Fatalf("unexpected second blocked execution run payload %+v / detail %+v", secondRunEnvelope, secondDetail.Run)
	}
	var secondStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(secondStepLive.PayloadJSON), &secondStepEnvelope); err != nil {
		t.Fatalf("decode second blocked execution step payload: %v", err)
	}
	if secondStepEnvelope.StepID != secondStepRecord.StepID || secondStepEnvelope.Status != secondStepRecord.Status || secondStepEnvelope.Title != secondStepRecord.Title {
		t.Fatalf("unexpected second blocked execution step payload %+v / detail %+v", secondStepEnvelope, secondStepRecord)
	}
	var queueEnvelope sqlite.OperatorQueueRecord
	for i, expectation := range ordered {
		if expectation.Type != "workspace.ops.updated" {
			continue
		}
		if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &queueEnvelope); err != nil {
			t.Fatalf("decode repeated session queue payload: %v", err)
		}
		break
	}
	if queueEnvelope.QueueID == "" {
		t.Fatalf("expected repeated session queue live payload to be present")
	}
	if queueEnvelope.QueueID != blockerQueues[0].QueueID || queueEnvelope.Summary != "Still blocked on final approval" || !strings.Contains(queueEnvelope.Details, "approve final rollout") {
		t.Fatalf("unexpected repeated session queue live payload %+v", queueEnvelope)
	}
}

func TestAgentSessionStatusMirrorsNewPersistedRowForRepeatedSessionEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-session-status-repeat",
		PrincipalType: "agent",
		PrincipalID:   "agent-a",
	})

	const (
		workspaceID = "ws-session-status-repeat"
		sessionID   = "sess-session-status-repeat"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Status Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	statusFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	}
	runFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "session:" + sessionID,
		Limit:       10,
	}
	stepFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		Limit:       20,
	}
	firstStatusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Summary:     "Waiting for reviewer assignment",
		Status:      model.SessionStatusBlocked,
	})
	if err != nil {
		t.Fatalf("marshal first status params: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(ctx, firstStatusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus first rpc error: %+v", rpcErr)
	}
	firstStatusLive := nextEventOfType(t, ch, "agent.session.status")
	firstStatusPersisted := mustRuntimeEvent(t, ctx, store, statusFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstStatusLive, firstStatusPersisted, "agent.session.status")
	firstRunPersisted := mustRuntimeEvent(t, ctx, store, runFilter)
	firstStepPersisted := mustRuntimeEvent(t, ctx, store, stepFilter)
	firstRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, firstRunLive, firstRunPersisted, "workspace.execution.run")
	firstStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, firstStepLive, firstStepPersisted, "workspace.execution.step")

	seenStatusEvents := snapshotRuntimeEventIDs(t, ctx, store, statusFilter)
	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, stepFilter)
	secondStatusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Summary:     "Hand-off pending to reviewer-b",
		Status:      model.SessionStatusHandoffPending,
		HandoffTo:   "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal second status params: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "reviewer-b",
		OwnerUserID: "developer",
		DisplayName: "Reviewer B",
	}); err != nil {
		t.Fatalf("register handoff agent: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(ctx, secondStatusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus second rpc error: %+v", rpcErr)
	}
	secondStatusLive := nextEventOfType(t, ch, "agent.session.status")
	secondStatusPersisted := mustNewRuntimeEvent(t, ctx, store, statusFilter, seenStatusEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondStatusLive, secondStatusPersisted, "agent.session.status")
	secondRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	secondStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	secondRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, secondRunLive, secondRunPersisted, "workspace.execution.run")
	secondStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, secondStepLive, secondStepPersisted, "workspace.execution.step")
	if secondStatusPersisted.EventID == firstStatusPersisted.EventID || secondStatusPersisted.IngestSeq <= firstStatusPersisted.IngestSeq {
		t.Fatalf("expected repeated session.status to mirror the newly appended runtime row, got first=%+v second=%+v", firstStatusPersisted, secondStatusPersisted)
	}
	if secondRunPersisted.EventID == firstRunPersisted.EventID || secondRunPersisted.IngestSeq <= firstRunPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution run to mirror the newly appended runtime row, got first=%+v second=%+v", firstRunPersisted, secondRunPersisted)
	}
	if secondStepPersisted.EventID == firstStepPersisted.EventID || secondStepPersisted.IngestSeq <= firstStepPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution step to mirror the newly appended runtime row, got first=%+v second=%+v", firstStepPersisted, secondStepPersisted)
	}
	var statusEnvelope sqlite.AgentSessionStateRecord
	if err := json.Unmarshal([]byte(secondStatusLive.PayloadJSON), &statusEnvelope); err != nil {
		t.Fatalf("decode second session status payload: %v", err)
	}
	if statusEnvelope.SessionID != sessionID || statusEnvelope.Status != model.SessionStatusHandoffPending || statusEnvelope.HandoffTo != "reviewer-b" {
		t.Fatalf("unexpected repeated session.status live payload %+v", statusEnvelope)
	}
}

func TestAgentSessionEventRPCPersistsRuntimeMetrics(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-session-rpc-metrics",
		PrincipalType: "agent",
		PrincipalID:   "agent-a",
	})

	const (
		workspaceID = "ws-session-rpc-metrics"
		sessionID   = "sess-session-rpc-metrics"
		agentID     = "agent-a"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session RPC Metrics",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	statusRaw := mustMarshalJSON(t, agentSessionEventParams{
		WorkspaceID:       workspaceID,
		SessionID:         sessionID,
		AgentID:           agentID,
		Summary:           "Status with token metrics",
		Status:            model.SessionStatusActive,
		Iterations:        2,
		TotalInputTokens:  123,
		TotalOutputTokens: 45,
		ToolCalls:         6,
	})
	if _, rpcErr := h.agentSessionStatus(ctx, statusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus rpc error: %+v", rpcErr)
	}
	record, err := store.GetAgentSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("get session after status: %v", err)
	}
	if record.Iterations != 2 || record.TotalInputTokens != 123 || record.TotalOutputTokens != 45 || record.ToolCalls != 6 {
		t.Fatalf("session metrics after status = %+v", record)
	}

	endRaw := mustMarshalJSON(t, agentSessionEventParams{
		WorkspaceID:       workspaceID,
		SessionID:         sessionID,
		AgentID:           agentID,
		Summary:           "Ended with final token metrics",
		Status:            model.SessionStatusEnded,
		KeepSessionActive: boolPtr(false),
		Iterations:        3,
		TotalInputTokens:  150,
		TotalOutputTokens: 70,
		ToolCalls:         7,
	})
	if _, rpcErr := h.agentSessionEnd(ctx, endRaw); rpcErr != nil {
		t.Fatalf("agentSessionEnd rpc error: %+v", rpcErr)
	}
	record, err = store.GetAgentSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("get session after end: %v", err)
	}
	if record.Status != model.SessionStatusEnded || record.Iterations != 3 || record.TotalInputTokens != 150 || record.TotalOutputTokens != 70 || record.ToolCalls != 7 {
		t.Fatalf("session metrics after end = %+v", record)
	}
}

func TestAgentSessionStatusMirrorsNewPersistedRowsForSessionExecution(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-session-execution-repeat",
		PrincipalType: "agent",
		PrincipalID:   "agent-a",
	})

	const (
		workspaceID = "ws-session-execution-repeat"
		sessionID   = "sess-session-execution-repeat"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Execution Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	statusFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	}
	runFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "session:" + sessionID,
		Limit:       10,
	}
	stepFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		Limit:       20,
	}

	firstStatusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Summary:     "Waiting for reviewer assignment",
		Status:      model.SessionStatusBlocked,
	})
	if err != nil {
		t.Fatalf("marshal first execution status params: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(ctx, firstStatusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus first execution rpc error: %+v", rpcErr)
	}

	firstStatusPersisted := mustRuntimeEvent(t, ctx, store, statusFilter)
	firstRunPersisted := mustRuntimeEvent(t, ctx, store, runFilter)
	firstStepPersisted := mustRuntimeEvent(t, ctx, store, stepFilter)
	firstDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get first session execution detail: %v", err)
	}
	firstStepRecord := mustExecutionStepFromDetail(t, firstDetail, firstStepPersisted.EntityID)
	assertSessionExecutionPromptContextEnvelope(t, firstDetail.Run.VerificationJSON, "agent.session.execution.run.sync")
	assertSessionExecutionPromptContextEnvelope(t, firstStepRecord.VerificationJSON, "agent.session.execution.step.sync")
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: firstStatusPersisted, Type: "agent.session.status"},
		runtimeEventExpectation{Event: firstRunPersisted, Type: "workspace.execution.run"},
		runtimeEventExpectation{Event: firstStepPersisted, Type: "workspace.execution.step"},
	)
	for i, expectation := range ordered {
		switch expectation.Type {
		case "agent.session.status":
			assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
		case "workspace.execution.run":
			var liveRun sqlite.ExecutionRunRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveRun); err != nil {
				t.Fatalf("decode first session execution run payload: %v", err)
			}
			if liveRun.RunID != firstDetail.Run.RunID || liveRun.Status != firstDetail.Run.Status || liveRun.Summary != firstDetail.Run.Summary {
				t.Fatalf("unexpected first session execution run live payload %+v / detail %+v", liveRun, firstDetail.Run)
			}
			assertSessionExecutionPromptContextEnvelope(t, liveRun.VerificationJSON, "agent.session.execution.run.sync")
		case "workspace.execution.step":
			var liveStep sqlite.ExecutionStepRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveStep); err != nil {
				t.Fatalf("decode first session execution step payload: %v", err)
			}
			if liveStep.StepID != firstStepRecord.StepID || liveStep.Status != firstStepRecord.Status || liveStep.Title != firstStepRecord.Title {
				t.Fatalf("unexpected first session execution step live payload %+v / detail %+v", liveStep, firstStepRecord)
			}
			assertSessionExecutionPromptContextEnvelope(t, liveStep.VerificationJSON, "agent.session.execution.step.sync")
		}
	}

	seenStatusEvents := snapshotRuntimeEventIDs(t, ctx, store, statusFilter)
	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, stepFilter)

	secondStatusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Summary:     "Still blocked pending reviewer assignment",
		Status:      model.SessionStatusBlocked,
	})
	if err != nil {
		t.Fatalf("marshal second execution status params: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(ctx, secondStatusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus second execution rpc error: %+v", rpcErr)
	}

	secondStatusPersisted := mustNewRuntimeEvent(t, ctx, store, statusFilter, seenStatusEvents)
	secondRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	secondStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	secondDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get second session execution detail: %v", err)
	}
	secondStepRecord := mustExecutionStepFromDetail(t, secondDetail, secondStepPersisted.EntityID)
	assertSessionExecutionPromptContextEnvelope(t, secondDetail.Run.VerificationJSON, "agent.session.execution.run.sync")
	assertSessionExecutionPromptContextEnvelope(t, secondStepRecord.VerificationJSON, "agent.session.execution.step.sync")
	ordered, liveEvents = nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: secondStatusPersisted, Type: "agent.session.status"},
		runtimeEventExpectation{Event: secondRunPersisted, Type: "workspace.execution.run"},
		runtimeEventExpectation{Event: secondStepPersisted, Type: "workspace.execution.step"},
	)
	for i, expectation := range ordered {
		switch expectation.Type {
		case "agent.session.status":
			assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
		case "workspace.execution.run":
			var liveRun sqlite.ExecutionRunRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveRun); err != nil {
				t.Fatalf("decode second session execution run payload: %v", err)
			}
			if liveRun.RunID != secondDetail.Run.RunID || liveRun.Status != secondDetail.Run.Status || liveRun.Summary != secondDetail.Run.Summary {
				t.Fatalf("unexpected second session execution run live payload %+v / detail %+v", liveRun, secondDetail.Run)
			}
			assertSessionExecutionPromptContextEnvelope(t, liveRun.VerificationJSON, "agent.session.execution.run.sync")
		case "workspace.execution.step":
			var liveStep sqlite.ExecutionStepRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveStep); err != nil {
				t.Fatalf("decode second session execution step payload: %v", err)
			}
			if liveStep.StepID != secondStepRecord.StepID || liveStep.Status != secondStepRecord.Status || liveStep.Title != secondStepRecord.Title {
				t.Fatalf("unexpected second session execution step live payload %+v / detail %+v", liveStep, secondStepRecord)
			}
			assertSessionExecutionPromptContextEnvelope(t, liveStep.VerificationJSON, "agent.session.execution.step.sync")
		}
	}
	if secondStatusPersisted.EventID == firstStatusPersisted.EventID || secondStatusPersisted.IngestSeq <= firstStatusPersisted.IngestSeq {
		t.Fatalf("expected repeated session.status execution mirror to advance, first=%+v second=%+v", firstStatusPersisted, secondStatusPersisted)
	}
	if secondRunPersisted.EventID == firstRunPersisted.EventID || secondRunPersisted.IngestSeq <= firstRunPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution run mirror to advance, first=%+v second=%+v", firstRunPersisted, secondRunPersisted)
	}
	if secondStepPersisted.EventID == firstStepPersisted.EventID || secondStepPersisted.IngestSeq <= firstStepPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution step mirror to advance, first=%+v second=%+v", firstStepPersisted, secondStepPersisted)
	}
}

func assertSessionExecutionPromptContextEnvelope(t *testing.T, verification map[string]any, wantSurface string) {
	t.Helper()
	envelope, ok := verification["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected session execution prompt context envelope, got %+v", verification)
	}
	if got := envelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected session execution context contract: %v", got)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_execution_write" {
		t.Fatalf("unexpected session execution context kind: %v", got)
	}
	if got := envelope["surface"]; got != wantSurface {
		t.Fatalf("unexpected session execution context surface: got %v want %s", got, wantSurface)
	}
	if got := envelope["origin"]; got != "server_session_projection" {
		t.Fatalf("unexpected session execution context origin: %v", got)
	}
	if got := envelope["principal_type"]; got != "agent" {
		t.Fatalf("unexpected session execution context principal type: %+v", envelope)
	}
	if got := envelope["principal_id"]; got != "agent-a" {
		t.Fatalf("unexpected session execution context principal id: %+v", envelope)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("session execution context must not claim daemon convergence: %+v", envelope)
	}
}

func TestAgentSessionLifecycleRuntimeEventCarriesPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-session-lifecycle-context", "agent", "agent-session-context")

	const (
		workspaceID = "ws-session-lifecycle-context"
		agentID     = "agent-session-context"
		sessionID   = "sess-session-lifecycle-context"
	)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Lifecycle Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, context.Background(), store, workspaceID)
	if err := store.RegisterAgent(context.Background(), sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Session Context Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "starting context-aware work",
	})
	if err != nil {
		t.Fatalf("marshal session start params: %v", err)
	}
	if _, rpcErr := h.agentSessionStart(ctx, raw); rpcErr != nil {
		t.Fatalf("agentSessionStart rpc error: %+v", rpcErr)
	}

	persisted := mustRuntimeEvent(t, context.Background(), store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStart,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	live := nextEventOfType(t, ch, "agent.session.start")
	assertLiveEventMirrorsRuntimeEvent(t, live, persisted, "agent.session.start")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), persisted.PayloadJSON)
	assertSessionLifecyclePromptContext(t, persisted.PayloadJSON, "agent.session.start", "server_rpc", workspaceID, "agent", agentID)

	updates, err := store.ListAgentUpdatesAfter(context.Background(), workspaceID, "", "", 10)
	if err != nil {
		t.Fatalf("list session updates: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected one session update, got %+v", updates)
	}
	assertSessionLifecyclePromptContext(t, updates[0].PayloadJSON, "agent.session.start", "server_rpc", workspaceID, "agent", agentID)
}

func TestAgentSessionTakeoverRuntimeEventCarriesPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID        = "ws-session-takeover-context"
		sourceAgentID      = "agent-session-takeover-a"
		successorAgentID   = "agent-session-takeover-b"
		sourceSessionID    = "sess-session-takeover-a"
		successorSessionID = "sess-session-takeover-b"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Takeover Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{sourceAgentID, successorAgentID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register %s: %v", agentID, err)
		}
	}

	startRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgentID,
		Summary:     "source starts work",
	})
	if err != nil {
		t.Fatalf("marshal start params: %v", err)
	}
	if _, rpcErr := h.agentSessionStart(testAuthContext(workspaceID, "agent", sourceAgentID), startRaw); rpcErr != nil {
		t.Fatalf("agentSessionStart rpc error: %+v", rpcErr)
	}
	statusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgentID,
		Status:      model.SessionStatusHandoffPending,
		Summary:     "handoff to successor",
		HandoffTo:   successorAgentID,
	})
	if err != nil {
		t.Fatalf("marshal status params: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(testAuthContext(workspaceID, "agent", sourceAgentID), statusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus rpc error: %+v", rpcErr)
	}

	takeoverRaw, err := json.Marshal(agentSessionTakeoverParams{
		WorkspaceID:        workspaceID,
		SessionID:          sourceSessionID,
		TakeoverAgentID:    successorAgentID,
		Summary:            "successor takes over",
		SuccessorSessionID: successorSessionID,
	})
	if err != nil {
		t.Fatalf("marshal takeover params: %v", err)
	}
	if _, rpcErr := h.agentSessionTakeover(testAuthContext(workspaceID, "agent", successorAgentID), takeoverRaw); rpcErr != nil {
		t.Fatalf("agentSessionTakeover rpc error: %+v", rpcErr)
	}
	persisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    sourceSessionID,
		Limit:       10,
	})
	assertSessionLifecyclePromptContext(t, persisted.PayloadJSON, "agent.session.takeover", "server_rpc", workspaceID, "agent", successorAgentID)
}

func assertSessionLifecyclePromptContext(t *testing.T, payloadJSON, wantSurface, wantOrigin, wantWorkspaceID, wantPrincipalType, wantPrincipalID string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, payloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected session prompt_context_envelope in payload, got %+v", payload)
	}
	assertSessionLifecyclePromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertSessionLifecyclePromptContextField(t, envelope, "context_kind", "authority_bearing_session_write")
	assertSessionLifecyclePromptContextField(t, envelope, "surface", wantSurface)
	assertSessionLifecyclePromptContextField(t, envelope, "origin", wantOrigin)
	assertSessionLifecyclePromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertSessionLifecyclePromptContextField(t, envelope, "principal_type", wantPrincipalType)
	assertSessionLifecyclePromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertSessionLifecyclePromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertSessionLifecyclePromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertSessionLifecyclePromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertSessionLifecyclePromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertSessionLifecyclePromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()
	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("session prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("session prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}

func TestAgentSessionStatusMirrorsNewPersistedRowsForSessionMemoryAndQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-session-sidecars-repeat",
		PrincipalType: "agent",
		PrincipalID:   "agent-a",
	})

	const (
		workspaceID = "ws-session-sidecars-repeat"
		sessionID   = "sess-session-sidecars-repeat"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Sidecars Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent-a: %v", err)
	}
	for _, reviewerID := range []string{"reviewer-a", "reviewer-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     reviewerID,
			OwnerUserID: "developer",
			DisplayName: reviewerID,
		}); err != nil {
			t.Fatalf("register %s: %v", reviewerID, err)
		}
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	memoryFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	}
	runFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "session:" + sessionID,
		Limit:       10,
	}
	stepFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		Limit:       20,
	}
	firstStatusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Summary:     "Hand-off pending to reviewer-a",
		Status:      model.SessionStatusHandoffPending,
		HandoffTo:   "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal first status params: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(ctx, firstStatusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus first rpc error: %+v", rpcErr)
	}
	firstMemoryPersisted := mustRuntimeEvent(t, ctx, store, memoryFilter)

	handoffQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "HANDOFF",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list handoff queue after first session status: %v", err)
	}
	if len(handoffQueues) != 1 {
		t.Fatalf("expected one handoff queue after first session status, got %+v", handoffQueues)
	}
	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    handoffQueues[0].QueueID,
		Limit:       10,
	}
	firstQueuePersisted := mustRuntimeEvent(t, ctx, store, queueFilter)

	statusFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	}
	firstStatusPersisted := mustRuntimeEvent(t, ctx, store, statusFilter)
	firstClaimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	})
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: firstStatusPersisted, Type: "agent.session.status"},
		runtimeEventExpectation{Event: firstMemoryPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: firstClaimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: firstQueuePersisted, Type: "workspace.ops.updated"},
	)
	for i, expectation := range ordered {
		if strings.HasPrefix(expectation.Type, "workspace.ops.") || expectation.Type == "workspace.claim.written" {
			continue
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}
	firstRunPersisted := mustRuntimeEvent(t, ctx, store, runFilter)
	firstStepPersisted := mustRuntimeEvent(t, ctx, store, stepFilter)
	firstDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get first sidecar execution detail: %v", err)
	}
	firstStepRecord := mustExecutionStepFromDetail(t, firstDetail, firstStepPersisted.EntityID)
	firstRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, firstRunLive, firstRunPersisted, "workspace.execution.run")
	firstStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, firstStepLive, firstStepPersisted, "workspace.execution.step")
	var firstRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(firstRunLive.PayloadJSON), &firstRunEnvelope); err != nil {
		t.Fatalf("decode first sidecar execution run payload: %v", err)
	}
	if firstRunEnvelope.RunID != firstDetail.Run.RunID || firstRunEnvelope.Status != firstDetail.Run.Status || firstRunEnvelope.Summary != firstDetail.Run.Summary {
		t.Fatalf("unexpected first sidecar execution run payload %+v / detail %+v", firstRunEnvelope, firstDetail.Run)
	}
	var firstStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(firstStepLive.PayloadJSON), &firstStepEnvelope); err != nil {
		t.Fatalf("decode first sidecar execution step payload: %v", err)
	}
	if firstStepEnvelope.StepID != firstStepRecord.StepID || firstStepEnvelope.Status != firstStepRecord.Status || firstStepEnvelope.Title != firstStepRecord.Title {
		t.Fatalf("unexpected first sidecar execution step payload %+v / detail %+v", firstStepEnvelope, firstStepRecord)
	}

	seenMemoryEvents := snapshotRuntimeEventIDs(t, ctx, store, memoryFilter)
	seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	})
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	seenStatusEvents := snapshotRuntimeEventIDs(t, ctx, store, statusFilter)
	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, stepFilter)

	secondStatusRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		Summary:     "Hand-off pending to reviewer-b",
		Status:      model.SessionStatusHandoffPending,
		HandoffTo:   "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal second status params: %v", err)
	}
	if _, rpcErr := h.agentSessionStatus(ctx, secondStatusRaw); rpcErr != nil {
		t.Fatalf("agentSessionStatus second rpc error: %+v", rpcErr)
	}
	secondMemoryPersisted := mustNewRuntimeEvent(t, ctx, store, memoryFilter, seenMemoryEvents)
	secondClaimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	}, seenClaimEvents)
	secondQueuePersisted := mustNewRuntimeEvent(t, ctx, store, queueFilter, seenQueueEvents)
	secondStatusPersisted := mustNewRuntimeEvent(t, ctx, store, statusFilter, seenStatusEvents)
	ordered, liveEvents = nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: secondStatusPersisted, Type: "agent.session.status"},
		runtimeEventExpectation{Event: secondMemoryPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: secondClaimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: secondQueuePersisted, Type: "workspace.ops.updated"},
	)
	for i, expectation := range ordered {
		if strings.HasPrefix(expectation.Type, "workspace.ops.") || expectation.Type == "workspace.claim.written" {
			continue
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
	}
	if secondMemoryPersisted.EventID == firstMemoryPersisted.EventID || secondMemoryPersisted.IngestSeq <= firstMemoryPersisted.IngestSeq {
		t.Fatalf("expected repeated session memory event to mirror the newly appended runtime row, got first=%+v second=%+v", firstMemoryPersisted, secondMemoryPersisted)
	}
	if secondQueuePersisted.EventID == firstQueuePersisted.EventID || secondQueuePersisted.IngestSeq <= firstQueuePersisted.IngestSeq {
		t.Fatalf("expected repeated session queue event to mirror the newly appended runtime row, got first=%+v second=%+v", firstQueuePersisted, secondQueuePersisted)
	}
	if secondStatusPersisted.EventID == firstStatusPersisted.EventID || secondStatusPersisted.IngestSeq <= firstStatusPersisted.IngestSeq {
		t.Fatalf("expected repeated session.status to mirror the newly appended runtime row, got first=%+v second=%+v", firstStatusPersisted, secondStatusPersisted)
	}
	secondRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	secondStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	secondDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get second sidecar execution detail: %v", err)
	}
	secondStepRecord := mustExecutionStepFromDetail(t, secondDetail, secondStepPersisted.EntityID)
	secondRunLive := nextEventOfType(t, ch, "workspace.execution.run")
	assertLiveEventMirrorsRuntimeEvent(t, secondRunLive, secondRunPersisted, "workspace.execution.run")
	secondStepLive := nextEventOfType(t, ch, "workspace.execution.step")
	assertLiveEventMirrorsRuntimeEvent(t, secondStepLive, secondStepPersisted, "workspace.execution.step")
	if secondRunPersisted.EventID == firstRunPersisted.EventID || secondRunPersisted.IngestSeq <= firstRunPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution run sidecar to mirror the newly appended runtime row, got first=%+v second=%+v", firstRunPersisted, secondRunPersisted)
	}
	if secondStepPersisted.EventID == firstStepPersisted.EventID || secondStepPersisted.IngestSeq <= firstStepPersisted.IngestSeq {
		t.Fatalf("expected repeated session execution step sidecar to mirror the newly appended runtime row, got first=%+v second=%+v", firstStepPersisted, secondStepPersisted)
	}
	var secondRunEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(secondRunLive.PayloadJSON), &secondRunEnvelope); err != nil {
		t.Fatalf("decode second sidecar execution run payload: %v", err)
	}
	if secondRunEnvelope.RunID != secondDetail.Run.RunID || secondRunEnvelope.Status != secondDetail.Run.Status || secondRunEnvelope.Summary != secondDetail.Run.Summary {
		t.Fatalf("unexpected second sidecar execution run payload %+v / detail %+v", secondRunEnvelope, secondDetail.Run)
	}
	var secondStepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(secondStepLive.PayloadJSON), &secondStepEnvelope); err != nil {
		t.Fatalf("decode second sidecar execution step payload: %v", err)
	}
	if secondStepEnvelope.StepID != secondStepRecord.StepID || secondStepEnvelope.Status != secondStepRecord.Status || secondStepEnvelope.Title != secondStepRecord.Title {
		t.Fatalf("unexpected second sidecar execution step payload %+v / detail %+v", secondStepEnvelope, secondStepRecord)
	}
	var queueEnvelope sqlite.OperatorQueueRecord
	for i, expectation := range ordered {
		if expectation.Type != "workspace.ops.updated" {
			continue
		}
		if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &queueEnvelope); err != nil {
			t.Fatalf("decode second session queue payload: %v", err)
		}
		break
	}
	if queueEnvelope.QueueID == "" {
		t.Fatalf("expected second session queue live payload to be present")
	}
	if queueEnvelope.QueueID != handoffQueues[0].QueueID || queueEnvelope.Summary != "Hand-off pending to reviewer-b" || queueEnvelope.AssignedTo != "reviewer-b" {
		t.Fatalf("unexpected repeated session queue live payload %+v", queueEnvelope)
	}
}

func TestAgentSessionDecisionNeededAndEndPublishJournalBackedAliasMirrors(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   "ws-session-aliases",
		PrincipalType: "agent",
		PrincipalID:   "agent-a",
	})

	const (
		workspaceID = "ws-session-aliases"
		sessionID   = "sess-session-aliases"
		taskID      = "task-session-aliases"
	)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Alias Mirrors",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "agent-a",
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
		AgentID:     "agent-a",
		Summary:     "claim before decision aliases session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	decisionRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		AgentID:            "agent-a",
		TaskID:             taskID,
		Summary:            "Need operator approval",
		DecisionNeededFrom: "developer",
		DecisionType:       "approval",
	})
	if err != nil {
		t.Fatalf("marshal decision-needed params: %v", err)
	}
	if _, rpcErr := h.agentSessionDecisionNeeded(ctx, decisionRaw); rpcErr != nil {
		t.Fatalf("agentSessionDecisionNeeded rpc error: %+v", rpcErr)
	}
	decisionLive := nextEventOfType(t, ch, "agent.session.decision_needed")
	decisionPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventDecisionNeeded,
		EntityType:  "agent_session",
		EntityID:    sessionID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, decisionLive, decisionPersisted, "agent.session.decision_needed")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, decisionLive.PayloadJSON), decisionPersisted.PayloadJSON)

	endRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Session complete",
	})
	if err != nil {
		t.Fatalf("marshal end params: %v", err)
	}
	if _, rpcErr := h.agentSessionEnd(ctx, endRaw); rpcErr != nil {
		t.Fatalf("agentSessionEnd rpc error: %+v", rpcErr)
	}
	endLive := nextEventOfType(t, ch, "agent.session.end")
	endPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventEnd,
		EntityType:  "agent_session",
		EntityID:    sessionID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, endLive, endPersisted, "agent.session.end")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, endLive.PayloadJSON), endPersisted.PayloadJSON)
}

func TestAgentSessionQueueResolvedMirrorsNewPersistedRowsForRepeatedSync(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-session-queue-resolved-repeat"
		sessionID   = "sess-session-queue-resolved-repeat"
		taskID      = "task-session-queue-resolved-repeat"
		agentID     = "agent-session-queue-resolved-repeat"
	)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Queue Resolved Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
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
		Summary:     "claim before queue resolved repeat session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	blockedState := sqlite.AgentSessionStateRecord{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		UpdateType:  model.SessionEventBlocked,
		Status:      model.SessionStatusBlocked,
		Summary:     "Waiting on human approval",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve rollout"}},
	}
	h.syncSessionOperatorQueue(ctx, blockedState)
	firstUpdatedLive := nextEventOfType(t, ch, "workspace.ops.updated")

	blockerQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "BLOCKER",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocker queues after blocked sync: %v", err)
	}
	if len(blockerQueues) != 1 {
		t.Fatalf("expected one blocker queue after blocked sync, got %+v", blockerQueues)
	}
	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
		Limit:       10,
	}
	firstUpdatedPersisted := mustRuntimeEvent(t, ctx, store, queueFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstUpdatedLive, firstUpdatedPersisted, "workspace.ops.updated")

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
		Limit:       10,
	}
	firstClearedState := sqlite.AgentSessionStateRecord{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		UpdateType:  model.SessionEventKeepalive,
		Status:      model.SessionStatusActive,
		Summary:     "Back in progress",
	}
	h.syncSessionOperatorQueue(ctx, firstClearedState)
	firstResolvedLive := nextEventOfType(t, ch, "workspace.ops.resolved")
	firstResolvedPersisted := mustRuntimeEvent(t, ctx, store, resolvedFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstResolvedLive, firstResolvedPersisted, "workspace.ops.resolved")

	secondBlockedState := blockedState
	secondBlockedState.Summary = "Still waiting on final approval"
	secondBlockedState.BlockedOn = []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve final rollout"}}
	h.syncSessionOperatorQueue(ctx, secondBlockedState)
	_ = nextEventOfType(t, ch, "workspace.ops.updated")

	seenResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	secondClearedState := sqlite.AgentSessionStateRecord{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		UpdateType:  model.SessionEventKeepalive,
		Status:      model.SessionStatusActive,
		Summary:     "Cleared after second approval",
	}
	h.syncSessionOperatorQueue(ctx, secondClearedState)
	secondResolvedLive := nextEventOfType(t, ch, "workspace.ops.resolved")
	secondResolvedPersisted := mustNewRuntimeEvent(t, ctx, store, resolvedFilter, seenResolvedEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondResolvedLive, secondResolvedPersisted, "workspace.ops.resolved")
	if secondResolvedPersisted.EventID == firstResolvedPersisted.EventID || secondResolvedPersisted.IngestSeq <= firstResolvedPersisted.IngestSeq {
		t.Fatalf("expected repeated session queue resolution to mirror the newly appended runtime row, got first=%+v second=%+v", firstResolvedPersisted, secondResolvedPersisted)
	}
	var queueEnvelope sqlite.OperatorQueueRecord
	if err := json.Unmarshal([]byte(secondResolvedLive.PayloadJSON), &queueEnvelope); err != nil {
		t.Fatalf("decode repeated session resolved payload: %v", err)
	}
	if queueEnvelope.QueueID != blockerQueues[0].QueueID || queueEnvelope.Status != "RESOLVED" || queueEnvelope.Resolution != "cleared_by_session_keepalive" {
		t.Fatalf("unexpected repeated session resolved payload %+v", queueEnvelope)
	}
}

func countServerAgentUpdatesForA310(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_updates WHERE workspace_id = ?`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count agent_updates for %s: %v", workspaceID, err)
	}
	return count
}

func countServerSessionRuntimeEventsForA310(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sessionID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, sessionID, err)
	}
	return len(events)
}
