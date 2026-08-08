package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentSessionLifecycleCarriesPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-session-prompt-context"
		agentID     = "agent-session-prompt-context"
		sessionID   = "sess-session-prompt-context"
	)
	createSessionPromptContextWorkspace(t, ctx, store, workspaceID, agentID)

	cases := []struct {
		eventType          string
		surface            string
		status             string
		summary            string
		blockedOn          []model.AgentUpdateBlockedRef
		decisionNeededFrom string
	}{
		{eventType: model.SessionEventStart, surface: "agent.session.start", summary: "start work"},
		{eventType: model.SessionEventStatus, surface: "agent.session.status", summary: "status update"},
		{
			eventType: model.SessionEventBlocked,
			surface:   "agent.session.blocked",
			summary:   "blocked on operator",
			blockedOn: []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve scope"}},
		},
		{
			eventType:          model.SessionEventDecisionNeeded,
			surface:            "agent.session.decision_needed",
			summary:            "decision needed",
			decisionNeededFrom: "developer",
		},
		{eventType: model.SessionEventKeepalive, surface: "agent.session.keepalive", summary: "still alive"},
		{eventType: model.SessionEventEnd, surface: "agent.session.end", summary: "done"},
	}

	for _, tc := range cases {
		t.Run(tc.eventType, func(t *testing.T) {
			_, event, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
				EventType:             tc.eventType,
				WorkspaceID:           workspaceID,
				SessionID:             sessionID,
				AgentID:               agentID,
				Status:                tc.status,
				Summary:               tc.summary,
				BlockedOn:             tc.blockedOn,
				DecisionNeededFrom:    tc.decisionNeededFrom,
				PromptContextEnvelope: sqlite.BuildSessionPromptContextEnvelope(tc.surface, "server_rpc", workspaceID, "agent", agentID),
			})
			if err != nil {
				t.Fatalf("record %s: %v", tc.eventType, err)
			}
			assertSessionPromptContextEnvelope(t, event.PayloadJSON, tc.surface, "server_rpc", workspaceID, "agent", agentID)
		})
	}

	updates, err := store.ListAgentUpdatesAfter(ctx, workspaceID, "", "", 20)
	if err != nil {
		t.Fatalf("list agent updates: %v", err)
	}
	if len(updates) != len(cases) {
		t.Fatalf("expected %d session updates, got %+v", len(cases), updates)
	}
	for i, update := range updates {
		assertSessionPromptContextEnvelope(t, update.PayloadJSON, cases[i].surface, "server_rpc", workspaceID, "agent", agentID)
	}
}

func TestAgentSessionTakeoverCarriesPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-session-takeover-prompt-context"
		sourceAgentID      = "agent-session-takeover-source"
		successorAgentID   = "agent-session-takeover-successor"
		sourceSessionID    = "sess-session-takeover-source"
		successorSessionID = "sess-session-takeover-successor"
	)
	createSessionPromptContextWorkspace(t, ctx, store, workspaceID, sourceAgentID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     successorAgentID,
		OwnerUserID: "developer",
		DisplayName: "Session Takeover Successor",
	}); err != nil {
		t.Fatalf("register successor agent: %v", err)
	}

	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgentID,
		Summary:     "source start",
	}); err != nil {
		t.Fatalf("start source session: %v", err)
	}
	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgentID,
		Status:      model.SessionStatusHandoffPending,
		Summary:     "handoff pending",
		HandoffTo:   successorAgentID,
	}); err != nil {
		t.Fatalf("mark handoff pending: %v", err)
	}

	_, event, err := store.TakeOverAgentSessionWithEvent(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:           workspaceID,
		SessionID:             sourceSessionID,
		SuccessorSessionID:    successorSessionID,
		TakeoverAgentID:       successorAgentID,
		Summary:               "take over active session",
		PromptContextEnvelope: sqlite.BuildSessionPromptContextEnvelope("agent.session.takeover", "server_rpc", workspaceID, "agent", successorAgentID),
	})
	if err != nil {
		t.Fatalf("take over session: %v", err)
	}
	assertSessionPromptContextEnvelope(t, event.PayloadJSON, "agent.session.takeover", "server_rpc", workspaceID, "agent", successorAgentID)

	endEvent := mustSessionRuntimeEvent(t, ctx, store, workspaceID, model.SessionEventEnd, sourceSessionID)
	assertSessionPromptContextEnvelope(t, endEvent.PayloadJSON, "agent.session.takeover", "server_rpc", workspaceID, "agent", successorAgentID)
	startEvent := mustSessionRuntimeEvent(t, ctx, store, workspaceID, model.SessionEventStart, successorSessionID)
	assertSessionPromptContextEnvelope(t, startEvent.PayloadJSON, "agent.session.takeover", "server_rpc", workspaceID, "agent", successorAgentID)
}

func TestAgentSessionPromptContextEnvelopeRejectsWrongSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-session-prompt-context-bad"
		agentID     = "agent-session-prompt-context-bad"
	)
	createSessionPromptContextWorkspace(t, ctx, store, workspaceID, agentID)

	_, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:             model.SessionEventStart,
		WorkspaceID:           workspaceID,
		SessionID:             "sess-session-prompt-context-bad",
		AgentID:               agentID,
		Summary:               "bad context",
		PromptContextEnvelope: sqlite.BuildSessionPromptContextEnvelope("session.start", "server_rpc", workspaceID, "agent", agentID),
	})
	if err == nil {
		t.Fatal("expected wrong session prompt context surface to be rejected")
	}
	if !strings.Contains(err.Error(), "unsupported surface") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func createSessionPromptContextWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Session Prompt Context Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
}

func mustSessionRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType, sessionID string) sqlite.RuntimeEventRecord {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session runtime events: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected session runtime event %s/%s", eventType, sessionID)
	}
	return events[0]
}

func assertSessionPromptContextEnvelope(t *testing.T, payloadJSON, wantSurface, wantOrigin, wantWorkspaceID, wantPrincipalType, wantPrincipalID string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode session prompt context payload: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected session prompt_context_envelope in payload, got %+v", payload)
	}
	assertSessionPromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertSessionPromptContextField(t, envelope, "context_kind", "authority_bearing_session_write")
	assertSessionPromptContextField(t, envelope, "surface", wantSurface)
	assertSessionPromptContextField(t, envelope, "origin", wantOrigin)
	assertSessionPromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertSessionPromptContextField(t, envelope, "principal_type", wantPrincipalType)
	assertSessionPromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertSessionPromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertSessionPromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertSessionPromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertSessionPromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertSessionPromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()

	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("session prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("session prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}
