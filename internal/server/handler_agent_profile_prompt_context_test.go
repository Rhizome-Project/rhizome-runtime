package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentProfileUpdateRecordsActorBoundPromptContextEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-agent-profile-update-evidence"
		agentID     = "agent-profile-update-evidence"
		actorID     = "operator-profile"
	)
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedAgentProfileUpdateWorkspace(t, ctx, store, workspaceID, actorID, agentID)
	authority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	result, rpcErr := h.agentProfileUpdate(ctx, mustJSONRaw(agentProfileUpdateParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		Bio:            "Observer profile without direct participation.",
		Specialization: "meta-analysis reviewer",
		Tags:           []string{"observer", "stability"},
		Metadata: map[string]any{
			"default_work_mode": "observer",
		},
	}))
	if rpcErr != nil {
		t.Fatalf("agent.profile.update rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok || payload["status"] != "UPDATED" || payload["event"] == nil || payload["profile"] == nil {
		t.Fatalf("unexpected agent.profile.update result %+v", result)
	}

	runtimeEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.profile.updated",
		EntityType:  "agent_profile",
		EntityID:    agentID,
		AgentID:     agentID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "agent.profile.updated"), runtimeEvent, "agent.profile.updated")
	assertServerRuntimeEventAuthorityMetadata(t, runtimeEvent, authority)
	assertAgentProfileRuntimePromptContext(t, decodeEventPayloadMap(t, runtimeEvent.PayloadJSON), workspaceID, "human", actorID, agentID, actorID)
}

func TestAgentProfileUpdateFailsClosedOnActorMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-agent-profile-actor-mismatch"
		agentID     = "agent-profile-actor-mismatch"
	)
	ctx := testAuthContext(workspaceID, "human", "operator-a")
	seedAgentProfileUpdateWorkspace(t, ctx, store, workspaceID, "operator-a", agentID)

	result, rpcErr := h.agentProfileUpdate(ctx, mustJSONRaw(agentProfileUpdateParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        "operator-b",
		Specialization: "builder",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched actor_id to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match actor_id" {
		t.Fatalf("unexpected mismatch error %+v", rpcErr)
	}
	assertAgentProfileUnchangedAndNoRuntimeEvent(t, ctx, store, workspaceID, agentID)
}

func TestAgentProfileUpdateRejectsAgentPrincipalUpdatingAnotherAgent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-agent-profile-agent-target-mismatch"
		actorAgent  = "agent-profile-actor"
		targetAgent = "agent-profile-target"
	)
	ctx := testAuthContext(workspaceID, "agent", actorAgent)
	seedAgentProfileUpdateWorkspace(t, ctx, store, workspaceID, "developer", actorAgent, targetAgent)

	result, rpcErr := h.agentProfileUpdate(ctx, mustJSONRaw(agentProfileUpdateParams{
		WorkspaceID:    workspaceID,
		AgentID:        targetAgent,
		ActorID:        actorAgent,
		Specialization: "builder",
	}))
	if rpcErr == nil {
		t.Fatal("expected agent principal target mismatch to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on target mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected target mismatch error %+v", rpcErr)
	}
	assertAgentProfileUnchangedAndNoRuntimeEvent(t, ctx, store, workspaceID, targetAgent)
}

func seedAgentProfileUpdateWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string, agentIDs ...string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: actorID,
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}

func assertAgentProfileRuntimePromptContext(t *testing.T, payload map[string]any, workspaceID, principalType, principalID, agentID, actorID string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent profile prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	for key, want := range map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_agent_profile_write",
		"surface":                            "agent.profile.update",
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"agent_id":                           agentID,
		"actor_id":                           actorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	} {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
	if got, ok := payload["autonomous_execution_allowed_after"].(bool); !ok || got {
		t.Fatalf("expected runtime payload to show observer profile disables autonomous work selection, got %+v", payload)
	}
}

func assertAgentProfileUnchangedAndNoRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	profile, err := store.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil {
		t.Fatalf("get profile after reject: %v", err)
	}
	if profile.Specialization != "" || len(profile.Tags) != 0 {
		t.Fatalf("rejected profile update mutated storage: %+v", profile)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.profile.updated",
		EntityType:  "agent_profile",
		EntityID:    agentID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list profile runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("rejected profile update recorded runtime events: %+v", events)
	}
}
