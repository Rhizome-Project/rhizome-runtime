package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestLimitsGroupMutationsRecordActorBoundPromptContextEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-limits-group-evidence"
	const actorID = "operator-a"
	const groupID = "grp-budget-alpha"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedLimitGroupMutationWorkspace(t, ctx, store, workspaceID, actorID)
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: actorID,
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register %s: %v", agentID, err)
		}
	}
	authority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if _, rpcErr := h.limitsGroupCreate(ctx, mustJSONRaw(limitsGroupCreateParams{
		WorkspaceID:      workspaceID,
		GroupID:          groupID,
		Title:            "Budget Alpha",
		OwnerName:        "Operations",
		SubscriptionTier: "pro",
		DailyLimit:       100,
		WeeklyLimit:      700,
		ActorID:          actorID,
	})); rpcErr != nil {
		t.Fatalf("limits.group.create rpc error: %+v", rpcErr)
	}
	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "limits.group.created",
		EntityType:  "limit_group",
		EntityID:    groupID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "limits.group.created"), createRuntime, "limits.group.created")
	assertServerRuntimeEventAuthorityMetadata(t, createRuntime, authority)
	createPayload := decodeEventPayloadMap(t, createRuntime.PayloadJSON)
	assertLimitGroupPromptContext(t, createPayload, "limits.group.create", workspaceID, "human", actorID, groupID, actorID)

	if _, rpcErr := h.limitsGroupUpdate(ctx, mustJSONRaw(limitsGroupUpdateParams{
		WorkspaceID: workspaceID,
		GroupID:     groupID,
		Title:       "Budget Alpha Updated",
		AgentIDs:    []string{"agent-b", "agent-a", "agent-a", " "},
		ActorID:     actorID,
	})); rpcErr != nil {
		t.Fatalf("limits.group.update rpc error: %+v", rpcErr)
	}
	updateRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "limits.group.updated",
		EntityType:  "limit_group",
		EntityID:    groupID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "limits.group.updated"), updateRuntime, "limits.group.updated")
	assertServerRuntimeEventAuthorityMetadata(t, updateRuntime, authority)
	updatePayload := decodeEventPayloadMap(t, updateRuntime.PayloadJSON)
	assertLimitGroupPromptContext(t, updatePayload, "limits.group.update", workspaceID, "human", actorID, groupID, actorID)
	if got := stringSliceFromPayload(t, updatePayload["agent_ids"]); !reflect.DeepEqual(got, []string{"agent-a", "agent-b"}) {
		t.Fatalf("update agent_ids = %+v, want deterministic [agent-a agent-b]", got)
	}
	if got := updatePayload["agent_ids_count"]; got != float64(2) {
		t.Fatalf("update agent_ids_count = %+v, want 2", got)
	}
	group, err := store.GetLimitGroup(ctx, workspaceID, groupID)
	if err != nil {
		t.Fatalf("get updated limit group: %v", err)
	}
	if !reflect.DeepEqual(group.Agents, []string{"agent-a", "agent-b"}) {
		t.Fatalf("persisted group agents = %+v, want deterministic [agent-a agent-b]", group.Agents)
	}

	if _, rpcErr := h.limitsGroupDelete(ctx, mustJSONRaw(limitsGroupDeleteParams{
		WorkspaceID: workspaceID,
		GroupID:     groupID,
		ActorID:     actorID,
	})); rpcErr != nil {
		t.Fatalf("limits.group.delete rpc error: %+v", rpcErr)
	}
	deleteRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "limits.group.deleted",
		EntityType:  "limit_group",
		EntityID:    groupID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "limits.group.deleted"), deleteRuntime, "limits.group.deleted")
	assertServerRuntimeEventAuthorityMetadata(t, deleteRuntime, authority)
	deletePayload := decodeEventPayloadMap(t, deleteRuntime.PayloadJSON)
	assertLimitGroupPromptContext(t, deletePayload, "limits.group.delete", workspaceID, "human", actorID, groupID, actorID)
	if got := stringSliceFromPayload(t, deletePayload["agent_ids"]); !reflect.DeepEqual(got, []string{"agent-a", "agent-b"}) {
		t.Fatalf("delete agent_ids = %+v, want deleted membership evidence", got)
	}
}

func TestLimitsGroupMutationsFailClosedBeforeStorageOnActorMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-limits-group-actor-mismatch"
	ctx := testAuthContext(workspaceID, "human", "operator-a")
	seedLimitGroupMutationWorkspace(t, ctx, store, workspaceID, "operator-a")

	result, rpcErr := h.limitsGroupCreate(ctx, mustJSONRaw(limitsGroupCreateParams{
		WorkspaceID: workspaceID,
		GroupID:     "grp-budget-mismatch",
		Title:       "Budget Mismatch",
		ActorID:     "operator-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched actor to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no create result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match actor_id" {
		t.Fatalf("unexpected mismatch error %+v", rpcErr)
	}
	if _, err := store.GetLimitGroup(ctx, workspaceID, "grp-budget-mismatch"); err == nil {
		t.Fatal("mismatched actor create mutated limit group storage")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "limits.group.created",
		EntityType:  "limit_group",
		EntityID:    "grp-budget-mismatch",
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("mismatched actor create recorded runtime events: %+v", events)
	}
}

func seedLimitGroupMutationWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "system",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("ensure local workspace authority for %s: %v", workspaceID, err)
	}
}

func assertLimitGroupPromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, groupID, actorID string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected limit group prompt_context_envelope in payload, got %+v", payload)
	}
	for key, want := range map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_limit_group_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"group_id":                           groupID,
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
}

func stringSliceFromPayload(t *testing.T, value any) []string {
	t.Helper()
	raw, ok := value.([]any)
	if !ok {
		t.Fatalf("payload value has type %T, want []any", value)
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("payload slice item has type %T, want string", item)
		}
		out = append(out, text)
	}
	return out
}
