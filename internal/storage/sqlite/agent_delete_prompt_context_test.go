package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestDeleteAgentWithEventRecordsPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-agent-delete-storage-evidence"
	const actorID = "operator-a"
	const agentID = "agent-delete-storage-evidence"

	seedAgentDeletePromptContextWorkspace(t, ctx, store, workspaceID, agentID)

	event, err := store.DeleteAgentWithEvent(ctx, sqlite.AgentDeleteInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildAgentLifecyclePromptContextEnvelope(
			"agent.delete",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "agent.delete",
	})
	if err != nil {
		t.Fatalf("delete agent with event: %v", err)
	}
	if event.EventType != "agent.deleted" || event.EntityType != "agent" || event.EntityID != agentID {
		t.Fatalf("unexpected event %+v", event)
	}
	if _, err := store.GetAgent(ctx, workspaceID, agentID); err == nil {
		t.Fatal("expected agent row to be deleted")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.deleted",
		EntityType:  "agent",
		EntityID:    agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent.deleted events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted delete event, got %+v", events)
	}
	payload := decodeAgentDeletePayload(t, event.PayloadJSON)
	assertAgentDeletePromptContext(t, payload, workspaceID, "human", actorID, agentID, actorID)
	if got := payload["display_name"]; got != "Agent Delete Storage Evidence" {
		t.Fatalf("display_name = %+v", got)
	}
}

func TestDeleteAgentWithEventRejectsForgedPromptPrincipal(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-agent-delete-forged-principal"
	const actorID = "operator-a"
	const forgedPrincipalID = "operator-b"
	const agentID = "agent-delete-forged-principal"

	seedAgentDeletePromptContextWorkspace(t, ctx, store, workspaceID, agentID)

	_, err := store.DeleteAgentWithEvent(ctx, sqlite.AgentDeleteInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildAgentLifecyclePromptContextEnvelope(
			"agent.delete",
			"server_rpc",
			workspaceID,
			"human",
			forgedPrincipalID,
		),
		PromptContextSurface: "agent.delete",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged principal_id rejection, got %v", err)
	}
	if _, err := store.GetAgent(ctx, workspaceID, agentID); err != nil {
		t.Fatalf("expected forged delete rollback to leave agent row: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.deleted",
		EntityType:  "agent",
		EntityID:    agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list forged delete events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected forged delete not to record runtime event, got %+v", events)
	}
}

func TestDeleteAgentWithEventRequiresPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-agent-delete-missing-prompt-context"
	const agentID = "agent-delete-missing-prompt-context"

	seedAgentDeletePromptContextWorkspace(t, ctx, store, workspaceID, agentID)

	_, err := store.DeleteAgentWithEvent(ctx, sqlite.AgentDeleteInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ActorID:     "operator-a",
		ActorType:   "human",
	})
	if err == nil || !strings.Contains(err.Error(), "prompt_context_envelope") {
		t.Fatalf("expected missing prompt_context_envelope rejection, got %v", err)
	}
	if _, err := store.GetAgent(ctx, workspaceID, agentID); err != nil {
		t.Fatalf("expected missing prompt context reject to leave agent row: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.deleted",
		EntityType:  "agent",
		EntityID:    agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list missing prompt-context delete events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected missing prompt context not to record runtime event, got %+v", events)
	}
}

func seedAgentDeletePromptContextWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("ensure local workspace authority: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Agent Delete Storage Evidence",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
}

func decodeAgentDeletePayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode agent delete payload: %v", err)
	}
	return payload
}

func assertAgentDeletePromptContext(t *testing.T, payload map[string]any, workspaceID, principalType, principalID, agentID, actorID string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload %+v", payload)
	}
	for key, want := range map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_agent_lifecycle_write",
		"surface":                            "agent.delete",
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
}
