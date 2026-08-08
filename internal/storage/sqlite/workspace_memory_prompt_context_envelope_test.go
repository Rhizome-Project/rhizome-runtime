package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPromptContextEnvelopeCarriesLifecycleEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-prompt-context-lifecycle"
		agentID     = "agent-memory-prompt-context-lifecycle"
	)
	createWorkspaceMemoryPromptContextFixture(t, ctx, store, workspaceID, agentID)

	record, event, effects, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		MemoryID:              "memory-prompt-context-lifecycle",
		WorkspaceID:           workspaceID,
		MemoryType:            "DECISION",
		Title:                 "Prompt context memory lifecycle",
		Body:                  "Workspace memory lifecycle events should carry operation-bound prompt context.",
		Summary:               "Memory prompt context lifecycle.",
		AgentID:               agentID,
		SourceKind:            "manual",
		SourceID:              "developer",
		Tags:                  []string{"memory", "prompt_context"},
		PromptContextEnvelope: sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.write", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("record workspace memory with prompt context: %v", err)
	}
	assertWorkspaceMemoryRuntimePromptContext(t, event, "workspace.memory.write", workspaceID, record.MemoryID, "human", "developer", map[string]string{
		"memory_type": record.MemoryType,
		"source_kind": record.SourceKind,
		"source_id":   record.SourceID,
		"agent_id":    agentID,
		"actor_type":  "agent",
		"actor_id":    agentID,
	})
	assertPromotedKnowledgeClaimEffectHasNoPromptContext(t, effects, "record")

	archived, archiveEvent, archiveEffects, err := store.ArchiveWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID:           workspaceID,
		MemoryID:              record.MemoryID,
		ArchivedBy:            "developer",
		Reason:                "prompt context archive parity",
		PromptContextEnvelope: sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.remove", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("archive workspace memory with prompt context: %v", err)
	}
	if archived.ArchivedAt == nil {
		t.Fatalf("expected archived memory, got %+v", archived)
	}
	assertWorkspaceMemoryRuntimePromptContext(t, archiveEvent, "workspace.memory.remove", workspaceID, record.MemoryID, "human", "developer", map[string]string{
		"memory_type": record.MemoryType,
		"source_kind": record.SourceKind,
		"source_id":   record.SourceID,
		"agent_id":    agentID,
		"actor_type":  "operator",
		"actor_id":    "developer",
		"archived_by": "developer",
	})
	assertPromotedKnowledgeClaimEffectHasNoPromptContext(t, archiveEffects, "archive")

	restored, restoreEvent, restoreEffects, err := store.RestoreWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID:           workspaceID,
		MemoryID:              record.MemoryID,
		RestoredBy:            "developer",
		RecoveryReason:        "prompt_context_restore_parity",
		PromptContextEnvelope: sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.restore", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("restore workspace memory with prompt context: %v", err)
	}
	if restored.ArchivedAt != nil {
		t.Fatalf("expected restored memory, got %+v", restored)
	}
	assertWorkspaceMemoryRuntimePromptContext(t, restoreEvent, "workspace.memory.restore", workspaceID, record.MemoryID, "human", "developer", map[string]string{
		"memory_type": record.MemoryType,
		"source_kind": record.SourceKind,
		"source_id":   record.SourceID,
		"agent_id":    agentID,
		"actor_type":  "operator",
		"actor_id":    "developer",
		"restored_by": "developer",
	})
	assertPromotedKnowledgeClaimEffectHasNoPromptContext(t, restoreEffects, "restore")
}

func TestWorkspaceMemoryPromptContextEnvelopeRejectsForgedBindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		envelope map[string]any
	}{
		{
			name:     "wrong surface",
			envelope: sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.remove", "server_rpc", "ws-memory-prompt-context-forged", "human", "developer"),
		},
		{
			name:     "wrong workspace",
			envelope: sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.write", "server_rpc", "ws-memory-prompt-context-other", "human", "developer"),
		},
		{
			name: "wrong memory id",
			envelope: func() map[string]any {
				envelope := sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.write", "server_rpc", "ws-memory-prompt-context-forged", "human", "developer")
				envelope["memory_id"] = "memory-forged-other"
				return envelope
			}(),
		},
		{
			name: "nested wrong workspace",
			envelope: func() map[string]any {
				envelope := sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.write", "server_rpc", "ws-memory-prompt-context-forged", "human", "developer")
				envelope["nested"] = map[string]any{
					"prompt_context_envelope": sqlite.BuildWorkspaceMemoryPromptContextEnvelope("workspace.memory.write", "server_rpc", "ws-memory-prompt-context-other", "human", "developer"),
				}
				return envelope
			}(),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			const workspaceID = "ws-memory-prompt-context-forged"
			createWorkspaceMemoryPromptContextFixture(t, ctx, store, workspaceID, "agent-memory-prompt-context-forged")

			_, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
				MemoryID:              "memory-forged",
				WorkspaceID:           workspaceID,
				MemoryType:            "NOTE",
				Title:                 "Forged memory prompt context",
				Body:                  "Forged workspace-memory prompt context should fail closed.",
				SourceKind:            "manual",
				SourceID:              "developer",
				PromptContextEnvelope: tt.envelope,
			})
			if err == nil {
				t.Fatal("expected forged workspace memory prompt context to fail")
			}
			if got := countWorkspaceMemoryRowsByID(t, ctx, store, workspaceID, "memory-forged"); got != 0 {
				t.Fatalf("expected no workspace_memory row after forged prompt context reject, got %d", got)
			}
			if got := countWorkspaceMemoryRuntimeEventsByID(t, ctx, store, workspaceID, "memory-forged", "workspace_memory.recorded"); got != 0 {
				t.Fatalf("expected no workspace_memory.recorded event after forged prompt context reject, got %d", got)
			}
		})
	}
}

func createWorkspaceMemoryPromptContextFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if strings.TrimSpace(agentID) != "" {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: "Memory Prompt Context Agent",
		}); err != nil {
			t.Fatalf("register agent: %v", err)
		}
	}
}

func assertWorkspaceMemoryRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, surface, workspaceID, memoryID, principalType, principalID string, extra map[string]string) {
	t.Helper()
	if event.EventID == "" {
		t.Fatal("expected runtime event")
	}
	payload := decodeWorkspaceMemoryPromptPayload(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_memory_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"memory_id":                          memoryID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		if got, _ := envelope[key].(string); got != want {
			t.Fatalf("expected envelope %s=%q, got %q in %+v", key, want, got, envelope)
		}
	}
	if got, _ := payload["workspace_id"].(string); got != workspaceID {
		t.Fatalf("expected payload workspace_id=%q, got %+v", workspaceID, payload)
	}
	if got, _ := payload["memory_id"].(string); got != memoryID {
		t.Fatalf("expected payload memory_id=%q, got %+v", memoryID, payload)
	}
}

func assertNoRuntimePromptContextEnvelope(t *testing.T, payloadJSON string) {
	t.Helper()
	payload := decodeWorkspaceMemoryPromptPayload(t, payloadJSON)
	if _, ok := payload["prompt_context_envelope"]; ok {
		t.Fatalf("did not expect prompt_context_envelope in derived event payload %+v", payload)
	}
}

func assertPromotedKnowledgeClaimEffectHasNoPromptContext(t *testing.T, effects *sqlite.PromotedKnowledgeClaimSyncEffects, phase string) {
	t.Helper()
	if effects == nil || effects.ClaimEvent == nil || strings.TrimSpace(effects.ClaimEvent.EventID) == "" {
		t.Fatalf("expected promoted knowledge claim %s effect, got %+v", phase, effects)
	}
	assertNoRuntimePromptContextEnvelope(t, effects.ClaimEvent.PayloadJSON)
}

func decodeWorkspaceMemoryPromptPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	payload := map[string]any{}
	if strings.TrimSpace(payloadJSON) == "" {
		return payload
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime payload: %v", err)
	}
	return payload
}

func countWorkspaceMemoryRowsByID(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_memory WHERE workspace_id = ? AND memory_id = ?`, workspaceID, memoryID).Scan(&count); err != nil {
		t.Fatalf("count workspace_memory rows: %v", err)
	}
	return count
}

func countWorkspaceMemoryRuntimeEventsByID(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID, eventType string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND entity_id = ? AND event_type = ?`, workspaceID, memoryID, eventType).Scan(&count); err != nil {
		t.Fatalf("count workspace_memory runtime events: %v", err)
	}
	return count
}
