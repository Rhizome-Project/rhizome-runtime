package sqlite_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceDocAndAgentUpdatePromptContextEnvelopeCarriesPrimaryEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-doc-update-prompt-context"
		agentID     = "agent-doc-update-prompt-context"
		docKey      = "runbook"
		updateID    = "update-doc-update-prompt-context"
	)
	createWorkspaceDocUpdatePromptContextFixture(t, ctx, store, workspaceID, agentID)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook v1",
		Content:     "hello world",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("seed workspace doc: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-doc-update-prompt-context",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:" + docKey,
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: workspaceDocPromptSHA("hello world"), Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed doc residency: %v", err)
	}

	putEvent, putInvalidations, err := store.UpsertWorkspaceDocWithEffects(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID:           workspaceID,
		DocKey:                docKey,
		Title:                 "Runbook v2",
		Content:               "hello again",
		UpdatedBy:             agentID,
		PromptContextEnvelope: sqlite.BuildWorkspaceDocPromptContextEnvelope("workspace.doc.put", "server_rpc", workspaceID, "agent", agentID),
	})
	if err != nil {
		t.Fatalf("upsert workspace doc with prompt context: %v", err)
	}
	assertWorkspaceDocUpdateRuntimePromptContext(t, putEvent, "authority_bearing_workspace_doc_write", "workspace.doc.put", workspaceID, "agent", agentID, map[string]string{
		"doc_key":    docKey,
		"title":      "Runbook v2",
		"updated_by": agentID,
	})
	if len(putInvalidations) == 0 {
		t.Fatal("expected doc update to enqueue memory invalidation side effects")
	}
	for _, event := range putInvalidations {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}

	archiveEvent, archiveInvalidations, err := store.ArchiveWorkspaceDocWithEffectsAndPromptContext(ctx, workspaceID, docKey, agentID,
		sqlite.BuildWorkspaceDocPromptContextEnvelope("workspace.doc.archive", "server_rpc", workspaceID, "agent", agentID))
	if err != nil {
		t.Fatalf("archive workspace doc with prompt context: %v", err)
	}
	assertWorkspaceDocUpdateRuntimePromptContext(t, archiveEvent, "authority_bearing_workspace_doc_write", "workspace.doc.archive", workspaceID, "agent", agentID, map[string]string{
		"doc_key":     docKey,
		"archived_by": agentID,
	})
	for _, event := range archiveInvalidations {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}

	deleteEvent, deleteInvalidations, err := store.DeleteWorkspaceDocWithEffectsAndPromptContext(ctx, workspaceID, docKey, agentID,
		sqlite.BuildWorkspaceDocPromptContextEnvelope("workspace.doc.delete", "server_rpc", workspaceID, "agent", agentID))
	if err != nil {
		t.Fatalf("delete workspace doc with prompt context: %v", err)
	}
	assertWorkspaceDocUpdateRuntimePromptContext(t, deleteEvent, "authority_bearing_workspace_doc_write", "workspace.doc.delete", workspaceID, "agent", agentID, map[string]string{
		"doc_key":    docKey,
		"deleted_by": agentID,
	})
	for _, event := range deleteInvalidations {
		assertNoRuntimePromptContextEnvelope(t, event.PayloadJSON)
	}

	updateEvent, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:              updateID,
		WorkspaceID:           workspaceID,
		AgentID:               agentID,
		UpdateType:            "progress",
		Summary:               "workspace update prompt context recorded",
		PayloadJSON:           `{"phase":"doc-update"}`,
		RequiresHuman:         true,
		PromptContextEnvelope: sqlite.BuildAgentUpdatePromptContextEnvelope("agent.update.post", "server_rpc", workspaceID, "agent", agentID),
	})
	if err != nil {
		t.Fatalf("record agent update with prompt context: %v", err)
	}
	assertWorkspaceDocUpdateRuntimePromptContext(t, updateEvent, "authority_bearing_agent_update_write", "agent.update.post", workspaceID, "agent", agentID, map[string]string{
		"update_id":      updateID,
		"agent_id":       agentID,
		"actor_agent_id": agentID,
		"update_type":    "progress",
		"summary":        "workspace update prompt context recorded",
		"requires_human": "true",
	})
	storedUpdatePayload := requireAgentUpdatePromptPayloadJSON(t, ctx, store, workspaceID, updateID)
	assertWorkspaceDocUpdatePromptContextEnvelope(t, storedUpdatePayload, "authority_bearing_agent_update_write", "agent.update.post", workspaceID, "agent", agentID, map[string]string{
		"update_id":      updateID,
		"agent_id":       agentID,
		"actor_agent_id": agentID,
		"update_type":    "progress",
		"summary":        "workspace update prompt context recorded",
		"requires_human": "true",
	})
}

func TestWorkspaceDocPromptContextEnvelopeRejectsForgedBindings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		payloadJSON string
		mutate      func(map[string]any)
		want        string
	}{
		{
			name: "wrong surface",
			mutate: func(envelope map[string]any) {
				envelope["surface"] = "workspace.doc.archive"
			},
			want: "surface",
		},
		{
			name: "wrong workspace",
			mutate: func(envelope map[string]any) {
				envelope["workspace_id"] = "ws-doc-prompt-context-other"
			},
			want: "workspace_id",
		},
		{
			name: "wrong principal",
			mutate: func(envelope map[string]any) {
				envelope["principal_id"] = "agent-doc-forged-other"
			},
			want: "principal_id",
		},
		{
			name: "wrong doc key",
			mutate: func(envelope map[string]any) {
				envelope["doc_key"] = "other-runbook"
			},
			want: "doc_key",
		},
		{
			name: "nested wrong doc key",
			mutate: func(envelope map[string]any) {
				nested := sqlite.BuildWorkspaceDocPromptContextEnvelope("workspace.doc.put", "server_rpc", "ws-doc-prompt-context-forged", "agent", "agent-doc-forged")
				nested["doc_key"] = "other-runbook"
				nested["title"] = "Forged doc prompt context"
				nested["updated_by"] = "agent-doc-forged"
				envelope["nested_false_context"] = nested
			},
			want: "doc_key",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			const (
				workspaceID = "ws-doc-prompt-context-forged"
				agentID     = "agent-doc-forged"
				docKey      = "runbook"
			)
			createWorkspaceDocUpdatePromptContextFixture(t, ctx, store, workspaceID, agentID)
			envelope := sqlite.BuildWorkspaceDocPromptContextEnvelope("workspace.doc.put", "server_rpc", workspaceID, "agent", agentID)
			tc.mutate(envelope)

			_, _, err := store.UpsertWorkspaceDocWithEffects(ctx, sqlite.WorkspaceDocInput{
				WorkspaceID:           workspaceID,
				DocKey:                docKey,
				Title:                 "Forged doc prompt context",
				Content:               "this should roll back",
				UpdatedBy:             agentID,
				PromptContextEnvelope: envelope,
			})
			if err == nil {
				t.Fatal("expected forged workspace doc prompt context to fail")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected forged doc prompt context error: %v", err)
			}
			if got := countWorkspaceDocPromptRows(t, ctx, store, workspaceID, docKey); got != 0 {
				t.Fatalf("expected no workspace_docs row after forged prompt context reject, got %d", got)
			}
			if got := countWorkspaceDocUpdatePromptRuntimeEvents(t, ctx, store, workspaceID, "workspace_doc", docKey, "workspace_doc.upserted"); got != 0 {
				t.Fatalf("expected no workspace_doc.upserted event after forged prompt context reject, got %d", got)
			}
		})
	}
}

func TestAgentUpdatePromptContextEnvelopeRejectsForgedBindings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		payloadJSON string
		mutate      func(map[string]any)
		want        string
	}{
		{
			name: "wrong surface",
			mutate: func(envelope map[string]any) {
				envelope["surface"] = "agent.request"
			},
			want: "not valid for agent_update",
		},
		{
			name: "wrong workspace",
			mutate: func(envelope map[string]any) {
				envelope["workspace_id"] = "ws-agent-update-prompt-context-other"
			},
			want: "workspace_id",
		},
		{
			name: "wrong principal",
			mutate: func(envelope map[string]any) {
				envelope["principal_id"] = "agent-update-forged-other"
			},
			want: "principal_id",
		},
		{
			name: "wrong agent",
			mutate: func(envelope map[string]any) {
				envelope["agent_id"] = "agent-update-forged-other"
			},
			want: "agent_id",
		},
		{
			name: "wrong update id",
			mutate: func(envelope map[string]any) {
				envelope["update_id"] = "update-agent-forged-other"
			},
			want: "update_id",
		},
		{
			name: "nested wrong agent",
			mutate: func(envelope map[string]any) {
				nested := boundAgentUpdatePromptContextEnvelope("ws-agent-update-prompt-context-forged", "update-agent-forged", "agent-update-forged", "progress", "forged agent update prompt context should fail", false)
				nested["agent_id"] = "agent-update-forged-other"
				envelope["nested_false_context"] = nested
			},
			want: "agent_id",
		},
		{
			name:        "caller supplied payload envelope",
			payloadJSON: `{"nested":{"prompt_context_envelope":{"contract":"prompt_context_envelope.v1"}}}`,
			mutate:      func(map[string]any) {},
			want:        "caller-supplied prompt_context_envelope",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			const (
				workspaceID = "ws-agent-update-prompt-context-forged"
				agentID     = "agent-update-forged"
				updateID    = "update-agent-forged"
			)
			createWorkspaceDocUpdatePromptContextFixture(t, ctx, store, workspaceID, agentID)
			envelope := sqlite.BuildAgentUpdatePromptContextEnvelope("agent.update.post", "server_rpc", workspaceID, "agent", agentID)
			tc.mutate(envelope)

			_, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
				UpdateID:              updateID,
				WorkspaceID:           workspaceID,
				AgentID:               agentID,
				UpdateType:            "progress",
				Summary:               "forged agent update prompt context should fail",
				PayloadJSON:           firstNonEmptyString(tc.payloadJSON, `{"phase":"forged"}`),
				RequiresHuman:         false,
				PromptContextEnvelope: envelope,
			})
			if err == nil {
				t.Fatal("expected forged agent update prompt context to fail")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected forged agent update prompt context error: %v", err)
			}
			if got := countAgentUpdatePromptRows(t, ctx, store, workspaceID, updateID); got != 0 {
				t.Fatalf("expected no agent_updates row after forged prompt context reject, got %d", got)
			}
			if got := countWorkspaceDocUpdatePromptRuntimeEvents(t, ctx, store, workspaceID, "agent_update", updateID, "agent_update.posted"); got != 0 {
				t.Fatalf("expected no agent_update.posted event after forged prompt context reject, got %d", got)
			}
		})
	}
}

func TestAgentUpdatePayloadJSONRejectsCallerSuppliedPromptContextMarkersWithoutEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-update-payload-marker-forged"
		agentID     = "agent-update-payload-marker"
		updateID    = "update-agent-payload-marker"
	)
	createWorkspaceDocUpdatePromptContextFixture(t, ctx, store, workspaceID, agentID)

	_, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:      updateID,
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "progress",
		Summary:       "direct storage must reject fake prompt context marker",
		PayloadJSON:   `{"nested":{"contract":"prompt_context_envelope.v1"}}`,
		RequiresHuman: false,
	})
	if err == nil {
		t.Fatal("expected direct agent update payload prompt marker to fail")
	}
	if !strings.Contains(err.Error(), "caller-supplied prompt_context_envelope.v1 contract marker") {
		t.Fatalf("unexpected direct payload marker error: %v", err)
	}
	if got := countAgentUpdatePromptRows(t, ctx, store, workspaceID, updateID); got != 0 {
		t.Fatalf("expected no agent_updates row after direct payload marker reject, got %d", got)
	}
	if got := countWorkspaceDocUpdatePromptRuntimeEvents(t, ctx, store, workspaceID, "agent_update", updateID, "agent_update.posted"); got != 0 {
		t.Fatalf("expected no agent_update.posted event after direct payload marker reject, got %d", got)
	}
}

func createWorkspaceDocUpdatePromptContextFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Doc Update Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
}

func assertWorkspaceDocUpdateRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, contextKind, surface, workspaceID, principalType, principalID string, extra map[string]string) {
	t.Helper()
	if event.EventID == "" {
		t.Fatal("expected runtime event")
	}
	assertWorkspaceDocUpdatePromptContextEnvelope(t, event.PayloadJSON, contextKind, surface, workspaceID, principalType, principalID, extra)
	payload := decodeWorkspaceDocUpdatePromptPayload(t, event.PayloadJSON)
	if got, _ := payload["workspace_id"].(string); got != workspaceID {
		t.Fatalf("expected payload workspace_id=%q, got %+v", workspaceID, payload)
	}
}

func assertWorkspaceDocUpdatePromptContextEnvelope(t *testing.T, payloadJSON, contextKind, surface, workspaceID, principalType, principalID string, extra map[string]string) {
	t.Helper()
	payload := decodeWorkspaceDocUpdatePromptPayload(t, payloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       contextKind,
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
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
}

func decodeWorkspaceDocUpdatePromptPayload(t *testing.T, payloadJSON string) map[string]any {
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

func countWorkspaceDocPromptRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, docKey string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_docs WHERE workspace_id = ? AND doc_key = ?`, workspaceID, docKey).Scan(&count); err != nil {
		t.Fatalf("count workspace_docs rows: %v", err)
	}
	return count
}

func countAgentUpdatePromptRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, updateID string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_updates WHERE workspace_id = ? AND update_id = ?`, workspaceID, updateID).Scan(&count); err != nil {
		t.Fatalf("count agent_updates rows: %v", err)
	}
	return count
}

func requireAgentUpdatePromptPayloadJSON(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, updateID string) string {
	t.Helper()
	var payloadJSON string
	if err := store.DB().QueryRowContext(ctx, `SELECT payload_json FROM agent_updates WHERE workspace_id = ? AND update_id = ?`, workspaceID, updateID).Scan(&payloadJSON); err != nil {
		t.Fatalf("read agent_updates payload_json: %v", err)
	}
	return payloadJSON
}

func countWorkspaceDocUpdatePromptRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, entityType, entityID, eventType string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND entity_type = ? AND entity_id = ? AND event_type = ?`, workspaceID, entityType, entityID, eventType).Scan(&count); err != nil {
		t.Fatalf("count runtime_events rows: %v", err)
	}
	return count
}

func workspaceDocPromptSHA(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func boundAgentUpdatePromptContextEnvelope(workspaceID, updateID, agentID, updateType, summary string, requiresHuman bool) map[string]any {
	envelope := sqlite.BuildAgentUpdatePromptContextEnvelope("agent.update.post", "server_rpc", workspaceID, "agent", agentID)
	envelope["workspace_id"] = strings.TrimSpace(workspaceID)
	envelope["update_id"] = strings.TrimSpace(updateID)
	envelope["agent_id"] = strings.TrimSpace(agentID)
	envelope["actor_agent_id"] = strings.TrimSpace(agentID)
	envelope["update_type"] = strings.TrimSpace(updateType)
	envelope["summary"] = strings.TrimSpace(summary)
	if requiresHuman {
		envelope["requires_human"] = "true"
	} else {
		envelope["requires_human"] = "false"
	}
	return envelope
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
