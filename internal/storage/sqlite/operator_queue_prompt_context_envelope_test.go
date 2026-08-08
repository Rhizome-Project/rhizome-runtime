package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestOperatorQueuePromptContextEnvelopePreservesPayloadAcrossManualWrites(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ops-prompt-context"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	for _, eventID := range []string{"evt-1", "evt-2"} {
		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:     eventID,
			WorkspaceID: workspaceID,
			EventType:   "test.parent",
			EntityType:  "test",
			EntityID:    eventID,
			PayloadJSON: "{}",
		}); err != nil {
			t.Fatalf("record parent runtime event %s: %v", eventID, err)
		}
	}

	created, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:deploy-gate",
		QueueType:   "FOLLOW_UP",
		Title:       "Deploy gate",
		PayloadJSON: `{"custom":"keep","root_cause_id":"root-1","provenance_group_id":"prov-1","parent_refs_json":["evt-1","evt-2"]}`,
		PromptContextEnvelope: sqlite.BuildOperatorQueuePromptContextEnvelope(
			"cli.workspace.ops.upsert",
			"cli_local",
			workspaceID,
			"operator",
			"local_cli",
		),
	})
	if err != nil {
		t.Fatalf("create operator queue prompt context: %v", err)
	}
	assertOperatorQueuePromptContextPayload(t, created.PayloadJSON, "cli.workspace.ops.upsert", "custom", "keep")

	refreshed, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             workspaceID,
		QueueKey:                "manual:deploy-gate",
		QueueType:               "FOLLOW_UP",
		Title:                   "Deploy gate refreshed",
		RequireCurrentRevision:  created.Revision,
		RequireCurrentUpdatedAt: created.UpdatedAt,
		PromptContextEnvelope: sqlite.BuildOperatorQueuePromptContextEnvelope(
			"workspace.ops.upsert",
			"server_rpc",
			workspaceID,
			"human",
			"operator-a",
		),
	})
	if err != nil {
		t.Fatalf("refresh operator queue prompt context: %v", err)
	}
	assertOperatorQueuePromptContextPayload(t, refreshed.PayloadJSON, "workspace.ops.upsert", "custom", "keep")

	escalated, err := store.EscalateOperatorQueueItem(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID:           workspaceID,
		QueueKey:              "manual:deploy-gate",
		EscalatedBy:           "operator-a",
		Reason:                "needs a human decision",
		Urgency:               "HIGH",
		PromptContextEnvelope: sqlite.BuildOperatorQueuePromptContextEnvelope("workspace.ops.escalate", "server_rpc", workspaceID, "human", "operator-a"),
	})
	if err != nil {
		t.Fatalf("escalate operator queue prompt context: %v", err)
	}
	assertOperatorQueuePromptContextPayload(t, escalated.PayloadJSON, "workspace.ops.escalate", "custom", "keep")

	resolved, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID:           workspaceID,
		QueueKey:              "manual:deploy-gate",
		Status:                "RESOLVED",
		ResolvedBy:            "operator-a",
		Resolution:            "approved",
		PromptContextEnvelope: sqlite.BuildOperatorQueuePromptContextEnvelope("workspace.ops.resolve", "server_rpc", workspaceID, "human", "operator-a"),
	})
	if err != nil {
		t.Fatalf("resolve operator queue prompt context: %v", err)
	}
	assertOperatorQueuePromptContextPayload(t, resolved.PayloadJSON, "workspace.ops.resolve", "custom", "keep")
	resolvedPayload := decodeOperatorQueuePromptContextPayload(t, resolved.PayloadJSON)
	if resolvedPayload["root_cause_id"] != "root-1" || resolvedPayload["provenance_group_id"] != "prov-1" {
		t.Fatalf("expected operator queue lineage to be preserved, got %+v", resolvedPayload)
	}
	parents, ok := resolvedPayload["parent_refs_json"].([]any)
	if !ok || len(parents) != 2 || parents[0] != "evt-1" || parents[1] != "evt-2" {
		t.Fatalf("expected operator queue parent refs to be preserved, got %+v", resolvedPayload["parent_refs_json"])
	}
}

func TestOperatorQueuePromptContextEnvelopeRejectsExecutionSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ops-prompt-context-reject"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)

	payload := sqlite.AttachExecutionPromptContextEnvelope(
		map[string]any{"custom": "keep"},
		sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "human", "operator-a"),
	)
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal bad operator queue payload: %v", err)
	}

	_, err = store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:bad-context",
		QueueType:   "FOLLOW_UP",
		Title:       "Bad context",
		PayloadJSON: string(payloadJSON),
	})
	if err == nil {
		t.Fatal("expected operator queue to reject execution prompt context envelope")
	}
	if !strings.Contains(err.Error(), "invalid context_kind") && !strings.Contains(err.Error(), "not valid for operator_queue") {
		t.Fatalf("unexpected operator queue prompt context error: %v", err)
	}
}

func TestOperatorQueuePromptContextEnvelopeRejectsMalformedManualPayload(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ops-prompt-context-malformed"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)

	for _, payloadJSON := range []string{
		`{"prompt_context_envelope":"not-an-object"}`,
		`{"prompt_context_envelope":{"contract":"prompt_context_envelope.v1"}}`,
		`{"prompt_context_envelope":{"contract":"prompt_context_envelope.v1","context_kind":"authority_bearing_operator_queue_write","surface":" workspace.ops.upsert ","origin":"server_rpc","workspace_id":"` + workspaceID + `","principal_type":"human","principal_id":"operator-a","authority_model":"workspace_authority","compiler_status":"non_daemon_context_envelope","daemon_prompt_compiler_convergence":"not_claimed","prompt_capability_evidence":"not_present"}}`,
	} {
		_, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
			WorkspaceID: workspaceID,
			QueueKey:    "manual:bad-context-" + strings.ReplaceAll(strings.ReplaceAll(payloadJSON[:20], "{", ""), "\"", ""),
			QueueType:   "FOLLOW_UP",
			Title:       "Bad context",
			PayloadJSON: payloadJSON,
		})
		if err == nil {
			t.Fatalf("expected malformed operator queue prompt context payload to fail: %s", payloadJSON)
		}
	}
}

func assertOperatorQueuePromptContextPayload(t *testing.T, payloadJSON, wantSurface, keepKey string, keepValue any) {
	t.Helper()
	payload := decodeOperatorQueuePromptContextPayload(t, payloadJSON)
	if got := payload[keepKey]; got != keepValue {
		t.Fatalf("expected payload %s=%v to be preserved, got %+v", keepKey, keepValue, payload)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected operator queue prompt context envelope, got %+v", payload)
	}
	if got := envelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected operator queue context contract: %v", got)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_operator_queue_write" {
		t.Fatalf("unexpected operator queue context kind: %v", got)
	}
	if got := envelope["surface"]; got != wantSurface {
		t.Fatalf("unexpected operator queue context surface: got %v want %s", got, wantSurface)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("operator queue context must not claim daemon convergence: %+v", envelope)
	}
}

func decodeOperatorQueuePromptContextPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode operator queue payload_json: %v; payload=%q", err, payloadJSON)
	}
	return payload
}
