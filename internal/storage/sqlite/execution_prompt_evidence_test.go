package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestExecutionPromptEvidenceRejectsLegacyRunVerification(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-legacy"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)

	_, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-legacy",
		Title:       "Legacy false green",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"prompt_compiler_status": "legacy_non_converged",
			"c2_1_convergence":       "excluded_until_migrated",
			"deployment_evidence":    "not_accepted_for_daemon_prompt_compiler_convergence",
		},
	})
	if err == nil {
		t.Fatal("expected legacy prompt convergence evidence to be rejected")
	}
	if !strings.Contains(err.Error(), "legacy/non-converged prompt evidence") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecutionPromptEvidenceRejectsPartialAcceptedStepVerification(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-partial"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-partial")

	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-partial",
		Phase:       "VERIFY",
		Title:       "Partial false green",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"prompt_capability_evidence": map[string]any{
				"c2_1_convergence": "daemon_prompt_compiler_converged",
			},
		},
	})
	if err == nil {
		t.Fatal("expected partial prompt convergence evidence to be rejected")
	}
	if !strings.Contains(err.Error(), "partial or unknown prompt-convergence evidence") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecutionPromptEvidenceAcceptsDaemonProof(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-proof"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-proof")

	step, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-proof",
		Phase:       "PLAN",
		Title:       "Record daemon prompt proof",
		Status:      "COMPLETED",
		Evidence:    []string{"capability_snapshot:cap_valid"},
		Verification: map[string]any{
			"prompt_capability_evidence": validExecutionPromptEvidence("cap_valid"),
		},
	})
	if err != nil {
		t.Fatalf("expected valid prompt capability evidence to be accepted: %v", err)
	}
	if step.StepID == "" {
		t.Fatalf("expected recorded step id, got %+v", step)
	}
}

func TestExecutionPromptEvidenceRejectsForgedProofWithoutEvidenceRef(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-forged"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-forged")

	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-forged",
		Phase:       "PLAN",
		Title:       "Forged prompt proof",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"prompt_capability_evidence": validExecutionPromptEvidence("cap_forged"),
		},
	})
	if err == nil {
		t.Fatal("expected self-consistent prompt proof without durable evidence ref to be rejected")
	}
	if !strings.Contains(err.Error(), `missing durable evidence ref "capability_snapshot:cap_forged"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecutionPromptEvidenceRejectsMalformedProjectionDigest(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-digest"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-digest")

	for _, tc := range []struct {
		name   string
		digest string
	}{
		{name: "non_hex", digest: "sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
		{name: "uppercase_hex", digest: "sha256:" + strings.ToUpper("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")},
		{name: "padded", digest: " sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef "},
		{name: "empty", digest: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proof := validExecutionPromptEvidence("cap_digest_" + tc.name)
			proof["projection_digest"] = tc.digest
			_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
				WorkspaceID: workspaceID,
				RunID:       "run-digest",
				Phase:       "PLAN",
				Title:       "Malformed digest " + tc.name,
				Status:      "COMPLETED",
				Evidence:    []string{"capability_snapshot:cap_digest_" + tc.name},
				Verification: map[string]any{
					"prompt_capability_evidence": proof,
				},
			})
			if err == nil {
				t.Fatalf("expected malformed projection_digest %q to be rejected", tc.digest)
			}
			if !strings.Contains(err.Error(), "invalid projection_digest") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestExecutionPromptEvidenceRejectsNonStringPromptKeys(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-types"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-types")

	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-types",
		Phase:       "VERIFY",
		Title:       "Typed false green",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"nested": map[string]any{
				"prompt_compiler_status": []any{"legacy_non_converged"},
			},
		},
	})
	if err == nil {
		t.Fatal("expected non-string prompt evidence key to be rejected")
	}
	if !strings.Contains(err.Error(), "non-string prompt_compiler_status") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecutionPromptEvidenceAllowsUnrelatedContractPayload(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-unrelated"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-unrelated")

	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-unrelated",
		Phase:       "VERIFY",
		Title:       "Unrelated verification contract",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"artifact": map[string]any{
				"contract": "repo_conflict_model.v1",
				"status":   "checked",
			},
		},
	})
	if err != nil {
		t.Fatalf("expected unrelated contract verification payload to be accepted: %v", err)
	}
}

func TestExecutionPromptEvidenceAllowsCapabilitySnapshotPromptContractObjectWithSeparateProof(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-cap-snapshot"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-cap-snapshot")

	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-cap-snapshot",
		Phase:       "PLAN",
		Title:       "Run capability snapshot",
		Status:      "COMPLETED",
		Evidence:    []string{"capability_snapshot:cap_live"},
		Verification: map[string]any{
			"capability_snapshot": map[string]any{
				"schema":      "daemon_capability_snapshot.v1",
				"snapshot_id": "cap_live",
				"prompt_contract": map[string]any{
					"contract_id": "prompt_capabilities.v1",
					"snapshot_id": "cap_live",
				},
			},
			"prompt_capability_evidence": validExecutionPromptEvidence("cap_live"),
		},
	})
	if err != nil {
		t.Fatalf("expected embedded capability snapshot prompt_contract object to be accepted: %v", err)
	}
}

func TestExecutionPromptContextEnvelopeAcceptsManualExecutionWrites(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-context"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-context")

	step, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-context",
		Phase:       "VERIFY",
		Title:       "Manual verifier result",
		Status:      "COMPLETED",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{"gate": "pass"},
			sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.step.write", "server_rpc", workspaceID, "human", "developer"),
		),
	})
	if err != nil {
		t.Fatalf("expected prompt context envelope to be accepted: %v", err)
	}
	if step.VerificationJSON["gate"] != "pass" {
		t.Fatalf("expected existing verification fields to be preserved, got %+v", step.VerificationJSON)
	}
	envelope, ok := step.VerificationJSON["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope, got %+v", step.VerificationJSON)
	}
	if got := envelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected envelope contract: %v", got)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("manual envelope must not claim daemon convergence, got %v", got)
	}
}

func TestExecutionStepWrittenEventCarriesPromptContextVerification(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-step-event-context"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-step-event-context")

	step, event, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:      "step-event-context",
		WorkspaceID: workspaceID,
		RunID:       "run-step-event-context",
		Phase:       "VERIFY",
		Title:       "Verifier context event",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"gate": "pass",
		},
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.step.write", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("record execution step with prompt context event: %v", err)
	}

	verification := decodeExecutionStepEventVerification(t, event.PayloadJSON)
	if verification["gate"] != step.VerificationJSON["gate"] {
		t.Fatalf("expected execution_step.written verification to match row fields, payload=%+v row=%+v", verification, step.VerificationJSON)
	}
	assertExecutionPromptContextEnvelopePayload(t, verification, "workspace.execution.step.write", "server_rpc", workspaceID, "human", "developer")
}

func TestExecutionStepWrittenEventCarriesPreservedVerificationOnRepeat(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-step-event-preserved-context"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-step-event-preserved-context")

	firstStep, _, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:      "step-event-preserved-context",
		WorkspaceID: workspaceID,
		RunID:       "run-step-event-preserved-context",
		Phase:       "EXECUTE",
		Title:       "First verifier context event",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"gate": "initial",
		},
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.step.write", "server_rpc", workspaceID, "human", "developer"),
	})
	if err != nil {
		t.Fatalf("record first execution step with prompt context event: %v", err)
	}

	secondStep, secondEvent, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:      firstStep.StepID,
		WorkspaceID: workspaceID,
		RunID:       firstStep.RunID,
		Phase:       "VERIFY",
		Title:       "Second verifier context event",
		Status:      "BLOCKED",
	})
	if err != nil {
		t.Fatalf("record second execution step with preserved verification: %v", err)
	}
	if secondStep.VerificationJSON["gate"] != "initial" {
		t.Fatalf("expected repeated execution step write to preserve verification row, got %+v", secondStep.VerificationJSON)
	}

	verification := decodeExecutionStepEventVerification(t, secondEvent.PayloadJSON)
	if verification["gate"] != "initial" {
		t.Fatalf("expected repeated execution_step.written event to carry preserved verification, got %+v", verification)
	}
	assertExecutionPromptContextEnvelopePayload(t, verification, "workspace.execution.step.write", "server_rpc", workspaceID, "human", "developer")
}

func TestExecutionStepWrittenEventRejectsForgedPromptContextWithoutEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-step-event-forged-context"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-step-event-forged-context")

	_, _, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:      "step-event-forged-context",
		WorkspaceID: workspaceID,
		RunID:       "run-step-event-forged-context",
		Phase:       "VERIFY",
		Title:       "Forged verifier context event",
		Status:      "COMPLETED",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{"gate": "pass"},
			sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "human", "developer"),
		),
	})
	if err == nil {
		t.Fatal("expected execution step event write to reject forged run surface")
	}
	if !strings.Contains(err.Error(), "not valid for execution_step") {
		t.Fatalf("unexpected forged prompt context error: %v", err)
	}

	detail, getErr := store.GetExecutionRun(ctx, workspaceID, "run-step-event-forged-context")
	if getErr != nil {
		t.Fatalf("get execution detail after forged event reject: %v", getErr)
	}
	for _, step := range detail.Steps {
		if step.StepID == "step-event-forged-context" {
			t.Fatalf("forged prompt context write persisted step: %+v", step)
		}
	}
	events, listErr := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    "step-event-forged-context",
		Limit:       10,
	})
	if listErr != nil {
		t.Fatalf("list runtime events after forged event reject: %v", listErr)
	}
	if len(events) != 0 {
		t.Fatalf("forged prompt context write appended events: %+v", events)
	}
}

func TestExecutionStepWrittenEventRejectsPromptContextWorkspaceMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-step-event-workspace-mismatch"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-step-event-workspace-mismatch")

	_, _, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:                "step-event-workspace-mismatch",
		WorkspaceID:           workspaceID,
		RunID:                 "run-step-event-workspace-mismatch",
		Phase:                 "VERIFY",
		Title:                 "Forged workspace context event",
		Status:                "COMPLETED",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.step.write", "server_rpc", "ws-other", "human", "developer"),
	})
	if err == nil {
		t.Fatal("expected execution step event write to reject prompt context workspace mismatch")
	}
	if !strings.Contains(err.Error(), "does not match record workspace_id") {
		t.Fatalf("unexpected workspace mismatch error: %v", err)
	}

	assertExecutionStepAbsent(t, ctx, store, workspaceID, "run-step-event-workspace-mismatch", "step-event-workspace-mismatch")
	assertNoExecutionStepWrittenEvents(t, ctx, store, workspaceID, "step-event-workspace-mismatch")
}

func TestExecutionStepWrittenEventRejectsPromptContextAgentPrincipalMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-step-event-agent-mismatch"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	registerExecutionPromptEvidenceAgent(t, ctx, store, workspaceID, "agent-real")
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-step-event-agent-mismatch",
		AgentID:     "agent-real",
		Title:       "Agent-bound execution run",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create agent-bound execution run: %v", err)
	}

	_, _, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:                "step-event-agent-mismatch",
		WorkspaceID:           workspaceID,
		RunID:                 "run-step-event-agent-mismatch",
		Phase:                 "VERIFY",
		Title:                 "Forged agent context event",
		Status:                "COMPLETED",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.step.write", "server_rpc", workspaceID, "agent", "agent-forged"),
	})
	if err == nil {
		t.Fatal("expected execution step event write to reject prompt context agent mismatch")
	}
	if !strings.Contains(err.Error(), "does not match record agent_id") {
		t.Fatalf("unexpected agent mismatch error: %v", err)
	}

	assertExecutionStepAbsent(t, ctx, store, workspaceID, "run-step-event-agent-mismatch", "step-event-agent-mismatch")
	assertNoExecutionStepWrittenEvents(t, ctx, store, workspaceID, "step-event-agent-mismatch")
}

func TestExecutionRunWrittenEventRejectsPromptContextBindingMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-run-event-binding-mismatch"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	registerExecutionPromptEvidenceAgent(t, ctx, store, workspaceID, "agent-real")

	_, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-event-binding-mismatch",
		AgentID:               "agent-real",
		Title:                 "Forged run context event",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", "ws-other", "agent", "agent-forged"),
	})
	if err == nil {
		t.Fatal("expected execution run event write to reject prompt context binding mismatch")
	}
	if !strings.Contains(err.Error(), "does not match record workspace_id") {
		t.Fatalf("unexpected run binding mismatch error: %v", err)
	}

	runs, listErr := store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if listErr != nil {
		t.Fatalf("list execution runs after forged run context reject: %v", listErr)
	}
	for _, run := range runs {
		if run.RunID == "run-event-binding-mismatch" {
			t.Fatalf("forged prompt context write persisted run: %+v", run)
		}
	}
	events, listEventsErr := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "run-event-binding-mismatch",
		Limit:       10,
	})
	if listEventsErr != nil {
		t.Fatalf("list runtime events after forged run context reject: %v", listEventsErr)
	}
	if len(events) != 0 {
		t.Fatalf("forged prompt context run write appended events: %+v", events)
	}
}

func TestExecutionPromptContextEnvelopeRejectsMalformedManualClaims(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-context-bad"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)

	_, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-context-bad",
		Title:       "Malformed manual context",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"prompt_context_envelope": "prompt_context_envelope.v1",
		},
	})
	if err == nil {
		t.Fatal("expected non-object prompt_context_envelope to be rejected")
	}
	if !strings.Contains(err.Error(), "prompt_context_envelope must be an object") {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-context-bad",
		Title:       "False daemon manual context",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"prompt_context_envelope": map[string]any{
				"contract":                           "prompt_context_envelope.v1",
				"context_kind":                       "authority_bearing_execution_write",
				"surface":                            "workspace.execution.run.write",
				"origin":                             "server_rpc",
				"workspace_id":                       workspaceID,
				"principal_type":                     "human",
				"principal_id":                       "developer",
				"authority_model":                    "workspace_authority",
				"compiler_status":                    "daemon_converged",
				"daemon_prompt_compiler_convergence": "daemon_prompt_compiler_converged",
				"prompt_capability_evidence":         "not_present",
			},
		},
	})
	if err == nil {
		t.Fatal("expected manual context envelope that claims daemon convergence to be rejected")
	}
	if !strings.Contains(err.Error(), "invalid compiler_status") &&
		!strings.Contains(err.Error(), "invalid daemon_prompt_compiler_convergence") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecutionPromptContextEnvelopeRejectsMismatchedRecordSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-context-surface"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-context-surface")

	run, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-operation-ledger-surface",
		Title:       "Operation ledger surface",
		Status:      "ACTIVE",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{"operation_ledger": map[string]any{"schema": "operation_ledger.v1"}},
			sqlite.BuildExecutionPromptContextEnvelope("mcp.tool.discover", "server_operation_ledger", workspaceID, "human", "developer"),
		),
	})
	if err != nil {
		t.Fatalf("expected execution run to accept operation-ledger prompt context envelope: %v", err)
	}
	envelope, ok := run.VerificationJSON["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected operation-ledger prompt context envelope, got %+v", run.VerificationJSON)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("operation-ledger envelope must not claim daemon convergence, got %v", got)
	}

	_, err = store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-context-surface",
		Phase:       "VERIFY",
		Title:       "Operation ledger surface on step",
		Status:      "COMPLETED",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{"gate": "pass"},
			sqlite.BuildExecutionPromptContextEnvelope("mcp.tool.discover", "server_operation_ledger", workspaceID, "human", "developer"),
		),
	})
	if err == nil {
		t.Fatal("expected execution step to reject operation-ledger run prompt context envelope")
	}
	if !strings.Contains(err.Error(), "not valid for execution_step") {
		t.Fatalf("unexpected operation-ledger step surface error: %v", err)
	}

	_, err = store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-action-surface",
		Title:       "Human action surface on execution run",
		Status:      "ACTIVE",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{"gate": "pass"},
			sqlite.BuildHumanActionPromptContextEnvelope("action.resolve", "server_rpc", workspaceID, "human", "developer"),
		),
	})
	if err == nil {
		t.Fatal("expected execution run to reject human-action prompt context envelope")
	}
	if !strings.Contains(err.Error(), "not valid for execution_run") &&
		!strings.Contains(err.Error(), "invalid context_kind") {
		t.Fatalf("unexpected human-action run surface error: %v", err)
	}

	_, err = store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-wrong-surface",
		Title:       "Wrong run surface",
		Status:      "ACTIVE",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{"gate": "pass"},
			sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.step.write", "server_rpc", workspaceID, "human", "developer"),
		),
	})
	if err == nil {
		t.Fatal("expected execution run to reject step-write prompt context envelope")
	}
	if !strings.Contains(err.Error(), "not valid for execution_run") {
		t.Fatalf("unexpected run surface error: %v", err)
	}

	_, err = store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-context-surface",
		Phase:       "VERIFY",
		Title:       "Wrong step surface",
		Status:      "COMPLETED",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{"gate": "pass"},
			sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "human", "developer"),
		),
	})
	if err == nil {
		t.Fatal("expected execution step to reject run-write prompt context envelope")
	}
	if !strings.Contains(err.Error(), "not valid for execution_step") {
		t.Fatalf("unexpected step surface error: %v", err)
	}
}

func TestHumanActionPromptContextEnvelopePayloadValidation(t *testing.T) {
	t.Parallel()

	workspaceID := "ws-action-prompt-context"
	payload, err := sqlite.AttachHumanActionPromptContextEnvelope(
		map[string]any{"action_id": "action-context", "status": "PENDING"},
		sqlite.BuildHumanActionPromptContextEnvelope("action.create", "server_rpc", workspaceID, "human", "developer"),
	)
	if err != nil {
		t.Fatalf("expected human action prompt context envelope to be accepted: %v", err)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected human action prompt_context_envelope, got %+v", payload)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_action_write" {
		t.Fatalf("unexpected human action context kind: %v", got)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("human action envelope must not claim daemon convergence, got %v", got)
	}

	if _, err := sqlite.AttachHumanActionPromptContextEnvelope(
		map[string]any{"action_id": "action-context", "message_id": "msg-context"},
		sqlite.BuildHumanActionPromptContextEnvelope("action.chat.send", "server_rpc", workspaceID, "human", "developer"),
	); err != nil {
		t.Fatalf("expected human action chat prompt context envelope to be accepted: %v", err)
	}

	_, err = sqlite.AttachHumanActionPromptContextEnvelope(
		map[string]any{"action_id": "action-context"},
		sqlite.BuildHumanActionPromptContextEnvelope("action.create", "cli_local", workspaceID, "human", "developer"),
	)
	if err == nil {
		t.Fatal("expected human action payload to reject cli_local origin")
	}
	if !strings.Contains(err.Error(), "not valid for human_action") {
		t.Fatalf("unexpected human action origin error: %v", err)
	}

	_, err = sqlite.AttachHumanActionPromptContextEnvelope(
		map[string]any{"action_id": "action-context"},
		sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "human", "developer"),
	)
	if err == nil {
		t.Fatal("expected human action payload to reject execution prompt context envelope")
	}
	if !strings.Contains(err.Error(), "invalid context_kind") {
		t.Fatalf("unexpected human action context-kind error: %v", err)
	}
}

func TestTaskPromptContextEnvelopePayloadValidation(t *testing.T) {
	t.Parallel()

	workspaceID := "ws-task-prompt-context"
	payload, err := sqlite.AttachTaskPromptContextEnvelope(
		map[string]any{"task_id": "task-context", "status": "PENDING"},
		sqlite.BuildTaskPromptContextEnvelope("task.submit", "server_rpc", workspaceID, "human", "developer"),
	)
	if err != nil {
		t.Fatalf("expected task prompt context envelope to be accepted: %v", err)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected task prompt_context_envelope, got %+v", payload)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_task_write" {
		t.Fatalf("unexpected task context kind: %v", got)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("task envelope must not claim daemon convergence, got %v", got)
	}

	if _, err := sqlite.AttachTaskPromptContextEnvelope(
		map[string]any{"task_id": "task-context", "status": "RESOLVED"},
		sqlite.BuildTaskPromptContextEnvelope("task.close", "server_rpc", workspaceID, "human", "developer"),
	); err != nil {
		t.Fatalf("expected task close prompt context envelope to be accepted: %v", err)
	}

	if _, err := sqlite.AttachTaskPromptContextEnvelope(
		map[string]any{"task_id": "task-context"},
		sqlite.BuildTaskPromptContextEnvelope("task.submit", "cli_local", workspaceID, "human", "developer"),
	); err != nil {
		t.Fatalf("expected task payload to accept cli_local origin: %v", err)
	}

	_, err = sqlite.AttachTaskPromptContextEnvelope(
		map[string]any{"task_id": "task-context"},
		sqlite.BuildTaskPromptContextEnvelope("task.submit", "server_session_projection", workspaceID, "human", "developer"),
	)
	if err == nil {
		t.Fatal("expected task payload to reject server_session_projection origin")
	}
	if !strings.Contains(err.Error(), "not valid for task_lifecycle") {
		t.Fatalf("unexpected task origin error: %v", err)
	}

	_, err = sqlite.AttachTaskPromptContextEnvelope(
		map[string]any{"task_id": "task-context"},
		sqlite.BuildHumanActionPromptContextEnvelope("action.create", "server_rpc", workspaceID, "human", "developer"),
	)
	if err == nil {
		t.Fatal("expected task payload to reject human-action prompt context envelope")
	}
	if !strings.Contains(err.Error(), "invalid context_kind") {
		t.Fatalf("unexpected task context-kind error: %v", err)
	}
}

func TestExecutionPromptEvidenceRejectsPaddedAcceptedProofFields(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-padded"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-padded")

	proof := validExecutionPromptEvidence("cap_padded")
	proof["contract"] = " daemon_prompt_capability_evidence.v1 "
	proof["prompt_compiler_status"] = " daemon_converged "
	proof["capability_snapshot_ref"] = " capability_snapshot:cap_padded "
	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       "run-padded",
		Phase:       "PLAN",
		Title:       "Padded proof",
		Status:      "COMPLETED",
		Evidence:    []string{"capability_snapshot:cap_padded"},
		Verification: map[string]any{
			"prompt_capability_evidence": proof,
		},
	})
	if err == nil {
		t.Fatal("expected padded prompt capability proof fields to fail closed")
	}
	if !strings.Contains(err.Error(), "prompt-convergence evidence") && !strings.Contains(err.Error(), "invalid contract") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecutionPromptEvidenceRejectsNonCanonicalCapabilitySnapshotID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-snapshot-id"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-snapshot-id")

	for _, snapshotID := range []string{" cap_bad ", "cap_bad value", "snapshot_bad", "cap_bad/value"} {
		proof := validExecutionPromptEvidence(snapshotID)
		_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
			WorkspaceID: workspaceID,
			RunID:       "run-snapshot-id",
			Phase:       "PLAN",
			Title:       "Non canonical snapshot id",
			Status:      "COMPLETED",
			Evidence:    []string{"capability_snapshot:" + snapshotID},
			Verification: map[string]any{
				"prompt_capability_evidence": proof,
			},
		})
		if err == nil {
			t.Fatalf("expected non-canonical capability_snapshot_id %q to fail closed", snapshotID)
		}
		if !strings.Contains(err.Error(), "invalid capability_snapshot_id") {
			t.Fatalf("unexpected error for %q: %v", snapshotID, err)
		}
	}
}

func TestExecutionPromptEvidencePreservesStepEvidenceOnOmittedUpdate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-preserve"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-preserve")

	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		StepID:      "step-preserve",
		WorkspaceID: workspaceID,
		RunID:       "run-preserve",
		Phase:       "PLAN",
		Title:       "Prompt proof",
		Status:      "COMPLETED",
		Evidence:    []string{"capability_snapshot:cap_preserve"},
		Verification: map[string]any{
			"prompt_capability_evidence": validExecutionPromptEvidence("cap_preserve"),
		},
	})
	if err != nil {
		t.Fatalf("record initial step proof: %v", err)
	}
	_, err = store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		StepID:      "step-preserve",
		WorkspaceID: workspaceID,
		RunID:       "run-preserve",
		Phase:       "PLAN",
		Title:       "Prompt proof updated",
		Status:      "COMPLETED",
	})
	if err != nil {
		t.Fatalf("record omitted verification update: %v", err)
	}
	detail, err := store.GetExecutionRun(ctx, workspaceID, "run-preserve")
	if err != nil {
		t.Fatalf("get execution run: %v", err)
	}
	if len(detail.Steps) != 1 {
		t.Fatalf("expected one step, got %d", len(detail.Steps))
	}
	step := detail.Steps[0]
	if !stringSliceContains(step.Evidence, "capability_snapshot:cap_preserve") {
		t.Fatalf("expected capability snapshot evidence to be preserved, got %+v", step.Evidence)
	}
	proof, ok := step.VerificationJSON["prompt_capability_evidence"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_capability_evidence to be preserved, got %+v", step.VerificationJSON)
	}
	if got := proof["capability_snapshot_ref"]; got != "capability_snapshot:cap_preserve" {
		t.Fatalf("expected preserved capability snapshot ref, got %v", got)
	}
}

func TestExecutionPromptEvidenceRejectsPromptMarkersInEvidenceRefs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-exec-prompt-ref-marker"
	createExecutionPromptEvidenceWorkspace(t, ctx, store, workspaceID)
	createExecutionPromptEvidenceRun(t, ctx, store, workspaceID, "run-ref-marker")

	for _, ref := range []string{
		"prompt_compiler_status: legacy_non_converged",
		"projection_digest: sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"projection_source: agent.runtime_capability_snapshot",
		"projection_contract: active_capability_snapshot_projection.v1",
		"prompt_capability_evidence: daemon_converged",
		"daemon_prompt_capability_evidence.v1",
		"capability_snapshot_ref: capability_snapshot:cap_bad",
	} {
		_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
			WorkspaceID: workspaceID,
			RunID:       "run-ref-marker",
			Phase:       "PLAN",
			Title:       "Bad evidence ref",
			Status:      "COMPLETED",
			Evidence:    []string{ref},
		})
		if err == nil {
			t.Fatalf("expected prompt marker evidence ref %q to be rejected", ref)
		}
		if !strings.Contains(err.Error(), "evidence ref contains prompt-convergence marker") {
			t.Fatalf("unexpected error for %q: %v", ref, err)
		}
	}
}

func createExecutionPromptEvidenceWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Prompt Evidence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
}

func createExecutionPromptEvidenceRun(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, runID string) {
	t.Helper()
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "Execution Prompt Evidence Run",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create execution run: %v", err)
	}
}

func registerExecutionPromptEvidenceAgent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register execution prompt evidence agent: %v", err)
	}
}

func validExecutionPromptEvidence(snapshotID string) map[string]any {
	return map[string]any{
		"contract":                "daemon_prompt_capability_evidence.v1",
		"prompt_compiler_status":  "daemon_converged",
		"c2_1_convergence":        "daemon_prompt_compiler_converged",
		"deployment_evidence":     "accepted_for_daemon_prompt_compiler_convergence",
		"capability_snapshot_id":  snapshotID,
		"capability_snapshot_ref": "capability_snapshot:" + snapshotID,
		"projection_source":       "agent.runtime_capability_snapshot",
		"projection_contract":     "active_capability_snapshot_projection.v1",
		"projection_digest":       "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"snapshot_schema":         "daemon_capability_snapshot.v1",
		"snapshot_kind":           "run",
		"snapshot_status":         "enabled",
		"prompt_contract":         "prompt_capabilities.v1",
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func decodeExecutionStepEventVerification(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode execution step event payload: %v", err)
	}
	verification, ok := payload["verification"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_step.written verification object, got %+v", payload)
	}
	return verification
}

func assertExecutionPromptContextEnvelopePayload(t *testing.T, verification map[string]any, wantSurface, wantOrigin, wantWorkspaceID, wantPrincipalType, wantPrincipalID string) {
	t.Helper()
	envelope, ok := verification["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in verification payload, got %+v", verification)
	}
	if got := envelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected envelope contract: %v", got)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_execution_write" {
		t.Fatalf("unexpected envelope context kind: %v", got)
	}
	if got := envelope["surface"]; got != wantSurface {
		t.Fatalf("unexpected envelope surface: got %v want %s", got, wantSurface)
	}
	if got := envelope["origin"]; got != wantOrigin {
		t.Fatalf("unexpected envelope origin: got %v want %s", got, wantOrigin)
	}
	if got := envelope["workspace_id"]; got != wantWorkspaceID {
		t.Fatalf("unexpected envelope workspace: got %v want %s", got, wantWorkspaceID)
	}
	if got := envelope["principal_type"]; got != wantPrincipalType {
		t.Fatalf("unexpected envelope principal type: got %v want %s", got, wantPrincipalType)
	}
	if got := envelope["principal_id"]; got != wantPrincipalID {
		t.Fatalf("unexpected envelope principal id: got %v want %s", got, wantPrincipalID)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("manual envelope must not claim daemon convergence, got %v", got)
	}
}

func assertExecutionStepAbsent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, runID, stepID string) {
	t.Helper()
	detail, err := store.GetExecutionRun(ctx, workspaceID, runID)
	if err != nil {
		t.Fatalf("get execution detail after rejected step context: %v", err)
	}
	for _, step := range detail.Steps {
		if step.StepID == stepID {
			t.Fatalf("rejected prompt context write persisted step: %+v", step)
		}
	}
}

func assertNoExecutionStepWrittenEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, stepID string) {
	t.Helper()
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    stepID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events after rejected step context: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("rejected prompt context write appended step events: %+v", events)
	}
}
