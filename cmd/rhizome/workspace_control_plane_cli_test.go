package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceControlPlaneCLIFlows(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-control-cli",
		"--title", "Control Plane CLI",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-control-cli")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-control-cli",
		"--agent-id", "agent-control-cli",
		"--owner-user-id", "developer",
		"--display-name", "Control Plane Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	opsOut, err := captureStdout(t, func() error {
		return runWorkspaceOps([]string{
			"upsert",
			"--workspace-id", "ws-control-cli",
			"--queue-key", "manual:deploy-gate",
			"--queue-type", "FOLLOW_UP",
			"--title", "Confirm deploy gate",
			"--summary", "Check live doctor verdict",
			"--assigned-to", "developer",
			"--urgency", "HIGH",
			"--source-kind", "manual",
			"--source-id", "developer",
			"--agent-id", "agent-control-cli",
			"--keep-session-active",
		})
	})
	if err != nil {
		t.Fatalf("workspace ops upsert failed: %v", err)
	}

	var opsPayload struct {
		Item struct {
			QueueID     string `json:"queue_id"`
			Status      string `json:"status"`
			Revision    int64  `json:"revision"`
			UpdatedAt   string `json:"updated_at"`
			Summary     string `json:"summary"`
			PayloadJSON string `json:"payload_json"`
		} `json:"item"`
	}
	if err := json.Unmarshal([]byte(opsOut), &opsPayload); err != nil {
		t.Fatalf("decode workspace ops upsert output: %v; output=%q", err, opsOut)
	}
	if opsPayload.Item.QueueID == "" || opsPayload.Item.Status != "OPEN" {
		t.Fatalf("unexpected workspace ops upsert payload %+v", opsPayload)
	}
	if strings.TrimSpace(opsPayload.Item.UpdatedAt) == "" {
		t.Fatalf("expected updated_at in workspace ops upsert payload %+v", opsPayload)
	}
	if opsPayload.Item.Revision != 1 {
		t.Fatalf("expected initial queue revision 1 in workspace ops payload %+v", opsPayload)
	}
	assertCLIOperatorQueuePromptContextSurface(t, opsPayload.Item.PayloadJSON, "cli.workspace.ops.upsert")

	refreshedOut, err := captureStdout(t, func() error {
		return runWorkspaceOps([]string{
			"upsert",
			"--workspace-id", "ws-control-cli",
			"--queue-key", "manual:deploy-gate",
			"--queue-type", "FOLLOW_UP",
			"--title", "Confirm deploy gate",
			"--summary", "Check live doctor verdict after refresh",
			"--assigned-to", "developer",
			"--urgency", "HIGH",
			"--source-kind", "manual",
			"--source-id", "developer",
			"--agent-id", "agent-control-cli",
			"--keep-session-active",
			"--current-revision", strconv.FormatInt(opsPayload.Item.Revision, 10),
			"--current-updated-at", opsPayload.Item.UpdatedAt,
		})
	})
	if err != nil {
		t.Fatalf("workspace ops refresh upsert failed: %v", err)
	}

	var refreshedPayload struct {
		Item struct {
			Revision    int64  `json:"revision"`
			UpdatedAt   string `json:"updated_at"`
			Summary     string `json:"summary"`
			PayloadJSON string `json:"payload_json"`
		} `json:"item"`
	}
	if err := json.Unmarshal([]byte(refreshedOut), &refreshedPayload); err != nil {
		t.Fatalf("decode refreshed workspace ops upsert output: %v; output=%q", err, refreshedOut)
	}
	if refreshedPayload.Item.Summary != "Check live doctor verdict after refresh" {
		t.Fatalf("unexpected refreshed workspace ops payload %+v", refreshedPayload)
	}
	if strings.TrimSpace(refreshedPayload.Item.UpdatedAt) == "" || refreshedPayload.Item.UpdatedAt == opsPayload.Item.UpdatedAt {
		t.Fatalf("expected refreshed updated_at after queue edit, initial=%q refreshed=%q", opsPayload.Item.UpdatedAt, refreshedPayload.Item.UpdatedAt)
	}
	if refreshedPayload.Item.Revision != opsPayload.Item.Revision+1 {
		t.Fatalf("expected refreshed revision after queue edit, initial=%d refreshed=%d", opsPayload.Item.Revision, refreshedPayload.Item.Revision)
	}
	assertCLIOperatorQueuePromptContextSurface(t, refreshedPayload.Item.PayloadJSON, "cli.workspace.ops.upsert")
	if err := runWorkspaceOps([]string{
		"upsert",
		"--workspace-id", "ws-control-cli",
		"--queue-key", "manual:deploy-gate",
		"--queue-type", "FOLLOW_UP",
		"--title", "Confirm deploy gate",
		"--summary", "Blind refresh should fail",
		"--assigned-to", "developer",
		"--urgency", "HIGH",
		"--source-kind", "manual",
		"--source-id", "developer",
		"--agent-id", "agent-control-cli",
		"--keep-session-active",
	}); err == nil || !strings.Contains(err.Error(), "current_revision") {
		t.Fatalf("expected blind workspace ops upsert to fail with current_revision guidance, got %v", err)
	}
	if err := runWorkspaceOps([]string{
		"upsert",
		"--workspace-id", "ws-control-cli",
		"--queue-id", "opq-bogus",
		"--queue-key", "manual:deploy-gate",
		"--queue-type", "FOLLOW_UP",
		"--title", "Confirm deploy gate",
		"--summary", "Mismatched identity should fail",
		"--assigned-to", "developer",
		"--urgency", "HIGH",
		"--source-kind", "manual",
		"--source-id", "developer",
		"--agent-id", "agent-control-cli",
		"--keep-session-active",
	}); err == nil || !strings.Contains(err.Error(), "queue_id and queue_key") {
		t.Fatalf("expected mismatched workspace ops upsert to fail with queue identity guidance, got %v", err)
	}

	if err := runWorkspaceOps([]string{
		"upsert",
		"--workspace-id", "ws-control-cli",
		"--queue-key", "manual:deploy-gate",
		"--queue-type", "FOLLOW_UP",
		"--title", "Confirm deploy gate",
		"--summary", "Stale edit should fail",
		"--assigned-to", "developer",
		"--urgency", "HIGH",
		"--source-kind", "manual",
		"--source-id", "developer",
		"--agent-id", "agent-control-cli",
		"--keep-session-active",
		"--current-revision", strconv.FormatInt(opsPayload.Item.Revision, 10),
		"--current-updated-at", opsPayload.Item.UpdatedAt,
	}); err == nil || !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale workspace ops upsert to fail with revision guard, got %v", err)
	}

	claimOut, err := captureStdout(t, func() error {
		return runWorkspaceClaim([]string{
			"write",
			"--workspace-id", "ws-control-cli",
			"--claim-type", "DECISION",
			"--subject", "Runtime journal is canonical",
			"--body", "Operators should use runtime events instead of archived traces.",
			"--summary", "Use runtime events as the source of truth.",
			"--confidence", "0.9",
			"--agent-id", "agent-control-cli",
			"--tags", "runtime,truth",
		})
	})
	if err != nil {
		t.Fatalf("workspace claim write failed: %v", err)
	}

	var claimPayload struct {
		Claim struct {
			ClaimID string `json:"claim_id"`
		} `json:"claim"`
	}
	if err := json.Unmarshal([]byte(claimOut), &claimPayload); err != nil {
		t.Fatalf("decode workspace claim write output: %v; output=%q", err, claimOut)
	}
	if claimPayload.Claim.ClaimID == "" {
		t.Fatalf("expected claim id in workspace claim write output, got %q", claimOut)
	}

	reviewOut, err := captureStdout(t, func() error {
		return runWorkspaceClaim([]string{
			"review",
			"--workspace-id", "ws-control-cli",
			"--claim-id", claimPayload.Claim.ClaimID,
			"--actor-id", "developer",
			"--reason", "needs explicit confirmation",
			"--due-at", "2026-03-23T09:00:00Z",
			"--assigned-to", "reviewer-cli",
		})
	})
	if err != nil {
		t.Fatalf("workspace claim review failed: %v", err)
	}

	var reviewPayload struct {
		Status string `json:"status"`
		Claim  struct {
			ReviewDueAt *string `json:"review_due_at"`
		} `json:"claim"`
	}
	if err := json.Unmarshal([]byte(reviewOut), &reviewPayload); err != nil {
		t.Fatalf("decode workspace claim review output: %v; output=%q", err, reviewOut)
	}
	if reviewPayload.Status != "REVIEW" || reviewPayload.Claim.ReviewDueAt == nil {
		t.Fatalf("unexpected workspace claim review payload %+v", reviewPayload)
	}

	escalateClaimOut, err := captureStdout(t, func() error {
		return runWorkspaceClaim([]string{
			"escalate",
			"--workspace-id", "ws-control-cli",
			"--claim-id", claimPayload.Claim.ClaimID,
			"--actor-id", "developer",
			"--reason", "review queue is approaching SLA breach",
			"--assigned-to", "reviewer-cli-escalated",
			"--urgency", "CRITICAL",
			"--due-at", "2099-01-01T00:00:00Z",
		})
	})
	if err != nil {
		t.Fatalf("workspace claim escalate failed: %v", err)
	}

	var escalateClaimPayload struct {
		Status string `json:"status"`
		Queue  struct {
			AssignedTo      string `json:"assigned_to"`
			Urgency         string `json:"urgency"`
			EscalationCount int    `json:"escalation_count"`
		} `json:"queue"`
	}
	if err := json.Unmarshal([]byte(escalateClaimOut), &escalateClaimPayload); err != nil {
		t.Fatalf("decode workspace claim escalate output: %v; output=%q", err, escalateClaimOut)
	}
	if escalateClaimPayload.Status != "REVIEW" || escalateClaimPayload.Queue.AssignedTo != "reviewer-cli-escalated" || escalateClaimPayload.Queue.Urgency != "CRITICAL" || escalateClaimPayload.Queue.EscalationCount != 1 {
		t.Fatalf("unexpected workspace claim escalate payload %+v", escalateClaimPayload)
	}

	confirmOut, err := captureStdout(t, func() error {
		return runWorkspaceClaim([]string{
			"confirm",
			"--workspace-id", "ws-control-cli",
			"--claim-id", claimPayload.Claim.ClaimID,
			"--actor-id", "developer",
			"--reason", "confirmed from live runtime",
		})
	})
	if err != nil {
		t.Fatalf("workspace claim confirm failed: %v", err)
	}

	var confirmPayload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(confirmOut), &confirmPayload); err != nil {
		t.Fatalf("decode workspace claim confirm output: %v; output=%q", err, confirmOut)
	}
	if confirmPayload.Status != "CONFIRMED" {
		t.Fatalf("unexpected workspace claim confirm payload %+v", confirmPayload)
	}

	searchOut, err := captureStdout(t, func() error {
		return runWorkspaceClaim([]string{
			"search",
			"--workspace-id", "ws-control-cli",
			"--query", "archived traces",
		})
	})
	if err != nil {
		t.Fatalf("workspace claim search failed: %v", err)
	}

	var searchPayload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(searchOut), &searchPayload); err != nil {
		t.Fatalf("decode workspace claim search output: %v; output=%q", err, searchOut)
	}
	if searchPayload.Count != 1 {
		t.Fatalf("expected one claim search result, got %+v", searchPayload)
	}

	runOut, err := captureStdout(t, func() error {
		return runWorkspaceExecution([]string{
			"run", "write",
			"--workspace-id", "ws-control-cli",
			"--run-id", "run-control-cli",
			"--agent-id", "agent-control-cli",
			"--title", "Rollout control plane",
			"--summary", "Track deploy and verification steps.",
			"--status", "ACTIVE",
		})
	})
	if err != nil {
		t.Fatalf("workspace execution run write failed: %v", err)
	}

	var runPayload struct {
		Run struct {
			RunID            string         `json:"run_id"`
			VerificationJSON map[string]any `json:"verification"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(runOut), &runPayload); err != nil {
		t.Fatalf("decode workspace execution run output: %v; output=%q", err, runOut)
	}
	if runPayload.Run.RunID != "run-control-cli" {
		t.Fatalf("unexpected execution run payload %+v", runPayload)
	}
	runEnvelope, ok := runPayload.Run.VerificationJSON["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution run prompt context envelope, got %+v", runPayload.Run.VerificationJSON)
	}
	if got := runEnvelope["surface"]; got != "cli.workspace.execution.run.write" {
		t.Fatalf("unexpected run prompt context surface: %v", got)
	}

	stepOut, err := captureStdout(t, func() error {
		return runWorkspaceExecution([]string{
			"step", "write",
			"--workspace-id", "ws-control-cli",
			"--run-id", "run-control-cli",
			"--phase", "EXECUTE",
			"--title", "Check deploy gate",
			"--summary", "Doctor must pass against live health.",
			"--status", "ACTIVE",
			"--evidence", "doctor,health",
			"--verification", `{"gate":"pass"}`,
		})
	})
	if err != nil {
		t.Fatalf("workspace execution step write failed: %v", err)
	}

	var stepPayload struct {
		Step struct {
			StepID           string         `json:"step_id"`
			VerificationJSON map[string]any `json:"verification"`
		} `json:"step"`
	}
	if err := json.Unmarshal([]byte(stepOut), &stepPayload); err != nil {
		t.Fatalf("decode workspace execution step output: %v; output=%q", err, stepOut)
	}
	if stepPayload.Step.StepID == "" {
		t.Fatalf("expected execution step id, got %+v", stepPayload)
	}
	if stepPayload.Step.VerificationJSON["gate"] != "pass" {
		t.Fatalf("expected CLI step verification to preserve caller fields, got %+v", stepPayload.Step.VerificationJSON)
	}
	stepEnvelope, ok := stepPayload.Step.VerificationJSON["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution step prompt context envelope, got %+v", stepPayload.Step.VerificationJSON)
	}
	if got := stepEnvelope["surface"]; got != "cli.workspace.execution.step.write" {
		t.Fatalf("unexpected step prompt context surface: %v", got)
	}
	if got := stepEnvelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("CLI envelope must not claim daemon convergence, got %+v", stepEnvelope)
	}

	policyOut, err := captureStdout(t, func() error {
		return runWorkspacePolicy([]string{
			"put",
			"--workspace-id", "ws-control-cli",
			"--subject-type", "agent",
			"--subject-id", "agent-control-cli",
			"--capability", "tool.call",
			"--tool-id", "dangerous-tool",
			"--effect", "REQUIRE_APPROVAL",
			"--reason", "manual approval required",
			"--created-by", "developer",
		})
	})
	if err != nil {
		t.Fatalf("workspace policy put failed: %v", err)
	}

	var policyPayload struct {
		Policy struct {
			PolicyID string `json:"policy_id"`
			Effect   string `json:"effect"`
		} `json:"policy"`
	}
	if err := json.Unmarshal([]byte(policyOut), &policyPayload); err != nil {
		t.Fatalf("decode workspace policy put output: %v; output=%q", err, policyOut)
	}
	if policyPayload.Policy.Effect != "REQUIRE_APPROVAL" {
		t.Fatalf("unexpected workspace policy put payload %+v", policyPayload)
	}
	policyEvent := requireCLIRuntimeEvent(t, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-control-cli",
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    policyPayload.Policy.PolicyID,
		Limit:       10,
	})
	policyRuntimePayload := decodeCLIRuntimePayload(t, policyEvent.PayloadJSON)
	policyEnvelope := requireCLIPromptContextEnvelope(t, policyRuntimePayload)
	assertCLIPromptContextEnvelope(t, policyEnvelope, "authority_bearing_capability_policy_write", "cli.workspace.policy.put", "ws-control-cli", "operator", "developer")
	if got := policyEnvelope["policy_id"]; got != policyPayload.Policy.PolicyID {
		t.Fatalf("unexpected CLI policy prompt context policy_id: got %v want %s in %+v", got, policyPayload.Policy.PolicyID, policyEnvelope)
	}
	if got := policyEnvelope["effect"]; got != "REQUIRE_APPROVAL" {
		t.Fatalf("unexpected CLI policy prompt context effect: got %v in %+v", got, policyEnvelope)
	}

	checkOut, err := captureStdout(t, func() error {
		return runWorkspacePolicy([]string{
			"check",
			"--workspace-id", "ws-control-cli",
			"--subject-type", "agent",
			"--subject-id", "agent-control-cli",
			"--capability", "tool.call",
			"--tool-id", "dangerous-tool",
		})
	})
	if err != nil {
		t.Fatalf("workspace policy check failed: %v", err)
	}

	var checkPayload struct {
		Check struct {
			Verdict string `json:"verdict"`
		} `json:"check"`
	}
	if err := json.Unmarshal([]byte(checkOut), &checkPayload); err != nil {
		t.Fatalf("decode workspace policy check output: %v; output=%q", err, checkOut)
	}
	if checkPayload.Check.Verdict != "REQUIRE_APPROVAL" {
		t.Fatalf("unexpected workspace policy check payload %+v", checkPayload)
	}

	eventsOut, err := captureStdout(t, func() error {
		return runWorkspaceEvents([]string{
			"list",
			"--workspace-id", "ws-control-cli",
			"--limit", "20",
		})
	})
	if err != nil {
		t.Fatalf("workspace events list failed: %v", err)
	}

	var eventsPayload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(eventsOut), &eventsPayload); err != nil {
		t.Fatalf("decode workspace events output: %v; output=%q", err, eventsOut)
	}
	if eventsPayload.Count < 4 {
		t.Fatalf("expected runtime events from control plane operations, got %+v", eventsPayload)
	}

	escalateOpsOut, err := captureStdout(t, func() error {
		return runWorkspaceOps([]string{
			"escalate",
			"--workspace-id", "ws-control-cli",
			"--queue-key", "manual:deploy-gate",
			"--escalated-by", "developer",
			"--reason", "deploy gate is overdue",
			"--urgency", "CRITICAL",
			"--current-revision", strconv.FormatInt(refreshedPayload.Item.Revision, 10),
			"--current-updated-at", refreshedPayload.Item.UpdatedAt,
		})
	})
	if err != nil {
		t.Fatalf("workspace ops escalate failed: %v", err)
	}

	var escalateOpsPayload struct {
		Item struct {
			EscalationCount int    `json:"escalation_count"`
			Revision        int64  `json:"revision"`
			UpdatedAt       string `json:"updated_at"`
			Urgency         string `json:"urgency"`
			PayloadJSON     string `json:"payload_json"`
		} `json:"item"`
	}
	if err := json.Unmarshal([]byte(escalateOpsOut), &escalateOpsPayload); err != nil {
		t.Fatalf("decode workspace ops escalate output: %v; output=%q", err, escalateOpsOut)
	}
	if escalateOpsPayload.Item.EscalationCount != 1 || escalateOpsPayload.Item.Urgency != "CRITICAL" {
		t.Fatalf("unexpected workspace ops escalate payload %+v", escalateOpsPayload)
	}
	if escalateOpsPayload.Item.Revision < refreshedPayload.Item.Revision+1 {
		t.Fatalf("expected escalate to advance queue revision, refreshed=%d escalated=%d", refreshedPayload.Item.Revision, escalateOpsPayload.Item.Revision)
	}
	assertCLIOperatorQueuePromptContextSurface(t, escalateOpsPayload.Item.PayloadJSON, "cli.workspace.ops.escalate")

	resolveOut, err := captureStdout(t, func() error {
		return runWorkspaceOps([]string{
			"resolve",
			"--workspace-id", "ws-control-cli",
			"--queue-key", "manual:deploy-gate",
			"--resolved-by", "developer",
			"--resolution", "doctor passed",
			"--current-revision", strconv.FormatInt(escalateOpsPayload.Item.Revision, 10),
			"--current-updated-at", escalateOpsPayload.Item.UpdatedAt,
		})
	})
	if err != nil {
		t.Fatalf("workspace ops resolve failed: %v", err)
	}

	var resolvePayload struct {
		Item struct {
			Status      string `json:"status"`
			Revision    int64  `json:"revision"`
			PayloadJSON string `json:"payload_json"`
		} `json:"item"`
	}
	if err := json.Unmarshal([]byte(resolveOut), &resolvePayload); err != nil {
		t.Fatalf("decode workspace ops resolve output: %v; output=%q", err, resolveOut)
	}
	if resolvePayload.Item.Status != "RESOLVED" {
		t.Fatalf("unexpected workspace ops resolve payload %+v", resolvePayload)
	}
	if resolvePayload.Item.Revision < escalateOpsPayload.Item.Revision+1 {
		t.Fatalf("expected resolve to advance queue revision, escalated=%d resolved=%d", escalateOpsPayload.Item.Revision, resolvePayload.Item.Revision)
	}
	assertCLIOperatorQueuePromptContextSurface(t, resolvePayload.Item.PayloadJSON, "cli.workspace.ops.resolve")

	archiveOut, err := captureStdout(t, func() error {
		return runWorkspaceClaim([]string{
			"archive",
			"--workspace-id", "ws-control-cli",
			"--claim-id", claimPayload.Claim.ClaimID,
			"--archived-by", "developer",
			"--reason", "superseded",
		})
	})
	if err != nil {
		t.Fatalf("workspace claim archive failed: %v", err)
	}

	var archivePayload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(archiveOut), &archivePayload); err != nil {
		t.Fatalf("decode workspace claim archive output: %v; output=%q", err, archiveOut)
	}
	if archivePayload.Status != "ARCHIVED" {
		t.Fatalf("unexpected workspace claim archive payload %+v", archivePayload)
	}

	getOut, err := captureStdout(t, func() error {
		return runWorkspaceExecution([]string{
			"run", "get",
			"--workspace-id", "ws-control-cli",
			"--run-id", "run-control-cli",
		})
	})
	if err != nil {
		t.Fatalf("workspace execution run get failed: %v", err)
	}

	var getPayload struct {
		Detail struct {
			Run struct {
				RunID string `json:"run_id"`
			} `json:"run"`
			Steps []struct {
				StepID string `json:"step_id"`
			} `json:"steps"`
		} `json:"detail"`
	}
	if err := json.Unmarshal([]byte(getOut), &getPayload); err != nil {
		t.Fatalf("decode workspace execution run get output: %v; output=%q", err, getOut)
	}
	if getPayload.Detail.Run.RunID != "run-control-cli" || len(getPayload.Detail.Steps) != 1 {
		t.Fatalf("unexpected workspace execution run get payload %+v", getPayload)
	}
}

func TestWorkspaceExecutionStepWriteCLIPreservesOmittedEvidenceAndVerification(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-control-cli-preserve-proof"
		runID       = "run-control-cli-preserve-proof"
		stepID      = "step-control-cli-preserve-proof"
		snapshotID  = "cap_cli_preserve"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Control Plane CLI Preserve Proof",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if err := runWorkspaceExecution([]string{
		"run", "write",
		"--workspace-id", workspaceID,
		"--run-id", runID,
		"--title", "Preserve proof run",
		"--status", "ACTIVE",
	}); err != nil {
		t.Fatalf("workspace execution run write failed: %v", err)
	}

	proofJSON, err := json.Marshal(map[string]any{
		"prompt_capability_evidence": validCLIPromptCapabilityEvidence(snapshotID),
	})
	if err != nil {
		t.Fatalf("marshal prompt proof: %v", err)
	}
	if err := runWorkspaceExecution([]string{
		"step", "write",
		"--workspace-id", workspaceID,
		"--run-id", runID,
		"--step-id", stepID,
		"--phase", "PLAN",
		"--title", "Initial proof",
		"--status", "COMPLETED",
		"--evidence", "capability_snapshot:" + snapshotID,
		"--verification", string(proofJSON),
	}); err != nil {
		t.Fatalf("initial workspace execution step write failed: %v", err)
	}

	updateOut, err := captureStdout(t, func() error {
		return runWorkspaceExecution([]string{
			"step", "write",
			"--workspace-id", workspaceID,
			"--run-id", runID,
			"--step-id", stepID,
			"--phase", "PLAN",
			"--title", "Updated proof title",
			"--status", "COMPLETED",
		})
	})
	if err != nil {
		t.Fatalf("omitted evidence/verification update failed: %v", err)
	}

	var payload struct {
		Step struct {
			Evidence         []string       `json:"evidence"`
			VerificationJSON map[string]any `json:"verification"`
		} `json:"step"`
	}
	if err := json.Unmarshal([]byte(updateOut), &payload); err != nil {
		t.Fatalf("decode update output: %v; output=%q", err, updateOut)
	}
	if !containsCLIString(payload.Step.Evidence, "capability_snapshot:"+snapshotID) {
		t.Fatalf("expected durable prompt proof evidence to survive omitted update, got %+v", payload.Step.Evidence)
	}
	if _, ok := payload.Step.VerificationJSON["prompt_capability_evidence"].(map[string]any); !ok {
		t.Fatalf("expected prompt proof to survive omitted update, got %+v", payload.Step.VerificationJSON)
	}
	if _, ok := payload.Step.VerificationJSON["prompt_context_envelope"].(map[string]any); !ok {
		t.Fatalf("expected CLI prompt context envelope after update, got %+v", payload.Step.VerificationJSON)
	}
}

func validCLIPromptCapabilityEvidence(snapshotID string) map[string]any {
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

func containsCLIString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func assertCLIOperatorQueuePromptContextSurface(t *testing.T, payloadJSON, wantSurface string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode CLI operator queue payload_json: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected CLI operator queue prompt context envelope, got %+v", payload)
	}
	if got := envelope["surface"]; got != wantSurface {
		t.Fatalf("unexpected CLI operator queue context surface: got %v want %s", got, wantSurface)
	}
	if got := envelope["origin"]; got != "cli_local" {
		t.Fatalf("unexpected CLI operator queue context origin: %v", got)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_operator_queue_write" {
		t.Fatalf("unexpected CLI operator queue context kind: %v", got)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("CLI operator queue context must not claim daemon convergence: %+v", envelope)
	}
}

func TestWorkspaceClaimWriteCLIRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-control-cli-claim-missing-authority"
		claimID     = "claim-control-cli-missing-authority"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Control Plane CLI Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	err := runWorkspaceClaim([]string{
		"write",
		"--workspace-id", workspaceID,
		"--claim-id", claimID,
		"--claim-type", "DECISION",
		"--subject", "CLI claim write missing authority",
		"--body", "should fail closed before any claim/event side effect",
		"--source-kind", "manual",
		"--source-id", "developer",
	})
	if err == nil {
		t.Fatal("expected workspace claim write CLI to fail without workspace authority")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority_missing reject, got %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	var claimCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM knowledge_claims WHERE workspace_id = ? AND claim_id = ?`, workspaceID, claimID).Scan(&claimCount); err != nil {
		t.Fatalf("count knowledge_claim rows: %v", err)
	}
	if claimCount != 0 {
		t.Fatalf("expected no knowledge_claim row after authority reject, got %d", claimCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no knowledge_claim.written events after authority reject, got %+v", events)
	}
	var invalidationCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM memory_invalidation_queue WHERE workspace_id = ? AND ref_kind = ? AND ref_id = ?`, workspaceID, "knowledge_claim", claimID).Scan(&invalidationCount); err != nil {
		t.Fatalf("count claim invalidation rows: %v", err)
	}
	if invalidationCount != 0 {
		t.Fatalf("expected no invalidation rows after authority reject, got %d", invalidationCount)
	}
}

func TestWorkspaceClaimWriteCLIStampsAuthorityMetadata(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-control-cli-claim-authority-metadata"
		claimID     = "claim-control-cli-authority-metadata"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Control Plane CLI Claim Authority Metadata",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	authority := claimCLITestWorkspaceAuthority(t, workspaceID)

	if err := runWorkspaceClaim([]string{
		"write",
		"--workspace-id", workspaceID,
		"--claim-id", claimID,
		"--claim-type", "DECISION",
		"--subject", "CLI claim write authority metadata",
		"--body", "operator-facing CLI should stamp authority metadata on knowledge_claim.written",
		"--source-kind", "manual",
		"--source-id", "developer",
	}); err != nil {
		t.Fatalf("workspace claim write failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one knowledge_claim.written event, got %d", len(events))
	}
	if events[0].AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %q", authority.HolderAuthorityNodeID, events[0].AuthorityHolderNodeID)
	}
	if events[0].AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %d", authority.Term, events[0].AuthorityTerm)
	}
	if got, want := events[0].AuthorityLeaseTokenFingerprint, cliTestAuthorityLeaseTokenFingerprint(authority.LeaseToken); got != want {
		t.Fatalf("expected authority lease fingerprint %q, got %q", want, got)
	}
}

func TestWorkspaceExecutionRunWriteCLIRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-control-cli-execution-run-missing-authority"
		runID       = "run-control-cli-missing-authority"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Control Plane CLI Execution Run Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	err := runWorkspaceExecution([]string{
		"run", "write",
		"--workspace-id", workspaceID,
		"--run-id", runID,
		"--title", "CLI execution run missing authority",
		"--summary", "should fail closed before any execution run side effect",
		"--status", "ACTIVE",
	})
	if err == nil {
		t.Fatal("expected workspace execution run write CLI to fail without workspace authority")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority_missing reject, got %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	var runCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_runs WHERE workspace_id = ? AND run_id = ?`, workspaceID, runID).Scan(&runCount); err != nil {
		t.Fatalf("count execution run rows: %v", err)
	}
	if runCount != 0 {
		t.Fatalf("expected no execution_run row after authority reject, got %d", runCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    runID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no execution_run.written events after authority reject, got %+v", events)
	}
}

func TestWorkspaceExecutionCLIStampsAuthorityMetadata(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-control-cli-execution-authority-metadata"
		runID       = "run-control-cli-authority-metadata"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Control Plane CLI Execution Authority Metadata",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	authority := claimCLITestWorkspaceAuthority(t, workspaceID)

	if err := runWorkspaceExecution([]string{
		"run", "write",
		"--workspace-id", workspaceID,
		"--run-id", runID,
		"--title", "CLI execution run authority metadata",
		"--summary", "operator-facing CLI should stamp authority metadata on execution_run.written",
		"--status", "ACTIVE",
	}); err != nil {
		t.Fatalf("workspace execution run write failed: %v", err)
	}

	if err := runWorkspaceExecution([]string{
		"step", "write",
		"--workspace-id", workspaceID,
		"--run-id", runID,
		"--phase", "EXECUTE",
		"--title", "CLI execution step authority metadata",
		"--summary", "operator-facing CLI should stamp authority metadata on execution_step.written",
		"--status", "ACTIVE",
		"--evidence", "doctor,health",
		"--verification", `{"gate":"pass"}`,
	}); err != nil {
		t.Fatalf("workspace execution step write failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	runEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    runID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list execution run events: %v", err)
	}
	if len(runEvents) != 1 {
		t.Fatalf("expected one execution_run.written event, got %d", len(runEvents))
	}
	if runEvents[0].AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected run authority holder %q, got %q", authority.HolderAuthorityNodeID, runEvents[0].AuthorityHolderNodeID)
	}
	if runEvents[0].AuthorityTerm != authority.Term {
		t.Fatalf("expected run authority term %d, got %d", authority.Term, runEvents[0].AuthorityTerm)
	}
	if got, want := runEvents[0].AuthorityLeaseTokenFingerprint, cliTestAuthorityLeaseTokenFingerprint(authority.LeaseToken); got != want {
		t.Fatalf("expected run authority lease fingerprint %q, got %q", want, got)
	}

	stepEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list execution step events: %v", err)
	}
	if len(stepEvents) != 1 {
		t.Fatalf("expected one execution_step.written event, got %d", len(stepEvents))
	}
	if stepEvents[0].AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected step authority holder %q, got %q", authority.HolderAuthorityNodeID, stepEvents[0].AuthorityHolderNodeID)
	}
	if stepEvents[0].AuthorityTerm != authority.Term {
		t.Fatalf("expected step authority term %d, got %d", authority.Term, stepEvents[0].AuthorityTerm)
	}
	if got, want := stepEvents[0].AuthorityLeaseTokenFingerprint, cliTestAuthorityLeaseTokenFingerprint(authority.LeaseToken); got != want {
		t.Fatalf("expected step authority lease fingerprint %q, got %q", want, got)
	}
}

func cliTestAuthorityLeaseTokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	encoded := hex.EncodeToString(sum[:])
	if len(encoded) > 16 {
		encoded = encoded[:16]
	}
	return "sha256:" + encoded
}

func TestWorkspaceEventsReplayAndEvaluateCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-cli-replay",
		"--title", "CLI Replay",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-cli-replay")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-cli-replay",
		"--agent-id", "agent-cli",
		"--owner-user-id", "developer",
		"--display-name", "Replay CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	keepAlive := true
	payloadBytes, err := json.Marshal(sqlite.AgentSessionStateRecord{
		SessionID:         "session-cli-replay",
		WorkspaceID:       "ws-cli-replay",
		AgentID:           "agent-cli",
		Status:            model.SessionStatusBlocked,
		Summary:           "Waiting on bridge wake acknowledgement",
		KeepSessionActive: &keepAlive,
		BlockedOn:         []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
		UpdatedAt:         "2026-03-22T11:00:00Z",
		StartedAt:         "2026-03-22T10:55:00Z",
	})
	if err != nil {
		t.Fatalf("marshal session payload: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-cli-replay",
		WorkspaceID: "ws-cli-replay",
		EventType:   "session.blocked",
		EntityType:  "agent_session",
		EntityID:    "session-cli-replay",
		ActorType:   "agent",
		ActorID:     "agent-cli",
		AgentID:     "agent-cli",
		PayloadJSON: string(payloadBytes),
		CreatedAt:   "2026-03-22T11:00:00Z",
	}); err != nil {
		t.Fatalf("record runtime event: %v", err)
	}

	replayOut, err := captureStdout(t, func() error {
		return runWorkspaceEvents([]string{
			"replay",
			"--workspace-id", "ws-cli-replay",
			"--include-events",
		})
	})
	if err != nil {
		t.Fatalf("workspace events replay failed: %v", err)
	}

	var replayPayload struct {
		Report struct {
			Events []struct {
				EventID string `json:"event_id"`
			} `json:"events"`
			Sessions []struct {
				SessionID string `json:"session_id"`
				Status    string `json:"status"`
			} `json:"sessions"`
		} `json:"report"`
	}
	if err := json.Unmarshal([]byte(replayOut), &replayPayload); err != nil {
		t.Fatalf("decode workspace events replay output: %v; output=%q", err, replayOut)
	}
	if len(replayPayload.Report.Events) != 1 || len(replayPayload.Report.Sessions) != 1 {
		t.Fatalf("unexpected replay payload %+v", replayPayload)
	}
	if replayPayload.Report.Sessions[0].SessionID != "session-cli-replay" || replayPayload.Report.Sessions[0].Status != model.SessionStatusBlocked {
		t.Fatalf("unexpected replay session payload %+v", replayPayload.Report.Sessions[0])
	}

	evaluateOut, err := captureStdout(t, func() error {
		return runWorkspaceEvents([]string{
			"evaluate",
			"--workspace-id", "ws-cli-replay",
		})
	})
	if err != nil {
		t.Fatalf("workspace events evaluate failed: %v", err)
	}

	var evaluatePayload struct {
		Evaluation struct {
			Verdict  string `json:"verdict"`
			Findings []struct {
				Code string `json:"code"`
			} `json:"findings"`
		} `json:"evaluation"`
	}
	if err := json.Unmarshal([]byte(evaluateOut), &evaluatePayload); err != nil {
		t.Fatalf("decode workspace events evaluate output: %v; output=%q", err, evaluateOut)
	}
	if evaluatePayload.Evaluation.Verdict != "warn" {
		t.Fatalf("expected warn verdict, got %+v", evaluatePayload.Evaluation)
	}
	foundMissingQueue := false
	for _, finding := range evaluatePayload.Evaluation.Findings {
		if finding.Code == "missing_operator_queue" {
			foundMissingQueue = true
			break
		}
	}
	if !foundMissingQueue {
		t.Fatalf("expected missing_operator_queue finding, got %+v", evaluatePayload.Evaluation.Findings)
	}
}
