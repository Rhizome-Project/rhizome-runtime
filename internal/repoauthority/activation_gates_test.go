package repoauthority

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMutationActivationGatesPatchOnlyModeStaysBlocked(t *testing.T) {
	input := mutationActivationReadyInput(t)
	input.AuthorityMode = ModePatchOnlyTempRepo

	result := EvaluateMutationActivationGates(input)

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	if result.MutationAllowed {
		t.Fatalf("mutation_allowed = true")
	}
	gate := mutationActivationGateByName(t, result, "controlled_authority_mode")
	if gate.Passed {
		t.Fatalf("controlled_authority_mode passed in patch-only mode")
	}
	if !strings.Contains(gate.Reason, ModePatchOnlyTempRepo) {
		t.Fatalf("gate reason = %q, want patch-only mode", gate.Reason)
	}
}

func TestMutationActivationGatesMissingProofsFailClosed(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{})

	if result.Schema != MutationActivationGateSchemaVersion {
		t.Fatalf("schema = %q", result.Schema)
	}
	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	if result.MutationAllowed {
		t.Fatalf("mutation_allowed = true")
	}
	for _, name := range []string{
		"controlled_authority_mode",
		"controlled_context_mode",
		"direct_merge_disabled",
		"durable_patch_queue",
		"canonical_worktree_identity",
		"live_actuator_target_identity",
		"mutation_binding",
		"merge_admission_conflict_safe",
		"bounded_retry",
		"rollback_proven",
		"reviewer_advisory_recorded",
		"reviewer_mesh_advisory_only",
		"operator_enablement_recorded",
		"materialization_preflight_verified",
		"live_mutation_verifier_enabled",
		"live_mutation_actuator_enabled",
	} {
		if mutationActivationGateByName(t, result, name).Passed {
			t.Fatalf("gate %s passed with empty input; result=%+v", name, result)
		}
	}
	if !isCanonicalSHA256Digest(result.Digest) {
		t.Fatalf("digest = %q, want canonical sha256", result.Digest)
	}
}

func TestMutationActivationGatesCarryCandidateSummaryInDigest(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{
		AuthorityMode: MutationActivationAuthorityModeControlledQueue,
		Source:        MutationActivationSourceDurableControlledQueueCandidate,
		Context: Context{
			Mode:        MutationActivationAuthorityModeControlledQueue,
			WorkspaceID: " ws-controlled ",
			PatchQueue: PatchQueueRef{
				QueueID: " patchq-main ",
				ItemID:  " patchitem-ready ",
			},
		},
		Candidate: &MutationActivationCandidateSummary{
			WorkspaceID: " ws-controlled ",
			ProjectID:   " project-controlled ",
			RepoID:      " repo-main ",
			QueueID:     " patchq-main ",
			ItemID:      " patchitem-ready ",
			BranchID:    " branch-ready ",
			BranchName:  " agent/alpha/task ",
			CheckoutID:  " checkout-alpha ",
			State:       " PROPOSED ",
			BaseSHA:     " " + strings.Repeat("a", 40) + " ",
			HeadSHA:     " " + strings.Repeat("b", 40) + " ",
		},
		WorktreeIdentity: WorktreeIdentityEvidence{
			RepoID:               " repo-main ",
			CheckoutID:           " checkout-alpha ",
			BranchID:             " branch-ready ",
			BranchName:           " agent/alpha/task ",
			MachineID:            " machine-a ",
			LocalPath:            " C:/fixtures/agents/alpha/project ",
			BaseSHA:              " " + strings.Repeat("a", 40) + " ",
			HeadSHA:              " " + strings.Repeat("b", 40) + " ",
			ReadbackState:        " ok ",
			ObservedWorktreeRoot: " C:/fixtures/agents/alpha/project ",
			ObservedBranchName:   " agent/alpha/task ",
			ObservedHeadSHA:      " " + strings.Repeat("b", 40) + " ",
			ObservedDirtyState:   " clean ",
		},
	})

	if result.Candidate == nil {
		t.Fatalf("expected candidate summary")
	}
	if result.Candidate.QueueID != "patchq-main" ||
		result.Candidate.ItemID != "patchitem-ready" ||
		result.Candidate.BranchName != "agent/alpha/task" ||
		result.Candidate.BaseSHA != strings.Repeat("a", 40) ||
		result.Candidate.HeadSHA != strings.Repeat("b", 40) {
		t.Fatalf("candidate summary was not normalized: %+v", result.Candidate)
	}
	if result.MutationBindingEvidence == nil ||
		result.MutationBindingEvidence.WorkspaceID != "ws-controlled" ||
		result.MutationBindingEvidence.PatchQueueID != "patchq-main" ||
		result.MutationBindingEvidence.PatchQueueItemID != "patchitem-ready" ||
		!testStringSliceContains(result.MutationBindingEvidence.MissingRefs, "task_id") {
		t.Fatalf("expected normalized fail-closed mutation binding evidence, got %+v", result.MutationBindingEvidence)
	}
	if err := VerifyMutationActivationGateResult(result); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}

	result.Candidate.ItemID = "patchitem-other"
	if err := VerifyMutationActivationGateResult(result); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("expected candidate mutation to invalidate digest, got %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsCandidateOutsideControlledSource(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{
		Source: MutationActivationSourceSyntheticPatchOnly,
		Candidate: &MutationActivationCandidateSummary{
			WorkspaceID: "ws-controlled",
			ProjectID:   "project-controlled",
			RepoID:      "repo-main",
			QueueID:     "patchq-main",
			ItemID:      "patchitem-ready",
			BranchID:    "branch-ready",
			BranchName:  "agent/alpha/task",
			CheckoutID:  "checkout-alpha",
			State:       "PROPOSED",
			BaseSHA:     strings.Repeat("a", 40),
			HeadSHA:     strings.Repeat("b", 40),
		},
	})

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected candidate on synthetic source to fail verification")
	}
	if !strings.Contains(err.Error(), "candidate summary requires source") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestMutationActivationBindingAllowsExactAddPathWithoutBaseHashes(t *testing.T) {
	ctx := validContext()
	ctx.Mode = ModeControlledQueue
	ctx.Base.FileHashes = map[string]string{}
	ctx.Pathset = []string{"web/new.js"}
	ctx.PatchQueue = PatchQueueRef{QueueID: "patchq-add", ItemID: "patchitem-add"}
	ctx.Operation = OperationRef{ID: "op-add", Kind: "repo_patch_apply"}

	contextDigest, err := patchQueueContextDigest(ctx)
	if err != nil {
		t.Fatalf("patch queue context digest: %v", err)
	}
	item := PatchQueueItem{
		Schema:        PatchQueueItemSchemaVersion,
		ID:            "record-add",
		QueueID:       ctx.PatchQueue.QueueID,
		ItemID:        ctx.PatchQueue.ItemID,
		State:         PatchQueueStateApplied,
		ContextDigest: contextDigest,
		RepoLeaseID:   ctx.Lease.ID,
		LeaseTerm:     ctx.Lease.Term,
		Pathset:       []string{"web/new.js"},
		OperationID:   ctx.Operation.ID,
		OperationKind: ctx.Operation.Kind,
	}

	result := EvaluateMutationActivationGates(MutationActivationGateInput{
		AuthorityMode:  ModeControlledQueue,
		Source:         MutationActivationSourceDurableControlledQueueCandidate,
		Context:        ctx,
		PatchQueueItem: item,
	})

	if !mutationActivationGateByName(t, result, "mutation_binding").Passed {
		t.Fatalf("mutation binding should pass for exact add-path without base hashes: %+v", result.MutationBindingEvidence)
	}
	if result.MutationBindingEvidence == nil || !result.MutationBindingEvidence.Ready {
		t.Fatalf("expected ready mutation binding evidence, got %+v", result.MutationBindingEvidence)
	}
	for _, ref := range []string{"base.file_hashes", "base.file_hashes[web/new.js]"} {
		if testStringSliceContains(result.MutationBindingEvidence.MissingRefs, ref) {
			t.Fatalf("mutation binding should not require %s for add candidate: %+v", ref, result.MutationBindingEvidence.MissingRefs)
		}
	}
}

func TestMutationActivationBindingAllowsConcreteBaseHashUnderScopedPathset(t *testing.T) {
	ctx := validContext()
	ctx.Mode = ModeControlledQueue
	ctx.Base.FileHashes = map[string]string{
		"internal/eval/eval_contract_test.go": "sha256:base-eval-contract",
	}
	ctx.Pathset = []string{"internal/eval/**"}
	ctx.PatchQueue = PatchQueueRef{QueueID: "patchq-eval", ItemID: "patchitem-eval"}
	ctx.Operation = OperationRef{ID: "op-eval", Kind: "repo_patch_apply"}

	contextDigest, err := patchQueueContextDigest(ctx)
	if err != nil {
		t.Fatalf("patch queue context digest: %v", err)
	}
	item := PatchQueueItem{
		Schema:        PatchQueueItemSchemaVersion,
		ID:            "record-eval",
		QueueID:       ctx.PatchQueue.QueueID,
		ItemID:        ctx.PatchQueue.ItemID,
		State:         PatchQueueStateProposed,
		ContextDigest: contextDigest,
		RepoLeaseID:   ctx.Lease.ID,
		LeaseTerm:     ctx.Lease.Term,
		Pathset:       []string{"internal/eval/**"},
		OperationID:   ctx.Operation.ID,
		OperationKind: ctx.Operation.Kind,
	}

	result := EvaluateMutationActivationGates(MutationActivationGateInput{
		AuthorityMode: ModeControlledQueue,
		Source:        MutationActivationSourceDurableControlledQueueCandidate,
		Candidate: &MutationActivationCandidateSummary{
			WorkspaceID: "ws-b1-2",
			ProjectID:   "project-eval",
			RepoID:      "repo-eval",
			QueueID:     ctx.PatchQueue.QueueID,
			ItemID:      ctx.PatchQueue.ItemID,
			BranchID:    "branch-eval",
			BranchName:  "agent/beta/eval",
			CheckoutID:  "checkout-eval",
			State:       PatchQueueStateProposed,
			BaseSHA:     strings.Repeat("a", 40),
			HeadSHA:     strings.Repeat("b", 40),
		},
		WorktreeIdentity: WorktreeIdentityEvidence{
			RepoID:        "repo-eval",
			CheckoutID:    "checkout-eval",
			BranchID:      "branch-eval",
			BranchName:    "agent/beta/eval",
			MachineID:     "machine-eval",
			LocalPath:     "C:/fixtures/agents/beta/eval",
			BaseSHA:       strings.Repeat("a", 40),
			HeadSHA:       strings.Repeat("b", 40),
			ReadbackState: "missing",
			ReadbackError: "stat checkout: no such file or directory",
		},
		Context:        ctx,
		PatchQueueItem: item,
	})

	if !mutationActivationGateByName(t, result, "mutation_binding").Passed {
		t.Fatalf("mutation binding should pass for concrete base hash under scoped pathset: %+v", result.MutationBindingEvidence)
	}
	if result.MutationBindingEvidence == nil || !result.MutationBindingEvidence.Ready {
		t.Fatalf("expected ready mutation binding evidence, got %+v", result.MutationBindingEvidence)
	}
	if err := VerifyMutationActivationGateResult(result); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}
}

func TestMutationActivationGatesRequireReviewerMeshAdvisory(t *testing.T) {
	input := mutationActivationReadyInput(t)
	input.ReviewerMeshMode = "merge_authority"

	result := EvaluateMutationActivationGates(input)

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	if result.MutationAllowed {
		t.Fatalf("mutation_allowed = true")
	}
	gate := mutationActivationGateByName(t, result, "reviewer_mesh_advisory_only")
	if gate.Passed {
		t.Fatalf("reviewer gate passed for merge_authority")
	}
}

func TestMutationActivationGatesRequirePassedTestEvidence(t *testing.T) {
	input := mutationActivationReadyInput(t)
	input.PatchQueueItem.TestEvidence = PatchQueueTestEvidence{}
	input.PatchQueueItem.TestEvidenceDigest = ""

	result := EvaluateMutationActivationGates(input)

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	gate := mutationActivationGateByName(t, result, "merge_admission_conflict_safe")
	if gate.Passed {
		t.Fatalf("merge admission passed without test evidence")
	}
	if !strings.Contains(gate.Reason, "test evidence") {
		t.Fatalf("gate reason = %q, want test evidence", gate.Reason)
	}
}

func TestMutationActivationGatesBindRollbackToPatchQueueItem(t *testing.T) {
	input := mutationActivationReadyInput(t)
	input.RollbackEvidence.SourceOperationID = "op-other"

	result := EvaluateMutationActivationGates(input)

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	gate := mutationActivationGateByName(t, result, "rollback_proven")
	if gate.Passed {
		t.Fatalf("rollback gate passed for unrelated source operation")
	}
	if !strings.Contains(gate.Reason, "source operation") {
		t.Fatalf("gate reason = %q, want source operation", gate.Reason)
	}
}

func TestMutationActivationGatesRequireReviewerAdvisoryEvidence(t *testing.T) {
	input := mutationActivationReadyInput(t)
	input.ReviewerAdvisory = PatchQueueReviewerAdvisory{}
	input.PatchQueueItem.ReviewerAdvisory = PatchQueueReviewerAdvisory{}
	input.PatchQueueItem.ReviewerAdvisoryDigest = ""

	result := EvaluateMutationActivationGates(input)

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	gate := mutationActivationGateByName(t, result, "reviewer_advisory_recorded")
	if gate.Passed {
		t.Fatalf("reviewer advisory gate passed without advisory evidence")
	}
	if !strings.Contains(gate.Reason, "reviewer advisory") {
		t.Fatalf("gate reason = %q, want reviewer advisory", gate.Reason)
	}
}

func TestMutationActivationGatesRequireOperatorEnablementAfterReviewerAdvisory(t *testing.T) {
	input := mutationActivationReadyInput(t)
	input.OperatorEnablement = PatchQueueOperatorEnablement{}
	input.PatchQueueItem.OperatorEnablement = PatchQueueOperatorEnablement{}
	input.PatchQueueItem.OperatorEnablementDigest = ""

	result := EvaluateMutationActivationGates(input)

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	if !mutationActivationGateByName(t, result, "reviewer_advisory_recorded").Passed {
		t.Fatalf("reviewer advisory gate should remain passed when only operator enablement is missing")
	}
	gate := mutationActivationGateByName(t, result, "operator_enablement_recorded")
	if gate.Passed {
		t.Fatalf("operator enablement gate passed without explicit enablement evidence")
	}
	if !strings.Contains(gate.Reason, "operator enablement") {
		t.Fatalf("gate reason = %q, want operator enablement", gate.Reason)
	}
}

func TestMutationActivationGatesRequirePatchMaterializationPreflight(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.PatchMaterialization = PatchMaterialization{}

	result := EvaluateMutationActivationGates(input)

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	gate := mutationActivationGateByName(t, result, "materialization_preflight_verified")
	if gate.Passed {
		t.Fatalf("materialization preflight gate passed without patch materialization")
	}
	if !strings.Contains(gate.Reason, "patch materialization") {
		t.Fatalf("gate reason = %q, want patch materialization", gate.Reason)
	}
	if err := VerifyMutationActivationGateResult(result); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}
}

func TestMutationActivationMaterializationPreflightRedactsContent(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	addDurableCandidateToMutationActivationInput(&input)
	addMaterializationAuthorityProofToMutationActivationInput(t, &input)

	result := EvaluateMutationActivationGates(input)

	gate := mutationActivationGateByName(t, result, "materialization_preflight_verified")
	if !gate.Passed {
		t.Fatalf("materialization preflight gate should pass with durable authority proof, got %+v", gate)
	}
	if result.MaterializationPreflight == nil {
		t.Fatalf("expected materialization preflight evidence")
	}
	if !result.MaterializationPreflight.Ready ||
		result.MaterializationPreflight.AuthorityProof == nil ||
		result.MaterializationPreflight.AuthorityProof.AuthorityDigest == "" {
		t.Fatalf("expected ready authority-proof backed preflight evidence, got %+v", result.MaterializationPreflight)
	}
	if result.MaterializationPreflight.Materialization.FileCount != 1 ||
		len(result.MaterializationPreflight.Materialization.Files) != 1 ||
		result.MaterializationPreflight.Materialization.Files[0].ContentDigest == "" {
		t.Fatalf("expected redacted materialization metadata, got %+v", result.MaterializationPreflight.Materialization)
	}
	raw, err := json.Marshal(result.MaterializationPreflight)
	if err != nil {
		t.Fatalf("marshal preflight evidence: %v", err)
	}
	if strings.Contains(string(raw), "candidate content") || strings.Contains(string(raw), `"content":"`) {
		t.Fatalf("materialization preflight diagnostics leaked raw content: %s", raw)
	}
	if err := VerifyMutationActivationGateResult(result); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}
}

func TestVerifyMutationActivationRejectsForgedMaterializationReadyError(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.PatchMaterialization = PatchMaterialization{}
	result := EvaluateMutationActivationGates(input)
	if result.MaterializationPreflight == nil || result.MaterializationPreflight.Ready {
		t.Fatalf("expected non-ready materialization preflight evidence, got %+v", result.MaterializationPreflight)
	}
	result.MaterializationPreflight.ReadyError = "patch materialization is merely delayed"
	for i := range result.Gates {
		if result.Gates[i].Name == "materialization_preflight_verified" {
			result.Gates[i].Reason = result.MaterializationPreflight.ReadyError
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged materialization ready_error to fail verification")
	}
	if !strings.Contains(err.Error(), "ready_error") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationRejectsForgedMaterializationAuthorityProof(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	addDurableCandidateToMutationActivationInput(&input)
	addMaterializationAuthorityProofToMutationActivationInput(t, &input)

	result := EvaluateMutationActivationGates(input)
	if result.MaterializationPreflight == nil || !result.MaterializationPreflight.Ready || result.MaterializationPreflight.AuthorityProof == nil {
		t.Fatalf("expected ready materialization preflight evidence, got %+v", result.MaterializationPreflight)
	}
	result.MaterializationPreflight.AuthorityProof.Files[0].ContentDigest = "sha256:" + strings.Repeat("9", 64)
	result.MaterializationPreflight.AuthorityProof.AuthorityDigest = PatchMaterializationAuthorityProofDigest(*result.MaterializationPreflight.AuthorityProof)
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged materialization authority proof to fail verification")
	}
	if !strings.Contains(err.Error(), "materialization_preflight_verified") && !strings.Contains(err.Error(), "authority proof") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestMutationActivationGatesRequireCanonicalWorktreeObjectIDs(t *testing.T) {
	input := mutationActivationReadyInput(t)
	input.WorktreeIdentity.BaseSHA = "base-sha"

	result := EvaluateMutationActivationGates(input)
	gate := mutationActivationGateByName(t, result, "canonical_worktree_identity")
	if gate.Passed {
		t.Fatalf("canonical worktree identity passed with placeholder base sha")
	}
}

func TestMutationActivationGatesPatchOnlyContextModeStaysBlocked(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInput(t))

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	if result.MutationAllowed {
		t.Fatalf("mutation_allowed = true")
	}
	gate := mutationActivationGateByName(t, result, "controlled_context_mode")
	if gate.Passed {
		t.Fatalf("controlled_context_mode passed for patch-only repoauthority context")
	}
	for _, gate := range result.Gates {
		if gate.Name == "controlled_context_mode" || gate.Name == "live_actuator_target_identity" || gate.Name == "materialization_preflight_verified" || gate.Name == "live_mutation_verifier_enabled" || gate.Name == "live_mutation_actuator_enabled" {
			continue
		}
		if !gate.Passed {
			t.Fatalf("unexpected gate %s failed: %s", gate.Name, gate.Reason)
		}
	}
	if !isCanonicalSHA256Digest(result.Digest) {
		t.Fatalf("digest = %q, want canonical sha256", result.Digest)
	}
}

func TestMutationActivationGatesControlledContextStaysBlockedUntilLiveVerifierAndActuatorEnabled(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInputForMode(t, ModeControlledQueue))

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked until verifier and actuator; result=%+v", result.Status, result)
	}
	if result.MutationAllowed {
		t.Fatalf("mutation_allowed = true before verifier; result=%+v", result)
	}
	for _, gate := range result.Gates {
		if gate.Name == "live_actuator_target_identity" || gate.Name == "materialization_preflight_verified" || gate.Name == "live_mutation_verifier_enabled" || gate.Name == "live_mutation_actuator_enabled" {
			if gate.Passed {
				t.Fatalf("%s gate passed while disabled", gate.Name)
			}
			continue
		}
		if !gate.Passed {
			t.Fatalf("unexpected gate %s failed in controlled context: %s", gate.Name, gate.Reason)
		}
	}
	if err := VerifyMutationActivationGateResult(result); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}
}

func TestVerifyMutationActivationGateResultAcceptsBlockedFailClosedResult(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInput(t))
	if err := VerifyMutationActivationGateResult(result); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsForgedReadyPatchOnlyContext(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInput(t))
	result.Status = MutationActivationStatusReady
	result.MutationAllowed = true
	result.LiveMutationVerifierEnabled = true
	result.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	result.LiveMutationActuatorEnabled = true
	result.LiveMutationActuatorSource = MutationActivationLiveActuatorSourceRuntime
	for i := range result.Gates {
		result.Gates[i].Passed = true
		result.Gates[i].Reason = ""
	}
	result.BlockingReasons = nil
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged ready patch-only context to fail verification")
	}
	if !strings.Contains(err.Error(), "top-level invariant") &&
		!strings.Contains(err.Error(), "controlled_context_mode") &&
		!strings.Contains(err.Error(), "materialization_preflight_verified") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsMissingRequiredGate(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInput(t))
	result.Gates = result.Gates[:len(result.Gates)-1]
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected missing required gate to fail verification")
	}
	if !strings.Contains(err.Error(), "canonical gate set") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsForgedWorktreeReadbackGate(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{})
	for i := range result.Gates {
		if result.Gates[i].Name == "canonical_worktree_identity" {
			result.Gates[i].Passed = true
			result.Gates[i].Reason = ""
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged canonical_worktree_identity gate to fail verification")
	}
	if !strings.Contains(err.Error(), "canonical_worktree_identity") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsForgedMutationBindingGate(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{})
	for i := range result.Gates {
		if result.Gates[i].Name == "mutation_binding" {
			result.Gates[i].Passed = true
			result.Gates[i].Reason = ""
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged mutation_binding gate to fail verification")
	}
	if !strings.Contains(err.Error(), "mutation_binding") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsForgedMergeAdmissionGate(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{})
	for i := range result.Gates {
		if result.Gates[i].Name == "merge_admission_conflict_safe" {
			result.Gates[i].Passed = true
			result.Gates[i].Reason = ""
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged merge_admission_conflict_safe gate to fail verification")
	}
	if !strings.Contains(err.Error(), "merge_admission_conflict_safe") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsForgedBoundedRetryGate(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{})
	for i := range result.Gates {
		if result.Gates[i].Name == "bounded_retry" {
			result.Gates[i].Passed = true
			result.Gates[i].Reason = ""
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged bounded_retry gate to fail verification")
	}
	if !strings.Contains(err.Error(), "bounded_retry") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsForgedRollbackProofGate(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{})
	for i := range result.Gates {
		if result.Gates[i].Name == "rollback_proven" {
			result.Gates[i].Passed = true
			result.Gates[i].Reason = ""
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged rollback_proven gate to fail verification")
	}
	if !strings.Contains(err.Error(), "rollback_proven") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsMalformedRetryEvidenceTimestamp(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInputForMode(t, ModeControlledQueue))
	if result.RetryBoundEvidence == nil || !result.RetryBoundEvidence.Ready {
		t.Fatalf("expected ready retry bound evidence, got %+v", result.RetryBoundEvidence)
	}
	result.RetryBoundEvidence.NextRetryAt = "not-a-time"
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected malformed retry timestamp to fail verification")
	}
	if !strings.Contains(err.Error(), "next_retry_at") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsMalformedRollbackEvidenceTimestamp(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInputForMode(t, ModeControlledQueue))
	if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil || !result.RollbackProofEvidence.Ready {
		t.Fatalf("expected ready rollback proof evidence, got %+v", result.RollbackProofEvidence)
	}
	result.RollbackProofEvidence.RollbackEvidence.RecordedAt = "not-a-time"
	readyErr := rollbackEvidenceReady(result.RollbackProofEvidence.RollbackEvidence, patchQueueItemFromMergeAdmissionEvidence(*result.MergeAdmissionEvidence))
	if readyErr == nil {
		t.Fatalf("expected malformed rollback recorded_at to fail local recompute")
	}
	result.RollbackProofEvidence.State = "invalid"
	result.RollbackProofEvidence.Ready = false
	result.RollbackProofEvidence.ReadyError = readyErr.Error()
	for i := range result.Gates {
		if result.Gates[i].Name == "rollback_proven" {
			result.Gates[i].Passed = false
			result.Gates[i].Reason = readyErr.Error()
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected malformed rollback recorded_at to fail verification")
	}
	if !strings.Contains(err.Error(), "recorded_at") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsForgedWorktreeRootReadback(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.WorktreeIdentity.ObservedWorktreeRoot = ""
	result := EvaluateMutationActivationGates(input)
	for i := range result.Gates {
		if result.Gates[i].Name == "canonical_worktree_identity" {
			result.Gates[i].Passed = true
			result.Gates[i].Reason = ""
		}
	}
	result.BlockingReasons = nil
	for _, gate := range result.Gates {
		if !gate.Passed {
			result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
		}
	}
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected forged worktree root readback to fail verification")
	}
	if !strings.Contains(err.Error(), "canonical_worktree_identity") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsBlockingReasonMismatch(t *testing.T) {
	result := EvaluateMutationActivationGates(mutationActivationReadyInput(t))
	if len(result.BlockingReasons) == 0 {
		t.Fatalf("expected blocked result to have blocking reasons")
	}
	result.BlockingReasons = result.BlockingReasons[:len(result.BlockingReasons)-1]
	result.Digest = digestMutationActivationGateResult(result)

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected blocking reason mismatch to fail verification")
	}
	if !strings.Contains(err.Error(), "blocking reasons") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsReadyWithoutDurableCandidateSource(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	input.LiveMutationActuatorEnabled = true
	input.LiveMutationActuatorSource = MutationActivationLiveActuatorSourceRuntime
	result := EvaluateMutationActivationGates(input)
	if result.Status != MutationActivationStatusBlocked || result.MutationAllowed {
		t.Fatalf("status = %q mutation_allowed=%v, want blocked/false before durable source and materialization authority; result=%+v", result.Status, result.MutationAllowed, result)
	}

	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected ready activation without durable controlled candidate source to fail")
	}
	if !strings.Contains(err.Error(), "live mutation verifier requires source") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultAcceptsReadyWithLiveVerifierAndDurableCandidate(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	input.LiveMutationActuatorEnabled = true
	input.LiveMutationActuatorSource = MutationActivationLiveActuatorSourceRuntime
	addDurableCandidateToMutationActivationInput(&input)
	addMaterializationAuthorityProofToMutationActivationInput(t, &input)

	result := EvaluateMutationActivationGates(input)
	if result.Status != MutationActivationStatusReady || !result.MutationAllowed {
		t.Fatalf("status = %q mutation_allowed=%v, want ready/true with durable materialization authority proof; result=%+v", result.Status, result.MutationAllowed, result)
	}
	gate := mutationActivationGateByName(t, result, "materialization_preflight_verified")
	if !gate.Passed {
		t.Fatalf("expected materialization authority proof gate to pass, got %+v", gate)
	}
	if err := VerifyMutationActivationGateResult(result); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}
}

func TestMutationActivationMaterializationPreflightPreservesAddChangeKind(t *testing.T) {
	content := "export const added = true;\n"
	candidateHash := PatchMaterializationContentDigest(content)
	authority := Context{
		Mode:        ModeControlledQueue,
		WorkspaceID: "ws-add-preflight",
		TaskID:      "task-add-preflight",
		SessionID:   "session-add-preflight",
		RunID:       "run-add-preflight",
		AgentID:     "worker-alpha",
		Principal: PrincipalRef{
			Type: "agent",
			ID:   "worker-alpha",
		},
		CapabilitySnapshot: CapabilitySnapshotRef{
			ID:     "cap-add-preflight",
			Schema: "runtime_capability_snapshot.v1",
		},
		RepoRoot: "C:/fixtures/agents/worker-alpha/project",
		Base: BaseIdentity{
			Ref:        "main",
			TreeHash:   "sha256:base-tree-add-preflight",
			FileHashes: map[string]string{},
		},
		Pathset: []string{"web/**"},
		Lease: LeaseRef{
			ID:   "lease-add-preflight",
			Term: 9,
		},
		PatchQueue: PatchQueueRef{
			QueueID: "patchq-add-preflight",
			ItemID:  "patchitem-add-preflight",
		},
		Operation: OperationRef{
			ID:   "op-add-preflight",
			Kind: "repo_patch_apply",
		},
	}
	contextDigest, err := authority.Digest()
	if err != nil {
		t.Fatalf("authority digest: %v", err)
	}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:             authority,
		CurrentFileHashes:   map[string]string{},
		CandidateFileHashes: map[string]string{"web/new.js": candidateHash},
	})
	if cas.Status != CASPatchStatusApplied ||
		len(cas.Paths) != 1 ||
		cas.Paths[0].ChangeKind != CASPatchChangeAdd {
		t.Fatalf("CAS status=%q paths=%+v issues=%+v", cas.Status, cas.Paths, cas.Issues)
	}
	testEvidence := PatchQueueTestEvidence{
		Schema:         PatchQueueTestEvidenceSchemaVersion,
		Name:           "activation add-file preflight",
		Command:        "go test ./internal/repoauthority",
		Status:         PatchQueueTestStatusPassed,
		ExitCode:       0,
		OutputDigest:   patchQueueTestDigest("add-preflight"),
		OutputSummary:  "ok",
		DurationMillis: 100,
	}
	item := PatchQueueItem{
		Schema:                   PatchQueueItemSchemaVersion,
		ID:                       "patchq-add-preflight/patchitem-add-preflight",
		QueueID:                  "patchq-add-preflight",
		ItemID:                   "patchitem-add-preflight",
		ReviewDocKey:             "doc-add-preflight-review",
		State:                    PatchQueueStateApplied,
		Attempt:                  1,
		MaxAttempts:              3,
		Pathset:                  []string{"web/**"},
		WorkspaceID:              "ws-add-preflight",
		ProjectID:                "project-add-preflight",
		TaskID:                   "task-add-preflight",
		SessionID:                "session-add-preflight",
		RunID:                    "run-add-preflight",
		AgentID:                  "worker-alpha",
		PrincipalType:            "agent",
		PrincipalID:              "worker-alpha",
		CapabilitySnapshotID:     "cap-add-preflight",
		CapabilitySnapshotSchema: "runtime_capability_snapshot.v1",
		BaseRef:                  "main",
		BaseTreeHash:             "sha256:base-tree-add-preflight",
		ContextDigest:            contextDigest,
		RepoLeaseID:              "lease-add-preflight",
		LeaseTerm:                9,
		CASResult:                cas,
		CASPatchDigest:           cas.PatchDigest,
		CASEvaluationDigest:      PatchQueueCASEvaluationDigest(cas),
		TestEvidence:             testEvidence,
		TestEvidenceDigest:       PatchQueueTestEvidenceDigest(testEvidence),
		OperationID:              "op-add-preflight",
		OperationKind:            "repo_patch_apply",
	}
	materialization, err := NormalizePatchMaterialization(PatchMaterialization{
		RecordedBy: "integrator-beta",
		Files: []PatchMaterializedFile{
			{Path: "web/new.js", Content: content},
		},
	}, item, time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NormalizePatchMaterialization: %v", err)
	}
	proof, err := BuildPatchMaterializationAuthorityProof(materialization, item)
	if err != nil {
		t.Fatalf("BuildPatchMaterializationAuthorityProof: %v", err)
	}

	input := MutationActivationGateInput{
		AuthorityMode:       MutationActivationAuthorityModeControlledQueue,
		DirectMergeDisabled: true,
		QueueDurable:        true,
		ReviewerMeshMode:    MutationActivationReviewerMeshAdvisoryOnly,
		Source:              MutationActivationSourceDurableControlledQueueCandidate,
		Candidate: &MutationActivationCandidateSummary{
			WorkspaceID: item.WorkspaceID,
			ProjectID:   item.ProjectID,
			RepoID:      "repo-add-preflight",
			QueueID:     item.QueueID,
			ItemID:      item.ItemID,
			BranchID:    "branch-add-preflight",
			BranchName:  "agent/worker-alpha/add-preflight",
			CheckoutID:  "checkout-add-preflight",
			State:       "CLAIMED",
			BaseSHA:     strings.Repeat("a", 40),
			HeadSHA:     strings.Repeat("b", 40),
		},
		WorktreeIdentity: WorktreeIdentityEvidence{
			RepoID:               "repo-add-preflight",
			CheckoutID:           "checkout-add-preflight",
			BranchID:             "branch-add-preflight",
			BranchName:           "agent/worker-alpha/add-preflight",
			MachineID:            "machine-add-preflight",
			LocalPath:            "C:/fixtures/agents/worker-alpha/project",
			BaseSHA:              strings.Repeat("a", 40),
			HeadSHA:              strings.Repeat("b", 40),
			ReadbackState:        "ok",
			ObservedWorktreeRoot: "C:/fixtures/agents/worker-alpha/project",
			ObservedBranchName:   "agent/worker-alpha/add-preflight",
			ObservedHeadSHA:      strings.Repeat("b", 40),
			ObservedDirtyState:   "clean",
		},
		Context:                     authority,
		PatchQueueItem:              item,
		PatchMaterialization:        materialization,
		PatchMaterializationProof:   proof,
		LiveMutationVerifierEnabled: true,
		LiveMutationVerifierSource:  MutationActivationLiveVerifierSourceEnv,
	}
	result := EvaluateMutationActivationGates(input)
	if result.MaterializationPreflight == nil ||
		!result.MaterializationPreflight.Ready ||
		len(result.MaterializationPreflight.Materialization.Files) != 1 ||
		result.MaterializationPreflight.Materialization.Files[0].ChangeKind != CASPatchChangeAdd ||
		result.MaterializationPreflight.AuthorityProof == nil ||
		len(result.MaterializationPreflight.AuthorityProof.Files) != 1 ||
		result.MaterializationPreflight.AuthorityProof.Files[0].ChangeKind != CASPatchChangeAdd {
		t.Fatalf("add-file materialization preflight did not preserve change_kind: %+v", result.MaterializationPreflight)
	}
	if err := validatePatchMaterializationDiagnostic(result.MaterializationPreflight.Materialization, item, input.WorktreeIdentity, materialization.MaterializationDigest); err != nil {
		t.Fatalf("validatePatchMaterializationDiagnostic: %v", err)
	}
	if err := validatePatchMaterializationAuthorityProofDiagnostic(proof, result.MaterializationPreflight.Materialization, item, materialization.MaterializationDigest); err != nil {
		t.Fatalf("validatePatchMaterializationAuthorityProofDiagnostic: %v", err)
	}

	materializationDiagnostic := result.MaterializationPreflight.Materialization
	materializationDiagnostic.Files = append([]PatchMaterializedFileDiagnostic(nil), materializationDiagnostic.Files...)
	materializationDiagnostic.Files[0].ChangeKind = ""
	if err := validatePatchMaterializationDiagnostic(materializationDiagnostic, item, input.WorktreeIdentity, materialization.MaterializationDigest); err == nil || !strings.Contains(err.Error(), "change_kind") {
		t.Fatalf("expected tampered add-file change_kind diagnostic to fail verification, got %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsNonCanonicalLiveVerifierSource(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = "test:unit"
	input.LiveMutationActuatorEnabled = true
	input.LiveMutationActuatorSource = MutationActivationLiveActuatorSourceRuntime
	addDurableCandidateToMutationActivationInput(&input)

	result := EvaluateMutationActivationGates(input)
	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected noncanonical live verifier source to fail verification")
	}
	if !strings.Contains(err.Error(), "live mutation verifier source") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsNonCanonicalLiveActuatorSource(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	input.LiveMutationActuatorEnabled = true
	input.LiveMutationActuatorSource = "test:actuator"
	addDurableCandidateToMutationActivationInput(&input)

	result := EvaluateMutationActivationGates(input)
	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected noncanonical live actuator source to fail verification")
	}
	if !strings.Contains(err.Error(), "live mutation actuator source") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsLiveVerifierOnSyntheticSource(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{
		Source:                      MutationActivationSourceSyntheticPatchOnly,
		LiveMutationVerifierEnabled: true,
		LiveMutationVerifierSource:  MutationActivationLiveVerifierSourceEnv,
	})

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected live verifier on synthetic source to fail verification")
	}
	if !strings.Contains(err.Error(), "live mutation verifier requires source") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActivationGateResultRejectsLiveActuatorWithoutVerifier(t *testing.T) {
	result := EvaluateMutationActivationGates(MutationActivationGateInput{
		Source:                      MutationActivationSourceDurableControlledQueueCandidate,
		LiveMutationActuatorEnabled: true,
		LiveMutationActuatorSource:  MutationActivationLiveActuatorSourceRuntime,
	})

	if result.Status != MutationActivationStatusBlocked {
		t.Fatalf("status = %q, want blocked; result=%+v", result.Status, result)
	}
	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected live actuator without verifier to fail verification")
	}
	if !strings.Contains(err.Error(), "live mutation actuator requires live mutation verifier") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestMutationActivationTargetWorktreeIdentityFailsClosedForMismatchAndAlias(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*MutationActivationGateInput)
	}{
		{
			name: "repo mismatch",
			mutate: func(input *MutationActivationGateInput) {
				input.TargetWorktreeIdentity.RepoID = "repo-other"
			},
		},
		{
			name: "base mismatch",
			mutate: func(input *MutationActivationGateInput) {
				input.TargetWorktreeIdentity.BaseSHA = strings.Repeat("c", 40)
			},
		},
		{
			name: "checkout alias",
			mutate: func(input *MutationActivationGateInput) {
				input.TargetWorktreeIdentity.CheckoutID = input.WorktreeIdentity.CheckoutID
			},
		},
		{
			name: "path alias",
			mutate: func(input *MutationActivationGateInput) {
				input.TargetWorktreeIdentity.LocalPath = input.WorktreeIdentity.LocalPath
				input.TargetWorktreeIdentity.ObservedWorktreeRoot = input.WorktreeIdentity.LocalPath
			},
		},
		{
			name: "branch alias",
			mutate: func(input *MutationActivationGateInput) {
				input.TargetWorktreeIdentity.BranchName = input.WorktreeIdentity.BranchName
				input.TargetWorktreeIdentity.ObservedBranchName = input.WorktreeIdentity.BranchName
				input.Candidate.TargetBranchName = input.WorktreeIdentity.BranchName
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
			addDurableCandidateToMutationActivationInput(&input)
			tt.mutate(&input)

			result := EvaluateMutationActivationGates(input)
			gate := mutationActivationGateByName(t, result, "live_actuator_target_identity")
			if gate.Passed {
				t.Fatalf("target identity gate passed for %s; result=%+v", tt.name, result)
			}
			if result.MutationAllowed {
				t.Fatalf("mutation_allowed = true for %s", tt.name)
			}
		})
	}
}

func TestVerifyMutationActivationGateResultRejectsTargetIdentityWithoutCandidateTargetFields(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	addDurableCandidateToMutationActivationInput(&input)
	input.Candidate.TargetCheckoutID = ""
	input.Candidate.TargetBranchName = ""

	result := EvaluateMutationActivationGates(input)
	err := VerifyMutationActivationGateResult(result)
	if err == nil {
		t.Fatalf("expected target worktree without candidate target fields to fail verification")
	}
	if !strings.Contains(err.Error(), "target_checkout_id") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func addDurableCandidateToMutationActivationInput(input *MutationActivationGateInput) {
	input.Source = MutationActivationSourceDurableControlledQueueCandidate
	if strings.TrimSpace(input.TargetWorktreeIdentity.CheckoutID) == "" {
		input.TargetWorktreeIdentity = WorktreeIdentityEvidence{
			RepoID:               input.WorktreeIdentity.RepoID,
			CheckoutID:           "checkout-integration-1",
			BranchID:             "checkout-integration-1",
			BranchName:           "integration",
			MachineID:            input.WorktreeIdentity.MachineID,
			LocalPath:            "C:/fixtures/integration/project",
			BaseSHA:              input.WorktreeIdentity.BaseSHA,
			HeadSHA:              input.WorktreeIdentity.BaseSHA,
			ReadbackState:        "ok",
			ObservedWorktreeRoot: "C:/fixtures/integration/project",
			ObservedBranchName:   "integration",
			ObservedHeadSHA:      input.WorktreeIdentity.BaseSHA,
			ObservedDirtyState:   "clean",
		}
	}
	input.Candidate = &MutationActivationCandidateSummary{
		WorkspaceID:      input.PatchQueueItem.WorkspaceID,
		ProjectID:        input.PatchQueueItem.ProjectID,
		RepoID:           input.WorktreeIdentity.RepoID,
		QueueID:          input.PatchQueueItem.QueueID,
		ItemID:           input.PatchQueueItem.ItemID,
		BranchID:         input.WorktreeIdentity.BranchID,
		BranchName:       input.WorktreeIdentity.BranchName,
		CheckoutID:       input.WorktreeIdentity.CheckoutID,
		TargetCheckoutID: input.TargetWorktreeIdentity.CheckoutID,
		TargetBranchName: input.TargetWorktreeIdentity.BranchName,
		State:            "CLAIMED",
		BaseSHA:          input.WorktreeIdentity.BaseSHA,
		HeadSHA:          input.WorktreeIdentity.HeadSHA,
	}
}

func addMaterializationAuthorityProofToMutationActivationInput(t *testing.T, input *MutationActivationGateInput) {
	t.Helper()
	proof, err := BuildPatchMaterializationAuthorityProof(input.PatchMaterialization, input.PatchQueueItem)
	if err != nil {
		t.Fatalf("BuildPatchMaterializationAuthorityProof: %v", err)
	}
	input.PatchMaterializationProof = proof
}

func mutationActivationReadyInput(t *testing.T) MutationActivationGateInput {
	t.Helper()
	return mutationActivationReadyInputForMode(t, ModePatchOnlyTempRepo)
}

func mutationActivationReadyInputForMode(t *testing.T, mode string) MutationActivationGateInput {
	t.Helper()
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixtureForMode(t, time.Hour, []string{"owned.go"}, mode)
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{
		Context:    queueCtx,
		LeaseStore: leaseStore,
		Now:        now,
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{
		Context: queueCtx,
		Now:     now.Add(time.Second),
	}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	applyCtx := queueCtx
	applyCtx.Operation = OperationRef{ID: "op-apply", Kind: "repo_patch_apply"}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           applyCtx,
		PatchID:           "patch-ready",
		CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
		CandidateFileHashes: map[string]string{
			"owned.go": PatchMaterializationContentDigest("candidate content\n"),
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("CAS status = %q issues=%+v", cas.Status, cas.Issues)
	}
	item, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:    applyCtx,
		LeaseStore: leaseStore,
		CASResult:  cas,
		TestEvidence: PatchQueueTestEvidence{
			Schema:         PatchQueueTestEvidenceSchemaVersion,
			Name:           "activation gate unit validation",
			Command:        "go test ./internal/repoauthority",
			Status:         PatchQueueTestStatusPassed,
			ExitCode:       0,
			OutputDigest:   patchQueueTestDigest("a"),
			OutputSummary:  "ok",
			DurationMillis: 100,
		},
		Now: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation: %v", err)
	}
	rollback := patchQueuePassedRollbackEvidenceForItem(item)
	rollback.Schema = PatchQueueRollbackSchemaVersion
	rollback.SourceOperationID = item.OperationID
	rollback.SourceOperationKind = item.OperationKind
	rollback.RollbackOperationID = "op-rollback"
	rollback.RollbackOperationKind = "repo_patch_apply"
	rollback.SourcePatchDigest = item.CASPatchDigest
	rollback.RecordedAt = now.Add(3 * time.Second).Format(time.RFC3339Nano)
	item.ProjectID = "project-1"
	item.ReviewDocKey = "doc-review-ready"
	item.RollbackEvidence = rollback
	item.RollbackEvidenceDigest = PatchQueueRollbackEvidenceDigest(rollback)
	reviewerAdvisory := PatchQueueReviewerAdvisory{
		Schema:                 PatchQueueReviewerAdvisorySchema,
		Mode:                   MutationActivationReviewerMeshAdvisoryOnly,
		Verdict:                PatchQueueReviewerAdvisoryVerdictReviewed,
		ReviewerID:             "reviewer-1",
		ReviewDocKey:           item.ReviewDocKey,
		OperationID:            item.OperationID,
		OperationKind:          item.OperationKind,
		CASPatchDigest:         item.CASPatchDigest,
		CASEvaluationDigest:    item.CASEvaluationDigest,
		RollbackEvidenceDigest: item.RollbackEvidenceDigest,
		Summary:                "reviewed for mutation activation",
		RecordedAt:             now.Add(4 * time.Second).Format(time.RFC3339Nano),
	}
	item.ReviewerAdvisory = reviewerAdvisory
	item.ReviewerAdvisoryDigest = PatchQueueReviewerAdvisoryDigest(reviewerAdvisory)
	operatorEnablement := PatchQueueOperatorEnablement{
		Schema:                 PatchQueueOperatorEnablementSchema,
		Scope:                  MutationActivationOperatorEnablementScope,
		Enabled:                true,
		EnabledBy:              "operator-1",
		EnabledAt:              now.Add(5 * time.Second).Format(time.RFC3339Nano),
		Reason:                 "first deployment mutation activation dry-run approval",
		WorkspaceID:            item.WorkspaceID,
		ProjectID:              item.ProjectID,
		QueueID:                item.QueueID,
		ItemID:                 item.ItemID,
		OperationID:            item.OperationID,
		CASPatchDigest:         item.CASPatchDigest,
		RollbackEvidenceDigest: item.RollbackEvidenceDigest,
		ReviewerAdvisoryDigest: item.ReviewerAdvisoryDigest,
	}
	item.OperatorEnablement = operatorEnablement
	item.OperatorEnablementDigest = PatchQueueOperatorEnablementDigest(operatorEnablement)
	materialization, err := NormalizePatchMaterialization(PatchMaterialization{
		RecordedBy: "integrator-1",
		Files: []PatchMaterializedFile{
			{Path: "owned.go", Content: "candidate content\n"},
		},
	}, item, now.Add(6*time.Second))
	if err != nil {
		t.Fatalf("NormalizePatchMaterialization: %v", err)
	}

	return MutationActivationGateInput{
		AuthorityMode:       MutationActivationAuthorityModeControlledQueue,
		DirectMergeDisabled: true,
		QueueDurable:        true,
		ReviewerMeshMode:    MutationActivationReviewerMeshAdvisoryOnly,
		WorktreeIdentity: WorktreeIdentityEvidence{
			RepoID:               "repo-1",
			CheckoutID:           "checkout-1",
			BranchID:             "branch-1",
			BranchName:           "agent/alpha/task",
			MachineID:            "machine-1",
			LocalPath:            "C:/fixtures/agents/alpha/project",
			BaseSHA:              strings.Repeat("a", 40),
			HeadSHA:              strings.Repeat("b", 40),
			ReadbackState:        "ok",
			ObservedWorktreeRoot: "C:/fixtures/agents/alpha/project",
			ObservedBranchName:   "agent/alpha/task",
			ObservedHeadSHA:      strings.Repeat("b", 40),
			ObservedDirtyState:   "clean",
		},
		Context:              applyCtx,
		PatchQueueItem:       item,
		RollbackEvidence:     rollback,
		ReviewerAdvisory:     reviewerAdvisory,
		OperatorEnablement:   operatorEnablement,
		PatchMaterialization: materialization,
	}
}

func patchQueueFixtureForMode(t *testing.T, ttl time.Duration, pathset []string, mode string) (*FileLeaseStore, *PatchQueueStore, Context, FileLease, time.Time) {
	t.Helper()
	now := time.Date(2026, 4, 21, 20, 45, 0, 0, time.UTC)
	preLease := patchQueuePreLeaseContext("agent-b1-6", "task-b1-6", "session-b1-6", "run-b1-6", "cap-b1-6", pathset)
	preLease.Mode = mode
	leaseStore := NewFileLeaseStore()
	lease, err := leaseStore.Acquire(AcquireFileLeaseInput{
		Context: preLease,
		TTL:     ttl,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	queueCtx := preLease
	queueCtx.Lease = LeaseRef{ID: lease.ID, Term: lease.Term}
	queueCtx.PatchQueue = PatchQueueRef{QueueID: "patchq-b1-6", ItemID: "patchitem-b1-6"}
	return leaseStore, NewPatchQueueStore(), queueCtx, lease, now
}

func mutationActivationGateByName(t *testing.T, result MutationActivationGateResult, name string) MutationActivationGate {
	t.Helper()
	for _, gate := range result.Gates {
		if gate.Name == name {
			return gate
		}
	}
	t.Fatalf("gate %q not found in %+v", name, result.Gates)
	return MutationActivationGate{}
}

func testStringSliceContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
