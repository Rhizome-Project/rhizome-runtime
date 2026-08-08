package repoauthority

import (
	"strings"
	"testing"
)

func TestMutationActuatorDryRunBlockedBeforeMaterializationAuthorityBoundary(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)

	activation := EvaluateMutationActivationGates(input)
	if activation.Status != MutationActivationStatusBlocked || activation.MutationAllowed {
		t.Fatalf("activation = %s allowed=%v, want blocked/false: %+v", activation.Status, activation.MutationAllowed, activation)
	}
	failed := mutationActivationFailedGateNames(activation)
	if len(failed) != 2 || failed[0] != "materialization_preflight_verified" || failed[1] != "live_mutation_actuator_enabled" {
		t.Fatalf("failed gates = %+v, want materialization_preflight_verified and live_mutation_actuator_enabled", failed)
	}

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{Activation: activation})
	if dryRun.Status != MutationActuatorDryRunStatusBlocked {
		t.Fatalf("dry-run status = %q, want blocked: %+v", dryRun.Status, dryRun)
	}
	if dryRun.ActivationDigest != activation.Digest {
		t.Fatalf("activation digest = %q, want %q", dryRun.ActivationDigest, activation.Digest)
	}
	if !dryRun.VerifierReady || dryRun.ActuatorEnabled || dryRun.WouldMutate || dryRun.MutationExecuted {
		t.Fatalf("dry-run must be verifier-aware without actuator or mutation, got %+v", dryRun)
	}
	if !testStringSliceContains(dryRun.BlockingReasons, "materialization_preflight_verified: "+MaterializationPreflightAuthorityProofRequired) {
		t.Fatalf("dry-run must carry materialization authority blocker, got %+v", dryRun.BlockingReasons)
	}
	if !testStringSliceContains(dryRun.BlockingReasons, "live_mutation_actuator_enabled: live mutation actuator is disabled; verifier readiness does not execute mutations") {
		t.Fatalf("dry-run must carry actuator blocking reason, got %+v", dryRun.BlockingReasons)
	}
	if err := VerifyMutationActuatorDryRunResult(dryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestMutationActuatorDryRunBlockedBeforeVerifierBoundary(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	addDurableCandidateToMutationActivationInput(&input)

	activation := EvaluateMutationActivationGates(input)
	if activation.Status != MutationActivationStatusBlocked || activation.MutationAllowed {
		t.Fatalf("activation = %s allowed=%v, want blocked/false: %+v", activation.Status, activation.MutationAllowed, activation)
	}

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{Activation: activation})
	if dryRun.Status != MutationActuatorDryRunStatusBlocked {
		t.Fatalf("dry-run status = %q, want blocked: %+v", dryRun.Status, dryRun)
	}
	if dryRun.VerifierReady || dryRun.ActuatorEnabled || dryRun.WouldMutate || dryRun.MutationExecuted {
		t.Fatalf("blocked dry-run must remain fully non-mutating, got %+v", dryRun)
	}
	if !testStringSliceContains(dryRun.BlockingReasons, "live_mutation_verifier_enabled: live mutation verifier is disabled; mutation activation remains fail-closed") {
		t.Fatalf("dry-run must carry verifier blocking reason, got %+v", dryRun.BlockingReasons)
	}
	if err := VerifyMutationActuatorDryRunResult(dryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestMutationActuatorDryRunReadyWithMaterializationAuthorityProof(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)
	addMaterializationAuthorityProofToMutationActivationInput(t, &input)

	activation := EvaluateMutationActivationGates(input)
	if activation.Status != MutationActivationStatusBlocked || activation.MutationAllowed {
		t.Fatalf("activation = %s allowed=%v, want blocked/false until live actuator: %+v", activation.Status, activation.MutationAllowed, activation)
	}
	failed := mutationActivationFailedGateNames(activation)
	if len(failed) != 1 || failed[0] != "live_mutation_actuator_enabled" {
		t.Fatalf("failed gates = %+v, want only live_mutation_actuator_enabled", failed)
	}

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{Activation: activation})
	if dryRun.Status != MutationActuatorDryRunStatusReady {
		t.Fatalf("dry-run status = %q, want ready: %+v", dryRun.Status, dryRun)
	}
	if dryRun.LiveScope != MutationActuatorLiveScopeAddModify {
		t.Fatalf("live scope = %q, want %q", dryRun.LiveScope, MutationActuatorLiveScopeAddModify)
	}
	if !mutationActuatorChangeKindSlicesEqual(dryRun.AllowedChangeKinds, []string{CASPatchChangeModify, CASPatchChangeAdd}) {
		t.Fatalf("allowed change kinds = %+v, want modify/add", dryRun.AllowedChangeKinds)
	}
	if !mutationActuatorChangeKindSlicesEqual(dryRun.ObservedChangeKinds, []string{CASPatchChangeModify}) {
		t.Fatalf("observed change kinds = %+v, want modify", dryRun.ObservedChangeKinds)
	}
	if len(dryRun.UnsupportedChangeKinds) != 0 {
		t.Fatalf("unsupported change kinds = %+v, want none", dryRun.UnsupportedChangeKinds)
	}
	if dryRun.MaterializationAuthorityProofDigest == "" ||
		activation.MaterializationPreflight == nil ||
		activation.MaterializationPreflight.AuthorityProof == nil ||
		dryRun.MaterializationAuthorityProofDigest != activation.MaterializationPreflight.AuthorityProof.AuthorityDigest {
		t.Fatalf("dry-run must bind materialization authority proof digest, got dry_run=%+v activation=%+v", dryRun, activation.MaterializationPreflight)
	}
	if !dryRun.VerifierReady || dryRun.ActuatorEnabled || dryRun.WouldMutate || dryRun.MutationExecuted {
		t.Fatalf("ready dry-run must remain non-mutating with actuator disabled, got %+v", dryRun)
	}
	if err := VerifyMutationActuatorDryRunResult(dryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestMutationActuatorDryRunBlocksUnsupportedObservedChangeKind(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)
	addMaterializationAuthorityProofToMutationActivationInput(t, &input)

	activation := EvaluateMutationActivationGates(input)
	if activation.MaterializationPreflight == nil ||
		len(activation.MaterializationPreflight.Materialization.Files) != 1 ||
		activation.MaterializationPreflight.AuthorityProof == nil ||
		len(activation.MaterializationPreflight.AuthorityProof.Files) != 1 {
		t.Fatalf("expected materialization preflight with one file, got %+v", activation.MaterializationPreflight)
	}
	activation.MaterializationPreflight.Materialization.Files[0].ChangeKind = "delete"
	activation.MaterializationPreflight.AuthorityProof.Files[0].ChangeKind = "delete"
	activation.MaterializationPreflight.AuthorityProof.AuthorityDigest = PatchMaterializationAuthorityProofDigest(*activation.MaterializationPreflight.AuthorityProof)
	activation.Digest = digestMutationActivationGateResult(activation)

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{Activation: activation})
	if dryRun.Status != MutationActuatorDryRunStatusBlocked {
		t.Fatalf("dry-run status = %q, want blocked: %+v", dryRun.Status, dryRun)
	}
	if !mutationActuatorChangeKindSlicesEqual(dryRun.ObservedChangeKinds, []string{"delete"}) {
		t.Fatalf("observed change kinds = %+v, want delete", dryRun.ObservedChangeKinds)
	}
	if !mutationActuatorChangeKindSlicesEqual(dryRun.UnsupportedChangeKinds, []string{"delete"}) {
		t.Fatalf("unsupported change kinds = %+v, want delete", dryRun.UnsupportedChangeKinds)
	}
	if !mutationActuatorHasLiveScopeBlockingReason(dryRun.BlockingReasons) {
		t.Fatalf("dry-run must carry live scope blocking reason, got %+v", dryRun.BlockingReasons)
	}
	if err := VerifyMutationActuatorDryRunResult(dryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestMutationActuatorDryRunReportsBlockedAddObservation(t *testing.T) {
	activation := MutationActivationGateResult{
		Schema:          MutationActivationGateSchemaVersion,
		Status:          MutationActivationStatusBlocked,
		Digest:          "sha256:" + strings.Repeat("1", 64),
		BlockingReasons: []string{"rollback_proven: added path rollback deletion semantics are not modeled"},
		MaterializationPreflight: &MaterializationPreflightEvidence{
			Materialization: PatchMaterializationDiagnostic{
				Files: []PatchMaterializedFileDiagnostic{
					{Path: "web/new.js", ChangeKind: CASPatchChangeAdd},
				},
			},
		},
	}

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{Activation: activation})
	if dryRun.Status != MutationActuatorDryRunStatusBlocked {
		t.Fatalf("dry-run status = %q, want blocked: %+v", dryRun.Status, dryRun)
	}
	if !mutationActuatorChangeKindSlicesEqual(dryRun.ObservedChangeKinds, []string{CASPatchChangeAdd}) {
		t.Fatalf("observed change kinds = %+v, want add", dryRun.ObservedChangeKinds)
	}
	if len(dryRun.UnsupportedChangeKinds) != 0 {
		t.Fatalf("unsupported change kinds = %+v, want none for add", dryRun.UnsupportedChangeKinds)
	}
	if err := VerifyMutationActuatorDryRunResult(dryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestVerifyMutationActuatorDryRunResultRejectsMutationClaims(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{
		Activation: EvaluateMutationActivationGates(input),
	})

	mutated := dryRun
	mutated.MutationExecuted = true
	mutated.Digest = digestMutationActuatorDryRunResult(mutated)
	if err := VerifyMutationActuatorDryRunResult(mutated); err == nil || !strings.Contains(err.Error(), "must not execute") {
		t.Fatalf("expected mutation execution claim to fail verification, got %v", err)
	}

	wouldMutate := dryRun
	wouldMutate.WouldMutate = true
	wouldMutate.Digest = digestMutationActuatorDryRunResult(wouldMutate)
	if err := VerifyMutationActuatorDryRunResult(wouldMutate); err == nil || !strings.Contains(err.Error(), "must not report would_mutate") {
		t.Fatalf("expected would_mutate claim to fail verification, got %v", err)
	}
}

func TestVerifyMutationActuatorDryRunResultRejectsReadyWithActuatorEnabled(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{
		Activation: EvaluateMutationActivationGates(input),
	})
	dryRun.ActuatorEnabled = true
	dryRun.Digest = digestMutationActuatorDryRunResult(dryRun)

	err := VerifyMutationActuatorDryRunResult(dryRun)
	if err == nil {
		t.Fatalf("expected actuator-enabled dry-run to fail verification")
	}
	if !strings.Contains(err.Error(), "actuator_enabled=false") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActuatorDryRunResultRejectsReadyWithUnsupportedChangeKind(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)
	addMaterializationAuthorityProofToMutationActivationInput(t, &input)

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{
		Activation: EvaluateMutationActivationGates(input),
	})
	if dryRun.Status != MutationActuatorDryRunStatusReady {
		t.Fatalf("expected ready dry-run fixture, got %+v", dryRun)
	}
	forged := dryRun
	forged.ObservedChangeKinds = []string{CASPatchChangeModify, "delete"}
	forged.UnsupportedChangeKinds = []string{"delete"}
	forged.BlockingReasons = append(forged.BlockingReasons, `live_scope_change_kind: unsupported change_kind "delete"; first live actuator scope allows only modify/add`)
	forged.Digest = digestMutationActuatorDryRunResult(forged)

	err := VerifyMutationActuatorDryRunResult(forged)
	if err == nil {
		t.Fatalf("expected ready dry-run with unsupported change kind to fail verification")
	}
	if !strings.Contains(err.Error(), "cannot be ready with unsupported change kinds") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActuatorDryRunResultRejectsNonCanonicalScopeSlices(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)
	addMaterializationAuthorityProofToMutationActivationInput(t, &input)

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{
		Activation: EvaluateMutationActivationGates(input),
	})
	nonCanonicalScope := dryRun
	nonCanonicalScope.LiveScope = " " + MutationActuatorLiveScopeAddModify
	nonCanonicalScope.Digest = digestMutationActuatorDryRunResult(nonCanonicalScope)
	if err := VerifyMutationActuatorDryRunResult(nonCanonicalScope); err == nil || !strings.Contains(err.Error(), "live scope") {
		t.Fatalf("expected noncanonical live scope to fail verification, got %v", err)
	}

	nonCanonicalAllowed := dryRun
	nonCanonicalAllowed.AllowedChangeKinds = []string{" " + CASPatchChangeModify, CASPatchChangeAdd}
	nonCanonicalAllowed.Digest = digestMutationActuatorDryRunResult(nonCanonicalAllowed)
	if err := VerifyMutationActuatorDryRunResult(nonCanonicalAllowed); err == nil || !strings.Contains(err.Error(), "allowed change kinds") {
		t.Fatalf("expected noncanonical allowed change kinds to fail verification, got %v", err)
	}

	nonCanonicalUnsupported := dryRun
	nonCanonicalUnsupported.Status = MutationActuatorDryRunStatusBlocked
	nonCanonicalUnsupported.ObservedChangeKinds = []string{"delete"}
	nonCanonicalUnsupported.UnsupportedChangeKinds = []string{" delete"}
	nonCanonicalUnsupported.BlockingReasons = []string{`live_scope_change_kind: unsupported change_kind "delete"; first live actuator scope allows only modify/add`}
	nonCanonicalUnsupported.Digest = digestMutationActuatorDryRunResult(nonCanonicalUnsupported)
	if err := VerifyMutationActuatorDryRunResult(nonCanonicalUnsupported); err == nil || !strings.Contains(err.Error(), "unsupported change kinds mismatch") {
		t.Fatalf("expected noncanonical unsupported change kinds to fail verification, got %v", err)
	}
}

func TestVerifyMutationActuatorDryRunResultRejectsReadyWithNonActuatorReason(t *testing.T) {
	input := mutationActivationReadyInputForMode(t, ModeControlledQueue)
	input.LiveMutationVerifierEnabled = true
	input.LiveMutationVerifierSource = MutationActivationLiveVerifierSourceEnv
	addDurableCandidateToMutationActivationInput(&input)

	dryRun := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{
		Activation: EvaluateMutationActivationGates(input),
	})
	dryRun.Status = MutationActuatorDryRunStatusReady
	dryRun.VerifierReady = true
	dryRun.BlockingReasons = []string{"mutation_binding: unrelated reason"}
	dryRun.Digest = digestMutationActuatorDryRunResult(dryRun)

	err := VerifyMutationActuatorDryRunResult(dryRun)
	if err == nil {
		t.Fatalf("expected non-actuator ready blocking reason to fail verification")
	}
	if !strings.Contains(err.Error(), "materialization authority proof") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}

func TestVerifyMutationActuatorDryRunResultRejectsForgedReadyWithoutAuthorityProof(t *testing.T) {
	blocked := EvaluateMutationActuatorDryRun(MutationActuatorDryRunInput{
		Activation: EvaluateMutationActivationGates(mutationActivationReadyInputForMode(t, ModeControlledQueue)),
	})
	forged := blocked
	forged.Status = MutationActuatorDryRunStatusReady
	forged.VerifierReady = true
	forged.BlockingReasons = []string{"live_mutation_actuator_enabled: live mutation actuator is disabled; verifier readiness does not execute mutations"}
	forged.Digest = digestMutationActuatorDryRunResult(forged)

	err := VerifyMutationActuatorDryRunResult(forged)
	if err == nil {
		t.Fatalf("expected forged ready dry-run to fail verification")
	}
	if !strings.Contains(err.Error(), "materialization authority proof") {
		t.Fatalf("unexpected verification error: %v", err)
	}
}
