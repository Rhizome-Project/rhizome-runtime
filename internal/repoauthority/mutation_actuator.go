package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	MutationActuatorDryRunSchemaVersion = "repo_mutation_actuator_dry_run.v1"
	MutationActuatorLiveScopeAddModify  = "repo_mutation_live_scope.add_modify.v1"

	MutationActuatorDryRunStatusBlocked = "blocked"
	MutationActuatorDryRunStatusReady   = "dry_run_ready"

	MutationActuatorDryRunSourceHealth = "health:repo_mutation_actuator_dry_run"
)

type MutationActuatorDryRunInput struct {
	Activation MutationActivationGateResult
	Source     string
}

type MutationActuatorDryRunResult struct {
	Schema                              string   `json:"schema"`
	Status                              string   `json:"status"`
	Source                              string   `json:"source"`
	LiveScope                           string   `json:"live_scope"`
	AllowedChangeKinds                  []string `json:"allowed_change_kinds,omitempty"`
	ObservedChangeKinds                 []string `json:"observed_change_kinds,omitempty"`
	UnsupportedChangeKinds              []string `json:"unsupported_change_kinds,omitempty"`
	ActivationDigest                    string   `json:"activation_digest,omitempty"`
	ActivationStatus                    string   `json:"activation_status,omitempty"`
	MaterializationAuthorityProofDigest string   `json:"materialization_authority_proof_digest,omitempty"`
	ActivationMutationAllowed           bool     `json:"activation_mutation_allowed"`
	VerifierReady                       bool     `json:"verifier_ready"`
	ActuatorEnabled                     bool     `json:"actuator_enabled"`
	WouldMutate                         bool     `json:"would_mutate"`
	MutationExecuted                    bool     `json:"mutation_executed"`
	BlockingReasons                     []string `json:"blocking_reasons,omitempty"`
	Digest                              string   `json:"digest"`
}

func EvaluateMutationActuatorDryRun(input MutationActuatorDryRunInput) MutationActuatorDryRunResult {
	source := strings.TrimSpace(input.Source)
	if source == "" {
		source = MutationActuatorDryRunSourceHealth
	}
	activation := input.Activation
	allowedChangeKinds := mutationActuatorAllowedChangeKinds()
	observedChangeKinds := mutationActuatorObservedChangeKinds(activation)
	unsupportedChangeKinds := mutationActuatorUnsupportedChangeKinds(observedChangeKinds)
	result := MutationActuatorDryRunResult{
		Schema:                              MutationActuatorDryRunSchemaVersion,
		Status:                              MutationActuatorDryRunStatusBlocked,
		Source:                              source,
		LiveScope:                           MutationActuatorLiveScopeAddModify,
		AllowedChangeKinds:                  allowedChangeKinds,
		ObservedChangeKinds:                 observedChangeKinds,
		UnsupportedChangeKinds:              unsupportedChangeKinds,
		ActivationDigest:                    strings.TrimSpace(activation.Digest),
		ActivationStatus:                    strings.TrimSpace(activation.Status),
		MaterializationAuthorityProofDigest: mutationActivationMaterializationAuthorityProofDigest(activation),
		ActivationMutationAllowed:           activation.MutationAllowed,
		VerifierReady:                       mutationActivationGatePassed(activation, "live_mutation_verifier_enabled"),
		ActuatorEnabled:                     activation.LiveMutationActuatorEnabled,
		WouldMutate:                         false,
		MutationExecuted:                    false,
	}
	result.BlockingReasons = append(result.BlockingReasons, mutationActuatorLiveScopeBlockingReasons(unsupportedChangeKinds)...)

	if err := VerifyMutationActivationGateResult(activation); err != nil {
		result.BlockingReasons = append(result.BlockingReasons, "activation verification failed: "+err.Error())
		result.Digest = digestMutationActuatorDryRunResult(result)
		return result
	}
	if strings.TrimSpace(activation.Source) != MutationActivationSourceDurableControlledQueueCandidate {
		result.BlockingReasons = append(result.BlockingReasons, "activation source is not a durable controlled queue candidate")
		result.Digest = digestMutationActuatorDryRunResult(result)
		return result
	}
	if len(unsupportedChangeKinds) > 0 {
		result.Digest = digestMutationActuatorDryRunResult(result)
		return result
	}
	if activation.MutationAllowed || activation.LiveMutationActuatorEnabled {
		result.BlockingReasons = append(result.BlockingReasons, "live actuator is enabled; dry-run actuator must not execute mutations")
		result.Digest = digestMutationActuatorDryRunResult(result)
		return result
	}
	failed := mutationActivationFailedGateNames(activation)
	if len(failed) != 1 || failed[0] != "live_mutation_actuator_enabled" {
		result.BlockingReasons = append(result.BlockingReasons, activation.BlockingReasons...)
		if len(result.BlockingReasons) == 0 {
			result.BlockingReasons = append(result.BlockingReasons, "activation is not verifier-ready")
		}
		result.Digest = digestMutationActuatorDryRunResult(result)
		return result
	}

	result.Status = MutationActuatorDryRunStatusReady
	result.BlockingReasons = append(result.BlockingReasons, activation.BlockingReasons...)
	result.Digest = digestMutationActuatorDryRunResult(result)
	return result
}

func VerifyMutationActuatorDryRunResult(result MutationActuatorDryRunResult) error {
	if strings.TrimSpace(result.Schema) != MutationActuatorDryRunSchemaVersion {
		return fmt.Errorf("mutation actuator dry-run schema is unsupported")
	}
	if strings.TrimSpace(result.Source) != MutationActuatorDryRunSourceHealth {
		return fmt.Errorf("mutation actuator dry-run source is unsupported")
	}
	if result.LiveScope != MutationActuatorLiveScopeAddModify {
		return fmt.Errorf("mutation actuator dry-run live scope is unsupported")
	}
	if !mutationActuatorChangeKindSlicesEqual(result.AllowedChangeKinds, mutationActuatorAllowedChangeKinds()) {
		return fmt.Errorf("mutation actuator dry-run allowed change kinds are unsupported")
	}
	if err := mutationActuatorValidateObservedChangeKinds(result.ObservedChangeKinds); err != nil {
		return err
	}
	expectedUnsupported := mutationActuatorUnsupportedChangeKinds(result.ObservedChangeKinds)
	if !mutationActuatorChangeKindSlicesEqual(result.UnsupportedChangeKinds, expectedUnsupported) {
		return fmt.Errorf("mutation actuator dry-run unsupported change kinds mismatch")
	}
	if !isCanonicalSHA256Digest(result.Digest) {
		return fmt.Errorf("mutation actuator dry-run digest is required")
	}
	expectedDigest := digestMutationActuatorDryRunResult(result)
	if result.Digest != expectedDigest {
		return fmt.Errorf("mutation actuator dry-run digest mismatch")
	}
	if result.WouldMutate {
		return fmt.Errorf("mutation actuator dry-run must not report would_mutate")
	}
	if result.MutationExecuted {
		return fmt.Errorf("mutation actuator dry-run must not execute mutation")
	}
	if result.ActivationMutationAllowed {
		return fmt.Errorf("mutation actuator dry-run requires activation mutation_allowed=false")
	}
	if result.ActuatorEnabled {
		return fmt.Errorf("mutation actuator dry-run requires actuator_enabled=false")
	}
	if len(result.UnsupportedChangeKinds) > 0 && result.Status == MutationActuatorDryRunStatusReady {
		return fmt.Errorf("mutation actuator dry-run cannot be ready with unsupported change kinds")
	}
	if len(result.UnsupportedChangeKinds) > 0 && !mutationActuatorHasLiveScopeBlockingReason(result.BlockingReasons) {
		return fmt.Errorf("mutation actuator dry-run with unsupported change kinds requires live scope blocking reason")
	}
	if !isCanonicalSHA256Digest(result.ActivationDigest) {
		return fmt.Errorf("mutation actuator dry-run activation digest is required")
	}
	if strings.TrimSpace(result.MaterializationAuthorityProofDigest) != "" && !isCanonicalSHA256Digest(result.MaterializationAuthorityProofDigest) {
		return fmt.Errorf("mutation actuator dry-run materialization authority proof digest must be canonical sha256")
	}
	if strings.TrimSpace(result.ActivationStatus) != MutationActivationStatusBlocked {
		return fmt.Errorf("mutation actuator dry-run requires blocked activation status")
	}
	switch result.Status {
	case MutationActuatorDryRunStatusBlocked:
		if len(result.BlockingReasons) == 0 {
			return fmt.Errorf("blocked mutation actuator dry-run requires blocking reasons")
		}
	case MutationActuatorDryRunStatusReady:
		if !result.VerifierReady {
			return fmt.Errorf("ready mutation actuator dry-run requires verifier_ready=true")
		}
		if !isCanonicalSHA256Digest(result.MaterializationAuthorityProofDigest) {
			return fmt.Errorf("ready mutation actuator dry-run requires materialization authority proof digest")
		}
		if len(result.BlockingReasons) != 1 || !strings.HasPrefix(result.BlockingReasons[0], "live_mutation_actuator_enabled: ") {
			return fmt.Errorf("ready mutation actuator dry-run requires only the live actuator blocking reason")
		}
	default:
		return fmt.Errorf("unsupported mutation actuator dry-run status %q", result.Status)
	}
	return nil
}

func mutationActivationMaterializationAuthorityProofDigest(activation MutationActivationGateResult) string {
	if activation.MaterializationPreflight == nil || activation.MaterializationPreflight.AuthorityProof == nil {
		return ""
	}
	return strings.TrimSpace(activation.MaterializationPreflight.AuthorityProof.AuthorityDigest)
}

func mutationActuatorAllowedChangeKinds() []string {
	return []string{CASPatchChangeModify, CASPatchChangeAdd}
}

func mutationActuatorObservedChangeKinds(activation MutationActivationGateResult) []string {
	if activation.MaterializationPreflight == nil {
		return nil
	}
	seen := map[string]struct{}{}
	kinds := make([]string, 0, len(activation.MaterializationPreflight.Materialization.Files))
	for _, file := range activation.MaterializationPreflight.Materialization.Files {
		kind := mutationActuatorNormalizeChangeKind(file.ChangeKind)
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		kinds = append(kinds, kind)
	}
	return kinds
}

func mutationActuatorNormalizeChangeKind(changeKind string) string {
	changeKind = strings.TrimSpace(changeKind)
	if changeKind == "" {
		return CASPatchChangeModify
	}
	return changeKind
}

func mutationActuatorUnsupportedChangeKinds(observed []string) []string {
	allowed := map[string]struct{}{}
	for _, kind := range mutationActuatorAllowedChangeKinds() {
		allowed[kind] = struct{}{}
	}
	unsupported := make([]string, 0)
	seen := map[string]struct{}{}
	for _, kind := range observed {
		kind = mutationActuatorNormalizeChangeKind(kind)
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		if _, ok := allowed[kind]; !ok {
			unsupported = append(unsupported, kind)
		}
	}
	return unsupported
}

func mutationActuatorLiveScopeBlockingReasons(unsupported []string) []string {
	reasons := make([]string, 0, len(unsupported))
	for _, kind := range unsupported {
		reasons = append(reasons, fmt.Sprintf("live_scope_change_kind: unsupported change_kind %q; first live actuator scope allows only modify/add", kind))
	}
	return reasons
}

func mutationActuatorHasLiveScopeBlockingReason(reasons []string) bool {
	for _, reason := range reasons {
		if strings.HasPrefix(strings.TrimSpace(reason), "live_scope_change_kind: ") {
			return true
		}
	}
	return false
}

func mutationActuatorValidateObservedChangeKinds(kinds []string) error {
	seen := map[string]struct{}{}
	for i, kind := range kinds {
		if strings.TrimSpace(kind) == "" {
			return fmt.Errorf("mutation actuator dry-run observed change kind[%d] is empty", i)
		}
		if kind != strings.TrimSpace(kind) {
			return fmt.Errorf("mutation actuator dry-run observed change kind[%d] is not normalized", i)
		}
		if _, ok := seen[kind]; ok {
			return fmt.Errorf("mutation actuator dry-run observed change kind %q is duplicated", kind)
		}
		seen[kind] = struct{}{}
	}
	return nil
}

func mutationActuatorChangeKindSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mutationActivationGatePassed(result MutationActivationGateResult, name string) bool {
	for _, gate := range result.Gates {
		if strings.TrimSpace(gate.Name) == name {
			return gate.Passed
		}
	}
	return false
}

func mutationActivationFailedGateNames(result MutationActivationGateResult) []string {
	failed := make([]string, 0)
	for _, gate := range result.Gates {
		if !gate.Passed {
			failed = append(failed, strings.TrimSpace(gate.Name))
		}
	}
	return failed
}

func digestMutationActuatorDryRunResult(result MutationActuatorDryRunResult) string {
	result.Digest = ""
	raw, _ := json.Marshal(result)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
