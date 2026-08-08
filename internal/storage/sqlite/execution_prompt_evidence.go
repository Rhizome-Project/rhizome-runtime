package sqlite

import (
	"fmt"
	"strings"
)

const (
	executionPromptCapabilityEvidenceContract = "daemon_prompt_capability_evidence.v1"
	executionPromptCompilerStatusConverged    = "daemon_converged"
	executionPromptConvergenceAccepted        = "daemon_prompt_compiler_converged"
	executionPromptDeploymentEvidenceAccepted = "accepted_for_daemon_prompt_compiler_convergence"
	executionPromptProjectionSource           = "agent.runtime_capability_snapshot"
	executionPromptProjectionContract         = "active_capability_snapshot_projection.v1"
	executionPromptSnapshotSchema             = "daemon_capability_snapshot.v1"
	executionPromptSnapshotKind               = "run"
	executionPromptSnapshotStatus             = "enabled"
	executionPromptContractID                 = "prompt_capabilities.v1"
)

func validateExecutionPromptCapabilityEvidence(surface string, verification map[string]any, evidence []string) error {
	for _, ref := range evidence {
		if executionEvidenceRefContainsPromptMarker(ref) {
			return fmt.Errorf("%s evidence ref contains prompt-convergence marker instead of a durable ref: %s", surface, strings.TrimSpace(ref))
		}
	}
	if len(verification) == 0 {
		return nil
	}
	return walkExecutionPromptEvidence(surface+".verification", verification, evidence)
}

func walkExecutionPromptEvidence(path string, value any, evidence []string) error {
	switch typed := value.(type) {
	case map[string]any:
		if err := validateExecutionPromptEvidenceMap(path, typed, evidence); err != nil {
			return err
		}
		for key, nested := range typed {
			if err := walkExecutionPromptEvidence(path+"."+strings.TrimSpace(key), nested, evidence); err != nil {
				return err
			}
		}
	case []any:
		for idx, nested := range typed {
			if err := walkExecutionPromptEvidence(fmt.Sprintf("%s[%d]", path, idx), nested, evidence); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateExecutionPromptEvidenceMap(path string, values map[string]any, evidence []string) error {
	claimsPromptEvidence := executionPromptEvidenceHasClaim(values) || executionPromptEvidencePathClaims(path)
	if !claimsPromptEvidence {
		return nil
	}
	if err := validateExecutionPromptEvidenceValueTypes(path, values); err != nil {
		return err
	}
	if executionPromptEvidenceHasLegacyMarker(values) {
		return fmt.Errorf("%s contains legacy/non-converged prompt evidence; execution verification cannot count it as daemon prompt compiler convergence", path)
	}
	if !executionPromptEvidenceClaimsAccepted(values) {
		return fmt.Errorf("%s contains partial or unknown prompt-convergence evidence; expected %s", path, executionPromptCapabilityEvidenceContract)
	}
	if err := validateAcceptedExecutionPromptEvidence(path, values, evidence); err != nil {
		return err
	}
	return nil
}

func validateAcceptedExecutionPromptEvidence(path string, values map[string]any, evidence []string) error {
	required := map[string]string{
		"contract":               executionPromptCapabilityEvidenceContract,
		"prompt_compiler_status": executionPromptCompilerStatusConverged,
		"c2_1_convergence":       executionPromptConvergenceAccepted,
		"deployment_evidence":    executionPromptDeploymentEvidenceAccepted,
		"projection_source":      executionPromptProjectionSource,
		"projection_contract":    executionPromptProjectionContract,
		"snapshot_schema":        executionPromptSnapshotSchema,
		"snapshot_kind":          executionPromptSnapshotKind,
		"snapshot_status":        executionPromptSnapshotStatus,
		"prompt_contract":        executionPromptContractID,
	}
	for key, want := range required {
		if got := executionPromptRawString(values, key); got != want {
			return fmt.Errorf("%s has invalid %s=%q; expected %q", path, key, got, want)
		}
	}
	snapshotID := executionPromptRawString(values, "capability_snapshot_id")
	if snapshotID == "" {
		return fmt.Errorf("%s missing capability_snapshot_id", path)
	}
	if !isCanonicalCapabilitySnapshotID(snapshotID) {
		return fmt.Errorf("%s has invalid capability_snapshot_id=%q", path, snapshotID)
	}
	snapshotRef := executionPromptRawString(values, "capability_snapshot_ref")
	if snapshotRef != "capability_snapshot:"+snapshotID {
		return fmt.Errorf("%s has invalid capability_snapshot_ref=%q for capability_snapshot_id=%q", path, snapshotRef, snapshotID)
	}
	if !executionEvidenceRefsContain(evidence, snapshotRef) {
		return fmt.Errorf("%s missing durable evidence ref %q", path, snapshotRef)
	}
	projectionDigest := executionPromptRawString(values, "projection_digest")
	if !isCanonicalSHA256Digest(projectionDigest) {
		return fmt.Errorf("%s has invalid projection_digest=%q", path, projectionDigest)
	}
	return nil
}

func validateExecutionPromptEvidenceValueTypes(path string, values map[string]any) error {
	keys := executionPromptEvidenceClaimKeys()
	if executionPromptEvidenceClaimsAccepted(values) || executionPromptEvidencePathClaims(path) {
		keys = append(keys,
			"contract",
			"capability_snapshot_id",
			"capability_snapshot_ref",
			"snapshot_schema",
			"snapshot_kind",
			"snapshot_status",
		)
	}
	for _, key := range keys {
		value, ok := values[key]
		if !ok || value == nil {
			continue
		}
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s has non-string %s; prompt evidence keys must be canonical strings", path, key)
		}
	}
	return nil
}

func executionPromptEvidenceHasClaim(values map[string]any) bool {
	if executionPromptEvidenceClaimsAccepted(values) {
		return true
	}
	for _, key := range executionPromptEvidenceStrongClaimKeys() {
		if _, ok := values[key]; ok {
			return true
		}
	}
	return false
}

func executionPromptEvidenceHasLegacyMarker(values map[string]any) bool {
	for _, value := range []string{
		executionPromptString(values, "prompt_compiler_status"),
		executionPromptString(values, "c2_1_convergence"),
		executionPromptString(values, "deployment_evidence"),
		executionPromptString(values, "daemon_capability_snapshot"),
	} {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if strings.Contains(normalized, "legacy") ||
			strings.Contains(normalized, "non_converged") ||
			normalized == "excluded_until_migrated" ||
			normalized == "not_accepted_for_daemon_prompt_compiler_convergence" ||
			normalized == "absent" {
			return true
		}
	}
	return false
}

func executionPromptEvidenceClaimsAccepted(values map[string]any) bool {
	return executionPromptRawString(values, "contract") == executionPromptCapabilityEvidenceContract
}

func executionPromptEvidenceClaimKeys() []string {
	return []string{
		"prompt_compiler_status",
		"c2_1_convergence",
		"deployment_evidence",
		"projection_source",
		"projection_contract",
		"projection_digest",
		"prompt_contract",
	}
}

func executionPromptEvidenceStrongClaimKeys() []string {
	return []string{
		"prompt_compiler_status",
		"c2_1_convergence",
		"deployment_evidence",
		"projection_source",
		"projection_contract",
		"projection_digest",
	}
}

func executionPromptEvidencePathClaims(path string) bool {
	normalized := strings.ToLower(strings.TrimSpace(path))
	return strings.HasSuffix(normalized, ".prompt_capability_evidence") ||
		strings.HasSuffix(normalized, ".daemon_prompt_compiler_proof") ||
		strings.HasSuffix(normalized, ".daemon_prompt_capability_evidence")
}

func executionEvidenceRefsContain(evidence []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, ref := range evidence {
		if strings.TrimSpace(ref) == want {
			return true
		}
	}
	return false
}

func isCanonicalSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	for _, ch := range strings.TrimPrefix(value, "sha256:") {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func isCanonicalCapabilitySnapshotID(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || !strings.HasPrefix(value, "cap_") {
		return false
	}
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '_' || ch == '-':
		default:
			return false
		}
	}
	return true
}

func executionEvidenceRefContainsPromptMarker(ref string) bool {
	normalized := strings.ToLower(strings.TrimSpace(ref))
	if normalized == "" {
		return false
	}
	for _, marker := range []string{
		"prompt_compiler_status:",
		"c2_1_convergence:",
		"deployment_evidence:",
		"projection_digest:",
		"projection_source:",
		"projection_contract:",
		"prompt_contract:",
		"prompt_capability_evidence:",
		"daemon_prompt_compiler_proof:",
		"daemon_prompt_capability_evidence:",
		"capability_snapshot_ref:",
		executionPromptCapabilityEvidenceContract,
		"legacy_non_converged",
		"excluded_until_migrated",
		"not_accepted_for_daemon_prompt_compiler_convergence",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func executionPromptString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	switch value := values[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func executionPromptRawString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	switch value := values[key].(type) {
	case string:
		return value
	default:
		return ""
	}
}
