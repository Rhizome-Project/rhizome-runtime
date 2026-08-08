package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	MutationActuatorLiveResultSchemaVersion = "repo_mutation_actuator_live_result.v1"

	MutationActuatorLiveStatusApplied = "applied"

	MutationActuatorLiveFileStatusApplied        = "applied"
	MutationActuatorLiveFileStatusAlreadyApplied = "already_applied"
)

type MutationActuatorLiveResult struct {
	Schema                              string                           `json:"schema"`
	Status                              string                           `json:"status"`
	Source                              string                           `json:"source"`
	LiveScope                           string                           `json:"live_scope"`
	WorkspaceID                         string                           `json:"workspace_id,omitempty"`
	ProjectID                           string                           `json:"project_id,omitempty"`
	RepoID                              string                           `json:"repo_id,omitempty"`
	QueueID                             string                           `json:"queue_id,omitempty"`
	ItemID                              string                           `json:"item_id,omitempty"`
	TargetCheckoutID                    string                           `json:"target_checkout_id,omitempty"`
	TargetBranchName                    string                           `json:"target_branch_name,omitempty"`
	TargetLocalPath                     string                           `json:"target_local_path,omitempty"`
	TargetHeadBefore                    string                           `json:"target_head_before,omitempty"`
	TargetHeadAfter                     string                           `json:"target_head_after,omitempty"`
	TargetDirtyStateAfter               string                           `json:"target_dirty_state_after,omitempty"`
	ActivationDigest                    string                           `json:"activation_digest"`
	MaterializationDigest               string                           `json:"materialization_digest"`
	MaterializationAuthorityProofDigest string                           `json:"materialization_authority_proof_digest"`
	Files                               []MutationActuatorLiveFileResult `json:"files,omitempty"`
	NoMerge                             bool                             `json:"no_merge"`
	NoPush                              bool                             `json:"no_push"`
	NoRebase                            bool                             `json:"no_rebase"`
	NoBranchSwitch                      bool                             `json:"no_branch_switch"`
	MutationExecuted                    bool                             `json:"mutation_executed"`
	Digest                              string                           `json:"digest"`
}

type MutationActuatorLiveFileResult struct {
	Path          string `json:"path"`
	Status        string `json:"status"`
	ChangeKind    string `json:"change_kind"`
	BaseHash      string `json:"base_hash,omitempty"`
	BeforeHash    string `json:"before_hash,omitempty"`
	CandidateHash string `json:"candidate_hash"`
	AfterHash     string `json:"after_hash"`
	ContentDigest string `json:"content_digest"`
}

func FinalizeMutationActuatorLiveResult(result MutationActuatorLiveResult) MutationActuatorLiveResult {
	result.Schema = mutationActuatorLiveFirstNonEmpty(result.Schema, MutationActuatorLiveResultSchemaVersion)
	result.Status = mutationActuatorLiveFirstNonEmpty(result.Status, MutationActuatorLiveStatusApplied)
	result.Source = mutationActuatorLiveFirstNonEmpty(result.Source, MutationActivationLiveActuatorSourceRuntime)
	result.LiveScope = mutationActuatorLiveFirstNonEmpty(result.LiveScope, MutationActuatorLiveScopeAddModify)
	result.NoMerge = true
	result.NoPush = true
	result.NoRebase = true
	result.NoBranchSwitch = true
	result.Digest = digestMutationActuatorLiveResult(result)
	return result
}

func VerifyMutationActuatorLiveResult(result MutationActuatorLiveResult) error {
	if strings.TrimSpace(result.Schema) != MutationActuatorLiveResultSchemaVersion {
		return fmt.Errorf("mutation actuator live result schema is unsupported")
	}
	if strings.TrimSpace(result.Source) != MutationActivationLiveActuatorSourceRuntime {
		return fmt.Errorf("mutation actuator live result source is unsupported")
	}
	if strings.TrimSpace(result.LiveScope) != MutationActuatorLiveScopeAddModify {
		return fmt.Errorf("mutation actuator live result scope is unsupported")
	}
	if strings.TrimSpace(result.Status) != MutationActuatorLiveStatusApplied {
		return fmt.Errorf("mutation actuator live result status %q is unsupported", result.Status)
	}
	if !result.NoMerge || !result.NoPush || !result.NoRebase || !result.NoBranchSwitch {
		return fmt.Errorf("mutation actuator live result must prove merge/push/rebase/branch-switch stayed disabled")
	}
	for field, value := range map[string]string{
		"workspace_id":                           result.WorkspaceID,
		"repo_id":                                result.RepoID,
		"queue_id":                               result.QueueID,
		"item_id":                                result.ItemID,
		"target_checkout_id":                     result.TargetCheckoutID,
		"target_branch_name":                     result.TargetBranchName,
		"target_local_path":                      result.TargetLocalPath,
		"target_head_before":                     result.TargetHeadBefore,
		"target_head_after":                      result.TargetHeadAfter,
		"activation_digest":                      result.ActivationDigest,
		"materialization_digest":                 result.MaterializationDigest,
		"materialization_authority_proof_digest": result.MaterializationAuthorityProofDigest,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("mutation actuator live result %s is required", field)
		}
	}
	if result.TargetHeadBefore != result.TargetHeadAfter {
		return fmt.Errorf("mutation actuator live result changed target HEAD")
	}
	if !isCanonicalGitObjectID(result.TargetHeadBefore) || !isCanonicalGitObjectID(result.TargetHeadAfter) {
		return fmt.Errorf("mutation actuator live result target HEAD must be canonical git object id")
	}
	for field, value := range map[string]string{
		"activation_digest":                      result.ActivationDigest,
		"materialization_digest":                 result.MaterializationDigest,
		"materialization_authority_proof_digest": result.MaterializationAuthorityProofDigest,
		"digest":                                 result.Digest,
	} {
		if !isCanonicalSHA256Digest(value) {
			return fmt.Errorf("mutation actuator live result %s must be canonical sha256", field)
		}
	}
	if len(result.Files) == 0 {
		return fmt.Errorf("mutation actuator live result requires file evidence")
	}
	sawWrite := false
	previousPath := ""
	for i, file := range result.Files {
		if err := verifyMutationActuatorLiveFileResult(i, file); err != nil {
			return err
		}
		if previousPath != "" && previousPath >= file.Path {
			return fmt.Errorf("mutation actuator live file evidence must be sorted and unique")
		}
		previousPath = file.Path
		if file.Status == MutationActuatorLiveFileStatusApplied {
			sawWrite = true
		}
	}
	if result.MutationExecuted != sawWrite {
		return fmt.Errorf("mutation actuator live result mutation_executed must match applied file evidence")
	}
	if result.Digest != digestMutationActuatorLiveResult(result) {
		return fmt.Errorf("mutation actuator live result digest mismatch")
	}
	return nil
}

func verifyMutationActuatorLiveFileResult(index int, file MutationActuatorLiveFileResult) error {
	path, err := NormalizePath(file.Path)
	if err != nil {
		return fmt.Errorf("mutation actuator live file[%d] path invalid: %w", index, err)
	}
	if path != file.Path {
		return fmt.Errorf("mutation actuator live file[%d] path is not normalized", index)
	}
	changeKind := strings.TrimSpace(file.ChangeKind)
	if changeKind != CASPatchChangeModify && changeKind != CASPatchChangeAdd {
		return fmt.Errorf("mutation actuator live file[%d] change kind %q is outside add/modify scope", index, file.ChangeKind)
	}
	status := strings.TrimSpace(file.Status)
	if status != MutationActuatorLiveFileStatusApplied && status != MutationActuatorLiveFileStatusAlreadyApplied {
		return fmt.Errorf("mutation actuator live file[%d] status %q is unsupported", index, file.Status)
	}
	for field, value := range map[string]string{
		"candidate_hash": file.CandidateHash,
		"after_hash":     file.AfterHash,
		"content_digest": file.ContentDigest,
	} {
		if !isCanonicalSHA256Digest(value) {
			return fmt.Errorf("mutation actuator live file[%d] %s must be canonical sha256", index, field)
		}
	}
	if file.CandidateHash != file.ContentDigest || file.AfterHash != file.ContentDigest {
		return fmt.Errorf("mutation actuator live file[%d] after/content digest must match candidate hash", index)
	}
	switch status {
	case MutationActuatorLiveFileStatusApplied:
		if changeKind == CASPatchChangeModify {
			if !isCanonicalSHA256Digest(file.BaseHash) || file.BeforeHash != file.BaseHash {
				return fmt.Errorf("mutation actuator live file[%d] modify apply requires before_hash to match base_hash", index)
			}
		} else if strings.TrimSpace(file.BeforeHash) != "" {
			return fmt.Errorf("mutation actuator live file[%d] add apply requires empty before_hash", index)
		}
	case MutationActuatorLiveFileStatusAlreadyApplied:
		if file.BeforeHash != file.ContentDigest {
			return fmt.Errorf("mutation actuator live file[%d] already_applied requires before_hash to match content_digest", index)
		}
	}
	return nil
}

func digestMutationActuatorLiveResult(result MutationActuatorLiveResult) string {
	result.Digest = ""
	raw, _ := json.Marshal(result)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mutationActuatorLiveFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
