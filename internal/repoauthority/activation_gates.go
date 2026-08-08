package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	MutationActivationGateSchemaVersion = "repo_mutation_activation_gates.v1"

	MutationActivationStatusBlocked = "blocked"
	MutationActivationStatusReady   = "ready"

	MutationActivationAuthorityModeControlledQueue = ModeControlledQueue
	MutationActivationReviewerMeshAdvisoryOnly     = "advisory_only"
	MutationActivationOperatorEnablementScope      = PatchQueueOperatorEnablementScopeMutationActivation

	MutationActivationSourceSyntheticPatchOnly                   = "synthetic_patch_only"
	MutationActivationSourceDurableQueueNoControlledCandidate    = "durable_queue_no_controlled_candidate"
	MutationActivationSourceDurableControlledQueueCandidate      = "durable_controlled_queue_candidate"
	MutationActivationSourceDurableControlledQueueCandidateError = "durable_controlled_queue_candidate_read_failed"

	MutationActivationLiveVerifierSourceEnv     = "env:RHIZOME_REPO_MUTATION_LIVE_VERIFIER"
	MutationActivationLiveActuatorSourceRuntime = "runtime:repo_mutation_actuator"

	MaterializationPreflightAuthorityProofRequired = "materialization preflight requires non-redacted materialization authority proof"
)

var mutationActivationRequiredGateNames = []string{
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
}

type MutationActivationGateInput struct {
	AuthorityMode               string
	DirectMergeDisabled         bool
	QueueDurable                bool
	ReviewerMeshMode            string
	LiveMutationVerifierEnabled bool
	LiveMutationVerifierSource  string
	LiveMutationActuatorEnabled bool
	LiveMutationActuatorSource  string
	Source                      string
	SourceError                 string
	Candidate                   *MutationActivationCandidateSummary
	WorktreeIdentity            WorktreeIdentityEvidence
	TargetWorktreeIdentity      WorktreeIdentityEvidence
	Context                     Context
	PatchQueueItem              PatchQueueItem
	RollbackEvidence            PatchQueueRollback
	ReviewerAdvisory            PatchQueueReviewerAdvisory
	OperatorEnablement          PatchQueueOperatorEnablement
	PatchMaterialization        PatchMaterialization
	PatchMaterializationProof   PatchMaterializationAuthorityProof
}

type WorktreeIdentityEvidence struct {
	RepoID     string `json:"repo_id"`
	CheckoutID string `json:"checkout_id"`
	BranchID   string `json:"branch_id"`
	BranchName string `json:"branch_name"`
	MachineID  string `json:"machine_id"`
	LocalPath  string `json:"local_path"`
	BaseSHA    string `json:"base_sha"`
	HeadSHA    string `json:"head_sha"`

	ReadbackState        string `json:"readback_state,omitempty"`
	ReadbackError        string `json:"readback_error,omitempty"`
	ObservedWorktreeRoot string `json:"observed_worktree_root,omitempty"`
	ObservedBranchName   string `json:"observed_branch_name,omitempty"`
	ObservedHeadSHA      string `json:"observed_head_sha,omitempty"`
	ObservedDirtyState   string `json:"observed_dirty_state,omitempty"`
}

type MutationActivationCandidateSummary struct {
	WorkspaceID      string `json:"workspace_id,omitempty"`
	ProjectID        string `json:"project_id,omitempty"`
	RepoID           string `json:"repo_id,omitempty"`
	QueueID          string `json:"queue_id,omitempty"`
	ItemID           string `json:"item_id,omitempty"`
	BranchID         string `json:"branch_id,omitempty"`
	BranchName       string `json:"branch_name,omitempty"`
	CheckoutID       string `json:"checkout_id,omitempty"`
	TargetCheckoutID string `json:"target_checkout_id,omitempty"`
	TargetBranchName string `json:"target_branch_name,omitempty"`
	State            string `json:"state,omitempty"`
	BaseSHA          string `json:"base_sha,omitempty"`
	HeadSHA          string `json:"head_sha,omitempty"`
}

type MutationActivationGateResult struct {
	Schema                      string                              `json:"schema"`
	Status                      string                              `json:"status"`
	MutationAllowed             bool                                `json:"mutation_allowed"`
	AuthorityMode               string                              `json:"authority_mode"`
	ContextMode                 string                              `json:"context_mode"`
	ReviewerMeshMode            string                              `json:"reviewer_mesh_mode"`
	LiveMutationVerifierEnabled bool                                `json:"live_mutation_verifier_enabled"`
	LiveMutationVerifierSource  string                              `json:"live_mutation_verifier_source,omitempty"`
	LiveMutationActuatorEnabled bool                                `json:"live_mutation_actuator_enabled"`
	LiveMutationActuatorSource  string                              `json:"live_mutation_actuator_source,omitempty"`
	QueueDurable                bool                                `json:"queue_durable"`
	Source                      string                              `json:"source,omitempty"`
	SourceError                 string                              `json:"source_error,omitempty"`
	Candidate                   *MutationActivationCandidateSummary `json:"candidate,omitempty"`
	WorktreeIdentity            *WorktreeIdentityEvidence           `json:"worktree_identity,omitempty"`
	TargetWorktreeIdentity      *WorktreeIdentityEvidence           `json:"target_worktree_identity,omitempty"`
	MutationBindingEvidence     *MutationBindingEvidence            `json:"mutation_binding_evidence,omitempty"`
	MergeAdmissionEvidence      *MergeAdmissionEvidence             `json:"merge_admission_evidence,omitempty"`
	RetryBoundEvidence          *RetryBoundEvidence                 `json:"retry_bound_evidence,omitempty"`
	RollbackProofEvidence       *RollbackProofEvidence              `json:"rollback_proof_evidence,omitempty"`
	ReviewerAdvisoryEvidence    *ReviewerAdvisoryEvidence           `json:"reviewer_advisory_evidence,omitempty"`
	OperatorEnablementEvidence  *OperatorEnablementEvidence         `json:"operator_enablement_evidence,omitempty"`
	MaterializationPreflight    *MaterializationPreflightEvidence   `json:"materialization_preflight,omitempty"`
	DirectMergeDisabled         bool                                `json:"direct_merge_disabled"`
	Gates                       []MutationActivationGate            `json:"gates"`
	BlockingReasons             []string                            `json:"blocking_reasons,omitempty"`
	Digest                      string                              `json:"digest"`
}

type MutationActivationGate struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Reason string `json:"reason,omitempty"`
}

type MutationBindingEvidence struct {
	State       string `json:"state"`
	Ready       bool   `json:"ready"`
	ReadyError  string `json:"ready_error,omitempty"`
	ContextMode string `json:"context_mode,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	RunID       string `json:"run_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`

	PrincipalType               string            `json:"principal_type,omitempty"`
	PrincipalID                 string            `json:"principal_id,omitempty"`
	CapabilitySnapshotID        string            `json:"capability_snapshot_id,omitempty"`
	CapabilitySnapshotSchema    string            `json:"capability_snapshot_schema,omitempty"`
	RepoRoot                    string            `json:"repo_root,omitempty"`
	BaseRef                     string            `json:"base_ref,omitempty"`
	BaseTreeHash                string            `json:"base_tree_hash,omitempty"`
	BaseFileHashCount           int               `json:"base_file_hash_count,omitempty"`
	BaseFileHashPaths           []string          `json:"base_file_hash_paths,omitempty"`
	BaseFileHashes              map[string]string `json:"base_file_hashes,omitempty"`
	RepoLeaseID                 string            `json:"repo_lease_id,omitempty"`
	LeaseTerm                   int64             `json:"lease_term,omitempty"`
	PatchQueueID                string            `json:"patch_queue_id,omitempty"`
	PatchQueueItemID            string            `json:"patch_queue_item_id,omitempty"`
	OperationID                 string            `json:"operation_id,omitempty"`
	OperationKind               string            `json:"operation_kind,omitempty"`
	OperationKindAccepted       bool              `json:"operation_kind_accepted"`
	ContextDigest               string            `json:"context_digest,omitempty"`
	ContextDigestError          string            `json:"context_digest_error,omitempty"`
	PatchQueueContextDigest     string            `json:"patch_queue_context_digest,omitempty"`
	PatchQueueContextError      string            `json:"patch_queue_context_error,omitempty"`
	PatchQueueItemSchema        string            `json:"patch_queue_item_schema,omitempty"`
	PatchQueueItemRecordID      string            `json:"patch_queue_item_record_id,omitempty"`
	PatchQueueItemQueueID       string            `json:"patch_queue_item_queue_id,omitempty"`
	PatchQueueItemItemID        string            `json:"patch_queue_item_item_id,omitempty"`
	PatchQueueItemState         string            `json:"patch_queue_item_state,omitempty"`
	PatchQueueItemContext       string            `json:"patch_queue_item_context_digest,omitempty"`
	PatchQueueItemRepoLeaseID   string            `json:"patch_queue_item_repo_lease_id,omitempty"`
	PatchQueueItemLeaseTerm     int64             `json:"patch_queue_item_lease_term,omitempty"`
	PatchQueueItemOperationID   string            `json:"patch_queue_item_operation_id,omitempty"`
	PatchQueueItemOperationKind string            `json:"patch_queue_item_operation_kind,omitempty"`
	Pathset                     []string          `json:"pathset,omitempty"`
	PatchQueueItemPathset       []string          `json:"patch_queue_item_pathset,omitempty"`
	MissingRefs                 []string          `json:"missing_refs,omitempty"`
	Mismatches                  []string          `json:"mismatches,omitempty"`
}

type MergeAdmissionEvidence struct {
	State      string `json:"state"`
	Ready      bool   `json:"ready"`
	ReadyError string `json:"ready_error,omitempty"`

	PatchQueueItemSchema        string   `json:"patch_queue_item_schema,omitempty"`
	PatchQueueItemRecordID      string   `json:"patch_queue_item_record_id,omitempty"`
	PatchQueueItemQueueID       string   `json:"patch_queue_item_queue_id,omitempty"`
	PatchQueueItemItemID        string   `json:"patch_queue_item_item_id,omitempty"`
	PatchQueueItemState         string   `json:"patch_queue_item_state,omitempty"`
	PatchQueueItemContext       string   `json:"patch_queue_item_context_digest,omitempty"`
	PatchQueueItemRepoLeaseID   string   `json:"patch_queue_item_repo_lease_id,omitempty"`
	PatchQueueItemLeaseTerm     int64    `json:"patch_queue_item_lease_term,omitempty"`
	PatchQueueItemOperationID   string   `json:"patch_queue_item_operation_id,omitempty"`
	PatchQueueItemOperationKind string   `json:"patch_queue_item_operation_kind,omitempty"`
	PatchQueueItemPathset       []string `json:"patch_queue_item_pathset,omitempty"`

	CASStatus           string                 `json:"cas_status,omitempty"`
	CASPatchDigest      string                 `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest string                 `json:"cas_evaluation_digest,omitempty"`
	CASResult           CASPatchApplyResult    `json:"cas_result,omitempty"`
	TestEvidence        PatchQueueTestEvidence `json:"test_evidence,omitempty"`
	TestEvidenceDigest  string                 `json:"test_evidence_digest,omitempty"`
}

type RetryBoundEvidence struct {
	State      string `json:"state"`
	Ready      bool   `json:"ready"`
	ReadyError string `json:"ready_error,omitempty"`

	PatchQueueItemSchema        string `json:"patch_queue_item_schema,omitempty"`
	PatchQueueItemRecordID      string `json:"patch_queue_item_record_id,omitempty"`
	PatchQueueItemQueueID       string `json:"patch_queue_item_queue_id,omitempty"`
	PatchQueueItemItemID        string `json:"patch_queue_item_item_id,omitempty"`
	PatchQueueItemState         string `json:"patch_queue_item_state,omitempty"`
	PatchQueueItemContext       string `json:"patch_queue_item_context_digest,omitempty"`
	PatchQueueItemRepoLeaseID   string `json:"patch_queue_item_repo_lease_id,omitempty"`
	PatchQueueItemLeaseTerm     int64  `json:"patch_queue_item_lease_term,omitempty"`
	PatchQueueItemOperationID   string `json:"patch_queue_item_operation_id,omitempty"`
	PatchQueueItemOperationKind string `json:"patch_queue_item_operation_kind,omitempty"`
	Attempt                     int    `json:"attempt"`
	MaxAttempts                 int    `json:"max_attempts"`
	NextRetryAt                 string `json:"next_retry_at,omitempty"`
	DeadLetteredAt              string `json:"dead_lettered_at,omitempty"`
}

type RollbackProofEvidence struct {
	State      string `json:"state"`
	Ready      bool   `json:"ready"`
	ReadyError string `json:"ready_error,omitempty"`

	PatchQueueItemSchema        string `json:"patch_queue_item_schema,omitempty"`
	PatchQueueItemRecordID      string `json:"patch_queue_item_record_id,omitempty"`
	PatchQueueItemQueueID       string `json:"patch_queue_item_queue_id,omitempty"`
	PatchQueueItemItemID        string `json:"patch_queue_item_item_id,omitempty"`
	PatchQueueItemState         string `json:"patch_queue_item_state,omitempty"`
	PatchQueueItemContext       string `json:"patch_queue_item_context_digest,omitempty"`
	PatchQueueItemRepoLeaseID   string `json:"patch_queue_item_repo_lease_id,omitempty"`
	PatchQueueItemLeaseTerm     int64  `json:"patch_queue_item_lease_term,omitempty"`
	PatchQueueItemOperationID   string `json:"patch_queue_item_operation_id,omitempty"`
	PatchQueueItemOperationKind string `json:"patch_queue_item_operation_kind,omitempty"`
	CASPatchDigest              string `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest         string `json:"cas_evaluation_digest,omitempty"`

	RollbackEvidence       PatchQueueRollback `json:"rollback_evidence,omitempty"`
	RollbackEvidenceDigest string             `json:"rollback_evidence_digest,omitempty"`
}

type ReviewerAdvisoryEvidence struct {
	State      string `json:"state"`
	Ready      bool   `json:"ready"`
	ReadyError string `json:"ready_error,omitempty"`

	PatchQueueItemSchema        string `json:"patch_queue_item_schema,omitempty"`
	PatchQueueItemRecordID      string `json:"patch_queue_item_record_id,omitempty"`
	PatchQueueItemQueueID       string `json:"patch_queue_item_queue_id,omitempty"`
	PatchQueueItemItemID        string `json:"patch_queue_item_item_id,omitempty"`
	PatchQueueItemState         string `json:"patch_queue_item_state,omitempty"`
	PatchQueueItemContext       string `json:"patch_queue_item_context_digest,omitempty"`
	PatchQueueItemRepoLeaseID   string `json:"patch_queue_item_repo_lease_id,omitempty"`
	PatchQueueItemLeaseTerm     int64  `json:"patch_queue_item_lease_term,omitempty"`
	PatchQueueItemOperationID   string `json:"patch_queue_item_operation_id,omitempty"`
	PatchQueueItemOperationKind string `json:"patch_queue_item_operation_kind,omitempty"`
	CASPatchDigest              string `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest         string `json:"cas_evaluation_digest,omitempty"`
	RollbackEvidenceDigest      string `json:"rollback_evidence_digest,omitempty"`
	WorkspaceID                 string `json:"workspace_id,omitempty"`
	ProjectID                   string `json:"project_id,omitempty"`

	Advisory       PatchQueueReviewerAdvisory `json:"advisory,omitempty"`
	AdvisoryDigest string                     `json:"advisory_digest,omitempty"`
}

type OperatorEnablementEvidence struct {
	State      string `json:"state"`
	Ready      bool   `json:"ready"`
	ReadyError string `json:"ready_error,omitempty"`

	PatchQueueItemSchema        string `json:"patch_queue_item_schema,omitempty"`
	PatchQueueItemRecordID      string `json:"patch_queue_item_record_id,omitempty"`
	PatchQueueItemQueueID       string `json:"patch_queue_item_queue_id,omitempty"`
	PatchQueueItemItemID        string `json:"patch_queue_item_item_id,omitempty"`
	PatchQueueItemState         string `json:"patch_queue_item_state,omitempty"`
	PatchQueueItemContext       string `json:"patch_queue_item_context_digest,omitempty"`
	PatchQueueItemRepoLeaseID   string `json:"patch_queue_item_repo_lease_id,omitempty"`
	PatchQueueItemLeaseTerm     int64  `json:"patch_queue_item_lease_term,omitempty"`
	PatchQueueItemOperationID   string `json:"patch_queue_item_operation_id,omitempty"`
	PatchQueueItemOperationKind string `json:"patch_queue_item_operation_kind,omitempty"`
	CASPatchDigest              string `json:"cas_patch_digest,omitempty"`
	RollbackEvidenceDigest      string `json:"rollback_evidence_digest,omitempty"`
	ReviewerAdvisoryDigest      string `json:"reviewer_advisory_digest,omitempty"`
	WorkspaceID                 string `json:"workspace_id,omitempty"`
	ProjectID                   string `json:"project_id,omitempty"`

	Enablement       PatchQueueOperatorEnablement `json:"enablement,omitempty"`
	EnablementDigest string                       `json:"enablement_digest,omitempty"`
}

type MaterializationPreflightEvidence struct {
	State      string `json:"state"`
	Ready      bool   `json:"ready"`
	ReadyError string `json:"ready_error,omitempty"`

	PatchQueueItemSchema        string `json:"patch_queue_item_schema,omitempty"`
	PatchQueueItemRecordID      string `json:"patch_queue_item_record_id,omitempty"`
	PatchQueueItemQueueID       string `json:"patch_queue_item_queue_id,omitempty"`
	PatchQueueItemItemID        string `json:"patch_queue_item_item_id,omitempty"`
	PatchQueueItemState         string `json:"patch_queue_item_state,omitempty"`
	PatchQueueItemContext       string `json:"patch_queue_item_context_digest,omitempty"`
	PatchQueueItemRepoLeaseID   string `json:"patch_queue_item_repo_lease_id,omitempty"`
	PatchQueueItemLeaseTerm     int64  `json:"patch_queue_item_lease_term,omitempty"`
	PatchQueueItemOperationID   string `json:"patch_queue_item_operation_id,omitempty"`
	PatchQueueItemOperationKind string `json:"patch_queue_item_operation_kind,omitempty"`
	CASPatchDigest              string `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest         string `json:"cas_evaluation_digest,omitempty"`
	RollbackEvidenceDigest      string `json:"rollback_evidence_digest,omitempty"`
	ReviewerAdvisoryDigest      string `json:"reviewer_advisory_digest,omitempty"`
	OperatorEnablementDigest    string `json:"operator_enablement_digest,omitempty"`
	WorkspaceID                 string `json:"workspace_id,omitempty"`
	ProjectID                   string `json:"project_id,omitempty"`

	WorktreeIdentity      WorktreeIdentityEvidence            `json:"worktree_identity,omitempty"`
	Materialization       PatchMaterializationDiagnostic      `json:"materialization,omitempty"`
	MaterializationDigest string                              `json:"materialization_digest,omitempty"`
	AuthorityProof        *PatchMaterializationAuthorityProof `json:"authority_proof,omitempty"`
}

type PatchMaterializationDiagnostic struct {
	Schema                string                            `json:"schema,omitempty"`
	WorkspaceID           string                            `json:"workspace_id,omitempty"`
	ProjectID             string                            `json:"project_id,omitempty"`
	QueueID               string                            `json:"queue_id,omitempty"`
	ItemID                string                            `json:"item_id,omitempty"`
	OperationID           string                            `json:"operation_id,omitempty"`
	OperationKind         string                            `json:"operation_kind,omitempty"`
	CASPatchDigest        string                            `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest   string                            `json:"cas_evaluation_digest,omitempty"`
	FileCount             int                               `json:"file_count"`
	Files                 []PatchMaterializedFileDiagnostic `json:"files,omitempty"`
	RecordedBy            string                            `json:"recorded_by,omitempty"`
	RecordedAt            string                            `json:"recorded_at,omitempty"`
	MaterializationDigest string                            `json:"materialization_digest,omitempty"`
}

type PatchMaterializedFileDiagnostic struct {
	Path            string `json:"path"`
	ChangeKind      string `json:"change_kind,omitempty"`
	BaseHash        string `json:"base_hash,omitempty"`
	CandidateHash   string `json:"candidate_hash,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	ContentDigest   string `json:"content_digest,omitempty"`
}

func EvaluateMutationActivationGates(input MutationActivationGateInput) MutationActivationGateResult {
	authorityMode := strings.TrimSpace(input.AuthorityMode)
	if authorityMode == "" {
		authorityMode = ModePatchOnlyTempRepo
	}
	contextMode := strings.TrimSpace(input.Context.Mode)
	if contextMode == "" {
		contextMode = ModePatchOnlyTempRepo
	}
	reviewerMeshMode := strings.TrimSpace(input.ReviewerMeshMode)
	if reviewerMeshMode == "" {
		reviewerMeshMode = "unknown"
	}

	result := MutationActivationGateResult{
		Schema:                      MutationActivationGateSchemaVersion,
		AuthorityMode:               authorityMode,
		ContextMode:                 contextMode,
		ReviewerMeshMode:            reviewerMeshMode,
		LiveMutationVerifierEnabled: input.LiveMutationVerifierEnabled,
		LiveMutationVerifierSource:  strings.TrimSpace(input.LiveMutationVerifierSource),
		LiveMutationActuatorEnabled: input.LiveMutationActuatorEnabled,
		LiveMutationActuatorSource:  strings.TrimSpace(input.LiveMutationActuatorSource),
		QueueDurable:                input.QueueDurable,
		Source:                      strings.TrimSpace(input.Source),
		SourceError:                 strings.TrimSpace(input.SourceError),
		Candidate:                   cloneMutationActivationCandidateSummary(input.Candidate),
		WorktreeIdentity:            cloneWorktreeIdentityEvidence(input.WorktreeIdentity),
		TargetWorktreeIdentity:      cloneWorktreeIdentityEvidence(input.TargetWorktreeIdentity),
		MutationBindingEvidence:     buildMutationBindingEvidence(input.Context, input.PatchQueueItem, strings.TrimSpace(input.Source) == MutationActivationSourceDurableControlledQueueCandidate),
		MergeAdmissionEvidence:      buildMergeAdmissionEvidence(input.Context, input.PatchQueueItem, strings.TrimSpace(input.Source) == MutationActivationSourceDurableControlledQueueCandidate),
		RetryBoundEvidence:          buildRetryBoundEvidence(input.PatchQueueItem, strings.TrimSpace(input.Source) == MutationActivationSourceDurableControlledQueueCandidate),
		RollbackProofEvidence:       buildRollbackProofEvidence(input.PatchQueueItem, input.RollbackEvidence, strings.TrimSpace(input.Source) == MutationActivationSourceDurableControlledQueueCandidate),
		ReviewerAdvisoryEvidence:    buildReviewerAdvisoryEvidence(input.PatchQueueItem, input.ReviewerAdvisory, strings.TrimSpace(input.Source) == MutationActivationSourceDurableControlledQueueCandidate),
		OperatorEnablementEvidence:  buildOperatorEnablementEvidence(input.PatchQueueItem, input.OperatorEnablement, input.ReviewerAdvisory, strings.TrimSpace(input.Source) == MutationActivationSourceDurableControlledQueueCandidate),
		MaterializationPreflight:    buildMaterializationPreflightEvidence(input.PatchQueueItem, input.WorktreeIdentity, input.PatchMaterialization, input.PatchMaterializationProof, strings.TrimSpace(input.Source) == MutationActivationSourceDurableControlledQueueCandidate),
		DirectMergeDisabled:         input.DirectMergeDisabled,
		Gates:                       make([]MutationActivationGate, 0, len(mutationActivationRequiredGateNames)),
	}

	result.addGate("controlled_authority_mode", authorityMode == MutationActivationAuthorityModeControlledQueue, fmt.Sprintf("authority mode is %q, not %q", authorityMode, MutationActivationAuthorityModeControlledQueue))
	result.addGate("controlled_context_mode", contextMode == MutationActivationAuthorityModeControlledQueue, fmt.Sprintf("repo authority context mode is %q, not %q", contextMode, MutationActivationAuthorityModeControlledQueue))
	result.addGate("direct_merge_disabled", input.DirectMergeDisabled, "direct merge/push/rebase must stay disabled while controlled queue activates")
	result.addGate("durable_patch_queue", input.QueueDurable, "patch queue durability across restart is not proven")
	result.addGate("canonical_worktree_identity", worktreeIdentityReady(input.WorktreeIdentity), "repo, checkout, branch, machine, local path, base sha, and head sha are required")
	result.addGate("live_actuator_target_identity", actuatorTargetWorktreeIdentityReady(input.WorktreeIdentity, input.TargetWorktreeIdentity), "live actuator target integration worktree is missing, non-canonical, or aliases the candidate checkout")

	if err := mutationBindingReady(input.Context, input.PatchQueueItem); err != nil {
		result.addGate("mutation_binding", false, err.Error())
	} else {
		result.addGate("mutation_binding", true, "")
	}
	if err := mergeAdmissionReady(input.Context, input.PatchQueueItem); err != nil {
		result.addGate("merge_admission_conflict_safe", false, err.Error())
	} else {
		result.addGate("merge_admission_conflict_safe", true, "")
	}
	if err := boundedRetryReady(input.PatchQueueItem); err != nil {
		result.addGate("bounded_retry", false, err.Error())
	} else {
		result.addGate("bounded_retry", true, "")
	}
	if err := rollbackEvidenceReady(input.RollbackEvidence, input.PatchQueueItem); err != nil {
		result.addGate("rollback_proven", false, err.Error())
	} else {
		result.addGate("rollback_proven", true, "")
	}
	if err := reviewerAdvisoryReady(input.ReviewerAdvisory, input.PatchQueueItem); err != nil {
		result.addGate("reviewer_advisory_recorded", false, err.Error())
	} else {
		result.addGate("reviewer_advisory_recorded", true, "")
	}
	result.addGate("reviewer_mesh_advisory_only", reviewerMeshMode == MutationActivationReviewerMeshAdvisoryOnly, fmt.Sprintf("reviewer mesh mode is %q, not %q", reviewerMeshMode, MutationActivationReviewerMeshAdvisoryOnly))
	if err := operatorEnablementReady(input.OperatorEnablement, input.PatchQueueItem, input.ReviewerAdvisory); err != nil {
		result.addGate("operator_enablement_recorded", false, err.Error())
	} else {
		result.addGate("operator_enablement_recorded", true, "")
	}
	if !mutationActivationResultMaterializationPreflightReady(result) {
		reason := "materialization preflight evidence is required"
		if result.MaterializationPreflight != nil && strings.TrimSpace(result.MaterializationPreflight.ReadyError) != "" {
			reason = result.MaterializationPreflight.ReadyError
		} else if result.MaterializationPreflight != nil && result.MaterializationPreflight.Ready {
			reason = "materialization preflight requires ready mutation binding, merge admission, rollback, reviewer advisory, operator enablement, and worktree identity evidence"
		}
		result.addGate("materialization_preflight_verified", false, reason)
	} else {
		result.addGate("materialization_preflight_verified", true, "")
	}
	result.addGate("live_mutation_verifier_enabled", input.LiveMutationVerifierEnabled, "live mutation verifier is disabled; mutation activation remains fail-closed")
	result.addGate("live_mutation_actuator_enabled", input.LiveMutationActuatorEnabled, "live mutation actuator is disabled; verifier readiness does not execute mutations")

	result.Status = MutationActivationStatusReady
	result.MutationAllowed = true
	for _, gate := range result.Gates {
		if gate.Passed {
			continue
		}
		result.Status = MutationActivationStatusBlocked
		result.MutationAllowed = false
		result.BlockingReasons = append(result.BlockingReasons, gate.Name+": "+gate.Reason)
	}
	result.Digest = digestMutationActivationGateResult(result)
	return result
}

func VerifyMutationActivationGateResult(result MutationActivationGateResult) error {
	if strings.TrimSpace(result.Schema) != MutationActivationGateSchemaVersion {
		return fmt.Errorf("mutation activation schema is unsupported")
	}
	if !isCanonicalSHA256Digest(result.Digest) {
		return fmt.Errorf("mutation activation digest is required")
	}
	expectedDigest := digestMutationActivationGateResult(result)
	if result.Digest != expectedDigest {
		return fmt.Errorf("mutation activation digest mismatch")
	}
	gates, err := verifyMutationActivationGateSet(result.Gates)
	if err != nil {
		return err
	}
	if err := verifyMutationActivationTopLevelGateInvariants(result, gates); err != nil {
		return err
	}
	if err := verifyMutationActivationDiagnosticMetadata(result); err != nil {
		return err
	}
	allPassed := true
	for _, gate := range result.Gates {
		if !gate.Passed {
			allPassed = false
		}
	}
	switch result.Status {
	case MutationActivationStatusBlocked:
		if result.MutationAllowed {
			return fmt.Errorf("blocked mutation activation must not allow mutation")
		}
		if allPassed {
			return fmt.Errorf("blocked mutation activation must have at least one failed gate")
		}
		if err := verifyMutationActivationBlockingReasons(result); err != nil {
			return err
		}
	case MutationActivationStatusReady:
		if !result.MutationAllowed {
			return fmt.Errorf("ready mutation activation must allow mutation")
		}
		if !allPassed {
			return fmt.Errorf("ready mutation activation must have all gates passed")
		}
		if len(result.BlockingReasons) > 0 {
			return fmt.Errorf("ready mutation activation must not include blocking reasons")
		}
		if result.AuthorityMode != MutationActivationAuthorityModeControlledQueue || result.ContextMode != MutationActivationAuthorityModeControlledQueue {
			return fmt.Errorf("ready mutation activation requires controlled authority and context modes")
		}
		if !result.LiveMutationVerifierEnabled {
			return fmt.Errorf("ready mutation activation requires live mutation verifier")
		}
		if !result.LiveMutationActuatorEnabled {
			return fmt.Errorf("ready mutation activation requires live mutation actuator")
		}
		if strings.TrimSpace(result.Source) != MutationActivationSourceDurableControlledQueueCandidate {
			return fmt.Errorf("ready mutation activation requires durable controlled queue candidate source")
		}
	default:
		return fmt.Errorf("unsupported mutation activation status %q", result.Status)
	}
	return nil
}

func verifyMutationActivationBlockingReasons(result MutationActivationGateResult) error {
	failed := make([]MutationActivationGate, 0)
	for _, gate := range result.Gates {
		if !gate.Passed {
			failed = append(failed, gate)
		}
	}
	if len(result.BlockingReasons) != len(failed) {
		return fmt.Errorf("blocked mutation activation blocking reasons must match failed gates")
	}
	for i, gate := range failed {
		prefix := gate.Name + ": "
		if !strings.HasPrefix(result.BlockingReasons[i], prefix) {
			return fmt.Errorf("blocked mutation activation reason %d must describe failed gate %q", i, gate.Name)
		}
	}
	return nil
}

func verifyMutationActivationGateSet(gates []MutationActivationGate) (map[string]MutationActivationGate, error) {
	if len(gates) != len(mutationActivationRequiredGateNames) {
		return nil, fmt.Errorf("mutation activation canonical gate set is required")
	}
	lookup := make(map[string]MutationActivationGate, len(gates))
	for _, gate := range gates {
		name := strings.TrimSpace(gate.Name)
		if name == "" {
			return nil, fmt.Errorf("mutation activation gate name is required")
		}
		if _, exists := lookup[name]; exists {
			return nil, fmt.Errorf("mutation activation gate %q is duplicated", name)
		}
		lookup[name] = gate
	}
	for _, name := range mutationActivationRequiredGateNames {
		if _, ok := lookup[name]; !ok {
			return nil, fmt.Errorf("mutation activation required gate %q is missing", name)
		}
	}
	return lookup, nil
}

func mutationActivationGateLookup(gates []MutationActivationGate, name string) MutationActivationGate {
	name = strings.TrimSpace(name)
	for _, gate := range gates {
		if strings.TrimSpace(gate.Name) == name {
			return gate
		}
	}
	return MutationActivationGate{}
}

func verifyMutationActivationTopLevelGateInvariants(result MutationActivationGateResult, gates map[string]MutationActivationGate) error {
	expected := map[string]bool{
		"controlled_authority_mode":          result.AuthorityMode == MutationActivationAuthorityModeControlledQueue,
		"controlled_context_mode":            result.ContextMode == MutationActivationAuthorityModeControlledQueue,
		"direct_merge_disabled":              result.DirectMergeDisabled,
		"durable_patch_queue":                result.QueueDurable,
		"canonical_worktree_identity":        mutationActivationResultWorktreeIdentityReady(result),
		"live_actuator_target_identity":      mutationActivationResultTargetWorktreeIdentityReady(result),
		"mutation_binding":                   mutationActivationResultMutationBindingReady(result),
		"merge_admission_conflict_safe":      mutationActivationResultMergeAdmissionReady(result),
		"bounded_retry":                      mutationActivationResultRetryBoundReady(result),
		"rollback_proven":                    mutationActivationResultRollbackProofReady(result),
		"reviewer_advisory_recorded":         mutationActivationResultReviewerAdvisoryReady(result),
		"reviewer_mesh_advisory_only":        result.ReviewerMeshMode == MutationActivationReviewerMeshAdvisoryOnly,
		"operator_enablement_recorded":       mutationActivationResultOperatorEnablementReady(result),
		"materialization_preflight_verified": mutationActivationResultMaterializationPreflightReady(result),
		"live_mutation_verifier_enabled":     result.LiveMutationVerifierEnabled,
		"live_mutation_actuator_enabled":     result.LiveMutationActuatorEnabled,
	}
	for name, want := range expected {
		if gates[name].Passed != want {
			return fmt.Errorf("mutation activation gate %q does not match top-level invariant", name)
		}
	}
	return nil
}

func mutationActivationResultWorktreeIdentityReady(result MutationActivationGateResult) bool {
	if result.WorktreeIdentity == nil {
		return false
	}
	return worktreeIdentityReady(*result.WorktreeIdentity)
}

func mutationActivationResultTargetWorktreeIdentityReady(result MutationActivationGateResult) bool {
	if result.WorktreeIdentity == nil || result.TargetWorktreeIdentity == nil {
		return false
	}
	return actuatorTargetWorktreeIdentityReady(*result.WorktreeIdentity, *result.TargetWorktreeIdentity)
}

func mutationActivationResultMutationBindingReady(result MutationActivationGateResult) bool {
	if result.MutationBindingEvidence == nil {
		return false
	}
	return mutationBindingEvidenceReady(*result.MutationBindingEvidence)
}

func mutationActivationResultMergeAdmissionReady(result MutationActivationGateResult) bool {
	if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil {
		return false
	}
	return mergeAdmissionEvidenceReady(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence)
}

func mutationActivationResultRetryBoundReady(result MutationActivationGateResult) bool {
	if result.MutationBindingEvidence == nil || result.RetryBoundEvidence == nil {
		return false
	}
	return retryBoundEvidenceReady(*result.MutationBindingEvidence, *result.RetryBoundEvidence)
}

func mutationActivationResultRollbackProofReady(result MutationActivationGateResult) bool {
	if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil {
		return false
	}
	return rollbackProofEvidenceReady(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence, *result.RollbackProofEvidence)
}

func mutationActivationResultReviewerAdvisoryReady(result MutationActivationGateResult) bool {
	if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil || result.ReviewerAdvisoryEvidence == nil {
		return false
	}
	return reviewerAdvisoryEvidenceReady(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence, *result.RollbackProofEvidence, *result.ReviewerAdvisoryEvidence)
}

func mutationActivationResultOperatorEnablementReady(result MutationActivationGateResult) bool {
	if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil || result.ReviewerAdvisoryEvidence == nil || result.OperatorEnablementEvidence == nil {
		return false
	}
	return operatorEnablementEvidenceReady(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence, *result.RollbackProofEvidence, *result.ReviewerAdvisoryEvidence, *result.OperatorEnablementEvidence)
}

func mutationActivationResultMaterializationPreflightReady(result MutationActivationGateResult) bool {
	if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil || result.ReviewerAdvisoryEvidence == nil || result.OperatorEnablementEvidence == nil || result.MaterializationPreflight == nil {
		return false
	}
	if result.WorktreeIdentity == nil {
		return false
	}
	return materializationPreflightEvidenceReady(
		*result.MutationBindingEvidence,
		*result.MergeAdmissionEvidence,
		*result.RollbackProofEvidence,
		*result.ReviewerAdvisoryEvidence,
		*result.OperatorEnablementEvidence,
		*result.WorktreeIdentity,
		*result.MaterializationPreflight,
	)
}

func verifyMutationActivationDiagnosticMetadata(result MutationActivationGateResult) error {
	source := strings.TrimSpace(result.Source)
	sourceError := strings.TrimSpace(result.SourceError)
	liveVerifierSource := strings.TrimSpace(result.LiveMutationVerifierSource)
	liveActuatorSource := strings.TrimSpace(result.LiveMutationActuatorSource)

	if result.LiveMutationVerifierEnabled && liveVerifierSource != MutationActivationLiveVerifierSourceEnv {
		return fmt.Errorf("live mutation verifier source must be %q when verifier is enabled", MutationActivationLiveVerifierSourceEnv)
	}
	if !result.LiveMutationVerifierEnabled && liveVerifierSource != "" {
		return fmt.Errorf("live mutation verifier source requires verifier to be enabled")
	}
	if result.LiveMutationVerifierEnabled && source != MutationActivationSourceDurableControlledQueueCandidate {
		return fmt.Errorf("live mutation verifier requires source %q", MutationActivationSourceDurableControlledQueueCandidate)
	}
	if result.LiveMutationActuatorEnabled && liveActuatorSource != MutationActivationLiveActuatorSourceRuntime {
		return fmt.Errorf("live mutation actuator source must be %q when actuator is enabled", MutationActivationLiveActuatorSourceRuntime)
	}
	if !result.LiveMutationActuatorEnabled && liveActuatorSource != "" {
		return fmt.Errorf("live mutation actuator source requires actuator to be enabled")
	}
	if result.LiveMutationActuatorEnabled && !result.LiveMutationVerifierEnabled {
		return fmt.Errorf("live mutation actuator requires live mutation verifier")
	}
	if result.LiveMutationActuatorEnabled && source != MutationActivationSourceDurableControlledQueueCandidate {
		return fmt.Errorf("live mutation actuator requires source %q", MutationActivationSourceDurableControlledQueueCandidate)
	}

	if result.Candidate != nil && source != MutationActivationSourceDurableControlledQueueCandidate {
		return fmt.Errorf("mutation activation candidate summary requires source %q", MutationActivationSourceDurableControlledQueueCandidate)
	}
	if result.TargetWorktreeIdentity != nil && source != MutationActivationSourceDurableControlledQueueCandidate {
		return fmt.Errorf("mutation activation target worktree identity requires source %q", MutationActivationSourceDurableControlledQueueCandidate)
	}
	if result.MutationBindingEvidence != nil {
		if err := verifyMutationBindingEvidence(*result.MutationBindingEvidence); err != nil {
			return err
		}
	}
	if result.MergeAdmissionEvidence != nil {
		if result.MutationBindingEvidence == nil {
			return fmt.Errorf("merge admission evidence requires mutation binding evidence")
		}
		if err := verifyMergeAdmissionEvidence(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence); err != nil {
			return err
		}
	}
	if result.RetryBoundEvidence != nil {
		if result.MutationBindingEvidence == nil {
			return fmt.Errorf("retry bound evidence requires mutation binding evidence")
		}
		if err := verifyRetryBoundEvidence(*result.MutationBindingEvidence, *result.RetryBoundEvidence); err != nil {
			return err
		}
	}
	if result.RollbackProofEvidence != nil {
		if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil {
			return fmt.Errorf("rollback proof evidence requires mutation binding and merge admission evidence")
		}
		if err := verifyRollbackProofEvidence(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence, *result.RollbackProofEvidence); err != nil {
			return err
		}
	}
	if result.ReviewerAdvisoryEvidence != nil {
		if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil {
			return fmt.Errorf("reviewer advisory evidence requires mutation binding, merge admission, and rollback proof evidence")
		}
		if err := verifyReviewerAdvisoryEvidence(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence, *result.RollbackProofEvidence, *result.ReviewerAdvisoryEvidence); err != nil {
			return err
		}
	}
	if result.OperatorEnablementEvidence != nil {
		if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil || result.ReviewerAdvisoryEvidence == nil {
			return fmt.Errorf("operator enablement evidence requires mutation binding, merge admission, rollback proof, and reviewer advisory evidence")
		}
		if err := verifyOperatorEnablementEvidence(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence, *result.RollbackProofEvidence, *result.ReviewerAdvisoryEvidence, *result.OperatorEnablementEvidence); err != nil {
			return err
		}
	}
	if result.MaterializationPreflight != nil {
		if result.MutationBindingEvidence == nil || result.MergeAdmissionEvidence == nil || result.RollbackProofEvidence == nil || result.ReviewerAdvisoryEvidence == nil || result.OperatorEnablementEvidence == nil || result.WorktreeIdentity == nil {
			if result.MaterializationPreflight.Ready {
				return fmt.Errorf("materialization preflight evidence requires mutation binding, merge admission, rollback proof, reviewer advisory, operator enablement, and worktree identity evidence")
			}
			if err := verifyMaterializationPreflightDiagnosticEvidence(*result.MaterializationPreflight); err != nil {
				return err
			}
		} else if err := verifyMaterializationPreflightEvidence(*result.MutationBindingEvidence, *result.MergeAdmissionEvidence, *result.RollbackProofEvidence, *result.ReviewerAdvisoryEvidence, *result.OperatorEnablementEvidence, *result.WorktreeIdentity, *result.MaterializationPreflight); err != nil {
			return err
		}
	}
	switch source {
	case "":
		if sourceError != "" {
			return fmt.Errorf("mutation activation source_error requires diagnostic source")
		}
	case MutationActivationSourceSyntheticPatchOnly, MutationActivationSourceDurableQueueNoControlledCandidate:
		if sourceError != "" {
			return fmt.Errorf("mutation activation source_error requires source %q", MutationActivationSourceDurableControlledQueueCandidateError)
		}
	case MutationActivationSourceDurableControlledQueueCandidate:
		if result.Candidate == nil {
			return fmt.Errorf("mutation activation source %q requires candidate summary", source)
		}
		if sourceError != "" {
			return fmt.Errorf("mutation activation source %q must not include source_error", source)
		}
		if result.AuthorityMode != MutationActivationAuthorityModeControlledQueue || result.ContextMode != MutationActivationAuthorityModeControlledQueue {
			return fmt.Errorf("mutation activation source %q requires controlled authority and context modes", source)
		}
		if err := verifyMutationActivationCandidateSummary(*result.Candidate); err != nil {
			return err
		}
		if result.WorktreeIdentity == nil {
			return fmt.Errorf("mutation activation source %q requires worktree identity evidence", source)
		}
		if err := verifyMutationActivationCandidateMatchesWorktree(*result.Candidate, *result.WorktreeIdentity); err != nil {
			return err
		}
		if result.TargetWorktreeIdentity != nil {
			if err := verifyMutationActivationCandidateMatchesTargetWorktree(*result.Candidate, *result.TargetWorktreeIdentity); err != nil {
				return err
			}
			if !actuatorTargetWorktreeIdentityReady(*result.WorktreeIdentity, *result.TargetWorktreeIdentity) && mutationActivationGateLookup(result.Gates, "live_actuator_target_identity").Passed {
				return fmt.Errorf("mutation activation target worktree identity gate is inconsistent")
			}
		}
		if result.MutationBindingEvidence == nil {
			return fmt.Errorf("mutation activation source %q requires mutation binding evidence", source)
		}
		if err := verifyMutationActivationCandidateMatchesMutationBinding(*result.Candidate, *result.MutationBindingEvidence); err != nil {
			return err
		}
	case MutationActivationSourceDurableControlledQueueCandidateError:
		if result.Candidate != nil {
			return fmt.Errorf("mutation activation read-failed source must not include candidate summary")
		}
		if sourceError == "" {
			return fmt.Errorf("mutation activation source %q requires source_error", source)
		}
	default:
		return fmt.Errorf("unsupported mutation activation source %q", source)
	}
	return nil
}

func verifyMutationActivationCandidateMatchesMutationBinding(candidate MutationActivationCandidateSummary, binding MutationBindingEvidence) error {
	expected := map[string][2]string{
		"workspace_id":        {candidate.WorkspaceID, binding.WorkspaceID},
		"patch_queue_id":      {candidate.QueueID, binding.PatchQueueID},
		"patch_queue_item_id": {candidate.ItemID, binding.PatchQueueItemID},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("mutation activation candidate %s does not match mutation binding evidence", field)
		}
	}
	return nil
}

func verifyMutationActivationCandidateMatchesWorktree(candidate MutationActivationCandidateSummary, worktree WorktreeIdentityEvidence) error {
	expected := map[string][2]string{
		"repo_id":     {candidate.RepoID, worktree.RepoID},
		"checkout_id": {candidate.CheckoutID, worktree.CheckoutID},
		"branch_id":   {candidate.BranchID, worktree.BranchID},
		"branch_name": {candidate.BranchName, worktree.BranchName},
		"base_sha":    {candidate.BaseSHA, worktree.BaseSHA},
		"head_sha":    {candidate.HeadSHA, worktree.HeadSHA},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("mutation activation candidate %s does not match worktree identity", field)
		}
	}
	return nil
}

func verifyMutationActivationCandidateMatchesTargetWorktree(candidate MutationActivationCandidateSummary, target WorktreeIdentityEvidence) error {
	if strings.TrimSpace(candidate.TargetCheckoutID) == "" {
		return fmt.Errorf("mutation activation candidate target_checkout_id is required with target worktree identity")
	}
	if strings.TrimSpace(candidate.TargetBranchName) == "" {
		return fmt.Errorf("mutation activation candidate target_branch_name is required with target worktree identity")
	}
	expected := map[string][2]string{
		"repo_id":              {candidate.RepoID, target.RepoID},
		"target_checkout_id":   {candidate.TargetCheckoutID, target.CheckoutID},
		"target_branch_name":   {candidate.TargetBranchName, target.BranchName},
		"target_base_sha":      {candidate.BaseSHA, target.BaseSHA},
		"target_expected_head": {candidate.BaseSHA, target.HeadSHA},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != "" && strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("mutation activation candidate %s does not match target worktree identity", field)
		}
	}
	return nil
}

func verifyMutationActivationCandidateSummary(candidate MutationActivationCandidateSummary) error {
	required := map[string]string{
		"workspace_id": candidate.WorkspaceID,
		"project_id":   candidate.ProjectID,
		"repo_id":      candidate.RepoID,
		"queue_id":     candidate.QueueID,
		"item_id":      candidate.ItemID,
		"branch_id":    candidate.BranchID,
		"branch_name":  candidate.BranchName,
		"checkout_id":  candidate.CheckoutID,
		"state":        candidate.State,
		"base_sha":     candidate.BaseSHA,
		"head_sha":     candidate.HeadSHA,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("mutation activation candidate %s is required", field)
		}
	}
	switch strings.ToUpper(strings.TrimSpace(candidate.State)) {
	case "PROPOSED", "CLAIMED":
	default:
		return fmt.Errorf("mutation activation candidate state %q is not live", candidate.State)
	}
	if !isCanonicalGitObjectID(candidate.BaseSHA) {
		return fmt.Errorf("mutation activation candidate base_sha must be canonical git object id")
	}
	if !isCanonicalGitObjectID(candidate.HeadSHA) {
		return fmt.Errorf("mutation activation candidate head_sha must be canonical git object id")
	}
	if strings.TrimSpace(candidate.TargetCheckoutID) != "" && strings.TrimSpace(candidate.TargetBranchName) == "" {
		return fmt.Errorf("mutation activation candidate target_branch_name is required with target_checkout_id")
	}
	return nil
}

func (r *MutationActivationGateResult) addGate(name string, passed bool, reason string) {
	gate := MutationActivationGate{Name: name, Passed: passed}
	if !passed {
		gate.Reason = strings.TrimSpace(reason)
	}
	r.Gates = append(r.Gates, gate)
}

func cloneMutationActivationCandidateSummary(candidate *MutationActivationCandidateSummary) *MutationActivationCandidateSummary {
	if candidate == nil {
		return nil
	}
	cloned := *candidate
	cloned.WorkspaceID = strings.TrimSpace(cloned.WorkspaceID)
	cloned.ProjectID = strings.TrimSpace(cloned.ProjectID)
	cloned.RepoID = strings.TrimSpace(cloned.RepoID)
	cloned.QueueID = strings.TrimSpace(cloned.QueueID)
	cloned.ItemID = strings.TrimSpace(cloned.ItemID)
	cloned.BranchID = strings.TrimSpace(cloned.BranchID)
	cloned.BranchName = strings.TrimSpace(cloned.BranchName)
	cloned.CheckoutID = strings.TrimSpace(cloned.CheckoutID)
	cloned.TargetCheckoutID = strings.TrimSpace(cloned.TargetCheckoutID)
	cloned.TargetBranchName = strings.TrimSpace(cloned.TargetBranchName)
	cloned.State = strings.TrimSpace(cloned.State)
	cloned.BaseSHA = strings.TrimSpace(cloned.BaseSHA)
	cloned.HeadSHA = strings.TrimSpace(cloned.HeadSHA)
	return &cloned
}

func cloneWorktreeIdentityEvidence(evidence WorktreeIdentityEvidence) *WorktreeIdentityEvidence {
	cloned := WorktreeIdentityEvidence{
		RepoID:               strings.TrimSpace(evidence.RepoID),
		CheckoutID:           strings.TrimSpace(evidence.CheckoutID),
		BranchID:             strings.TrimSpace(evidence.BranchID),
		BranchName:           strings.TrimSpace(evidence.BranchName),
		MachineID:            strings.TrimSpace(evidence.MachineID),
		LocalPath:            strings.TrimSpace(evidence.LocalPath),
		BaseSHA:              strings.TrimSpace(evidence.BaseSHA),
		HeadSHA:              strings.TrimSpace(evidence.HeadSHA),
		ReadbackState:        strings.TrimSpace(evidence.ReadbackState),
		ReadbackError:        strings.TrimSpace(evidence.ReadbackError),
		ObservedWorktreeRoot: strings.TrimSpace(evidence.ObservedWorktreeRoot),
		ObservedBranchName:   strings.TrimSpace(evidence.ObservedBranchName),
		ObservedHeadSHA:      strings.TrimSpace(evidence.ObservedHeadSHA),
		ObservedDirtyState:   strings.TrimSpace(evidence.ObservedDirtyState),
	}
	for _, value := range []string{
		cloned.RepoID,
		cloned.CheckoutID,
		cloned.BranchID,
		cloned.BranchName,
		cloned.MachineID,
		cloned.LocalPath,
		cloned.BaseSHA,
		cloned.HeadSHA,
		cloned.ReadbackState,
		cloned.ReadbackError,
		cloned.ObservedWorktreeRoot,
		cloned.ObservedBranchName,
		cloned.ObservedHeadSHA,
		cloned.ObservedDirtyState,
	} {
		if value != "" {
			return &cloned
		}
	}
	return nil
}

func clonePatchQueueRollbackEvidence(evidence PatchQueueRollback) PatchQueueRollback {
	cloned := PatchQueueRollback{
		Schema:                     strings.TrimSpace(evidence.Schema),
		SourceOperationID:          strings.TrimSpace(evidence.SourceOperationID),
		SourceOperationKind:        strings.TrimSpace(evidence.SourceOperationKind),
		RollbackOperationID:        strings.TrimSpace(evidence.RollbackOperationID),
		RollbackOperationKind:      strings.TrimSpace(evidence.RollbackOperationKind),
		Reason:                     strings.TrimSpace(evidence.Reason),
		SourcePatchDigest:          strings.TrimSpace(evidence.SourcePatchDigest),
		RollbackPatchDigest:        strings.TrimSpace(evidence.RollbackPatchDigest),
		VerificationCommand:        strings.TrimSpace(evidence.VerificationCommand),
		VerificationStatus:         strings.TrimSpace(evidence.VerificationStatus),
		VerificationExitCode:       evidence.VerificationExitCode,
		VerificationOutputDigest:   strings.TrimSpace(evidence.VerificationOutputDigest),
		VerificationOutputSummary:  strings.TrimSpace(evidence.VerificationOutputSummary),
		VerificationDurationMillis: evidence.VerificationDurationMillis,
		RecordedAt:                 strings.TrimSpace(evidence.RecordedAt),
	}
	if len(evidence.RollbackPaths) > 0 {
		cloned.RollbackPaths = make([]PatchQueueRollbackPath, 0, len(evidence.RollbackPaths))
		for _, path := range evidence.RollbackPaths {
			cloned.RollbackPaths = append(cloned.RollbackPaths, PatchQueueRollbackPath{
				Path:                  strings.TrimSpace(path.Path),
				SourceBaseHash:        strings.TrimSpace(path.SourceBaseHash),
				SourceAppliedHash:     strings.TrimSpace(path.SourceAppliedHash),
				RollbackCandidateHash: strings.TrimSpace(path.RollbackCandidateHash),
			})
		}
	}
	return cloned
}

func clonePatchQueueReviewerAdvisory(evidence PatchQueueReviewerAdvisory) PatchQueueReviewerAdvisory {
	return PatchQueueReviewerAdvisory{
		Schema:                 strings.TrimSpace(evidence.Schema),
		Mode:                   strings.TrimSpace(evidence.Mode),
		Verdict:                strings.TrimSpace(evidence.Verdict),
		ReviewerID:             strings.TrimSpace(evidence.ReviewerID),
		ReviewDocKey:           strings.TrimSpace(evidence.ReviewDocKey),
		OperationID:            strings.TrimSpace(evidence.OperationID),
		OperationKind:          strings.TrimSpace(evidence.OperationKind),
		CASPatchDigest:         strings.TrimSpace(evidence.CASPatchDigest),
		CASEvaluationDigest:    strings.TrimSpace(evidence.CASEvaluationDigest),
		RollbackEvidenceDigest: strings.TrimSpace(evidence.RollbackEvidenceDigest),
		Summary:                strings.TrimSpace(evidence.Summary),
		RecordedAt:             strings.TrimSpace(evidence.RecordedAt),
	}
}

func clonePatchQueueOperatorEnablement(evidence PatchQueueOperatorEnablement) PatchQueueOperatorEnablement {
	return PatchQueueOperatorEnablement{
		Schema:                 strings.TrimSpace(evidence.Schema),
		Scope:                  strings.TrimSpace(evidence.Scope),
		Enabled:                evidence.Enabled,
		EnabledBy:              strings.TrimSpace(evidence.EnabledBy),
		EnabledAt:              strings.TrimSpace(evidence.EnabledAt),
		Reason:                 strings.TrimSpace(evidence.Reason),
		WorkspaceID:            strings.TrimSpace(evidence.WorkspaceID),
		ProjectID:              strings.TrimSpace(evidence.ProjectID),
		QueueID:                strings.TrimSpace(evidence.QueueID),
		ItemID:                 strings.TrimSpace(evidence.ItemID),
		OperationID:            strings.TrimSpace(evidence.OperationID),
		CASPatchDigest:         strings.TrimSpace(evidence.CASPatchDigest),
		RollbackEvidenceDigest: strings.TrimSpace(evidence.RollbackEvidenceDigest),
		ReviewerAdvisoryDigest: strings.TrimSpace(evidence.ReviewerAdvisoryDigest),
	}
}

func buildMutationBindingEvidence(authority Context, item PatchQueueItem, force bool) *MutationBindingEvidence {
	authority = authority.WithDefaults()
	if !force && !mutationBindingInputHasAnyEvidence(authority, item) {
		return nil
	}

	pathset := append([]string(nil), authority.Pathset...)
	if normalized, err := NormalizePathSet(pathset); err == nil {
		pathset = normalized
	}
	itemPathset := append([]string(nil), item.Pathset...)
	if normalized, err := NormalizePathSet(itemPathset); err == nil {
		itemPathset = normalized
	}
	baseFileHashPaths := make([]string, 0, len(authority.Base.FileHashes))
	baseFileHashes := make(map[string]string, len(authority.Base.FileHashes))
	for rawPath, rawHash := range authority.Base.FileHashes {
		path := strings.TrimSpace(rawPath)
		baseFileHashPaths = append(baseFileHashPaths, path)
		baseFileHashes[path] = strings.TrimSpace(rawHash)
	}
	if normalized, err := NormalizePathSet(baseFileHashPaths); err == nil {
		baseFileHashPaths = normalized
	}
	if len(baseFileHashes) == 0 {
		baseFileHashes = nil
	}

	evidence := MutationBindingEvidence{
		ContextMode:                 strings.TrimSpace(authority.Mode),
		WorkspaceID:                 strings.TrimSpace(authority.WorkspaceID),
		TaskID:                      strings.TrimSpace(authority.TaskID),
		SessionID:                   strings.TrimSpace(authority.SessionID),
		RunID:                       strings.TrimSpace(authority.RunID),
		AgentID:                     strings.TrimSpace(authority.AgentID),
		PrincipalType:               strings.TrimSpace(authority.Principal.Type),
		PrincipalID:                 strings.TrimSpace(authority.Principal.ID),
		CapabilitySnapshotID:        strings.TrimSpace(authority.CapabilitySnapshot.ID),
		CapabilitySnapshotSchema:    strings.TrimSpace(authority.CapabilitySnapshot.Schema),
		RepoRoot:                    strings.TrimSpace(authority.RepoRoot),
		BaseRef:                     strings.TrimSpace(authority.Base.Ref),
		BaseTreeHash:                strings.TrimSpace(authority.Base.TreeHash),
		BaseFileHashCount:           len(authority.Base.FileHashes),
		BaseFileHashPaths:           baseFileHashPaths,
		BaseFileHashes:              baseFileHashes,
		RepoLeaseID:                 strings.TrimSpace(authority.Lease.ID),
		LeaseTerm:                   authority.Lease.Term,
		PatchQueueID:                strings.TrimSpace(authority.PatchQueue.QueueID),
		PatchQueueItemID:            strings.TrimSpace(authority.PatchQueue.ItemID),
		OperationID:                 strings.TrimSpace(authority.Operation.ID),
		OperationKind:               strings.TrimSpace(authority.Operation.Kind),
		PatchQueueItemSchema:        strings.TrimSpace(item.Schema),
		PatchQueueItemRecordID:      strings.TrimSpace(item.ID),
		PatchQueueItemQueueID:       strings.TrimSpace(item.QueueID),
		PatchQueueItemItemID:        strings.TrimSpace(item.ItemID),
		PatchQueueItemState:         strings.TrimSpace(item.State),
		PatchQueueItemContext:       strings.TrimSpace(item.ContextDigest),
		PatchQueueItemRepoLeaseID:   strings.TrimSpace(item.RepoLeaseID),
		PatchQueueItemLeaseTerm:     item.LeaseTerm,
		PatchQueueItemOperationID:   strings.TrimSpace(item.OperationID),
		PatchQueueItemOperationKind: strings.TrimSpace(item.OperationKind),
		Pathset:                     pathset,
		PatchQueueItemPathset:       itemPathset,
	}
	_, evidence.OperationKindAccepted = allowedMutationOperationKinds[evidence.OperationKind]

	if digest, err := authority.Digest(); err != nil {
		evidence.ContextDigestError = err.Error()
	} else {
		evidence.ContextDigest = digest
	}
	if digest, err := patchQueueContextDigest(authority); err != nil {
		evidence.PatchQueueContextError = err.Error()
	} else {
		evidence.PatchQueueContextDigest = digest
	}
	evidence.MissingRefs = mutationBindingEvidenceMissingRefs(evidence)
	evidence.Mismatches = mutationBindingEvidenceMismatches(evidence)

	if err := mutationBindingReady(authority, item); err != nil {
		evidence.Ready = false
		evidence.ReadyError = err.Error()
		switch {
		case len(evidence.MissingRefs) > 0:
			evidence.State = "missing_refs"
		case len(evidence.Mismatches) > 0:
			evidence.State = "mismatch"
		default:
			evidence.State = "invalid"
		}
		return &evidence
	}
	evidence.Ready = true
	evidence.State = "ready"
	return &evidence
}

func buildMergeAdmissionEvidence(authority Context, item PatchQueueItem, force bool) *MergeAdmissionEvidence {
	authority = authority.WithDefaults()
	if !force && !mergeAdmissionInputHasAnyEvidence(item) {
		return nil
	}
	itemPathset := append([]string(nil), item.Pathset...)
	if normalized, err := NormalizePathSet(itemPathset); err == nil {
		itemPathset = normalized
	}
	evidence := MergeAdmissionEvidence{
		PatchQueueItemSchema:        strings.TrimSpace(item.Schema),
		PatchQueueItemRecordID:      strings.TrimSpace(item.ID),
		PatchQueueItemQueueID:       strings.TrimSpace(item.QueueID),
		PatchQueueItemItemID:        strings.TrimSpace(item.ItemID),
		PatchQueueItemState:         strings.TrimSpace(item.State),
		PatchQueueItemContext:       strings.TrimSpace(item.ContextDigest),
		PatchQueueItemRepoLeaseID:   strings.TrimSpace(item.RepoLeaseID),
		PatchQueueItemLeaseTerm:     item.LeaseTerm,
		PatchQueueItemOperationID:   strings.TrimSpace(item.OperationID),
		PatchQueueItemOperationKind: strings.TrimSpace(item.OperationKind),
		PatchQueueItemPathset:       itemPathset,
		CASStatus:                   strings.TrimSpace(item.CASResult.Status),
		CASPatchDigest:              strings.TrimSpace(item.CASPatchDigest),
		CASEvaluationDigest:         strings.TrimSpace(item.CASEvaluationDigest),
		CASResult:                   cloneCASPatchApplyResult(item.CASResult),
		TestEvidence:                item.TestEvidence,
		TestEvidenceDigest:          strings.TrimSpace(item.TestEvidenceDigest),
	}
	if err := mergeAdmissionReady(authority, item); err != nil {
		evidence.Ready = false
		evidence.ReadyError = err.Error()
		evidence.State = "invalid"
		return &evidence
	}
	evidence.Ready = true
	evidence.State = "ready"
	return &evidence
}

func mergeAdmissionInputHasAnyEvidence(item PatchQueueItem) bool {
	if strings.TrimSpace(item.State) == PatchQueueStateApplied {
		return true
	}
	if len(item.CASResult.Paths) > 0 || len(item.CASResult.Issues) > 0 {
		return true
	}
	for _, value := range []string{
		item.CASResult.Schema,
		item.CASResult.Status,
		item.CASResult.PatchID,
		item.CASResult.PatchDigest,
		item.CASResult.ContextDigest,
		item.CASPatchDigest,
		item.CASEvaluationDigest,
		item.TestEvidence.Schema,
		item.TestEvidence.Name,
		item.TestEvidence.Command,
		item.TestEvidence.Status,
		item.TestEvidence.OutputDigest,
		item.TestEvidence.OutputSummary,
		item.TestEvidenceDigest,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return item.TestEvidence.ExitCode != 0 || item.TestEvidence.DurationMillis != 0
}

func buildRetryBoundEvidence(item PatchQueueItem, force bool) *RetryBoundEvidence {
	if !force && !retryBoundInputHasAnyEvidence(item) {
		return nil
	}
	evidence := RetryBoundEvidence{
		PatchQueueItemSchema:        strings.TrimSpace(item.Schema),
		PatchQueueItemRecordID:      strings.TrimSpace(item.ID),
		PatchQueueItemQueueID:       strings.TrimSpace(item.QueueID),
		PatchQueueItemItemID:        strings.TrimSpace(item.ItemID),
		PatchQueueItemState:         strings.TrimSpace(item.State),
		PatchQueueItemContext:       strings.TrimSpace(item.ContextDigest),
		PatchQueueItemRepoLeaseID:   strings.TrimSpace(item.RepoLeaseID),
		PatchQueueItemLeaseTerm:     item.LeaseTerm,
		PatchQueueItemOperationID:   strings.TrimSpace(item.OperationID),
		PatchQueueItemOperationKind: strings.TrimSpace(item.OperationKind),
		Attempt:                     item.Attempt,
		MaxAttempts:                 item.MaxAttempts,
		NextRetryAt:                 strings.TrimSpace(item.NextRetryAt),
		DeadLetteredAt:              strings.TrimSpace(item.DeadLetteredAt),
	}
	if err := boundedRetryReady(item); err != nil {
		evidence.Ready = false
		evidence.ReadyError = err.Error()
		evidence.State = "invalid"
		return &evidence
	}
	evidence.Ready = true
	evidence.State = "ready"
	return &evidence
}

func retryBoundInputHasAnyEvidence(item PatchQueueItem) bool {
	return item.Attempt != 0 ||
		item.MaxAttempts != 0 ||
		strings.TrimSpace(item.NextRetryAt) != "" ||
		strings.TrimSpace(item.DeadLetteredAt) != ""
}

func buildRollbackProofEvidence(item PatchQueueItem, rollback PatchQueueRollback, force bool) *RollbackProofEvidence {
	if !force && !rollbackProofInputHasAnyEvidence(item, rollback) {
		return nil
	}
	evidence := RollbackProofEvidence{
		PatchQueueItemSchema:        strings.TrimSpace(item.Schema),
		PatchQueueItemRecordID:      strings.TrimSpace(item.ID),
		PatchQueueItemQueueID:       strings.TrimSpace(item.QueueID),
		PatchQueueItemItemID:        strings.TrimSpace(item.ItemID),
		PatchQueueItemState:         strings.TrimSpace(item.State),
		PatchQueueItemContext:       strings.TrimSpace(item.ContextDigest),
		PatchQueueItemRepoLeaseID:   strings.TrimSpace(item.RepoLeaseID),
		PatchQueueItemLeaseTerm:     item.LeaseTerm,
		PatchQueueItemOperationID:   strings.TrimSpace(item.OperationID),
		PatchQueueItemOperationKind: strings.TrimSpace(item.OperationKind),
		CASPatchDigest:              strings.TrimSpace(item.CASPatchDigest),
		CASEvaluationDigest:         strings.TrimSpace(item.CASEvaluationDigest),
		RollbackEvidence:            clonePatchQueueRollbackEvidence(rollback),
		RollbackEvidenceDigest:      strings.TrimSpace(item.RollbackEvidenceDigest),
	}
	if evidence.RollbackEvidenceDigest == "" && rollbackEvidenceInputHasAnyEvidence(evidence.RollbackEvidence) {
		evidence.RollbackEvidenceDigest = digestPatchQueueRollbackEvidence(evidence.RollbackEvidence)
	}
	if err := rollbackEvidenceReady(evidence.RollbackEvidence, item); err != nil {
		evidence.Ready = false
		evidence.ReadyError = err.Error()
		evidence.State = "invalid"
		return &evidence
	}
	evidence.Ready = true
	evidence.State = "ready"
	return &evidence
}

func rollbackProofInputHasAnyEvidence(item PatchQueueItem, rollback PatchQueueRollback) bool {
	if strings.TrimSpace(item.RollbackEvidenceDigest) != "" ||
		rollbackEvidenceInputHasAnyEvidence(item.RollbackEvidence) ||
		rollbackEvidenceInputHasAnyEvidence(rollback) {
		return true
	}
	return false
}

func rollbackEvidenceInputHasAnyEvidence(evidence PatchQueueRollback) bool {
	if len(evidence.RollbackPaths) > 0 ||
		evidence.VerificationExitCode != 0 ||
		evidence.VerificationDurationMillis != 0 {
		return true
	}
	for _, value := range []string{
		evidence.Schema,
		evidence.SourceOperationID,
		evidence.SourceOperationKind,
		evidence.RollbackOperationID,
		evidence.RollbackOperationKind,
		evidence.Reason,
		evidence.SourcePatchDigest,
		evidence.RollbackPatchDigest,
		evidence.VerificationCommand,
		evidence.VerificationStatus,
		evidence.VerificationOutputDigest,
		evidence.VerificationOutputSummary,
		evidence.RecordedAt,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func buildReviewerAdvisoryEvidence(item PatchQueueItem, advisory PatchQueueReviewerAdvisory, force bool) *ReviewerAdvisoryEvidence {
	if !force && !reviewerAdvisoryInputHasAnyEvidence(item, advisory) {
		return nil
	}
	evidence := ReviewerAdvisoryEvidence{
		PatchQueueItemSchema:        strings.TrimSpace(item.Schema),
		PatchQueueItemRecordID:      strings.TrimSpace(item.ID),
		PatchQueueItemQueueID:       strings.TrimSpace(item.QueueID),
		PatchQueueItemItemID:        strings.TrimSpace(item.ItemID),
		PatchQueueItemState:         strings.TrimSpace(item.State),
		PatchQueueItemContext:       strings.TrimSpace(item.ContextDigest),
		PatchQueueItemRepoLeaseID:   strings.TrimSpace(item.RepoLeaseID),
		PatchQueueItemLeaseTerm:     item.LeaseTerm,
		PatchQueueItemOperationID:   strings.TrimSpace(item.OperationID),
		PatchQueueItemOperationKind: strings.TrimSpace(item.OperationKind),
		CASPatchDigest:              strings.TrimSpace(item.CASPatchDigest),
		CASEvaluationDigest:         strings.TrimSpace(item.CASEvaluationDigest),
		RollbackEvidenceDigest:      strings.TrimSpace(item.RollbackEvidenceDigest),
		WorkspaceID:                 strings.TrimSpace(item.WorkspaceID),
		ProjectID:                   strings.TrimSpace(item.ProjectID),
		Advisory:                    clonePatchQueueReviewerAdvisory(advisory),
		AdvisoryDigest:              strings.TrimSpace(item.ReviewerAdvisoryDigest),
	}
	if evidence.AdvisoryDigest == "" && reviewerAdvisoryRawHasAnyEvidence(evidence.Advisory) {
		evidence.AdvisoryDigest = digestPatchQueueReviewerAdvisory(evidence.Advisory)
	}
	if err := reviewerAdvisoryReady(evidence.Advisory, item); err != nil {
		evidence.Ready = false
		evidence.ReadyError = err.Error()
		evidence.State = "invalid"
		return &evidence
	}
	evidence.Ready = true
	evidence.State = "ready"
	return &evidence
}

func reviewerAdvisoryInputHasAnyEvidence(item PatchQueueItem, advisory PatchQueueReviewerAdvisory) bool {
	return strings.TrimSpace(item.ReviewerAdvisoryDigest) != "" ||
		reviewerAdvisoryRawHasAnyEvidence(item.ReviewerAdvisory) ||
		reviewerAdvisoryRawHasAnyEvidence(advisory)
}

func reviewerAdvisoryRawHasAnyEvidence(evidence PatchQueueReviewerAdvisory) bool {
	for _, value := range []string{
		evidence.Schema,
		evidence.Mode,
		evidence.Verdict,
		evidence.ReviewerID,
		evidence.ReviewDocKey,
		evidence.OperationID,
		evidence.OperationKind,
		evidence.CASPatchDigest,
		evidence.CASEvaluationDigest,
		evidence.RollbackEvidenceDigest,
		evidence.Summary,
		evidence.RecordedAt,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func buildOperatorEnablementEvidence(item PatchQueueItem, enablement PatchQueueOperatorEnablement, advisory PatchQueueReviewerAdvisory, force bool) *OperatorEnablementEvidence {
	if !force && !operatorEnablementInputHasAnyEvidence(item, enablement) {
		return nil
	}
	evidence := OperatorEnablementEvidence{
		PatchQueueItemSchema:        strings.TrimSpace(item.Schema),
		PatchQueueItemRecordID:      strings.TrimSpace(item.ID),
		PatchQueueItemQueueID:       strings.TrimSpace(item.QueueID),
		PatchQueueItemItemID:        strings.TrimSpace(item.ItemID),
		PatchQueueItemState:         strings.TrimSpace(item.State),
		PatchQueueItemContext:       strings.TrimSpace(item.ContextDigest),
		PatchQueueItemRepoLeaseID:   strings.TrimSpace(item.RepoLeaseID),
		PatchQueueItemLeaseTerm:     item.LeaseTerm,
		PatchQueueItemOperationID:   strings.TrimSpace(item.OperationID),
		PatchQueueItemOperationKind: strings.TrimSpace(item.OperationKind),
		CASPatchDigest:              strings.TrimSpace(item.CASPatchDigest),
		RollbackEvidenceDigest:      strings.TrimSpace(item.RollbackEvidenceDigest),
		ReviewerAdvisoryDigest:      strings.TrimSpace(item.ReviewerAdvisoryDigest),
		WorkspaceID:                 strings.TrimSpace(item.WorkspaceID),
		ProjectID:                   strings.TrimSpace(item.ProjectID),
		Enablement:                  clonePatchQueueOperatorEnablement(enablement),
		EnablementDigest:            strings.TrimSpace(item.OperatorEnablementDigest),
	}
	if evidence.EnablementDigest == "" && operatorEnablementRawHasAnyEvidence(evidence.Enablement) {
		evidence.EnablementDigest = digestPatchQueueOperatorEnablement(evidence.Enablement)
	}
	if evidence.ReviewerAdvisoryDigest == "" && reviewerAdvisoryRawHasAnyEvidence(advisory) {
		evidence.ReviewerAdvisoryDigest = digestPatchQueueReviewerAdvisory(advisory)
	}
	if err := operatorEnablementReady(evidence.Enablement, item, advisory); err != nil {
		evidence.Ready = false
		evidence.ReadyError = err.Error()
		evidence.State = "invalid"
		return &evidence
	}
	evidence.Ready = true
	evidence.State = "ready"
	return &evidence
}

func operatorEnablementInputHasAnyEvidence(item PatchQueueItem, enablement PatchQueueOperatorEnablement) bool {
	return strings.TrimSpace(item.OperatorEnablementDigest) != "" ||
		operatorEnablementRawHasAnyEvidence(item.OperatorEnablement) ||
		operatorEnablementRawHasAnyEvidence(enablement)
}

func operatorEnablementRawHasAnyEvidence(evidence PatchQueueOperatorEnablement) bool {
	if evidence.Enabled {
		return true
	}
	for _, value := range []string{
		evidence.Schema,
		evidence.Scope,
		evidence.EnabledBy,
		evidence.EnabledAt,
		evidence.Reason,
		evidence.WorkspaceID,
		evidence.ProjectID,
		evidence.QueueID,
		evidence.ItemID,
		evidence.OperationID,
		evidence.CASPatchDigest,
		evidence.RollbackEvidenceDigest,
		evidence.ReviewerAdvisoryDigest,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func mutationBindingInputHasAnyEvidence(authority Context, item PatchQueueItem) bool {
	for _, value := range []string{
		authority.WorkspaceID,
		authority.TaskID,
		authority.SessionID,
		authority.RunID,
		authority.AgentID,
		authority.Principal.Type,
		authority.Principal.ID,
		authority.CapabilitySnapshot.ID,
		authority.RepoRoot,
		authority.Base.Ref,
		authority.Base.TreeHash,
		authority.Lease.ID,
		authority.PatchQueue.QueueID,
		authority.PatchQueue.ItemID,
		authority.Operation.ID,
		authority.Operation.Kind,
		item.Schema,
		item.ID,
		item.QueueID,
		item.ItemID,
		item.ContextDigest,
		item.RepoLeaseID,
		item.OperationID,
		item.OperationKind,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return authority.Lease.Term != 0 ||
		item.LeaseTerm != 0 ||
		len(authority.Pathset) > 0 ||
		len(authority.Base.FileHashes) > 0 ||
		len(item.Pathset) > 0
}

func mutationBindingEvidenceMissingRefs(evidence MutationBindingEvidence) []string {
	missing := make([]string, 0, 24)
	appendMissing := func(field string) {
		missing = append(missing, field)
	}
	appendIfEmpty := func(field, value string) {
		if strings.TrimSpace(value) == "" {
			appendMissing(field)
		}
	}

	appendIfEmpty("mode", evidence.ContextMode)
	appendIfEmpty("workspace_id", evidence.WorkspaceID)
	appendIfEmpty("task_id", evidence.TaskID)
	appendIfEmpty("session_id", evidence.SessionID)
	appendIfEmpty("run_id", evidence.RunID)
	appendIfEmpty("agent_id", evidence.AgentID)
	appendIfEmpty("principal.type", evidence.PrincipalType)
	appendIfEmpty("principal.id", evidence.PrincipalID)
	appendIfEmpty("capability_snapshot.id", evidence.CapabilitySnapshotID)
	appendIfEmpty("repo_root", evidence.RepoRoot)
	if strings.TrimSpace(evidence.BaseRef) == "" && strings.TrimSpace(evidence.BaseTreeHash) == "" {
		appendMissing("base.ref_or_tree_hash")
	}
	if len(evidence.Pathset) == 0 {
		appendMissing("pathset")
	}
	if err := rejectVagueBindingLabel("repo_lease_id", evidence.RepoLeaseID); err != nil {
		appendMissing("repo_lease_id")
	}
	if evidence.LeaseTerm <= 0 {
		appendMissing("lease_term")
	}
	if err := rejectVagueBindingLabel("patch_queue_id", evidence.PatchQueueID); err != nil {
		appendMissing("patch_queue_id")
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", evidence.PatchQueueItemID); err != nil {
		appendMissing("patch_queue_item_id")
	}
	if err := rejectVagueBindingLabel("operation_id", evidence.OperationID); err != nil {
		appendMissing("operation_id")
	}
	if err := rejectVagueBindingLabel("operation_kind", evidence.OperationKind); err != nil {
		appendMissing("operation_kind")
	} else if _, ok := allowedMutationOperationKinds[strings.TrimSpace(evidence.OperationKind)]; !ok {
		appendMissing("operation_kind")
	}

	appendIfEmpty("patch_queue_item.schema", evidence.PatchQueueItemSchema)
	appendIfEmpty("patch_queue_item.queue_id", evidence.PatchQueueItemQueueID)
	appendIfEmpty("patch_queue_item.item_id", evidence.PatchQueueItemItemID)
	appendIfEmpty("patch_queue_item.context_digest", evidence.PatchQueueItemContext)
	appendIfEmpty("patch_queue_item.repo_lease_id", evidence.PatchQueueItemRepoLeaseID)
	if evidence.PatchQueueItemLeaseTerm <= 0 {
		appendMissing("patch_queue_item.lease_term")
	}
	appendIfEmpty("patch_queue_item.operation_id", evidence.PatchQueueItemOperationID)
	if err := rejectVagueBindingLabel("patch_queue_item.operation_kind", evidence.PatchQueueItemOperationKind); err != nil {
		appendMissing("patch_queue_item.operation_kind")
	} else if _, ok := allowedMutationOperationKinds[strings.TrimSpace(evidence.PatchQueueItemOperationKind)]; !ok {
		appendMissing("patch_queue_item.operation_kind")
	}
	if len(evidence.PatchQueueItemPathset) == 0 {
		appendMissing("patch_queue_item.pathset")
	}
	return missing
}

func mutationBindingEvidenceMismatches(evidence MutationBindingEvidence) []string {
	mismatches := make([]string, 0, 8)
	appendMismatch := func(field string) {
		mismatches = append(mismatches, field)
	}
	compareString := func(field, left, right string) {
		left = strings.TrimSpace(left)
		right = strings.TrimSpace(right)
		if left != "" && right != "" && left != right {
			appendMismatch(field)
		}
	}
	compareString("patch_queue_id", evidence.PatchQueueID, evidence.PatchQueueItemQueueID)
	compareString("patch_queue_item_id", evidence.PatchQueueItemID, evidence.PatchQueueItemItemID)
	compareString("repo_lease_id", evidence.RepoLeaseID, evidence.PatchQueueItemRepoLeaseID)
	if evidence.LeaseTerm > 0 && evidence.PatchQueueItemLeaseTerm > 0 && evidence.LeaseTerm != evidence.PatchQueueItemLeaseTerm {
		appendMismatch("lease_term")
	}
	compareString("patch_queue_context_digest", evidence.PatchQueueContextDigest, evidence.PatchQueueItemContext)
	if len(evidence.Pathset) > 0 && len(evidence.PatchQueueItemPathset) > 0 && !sameStringSlice(evidence.Pathset, evidence.PatchQueueItemPathset) {
		appendMismatch("pathset")
	}
	compareString("operation_id", evidence.OperationID, evidence.PatchQueueItemOperationID)
	compareString("operation_kind", evidence.OperationKind, evidence.PatchQueueItemOperationKind)
	return mismatches
}

func stringSliceContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func verifyMutationBindingEvidenceDigests(evidence MutationBindingEvidence) error {
	authority := mutationBindingEvidenceContext(evidence)
	contextDigest, contextDigestError := mutationBindingEvidenceDigestResult(authority.Digest())
	if strings.TrimSpace(evidence.ContextDigest) != contextDigest ||
		strings.TrimSpace(evidence.ContextDigestError) != contextDigestError {
		return fmt.Errorf("mutation binding evidence context digest is inconsistent")
	}
	patchQueueDigest, patchQueueDigestError := mutationBindingEvidenceDigestResult(patchQueueContextDigest(authority))
	if strings.TrimSpace(evidence.PatchQueueContextDigest) != patchQueueDigest ||
		strings.TrimSpace(evidence.PatchQueueContextError) != patchQueueDigestError {
		return fmt.Errorf("mutation binding evidence patch queue context digest is inconsistent")
	}
	return nil
}

func mutationBindingEvidenceDigestResult(digest string, err error) (string, string) {
	if err != nil {
		return "", err.Error()
	}
	return strings.TrimSpace(digest), ""
}

func mutationBindingEvidenceContext(evidence MutationBindingEvidence) Context {
	fileHashes := make(map[string]string, len(evidence.BaseFileHashes))
	for rawPath, rawHash := range evidence.BaseFileHashes {
		path := strings.TrimSpace(rawPath)
		if path == "" {
			continue
		}
		fileHashes[path] = strings.TrimSpace(rawHash)
	}
	if len(fileHashes) == 0 {
		fileHashes = nil
	}
	return Context{
		Mode:        strings.TrimSpace(evidence.ContextMode),
		WorkspaceID: strings.TrimSpace(evidence.WorkspaceID),
		TaskID:      strings.TrimSpace(evidence.TaskID),
		SessionID:   strings.TrimSpace(evidence.SessionID),
		RunID:       strings.TrimSpace(evidence.RunID),
		AgentID:     strings.TrimSpace(evidence.AgentID),
		Principal: PrincipalRef{
			Type: strings.TrimSpace(evidence.PrincipalType),
			ID:   strings.TrimSpace(evidence.PrincipalID),
		},
		CapabilitySnapshot: CapabilitySnapshotRef{
			ID:     strings.TrimSpace(evidence.CapabilitySnapshotID),
			Schema: strings.TrimSpace(evidence.CapabilitySnapshotSchema),
		},
		RepoRoot: strings.TrimSpace(evidence.RepoRoot),
		Base: BaseIdentity{
			Ref:        strings.TrimSpace(evidence.BaseRef),
			TreeHash:   strings.TrimSpace(evidence.BaseTreeHash),
			FileHashes: fileHashes,
		},
		Pathset: append([]string(nil), evidence.Pathset...),
		Lease: LeaseRef{
			ID:   strings.TrimSpace(evidence.RepoLeaseID),
			Term: evidence.LeaseTerm,
		},
		PatchQueue: PatchQueueRef{
			QueueID: strings.TrimSpace(evidence.PatchQueueID),
			ItemID:  strings.TrimSpace(evidence.PatchQueueItemID),
		},
		Operation: OperationRef{
			ID:   strings.TrimSpace(evidence.OperationID),
			Kind: strings.TrimSpace(evidence.OperationKind),
		},
	}
}

func mutationBindingEvidenceReady(evidence MutationBindingEvidence) bool {
	if !evidence.Ready || strings.TrimSpace(evidence.State) != "ready" {
		return false
	}
	if strings.TrimSpace(evidence.ReadyError) != "" ||
		strings.TrimSpace(evidence.ContextDigestError) != "" ||
		strings.TrimSpace(evidence.PatchQueueContextError) != "" ||
		len(evidence.MissingRefs) > 0 ||
		len(evidence.Mismatches) > 0 {
		return false
	}
	if !evidence.OperationKindAccepted {
		return false
	}
	for _, value := range []string{
		evidence.ContextMode,
		evidence.WorkspaceID,
		evidence.TaskID,
		evidence.SessionID,
		evidence.RunID,
		evidence.AgentID,
		evidence.PrincipalType,
		evidence.PrincipalID,
		evidence.CapabilitySnapshotID,
		evidence.RepoRoot,
		evidence.RepoLeaseID,
		evidence.PatchQueueID,
		evidence.PatchQueueItemID,
		evidence.OperationID,
		evidence.OperationKind,
		evidence.PatchQueueItemSchema,
		evidence.PatchQueueItemQueueID,
		evidence.PatchQueueItemItemID,
		evidence.PatchQueueItemContext,
		evidence.PatchQueueItemRepoLeaseID,
		evidence.PatchQueueItemOperationID,
		evidence.PatchQueueItemOperationKind,
	} {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	if evidence.LeaseTerm <= 0 || evidence.PatchQueueItemLeaseTerm <= 0 {
		return false
	}
	if len(evidence.Pathset) == 0 || len(evidence.PatchQueueItemPathset) == 0 {
		return false
	}
	if strings.TrimSpace(evidence.PatchQueueItemSchema) != PatchQueueItemSchemaVersion {
		return false
	}
	if !isCanonicalSHA256Digest(evidence.ContextDigest) || !isCanonicalSHA256Digest(evidence.PatchQueueContextDigest) {
		return false
	}
	if strings.TrimSpace(evidence.PatchQueueContextDigest) != strings.TrimSpace(evidence.PatchQueueItemContext) {
		return false
	}
	if !sameStringSlice(evidence.Pathset, evidence.PatchQueueItemPathset) {
		return false
	}
	return true
}

func verifyMutationBindingEvidence(evidence MutationBindingEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "ready", "missing_refs", "mismatch", "invalid":
	default:
		return fmt.Errorf("mutation binding evidence state %q is unsupported", evidence.State)
	}
	if evidence.Ready != mutationBindingEvidenceReady(evidence) {
		return fmt.Errorf("mutation binding evidence ready flag is inconsistent")
	}
	if evidence.Ready && state != "ready" {
		return fmt.Errorf("ready mutation binding evidence must use ready state")
	}
	if !evidence.Ready && state == "ready" {
		return fmt.Errorf("non-ready mutation binding evidence must not use ready state")
	}
	if !sameStringSlice(evidence.MissingRefs, mutationBindingEvidenceMissingRefs(evidence)) {
		return fmt.Errorf("mutation binding evidence missing_refs are inconsistent")
	}
	if !sameStringSlice(evidence.Mismatches, mutationBindingEvidenceMismatches(evidence)) {
		return fmt.Errorf("mutation binding evidence mismatches are inconsistent")
	}
	if err := verifyMutationBindingEvidenceDigests(evidence); err != nil {
		return err
	}
	if strings.TrimSpace(evidence.ContextDigest) != "" && !isCanonicalSHA256Digest(evidence.ContextDigest) {
		return fmt.Errorf("mutation binding context_digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.PatchQueueContextDigest) != "" && !isCanonicalSHA256Digest(evidence.PatchQueueContextDigest) {
		return fmt.Errorf("mutation binding patch_queue_context_digest must be canonical sha256")
	}
	if evidence.BaseFileHashCount != len(evidence.BaseFileHashes) {
		return fmt.Errorf("mutation binding base_file_hash_count is inconsistent")
	}
	expectedBaseFileHashPaths := make([]string, 0, len(evidence.BaseFileHashes))
	for path := range evidence.BaseFileHashes {
		expectedBaseFileHashPaths = append(expectedBaseFileHashPaths, strings.TrimSpace(path))
	}
	if normalized, err := NormalizePathSet(expectedBaseFileHashPaths); err == nil {
		expectedBaseFileHashPaths = normalized
	}
	if !sameStringSlice(evidence.BaseFileHashPaths, expectedBaseFileHashPaths) {
		return fmt.Errorf("mutation binding base_file_hash_paths are inconsistent")
	}
	for _, p := range evidence.Pathset {
		normalized, err := NormalizePath(p)
		if err != nil || normalized != p {
			return fmt.Errorf("mutation binding pathset contains non-normalized path %q", p)
		}
	}
	for _, p := range evidence.PatchQueueItemPathset {
		normalized, err := NormalizePath(p)
		if err != nil || normalized != p {
			return fmt.Errorf("mutation binding patch queue item pathset contains non-normalized path %q", p)
		}
	}
	for _, p := range evidence.BaseFileHashPaths {
		normalized, err := NormalizePath(p)
		if err != nil || normalized != p {
			return fmt.Errorf("mutation binding base file hash paths contain non-normalized path %q", p)
		}
		if len(evidence.Pathset) > 0 && !pathsetCoversPath(evidence.Pathset, p) {
			return fmt.Errorf("mutation binding base file hash path %q is outside pathset", p)
		}
	}
	return nil
}

func mergeAdmissionEvidenceReady(binding MutationBindingEvidence, evidence MergeAdmissionEvidence) bool {
	if !evidence.Ready || strings.TrimSpace(evidence.State) != "ready" || strings.TrimSpace(evidence.ReadyError) != "" {
		return false
	}
	return recomputeMergeAdmissionEvidence(binding, evidence) == nil
}

func verifyMergeAdmissionEvidence(binding MutationBindingEvidence, evidence MergeAdmissionEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "ready", "invalid":
	default:
		return fmt.Errorf("merge admission evidence state %q is unsupported", evidence.State)
	}
	err := recomputeMergeAdmissionEvidence(binding, evidence)
	readyError := ""
	if err != nil {
		readyError = err.Error()
	}
	expectedReady := err == nil
	if evidence.Ready != expectedReady {
		return fmt.Errorf("merge admission evidence ready flag is inconsistent")
	}
	if strings.TrimSpace(evidence.ReadyError) != readyError {
		return fmt.Errorf("merge admission evidence ready_error is inconsistent")
	}
	if evidence.Ready && state != "ready" {
		return fmt.Errorf("ready merge admission evidence must use ready state")
	}
	if !evidence.Ready && state == "ready" {
		return fmt.Errorf("non-ready merge admission evidence must not use ready state")
	}
	if strings.TrimSpace(evidence.CASPatchDigest) != "" && !isCanonicalSHA256Digest(evidence.CASPatchDigest) {
		return fmt.Errorf("merge admission CAS patch digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.CASEvaluationDigest) != "" && !isCanonicalSHA256Digest(evidence.CASEvaluationDigest) {
		return fmt.Errorf("merge admission CAS evaluation digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.TestEvidenceDigest) != "" && !isCanonicalSHA256Digest(evidence.TestEvidenceDigest) {
		return fmt.Errorf("merge admission test evidence digest must be canonical sha256")
	}
	for _, p := range evidence.PatchQueueItemPathset {
		normalized, err := NormalizePath(p)
		if err != nil || normalized != p {
			return fmt.Errorf("merge admission patch queue item pathset contains non-normalized path %q", p)
		}
	}
	return nil
}

func recomputeMergeAdmissionEvidence(binding MutationBindingEvidence, evidence MergeAdmissionEvidence) error {
	if err := verifyMergeAdmissionEvidenceMatchesMutationBinding(binding, evidence); err != nil {
		return err
	}
	return mergeAdmissionReady(mutationBindingEvidenceContext(binding), patchQueueItemFromMergeAdmissionEvidence(evidence))
}

func verifyMergeAdmissionEvidenceMatchesMutationBinding(binding MutationBindingEvidence, evidence MergeAdmissionEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {binding.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {binding.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {binding.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {binding.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {binding.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {binding.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {binding.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("merge admission evidence %s does not match mutation binding evidence", field)
		}
	}
	if binding.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("merge admission evidence lease_term does not match mutation binding evidence")
	}
	if !sameStringSlice(binding.PatchQueueItemPathset, evidence.PatchQueueItemPathset) {
		return fmt.Errorf("merge admission evidence pathset does not match mutation binding evidence")
	}
	return nil
}

func patchQueueItemFromMergeAdmissionEvidence(evidence MergeAdmissionEvidence) PatchQueueItem {
	return PatchQueueItem{
		Schema:              strings.TrimSpace(evidence.PatchQueueItemSchema),
		ID:                  strings.TrimSpace(evidence.PatchQueueItemRecordID),
		QueueID:             strings.TrimSpace(evidence.PatchQueueItemQueueID),
		ItemID:              strings.TrimSpace(evidence.PatchQueueItemItemID),
		State:               strings.TrimSpace(evidence.PatchQueueItemState),
		ContextDigest:       strings.TrimSpace(evidence.PatchQueueItemContext),
		RepoLeaseID:         strings.TrimSpace(evidence.PatchQueueItemRepoLeaseID),
		LeaseTerm:           evidence.PatchQueueItemLeaseTerm,
		Pathset:             append([]string(nil), evidence.PatchQueueItemPathset...),
		CASResult:           cloneCASPatchApplyResult(evidence.CASResult),
		CASPatchDigest:      strings.TrimSpace(evidence.CASPatchDigest),
		CASEvaluationDigest: strings.TrimSpace(evidence.CASEvaluationDigest),
		TestEvidence:        evidence.TestEvidence,
		TestEvidenceDigest:  strings.TrimSpace(evidence.TestEvidenceDigest),
		OperationID:         strings.TrimSpace(evidence.PatchQueueItemOperationID),
		OperationKind:       strings.TrimSpace(evidence.PatchQueueItemOperationKind),
	}
}

func retryBoundEvidenceReady(binding MutationBindingEvidence, evidence RetryBoundEvidence) bool {
	if !evidence.Ready || strings.TrimSpace(evidence.State) != "ready" || strings.TrimSpace(evidence.ReadyError) != "" {
		return false
	}
	return recomputeRetryBoundEvidence(binding, evidence) == nil
}

func verifyRetryBoundEvidence(binding MutationBindingEvidence, evidence RetryBoundEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "ready", "invalid":
	default:
		return fmt.Errorf("retry bound evidence state %q is unsupported", evidence.State)
	}
	err := recomputeRetryBoundEvidence(binding, evidence)
	readyError := ""
	if err != nil {
		readyError = err.Error()
	}
	expectedReady := err == nil
	if evidence.Ready != expectedReady {
		return fmt.Errorf("retry bound evidence ready flag is inconsistent")
	}
	if strings.TrimSpace(evidence.ReadyError) != readyError {
		return fmt.Errorf("retry bound evidence ready_error is inconsistent")
	}
	if evidence.Ready && state != "ready" {
		return fmt.Errorf("ready retry bound evidence must use ready state")
	}
	if !evidence.Ready && state == "ready" {
		return fmt.Errorf("non-ready retry bound evidence must not use ready state")
	}
	if err := verifyRetryBoundEvidenceTimestamps(evidence); err != nil {
		return err
	}
	return nil
}

func verifyRetryBoundEvidenceTimestamps(evidence RetryBoundEvidence) error {
	for field, value := range map[string]string{
		"next_retry_at":    evidence.NextRetryAt,
		"dead_lettered_at": evidence.DeadLetteredAt,
	} {
		if strings.TrimSpace(value) == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("retry bound evidence %s must be RFC3339Nano: %v", field, err)
		}
	}
	return nil
}

func recomputeRetryBoundEvidence(binding MutationBindingEvidence, evidence RetryBoundEvidence) error {
	if err := verifyRetryBoundEvidenceMatchesMutationBinding(binding, evidence); err != nil {
		return err
	}
	return boundedRetryReady(patchQueueItemFromRetryBoundEvidence(evidence))
}

func verifyRetryBoundEvidenceMatchesMutationBinding(binding MutationBindingEvidence, evidence RetryBoundEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {binding.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {binding.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {binding.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {binding.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {binding.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {binding.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {binding.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("retry bound evidence %s does not match mutation binding evidence", field)
		}
	}
	if binding.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("retry bound evidence lease_term does not match mutation binding evidence")
	}
	return nil
}

func patchQueueItemFromRetryBoundEvidence(evidence RetryBoundEvidence) PatchQueueItem {
	return PatchQueueItem{
		Schema:         strings.TrimSpace(evidence.PatchQueueItemSchema),
		ID:             strings.TrimSpace(evidence.PatchQueueItemRecordID),
		QueueID:        strings.TrimSpace(evidence.PatchQueueItemQueueID),
		ItemID:         strings.TrimSpace(evidence.PatchQueueItemItemID),
		State:          strings.TrimSpace(evidence.PatchQueueItemState),
		ContextDigest:  strings.TrimSpace(evidence.PatchQueueItemContext),
		RepoLeaseID:    strings.TrimSpace(evidence.PatchQueueItemRepoLeaseID),
		LeaseTerm:      evidence.PatchQueueItemLeaseTerm,
		Attempt:        evidence.Attempt,
		MaxAttempts:    evidence.MaxAttempts,
		NextRetryAt:    strings.TrimSpace(evidence.NextRetryAt),
		DeadLetteredAt: strings.TrimSpace(evidence.DeadLetteredAt),
		OperationID:    strings.TrimSpace(evidence.PatchQueueItemOperationID),
		OperationKind:  strings.TrimSpace(evidence.PatchQueueItemOperationKind),
	}
}

func rollbackProofEvidenceReady(binding MutationBindingEvidence, merge MergeAdmissionEvidence, evidence RollbackProofEvidence) bool {
	if !evidence.Ready || strings.TrimSpace(evidence.State) != "ready" || strings.TrimSpace(evidence.ReadyError) != "" {
		return false
	}
	return recomputeRollbackProofEvidence(binding, merge, evidence) == nil
}

func verifyRollbackProofEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, evidence RollbackProofEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "ready", "invalid":
	default:
		return fmt.Errorf("rollback proof evidence state %q is unsupported", evidence.State)
	}
	err := recomputeRollbackProofEvidence(binding, merge, evidence)
	readyError := ""
	if err != nil {
		readyError = err.Error()
	}
	expectedReady := err == nil
	if evidence.Ready != expectedReady {
		return fmt.Errorf("rollback proof evidence ready flag is inconsistent")
	}
	if strings.TrimSpace(evidence.ReadyError) != readyError {
		return fmt.Errorf("rollback proof evidence ready_error is inconsistent")
	}
	if evidence.Ready && state != "ready" {
		return fmt.Errorf("ready rollback proof evidence must use ready state")
	}
	if !evidence.Ready && state == "ready" {
		return fmt.Errorf("non-ready rollback proof evidence must not use ready state")
	}
	if strings.TrimSpace(evidence.CASPatchDigest) != "" && !isCanonicalSHA256Digest(evidence.CASPatchDigest) {
		return fmt.Errorf("rollback proof CAS patch digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.CASEvaluationDigest) != "" && !isCanonicalSHA256Digest(evidence.CASEvaluationDigest) {
		return fmt.Errorf("rollback proof CAS evaluation digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.RollbackEvidenceDigest) != "" && !isCanonicalSHA256Digest(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("rollback proof evidence digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.RollbackEvidence.RecordedAt) != "" {
		if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(evidence.RollbackEvidence.RecordedAt)); err != nil {
			return fmt.Errorf("rollback proof evidence recorded_at must be RFC3339Nano: %v", err)
		}
	}
	return nil
}

func recomputeRollbackProofEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, evidence RollbackProofEvidence) error {
	if err := verifyRollbackProofEvidenceMatchesMutationBinding(binding, evidence); err != nil {
		return err
	}
	if err := verifyRollbackProofEvidenceMatchesMergeAdmission(merge, evidence); err != nil {
		return err
	}
	if err := rollbackEvidenceReady(evidence.RollbackEvidence, patchQueueItemFromMergeAdmissionEvidence(merge)); err != nil {
		return err
	}
	if strings.TrimSpace(evidence.RollbackEvidenceDigest) == "" {
		return fmt.Errorf("rollback evidence digest is required")
	}
	if digestPatchQueueRollbackEvidence(evidence.RollbackEvidence) != strings.TrimSpace(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("rollback evidence digest mismatch")
	}
	return nil
}

func verifyRollbackProofEvidenceMatchesMutationBinding(binding MutationBindingEvidence, evidence RollbackProofEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {binding.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {binding.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {binding.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {binding.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {binding.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {binding.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {binding.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("rollback proof evidence %s does not match mutation binding evidence", field)
		}
	}
	if binding.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("rollback proof evidence lease_term does not match mutation binding evidence")
	}
	return nil
}

func verifyRollbackProofEvidenceMatchesMergeAdmission(merge MergeAdmissionEvidence, evidence RollbackProofEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {merge.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {merge.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {merge.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {merge.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {merge.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {merge.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {merge.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
		"cas_patch_digest":    {merge.CASPatchDigest, evidence.CASPatchDigest},
		"cas_eval_digest":     {merge.CASEvaluationDigest, evidence.CASEvaluationDigest},
		"source_patch_digest": {merge.CASPatchDigest, evidence.RollbackEvidence.SourcePatchDigest},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("rollback proof evidence %s does not match merge admission evidence", field)
		}
	}
	if merge.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("rollback proof evidence lease_term does not match merge admission evidence")
	}
	return nil
}

func reviewerAdvisoryEvidenceReady(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, evidence ReviewerAdvisoryEvidence) bool {
	if !evidence.Ready || strings.TrimSpace(evidence.State) != "ready" || strings.TrimSpace(evidence.ReadyError) != "" {
		return false
	}
	return recomputeReviewerAdvisoryEvidence(binding, merge, rollback, evidence) == nil
}

func verifyReviewerAdvisoryEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, evidence ReviewerAdvisoryEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "ready", "invalid":
	default:
		return fmt.Errorf("reviewer advisory evidence state %q is unsupported", evidence.State)
	}
	err := recomputeReviewerAdvisoryEvidence(binding, merge, rollback, evidence)
	readyError := ""
	if err != nil {
		readyError = err.Error()
	}
	expectedReady := err == nil
	if evidence.Ready != expectedReady {
		return fmt.Errorf("reviewer advisory evidence ready flag is inconsistent")
	}
	if strings.TrimSpace(evidence.ReadyError) != readyError {
		return fmt.Errorf("reviewer advisory evidence ready_error is inconsistent")
	}
	if evidence.Ready && state != "ready" {
		return fmt.Errorf("ready reviewer advisory evidence must use ready state")
	}
	if !evidence.Ready && state == "ready" {
		return fmt.Errorf("non-ready reviewer advisory evidence must not use ready state")
	}
	if strings.TrimSpace(evidence.CASPatchDigest) != "" && !isCanonicalSHA256Digest(evidence.CASPatchDigest) {
		return fmt.Errorf("reviewer advisory CAS patch digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.CASEvaluationDigest) != "" && !isCanonicalSHA256Digest(evidence.CASEvaluationDigest) {
		return fmt.Errorf("reviewer advisory CAS evaluation digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.RollbackEvidenceDigest) != "" && !isCanonicalSHA256Digest(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("reviewer advisory rollback evidence digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.AdvisoryDigest) != "" && !isCanonicalSHA256Digest(evidence.AdvisoryDigest) {
		return fmt.Errorf("reviewer advisory digest must be canonical sha256")
	}
	return nil
}

func recomputeReviewerAdvisoryEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, evidence ReviewerAdvisoryEvidence) error {
	item := patchQueueItemFromReviewerAdvisoryEvidence(evidence)
	if err := reviewerAdvisoryReady(evidence.Advisory, item); err != nil {
		return err
	}
	if err := verifyReviewerAdvisoryEvidenceMatchesMutationBinding(binding, evidence); err != nil {
		return err
	}
	if err := verifyReviewerAdvisoryEvidenceMatchesMergeAdmission(merge, evidence); err != nil {
		return err
	}
	if strings.TrimSpace(rollback.RollbackEvidenceDigest) != strings.TrimSpace(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("reviewer advisory evidence rollback digest does not match rollback proof evidence")
	}
	if strings.TrimSpace(evidence.AdvisoryDigest) == "" {
		return fmt.Errorf("reviewer advisory digest is required")
	}
	if digestPatchQueueReviewerAdvisory(evidence.Advisory) != strings.TrimSpace(evidence.AdvisoryDigest) {
		return fmt.Errorf("reviewer advisory digest mismatch")
	}
	return nil
}

func patchQueueItemFromReviewerAdvisoryEvidence(evidence ReviewerAdvisoryEvidence) PatchQueueItem {
	return PatchQueueItem{
		Schema:                 strings.TrimSpace(evidence.PatchQueueItemSchema),
		ID:                     strings.TrimSpace(evidence.PatchQueueItemRecordID),
		QueueID:                strings.TrimSpace(evidence.PatchQueueItemQueueID),
		ItemID:                 strings.TrimSpace(evidence.PatchQueueItemItemID),
		State:                  strings.TrimSpace(evidence.PatchQueueItemState),
		ContextDigest:          strings.TrimSpace(evidence.PatchQueueItemContext),
		RepoLeaseID:            strings.TrimSpace(evidence.PatchQueueItemRepoLeaseID),
		LeaseTerm:              evidence.PatchQueueItemLeaseTerm,
		OperationID:            strings.TrimSpace(evidence.PatchQueueItemOperationID),
		OperationKind:          strings.TrimSpace(evidence.PatchQueueItemOperationKind),
		CASPatchDigest:         strings.TrimSpace(evidence.CASPatchDigest),
		CASEvaluationDigest:    strings.TrimSpace(evidence.CASEvaluationDigest),
		RollbackEvidenceDigest: strings.TrimSpace(evidence.RollbackEvidenceDigest),
		ReviewerAdvisoryDigest: strings.TrimSpace(evidence.AdvisoryDigest),
		WorkspaceID:            strings.TrimSpace(evidence.WorkspaceID),
		ProjectID:              strings.TrimSpace(evidence.ProjectID),
	}
}

func verifyReviewerAdvisoryEvidenceMatchesMutationBinding(binding MutationBindingEvidence, evidence ReviewerAdvisoryEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {binding.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {binding.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {binding.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {binding.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {binding.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {binding.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {binding.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("reviewer advisory evidence %s does not match mutation binding evidence", field)
		}
	}
	if binding.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("reviewer advisory evidence lease_term does not match mutation binding evidence")
	}
	return nil
}

func verifyReviewerAdvisoryEvidenceMatchesMergeAdmission(merge MergeAdmissionEvidence, evidence ReviewerAdvisoryEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {merge.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {merge.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {merge.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {merge.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {merge.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {merge.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {merge.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
		"cas_patch_digest":    {merge.CASPatchDigest, evidence.CASPatchDigest},
		"cas_eval_digest":     {merge.CASEvaluationDigest, evidence.CASEvaluationDigest},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("reviewer advisory evidence %s does not match merge admission evidence", field)
		}
	}
	if merge.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("reviewer advisory evidence lease_term does not match merge admission evidence")
	}
	return nil
}

func operatorEnablementEvidenceReady(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, reviewer ReviewerAdvisoryEvidence, evidence OperatorEnablementEvidence) bool {
	if !evidence.Ready || strings.TrimSpace(evidence.State) != "ready" || strings.TrimSpace(evidence.ReadyError) != "" {
		return false
	}
	return recomputeOperatorEnablementEvidence(binding, merge, rollback, reviewer, evidence) == nil
}

func verifyOperatorEnablementEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, reviewer ReviewerAdvisoryEvidence, evidence OperatorEnablementEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "ready", "invalid":
	default:
		return fmt.Errorf("operator enablement evidence state %q is unsupported", evidence.State)
	}
	err := recomputeOperatorEnablementEvidence(binding, merge, rollback, reviewer, evidence)
	readyError := ""
	if err != nil {
		readyError = err.Error()
	}
	expectedReady := err == nil
	if evidence.Ready != expectedReady {
		return fmt.Errorf("operator enablement evidence ready flag is inconsistent")
	}
	if strings.TrimSpace(evidence.ReadyError) != readyError {
		return fmt.Errorf("operator enablement evidence ready_error is inconsistent")
	}
	if evidence.Ready && state != "ready" {
		return fmt.Errorf("ready operator enablement evidence must use ready state")
	}
	if !evidence.Ready && state == "ready" {
		return fmt.Errorf("non-ready operator enablement evidence must not use ready state")
	}
	if strings.TrimSpace(evidence.CASPatchDigest) != "" && !isCanonicalSHA256Digest(evidence.CASPatchDigest) {
		return fmt.Errorf("operator enablement CAS patch digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.RollbackEvidenceDigest) != "" && !isCanonicalSHA256Digest(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("operator enablement rollback evidence digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.ReviewerAdvisoryDigest) != "" && !isCanonicalSHA256Digest(evidence.ReviewerAdvisoryDigest) {
		return fmt.Errorf("operator enablement reviewer advisory digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.EnablementDigest) != "" && !isCanonicalSHA256Digest(evidence.EnablementDigest) {
		return fmt.Errorf("operator enablement digest must be canonical sha256")
	}
	return nil
}

func recomputeOperatorEnablementEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, reviewer ReviewerAdvisoryEvidence, evidence OperatorEnablementEvidence) error {
	item := patchQueueItemFromOperatorEnablementEvidence(evidence)
	if err := operatorEnablementReady(evidence.Enablement, item, reviewer.Advisory); err != nil {
		return err
	}
	if err := verifyOperatorEnablementEvidenceMatchesMutationBinding(binding, evidence); err != nil {
		return err
	}
	if err := verifyOperatorEnablementEvidenceMatchesMergeAdmission(merge, evidence); err != nil {
		return err
	}
	if strings.TrimSpace(rollback.RollbackEvidenceDigest) != strings.TrimSpace(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("operator enablement evidence rollback digest does not match rollback proof evidence")
	}
	if strings.TrimSpace(reviewer.AdvisoryDigest) != strings.TrimSpace(evidence.ReviewerAdvisoryDigest) {
		return fmt.Errorf("operator enablement evidence reviewer advisory digest does not match reviewer evidence")
	}
	if strings.TrimSpace(evidence.EnablementDigest) == "" {
		return fmt.Errorf("operator enablement digest is required")
	}
	if digestPatchQueueOperatorEnablement(evidence.Enablement) != strings.TrimSpace(evidence.EnablementDigest) {
		return fmt.Errorf("operator enablement digest mismatch")
	}
	return nil
}

func patchQueueItemFromOperatorEnablementEvidence(evidence OperatorEnablementEvidence) PatchQueueItem {
	return PatchQueueItem{
		Schema:                   strings.TrimSpace(evidence.PatchQueueItemSchema),
		ID:                       strings.TrimSpace(evidence.PatchQueueItemRecordID),
		QueueID:                  strings.TrimSpace(evidence.PatchQueueItemQueueID),
		ItemID:                   strings.TrimSpace(evidence.PatchQueueItemItemID),
		State:                    strings.TrimSpace(evidence.PatchQueueItemState),
		ContextDigest:            strings.TrimSpace(evidence.PatchQueueItemContext),
		RepoLeaseID:              strings.TrimSpace(evidence.PatchQueueItemRepoLeaseID),
		LeaseTerm:                evidence.PatchQueueItemLeaseTerm,
		OperationID:              strings.TrimSpace(evidence.PatchQueueItemOperationID),
		OperationKind:            strings.TrimSpace(evidence.PatchQueueItemOperationKind),
		CASPatchDigest:           strings.TrimSpace(evidence.CASPatchDigest),
		RollbackEvidenceDigest:   strings.TrimSpace(evidence.RollbackEvidenceDigest),
		ReviewerAdvisoryDigest:   strings.TrimSpace(evidence.ReviewerAdvisoryDigest),
		OperatorEnablementDigest: strings.TrimSpace(evidence.EnablementDigest),
		WorkspaceID:              strings.TrimSpace(evidence.WorkspaceID),
		ProjectID:                strings.TrimSpace(evidence.ProjectID),
	}
}

func verifyOperatorEnablementEvidenceMatchesMutationBinding(binding MutationBindingEvidence, evidence OperatorEnablementEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {binding.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {binding.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {binding.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {binding.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {binding.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {binding.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {binding.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("operator enablement evidence %s does not match mutation binding evidence", field)
		}
	}
	if binding.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("operator enablement evidence lease_term does not match mutation binding evidence")
	}
	return nil
}

func verifyOperatorEnablementEvidenceMatchesMergeAdmission(merge MergeAdmissionEvidence, evidence OperatorEnablementEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {merge.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {merge.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {merge.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {merge.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {merge.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {merge.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {merge.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
		"cas_patch_digest":    {merge.CASPatchDigest, evidence.CASPatchDigest},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("operator enablement evidence %s does not match merge admission evidence", field)
		}
	}
	if merge.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("operator enablement evidence lease_term does not match merge admission evidence")
	}
	return nil
}

func buildMaterializationPreflightEvidence(item PatchQueueItem, worktree WorktreeIdentityEvidence, materialization PatchMaterialization, authorityProof PatchMaterializationAuthorityProof, force bool) *MaterializationPreflightEvidence {
	if !force && !materializationPreflightInputHasAnyEvidence(item, materialization, authorityProof, worktree) {
		return nil
	}
	evidence := MaterializationPreflightEvidence{
		PatchQueueItemSchema:        strings.TrimSpace(item.Schema),
		PatchQueueItemRecordID:      strings.TrimSpace(item.ID),
		PatchQueueItemQueueID:       strings.TrimSpace(item.QueueID),
		PatchQueueItemItemID:        strings.TrimSpace(item.ItemID),
		PatchQueueItemState:         strings.TrimSpace(item.State),
		PatchQueueItemContext:       strings.TrimSpace(item.ContextDigest),
		PatchQueueItemRepoLeaseID:   strings.TrimSpace(item.RepoLeaseID),
		PatchQueueItemLeaseTerm:     item.LeaseTerm,
		PatchQueueItemOperationID:   strings.TrimSpace(item.OperationID),
		PatchQueueItemOperationKind: strings.TrimSpace(item.OperationKind),
		CASPatchDigest:              strings.TrimSpace(item.CASPatchDigest),
		CASEvaluationDigest:         strings.TrimSpace(item.CASEvaluationDigest),
		RollbackEvidenceDigest:      strings.TrimSpace(item.RollbackEvidenceDigest),
		ReviewerAdvisoryDigest:      strings.TrimSpace(item.ReviewerAdvisoryDigest),
		OperatorEnablementDigest:    strings.TrimSpace(item.OperatorEnablementDigest),
		WorkspaceID:                 strings.TrimSpace(item.WorkspaceID),
		ProjectID:                   strings.TrimSpace(item.ProjectID),
		WorktreeIdentity:            cloneWorktreeIdentityEvidenceValue(worktree),
		Materialization:             patchMaterializationDiagnostic(materialization),
		MaterializationDigest:       strings.TrimSpace(materialization.MaterializationDigest),
	}
	if hasPatchMaterializationAuthorityProof(authorityProof) {
		proof := authorityProof
		evidence.AuthorityProof = &proof
	}
	if err := materializationPreflightReady(materialization, item, worktree); err != nil {
		evidence.Ready = false
		evidence.ReadyError = err.Error()
		if strings.TrimSpace(materialization.Schema) == "" && len(materialization.Files) == 0 {
			evidence.State = "missing_refs"
		} else {
			evidence.State = "invalid"
		}
		return &evidence
	}
	if !hasPatchMaterializationAuthorityProof(authorityProof) {
		evidence.Ready = false
		evidence.ReadyError = MaterializationPreflightAuthorityProofRequired
		evidence.State = "authority_required"
		return &evidence
	}
	if err := ValidatePatchMaterializationAuthorityProof(authorityProof, materialization, item); err != nil {
		evidence.Ready = false
		evidence.ReadyError = "materialization authority proof invalid: " + err.Error()
		evidence.State = "invalid"
		return &evidence
	}
	evidence.Ready = true
	evidence.State = "ready"
	return &evidence
}

func materializationPreflightInputHasAnyEvidence(item PatchQueueItem, materialization PatchMaterialization, authorityProof PatchMaterializationAuthorityProof, worktree WorktreeIdentityEvidence) bool {
	if strings.TrimSpace(materialization.Schema) != "" ||
		strings.TrimSpace(materialization.WorkspaceID) != "" ||
		strings.TrimSpace(materialization.QueueID) != "" ||
		strings.TrimSpace(materialization.ItemID) != "" ||
		strings.TrimSpace(materialization.OperationID) != "" ||
		strings.TrimSpace(materialization.MaterializationDigest) != "" ||
		len(materialization.Files) > 0 {
		return true
	}
	if hasPatchMaterializationAuthorityProof(authorityProof) {
		return true
	}
	return strings.TrimSpace(item.CASPatchDigest) != "" && worktreeIdentityReady(worktree)
}

func materializationPreflightReady(materialization PatchMaterialization, item PatchQueueItem, worktree WorktreeIdentityEvidence) error {
	if !worktreeIdentityReady(worktree) {
		return fmt.Errorf("materialization preflight requires canonical worktree identity")
	}
	if strings.TrimSpace(materialization.Schema) == "" && len(materialization.Files) == 0 {
		return fmt.Errorf("patch materialization is required")
	}
	if err := ValidatePatchMaterialization(materialization, item); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	return nil
}

func materializationPreflightEvidenceReady(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, reviewer ReviewerAdvisoryEvidence, operator OperatorEnablementEvidence, worktree WorktreeIdentityEvidence, evidence MaterializationPreflightEvidence) bool {
	return evidence.Ready && recomputeMaterializationPreflightEvidence(binding, merge, rollback, reviewer, operator, worktree, evidence) == nil
}

func verifyMaterializationPreflightDiagnosticEvidence(evidence MaterializationPreflightEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "missing_refs", "invalid", "authority_required":
	default:
		return fmt.Errorf("materialization preflight evidence state %q is unsupported without upstream evidence", evidence.State)
	}
	if evidence.Ready {
		return fmt.Errorf("ready materialization preflight evidence requires upstream evidence")
	}
	if strings.TrimSpace(evidence.ReadyError) == "" {
		return fmt.Errorf("non-ready materialization preflight evidence requires ready_error")
	}
	if strings.TrimSpace(evidence.CASPatchDigest) != "" && !isCanonicalSHA256Digest(evidence.CASPatchDigest) {
		return fmt.Errorf("materialization preflight CAS patch digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.CASEvaluationDigest) != "" && !isCanonicalSHA256Digest(evidence.CASEvaluationDigest) {
		return fmt.Errorf("materialization preflight CAS evaluation digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.RollbackEvidenceDigest) != "" && !isCanonicalSHA256Digest(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("materialization preflight rollback evidence digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.ReviewerAdvisoryDigest) != "" && !isCanonicalSHA256Digest(evidence.ReviewerAdvisoryDigest) {
		return fmt.Errorf("materialization preflight reviewer advisory digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.OperatorEnablementDigest) != "" && !isCanonicalSHA256Digest(evidence.OperatorEnablementDigest) {
		return fmt.Errorf("materialization preflight operator enablement digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.MaterializationDigest) != "" && !isCanonicalSHA256Digest(evidence.MaterializationDigest) {
		return fmt.Errorf("materialization preflight digest must be canonical sha256")
	}
	if evidence.AuthorityProof != nil && !hasPatchMaterializationAuthorityProof(*evidence.AuthorityProof) {
		return fmt.Errorf("materialization preflight authority proof is empty")
	}
	return nil
}

func verifyMaterializationPreflightEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, reviewer ReviewerAdvisoryEvidence, operator OperatorEnablementEvidence, worktree WorktreeIdentityEvidence, evidence MaterializationPreflightEvidence) error {
	state := strings.TrimSpace(evidence.State)
	switch state {
	case "ready", "missing_refs", "invalid", "authority_required":
	default:
		return fmt.Errorf("materialization preflight evidence state %q is unsupported", evidence.State)
	}
	err := recomputeMaterializationPreflightEvidence(binding, merge, rollback, reviewer, operator, worktree, evidence)
	readyError := ""
	expectedReady := err == nil
	if err != nil {
		readyError = err.Error()
	}
	if evidence.Ready != expectedReady {
		return fmt.Errorf("materialization preflight evidence ready flag is inconsistent")
	}
	if expectedReady && strings.TrimSpace(evidence.ReadyError) != "" {
		return fmt.Errorf("ready materialization preflight evidence must not include ready_error")
	}
	if !expectedReady && strings.TrimSpace(evidence.ReadyError) == "" {
		return fmt.Errorf("non-ready materialization preflight evidence requires ready_error")
	}
	if strings.TrimSpace(evidence.ReadyError) != readyError {
		return fmt.Errorf("materialization preflight evidence ready_error is inconsistent")
	}
	if evidence.Ready && state != "ready" {
		return fmt.Errorf("ready materialization preflight evidence must use ready state")
	}
	if !evidence.Ready && state == "ready" {
		return fmt.Errorf("non-ready materialization preflight evidence must not use ready state")
	}
	if strings.TrimSpace(evidence.CASPatchDigest) != "" && !isCanonicalSHA256Digest(evidence.CASPatchDigest) {
		return fmt.Errorf("materialization preflight CAS patch digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.CASEvaluationDigest) != "" && !isCanonicalSHA256Digest(evidence.CASEvaluationDigest) {
		return fmt.Errorf("materialization preflight CAS evaluation digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.RollbackEvidenceDigest) != "" && !isCanonicalSHA256Digest(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("materialization preflight rollback evidence digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.ReviewerAdvisoryDigest) != "" && !isCanonicalSHA256Digest(evidence.ReviewerAdvisoryDigest) {
		return fmt.Errorf("materialization preflight reviewer advisory digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.OperatorEnablementDigest) != "" && !isCanonicalSHA256Digest(evidence.OperatorEnablementDigest) {
		return fmt.Errorf("materialization preflight operator enablement digest must be canonical sha256")
	}
	if strings.TrimSpace(evidence.MaterializationDigest) != "" && !isCanonicalSHA256Digest(evidence.MaterializationDigest) {
		return fmt.Errorf("materialization preflight digest must be canonical sha256")
	}
	if evidence.AuthorityProof != nil && !hasPatchMaterializationAuthorityProof(*evidence.AuthorityProof) {
		return fmt.Errorf("materialization preflight authority proof is empty")
	}
	return nil
}

func recomputeMaterializationPreflightEvidence(binding MutationBindingEvidence, merge MergeAdmissionEvidence, rollback RollbackProofEvidence, reviewer ReviewerAdvisoryEvidence, operator OperatorEnablementEvidence, worktree WorktreeIdentityEvidence, evidence MaterializationPreflightEvidence) error {
	if !worktreeIdentityReady(evidence.WorktreeIdentity) {
		return fmt.Errorf("materialization preflight requires canonical worktree identity")
	}
	if strings.TrimSpace(evidence.Materialization.Schema) == "" && evidence.Materialization.FileCount == 0 && len(evidence.Materialization.Files) == 0 {
		return fmt.Errorf("patch materialization is required")
	}
	if err := verifyMaterializationPreflightEvidenceMatchesMutationBinding(binding, evidence); err != nil {
		return err
	}
	if err := verifyMaterializationPreflightEvidenceMatchesMergeAdmission(merge, evidence); err != nil {
		return err
	}
	if strings.TrimSpace(rollback.RollbackEvidenceDigest) != strings.TrimSpace(evidence.RollbackEvidenceDigest) {
		return fmt.Errorf("materialization preflight rollback digest does not match rollback proof evidence")
	}
	if strings.TrimSpace(reviewer.AdvisoryDigest) != strings.TrimSpace(evidence.ReviewerAdvisoryDigest) {
		return fmt.Errorf("materialization preflight reviewer advisory digest does not match reviewer evidence")
	}
	if strings.TrimSpace(operator.EnablementDigest) != strings.TrimSpace(evidence.OperatorEnablementDigest) {
		return fmt.Errorf("materialization preflight operator enablement digest does not match operator evidence")
	}
	if err := verifyMaterializationPreflightEvidenceMatchesWorktree(worktree, evidence.WorktreeIdentity); err != nil {
		return err
	}
	if strings.TrimSpace(evidence.MaterializationDigest) == "" {
		return fmt.Errorf("materialization preflight digest is required")
	}
	if strings.TrimSpace(evidence.Materialization.MaterializationDigest) != strings.TrimSpace(evidence.MaterializationDigest) {
		return fmt.Errorf("materialization preflight digest does not match materialization")
	}
	item := patchQueueItemFromMergeAdmissionEvidence(merge)
	item.WorkspaceID = strings.TrimSpace(evidence.WorkspaceID)
	item.ProjectID = strings.TrimSpace(evidence.ProjectID)
	if err := validatePatchMaterializationDiagnostic(evidence.Materialization, item, evidence.WorktreeIdentity, evidence.MaterializationDigest); err != nil {
		return err
	}
	if evidence.AuthorityProof == nil || !hasPatchMaterializationAuthorityProof(*evidence.AuthorityProof) {
		return fmt.Errorf(MaterializationPreflightAuthorityProofRequired)
	}
	if err := validatePatchMaterializationAuthorityProofDiagnostic(*evidence.AuthorityProof, evidence.Materialization, item, evidence.MaterializationDigest); err != nil {
		return fmt.Errorf("materialization authority proof invalid: %w", err)
	}
	return nil
}

func verifyMaterializationPreflightEvidenceMatchesMutationBinding(binding MutationBindingEvidence, evidence MaterializationPreflightEvidence) error {
	expected := map[string][2]string{
		"workspace_id":        {binding.WorkspaceID, evidence.WorkspaceID},
		"patch_queue_schema":  {binding.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {binding.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {binding.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {binding.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {binding.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {binding.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {binding.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("materialization preflight evidence %s does not match mutation binding evidence", field)
		}
	}
	if binding.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("materialization preflight evidence lease_term does not match mutation binding evidence")
	}
	return nil
}

func verifyMaterializationPreflightEvidenceMatchesMergeAdmission(merge MergeAdmissionEvidence, evidence MaterializationPreflightEvidence) error {
	expected := map[string][2]string{
		"patch_queue_schema":  {merge.PatchQueueItemSchema, evidence.PatchQueueItemSchema},
		"patch_queue_id":      {merge.PatchQueueItemQueueID, evidence.PatchQueueItemQueueID},
		"patch_queue_item_id": {merge.PatchQueueItemItemID, evidence.PatchQueueItemItemID},
		"patch_queue_context": {merge.PatchQueueItemContext, evidence.PatchQueueItemContext},
		"repo_lease_id":       {merge.PatchQueueItemRepoLeaseID, evidence.PatchQueueItemRepoLeaseID},
		"operation_id":        {merge.PatchQueueItemOperationID, evidence.PatchQueueItemOperationID},
		"operation_kind":      {merge.PatchQueueItemOperationKind, evidence.PatchQueueItemOperationKind},
		"cas_patch_digest":    {merge.CASPatchDigest, evidence.CASPatchDigest},
		"cas_eval_digest":     {merge.CASEvaluationDigest, evidence.CASEvaluationDigest},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("materialization preflight evidence %s does not match merge admission evidence", field)
		}
	}
	if merge.PatchQueueItemLeaseTerm != evidence.PatchQueueItemLeaseTerm {
		return fmt.Errorf("materialization preflight evidence lease_term does not match merge admission evidence")
	}
	return nil
}

func verifyMaterializationPreflightEvidenceMatchesWorktree(want WorktreeIdentityEvidence, got WorktreeIdentityEvidence) error {
	expected := map[string][2]string{
		"repo_id":                {want.RepoID, got.RepoID},
		"checkout_id":            {want.CheckoutID, got.CheckoutID},
		"branch_id":              {want.BranchID, got.BranchID},
		"branch_name":            {want.BranchName, got.BranchName},
		"machine_id":             {want.MachineID, got.MachineID},
		"local_path":             {want.LocalPath, got.LocalPath},
		"base_sha":               {want.BaseSHA, got.BaseSHA},
		"head_sha":               {want.HeadSHA, got.HeadSHA},
		"readback_state":         {want.ReadbackState, got.ReadbackState},
		"observed_worktree_root": {want.ObservedWorktreeRoot, got.ObservedWorktreeRoot},
		"observed_branch_name":   {want.ObservedBranchName, got.ObservedBranchName},
		"observed_head_sha":      {want.ObservedHeadSHA, got.ObservedHeadSHA},
		"observed_dirty_state":   {want.ObservedDirtyState, got.ObservedDirtyState},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("materialization preflight worktree %s does not match activation worktree identity", field)
		}
	}
	if !worktreeIdentityReady(got) {
		return fmt.Errorf("materialization preflight worktree identity is not ready")
	}
	return nil
}

func cloneWorktreeIdentityEvidenceValue(evidence WorktreeIdentityEvidence) WorktreeIdentityEvidence {
	cloned := cloneWorktreeIdentityEvidence(evidence)
	if cloned == nil {
		return WorktreeIdentityEvidence{}
	}
	return *cloned
}

func patchMaterializationDiagnostic(materialization PatchMaterialization) PatchMaterializationDiagnostic {
	diagnostic := PatchMaterializationDiagnostic{
		Schema:                strings.TrimSpace(materialization.Schema),
		WorkspaceID:           strings.TrimSpace(materialization.WorkspaceID),
		ProjectID:             strings.TrimSpace(materialization.ProjectID),
		QueueID:               strings.TrimSpace(materialization.QueueID),
		ItemID:                strings.TrimSpace(materialization.ItemID),
		OperationID:           strings.TrimSpace(materialization.OperationID),
		OperationKind:         strings.TrimSpace(materialization.OperationKind),
		CASPatchDigest:        strings.TrimSpace(materialization.CASPatchDigest),
		CASEvaluationDigest:   strings.TrimSpace(materialization.CASEvaluationDigest),
		RecordedBy:            strings.TrimSpace(materialization.RecordedBy),
		RecordedAt:            strings.TrimSpace(materialization.RecordedAt),
		MaterializationDigest: strings.TrimSpace(materialization.MaterializationDigest),
		FileCount:             len(materialization.Files),
	}
	if len(materialization.Files) > 0 {
		diagnostic.Files = make([]PatchMaterializedFileDiagnostic, 0, len(materialization.Files))
		for _, file := range materialization.Files {
			diagnostic.Files = append(diagnostic.Files, PatchMaterializedFileDiagnostic{
				Path:            strings.TrimSpace(file.Path),
				ChangeKind:      patchMaterializationDiagnosticChangeKind(file.ChangeKind),
				BaseHash:        strings.TrimSpace(file.BaseHash),
				CandidateHash:   strings.TrimSpace(file.CandidateHash),
				ContentEncoding: strings.TrimSpace(file.ContentEncoding),
				ContentDigest:   strings.TrimSpace(file.ContentDigest),
			})
		}
	}
	return diagnostic
}

func validatePatchMaterializationDiagnostic(diagnostic PatchMaterializationDiagnostic, item PatchQueueItem, worktree WorktreeIdentityEvidence, expectedDigest string) error {
	if !worktreeIdentityReady(worktree) {
		return fmt.Errorf("materialization preflight requires canonical worktree identity")
	}
	if strings.TrimSpace(diagnostic.Schema) == "" && diagnostic.FileCount == 0 && len(diagnostic.Files) == 0 {
		return fmt.Errorf("patch materialization is required")
	}
	if strings.TrimSpace(diagnostic.Schema) != PatchMaterializationSchemaVersion {
		return fmt.Errorf("patch materialization invalid: patch materialization schema is unsupported")
	}
	if err := patchMaterializationMatch("workspace_id", diagnostic.WorkspaceID, item.WorkspaceID); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if strings.TrimSpace(item.ProjectID) != "" {
		if err := patchMaterializationMatch("project_id", diagnostic.ProjectID, item.ProjectID); err != nil {
			return fmt.Errorf("patch materialization invalid: %w", err)
		}
	}
	if err := patchMaterializationMatch("queue_id", diagnostic.QueueID, item.QueueID); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if err := patchMaterializationMatch("item_id", diagnostic.ItemID, item.ItemID); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if err := patchMaterializationMatch("operation_id", diagnostic.OperationID, item.OperationID); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if err := patchMaterializationMatch("operation_kind", diagnostic.OperationKind, item.OperationKind); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if err := patchMaterializationMatch("cas_patch_digest", diagnostic.CASPatchDigest, item.CASPatchDigest); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if err := patchMaterializationMatch("cas_evaluation_digest", diagnostic.CASEvaluationDigest, item.CASEvaluationDigest); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if err := patchMaterializationMatch("materialization_digest", diagnostic.MaterializationDigest, expectedDigest); err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if !isCanonicalSHA256Digest(diagnostic.MaterializationDigest) {
		return fmt.Errorf("patch materialization invalid: patch materialization digest is required")
	}
	expectedPaths, casPaths, err := patchMaterializationAppliedCASPaths(item)
	if err != nil {
		return fmt.Errorf("patch materialization invalid: %w", err)
	}
	if diagnostic.FileCount != len(expectedPaths) || len(diagnostic.Files) != len(expectedPaths) {
		return fmt.Errorf("patch materialization invalid: patch materialization file count %d does not match applied CAS path count %d", len(diagnostic.Files), len(expectedPaths))
	}
	for i, file := range diagnostic.Files {
		path, err := NormalizePath(file.Path)
		if err != nil {
			return fmt.Errorf("patch materialization invalid: materialized file[%d] path invalid: %w", i, err)
		}
		if file.Path != path {
			return fmt.Errorf("patch materialization invalid: materialized file[%d] path is not normalized: got %q want %q", i, file.Path, path)
		}
		if i > 0 && diagnostic.Files[i-1].Path >= file.Path {
			return fmt.Errorf("patch materialization invalid: materialized files must be sorted and unique")
		}
		if expectedPaths[i] != file.Path {
			return fmt.Errorf("patch materialization invalid: materialized file[%d] path %q does not match applied CAS path %q", i, file.Path, expectedPaths[i])
		}
		casPath, ok := casPaths[file.Path]
		if !ok {
			return fmt.Errorf("patch materialization invalid: materialized file %s is missing from CAS result", file.Path)
		}
		if casPath.Status != CASPatchStatusApplied {
			return fmt.Errorf("patch materialization invalid: materialized file %s CAS status is %q", file.Path, casPath.Status)
		}
		changeKind := patchMaterializationDiagnosticChangeKind(file.ChangeKind)
		if changeKind != casPatchPathChangeKind(casPath) {
			return fmt.Errorf("patch materialization invalid: materialized file %s change_kind mismatch: got %q want %q", file.Path, file.ChangeKind, casPatchPathChangeKind(casPath))
		}
		if changeKind == CASPatchChangeAdd {
			if strings.TrimSpace(file.BaseHash) != "" || strings.TrimSpace(casPath.BaseHash) != "" {
				return fmt.Errorf("patch materialization invalid: materialized file %s: added path must not carry base_hash", file.Path)
			}
		} else if err := patchMaterializationMatch("base_hash", file.BaseHash, casPath.BaseHash); err != nil {
			return fmt.Errorf("patch materialization invalid: materialized file %s: %w", file.Path, err)
		}
		if err := patchMaterializationMatch("candidate_hash", file.CandidateHash, casPath.CandidateHash); err != nil {
			return fmt.Errorf("patch materialization invalid: materialized file %s: %w", file.Path, err)
		}
		if strings.TrimSpace(file.ContentEncoding) != PatchMaterializationEncodingUTF8 {
			return fmt.Errorf("patch materialization invalid: materialized file %s has unsupported content_encoding %q", file.Path, file.ContentEncoding)
		}
		if !isCanonicalSHA256Digest(file.ContentDigest) {
			return fmt.Errorf("patch materialization invalid: materialized file %s content_digest mismatch", file.Path)
		}
		if strings.TrimSpace(file.CandidateHash) != strings.TrimSpace(file.ContentDigest) {
			return fmt.Errorf("patch materialization invalid: materialized file %s candidate_hash does not match content_digest", file.Path)
		}
	}
	return nil
}

func patchMaterializationDiagnosticChangeKind(changeKind string) string {
	changeKind = strings.TrimSpace(changeKind)
	if changeKind == "" {
		return CASPatchChangeModify
	}
	return changeKind
}

func hasPatchMaterializationAuthorityProof(proof PatchMaterializationAuthorityProof) bool {
	if len(proof.Files) > 0 {
		return true
	}
	for _, value := range []string{
		proof.Schema,
		proof.Source,
		proof.WorkspaceID,
		proof.QueueID,
		proof.ItemID,
		proof.OperationID,
		proof.OperationKind,
		proof.CASPatchDigest,
		proof.CASEvaluationDigest,
		proof.MaterializationDigest,
		proof.MaterializationJSONDigest,
		proof.AuthorityDigest,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func validatePatchMaterializationAuthorityProofDiagnostic(proof PatchMaterializationAuthorityProof, diagnostic PatchMaterializationDiagnostic, item PatchQueueItem, expectedDigest string) error {
	if strings.TrimSpace(proof.Schema) != PatchMaterializationAuthorityProofSchemaVersion {
		return fmt.Errorf("schema is unsupported")
	}
	if strings.TrimSpace(proof.Source) != PatchMaterializationAuthorityProofSourceSQLite {
		return fmt.Errorf("source is unsupported")
	}
	expected := map[string][2]string{
		"workspace_id":           {proof.WorkspaceID, item.WorkspaceID},
		"queue_id":               {proof.QueueID, item.QueueID},
		"item_id":                {proof.ItemID, item.ItemID},
		"operation_id":           {proof.OperationID, item.OperationID},
		"operation_kind":         {proof.OperationKind, item.OperationKind},
		"cas_patch_digest":       {proof.CASPatchDigest, item.CASPatchDigest},
		"cas_evaluation_digest":  {proof.CASEvaluationDigest, item.CASEvaluationDigest},
		"materialization_digest": {proof.MaterializationDigest, expectedDigest},
		"recorded_by":            {proof.RecordedBy, diagnostic.RecordedBy},
		"recorded_at":            {proof.RecordedAt, diagnostic.RecordedAt},
	}
	if strings.TrimSpace(item.ProjectID) != "" {
		expected["project_id"] = [2]string{proof.ProjectID, item.ProjectID}
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("%s mismatch", field)
		}
	}
	if strings.TrimSpace(proof.MaterializationDigest) != strings.TrimSpace(diagnostic.MaterializationDigest) {
		return fmt.Errorf("materialization digest does not match diagnostic")
	}
	if !isCanonicalSHA256Digest(proof.MaterializationDigest) {
		return fmt.Errorf("materialization digest must be canonical sha256")
	}
	if !isCanonicalSHA256Digest(proof.MaterializationJSONDigest) {
		return fmt.Errorf("materialization JSON digest must be canonical sha256")
	}
	if !isCanonicalSHA256Digest(proof.AuthorityDigest) {
		return fmt.Errorf("authority digest must be canonical sha256")
	}
	if proof.AuthorityDigest != PatchMaterializationAuthorityProofDigest(proof) {
		return fmt.Errorf("authority digest mismatch")
	}
	if proof.FileCount != diagnostic.FileCount || len(proof.Files) != len(diagnostic.Files) {
		return fmt.Errorf("file count mismatch")
	}
	for i, file := range diagnostic.Files {
		got := proof.Files[i]
		if patchMaterializationDiagnosticChangeKind(got.ChangeKind) != patchMaterializationDiagnosticChangeKind(file.ChangeKind) {
			return fmt.Errorf("file[%d] change_kind mismatch", i)
		}
		expectedFile := map[string][2]string{
			"path":             {got.Path, file.Path},
			"base_hash":        {got.BaseHash, file.BaseHash},
			"candidate_hash":   {got.CandidateHash, file.CandidateHash},
			"content_encoding": {got.ContentEncoding, file.ContentEncoding},
			"content_digest":   {got.ContentDigest, file.ContentDigest},
		}
		for field, pair := range expectedFile {
			if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
				return fmt.Errorf("file[%d] %s mismatch", i, field)
			}
		}
	}
	return nil
}

func reviewerAdvisoryReady(advisory PatchQueueReviewerAdvisory, item PatchQueueItem) error {
	advisory = clonePatchQueueReviewerAdvisory(advisory)
	required := []struct {
		field string
		value string
	}{
		{"schema", advisory.Schema},
		{"mode", advisory.Mode},
		{"verdict", advisory.Verdict},
		{"reviewer_id", advisory.ReviewerID},
		{"review_doc_key", advisory.ReviewDocKey},
		{"operation_id", advisory.OperationID},
		{"operation_kind", advisory.OperationKind},
		{"cas_patch_digest", advisory.CASPatchDigest},
		{"cas_evaluation_digest", advisory.CASEvaluationDigest},
		{"rollback_evidence_digest", advisory.RollbackEvidenceDigest},
		{"recorded_at", advisory.RecordedAt},
	}
	for _, entry := range required {
		if strings.TrimSpace(entry.value) == "" {
			return fmt.Errorf("reviewer advisory %s is required", entry.field)
		}
	}
	if advisory.Schema != PatchQueueReviewerAdvisorySchema {
		return fmt.Errorf("reviewer advisory schema is %q, not %q", advisory.Schema, PatchQueueReviewerAdvisorySchema)
	}
	if advisory.Mode != MutationActivationReviewerMeshAdvisoryOnly {
		return fmt.Errorf("reviewer advisory mode is %q, not %q", advisory.Mode, MutationActivationReviewerMeshAdvisoryOnly)
	}
	if advisory.Verdict != PatchQueueReviewerAdvisoryVerdictReviewed {
		return fmt.Errorf("reviewer advisory verdict is %q, not %q", advisory.Verdict, PatchQueueReviewerAdvisoryVerdictReviewed)
	}
	if _, err := time.Parse(time.RFC3339Nano, advisory.RecordedAt); err != nil {
		return fmt.Errorf("reviewer advisory recorded_at must be RFC3339Nano: %v", err)
	}
	expected := []struct {
		field string
		want  string
		got   string
	}{
		{"operation_id", item.OperationID, advisory.OperationID},
		{"operation_kind", item.OperationKind, advisory.OperationKind},
		{"cas_patch_digest", item.CASPatchDigest, advisory.CASPatchDigest},
		{"cas_evaluation_digest", item.CASEvaluationDigest, advisory.CASEvaluationDigest},
		{"rollback_evidence_digest", item.RollbackEvidenceDigest, advisory.RollbackEvidenceDigest},
	}
	if strings.TrimSpace(item.ReviewDocKey) != "" {
		expected = append(expected, struct {
			field string
			want  string
			got   string
		}{"review_doc_key", item.ReviewDocKey, advisory.ReviewDocKey})
	}
	for _, entry := range expected {
		if strings.TrimSpace(entry.want) != strings.TrimSpace(entry.got) {
			return fmt.Errorf("reviewer advisory %s does not match patch queue item", entry.field)
		}
	}
	if strings.TrimSpace(item.ReviewerAdvisoryDigest) != "" && item.ReviewerAdvisoryDigest != digestPatchQueueReviewerAdvisory(advisory) {
		return fmt.Errorf("reviewer advisory digest does not match patch queue item")
	}
	return nil
}

func operatorEnablementReady(enablement PatchQueueOperatorEnablement, item PatchQueueItem, advisory PatchQueueReviewerAdvisory) error {
	enablement = clonePatchQueueOperatorEnablement(enablement)
	required := []struct {
		field string
		value string
	}{
		{"schema", enablement.Schema},
		{"scope", enablement.Scope},
		{"enabled_by", enablement.EnabledBy},
		{"enabled_at", enablement.EnabledAt},
		{"reason", enablement.Reason},
		{"workspace_id", enablement.WorkspaceID},
		{"queue_id", enablement.QueueID},
		{"item_id", enablement.ItemID},
		{"operation_id", enablement.OperationID},
		{"cas_patch_digest", enablement.CASPatchDigest},
		{"rollback_evidence_digest", enablement.RollbackEvidenceDigest},
		{"reviewer_advisory_digest", enablement.ReviewerAdvisoryDigest},
	}
	for _, entry := range required {
		if strings.TrimSpace(entry.value) == "" {
			return fmt.Errorf("operator enablement %s is required", entry.field)
		}
	}
	if enablement.Schema != PatchQueueOperatorEnablementSchema {
		return fmt.Errorf("operator enablement schema is %q, not %q", enablement.Schema, PatchQueueOperatorEnablementSchema)
	}
	if enablement.Scope != MutationActivationOperatorEnablementScope {
		return fmt.Errorf("operator enablement scope is %q, not %q", enablement.Scope, MutationActivationOperatorEnablementScope)
	}
	if !enablement.Enabled {
		return fmt.Errorf("operator enablement must be explicitly enabled")
	}
	if _, err := time.Parse(time.RFC3339Nano, enablement.EnabledAt); err != nil {
		return fmt.Errorf("operator enablement enabled_at must be RFC3339Nano: %v", err)
	}
	advisoryDigest := digestPatchQueueReviewerAdvisory(advisory)
	expected := []struct {
		field string
		want  string
		got   string
	}{
		{"workspace_id", item.WorkspaceID, enablement.WorkspaceID},
		{"queue_id", item.QueueID, enablement.QueueID},
		{"item_id", item.ItemID, enablement.ItemID},
		{"operation_id", item.OperationID, enablement.OperationID},
		{"cas_patch_digest", item.CASPatchDigest, enablement.CASPatchDigest},
		{"rollback_evidence_digest", item.RollbackEvidenceDigest, enablement.RollbackEvidenceDigest},
		{"reviewer_advisory_digest", advisoryDigest, enablement.ReviewerAdvisoryDigest},
	}
	if strings.TrimSpace(item.ProjectID) != "" {
		expected = append(expected, struct {
			field string
			want  string
			got   string
		}{"project_id", item.ProjectID, enablement.ProjectID})
	}
	for _, entry := range expected {
		if strings.TrimSpace(entry.want) != strings.TrimSpace(entry.got) {
			return fmt.Errorf("operator enablement %s does not match patch queue item", entry.field)
		}
	}
	if strings.TrimSpace(item.OperatorEnablementDigest) != "" && item.OperatorEnablementDigest != digestPatchQueueOperatorEnablement(enablement) {
		return fmt.Errorf("operator enablement digest does not match patch queue item")
	}
	return nil
}

func ValidatePatchQueueReviewerAdvisory(evidence PatchQueueReviewerAdvisory, item PatchQueueItem) error {
	return reviewerAdvisoryReady(evidence, item)
}

func ValidatePatchQueueOperatorEnablement(evidence PatchQueueOperatorEnablement, item PatchQueueItem, advisory PatchQueueReviewerAdvisory) error {
	return operatorEnablementReady(evidence, item, advisory)
}

func worktreeIdentityReady(evidence WorktreeIdentityEvidence) bool {
	for _, value := range []string{
		evidence.RepoID,
		evidence.CheckoutID,
		evidence.BranchID,
		evidence.BranchName,
		evidence.MachineID,
		evidence.LocalPath,
		evidence.BaseSHA,
		evidence.HeadSHA,
	} {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	if !isCanonicalGitObjectID(evidence.BaseSHA) || !isCanonicalGitObjectID(evidence.HeadSHA) {
		return false
	}
	if strings.TrimSpace(evidence.ReadbackState) != "ok" {
		return false
	}
	if strings.TrimSpace(evidence.ObservedWorktreeRoot) == "" || !mutationActivationPathsEqual(evidence.ObservedWorktreeRoot, evidence.LocalPath) {
		return false
	}
	if strings.TrimSpace(evidence.ObservedDirtyState) != "clean" {
		return false
	}
	if strings.TrimSpace(evidence.ObservedBranchName) != strings.TrimSpace(evidence.BranchName) {
		return false
	}
	if strings.TrimSpace(evidence.ObservedHeadSHA) != strings.TrimSpace(evidence.HeadSHA) {
		return false
	}
	return isCanonicalGitObjectID(evidence.ObservedHeadSHA)
}

func actuatorTargetWorktreeIdentityReady(source WorktreeIdentityEvidence, target WorktreeIdentityEvidence) bool {
	if !worktreeIdentityReady(source) {
		return false
	}
	if !worktreeIdentityReady(target) {
		return false
	}
	if strings.TrimSpace(source.RepoID) == "" || strings.TrimSpace(source.RepoID) != strings.TrimSpace(target.RepoID) {
		return false
	}
	if strings.TrimSpace(source.CheckoutID) != "" && strings.TrimSpace(source.CheckoutID) == strings.TrimSpace(target.CheckoutID) {
		return false
	}
	if strings.TrimSpace(source.LocalPath) != "" && mutationActivationPathsEqual(source.LocalPath, target.LocalPath) {
		return false
	}
	if strings.TrimSpace(source.BranchName) != "" && strings.TrimSpace(source.BranchName) == strings.TrimSpace(target.BranchName) {
		return false
	}
	if strings.TrimSpace(source.BaseSHA) == "" ||
		strings.TrimSpace(target.BaseSHA) != strings.TrimSpace(source.BaseSHA) ||
		strings.TrimSpace(target.HeadSHA) != strings.TrimSpace(source.BaseSHA) {
		return false
	}
	return true
}

func mutationActivationPathsEqual(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return left == right
	}
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func isCanonicalGitObjectID(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func mutationBindingReady(authority Context, item PatchQueueItem) error {
	authority = authority.WithDefaults()
	if err := requireConcreteMutationOperationRefs(authority); err != nil {
		return err
	}
	if len(authority.Pathset) == 0 {
		return fmt.Errorf("pathset is required")
	}
	if _, err := authority.Digest(); err != nil {
		return fmt.Errorf("repo authority context: %w", err)
	}
	if strings.TrimSpace(item.Schema) != PatchQueueItemSchemaVersion {
		return fmt.Errorf("patch queue item schema is required")
	}
	if err := verifyPatchQueueItemContext(item, authority); err != nil {
		return err
	}
	if strings.TrimSpace(item.OperationID) != strings.TrimSpace(authority.Operation.ID) ||
		strings.TrimSpace(item.OperationKind) != strings.TrimSpace(authority.Operation.Kind) {
		return fmt.Errorf("patch queue item operation refs must match mutation context")
	}
	return nil
}

func mergeAdmissionReady(authority Context, item PatchQueueItem) error {
	authority = authority.WithDefaults()
	if item.State != PatchQueueStateApplied {
		return fmt.Errorf("patch queue state is %q, not %q", item.State, PatchQueueStateApplied)
	}
	if err := validateCASEvidence(item.CASResult, authority); err != nil {
		return err
	}
	if strings.TrimSpace(item.OperationID) != strings.TrimSpace(authority.Operation.ID) || strings.TrimSpace(item.OperationKind) != strings.TrimSpace(authority.Operation.Kind) {
		return fmt.Errorf("patch queue operation refs must match mutation context")
	}
	if item.CASResult.Status != CASPatchStatusApplied {
		return fmt.Errorf("CAS status is %q, not %q", item.CASResult.Status, CASPatchStatusApplied)
	}
	if !isCanonicalSHA256Digest(item.CASPatchDigest) {
		return fmt.Errorf("canonical CAS patch digest is required")
	}
	if strings.TrimSpace(item.CASResult.PatchDigest) != strings.TrimSpace(item.CASPatchDigest) {
		return fmt.Errorf("CAS patch digest mismatch")
	}
	if err := verifyCASPatchDigestFromPathResults(item.CASResult); err != nil {
		return err
	}
	if !isCanonicalSHA256Digest(item.CASEvaluationDigest) {
		return fmt.Errorf("canonical CAS evaluation digest is required")
	}
	if digestCASPatchApplyResult(item.CASResult) != item.CASEvaluationDigest {
		return fmt.Errorf("CAS evaluation digest mismatch")
	}
	if len(item.ConflictIssues) > 0 {
		return fmt.Errorf("conflict issues must be empty")
	}
	if err := patchQueueVerificationEvidenceReady(item.TestEvidence, item.TestEvidenceDigest); err != nil {
		return err
	}
	return nil
}

func ValidatePatchQueueMergeAdmission(authority Context, item PatchQueueItem) error {
	return mergeAdmissionReady(authority, item)
}

func ValidatePatchQueueRollbackEvidence(evidence PatchQueueRollback, item PatchQueueItem) error {
	return rollbackEvidenceReady(evidence, item)
}

func boundedRetryReady(item PatchQueueItem) error {
	if item.MaxAttempts <= 0 {
		return fmt.Errorf("max_attempts must be positive")
	}
	if item.Attempt <= 0 {
		return fmt.Errorf("attempt must be positive")
	}
	if item.Attempt > item.MaxAttempts {
		return fmt.Errorf("attempt %d exceeds max_attempts %d", item.Attempt, item.MaxAttempts)
	}
	return nil
}

func patchQueueVerificationEvidenceReady(evidence PatchQueueTestEvidence, evidenceDigest string) error {
	if strings.TrimSpace(evidence.Schema) != PatchQueueTestEvidenceSchemaVersion {
		return fmt.Errorf("test evidence schema is required")
	}
	for field, value := range map[string]string{
		"test_name":            evidence.Name,
		"test_command":         evidence.Command,
		"test_status":          evidence.Status,
		"test_output_digest":   evidence.OutputDigest,
		"test_evidence_digest": evidenceDigest,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	if evidence.Status != PatchQueueTestStatusPassed {
		return fmt.Errorf("test evidence status is %q, not %q", evidence.Status, PatchQueueTestStatusPassed)
	}
	if evidence.ExitCode != 0 {
		return fmt.Errorf("test evidence exit code must be 0")
	}
	if !isCanonicalSHA256Digest(evidence.OutputDigest) {
		return fmt.Errorf("test evidence output digest must be canonical sha256")
	}
	if !isCanonicalSHA256Digest(evidenceDigest) {
		return fmt.Errorf("test evidence digest must be canonical sha256")
	}
	if digestPatchQueueTestEvidence(evidence) != evidenceDigest {
		return fmt.Errorf("test evidence digest mismatch")
	}
	return nil
}

func rollbackEvidenceReady(evidence PatchQueueRollback, item PatchQueueItem) error {
	if strings.TrimSpace(evidence.Schema) != PatchQueueRollbackSchemaVersion {
		return fmt.Errorf("rollback evidence schema is required")
	}
	for field, value := range map[string]string{
		"source_operation_id":        evidence.SourceOperationID,
		"source_operation_kind":      evidence.SourceOperationKind,
		"rollback_operation_id":      evidence.RollbackOperationID,
		"rollback_operation_kind":    evidence.RollbackOperationKind,
		"reason":                     evidence.Reason,
		"source_patch_digest":        evidence.SourcePatchDigest,
		"rollback_patch_digest":      evidence.RollbackPatchDigest,
		"verification_command":       evidence.VerificationCommand,
		"verification_status":        evidence.VerificationStatus,
		"verification_output_digest": evidence.VerificationOutputDigest,
		"recorded_at":                evidence.RecordedAt,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("rollback evidence %s is required", field)
		}
	}
	if evidence.SourceOperationID == evidence.RollbackOperationID {
		return fmt.Errorf("rollback operation must be distinct from source operation")
	}
	if evidence.SourceOperationID != item.OperationID || evidence.SourceOperationKind != item.OperationKind {
		return fmt.Errorf("rollback source operation must match patch queue item")
	}
	if evidence.SourcePatchDigest != item.CASPatchDigest {
		return fmt.Errorf("rollback source patch digest must match patch queue item")
	}
	if !isCanonicalSHA256Digest(evidence.SourcePatchDigest) {
		return fmt.Errorf("rollback source patch digest must be canonical sha256")
	}
	if !isCanonicalSHA256Digest(evidence.RollbackPatchDigest) {
		return fmt.Errorf("rollback patch digest must be canonical sha256")
	}
	if evidence.VerificationStatus != PatchQueueTestStatusPassed {
		return fmt.Errorf("rollback verification status is %q, not %q", evidence.VerificationStatus, PatchQueueTestStatusPassed)
	}
	if evidence.VerificationExitCode != 0 {
		return fmt.Errorf("rollback verification exit code must be 0")
	}
	if !isCanonicalSHA256Digest(evidence.VerificationOutputDigest) {
		return fmt.Errorf("rollback verification output digest must be canonical sha256")
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(evidence.RecordedAt)); err != nil {
		return fmt.Errorf("rollback evidence recorded_at must be RFC3339Nano: %v", err)
	}
	if evidence.VerificationDurationMillis < 0 {
		return fmt.Errorf("rollback verification duration must be non-negative")
	}
	if len(evidence.RollbackPaths) == 0 {
		return fmt.Errorf("rollback paths are required")
	}
	_, rollbackPatchDigest, err := normalizePatchQueueRollbackPaths(evidence.RollbackPaths, item)
	if err != nil {
		return err
	}
	if evidence.RollbackPatchDigest != rollbackPatchDigest {
		return fmt.Errorf("rollback patch digest mismatch")
	}
	return nil
}

func digestMutationActivationGateResult(result MutationActivationGateResult) string {
	result.Digest = ""
	raw, _ := json.Marshal(result)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
