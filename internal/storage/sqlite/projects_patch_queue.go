package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
)

const (
	ProjectPatchQueueAuthorityModePatchOnly       = repoauthority.ModePatchOnlyTempRepo
	ProjectPatchQueueAuthorityModeControlledQueue = repoauthority.ModeControlledQueue
	ProjectPatchQueueOperationBindingSchema       = "project_patch_queue_operation_binding.v1"
	ProjectPatchQueueCASEvidenceSchema            = "project_patch_queue_cas_evidence.v1"
	ProjectPatchQueueMaterializationSchema        = repoauthority.PatchMaterializationSchemaVersion
	ProjectPatchQueueRollbackEvidenceSchema       = "project_patch_queue_rollback_evidence.v1"
	ProjectPatchQueueReviewerAdvisorySchema       = "project_patch_queue_reviewer_advisory.v1"
	ProjectPatchQueueOperatorEnablementSchema     = "project_patch_queue_operator_enablement.v1"
	ProjectPatchQueueOperationLedgerSchema        = "operation_ledger.v1"
	ProjectPatchQueueOperationKindRepoPatchApply  = "repo_patch_apply"
	ProjectPatchQueueActuatorStartedEventType     = "project.patch_queue.actuator_started"
	ProjectPatchQueueActuatorAppliedEventType     = "project.patch_queue.actuator_applied"
	ProjectPatchQueueIntegrationAdmittedEventType = "project.patch_queue.integration_admitted"
	ProjectPatchQueueIntegratedEventType          = "project.patch_queue.integrated"
	ProjectPatchQueueIntegrationRepairEventType   = "project.patch_queue.integration_repair"
	ProjectPatchQueueActuatorActorID              = "repo_mutation_actuator"
	ProjectPatchQueueIntegrationModeMaterialized  = "materialization"
	ProjectPatchQueueIntegrationModeDirectMerge   = "direct_merge"
	ProjectPatchQueueIntegrationOutcomeAdmitted   = "admitted"
	ProjectPatchQueueIntegrationOutcomeIntegrated = "integrated"
	ProjectPatchQueueIntegrationOutcomeRepair     = "repair"

	ProjectPatchQueueMaterializationMaxFiles                   = repoauthority.PatchMaterializationMaxFiles
	ProjectPatchQueueMaterializationMaxFileBytes               = repoauthority.PatchMaterializationMaxFileBytes
	ProjectPatchQueueMaterializationMaxTotalBytes              = repoauthority.PatchMaterializationMaxTotalBytes
	ProjectPatchQueueMaterializationMaxJSONBytes               = int64(8 << 20)
	ProjectPatchQueueMaterializationMaxAuthorityProofJSONBytes = int64(1 << 20)

	ProjectPatchQueueStateProposed   = "PROPOSED"
	ProjectPatchQueueStateClaimed    = "CLAIMED"
	ProjectPatchQueueStateAccepted   = "ACCEPTED"
	ProjectPatchQueueStateIntegrated = "INTEGRATED"
	ProjectPatchQueueStateRejected   = "REJECTED"
	ProjectPatchQueueStateBlocked    = "BLOCKED"
	ProjectPatchQueueStateCanceled   = "CANCELED"
)

var ErrProjectPatchQueueInvalid = errors.New("project patch queue invalid")

type ProjectPatchQueueItemRecord struct {
	QueueID                             string                                           `json:"queue_id"`
	ItemID                              string                                           `json:"item_id"`
	WorkspaceID                         string                                           `json:"workspace_id"`
	ProjectID                           string                                           `json:"project_id"`
	RepoID                              string                                           `json:"repo_id"`
	BranchID                            string                                           `json:"branch_id"`
	ReviewDocKey                        string                                           `json:"review_doc_key"`
	SupersedesQueueID                   string                                           `json:"supersedes_queue_id,omitempty"`
	SupersedesItemID                    string                                           `json:"supersedes_item_id,omitempty"`
	EvidenceDocKey                      string                                           `json:"evidence_doc_key,omitempty"`
	RepoAuthorityMode                   string                                           `json:"repo_authority_mode"`
	State                               string                                           `json:"state"`
	Attempt                             int                                              `json:"attempt"`
	MaxAttempts                         int                                              `json:"max_attempts"`
	NextRetryAt                         string                                           `json:"next_retry_at,omitempty"`
	DeadLetteredAt                      string                                           `json:"dead_lettered_at,omitempty"`
	Pathset                             []string                                         `json:"pathset,omitempty"`
	PathsetJSON                         string                                           `json:"pathset_json,omitempty"`
	BaseRef                             string                                           `json:"base_ref,omitempty"`
	BaseSHA                             string                                           `json:"base_sha,omitempty"`
	HeadSHA                             string                                           `json:"head_sha,omitempty"`
	AutoMerge                           bool                                             `json:"auto_merge"`
	SubmittedBy                         string                                           `json:"submitted_by,omitempty"`
	TaskID                              string                                           `json:"task_id,omitempty"`
	SessionID                           string                                           `json:"session_id,omitempty"`
	RunID                               string                                           `json:"run_id,omitempty"`
	AgentID                             string                                           `json:"agent_id,omitempty"`
	PrincipalType                       string                                           `json:"principal_type,omitempty"`
	PrincipalID                         string                                           `json:"principal_id,omitempty"`
	CapabilitySnapshotID                string                                           `json:"capability_snapshot_id,omitempty"`
	CapabilitySnapshotSchema            string                                           `json:"capability_snapshot_schema,omitempty"`
	RepoRoot                            string                                           `json:"repo_root,omitempty"`
	BaseTreeHash                        string                                           `json:"base_tree_hash,omitempty"`
	BaseFileHashes                      map[string]string                                `json:"base_file_hashes,omitempty"`
	BaseFileHashesJSON                  string                                           `json:"base_file_hashes_json,omitempty"`
	ContextDigest                       string                                           `json:"context_digest,omitempty"`
	RepoLeaseID                         string                                           `json:"repo_lease_id,omitempty"`
	LeaseTerm                           int64                                            `json:"lease_term,omitempty"`
	OperationID                         string                                           `json:"operation_id,omitempty"`
	OperationKind                       string                                           `json:"operation_kind,omitempty"`
	OperationBindingSchema              string                                           `json:"operation_binding_schema,omitempty"`
	OperationBindingAccepted            bool                                             `json:"operation_binding_accepted,omitempty"`
	OperationContextDigest              string                                           `json:"operation_context_digest,omitempty"`
	OperationLeaseContextDigest         string                                           `json:"operation_lease_context_digest,omitempty"`
	OperationMutationPaths              []string                                         `json:"operation_mutation_paths,omitempty"`
	OperationMutationPathsJSON          string                                           `json:"operation_mutation_paths_json,omitempty"`
	OperationBoundBy                    string                                           `json:"operation_bound_by,omitempty"`
	OperationBoundAt                    string                                           `json:"operation_bound_at,omitempty"`
	CASEvidenceSchema                   string                                           `json:"cas_evidence_schema,omitempty"`
	CASEvidenceAccepted                 bool                                             `json:"cas_evidence_accepted,omitempty"`
	CASStatus                           string                                           `json:"cas_status,omitempty"`
	CASPatchDigest                      string                                           `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest                 string                                           `json:"cas_evaluation_digest,omitempty"`
	CASResult                           repoauthority.CASPatchApplyResult                `json:"cas_result,omitempty"`
	CASResultJSON                       string                                           `json:"cas_result_json,omitempty"`
	CASTestEvidence                     repoauthority.PatchQueueTestEvidence             `json:"cas_test_evidence,omitempty"`
	CASTestEvidenceJSON                 string                                           `json:"cas_test_evidence_json,omitempty"`
	CASTestEvidenceDigest               string                                           `json:"cas_test_evidence_digest,omitempty"`
	CASRecordedBy                       string                                           `json:"cas_recorded_by,omitempty"`
	CASRecordedAt                       string                                           `json:"cas_recorded_at,omitempty"`
	MaterializationSchema               string                                           `json:"materialization_schema,omitempty"`
	MaterializationAccepted             bool                                             `json:"materialization_accepted,omitempty"`
	Materialization                     repoauthority.PatchMaterialization               `json:"materialization,omitempty"`
	MaterializationJSON                 string                                           `json:"materialization_json,omitempty"`
	MaterializationDigest               string                                           `json:"materialization_digest,omitempty"`
	MaterializationRecordedBy           string                                           `json:"materialization_recorded_by,omitempty"`
	MaterializationRecordedAt           string                                           `json:"materialization_recorded_at,omitempty"`
	MaterializationAuthorityProof       repoauthority.PatchMaterializationAuthorityProof `json:"materialization_authority_proof,omitempty"`
	MaterializationAuthorityProofJSON   string                                           `json:"materialization_authority_proof_json,omitempty"`
	MaterializationAuthorityProofDigest string                                           `json:"materialization_authority_proof_digest,omitempty"`
	RollbackEvidenceSchema              string                                           `json:"rollback_evidence_schema,omitempty"`
	RollbackEvidenceAccepted            bool                                             `json:"rollback_evidence_accepted,omitempty"`
	RollbackEvidence                    repoauthority.PatchQueueRollback                 `json:"rollback_evidence,omitempty"`
	RollbackEvidenceJSON                string                                           `json:"rollback_evidence_json,omitempty"`
	RollbackEvidenceDigest              string                                           `json:"rollback_evidence_digest,omitempty"`
	RollbackRecordedBy                  string                                           `json:"rollback_recorded_by,omitempty"`
	RollbackRecordedAt                  string                                           `json:"rollback_recorded_at,omitempty"`
	ReviewerAdvisorySchema              string                                           `json:"reviewer_advisory_schema,omitempty"`
	ReviewerAdvisoryAccepted            bool                                             `json:"reviewer_advisory_accepted,omitempty"`
	ReviewerAdvisory                    repoauthority.PatchQueueReviewerAdvisory         `json:"reviewer_advisory,omitempty"`
	ReviewerAdvisoryJSON                string                                           `json:"reviewer_advisory_json,omitempty"`
	ReviewerAdvisoryDigest              string                                           `json:"reviewer_advisory_digest,omitempty"`
	ReviewerRecordedBy                  string                                           `json:"reviewer_recorded_by,omitempty"`
	ReviewerRecordedAt                  string                                           `json:"reviewer_recorded_at,omitempty"`
	OperatorEnablementSchema            string                                           `json:"operator_enablement_schema,omitempty"`
	OperatorEnablementAccepted          bool                                             `json:"operator_enablement_accepted,omitempty"`
	OperatorEnablement                  repoauthority.PatchQueueOperatorEnablement       `json:"operator_enablement,omitempty"`
	OperatorEnablementJSON              string                                           `json:"operator_enablement_json,omitempty"`
	OperatorEnablementDigest            string                                           `json:"operator_enablement_digest,omitempty"`
	OperatorEnabledBy                   string                                           `json:"operator_enabled_by,omitempty"`
	OperatorEnabledAt                   string                                           `json:"operator_enabled_at,omitempty"`
	ClaimedBy                           string                                           `json:"claimed_by,omitempty"`
	ClaimToken                          string                                           `json:"claim_token,omitempty"`
	ClaimedAt                           string                                           `json:"claimed_at,omitempty"`
	ClaimExpiresAt                      string                                           `json:"claim_expires_at,omitempty"`
	DecisionDocKey                      string                                           `json:"decision_doc_key,omitempty"`
	DecisionSummary                     string                                           `json:"decision_summary,omitempty"`
	DecidedBy                           string                                           `json:"decided_by,omitempty"`
	DecidedAt                           string                                           `json:"decided_at,omitempty"`
	CreatedAt                           string                                           `json:"created_at"`
	UpdatedAt                           string                                           `json:"updated_at"`
	ReviewTaskID                        string                                           `json:"review_task_id,omitempty"`
	ReviewTaskStatus                    string                                           `json:"review_task_status,omitempty"`
	ReviewTaskEventID                   string                                           `json:"review_task_event_id,omitempty"`
	MissingReviewTask                   bool                                             `json:"missing_review_task,omitempty"`
}

type ProjectRepoMutationActivationCandidate struct {
	QueueItem      ProjectPatchQueueItemRecord `json:"queue_item"`
	Repository     ProjectRepositoryRecord     `json:"repository,omitempty"`
	Branch         ProjectBranchRecord         `json:"branch,omitempty"`
	Checkout       ProjectCheckoutRecord       `json:"checkout,omitempty"`
	TargetCheckout ProjectCheckoutRecord       `json:"target_checkout,omitempty"`
}

func ProjectPatchQueueReviewTaskID(item ProjectPatchQueueItemRecord) string {
	stem := strings.Join([]string{
		projectPatchQueueReviewTaskIDPart(item.ProjectID),
		projectPatchQueueReviewTaskIDPart(item.QueueID),
		projectPatchQueueReviewTaskIDPart(firstNonEmpty(item.ItemID, item.BranchID)),
	}, "-")
	stem = strings.Trim(stem, "-")
	if stem == "" {
		stem = projectPatchQueueReviewTaskIDPart(firstNonEmpty(item.BranchID, item.ItemID, item.QueueID))
	}
	if stem == "" {
		stem = "unknown"
	}
	digestInput := strings.Join([]string{item.WorkspaceID, item.ProjectID, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA}, "\x00")
	sum := sha256.Sum256([]byte(digestInput))
	digest := fmt.Sprintf("%x", sum)[:16]
	maxStem := 220 - len("task-review-") - len(digest) - 1
	if len(stem) > maxStem {
		stem = strings.Trim(stem[:maxStem], "-")
	}
	return "task-review-" + stem + "-" + digest
}

func ProjectPatchQueueReviewTaskIDWithAttempt(base string, attempt int) string {
	suffix := fmt.Sprintf("-r%d", attempt)
	maxBase := 240 - len(suffix)
	if len(base) > maxBase {
		base = strings.Trim(base[:maxBase], "-")
	}
	return base + suffix
}

func projectPatchQueueReviewTaskIDPart(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.' {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

type ProjectPatchQueueSubmitInput struct {
	WorkspaceID              string
	ProjectID                string
	QueueID                  string
	ItemID                   string
	RepoID                   string
	BranchID                 string
	ReviewDocKey             string
	SupersedesQueueID        string
	SupersedesItemID         string
	EvidenceDocKey           string
	RepoAuthorityMode        string
	PathsetJSON              string
	BaseRef                  string
	BaseSHA                  string
	HeadSHA                  string
	AutoMerge                bool
	ActorID                  string
	ActorType                string
	TaskID                   string
	SessionID                string
	RunID                    string
	AgentID                  string
	PrincipalType            string
	PrincipalID              string
	CapabilitySnapshotID     string
	CapabilitySnapshotSchema string
	RepoRoot                 string
	BaseTreeHash             string
	BaseFileHashes           map[string]string
	BaseFileHashesJSON       string
	ContextDigest            string
	RepoLeaseID              string
	LeaseTerm                int64
	OperationID              string
	OperationKind            string
	MaxAttempts              int

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueSupersedeInput struct {
	WorkspaceID              string
	ProjectID                string
	QueueID                  string
	ItemID                   string
	NewItemID                string
	EvidenceDocKey           string
	ActorID                  string
	ActorType                string
	TaskID                   string
	SessionID                string
	RunID                    string
	AgentID                  string
	PrincipalType            string
	PrincipalID              string
	CapabilitySnapshotID     string
	CapabilitySnapshotSchema string
	RepoRoot                 string
	BaseTreeHash             string
	BaseFileHashes           map[string]string
	BaseFileHashesJSON       string
	ContextDigest            string
	RepoLeaseID              string
	LeaseTerm                int64
	MaxAttempts              int

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueOperationBindInput struct {
	WorkspaceID       string
	ProjectID         string
	QueueID           string
	ItemID            string
	OperationID       string
	OperationKind     string
	MutationPathsJSON string
	ClaimToken        string
	ActorID           string
	ActorType         string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueCASRecordInput struct {
	WorkspaceID  string
	ProjectID    string
	QueueID      string
	ItemID       string
	CASResult    repoauthority.CASPatchApplyResult
	TestEvidence repoauthority.PatchQueueTestEvidence
	ClaimToken   string
	ActorID      string
	ActorType    string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueMaterializationRecordInput struct {
	WorkspaceID     string
	ProjectID       string
	QueueID         string
	ItemID          string
	Materialization repoauthority.PatchMaterialization
	ClaimToken      string
	ActorID         string
	ActorType       string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueActuatorResultRecordInput struct {
	WorkspaceID string
	ProjectID   string
	QueueID     string
	ItemID      string
	Result      repoauthority.MutationActuatorLiveResult
	ActorID     string
	ActorType   string
}

type ProjectPatchQueueIntegrationRecordInput struct {
	WorkspaceID           string
	ProjectID             string
	QueueID               string
	ItemID                string
	ActorID               string
	ActorType             string
	Outcome               string
	IntegrationMode       string
	RepoID                string
	SourceBranchID        string
	SourceHeadSHA         string
	TargetBranch          string
	TargetHeadBefore      string
	TargetHeadAfter       string
	RemoteTargetHeadAfter string
	MergePerformed        bool
	PushAttempted         bool
	PushSucceeded         bool
	AlreadyIntegrated     bool
	RepairReason          string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueRollbackRecordInput struct {
	WorkspaceID      string
	ProjectID        string
	QueueID          string
	ItemID           string
	RollbackEvidence repoauthority.PatchQueueRollback
	ClaimToken       string
	ActorID          string
	ActorType        string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueReviewerAdvisoryRecordInput struct {
	WorkspaceID      string
	ProjectID        string
	QueueID          string
	ItemID           string
	ReviewerAdvisory repoauthority.PatchQueueReviewerAdvisory
	ClaimToken       string
	ActorID          string
	ActorType        string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueOperatorEnablementRecordInput struct {
	WorkspaceID        string
	ProjectID          string
	QueueID            string
	ItemID             string
	OperatorEnablement repoauthority.PatchQueueOperatorEnablement
	ClaimToken         string
	ActorID            string
	ActorType          string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueListFilter struct {
	WorkspaceID string
	ProjectID   string
	RepoID      string
	BranchID    string
	State       string
}

const projectPatchQueueItemSelectColumns = `
       queue_id, item_id, workspace_id, project_id, repo_id, branch_id, review_doc_key,
       supersedes_queue_id, supersedes_item_id, evidence_doc_key,
       repo_authority_mode, state, attempt, max_attempts, next_retry_at, dead_lettered_at,
       pathset_json, base_ref, base_sha, head_sha,
       auto_merge, submitted_by, task_id, session_id, run_id, agent_id, principal_type,
       principal_id, capability_snapshot_id, capability_snapshot_schema, repo_root,
       base_tree_hash, base_file_hashes_json, context_digest, repo_lease_id, lease_term,
       operation_id, operation_kind, operation_binding_schema, operation_binding_accepted,
       operation_context_digest, operation_lease_context_digest, operation_mutation_paths_json,
       operation_bound_by, operation_bound_at, cas_evidence_schema, cas_evidence_accepted,
       cas_status, cas_patch_digest, cas_evaluation_digest, cas_result_json,
       cas_test_evidence_json, cas_test_evidence_digest, cas_recorded_by, cas_recorded_at,
       materialization_schema, materialization_accepted, materialization_json,
       materialization_digest, materialization_recorded_by, materialization_recorded_at,
       materialization_authority_proof_json, materialization_authority_proof_digest,
       rollback_evidence_schema, rollback_evidence_accepted, rollback_evidence_json,
       rollback_evidence_digest, rollback_recorded_by, rollback_recorded_at,
       reviewer_advisory_schema, reviewer_advisory_accepted, reviewer_advisory_json,
       reviewer_advisory_digest, reviewer_recorded_by, reviewer_recorded_at,
       operator_enablement_schema, operator_enablement_accepted, operator_enablement_json,
       operator_enablement_digest, operator_enabled_by, operator_enabled_at,
       claimed_by, claim_token, claimed_at, claim_expires_at,
       decision_doc_key, decision_summary, decided_by, decided_at, created_at, updated_at`

type ProjectPatchQueueClaimInput struct {
	WorkspaceID  string
	ProjectID    string
	QueueID      string
	ItemID       string
	ClaimToken   string
	LeaseSeconds int
	ActorID      string
	ActorType    string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueReleaseInput struct {
	WorkspaceID string
	ProjectID   string
	QueueID     string
	ItemID      string
	ClaimToken  string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type projectPatchQueueClaimReleaseReport struct {
	Released []map[string]any `json:"released,omitempty"`
	Skipped  []map[string]any `json:"skipped,omitempty"`
}

func (r projectPatchQueueClaimReleaseReport) empty() bool {
	return len(r.Released) == 0 && len(r.Skipped) == 0
}

type projectPatchQueueTaskClaimReleaseIdentity struct {
	ProjectID string
	QueueID   string
	ItemID    string
	BranchID  string
	HeadSHA   string
	TaskKind  string
}

type ProjectPatchQueueDecisionInput struct {
	WorkspaceID          string
	ProjectID            string
	QueueID              string
	ItemID               string
	Decision             string
	DecisionDocKey       string
	DecisionSummary      string
	CheckedSourceDocKeys []string
	ClaimToken           string
	ActorID              string
	ActorType            string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPatchQueueDecisionContinuationRecord struct {
	OutboxID           string `json:"outbox_id"`
	WorkspaceID        string `json:"workspace_id"`
	ProjectID          string `json:"project_id"`
	QueueID            string `json:"queue_id"`
	ItemID             string `json:"item_id"`
	BranchID           string `json:"branch_id,omitempty"`
	HeadSHA            string `json:"head_sha,omitempty"`
	Decision           string `json:"decision"`
	FollowupKind       string `json:"followup_kind"`
	ContinuationTaskID string `json:"continuation_task_id,omitempty"`
	State              string `json:"state"`
	DecisionEventID    string `json:"decision_event_id"`
	DecisionDocKey     string `json:"decision_doc_key,omitempty"`
	DecisionSummary    string `json:"decision_summary,omitempty"`
	PayloadJSON        string `json:"payload_json"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
}

type ProjectPatchQueueDecisionContinuationFilter struct {
	WorkspaceID  string
	ProjectID    string
	QueueID      string
	ItemID       string
	State        string
	FollowupKind string
}

type ProjectPatchQueueReviewTaskReconcileInput struct {
	WorkspaceID string
	ProjectID   string
	QueueID     string
	ItemID      string
	ActorID     string
	ActorType   string
}

type ProjectPatchQueueDecisionContinuationConsumeInput struct {
	WorkspaceID string
	ProjectID   string
	OutboxID    string
	QueueID     string
	ItemID      string
	ActorID     string
	ActorType   string
}

func (s *Store) SubmitProjectPatchQueueItemWithEvent(ctx context.Context, input ProjectPatchQueueSubmitInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	repoID := strings.TrimSpace(input.RepoID)
	branchID := strings.TrimSpace(input.BranchID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || repoID == "" || branchID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, repo_id, and branch_id are required")
	}
	if actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	if input.AutoMerge {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: auto_merge must remain false", ErrProjectPatchQueueInvalid)
	}
	mode, err := normalizeProjectPatchQueueAuthorityMode(input.RepoAuthorityMode)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	if mode == ProjectPatchQueueAuthorityModePatchOnly && !s.allowLegacyPatchOnlySubmits {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: patch_only_temp_repo proposals are legacy/invalid; submit repoauthority_controlled_queue or cancel+replace an existing legacy item", ErrProjectPatchQueueInvalid)
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, actorID, now); err != nil {
			return err
		}
		repo, err := getProjectRepositoryTx(ctx, tx, workspaceID, projectID, repoID)
		if err != nil {
			return err
		}
		if repo.RepoStatus != ProjectRepositoryStatusReady || strings.TrimSpace(repo.RemoteURL) == "" {
			return fmt.Errorf("%w: repo %s must be READY with remote_url before patch queue submission", ErrProjectPatchQueueInvalid, repoID)
		}
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		branch, ok, err := validateProjectBranchIDScopeTx(ctx, tx, branchID, workspaceID, projectID, repoID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: branch_id %s not found", ErrProjectPatchQueueInvalid, branchID)
		}
		if strings.EqualFold(effectiveActorType, "agent") && strings.TrimSpace(branch.AgentID) != actorID {
			return fmt.Errorf("%w: branch %s belongs to agent %s", ErrProjectPatchQueueInvalid, branchID, strings.TrimSpace(branch.AgentID))
		}
		if branch.Status != ProjectBranchStatusReadyForReview {
			return fmt.Errorf("%w: branch %s must be READY_FOR_REVIEW before patch queue submission", ErrProjectPatchQueueInvalid, branchID)
		}
		branchHeadSHA := strings.TrimSpace(branch.HeadSHA)
		if !isCanonicalProjectGitObjectID(branchHeadSHA) {
			return fmt.Errorf("%w: branch %s must have canonical reviewed head_sha before patch queue submission", ErrProjectPatchQueueInvalid, branchID)
		}
		if submittedHeadSHA := strings.TrimSpace(input.HeadSHA); submittedHeadSHA != "" && submittedHeadSHA != branchHeadSHA {
			return fmt.Errorf("%w: head_sha must match the READY_FOR_REVIEW branch evidence", ErrProjectPatchQueueInvalid)
		}
		branchBaseRef := strings.TrimSpace(branch.BaseBranch)
		if submittedBaseRef := strings.TrimSpace(input.BaseRef); submittedBaseRef != "" && branchBaseRef != "" && submittedBaseRef != branchBaseRef {
			return fmt.Errorf("%w: base_ref must match the READY_FOR_REVIEW branch evidence", ErrProjectPatchQueueInvalid)
		}
		branchBaseSHA := strings.TrimSpace(branch.BaseSHA)
		if !isCanonicalProjectGitObjectID(branchBaseSHA) {
			return fmt.Errorf("%w: branch %s must have canonical reviewed base_sha before patch queue submission", ErrProjectPatchQueueInvalid, branchID)
		}
		if submittedBaseSHA := strings.TrimSpace(input.BaseSHA); submittedBaseSHA != "" && branchBaseSHA != "" && submittedBaseSHA != branchBaseSHA {
			return fmt.Errorf("%w: base_sha must match the READY_FOR_REVIEW branch evidence", ErrProjectPatchQueueInvalid)
		}
		reviewDocKey := firstNonEmpty(strings.TrimSpace(input.ReviewDocKey), strings.TrimSpace(branch.ReviewDocKey))
		if reviewDocKey == "" || reviewDocKey != strings.TrimSpace(branch.ReviewDocKey) {
			return fmt.Errorf("%w: review_doc_key must match the branch review evidence", ErrProjectPatchQueueInvalid)
		}
		if doc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, reviewDocKey); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: review_doc_key %s not found", ErrProjectPatchQueueInvalid, reviewDocKey)
			}
			return err
		} else if doc.ArchivedAt != nil {
			return fmt.Errorf("%w: review_doc_key %s is archived", ErrProjectPatchQueueInvalid, reviewDocKey)
		} else {
			fidelity, err := s.projectSourceFidelityContextTx(ctx, tx, workspaceID, projectID)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
			}
			if err := s.validateProjectSourceFidelityTraceTx(ctx, tx, workspaceID, fidelity); err != nil {
				return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
			}
			if err := validateProjectSourceFidelityReviewDoc(doc, fidelity); err != nil {
				return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
			}
		}
		supersedesQueueID := strings.TrimSpace(input.SupersedesQueueID)
		supersedesItemID := strings.TrimSpace(input.SupersedesItemID)
		evidenceDocKey := strings.TrimSpace(input.EvidenceDocKey)
		pathsetJSON, pathset, err := normalizeProjectPatchQueuePathsetJSON(firstNonEmpty(strings.TrimSpace(input.PathsetJSON), branch.WriteScopeJSON))
		if err != nil {
			return err
		}
		branchPathset := projectBranchReviewScopePaths(branch.WriteScopeJSON)
		if len(branchPathset) == 0 || !writeScopePathsCoveredBy(pathset, branchPathset) {
			return fmt.Errorf("%w: pathset_json cannot widen branch write_scope_json", ErrProjectPatchQueueInvalid)
		}
		if err := s.validateProjectPatchQueueSubmitTaskBindingTx(ctx, tx, branch, strings.TrimSpace(input.TaskID), pathset); err != nil {
			return err
		}
		queueID := firstNonEmpty(strings.TrimSpace(input.QueueID), defaultProjectPatchQueueID(projectID, repoID))
		itemID := firstNonEmpty(strings.TrimSpace(input.ItemID), defaultProjectPatchQueueItemID(branchID))
		existing, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if ok && projectPatchQueueExistingControlledLegacyReplacement(existing, workspaceID, projectID, repoID, branchID, queueID, itemID, supersedesQueueID, supersedesItemID) {
			if evidenceDocKey != "" {
				return fmt.Errorf("%w: legacy patch-only cancel+replace does not accept evidence_doc_key; replacement provenance is supersedes_queue_id/supersedes_item_id plus the cancellation receipt", ErrProjectPatchQueueInvalid)
			}
			if strings.TrimSpace(existing.ContextDigest) == "" {
				return fmt.Errorf("%w: existing legacy patch-only replacement %s/%s is missing controlled binding receipt", ErrProjectPatchQueueInvalid, existing.QueueID, existing.ItemID)
			}
			submittedEvent, eventOK, err := projectPatchQueueRuntimeEventForItemTx(ctx, tx, existing, "project.patch_queue.submitted")
			if err != nil {
				return err
			}
			if !eventOK {
				return fmt.Errorf("%w: existing legacy patch-only replacement %s/%s is missing submitted runtime event receipt", ErrProjectPatchQueueInvalid, existing.QueueID, existing.ItemID)
			}
			item = existing
			event = submittedEvent
			return nil
		}
		legacyReplacement := false
		var legacyReplacementItem ProjectPatchQueueItemRecord
		supersessionValidated := false
		if latest, ok, err := getLatestProjectPatchQueueTerminalItemForBranchHeadTx(ctx, tx, workspaceID, projectID, branchID, branchHeadSHA); err != nil {
			return err
		} else if ok {
			switch strings.ToUpper(strings.TrimSpace(latest.State)) {
			case ProjectPatchQueueStateAccepted:
				return fmt.Errorf("%w: patch queue item %s/%s is already ACCEPTED for this branch/head; integration, rebuild, or explicit supersession must consume that boundary before a fresh item_id is allowed", ErrProjectPatchQueueInvalid, latest.QueueID, latest.ItemID)
			case ProjectPatchQueueStateIntegrated:
				return fmt.Errorf("%w: patch queue item %s/%s is already INTEGRATED for this branch/head; create a new commit before submitting again", ErrProjectPatchQueueInvalid, latest.QueueID, latest.ItemID)
			case ProjectPatchQueueStateRejected:
				return fmt.Errorf("%w: patch queue item %s/%s is already REJECTED for this branch/head; create a new commit before submitting again", ErrProjectPatchQueueInvalid, latest.QueueID, latest.ItemID)
			case ProjectPatchQueueStateBlocked:
				if supersedesQueueID != latest.QueueID || supersedesItemID != latest.ItemID {
					return fmt.Errorf("%w: same-head BLOCKED patch queue requeue must supersede latest blocked item %s/%s", ErrProjectPatchQueueInvalid, latest.QueueID, latest.ItemID)
				}
				if err := s.validateProjectPatchQueueSupersessionEvidenceTx(ctx, tx, workspaceID, projectID, branchID, branchHeadSHA, supersedesQueueID, supersedesItemID, evidenceDocKey); err != nil {
					return err
				}
				supersessionValidated = true
			}
		}
		if !supersessionValidated && ok && projectPatchQueueExistingControlledLegacyReplacement(existing, workspaceID, projectID, repoID, branchID, queueID, itemID, supersedesQueueID, supersedesItemID) {
			if evidenceDocKey != "" {
				return fmt.Errorf("%w: legacy patch-only cancel+replace does not accept evidence_doc_key; replacement provenance is supersedes_queue_id/supersedes_item_id plus the cancellation receipt", ErrProjectPatchQueueInvalid)
			}
			supersessionValidated = true
		}
		if !supersessionValidated && (supersedesQueueID != "" || supersedesItemID != "" || evidenceDocKey != "") {
			if candidate, ok, err := s.projectPatchQueueLegacyControlledReplacementTx(ctx, tx, workspaceID, projectID, repoID, branchID, branchHeadSHA, queueID, mode, supersedesQueueID, supersedesItemID, evidenceDocKey); err != nil {
				return err
			} else if ok {
				legacyReplacement = true
				legacyReplacementItem = candidate
			} else if err := s.validateProjectPatchQueueSupersessionEvidenceTx(ctx, tx, workspaceID, projectID, branchID, branchHeadSHA, supersedesQueueID, supersedesItemID, evidenceDocKey); err != nil {
				return err
			}
		}
		if ok && (existing.WorkspaceID != workspaceID || existing.ProjectID != projectID || existing.RepoID != repoID || existing.BranchID != branchID) {
			return fmt.Errorf("%w: patch queue item %s/%s belongs to workspace=%s project=%s repo=%s branch=%s", ErrProjectPatchQueueInvalid, queueID, itemID, existing.WorkspaceID, existing.ProjectID, existing.RepoID, existing.BranchID)
		}
		if legacyReplacement && ok && existing.QueueID == legacyReplacementItem.QueueID && existing.ItemID == legacyReplacementItem.ItemID {
			return fmt.Errorf("%w: controlled_queue replacement for legacy patch-only proposal %s/%s must use a fresh item_id", ErrProjectPatchQueueInvalid, legacyReplacementItem.QueueID, legacyReplacementItem.ItemID)
		}
		if ok && mode == ProjectPatchQueueAuthorityModeControlledQueue &&
			strings.TrimSpace(existing.RepoAuthorityMode) == ProjectPatchQueueAuthorityModePatchOnly {
			return fmt.Errorf("%w: controlled_queue cannot upgrade legacy patch-only proposal %s/%s in place; cancel+replace requires a fresh item_id with supersedes_queue_id/supersedes_item_id", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if ok && existing.State != ProjectPatchQueueStateProposed {
			return fmt.Errorf("%w: patch queue item %s/%s is already %s; use a new item_id after revision", ErrProjectPatchQueueInvalid, queueID, itemID, existing.State)
		}
		if ok && projectPatchQueueOperationBindingEvidencePresent(existing) {
			return fmt.Errorf("%w: patch queue item %s/%s already has operation binding evidence; use a new item_id after revision", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if legacyReplacement {
			if queueID == legacyReplacementItem.QueueID && itemID == legacyReplacementItem.ItemID {
				return fmt.Errorf("%w: controlled_queue replacement for legacy patch-only proposal %s/%s must use a fresh item_id", ErrProjectPatchQueueInvalid, legacyReplacementItem.QueueID, legacyReplacementItem.ItemID)
			}
			canceled := legacyReplacementItem
			canceled.State = ProjectPatchQueueStateCanceled
			canceled.DecisionSummary = projectPatchQueueLegacyControlledReplacementReason(legacyReplacementItem, queueID, itemID)
			canceled.DecidedBy = actorID
			canceled.DecidedAt = now
			canceled.ClaimedBy = ""
			canceled.ClaimToken = ""
			canceled.ClaimedAt = ""
			canceled.ClaimExpiresAt = ""
			canceled.UpdatedAt = now
			if err := updateProjectPatchQueueLifecycleTx(ctx, tx, canceled); err != nil {
				return err
			}
			payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(canceled, actorID, "patch_queue.legacy_cancel_replace"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.submit"), map[string]string{
				"workspace_id":         workspaceID,
				"project_id":           projectID,
				"repo_id":              canceled.RepoID,
				"branch_id":            canceled.BranchID,
				"queue_id":             canceled.QueueID,
				"item_id":              canceled.ItemID,
				"replacement_queue_id": queueID,
				"replacement_item_id":  itemID,
				"cancel_reason":        canceled.DecisionSummary,
				"actor_id":             actorID,
				"principal_type":       effectiveActorType,
				"principal_id":         actorID,
			})
			if err != nil {
				return err
			}
			if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
				WorkspaceID: workspaceID,
				EventType:   "project.patch_queue.canceled",
				EntityType:  "project_patch_queue_item",
				EntityID:    canceled.QueueID + "/" + canceled.ItemID,
				ActorType:   effectiveActorType,
				ActorID:     actorID,
				PayloadJSON: mustJSON(payload),
				CreatedAt:   now,
			}); err != nil {
				return err
			}
		}
		if conflict, ok, err := getLiveProjectPatchQueueItemByBranchTx(ctx, tx, workspaceID, projectID, branchID); err != nil {
			return err
		} else if ok && (conflict.QueueID != queueID || conflict.ItemID != itemID) {
			return fmt.Errorf("%w: branch %s already has live patch queue item %s/%s", ErrProjectPatchQueueInvalid, branchID, conflict.QueueID, conflict.ItemID)
		}
		item = ProjectPatchQueueItemRecord{
			QueueID:                  queueID,
			ItemID:                   itemID,
			WorkspaceID:              workspaceID,
			ProjectID:                projectID,
			RepoID:                   repoID,
			BranchID:                 branchID,
			ReviewDocKey:             reviewDocKey,
			SupersedesQueueID:        supersedesQueueID,
			SupersedesItemID:         supersedesItemID,
			EvidenceDocKey:           evidenceDocKey,
			RepoAuthorityMode:        mode,
			State:                    ProjectPatchQueueStateProposed,
			Attempt:                  1,
			MaxAttempts:              normalizeProjectPatchQueueMaxAttempts(input.MaxAttempts),
			Pathset:                  pathset,
			PathsetJSON:              pathsetJSON,
			BaseRef:                  firstNonEmpty(branchBaseRef, strings.TrimSpace(input.BaseRef)),
			BaseSHA:                  firstNonEmpty(branchBaseSHA, strings.TrimSpace(input.BaseSHA)),
			HeadSHA:                  branchHeadSHA,
			AutoMerge:                false,
			SubmittedBy:              actorID,
			TaskID:                   strings.TrimSpace(input.TaskID),
			SessionID:                strings.TrimSpace(input.SessionID),
			RunID:                    strings.TrimSpace(input.RunID),
			AgentID:                  strings.TrimSpace(input.AgentID),
			PrincipalType:            strings.TrimSpace(input.PrincipalType),
			PrincipalID:              strings.TrimSpace(input.PrincipalID),
			CapabilitySnapshotID:     strings.TrimSpace(input.CapabilitySnapshotID),
			CapabilitySnapshotSchema: strings.TrimSpace(input.CapabilitySnapshotSchema),
			RepoRoot:                 strings.TrimSpace(input.RepoRoot),
			BaseTreeHash:             strings.TrimSpace(input.BaseTreeHash),
			BaseFileHashes:           input.BaseFileHashes,
			BaseFileHashesJSON:       strings.TrimSpace(input.BaseFileHashesJSON),
			ContextDigest:            strings.TrimSpace(input.ContextDigest),
			RepoLeaseID:              strings.TrimSpace(input.RepoLeaseID),
			LeaseTerm:                input.LeaseTerm,
			OperationID:              strings.TrimSpace(input.OperationID),
			OperationKind:            strings.TrimSpace(input.OperationKind),
			CreatedAt:                now,
			UpdatedAt:                now,
		}
		if err := normalizeProjectPatchQueueBindingRecord(&item, effectiveActorType, actorID); err != nil {
			return err
		}
		if mode == ProjectPatchQueueAuthorityModeControlledQueue && strings.TrimSpace(item.ContextDigest) == "" {
			return fmt.Errorf("%w: repoauthority_controlled_queue requires complete binding refs including task/session/run, principal, capability snapshot, repo root, base tree, lease, pathset, and patch queue identity", ErrProjectPatchQueueInvalid)
		}
		if err := upsertProjectPatchQueueItemTx(ctx, tx, item); err != nil {
			return err
		}
		if _, _, err := s.ensureProjectPatchQueueReviewTaskTx(ctx, tx, authority, item, branch, actorID, effectiveActorType, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue submit"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.submit"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.submit"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"repo_id":        repoID,
			"branch_id":      branchID,
			"queue_id":       queueID,
			"item_id":        itemID,
			"actor_id":       actorID,
			"principal_type": effectiveActorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.submitted",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) validateProjectPatchQueueSubmitTaskBindingTx(ctx context.Context, tx *sql.Tx, branch ProjectBranchRecord, inputTaskID string, pathset []string) error {
	branchTaskID := strings.TrimSpace(branch.ActiveTaskID)
	inputTaskID = strings.TrimSpace(inputTaskID)
	if branchTaskID == "" {
		return fmt.Errorf("%w: branch %s is missing active_task_id for patch queue submission", ErrProjectPatchQueueInvalid, strings.TrimSpace(branch.BranchID))
	}
	if inputTaskID != "" && inputTaskID != branchTaskID {
		return fmt.Errorf("%w: patch queue task_id %s must match branch active_task_id %s for branch %s", ErrProjectPatchQueueInvalid, firstNonEmpty(inputTaskID, "<empty>"), firstNonEmpty(branchTaskID, "<empty>"), strings.TrimSpace(branch.BranchID))
	}
	if err := s.validateProjectBranchActiveTaskPathsetBindingTx(ctx, tx, branch, pathset); err != nil {
		return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func (s *Store) SupersedeProjectPatchQueueItemWithEvent(ctx context.Context, input ProjectPatchQueueSupersedeInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, bool, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	newItemID := strings.TrimSpace(input.NewItemID)
	evidenceDocKey := strings.TrimSpace(input.EvidenceDocKey)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || newItemID == "" || evidenceDocKey == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, false, errors.New("workspace_id, project_id, queue_id, item_id, new_item_id, evidence_doc_key, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, false, errors.New("prompt_context_envelope is required")
	}
	if newItemID == itemID {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, false, fmt.Errorf("%w: new_item_id must differ from superseded item_id %s", ErrProjectPatchQueueInvalid, itemID)
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, false, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, false, fmt.Errorf("begin project patch queue supersede tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	existingReturned := false
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		oldItem, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || oldItem.WorkspaceID != workspaceID || oldItem.ProjectID != projectID {
			return fmt.Errorf("%w: superseded patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if oldItem.State != ProjectPatchQueueStateBlocked {
			return fmt.Errorf("%w: superseded patch queue item %s/%s must be BLOCKED, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, oldItem.State)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, oldItem); err != nil {
			return err
		}
		repo, err := getProjectRepositoryTx(ctx, tx, workspaceID, projectID, oldItem.RepoID)
		if err != nil {
			return err
		}
		if repo.RepoStatus != ProjectRepositoryStatusReady || strings.TrimSpace(repo.RemoteURL) == "" {
			return fmt.Errorf("%w: repo %s must be READY with remote_url before patch queue supersession", ErrProjectPatchQueueInvalid, oldItem.RepoID)
		}
		branch, ok, err := validateProjectBranchIDScopeTx(ctx, tx, oldItem.BranchID, workspaceID, projectID, oldItem.RepoID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: branch_id %s not found", ErrProjectPatchQueueInvalid, oldItem.BranchID)
		}
		if branch.Status != ProjectBranchStatusReadyForReview {
			return fmt.Errorf("%w: branch %s must be READY_FOR_REVIEW before patch queue supersession", ErrProjectPatchQueueInvalid, oldItem.BranchID)
		}
		branchHeadSHA := strings.TrimSpace(branch.HeadSHA)
		if branchHeadSHA == "" || branchHeadSHA != strings.TrimSpace(oldItem.HeadSHA) {
			return fmt.Errorf("%w: branch head_sha must still match superseded BLOCKED item head_sha", ErrProjectPatchQueueInvalid)
		}
		if err := s.validateProjectPatchQueueSupersessionEvidenceTx(ctx, tx, workspaceID, projectID, oldItem.BranchID, oldItem.HeadSHA, oldItem.QueueID, oldItem.ItemID, evidenceDocKey); err != nil {
			return err
		}
		if existing, ok, err := getProjectPatchQueueItemTx(ctx, tx, oldItem.QueueID, newItemID); err != nil {
			return err
		} else if ok {
			if existing.WorkspaceID == workspaceID &&
				existing.ProjectID == projectID &&
				existing.RepoID == oldItem.RepoID &&
				existing.BranchID == oldItem.BranchID &&
				existing.SupersedesQueueID == oldItem.QueueID &&
				existing.SupersedesItemID == oldItem.ItemID &&
				existing.EvidenceDocKey == evidenceDocKey &&
				(existing.State == ProjectPatchQueueStateProposed || existing.State == ProjectPatchQueueStateClaimed) {
				if strings.TrimSpace(existing.RepoAuthorityMode) != ProjectPatchQueueAuthorityModeControlledQueue ||
					strings.TrimSpace(existing.ContextDigest) == "" {
					return fmt.Errorf("%w: existing patch queue supersession %s/%s is missing controlled binding receipt", ErrProjectPatchQueueInvalid, existing.QueueID, existing.ItemID)
				}
				if err := s.validateProjectPatchQueueSubmitTaskBindingTx(ctx, tx, branch, strings.TrimSpace(existing.TaskID), existing.Pathset); err != nil {
					return err
				}
				item = existing
				if _, changed, err := s.ensureProjectPatchQueueReviewTaskTx(ctx, tx, authority, item, branch, actorID, effectiveActorType, now); err != nil {
					return err
				} else if changed {
					if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue supersede review task repair"); err != nil {
						return err
					}
				}
				existingReturned = true
				return nil
			}
			return fmt.Errorf("%w: patch queue item %s/%s already exists and is not the requested live supersession", ErrProjectPatchQueueInvalid, oldItem.QueueID, newItemID)
		}
		if conflict, ok, err := getLiveProjectPatchQueueItemByBranchTx(ctx, tx, workspaceID, projectID, oldItem.BranchID); err != nil {
			return err
		} else if ok {
			if conflict.SupersedesQueueID == oldItem.QueueID &&
				conflict.SupersedesItemID == oldItem.ItemID &&
				conflict.EvidenceDocKey == evidenceDocKey {
				if strings.TrimSpace(conflict.RepoAuthorityMode) != ProjectPatchQueueAuthorityModeControlledQueue ||
					strings.TrimSpace(conflict.ContextDigest) == "" {
					return fmt.Errorf("%w: existing patch queue supersession %s/%s is missing controlled binding receipt", ErrProjectPatchQueueInvalid, conflict.QueueID, conflict.ItemID)
				}
				if err := s.validateProjectPatchQueueSubmitTaskBindingTx(ctx, tx, branch, strings.TrimSpace(conflict.TaskID), conflict.Pathset); err != nil {
					return err
				}
				item = conflict
				if _, changed, err := s.ensureProjectPatchQueueReviewTaskTx(ctx, tx, authority, item, branch, actorID, effectiveActorType, now); err != nil {
					return err
				} else if changed {
					if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue supersede review task repair"); err != nil {
						return err
					}
				}
				existingReturned = true
				return nil
			}
			return fmt.Errorf("%w: branch %s already has live patch queue item %s/%s", ErrProjectPatchQueueInvalid, oldItem.BranchID, conflict.QueueID, conflict.ItemID)
		}
		pathsetJSON, pathset, err := normalizeProjectPatchQueuePathsetJSON(oldItem.PathsetJSON)
		if err != nil {
			return err
		}
		bindingRefsSupplied := projectPatchQueueSupersedeInputBindingRefsPresent(input)
		binding := projectPatchQueueSupersedeBindingInput(input, oldItem)
		item = ProjectPatchQueueItemRecord{
			QueueID:                  oldItem.QueueID,
			ItemID:                   newItemID,
			WorkspaceID:              workspaceID,
			ProjectID:                projectID,
			RepoID:                   oldItem.RepoID,
			BranchID:                 oldItem.BranchID,
			ReviewDocKey:             oldItem.ReviewDocKey,
			SupersedesQueueID:        oldItem.QueueID,
			SupersedesItemID:         oldItem.ItemID,
			EvidenceDocKey:           evidenceDocKey,
			RepoAuthorityMode:        ProjectPatchQueueAuthorityModeControlledQueue,
			State:                    ProjectPatchQueueStateProposed,
			Attempt:                  1,
			MaxAttempts:              normalizeProjectPatchQueueMaxAttempts(firstNonZeroInt(input.MaxAttempts, oldItem.MaxAttempts)),
			Pathset:                  pathset,
			PathsetJSON:              pathsetJSON,
			BaseRef:                  oldItem.BaseRef,
			BaseSHA:                  oldItem.BaseSHA,
			HeadSHA:                  oldItem.HeadSHA,
			AutoMerge:                false,
			SubmittedBy:              actorID,
			TaskID:                   binding.TaskID,
			SessionID:                binding.SessionID,
			RunID:                    binding.RunID,
			AgentID:                  binding.AgentID,
			PrincipalType:            binding.PrincipalType,
			PrincipalID:              binding.PrincipalID,
			CapabilitySnapshotID:     binding.CapabilitySnapshotID,
			CapabilitySnapshotSchema: binding.CapabilitySnapshotSchema,
			RepoRoot:                 binding.RepoRoot,
			BaseTreeHash:             binding.BaseTreeHash,
			BaseFileHashes:           binding.BaseFileHashes,
			BaseFileHashesJSON:       binding.BaseFileHashesJSON,
			ContextDigest:            binding.ContextDigest,
			RepoLeaseID:              binding.RepoLeaseID,
			LeaseTerm:                binding.LeaseTerm,
			CreatedAt:                now,
			UpdatedAt:                now,
		}
		if err := normalizeProjectPatchQueueBindingRecordWithPrincipalGuard(&item, effectiveActorType, actorID, bindingRefsSupplied); err != nil {
			return err
		}
		if strings.TrimSpace(item.ContextDigest) == "" {
			return fmt.Errorf("%w: repoauthority_controlled_queue supersession requires complete binding refs including task/session/run, principal, capability snapshot, repo root, base tree, lease, pathset, and patch queue identity", ErrProjectPatchQueueInvalid)
		}
		if err := s.validateProjectPatchQueueSubmitTaskBindingTx(ctx, tx, branch, strings.TrimSpace(item.TaskID), pathset); err != nil {
			return err
		}
		if err := upsertProjectPatchQueueItemTx(ctx, tx, item); err != nil {
			return err
		}
		if _, _, err := s.ensureProjectPatchQueueReviewTaskTx(ctx, tx, authority, item, branch, actorID, effectiveActorType, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue supersede"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.supersede"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.supersede"), map[string]string{
			"workspace_id":        workspaceID,
			"project_id":          projectID,
			"repo_id":             item.RepoID,
			"branch_id":           item.BranchID,
			"queue_id":            item.QueueID,
			"item_id":             item.ItemID,
			"new_item_id":         item.ItemID,
			"supersedes_queue_id": oldItem.QueueID,
			"supersedes_item_id":  oldItem.ItemID,
			"evidence_doc_key":    evidenceDocKey,
			"actor_id":            actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.submitted",
			EntityType:  "project_patch_queue_item",
			EntityID:    item.QueueID + "/" + item.ItemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, false, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, false, fmt.Errorf("commit project patch queue supersede tx: %w", err)
	}
	return item, event, existingReturned, nil
}

func (s *Store) ensureProjectPatchQueueReviewTaskTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, actorID, actorType, now string) (TaskStatus, bool, error) {
	baseTaskID := ProjectPatchQueueReviewTaskID(item)
	taskID := baseTaskID
	for attempt := 0; attempt < 20; attempt++ {
		if attempt > 0 {
			taskID = ProjectPatchQueueReviewTaskIDWithAttempt(baseTaskID, attempt+1)
		}
		status, ok, err := taskStatusForWorkspaceTx(ctx, tx, item.WorkspaceID, taskID)
		if err != nil {
			return TaskStatus{}, false, err
		}
		if ok {
			if projectPatchQueueReviewTaskStatusReusable(status, item) {
				eventID, err := projectPatchQueueReviewTaskEventIDTx(ctx, tx, item.WorkspaceID, status.TaskID)
				if err != nil {
					return TaskStatus{}, false, err
				}
				if strings.TrimSpace(eventID) == "" {
					if _, err := s.appendProjectPatchQueueReviewTaskCreatedEventTx(ctx, tx, authority, item, status, branch, actorID, actorType, now); err != nil {
						return TaskStatus{}, false, err
					}
					return status, true, nil
				}
				return status, false, nil
			}
			continue
		}
		return s.createProjectPatchQueueReviewTaskTx(ctx, tx, authority, item, branch, taskID, actorID, actorType, now)
	}
	return TaskStatus{}, false, fmt.Errorf("%w: patch queue review task id allocation failed for %s/%s", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
}

func (s *Store) ReconcileProjectPatchQueueReviewTaskReceipt(ctx context.Context, input ProjectPatchQueueReviewTaskReconcileInput) (TaskStatus, string, bool, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return TaskStatus{}, "", false, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return TaskStatus{}, "", false, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TaskStatus{}, "", false, fmt.Errorf("begin project patch queue review task reconcile tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status TaskStatus
	var eventID string
	var repaired bool
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		item, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || item.WorkspaceID != workspaceID || item.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if !projectPatchQueueReviewTaskRequired(item) {
			return fmt.Errorf("%w: patch queue item %s/%s does not require a live review task receipt in state %s", ErrProjectPatchQueueInvalid, queueID, itemID, item.State)
		}
		branch, branchOK, err := validateProjectBranchIDScopeTx(ctx, tx, item.BranchID, item.WorkspaceID, item.ProjectID, item.RepoID)
		if err != nil {
			return err
		}
		if !branchOK {
			branch = ProjectBranchRecord{
				WorkspaceID: item.WorkspaceID,
				ProjectID:   item.ProjectID,
				RepoID:      item.RepoID,
				BranchID:    item.BranchID,
				BranchName:  item.BranchID,
				BaseBranch:  item.BaseRef,
				BaseSHA:     item.BaseSHA,
				HeadSHA:     item.HeadSHA,
			}
		}
		ensured, changed, err := s.ensureProjectPatchQueueReviewTaskTx(ctx, tx, authority, item, branch, actorID, effectiveActorType, now)
		if err != nil {
			return err
		}
		receiptStatus, receiptEventID, ok, err := projectPatchQueueReviewTaskReceiptTx(ctx, tx, item)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: patch queue review task receipt for %s/%s was not readable after reconcile", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if strings.TrimSpace(receiptStatus.TaskID) == "" {
			receiptStatus = ensured
		}
		status = receiptStatus
		eventID = receiptEventID
		repaired = changed
		return s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue review task reconcile")
	}); err != nil {
		return TaskStatus{}, "", false, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TaskStatus{}, "", false, fmt.Errorf("commit project patch queue review task reconcile tx: %w", err)
	}
	return status, eventID, repaired, nil
}

func (s *Store) ConsumeProjectPatchQueueDecisionContinuation(ctx context.Context, input ProjectPatchQueueDecisionContinuationConsumeInput) (TaskStatus, ProjectPatchQueueDecisionContinuationRecord, bool, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	outboxID := strings.TrimSpace(input.OutboxID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || actorID == "" || (outboxID == "" && (queueID == "" || itemID == "")) {
		return TaskStatus{}, ProjectPatchQueueDecisionContinuationRecord{}, false, errors.New("workspace_id, project_id, actor_id, and outbox_id or queue_id/item_id are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return TaskStatus{}, ProjectPatchQueueDecisionContinuationRecord{}, false, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TaskStatus{}, ProjectPatchQueueDecisionContinuationRecord{}, false, fmt.Errorf("begin project patch queue decision continuation consume tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status TaskStatus
	var record ProjectPatchQueueDecisionContinuationRecord
	var created bool
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		loaded, ok, err := projectPatchQueueDecisionContinuationTx(ctx, tx, workspaceID, projectID, outboxID, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: patch queue decision continuation not found", ErrProjectPatchQueueInvalid)
		}
		record = loaded
		if strings.EqualFold(record.State, "SUPPRESSED") || strings.TrimSpace(record.ContinuationTaskID) == "" {
			record.State = "SUPPRESSED"
			record.UpdatedAt = now
			return s.markProjectPatchQueueDecisionContinuationStateTx(ctx, tx, record.OutboxID, "SUPPRESSED", now)
		}
		// DEFERRED is admissible here too (checklist #8/#10): a continuation that deferred at its creation event
		// (awaiting a role) can be explicitly consumed before the sweep reaches it - it re-runs the SAME modality
		// below (now->mint / still-awaiting->stay deferred / never->terminal), never erroring as "unsupported".
		if !strings.EqualFold(record.State, "PENDING") && !strings.EqualFold(record.State, "CONSUMED") && !strings.EqualFold(record.State, "DEFERRED") {
			return fmt.Errorf("%w: patch queue decision continuation %s is in unsupported state %s", ErrProjectPatchQueueInvalid, record.OutboxID, record.State)
		}
		item, ok, err := getProjectPatchQueueItemTx(ctx, tx, record.QueueID, record.ItemID)
		if err != nil {
			return err
		}
		if !ok || item.WorkspaceID != workspaceID || item.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found for decision continuation", ErrProjectPatchQueueInvalid, record.QueueID, record.ItemID)
		}
		if !strings.EqualFold(strings.TrimSpace(item.State), strings.TrimSpace(record.Decision)) {
			return fmt.Errorf("%w: patch queue item %s/%s state %s no longer matches continuation decision %s", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID, item.State, record.Decision)
		}
		taskID := strings.TrimSpace(record.ContinuationTaskID)
		if existing, ok, err := taskStatusForWorkspaceTx(ctx, tx, workspaceID, taskID); err != nil {
			return err
		} else if ok {
			if !projectPatchQueueDecisionContinuationTaskStatusReusable(existing, record) {
				return fmt.Errorf("%w: existing patch queue continuation task %s does not match decision continuation %s", ErrProjectPatchQueueInvalid, taskID, record.OutboxID)
			}
			status = existing
			if eventID, err := projectPatchQueueReviewTaskEventIDTx(ctx, tx, workspaceID, taskID); err != nil {
				return err
			} else if strings.TrimSpace(eventID) == "" {
				if _, err := s.appendProjectPatchQueueDecisionContinuationTaskCreatedEventTx(ctx, tx, authority, record, status, actorID, effectiveActorType, now); err != nil {
					return err
				}
			}
			record.State = "CONSUMED"
			record.UpdatedAt = now
			return s.markProjectPatchQueueDecisionContinuationStateTx(ctx, tx, record.OutboxID, "CONSUMED", now)
		}
		// Stage 4 modality gate: an explicit consume must also never mint an unclaimable carrier. NOW -> mint;
		// AWAITING -> defer (the sweep re-attempts); NEVER/undetermined -> typed-terminal (no task minted).
		route, resolvedOwner, err := s.decisivePathContinuationModalityTx(ctx, tx, record, item, actorID)
		if err != nil {
			return err
		}
		switch route.Route {
		case decisivePathRouteDeferred:
			// awaiting_role: ensure the row rests DEFERRED for the sweep. If it is ALREADY DEFERRED, leave
			// updated_at (the bounded-defer TTL clock, I1) untouched - re-marking would reset it, mirroring the
			// sweep's own "still awaiting -> leave untouched" discipline.
			if !strings.EqualFold(record.State, "DEFERRED") {
				if _, err := s.markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx, tx, record.OutboxID, record.State, "DEFERRED", now); err != nil {
					return err
				}
			}
			return s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue decision continuation defer")
		case decisivePathRouteYield:
			// satisfiable_now: claim the row FIRST (guarded record.State->CONSUMED) so a racing sweep/hook chasing
			// the same row is a clean no-op, THEN mint (checklist #3/a). A lost race re-reads the winner's task.
			claimed, err := s.markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx, tx, record.OutboxID, record.State, "CONSUMED", now)
			if err != nil {
				return err
			}
			if !claimed {
				reused, ok, err := taskStatusForWorkspaceTx(ctx, tx, workspaceID, taskID)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("%w: patch queue continuation %s lost the materialize race but task %s is unreadable", ErrProjectPatchQueueInvalid, record.OutboxID, taskID)
				}
				status = reused
				record.State = "CONSUMED"
				record.UpdatedAt = now
				return s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue decision continuation consume")
			}
		default:
			if err := s.markProjectPatchQueueDecisionContinuationTerminalTx(ctx, tx, record, route.Reason, now); err != nil {
				return err
			}
			return s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue decision continuation terminal")
		}
		// yield winner: the guarded claim above already set CONSUMED; reuse-or-mint the carrier via the single mint
		// chokepoint (shared with the event-time materializer and the sweep - no parallel mints).
		mintedStatus, mintedCreated, err := s.reuseOrMintProjectPatchQueueDecisionContinuationCarrierTx(ctx, tx, authority, record, item, resolvedOwner, actorID, effectiveActorType, now)
		if err != nil {
			return err
		}
		status = mintedStatus
		created = mintedCreated
		record.State = "CONSUMED"
		record.UpdatedAt = now
		return s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue decision continuation consume")
	}); err != nil {
		return TaskStatus{}, ProjectPatchQueueDecisionContinuationRecord{}, false, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TaskStatus{}, ProjectPatchQueueDecisionContinuationRecord{}, false, fmt.Errorf("commit project patch queue decision continuation consume tx: %w", err)
	}
	return status, record, created, nil
}

func projectPatchQueueDecisionContinuationTaskStatusReusable(status TaskStatus, record ProjectPatchQueueDecisionContinuationRecord) bool {
	if strings.TrimSpace(status.TaskID) == "" || strings.TrimSpace(record.ContinuationTaskID) == "" ||
		!strings.EqualFold(strings.TrimSpace(status.TaskID), strings.TrimSpace(record.ContinuationTaskID)) {
		return false
	}
	if strings.TrimSpace(status.ProjectID) != "" && !strings.EqualFold(strings.TrimSpace(status.ProjectID), strings.TrimSpace(record.ProjectID)) {
		return false
	}
	if lane := strings.TrimSpace(projectPatchQueueDecisionContinuationProjectLane(record)); lane != "" &&
		strings.TrimSpace(status.ProjectLane) != "" &&
		!strings.EqualFold(strings.TrimSpace(status.ProjectLane), lane) {
		return false
	}
	var req map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(status.TaskRequirementsJSON)), &req); err != nil || len(req) == 0 {
		return false
	}
	required := map[string]string{
		"decision_outbox_id":    strings.TrimSpace(record.OutboxID),
		"queue_id":              strings.TrimSpace(record.QueueID),
		"item_id":               strings.TrimSpace(record.ItemID),
		"decision":              strings.TrimSpace(record.Decision),
		"patch_queue_task_kind": strings.TrimSpace(record.FollowupKind),
	}
	for key, want := range required {
		if want == "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(fmt.Sprint(req[key])), want) {
			return false
		}
	}
	for key, want := range map[string]string{
		"branch_id": strings.TrimSpace(record.BranchID),
		"head_sha":  strings.TrimSpace(record.HeadSHA),
	} {
		if want == "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(fmt.Sprint(req[key])), want) {
			return false
		}
	}
	return true
}

func taskStatusForWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (TaskStatus, bool, error) {
	var status TaskStatus
	var requiresProjectGate int
	var taskRequirementsJSON string
	err := tx.QueryRowContext(ctx, `
SELECT t.task_id, t.title, t.description, t.owner_user_id, t.priority, t.status,
       t.task_kind, t.task_template, COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''),
       COALESCE(t.requires_project_gate, 0), COALESCE(t.task_requirements_json, '{}'), t.created_at, t.updated_at
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
 WHERE wt.workspace_id = ? AND t.task_id = ?
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(taskID),
	).Scan(
		&status.TaskID,
		&status.Title,
		&status.Description,
		&status.OwnerUserID,
		&status.Priority,
		&status.Status,
		&status.TaskKind,
		&status.TaskTemplate,
		&status.ProjectID,
		&status.ProjectLane,
		&requiresProjectGate,
		&taskRequirementsJSON,
		&status.CreatedAt,
		&status.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TaskStatus{}, false, nil
		}
		return TaskStatus{}, false, fmt.Errorf("load patch queue review task status: %w", err)
	}
	status.RequiresProjectGate = sqliteIntToBool(requiresProjectGate)
	status.TaskRequirementsJSON = normalizeTaskRequirementsJSON(taskRequirementsJSON)
	return status, true, nil
}

func projectPatchQueueReviewTaskStatusReusable(status TaskStatus, item ProjectPatchQueueItemRecord) bool {
	if strings.TrimSpace(status.ProjectID) != strings.TrimSpace(item.ProjectID) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(status.ProjectLane), "review") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(status.TaskKind), "COORDINATION") {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(status.Status)) {
	case "RESOLVED", "FAILED", "CANCELLED":
		return false
	}
	return projectPatchQueueReviewTaskIdentityMatches(status, item)
}

func projectPatchQueueReviewTaskIdentityMatches(status TaskStatus, item ProjectPatchQueueItemRecord) bool {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(status.TaskRequirementsJSON)), &payload); err != nil {
		return false
	}
	required := map[string]string{
		"patch_queue_task_kind": "review_receipt",
		"project_id":            item.ProjectID,
		"queue_id":              item.QueueID,
		"item_id":               item.ItemID,
		"branch_id":             item.BranchID,
		"head_sha":              item.HeadSHA,
	}
	for key, want := range required {
		got, _ := payload[key].(string)
		if strings.TrimSpace(got) != strings.TrimSpace(want) {
			return false
		}
	}
	return true
}

func projectPatchQueueReviewTaskRequired(item ProjectPatchQueueItemRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(item.State)) {
	case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed:
		return true
	default:
		return false
	}
}

func (s *Store) projectPatchQueueReviewTaskReceipt(ctx context.Context, item ProjectPatchQueueItemRecord) (TaskStatus, string, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return TaskStatus{}, "", false, fmt.Errorf("begin patch queue review task receipt read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	status, eventID, ok, err := projectPatchQueueReviewTaskReceiptTx(ctx, tx, item)
	if err != nil {
		return TaskStatus{}, "", false, err
	}
	if err := tx.Commit(); err != nil {
		return TaskStatus{}, "", false, fmt.Errorf("commit patch queue review task receipt read: %w", err)
	}
	return status, eventID, ok, nil
}

func (s *Store) annotateProjectPatchQueueReviewTaskReceipts(ctx context.Context, items []ProjectPatchQueueItemRecord) ([]ProjectPatchQueueItemRecord, error) {
	for i := range items {
		status, eventID, ok, err := s.projectPatchQueueReviewTaskReceipt(ctx, items[i])
		if err != nil {
			return nil, err
		}
		if ok {
			items[i].ReviewTaskID = strings.TrimSpace(status.TaskID)
			items[i].ReviewTaskStatus = strings.TrimSpace(status.Status)
			items[i].ReviewTaskEventID = strings.TrimSpace(eventID)
			items[i].MissingReviewTask = false
			continue
		}
		if projectPatchQueueReviewTaskRequired(items[i]) {
			items[i].MissingReviewTask = true
		}
	}
	return items, nil
}

func annotateProjectPatchQueueReviewTaskReceiptsTx(ctx context.Context, tx *sql.Tx, items []ProjectPatchQueueItemRecord) ([]ProjectPatchQueueItemRecord, error) {
	for i := range items {
		status, eventID, ok, err := projectPatchQueueReviewTaskReceiptTx(ctx, tx, items[i])
		if err != nil {
			return nil, err
		}
		if ok {
			items[i].ReviewTaskID = strings.TrimSpace(status.TaskID)
			items[i].ReviewTaskStatus = strings.TrimSpace(status.Status)
			items[i].ReviewTaskEventID = strings.TrimSpace(eventID)
			items[i].MissingReviewTask = false
			continue
		}
		if projectPatchQueueReviewTaskRequired(items[i]) {
			items[i].MissingReviewTask = true
		}
	}
	return items, nil
}

func projectPatchQueueReviewTaskReceiptTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) (TaskStatus, string, bool, error) {
	baseTaskID := ProjectPatchQueueReviewTaskID(item)
	for attempt := 0; attempt < 20; attempt++ {
		taskID := baseTaskID
		if attempt > 0 {
			taskID = ProjectPatchQueueReviewTaskIDWithAttempt(baseTaskID, attempt+1)
		}
		status, ok, err := taskStatusForWorkspaceTx(ctx, tx, item.WorkspaceID, taskID)
		if err != nil {
			return TaskStatus{}, "", false, err
		}
		if !ok || !projectPatchQueueReviewTaskStatusReusable(status, item) {
			continue
		}
		eventID, err := projectPatchQueueReviewTaskEventIDTx(ctx, tx, item.WorkspaceID, taskID)
		if err != nil {
			return TaskStatus{}, "", false, err
		}
		if strings.TrimSpace(eventID) == "" {
			continue
		}
		return status, eventID, true, nil
	}
	return TaskStatus{}, "", false, nil
}

func projectPatchQueueReviewTaskEventIDTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (string, error) {
	var eventID string
	err := tx.QueryRowContext(ctx, `
SELECT event_id
  FROM runtime_events
 WHERE workspace_id = ? AND entity_type = 'task' AND entity_id = ? AND event_type = 'task.created'
 ORDER BY created_at DESC, event_id DESC
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(taskID),
	).Scan(&eventID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("load patch queue review task event id: %w", err)
	}
	return strings.TrimSpace(eventID), nil
}

func (s *Store) createProjectPatchQueueReviewTaskTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, taskID, actorID, actorType, now string) (TaskStatus, bool, error) {
	ownerID := firstNonEmpty(strings.TrimSpace(actorID), strings.TrimSpace(item.SubmittedBy), strings.TrimSpace(branch.AgentID), "system")
	title := "Review patch queue candidate for " + strings.TrimSpace(item.ProjectID)
	description := strings.Join([]string{
		"Review the READY_FOR_REVIEW branch as a lane-scoped candidate and record a shared patch queue decision.",
		"",
		"Project: " + item.ProjectID,
		"Repository: " + item.RepoID,
		"Branch ID: " + item.BranchID,
		"Source task: " + firstNonEmpty(strings.TrimSpace(branch.ActiveTaskID), strings.TrimSpace(item.TaskID), "<unknown>"),
		"Patch queue: " + item.QueueID + "/" + item.ItemID,
		"Review packet: " + item.ReviewDocKey,
		"Base: " + firstNonEmpty(item.BaseRef, branch.BaseBranch, "main") + " @ " + firstNonEmpty(item.BaseSHA, branch.BaseSHA),
		"Head: " + firstNonEmpty(item.HeadSHA, branch.HeadSHA),
		"Pathset: " + strings.Join(uniqueTrimmedStrings(item.Pathset), ", "),
		"Review granularity: lane-scoped candidate unless the review packet explicitly claims final/full-product coverage.",
		"Integration boundary: ACCEPTED means this lane may enter integration. It is not a canonical merge, not a full-product verdict, and not a substitute for integrated build/verify evidence.",
		"",
		"Lane review contract:",
		"- Judge the branch against its source task, review packet, pathset, changed files, and lane-owned acceptance criteria.",
		"- Do not block solely because unrelated sibling lanes or final integration are incomplete.",
		"- Use BLOCKED_SPEC_DRIFT for lane candidates only when this branch misses its own lane contract, falsely claims full-product coverage, or implements an adjacent product.",
		"- Full-product completeness must be checked after accepted lanes are assembled by integration/final validation work.",
		"",
		"Expected flow:",
		"1. Read the review packet and project coordination context.",
		"2. Claim this patch queue item with project_patch_queue_lifecycle.",
		"3. Judge the owned pathset and lane/task claims, not the whole unfinished product. A correct partial lane may be ACCEPTED for integration even when sibling lanes remain pending.",
		"4. Use BLOCKED_SPEC_DRIFT only for defects, missing evidence, or false claims inside this lane's stated scope. Do not block solely because full-product ACs require other branches.",
		"5. Record accept, block, or reject with a concrete decision_summary once the candidate itself has enough evidence. Advanced operation/CAS/materialization receipts belong to integration/materialization follow-up, not to the first lane review unless you explicitly hold that authority and need it.",
		"   Operation binding, CAS, materialization, rollback, and canonical merge are post-review/integration evidence gates.",
		"6. If browser/build/source evidence is missing for this lane, block or create an explicit follow-up task instead of silently leaving the queue open.",
		"7. Close this review task only after the patch queue item is no longer PROPOSED.",
	}, "\n")
	if err := s.createTaskWithGraphTx(ctx, tx, TaskCreateInput{
		WorkspaceID:         item.WorkspaceID,
		TaskID:              taskID,
		OwnerUserID:         ownerID,
		Priority:            "high",
		Title:               title,
		Description:         description,
		TaskKind:            "COORDINATION",
		TaskTemplate:        "research",
		Tags:                []string{"review", "reviewer", "patch_queue", "project"},
		ProjectID:           item.ProjectID,
		ProjectLane:         "review",
		RequiresProjectGate: false,
		TaskRequirementsJSON: string(mustJSON(map[string]any{
			"patch_queue_task_kind": "review_receipt",
			// RPF-58A: review tasks MUST drive a durable patch-queue decision before they may
			// complete. Without this, a review session could end carrying only verdict prose
			// (or a verdict delivered over the read-only model.ask channel, as in R58) while
			// the claimed item stayed CLAIMED forever. The generic required-tool gate now
			// refuses completion/doc-substitution unless a project_patch_queue_lifecycle
			// receipt (accept/reject/block) or a typed blocker is present in the trace.
			"required_tool":        "project_patch_queue_lifecycle",
			"project_id":           item.ProjectID,
			"queue_id":             item.QueueID,
			"item_id":              item.ItemID,
			"branch_id":            item.BranchID,
			"head_sha":             item.HeadSHA,
			"candidate_pathset":    uniqueTrimmedStrings(item.Pathset),
			"review_scope":         "lane_scoped_patch_queue_candidate",
			"source_task_id":       firstNonEmpty(strings.TrimSpace(branch.ActiveTaskID), strings.TrimSpace(item.TaskID)),
			"integration_boundary": "full_product_acceptance_deferred_to_integration_build_verify",
		})),
	}, dag.DefaultGraph(), now); err != nil {
		return TaskStatus{}, false, err
	}
	if err := s.attachTaskToWorkspaceTx(ctx, tx, TaskAttachmentInput{
		WorkspaceID: item.WorkspaceID,
		TaskID:      taskID,
		LinkedBy:    ownerID,
	}, now); err != nil {
		return TaskStatus{}, false, err
	}
	status, ok, err := taskStatusForWorkspaceTx(ctx, tx, item.WorkspaceID, taskID)
	if err != nil {
		return TaskStatus{}, false, err
	}
	if !ok {
		return TaskStatus{}, false, fmt.Errorf("%w: patch queue review task %s was not readable after create", ErrProjectPatchQueueInvalid, taskID)
	}
	if _, err := s.appendProjectPatchQueueReviewTaskCreatedEventTx(ctx, tx, authority, item, status, branch, actorID, actorType, now); err != nil {
		return TaskStatus{}, false, err
	}
	return status, true, nil
}

func (s *Store) appendProjectPatchQueueReviewTaskCreatedEventTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, item ProjectPatchQueueItemRecord, status TaskStatus, branch ProjectBranchRecord, actorID, actorType, now string) (RuntimeEventRecord, error) {
	payload := map[string]any{
		"workspace_id":           item.WorkspaceID,
		"project_id":             item.ProjectID,
		"task_id":                status.TaskID,
		"title":                  status.Title,
		"description":            status.Description,
		"priority":               status.Priority,
		"owner_user_id":          status.OwnerUserID,
		"linked_by":              firstNonEmpty(status.OwnerUserID, strings.TrimSpace(actorID)),
		"project_lane":           status.ProjectLane,
		"requires_project_gate":  status.RequiresProjectGate,
		"patch_queue_queue_id":   item.QueueID,
		"patch_queue_item_id":    item.ItemID,
		"patch_queue_branch_id":  item.BranchID,
		"patch_queue_head_sha":   item.HeadSHA,
		"patch_queue_review_doc": item.ReviewDocKey,
		"branch_name":            strings.TrimSpace(branch.BranchName),
		"summary":                "Patch queue review task created for " + item.QueueID + "/" + item.ItemID,
		"status":                 status.Status,
	}
	envelope := BuildTaskPromptContextEnvelope("task.patch_queue.review.create", "server_rpc", item.WorkspaceID, firstNonEmpty(strings.TrimSpace(actorType), "agent"), strings.TrimSpace(actorID))
	payload, err := AttachTaskPromptContextEnvelope(payload, envelope)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	agentID := ""
	if strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		agentID = strings.TrimSpace(actorID)
	}
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		DedupKey:    "task:" + status.TaskID + ":created",
		WorkspaceID: item.WorkspaceID,
		EventType:   "task.created",
		EntityType:  "task",
		EntityID:    status.TaskID,
		ActorType:   strings.TrimSpace(actorType),
		ActorID:     strings.TrimSpace(actorID),
		AgentID:     agentID,
		TaskID:      status.TaskID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
}

func (s *Store) projectPatchQueueLegacyControlledReplacementTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, repoID, branchID, headSHA, replacementQueueID, mode, supersedesQueueID, supersedesItemID, evidenceDocKey string) (ProjectPatchQueueItemRecord, bool, error) {
	if strings.TrimSpace(mode) != ProjectPatchQueueAuthorityModeControlledQueue {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	evidenceDocKey = strings.TrimSpace(evidenceDocKey)
	if evidenceDocKey != "" {
		return ProjectPatchQueueItemRecord{}, false, fmt.Errorf("%w: legacy patch-only cancel+replace does not accept evidence_doc_key; replacement provenance is supersedes_queue_id/supersedes_item_id plus the cancellation receipt", ErrProjectPatchQueueInvalid)
	}
	supersedesQueueID = strings.TrimSpace(supersedesQueueID)
	supersedesItemID = strings.TrimSpace(supersedesItemID)
	if supersedesQueueID == "" || supersedesItemID == "" {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	item, ok, err := getProjectPatchQueueItemTx(ctx, tx, supersedesQueueID, supersedesItemID)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	if !ok || !projectPatchQueueLegacyPatchOnlyProposalMatches(item, workspaceID, projectID, repoID, branchID, headSHA) {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	if strings.TrimSpace(replacementQueueID) != strings.TrimSpace(item.QueueID) {
		return ProjectPatchQueueItemRecord{}, false, fmt.Errorf("%w: legacy patch-only cancel+replace must keep queue_id=%s and use a fresh item_id", ErrProjectPatchQueueInvalid, item.QueueID)
	}
	return item, true, nil
}

func projectPatchQueueLegacyPatchOnlyProposalMatches(item ProjectPatchQueueItemRecord, workspaceID, projectID, repoID, branchID, headSHA string) bool {
	return strings.TrimSpace(item.WorkspaceID) == strings.TrimSpace(workspaceID) &&
		strings.TrimSpace(item.ProjectID) == strings.TrimSpace(projectID) &&
		strings.TrimSpace(item.RepoID) == strings.TrimSpace(repoID) &&
		strings.TrimSpace(item.BranchID) == strings.TrimSpace(branchID) &&
		strings.TrimSpace(item.RepoAuthorityMode) == ProjectPatchQueueAuthorityModePatchOnly &&
		strings.TrimSpace(item.State) == ProjectPatchQueueStateProposed &&
		!projectPatchQueueOperationBindingEvidencePresent(item)
}

func projectPatchQueueExistingControlledLegacyReplacement(item ProjectPatchQueueItemRecord, workspaceID, projectID, repoID, branchID, queueID, itemID, supersedesQueueID, supersedesItemID string) bool {
	return strings.TrimSpace(item.WorkspaceID) == strings.TrimSpace(workspaceID) &&
		strings.TrimSpace(item.ProjectID) == strings.TrimSpace(projectID) &&
		strings.TrimSpace(item.RepoID) == strings.TrimSpace(repoID) &&
		strings.TrimSpace(item.BranchID) == strings.TrimSpace(branchID) &&
		strings.TrimSpace(item.QueueID) == strings.TrimSpace(queueID) &&
		strings.TrimSpace(item.ItemID) == strings.TrimSpace(itemID) &&
		strings.TrimSpace(item.SupersedesQueueID) == strings.TrimSpace(supersedesQueueID) &&
		strings.TrimSpace(item.SupersedesItemID) == strings.TrimSpace(supersedesItemID) &&
		strings.TrimSpace(item.RepoAuthorityMode) == ProjectPatchQueueAuthorityModeControlledQueue &&
		strings.TrimSpace(item.State) == ProjectPatchQueueStateProposed
}

func projectPatchQueueLegacyControlledReplacementReason(oldItem ProjectPatchQueueItemRecord, replacementQueueID, replacementItemID string) string {
	return fmt.Sprintf("legacy patch-only proposal canceled before controlled_queue replacement; replacement=%s/%s; historical_evidence=%s/%s",
		strings.TrimSpace(replacementQueueID),
		strings.TrimSpace(replacementItemID),
		strings.TrimSpace(oldItem.QueueID),
		strings.TrimSpace(oldItem.ItemID),
	)
}

func projectPatchQueueSupersedeBindingInput(input ProjectPatchQueueSupersedeInput, oldItem ProjectPatchQueueItemRecord) ProjectPatchQueueSupersedeInput {
	if projectPatchQueueSupersedeInputBindingRefsPresent(input) {
		return input
	}
	if strings.TrimSpace(oldItem.RepoAuthorityMode) != ProjectPatchQueueAuthorityModeControlledQueue ||
		strings.TrimSpace(oldItem.ContextDigest) == "" {
		return input
	}
	return ProjectPatchQueueSupersedeInput{
		TaskID:                   strings.TrimSpace(oldItem.TaskID),
		SessionID:                strings.TrimSpace(oldItem.SessionID),
		RunID:                    strings.TrimSpace(oldItem.RunID),
		AgentID:                  strings.TrimSpace(oldItem.AgentID),
		PrincipalType:            strings.TrimSpace(oldItem.PrincipalType),
		PrincipalID:              strings.TrimSpace(oldItem.PrincipalID),
		CapabilitySnapshotID:     strings.TrimSpace(oldItem.CapabilitySnapshotID),
		CapabilitySnapshotSchema: strings.TrimSpace(oldItem.CapabilitySnapshotSchema),
		RepoRoot:                 strings.TrimSpace(oldItem.RepoRoot),
		BaseTreeHash:             strings.TrimSpace(oldItem.BaseTreeHash),
		BaseFileHashes:           oldItem.BaseFileHashes,
		BaseFileHashesJSON:       strings.TrimSpace(oldItem.BaseFileHashesJSON),
		RepoLeaseID:              strings.TrimSpace(oldItem.RepoLeaseID),
		LeaseTerm:                oldItem.LeaseTerm,
	}
}

func projectPatchQueueSupersedeInputBindingRefsPresent(input ProjectPatchQueueSupersedeInput) bool {
	if input.LeaseTerm != 0 || len(input.BaseFileHashes) > 0 {
		return true
	}
	for _, value := range []string{
		input.TaskID,
		input.SessionID,
		input.RunID,
		input.AgentID,
		input.PrincipalType,
		input.PrincipalID,
		input.CapabilitySnapshotID,
		input.CapabilitySnapshotSchema,
		input.RepoRoot,
		input.BaseTreeHash,
		input.BaseFileHashesJSON,
		input.ContextDigest,
		input.RepoLeaseID,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func (s *Store) validateProjectPatchQueueSupersessionEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID, headSHA, supersedesQueueID, supersedesItemID, evidenceDocKey string) error {
	supersedesQueueID = strings.TrimSpace(supersedesQueueID)
	supersedesItemID = strings.TrimSpace(supersedesItemID)
	evidenceDocKey = strings.TrimSpace(evidenceDocKey)
	if supersedesQueueID == "" || supersedesItemID == "" || evidenceDocKey == "" {
		return fmt.Errorf("%w: superseded patch queue requeue requires supersedes_queue_id, supersedes_item_id, and evidence_doc_key", ErrProjectPatchQueueInvalid)
	}
	oldItem, ok, err := getProjectPatchQueueItemTx(ctx, tx, supersedesQueueID, supersedesItemID)
	if err != nil {
		return err
	}
	if !ok || oldItem.WorkspaceID != workspaceID || oldItem.ProjectID != projectID {
		return fmt.Errorf("%w: superseded patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, supersedesQueueID, supersedesItemID)
	}
	if oldItem.State != ProjectPatchQueueStateBlocked {
		return fmt.Errorf("%w: superseded patch queue item %s/%s must be BLOCKED, got %s", ErrProjectPatchQueueInvalid, supersedesQueueID, supersedesItemID, oldItem.State)
	}
	if strings.TrimSpace(oldItem.BranchID) != strings.TrimSpace(branchID) || strings.TrimSpace(oldItem.HeadSHA) != strings.TrimSpace(headSHA) {
		return fmt.Errorf("%w: superseded patch queue item must match branch_id/head_sha for same-head requeue", ErrProjectPatchQueueInvalid)
	}
	if latest, latestOK, err := getLatestProjectPatchQueueTerminalItemForBranchHeadTx(ctx, tx, workspaceID, projectID, strings.TrimSpace(oldItem.BranchID), strings.TrimSpace(oldItem.HeadSHA)); err != nil {
		return err
	} else if latestOK {
		if strings.TrimSpace(latest.QueueID) != strings.TrimSpace(oldItem.QueueID) || strings.TrimSpace(latest.ItemID) != strings.TrimSpace(oldItem.ItemID) {
			return fmt.Errorf("%w: same-head supersede must target latest terminal patch queue item %s/%s, not stale item %s/%s", ErrProjectPatchQueueInvalid, latest.QueueID, latest.ItemID, oldItem.QueueID, oldItem.ItemID)
		}
		if strings.ToUpper(strings.TrimSpace(latest.State)) == ProjectPatchQueueStateRejected {
			return fmt.Errorf("%w: same-head patch queue item %s/%s is already REJECTED; create a new commit before submitting again", ErrProjectPatchQueueInvalid, latest.QueueID, latest.ItemID)
		}
	}
	branch, branchFound, err := validateProjectBranchIDScopeTx(ctx, tx, oldItem.BranchID, workspaceID, projectID, oldItem.RepoID)
	if err != nil {
		return err
	}
	if !branchFound {
		branch = ProjectBranchRecord{
			BranchID: oldItem.BranchID,
			HeadSHA:  oldItem.HeadSHA,
		}
	}
	if strings.TrimSpace(oldItem.DecisionDocKey) != "" && evidenceDocKey == strings.TrimSpace(oldItem.DecisionDocKey) {
		return fmt.Errorf("%w: evidence_doc_key cannot be the superseded BLOCKED decision_doc_key", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(oldItem.EvidenceDocKey) != "" && evidenceDocKey == strings.TrimSpace(oldItem.EvidenceDocKey) {
		return fmt.Errorf("%w: evidence_doc_key %s was already consumed by superseded patch queue item %s/%s", ErrProjectPatchQueueInvalid, evidenceDocKey, oldItem.QueueID, oldItem.ItemID)
	}
	if consumed, ok, err := projectPatchQueueConsumedTerminalEvidenceKeyTx(ctx, tx, workspaceID, projectID, oldItem.BranchID, oldItem.HeadSHA, oldItem.QueueID, oldItem.ItemID, evidenceDocKey); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("%w: evidence_doc_key %s was already consumed by terminal same-head patch queue item %s/%s", ErrProjectPatchQueueInvalid, evidenceDocKey, consumed.QueueID, consumed.ItemID)
	}
	doc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, evidenceDocKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: evidence_doc_key %s not found", ErrProjectPatchQueueInvalid, evidenceDocKey)
		}
		return err
	}
	if doc.ArchivedAt != nil {
		return fmt.Errorf("%w: evidence_doc_key %s is archived", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	if projectPatchQueueSupersessionEvidenceDocIsCoordinationResponse(doc) {
		return fmt.Errorf("%w: evidence_doc_key %s is coordination response evidence, not positive same-head validation evidence", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	if projectPatchQueueSupersessionEvidenceDocIsAgentState(doc) {
		return fmt.Errorf("%w: evidence_doc_key %s is agent state evidence, not positive same-head validation evidence", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	if projectPatchQueueSupersessionEvidenceDocIsReflectiveSummary(doc) {
		return fmt.Errorf("%w: evidence_doc_key %s is reflection/heartbeat evidence, not positive same-head validation evidence", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	if projectPatchQueueSupersessionEvidenceDocIsTaskBrief(doc) {
		return fmt.Errorf("%w: evidence_doc_key %s is a task brief, not positive same-head validation evidence", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	referenceAt := firstNonEmpty(strings.TrimSpace(oldItem.DecidedAt), strings.TrimSpace(oldItem.UpdatedAt), strings.TrimSpace(oldItem.CreatedAt))
	if referenceAt != "" {
		docUpdatedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(doc.UpdatedAt))
		if err != nil {
			return fmt.Errorf("%w: evidence_doc_key %s has invalid updated_at %q", ErrProjectPatchQueueInvalid, evidenceDocKey, doc.UpdatedAt)
		}
		blockedAt, err := time.Parse(time.RFC3339Nano, referenceAt)
		if err != nil {
			return fmt.Errorf("%w: superseded BLOCKED item %s/%s has invalid decision timestamp %q", ErrProjectPatchQueueInvalid, supersedesQueueID, supersedesItemID, referenceAt)
		}
		if !docUpdatedAt.After(blockedAt) {
			return fmt.Errorf("%w: evidence_doc_key %s must be newer than superseded BLOCKED decision", ErrProjectPatchQueueInvalid, evidenceDocKey)
		}
	}
	evidenceText := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	if missing := projectPatchQueueSupersessionEvidenceMissingTargetRef(evidenceText, oldItem, branch); missing != "" {
		return fmt.Errorf("%w: evidence_doc_key %s must name the exact same queue_id, item_id, branch_id or branch_name, and head_sha; missing %s", ErrProjectPatchQueueInvalid, evidenceDocKey, missing)
	}
	if consumed, ok, err := s.projectPatchQueueConsumedTerminalEvidenceBasisTx(ctx, tx, workspaceID, projectID, oldItem.BranchID, oldItem.HeadSHA, oldItem, branch, doc); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("%w: evidence_doc_key %s repeats evidence already consumed by terminal same-head patch queue item %s/%s", ErrProjectPatchQueueInvalid, evidenceDocKey, consumed.QueueID, consumed.ItemID)
	}
	if projectPatchQueueSupersessionEvidenceHasExplicitNegativeVerdict(evidenceText) {
		return fmt.Errorf("%w: evidence_doc_key %s describes missing/blocking validation evidence; supersede requires positive same-head validation evidence", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	hasPositiveValidation := projectPatchQueueSupersessionEvidenceHasPositiveValidation(evidenceText)
	if projectPatchQueueSupersessionEvidenceRejectsProgress(evidenceText) && !projectPatchQueueSupersessionEvidenceClosesStaleBlocker(evidenceText, hasPositiveValidation) {
		return fmt.Errorf("%w: evidence_doc_key %s describes missing/blocking validation evidence; supersede requires positive same-head validation evidence", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	if missing := projectPatchQueueSupersessionVisualAcceptanceMissingRequirements(evidenceText, oldItem); len(missing) > 0 {
		return fmt.Errorf("%w: evidence_doc_key %s must be complete visual acceptance evidence before same-head supersede; missing %s", ErrProjectPatchQueueInvalid, evidenceDocKey, strings.Join(missing, "; "))
	}
	if !hasPositiveValidation {
		return fmt.Errorf("%w: evidence_doc_key %s must be positive same-head validation evidence, not a review/blocker summary", ErrProjectPatchQueueInvalid, evidenceDocKey)
	}
	return nil
}

func projectPatchQueueConsumedTerminalEvidenceKeyTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID, headSHA, currentQueueID, currentItemID, evidenceDocKey string) (ProjectPatchQueueItemRecord, bool, error) {
	evidenceDocKey = strings.TrimSpace(evidenceDocKey)
	if evidenceDocKey == "" {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT queue_id, item_id, workspace_id, project_id, repo_id, branch_id, state, head_sha, evidence_doc_key, updated_at, decided_at
FROM project_patch_queue_items
WHERE workspace_id = ?
  AND project_id = ?
  AND branch_id = ?
  AND head_sha = ?
  AND evidence_doc_key = ?
  AND state IN (?, ?)
ORDER BY COALESCE(decided_at, updated_at, created_at) DESC, item_id DESC
LIMIT 1`,
		workspaceID,
		projectID,
		strings.TrimSpace(branchID),
		strings.TrimSpace(headSHA),
		evidenceDocKey,
		ProjectPatchQueueStateBlocked,
		ProjectPatchQueueStateRejected,
	)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var item ProjectPatchQueueItemRecord
		if err := rows.Scan(&item.QueueID, &item.ItemID, &item.WorkspaceID, &item.ProjectID, &item.RepoID, &item.BranchID, &item.State, &item.HeadSHA, &item.EvidenceDocKey, &item.UpdatedAt, &item.DecidedAt); err != nil {
			return ProjectPatchQueueItemRecord{}, false, err
		}
		if strings.TrimSpace(item.QueueID) == strings.TrimSpace(currentQueueID) && strings.TrimSpace(item.ItemID) == strings.TrimSpace(currentItemID) {
			continue
		}
		return item, true, nil
	}
	if err := rows.Err(); err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	return ProjectPatchQueueItemRecord{}, false, nil
}

func (s *Store) projectPatchQueueConsumedTerminalEvidenceBasisTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID, headSHA string, current ProjectPatchQueueItemRecord, branch ProjectBranchRecord, candidateDoc WorkspaceDocRecord) (ProjectPatchQueueItemRecord, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND branch_id = ?
   AND head_sha = ?
   AND evidence_doc_key IS NOT NULL
   AND TRIM(evidence_doc_key) <> ''
   AND state IN (?, ?)
 ORDER BY COALESCE(decided_at, updated_at, created_at) DESC, item_id DESC`,
		workspaceID,
		projectID,
		strings.TrimSpace(branchID),
		strings.TrimSpace(headSHA),
		ProjectPatchQueueStateBlocked,
		ProjectPatchQueueStateRejected,
	)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	defer rows.Close()

	items := []ProjectPatchQueueItemRecord{current}
	for rows.Next() {
		item, err := scanProjectPatchQueueItem(rows)
		if err != nil {
			return ProjectPatchQueueItemRecord{}, false, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	candidateDigest := projectPatchQueueSupersessionEvidenceBasisDigest(candidateDoc, items, branch)
	if candidateDigest == "" {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	for _, item := range items {
		docKey := strings.TrimSpace(item.EvidenceDocKey)
		if docKey == "" || docKey == strings.TrimSpace(candidateDoc.DocKey) {
			continue
		}
		doc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, docKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return ProjectPatchQueueItemRecord{}, false, err
		}
		if projectPatchQueueSupersessionEvidenceBasisDigest(doc, items, branch) == candidateDigest {
			return item, true, nil
		}
	}
	return ProjectPatchQueueItemRecord{}, false, nil
}

func projectPatchQueueSupersessionEvidenceBasisDigest(doc WorkspaceDocRecord, items []ProjectPatchQueueItemRecord, branch ProjectBranchRecord) string {
	normalized := projectPatchQueueNormalizeSupersessionEvidenceBasis(doc.Content, items, branch)
	if normalized == "" {
		return ""
	}
	return contentSHA256(normalized)
}

func projectPatchQueueNormalizeSupersessionEvidenceBasis(text string, items []ProjectPatchQueueItemRecord, branch ProjectBranchRecord) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	if normalized == "" {
		return ""
	}
	refs := projectPatchQueueSupersessionEvidenceBasisRefs(items, branch)
	for _, ref := range refs {
		normalized = strings.ReplaceAll(normalized, ref, "__ref__")
	}
	return strings.Join(strings.Fields(normalized), " ")
}

func projectPatchQueueSupersessionEvidenceBasisRefs(items []ProjectPatchQueueItemRecord, branch ProjectBranchRecord) []string {
	seen := map[string]struct{}{}
	add := func(value string) {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return
		}
		seen[value] = struct{}{}
	}
	for _, item := range items {
		add(item.QueueID)
		add(item.ItemID)
		add(item.SupersedesQueueID)
		add(item.SupersedesItemID)
		add(item.BranchID)
		add(item.HeadSHA)
	}
	add(branch.BranchID)
	add(branch.BranchName)
	add(branch.HeadSHA)
	refs := make([]string, 0, len(seen))
	for ref := range seen {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if len(refs[i]) != len(refs[j]) {
			return len(refs[i]) > len(refs[j])
		}
		return refs[i] < refs[j]
	})
	return refs
}

func projectPatchQueueSupersessionEvidenceMissingTargetRef(evidenceText string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) string {
	text := strings.ToLower(evidenceText)
	headSHA := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.HeadSHA, branch.HeadSHA)))
	if headSHA != "" && !strings.Contains(text, headSHA) {
		return headSHA
	}
	queueID := strings.ToLower(strings.TrimSpace(item.QueueID))
	itemID := strings.ToLower(strings.TrimSpace(item.ItemID))
	if queueID != "" && !strings.Contains(text, queueID) {
		return queueID
	}
	if itemID != "" && !strings.Contains(text, itemID) {
		return itemID
	}
	branchID := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.BranchID, branch.BranchID)))
	branchName := strings.ToLower(strings.TrimSpace(branch.BranchName))
	if branchID != "" && strings.Contains(text, branchID) {
		return ""
	}
	if branchName != "" && strings.Contains(text, branchName) {
		return ""
	}
	return firstNonEmpty(branchID, branchName)
}

func projectPatchQueueSupersessionEvidenceDocIsCoordinationResponse(doc WorkspaceDocRecord) bool {
	key := strings.ToLower(strings.TrimSpace(doc.DocKey))
	if strings.Contains(key, ".agent_response.") || strings.HasSuffix(key, ".agent_response") || strings.HasPrefix(key, "agent_response.") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{doc.Title, doc.Content}, "\n"))
	if strings.Contains(text, "evidence_scope: coordination_ack_not_validation") {
		return true
	}
	hasAgentResponseHeader := strings.Contains(text, "agent request response evidence")
	hasCoordinationScope := strings.Contains(text, "evidence_scope:") && strings.Contains(text, "coordination")
	return hasAgentResponseHeader && hasCoordinationScope
}

func projectPatchQueueSupersessionEvidenceDocIsAgentState(doc WorkspaceDocRecord) bool {
	key := strings.ToLower(strings.TrimSpace(doc.DocKey))
	if key == "claimed_work" || key == "current_context" || strings.HasSuffix(key, ".claimed_work") || strings.HasSuffix(key, ".current_context") {
		return true
	}
	parts := strings.Split(key, ".")
	if len(parts) >= 3 && parts[0] == "agent" {
		suffix := strings.TrimSpace(parts[len(parts)-1])
		if strings.HasPrefix(suffix, "claimed_work") || strings.HasPrefix(suffix, "current_context") {
			return true
		}
	}
	text := strings.ToLower(strings.Join([]string{doc.Title, doc.Content}, "\n"))
	if strings.Contains(text, "claimed work ledger") || strings.Contains(text, "active_claimed_work:") {
		return true
	}
	if strings.Contains(text, "current context") && strings.Contains(text, "agent") && strings.Contains(text, "workspace") {
		return true
	}
	return false
}

func projectPatchQueueSupersessionEvidenceDocIsReflectiveSummary(doc WorkspaceDocRecord) bool {
	key := strings.ToLower(strings.TrimSpace(doc.DocKey))
	for _, marker := range []string{
		".reflection_board",
		".reflection-board",
		".heartbeat.",
		".heartbeat_",
		".meta_reflection",
		".meta-reflection",
	} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	text := strings.ToLower(strings.Join([]string{doc.Title, doc.Content}, "\n"))
	if strings.Contains(text, "reflection board") || strings.Contains(text, "meta-reflection") || strings.Contains(text, "meta reflection") {
		return true
	}
	if strings.Contains(text, "heartbeat") && (strings.Contains(text, "agent") || strings.Contains(text, "cycle") || strings.Contains(text, "observation")) {
		return true
	}
	return false
}

func projectPatchQueueSupersessionEvidenceDocIsTaskBrief(doc WorkspaceDocRecord) bool {
	title := strings.ToLower(strings.TrimSpace(doc.Title))
	content := strings.ToLower(strings.TrimSpace(doc.Content))
	if strings.HasPrefix(title, "task brief -") || strings.HasPrefix(content, "# task brief -") {
		return true
	}
	return strings.Contains(content, "this canonical task document was created by task_submit")
}

func projectPatchQueueSupersessionEvidenceHasExplicitNegativeVerdict(evidenceText string) bool {
	if projectPatchQueueEvidenceHasStructuredFieldValue(evidenceText,
		[]string{
			"visual_verdict", "visualverdict",
			"validation_verdict", "validationverdict",
			"acceptance_verdict", "acceptanceverdict",
			"task_verdict", "taskverdict",
			"verdict", "result",
			"status", "acceptance_status", "acceptancestatus",
			"pass_for_acceptance", "passforacceptance",
		},
		[]string{
			"block", "blocked", "blocker",
			"fail", "failed",
			"provisional_fail", "provisional-fail",
			"reject", "rejected",
			"under_evidenced", "under-evidenced", "underevidenced",
			"not_accepted", "not-accepted", "notaccepted",
			"not_ready", "not-ready", "notready",
			"provisional_non_pass", "provisional-non-pass", "provisionalnonpass",
			"false", "no",
		}) {
		return true
	}
	compact := strings.ToLower(strings.NewReplacer(
		" ", "",
		"\t", "",
		"\r", "",
		"\n", "",
		"\"", "",
		"'", "",
		"`", "",
	).Replace(evidenceText))
	fields := []string{"visual_verdict", "visualverdict", "validation_verdict", "validationverdict", "acceptance_verdict", "acceptanceverdict", "task_verdict", "taskverdict", "verdict", "result"}
	values := []string{"block", "blocked", "blocker", "fail", "failed", "provisional_fail", "provisionalfail", "reject", "rejected", "under_evidenced", "underevidenced", "not_accepted", "notaccepted", "provisional_non_pass", "provisionalnonpass"}
	for _, field := range fields {
		for _, value := range values {
			if strings.Contains(compact, field+":"+value) || strings.Contains(compact, field+"="+value) {
				return true
			}
		}
	}
	return false
}

func projectPatchQueueSupersessionEvidenceRejectsProgress(evidenceText string) bool {
	for _, marker := range []string{
		"pass_for_acceptance: false",
		"pass_for_acceptance=false",
		"pass for acceptance: false",
		"status: provisional_non_pass",
		"status=provisional_non_pass",
		"provisional_non_pass",
		"provisional non pass",
		"visual_verdict: under_evidenced",
		"visual_verdict=under_evidenced",
		"visual verdict: under evidenced",
		"under_evidenced",
		"under-evidenced",
		"under evidenced",
		"acceptance_status: not_accepted",
		"acceptance_status=not_accepted",
		"acceptance status: not accepted",
		"not_accepted",
		"not accepted",
		"provisional_non_canonical_review",
		"blocked:",
		"decision: blocked",
		"state: blocked",
		"still missing",
		"still lacks",
		"still pending",
		"still no pass",
		"lacks fresh",
		"lacks pass",
		"missing browser",
		"missing fresh",
		"missing result/export",
		"missing result export",
		"evidence is missing",
		"evidence still pending",
		"visual evidence pending",
		"visual evidence still pending",
		"visual evidence is still pending",
		"visual acceptance pending",
		"visual acceptance: pending",
		"visual acceptance evidence pending",
		"visual acceptance evidence still pending",
		"visual acceptance evidence is still pending",
		"acceptance status: not ready",
		"acceptance_status: not_ready",
		"not_ready_for_patch_queue_accept",
		"not ready",
		"not run",
		"was not run",
		"not executed",
		"not satisfied",
		"not evidenced",
		"not as a same-head requeue candidate",
		"not as same-head requeue candidate",
		"not same-head requeue",
		"not just a documentation gap",
		"did not pass",
		"does not pass",
		"does not yet",
		"primary_flow: not",
		"primary flow: not",
		"result_state: not",
		"result state: not",
		"pass_for_initial_state_only",
		"initial_state_only",
		"initial state only",
		"only covers initial",
		"only proves startup",
		"only proves the app loaded",
		"page-load only",
		"load-only",
		"startup only",
		"no pass-grade",
		"no pass grade",
		"no browser smoke",
		"no browser-smoke",
		"without browser smoke",
		"without browser-smoke",
		"browser smoke not run",
		"browser-smoke not run",
		"browser smoke skipped",
		"browser-smoke skipped",
		"browser smoke was skipped",
		"browser-smoke was skipped",
		"smoke not run",
		"smoke skipped",
		"smoke was skipped",
		"smoke not executed",
		"visual check not exercised",
		"visual checks not exercised",
		"visual acceptance not exercised",
		"browser not exercised",
		"browser smoke failed",
		"browser-smoke failed",
		"smoke failed",
		"validation failed",
		"test failed",
		"tests failed",
		"command failed",
		"real revision need",
		"blocker_evidence",
		"current blocker",
		"blocker is current",
		"blocker freshness: current",
		"gap remains",
		"gap still remains",
		"remains blocked",
		"visual_verdict: block",
		"visual_verdict=block",
		"visual verdict: block",
		"visual_verdict: fail",
		"visual_verdict=fail",
		"visual_verdict: failed",
		"visual_verdict=failed",
		"visual_verdict: blocked",
		"validation_verdict: fail",
		"validation_verdict=fail",
		"validation_verdict: failed",
		"validation_verdict=failed",
		"validation_verdict: blocker",
		"validation_verdict=blocker",
		"regression observed",
		"crash observed",
		"cannot accept",
		"не готов",
		"не готова",
		"отсутств",
		"не хватает",
		"нет свеж",
		"заблокирован",
		"заблокировано",
	} {
		if strings.Contains(evidenceText, marker) {
			return true
		}
	}
	return false
}

func projectPatchQueueSupersessionVisualAcceptanceMissingRequirements(evidenceText string, item ProjectPatchQueueItemRecord) []string {
	text := strings.ToLower(strings.TrimSpace(evidenceText))
	if !projectPatchQueueSupersessionNeedsCompleteVisualAcceptance(text, item) {
		return nil
	}
	missing := []string{}
	if !strings.Contains(text, "rhizome_visual_acceptance_v1") {
		missing = append(missing, "schema: rhizome_visual_acceptance_v1")
	}
	if !projectPatchQueueVisualEvidenceHasPassVerdict(text) {
		missing = append(missing, "visual_verdict: pass")
	}
	if !projectPatchQueueVisualEvidenceNamesState(text, "initial_state", "initial state", "empty_state", "empty state", "first_viewport", "first viewport", "first_screen", "first screen") {
		missing = append(missing, "initial_state evidence")
	}
	if !projectPatchQueueVisualEvidenceNamesState(text, "primary_flow", "primary flow", "primary_path", "primary path", "happy_path", "happy path", "user_flow", "user flow") {
		missing = append(missing, "primary_flow evidence")
	}
	if !projectPatchQueueVisualEvidenceNamesState(text, "result_state", "result state", "output_state", "output state", "export_state", "export state", "post_action", "post-action", "post action") &&
		!projectPatchQueueEvidenceContainsAny(text, "result_state: not_applicable", "result_state: n/a", "result state: not applicable", "result state: n/a") {
		missing = append(missing, "result_state evidence or not-applicable note")
	}
	if !projectPatchQueueEvidenceContainsAny(text, "screenshot", "screenshot_path", "screenshot_ref", "artifact_path", ".png", ".jpg", ".jpeg", ".webp") {
		missing = append(missing, "state-specific screenshot refs or paths")
	}
	if !projectPatchQueueEvidenceContainsAny(text, "viewport_matrix", "viewport matrix", "viewport") ||
		!projectPatchQueueEvidenceContainsAny(text, "desktop", "wide viewport", "1365", "1440", "1280") ||
		!projectPatchQueueEvidenceContainsAny(text, "mobile", "narrow", "390", "375", "small viewport") {
		missing = append(missing, "desktop and narrow/mobile viewport matrix")
	}
	if !projectPatchQueueEvidenceContainsAny(text, "product_intent", "acceptance criteria", "core user", "user promise", "primary happy path", "primary user path") {
		missing = append(missing, "product intent or primary user promise exercised")
	}
	if !projectPatchQueueEvidenceContainsAny(text, "overlap", "clipping", "contrast", "readability", "responsive", "typography", "hierarchy", "spacing", "usability") {
		missing = append(missing, "visual layout/readability checks")
	}
	return uniqueTrimmedStrings(missing)
}

func projectPatchQueueSupersessionNeedsCompleteVisualAcceptance(evidenceText string, item ProjectPatchQueueItemRecord) bool {
	if projectPatchQueueEvidenceContainsAny(evidenceText, "rhizome_visual_acceptance_v1", "visual_acceptance", "visual-acceptance", "visual acceptance") {
		return true
	}
	blockerText := strings.ToLower(strings.Join([]string{
		item.DecisionSummary,
		item.DecisionDocKey,
		item.ReviewDocKey,
		item.EvidenceDocKey,
		strings.Join(item.Pathset, "\n"),
		item.PathsetJSON,
	}, "\n"))
	return projectPatchQueueEvidenceContainsAny(blockerText,
		"rhizome_visual_acceptance_v1",
		"visual_acceptance",
		"visual-acceptance",
		"visual acceptance",
		"visual_verdict",
		"browser/visual",
	)
}

func projectPatchQueueVisualEvidenceHasPassVerdict(text string) bool {
	return projectPatchQueueEvidenceHasStructuredFieldValue(text, []string{"visual_verdict", "visualverdict"}, []string{"pass", "passed"})
}

func projectPatchQueueVisualEvidenceNamesState(text string, markers ...string) bool {
	for _, marker := range markers {
		if marker != "" && strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func projectPatchQueueSupersessionEvidenceHasPositiveValidation(evidenceText string) bool {
	hasRuntimeSurface := false
	for _, marker := range []string{
		"browser", "browser-smoke", "smoke", "playwright", "user scenario", "desktop/mobile", "rhizome_visual_acceptance_v1", "visual_acceptance", "visual acceptance", "screenshot",
		"go test", "go test ./", "go build", "go vet", "unit test", "unit tests", "integration test", "integration tests", "test command", "build command", "command:", "exit_code", "exit code", "stdout", "stderr",
		"npm test", "npm run test", "npm run build", "pnpm test", "pnpm build", "pytest", "cargo test",
	} {
		if strings.Contains(evidenceText, marker) {
			hasRuntimeSurface = true
			break
		}
	}
	if !hasRuntimeSurface {
		return false
	}
	if projectPatchQueueEvidenceHasStructuredFieldValue(evidenceText,
		[]string{"visual_verdict", "visualverdict", "validation_verdict", "validationverdict", "acceptance_verdict", "acceptanceverdict", "task_verdict", "taskverdict", "verdict", "result"},
		[]string{"pass", "passed"}) {
		return true
	}
	for _, marker := range []string{
		"validation passed",
		"browser smoke: passed",
		"browser-smoke: passed",
		"browser smoke passed",
		"browser-smoke passed",
		"smoke passed",
		"passed browser smoke",
		"browser smoke evidence passed",
		"browser-smoke evidence passed",
		"browser smoke evidence: pass",
		"browser-smoke evidence: pass",
		"visual acceptance passed",
		"records a pass",
		"recorded a pass",
		"positive same-head validation evidence",
		"no failure was observed",
		"user scenarios passed",
		"desktop/mobile user scenarios passed",
		"go test passed",
		"go tests passed",
		"build passed",
		"build/test passed",
		"build and test passed",
		"unit tests passed",
		"integration tests passed",
		"exit code 0",
		"exit_code: 0",
		"exit_code=0",
		"status: passed",
		"status=passed",
		"tests passed",
		"проверка пройд",
		"smoke пройд",
		"валидация пройд",
	} {
		if strings.Contains(evidenceText, marker) {
			return true
		}
	}
	return false
}

func projectPatchQueueSupersessionEvidenceClosesStaleBlocker(evidenceText string, hasPositiveValidation bool) bool {
	if !hasPositiveValidation {
		return false
	}
	return projectPatchQueueEvidenceContainsAny(evidenceText,
		"blocker is stale",
		"blocker now stale",
		"blocker has become stale",
		"blocked reason is stale",
		"old blocked reason",
		"old blocker",
		"previous blocker",
		"previous blocked reason",
		"stale blocker",
		"stale blocked reason",
		"missing fresh browser-smoke evidence is stale",
		"missing fresh browser smoke evidence is stale",
		"missing browser-smoke evidence is stale",
		"missing browser smoke evidence is stale",
		"validation gap is closed",
		"validation gap closed",
		"gap is closed",
		"gap closed",
	)
}

func projectPatchQueueEvidenceContainsAny(text string, markers ...string) bool {
	for _, marker := range markers {
		if marker != "" && strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func projectPatchQueueEvidenceHasStructuredFieldValue(text string, fields, values []string) bool {
	if strings.TrimSpace(text) == "" || len(fields) == 0 || len(values) == 0 {
		return false
	}
	fieldSet := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if normalized := projectPatchQueueEvidenceNormalizeFieldToken(field); normalized != "" {
			fieldSet[normalized] = struct{}{}
		}
	}
	valueSet := make(map[string]struct{}, len(values))
	for _, value := range values {
		if normalized := projectPatchQueueEvidenceNormalizeValueToken(value); normalized != "" {
			valueSet[normalized] = struct{}{}
		}
	}
	cleaned := strings.ToLower(strings.NewReplacer(
		"\"", "",
		"'", "",
		"`", "",
		"{", "\n",
		"}", "\n",
		"[", "\n",
		"]", "\n",
		",", "\n",
		"\r", "\n",
	).Replace(text))
	for _, rawLine := range strings.Split(cleaned, "\n") {
		line := strings.TrimSpace(rawLine)
		line = strings.TrimLeft(line, "-* \t")
		if line == "" {
			continue
		}
		sep := strings.IndexAny(line, ":=")
		if sep <= 0 {
			continue
		}
		field := projectPatchQueueEvidenceNormalizeFieldToken(line[:sep])
		if _, ok := fieldSet[field]; !ok {
			continue
		}
		value := strings.TrimSpace(line[sep+1:])
		value = strings.Trim(value, " \t;.")
		if _, ok := valueSet[projectPatchQueueEvidenceNormalizeValueToken(value)]; ok {
			return true
		}
	}
	return false
}

func projectPatchQueueEvidenceNormalizeFieldToken(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.Trim(value, "-* .")
	return strings.NewReplacer(" ", "", "_", "", "-", "").Replace(value)
}

func projectPatchQueueEvidenceNormalizeValueToken(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.Trim(value, " \t;.,")
	return strings.NewReplacer(" ", "_", "-", "_").Replace(value)
}

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func (s *Store) ClaimProjectPatchQueueItemWithEvent(ctx context.Context, input ProjectPatchQueueClaimInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}
		switch current.State {
		case ProjectPatchQueueStateProposed:
		case ProjectPatchQueueStateClaimed:
			if projectPatchQueueClaimActiveAt(current, nowTime) && strings.TrimSpace(current.ClaimedBy) != actorID {
				return fmt.Errorf("%w: patch queue item %s/%s is already claimed by %s", ErrProjectPatchQueueInvalid, queueID, itemID, strings.TrimSpace(current.ClaimedBy))
			}
			if strings.TrimSpace(current.ClaimedBy) == actorID {
				if err := requireProjectPatchQueueClaimToken(current, input.ClaimToken); err != nil && strings.TrimSpace(input.ClaimToken) != "" {
					return err
				}
			}
		default:
			return fmt.Errorf("%w: patch queue item %s/%s is %s and cannot be claimed", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		claimToken := strings.TrimSpace(input.ClaimToken)
		if current.State == ProjectPatchQueueStateClaimed && strings.TrimSpace(current.ClaimedBy) == actorID && strings.TrimSpace(current.ClaimToken) != "" {
			claimToken = current.ClaimToken
		}
		if claimToken == "" {
			claimToken = nextID("patchqclaim")
		}
		item = current
		item.State = ProjectPatchQueueStateClaimed
		item.ClaimedBy = actorID
		item.ClaimToken = claimToken
		item.ClaimedAt = firstNonEmpty(strings.TrimSpace(item.ClaimedAt), now)
		item.ClaimExpiresAt = projectPatchQueueClaimLeaseUntil(nowTime, input.LeaseSeconds)
		item.UpdatedAt = now
		if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue claim"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.claim"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.claim"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"queue_id":       queueID,
			"item_id":        itemID,
			"actor_id":       actorID,
			"principal_type": effectiveActorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.claimed",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue claim tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) BindProjectPatchQueueMutationOperationWithEvent(ctx context.Context, input ProjectPatchQueueOperationBindInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	operationID := strings.TrimSpace(input.OperationID)
	operationKind := firstNonEmpty(strings.TrimSpace(input.OperationKind), ProjectPatchQueueOperationKindRepoPatchApply)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue operation bind tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.RepoAuthorityMode != ProjectPatchQueueAuthorityModeControlledQueue {
			return fmt.Errorf("%w: patch queue item %s/%s must use controlled queue authority before operation binding", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.State != ProjectPatchQueueStateClaimed {
			return fmt.Errorf("%w: patch queue item %s/%s must be CLAIMED before operation binding, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if !projectPatchQueueClaimActiveAt(current, nowTime) {
			return fmt.Errorf("%w: patch queue claim for %s/%s is expired; reclaim before operation binding", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
			return err
		}
		if projectPatchQueueOperationBindingEvidencePresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s already has operation binding evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if !projectPatchQueueBindingRefsPresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s requires complete durable binding refs before operation binding", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}
		mutationPathsJSON, mutationPaths, err := normalizeProjectPatchQueuePathsetJSON(firstNonEmpty(strings.TrimSpace(input.MutationPathsJSON), current.PathsetJSON))
		if err != nil {
			return fmt.Errorf("%w: mutation_paths_json is invalid: %v", ErrProjectPatchQueueInvalid, err)
		}
		_, currentPathset, err := normalizeProjectPatchQueuePathsetJSON(current.PathsetJSON)
		if err != nil {
			return fmt.Errorf("%w: patch queue pathset_json is invalid during operation binding: %v", ErrProjectPatchQueueInvalid, err)
		}
		if !stringSliceEqual(mutationPaths, currentPathset) {
			return fmt.Errorf("%w: mutation_paths_json must exactly match the patch queue pathset for this operation binding", ErrProjectPatchQueueInvalid)
		}
		ledgerRef, err := s.ensureProjectPatchQueueOperationLedgerTx(ctx, tx, authority, current, operationID, operationKind, effectiveActorType, actorID, now)
		if err != nil {
			return err
		}

		item = current
		item.OperationID = ledgerRef.OperationID
		item.OperationKind = ledgerRef.OperationKind
		item.OperationBindingSchema = ProjectPatchQueueOperationBindingSchema
		item.OperationBindingAccepted = true
		item.OperationMutationPathsJSON = mutationPathsJSON
		item.OperationMutationPaths = mutationPaths
		item.OperationBoundBy = actorID
		item.OperationBoundAt = now
		item.UpdatedAt = now
		if err := normalizeProjectPatchQueueOperationBindingRecord(&item); err != nil {
			return err
		}
		if err := validateProjectPatchQueueOperationBindingEvidence(item); err != nil {
			return err
		}
		if err := updateProjectPatchQueueOperationBindingTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue operation bind"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.operation_bind"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.operation_bind"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"queue_id":       queueID,
			"item_id":        itemID,
			"operation_id":   item.OperationID,
			"operation_kind": item.OperationKind,
			"actor_id":       actorID,
			"principal_type": effectiveActorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.operation_bound",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue operation bind tx: %w", err)
	}
	return item, event, nil
}

type projectPatchQueueOperationLedgerRef struct {
	OperationID   string
	OperationKind string
}

func (s *Store) ensureProjectPatchQueueOperationLedgerTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, item ProjectPatchQueueItemRecord, operationID, operationKind, actorType, actorID, now string) (projectPatchQueueOperationLedgerRef, error) {
	operationID = strings.TrimSpace(operationID)
	operationKind = firstNonEmpty(strings.TrimSpace(operationKind), ProjectPatchQueueOperationKindRepoPatchApply)
	if operationKind != ProjectPatchQueueOperationKindRepoPatchApply {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation_kind %q is not supported for project patch queue mutation binding", ErrProjectPatchQueueInvalid, operationKind)
	}
	if operationID == "" {
		operationID = nextID("repoop")
	}

	run, ok, err := getExecutionRunMaybeTx(ctx, tx, item.WorkspaceID, operationID)
	if err != nil {
		return projectPatchQueueOperationLedgerRef{}, err
	}
	if !ok {
		run, err = s.recordProjectPatchQueueOperationLedgerTx(ctx, tx, authority, item, operationID, operationKind, actorType, actorID, now)
		if err != nil {
			return projectPatchQueueOperationLedgerRef{}, err
		}
	}
	return validateProjectPatchQueueOperationLedgerRun(item, run, operationID, operationKind)
}

func (s *Store) recordProjectPatchQueueOperationLedgerTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, item ProjectPatchQueueItemRecord, operationID, operationKind, actorType, actorID, now string) (ExecutionRunRecord, error) {
	ledger := projectPatchQueueOperationLedgerPayload(item, operationID, operationKind, actorType, actorID, now)
	record := ExecutionRunRecord{
		RunID:       operationID,
		WorkspaceID: item.WorkspaceID,
		AgentID:     strings.TrimSpace(actorID),
		Title:       "Repo patch apply: " + item.QueueID + "/" + item.ItemID,
		Summary:     "Durable repo patch apply operation ledger for project patch queue binding.",
		Status:      "ACTIVE",
		Outcome:     "RUNNING",
		VerificationJSON: map[string]any{
			"operation_ledger": ledger,
		},
		promptContextEnvelope: BuildExecutionPromptContextEnvelope("project.patch_queue.operation_bind", "server_operation_ledger", item.WorkspaceID, actorType, actorID),
	}
	run, _, err := s.upsertExecutionRunTx(ctx, tx, authority, record, now)
	if err != nil {
		return ExecutionRunRecord{}, fmt.Errorf("record project patch queue operation ledger: %w", err)
	}
	return run, nil
}

func projectPatchQueueOperationLedgerPayload(item ProjectPatchQueueItemRecord, operationID, operationKind, actorType, actorID, now string) map[string]any {
	return map[string]any{
		"schema":         ProjectPatchQueueOperationLedgerSchema,
		"operation_id":   strings.TrimSpace(operationID),
		"operation_key":  "project_patch_queue:" + strings.TrimSpace(item.QueueID) + "/" + strings.TrimSpace(item.ItemID),
		"operation_kind": strings.TrimSpace(operationKind),
		"operation_name": "project.patch_queue.operation_bind:" + strings.TrimSpace(item.QueueID) + "/" + strings.TrimSpace(item.ItemID),
		"status":         "running",
		"terminal":       false,
		"created_at":     strings.TrimSpace(now),
		"started_at":     strings.TrimSpace(now),
		"updated_at":     strings.TrimSpace(now),
		"attempt":        item.Attempt,
		"binding": map[string]any{
			"workspace_id":               strings.TrimSpace(item.WorkspaceID),
			"project_id":                 strings.TrimSpace(item.ProjectID),
			"repo_id":                    strings.TrimSpace(item.RepoID),
			"branch_id":                  strings.TrimSpace(item.BranchID),
			"queue_id":                   strings.TrimSpace(item.QueueID),
			"item_id":                    strings.TrimSpace(item.ItemID),
			"task_id":                    strings.TrimSpace(item.TaskID),
			"session_id":                 strings.TrimSpace(item.SessionID),
			"parent_run_id":              strings.TrimSpace(item.RunID),
			"agent_id":                   strings.TrimSpace(item.AgentID),
			"principal_type":             strings.TrimSpace(item.PrincipalType),
			"principal_id":               strings.TrimSpace(item.PrincipalID),
			"capability_snapshot_id":     strings.TrimSpace(item.CapabilitySnapshotID),
			"capability_snapshot_schema": strings.TrimSpace(item.CapabilitySnapshotSchema),
			"claim_actor_id":             strings.TrimSpace(item.ClaimedBy),
			"recorded_by_type":           strings.TrimSpace(actorType),
			"recorded_by_id":             strings.TrimSpace(actorID),
		},
		"capability_snapshot": map[string]any{
			"snapshot_schema":                ProjectPatchQueueOperationLedgerSchema,
			"requested_capability":           "project.patch_queue.operation_bind",
			"surface_id":                     "project.patch_queue.operation_bind",
			"surface_status_at_start":        "enabled",
			"policy_verdict_at_start":        "ALLOW",
			"disabled_reason_codes_at_start": []string{},
		},
		"request": map[string]any{
			"summary":           "bind controlled patch queue mutation operation",
			"idempotency_scope": "workspace",
			"details": map[string]any{
				"workspace_id": strings.TrimSpace(item.WorkspaceID),
				"project_id":   strings.TrimSpace(item.ProjectID),
				"queue_id":     strings.TrimSpace(item.QueueID),
				"item_id":      strings.TrimSpace(item.ItemID),
				"branch_id":    strings.TrimSpace(item.BranchID),
				"repo_id":      strings.TrimSpace(item.RepoID),
			},
		},
		"fence": map[string]any{
			"canonical_mutation_allowed": false,
			"canonical_mutation_reason":  "operation ledger proves binding only; patch application remains disabled",
		},
		"causality": map[string]any{
			"source":      "server",
			"parent_refs": []string{strings.TrimSpace(item.RunID)},
		},
	}
}

func getExecutionRunMaybeTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string) (ExecutionRunRecord, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT run_id, workspace_id, COALESCE(task_id,''), COALESCE(session_id,''), COALESCE(agent_id,''),
		        title, summary, status, outcome, verification_json, created_at, updated_at, closed_at
		   FROM execution_runs
		  WHERE workspace_id = ? AND run_id = ?`,
		workspaceID,
		runID,
	)
	record, err := scanExecutionRunRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionRunRecord{}, false, nil
		}
		return ExecutionRunRecord{}, false, err
	}
	return record, true, nil
}

func validateProjectPatchQueueOperationLedgerRun(item ProjectPatchQueueItemRecord, run ExecutionRunRecord, operationID, operationKind string) (projectPatchQueueOperationLedgerRef, error) {
	if run.RunID != strings.TrimSpace(operationID) || run.WorkspaceID != strings.TrimSpace(item.WorkspaceID) {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run does not match requested workspace/operation", ErrProjectPatchQueueInvalid)
	}
	if normalizeExecutionRunStatus(run.Status) != "ACTIVE" || isExecutionRunTerminal(run.Status) {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s must be ACTIVE while binding mutation operation", ErrProjectPatchQueueInvalid, operationID)
	}
	if strings.TrimSpace(run.Outcome) != "RUNNING" {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s must keep RUNNING outcome while binding mutation operation", ErrProjectPatchQueueInvalid, operationID)
	}
	if run.ClosedAt != nil && strings.TrimSpace(*run.ClosedAt) != "" {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s must not be closed while binding mutation operation", ErrProjectPatchQueueInvalid, operationID)
	}
	envelope, ok := projectPatchQueueMapField(run.VerificationJSON, executionPromptContextEnvelopeKey)
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s is missing prompt context envelope", ErrProjectPatchQueueInvalid, operationID)
	}
	if projectPatchQueueStringField(envelope, "origin") != "server_operation_ledger" {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s must come from server_operation_ledger", ErrProjectPatchQueueInvalid, operationID)
	}
	if projectPatchQueueStringField(envelope, "surface") != "project.patch_queue.operation_bind" {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s has unexpected prompt surface", ErrProjectPatchQueueInvalid, operationID)
	}
	expectedRecordedBy := firstNonEmpty(strings.TrimSpace(item.OperationBoundBy), strings.TrimSpace(item.ClaimedBy))
	if expectedRecordedBy == "" {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger binding actor is missing from patch queue item", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(run.AgentID) != expectedRecordedBy {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run agent_id does not match binding actor", ErrProjectPatchQueueInvalid)
	}
	envelopePrincipalType := projectPatchQueueStringField(envelope, "principal_type")
	envelopePrincipalID := projectPatchQueueStringField(envelope, "principal_id")
	if envelopePrincipalType == "" || envelopePrincipalID != expectedRecordedBy {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger prompt principal does not match binding actor", ErrProjectPatchQueueInvalid)
	}

	ledger, ok := projectPatchQueueMapField(run.VerificationJSON, "operation_ledger")
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s is missing operation_ledger evidence", ErrProjectPatchQueueInvalid, operationID)
	}
	if projectPatchQueueStringField(ledger, "schema") != ProjectPatchQueueOperationLedgerSchema {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger run %s has unsupported schema", ErrProjectPatchQueueInvalid, operationID)
	}
	ledgerOperationID := projectPatchQueueStringField(ledger, "operation_id")
	ledgerOperationKind := projectPatchQueueStringField(ledger, "operation_kind")
	if ledgerOperationID != strings.TrimSpace(operationID) || ledgerOperationID != run.RunID {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger operation_id does not match run", ErrProjectPatchQueueInvalid)
	}
	if ledgerOperationKind != strings.TrimSpace(operationKind) || ledgerOperationKind != ProjectPatchQueueOperationKindRepoPatchApply {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger operation_kind %q is not %q", ErrProjectPatchQueueInvalid, ledgerOperationKind, ProjectPatchQueueOperationKindRepoPatchApply)
	}
	ledgerTerminal, ok := projectPatchQueueRequiredBoolField(ledger, "terminal")
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger terminal must be an explicit bool", ErrProjectPatchQueueInvalid)
	}
	if projectPatchQueueStringField(ledger, "status") != "running" || ledgerTerminal {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger must be running and non-terminal while binding", ErrProjectPatchQueueInvalid)
	}
	capability, ok := projectPatchQueueMapField(ledger, "capability_snapshot")
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger capability_snapshot is required", ErrProjectPatchQueueInvalid)
	}
	if projectPatchQueueStringField(capability, "requested_capability") != "project.patch_queue.operation_bind" ||
		projectPatchQueueStringField(capability, "surface_id") != "project.patch_queue.operation_bind" ||
		projectPatchQueueStringField(capability, "policy_verdict_at_start") != "ALLOW" {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger capability snapshot does not authorize operation_bind", ErrProjectPatchQueueInvalid)
	}
	fence, ok := projectPatchQueueMapField(ledger, "fence")
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger fence is required", ErrProjectPatchQueueInvalid)
	}
	canonicalMutationAllowed, ok := projectPatchQueueRequiredBoolField(fence, "canonical_mutation_allowed")
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger fence canonical_mutation_allowed must be an explicit bool", ErrProjectPatchQueueInvalid)
	}
	if canonicalMutationAllowed {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger fence must not claim canonical mutation is allowed", ErrProjectPatchQueueInvalid)
	}
	causality, ok := projectPatchQueueMapField(ledger, "causality")
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger causality is required", ErrProjectPatchQueueInvalid)
	}
	if projectPatchQueueStringField(causality, "source") != "server" {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger causality source must be server", ErrProjectPatchQueueInvalid)
	}
	binding, ok := projectPatchQueueMapField(ledger, "binding")
	if !ok {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger binding is required", ErrProjectPatchQueueInvalid)
	}
	expected := map[string]string{
		"workspace_id":               item.WorkspaceID,
		"project_id":                 item.ProjectID,
		"repo_id":                    item.RepoID,
		"branch_id":                  item.BranchID,
		"queue_id":                   item.QueueID,
		"item_id":                    item.ItemID,
		"task_id":                    item.TaskID,
		"session_id":                 item.SessionID,
		"parent_run_id":              item.RunID,
		"agent_id":                   item.AgentID,
		"principal_type":             item.PrincipalType,
		"principal_id":               item.PrincipalID,
		"capability_snapshot_id":     item.CapabilitySnapshotID,
		"capability_snapshot_schema": item.CapabilitySnapshotSchema,
	}
	for field, want := range expected {
		if projectPatchQueueStringField(binding, field) != strings.TrimSpace(want) {
			return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger binding field %s does not match patch queue item", ErrProjectPatchQueueInvalid, field)
		}
	}
	if projectPatchQueueStringField(binding, "claim_actor_id") != strings.TrimSpace(item.ClaimedBy) {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger binding field claim_actor_id does not match patch queue claim", ErrProjectPatchQueueInvalid)
	}
	if projectPatchQueueStringField(binding, "recorded_by_type") != envelopePrincipalType {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger binding field recorded_by_type does not match prompt principal", ErrProjectPatchQueueInvalid)
	}
	if projectPatchQueueStringField(binding, "recorded_by_id") != expectedRecordedBy {
		return projectPatchQueueOperationLedgerRef{}, fmt.Errorf("%w: operation ledger binding field recorded_by_id does not match binding actor", ErrProjectPatchQueueInvalid)
	}
	return projectPatchQueueOperationLedgerRef{OperationID: ledgerOperationID, OperationKind: ledgerOperationKind}, nil
}

func validateProjectPatchQueueOperationLedgerEvidenceTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	run, ok, err := getExecutionRunMaybeTx(ctx, tx, item.WorkspaceID, item.OperationID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: operation ledger run %s not found", ErrProjectPatchQueueInvalid, strings.TrimSpace(item.OperationID))
	}
	if _, err := validateProjectPatchQueueOperationLedgerRun(item, run, item.OperationID, item.OperationKind); err != nil {
		return fmt.Errorf("%w: live patch queue operation refs require durable operation ledger evidence: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func projectPatchQueueMapField(values map[string]any, key string) (map[string]any, bool) {
	if values == nil {
		return nil, false
	}
	raw, ok := values[key]
	if !ok {
		return nil, false
	}
	typed, ok := raw.(map[string]any)
	return typed, ok
}

func projectPatchQueueStringField(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	raw, ok := values[key]
	if !ok {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func projectPatchQueueBoolField(values map[string]any, key string) bool {
	if values == nil {
		return false
	}
	value, _ := values[key].(bool)
	return value
}

func projectPatchQueueRequiredBoolField(values map[string]any, key string) (bool, bool) {
	if values == nil {
		return false, false
	}
	value, ok := values[key].(bool)
	return value, ok
}

func (s *Store) RecordProjectPatchQueueCASEvidenceWithEvent(ctx context.Context, input ProjectPatchQueueCASRecordInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue CAS evidence tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.RepoAuthorityMode != ProjectPatchQueueAuthorityModeControlledQueue {
			return fmt.Errorf("%w: patch queue item %s/%s must use controlled queue authority before CAS evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.State != ProjectPatchQueueStateClaimed {
			return fmt.Errorf("%w: patch queue item %s/%s must be CLAIMED before CAS evidence, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if !projectPatchQueueClaimActiveAt(current, nowTime) {
			return fmt.Errorf("%w: patch queue claim for %s/%s is expired; reclaim before CAS evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
			return err
		}
		if !ProjectPatchQueueOperationBindingReady(current) {
			return fmt.Errorf("%w: patch queue item %s/%s requires verified operation binding before CAS evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if projectPatchQueueCASEvidencePresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s already has CAS evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}

		item = current
		item.CASEvidenceSchema = ProjectPatchQueueCASEvidenceSchema
		item.CASEvidenceAccepted = true
		item.CASResult = input.CASResult
		item.CASTestEvidence = input.TestEvidence
		item.CASRecordedBy = actorID
		item.CASRecordedAt = now
		item.UpdatedAt = now
		if err := normalizeProjectPatchQueueCASEvidenceRecord(&item); err != nil {
			return err
		}
		if err := validateProjectPatchQueueCASEvidence(item); err != nil {
			return err
		}
		if err := updateProjectPatchQueueCASEvidenceTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue CAS evidence"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.cas_record"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.cas_record"), map[string]string{
			"workspace_id":     workspaceID,
			"project_id":       projectID,
			"queue_id":         queueID,
			"item_id":          itemID,
			"operation_id":     strings.TrimSpace(item.OperationID),
			"operation_kind":   strings.TrimSpace(item.OperationKind),
			"cas_status":       strings.TrimSpace(item.CASStatus),
			"cas_patch_digest": strings.TrimSpace(item.CASPatchDigest),
			"actor_id":         actorID,
			"principal_type":   effectiveActorType,
			"principal_id":     actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.cas_recorded",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue CAS evidence tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) RecordProjectPatchQueueMaterializationWithEvent(ctx context.Context, input ProjectPatchQueueMaterializationRecordInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue materialization tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.RepoAuthorityMode != ProjectPatchQueueAuthorityModeControlledQueue {
			return fmt.Errorf("%w: patch queue item %s/%s must use controlled queue authority before materialization", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.State != ProjectPatchQueueStateClaimed {
			return fmt.Errorf("%w: patch queue item %s/%s must be CLAIMED before materialization, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if !projectPatchQueueClaimActiveAt(current, nowTime) {
			return fmt.Errorf("%w: patch queue claim for %s/%s is expired; reclaim before materialization", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
			return err
		}
		if !ProjectPatchQueueCASEvidenceReady(current) {
			return fmt.Errorf("%w: patch queue item %s/%s requires verified CAS evidence before materialization", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if projectPatchQueueMaterializationPresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s already has materialization", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}

		materialization := input.Materialization
		if strings.TrimSpace(materialization.RecordedBy) == "" {
			materialization.RecordedBy = actorID
		}
		item = current
		item.MaterializationSchema = ProjectPatchQueueMaterializationSchema
		item.MaterializationAccepted = true
		item.Materialization = materialization
		item.MaterializationRecordedBy = actorID
		item.MaterializationRecordedAt = now
		item.UpdatedAt = now
		if err := normalizeProjectPatchQueueMaterializationRecord(&item); err != nil {
			return err
		}
		if err := validateProjectPatchQueueMaterialization(item); err != nil {
			return err
		}
		if err := updateProjectPatchQueueMaterializationTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue materialization"); err != nil {
			return err
		}
		eventItem := projectPatchQueueRedactMaterializationContent(item)
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(eventItem, actorID, "patch_queue.materialization_record"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.materialization_record"), map[string]string{
			"workspace_id":                           workspaceID,
			"project_id":                             projectID,
			"queue_id":                               queueID,
			"item_id":                                itemID,
			"operation_id":                           strings.TrimSpace(item.OperationID),
			"operation_kind":                         strings.TrimSpace(item.OperationKind),
			"cas_patch_digest":                       strings.TrimSpace(item.CASPatchDigest),
			"materialization_digest":                 strings.TrimSpace(item.MaterializationDigest),
			"materialization_authority_proof_digest": strings.TrimSpace(item.MaterializationAuthorityProofDigest),
			"materialized_file_count":                fmt.Sprint(len(item.Materialization.Files)),
			"actor_id":                               actorID,
			"principal_type":                         effectiveActorType,
			"principal_id":                           actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.materialization_recorded",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue materialization tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) RecordProjectPatchQueueActuatorResultWithEvent(ctx context.Context, input ProjectPatchQueueActuatorResultRecordInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = ProjectPatchQueueActuatorActorID
	}
	actorType := strings.TrimSpace(input.ActorType)
	if actorType == "" {
		actorType = "system"
	}
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, and item_id are required")
	}
	result := input.Result
	if err := repoauthority.VerifyMutationActuatorLiveResult(result); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue actuator result tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.RepoAuthorityMode != ProjectPatchQueueAuthorityModeControlledQueue {
			return fmt.Errorf("%w: patch queue item %s/%s must use controlled queue authority before actuator evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if !ProjectPatchQueueMaterializationReady(current) {
			return fmt.Errorf("%w: patch queue item %s/%s requires durable materialization before actuator evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if strings.TrimSpace(result.WorkspaceID) != current.WorkspaceID ||
			strings.TrimSpace(result.ProjectID) != current.ProjectID ||
			strings.TrimSpace(result.RepoID) != current.RepoID ||
			strings.TrimSpace(result.QueueID) != current.QueueID ||
			strings.TrimSpace(result.ItemID) != current.ItemID ||
			strings.TrimSpace(result.MaterializationDigest) != current.MaterializationDigest ||
			strings.TrimSpace(result.MaterializationAuthorityProofDigest) != current.MaterializationAuthorityProofDigest {
			return fmt.Errorf("%w: actuator result identity does not match patch queue materialization", ErrProjectPatchQueueInvalid)
		}
		actuatorDedupKey := projectPatchQueueActuatorDedupKey(workspaceID, queueID, itemID, result.MaterializationDigest)
		// CR-23 class (domain idempotency): the same item+materialization already has an applied
		// receipt -> return it instead of letting appendRuntimeEventTx error on incidental payload
		// drift (target_head_before / target_dirty_state_after). Identity was validated just above.
		if existing, lookupErr := s.getRuntimeEventByDedupKeyTx(ctx, tx, workspaceID, actuatorDedupKey); lookupErr == nil {
			item = current
			event = existing
			return nil
		} else if !errors.Is(lookupErr, sql.ErrNoRows) {
			return lookupErr
		}
		payload := map[string]any{
			"schema":                                 result.Schema,
			"status":                                 result.Status,
			"source":                                 result.Source,
			"workspace_id":                           workspaceID,
			"project_id":                             projectID,
			"repo_id":                                current.RepoID,
			"queue_id":                               queueID,
			"item_id":                                itemID,
			"target_checkout_id":                     result.TargetCheckoutID,
			"target_branch_name":                     result.TargetBranchName,
			"target_head_before":                     result.TargetHeadBefore,
			"target_head_after":                      result.TargetHeadAfter,
			"target_dirty_state_after":               result.TargetDirtyStateAfter,
			"activation_digest":                      result.ActivationDigest,
			"materialization_digest":                 result.MaterializationDigest,
			"materialization_authority_proof_digest": result.MaterializationAuthorityProofDigest,
			"mutation_executed":                      result.MutationExecuted,
			"file_count":                             len(result.Files),
			"result_digest":                          result.Digest,
			"result":                                 result,
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			DedupKey:    projectPatchQueueActuatorDedupKey(workspaceID, queueID, itemID, result.MaterializationDigest),
			WorkspaceID: workspaceID,
			EventType:   ProjectPatchQueueActuatorAppliedEventType,
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		if appendErr != nil {
			return appendErr
		}
		item = current
		return s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue actuator result")
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue actuator result tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) RecordProjectPatchQueueIntegrationWithEvent(ctx context.Context, input ProjectPatchQueueIntegrationRecordInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	outcome, err := normalizeProjectPatchQueueIntegrationOutcome(input.Outcome)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if outcome == ProjectPatchQueueIntegrationOutcomeRepair && strings.TrimSpace(input.RepairReason) == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: repair_reason is required for patch queue integration repair receipt", ErrProjectPatchQueueInvalid)
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue integration receipt tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if repoID := strings.TrimSpace(input.RepoID); repoID != "" && repoID != strings.TrimSpace(current.RepoID) {
			return fmt.Errorf("%w: repo_id guard %s does not match patch queue item repo_id %s", ErrProjectPatchQueueInvalid, repoID, current.RepoID)
		}
		sourceBranchID := strings.TrimSpace(current.BranchID)
		if inputSourceBranchID := strings.TrimSpace(input.SourceBranchID); inputSourceBranchID != "" && inputSourceBranchID != sourceBranchID {
			return fmt.Errorf("%w: source_branch_id guard %s does not match patch queue item branch_id %s", ErrProjectPatchQueueInvalid, inputSourceBranchID, sourceBranchID)
		}
		sourceHeadSHA := strings.TrimSpace(current.HeadSHA)
		if inputSourceHeadSHA := strings.TrimSpace(input.SourceHeadSHA); inputSourceHeadSHA != "" && inputSourceHeadSHA != sourceHeadSHA {
			return fmt.Errorf("%w: source_head_sha guard %s does not match patch queue item head_sha %s", ErrProjectPatchQueueInvalid, inputSourceHeadSHA, sourceHeadSHA)
		}
		if projectPatchQueueReviewerAdvisoryDefeatsAcceptance(current) {
			return fmt.Errorf("%w: patch queue item %s/%s has unresolved same-head reviewer defect evidence; repair or supersede before integration", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		integrationMode, err := projectPatchQueueIntegrationModeForItem(current, input.IntegrationMode)
		if err != nil {
			return err
		}
		hasIntegratedReceipt := false
		switch outcome {
		case ProjectPatchQueueIntegrationOutcomeAdmitted:
			existingAdmissionReceipt, found, err := projectPatchQueueIntegrationReceiptForItemTx(ctx, tx, workspaceID, projectID, current, ProjectPatchQueueIntegrationAdmittedEventType, input.TargetBranch, input.TargetHeadAfter)
			if err != nil {
				return err
			}
			if found {
				item = current
				event = existingAdmissionReceipt
				return nil
			}
		case ProjectPatchQueueIntegrationOutcomeIntegrated:
			if canonicalProjectPatchQueueIntegrationTargetBranch(input.TargetBranch) == "" {
				return fmt.Errorf("%w: target_branch is required for integrated receipt identity", ErrProjectPatchQueueInvalid)
			}
			if !isCanonicalProjectGitObjectID(strings.TrimSpace(input.TargetHeadAfter)) {
				return fmt.Errorf("%w: target_head_after must be a canonical git object id for integrated receipt", ErrProjectPatchQueueInvalid)
			}
			existingIntegratedReceipt, found, err := projectPatchQueueIntegrationReceiptForItemTx(ctx, tx, workspaceID, projectID, current, ProjectPatchQueueIntegratedEventType, input.TargetBranch, input.TargetHeadAfter)
			if err != nil {
				return err
			}
			if !found && current.State == ProjectPatchQueueStateAccepted {
				existingIntegratedReceipt, found, err = projectPatchQueueIntegrationReceiptForItemTx(ctx, tx, workspaceID, projectID, current, ProjectPatchQueueIntegratedEventType, "", "")
				if err != nil {
					return err
				}
			}
			if found {
				replayInput := projectPatchQueueIntegrationInputFromReceipt(input, existingIntegratedReceipt)
				terminalItem, err := markProjectPatchQueueItemIntegratedTx(ctx, tx, current, now)
				if err != nil {
					return err
				}
				if _, err := s.terminalizeIntegratedPatchQueueWorkTx(ctx, tx, authority, workspaceID, projectID, terminalItem, replayInput, actorID, effectiveActorType, now); err != nil {
					return err
				}
				if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue integrated receipt replay"); err != nil {
					return err
				}
				item = terminalItem
				event = existingIntegratedReceipt
				return nil
			}
		case ProjectPatchQueueIntegrationOutcomeRepair:
			existingIntegratedReceipt, found, err := projectPatchQueueIntegrationReceiptForItemTx(ctx, tx, workspaceID, projectID, current, ProjectPatchQueueIntegratedEventType, input.TargetBranch, input.TargetHeadAfter)
			if err != nil {
				return err
			}
			if !found {
				existingIntegratedReceipt, found, err = projectPatchQueueIntegrationReceiptForItemTx(ctx, tx, workspaceID, projectID, current, ProjectPatchQueueIntegratedEventType, "", "")
				if err != nil {
					return err
				}
			}
			if found && current.State == ProjectPatchQueueStateAccepted {
				replayInput := projectPatchQueueIntegrationInputFromReceipt(input, existingIntegratedReceipt)
				terminalItem, err := markProjectPatchQueueItemIntegratedTx(ctx, tx, current, now)
				if err != nil {
					return err
				}
				if _, err := s.terminalizeIntegratedPatchQueueWorkTx(ctx, tx, authority, workspaceID, projectID, terminalItem, replayInput, actorID, effectiveActorType, now); err != nil {
					return err
				}
				if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue integrated receipt repair replay"); err != nil {
					return err
				}
				item = terminalItem
				event = existingIntegratedReceipt
				return nil
			}
			hasIntegratedReceipt = found
		}
		if current.State != ProjectPatchQueueStateAccepted {
			alreadyIntegratedState := current.State == ProjectPatchQueueStateIntegrated &&
				(outcome == ProjectPatchQueueIntegrationOutcomeIntegrated ||
					(outcome == ProjectPatchQueueIntegrationOutcomeRepair && hasIntegratedReceipt))
			if !alreadyIntegratedState {
				return fmt.Errorf("%w: patch queue item %s/%s must be ACCEPTED before integration receipt, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
			}
		}
		if outcome == ProjectPatchQueueIntegrationOutcomeIntegrated {
			if !input.PushSucceeded && !input.AlreadyIntegrated {
				return fmt.Errorf("%w: integrated receipt requires push_succeeded or canonical already_integrated remote proof", ErrProjectPatchQueueInvalid)
			}
			remoteHead := strings.TrimSpace(input.RemoteTargetHeadAfter)
			if !isCanonicalProjectGitObjectID(remoteHead) {
				return fmt.Errorf("%w: remote_target_head_after must be a canonical git object id for integrated receipt", ErrProjectPatchQueueInvalid)
			}
			if remoteHead != strings.TrimSpace(input.TargetHeadAfter) {
				return fmt.Errorf("%w: remote_target_head_after %s does not match target_head_after %s", ErrProjectPatchQueueInvalid, remoteHead, strings.TrimSpace(input.TargetHeadAfter))
			}
		}
		item = current
		repairBlocksItem := outcome == ProjectPatchQueueIntegrationOutcomeRepair && item.State == ProjectPatchQueueStateAccepted && !hasIntegratedReceipt
		if outcome == ProjectPatchQueueIntegrationOutcomeIntegrated && item.State == ProjectPatchQueueStateAccepted {
			terminalItem, err := markProjectPatchQueueItemIntegratedTx(ctx, tx, item, now)
			if err != nil {
				return err
			}
			item = terminalItem
		}
		if repairBlocksItem {
			item.State = ProjectPatchQueueStateBlocked
			item.DecisionSummary = "Integration repair required: " + strings.TrimSpace(input.RepairReason)
			item.DecidedBy = actorID
			item.DecidedAt = now
			item.ClaimedBy = ""
			item.ClaimToken = ""
			item.ClaimedAt = ""
			item.ClaimExpiresAt = ""
			item.UpdatedAt = now
			if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
				return err
			}
		}
		terminalization := map[string]any(nil)
		if outcome == ProjectPatchQueueIntegrationOutcomeIntegrated {
			terminalization, err = s.terminalizeIntegratedPatchQueueWorkTx(ctx, tx, authority, workspaceID, projectID, item, input, actorID, effectiveActorType, now)
			if err != nil {
				return err
			}
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue integration receipt"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.integration_"+outcome), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.integration_"+outcome), map[string]string{
			"workspace_id":             workspaceID,
			"project_id":               projectID,
			"repo_id":                  strings.TrimSpace(item.RepoID),
			"branch_id":                strings.TrimSpace(item.BranchID),
			"queue_id":                 queueID,
			"item_id":                  itemID,
			"outcome":                  outcome,
			"integration_mode":         integrationMode,
			"target_branch":            strings.TrimSpace(input.TargetBranch),
			"target_head_before":       strings.TrimSpace(input.TargetHeadBefore),
			"target_head_after":        strings.TrimSpace(input.TargetHeadAfter),
			"remote_target_head_after": strings.TrimSpace(input.RemoteTargetHeadAfter),
			"source_branch_id":         sourceBranchID,
			"source_head_sha":          sourceHeadSHA,
			"repair_reason":            strings.TrimSpace(input.RepairReason),
			"actor_id":                 actorID,
			"principal_type":           effectiveActorType,
			"principal_id":             actorID,
			"merge_performed":          fmt.Sprint(input.MergePerformed),
			"push_attempted":           fmt.Sprint(input.PushAttempted),
			"push_succeeded":           fmt.Sprint(input.PushSucceeded),
			"already_integrated":       fmt.Sprint(input.AlreadyIntegrated),
			"materialization_ref":      strings.TrimSpace(item.MaterializationDigest),
		})
		if err != nil {
			return err
		}
		payload["source_branch_id"] = sourceBranchID
		payload["source_head_sha"] = sourceHeadSHA
		payload["integration_mode"] = integrationMode
		payload["target_branch"] = strings.TrimSpace(input.TargetBranch)
		payload["target_head_before"] = strings.TrimSpace(input.TargetHeadBefore)
		payload["target_head_after"] = strings.TrimSpace(input.TargetHeadAfter)
		payload["remote_target_head_after"] = strings.TrimSpace(input.RemoteTargetHeadAfter)
		payload["repair_reason"] = strings.TrimSpace(input.RepairReason)
		if len(terminalization) > 0 {
			payload["integration_terminalization"] = terminalization
		}
		eventType := projectPatchQueueIntegrationEventType(outcome)
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			DedupKey:    projectPatchQueueIntegrationDedupKey(workspaceID, queueID, itemID, outcome, input.TargetBranch, input.TargetHeadAfter, input.RepairReason),
			WorkspaceID: workspaceID,
			EventType:   eventType,
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		if appendErr != nil {
			return appendErr
		}
		if repairBlocksItem {
			record, err := s.upsertProjectPatchQueueDecisionContinuationTx(ctx, tx, item, event, now)
			if err != nil {
				return err
			}
			return s.materializeProjectPatchQueueDecisionContinuationTx(ctx, tx, authority, record, item, actorID, effectiveActorType, now)
		}
		return nil
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue integration receipt tx: %w", err)
	}
	return item, event, nil
}

type integratedPatchQueueWorkCandidate struct {
	TaskID      string
	Status      string
	ProjectLane string
	Kind        string
	ClaimAgent  string
	ClaimStatus string
}

func markProjectPatchQueueItemIntegratedTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord, now string) (ProjectPatchQueueItemRecord, error) {
	if item.State == ProjectPatchQueueStateIntegrated {
		return item, nil
	}
	item.State = ProjectPatchQueueStateIntegrated
	item.ClaimedBy = ""
	item.ClaimToken = ""
	item.ClaimedAt = ""
	item.ClaimExpiresAt = ""
	item.UpdatedAt = now
	if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
		return ProjectPatchQueueItemRecord{}, err
	}
	return item, nil
}

func (s *Store) terminalizeIntegratedPatchQueueWorkTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, projectID string, item ProjectPatchQueueItemRecord, input ProjectPatchQueueIntegrationRecordInput, actorID, actorType, now string) (map[string]any, error) {
	queueID := strings.TrimSpace(item.QueueID)
	itemID := strings.TrimSpace(item.ItemID)
	branchID := strings.TrimSpace(item.BranchID)
	headSHA := strings.ToLower(strings.TrimSpace(item.HeadSHA))
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT t.task_id,
       COALESCE(t.status, ''),
       COALESCE(t.project_lane, ''),
       COALESCE(i.kind, ''),
       COALESCE(tc.agent_id, ''),
       COALESCE(tc.claim_status, '')
  FROM task_patch_queue_identities i
  JOIN workspace_tasks wt ON wt.workspace_id = i.workspace_id AND wt.task_id = i.task_id
  JOIN tasks t ON t.task_id = i.task_id
  LEFT JOIN task_claims tc ON tc.workspace_id = i.workspace_id AND tc.task_id = i.task_id
 WHERE i.workspace_id = ?
   AND i.project_id = ?
   AND (
        (i.queue_id = ? AND i.item_id = ?)
     OR (? <> '' AND ? <> '' AND i.branch_id = ? AND i.head_sha = ?)
     OR (? <> '' AND ? <> '' AND i.item_id = ? AND i.head_sha = ?)
     OR (? <> '' AND i.queue_id = '' AND i.item_id = '' AND i.branch_id = '' AND i.head_sha = ?)
   )
   AND (
        LOWER(TRIM(i.kind)) IN ('review', 'review_receipt', 'integration')
     OR LOWER(TRIM(t.project_lane)) IN ('review', 'integration')
   )
   AND t.status NOT IN (?, ?, ?)
 ORDER BY t.updated_at ASC, t.task_id ASC`,
		workspaceID,
		projectID,
		queueID,
		itemID,
		branchID, headSHA, branchID, headSHA,
		itemID, headSHA, itemID, headSHA,
		headSHA, headSHA,
		model.TaskStatusResolved,
		model.TaskStatusFailed,
		model.TaskStatusCancelled,
	)
	if err != nil {
		return nil, fmt.Errorf("query integrated patch queue work candidates: %w", err)
	}
	defer rows.Close()

	candidates := []integratedPatchQueueWorkCandidate{}
	for rows.Next() {
		var candidate integratedPatchQueueWorkCandidate
		if err := rows.Scan(
			&candidate.TaskID,
			&candidate.Status,
			&candidate.ProjectLane,
			&candidate.Kind,
			&candidate.ClaimAgent,
			&candidate.ClaimStatus,
		); err != nil {
			return nil, fmt.Errorf("scan integrated patch queue work candidate: %w", err)
		}
		candidate.TaskID = strings.TrimSpace(candidate.TaskID)
		if candidate.TaskID != "" {
			candidates = append(candidates, candidate)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate integrated patch queue work candidates: %w", err)
	}

	summary := fmt.Sprintf("closed by patch queue integration receipt for %s/%s at %s", queueID, itemID, firstNonEmpty(headSHA, strings.TrimSpace(input.TargetHeadAfter)))
	terminalizedTasks := make([]string, 0, len(candidates))
	completedClaims := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, err := tx.ExecContext(ctx, `
UPDATE tasks
   SET status = ?,
       close_reason = ?,
       updated_at = ?
 WHERE task_id = ?
   AND status NOT IN (?, ?, ?)`,
			model.TaskStatusResolved,
			summary,
			now,
			candidate.TaskID,
			model.TaskStatusResolved,
			model.TaskStatusFailed,
			model.TaskStatusCancelled,
		); err != nil {
			return nil, fmt.Errorf("terminalize integrated patch queue task %s: %w", candidate.TaskID, err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE dag_nodes
   SET status = ?,
       last_error = NULL,
       updated_at = ?
 WHERE task_id = ?
   AND status NOT IN (?, ?, ?)`,
			mapTaskResolutionToNodeStatus(model.TaskStatusResolved),
			now,
			candidate.TaskID,
			model.NodeStatusResolved,
			model.NodeStatusFailed,
			model.NodeStatusCancelled,
		); err != nil {
			return nil, fmt.Errorf("terminalize integrated patch queue task nodes %s: %w", candidate.TaskID, err)
		}
		claimResult, err := tx.ExecContext(ctx, `
UPDATE task_claims
   SET claim_status = ?,
       summary = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND task_id = ?
   AND claim_status NOT IN (?, ?, ?)`,
			model.TaskClaimStatusCompleted,
			summary,
			now,
			workspaceID,
			candidate.TaskID,
			model.TaskClaimStatusCompleted,
			model.TaskClaimStatusFailed,
			model.TaskClaimStatusCancelled,
		)
		if err != nil {
			return nil, fmt.Errorf("terminalize integrated patch queue task claim %s: %w", candidate.TaskID, err)
		}
		if affected, _ := claimResult.RowsAffected(); affected > 0 {
			completedClaims = append(completedClaims, candidate.TaskID)
		}
		if _, err := closeTaskExecutionRunsTx(ctx, tx, workspaceID, candidate.TaskID, model.TaskStatusResolved, now); err != nil {
			return nil, err
		}
		if err := s.resolveOpenOperatorQueuesForClosedTaskTx(ctx, tx, workspaceID, candidate.TaskID, model.TaskStatusResolved, actorID, now); err != nil {
			return nil, err
		}
		terminalizedTasks = append(terminalizedTasks, candidate.TaskID)
	}

	branchRefsCleared := 0
	checkoutRefsCleared := 0
	for _, taskID := range terminalizedTasks {
		branchResult, err := tx.ExecContext(ctx, `
UPDATE project_branch_registry
   SET active_task_id = '',
       active_claim_id = '',
       updated_by = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND (active_task_id = ? OR active_claim_id = ?)`,
			actorID, now, workspaceID, taskID, taskID)
		if err != nil {
			return nil, fmt.Errorf("clear integrated patch queue branch active refs for task %s: %w", taskID, err)
		}
		if affected, _ := branchResult.RowsAffected(); affected > 0 {
			branchRefsCleared += int(affected)
		}
		checkoutResult, err := tx.ExecContext(ctx, `
UPDATE project_checkout_registry
   SET active_task_id = '',
       active_claim_id = '',
       updated_by = ?,
       updated_at = ?,
       last_seen_at = ?
 WHERE workspace_id = ?
   AND (active_task_id = ? OR active_claim_id = ?)`,
			actorID, now, now, workspaceID, taskID, taskID)
		if err != nil {
			return nil, fmt.Errorf("clear integrated patch queue checkout active refs for task %s: %w", taskID, err)
		}
		if affected, _ := checkoutResult.RowsAffected(); affected > 0 {
			checkoutRefsCleared += int(affected)
		}
	}
	if branchID != "" {
		branchResult, err := tx.ExecContext(ctx, `
UPDATE project_branch_registry
   SET active_task_id = '',
       active_claim_id = '',
       updated_by = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND project_id = ?
   AND branch_id = ?
   AND status = ?
   AND (active_task_id <> '' OR active_claim_id <> '')`,
			actorID, now, workspaceID, projectID, branchID, ProjectBranchStatusMerged)
		if err != nil {
			return nil, fmt.Errorf("clear merged patch queue source branch active refs: %w", err)
		}
		if affected, _ := branchResult.RowsAffected(); affected > 0 {
			branchRefsCleared += int(affected)
		}
	}

	if len(terminalizedTasks) == 0 && branchRefsCleared == 0 && checkoutRefsCleared == 0 {
		return nil, nil
	}
	result := map[string]any{
		"schema":                   "project_patch_queue_integration_terminalization.v1",
		"terminalized_task_ids":    terminalizedTasks,
		"completed_claim_task_ids": completedClaims,
		"branch_refs_cleared":      branchRefsCleared,
		"checkout_refs_cleared":    checkoutRefsCleared,
		"queue_id":                 queueID,
		"item_id":                  itemID,
		"branch_id":                branchID,
		"head_sha":                 headSHA,
	}
	dedupKey := projectPatchQueueIntegrationTerminalizationDedupKey(workspaceID, queueID, itemID, input.TargetBranch, input.TargetHeadAfter)
	if _, err := s.getRuntimeEventByDedupKeyTx(ctx, tx, workspaceID, dedupKey); err == nil {
		result["terminalization_event_already_recorded"] = true
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue integration terminalization replay"); err != nil {
			return nil, err
		}
		return result, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		DedupKey:    dedupKey,
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.integration_terminalized",
		EntityType:  "project_patch_queue_item",
		EntityID:    queueID + "/" + itemID,
		ActorType:   firstNonEmpty(strings.TrimSpace(actorType), "agent"),
		ActorID:     actorID,
		PayloadJSON: mustJSON(result),
		CreatedAt:   now,
	}); err != nil {
		return nil, err
	}
	if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue integration terminalization"); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) RecordProjectPatchQueueRollbackEvidenceWithEvent(ctx context.Context, input ProjectPatchQueueRollbackRecordInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue rollback evidence tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.RepoAuthorityMode != ProjectPatchQueueAuthorityModeControlledQueue {
			return fmt.Errorf("%w: patch queue item %s/%s must use controlled queue authority before rollback evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.State != ProjectPatchQueueStateClaimed {
			return fmt.Errorf("%w: patch queue item %s/%s must be CLAIMED before rollback evidence, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if !projectPatchQueueClaimActiveAt(current, nowTime) {
			return fmt.Errorf("%w: patch queue claim for %s/%s is expired; reclaim before rollback evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
			return err
		}
		if !ProjectPatchQueueOperationBindingReady(current) {
			return fmt.Errorf("%w: patch queue item %s/%s requires verified operation binding before rollback evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if !ProjectPatchQueueCASEvidenceReady(current) {
			return fmt.Errorf("%w: patch queue item %s/%s requires verified CAS evidence before rollback evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if projectPatchQueueRollbackEvidencePresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s already has rollback evidence", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}

		item = current
		item.RollbackEvidenceSchema = ProjectPatchQueueRollbackEvidenceSchema
		item.RollbackEvidenceAccepted = true
		item.RollbackEvidence = input.RollbackEvidence
		item.RollbackRecordedBy = actorID
		item.RollbackRecordedAt = now
		item.UpdatedAt = now
		if err := normalizeProjectPatchQueueRollbackEvidenceRecord(&item); err != nil {
			return err
		}
		if err := validateProjectPatchQueueRollbackEvidence(item); err != nil {
			return err
		}
		if err := updateProjectPatchQueueRollbackEvidenceTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue rollback evidence"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.rollback_record"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.rollback_record"), map[string]string{
			"workspace_id":             workspaceID,
			"project_id":               projectID,
			"queue_id":                 queueID,
			"item_id":                  itemID,
			"operation_id":             strings.TrimSpace(item.OperationID),
			"operation_kind":           strings.TrimSpace(item.OperationKind),
			"rollback_operation_id":    strings.TrimSpace(item.RollbackEvidence.RollbackOperationID),
			"rollback_operation_kind":  strings.TrimSpace(item.RollbackEvidence.RollbackOperationKind),
			"rollback_evidence_digest": strings.TrimSpace(item.RollbackEvidenceDigest),
			"actor_id":                 actorID,
			"principal_type":           effectiveActorType,
			"principal_id":             actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.rollback_recorded",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue rollback evidence tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx context.Context, input ProjectPatchQueueReviewerAdvisoryRecordInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue reviewer advisory tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.RepoAuthorityMode != ProjectPatchQueueAuthorityModeControlledQueue {
			return fmt.Errorf("%w: patch queue item %s/%s must use controlled queue authority before reviewer advisory", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		defeatsAccepted, err := projectPatchQueueReviewerAdvisoryDefeatsAcceptedItem(current, input.ReviewerAdvisory)
		if err != nil {
			return err
		}
		switch current.State {
		case ProjectPatchQueueStateClaimed:
			if !projectPatchQueueClaimActiveAt(current, nowTime) {
				return fmt.Errorf("%w: patch queue claim for %s/%s is expired; reclaim before reviewer advisory", ErrProjectPatchQueueInvalid, queueID, itemID)
			}
			if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
				return err
			}
			if !ProjectPatchQueueRollbackEvidenceReady(current) {
				return fmt.Errorf("%w: patch queue item %s/%s requires verified rollback evidence before reviewer advisory", ErrProjectPatchQueueInvalid, queueID, itemID)
			}
		case ProjectPatchQueueStateAccepted:
			if !defeatsAccepted {
				return fmt.Errorf("%w: accepted patch queue item %s/%s only accepts same-head lane-correctness reviewer advisory defects before integration", ErrProjectPatchQueueInvalid, queueID, itemID)
			}
		default:
			return fmt.Errorf("%w: patch queue item %s/%s must be CLAIMED or same-head ACCEPTED before reviewer advisory, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if projectPatchQueueReviewerAdvisoryPresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s already has reviewer advisory", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}

		item = current
		item.ReviewerAdvisorySchema = ProjectPatchQueueReviewerAdvisorySchema
		item.ReviewerAdvisoryAccepted = true
		item.ReviewerAdvisory = input.ReviewerAdvisory
		item.ReviewerRecordedBy = actorID
		item.ReviewerRecordedAt = now
		item.UpdatedAt = now
		if err := normalizeProjectPatchQueueReviewerAdvisoryRecord(&item); err != nil {
			return err
		}
		if err := validateProjectPatchQueueReviewerAdvisory(item); err != nil {
			return err
		}
		if err := updateProjectPatchQueueReviewerAdvisoryTx(ctx, tx, item); err != nil {
			return err
		}
		if defeatsAccepted {
			item.State = ProjectPatchQueueStateBlocked
			item.DecisionDocKey = strings.TrimSpace(item.ReviewerAdvisory.ReviewDocKey)
			item.DecisionSummary = "Acceptance defeated by same-head lane reviewer advisory: " + strings.TrimSpace(item.ReviewerAdvisory.Summary)
			item.DecidedBy = actorID
			item.DecidedAt = now
			item.ClaimedBy = ""
			item.ClaimToken = ""
			item.ClaimedAt = ""
			item.ClaimExpiresAt = ""
			item.UpdatedAt = now
			if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
				return err
			}
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue reviewer advisory"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.reviewer_advisory_record"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.reviewer_advisory_record"), map[string]string{
			"workspace_id":             workspaceID,
			"project_id":               projectID,
			"queue_id":                 queueID,
			"item_id":                  itemID,
			"operation_id":             strings.TrimSpace(item.OperationID),
			"cas_patch_digest":         strings.TrimSpace(item.CASPatchDigest),
			"rollback_evidence_digest": strings.TrimSpace(item.RollbackEvidenceDigest),
			"reviewer_advisory_digest": strings.TrimSpace(item.ReviewerAdvisoryDigest),
			"reviewer_id":              strings.TrimSpace(item.ReviewerAdvisory.ReviewerID),
			"acceptance_defeated":      fmt.Sprint(defeatsAccepted),
			"advisory_scope":           strings.TrimSpace(item.ReviewerAdvisory.Scope),
			"advisory_head_sha":        strings.TrimSpace(item.ReviewerAdvisory.HeadSHA),
			"actor_id":                 actorID,
			"principal_type":           effectiveActorType,
			"principal_id":             actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.reviewer_advisory_recorded",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		if appendErr != nil {
			return appendErr
		}
		if defeatsAccepted {
			record, err := s.upsertProjectPatchQueueDecisionContinuationTx(ctx, tx, item, event, now)
			if err != nil {
				return err
			}
			return s.materializeProjectPatchQueueDecisionContinuationTx(ctx, tx, authority, record, item, actorID, effectiveActorType, now)
		}
		return nil
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue reviewer advisory tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) RecordProjectPatchQueueOperatorEnablementWithEvent(ctx context.Context, input ProjectPatchQueueOperatorEnablementRecordInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue operator enablement tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(effectiveActorType), "agent") {
			return fmt.Errorf("%w: operator enablement requires a non-agent operator principal", ErrProjectPatchQueueInvalid)
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.RepoAuthorityMode != ProjectPatchQueueAuthorityModeControlledQueue {
			return fmt.Errorf("%w: patch queue item %s/%s must use controlled queue authority before operator enablement", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.State != ProjectPatchQueueStateClaimed {
			return fmt.Errorf("%w: patch queue item %s/%s must be CLAIMED before operator enablement, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if !projectPatchQueueClaimActiveAt(current, nowTime) {
			return fmt.Errorf("%w: patch queue claim for %s/%s is expired; reclaim before operator enablement", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
			return err
		}
		if !ProjectPatchQueueReviewerAdvisoryReady(current) {
			return fmt.Errorf("%w: patch queue item %s/%s requires verified reviewer advisory before operator enablement", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if projectPatchQueueOperatorEnablementPresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s already has operator enablement", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}

		item = current
		item.OperatorEnablementSchema = ProjectPatchQueueOperatorEnablementSchema
		item.OperatorEnablementAccepted = true
		item.OperatorEnablement = input.OperatorEnablement
		item.OperatorEnabledBy = actorID
		item.OperatorEnabledAt = now
		item.UpdatedAt = now
		if err := normalizeProjectPatchQueueOperatorEnablementRecord(&item); err != nil {
			return err
		}
		if err := validateProjectPatchQueueOperatorEnablement(item); err != nil {
			return err
		}
		if err := updateProjectPatchQueueOperatorEnablementTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue operator enablement"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.operator_enablement_record"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.operator_enablement_record"), map[string]string{
			"workspace_id":               workspaceID,
			"project_id":                 projectID,
			"queue_id":                   queueID,
			"item_id":                    itemID,
			"operation_id":               strings.TrimSpace(item.OperationID),
			"cas_patch_digest":           strings.TrimSpace(item.CASPatchDigest),
			"rollback_evidence_digest":   strings.TrimSpace(item.RollbackEvidenceDigest),
			"reviewer_advisory_digest":   strings.TrimSpace(item.ReviewerAdvisoryDigest),
			"operator_enablement_digest": strings.TrimSpace(item.OperatorEnablementDigest),
			"operator_enabled_by":        strings.TrimSpace(item.OperatorEnablement.EnabledBy),
			"actor_id":                   actorID,
			"principal_type":             effectiveActorType,
			"principal_id":               actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.operator_enablement_recorded",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue operator enablement tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) ReleaseProjectPatchQueueClaimWithEvent(ctx context.Context, input ProjectPatchQueueReleaseInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue release tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.State != ProjectPatchQueueStateClaimed {
			return fmt.Errorf("%w: patch queue item %s/%s is %s and cannot be released", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
			return err
		}
		if projectPatchQueueOperationBindingEvidencePresent(current) {
			return fmt.Errorf("%w: patch queue item %s/%s has operation binding evidence and must be decided or canceled, not released", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		item = current
		item.State = ProjectPatchQueueStateProposed
		item.ClaimedBy = ""
		item.ClaimToken = ""
		item.ClaimedAt = ""
		item.ClaimExpiresAt = ""
		item.UpdatedAt = now
		if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue release"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectPatchQueueEventPayload(item, actorID, "patch_queue.release"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.release"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"queue_id":       queueID,
			"item_id":        itemID,
			"actor_id":       actorID,
			"principal_type": effectiveActorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.released",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue release tx: %w", err)
	}
	return item, event, nil
}

func (s *Store) releaseProjectPatchQueueClaimsForReleasedTaskTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, taskID, agentID, actorType, actorID, now string) (projectPatchQueueClaimReleaseReport, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	actorType = strings.TrimSpace(actorType)
	if actorType == "" {
		actorType = "agent"
	}
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		actorID = agentID
	}
	if workspaceID == "" || taskID == "" || agentID == "" {
		return projectPatchQueueClaimReleaseReport{}, nil
	}

	var projectID, requirementsJSON string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(project_id, ''), COALESCE(task_requirements_json, '{}')
  FROM tasks
 WHERE task_id = ?`, taskID).Scan(&projectID, &requirementsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return projectPatchQueueClaimReleaseReport{}, nil
		}
		return projectPatchQueueClaimReleaseReport{}, fmt.Errorf("load task patch queue release identity: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:               taskID,
		ProjectID:            strings.TrimSpace(projectID),
		TaskRequirementsJSON: normalizeTaskRequirementsJSON(requirementsJSON),
	}
	identity, ok := projectPatchQueueTaskClaimReleaseIdentityFromTask(task)
	if !ok {
		return projectPatchQueueClaimReleaseReport{}, nil
	}
	identity.ProjectID = firstNonEmpty(identity.ProjectID, strings.TrimSpace(projectID))
	if identity.ProjectID == "" {
		return projectPatchQueueClaimReleaseReport{}, nil
	}

	candidates, err := projectPatchQueueReleaseCandidatesForTaskIdentityTx(ctx, tx, workspaceID, identity)
	if err != nil {
		return projectPatchQueueClaimReleaseReport{}, err
	}
	report := projectPatchQueueClaimReleaseReport{}
	seen := map[string]struct{}{}
	for _, current := range candidates {
		key := current.QueueID + "/" + current.ItemID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if current.WorkspaceID != workspaceID || current.ProjectID != identity.ProjectID {
			continue
		}
		if current.State != ProjectPatchQueueStateClaimed {
			continue
		}
		if strings.TrimSpace(current.ClaimedBy) != agentID {
			report.Skipped = append(report.Skipped, map[string]any{
				"queue_id":   current.QueueID,
				"item_id":    current.ItemID,
				"branch_id":  current.BranchID,
				"reason":     "claim_owner_mismatch",
				"claimed_by": strings.TrimSpace(current.ClaimedBy),
			})
			continue
		}
		if projectPatchQueueOperationBindingEvidencePresent(current) {
			report.Skipped = append(report.Skipped, map[string]any{
				"queue_id":  current.QueueID,
				"item_id":   current.ItemID,
				"branch_id": current.BranchID,
				"reason":    "operation_binding_present",
			})
			continue
		}
		previousClaimedBy := strings.TrimSpace(current.ClaimedBy)
		item := current
		item.State = ProjectPatchQueueStateProposed
		item.ClaimedBy = ""
		item.ClaimToken = ""
		item.ClaimedAt = ""
		item.ClaimExpiresAt = ""
		item.UpdatedAt = now
		if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
			return projectPatchQueueClaimReleaseReport{}, err
		}
		payload := projectPatchQueueEventPayload(item, actorID, "patch_queue.release_by_task_release")
		payload["released_by_task_release"] = true
		payload["release_task_id"] = taskID
		payload["release_task_agent_id"] = agentID
		payload["released_claim_owner"] = previousClaimedBy
		payload["patch_queue_task_kind"] = identity.TaskKind
		if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.released",
			EntityType:  "project_patch_queue_item",
			EntityID:    item.QueueID + "/" + item.ItemID,
			ActorType:   actorType,
			ActorID:     actorID,
			AgentID:     agentID,
			TaskID:      taskID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		}); err != nil {
			return projectPatchQueueClaimReleaseReport{}, err
		}
		report.Released = append(report.Released, map[string]any{
			"queue_id":  item.QueueID,
			"item_id":   item.ItemID,
			"branch_id": item.BranchID,
			"state":     item.State,
		})
	}
	return report, nil
}

func projectPatchQueueTaskClaimReleaseIdentityFromTask(task WorkspaceTaskRecord) (projectPatchQueueTaskClaimReleaseIdentity, bool) {
	projectID := firstNonEmpty(
		agentWorkTaskRequirementString(task, "project_id"),
		task.ProjectID,
	)
	queueID := agentWorkTaskRequirementString(task, "queue_id")
	itemID := agentWorkTaskRequirementString(task, "item_id")
	branchID := agentWorkTaskRequirementString(task, "branch_id")
	headSHA := agentWorkTaskRequirementString(task, "head_sha")
	taskKind := strings.ToLower(firstNonEmpty(
		agentWorkTaskRequirementString(task, "patch_queue_task_kind"),
		agentWorkTaskRequirementString(task, "kind"),
	))
	requiredTool := strings.ToLower(firstNonEmpty(
		agentWorkTaskRequirementString(task, "required_tool"),
		agentWorkTaskRequirementString(task, "tool"),
	))
	identitySchema := agentWorkTaskRequirementString(task, "patch_queue_task_identity")
	if strings.TrimSpace(queueID) == "" || (strings.TrimSpace(itemID) == "" && strings.TrimSpace(branchID) == "") {
		return projectPatchQueueTaskClaimReleaseIdentity{}, false
	}
	if strings.TrimSpace(identitySchema) == "" && taskKind == "" && !strings.HasPrefix(requiredTool, "project_patch_queue") {
		return projectPatchQueueTaskClaimReleaseIdentity{}, false
	}
	return projectPatchQueueTaskClaimReleaseIdentity{
		ProjectID: strings.TrimSpace(projectID),
		QueueID:   strings.TrimSpace(queueID),
		ItemID:    strings.TrimSpace(itemID),
		BranchID:  strings.TrimSpace(branchID),
		HeadSHA:   strings.TrimSpace(headSHA),
		TaskKind:  strings.TrimSpace(taskKind),
	}, true
}

func projectPatchQueueReleaseCandidatesForTaskIdentityTx(ctx context.Context, tx *sql.Tx, workspaceID string, identity projectPatchQueueTaskClaimReleaseIdentity) ([]ProjectPatchQueueItemRecord, error) {
	var candidates []ProjectPatchQueueItemRecord
	if identity.QueueID != "" && identity.ItemID != "" {
		item, ok, err := getProjectPatchQueueItemTx(ctx, tx, identity.QueueID, identity.ItemID)
		if err != nil {
			return nil, err
		}
		if ok {
			candidates = append(candidates, item)
		}
	}
	if identity.BranchID != "" {
		item, ok, err := getLiveProjectPatchQueueItemByBranchTx(ctx, tx, workspaceID, identity.ProjectID, identity.BranchID)
		if err != nil {
			return nil, err
		}
		if ok {
			candidates = append(candidates, item)
		}
	}
	return candidates, nil
}

func (s *Store) DecideProjectPatchQueueItemWithEvent(ctx context.Context, input ProjectPatchQueueDecisionInput) (ProjectPatchQueueItemRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	queueID := strings.TrimSpace(input.QueueID)
	itemID := strings.TrimSpace(input.ItemID)
	actorID := strings.TrimSpace(input.ActorID)
	decision, err := normalizeProjectPatchQueueDecision(input.Decision)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	summary := strings.TrimSpace(input.DecisionSummary)
	checkedSourceDocKeys := normalizeProjectPatchQueueDecisionSourceDocKeys(input.CheckedSourceDocKeys)
	if workspaceID == "" || projectID == "" || queueID == "" || itemID == "" || actorID == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, queue_id, item_id, and actor_id are required")
	}
	if summary == "" {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: decision_summary is required", ErrProjectPatchQueueInvalid)
	}
	if input.PromptContextEnvelope == nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, err
	}
	// Anti-gaming NO-VENDORING (ground-truth): compute the verdict BEFORE the write-tx so the
	// go.mod read off the managed-remote never execs git under the SQLite write lock. Enforced
	// inside the accept block below. Scoped opt-in; no-op for non-interpreter projects.
	var antiVendoringVerdict interpreterVendoringVerdict
	var diffClaimDiffVerdict diffClaimVerdict
	if decision == ProjectPatchQueueStateAccepted {
		antiVendoringVerdict, err = s.computeInterpreterVendoringVerdict(ctx, workspaceID, projectID, queueID, itemID)
		if err != nil {
			return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: compute no-vendoring verdict: %v", ErrProjectPatchQueueInvalid, err)
		}
		diffClaimDiffVerdict, err = s.computeDiffClaimDiffStats(ctx, workspaceID, projectID, queueID, itemID)
		if err != nil {
			return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: compute diff-implements-claim verdict: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project patch queue decision tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var item ProjectPatchQueueItemRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectPatchQueueIntegrationActorTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now); err != nil {
			return err
		}
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		if !ok || current.WorkspaceID != workspaceID || current.ProjectID != projectID {
			return fmt.Errorf("%w: patch queue item %s/%s not found in project", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if current.State != ProjectPatchQueueStateClaimed {
			return fmt.Errorf("%w: patch queue item %s/%s must be CLAIMED before decision, got %s", ErrProjectPatchQueueInvalid, queueID, itemID, current.State)
		}
		if !projectPatchQueueClaimActiveAt(current, nowTime) {
			return fmt.Errorf("%w: patch queue claim for %s/%s is expired; reclaim before recording a decision", ErrProjectPatchQueueInvalid, queueID, itemID)
		}
		if err := requireProjectPatchQueueClaimOwner(current, actorID, effectiveActorType, input.ClaimToken); err != nil {
			return err
		}
		// Reviewer independence (S1 R03 caveat): the submitter may withdraw, block, or reject
		// its OWN candidate, but may never ACCEPT it - acceptance is the adversarial-review
		// receipt and requires an independent decider. Without this, an implementer that
		// claims the generated review task for its own item can self-accept (observed live:
		// for example, when submitted_by and decided_by identify the same actor).
		if strings.EqualFold(decision, ProjectPatchQueueStateAccepted) &&
			strings.TrimSpace(current.SubmittedBy) != "" &&
			strings.TrimSpace(current.SubmittedBy) == actorID {
			return fmt.Errorf("%w: submitter %s cannot accept own patch queue candidate %s/%s; an independent reviewer must record the accept decision (block/reject/cancel by the submitter remain allowed)", ErrProjectPatchQueueInvalid, actorID, queueID, itemID)
		}
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, current); err != nil {
			return err
		}
		decisionDocKey := strings.TrimSpace(input.DecisionDocKey)
		decisionText := summary
		if decisionDocKey != "" {
			if doc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, decisionDocKey); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("%w: decision_doc_key %s not found", ErrProjectPatchQueueInvalid, decisionDocKey)
				}
				return err
			} else if doc.ArchivedAt != nil {
				return fmt.Errorf("%w: decision_doc_key %s is archived", ErrProjectPatchQueueInvalid, decisionDocKey)
			} else {
				decisionText = summary + "\n" + doc.Content
			}
		}
		if len(checkedSourceDocKeys) > 0 {
			decisionText += "\n" + renderProjectPatchQueueDecisionCheckedSourceDocKeys(checkedSourceDocKeys)
		}
		if decision == ProjectPatchQueueStateBlocked && projectPatchQueueDecisionSummaryLooksLikeActorAuthorityGap(summary) {
			return fmt.Errorf("%w: BLOCKED patch queue decisions must describe candidate defects or missing candidate evidence, not the reviewer's missing integration authority; release the item, request/repair role authority, or record a non-terminal review/advisory note instead", ErrProjectPatchQueueInvalid)
		}
		if decision == ProjectPatchQueueStateAccepted {
			fidelity, err := s.projectSourceFidelityContextTx(ctx, tx, workspaceID, projectID)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
			}
			if err := s.validateProjectSourceFidelityTraceTx(ctx, tx, workspaceID, fidelity); err != nil {
				return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
			}
			if fidelity.Required {
				if err := validateProjectPatchQueueAcceptedSourceFidelity(decisionText, fidelity); err != nil {
					return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
				}
			}
			branch, branchOK, err := validateProjectBranchIDScopeTx(ctx, tx, current.BranchID, workspaceID, projectID, current.RepoID)
			if err != nil {
				return err
			}
			if !branchOK {
				branch = ProjectBranchRecord{
					BranchID:   current.BranchID,
					BranchName: current.BranchID,
					HeadSHA:    current.HeadSHA,
				}
			}
			if err := s.validateProjectPatchQueueAcceptedVisualAcceptanceTx(ctx, tx, workspaceID, current, branch, summary, decisionDocKey); err != nil {
				return err
			}
			// Anti-gaming NO-VENDORING (ground-truth, fail-closed): the verdict was computed
			// pre-tx from go.mod at the candidate head. Block accept if the product interpreter
			// vendors a third-party interpreter/runtime, or if the head drifted under the check.
			if antiVendoringVerdict.Required {
				if strings.TrimSpace(current.HeadSHA) != strings.TrimSpace(antiVendoringVerdict.HeadSHA) {
					return fmt.Errorf("%w: NO-VENDORING ground-truth check observed candidate head drift during accept; re-run accept", ErrProjectPatchQueueInvalid)
				}
				if len(antiVendoringVerdict.Violations) > 0 {
					return fmt.Errorf("%w: ACCEPTED candidate fails NO-VENDORING (the product interpreter must be roster-built, not vendored): %s", ErrProjectPatchQueueInvalid, strings.Join(antiVendoringVerdict.Violations, "; "))
				}
			}
			// Anti-gaming diff-implements-claim (ground-truth, fail-closed): a structured capability
			// claim in the decision text must be backed by substantive implementation in the
			// base..head diff (not a dep-add/stub/delegation, not just "tests green").
			if diffClaimDiffVerdict.Required {
				if strings.TrimSpace(current.HeadSHA) != strings.TrimSpace(diffClaimDiffVerdict.HeadSHA) {
					return fmt.Errorf("%w: diff-implements-claim check observed candidate head drift during accept; re-run accept", ErrProjectPatchQueueInvalid)
				}
				if claim := parseCapabilityClaim(decisionText); claim.Present {
					if violations := diffImplementsClaimViolations(claim, diffClaimDiffVerdict.AddedLOC); len(violations) > 0 {
						return fmt.Errorf("%w: ACCEPTED candidate fails diff-implements-claim (declared capability not implemented in the diff): %s", ErrProjectPatchQueueInvalid, strings.Join(violations, "; "))
					}
				}
			}
		}
		item = current
		item.State = decision
		item.DecisionDocKey = decisionDocKey
		item.DecisionSummary = summary
		item.DecidedBy = actorID
		item.DecidedAt = now
		item.ClaimedBy = ""
		item.ClaimToken = ""
		item.ClaimedAt = ""
		item.ClaimExpiresAt = ""
		item.UpdatedAt = now
		if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue decision"); err != nil {
			return err
		}
		basePayload := projectPatchQueueEventPayload(item, actorID, "patch_queue.decision")
		if len(checkedSourceDocKeys) > 0 {
			basePayload["checked_source_doc_keys"] = checkedSourceDocKeys
		}
		payload, err := attachProjectPromptContextEnvelope(basePayload, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.patch_queue.decision"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"queue_id":       queueID,
			"item_id":        itemID,
			"decision":       decision,
			"actor_id":       actorID,
			"principal_type": effectiveActorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue." + strings.ToLower(decision),
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		if appendErr != nil {
			return appendErr
		}
		record, err := s.upsertProjectPatchQueueDecisionContinuationTx(ctx, tx, item, event, now)
		if err != nil {
			return err
		}
		return s.materializeProjectPatchQueueDecisionContinuationTx(ctx, tx, authority, record, item, actorID, effectiveActorType, now)
	}); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectPatchQueueItemRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project patch queue decision tx: %w", err)
	}
	return item, event, nil
}

func normalizeProjectPatchQueueDecisionSourceDocKeys(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range uniqueTrimmedSourceDocKeys(values) {
		value = strings.TrimSpace(value)
		if value == "" || !validSourceDocKey(value) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func renderProjectPatchQueueDecisionCheckedSourceDocKeys(keys []string) string {
	keys = normalizeProjectPatchQueueDecisionSourceDocKeys(keys)
	if len(keys) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("checked_source_doc_keys:\n")
	for _, key := range keys {
		b.WriteString("- ")
		b.WriteString(key)
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func (s *Store) upsertProjectPatchQueueDecisionContinuationTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord, event RuntimeEventRecord, now string) (ProjectPatchQueueDecisionContinuationRecord, error) {
	record := projectPatchQueueDecisionContinuationRecord(item, event, now)
	if strings.TrimSpace(record.OutboxID) == "" {
		return ProjectPatchQueueDecisionContinuationRecord{}, nil
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO project_patch_queue_decision_continuation_outbox(
	outbox_id, workspace_id, project_id, queue_id, item_id, branch_id, head_sha,
	decision, followup_kind, continuation_task_id, state, decision_event_id,
	decision_doc_key, decision_summary, payload_json, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, project_id, queue_id, item_id, decision) DO UPDATE SET
	branch_id = excluded.branch_id,
	head_sha = excluded.head_sha,
	followup_kind = excluded.followup_kind,
	continuation_task_id = excluded.continuation_task_id,
	state = excluded.state,
	decision_event_id = excluded.decision_event_id,
	decision_doc_key = excluded.decision_doc_key,
	decision_summary = excluded.decision_summary,
	payload_json = excluded.payload_json,
	updated_at = excluded.updated_at`,
		record.OutboxID,
		record.WorkspaceID,
		record.ProjectID,
		record.QueueID,
		record.ItemID,
		record.BranchID,
		record.HeadSHA,
		record.Decision,
		record.FollowupKind,
		record.ContinuationTaskID,
		record.State,
		record.DecisionEventID,
		record.DecisionDocKey,
		record.DecisionSummary,
		record.PayloadJSON,
		record.CreatedAt,
		record.UpdatedAt,
	); err != nil {
		return ProjectPatchQueueDecisionContinuationRecord{}, fmt.Errorf("upsert project patch queue decision continuation outbox: %w", err)
	}
	return record, nil
}

// reuseOrMintProjectPatchQueueDecisionContinuationCarrierTx is the SINGLE idempotent reuse-or-mint chokepoint
// shared by all three continuation drivers - the event-time materializer, the explicit consume, and the periodic
// sweep. It is the only caller of createProjectPatchQueueDecisionContinuationTaskTx, so the three drivers can no
// longer drift into three parallel mints. Given a continuation whose outbox row is already being claimed CONSUMED
// by the caller, it REUSES an already-minted carrier (matched by deterministic task id) - emitting the
// created-event if missing - otherwise mints a fresh one. Reusing instead of blindly re-minting is what stops a
// duplicate-identity continuation (e.g. a validation carrier whose task id already exists) from failing
// enforceTaskSubmitPatchQueueGateTx every cycle - the poison that, with the sweep's abort-on-first-error loop,
// wedged work-next fleet-wide (A1). Returns the carrier status and whether it was freshly created.
func (s *Store) reuseOrMintProjectPatchQueueDecisionContinuationCarrierTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord, ownerUserID, actorID, actorType, now string) (TaskStatus, bool, error) {
	taskID := strings.TrimSpace(record.ContinuationTaskID)
	if existing, ok, err := taskStatusForWorkspaceTx(ctx, tx, record.WorkspaceID, taskID); err != nil {
		return TaskStatus{}, false, err
	} else if ok {
		// Idempotent reuse: the carrier already exists (a prior materialize/consume/sweep or a racing writer won).
		if !projectPatchQueueDecisionContinuationTaskStatusReusable(existing, record) {
			return TaskStatus{}, false, fmt.Errorf("%w: existing patch queue continuation task %s does not match decision continuation %s", ErrProjectPatchQueueInvalid, taskID, record.OutboxID)
		}
		if eventID, err := projectPatchQueueReviewTaskEventIDTx(ctx, tx, record.WorkspaceID, taskID); err != nil {
			return TaskStatus{}, false, err
		} else if strings.TrimSpace(eventID) == "" {
			if _, err := s.appendProjectPatchQueueDecisionContinuationTaskCreatedEventTx(ctx, tx, authority, record, existing, actorID, actorType, now); err != nil {
				return TaskStatus{}, false, err
			}
		}
		return existing, false, nil
	}
	if err := s.createProjectPatchQueueDecisionContinuationTaskTx(ctx, tx, authority, record, item, ownerUserID, actorID, actorType, now); err != nil {
		return TaskStatus{}, false, err
	}
	created, ok, err := taskStatusForWorkspaceTx(ctx, tx, record.WorkspaceID, taskID)
	if err != nil {
		return TaskStatus{}, false, err
	}
	if !ok {
		return TaskStatus{}, false, fmt.Errorf("%w: patch queue continuation task %s was not readable after create", ErrProjectPatchQueueInvalid, taskID)
	}
	return created, true, nil
}

func (s *Store) materializeProjectPatchQueueDecisionContinuationTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord, actorID, actorType, now string) error {
	if strings.TrimSpace(record.OutboxID) == "" {
		return nil
	}
	// SUPPRESSED stays intentionally inert (checklist #9/#10): a consumer-less row is never revived; a row
	// with no continuation task id cannot materialize. (This is distinct from DEFERRED, which IS swept.)
	if strings.EqualFold(record.State, "SUPPRESSED") || strings.TrimSpace(record.ContinuationTaskID) == "" {
		return s.markProjectPatchQueueDecisionContinuationStateTx(ctx, tx, record.OutboxID, "SUPPRESSED", now)
	}
	taskID := strings.TrimSpace(record.ContinuationTaskID)
	if existing, ok, err := taskStatusForWorkspaceTx(ctx, tx, record.WorkspaceID, taskID); err != nil {
		return err
	} else if ok {
		// Idempotent reuse: the task already exists (a prior materialize/consume or a racing writer won).
		if !projectPatchQueueDecisionContinuationTaskStatusReusable(existing, record) {
			return fmt.Errorf("%w: existing patch queue continuation task %s does not match decision continuation %s", ErrProjectPatchQueueInvalid, taskID, record.OutboxID)
		}
		if eventID, err := projectPatchQueueReviewTaskEventIDTx(ctx, tx, record.WorkspaceID, taskID); err != nil {
			return err
		} else if strings.TrimSpace(eventID) == "" {
			if _, err := s.appendProjectPatchQueueDecisionContinuationTaskCreatedEventTx(ctx, tx, authority, record, existing, actorID, actorType, now); err != nil {
				return err
			}
		}
		return s.markProjectPatchQueueDecisionContinuationStateTx(ctx, tx, record.OutboxID, "CONSUMED", now)
	}
	// Stage 4: the decisive-path modality gate replaces the old integration-only ShouldAutoMaterialize gate
	// (which left validation/revision PENDING-with-no-consumer = #6). EVERY kind is classified and enacted as
	// exactly one of mint (NOW) / defer (AWAITING) / typed-terminal (NEVER) - never PENDING-no-consumer.
	route, resolvedOwner, err := s.decisivePathContinuationModalityTx(ctx, tx, record, item, actorID)
	if err != nil {
		return err
	}
	switch route.Route {
	case decisivePathRouteYield:
		// satisfiable_now -> claim the row FIRST (guarded), then mint a claimable carrier owned by the
		// satisfiable owner. The guard makes a racing sweep/hook a no-op, not a second mint (checklist #3/a).
		changed, err := s.markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx, tx, record.OutboxID, record.State, "CONSUMED", now)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		_, _, err = s.reuseOrMintProjectPatchQueueDecisionContinuationCarrierTx(ctx, tx, authority, record, item, resolvedOwner, actorID, actorType, now)
		return err
	case decisivePathRouteDeferred:
		// awaiting_role -> defer (observable, NOT silent PENDING); the sweep re-attempts within the TTL (I1/I2).
		_, err := s.markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx, tx, record.OutboxID, record.State, "DEFERRED", now)
		return err
	default:
		// typed_terminal_blocker (never / undetermined) -> terminal. No task minted -> no closeTaskTx, no
		// CANCELLED leg (fence by-construction, checklist #6). #4(a): a non-agent-owned carrier ends here.
		return s.markProjectPatchQueueDecisionContinuationTerminalTx(ctx, tx, record, route.Reason, now)
	}
}

// (projectPatchQueueDecisionContinuationShouldAutoMaterialize removed in stage 4: the integration-only gate
// left validation/revision PENDING-with-no-consumer (#6). The materializer now routes EVERY kind through
// the decisive-path modality - mint / defer / typed-terminal - so none stays PENDING-no-consumer.)

func (s *Store) backfillProjectPatchQueueDecisionContinuationsTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items AS p
 WHERE p.state IN ('ACCEPTED', 'REJECTED', 'BLOCKED', 'CANCELED')
   AND NOT EXISTS (
     SELECT 1
       FROM project_patch_queue_decision_continuation_outbox AS o
      WHERE o.workspace_id = p.workspace_id
        AND o.project_id = p.project_id
        AND o.queue_id = p.queue_id
        AND o.item_id = p.item_id
        AND o.decision = p.state
   )
 ORDER BY p.updated_at ASC, p.queue_id ASC, p.item_id ASC`)
	if err != nil {
		if strings.Contains(err.Error(), "no such table: project_patch_queue_decision_continuation_outbox") {
			return nil
		}
		return fmt.Errorf("list terminal patch queue items needing decision continuation backfill: %w", err)
	}
	defer rows.Close()
	var items []ProjectPatchQueueItemRecord
	for rows.Next() {
		item, err := scanProjectPatchQueueItem(rows)
		if err != nil {
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate terminal patch queue items needing decision continuation backfill: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, item := range items {
		event, ok, err := projectPatchQueueDecisionEventForItemTx(ctx, tx, item)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, err := s.upsertProjectPatchQueueDecisionContinuationTx(ctx, tx, item, event, firstNonEmpty(item.DecidedAt, item.UpdatedAt, now)); err != nil {
			return err
		}
	}
	return nil
}

func projectPatchQueueDecisionEventForItemTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) (RuntimeEventRecord, bool, error) {
	eventType := "project.patch_queue." + strings.ToLower(strings.TrimSpace(item.State))
	if strings.TrimSpace(eventType) == "project.patch_queue." {
		return RuntimeEventRecord{}, false, nil
	}
	event, ok, err := projectPatchQueueRuntimeEventForItemTx(ctx, tx, item, eventType)
	if err != nil {
		return RuntimeEventRecord{}, false, fmt.Errorf("load patch queue decision event for continuation backfill: %w", err)
	}
	return event, ok, nil
}

func projectPatchQueueRuntimeEventForItemTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord, eventType string) (RuntimeEventRecord, bool, error) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return RuntimeEventRecord{}, false, nil
	}
	var event RuntimeEventRecord
	err := tx.QueryRowContext(ctx, `
SELECT event_id, workspace_id, event_type, entity_type, entity_id, actor_type, actor_id,
       COALESCE(agent_id, ''), COALESCE(session_id, ''), COALESCE(task_id, ''),
       payload_json, created_at
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND entity_type = 'project_patch_queue_item'
   AND entity_id = ?
 ORDER BY created_at DESC, event_id DESC
 LIMIT 1`,
		strings.TrimSpace(item.WorkspaceID),
		eventType,
		strings.TrimSpace(item.QueueID)+"/"+strings.TrimSpace(item.ItemID),
	).Scan(
		&event.EventID,
		&event.WorkspaceID,
		&event.EventType,
		&event.EntityType,
		&event.EntityID,
		&event.ActorType,
		&event.ActorID,
		&event.AgentID,
		&event.SessionID,
		&event.TaskID,
		&event.PayloadJSON,
		&event.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, false, nil
		}
		return RuntimeEventRecord{}, false, fmt.Errorf("load patch queue runtime event: %w", err)
	}
	return event, true, nil
}

func projectPatchQueueDecisionContinuationRecord(item ProjectPatchQueueItemRecord, event RuntimeEventRecord, now string) ProjectPatchQueueDecisionContinuationRecord {
	decision := strings.ToUpper(strings.TrimSpace(item.State))
	followupKind := projectPatchQueueDecisionContinuationKind(decision, item.DecisionSummary)
	state := "PENDING"
	if followupKind == "" {
		followupKind = "none"
		state = "SUPPRESSED"
	}
	taskID := ""
	if state == "PENDING" {
		taskID = ProjectPatchQueueDecisionContinuationTaskID(item.ProjectID, item, followupKind)
	}
	outboxID := projectPatchQueueDecisionContinuationOutboxID(item, decision)
	payload := map[string]any{
		"workspace_id":          strings.TrimSpace(item.WorkspaceID),
		"project_id":            strings.TrimSpace(item.ProjectID),
		"repo_id":               strings.TrimSpace(item.RepoID),
		"queue_id":              strings.TrimSpace(item.QueueID),
		"item_id":               strings.TrimSpace(item.ItemID),
		"branch_id":             strings.TrimSpace(item.BranchID),
		"head_sha":              strings.TrimSpace(item.HeadSHA),
		"decision":              decision,
		"followup_kind":         followupKind,
		"continuation_task_id":  taskID,
		"state":                 state,
		"decision_event_id":     strings.TrimSpace(event.EventID),
		"decision_doc_key":      strings.TrimSpace(item.DecisionDocKey),
		"decision_summary":      strings.TrimSpace(item.DecisionSummary),
		"continuation_contract": "project_patch_queue_decision_continuation_outbox.v1",
	}
	return ProjectPatchQueueDecisionContinuationRecord{
		OutboxID:           outboxID,
		WorkspaceID:        strings.TrimSpace(item.WorkspaceID),
		ProjectID:          strings.TrimSpace(item.ProjectID),
		QueueID:            strings.TrimSpace(item.QueueID),
		ItemID:             strings.TrimSpace(item.ItemID),
		BranchID:           strings.TrimSpace(item.BranchID),
		HeadSHA:            strings.ToLower(strings.TrimSpace(item.HeadSHA)),
		Decision:           decision,
		FollowupKind:       followupKind,
		ContinuationTaskID: taskID,
		State:              state,
		DecisionEventID:    strings.TrimSpace(event.EventID),
		DecisionDocKey:     strings.TrimSpace(item.DecisionDocKey),
		DecisionSummary:    strings.TrimSpace(item.DecisionSummary),
		PayloadJSON:        mustJSON(payload),
		CreatedAt:          strings.TrimSpace(now),
		UpdatedAt:          strings.TrimSpace(now),
	}
}

func projectPatchQueueDecisionContinuationKind(decision, summary string) string {
	switch strings.ToUpper(strings.TrimSpace(decision)) {
	case ProjectPatchQueueStateAccepted:
		return "integration"
	case ProjectPatchQueueStateRejected:
		return "revision"
	case ProjectPatchQueueStateBlocked:
		return projectPatchQueueBlockedContinuationKind(summary)
	case ProjectPatchQueueStateCanceled:
		return ""
	default:
		return ""
	}
}

func projectPatchQueueDecisionSummaryLooksLikeActorAuthorityGap(summary string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	if text == "" {
		return false
	}
	authoritySignal := false
	for _, marker := range []string{
		"integrator authority",
		"integration authority",
		"active integrator",
		"active integration role",
		"lacks integrator",
		"lack integrator",
		"missing integrator",
		"without integrator",
		"lacks integration role",
		"missing integration role",
		"controlled-queue completion",
		"controlled queue completion",
	} {
		if strings.Contains(text, marker) {
			authoritySignal = true
			break
		}
	}
	if !authoritySignal {
		return false
	}
	for _, actorMarker := range []string{
		"reviewer",
		"actor",
		"principal",
		"agent",
		"claimed by",
		"claimant",
		"i lack",
		"i lacks",
		"iota lacks",
		"epsilon lacks",
		"delta lacks",
		"because",
	} {
		if strings.Contains(text, actorMarker) {
			return true
		}
	}
	return false
}

func projectPatchQueueBlockedContinuationKind(summary string) string {
	text := strings.ToLower(strings.TrimSpace(summary))
	needsValidation := strings.Contains(text, "missing evidence") ||
		strings.Contains(text, "evidence gap") ||
		strings.Contains(text, "validation evidence") ||
		strings.Contains(text, "browser validation") ||
		strings.Contains(text, "browser evidence") ||
		strings.Contains(text, "visual acceptance evidence") ||
		strings.Contains(text, "source-fidelity evidence")
	needsRevision := strings.Contains(text, "visual fail") ||
		strings.Contains(text, "visual_verdict: fail") ||
		strings.Contains(text, "source drift") ||
		strings.Contains(text, "spec drift") ||
		strings.Contains(text, "regression") ||
		strings.Contains(text, "broken") ||
		strings.Contains(text, "bug") ||
		strings.Contains(text, "overlap") ||
		strings.Contains(text, "clipping")
	if needsValidation && !needsRevision {
		return "validation"
	}
	return "revision"
}

func projectPatchQueueDecisionContinuationOutboxID(item ProjectPatchQueueItemRecord, decision string) string {
	raw := strings.Join([]string{
		strings.TrimSpace(item.WorkspaceID),
		strings.TrimSpace(item.ProjectID),
		strings.TrimSpace(item.QueueID),
		strings.TrimSpace(item.ItemID),
		strings.ToUpper(strings.TrimSpace(decision)),
	}, "\x00")
	sum := sha256.Sum256([]byte(raw))
	return "patchq-cont-" + fmt.Sprintf("%x", sum)[:24]
}

func ProjectPatchQueueDecisionContinuationTaskID(projectID string, item ProjectPatchQueueItemRecord, kind string) string {
	parts := []string{"patchq", strings.ToLower(strings.TrimSpace(kind)), strings.TrimSpace(projectID), strings.TrimSpace(item.QueueID), strings.TrimSpace(item.ItemID)}
	raw := strings.Join(parts, "-")
	sum := sha256.Sum256([]byte(raw))
	suffix := fmt.Sprintf("%x", sum)[:12]
	slug := projectPatchQueueContinuationTaskIDSlug(raw)
	const maxSlugLen = 120
	maxBaseLen := maxSlugLen - len(suffix) - 1
	if len(slug) > maxBaseLen {
		slug = strings.Trim(slug[:maxBaseLen], "-")
	}
	if slug == "" {
		slug = "item"
	}
	return "task-" + slug + "-" + suffix
}

func projectPatchQueueContinuationTaskIDSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "item"
	}
	if len(out) > 120 {
		out = strings.Trim(out[:120], "-")
	}
	return out
}

func (s *Store) ListProjectPatchQueueDecisionContinuations(ctx context.Context, filter ProjectPatchQueueDecisionContinuationFilter) ([]ProjectPatchQueueDecisionContinuationRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	clauses := []string{"1 = 1"}
	args := make([]any, 0, 6)
	if workspaceID := strings.TrimSpace(filter.WorkspaceID); workspaceID != "" {
		clauses = append(clauses, "workspace_id = ?")
		args = append(args, workspaceID)
	}
	if projectID := strings.TrimSpace(filter.ProjectID); projectID != "" {
		clauses = append(clauses, "project_id = ?")
		args = append(args, projectID)
	}
	if queueID := strings.TrimSpace(filter.QueueID); queueID != "" {
		clauses = append(clauses, "queue_id = ?")
		args = append(args, queueID)
	}
	if itemID := strings.TrimSpace(filter.ItemID); itemID != "" {
		clauses = append(clauses, "item_id = ?")
		args = append(args, itemID)
	}
	if state := strings.ToUpper(strings.TrimSpace(filter.State)); state != "" {
		clauses = append(clauses, "state = ?")
		args = append(args, state)
	}
	if kind := strings.ToLower(strings.TrimSpace(filter.FollowupKind)); kind != "" {
		clauses = append(clauses, "followup_kind = ?")
		args = append(args, kind)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT outbox_id, workspace_id, project_id, queue_id, item_id, branch_id, head_sha,
       decision, followup_kind, continuation_task_id, state, decision_event_id,
       decision_doc_key, decision_summary, payload_json, created_at, updated_at
  FROM project_patch_queue_decision_continuation_outbox
 WHERE `+strings.Join(clauses, " AND ")+`
 ORDER BY updated_at DESC, outbox_id ASC`, args...)
	if err != nil {
		return nil, fmt.Errorf("list project patch queue decision continuation outbox: %w", err)
	}
	defer rows.Close()
	var out []ProjectPatchQueueDecisionContinuationRecord
	for rows.Next() {
		var record ProjectPatchQueueDecisionContinuationRecord
		if err := rows.Scan(
			&record.OutboxID,
			&record.WorkspaceID,
			&record.ProjectID,
			&record.QueueID,
			&record.ItemID,
			&record.BranchID,
			&record.HeadSHA,
			&record.Decision,
			&record.FollowupKind,
			&record.ContinuationTaskID,
			&record.State,
			&record.DecisionEventID,
			&record.DecisionDocKey,
			&record.DecisionSummary,
			&record.PayloadJSON,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan project patch queue decision continuation outbox: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate project patch queue decision continuation outbox: %w", err)
	}
	return out, nil
}

func projectPatchQueueDecisionContinuationTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, outboxID, queueID, itemID string) (ProjectPatchQueueDecisionContinuationRecord, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	outboxID = strings.TrimSpace(outboxID)
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	clauses := []string{"workspace_id = ?", "project_id = ?"}
	args := []any{workspaceID, projectID}
	if outboxID != "" {
		clauses = append(clauses, "outbox_id = ?")
		args = append(args, outboxID)
	} else {
		clauses = append(clauses, "queue_id = ?", "item_id = ?")
		args = append(args, queueID, itemID)
	}
	row := tx.QueryRowContext(ctx, `
SELECT outbox_id, workspace_id, project_id, queue_id, item_id, branch_id, head_sha,
       decision, followup_kind, continuation_task_id, state, decision_event_id,
       decision_doc_key, decision_summary, payload_json, created_at, updated_at
  FROM project_patch_queue_decision_continuation_outbox
 WHERE `+strings.Join(clauses, " AND ")+`
 ORDER BY CASE state WHEN 'PENDING' THEN 0 WHEN 'CONSUMED' THEN 1 ELSE 2 END, updated_at DESC, outbox_id ASC
 LIMIT 1`, args...)
	var record ProjectPatchQueueDecisionContinuationRecord
	if err := row.Scan(
		&record.OutboxID,
		&record.WorkspaceID,
		&record.ProjectID,
		&record.QueueID,
		&record.ItemID,
		&record.BranchID,
		&record.HeadSHA,
		&record.Decision,
		&record.FollowupKind,
		&record.ContinuationTaskID,
		&record.State,
		&record.DecisionEventID,
		&record.DecisionDocKey,
		&record.DecisionSummary,
		&record.PayloadJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectPatchQueueDecisionContinuationRecord{}, false, nil
		}
		return ProjectPatchQueueDecisionContinuationRecord{}, false, fmt.Errorf("load project patch queue decision continuation outbox: %w", err)
	}
	return record, true, nil
}

func (s *Store) createProjectPatchQueueDecisionContinuationTaskTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord, ownerUserID, actorID, actorType, now string) error {
	taskID := strings.TrimSpace(record.ContinuationTaskID)
	if taskID == "" {
		return fmt.Errorf("%w: continuation task id is required", ErrProjectPatchQueueInvalid)
	}
	// Stage 4: the owner is resolved ONCE upstream by the decisive-path modality gate (a NOW-satisfiable
	// owner; the gate never reaches create for AWAITING/NEVER), so there is no in-create owner re-resolution
	// (the old 'system' fallback path is gone - that was the #4 born-unclaimable carrier).
	ownerUserID = strings.TrimSpace(ownerUserID)
	if ownerUserID == "" {
		return fmt.Errorf("%w: continuation owner is required; the modality gate must resolve a satisfiable owner before create", ErrProjectPatchQueueInvalid)
	}
	title := projectPatchQueueDecisionContinuationTaskTitle(record, item)
	description := projectPatchQueueDecisionContinuationTaskDescription(record, item)
	taskRequirements := projectPatchQueueDecisionContinuationTaskRequirements(record, item)
	if err := s.createTaskWithGraphTx(ctx, tx, TaskCreateInput{
		WorkspaceID:          item.WorkspaceID,
		TaskID:               taskID,
		OwnerUserID:          ownerUserID,
		Priority:             "high",
		Title:                title,
		Description:          description,
		TaskKind:             "COORDINATION",
		TaskTemplate:         "research",
		CarrierKind:          decisivePathCarrierKindForFollowup(record.FollowupKind),
		Tags:                 []string{"project", "patch_queue", strings.TrimSpace(record.FollowupKind), "decision_continuation"},
		ProjectID:            item.ProjectID,
		ProjectLane:          projectPatchQueueDecisionContinuationProjectLane(record),
		RequiresProjectGate:  true,
		TaskRequirementsJSON: string(mustJSON(taskRequirements)),
		WriteScopeHints:      projectPatchQueueContinuationWriteScopeHints(record, item),
	}, dag.DefaultGraph(), now); err != nil {
		return err
	}
	if err := s.attachTaskToWorkspaceTx(ctx, tx, TaskAttachmentInput{
		WorkspaceID: item.WorkspaceID,
		TaskID:      taskID,
		LinkedBy:    firstNonEmpty(strings.TrimSpace(actorID), strings.TrimSpace(item.DecidedBy), "system"),
	}, now); err != nil {
		return err
	}
	status, ok, err := taskStatusForWorkspaceTx(ctx, tx, item.WorkspaceID, taskID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: patch queue continuation task %s was not readable after create", ErrProjectPatchQueueInvalid, taskID)
	}
	_, err = s.appendProjectPatchQueueDecisionContinuationTaskCreatedEventTx(ctx, tx, authority, record, status, actorID, actorType, now)
	return err
}

// (projectPatchQueueDecisionContinuationOwnerTx removed in stage 4: its inline INTEGRATOR holder query +
// 'system' fallback are subsumed by decisivePathOwnerSatisfiabilityTx, which resolves the owner via the
// shared activeProjectRoleHolderTx and NEVER falls back to a non-agent 'system' owner - checklist #4.)

func (s *Store) appendProjectPatchQueueDecisionContinuationTaskCreatedEventTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record ProjectPatchQueueDecisionContinuationRecord, status TaskStatus, actorID, actorType, now string) (RuntimeEventRecord, error) {
	payload := map[string]any{
		"workspace_id":          record.WorkspaceID,
		"project_id":            record.ProjectID,
		"task_id":               status.TaskID,
		"title":                 status.Title,
		"description":           status.Description,
		"priority":              status.Priority,
		"owner_user_id":         status.OwnerUserID,
		"linked_by":             firstNonEmpty(status.OwnerUserID, strings.TrimSpace(actorID)),
		"project_lane":          status.ProjectLane,
		"requires_project_gate": status.RequiresProjectGate,
		"queue_id":              record.QueueID,
		"item_id":               record.ItemID,
		"branch_id":             record.BranchID,
		"head_sha":              record.HeadSHA,
		"decision":              record.Decision,
		"followup_kind":         record.FollowupKind,
		"decision_event_id":     record.DecisionEventID,
		"outbox_id":             record.OutboxID,
		"summary":               "Patch queue decision continuation task created for " + record.QueueID + "/" + record.ItemID,
		"status":                status.Status,
	}
	envelope := BuildTaskPromptContextEnvelope("task.patch_queue.decision_continuation.create", "server_rpc", record.WorkspaceID, firstNonEmpty(strings.TrimSpace(actorType), "agent"), strings.TrimSpace(actorID))
	var err error
	payload, err = AttachTaskPromptContextEnvelope(payload, envelope)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	agentID := ""
	if strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		agentID = strings.TrimSpace(actorID)
	}
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		DedupKey:    "task:" + status.TaskID + ":created",
		WorkspaceID: record.WorkspaceID,
		EventType:   "task.created",
		EntityType:  "task",
		EntityID:    status.TaskID,
		ActorType:   strings.TrimSpace(actorType),
		ActorID:     strings.TrimSpace(actorID),
		AgentID:     agentID,
		TaskID:      status.TaskID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
}

func (s *Store) markProjectPatchQueueDecisionContinuationStateTx(ctx context.Context, tx *sql.Tx, outboxID, state, now string) error {
	if strings.TrimSpace(outboxID) == "" || strings.TrimSpace(state) == "" {
		return fmt.Errorf("%w: continuation outbox_id and state are required", ErrProjectPatchQueueInvalid)
	}
	_, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_decision_continuation_outbox
   SET state = ?, updated_at = ?
 WHERE outbox_id = ?`,
		strings.ToUpper(strings.TrimSpace(state)),
		strings.TrimSpace(now),
		strings.TrimSpace(outboxID),
	)
	if err != nil {
		return fmt.Errorf("mark project patch queue decision continuation state: %w", err)
	}
	return nil
}

func projectPatchQueueDecisionContinuationTaskTitle(record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord) string {
	switch strings.ToLower(strings.TrimSpace(record.FollowupKind)) {
	case "integration":
		return "Integrate accepted patch queue candidate for " + strings.TrimSpace(item.ProjectID)
	case "validation":
		return "Validate blocked patch queue candidate for " + strings.TrimSpace(item.ProjectID)
	case "rebuild":
		return "Rebuild unavailable accepted patch queue candidate for " + strings.TrimSpace(item.ProjectID)
	default:
		return "Revise patch queue candidate for " + strings.TrimSpace(item.ProjectID)
	}
}

func projectPatchQueueDecisionContinuationTaskDescription(record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord) string {
	kind := strings.ToLower(strings.TrimSpace(record.FollowupKind))
	lines := []string{
		"Continue a terminal patch queue decision through a visible task instead of private coordination chatter.",
		"",
		"Project: " + item.ProjectID,
		"Repository: " + item.RepoID,
		"Patch queue: " + item.QueueID + "/" + item.ItemID,
		"Branch ID: " + item.BranchID,
		"Head SHA: " + item.HeadSHA,
		"Decision: " + strings.TrimSpace(record.Decision),
		"Follow-up kind: " + kind,
		"Decision event: " + strings.TrimSpace(record.DecisionEventID),
		"Decision doc: " + strings.TrimSpace(record.DecisionDocKey),
	}
	if paths := projectPatchQueueContinuationPathset(item); len(paths) > 0 {
		if kind == "revision" {
			lines = append(lines, "Candidate pathset: "+strings.Join(paths, ", "))
		} else {
			lines = append(lines, "Write scope hints: "+strings.Join(paths, ", "))
		}
	}
	lines = append(lines,
		"",
		"Decision summary:",
		strings.TrimSpace(firstNonEmpty(record.DecisionSummary, item.DecisionSummary)),
		"",
		"Required transition:",
	)
	switch kind {
	case "integration":
		lines = append(lines, "Run project_patch_queue_integrate or produce a typed blocker proving the accepted branch/head cannot become canonical baseline.")
	case "validation":
		lines = append(lines, "Produce fresh exact branch/head validation evidence, then use project_patch_queue_lifecycle supersede/requeue if the blocked item can be reconsidered.")
	case "rebuild":
		lines = append(lines, "Produce a fresh equivalent candidate from the project brief and acceptance evidence because the accepted source branch/head is unavailable.")
	default:
		lines = append(lines, "Revise the referenced branch/head or create a successor candidate tied to this queue/item.")
		lines = append(lines, "Treat the candidate pathset as changed-path evidence, not automatic claim scope; narrow the repair lane before claiming when neighboring live branches only share manifest/config sidecars.")
	}
	return strings.Join(lines, "\n")
}

func projectPatchQueueDecisionContinuationTaskRequirements(record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord) map[string]any {
	kind := strings.ToLower(strings.TrimSpace(record.FollowupKind))
	requirements := map[string]any{
		"patch_queue_task_kind": kind,
		"project_id":            item.ProjectID,
		"queue_id":              item.QueueID,
		"item_id":               item.ItemID,
		"branch_id":             item.BranchID,
		"head_sha":              item.HeadSHA,
		"decision":              strings.TrimSpace(record.Decision),
		"decision_event_id":     strings.TrimSpace(record.DecisionEventID),
		"decision_outbox_id":    strings.TrimSpace(record.OutboxID),
	}
	if paths := projectPatchQueueContinuationPathset(item); len(paths) > 0 {
		if kind == "revision" {
			requirements["candidate_pathset"] = paths
			requirements["candidate_pathset_role"] = "historical_changed_path_evidence_not_claim_scope"
		} else {
			requirements["write_scope_hints"] = paths
		}
	}
	if kind == "integration" {
		requirements["required_project_role"] = "INTEGRATOR"
		requirements["required_tool"] = "project_patch_queue_integrate"
		requirements["required_transition"] = "project_patch_queue_integrate_then_full_product_verify"
		requirements["integration_boundary"] = "accepted_lane_candidate_must_be_assembled_before_full_product_acceptance"
		requirements["integration_completion_gate"] = "canonical_target_build_and_verifier_mesh"
		requirements["required_evidence"] = []string{"integration_receipt", "canonical_target_head", "build_or_test_command", "full_product_verdict"}
		requirements["forbidden_substitutes"] = []string{"shell_only", "workspace_doc_put_only", "task_submit_only", "review_decision_without_integration_receipt"}
	}
	if kind == "revision" && strings.EqualFold(strings.TrimSpace(record.Decision), string(ProjectPatchQueueStateBlocked)) {
		requirements["required_transition"] = "project_patch_queue_revision_commit_review_submit"
		requirements["required_first_publication_tool"] = "project_branch_commit"
		requirements["required_tool_sequence"] = []string{"project_branch_commit", "project_branch_review_ready", "project_patch_queue_submit"}
		requirements["required_terminal_tool"] = "project_patch_queue_submit"
		requirements["historical_source_branch_role"] = "read_only_defeated_source_branch_evidence"
		requirements["live_repair_branch_required"] = true
		requirements["forbidden_substitutes"] = []string{"status_only", "workspace_doc_put_only", "historical_branch_mutation", "same_head_retry_without_revision"}
	}
	return requirements
}

func projectPatchQueueDecisionContinuationProjectLane(record ProjectPatchQueueDecisionContinuationRecord) string {
	switch strings.ToLower(strings.TrimSpace(record.FollowupKind)) {
	case "integration":
		return "integration"
	case "validation":
		return "validation"
	default:
		return "implementation"
	}
}

func projectPatchQueueContinuationWriteScopeHints(record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord) []string {
	if strings.EqualFold(strings.TrimSpace(record.FollowupKind), "revision") {
		return nil
	}
	return projectPatchQueueContinuationPathset(item)
}

func projectPatchQueueContinuationPathset(item ProjectPatchQueueItemRecord) []string {
	paths := append([]string(nil), item.Pathset...)
	if len(paths) == 0 {
		paths = projectBranchReviewScopePaths(item.PathsetJSON)
	}
	return uniqueTrimmedStrings(paths)
}

func (s *Store) validateProjectPatchQueueAcceptedVisualAcceptanceTx(ctx context.Context, tx *sql.Tx, workspaceID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, summary, decisionDocKey string) error {
	docs, err := s.projectPatchQueueAcceptedVisualAcceptanceCandidateDocsTx(ctx, tx, workspaceID, item, branch, summary, decisionDocKey)
	if err != nil {
		return err
	}
	reasons := projectPatchQueueAcceptedVisualAcceptanceRequiredReasons(item, branch, summary, docs)
	if projectPatchQueueAcceptedVisualAcceptanceHasExplicitPacketDoc(docs) {
		reasons = append(reasons, "doc:explicit_visual_packet")
	}
	reasons = uniqueTrimmedStrings(reasons)
	if len(reasons) == 0 {
		return nil
	}
	ok, evidenceDocKeys, missing, blocking := projectPatchQueueAcceptedVisualAcceptanceSatisfied(docs, item, branch)
	if ok {
		return nil
	}
	details := []string{"reasons " + strings.Join(reasons, ", ")}
	if len(evidenceDocKeys) > 0 {
		details = append(details, "evidence_doc_keys "+strings.Join(evidenceDocKeys, ", "))
	}
	if len(missing) > 0 {
		details = append(details, "missing "+strings.Join(missing, "; "))
	}
	if len(blocking) > 0 {
		details = append(details, "blocking "+strings.Join(blocking, "; "))
	}
	return fmt.Errorf("%w: ACCEPTED UI-facing patch queue item requires complete rhizome_visual_acceptance_v1 evidence; %s", ErrProjectPatchQueueInvalid, strings.Join(details, "; "))
}

func (s *Store) projectPatchQueueAcceptedVisualAcceptanceCandidateDocsTx(ctx context.Context, tx *sql.Tx, workspaceID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, summary, decisionDocKey string) ([]WorkspaceDocRecord, error) {
	keys := uniqueTrimmedStrings([]string{
		strings.TrimSpace(decisionDocKey),
		strings.TrimSpace(item.DecisionDocKey),
		strings.TrimSpace(item.ReviewDocKey),
		strings.TrimSpace(item.EvidenceDocKey),
		strings.TrimSpace(item.ReviewerAdvisory.ReviewDocKey),
	})
	docs := make([]WorkspaceDocRecord, 0, len(keys)+1)
	if strings.TrimSpace(summary) != "" {
		docs = append(docs, WorkspaceDocRecord{
			DocKey:  "inline.decision_summary",
			Title:   "Inline patch queue decision summary",
			Content: summary,
		})
	}
	for _, key := range keys {
		doc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if strings.TrimSpace(key) == strings.TrimSpace(decisionDocKey) {
					return nil, fmt.Errorf("%w: decision_doc_key %s not found", ErrProjectPatchQueueInvalid, key)
				}
				continue
			}
			return nil, err
		}
		if doc.ArchivedAt != nil {
			if strings.TrimSpace(key) == strings.TrimSpace(decisionDocKey) {
				return nil, fmt.Errorf("%w: decision_doc_key %s is archived", ErrProjectPatchQueueInvalid, key)
			}
			continue
		}
		docs = append(docs, doc)
	}
	unlinked, err := s.projectPatchQueueAcceptedVisualAcceptanceUnlinkedPacketDocsTx(ctx, tx, workspaceID, item, branch, keys)
	if err != nil {
		return nil, err
	}
	docs = append(docs, unlinked...)
	return docs, nil
}

func (s *Store) projectPatchQueueAcceptedVisualAcceptanceUnlinkedPacketDocsTx(ctx context.Context, tx *sql.Tx, workspaceID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, linkedKeys []string) ([]WorkspaceDocRecord, error) {
	seen := make(map[string]struct{}, len(linkedKeys)+1)
	for _, key := range linkedKeys {
		key = strings.TrimSpace(key)
		if key != "" {
			seen[key] = struct{}{}
		}
	}
	rows, err := tx.QueryContext(ctx, `
SELECT doc_key, title, content, updated_by, created_at, updated_at, archived_at, archived_by
  FROM workspace_docs
 WHERE workspace_id = ?
   AND archived_at IS NULL
 ORDER BY updated_at DESC, doc_key ASC
 LIMIT 500`, strings.TrimSpace(workspaceID))
	if err != nil {
		return nil, fmt.Errorf("list visual acceptance candidate docs: %w", err)
	}
	defer rows.Close()
	out := []WorkspaceDocRecord{}
	for rows.Next() {
		var doc WorkspaceDocRecord
		var archivedAt, archivedBy sql.NullString
		if err := rows.Scan(&doc.DocKey, &doc.Title, &doc.Content, &doc.UpdatedBy, &doc.CreatedAt, &doc.UpdatedAt, &archivedAt, &archivedBy); err != nil {
			return nil, fmt.Errorf("scan visual acceptance candidate doc: %w", err)
		}
		if archivedAt.Valid {
			value := archivedAt.String
			doc.ArchivedAt = &value
		}
		if archivedBy.Valid {
			value := archivedBy.String
			doc.ArchivedBy = &value
		}
		key := strings.TrimSpace(doc.DocKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
		if !projectPatchQueueAcceptedVisualAcceptanceLooksLikePacket(text) {
			continue
		}
		if projectPatchQueueSupersessionEvidenceDocIsCoordinationResponse(doc) ||
			projectPatchQueueSupersessionEvidenceDocIsAgentState(doc) ||
			projectPatchQueueSupersessionEvidenceDocIsReflectiveSummary(doc) ||
			projectPatchQueueSupersessionEvidenceDocIsTaskBrief(doc) {
			continue
		}
		if missing := projectPatchQueueAcceptedVisualAcceptanceCandidateMissing(text, item, branch); len(missing) > 0 {
			continue
		}
		doc.SHA = contentSHA256(doc.Content)
		out = append(out, doc)
		seen[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate visual acceptance candidate docs: %w", err)
	}
	return out, nil
}

func projectPatchQueueAcceptedVisualAcceptanceRequiredReasons(item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, summary string, docs []WorkspaceDocRecord) []string {
	signals := make([]string, 0, 6)
	pathSignals := projectPatchQueueAcceptedVisualAcceptancePathSignals(item, branch)
	signals = append(signals, pathSignals...)
	text := strings.ToLower(strings.Join([]string{summary, projectPatchQueueAcceptedVisualAcceptanceDocsText(docs)}, "\n"))
	coreOnly := projectPatchQueueAcceptedVisualAcceptanceCoreOnlySignals(text)
	// provablyNonUI suppresses the standalone doc-text signals ONLY for a PROVABLY non-UI pathset
	// (ALLOW-LIST polarity). This is the AUTHORITATIVE gate (DecideProjectPatchQueueItemWithEvent),
	// false-negative-critical, so the default must be GATE: anything not provably backend - unknown
	// extension, ambiguous src/, garbage, empty - keeps the doc-text signals live. Keep byte-aligned
	// with the agent twin pathsetIsProvablyNonUI (visual_acceptance.go); the doc:* needle union
	// below also matches agent (react/vite/next.js included).
	provablyNonUI := projectPatchQueueAcceptedVisualAcceptancePathsetIsProvablyNonUI(item)
	if !coreOnly && !provablyNonUI {
		if projectPatchQueueEvidenceContainsAny(text, "frontend", "front-end", "web app", "browser app", "react", "vite", "next.js", "nextjs", "tsx", "jsx") {
			signals = append(signals, "doc:frontend")
		}
		if projectPatchQueueEvidenceContainsAny(text, "ui/ux", "visual", "layout", "screenshot", "viewport", "canvas") {
			signals = append(signals, "doc:visual")
		}
	}
	return uniqueTrimmedStrings(signals)
}

// --- Visual-acceptance non-UI ALLOW-LIST (keep byte-aligned with the agent twin in
// agent/visual_acceptance.go). The gate is false-negative-critical, so suppression is an
// allow-list: a path is suppressible ONLY if it carries no UI marker AND positively lives in
// backend territory. Unknown -> gate. ---

// (No UI-extension list by design: enumerating UI extensions is deny-list thinking, and an
// incomplete list is exactly how a UI surface leaks. The allow-list lists only BACKEND extensions;
// every other extension - known-UI, unknown, or glob-dressed - gates.)

// visualGateUIDirSegments: any path segment that conventionally holds frontend/UI assets, in both
// plural and singular forms. REJECT.
var visualGateUIDirSegments = map[string]bool{
	"public": true, "web": true, "frontend": true, "client": true, "dashboard": true,
	"src": true, "app": true, "ui": true,
	"components": true, "component": true, "pages": true, "page": true,
	"static": true, "assets": true, "styles": true,
	"layouts": true, "layout": true, "themes": true, "theme": true,
	"widgets": true, "widget": true, "screens": true, "screen": true,
	"views": true, "view": true, "templates": true, "template": true, "render": true,
}

// visualGateUIManifestMarkers: frontend build manifests / configs. REJECT (substring match).
var visualGateUIManifestMarkers = []string{
	"vite.config", "next.config", "tailwind.config", "postcss.config", "svelte.config",
	"webpack.config", "rollup.config", ".storybook", "package.json", "manifest.json", "sw.js",
}

// visualGateUIFilenameMarkers: substring markers that flag a file (commonly a .go file) that renders
// a browser/HTML/visual surface - which a file EXTENSION cannot distinguish from backend Go. Checked
// BEFORE the backend-extension ALLOW so a UI-rendering .go file gates. Pragmatic and admittedly
// INCOMPLETE (a novel-named Go-UI file still leaks; the robust closure is an explicit per-item
// renders-browser-UI flag, tracked as a follow-up) - but it covers the present-day live dashboard
// set (internal/server/dashboard.go, agent/web_dashboard_*.go). REJECT (substring match).
// NOTE: "render"/"template" are deliberately NOT substring-markers here - they over-match legitimate
// interpreter files (eval/render.go, template.go). They live in visualGateUIDirSegments instead, so
// they gate only as a whole path SEGMENT (a render/ or template/ DIR), never inside a filename.
var visualGateUIFilenameMarkers = []string{
	"dashboard", "web_", "_html", "html_", "webview", "_styles", "_script",
}

// visualGateBackendExtensions: unambiguously non-UI file extensions. ALLOW (positive match).
// .lua is the Lua campaign's own source/fixture extension - never a browser surface, and reachable
// on the milestone path (the seed ships tracked .lua smoke fixtures), so it must not over-gate.
var visualGateBackendExtensions = []string{
	".go", ".lua", ".md", ".sql", ".proto", ".sh", ".txt", ".rst",
}

// visualGateBackendFiles: exact non-UI repo files. ALLOW.
var visualGateBackendFiles = map[string]bool{
	"go.mod": true, "go.sum": true, "makefile": true, "dockerfile": true,
	".gitignore": true, ".dockerignore": true,
}

// visualGateBackendRoots: top-level directories that, by Go/backend convention, hold no browser UI.
// Deliberately EXCLUDES ambiguous roots (src, app, api, server, view) so they gate. ALLOW.
var visualGateBackendRoots = map[string]bool{
	"internal": true, "cmd": true, "pkg": true, "lib": true, "vendor": true,
	"testdata": true, "test": true, "tests": true, "docs": true, "doc": true,
	"scripts": true, "examples": true, "example": true, "tools": true,
	"migrations": true, "migration": true, "proto": true,
}

// projectPatchQueueAcceptedVisualAcceptancePathsetIsProvablyNonUI reports true ONLY when the item
// declares a concrete pathset whose EVERY entry is PROVABLY non-UI. Allow-list polarity: empty/
// garbage/unknown/ambiguous all return false (gate). Resolves item.Pathset only (trim+drop empties)
// - no branch fallback - so it converges byte-for-byte with the agent twin and stays fail-safe
// on an unresolved pathset. Keep byte-aligned with the agent twin pathsetIsProvablyNonUI.
func projectPatchQueueAcceptedVisualAcceptancePathsetIsProvablyNonUI(item ProjectPatchQueueItemRecord) bool {
	cleaned := make([]string, 0, len(item.Pathset))
	for _, p := range item.Pathset {
		if t := strings.TrimSpace(p); t != "" {
			cleaned = append(cleaned, t)
		}
	}
	if len(cleaned) == 0 {
		return false // no concrete declared scope -> cannot prove non-UI -> gate
	}
	for _, p := range cleaned {
		if !projectPatchQueuePathIsProvablyNonUI(p) {
			return false // any entry not provably non-UI -> gate the whole candidate
		}
	}
	return true
}

// projectPatchQueuePathIsProvablyNonUI reports whether ONE pathset entry is provably backend: no UI
// marker AND a positive backend match. Self-contained (no inter-function calls) so its body stays
// byte-identical to the agent twin pathIsProvablyNonUI.
func projectPatchQueuePathIsProvablyNonUI(p string) bool {
	n := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))), "./")
	if n == "" {
		return false
	}
	// REJECT overrides: a UI directory segment, a UI build manifest, or a UI-rendering filename
	// marker gates even an otherwise backend-looking (.go) path.
	for _, seg := range strings.Split(n, "/") {
		if visualGateUIDirSegments[seg] {
			return false
		}
	}
	for _, marker := range visualGateUIManifestMarkers {
		if strings.Contains(n, marker) {
			return false
		}
	}
	for _, marker := range visualGateUIFilenameMarkers {
		if strings.Contains(n, marker) {
			return false
		}
	}
	// An exact backend file (go.mod, go.sum, .gitignore) suppresses despite its dotted name.
	if visualGateBackendFiles[n] {
		return true
	}
	last := n
	if i := strings.LastIndex(n, "/"); i >= 0 {
		last = n[i+1:]
	}
	// ANY dot in the last segment marks a FILE-TARGETING path (concrete, glob, or dotfile). The exact
	// backend file was already matched above; here ONLY a recognized backend EXTENSION (a non-leading
	// dot, glob-aware) suppresses. Everything else gates and must NOT fall through to the directory
	// rescue: an unrecognized extension (theme.json), an unreadable one (page.html*, foo., *.*), and a
	// stem-less leading-dot leaf (.vue, a nested .gitignore). ONLY a dot-LESS last segment
	// (internal/lexer/**, a bare *, a plain dir name) reaches the backend-root rescue.
	if dot := strings.LastIndex(last, "."); dot >= 0 {
		if dot > 0 {
			ext := strings.TrimRight(last[dot:], "*?")
			if ext != "." {
				for _, e := range visualGateBackendExtensions {
					if ext == e {
						return true
					}
				}
			}
		}
		return false
	}
	first := n
	if i := strings.Index(n, "/"); i >= 0 {
		first = n[:i]
	}
	return visualGateBackendRoots[first]
}

func projectPatchQueueAcceptedVisualAcceptancePathSignals(item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) []string {
	paths := item.Pathset
	if len(paths) == 0 {
		paths = projectBranchReviewScopePaths(firstNonEmpty(item.PathsetJSON, branch.WriteScopeJSON))
	}
	signals := make([]string, 0, len(paths))
	hasPackageJSON := false
	hasGenericSrcScope := false
	for _, p := range paths {
		normalized := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(p, "\\", "/")))
		if normalized == "" {
			continue
		}
		switch {
		case normalized == "index.html":
			signals = append(signals, "path:index.html")
		case strings.HasPrefix(normalized, "public/") || normalized == "public/**":
			signals = append(signals, "path:public")
		case strings.HasPrefix(normalized, "web/") || normalized == "web/**":
			signals = append(signals, "path:web")
		case strings.HasSuffix(normalized, ".vue") || strings.HasSuffix(normalized, ".svelte"):
			signals = append(signals, "path:ui-component")
		case (strings.Contains(normalized, "src/app") || strings.Contains(normalized, "src/components") || strings.Contains(normalized, "src/pages")) &&
			(strings.HasSuffix(normalized, ".tsx") || strings.HasSuffix(normalized, ".jsx") || strings.HasSuffix(normalized, ".css") || strings.HasSuffix(normalized, ".scss") || strings.HasSuffix(normalized, ".vue") || strings.HasSuffix(normalized, ".svelte")):
			signals = append(signals, "path:react-layout")
		case normalized == "src" || normalized == "src/**" || strings.HasPrefix(normalized, "src/"):
			hasGenericSrcScope = true
		case normalized == "app" || normalized == "app/**" || strings.HasPrefix(normalized, "app/"):
			signals = append(signals, "path:app-ui-scope")
		case normalized == "components" || normalized == "components/**" || strings.HasPrefix(normalized, "components/"):
			signals = append(signals, "path:components-ui-scope")
		case normalized == "pages" || normalized == "pages/**" || strings.HasPrefix(normalized, "pages/"):
			signals = append(signals, "path:pages-ui-scope")
		case normalized == "ui" || normalized == "ui/**" || strings.HasPrefix(normalized, "ui/"):
			signals = append(signals, "path:ui-scope")
		case normalized == "static" || normalized == "static/**" || strings.HasPrefix(normalized, "static/"):
			signals = append(signals, "path:static-ui-asset")
		case normalized == "assets" || normalized == "assets/**" || strings.HasPrefix(normalized, "assets/"):
			signals = append(signals, "path:assets-ui-asset")
		case normalized == "styles" || normalized == "styles/**" || strings.HasPrefix(normalized, "styles/"):
			signals = append(signals, "path:styles-ui-scope")
		case strings.HasSuffix(normalized, ".tsx") || strings.HasSuffix(normalized, ".jsx") || strings.HasSuffix(normalized, ".css") || strings.HasSuffix(normalized, ".scss"):
			signals = append(signals, "path:ui-asset")
		case strings.Contains(normalized, "vite.config"):
			signals = append(signals, "path:vite")
		case strings.Contains(normalized, "next.config"):
			signals = append(signals, "path:next")
		case strings.Contains(normalized, "tailwind.config"):
			signals = append(signals, "path:tailwind")
		case normalized == "package.json":
			hasPackageJSON = true
		}
	}
	joined := strings.ToLower(strings.Join(paths, "\n"))
	if hasGenericSrcScope && projectPatchQueueEvidenceContainsAny(joined, "tsx", "jsx", "component", "components", "pages") {
		signals = append(signals, "path:src-ui-scope")
	}
	if hasPackageJSON && projectPatchQueueEvidenceContainsAny(joined, "vite", "next", "react") {
		signals = append(signals, "path:package-ui")
	}
	return uniqueTrimmedStrings(signals)
}

func projectPatchQueueAcceptedVisualAcceptanceCoreOnlySignals(text string) bool {
	return projectPatchQueueEvidenceContainsAny(strings.ToLower(strings.TrimSpace(text)),
		"core-only",
		"core only",
		"core slice",
		"non-ui",
		"non ui",
		"not ui-facing",
		"not ui facing",
		"not browser-facing",
		"not browser facing",
		"no browser/app surface",
		"no browser surface",
		"no app surface",
		"no ui surface",
		"library slice",
		"normalization/export core",
		"normalization and export core",
		"src/core",
		"tests/core",
	)
}

func projectPatchQueueAcceptedVisualAcceptanceDocsText(docs []WorkspaceDocRecord) string {
	parts := make([]string, 0, len(docs)*3)
	for _, doc := range docs {
		parts = append(parts, doc.DocKey, doc.Title, doc.Content)
	}
	return strings.Join(parts, "\n")
}

func projectPatchQueueAcceptedVisualAcceptanceHasExplicitPacketDoc(docs []WorkspaceDocRecord) bool {
	for _, doc := range docs {
		if strings.EqualFold(strings.TrimSpace(doc.DocKey), "inline.decision_summary") {
			continue
		}
		text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
		if projectPatchQueueAcceptedVisualAcceptanceLooksLikePacket(text) {
			return true
		}
	}
	return false
}

func projectPatchQueueAcceptedVisualAcceptanceSatisfied(docs []WorkspaceDocRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) (bool, []string, []string, []string) {
	evidenceDocKeys := make([]string, 0, len(docs))
	missing := []string{}
	blocking := []string{}
	for _, doc := range projectPatchQueueAcceptedVisualAcceptanceSortedDocs(docs) {
		text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
		if strings.TrimSpace(text) == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(doc.DocKey), "inline.decision_summary") {
			blocking = append(blocking, projectPatchQueueAcceptedVisualAcceptanceBlockingSignals(text)...)
			continue
		}
		if projectPatchQueueSupersessionEvidenceDocIsCoordinationResponse(doc) ||
			projectPatchQueueSupersessionEvidenceDocIsAgentState(doc) ||
			projectPatchQueueSupersessionEvidenceDocIsReflectiveSummary(doc) ||
			projectPatchQueueSupersessionEvidenceDocIsTaskBrief(doc) {
			blocking = append(blocking, "visual acceptance evidence cannot be coordination response, agent state, reflection, heartbeat, or task brief doc "+strings.TrimSpace(doc.DocKey))
			continue
		}
		if !projectPatchQueueAcceptedVisualAcceptanceLooksLikePacket(text) {
			continue
		}
		evidenceDocKeys = append(evidenceDocKeys, strings.TrimSpace(doc.DocKey))
		docMissing := projectPatchQueueSupersessionVisualAcceptanceMissingRequirements(text, item)
		docMissing = append(docMissing, projectPatchQueueAcceptedVisualAcceptanceCandidateMissing(text, item, branch)...)
		docMissing = append(docMissing, projectPatchQueueAcceptedVisualAcceptanceFreshnessMissing(doc, item)...)
		docBlocking := projectPatchQueueAcceptedVisualAcceptanceBlockingSignals(text)
		blocking = append(blocking, docBlocking...)
		missing = append(missing, docMissing...)
		if len(docMissing) == 0 && len(blocking) == 0 {
			return true, evidenceDocKeys, nil, nil
		}
		return false, uniqueTrimmedStrings(evidenceDocKeys), uniqueTrimmedStrings(missing), uniqueTrimmedStrings(blocking)
	}
	if len(evidenceDocKeys) == 0 {
		missing = append(missing, "workspace doc containing rhizome_visual_acceptance_v1 plus screenshot, viewport, scenario, and visual check evidence")
	}
	return false, uniqueTrimmedStrings(evidenceDocKeys), uniqueTrimmedStrings(missing), uniqueTrimmedStrings(blocking)
}

func projectPatchQueueAcceptedVisualAcceptanceSortedDocs(docs []WorkspaceDocRecord) []WorkspaceDocRecord {
	out := append([]WorkspaceDocRecord(nil), docs...)
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.TrimSpace(out[i].UpdatedAt)
		right := strings.TrimSpace(out[j].UpdatedAt)
		if left != "" || right != "" {
			leftTime, leftOK := projectPatchQueueAcceptedVisualAcceptanceParseUpdatedAt(left)
			rightTime, rightOK := projectPatchQueueAcceptedVisualAcceptanceParseUpdatedAt(right)
			switch {
			case leftOK && rightOK:
				if !leftTime.Equal(rightTime) {
					return leftTime.After(rightTime)
				}
			case leftOK != rightOK:
				return leftOK
			case left != right:
				return left > right
			}
		}
		leftKey := strings.TrimSpace(out[i].DocKey)
		rightKey := strings.TrimSpace(out[j].DocKey)
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		return strings.TrimSpace(out[i].Title) < strings.TrimSpace(out[j].Title)
	})
	return out
}

func projectPatchQueueAcceptedVisualAcceptanceParseUpdatedAt(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func projectPatchQueueAcceptedVisualAcceptanceLooksLikePacket(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	if projectPatchQueueEvidenceContainsAny(text, "rhizome_visual_acceptance_v1") {
		return true
	}
	hasStructuredVerdict := projectPatchQueueEvidenceContainsAny(text,
		"visual_verdict:",
		"visual verdict:",
		"visual_verdict =",
		"visual verdict =",
		"visual_acceptance:",
		"visual_acceptance =",
	)
	hasEvidenceShape := projectPatchQueueEvidenceContainsAny(text, "screenshot", "screenshots", "screenshot_path", "screenshot_ref") &&
		projectPatchQueueEvidenceContainsAny(text, "viewport", "viewports", "desktop", "mobile", "narrow") &&
		projectPatchQueueEvidenceContainsAny(text, "scenario", "user scenario", "real user") &&
		projectPatchQueueEvidenceContainsAny(text, "overlap", "clipping", "contrast", "readability", "layout")
	return hasStructuredVerdict && hasEvidenceShape
}

func projectPatchQueueAcceptedVisualAcceptanceCandidateMissing(text string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) []string {
	missing := []string{}
	branchID := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.BranchID, branch.BranchID)))
	branchName := strings.ToLower(strings.TrimSpace(branch.BranchName))
	queueID := strings.ToLower(strings.TrimSpace(item.QueueID))
	itemID := strings.ToLower(strings.TrimSpace(item.ItemID))
	headSHA := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.HeadSHA, branch.HeadSHA)))
	if branchID != "" || branchName != "" || queueID != "" || itemID != "" {
		branchMatches := branchID != "" && strings.Contains(text, branchID)
		branchNameMatches := branchName != "" && strings.Contains(text, branchName)
		queueItemMatches := queueID != "" && itemID != "" && strings.Contains(text, queueID) && strings.Contains(text, itemID)
		if !branchMatches && !branchNameMatches && !queueItemMatches {
			missing = append(missing, "visual packet candidate provenance matching branch_id/branch_name or queue_id/item_id")
		}
	}
	if headSHA != "" && !strings.Contains(text, headSHA) {
		missing = append(missing, "visual packet exact head_sha matching candidate")
	}
	return uniqueTrimmedStrings(missing)
}

func projectPatchQueueAcceptedVisualAcceptanceFreshnessMissing(doc WorkspaceDocRecord, item ProjectPatchQueueItemRecord) []string {
	referenceAt := firstNonEmpty(strings.TrimSpace(item.CreatedAt), strings.TrimSpace(item.UpdatedAt))
	if referenceAt == "" || strings.TrimSpace(doc.UpdatedAt) == "" {
		return nil
	}
	docTime, docErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(doc.UpdatedAt))
	refTime, refErr := time.Parse(time.RFC3339Nano, referenceAt)
	if docErr != nil || refErr != nil {
		if strings.TrimSpace(doc.UpdatedAt) >= referenceAt {
			return nil
		}
		return []string{"visual packet newer than patch queue item creation"}
	}
	if docTime.Before(refTime) {
		return []string{"visual packet newer than patch queue item creation"}
	}
	return nil
}

func projectPatchQueueAcceptedVisualAcceptanceBlockingSignals(text string) []string {
	text = strings.ToLower(strings.TrimSpace(text))
	blocking := []string{}
	if text == "" {
		return nil
	}
	if projectPatchQueueSupersessionEvidenceHasExplicitNegativeVerdict(text) ||
		projectPatchQueueEvidenceHasStructuredFieldValue(text,
			[]string{"visual_verdict", "visualverdict", "validation_verdict", "validationverdict", "acceptance_status", "candidate_status"},
			[]string{"fail", "failed", "block", "blocked", "under_evidenced", "not_accepted", "provisional_non_canonical_review_target"}) {
		blocking = append(blocking, "visual packet contains blocking/non-pass verdict")
	}
	if projectPatchQueueEvidenceContainsAny(text,
		"dirty_checkout: true",
		"dirty checkout: true",
		"dirty_state: dirty",
		"candidate_status: provisional_non_canonical_review_target",
		"provisional_non_canonical_review_target",
	) {
		blocking = append(blocking, "visual packet is dirty or provisional non-canonical evidence")
	}
	return uniqueTrimmedStrings(blocking)
}

func (s *Store) ListProjectPatchQueueItems(ctx context.Context, filter ProjectPatchQueueListFilter) ([]ProjectPatchQueueItemRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT ` + projectPatchQueueItemSelectColumns + `
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if repoID := strings.TrimSpace(filter.RepoID); repoID != "" {
		query += ` AND repo_id = ?`
		args = append(args, repoID)
	}
	if branchID := strings.TrimSpace(filter.BranchID); branchID != "" {
		query += ` AND branch_id = ?`
		args = append(args, branchID)
	}
	if state := strings.ToUpper(strings.TrimSpace(filter.State)); state != "" {
		query += ` AND state = ?`
		args = append(args, state)
	}
	query += ` ORDER BY updated_at DESC, queue_id ASC, item_id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project patch queue items: %w", err)
	}
	defer rows.Close()
	var items []ProjectPatchQueueItemRecord
	for rows.Next() {
		item, err := scanProjectPatchQueueItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return s.annotateProjectPatchQueueReviewTaskReceipts(ctx, items)
}

func (s *Store) FirstProjectRepoMutationActivationCandidate(ctx context.Context) (ProjectRepoMutationActivationCandidate, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProjectRepoMutationActivationCandidate{}, false, fmt.Errorf("begin repo mutation activation candidate read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	items, err := listControlledProjectPatchQueueItemsTx(ctx, tx, 32)
	if err != nil {
		return ProjectRepoMutationActivationCandidate{}, false, err
	}
	for _, item := range items {
		if err := s.validateProjectPatchQueueItemEvidenceTx(ctx, tx, item); err != nil {
			continue
		}
		candidate := ProjectRepoMutationActivationCandidate{QueueItem: item}
		branch, branchOK, err := validateProjectBranchIDScopeTx(ctx, tx, item.BranchID, item.WorkspaceID, item.ProjectID, item.RepoID)
		if err != nil {
			return ProjectRepoMutationActivationCandidate{}, false, err
		}
		if branchOK {
			candidate.Branch = branch
			if checkoutID := strings.TrimSpace(branch.CheckoutID); checkoutID != "" {
				checkout, checkoutOK, err := validateProjectCheckoutIDScopeTx(ctx, tx, checkoutID, item.WorkspaceID, item.ProjectID, item.RepoID)
				if err != nil {
					return ProjectRepoMutationActivationCandidate{}, false, err
				}
				if checkoutOK {
					candidate.Checkout = checkout
				}
			}
		}
		repo, err := getProjectRepositoryTx(ctx, tx, item.WorkspaceID, item.ProjectID, item.RepoID)
		if err != nil {
			return ProjectRepoMutationActivationCandidate{}, false, err
		}
		candidate.Repository = repo
		if target, targetOK, err := findProjectRepoMutationTargetCheckoutTx(ctx, tx, repo); err != nil {
			return ProjectRepoMutationActivationCandidate{}, false, err
		} else if targetOK {
			candidate.TargetCheckout = target
		}
		if err := tx.Commit(); err != nil {
			return ProjectRepoMutationActivationCandidate{}, false, fmt.Errorf("commit repo mutation activation candidate read tx: %w", err)
		}
		return candidate, true, nil
	}
	if err := tx.Commit(); err != nil {
		return ProjectRepoMutationActivationCandidate{}, false, fmt.Errorf("commit repo mutation activation empty read tx: %w", err)
	}
	return ProjectRepoMutationActivationCandidate{}, false, nil
}

func findProjectRepoMutationTargetCheckoutTx(ctx context.Context, tx *sql.Tx, repo ProjectRepositoryRecord) (ProjectCheckoutRecord, bool, error) {
	targetBranch := strings.TrimSpace(repo.IntegrationBranch)
	if targetBranch == "" {
		targetBranch = strings.TrimSpace(repo.DefaultBranch)
	}
	if targetBranch == "" {
		return ProjectCheckoutRecord{}, false, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT checkout_id, workspace_id, project_id, repo_id, machine_id, machine_label, owner_user_id, agent_id,
       local_path, checkout_kind, branch_name, base_branch, head_sha, base_sha, dirty_state,
       active_task_id, active_claim_id, status, last_seen_at, updated_by, created_at, updated_at
  FROM project_checkout_registry
 WHERE workspace_id = ? AND project_id = ? AND repo_id = ?
   AND status = 'ACTIVE'
   AND checkout_kind = ?
   AND branch_name = ?
 ORDER BY updated_at DESC, checkout_id ASC
 LIMIT 16`,
		strings.TrimSpace(repo.WorkspaceID), strings.TrimSpace(repo.ProjectID), strings.TrimSpace(repo.RepoID),
		ProjectCheckoutKindIntegration, targetBranch)
	if err != nil {
		return ProjectCheckoutRecord{}, false, err
	}
	defer rows.Close()
	referenceAt := time.Now().UTC()
	for rows.Next() {
		checkout, err := scanProjectCheckout(rows)
		if err != nil {
			return ProjectCheckoutRecord{}, false, err
		}
		if deriveProjectCheckoutStatus(checkout, referenceAt, time.Hour) != ProjectCheckoutStatusActive {
			continue
		}
		return checkout, true, nil
	}
	if err := rows.Err(); err != nil {
		return ProjectCheckoutRecord{}, false, err
	}
	return ProjectCheckoutRecord{}, false, nil
}

func normalizeProjectPatchQueuePathsetJSON(raw string) (string, []string, error) {
	paths, err := projectPatchQueuePathsetPaths(raw)
	if err != nil {
		return "", nil, err
	}
	if len(paths) == 0 {
		return "", nil, fmt.Errorf("%w: pathset_json must contain non-empty paths", ErrProjectPatchQueueInvalid)
	}
	sort.Strings(paths)
	encoded, err := json.Marshal(paths)
	if err != nil {
		return "", nil, err
	}
	return string(encoded), paths, nil
}

func projectPatchQueuePathsetPaths(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, nil
	}
	seen := map[string]struct{}{}
	paths := []string{}
	add := func(value string) error {
		path, err := repoauthority.NormalizePath(value)
		if err != nil {
			return fmt.Errorf("%w: pathset_json path %q invalid: %v", ErrProjectPatchQueueInvalid, value, err)
		}
		if _, ok := seen[path]; ok {
			return nil
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
		return nil
	}
	var walk func(any) error
	walk = func(value any) error {
		switch typed := value.(type) {
		case string:
			return add(typed)
		case []any:
			for _, item := range typed {
				if err := walk(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			if err := walk(object[key]); err != nil {
				return nil, err
			}
		}
		return paths, nil
	}
	if err := walk(decoded); err != nil {
		return nil, err
	}
	return paths, nil
}

func normalizeProjectPatchQueueBindingRecord(item *ProjectPatchQueueItemRecord, actorType, actorID string) error {
	return normalizeProjectPatchQueueBindingRecordWithPrincipalGuard(item, actorType, actorID, true)
}

func normalizeProjectPatchQueueBindingRecordWithPrincipalGuard(item *ProjectPatchQueueItemRecord, actorType, actorID string, requirePrincipalActorMatch bool) error {
	if item == nil {
		return nil
	}
	item.TaskID = strings.TrimSpace(item.TaskID)
	item.SessionID = strings.TrimSpace(item.SessionID)
	item.RunID = strings.TrimSpace(item.RunID)
	item.AgentID = strings.TrimSpace(item.AgentID)
	item.PrincipalType = strings.TrimSpace(item.PrincipalType)
	item.PrincipalID = strings.TrimSpace(item.PrincipalID)
	item.CapabilitySnapshotID = strings.TrimSpace(item.CapabilitySnapshotID)
	item.CapabilitySnapshotSchema = strings.TrimSpace(item.CapabilitySnapshotSchema)
	item.RepoRoot = strings.TrimSpace(item.RepoRoot)
	item.BaseTreeHash = strings.TrimSpace(item.BaseTreeHash)
	item.ContextDigest = strings.TrimSpace(item.ContextDigest)
	item.RepoLeaseID = strings.TrimSpace(item.RepoLeaseID)
	item.OperationID = strings.TrimSpace(item.OperationID)
	item.OperationKind = strings.TrimSpace(item.OperationKind)
	if item.OperationID != "" || item.OperationKind != "" {
		return fmt.Errorf("%w: operation refs cannot be submitted with a patch queue proposal; bind mutation operations through durable operation evidence", ErrProjectPatchQueueInvalid)
	}

	baseFileHashesJSON, baseFileHashes, err := normalizeProjectPatchQueueBaseFileHashes(item.BaseFileHashesJSON, item.BaseFileHashes)
	if err != nil {
		return err
	}
	item.BaseFileHashesJSON = baseFileHashesJSON
	item.BaseFileHashes = baseFileHashes

	if !projectPatchQueueBindingRefsPresent(*item) {
		return nil
	}
	actorType = strings.TrimSpace(actorType)
	actorID = strings.TrimSpace(actorID)
	if requirePrincipalActorMatch {
		if item.PrincipalType != "" && actorType != "" && !strings.EqualFold(item.PrincipalType, actorType) {
			return fmt.Errorf("%w: patch queue binding principal_type must match authenticated actor type", ErrProjectPatchQueueInvalid)
		}
		if item.PrincipalID != "" && actorID != "" && item.PrincipalID != actorID {
			return fmt.Errorf("%w: patch queue binding principal_id must match authenticated actor id", ErrProjectPatchQueueInvalid)
		}
	}
	if item.PrincipalType == "" {
		item.PrincipalType = actorType
	} else if requirePrincipalActorMatch && actorType != "" {
		item.PrincipalType = actorType
	}
	if item.PrincipalID == "" {
		item.PrincipalID = actorID
	}
	if item.AgentID == "" && strings.EqualFold(item.PrincipalType, "agent") {
		item.AgentID = item.PrincipalID
	}
	context := projectPatchQueueBindingContext(*item, false)
	digest, err := context.Digest()
	if err != nil {
		return fmt.Errorf("%w: patch queue binding context is incomplete: %v", ErrProjectPatchQueueInvalid, err)
	}
	if item.ContextDigest != "" && item.ContextDigest != digest {
		return fmt.Errorf("%w: context_digest mismatch: got %q want %q", ErrProjectPatchQueueInvalid, item.ContextDigest, digest)
	}
	item.ContextDigest = digest
	return nil
}

func normalizeProjectPatchQueueOperationBindingRecord(item *ProjectPatchQueueItemRecord) error {
	if item == nil {
		return nil
	}
	item.OperationID = strings.TrimSpace(item.OperationID)
	item.OperationKind = strings.TrimSpace(item.OperationKind)
	item.OperationBindingSchema = strings.TrimSpace(item.OperationBindingSchema)
	item.OperationContextDigest = strings.TrimSpace(item.OperationContextDigest)
	item.OperationLeaseContextDigest = strings.TrimSpace(item.OperationLeaseContextDigest)
	item.OperationMutationPathsJSON = strings.TrimSpace(item.OperationMutationPathsJSON)
	item.OperationBoundBy = strings.TrimSpace(item.OperationBoundBy)
	item.OperationBoundAt = strings.TrimSpace(item.OperationBoundAt)

	if !projectPatchQueueOperationBindingEvidencePresent(*item) {
		return nil
	}
	if !projectPatchQueueBindingRefsPresent(*item) {
		return fmt.Errorf("%w: operation binding requires complete durable binding refs", ErrProjectPatchQueueInvalid)
	}
	if item.OperationBindingSchema == "" {
		item.OperationBindingSchema = ProjectPatchQueueOperationBindingSchema
	}
	mutationPathsJSON, mutationPaths, err := normalizeProjectPatchQueuePathsetJSON(item.OperationMutationPathsJSON)
	if err != nil {
		return fmt.Errorf("%w: operation_mutation_paths_json is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	_, itemPathset, err := normalizeProjectPatchQueuePathsetJSON(item.PathsetJSON)
	if err != nil {
		return fmt.Errorf("%w: patch queue pathset_json is invalid during operation binding: %v", ErrProjectPatchQueueInvalid, err)
	}
	if !stringSliceEqual(mutationPaths, itemPathset) {
		return fmt.Errorf("%w: operation mutation paths must exactly match patch queue pathset", ErrProjectPatchQueueInvalid)
	}
	item.OperationMutationPathsJSON = mutationPathsJSON
	item.OperationMutationPaths = mutationPaths

	proposalDigest, err := projectPatchQueueBindingContext(*item, false).Digest()
	if err != nil {
		return fmt.Errorf("%w: patch queue proposal binding context is invalid during operation binding: %v", ErrProjectPatchQueueInvalid, err)
	}
	if storedDigest := strings.TrimSpace(item.ContextDigest); storedDigest != "" && storedDigest != proposalDigest {
		return fmt.Errorf("%w: patch queue binding context_digest drifted before operation binding", ErrProjectPatchQueueInvalid)
	}
	item.ContextDigest = proposalDigest

	operationContext := projectPatchQueueBindingContext(*item, true)
	if err := repoauthority.ValidateConcreteMutationOperationRefs(operationContext); err != nil {
		return fmt.Errorf("%w: concrete mutation operation refs are invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	operationDigest, err := operationContext.Digest()
	if err != nil {
		return fmt.Errorf("%w: operation binding context is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	if item.OperationContextDigest != "" && item.OperationContextDigest != operationDigest {
		return fmt.Errorf("%w: operation_context_digest mismatch: got %q want %q", ErrProjectPatchQueueInvalid, item.OperationContextDigest, operationDigest)
	}
	item.OperationContextDigest = operationDigest

	leaseContextDigest, err := projectPatchQueueOperationLeaseContextDigest(operationContext)
	if err != nil {
		return fmt.Errorf("%w: operation lease context is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	if item.OperationLeaseContextDigest != "" && item.OperationLeaseContextDigest != leaseContextDigest {
		return fmt.Errorf("%w: operation_lease_context_digest mismatch: got %q want %q", ErrProjectPatchQueueInvalid, item.OperationLeaseContextDigest, leaseContextDigest)
	}
	item.OperationLeaseContextDigest = leaseContextDigest
	return nil
}

func ProjectPatchQueueOperationBindingReady(item ProjectPatchQueueItemRecord) bool {
	return validateProjectPatchQueueOperationBindingEvidence(item) == nil
}

func projectPatchQueueOperationBindingEvidencePresent(item ProjectPatchQueueItemRecord) bool {
	if item.OperationBindingAccepted || len(item.OperationMutationPaths) > 0 {
		return true
	}
	for _, value := range []string{
		item.OperationID,
		item.OperationKind,
		item.OperationBindingSchema,
		item.OperationContextDigest,
		item.OperationLeaseContextDigest,
		item.OperationMutationPathsJSON,
		item.OperationBoundBy,
		item.OperationBoundAt,
	} {
		if strings.TrimSpace(value) != "" && strings.TrimSpace(value) != "[]" {
			return true
		}
	}
	return false
}

func validateProjectPatchQueueOperationBindingEvidence(item ProjectPatchQueueItemRecord) error {
	if !projectPatchQueueOperationBindingEvidencePresent(item) {
		return fmt.Errorf("%w: operation binding evidence is missing", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.OperationBindingSchema) != ProjectPatchQueueOperationBindingSchema {
		return fmt.Errorf("%w: operation binding schema is %q, not %q", ErrProjectPatchQueueInvalid, item.OperationBindingSchema, ProjectPatchQueueOperationBindingSchema)
	}
	if !item.OperationBindingAccepted {
		return fmt.Errorf("%w: operation binding must be accepted", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.OperationBoundBy) == "" {
		return fmt.Errorf("%w: operation_bound_by is required", ErrProjectPatchQueueInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.OperationBoundAt)); err != nil {
		return fmt.Errorf("%w: operation_bound_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	normalized := item
	if err := normalizeProjectPatchQueueOperationBindingRecord(&normalized); err != nil {
		return err
	}
	if normalized.OperationContextDigest != strings.TrimSpace(item.OperationContextDigest) ||
		normalized.OperationLeaseContextDigest != strings.TrimSpace(item.OperationLeaseContextDigest) ||
		normalized.ContextDigest != strings.TrimSpace(item.ContextDigest) ||
		normalized.OperationMutationPathsJSON != strings.TrimSpace(item.OperationMutationPathsJSON) {
		return fmt.Errorf("%w: operation binding evidence is not canonical", ErrProjectPatchQueueInvalid)
	}
	return nil
}

func normalizeProjectPatchQueueCASEvidenceRecord(item *ProjectPatchQueueItemRecord) error {
	if item == nil {
		return nil
	}
	item.CASEvidenceSchema = strings.TrimSpace(item.CASEvidenceSchema)
	item.CASStatus = strings.TrimSpace(item.CASStatus)
	item.CASPatchDigest = strings.TrimSpace(item.CASPatchDigest)
	item.CASEvaluationDigest = strings.TrimSpace(item.CASEvaluationDigest)
	item.CASResultJSON = strings.TrimSpace(item.CASResultJSON)
	item.CASTestEvidenceJSON = strings.TrimSpace(item.CASTestEvidenceJSON)
	item.CASTestEvidenceDigest = strings.TrimSpace(item.CASTestEvidenceDigest)
	item.CASRecordedBy = strings.TrimSpace(item.CASRecordedBy)
	item.CASRecordedAt = strings.TrimSpace(item.CASRecordedAt)

	if !projectPatchQueueCASEvidencePresent(*item) {
		return nil
	}
	if !ProjectPatchQueueOperationBindingReady(*item) {
		return fmt.Errorf("%w: CAS evidence requires verified operation binding evidence", ErrProjectPatchQueueInvalid)
	}
	if item.CASEvidenceSchema == "" {
		item.CASEvidenceSchema = ProjectPatchQueueCASEvidenceSchema
	}
	casResult, casResultJSON, err := normalizeProjectPatchQueueCASResult(item.CASResultJSON, item.CASResult)
	if err != nil {
		return err
	}
	item.CASResult = casResult
	item.CASResultJSON = casResultJSON
	item.CASStatus = strings.TrimSpace(casResult.Status)
	item.CASPatchDigest = strings.TrimSpace(casResult.PatchDigest)
	item.CASEvaluationDigest = repoauthority.PatchQueueCASEvaluationDigest(casResult)

	testEvidence, testEvidenceJSON, err := normalizeProjectPatchQueueCASTestEvidence(item.CASTestEvidenceJSON, item.CASTestEvidence)
	if err != nil {
		return err
	}
	item.CASTestEvidence = testEvidence
	item.CASTestEvidenceJSON = testEvidenceJSON
	item.CASTestEvidenceDigest = repoauthority.PatchQueueTestEvidenceDigest(testEvidence)

	authority := projectPatchQueueBindingContext(*item, true)
	projected := projectPatchQueueRepoAuthorityAppliedItem(*item)
	if err := repoauthority.ValidatePatchQueueMergeAdmission(authority, projected); err != nil {
		return fmt.Errorf("%w: CAS evidence does not satisfy merge admission: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func ProjectPatchQueueCASEvidenceReady(item ProjectPatchQueueItemRecord) bool {
	return validateProjectPatchQueueCASEvidence(item) == nil
}

func normalizeProjectPatchQueueMaterializationRecord(item *ProjectPatchQueueItemRecord) error {
	if item == nil {
		return nil
	}
	item.MaterializationSchema = strings.TrimSpace(item.MaterializationSchema)
	item.MaterializationJSON = strings.TrimSpace(item.MaterializationJSON)
	item.MaterializationDigest = strings.TrimSpace(item.MaterializationDigest)
	item.MaterializationRecordedBy = strings.TrimSpace(item.MaterializationRecordedBy)
	item.MaterializationRecordedAt = strings.TrimSpace(item.MaterializationRecordedAt)
	item.MaterializationAuthorityProofJSON = strings.TrimSpace(item.MaterializationAuthorityProofJSON)
	item.MaterializationAuthorityProofDigest = strings.TrimSpace(item.MaterializationAuthorityProofDigest)

	if !projectPatchQueueMaterializationPresent(*item) {
		return nil
	}
	if !ProjectPatchQueueCASEvidenceReady(*item) {
		return fmt.Errorf("%w: materialization requires verified CAS evidence", ErrProjectPatchQueueInvalid)
	}
	if item.MaterializationSchema == "" {
		item.MaterializationSchema = ProjectPatchQueueMaterializationSchema
	}
	if item.MaterializationRecordedAt == "" {
		item.MaterializationRecordedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	recordedAt, err := time.Parse(time.RFC3339Nano, item.MaterializationRecordedAt)
	if err != nil {
		return fmt.Errorf("%w: materialization_recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	materialization, materializationJSON, materializationDigest, err := normalizeProjectPatchQueueMaterialization(item.MaterializationJSON, item.Materialization, projectPatchQueueRepoAuthorityAppliedItem(*item), item.MaterializationRecordedBy, recordedAt)
	if err != nil {
		return err
	}
	item.Materialization = materialization
	item.MaterializationJSON = materializationJSON
	item.MaterializationDigest = materializationDigest
	authorityProof, authorityProofJSON, authorityProofDigest, err := normalizeProjectPatchQueueMaterializationAuthorityProof(materialization, projectPatchQueueRepoAuthorityAppliedItem(*item))
	if err != nil {
		return err
	}
	item.MaterializationAuthorityProof = authorityProof
	item.MaterializationAuthorityProofJSON = authorityProofJSON
	item.MaterializationAuthorityProofDigest = authorityProofDigest
	return nil
}

func ProjectPatchQueueMaterializationReady(item ProjectPatchQueueItemRecord) bool {
	return validateProjectPatchQueueMaterialization(item) == nil
}

func (s *Store) backfillProjectPatchQueueMaterializationAuthorityProofsTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE materialization_accepted = 1
   AND materialization_json <> '{}'
   AND materialization_digest <> ''
   AND (
     materialization_authority_proof_json = ''
     OR materialization_authority_proof_json = '{}'
     OR materialization_authority_proof_digest = ''
   )
 ORDER BY updated_at ASC, queue_id ASC, item_id ASC`)
	if err != nil {
		return fmt.Errorf("query project patch queue materialization authority proof backfill: %w", err)
	}
	var items []ProjectPatchQueueItemRecord
	for rows.Next() {
		item, err := scanProjectPatchQueueItem(rows)
		if err != nil {
			_ = rows.Close()
			return err
		}
		if err := normalizeProjectPatchQueueMaterializationRecord(&item); err != nil {
			continue
		}
		items = append(items, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range items {
		if err := updateProjectPatchQueueMaterializationTx(ctx, tx, item); err != nil {
			return err
		}
	}
	return nil
}

func normalizeProjectPatchQueueRollbackEvidenceRecord(item *ProjectPatchQueueItemRecord) error {
	if item == nil {
		return nil
	}
	item.RollbackEvidenceSchema = strings.TrimSpace(item.RollbackEvidenceSchema)
	item.RollbackEvidenceJSON = strings.TrimSpace(item.RollbackEvidenceJSON)
	item.RollbackEvidenceDigest = strings.TrimSpace(item.RollbackEvidenceDigest)
	item.RollbackRecordedBy = strings.TrimSpace(item.RollbackRecordedBy)
	item.RollbackRecordedAt = strings.TrimSpace(item.RollbackRecordedAt)

	if !projectPatchQueueRollbackEvidencePresent(*item) {
		return nil
	}
	if !ProjectPatchQueueCASEvidenceReady(*item) {
		return fmt.Errorf("%w: rollback evidence requires verified CAS evidence", ErrProjectPatchQueueInvalid)
	}
	if item.RollbackEvidenceSchema == "" {
		item.RollbackEvidenceSchema = ProjectPatchQueueRollbackEvidenceSchema
	}
	if item.RollbackRecordedAt == "" {
		item.RollbackRecordedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	recordedAt, err := time.Parse(time.RFC3339Nano, item.RollbackRecordedAt)
	if err != nil {
		return fmt.Errorf("%w: rollback_recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	rollback, rollbackJSON, rollbackDigest, err := normalizeProjectPatchQueueRollbackEvidence(item.RollbackEvidenceJSON, item.RollbackEvidence, projectPatchQueueRepoAuthorityAppliedItem(*item), recordedAt)
	if err != nil {
		return err
	}
	item.RollbackEvidence = rollback
	item.RollbackEvidenceJSON = rollbackJSON
	item.RollbackEvidenceDigest = rollbackDigest
	return nil
}

func ProjectPatchQueueRollbackEvidenceReady(item ProjectPatchQueueItemRecord) bool {
	return validateProjectPatchQueueRollbackEvidence(item) == nil
}

func normalizeProjectPatchQueueReviewerAdvisoryRecord(item *ProjectPatchQueueItemRecord) error {
	if item == nil {
		return nil
	}
	item.ReviewerAdvisorySchema = strings.TrimSpace(item.ReviewerAdvisorySchema)
	item.ReviewerAdvisoryJSON = strings.TrimSpace(item.ReviewerAdvisoryJSON)
	item.ReviewerAdvisoryDigest = strings.TrimSpace(item.ReviewerAdvisoryDigest)
	item.ReviewerRecordedBy = strings.TrimSpace(item.ReviewerRecordedBy)
	item.ReviewerRecordedAt = strings.TrimSpace(item.ReviewerRecordedAt)

	if !projectPatchQueueReviewerAdvisoryPresent(*item) {
		return nil
	}
	if projectPatchQueueReviewerAdvisoryDefeatsAcceptance(*item) {
		return normalizeProjectPatchQueueDefeatingReviewerAdvisoryRecord(item)
	}
	if !ProjectPatchQueueRollbackEvidenceReady(*item) {
		return fmt.Errorf("%w: reviewer advisory requires verified rollback evidence", ErrProjectPatchQueueInvalid)
	}
	if item.ReviewerAdvisorySchema == "" {
		item.ReviewerAdvisorySchema = ProjectPatchQueueReviewerAdvisorySchema
	}
	if item.ReviewerRecordedAt == "" {
		item.ReviewerRecordedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	recordedAt, err := time.Parse(time.RFC3339Nano, item.ReviewerRecordedAt)
	if err != nil {
		return fmt.Errorf("%w: reviewer_recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	advisory, advisoryJSON, advisoryDigest, err := normalizeProjectPatchQueueReviewerAdvisory(item.ReviewerAdvisoryJSON, item.ReviewerAdvisory, projectPatchQueueRepoAuthorityAppliedItem(*item), item.ReviewerRecordedBy, recordedAt)
	if err != nil {
		return err
	}
	item.ReviewerAdvisory = advisory
	item.ReviewerAdvisoryJSON = advisoryJSON
	item.ReviewerAdvisoryDigest = advisoryDigest
	return nil
}

func ProjectPatchQueueReviewerAdvisoryReady(item ProjectPatchQueueItemRecord) bool {
	return validateProjectPatchQueueReviewerAdvisory(item) == nil
}

func normalizeProjectPatchQueueDefeatingReviewerAdvisoryRecord(item *ProjectPatchQueueItemRecord) error {
	if item == nil {
		return nil
	}
	evidence := item.ReviewerAdvisory
	evidence.Schema = firstNonEmpty(strings.TrimSpace(evidence.Schema), repoauthority.PatchQueueReviewerAdvisorySchema)
	evidence.Mode = firstNonEmpty(strings.TrimSpace(evidence.Mode), repoauthority.MutationActivationReviewerMeshAdvisoryOnly)
	evidence.Verdict = firstNonEmpty(strings.TrimSpace(evidence.Verdict), repoauthority.PatchQueueReviewerAdvisoryVerdictRepairRequired)
	evidence.Scope = firstNonEmpty(projectPatchQueueReviewerAdvisoryNormalizeScope(evidence.Scope), projectPatchQueueReviewerAdvisorySummaryScope(evidence.Summary))
	evidence.HeadSHA = firstNonEmpty(strings.TrimSpace(evidence.HeadSHA), strings.TrimSpace(item.HeadSHA))
	evidence.ReviewerID = firstNonEmpty(strings.TrimSpace(evidence.ReviewerID), strings.TrimSpace(item.ReviewerRecordedBy))
	evidence.RecordedAt = firstNonEmpty(strings.TrimSpace(evidence.RecordedAt), strings.TrimSpace(item.ReviewerRecordedAt))
	evidence.DefeatsAcceptance = true
	if evidence.Scope != repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness {
		if evidence.Scope == repoauthority.PatchQueueReviewerAdvisoryScopeIntegrationCompleteness {
			return fmt.Errorf("%w: integration-completeness reviewer advisory cannot defeat a lane patch queue item; bind it to integration validation instead", ErrProjectPatchQueueInvalid)
		}
		return fmt.Errorf("%w: defeating reviewer advisory requires scope %q", ErrProjectPatchQueueInvalid, repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness)
	}
	if strings.TrimSpace(evidence.ReviewDocKey) == "" {
		return fmt.Errorf("%w: defeating reviewer advisory requires review_doc_key", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(evidence.Summary) == "" {
		return fmt.Errorf("%w: defeating reviewer advisory requires summary", ErrProjectPatchQueueInvalid)
	}
	if !projectPatchQueueReviewerAdvisoryNegativeSummary(evidence.Summary) &&
		!strings.EqualFold(strings.TrimSpace(evidence.Verdict), repoauthority.PatchQueueReviewerAdvisoryVerdictRepairRequired) {
		return fmt.Errorf("%w: defeating reviewer advisory summary must describe a lane correctness defect or repair requirement", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(evidence.HeadSHA) == "" || !strings.EqualFold(strings.TrimSpace(evidence.HeadSHA), strings.TrimSpace(item.HeadSHA)) {
		return fmt.Errorf("%w: defeating reviewer advisory head_sha %s does not match accepted item head_sha %s", ErrProjectPatchQueueInvalid, strings.TrimSpace(evidence.HeadSHA), strings.TrimSpace(item.HeadSHA))
	}
	if evidence.Schema != repoauthority.PatchQueueReviewerAdvisorySchema {
		return fmt.Errorf("%w: reviewer advisory schema is %q, not %q", ErrProjectPatchQueueInvalid, evidence.Schema, repoauthority.PatchQueueReviewerAdvisorySchema)
	}
	if evidence.Mode != repoauthority.MutationActivationReviewerMeshAdvisoryOnly {
		return fmt.Errorf("%w: reviewer advisory mode is %q, not %q", ErrProjectPatchQueueInvalid, evidence.Mode, repoauthority.MutationActivationReviewerMeshAdvisoryOnly)
	}
	if strings.TrimSpace(evidence.ReviewerID) == "" {
		return fmt.Errorf("%w: reviewer advisory reviewer_id is required", ErrProjectPatchQueueInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, evidence.RecordedAt); err != nil {
		return fmt.Errorf("%w: reviewer advisory recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return fmt.Errorf("marshal project patch queue defeating reviewer advisory: %w", err)
	}
	item.ReviewerAdvisory = evidence
	item.ReviewerAdvisoryJSON = string(encoded)
	item.ReviewerAdvisoryDigest = repoauthority.PatchQueueReviewerAdvisoryDigest(evidence)
	return nil
}

func projectPatchQueueReviewerAdvisoryDefeatsAcceptedItem(item ProjectPatchQueueItemRecord, evidence repoauthority.PatchQueueReviewerAdvisory) (bool, error) {
	if strings.ToUpper(strings.TrimSpace(item.State)) != ProjectPatchQueueStateAccepted {
		return false, nil
	}
	if head := strings.TrimSpace(evidence.HeadSHA); head != "" && !strings.EqualFold(head, strings.TrimSpace(item.HeadSHA)) {
		return false, fmt.Errorf("%w: reviewer advisory head_sha %s does not match accepted item head_sha %s", ErrProjectPatchQueueInvalid, head, strings.TrimSpace(item.HeadSHA))
	}
	scope := firstNonEmpty(projectPatchQueueReviewerAdvisoryNormalizeScope(evidence.Scope), projectPatchQueueReviewerAdvisorySummaryScope(evidence.Summary))
	if scope == repoauthority.PatchQueueReviewerAdvisoryScopeIntegrationCompleteness {
		return false, fmt.Errorf("%w: integration-completeness reviewer advisory cannot defeat a lane patch queue item; bind it to integration validation instead", ErrProjectPatchQueueInvalid)
	}
	if evidence.DefeatsAcceptance {
		if scope != "" && scope != repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness {
			return false, fmt.Errorf("%w: defeating reviewer advisory requires scope %q", ErrProjectPatchQueueInvalid, repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness)
		}
		return true, nil
	}
	if scope == repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness {
		return true, nil
	}
	if strings.EqualFold(strings.TrimSpace(evidence.Verdict), repoauthority.PatchQueueReviewerAdvisoryVerdictRepairRequired) {
		return true, nil
	}
	if projectPatchQueueReviewerAdvisoryNegativeSummary(evidence.Summary) && scope == "" {
		return true, nil
	}
	return false, nil
}

func projectPatchQueueReviewerAdvisoryDefeatsAcceptance(item ProjectPatchQueueItemRecord) bool {
	if item.ReviewerAdvisory.DefeatsAcceptance {
		return true
	}
	if strings.EqualFold(projectPatchQueueReviewerAdvisoryNormalizeScope(item.ReviewerAdvisory.Scope), repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness) &&
		strings.EqualFold(strings.TrimSpace(item.ReviewerAdvisory.HeadSHA), strings.TrimSpace(item.HeadSHA)) &&
		(strings.EqualFold(strings.TrimSpace(item.ReviewerAdvisory.Verdict), repoauthority.PatchQueueReviewerAdvisoryVerdictRepairRequired) || projectPatchQueueReviewerAdvisoryNegativeSummary(item.ReviewerAdvisory.Summary)) {
		return true
	}
	return false
}

func projectPatchQueueReviewerAdvisoryNormalizeScope(scope string) string {
	scope = strings.ToLower(strings.TrimSpace(scope))
	scope = strings.ReplaceAll(scope, "-", "_")
	scope = strings.ReplaceAll(scope, " ", "_")
	switch scope {
	case "lane", "lane_defect", "lane_correctness", "candidate_defect", "candidate_correctness":
		return repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness
	case "integration", "integration_completeness", "full_product", "full_product_completeness", "product_completeness":
		return repoauthority.PatchQueueReviewerAdvisoryScopeIntegrationCompleteness
	default:
		return scope
	}
}

func projectPatchQueueReviewerAdvisorySummaryScope(summary string) string {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	if text == "" {
		return ""
	}
	integrationSignals := []string{
		"full product",
		"whole product",
		"final product",
		"product completion",
		"canonical integration",
		"integration receipt",
		"assembled full-product",
		"assembled full product",
		"not merged",
	}
	laneSignals := []string{
		"lane-scoped",
		"lane scoped",
		"candidate defect",
		"lane defect",
		"lexer defect",
		"parser defect",
		"evaluator defect",
		"runtime defect",
		"tokenization",
		"source-position",
	}
	for _, marker := range laneSignals {
		if strings.Contains(text, marker) {
			return repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness
		}
	}
	for _, marker := range integrationSignals {
		if strings.Contains(text, marker) {
			return repoauthority.PatchQueueReviewerAdvisoryScopeIntegrationCompleteness
		}
	}
	return ""
}

func projectPatchQueueReviewerAdvisoryNegativeSummary(summary string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	if text == "" {
		return false
	}
	for _, marker := range []string{
		"defect",
		"bug",
		"broken",
		"incorrect",
		"invalid",
		"reject",
		"not accepted-ready",
		"not accepted ready",
		"repair needed",
		"repair required",
		"follow-up repair",
		"follow up repair",
		"blocking",
		"fails",
		"failed",
		"wrong",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func normalizeProjectPatchQueueOperatorEnablementRecord(item *ProjectPatchQueueItemRecord) error {
	if item == nil {
		return nil
	}
	item.OperatorEnablementSchema = strings.TrimSpace(item.OperatorEnablementSchema)
	item.OperatorEnablementJSON = strings.TrimSpace(item.OperatorEnablementJSON)
	item.OperatorEnablementDigest = strings.TrimSpace(item.OperatorEnablementDigest)
	item.OperatorEnabledBy = strings.TrimSpace(item.OperatorEnabledBy)
	item.OperatorEnabledAt = strings.TrimSpace(item.OperatorEnabledAt)

	if !projectPatchQueueOperatorEnablementPresent(*item) {
		return nil
	}
	if !ProjectPatchQueueReviewerAdvisoryReady(*item) {
		return fmt.Errorf("%w: operator enablement requires verified reviewer advisory", ErrProjectPatchQueueInvalid)
	}
	if item.OperatorEnablementSchema == "" {
		item.OperatorEnablementSchema = ProjectPatchQueueOperatorEnablementSchema
	}
	if item.OperatorEnabledAt == "" {
		item.OperatorEnabledAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	enabledAt, err := time.Parse(time.RFC3339Nano, item.OperatorEnabledAt)
	if err != nil {
		return fmt.Errorf("%w: operator_enabled_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	enablement, enablementJSON, enablementDigest, err := normalizeProjectPatchQueueOperatorEnablement(item.OperatorEnablementJSON, item.OperatorEnablement, projectPatchQueueRepoAuthorityAppliedItem(*item), item.ReviewerAdvisory, item.OperatorEnabledBy, enabledAt)
	if err != nil {
		return err
	}
	item.OperatorEnablement = enablement
	item.OperatorEnablementJSON = enablementJSON
	item.OperatorEnablementDigest = enablementDigest
	return nil
}

func ProjectPatchQueueOperatorEnablementReady(item ProjectPatchQueueItemRecord) bool {
	return validateProjectPatchQueueOperatorEnablement(item) == nil
}

func projectPatchQueueCASEvidencePresent(item ProjectPatchQueueItemRecord) bool {
	if item.CASEvidenceAccepted {
		return true
	}
	if hasProjectPatchQueueCASResult(item.CASResult) || hasProjectPatchQueueCASTestEvidence(item.CASTestEvidence) {
		return true
	}
	for _, value := range []string{
		item.CASEvidenceSchema,
		item.CASStatus,
		item.CASPatchDigest,
		item.CASEvaluationDigest,
		item.CASResultJSON,
		item.CASTestEvidenceJSON,
		item.CASTestEvidenceDigest,
		item.CASRecordedBy,
		item.CASRecordedAt,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func projectPatchQueueMaterializationPresent(item ProjectPatchQueueItemRecord) bool {
	if item.MaterializationAccepted {
		return true
	}
	if hasProjectPatchQueueMaterialization(item.Materialization) {
		return true
	}
	for _, value := range []string{
		item.MaterializationSchema,
		item.MaterializationJSON,
		item.MaterializationDigest,
		item.MaterializationRecordedBy,
		item.MaterializationRecordedAt,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func projectPatchQueueRollbackEvidencePresent(item ProjectPatchQueueItemRecord) bool {
	if item.RollbackEvidenceAccepted {
		return true
	}
	if hasProjectPatchQueueRollbackEvidence(item.RollbackEvidence) {
		return true
	}
	for _, value := range []string{
		item.RollbackEvidenceSchema,
		item.RollbackEvidenceJSON,
		item.RollbackEvidenceDigest,
		item.RollbackRecordedBy,
		item.RollbackRecordedAt,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func projectPatchQueueReviewerAdvisoryPresent(item ProjectPatchQueueItemRecord) bool {
	if item.ReviewerAdvisoryAccepted {
		return true
	}
	if hasProjectPatchQueueReviewerAdvisory(item.ReviewerAdvisory) {
		return true
	}
	for _, value := range []string{
		item.ReviewerAdvisorySchema,
		item.ReviewerAdvisoryJSON,
		item.ReviewerAdvisoryDigest,
		item.ReviewerRecordedBy,
		item.ReviewerRecordedAt,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func projectPatchQueueOperatorEnablementPresent(item ProjectPatchQueueItemRecord) bool {
	if item.OperatorEnablementAccepted {
		return true
	}
	if hasProjectPatchQueueOperatorEnablement(item.OperatorEnablement) {
		return true
	}
	for _, value := range []string{
		item.OperatorEnablementSchema,
		item.OperatorEnablementJSON,
		item.OperatorEnablementDigest,
		item.OperatorEnabledBy,
		item.OperatorEnabledAt,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func validateProjectPatchQueueCASEvidence(item ProjectPatchQueueItemRecord) error {
	if !projectPatchQueueCASEvidencePresent(item) {
		return fmt.Errorf("%w: CAS evidence is missing", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.CASEvidenceSchema) != ProjectPatchQueueCASEvidenceSchema {
		return fmt.Errorf("%w: CAS evidence schema is %q, not %q", ErrProjectPatchQueueInvalid, item.CASEvidenceSchema, ProjectPatchQueueCASEvidenceSchema)
	}
	if !item.CASEvidenceAccepted {
		return fmt.Errorf("%w: CAS evidence must be accepted", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.CASRecordedBy) == "" {
		return fmt.Errorf("%w: cas_recorded_by is required", ErrProjectPatchQueueInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.CASRecordedAt)); err != nil {
		return fmt.Errorf("%w: cas_recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	normalized := item
	if err := normalizeProjectPatchQueueCASEvidenceRecord(&normalized); err != nil {
		return err
	}
	if normalized.CASStatus != strings.TrimSpace(item.CASStatus) ||
		normalized.CASPatchDigest != strings.TrimSpace(item.CASPatchDigest) ||
		normalized.CASEvaluationDigest != strings.TrimSpace(item.CASEvaluationDigest) ||
		normalized.CASResultJSON != strings.TrimSpace(item.CASResultJSON) ||
		normalized.CASTestEvidenceJSON != strings.TrimSpace(item.CASTestEvidenceJSON) ||
		normalized.CASTestEvidenceDigest != strings.TrimSpace(item.CASTestEvidenceDigest) {
		return fmt.Errorf("%w: CAS evidence is not canonical", ErrProjectPatchQueueInvalid)
	}
	return nil
}

func validateProjectPatchQueueMaterialization(item ProjectPatchQueueItemRecord) error {
	if !projectPatchQueueMaterializationPresent(item) {
		return fmt.Errorf("%w: materialization is missing", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.MaterializationSchema) != ProjectPatchQueueMaterializationSchema {
		return fmt.Errorf("%w: materialization schema is %q, not %q", ErrProjectPatchQueueInvalid, item.MaterializationSchema, ProjectPatchQueueMaterializationSchema)
	}
	if !item.MaterializationAccepted {
		return fmt.Errorf("%w: materialization must be accepted", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.MaterializationRecordedBy) == "" {
		return fmt.Errorf("%w: materialization_recorded_by is required", ErrProjectPatchQueueInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.MaterializationRecordedAt)); err != nil {
		return fmt.Errorf("%w: materialization_recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	normalized := item
	if err := normalizeProjectPatchQueueMaterializationRecord(&normalized); err != nil {
		return err
	}
	if normalized.MaterializationJSON != strings.TrimSpace(item.MaterializationJSON) ||
		normalized.MaterializationDigest != strings.TrimSpace(item.MaterializationDigest) ||
		normalized.MaterializationAuthorityProofJSON != strings.TrimSpace(item.MaterializationAuthorityProofJSON) ||
		normalized.MaterializationAuthorityProofDigest != strings.TrimSpace(item.MaterializationAuthorityProofDigest) {
		return fmt.Errorf("%w: materialization is not canonical", ErrProjectPatchQueueInvalid)
	}
	if err := repoauthority.ValidatePatchMaterialization(item.Materialization, projectPatchQueueRepoAuthorityAppliedItem(item)); err != nil {
		return fmt.Errorf("%w: materialization does not satisfy content-bound CAS proof: %v", ErrProjectPatchQueueInvalid, err)
	}
	if err := repoauthority.ValidatePatchMaterializationAuthorityProof(item.MaterializationAuthorityProof, item.Materialization, projectPatchQueueRepoAuthorityAppliedItem(item)); err != nil {
		return fmt.Errorf("%w: materialization authority proof is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func validateProjectPatchQueueRollbackEvidence(item ProjectPatchQueueItemRecord) error {
	if !projectPatchQueueRollbackEvidencePresent(item) {
		return fmt.Errorf("%w: rollback evidence is missing", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.RollbackEvidenceSchema) != ProjectPatchQueueRollbackEvidenceSchema {
		return fmt.Errorf("%w: rollback evidence schema is %q, not %q", ErrProjectPatchQueueInvalid, item.RollbackEvidenceSchema, ProjectPatchQueueRollbackEvidenceSchema)
	}
	if !item.RollbackEvidenceAccepted {
		return fmt.Errorf("%w: rollback evidence must be accepted", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.RollbackRecordedBy) == "" {
		return fmt.Errorf("%w: rollback_recorded_by is required", ErrProjectPatchQueueInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.RollbackRecordedAt)); err != nil {
		return fmt.Errorf("%w: rollback_recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	normalized := item
	if err := normalizeProjectPatchQueueRollbackEvidenceRecord(&normalized); err != nil {
		return err
	}
	if normalized.RollbackEvidenceJSON != strings.TrimSpace(item.RollbackEvidenceJSON) ||
		normalized.RollbackEvidenceDigest != strings.TrimSpace(item.RollbackEvidenceDigest) {
		return fmt.Errorf("%w: rollback evidence is not canonical", ErrProjectPatchQueueInvalid)
	}
	if err := repoauthority.ValidatePatchQueueRollbackEvidence(item.RollbackEvidence, projectPatchQueueRepoAuthorityAppliedItem(item)); err != nil {
		return fmt.Errorf("%w: rollback evidence does not satisfy rollback proof: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func validateProjectPatchQueueReviewerAdvisory(item ProjectPatchQueueItemRecord) error {
	if !projectPatchQueueReviewerAdvisoryPresent(item) {
		return fmt.Errorf("%w: reviewer advisory is missing", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.ReviewerAdvisorySchema) != ProjectPatchQueueReviewerAdvisorySchema {
		return fmt.Errorf("%w: reviewer advisory schema is %q, not %q", ErrProjectPatchQueueInvalid, item.ReviewerAdvisorySchema, ProjectPatchQueueReviewerAdvisorySchema)
	}
	if !item.ReviewerAdvisoryAccepted {
		return fmt.Errorf("%w: reviewer advisory must be accepted", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.ReviewerRecordedBy) == "" {
		return fmt.Errorf("%w: reviewer_recorded_by is required", ErrProjectPatchQueueInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.ReviewerRecordedAt)); err != nil {
		return fmt.Errorf("%w: reviewer_recorded_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	normalized := item
	if err := normalizeProjectPatchQueueReviewerAdvisoryRecord(&normalized); err != nil {
		return err
	}
	if normalized.ReviewerAdvisoryJSON != strings.TrimSpace(item.ReviewerAdvisoryJSON) ||
		normalized.ReviewerAdvisoryDigest != strings.TrimSpace(item.ReviewerAdvisoryDigest) {
		return fmt.Errorf("%w: reviewer advisory is not canonical", ErrProjectPatchQueueInvalid)
	}
	if projectPatchQueueReviewerAdvisoryDefeatsAcceptance(item) {
		return nil
	}
	if err := repoauthority.ValidatePatchQueueReviewerAdvisory(item.ReviewerAdvisory, projectPatchQueueRepoAuthorityAppliedItem(item)); err != nil {
		return fmt.Errorf("%w: reviewer advisory does not satisfy advisory proof: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func validateProjectPatchQueueOperatorEnablement(item ProjectPatchQueueItemRecord) error {
	if !projectPatchQueueOperatorEnablementPresent(item) {
		return fmt.Errorf("%w: operator enablement is missing", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.OperatorEnablementSchema) != ProjectPatchQueueOperatorEnablementSchema {
		return fmt.Errorf("%w: operator enablement schema is %q, not %q", ErrProjectPatchQueueInvalid, item.OperatorEnablementSchema, ProjectPatchQueueOperatorEnablementSchema)
	}
	if !item.OperatorEnablementAccepted {
		return fmt.Errorf("%w: operator enablement must be accepted", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.OperatorEnabledBy) == "" {
		return fmt.Errorf("%w: operator_enabled_by is required", ErrProjectPatchQueueInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.OperatorEnabledAt)); err != nil {
		return fmt.Errorf("%w: operator_enabled_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
	}
	normalized := item
	if err := normalizeProjectPatchQueueOperatorEnablementRecord(&normalized); err != nil {
		return err
	}
	if normalized.OperatorEnablementJSON != strings.TrimSpace(item.OperatorEnablementJSON) ||
		normalized.OperatorEnablementDigest != strings.TrimSpace(item.OperatorEnablementDigest) {
		return fmt.Errorf("%w: operator enablement is not canonical", ErrProjectPatchQueueInvalid)
	}
	if err := repoauthority.ValidatePatchQueueOperatorEnablement(item.OperatorEnablement, projectPatchQueueRepoAuthorityAppliedItem(item), item.ReviewerAdvisory); err != nil {
		return fmt.Errorf("%w: operator enablement does not satisfy enablement proof: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func normalizeProjectPatchQueueMaterialization(raw string, supplied repoauthority.PatchMaterialization, applied repoauthority.PatchQueueItem, recordedBy string, recordedAt time.Time) (repoauthority.PatchMaterialization, string, string, error) {
	raw = strings.TrimSpace(raw)
	materialization := supplied
	if !hasProjectPatchQueueMaterialization(materialization) && raw != "" && raw != "{}" {
		if int64(len([]byte(raw))) > ProjectPatchQueueMaterializationMaxJSONBytes {
			return repoauthority.PatchMaterialization{}, "", "", fmt.Errorf("%w: materialization storage policy exceeded: materialization_json size %d exceeds limit %d bytes", ErrProjectPatchQueueInvalid, len([]byte(raw)), ProjectPatchQueueMaterializationMaxJSONBytes)
		}
		if err := json.Unmarshal([]byte(raw), &materialization); err != nil {
			return repoauthority.PatchMaterialization{}, "", "", fmt.Errorf("%w: materialization_json must be a patch materialization object: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	if err := validateProjectPatchQueueMaterializationContentBackpressure(materialization); err != nil {
		return repoauthority.PatchMaterialization{}, "", "", err
	}
	recordedBy = strings.TrimSpace(recordedBy)
	if suppliedRecordedBy := strings.TrimSpace(materialization.RecordedBy); suppliedRecordedBy != "" && suppliedRecordedBy != recordedBy {
		return repoauthority.PatchMaterialization{}, "", "", fmt.Errorf("%w: materialization recorded_by must match recording principal", ErrProjectPatchQueueInvalid)
	}
	materialization.RecordedBy = recordedBy
	recordedAtValue := recordedAt.Format(time.RFC3339Nano)
	if suppliedRecordedAt := strings.TrimSpace(materialization.RecordedAt); suppliedRecordedAt != "" && suppliedRecordedAt != recordedAtValue {
		return repoauthority.PatchMaterialization{}, "", "", fmt.Errorf("%w: materialization recorded_at must match storage timestamp", ErrProjectPatchQueueInvalid)
	}
	materialization.RecordedAt = recordedAtValue
	normalized, err := repoauthority.NormalizePatchMaterialization(materialization, applied, recordedAt)
	if err != nil {
		return repoauthority.PatchMaterialization{}, "", "", fmt.Errorf("%w: materialization is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return repoauthority.PatchMaterialization{}, "", "", fmt.Errorf("marshal project patch queue materialization: %w", err)
	}
	materializationJSON := string(encoded)
	if int64(len([]byte(materializationJSON))) > ProjectPatchQueueMaterializationMaxJSONBytes {
		return repoauthority.PatchMaterialization{}, "", "", fmt.Errorf("%w: materialization storage policy exceeded: materialization_json size %d exceeds limit %d bytes", ErrProjectPatchQueueInvalid, len([]byte(materializationJSON)), ProjectPatchQueueMaterializationMaxJSONBytes)
	}
	return normalized, materializationJSON, repoauthority.PatchMaterializationDigest(normalized), nil
}

func normalizeProjectPatchQueueMaterializationAuthorityProof(materialization repoauthority.PatchMaterialization, applied repoauthority.PatchQueueItem) (repoauthority.PatchMaterializationAuthorityProof, string, string, error) {
	proof, err := repoauthority.BuildPatchMaterializationAuthorityProof(materialization, applied)
	if err != nil {
		return repoauthority.PatchMaterializationAuthorityProof{}, "", "", fmt.Errorf("%w: materialization authority proof is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		return repoauthority.PatchMaterializationAuthorityProof{}, "", "", fmt.Errorf("marshal project patch queue materialization authority proof: %w", err)
	}
	authorityProofJSON := string(encoded)
	if int64(len([]byte(authorityProofJSON))) > ProjectPatchQueueMaterializationMaxAuthorityProofJSONBytes {
		return repoauthority.PatchMaterializationAuthorityProof{}, "", "", fmt.Errorf("%w: materialization storage policy exceeded: authority proof JSON size %d exceeds limit %d bytes", ErrProjectPatchQueueInvalid, len([]byte(authorityProofJSON)), ProjectPatchQueueMaterializationMaxAuthorityProofJSONBytes)
	}
	return proof, authorityProofJSON, proof.AuthorityDigest, nil
}

func validateProjectPatchQueueMaterializationContentBackpressure(materialization repoauthority.PatchMaterialization) error {
	if err := repoauthority.ValidatePatchMaterializationContentBounds(materialization); err != nil {
		return fmt.Errorf("%w: materialization storage policy exceeded: %v", ErrProjectPatchQueueInvalid, err)
	}
	return nil
}

func normalizeProjectPatchQueueRollbackEvidence(raw string, supplied repoauthority.PatchQueueRollback, applied repoauthority.PatchQueueItem, recordedAt time.Time) (repoauthority.PatchQueueRollback, string, string, error) {
	raw = strings.TrimSpace(raw)
	evidence := supplied
	if !hasProjectPatchQueueRollbackEvidence(evidence) && raw != "" && raw != "{}" {
		if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
			return repoauthority.PatchQueueRollback{}, "", "", fmt.Errorf("%w: rollback_evidence_json must be a patch queue rollback evidence object: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	operation := repoauthority.OperationRef{
		ID:   strings.TrimSpace(evidence.RollbackOperationID),
		Kind: strings.TrimSpace(evidence.RollbackOperationKind),
	}
	normalized, err := repoauthority.NormalizePatchQueueRollbackEvidence(evidence, applied, operation, recordedAt)
	if err != nil {
		return repoauthority.PatchQueueRollback{}, "", "", fmt.Errorf("%w: rollback evidence is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return repoauthority.PatchQueueRollback{}, "", "", fmt.Errorf("marshal project patch queue rollback evidence: %w", err)
	}
	return normalized, string(encoded), repoauthority.PatchQueueRollbackEvidenceDigest(normalized), nil
}

func normalizeProjectPatchQueueReviewerAdvisory(raw string, supplied repoauthority.PatchQueueReviewerAdvisory, applied repoauthority.PatchQueueItem, reviewerID string, recordedAt time.Time) (repoauthority.PatchQueueReviewerAdvisory, string, string, error) {
	raw = strings.TrimSpace(raw)
	evidence := supplied
	if !hasProjectPatchQueueReviewerAdvisory(evidence) && raw != "" && raw != "{}" {
		if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
			return repoauthority.PatchQueueReviewerAdvisory{}, "", "", fmt.Errorf("%w: reviewer_advisory_json must be a reviewer advisory object: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	evidence.Schema = firstNonEmpty(strings.TrimSpace(evidence.Schema), repoauthority.PatchQueueReviewerAdvisorySchema)
	evidence.Mode = firstNonEmpty(strings.TrimSpace(evidence.Mode), repoauthority.MutationActivationReviewerMeshAdvisoryOnly)
	evidence.Verdict = firstNonEmpty(strings.TrimSpace(evidence.Verdict), repoauthority.PatchQueueReviewerAdvisoryVerdictReviewed)
	reviewerID = strings.TrimSpace(reviewerID)
	if suppliedReviewerID := strings.TrimSpace(evidence.ReviewerID); suppliedReviewerID != "" && suppliedReviewerID != reviewerID {
		return repoauthority.PatchQueueReviewerAdvisory{}, "", "", fmt.Errorf("%w: reviewer advisory reviewer_id must match recording principal", ErrProjectPatchQueueInvalid)
	}
	evidence.ReviewerID = reviewerID
	evidence.ReviewDocKey = firstNonEmpty(strings.TrimSpace(evidence.ReviewDocKey), strings.TrimSpace(applied.ReviewDocKey))
	evidence.OperationID = firstNonEmpty(strings.TrimSpace(evidence.OperationID), strings.TrimSpace(applied.OperationID))
	evidence.OperationKind = firstNonEmpty(strings.TrimSpace(evidence.OperationKind), strings.TrimSpace(applied.OperationKind))
	evidence.CASPatchDigest = firstNonEmpty(strings.TrimSpace(evidence.CASPatchDigest), strings.TrimSpace(applied.CASPatchDigest))
	evidence.CASEvaluationDigest = firstNonEmpty(strings.TrimSpace(evidence.CASEvaluationDigest), strings.TrimSpace(applied.CASEvaluationDigest))
	evidence.RollbackEvidenceDigest = firstNonEmpty(strings.TrimSpace(evidence.RollbackEvidenceDigest), strings.TrimSpace(applied.RollbackEvidenceDigest))
	evidence.Summary = strings.TrimSpace(evidence.Summary)
	recordedAtValue := recordedAt.Format(time.RFC3339Nano)
	if suppliedRecordedAt := strings.TrimSpace(evidence.RecordedAt); suppliedRecordedAt != "" && suppliedRecordedAt != recordedAtValue {
		return repoauthority.PatchQueueReviewerAdvisory{}, "", "", fmt.Errorf("%w: reviewer advisory recorded_at must match storage timestamp", ErrProjectPatchQueueInvalid)
	}
	evidence.RecordedAt = recordedAtValue
	if err := repoauthority.ValidatePatchQueueReviewerAdvisory(evidence, applied); err != nil {
		return repoauthority.PatchQueueReviewerAdvisory{}, "", "", fmt.Errorf("%w: reviewer advisory is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return repoauthority.PatchQueueReviewerAdvisory{}, "", "", fmt.Errorf("marshal project patch queue reviewer advisory: %w", err)
	}
	return evidence, string(encoded), repoauthority.PatchQueueReviewerAdvisoryDigest(evidence), nil
}

func normalizeProjectPatchQueueOperatorEnablement(raw string, supplied repoauthority.PatchQueueOperatorEnablement, applied repoauthority.PatchQueueItem, advisory repoauthority.PatchQueueReviewerAdvisory, operatorID string, enabledAt time.Time) (repoauthority.PatchQueueOperatorEnablement, string, string, error) {
	raw = strings.TrimSpace(raw)
	evidence := supplied
	if !hasProjectPatchQueueOperatorEnablement(evidence) && raw != "" && raw != "{}" {
		if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
			return repoauthority.PatchQueueOperatorEnablement{}, "", "", fmt.Errorf("%w: operator_enablement_json must be an operator enablement object: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	evidence.Schema = firstNonEmpty(strings.TrimSpace(evidence.Schema), repoauthority.PatchQueueOperatorEnablementSchema)
	evidence.Scope = firstNonEmpty(strings.TrimSpace(evidence.Scope), repoauthority.PatchQueueOperatorEnablementScopeMutationActivation)
	if !evidence.Enabled {
		return repoauthority.PatchQueueOperatorEnablement{}, "", "", fmt.Errorf("%w: operator enablement must be explicitly enabled", ErrProjectPatchQueueInvalid)
	}
	operatorID = strings.TrimSpace(operatorID)
	if suppliedEnabledBy := strings.TrimSpace(evidence.EnabledBy); suppliedEnabledBy != "" && suppliedEnabledBy != operatorID {
		return repoauthority.PatchQueueOperatorEnablement{}, "", "", fmt.Errorf("%w: operator enablement enabled_by must match operator principal", ErrProjectPatchQueueInvalid)
	}
	evidence.EnabledBy = operatorID
	enabledAtValue := enabledAt.Format(time.RFC3339Nano)
	if suppliedEnabledAt := strings.TrimSpace(evidence.EnabledAt); suppliedEnabledAt != "" && suppliedEnabledAt != enabledAtValue {
		return repoauthority.PatchQueueOperatorEnablement{}, "", "", fmt.Errorf("%w: operator enablement enabled_at must match storage timestamp", ErrProjectPatchQueueInvalid)
	}
	evidence.EnabledAt = enabledAtValue
	evidence.Reason = strings.TrimSpace(evidence.Reason)
	if evidence.Reason == "" {
		evidence.Reason = "explicit operator enablement for repo mutation activation"
	}
	evidence.WorkspaceID = firstNonEmpty(strings.TrimSpace(evidence.WorkspaceID), strings.TrimSpace(applied.WorkspaceID))
	evidence.ProjectID = firstNonEmpty(strings.TrimSpace(evidence.ProjectID), strings.TrimSpace(applied.ProjectID))
	evidence.QueueID = firstNonEmpty(strings.TrimSpace(evidence.QueueID), strings.TrimSpace(applied.QueueID))
	evidence.ItemID = firstNonEmpty(strings.TrimSpace(evidence.ItemID), strings.TrimSpace(applied.ItemID))
	evidence.OperationID = firstNonEmpty(strings.TrimSpace(evidence.OperationID), strings.TrimSpace(applied.OperationID))
	evidence.CASPatchDigest = firstNonEmpty(strings.TrimSpace(evidence.CASPatchDigest), strings.TrimSpace(applied.CASPatchDigest))
	evidence.RollbackEvidenceDigest = firstNonEmpty(strings.TrimSpace(evidence.RollbackEvidenceDigest), strings.TrimSpace(applied.RollbackEvidenceDigest))
	evidence.ReviewerAdvisoryDigest = firstNonEmpty(strings.TrimSpace(evidence.ReviewerAdvisoryDigest), repoauthority.PatchQueueReviewerAdvisoryDigest(advisory))
	if err := repoauthority.ValidatePatchQueueOperatorEnablement(evidence, applied, advisory); err != nil {
		return repoauthority.PatchQueueOperatorEnablement{}, "", "", fmt.Errorf("%w: operator enablement is invalid: %v", ErrProjectPatchQueueInvalid, err)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return repoauthority.PatchQueueOperatorEnablement{}, "", "", fmt.Errorf("marshal project patch queue operator enablement: %w", err)
	}
	return evidence, string(encoded), repoauthority.PatchQueueOperatorEnablementDigest(evidence), nil
}

func normalizeProjectPatchQueueCASResult(raw string, supplied repoauthority.CASPatchApplyResult) (repoauthority.CASPatchApplyResult, string, error) {
	raw = strings.TrimSpace(raw)
	result := supplied
	if !hasProjectPatchQueueCASResult(result) && raw != "" && raw != "{}" {
		if err := json.Unmarshal([]byte(raw), &result); err != nil {
			return repoauthority.CASPatchApplyResult{}, "", fmt.Errorf("%w: cas_result_json must be a CAS apply result object: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	result.Schema = strings.TrimSpace(result.Schema)
	result.Status = strings.TrimSpace(result.Status)
	result.PatchID = strings.TrimSpace(result.PatchID)
	result.PatchDigest = strings.TrimSpace(result.PatchDigest)
	result.ContextDigest = strings.TrimSpace(result.ContextDigest)
	for i := range result.Paths {
		result.Paths[i].Path = strings.TrimSpace(result.Paths[i].Path)
		result.Paths[i].Status = strings.TrimSpace(result.Paths[i].Status)
		result.Paths[i].BaseHash = strings.TrimSpace(result.Paths[i].BaseHash)
		result.Paths[i].CurrentHash = strings.TrimSpace(result.Paths[i].CurrentHash)
		result.Paths[i].CandidateHash = strings.TrimSpace(result.Paths[i].CandidateHash)
	}
	sort.Slice(result.Paths, func(i, j int) bool {
		return result.Paths[i].Path < result.Paths[j].Path
	})
	for i := range result.Issues {
		result.Issues[i].Status = strings.TrimSpace(result.Issues[i].Status)
		result.Issues[i].Kind = strings.TrimSpace(result.Issues[i].Kind)
		result.Issues[i].Path = strings.TrimSpace(result.Issues[i].Path)
		result.Issues[i].Message = strings.TrimSpace(result.Issues[i].Message)
		result.Issues[i].ExpectedHash = strings.TrimSpace(result.Issues[i].ExpectedHash)
		result.Issues[i].ActualHash = strings.TrimSpace(result.Issues[i].ActualHash)
		result.Issues[i].CandidateHash = strings.TrimSpace(result.Issues[i].CandidateHash)
	}
	sort.Slice(result.Issues, func(i, j int) bool {
		if result.Issues[i].Status != result.Issues[j].Status {
			return result.Issues[i].Status < result.Issues[j].Status
		}
		if result.Issues[i].Path != result.Issues[j].Path {
			return result.Issues[i].Path < result.Issues[j].Path
		}
		return result.Issues[i].Kind < result.Issues[j].Kind
	})
	if len(result.Paths) == 0 {
		result.Paths = nil
	}
	if len(result.Issues) == 0 {
		result.Issues = nil
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return repoauthority.CASPatchApplyResult{}, "", fmt.Errorf("marshal project patch queue CAS result: %w", err)
	}
	return result, string(encoded), nil
}

func normalizeProjectPatchQueueCASTestEvidence(raw string, supplied repoauthority.PatchQueueTestEvidence) (repoauthority.PatchQueueTestEvidence, string, error) {
	raw = strings.TrimSpace(raw)
	evidence := supplied
	if !hasProjectPatchQueueCASTestEvidence(evidence) && raw != "" && raw != "{}" {
		if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
			return repoauthority.PatchQueueTestEvidence{}, "", fmt.Errorf("%w: cas_test_evidence_json must be a patch queue test evidence object: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	evidence.Schema = strings.TrimSpace(evidence.Schema)
	if evidence.Schema == "" && hasProjectPatchQueueCASTestEvidence(evidence) {
		evidence.Schema = repoauthority.PatchQueueTestEvidenceSchemaVersion
	}
	evidence.Name = strings.TrimSpace(evidence.Name)
	evidence.Command = strings.TrimSpace(evidence.Command)
	evidence.Status = strings.TrimSpace(evidence.Status)
	evidence.OutputDigest = strings.TrimSpace(evidence.OutputDigest)
	evidence.OutputSummary = strings.TrimSpace(evidence.OutputSummary)
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return repoauthority.PatchQueueTestEvidence{}, "", fmt.Errorf("marshal project patch queue CAS test evidence: %w", err)
	}
	return evidence, string(encoded), nil
}

func hasProjectPatchQueueCASResult(result repoauthority.CASPatchApplyResult) bool {
	if len(result.Paths) > 0 || len(result.Issues) > 0 {
		return true
	}
	for _, value := range []string{
		result.Schema,
		result.Status,
		result.PatchID,
		result.PatchDigest,
		result.ContextDigest,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func hasProjectPatchQueueCASTestEvidence(evidence repoauthority.PatchQueueTestEvidence) bool {
	if evidence.ExitCode != 0 || evidence.DurationMillis != 0 {
		return true
	}
	for _, value := range []string{
		evidence.Schema,
		evidence.Name,
		evidence.Command,
		evidence.Status,
		evidence.OutputDigest,
		evidence.OutputSummary,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func hasProjectPatchQueueMaterialization(materialization repoauthority.PatchMaterialization) bool {
	if len(materialization.Files) > 0 {
		return true
	}
	for _, value := range []string{
		materialization.Schema,
		materialization.WorkspaceID,
		materialization.ProjectID,
		materialization.QueueID,
		materialization.ItemID,
		materialization.OperationID,
		materialization.OperationKind,
		materialization.CASPatchDigest,
		materialization.CASEvaluationDigest,
		materialization.RecordedBy,
		materialization.RecordedAt,
		materialization.MaterializationDigest,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func hasProjectPatchQueueRollbackEvidence(evidence repoauthority.PatchQueueRollback) bool {
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

func hasProjectPatchQueueReviewerAdvisory(evidence repoauthority.PatchQueueReviewerAdvisory) bool {
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

func hasProjectPatchQueueOperatorEnablement(evidence repoauthority.PatchQueueOperatorEnablement) bool {
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

func projectPatchQueueRepoAuthorityAppliedItem(item ProjectPatchQueueItemRecord) repoauthority.PatchQueueItem {
	return repoauthority.PatchQueueItem{
		Schema:                   repoauthority.PatchQueueItemSchemaVersion,
		ID:                       strings.TrimSpace(item.QueueID) + "/" + strings.TrimSpace(item.ItemID),
		QueueID:                  strings.TrimSpace(item.QueueID),
		ItemID:                   strings.TrimSpace(item.ItemID),
		ReviewDocKey:             strings.TrimSpace(item.ReviewDocKey),
		State:                    repoauthority.PatchQueueStateApplied,
		Attempt:                  item.Attempt,
		MaxAttempts:              item.MaxAttempts,
		NextRetryAt:              strings.TrimSpace(item.NextRetryAt),
		DeadLetteredAt:           strings.TrimSpace(item.DeadLetteredAt),
		ContextDigest:            strings.TrimSpace(item.ContextDigest),
		RepoLeaseID:              strings.TrimSpace(item.RepoLeaseID),
		LeaseTerm:                item.LeaseTerm,
		Pathset:                  append([]string(nil), item.Pathset...),
		WorkspaceID:              strings.TrimSpace(item.WorkspaceID),
		ProjectID:                strings.TrimSpace(item.ProjectID),
		TaskID:                   strings.TrimSpace(item.TaskID),
		SessionID:                strings.TrimSpace(item.SessionID),
		RunID:                    strings.TrimSpace(item.RunID),
		AgentID:                  strings.TrimSpace(item.AgentID),
		PrincipalType:            strings.TrimSpace(item.PrincipalType),
		PrincipalID:              strings.TrimSpace(item.PrincipalID),
		CapabilitySnapshotID:     strings.TrimSpace(item.CapabilitySnapshotID),
		CapabilitySnapshotSchema: strings.TrimSpace(item.CapabilitySnapshotSchema),
		BaseRef:                  strings.TrimSpace(item.BaseRef),
		BaseTreeHash:             strings.TrimSpace(item.BaseTreeHash),
		CASResult:                item.CASResult,
		CASPatchDigest:           strings.TrimSpace(item.CASPatchDigest),
		CASEvaluationDigest:      strings.TrimSpace(item.CASEvaluationDigest),
		TestEvidence:             item.CASTestEvidence,
		TestEvidenceDigest:       strings.TrimSpace(item.CASTestEvidenceDigest),
		RollbackEvidence:         item.RollbackEvidence,
		RollbackEvidenceDigest:   strings.TrimSpace(item.RollbackEvidenceDigest),
		ReviewerAdvisory:         item.ReviewerAdvisory,
		ReviewerAdvisoryDigest:   strings.TrimSpace(item.ReviewerAdvisoryDigest),
		OperatorEnablement:       item.OperatorEnablement,
		OperatorEnablementDigest: strings.TrimSpace(item.OperatorEnablementDigest),
		OperationID:              strings.TrimSpace(item.OperationID),
		OperationKind:            strings.TrimSpace(item.OperationKind),
		CreatedAt:                strings.TrimSpace(item.CreatedAt),
		UpdatedAt:                strings.TrimSpace(item.UpdatedAt),
	}
}

func projectPatchQueueOperationLeaseContextDigest(authority repoauthority.Context) (string, error) {
	authority = authority.WithDefaults()
	authority.Lease = repoauthority.LeaseRef{}
	authority.PatchQueue = repoauthority.PatchQueueRef{}
	authority.Operation = repoauthority.OperationRef{}
	return authority.Digest()
}

func normalizeProjectPatchQueueBaseFileHashes(raw string, supplied map[string]string) (string, map[string]string, error) {
	raw = strings.TrimSpace(raw)
	var source map[string]string
	if len(supplied) > 0 {
		source = supplied
	} else if raw != "" && raw != "{}" {
		if err := json.Unmarshal([]byte(raw), &source); err != nil {
			return "", nil, fmt.Errorf("%w: base_file_hashes_json must be a JSON object of path hashes", ErrProjectPatchQueueInvalid)
		}
	}
	if len(source) == 0 {
		return "{}", nil, nil
	}
	normalized := make(map[string]string, len(source))
	for rawPath, rawHash := range source {
		path, err := repoauthority.NormalizePath(rawPath)
		if err != nil {
			return "", nil, fmt.Errorf("%w: base_file_hashes_json path %q invalid: %v", ErrProjectPatchQueueInvalid, rawPath, err)
		}
		hash := strings.TrimSpace(rawHash)
		if hash == "" {
			return "", nil, fmt.Errorf("%w: base_file_hashes_json[%s] is required", ErrProjectPatchQueueInvalid, path)
		}
		normalized[path] = hash
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", nil, err
	}
	return string(encoded), normalized, nil
}

func decodeProjectPatchQueueBaseFileHashes(raw string) map[string]string {
	_, decoded, err := normalizeProjectPatchQueueBaseFileHashes(raw, nil)
	if err != nil || len(decoded) == 0 {
		return nil
	}
	return decoded
}

func projectPatchQueueBindingRefsPresent(item ProjectPatchQueueItemRecord) bool {
	if item.LeaseTerm != 0 || len(item.BaseFileHashes) > 0 {
		return true
	}
	for _, value := range []string{
		item.TaskID,
		item.SessionID,
		item.RunID,
		item.AgentID,
		item.PrincipalType,
		item.PrincipalID,
		item.CapabilitySnapshotID,
		item.CapabilitySnapshotSchema,
		item.RepoRoot,
		item.BaseTreeHash,
		item.BaseFileHashesJSON,
		item.ContextDigest,
		item.RepoLeaseID,
		item.OperationID,
		item.OperationKind,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func projectPatchQueueBindingContext(item ProjectPatchQueueItemRecord, includeOperation bool) repoauthority.Context {
	context := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: strings.TrimSpace(item.WorkspaceID),
		TaskID:      strings.TrimSpace(item.TaskID),
		SessionID:   strings.TrimSpace(item.SessionID),
		RunID:       strings.TrimSpace(item.RunID),
		AgentID:     strings.TrimSpace(item.AgentID),
		Principal: repoauthority.PrincipalRef{
			Type: strings.TrimSpace(item.PrincipalType),
			ID:   strings.TrimSpace(item.PrincipalID),
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     strings.TrimSpace(item.CapabilitySnapshotID),
			Schema: strings.TrimSpace(item.CapabilitySnapshotSchema),
		},
		RepoRoot: strings.TrimSpace(item.RepoRoot),
		Base: repoauthority.BaseIdentity{
			Ref:        strings.TrimSpace(item.BaseRef),
			TreeHash:   strings.TrimSpace(item.BaseTreeHash),
			FileHashes: item.BaseFileHashes,
		},
		Pathset: append([]string(nil), item.Pathset...),
		Lease: repoauthority.LeaseRef{
			ID:   strings.TrimSpace(item.RepoLeaseID),
			Term: item.LeaseTerm,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: strings.TrimSpace(item.QueueID),
			ItemID:  strings.TrimSpace(item.ItemID),
		},
	}
	if includeOperation {
		context.Operation = repoauthority.OperationRef{
			ID:   strings.TrimSpace(item.OperationID),
			Kind: strings.TrimSpace(item.OperationKind),
		}
	}
	return context
}

func getProjectPatchQueueItemTx(ctx context.Context, tx *sql.Tx, queueID, itemID string) (ProjectPatchQueueItemRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE queue_id = ? AND item_id = ?`,
		strings.TrimSpace(queueID), strings.TrimSpace(itemID))
	item, err := scanProjectPatchQueueItem(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectPatchQueueItemRecord{}, false, nil
		}
		return ProjectPatchQueueItemRecord{}, false, err
	}
	return item, true, nil
}

func getLiveProjectPatchQueueItemByBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID string) (ProjectPatchQueueItemRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ? AND state IN ('PROPOSED', 'CLAIMED')
 ORDER BY updated_at DESC, queue_id ASC, item_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(branchID))
	item, err := scanProjectPatchQueueItem(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectPatchQueueItemRecord{}, false, nil
		}
		return ProjectPatchQueueItemRecord{}, false, err
	}
	return item, true, nil
}

func getLatestAcceptedProjectPatchQueueItemByBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID string) (ProjectPatchQueueItemRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ? AND state = 'ACCEPTED'
 ORDER BY decided_at DESC, updated_at DESC, queue_id ASC, item_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(branchID))
	item, err := scanProjectPatchQueueItem(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectPatchQueueItemRecord{}, false, nil
		}
		return ProjectPatchQueueItemRecord{}, false, err
	}
	return item, true, nil
}

func getLatestProjectPatchQueueTerminalItemForBranchHeadTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID, headSHA string) (ProjectPatchQueueItemRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ? AND head_sha = ?
   AND state IN ('ACCEPTED', 'INTEGRATED', 'REJECTED', 'BLOCKED', 'CANCELED')
 ORDER BY updated_at DESC, queue_id ASC, item_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(branchID), strings.TrimSpace(headSHA))
	item, err := scanProjectPatchQueueItem(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectPatchQueueItemRecord{}, false, nil
		}
		return ProjectPatchQueueItemRecord{}, false, err
	}
	return item, true, nil
}

func listControlledProjectPatchQueueItemsTx(ctx context.Context, tx *sql.Tx, limit int) ([]ProjectPatchQueueItemRecord, error) {
	if limit <= 0 {
		limit = 32
	}
	rows, err := tx.QueryContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items AS p
 WHERE repo_authority_mode = ? AND state IN ('PROPOSED', 'CLAIMED')
   AND NOT EXISTS (
     SELECT 1
       FROM runtime_events AS r
      WHERE r.workspace_id = p.workspace_id
        AND r.event_type = ?
        AND r.entity_type = 'project_patch_queue_item'
        AND r.entity_id = p.queue_id || '/' || p.item_id
   )
 ORDER BY updated_at DESC, queue_id ASC, item_id ASC
 LIMIT ?`,
		ProjectPatchQueueAuthorityModeControlledQueue, ProjectPatchQueueActuatorAppliedEventType, limit)
	if err != nil {
		return nil, fmt.Errorf("list controlled project patch queue candidates: %w", err)
	}
	defer rows.Close()
	var items []ProjectPatchQueueItemRecord
	for rows.Next() {
		item, err := scanProjectPatchQueueItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func upsertProjectPatchQueueItemTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO project_patch_queue_items (
  queue_id, item_id, workspace_id, project_id, repo_id, branch_id, review_doc_key,
  supersedes_queue_id, supersedes_item_id, evidence_doc_key,
  repo_authority_mode, state, attempt, max_attempts, next_retry_at, dead_lettered_at,
  pathset_json, base_ref, base_sha, head_sha,
  auto_merge, submitted_by, task_id, session_id, run_id, agent_id, principal_type,
  principal_id, capability_snapshot_id, capability_snapshot_schema, repo_root,
  base_tree_hash, base_file_hashes_json, context_digest, repo_lease_id, lease_term,
  operation_id, operation_kind, operation_binding_schema, operation_binding_accepted,
  operation_context_digest, operation_lease_context_digest, operation_mutation_paths_json,
  operation_bound_by, operation_bound_at, claimed_by, claim_token, claimed_at, claim_expires_at,
  decision_doc_key, decision_summary, decided_by, decided_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(queue_id, item_id) DO UPDATE SET
  review_doc_key = excluded.review_doc_key,
  supersedes_queue_id = excluded.supersedes_queue_id,
  supersedes_item_id = excluded.supersedes_item_id,
  evidence_doc_key = excluded.evidence_doc_key,
  repo_authority_mode = excluded.repo_authority_mode,
  state = excluded.state,
  attempt = excluded.attempt,
  max_attempts = excluded.max_attempts,
  next_retry_at = '',
  dead_lettered_at = '',
  pathset_json = excluded.pathset_json,
  base_ref = excluded.base_ref,
  base_sha = excluded.base_sha,
  head_sha = excluded.head_sha,
  auto_merge = excluded.auto_merge,
  submitted_by = excluded.submitted_by,
  task_id = excluded.task_id,
  session_id = excluded.session_id,
  run_id = excluded.run_id,
  agent_id = excluded.agent_id,
  principal_type = excluded.principal_type,
  principal_id = excluded.principal_id,
  capability_snapshot_id = excluded.capability_snapshot_id,
  capability_snapshot_schema = excluded.capability_snapshot_schema,
  repo_root = excluded.repo_root,
  base_tree_hash = excluded.base_tree_hash,
  base_file_hashes_json = excluded.base_file_hashes_json,
  context_digest = excluded.context_digest,
  repo_lease_id = excluded.repo_lease_id,
  lease_term = excluded.lease_term,
  operation_id = excluded.operation_id,
  operation_kind = excluded.operation_kind,
  operation_binding_schema = excluded.operation_binding_schema,
  operation_binding_accepted = excluded.operation_binding_accepted,
  operation_context_digest = excluded.operation_context_digest,
  operation_lease_context_digest = excluded.operation_lease_context_digest,
  operation_mutation_paths_json = excluded.operation_mutation_paths_json,
  operation_bound_by = excluded.operation_bound_by,
  operation_bound_at = excluded.operation_bound_at,
  cas_evidence_schema = '',
  cas_evidence_accepted = 0,
  cas_status = '',
  cas_patch_digest = '',
  cas_evaluation_digest = '',
  cas_result_json = '{}',
  cas_test_evidence_json = '{}',
  cas_test_evidence_digest = '',
  cas_recorded_by = '',
  cas_recorded_at = '',
  materialization_schema = '',
  materialization_accepted = 0,
  materialization_json = '{}',
  materialization_digest = '',
  materialization_recorded_by = '',
  materialization_recorded_at = '',
  materialization_authority_proof_json = '{}',
  materialization_authority_proof_digest = '',
  rollback_evidence_schema = '',
  rollback_evidence_accepted = 0,
  rollback_evidence_json = '{}',
  rollback_evidence_digest = '',
  rollback_recorded_by = '',
  rollback_recorded_at = '',
  reviewer_advisory_schema = '',
  reviewer_advisory_accepted = 0,
  reviewer_advisory_json = '{}',
  reviewer_advisory_digest = '',
  reviewer_recorded_by = '',
  reviewer_recorded_at = '',
  operator_enablement_schema = '',
  operator_enablement_accepted = 0,
  operator_enablement_json = '{}',
  operator_enablement_digest = '',
  operator_enabled_by = '',
  operator_enabled_at = '',
  claimed_by = '',
  claim_token = '',
  claimed_at = '',
  claim_expires_at = '',
  decision_doc_key = '',
  decision_summary = '',
  decided_by = '',
  decided_at = '',
 updated_at = excluded.updated_at`,
		item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID, item.RepoID, item.BranchID, item.ReviewDocKey,
		item.SupersedesQueueID, item.SupersedesItemID, item.EvidenceDocKey,
		item.RepoAuthorityMode, item.State, item.Attempt, item.MaxAttempts, item.NextRetryAt, item.DeadLetteredAt,
		item.PathsetJSON, item.BaseRef, item.BaseSHA, item.HeadSHA,
		boolToSQLiteInt(item.AutoMerge), item.SubmittedBy, item.TaskID, item.SessionID, item.RunID, item.AgentID,
		item.PrincipalType, item.PrincipalID, item.CapabilitySnapshotID, item.CapabilitySnapshotSchema, item.RepoRoot,
		item.BaseTreeHash, item.BaseFileHashesJSON, item.ContextDigest, item.RepoLeaseID, item.LeaseTerm,
		item.OperationID, item.OperationKind, item.OperationBindingSchema, boolToSQLiteInt(item.OperationBindingAccepted),
		item.OperationContextDigest, item.OperationLeaseContextDigest, item.OperationMutationPathsJSON,
		item.OperationBoundBy, item.OperationBoundAt, item.ClaimedBy, item.ClaimToken, item.ClaimedAt, item.ClaimExpiresAt,
		item.DecisionDocKey, item.DecisionSummary, item.DecidedBy, item.DecidedAt, item.CreatedAt, item.UpdatedAt); err != nil {
		return fmt.Errorf("upsert project patch queue item: %w", err)
	}
	return nil
}

func updateProjectPatchQueueLifecycleTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	result, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET state = ?,
       claimed_by = ?,
       claim_token = ?,
       claimed_at = ?,
       claim_expires_at = ?,
       decision_doc_key = ?,
       decision_summary = ?,
       decided_by = ?,
       decided_at = ?,
       updated_at = ?
 WHERE queue_id = ? AND item_id = ? AND workspace_id = ? AND project_id = ?`,
		item.State, item.ClaimedBy, item.ClaimToken, item.ClaimedAt, item.ClaimExpiresAt,
		item.DecisionDocKey, item.DecisionSummary, item.DecidedBy, item.DecidedAt, item.UpdatedAt,
		item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID)
	if err != nil {
		return fmt.Errorf("update project patch queue lifecycle: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("%w: patch queue item %s/%s not found", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return nil
}

func updateProjectPatchQueueOperationBindingTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	result, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operation_id = ?,
       operation_kind = ?,
       operation_binding_schema = ?,
       operation_binding_accepted = ?,
       operation_context_digest = ?,
       operation_lease_context_digest = ?,
       operation_mutation_paths_json = ?,
       operation_bound_by = ?,
       operation_bound_at = ?,
       updated_at = ?
 WHERE queue_id = ? AND item_id = ? AND workspace_id = ? AND project_id = ?`,
		item.OperationID, item.OperationKind, item.OperationBindingSchema, boolToSQLiteInt(item.OperationBindingAccepted),
		item.OperationContextDigest, item.OperationLeaseContextDigest, item.OperationMutationPathsJSON,
		item.OperationBoundBy, item.OperationBoundAt, item.UpdatedAt,
		item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID)
	if err != nil {
		return fmt.Errorf("update project patch queue operation binding: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("%w: patch queue item %s/%s not found", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return nil
}

func updateProjectPatchQueueCASEvidenceTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	result, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET cas_evidence_schema = ?,
       cas_evidence_accepted = ?,
       cas_status = ?,
       cas_patch_digest = ?,
       cas_evaluation_digest = ?,
       cas_result_json = ?,
       cas_test_evidence_json = ?,
       cas_test_evidence_digest = ?,
       cas_recorded_by = ?,
       cas_recorded_at = ?,
       updated_at = ?
 WHERE queue_id = ? AND item_id = ? AND workspace_id = ? AND project_id = ?`,
		item.CASEvidenceSchema, boolToSQLiteInt(item.CASEvidenceAccepted), item.CASStatus,
		item.CASPatchDigest, item.CASEvaluationDigest, item.CASResultJSON,
		item.CASTestEvidenceJSON, item.CASTestEvidenceDigest, item.CASRecordedBy,
		item.CASRecordedAt, item.UpdatedAt, item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID)
	if err != nil {
		return fmt.Errorf("update project patch queue CAS evidence: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("%w: patch queue item %s/%s not found", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return nil
}

func updateProjectPatchQueueMaterializationTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	result, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_schema = ?,
       materialization_accepted = ?,
       materialization_json = ?,
       materialization_digest = ?,
       materialization_recorded_by = ?,
       materialization_recorded_at = ?,
       materialization_authority_proof_json = ?,
       materialization_authority_proof_digest = ?,
       updated_at = ?
 WHERE queue_id = ? AND item_id = ? AND workspace_id = ? AND project_id = ?`,
		item.MaterializationSchema, boolToSQLiteInt(item.MaterializationAccepted),
		item.MaterializationJSON, item.MaterializationDigest, item.MaterializationRecordedBy,
		item.MaterializationRecordedAt, item.MaterializationAuthorityProofJSON, item.MaterializationAuthorityProofDigest,
		item.UpdatedAt, item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID)
	if err != nil {
		return fmt.Errorf("update project patch queue materialization: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("%w: patch queue item %s/%s not found", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return nil
}

func updateProjectPatchQueueRollbackEvidenceTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	result, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET rollback_evidence_schema = ?,
       rollback_evidence_accepted = ?,
       rollback_evidence_json = ?,
       rollback_evidence_digest = ?,
       rollback_recorded_by = ?,
       rollback_recorded_at = ?,
       updated_at = ?
 WHERE queue_id = ? AND item_id = ? AND workspace_id = ? AND project_id = ?`,
		item.RollbackEvidenceSchema, boolToSQLiteInt(item.RollbackEvidenceAccepted),
		item.RollbackEvidenceJSON, item.RollbackEvidenceDigest, item.RollbackRecordedBy,
		item.RollbackRecordedAt, item.UpdatedAt, item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID)
	if err != nil {
		return fmt.Errorf("update project patch queue rollback evidence: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("%w: patch queue item %s/%s not found", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return nil
}

func updateProjectPatchQueueReviewerAdvisoryTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	result, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET reviewer_advisory_schema = ?,
       reviewer_advisory_accepted = ?,
       reviewer_advisory_json = ?,
       reviewer_advisory_digest = ?,
       reviewer_recorded_by = ?,
       reviewer_recorded_at = ?,
       updated_at = ?
 WHERE queue_id = ? AND item_id = ? AND workspace_id = ? AND project_id = ?`,
		item.ReviewerAdvisorySchema, boolToSQLiteInt(item.ReviewerAdvisoryAccepted),
		item.ReviewerAdvisoryJSON, item.ReviewerAdvisoryDigest, item.ReviewerRecordedBy,
		item.ReviewerRecordedAt, item.UpdatedAt, item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID)
	if err != nil {
		return fmt.Errorf("update project patch queue reviewer advisory: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("%w: patch queue item %s/%s not found", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return nil
}

func updateProjectPatchQueueOperatorEnablementTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	result, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operator_enablement_schema = ?,
       operator_enablement_accepted = ?,
       operator_enablement_json = ?,
       operator_enablement_digest = ?,
       operator_enabled_by = ?,
       operator_enabled_at = ?,
       updated_at = ?
 WHERE queue_id = ? AND item_id = ? AND workspace_id = ? AND project_id = ?`,
		item.OperatorEnablementSchema, boolToSQLiteInt(item.OperatorEnablementAccepted),
		item.OperatorEnablementJSON, item.OperatorEnablementDigest, item.OperatorEnabledBy,
		item.OperatorEnabledAt, item.UpdatedAt, item.QueueID, item.ItemID, item.WorkspaceID, item.ProjectID)
	if err != nil {
		return fmt.Errorf("update project patch queue operator enablement: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("%w: patch queue item %s/%s not found", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return nil
}

func scanProjectPatchQueueItem(row interface{ Scan(dest ...any) error }) (ProjectPatchQueueItemRecord, error) {
	var item ProjectPatchQueueItemRecord
	var autoMerge int
	var operationBindingAccepted int
	var casEvidenceAccepted int
	var materializationAccepted int
	var rollbackEvidenceAccepted int
	var reviewerAdvisoryAccepted int
	var operatorEnablementAccepted int
	if err := row.Scan(
		&item.QueueID,
		&item.ItemID,
		&item.WorkspaceID,
		&item.ProjectID,
		&item.RepoID,
		&item.BranchID,
		&item.ReviewDocKey,
		&item.SupersedesQueueID,
		&item.SupersedesItemID,
		&item.EvidenceDocKey,
		&item.RepoAuthorityMode,
		&item.State,
		&item.Attempt,
		&item.MaxAttempts,
		&item.NextRetryAt,
		&item.DeadLetteredAt,
		&item.PathsetJSON,
		&item.BaseRef,
		&item.BaseSHA,
		&item.HeadSHA,
		&autoMerge,
		&item.SubmittedBy,
		&item.TaskID,
		&item.SessionID,
		&item.RunID,
		&item.AgentID,
		&item.PrincipalType,
		&item.PrincipalID,
		&item.CapabilitySnapshotID,
		&item.CapabilitySnapshotSchema,
		&item.RepoRoot,
		&item.BaseTreeHash,
		&item.BaseFileHashesJSON,
		&item.ContextDigest,
		&item.RepoLeaseID,
		&item.LeaseTerm,
		&item.OperationID,
		&item.OperationKind,
		&item.OperationBindingSchema,
		&operationBindingAccepted,
		&item.OperationContextDigest,
		&item.OperationLeaseContextDigest,
		&item.OperationMutationPathsJSON,
		&item.OperationBoundBy,
		&item.OperationBoundAt,
		&item.CASEvidenceSchema,
		&casEvidenceAccepted,
		&item.CASStatus,
		&item.CASPatchDigest,
		&item.CASEvaluationDigest,
		&item.CASResultJSON,
		&item.CASTestEvidenceJSON,
		&item.CASTestEvidenceDigest,
		&item.CASRecordedBy,
		&item.CASRecordedAt,
		&item.MaterializationSchema,
		&materializationAccepted,
		&item.MaterializationJSON,
		&item.MaterializationDigest,
		&item.MaterializationRecordedBy,
		&item.MaterializationRecordedAt,
		&item.MaterializationAuthorityProofJSON,
		&item.MaterializationAuthorityProofDigest,
		&item.RollbackEvidenceSchema,
		&rollbackEvidenceAccepted,
		&item.RollbackEvidenceJSON,
		&item.RollbackEvidenceDigest,
		&item.RollbackRecordedBy,
		&item.RollbackRecordedAt,
		&item.ReviewerAdvisorySchema,
		&reviewerAdvisoryAccepted,
		&item.ReviewerAdvisoryJSON,
		&item.ReviewerAdvisoryDigest,
		&item.ReviewerRecordedBy,
		&item.ReviewerRecordedAt,
		&item.OperatorEnablementSchema,
		&operatorEnablementAccepted,
		&item.OperatorEnablementJSON,
		&item.OperatorEnablementDigest,
		&item.OperatorEnabledBy,
		&item.OperatorEnabledAt,
		&item.ClaimedBy,
		&item.ClaimToken,
		&item.ClaimedAt,
		&item.ClaimExpiresAt,
		&item.DecisionDocKey,
		&item.DecisionSummary,
		&item.DecidedBy,
		&item.DecidedAt,
		&item.CreatedAt,
		&item.UpdatedAt,
	); err != nil {
		return ProjectPatchQueueItemRecord{}, err
	}
	item.AutoMerge = sqliteIntToBool(autoMerge)
	item.OperationBindingAccepted = sqliteIntToBool(operationBindingAccepted)
	item.CASEvidenceAccepted = sqliteIntToBool(casEvidenceAccepted)
	item.MaterializationAccepted = sqliteIntToBool(materializationAccepted)
	item.RollbackEvidenceAccepted = sqliteIntToBool(rollbackEvidenceAccepted)
	item.ReviewerAdvisoryAccepted = sqliteIntToBool(reviewerAdvisoryAccepted)
	item.OperatorEnablementAccepted = sqliteIntToBool(operatorEnablementAccepted)
	_ = json.Unmarshal([]byte(item.PathsetJSON), &item.Pathset)
	_ = json.Unmarshal([]byte(item.OperationMutationPathsJSON), &item.OperationMutationPaths)
	_ = json.Unmarshal([]byte(item.CASResultJSON), &item.CASResult)
	_ = json.Unmarshal([]byte(item.CASTestEvidenceJSON), &item.CASTestEvidence)
	_ = json.Unmarshal([]byte(item.MaterializationJSON), &item.Materialization)
	_ = json.Unmarshal([]byte(item.MaterializationAuthorityProofJSON), &item.MaterializationAuthorityProof)
	_ = json.Unmarshal([]byte(item.RollbackEvidenceJSON), &item.RollbackEvidence)
	_ = json.Unmarshal([]byte(item.ReviewerAdvisoryJSON), &item.ReviewerAdvisory)
	_ = json.Unmarshal([]byte(item.OperatorEnablementJSON), &item.OperatorEnablement)
	item.BaseFileHashes = decodeProjectPatchQueueBaseFileHashes(item.BaseFileHashesJSON)
	return item, nil
}

func (s *Store) validateProjectPatchQueueItemEvidenceTx(ctx context.Context, tx *sql.Tx, item ProjectPatchQueueItemRecord) error {
	repo, err := getProjectRepositoryTx(ctx, tx, item.WorkspaceID, item.ProjectID, item.RepoID)
	if err != nil {
		return err
	}
	if repo.RepoStatus != ProjectRepositoryStatusReady || strings.TrimSpace(repo.RemoteURL) == "" {
		return fmt.Errorf("%w: repo %s must remain READY with remote_url", ErrProjectPatchQueueInvalid, item.RepoID)
	}
	branch, ok, err := validateProjectBranchIDScopeTx(ctx, tx, item.BranchID, item.WorkspaceID, item.ProjectID, item.RepoID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: branch_id %s not found", ErrProjectPatchQueueInvalid, item.BranchID)
	}
	if branch.Status != ProjectBranchStatusReadyForReview {
		return fmt.Errorf("%w: branch %s must remain READY_FOR_REVIEW", ErrProjectPatchQueueInvalid, item.BranchID)
	}
	if !isCanonicalProjectGitObjectID(branch.HeadSHA) || !isCanonicalProjectGitObjectID(item.HeadSHA) || strings.TrimSpace(branch.HeadSHA) != strings.TrimSpace(item.HeadSHA) {
		return fmt.Errorf("%w: branch head_sha drifted from patch queue evidence", ErrProjectPatchQueueInvalid)
	}
	if !isCanonicalProjectGitObjectID(branch.BaseSHA) || !isCanonicalProjectGitObjectID(item.BaseSHA) || strings.TrimSpace(branch.BaseSHA) != strings.TrimSpace(item.BaseSHA) {
		return fmt.Errorf("%w: branch base_sha drifted from patch queue evidence", ErrProjectPatchQueueInvalid)
	}
	if strings.TrimSpace(item.ReviewDocKey) == "" || strings.TrimSpace(item.ReviewDocKey) != strings.TrimSpace(branch.ReviewDocKey) {
		return fmt.Errorf("%w: review_doc_key drifted from branch evidence", ErrProjectPatchQueueInvalid)
	}
	if doc, err := s.loadWorkspaceDocTx(ctx, tx, item.WorkspaceID, item.ReviewDocKey); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: review_doc_key %s not found", ErrProjectPatchQueueInvalid, item.ReviewDocKey)
		}
		return err
	} else if doc.ArchivedAt != nil {
		return fmt.Errorf("%w: review_doc_key %s is archived", ErrProjectPatchQueueInvalid, item.ReviewDocKey)
	} else {
		fidelity, err := s.projectSourceFidelityContextTx(ctx, tx, item.WorkspaceID, item.ProjectID)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
		}
		if err := s.validateProjectSourceFidelityTraceTx(ctx, tx, item.WorkspaceID, fidelity); err != nil {
			return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
		}
		if err := validateProjectSourceFidelityReviewDoc(doc, fidelity); err != nil {
			return fmt.Errorf("%w: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	itemPathset := projectBranchReviewScopePaths(item.PathsetJSON)
	branchPathset := projectBranchReviewScopePaths(branch.WriteScopeJSON)
	if len(itemPathset) == 0 || len(branchPathset) == 0 || !writeScopePathsCoveredBy(itemPathset, branchPathset) {
		return fmt.Errorf("%w: patch queue pathset drifted outside branch write_scope_json", ErrProjectPatchQueueInvalid)
	}
	if err := validateProjectPatchQueueRetryContract(item); err != nil {
		return err
	}
	if projectPatchQueueStateIsLive(item.State) && projectPatchQueueOperationBindingEvidencePresent(item) {
		if err := validateProjectPatchQueueOperationBindingEvidence(item); err != nil {
			return fmt.Errorf("%w: live patch queue operation refs require verified operation binding evidence: %v", ErrProjectPatchQueueInvalid, err)
		}
		if err := validateProjectPatchQueueOperationLedgerEvidenceTx(ctx, tx, item); err != nil {
			return err
		}
	}
	if projectPatchQueueStateIsLive(item.State) && projectPatchQueueCASEvidencePresent(item) {
		if err := validateProjectPatchQueueCASEvidence(item); err != nil {
			return fmt.Errorf("%w: live patch queue CAS evidence requires verified conflict-safe evidence: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	if projectPatchQueueStateIsLive(item.State) && projectPatchQueueMaterializationPresent(item) {
		if err := validateProjectPatchQueueMaterialization(item); err != nil {
			return fmt.Errorf("%w: live patch queue materialization requires verified content-bound CAS proof: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	if projectPatchQueueStateIsLive(item.State) && projectPatchQueueRollbackEvidencePresent(item) {
		if err := validateProjectPatchQueueRollbackEvidence(item); err != nil {
			return fmt.Errorf("%w: live patch queue rollback evidence requires verified rollback proof: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	if projectPatchQueueStateIsLive(item.State) && projectPatchQueueReviewerAdvisoryPresent(item) {
		if err := validateProjectPatchQueueReviewerAdvisory(item); err != nil {
			return fmt.Errorf("%w: live patch queue reviewer advisory requires verified advisory proof: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	if projectPatchQueueStateIsLive(item.State) && projectPatchQueueOperatorEnablementPresent(item) {
		if err := validateProjectPatchQueueOperatorEnablement(item); err != nil {
			return fmt.Errorf("%w: live patch queue operator enablement requires verified operator proof: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	if projectPatchQueueBindingRefsPresent(item) {
		contextDigest, err := projectPatchQueueBindingContext(item, false).Digest()
		if err != nil {
			return fmt.Errorf("%w: patch queue binding context is invalid: %v", ErrProjectPatchQueueInvalid, err)
		}
		if storedDigest := strings.TrimSpace(item.ContextDigest); storedDigest != "" && storedDigest != contextDigest {
			return fmt.Errorf("%w: patch queue binding context_digest drifted from stored refs", ErrProjectPatchQueueInvalid)
		}
	}
	return nil
}

func (s *Store) requireProjectPatchQueueIntegrationActorTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID, actorType, now string) error {
	if !strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		return nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND status = 'ACTIVE'
   AND (
     role_type = ?
     OR role_type = ?
     OR (role_type = ? AND (lease_expires_at = '' OR lease_expires_at > ?))
   )`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(actorID),
		ProjectRoleIntegrator, ProjectRoleReviewer, ProjectRoleStrategicLead, strings.TrimSpace(now)).Scan(&count); err != nil {
		return fmt.Errorf("check project patch queue integration role: %w", err)
	}
	if count > 0 {
		return nil
	}
	var registeredRole string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(role, '')
  FROM agents
 WHERE workspace_id = ? AND agent_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(actorID)).Scan(&registeredRole); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check project patch queue registered agent role: %w", err)
	}
	if projectPatchQueueRegisteredAgentRoleAllowsIntegration(registeredRole) {
		return nil
	}
	if count == 0 {
		return fmt.Errorf("%w: agent %s requires active INTEGRATOR/REVIEWER role, strategic lead lease, or registered reviewer/integrator agent role", ErrProjectPatchQueueInvalid, actorID)
	}
	return nil
}

func projectPatchQueueRegisteredAgentRoleAllowsIntegration(role string) bool {
	role = normalizeProjectRegisteredAgentRole(role)
	if projectRegisteredAgentRoleAllowsIntegrationLane(role) {
		return true
	}
	return projectRegisteredAgentRoleAllowsReviewLane(role)
}

func normalizeProjectRegisteredAgentRole(role string) string {
	role = strings.ToLower(strings.TrimSpace(role))
	role = strings.ReplaceAll(role, "_", " ")
	role = strings.ReplaceAll(role, "-", " ")
	role = strings.ReplaceAll(role, "/", " ")
	return strings.Join(strings.Fields(role), " ")
}

func projectRegisteredAgentRoleAllowsIntegrationLane(role string) bool {
	role = normalizeProjectRegisteredAgentRole(role)
	if role == "" {
		return false
	}
	switch role {
	case "integrator", "integration":
		return true
	}
	return projectRegisteredAgentRoleHasToken(role, "integrator", "integration") ||
		projectRegisteredAgentRoleHasPhrase(role, "release captain", "release owner", "patch queue integrator", "patch queue integration")
}

func projectRegisteredAgentRoleAllowsReviewLane(role string) bool {
	role = normalizeProjectRegisteredAgentRole(role)
	if role == "" {
		return false
	}
	switch role {
	case "reviewer", "review", "qa", "tester", "verifier", "quality assurance":
		return true
	}
	if projectRegisteredAgentRoleHasStrongReviewSignal(role) {
		return true
	}
	if projectRegisteredAgentRoleHasImplementationSignal(role) {
		return false
	}
	return projectRegisteredAgentRoleHasToken(role, "reviewer", "review")
}

func projectRegisteredAgentRoleHasStrongReviewSignal(role string) bool {
	role = normalizeProjectRegisteredAgentRole(role)
	return projectRegisteredAgentRoleHasToken(role, "qa", "tester", "testing", "verifier", "validation", "validator", "critic", "usability", "accessibility") ||
		projectRegisteredAgentRoleHasPhrase(role, "quality assurance", "browser smoke", "performance tester", "visual algorithm verifier", "real user", "ux critic", "ux qa", "ux review", "user experience")
}

func projectRegisteredAgentRoleHasImplementationSignal(role string) bool {
	role = normalizeProjectRegisteredAgentRole(role)
	return projectRegisteredAgentRoleHasToken(role, "implementer", "implementation", "builder", "frontend", "backend", "fullstack", "coder")
}

func projectRegisteredAgentRoleHasToken(role string, tokens ...string) bool {
	fields := strings.Fields(normalizeProjectRegisteredAgentRole(role))
	if len(fields) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		seen[field] = struct{}{}
	}
	for _, token := range tokens {
		token = normalizeProjectRegisteredAgentRole(token)
		if token == "" || strings.Contains(token, " ") {
			continue
		}
		if _, ok := seen[token]; ok {
			return true
		}
	}
	return false
}

func projectRegisteredAgentRoleHasPhrase(role string, phrases ...string) bool {
	role = normalizeProjectRegisteredAgentRole(role)
	if role == "" {
		return false
	}
	for _, phrase := range phrases {
		phrase = normalizeProjectRegisteredAgentRole(phrase)
		if phrase != "" && strings.Contains(role, phrase) {
			return true
		}
	}
	return false
}

func projectPatchQueueClaimLeaseUntil(now time.Time, seconds int) string {
	if seconds <= 0 {
		seconds = 3600
	}
	if seconds > 86400 {
		seconds = 86400
	}
	return now.Add(time.Duration(seconds) * time.Second).UTC().Format(time.RFC3339Nano)
}

func projectPatchQueueActuatorDedupKey(workspaceID, queueID, itemID, materializationDigest string) string {
	return "project.patch_queue.actuator_applied:" +
		strings.TrimSpace(workspaceID) + ":" +
		strings.TrimSpace(queueID) + "/" + strings.TrimSpace(itemID) + ":" +
		strings.TrimSpace(materializationDigest)
}

func projectPatchQueueClaimActiveAt(item ProjectPatchQueueItemRecord, now time.Time) bool {
	expiresAt := strings.TrimSpace(item.ClaimExpiresAt)
	if expiresAt == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return false
	}
	return parsed.After(now)
}

func normalizeProjectPatchQueueIntegrationOutcome(value string) (string, error) {
	outcome := strings.ToLower(strings.TrimSpace(value))
	switch outcome {
	case ProjectPatchQueueIntegrationOutcomeAdmitted, ProjectPatchQueueIntegrationOutcomeIntegrated, ProjectPatchQueueIntegrationOutcomeRepair:
		return outcome, nil
	default:
		return "", fmt.Errorf("%w: unsupported patch queue integration outcome %q", ErrProjectPatchQueueInvalid, value)
	}
}

func projectPatchQueueIntegrationModeForItem(item ProjectPatchQueueItemRecord, requested string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(requested))
	switch mode {
	case "", ProjectPatchQueueIntegrationModeMaterialized, ProjectPatchQueueIntegrationModeDirectMerge:
	default:
		return "", fmt.Errorf("%w: unsupported patch queue integration_mode %s", ErrProjectPatchQueueInvalid, requested)
	}
	if strings.TrimSpace(item.RepoAuthorityMode) != ProjectPatchQueueAuthorityModeControlledQueue {
		return firstNonEmpty(mode, ProjectPatchQueueIntegrationModeDirectMerge), nil
	}
	if ProjectPatchQueueMaterializationReady(item) {
		return firstNonEmpty(mode, ProjectPatchQueueIntegrationModeMaterialized), nil
	}
	if mode != ProjectPatchQueueIntegrationModeDirectMerge {
		return "", fmt.Errorf("%w: controlled patch queue item %s/%s requires durable materialization before integration or explicit direct_merge integration_mode", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID)
	}
	return mode, nil
}

func projectPatchQueueIntegrationEventType(outcome string) string {
	switch outcome {
	case ProjectPatchQueueIntegrationOutcomeAdmitted:
		return ProjectPatchQueueIntegrationAdmittedEventType
	case ProjectPatchQueueIntegrationOutcomeIntegrated:
		return ProjectPatchQueueIntegratedEventType
	case ProjectPatchQueueIntegrationOutcomeRepair:
		return ProjectPatchQueueIntegrationRepairEventType
	default:
		return "project.patch_queue.integration_" + strings.TrimSpace(outcome)
	}
}

func canonicalProjectPatchQueueIntegrationTargetBranch(branch string) string {
	branch = strings.TrimSpace(branch)
	branch = strings.TrimPrefix(branch, "refs/heads/")
	return strings.TrimSpace(branch)
}

func projectPatchQueueIntegrationReceiptForItemTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string, item ProjectPatchQueueItemRecord, eventType, targetBranch, targetHeadAfter string) (RuntimeEventRecord, bool, error) {
	eventType = strings.TrimSpace(eventType)
	targetBranch = canonicalProjectPatchQueueIntegrationTargetBranch(targetBranch)
	targetHeadAfter = strings.TrimSpace(targetHeadAfter)
	sourceBranchID := strings.TrimSpace(item.BranchID)
	sourceHeadSHA := strings.TrimSpace(item.HeadSHA)
	if eventType == "" || sourceBranchID == "" || sourceHeadSHA == "" {
		return RuntimeEventRecord{}, false, nil
	}
	var event RuntimeEventRecord
	err := scanRuntimeEvent(tx.QueryRowContext(ctx, `
SELECT `+runtimeEventSelectColumns+`
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND entity_type = 'project_patch_queue_item'
   AND entity_id = ?
   AND json_valid(payload_json)
   AND TRIM(COALESCE(json_extract(payload_json, '$.project_id'), '')) = ?
   AND TRIM(COALESCE(json_extract(payload_json, '$.source_branch_id'), '')) = ?
   AND TRIM(COALESCE(json_extract(payload_json, '$.source_head_sha'), '')) = ?
   AND (? = '' OR TRIM(CASE
       WHEN TRIM(COALESCE(json_extract(payload_json, '$.target_branch'), '')) LIKE 'refs/heads/%'
       THEN substr(TRIM(COALESCE(json_extract(payload_json, '$.target_branch'), '')), 12)
       ELSE TRIM(COALESCE(json_extract(payload_json, '$.target_branch'), ''))
   END) = ?)
   AND (? = '' OR TRIM(COALESCE(json_extract(payload_json, '$.target_head_after'), '')) = ?)
 ORDER BY ingest_seq DESC, event_id DESC
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		eventType,
		strings.TrimSpace(item.QueueID)+"/"+strings.TrimSpace(item.ItemID),
		strings.TrimSpace(projectID),
		sourceBranchID,
		sourceHeadSHA,
		targetBranch,
		targetBranch,
		targetHeadAfter,
		targetHeadAfter,
	), &event)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, false, nil
		}
		return RuntimeEventRecord{}, false, fmt.Errorf("load existing project patch queue integration receipt: %w", err)
	}
	return event, true, nil
}

func projectPatchQueueIntegrationInputFromReceipt(input ProjectPatchQueueIntegrationRecordInput, event RuntimeEventRecord) ProjectPatchQueueIntegrationRecordInput {
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(event.PayloadJSON)), &payload); err != nil {
		return input
	}
	if value := projectPatchQueueReceiptPayloadString(payload, "target_branch"); value != "" {
		input.TargetBranch = value
	}
	if value := projectPatchQueueReceiptPayloadString(payload, "target_head_before"); value != "" {
		input.TargetHeadBefore = value
	}
	if value := projectPatchQueueReceiptPayloadString(payload, "target_head_after"); value != "" {
		input.TargetHeadAfter = value
	}
	if value := projectPatchQueueReceiptPayloadString(payload, "remote_target_head_after"); value != "" {
		input.RemoteTargetHeadAfter = value
	}
	if value := projectPatchQueueReceiptPayloadString(payload, "integration_mode"); value != "" {
		input.IntegrationMode = value
	}
	if value := projectPatchQueueReceiptPayloadString(payload, "source_branch_id"); value != "" {
		input.SourceBranchID = value
	}
	if value := projectPatchQueueReceiptPayloadString(payload, "source_head_sha"); value != "" {
		input.SourceHeadSHA = value
	}
	return input
}

func projectPatchQueueReceiptPayloadString(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func projectPatchQueueIntegrationDedupKey(workspaceID, queueID, itemID, outcome, targetBranch, targetHeadAfter, repairReason string) string {
	reasonHash := ""
	if reason := strings.TrimSpace(repairReason); reason != "" {
		sum := sha256.Sum256([]byte(reason))
		reasonHash = ":" + fmt.Sprintf("%x", sum)[:16]
	}
	return "project.patch_queue.integration:" +
		strings.TrimSpace(workspaceID) + ":" +
		strings.TrimSpace(queueID) + "/" + strings.TrimSpace(itemID) + ":" +
		strings.TrimSpace(outcome) + ":" +
		canonicalProjectPatchQueueIntegrationTargetBranch(targetBranch) + ":" +
		strings.TrimSpace(targetHeadAfter) +
		reasonHash
}

func projectPatchQueueIntegrationTerminalizationDedupKey(workspaceID, queueID, itemID, targetBranch, targetHeadAfter string) string {
	return "project.patch_queue.integration_terminalized:" +
		strings.TrimSpace(workspaceID) + ":" +
		strings.TrimSpace(queueID) + "/" + strings.TrimSpace(itemID) + ":" +
		canonicalProjectPatchQueueIntegrationTargetBranch(targetBranch) + ":" +
		strings.TrimSpace(targetHeadAfter)
}

func projectPatchQueueStateIsLive(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed:
		return true
	default:
		return false
	}
}

func normalizeProjectPatchQueueAttempt(attempt int) int {
	if attempt <= 0 {
		return 1
	}
	return attempt
}

func normalizeProjectPatchQueueMaxAttempts(maxAttempts int) int {
	if maxAttempts <= 0 {
		return 1
	}
	return maxAttempts
}

func validateProjectPatchQueueRetryContract(item ProjectPatchQueueItemRecord) error {
	attempt := normalizeProjectPatchQueueAttempt(item.Attempt)
	maxAttempts := normalizeProjectPatchQueueMaxAttempts(item.MaxAttempts)
	if item.Attempt != attempt || item.MaxAttempts != maxAttempts {
		return fmt.Errorf("%w: patch queue retry contract must be canonical", ErrProjectPatchQueueInvalid)
	}
	if attempt > maxAttempts {
		return fmt.Errorf("%w: patch queue attempt %d exceeds max_attempts %d", ErrProjectPatchQueueInvalid, attempt, maxAttempts)
	}
	if strings.TrimSpace(item.NextRetryAt) != "" {
		if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.NextRetryAt)); err != nil {
			return fmt.Errorf("%w: next_retry_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	if strings.TrimSpace(item.DeadLetteredAt) != "" {
		if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.DeadLetteredAt)); err != nil {
			return fmt.Errorf("%w: dead_lettered_at must be RFC3339Nano: %v", ErrProjectPatchQueueInvalid, err)
		}
	}
	return nil
}

func requireProjectPatchQueueClaimOwner(item ProjectPatchQueueItemRecord, actorID, actorType, token string) error {
	if strings.EqualFold(strings.TrimSpace(actorType), "agent") && strings.TrimSpace(item.ClaimedBy) != strings.TrimSpace(actorID) {
		return fmt.Errorf("%w: patch queue item %s/%s is claimed by %s", ErrProjectPatchQueueInvalid, item.QueueID, item.ItemID, strings.TrimSpace(item.ClaimedBy))
	}
	return requireProjectPatchQueueClaimToken(item, token)
}

func requireProjectPatchQueueClaimToken(item ProjectPatchQueueItemRecord, token string) error {
	if strings.TrimSpace(item.ClaimToken) != "" && strings.TrimSpace(token) != strings.TrimSpace(item.ClaimToken) {
		return fmt.Errorf("%w: claim_token does not match patch queue claim", ErrProjectPatchQueueInvalid)
	}
	return nil
}

func normalizeProjectPatchQueueDecision(value string) (string, error) {
	decision := strings.ToUpper(strings.TrimSpace(value))
	decision = strings.ReplaceAll(decision, "-", "_")
	decision = strings.ReplaceAll(decision, " ", "_")
	switch decision {
	case "ACCEPT", "APPROVE", "APPROVED":
		decision = ProjectPatchQueueStateAccepted
	case "REJECT":
		decision = ProjectPatchQueueStateRejected
	case "BLOCK":
		decision = ProjectPatchQueueStateBlocked
	case "CANCEL":
		decision = ProjectPatchQueueStateCanceled
	}
	switch decision {
	case ProjectPatchQueueStateAccepted, ProjectPatchQueueStateRejected, ProjectPatchQueueStateBlocked, ProjectPatchQueueStateCanceled:
		return decision, nil
	default:
		return "", fmt.Errorf("%w: invalid patch queue decision %q", ErrProjectPatchQueueInvalid, value)
	}
}

func normalizeProjectPatchQueueAuthorityMode(value string) (string, error) {
	mode := strings.TrimSpace(value)
	if mode == "" {
		return ProjectPatchQueueAuthorityModePatchOnly, nil
	}
	switch mode {
	case ProjectPatchQueueAuthorityModePatchOnly, ProjectPatchQueueAuthorityModeControlledQueue:
		return mode, nil
	default:
		return "", fmt.Errorf("%w: unsupported repo_authority_mode %s", ErrProjectPatchQueueInvalid, mode)
	}
}

func defaultProjectPatchQueueID(projectID, repoID string) string {
	return "patchq-" + projectPatchQueueKeyPart(projectID) + "-" + projectPatchQueueKeyPart(repoID)
}

func defaultProjectPatchQueueItemID(branchID string) string {
	return "patchitem-" + projectPatchQueueKeyPart(branchID)
}

func projectPatchQueueKeyPart(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "unknown"
	}
	return out
}

func projectPatchQueueEventPayload(item ProjectPatchQueueItemRecord, actorID, operation string) map[string]any {
	eventItem := RedactProjectPatchQueueItemClaimToken(item)
	return map[string]any{
		"workspace_id":          item.WorkspaceID,
		"project_id":            item.ProjectID,
		"repo_id":               item.RepoID,
		"branch_id":             item.BranchID,
		"queue_id":              item.QueueID,
		"item_id":               item.ItemID,
		"review_doc_key":        item.ReviewDocKey,
		"supersedes_queue_id":   item.SupersedesQueueID,
		"supersedes_item_id":    item.SupersedesItemID,
		"evidence_doc_key":      item.EvidenceDocKey,
		"repo_authority_mode":   item.RepoAuthorityMode,
		"state":                 item.State,
		"auto_merge":            item.AutoMerge,
		"actor_id":              strings.TrimSpace(actorID),
		"entity_type":           "project_patch_queue_item",
		"entity_id":             item.QueueID + "/" + item.ItemID,
		"summary":               "Project patch queue candidate submitted: " + item.QueueID + "/" + item.ItemID,
		"mutation_operation":    operation,
		"patch_queue_candidate": eventItem,
	}
}

// RedactProjectPatchQueueItemClaimToken returns a presentation-safe copy of a
// patch queue item. The original record keeps its claim token so internal
// fencing checks can continue to use it, while read-side receipts and durable
// event payloads cannot disclose the bearer credential.
func RedactProjectPatchQueueItemClaimToken(item ProjectPatchQueueItemRecord) ProjectPatchQueueItemRecord {
	item.ClaimToken = ""
	return item
}

func projectPatchQueueRedactMaterializationContent(item ProjectPatchQueueItemRecord) ProjectPatchQueueItemRecord {
	if !projectPatchQueueMaterializationPresent(item) {
		return item
	}
	item.MaterializationJSON = ""
	if len(item.Materialization.Files) > 0 {
		item.Materialization.Files = append([]repoauthority.PatchMaterializedFile(nil), item.Materialization.Files...)
	}
	for i := range item.Materialization.Files {
		item.Materialization.Files[i].Content = ""
	}
	return item
}
