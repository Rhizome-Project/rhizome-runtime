package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	ConflictModelSchemaVersion = "repo_conflict_model.v1"

	ConflictStatusConflict = "conflict"

	ConflictReasonPathConflict   = "path_conflict"
	ConflictReasonBaseDrift      = "base_drift"
	ConflictReasonTestConflict   = "test_conflict"
	ConflictReasonLateStaleWrite = "late_stale_write"
	ConflictReasonSemantic       = "semantic_conflict"

	SemanticConflictKindAPIContract = "api_contract_collision"
	SemanticConflictKindTestFixture = "test_fixture_collision"
	SemanticConflictKindBehavior    = "behavior_collision"
	SemanticConflictKindPlaceholder = "semantic_collision"

	SemanticConflictEvidenceSummaryMaxBytes = 2048

	LateStaleWriteRejectionExpiredLease   = "expired_lease"
	LateStaleWriteRejectionReleasedLease  = "released_lease"
	LateStaleWriteRejectionRevokedLease   = "revoked_lease"
	LateStaleWriteRejectionStaleLeaseTerm = "stale_lease_term"
	LateStaleWriteRejectionStaleHolder    = "stale_holder"
)

type ConflictModelResult struct {
	Schema                   string                      `json:"schema"`
	ConflictID               string                      `json:"conflict_id"`
	Status                   string                      `json:"status"`
	Reason                   string                      `json:"reason"`
	Path                     string                      `json:"path"`
	Paths                    []string                    `json:"paths,omitempty"`
	WorkspaceID              string                      `json:"workspace_id"`
	TaskID                   string                      `json:"task_id,omitempty"`
	SessionID                string                      `json:"session_id,omitempty"`
	RunID                    string                      `json:"run_id,omitempty"`
	AgentID                  string                      `json:"agent_id,omitempty"`
	PrincipalType            string                      `json:"principal_type,omitempty"`
	PrincipalID              string                      `json:"principal_id,omitempty"`
	CapabilitySnapshotID     string                      `json:"capability_snapshot_id,omitempty"`
	CapabilitySnapshotSchema string                      `json:"capability_snapshot_schema,omitempty"`
	ContextDigest            string                      `json:"context_digest,omitempty"`
	RepoLeaseID              string                      `json:"repo_lease_id,omitempty"`
	LeaseTerm                int64                       `json:"lease_term,omitempty"`
	PatchQueueID             string                      `json:"patch_queue_id,omitempty"`
	PatchQueueItemID         string                      `json:"patch_queue_item_id,omitempty"`
	PatchQueueState          string                      `json:"patch_queue_state,omitempty"`
	BaseRef                  string                      `json:"base_ref,omitempty"`
	BaseTreeHash             string                      `json:"base_tree_hash,omitempty"`
	ExpectedHash             string                      `json:"expected_hash,omitempty"`
	ActualHash               string                      `json:"actual_hash,omitempty"`
	CandidateHash            string                      `json:"candidate_hash,omitempty"`
	CASStatus                string                      `json:"cas_status,omitempty"`
	CASPatchDigest           string                      `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest      string                      `json:"cas_evaluation_digest,omitempty"`
	CASIssueKind             string                      `json:"cas_issue_kind,omitempty"`
	TestEvidence             *PatchQueueTestEvidence     `json:"test_evidence,omitempty"`
	TestEvidenceDigest       string                      `json:"test_evidence_digest,omitempty"`
	OperationID              string                      `json:"operation_id,omitempty"`
	OperationKind            string                      `json:"operation_kind,omitempty"`
	MutationPaths            []string                    `json:"mutation_paths,omitempty"`
	AttemptedAt              string                      `json:"attempted_at,omitempty"`
	RejectionKind            string                      `json:"rejection_kind,omitempty"`
	RejectionMessage         string                      `json:"rejection_message,omitempty"`
	AttemptedHolder          *FileLeaseHolder            `json:"attempted_holder,omitempty"`
	AttemptedHolderDigest    string                      `json:"attempted_holder_digest,omitempty"`
	ObservedLease            *FileLease                  `json:"observed_lease,omitempty"`
	ObservedLeaseDigest      string                      `json:"observed_lease_digest,omitempty"`
	ObservedLeaseStatus      string                      `json:"observed_lease_status,omitempty"`
	ObservedLeaseTerm        int64                       `json:"observed_lease_term,omitempty"`
	ObservedHolder           *FileLeaseHolder            `json:"observed_holder,omitempty"`
	ObservedHolderDigest     string                      `json:"observed_holder_digest,omitempty"`
	SemanticKind             string                      `json:"semantic_kind,omitempty"`
	SemanticSubject          string                      `json:"semantic_subject,omitempty"`
	SemanticEvidenceSummary  string                      `json:"semantic_evidence_summary,omitempty"`
	SemanticEvidenceDigest   string                      `json:"semantic_evidence_digest,omitempty"`
	SemanticContenders       []SemanticConflictContender `json:"semantic_contenders,omitempty"`
	Contenders               []ConflictContender         `json:"contenders,omitempty"`
}

type ConflictContender struct {
	WorkspaceID         string `json:"workspace_id"`
	QueueID             string `json:"queue_id"`
	ItemID              string `json:"item_id"`
	State               string `json:"state"`
	ContextDigest       string `json:"context_digest"`
	RepoLeaseID         string `json:"repo_lease_id"`
	LeaseTerm           int64  `json:"lease_term"`
	TaskID              string `json:"task_id,omitempty"`
	SessionID           string `json:"session_id,omitempty"`
	RunID               string `json:"run_id,omitempty"`
	AgentID             string `json:"agent_id,omitempty"`
	BaseRef             string `json:"base_ref,omitempty"`
	BaseTreeHash        string `json:"base_tree_hash,omitempty"`
	CASPatchDigest      string `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest string `json:"cas_evaluation_digest,omitempty"`
}

type SemanticConflictContender struct {
	WorkspaceID              string   `json:"workspace_id"`
	QueueID                  string   `json:"queue_id"`
	ItemID                   string   `json:"item_id"`
	State                    string   `json:"state"`
	ContextDigest            string   `json:"context_digest"`
	RepoLeaseID              string   `json:"repo_lease_id"`
	LeaseTerm                int64    `json:"lease_term"`
	Pathset                  []string `json:"pathset"`
	TaskID                   string   `json:"task_id"`
	SessionID                string   `json:"session_id"`
	RunID                    string   `json:"run_id"`
	AgentID                  string   `json:"agent_id"`
	PrincipalType            string   `json:"principal_type"`
	PrincipalID              string   `json:"principal_id"`
	CapabilitySnapshotID     string   `json:"capability_snapshot_id"`
	CapabilitySnapshotSchema string   `json:"capability_snapshot_schema,omitempty"`
	BaseRef                  string   `json:"base_ref"`
	BaseTreeHash             string   `json:"base_tree_hash"`
	CASPatchDigest           string   `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest      string   `json:"cas_evaluation_digest,omitempty"`
}

type BuildBaseDriftConflictInput struct {
	Context        Context
	CASResult      CASPatchApplyResult
	PatchQueueItem PatchQueueItem
	Path           string
}

type BuildPathConflictInput struct {
	Path  string
	Left  PatchQueueItem
	Right PatchQueueItem
}

type BuildTestConflictInput struct {
	Context        Context
	PatchQueueItem PatchQueueItem
	Path           string
}

type BuildLateStaleWriteConflictInput struct {
	Context       Context
	LeaseStore    *FileLeaseStore
	MutationPaths []string
	Now           time.Time
	Path          string
}

type BuildSemanticConflictInput struct {
	Kind            string
	Subject         string
	EvidenceSummary string
	Left            PatchQueueItem
	Right           PatchQueueItem
	PatchQueueStore *PatchQueueStore
	Path            string
}

func BuildBaseDriftConflict(input BuildBaseDriftConflictInput) (ConflictModelResult, error) {
	authority := input.Context.WithDefaults()
	if err := validateCASEvidence(input.CASResult, authority); err != nil {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict CAS evidence: %w", err)
	}
	if input.CASResult.Status != CASPatchStatusConflict {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict requires CAS status %q", CASPatchStatusConflict)
	}
	if strings.TrimSpace(input.PatchQueueItem.ID) == "" {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict requires patch queue item evidence")
	}
	if input.PatchQueueItem.State != PatchQueueStateConflict {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict requires patch queue state %q", PatchQueueStateConflict)
	}
	if strings.TrimSpace(input.PatchQueueItem.CASPatchDigest) == "" {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict requires patch queue CAS patch digest evidence")
	}
	if strings.TrimSpace(input.PatchQueueItem.CASPatchDigest) != strings.TrimSpace(input.CASResult.PatchDigest) {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict CAS patch digest mismatch")
	}
	evaluationDigest := digestCASPatchApplyResult(input.CASResult)
	if strings.TrimSpace(input.PatchQueueItem.CASEvaluationDigest) == "" {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict requires patch queue CAS evaluation digest evidence")
	}
	if strings.TrimSpace(input.PatchQueueItem.CASEvaluationDigest) != evaluationDigest {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict CAS evaluation digest mismatch")
	}
	if err := verifyPatchQueueItemContext(input.PatchQueueItem, authority); err != nil {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict patch queue context: %w", err)
	}

	path, err := chooseBaseDriftPath(input.CASResult, input.Path)
	if err != nil {
		return ConflictModelResult{}, err
	}
	issue, pathResult, err := baseDriftIssueAndPathResult(input.CASResult, path)
	if err != nil {
		return ConflictModelResult{}, err
	}
	contextDigest, err := authority.Digest()
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("base_drift conflict context digest: %w", err)
	}
	result := ConflictModelResult{
		Schema:                   ConflictModelSchemaVersion,
		Status:                   ConflictStatusConflict,
		Reason:                   ConflictReasonBaseDrift,
		Path:                     path,
		WorkspaceID:              strings.TrimSpace(authority.WorkspaceID),
		TaskID:                   strings.TrimSpace(authority.TaskID),
		SessionID:                strings.TrimSpace(authority.SessionID),
		RunID:                    strings.TrimSpace(authority.RunID),
		AgentID:                  strings.TrimSpace(authority.AgentID),
		PrincipalType:            strings.TrimSpace(authority.Principal.Type),
		PrincipalID:              strings.TrimSpace(authority.Principal.ID),
		CapabilitySnapshotID:     strings.TrimSpace(authority.CapabilitySnapshot.ID),
		CapabilitySnapshotSchema: strings.TrimSpace(authority.CapabilitySnapshot.Schema),
		ContextDigest:            contextDigest,
		RepoLeaseID:              strings.TrimSpace(authority.Lease.ID),
		LeaseTerm:                authority.Lease.Term,
		PatchQueueID:             strings.TrimSpace(authority.PatchQueue.QueueID),
		PatchQueueItemID:         strings.TrimSpace(authority.PatchQueue.ItemID),
		PatchQueueState:          strings.TrimSpace(input.PatchQueueItem.State),
		BaseRef:                  strings.TrimSpace(authority.Base.Ref),
		BaseTreeHash:             strings.TrimSpace(authority.Base.TreeHash),
		ExpectedHash:             strings.TrimSpace(issue.ExpectedHash),
		ActualHash:               strings.TrimSpace(issue.ActualHash),
		CandidateHash:            strings.TrimSpace(issue.CandidateHash),
		CASPatchDigest:           strings.TrimSpace(input.CASResult.PatchDigest),
		CASEvaluationDigest:      evaluationDigest,
		CASIssueKind:             CASPatchIssueBaseDrift,
	}
	if result.ExpectedHash == "" {
		result.ExpectedHash = strings.TrimSpace(pathResult.BaseHash)
	}
	if result.ActualHash == "" {
		result.ActualHash = strings.TrimSpace(pathResult.CurrentHash)
	}
	if result.CandidateHash == "" {
		result.CandidateHash = strings.TrimSpace(pathResult.CandidateHash)
	}
	result.ConflictID = conflictModelID(result)
	if err := ValidateConflictModelResult(result); err != nil {
		return ConflictModelResult{}, err
	}
	return result, nil
}

func BuildPathConflict(input BuildPathConflictInput) (ConflictModelResult, error) {
	path, err := NormalizePath(input.Path)
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("path_conflict path: %w", err)
	}
	left, err := conflictContenderFromPatchQueueItem(input.Left, path, "left")
	if err != nil {
		return ConflictModelResult{}, err
	}
	right, err := conflictContenderFromPatchQueueItem(input.Right, path, "right")
	if err != nil {
		return ConflictModelResult{}, err
	}
	if left.WorkspaceID != right.WorkspaceID {
		return ConflictModelResult{}, fmt.Errorf("path_conflict contenders must be in the same workspace")
	}
	if left.QueueID == right.QueueID && left.ItemID == right.ItemID {
		return ConflictModelResult{}, fmt.Errorf("path_conflict contenders must be different patch queue items")
	}
	contenders := []ConflictContender{left, right}
	sort.Slice(contenders, func(i, j int) bool {
		if contenders[i].QueueID != contenders[j].QueueID {
			return contenders[i].QueueID < contenders[j].QueueID
		}
		return contenders[i].ItemID < contenders[j].ItemID
	})
	result := ConflictModelResult{
		Schema:      ConflictModelSchemaVersion,
		Status:      ConflictStatusConflict,
		Reason:      ConflictReasonPathConflict,
		Path:        path,
		WorkspaceID: left.WorkspaceID,
		Contenders:  contenders,
	}
	result.ConflictID = conflictModelID(result)
	if err := ValidateConflictModelResult(result); err != nil {
		return ConflictModelResult{}, err
	}
	return result, nil
}

func BuildTestConflict(input BuildTestConflictInput) (ConflictModelResult, error) {
	authority := input.Context.WithDefaults()
	if strings.TrimSpace(input.PatchQueueItem.ID) == "" {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires patch queue item evidence")
	}
	if input.PatchQueueItem.State != PatchQueueStateTestConflict {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires patch queue state %q", PatchQueueStateTestConflict)
	}
	if input.PatchQueueItem.OperationID != "" || input.PatchQueueItem.OperationKind != "" {
		return ConflictModelResult{}, fmt.Errorf("test_conflict must not include canonical mutation operation refs")
	}
	if authority.Operation.ID != "" || authority.Operation.Kind != "" {
		return ConflictModelResult{}, fmt.Errorf("test_conflict context must not include canonical mutation operation refs")
	}
	if input.PatchQueueItem.CASResult.Status != CASPatchStatusApplied {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires applied CAS result, got %q", input.PatchQueueItem.CASResult.Status)
	}
	if err := validateCASEvidence(input.PatchQueueItem.CASResult, authority); err != nil {
		return ConflictModelResult{}, fmt.Errorf("test_conflict CAS evidence: %w", err)
	}
	if strings.TrimSpace(input.PatchQueueItem.CASPatchDigest) == "" {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires patch queue CAS patch digest evidence")
	}
	if strings.TrimSpace(input.PatchQueueItem.CASPatchDigest) != strings.TrimSpace(input.PatchQueueItem.CASResult.PatchDigest) {
		return ConflictModelResult{}, fmt.Errorf("test_conflict CAS patch digest mismatch")
	}
	evaluationDigest := digestCASPatchApplyResult(input.PatchQueueItem.CASResult)
	if strings.TrimSpace(input.PatchQueueItem.CASEvaluationDigest) == "" {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires patch queue CAS evaluation digest evidence")
	}
	if strings.TrimSpace(input.PatchQueueItem.CASEvaluationDigest) != evaluationDigest {
		return ConflictModelResult{}, fmt.Errorf("test_conflict CAS evaluation digest mismatch")
	}
	if err := verifyPatchQueueItemContext(input.PatchQueueItem, authority); err != nil {
		return ConflictModelResult{}, fmt.Errorf("test_conflict patch queue context: %w", err)
	}
	testEvidence, hasTestEvidence, err := normalizePatchQueueTestEvidence(input.PatchQueueItem.TestEvidence)
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("test_conflict test evidence: %w", err)
	}
	if !hasTestEvidence {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires test evidence")
	}
	if testEvidence.Status != PatchQueueTestStatusFailed {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires failed test evidence")
	}
	if strings.TrimSpace(input.PatchQueueItem.TestEvidenceDigest) == "" {
		return ConflictModelResult{}, fmt.Errorf("test_conflict requires test evidence digest")
	}
	if strings.TrimSpace(input.PatchQueueItem.TestEvidenceDigest) != digestPatchQueueTestEvidence(testEvidence) {
		return ConflictModelResult{}, fmt.Errorf("test_conflict test evidence digest mismatch")
	}
	paths, path, err := chooseTestConflictPaths(input.PatchQueueItem.Pathset, input.Path)
	if err != nil {
		return ConflictModelResult{}, err
	}
	contextDigest, err := authority.Digest()
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("test_conflict context digest: %w", err)
	}
	result := ConflictModelResult{
		Schema:                   ConflictModelSchemaVersion,
		Status:                   ConflictStatusConflict,
		Reason:                   ConflictReasonTestConflict,
		Path:                     path,
		Paths:                    paths,
		WorkspaceID:              strings.TrimSpace(authority.WorkspaceID),
		TaskID:                   strings.TrimSpace(authority.TaskID),
		SessionID:                strings.TrimSpace(authority.SessionID),
		RunID:                    strings.TrimSpace(authority.RunID),
		AgentID:                  strings.TrimSpace(authority.AgentID),
		PrincipalType:            strings.TrimSpace(authority.Principal.Type),
		PrincipalID:              strings.TrimSpace(authority.Principal.ID),
		CapabilitySnapshotID:     strings.TrimSpace(authority.CapabilitySnapshot.ID),
		CapabilitySnapshotSchema: strings.TrimSpace(authority.CapabilitySnapshot.Schema),
		ContextDigest:            contextDigest,
		RepoLeaseID:              strings.TrimSpace(authority.Lease.ID),
		LeaseTerm:                authority.Lease.Term,
		PatchQueueID:             strings.TrimSpace(authority.PatchQueue.QueueID),
		PatchQueueItemID:         strings.TrimSpace(authority.PatchQueue.ItemID),
		PatchQueueState:          strings.TrimSpace(input.PatchQueueItem.State),
		BaseRef:                  strings.TrimSpace(authority.Base.Ref),
		BaseTreeHash:             strings.TrimSpace(authority.Base.TreeHash),
		CASStatus:                strings.TrimSpace(input.PatchQueueItem.CASResult.Status),
		CASPatchDigest:           strings.TrimSpace(input.PatchQueueItem.CASPatchDigest),
		CASEvaluationDigest:      evaluationDigest,
		TestEvidence:             &testEvidence,
		TestEvidenceDigest:       strings.TrimSpace(input.PatchQueueItem.TestEvidenceDigest),
		OperationID:              strings.TrimSpace(input.PatchQueueItem.OperationID),
		OperationKind:            strings.TrimSpace(input.PatchQueueItem.OperationKind),
	}
	result.ConflictID = conflictModelID(result)
	if err := ValidateConflictModelResult(result); err != nil {
		return ConflictModelResult{}, err
	}
	return result, nil
}

func BuildLateStaleWriteConflict(input BuildLateStaleWriteConflictInput) (ConflictModelResult, error) {
	if input.LeaseStore == nil {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write requires file lease store")
	}
	authority := input.Context.WithDefaults()
	if err := requireConcreteMutationOperationRefs(authority); err != nil {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write operation refs: %w", err)
	}
	contextDigest, err := authority.Digest()
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write context digest: %w", err)
	}
	lease, err := findBindingLease(input.LeaseStore, authority.Lease.ID)
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write requires existing lease evidence: %w", err)
	}
	mutationPaths := input.MutationPaths
	if len(mutationPaths) == 0 {
		mutationPaths = authority.Pathset
	}
	normalizedPaths, err := NormalizePathSet(mutationPaths)
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write mutation paths: %w", err)
	}
	if len(normalizedPaths) == 0 {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write mutation paths are required")
	}
	paths, path, err := chooseLateStaleWritePaths(normalizedPaths, input.Path)
	if err != nil {
		return ConflictModelResult{}, err
	}
	attemptedHolder, err := fileLeaseHolderFromContext(authority)
	if err != nil {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write attempted holder: %w", err)
	}

	_, bindErr := BindMutationOperation(MutationOperationBindingInput{
		Context:       authority,
		LeaseStore:    input.LeaseStore,
		MutationPaths: normalizedPaths,
		Now:           input.Now,
	})
	if bindErr == nil {
		return ConflictModelResult{}, fmt.Errorf("late_stale_write requires rejected mutation operation binding")
	}
	observedLease, err := findBindingLease(input.LeaseStore, authority.Lease.ID)
	if err != nil {
		observedLease = lease
	}
	rejectionKind, err := classifyLateStaleWriteRejection(bindErr, authority, observedLease, attemptedHolder)
	if err != nil {
		return ConflictModelResult{}, err
	}
	observedHolder := HolderForLease(observedLease)
	observedLeaseEvidence := observedLease
	result := ConflictModelResult{
		Schema:                   ConflictModelSchemaVersion,
		Status:                   ConflictStatusConflict,
		Reason:                   ConflictReasonLateStaleWrite,
		Path:                     path,
		Paths:                    paths,
		WorkspaceID:              authority.WorkspaceID,
		TaskID:                   authority.TaskID,
		SessionID:                authority.SessionID,
		RunID:                    authority.RunID,
		AgentID:                  authority.AgentID,
		PrincipalType:            authority.Principal.Type,
		PrincipalID:              authority.Principal.ID,
		CapabilitySnapshotID:     authority.CapabilitySnapshot.ID,
		CapabilitySnapshotSchema: authority.CapabilitySnapshot.Schema,
		ContextDigest:            contextDigest,
		RepoLeaseID:              authority.Lease.ID,
		LeaseTerm:                authority.Lease.Term,
		PatchQueueID:             authority.PatchQueue.QueueID,
		PatchQueueItemID:         authority.PatchQueue.ItemID,
		BaseRef:                  authority.Base.Ref,
		BaseTreeHash:             authority.Base.TreeHash,
		OperationID:              authority.Operation.ID,
		OperationKind:            authority.Operation.Kind,
		MutationPaths:            append([]string(nil), normalizedPaths...),
		AttemptedAt:              formatLeaseTime(normalizeLeaseTime(input.Now)),
		RejectionKind:            rejectionKind,
		RejectionMessage:         bindErr.Error(),
		AttemptedHolder:          &attemptedHolder,
		AttemptedHolderDigest:    digestFileLeaseHolder(attemptedHolder),
		ObservedLease:            &observedLeaseEvidence,
		ObservedLeaseDigest:      digestFileLease(observedLeaseEvidence),
		ObservedLeaseStatus:      observedLease.Status,
		ObservedLeaseTerm:        observedLease.Term,
		ObservedHolder:           &observedHolder,
		ObservedHolderDigest:     digestFileLeaseHolder(observedHolder),
	}
	result.ConflictID = conflictModelID(result)
	if err := ValidateConflictModelResultWithLeaseStore(result, input.LeaseStore); err != nil {
		return ConflictModelResult{}, err
	}
	return result, nil
}

func BuildSemanticConflict(input BuildSemanticConflictInput) (ConflictModelResult, error) {
	if input.PatchQueueStore == nil {
		return ConflictModelResult{}, fmt.Errorf("semantic_conflict requires patch queue store")
	}
	kind, subject, summary, err := normalizeSemanticConflictEvidence(input.Kind, input.Subject, input.EvidenceSummary)
	if err != nil {
		return ConflictModelResult{}, err
	}
	left, err := semanticContenderFromPatchQueueItem(input.Left, "left")
	if err != nil {
		return ConflictModelResult{}, err
	}
	right, err := semanticContenderFromPatchQueueItem(input.Right, "right")
	if err != nil {
		return ConflictModelResult{}, err
	}
	if left.WorkspaceID != right.WorkspaceID {
		return ConflictModelResult{}, fmt.Errorf("semantic_conflict contenders must be in the same workspace")
	}
	if left.QueueID != right.QueueID {
		return ConflictModelResult{}, fmt.Errorf("semantic_conflict contenders must be in the same patch queue")
	}
	if left.BaseRef != right.BaseRef || left.BaseTreeHash != right.BaseTreeHash {
		return ConflictModelResult{}, fmt.Errorf("semantic_conflict contenders must share the same base identity")
	}
	if left.QueueID == right.QueueID && left.ItemID == right.ItemID {
		return ConflictModelResult{}, fmt.Errorf("semantic_conflict contenders must be different patch queue items")
	}
	if pathsetsOverlap(left.Pathset, right.Pathset) {
		return ConflictModelResult{}, fmt.Errorf("semantic_conflict contenders share a path; use path_conflict")
	}
	contenders := []SemanticConflictContender{left, right}
	sort.Slice(contenders, func(i, j int) bool {
		if contenders[i].QueueID != contenders[j].QueueID {
			return contenders[i].QueueID < contenders[j].QueueID
		}
		return contenders[i].ItemID < contenders[j].ItemID
	})
	paths := mergeSemanticContenderPaths(contenders)
	path, err := chooseSemanticConflictPath(paths, input.Path)
	if err != nil {
		return ConflictModelResult{}, err
	}
	result := ConflictModelResult{
		Schema:                  ConflictModelSchemaVersion,
		Status:                  ConflictStatusConflict,
		Reason:                  ConflictReasonSemantic,
		Path:                    path,
		Paths:                   paths,
		WorkspaceID:             left.WorkspaceID,
		SemanticKind:            kind,
		SemanticSubject:         subject,
		SemanticEvidenceSummary: summary,
		SemanticContenders:      contenders,
	}
	result.SemanticEvidenceDigest = digestSemanticConflictEvidence(result)
	result.ConflictID = conflictModelID(result)
	if err := ValidateConflictModelResultWithPatchQueueStore(result, input.PatchQueueStore); err != nil {
		return ConflictModelResult{}, err
	}
	return result, nil
}

func ValidateConflictModelResult(result ConflictModelResult) error {
	return validateConflictModelResult(result, nil, nil)
}

func ValidateConflictModelResultWithLeaseStore(result ConflictModelResult, leaseStore *FileLeaseStore) error {
	return validateConflictModelResult(result, leaseStore, nil)
}

func ValidateConflictModelResultWithPatchQueueStore(result ConflictModelResult, patchQueueStore *PatchQueueStore) error {
	return validateConflictModelResult(result, nil, patchQueueStore)
}

func validateConflictModelResult(result ConflictModelResult, leaseStore *FileLeaseStore, patchQueueStore *PatchQueueStore) error {
	if result.Schema != ConflictModelSchemaVersion {
		return fmt.Errorf("conflict schema is required")
	}
	if strings.TrimSpace(result.ConflictID) == "" {
		return fmt.Errorf("conflict_id is required")
	}
	if result.Status != ConflictStatusConflict {
		return fmt.Errorf("conflict status must be %q", ConflictStatusConflict)
	}
	if err := requireCanonicalConflictValue("conflict", "workspace_id", result.WorkspaceID); err != nil {
		return err
	}
	var err error
	switch result.Reason {
	case ConflictReasonBaseDrift:
		if err := validateConflictPath(result.Path); err != nil {
			return err
		}
		err = validateBaseDriftConflictModel(result)
	case ConflictReasonPathConflict:
		if err := validateConflictPath(result.Path); err != nil {
			return err
		}
		err = validatePathConflictModel(result)
	case ConflictReasonTestConflict:
		err = validateTestConflictModel(result)
	case ConflictReasonLateStaleWrite:
		if leaseStore == nil {
			return fmt.Errorf("late_stale_write requires lease store validation")
		}
		err = validateLateStaleWriteConflictModel(result, leaseStore)
	case ConflictReasonSemantic:
		if patchQueueStore == nil {
			return fmt.Errorf("semantic_conflict requires patch queue store validation")
		}
		err = validateSemanticConflictModel(result, patchQueueStore)
	default:
		return fmt.Errorf("conflict reason %q is not supported", result.Reason)
	}
	if err != nil {
		return err
	}
	if result.ConflictID != conflictModelID(result) {
		return fmt.Errorf("conflict_id mismatch")
	}
	return nil
}

func validateBaseDriftConflictModel(result ConflictModelResult) error {
	for field, value := range map[string]string{
		"context_digest":        result.ContextDigest,
		"repo_lease_id":         result.RepoLeaseID,
		"patch_queue_id":        result.PatchQueueID,
		"patch_queue_item_id":   result.PatchQueueItemID,
		"patch_queue_state":     result.PatchQueueState,
		"expected_hash":         result.ExpectedHash,
		"actual_hash":           result.ActualHash,
		"candidate_hash":        result.CandidateHash,
		"cas_patch_digest":      result.CASPatchDigest,
		"cas_evaluation_digest": result.CASEvaluationDigest,
		"cas_issue_kind":        result.CASIssueKind,
	} {
		if err := requireCanonicalConflictValue("base_drift conflict", field, value); err != nil {
			return err
		}
	}
	if result.LeaseTerm <= 0 {
		return fmt.Errorf("base_drift conflict lease_term is required")
	}
	if result.CASIssueKind != CASPatchIssueBaseDrift {
		return fmt.Errorf("base_drift conflict cas_issue_kind must be %q", CASPatchIssueBaseDrift)
	}
	if result.PatchQueueState != PatchQueueStateConflict {
		return fmt.Errorf("base_drift conflict patch_queue_state must be %q", PatchQueueStateConflict)
	}
	if strings.TrimSpace(result.ExpectedHash) == strings.TrimSpace(result.ActualHash) {
		return fmt.Errorf("base_drift conflict expected_hash and actual_hash must differ")
	}
	if len(result.Contenders) != 0 {
		return fmt.Errorf("base_drift conflict must not include path_conflict contenders")
	}
	return nil
}

func validatePathConflictModel(result ConflictModelResult) error {
	if len(result.Contenders) != 2 {
		return fmt.Errorf("path_conflict requires exactly two contenders")
	}
	seen := map[string]struct{}{}
	for i, contender := range result.Contenders {
		if err := validateConflictContender(contender, result.WorkspaceID, i); err != nil {
			return err
		}
		key := contender.QueueID + "\x00" + contender.ItemID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("path_conflict contender %s/%s is duplicated", contender.QueueID, contender.ItemID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateTestConflictModel(result ConflictModelResult) error {
	if err := validateConflictPath(result.Path); err != nil {
		return err
	}
	paths, err := NormalizePathSet(result.Paths)
	if err != nil {
		return fmt.Errorf("test_conflict paths: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("test_conflict paths are required")
	}
	if !sameStringSlice(paths, result.Paths) {
		return fmt.Errorf("test_conflict paths are not normalized")
	}
	if !stringSliceContainsPath(paths, result.Path) {
		return fmt.Errorf("test_conflict path %q must be included in paths", result.Path)
	}
	for field, value := range map[string]string{
		"task_id":                    result.TaskID,
		"session_id":                 result.SessionID,
		"run_id":                     result.RunID,
		"agent_id":                   result.AgentID,
		"principal_type":             result.PrincipalType,
		"principal_id":               result.PrincipalID,
		"capability_snapshot_id":     result.CapabilitySnapshotID,
		"capability_snapshot_schema": result.CapabilitySnapshotSchema,
		"context_digest":             result.ContextDigest,
		"repo_lease_id":              result.RepoLeaseID,
		"patch_queue_id":             result.PatchQueueID,
		"patch_queue_item_id":        result.PatchQueueItemID,
		"patch_queue_state":          result.PatchQueueState,
		"base_ref":                   result.BaseRef,
		"base_tree_hash":             result.BaseTreeHash,
		"cas_status":                 result.CASStatus,
		"cas_patch_digest":           result.CASPatchDigest,
		"cas_evaluation_digest":      result.CASEvaluationDigest,
		"test_evidence_digest":       result.TestEvidenceDigest,
	} {
		if err := requireCanonicalConflictValue("test_conflict", field, value); err != nil {
			return err
		}
	}
	if result.LeaseTerm <= 0 {
		return fmt.Errorf("test_conflict lease_term is required")
	}
	if result.PatchQueueState != PatchQueueStateTestConflict {
		return fmt.Errorf("test_conflict patch_queue_state must be %q", PatchQueueStateTestConflict)
	}
	if result.CASStatus != CASPatchStatusApplied {
		return fmt.Errorf("test_conflict cas_status must be %q", CASPatchStatusApplied)
	}
	if result.OperationID != "" || result.OperationKind != "" {
		return fmt.Errorf("test_conflict must not include canonical mutation operation refs")
	}
	if result.TestEvidence == nil {
		return fmt.Errorf("test_conflict test evidence is required")
	}
	testEvidence, hasTestEvidence, err := normalizePatchQueueTestEvidence(*result.TestEvidence)
	if err != nil {
		return fmt.Errorf("test_conflict test evidence: %w", err)
	}
	if !hasTestEvidence {
		return fmt.Errorf("test_conflict test evidence is required")
	}
	if *result.TestEvidence != testEvidence {
		return fmt.Errorf("test_conflict test evidence is not canonical")
	}
	if testEvidence.Status != PatchQueueTestStatusFailed {
		return fmt.Errorf("test_conflict requires failed test evidence")
	}
	if strings.TrimSpace(result.TestEvidenceDigest) != digestPatchQueueTestEvidence(testEvidence) {
		return fmt.Errorf("test_conflict test evidence digest mismatch")
	}
	if len(result.Contenders) != 0 {
		return fmt.Errorf("test_conflict must not include path_conflict contenders")
	}
	return nil
}

func validateLateStaleWriteConflictModel(result ConflictModelResult, leaseStore *FileLeaseStore) error {
	if err := validateConflictPath(result.Path); err != nil {
		return err
	}
	paths, err := NormalizePathSet(result.Paths)
	if err != nil {
		return fmt.Errorf("late_stale_write paths: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("late_stale_write paths are required")
	}
	if !sameStringSlice(paths, result.Paths) {
		return fmt.Errorf("late_stale_write paths are not canonical")
	}
	mutationPaths, err := NormalizePathSet(result.MutationPaths)
	if err != nil {
		return fmt.Errorf("late_stale_write mutation paths: %w", err)
	}
	if len(mutationPaths) == 0 {
		return fmt.Errorf("late_stale_write mutation paths are required")
	}
	if !sameStringSlice(mutationPaths, result.MutationPaths) {
		return fmt.Errorf("late_stale_write mutation paths are not canonical")
	}
	if !stringSliceContainsPath(paths, result.Path) {
		return fmt.Errorf("late_stale_write path %q must be included in paths", result.Path)
	}
	if !stringSliceContainsPath(mutationPaths, result.Path) {
		return fmt.Errorf("late_stale_write path %q must be included in mutation paths", result.Path)
	}
	for field, value := range map[string]string{
		"task_id":                    result.TaskID,
		"session_id":                 result.SessionID,
		"run_id":                     result.RunID,
		"agent_id":                   result.AgentID,
		"principal_type":             result.PrincipalType,
		"principal_id":               result.PrincipalID,
		"capability_snapshot_id":     result.CapabilitySnapshotID,
		"capability_snapshot_schema": result.CapabilitySnapshotSchema,
		"context_digest":             result.ContextDigest,
		"repo_lease_id":              result.RepoLeaseID,
		"patch_queue_id":             result.PatchQueueID,
		"patch_queue_item_id":        result.PatchQueueItemID,
		"base_ref":                   result.BaseRef,
		"base_tree_hash":             result.BaseTreeHash,
		"operation_id":               result.OperationID,
		"operation_kind":             result.OperationKind,
		"attempted_at":               result.AttemptedAt,
		"rejection_kind":             result.RejectionKind,
		"rejection_message":          result.RejectionMessage,
		"attempted_holder_digest":    result.AttemptedHolderDigest,
		"observed_lease_digest":      result.ObservedLeaseDigest,
		"observed_lease_status":      result.ObservedLeaseStatus,
		"observed_holder_digest":     result.ObservedHolderDigest,
	} {
		if err := requireCanonicalConflictValue("late_stale_write", field, value); err != nil {
			return err
		}
	}
	if result.LeaseTerm <= 0 {
		return fmt.Errorf("late_stale_write lease_term is required")
	}
	if result.ObservedLeaseTerm <= 0 {
		return fmt.Errorf("late_stale_write observed_lease_term is required")
	}
	if _, err := time.Parse(time.RFC3339Nano, result.AttemptedAt); err != nil {
		return fmt.Errorf("late_stale_write attempted_at must be RFC3339Nano")
	}
	if err := rejectVagueBindingLabel("repo_lease_id", result.RepoLeaseID); err != nil {
		return fmt.Errorf("late_stale_write %w", err)
	}
	if err := rejectVagueBindingLabel("patch_queue_id", result.PatchQueueID); err != nil {
		return fmt.Errorf("late_stale_write %w", err)
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", result.PatchQueueItemID); err != nil {
		return fmt.Errorf("late_stale_write %w", err)
	}
	if err := rejectVagueBindingLabel("operation_id", result.OperationID); err != nil {
		return fmt.Errorf("late_stale_write %w", err)
	}
	if err := rejectVagueBindingLabel("operation_kind", result.OperationKind); err != nil {
		return fmt.Errorf("late_stale_write %w", err)
	}
	if _, ok := allowedMutationOperationKinds[result.OperationKind]; !ok {
		return fmt.Errorf("late_stale_write operation_kind %q is not an accepted repo mutation kind", result.OperationKind)
	}
	if result.AttemptedHolder == nil {
		return fmt.Errorf("late_stale_write attempted_holder is required")
	}
	if result.ObservedLease == nil {
		return fmt.Errorf("late_stale_write observed_lease is required")
	}
	if result.ObservedHolder == nil {
		return fmt.Errorf("late_stale_write observed_holder is required")
	}
	if err := validateCanonicalFileLeaseHolder("attempted_holder", *result.AttemptedHolder); err != nil {
		return fmt.Errorf("late_stale_write attempted_holder: %w", err)
	}
	if err := validateCanonicalFileLeaseHolder("observed_holder", *result.ObservedHolder); err != nil {
		return fmt.Errorf("late_stale_write observed_holder: %w", err)
	}
	if result.AttemptedHolderDigest != digestFileLeaseHolder(*result.AttemptedHolder) {
		return fmt.Errorf("late_stale_write attempted_holder_digest mismatch")
	}
	if result.ObservedHolderDigest != digestFileLeaseHolder(*result.ObservedHolder) {
		return fmt.Errorf("late_stale_write observed_holder_digest mismatch")
	}
	if err := validateLateStaleWriteHolderRefs(result); err != nil {
		return err
	}
	if err := validateLateStaleWriteObservedLease(result); err != nil {
		return err
	}
	if err := validateLateStaleWriteRejectionEvidence(result); err != nil {
		return err
	}
	if err := validateLateStaleWriteObservedLeaseStore(result, leaseStore); err != nil {
		return err
	}
	if len(result.Contenders) != 0 {
		return fmt.Errorf("late_stale_write must not include path_conflict contenders")
	}
	if result.TestEvidence != nil || result.TestEvidenceDigest != "" {
		return fmt.Errorf("late_stale_write must not include test_conflict evidence")
	}
	if result.CASStatus != "" || result.CASPatchDigest != "" || result.CASEvaluationDigest != "" || result.CASIssueKind != "" {
		return fmt.Errorf("late_stale_write must not include CAS conflict evidence")
	}
	return nil
}

func validateSemanticConflictModel(result ConflictModelResult, patchQueueStore *PatchQueueStore) error {
	if err := validateConflictPath(result.Path); err != nil {
		return err
	}
	paths, err := NormalizePathSet(result.Paths)
	if err != nil {
		return fmt.Errorf("semantic_conflict paths: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("semantic_conflict paths are required")
	}
	if !sameStringSlice(paths, result.Paths) {
		return fmt.Errorf("semantic_conflict paths are not canonical")
	}
	if !stringSliceContainsPath(paths, result.Path) {
		return fmt.Errorf("semantic_conflict path %q must be included in paths", result.Path)
	}
	kind, subject, summary, err := normalizeSemanticConflictEvidence(result.SemanticKind, result.SemanticSubject, result.SemanticEvidenceSummary)
	if err != nil {
		return err
	}
	if result.SemanticKind != kind || result.SemanticSubject != subject || result.SemanticEvidenceSummary != summary {
		return fmt.Errorf("semantic_conflict evidence is not canonical")
	}
	if len(result.SemanticContenders) != 2 {
		return fmt.Errorf("semantic_conflict requires exactly two contenders")
	}
	seen := map[string]struct{}{}
	mergedPaths := make([]string, 0)
	for i, contender := range result.SemanticContenders {
		if err := validateSemanticConflictContender(contender, result.WorkspaceID, i); err != nil {
			return err
		}
		key := contender.QueueID + "\x00" + contender.ItemID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("semantic_conflict contender %s/%s is duplicated", contender.QueueID, contender.ItemID)
		}
		seen[key] = struct{}{}
		mergedPaths = append(mergedPaths, contender.Pathset...)
		if i > 0 {
			previous := result.SemanticContenders[i-1]
			if previous.QueueID > contender.QueueID || (previous.QueueID == contender.QueueID && previous.ItemID > contender.ItemID) {
				return fmt.Errorf("semantic_conflict contenders are not canonical")
			}
			if previous.WorkspaceID != contender.WorkspaceID {
				return fmt.Errorf("semantic_conflict contenders must be in the same workspace")
			}
			if previous.QueueID != contender.QueueID {
				return fmt.Errorf("semantic_conflict contenders must be in the same patch queue")
			}
			if previous.BaseRef != contender.BaseRef || previous.BaseTreeHash != contender.BaseTreeHash {
				return fmt.Errorf("semantic_conflict contenders must share the same base identity")
			}
			if pathsetsOverlap(previous.Pathset, contender.Pathset) {
				return fmt.Errorf("semantic_conflict contenders share a path; use path_conflict")
			}
		}
	}
	normalizedMergedPaths, err := NormalizePathSet(mergedPaths)
	if err != nil {
		return fmt.Errorf("semantic_conflict merged paths: %w", err)
	}
	if !sameStringSlice(normalizedMergedPaths, result.Paths) {
		return fmt.Errorf("semantic_conflict paths do not match contender pathsets")
	}
	if result.SemanticEvidenceDigest != digestSemanticConflictEvidence(result) {
		return fmt.Errorf("semantic_conflict evidence digest mismatch")
	}
	if len(result.Contenders) != 0 {
		return fmt.Errorf("semantic_conflict must not include path_conflict contenders")
	}
	if result.TestEvidence != nil || result.TestEvidenceDigest != "" {
		return fmt.Errorf("semantic_conflict must not include test_conflict evidence")
	}
	if result.CASStatus != "" ||
		result.CASPatchDigest != "" ||
		result.CASEvaluationDigest != "" ||
		result.CASIssueKind != "" ||
		result.ExpectedHash != "" ||
		result.ActualHash != "" ||
		result.CandidateHash != "" {
		return fmt.Errorf("semantic_conflict must not include base_drift evidence")
	}
	if result.OperationID != "" || result.OperationKind != "" || len(result.MutationPaths) != 0 {
		return fmt.Errorf("semantic_conflict must not include mutation operation evidence")
	}
	if result.TaskID != "" ||
		result.SessionID != "" ||
		result.RunID != "" ||
		result.AgentID != "" ||
		result.PrincipalType != "" ||
		result.PrincipalID != "" ||
		result.CapabilitySnapshotID != "" ||
		result.CapabilitySnapshotSchema != "" ||
		result.ContextDigest != "" ||
		result.RepoLeaseID != "" ||
		result.LeaseTerm != 0 ||
		result.PatchQueueID != "" ||
		result.PatchQueueItemID != "" ||
		result.PatchQueueState != "" ||
		result.BaseRef != "" ||
		result.BaseTreeHash != "" {
		return fmt.Errorf("semantic_conflict must not include top-level contender evidence")
	}
	if result.AttemptedAt != "" ||
		result.RejectionKind != "" ||
		result.RejectionMessage != "" ||
		result.AttemptedHolder != nil ||
		result.AttemptedHolderDigest != "" ||
		result.ObservedLease != nil ||
		result.ObservedLeaseDigest != "" ||
		result.ObservedLeaseStatus != "" ||
		result.ObservedLeaseTerm != 0 ||
		result.ObservedHolder != nil ||
		result.ObservedHolderDigest != "" {
		return fmt.Errorf("semantic_conflict must not include late_stale_write evidence")
	}
	if err := validateSemanticConflictPatchQueueStore(result, patchQueueStore); err != nil {
		return err
	}
	return nil
}

func validateSemanticConflictPatchQueueStore(result ConflictModelResult, patchQueueStore *PatchQueueStore) error {
	if patchQueueStore == nil {
		return fmt.Errorf("semantic_conflict requires patch queue store validation")
	}
	for _, contender := range result.SemanticContenders {
		item, ok := patchQueueStore.Get(contender.QueueID, contender.ItemID)
		if !ok {
			return fmt.Errorf("semantic_conflict contender store lookup: patch queue item %s/%s not found", contender.QueueID, contender.ItemID)
		}
		expected, err := semanticContenderFromPatchQueueItem(item, "store")
		if err != nil {
			return fmt.Errorf("semantic_conflict contender store evidence: %w", err)
		}
		if digestSemanticConflictContender(expected) != digestSemanticConflictContender(contender) {
			return fmt.Errorf("semantic_conflict contender store mismatch")
		}
	}
	return nil
}

func validateConflictContender(contender ConflictContender, workspaceID string, index int) error {
	prefix := fmt.Sprintf("path_conflict contender[%d]", index)
	for field, value := range map[string]string{
		"workspace_id":          contender.WorkspaceID,
		"queue_id":              contender.QueueID,
		"item_id":               contender.ItemID,
		"state":                 contender.State,
		"context_digest":        contender.ContextDigest,
		"repo_lease_id":         contender.RepoLeaseID,
		"task_id":               contender.TaskID,
		"session_id":            contender.SessionID,
		"run_id":                contender.RunID,
		"agent_id":              contender.AgentID,
		"base_ref":              contender.BaseRef,
		"base_tree_hash":        contender.BaseTreeHash,
		"cas_patch_digest":      contender.CASPatchDigest,
		"cas_evaluation_digest": contender.CASEvaluationDigest,
	} {
		if err := requireCanonicalConflictValue(prefix, field, value); err != nil {
			return err
		}
	}
	if contender.WorkspaceID != workspaceID {
		return fmt.Errorf("%s workspace mismatch: got %q want %q", prefix, contender.WorkspaceID, workspaceID)
	}
	if contender.LeaseTerm <= 0 {
		return fmt.Errorf("%s lease_term is required", prefix)
	}
	if !pathConflictContenderStateAllowed(contender.State) {
		return fmt.Errorf("%s state %q is not an active path_conflict contender state", prefix, contender.State)
	}
	return nil
}

func validateSemanticConflictContender(contender SemanticConflictContender, workspaceID string, index int) error {
	prefix := fmt.Sprintf("semantic_conflict contender[%d]", index)
	for field, value := range map[string]string{
		"workspace_id":               contender.WorkspaceID,
		"queue_id":                   contender.QueueID,
		"item_id":                    contender.ItemID,
		"state":                      contender.State,
		"context_digest":             contender.ContextDigest,
		"repo_lease_id":              contender.RepoLeaseID,
		"task_id":                    contender.TaskID,
		"session_id":                 contender.SessionID,
		"run_id":                     contender.RunID,
		"agent_id":                   contender.AgentID,
		"principal_type":             contender.PrincipalType,
		"principal_id":               contender.PrincipalID,
		"capability_snapshot_id":     contender.CapabilitySnapshotID,
		"capability_snapshot_schema": contender.CapabilitySnapshotSchema,
		"base_ref":                   contender.BaseRef,
		"base_tree_hash":             contender.BaseTreeHash,
	} {
		if err := requireCanonicalConflictValue(prefix, field, value); err != nil {
			return err
		}
	}
	if contender.WorkspaceID != workspaceID {
		return fmt.Errorf("%s workspace mismatch: got %q want %q", prefix, contender.WorkspaceID, workspaceID)
	}
	if contender.LeaseTerm <= 0 {
		return fmt.Errorf("%s lease_term is required", prefix)
	}
	if !semanticConflictContenderStateAllowed(contender.State) {
		return fmt.Errorf("%s state %q is not an active semantic_conflict contender state", prefix, contender.State)
	}
	pathset, err := NormalizePathSet(contender.Pathset)
	if err != nil {
		return fmt.Errorf("%s pathset: %w", prefix, err)
	}
	if len(pathset) == 0 {
		return fmt.Errorf("%s pathset is required", prefix)
	}
	if !sameStringSlice(pathset, contender.Pathset) {
		return fmt.Errorf("%s pathset is not canonical", prefix)
	}
	return nil
}

func validateConflictPath(path string) error {
	normalized, err := NormalizePath(path)
	if err != nil {
		return fmt.Errorf("conflict path: %w", err)
	}
	if normalized != path {
		return fmt.Errorf("conflict path is not normalized: got %q want %q", path, normalized)
	}
	return nil
}

func requireCanonicalConflictValue(prefix, field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s %s is required", prefix, field)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s %s is not canonical", prefix, field)
	}
	return nil
}

func normalizeSemanticConflictEvidence(kind, subject, summary string) (string, string, string, error) {
	kind = strings.TrimSpace(kind)
	subject = strings.TrimSpace(subject)
	summary = strings.TrimSpace(summary)
	if kind == "" {
		return "", "", "", fmt.Errorf("semantic_conflict semantic_kind is required")
	}
	if !semanticConflictKindAllowed(kind) {
		return "", "", "", fmt.Errorf("semantic_conflict semantic_kind %q is not supported", kind)
	}
	if subject == "" {
		return "", "", "", fmt.Errorf("semantic_conflict semantic_subject is required")
	}
	if summary == "" {
		return "", "", "", fmt.Errorf("semantic_conflict semantic_evidence_summary is required")
	}
	if len([]byte(summary)) > SemanticConflictEvidenceSummaryMaxBytes {
		return "", "", "", fmt.Errorf("semantic_conflict semantic_evidence_summary exceeds %d bytes", SemanticConflictEvidenceSummaryMaxBytes)
	}
	return kind, subject, summary, nil
}

func semanticConflictKindAllowed(kind string) bool {
	switch kind {
	case SemanticConflictKindAPIContract,
		SemanticConflictKindTestFixture,
		SemanticConflictKindBehavior,
		SemanticConflictKindPlaceholder:
		return true
	default:
		return false
	}
}

func semanticContenderFromPatchQueueItem(item PatchQueueItem, label string) (SemanticConflictContender, error) {
	for field, value := range map[string]string{
		"queue_id":                   item.QueueID,
		"item_id":                    item.ItemID,
		"state":                      item.State,
		"context_digest":             item.ContextDigest,
		"repo_lease_id":              item.RepoLeaseID,
		"workspace_id":               item.WorkspaceID,
		"task_id":                    item.TaskID,
		"session_id":                 item.SessionID,
		"run_id":                     item.RunID,
		"agent_id":                   item.AgentID,
		"principal_type":             item.PrincipalType,
		"principal_id":               item.PrincipalID,
		"capability_snapshot_id":     item.CapabilitySnapshotID,
		"capability_snapshot_schema": item.CapabilitySnapshotSchema,
		"base_ref":                   item.BaseRef,
		"base_tree_hash":             item.BaseTreeHash,
	} {
		if err := requireCanonicalConflictValue("semantic_conflict "+label, field, value); err != nil {
			return SemanticConflictContender{}, err
		}
	}
	if item.LeaseTerm <= 0 {
		return SemanticConflictContender{}, fmt.Errorf("semantic_conflict %s lease_term is required", label)
	}
	if !semanticConflictContenderStateAllowed(item.State) {
		return SemanticConflictContender{}, fmt.Errorf("semantic_conflict %s state %q is not an active semantic_conflict contender state", label, item.State)
	}
	pathset, err := NormalizePathSet(item.Pathset)
	if err != nil {
		return SemanticConflictContender{}, fmt.Errorf("semantic_conflict %s pathset: %w", label, err)
	}
	if len(pathset) == 0 {
		return SemanticConflictContender{}, fmt.Errorf("semantic_conflict %s pathset is required", label)
	}
	return SemanticConflictContender{
		WorkspaceID:              item.WorkspaceID,
		QueueID:                  item.QueueID,
		ItemID:                   item.ItemID,
		State:                    item.State,
		ContextDigest:            item.ContextDigest,
		RepoLeaseID:              item.RepoLeaseID,
		LeaseTerm:                item.LeaseTerm,
		Pathset:                  append([]string(nil), pathset...),
		TaskID:                   item.TaskID,
		SessionID:                item.SessionID,
		RunID:                    item.RunID,
		AgentID:                  item.AgentID,
		PrincipalType:            item.PrincipalType,
		PrincipalID:              item.PrincipalID,
		CapabilitySnapshotID:     item.CapabilitySnapshotID,
		CapabilitySnapshotSchema: item.CapabilitySnapshotSchema,
		BaseRef:                  item.BaseRef,
		BaseTreeHash:             item.BaseTreeHash,
		CASPatchDigest:           strings.TrimSpace(item.CASPatchDigest),
		CASEvaluationDigest:      strings.TrimSpace(item.CASEvaluationDigest),
	}, nil
}

func semanticConflictContenderStateAllowed(state string) bool {
	switch strings.TrimSpace(state) {
	case PatchQueueStateProposed, PatchQueueStateValidating:
		return true
	default:
		return false
	}
}

func pathsetsOverlap(left, right []string) bool {
	seen := make(map[string]struct{}, len(left))
	for _, path := range left {
		seen[path] = struct{}{}
	}
	for _, path := range right {
		if _, ok := seen[path]; ok {
			return true
		}
	}
	return false
}

func mergeSemanticContenderPaths(contenders []SemanticConflictContender) []string {
	merged := make([]string, 0)
	for _, contender := range contenders {
		merged = append(merged, contender.Pathset...)
	}
	normalized, err := NormalizePathSet(merged)
	if err != nil {
		return nil
	}
	return normalized
}

func chooseSemanticConflictPath(paths []string, requested string) (string, error) {
	normalizedPaths, err := NormalizePathSet(paths)
	if err != nil {
		return "", fmt.Errorf("semantic_conflict paths: %w", err)
	}
	if len(normalizedPaths) == 0 {
		return "", fmt.Errorf("semantic_conflict paths are required")
	}
	requested = strings.TrimSpace(requested)
	if requested != "" {
		normalized, err := NormalizePath(requested)
		if err != nil {
			return "", fmt.Errorf("semantic_conflict path: %w", err)
		}
		if normalized != requested {
			return "", fmt.Errorf("semantic_conflict path is not normalized: got %q want %q", requested, normalized)
		}
		if !stringSliceContainsPath(normalizedPaths, requested) {
			return "", fmt.Errorf("semantic_conflict path %q must be included in paths", requested)
		}
		return requested, nil
	}
	return normalizedPaths[0], nil
}

func digestSemanticConflictEvidence(result ConflictModelResult) string {
	evidence := struct {
		Kind       string                      `json:"kind"`
		Subject    string                      `json:"subject"`
		Summary    string                      `json:"summary"`
		Contenders []SemanticConflictContender `json:"contenders"`
	}{
		Kind:       result.SemanticKind,
		Subject:    result.SemanticSubject,
		Summary:    result.SemanticEvidenceSummary,
		Contenders: append([]SemanticConflictContender(nil), result.SemanticContenders...),
	}
	raw, _ := json.Marshal(evidence)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func digestSemanticConflictContender(contender SemanticConflictContender) string {
	raw, _ := json.Marshal(contender)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validateLateStaleWriteObservedLeaseStore(result ConflictModelResult, leaseStore *FileLeaseStore) error {
	if leaseStore == nil {
		return fmt.Errorf("late_stale_write requires lease store validation")
	}
	lease, err := findBindingLease(leaseStore, result.RepoLeaseID)
	if err != nil {
		return fmt.Errorf("late_stale_write observed_lease store lookup: %w", err)
	}
	storeDigest := digestFileLease(lease)
	if storeDigest != result.ObservedLeaseDigest || result.ObservedLease == nil || storeDigest != digestFileLease(*result.ObservedLease) {
		return fmt.Errorf("late_stale_write observed_lease store mismatch")
	}
	return nil
}

func fileLeaseHolderFromContext(authority Context) (FileLeaseHolder, error) {
	digest, err := authority.Digest()
	if err != nil {
		return FileLeaseHolder{}, err
	}
	return FileLeaseHolder{
		WorkspaceID:          authority.WorkspaceID,
		TaskID:               authority.TaskID,
		SessionID:            authority.SessionID,
		RunID:                authority.RunID,
		AgentID:              authority.AgentID,
		Principal:            authority.Principal,
		CapabilitySnapshotID: authority.CapabilitySnapshot.ID,
		ContextDigest:        digest,
	}, nil
}

func classifyLateStaleWriteRejection(bindErr error, authority Context, observedLease FileLease, attemptedHolder FileLeaseHolder) (string, error) {
	if bindErr == nil {
		return "", fmt.Errorf("late_stale_write requires rejected mutation operation binding")
	}
	message := strings.ToLower(bindErr.Error())
	switch {
	case observedLease.Status == FileLeaseStatusExpired || strings.Contains(message, "expired"):
		return LateStaleWriteRejectionExpiredLease, nil
	case observedLease.Status == FileLeaseStatusReleased || strings.Contains(message, "released"):
		return LateStaleWriteRejectionReleasedLease, nil
	case observedLease.Status == FileLeaseStatusRevoked || strings.Contains(message, "revoked"):
		return LateStaleWriteRejectionRevokedLease, nil
	case authority.Lease.Term != observedLease.Term || strings.Contains(message, "lease_term mismatch") || strings.Contains(message, "lease.term mismatch"):
		return LateStaleWriteRejectionStaleLeaseTerm, nil
	case !sameFileLeaseHolderIdentity(attemptedHolder, HolderForLease(observedLease)) ||
		strings.Contains(message, "lease context") ||
		strings.Contains(message, "lease holder") ||
		strings.Contains(message, "principal mismatch") ||
		strings.Contains(message, "context digest mismatch"):
		return LateStaleWriteRejectionStaleHolder, nil
	default:
		return "", fmt.Errorf("late_stale_write requires stale lease rejection, got: %v", bindErr)
	}
}

func validateLateStaleWriteHolderRefs(result ConflictModelResult) error {
	attempted := result.AttemptedHolder
	if attempted.WorkspaceID != result.WorkspaceID {
		return fmt.Errorf("late_stale_write attempted_holder workspace mismatch")
	}
	if attempted.TaskID != result.TaskID {
		return fmt.Errorf("late_stale_write attempted_holder task mismatch")
	}
	if attempted.SessionID != result.SessionID {
		return fmt.Errorf("late_stale_write attempted_holder session mismatch")
	}
	if attempted.RunID != result.RunID {
		return fmt.Errorf("late_stale_write attempted_holder run mismatch")
	}
	if attempted.AgentID != result.AgentID {
		return fmt.Errorf("late_stale_write attempted_holder agent mismatch")
	}
	if attempted.Principal.Type != result.PrincipalType || attempted.Principal.ID != result.PrincipalID {
		return fmt.Errorf("late_stale_write attempted_holder principal mismatch")
	}
	if attempted.CapabilitySnapshotID != result.CapabilitySnapshotID {
		return fmt.Errorf("late_stale_write attempted_holder capability snapshot mismatch")
	}
	if attempted.ContextDigest != result.ContextDigest {
		return fmt.Errorf("late_stale_write attempted_holder context digest mismatch")
	}
	observed := result.ObservedHolder
	if observed.WorkspaceID != result.WorkspaceID {
		return fmt.Errorf("late_stale_write observed_holder workspace mismatch")
	}
	if observed.TaskID != result.TaskID {
		return fmt.Errorf("late_stale_write observed_holder task mismatch")
	}
	return nil
}

func validateLateStaleWriteObservedLease(result ConflictModelResult) error {
	lease := *result.ObservedLease
	if result.ObservedLeaseDigest != digestFileLease(lease) {
		return fmt.Errorf("late_stale_write observed_lease_digest mismatch")
	}
	if lease.Schema != FileLeaseSchemaVersion {
		return fmt.Errorf("late_stale_write observed_lease schema mismatch")
	}
	for field, value := range map[string]string{
		"id":                     lease.ID,
		"status":                 lease.Status,
		"workspace_id":           lease.WorkspaceID,
		"task_id":                lease.TaskID,
		"session_id":             lease.SessionID,
		"run_id":                 lease.RunID,
		"agent_id":               lease.AgentID,
		"principal_type":         lease.Principal.Type,
		"principal_id":           lease.Principal.ID,
		"capability_snapshot_id": lease.CapabilitySnapshotID,
		"repo_root":              lease.RepoRoot,
		"context_digest":         lease.ContextDigest,
		"acquired_at":            lease.AcquiredAt,
		"updated_at":             lease.UpdatedAt,
		"expires_at":             lease.ExpiresAt,
	} {
		if err := requireCanonicalConflictValue("observed_lease", field, value); err != nil {
			return err
		}
	}
	parsedTimes := map[string]time.Time{}
	for field, value := range map[string]string{
		"acquired_at": lease.AcquiredAt,
		"updated_at":  lease.UpdatedAt,
		"expires_at":  lease.ExpiresAt,
	} {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return fmt.Errorf("late_stale_write observed_lease %s timestamp is not canonical", field)
		}
		parsedTimes[field] = parsed
	}
	attemptedAt, err := time.Parse(time.RFC3339Nano, result.AttemptedAt)
	if err != nil {
		return fmt.Errorf("late_stale_write attempted_at must be RFC3339Nano")
	}
	if err := validateLateStaleWriteObservedLeaseTime(lease, attemptedAt, parsedTimes["acquired_at"], parsedTimes["updated_at"], parsedTimes["expires_at"]); err != nil {
		return err
	}
	if lease.ID != result.RepoLeaseID {
		return fmt.Errorf("late_stale_write observed_lease id mismatch")
	}
	expectedLeaseID := buildFileLeaseID(Context{
		WorkspaceID: lease.WorkspaceID,
		TaskID:      lease.TaskID,
		AgentID:     lease.AgentID,
	}, lease.Term, lease.ContextDigest)
	if lease.ID != expectedLeaseID {
		return fmt.Errorf("late_stale_write observed_lease id binding mismatch")
	}
	if lease.Term != result.ObservedLeaseTerm {
		return fmt.Errorf("late_stale_write observed_lease term mismatch")
	}
	if lease.Status != result.ObservedLeaseStatus {
		return fmt.Errorf("late_stale_write observed_lease status mismatch")
	}
	if lease.WorkspaceID != result.WorkspaceID {
		return fmt.Errorf("late_stale_write observed_lease workspace mismatch")
	}
	if lease.TaskID != result.TaskID {
		return fmt.Errorf("late_stale_write observed_lease task mismatch")
	}
	leaseHolder := HolderForLease(lease)
	if !sameFileLeaseHolder(leaseHolder, *result.ObservedHolder) {
		return fmt.Errorf("late_stale_write observed_lease holder mismatch")
	}
	leasePathset, err := NormalizePathSet(lease.Pathset)
	if err != nil {
		return fmt.Errorf("late_stale_write observed_lease pathset: %w", err)
	}
	if len(leasePathset) == 0 || !sameStringSlice(leasePathset, lease.Pathset) {
		return fmt.Errorf("late_stale_write observed_lease pathset is not canonical")
	}
	for _, path := range result.MutationPaths {
		if !stringSliceContainsPath(leasePathset, path) {
			return fmt.Errorf("late_stale_write observed_lease does not cover mutation path %q", path)
		}
	}
	return nil
}

func validateLateStaleWriteObservedLeaseTime(lease FileLease, attemptedAt time.Time, acquiredAt time.Time, updatedAt time.Time, expiresAt time.Time) error {
	if updatedAt.Before(acquiredAt) {
		return fmt.Errorf("late_stale_write observed_lease updated_at before acquired_at")
	}
	if !expiresAt.After(acquiredAt) {
		return fmt.Errorf("late_stale_write observed_lease expires_at before acquired_at")
	}
	if attemptedAt.Before(acquiredAt) {
		return fmt.Errorf("late_stale_write observed_lease attempted_at before acquired_at")
	}
	if updatedAt.After(attemptedAt) {
		return fmt.Errorf("late_stale_write observed_lease updated_at after attempted_at")
	}
	switch lease.Status {
	case FileLeaseStatusActive:
		if !attemptedAt.Before(expiresAt) {
			return fmt.Errorf("late_stale_write observed_lease active lease expired at attempted_at")
		}
	case FileLeaseStatusExpired:
		if attemptedAt.Before(expiresAt) || updatedAt.Before(expiresAt) {
			return fmt.Errorf("late_stale_write observed_lease expired lease timing is inconsistent")
		}
	case FileLeaseStatusReleased, FileLeaseStatusRevoked:
		if !updatedAt.Before(expiresAt) {
			return fmt.Errorf("late_stale_write observed_lease terminal lease timing is inconsistent")
		}
	default:
		return fmt.Errorf("late_stale_write observed_lease status %q is not supported", lease.Status)
	}
	return nil
}

func validateLateStaleWriteRejectionEvidence(result ConflictModelResult) error {
	message := strings.ToLower(result.RejectionMessage)
	attempted := *result.AttemptedHolder
	observed := *result.ObservedHolder
	sameHolderIdentity := sameFileLeaseHolderIdentity(attempted, observed)
	switch result.RejectionKind {
	case LateStaleWriteRejectionExpiredLease:
		if result.ObservedLeaseStatus != FileLeaseStatusExpired || !strings.Contains(message, "expired") || !sameHolderIdentity {
			return fmt.Errorf("late_stale_write expired_lease evidence is inconsistent")
		}
	case LateStaleWriteRejectionReleasedLease:
		if result.ObservedLeaseStatus != FileLeaseStatusReleased || !strings.Contains(message, "released") || !sameHolderIdentity {
			return fmt.Errorf("late_stale_write released_lease evidence is inconsistent")
		}
	case LateStaleWriteRejectionRevokedLease:
		if result.ObservedLeaseStatus != FileLeaseStatusRevoked || !strings.Contains(message, "revoked") || !sameHolderIdentity {
			return fmt.Errorf("late_stale_write revoked_lease evidence is inconsistent")
		}
	case LateStaleWriteRejectionStaleLeaseTerm:
		if result.ObservedLeaseStatus != FileLeaseStatusActive ||
			result.LeaseTerm == result.ObservedLeaseTerm ||
			(!strings.Contains(message, "lease_term mismatch") && !strings.Contains(message, "lease.term mismatch")) ||
			!sameHolderIdentity {
			return fmt.Errorf("late_stale_write stale_lease_term evidence is inconsistent")
		}
	case LateStaleWriteRejectionStaleHolder:
		if result.ObservedLeaseStatus != FileLeaseStatusActive || sameHolderIdentity {
			return fmt.Errorf("late_stale_write stale_holder evidence is inconsistent")
		}
		if err := validateLateStaleWriteStaleHolderMessage(result.RejectionMessage, attempted, observed); err != nil {
			return err
		}
	default:
		return fmt.Errorf("late_stale_write rejection_kind %q is not supported", result.RejectionKind)
	}
	return nil
}

func validateLateStaleWriteStaleHolderMessage(message string, attempted FileLeaseHolder, observed FileLeaseHolder) error {
	expected := []string{}
	if attempted.WorkspaceID != observed.WorkspaceID {
		expected = append(expected, fmt.Sprintf("lease context workspace mismatch: got %q want %q", attempted.WorkspaceID, observed.WorkspaceID))
	}
	if attempted.TaskID != observed.TaskID {
		expected = append(expected, fmt.Sprintf("lease context task mismatch: got %q want %q", attempted.TaskID, observed.TaskID))
	}
	if attempted.SessionID != observed.SessionID {
		expected = append(expected, fmt.Sprintf("lease context session mismatch: got %q want %q", attempted.SessionID, observed.SessionID))
	}
	if attempted.RunID != observed.RunID {
		expected = append(expected, fmt.Sprintf("lease context run mismatch: got %q want %q", attempted.RunID, observed.RunID))
	}
	if attempted.AgentID != observed.AgentID {
		expected = append(expected, fmt.Sprintf("lease context agent mismatch: got %q want %q", attempted.AgentID, observed.AgentID))
	}
	if attempted.Principal.Type != observed.Principal.Type || attempted.Principal.ID != observed.Principal.ID {
		expected = append(expected, "lease context principal mismatch")
	}
	if attempted.CapabilitySnapshotID != observed.CapabilitySnapshotID {
		expected = append(expected, fmt.Sprintf("lease context capability snapshot mismatch: got %q want %q", attempted.CapabilitySnapshotID, observed.CapabilitySnapshotID))
	}
	if attempted.ContextDigest != observed.ContextDigest {
		expected = append(expected, fmt.Sprintf("lease acquisition context digest mismatch: got %q want %q", attempted.ContextDigest, observed.ContextDigest))
	}
	for _, candidate := range expected {
		if message == candidate {
			return nil
		}
	}
	return fmt.Errorf("late_stale_write stale_holder rejection message is inconsistent")
}

func validateCanonicalFileLeaseHolder(prefix string, holder FileLeaseHolder) error {
	if err := validateFileLeaseHolder(holder); err != nil {
		return err
	}
	for field, value := range map[string]string{
		"workspace_id":           holder.WorkspaceID,
		"task_id":                holder.TaskID,
		"session_id":             holder.SessionID,
		"run_id":                 holder.RunID,
		"agent_id":               holder.AgentID,
		"principal_type":         holder.Principal.Type,
		"principal_id":           holder.Principal.ID,
		"capability_snapshot_id": holder.CapabilitySnapshotID,
		"context_digest":         holder.ContextDigest,
	} {
		if err := requireCanonicalConflictValue(prefix, field, value); err != nil {
			return err
		}
	}
	return nil
}

func digestFileLeaseHolder(holder FileLeaseHolder) string {
	raw, _ := json.Marshal(holder)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func digestFileLease(lease FileLease) string {
	raw, _ := json.Marshal(lease)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func sameFileLeaseHolder(left, right FileLeaseHolder) bool {
	return left.WorkspaceID == right.WorkspaceID &&
		left.TaskID == right.TaskID &&
		left.SessionID == right.SessionID &&
		left.RunID == right.RunID &&
		left.AgentID == right.AgentID &&
		left.Principal.Type == right.Principal.Type &&
		left.Principal.ID == right.Principal.ID &&
		left.CapabilitySnapshotID == right.CapabilitySnapshotID &&
		left.ContextDigest == right.ContextDigest
}

func sameFileLeaseHolderIdentity(left, right FileLeaseHolder) bool {
	return left.WorkspaceID == right.WorkspaceID &&
		left.TaskID == right.TaskID &&
		left.SessionID == right.SessionID &&
		left.RunID == right.RunID &&
		left.AgentID == right.AgentID &&
		left.Principal.Type == right.Principal.Type &&
		left.Principal.ID == right.Principal.ID &&
		left.CapabilitySnapshotID == right.CapabilitySnapshotID
}

func chooseBaseDriftPath(result CASPatchApplyResult, requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	if requested != "" {
		normalized, err := NormalizePath(requested)
		if err != nil {
			return "", fmt.Errorf("base_drift conflict path: %w", err)
		}
		if normalized != requested {
			return "", fmt.Errorf("base_drift conflict path is not normalized: got %q want %q", requested, normalized)
		}
		return requested, nil
	}
	paths := map[string]struct{}{}
	for _, issue := range result.Issues {
		if issue.Status == CASPatchStatusConflict && issue.Kind == CASPatchIssueBaseDrift {
			paths[strings.TrimSpace(issue.Path)] = struct{}{}
		}
	}
	if len(paths) != 1 {
		return "", fmt.Errorf("base_drift conflict path is ambiguous; provide path")
	}
	for path := range paths {
		if path == "" {
			return "", fmt.Errorf("base_drift conflict path is required")
		}
		return path, nil
	}
	return "", fmt.Errorf("base_drift conflict path is required")
}

func chooseTestConflictPaths(pathset []string, requested string) ([]string, string, error) {
	paths, err := NormalizePathSet(pathset)
	if err != nil {
		return nil, "", fmt.Errorf("test_conflict paths: %w", err)
	}
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("test_conflict paths are required")
	}
	requested = strings.TrimSpace(requested)
	if requested != "" {
		normalized, err := NormalizePath(requested)
		if err != nil {
			return nil, "", fmt.Errorf("test_conflict path: %w", err)
		}
		if normalized != requested {
			return nil, "", fmt.Errorf("test_conflict path is not normalized: got %q want %q", requested, normalized)
		}
		if !stringSliceContainsPath(paths, requested) {
			return nil, "", fmt.Errorf("test_conflict path %q must be included in paths", requested)
		}
		return paths, requested, nil
	}
	return paths, paths[0], nil
}

func chooseLateStaleWritePaths(mutationPaths []string, requested string) ([]string, string, error) {
	paths, err := NormalizePathSet(mutationPaths)
	if err != nil {
		return nil, "", fmt.Errorf("late_stale_write paths: %w", err)
	}
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("late_stale_write paths are required")
	}
	requested = strings.TrimSpace(requested)
	if requested != "" {
		normalized, err := NormalizePath(requested)
		if err != nil {
			return nil, "", fmt.Errorf("late_stale_write path: %w", err)
		}
		if normalized != requested {
			return nil, "", fmt.Errorf("late_stale_write path is not normalized: got %q want %q", requested, normalized)
		}
		if !stringSliceContainsPath(paths, requested) {
			return nil, "", fmt.Errorf("late_stale_write path %q must be included in mutation paths", requested)
		}
		return paths, requested, nil
	}
	return paths, paths[0], nil
}

func baseDriftIssueAndPathResult(result CASPatchApplyResult, path string) (CASPatchIssue, CASPatchPathResult, error) {
	var issue CASPatchIssue
	var pathResult CASPatchPathResult
	issueFound := false
	pathFound := false
	for _, candidate := range result.Issues {
		if candidate.Status == CASPatchStatusConflict && candidate.Kind == CASPatchIssueBaseDrift && candidate.Path == path {
			if issueFound {
				return CASPatchIssue{}, CASPatchPathResult{}, fmt.Errorf("base_drift conflict has duplicate issue for %q", path)
			}
			issue = candidate
			issueFound = true
		}
	}
	for _, candidate := range result.Paths {
		if candidate.Path == path {
			if pathFound {
				return CASPatchIssue{}, CASPatchPathResult{}, fmt.Errorf("base_drift conflict has duplicate path result for %q", path)
			}
			pathResult = candidate
			pathFound = true
		}
	}
	if !issueFound {
		return CASPatchIssue{}, CASPatchPathResult{}, fmt.Errorf("base_drift conflict requires base_drift issue for %q", path)
	}
	if !pathFound || pathResult.Status != CASPatchStatusConflict {
		return CASPatchIssue{}, CASPatchPathResult{}, fmt.Errorf("base_drift conflict requires conflict path result for %q", path)
	}
	return issue, pathResult, nil
}

func conflictContenderFromPatchQueueItem(item PatchQueueItem, path, label string) (ConflictContender, error) {
	for field, value := range map[string]string{
		"queue_id":       item.QueueID,
		"item_id":        item.ItemID,
		"state":          item.State,
		"context_digest": item.ContextDigest,
		"repo_lease_id":  item.RepoLeaseID,
		"workspace_id":   item.WorkspaceID,
	} {
		if strings.TrimSpace(value) == "" {
			return ConflictContender{}, fmt.Errorf("path_conflict %s %s is required", label, field)
		}
	}
	if item.LeaseTerm <= 0 {
		return ConflictContender{}, fmt.Errorf("path_conflict %s lease_term is required", label)
	}
	if !pathConflictContenderStateAllowed(item.State) {
		return ConflictContender{}, fmt.Errorf("path_conflict %s state %q is not an active path_conflict contender state", label, item.State)
	}
	if !pathsetCoversPath(item.Pathset, path) {
		return ConflictContender{}, fmt.Errorf("path_conflict %s item %s/%s does not cover path %q", label, item.QueueID, item.ItemID, path)
	}
	return ConflictContender{
		WorkspaceID:         strings.TrimSpace(item.WorkspaceID),
		QueueID:             strings.TrimSpace(item.QueueID),
		ItemID:              strings.TrimSpace(item.ItemID),
		State:               strings.TrimSpace(item.State),
		ContextDigest:       strings.TrimSpace(item.ContextDigest),
		RepoLeaseID:         strings.TrimSpace(item.RepoLeaseID),
		LeaseTerm:           item.LeaseTerm,
		TaskID:              strings.TrimSpace(item.TaskID),
		SessionID:           strings.TrimSpace(item.SessionID),
		RunID:               strings.TrimSpace(item.RunID),
		AgentID:             strings.TrimSpace(item.AgentID),
		BaseRef:             strings.TrimSpace(item.BaseRef),
		BaseTreeHash:        strings.TrimSpace(item.BaseTreeHash),
		CASPatchDigest:      strings.TrimSpace(item.CASPatchDigest),
		CASEvaluationDigest: strings.TrimSpace(item.CASEvaluationDigest),
	}, nil
}

func stringSliceContainsPath(paths []string, want string) bool {
	for _, path := range paths {
		if strings.TrimSpace(path) == want {
			return true
		}
	}
	return false
}

func pathConflictContenderStateAllowed(state string) bool {
	switch strings.TrimSpace(state) {
	case PatchQueueStateProposed, PatchQueueStateValidating:
		return true
	default:
		return false
	}
}

func conflictModelID(result ConflictModelResult) string {
	clone := result
	clone.ConflictID = ""
	raw, _ := json.Marshal(clone)
	sum := sha256.Sum256(raw)
	return "conflict_" + hex.EncodeToString(sum[:8])
}
