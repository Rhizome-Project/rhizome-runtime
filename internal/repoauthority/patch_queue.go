package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	PatchQueueItemSchemaVersion = "repo_patch_queue_item.v1"

	PatchQueueStateProposed     = "proposed"
	PatchQueueStateValidating   = "validating"
	PatchQueueStateApplied      = "applied"
	PatchQueueStateFailed       = "failed"
	PatchQueueStateConflict     = "conflict"
	PatchQueueStateTestConflict = "test_conflict"
	PatchQueueStateCanceled     = "canceled"
	PatchQueueStateRetryPending = "retry_pending"
	PatchQueueStateDeadLetter   = "dead_letter"
	PatchQueueStateRolledBack   = "rolled_back"

	PatchQueueTestEvidenceSchemaVersion = "repo_patch_queue_test_evidence.v1"
	PatchQueueRecoverySchemaVersion     = "repo_patch_queue_recovery_decision.v1"
	PatchQueueRollbackSchemaVersion     = "repo_patch_queue_rollback_evidence.v1"
	PatchQueueReviewerAdvisorySchema    = "repo_patch_queue_reviewer_advisory.v1"
	PatchQueueOperatorEnablementSchema  = "repo_patch_queue_operator_enablement.v1"

	PatchQueueTestStatusPassed = "passed"
	PatchQueueTestStatusFailed = "failed"

	PatchQueueRecoveryDecisionRetry      = "retry"
	PatchQueueRecoveryDecisionDeadLetter = "dead_letter"

	PatchQueueReviewerAdvisoryVerdictReviewed              = "reviewed"
	PatchQueueReviewerAdvisoryVerdictRepairRequired        = "repair_required"
	PatchQueueReviewerAdvisoryScopeLaneCorrectness         = "lane_correctness"
	PatchQueueReviewerAdvisoryScopeIntegrationCompleteness = "integration_completeness"
	PatchQueueOperatorEnablementScopeMutationActivation    = "repo_mutation_activation"
)

const patchQueueTestOutputSummaryMaxBytes = 4096
const patchQueueRecoveryReasonMaxBytes = 4096

type PatchQueueItem struct {
	Schema                   string                       `json:"schema"`
	ID                       string                       `json:"id"`
	QueueID                  string                       `json:"queue_id"`
	ItemID                   string                       `json:"item_id"`
	ReviewDocKey             string                       `json:"review_doc_key,omitempty"`
	State                    string                       `json:"state"`
	Attempt                  int                          `json:"attempt"`
	MaxAttempts              int                          `json:"max_attempts"`
	NextRetryAt              string                       `json:"next_retry_at,omitempty"`
	DeadLetteredAt           string                       `json:"dead_lettered_at,omitempty"`
	ContextDigest            string                       `json:"context_digest"`
	RepoLeaseID              string                       `json:"repo_lease_id"`
	LeaseTerm                int64                        `json:"lease_term"`
	Pathset                  []string                     `json:"pathset"`
	WorkspaceID              string                       `json:"workspace_id"`
	ProjectID                string                       `json:"project_id,omitempty"`
	TaskID                   string                       `json:"task_id"`
	SessionID                string                       `json:"session_id"`
	RunID                    string                       `json:"run_id"`
	AgentID                  string                       `json:"agent_id"`
	PrincipalType            string                       `json:"principal_type"`
	PrincipalID              string                       `json:"principal_id"`
	CapabilitySnapshotID     string                       `json:"capability_snapshot_id"`
	CapabilitySnapshotSchema string                       `json:"capability_snapshot_schema,omitempty"`
	BaseRef                  string                       `json:"base_ref,omitempty"`
	BaseTreeHash             string                       `json:"base_tree_hash,omitempty"`
	CASResult                CASPatchApplyResult          `json:"cas_result,omitempty"`
	CASPatchDigest           string                       `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest      string                       `json:"cas_evaluation_digest,omitempty"`
	ConflictIssues           []CASPatchIssue              `json:"conflict_issues,omitempty"`
	TestEvidence             PatchQueueTestEvidence       `json:"test_evidence,omitempty"`
	TestEvidenceDigest       string                       `json:"test_evidence_digest,omitempty"`
	RecoveryDecisions        []PatchQueueRecovery         `json:"recovery_decisions,omitempty"`
	RecoveryDecisionDigest   string                       `json:"recovery_decision_digest,omitempty"`
	RollbackEvidence         PatchQueueRollback           `json:"rollback_evidence,omitempty"`
	RollbackEvidenceDigest   string                       `json:"rollback_evidence_digest,omitempty"`
	ReviewerAdvisory         PatchQueueReviewerAdvisory   `json:"reviewer_advisory,omitempty"`
	ReviewerAdvisoryDigest   string                       `json:"reviewer_advisory_digest,omitempty"`
	OperatorEnablement       PatchQueueOperatorEnablement `json:"operator_enablement,omitempty"`
	OperatorEnablementDigest string                       `json:"operator_enablement_digest,omitempty"`
	OperationID              string                       `json:"operation_id,omitempty"`
	OperationKind            string                       `json:"operation_kind,omitempty"`
	CreatedAt                string                       `json:"created_at"`
	UpdatedAt                string                       `json:"updated_at"`
}

type PatchQueueTestEvidence struct {
	Schema         string `json:"schema"`
	Name           string `json:"name"`
	Command        string `json:"command"`
	Status         string `json:"status"`
	ExitCode       int    `json:"exit_code"`
	OutputDigest   string `json:"output_digest"`
	OutputSummary  string `json:"output_summary,omitempty"`
	DurationMillis int64  `json:"duration_ms,omitempty"`
}

type PatchQueueRecovery struct {
	Schema         string `json:"schema"`
	Decision       string `json:"decision"`
	SourceState    string `json:"source_state"`
	Reason         string `json:"reason"`
	Attempt        int    `json:"attempt"`
	MaxAttempts    int    `json:"max_attempts"`
	Retryable      bool   `json:"retryable"`
	NextRetryAt    string `json:"next_retry_at,omitempty"`
	DeadLetteredAt string `json:"dead_lettered_at,omitempty"`
	RecordedAt     string `json:"recorded_at"`
}

type PatchQueueRollback struct {
	Schema                     string                   `json:"schema"`
	SourceOperationID          string                   `json:"source_operation_id"`
	SourceOperationKind        string                   `json:"source_operation_kind"`
	RollbackOperationID        string                   `json:"rollback_operation_id"`
	RollbackOperationKind      string                   `json:"rollback_operation_kind"`
	Reason                     string                   `json:"reason"`
	SourcePatchDigest          string                   `json:"source_patch_digest"`
	RollbackPatchDigest        string                   `json:"rollback_patch_digest"`
	RollbackPaths              []PatchQueueRollbackPath `json:"rollback_paths,omitempty"`
	VerificationCommand        string                   `json:"verification_command"`
	VerificationStatus         string                   `json:"verification_status"`
	VerificationExitCode       int                      `json:"verification_exit_code"`
	VerificationOutputDigest   string                   `json:"verification_output_digest"`
	VerificationOutputSummary  string                   `json:"verification_output_summary,omitempty"`
	VerificationDurationMillis int64                    `json:"verification_duration_ms,omitempty"`
	RecordedAt                 string                   `json:"recorded_at"`
}

type PatchQueueRollbackPath struct {
	Path                  string `json:"path"`
	SourceBaseHash        string `json:"source_base_hash"`
	SourceAppliedHash     string `json:"source_applied_hash"`
	RollbackCandidateHash string `json:"rollback_candidate_hash"`
}

type PatchQueueReviewerAdvisory struct {
	Schema                 string `json:"schema"`
	Mode                   string `json:"mode"`
	Verdict                string `json:"verdict"`
	Scope                  string `json:"scope,omitempty"`
	HeadSHA                string `json:"head_sha,omitempty"`
	DefeatsAcceptance      bool   `json:"defeats_acceptance,omitempty"`
	ReviewerID             string `json:"reviewer_id"`
	ReviewDocKey           string `json:"review_doc_key"`
	OperationID            string `json:"operation_id"`
	OperationKind          string `json:"operation_kind"`
	CASPatchDigest         string `json:"cas_patch_digest"`
	CASEvaluationDigest    string `json:"cas_evaluation_digest"`
	RollbackEvidenceDigest string `json:"rollback_evidence_digest"`
	Summary                string `json:"summary,omitempty"`
	RecordedAt             string `json:"recorded_at"`
}

type PatchQueueOperatorEnablement struct {
	Schema                 string `json:"schema"`
	Scope                  string `json:"scope"`
	Enabled                bool   `json:"enabled"`
	EnabledBy              string `json:"enabled_by"`
	EnabledAt              string `json:"enabled_at"`
	Reason                 string `json:"reason"`
	WorkspaceID            string `json:"workspace_id"`
	ProjectID              string `json:"project_id"`
	QueueID                string `json:"queue_id"`
	ItemID                 string `json:"item_id"`
	OperationID            string `json:"operation_id"`
	CASPatchDigest         string `json:"cas_patch_digest"`
	RollbackEvidenceDigest string `json:"rollback_evidence_digest"`
	ReviewerAdvisoryDigest string `json:"reviewer_advisory_digest"`
}

type PatchQueueStore struct {
	mu    sync.Mutex
	items map[string]PatchQueueItem
}

type ProposePatchQueueItemInput struct {
	Context     Context
	LeaseStore  *FileLeaseStore
	Now         time.Time
	MaxAttempts int
}

type PatchQueueTransitionInput struct {
	QueueID string
	ItemID  string
	Context Context
	Now     time.Time
}

type StartPatchQueueRetryValidationInput struct {
	QueueID    string
	ItemID     string
	Context    Context
	LeaseStore *FileLeaseStore
	Now        time.Time
}

type CompletePatchQueueValidationInput struct {
	QueueID      string
	ItemID       string
	Context      Context
	LeaseStore   *FileLeaseStore
	CASResult    CASPatchApplyResult
	TestEvidence PatchQueueTestEvidence
	Now          time.Time
}

type RecordPatchQueueRecoveryInput struct {
	QueueID    string
	ItemID     string
	Context    Context
	Reason     string
	RetryDelay time.Duration
	Now        time.Time
}

type RecordPatchQueueRollbackInput struct {
	QueueID    string
	ItemID     string
	Context    Context
	LeaseStore *FileLeaseStore
	Evidence   PatchQueueRollback
	Now        time.Time
}

func NewPatchQueueStore() *PatchQueueStore {
	return &PatchQueueStore{
		items: make(map[string]PatchQueueItem),
	}
}

func (s *PatchQueueStore) Propose(input ProposePatchQueueItemInput) (PatchQueueItem, error) {
	if s == nil {
		return PatchQueueItem{}, fmt.Errorf("patch queue store is required")
	}
	authority := input.Context.WithDefaults()
	if err := requirePatchQueueProposalRefs(authority); err != nil {
		return PatchQueueItem{}, err
	}
	if err := verifyPatchQueueLeaseAuthority(authority, input.LeaseStore, input.Now); err != nil {
		return PatchQueueItem{}, err
	}
	contextDigest, err := patchQueueContextDigest(authority)
	if err != nil {
		return PatchQueueItem{}, err
	}
	pathset, err := NormalizePathSet(authority.Pathset)
	if err != nil {
		return PatchQueueItem{}, fmt.Errorf("patch queue pathset: %w", err)
	}
	now := normalizeLeaseTime(input.Now)
	item := PatchQueueItem{
		Schema:                   PatchQueueItemSchemaVersion,
		ID:                       buildPatchQueueItemID(authority.PatchQueue.QueueID, authority.PatchQueue.ItemID, contextDigest),
		QueueID:                  strings.TrimSpace(authority.PatchQueue.QueueID),
		ItemID:                   strings.TrimSpace(authority.PatchQueue.ItemID),
		State:                    PatchQueueStateProposed,
		Attempt:                  1,
		MaxAttempts:              normalizePatchQueueMaxAttempts(input.MaxAttempts),
		ContextDigest:            contextDigest,
		RepoLeaseID:              strings.TrimSpace(authority.Lease.ID),
		LeaseTerm:                authority.Lease.Term,
		Pathset:                  append([]string(nil), pathset...),
		WorkspaceID:              authority.WorkspaceID,
		TaskID:                   authority.TaskID,
		SessionID:                authority.SessionID,
		RunID:                    authority.RunID,
		AgentID:                  authority.AgentID,
		PrincipalType:            authority.Principal.Type,
		PrincipalID:              authority.Principal.ID,
		CapabilitySnapshotID:     authority.CapabilitySnapshot.ID,
		CapabilitySnapshotSchema: authority.CapabilitySnapshot.Schema,
		BaseRef:                  authority.Base.Ref,
		BaseTreeHash:             authority.Base.TreeHash,
		CreatedAt:                formatLeaseTime(now),
		UpdatedAt:                formatLeaseTime(now),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := patchQueueKey(item.QueueID, item.ItemID)
	if _, exists := s.items[key]; exists {
		return PatchQueueItem{}, fmt.Errorf("patch queue item %s/%s already exists", item.QueueID, item.ItemID)
	}
	s.items[key] = clonePatchQueueItem(item)
	return clonePatchQueueItem(item), nil
}

func (s *PatchQueueStore) StartValidation(input PatchQueueTransitionInput) (PatchQueueItem, error) {
	return s.transition(input, PatchQueueStateValidating, nil)
}

func (s *PatchQueueStore) StartRetryValidation(input StartPatchQueueRetryValidationInput) (PatchQueueItem, error) {
	if s == nil {
		return PatchQueueItem{}, fmt.Errorf("patch queue store is required")
	}
	authority := input.Context.WithDefaults()
	queueID, itemID := patchQueueInputIDs(input.QueueID, input.ItemID, authority)
	if err := rejectVagueBindingLabel("patch_queue_id", queueID); err != nil {
		return PatchQueueItem{}, err
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", itemID); err != nil {
		return PatchQueueItem{}, err
	}
	now := normalizeLeaseTime(input.Now)

	s.mu.Lock()
	defer s.mu.Unlock()
	key := patchQueueKey(queueID, itemID)
	current, ok := s.items[key]
	if !ok {
		return PatchQueueItem{}, fmt.Errorf("patch queue item %s/%s not found", queueID, itemID)
	}
	if current.State != PatchQueueStateRetryPending {
		return PatchQueueItem{}, fmt.Errorf("illegal patch queue transition %s -> %s", current.State, PatchQueueStateValidating)
	}
	if err := verifyPatchQueueItemContext(current, authority); err != nil {
		return PatchQueueItem{}, err
	}
	if strings.TrimSpace(current.NextRetryAt) == "" {
		return PatchQueueItem{}, fmt.Errorf("retry_pending patch queue item requires next_retry_at")
	}
	nextRetryAt, err := time.Parse(time.RFC3339Nano, current.NextRetryAt)
	if err != nil {
		return PatchQueueItem{}, fmt.Errorf("retry_pending patch queue item has invalid next_retry_at: %w", err)
	}
	if now.Before(nextRetryAt) {
		return PatchQueueItem{}, fmt.Errorf("retry_pending patch queue item is not retryable until %s", current.NextRetryAt)
	}
	if err := verifyPatchQueueLeaseAuthority(authority, input.LeaseStore, input.Now); err != nil {
		return PatchQueueItem{}, err
	}

	next := clonePatchQueueItem(current)
	next.State = PatchQueueStateValidating
	next.Attempt = normalizePatchQueueAttempt(current.Attempt) + 1
	next.NextRetryAt = ""
	next.CASResult = CASPatchApplyResult{}
	next.CASPatchDigest = ""
	next.CASEvaluationDigest = ""
	next.ConflictIssues = nil
	next.TestEvidence = PatchQueueTestEvidence{}
	next.TestEvidenceDigest = ""
	next.UpdatedAt = formatLeaseTime(now)
	s.items[key] = clonePatchQueueItem(next)
	return clonePatchQueueItem(next), nil
}

func (s *PatchQueueStore) Cancel(input PatchQueueTransitionInput) (PatchQueueItem, error) {
	return s.transition(input, PatchQueueStateCanceled, func(current PatchQueueItem) error {
		if current.State != PatchQueueStateProposed && current.State != PatchQueueStateValidating {
			return fmt.Errorf("illegal patch queue transition %s -> %s", current.State, PatchQueueStateCanceled)
		}
		return nil
	})
}

func (s *PatchQueueStore) CompleteValidation(input CompletePatchQueueValidationInput) (PatchQueueItem, error) {
	if s == nil {
		return PatchQueueItem{}, fmt.Errorf("patch queue store is required")
	}
	authority := input.Context.WithDefaults()
	queueID, itemID := patchQueueInputIDs(input.QueueID, input.ItemID, authority)
	if err := rejectVagueBindingLabel("patch_queue_id", queueID); err != nil {
		return PatchQueueItem{}, err
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", itemID); err != nil {
		return PatchQueueItem{}, err
	}
	if err := validateCASEvidence(input.CASResult, authority); err != nil {
		return PatchQueueItem{}, err
	}
	testEvidence, hasTestEvidence, err := normalizePatchQueueTestEvidence(input.TestEvidence)
	if err != nil {
		return PatchQueueItem{}, err
	}
	if hasTestEvidence && input.CASResult.Status != CASPatchStatusApplied {
		return PatchQueueItem{}, fmt.Errorf("patch queue test evidence requires applied CAS evidence, got %q", input.CASResult.Status)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := patchQueueKey(queueID, itemID)
	current, ok := s.items[key]
	if !ok {
		return PatchQueueItem{}, fmt.Errorf("patch queue item %s/%s not found", queueID, itemID)
	}
	if current.State != PatchQueueStateValidating {
		return PatchQueueItem{}, fmt.Errorf("illegal patch queue transition %s -> terminal", current.State)
	}
	if err := verifyPatchQueueItemContext(current, authority); err != nil {
		return PatchQueueItem{}, err
	}

	next := clonePatchQueueItem(current)
	next.CASResult = cloneCASPatchApplyResult(input.CASResult)
	next.CASPatchDigest = strings.TrimSpace(input.CASResult.PatchDigest)
	next.CASEvaluationDigest = digestCASPatchApplyResult(input.CASResult)
	next.TestEvidence = PatchQueueTestEvidence{}
	next.TestEvidenceDigest = ""
	if hasTestEvidence {
		next.TestEvidence = testEvidence
		next.TestEvidenceDigest = digestPatchQueueTestEvidence(testEvidence)
	}
	next.UpdatedAt = formatLeaseTime(normalizeLeaseTime(input.Now))

	switch input.CASResult.Status {
	case CASPatchStatusApplied:
		if hasTestEvidence && testEvidence.Status == PatchQueueTestStatusFailed {
			next.State = PatchQueueStateTestConflict
			break
		}
		if err := requireConcreteMutationOperationRefs(authority); err != nil {
			return PatchQueueItem{}, fmt.Errorf("applied patch queue item requires B1.7-ready refs: %w", err)
		}
		if _, err := BindMutationOperation(MutationOperationBindingInput{
			Context:       authority,
			LeaseStore:    input.LeaseStore,
			MutationPaths: current.Pathset,
			Now:           input.Now,
		}); err != nil {
			return PatchQueueItem{}, fmt.Errorf("applied patch queue item requires live mutation operation binding: %w", err)
		}
		next.State = PatchQueueStateApplied
		next.OperationID = strings.TrimSpace(authority.Operation.ID)
		next.OperationKind = strings.TrimSpace(authority.Operation.Kind)
	case CASPatchStatusConflict:
		next.State = PatchQueueStateConflict
		next.ConflictIssues = conflictIssuesFromCAS(input.CASResult)
	case CASPatchStatusFailed:
		next.State = PatchQueueStateFailed
	default:
		return PatchQueueItem{}, fmt.Errorf("unsupported CAS status %q", input.CASResult.Status)
	}

	s.items[key] = clonePatchQueueItem(next)
	return clonePatchQueueItem(next), nil
}

func (s *PatchQueueStore) RecordRecoveryDecision(input RecordPatchQueueRecoveryInput) (PatchQueueItem, error) {
	if s == nil {
		return PatchQueueItem{}, fmt.Errorf("patch queue store is required")
	}
	authority := input.Context.WithDefaults()
	queueID, itemID := patchQueueInputIDs(input.QueueID, input.ItemID, authority)
	if err := rejectVagueBindingLabel("patch_queue_id", queueID); err != nil {
		return PatchQueueItem{}, err
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", itemID); err != nil {
		return PatchQueueItem{}, err
	}
	reason, err := normalizePatchQueueRecoveryReason(input.Reason)
	if err != nil {
		return PatchQueueItem{}, err
	}
	if input.RetryDelay < 0 {
		return PatchQueueItem{}, fmt.Errorf("patch queue retry_delay must be non-negative")
	}
	now := normalizeLeaseTime(input.Now)

	s.mu.Lock()
	defer s.mu.Unlock()
	key := patchQueueKey(queueID, itemID)
	current, ok := s.items[key]
	if !ok {
		return PatchQueueItem{}, fmt.Errorf("patch queue item %s/%s not found", queueID, itemID)
	}
	if !patchQueueTerminalFailureState(current.State) {
		return PatchQueueItem{}, fmt.Errorf("illegal patch queue recovery decision from state %s", current.State)
	}
	if err := verifyPatchQueueItemContext(current, authority); err != nil {
		return PatchQueueItem{}, err
	}

	attempt := normalizePatchQueueAttempt(current.Attempt)
	maxAttempts := normalizePatchQueueMaxAttempts(current.MaxAttempts)
	decision := PatchQueueRecovery{
		Schema:      PatchQueueRecoverySchemaVersion,
		SourceState: current.State,
		Reason:      reason,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		RecordedAt:  formatLeaseTime(now),
	}

	next := clonePatchQueueItem(current)
	next.NextRetryAt = ""
	next.DeadLetteredAt = ""
	if attempt < maxAttempts {
		decision.Decision = PatchQueueRecoveryDecisionRetry
		decision.Retryable = true
		decision.NextRetryAt = formatLeaseTime(now.Add(input.RetryDelay))
		next.State = PatchQueueStateRetryPending
		next.NextRetryAt = decision.NextRetryAt
	} else {
		decision.Decision = PatchQueueRecoveryDecisionDeadLetter
		decision.Retryable = false
		decision.DeadLetteredAt = formatLeaseTime(now)
		next.State = PatchQueueStateDeadLetter
		next.DeadLetteredAt = decision.DeadLetteredAt
	}
	next.Attempt = attempt
	next.MaxAttempts = maxAttempts
	next.RecoveryDecisions = append(next.RecoveryDecisions, decision)
	next.RecoveryDecisionDigest = digestPatchQueueRecoveryDecisions(next.RecoveryDecisions)
	next.UpdatedAt = formatLeaseTime(now)
	s.items[key] = clonePatchQueueItem(next)
	return clonePatchQueueItem(next), nil
}

func (s *PatchQueueStore) RecordRollback(input RecordPatchQueueRollbackInput) (PatchQueueItem, error) {
	if s == nil {
		return PatchQueueItem{}, fmt.Errorf("patch queue store is required")
	}
	authority := input.Context.WithDefaults()
	queueID, itemID := patchQueueInputIDs(input.QueueID, input.ItemID, authority)
	if err := rejectVagueBindingLabel("patch_queue_id", queueID); err != nil {
		return PatchQueueItem{}, err
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", itemID); err != nil {
		return PatchQueueItem{}, err
	}
	if err := requireConcreteMutationOperationRefs(authority); err != nil {
		return PatchQueueItem{}, fmt.Errorf("rolled back patch queue item requires B1.7-ready refs: %w", err)
	}
	now := normalizeLeaseTime(input.Now)

	s.mu.Lock()
	defer s.mu.Unlock()
	key := patchQueueKey(queueID, itemID)
	current, ok := s.items[key]
	if !ok {
		return PatchQueueItem{}, fmt.Errorf("patch queue item %s/%s not found", queueID, itemID)
	}
	if current.State != PatchQueueStateApplied {
		return PatchQueueItem{}, fmt.Errorf("illegal patch queue rollback from state %s", current.State)
	}
	if err := verifyPatchQueueItemContext(current, authority); err != nil {
		return PatchQueueItem{}, err
	}
	evidence, err := normalizePatchQueueRollbackEvidence(input.Evidence, current, authority, now)
	if err != nil {
		return PatchQueueItem{}, err
	}
	if _, err := BindMutationOperation(MutationOperationBindingInput{
		Context:       authority,
		LeaseStore:    input.LeaseStore,
		MutationPaths: current.Pathset,
		Now:           input.Now,
	}); err != nil {
		return PatchQueueItem{}, fmt.Errorf("rolled back patch queue item requires live mutation operation binding: %w", err)
	}

	next := clonePatchQueueItem(current)
	next.State = PatchQueueStateRolledBack
	next.RollbackEvidence = evidence
	next.RollbackEvidenceDigest = digestPatchQueueRollbackEvidence(evidence)
	next.UpdatedAt = formatLeaseTime(now)
	s.items[key] = clonePatchQueueItem(next)
	return clonePatchQueueItem(next), nil
}

func (s *PatchQueueStore) Get(queueID, itemID string) (PatchQueueItem, bool) {
	if s == nil {
		return PatchQueueItem{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[patchQueueKey(queueID, itemID)]
	return clonePatchQueueItem(item), ok
}

func (s *PatchQueueStore) Snapshot() []PatchQueueItem {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PatchQueueItem, 0, len(s.items))
	for _, item := range s.items {
		out = append(out, clonePatchQueueItem(item))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].QueueID != out[j].QueueID {
			return out[i].QueueID < out[j].QueueID
		}
		return out[i].ItemID < out[j].ItemID
	})
	return out
}

func (s *PatchQueueStore) transition(input PatchQueueTransitionInput, nextState string, guard func(PatchQueueItem) error) (PatchQueueItem, error) {
	if s == nil {
		return PatchQueueItem{}, fmt.Errorf("patch queue store is required")
	}
	authority := input.Context.WithDefaults()
	queueID, itemID := patchQueueInputIDs(input.QueueID, input.ItemID, authority)
	if err := rejectVagueBindingLabel("patch_queue_id", queueID); err != nil {
		return PatchQueueItem{}, err
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", itemID); err != nil {
		return PatchQueueItem{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := patchQueueKey(queueID, itemID)
	current, ok := s.items[key]
	if !ok {
		return PatchQueueItem{}, fmt.Errorf("patch queue item %s/%s not found", queueID, itemID)
	}
	if err := verifyPatchQueueItemContext(current, authority); err != nil {
		return PatchQueueItem{}, err
	}
	if guard != nil {
		if err := guard(current); err != nil {
			return PatchQueueItem{}, err
		}
	} else if current.State != PatchQueueStateProposed || nextState != PatchQueueStateValidating {
		return PatchQueueItem{}, fmt.Errorf("illegal patch queue transition %s -> %s", current.State, nextState)
	}
	next := clonePatchQueueItem(current)
	next.State = nextState
	next.UpdatedAt = formatLeaseTime(normalizeLeaseTime(input.Now))
	s.items[key] = clonePatchQueueItem(next)
	return clonePatchQueueItem(next), nil
}

func requirePatchQueueProposalRefs(authority Context) error {
	if strings.TrimSpace(authority.Operation.ID) != "" || strings.TrimSpace(authority.Operation.Kind) != "" {
		return fmt.Errorf("patch queue proposal context must not attach operation refs")
	}
	if err := authority.Validate(); err != nil {
		return fmt.Errorf("repo authority context: %w", err)
	}
	if err := rejectVagueBindingLabel("repo_lease_id", authority.Lease.ID); err != nil {
		return err
	}
	if authority.Lease.Term <= 0 {
		return fmt.Errorf("lease_term is required")
	}
	if err := rejectVagueBindingLabel("patch_queue_id", authority.PatchQueue.QueueID); err != nil {
		return err
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", authority.PatchQueue.ItemID); err != nil {
		return err
	}
	return nil
}

func verifyPatchQueueLeaseAuthority(authority Context, store *FileLeaseStore, now time.Time) error {
	if store == nil {
		return fmt.Errorf("file lease store is required")
	}
	lease, err := findBindingLease(store, authority.Lease.ID)
	if err != nil {
		return err
	}
	if err := verifyMutationBindingContext(authority, lease); err != nil {
		return err
	}
	if err := verifyLeaseAcquisitionContextDigest(authority, lease); err != nil {
		return err
	}
	return store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: authority.Lease.ID,
		Term:    authority.Lease.Term,
		Holder:  HolderForLease(lease),
		Paths:   authority.Pathset,
		Now:     now,
	})
}

func verifyPatchQueueItemContext(item PatchQueueItem, authority Context) error {
	queueDigest, err := patchQueueContextDigest(authority)
	if err != nil {
		return err
	}
	if queueDigest != item.ContextDigest {
		return fmt.Errorf("patch queue context digest mismatch: got %q want %q", queueDigest, item.ContextDigest)
	}
	if strings.TrimSpace(authority.PatchQueue.QueueID) != item.QueueID {
		return fmt.Errorf("patch_queue_id mismatch: got %q want %q", authority.PatchQueue.QueueID, item.QueueID)
	}
	if strings.TrimSpace(authority.PatchQueue.ItemID) != item.ItemID {
		return fmt.Errorf("patch_queue_item_id mismatch: got %q want %q", authority.PatchQueue.ItemID, item.ItemID)
	}
	if strings.TrimSpace(authority.Lease.ID) != item.RepoLeaseID {
		return fmt.Errorf("repo_lease_id mismatch: got %q want %q", authority.Lease.ID, item.RepoLeaseID)
	}
	if authority.Lease.Term != item.LeaseTerm {
		return fmt.Errorf("lease_term mismatch: got %d want %d", authority.Lease.Term, item.LeaseTerm)
	}
	pathset, err := NormalizePathSet(authority.Pathset)
	if err != nil {
		return fmt.Errorf("patch queue context pathset: %w", err)
	}
	if !sameStringSlice(pathset, item.Pathset) {
		return fmt.Errorf("patch queue pathset mismatch: got %#v want %#v", pathset, item.Pathset)
	}
	return nil
}

func validateCASEvidence(result CASPatchApplyResult, authority Context) error {
	if strings.TrimSpace(result.Schema) != CASPatchApplySchemaVersion {
		return fmt.Errorf("CAS evidence schema is required")
	}
	switch result.Status {
	case CASPatchStatusApplied, CASPatchStatusConflict, CASPatchStatusFailed:
	default:
		return fmt.Errorf("CAS evidence status %q is not supported", result.Status)
	}
	fullDigest, err := authority.Digest()
	if err != nil {
		return fmt.Errorf("CAS evidence context: %w", err)
	}
	if strings.TrimSpace(result.ContextDigest) != "" && strings.TrimSpace(result.ContextDigest) != fullDigest {
		return fmt.Errorf("CAS evidence context digest mismatch: got %q want %q", result.ContextDigest, fullDigest)
	}
	pathset, err := NormalizePathSet(authority.Pathset)
	if err != nil {
		return fmt.Errorf("CAS evidence context pathset: %w", err)
	}
	pathResults := make(map[string]CASPatchPathResult, len(result.Paths))
	for _, pathResult := range result.Paths {
		normalized, err := normalizeResolverPath(pathResult.Path)
		if err != nil {
			return fmt.Errorf("CAS evidence path %q is invalid: %w", pathResult.Path, err)
		}
		if normalized != pathResult.Path {
			return fmt.Errorf("CAS evidence path is not normalized: got %q want %q", pathResult.Path, normalized)
		}
		switch pathResult.Status {
		case CASPatchStatusApplied, CASPatchStatusConflict, CASPatchStatusFailed:
		default:
			return fmt.Errorf("CAS evidence path %q has unsupported status %q", pathResult.Path, pathResult.Status)
		}
		if !pathsetCoversPath(pathset, pathResult.Path) {
			return fmt.Errorf("CAS evidence path %q is outside patch queue context", pathResult.Path)
		}
		if _, exists := pathResults[pathResult.Path]; exists {
			return fmt.Errorf("CAS evidence path %q is duplicated", pathResult.Path)
		}
		changeKind, err := validateCASPathChangeKind(pathResult)
		if err != nil {
			return err
		}
		pathResult.ChangeKind = strings.TrimSpace(pathResult.ChangeKind)
		baseHash := strings.TrimSpace(authority.Base.FileHashes[pathResult.Path])
		switch changeKind {
		case CASPatchChangeAdd:
			if baseHash != "" {
				return fmt.Errorf("CAS evidence added path %q already has authority base hash", pathResult.Path)
			}
			if strings.TrimSpace(pathResult.BaseHash) != "" || strings.TrimSpace(pathResult.CurrentHash) != "" {
				return fmt.Errorf("CAS evidence added path %q must not carry base or current hashes", pathResult.Path)
			}
			if strings.TrimSpace(pathResult.CandidateHash) == "" {
				return fmt.Errorf("CAS evidence added path %q requires candidate hash", pathResult.Path)
			}
			if pathResult.Status == CASPatchStatusConflict {
				return fmt.Errorf("CAS evidence added path %q cannot be a base-drift conflict", pathResult.Path)
			}
		case CASPatchChangeModify:
			if baseHash == "" {
				return fmt.Errorf("CAS evidence path %q has no authority base hash", pathResult.Path)
			}
			if strings.TrimSpace(pathResult.BaseHash) != baseHash {
				return fmt.Errorf("CAS evidence path %q base hash mismatch: got %q want %q", pathResult.Path, pathResult.BaseHash, baseHash)
			}
			if pathResult.Status == CASPatchStatusConflict {
				if strings.TrimSpace(pathResult.CurrentHash) == "" || strings.TrimSpace(pathResult.CandidateHash) == "" {
					return fmt.Errorf("CAS conflict path %q requires current and candidate hashes", pathResult.Path)
				}
				if strings.TrimSpace(pathResult.CurrentHash) == strings.TrimSpace(pathResult.BaseHash) {
					return fmt.Errorf("CAS conflict path %q does not show base drift", pathResult.Path)
				}
			}
		}
		pathResults[pathResult.Path] = pathResult
	}
	for _, issue := range result.Issues {
		if err := validateCASIssueEvidence(issue, result.Status, pathset, pathResults); err != nil {
			return err
		}
	}
	switch result.Status {
	case CASPatchStatusApplied:
		if strings.TrimSpace(result.ContextDigest) == "" {
			return fmt.Errorf("applied CAS evidence requires context digest")
		}
		if strings.TrimSpace(result.PatchDigest) == "" {
			return fmt.Errorf("applied CAS evidence requires patch digest")
		}
		if len(result.Paths) == 0 {
			return fmt.Errorf("applied CAS evidence requires path results")
		}
		if len(result.Issues) != 0 {
			return fmt.Errorf("applied CAS evidence must not contain issues")
		}
		if err := verifyCASAppliedPathsetCoverage(pathset, pathResults); err != nil {
			return err
		}
		for _, pathResult := range pathResults {
			if pathResult.Status != CASPatchStatusApplied {
				return fmt.Errorf("applied CAS evidence path %q has non-applied status %q", pathResult.Path, pathResult.Status)
			}
			if err := validateCASAppliedPathHashes(pathResult); err != nil {
				return err
			}
		}
		if err := verifyCASPatchDigestFromPathResults(result); err != nil {
			return err
		}
	case CASPatchStatusConflict:
		if strings.TrimSpace(result.ContextDigest) == "" {
			return fmt.Errorf("conflict CAS evidence requires context digest")
		}
		if strings.TrimSpace(result.PatchDigest) == "" {
			return fmt.Errorf("conflict CAS evidence requires patch digest")
		}
		if len(conflictIssuesFromCAS(result)) == 0 {
			return fmt.Errorf("conflict CAS evidence requires conflict issues")
		}
		if err := verifyCASConflictPathIssueConsistency(result, pathResults); err != nil {
			return err
		}
		if err := verifyCASPatchDigestFromPathResults(result); err != nil {
			return err
		}
	case CASPatchStatusFailed:
		if len(result.Issues) == 0 {
			return fmt.Errorf("failed CAS evidence requires issues")
		}
	}
	return nil
}

func verifyCASAppliedPathsetCoverage(pathset []string, pathResults map[string]CASPatchPathResult) error {
	for _, path := range pathset {
		if pathsetEntryIsScoped(path) {
			continue
		}
		if _, ok := pathResults[path]; !ok {
			return fmt.Errorf("applied CAS evidence missing path %q from patch queue pathset", path)
		}
	}
	return nil
}

func verifyCASConflictPathIssueConsistency(result CASPatchApplyResult, pathResults map[string]CASPatchPathResult) error {
	conflictPaths := make(map[string]struct{})
	for path, pathResult := range pathResults {
		switch pathResult.Status {
		case CASPatchStatusApplied:
			if err := validateCASAppliedPathHashes(pathResult); err != nil {
				return fmt.Errorf("conflict CAS evidence applied path result %q: %w", path, err)
			}
		case CASPatchStatusFailed:
			return fmt.Errorf("conflict CAS evidence cannot contain failed path result %q", path)
		case CASPatchStatusConflict:
			conflictPaths[path] = struct{}{}
		}
	}

	issuePaths := make(map[string]struct{})
	for _, issue := range result.Issues {
		if issue.Status != CASPatchStatusConflict || issue.Kind != CASPatchIssueBaseDrift {
			continue
		}
		path := strings.TrimSpace(issue.Path)
		if _, exists := issuePaths[path]; exists {
			return fmt.Errorf("conflict CAS evidence has duplicate base_drift issue for %q", path)
		}
		issuePaths[path] = struct{}{}
	}
	if len(conflictPaths) == 0 {
		return fmt.Errorf("conflict CAS evidence requires conflict path results")
	}
	if len(conflictPaths) != len(issuePaths) {
		return fmt.Errorf("conflict CAS evidence conflict path/result issue count mismatch")
	}
	for path := range conflictPaths {
		if _, ok := issuePaths[path]; !ok {
			return fmt.Errorf("conflict CAS evidence path %q requires matching base_drift issue", path)
		}
	}
	return nil
}

func validateCASPathChangeKind(pathResult CASPatchPathResult) (string, error) {
	changeKind := casPatchPathChangeKind(pathResult)
	switch changeKind {
	case CASPatchChangeModify, CASPatchChangeAdd:
		return changeKind, nil
	default:
		return "", fmt.Errorf("CAS evidence path %q has unsupported change_kind %q", pathResult.Path, pathResult.ChangeKind)
	}
}

func validateCASAppliedPathHashes(pathResult CASPatchPathResult) error {
	changeKind, err := validateCASPathChangeKind(pathResult)
	if err != nil {
		return err
	}
	switch changeKind {
	case CASPatchChangeAdd:
		if strings.TrimSpace(pathResult.BaseHash) != "" || strings.TrimSpace(pathResult.CurrentHash) != "" {
			return fmt.Errorf("applied CAS evidence added path %q must not carry base or current hashes", pathResult.Path)
		}
		if strings.TrimSpace(pathResult.CandidateHash) == "" {
			return fmt.Errorf("applied CAS evidence added path %q requires candidate hash", pathResult.Path)
		}
	case CASPatchChangeModify:
		if strings.TrimSpace(pathResult.CurrentHash) == "" || strings.TrimSpace(pathResult.CandidateHash) == "" {
			return fmt.Errorf("applied CAS evidence path %q requires current and candidate hashes", pathResult.Path)
		}
		if strings.TrimSpace(pathResult.CurrentHash) != strings.TrimSpace(pathResult.BaseHash) {
			return fmt.Errorf("applied CAS evidence path %q current hash must match base hash", pathResult.Path)
		}
	}
	return nil
}

func verifyCASPatchDigestFromPathResults(result CASPatchApplyResult) error {
	candidates := make([]casPatchCandidateEntry, 0, len(result.Paths))
	for _, pathResult := range result.Paths {
		candidateHash := strings.TrimSpace(pathResult.CandidateHash)
		if candidateHash == "" {
			return fmt.Errorf("CAS evidence path %q candidate hash is required for patch digest", pathResult.Path)
		}
		candidates = append(candidates, casPatchCandidateEntry{
			path: pathResult.Path,
			hash: candidateHash,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].path < candidates[j].path
	})
	expected := digestCASPatchCandidates(candidates)
	if strings.TrimSpace(result.PatchDigest) != expected {
		return fmt.Errorf("CAS evidence patch digest mismatch: got %q want %q", result.PatchDigest, expected)
	}
	return nil
}

func validateCASIssueEvidence(issue CASPatchIssue, resultStatus string, pathset []string, pathResults map[string]CASPatchPathResult) error {
	status := strings.TrimSpace(issue.Status)
	kind := strings.TrimSpace(issue.Kind)
	switch status {
	case CASPatchStatusConflict, CASPatchStatusFailed:
	default:
		return fmt.Errorf("CAS issue has unsupported status %q", issue.Status)
	}
	if !casPatchIssueKindAllowed(kind) {
		return fmt.Errorf("CAS issue kind %q is not supported", issue.Kind)
	}
	if resultStatus == CASPatchStatusConflict {
		if status != CASPatchStatusConflict {
			return fmt.Errorf("conflict CAS evidence cannot contain %q issue", status)
		}
		if kind != CASPatchIssueBaseDrift {
			return fmt.Errorf("conflict CAS evidence issue kind %q is not supported", kind)
		}
	}

	path := strings.TrimSpace(issue.Path)
	if status == CASPatchStatusConflict || kind == CASPatchIssueBaseDrift {
		if path == "" {
			return fmt.Errorf("CAS conflict issue %q requires path", kind)
		}
	}
	if path != "" {
		normalized, err := normalizeResolverPath(path)
		if err != nil {
			return fmt.Errorf("CAS issue path %q is invalid: %w", path, err)
		}
		if normalized != path {
			return fmt.Errorf("CAS issue path is not normalized: got %q want %q", path, normalized)
		}
		if !pathsetCoversPath(pathset, path) {
			return fmt.Errorf("CAS issue path %q is outside patch queue context", path)
		}
	}
	if kind == CASPatchIssueBaseDrift {
		if strings.TrimSpace(issue.ExpectedHash) == "" || strings.TrimSpace(issue.ActualHash) == "" || strings.TrimSpace(issue.CandidateHash) == "" {
			return fmt.Errorf("CAS base_drift issue for %q requires expected, actual and candidate hashes", path)
		}
		pathResult, ok := pathResults[path]
		if !ok || pathResult.Status != CASPatchStatusConflict {
			return fmt.Errorf("CAS base_drift issue for %q requires matching conflict path result", path)
		}
		if strings.TrimSpace(issue.ExpectedHash) != strings.TrimSpace(pathResult.BaseHash) ||
			strings.TrimSpace(issue.ActualHash) != strings.TrimSpace(pathResult.CurrentHash) ||
			strings.TrimSpace(issue.CandidateHash) != strings.TrimSpace(pathResult.CandidateHash) {
			return fmt.Errorf("CAS base_drift issue for %q hashes do not match conflict path result", path)
		}
		if strings.TrimSpace(pathResult.CurrentHash) == strings.TrimSpace(pathResult.BaseHash) {
			return fmt.Errorf("CAS base_drift issue for %q does not show base drift", path)
		}
	}
	pathResult, ok := pathResults[path]
	if status == CASPatchStatusConflict && (!ok || pathResult.Status != CASPatchStatusConflict) {
		return fmt.Errorf("CAS conflict issue for %q requires matching conflict path result", path)
	}
	return nil
}

func casPatchIssueKindAllowed(kind string) bool {
	switch kind {
	case CASPatchIssueContextInvalid,
		CASPatchIssueCandidateHashesRequired,
		CASPatchIssueCandidatePathInvalid,
		CASPatchIssueCandidatePathUnstable,
		CASPatchIssueCandidatePathDuplicate,
		CASPatchIssueCandidateHashMissing,
		CASPatchIssuePathOutsideContext,
		CASPatchIssueBaseHashMissing,
		CASPatchIssueCurrentHashMissing,
		CASPatchIssueBaseDrift:
		return true
	default:
		return false
	}
}

func normalizePatchQueueTestEvidence(evidence PatchQueueTestEvidence) (PatchQueueTestEvidence, bool, error) {
	if !hasPatchQueueTestEvidence(evidence) {
		return PatchQueueTestEvidence{}, false, nil
	}
	evidence.Schema = strings.TrimSpace(evidence.Schema)
	if evidence.Schema == "" {
		evidence.Schema = PatchQueueTestEvidenceSchemaVersion
	}
	evidence.Name = strings.TrimSpace(evidence.Name)
	evidence.Command = strings.TrimSpace(evidence.Command)
	evidence.Status = strings.TrimSpace(evidence.Status)
	evidence.OutputDigest = strings.TrimSpace(evidence.OutputDigest)
	evidence.OutputSummary = strings.TrimSpace(evidence.OutputSummary)
	if err := validatePatchQueueTestEvidence(evidence); err != nil {
		return PatchQueueTestEvidence{}, true, err
	}
	return evidence, true, nil
}

func hasPatchQueueTestEvidence(evidence PatchQueueTestEvidence) bool {
	return strings.TrimSpace(evidence.Schema) != "" ||
		strings.TrimSpace(evidence.Name) != "" ||
		strings.TrimSpace(evidence.Command) != "" ||
		strings.TrimSpace(evidence.Status) != "" ||
		evidence.ExitCode != 0 ||
		strings.TrimSpace(evidence.OutputDigest) != "" ||
		strings.TrimSpace(evidence.OutputSummary) != "" ||
		evidence.DurationMillis != 0
}

func validatePatchQueueTestEvidence(evidence PatchQueueTestEvidence) error {
	if evidence.Schema != PatchQueueTestEvidenceSchemaVersion {
		return fmt.Errorf("test evidence schema must be %q", PatchQueueTestEvidenceSchemaVersion)
	}
	for field, value := range map[string]string{
		"name":          evidence.Name,
		"command":       evidence.Command,
		"status":        evidence.Status,
		"output_digest": evidence.OutputDigest,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("test evidence %s is required", field)
		}
	}
	if !isCanonicalSHA256Digest(evidence.OutputDigest) {
		return fmt.Errorf("test evidence output_digest must be a canonical sha256 digest")
	}
	if len([]byte(evidence.OutputSummary)) > patchQueueTestOutputSummaryMaxBytes {
		return fmt.Errorf("test evidence output_summary exceeds %d bytes", patchQueueTestOutputSummaryMaxBytes)
	}
	if evidence.DurationMillis < 0 {
		return fmt.Errorf("test evidence duration_ms must be non-negative")
	}
	switch evidence.Status {
	case PatchQueueTestStatusPassed:
		if evidence.ExitCode != 0 {
			return fmt.Errorf("passed test evidence exit_code must be 0")
		}
	case PatchQueueTestStatusFailed:
		if evidence.ExitCode == 0 {
			return fmt.Errorf("failed test evidence exit_code must be non-zero")
		}
	default:
		return fmt.Errorf("test evidence status %q is not supported", evidence.Status)
	}
	return nil
}

func normalizePatchQueueRollbackEvidence(evidence PatchQueueRollback, current PatchQueueItem, authority Context, now time.Time) (PatchQueueRollback, error) {
	evidence.Schema = strings.TrimSpace(evidence.Schema)
	if evidence.Schema == "" {
		evidence.Schema = PatchQueueRollbackSchemaVersion
	}
	evidence.SourceOperationID = strings.TrimSpace(evidence.SourceOperationID)
	evidence.SourceOperationKind = strings.TrimSpace(evidence.SourceOperationKind)
	evidence.RollbackOperationID = strings.TrimSpace(evidence.RollbackOperationID)
	evidence.RollbackOperationKind = strings.TrimSpace(evidence.RollbackOperationKind)
	evidence.Reason = strings.TrimSpace(evidence.Reason)
	evidence.SourcePatchDigest = strings.TrimSpace(evidence.SourcePatchDigest)
	evidence.RollbackPatchDigest = strings.TrimSpace(evidence.RollbackPatchDigest)
	evidence.VerificationCommand = strings.TrimSpace(evidence.VerificationCommand)
	evidence.VerificationStatus = strings.TrimSpace(evidence.VerificationStatus)
	evidence.VerificationOutputDigest = strings.TrimSpace(evidence.VerificationOutputDigest)
	evidence.VerificationOutputSummary = strings.TrimSpace(evidence.VerificationOutputSummary)

	if evidence.SourceOperationID == "" {
		evidence.SourceOperationID = current.OperationID
	}
	if evidence.SourceOperationKind == "" {
		evidence.SourceOperationKind = current.OperationKind
	}
	if evidence.RollbackOperationID == "" {
		evidence.RollbackOperationID = authority.Operation.ID
	}
	if evidence.RollbackOperationKind == "" {
		evidence.RollbackOperationKind = authority.Operation.Kind
	}
	if evidence.SourcePatchDigest == "" {
		evidence.SourcePatchDigest = current.CASPatchDigest
	}
	evidence.RecordedAt = formatLeaseTime(now)
	rollbackPaths, rollbackPatchDigest, err := normalizePatchQueueRollbackPaths(evidence.RollbackPaths, current)
	if err != nil {
		return PatchQueueRollback{}, err
	}
	evidence.RollbackPaths = rollbackPaths
	if evidence.RollbackPatchDigest == "" {
		evidence.RollbackPatchDigest = rollbackPatchDigest
	}

	if evidence.Schema != PatchQueueRollbackSchemaVersion {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence schema must be %q", PatchQueueRollbackSchemaVersion)
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
	} {
		if strings.TrimSpace(value) == "" {
			return PatchQueueRollback{}, fmt.Errorf("rollback evidence %s is required", field)
		}
	}
	if evidence.SourceOperationID != current.OperationID || evidence.SourceOperationKind != current.OperationKind {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence source operation must match applied patch queue item")
	}
	if evidence.RollbackOperationID != authority.Operation.ID || evidence.RollbackOperationKind != authority.Operation.Kind {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence operation must match rollback context")
	}
	if evidence.RollbackOperationID == evidence.SourceOperationID {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence rollback operation must be distinct from source operation")
	}
	if evidence.SourcePatchDigest != current.CASPatchDigest {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence source_patch_digest must match applied patch digest")
	}
	if !isCanonicalSHA256Digest(evidence.SourcePatchDigest) {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence source_patch_digest must be a canonical sha256 digest")
	}
	if !isCanonicalSHA256Digest(evidence.RollbackPatchDigest) {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence rollback_patch_digest must be a canonical sha256 digest")
	}
	if evidence.RollbackPatchDigest != rollbackPatchDigest {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence rollback_patch_digest mismatch: got %q want %q", evidence.RollbackPatchDigest, rollbackPatchDigest)
	}
	if !isCanonicalSHA256Digest(evidence.VerificationOutputDigest) {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence verification_output_digest must be a canonical sha256 digest")
	}
	if len([]byte(evidence.Reason)) > patchQueueRecoveryReasonMaxBytes {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence reason exceeds %d bytes", patchQueueRecoveryReasonMaxBytes)
	}
	if len([]byte(evidence.VerificationOutputSummary)) > patchQueueTestOutputSummaryMaxBytes {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence verification_output_summary exceeds %d bytes", patchQueueTestOutputSummaryMaxBytes)
	}
	if evidence.VerificationDurationMillis < 0 {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence verification_duration_ms must be non-negative")
	}
	if evidence.VerificationStatus != PatchQueueTestStatusPassed {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence verification_status must be %q", PatchQueueTestStatusPassed)
	}
	if evidence.VerificationExitCode != 0 {
		return PatchQueueRollback{}, fmt.Errorf("rollback evidence verification_exit_code must be 0")
	}
	return evidence, nil
}

func NormalizePatchQueueRollbackEvidence(evidence PatchQueueRollback, current PatchQueueItem, rollbackOperation OperationRef, now time.Time) (PatchQueueRollback, error) {
	return normalizePatchQueueRollbackEvidence(evidence, current, Context{
		Operation: OperationRef{
			ID:   strings.TrimSpace(rollbackOperation.ID),
			Kind: strings.TrimSpace(rollbackOperation.Kind),
		},
	}, now)
}

func normalizePatchQueueRollbackPaths(paths []PatchQueueRollbackPath, current PatchQueueItem) ([]PatchQueueRollbackPath, string, error) {
	if current.CASResult.Status != CASPatchStatusApplied {
		return nil, "", fmt.Errorf("rollback evidence requires applied CAS result")
	}
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("rollback evidence rollback_paths are required")
	}
	normalizedPathset, err := NormalizePathSet(current.Pathset)
	if err != nil {
		return nil, "", fmt.Errorf("rollback evidence pathset: %w", err)
	}
	appliedPaths := make(map[string]CASPatchPathResult, len(current.CASResult.Paths))
	for i, pathResult := range current.CASResult.Paths {
		path, err := NormalizePath(pathResult.Path)
		if err != nil {
			return nil, "", fmt.Errorf("rollback evidence CAS path[%d]: %w", i, err)
		}
		if pathResult.Path != path {
			return nil, "", fmt.Errorf("rollback evidence CAS path[%d] is not normalized: got %q want %q", i, pathResult.Path, path)
		}
		if pathResult.Status != CASPatchStatusApplied {
			return nil, "", fmt.Errorf("rollback evidence CAS path %q is not applied", pathResult.Path)
		}
		if !pathsetCoversPath(normalizedPathset, pathResult.Path) {
			return nil, "", fmt.Errorf("rollback evidence CAS path %q is outside patch queue pathset", pathResult.Path)
		}
		if _, exists := appliedPaths[pathResult.Path]; exists {
			return nil, "", fmt.Errorf("rollback evidence CAS path %q is duplicated", pathResult.Path)
		}
		appliedPaths[pathResult.Path] = pathResult
	}

	out := make([]PatchQueueRollbackPath, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	candidates := make([]casPatchCandidateEntry, 0, len(paths))
	for i, pathEvidence := range paths {
		pathEvidence.Path = strings.TrimSpace(pathEvidence.Path)
		pathEvidence.SourceBaseHash = strings.TrimSpace(pathEvidence.SourceBaseHash)
		pathEvidence.SourceAppliedHash = strings.TrimSpace(pathEvidence.SourceAppliedHash)
		pathEvidence.RollbackCandidateHash = strings.TrimSpace(pathEvidence.RollbackCandidateHash)

		normalizedPath, err := NormalizePath(pathEvidence.Path)
		if err != nil {
			return nil, "", fmt.Errorf("rollback evidence rollback_paths[%d] path: %w", i, err)
		}
		if normalizedPath != pathEvidence.Path {
			return nil, "", fmt.Errorf("rollback evidence rollback_paths[%d] path is not normalized: got %q want %q", i, pathEvidence.Path, normalizedPath)
		}
		if !pathsetCoversPath(normalizedPathset, pathEvidence.Path) {
			return nil, "", fmt.Errorf("rollback evidence path %q is outside patch queue pathset", pathEvidence.Path)
		}
		if _, exists := seen[pathEvidence.Path]; exists {
			return nil, "", fmt.Errorf("rollback evidence duplicate path %q", pathEvidence.Path)
		}
		seen[pathEvidence.Path] = struct{}{}

		appliedPath, ok := appliedPaths[pathEvidence.Path]
		if !ok || appliedPath.Status != CASPatchStatusApplied {
			return nil, "", fmt.Errorf("rollback evidence path %q requires matching applied CAS path", pathEvidence.Path)
		}
		if casPatchPathChangeKind(appliedPath) == CASPatchChangeAdd {
			return nil, "", fmt.Errorf("rollback evidence for added path %q requires deletion rollback support", pathEvidence.Path)
		}
		if pathEvidence.SourceBaseHash == "" {
			pathEvidence.SourceBaseHash = appliedPath.BaseHash
		}
		if pathEvidence.SourceAppliedHash == "" {
			pathEvidence.SourceAppliedHash = appliedPath.CandidateHash
		}
		if pathEvidence.RollbackCandidateHash == "" {
			pathEvidence.RollbackCandidateHash = appliedPath.BaseHash
		}
		if pathEvidence.SourceBaseHash != appliedPath.BaseHash {
			return nil, "", fmt.Errorf("rollback evidence path %q source_base_hash mismatch", pathEvidence.Path)
		}
		if pathEvidence.SourceAppliedHash != appliedPath.CandidateHash {
			return nil, "", fmt.Errorf("rollback evidence path %q source_applied_hash mismatch", pathEvidence.Path)
		}
		if pathEvidence.RollbackCandidateHash != appliedPath.BaseHash {
			return nil, "", fmt.Errorf("rollback evidence path %q rollback_candidate_hash must restore source base hash", pathEvidence.Path)
		}
		out = append(out, pathEvidence)
		candidates = append(candidates, casPatchCandidateEntry{
			path: pathEvidence.Path,
			hash: pathEvidence.RollbackCandidateHash,
		})
	}
	for path := range appliedPaths {
		if _, ok := seen[path]; !ok {
			return nil, "", fmt.Errorf("rollback evidence missing applied CAS path %q", path)
		}
	}
	for _, path := range normalizedPathset {
		if pathsetEntryIsScoped(path) {
			continue
		}
		if _, ok := seen[path]; !ok {
			return nil, "", fmt.Errorf("rollback evidence must cover every patch queue path")
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].path < candidates[j].path
	})
	return out, digestCASPatchCandidates(candidates), nil
}

func normalizePatchQueueRecoveryReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "", fmt.Errorf("patch queue recovery reason is required")
	}
	if len([]byte(reason)) > patchQueueRecoveryReasonMaxBytes {
		return "", fmt.Errorf("patch queue recovery reason exceeds %d bytes", patchQueueRecoveryReasonMaxBytes)
	}
	return reason, nil
}

func normalizePatchQueueAttempt(attempt int) int {
	if attempt <= 0 {
		return 1
	}
	return attempt
}

func normalizePatchQueueMaxAttempts(maxAttempts int) int {
	if maxAttempts <= 0 {
		return 1
	}
	return maxAttempts
}

func patchQueueTerminalFailureState(state string) bool {
	switch strings.TrimSpace(state) {
	case PatchQueueStateFailed, PatchQueueStateConflict, PatchQueueStateTestConflict:
		return true
	default:
		return false
	}
}

func isCanonicalSHA256Digest(value string) bool {
	value = strings.TrimSpace(value)
	const prefix = "sha256:"
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	hexDigest := strings.TrimPrefix(value, prefix)
	if len(hexDigest) != 64 {
		return false
	}
	for _, ch := range hexDigest {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func patchQueueContextDigest(authority Context) (string, error) {
	authority = authority.WithDefaults()
	authority.Operation = OperationRef{}
	digest, err := authority.Digest()
	if err != nil {
		return "", fmt.Errorf("patch queue context digest: %w", err)
	}
	return digest, nil
}

func patchQueueInputIDs(queueID, itemID string, authority Context) (string, string) {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	if queueID == "" {
		queueID = strings.TrimSpace(authority.PatchQueue.QueueID)
	}
	if itemID == "" {
		itemID = strings.TrimSpace(authority.PatchQueue.ItemID)
	}
	return queueID, itemID
}

func patchQueueKey(queueID, itemID string) string {
	return strings.TrimSpace(queueID) + "\x00" + strings.TrimSpace(itemID)
}

func buildPatchQueueItemID(queueID, itemID, contextDigest string) string {
	shortDigest := strings.TrimPrefix(contextDigest, "sha256:")
	if len(shortDigest) > 12 {
		shortDigest = shortDigest[:12]
	}
	return strings.Join([]string{
		"patchqitem",
		"v1",
		cleanKeyPart(queueID),
		cleanKeyPart(itemID),
		shortDigest,
	}, ":")
}

func clonePatchQueueItem(item PatchQueueItem) PatchQueueItem {
	item.Pathset = append([]string(nil), item.Pathset...)
	item.CASResult = cloneCASPatchApplyResult(item.CASResult)
	item.ConflictIssues = cloneCASPatchIssues(item.ConflictIssues)
	item.RecoveryDecisions = append([]PatchQueueRecovery(nil), item.RecoveryDecisions...)
	item.RollbackEvidence.RollbackPaths = append([]PatchQueueRollbackPath(nil), item.RollbackEvidence.RollbackPaths...)
	return item
}

func cloneCASPatchApplyResult(result CASPatchApplyResult) CASPatchApplyResult {
	result.Paths = append([]CASPatchPathResult(nil), result.Paths...)
	result.Issues = cloneCASPatchIssues(result.Issues)
	return result
}

func cloneCASPatchIssues(issues []CASPatchIssue) []CASPatchIssue {
	return append([]CASPatchIssue(nil), issues...)
}

func conflictIssuesFromCAS(result CASPatchApplyResult) []CASPatchIssue {
	issues := make([]CASPatchIssue, 0, len(result.Issues))
	for _, issue := range result.Issues {
		if issue.Status == CASPatchStatusConflict {
			issues = append(issues, issue)
		}
	}
	return issues
}

func digestCASPatchApplyResult(result CASPatchApplyResult) string {
	result = cloneCASPatchApplyResult(result)
	raw, _ := json.Marshal(result)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PatchQueueCASEvaluationDigest(result CASPatchApplyResult) string {
	return digestCASPatchApplyResult(result)
}

func digestPatchQueueTestEvidence(evidence PatchQueueTestEvidence) string {
	raw, _ := json.Marshal(evidence)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PatchQueueTestEvidenceDigest(evidence PatchQueueTestEvidence) string {
	return digestPatchQueueTestEvidence(evidence)
}

func digestPatchQueueRecoveryDecisions(decisions []PatchQueueRecovery) string {
	raw, _ := json.Marshal(decisions)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func digestPatchQueueRollbackEvidence(evidence PatchQueueRollback) string {
	raw, _ := json.Marshal(evidence)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PatchQueueRollbackEvidenceDigest(evidence PatchQueueRollback) string {
	return digestPatchQueueRollbackEvidence(evidence)
}

func digestPatchQueueReviewerAdvisory(evidence PatchQueueReviewerAdvisory) string {
	raw, _ := json.Marshal(evidence)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PatchQueueReviewerAdvisoryDigest(evidence PatchQueueReviewerAdvisory) string {
	return digestPatchQueueReviewerAdvisory(evidence)
}

func digestPatchQueueOperatorEnablement(evidence PatchQueueOperatorEnablement) string {
	raw, _ := json.Marshal(evidence)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PatchQueueOperatorEnablementDigest(evidence PatchQueueOperatorEnablement) string {
	return digestPatchQueueOperatorEnablement(evidence)
}
