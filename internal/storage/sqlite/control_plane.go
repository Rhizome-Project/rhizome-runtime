package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

var ErrOperatorQueueItemNotFound = errors.New("operator queue item not found")

type RuntimeEventInput struct {
	EventID                        string
	DedupKey                       string
	WorkspaceID                    string
	EventType                      string
	EntityType                     string
	EntityID                       string
	ActorType                      string
	ActorID                        string
	AgentID                        string
	SessionID                      string
	TaskID                         string
	RootCauseID                    string
	ProvenanceGroupID              string
	ParentRefsJSON                 string
	PayloadJSON                    string
	CreatedAt                      string
	AuthorityHolderNodeID          string
	AuthorityTerm                  int64
	AuthorityLeaseTokenFingerprint string
}

type RuntimeEventRecord struct {
	EventID                        string `json:"event_id"`
	DedupKey                       string `json:"dedup_key,omitempty"`
	WorkspaceID                    string `json:"workspace_id"`
	EventType                      string `json:"event_type"`
	EntityType                     string `json:"entity_type"`
	EntityID                       string `json:"entity_id"`
	ActorType                      string `json:"actor_type,omitempty"`
	ActorID                        string `json:"actor_id,omitempty"`
	AgentID                        string `json:"agent_id,omitempty"`
	SessionID                      string `json:"session_id,omitempty"`
	TaskID                         string `json:"task_id,omitempty"`
	RootCauseID                    string `json:"root_cause_id,omitempty"`
	ProvenanceGroupID              string `json:"provenance_group_id,omitempty"`
	ParentRefsJSON                 string `json:"parent_refs_json,omitempty"`
	PayloadJSON                    string `json:"payload_json,omitempty"`
	CreatedAt                      string `json:"created_at"`
	AuthorityHolderNodeID          string `json:"authority_holder_node_id,omitempty"`
	AuthorityTerm                  int64  `json:"authority_term,omitempty"`
	AuthorityLeaseTokenFingerprint string `json:"authority_lease_token_fingerprint,omitempty"`
	IngestSeq                      int64  `json:"ingest_seq,omitempty"`
}

type RuntimeEventFilter struct {
	WorkspaceID      string
	EventType        string
	EntityType       string
	EntityID         string
	AgentID          string
	SessionID        string
	TaskID           string
	ExcludeSynthetic bool
	CursorIngestSeq  *int64
	CursorEventID    string
	Limit            int
}

const runtimeEventSelectColumns = `event_id, COALESCE(dedup_key,''), workspace_id, event_type, entity_type, entity_id,
	        actor_type, actor_id, COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
	        payload_json, COALESCE(root_cause_id,''), COALESCE(provenance_group_id,''), COALESCE(parent_refs_json,'[]'), created_at,
	        COALESCE(authority_holder_node_id,''), COALESCE(authority_term,0), COALESCE(authority_lease_token_fingerprint,''), COALESCE(ingest_seq,0)`

type OperatorQueueUpsertInput struct {
	QueueID                 string
	WorkspaceID             string
	QueueKey                string
	QueueType               string
	Title                   string
	Summary                 string
	Details                 string
	PayloadJSON             string
	AssignedTo              string
	Urgency                 string
	SourceKind              string
	SourceID                string
	TaskID                  string
	SessionID               string
	AgentID                 string
	KeepSessionActive       bool
	DueAt                   string
	RequireMissing          bool
	RequireCurrentStatus    string
	RequireCurrentRevision  int64
	RequireCurrentUpdatedAt string
	PromptContextEnvelope   map[string]any
}

type OperatorQueueResolveInput struct {
	WorkspaceID             string
	QueueID                 string
	QueueKey                string
	Status                  string
	ResolvedBy              string
	Resolution              string
	Summary                 string
	Details                 string
	PayloadJSON             string
	RequireCurrentStatus    string
	RequireCurrentRevision  int64
	RequireCurrentUpdatedAt string
	PromptContextEnvelope   map[string]any
}

type OperatorQueueEscalateInput struct {
	WorkspaceID             string
	QueueID                 string
	QueueKey                string
	EscalatedBy             string
	Reason                  string
	AssignedTo              string
	Urgency                 string
	DueAt                   string
	RequireCurrentRevision  int64
	RequireCurrentUpdatedAt string
	PromptContextEnvelope   map[string]any
}

type OperatorQueueFilter struct {
	WorkspaceID string
	QueueType   string
	Status      string
	AssignedTo  string
	SessionID   string
	TaskID      string
	AgentID     string
	Limit       int
}

type OperatorQueueRecord struct {
	QueueID                 string                 `json:"queue_id"`
	WorkspaceID             string                 `json:"workspace_id"`
	QueueKey                string                 `json:"queue_key"`
	QueueType               string                 `json:"queue_type"`
	Status                  string                 `json:"status"`
	Title                   string                 `json:"title"`
	Summary                 string                 `json:"summary,omitempty"`
	Details                 string                 `json:"details,omitempty"`
	PayloadJSON             string                 `json:"payload_json,omitempty"`
	AssignedTo              string                 `json:"assigned_to,omitempty"`
	Urgency                 string                 `json:"urgency"`
	SourceKind              string                 `json:"source_kind,omitempty"`
	SourceID                string                 `json:"source_id,omitempty"`
	TaskID                  string                 `json:"task_id,omitempty"`
	SessionID               string                 `json:"session_id,omitempty"`
	AgentID                 string                 `json:"agent_id,omitempty"`
	KeepSessionActive       bool                   `json:"keep_session_active"`
	DueAt                   *string                `json:"due_at,omitempty"`
	Resolution              string                 `json:"resolution,omitempty"`
	ResolvedAt              *string                `json:"resolved_at,omitempty"`
	ResolvedBy              *string                `json:"resolved_by,omitempty"`
	EscalationCount         int                    `json:"escalation_count"`
	LastEscalatedAt         *string                `json:"last_escalated_at,omitempty"`
	LastEscalatedBy         *string                `json:"last_escalated_by,omitempty"`
	EscalationReason        string                 `json:"escalation_reason,omitempty"`
	Revision                int64                  `json:"revision"`
	CreatedAt               string                 `json:"created_at"`
	UpdatedAt               string                 `json:"updated_at"`
	TimeAuthority           WorkspaceTimeAuthority `json:"time_authority"`
	RequireMissing          bool                   `json:"-"`
	RequireCurrentStatus    string                 `json:"-"`
	RequireCurrentRevision  int64                  `json:"-"`
	RequireCurrentUpdatedAt string                 `json:"-"`
	promptContextEnvelope   map[string]any
}

type OperatorQueueSyncEvent struct {
	Record OperatorQueueRecord `json:"record"`
	Event  RuntimeEventRecord  `json:"event"`
}

type OperatorQueueSyncResult struct {
	Opened   []OperatorQueueSyncEvent `json:"opened,omitempty"`
	Resolved []OperatorQueueSyncEvent `json:"resolved,omitempty"`
}

type KnowledgeClaimInput struct {
	ClaimID               string
	WorkspaceID           string
	ClaimType             string
	Status                string
	Subject               string
	Body                  string
	Summary               string
	Confidence            float64
	SourceKind            string
	SourceID              string
	MemoryID              string
	TaskID                string
	SessionID             string
	AgentID               string
	SupersedesClaimID     string
	ConflictsClaimID      string
	Evidence              []string
	Tags                  []string
	PromptContextEnvelope map[string]any
}

type KnowledgeClaimArchiveInput struct {
	WorkspaceID           string
	ClaimID               string
	ArchivedBy            string
	Reason                string
	PromptContextEnvelope map[string]any
}

type KnowledgeClaimLifecycleInput struct {
	WorkspaceID           string
	ClaimID               string
	ActorID               string
	Reason                string
	ReviewDueAt           string
	AssignedTo            string
	Urgency               string
	SupersedingClaimID    string
	ConflictsClaimID      string
	PromptContextEnvelope map[string]any
}

type KnowledgeClaimReviewEscalationRecord struct {
	Claim KnowledgeClaimRecord `json:"claim"`
	Queue OperatorQueueRecord  `json:"queue"`
}

type KnowledgeClaimFilter struct {
	WorkspaceID     string
	Query           string
	ClaimType       string
	Status          string
	AgentID         string
	SessionID       string
	TaskID          string
	MemoryID        string
	SourceKind      string
	IncludeArchived bool
	Limit           int
}

type KnowledgeClaimRecord struct {
	ClaimID             string   `json:"claim_id"`
	WorkspaceID         string   `json:"workspace_id"`
	ClaimType           string   `json:"claim_type"`
	Status              string   `json:"status"`
	Subject             string   `json:"subject"`
	Body                string   `json:"body"`
	Summary             string   `json:"summary,omitempty"`
	Confidence          float64  `json:"confidence"`
	SourceKind          string   `json:"source_kind"`
	SourceID            string   `json:"source_id,omitempty"`
	MemoryID            string   `json:"memory_id,omitempty"`
	TaskID              string   `json:"task_id,omitempty"`
	SessionID           string   `json:"session_id,omitempty"`
	AgentID             string   `json:"agent_id,omitempty"`
	SupersedesClaimID   string   `json:"supersedes_claim_id,omitempty"`
	SupersededByClaimID string   `json:"superseded_by_claim_id,omitempty"`
	ConflictsClaimID    string   `json:"conflicts_claim_id,omitempty"`
	Evidence            []string `json:"evidence,omitempty"`
	Tags                []string `json:"tags,omitempty"`
	LifecycleReason     string   `json:"lifecycle_reason,omitempty"`
	ReviewDueAt         *string  `json:"review_due_at,omitempty"`
	ReviewedAt          *string  `json:"reviewed_at,omitempty"`
	ReviewedBy          *string  `json:"reviewed_by,omitempty"`
	CreatedAt           string   `json:"created_at"`
	UpdatedAt           string   `json:"updated_at"`
	ArchivedAt          *string  `json:"archived_at,omitempty"`
	ArchivedBy          *string  `json:"archived_by,omitempty"`
	RecoveryReason      string   `json:"recovery_reason,omitempty"`
	Freshness           string   `json:"freshness,omitempty"`
	ProvenanceStrength  string   `json:"provenance_strength,omitempty"`
	DowngradeReason     string   `json:"downgrade_reason,omitempty"`
	FreshnessReasons    []string `json:"freshness_reasons,omitempty"`
}

type ExecutionRunInput struct {
	RunID                 string
	WorkspaceID           string
	TaskID                string
	SessionID             string
	AgentID               string
	Title                 string
	Summary               string
	Status                string
	Outcome               string
	Verification          map[string]any
	PromptContextEnvelope map[string]any
}

type ExecutionRunFilter struct {
	WorkspaceID string
	Status      string
	TaskID      string
	SessionID   string
	AgentID     string
	Limit       int
}

type ExecutionAgentRunsCancelInput struct {
	WorkspaceID string
	AgentID     string
	ActorType   string
	ActorID     string
	Summary     string
	Outcome     string
}

type ExecutionAgentRunsCancelResult struct {
	WorkspaceID     string `json:"workspace_id"`
	AgentID         string `json:"agent_id"`
	RunsCancelled   int64  `json:"runs_cancelled"`
	StepsCancelled  int64  `json:"steps_cancelled"`
	Outcome         string `json:"outcome"`
	Summary         string `json:"summary,omitempty"`
	RuntimeEventID  string `json:"runtime_event_id,omitempty"`
	TransitionState string `json:"transition_state"`
}

type ExecutionRunRecord struct {
	RunID            string         `json:"run_id"`
	WorkspaceID      string         `json:"workspace_id"`
	TaskID           string         `json:"task_id,omitempty"`
	SessionID        string         `json:"session_id,omitempty"`
	AgentID          string         `json:"agent_id,omitempty"`
	Title            string         `json:"title"`
	Summary          string         `json:"summary,omitempty"`
	Status           string         `json:"status"`
	Outcome          string         `json:"outcome,omitempty"`
	VerificationJSON map[string]any `json:"verification,omitempty"`
	CreatedAt        string         `json:"created_at"`
	UpdatedAt        string         `json:"updated_at"`
	ClosedAt         *string        `json:"closed_at,omitempty"`

	promptContextEnvelope map[string]any
}

type ExecutionStepInput struct {
	StepID                string
	RunID                 string
	WorkspaceID           string
	ParentStepID          string
	Phase                 string
	Title                 string
	Summary               string
	Status                string
	SortOrder             int
	Evidence              []string
	Verification          map[string]any
	PromptContextEnvelope map[string]any
}

type ExecutionStepRecord struct {
	StepID           string         `json:"step_id"`
	RunID            string         `json:"run_id"`
	WorkspaceID      string         `json:"workspace_id"`
	ParentStepID     string         `json:"parent_step_id,omitempty"`
	Phase            string         `json:"phase"`
	Title            string         `json:"title"`
	Summary          string         `json:"summary,omitempty"`
	Status           string         `json:"status"`
	SortOrder        int            `json:"sort_order"`
	Evidence         []string       `json:"evidence,omitempty"`
	VerificationJSON map[string]any `json:"verification,omitempty"`
	CreatedAt        string         `json:"created_at"`
	UpdatedAt        string         `json:"updated_at"`
	CompletedAt      *string        `json:"completed_at,omitempty"`

	evidenceProvided      bool
	verificationProvided  bool
	promptContextEnvelope map[string]any
}

type ExecutionRunDetail struct {
	TimeAuthority WorkspaceTimeAuthority `json:"time_authority"`
	Run           ExecutionRunRecord     `json:"run"`
	Steps         []ExecutionStepRecord  `json:"steps"`
}

type CapabilityPolicyInput struct {
	PolicyID              string
	WorkspaceID           string
	SubjectType           string
	SubjectID             string
	Capability            string
	ToolID                string
	Effect                string
	Reason                string
	CreatedBy             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type CapabilityPolicyFilter struct {
	WorkspaceID string
	SubjectType string
	SubjectID   string
	Capability  string
	ToolID      string
	Limit       int
}

type CapabilityPolicyRecord struct {
	PolicyID    string `json:"policy_id"`
	WorkspaceID string `json:"workspace_id"`
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	Capability  string `json:"capability"`
	ToolID      string `json:"tool_id"`
	Effect      string `json:"effect"`
	Reason      string `json:"reason,omitempty"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type CapabilityCheckInput struct {
	WorkspaceID string
	SubjectType string
	SubjectID   string
	Capability  string
	ToolID      string
}

type CapabilityCheckResult struct {
	WorkspaceID     string                   `json:"workspace_id"`
	SubjectType     string                   `json:"subject_type"`
	SubjectID       string                   `json:"subject_id"`
	Capability      string                   `json:"capability"`
	ToolID          string                   `json:"tool_id"`
	Verdict         string                   `json:"verdict"`
	MatchedPolicy   *CapabilityPolicyRecord  `json:"matched_policy,omitempty"`
	MatchedPolicies []CapabilityPolicyRecord `json:"matched_policies,omitempty"`
}

func (s *Store) RecordRuntimeEvent(ctx context.Context, input RuntimeEventInput) (RuntimeEventRecord, error) {
	if runtimeEventRequiresToolCallPromptContext(input.EventType) {
		return RuntimeEventRecord{}, fmt.Errorf("raw runtime event API cannot record %s; use authority-backed tool call runtime event path", strings.TrimSpace(input.EventType))
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin runtime event tx: %w", err)
	}
	record, err := s.appendRuntimeEventTx(ctx, tx, input)
	if err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit runtime event tx: %w", err)
	}
	return record, nil
}

func (s *Store) RecordRuntimeEventWithAuthority(ctx context.Context, authority WorkspaceAuthorityRecord, input RuntimeEventInput) (RuntimeEventRecord, error) {
	referenceAt := strings.TrimSpace(input.CreatedAt)
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	fenceInput, err := workspaceAuthorityFenceInputFromRecord(authority, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin authority-backed runtime event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	record := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, checkedAuthority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, checkedAuthority, input)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit authority-backed runtime event tx: %w", err)
	}
	return record, nil
}

func (s *Store) RecordRuntimeEventWithLocalWorkspaceAuthority(ctx context.Context, workspaceID string, input RuntimeEventInput) (RuntimeEventRecord, error) {
	workspaceID = strings.TrimSpace(firstNonEmpty(workspaceID, input.WorkspaceID))
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	input.WorkspaceID = workspaceID
	referenceAt := strings.TrimSpace(input.CreatedAt)
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin local-authority runtime event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	record := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, checkedAuthority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, checkedAuthority, input)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit local-authority runtime event tx: %w", err)
	}
	return record, nil
}

func (s *Store) ListRuntimeEvents(ctx context.Context, filter RuntimeEventFilter) ([]RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	out := make([]RuntimeEventRecord, 0, filter.Limit)

	// If ExcludeSynthetic is on, we might need to fetch multiple batches. We do this by mutating the filter and looping.
	cursorSeq := filter.CursorIngestSeq
	cursorEvent := filter.CursorEventID

	batchSize := filter.Limit
	if filter.ExcludeSynthetic {
		batchSize = maxInt(filter.Limit*2, 100)
	}

	for len(out) < filter.Limit {
		// Rebuild the query iteratively since the cursor changes for pagination
		batchQuery := strings.Builder{}
		batchQuery.WriteString(`SELECT `)
		batchQuery.WriteString(runtimeEventSelectColumns)
		batchQuery.WriteString(` FROM runtime_events WHERE workspace_id = ?`)
		batchArgs := []any{workspaceID}

		if trimmed := strings.TrimSpace(filter.EventType); trimmed != "" {
			batchQuery.WriteString(` AND event_type = ?`)
			batchArgs = append(batchArgs, trimmed)
		}
		if trimmed := strings.TrimSpace(filter.EntityType); trimmed != "" {
			batchQuery.WriteString(` AND entity_type = ?`)
			batchArgs = append(batchArgs, trimmed)
		}
		if trimmed := strings.TrimSpace(filter.EntityID); trimmed != "" {
			batchQuery.WriteString(` AND entity_id = ?`)
			batchArgs = append(batchArgs, trimmed)
		}
		// COALESCE(..., '') was a query shape anti-pattern that broke indices.
		// Replaced with exactly matching column filters for the optimized covered cursors.
		if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
			batchQuery.WriteString(` AND agent_id = ?`)
			batchArgs = append(batchArgs, trimmed)
		}
		if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
			batchQuery.WriteString(` AND session_id = ?`)
			batchArgs = append(batchArgs, trimmed)
		}
		if trimmed := strings.TrimSpace(filter.TaskID); trimmed != "" {
			batchQuery.WriteString(` AND task_id = ?`)
			batchArgs = append(batchArgs, trimmed)
		}

		if cursorEvent != "" {
			if cursorSeq != nil {
				batchQuery.WriteString(` AND (COALESCE(ingest_seq,0) < ? OR (COALESCE(ingest_seq,0) = ? AND event_id < ?))`)
				batchArgs = append(batchArgs, *cursorSeq, *cursorSeq, cursorEvent)
			} else {
				batchQuery.WriteString(` AND (COALESCE(ingest_seq,0) = 0 AND event_id < ?)`)
				batchArgs = append(batchArgs, cursorEvent)
			}
		}
		batchQuery.WriteString(` ORDER BY ingest_seq DESC, event_id DESC LIMIT ?`)
		batchArgs = append(batchArgs, batchSize)

		batchCount, err := func() (int, error) {
			rows, err := s.db.QueryContext(ctx, batchQuery.String(), batchArgs...)
			if err != nil {
				return 0, fmt.Errorf("query runtime events: %w", err)
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				var record RuntimeEventRecord
				if err := scanRuntimeEvent(rows, &record); err != nil {
					return 0, fmt.Errorf("scan runtime event: %w", err)
				}
				count++
				cursorSeq = &record.IngestSeq
				cursorEvent = record.EventID
				if filter.ExcludeSynthetic && isSyntheticOperationalEvent(record) {
					continue
				}
				out = append(out, record)

				if len(out) >= filter.Limit {
					break
				}
			}
			if err := rows.Err(); err != nil {
				return 0, fmt.Errorf("iterate runtime events: %w", err)
			}
			return count, nil
		}()
		if err != nil {
			return nil, err
		}
		if batchCount < batchSize {
			break
		}
	}
	return out, nil
}

type runtimeEventScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeEvent(scanner runtimeEventScanner, record *RuntimeEventRecord) error {
	return scanner.Scan(
		&record.EventID,
		&record.DedupKey,
		&record.WorkspaceID,
		&record.EventType,
		&record.EntityType,
		&record.EntityID,
		&record.ActorType,
		&record.ActorID,
		&record.AgentID,
		&record.SessionID,
		&record.TaskID,
		&record.PayloadJSON,
		&record.RootCauseID,
		&record.ProvenanceGroupID,
		&record.ParentRefsJSON,
		&record.CreatedAt,
		&record.AuthorityHolderNodeID,
		&record.AuthorityTerm,
		&record.AuthorityLeaseTokenFingerprint,
		&record.IngestSeq,
	)
}

func normalizeRuntimeEventPayload(raw string) string {
	if raw == "" {
		return "{}"
	}
	return raw
}

func authorityBackedRuntimeEventInput(input RuntimeEventInput, authority WorkspaceAuthorityRecord) (RuntimeEventInput, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventInput{}, errors.New("authority-backed runtime event requires workspace_id")
	}
	if strings.TrimSpace(authority.WorkspaceID) == "" || strings.TrimSpace(authority.WorkspaceID) != workspaceID {
		return RuntimeEventInput{}, fmt.Errorf("authority-backed runtime event workspace mismatch for %q", workspaceID)
	}
	if normalizeWorkspaceAuthorityScope(authority.Scope) != authorityScopeWorkspace {
		return RuntimeEventInput{}, fmt.Errorf("authority-backed runtime event requires workspace scope authority, got %q", authority.Scope)
	}
	if authority.Status != WorkspaceAuthorityStatusActive {
		return RuntimeEventInput{}, fmt.Errorf("authority-backed runtime event requires ACTIVE workspace authority, got %s", authority.Status)
	}
	holder := strings.TrimSpace(authority.HolderAuthorityNodeID)
	if holder == "" {
		return RuntimeEventInput{}, errors.New("authority-backed runtime event requires holder_authority_node_id")
	}
	if authority.Term <= 0 {
		return RuntimeEventInput{}, errors.New("authority-backed runtime event requires positive authority term")
	}
	fingerprint := authorityLeaseTokenFingerprint(authority.LeaseToken)
	if fingerprint == "" {
		return RuntimeEventInput{}, errors.New("authority-backed runtime event requires lease token fingerprint")
	}
	input.WorkspaceID = workspaceID
	input.AuthorityHolderNodeID = holder
	input.AuthorityTerm = authority.Term
	input.AuthorityLeaseTokenFingerprint = fingerprint
	return input, nil
}

func (s *Store) appendAuthorityBackedRuntimeEventTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input RuntimeEventInput) (RuntimeEventRecord, error) {
	input, err := authorityBackedRuntimeEventInput(input, authority)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return s.appendRuntimeEventTx(ctx, tx, input)
}

func workspaceAuthorityEnvelopeProvided(authority WorkspaceAuthorityRecord) bool {
	return strings.TrimSpace(authority.WorkspaceID) != "" ||
		strings.TrimSpace(authority.Scope) != "" ||
		strings.TrimSpace(authority.HolderAuthorityNodeID) != "" ||
		strings.TrimSpace(authority.LeaseToken) != "" ||
		authority.Term != 0 ||
		strings.TrimSpace(authority.LeaseExpiresAt) != "" ||
		authority.CommitWatermark != 0 ||
		authority.AppliedWatermark != 0 ||
		authority.Status != "" ||
		strings.TrimSpace(authority.UpdatedAt) != ""
}

func (s *Store) appendRuntimeEventWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input RuntimeEventInput) (RuntimeEventRecord, error) {
	if !workspaceAuthorityEnvelopeProvided(authority) {
		return s.appendRuntimeEventTx(ctx, tx, input)
	}
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, input)
}

func canonicalizeRuntimeEventParentRefs(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return []string{}, nil
	}
	var refs []any
	if err := json.Unmarshal([]byte(trimmed), &refs); err != nil {
		return nil, fmt.Errorf("parent_refs_json must be a JSON array: %w", err)
	}
	out := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, rawRef := range refs {
		ref, ok := rawRef.(string)
		if !ok {
			return nil, errors.New("parent_refs_json must be a JSON array of runtime event ids")
		}
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return nil, errors.New("parent_refs_json must not contain empty runtime event ids")
		}
		if _, exists := seen[ref]; exists {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	sort.Strings(out)
	return out, nil
}

func normalizeRuntimeEventParentRefs(raw string) (string, error) {
	refs, err := canonicalizeRuntimeEventParentRefs(raw)
	if err != nil {
		return "", err
	}
	normalized, err := json.Marshal(refs)
	if err != nil {
		return "", fmt.Errorf("encode canonical parent_refs_json: %w", err)
	}
	return string(normalized), nil
}

func parseRuntimeEventParentRefs(raw string) ([]string, error) {
	return canonicalizeRuntimeEventParentRefs(raw)
}

func runtimeEventsSemanticallyEquivalent(existing, candidate RuntimeEventRecord) bool {
	return runtimeReplayEventsSemanticallyEquivalent(existing, candidate)
}

func runtimeEventIdentityConflict(record RuntimeEventRecord, existing RuntimeEventRecord) error {
	return fmt.Errorf("event_id %q already belongs to a runtime event with different semantics", existing.EventID)
}

func (s *Store) getRuntimeEventByDedupKeyTx(ctx context.Context, tx *sql.Tx, workspaceID, dedupKey string) (RuntimeEventRecord, error) {
	dedupKey = strings.TrimSpace(dedupKey)
	if dedupKey == "" {
		return RuntimeEventRecord{}, sql.ErrNoRows
	}
	var record RuntimeEventRecord
	if err := scanRuntimeEvent(
		tx.QueryRowContext(
			ctx,
			`SELECT `+runtimeEventSelectColumns+`
			   FROM runtime_events
			  WHERE workspace_id = ? AND dedup_key = ?
			  ORDER BY ingest_seq DESC, event_id DESC
			  LIMIT 1`,
			workspaceID,
			dedupKey,
		),
		&record,
	); err == nil {
		return record, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return RuntimeEventRecord{}, fmt.Errorf("lookup runtime event by persisted dedup key: %w", err)
	}
	rows, err := tx.QueryContext(
		ctx,
		`SELECT `+runtimeEventSelectColumns+`
		   FROM runtime_events
		  WHERE workspace_id = ?
		    AND (dedup_key IS NULL OR dedup_key = '')
		  ORDER BY ingest_seq DESC, event_id DESC`,
		workspaceID,
	)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("query runtime events by dedup key: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var record RuntimeEventRecord
		if err := scanRuntimeEvent(rows, &record); err != nil {
			return RuntimeEventRecord{}, fmt.Errorf("scan runtime event by dedup key: %w", err)
		}
		if runtimeReplayEventDedupKey(record) == dedupKey {
			return record, nil
		}
	}
	if err := rows.Err(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("iterate runtime events by dedup key: %w", err)
	}
	return RuntimeEventRecord{}, sql.ErrNoRows
}

func isRuntimeEventIdentityConflictError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint failed") &&
		strings.Contains(message, "runtime_events") &&
		strings.Contains(message, "event_id")
}

func (s *Store) getRuntimeEventByIDTx(ctx context.Context, tx *sql.Tx, workspaceID, eventID string) (RuntimeEventRecord, error) {
	var record RuntimeEventRecord
	if err := scanRuntimeEvent(
		tx.QueryRowContext(
			ctx,
			`SELECT `+runtimeEventSelectColumns+`
			   FROM runtime_events
			  WHERE workspace_id = ? AND event_id = ?
			  LIMIT 1`,
			workspaceID,
			eventID,
		),
		&record,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, sql.ErrNoRows
		}
		return RuntimeEventRecord{}, fmt.Errorf("lookup runtime event by id: %w", err)
	}
	return record, nil
}

func (s *Store) lookupRuntimeEventByID(ctx context.Context, workspaceID, eventID string) (RuntimeEventRecord, error) {
	var record RuntimeEventRecord
	if err := scanRuntimeEvent(
		s.db.QueryRowContext(
			ctx,
			`SELECT `+runtimeEventSelectColumns+`
			   FROM runtime_events
			  WHERE workspace_id = ? AND event_id = ?
			  LIMIT 1`,
			workspaceID,
			eventID,
		),
		&record,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, sql.ErrNoRows
		}
		return RuntimeEventRecord{}, fmt.Errorf("lookup runtime event by id: %w", err)
	}
	return record, nil
}

func (s *Store) nextRuntimeEventIngestSeqTx(ctx context.Context, tx *sql.Tx, workspaceID string) (int64, error) {
	var nextSeq int64
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(ingest_seq), 0) + 1
		   FROM runtime_events
		  WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&nextSeq); err != nil {
		return 0, fmt.Errorf("allocate runtime event ingest sequence: %w", err)
	}
	return nextSeq, nil
}

func (s *Store) validateRuntimeEventParentRefsTx(ctx context.Context, tx *sql.Tx, workspaceID, eventID, parentRefsJSON string) error {
	parentRefs, err := parseRuntimeEventParentRefs(parentRefsJSON)
	if err != nil {
		return err
	}
	for _, parentID := range parentRefs {
		if parentID == strings.TrimSpace(eventID) {
			return fmt.Errorf("parent_refs_json must not reference event_id %q itself", parentID)
		}
		parent, err := s.getRuntimeEventByIDTx(ctx, tx, workspaceID, parentID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("parent_ref %q not found in workspace runtime journal", parentID)
		}
		if err != nil {
			return err
		}
		if strings.TrimSpace(eventID) == "" {
			continue
		}
		contains, err := s.runtimeEventLineageContainsTx(ctx, tx, workspaceID, parent.EventID, eventID, map[string]bool{})
		if err != nil {
			return err
		}
		if contains {
			return fmt.Errorf("parent_ref %q would create a runtime-event lineage cycle involving %q", parentID, eventID)
		}
	}
	return nil
}

func (s *Store) runtimeEventLineageContainsTx(ctx context.Context, tx *sql.Tx, workspaceID, startID, targetID string, visited map[string]bool) (bool, error) {
	startID = strings.TrimSpace(startID)
	targetID = strings.TrimSpace(targetID)
	if startID == "" || targetID == "" {
		return false, nil
	}
	if startID == targetID {
		return true, nil
	}
	if visited[startID] {
		return false, nil
	}
	visited[startID] = true
	record, err := s.getRuntimeEventByIDTx(ctx, tx, workspaceID, startID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	parentRefs, err := parseRuntimeEventParentRefs(record.ParentRefsJSON)
	if err != nil {
		return false, err
	}
	for _, parentID := range parentRefs {
		if parentID == targetID {
			return true, nil
		}
		contains, err := s.runtimeEventLineageContainsTx(ctx, tx, workspaceID, parentID, targetID, visited)
		if err != nil {
			return false, err
		}
		if contains {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) UpsertOperatorQueueItem(ctx context.Context, input OperatorQueueUpsertInput) (OperatorQueueRecord, error) {
	record := normalizeOperatorQueueUpsertInput(input)
	if record.WorkspaceID == "" {
		return OperatorQueueRecord{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRecord{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRecord{}, errors.New("title is required")
	}
	if err := validateGenericOperatorQueueUpsertRecord(record); err != nil {
		return OperatorQueueRecord{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, err
	}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, _, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, record, now)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, err
	}
	return s.getOperatorQueueItem(ctx, record.WorkspaceID, record.QueueID, "")
}

func (s *Store) UpsertOperatorQueueItemWithEvent(ctx context.Context, input OperatorQueueUpsertInput) (OperatorQueueRecord, RuntimeEventRecord, error) {
	record := normalizeOperatorQueueUpsertInput(input)
	if record.WorkspaceID == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("title is required")
	}
	if err := validateGenericOperatorQueueUpsertRecord(record); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	existingQueueID, err := s.lookupOperatorQueueIDByKey(ctx, record.WorkspaceID, record.QueueKey)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	record.QueueID = firstNonEmpty(existingQueueID, record.QueueID, nextID("opq"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	queue := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		queue, event, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, record, now)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	queue, err = s.getOperatorQueueItem(ctx, queue.WorkspaceID, queue.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return queue, event, nil
}

func (s *Store) UpsertRollbackFailureQueueItemWithCurrentCreateLineage(ctx context.Context, input OperatorQueueUpsertInput, sourceQueueID, sourceQueueKey string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	record := normalizeOperatorQueueUpsertInput(input)
	if record.WorkspaceID == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("title is required")
	}
	if err := validateGenericOperatorQueueUpsertRecord(record); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	existingQueueID, err := s.lookupOperatorQueueIDByKey(ctx, record.WorkspaceID, record.QueueKey)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	record.QueueID = firstNonEmpty(existingQueueID, record.QueueID, nextID("opq"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	queue := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		currentRecord := record
		if payload, ok := decodeLinkedRollbackFailurePayload(currentRecord.PayloadJSON, currentRecord.QueueKey); ok {
			sourceQueueID, sourceQueueKey, ok, err := s.currentRollbackFailureCreateSourceQueueProofTx(ctx, tx, currentRecord.WorkspaceID, sourceQueueID, sourceQueueKey)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("rollback-failure create requires a current linked source queue")
			}
			payload.SourceQueueID = sourceQueueID
			payload.SourceQueueKey = sourceQueueKey
			lineage, ok, err := s.currentRollbackFailureCreateLineageTx(ctx, tx, currentRecord.WorkspaceID, sourceQueueID, sourceQueueKey)
			if err != nil {
				return err
			}
			if ok {
				applyRollbackFailurePayloadLineage(&payload, lineage)
			}
			payload.Normalize()
			currentRecord.PayloadJSON = string(mustJSON(payload))
		}
		var innerErr error
		queue, event, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, currentRecord, now)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	queue, err = s.getOperatorQueueItem(ctx, queue.WorkspaceID, queue.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return queue, event, nil
}

func (s *Store) UpsertRollbackFailureQueueItemWithCurrentLinkedActionCreateContext(ctx context.Context, input OperatorQueueUpsertInput, actionID string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	record := normalizeOperatorQueueUpsertInput(input)
	if record.WorkspaceID == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("title is required")
	}
	if err := validateGenericOperatorQueueUpsertRecord(record); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	existingQueueID, err := s.lookupOperatorQueueIDByKey(ctx, record.WorkspaceID, record.QueueKey)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	record.QueueID = firstNonEmpty(existingQueueID, record.QueueID, nextID("opq"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	queue := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		currentRecord := record
		if payload, ok := decodeLinkedRollbackFailurePayload(currentRecord.PayloadJSON, currentRecord.QueueKey); ok {
			sourceQueueID, sourceQueueKey, ok, err := s.currentRollbackFailureCreateSourceQueueFromActionTx(ctx, tx, currentRecord.WorkspaceID, actionID)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("rollback-failure create requires a current linked source queue for action context")
			}
			payload.ActionID = firstNonEmpty(strings.TrimSpace(payload.ActionID), strings.TrimSpace(actionID))
			payload.SourceQueueID = sourceQueueID
			payload.SourceQueueKey = sourceQueueKey
			lineage, ok, err := s.currentRollbackFailureCreateLineageTx(ctx, tx, currentRecord.WorkspaceID, sourceQueueID, sourceQueueKey)
			if err != nil {
				return err
			}
			if ok {
				applyRollbackFailurePayloadLineage(&payload, lineage)
			}
			payload.Normalize()
			currentRecord.PayloadJSON = string(mustJSON(payload))
		}
		var innerErr error
		queue, event, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, currentRecord, now)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	queue, err = s.getOperatorQueueItem(ctx, queue.WorkspaceID, queue.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return queue, event, nil
}

func (s *Store) UpsertRollbackFailureQueueItemWithCurrentLinkedRepairCreateContext(ctx context.Context, input OperatorQueueUpsertInput, repairTensionID string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	record := normalizeOperatorQueueUpsertInput(input)
	if record.WorkspaceID == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("title is required")
	}
	if err := validateGenericOperatorQueueUpsertRecord(record); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	existingQueueID, err := s.lookupOperatorQueueIDByKey(ctx, record.WorkspaceID, record.QueueKey)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	record.QueueID = firstNonEmpty(existingQueueID, record.QueueID, nextID("opq"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	queue := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		currentRecord := record
		if payload, ok := decodeLinkedRollbackFailurePayload(currentRecord.PayloadJSON, currentRecord.QueueKey); ok {
			sourceQueueID, sourceQueueKey, actionID, ok, err := s.currentRollbackFailureCreateSourceQueueFromRepairTensionTx(ctx, tx, currentRecord.WorkspaceID, repairTensionID)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("rollback-failure create requires a current pending linked rebase follow-up carrier for repair tension context")
			}
			payload.RepairTensionID = firstNonEmpty(strings.TrimSpace(payload.RepairTensionID), strings.TrimSpace(repairTensionID))
			payload.ActionID = firstNonEmpty(strings.TrimSpace(payload.ActionID), actionID)
			payload.SourceQueueID = sourceQueueID
			payload.SourceQueueKey = sourceQueueKey
			lineage, ok, err := s.currentRollbackFailureCreateLineageTx(ctx, tx, currentRecord.WorkspaceID, sourceQueueID, sourceQueueKey)
			if err != nil {
				return err
			}
			if ok {
				applyRollbackFailurePayloadLineage(&payload, lineage)
			}
			payload.Normalize()
			currentRecord.PayloadJSON = string(mustJSON(payload))
		}
		var innerErr error
		queue, event, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, currentRecord, now)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	queue, err = s.getOperatorQueueItem(ctx, queue.WorkspaceID, queue.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return queue, event, nil
}

func (s *Store) UpsertRollbackFailureQueueItemWithCurrentTaskCreateContext(ctx context.Context, input OperatorQueueUpsertInput, taskID, runID string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	record := normalizeOperatorQueueUpsertInput(input)
	if record.WorkspaceID == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("title is required")
	}
	if err := validateGenericOperatorQueueUpsertRecord(record); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	existingQueueID, err := s.lookupOperatorQueueIDByKey(ctx, record.WorkspaceID, record.QueueKey)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	record.QueueID = firstNonEmpty(existingQueueID, record.QueueID, nextID("opq"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	queue := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		currentRecord := record
		if payload, ok := decodeLinkedRollbackFailurePayload(currentRecord.PayloadJSON, currentRecord.QueueKey); ok {
			sourceQueueID, sourceQueueKey, actionID, linkedRepairTensionID, ok, err := s.currentRollbackFailureCreateSourceQueueFromTaskOrRunTx(ctx, tx, currentRecord.WorkspaceID, taskID, runID)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("rollback-failure create requires a unique active linked rebase follow-up carrier for task/run context")
			}
			payload.ActionID = firstNonEmpty(strings.TrimSpace(payload.ActionID), actionID)
			payload.RepairTensionID = firstNonEmpty(strings.TrimSpace(payload.RepairTensionID), linkedRepairTensionID)
			payload.SourceQueueID = sourceQueueID
			payload.SourceQueueKey = sourceQueueKey
			lineage, ok, err := s.currentRollbackFailureCreateLineageTx(ctx, tx, currentRecord.WorkspaceID, sourceQueueID, sourceQueueKey)
			if err != nil {
				return err
			}
			if ok {
				applyRollbackFailurePayloadLineage(&payload, lineage)
			}
			payload.Normalize()
			currentRecord.PayloadJSON = string(mustJSON(payload))
		}
		var innerErr error
		queue, event, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, currentRecord, now)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	queue, err = s.getOperatorQueueItem(ctx, queue.WorkspaceID, queue.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return queue, event, nil
}

func normalizeOperatorQueueLineage(lineage operatorQueueEventLineagePayload) operatorQueueEventLineagePayload {
	lineage.RootCauseID = strings.TrimSpace(lineage.RootCauseID)
	lineage.ProvenanceGroupID = strings.TrimSpace(lineage.ProvenanceGroupID)
	if len(lineage.ParentRefsJSON) == 0 {
		lineage.ParentRefsJSON = nil
		return lineage
	}
	seen := make(map[string]struct{}, len(lineage.ParentRefsJSON))
	normalized := make([]string, 0, len(lineage.ParentRefsJSON))
	for _, ref := range lineage.ParentRefsJSON {
		trimmed := strings.TrimSpace(ref)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	sort.Strings(normalized)
	if len(normalized) == 0 {
		lineage.ParentRefsJSON = nil
		return lineage
	}
	lineage.ParentRefsJSON = normalized
	return lineage
}

func operatorQueueLineagePresent(lineage operatorQueueEventLineagePayload) bool {
	lineage = normalizeOperatorQueueLineage(lineage)
	return lineage.RootCauseID != "" || lineage.ProvenanceGroupID != "" || len(lineage.ParentRefsJSON) > 0
}

func operatorQueueLineageFromRuntimeEvent(event RuntimeEventRecord) operatorQueueEventLineagePayload {
	lineage := operatorQueueEventLineagePayload{
		RootCauseID:       firstNonEmpty(strings.TrimSpace(event.RootCauseID), strings.TrimSpace(event.EventID)),
		ProvenanceGroupID: firstNonEmpty(strings.TrimSpace(event.ProvenanceGroupID), strings.TrimSpace(event.EventID)),
	}
	if trimmed := strings.TrimSpace(event.EventID); trimmed != "" {
		lineage.ParentRefsJSON = []string{trimmed}
	}
	return normalizeOperatorQueueLineage(lineage)
}

func applyRollbackFailurePayloadLineage(payload *model.RebaseRollbackFailurePayload, lineage operatorQueueEventLineagePayload) {
	if payload == nil {
		return
	}
	lineage = normalizeOperatorQueueLineage(lineage)
	if !operatorQueueLineagePresent(lineage) {
		return
	}
	payload.RootCauseID = lineage.RootCauseID
	payload.ProvenanceGroupID = lineage.ProvenanceGroupID
	payload.ParentRefsJSON = append([]string(nil), lineage.ParentRefsJSON...)
}

func (s *Store) currentRollbackFailureCreateLineageTx(ctx context.Context, tx *sql.Tx, workspaceID, sourceQueueID, sourceQueueKey string) (operatorQueueEventLineagePayload, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	sourceQueueID = strings.TrimSpace(sourceQueueID)
	sourceQueueKey = strings.TrimSpace(sourceQueueKey)
	if workspaceID == "" || (sourceQueueID == "" && sourceQueueKey == "") {
		return operatorQueueEventLineagePayload{}, false, nil
	}
	queue, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, sourceQueueID, sourceQueueKey)
	if err != nil {
		if errors.Is(err, ErrOperatorQueueItemNotFound) {
			return operatorQueueEventLineagePayload{}, false, nil
		}
		return operatorQueueEventLineagePayload{}, false, err
	}
	if payload, ok := decodeLinkedRollbackFailurePayload(queue.PayloadJSON, queue.QueueKey); ok {
		lineage := normalizeOperatorQueueLineage(operatorQueueEventLineagePayload{
			RootCauseID:       payload.RootCauseID,
			ProvenanceGroupID: payload.ProvenanceGroupID,
			ParentRefsJSON:    append([]string(nil), payload.ParentRefsJSON...),
		})
		return lineage, operatorQueueLineagePresent(lineage), nil
	}
	if payload, ok := decodeLinkedRebaseFollowupPayload(queue.PayloadJSON, queue.QueueKey); ok {
		lineage := normalizeOperatorQueueLineage(operatorQueueEventLineagePayload{
			RootCauseID:       payload.RootCauseID,
			ProvenanceGroupID: payload.ProvenanceGroupID,
			ParentRefsJSON:    append([]string(nil), payload.ParentRefsJSON...),
		})
		if operatorQueueLineagePresent(lineage) {
			return lineage, true, nil
		}
		actionID := strings.TrimSpace(payload.ActionID)
		if actionID == "" {
			actionID = strings.TrimSpace(payload.LastFailedActionID)
		}
		return s.latestHumanActionLineagePayloadTx(ctx, tx, workspaceID, actionID)
	}
	return operatorQueueEventLineagePayload{}, false, nil
}

func (s *Store) currentRollbackFailureCreateSourceQueueProofTx(ctx context.Context, tx *sql.Tx, workspaceID, sourceQueueID, sourceQueueKey string) (string, string, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	sourceQueueID = strings.TrimSpace(sourceQueueID)
	sourceQueueKey = strings.TrimSpace(sourceQueueKey)
	if workspaceID == "" || (sourceQueueID == "" && sourceQueueKey == "") {
		return "", "", false, nil
	}
	queue, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, sourceQueueID, sourceQueueKey)
	if err != nil {
		if errors.Is(err, ErrOperatorQueueItemNotFound) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	if _, ok := decodeLinkedRollbackFailurePayload(queue.PayloadJSON, queue.QueueKey); !ok {
		if _, ok := decodeLinkedRebaseFollowupPayload(queue.PayloadJSON, queue.QueueKey); !ok {
			return "", "", false, nil
		}
	}
	return strings.TrimSpace(queue.QueueID), strings.TrimSpace(queue.QueueKey), true, nil
}

func (s *Store) currentRollbackFailureCreateSourceQueueFromActionTx(ctx context.Context, tx *sql.Tx, workspaceID, actionID string) (string, string, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	actionID = strings.TrimSpace(actionID)
	if workspaceID == "" || actionID == "" {
		return "", "", false, nil
	}
	actionQueue, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, "", "action:"+actionID)
	if err != nil {
		if errors.Is(err, ErrOperatorQueueItemNotFound) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	payload, ok := decodeHumanActionQueuePayload(actionQueue.PayloadJSON, actionQueue.QueueKey)
	if !ok {
		return "", "", false, nil
	}
	if payloadActionID := strings.TrimSpace(payload.ActionID); payloadActionID != "" && payloadActionID != actionID {
		return "", "", false, nil
	}
	sourceQueueID := strings.TrimSpace(payload.SourceQueueID)
	sourceQueueKey := strings.TrimSpace(payload.SourceQueueKey)
	if sourceQueueID == "" && sourceQueueKey == "" {
		return "", "", false, nil
	}
	sourceQueue, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, sourceQueueID, sourceQueueKey)
	if err != nil {
		if errors.Is(err, ErrOperatorQueueItemNotFound) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	if _, ok := decodeLinkedRebaseFollowupPayload(sourceQueue.PayloadJSON, sourceQueue.QueueKey); !ok {
		if _, ok := decodeLinkedRollbackFailurePayload(sourceQueue.PayloadJSON, sourceQueue.QueueKey); !ok {
			return "", "", false, nil
		}
	}
	return strings.TrimSpace(sourceQueue.QueueID), strings.TrimSpace(sourceQueue.QueueKey), true, nil
}

func (s *Store) currentRollbackFailureCreateSourceQueueFromRepairTensionTx(ctx context.Context, tx *sql.Tx, workspaceID, repairTensionID string) (string, string, string, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	repairTensionID = strings.TrimSpace(repairTensionID)
	if workspaceID == "" || repairTensionID == "" {
		return "", "", "", false, nil
	}
	sourceQueue, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, "", model.RebaseFollowupQueueKeyPrefix+repairTensionID)
	if err != nil {
		if errors.Is(err, ErrOperatorQueueItemNotFound) {
			return "", "", "", false, nil
		}
		return "", "", "", false, err
	}
	if strings.ToUpper(strings.TrimSpace(sourceQueue.Status)) != "OPEN" {
		return "", "", "", false, nil
	}
	payload, ok := decodeLinkedRebaseFollowupPayload(sourceQueue.PayloadJSON, sourceQueue.QueueKey)
	if !ok {
		return "", "", "", false, nil
	}
	if strings.TrimSpace(payload.RepairTensionID) != repairTensionID {
		return "", "", "", false, nil
	}
	actionID := strings.TrimSpace(payload.ActionID)
	if actionID == "" {
		return "", "", "", false, nil
	}
	action, err := s.getHumanActionTx(ctx, tx, actionID)
	if err != nil {
		if isHumanActionNotFoundError(err) {
			return "", "", "", false, nil
		}
		return "", "", "", false, err
	}
	if strings.ToUpper(strings.TrimSpace(action.Status)) != model.ActionStatusPending {
		return "", "", "", false, nil
	}
	return strings.TrimSpace(sourceQueue.QueueID), strings.TrimSpace(sourceQueue.QueueKey), actionID, true, nil
}

func currentRollbackFailureCreateSourceQueueFromTaskQueues(resolveAction func(string) (HumanActionRecord, error), queues []OperatorQueueRecord) (string, string, string, string, bool, error) {
	type match struct {
		queueID         string
		queueKey        string
		actionID        string
		repairTensionID string
	}
	matches := make([]match, 0, 1)
	for _, queue := range queues {
		payload, ok := decodeLinkedRebaseFollowupPayload(queue.PayloadJSON, queue.QueueKey)
		if !ok {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(queue.Status)) != "OPEN" {
			continue
		}
		if !payload.IsRebaseFollowup(queue.QueueKey) || !payload.LinkedActionExists() {
			continue
		}
		if strings.TrimSpace(payload.LastFailedActionID) != "" {
			continue
		}
		if strings.TrimSpace(payload.RebaseWorkflowState) != model.RebaseWorkflowStateInProgress ||
			strings.TrimSpace(payload.RebaseWorkflowStep) != model.RebaseWorkflowStepOperatorClaimed {
			continue
		}
		actionID := strings.TrimSpace(payload.ActionID)
		if actionID == "" {
			continue
		}
		action, err := resolveAction(actionID)
		if err != nil {
			if isHumanActionNotFoundError(err) {
				continue
			}
			return "", "", "", "", false, err
		}
		if strings.ToUpper(strings.TrimSpace(action.Status)) != model.ActionStatusPending {
			continue
		}
		matches = append(matches, match{
			queueID:         strings.TrimSpace(queue.QueueID),
			queueKey:        strings.TrimSpace(queue.QueueKey),
			actionID:        actionID,
			repairTensionID: strings.TrimSpace(payload.RepairTensionID),
		})
	}
	switch len(matches) {
	case 0:
		return "", "", "", "", false, nil
	case 1:
		return matches[0].queueID, matches[0].queueKey, matches[0].actionID, matches[0].repairTensionID, true, nil
	default:
		return "", "", "", "", false, errors.New("multiple active linked rebase follow-up queues for rollback-failure create")
	}
}

func resolveRollbackFailureCreateTaskIDFromTaskOrRun(loadRun func(string) (ExecutionRunRecord, error), taskID, runID string) (string, bool, error) {
	taskID = strings.TrimSpace(taskID)
	runID = strings.TrimSpace(runID)
	if taskID == "" && runID == "" {
		return "", false, nil
	}
	if runID == "" {
		return taskID, taskID != "", nil
	}
	run, err := loadRun(runID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "execution run not found") {
			return "", false, nil
		}
		return "", false, err
	}
	runTaskID := strings.TrimSpace(run.TaskID)
	if runTaskID == "" {
		return "", false, nil
	}
	if taskID != "" && taskID != runTaskID {
		return "", false, errors.New("rollback-failure create requires task_id to match run_id task context")
	}
	return runTaskID, true, nil
}

func (s *Store) CurrentRollbackFailureCreateSourceQueueFromTaskOrRun(ctx context.Context, workspaceID, taskID, runID string) (string, string, string, string, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	runID = strings.TrimSpace(runID)
	if workspaceID == "" {
		return "", "", "", "", false, nil
	}
	resolvedTaskID, ok, err := resolveRollbackFailureCreateTaskIDFromTaskOrRun(func(currentRunID string) (ExecutionRunRecord, error) {
		return s.getExecutionRun(ctx, workspaceID, currentRunID)
	}, taskID, runID)
	if err != nil {
		return "", "", "", "", false, err
	}
	if !ok {
		return "", "", "", "", false, nil
	}
	taskID = resolvedTaskID
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT `+operatorQueueSelectColumns("")+`
		   FROM operator_queue_items
		  WHERE workspace_id = ? AND queue_type = ? AND status = ? AND COALESCE(task_id,'') = ?`,
		workspaceID,
		"FOLLOW_UP",
		"OPEN",
		taskID,
	)
	if err != nil {
		return "", "", "", "", false, err
	}
	defer rows.Close()
	queues, err := collectOperatorQueueRows(rows)
	if err != nil {
		return "", "", "", "", false, err
	}
	return currentRollbackFailureCreateSourceQueueFromTaskQueues(func(actionID string) (HumanActionRecord, error) {
		return s.GetHumanAction(ctx, actionID)
	}, queues)
}

func (s *Store) currentRollbackFailureCreateSourceQueueFromTaskOrRunTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, runID string) (string, string, string, string, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	runID = strings.TrimSpace(runID)
	if workspaceID == "" {
		return "", "", "", "", false, nil
	}
	resolvedTaskID, ok, err := resolveRollbackFailureCreateTaskIDFromTaskOrRun(func(currentRunID string) (ExecutionRunRecord, error) {
		return s.getExecutionRunTx(ctx, tx, workspaceID, currentRunID)
	}, taskID, runID)
	if err != nil {
		return "", "", "", "", false, err
	}
	if !ok {
		return "", "", "", "", false, nil
	}
	taskID = resolvedTaskID
	rows, err := tx.QueryContext(
		ctx,
		`SELECT `+operatorQueueSelectColumns("")+`
		   FROM operator_queue_items
		  WHERE workspace_id = ? AND queue_type = ? AND status = ? AND COALESCE(task_id,'') = ?`,
		workspaceID,
		"FOLLOW_UP",
		"OPEN",
		taskID,
	)
	if err != nil {
		return "", "", "", "", false, err
	}
	defer rows.Close()
	queues, err := collectOperatorQueueRows(rows)
	if err != nil {
		return "", "", "", "", false, err
	}
	return currentRollbackFailureCreateSourceQueueFromTaskQueues(func(actionID string) (HumanActionRecord, error) {
		return s.getHumanActionTx(ctx, tx, actionID)
	}, queues)
}

func (s *Store) latestHumanActionLineagePayloadTx(ctx context.Context, tx *sql.Tx, workspaceID, actionID string) (operatorQueueEventLineagePayload, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	actionID = strings.TrimSpace(actionID)
	if workspaceID == "" || actionID == "" {
		return operatorQueueEventLineagePayload{}, false, nil
	}
	event, ok, err := s.latestRuntimeEventTx(ctx, tx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       1,
	})
	if err != nil {
		return operatorQueueEventLineagePayload{}, false, err
	}
	if !ok {
		return operatorQueueEventLineagePayload{}, false, nil
	}
	lineage := operatorQueueLineageFromRuntimeEvent(event)
	return lineage, operatorQueueLineagePresent(lineage), nil
}

func (s *Store) ResolveOperatorQueueItem(ctx context.Context, input OperatorQueueResolveInput) (OperatorQueueRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return OperatorQueueRecord{}, errors.New("workspace_id is required")
	}
	if strings.TrimSpace(input.QueueID) == "" && strings.TrimSpace(input.QueueKey) == "" {
		return OperatorQueueRecord{}, errors.New("queue_id or queue_key is required")
	}
	resolvedBy := strings.TrimSpace(input.ResolvedBy)
	if resolvedBy == "" {
		return OperatorQueueRecord{}, errors.New("resolved_by is required")
	}
	status := normalizeOperatorQueueStatus(input.Status)
	if status == "OPEN" {
		status = "RESOLVED"
	}
	requiredStatus := strings.TrimSpace(input.RequireCurrentStatus)
	if requiredStatus == "" {
		requiredStatus = "OPEN"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, err
	}

	record := OperatorQueueRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		var innerErr error
		record, _, innerErr = s.resolveOperatorQueueItemWithAuthorityTx(ctx, tx, authority, workspaceID, strings.TrimSpace(input.QueueID), strings.TrimSpace(input.QueueKey), status, resolvedBy, strings.TrimSpace(input.Resolution), "", "", "", requiredStatus, input.RequireCurrentRevision, strings.TrimSpace(input.RequireCurrentUpdatedAt), input.PromptContextEnvelope, now)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, err
	}
	return s.getOperatorQueueItem(ctx, workspaceID, record.QueueID, "")
}

func (s *Store) ResolveOperatorQueueItemWithEvent(ctx context.Context, input OperatorQueueResolveInput) (OperatorQueueRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	queueID := strings.TrimSpace(input.QueueID)
	if queueID == "" {
		var err error
		queueID, err = s.lookupOperatorQueueIDByKey(ctx, workspaceID, strings.TrimSpace(input.QueueKey))
		if err != nil {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, err
		}
	}
	resolvedBy := strings.TrimSpace(input.ResolvedBy)
	if resolvedBy == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, errors.New("resolved_by is required")
	}
	status := normalizeOperatorQueueStatus(input.Status)
	if status == "OPEN" {
		status = "RESOLVED"
	}
	requiredStatus := strings.TrimSpace(input.RequireCurrentStatus)
	if requiredStatus == "" {
		requiredStatus = "OPEN"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	record := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		var innerErr error
		record, event, innerErr = s.resolveOperatorQueueItemWithAuthorityTx(
			ctx,
			tx,
			authority,
			workspaceID,
			queueID,
			strings.TrimSpace(input.QueueKey),
			status,
			resolvedBy,
			strings.TrimSpace(input.Resolution),
			strings.TrimSpace(input.Summary),
			strings.TrimSpace(input.Details),
			strings.TrimSpace(input.PayloadJSON),
			requiredStatus,
			input.RequireCurrentRevision,
			strings.TrimSpace(input.RequireCurrentUpdatedAt),
			input.PromptContextEnvelope,
			now,
		)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	record, err = s.getOperatorQueueItem(ctx, workspaceID, record.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func (s *Store) EscalateOperatorQueueItem(ctx context.Context, input OperatorQueueEscalateInput) (OperatorQueueRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return OperatorQueueRecord{}, errors.New("workspace_id is required")
	}
	if strings.TrimSpace(input.QueueID) == "" && strings.TrimSpace(input.QueueKey) == "" {
		return OperatorQueueRecord{}, errors.New("queue_id or queue_key is required")
	}
	escalatedBy := strings.TrimSpace(input.EscalatedBy)
	if escalatedBy == "" {
		return OperatorQueueRecord{}, errors.New("escalated_by is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, err
	}

	record := OperatorQueueRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		var innerErr error
		record, _, _, innerErr = s.escalateOperatorQueueItemWithAuthorityTx(
			ctx,
			tx,
			authority,
			workspaceID,
			strings.TrimSpace(input.QueueID),
			strings.TrimSpace(input.QueueKey),
			escalatedBy,
			strings.TrimSpace(input.Reason),
			strings.TrimSpace(input.AssignedTo),
			strings.TrimSpace(input.Urgency),
			strings.TrimSpace(input.DueAt),
			input.RequireCurrentRevision,
			strings.TrimSpace(input.RequireCurrentUpdatedAt),
			input.PromptContextEnvelope,
			now,
		)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, err
	}
	return s.getOperatorQueueItem(ctx, workspaceID, record.QueueID, "")
}

func (s *Store) EscalateOperatorQueueItemWithEvent(ctx context.Context, input OperatorQueueEscalateInput) (OperatorQueueRecord, RuntimeEventRecord, *OperatorQueueSyncEvent, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	queueID := strings.TrimSpace(input.QueueID)
	if queueID == "" {
		var err error
		queueID, err = s.lookupOperatorQueueIDByKey(ctx, workspaceID, strings.TrimSpace(input.QueueKey))
		if err != nil {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	escalatedBy := strings.TrimSpace(input.EscalatedBy)
	if escalatedBy == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, errors.New("escalated_by is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}

	record := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	var linkedActionQueueEvent *OperatorQueueSyncEvent
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		var innerErr error
		record, event, linkedActionQueueEvent, innerErr = s.escalateOperatorQueueItemWithAuthorityTx(
			ctx,
			tx,
			authority,
			workspaceID,
			queueID,
			strings.TrimSpace(input.QueueKey),
			escalatedBy,
			strings.TrimSpace(input.Reason),
			strings.TrimSpace(input.AssignedTo),
			strings.TrimSpace(input.Urgency),
			strings.TrimSpace(input.DueAt),
			input.RequireCurrentRevision,
			strings.TrimSpace(input.RequireCurrentUpdatedAt),
			input.PromptContextEnvelope,
			now,
		)
		return innerErr
	}); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}
	record, err = s.getOperatorQueueItem(ctx, workspaceID, record.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}
	event.PayloadJSON = mustJSON(operatorQueuePayload(record))
	if linkedActionQueueEvent != nil {
		linkedRecord, err := s.getOperatorQueueItem(ctx, workspaceID, linkedActionQueueEvent.Record.QueueID, "")
		if err != nil {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
		}
		linkedActionQueueEvent.Record = linkedRecord
		linkedActionQueueEvent.Event.PayloadJSON = mustJSON(operatorQueuePayload(linkedRecord))
	}
	return record, event, linkedActionQueueEvent, nil
}

func (s *Store) upsertOperatorQueueItemWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record OperatorQueueRecord, now string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	if record.AgentID != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, record.WorkspaceID, record.AgentID); err != nil {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, err
		}
	}
	if record.TaskID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, record.WorkspaceID, record.TaskID); err != nil {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, err
		}
	}
	if record.SessionID != "" {
		if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, record.WorkspaceID, record.SessionID); err != nil {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, err
		}
	}

	var previousStatus string
	var existingQueueID, createdAt, currentUpdatedAt, currentPayloadJSON string
	var currentRevision int64
	err := tx.QueryRowContext(
		ctx,
		`SELECT queue_id, status, created_at, updated_at, revision, payload_json
		   FROM operator_queue_items
		  WHERE workspace_id = ? AND queue_key = ?`,
		record.WorkspaceID,
		record.QueueKey,
	).Scan(&existingQueueID, &previousStatus, &createdAt, &currentUpdatedAt, &currentRevision, &currentPayloadJSON)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if record.RequireCurrentRevision > 0 || strings.TrimSpace(record.RequireCurrentUpdatedAt) != "" {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueRevisionMismatchError(firstNonEmpty(record.QueueID, record.QueueKey))
		}
		record.QueueID = firstNonEmpty(record.QueueID, nextID("opq"))
		record.Revision = 1
		record.CreatedAt = now
	case err != nil:
		return OperatorQueueRecord{}, RuntimeEventRecord{}, fmt.Errorf("query operator queue item: %w", err)
	default:
		record.QueueID = existingQueueID
		record.CreatedAt = createdAt
		record.Revision = currentRevision + 1
		if record.RequireMissing {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueRevisionMismatchError(firstNonEmpty(record.QueueID, record.QueueKey))
		}
		normalizedPreviousStatus := normalizeOperatorQueueStatus(previousStatus)
		requiredStatus := strings.TrimSpace(record.RequireCurrentStatus)
		if requiredStatus != "" && normalizedPreviousStatus != normalizeOperatorQueueStatus(requiredStatus) {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueStatusMismatchError(requiredStatus, record.QueueID)
		}
		if requiredRevision := record.RequireCurrentRevision; requiredRevision > 0 && currentRevision != requiredRevision {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueRevisionMismatchError(record.QueueID)
		}
		requiredUpdatedAt := strings.TrimSpace(record.RequireCurrentUpdatedAt)
		if requiredUpdatedAt != "" && currentUpdatedAt != requiredUpdatedAt {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueRevisionMismatchError(record.QueueID)
		}
		if normalizedPreviousStatus != "OPEN" && requiredStatus == "" {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueStatusMismatchError("OPEN", record.QueueID)
		}
	}
	if record.promptContextEnvelope != nil && strings.TrimSpace(record.PayloadJSON) == "" {
		record.PayloadJSON = currentPayloadJSON
	}
	payloadJSON, err := mergeOperatorQueuePromptContextPayloadJSON(record.PayloadJSON, record.promptContextEnvelope)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	record.PayloadJSON = payloadJSON
	record.UpdatedAt = now

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO operator_queue_items(
		    queue_id, workspace_id, queue_key, queue_type, status, title, summary, details,
		    payload_json, assigned_to, urgency, source_kind, source_id, task_id, session_id, agent_id,
		    keep_session_active, due_at, resolution, resolved_at, resolved_by, revision, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', NULL, NULL, ?, ?, ?)
		  ON CONFLICT(workspace_id, queue_key) DO UPDATE SET
		    queue_type = excluded.queue_type,
		    status = excluded.status,
		    title = excluded.title,
		    summary = excluded.summary,
		    details = excluded.details,
		    payload_json = CASE
		      WHEN excluded.payload_json IS NOT NULL AND excluded.payload_json != '' THEN excluded.payload_json
		      ELSE operator_queue_items.payload_json
		    END,
		    assigned_to = excluded.assigned_to,
		    urgency = excluded.urgency,
		    source_kind = excluded.source_kind,
		    source_id = excluded.source_id,
		    task_id = excluded.task_id,
		    session_id = excluded.session_id,
		    agent_id = excluded.agent_id,
		    keep_session_active = excluded.keep_session_active,
		    due_at = excluded.due_at,
		    resolution = '',
		    resolved_at = NULL,
		    resolved_by = NULL,
		    revision = operator_queue_items.revision + 1,
		    updated_at = excluded.updated_at`,
		record.QueueID,
		record.WorkspaceID,
		record.QueueKey,
		record.QueueType,
		record.Status,
		record.Title,
		record.Summary,
		record.Details,
		strings.TrimSpace(record.PayloadJSON),
		record.AssignedTo,
		record.Urgency,
		record.SourceKind,
		record.SourceID,
		blankStringOrNil(record.TaskID),
		blankStringOrNil(record.SessionID),
		blankStringOrNil(record.AgentID),
		boolToInt(record.KeepSessionActive),
		blankStringOrNil(derefString(record.DueAt)),
		record.Revision,
		record.CreatedAt,
		record.UpdatedAt,
	); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, fmt.Errorf("upsert operator queue item: %w", err)
	}
	record, err = s.getOperatorQueueItemTx(ctx, tx, record.WorkspaceID, record.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	eventType := "operator_queue.updated"
	normalizedPreviousStatus := normalizeOperatorQueueStatus(previousStatus)
	if previousStatus == "" {
		eventType = "operator_queue.created"
	} else if normalizedPreviousStatus != "OPEN" {
		eventType = "operator_queue.reopened"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, record.WorkspaceID); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch workspace after operator queue upsert: %w", err)
	}
	appendInput := RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   eventType,
		EntityType:  "operator_queue",
		EntityID:    record.QueueID,
		ActorType:   "system",
		ActorID:     firstNonEmpty(record.AgentID, record.SourceID, record.WorkspaceID),
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		PayloadJSON: mustJSON(operatorQueuePayload(record)),
		CreatedAt:   now,
	}
	var event RuntimeEventRecord
	if strings.TrimSpace(authority.WorkspaceID) == "" && strings.TrimSpace(authority.HolderAuthorityNodeID) == "" && authority.Term <= 0 && strings.TrimSpace(authority.LeaseToken) == "" {
		event, err = s.appendRuntimeEventTx(ctx, tx, appendInput)
	} else {
		event, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, appendInput)
	}
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func (s *Store) resolveOperatorQueueItemWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, queueID, queueKey, status, resolvedBy, resolution, summary, details, payloadJSON, requiredStatus string, requiredRevision int64, requiredUpdatedAt string, promptContextEnvelope map[string]any, now string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	where := `workspace_id = ? AND queue_id = ?`
	arg := strings.TrimSpace(queueID)
	if arg == "" {
		where = `workspace_id = ? AND queue_key = ?`
		arg = strings.TrimSpace(queueKey)
	}

	resolvedQueueID := ""
	currentStatus := ""
	currentUpdatedAt := ""
	currentPayloadJSON := ""
	currentRevision := int64(0)
	if err := tx.QueryRowContext(ctx, `SELECT queue_id, status, updated_at, revision, payload_json FROM operator_queue_items WHERE `+where, workspaceID, arg).Scan(&resolvedQueueID, &currentStatus, &currentUpdatedAt, &currentRevision, &currentPayloadJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueItemNotFoundError(arg)
		}
		return OperatorQueueRecord{}, RuntimeEventRecord{}, fmt.Errorf("query operator queue item for resolve: %w", err)
	}
	requiredStatus = strings.TrimSpace(requiredStatus)
	if requiredStatus != "" && normalizeOperatorQueueStatus(currentStatus) != normalizeOperatorQueueStatus(requiredStatus) {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueStatusMismatchError(requiredStatus, resolvedQueueID)
	}
	if requiredRevision > 0 && currentRevision != requiredRevision {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueRevisionMismatchError(resolvedQueueID)
	}
	requiredUpdatedAt = strings.TrimSpace(requiredUpdatedAt)
	if requiredUpdatedAt != "" && currentUpdatedAt != requiredUpdatedAt {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, operatorQueueRevisionMismatchError(resolvedQueueID)
	}
	if promptContextEnvelope != nil && strings.TrimSpace(payloadJSON) == "" {
		payloadJSON = currentPayloadJSON
	}
	payloadJSON, err := mergeOperatorQueuePromptContextPayloadJSON(payloadJSON, promptContextEnvelope)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE operator_queue_items
		    SET status = ?,
		        summary = CASE WHEN ? != '' THEN ? ELSE summary END,
		        details = CASE WHEN ? != '' THEN ? ELSE details END,
		        payload_json = CASE WHEN ? != '' THEN ? ELSE payload_json END,
		        resolution = ?, resolved_at = ?, resolved_by = ?, revision = revision + 1, updated_at = ?
		  WHERE workspace_id = ? AND queue_id = ?`,
		status,
		summary,
		summary,
		details,
		details,
		payloadJSON,
		payloadJSON,
		resolution,
		now,
		resolvedBy,
		now,
		workspaceID,
		resolvedQueueID,
	); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, fmt.Errorf("resolve operator queue item: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch workspace after operator queue resolve: %w", err)
	}
	record, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, resolvedQueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	appendInput := RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "operator_queue." + strings.ToLower(status),
		EntityType:  "operator_queue",
		EntityID:    resolvedQueueID,
		ActorType:   "operator",
		ActorID:     resolvedBy,
		PayloadJSON: mustJSON(operatorQueuePayload(record)),
		CreatedAt:   now,
	}
	var event RuntimeEventRecord
	if strings.TrimSpace(authority.WorkspaceID) == "" && strings.TrimSpace(authority.HolderAuthorityNodeID) == "" && authority.Term <= 0 && strings.TrimSpace(authority.LeaseToken) == "" {
		event, err = s.appendRuntimeEventTx(ctx, tx, appendInput)
	} else {
		event, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, appendInput)
	}
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func (s *Store) escalateOperatorQueueItemWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, queueID, queueKey, escalatedBy, reason, assignedTo, urgency, dueAt string, requiredRevision int64, requiredUpdatedAt string, promptContextEnvelope map[string]any, now string) (OperatorQueueRecord, RuntimeEventRecord, *OperatorQueueSyncEvent, error) {
	record, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, queueID, queueKey)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}
	if normalizeOperatorQueueStatus(record.Status) != "OPEN" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("operator queue item is not open: %s", record.QueueID)
	}
	if requiredRevision > 0 && record.Revision != requiredRevision {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, operatorQueueRevisionMismatchError(record.QueueID)
	}
	if trimmedRequiredUpdatedAt := strings.TrimSpace(requiredUpdatedAt); trimmedRequiredUpdatedAt != "" && strings.TrimSpace(record.UpdatedAt) != trimmedRequiredUpdatedAt {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, operatorQueueRevisionMismatchError(record.QueueID)
	}
	record.AssignedTo = firstNonEmpty(strings.TrimSpace(assignedTo), strings.TrimSpace(record.AssignedTo))
	record.Urgency = nextOperatorQueueUrgency(record.Urgency, urgency)
	if err := s.syncEscalatedStandaloneHumanActionQueueTx(ctx, tx, &record); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}
	if _, hasRollback := decodeLinkedRollbackFailurePayload(record.PayloadJSON, record.QueueKey); !hasRollback {
		if rebasePayload, hasRebase := decodeLinkedRebaseFollowupPayload(record.PayloadJSON, record.QueueKey); hasRebase {
			rebasePayload.ActionAssignedTo = strings.TrimSpace(record.AssignedTo)
			rebasePayload.Normalize()
			record.PayloadJSON = marshalActionQueuePayload(rebasePayload)
		}
	}
	if trimmedDueAt := strings.TrimSpace(dueAt); trimmedDueAt != "" {
		record.DueAt = stringPtr(trimmedDueAt)
	}
	if trimmedReason := strings.TrimSpace(reason); trimmedReason != "" {
		record.EscalationReason = trimmedReason
	}
	record.EscalationCount++
	record.LastEscalatedAt = stringPtr(now)
	record.LastEscalatedBy = stringPtr(escalatedBy)
	record.UpdatedAt = now
	record.PayloadJSON, err = mergeOperatorQueuePromptContextPayloadJSON(record.PayloadJSON, promptContextEnvelope)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE operator_queue_items
		    SET payload_json = ?, assigned_to = ?, urgency = ?, due_at = ?, escalation_count = ?, last_escalated_at = ?, last_escalated_by = ?, escalation_reason = ?, revision = revision + 1, updated_at = ?
		  WHERE workspace_id = ? AND queue_id = ?`,
		blankStringOrNil(strings.TrimSpace(record.PayloadJSON)),
		record.AssignedTo,
		record.Urgency,
		blankStringOrNil(derefString(record.DueAt)),
		record.EscalationCount,
		blankStringOrNil(derefString(record.LastEscalatedAt)),
		blankStringOrNil(derefString(record.LastEscalatedBy)),
		record.EscalationReason,
		record.UpdatedAt,
		workspaceID,
		record.QueueID,
	); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("escalate operator queue item: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("touch workspace after operator queue escalation: %w", err)
	}
	record, err = s.getOperatorQueueItemTx(ctx, tx, workspaceID, record.QueueID, "")
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}
	appendInput := RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    record.QueueID,
		ActorType:   "operator",
		ActorID:     escalatedBy,
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		PayloadJSON: mustJSON(operatorQueuePayload(record)),
		CreatedAt:   now,
	}
	var event RuntimeEventRecord
	if strings.TrimSpace(authority.WorkspaceID) == "" && strings.TrimSpace(authority.HolderAuthorityNodeID) == "" && authority.Term <= 0 && strings.TrimSpace(authority.LeaseToken) == "" {
		event, err = s.appendRuntimeEventTx(ctx, tx, appendInput)
	} else {
		event, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, appendInput)
	}
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}
	linkedActionQueueEvent, err := s.syncEscalatedLinkedHumanActionTx(ctx, tx, authority, &record, now)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil, err
	}
	return record, event, linkedActionQueueEvent, nil
}

func (s *Store) ListOperatorQueueItems(ctx context.Context, filter OperatorQueueFilter) ([]OperatorQueueRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	query := strings.Builder{}
	query.WriteString(`SELECT ` + operatorQueueSelectColumns("") + `
	   FROM operator_queue_items
	  WHERE workspace_id = ?`)
	args := []any{workspaceID}
	if trimmed := strings.TrimSpace(filter.QueueType); trimmed != "" {
		query.WriteString(` AND queue_type = ?`)
		args = append(args, normalizeOperatorQueueType(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.Status); trimmed != "" {
		query.WriteString(` AND status = ?`)
		args = append(args, normalizeOperatorQueueStatus(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.AssignedTo); trimmed != "" {
		query.WriteString(` AND assigned_to = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
		query.WriteString(` AND COALESCE(session_id,'') = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.TaskID); trimmed != "" {
		query.WriteString(` AND COALESCE(task_id,'') = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		query.WriteString(` AND COALESCE(agent_id,'') = ?`)
		args = append(args, trimmed)
	}
	query.WriteString(` ORDER BY
	        CASE status WHEN 'OPEN' THEN 0 WHEN 'RESOLVED' THEN 1 ELSE 2 END,
	        CASE WHEN due_at IS NULL OR due_at = '' THEN 1 ELSE 0 END,
	        due_at ASC,
	        escalation_count DESC,
	        updated_at DESC, queue_id DESC`)
	if filter.Limit > 0 {
		query.WriteString(` LIMIT ?`)
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query operator queue items: %w", err)
	}
	defer rows.Close()
	items, err := collectOperatorQueueRows(rows)
	if err != nil {
		return nil, err
	}
	return s.withOperatorQueueTimeAuthority(ctx, workspaceID, items)
}

func (s *Store) SyncOperatorQueueFromSessionState(ctx context.Context, state AgentSessionStateRecord) (OperatorQueueSyncResult, error) {
	sessionID := strings.TrimSpace(state.SessionID)
	workspaceID := strings.TrimSpace(state.WorkspaceID)
	if sessionID == "" || workspaceID == "" {
		return OperatorQueueSyncResult{}, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueSyncResult{}, err
	}
	result := OperatorQueueSyncResult{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		result, innerErr = s.syncOperatorQueueFromSessionStateWithAuthorityTx(ctx, tx, authority, state, now, "")
		return innerErr
	}); err != nil {
		return OperatorQueueSyncResult{}, err
	}
	return result, nil
}

// operatorQueueTypeForSessionStatus maps a canonical session status to the
// operator-queue type that status keeps OPEN, or "" if the status is not a
// blocking phase. Used by the resolve loop (CA-18) so a keepalive/status event
// does not archive a queue whose blocking phase the session is still in.
func operatorQueueTypeForSessionStatus(status string) string {
	switch model.NormalizeStatus(status) {
	case model.SessionStatusBlocked:
		return "BLOCKER"
	case model.SessionStatusWaitingDecision:
		return "DECISION"
	case model.SessionStatusHandoffPending:
		return "HANDOFF"
	default:
		return ""
	}
}

func (s *Store) syncOperatorQueueFromSessionStateWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, state AgentSessionStateRecord, now, resolvedByOverride string) (OperatorQueueSyncResult, error) {
	sessionID := strings.TrimSpace(state.SessionID)
	workspaceID := strings.TrimSpace(state.WorkspaceID)
	if sessionID == "" || workspaceID == "" {
		return OperatorQueueSyncResult{}, nil
	}

	targetType := ""
	eventType := model.NormalizeSessionEventType(state.UpdateType)
	switch {
	case eventType == model.SessionEventBlocked:
		targetType = "BLOCKER"
	case eventType == model.SessionEventDecisionNeeded:
		targetType = "DECISION"
	case eventType == model.SessionEventStatus && (model.NormalizeStatus(state.Status) == model.SessionStatusHandoffPending || strings.TrimSpace(state.HandoffTo) != ""):
		targetType = "HANDOFF"
	}

	result := OperatorQueueSyncResult{}
	// CA-06: a delayed blocked/decision/handoff event whose record-tx committed while
	// the session was still live can run this queue-sync AFTER a reclaim already ended
	// the session and resolved its queues. Re-read the canonical session status in-tx
	// and refuse to (re)open a session-derived queue for an already-ended session, so
	// the reaper's resolution is not silently undone. The resolve loop below still
	// runs (idempotent against already-resolved queues).
	sessionTerminal := false
	if sessionState, err := s.getAgentSessionStateTx(ctx, tx, workspaceID, sessionID); err == nil {
		sessionTerminal = strings.TrimSpace(sessionState.CompletedAt) != "" ||
			strings.EqualFold(strings.TrimSpace(sessionState.Status), "ENDED")
	}
	if targetType != "" && !sessionTerminal {
		input := buildSessionOperatorQueueInput(state, targetType)
		if err := s.applyOperatorQueueTerminalReopenGuardTx(ctx, tx, workspaceID, input.QueueKey, &input); err != nil {
			return OperatorQueueSyncResult{}, err
		}
		currentQueue, found, err := s.loadOperatorQueueItemByKeyTx(ctx, tx, workspaceID, input.QueueKey)
		if err != nil {
			return OperatorQueueSyncResult{}, err
		}
		if found {
			input.RequireCurrentRevision = currentQueue.Revision
			input.RequireCurrentUpdatedAt = currentQueue.UpdatedAt
		}
		record, event, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(input), now)
		if err != nil {
			return OperatorQueueSyncResult{}, err
		}
		result.Opened = append(result.Opened, OperatorQueueSyncEvent{Record: record, Event: event})
	}

	resolvedBy := firstNonEmpty(strings.TrimSpace(resolvedByOverride), strings.TrimSpace(state.AgentID), workspaceID)
	stillInPhase := operatorQueueTypeForSessionStatus(state.Status)
	for _, queueType := range []string{"BLOCKER", "DECISION", "HANDOFF"} {
		if queueType == targetType {
			continue
		}
		// CA-18: a keepalive/status event that is not itself a blocked/decision/handoff
		// transition still ran this resolve loop for all three queue types. If the
		// event's canonical session status shows the session is STILL in this queue's
		// phase (e.g. a keepalive carrying Status=BLOCKED), do not archive the
		// still-required queue. A default keepalive (Status=ACTIVE) maps to no phase, so
		// resolution stays consistent.
		if queueType == stillInPhase {
			continue
		}
		currentQueue, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, "", sessionOperatorQueueKey(sessionID, queueType))
		if err != nil {
			if errors.Is(err, ErrOperatorQueueItemNotFound) {
				continue
			}
			return OperatorQueueSyncResult{}, err
		}
		if !operatorQueueMatchesSessionDerivedLineage(currentQueue, sessionID, queueType) {
			continue
		}
		record, event, err := s.resolveOperatorQueueItemWithAuthorityTx(
			ctx,
			tx,
			authority,
			workspaceID,
			currentQueue.QueueID,
			"",
			"RESOLVED",
			resolvedBy,
			"cleared_by_"+strings.ReplaceAll(eventType, ".", "_"),
			"",
			"",
			"",
			"OPEN",
			currentQueue.Revision,
			currentQueue.UpdatedAt,
			nil,
			now,
		)
		if err != nil {
			if errors.Is(err, ErrOperatorQueueItemNotFound) || strings.Contains(err.Error(), "operator queue item is not open") {
				continue
			}
			return OperatorQueueSyncResult{}, err
		}
		result.Resolved = append(result.Resolved, OperatorQueueSyncEvent{Record: record, Event: event})
	}
	if sessionTerminal {
		sessionQueues, err := s.listOpenSessionSourcedOperatorQueuesTx(ctx, tx, workspaceID, sessionID)
		if err != nil {
			return OperatorQueueSyncResult{}, err
		}
		for _, currentQueue := range sessionQueues {
			record, event, err := s.resolveOperatorQueueItemWithAuthorityTx(
				ctx,
				tx,
				authority,
				workspaceID,
				currentQueue.QueueID,
				"",
				"RESOLVED",
				resolvedBy,
				"cleared_by_session_end",
				"",
				"",
				"",
				"OPEN",
				currentQueue.Revision,
				currentQueue.UpdatedAt,
				nil,
				now,
			)
			if err != nil {
				if errors.Is(err, ErrOperatorQueueItemNotFound) || strings.Contains(err.Error(), "operator queue item is not open") {
					continue
				}
				return OperatorQueueSyncResult{}, err
			}
			result.Resolved = append(result.Resolved, OperatorQueueSyncEvent{Record: record, Event: event})
		}
	}
	return result, nil
}

func (s *Store) appendRuntimeEventTx(ctx context.Context, tx *sql.Tx, input RuntimeEventInput) (RuntimeEventRecord, error) {
	record := RuntimeEventRecord{
		EventID:                        strings.TrimSpace(input.EventID),
		DedupKey:                       strings.TrimSpace(input.DedupKey),
		WorkspaceID:                    strings.TrimSpace(input.WorkspaceID),
		EventType:                      strings.TrimSpace(input.EventType),
		EntityType:                     strings.TrimSpace(input.EntityType),
		EntityID:                       strings.TrimSpace(input.EntityID),
		ActorType:                      strings.TrimSpace(input.ActorType),
		ActorID:                        strings.TrimSpace(input.ActorID),
		AgentID:                        strings.TrimSpace(input.AgentID),
		SessionID:                      strings.TrimSpace(input.SessionID),
		TaskID:                         strings.TrimSpace(input.TaskID),
		RootCauseID:                    strings.TrimSpace(input.RootCauseID),
		ProvenanceGroupID:              strings.TrimSpace(input.ProvenanceGroupID),
		ParentRefsJSON:                 strings.TrimSpace(input.ParentRefsJSON),
		PayloadJSON:                    strings.TrimSpace(input.PayloadJSON),
		CreatedAt:                      strings.TrimSpace(input.CreatedAt),
		AuthorityHolderNodeID:          strings.TrimSpace(input.AuthorityHolderNodeID),
		AuthorityTerm:                  input.AuthorityTerm,
		AuthorityLeaseTokenFingerprint: strings.TrimSpace(input.AuthorityLeaseTokenFingerprint),
	}
	if record.WorkspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if record.EventType == "" {
		return RuntimeEventRecord{}, errors.New("event_type is required")
	}
	if record.EntityType == "" {
		return RuntimeEventRecord{}, errors.New("entity_type is required")
	}
	if record.EntityID == "" {
		return RuntimeEventRecord{}, errors.New("entity_id is required")
	}
	record.PayloadJSON = normalizeRuntimeEventPayload(record.PayloadJSON)
	if err := validateRuntimeEventToolCallContract(record); err != nil {
		return RuntimeEventRecord{}, err
	}
	var err error
	record.ParentRefsJSON, err = normalizeRuntimeEventParentRefs(record.ParentRefsJSON)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	lineage := runtimeReplayEventLineage(record)
	if record.DedupKey == "" {
		record.DedupKey = lineage.DedupKey
	}
	if record.RootCauseID == "" {
		record.RootCauseID = lineage.RootCauseID
	}
	if record.ProvenanceGroupID == "" {
		record.ProvenanceGroupID = lineage.ProvenanceGroupID
	}
	if record.ParentRefsJSON == "[]" && lineage.ParentRefsJSON != "" {
		record.ParentRefsJSON = lineage.ParentRefsJSON
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return RuntimeEventRecord{}, err
	}
	if record.AgentID != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, record.WorkspaceID, record.AgentID); err != nil {
			return RuntimeEventRecord{}, err
		}
	}
	if record.TaskID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, record.WorkspaceID, record.TaskID); err != nil {
			return RuntimeEventRecord{}, err
		}
	}
	if record.SessionID != "" {
		if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, record.WorkspaceID, record.SessionID); err != nil {
			return RuntimeEventRecord{}, err
		}
	}
	if record.EventID == "" {
		record.EventID = nextID("rtev")
	}
	if err := s.validateRuntimeEventParentRefsTx(ctx, tx, record.WorkspaceID, record.EventID, record.ParentRefsJSON); err != nil {
		return RuntimeEventRecord{}, err
	}
	dedupKey := runtimeReplayEventDedupKey(record)
	if dedupKey != "" {
		existing, err := s.getRuntimeEventByDedupKeyTx(ctx, tx, record.WorkspaceID, dedupKey)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, err
		}
		if err == nil {
			if runtimeEventsSemanticallyEquivalent(existing, record) {
				return existing, nil
			}
			return RuntimeEventRecord{}, fmt.Errorf("dedup_key %q already belongs to a runtime event with different semantics", dedupKey)
		}
	}
	if record.EventID != "" {
		existing, err := s.getRuntimeEventByIDTx(ctx, tx, record.WorkspaceID, record.EventID)
		if err == nil {
			if !runtimeEventsSemanticallyEquivalent(existing, record) {
				return RuntimeEventRecord{}, runtimeEventIdentityConflict(record, existing)
			}
			return existing, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, err
		}
	}
	if record.CreatedAt == "" {
		record.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	record.IngestSeq, err = s.nextRuntimeEventIngestSeqTx(ctx, tx, record.WorkspaceID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO runtime_events(event_id, dedup_key, workspace_id, event_type, entity_type, entity_id, actor_type, actor_id, agent_id, session_id, task_id, root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, authority_holder_node_id, authority_term, authority_lease_token_fingerprint, ingest_seq)
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.EventID,
		blankStringOrNil(record.DedupKey),
		record.WorkspaceID,
		record.EventType,
		record.EntityType,
		record.EntityID,
		record.ActorType,
		record.ActorID,
		blankStringOrNil(record.AgentID),
		blankStringOrNil(record.SessionID),
		blankStringOrNil(record.TaskID),
		blankStringOrNil(record.RootCauseID),
		blankStringOrNil(record.ProvenanceGroupID),
		record.ParentRefsJSON,
		record.PayloadJSON,
		record.CreatedAt,
		blankStringOrNil(record.AuthorityHolderNodeID),
		blankInt64OrNil(record.AuthorityTerm),
		blankStringOrNil(record.AuthorityLeaseTokenFingerprint),
		record.IngestSeq,
	); err != nil {
		if record.EventID != "" && isRuntimeEventIdentityConflictError(err) {
			existing, lookupErr := s.getRuntimeEventByIDTx(ctx, tx, record.WorkspaceID, record.EventID)
			if lookupErr == nil && runtimeEventsSemanticallyEquivalent(existing, record) {
				return existing, nil
			}
			if lookupErr == nil {
				return RuntimeEventRecord{}, runtimeEventIdentityConflict(record, existing)
			}
			if !errors.Is(lookupErr, sql.ErrNoRows) {
				return RuntimeEventRecord{}, lookupErr
			}
		}
		return RuntimeEventRecord{}, fmt.Errorf("insert runtime event: %w", err)
	}

	if !isSyntheticOperationalEvent(record) {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO runtime_event_firehose_outbox(workspace_id, event_id, ingest_seq, created_at)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(workspace_id, event_id) DO NOTHING`,
			record.WorkspaceID, record.EventID, record.IngestSeq, record.CreatedAt,
		); err != nil {
			return RuntimeEventRecord{}, fmt.Errorf("enqueue runtime event firehose outbox: %w", err)
		}
	}

	return record, nil
}

func (s *Store) getOperatorQueueItem(ctx context.Context, workspaceID, queueID, queueKey string) (OperatorQueueRecord, error) {
	record, err := s.getOperatorQueueItemTx(ctx, nil, workspaceID, queueID, queueKey)
	if err != nil {
		return OperatorQueueRecord{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, strings.TrimSpace(workspaceID))
	if err != nil {
		return OperatorQueueRecord{}, err
	}
	record.TimeAuthority = authority
	return record, nil
}

func (s *Store) GetOperatorQueueItem(ctx context.Context, workspaceID, queueID, queueKey string) (OperatorQueueRecord, error) {
	return s.getOperatorQueueItem(ctx, workspaceID, queueID, queueKey)
}

func (s *Store) getOperatorQueueItemTx(ctx context.Context, tx *sql.Tx, workspaceID, queueID, queueKey string) (OperatorQueueRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return OperatorQueueRecord{}, errors.New("workspace_id is required")
	}
	column := "queue_id"
	value := strings.TrimSpace(queueID)
	if value == "" {
		column = "queue_key"
		value = strings.TrimSpace(queueKey)
	}
	if value == "" {
		return OperatorQueueRecord{}, errors.New("queue_id or queue_key is required")
	}
	query := `SELECT ` + operatorQueueSelectColumns("") + `
	   FROM operator_queue_items
	  WHERE workspace_id = ? AND ` + column + ` = ?`
	var row interface{ Scan(dest ...any) error }
	if tx != nil {
		row = tx.QueryRowContext(ctx, query, workspaceID, value)
	} else {
		row = s.db.QueryRowContext(ctx, query, workspaceID, value)
	}
	record, err := scanOperatorQueueRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return OperatorQueueRecord{}, operatorQueueItemNotFoundError(workspaceID + "/" + value)
		}
		return OperatorQueueRecord{}, err
	}
	return record, nil
}

func collectOperatorQueueRows(rows *sql.Rows) ([]OperatorQueueRecord, error) {
	out := []OperatorQueueRecord{}
	for rows.Next() {
		record, err := scanOperatorQueueRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operator queue rows: %w", err)
	}
	return out, nil
}

func (s *Store) withOperatorQueueTimeAuthority(ctx context.Context, workspaceID string, items []OperatorQueueRecord) ([]OperatorQueueRecord, error) {
	if len(items) == 0 {
		return items, nil
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, strings.TrimSpace(workspaceID))
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].TimeAuthority = authority
	}
	return items, nil
}

func scanOperatorQueueRecord(scanner interface{ Scan(dest ...any) error }) (OperatorQueueRecord, error) {
	var record OperatorQueueRecord
	var keepAlive int
	var dueAt sql.NullString
	var resolvedAt sql.NullString
	var resolvedBy sql.NullString
	var lastEscalatedAt sql.NullString
	var lastEscalatedBy sql.NullString
	if err := scanner.Scan(
		&record.QueueID,
		&record.WorkspaceID,
		&record.QueueKey,
		&record.QueueType,
		&record.Status,
		&record.Title,
		&record.Summary,
		&record.Details,
		&record.PayloadJSON,
		&record.AssignedTo,
		&record.Urgency,
		&record.SourceKind,
		&record.SourceID,
		&record.TaskID,
		&record.SessionID,
		&record.AgentID,
		&keepAlive,
		&dueAt,
		&record.Resolution,
		&resolvedAt,
		&resolvedBy,
		&record.EscalationCount,
		&lastEscalatedAt,
		&lastEscalatedBy,
		&record.EscalationReason,
		&record.Revision,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return OperatorQueueRecord{}, err
	}
	record.KeepSessionActive = keepAlive != 0
	record.PayloadJSON = firstNonEmpty(strings.TrimSpace(record.PayloadJSON), "{}")
	record.DueAt = nullStringPtr(dueAt)
	record.ResolvedAt = nullStringPtr(resolvedAt)
	record.ResolvedBy = nullStringPtr(resolvedBy)
	record.LastEscalatedAt = nullStringPtr(lastEscalatedAt)
	record.LastEscalatedBy = nullStringPtr(lastEscalatedBy)
	return record, nil
}

func buildSessionOperatorQueueInput(state AgentSessionStateRecord, queueType string) OperatorQueueUpsertInput {
	keepAlive := true
	if state.KeepSessionActive != nil {
		keepAlive = *state.KeepSessionActive
	}
	return OperatorQueueUpsertInput{
		WorkspaceID:       strings.TrimSpace(state.WorkspaceID),
		QueueKey:          sessionOperatorQueueKey(state.SessionID, queueType),
		QueueType:         queueType,
		Title:             sessionOperatorQueueTitle(state, queueType),
		Summary:           clipSummary(state.Summary, 240),
		Details:           sessionOperatorQueueDetails(state, queueType),
		AssignedTo:        sessionOperatorQueueAssignee(state, queueType),
		Urgency:           sessionOperatorQueueUrgency(queueType),
		SourceKind:        "session_event",
		SourceID:          strings.TrimSpace(state.SessionID),
		TaskID:            strings.TrimSpace(state.TaskID),
		SessionID:         strings.TrimSpace(state.SessionID),
		AgentID:           strings.TrimSpace(state.AgentID),
		KeepSessionActive: keepAlive,
	}
}

func clipSummary(summary string, limit int) string {
	summary = strings.TrimSpace(summary)
	if summary == "" || limit <= 0 {
		return summary
	}
	runes := []rune(summary)
	if len(runes) <= limit {
		return summary
	}
	if limit <= 3 {
		return string(runes[:limit])
	}
	return strings.TrimSpace(string(runes[:limit-3])) + "..."
}

func promotedKnowledgeClaimID(memoryID string) string {
	memoryID = strings.TrimSpace(memoryID)
	if memoryID == "" {
		return ""
	}
	return "claim:memory:" + memoryID
}

func knowledgeClaimTypeForMemoryType(memoryType string) (string, bool) {
	switch normalizeWorkspaceMemoryType(memoryType) {
	case "DECISION":
		return "DECISION", true
	case "ALTERNATIVE_BRANCH":
		return "ALTERNATIVE_BRANCH", true
	case "DISSENT_MARKER":
		return "DISSENT_MARKER", true
	case "DISSENT_CONTENT":
		return "DISSENT_CONTENT", true
	case "LESSON":
		return "LESSON", true
	case "PROCEDURE":
		return "PROCEDURE", true
	case "ANTI_PROCEDURE":
		return "ANTI_PROCEDURE", true
	case "INCIDENT":
		return "INCIDENT", true
	case "UPDATE_DIGEST":
		return "UPDATE_DIGEST", true
	case "ENTITY":
		return "FACT", true
	default:
		return "", false
	}
}

func promotedKnowledgeClaimFromWorkspaceMemory(record WorkspaceMemoryRecord) (KnowledgeClaimRecord, bool) {
	claimType, ok := knowledgeClaimTypeForMemoryType(record.MemoryType)
	if !ok {
		return KnowledgeClaimRecord{}, false
	}
	memoryID := strings.TrimSpace(record.MemoryID)
	if memoryID == "" {
		return KnowledgeClaimRecord{}, false
	}
	subject := firstNonEmpty(strings.TrimSpace(record.Title), strings.TrimSpace(record.Summary), clipSummary(record.Body, 96))
	summary := firstNonEmpty(strings.TrimSpace(record.Summary), clipSummary(record.Body, 240))
	tags := append([]string{}, normalizeStringSlice(record.Tags)...)
	tags = append(tags, "derived_claim", "memory:"+strings.ToLower(normalizeWorkspaceMemoryType(record.MemoryType)))
	return KnowledgeClaimRecord{
		ClaimID:     promotedKnowledgeClaimID(memoryID),
		WorkspaceID: strings.TrimSpace(record.WorkspaceID),
		ClaimType:   claimType,
		Status:      "ACTIVE",
		Subject:     subject,
		Body:        strings.TrimSpace(record.Body),
		Summary:     summary,
		Confidence:  clampUnitInterval(record.Confidence),
		SourceKind:  "workspace_memory",
		SourceID:    memoryID,
		MemoryID:    memoryID,
		TaskID:      strings.TrimSpace(record.TaskID),
		SessionID:   strings.TrimSpace(record.SessionID),
		AgentID:     strings.TrimSpace(record.AgentID),
		Evidence:    []string{"memory:" + memoryID},
		Tags:        normalizeStringSlice(tags),
	}, true
}

type PromotedKnowledgeClaimSyncEffects struct {
	Claim              *KnowledgeClaimRecord `json:"claim,omitempty"`
	ClaimEvent         *RuntimeEventRecord   `json:"claim_event,omitempty"`
	Queue              *OperatorQueueRecord  `json:"queue,omitempty"`
	QueueEvent         *RuntimeEventRecord   `json:"queue_event,omitempty"`
	InvalidationEvents []RuntimeEventRecord  `json:"invalidation_events,omitempty"`
}

func sessionOperatorQueueKey(sessionID, queueType string) string {
	sessionID = strings.TrimSpace(sessionID)
	queueType = strings.ToLower(strings.TrimSpace(queueType))
	if sessionID == "" || queueType == "" {
		return ""
	}
	return "session:" + sessionID + ":" + queueType
}

func reservedSessionOperatorQueueKey(queueKey string) (sessionID, queueType string, ok bool) {
	trimmed := strings.TrimSpace(queueKey)
	if len(trimmed) <= len("session:") || !strings.EqualFold(trimmed[:len("session:")], "session:") {
		return "", "", false
	}
	lastSeparator := strings.LastIndex(trimmed, ":")
	if lastSeparator < len("session:") || lastSeparator >= len(trimmed)-1 {
		return "", "", false
	}
	sessionID = strings.TrimSpace(trimmed[len("session:"):lastSeparator])
	queueType = normalizeOperatorQueueType(trimmed[lastSeparator+1:])
	if sessionID == "" {
		return "", "", false
	}
	switch queueType {
	case "BLOCKER", "DECISION", "HANDOFF":
		return sessionID, queueType, true
	default:
		return "", "", false
	}
}

func validateGenericOperatorQueueUpsertRecord(record OperatorQueueRecord) error {
	sessionID, queueType, ok := reservedSessionOperatorQueueKey(record.QueueKey)
	if !ok {
		return nil
	}
	return fmt.Errorf(
		"queue_key has invalid value: reserved session queue namespace is workflow-managed; use session coordination for %s %s",
		sessionID,
		strings.ToLower(queueType),
	)
}

func operatorQueueMatchesSessionDerivedLineage(record OperatorQueueRecord, sessionID, queueType string) bool {
	sessionID = strings.TrimSpace(sessionID)
	return normalizeOperatorQueueType(record.QueueType) == normalizeOperatorQueueType(queueType) &&
		strings.EqualFold(strings.TrimSpace(record.SourceKind), "session_event") &&
		strings.TrimSpace(record.SourceID) == sessionID &&
		strings.TrimSpace(record.SessionID) == sessionID
}

func (s *Store) listOpenSessionSourcedOperatorQueuesTx(ctx context.Context, tx *sql.Tx, workspaceID, sessionID string) ([]OperatorQueueRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	sessionID = strings.TrimSpace(sessionID)
	if workspaceID == "" || sessionID == "" {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT `+operatorQueueSelectColumns("")+`
  FROM operator_queue_items
 WHERE workspace_id = ?
   AND status = ?
   AND COALESCE(session_id,'') = ?
   AND COALESCE(source_id,'') = ?
   AND LOWER(COALESCE(source_kind,'')) IN ('session', 'session_event')
 ORDER BY updated_at ASC, queue_id ASC`,
		workspaceID,
		"OPEN",
		sessionID,
		sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("query open session-sourced operator queues: %w", err)
	}
	defer rows.Close()
	queues, err := collectOperatorQueueRows(rows)
	if err != nil {
		return nil, err
	}
	return queues, nil
}

func sessionOperatorQueueTitle(state AgentSessionStateRecord, queueType string) string {
	base := "Operator follow-up"
	switch normalizeOperatorQueueType(queueType) {
	case "BLOCKER":
		base = "Blocked session"
	case "DECISION":
		base = "Decision needed"
	case "HANDOFF":
		base = "Handoff pending"
	}
	if taskID := strings.TrimSpace(state.TaskID); taskID != "" {
		return base + " for task " + taskID
	}
	if sessionID := strings.TrimSpace(state.SessionID); sessionID != "" {
		return base + " for " + sessionID
	}
	return base
}

func sessionOperatorQueueDetails(state AgentSessionStateRecord, queueType string) string {
	lines := []string{
		fmt.Sprintf("Queue type: %s", normalizeOperatorQueueType(queueType)),
		fmt.Sprintf("Session ID: %s", strings.TrimSpace(state.SessionID)),
		fmt.Sprintf("Agent ID: %s", strings.TrimSpace(state.AgentID)),
	}
	if taskID := strings.TrimSpace(state.TaskID); taskID != "" {
		lines = append(lines, fmt.Sprintf("Task ID: %s", taskID))
	}
	if summary := strings.TrimSpace(state.Summary); summary != "" {
		lines = append(lines, fmt.Sprintf("Summary: %s", summary))
	}
	if decisionFrom := strings.TrimSpace(state.DecisionNeededFrom); decisionFrom != "" {
		lines = append(lines, fmt.Sprintf("Decision needed from: %s", decisionFrom))
	}
	if decisionType := strings.TrimSpace(state.DecisionType); decisionType != "" {
		lines = append(lines, fmt.Sprintf("Decision type: %s", decisionType))
	}
	if handoffTo := strings.TrimSpace(state.HandoffTo); handoffTo != "" {
		lines = append(lines, fmt.Sprintf("Handoff to: %s", handoffTo))
	}
	if len(state.BlockedOn) > 0 {
		lines = append(lines, "Blocked on:")
		for _, item := range state.BlockedOn {
			lines = append(lines, fmt.Sprintf("- %s: %s", strings.TrimSpace(item.Kind), strings.TrimSpace(item.Detail)))
		}
	}
	return strings.Join(lines, "\n")
}

func sessionOperatorQueueAssignee(state AgentSessionStateRecord, queueType string) string {
	switch normalizeOperatorQueueType(queueType) {
	case "DECISION":
		return strings.TrimSpace(state.DecisionNeededFrom)
	case "HANDOFF":
		return strings.TrimSpace(state.HandoffTo)
	default:
		return ""
	}
}

func sessionOperatorQueueUrgency(queueType string) string {
	switch normalizeOperatorQueueType(queueType) {
	case "DECISION", "HANDOFF":
		return "HIGH"
	default:
		return "NORMAL"
	}
}

func normalizeOperatorQueueUpsertInput(input OperatorQueueUpsertInput) OperatorQueueRecord {
	dueAt := strings.TrimSpace(input.DueAt)
	record := OperatorQueueRecord{
		QueueID:                 strings.TrimSpace(input.QueueID),
		WorkspaceID:             strings.TrimSpace(input.WorkspaceID),
		QueueKey:                strings.TrimSpace(input.QueueKey),
		QueueType:               normalizeOperatorQueueType(input.QueueType),
		Status:                  "OPEN",
		Title:                   strings.TrimSpace(input.Title),
		Summary:                 strings.TrimSpace(input.Summary),
		Details:                 strings.TrimSpace(input.Details),
		PayloadJSON:             strings.TrimSpace(input.PayloadJSON),
		AssignedTo:              strings.TrimSpace(input.AssignedTo),
		Urgency:                 normalizeOperatorQueueUrgency(input.Urgency),
		SourceKind:              strings.TrimSpace(input.SourceKind),
		SourceID:                strings.TrimSpace(input.SourceID),
		TaskID:                  strings.TrimSpace(input.TaskID),
		SessionID:               strings.TrimSpace(input.SessionID),
		AgentID:                 strings.TrimSpace(input.AgentID),
		KeepSessionActive:       input.KeepSessionActive,
		RequireMissing:          input.RequireMissing,
		RequireCurrentStatus:    strings.TrimSpace(input.RequireCurrentStatus),
		RequireCurrentRevision:  input.RequireCurrentRevision,
		RequireCurrentUpdatedAt: strings.TrimSpace(input.RequireCurrentUpdatedAt),
		promptContextEnvelope:   input.PromptContextEnvelope,
	}
	if dueAt != "" {
		record.DueAt = &dueAt
	}
	return record
}

func operatorQueueSelectColumns(alias string) string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}
	return prefix + `queue_id, ` + prefix + `workspace_id, ` + prefix + `queue_key, ` + prefix + `queue_type, ` + prefix + `status, ` + prefix + `title, ` + prefix + `summary, ` + prefix + `details,
	        ` + prefix + `payload_json, ` + prefix + `assigned_to, ` + prefix + `urgency, ` + prefix + `source_kind, ` + prefix + `source_id,
	        COALESCE(` + prefix + `task_id,''), COALESCE(` + prefix + `session_id,''), COALESCE(` + prefix + `agent_id,''),
	        ` + prefix + `keep_session_active, ` + prefix + `due_at, ` + prefix + `resolution, ` + prefix + `resolved_at, ` + prefix + `resolved_by,
	        ` + prefix + `escalation_count, ` + prefix + `last_escalated_at, ` + prefix + `last_escalated_by, ` + prefix + `escalation_reason, ` + prefix + `revision,
	        ` + prefix + `created_at, ` + prefix + `updated_at`
}

func operatorQueuePayload(record OperatorQueueRecord) map[string]any {
	payload := map[string]any{
		"queue_id":            record.QueueID,
		"queue_key":           record.QueueKey,
		"queue_type":          record.QueueType,
		"status":              record.Status,
		"title":               record.Title,
		"summary":             record.Summary,
		"payload_json":        record.PayloadJSON,
		"assigned_to":         record.AssignedTo,
		"urgency":             record.Urgency,
		"keep_session_active": record.KeepSessionActive,
		"source_kind":         record.SourceKind,
		"source_id":           record.SourceID,
		"due_at":              derefString(record.DueAt),
		"escalation_count":    record.EscalationCount,
		"last_escalated_at":   derefString(record.LastEscalatedAt),
		"last_escalated_by":   derefString(record.LastEscalatedBy),
		"escalation_reason":   strings.TrimSpace(record.EscalationReason),
		"revision":            record.Revision,
		"resolution":          strings.TrimSpace(record.Resolution),
		"resolved_by":         derefString(record.ResolvedBy),
	}
	if lineage, ok := operatorQueuePayloadLineage(record.PayloadJSON); ok {
		if lineage.RootCauseID != "" {
			payload["root_cause_id"] = lineage.RootCauseID
		}
		if lineage.ProvenanceGroupID != "" {
			payload["provenance_group_id"] = lineage.ProvenanceGroupID
		}
		if len(lineage.ParentRefsJSON) > 0 {
			payload["parent_refs_json"] = lineage.ParentRefsJSON
		}
	}
	return payload
}

type operatorQueueEventLineagePayload struct {
	RootCauseID       string   `json:"root_cause_id"`
	ProvenanceGroupID string   `json:"provenance_group_id"`
	ParentRefsJSON    []string `json:"parent_refs_json"`
}

func operatorQueuePayloadLineage(payloadJSON string) (operatorQueueEventLineagePayload, bool) {
	var payload operatorQueueEventLineagePayload
	if strings.TrimSpace(payloadJSON) == "" {
		return payload, false
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return operatorQueueEventLineagePayload{}, false
	}
	payload.RootCauseID = strings.TrimSpace(payload.RootCauseID)
	payload.ProvenanceGroupID = strings.TrimSpace(payload.ProvenanceGroupID)
	if len(payload.ParentRefsJSON) > 0 {
		normalized, err := normalizeRuntimeEventParentRefs(string(mustJSON(payload.ParentRefsJSON)))
		if err == nil {
			_ = json.Unmarshal([]byte(normalized), &payload.ParentRefsJSON)
		}
	}
	if payload.RootCauseID == "" && payload.ProvenanceGroupID == "" && len(payload.ParentRefsJSON) == 0 {
		return operatorQueueEventLineagePayload{}, false
	}
	return payload, true
}

func nextOperatorQueueUrgency(current, requested string) string {
	if trimmed := normalizeOperatorQueueUrgency(requested); strings.TrimSpace(requested) != "" {
		return trimmed
	}
	switch normalizeOperatorQueueUrgency(current) {
	case "LOW":
		return "NORMAL"
	case "NORMAL":
		return "HIGH"
	default:
		return "CRITICAL"
	}
}

func normalizeOperatorQueueType(queueType string) string {
	switch strings.ToUpper(strings.TrimSpace(queueType)) {
	case "DECISION":
		return "DECISION"
	case "HANDOFF":
		return "HANDOFF"
	case "FOLLOW_UP":
		return "FOLLOW_UP"
	default:
		return "BLOCKER"
	}
}

func (s *Store) lookupOperatorQueueIDByKey(ctx context.Context, workspaceID, queueKey string) (string, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT queue_id
		   FROM operator_queue_items
		  WHERE workspace_id = ? AND queue_key = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(queueKey),
	)
	var queueID string
	if err := row.Scan(&queueID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return queueID, nil
}

func normalizeOperatorQueueStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "RESOLVED":
		return "RESOLVED"
	case "CANCELLED":
		return "CANCELLED"
	default:
		return "OPEN"
	}
}

func operatorQueueStatusMismatchError(requiredStatus, queueID string) error {
	requiredStatus = normalizeOperatorQueueStatus(requiredStatus)
	if requiredStatus == "OPEN" {
		return fmt.Errorf("operator queue item is not open: %s", queueID)
	}
	return fmt.Errorf("operator queue item is not %s: %s", strings.ToLower(requiredStatus), queueID)
}

func operatorQueueRevisionMismatchError(queueRef string) error {
	queueRef = strings.TrimSpace(queueRef)
	return fmt.Errorf("operator queue item was updated concurrently: %s", queueRef)
}

func operatorQueueItemNotFoundError(queueRef string) error {
	queueRef = strings.TrimSpace(queueRef)
	if queueRef == "" {
		return ErrOperatorQueueItemNotFound
	}
	return fmt.Errorf("%w: %s", ErrOperatorQueueItemNotFound, queueRef)
}

func (s *Store) applyOperatorQueueTerminalReopenGuard(ctx context.Context, workspaceID, queueKey string, input *OperatorQueueUpsertInput) error {
	if input == nil || strings.TrimSpace(input.RequireCurrentStatus) != "" {
		return nil
	}
	status, found, err := s.lookupOperatorQueueStatusByKey(ctx, workspaceID, queueKey)
	if err != nil {
		return err
	}
	if found && normalizeOperatorQueueStatus(status) != "OPEN" {
		input.RequireCurrentStatus = normalizeOperatorQueueStatus(status)
	}
	return nil
}

func (s *Store) applyOperatorQueueTerminalReopenGuardTx(ctx context.Context, tx *sql.Tx, workspaceID, queueKey string, input *OperatorQueueUpsertInput) error {
	if input == nil || strings.TrimSpace(input.RequireCurrentStatus) != "" {
		return nil
	}
	status, found, err := s.lookupOperatorQueueStatusByKeyTx(ctx, tx, workspaceID, queueKey)
	if err != nil {
		return err
	}
	if found && normalizeOperatorQueueStatus(status) != "OPEN" {
		input.RequireCurrentStatus = normalizeOperatorQueueStatus(status)
	}
	return nil
}

func (s *Store) loadOperatorQueueItemByKeyTx(ctx context.Context, tx *sql.Tx, workspaceID, queueKey string) (OperatorQueueRecord, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	queueKey = strings.TrimSpace(queueKey)
	if workspaceID == "" || queueKey == "" {
		return OperatorQueueRecord{}, false, nil
	}
	record, err := s.getOperatorQueueItemTx(ctx, tx, workspaceID, "", queueKey)
	if err != nil {
		if errors.Is(err, ErrOperatorQueueItemNotFound) {
			return OperatorQueueRecord{}, false, nil
		}
		return OperatorQueueRecord{}, false, err
	}
	return record, true, nil
}

func (s *Store) lookupOperatorQueueStatusByKey(ctx context.Context, workspaceID, queueKey string) (string, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	queueKey = strings.TrimSpace(queueKey)
	if workspaceID == "" || queueKey == "" {
		return "", false, nil
	}
	var status string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT status FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		queueKey,
	).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("query operator queue status by key: %w", err)
	}
	return status, true, nil
}

func (s *Store) lookupOperatorQueueStatusByKeyTx(ctx context.Context, tx *sql.Tx, workspaceID, queueKey string) (string, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	queueKey = strings.TrimSpace(queueKey)
	if workspaceID == "" || queueKey == "" {
		return "", false, nil
	}
	var status string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT status FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		queueKey,
	).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("query operator queue status by key: %w", err)
	}
	return status, true, nil
}

func normalizeOperatorQueueUrgency(urgency string) string {
	switch strings.ToUpper(strings.TrimSpace(urgency)) {
	case "LOW":
		return "LOW"
	case "HIGH":
		return "HIGH"
	case "CRITICAL":
		return "CRITICAL"
	default:
		return "NORMAL"
	}
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func (s *Store) RecordKnowledgeClaim(ctx context.Context, input KnowledgeClaimInput) (KnowledgeClaimRecord, error) {
	record, _, _, err := s.RecordKnowledgeClaimWithAuthorityEffects(ctx, input)
	return record, err
}

func (s *Store) RecordKnowledgeClaimWithEvent(ctx context.Context, input KnowledgeClaimInput) (KnowledgeClaimRecord, RuntimeEventRecord, error) {
	record, event, _, err := s.RecordKnowledgeClaimWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) RecordKnowledgeClaimWithEffects(ctx context.Context, input KnowledgeClaimInput) (KnowledgeClaimRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.RecordKnowledgeClaimWithAuthorityEffects(ctx, input)
}

func (s *Store) RecordKnowledgeClaimWithAuthorityEffects(ctx context.Context, input KnowledgeClaimInput) (KnowledgeClaimRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	record, err := normalizeKnowledgeClaimInput(input)
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	if strings.TrimSpace(record.ClaimID) == "" {
		record.ClaimID = nextID("claim")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	claim := KnowledgeClaimRecord{}
	event := RuntimeEventRecord{}
	invalidationEvents := []RuntimeEventRecord(nil)
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		claim, event, invalidationEvents, innerErr = s.upsertKnowledgeClaimWithAuthorityTx(ctx, tx, authority, record, now, input.PromptContextEnvelope, "workspace.claim.write")
		return innerErr
	}); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	claim, err = s.GetKnowledgeClaim(ctx, claim.WorkspaceID, claim.ClaimID)
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	return claim, event, invalidationEvents, nil
}

func (s *Store) loadKnowledgeClaimRecordTx(ctx context.Context, tx *sql.Tx, workspaceID, claimID string) (KnowledgeClaimRecord, error) {
	query := `SELECT ` + knowledgeClaimSelectColumns("") + `
	   FROM knowledge_claims
	  WHERE workspace_id = ? AND claim_id = ?`
	var row interface{ Scan(dest ...any) error }
	if tx != nil {
		row = tx.QueryRowContext(ctx, query, workspaceID, claimID)
	} else {
		row = s.db.QueryRowContext(ctx, query, workspaceID, claimID)
	}
	return scanKnowledgeClaimRecord(row)
}

func (s *Store) GetKnowledgeClaim(ctx context.Context, workspaceID, claimID string) (KnowledgeClaimRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	claimID = strings.TrimSpace(claimID)
	if workspaceID == "" {
		return KnowledgeClaimRecord{}, errors.New("workspace_id is required")
	}
	if claimID == "" {
		return KnowledgeClaimRecord{}, errors.New("claim_id is required")
	}
	record, err := s.loadKnowledgeClaimRecordTx(ctx, nil, workspaceID, claimID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return KnowledgeClaimRecord{}, fmt.Errorf("knowledge claim not found: %s/%s", workspaceID, claimID)
		}
		return KnowledgeClaimRecord{}, err
	}
	return record, nil
}

func (s *Store) ListKnowledgeClaims(ctx context.Context, filter KnowledgeClaimFilter) ([]KnowledgeClaimRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	query := strings.Builder{}
	query.WriteString(`SELECT ` + knowledgeClaimSelectColumns("") + ` FROM knowledge_claims`)
	args := make([]any, 0, 8)
	where := buildKnowledgeClaimWhere(filter, "", &args)
	if len(where) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(where, " AND "))
	}
	query.WriteString(` ORDER BY ` + knowledgeClaimStatusPrioritySQL("") + ` DESC, ` + knowledgeClaimTypePrioritySQL("") + ` DESC, updated_at DESC, confidence DESC, claim_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list knowledge claims: %w", err)
	}
	defer rows.Close()
	return collectKnowledgeClaimRows(rows)
}

func (s *Store) SearchKnowledgeClaims(ctx context.Context, filter KnowledgeClaimFilter) ([]KnowledgeClaimRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	if strings.TrimSpace(filter.Query) == "" {
		return s.ListKnowledgeClaims(ctx, filter)
	}
	args := make([]any, 0, 8)
	where := buildKnowledgeClaimWhere(filter, "c", &args)
	queryText := strings.TrimSpace(filter.Query)
	ftsQuery := buildWorkspaceMemoryFTSQuery(queryText)
	useFTS := ftsQuery != ""

	sqlText := strings.Builder{}
	sqlText.WriteString(`SELECT ` + knowledgeClaimSelectColumns("c") + ` FROM knowledge_claims c`)
	if useFTS {
		sqlText.WriteString(` JOIN knowledge_claims_fts ON knowledge_claims_fts.rowid = c.rowid`)
	}
	clauses := make([]string, 0, len(where)+1)
	clauses = append(clauses, where...)
	if useFTS {
		clauses = append(clauses, `knowledge_claims_fts MATCH ?`)
		args = append(args, ftsQuery)
	} else {
		needle := "%" + queryText + "%"
		clauses = append(clauses, `(c.claim_type LIKE ? OR c.subject LIKE ? OR c.body LIKE ? OR c.summary LIKE ? OR c.tags_json LIKE ?)`)
		args = append(args, needle, needle, needle, needle, needle)
	}
	sqlText.WriteString(" WHERE ")
	sqlText.WriteString(strings.Join(clauses, " AND "))
	if useFTS {
		sqlText.WriteString(` ORDER BY ` + knowledgeClaimStatusPrioritySQL("c") + ` DESC, ` + knowledgeClaimTypePrioritySQL("c") + ` DESC, bm25(knowledge_claims_fts), c.confidence DESC, c.updated_at DESC, c.claim_id DESC`)
	} else {
		sqlText.WriteString(` ORDER BY ` + knowledgeClaimStatusPrioritySQL("c") + ` DESC, ` + knowledgeClaimTypePrioritySQL("c") + ` DESC, c.confidence DESC, c.updated_at DESC, c.claim_id DESC`)
	}
	sqlText.WriteString(` LIMIT ?`)
	args = append(args, filter.Limit)

	if useFTS {
		records, err := s.searchKnowledgeClaimsWithFTSRepair(ctx, sqlText.String(), args...)
		if err != nil {
			return nil, fmt.Errorf("search knowledge claims: %w", err)
		}
		return records, nil
	}
	rows, err := s.db.QueryContext(ctx, sqlText.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("search knowledge claims: %w", err)
	}
	defer rows.Close()
	return collectKnowledgeClaimRows(rows)
}

func (s *Store) upsertKnowledgeClaimTx(ctx context.Context, tx *sql.Tx, record KnowledgeClaimRecord, now string) (KnowledgeClaimRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, strings.TrimSpace(record.WorkspaceID), authorityScopeWorkspace, now)
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	var (
		claim              KnowledgeClaimRecord
		event              RuntimeEventRecord
		invalidationEvents []RuntimeEventRecord
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		claim, event, invalidationEvents, innerErr = s.upsertKnowledgeClaimWithAuthorityTx(ctx, tx, authority, record, now, nil, "")
		return innerErr
	}); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	return claim, event, invalidationEvents, nil
}

func (s *Store) upsertKnowledgeClaimWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record KnowledgeClaimRecord, now string, promptContextEnvelope map[string]any, promptContextSurface string) (KnowledgeClaimRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	if !workspaceAuthorityEnvelopeProvided(authority) {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace authority is required")
	}
	evidenceJSON, err := json.Marshal(record.Evidence)
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("encode claim evidence: %w", err)
	}
	tagsJSON, err := json.Marshal(record.Tags)
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("encode claim tags: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	if record.AgentID != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, record.WorkspaceID, record.AgentID); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if record.TaskID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, record.WorkspaceID, record.TaskID); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if record.SessionID != "" {
		if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, record.WorkspaceID, record.SessionID); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if record.MemoryID != "" {
		if err := s.ensureWorkspaceMemoryInWorkspaceTx(ctx, tx, record.WorkspaceID, record.MemoryID); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if record.ClaimID == "" {
		record.ClaimID = nextID("claim")
	}
	for _, targetClaimID := range knowledgeClaimRelationTargets(record) {
		if err := s.ensureKnowledgeClaimInWorkspaceTx(ctx, tx, record.WorkspaceID, targetClaimID); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if record.SupersededByClaimID != "" {
		if err := s.ensureKnowledgeClaimInWorkspaceTx(ctx, tx, record.WorkspaceID, record.SupersededByClaimID); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
		}
	}

	createdAt := now
	var existingCreatedAt string
	err = tx.QueryRowContext(
		ctx,
		`SELECT created_at FROM knowledge_claims WHERE workspace_id = ? AND claim_id = ?`,
		record.WorkspaceID,
		record.ClaimID,
	).Scan(&existingCreatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("query knowledge claim: %w", err)
	default:
		createdAt = existingCreatedAt
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = now
	invalidationEvents := make([]RuntimeEventRecord, 0, 2)
	if _, err := execKnowledgeClaimWriteWithFTSRepairTx(
		ctx,
		tx,
		`INSERT INTO knowledge_claims(
		    claim_id, workspace_id, claim_type, status, subject, body, summary, confidence,
		    source_kind, source_id, memory_id, task_id, session_id, agent_id,
		    supersedes_claim_id, superseded_by_claim_id, conflicts_claim_id, evidence_json, tags_json,
		    lifecycle_reason, review_due_at, reviewed_at, reviewed_by,
		    archived_at, archived_by, created_at, updated_at, recovery_reason
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, '')
		  ON CONFLICT(claim_id) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    claim_type = excluded.claim_type,
		    status = excluded.status,
		    subject = excluded.subject,
		    body = excluded.body,
		    summary = excluded.summary,
		    confidence = excluded.confidence,
		    source_kind = excluded.source_kind,
		    source_id = excluded.source_id,
		    memory_id = excluded.memory_id,
		    task_id = excluded.task_id,
		    session_id = excluded.session_id,
		    agent_id = excluded.agent_id,
		    supersedes_claim_id = excluded.supersedes_claim_id,
		    superseded_by_claim_id = excluded.superseded_by_claim_id,
		    conflicts_claim_id = excluded.conflicts_claim_id,
		    evidence_json = excluded.evidence_json,
		    tags_json = excluded.tags_json,
		    lifecycle_reason = excluded.lifecycle_reason,
		    review_due_at = excluded.review_due_at,
		    reviewed_at = excluded.reviewed_at,
		    reviewed_by = excluded.reviewed_by,
		    archived_at = NULL,
		    archived_by = NULL,
		    updated_at = excluded.updated_at,
		    recovery_reason = ''`,
		record.ClaimID,
		record.WorkspaceID,
		record.ClaimType,
		record.Status,
		record.Subject,
		record.Body,
		record.Summary,
		record.Confidence,
		record.SourceKind,
		record.SourceID,
		blankStringOrNil(record.MemoryID),
		blankStringOrNil(record.TaskID),
		blankStringOrNil(record.SessionID),
		blankStringOrNil(record.AgentID),
		blankStringOrNil(record.SupersedesClaimID),
		blankStringOrNil(record.SupersededByClaimID),
		blankStringOrNil(record.ConflictsClaimID),
		string(evidenceJSON),
		string(tagsJSON),
		record.LifecycleReason,
		blankStringOrNil(derefString(record.ReviewDueAt)),
		blankStringOrNil(derefString(record.ReviewedAt)),
		blankStringOrNil(derefString(record.ReviewedBy)),
		record.CreatedAt,
		record.UpdatedAt,
	); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("upsert knowledge claim: %w", err)
	}

	if trimmedSupersedes := strings.TrimSpace(record.SupersedesClaimID); trimmedSupersedes != "" {
		reason := "superseded_by_" + record.ClaimID
		if _, err := execKnowledgeClaimWriteWithFTSRepairTx(
			ctx,
			tx,
			`UPDATE knowledge_claims
			    SET status = 'SUPERSEDED',
			        superseded_by_claim_id = ?,
			        lifecycle_reason = ?,
			        review_due_at = NULL,
			        updated_at = ?
			  WHERE workspace_id = ? AND claim_id = ? AND status != 'ARCHIVED'`,
			record.ClaimID,
			reason,
			now,
			record.WorkspaceID,
			trimmedSupersedes,
		); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("demote superseded claim: %w", err)
		}
		if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, record.WorkspaceID, memoryInvalidationRefChange{
			RefKind:             "knowledge_claim",
			RefID:               trimmedSupersedes,
			CurrentVersionToken: now,
			Cause:               "knowledge_claim.superseded",
		}); err != nil {
			return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
		} else {
			invalidationEvents = append(invalidationEvents, events...)
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, record.WorkspaceID); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("touch workspace after claim update: %w", err)
	}
	actorID := firstNonEmpty(record.AgentID, record.SourceID, record.WorkspaceID)
	payload := map[string]any{
		"workspace_id":           record.WorkspaceID,
		"claim_id":               record.ClaimID,
		"claim_type":             record.ClaimType,
		"status":                 record.Status,
		"subject":                record.Subject,
		"summary":                record.Summary,
		"source_kind":            record.SourceKind,
		"source_id":              record.SourceID,
		"memory_id":              record.MemoryID,
		"task_id":                record.TaskID,
		"session_id":             record.SessionID,
		"agent_id":               record.AgentID,
		"supersedes_claim_id":    record.SupersedesClaimID,
		"superseded_by_claim_id": record.SupersededByClaimID,
		"conflicts_claim_id":     record.ConflictsClaimID,
		"confidence":             record.Confidence,
		"lifecycle_reason":       record.LifecycleReason,
		"review_due_at":          derefString(record.ReviewDueAt),
		"reviewed_at":            derefString(record.ReviewedAt),
		"reviewed_by":            derefString(record.ReviewedBy),
		"actor_type":             "agent",
		"actor_id":               actorID,
	}
	payload, err = attachKnowledgeClaimPromptContextEnvelope(payload, promptContextEnvelope, promptContextSurface, knowledgeClaimPromptContextFields(record, "agent", actorID))
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    record.ClaimID,
		ActorType:   "agent",
		ActorID:     actorID,
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	if err := s.syncKnowledgeClaimRelationsTx(ctx, tx, record, now); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	if err := s.enqueueMemoryProjectionOutboxTx(ctx, tx, record.WorkspaceID, memoryProjectionKindKnowledgeClaim, record.ClaimID, now); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, record.WorkspaceID, memoryInvalidationRefChange{
		RefKind:             "knowledge_claim",
		RefID:               record.ClaimID,
		CurrentVersionToken: record.UpdatedAt,
		Cause:               "knowledge_claim.written",
	}); err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	} else {
		invalidationEvents = append(invalidationEvents, events...)
	}
	return record, event, invalidationEvents, nil
}

func knowledgeClaimPromptContextFields(record KnowledgeClaimRecord, actorType, actorID string) map[string]string {
	fields := map[string]string{
		"workspace_id": strings.TrimSpace(record.WorkspaceID),
		"claim_id":     strings.TrimSpace(record.ClaimID),
		"claim_type":   strings.TrimSpace(record.ClaimType),
		"status":       strings.TrimSpace(record.Status),
		"actor_type":   strings.TrimSpace(actorType),
		"actor_id":     strings.TrimSpace(actorID),
	}
	for key, value := range map[string]string{
		"source_kind":            record.SourceKind,
		"source_id":              record.SourceID,
		"memory_id":              record.MemoryID,
		"task_id":                record.TaskID,
		"session_id":             record.SessionID,
		"agent_id":               record.AgentID,
		"supersedes_claim_id":    record.SupersedesClaimID,
		"superseded_by_claim_id": record.SupersededByClaimID,
		"conflicts_claim_id":     record.ConflictsClaimID,
		"lifecycle_reason":       record.LifecycleReason,
		"review_due_at":          derefString(record.ReviewDueAt),
		"reviewed_at":            derefString(record.ReviewedAt),
		"reviewed_by":            derefString(record.ReviewedBy),
		"archived_at":            derefString(record.ArchivedAt),
		"archived_by":            derefString(record.ArchivedBy),
	} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			fields[key] = trimmed
		}
	}
	return fields
}

func (s *Store) archiveKnowledgeClaimTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, claimID, archivedBy, reason, now string, promptContextEnvelope map[string]any, promptContextSurface string) (bool, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	if !workspaceAuthorityEnvelopeProvided(authority) {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace authority is required")
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record, err := s.loadKnowledgeClaimRecordTx(ctx, tx, workspaceID, claimID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, nil
		}
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	result, err := execKnowledgeClaimWriteWithFTSRepairTx(
		ctx,
		tx,
		`UPDATE knowledge_claims
		    SET status = 'ARCHIVED', lifecycle_reason = ?, review_due_at = NULL, archived_at = ?, archived_by = ?, updated_at = ?
		  WHERE workspace_id = ? AND claim_id = ?`,
		strings.TrimSpace(reason),
		now,
		archivedBy,
		now,
		workspaceID,
		claimID,
	)
	if err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("archive knowledge claim: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("touch workspace after claim archive: %w", err)
	}
	record.Status = "ARCHIVED"
	record.LifecycleReason = strings.TrimSpace(reason)
	record.ReviewDueAt = nil
	record.ArchivedAt = &now
	record.ArchivedBy = &archivedBy
	record.UpdatedAt = now
	queueRecord, queueEvent, err := s.syncKnowledgeClaimReviewQueueWithAuthorityTx(ctx, tx, authority, record, archivedBy, "", "", "claim_archived", now)
	if err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	if err := s.enqueueMemoryProjectionOutboxTx(ctx, tx, record.WorkspaceID, memoryProjectionKindKnowledgeClaim, record.ClaimID, now); err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	payload := map[string]any{
		"workspace_id": workspaceID,
		"claim_id":     claimID,
		"archived_by":  archivedBy,
		"reason":       reason,
		"status":       "ARCHIVED",
		"actor_type":   "operator",
		"actor_id":     archivedBy,
	}
	payload, err = attachKnowledgeClaimPromptContextEnvelope(payload, promptContextEnvelope, promptContextSurface, knowledgeClaimPromptContextFields(record, "operator", archivedBy))
	if err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		ActorType:   "operator",
		ActorID:     archivedBy,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	invalidationEvents := make([]RuntimeEventRecord, 0, 1)
	if _, events, err := s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, authority, workspaceID, memoryInvalidationRefChange{
		RefKind:             "knowledge_claim",
		RefID:               claimID,
		CurrentVersionToken: record.UpdatedAt,
		Cause:               "knowledge_claim.archived",
	}); err != nil {
		return false, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	} else {
		invalidationEvents = append(invalidationEvents, events...)
	}
	return true, queueRecord, event, queueEvent, invalidationEvents, nil
}

func (s *Store) syncPromotedKnowledgeClaimForMemoryTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record WorkspaceMemoryRecord, actorID, reason, now string) (*PromotedKnowledgeClaimSyncEffects, error) {
	claim, ok := promotedKnowledgeClaimFromWorkspaceMemory(record)
	if !ok || (record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "") {
		claimID := promotedKnowledgeClaimID(record.MemoryID)
		if claimID == "" {
			return nil, nil
		}
		found, queueRecord, event, queueEvent, invalidationEvents, err := s.archiveKnowledgeClaimTx(ctx, tx, authority, strings.TrimSpace(record.WorkspaceID), claimID, firstNonEmpty(strings.TrimSpace(actorID), "system"), strings.TrimSpace(reason), now, nil, "")
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
		claimRecord, err := s.loadKnowledgeClaimRecordTx(ctx, tx, strings.TrimSpace(record.WorkspaceID), claimID)
		if err != nil {
			return nil, err
		}
		effects := &PromotedKnowledgeClaimSyncEffects{
			Claim:              &claimRecord,
			ClaimEvent:         &event,
			InvalidationEvents: append([]RuntimeEventRecord(nil), invalidationEvents...),
		}
		if queueEvent.EventID != "" {
			queueRecordCopy := queueRecord
			queueEventCopy := queueEvent
			effects.Queue = &queueRecordCopy
			effects.QueueEvent = &queueEventCopy
		}
		return effects, nil
	}
	if existing, err := s.loadKnowledgeClaimRecordTx(ctx, tx, claim.WorkspaceID, claim.ClaimID); err == nil {
		if normalized := normalizeKnowledgeClaimStatus(existing.Status); normalized != "ACTIVE" && normalized != "ARCHIVED" {
			claim.Status = existing.Status
			claim.LifecycleReason = existing.LifecycleReason
			claim.ReviewDueAt = existing.ReviewDueAt
			claim.ReviewedAt = existing.ReviewedAt
			claim.ReviewedBy = existing.ReviewedBy
			claim.ConflictsClaimID = existing.ConflictsClaimID
			claim.SupersedesClaimID = firstNonEmpty(strings.TrimSpace(claim.SupersedesClaimID), strings.TrimSpace(existing.SupersedesClaimID))
			claim.SupersededByClaimID = existing.SupersededByClaimID
		}
	}
	claimRecord, event, invalidationEvents, err := s.upsertKnowledgeClaimWithAuthorityTx(ctx, tx, authority, claim, now, nil, "")
	if err != nil {
		return nil, err
	}
	eventCopy := event
	claimCopy := claimRecord
	return &PromotedKnowledgeClaimSyncEffects{
		Claim:              &claimCopy,
		ClaimEvent:         &eventCopy,
		InvalidationEvents: append([]RuntimeEventRecord(nil), invalidationEvents...),
	}, nil
}

func knowledgeClaimReviewQueueKey(claimID string) string {
	claimID = strings.TrimSpace(claimID)
	if claimID == "" {
		return ""
	}
	return "knowledge_claim:" + claimID + ":review"
}

func knowledgeClaimReviewQueueInput(record KnowledgeClaimRecord, assignedTo, dueAt, urgencyOverride string) OperatorQueueUpsertInput {
	status := normalizeKnowledgeClaimStatus(record.Status)
	title := "Review claim"
	urgency := "NORMAL"
	switch status {
	case "DISPUTED":
		title = "Resolve disputed claim"
		urgency = "HIGH"
	case "STALE":
		title = "Refresh stale claim"
	case "REVIEW":
		title = "Review claim"
	}
	title = title + " " + firstNonEmpty(strings.TrimSpace(record.Subject), strings.TrimSpace(record.ClaimID))
	lines := []string{
		fmt.Sprintf("Claim ID: %s", strings.TrimSpace(record.ClaimID)),
		fmt.Sprintf("Claim type: %s", strings.TrimSpace(record.ClaimType)),
		fmt.Sprintf("Status: %s", status),
	}
	if summary := strings.TrimSpace(record.Summary); summary != "" {
		lines = append(lines, fmt.Sprintf("Summary: %s", summary))
	}
	if reason := strings.TrimSpace(record.LifecycleReason); reason != "" {
		lines = append(lines, fmt.Sprintf("Reason: %s", reason))
	}
	if memoryID := strings.TrimSpace(record.MemoryID); memoryID != "" {
		lines = append(lines, fmt.Sprintf("Memory ID: %s", memoryID))
	}
	if sessionID := strings.TrimSpace(record.SessionID); sessionID != "" {
		lines = append(lines, fmt.Sprintf("Session ID: %s", sessionID))
	}
	if taskID := strings.TrimSpace(record.TaskID); taskID != "" {
		lines = append(lines, fmt.Sprintf("Task ID: %s", taskID))
	}
	if conflictID := strings.TrimSpace(record.ConflictsClaimID); conflictID != "" {
		lines = append(lines, fmt.Sprintf("Conflicts with claim: %s", conflictID))
	}
	return OperatorQueueUpsertInput{
		WorkspaceID: record.WorkspaceID,
		QueueKey:    knowledgeClaimReviewQueueKey(record.ClaimID),
		QueueType:   "FOLLOW_UP",
		Title:       title,
		Summary:     firstNonEmpty(strings.TrimSpace(record.Summary), strings.TrimSpace(record.Subject)),
		Details:     strings.Join(lines, "\n"),
		AssignedTo:  strings.TrimSpace(assignedTo),
		Urgency:     firstNonEmpty(strings.TrimSpace(urgencyOverride), urgency),
		SourceKind:  "knowledge_claim",
		SourceID:    strings.TrimSpace(record.ClaimID),
		TaskID:      strings.TrimSpace(record.TaskID),
		SessionID:   strings.TrimSpace(record.SessionID),
		AgentID:     strings.TrimSpace(record.AgentID),
		DueAt:       strings.TrimSpace(dueAt),
	}
}

func (s *Store) syncKnowledgeClaimReviewQueueTx(ctx context.Context, tx *sql.Tx, record KnowledgeClaimRecord, actorID, assignedTo, urgency, resolution, now string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	return s.syncKnowledgeClaimReviewQueueWithAuthorityTx(ctx, tx, WorkspaceAuthorityRecord{}, record, actorID, assignedTo, urgency, resolution, now)
}

func (s *Store) syncKnowledgeClaimReviewQueueWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record KnowledgeClaimRecord, actorID, assignedTo, urgency, resolution, now string) (OperatorQueueRecord, RuntimeEventRecord, error) {
	queueKey := knowledgeClaimReviewQueueKey(record.ClaimID)
	if queueKey == "" {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil
	}
	currentQueue, found, err := s.loadOperatorQueueItemByKeyTx(ctx, tx, strings.TrimSpace(record.WorkspaceID), queueKey)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	if isKnowledgeClaimReviewStatus(record.Status) {
		input := knowledgeClaimReviewQueueInput(record, assignedTo, derefString(record.ReviewDueAt), urgency)
		if err := s.applyOperatorQueueTerminalReopenGuardTx(ctx, tx, strings.TrimSpace(record.WorkspaceID), queueKey, &input); err != nil {
			return OperatorQueueRecord{}, RuntimeEventRecord{}, err
		}
		if found {
			input.RequireCurrentRevision = currentQueue.Revision
			input.RequireCurrentUpdatedAt = currentQueue.UpdatedAt
		}
		queue, event, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(input), now)
		return queue, event, err
	}
	queueID := ""
	requiredRevision := int64(0)
	requiredUpdatedAt := ""
	if found {
		queueID = currentQueue.QueueID
		requiredRevision = currentQueue.Revision
		requiredUpdatedAt = currentQueue.UpdatedAt
	}
	queue, event, err := s.resolveOperatorQueueItemWithAuthorityTx(ctx, tx, authority, strings.TrimSpace(record.WorkspaceID), queueID, queueKey, "RESOLVED", firstNonEmpty(strings.TrimSpace(actorID), strings.TrimSpace(record.AgentID), strings.TrimSpace(record.WorkspaceID)), resolution, "", "", "", "", requiredRevision, requiredUpdatedAt, nil, now)
	if errors.Is(err, ErrOperatorQueueItemNotFound) {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, nil
	}
	return queue, event, err
}

func (s *Store) applyKnowledgeClaimLifecycleTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record KnowledgeClaimRecord, actorID, eventType string, payload map[string]any, now string, promptContextEnvelope map[string]any, promptContextSurface string) (KnowledgeClaimRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	record.UpdatedAt = now
	_, _, invalidationEvents, err := s.upsertKnowledgeClaimWithAuthorityTx(ctx, tx, authority, record, now, promptContextEnvelope, promptContextSurface)
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["workspace_id"] = record.WorkspaceID
	payload["claim_id"] = record.ClaimID
	payload["status"] = record.Status
	payload["actor_type"] = "operator"
	payload["actor_id"] = actorID
	payload, err = attachKnowledgeClaimPromptContextEnvelope(payload, promptContextEnvelope, promptContextSurface, knowledgeClaimPromptContextFields(record, "operator", actorID))
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   eventType,
		EntityType:  "knowledge_claim",
		EntityID:    record.ClaimID,
		ActorType:   "operator",
		ActorID:     actorID,
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return KnowledgeClaimRecord{}, RuntimeEventRecord{}, nil, err
	}
	return record, event, invalidationEvents, nil
}

func (s *Store) RequestKnowledgeClaimReview(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, error) {
	return s.transitionKnowledgeClaimLifecycle(ctx, input, "REVIEW", "knowledge_claim.review_requested")
}

func (s *Store) RequestKnowledgeClaimReviewWithEvent(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, RuntimeEventRecord, error) {
	record, _, event, _, _, err := s.RequestKnowledgeClaimReviewWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) RequestKnowledgeClaimReviewWithEffects(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.transitionKnowledgeClaimLifecycleWithEvent(ctx, input, "REVIEW", "knowledge_claim.review_requested")
}

func (s *Store) ConfirmKnowledgeClaim(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, error) {
	return s.transitionKnowledgeClaimLifecycle(ctx, input, "CONFIRMED", "knowledge_claim.confirmed")
}

func (s *Store) ConfirmKnowledgeClaimWithEvent(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, RuntimeEventRecord, error) {
	record, _, event, _, _, err := s.ConfirmKnowledgeClaimWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) ConfirmKnowledgeClaimWithEffects(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.transitionKnowledgeClaimLifecycleWithEvent(ctx, input, "CONFIRMED", "knowledge_claim.confirmed")
}

func (s *Store) DisputeKnowledgeClaim(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, error) {
	return s.transitionKnowledgeClaimLifecycle(ctx, input, "DISPUTED", "knowledge_claim.disputed")
}

func (s *Store) DisputeKnowledgeClaimWithEvent(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, RuntimeEventRecord, error) {
	record, _, event, _, _, err := s.DisputeKnowledgeClaimWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) DisputeKnowledgeClaimWithEffects(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.transitionKnowledgeClaimLifecycleWithEvent(ctx, input, "DISPUTED", "knowledge_claim.disputed")
}

func (s *Store) SupersedeKnowledgeClaim(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, error) {
	return s.transitionKnowledgeClaimLifecycle(ctx, input, "SUPERSEDED", "knowledge_claim.superseded")
}

func (s *Store) SupersedeKnowledgeClaimWithEvent(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, RuntimeEventRecord, error) {
	record, _, event, _, _, err := s.SupersedeKnowledgeClaimWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) SupersedeKnowledgeClaimWithEffects(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.transitionKnowledgeClaimLifecycleWithEvent(ctx, input, "SUPERSEDED", "knowledge_claim.superseded")
}

func (s *Store) MarkKnowledgeClaimStale(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, error) {
	return s.transitionKnowledgeClaimLifecycle(ctx, input, "STALE", "knowledge_claim.stale")
}

func (s *Store) MarkKnowledgeClaimStaleWithEvent(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, RuntimeEventRecord, error) {
	record, _, event, _, _, err := s.MarkKnowledgeClaimStaleWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) MarkKnowledgeClaimStaleWithEffects(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimRecord, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.transitionKnowledgeClaimLifecycleWithEvent(ctx, input, "STALE", "knowledge_claim.stale")
}

func knowledgeClaimPromptSurfaceForStatus(status string) string {
	switch normalizeKnowledgeClaimStatus(status) {
	case "REVIEW":
		return "workspace.claim.review"
	case "CONFIRMED":
		return "workspace.claim.confirm"
	case "DISPUTED":
		return "workspace.claim.dispute"
	case "SUPERSEDED":
		return "workspace.claim.supersede"
	case "STALE":
		return "workspace.claim.stale"
	default:
		return "workspace.claim.lifecycle"
	}
}

func (s *Store) EscalateKnowledgeClaimReview(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimReviewEscalationRecord, error) {
	record, _, _, _, err := s.EscalateKnowledgeClaimReviewWithEffects(ctx, input)
	return record, err
}

func (s *Store) escalateKnowledgeClaimReviewWithEvents(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimReviewEscalationRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	claimID := strings.TrimSpace(input.ClaimID)
	if claimID == "" {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("claim_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("actor_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record := KnowledgeClaimReviewEscalationRecord{}
	claimEvent := RuntimeEventRecord{}
	queueEvent := RuntimeEventRecord{}
	invalidationEvents := []RuntimeEventRecord(nil)
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, claimEvent, queueEvent, invalidationEvents, innerErr = s.escalateKnowledgeClaimReviewWithEventsTx(ctx, tx, authority, input, now)
		return innerErr
	}); err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record.Claim, err = s.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record.Queue, err = s.getOperatorQueueItem(ctx, workspaceID, record.Queue.QueueID, "")
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	return record, claimEvent, queueEvent, invalidationEvents, nil
}

func (s *Store) EscalateKnowledgeClaimReviewWithEvents(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimReviewEscalationRecord, RuntimeEventRecord, RuntimeEventRecord, error) {
	record, claimEvent, queueEvent, _, err := s.EscalateKnowledgeClaimReviewWithEffects(ctx, input)
	return record, claimEvent, queueEvent, err
}

func (s *Store) EscalateKnowledgeClaimReviewWithEffects(ctx context.Context, input KnowledgeClaimLifecycleInput) (KnowledgeClaimReviewEscalationRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	return s.escalateKnowledgeClaimReviewWithEvents(ctx, input)
}

func (s *Store) escalateKnowledgeClaimReviewWithEventsTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input KnowledgeClaimLifecycleInput, now string) (KnowledgeClaimReviewEscalationRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	claimID := strings.TrimSpace(input.ClaimID)
	actorID := strings.TrimSpace(input.ActorID)
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record, err := s.loadKnowledgeClaimRecordTx(ctx, tx, workspaceID, claimID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("knowledge claim not found: %s/%s", workspaceID, claimID)
		}
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("knowledge claim is archived: %s/%s", workspaceID, claimID)
	}
	if !isKnowledgeClaimReviewStatus(record.Status) {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("knowledge claim is not in review workflow: %s", record.Status)
	}
	scarcity, err := s.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	if strings.EqualFold(strings.TrimSpace(scarcity.Status), "SATURATED") {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("reviewer scarcity is saturated; defer new reviewer admission work")
	}
	if trimmedReason := strings.TrimSpace(input.Reason); trimmedReason != "" {
		record.LifecycleReason = trimmedReason
	}
	if trimmedDueAt := strings.TrimSpace(input.ReviewDueAt); trimmedDueAt != "" {
		record.ReviewDueAt = stringPtr(trimmedDueAt)
	}
	record.UpdatedAt = now
	_, _, invalidationEvents, err := s.upsertKnowledgeClaimWithAuthorityTx(ctx, tx, authority, record, now, input.PromptContextEnvelope, "workspace.claim.escalate")
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	if _, _, err := s.syncKnowledgeClaimReviewQueueWithAuthorityTx(ctx, tx, authority, record, actorID, strings.TrimSpace(input.AssignedTo), strings.TrimSpace(input.Urgency), "", now); err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	queue, queueEvent, _, err := s.escalateOperatorQueueItemWithAuthorityTx(
		ctx,
		tx,
		authority,
		workspaceID,
		"",
		knowledgeClaimReviewQueueKey(claimID),
		actorID,
		strings.TrimSpace(input.Reason),
		strings.TrimSpace(input.AssignedTo),
		strings.TrimSpace(input.Urgency),
		derefString(record.ReviewDueAt),
		0,
		"",
		nil,
		now,
	)
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	payload := map[string]any{
		"workspace_id":      workspaceID,
		"claim_id":          record.ClaimID,
		"status":            record.Status,
		"reason":            record.LifecycleReason,
		"review_due_at":     derefString(record.ReviewDueAt),
		"assigned_to":       queue.AssignedTo,
		"urgency":           queue.Urgency,
		"queue_id":          queue.QueueID,
		"escalation_count":  queue.EscalationCount,
		"last_escalated_at": derefString(queue.LastEscalatedAt),
		"last_escalated_by": derefString(queue.LastEscalatedBy),
		"escalation_reason": strings.TrimSpace(queue.EscalationReason),
		"actor_type":        "operator",
		"actor_id":          actorID,
	}
	payload, err = attachKnowledgeClaimPromptContextEnvelope(payload, input.PromptContextEnvelope, "workspace.claim.escalate", knowledgeClaimPromptContextFields(record, "operator", actorID))
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	claimEvent, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    record.ClaimID,
		ActorType:   "operator",
		ActorID:     actorID,
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return KnowledgeClaimReviewEscalationRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	return KnowledgeClaimReviewEscalationRecord{Claim: record, Queue: queue}, claimEvent, queueEvent, invalidationEvents, nil
}

func (s *Store) transitionKnowledgeClaimLifecycle(ctx context.Context, input KnowledgeClaimLifecycleInput, targetStatus, eventType string) (KnowledgeClaimRecord, error) {
	record, _, _, _, _, err := s.transitionKnowledgeClaimLifecycleWithEvent(ctx, input, targetStatus, eventType)
	return record, err
}

func (s *Store) transitionKnowledgeClaimLifecycleWithEvent(ctx context.Context, input KnowledgeClaimLifecycleInput, targetStatus, eventType string) (KnowledgeClaimRecord, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	claimID := strings.TrimSpace(input.ClaimID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	if claimID == "" {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("claim_id is required")
	}
	if actorID == "" {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("actor_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}

	record := KnowledgeClaimRecord{}
	queueRecord := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	queueEvent := RuntimeEventRecord{}
	invalidationEvents := []RuntimeEventRecord(nil)
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		current, err := s.loadKnowledgeClaimRecordTx(ctx, tx, workspaceID, claimID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("knowledge claim not found: %s/%s", workspaceID, claimID)
			}
			return err
		}
		if current.ArchivedAt != nil && strings.TrimSpace(*current.ArchivedAt) != "" {
			return fmt.Errorf("knowledge claim is archived: %s/%s", workspaceID, claimID)
		}
		normalizedStatus := normalizeKnowledgeClaimStatus(targetStatus)
		if normalizedStatus == "REVIEW" || normalizedStatus == "DISPUTED" {
			scarcity, err := s.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
			if err != nil {
				return err
			}
			if strings.EqualFold(strings.TrimSpace(scarcity.Status), "SATURATED") {
				return errors.New("reviewer scarcity is saturated; defer new reviewer admission work")
			}
		}
		current.Status = normalizedStatus
		current.LifecycleReason = strings.TrimSpace(input.Reason)
		switch current.Status {
		case "REVIEW":
			current.ReviewDueAt = stringPtr(strings.TrimSpace(input.ReviewDueAt))
		case "DISPUTED":
			current.ReviewDueAt = stringPtr(strings.TrimSpace(input.ReviewDueAt))
			if conflictID := strings.TrimSpace(input.ConflictsClaimID); conflictID != "" {
				if err := s.ensureKnowledgeClaimInWorkspaceTx(ctx, tx, workspaceID, conflictID); err != nil {
					return err
				}
				current.ConflictsClaimID = conflictID
			}
			current.ReviewedAt = stringPtr(now)
			current.ReviewedBy = stringPtr(actorID)
		case "STALE":
			current.ReviewDueAt = stringPtr(strings.TrimSpace(input.ReviewDueAt))
			current.ReviewedAt = stringPtr(now)
			current.ReviewedBy = stringPtr(actorID)
		case "CONFIRMED":
			current.ReviewDueAt = nil
			current.ReviewedAt = stringPtr(now)
			current.ReviewedBy = stringPtr(actorID)
		case "SUPERSEDED":
			current.ReviewDueAt = nil
			current.ReviewedAt = stringPtr(now)
			current.ReviewedBy = stringPtr(actorID)
			supersedingClaimID := strings.TrimSpace(input.SupersedingClaimID)
			if supersedingClaimID == "" {
				return errors.New("superseding_claim_id is required")
			}
			if supersedingClaimID == claimID {
				return errors.New("superseding_claim_id must differ from claim_id")
			}
			if err := s.ensureKnowledgeClaimInWorkspaceTx(ctx, tx, workspaceID, supersedingClaimID); err != nil {
				return err
			}
			current.SupersededByClaimID = supersedingClaimID
		}
		var innerErr error
		record, event, invalidationEvents, innerErr = s.applyKnowledgeClaimLifecycleTx(ctx, tx, authority, current, actorID, eventType, map[string]any{
			"claim_id":               current.ClaimID,
			"status":                 current.Status,
			"reason":                 current.LifecycleReason,
			"actor_id":               actorID,
			"review_due_at":          derefString(current.ReviewDueAt),
			"reviewed_at":            derefString(current.ReviewedAt),
			"reviewed_by":            derefString(current.ReviewedBy),
			"superseded_by_claim_id": current.SupersededByClaimID,
			"conflicts_claim_id":     current.ConflictsClaimID,
		}, now, input.PromptContextEnvelope, knowledgeClaimPromptSurfaceForStatus(current.Status))
		if innerErr != nil {
			return innerErr
		}
		resolution := "claim_" + strings.ToLower(strings.TrimSpace(record.Status))
		queueRecord, queueEvent, innerErr = s.syncKnowledgeClaimReviewQueueWithAuthorityTx(ctx, tx, authority, record, actorID, strings.TrimSpace(input.AssignedTo), strings.TrimSpace(input.Urgency), resolution, now)
		return innerErr
	}); err != nil {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record, err = s.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	return record, queueRecord, event, queueEvent, invalidationEvents, nil
}

func (s *Store) ArchiveKnowledgeClaim(ctx context.Context, input KnowledgeClaimArchiveInput) (KnowledgeClaimRecord, error) {
	record, _, _, _, _, err := s.ArchiveKnowledgeClaimWithEffects(ctx, input)
	return record, err
}

func (s *Store) ArchiveKnowledgeClaimWithEvent(ctx context.Context, input KnowledgeClaimArchiveInput) (KnowledgeClaimRecord, RuntimeEventRecord, error) {
	record, _, event, _, _, err := s.ArchiveKnowledgeClaimWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) ArchiveKnowledgeClaimWithEffects(ctx context.Context, input KnowledgeClaimArchiveInput) (KnowledgeClaimRecord, OperatorQueueRecord, RuntimeEventRecord, RuntimeEventRecord, []RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	claimID := strings.TrimSpace(input.ClaimID)
	if claimID == "" {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("claim_id is required")
	}
	archivedBy := strings.TrimSpace(input.ArchivedBy)
	if archivedBy == "" {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, errors.New("archived_by is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record := KnowledgeClaimRecord{}
	queueRecord := OperatorQueueRecord{}
	event := RuntimeEventRecord{}
	queueEvent := RuntimeEventRecord{}
	invalidationEvents := []RuntimeEventRecord(nil)
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		found, innerQueueRecord, innerEvent, innerQueueEvent, innerInvalidationEvents, innerErr := s.archiveKnowledgeClaimTx(ctx, tx, authority, workspaceID, claimID, archivedBy, strings.TrimSpace(input.Reason), now, input.PromptContextEnvelope, "workspace.claim.archive")
		if innerErr != nil {
			return innerErr
		}
		if !found {
			return fmt.Errorf("knowledge claim not found: %s/%s", workspaceID, claimID)
		}
		queueRecord = innerQueueRecord
		event = innerEvent
		queueEvent = innerQueueEvent
		invalidationEvents = innerInvalidationEvents
		return nil
	}); err != nil {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	record, err = s.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		return KnowledgeClaimRecord{}, OperatorQueueRecord{}, RuntimeEventRecord{}, RuntimeEventRecord{}, nil, err
	}
	return record, queueRecord, event, queueEvent, invalidationEvents, nil
}

func normalizeKnowledgeClaimInput(input KnowledgeClaimInput) (KnowledgeClaimRecord, error) {
	record := KnowledgeClaimRecord{
		ClaimID:           strings.TrimSpace(input.ClaimID),
		WorkspaceID:       strings.TrimSpace(input.WorkspaceID),
		ClaimType:         normalizeKnowledgeClaimType(input.ClaimType),
		Status:            normalizeKnowledgeClaimStatus(input.Status),
		Subject:           strings.TrimSpace(input.Subject),
		Body:              strings.TrimSpace(input.Body),
		Summary:           strings.TrimSpace(input.Summary),
		Confidence:        clampUnitInterval(input.Confidence),
		SourceKind:        strings.TrimSpace(input.SourceKind),
		SourceID:          strings.TrimSpace(input.SourceID),
		MemoryID:          strings.TrimSpace(input.MemoryID),
		TaskID:            strings.TrimSpace(input.TaskID),
		SessionID:         strings.TrimSpace(input.SessionID),
		AgentID:           strings.TrimSpace(input.AgentID),
		SupersedesClaimID: strings.TrimSpace(input.SupersedesClaimID),
		ConflictsClaimID:  strings.TrimSpace(input.ConflictsClaimID),
		Evidence:          normalizeStringSlice(input.Evidence),
		Tags:              normalizeStringSlice(input.Tags),
	}
	if record.WorkspaceID == "" {
		return KnowledgeClaimRecord{}, errors.New("workspace_id is required")
	}
	if record.Subject == "" {
		return KnowledgeClaimRecord{}, errors.New("subject is required")
	}
	if record.Body == "" {
		return KnowledgeClaimRecord{}, errors.New("body is required")
	}
	if record.Status == "ARCHIVED" {
		return KnowledgeClaimRecord{}, errors.New("claim status ARCHIVED requires archive flow")
	}
	if record.SourceKind == "" {
		record.SourceKind = "manual"
	}
	return record, nil
}

func normalizeKnowledgeClaimType(claimType string) string {
	switch strings.ToUpper(strings.TrimSpace(claimType)) {
	case "DECISION":
		return "DECISION"
	case "DECISION_RECORD":
		return "DECISION_RECORD"
	case "ALTERNATIVE_BRANCH":
		return "ALTERNATIVE_BRANCH"
	case "LESSON":
		return "LESSON"
	case "PROCEDURE":
		return "PROCEDURE"
	case "ANTI_PROCEDURE":
		return "ANTI_PROCEDURE"
	case "INCIDENT":
		return "INCIDENT"
	case "BLOCKER":
		return "BLOCKER"
	case "BLOCKER_SYMPTOM":
		return "BLOCKER_SYMPTOM"
	case "BLOCKER_HYPOTHESIS":
		return "BLOCKER_HYPOTHESIS"
	case "CONSTRAINT":
		return "CONSTRAINT"
	case "DISSENT":
		return "DISSENT"
	case "DISSENT_MARKER":
		return "DISSENT_MARKER"
	case "DISSENT_CONTENT":
		return "DISSENT_CONTENT"
	case "HYPOTHESIS":
		return "HYPOTHESIS"
	case "ENTITY":
		return "ENTITY"
	case "UPDATE_DIGEST":
		return "UPDATE_DIGEST"
	case "SUMMARY":
		return "SUMMARY"
	case "EXPERIENCE":
		return "EXPERIENCE"
	default:
		return "FACT"
	}
}

func normalizeKnowledgeClaimStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "CONFIRMED":
		return "CONFIRMED"
	case "REVIEW":
		return "REVIEW"
	case "STALE":
		return "STALE"
	case "SUPERSEDED":
		return "SUPERSEDED"
	case "DISPUTED":
		return "DISPUTED"
	case "ARCHIVED":
		return "ARCHIVED"
	default:
		return "ACTIVE"
	}
}

func knowledgeClaimSelectColumns(alias string) string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}
	return prefix + `claim_id, ` + prefix + `workspace_id, ` + prefix + `claim_type, ` + prefix + `status, ` + prefix + `subject, ` + prefix + `body, ` + prefix + `summary, ` + prefix + `confidence,
	        ` + prefix + `source_kind, ` + prefix + `source_id, COALESCE(` + prefix + `memory_id,''), COALESCE(` + prefix + `task_id,''), COALESCE(` + prefix + `session_id,''), COALESCE(` + prefix + `agent_id,''),
	        COALESCE(` + prefix + `supersedes_claim_id,''), COALESCE(` + prefix + `superseded_by_claim_id,''), COALESCE(` + prefix + `conflicts_claim_id,''), ` + prefix + `evidence_json, ` + prefix + `tags_json,
	        ` + prefix + `lifecycle_reason, ` + prefix + `review_due_at, ` + prefix + `reviewed_at, ` + prefix + `reviewed_by, ` + prefix + `archived_at, ` + prefix + `archived_by, ` + prefix + `created_at, ` + prefix + `updated_at, ` + prefix + `recovery_reason`
}

func buildKnowledgeClaimWhere(filter KnowledgeClaimFilter, alias string, args *[]any) []string {
	prefix := alias
	if prefix != "" {
		prefix += "."
	}
	where := []string{prefix + `workspace_id = ?`}
	*args = append(*args, strings.TrimSpace(filter.WorkspaceID))
	if !filter.IncludeArchived {
		where = append(where, prefix+`archived_at IS NULL`, prefix+`status <> 'ARCHIVED'`)
	}
	if trimmed := strings.TrimSpace(filter.ClaimType); trimmed != "" {
		where = append(where, prefix+`claim_type = ?`)
		*args = append(*args, normalizeKnowledgeClaimType(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.Status); trimmed != "" {
		where = append(where, prefix+`status = ?`)
		*args = append(*args, normalizeKnowledgeClaimStatus(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		where = append(where, `COALESCE(`+prefix+`agent_id,'') = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
		where = append(where, `COALESCE(`+prefix+`session_id,'') = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.TaskID); trimmed != "" {
		where = append(where, `COALESCE(`+prefix+`task_id,'') = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.MemoryID); trimmed != "" {
		where = append(where, `COALESCE(`+prefix+`memory_id,'') = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.SourceKind); trimmed != "" {
		where = append(where, prefix+`source_kind = ?`)
		*args = append(*args, trimmed)
	}
	return where
}

func knowledgeClaimTypePrioritySQL(alias string) string {
	prefix := alias
	if prefix != "" {
		prefix += "."
	}
	return `CASE ` + prefix + `claim_type
		WHEN 'DECISION' THEN 6
		WHEN 'PROCEDURE' THEN 5
		WHEN 'ANTI_PROCEDURE' THEN 5
		WHEN 'INCIDENT' THEN 4
		WHEN 'FACT' THEN 3
		WHEN 'LESSON' THEN 2
		WHEN 'UPDATE_DIGEST' THEN 1
		ELSE 0
	END`
}

func knowledgeClaimStatusPrioritySQL(alias string) string {
	prefix := alias
	if prefix != "" {
		prefix += "."
	}
	return `CASE ` + prefix + `status
		WHEN 'CONFIRMED' THEN 6
		WHEN 'ACTIVE' THEN 5
		WHEN 'REVIEW' THEN 4
		WHEN 'STALE' THEN 3
		WHEN 'DISPUTED' THEN 2
		WHEN 'SUPERSEDED' THEN 1
		ELSE 0
	END`
}

func isKnowledgeClaimOperationalStatus(status string) bool {
	switch normalizeKnowledgeClaimStatus(status) {
	case "ACTIVE", "CONFIRMED", "REVIEW":
		return true
	default:
		return false
	}
}

func isKnowledgeClaimReviewStatus(status string) bool {
	switch normalizeKnowledgeClaimStatus(status) {
	case "REVIEW", "DISPUTED", "STALE":
		return true
	default:
		return false
	}
}

func collectKnowledgeClaimRows(rows *sql.Rows) ([]KnowledgeClaimRecord, error) {
	out := []KnowledgeClaimRecord{}
	for rows.Next() {
		record, err := scanKnowledgeClaimRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate knowledge claim rows: %w", err)
	}
	return out, nil
}

func scanKnowledgeClaimRecord(scanner interface{ Scan(dest ...any) error }) (KnowledgeClaimRecord, error) {
	var record KnowledgeClaimRecord
	var evidenceJSON string
	var tagsJSON string
	var reviewDueAt sql.NullString
	var reviewedAt sql.NullString
	var reviewedBy sql.NullString
	var archivedAt sql.NullString
	var archivedBy sql.NullString
	if err := scanner.Scan(
		&record.ClaimID,
		&record.WorkspaceID,
		&record.ClaimType,
		&record.Status,
		&record.Subject,
		&record.Body,
		&record.Summary,
		&record.Confidence,
		&record.SourceKind,
		&record.SourceID,
		&record.MemoryID,
		&record.TaskID,
		&record.SessionID,
		&record.AgentID,
		&record.SupersedesClaimID,
		&record.SupersededByClaimID,
		&record.ConflictsClaimID,
		&evidenceJSON,
		&tagsJSON,
		&record.LifecycleReason,
		&reviewDueAt,
		&reviewedAt,
		&reviewedBy,
		&archivedAt,
		&archivedBy,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.RecoveryReason,
	); err != nil {
		return KnowledgeClaimRecord{}, err
	}
	_ = json.Unmarshal([]byte(evidenceJSON), &record.Evidence)
	_ = json.Unmarshal([]byte(tagsJSON), &record.Tags)
	record.ReviewDueAt = nullStringPtr(reviewDueAt)
	record.ReviewedAt = nullStringPtr(reviewedAt)
	record.ReviewedBy = nullStringPtr(reviewedBy)
	record.ArchivedAt = nullStringPtr(archivedAt)
	record.ArchivedBy = nullStringPtr(archivedBy)
	return record, nil
}

func (s *Store) ensureWorkspaceMemoryInWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) error {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM workspace_memory WHERE workspace_id = ? AND memory_id = ?`,
		workspaceID,
		memoryID,
	).Scan(&count); err != nil {
		return fmt.Errorf("check workspace memory existence: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("workspace memory not found: %s/%s", workspaceID, memoryID)
	}
	return nil
}

func (s *Store) ensureKnowledgeClaimInWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, claimID string) error {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM knowledge_claims WHERE workspace_id = ? AND claim_id = ?`,
		workspaceID,
		claimID,
	).Scan(&count); err != nil {
		return fmt.Errorf("check knowledge claim existence: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("knowledge claim not found: %s/%s", workspaceID, claimID)
	}
	return nil
}

func (s *Store) UpsertExecutionRun(ctx context.Context, input ExecutionRunInput) (ExecutionRunRecord, error) {
	record, err := normalizeExecutionRunInput(input)
	if err != nil {
		return ExecutionRunRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ExecutionRunRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, _, innerErr = s.upsertExecutionRunTx(ctx, tx, authority, record, now)
		return innerErr
	}); err != nil {
		return ExecutionRunRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if record.TaskID != "" && isExecutionRunTerminal(record.Status) && record.Status == "FAILED" {
		_ = s.checkAndGenerateFailureTension(ctx, record.WorkspaceID, record.TaskID)
	}

	return s.getExecutionRun(ctx, record.WorkspaceID, record.RunID)
}

func (s *Store) UpsertExecutionRunWithEvent(ctx context.Context, input ExecutionRunInput) (ExecutionRunRecord, RuntimeEventRecord, error) {
	record, err := normalizeExecutionRunInput(input)
	if err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	record.RunID = firstNonEmpty(record.RunID, nextID("run"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	var (
		run   ExecutionRunRecord
		event RuntimeEventRecord
	)
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		run, event, innerErr = s.upsertExecutionRunTx(ctx, tx, authority, record, now)
		return innerErr
	}); err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	run, err = s.getExecutionRun(ctx, run.WorkspaceID, run.RunID)
	if err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	return run, event, nil
}

func (s *Store) upsertExecutionRunTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record ExecutionRunRecord, now string) (ExecutionRunRecord, RuntimeEventRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	if record.AgentID != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, record.WorkspaceID, record.AgentID); err != nil {
			return ExecutionRunRecord{}, RuntimeEventRecord{}, err
		}
	}
	if record.TaskID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, record.WorkspaceID, record.TaskID); err != nil {
			return ExecutionRunRecord{}, RuntimeEventRecord{}, err
		}
	}
	if record.SessionID != "" {
		if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, record.WorkspaceID, record.SessionID); err != nil {
			return ExecutionRunRecord{}, RuntimeEventRecord{}, err
		}
	}
	record.RunID = firstNonEmpty(record.RunID, nextID("run"))
	createdAt := now
	var existingCreatedAt string
	var existingWorkspaceID string
	var existingVerificationJSON string
	var existingTaskID, existingSessionID, existingAgentID, existingStatus string
	existingRunFound := false
	err := tx.QueryRowContext(ctx, `SELECT workspace_id, created_at, verification_json, COALESCE(task_id,''), COALESCE(session_id,''), COALESCE(agent_id,''), status FROM execution_runs WHERE run_id = ?`, record.RunID).Scan(&existingWorkspaceID, &existingCreatedAt, &existingVerificationJSON, &existingTaskID, &existingSessionID, &existingAgentID, &existingStatus)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if record.VerificationJSON == nil {
			record.VerificationJSON = map[string]any{}
		} else {
			record.VerificationJSON = stripExecutionRunLiveBindingProof(record.VerificationJSON)
		}
	case err != nil:
		return ExecutionRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("query execution run: %w", err)
	default:
		if strings.TrimSpace(existingWorkspaceID) != strings.TrimSpace(record.WorkspaceID) {
			return ExecutionRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: execution run workspace mismatch for existing run_id=%s existing_workspace_id=%s workspace_id=%s", ErrExecutionRunBindingInvalid, record.RunID, existingWorkspaceID, record.WorkspaceID)
		}
		existingRunFound = true
		createdAt = existingCreatedAt
		var bindingErr error
		record, bindingErr = preserveExistingExecutionRunBinding(record, existingTaskID, existingSessionID, existingAgentID)
		if bindingErr != nil {
			return ExecutionRunRecord{}, RuntimeEventRecord{}, bindingErr
		}
		if strings.TrimSpace(existingVerificationJSON) == "" {
			existingVerificationJSON = "{}"
		}
		existingVerification := map[string]any{}
		if err := json.Unmarshal([]byte(existingVerificationJSON), &existingVerification); err != nil {
			return ExecutionRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("decode existing execution run verification: %w", err)
		}
		if existingVerification == nil {
			existingVerification = map[string]any{}
		}
		if record.VerificationJSON == nil {
			record.VerificationJSON = existingVerification
		} else {
			record.VerificationJSON = stripExecutionRunLiveBindingProof(record.VerificationJSON)
			if reusableProof := executionRunReusableBindingProofValue(existingVerification); reusableProof != nil {
				record.VerificationJSON["live_execution_binding"] = reusableProof
			}
		}
	}
	if record.promptContextEnvelope != nil {
		record.VerificationJSON = AttachExecutionPromptContextEnvelope(record.VerificationJSON, record.promptContextEnvelope)
	}
	if err := validateExecutionPromptCapabilityEvidence("execution_run", record.VerificationJSON, nil); err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	if err := validateExecutionPromptContextEnvelope("execution_run", record.VerificationJSON); err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	record, err = s.validateExecutionRunBindingTx(ctx, tx, record, existingRunFound, existingStatus)
	if err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	if existingRunFound && isExecutionRunTerminal(existingStatus) && !isExecutionRunTerminal(record.Status) {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: run_id=%s existing_status=%s requested_status=%s", ErrExecutionRunTerminalRegression, record.RunID, strings.TrimSpace(existingStatus), strings.TrimSpace(record.Status))
	}
	if err := validateExecutionPromptContextEnvelopeBinding("execution_run", record.VerificationJSON, record.WorkspaceID, record.AgentID); err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = now
	if isExecutionRunTerminal(record.Status) {
		record.ClosedAt = &record.UpdatedAt
	}
	verificationJSON := mustJSON(record.VerificationJSON)

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO execution_runs(run_id, workspace_id, task_id, session_id, agent_id, title, summary, status, outcome, verification_json, created_at, updated_at, closed_at)
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(run_id) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    task_id = excluded.task_id,
		    session_id = excluded.session_id,
		    agent_id = excluded.agent_id,
		    title = excluded.title,
		    summary = excluded.summary,
		    status = excluded.status,
		    outcome = excluded.outcome,
		    verification_json = excluded.verification_json,
		    updated_at = excluded.updated_at,
		    closed_at = excluded.closed_at`,
		record.RunID,
		record.WorkspaceID,
		blankStringOrNil(record.TaskID),
		blankStringOrNil(record.SessionID),
		blankStringOrNil(record.AgentID),
		record.Title,
		record.Summary,
		record.Status,
		record.Outcome,
		verificationJSON,
		record.CreatedAt,
		record.UpdatedAt,
		blankStringOrNil(derefString(record.ClosedAt)),
	); err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("upsert execution run: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, record.WorkspaceID); err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch workspace after execution run update: %w", err)
	}
	event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    record.RunID,
		ActorType:   "agent",
		ActorID:     firstNonEmpty(record.AgentID, record.WorkspaceID),
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		PayloadJSON: mustJSON(map[string]any{
			"status":       record.Status,
			"outcome":      record.Outcome,
			"title":        record.Title,
			"summary":      record.Summary,
			"agent_id":     record.AgentID,
			"session_id":   record.SessionID,
			"task_id":      record.TaskID,
			"verification": record.VerificationJSON,
		}),
		CreatedAt: now,
	})
	if err != nil {
		return ExecutionRunRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func (s *Store) checkAndGenerateFailureTension(ctx context.Context, workspaceID, taskID string) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT status
		FROM execution_runs
		WHERE workspace_id = ? AND task_id = ?
		ORDER BY created_at DESC
		LIMIT 3
	`, workspaceID, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()

	failureCount := 0
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return err
		}
		if status == "FAILED" {
			failureCount++
		} else {
			break
		}
	}

	if failureCount >= 3 {
		return s.EnsureGovernedTension(ctx, EnsureGovernedTensionInput{
			WorkspaceID:    workspaceID,
			TensionType:    "failure",
			ProtoClusterID: "",
			AnchorRef:      taskID,
			Title:          "Repeated Task Failures: " + taskID,
			Summary:        "Task has failed 3 or more times consecutively. Requires immediate debugging and stabilization.",
			EvidenceRefs:   []string{"task:" + taskID},
		})
	}
	return nil
}

func (s *Store) RecordExecutionStep(ctx context.Context, input ExecutionStepInput) (ExecutionStepRecord, error) {
	record, err := normalizeExecutionStepInput(input)
	if err != nil {
		return ExecutionStepRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ExecutionStepRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, _, innerErr = s.recordExecutionStepTx(ctx, tx, authority, record, now)
		return innerErr
	}); err != nil {
		return ExecutionStepRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	return s.getExecutionStep(ctx, record.WorkspaceID, record.StepID)
}

func (s *Store) RecordExecutionStepWithEvent(ctx context.Context, input ExecutionStepInput) (ExecutionStepRecord, RuntimeEventRecord, error) {
	record, err := normalizeExecutionStepInput(input)
	if err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	record.StepID = firstNonEmpty(record.StepID, nextID("step"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	var (
		step  ExecutionStepRecord
		event RuntimeEventRecord
	)
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		step, event, innerErr = s.recordExecutionStepTx(ctx, tx, authority, record, now)
		return innerErr
	}); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	step, err = s.getExecutionStep(ctx, step.WorkspaceID, step.StepID)
	if err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	return step, event, nil
}

func (s *Store) recordExecutionStepTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record ExecutionStepRecord, now string) (ExecutionStepRecord, RuntimeEventRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	run, err := s.getExecutionRunTx(ctx, tx, record.WorkspaceID, record.RunID)
	if err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	if record.ParentStepID != "" {
		if err := s.ensureExecutionStepInRunTx(ctx, tx, record.WorkspaceID, record.RunID, record.ParentStepID); err != nil {
			return ExecutionStepRecord{}, RuntimeEventRecord{}, err
		}
	}
	record.StepID = firstNonEmpty(record.StepID, nextID("step"))
	createdAt := now
	var existingCreatedAt string
	var existingEvidenceJSON string
	var existingVerificationJSON string
	err = tx.QueryRowContext(ctx, `SELECT created_at, evidence_json, verification_json FROM execution_steps WHERE workspace_id = ? AND step_id = ?`, record.WorkspaceID, record.StepID).Scan(&existingCreatedAt, &existingEvidenceJSON, &existingVerificationJSON)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if record.Evidence == nil {
			record.Evidence = []string{}
		}
		if record.VerificationJSON == nil {
			record.VerificationJSON = map[string]any{}
		}
	case err != nil:
		return ExecutionStepRecord{}, RuntimeEventRecord{}, fmt.Errorf("query execution step: %w", err)
	default:
		createdAt = existingCreatedAt
		if !record.evidenceProvided {
			if strings.TrimSpace(existingEvidenceJSON) == "" {
				existingEvidenceJSON = "[]"
			}
			if err := json.Unmarshal([]byte(existingEvidenceJSON), &record.Evidence); err != nil {
				return ExecutionStepRecord{}, RuntimeEventRecord{}, fmt.Errorf("decode existing execution step evidence: %w", err)
			}
			if record.Evidence == nil {
				record.Evidence = []string{}
			}
		}
		if !record.verificationProvided {
			if strings.TrimSpace(existingVerificationJSON) == "" {
				existingVerificationJSON = "{}"
			}
			if err := json.Unmarshal([]byte(existingVerificationJSON), &record.VerificationJSON); err != nil {
				return ExecutionStepRecord{}, RuntimeEventRecord{}, fmt.Errorf("decode existing execution step verification: %w", err)
			}
			if record.VerificationJSON == nil {
				record.VerificationJSON = map[string]any{}
			}
		}
	}
	if record.promptContextEnvelope != nil {
		record.VerificationJSON = AttachExecutionPromptContextEnvelope(record.VerificationJSON, record.promptContextEnvelope)
	}
	if err := validateExecutionPromptCapabilityEvidence("execution_step", record.VerificationJSON, record.Evidence); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	if err := validateExecutionPromptContextEnvelope("execution_step", record.VerificationJSON); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	if err := validateExecutionPromptContextEnvelopeBinding("execution_step", record.VerificationJSON, record.WorkspaceID, run.AgentID); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	evidenceJSON, err := json.Marshal(record.Evidence)
	if err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, fmt.Errorf("encode execution step evidence: %w", err)
	}
	verificationJSON := mustJSON(record.VerificationJSON)
	record.CreatedAt = createdAt
	record.UpdatedAt = now
	if isExecutionStepTerminal(record.Status) {
		record.CompletedAt = &record.UpdatedAt
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO execution_steps(
		    step_id, run_id, workspace_id, parent_step_id, phase, title, summary, status,
		    sort_order, evidence_json, verification_json, created_at, updated_at, completed_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(step_id) DO UPDATE SET
		    run_id = excluded.run_id,
		    workspace_id = excluded.workspace_id,
		    parent_step_id = excluded.parent_step_id,
		    phase = excluded.phase,
		    title = excluded.title,
		    summary = excluded.summary,
		    status = excluded.status,
		    sort_order = excluded.sort_order,
		    evidence_json = excluded.evidence_json,
		    verification_json = excluded.verification_json,
		    updated_at = excluded.updated_at,
		    completed_at = excluded.completed_at`,
		record.StepID,
		record.RunID,
		record.WorkspaceID,
		blankStringOrNil(record.ParentStepID),
		record.Phase,
		record.Title,
		record.Summary,
		record.Status,
		record.SortOrder,
		string(evidenceJSON),
		verificationJSON,
		record.CreatedAt,
		record.UpdatedAt,
		blankStringOrNil(derefString(record.CompletedAt)),
	); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, fmt.Errorf("upsert execution step: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE execution_runs SET updated_at = ? WHERE run_id = ?`, now, run.RunID); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch execution run after step update: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, record.WorkspaceID); err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch workspace after execution step update: %w", err)
	}
	event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    record.StepID,
		ActorType:   "agent",
		ActorID:     firstNonEmpty(run.AgentID, record.WorkspaceID),
		AgentID:     run.AgentID,
		SessionID:   run.SessionID,
		TaskID:      run.TaskID,
		PayloadJSON: mustJSON(map[string]any{
			"run_id":       record.RunID,
			"phase":        record.Phase,
			"status":       record.Status,
			"title":        record.Title,
			"summary":      record.Summary,
			"sort_order":   record.SortOrder,
			"verification": record.VerificationJSON,
		}),
		CreatedAt: now,
	})
	if err != nil {
		return ExecutionStepRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func (s *Store) GetExecutionRun(ctx context.Context, workspaceID, runID string) (ExecutionRunDetail, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	runID = strings.TrimSpace(runID)
	if workspaceID == "" {
		return ExecutionRunDetail{}, errors.New("workspace_id is required")
	}
	if runID == "" {
		return ExecutionRunDetail{}, errors.New("run_id is required")
	}
	run, err := s.getExecutionRun(ctx, workspaceID, runID)
	if err != nil {
		return ExecutionRunDetail{}, err
	}
	steps, err := s.listExecutionSteps(ctx, workspaceID, runID)
	if err != nil {
		return ExecutionRunDetail{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return ExecutionRunDetail{}, err
	}
	return ExecutionRunDetail{TimeAuthority: authority, Run: run, Steps: steps}, nil
}

func (s *Store) ListExecutionRuns(ctx context.Context, filter ExecutionRunFilter) ([]ExecutionRunRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	query := strings.Builder{}
	query.WriteString(`SELECT run_id, workspace_id, COALESCE(task_id,''), COALESCE(session_id,''), COALESCE(agent_id,''),
	        title, summary, status, outcome, verification_json, created_at, updated_at, closed_at
	   FROM execution_runs
	  WHERE workspace_id = ?`)
	args := []any{workspaceID}
	if trimmed := strings.TrimSpace(filter.Status); trimmed != "" {
		query.WriteString(` AND status = ?`)
		args = append(args, normalizeExecutionRunStatus(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.TaskID); trimmed != "" {
		query.WriteString(` AND COALESCE(task_id,'') = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
		query.WriteString(` AND COALESCE(session_id,'') = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		query.WriteString(` AND COALESCE(agent_id,'') = ?`)
		args = append(args, trimmed)
	}
	query.WriteString(` ORDER BY updated_at DESC, run_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query execution runs: %w", err)
	}
	defer rows.Close()

	out := []ExecutionRunRecord{}
	for rows.Next() {
		record, err := scanExecutionRunRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution runs: %w", err)
	}
	return out, nil
}

func (s *Store) CancelExecutionRunsForAgentStopWithEvent(ctx context.Context, input ExecutionAgentRunsCancelInput) (ExecutionAgentRunsCancelResult, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	agentID := strings.TrimSpace(input.AgentID)
	if workspaceID == "" {
		return ExecutionAgentRunsCancelResult{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if agentID == "" {
		return ExecutionAgentRunsCancelResult{}, RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	outcome := strings.ToUpper(strings.TrimSpace(input.Outcome))
	if outcome == "" {
		outcome = "STOPPED_BY_MANAGER"
	}
	summary := strings.TrimSpace(input.Summary)
	if summary == "" {
		summary = "Cancelled by managed agent stop."
	}
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "manager")
	actorID := firstNonEmpty(strings.TrimSpace(input.ActorID), agentID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ExecutionAgentRunsCancelResult{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ExecutionAgentRunsCancelResult{}, RuntimeEventRecord{}, fmt.Errorf("begin execution agent stop cancel tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result := ExecutionAgentRunsCancelResult{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		Outcome:         outcome,
		Summary:         summary,
		TransitionState: "terminalized",
	}
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		stepResult, err := tx.ExecContext(ctx, `
UPDATE execution_steps
   SET status = 'CANCELLED',
       completed_at = COALESCE(completed_at, ?),
       updated_at = ?
 WHERE workspace_id = ?
   AND run_id IN (
       SELECT run_id
         FROM execution_runs
        WHERE workspace_id = ?
          AND COALESCE(agent_id, '') = ?
          AND status IN ('PLANNED', 'ACTIVE', 'BLOCKED', 'VERIFYING')
   )
   AND status IN ('PENDING', 'ACTIVE', 'BLOCKED')`,
			now, now, workspaceID, workspaceID, agentID)
		if err != nil {
			return fmt.Errorf("cancel execution steps for stopped agent: %w", err)
		}
		runResult, err := tx.ExecContext(ctx, `
UPDATE execution_runs
   SET status = 'CANCELLED',
       outcome = ?,
       summary = CASE
           WHEN COALESCE(summary, '') = '' THEN ?
           ELSE summary || '; ' || ?
       END,
       closed_at = COALESCE(closed_at, ?),
       updated_at = ?
 WHERE workspace_id = ?
   AND COALESCE(agent_id, '') = ?
   AND status IN ('PLANNED', 'ACTIVE', 'BLOCKED', 'VERIFYING')`,
			outcome, summary, summary, now, now, workspaceID, agentID)
		if err != nil {
			return fmt.Errorf("cancel execution runs for stopped agent: %w", err)
		}
		result.StepsCancelled, err = stepResult.RowsAffected()
		if err != nil {
			return fmt.Errorf("cancel execution steps rows affected: %w", err)
		}
		result.RunsCancelled, err = runResult.RowsAffected()
		if err != nil {
			return fmt.Errorf("cancel execution runs rows affected: %w", err)
		}
		payload := map[string]any{
			"workspace_id":     workspaceID,
			"agent_id":         agentID,
			"runs_cancelled":   result.RunsCancelled,
			"steps_cancelled":  result.StepsCancelled,
			"outcome":          outcome,
			"summary":          summary,
			"transition_state": result.TransitionState,
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: workspaceID,
			EventType:   "workspace.execution.agent_runs.cancelled",
			EntityType:  "agent",
			EntityID:    agentID,
			ActorType:   actorType,
			ActorID:     actorID,
			AgentID:     agentID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ExecutionAgentRunsCancelResult{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ExecutionAgentRunsCancelResult{}, RuntimeEventRecord{}, fmt.Errorf("commit execution agent stop cancel tx: %w", err)
	}
	result.RuntimeEventID = event.EventID
	return result, event, nil
}

func normalizeExecutionRunInput(input ExecutionRunInput) (ExecutionRunRecord, error) {
	record := ExecutionRunRecord{
		RunID:            strings.TrimSpace(input.RunID),
		WorkspaceID:      strings.TrimSpace(input.WorkspaceID),
		TaskID:           strings.TrimSpace(input.TaskID),
		SessionID:        strings.TrimSpace(input.SessionID),
		AgentID:          strings.TrimSpace(input.AgentID),
		Title:            strings.TrimSpace(input.Title),
		Summary:          strings.TrimSpace(input.Summary),
		Status:           normalizeExecutionRunStatus(input.Status),
		Outcome:          strings.TrimSpace(input.Outcome),
		VerificationJSON: input.Verification,

		promptContextEnvelope: input.PromptContextEnvelope,
	}
	if record.WorkspaceID == "" {
		return ExecutionRunRecord{}, errors.New("workspace_id is required")
	}
	if record.Title == "" {
		return ExecutionRunRecord{}, errors.New("title is required")
	}
	if err := validateExecutionPromptCapabilityEvidence("execution_run", record.VerificationJSON, nil); err != nil {
		return ExecutionRunRecord{}, err
	}
	if err := validateExecutionPromptContextEnvelope("execution_run", record.VerificationJSON); err != nil {
		return ExecutionRunRecord{}, err
	}
	return record, nil
}

func normalizeExecutionRunStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE":
		return "ACTIVE"
	case "BLOCKED":
		return "BLOCKED"
	case "VERIFYING":
		return "VERIFYING"
	case "COMPLETED":
		return "COMPLETED"
	case "FAILED":
		return "FAILED"
	case "CANCELLED":
		return "CANCELLED"
	case "TIMED_OUT":
		return "TIMED_OUT"
	default:
		return "PLANNED"
	}
}

func isExecutionRunTerminal(status string) bool {
	switch normalizeExecutionRunStatus(status) {
	case "COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT":
		return true
	default:
		return false
	}
}

func normalizeExecutionStepInput(input ExecutionStepInput) (ExecutionStepRecord, error) {
	record := ExecutionStepRecord{
		StepID:           strings.TrimSpace(input.StepID),
		RunID:            strings.TrimSpace(input.RunID),
		WorkspaceID:      strings.TrimSpace(input.WorkspaceID),
		ParentStepID:     strings.TrimSpace(input.ParentStepID),
		Phase:            normalizeExecutionStepPhase(input.Phase),
		Title:            strings.TrimSpace(input.Title),
		Summary:          strings.TrimSpace(input.Summary),
		Status:           normalizeExecutionStepStatus(input.Status),
		SortOrder:        input.SortOrder,
		Evidence:         normalizeStringSlice(input.Evidence),
		VerificationJSON: input.Verification,

		evidenceProvided:      input.Evidence != nil,
		verificationProvided:  input.Verification != nil,
		promptContextEnvelope: input.PromptContextEnvelope,
	}
	if record.WorkspaceID == "" {
		return ExecutionStepRecord{}, errors.New("workspace_id is required")
	}
	if record.RunID == "" {
		return ExecutionStepRecord{}, errors.New("run_id is required")
	}
	if record.Title == "" {
		return ExecutionStepRecord{}, errors.New("title is required")
	}
	return record, nil
}

func normalizeExecutionStepPhase(phase string) string {
	switch strings.ToUpper(strings.TrimSpace(phase)) {
	case "EXECUTE":
		return "EXECUTE"
	case "VERIFY":
		return "VERIFY"
	default:
		return "PLAN"
	}
}

func normalizeExecutionStepStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE":
		return "ACTIVE"
	case "BLOCKED":
		return "BLOCKED"
	case "COMPLETED":
		return "COMPLETED"
	case "FAILED":
		return "FAILED"
	case "CANCELLED":
		return "CANCELLED"
	case "TIMED_OUT":
		return "TIMED_OUT"
	case "SKIPPED":
		return "SKIPPED"
	default:
		return "PENDING"
	}
}

func isExecutionStepTerminal(status string) bool {
	switch normalizeExecutionStepStatus(status) {
	case "COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT", "SKIPPED":
		return true
	default:
		return false
	}
}

func (s *Store) getExecutionRun(ctx context.Context, workspaceID, runID string) (ExecutionRunRecord, error) {
	row := s.db.QueryRowContext(
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
			return ExecutionRunRecord{}, fmt.Errorf("execution run not found: %s/%s", workspaceID, runID)
		}
		return ExecutionRunRecord{}, err
	}
	return record, nil
}

func (s *Store) getExecutionRunTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string) (ExecutionRunRecord, error) {
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
			return ExecutionRunRecord{}, fmt.Errorf("execution run not found: %s/%s", workspaceID, runID)
		}
		return ExecutionRunRecord{}, err
	}
	return record, nil
}

func (s *Store) listExecutionSteps(ctx context.Context, workspaceID, runID string) ([]ExecutionStepRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT step_id, run_id, workspace_id, COALESCE(parent_step_id,''), phase, title, summary, status,
		        sort_order, evidence_json, verification_json, created_at, updated_at, completed_at
		   FROM execution_steps
		  WHERE workspace_id = ? AND run_id = ?
		  ORDER BY phase, sort_order, created_at, step_id`,
		workspaceID,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("query execution steps: %w", err)
	}
	defer rows.Close()

	out := []ExecutionStepRecord{}
	for rows.Next() {
		record, err := scanExecutionStepRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution steps: %w", err)
	}
	return out, nil
}

func (s *Store) getExecutionStep(ctx context.Context, workspaceID, stepID string) (ExecutionStepRecord, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT step_id, run_id, workspace_id, COALESCE(parent_step_id,''), phase, title, summary, status,
		        sort_order, evidence_json, verification_json, created_at, updated_at, completed_at
		   FROM execution_steps
		  WHERE workspace_id = ? AND step_id = ?`,
		workspaceID,
		stepID,
	)
	record, err := scanExecutionStepRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionStepRecord{}, fmt.Errorf("execution step not found: %s/%s", workspaceID, stepID)
		}
		return ExecutionStepRecord{}, err
	}
	return record, nil
}

func scanExecutionRunRecord(scanner interface{ Scan(dest ...any) error }) (ExecutionRunRecord, error) {
	var record ExecutionRunRecord
	var verificationJSON string
	var closedAt sql.NullString
	if err := scanner.Scan(
		&record.RunID,
		&record.WorkspaceID,
		&record.TaskID,
		&record.SessionID,
		&record.AgentID,
		&record.Title,
		&record.Summary,
		&record.Status,
		&record.Outcome,
		&verificationJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
		&closedAt,
	); err != nil {
		return ExecutionRunRecord{}, err
	}
	if strings.TrimSpace(verificationJSON) == "" {
		verificationJSON = "{}"
	}
	if err := json.Unmarshal([]byte(verificationJSON), &record.VerificationJSON); err != nil {
		return ExecutionRunRecord{}, fmt.Errorf("decode execution run verification: %w", err)
	}
	if record.VerificationJSON == nil {
		record.VerificationJSON = map[string]any{}
	}
	record.ClosedAt = nullStringPtr(closedAt)
	return record, nil
}

func scanExecutionStepRecord(scanner interface{ Scan(dest ...any) error }) (ExecutionStepRecord, error) {
	var record ExecutionStepRecord
	var evidenceJSON string
	var verificationJSON string
	var completedAt sql.NullString
	if err := scanner.Scan(
		&record.StepID,
		&record.RunID,
		&record.WorkspaceID,
		&record.ParentStepID,
		&record.Phase,
		&record.Title,
		&record.Summary,
		&record.Status,
		&record.SortOrder,
		&evidenceJSON,
		&verificationJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
		&completedAt,
	); err != nil {
		return ExecutionStepRecord{}, err
	}
	_ = json.Unmarshal([]byte(evidenceJSON), &record.Evidence)
	record.VerificationJSON = map[string]any{}
	_ = json.Unmarshal([]byte(verificationJSON), &record.VerificationJSON)
	record.CompletedAt = nullStringPtr(completedAt)
	return record, nil
}

func (s *Store) ensureExecutionStepInRunTx(ctx context.Context, tx *sql.Tx, workspaceID, runID, stepID string) error {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM execution_steps WHERE workspace_id = ? AND run_id = ? AND step_id = ?`,
		workspaceID,
		runID,
		stepID,
	).Scan(&count); err != nil {
		return fmt.Errorf("check execution step existence: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("execution step not found: %s/%s", runID, stepID)
	}
	return nil
}

func (s *Store) PutCapabilityPolicy(ctx context.Context, input CapabilityPolicyInput) (CapabilityPolicyRecord, error) {
	record, err := normalizeCapabilityPolicyInput(input)
	if err != nil {
		return CapabilityPolicyRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return CapabilityPolicyRecord{}, err
	}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, _, innerErr = s.putCapabilityPolicyTx(ctx, tx, authority, record, now, input.PromptContextEnvelope, capabilityPolicyPromptContextSurface(input.PromptContextSurface))
		return innerErr
	}); err != nil {
		return CapabilityPolicyRecord{}, err
	}
	return s.getCapabilityPolicyByMatch(ctx, record.WorkspaceID, record.SubjectType, record.SubjectID, record.Capability, record.ToolID)
}

func (s *Store) PutCapabilityPolicyWithEvent(ctx context.Context, input CapabilityPolicyInput) (CapabilityPolicyRecord, RuntimeEventRecord, error) {
	record, err := normalizeCapabilityPolicyInput(input)
	if err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	policy := CapabilityPolicyRecord{}
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		policy, event, innerErr = s.putCapabilityPolicyTx(ctx, tx, authority, record, now, input.PromptContextEnvelope, capabilityPolicyPromptContextSurface(input.PromptContextSurface))
		return innerErr
	}); err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	policy, err = s.getCapabilityPolicyByMatch(ctx, policy.WorkspaceID, policy.SubjectType, policy.SubjectID, policy.Capability, policy.ToolID)
	if err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	return policy, event, nil
}

func (s *Store) putCapabilityPolicyTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, record CapabilityPolicyRecord, now string, promptContextEnvelope map[string]any, promptContextSurface string) (CapabilityPolicyRecord, RuntimeEventRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	existingPolicyID, err := s.lookupCapabilityPolicyIDByMatchTx(ctx, tx, record.WorkspaceID, record.SubjectType, record.SubjectID, record.Capability, record.ToolID)
	if err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	record.PolicyID = firstNonEmpty(existingPolicyID, record.PolicyID, nextID("policy"))
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspace_capability_policies(
		    policy_id, workspace_id, subject_type, subject_id, capability, tool_id, effect, reason, created_by, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(workspace_id, subject_type, subject_id, capability, tool_id) DO UPDATE SET
		    effect = excluded.effect,
		    reason = excluded.reason,
		    created_by = excluded.created_by,
		    updated_at = excluded.updated_at`,
		record.PolicyID,
		record.WorkspaceID,
		record.SubjectType,
		record.SubjectID,
		record.Capability,
		record.ToolID,
		record.Effect,
		record.Reason,
		record.CreatedBy,
		now,
		now,
	); err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, fmt.Errorf("upsert capability policy: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, record.WorkspaceID); err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch workspace after capability policy update: %w", err)
	}
	payload := map[string]any{
		"policy_id":    record.PolicyID,
		"workspace_id": record.WorkspaceID,
		"subject_type": record.SubjectType,
		"subject_id":   record.SubjectID,
		"capability":   record.Capability,
		"tool_id":      record.ToolID,
		"effect":       record.Effect,
		"reason":       record.Reason,
		"created_by":   record.CreatedBy,
		"created_at":   now,
		"updated_at":   now,
		"event_type":   "capability_policy.put",
		"entity_type":  "capability_policy",
		"entity_id":    record.PolicyID,
		"actor_type":   "operator",
		"actor_id":     record.CreatedBy,
		"ownership":    capabilityPolicyOwnership(record.Capability),
	}
	payload, err = attachCapabilityPolicyPromptContextEnvelope(payload, promptContextEnvelope, promptContextSurface, map[string]string{
		"workspace_id": record.WorkspaceID,
		"policy_id":    record.PolicyID,
		"subject_type": record.SubjectType,
		"subject_id":   record.SubjectID,
		"capability":   record.Capability,
		"tool_id":      record.ToolID,
		"effect":       record.Effect,
		"created_by":   record.CreatedBy,
		"event_type":   "capability_policy.put",
		"entity_type":  "capability_policy",
		"entity_id":    record.PolicyID,
		"actor_type":   "operator",
		"actor_id":     record.CreatedBy,
	})
	if err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    record.PolicyID,
		ActorType:   "operator",
		ActorID:     record.CreatedBy,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return CapabilityPolicyRecord{}, RuntimeEventRecord{}, err
	}
	return record, event, nil
}

func capabilityPolicyPromptContextSurface(surface string) string {
	if surface = strings.TrimSpace(surface); surface != "" {
		return surface
	}
	return "workspace.policy.put"
}

func capabilityPolicyOwnership(capability string) map[string]any {
	capability = strings.ToLower(strings.TrimSpace(capability))
	ownership := map[string]any{
		"actuator_owner": "OPERATOR",
		"rrp":            controlOwnershipRoleNone,
		"rmp":            controlOwnershipRoleNone,
		"rsp":            controlOwnershipRoleNone,
		"apply_mode":     "capability_policy_write",
		"summary":        "Operator-owned capability policy write outside the control-command actuator families.",
	}
	if strings.HasPrefix(capability, "rsp.") {
		ownership["actuator_owner"] = "RSP"
		ownership["rsp"] = controlOwnershipRoleActuatorOwner
		ownership["summary"] = "RSP owns capability-flag policy for the statistical layer; RRP and RMP stay non-authoritative here."
	}
	return ownership
}

func (s *Store) ListCapabilityPolicies(ctx context.Context, filter CapabilityPolicyFilter) ([]CapabilityPolicyRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	query := strings.Builder{}
	query.WriteString(`SELECT policy_id, workspace_id, subject_type, subject_id, capability, tool_id, effect, reason, created_by, created_at, updated_at
	   FROM workspace_capability_policies
	  WHERE workspace_id = ?`)
	args := []any{workspaceID}
	if trimmed := strings.TrimSpace(filter.SubjectType); trimmed != "" {
		query.WriteString(` AND subject_type = ?`)
		args = append(args, normalizePolicyField(trimmed, false))
	}
	if trimmed := strings.TrimSpace(filter.SubjectID); trimmed != "" {
		query.WriteString(` AND subject_id = ?`)
		args = append(args, normalizePolicyField(trimmed, true))
	}
	if trimmed := strings.TrimSpace(filter.Capability); trimmed != "" {
		query.WriteString(` AND capability = ?`)
		args = append(args, normalizePolicyField(trimmed, true))
	}
	if trimmed := strings.TrimSpace(filter.ToolID); trimmed != "" {
		query.WriteString(` AND tool_id = ?`)
		args = append(args, normalizePolicyField(trimmed, true))
	}
	query.WriteString(` ORDER BY subject_type, subject_id, capability, tool_id, updated_at DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query capability policies: %w", err)
	}
	defer rows.Close()

	out := []CapabilityPolicyRecord{}
	for rows.Next() {
		var record CapabilityPolicyRecord
		if err := rows.Scan(
			&record.PolicyID,
			&record.WorkspaceID,
			&record.SubjectType,
			&record.SubjectID,
			&record.Capability,
			&record.ToolID,
			&record.Effect,
			&record.Reason,
			&record.CreatedBy,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan capability policy: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate capability policies: %w", err)
	}
	return out, nil
}

func (s *Store) CheckCapabilityPolicy(ctx context.Context, input CapabilityCheckInput) (CapabilityCheckResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return CapabilityCheckResult{}, errors.New("workspace_id is required")
	}
	subjectType := normalizePolicyField(input.SubjectType, false)
	if subjectType == "" {
		return CapabilityCheckResult{}, errors.New("subject_type is required")
	}
	subjectID := normalizePolicyField(input.SubjectID, true)
	if subjectID == "" {
		return CapabilityCheckResult{}, errors.New("subject_id is required")
	}
	capability := normalizePolicyField(input.Capability, true)
	if capability == "" {
		return CapabilityCheckResult{}, errors.New("capability is required")
	}
	toolID := normalizePolicyField(input.ToolID, true)
	if toolID == "" {
		toolID = "*"
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT policy_id, workspace_id, subject_type, subject_id, capability, tool_id, effect, reason, created_by, created_at, updated_at
		   FROM workspace_capability_policies
		  WHERE workspace_id = ?
		    AND (subject_type = ? OR subject_type = '*')
		    AND (subject_id = ? OR subject_id = '*')
		    AND (capability = ? OR capability = '*')
		    AND (tool_id = ? OR tool_id = '*')`,
		workspaceID,
		subjectType,
		subjectID,
		capability,
		toolID,
	)
	if err != nil {
		return CapabilityCheckResult{}, fmt.Errorf("query capability policies for check: %w", err)
	}
	defer rows.Close()

	candidates := []CapabilityPolicyRecord{}
	for rows.Next() {
		var record CapabilityPolicyRecord
		if err := rows.Scan(
			&record.PolicyID,
			&record.WorkspaceID,
			&record.SubjectType,
			&record.SubjectID,
			&record.Capability,
			&record.ToolID,
			&record.Effect,
			&record.Reason,
			&record.CreatedBy,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return CapabilityCheckResult{}, fmt.Errorf("scan capability policy for check: %w", err)
		}
		candidates = append(candidates, record)
	}
	if err := rows.Err(); err != nil {
		return CapabilityCheckResult{}, fmt.Errorf("iterate capability policies for check: %w", err)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		left := capabilityPolicySpecificity(candidates[i], subjectType, subjectID, capability, toolID)
		right := capabilityPolicySpecificity(candidates[j], subjectType, subjectID, capability, toolID)
		if left != right {
			return left > right
		}
		leftEffect := capabilityPolicyEffectStrength(candidates[i].Effect)
		rightEffect := capabilityPolicyEffectStrength(candidates[j].Effect)
		if leftEffect != rightEffect {
			return leftEffect > rightEffect
		}
		return candidates[i].UpdatedAt > candidates[j].UpdatedAt
	})

	result := CapabilityCheckResult{
		WorkspaceID:     workspaceID,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		Capability:      capability,
		ToolID:          toolID,
		Verdict:         "ALLOW",
		MatchedPolicies: candidates,
	}
	if len(candidates) > 0 {
		best := candidates[0]
		result.MatchedPolicy = &best
		result.Verdict = best.Effect
	}
	return result, nil
}

func normalizeCapabilityPolicyInput(input CapabilityPolicyInput) (CapabilityPolicyRecord, error) {
	record := CapabilityPolicyRecord{
		PolicyID:    strings.TrimSpace(input.PolicyID),
		WorkspaceID: strings.TrimSpace(input.WorkspaceID),
		SubjectType: normalizePolicyField(input.SubjectType, false),
		SubjectID:   normalizePolicyField(input.SubjectID, true),
		Capability:  normalizePolicyField(input.Capability, true),
		ToolID:      normalizePolicyField(input.ToolID, true),
		Effect:      normalizeCapabilityPolicyEffect(input.Effect),
		Reason:      strings.TrimSpace(input.Reason),
		CreatedBy:   strings.TrimSpace(input.CreatedBy),
	}
	if record.WorkspaceID == "" {
		return CapabilityPolicyRecord{}, errors.New("workspace_id is required")
	}
	if record.SubjectType == "" {
		return CapabilityPolicyRecord{}, errors.New("subject_type is required")
	}
	if record.SubjectID == "" {
		record.SubjectID = "*"
	}
	if record.Capability == "" {
		record.Capability = "*"
	}
	if record.ToolID == "" {
		record.ToolID = "*"
	}
	if record.CreatedBy == "" {
		return CapabilityPolicyRecord{}, errors.New("created_by is required")
	}
	return record, nil
}

func normalizePolicyField(value string, allowWildcard bool) string {
	value = strings.TrimSpace(value)
	if value == "" {
		if allowWildcard {
			return "*"
		}
		return ""
	}
	if value == "*" {
		return value
	}
	return strings.ToLower(value)
}

func normalizeCapabilityPolicyEffect(effect string) string {
	switch strings.ToUpper(strings.TrimSpace(effect)) {
	case "DENY":
		return "DENY"
	case "REQUIRE_APPROVAL":
		return "REQUIRE_APPROVAL"
	default:
		return "ALLOW"
	}
}

func capabilityPolicySpecificity(record CapabilityPolicyRecord, subjectType, subjectID, capability, toolID string) int {
	score := 0
	if record.SubjectType == subjectType {
		score += 16
	}
	if record.SubjectID == subjectID {
		score += 8
	}
	if record.Capability == capability {
		score += 4
	}
	if record.ToolID == toolID {
		score += 2
	}
	return score
}

func capabilityPolicyEffectStrength(effect string) int {
	switch normalizeCapabilityPolicyEffect(effect) {
	case "DENY":
		return 3
	case "REQUIRE_APPROVAL":
		return 2
	default:
		return 1
	}
}

func (s *Store) lookupCapabilityPolicyIDByMatch(ctx context.Context, workspaceID, subjectType, subjectID, capability, toolID string) (string, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT policy_id
		   FROM workspace_capability_policies
		  WHERE workspace_id = ? AND subject_type = ? AND subject_id = ? AND capability = ? AND tool_id = ?`,
		workspaceID,
		subjectType,
		subjectID,
		capability,
		toolID,
	)
	var policyID string
	if err := row.Scan(&policyID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return policyID, nil
}

func (s *Store) lookupCapabilityPolicyIDByMatchTx(ctx context.Context, tx *sql.Tx, workspaceID, subjectType, subjectID, capability, toolID string) (string, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT policy_id
		   FROM workspace_capability_policies
		  WHERE workspace_id = ? AND subject_type = ? AND subject_id = ? AND capability = ? AND tool_id = ?`,
		workspaceID,
		subjectType,
		subjectID,
		capability,
		toolID,
	)
	var policyID string
	if err := row.Scan(&policyID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return policyID, nil
}

func (s *Store) getCapabilityPolicyByMatch(ctx context.Context, workspaceID, subjectType, subjectID, capability, toolID string) (CapabilityPolicyRecord, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT policy_id, workspace_id, subject_type, subject_id, capability, tool_id, effect, reason, created_by, created_at, updated_at
		   FROM workspace_capability_policies
		  WHERE workspace_id = ? AND subject_type = ? AND subject_id = ? AND capability = ? AND tool_id = ?`,
		workspaceID,
		subjectType,
		subjectID,
		capability,
		toolID,
	)
	var record CapabilityPolicyRecord
	if err := row.Scan(
		&record.PolicyID,
		&record.WorkspaceID,
		&record.SubjectType,
		&record.SubjectID,
		&record.Capability,
		&record.ToolID,
		&record.Effect,
		&record.Reason,
		&record.CreatedBy,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CapabilityPolicyRecord{}, fmt.Errorf("capability policy not found: %s/%s/%s/%s", workspaceID, subjectType, subjectID, capability)
		}
		return CapabilityPolicyRecord{}, err
	}
	return record, nil
}
