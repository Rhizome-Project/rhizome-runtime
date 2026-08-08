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

type RuntimeReplayFilter struct {
	WorkspaceID      string `json:"workspace_id"`
	AgentID          string `json:"agent_id,omitempty"`
	SessionID        string `json:"session_id,omitempty"`
	TaskID           string `json:"task_id,omitempty"`
	ExcludeSynthetic bool   `json:"exclude_synthetic,omitempty"`
	Limit            int    `json:"limit"`
}

func normalizeRuntimeReplayFilter(filter RuntimeReplayFilter) RuntimeReplayFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.Limit = clampReadSurfaceLimit(filter.Limit, readSurfaceReplayLimitDefault, readSurfaceReplayLimitMax)
	return filter
}

type RuntimeReplaySession struct {
	SessionID          string                        `json:"session_id"`
	WorkspaceID        string                        `json:"workspace_id"`
	AgentID            string                        `json:"agent_id,omitempty"`
	TaskID             string                        `json:"task_id,omitempty"`
	Status             string                        `json:"status"`
	Summary            string                        `json:"summary,omitempty"`
	OwnerScope         string                        `json:"owner_scope,omitempty"`
	BlockedOn          []model.AgentUpdateBlockedRef `json:"blocked_on,omitempty"`
	DecisionNeededFrom string                        `json:"decision_needed_from,omitempty"`
	DecisionType       string                        `json:"decision_type,omitempty"`
	KeepSessionActive  *bool                         `json:"keep_session_active,omitempty"`
	HandoffTo          string                        `json:"handoff_to,omitempty"`
	RelatedDocKeys     []string                      `json:"related_doc_keys,omitempty"`
	LastEventType      string                        `json:"last_event_type,omitempty"`
	EventCount         int                           `json:"event_count"`
	StartedAt          string                        `json:"started_at,omitempty"`
	UpdatedAt          string                        `json:"updated_at"`
	CompletedAt        string                        `json:"completed_at,omitempty"`
}

type RuntimeReplayQueue struct {
	QueueID           string  `json:"queue_id"`
	WorkspaceID       string  `json:"workspace_id"`
	QueueKey          string  `json:"queue_key,omitempty"`
	QueueType         string  `json:"queue_type,omitempty"`
	Status            string  `json:"status"`
	Title             string  `json:"title,omitempty"`
	Summary           string  `json:"summary,omitempty"`
	AssignedTo        string  `json:"assigned_to,omitempty"`
	Urgency           string  `json:"urgency,omitempty"`
	SourceKind        string  `json:"source_kind,omitempty"`
	SourceID          string  `json:"source_id,omitempty"`
	AgentID           string  `json:"agent_id,omitempty"`
	SessionID         string  `json:"session_id,omitempty"`
	TaskID            string  `json:"task_id,omitempty"`
	KeepSessionActive bool    `json:"keep_session_active"`
	Resolution        string  `json:"resolution,omitempty"`
	ResolvedBy        string  `json:"resolved_by,omitempty"`
	DueAt             *string `json:"due_at,omitempty"`
	EscalationCount   int     `json:"escalation_count"`
	LastEscalatedAt   *string `json:"last_escalated_at,omitempty"`
	LastEscalatedBy   string  `json:"last_escalated_by,omitempty"`
	EscalationReason  string  `json:"escalation_reason,omitempty"`
	LastEventType     string  `json:"last_event_type,omitempty"`
	EventCount        int     `json:"event_count"`
	CreatedAt         string  `json:"created_at,omitempty"`
	UpdatedAt         string  `json:"updated_at"`
	ResolvedAt        *string `json:"resolved_at,omitempty"`
}

type RuntimeReplayClaim struct {
	ClaimID             string  `json:"claim_id"`
	WorkspaceID         string  `json:"workspace_id"`
	ClaimType           string  `json:"claim_type,omitempty"`
	Status              string  `json:"status"`
	Subject             string  `json:"subject,omitempty"`
	Summary             string  `json:"summary,omitempty"`
	SourceKind          string  `json:"source_kind,omitempty"`
	SourceID            string  `json:"source_id,omitempty"`
	MemoryID            string  `json:"memory_id,omitempty"`
	TaskID              string  `json:"task_id,omitempty"`
	SessionID           string  `json:"session_id,omitempty"`
	AgentID             string  `json:"agent_id,omitempty"`
	SupersedesClaimID   string  `json:"supersedes_claim_id,omitempty"`
	SupersededByClaimID string  `json:"superseded_by_claim_id,omitempty"`
	ConflictsClaimID    string  `json:"conflicts_claim_id,omitempty"`
	LifecycleReason     string  `json:"lifecycle_reason,omitempty"`
	ReviewDueAt         *string `json:"review_due_at,omitempty"`
	ReviewedAt          *string `json:"reviewed_at,omitempty"`
	ReviewedBy          string  `json:"reviewed_by,omitempty"`
	Confidence          float64 `json:"confidence,omitempty"`
	LastEventType       string  `json:"last_event_type,omitempty"`
	EventCount          int     `json:"event_count"`
	CreatedAt           string  `json:"created_at,omitempty"`
	UpdatedAt           string  `json:"updated_at"`
	ArchivedAt          *string `json:"archived_at,omitempty"`
	ArchivedBy          string  `json:"archived_by,omitempty"`
}

type RuntimeReplayWorkspaceMemory struct {
	MemoryID       string  `json:"memory_id"`
	WorkspaceID    string  `json:"workspace_id"`
	MemoryType     string  `json:"memory_type,omitempty"`
	SourceKind     string  `json:"source_kind,omitempty"`
	SourceID       string  `json:"source_id,omitempty"`
	TaskID         string  `json:"task_id,omitempty"`
	SessionID      string  `json:"session_id,omitempty"`
	AgentID        string  `json:"agent_id,omitempty"`
	RecoveryReason string  `json:"recovery_reason,omitempty"`
	LastEventType  string  `json:"last_event_type,omitempty"`
	EventCount     int     `json:"event_count"`
	CreatedAt      string  `json:"created_at,omitempty"`
	UpdatedAt      string  `json:"updated_at"`
	ArchivedAt     *string `json:"archived_at,omitempty"`
	ArchivedBy     string  `json:"archived_by,omitempty"`
	ArchivedReason string  `json:"archived_reason,omitempty"`
}

type RuntimeReplayExecutionRun struct {
	RunID            string         `json:"run_id"`
	WorkspaceID      string         `json:"workspace_id"`
	TaskID           string         `json:"task_id,omitempty"`
	SessionID        string         `json:"session_id,omitempty"`
	AgentID          string         `json:"agent_id,omitempty"`
	Title            string         `json:"title,omitempty"`
	Summary          string         `json:"summary,omitempty"`
	Status           string         `json:"status"`
	Outcome          string         `json:"outcome,omitempty"`
	PhaseCounts      map[string]int `json:"phase_counts,omitempty"`
	StepStatusCounts map[string]int `json:"step_status_counts,omitempty"`
	LastEventType    string         `json:"last_event_type,omitempty"`
	EventCount       int            `json:"event_count"`
	RunEventCount    int            `json:"run_event_count"`
	StepEventCount   int            `json:"step_event_count"`
	CreatedAt        string         `json:"created_at,omitempty"`
	UpdatedAt        string         `json:"updated_at"`
	ClosedAt         *string        `json:"closed_at,omitempty"`
}

type RuntimeReplayMetrics struct {
	TotalEvents               int            `json:"total_events"`
	AppliedEvents             int            `json:"applied_events,omitempty"`
	SuppressedDuplicateEvents int            `json:"suppressed_duplicate_events,omitempty"`
	ConflictingDuplicateKeys  int            `json:"conflicting_duplicate_keys,omitempty"`
	EventTypeCounts           map[string]int `json:"event_type_counts,omitempty"`
	EntityTypeCounts          map[string]int `json:"entity_type_counts,omitempty"`
	ActiveSessionCount        int            `json:"active_session_count"`
	OpenQueueCount            int            `json:"open_queue_count"`
	OverdueQueueCount         int            `json:"overdue_queue_count"`
	ActiveClaimCount          int            `json:"active_claim_count"`
	OpenExecutionRuns         int            `json:"open_execution_runs"`
}

type RuntimeReplayFinding struct {
	Code                    string `json:"code"`
	Severity                string `json:"severity"`
	Message                 string `json:"message"`
	EntityType              string `json:"entity_type,omitempty"`
	EntityID                string `json:"entity_id,omitempty"`
	SourceEventType         string `json:"source_event_type,omitempty"`
	SourceEventID           string `json:"source_event_id,omitempty"`
	SourceDedupKey          string `json:"source_dedup_key,omitempty"`
	SourceRootCauseID       string `json:"source_root_cause_id,omitempty"`
	SourceProvenanceGroupID string `json:"source_provenance_group_id,omitempty"`
	SourceParentRefsJSON    string `json:"source_parent_refs_json,omitempty"`
}

type RuntimeReplayEvaluation struct {
	Verdict           string                         `json:"verdict"`
	ErrorCount        int                            `json:"error_count"`
	WarningCount      int                            `json:"warning_count"`
	RetentionRisk     RuntimeReplayRetentionRisk     `json:"retention_risk,omitempty"`
	FindingSummary    RuntimeReplayFindingSummary    `json:"finding_summary"`
	ProvenanceSummary RuntimeReplayProvenanceSummary `json:"provenance_summary"`
	Findings          []RuntimeReplayFinding         `json:"findings,omitempty"`
}

type RuntimeReplayFindingSummary struct {
	TotalFindings                            int `json:"total_findings"`
	ErrorFindingCount                        int `json:"error_finding_count"`
	WarningFindingCount                      int `json:"warning_finding_count"`
	InfoFindingCount                         int `json:"info_finding_count"`
	DedupConflictCount                       int `json:"dedup_conflict_count"`
	CausalOrderCount                         int `json:"causal_order_count"`
	MissingParentCount                       int `json:"missing_parent_count"`
	CycleCount                               int `json:"cycle_count"`
	CycleSelfParentCount                     int `json:"cycle_self_parent_count"`
	CycleParentComponentCount                int `json:"cycle_parent_component_count"`
	ScopePartialCount                        int `json:"scope_partial_count"`
	RetentionFindingCount                    int `json:"retention_finding_count"`
	RetentionCompactionCandidateCount        int `json:"retention_compaction_candidate_count"`
	RetentionCompactedSessionCount           int `json:"retention_compacted_session_count"`
	RetentionSnapshotWithoutEpisodePackCount int `json:"retention_snapshot_without_episode_pack_count"`
	ExecutionRunIntegrityCount               int `json:"execution_run_integrity_count"`
	MissingExecutionRunCount                 int `json:"missing_execution_run_count"`
	ExecutionRunOutOfSyncCount               int `json:"execution_run_out_of_sync_count"`
	ExecutionRunWithoutStepsCount            int `json:"execution_run_without_steps_count"`
	ClaimIntegrityCount                      int `json:"claim_integrity_count"`
	ClaimMissingMemoryLinkCount              int `json:"claim_missing_memory_link_count"`
	ClaimMissingReviewQueueCount             int `json:"claim_missing_review_queue_count"`
	StaleClaimReviewQueueCount               int `json:"stale_claim_review_queue_count"`
	SupersededClaimMissingLinkCount          int `json:"superseded_claim_missing_link_count"`
	DuplicateActiveMemoryClaimCount          int `json:"duplicate_active_memory_claim_count"`
	OperatorQueueIntegrityCount              int `json:"operator_queue_integrity_count"`
	OverdueOperatorQueueCount                int `json:"overdue_operator_queue_count"`
	OverdueClaimReviewUnescalatedCount       int `json:"overdue_claim_review_unescalated_count"`
	MissingOperatorQueueCount                int `json:"missing_operator_queue_count"`
	StaleOpenQueueCount                      int `json:"stale_open_queue_count"`
	PayloadIntegrityCount                    int `json:"payload_integrity_count"`
	MalformedEventPayloadCount               int `json:"malformed_event_payload_count"`
	OtherFindingCount                        int `json:"other_finding_count"`
}

type RuntimeReplayProvenanceSummary struct {
	TotalFindingsWithSourceEvent  int `json:"total_findings_with_source_event"`
	FindingsWithSourceDedupKey    int `json:"findings_with_source_dedup_key"`
	FindingsWithRootCauseID       int `json:"findings_with_root_cause_id"`
	FindingsWithProvenanceGroupID int `json:"findings_with_provenance_group_id"`
	FindingsWithParentRefs        int `json:"findings_with_parent_refs"`
	FullLineageFieldFindingCount  int `json:"full_lineage_field_finding_count"`
}

type RuntimeReplayRetentionRisk struct {
	Band                     string   `json:"band,omitempty"`
	CompactionCandidateCount int      `json:"compaction_candidate_count"`
	CompactionSnapshotCount  int      `json:"compaction_snapshot_count"`
	EpisodePackCount         int      `json:"episode_pack_count"`
	LatestSnapshotAt         string   `json:"latest_snapshot_at,omitempty"`
	CandidateSessionIDs      []string `json:"candidate_session_ids,omitempty"`
	SnapshotSessionIDs       []string `json:"snapshot_session_ids,omitempty"`
	Reasons                  []string `json:"reasons,omitempty"`
}

type RuntimeReplayScopeAssessment struct {
	Authoritative         bool     `json:"authoritative"`
	IntegrityBand         string   `json:"integrity_band"`
	Reasons               []string `json:"reasons,omitempty"`
	SuppressedConclusions []string `json:"suppressed_conclusions,omitempty"`
}

type RuntimeReplayReplicaCoverage struct {
	ReplicaCount        int   `json:"replica_count"`
	LaggingReplicaCount int   `json:"lagging_replica_count"`
	RetryPendingCount   int   `json:"retry_pending_count"`
	DeadLetterCount     int   `json:"dead_letter_count"`
	MaxExportLag        int64 `json:"max_export_lag"`
	MaxFetchLag         int64 `json:"max_fetch_lag"`
	MaxAckLag           int64 `json:"max_ack_lag"`
	MaxApplyLag         int64 `json:"max_apply_lag"`
}

type RuntimeReplayReport struct {
	WorkspaceID           string                                `json:"workspace_id"`
	TimeAuthority         WorkspaceTimeAuthority                `json:"time_authority"`
	Filter                RuntimeReplayFilter                   `json:"filter"`
	Truncated             bool                                  `json:"truncated"`
	WindowIncomplete      bool                                  `json:"window_incomplete"`
	MissingParentRefs     []string                              `json:"missing_parent_refs,omitempty"`
	Scope                 RuntimeReplayScopeAssessment          `json:"scope"`
	ReplicaCoverage       RuntimeReplayReplicaCoverage          `json:"replica_coverage"`
	ReplicaFreshness      []WorkspaceReplicaFreshnessRecord     `json:"replica_freshness,omitempty"`
	LocalReplicaFreshness *WorkspaceReplicaFreshnessRecord      `json:"local_replica_freshness,omitempty"`
	EventsOrder           string                                `json:"events_order,omitempty"`
	Events                []RuntimeEventRecord                  `json:"events,omitempty"`
	AppliedOrder          string                                `json:"applied_order,omitempty"`
	AppliedEventIDs       []string                              `json:"applied_event_ids,omitempty"`
	Sessions              []RuntimeReplaySession                `json:"sessions,omitempty"`
	Queues                []RuntimeReplayQueue                  `json:"queues,omitempty"`
	Claims                []RuntimeReplayClaim                  `json:"claims,omitempty"`
	WorkspaceMemory       []RuntimeReplayWorkspaceMemory        `json:"workspace_memory,omitempty"`
	ExecutionRuns         []RuntimeReplayExecutionRun           `json:"execution_runs,omitempty"`
	CapabilityPolicies    []CapabilityPolicyRecord              `json:"capability_policies,omitempty"`
	ControlCommands       []ControlCommandRecord                `json:"control_commands,omitempty"`
	EffectiveControls     []EffectiveControlsRecord             `json:"effective_controls,omitempty"`
	UnifiedSnapshots      []RuntimeReplayUnifiedControlSnapshot `json:"unified_control_snapshots,omitempty"`
	MemoryInvalidations   []RuntimeReplayMemoryInvalidation     `json:"memory_invalidations,omitempty"`
	Metrics               RuntimeReplayMetrics                  `json:"metrics"`
	Evaluation            RuntimeReplayEvaluation               `json:"evaluation"`
	readSurfacePolicy     ReadSurfacePolicy                     `json:"-"`
}

type RuntimeReplayUnifiedControlSnapshot struct {
	SnapshotKey            string                                `json:"snapshot_key"`
	WorkspaceID            string                                `json:"workspace_id"`
	EntityID               string                                `json:"entity_id"`
	EventType              string                                `json:"event_type"`
	TypedEventType         string                                `json:"typed_event_type,omitempty"`
	AdvisoryOnly           bool                                  `json:"advisory_only"`
	Resolved               bool                                  `json:"resolved"`
	ResolvedFrom           string                                `json:"resolved_from,omitempty"`
	Summary                string                                `json:"summary,omitempty"`
	ProtoClusterID         string                                `json:"proto_cluster_id,omitempty"`
	SessionID              string                                `json:"session_id,omitempty"`
	TaskID                 string                                `json:"task_id,omitempty"`
	Filter                 UnifiedControlReportFilter            `json:"filter"`
	ControlMode            string                                `json:"control_mode,omitempty"`
	CandidateMode          string                                `json:"candidate_mode,omitempty"`
	AttentionBand          string                                `json:"attention_band,omitempty"`
	MemoryNeedsAttention   bool                                  `json:"memory_needs_attention"`
	MemoryAttentionReasons []string                              `json:"memory_attention_reasons,omitempty"`
	CandidateControls      ControlSuggestedControls              `json:"candidate_controls"`
	AdvisoryControls       ControlSuggestedControls              `json:"advisory_controls"`
	EffectiveControls      ControlSuggestedControls              `json:"effective_controls"`
	EffectiveControlsAudit *UnifiedControlEffectiveControlsAudit `json:"effective_controls_audit,omitempty"`
	AppliedActions         []string                              `json:"applied_actions,omitempty"`
	AppliedActionAudit     []UnifiedControlAppliedActionAudit    `json:"applied_action_audit,omitempty"`
	SuppressedHints        []string                              `json:"suppressed_hints,omitempty"`
	SuppressedHintAudit    []UnifiedControlSuppressedHintAudit   `json:"suppressed_hint_audit,omitempty"`
	AuditSummary           *UnifiedControlAuditSummary           `json:"audit_summary,omitempty"`
	AuditCoverage          *UnifiedControlAuditCoverage          `json:"audit_coverage,omitempty"`
	GovernedHintOutcomes   []UnifiedControlGovernedHintOutcome   `json:"governed_hint_outcomes,omitempty"`
	Contradictions         []string                              `json:"contradictions,omitempty"`
	CooldownBasis          *UnifiedControlCooldownBasis          `json:"cooldown_basis,omitempty"`
	GeneratedAt            string                                `json:"generated_at,omitempty"`
	UpdatedAt              string                                `json:"updated_at"`
	LastEventID            string                                `json:"last_event_id,omitempty"`
}

type RuntimeReplayMemoryInvalidation struct {
	InvalidationID              string                        `json:"invalidation_id"`
	WorkspaceID                 string                        `json:"workspace_id"`
	AgentID                     string                        `json:"agent_id,omitempty"`
	SessionID                   string                        `json:"session_id,omitempty"`
	ReportScope                 string                        `json:"report_scope,omitempty"`
	ReportID                    string                        `json:"report_id,omitempty"`
	ResidencyTier               string                        `json:"residency_tier,omitempty"`
	ReplicaKind                 string                        `json:"replica_kind,omitempty"`
	CoherenceClass              string                        `json:"coherence_class,omitempty"`
	CanonicalMemoryID           string                        `json:"canonical_memory_id,omitempty"`
	CacheKey                    string                        `json:"cache_key,omitempty"`
	RefKind                     string                        `json:"ref_kind,omitempty"`
	RefID                       string                        `json:"ref_id,omitempty"`
	PreviousVersionToken        string                        `json:"previous_version_token,omitempty"`
	CurrentVersionToken         string                        `json:"current_version_token,omitempty"`
	DependencyRevisionVector    []MemoryResidencyVersionGuard `json:"dependency_revision_vector,omitempty"`
	DependencyVectorMalformed   bool                          `json:"dependency_revision_vector_malformed,omitempty"`
	Reason                      string                        `json:"reason,omitempty"`
	TriggerCause                string                        `json:"trigger_cause,omitempty"`
	RecoveredFromInvalidationID string                        `json:"recovered_from_invalidation_id,omitempty"`
	RecoveredAt                 string                        `json:"recovered_at,omitempty"`
	RecoveryCause               string                        `json:"recovery_cause,omitempty"`
	State                       string                        `json:"state,omitempty"`
	DeliveredAt                 string                        `json:"delivered_at,omitempty"`
	AcknowledgedAt              string                        `json:"acknowledged_at,omitempty"`
	DeadLetteredAt              string                        `json:"dead_lettered_at,omitempty"`
	DeliveryAttemptCount        int                           `json:"delivery_attempt_count"`
	LastDeliveryAttemptAt       string                        `json:"last_delivery_attempt_at,omitempty"`
	LeaseExpiresAt              string                        `json:"lease_expires_at,omitempty"`
	NextDeliveryAt              string                        `json:"next_delivery_at,omitempty"`
	FailureCount                int                           `json:"failure_count"`
	LastFailureAt               string                        `json:"last_failure_at,omitempty"`
	LastFailureReason           string                        `json:"last_failure_reason,omitempty"`
	TypedEventType              string                        `json:"typed_event_type,omitempty"`
	LastEventType               string                        `json:"last_event_type,omitempty"`
	CreatedAt                   string                        `json:"created_at,omitempty"`
	UpdatedAt                   string                        `json:"updated_at"`
	LastEventID                 string                        `json:"last_event_id,omitempty"`
}

type replayOperatorQueuePayload struct {
	QueueID           string `json:"queue_id"`
	QueueKey          string `json:"queue_key"`
	QueueType         string `json:"queue_type"`
	Status            string `json:"status"`
	Title             string `json:"title"`
	Summary           string `json:"summary"`
	AssignedTo        string `json:"assigned_to"`
	Urgency           string `json:"urgency"`
	SourceKind        string `json:"source_kind"`
	SourceID          string `json:"source_id"`
	KeepSessionActive bool   `json:"keep_session_active"`
	DueAt             string `json:"due_at"`
	Resolution        string `json:"resolution"`
	ResolvedBy        string `json:"resolved_by"`
	EscalationCount   int    `json:"escalation_count"`
	LastEscalatedAt   string `json:"last_escalated_at"`
	LastEscalatedBy   string `json:"last_escalated_by"`
	EscalationReason  string `json:"escalation_reason"`
}

type replayKnowledgeClaimPayload struct {
	ClaimType           string  `json:"claim_type"`
	Status              string  `json:"status"`
	Subject             string  `json:"subject"`
	Summary             string  `json:"summary"`
	SourceKind          string  `json:"source_kind"`
	SourceID            string  `json:"source_id"`
	MemoryID            string  `json:"memory_id"`
	SupersedesClaimID   string  `json:"supersedes_claim_id"`
	SupersededByClaimID string  `json:"superseded_by_claim_id"`
	ConflictsClaimID    string  `json:"conflicts_claim_id"`
	LifecycleReason     string  `json:"lifecycle_reason"`
	ReviewDueAt         string  `json:"review_due_at"`
	ReviewedAt          string  `json:"reviewed_at"`
	ReviewedBy          string  `json:"reviewed_by"`
	Confidence          float64 `json:"confidence"`
	ArchivedBy          string  `json:"archived_by"`
}

type replayWorkspaceMemoryPayload struct {
	MemoryType     string `json:"memory_type"`
	SourceKind     string `json:"source_kind"`
	SourceID       string `json:"source_id"`
	Reason         string `json:"reason"`
	ArchivedBy     string `json:"archived_by"`
	RestoredBy     string `json:"restored_by"`
	RecoveryReason string `json:"recovery_reason"`
}

type replayExecutionRunPayload struct {
	Status    string `json:"status"`
	Outcome   string `json:"outcome"`
	Title     string `json:"title"`
	Summary   string `json:"summary"`
	AgentID   string `json:"agent_id"`
	SessionID string `json:"session_id"`
	TaskID    string `json:"task_id"`
}

type replayExecutionStepPayload struct {
	RunID     string `json:"run_id"`
	Phase     string `json:"phase"`
	Status    string `json:"status"`
	Title     string `json:"title"`
	Summary   string `json:"summary"`
	SortOrder int    `json:"sort_order"`
}

type replayCapabilityPolicyPayload struct {
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	Capability  string `json:"capability"`
	ToolID      string `json:"tool_id"`
	Effect      string `json:"effect"`
	Reason      string `json:"reason"`
	CreatedBy   string `json:"created_by"`
}

type replayControlCommandPayload struct {
	CommandType    string   `json:"command_type"`
	Scope          string   `json:"scope"`
	ProtoClusterID string   `json:"proto_cluster_id"`
	TensionID      string   `json:"tension_id"`
	AgentID        string   `json:"agent_id"`
	TargetMode     string   `json:"target_mode"`
	TTLSeconds     int      `json:"ttl_seconds"`
	Reason         string   `json:"reason"`
	RequestedBy    string   `json:"requested_by"`
	ActorType      string   `json:"actor_type"`
	RequestedAt    string   `json:"requested_at"`
	AppliedInline  bool     `json:"applied_inline"`
	ExpiresAt      string   `json:"expires_at"`
	ParentRefs     []string `json:"parent_refs"`
}

type replayEffectiveControlsPayload struct {
	WorkspaceID       string                   `json:"workspace_id"`
	ProtoClusterID    string                   `json:"proto_cluster_id"`
	Epoch             int                      `json:"epoch"`
	TTLSeconds        int                      `json:"ttl_seconds"`
	ExpiresAt         string                   `json:"expires_at"`
	ControlMode       string                   `json:"control_mode"`
	CandidateMode     string                   `json:"candidate_mode"`
	CandidateControls ControlSuggestedControls `json:"candidate_controls"`
	AdvisoryControls  ControlSuggestedControls `json:"advisory_controls"`
	EffectiveControls ControlSuggestedControls `json:"effective_controls"`
	ResolvedFrom      string                   `json:"resolved_from"`
	MatchScore        int                      `json:"match_score"`
	BasisSummary      string                   `json:"basis_summary"`
	GeneratedAt       string                   `json:"generated_at"`
	ActorID           string                   `json:"actor_id"`
	CreatedAt         string                   `json:"created_at"`
	UpdatedAt         string                   `json:"updated_at"`
}

type replayUnifiedControlSnapshotPayload struct {
	WorkspaceID                  string                     `json:"workspace_id"`
	Filter                       UnifiedControlReportFilter `json:"filter"`
	Report                       UnifiedControlReport       `json:"report"`
	Summary                      string                     `json:"summary"`
	TypedEventType               string                     `json:"typed_event_type"`
	EventKind                    string                     `json:"event_kind"`
	Resolved                     bool                       `json:"resolved"`
	ResolvedFrom                 string                     `json:"resolved_from"`
	AdvisoryOnly                 bool                       `json:"advisory_only"`
	EffectiveControlsFound       bool                       `json:"effective_controls_found"`
	EffectiveControlsLive        bool                       `json:"effective_controls_live"`
	EffectiveControlsExpired     bool                       `json:"effective_controls_expired"`
	EffectiveControlsPending     bool                       `json:"effective_controls_pending"`
	EffectiveControlsScopeSource string                     `json:"effective_controls_scope_source"`
}

type replayMemoryInvalidationPayload struct {
	InvalidationID              string                        `json:"invalidation_id"`
	AgentID                     string                        `json:"agent_id"`
	SessionID                   string                        `json:"session_id"`
	ReportScope                 string                        `json:"report_scope"`
	ReportID                    string                        `json:"report_id"`
	ResidencyTier               string                        `json:"residency_tier"`
	ReplicaKind                 string                        `json:"replica_kind"`
	CoherenceClass              string                        `json:"coherence_class"`
	CanonicalMemoryID           string                        `json:"canonical_memory_id"`
	CacheKey                    string                        `json:"cache_key"`
	RefKind                     string                        `json:"ref_kind"`
	RefID                       string                        `json:"ref_id"`
	PreviousVersionToken        string                        `json:"previous_version_token"`
	CurrentVersionToken         string                        `json:"current_version_token"`
	DependencyRevisionVector    []MemoryResidencyVersionGuard `json:"dependency_revision_vector"`
	DependencyVectorMalformed   bool                          `json:"dependency_revision_vector_malformed"`
	Reason                      string                        `json:"reason"`
	TriggerCause                string                        `json:"trigger_cause"`
	RecoveredFromInvalidationID string                        `json:"recovered_from_invalidation_id"`
	RecoveredAt                 string                        `json:"recovered_at"`
	RecoveryCause               string                        `json:"recovery_cause"`
	State                       string                        `json:"state"`
	DeliveryAttemptCount        int                           `json:"delivery_attempt_count"`
	LastDeliveryAttemptAt       string                        `json:"last_delivery_attempt_at"`
	LeaseExpiresAt              string                        `json:"lease_expires_at"`
	NextDeliveryAt              string                        `json:"next_delivery_at"`
	FailureCount                int                           `json:"failure_count"`
	LastFailureAt               string                        `json:"last_failure_at"`
	LastFailureReason           string                        `json:"last_failure_reason"`
	TypedEventType              string                        `json:"typed_event_type"`
}

type runtimeReplayDedupState struct {
	firstEvent RuntimeEventRecord
	conflicted bool
}

type runtimeReplayFindingSource struct {
	EventType         string
	EventID           string
	DedupKey          string
	RootCauseID       string
	ProvenanceGroupID string
	ParentRefsJSON    string
}

type runtimeReplayEvaluationSources struct {
	sessions         map[string]runtimeReplayFindingSource
	queues           map[string]runtimeReplayFindingSource
	claims           map[string]runtimeReplayFindingSource
	runs             map[string]runtimeReplayFindingSource
	memoryClaims     map[string]runtimeReplayFindingSource
	unifiedSnapshots map[string]runtimeReplayFindingSource
}

func (s *Store) ReplayRuntimeJournal(ctx context.Context, filter RuntimeReplayFilter) (RuntimeReplayReport, error) {
	filter = normalizeRuntimeReplayFilter(filter)
	if filter.WorkspaceID == "" {
		return RuntimeReplayReport{}, errors.New("workspace_id is required")
	}
	probeLimit := filter.Limit + 1
	events, err := s.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID:      filter.WorkspaceID,
		AgentID:          filter.AgentID,
		SessionID:        filter.SessionID,
		TaskID:           filter.TaskID,
		ExcludeSynthetic: filter.ExcludeSynthetic,
		Limit:            probeLimit,
	})
	if err != nil {
		return RuntimeReplayReport{}, fmt.Errorf("list runtime events for replay: %w", err)
	}
	truncated := len(events) > filter.Limit
	if truncated {
		events = append([]RuntimeEventRecord(nil), events[:filter.Limit]...)
	}

	report := RuntimeReplayReport{
		WorkspaceID:  filter.WorkspaceID,
		Filter:       filter,
		Events:       append([]RuntimeEventRecord(nil), events...),
		EventsOrder:  "latest_first_ingest",
		AppliedOrder: "causal_parent_before_child",
		Truncated:    truncated,
		Metrics: RuntimeReplayMetrics{
			EventTypeCounts:  map[string]int{},
			EntityTypeCounts: map[string]int{},
		},
		readSurfacePolicy: runtimeReplayReadSurfacePolicy(filter),
	}
	report.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return RuntimeReplayReport{}, err
	}
	chronological := append([]RuntimeEventRecord(nil), events...)
	for left, right := 0, len(chronological)-1; left < right; left, right = left+1, right-1 {
		chronological[left], chronological[right] = chronological[right], chronological[left]
	}
	_, cycleAffectedEvents := runtimeReplayCausalOrder(chronological)

	sessions := map[string]*RuntimeReplaySession{}
	queues := map[string]*RuntimeReplayQueue{}
	claims := map[string]*RuntimeReplayClaim{}
	memories := map[string]*RuntimeReplayWorkspaceMemory{}
	runs := map[string]*RuntimeReplayExecutionRun{}
	capabilityPolicies := map[string]*CapabilityPolicyRecord{}
	controlCommands := map[string]*ControlCommandRecord{}
	effectiveControls := map[string]*EffectiveControlsRecord{}
	unifiedSnapshots := map[string]*RuntimeReplayUnifiedControlSnapshot{}
	memoryInvalidations := map[string]*RuntimeReplayMemoryInvalidation{}
	findings := make([]RuntimeReplayFinding, 0, 8)
	dedupState := map[string]*runtimeReplayDedupState{}
	appliedEvents := make([]RuntimeEventRecord, 0, len(chronological))
	sources := runtimeReplayEvaluationSources{
		sessions:         map[string]runtimeReplayFindingSource{},
		queues:           map[string]runtimeReplayFindingSource{},
		claims:           map[string]runtimeReplayFindingSource{},
		runs:             map[string]runtimeReplayFindingSource{},
		memoryClaims:     map[string]runtimeReplayFindingSource{},
		unifiedSnapshots: map[string]runtimeReplayFindingSource{},
	}
	eventOrder := map[string]int{}
	for idx, event := range chronological {
		if eventID := strings.TrimSpace(event.EventID); eventID != "" {
			eventOrder[eventID] = idx
		}
	}

	for idx, event := range chronological {
		report.Metrics.TotalEvents++
		report.Metrics.EventTypeCounts[event.EventType]++
		report.Metrics.EntityTypeCounts[event.EntityType]++
		findings = append(findings, replayEventOrderingFindings(event, idx, eventOrder, cycleAffectedEvents)...)
		if !shouldApplyReplayEvent(event, dedupState, &report.Metrics, &findings) {
			continue
		}
		report.Metrics.AppliedEvents++
		appliedEvents = append(appliedEvents, event)
	}

	applyOrder, applyCycleAffected := runtimeReplayCausalOrder(appliedEvents)
	if len(applyCycleAffected) > 0 {
		report.AppliedOrder = "causal_parent_before_child_with_cycle_ingest_fallback"
	}
	report.AppliedEventIDs = runtimeReplayEventIDs(applyOrder)
	for _, event := range applyOrder {
		switch event.EntityType {
		case "agent_session":
			replayAgentSessionEvent(event, sessions, sources.sessions, &findings)
		case "operator_queue":
			replayOperatorQueueEvent(event, queues, sources.queues, &findings)
		case "knowledge_claim":
			replayKnowledgeClaimEvent(event, claims, sources.claims, &findings)
		case "workspace_memory":
			replayWorkspaceMemoryEvent(event, memories, &findings)
		case "execution_run":
			replayExecutionRunEvent(event, runs, sources.runs, &findings)
		case "execution_step":
			replayExecutionStepEvent(event, runs, sources.runs, &findings)
		case "capability_policy":
			replayCapabilityPolicyEvent(event, capabilityPolicies, &findings)
		case "control_command":
			replayControlCommandEvent(event, controlCommands, &findings)
		case effectiveControlsEntityType:
			replayEffectiveControlsEvent(event, effectiveControls, &findings)
		case "instrumentation_unified_control":
			replayUnifiedControlSnapshotEvent(event, unifiedSnapshots, sources.unifiedSnapshots, &findings)
		case "memory_invalidation":
			replayMemoryInvalidationEvent(event, memoryInvalidations, &findings)
		}
	}

	report.Sessions = replaySessionSlice(sessions)
	report.Queues = replayQueueSlice(queues)
	report.Claims = replayClaimSlice(claims)
	report.WorkspaceMemory = replayWorkspaceMemorySlice(memories)
	report.ExecutionRuns = replayExecutionRunSlice(runs)
	report.CapabilityPolicies = replayCapabilityPolicySlice(capabilityPolicies)
	report.ControlCommands = replayControlCommandSlice(controlCommands)
	referenceTime := runtimeReplayReferenceTime(report)
	report.EffectiveControls = replayEffectiveControlsSlice(effectiveControls, referenceTime)
	report.UnifiedSnapshots = replayUnifiedControlSnapshotSlice(unifiedSnapshots)
	report.MemoryInvalidations = replayMemoryInvalidationSlice(memoryInvalidations)
	report.ReplicaFreshness, err = s.ListWorkspaceReplicaFreshness(ctx, filter.WorkspaceID, authorityScopeWorkspace)
	if err != nil {
		return RuntimeReplayReport{}, fmt.Errorf("list replica freshness for replay: %w", err)
	}
	report.ReplicaCoverage = runtimeReplayReplicaCoverage(report.ReplicaFreshness)
	for _, session := range report.Sessions {
		if model.IsSessionStatusActive(session.Status) {
			report.Metrics.ActiveSessionCount++
		}
	}
	for _, queue := range report.Queues {
		if normalizeOperatorQueueStatus(queue.Status) == "OPEN" {
			report.Metrics.OpenQueueCount++
			if operatorQueueIsOverdue(queue, referenceTime) {
				report.Metrics.OverdueQueueCount++
			}
		}
	}
	for _, claim := range report.Claims {
		if isKnowledgeClaimOperationalStatus(claim.Status) {
			report.Metrics.ActiveClaimCount++
		}
	}
	for _, run := range report.ExecutionRuns {
		if !isExecutionRunTerminal(run.Status) {
			report.Metrics.OpenExecutionRuns++
		}
	}
	report.MissingParentRefs = runtimeReplayMissingParentRefs(findings)
	report.WindowIncomplete = report.Truncated || len(report.MissingParentRefs) > 0
	var currentAuthority *WorkspaceAuthorityRecord
	if authority, authorityErr := s.GetWorkspaceAuthority(ctx, filter.WorkspaceID, authorityScopeWorkspace); authorityErr == nil {
		currentAuthority = &authority
	} else if !errors.Is(authorityErr, sql.ErrNoRows) {
		return RuntimeReplayReport{}, authorityErr
	}
	localAuthorityNodeID := s.runtimeReplayLocalAuthorityNodeID()
	report.LocalReplicaFreshness = s.runtimeReplayLocalReplicaFreshness(report.ReplicaFreshness)
	report.Scope = runtimeReplayScopeAssessment(report, currentAuthority, localAuthorityNodeID)
	retentionRisk := s.runtimeReplayRetentionRisk(ctx, report)
	report.Evaluation = evaluateRuntimeReplay(report, findings, sources, retentionRisk)

	return report, nil
}

func runtimeReplayReplicaCoverage(rows []WorkspaceReplicaFreshnessRecord) RuntimeReplayReplicaCoverage {
	coverage := RuntimeReplayReplicaCoverage{ReplicaCount: len(rows)}
	for _, row := range rows {
		if row.ExportLag > 0 || row.FetchLag > 0 || row.AckLag > 0 || row.ApplyLag > 0 {
			coverage.LaggingReplicaCount++
		}
		if row.ApplyStatus == WorkspaceReplicaApplyStatusRetryPending {
			coverage.RetryPendingCount++
		}
		if row.ApplyStatus == WorkspaceReplicaApplyStatusDeadLetter {
			coverage.DeadLetterCount++
		}
		coverage.MaxExportLag = maxInt64(coverage.MaxExportLag, row.ExportLag)
		coverage.MaxFetchLag = maxInt64(coverage.MaxFetchLag, row.FetchLag)
		coverage.MaxAckLag = maxInt64(coverage.MaxAckLag, row.AckLag)
		coverage.MaxApplyLag = maxInt64(coverage.MaxApplyLag, row.ApplyLag)
	}
	return coverage
}

func runtimeReplayMissingParentRefs(findings []RuntimeReplayFinding) []string {
	missing := map[string]struct{}{}
	var missingRefs []string
	for _, finding := range findings {
		if finding.Code != "runtime_event_parent_ref_missing" {
			continue
		}
		parts := strings.Split(finding.Message, " ")
		if len(parts) < 4 {
			continue
		}
		ref := parts[3]
		if _, seen := missing[ref]; seen {
			continue
		}
		missing[ref] = struct{}{}
		missingRefs = append(missingRefs, ref)
	}
	return missingRefs
}

func (s *Store) runtimeReplayLocalAuthorityNodeID() string {
	if s == nil || strings.TrimSpace(s.authorityIdentityPath) == "" {
		return ""
	}
	localAuthorityNodeID, err := readAuthorityNodeID(s.authorityIdentityPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(localAuthorityNodeID)
}

func (s *Store) runtimeReplayLocalReplicaFreshness(rows []WorkspaceReplicaFreshnessRecord) *WorkspaceReplicaFreshnessRecord {
	localAuthorityNodeID := s.runtimeReplayLocalAuthorityNodeID()
	if localAuthorityNodeID == "" || len(rows) == 0 {
		return nil
	}
	for _, row := range rows {
		if strings.TrimSpace(row.ReplicaAuthorityNodeID) != localAuthorityNodeID {
			continue
		}
		record := row
		return &record
	}
	return nil
}

func runtimeReplayLocalReplicaScopeReasons(localAuthorityNodeID string, local *WorkspaceReplicaFreshnessRecord, currentAuthority *WorkspaceAuthorityRecord) []string {
	if local == nil {
		if localAuthorityNodeID != "" && currentAuthority != nil && strings.TrimSpace(currentAuthority.HolderAuthorityNodeID) != "" && strings.TrimSpace(currentAuthority.HolderAuthorityNodeID) != localAuthorityNodeID {
			return []string{
				"local_store_not_current_authority_holder",
				"local_replica_state_missing",
			}
		}
		return nil
	}
	reasons := make([]string, 0, 8)
	if currentAuthority != nil {
		if strings.TrimSpace(currentAuthority.HolderAuthorityNodeID) != strings.TrimSpace(local.ReplicaAuthorityNodeID) {
			reasons = append(reasons, "local_store_not_current_authority_holder")
		}
		if local.AuthorityTerm != currentAuthority.Term {
			reasons = append(reasons, "local_replica_authority_term_mismatch")
		}
		if strings.TrimSpace(local.LeaderAuthorityNodeID) != "" && strings.TrimSpace(local.LeaderAuthorityNodeID) != strings.TrimSpace(currentAuthority.HolderAuthorityNodeID) {
			reasons = append(reasons, "local_replica_leader_mismatch")
		}
	}
	if local.ReplicaRole != WorkspaceReplicaRoleFollower {
		return reasons
	}
	switch local.MembershipState {
	case WorkspaceReplicaMembershipProvisional:
		reasons = append(reasons, "local_replica_membership_provisional")
	case WorkspaceReplicaMembershipCatchingUp:
		reasons = append(reasons, "local_replica_membership_catching_up")
	case WorkspaceReplicaMembershipStale:
		reasons = append(reasons, "local_replica_membership_stale")
	case WorkspaceReplicaMembershipRejoinPending:
		reasons = append(reasons, "local_replica_membership_rejoin_pending")
	case WorkspaceReplicaMembershipRejected:
		reasons = append(reasons, "local_replica_membership_rejected")
	case WorkspaceReplicaMembershipDisbanded:
		reasons = append(reasons, "local_replica_membership_disbanded")
	}
	if local.ExportLag > 0 {
		reasons = append(reasons, "local_replica_export_lag")
	}
	if local.FetchLag > 0 {
		reasons = append(reasons, "local_replica_fetch_lag")
	}
	if local.AckLag > 0 {
		reasons = append(reasons, "local_replica_ack_lag")
	}
	if local.ApplyLag > 0 {
		reasons = append(reasons, "local_replica_apply_lag")
	}
	switch local.ApplyStatus {
	case WorkspaceReplicaApplyStatusRetryPending:
		reasons = append(reasons, "local_replica_apply_retry_pending")
	case WorkspaceReplicaApplyStatusDeadLetter:
		reasons = append(reasons, "local_replica_apply_dead_letter")
	}
	return reasons
}

func runtimeReplayScopeAssessment(report RuntimeReplayReport, currentAuthority *WorkspaceAuthorityRecord, localAuthorityNodeID string) RuntimeReplayScopeAssessment {
	scope := RuntimeReplayScopeAssessment{
		Authoritative: true,
		IntegrityBand: "COMPLETE",
	}
	if report.Truncated {
		scope.Reasons = append(scope.Reasons, "truncated_window")
	}
	if strings.TrimSpace(report.Filter.AgentID) != "" {
		scope.Reasons = append(scope.Reasons, "agent_filtered_scope")
	}
	if strings.TrimSpace(report.Filter.SessionID) != "" {
		scope.Reasons = append(scope.Reasons, "session_filtered_scope")
	}
	if strings.TrimSpace(report.Filter.TaskID) != "" {
		scope.Reasons = append(scope.Reasons, "task_filtered_scope")
	}
	if report.Filter.ExcludeSynthetic {
		scope.Reasons = append(scope.Reasons, "synthetic_events_excluded")
	}
	if len(report.MissingParentRefs) > 0 {
		scope.Reasons = append(scope.Reasons, "missing_parent_refs")
	}
	scope.Reasons = append(scope.Reasons, runtimeReplayLocalReplicaScopeReasons(localAuthorityNodeID, report.LocalReplicaFreshness, currentAuthority)...)
	if len(scope.Reasons) == 0 {
		return scope
	}
	scope.Authoritative = false
	scope.IntegrityBand = "PARTIAL"
	scope.SuppressedConclusions = []string{
		"negative_absence_claims",
		"rollback_trace_absence",
	}
	return scope
}

func replayAgentSessionEvent(event RuntimeEventRecord, sessions map[string]*RuntimeReplaySession, sessionSources map[string]runtimeReplayFindingSource, findings *[]RuntimeReplayFinding) {
	if strings.TrimSpace(event.EventType) == "session.takeover" {
		var takeover AgentSessionTakeoverRecord
		if !decodeReplayPayload(event, &takeover) {
			*findings = append(*findings, malformedReplayEvent(event, "decode session takeover payload"))
			return
		}
		applyReplaySessionState(sessions, sessionSources, takeover.SourceState, event)
		applyReplaySessionState(sessions, sessionSources, takeover.SuccessorState, event)
		return
	}

	var state AgentSessionStateRecord
	if !decodeReplayPayload(event, &state) {
		*findings = append(*findings, malformedReplayEvent(event, "decode session payload"))
		return
	}
	applyReplaySessionState(sessions, sessionSources, state, event)
}

func shouldApplyReplayEvent(event RuntimeEventRecord, dedupState map[string]*runtimeReplayDedupState, metrics *RuntimeReplayMetrics, findings *[]RuntimeReplayFinding) bool {
	dedupKey := runtimeReplayEventDedupKey(event)
	if dedupKey == "" {
		return true
	}
	state := dedupState[dedupKey]
	if state == nil {
		dedupState[dedupKey] = &runtimeReplayDedupState{firstEvent: event}
		return true
	}
	if runtimeReplayEventsSemanticallyEquivalent(state.firstEvent, event) {
		metrics.SuppressedDuplicateEvents++
		return false
	}
	if state.conflicted {
		metrics.SuppressedDuplicateEvents++
		return false
	}
	state.conflicted = true
	metrics.ConflictingDuplicateKeys++
	metrics.SuppressedDuplicateEvents++
	*findings = append(*findings, replayFindingFromEvent(
		event,
		"runtime_event_dedup_conflict",
		"warning",
		fmt.Sprintf("dedup_key %s reappears with different runtime-event semantics", dedupKey),
	))
	return false
}

func runtimeReplayEventsSemanticallyEquivalent(left, right RuntimeEventRecord) bool {
	leftLineage := runtimeReplayEventLineage(left)
	rightLineage := runtimeReplayEventLineage(right)
	return left.WorkspaceID == right.WorkspaceID &&
		leftLineage.DedupKey == rightLineage.DedupKey &&
		left.EventType == right.EventType &&
		left.EntityType == right.EntityType &&
		left.EntityID == right.EntityID &&
		left.ActorType == right.ActorType &&
		left.ActorID == right.ActorID &&
		left.AgentID == right.AgentID &&
		left.SessionID == right.SessionID &&
		left.TaskID == right.TaskID &&
		leftLineage.RootCauseID == rightLineage.RootCauseID &&
		leftLineage.ProvenanceGroupID == rightLineage.ProvenanceGroupID &&
		leftLineage.ParentRefsJSON == rightLineage.ParentRefsJSON &&
		runtimeReplayCanonicalBusinessPayload(left.PayloadJSON) == runtimeReplayCanonicalBusinessPayload(right.PayloadJSON)
}

type runtimeReplayLineageFields struct {
	DedupKey          string
	RootCauseID       string
	ProvenanceGroupID string
	ParentRefsJSON    string
}

type runtimeReplayPayloadLineage struct {
	DedupKey          string          `json:"dedup_key"`
	RootCauseID       string          `json:"root_cause_id"`
	ProvenanceGroupID string          `json:"provenance_group_id"`
	ParentRefs        json.RawMessage `json:"parent_refs"`
	ParentRefsJSON    json.RawMessage `json:"parent_refs_json"`
}

func runtimeReplayEventDedupKey(event RuntimeEventRecord) string {
	return runtimeReplayEventLineage(event).DedupKey
}

func runtimeReplayEventLineage(event RuntimeEventRecord) runtimeReplayLineageFields {
	lineage := runtimeReplayLineageFields{
		DedupKey:          strings.TrimSpace(event.DedupKey),
		RootCauseID:       strings.TrimSpace(event.RootCauseID),
		ProvenanceGroupID: strings.TrimSpace(event.ProvenanceGroupID),
		ParentRefsJSON:    runtimeReplayCanonicalParentRefsJSON(event.ParentRefsJSON),
	}

	var payload runtimeReplayPayloadLineage
	if strings.TrimSpace(event.PayloadJSON) == "" || json.Unmarshal([]byte(event.PayloadJSON), &payload) != nil {
		return lineage
	}
	if lineage.DedupKey == "" {
		lineage.DedupKey = strings.TrimSpace(payload.DedupKey)
	}
	if lineage.RootCauseID == "" {
		lineage.RootCauseID = strings.TrimSpace(payload.RootCauseID)
	}
	if lineage.ProvenanceGroupID == "" {
		lineage.ProvenanceGroupID = strings.TrimSpace(payload.ProvenanceGroupID)
	}
	if lineage.ParentRefsJSON == "[]" {
		switch {
		case len(payload.ParentRefs) > 0:
			lineage.ParentRefsJSON = runtimeReplayCanonicalParentRefsJSON(string(payload.ParentRefs))
		case len(payload.ParentRefsJSON) > 0:
			lineage.ParentRefsJSON = runtimeReplayCanonicalParentRefsJSON(string(payload.ParentRefsJSON))
		}
	}
	return lineage
}

func runtimeReplayCanonicalParentRefsJSON(raw string) string {
	normalized, err := normalizeRuntimeEventParentRefs(raw)
	if err == nil {
		return normalized
	}
	return runtimeReplayCanonicalJSON(raw, "[]")
}

func runtimeReplayCanonicalBusinessPayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "{}"
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &object); err != nil {
		return runtimeReplayCanonicalJSON(trimmed, "{}")
	}
	delete(object, "dedup_key")
	delete(object, "root_cause_id")
	delete(object, "provenance_group_id")
	delete(object, "parent_refs")
	delete(object, "parent_refs_json")
	encoded, err := json.Marshal(object)
	if err != nil {
		return runtimeReplayCanonicalJSON(trimmed, "{}")
	}
	return string(encoded)
}

func runtimeReplayCanonicalJSON(raw string, emptyDefault string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = emptyDefault
	}
	var value any
	if err := json.Unmarshal([]byte(trimmed), &value); err != nil {
		return trimmed
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return trimmed
	}
	return string(encoded)
}

func replayOperatorQueueEvent(event RuntimeEventRecord, queues map[string]*RuntimeReplayQueue, queueSources map[string]runtimeReplayFindingSource, findings *[]RuntimeReplayFinding) {
	record := replayQueueState(queues, event.EntityID)
	queueSources[record.QueueID] = replayFindingSourceFromEvent(event)
	record.WorkspaceID = firstNonEmpty(record.WorkspaceID, event.WorkspaceID)
	record.AgentID = firstNonEmpty(strings.TrimSpace(event.AgentID), record.AgentID)
	record.SessionID = firstNonEmpty(strings.TrimSpace(event.SessionID), record.SessionID)
	record.TaskID = firstNonEmpty(strings.TrimSpace(event.TaskID), record.TaskID)
	record.LastEventType = event.EventType
	record.EventCount++
	record.UpdatedAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), record.UpdatedAt)
	if record.CreatedAt == "" {
		record.CreatedAt = record.UpdatedAt
	}
	var payload replayOperatorQueuePayload
	if strings.TrimSpace(event.PayloadJSON) != "" && !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode operator queue payload"))
		return
	}
	record.QueueKey = firstNonEmpty(strings.TrimSpace(payload.QueueKey), record.QueueKey)
	record.QueueType = firstNonEmpty(strings.TrimSpace(payload.QueueType), record.QueueType)
	record.Status = firstNonEmpty(strings.TrimSpace(payload.Status), record.Status, "OPEN")
	record.Title = firstNonEmpty(strings.TrimSpace(payload.Title), record.Title)
	record.Summary = firstNonEmpty(strings.TrimSpace(payload.Summary), record.Summary)
	record.AssignedTo = firstNonEmpty(strings.TrimSpace(payload.AssignedTo), record.AssignedTo)
	record.Urgency = firstNonEmpty(strings.TrimSpace(payload.Urgency), record.Urgency)
	record.SourceKind = firstNonEmpty(strings.TrimSpace(payload.SourceKind), record.SourceKind)
	record.SourceID = firstNonEmpty(strings.TrimSpace(payload.SourceID), record.SourceID)
	if dueAt := stringPtr(strings.TrimSpace(payload.DueAt)); dueAt != nil {
		record.DueAt = dueAt
	}
	record.Resolution = firstNonEmpty(strings.TrimSpace(payload.Resolution), record.Resolution)
	record.ResolvedBy = firstNonEmpty(strings.TrimSpace(payload.ResolvedBy), record.ResolvedBy)
	if payload.EscalationCount > record.EscalationCount {
		record.EscalationCount = payload.EscalationCount
	}
	if lastEscalatedAt := stringPtr(strings.TrimSpace(payload.LastEscalatedAt)); lastEscalatedAt != nil {
		record.LastEscalatedAt = lastEscalatedAt
	}
	record.LastEscalatedBy = firstNonEmpty(strings.TrimSpace(payload.LastEscalatedBy), record.LastEscalatedBy)
	record.EscalationReason = firstNonEmpty(strings.TrimSpace(payload.EscalationReason), record.EscalationReason)
	if strings.TrimSpace(event.EventType) == "operator_queue.resolved" || strings.TrimSpace(event.EventType) == "operator_queue.cancelled" {
		record.ResolvedAt = stringPtr(strings.TrimSpace(event.CreatedAt))
	}
	record.KeepSessionActive = record.KeepSessionActive || payload.KeepSessionActive
}

func replayKnowledgeClaimEvent(event RuntimeEventRecord, claims map[string]*RuntimeReplayClaim, claimSources map[string]runtimeReplayFindingSource, findings *[]RuntimeReplayFinding) {
	record := replayClaimState(claims, event.EntityID)
	claimSources[record.ClaimID] = replayFindingSourceFromEvent(event)
	record.WorkspaceID = firstNonEmpty(record.WorkspaceID, event.WorkspaceID)
	record.AgentID = firstNonEmpty(strings.TrimSpace(event.AgentID), record.AgentID)
	record.SessionID = firstNonEmpty(strings.TrimSpace(event.SessionID), record.SessionID)
	record.TaskID = firstNonEmpty(strings.TrimSpace(event.TaskID), record.TaskID)
	record.LastEventType = event.EventType
	record.EventCount++
	record.UpdatedAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), record.UpdatedAt)
	if record.CreatedAt == "" {
		record.CreatedAt = record.UpdatedAt
	}
	var payload replayKnowledgeClaimPayload
	if strings.TrimSpace(event.PayloadJSON) != "" && !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode knowledge claim payload"))
		return
	}
	record.ClaimType = firstNonEmpty(strings.TrimSpace(payload.ClaimType), record.ClaimType)
	record.Status = firstNonEmpty(strings.TrimSpace(payload.Status), record.Status, "ACTIVE")
	record.Subject = firstNonEmpty(strings.TrimSpace(payload.Subject), record.Subject)
	record.Summary = firstNonEmpty(strings.TrimSpace(payload.Summary), record.Summary)
	record.SourceKind = firstNonEmpty(strings.TrimSpace(payload.SourceKind), record.SourceKind)
	record.SourceID = firstNonEmpty(strings.TrimSpace(payload.SourceID), record.SourceID)
	record.MemoryID = firstNonEmpty(strings.TrimSpace(payload.MemoryID), record.MemoryID)
	record.SupersedesClaimID = firstNonEmpty(strings.TrimSpace(payload.SupersedesClaimID), record.SupersedesClaimID)
	record.SupersededByClaimID = firstNonEmpty(strings.TrimSpace(payload.SupersededByClaimID), record.SupersededByClaimID)
	record.ConflictsClaimID = firstNonEmpty(strings.TrimSpace(payload.ConflictsClaimID), record.ConflictsClaimID)
	record.LifecycleReason = firstNonEmpty(strings.TrimSpace(payload.LifecycleReason), record.LifecycleReason)
	if dueAt := stringPtr(strings.TrimSpace(payload.ReviewDueAt)); dueAt != nil {
		record.ReviewDueAt = dueAt
	}
	if reviewedAt := stringPtr(strings.TrimSpace(payload.ReviewedAt)); reviewedAt != nil {
		record.ReviewedAt = reviewedAt
	}
	record.ReviewedBy = firstNonEmpty(strings.TrimSpace(payload.ReviewedBy), record.ReviewedBy)
	if payload.Confidence > 0 || record.Confidence == 0 {
		record.Confidence = payload.Confidence
	}
	if strings.TrimSpace(event.EventType) == "knowledge_claim.archived" {
		record.Status = "ARCHIVED"
		record.ArchivedAt = stringPtr(strings.TrimSpace(event.CreatedAt))
		record.ArchivedBy = firstNonEmpty(strings.TrimSpace(payload.ArchivedBy), record.ArchivedBy)
	} else if normalizeKnowledgeClaimStatus(record.Status) != "ARCHIVED" {
		record.ArchivedAt = nil
		record.ArchivedBy = ""
	}
}

func replayWorkspaceMemoryEvent(event RuntimeEventRecord, memories map[string]*RuntimeReplayWorkspaceMemory, findings *[]RuntimeReplayFinding) {
	record := replayWorkspaceMemoryState(memories, event.EntityID)
	record.WorkspaceID = firstNonEmpty(record.WorkspaceID, event.WorkspaceID)
	record.AgentID = firstNonEmpty(strings.TrimSpace(event.AgentID), record.AgentID)
	record.SessionID = firstNonEmpty(strings.TrimSpace(event.SessionID), record.SessionID)
	record.TaskID = firstNonEmpty(strings.TrimSpace(event.TaskID), record.TaskID)
	record.LastEventType = event.EventType
	record.EventCount++
	record.UpdatedAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), record.UpdatedAt)
	if record.CreatedAt == "" {
		record.CreatedAt = record.UpdatedAt
	}
	var payload replayWorkspaceMemoryPayload
	if strings.TrimSpace(event.PayloadJSON) != "" && !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode workspace memory payload"))
		return
	}
	record.MemoryType = firstNonEmpty(strings.TrimSpace(payload.MemoryType), record.MemoryType)
	record.SourceKind = firstNonEmpty(strings.TrimSpace(payload.SourceKind), record.SourceKind)
	record.SourceID = firstNonEmpty(strings.TrimSpace(payload.SourceID), record.SourceID)
	switch strings.TrimSpace(event.EventType) {
	case "workspace_memory.archived":
		record.ArchivedAt = stringPtr(strings.TrimSpace(event.CreatedAt))
		record.ArchivedBy = firstNonEmpty(strings.TrimSpace(payload.ArchivedBy), strings.TrimSpace(event.ActorID), record.ArchivedBy)
		record.ArchivedReason = firstNonEmpty(strings.TrimSpace(payload.Reason), record.ArchivedReason)
	case "workspace_memory.restored":
		record.ArchivedAt = nil
		record.ArchivedBy = ""
		record.ArchivedReason = ""
		record.RecoveryReason = firstNonEmpty(strings.TrimSpace(payload.RecoveryReason), record.RecoveryReason)
	}
}

func replayExecutionRunEvent(event RuntimeEventRecord, runs map[string]*RuntimeReplayExecutionRun, runSources map[string]runtimeReplayFindingSource, findings *[]RuntimeReplayFinding) {
	record := replayExecutionRunState(runs, event.EntityID)
	runSources[record.RunID] = replayFindingSourceFromEvent(event)
	record.WorkspaceID = firstNonEmpty(record.WorkspaceID, event.WorkspaceID)
	record.LastEventType = event.EventType
	record.EventCount++
	record.RunEventCount++
	record.UpdatedAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), record.UpdatedAt)
	if record.CreatedAt == "" {
		record.CreatedAt = record.UpdatedAt
	}
	var payload replayExecutionRunPayload
	if strings.TrimSpace(event.PayloadJSON) != "" && !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode execution run payload"))
		return
	}
	record.AgentID = firstNonEmpty(strings.TrimSpace(event.AgentID), strings.TrimSpace(payload.AgentID), record.AgentID)
	record.SessionID = firstNonEmpty(strings.TrimSpace(event.SessionID), strings.TrimSpace(payload.SessionID), record.SessionID)
	record.TaskID = firstNonEmpty(strings.TrimSpace(event.TaskID), strings.TrimSpace(payload.TaskID), record.TaskID)
	record.Status = firstNonEmpty(strings.TrimSpace(payload.Status), record.Status, "PLANNED")
	record.Outcome = firstNonEmpty(strings.TrimSpace(payload.Outcome), record.Outcome)
	record.Title = firstNonEmpty(strings.TrimSpace(payload.Title), record.Title)
	record.Summary = firstNonEmpty(strings.TrimSpace(payload.Summary), record.Summary)
	if isExecutionRunTerminal(record.Status) {
		record.ClosedAt = stringPtr(strings.TrimSpace(event.CreatedAt))
	}
}

func replayExecutionStepEvent(event RuntimeEventRecord, runs map[string]*RuntimeReplayExecutionRun, runSources map[string]runtimeReplayFindingSource, findings *[]RuntimeReplayFinding) {
	var payload replayExecutionStepPayload
	if !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode execution step payload"))
		return
	}
	runID := strings.TrimSpace(payload.RunID)
	if runID == "" {
		*findings = append(*findings, replayFindingFromEvent(
			event,
			"execution_step_missing_run",
			"error",
			"execution step event is missing run_id",
		))
		return
	}
	record := replayExecutionRunState(runs, runID)
	runSources[record.RunID] = replayFindingSourceFromEvent(event)
	record.WorkspaceID = firstNonEmpty(record.WorkspaceID, event.WorkspaceID)
	record.AgentID = firstNonEmpty(strings.TrimSpace(event.AgentID), record.AgentID)
	record.SessionID = firstNonEmpty(strings.TrimSpace(event.SessionID), record.SessionID)
	record.TaskID = firstNonEmpty(strings.TrimSpace(event.TaskID), record.TaskID)
	record.LastEventType = event.EventType
	record.EventCount++
	record.StepEventCount++
	record.UpdatedAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), record.UpdatedAt)
	if record.CreatedAt == "" {
		record.CreatedAt = record.UpdatedAt
	}
	phase := strings.TrimSpace(payload.Phase)
	if phase != "" {
		record.PhaseCounts[phase]++
	}
	status := strings.TrimSpace(payload.Status)
	if status != "" {
		record.StepStatusCounts[status]++
	}
	if record.Title == "" && strings.TrimSpace(payload.Title) != "" {
		record.Title = clipSummary(strings.TrimSpace(payload.Title), 128)
	}
	if record.Summary == "" && strings.TrimSpace(payload.Summary) != "" {
		record.Summary = clipSummary(strings.TrimSpace(payload.Summary), 240)
	}
}

func replayCapabilityPolicyEvent(event RuntimeEventRecord, policies map[string]*CapabilityPolicyRecord, findings *[]RuntimeReplayFinding) {
	var payload replayCapabilityPolicyPayload
	if !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode capability policy payload"))
		return
	}
	policyID := strings.TrimSpace(event.EntityID)
	if policyID == "" {
		*findings = append(*findings, malformedReplayEvent(event, "normalize capability policy payload"))
		return
	}
	record, err := normalizeCapabilityPolicyInput(CapabilityPolicyInput{
		PolicyID:    policyID,
		WorkspaceID: event.WorkspaceID,
		SubjectType: payload.SubjectType,
		SubjectID:   payload.SubjectID,
		Capability:  payload.Capability,
		ToolID:      payload.ToolID,
		Effect:      payload.Effect,
		Reason:      payload.Reason,
		CreatedBy:   firstNonEmpty(strings.TrimSpace(payload.CreatedBy), strings.TrimSpace(event.ActorID)),
	})
	if err != nil {
		*findings = append(*findings, malformedReplayEvent(event, "normalize capability policy payload"))
		return
	}
	record.PolicyID = policyID
	record.CreatedAt = strings.TrimSpace(event.CreatedAt)
	record.UpdatedAt = strings.TrimSpace(event.CreatedAt)

	state := replayCapabilityPolicyState(policies, policyID)
	if strings.TrimSpace(state.CreatedAt) == "" {
		state.CreatedAt = record.CreatedAt
	}
	state.PolicyID = record.PolicyID
	state.WorkspaceID = record.WorkspaceID
	state.SubjectType = record.SubjectType
	state.SubjectID = record.SubjectID
	state.Capability = record.Capability
	state.ToolID = record.ToolID
	state.Effect = record.Effect
	state.Reason = record.Reason
	state.CreatedBy = record.CreatedBy
	state.UpdatedAt = record.UpdatedAt
}

func replayControlCommandEvent(event RuntimeEventRecord, commands map[string]*ControlCommandRecord, findings *[]RuntimeReplayFinding) {
	var payload replayControlCommandPayload
	if !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode control command payload"))
		return
	}
	commandID := strings.TrimSpace(event.EntityID)
	if commandID == "" {
		*findings = append(*findings, malformedReplayEvent(event, "normalize control command payload"))
		return
	}
	parentRefs := runtimeReplayParentRefIDs(runtimeReplayEventLineage(event).ParentRefsJSON)
	if len(parentRefs) == 0 && len(payload.ParentRefs) > 0 {
		parentRefs = append([]string(nil), payload.ParentRefs...)
	}
	record, err := normalizeControlCommandInput(ControlCommandInput{
		CommandID:      commandID,
		WorkspaceID:    event.WorkspaceID,
		CommandType:    payload.CommandType,
		Scope:          payload.Scope,
		ProtoClusterID: payload.ProtoClusterID,
		TensionID:      payload.TensionID,
		AgentID:        firstNonEmpty(strings.TrimSpace(payload.AgentID), strings.TrimSpace(event.AgentID)),
		TargetMode:     payload.TargetMode,
		TTLSeconds:     payload.TTLSeconds,
		Reason:         payload.Reason,
		RequestedBy:    firstNonEmpty(strings.TrimSpace(payload.RequestedBy), strings.TrimSpace(event.ActorID)),
		ActorType:      firstNonEmpty(strings.TrimSpace(payload.ActorType), strings.TrimSpace(event.ActorType)),
		ParentRefs:     parentRefs,
	})
	if err != nil {
		*findings = append(*findings, malformedReplayEvent(event, "normalize control command payload"))
		return
	}
	record.CommandID = commandID
	record.RequestedAt = firstNonEmpty(strings.TrimSpace(payload.RequestedAt), strings.TrimSpace(event.CreatedAt))
	record.AppliedInline = payload.AppliedInline
	record.ExpiresAt = strings.TrimSpace(payload.ExpiresAt)

	state := replayControlCommandState(commands, commandID)
	*state = record
}

func replayEffectiveControlsEvent(event RuntimeEventRecord, controls map[string]*EffectiveControlsRecord, findings *[]RuntimeReplayFinding) {
	var payload replayEffectiveControlsPayload
	if !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode effective controls payload"))
		return
	}
	scopeKey := strings.TrimSpace(event.EntityID)
	workspaceID := firstNonEmpty(strings.TrimSpace(event.WorkspaceID), strings.TrimSpace(payload.WorkspaceID))
	if scopeKey == "" || workspaceID == "" || payload.TTLSeconds <= 0 {
		*findings = append(*findings, malformedReplayEvent(event, "normalize effective controls payload"))
		return
	}
	record := EffectiveControlsRecord{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    strings.TrimSpace(payload.ProtoClusterID),
		Epoch:             payload.Epoch,
		TTLSeconds:        payload.TTLSeconds,
		ExpiresAt:         strings.TrimSpace(payload.ExpiresAt),
		ControlMode:       strings.TrimSpace(payload.ControlMode),
		CandidateMode:     strings.TrimSpace(payload.CandidateMode),
		CandidateControls: payload.CandidateControls,
		AdvisoryControls:  payload.AdvisoryControls,
		EffectiveControls: payload.EffectiveControls,
		ResolvedFrom:      strings.TrimSpace(payload.ResolvedFrom),
		MatchScore:        payload.MatchScore,
		BasisSummary:      strings.TrimSpace(payload.BasisSummary),
		GeneratedAt:       strings.TrimSpace(payload.GeneratedAt),
		ActorID:           firstNonEmpty(strings.TrimSpace(payload.ActorID), strings.TrimSpace(event.ActorID)),
		CreatedAt:         firstNonEmpty(strings.TrimSpace(payload.CreatedAt), strings.TrimSpace(event.CreatedAt)),
		UpdatedAt:         firstNonEmpty(strings.TrimSpace(payload.UpdatedAt), strings.TrimSpace(event.CreatedAt)),
	}
	if effectiveControlsRuntimeEntityID(record) != scopeKey {
		*findings = append(*findings, malformedReplayEvent(event, "normalize effective controls payload"))
		return
	}
	state := replayEffectiveControlsState(controls, scopeKey)
	*state = record
}

func replayUnifiedControlSnapshotEvent(event RuntimeEventRecord, snapshots map[string]*RuntimeReplayUnifiedControlSnapshot, snapshotSources map[string]runtimeReplayFindingSource, findings *[]RuntimeReplayFinding) {
	var payload replayUnifiedControlSnapshotPayload
	if !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode unified control snapshot payload"))
		return
	}
	entityID := strings.TrimSpace(event.EntityID)
	workspaceID := firstNonEmpty(strings.TrimSpace(event.WorkspaceID), strings.TrimSpace(payload.WorkspaceID), strings.TrimSpace(payload.Report.WorkspaceID))
	expectedAdvisory := strings.TrimSpace(event.EventType) == "cluster.unified_control_advisory_snapshot"
	expectedTyped := "UNIFIED_CONTROL_EFFECTIVE_SNAPSHOT"
	if expectedAdvisory {
		expectedTyped = "UNIFIED_CONTROL_ADVISORY_SNAPSHOT"
	}
	if entityID == "" || workspaceID == "" ||
		(strings.TrimSpace(payload.EventKind) != "" && strings.TrimSpace(payload.EventKind) != strings.TrimSpace(event.EventType)) ||
		(strings.TrimSpace(payload.TypedEventType) != "" && strings.TrimSpace(payload.TypedEventType) != expectedTyped) ||
		payload.AdvisoryOnly != expectedAdvisory ||
		payload.Report.AdvisoryOnly != expectedAdvisory ||
		!replayUnifiedControlSnapshotPayloadConsistent(payload) {
		*findings = append(*findings, malformedReplayEvent(event, "normalize unified control snapshot payload"))
		return
	}
	key := replayUnifiedControlSnapshotKey(entityID, event.EventType)
	snapshotSources[key] = replayFindingSourceFromEvent(event)
	state := replayUnifiedControlSnapshotState(snapshots, key)
	state.SnapshotKey = key
	state.WorkspaceID = workspaceID
	state.EntityID = entityID
	state.EventType = strings.TrimSpace(event.EventType)
	state.TypedEventType = firstNonEmpty(strings.TrimSpace(payload.TypedEventType), expectedTyped)
	state.AdvisoryOnly = expectedAdvisory
	state.Resolved = payload.Report.Resolved
	state.ResolvedFrom = firstNonEmpty(strings.TrimSpace(payload.Report.ResolvedFrom), strings.TrimSpace(payload.ResolvedFrom))
	state.Summary = firstNonEmpty(strings.TrimSpace(payload.Summary), strings.TrimSpace(payload.Report.Summary))
	state.ProtoClusterID = strings.TrimSpace(payload.Report.ProtoClusterID)
	state.SessionID = firstNonEmpty(strings.TrimSpace(event.SessionID), strings.TrimSpace(payload.Filter.SessionID))
	state.TaskID = firstNonEmpty(strings.TrimSpace(event.TaskID), strings.TrimSpace(payload.Filter.TaskID))
	state.Filter = payload.Filter
	state.ControlMode = strings.TrimSpace(payload.Report.ControlMode)
	state.CandidateMode = strings.TrimSpace(payload.Report.CandidateMode)
	state.AttentionBand = strings.TrimSpace(payload.Report.AttentionBand)
	state.MemoryNeedsAttention = payload.Report.MemoryNeedsAttention
	state.MemoryAttentionReasons = append([]string(nil), payload.Report.MemoryAttentionReasons...)
	state.CandidateControls = payload.Report.CandidateControls
	state.AdvisoryControls = payload.Report.AdvisoryControls
	state.AppliedActions = append([]string(nil), payload.Report.AppliedActions...)
	state.AppliedActionAudit = append([]UnifiedControlAppliedActionAudit(nil), payload.Report.AppliedActionAudit...)
	state.SuppressedHints = append([]string(nil), payload.Report.SuppressedHints...)
	state.SuppressedHintAudit = append([]UnifiedControlSuppressedHintAudit(nil), payload.Report.SuppressedHintAudit...)
	state.GovernedHintOutcomes = append([]UnifiedControlGovernedHintOutcome(nil), payload.Report.GovernedHintOutcomes...)
	state.Contradictions = append([]string(nil), payload.Report.Contradictions...)
	state.GeneratedAt = strings.TrimSpace(payload.Report.GeneratedAt)
	state.UpdatedAt = strings.TrimSpace(event.CreatedAt)
	state.LastEventID = strings.TrimSpace(event.EventID)
	if payload.Report.EffectiveControlsAudit != nil {
		auditCopy := *payload.Report.EffectiveControlsAudit
		state.EffectiveControlsAudit = &auditCopy
	} else {
		state.EffectiveControlsAudit = nil
	}
	if state.AdvisoryOnly && (state.EffectiveControlsAudit == nil || !state.EffectiveControlsAudit.Found) {
		state.EffectiveControls = ControlSuggestedControls{}
	} else {
		state.EffectiveControls = payload.Report.EffectiveControls
	}
	if payload.Report.AuditSummary != nil {
		summaryCopy := *payload.Report.AuditSummary
		state.AuditSummary = &summaryCopy
	} else {
		state.AuditSummary = nil
	}
	if payload.Report.AuditCoverage != nil {
		coverageCopy := *payload.Report.AuditCoverage
		state.AuditCoverage = &coverageCopy
	} else {
		state.AuditCoverage = nil
	}
	if payload.Report.CooldownBasis != nil {
		cooldownCopy := *payload.Report.CooldownBasis
		state.CooldownBasis = &cooldownCopy
	} else {
		state.CooldownBasis = nil
	}
}

func replayMemoryInvalidationEvent(event RuntimeEventRecord, invalidations map[string]*RuntimeReplayMemoryInvalidation, findings *[]RuntimeReplayFinding) {
	var payload replayMemoryInvalidationPayload
	if !decodeReplayPayload(event, &payload) {
		*findings = append(*findings, malformedReplayEvent(event, "decode memory invalidation payload"))
		return
	}
	invalidationID := strings.TrimSpace(event.EntityID)
	expectedTyped := replayMemoryInvalidationTypedEventType(event.EventType)
	if invalidationID == "" || strings.TrimSpace(payload.RefKind) == "" || strings.TrimSpace(payload.RefID) == "" || strings.TrimSpace(payload.Reason) == "" ||
		(expectedTyped != "" && strings.TrimSpace(payload.TypedEventType) != "" && strings.TrimSpace(payload.TypedEventType) != expectedTyped) {
		*findings = append(*findings, malformedReplayEvent(event, "normalize memory invalidation payload"))
		return
	}
	derivedState := replayMemoryInvalidationDerivedState(event.EventType)
	if strings.TrimSpace(payload.State) != "" && strings.TrimSpace(payload.State) != derivedState {
		*findings = append(*findings, malformedReplayEvent(event, "normalize memory invalidation payload"))
		return
	}
	state := replayMemoryInvalidationState(invalidations, invalidationID)
	if strings.TrimSpace(state.CreatedAt) == "" {
		state.CreatedAt = strings.TrimSpace(event.CreatedAt)
	}
	state.InvalidationID = invalidationID
	state.WorkspaceID = firstNonEmpty(strings.TrimSpace(event.WorkspaceID), state.WorkspaceID)
	state.AgentID = firstNonEmpty(strings.TrimSpace(payload.AgentID), strings.TrimSpace(event.AgentID), state.AgentID)
	state.SessionID = firstNonEmpty(strings.TrimSpace(payload.SessionID), strings.TrimSpace(event.SessionID), state.SessionID)
	state.ReportScope = firstNonEmpty(strings.TrimSpace(payload.ReportScope), state.ReportScope)
	state.ReportID = firstNonEmpty(strings.TrimSpace(payload.ReportID), state.ReportID)
	state.ResidencyTier = firstNonEmpty(strings.TrimSpace(payload.ResidencyTier), state.ResidencyTier)
	state.ReplicaKind = firstNonEmpty(strings.TrimSpace(payload.ReplicaKind), state.ReplicaKind)
	state.CoherenceClass = firstNonEmpty(strings.TrimSpace(payload.CoherenceClass), state.CoherenceClass)
	state.CanonicalMemoryID = firstNonEmpty(strings.TrimSpace(payload.CanonicalMemoryID), state.CanonicalMemoryID)
	state.CacheKey = firstNonEmpty(strings.TrimSpace(payload.CacheKey), state.CacheKey)
	state.RefKind = firstNonEmpty(strings.TrimSpace(payload.RefKind), state.RefKind)
	state.RefID = firstNonEmpty(strings.TrimSpace(payload.RefID), state.RefID)
	state.PreviousVersionToken = firstNonEmpty(strings.TrimSpace(payload.PreviousVersionToken), state.PreviousVersionToken)
	state.CurrentVersionToken = firstNonEmpty(strings.TrimSpace(payload.CurrentVersionToken), state.CurrentVersionToken)
	if len(payload.DependencyRevisionVector) > 0 {
		state.DependencyRevisionVector = append([]MemoryResidencyVersionGuard(nil), payload.DependencyRevisionVector...)
	}
	state.DependencyVectorMalformed = state.DependencyVectorMalformed || payload.DependencyVectorMalformed
	state.Reason = firstNonEmpty(strings.TrimSpace(payload.Reason), state.Reason)
	state.TriggerCause = firstNonEmpty(strings.TrimSpace(payload.TriggerCause), state.TriggerCause)
	state.RecoveredFromInvalidationID = firstNonEmpty(strings.TrimSpace(payload.RecoveredFromInvalidationID), state.RecoveredFromInvalidationID)
	state.RecoveredAt = firstNonEmpty(strings.TrimSpace(payload.RecoveredAt), state.RecoveredAt)
	state.RecoveryCause = firstNonEmpty(strings.TrimSpace(payload.RecoveryCause), state.RecoveryCause)
	state.State = firstNonEmpty(derivedState, state.State)
	state.DeliveryAttemptCount = payload.DeliveryAttemptCount
	state.LastDeliveryAttemptAt = firstNonEmpty(strings.TrimSpace(payload.LastDeliveryAttemptAt), state.LastDeliveryAttemptAt)
	state.LeaseExpiresAt = firstNonEmpty(strings.TrimSpace(payload.LeaseExpiresAt), state.LeaseExpiresAt)
	state.NextDeliveryAt = firstNonEmpty(strings.TrimSpace(payload.NextDeliveryAt), state.NextDeliveryAt)
	state.FailureCount = payload.FailureCount
	state.LastFailureAt = firstNonEmpty(strings.TrimSpace(payload.LastFailureAt), state.LastFailureAt)
	state.LastFailureReason = firstNonEmpty(strings.TrimSpace(payload.LastFailureReason), state.LastFailureReason)
	state.TypedEventType = firstNonEmpty(strings.TrimSpace(payload.TypedEventType), expectedTyped, state.TypedEventType)
	state.LastEventType = strings.TrimSpace(event.EventType)
	state.UpdatedAt = strings.TrimSpace(event.CreatedAt)
	state.LastEventID = strings.TrimSpace(event.EventID)
	switch strings.TrimSpace(event.EventType) {
	case "memory.invalidation_delivered":
		state.DeliveredAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), state.DeliveredAt)
	case "memory.invalidation_acked":
		state.AcknowledgedAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), state.AcknowledgedAt)
	case "memory.invalidation_dead_lettered":
		state.DeadLetteredAt = firstNonEmpty(strings.TrimSpace(event.CreatedAt), state.DeadLetteredAt)
	}
}

func applyReplaySessionState(sessions map[string]*RuntimeReplaySession, sessionSources map[string]runtimeReplayFindingSource, state AgentSessionStateRecord, event RuntimeEventRecord) {
	session := replaySessionState(sessions, state.SessionID)
	sessionSources[session.SessionID] = replayFindingSourceFromEvent(event)
	session.WorkspaceID = firstNonEmpty(session.WorkspaceID, strings.TrimSpace(state.WorkspaceID))
	session.AgentID = firstNonEmpty(strings.TrimSpace(state.AgentID), session.AgentID)
	session.TaskID = firstNonEmpty(strings.TrimSpace(state.TaskID), session.TaskID)
	session.Status = firstNonEmpty(strings.TrimSpace(state.Status), session.Status, model.DefaultSessionStatusForEvent(event.EventType))
	session.Summary = firstNonEmpty(strings.TrimSpace(state.Summary), session.Summary)
	session.OwnerScope = firstNonEmpty(strings.TrimSpace(state.OwnerScope), session.OwnerScope)
	if len(state.BlockedOn) > 0 {
		session.BlockedOn = append([]model.AgentUpdateBlockedRef{}, state.BlockedOn...)
	}
	session.DecisionNeededFrom = firstNonEmpty(strings.TrimSpace(state.DecisionNeededFrom), session.DecisionNeededFrom)
	session.DecisionType = firstNonEmpty(strings.TrimSpace(state.DecisionType), session.DecisionType)
	if state.KeepSessionActive != nil {
		value := *state.KeepSessionActive
		session.KeepSessionActive = &value
	}
	session.HandoffTo = firstNonEmpty(strings.TrimSpace(state.HandoffTo), session.HandoffTo)
	if len(state.RelatedDocKeys) > 0 {
		session.RelatedDocKeys = append([]string{}, state.RelatedDocKeys...)
	}
	session.LastEventType = event.EventType
	session.EventCount++
	session.StartedAt = firstNonEmpty(strings.TrimSpace(state.StartedAt), session.StartedAt, strings.TrimSpace(event.CreatedAt))
	session.UpdatedAt = firstNonEmpty(strings.TrimSpace(state.UpdatedAt), strings.TrimSpace(event.CreatedAt), session.UpdatedAt)
	session.CompletedAt = firstNonEmpty(strings.TrimSpace(state.CompletedAt), session.CompletedAt)
	if session.CompletedAt == "" && !model.IsSessionStatusActive(session.Status) {
		session.CompletedAt = strings.TrimSpace(event.CreatedAt)
	}
}

func evaluateRuntimeReplay(report RuntimeReplayReport, baseFindings []RuntimeReplayFinding, sources runtimeReplayEvaluationSources, retentionRisk RuntimeReplayRetentionRisk) RuntimeReplayEvaluation {
	findings := append([]RuntimeReplayFinding{}, baseFindings...)
	referenceTime := runtimeReplayReferenceTime(report)
	if !report.Scope.Authoritative {
		message := "replay was evaluated from a partial event scope; negative absence conclusions remain non-authoritative"
		if len(report.Scope.Reasons) > 0 {
			message = fmt.Sprintf("%s (%s)", message, strings.Join(report.Scope.Reasons, ", "))
		}
		findings = append(findings, RuntimeReplayFinding{
			Code:     "replay_scope_partial",
			Severity: "warning",
			Message:  message,
		})
	}

	openQueuesBySessionType := map[string]map[string]RuntimeReplayQueue{}
	openClaimQueues := map[string][]RuntimeReplayQueue{}
	runsBySession := map[string]RuntimeReplayExecutionRun{}
	activeClaimByMemory := map[string]int{}
	for _, queue := range report.Queues {
		if normalizeOperatorQueueStatus(queue.Status) != "OPEN" {
			continue
		}
		if operatorQueueIsOverdue(queue, referenceTime) {
			findings = append(findings, replayFindingForEntity(
				"overdue_operator_queue",
				"warning",
				fmt.Sprintf("open %s queue %s is overdue", normalizeOperatorQueueType(queue.QueueType), queue.QueueID),
				"operator_queue",
				queue.QueueID,
				sources.queues[queue.QueueID],
			))
			if strings.EqualFold(strings.TrimSpace(queue.SourceKind), "knowledge_claim") && normalizeOperatorQueueType(queue.QueueType) == "FOLLOW_UP" && queue.EscalationCount == 0 {
				findings = append(findings, replayFindingForEntity(
					"overdue_claim_review_unescalated",
					"warning",
					fmt.Sprintf("overdue claim review queue %s has not been escalated", queue.QueueID),
					"operator_queue",
					queue.QueueID,
					sources.queues[queue.QueueID],
				))
			}
		}
		if strings.TrimSpace(queue.SessionID) != "" {
			if openQueuesBySessionType[queue.SessionID] == nil {
				openQueuesBySessionType[queue.SessionID] = map[string]RuntimeReplayQueue{}
			}
			openQueuesBySessionType[queue.SessionID][normalizeOperatorQueueType(queue.QueueType)] = queue
		}
		if strings.EqualFold(strings.TrimSpace(queue.SourceKind), "knowledge_claim") && strings.TrimSpace(queue.SourceID) != "" {
			openClaimQueues[strings.TrimSpace(queue.SourceID)] = append(openClaimQueues[strings.TrimSpace(queue.SourceID)], queue)
		}
	}
	for _, run := range report.ExecutionRuns {
		if strings.TrimSpace(run.SessionID) != "" {
			runsBySession[run.SessionID] = run
		}
	}
	for _, claim := range report.Claims {
		if isKnowledgeClaimOperationalStatus(claim.Status) && strings.TrimSpace(claim.MemoryID) != "" {
			activeClaimByMemory[claim.MemoryID]++
			sources.memoryClaims[claim.MemoryID] = sources.claims[claim.ClaimID]
		}
	}

	for _, session := range report.Sessions {
		queueType := ""
		switch strings.ToUpper(strings.TrimSpace(session.Status)) {
		case model.SessionStatusBlocked:
			queueType = "BLOCKER"
		case model.SessionStatusWaitingDecision:
			queueType = "DECISION"
		case model.SessionStatusHandoffPending:
			queueType = "HANDOFF"
		}
		if queueType != "" {
			if _, ok := openQueuesBySessionType[session.SessionID][queueType]; !ok {
				findings = append(findings, replayFindingForEntity(
					"missing_operator_queue",
					"warning",
					fmt.Sprintf("session status %s has no open %s operator queue", session.Status, queueType),
					"agent_session",
					session.SessionID,
					sources.sessions[session.SessionID],
				))
			}
		}
		if !model.IsSessionStatusActive(session.Status) {
			for _, queue := range openQueuesBySessionType[session.SessionID] {
				findings = append(findings, replayFindingForEntity(
					"stale_open_queue",
					"warning",
					fmt.Sprintf("terminal session still has open %s queue %s", normalizeOperatorQueueType(queue.QueueType), queue.QueueID),
					"operator_queue",
					queue.QueueID,
					sources.queues[queue.QueueID],
				))
			}
			if run, ok := runsBySession[session.SessionID]; ok && !isExecutionRunTerminal(run.Status) {
				findings = append(findings, replayFindingForEntity(
					"execution_run_out_of_sync",
					"warning",
					fmt.Sprintf("terminal session has non-terminal execution run %s (%s)", run.RunID, run.Status),
					"execution_run",
					run.RunID,
					sources.runs[run.RunID],
				))
			}
			continue
		}
		if strings.TrimSpace(session.TaskID) != "" || strings.TrimSpace(session.Summary) != "" {
			if _, ok := runsBySession[session.SessionID]; !ok {
				findings = append(findings, replayFindingForEntity(
					"missing_execution_run",
					"warning",
					"active session has no execution run in the replayed journal",
					"agent_session",
					session.SessionID,
					sources.sessions[session.SessionID],
				))
			}
		}
	}

	for _, run := range report.ExecutionRuns {
		if !isExecutionRunTerminal(run.Status) && strings.ToUpper(strings.TrimSpace(run.Status)) != "PLANNED" && run.StepEventCount == 0 {
			findings = append(findings, replayFindingForEntity(
				"execution_run_without_steps",
				"warning",
				"execution run has no step events in the replayed journal",
				"execution_run",
				run.RunID,
				sources.runs[run.RunID],
			))
		}
	}

	for _, claim := range report.Claims {
		if isKnowledgeClaimOperationalStatus(claim.Status) &&
			strings.EqualFold(strings.TrimSpace(claim.SourceKind), "workspace_memory") &&
			strings.TrimSpace(claim.MemoryID) == "" {
			findings = append(findings, replayFindingForEntity(
				"claim_missing_memory_link",
				"warning",
				"active workspace_memory claim is missing memory_id linkage",
				"knowledge_claim",
				claim.ClaimID,
				sources.claims[claim.ClaimID],
			))
		}
		if isKnowledgeClaimReviewStatus(claim.Status) {
			if len(openClaimQueues[claim.ClaimID]) == 0 {
				findings = append(findings, replayFindingForEntity(
					"claim_missing_review_queue",
					"warning",
					fmt.Sprintf("claim status %s has no open follow-up queue", claim.Status),
					"knowledge_claim",
					claim.ClaimID,
					sources.claims[claim.ClaimID],
				))
			}
		} else if len(openClaimQueues[claim.ClaimID]) > 0 {
			findings = append(findings, replayFindingForEntity(
				"stale_claim_review_queue",
				"warning",
				fmt.Sprintf("claim status %s still has open follow-up queue", claim.Status),
				"knowledge_claim",
				claim.ClaimID,
				sources.claims[claim.ClaimID],
			))
		}
		if normalizeKnowledgeClaimStatus(claim.Status) == "SUPERSEDED" && strings.TrimSpace(claim.SupersededByClaimID) == "" {
			findings = append(findings, replayFindingForEntity(
				"superseded_claim_missing_link",
				"warning",
				"superseded claim is missing superseded_by_claim_id linkage",
				"knowledge_claim",
				claim.ClaimID,
				sources.claims[claim.ClaimID],
			))
		}
	}
	for memoryID, count := range activeClaimByMemory {
		if count > 1 {
			findings = append(findings, replayFindingForEntity(
				"duplicate_active_memory_claim",
				"warning",
				fmt.Sprintf("memory %s has %d active claims", memoryID, count),
				"knowledge_claim",
				memoryID,
				sources.memoryClaims[memoryID],
			))
		}
	}
	findings = append(findings, runtimeReplayRetentionFindings(retentionRisk)...)
	findings = append(findings, runtimeReplayUnifiedControlTraceFindings(report, sources)...)

	sort.Slice(findings, func(i, j int) bool {
		if replayFindingSeverityRank(findings[i].Severity) != replayFindingSeverityRank(findings[j].Severity) {
			return replayFindingSeverityRank(findings[i].Severity) < replayFindingSeverityRank(findings[j].Severity)
		}
		if findings[i].Code != findings[j].Code {
			return findings[i].Code < findings[j].Code
		}
		if findings[i].EntityType != findings[j].EntityType {
			return findings[i].EntityType < findings[j].EntityType
		}
		if findings[i].EntityID != findings[j].EntityID {
			return findings[i].EntityID < findings[j].EntityID
		}
		return findings[i].SourceEventID < findings[j].SourceEventID
	})

	evaluation := RuntimeReplayEvaluation{
		Verdict:       "pass",
		RetentionRisk: retentionRisk,
		Findings:      findings,
	}
	for _, finding := range findings {
		switch strings.ToLower(strings.TrimSpace(finding.Severity)) {
		case "error":
			evaluation.ErrorCount++
		case "warning":
			evaluation.WarningCount++
		}
	}
	evaluation.FindingSummary = runtimeReplayBuildFindingSummary(findings)
	evaluation.ProvenanceSummary = runtimeReplayBuildProvenanceSummary(findings)
	switch {
	case evaluation.ErrorCount > 0:
		evaluation.Verdict = "fail"
	case evaluation.WarningCount > 0:
		evaluation.Verdict = "warn"
	}
	return evaluation
}

func (s *Store) runtimeReplayRetentionRisk(ctx context.Context, report RuntimeReplayReport) RuntimeReplayRetentionRisk {
	risk := RuntimeReplayRetentionRisk{
		Band: "CLEAR",
	}
	relevantSessions := runtimeReplayRelevantSessionIDs(report)
	if len(relevantSessions) == 0 {
		return risk
	}
	sessionSet := make(map[string]struct{}, len(relevantSessions))
	for _, sessionID := range relevantSessions {
		sessionSet[sessionID] = struct{}{}
	}

	candidates, err := s.ListSessionCompactionCandidates(ctx, SessionCompactionFilter{
		WorkspaceID: report.WorkspaceID,
		AgentID:     report.Filter.AgentID,
		ActiveOnly:  false,
		MinMessages: 1,
		MinTokens:   1,
		Limit:       maxInt(len(relevantSessions)*4, 20),
	})
	if err != nil {
		risk.Band = "UNKNOWN"
		risk.Reasons = appendOrderedUnique(risk.Reasons, "compaction_candidate_probe_failed")
	} else {
		for _, item := range candidates {
			if _, ok := sessionSet[strings.TrimSpace(item.SessionID)]; !ok {
				continue
			}
			risk.CompactionCandidateCount++
			risk.CandidateSessionIDs = appendOrderedUnique(risk.CandidateSessionIDs, strings.TrimSpace(item.SessionID))
		}
	}

	snapshotFilter := SessionCompactionSnapshotFilter{
		WorkspaceID: report.WorkspaceID,
		AgentID:     report.Filter.AgentID,
		Limit:       maxInt(len(relevantSessions)*4, 20),
	}
	if report.Filter.SessionID != "" {
		snapshotFilter.SessionID = report.Filter.SessionID
	}
	snapshots, err := s.ListSessionCompactionSnapshots(ctx, snapshotFilter)
	if err != nil {
		risk.Band = "UNKNOWN"
		risk.Reasons = appendOrderedUnique(risk.Reasons, "compaction_snapshot_probe_failed")
	} else {
		for _, item := range snapshots {
			if _, ok := sessionSet[strings.TrimSpace(item.SessionID)]; !ok {
				continue
			}
			risk.CompactionSnapshotCount++
			risk.SnapshotSessionIDs = appendOrderedUnique(risk.SnapshotSessionIDs, strings.TrimSpace(item.SessionID))
			if strings.TrimSpace(item.EpisodePackID) != "" {
				risk.EpisodePackCount++
			}
			if strings.TrimSpace(item.CreatedAt) > strings.TrimSpace(risk.LatestSnapshotAt) {
				risk.LatestSnapshotAt = strings.TrimSpace(item.CreatedAt)
			}
		}
	}

	if risk.CompactionCandidateCount > 0 {
		risk.Reasons = appendOrderedUnique(risk.Reasons, "session_compaction_candidate_present")
	}
	if risk.CompactionSnapshotCount > 0 {
		risk.Reasons = appendOrderedUnique(risk.Reasons, "session_compaction_snapshot_present")
	}
	if risk.CompactionSnapshotCount > risk.EpisodePackCount {
		risk.Reasons = appendOrderedUnique(risk.Reasons, "snapshot_without_episode_pack")
	}

	switch {
	case risk.Band == "UNKNOWN":
	case risk.CompactionSnapshotCount > risk.EpisodePackCount:
		risk.Band = "AT_RISK"
	case risk.CompactionSnapshotCount > 0:
		risk.Band = "COMPACTED"
	case risk.CompactionCandidateCount > 0:
		risk.Band = "WATCH"
	default:
		risk.Band = "CLEAR"
	}
	return risk
}

func runtimeReplayRelevantSessionIDs(report RuntimeReplayReport) []string {
	var sessionIDs []string
	if trimmed := strings.TrimSpace(report.Filter.SessionID); trimmed != "" {
		sessionIDs = append(sessionIDs, trimmed)
	}
	for _, session := range report.Sessions {
		sessionIDs = appendOrderedUnique(sessionIDs, strings.TrimSpace(session.SessionID))
	}
	return sessionIDs
}

func runtimeReplayRetentionFindings(risk RuntimeReplayRetentionRisk) []RuntimeReplayFinding {
	findings := make([]RuntimeReplayFinding, 0, len(risk.CandidateSessionIDs)+len(risk.SnapshotSessionIDs)+1)
	for _, sessionID := range risk.CandidateSessionIDs {
		findings = append(findings, RuntimeReplayFinding{
			Code:       "runtime_event_retention_compaction_candidate",
			Severity:   "info",
			Message:    "session is a compaction candidate on the adjacent session-compaction surface; inspectability only until retention policy hardens further",
			EntityType: "agent_session",
			EntityID:   sessionID,
		})
	}
	for _, sessionID := range risk.SnapshotSessionIDs {
		findings = append(findings, RuntimeReplayFinding{
			Code:       "runtime_event_retention_compacted_session",
			Severity:   "info",
			Message:    "session already has compaction snapshot artifacts; inspect episode-pack or summary-memory lineage for adjacent rolled-up context",
			EntityType: "agent_session",
			EntityID:   sessionID,
		})
	}
	if containsString(risk.Reasons, "snapshot_without_episode_pack") {
		findings = append(findings, RuntimeReplayFinding{
			Code:     "runtime_event_retention_snapshot_without_episode_pack",
			Severity: "info",
			Message:  "one or more compaction snapshots are missing episode-pack lineage, so adjacent rolled-up coverage may be incomplete",
		})
	}
	return findings
}

func runtimeReplayBuildFindingSummary(findings []RuntimeReplayFinding) RuntimeReplayFindingSummary {
	summary := RuntimeReplayFindingSummary{
		TotalFindings: len(findings),
	}
	for _, finding := range findings {
		switch strings.ToLower(strings.TrimSpace(finding.Severity)) {
		case "error":
			summary.ErrorFindingCount++
		case "warning":
			summary.WarningFindingCount++
		default:
			summary.InfoFindingCount++
		}
		switch runtimeReplayFindingFamily(finding.Code) {
		case "dedup_conflict":
			summary.DedupConflictCount++
		case "causal_order":
			summary.CausalOrderCount++
		case "missing_parent":
			summary.MissingParentCount++
		case "cycle":
			summary.CycleCount++
			switch strings.TrimSpace(finding.Code) {
			case "runtime_event_self_parent_ref":
				summary.CycleSelfParentCount++
			case "runtime_event_parent_ref_cycle":
				summary.CycleParentComponentCount++
			}
		case "scope_partial":
			summary.ScopePartialCount++
		case "retention":
			summary.RetentionFindingCount++
			switch strings.TrimSpace(finding.Code) {
			case "runtime_event_retention_compaction_candidate":
				summary.RetentionCompactionCandidateCount++
			case "runtime_event_retention_compacted_session":
				summary.RetentionCompactedSessionCount++
			case "runtime_event_retention_snapshot_without_episode_pack":
				summary.RetentionSnapshotWithoutEpisodePackCount++
			}
		case "execution_run_integrity":
			summary.ExecutionRunIntegrityCount++
			switch strings.TrimSpace(finding.Code) {
			case "missing_execution_run":
				summary.MissingExecutionRunCount++
			case "execution_run_out_of_sync":
				summary.ExecutionRunOutOfSyncCount++
			case "execution_run_without_steps":
				summary.ExecutionRunWithoutStepsCount++
			}
		case "claim_integrity":
			summary.ClaimIntegrityCount++
			switch strings.TrimSpace(finding.Code) {
			case "claim_missing_memory_link":
				summary.ClaimMissingMemoryLinkCount++
			case "claim_missing_review_queue":
				summary.ClaimMissingReviewQueueCount++
			case "stale_claim_review_queue":
				summary.StaleClaimReviewQueueCount++
			case "superseded_claim_missing_link":
				summary.SupersededClaimMissingLinkCount++
			case "duplicate_active_memory_claim":
				summary.DuplicateActiveMemoryClaimCount++
			}
		case "operator_queue_integrity":
			summary.OperatorQueueIntegrityCount++
			switch strings.TrimSpace(finding.Code) {
			case "overdue_operator_queue":
				summary.OverdueOperatorQueueCount++
			case "overdue_claim_review_unescalated":
				summary.OverdueClaimReviewUnescalatedCount++
			case "missing_operator_queue":
				summary.MissingOperatorQueueCount++
			case "stale_open_queue":
				summary.StaleOpenQueueCount++
			}
		case "payload_integrity":
			summary.PayloadIntegrityCount++
			switch strings.TrimSpace(finding.Code) {
			case "malformed_event_payload":
				summary.MalformedEventPayloadCount++
			}
		default:
			summary.OtherFindingCount++
		}
	}
	return summary
}

func runtimeReplayBuildProvenanceSummary(findings []RuntimeReplayFinding) RuntimeReplayProvenanceSummary {
	summary := RuntimeReplayProvenanceSummary{}
	for _, finding := range findings {
		hasSourceEvent := strings.TrimSpace(finding.SourceEventID) != ""
		hasSourceDedupKey := strings.TrimSpace(finding.SourceDedupKey) != ""
		hasRootCause := strings.TrimSpace(finding.SourceRootCauseID) != ""
		hasProvenanceGroup := strings.TrimSpace(finding.SourceProvenanceGroupID) != ""
		hasParentRefs := strings.TrimSpace(finding.SourceParentRefsJSON) != "" && strings.TrimSpace(finding.SourceParentRefsJSON) != "[]"
		if hasSourceEvent {
			summary.TotalFindingsWithSourceEvent++
		}
		if hasSourceDedupKey {
			summary.FindingsWithSourceDedupKey++
		}
		if hasRootCause {
			summary.FindingsWithRootCauseID++
		}
		if hasProvenanceGroup {
			summary.FindingsWithProvenanceGroupID++
		}
		if hasParentRefs {
			summary.FindingsWithParentRefs++
		}
		if hasSourceEvent && hasRootCause && hasProvenanceGroup && hasParentRefs {
			summary.FullLineageFieldFindingCount++
		}
	}
	return summary
}

func runtimeReplayFindingFamily(code string) string {
	switch strings.TrimSpace(code) {
	case "runtime_event_dedup_conflict":
		return "dedup_conflict"
	case "runtime_event_parent_ref_out_of_order":
		return "causal_order"
	case "runtime_event_parent_ref_missing":
		return "missing_parent"
	case "runtime_event_parent_ref_cycle", "runtime_event_self_parent_ref":
		return "cycle"
	case "replay_scope_partial":
		return "scope_partial"
	case "unified_control_effective_snapshot_rollback_trace_window_incomplete":
		return "scope_partial"
	case "missing_execution_run", "execution_run_out_of_sync", "execution_run_without_steps":
		return "execution_run_integrity"
	case "claim_missing_memory_link", "claim_missing_review_queue", "stale_claim_review_queue", "superseded_claim_missing_link", "duplicate_active_memory_claim":
		return "claim_integrity"
	case "overdue_operator_queue", "overdue_claim_review_unescalated", "missing_operator_queue", "stale_open_queue":
		return "operator_queue_integrity"
	case "malformed_event_payload":
		return "payload_integrity"
	default:
		if strings.HasPrefix(strings.TrimSpace(code), "runtime_event_retention_") {
			return "retention"
		}
		return "other"
	}
}

func replaySessionSlice(items map[string]*RuntimeReplaySession) []RuntimeReplaySession {
	out := make([]RuntimeReplaySession, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].SessionID < out[j].SessionID
	})
	return out
}

func runtimeReplayCausalOrder(events []RuntimeEventRecord) ([]RuntimeEventRecord, map[string]struct{}) {
	if len(events) < 2 {
		return events, nil
	}

	indexByEventID := make(map[string]int, len(events))
	for idx, event := range events {
		if eventID := strings.TrimSpace(event.EventID); eventID != "" {
			indexByEventID[eventID] = idx
		}
	}

	indegree := make([]int, len(events))
	children := make(map[string][]int, len(events))
	for idx, event := range events {
		eventID := strings.TrimSpace(event.EventID)
		if eventID == "" {
			continue
		}
		seenParents := map[string]struct{}{}
		for _, parentID := range runtimeReplayParentRefIDs(runtimeReplayEventLineage(event).ParentRefsJSON) {
			parentID = strings.TrimSpace(parentID)
			switch {
			case parentID == "", parentID == eventID:
				continue
			}
			_, ok := indexByEventID[parentID]
			if !ok {
				continue
			}
			if _, duplicate := seenParents[parentID]; duplicate {
				continue
			}
			seenParents[parentID] = struct{}{}
			indegree[idx]++
			children[parentID] = append(children[parentID], idx)
		}
	}

	ready := make([]int, 0, len(events))
	for idx := range events {
		if indegree[idx] == 0 {
			ready = append(ready, idx)
		}
	}
	sort.Ints(ready)

	ordered := make([]RuntimeEventRecord, 0, len(events))
	processed := make([]bool, len(events))
	for len(ready) > 0 {
		idx := ready[0]
		ready = ready[1:]
		if processed[idx] {
			continue
		}
		processed[idx] = true
		ordered = append(ordered, events[idx])

		eventID := strings.TrimSpace(events[idx].EventID)
		for _, childIdx := range children[eventID] {
			indegree[childIdx]--
			if indegree[childIdx] == 0 {
				ready = append(ready, childIdx)
				sort.Ints(ready)
			}
		}
	}

	var cycleAffected map[string]struct{}
	if len(ordered) != len(events) {
		cycleAffected = map[string]struct{}{}
		for idx, event := range events {
			if processed[idx] {
				continue
			}
			if eventID := strings.TrimSpace(event.EventID); eventID != "" {
				cycleAffected[eventID] = struct{}{}
			}
			ordered = append(ordered, event)
		}
	}

	return ordered, cycleAffected
}

func replayQueueSlice(items map[string]*RuntimeReplayQueue) []RuntimeReplayQueue {
	out := make([]RuntimeReplayQueue, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].QueueID < out[j].QueueID
	})
	return out
}

func replayClaimSlice(items map[string]*RuntimeReplayClaim) []RuntimeReplayClaim {
	out := make([]RuntimeReplayClaim, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].ClaimID < out[j].ClaimID
	})
	return out
}

func replayWorkspaceMemorySlice(items map[string]*RuntimeReplayWorkspaceMemory) []RuntimeReplayWorkspaceMemory {
	out := make([]RuntimeReplayWorkspaceMemory, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].MemoryID < out[j].MemoryID
	})
	return out
}

func replayExecutionRunSlice(items map[string]*RuntimeReplayExecutionRun) []RuntimeReplayExecutionRun {
	out := make([]RuntimeReplayExecutionRun, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].RunID < out[j].RunID
	})
	return out
}

func replayEffectiveControlsSlice(items map[string]*EffectiveControlsRecord, referenceTime *time.Time) []EffectiveControlsRecord {
	out := make([]EffectiveControlsRecord, 0, len(items))
	referenceAt := ""
	if referenceTime != nil {
		referenceAt = referenceTime.UTC().Format(time.RFC3339Nano)
	}
	for _, item := range items {
		record := *item
		if referenceAt != "" {
			record.Expired = effectiveControlsExpired(record.ExpiresAt, referenceAt)
			record.Pending = effectiveControlsPending(record.GeneratedAt, referenceAt)
			applyEffectiveControlsTemporalContract(&record, referenceAt, "")
		}
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		if out[i].ProtoClusterID != out[j].ProtoClusterID {
			return out[i].ProtoClusterID < out[j].ProtoClusterID
		}
		return out[i].WorkspaceID < out[j].WorkspaceID
	})
	return out
}

func replayUnifiedControlSnapshotSlice(items map[string]*RuntimeReplayUnifiedControlSnapshot) []RuntimeReplayUnifiedControlSnapshot {
	out := make([]RuntimeReplayUnifiedControlSnapshot, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].SnapshotKey < out[j].SnapshotKey
	})
	return out
}

func replayMemoryInvalidationSlice(items map[string]*RuntimeReplayMemoryInvalidation) []RuntimeReplayMemoryInvalidation {
	out := make([]RuntimeReplayMemoryInvalidation, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].InvalidationID < out[j].InvalidationID
	})
	return out
}

func replayCapabilityPolicySlice(items map[string]*CapabilityPolicyRecord) []CapabilityPolicyRecord {
	out := make([]CapabilityPolicyRecord, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		return out[i].PolicyID < out[j].PolicyID
	})
	return out
}

func replayControlCommandSlice(items map[string]*ControlCommandRecord) []ControlCommandRecord {
	out := make([]ControlCommandRecord, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RequestedAt != out[j].RequestedAt {
			return out[i].RequestedAt > out[j].RequestedAt
		}
		return out[i].CommandID < out[j].CommandID
	})
	return out
}

func runtimeReplayReferenceTime(report RuntimeReplayReport) *time.Time {
	var latest time.Time
	for _, event := range report.Events {
		if ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(event.CreatedAt)); err == nil && ts.After(latest) {
			latest = ts
		}
	}
	if latest.IsZero() {
		return nil
	}
	return &latest
}

func operatorQueueIsOverdue(queue RuntimeReplayQueue, referenceTime *time.Time) bool {
	if normalizeOperatorQueueStatus(queue.Status) != "OPEN" || referenceTime == nil || queue.DueAt == nil {
		return false
	}
	dueAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*queue.DueAt))
	if err != nil {
		return false
	}
	return dueAt.Before(*referenceTime)
}

func replaySessionState(items map[string]*RuntimeReplaySession, sessionID string) *RuntimeReplaySession {
	sessionID = strings.TrimSpace(sessionID)
	if items[sessionID] == nil {
		items[sessionID] = &RuntimeReplaySession{SessionID: sessionID}
	}
	return items[sessionID]
}

func replayQueueState(items map[string]*RuntimeReplayQueue, queueID string) *RuntimeReplayQueue {
	queueID = strings.TrimSpace(queueID)
	if items[queueID] == nil {
		items[queueID] = &RuntimeReplayQueue{QueueID: queueID}
	}
	return items[queueID]
}

func replayClaimState(items map[string]*RuntimeReplayClaim, claimID string) *RuntimeReplayClaim {
	claimID = strings.TrimSpace(claimID)
	if items[claimID] == nil {
		items[claimID] = &RuntimeReplayClaim{ClaimID: claimID}
	}
	return items[claimID]
}

func replayWorkspaceMemoryState(items map[string]*RuntimeReplayWorkspaceMemory, memoryID string) *RuntimeReplayWorkspaceMemory {
	memoryID = strings.TrimSpace(memoryID)
	if items[memoryID] == nil {
		items[memoryID] = &RuntimeReplayWorkspaceMemory{MemoryID: memoryID}
	}
	return items[memoryID]
}

func replayExecutionRunState(items map[string]*RuntimeReplayExecutionRun, runID string) *RuntimeReplayExecutionRun {
	runID = strings.TrimSpace(runID)
	if items[runID] == nil {
		items[runID] = &RuntimeReplayExecutionRun{
			RunID:            runID,
			PhaseCounts:      map[string]int{},
			StepStatusCounts: map[string]int{},
		}
	}
	return items[runID]
}

func replayCapabilityPolicyState(items map[string]*CapabilityPolicyRecord, policyID string) *CapabilityPolicyRecord {
	if items[policyID] == nil {
		items[policyID] = &CapabilityPolicyRecord{PolicyID: policyID}
	}
	return items[policyID]
}

func replayControlCommandState(items map[string]*ControlCommandRecord, commandID string) *ControlCommandRecord {
	if items[commandID] == nil {
		items[commandID] = &ControlCommandRecord{CommandID: commandID}
	}
	return items[commandID]
}

func replayEffectiveControlsState(items map[string]*EffectiveControlsRecord, scopeKey string) *EffectiveControlsRecord {
	if items[scopeKey] == nil {
		items[scopeKey] = &EffectiveControlsRecord{}
	}
	return items[scopeKey]
}

func replayUnifiedControlSnapshotState(items map[string]*RuntimeReplayUnifiedControlSnapshot, snapshotKey string) *RuntimeReplayUnifiedControlSnapshot {
	if items[snapshotKey] == nil {
		items[snapshotKey] = &RuntimeReplayUnifiedControlSnapshot{SnapshotKey: snapshotKey}
	}
	return items[snapshotKey]
}

func replayMemoryInvalidationState(items map[string]*RuntimeReplayMemoryInvalidation, invalidationID string) *RuntimeReplayMemoryInvalidation {
	if items[invalidationID] == nil {
		items[invalidationID] = &RuntimeReplayMemoryInvalidation{InvalidationID: invalidationID}
	}
	return items[invalidationID]
}

func replayUnifiedControlSnapshotKey(entityID, eventType string) string {
	return strings.TrimSpace(entityID) + "|" + strings.TrimSpace(eventType)
}

func replayMemoryInvalidationTypedEventType(eventType string) string {
	switch strings.TrimSpace(eventType) {
	case "memory.invalidation_enqueued":
		return "MEMORY_INVALIDATION"
	case "memory.invalidation_delivered":
		return "MEMORY_INVALIDATION_DELIVERED"
	case "memory.invalidation_acked":
		return "MEMORY_INVALIDATION_ACK"
	case "memory.invalidation_failed":
		return "MEMORY_INVALIDATION_FAILED"
	case "memory.invalidation_dead_lettered":
		return "MEMORY_INVALIDATION_DEAD_LETTER"
	case "memory.invalidation_refreshed":
		return "MEMORY_INVALIDATION_REFRESHED"
	case "memory.invalidation_requeued":
		return "MEMORY_INVALIDATION_REQUEUE"
	default:
		return ""
	}
}

func replayMemoryInvalidationDerivedState(eventType string) string {
	switch strings.TrimSpace(eventType) {
	case "memory.invalidation_enqueued", "memory.invalidation_delivered", "memory.invalidation_failed", "memory.invalidation_refreshed", "memory.invalidation_requeued":
		return "OPEN"
	case "memory.invalidation_acked":
		return "ACKED"
	case "memory.invalidation_dead_lettered":
		return "DEAD_LETTER"
	default:
		return ""
	}
}

func runtimeReplayUnifiedControlTraceFindings(report RuntimeReplayReport, sources runtimeReplayEvaluationSources) []RuntimeReplayFinding {
	effectiveByScope := make(map[string]EffectiveControlsRecord, len(report.EffectiveControls))
	for _, record := range report.EffectiveControls {
		effectiveByScope[effectiveControlsRuntimeEntityID(record)] = record
	}
	eventByID := make(map[string]RuntimeEventRecord, len(report.Events))
	for _, event := range report.Events {
		if eventID := strings.TrimSpace(event.EventID); eventID != "" {
			eventByID[eventID] = event
		}
	}

	advisoryByEntity := make(map[string]RuntimeReplayUnifiedControlSnapshot)
	effectiveByEntity := make(map[string]RuntimeReplayUnifiedControlSnapshot)
	for _, snapshot := range report.UnifiedSnapshots {
		entityID := strings.TrimSpace(snapshot.EntityID)
		if entityID == "" {
			continue
		}
		if snapshot.AdvisoryOnly {
			if _, exists := advisoryByEntity[entityID]; !exists {
				advisoryByEntity[entityID] = snapshot
			}
			continue
		}
		if _, exists := effectiveByEntity[entityID]; !exists {
			effectiveByEntity[entityID] = snapshot
		}
	}

	findings := make([]RuntimeReplayFinding, 0, len(effectiveByEntity))
	windowIncomplete := !report.Scope.Authoritative
	for entityID, effectiveSnapshot := range effectiveByEntity {
		scopeKey := runtimeReplayUnifiedControlSnapshotScopeKey(effectiveSnapshot)
		record, hasCurrentBasis := effectiveByScope[scopeKey]
		currentBasisNonLive := !hasCurrentBasis || record.Pending || record.Expired

		if advisorySnapshot, ok := advisoryByEntity[entityID]; ok && runtimeReplaySnapshotHappenedLater(eventByID, advisorySnapshot, effectiveSnapshot) {
			if reason, ok := runtimeReplayUnifiedControlRollbackReason(advisorySnapshot); ok {
				if currentBasisNonLive {
					findings = append(findings, replayFindingForEntity(
						"unified_control_effective_snapshot_rolled_back",
						"info",
						fmt.Sprintf("effective snapshot for %s later fell back to advisory posture: %s", entityID, reason),
						"instrumentation_unified_control",
						entityID,
						sources.unifiedSnapshots[strings.TrimSpace(advisorySnapshot.SnapshotKey)],
					))
				} else {
					findings = append(findings, replayFindingForEntity(
						"unified_control_effective_snapshot_rollback_trace_unexplained",
						"warning",
						fmt.Sprintf("later advisory snapshot for %s suggests rollback (%s) but the current effective-controls basis is still live", entityID, reason),
						"instrumentation_unified_control",
						entityID,
						sources.unifiedSnapshots[strings.TrimSpace(advisorySnapshot.SnapshotKey)],
					))
				}
			} else {
				findings = append(findings, replayFindingForEntity(
					"unified_control_effective_snapshot_rollback_trace_unexplained",
					"warning",
					fmt.Sprintf("later advisory snapshot for %s does not explain why the earlier effective snapshot was demoted", entityID),
					"instrumentation_unified_control",
					entityID,
					sources.unifiedSnapshots[strings.TrimSpace(advisorySnapshot.SnapshotKey)],
				))
			}
			continue
		}

		if scopeKey == "" {
			continue
		}
		if windowIncomplete {
			findings = append(findings, replayFindingForEntity(
				"unified_control_effective_snapshot_rollback_trace_window_incomplete",
				"info",
				fmt.Sprintf("bounded replay scope cannot confirm rollback lineage for %s outside the current window", entityID),
				"instrumentation_unified_control",
				entityID,
				sources.unifiedSnapshots[strings.TrimSpace(effectiveSnapshot.SnapshotKey)],
			))
			continue
		}
		if !hasCurrentBasis {
			findings = append(findings, replayFindingForEntity(
				"unified_control_effective_snapshot_missing_rollback_trace",
				"warning",
				fmt.Sprintf("effective snapshot for %s has no current effective_controls basis and no later advisory rollback trace", entityID),
				"instrumentation_unified_control",
				entityID,
				sources.unifiedSnapshots[strings.TrimSpace(effectiveSnapshot.SnapshotKey)],
			))
			continue
		}
		if record.Pending || record.Expired {
			reason := "no longer live"
			switch {
			case record.Pending:
				reason = "pending"
			case record.Expired:
				reason = "expired"
			}
			findings = append(findings, replayFindingForEntity(
				"unified_control_effective_snapshot_missing_rollback_trace",
				"warning",
				fmt.Sprintf("effective snapshot for %s is no longer backed by live effective controls (%s) and no later advisory rollback trace was replayed", entityID, reason),
				"instrumentation_unified_control",
				entityID,
				sources.unifiedSnapshots[strings.TrimSpace(effectiveSnapshot.SnapshotKey)],
			))
		}
	}

	return findings
}

func runtimeReplaySnapshotHappenedLater(events map[string]RuntimeEventRecord, candidate, reference RuntimeReplayUnifiedControlSnapshot) bool {
	candidateSeq := runtimeReplaySnapshotIngestSeq(events, candidate)
	referenceSeq := runtimeReplaySnapshotIngestSeq(events, reference)
	switch {
	case candidateSeq > 0 && referenceSeq > 0:
		return candidateSeq > referenceSeq
	case candidateSeq > 0:
		return true
	case referenceSeq > 0:
		return false
	default:
		return candidate.UpdatedAt > reference.UpdatedAt
	}
}

func runtimeReplaySnapshotIngestSeq(events map[string]RuntimeEventRecord, snapshot RuntimeReplayUnifiedControlSnapshot) int64 {
	if event, ok := events[strings.TrimSpace(snapshot.LastEventID)]; ok {
		return event.IngestSeq
	}
	return 0
}

func runtimeReplayUnifiedControlRollbackReason(snapshot RuntimeReplayUnifiedControlSnapshot) (string, bool) {
	audit := snapshot.EffectiveControlsAudit
	if audit == nil {
		return "", false
	}
	switch {
	case !audit.Found:
		if scopeSource := strings.TrimSpace(audit.ScopeSource); scopeSource != "" {
			return fmt.Sprintf("effective controls were not found (%s)", scopeSource), true
		}
		return "effective controls were not found", true
	case audit.Pending:
		return "effective controls are still pending", true
	case audit.Expired:
		return "effective controls expired", true
	case strings.TrimSpace(audit.ScopeSource) == "workspace_fallback":
		return "scope resolution fell back to workspace controls", true
	case strings.TrimSpace(audit.ScopeSource) == "candidate_only":
		return "candidate-only controls remained advisory", true
	case !audit.Live:
		return "effective controls are not live", true
	default:
		return "", false
	}
}

func runtimeReplayUnifiedControlSnapshotScopeKey(snapshot RuntimeReplayUnifiedControlSnapshot) string {
	if snapshot.EffectiveControlsAudit != nil {
		switch strings.TrimSpace(snapshot.EffectiveControlsAudit.ScopeSource) {
		case "workspace", "workspace_fallback":
			if workspaceID := strings.TrimSpace(snapshot.WorkspaceID); workspaceID != "" {
				return "workspace:" + workspaceID
			}
		case "proto_cluster":
			if clusterID := strings.TrimSpace(snapshot.ProtoClusterID); clusterID != "" {
				return "proto_cluster:" + clusterID
			}
		}
	}
	if clusterID := strings.TrimSpace(snapshot.ProtoClusterID); clusterID != "" {
		return "proto_cluster:" + clusterID
	}
	if workspaceID := strings.TrimSpace(snapshot.WorkspaceID); workspaceID != "" {
		return "workspace:" + workspaceID
	}
	return ""
}

func replayUnifiedControlSnapshotPayloadConsistent(payload replayUnifiedControlSnapshotPayload) bool {
	if payload.Resolved != payload.Report.Resolved {
		return false
	}
	if resolvedFrom := strings.TrimSpace(payload.ResolvedFrom); resolvedFrom != "" && resolvedFrom != strings.TrimSpace(payload.Report.ResolvedFrom) {
		return false
	}
	audit := payload.Report.EffectiveControlsAudit
	if audit == nil {
		return !(payload.EffectiveControlsFound || payload.EffectiveControlsLive || payload.EffectiveControlsExpired || payload.EffectiveControlsPending ||
			strings.TrimSpace(payload.EffectiveControlsScopeSource) != "")
	}
	if payload.EffectiveControlsFound != audit.Found ||
		payload.EffectiveControlsLive != audit.Live ||
		payload.EffectiveControlsExpired != audit.Expired ||
		payload.EffectiveControlsPending != audit.Pending {
		return false
	}
	if scopeSource := strings.TrimSpace(payload.EffectiveControlsScopeSource); scopeSource != "" && scopeSource != strings.TrimSpace(audit.ScopeSource) {
		return false
	}
	return true
}

func decodeReplayPayload(event RuntimeEventRecord, dst any) bool {
	payload := strings.TrimSpace(event.PayloadJSON)
	if payload == "" {
		return false
	}
	return json.Unmarshal([]byte(payload), dst) == nil
}

func malformedReplayEvent(event RuntimeEventRecord, action string) RuntimeReplayFinding {
	return replayFindingFromEvent(event, "malformed_event_payload", "error", action)
}

func replayFindingSourceFromEvent(event RuntimeEventRecord) runtimeReplayFindingSource {
	lineage := runtimeReplayEventLineage(event)
	return runtimeReplayFindingSource{
		EventType:         strings.TrimSpace(event.EventType),
		EventID:           strings.TrimSpace(event.EventID),
		DedupKey:          lineage.DedupKey,
		RootCauseID:       lineage.RootCauseID,
		ProvenanceGroupID: lineage.ProvenanceGroupID,
		ParentRefsJSON:    lineage.ParentRefsJSON,
	}
}

func replayFindingForEntity(code, severity, message, entityType, entityID string, source runtimeReplayFindingSource) RuntimeReplayFinding {
	return RuntimeReplayFinding{
		Code:                    code,
		Severity:                severity,
		Message:                 message,
		EntityType:              entityType,
		EntityID:                entityID,
		SourceEventType:         source.EventType,
		SourceEventID:           source.EventID,
		SourceDedupKey:          source.DedupKey,
		SourceRootCauseID:       source.RootCauseID,
		SourceProvenanceGroupID: source.ProvenanceGroupID,
		SourceParentRefsJSON:    source.ParentRefsJSON,
	}
}

func replayFindingFromEvent(event RuntimeEventRecord, code, severity, message string) RuntimeReplayFinding {
	return replayFindingForEntity(code, severity, message, event.EntityType, event.EntityID, replayFindingSourceFromEvent(event))
}

func replayEventOrderingFindings(event RuntimeEventRecord, eventIndex int, eventOrder map[string]int, cycleAffectedEvents map[string]struct{}) []RuntimeReplayFinding {
	parentRefs := runtimeReplayParentRefIDs(runtimeReplayEventLineage(event).ParentRefsJSON)
	if len(parentRefs) == 0 {
		return nil
	}
	findings := make([]RuntimeReplayFinding, 0, len(parentRefs))
	cycleFindingEmitted := false
	_, cycleAffected := cycleAffectedEvents[strings.TrimSpace(event.EventID)]
	for _, parentID := range parentRefs {
		parentIndex, parentFound := eventOrder[parentID]
		switch {
		case parentID == "":
			continue
		case parentID == strings.TrimSpace(event.EventID):
			findings = append(findings, replayFindingFromEvent(
				event,
				"runtime_event_self_parent_ref",
				"error",
				fmt.Sprintf("runtime event references itself as parent: %s", parentID),
			))
		case !parentFound:
			findings = append(findings, replayFindingFromEvent(
				event,
				"runtime_event_parent_ref_missing",
				"warning",
				fmt.Sprintf("runtime event parent_ref %s is missing from replay scope", parentID),
			))
		case parentIndex > eventIndex:
			if cycleAffected && !cycleFindingEmitted {
				findings = append(findings, replayFindingFromEvent(
					event,
					"runtime_event_parent_ref_cycle",
					"warning",
					"runtime event parent_refs participate in a cycle-affected component; replay fell back to ingest order for those edges",
				))
				cycleFindingEmitted = true
				continue
			}
			findings = append(findings, replayFindingFromEvent(
				event,
				"runtime_event_parent_ref_out_of_order",
				"warning",
				fmt.Sprintf("runtime event parent_ref %s appears later in ingest order", parentID),
			))
		}
	}
	return findings
}

func runtimeReplayEventIDs(events []RuntimeEventRecord) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		if eventID := strings.TrimSpace(event.EventID); eventID != "" {
			out = append(out, eventID)
		}
	}
	return out
}

func runtimeReplayParentRefIDs(raw string) []string {
	refs, err := parseRuntimeEventParentRefs(raw)
	if err == nil {
		return refs
	}
	canonical := runtimeReplayCanonicalJSON(raw, "[]")
	var fallback []string
	if err := json.Unmarshal([]byte(canonical), &fallback); err != nil {
		return nil
	}
	out := make([]string, 0, len(fallback))
	for _, ref := range fallback {
		if trimmed := strings.TrimSpace(ref); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func replayFindingSeverityRank(severity string) int {
	if strings.EqualFold(strings.TrimSpace(severity), "error") {
		return 0
	}
	return 1
}

func stringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
