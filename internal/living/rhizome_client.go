package living

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/sessionmemory"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// ── Domain types (independent of storage layer) ─────────────────────

// Task represents a task as seen by the living agent.
type Task struct {
	TaskID      string `json:"task_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Priority    string `json:"priority"`
	Status      string `json:"status"`
	TaskKind    string `json:"task_kind"`
	OwnerUserID string `json:"owner_user_id"`
	CreatedAt   string `json:"created_at"`
}

// Update represents an agent update record.
type Update struct {
	UpdateID      string `json:"update_id"`
	AgentID       string `json:"agent_id"`
	UpdateType    string `json:"update_type"`
	Summary       string `json:"summary"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	RequiresHuman bool   `json:"requires_human,omitempty"`
	CreatedAt     string `json:"created_at"`
}

// Message represents an agent message.
type Message struct {
	MessageID   string `json:"message_id"`
	FromAgentID string `json:"from_agent_id"`
	ToAgentID   string `json:"to_agent_id"`
	Channel     string `json:"channel"`
	Content     string `json:"content"`
	CreatedAt   string `json:"created_at"`
}

// SessionEventInput captures an explicit session coordination event sourced
// from the current runtime.
type SessionEventInput struct {
	WorkspaceID         string                         `json:"workspace_id,omitempty"`
	EventType           string                         `json:"event_type"`
	SessionID           string                         `json:"session_id"`
	AgentID             string                         `json:"agent_id"`
	TaskID              string                         `json:"task_id,omitempty"`
	Summary             string                         `json:"summary"`
	Status              string                         `json:"status,omitempty"`
	OwnerScope          string                         `json:"owner_scope,omitempty"`
	BlockedOn           []model.AgentUpdateBlockedRef  `json:"blocked_on,omitempty"`
	DecisionNeededFrom  string                         `json:"decision_needed_from,omitempty"`
	DecisionType        string                         `json:"decision_type,omitempty"`
	KeepSessionActive   *bool                          `json:"keep_session_active,omitempty"`
	HandoffTo           string                         `json:"handoff_to,omitempty"`
	RelatedDocKeys      []string                       `json:"related_doc_keys,omitempty"`
	RelatedArtifactRefs []model.AgentUpdateArtifactRef `json:"related_artifact_refs,omitempty"`
	UpdatedAt           string                         `json:"updated_at,omitempty"`
}

type WorkspaceMemoryInput struct {
	WorkspaceID string   `json:"workspace_id,omitempty"`
	MemoryID    string   `json:"memory_id,omitempty"`
	MemoryType  string   `json:"memory_type,omitempty"`
	Title       string   `json:"title,omitempty"`
	Body        string   `json:"body"`
	Summary     string   `json:"summary,omitempty"`
	AgentID     string   `json:"agent_id,omitempty"`
	SessionID   string   `json:"session_id,omitempty"`
	TaskID      string   `json:"task_id,omitempty"`
	SourceKind  string   `json:"source_kind,omitempty"`
	SourceID    string   `json:"source_id,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Importance  float64  `json:"importance,omitempty"`
	Confidence  float64  `json:"confidence,omitempty"`
}

type WorkspaceMemorySearchFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Query       string `json:"query,omitempty"`
	MemoryType  string `json:"memory_type,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	SourceKind  string `json:"source_kind,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type WorkspaceMemoryPacketFilter struct {
	WorkspaceID    string                     `json:"workspace_id,omitempty"`
	TaskID         string                     `json:"task_id,omitempty"`
	SessionID      string                     `json:"session_id,omitempty"`
	AgentID        string                     `json:"agent_id,omitempty"`
	DocKeys        []string                   `json:"doc_keys,omitempty"`
	ArtifactRefs   []string                   `json:"artifact_refs,omitempty"`
	IncludeAllDocs bool                       `json:"include_all_docs,omitempty"`
	Budget         *sqlite.MemoryPacketBudget `json:"budget,omitempty"`
}

type WorkspaceMemoryPromotionFilter struct {
	WorkspaceID   string `json:"workspace_id,omitempty"`
	PromotionID   string `json:"promotion_id,omitempty"`
	State         string `json:"state,omitempty"`
	CandidateKind string `json:"candidate_kind,omitempty"`
	CandidateType string `json:"candidate_type,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

type WorkspaceMemoryCoherenceFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	ReportScope string `json:"report_scope,omitempty"`
}

type WorkspaceMemoryInvalidationListFilter struct {
	WorkspaceID       string `json:"workspace_id,omitempty"`
	AgentID           string `json:"agent_id,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	IncludeAcked      bool   `json:"include_acked,omitempty"`
	IncludeDeadLetter bool   `json:"include_dead_letter,omitempty"`
	Limit             int    `json:"limit,omitempty"`
}

type WorkspaceMemoryInvalidationListResult struct {
	WorkspaceID   string                            `json:"workspace_id"`
	AgentID       string                            `json:"agent_id"`
	TimeAuthority sqlite.WorkspaceTimeAuthority     `json:"time_authority"`
	Items         []sqlite.MemoryInvalidationRecord `json:"items"`
	Count         int                               `json:"count"`
}

type WorkspaceMemoryInvalidationGetFilter struct {
	WorkspaceID    string `json:"workspace_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	InvalidationID string `json:"invalidation_id,omitempty"`
}

type WorkspaceMemoryInvalidationCursorFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
}

type WorkspaceMemoryGraphListFilter struct {
	WorkspaceID     string `json:"workspace_id,omitempty"`
	MemoryType      string `json:"memory_type,omitempty"`
	MemoryLayer     string `json:"memory_layer,omitempty"`
	Visibility      string `json:"visibility,omitempty"`
	EpistemicStatus string `json:"epistemic_status,omitempty"`
	LifecycleState  string `json:"lifecycle_state,omitempty"`
	OriginKind      string `json:"origin_kind,omitempty"`
	OriginID        string `json:"origin_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

type WorkspaceMemoryGraphListResult struct {
	WorkspaceID      string                             `json:"workspace_id"`
	TimeAuthority    sqlite.WorkspaceTimeAuthority      `json:"time_authority"`
	BoundaryContract sqlite.MemoryShapeBoundaryContract `json:"boundary_contract"`
	Items            []sqlite.MemoryGraphNodeRecord     `json:"items"`
	Count            int                                `json:"count"`
}

type WorkspaceMemoryGraphGetFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	MemoryID    string `json:"memory_id,omitempty"`
}

type WorkspaceMemoryNodeSearchFilter struct {
	WorkspaceID     string `json:"workspace_id,omitempty"`
	Query           string `json:"query,omitempty"`
	MemoryType      string `json:"memory_type,omitempty"`
	MemoryLayer     string `json:"memory_layer,omitempty"`
	Visibility      string `json:"visibility,omitempty"`
	EpistemicStatus string `json:"epistemic_status,omitempty"`
	LifecycleState  string `json:"lifecycle_state,omitempty"`
	OriginKind      string `json:"origin_kind,omitempty"`
	OriginID        string `json:"origin_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

type WorkspaceRSPStateFilter struct {
	WorkspaceID    string   `json:"workspace_id,omitempty"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	FrontierLimit  int      `json:"frontier_limit,omitempty"`
}

type WorkspaceRSPForecastFilter struct {
	WorkspaceID    string   `json:"workspace_id,omitempty"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	FrontierLimit  int      `json:"frontier_limit,omitempty"`
}

type WorkspaceRSPBeliefFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	ClaimType   string `json:"claim_type,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type WorkspaceRSPBeliefClaimFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	ClaimID     string `json:"claim_id,omitempty"`
}

type WorkspaceRSPTelemetryFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type WorkspaceUnifiedControlFilter struct {
	WorkspaceID    string   `json:"workspace_id,omitempty"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	FrontierLimit  int      `json:"frontier_limit,omitempty"`
}

type WorkspaceControlReportFilter struct {
	WorkspaceID    string `json:"workspace_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type WorkspaceControlClusterFilter struct {
	WorkspaceID    string `json:"workspace_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
}

type WorkspaceControlStateFilter struct {
	WorkspaceID    string `json:"workspace_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Mode           string `json:"mode,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type WorkspaceControlStateClusterFilter struct {
	WorkspaceID    string `json:"workspace_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
}

type WorkspaceTensionFilter struct {
	WorkspaceID    string `json:"workspace_id,omitempty"`
	TensionType    string `json:"tension_type,omitempty"`
	LifecycleState string `json:"lifecycle_state,omitempty"`
	ReviewStatus   string `json:"review_status,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type WorkspaceTensionGetFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	TensionID   string `json:"tension_id,omitempty"`
}

type WorkspaceTensionListResult struct {
	WorkspaceID   string                        `json:"workspace_id"`
	TimeAuthority sqlite.WorkspaceTimeAuthority `json:"time_authority"`
	Items         []sqlite.TensionRecord        `json:"items"`
	Count         int                           `json:"count"`
}

type WorkspaceTensionFrontierResult struct {
	WorkspaceID   string                        `json:"workspace_id"`
	TimeAuthority sqlite.WorkspaceTimeAuthority `json:"time_authority"`
	Items         []sqlite.TensionFrontierItem  `json:"items"`
	Count         int                           `json:"count"`
}

type WorkspaceTensionAttachableFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
}

type WorkspaceTensionAttachableResult struct {
	WorkspaceID string                 `json:"workspace_id"`
	AgentID     string                 `json:"agent_id"`
	Items       []sqlite.ScoredTension `json:"items"`
	Count       int                    `json:"count"`
}

type WorkspaceRSPCapabilityFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
}

type WorkspaceEventsListFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	EventType   string `json:"event_type,omitempty"`
	EntityType  string `json:"entity_type,omitempty"`
	EntityID    string `json:"entity_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type WorkspaceEventsListResult struct {
	WorkspaceID   string                        `json:"workspace_id"`
	TimeAuthority sqlite.WorkspaceTimeAuthority `json:"time_authority"`
	Items         []sqlite.RuntimeEventRecord   `json:"items"`
	Count         int                           `json:"count"`
}

type WorkspaceEventsReplayFilter struct {
	WorkspaceID   string `json:"workspace_id,omitempty"`
	AgentID       string `json:"agent_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	TaskID        string `json:"task_id,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	IncludeEvents bool   `json:"include_events,omitempty"`
}

type WorkspaceCompactionSnapshotFilter struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type WorkspaceMemoryRecord struct {
	MemoryID    string   `json:"memory_id"`
	WorkspaceID string   `json:"workspace_id"`
	MemoryType  string   `json:"memory_type"`
	Title       string   `json:"title"`
	Body        string   `json:"body"`
	Summary     string   `json:"summary"`
	AgentID     string   `json:"agent_id,omitempty"`
	SessionID   string   `json:"session_id,omitempty"`
	TaskID      string   `json:"task_id,omitempty"`
	SourceKind  string   `json:"source_kind"`
	SourceID    string   `json:"source_id,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Importance  float64  `json:"importance"`
	Confidence  float64  `json:"confidence"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
}

type PromotedClaimEffects struct {
	Claim              *sqlite.KnowledgeClaimRecord `json:"claim,omitempty"`
	ClaimEvent         *sqlite.RuntimeEventRecord   `json:"claim_event,omitempty"`
	Queue              *sqlite.OperatorQueueRecord  `json:"queue,omitempty"`
	QueueEvent         *sqlite.RuntimeEventRecord   `json:"queue_event,omitempty"`
	InvalidationEvents []sqlite.RuntimeEventRecord  `json:"invalidation_events,omitempty"`
}

type WorkspaceMemoryWriteResult struct {
	Memory               WorkspaceMemoryRecord `json:"memory"`
	PromotedClaimEffects *PromotedClaimEffects `json:"promoted_claim_effects,omitempty"`
}

type SessionCompactionCandidate struct {
	SessionID         string `json:"session_id"`
	WorkspaceID       string `json:"workspace_id"`
	AgentID           string `json:"agent_id"`
	TaskID            string `json:"task_id,omitempty"`
	Status            string `json:"status"`
	MessageCount      int    `json:"message_count"`
	MessageTokens     int    `json:"message_tokens"`
	TotalInputTokens  int    `json:"total_input_tokens"`
	TotalOutputTokens int    `json:"total_output_tokens"`
	TotalTokens       int    `json:"total_tokens"`
	StartedAt         string `json:"started_at"`
	LastMessageAt     string `json:"last_message_at"`
}

// SessionAwareRhizomeClient is an optional extension for runtimes that can
// publish explicit task/session coordination signals.
type SessionAwareRhizomeClient interface {
	RecordSessionEvent(ctx context.Context, input SessionEventInput) error
}

type WorkspaceMemoryAwareRhizomeClient interface {
	RecordWorkspaceMemory(ctx context.Context, input WorkspaceMemoryInput) (WorkspaceMemoryRecord, error)
	ListWorkspaceMemory(ctx context.Context, filter WorkspaceMemorySearchFilter) ([]WorkspaceMemoryRecord, error)
	SearchWorkspaceMemory(ctx context.Context, filter WorkspaceMemorySearchFilter) ([]WorkspaceMemoryRecord, error)
}

type WorkspaceMemoryPacketAwareRhizomeClient interface {
	BuildMemoryKernelPacket(ctx context.Context, filter WorkspaceMemoryPacketFilter) (sqlite.MemoryKernelPacket, error)
	BuildMemoryShellPacket(ctx context.Context, filter WorkspaceMemoryPacketFilter) (sqlite.MemoryShellPacket, error)
}

type WorkspaceMemoryPromotionAwareRhizomeClient interface {
	GetMemoryPromotion(ctx context.Context, filter WorkspaceMemoryPromotionFilter) (sqlite.MemoryPromotionRecord, error)
	ListMemoryPromotions(ctx context.Context, filter WorkspaceMemoryPromotionFilter) ([]sqlite.MemoryPromotionRecord, error)
}

type WorkspaceMemoryCoherenceAwareRhizomeClient interface {
	GetMemoryCoherenceScope(ctx context.Context, filter WorkspaceMemoryCoherenceFilter) (sqlite.MemoryCoherenceScopeReport, error)
}

type WorkspaceMemoryInvalidationAwareRhizomeClient interface {
	ListMemoryInvalidations(ctx context.Context, filter WorkspaceMemoryInvalidationListFilter) (WorkspaceMemoryInvalidationListResult, error)
	GetMemoryInvalidation(ctx context.Context, filter WorkspaceMemoryInvalidationGetFilter) (sqlite.MemoryInvalidationRecord, error)
	GetMemoryInvalidationCursor(ctx context.Context, filter WorkspaceMemoryInvalidationCursorFilter) (sqlite.MemoryInvalidationCursorRecord, error)
}

type WorkspaceMemoryGraphAwareRhizomeClient interface {
	ListMemoryGraphNodes(ctx context.Context, filter WorkspaceMemoryGraphListFilter) (WorkspaceMemoryGraphListResult, error)
	GetMemoryGraphNode(ctx context.Context, filter WorkspaceMemoryGraphGetFilter) (sqlite.MemoryGraphNodeDetail, error)
}

type WorkspaceMemoryNodeSearchAwareRhizomeClient interface {
	SearchMemoryNodes(ctx context.Context, filter WorkspaceMemoryNodeSearchFilter) (sqlite.MemoryNodeSearchResult, error)
}

type WorkspaceRSPStateAwareRhizomeClient interface {
	GetRSPStateReport(ctx context.Context, filter WorkspaceRSPStateFilter) (sqlite.RSPStateReport, error)
}

type WorkspaceRSPForecastAwareRhizomeClient interface {
	GetRSPForecastReport(ctx context.Context, filter WorkspaceRSPForecastFilter) (sqlite.RSPForecastReport, error)
}

type WorkspaceRSPBeliefAwareRhizomeClient interface {
	GetRSPBeliefReport(ctx context.Context, filter WorkspaceRSPBeliefFilter) (sqlite.RSPBeliefReport, error)
	GetRSPBeliefClaim(ctx context.Context, filter WorkspaceRSPBeliefClaimFilter) (sqlite.RSPBeliefClaimReport, error)
}

type WorkspaceRSPTelemetryAwareRhizomeClient interface {
	GetRSPTelemetryDump(ctx context.Context, filter WorkspaceRSPTelemetryFilter) (sqlite.RSPTelemetryDump, error)
}

type WorkspaceUnifiedControlAwareRhizomeClient interface {
	GetUnifiedControlReport(ctx context.Context, filter WorkspaceUnifiedControlFilter) (sqlite.UnifiedControlReport, error)
}

type WorkspaceControlReportAwareRhizomeClient interface {
	GetControlReport(ctx context.Context, filter WorkspaceControlReportFilter) (sqlite.ControlReport, error)
}

type WorkspaceControlClusterAwareRhizomeClient interface {
	GetControlClusterDetail(ctx context.Context, filter WorkspaceControlClusterFilter) (sqlite.ControlClusterDetail, error)
}

type WorkspaceControlStateAwareRhizomeClient interface {
	GetControlStateReport(ctx context.Context, filter WorkspaceControlStateFilter) (sqlite.ClusterControlStateReport, error)
}

type WorkspaceControlStateClusterAwareRhizomeClient interface {
	GetControlStateClusterDetail(ctx context.Context, filter WorkspaceControlStateClusterFilter) (sqlite.ClusterControlStateDetail, error)
}

type WorkspaceTensionAwareRhizomeClient interface {
	ListTensions(ctx context.Context, filter WorkspaceTensionFilter) (WorkspaceTensionListResult, error)
	GetTension(ctx context.Context, filter WorkspaceTensionGetFilter) (sqlite.TensionDetail, error)
	ListTensionFrontier(ctx context.Context, filter WorkspaceTensionFilter) (WorkspaceTensionFrontierResult, error)
}

type WorkspaceTensionAttachableAwareRhizomeClient interface {
	ListAttachableTensions(ctx context.Context, filter WorkspaceTensionAttachableFilter) (WorkspaceTensionAttachableResult, error)
}

type WorkspaceRSPCapabilityAwareRhizomeClient interface {
	GetRSPCapabilityFlags(ctx context.Context, filter WorkspaceRSPCapabilityFilter) (sqlite.RSPCapabilityFlags, error)
}

type WorkspaceMemoryEffectsAwareRhizomeClient interface {
	RecordWorkspaceMemoryWithEffects(ctx context.Context, input WorkspaceMemoryInput) (WorkspaceMemoryWriteResult, error)
}

type CompactionAwareRhizomeClient interface {
	ListSessionCompactionCandidates(ctx context.Context, agentID string, minMessages, minTokens int) ([]SessionCompactionCandidate, error)
}

type CompactionSnapshotAwareRhizomeClient interface {
	ListSessionCompactionSnapshots(ctx context.Context, filter WorkspaceCompactionSnapshotFilter) ([]sqlite.SessionCompactionSnapshotRecord, error)
}

type WorkspaceEventsReplayAwareRhizomeClient interface {
	ReplayWorkspaceEvents(ctx context.Context, filter WorkspaceEventsReplayFilter) (sqlite.RuntimeReplayReport, error)
}

type WorkspaceEventsListAwareRhizomeClient interface {
	ListWorkspaceEvents(ctx context.Context, filter WorkspaceEventsListFilter) (WorkspaceEventsListResult, error)
}

// ── Interface ───────────────────────────────────────────────────────

// RhizomeClient defines the operations the living agent needs from Rhizome.
// It can be backed by a direct SQLite store (in-process) or by an HTTP
// JSON-RPC client (remote Rhizome server).
type RhizomeClient interface {
	// FetchTasks returns pending tasks in the workspace whose task_kind matches
	// one of the given taskTypes. If taskTypes is empty, all pending tasks are
	// returned.
	FetchTasks(ctx context.Context, taskTypes []string) ([]Task, error)

	// ClaimTask claims a task for the given agent.
	ClaimTask(ctx context.Context, taskID, agentID string) error

	// ReleaseTask releases a previously-claimed task.
	ReleaseTask(ctx context.Context, taskID, agentID, reason string) error

	// CompleteTask marks a task as completed by the agent.
	CompleteTask(ctx context.Context, taskID string, result string) error

	// FailTask marks a task as failed (blocks with an error message).
	FailTask(ctx context.Context, taskID string, errMsg string) error

	// GetTaskUpdates returns agent updates for the workspace created after
	// the given time.
	GetTaskUpdates(ctx context.Context, taskID string, since time.Time) ([]Update, error)

	// SendUpdate posts an agent update to the workspace.
	SendUpdate(ctx context.Context, agentID, workspaceID, updateType, summary, payload string) error

	// FetchMessages polls messages for the given agent created after `since`.
	FetchMessages(ctx context.Context, agentID string, since time.Time) ([]Message, error)

	// SendMessage sends a message from one agent to another.
	SendMessage(ctx context.Context, from, to, content string, taskID string) error

	// Heartbeat records an agent heartbeat.
	Heartbeat(ctx context.Context, agentID, status, summary string) error

	// EscalateTask posts an escalation update for a task.
	EscalateTask(ctx context.Context, taskID, reason string) error
}

// ── Direct (in-process) implementation ──────────────────────────────

// DirectRhizomeClient wraps a *sqlite.Store to satisfy RhizomeClient
// without going through HTTP. This is used when the agent runs in the
// same process as the Rhizome data store.
type DirectRhizomeClient struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
	mu          sync.Mutex
	messagePoll map[string]directMessageCursor
}

type directMessageCursor struct {
	CreatedAt string
	MessageID string
}

// NewDirectRhizomeClient creates a DirectRhizomeClient that operates
// within the given workspace.
func NewDirectRhizomeClient(store *sqlite.Store, workspaceID string) *DirectRhizomeClient {
	return &DirectRhizomeClient{
		store:       store,
		workspaceID: workspaceID,
		messagePoll: make(map[string]directMessageCursor),
	}
}

// compile-time check
var _ RhizomeClient = (*DirectRhizomeClient)(nil)
var _ SessionAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceMemoryAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceMemoryEffectsAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceMemoryPacketAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceMemoryPromotionAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceMemoryCoherenceAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceMemoryInvalidationAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceControlReportAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceControlClusterAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceControlStateAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceControlStateClusterAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceTensionAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceTensionAttachableAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ CompactionAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ CompactionSnapshotAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceEventsReplayAwareRhizomeClient = (*DirectRhizomeClient)(nil)
var _ WorkspaceEventsListAwareRhizomeClient = (*DirectRhizomeClient)(nil)

// FetchTasks lists workspace tasks with status PENDING and optionally
// filters by task_kind matching any of the given taskTypes.
func (c *DirectRhizomeClient) FetchTasks(ctx context.Context, taskTypes []string) ([]Task, error) {
	records, err := c.store.ListWorkspaceTasks(ctx, c.workspaceID)
	if err != nil {
		return nil, fmt.Errorf("fetch tasks: %w", err)
	}

	typeSet := make(map[string]struct{}, len(taskTypes))
	for _, t := range taskTypes {
		typeSet[t] = struct{}{}
	}

	var tasks []Task
	for _, r := range records {
		if r.Status != "PENDING" {
			continue
		}
		if !claimAvailable(r.ClaimStatus) {
			continue
		}
		if len(typeSet) > 0 {
			if _, ok := typeSet[r.TaskKind]; !ok {
				continue
			}
		}
		tasks = append(tasks, Task{
			TaskID:      r.TaskID,
			Title:       r.Title,
			Description: r.Description,
			Priority:    r.Priority,
			Status:      r.Status,
			TaskKind:    r.TaskKind,
			OwnerUserID: r.OwnerUserID,
			CreatedAt:   r.LinkedAt,
		})
	}
	return tasks, nil
}

// ClaimTask claims a task for the given agent in this workspace.
func (c *DirectRhizomeClient) ClaimTask(ctx context.Context, taskID, agentID string) error {
	return c.store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: c.workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	})
}

// ReleaseTask releases a previously-claimed task.
func (c *DirectRhizomeClient) ReleaseTask(ctx context.Context, taskID, agentID, reason string) error {
	return c.store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: c.workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      reason,
	})
}

// CompleteTask marks a task as completed.
func (c *DirectRhizomeClient) CompleteTask(ctx context.Context, taskID string, result string) error {
	return c.store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: c.workspaceID,
		TaskID:      taskID,
		AgentID:     c.agentID,
		Summary:     result,
	})
}

// FailTask marks a task as blocked with an error message.
func (c *DirectRhizomeClient) FailTask(ctx context.Context, taskID string, errMsg string) error {
	return c.store.BlockTask(ctx, sqlite.TaskBlockInput{
		WorkspaceID: c.workspaceID,
		TaskID:      taskID,
		AgentID:     c.agentID,
		Reason:      errMsg,
	})
}

// GetTaskUpdates returns agent updates created after `since`.
func (c *DirectRhizomeClient) GetTaskUpdates(ctx context.Context, taskID string, since time.Time) ([]Update, error) {
	afterCreatedAt := ""
	if !since.IsZero() {
		afterCreatedAt = since.UTC().Format(time.RFC3339Nano)
	}
	records, err := c.store.ListAgentUpdatesAfter(ctx, c.workspaceID, afterCreatedAt, "", 100)
	if err != nil {
		return nil, fmt.Errorf("get task updates: %w", err)
	}

	var updates []Update
	for _, r := range records {
		updates = append(updates, Update{
			UpdateID:      r.UpdateID,
			AgentID:       r.AgentID,
			UpdateType:    r.UpdateType,
			Summary:       r.Summary,
			PayloadJSON:   r.PayloadJSON,
			RequiresHuman: r.RequiresHuman,
			CreatedAt:     r.CreatedAt,
		})
	}
	return updates, nil
}

// SendUpdate posts an agent update to the workspace.
func (c *DirectRhizomeClient) SendUpdate(ctx context.Context, agentID, workspaceID, updateType, summary, payload string) error {
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  updateType,
		Summary:     summary,
		PayloadJSON: payload,
	})
}

// FetchMessages polls messages for the given agent created after `since`.
func (c *DirectRhizomeClient) FetchMessages(ctx context.Context, agentID string, since time.Time) ([]Message, error) {
	afterCreatedAt := ""
	if !since.IsZero() {
		afterCreatedAt = since.UTC().Format(time.RFC3339Nano)
	}
	cursor := c.messageCursorForFetch(agentID, since, afterCreatedAt)
	records, err := c.store.PollMessages(ctx, c.workspaceID, agentID, cursor, 50, 24)
	if err != nil {
		return nil, fmt.Errorf("fetch messages: %w", err)
	}
	c.rememberFetchedMessageCursor(agentID, records)

	var messages []Message
	for _, r := range records {
		messages = append(messages, Message{
			MessageID:   r.MessageID,
			FromAgentID: r.FromAgentID,
			ToAgentID:   r.ToAgentID,
			Channel:     r.Channel,
			Content:     r.Content,
			CreatedAt:   r.CreatedAt,
		})
	}
	return messages, nil
}

func (c *DirectRhizomeClient) messageCursorForFetch(agentID string, since time.Time, afterCreatedAt string) string {
	afterCreatedAt = strings.TrimSpace(afterCreatedAt)
	if afterCreatedAt == "" {
		return ""
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return afterCreatedAt
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cursor, ok := c.messagePoll[agentID]
	if !ok || !messageCursorMatchesTime(cursor.CreatedAt, since) || strings.TrimSpace(cursor.MessageID) == "" {
		return afterCreatedAt
	}
	return sqlite.EncodeMessageCursor(strings.TrimSpace(cursor.CreatedAt), cursor.MessageID)
}

func (c *DirectRhizomeClient) rememberFetchedMessageCursor(agentID string, records []sqlite.MessageRecord) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || len(records) == 0 {
		return
	}
	last := records[len(records)-1]
	createdAt := strings.TrimSpace(last.CreatedAt)
	messageID := strings.TrimSpace(last.MessageID)
	if createdAt == "" || messageID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.messagePoll == nil {
		c.messagePoll = make(map[string]directMessageCursor)
	}
	c.messagePoll[agentID] = directMessageCursor{
		CreatedAt: createdAt,
		MessageID: messageID,
	}
}

func messageCursorMatchesTime(createdAt string, since time.Time) bool {
	createdAt = strings.TrimSpace(createdAt)
	if createdAt == "" || since.IsZero() {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return false
	}
	return parsed.UTC().Equal(since.UTC())
}

// SendMessage sends a message from one agent to another. If taskID is
// non-empty it is included in the message metadata.
func (c *DirectRhizomeClient) SendMessage(ctx context.Context, from, to, content string, taskID string) error {
	metadataJSON := "{}"
	if taskID != "" {
		meta, _ := json.Marshal(map[string]string{"task_id": taskID})
		metadataJSON = string(meta)
	}
	_, err := c.store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID:  c.workspaceID,
		FromAgentID:  from,
		ToAgentID:    to,
		Channel:      "default",
		Content:      content,
		MetadataJSON: metadataJSON,
	})
	return err
}

// Heartbeat records an agent heartbeat.
func (c *DirectRhizomeClient) Heartbeat(ctx context.Context, agentID, status, summary string) error {
	return c.store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: c.workspaceID,
		AgentID:     agentID,
		Status:      status,
		Summary:     summary,
	})
}

// EscalateTask posts an escalation update for the given task.
func (c *DirectRhizomeClient) EscalateTask(ctx context.Context, taskID, reason string) error {
	payload, _ := json.Marshal(map[string]string{
		"task_id": taskID,
		"reason":  reason,
	})
	return c.store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID:   c.workspaceID,
		AgentID:       c.agentID,
		UpdateType:    "escalation",
		Summary:       fmt.Sprintf("Escalation for task %s: %s", taskID, reason),
		PayloadJSON:   string(payload),
		RequiresHuman: true,
	})
}

// SetAgentID sets the agent ID used for operations that need it (CompleteTask,
// FailTask, EscalateTask). This is separate from the constructor so that the
// same client can be reused across agent registration.
func (c *DirectRhizomeClient) SetAgentID(agentID string) {
	c.agentID = agentID
}

// RecordSessionEvent persists an explicit active-session protocol event using
// the current workspace as canonical runtime context.
func (c *DirectRhizomeClient) RecordSessionEvent(ctx context.Context, input SessionEventInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	state, err := c.store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:           input.EventType,
		WorkspaceID:         workspaceID,
		SessionID:           input.SessionID,
		AgentID:             input.AgentID,
		TaskID:              input.TaskID,
		Summary:             input.Summary,
		Status:              input.Status,
		OwnerScope:          input.OwnerScope,
		BlockedOn:           input.BlockedOn,
		DecisionNeededFrom:  input.DecisionNeededFrom,
		DecisionType:        input.DecisionType,
		KeepSessionActive:   input.KeepSessionActive,
		HandoffTo:           input.HandoffTo,
		RelatedDocKeys:      input.RelatedDocKeys,
		RelatedArtifactRefs: input.RelatedArtifactRefs,
		UpdatedAt:           input.UpdatedAt,
	})
	if err != nil {
		return err
	}
	c.syncSessionEventMemory(ctx, workspaceID, state)
	_, _ = c.store.SyncOperatorQueueFromSessionState(ctx, state)
	_ = c.store.SyncExecutionRunFromSessionState(ctx, state)
	return nil
}

func (c *DirectRhizomeClient) RecordWorkspaceMemory(ctx context.Context, input WorkspaceMemoryInput) (WorkspaceMemoryRecord, error) {
	result, err := c.RecordWorkspaceMemoryWithEffects(ctx, input)
	if err != nil {
		return WorkspaceMemoryRecord{}, err
	}
	return result.Memory, nil
}

func (c *DirectRhizomeClient) RecordWorkspaceMemoryWithEffects(ctx context.Context, input WorkspaceMemoryInput) (WorkspaceMemoryWriteResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	record, _, effects, err := c.store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		MemoryID:    input.MemoryID,
		WorkspaceID: workspaceID,
		MemoryType:  input.MemoryType,
		Title:       input.Title,
		Body:        input.Body,
		Summary:     input.Summary,
		AgentID:     input.AgentID,
		SessionID:   input.SessionID,
		TaskID:      input.TaskID,
		SourceKind:  input.SourceKind,
		SourceID:    input.SourceID,
		Tags:        input.Tags,
		Importance:  input.Importance,
		Confidence:  input.Confidence,
	})
	if err != nil {
		return WorkspaceMemoryWriteResult{}, err
	}
	return WorkspaceMemoryWriteResult{
		Memory:               convertWorkspaceMemoryRecord(record),
		PromotedClaimEffects: convertPromotedClaimEffects(effects),
	}, nil
}

func (c *DirectRhizomeClient) ListWorkspaceMemory(ctx context.Context, filter WorkspaceMemorySearchFilter) ([]WorkspaceMemoryRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	records, err := c.store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		MemoryType:  filter.MemoryType,
		AgentID:     filter.AgentID,
		SessionID:   filter.SessionID,
		TaskID:      filter.TaskID,
		SourceKind:  filter.SourceKind,
		Limit:       filter.Limit,
	})
	if err != nil {
		return nil, err
	}
	return convertWorkspaceMemoryRecords(records), nil
}

func (c *DirectRhizomeClient) SearchWorkspaceMemory(ctx context.Context, filter WorkspaceMemorySearchFilter) ([]WorkspaceMemoryRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	records, err := c.store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		Query:       filter.Query,
		MemoryType:  filter.MemoryType,
		AgentID:     filter.AgentID,
		SessionID:   filter.SessionID,
		TaskID:      filter.TaskID,
		SourceKind:  filter.SourceKind,
		Limit:       filter.Limit,
	})
	if err != nil {
		return nil, err
	}
	return convertWorkspaceMemoryRecords(records), nil
}

func (c *DirectRhizomeClient) BuildMemoryShellPacket(ctx context.Context, filter WorkspaceMemoryPacketFilter) (sqlite.MemoryShellPacket, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.BuildMemoryShellPacket(ctx, sqlite.MemoryPacketFilter{
		WorkspaceID:    workspaceID,
		TaskID:         filter.TaskID,
		SessionID:      filter.SessionID,
		AgentID:        agentID,
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		IncludeAllDocs: filter.IncludeAllDocs,
		Budget:         filter.Budget,
	})
}

func (c *DirectRhizomeClient) BuildMemoryKernelPacket(ctx context.Context, filter WorkspaceMemoryPacketFilter) (sqlite.MemoryKernelPacket, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.BuildMemoryKernelPacket(ctx, sqlite.MemoryPacketFilter{
		WorkspaceID:    workspaceID,
		TaskID:         filter.TaskID,
		SessionID:      filter.SessionID,
		AgentID:        strings.TrimSpace(filter.AgentID),
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		IncludeAllDocs: filter.IncludeAllDocs,
		Budget:         filter.Budget,
	})
}

func (c *DirectRhizomeClient) GetMemoryPromotion(ctx context.Context, filter WorkspaceMemoryPromotionFilter) (sqlite.MemoryPromotionRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.GetMemoryPromotion(ctx, workspaceID, strings.TrimSpace(filter.PromotionID))
}

func (c *DirectRhizomeClient) ListMemoryPromotions(ctx context.Context, filter WorkspaceMemoryPromotionFilter) ([]sqlite.MemoryPromotionRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.ListMemoryPromotions(ctx, sqlite.MemoryPromotionFilter{
		WorkspaceID:   workspaceID,
		State:         filter.State,
		CandidateKind: filter.CandidateKind,
		CandidateType: filter.CandidateType,
		Limit:         filter.Limit,
	})
}

func (c *DirectRhizomeClient) GetMemoryCoherenceScope(ctx context.Context, filter WorkspaceMemoryCoherenceFilter) (sqlite.MemoryCoherenceScopeReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.GetMemoryCoherenceScope(
		ctx,
		workspaceID,
		strings.TrimSpace(filter.AgentID),
		strings.TrimSpace(filter.SessionID),
		strings.TrimSpace(filter.ReportScope),
	)
}

func (c *DirectRhizomeClient) ListMemoryInvalidations(ctx context.Context, filter WorkspaceMemoryInvalidationListFilter) (WorkspaceMemoryInvalidationListResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	items, err := c.store.ListMemoryInvalidations(ctx, sqlite.MemoryInvalidationListFilter{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		SessionID:         strings.TrimSpace(filter.SessionID),
		IncludeAcked:      filter.IncludeAcked,
		IncludeDeadLetter: filter.IncludeDeadLetter,
		Limit:             filter.Limit,
	})
	if err != nil {
		return WorkspaceMemoryInvalidationListResult{}, err
	}
	authority, err := c.store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return WorkspaceMemoryInvalidationListResult{}, err
	}
	return WorkspaceMemoryInvalidationListResult{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		TimeAuthority: authority,
		Items:         items,
		Count:         len(items),
	}, nil
}

func (c *DirectRhizomeClient) GetMemoryInvalidation(ctx context.Context, filter WorkspaceMemoryInvalidationGetFilter) (sqlite.MemoryInvalidationRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.GetMemoryInvalidation(ctx, workspaceID, agentID, strings.TrimSpace(filter.InvalidationID))
}

func (c *DirectRhizomeClient) GetMemoryInvalidationCursor(ctx context.Context, filter WorkspaceMemoryInvalidationCursorFilter) (sqlite.MemoryInvalidationCursorRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.GetMemoryInvalidationCursor(ctx, workspaceID, agentID, strings.TrimSpace(filter.SessionID))
}

func (c *DirectRhizomeClient) ListMemoryGraphNodes(ctx context.Context, filter WorkspaceMemoryGraphListFilter) (WorkspaceMemoryGraphListResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	items, err := c.store.ListMemoryGraphNodes(ctx, sqlite.MemoryGraphNodeFilter{
		WorkspaceID:     workspaceID,
		MemoryType:      strings.TrimSpace(filter.MemoryType),
		MemoryLayer:     strings.TrimSpace(filter.MemoryLayer),
		Visibility:      strings.TrimSpace(filter.Visibility),
		EpistemicStatus: strings.TrimSpace(filter.EpistemicStatus),
		LifecycleState:  strings.TrimSpace(filter.LifecycleState),
		OriginKind:      strings.TrimSpace(filter.OriginKind),
		OriginID:        strings.TrimSpace(filter.OriginID),
		SourceKind:      strings.TrimSpace(filter.SourceKind),
		AgentID:         agentID,
		SessionID:       strings.TrimSpace(filter.SessionID),
		TaskID:          strings.TrimSpace(filter.TaskID),
		IncludeArchived: filter.IncludeArchived,
		Limit:           filter.Limit,
	})
	if err != nil {
		return WorkspaceMemoryGraphListResult{}, err
	}
	authority, err := c.store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return WorkspaceMemoryGraphListResult{}, err
	}
	return WorkspaceMemoryGraphListResult{
		WorkspaceID:      workspaceID,
		TimeAuthority:    authority,
		BoundaryContract: sqlite.DefaultMemoryShapeBoundaryContract(),
		Items:            items,
		Count:            len(items),
	}, nil
}

func (c *DirectRhizomeClient) GetMemoryGraphNode(ctx context.Context, filter WorkspaceMemoryGraphGetFilter) (sqlite.MemoryGraphNodeDetail, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.GetMemoryGraphNode(ctx, workspaceID, strings.TrimSpace(filter.MemoryID))
}

func (c *DirectRhizomeClient) SearchMemoryNodes(ctx context.Context, filter WorkspaceMemoryNodeSearchFilter) (sqlite.MemoryNodeSearchResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.SearchMemoryNodes(ctx, sqlite.MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           strings.TrimSpace(filter.Query),
		MemoryType:      strings.TrimSpace(filter.MemoryType),
		MemoryLayer:     strings.TrimSpace(filter.MemoryLayer),
		Visibility:      strings.TrimSpace(filter.Visibility),
		EpistemicStatus: strings.TrimSpace(filter.EpistemicStatus),
		LifecycleState:  strings.TrimSpace(filter.LifecycleState),
		OriginKind:      strings.TrimSpace(filter.OriginKind),
		OriginID:        strings.TrimSpace(filter.OriginID),
		SourceKind:      strings.TrimSpace(filter.SourceKind),
		AgentID:         agentID,
		SessionID:       strings.TrimSpace(filter.SessionID),
		TaskID:          strings.TrimSpace(filter.TaskID),
		IncludeArchived: filter.IncludeArchived,
		Limit:           filter.Limit,
	})
}

func (c *DirectRhizomeClient) GetRSPStateReport(ctx context.Context, filter WorkspaceRSPStateFilter) (sqlite.RSPStateReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.BuildRSPStateReport(ctx, sqlite.RSPStateReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: strings.TrimSpace(filter.ProtoClusterID),
		AgentID:        agentID,
		SessionID:      strings.TrimSpace(filter.SessionID),
		TaskID:         strings.TrimSpace(filter.TaskID),
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	})
}

func (c *DirectRhizomeClient) GetRSPForecastReport(ctx context.Context, filter WorkspaceRSPForecastFilter) (sqlite.RSPForecastReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.BuildRSPForecastReport(ctx, sqlite.RSPForecastReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: strings.TrimSpace(filter.ProtoClusterID),
		AgentID:        agentID,
		SessionID:      strings.TrimSpace(filter.SessionID),
		TaskID:         strings.TrimSpace(filter.TaskID),
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	})
}

func (c *DirectRhizomeClient) GetRSPBeliefReport(ctx context.Context, filter WorkspaceRSPBeliefFilter) (sqlite.RSPBeliefReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.BuildRSPBeliefReport(ctx, sqlite.RSPBeliefReportFilter{
		WorkspaceID: workspaceID,
		ClaimType:   strings.TrimSpace(filter.ClaimType),
		AgentID:     agentID,
		SessionID:   strings.TrimSpace(filter.SessionID),
		TaskID:      strings.TrimSpace(filter.TaskID),
		Limit:       filter.Limit,
	})
}

func (c *DirectRhizomeClient) GetRSPBeliefClaim(ctx context.Context, filter WorkspaceRSPBeliefClaimFilter) (sqlite.RSPBeliefClaimReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.GetRSPBeliefClaim(ctx, workspaceID, strings.TrimSpace(filter.ClaimID))
}

func (c *DirectRhizomeClient) GetRSPTelemetryDump(ctx context.Context, filter WorkspaceRSPTelemetryFilter) (sqlite.RSPTelemetryDump, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.DumpRSPTelemetry(ctx, sqlite.RSPTelemetryDumpFilter{
		WorkspaceID: workspaceID,
		Limit:       filter.Limit,
	})
}

func (c *DirectRhizomeClient) GetUnifiedControlReport(ctx context.Context, filter WorkspaceUnifiedControlFilter) (sqlite.UnifiedControlReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.BuildUnifiedControlReport(ctx, sqlite.UnifiedControlReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: strings.TrimSpace(filter.ProtoClusterID),
		AgentID:        agentID,
		TaskID:         strings.TrimSpace(filter.TaskID),
		SessionID:      strings.TrimSpace(filter.SessionID),
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	})
}

func (c *DirectRhizomeClient) GetControlReport(ctx context.Context, filter WorkspaceControlReportFilter) (sqlite.ControlReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.BuildControlReport(ctx, sqlite.ControlReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: strings.TrimSpace(filter.ProtoClusterID),
		Limit:          filter.Limit,
	})
}

func (c *DirectRhizomeClient) GetControlClusterDetail(ctx context.Context, filter WorkspaceControlClusterFilter) (sqlite.ControlClusterDetail, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.BuildControlClusterDetail(ctx, workspaceID, strings.TrimSpace(filter.ProtoClusterID))
}

func (c *DirectRhizomeClient) GetControlStateReport(ctx context.Context, filter WorkspaceControlStateFilter) (sqlite.ClusterControlStateReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.BuildClusterControlStateReport(ctx, sqlite.ClusterControlStateFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: strings.TrimSpace(filter.ProtoClusterID),
		Mode:           strings.TrimSpace(filter.Mode),
		Limit:          filter.Limit,
	})
}

func (c *DirectRhizomeClient) GetControlStateClusterDetail(ctx context.Context, filter WorkspaceControlStateClusterFilter) (sqlite.ClusterControlStateDetail, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.BuildClusterControlStateDetail(ctx, workspaceID, strings.TrimSpace(filter.ProtoClusterID))
}

func (c *DirectRhizomeClient) ListTensions(ctx context.Context, filter WorkspaceTensionFilter) (WorkspaceTensionListResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	items, err := c.store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID:    workspaceID,
		TensionType:    strings.TrimSpace(filter.TensionType),
		LifecycleState: strings.TrimSpace(filter.LifecycleState),
		ReviewStatus:   strings.TrimSpace(filter.ReviewStatus),
		ProtoClusterID: strings.TrimSpace(filter.ProtoClusterID),
		TaskID:         strings.TrimSpace(filter.TaskID),
		AgentID:        agentID,
		Limit:          filter.Limit,
	})
	if err != nil {
		return WorkspaceTensionListResult{}, err
	}
	authority, err := c.store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return WorkspaceTensionListResult{}, err
	}
	return WorkspaceTensionListResult{
		WorkspaceID:   workspaceID,
		TimeAuthority: authority,
		Items:         items,
		Count:         len(items),
	}, nil
}

func (c *DirectRhizomeClient) GetTension(ctx context.Context, filter WorkspaceTensionGetFilter) (sqlite.TensionDetail, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.GetTension(ctx, workspaceID, strings.TrimSpace(filter.TensionID))
}

func (c *DirectRhizomeClient) ListTensionFrontier(ctx context.Context, filter WorkspaceTensionFilter) (WorkspaceTensionFrontierResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	items, err := c.store.ListTensionFrontier(ctx, sqlite.TensionFilter{
		WorkspaceID:    workspaceID,
		TensionType:    strings.TrimSpace(filter.TensionType),
		LifecycleState: strings.TrimSpace(filter.LifecycleState),
		ReviewStatus:   strings.TrimSpace(filter.ReviewStatus),
		ProtoClusterID: strings.TrimSpace(filter.ProtoClusterID),
		TaskID:         strings.TrimSpace(filter.TaskID),
		AgentID:        agentID,
		Limit:          filter.Limit,
	})
	if err != nil {
		return WorkspaceTensionFrontierResult{}, err
	}
	authority, err := c.store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return WorkspaceTensionFrontierResult{}, err
	}
	return WorkspaceTensionFrontierResult{
		WorkspaceID:   workspaceID,
		TimeAuthority: authority,
		Items:         items,
		Count:         len(items),
	}, nil
}

func (c *DirectRhizomeClient) ListAttachableTensions(ctx context.Context, filter WorkspaceTensionAttachableFilter) (WorkspaceTensionAttachableResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	items, err := c.store.ListAgentAvailableTensionsScored(ctx, workspaceID, agentID)
	if err != nil {
		return WorkspaceTensionAttachableResult{}, err
	}
	return WorkspaceTensionAttachableResult{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Items:       items,
		Count:       len(items),
	}, nil
}

func (c *DirectRhizomeClient) GetRSPCapabilityFlags(ctx context.Context, filter WorkspaceRSPCapabilityFilter) (sqlite.RSPCapabilityFlags, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	return c.store.GetRSPCapabilityFlags(ctx, workspaceID), nil
}

func (c *DirectRhizomeClient) ListWorkspaceEvents(ctx context.Context, filter WorkspaceEventsListFilter) (WorkspaceEventsListResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	items, err := c.store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   strings.TrimSpace(filter.EventType),
		EntityType:  strings.TrimSpace(filter.EntityType),
		EntityID:    strings.TrimSpace(filter.EntityID),
		AgentID:     agentID,
		SessionID:   strings.TrimSpace(filter.SessionID),
		TaskID:      strings.TrimSpace(filter.TaskID),
		Limit:       filter.Limit,
	})
	if err != nil {
		return WorkspaceEventsListResult{}, err
	}
	authority, err := c.store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return WorkspaceEventsListResult{}, err
	}
	return WorkspaceEventsListResult{
		WorkspaceID:   workspaceID,
		TimeAuthority: authority,
		Items:         items,
		Count:         len(items),
	}, nil
}

func (c *DirectRhizomeClient) ReplayWorkspaceEvents(ctx context.Context, filter WorkspaceEventsReplayFilter) (sqlite.RuntimeReplayReport, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	report, err := c.store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   strings.TrimSpace(filter.SessionID),
		TaskID:      strings.TrimSpace(filter.TaskID),
		Limit:       filter.Limit,
	})
	if err != nil {
		return sqlite.RuntimeReplayReport{}, err
	}
	if !filter.IncludeEvents {
		report.Events = nil
	}
	return report, nil
}

func (c *DirectRhizomeClient) ListSessionCompactionCandidates(ctx context.Context, agentID string, minMessages, minTokens int) ([]SessionCompactionCandidate, error) {
	if strings.TrimSpace(agentID) == "" {
		agentID = c.agentID
	}
	records, err := c.store.ListSessionCompactionCandidates(ctx, sqlite.SessionCompactionFilter{
		WorkspaceID: c.workspaceID,
		AgentID:     strings.TrimSpace(agentID),
		ActiveOnly:  true,
		MinMessages: minMessages,
		MinTokens:   minTokens,
		Limit:       20,
	})
	if err != nil {
		return nil, err
	}
	out := make([]SessionCompactionCandidate, 0, len(records))
	for _, record := range records {
		out = append(out, SessionCompactionCandidate{
			SessionID:         record.SessionID,
			WorkspaceID:       record.WorkspaceID,
			AgentID:           record.AgentID,
			TaskID:            record.TaskID,
			Status:            record.Status,
			MessageCount:      record.MessageCount,
			MessageTokens:     record.MessageTokens,
			TotalInputTokens:  record.TotalInputTokens,
			TotalOutputTokens: record.TotalOutputTokens,
			TotalTokens:       record.TotalTokens,
			StartedAt:         record.StartedAt,
			LastMessageAt:     record.LastMessageAt,
		})
	}
	return out, nil
}

func (c *DirectRhizomeClient) ListSessionCompactionSnapshots(ctx context.Context, filter WorkspaceCompactionSnapshotFilter) ([]sqlite.SessionCompactionSnapshotRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = c.workspaceID
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		agentID = c.agentID
	}
	return c.store.ListSessionCompactionSnapshots(ctx, sqlite.SessionCompactionSnapshotFilter{
		WorkspaceID: workspaceID,
		SessionID:   strings.TrimSpace(filter.SessionID),
		AgentID:     agentID,
		Limit:       filter.Limit,
	})
}

func (c *DirectRhizomeClient) syncSessionEventMemory(ctx context.Context, workspaceID string, state sqlite.AgentSessionStateRecord) {
	_ = sessionmemory.SyncEvent(ctx, c.store, workspaceID, state)
}

func convertWorkspaceMemoryRecords(records []sqlite.WorkspaceMemoryRecord) []WorkspaceMemoryRecord {
	out := make([]WorkspaceMemoryRecord, 0, len(records))
	for _, record := range records {
		out = append(out, convertWorkspaceMemoryRecord(record))
	}
	return out
}

func convertWorkspaceMemoryRecord(record sqlite.WorkspaceMemoryRecord) WorkspaceMemoryRecord {
	return WorkspaceMemoryRecord{
		MemoryID:    record.MemoryID,
		WorkspaceID: record.WorkspaceID,
		MemoryType:  record.MemoryType,
		Title:       record.Title,
		Body:        record.Body,
		Summary:     record.Summary,
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		SourceKind:  record.SourceKind,
		SourceID:    record.SourceID,
		Tags:        record.Tags,
		Importance:  record.Importance,
		Confidence:  record.Confidence,
		CreatedAt:   record.CreatedAt,
		UpdatedAt:   record.UpdatedAt,
	}
}

func convertPromotedClaimEffects(effects *sqlite.PromotedKnowledgeClaimSyncEffects) *PromotedClaimEffects {
	if effects == nil {
		return nil
	}
	out := &PromotedClaimEffects{
		InvalidationEvents: append([]sqlite.RuntimeEventRecord(nil), effects.InvalidationEvents...),
	}
	if effects.Claim != nil {
		claimCopy := *effects.Claim
		out.Claim = &claimCopy
	}
	if effects.ClaimEvent != nil {
		eventCopy := *effects.ClaimEvent
		out.ClaimEvent = &eventCopy
	}
	if effects.Queue != nil {
		queueCopy := *effects.Queue
		out.Queue = &queueCopy
	}
	if effects.QueueEvent != nil {
		queueEventCopy := *effects.QueueEvent
		out.QueueEvent = &queueEventCopy
	}
	return out
}

func claimAvailable(status *string) bool {
	if status == nil {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(*status)) {
	case "", model.TaskClaimStatusReleased:
		return true
	default:
		return false
	}
}
