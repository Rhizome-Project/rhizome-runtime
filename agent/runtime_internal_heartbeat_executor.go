package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	internalHeartbeatLocalResultContractVersion    = "internal-heartbeat-local-result/v1"
	internalHeartbeatSummaryContractVersion        = "internal-heartbeat-summary/v1"
	internalHeartbeatPublicSummaryMinInterval      = time.Hour
	internalHeartbeatMaxBacklogWrites              = 3
	internalHeartbeatMinPromotionScore             = 70
	internalHeartbeatBrowserProbeLimit             = 2
	internalHeartbeatVisualSensorSource            = "typed_visual_preflight"
	internalHeartbeatSelfCheckSensorSource         = "typed_self_check"
	internalHeartbeatProjectInitiativeSensorSource = "typed_project_initiative"
	internalHeartbeatPatchQueueVigilanceSource     = "typed_patch_queue_vigilance"
	internalHeartbeatActionRequestSource           = "internal_heartbeat_action_request"
	internalHeartbeatBacklogArbiterSource          = "typed_personal_backlog_arbiter"
	internalHeartbeatActionRequestPromoterSource   = "typed_action_request_promoter"
	internalHeartbeatCapabilitySessionSource       = "typed_capability_session"
)

var internalHeartbeatURLPattern = regexp.MustCompile(`https?://[^\s<>"'\)\]\}]+`)
var internalHeartbeatHTMLTitlePattern = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
var internalHeartbeatLocalImagePathPattern = regexp.MustCompile(`(?i)(?:@fs[\\/])?(?:\\\\\?\\[A-Za-z]:|[A-Za-z]:|\\\\[^\\/:*?"<>|\r\n]+\\[^\\/:*?"<>|\r\n]+|/(?:users|home|tmp|var|private))[/\\][^<>"'\r\n]*?\.(?:png|jpe?g|webp|bmp|gif)`)
var internalHeartbeatWindowsPathPattern = regexp.MustCompile(`[A-Za-z]:[\\/][^\s<>"'\)\]\}]+`)
var internalHeartbeatSensitiveUnixPathPattern = regexp.MustCompile(`(?i)(?:/|\\)(?:users|home|tmp|var|private)(?:/|\\)[^\s<>"'\)\]\}]+`)
var internalHeartbeatViteFSPathPattern = regexp.MustCompile(`@fs[^\s<>"'\)\]\}]+`)

type InternalHeartbeatExecutionPolicy struct {
	HeartbeatID           string   `json:"heartbeat_id,omitempty"`
	Kind                  string   `json:"kind,omitempty"`
	ToolSuites            []string `json:"tool_suites,omitempty"`
	ContextSelectors      []string `json:"context_selectors,omitempty"`
	OutputContracts       []string `json:"output_contracts,omitempty"`
	PromotionSignals      []string `json:"promotion_signals,omitempty"`
	LocalOnly             bool     `json:"local_only,omitempty"`
	AllowLLM              bool     `json:"allow_llm,omitempty"`
	AllowPublicDocs       bool     `json:"allow_public_docs,omitempty"`
	AllowTaskSubmit       bool     `json:"allow_task_submit,omitempty"`
	MaxTaskSubmits        int      `json:"max_task_submits,omitempty"`
	MaxToolIterations     int      `json:"max_tool_iterations,omitempty"`
	AllowAgentRequest     bool     `json:"allow_agent_request,omitempty"`
	RequiresTaskLoop      bool     `json:"requires_task_loop,omitempty"`
	RequireSession        bool     `json:"require_session,omitempty"`
	ExpectsLocalMemory    bool     `json:"expects_local_memory,omitempty"`
	AllowWillDirectives   bool     `json:"allow_will_directives,omitempty"`
	WillActions           []string `json:"will_actions,omitempty"`
	MaxWillDirectives     int      `json:"max_will_directives,omitempty"`
	WillRequiresEvidence  bool     `json:"will_requires_evidence,omitempty"`
	WillPublishVisibility string   `json:"will_publish_visibility,omitempty"`
}

type InternalHeartbeatExecutionResult struct {
	SessionID        string   `json:"session_id,omitempty"`
	HeartbeatID      string   `json:"heartbeat_id,omitempty"`
	Status           string   `json:"status,omitempty"`
	Outcome          string   `json:"outcome,omitempty"`
	Summary          string   `json:"summary,omitempty"`
	Trigger          string   `json:"trigger,omitempty"`
	PromotionBlocked bool     `json:"promotion_blocked,omitempty"`
	ToolSuites       []string `json:"tool_suites,omitempty"`
	ContextSelectors []string `json:"context_selectors,omitempty"`
	OutputContracts  []string `json:"output_contracts,omitempty"`
	PromotedRefs     []string `json:"promoted_refs,omitempty"`
}

type InternalHeartbeatPublicSummaryPayload struct {
	ContractVersion       string   `json:"contract_version"`
	WorkspaceID           string   `json:"workspace_id,omitempty"`
	AgentID               string   `json:"agent_id,omitempty"`
	SessionID             string   `json:"session_id,omitempty"`
	HeartbeatID           string   `json:"heartbeat_id,omitempty"`
	HeartbeatKind         string   `json:"heartbeat_kind,omitempty"`
	Status                string   `json:"status,omitempty"`
	Outcome               string   `json:"outcome,omitempty"`
	Summary               string   `json:"summary,omitempty"`
	Trigger               string   `json:"trigger,omitempty"`
	PublishReason         string   `json:"publish_reason,omitempty"`
	ObservabilityOnly     bool     `json:"observability_only"`
	LocalOnly             bool     `json:"local_only"`
	AllowTaskSubmit       bool     `json:"allow_task_submit"`
	ToolSuites            []string `json:"tool_suites,omitempty"`
	ContextSelectors      []string `json:"context_selectors,omitempty"`
	OutputContracts       []string `json:"output_contracts,omitempty"`
	PromotedRefs          []string `json:"promoted_refs,omitempty"`
	PromotedRefCount      int      `json:"promoted_ref_count,omitempty"`
	AnatomyDigest         string   `json:"anatomy_digest,omitempty"`
	StartedAt             string   `json:"started_at,omitempty"`
	EndedAt               string   `json:"ended_at,omitempty"`
	PrivateMemoryRedacted bool     `json:"private_memory_redacted"`
}

type InternalHeartbeatContextPacket struct {
	WorkspaceID           string                                  `json:"workspace_id,omitempty"`
	AgentID               string                                  `json:"agent_id,omitempty"`
	ProfileID             string                                  `json:"profile_id,omitempty"`
	HeartbeatID           string                                  `json:"heartbeat_id,omitempty"`
	HeartbeatKind         string                                  `json:"heartbeat_kind,omitempty"`
	HeartbeatObjective    string                                  `json:"heartbeat_objective,omitempty"`
	HeartbeatTriggers     []string                                `json:"heartbeat_triggers,omitempty"`
	HeartbeatNotes        []string                                `json:"heartbeat_notes,omitempty"`
	Instructions          []string                                `json:"instructions,omitempty"`
	MemoryLanes           []string                                `json:"memory_lanes,omitempty"`
	Trigger               string                                  `json:"trigger,omitempty"`
	Now                   string                                  `json:"now,omitempty"`
	ActiveTaskID          string                                  `json:"active_task_id,omitempty"`
	LocalOnly             bool                                    `json:"local_only"`
	AllowTaskSubmit       bool                                    `json:"allow_task_submit"`
	MaxTaskSubmits        int                                     `json:"max_task_submits,omitempty"`
	MaxToolIterations     int                                     `json:"max_tool_iterations,omitempty"`
	ToolSuites            []string                                `json:"tool_suites,omitempty"`
	ActionPolicy          InternalHeartbeatActionPolicy           `json:"action_policy,omitempty"`
	ContextSelectors      []string                                `json:"context_selectors,omitempty"`
	OutputContracts       []string                                `json:"output_contracts,omitempty"`
	PromotionSignals      []string                                `json:"promotion_signals,omitempty"`
	TrustedScope          InternalHeartbeatTrustedScope           `json:"trusted_scope,omitempty"`
	RecentSessions        []InternalHeartbeatSessionSummary       `json:"recent_sessions,omitempty"`
	BacklogCandidates     []InternalHeartbeatBacklogSummary       `json:"backlog_candidates,omitempty"`
	ActiveMemoryPolicy    InternalHeartbeatActiveMemoryPolicy     `json:"active_memory_policy,omitempty"`
	ActiveMemory          []InternalHeartbeatActiveMemoryEntry    `json:"active_memory,omitempty"`
	SelectorPayloads      []InternalHeartbeatSelectorPacket       `json:"selector_payloads,omitempty"`
	RequiredToolArtifacts []InternalHeartbeatRequiredToolArtifact `json:"required_tool_artifacts,omitempty"`
	PolicyInstructions    []string                                `json:"policy_instructions,omitempty"`
}

type InternalHeartbeatActiveMemoryPolicy struct {
	Lane                    string `json:"lane,omitempty"`
	MaxEntries              int    `json:"max_entries,omitempty"`
	IncludeSessionSummaries bool   `json:"include_session_summaries,omitempty"`
	IncludeBacklog          bool   `json:"include_backlog,omitempty"`
}

type InternalHeartbeatActiveMemoryEntry struct {
	Source        string   `json:"source,omitempty"`
	Lane          string   `json:"lane,omitempty"`
	SessionID     string   `json:"session_id,omitempty"`
	BacklogItemID string   `json:"backlog_item_id,omitempty"`
	Outcome       string   `json:"outcome,omitempty"`
	Summary       string   `json:"summary,omitempty"`
	EvidenceRefs  []string `json:"evidence_refs,omitempty"`
	At            string   `json:"at,omitempty"`
}

type InternalHeartbeatRequiredToolArtifact struct {
	Tool            string `json:"tool,omitempty"`
	ContractVersion string `json:"contract_version,omitempty"`
	Capability      string `json:"capability,omitempty"`
	ToolSuite       string `json:"tool_suite,omitempty"`
	When            string `json:"when,omitempty"`
	RequiredNow     bool   `json:"required_now"`
	Reason          string `json:"reason,omitempty"`
	Purpose         string `json:"purpose,omitempty"`
	BlockerGuidance string `json:"blocker_guidance,omitempty"`
}

type InternalHeartbeatActionPolicy struct {
	AuthorityBoundary   string   `json:"authority_boundary,omitempty"`
	AllowedCapabilities []string `json:"allowed_capabilities,omitempty"`
	BlockedCapabilities []string `json:"blocked_capabilities,omitempty"`
	BlockedToolSuites   []string `json:"blocked_tool_suites,omitempty"`
	ActionRequestFormat string   `json:"action_request_format,omitempty"`
}

type InternalHeartbeatTrustedScope struct {
	ProjectID   string `json:"project_id,omitempty"`
	ProjectLane string `json:"project_lane,omitempty"`
	Source      string `json:"source,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type InternalHeartbeatSessionSummary struct {
	SessionID   string `json:"session_id,omitempty"`
	HeartbeatID string `json:"heartbeat_id,omitempty"`
	Status      string `json:"status,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	Summary     string `json:"summary,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	EndedAt     string `json:"ended_at,omitempty"`
}

type InternalHeartbeatBacklogSummary struct {
	ItemID                   string   `json:"item_id,omitempty"`
	DedupKey                 string   `json:"dedup_key,omitempty"`
	HeartbeatID              string   `json:"heartbeat_id,omitempty"`
	Kind                     string   `json:"kind,omitempty"`
	Status                   string   `json:"status,omitempty"`
	Title                    string   `json:"title,omitempty"`
	Summary                  string   `json:"summary,omitempty"`
	Score                    int      `json:"score,omitempty"`
	SeenCount                int      `json:"seen_count,omitempty"`
	CreatedAt                string   `json:"created_at,omitempty"`
	LastSeenAt               string   `json:"last_seen_at,omitempty"`
	EvidenceRefs             []string `json:"evidence_refs,omitempty"`
	Source                   string   `json:"source,omitempty"`
	ActionCapability         string   `json:"action_capability,omitempty"`
	ActionToolSuite          string   `json:"action_tool_suite,omitempty"`
	ActionRequiresTaskLoop   bool     `json:"action_requires_task_loop,omitempty"`
	ActionRequiresHumanInput bool     `json:"action_requires_human_input,omitempty"`
	TargetProjectID          string   `json:"target_project_id,omitempty"`
	TargetProjectLane        string   `json:"target_project_lane,omitempty"`
	PromotionBlocked         bool     `json:"promotion_blocked,omitempty"`
}

type InternalHeartbeatSelectorPacket struct {
	Selector      string                                 `json:"selector"`
	Status        string                                 `json:"status"`
	Summary       string                                 `json:"summary,omitempty"`
	Workspace     InternalHeartbeatWorkspaceSummary      `json:"workspace,omitempty"`
	TrustedScope  InternalHeartbeatTrustedScope          `json:"trusted_scope,omitempty"`
	Projects      []InternalHeartbeatProjectSummary      `json:"projects,omitempty"`
	Tasks         []InternalHeartbeatTaskSummary         `json:"tasks,omitempty"`
	Checkouts     []InternalHeartbeatCheckoutSummary     `json:"checkouts,omitempty"`
	PatchQueue    []InternalHeartbeatPatchQueueSummary   `json:"patch_queue,omitempty"`
	Docs          []InternalHeartbeatDocSummary          `json:"docs,omitempty"`
	Agents        []InternalHeartbeatAgentSummary        `json:"agents,omitempty"`
	Roles         []InternalHeartbeatProjectRoleSummary  `json:"roles,omitempty"`
	ServiceRuns   []InternalHeartbeatServiceRunSummary   `json:"service_runs,omitempty"`
	Surfaces      []InternalHeartbeatRunnableSurface     `json:"surfaces,omitempty"`
	BrowserProbes []InternalHeartbeatBrowserProbe        `json:"browser_probes,omitempty"`
	VisualAudit   *InternalHeartbeatVisualAuditPlan      `json:"visual_audit,omitempty"`
	Backlog       []InternalHeartbeatBacklogSummary      `json:"backlog,omitempty"`
	RecentUpdates []InternalHeartbeatRecentUpdateSummary `json:"recent_updates,omitempty"`
}

type InternalHeartbeatWorkspaceSummary struct {
	WorkspaceID        string         `json:"workspace_id,omitempty"`
	Title              string         `json:"title,omitempty"`
	ProjectCount       int            `json:"project_count,omitempty"`
	TaskCount          int            `json:"task_count,omitempty"`
	OpenTaskCount      int            `json:"open_task_count,omitempty"`
	BlockedTaskCount   int            `json:"blocked_task_count,omitempty"`
	ClaimedTaskCount   int            `json:"claimed_task_count,omitempty"`
	RecentDocCount     int            `json:"recent_doc_count,omitempty"`
	TaskCountsByStatus map[string]int `json:"task_counts_by_status,omitempty"`
	TaskCountsByLane   map[string]int `json:"task_counts_by_lane,omitempty"`
}

type InternalHeartbeatProjectSummary struct {
	ProjectID        string `json:"project_id,omitempty"`
	Title            string `json:"title,omitempty"`
	Status           string `json:"status,omitempty"`
	TaskCount        int    `json:"task_count,omitempty"`
	OpenTaskCount    int    `json:"open_task_count,omitempty"`
	BlockedTaskCount int    `json:"blocked_task_count,omitempty"`
	ClaimedTaskCount int    `json:"claimed_task_count,omitempty"`
}

type InternalHeartbeatTaskSummary struct {
	TaskID               string   `json:"task_id,omitempty"`
	Title                string   `json:"title,omitempty"`
	Description          string   `json:"description,omitempty"`
	Status               string   `json:"status,omitempty"`
	ClaimStatus          string   `json:"claim_status,omitempty"`
	ClaimAgentID         string   `json:"claim_agent_id,omitempty"`
	ClaimUpdatedAt       string   `json:"claim_updated_at,omitempty"`
	UpdatedAt            string   `json:"updated_at,omitempty"`
	ProjectID            string   `json:"project_id,omitempty"`
	ProjectLane          string   `json:"project_lane,omitempty"`
	TaskKind             string   `json:"task_kind,omitempty"`
	Priority             string   `json:"priority,omitempty"`
	TaskRequirementsJSON string   `json:"task_requirements_json,omitempty"`
	Tags                 []string `json:"tags,omitempty"`
}

type InternalHeartbeatCheckoutSummary struct {
	CheckoutID    string `json:"checkout_id,omitempty"`
	ProjectID     string `json:"project_id,omitempty"`
	RepoID        string `json:"repo_id,omitempty"`
	AgentID       string `json:"agent_id,omitempty"`
	ActiveTaskID  string `json:"active_task_id,omitempty"`
	ActiveClaimID string `json:"active_claim_id,omitempty"`
	BranchName    string `json:"branch_name,omitempty"`
	DirtyState    string `json:"dirty_state,omitempty"`
	Status        string `json:"status,omitempty"`
	DerivedStatus string `json:"derived_status,omitempty"`
	LocalPathRef  string `json:"local_path_ref,omitempty"`
	LastSeenAt    string `json:"last_seen_at,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

type InternalHeartbeatPatchQueueSummary struct {
	QueueID                 string   `json:"queue_id,omitempty"`
	ItemID                  string   `json:"item_id,omitempty"`
	ProjectID               string   `json:"project_id,omitempty"`
	RepoID                  string   `json:"repo_id,omitempty"`
	BranchID                string   `json:"branch_id,omitempty"`
	BranchName              string   `json:"branch_name,omitempty"`
	BranchStatus            string   `json:"branch_status,omitempty"`
	State                   string   `json:"state,omitempty"`
	HeadSHA                 string   `json:"head_sha,omitempty"`
	RepoAuthorityMode       string   `json:"repo_authority_mode,omitempty"`
	MaterializationAccepted bool     `json:"materialization_accepted,omitempty"`
	MaterializationSchema   string   `json:"materialization_schema,omitempty"`
	MaterializationDigest   string   `json:"materialization_digest,omitempty"`
	SupersedesQueueID       string   `json:"supersedes_queue_id,omitempty"`
	SupersedesItemID        string   `json:"supersedes_item_id,omitempty"`
	ReviewDocKey            string   `json:"review_doc_key,omitempty"`
	EvidenceDocKey          string   `json:"evidence_doc_key,omitempty"`
	DecisionDocKey          string   `json:"decision_doc_key,omitempty"`
	DecisionSummary         string   `json:"decision_summary,omitempty"`
	ClaimedBy               string   `json:"claimed_by,omitempty"`
	ClaimExpiresAt          string   `json:"claim_expires_at,omitempty"`
	UpdatedAt               string   `json:"updated_at,omitempty"`
	DecidedAt               string   `json:"decided_at,omitempty"`
	PathHints               []string `json:"path_hints,omitempty"`
}

type InternalHeartbeatDocSummary struct {
	DocKey    string `json:"doc_key,omitempty"`
	Title     string `json:"title,omitempty"`
	UpdatedBy string `json:"updated_by,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type InternalHeartbeatAgentSummary struct {
	AgentID                 string   `json:"agent_id,omitempty"`
	DisplayName             string   `json:"display_name,omitempty"`
	Role                    string   `json:"role,omitempty"`
	Status                  string   `json:"status,omitempty"`
	Online                  bool     `json:"online,omitempty"`
	LastSeenAt              string   `json:"last_seen_at,omitempty"`
	ActiveTaskIDs           []string `json:"active_task_ids,omitempty"`
	CurrentSessionID        string   `json:"current_session_id,omitempty"`
	CurrentSessionStatus    string   `json:"current_session_status,omitempty"`
	CurrentSessionUpdatedAt string   `json:"current_session_updated_at,omitempty"`
	CurrentSessionSummary   string   `json:"current_session_summary,omitempty"`
}

type InternalHeartbeatProjectRoleSummary struct {
	RoleID    string `json:"role_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
	AgentID   string `json:"agent_id,omitempty"`
	RoleType  string `json:"role_type,omitempty"`
	Status    string `json:"status,omitempty"`
	Summary   string `json:"summary,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type InternalHeartbeatServiceRunSummary struct {
	RunID            string `json:"run_id,omitempty"`
	CandidateID      string `json:"candidate_id,omitempty"`
	ProjectID        string `json:"project_id,omitempty"`
	Title            string `json:"title,omitempty"`
	Status           string `json:"status,omitempty"`
	DeployTarget     string `json:"deploy_target,omitempty"`
	PublicURL        string `json:"public_url,omitempty"`
	HealthCheckURL   string `json:"health_check_url,omitempty"`
	CredentialPolicy string `json:"credential_policy,omitempty"`
	NextAction       string `json:"next_action,omitempty"`
	UpdatedAt        string `json:"updated_at,omitempty"`
}

type InternalHeartbeatRecentUpdateSummary struct {
	AgentID    string `json:"agent_id,omitempty"`
	UpdateType string `json:"update_type,omitempty"`
	Summary    string `json:"summary,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
}

type InternalHeartbeatRunnableSurface struct {
	URL                  string `json:"url,omitempty"`
	SourceKind           string `json:"source_kind,omitempty"`
	SourceRef            string `json:"source_ref,omitempty"`
	Label                string `json:"label,omitempty"`
	Confidence           int    `json:"confidence,omitempty"`
	Reason               string `json:"reason,omitempty"`
	Localhost            bool   `json:"localhost,omitempty"`
	VerificationRequired bool   `json:"verification_required,omitempty"`
}

type InternalHeartbeatBrowserProbe struct {
	URL                        string `json:"url,omitempty"`
	Status                     string `json:"status,omitempty"`
	HTTPStatus                 int    `json:"http_status,omitempty"`
	ContentType                string `json:"content_type,omitempty"`
	Title                      string `json:"title,omitempty"`
	MatchedMarker              string `json:"matched_marker,omitempty"`
	MarkerSource               string `json:"marker_source,omitempty"`
	ProductMarkerVerified      bool   `json:"product_marker_verified,omitempty"`
	VisualVerificationRequired bool   `json:"visual_verification_required"`
	Localhost                  bool   `json:"localhost,omitempty"`
	DurationMillis             int64  `json:"duration_millis,omitempty"`
	Error                      string `json:"error,omitempty"`
}

type InternalHeartbeatVisualAuditPlan struct {
	Status                     string                                 `json:"status,omitempty"`
	Summary                    string                                 `json:"summary,omitempty"`
	SurfaceURL                 string                                 `json:"surface_url,omitempty"`
	ProductMarkerVerified      bool                                   `json:"product_marker_verified,omitempty"`
	ExistingEvidenceSatisfied  bool                                   `json:"existing_evidence_satisfied"`
	ExistingEvidenceDocKeys    []string                               `json:"existing_evidence_doc_keys,omitempty"`
	MissingEvidence            []string                               `json:"missing_evidence,omitempty"`
	BlockingEvidence           []string                               `json:"blocking_evidence,omitempty"`
	Viewports                  []InternalHeartbeatVisualAuditViewport `json:"viewports,omitempty"`
	Scenarios                  []InternalHeartbeatVisualAuditScenario `json:"scenarios,omitempty"`
	RequiredChecks             []string                               `json:"required_checks,omitempty"`
	RequiredArtifactProperties []string                               `json:"required_artifact_properties,omitempty"`
	EvidenceRequired           bool                                   `json:"evidence_required,omitempty"`
	EvidenceRequests           []InternalHeartbeatEvidenceRequest     `json:"evidence_requests,omitempty"`
	VisualVerdictAllowed       bool                                   `json:"visual_verdict_allowed"`
}

type InternalHeartbeatEvidenceRequest struct {
	RequestID            string `json:"request_id,omitempty"`
	Kind                 string `json:"kind,omitempty"`
	SurfaceURL           string `json:"surface_url,omitempty"`
	DimensionID          string `json:"dimension_id,omitempty"`
	Width                int    `json:"width,omitempty"`
	Height               int    `json:"height,omitempty"`
	StateID              string `json:"state_id,omitempty"`
	StateLabel           string `json:"state_label,omitempty"`
	RequiredState        string `json:"required_state,omitempty"`
	Required             bool   `json:"required"`
	ExpectedEvidenceKind string `json:"expected_evidence_kind,omitempty"`
	ArtifactRefHint      string `json:"artifact_ref_hint,omitempty"`
}

type InternalHeartbeatVisualAuditViewport struct {
	ID      string `json:"id,omitempty"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
	Purpose string `json:"purpose,omitempty"`
}

type InternalHeartbeatVisualAuditScenario struct {
	ID                   string `json:"id,omitempty"`
	Label                string `json:"label,omitempty"`
	RequiredState        string `json:"required_state,omitempty"`
	ScreenshotRequired   bool   `json:"screenshot_required"`
	RealUserQuestion     string `json:"real_user_question,omitempty"`
	ExpectedEvidenceKind string `json:"expected_evidence_kind,omitempty"`
}

type InternalHeartbeatLocalResult struct {
	ContractVersion string                              `json:"contract_version"`
	Outcome         string                              `json:"outcome"`
	Summary         string                              `json:"summary"`
	ActiveMemory    []InternalHeartbeatActiveMemoryNote `json:"active_memory,omitempty"`
	BacklogItems    []InternalHeartbeatFinding          `json:"backlog_items,omitempty"`
	ActionRequests  []InternalHeartbeatActionRequest    `json:"action_requests,omitempty"`
	WillDirectives  []InternalHeartbeatWillDirective    `json:"will_directives,omitempty"`
}

type InternalHeartbeatActiveMemoryNote struct {
	Lane         string   `json:"lane,omitempty"`
	Note         string   `json:"note,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
	Tags         []string `json:"tags,omitempty"`
}

type InternalHeartbeatFinding struct {
	DedupKey     string            `json:"dedup_key,omitempty"`
	Kind         string            `json:"kind,omitempty"`
	Source       string            `json:"source,omitempty"`
	ProjectID    string            `json:"project_id,omitempty"`
	ProjectLane  string            `json:"project_lane,omitempty"`
	BlockPromote bool              `json:"block_promote,omitempty"`
	Title        string            `json:"title,omitempty"`
	Summary      string            `json:"summary,omitempty"`
	Score        int               `json:"score,omitempty"`
	EvidenceRefs []string          `json:"evidence_refs,omitempty"`
	Promote      bool              `json:"promote,omitempty"`
	Reason       string            `json:"reason,omitempty"`
	Meta         map[string]string `json:"meta,omitempty"`
}

func (finding *InternalHeartbeatFinding) UnmarshalJSON(data []byte) error {
	type findingWire struct {
		DedupKey     string            `json:"dedup_key,omitempty"`
		Kind         string            `json:"kind,omitempty"`
		Source       string            `json:"source,omitempty"`
		ProjectID    string            `json:"project_id,omitempty"`
		ProjectLane  string            `json:"project_lane,omitempty"`
		BlockPromote bool              `json:"block_promote,omitempty"`
		Title        string            `json:"title,omitempty"`
		Summary      string            `json:"summary,omitempty"`
		Score        json.RawMessage   `json:"score,omitempty"`
		EvidenceRefs []string          `json:"evidence_refs,omitempty"`
		Promote      bool              `json:"promote,omitempty"`
		Reason       string            `json:"reason,omitempty"`
		Meta         map[string]string `json:"meta,omitempty"`
	}
	var wire findingWire
	if err := decodeStrictJSONObject(data, &wire); err != nil {
		return err
	}
	score, err := parseInternalHeartbeatScore(wire.Score)
	if err != nil {
		return err
	}
	*finding = InternalHeartbeatFinding{
		DedupKey:     wire.DedupKey,
		Kind:         wire.Kind,
		Source:       wire.Source,
		ProjectID:    wire.ProjectID,
		ProjectLane:  wire.ProjectLane,
		BlockPromote: wire.BlockPromote,
		Title:        wire.Title,
		Summary:      wire.Summary,
		Score:        score,
		EvidenceRefs: wire.EvidenceRefs,
		Promote:      wire.Promote,
		Reason:       wire.Reason,
		Meta:         wire.Meta,
	}
	return nil
}

type InternalHeartbeatActionRequest struct {
	RequestID          string   `json:"request_id,omitempty"`
	Capability         string   `json:"capability,omitempty"`
	ToolSuite          string   `json:"tool_suite,omitempty"`
	ProjectID          string   `json:"project_id,omitempty"`
	ProjectLane        string   `json:"project_lane,omitempty"`
	Title              string   `json:"title,omitempty"`
	Summary            string   `json:"summary,omitempty"`
	Score              int      `json:"score,omitempty"`
	EvidenceRefs       []string `json:"evidence_refs,omitempty"`
	Promote            bool     `json:"promote,omitempty"`
	Reason             string   `json:"reason,omitempty"`
	RequiresTaskLoop   bool     `json:"requires_task_loop,omitempty"`
	RequiresHumanInput bool     `json:"requires_human_input,omitempty"`
}

type InternalHeartbeatWillDirective struct {
	DirectiveID  string   `json:"directive_id,omitempty"`
	Action       string   `json:"action,omitempty"`
	TaskID       string   `json:"task_id,omitempty"`
	SessionID    string   `json:"session_id,omitempty"`
	Summary      string   `json:"summary,omitempty"`
	Reason       string   `json:"reason,omitempty"`
	Priority     string   `json:"priority,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

func (request *InternalHeartbeatActionRequest) UnmarshalJSON(data []byte) error {
	type requestWire struct {
		RequestID          string          `json:"request_id,omitempty"`
		Capability         string          `json:"capability,omitempty"`
		ToolSuite          string          `json:"tool_suite,omitempty"`
		ProjectID          string          `json:"project_id,omitempty"`
		ProjectLane        string          `json:"project_lane,omitempty"`
		Title              string          `json:"title,omitempty"`
		Summary            string          `json:"summary,omitempty"`
		Score              json.RawMessage `json:"score,omitempty"`
		EvidenceRefs       []string        `json:"evidence_refs,omitempty"`
		Promote            bool            `json:"promote,omitempty"`
		Reason             string          `json:"reason,omitempty"`
		RequiresTaskLoop   bool            `json:"requires_task_loop,omitempty"`
		RequiresHumanInput bool            `json:"requires_human_input,omitempty"`
	}
	var wire requestWire
	if err := decodeStrictJSONObject(data, &wire); err != nil {
		return err
	}
	score, err := parseInternalHeartbeatScore(wire.Score)
	if err != nil {
		return err
	}
	*request = InternalHeartbeatActionRequest{
		RequestID:          wire.RequestID,
		Capability:         wire.Capability,
		ToolSuite:          wire.ToolSuite,
		ProjectID:          wire.ProjectID,
		ProjectLane:        wire.ProjectLane,
		Title:              wire.Title,
		Summary:            wire.Summary,
		Score:              score,
		EvidenceRefs:       wire.EvidenceRefs,
		Promote:            wire.Promote,
		Reason:             wire.Reason,
		RequiresTaskLoop:   wire.RequiresTaskLoop,
		RequiresHumanInput: wire.RequiresHumanInput,
	}
	return nil
}

func internalHeartbeatExecutionPolicy(spec AgentHeartbeatSpec) InternalHeartbeatExecutionPolicy {
	spec = normalizeAgentHeartbeatSpec(spec)
	policy := InternalHeartbeatExecutionPolicy{
		HeartbeatID:       spec.ID,
		Kind:              spec.Kind,
		ToolSuites:        append([]string(nil), spec.ToolSuites...),
		ContextSelectors:  append([]string(nil), spec.ContextSelectors...),
		OutputContracts:   append([]string(nil), spec.OutputContracts...),
		PromotionSignals:  append([]string(nil), spec.PromotionSignals...),
		AllowLLM:          true,
		MaxTaskSubmits:    0,
		MaxToolIterations: spec.MaxToolIterations,
		RequireSession:    true,
	}
	if strings.EqualFold(strings.TrimSpace(spec.ID), "personal_backlog_arbiter") {
		policy.AllowLLM = false
	}
	policy.RequiresTaskLoop = containsTrimmedString(spec.ToolSuites, "task_authority") || strings.EqualFold(spec.Kind, "task_execution")
	policy.LocalOnly = containsTrimmedString(spec.Locks, "local_only") || !heartbeatHasPublicAuthoritySuite(spec)
	policy.AllowTaskSubmit = !policy.LocalOnly && containsTrimmedString(spec.ToolSuites, "bounded_task_submit")
	policy.AllowPublicDocs = !policy.LocalOnly && (containsTrimmedString(spec.ToolSuites, "workspace_tools") || containsTrimmedString(spec.ToolSuites, "task_authority"))
	if spec.WillPolicy != nil && spec.WillPolicy.Enabled != nil && *spec.WillPolicy.Enabled {
		policy.AllowWillDirectives = true
		policy.WillActions = append([]string(nil), spec.WillPolicy.AllowedActions...)
		policy.MaxWillDirectives = spec.WillPolicy.MaxDirectives
		policy.WillPublishVisibility = spec.WillPolicy.PublishVisibility
		if spec.WillPolicy.RequiresEvidence != nil {
			policy.WillRequiresEvidence = *spec.WillPolicy.RequiresEvidence
		}
	}
	if policy.AllowWillDirectives && len(policy.WillActions) == 0 {
		policy.WillActions = []string{"advisory_signal"}
	}
	if policy.MaxWillDirectives <= 0 {
		policy.MaxWillDirectives = 1
	}
	if policy.WillPublishVisibility == "" {
		policy.WillPublishVisibility = "private"
	}
	if policy.AllowTaskSubmit {
		policy.MaxTaskSubmits = 1
	}
	policy.AllowAgentRequest = !policy.LocalOnly && (containsTrimmedString(spec.ToolSuites, "bounded_task_submit") || containsTrimmedString(spec.ToolSuites, "task_authority"))
	policy.ExpectsLocalMemory = containsTrimmedString(spec.OutputContracts, "local_memory")
	if policy.LocalOnly {
		policy.AllowTaskSubmit = false
		policy.AllowPublicDocs = false
		policy.AllowAgentRequest = false
		policy.RequiresTaskLoop = false
		policy.MaxTaskSubmits = 0
		policy.ToolSuites = localOnlyHeartbeatToolSuites(policy.ToolSuites)
	}
	if policy.MaxToolIterations < 0 {
		policy.MaxToolIterations = 0
	}
	return policy
}

func heartbeatHasPublicAuthoritySuite(spec AgentHeartbeatSpec) bool {
	for _, suite := range spec.ToolSuites {
		switch strings.TrimSpace(suite) {
		case "bounded_task_submit", "task_authority", "workspace_tools", "local_execution", "project_governance_review":
			return true
		}
	}
	return false
}

func localOnlyHeartbeatToolSuites(suites []string) []string {
	out := make([]string, 0, len(suites))
	for _, suite := range suites {
		switch strings.TrimSpace(suite) {
		case "task_authority", "workspace_tools", "local_execution", "bounded_task_submit":
			continue
		default:
			out = append(out, suite)
		}
	}
	return uniqueTrimmedCSVStrings(out)
}

func internalHeartbeatActionPolicySummary(policy InternalHeartbeatExecutionPolicy) InternalHeartbeatActionPolicy {
	allowed := []string{
		"compact_context_read",
		"local_personal_backlog",
		"strict_json_result",
	}
	blocked := []string{}
	blockedSuites := []string{}
	boundary := "local_memory_only"
	if policy.MaxToolIterations > 0 {
		if internalHeartbeatAnyReadOnlyToolSuite(policy.ToolSuites) {
			allowed = append(allowed, "read_only_tool_loop")
		}
	}
	if policy.AllowTaskSubmit {
		allowed = append(allowed, "bounded_task_submit")
		boundary = "bounded_public_promotion"
	} else {
		blocked = append(blocked, "public_task_submit")
	}
	if policy.AllowPublicDocs {
		allowed = append(allowed, "workspace_doc_write")
	} else {
		blocked = append(blocked, "workspace_doc_write")
	}
	if policy.AllowAgentRequest {
		allowed = append(allowed, "narrow_delegate_task")
	} else {
		blocked = append(blocked, "agent_request")
	}
	if policy.AllowWillDirectives {
		allowed = append(allowed, "will_directives")
	} else {
		blocked = append(blocked, "will_directives")
	}
	if policy.LocalOnly {
		boundary = "local_only"
	}
	for _, suite := range []string{"browser_read_only", "screenshot_capture", "console_read", "local_execution", "workspace_tools", "task_authority"} {
		if containsTrimmedString(policy.ToolSuites, suite) && !internalHeartbeatToolSuiteHasExecutableHeartbeatManifest(policy, suite) {
			blockedSuites = append(blockedSuites, suite)
		}
	}
	if containsTrimmedString(policy.ToolSuites, "browser_unrestricted") || containsTrimmedString(policy.ToolSuites, "browser_interactive") {
		allowed = append(allowed, "browser_session")
	}
	if containsTrimmedString(policy.ToolSuites, "browser_unrestricted") || containsTrimmedString(policy.ToolSuites, "browser_interactive") || containsTrimmedString(policy.ToolSuites, "browser_read_only") || containsTrimmedString(policy.ToolSuites, "screenshot_capture") {
		allowed = append(allowed, "browser_visual_probe")
	}
	blocked = append(blocked, "shell_execution", "source_file_mutation")
	if !containsTrimmedString(policy.ToolSuites, "browser_unrestricted") {
		blocked = append(blocked, "browser_interaction")
	}
	if !containsTrimmedString(policy.ToolSuites, "screenshot_capture") {
		blocked = append(blocked, "screenshot_capture")
	}
	if policy.RequiresTaskLoop {
		boundary = "task_loop_required"
	}
	return InternalHeartbeatActionPolicy{
		AuthorityBoundary:   boundary,
		AllowedCapabilities: uniqueTrimmedCSVStrings(allowed),
		BlockedCapabilities: uniqueTrimmedCSVStrings(blocked),
		BlockedToolSuites:   uniqueTrimmedCSVStrings(blockedSuites),
		ActionRequestFormat: "Use action_requests[] for unavailable capabilities; keep them local unless this heartbeat has bounded_task_submit and a high-confidence promote=true request.",
	}
}

func internalHeartbeatActiveMemoryPolicySummary(spec AgentHeartbeatSpec) InternalHeartbeatActiveMemoryPolicy {
	spec = normalizeAgentHeartbeatSpec(spec)
	if spec.ActiveMemory == nil {
		return InternalHeartbeatActiveMemoryPolicy{}
	}
	policy := InternalHeartbeatActiveMemoryPolicy{
		Lane:                    strings.TrimSpace(spec.ActiveMemory.Lane),
		MaxEntries:              spec.ActiveMemory.MaxEntries,
		IncludeSessionSummaries: true,
		IncludeBacklog:          true,
	}
	if spec.ActiveMemory.IncludeSessionSummaries != nil {
		policy.IncludeSessionSummaries = *spec.ActiveMemory.IncludeSessionSummaries
	}
	if spec.ActiveMemory.IncludeBacklog != nil {
		policy.IncludeBacklog = *spec.ActiveMemory.IncludeBacklog
	}
	if policy.MaxEntries <= 0 {
		policy.MaxEntries = 6
	}
	if policy.MaxEntries > 20 {
		policy.MaxEntries = 20
	}
	return policy
}

func internalHeartbeatActiveMemoryEntries(state AgentInternalSessionState, spec AgentHeartbeatSpec, policy InternalHeartbeatActiveMemoryPolicy) []InternalHeartbeatActiveMemoryEntry {
	if strings.TrimSpace(policy.Lane) == "" || policy.MaxEntries <= 0 {
		return nil
	}
	out := make([]InternalHeartbeatActiveMemoryEntry, 0, policy.MaxEntries)
	if policy.IncludeSessionSummaries {
		sessions := append([]AgentInternalSessionRecord(nil), state.Sessions...)
		sort.SliceStable(sessions, func(i, j int) bool {
			return strings.TrimSpace(sessions[i].StartedAt) > strings.TrimSpace(sessions[j].StartedAt)
		})
		for _, session := range sessions {
			if strings.TrimSpace(session.HeartbeatID) != strings.TrimSpace(spec.ID) || strings.TrimSpace(session.Status) == "running" {
				continue
			}
			for _, note := range internalHeartbeatActiveMemoryNotesFromMeta(session.Meta) {
				out = append(out, InternalHeartbeatActiveMemoryEntry{
					Source:       "active_memory_note",
					Lane:         firstNonEmpty(note.Lane, policy.Lane),
					SessionID:    strings.TrimSpace(session.SessionID),
					Outcome:      strings.TrimSpace(session.Outcome),
					Summary:      internalHeartbeatSurfaceField(note.Note, 320),
					EvidenceRefs: uniqueTrimmedCSVStrings(note.EvidenceRefs),
					At:           firstNonEmpty(session.EndedAt, session.StartedAt),
				})
				if len(out) >= policy.MaxEntries {
					return out
				}
			}
			summary := internalHeartbeatSurfaceField(firstNonEmpty(session.Summary, session.Outcome), 320)
			if summary == "" {
				continue
			}
			out = append(out, InternalHeartbeatActiveMemoryEntry{
				Source:    "session_summary",
				Lane:      policy.Lane,
				SessionID: strings.TrimSpace(session.SessionID),
				Outcome:   strings.TrimSpace(session.Outcome),
				Summary:   summary,
				At:        firstNonEmpty(session.EndedAt, session.StartedAt),
			})
			if len(out) >= policy.MaxEntries {
				return out
			}
		}
	}
	if policy.IncludeBacklog {
		backlog := append([]AgentPersonalBacklogItem(nil), state.Backlog...)
		sort.SliceStable(backlog, func(i, j int) bool {
			return strings.TrimSpace(backlog[i].UpdatedAt) > strings.TrimSpace(backlog[j].UpdatedAt)
		})
		for _, item := range backlog {
			if strings.TrimSpace(item.HeartbeatID) != strings.TrimSpace(spec.ID) {
				continue
			}
			summary := internalHeartbeatSurfaceField(firstNonEmpty(item.Summary, item.Title), 320)
			if summary == "" {
				continue
			}
			out = append(out, InternalHeartbeatActiveMemoryEntry{
				Source:        "personal_backlog",
				Lane:          policy.Lane,
				BacklogItemID: strings.TrimSpace(item.ItemID),
				Outcome:       strings.TrimSpace(item.Status),
				Summary:       summary,
				EvidenceRefs:  uniqueTrimmedCSVStrings(item.EvidenceRefs),
				At:            firstNonEmpty(item.UpdatedAt, item.LastSeenAt, item.CreatedAt),
			})
			if len(out) >= policy.MaxEntries {
				return out
			}
		}
	}
	return out
}

func internalHeartbeatActiveMemoryNotesFromMeta(meta map[string]string) []InternalHeartbeatActiveMemoryNote {
	if len(meta) == 0 {
		return nil
	}
	count, _ := strconv.Atoi(strings.TrimSpace(meta["active_memory_note_count"]))
	if count <= 0 {
		return nil
	}
	out := make([]InternalHeartbeatActiveMemoryNote, 0, count)
	for i := 1; i <= count; i++ {
		raw := strings.TrimSpace(meta[fmt.Sprintf("active_memory_note_%d", i)])
		if raw == "" {
			continue
		}
		var note InternalHeartbeatActiveMemoryNote
		if err := json.Unmarshal([]byte(raw), &note); err != nil {
			continue
		}
		note = normalizeInternalHeartbeatActiveMemoryNote(note)
		if strings.TrimSpace(note.Note) != "" {
			out = append(out, note)
		}
	}
	return out
}

func internalHeartbeatAnyReadOnlyToolSuite(suites []string) bool {
	for _, suite := range suites {
		switch strings.TrimSpace(suite) {
		case "memory_and_docs_read", "workspace_docs_read", "local_log_read", "local_tests_read", "rhizome_read", "patch_queue_read":
			return true
		}
	}
	return false
}

func internalHeartbeatToolSuiteHasExecutableHeartbeatManifest(policy InternalHeartbeatExecutionPolicy, suite string) bool {
	switch strings.TrimSpace(suite) {
	case "browser_unrestricted", "browser_interactive", "browser_read_only", "screenshot_capture":
		return true
	case "console_read":
		return false
	case "local_execution":
		return policy.RequiresTaskLoop
	case "workspace_tools", "task_authority":
		return !policy.LocalOnly && policy.RequiresTaskLoop
	default:
		return true
	}
}

func (policy InternalHeartbeatExecutionPolicy) AllowsTool(toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return false
	}
	if policy.LocalOnly && internalHeartbeatToolHasPublicSideEffect(toolName) {
		return false
	}
	if policy.RequiresTaskLoop {
		return true
	}
	if toolName == "task_submit" {
		return policy.AllowTaskSubmit
	}
	if toolName == "workspace_doc_put" {
		return policy.AllowPublicDocs
	}
	if toolName == "agent_request" {
		return policy.AllowAgentRequest
	}
	for _, suite := range policy.ToolSuites {
		if internalHeartbeatToolSuiteAllows(suite, toolName) {
			return true
		}
	}
	return false
}

func internalHeartbeatToolHasPublicSideEffect(toolName string) bool {
	switch strings.TrimSpace(toolName) {
	case "task_submit",
		"agent_request",
		"workspace_doc_put",
		"project_bootstrap",
		"project_role_assign",
		"project_phase_transition",
		"project_checkout_materialize",
		"project_governance_challenge",
		"project_branch_commit",
		"project_branch_review_ready",
		"project_patch_queue_materialize",
		"project_patch_queue_cas_record",
		"service_direction_upsert",
		"service_candidate_upsert",
		"service_run_start",
		"budget_account_ensure",
		"budget_reservation_create",
		"budget_spend_record":
		return true
	default:
		return false
	}
}

func internalHeartbeatToolSuiteAllows(suite, toolName string) bool {
	suite = strings.TrimSpace(suite)
	toolName = strings.TrimSpace(toolName)
	switch suite {
	case "memory_and_docs_read":
		return stringInSet(toolName, "memory_read", "memory_search", "memory_coherence_read", "memory_promotion_read", "workspace_doc_get")
	case "workspace_docs_read":
		return toolName == "workspace_doc_get"
	case "local_log_read", "local_tests_read":
		return stringInSet(toolName, "read_file", "list_directory")
	case "rhizome_read":
		return stringInSet(toolName,
			"workspace_doc_get",
			"project_patch_queue_list",
			"coalition_status",
			"reviewer_route",
			"reviewer_scarcity",
			"service_direction_list",
			"service_direction_get",
			"service_candidate_list",
			"service_candidate_get",
			"service_run_list",
			"service_run_get",
			"service_coordination_get",
			"budget_account_get",
			"budget_ledger_list",
			"budget_reservations_list",
		)
	case "project_governance_review":
		return stringInSet(toolName,
			"workspace_doc_get",
			"project_patch_queue_list",
			"project_governance_challenge",
		)
	case "patch_queue_read":
		return toolName == "project_patch_queue_list"
	case "bounded_task_submit":
		return toolName == "task_submit" || toolName == "workspace_doc_get"
	case "browser_read_only":
		return toolName == "browser_visual_probe"
	case "browser_unrestricted":
		return toolName == "browser_visual_probe" || toolName == "browser_session"
	case "browser_interactive":
		return toolName == "browser_session" || toolName == "browser_visual_probe"
	case "screenshot_capture":
		return toolName == "browser_visual_probe"
	case "console_read":
		return false
	case "task_authority", "workspace_tools", "local_execution":
		return true
	default:
		return false
	}
}

func internalHeartbeatReadOnlyToolLoopAllows(policy InternalHeartbeatExecutionPolicy, toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" || !policy.AllowsTool(toolName) {
		return false
	}
	return stringInSet(toolName,
		"read_file",
		"list_directory",
		"memory_read",
		"memory_search",
		"memory_coherence_read",
		"memory_promotion_read",
		"workspace_doc_get",
		"project_patch_queue_list",
		"project_governance_challenge",
		"browser_session",
		"browser_visual_probe",
		"coalition_status",
		"reviewer_route",
		"reviewer_scarcity",
		"service_direction_list",
		"service_direction_get",
		"service_candidate_list",
		"service_candidate_get",
		"service_run_list",
		"service_run_get",
		"service_coordination_get",
		"budget_account_get",
		"budget_ledger_list",
		"budget_reservations_list",
	)
}

func internalHeartbeatReadOnlyToolLoopAllowsWithRegistry(policy InternalHeartbeatExecutionPolicy, registry *ToolRegistry, toolName string) bool {
	if internalHeartbeatReadOnlyToolLoopAllows(policy, toolName) {
		return true
	}
	return internalHeartbeatRegistryToolAllowedByManifest(policy, registry, toolName)
}

func internalHeartbeatRegistryToolAllowedByManifest(policy InternalHeartbeatExecutionPolicy, registry *ToolRegistry, toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" || registry == nil {
		return false
	}
	if policy.LocalOnly && internalHeartbeatToolHasPublicSideEffect(toolName) {
		return false
	}
	tool, ok := registry.Get(toolName)
	if !ok {
		return false
	}
	bundle, ok := tool.(*InstalledToolBundleTool)
	if !ok || bundle == nil {
		return false
	}
	for _, suite := range policy.ToolSuites {
		suite = strings.TrimSpace(suite)
		if suite == "" || !containsTrimmedString(bundle.manifest.CapabilitySuites, suite) {
			continue
		}
		switch suite {
		case "browser_read_only", "screenshot_capture", "console_read", "local_log_read", "local_tests_read":
			return true
		}
	}
	return false
}

func (r *Runtime) internalHeartbeatToolExecutorWithPolicy(ctx context.Context, registry *ToolRegistry, call ToolCall, policy InternalHeartbeatExecutionPolicy, taskSubmitCount *int) ToolResult {
	toolName := strings.TrimSpace(call.Function.Name)
	if !policy.AllowsTool(toolName) && !internalHeartbeatRegistryToolAllowedByManifest(policy, registry, toolName) {
		return ToolResult{
			Output:  fmt.Sprintf("internal heartbeat %s blocked tool %s by typed execution policy; claim/create a concrete task or use a heartbeat with an explicit tool suite before public authority changes.", firstNonEmpty(policy.HeartbeatID, "unknown"), firstNonEmpty(toolName, "unknown")),
			IsError: true,
		}
	}
	if toolName == "agent_request" && !ambientAutonomyAgentRequestAllowed(call) && !policy.RequiresTaskLoop {
		return ToolResult{Output: "internal heartbeat blocked broad agent_request; use delegate_task with an exact existing task_id or promote a bounded task first.", IsError: true}
	}
	if toolName == "task_submit" {
		if !policy.AllowTaskSubmit {
			return ToolResult{Output: "internal heartbeat blocked task_submit because this heartbeat is local-only or lacks bounded_task_submit.", IsError: true}
		}
		if taskSubmitCount != nil && policy.MaxTaskSubmits > 0 && *taskSubmitCount >= policy.MaxTaskSubmits {
			return ToolResult{Output: fmt.Sprintf("internal heartbeat blocked task_submit after reaching max_task_submits=%d", policy.MaxTaskSubmits), IsError: true}
		}
		call = internalHeartbeatCallWithDeterministicTaskID(policy, call)
		result := r.executeInternalHeartbeatTool(ctx, registry, call)
		if taskSubmitCount != nil && !result.IsError {
			*taskSubmitCount++
		}
		return result
	}
	return r.executeInternalHeartbeatTool(ctx, registry, call)
}

func (r *Runtime) buildInternalHeartbeatContextPacket(store *AgentInternalSessionStore, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, trigger string, now time.Time) InternalHeartbeatContextPacket {
	spec = normalizeAgentHeartbeatSpec(spec)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var cfg RuntimeConfig
	if r != nil {
		cfg = r.cfg
	}
	var snapshot WorkspaceSnapshot
	var activeScope InternalHeartbeatTrustedScope
	var activeTaskID string
	var activeCoordination ProjectCoordinationRecord
	var hasActiveCoordination bool
	if r != nil {
		r.mu.Lock()
		activeTaskID = firstNonEmpty(strings.TrimSpace(r.scratch.ActiveTaskID), runtimeActiveTaskIDLocked(r))
		snapshot = r.bootstrap.Snapshot
		activeScope = internalHeartbeatActiveScopeLocked(r)
		if r.activeWorkPacket != nil {
			activeCoordination, hasActiveCoordination = internalHeartbeatProjectCoordinationFromRaw(r.activeWorkPacket.ProjectCoordination)
		}
		r.mu.Unlock()
	}
	trustedScope := internalHeartbeatTrustedScope(spec, snapshot, activeScope, AgentPersonalBacklogItem{})
	packet := InternalHeartbeatContextPacket{
		WorkspaceID:        strings.TrimSpace(cfg.WorkspaceID),
		AgentID:            strings.TrimSpace(cfg.AgentID),
		ProfileID:          strings.TrimSpace(runtimeProfileForAnatomy(cfg).AgentID),
		HeartbeatID:        spec.ID,
		HeartbeatKind:      spec.Kind,
		HeartbeatObjective: strings.TrimSpace(spec.Objective),
		HeartbeatTriggers:  append([]string(nil), spec.Triggers...),
		HeartbeatNotes:     append([]string(nil), spec.Notes...),
		Instructions:       append([]string(nil), spec.Instructions...),
		MemoryLanes:        append([]string(nil), spec.MemoryLanes...),
		Trigger:            firstNonEmpty(trigger, "typed_internal_heartbeat"),
		Now:                now.UTC().Format(time.RFC3339Nano),
		ActiveTaskID:       activeTaskID,
		LocalOnly:          policy.LocalOnly,
		AllowTaskSubmit:    policy.AllowTaskSubmit,
		MaxTaskSubmits:     policy.MaxTaskSubmits,
		MaxToolIterations:  policy.MaxToolIterations,
		ToolSuites:         append([]string(nil), policy.ToolSuites...),
		ActionPolicy:       internalHeartbeatActionPolicySummary(policy),
		ContextSelectors:   append([]string(nil), policy.ContextSelectors...),
		OutputContracts:    append([]string(nil), policy.OutputContracts...),
		PromotionSignals:   append([]string(nil), policy.PromotionSignals...),
		TrustedScope:       trustedScope,
		ActiveMemoryPolicy: internalHeartbeatActiveMemoryPolicySummary(spec),
		PolicyInstructions: []string{
			"return strict JSON matching InternalHeartbeatLocalResult",
			"record private observations as local memory/backlog candidates first",
			"when required authority, tools, evidence, or human input are unavailable, emit action_requests instead of pretending the check passed",
			"when the heartbeat detects the agent should change course, emit will_directives within the configured will_policy instead of relying on prose",
			"promote public work only through the bounded promotion path when policy allows it",
			"context selector payloads are compact trusted summaries; do not infer missing raw content",
		},
	}
	if policy.LocalOnly {
		packet.PolicyInstructions = append(packet.PolicyInstructions,
			"local-only heartbeat: do not write workspace docs, submit tasks, request agents, run shell commands, or mutate source files",
		)
	}
	if policy.MaxToolIterations > 0 {
		packet.PolicyInstructions = append(packet.PolicyInstructions,
			fmt.Sprintf("tool-enabled heartbeat: use at most %d tool iteration(s), then return the strict local JSON result", policy.MaxToolIterations),
		)
	}
	if policy.AllowWillDirectives {
		packet.PolicyInstructions = append(packet.PolicyInstructions,
			"will-enabled heartbeat: may emit will_directives with actions limited to "+strings.Join(policy.WillActions, ", "),
		)
		if policy.WillRequiresEvidence {
			packet.PolicyInstructions = append(packet.PolicyInstructions,
				"will directives require evidence_refs or a concrete context packet reference",
			)
		}
	}
	if store != nil {
		state := store.Snapshot()
		packet.RecentSessions = internalHeartbeatSessionSummaries(state.Sessions, 5)
		backlogLimit := 5
		if strings.EqualFold(strings.TrimSpace(spec.ID), "personal_backlog_arbiter") {
			backlogLimit = 20
		}
		packet.BacklogCandidates = internalHeartbeatBacklogSummaries(store.ListBacklogPromotionCandidates(backlogLimit, 0, now))
		packet.ActiveMemory = internalHeartbeatActiveMemoryEntries(state, spec, packet.ActiveMemoryPolicy)
	}
	packet.SelectorPayloads = internalHeartbeatSelectorPayloads(spec.ContextSelectors, snapshot, store, trustedScope, activeCoordination, hasActiveCoordination, strings.TrimSpace(cfg.AgentID), activeTaskID, now)
	internalHeartbeatRefreshRequiredToolArtifacts(&packet, spec)
	return packet
}

func internalHeartbeatRefreshRequiredToolArtifacts(packet *InternalHeartbeatContextPacket, spec AgentHeartbeatSpec) {
	if packet == nil {
		return
	}
	spec = normalizeAgentHeartbeatSpec(spec)
	packet.RequiredToolArtifacts = internalHeartbeatRequiredToolArtifacts(spec, *packet)
	if len(packet.RequiredToolArtifacts) == 0 {
		return
	}
	instruction := "required tool artifact contract: when required_now=true, call the named tool before returning a pass/no_action; if the tool or inputs are unavailable, return an action_request/capability blocker instead of substituting build/source/doc review"
	if !containsTrimmedString(packet.PolicyInstructions, instruction) {
		packet.PolicyInstructions = append(packet.PolicyInstructions, instruction)
	}
}

func internalHeartbeatRequiredToolArtifacts(spec AgentHeartbeatSpec, packet InternalHeartbeatContextPacket) []InternalHeartbeatRequiredToolArtifact {
	if spec.EvidenceContract == nil || len(spec.EvidenceContract.RequiredToolArtifacts) == 0 {
		return nil
	}
	out := make([]InternalHeartbeatRequiredToolArtifact, 0, len(spec.EvidenceContract.RequiredToolArtifacts))
	for _, artifact := range spec.EvidenceContract.RequiredToolArtifacts {
		tool := strings.TrimSpace(artifact.Tool)
		if tool == "" {
			continue
		}
		requiredNow, reason := internalHeartbeatRequiredToolArtifactApplies(packet, artifact)
		out = append(out, InternalHeartbeatRequiredToolArtifact{
			Tool:            tool,
			ContractVersion: strings.TrimSpace(artifact.ContractVersion),
			Capability:      strings.TrimSpace(artifact.Capability),
			ToolSuite:       strings.TrimSpace(artifact.ToolSuite),
			When:            strings.TrimSpace(artifact.When),
			RequiredNow:     requiredNow,
			Reason:          reason,
			Purpose:         strings.TrimSpace(artifact.Purpose),
			BlockerGuidance: strings.TrimSpace(artifact.BlockerGuidance),
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func internalHeartbeatRequiredToolArtifactApplies(packet InternalHeartbeatContextPacket, artifact AgentHeartbeatRequiredToolArtifactSpec) (bool, string) {
	when := strings.ToLower(strings.TrimSpace(artifact.When))
	switch when {
	case "", "always":
		return true, "contract condition always applies"
	case "runnable_surface_present", "surface_present":
		if internalHeartbeatPacketHasRunnableSurface(packet) {
			return true, "runnable surface selector found at least one candidate surface"
		}
		return false, "no runnable surface candidate was present"
	case "verified_surface", "surface_preflight_verified", "browser_preflight_verified":
		if internalHeartbeatPacketHasVerifiedSurface(packet) {
			return true, "browser preflight verified the product marker on a runnable surface"
		}
		return false, "no browser preflight verified surface was present"
	case "visual_evidence_required", "evidence_required":
		if internalHeartbeatPacketHasRequiredVisualEvidence(packet) {
			return true, "visual audit plan requires state-specific evidence"
		}
		return false, "no visual audit evidence requirement is active"
	default:
		return true, "custom contract condition " + when + " is treated as active"
	}
}

func internalHeartbeatPacketHasRunnableSurface(packet InternalHeartbeatContextPacket) bool {
	for _, payload := range packet.SelectorPayloads {
		if payload.Selector == "runnable_surface" && len(payload.Surfaces) > 0 {
			return true
		}
	}
	return false
}

func internalHeartbeatPacketHasVerifiedSurface(packet InternalHeartbeatContextPacket) bool {
	return internalHeartbeatPacketHasVerifiedSurfaceForProject(packet, "")
}

func internalHeartbeatPacketHasVerifiedSurfaceForProject(packet InternalHeartbeatContextPacket, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	for _, payload := range packet.SelectorPayloads {
		if payload.Selector != "runnable_surface" {
			continue
		}
		if projectID != "" && !internalHeartbeatSelectorPayloadMatchesProject(payload, projectID) {
			continue
		}
		if payload.Status == "surface_preflight_verified" {
			return true
		}
		for _, probe := range payload.BrowserProbes {
			if probe.ProductMarkerVerified {
				return true
			}
		}
	}
	if internalHeartbeatBacklogHasVerifiedSurfaceForProject(packet, projectID) {
		return true
	}
	return false
}

func internalHeartbeatSelectorPayloadMatchesProject(payload InternalHeartbeatSelectorPacket, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(payload.TrustedScope.ProjectID), projectID) {
		return true
	}
	for _, task := range payload.Tasks {
		if strings.EqualFold(strings.TrimSpace(task.ProjectID), projectID) {
			return true
		}
	}
	for _, project := range payload.Projects {
		if strings.EqualFold(strings.TrimSpace(project.ProjectID), projectID) {
			return true
		}
	}
	for _, surface := range payload.Surfaces {
		if internalHeartbeatRunnableSurfaceMatchesProject(surface, projectID) {
			return true
		}
	}
	return false
}

func internalHeartbeatRunnableSurfaceMatchesProject(surface InternalHeartbeatRunnableSurface, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return true
	}
	lowerProjectID := strings.ToLower(projectID)
	for _, value := range []string{surface.SourceRef, surface.Label, surface.Reason} {
		if strings.Contains(strings.ToLower(strings.TrimSpace(value)), lowerProjectID) {
			return true
		}
	}
	return false
}

func internalHeartbeatBacklogHasVerifiedSurfaceForProject(packet InternalHeartbeatContextPacket, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	for _, item := range packet.BacklogCandidates {
		if projectID != "" && !strings.EqualFold(strings.TrimSpace(item.TargetProjectID), projectID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Source), internalHeartbeatVisualSensorSource) {
			continue
		}
		if containsTrimmedString(item.EvidenceRefs, "status:surface_preflight_verified") ||
			containsTrimmedString(item.EvidenceRefs, "browser_probe:verified_product_marker") {
			return true
		}
	}
	return false
}

func internalHeartbeatPacketHasRequiredVisualEvidence(packet InternalHeartbeatContextPacket) bool {
	for _, payload := range packet.SelectorPayloads {
		if payload.VisualAudit != nil && payload.VisualAudit.EvidenceRequired {
			return true
		}
	}
	return false
}

func runtimeActiveTaskIDLocked(r *Runtime) string {
	if r == nil || r.activeTask == nil {
		return ""
	}
	return strings.TrimSpace(r.activeTask.TaskID)
}

func runtimeActiveSessionIDLocked(r *Runtime) string {
	if r == nil || r.activeSession == nil {
		return ""
	}
	return strings.TrimSpace(r.activeSession.SessionID)
}

func internalHeartbeatActiveScopeLocked(r *Runtime) InternalHeartbeatTrustedScope {
	if r == nil {
		return InternalHeartbeatTrustedScope{}
	}
	if r.activeTask != nil {
		projectID := strings.TrimSpace(r.activeTask.ProjectID)
		if projectID != "" {
			return InternalHeartbeatTrustedScope{
				ProjectID:   projectID,
				ProjectLane: strings.TrimSpace(r.activeTask.ProjectLane),
				Source:      "active_task",
				Reason:      "active task carries project authority",
			}
		}
	}
	if r.activeWorkPacket != nil {
		projectID := strings.TrimSpace(r.activeWorkPacket.ProjectID)
		if projectID != "" {
			return InternalHeartbeatTrustedScope{
				ProjectID:   projectID,
				ProjectLane: strings.TrimSpace(r.activeWorkPacket.ProjectLane),
				Source:      "active_work_packet",
				Reason:      "active work packet carries project authority",
			}
		}
	}
	return InternalHeartbeatTrustedScope{}
}

func internalHeartbeatTrustedScope(spec AgentHeartbeatSpec, snapshot WorkspaceSnapshot, active InternalHeartbeatTrustedScope, item AgentPersonalBacklogItem) InternalHeartbeatTrustedScope {
	spec = normalizeAgentHeartbeatSpec(spec)
	if strings.TrimSpace(active.ProjectID) != "" || strings.TrimSpace(active.Source) != "" {
		if strings.TrimSpace(active.ProjectLane) == "" && strings.TrimSpace(active.ProjectID) != "" {
			active.ProjectLane = internalHeartbeatDefaultProjectLane(spec, item)
		}
		return active
	}
	if strings.TrimSpace(snapshot.Workspace.WorkspaceID) != "" || len(snapshot.Projects) > 0 || len(snapshot.Tasks) > 0 || len(snapshot.Docs) > 0 {
		return InternalHeartbeatTrustedScope{
			Source: "workspace_snapshot_context",
			Reason: "snapshot is prompt context only and does not grant public project authority",
		}
	}
	return InternalHeartbeatTrustedScope{}
}

func internalHeartbeatProjectCoordinationFromRaw(raw json.RawMessage) (ProjectCoordinationRecord, bool) {
	if len(raw) == 0 || strings.EqualFold(strings.TrimSpace(string(raw)), "null") {
		return ProjectCoordinationRecord{}, false
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(raw, &coordination); err != nil {
		return ProjectCoordinationRecord{}, false
	}
	if strings.TrimSpace(coordination.Project.ProjectID) != "" ||
		strings.TrimSpace(coordination.Profile.ProjectID) != "" ||
		coordination.StrategicLead != nil ||
		len(coordination.Roles) > 0 ||
		len(coordination.Repositories) > 0 ||
		len(coordination.Checkouts) > 0 ||
		len(coordination.Branches) > 0 ||
		len(coordination.PatchQueueItems) > 0 ||
		len(coordination.ServiceRuns) > 0 {
		return coordination, true
	}
	return ProjectCoordinationRecord{}, false
}

func internalHeartbeatSelectorPayloads(selectors []string, snapshot WorkspaceSnapshot, store *AgentInternalSessionStore, trustedScope InternalHeartbeatTrustedScope, coordination ProjectCoordinationRecord, hasCoordination bool, agentID, activeTaskID string, now time.Time) []InternalHeartbeatSelectorPacket {
	selectors = uniqueTrimmedCSVStrings(selectors)
	if len(selectors) == 0 {
		return nil
	}
	backlog := []InternalHeartbeatBacklogSummary(nil)
	if store != nil {
		backlog = internalHeartbeatBacklogSummaries(store.ListBacklogPromotionCandidates(8, 0, now))
	}
	out := make([]InternalHeartbeatSelectorPacket, 0, len(selectors))
	for _, selector := range selectors {
		packet := InternalHeartbeatSelectorPacket{
			Selector:     selector,
			Status:       "hydrated",
			TrustedScope: trustedScope,
		}
		switch selector {
		case "workspace_state", "workspace_queue":
			packet.Summary = "Compact workspace/project/task state from the latest runtime snapshot."
			packet.Workspace = internalHeartbeatWorkspaceSummary(snapshot)
			packet.Projects = internalHeartbeatProjectSummaries(snapshot.Projects, snapshot.Tasks, 6)
			packet.Tasks = internalHeartbeatTaskSummaries(snapshot.Tasks, nil, 12)
		case "active_task_state", "hydrated_task":
			packet.Summary = "Active and claimed task metadata only."
			packet.Tasks = internalHeartbeatTaskSummaries(snapshot.Tasks, func(task WorkspaceTaskRecord) bool {
				return taskClaimStatus(task) != "" || strings.EqualFold(strings.TrimSpace(task.Status), "RUNNING")
			}, 8)
			if hasCoordination {
				packet.Checkouts = internalHeartbeatCheckoutSummaries(coordination.Checkouts, func(checkout ProjectCheckoutRecord) bool {
					return internalHeartbeatCheckoutMatchesAgentOrTask(checkout, agentID, activeTaskID)
				}, 6)
			}
			if len(packet.Tasks) == 0 && len(packet.Checkouts) == 0 {
				packet.Status = "empty"
			}
		case "project_contracts", "product_contract", "design_plan", "visual_evidence", "recent_decisions", "review_packets", "recent_verification", "runnable_surface":
			packet.Summary = "Relevant workspace document metadata only; document content is intentionally excluded."
			packet.Docs = internalHeartbeatDocSummaries(snapshot.Docs, selector, 8)
			if selector == "runnable_surface" {
				packet.Summary = "Sanitized candidate runnable product surfaces extracted from selected evidence text; verify product markers before judging UX."
				packet.Surfaces = internalHeartbeatRunnableSurfaceSummaries(snapshot, 8)
				packet.Tasks = internalHeartbeatTaskSummaries(snapshot.Tasks, func(task WorkspaceTaskRecord) bool {
					return internalHeartbeatTextMatchesSelector(selector, strings.Join([]string{task.Title, task.Description, task.TaskKind, task.ProjectLane}, " "))
				}, 8)
				if len(packet.Surfaces) == 0 {
					packet.Status = "missing_surface"
					packet.Summary = "No runnable URL candidate found; browser critic must report missing surface instead of claiming visual pass."
				}
			}
			if len(packet.Docs) == 0 && len(packet.Tasks) == 0 && len(packet.Surfaces) == 0 {
				if selector != "runnable_surface" {
					packet.Status = "empty"
				}
			}
		case "open_quality_gaps", "recent_ui_findings":
			packet.Summary = "Open quality/UI gap candidates from task metadata and relevant doc metadata."
			packet.Tasks = internalHeartbeatTaskSummaries(snapshot.Tasks, func(task WorkspaceTaskRecord) bool {
				if taskSubmitTaskIsTerminal(task) {
					return false
				}
				return internalHeartbeatTextMatchesSelector(selector, strings.Join([]string{task.Title, task.Description, task.TaskKind, task.ProjectLane, strings.Join(task.Tags, " ")}, " "))
			}, 10)
			packet.Docs = internalHeartbeatDocSummaries(snapshot.Docs, selector, 6)
			packet.Backlog = backlog
			if len(packet.Tasks) == 0 && len(packet.Docs) == 0 && len(packet.Backlog) == 0 {
				packet.Status = "empty"
			}
		case "patch_queue", "integration_state", "project_coordination":
			packet.Summary = "Project coordination selector is represented by project/task metadata in this internal heartbeat packet."
			packet.Projects = internalHeartbeatProjectSummaries(snapshot.Projects, snapshot.Tasks, 6)
			taskSource := snapshot.Tasks
			if hasCoordination {
				packet.Checkouts = internalHeartbeatCheckoutSummaries(coordination.Checkouts, nil, 8)
				packet.PatchQueue = internalHeartbeatPatchQueueSummaries(coordination.PatchQueueItems, coordination.Branches, 12)
				packet.Roles = internalHeartbeatProjectRoleSummaries(coordination, 12)
				packet.ServiceRuns = internalHeartbeatServiceRunSummaries(coordination.ServiceRuns, "", 8)
				taskSource = internalHeartbeatMergeWorkspaceTasks(taskSource, coordination.Tasks)
			}
			packet.Tasks = internalHeartbeatPatchQueueTaskSummaries(taskSource, coordination.PatchQueueItems, 16)
			if len(packet.Tasks) == 0 && len(packet.Projects) == 0 && len(packet.Checkouts) == 0 && len(packet.PatchQueue) == 0 && len(packet.Roles) == 0 && len(packet.ServiceRuns) == 0 {
				packet.Status = "empty"
			}
		case "service_pipeline", "service_runs", "portfolio_state":
			packet.Summary = "Compact service-run pipeline metadata from project coordination; credential ids, idempotency keys, budget accounts, and raw service coordination payloads are excluded."
			if hasCoordination {
				packet.ServiceRuns = internalHeartbeatServiceRunSummaries(coordination.ServiceRuns, "", 10)
			}
			packet.Docs = internalHeartbeatDocSummaries(snapshot.Docs, selector, 6)
			packet.Tasks = internalHeartbeatTaskSummaries(snapshot.Tasks, func(task WorkspaceTaskRecord) bool {
				return internalHeartbeatTextMatchesSelector(selector, strings.Join([]string{task.Title, task.Description, task.TaskKind, task.ProjectLane, strings.Join(task.Tags, " ")}, " "))
			}, 8)
			if len(packet.ServiceRuns) == 0 && len(packet.Docs) == 0 && len(packet.Tasks) == 0 {
				packet.Status = "empty"
			}
		case "reflection_boards", "reflection_board", "project_reflection":
			packet.Summary = "Shared reflection-board document metadata and recent reflection updates; raw board/doc bodies are intentionally excluded from the heartbeat packet."
			packet.Docs = internalHeartbeatDocSummaries(snapshot.Docs, selector, 10)
			packet.RecentUpdates = internalHeartbeatRecentUpdateSummaries(snapshot.RecentUpdates, 10)
			if len(packet.Docs) == 0 && len(packet.RecentUpdates) == 0 {
				packet.Status = "empty"
			}
		case "role_coverage", "agent_activity", "active_agents", "peer_roster":
			packet.Summary = "Compact agent/role activity metadata plus recent update summaries; raw session bodies and update payload JSON are excluded."
			packet.Agents = internalHeartbeatAgentSummaries(snapshot.Agents, 12)
			packet.RecentUpdates = internalHeartbeatRecentUpdateSummaries(snapshot.RecentUpdates, 12)
			if hasCoordination {
				packet.Roles = internalHeartbeatProjectRoleSummaries(coordination, 12)
			}
			packet.Tasks = internalHeartbeatTaskSummaries(snapshot.Tasks, func(task WorkspaceTaskRecord) bool {
				return taskClaimStatus(task) != "" || strings.EqualFold(strings.TrimSpace(task.Status), "RUNNING")
			}, 8)
			if len(packet.Agents) == 0 && len(packet.Roles) == 0 && len(packet.RecentUpdates) == 0 && len(packet.Tasks) == 0 {
				packet.Status = "empty"
			}
		case "recent_internal_sessions":
			packet.Summary = "Recent internal session summaries are exposed in the packet-level recent_sessions field."
			packet.Status = "available_in_packet"
		case "local_memory", "role_memory", "recent_memory":
			packet.Summary = "Raw memory bodies are intentionally excluded; local promotion candidates are shown as backlog metadata."
			packet.Status = "metadata_only"
			packet.Backlog = backlog
			if len(packet.Backlog) == 0 {
				packet.Status = "empty"
			}
		case "recent_runtime_events", "throughput_window":
			packet.Summary = "Recent runtime update summaries without raw payload JSON."
			packet.RecentUpdates = internalHeartbeatRecentUpdateSummaries(snapshot.RecentUpdates, 8)
			if selector == "throughput_window" {
				packet.Summary = "Recent runtime updates plus workspace open/blocked/claimed counts as a rolling throughput proxy."
				packet.Workspace = internalHeartbeatWorkspaceSummary(snapshot)
			}
			if len(packet.RecentUpdates) == 0 && packet.Workspace.TaskCount == 0 {
				packet.Status = "empty"
			}
		default:
			packet.Status = "not_hydrated"
			packet.Summary = "Selector is recognized by the role prompt only; no typed hydration is available in this runtime slice."
		}
		out = append(out, packet)
	}
	return out
}

func internalHeartbeatWorkspaceSummary(snapshot WorkspaceSnapshot) InternalHeartbeatWorkspaceSummary {
	statusCounts := map[string]int{}
	laneCounts := map[string]int{}
	openCount := 0
	blockedCount := 0
	claimedCount := 0
	for _, task := range snapshot.Tasks {
		status := strings.ToUpper(firstNonEmpty(task.Status, "UNKNOWN"))
		statusCounts[status]++
		lane := strings.TrimSpace(task.ProjectLane)
		if lane != "" {
			laneCounts[lane]++
		}
		if !taskSubmitTaskIsTerminal(task) {
			openCount++
		}
		if strings.EqualFold(status, "BLOCKED") || taskClaimStatus(task) == "BLOCKED" {
			blockedCount++
		}
		if taskClaimStatus(task) != "" {
			claimedCount++
		}
	}
	return InternalHeartbeatWorkspaceSummary{
		WorkspaceID:        strings.TrimSpace(snapshot.Workspace.WorkspaceID),
		Title:              strings.TrimSpace(snapshot.Workspace.Title),
		ProjectCount:       len(snapshot.Projects),
		TaskCount:          len(snapshot.Tasks),
		OpenTaskCount:      openCount,
		BlockedTaskCount:   blockedCount,
		ClaimedTaskCount:   claimedCount,
		RecentDocCount:     len(snapshot.Docs),
		TaskCountsByStatus: internalHeartbeatNonEmptyIntMap(statusCounts),
		TaskCountsByLane:   internalHeartbeatNonEmptyIntMap(laneCounts),
	}
}

func internalHeartbeatProjectSummaries(projects []ProjectRecord, tasks []WorkspaceTaskRecord, limit int) []InternalHeartbeatProjectSummary {
	if limit <= 0 {
		return nil
	}
	out := make([]InternalHeartbeatProjectSummary, 0, len(projects))
	for _, project := range projects {
		projectID := strings.TrimSpace(project.ProjectID)
		if projectID == "" {
			continue
		}
		summary := InternalHeartbeatProjectSummary{
			ProjectID: projectID,
			Title:     strings.TrimSpace(project.Title),
			Status:    strings.TrimSpace(project.Status),
			TaskCount: project.TaskCount,
		}
		for _, task := range tasks {
			if strings.TrimSpace(task.ProjectID) != projectID {
				continue
			}
			if !taskSubmitTaskIsTerminal(task) {
				summary.OpenTaskCount++
			}
			if strings.EqualFold(strings.TrimSpace(task.Status), "BLOCKED") || taskClaimStatus(task) == "BLOCKED" {
				summary.BlockedTaskCount++
			}
			if taskClaimStatus(task) != "" {
				summary.ClaimedTaskCount++
			}
		}
		if summary.TaskCount == 0 {
			summary.TaskCount = summary.OpenTaskCount
		}
		out = append(out, summary)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Status != out[j].Status {
			return out[i].Status < out[j].Status
		}
		if out[i].Title != out[j].Title {
			return out[i].Title < out[j].Title
		}
		return out[i].ProjectID < out[j].ProjectID
	})
	return internalHeartbeatLimit(out, limit)
}

func internalHeartbeatTaskSummaries(tasks []WorkspaceTaskRecord, include func(WorkspaceTaskRecord) bool, limit int) []InternalHeartbeatTaskSummary {
	if limit <= 0 {
		return nil
	}
	filtered := make([]WorkspaceTaskRecord, 0, len(tasks))
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == "" {
			continue
		}
		if include != nil && !include(task) {
			continue
		}
		filtered = append(filtered, task)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if taskSubmitTaskIsTerminal(filtered[i]) != taskSubmitTaskIsTerminal(filtered[j]) {
			return !taskSubmitTaskIsTerminal(filtered[i])
		}
		if internalHeartbeatPriorityRank(filtered[i].Priority) != internalHeartbeatPriorityRank(filtered[j].Priority) {
			return internalHeartbeatPriorityRank(filtered[i].Priority) < internalHeartbeatPriorityRank(filtered[j].Priority)
		}
		if filtered[i].ProjectID != filtered[j].ProjectID {
			return filtered[i].ProjectID < filtered[j].ProjectID
		}
		if filtered[i].ProjectLane != filtered[j].ProjectLane {
			return filtered[i].ProjectLane < filtered[j].ProjectLane
		}
		if filtered[i].Status != filtered[j].Status {
			return filtered[i].Status < filtered[j].Status
		}
		return filtered[i].TaskID < filtered[j].TaskID
	})
	out := make([]InternalHeartbeatTaskSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, task := range filtered {
		out = append(out, internalHeartbeatTaskSummaryFromTask(task))
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatTaskSummaryFromTask(task WorkspaceTaskRecord) InternalHeartbeatTaskSummary {
	return InternalHeartbeatTaskSummary{
		TaskID:               strings.TrimSpace(task.TaskID),
		Title:                strings.TrimSpace(task.Title),
		Description:          internalHeartbeatSanitizeContextText(task.Description, 240),
		Status:               strings.TrimSpace(task.Status),
		ClaimStatus:          taskClaimStatus(task),
		ClaimAgentID:         strings.TrimSpace(pointerValue(task.ClaimAgentID)),
		ClaimUpdatedAt:       strings.TrimSpace(pointerValue(task.ClaimUpdatedAt)),
		UpdatedAt:            strings.TrimSpace(task.UpdatedAt),
		ProjectID:            strings.TrimSpace(task.ProjectID),
		ProjectLane:          strings.TrimSpace(task.ProjectLane),
		TaskKind:             strings.TrimSpace(task.TaskKind),
		Priority:             strings.TrimSpace(task.Priority),
		TaskRequirementsJSON: strings.TrimSpace(task.TaskRequirementsJSON),
		Tags:                 internalHeartbeatContextTags(task.Tags, 8),
	}
}

func internalHeartbeatPatchQueueTaskSummaries(tasks []WorkspaceTaskRecord, items []ProjectPatchQueueItemRecord, limit int) []InternalHeartbeatTaskSummary {
	if limit <= 0 {
		return nil
	}
	filtered := make([]WorkspaceTaskRecord, 0, len(tasks))
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == "" {
			continue
		}
		if internalHeartbeatWorkspaceTaskPatchQueueExactMatch(task, items) ||
			internalHeartbeatTextMatchesSelector("patch_queue", strings.Join([]string{task.Title, task.Description, task.TaskKind, task.ProjectLane, strings.Join(task.Tags, " ")}, " ")) {
			filtered = append(filtered, task)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if internalHeartbeatPatchQueueTaskSummaryRank(filtered[i], items) != internalHeartbeatPatchQueueTaskSummaryRank(filtered[j], items) {
			return internalHeartbeatPatchQueueTaskSummaryRank(filtered[i], items) < internalHeartbeatPatchQueueTaskSummaryRank(filtered[j], items)
		}
		if taskSubmitTaskIsTerminal(filtered[i]) != taskSubmitTaskIsTerminal(filtered[j]) {
			return !taskSubmitTaskIsTerminal(filtered[i])
		}
		if internalHeartbeatPriorityRank(filtered[i].Priority) != internalHeartbeatPriorityRank(filtered[j].Priority) {
			return internalHeartbeatPriorityRank(filtered[i].Priority) < internalHeartbeatPriorityRank(filtered[j].Priority)
		}
		if filtered[i].ProjectID != filtered[j].ProjectID {
			return filtered[i].ProjectID < filtered[j].ProjectID
		}
		if filtered[i].ProjectLane != filtered[j].ProjectLane {
			return filtered[i].ProjectLane < filtered[j].ProjectLane
		}
		if filtered[i].Status != filtered[j].Status {
			return filtered[i].Status < filtered[j].Status
		}
		return filtered[i].TaskID < filtered[j].TaskID
	})
	out := make([]InternalHeartbeatTaskSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, task := range filtered {
		out = append(out, internalHeartbeatTaskSummaryFromTask(task))
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatPatchQueueTaskSummaryRank(task WorkspaceTaskRecord, items []ProjectPatchQueueItemRecord) int {
	if internalHeartbeatWorkspaceTaskPatchQueueExactMatch(task, items) {
		return 0
	}
	return 1
}

func internalHeartbeatWorkspaceTaskPatchQueueExactMatch(task WorkspaceTaskRecord, items []ProjectPatchQueueItemRecord) bool {
	if len(items) == 0 {
		return false
	}
	summary := InternalHeartbeatTaskSummary{
		TaskRequirementsJSON: strings.TrimSpace(task.TaskRequirementsJSON),
	}
	for _, item := range items {
		if strings.TrimSpace(task.ProjectID) != "" && strings.TrimSpace(item.ProjectID) != "" && strings.TrimSpace(task.ProjectID) != strings.TrimSpace(item.ProjectID) {
			continue
		}
		if internalHeartbeatPatchQueueTaskRequirementsMatch(summary, internalHeartbeatPatchQueueSummaryForMatch(item), "queue") {
			return true
		}
	}
	return false
}

func internalHeartbeatPatchQueueSummaryForMatch(item ProjectPatchQueueItemRecord) InternalHeartbeatPatchQueueSummary {
	return InternalHeartbeatPatchQueueSummary{
		QueueID:                 strings.TrimSpace(item.QueueID),
		ItemID:                  strings.TrimSpace(item.ItemID),
		ProjectID:               strings.TrimSpace(item.ProjectID),
		RepoID:                  strings.TrimSpace(item.RepoID),
		BranchID:                strings.TrimSpace(item.BranchID),
		State:                   strings.ToUpper(strings.TrimSpace(item.State)),
		HeadSHA:                 strings.TrimSpace(item.HeadSHA),
		RepoAuthorityMode:       strings.TrimSpace(item.RepoAuthorityMode),
		MaterializationAccepted: item.MaterializationAccepted,
		MaterializationSchema:   strings.TrimSpace(item.MaterializationSchema),
		MaterializationDigest:   strings.TrimSpace(item.MaterializationDigest),
	}
}

func internalHeartbeatMergeWorkspaceTasks(primary, extra []WorkspaceTaskRecord) []WorkspaceTaskRecord {
	if len(extra) == 0 {
		return primary
	}
	merged := make([]WorkspaceTaskRecord, 0, len(primary)+len(extra))
	seen := map[string]bool{}
	for _, task := range primary {
		id := strings.TrimSpace(task.TaskID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		merged = append(merged, task)
	}
	for _, task := range extra {
		id := strings.TrimSpace(task.TaskID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		merged = append(merged, task)
	}
	return merged
}

func internalHeartbeatCheckoutSummaries(checkouts []ProjectCheckoutRecord, include func(ProjectCheckoutRecord) bool, limit int) []InternalHeartbeatCheckoutSummary {
	if limit <= 0 {
		return nil
	}
	filtered := make([]ProjectCheckoutRecord, 0, len(checkouts))
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.CheckoutID) == "" && strings.TrimSpace(checkout.LocalPath) == "" {
			continue
		}
		if include != nil && !include(checkout) {
			continue
		}
		filtered = append(filtered, checkout)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].UpdatedAt != filtered[j].UpdatedAt {
			return filtered[i].UpdatedAt > filtered[j].UpdatedAt
		}
		if filtered[i].ActiveTaskID != filtered[j].ActiveTaskID {
			return filtered[i].ActiveTaskID < filtered[j].ActiveTaskID
		}
		return filtered[i].CheckoutID < filtered[j].CheckoutID
	})
	out := make([]InternalHeartbeatCheckoutSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, checkout := range filtered {
		out = append(out, InternalHeartbeatCheckoutSummary{
			CheckoutID:    strings.TrimSpace(checkout.CheckoutID),
			ProjectID:     strings.TrimSpace(checkout.ProjectID),
			RepoID:        strings.TrimSpace(checkout.RepoID),
			AgentID:       strings.TrimSpace(checkout.AgentID),
			ActiveTaskID:  strings.TrimSpace(checkout.ActiveTaskID),
			ActiveClaimID: strings.TrimSpace(checkout.ActiveClaimID),
			BranchName:    strings.TrimSpace(checkout.BranchName),
			DirtyState:    strings.TrimSpace(checkout.DirtyState),
			Status:        strings.TrimSpace(checkout.Status),
			DerivedStatus: strings.TrimSpace(checkout.DerivedStatus),
			LocalPathRef:  internalHeartbeatLocalPathRef(checkout.LocalPath),
			LastSeenAt:    strings.TrimSpace(checkout.LastSeenAt),
			UpdatedAt:     strings.TrimSpace(checkout.UpdatedAt),
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatCheckoutMatchesAgentOrTask(checkout ProjectCheckoutRecord, agentID, activeTaskID string) bool {
	agentID = strings.TrimSpace(agentID)
	activeTaskID = strings.TrimSpace(activeTaskID)
	if activeTaskID != "" && (strings.TrimSpace(checkout.ActiveTaskID) == activeTaskID || strings.TrimSpace(checkout.ActiveClaimID) == activeTaskID) {
		return true
	}
	if agentID != "" && strings.TrimSpace(checkout.AgentID) == agentID {
		return true
	}
	return false
}

func internalHeartbeatLocalPathRef(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	cleaned := filepath.Clean(path)
	base := strings.TrimSpace(filepath.Base(cleaned))
	if base == "." || base == string(filepath.Separator) {
		return "<local-checkout>"
	}
	return "<local-checkout>/" + internalHeartbeatSurfaceField(base, 80)
}

func internalHeartbeatPatchQueueSummaries(items []ProjectPatchQueueItemRecord, branches []ProjectBranchRecord, limit int) []InternalHeartbeatPatchQueueSummary {
	if limit <= 0 {
		return nil
	}
	branchesByID := map[string]ProjectBranchRecord{}
	for _, branch := range branches {
		if branchID := strings.TrimSpace(branch.BranchID); branchID != "" {
			branchesByID[branchID] = branch
		}
	}
	filtered := make([]ProjectPatchQueueItemRecord, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) == "" && strings.TrimSpace(item.ItemID) == "" {
			continue
		}
		filtered = append(filtered, item)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if internalHeartbeatPatchQueueStateRank(filtered[i].State) != internalHeartbeatPatchQueueStateRank(filtered[j].State) {
			return internalHeartbeatPatchQueueStateRank(filtered[i].State) < internalHeartbeatPatchQueueStateRank(filtered[j].State)
		}
		if patchQueueItemObservationTime(filtered[i]) != patchQueueItemObservationTime(filtered[j]) {
			return patchQueueItemObservationTime(filtered[i]).After(patchQueueItemObservationTime(filtered[j]))
		}
		if filtered[i].ProjectID != filtered[j].ProjectID {
			return filtered[i].ProjectID < filtered[j].ProjectID
		}
		if filtered[i].QueueID != filtered[j].QueueID {
			return filtered[i].QueueID < filtered[j].QueueID
		}
		return filtered[i].ItemID < filtered[j].ItemID
	})
	out := make([]InternalHeartbeatPatchQueueSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, item := range filtered {
		branch := branchesByID[strings.TrimSpace(item.BranchID)]
		head := firstNonEmpty(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(branch.HeadSHA))
		if len(head) > 12 {
			head = head[:12]
		}
		out = append(out, InternalHeartbeatPatchQueueSummary{
			QueueID:                 strings.TrimSpace(item.QueueID),
			ItemID:                  strings.TrimSpace(item.ItemID),
			ProjectID:               strings.TrimSpace(item.ProjectID),
			RepoID:                  strings.TrimSpace(item.RepoID),
			BranchID:                strings.TrimSpace(item.BranchID),
			BranchName:              strings.TrimSpace(branch.BranchName),
			BranchStatus:            strings.TrimSpace(branch.Status),
			State:                   strings.ToUpper(strings.TrimSpace(item.State)),
			HeadSHA:                 head,
			RepoAuthorityMode:       strings.TrimSpace(item.RepoAuthorityMode),
			MaterializationAccepted: item.MaterializationAccepted,
			MaterializationSchema:   strings.TrimSpace(item.MaterializationSchema),
			MaterializationDigest:   strings.TrimSpace(item.MaterializationDigest),
			SupersedesQueueID:       strings.TrimSpace(item.SupersedesQueueID),
			SupersedesItemID:        strings.TrimSpace(item.SupersedesItemID),
			ReviewDocKey:            strings.TrimSpace(firstNonEmpty(item.ReviewDocKey, branch.ReviewDocKey)),
			EvidenceDocKey:          strings.TrimSpace(item.EvidenceDocKey),
			DecisionDocKey:          strings.TrimSpace(item.DecisionDocKey),
			DecisionSummary:         internalHeartbeatSanitizeContextText(item.DecisionSummary, 180),
			ClaimedBy:               strings.TrimSpace(item.ClaimedBy),
			ClaimExpiresAt:          strings.TrimSpace(item.ClaimExpiresAt),
			UpdatedAt:               strings.TrimSpace(item.UpdatedAt),
			DecidedAt:               strings.TrimSpace(item.DecidedAt),
			PathHints:               internalHeartbeatPatchQueuePathHints(item, 6),
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatPatchQueueStateRank(state string) int {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "ACCEPTED":
		return 0
	case "BLOCKED":
		return 1
	case "CLAIMED":
		return 2
	case "REVIEW_READY", "READY_FOR_REVIEW", "PENDING_REVIEW":
		return 3
	case "PENDING", "READY":
		return 4
	case "REJECTED", "SUPERSEDED", "MERGED", "INTEGRATED", "CLOSED":
		return 9
	default:
		return 5
	}
}

func internalHeartbeatPatchQueuePathHints(item ProjectPatchQueueItemRecord, limit int) []string {
	if limit <= 0 {
		return nil
	}
	out := make([]string, 0, limit)
	for _, raw := range patchQueueItemPathset(item) {
		value := internalHeartbeatSanitizeVisualEvidenceText(raw)
		value = internalHeartbeatSurfaceField(value, 100)
		if value == "" {
			continue
		}
		out = append(out, value)
		if len(out) >= limit {
			break
		}
	}
	return uniqueTrimmedCSVStrings(out)
}

func internalHeartbeatSanitizeContextText(value string, limit int) string {
	value = internalHeartbeatSanitizeVisualEvidenceText(value)
	if internalHeartbeatTextLooksSecretBearing(value) {
		return "<redacted>"
	}
	return internalHeartbeatSurfaceField(oneLine(value), limit)
}

func internalHeartbeatContextTags(tags []string, limit int) []string {
	if limit <= 0 || len(tags) == 0 {
		return nil
	}
	out := make([]string, 0, internalHeartbeatMinInt(len(tags), limit))
	for _, tag := range tags {
		value := internalHeartbeatSanitizeContextText(tag, 80)
		if value == "" || value == "<redacted>" {
			continue
		}
		out = append(out, sanitizeRefSegment(value))
		if len(out) >= limit {
			break
		}
	}
	return uniqueTrimmedCSVStrings(out)
}

func internalHeartbeatDocSummaries(docs []WorkspaceDocRecord, selector string, limit int) []InternalHeartbeatDocSummary {
	if limit <= 0 {
		return nil
	}
	filtered := make([]WorkspaceDocRecord, 0, len(docs))
	for _, doc := range docs {
		if strings.TrimSpace(doc.DocKey) == "" || doc.ArchivedAt != nil {
			continue
		}
		if selector != "" && !internalHeartbeatTextMatchesSelector(selector, strings.Join([]string{doc.DocKey, doc.Title}, " ")) {
			continue
		}
		filtered = append(filtered, doc)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].UpdatedAt != filtered[j].UpdatedAt {
			return filtered[i].UpdatedAt > filtered[j].UpdatedAt
		}
		return filtered[i].DocKey < filtered[j].DocKey
	})
	out := make([]InternalHeartbeatDocSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, doc := range filtered {
		out = append(out, InternalHeartbeatDocSummary{
			DocKey:    strings.TrimSpace(doc.DocKey),
			Title:     strings.TrimSpace(doc.Title),
			UpdatedBy: strings.TrimSpace(doc.UpdatedBy),
			UpdatedAt: strings.TrimSpace(doc.UpdatedAt),
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatAgentSummaries(agents []AgentRecord, limit int) []InternalHeartbeatAgentSummary {
	if limit <= 0 {
		return nil
	}
	filtered := make([]AgentRecord, 0, len(agents))
	for _, agent := range agents {
		if strings.TrimSpace(agent.AgentID) == "" {
			continue
		}
		filtered = append(filtered, agent)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].IsOnline != filtered[j].IsOnline {
			return filtered[i].IsOnline
		}
		leftSeen := strings.TrimSpace(pointerValue(filtered[i].LastSeenAt))
		rightSeen := strings.TrimSpace(pointerValue(filtered[j].LastSeenAt))
		if leftSeen != rightSeen {
			return leftSeen > rightSeen
		}
		return filtered[i].AgentID < filtered[j].AgentID
	})
	out := make([]InternalHeartbeatAgentSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, agent := range filtered {
		activeTaskIDs := make([]string, 0, len(agent.ActiveTasks))
		for _, active := range agent.ActiveTasks {
			if taskID := strings.TrimSpace(active.TaskID); taskID != "" {
				activeTaskIDs = append(activeTaskIDs, taskID)
			}
		}
		summary := InternalHeartbeatAgentSummary{
			AgentID:       strings.TrimSpace(agent.AgentID),
			DisplayName:   internalHeartbeatSurfaceField(agent.DisplayName, 80),
			Role:          internalHeartbeatSurfaceField(agent.Role, 180),
			Status:        strings.TrimSpace(agent.Status),
			Online:        agent.IsOnline,
			LastSeenAt:    strings.TrimSpace(pointerValue(agent.LastSeenAt)),
			ActiveTaskIDs: uniqueTrimmedCSVStrings(activeTaskIDs),
		}
		if agent.CurrentSession != nil {
			summary.CurrentSessionID = strings.TrimSpace(agent.CurrentSession.SessionID)
			summary.CurrentSessionStatus = strings.TrimSpace(agent.CurrentSession.Status)
			summary.CurrentSessionUpdatedAt = strings.TrimSpace(agent.CurrentSession.UpdatedAt)
			summary.CurrentSessionSummary = internalHeartbeatSurfaceField(agent.CurrentSession.Summary, 180)
		}
		out = append(out, summary)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatProjectRoleSummaries(coordination ProjectCoordinationRecord, limit int) []InternalHeartbeatProjectRoleSummary {
	if limit <= 0 {
		return nil
	}
	roles := make([]ProjectRoleRecord, 0, len(coordination.Roles)+1)
	seen := map[string]bool{}
	appendRole := func(role ProjectRoleRecord) {
		key := strings.TrimSpace(firstNonEmpty(role.RoleID, role.ProjectID+"\x00"+role.AgentID+"\x00"+role.RoleType))
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		roles = append(roles, role)
	}
	if coordination.StrategicLead != nil {
		role := *coordination.StrategicLead
		if strings.TrimSpace(role.RoleType) == "" {
			role.RoleType = "strategic_lead"
		}
		appendRole(role)
	}
	for _, role := range coordination.Roles {
		appendRole(role)
	}
	sort.SliceStable(roles, func(i, j int) bool {
		if internalHeartbeatProjectRoleActiveRank(roles[i]) != internalHeartbeatProjectRoleActiveRank(roles[j]) {
			return internalHeartbeatProjectRoleActiveRank(roles[i]) < internalHeartbeatProjectRoleActiveRank(roles[j])
		}
		if roles[i].RoleType != roles[j].RoleType {
			return roles[i].RoleType < roles[j].RoleType
		}
		if roles[i].AgentID != roles[j].AgentID {
			return roles[i].AgentID < roles[j].AgentID
		}
		return roles[i].RoleID < roles[j].RoleID
	})
	out := make([]InternalHeartbeatProjectRoleSummary, 0, internalHeartbeatMinInt(len(roles), limit))
	for _, role := range roles {
		out = append(out, InternalHeartbeatProjectRoleSummary{
			RoleID:    strings.TrimSpace(role.RoleID),
			ProjectID: strings.TrimSpace(role.ProjectID),
			AgentID:   strings.TrimSpace(role.AgentID),
			RoleType:  internalHeartbeatSurfaceField(role.RoleType, 80),
			Status:    strings.TrimSpace(role.Status),
			Summary:   internalHeartbeatSurfaceField(role.Summary, 180),
			UpdatedAt: strings.TrimSpace(role.UpdatedAt),
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatProjectRoleActiveRank(role ProjectRoleRecord) int {
	switch strings.ToUpper(strings.TrimSpace(role.Status)) {
	case "ACTIVE", "CLAIMED", "LEASED", "RUNNING", "IN_PROGRESS":
		return 0
	case "", "PENDING", "REQUESTED":
		return 1
	case "RELEASED", "COMPLETED", "DONE", "CANCELLED", "CANCELED", "EXPIRED", "ARCHIVED":
		return 3
	default:
		return 2
	}
}

func internalHeartbeatServiceRunSummaries(runs []ServiceRunRecord, projectID string, limit int) []InternalHeartbeatServiceRunSummary {
	if limit <= 0 || len(runs) == 0 {
		return nil
	}
	projectID = strings.TrimSpace(projectID)
	filtered := make([]ServiceRunRecord, 0, len(runs))
	for _, run := range runs {
		if strings.TrimSpace(run.RunID) == "" && strings.TrimSpace(run.Title) == "" {
			continue
		}
		if projectID != "" && strings.TrimSpace(run.ProjectID) != projectID {
			continue
		}
		filtered = append(filtered, run)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if serviceRunPromptRank(filtered[i].Status) != serviceRunPromptRank(filtered[j].Status) {
			return serviceRunPromptRank(filtered[i].Status) < serviceRunPromptRank(filtered[j].Status)
		}
		if strings.TrimSpace(filtered[i].UpdatedAt) != strings.TrimSpace(filtered[j].UpdatedAt) {
			return strings.TrimSpace(filtered[i].UpdatedAt) > strings.TrimSpace(filtered[j].UpdatedAt)
		}
		return strings.TrimSpace(filtered[i].RunID) < strings.TrimSpace(filtered[j].RunID)
	})
	out := make([]InternalHeartbeatServiceRunSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, run := range filtered {
		out = append(out, InternalHeartbeatServiceRunSummary{
			RunID:            strings.TrimSpace(run.RunID),
			CandidateID:      strings.TrimSpace(run.CandidateID),
			ProjectID:        strings.TrimSpace(run.ProjectID),
			Title:            internalHeartbeatSafePublicText(run.Title, 120),
			Status:           strings.TrimSpace(run.Status),
			DeployTarget:     internalHeartbeatSafePublicText(run.DeployTarget, 80),
			PublicURL:        internalHeartbeatSanitizeSurfaceURL(run.PublicURL),
			HealthCheckURL:   internalHeartbeatSanitizeSurfaceURL(run.HealthCheckURL),
			CredentialPolicy: internalHeartbeatSafePublicText(run.CredentialPolicy, 60),
			NextAction:       internalHeartbeatSurfaceField(serviceRunPromptNextAction(run), 140),
			UpdatedAt:        strings.TrimSpace(run.UpdatedAt),
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatRecentUpdateSummaries(updates []AgentUpdateRecord, limit int) []InternalHeartbeatRecentUpdateSummary {
	if limit <= 0 {
		return nil
	}
	filtered := append([]AgentUpdateRecord(nil), updates...)
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt != filtered[j].CreatedAt {
			return filtered[i].CreatedAt > filtered[j].CreatedAt
		}
		return filtered[i].UpdateID < filtered[j].UpdateID
	})
	out := make([]InternalHeartbeatRecentUpdateSummary, 0, internalHeartbeatMinInt(len(filtered), limit))
	for _, update := range filtered {
		out = append(out, InternalHeartbeatRecentUpdateSummary{
			AgentID:    strings.TrimSpace(update.AgentID),
			UpdateType: strings.TrimSpace(update.UpdateType),
			Summary:    internalHeartbeatSurfaceField(update.Summary, 220),
			CreatedAt:  strings.TrimSpace(update.CreatedAt),
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func internalHeartbeatRunnableSurfaceSummaries(snapshot WorkspaceSnapshot, limit int) []InternalHeartbeatRunnableSurface {
	if limit <= 0 {
		return nil
	}
	byURL := map[string]InternalHeartbeatRunnableSurface{}
	consider := func(surface InternalHeartbeatRunnableSurface) {
		surface.URL = internalHeartbeatSanitizeSurfaceURL(surface.URL)
		if surface.URL == "" {
			return
		}
		surface.SourceKind = internalHeartbeatSurfaceField(surface.SourceKind, 40)
		surface.SourceRef = internalHeartbeatSurfaceField(surface.SourceRef, 96)
		surface.Label = internalHeartbeatSurfaceField(surface.Label, 120)
		surface.Reason = internalHeartbeatSurfaceField(surface.Reason, 160)
		surface.Localhost = internalHeartbeatSurfaceURLIsLocalhost(surface.URL)
		surface.VerificationRequired = true
		if surface.Confidence <= 0 {
			surface.Confidence = 50
		}
		if surface.Localhost && surface.Confidence > 55 {
			surface.Confidence = 55
		}
		if existing, ok := byURL[surface.URL]; ok {
			if existing.Confidence > surface.Confidence {
				return
			}
			if existing.Confidence == surface.Confidence && existing.SourceRef <= surface.SourceRef {
				return
			}
		}
		byURL[surface.URL] = surface
	}
	for _, doc := range snapshot.Docs {
		if strings.TrimSpace(doc.DocKey) == "" || doc.ArchivedAt != nil {
			continue
		}
		if !internalHeartbeatDocLooksLikeRunnableSurfaceSource(doc) {
			continue
		}
		text := strings.Join([]string{doc.Title, doc.Content}, "\n")
		for _, rawURL := range internalHeartbeatExtractSurfaceURLs(text) {
			consider(InternalHeartbeatRunnableSurface{
				URL:        rawURL,
				SourceKind: "doc",
				SourceRef:  strings.TrimSpace(doc.DocKey),
				Label:      firstNonEmpty(doc.Title, doc.DocKey),
				Confidence: 85,
				Reason:     "url mentioned in workspace doc",
			})
		}
	}
	for _, task := range snapshot.Tasks {
		if strings.TrimSpace(task.TaskID) == "" {
			continue
		}
		text := strings.Join([]string{task.Title, task.Description}, "\n")
		for _, rawURL := range internalHeartbeatExtractSurfaceURLs(text) {
			confidence := 70
			if !taskSubmitTaskIsTerminal(task) {
				confidence = 78
			}
			consider(InternalHeartbeatRunnableSurface{
				URL:        rawURL,
				SourceKind: "task",
				SourceRef:  strings.TrimSpace(task.TaskID),
				Label:      firstNonEmpty(task.Title, task.TaskID),
				Confidence: confidence,
				Reason:     "url mentioned in task metadata",
			})
		}
	}
	for _, update := range snapshot.RecentUpdates {
		if strings.TrimSpace(update.UpdateID) == "" {
			continue
		}
		// Do not scan PayloadJSON here; it can contain raw tool output or private paths.
		for _, rawURL := range internalHeartbeatExtractSurfaceURLs(update.Summary) {
			consider(InternalHeartbeatRunnableSurface{
				URL:        rawURL,
				SourceKind: "update",
				SourceRef:  strings.TrimSpace(update.UpdateID),
				Label:      firstNonEmpty(update.UpdateType, update.AgentID, update.UpdateID),
				Confidence: 60,
				Reason:     "url mentioned in recent update summary",
			})
		}
	}
	if len(byURL) == 0 {
		return nil
	}
	out := make([]InternalHeartbeatRunnableSurface, 0, len(byURL))
	for _, surface := range byURL {
		out = append(out, surface)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Localhost != out[j].Localhost {
			return !out[i].Localhost
		}
		if out[i].Confidence != out[j].Confidence {
			return out[i].Confidence > out[j].Confidence
		}
		if out[i].SourceKind != out[j].SourceKind {
			return out[i].SourceKind < out[j].SourceKind
		}
		if out[i].SourceRef != out[j].SourceRef {
			return out[i].SourceRef < out[j].SourceRef
		}
		return out[i].URL < out[j].URL
	})
	return internalHeartbeatLimit(out, limit)
}

func internalHeartbeatExtractSurfaceURLs(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	raw := internalHeartbeatURLPattern.FindAllString(text, -1)
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		if sanitized := internalHeartbeatSanitizeSurfaceURL(value); sanitized != "" {
			out = append(out, sanitized)
		}
	}
	return uniqueTrimmedCSVStrings(out)
}

func internalHeartbeatSanitizeSurfaceURL(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, ".,;:!?")
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(parsed.Scheme)) {
	case "http", "https":
	default:
		return ""
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	if internalHeartbeatSurfacePathLooksSensitive(parsed.EscapedPath()) {
		parsed.Path = ""
		parsed.RawPath = ""
	}
	if len(parsed.EscapedPath()) > 120 {
		parsed.Path = "/"
		parsed.RawPath = ""
	}
	return internalHeartbeatSurfaceField(parsed.String(), 220)
}

func internalHeartbeatSurfaceURLIsLocalhost(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil {
		return false
	}
	host := strings.ToLower(strings.Trim(parsed.Hostname(), "[]"))
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasPrefix(host, "127.")
}

func internalHeartbeatDocLooksLikeRunnableSurfaceSource(doc WorkspaceDocRecord) bool {
	text := strings.Join([]string{doc.DocKey, doc.Title}, " ")
	return containsAny(text,
		"runbook",
		"runnable",
		"preview",
		"deploy",
		"deployment",
		"localhost",
		"url",
		"surface",
		"browser",
		"smoke",
		"visual",
		"acceptance",
		"evidence",
		"review",
	)
}

func internalHeartbeatSurfacePathLooksSensitive(path string) bool {
	decoded, err := url.PathUnescape(path)
	if err != nil {
		decoded = path
	}
	lower := strings.ToLower(strings.TrimSpace(decoded))
	if lower == "" || lower == "/" {
		return false
	}
	return containsAny(lower,
		"@fs",
		"c:/",
		"c:\\",
		"/users/",
		"\\users\\",
		"token",
		"secret",
		"api_key",
		"apikey",
		"access_key",
		"access-token",
		"credential",
		".env",
	)
}

func (r *Runtime) enrichInternalHeartbeatContextPacket(ctx context.Context, packet *InternalHeartbeatContextPacket, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, now time.Time) {
	if packet == nil {
		return
	}
	spec = normalizeAgentHeartbeatSpec(spec)
	if internalHeartbeatSpecWantsTypedBrowserProbe(spec, policy) {
		r.enrichVisualProductAuditBrowserProbes(ctx, packet, spec, now)
	}
	internalHeartbeatRefreshRequiredToolArtifacts(packet, spec)
}

func internalHeartbeatSpecWantsTypedBrowserProbe(spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy) bool {
	if strings.EqualFold(strings.TrimSpace(spec.ID), "visual_product_audit") || strings.EqualFold(strings.TrimSpace(spec.Kind), "browser_critic") {
		return containsTrimmedString(policy.ToolSuites, "browser_read_only")
	}
	return false
}

func (r *Runtime) enrichVisualProductAuditBrowserProbes(ctx context.Context, packet *InternalHeartbeatContextPacket, spec AgentHeartbeatSpec, now time.Time) {
	if packet == nil {
		return
	}
	spec = normalizeAgentHeartbeatSpec(spec)
	markers := internalHeartbeatVisualAuditProductMarkers(*packet)
	docs := r.internalHeartbeatSnapshotDocs()
	for idx := range packet.SelectorPayloads {
		payload := &packet.SelectorPayloads[idx]
		if payload.Selector != "runnable_surface" {
			continue
		}
		if len(payload.Surfaces) == 0 {
			payload.Status = "missing_surface"
			payload.Summary = "No runnable URL candidate found; browser critic must report missing surface instead of claiming visual pass."
			continue
		}
		payload.BrowserProbes = internalHeartbeatProbeRunnableSurfaces(ctx, payload.Surfaces, markers, internalHeartbeatBrowserProbeLimit, now)
		if len(payload.BrowserProbes) == 0 {
			payload.Status = "probe_skipped"
			payload.Summary = "Runnable surface candidates exist, but no candidate was eligible for automatic read-only probing; manual browser evidence is still required."
			continue
		}
		if internalHeartbeatHasVerifiedBrowserProbe(payload.BrowserProbes) {
			payload.Status = "surface_preflight_verified"
			payload.Summary = "Read-only browser preflight loaded a candidate surface and matched product markers; this is navigation evidence only, not visual acceptance without screenshots."
		} else {
			payload.Status = "surface_preflight_unverified"
			payload.Summary = "Read-only browser preflight did not verify a matching product surface; browser critic must not claim visual pass and should record a missing/wrong-surface finding."
		}
		payload.VisualAudit = internalHeartbeatVisualAuditPlan(*payload, docs, internalHeartbeatVisualAuditContractFromSpec(spec))
	}
	packet.PolicyInstructions = append(packet.PolicyInstructions,
		"visual browser probe: browser_probes are bounded page-load/product-marker checks only; they are not screenshot evidence and cannot justify visual_verdict: pass",
		"visual browser probe: if no probe has product_marker_verified=true, record missing or wrong runnable surface instead of judging UI quality",
		"visual audit plan: a verified product surface still needs state-specific screenshot evidence for desktop and narrow viewports before any visual pass",
		"visual audit plan: low generic layout risk does not replace semantic screenshot judgment; boards, grids, canvases, charts, editors, maps, and game surfaces need primary-surface geometry/density and mode/preset/difficulty-specific fit checks",
	)
}

func (r *Runtime) internalHeartbeatSnapshotDocs() []WorkspaceDocRecord {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]WorkspaceDocRecord(nil), r.bootstrap.Snapshot.Docs...)
}

func internalHeartbeatVisualAuditContractFromSpec(spec AgentHeartbeatSpec) *AgentHeartbeatVisualAuditSpec {
	spec = normalizeAgentHeartbeatSpec(spec)
	if spec.VisualAudit != nil {
		return spec.VisualAudit
	}
	if spec.EvidenceContract != nil {
		return internalHeartbeatVisualAuditContractFromEvidenceContract(spec.EvidenceContract)
	}
	return nil
}

func internalHeartbeatVisualAuditContractFromEvidenceContract(contract *AgentHeartbeatEvidenceContractSpec) *AgentHeartbeatVisualAuditSpec {
	contract = normalizeAgentHeartbeatEvidenceContractSpec(contract)
	if contract == nil {
		return nil
	}
	out := &AgentHeartbeatVisualAuditSpec{
		Checks:               append([]string(nil), contract.Checks...),
		ArtifactRequirements: append([]string(nil), contract.ArtifactRequirements...),
	}
	for _, dimension := range contract.Dimensions {
		if strings.TrimSpace(dimension.ID) == "" || dimension.Width <= 0 || dimension.Height <= 0 {
			continue
		}
		out.Viewports = append(out.Viewports, AgentHeartbeatVisualAuditViewportSpec{
			ID:      dimension.ID,
			Width:   dimension.Width,
			Height:  dimension.Height,
			Purpose: firstNonEmpty(dimension.Purpose, dimension.Label),
		})
	}
	for _, state := range contract.States {
		if strings.TrimSpace(state.ID) == "" {
			continue
		}
		required := true
		if state.EvidenceRequired != nil {
			required = *state.EvidenceRequired
		}
		out.Scenarios = append(out.Scenarios, AgentHeartbeatVisualAuditScenarioSpec{
			ID:                   state.ID,
			Label:                state.Label,
			RequiredState:        state.RequiredState,
			ScreenshotRequired:   boolPtr(required),
			RealUserQuestion:     state.RealUserQuestion,
			ExpectedEvidenceKind: state.ExpectedEvidenceKind,
		})
	}
	return normalizeAgentHeartbeatVisualAuditSpec(out)
}

func internalHeartbeatVisualAuditPlan(payload InternalHeartbeatSelectorPacket, docs []WorkspaceDocRecord, contract *AgentHeartbeatVisualAuditSpec) *InternalHeartbeatVisualAuditPlan {
	plan := &InternalHeartbeatVisualAuditPlan{
		Status:                     "blocked",
		Summary:                    "Visual audit requires a verified surface plus state-specific screenshots; no visual pass is allowed from page-load evidence alone.",
		Viewports:                  internalHeartbeatVisualAuditViewports(contract),
		Scenarios:                  internalHeartbeatVisualAuditScenarios(contract),
		RequiredChecks:             internalHeartbeatVisualAuditChecks(contract),
		RequiredArtifactProperties: internalHeartbeatVisualAuditArtifactProperties(contract),
		VisualVerdictAllowed:       false,
	}
	if probe, ok := internalHeartbeatVerifiedBrowserProbe(payload.BrowserProbes); ok {
		plan.SurfaceURL = probe.URL
		plan.ProductMarkerVerified = true
	}
	evidenceOK, evidenceDocKeys, missing, blocking := visualAcceptanceEvidenceSatisfiedForCandidateWithOptions(docs, ProjectPatchQueueItemRecord{}, visualAcceptanceEvidenceOptions{VerifyScreenshotArtifacts: true})
	plan.ExistingEvidenceSatisfied = evidenceOK
	plan.ExistingEvidenceDocKeys = internalHeartbeatSurfaceFields(evidenceDocKeys, 120)
	plan.MissingEvidence = internalHeartbeatVisualEvidenceFields(missing, 160)
	plan.BlockingEvidence = internalHeartbeatVisualEvidenceFields(blocking, 160)
	switch {
	case payload.Status == "missing_surface":
		plan.Status = "missing_surface"
		plan.Summary = "No runnable URL is known; the UI critic must first surface missing runnable evidence."
	case payload.Status == "surface_preflight_unverified" || payload.Status == "probe_skipped":
		plan.Status = "surface_unverified"
		plan.Summary = "Candidate surface was not verified as the intended product; visual critique would be unreliable."
	case plan.ProductMarkerVerified && evidenceOK:
		plan.Status = "visual_evidence_already_satisfied"
		plan.Summary = "A product surface was verified and existing visual acceptance evidence appears complete in workspace docs."
		plan.VisualVerdictAllowed = true
	case plan.ProductMarkerVerified:
		plan.Status = "needs_visual_evidence"
		plan.Summary = "A product surface was verified, but durable screenshot/viewport/scenario evidence is still missing or incomplete."
	default:
		plan.Status = "surface_not_ready"
	}
	if plan.ProductMarkerVerified && !evidenceOK && len(plan.BlockingEvidence) == 0 {
		plan.EvidenceRequired = true
		plan.EvidenceRequests = internalHeartbeatVisualEvidenceRequests(plan)
	}
	return plan
}

func internalHeartbeatVisualEvidenceRequests(plan *InternalHeartbeatVisualAuditPlan) []InternalHeartbeatEvidenceRequest {
	if plan == nil || !plan.ProductMarkerVerified || strings.TrimSpace(plan.SurfaceURL) == "" {
		return nil
	}
	requests := []InternalHeartbeatEvidenceRequest{}
	for _, viewport := range plan.Viewports {
		if strings.TrimSpace(viewport.ID) == "" || viewport.Width <= 0 || viewport.Height <= 0 {
			continue
		}
		for _, scenario := range plan.Scenarios {
			if strings.TrimSpace(scenario.ID) == "" || !scenario.ScreenshotRequired {
				continue
			}
			seed := strings.Join([]string{
				plan.SurfaceURL,
				viewport.ID,
				fmt.Sprint(viewport.Width),
				fmt.Sprint(viewport.Height),
				scenario.ID,
			}, "\x00")
			request := InternalHeartbeatEvidenceRequest{
				RequestID:            "evidence-" + shortRefHash(seed),
				Kind:                 "screenshot",
				SurfaceURL:           internalHeartbeatSanitizeSurfaceURL(plan.SurfaceURL),
				DimensionID:          internalHeartbeatSurfaceField(viewport.ID, 40),
				Width:                viewport.Width,
				Height:               viewport.Height,
				StateID:              internalHeartbeatSurfaceField(scenario.ID, 60),
				StateLabel:           internalHeartbeatSurfaceField(scenario.Label, 120),
				RequiredState:        internalHeartbeatSurfaceField(scenario.RequiredState, 120),
				Required:             scenario.ScreenshotRequired,
				ExpectedEvidenceKind: internalHeartbeatSurfaceField(scenario.ExpectedEvidenceKind, 120),
				ArtifactRefHint:      internalHeartbeatEvidenceArtifactHint("screenshot", viewport.ID, scenario.ID),
			}
			requests = append(requests, request)
			if len(requests) >= 12 {
				return requests
			}
		}
	}
	return requests
}

func internalHeartbeatEvidenceArtifactHint(kind, dimensionID, stateID string) string {
	kind = sanitizeRefSegment(firstNonEmpty(kind, "evidence"))
	dimensionID = sanitizeRefSegment(firstNonEmpty(dimensionID, "dimension"))
	stateID = sanitizeRefSegment(firstNonEmpty(stateID, "state"))
	return "artifacts/evidence/" + kind + "-" + dimensionID + "-" + stateID + ".png"
}

func internalHeartbeatVisualAuditViewports(contract *AgentHeartbeatVisualAuditSpec) []InternalHeartbeatVisualAuditViewport {
	if contract == nil || len(contract.Viewports) == 0 {
		return internalHeartbeatDefaultVisualAuditViewports()
	}
	out := make([]InternalHeartbeatVisualAuditViewport, 0, len(contract.Viewports))
	for _, viewport := range contract.Viewports {
		if strings.TrimSpace(viewport.ID) == "" || viewport.Width <= 0 || viewport.Height <= 0 {
			continue
		}
		out = append(out, InternalHeartbeatVisualAuditViewport{
			ID:      internalHeartbeatSurfaceField(viewport.ID, 40),
			Width:   viewport.Width,
			Height:  viewport.Height,
			Purpose: internalHeartbeatSurfaceField(viewport.Purpose, 160),
		})
	}
	if len(out) == 0 {
		return internalHeartbeatDefaultVisualAuditViewports()
	}
	return out
}

func internalHeartbeatDefaultVisualAuditViewports() []InternalHeartbeatVisualAuditViewport {
	return []InternalHeartbeatVisualAuditViewport{
		{ID: "desktop", Width: 1365, Height: 900, Purpose: "first viewport and main workflow at a common laptop/desktop size"},
		{ID: "narrow", Width: 390, Height: 844, Purpose: "mobile/narrow layout, text fit, controls, and horizontal overflow"},
	}
}

func internalHeartbeatVisualAuditScenarios(contract *AgentHeartbeatVisualAuditSpec) []InternalHeartbeatVisualAuditScenario {
	if contract == nil || len(contract.Scenarios) == 0 {
		return internalHeartbeatDefaultVisualAuditScenarios()
	}
	out := make([]InternalHeartbeatVisualAuditScenario, 0, len(contract.Scenarios))
	for _, scenario := range contract.Scenarios {
		if strings.TrimSpace(scenario.ID) == "" {
			continue
		}
		screenshotRequired := true
		if scenario.ScreenshotRequired != nil {
			screenshotRequired = *scenario.ScreenshotRequired
		}
		out = append(out, InternalHeartbeatVisualAuditScenario{
			ID:                   internalHeartbeatSurfaceField(scenario.ID, 60),
			Label:                internalHeartbeatSurfaceField(scenario.Label, 120),
			RequiredState:        internalHeartbeatSurfaceField(scenario.RequiredState, 120),
			ScreenshotRequired:   screenshotRequired,
			RealUserQuestion:     internalHeartbeatSurfaceField(scenario.RealUserQuestion, 180),
			ExpectedEvidenceKind: internalHeartbeatSurfaceField(scenario.ExpectedEvidenceKind, 120),
		})
	}
	if len(out) == 0 {
		return internalHeartbeatDefaultVisualAuditScenarios()
	}
	return out
}

func internalHeartbeatDefaultVisualAuditScenarios() []InternalHeartbeatVisualAuditScenario {
	return []InternalHeartbeatVisualAuditScenario{
		{ID: "initial_state", Label: "First viewport / empty state", RequiredState: "before user input", ScreenshotRequired: true, RealUserQuestion: "Can a new user understand what to do without layout glitches?", ExpectedEvidenceKind: "state-specific screenshot"},
		{ID: "primary_flow", Label: "Primary happy path", RequiredState: "after performing the core action", ScreenshotRequired: true, RealUserQuestion: "Can the user complete the main workflow without awkward controls or lag?", ExpectedEvidenceKind: "state-specific screenshot plus observed behavior"},
		{ID: "result_state", Label: "Output / export / post-action state", RequiredState: "after the product produces its result", ScreenshotRequired: true, RealUserQuestion: "Is the final result visible, correctly sized, and ready to use?", ExpectedEvidenceKind: "state-specific screenshot or explicit not-applicable note"},
	}
}

func internalHeartbeatVisualAuditChecks(contract *AgentHeartbeatVisualAuditSpec) []string {
	if contract == nil || len(contract.Checks) == 0 {
		return internalHeartbeatDefaultVisualAuditChecks()
	}
	return internalHeartbeatSurfaceFields(contract.Checks, 80)
}

func internalHeartbeatDefaultVisualAuditChecks() []string {
	return []string{
		"overlap",
		"clipping",
		"contrast",
		"readability",
		"responsive_fit",
		"typography_hierarchy",
		"spacing",
		"primary_action_visibility",
		"loading_error_empty_states",
		"performance_symptoms",
	}
}

func internalHeartbeatVisualAuditArtifactProperties(contract *AgentHeartbeatVisualAuditSpec) []string {
	if contract == nil || len(contract.ArtifactRequirements) == 0 {
		return internalHeartbeatDefaultVisualAuditArtifactProperties()
	}
	return internalHeartbeatSurfaceFields(contract.ArtifactRequirements, 160)
}

func internalHeartbeatDefaultVisualAuditArtifactProperties() []string {
	return []string{
		"distinct screenshots for initial_state, primary_flow, and result_state",
		"at least one desktop screenshot and one narrow/mobile screenshot",
		"locally decodable screenshot files or durable workspace artifact refs",
		"branch/head/url/checkout provenance",
		"primary-surface geometry/density judgment for boards, grids, canvases, charts, editors, maps, or game surfaces",
		"visible modes/presets/difficulties that change the primary surface are checked or explicitly marked not applicable",
		"visual_verdict: pass only when no blocking findings remain",
	}
}

func internalHeartbeatVerifiedBrowserProbe(probes []InternalHeartbeatBrowserProbe) (InternalHeartbeatBrowserProbe, bool) {
	for _, probe := range probes {
		if probe.ProductMarkerVerified {
			return probe, true
		}
	}
	return InternalHeartbeatBrowserProbe{}, false
}

func internalHeartbeatSurfaceFields(values []string, limit int) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = internalHeartbeatSurfaceField(value, limit)
		if value != "" {
			out = append(out, value)
		}
	}
	return uniqueTrimmedCSVStrings(out)
}

func internalHeartbeatVisualEvidenceFields(values []string, limit int) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = internalHeartbeatSanitizeVisualEvidenceText(value)
		value = internalHeartbeatSurfaceField(value, limit)
		if value != "" {
			out = append(out, value)
		}
	}
	return uniqueTrimmedCSVStrings(out)
}

func internalHeartbeatSanitizeVisualEvidenceText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = internalHeartbeatURLPattern.ReplaceAllStringFunc(value, func(raw string) string {
		if sanitized := internalHeartbeatSanitizeSurfaceURL(raw); sanitized != "" {
			return sanitized
		}
		return "<url>"
	})
	value = internalHeartbeatLocalImagePathPattern.ReplaceAllString(value, "<local-path>")
	value = internalHeartbeatViteFSPathPattern.ReplaceAllString(value, "<local-path>")
	value = internalHeartbeatWindowsPathPattern.ReplaceAllString(value, "<local-path>")
	value = internalHeartbeatSensitiveUnixPathPattern.ReplaceAllString(value, "<local-path>")
	return value
}

func internalHeartbeatVisualAuditProductMarkers(packet InternalHeartbeatContextPacket) []string {
	type marker struct {
		value  string
		source string
	}
	candidates := []marker{{packet.WorkspaceID, "workspace_id"}}
	for _, payload := range packet.SelectorPayloads {
		if payload.Workspace.Title != "" {
			candidates = append(candidates, marker{payload.Workspace.Title, "workspace_title"})
		}
		for _, project := range payload.Projects {
			candidates = append(candidates, marker{project.Title, "project_title"})
			candidates = append(candidates, marker{project.ProjectID, "project_id"})
		}
		for _, task := range payload.Tasks {
			candidates = append(candidates, marker{task.Title, "task_title"})
			candidates = append(candidates, marker{task.ProjectID, "task_project_id"})
		}
		for _, doc := range payload.Docs {
			candidates = append(candidates, marker{doc.Title, "doc_title"})
		}
	}
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		for _, value := range internalHeartbeatVisualAuditMarkerVariants(candidate.value) {
			value = internalHeartbeatSurfaceField(value, 80)
			if internalHeartbeatVisualAuditMarkerUsable(value) {
				out = append(out, value+"|"+candidate.source)
			}
		}
	}
	return uniqueTrimmedCSVStrings(out)
}

func internalHeartbeatVisualAuditMarkerVariants(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	variants := []string{value}
	lower := strings.ToLower(value)
	for _, suffix := range []string{
		" runnable surface",
		" preview surface",
		" runbook",
		" preview",
		" deployment",
		" visual evidence",
		" acceptance packet",
		" product contract",
		" design plan",
	} {
		if strings.HasSuffix(lower, suffix) && len(value) > len(suffix) {
			variants = append(variants, strings.TrimSpace(value[:len(value)-len(suffix)]))
		}
	}
	words := strings.Fields(value)
	if len(words) > 3 && !strings.HasPrefix(lower, "task-") && !strings.HasPrefix(lower, "project-") {
		variants = append(variants, strings.Join(words[:3], " "))
	}
	return uniqueTrimmedCSVStrings(variants)
}

func internalHeartbeatVisualAuditMarkerUsable(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 4 {
		return false
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "task-") || strings.HasPrefix(lower, "project-") || strings.HasPrefix(lower, "ws-") {
		return len(value) >= 8
	}
	return containsAny(lower,
		"app",
		"tool",
		"runner",
		"workflow",
		"converter",
		"sprite",
		"pixel",
		"image",
		"editor",
		"export",
		"dashboard",
		"studio",
		"service",
	) || strings.Contains(value, " ")
}

func internalHeartbeatProbeRunnableSurfaces(ctx context.Context, surfaces []InternalHeartbeatRunnableSurface, markers []string, limit int, now time.Time) []InternalHeartbeatBrowserProbe {
	if limit <= 0 {
		return nil
	}
	out := make([]InternalHeartbeatBrowserProbe, 0, internalHeartbeatMinInt(len(surfaces), limit))
	for _, surface := range surfaces {
		if len(out) >= limit {
			break
		}
		probe := internalHeartbeatProbeRunnableSurface(ctx, surface, markers, now)
		if probe.URL == "" {
			continue
		}
		out = append(out, probe)
	}
	return out
}

func internalHeartbeatProbeRunnableSurface(ctx context.Context, surface InternalHeartbeatRunnableSurface, markers []string, now time.Time) InternalHeartbeatBrowserProbe {
	urlValue := internalHeartbeatSanitizeSurfaceURL(surface.URL)
	probe := InternalHeartbeatBrowserProbe{
		URL:                        urlValue,
		Status:                     "skipped",
		Localhost:                  internalHeartbeatSurfaceURLIsLocalhost(urlValue),
		VisualVerificationRequired: true,
	}
	if urlValue == "" {
		probe.Error = "missing_url"
		return probe
	}
	if ok, reason := internalHeartbeatBrowserProbeURLAllowed(urlValue); !ok {
		probe.Error = reason
		return probe
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodGet, urlValue, nil)
	if err != nil {
		probe.Status = "failed"
		probe.Error = internalHeartbeatSurfaceField(err.Error(), 160)
		return probe
	}
	req.Header.Set("Accept", "text/html,application/xhtml+xml,text/plain;q=0.8,*/*;q=0.1")
	req.Header.Set("User-Agent", "rhizome-internal-heartbeat-browser-probe/1.0")
	start := time.Now()
	client := &http.Client{
		Timeout: 4 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	probe.DurationMillis = time.Since(start).Milliseconds()
	_ = now
	if err != nil {
		probe.Status = "unreachable"
		probe.Error = internalHeartbeatSurfaceField(err.Error(), 160)
		return probe
	}
	defer resp.Body.Close()
	probe.HTTPStatus = resp.StatusCode
	probe.ContentType = internalHeartbeatSurfaceField(resp.Header.Get("Content-Type"), 80)
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 128*1024))
	body := string(bodyBytes)
	probe.Title = internalHeartbeatExtractHTMLTitle(body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		probe.Status = "http_error"
		probe.Error = internalHeartbeatSurfaceField(resp.Status, 120)
		return probe
	}
	matched, source := internalHeartbeatMatchProductMarker(body, probe.Title, markers)
	if matched != "" {
		probe.Status = "verified"
		probe.MatchedMarker = internalHeartbeatSurfaceField(matched, 80)
		probe.MarkerSource = internalHeartbeatSurfaceField(source, 40)
		probe.ProductMarkerVerified = true
		return probe
	}
	probe.Status = "loaded_unverified"
	probe.Error = "page loaded but no product marker matched"
	return probe
}

func internalHeartbeatBrowserProbeURLAllowed(raw string) (bool, string) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || strings.TrimSpace(parsed.Host) == "" {
		return false, "invalid_url"
	}
	scheme := strings.ToLower(parsed.Scheme)
	host := strings.ToLower(strings.Trim(parsed.Hostname(), "[]"))
	if host == "" {
		return false, "invalid_host"
	}
	if internalHeartbeatSurfaceURLIsLocalhost(raw) {
		return scheme == "http" || scheme == "https", ""
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			return false, "private_ip_disallowed"
		}
	}
	if scheme != "https" {
		return false, "public_http_disallowed"
	}
	return true, ""
}

func internalHeartbeatExtractHTMLTitle(body string) string {
	match := internalHeartbeatHTMLTitlePattern.FindStringSubmatch(body)
	if len(match) < 2 {
		return ""
	}
	title := html.UnescapeString(match[1])
	title = regexp.MustCompile(`\s+`).ReplaceAllString(title, " ")
	return internalHeartbeatSurfaceField(title, 120)
}

func internalHeartbeatMatchProductMarker(body, title string, markers []string) (string, string) {
	haystack := strings.ToLower(title + "\n" + body)
	for _, entry := range markers {
		markerValue, markerSource, ok := strings.Cut(entry, "|")
		if !ok {
			markerValue = entry
		}
		markerValue = strings.TrimSpace(markerValue)
		if markerValue == "" {
			continue
		}
		if strings.Contains(haystack, strings.ToLower(markerValue)) {
			return markerValue, markerSource
		}
	}
	return "", ""
}

func internalHeartbeatHasVerifiedBrowserProbe(probes []InternalHeartbeatBrowserProbe) bool {
	for _, probe := range probes {
		if probe.ProductMarkerVerified {
			return true
		}
	}
	return false
}

func internalHeartbeatSurfaceField(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return strings.TrimSpace(value[:limit])
}

func internalHeartbeatSafePublicText(value string, limit int) string {
	value = internalHeartbeatSanitizeVisualEvidenceText(value)
	if internalHeartbeatTextLooksSecretBearing(value) {
		return "<redacted>"
	}
	return internalHeartbeatSurfaceField(value, limit)
}

func internalHeartbeatTextLooksSecretBearing(value string) bool {
	text := strings.ToLower(strings.TrimSpace(value))
	if text == "" {
		return false
	}
	return containsAny(text,
		"secret", "token", "api key", "apikey", "api_key",
		"password", "passwd", "credential", "private key", "access key",
		"client_secret", "refresh_token", "bearer ",
	)
}

func internalHeartbeatTextMatchesSelector(selector, text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	switch strings.TrimSpace(selector) {
	case "project_contracts", "product_contract":
		return containsAny(text, "contract", "spec", "requirements", "brief", "product", "acceptance")
	case "design_plan":
		return containsAny(text, "design", "plan", "ui", "ux", "layout", "interaction")
	case "visual_evidence":
		return containsAny(text, "visual", "screenshot", "browser", "evidence", "acceptance", "viewport")
	case "recent_decisions":
		return containsAny(text, "decision", "accepted", "blocked", "review", "post-mvp", "reflection")
	case "reflection_boards", "reflection_board", "project_reflection":
		return containsAny(text, "reflection", "reflection_board", "sensemaking", "retrospective", "post-mvp", "post_mvp", "quality loop", "learning", "next step")
	case "review_packets":
		return containsAny(text, "review", "packet", "critique", "decision", "qa")
	case "recent_verification":
		return containsAny(text, "verification", "smoke", "test", "qa", "build", "evidence")
	case "runnable_surface":
		return containsAny(text, "run", "dev server", "localhost", "deploy", "url", "surface", "browser")
	case "service_pipeline", "service_runs", "portfolio_state":
		return containsAny(text, "service", "venture", "portfolio", "candidate", "deploy", "deployment", "public url", "analytics", "measurement", "monetization", "ads", "revenue", "spend", "run")
	case "open_quality_gaps", "recent_ui_findings":
		return containsAny(text, "quality", "qa", "test", "smoke", "visual", "ui", "ux", "review", "validation", "acceptance", "bug", "regression", "layout", "overlap", "jank")
	case "patch_queue":
		return containsAny(text, "patch", "queue", "branch", "merge", "accepted", "blocked")
	case "integration_state", "project_coordination":
		return containsAny(text, "integration", "coordination", "handoff", "blocked", "queue", "gate")
	default:
		return true
	}
}

func internalHeartbeatDefaultProjectLane(spec AgentHeartbeatSpec, item AgentPersonalBacklogItem) string {
	text := strings.Join([]string{spec.ID, spec.Kind, item.Kind, item.Title, item.Summary}, " ")
	switch {
	case containsAny(text, "visual", "ui", "ux", "quality", "qa", "test", "validation", "smoke", "review", "acceptance"):
		return "qa"
	case containsAny(text, "integrat", "patch", "queue", "merge", "release"):
		return "integration"
	case containsAny(text, "implement", "code", "fix", "repair", "build"):
		return "implementation"
	default:
		return "coordination"
	}
}

func internalHeartbeatPriorityRank(priority string) int {
	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "CRITICAL", "P0", "URGENT":
		return 0
	case "HIGH", "P1":
		return 1
	case "MEDIUM", "NORMAL", "P2":
		return 2
	case "LOW", "P3":
		return 3
	default:
		return 4
	}
}

func internalHeartbeatNonEmptyIntMap(values map[string]int) map[string]int {
	out := map[string]int{}
	for key, value := range values {
		key = strings.TrimSpace(key)
		if key == "" || value == 0 {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func internalHeartbeatLimit[T any](values []T, limit int) []T {
	if limit <= 0 || len(values) == 0 {
		return nil
	}
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}

func internalHeartbeatMinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func renderInternalHeartbeatPrompt(packet InternalHeartbeatContextPacket) string {
	raw, _ := json.MarshalIndent(packet, "", "  ")
	var b strings.Builder
	b.WriteString("You are running an internal heartbeat session for one Rhizome agent.\n")
	b.WriteString("Use the context packet, your local memory, and the heartbeat role to decide whether there is an actionable local finding.\n")
	if strings.TrimSpace(packet.HeartbeatObjective) != "" {
		b.WriteString("Heartbeat objective: " + strings.TrimSpace(packet.HeartbeatObjective) + "\n")
	}
	if len(packet.Instructions) > 0 {
		b.WriteString("Heartbeat instructions:\n")
		for _, instruction := range packet.Instructions {
			if instruction = strings.TrimSpace(instruction); instruction != "" {
				b.WriteString("- " + instruction + "\n")
			}
		}
	}
	if len(packet.MemoryLanes) > 0 {
		b.WriteString("Shared local memory lanes for this heartbeat: " + strings.Join(packet.MemoryLanes, ", ") + "\n")
	}
	if strings.TrimSpace(packet.ActiveMemoryPolicy.Lane) != "" {
		b.WriteString(fmt.Sprintf("Private active memory lane for this heartbeat: %s (return active_memory[] notes for compact observations worth carrying into future runs).\n", strings.TrimSpace(packet.ActiveMemoryPolicy.Lane)))
	}
	if packet.LocalOnly {
		b.WriteString("This heartbeat is LOCAL-ONLY: do not submit tasks, write workspace docs, request agents, run shell commands, or mutate source files.\n")
	}
	if packet.ActionPolicy.AuthorityBoundary != "" && containsTrimmedString(packet.ActionPolicy.AllowedCapabilities, "will_directives") {
		b.WriteString("This heartbeat may steer the agent through will_directives, constrained by the action_policy and policy_instructions.\n")
	}
	if len(packet.RequiredToolArtifacts) > 0 {
		b.WriteString("Required tool artifact contract:\n")
		for _, artifact := range packet.RequiredToolArtifacts {
			name := firstNonEmpty(strings.TrimSpace(artifact.Tool), "unknown_tool")
			when := firstNonEmpty(strings.TrimSpace(artifact.When), "always")
			status := "not due now"
			if artifact.RequiredNow {
				status = "REQUIRED NOW"
			}
			b.WriteString("- " + name + " (" + status + ", when=" + when + ")")
			if artifact.Purpose != "" {
				b.WriteString(": " + artifact.Purpose)
			}
			if artifact.BlockerGuidance != "" {
				b.WriteString(" Blocker guidance: " + artifact.BlockerGuidance)
			}
			b.WriteString("\n")
		}
		b.WriteString("When a required tool artifact is REQUIRED NOW, build/source/doc review cannot replace it. Call the named tool, or emit an action_request/capability blocker if it cannot run.\n")
	}
	b.WriteString("Return strict JSON only with this shape: {\"contract_version\":\"internal-heartbeat-local-result/v1\",\"outcome\":\"no_action|backlog_recorded|will_updated|blocked|failed\",\"summary\":\"...\",\"active_memory\":[{\"lane\":\"optional lane\",\"note\":\"compact observation/action memory\",\"evidence_refs\":[\"...\"],\"tags\":[\"...\"]}],\"backlog_items\":[{\"dedup_key\":\"stable optional\",\"kind\":\"...\",\"project_id\":\"optional target scope\",\"project_lane\":\"optional target lane\",\"block_promote\":false,\"title\":\"...\",\"summary\":\"...\",\"score\":0,\"evidence_refs\":[\"...\"],\"promote\":false,\"reason\":\"...\"}],\"action_requests\":[{\"request_id\":\"stable optional\",\"capability\":\"browser_screenshot|shell_execution|workspace_write|task_loop|human_input|other\",\"tool_suite\":\"optional configured suite\",\"project_id\":\"optional target scope\",\"project_lane\":\"optional target lane\",\"title\":\"...\",\"summary\":\"...\",\"score\":0,\"evidence_refs\":[\"...\"],\"promote\":false,\"reason\":\"...\",\"requires_task_loop\":false,\"requires_human_input\":false}],\"will_directives\":[{\"directive_id\":\"stable optional\",\"action\":\"advisory_signal|request_resume|replan_active_work|runtime_switch_task|publish_rhizome_update\",\"task_id\":\"optional existing target task\",\"session_id\":\"optional existing target session\",\"summary\":\"what should change\",\"reason\":\"why\",\"priority\":\"P0|P1|P2|P3\",\"evidence_refs\":[\"...\"]}]}.\n")
	b.WriteString("Do not include materialize, memory_body, task_id, doc_key, or any other public task-cycle fields.\n")
	b.WriteString("backlog_items are agent-local personal backlog candidates. They are not public Rhizome tasks until a separate bounded promotion policy promotes them.\n")
	b.WriteString("action_requests are local records for missing capabilities, authority, evidence, or human input. Use them when the heartbeat cannot truthfully complete a check with its current action_policy.\n")
	b.WriteString("will_directives are local steering requests for the runtime planner; use them only when the configured will_policy allows the action and evidence supports the change.\n")
	b.WriteString("Context packet:\n")
	b.Write(raw)
	return strings.TrimSpace(b.String())
}

func (r *Runtime) runInternalHeartbeatNoToolLLM(ctx context.Context, packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool, error) {
	if r == nil || r.agent == nil || r.agent.LLM == nil {
		return InternalHeartbeatLocalResult{}, false, nil
	}
	messages := []Message{
		{
			Role: "system",
			Content: strings.TrimSpace(
				"Run one private Rhizome internal heartbeat. Output only strict JSON for the requested contract. Do not call tools.",
			),
		},
		{Role: "user", Content: renderInternalHeartbeatPrompt(packet)},
	}
	response, err := r.agent.LLM.Chat(ctx, messages, nil)
	if err != nil {
		return InternalHeartbeatLocalResult{}, true, err
	}
	if response == nil {
		return InternalHeartbeatLocalResult{}, true, fmt.Errorf("internal heartbeat LLM returned nil response")
	}
	if len(response.ToolCalls) > 0 {
		return InternalHeartbeatLocalResult{}, true, fmt.Errorf("internal heartbeat LLM returned %d tool call(s) despite no-tool contract", len(response.ToolCalls))
	}
	result, err := parseInternalHeartbeatLocalResult(response.Content)
	if err != nil {
		return InternalHeartbeatLocalResult{}, true, err
	}
	return result, true, nil
}

func (r *Runtime) runInternalHeartbeatLLM(ctx context.Context, packet InternalHeartbeatContextPacket, policy InternalHeartbeatExecutionPolicy) (InternalHeartbeatLocalResult, bool, error) {
	if policy.MaxToolIterations <= 0 {
		return r.runInternalHeartbeatNoToolLLM(ctx, packet)
	}
	return r.runInternalHeartbeatToolLoopLLM(ctx, packet, policy)
}

func (r *Runtime) runInternalHeartbeatToolLoopLLM(ctx context.Context, packet InternalHeartbeatContextPacket, policy InternalHeartbeatExecutionPolicy) (InternalHeartbeatLocalResult, bool, error) {
	if r == nil || r.agent == nil || r.agent.LLM == nil {
		return InternalHeartbeatLocalResult{}, false, nil
	}
	registry := r.agent.registry
	if registry == nil {
		registry = NewToolRegistry()
	}
	filtered := registry.Filter(func(toolName string) bool {
		return internalHeartbeatReadOnlyToolLoopAllowsWithRegistry(policy, registry, toolName)
	})
	messages := []Message{
		{
			Role: "system",
			Content: strings.TrimSpace(
				"Run one private Rhizome internal heartbeat. You may call only the provided tools. Use tools only when they materially improve the heartbeat finding. Output final strict JSON for the requested contract.",
			),
		},
		{Role: "user", Content: renderInternalHeartbeatPrompt(packet)},
	}
	taskSubmitCount := 0
	executor := func(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
		if !internalHeartbeatReadOnlyToolLoopAllowsWithRegistry(policy, registry, call.Function.Name) {
			return ToolResult{
				Output:  fmt.Sprintf("internal heartbeat %s blocked tool %s by read-only heartbeat tool-loop policy; claim/create a concrete task or use a future typed executor for browser/public-authority work.", firstNonEmpty(policy.HeartbeatID, "unknown"), firstNonEmpty(call.Function.Name, "unknown")),
				IsError: true,
			}
		}
		return r.internalHeartbeatToolExecutorWithPolicy(ctx, registry, call, policy, &taskSubmitCount)
	}
	run, err := RunToolLoopDetailedWithLimit(ctx, r.agent.LLM, filtered, messages, executor, nil, policy.MaxToolIterations)
	if err != nil {
		return InternalHeartbeatLocalResult{}, true, err
	}
	result, err := parseInternalHeartbeatLocalResult(run.Content)
	if err != nil {
		return InternalHeartbeatLocalResult{}, true, err
	}
	result = enforceInternalHeartbeatRequiredToolArtifacts(packet, policy, run.Messages, run.ToolResults, result)
	return result, true, nil
}

func enforceInternalHeartbeatRequiredToolArtifacts(packet InternalHeartbeatContextPacket, policy InternalHeartbeatExecutionPolicy, messages []Message, toolResults []ToolLoopToolResult, result InternalHeartbeatLocalResult) InternalHeartbeatLocalResult {
	result = normalizeInternalHeartbeatLocalResult(result)
	calledTools := internalHeartbeatToolCallsInMessages(messages)
	for _, artifact := range packet.RequiredToolArtifacts {
		if !artifact.RequiredNow {
			continue
		}
		tool := strings.TrimSpace(artifact.Tool)
		if tool == "" {
			continue
		}
		if !calledTools[tool] {
			result.ActionRequests = append(result.ActionRequests, internalHeartbeatMissingRequiredToolArtifactRequest(packet, policy, artifact))
			continue
		}
		if len(toolResults) == 0 {
			continue
		}
		if issue, evidenceRefs := internalHeartbeatRequiredToolArtifactTraceIssue(artifact, toolResults); issue != "" {
			result.ActionRequests = append(result.ActionRequests, internalHeartbeatInvalidRequiredToolArtifactRequest(packet, policy, artifact, issue, evidenceRefs))
		}
	}
	if len(result.ActionRequests) > 0 {
		result.Outcome = "backlog_recorded"
		if strings.TrimSpace(result.Summary) == "" || strings.EqualFold(result.Summary, "no action") {
			result.Summary = "Required heartbeat tool artifact was not produced; recorded an explicit capability/evidence blocker."
		}
	}
	return normalizeInternalHeartbeatLocalResult(result)
}

func internalHeartbeatRequiredToolArtifactTraceIssue(artifact InternalHeartbeatRequiredToolArtifact, toolResults []ToolLoopToolResult) (string, []string) {
	tool := strings.TrimSpace(artifact.Tool)
	if tool == "" {
		return "", nil
	}
	expectedContract := strings.TrimSpace(artifact.ContractVersion)
	toolMatches := []ToolLoopToolResult{}
	contractMatches := []ToolLoopToolResult{}
	for _, trace := range toolResults {
		if strings.TrimSpace(trace.ToolName) != tool {
			continue
		}
		toolMatches = append(toolMatches, trace)
		if expectedContract == "" || strings.TrimSpace(trace.ContractVersion) == expectedContract {
			contractMatches = append(contractMatches, trace)
		}
	}
	if len(toolMatches) == 0 {
		return "required tool artifact call had no structured tool-result trace", []string{"missing_tool_result_trace"}
	}
	if expectedContract != "" && len(contractMatches) == 0 {
		refs := []string{"missing_contract_version", "expected_contract:" + sanitizeRefSegment(expectedContract)}
		for _, trace := range toolMatches {
			if trace.ContractVersion != "" {
				refs = append(refs, "actual_contract:"+sanitizeRefSegment(trace.ContractVersion))
			}
		}
		return fmt.Sprintf("required tool artifact contract %s was not produced by %s", expectedContract, tool), refs
	}
	var firstIssue string
	var firstRefs []string
	for _, trace := range contractMatches {
		issue, refs := internalHeartbeatRequiredToolArtifactSingleTraceIssue(artifact, trace)
		if issue == "" {
			return "", nil
		}
		if firstIssue == "" {
			firstIssue = issue
			firstRefs = refs
		}
	}
	return firstIssue, firstRefs
}

func internalHeartbeatRequiredToolArtifactSingleTraceIssue(artifact InternalHeartbeatRequiredToolArtifact, trace ToolLoopToolResult) (string, []string) {
	refs := []string{"tool_result_trace", "required_tool:" + sanitizeRefSegment(artifact.Tool)}
	if trace.ToolCallID != "" {
		refs = append(refs, "tool_call:"+sanitizeRefSegment(trace.ToolCallID))
	}
	if trace.ContractVersion != "" {
		refs = append(refs, "contract_version:"+sanitizeRefSegment(trace.ContractVersion))
	}
	if trace.IsError {
		return "required tool artifact result was marked as a tool error", append(refs, "tool_result_error")
	}
	status := strings.ToLower(strings.TrimSpace(trace.Status))
	switch status {
	case "pass", "passed", "ok", "success", "succeeded":
	case "":
		return "required tool artifact result did not include a machine-readable status", append(refs, "missing_status")
	case "warn", "warning", "needs_semantic_review":
		return "required tool artifact result returned warning status and requires follow-up before clean visual acceptance", append(refs, "status:"+sanitizeRefSegment(status))
	case "block", "blocked", "fail", "failed", "failure", "error":
		return "required tool artifact result returned blocking/failing status", append(refs, "status:"+sanitizeRefSegment(status))
	default:
		return "required tool artifact result returned unknown status " + status, append(refs, "status:"+sanitizeRefSegment(status))
	}
	if strings.EqualFold(strings.TrimSpace(artifact.Tool), "browser_visual_probe") || strings.EqualFold(strings.TrimSpace(artifact.ContractVersion), "browser_visual_probe_result_v1") {
		return internalHeartbeatBrowserVisualProbeTraceIssue(trace, refs)
	}
	return "", nil
}

func internalHeartbeatBrowserVisualProbeTraceIssue(trace ToolLoopToolResult, refs []string) (string, []string) {
	if len(trace.ArtifactPaths) == 0 && len(trace.ArtifactRefs) == 0 {
		return "browser_visual_probe result did not include screenshot artifact path/ref metadata", append(refs, "missing_artifact_path")
	}
	if len(trace.ArtifactHashes) == 0 && strings.TrimSpace(trace.ArtifactHash) == "" {
		return "browser_visual_probe result did not include screenshot artifact hash metadata", append(refs, "missing_artifact_hash")
	}
	if len(trace.ViewportIDs) == 0 && strings.TrimSpace(trace.ViewportID) == "" {
		return "browser_visual_probe result did not include viewport_id metadata", append(refs, "missing_viewport_id")
	}
	if len(trace.ScenarioIDs) == 0 && strings.TrimSpace(trace.ScenarioID) == "" {
		return "browser_visual_probe result did not include scenario_id metadata", append(refs, "missing_scenario_id")
	}
	if len(trace.StateIDs) == 0 && strings.TrimSpace(trace.StateID) == "" {
		return "browser_visual_probe result did not include state_id metadata", append(refs, "missing_state_id")
	}
	for _, artifactPath := range trace.ArtifactPaths {
		if !internalHeartbeatToolLoopArtifactPathExists(trace, artifactPath) {
			return "browser_visual_probe result referenced a missing/stale screenshot artifact path: " + artifactPath, append(refs, "stale_artifact_path")
		}
	}
	return "", nil
}

func internalHeartbeatToolLoopArtifactPathExists(trace ToolLoopToolResult, artifactPath string) bool {
	artifactPath = strings.TrimSpace(strings.Trim(artifactPath, "\"'`"))
	if artifactPath == "" {
		return false
	}
	if strings.HasPrefix(strings.ToLower(artifactPath), "file://") {
		if localPath, ok := visualAcceptanceLocalPathFromFileURL(artifactPath); ok {
			artifactPath = localPath
		}
	}
	local := filepath.Clean(filepath.FromSlash(artifactPath))
	if filepath.IsAbs(local) {
		return visualAcceptanceFileExists(local)
	}
	candidates := []string{}
	if root := strings.TrimSpace(trace.ArtifactRoot); root != "" {
		candidates = append(candidates, filepath.Join(root, local))
	}
	candidates = append(candidates, local)
	for _, candidate := range uniqueTrimmedCSVStrings(candidates) {
		if visualAcceptanceFileExists(candidate) {
			return true
		}
	}
	return false
}

func internalHeartbeatToolCallsInMessages(messages []Message) map[string]bool {
	out := map[string]bool{}
	for _, message := range messages {
		for _, call := range message.ToolCalls {
			name := strings.TrimSpace(call.Function.Name)
			if name != "" {
				out[name] = true
			}
		}
	}
	return out
}

func internalHeartbeatMissingRequiredToolArtifactRequest(packet InternalHeartbeatContextPacket, policy InternalHeartbeatExecutionPolicy, artifact InternalHeartbeatRequiredToolArtifact) InternalHeartbeatActionRequest {
	tool := strings.TrimSpace(artifact.Tool)
	capability := firstNonEmpty(strings.TrimSpace(artifact.Capability), tool, "required_tool_artifact")
	toolSuite := strings.TrimSpace(artifact.ToolSuite)
	reason := firstNonEmpty(
		strings.TrimSpace(artifact.Reason),
		"required tool artifact was due but the heartbeat returned without calling the tool",
	)
	summary := fmt.Sprintf("Heartbeat %s required tool artifact %s", firstNonEmpty(packet.HeartbeatID, policy.HeartbeatID, "unknown"), tool)
	if artifact.ContractVersion != "" {
		summary += " (" + artifact.ContractVersion + ")"
	}
	summary += " because " + reason + ". Build/source/doc checks cannot satisfy this contract."
	if artifact.BlockerGuidance != "" {
		summary += " " + artifact.BlockerGuidance
	}
	evidenceRefs := []string{
		"required_tool:" + sanitizeRefSegment(tool),
		"heartbeat:" + sanitizeRefSegment(firstNonEmpty(packet.HeartbeatID, policy.HeartbeatID, "unknown")),
		"condition:" + sanitizeRefSegment(firstNonEmpty(artifact.When, "always")),
		"missing_tool_call",
	}
	if artifact.ContractVersion != "" {
		evidenceRefs = append(evidenceRefs, "contract_version:"+sanitizeRefSegment(artifact.ContractVersion))
	}
	if artifact.ToolSuite != "" {
		evidenceRefs = append(evidenceRefs, "tool_suite:"+sanitizeRefSegment(artifact.ToolSuite))
	}
	return InternalHeartbeatActionRequest{
		RequestID:        "required-tool-artifact:" + sanitizeRefSegment(firstNonEmpty(packet.HeartbeatID, policy.HeartbeatID, "heartbeat")) + ":" + sanitizeRefSegment(tool),
		Capability:       capability,
		ToolSuite:        toolSuite,
		ProjectID:        strings.TrimSpace(packet.TrustedScope.ProjectID),
		ProjectLane:      firstNonEmpty(strings.TrimSpace(packet.TrustedScope.ProjectLane), internalHeartbeatCapabilityDefaultLane(capability)),
		Title:            "Required heartbeat tool artifact missing: " + tool,
		Summary:          summary,
		Score:            88,
		EvidenceRefs:     uniqueTrimmedCSVStrings(evidenceRefs),
		Promote:          policy.AllowTaskSubmit,
		Reason:           reason,
		RequiresTaskLoop: true,
	}
}

func internalHeartbeatInvalidRequiredToolArtifactRequest(packet InternalHeartbeatContextPacket, policy InternalHeartbeatExecutionPolicy, artifact InternalHeartbeatRequiredToolArtifact, issue string, evidenceRefs []string) InternalHeartbeatActionRequest {
	request := internalHeartbeatMissingRequiredToolArtifactRequest(packet, policy, artifact)
	tool := strings.TrimSpace(artifact.Tool)
	issue = strings.TrimSpace(issue)
	if issue == "" {
		issue = "required tool artifact output was not valid enough to satisfy the evidence contract"
	}
	request.RequestID = "invalid-required-tool-artifact:" + sanitizeRefSegment(firstNonEmpty(packet.HeartbeatID, policy.HeartbeatID, "heartbeat")) + ":" + sanitizeRefSegment(tool)
	request.Title = "Required heartbeat tool artifact invalid: " + tool
	request.Summary = fmt.Sprintf("Heartbeat %s called %s, but its output did not satisfy the required evidence contract: %s.", firstNonEmpty(packet.HeartbeatID, policy.HeartbeatID, "unknown"), tool, issue)
	if artifact.ContractVersion != "" {
		request.Summary += " Expected contract_version " + artifact.ContractVersion + "."
	}
	request.Reason = issue
	request.EvidenceRefs = uniqueTrimmedCSVStrings(request.EvidenceRefs, evidenceRefs, []string{"invalid_tool_result"})
	return request
}

func internalHeartbeatTypedSensorLocalResult(packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool) {
	merged := normalizeInternalHeartbeatLocalResult(InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "no_action",
	})
	hasResult := false
	for _, sensor := range []func(InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool){
		internalHeartbeatLoopSelfCheckLocalResult,
		internalHeartbeatPersonalBacklogArbiterLocalResult,
		internalHeartbeatActionRequestPromoterLocalResult,
		internalHeartbeatProjectInitiativeLocalResult,
		internalHeartbeatPatchQueueVigilanceLocalResult,
		internalHeartbeatVisualProbeLocalResult,
	} {
		result, ok := sensor(packet)
		if !ok {
			continue
		}
		if !hasResult {
			merged = result
			hasResult = true
			continue
		}
		merged = mergeInternalHeartbeatLocalResults(merged, result)
	}
	if !hasResult {
		return InternalHeartbeatLocalResult{}, false
	}
	return normalizeInternalHeartbeatLocalResult(merged), true
}

func internalHeartbeatLoopSelfCheckLocalResult(packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool) {
	if !strings.EqualFold(strings.TrimSpace(packet.HeartbeatID), "loop_self_check") {
		return InternalHeartbeatLocalResult{}, false
	}
	findings := []InternalHeartbeatFinding{}
	if finding, ok := internalHeartbeatDirtyCheckoutFinding(packet); ok {
		findings = append(findings, finding)
	}
	if finding, ok := internalHeartbeatStaleClaimFinding(packet); ok {
		findings = append(findings, finding)
	}
	if finding, ok := internalHeartbeatImplementationMissingReviewFinding(packet); ok {
		findings = append(findings, finding)
	}
	if finding, ok := internalHeartbeatRepeatedFailureFinding(packet); ok {
		findings = append(findings, finding)
	}
	if finding, ok := internalHeartbeatRepeatedBacklogFinding(packet); ok {
		findings = append(findings, finding)
	}
	if len(findings) == 0 {
		return InternalHeartbeatLocalResult{}, false
	}
	return normalizeInternalHeartbeatLocalResult(InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         "Typed self-check sensor recorded a local runtime-loop finding before any public escalation.",
		BacklogItems:    findings,
	}), true
}

func internalHeartbeatPersonalBacklogArbiterLocalResult(packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool) {
	if !strings.EqualFold(strings.TrimSpace(packet.HeartbeatID), "personal_backlog_arbiter") {
		return InternalHeartbeatLocalResult{}, false
	}
	findings := []InternalHeartbeatFinding{}
	if finding, ok := internalHeartbeatActionRequestRouteFinding(packet); ok {
		findings = append(findings, finding)
	}
	if finding, ok := internalHeartbeatStaleBacklogDecisionFinding(packet); ok {
		findings = append(findings, finding)
	}
	if len(findings) == 0 {
		return InternalHeartbeatLocalResult{}, false
	}
	return normalizeInternalHeartbeatLocalResult(InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         "Typed personal backlog arbiter routed local backlog items without public side effects.",
		BacklogItems:    findings,
	}), true
}

func internalHeartbeatActionRequestPromoterLocalResult(packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool) {
	if !strings.EqualFold(strings.TrimSpace(packet.HeartbeatID), "action_request_promoter") {
		return InternalHeartbeatLocalResult{}, false
	}
	finding, ok := internalHeartbeatActionRequestPromotionFinding(packet)
	if !ok {
		return InternalHeartbeatLocalResult{}, false
	}
	return normalizeInternalHeartbeatLocalResult(InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         "Typed action-request promoter selected one bounded private route for public Rhizome work.",
		BacklogItems:    []InternalHeartbeatFinding{finding},
	}), true
}

func internalHeartbeatActionRequestPromotionFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	var best InternalHeartbeatBacklogSummary
	for _, item := range packet.BacklogCandidates {
		if !internalHeartbeatBacklogSummaryIsOpen(item) {
			continue
		}
		if !internalHeartbeatBacklogSummaryIsPromotableActionRoute(item) {
			continue
		}
		if strings.TrimSpace(item.TargetProjectID) == "" {
			continue
		}
		if item.ActionRequiresHumanInput || containsAny(item.ActionCapability, "human", "operator", "credential", "secret", "domain", "ads", "payment", "budget", "deploy") {
			continue
		}
		if internalHeartbeatActionRouteNeedsVerifiedVisualSurface(item) && !internalHeartbeatPacketHasVerifiedSurfaceForProject(packet, item.TargetProjectID) {
			continue
		}
		if !internalHeartbeatActionRouteReadyForPublicPromotion(item, packet.Now) {
			continue
		}
		if best.ItemID == "" || item.Score > best.Score || (item.Score == best.Score && item.SeenCount > best.SeenCount) || (item.Score == best.Score && item.SeenCount == best.SeenCount && item.ItemID < best.ItemID) {
			best = item
		}
	}
	if strings.TrimSpace(best.ItemID) == "" {
		return InternalHeartbeatFinding{}, false
	}
	capability := firstNonEmpty(best.ActionCapability, "routed_action")
	toolSuite := strings.TrimSpace(best.ActionToolSuite)
	refs := []string{
		"backlog_item:" + best.ItemID,
		"source_dedup:" + best.DedupKey,
		"capability:" + sanitizeRefSegment(capability),
		"target_project:" + sanitizeRefSegment(best.TargetProjectID),
	}
	if toolSuite != "" {
		refs = append(refs, "tool_suite:"+sanitizeRefSegment(toolSuite))
	}
	if best.ActionRequiresTaskLoop {
		refs = append(refs, "requires:task_loop")
	}
	return InternalHeartbeatFinding{
		DedupKey:    "action-request-promoter:" + sanitizeRefSegment(firstNonEmpty(best.DedupKey, best.ItemID)),
		Kind:        "action_request_public_followup",
		Source:      internalHeartbeatActionRequestPromoterSource,
		ProjectID:   best.TargetProjectID,
		ProjectLane: internalHeartbeatActionRoutePromotionLane(capability, best.TargetProjectLane),
		Title:       "Resolve routed agent action request: " + capability,
		Summary: fmt.Sprintf(
			"Private backlog route %s has score %d and has recurred %d time(s); it needs a bounded public owner for %s%s instead of remaining only in local agent memory.",
			firstNonEmpty(best.Title, best.ItemID),
			best.Score,
			best.SeenCount,
			capability,
			internalHeartbeatActionRouteToolSuiteSuffix(toolSuite),
		),
		Score:        internalHeartbeatClampScore(best.Score + 4),
		EvidenceRefs: uniqueTrimmedCSVStrings(refs),
		Promote:      true,
		Reason:       "project-scoped private action route is stale/high-score enough to become public Rhizome work",
	}, true
}

func internalHeartbeatActionRouteNeedsVerifiedVisualSurface(item InternalHeartbeatBacklogSummary) bool {
	if internalHeartbeatActionRouteClassNeedsVerifiedVisualSurface(item.ActionCapability) ||
		internalHeartbeatActionRouteClassNeedsVerifiedVisualSurface(item.ActionToolSuite) {
		return true
	}
	for _, ref := range item.EvidenceRefs {
		if internalHeartbeatActionRouteClassNeedsVerifiedVisualSurface(ref) {
			return true
		}
	}
	text := strings.ToLower(strings.Join(append([]string{
		item.HeartbeatID,
		item.Kind,
		item.Title,
		item.Summary,
		item.ActionCapability,
		item.ActionToolSuite,
	}, item.EvidenceRefs...), " "))
	if !containsAny(text, "visual", "browser", "screenshot", "runnable_surface", "runnable surface", "surface") {
		return false
	}
	return containsAny(text, "missing_surface", "missing surface", "runnable surface is missing", "provide a runnable", "browser_screenshot", "screenshot_capture", "capture screenshot", "visual audit", "visual probe", "browser_visual_probe", "browser session", "browser_read_only")
}

func internalHeartbeatActionRouteClassNeedsVerifiedVisualSurface(value string) bool {
	normalized := internalHeartbeatNormalizeActionRouteClass(value)
	if normalized == "" {
		return false
	}
	if strings.HasPrefix(normalized, "browser_") || strings.HasPrefix(normalized, "visual_") {
		return true
	}
	return stringInSet(normalized,
		"browser",
		"browser_session",
		"browser_interaction",
		"browser_screenshot",
		"browser_visual_probe",
		"browser_read_only",
		"screenshot",
		"screenshot_capture",
		"capture_screenshot",
		"visual",
		"visual_audit",
		"visual_probe",
		"visual_qa",
		"browser_smoke",
		"runnable_surface",
	)
}

func internalHeartbeatNormalizeActionRouteClass(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, "capability:")
	value = strings.TrimPrefix(value, "tool_suite:")
	replacer := strings.NewReplacer("-", "_", " ", "_", "/", "_", ".", "_")
	value = replacer.Replace(value)
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ':' || r == ';' || r == ',' || r == '[' || r == ']' || r == '(' || r == ')' || r == '{' || r == '}'
	})
	if len(fields) == 0 {
		return ""
	}
	return strings.Trim(fields[len(fields)-1], "_")
}

func internalHeartbeatActionRouteReadyForPublicPromotion(item InternalHeartbeatBacklogSummary, nowRaw string) bool {
	if item.Score < 80 || item.SeenCount < 2 {
		return false
	}
	now := internalHeartbeatParseTime(nowRaw)
	routeAgeStart := internalHeartbeatParseTime(item.CreatedAt)
	if routeAgeStart.IsZero() {
		routeAgeStart = internalHeartbeatParseTime(item.LastSeenAt)
	}
	if !now.IsZero() && !routeAgeStart.IsZero() && now.Sub(routeAgeStart) < 10*time.Minute {
		return false
	}
	return true
}

func internalHeartbeatBacklogSummaryIsPromotableActionRoute(item InternalHeartbeatBacklogSummary) bool {
	if strings.EqualFold(strings.TrimSpace(item.Kind), "personal_backlog_action_route") &&
		strings.EqualFold(strings.TrimSpace(item.Source), internalHeartbeatBacklogArbiterSource) {
		return true
	}
	return internalHeartbeatBacklogSummaryIsActionRequest(item)
}

func internalHeartbeatProjectInitiativeLocalResult(packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool) {
	if !internalHeartbeatUsesProjectInitiativeSensor(packet) {
		return InternalHeartbeatLocalResult{}, false
	}
	finding, ok := internalHeartbeatPostMVPProjectInitiativeFinding(packet)
	if !ok {
		return InternalHeartbeatLocalResult{}, false
	}
	return normalizeInternalHeartbeatLocalResult(InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         "Typed project initiative sensor found one unowned post-MVP quality loop after public tasks closed.",
		BacklogItems:    []InternalHeartbeatFinding{finding},
	}), true
}

func internalHeartbeatPatchQueueVigilanceLocalResult(packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool) {
	if !internalHeartbeatUsesPatchQueueVigilanceSensor(packet) {
		return InternalHeartbeatLocalResult{}, false
	}
	findings := internalHeartbeatPatchQueueVigilanceFindings(packet)
	if len(findings) == 0 {
		return InternalHeartbeatLocalResult{}, false
	}
	return normalizeInternalHeartbeatLocalResult(InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         "Typed patch queue vigilance sensor recorded actionable queue debt before it could silently stall.",
		BacklogItems:    findings,
	}), true
}

func internalHeartbeatUsesPatchQueueVigilanceSensor(packet InternalHeartbeatContextPacket) bool {
	if strings.EqualFold(strings.TrimSpace(packet.HeartbeatID), "patch_queue_vigilance") {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(packet.HeartbeatKind), "integration_vigilance") {
		return false
	}
	return containsTrimmedString(packet.ContextSelectors, "patch_queue") &&
		(containsTrimmedString(packet.OutputContracts, "queue_health_note") ||
			containsTrimmedString(packet.OutputContracts, "bounded_integration_task_if_safe")) &&
		(containsTrimmedString(packet.PromotionSignals, "accepted_queue_stale") ||
			containsTrimmedString(packet.PromotionSignals, "missing_integration_owner") ||
			containsTrimmedString(packet.PromotionSignals, "verification_gap"))
}

func internalHeartbeatSpecUsesPatchQueueVigilancePromotion(spec AgentHeartbeatSpec) bool {
	spec = normalizeAgentHeartbeatSpec(spec)
	if strings.EqualFold(strings.TrimSpace(spec.ID), "patch_queue_vigilance") {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(spec.Kind), "integration_vigilance") {
		return false
	}
	return containsTrimmedString(spec.ContextSelectors, "patch_queue") &&
		(containsTrimmedString(spec.OutputContracts, "queue_health_note") ||
			containsTrimmedString(spec.OutputContracts, "bounded_integration_task_if_safe")) &&
		(containsTrimmedString(spec.PromotionSignals, "accepted_queue_stale") ||
			containsTrimmedString(spec.PromotionSignals, "missing_integration_owner") ||
			containsTrimmedString(spec.PromotionSignals, "verification_gap"))
}

func internalHeartbeatPatchQueueVigilanceFindings(packet InternalHeartbeatContextPacket) []InternalHeartbeatFinding {
	items := internalHeartbeatAllPatchQueueSummaries(packet)
	if len(items) == 0 {
		return nil
	}
	now := internalHeartbeatParseTime(packet.Now)
	out := make([]InternalHeartbeatFinding, 0, 3)
	for _, item := range items {
		if len(out) >= 3 {
			break
		}
		if finding, ok := internalHeartbeatPatchQueueVisualEvidenceFinding(packet, item, now); ok {
			out = append(out, finding)
			continue
		}
		if finding, ok := internalHeartbeatPatchQueueAcceptedIntegrationFinding(packet, item, now); ok {
			out = append(out, finding)
			continue
		}
		if finding, ok := internalHeartbeatPatchQueueBlockedOrClaimedFinding(packet, item, now); ok {
			out = append(out, finding)
			continue
		}
	}
	return out
}

func internalHeartbeatPatchQueueVisualEvidenceFinding(packet InternalHeartbeatContextPacket, item InternalHeartbeatPatchQueueSummary, now time.Time) (InternalHeartbeatFinding, bool) {
	if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") || !internalHeartbeatPatchQueueItemLooksUIFacing(item) {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatPatchQueueItemResolved(item) {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatPatchQueueFollowupExists(packet, item, "visual") || internalHeartbeatPatchQueueVisualEvidenceDocExists(packet, item) {
		return InternalHeartbeatFinding{}, false
	}
	return InternalHeartbeatFinding{
		DedupKey:    "patch-queue-vigilance:visual-evidence:" + internalHeartbeatPatchQueueItemKey(item),
		Kind:        "patch_queue_visual_evidence_gap",
		Source:      internalHeartbeatPatchQueueVigilanceSource,
		ProjectID:   strings.TrimSpace(item.ProjectID),
		ProjectLane: "qa",
		Title:       "Accepted UI patch queue candidate lacks visual evidence",
		Summary: fmt.Sprintf(
			"Patch queue item %s/%s is ACCEPTED and appears UI-facing, but no matching visual acceptance evidence doc or open visual follow-up is visible.",
			firstNonEmpty(item.QueueID, "queue"),
			firstNonEmpty(item.ItemID, "item"),
		),
		Score:        91,
		EvidenceRefs: internalHeartbeatPatchQueueEvidenceRefs(item, "missing:visual_acceptance_evidence", "state:ACCEPTED"),
		Promote:      packet.AllowTaskSubmit,
		Reason:       "accepted UI-facing patch queue candidate lacks durable visual evidence",
		Meta:         internalHeartbeatPatchQueueFindingMeta(item),
	}, true
}

func internalHeartbeatPatchQueueAcceptedIntegrationFinding(packet InternalHeartbeatContextPacket, item InternalHeartbeatPatchQueueSummary, now time.Time) (InternalHeartbeatFinding, bool) {
	if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatPatchQueueItemResolved(item) {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatPatchQueueFollowupExists(packet, item, "integration") {
		return InternalHeartbeatFinding{}, false
	}
	score := 88
	if !now.IsZero() {
		if observed := internalHeartbeatPatchQueueObservedAt(item); !observed.IsZero() && now.Sub(observed) >= 30*time.Minute {
			score = 92
		}
	}
	evidenceRefs := []string{"state:ACCEPTED", "missing:integration_owner"}
	if projectPatchQueueAcceptedIntegrationNeedsDirectMerge(item.RepoAuthorityMode, item.MaterializationSchema, item.MaterializationDigest, item.MaterializationAccepted) {
		evidenceRefs = append(evidenceRefs,
			"repo_authority_mode:repoauthority_controlled_queue",
			"materialization:missing",
			"integration_mode:direct_merge",
		)
	}
	return InternalHeartbeatFinding{
		DedupKey:    "patch-queue-vigilance:accepted-integration:" + internalHeartbeatPatchQueueItemKey(item),
		Kind:        "patch_queue_accepted_integration_gap",
		Source:      internalHeartbeatPatchQueueVigilanceSource,
		ProjectID:   strings.TrimSpace(item.ProjectID),
		ProjectLane: "integration",
		Title:       "Accepted patch queue candidate needs integration ownership",
		Summary: fmt.Sprintf(
			"Patch queue item %s/%s is ACCEPTED, but no open integration follow-up is visible; an integrator should verify and move the accepted candidate toward canonical main.",
			firstNonEmpty(item.QueueID, "queue"),
			firstNonEmpty(item.ItemID, "item"),
		),
		Score:        score,
		EvidenceRefs: internalHeartbeatPatchQueueEvidenceRefs(item, evidenceRefs...),
		Promote:      packet.AllowTaskSubmit,
		Reason:       "accepted patch queue candidate has no visible integration owner",
		Meta:         internalHeartbeatPatchQueueFindingMeta(item),
	}, true
}

func internalHeartbeatPatchQueueBlockedOrClaimedFinding(packet InternalHeartbeatContextPacket, item InternalHeartbeatPatchQueueSummary, now time.Time) (InternalHeartbeatFinding, bool) {
	state := strings.ToUpper(strings.TrimSpace(item.State))
	if state != "BLOCKED" && state != "CLAIMED" && state != "REVIEW_READY" && state != "READY_FOR_REVIEW" && state != "PENDING_REVIEW" {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatPatchQueueItemResolved(item) {
		return InternalHeartbeatFinding{}, false
	}
	if strings.TrimSpace(item.SupersedesQueueID) != "" || strings.TrimSpace(item.SupersedesItemID) != "" {
		return InternalHeartbeatFinding{}, false
	}
	if state == "CLAIMED" && !internalHeartbeatPatchQueueClaimLooksStale(item, now) {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatPatchQueueFollowupExists(packet, item, "queue") {
		return InternalHeartbeatFinding{}, false
	}
	lane := "integration"
	kind := "patch_queue_convergence_gap"
	title := "Patch queue candidate needs queue stewardship"
	reason := "patch queue item is blocked or stale-claimed without visible stewardship"
	score := 84
	extra := "missing:queue_stewardship"
	if state == "REVIEW_READY" || state == "READY_FOR_REVIEW" || state == "PENDING_REVIEW" {
		lane = "review"
		kind = "patch_queue_review_owner_gap"
		title = "Review-ready patch queue candidate lacks reviewer ownership"
		reason = "review-ready patch queue item has no visible reviewer follow-up"
		score = 80
		extra = "missing:review_owner"
	}
	return InternalHeartbeatFinding{
		DedupKey:    "patch-queue-vigilance:" + sanitizeRefSegment(strings.ToLower(kind)) + ":" + internalHeartbeatPatchQueueItemKey(item),
		Kind:        kind,
		Source:      internalHeartbeatPatchQueueVigilanceSource,
		ProjectID:   strings.TrimSpace(item.ProjectID),
		ProjectLane: lane,
		Title:       title,
		Summary: fmt.Sprintf(
			"Patch queue item %s/%s is %s and has no visible active follow-up; create one bounded stewardship task instead of mutating the queue directly from heartbeat.",
			firstNonEmpty(item.QueueID, "queue"),
			firstNonEmpty(item.ItemID, "item"),
			firstNonEmpty(state, "unknown"),
		),
		Score:        score,
		EvidenceRefs: internalHeartbeatPatchQueueEvidenceRefs(item, "state:"+firstNonEmpty(state, "unknown"), extra),
		Promote:      packet.AllowTaskSubmit,
		Reason:       reason,
		Meta:         internalHeartbeatPatchQueueFindingMeta(item),
	}, true
}

func internalHeartbeatPatchQueueFindingMeta(item InternalHeartbeatPatchQueueSummary) map[string]string {
	meta := map[string]string{
		"queue_id":                 strings.TrimSpace(item.QueueID),
		"item_id":                  strings.TrimSpace(item.ItemID),
		"project_id":               strings.TrimSpace(item.ProjectID),
		"repo_id":                  strings.TrimSpace(item.RepoID),
		"branch_id":                strings.TrimSpace(item.BranchID),
		"head_sha":                 strings.TrimSpace(item.HeadSHA),
		"state":                    strings.ToUpper(strings.TrimSpace(item.State)),
		"repo_authority_mode":      strings.TrimSpace(item.RepoAuthorityMode),
		"materialization_accepted": fmt.Sprint(item.MaterializationAccepted),
		"materialization_schema":   strings.TrimSpace(item.MaterializationSchema),
		"materialization_digest":   strings.TrimSpace(item.MaterializationDigest),
		"review_doc_key":           strings.TrimSpace(item.ReviewDocKey),
		"evidence_doc_key":         strings.TrimSpace(item.EvidenceDocKey),
		"decision_doc_key":         strings.TrimSpace(item.DecisionDocKey),
		"decision_summary":         strings.TrimSpace(item.DecisionSummary),
		"supersedes_queue_id":      strings.TrimSpace(item.SupersedesQueueID),
		"supersedes_item_id":       strings.TrimSpace(item.SupersedesItemID),
	}
	for key, value := range meta {
		if strings.TrimSpace(value) == "" {
			delete(meta, key)
		}
	}
	return meta
}

func internalHeartbeatPatchQueueClaimLooksStale(item InternalHeartbeatPatchQueueSummary, now time.Time) bool {
	if now.IsZero() {
		return false
	}
	if expires := internalHeartbeatParseTime(item.ClaimExpiresAt); !expires.IsZero() {
		return expires.Before(now)
	}
	observed := internalHeartbeatPatchQueueObservedAt(item)
	return !observed.IsZero() && now.Sub(observed) >= 45*time.Minute
}

func internalHeartbeatPatchQueueItemResolved(item InternalHeartbeatPatchQueueSummary) bool {
	state := strings.ToUpper(strings.TrimSpace(item.State))
	branchStatus := strings.ToUpper(strings.TrimSpace(item.BranchStatus))
	if stringInSet(state, "MERGED", "INTEGRATED", "SUPERSEDED", "CLOSED", "CANCELED", "CANCELLED") {
		return true
	}
	if stringInSet(branchStatus, "MERGED", "INTEGRATED", "DELETED", "ARCHIVED", "CLOSED", "SUPERSEDED") {
		return true
	}
	return false
}

func internalHeartbeatPatchQueueItemLooksUIFacing(item InternalHeartbeatPatchQueueSummary) bool {
	text := strings.ToLower(strings.Join(append([]string{item.DecisionSummary}, item.PathHints...), "\n"))
	return containsAnySignal(text, []string{
		"index.html", "public/", "web/", "src/", "app/", "components/", "pages/", "ui/", "static/", "assets/", "styles/",
		".tsx", ".jsx", ".css", ".scss", ".vue", ".svelte", "vite", "next", "tailwind",
		"frontend", "front-end", "web app", "browser app", "react", "visual", "layout", "screenshot", "viewport",
	})
}

func internalHeartbeatPatchQueueVisualEvidenceDocExists(packet InternalHeartbeatContextPacket, item InternalHeartbeatPatchQueueSummary) bool {
	identifiers := internalHeartbeatPatchQueueIdentifiers(item)
	if len(identifiers) == 0 {
		return false
	}
	for _, doc := range internalHeartbeatAllDocSummaries(packet) {
		text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title}, " "))
		if !containsAnySignal(text, []string{"visual", "screenshot", "viewport", "browser", "rhizome_visual_acceptance_v1", "visual_acceptance", "acceptance"}) {
			continue
		}
		for _, id := range identifiers {
			if id != "" && strings.Contains(text, strings.ToLower(id)) {
				return true
			}
		}
	}
	return false
}

func internalHeartbeatPatchQueueFollowupExists(packet InternalHeartbeatContextPacket, item InternalHeartbeatPatchQueueSummary, kind string) bool {
	identifiers := internalHeartbeatPatchQueueIdentifiers(item)
	if len(identifiers) == 0 {
		return false
	}
	for _, task := range internalHeartbeatAllTaskSummaries(packet) {
		if taskSubmitTaskIsTerminalStatus(task.Status) {
			continue
		}
		if item.ProjectID != "" && task.ProjectID != "" && strings.TrimSpace(task.ProjectID) != strings.TrimSpace(item.ProjectID) {
			continue
		}
		if internalHeartbeatPatchQueueTaskRequirementsMatch(task, item, kind) {
			return true
		}
		text := strings.ToLower(strings.Join([]string{task.TaskID, task.Title, task.Description, task.ProjectLane, task.TaskKind, strings.Join(task.Tags, " ")}, " "))
		matchesID := false
		for _, id := range identifiers {
			if id != "" && strings.Contains(text, strings.ToLower(id)) {
				matchesID = true
				break
			}
		}
		if !matchesID {
			continue
		}
		switch strings.TrimSpace(kind) {
		case "visual":
			if containsAnySignal(text, []string{"visual", "screenshot", "viewport", "browser", "acceptance", "qa"}) {
				return true
			}
		case "integration":
			if containsAnySignal(text, []string{"integrat", "merge", "canonical", "accepted", "release"}) {
				return true
			}
		case "queue":
			if containsAnySignal(text, []string{"patch", "queue", "review", "claim", "steward", "supersede", "requeue", "blocked", "integration", "verification"}) {
				return true
			}
		default:
			return true
		}
	}
	return false
}

func internalHeartbeatPatchQueueTaskRequirementsMatch(task InternalHeartbeatTaskSummary, item InternalHeartbeatPatchQueueSummary, kind string) bool {
	requirements := internalHeartbeatTaskRequirements(task)
	if len(requirements) == 0 {
		return false
	}
	if reqKind := strings.ToLower(strings.TrimSpace(stringMapValue(requirements, "patch_queue_task_kind"))); reqKind != "" {
		switch strings.ToLower(strings.TrimSpace(kind)) {
		case "integration":
			if reqKind != "integration" {
				return false
			}
		case "visual":
			if reqKind != "visual" && reqKind != "validation" {
				return false
			}
		case "queue":
			if !stringInSet(reqKind, "integration", "revision", "validation", "rebuild", "review", "review_receipt") {
				return false
			}
		default:
			if reqKind != strings.ToLower(strings.TrimSpace(kind)) {
				return false
			}
		}
	}
	for _, pair := range []struct {
		key   string
		value string
	}{
		{"queue_id", item.QueueID},
		{"patch_queue_id", item.QueueID},
		{"item_id", item.ItemID},
		{"branch_id", item.BranchID},
		{"head_sha", item.HeadSHA},
	} {
		reqValue := strings.TrimSpace(stringMapValue(requirements, pair.key))
		if reqValue == "" || strings.TrimSpace(pair.value) == "" {
			continue
		}
		if pair.key == "head_sha" {
			if !internalHeartbeatHeadMatches(reqValue, pair.value) {
				return false
			}
			continue
		}
		if !strings.EqualFold(reqValue, strings.TrimSpace(pair.value)) {
			return false
		}
	}
	return strings.TrimSpace(stringMapValue(requirements, "queue_id")) != "" ||
		strings.TrimSpace(stringMapValue(requirements, "patch_queue_id")) != "" ||
		strings.TrimSpace(stringMapValue(requirements, "item_id")) != "" ||
		strings.TrimSpace(stringMapValue(requirements, "branch_id")) != "" ||
		strings.TrimSpace(stringMapValue(requirements, "head_sha")) != ""
}

func internalHeartbeatTaskRequirements(task InternalHeartbeatTaskSummary) map[string]any {
	raw := strings.TrimSpace(task.TaskRequirementsJSON)
	if raw == "" || raw == "{}" {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

func internalHeartbeatHeadMatches(requirementHead, itemHead string) bool {
	requirementHead = strings.ToLower(strings.TrimSpace(requirementHead))
	itemHead = strings.ToLower(strings.TrimSpace(itemHead))
	if requirementHead == "" || itemHead == "" {
		return true
	}
	if requirementHead == itemHead {
		return true
	}
	return len(requirementHead) >= 8 && len(itemHead) >= 8 && (strings.HasPrefix(requirementHead, itemHead) || strings.HasPrefix(itemHead, requirementHead))
}

func internalHeartbeatPatchQueueIdentifiers(item InternalHeartbeatPatchQueueSummary) []string {
	head := strings.TrimSpace(item.HeadSHA)
	if len(head) > 12 {
		head = head[:12]
	}
	return uniqueTrimmedCSVStrings([]string{
		strings.TrimSpace(item.QueueID),
		strings.TrimSpace(item.ItemID),
		strings.TrimSpace(item.BranchID),
		head,
	})
}

func internalHeartbeatPatchQueueItemKey(item InternalHeartbeatPatchQueueSummary) string {
	return sanitizeRefSegment(strings.Join(uniqueTrimmedCSVStrings([]string{
		strings.TrimSpace(item.ProjectID),
		strings.TrimSpace(item.QueueID),
		strings.TrimSpace(item.ItemID),
		strings.TrimSpace(item.BranchID),
		strings.TrimSpace(item.HeadSHA),
	}), "-"))
}

func internalHeartbeatPatchQueueObservedAt(item InternalHeartbeatPatchQueueSummary) time.Time {
	for _, value := range []string{item.DecidedAt, item.UpdatedAt, item.ClaimExpiresAt} {
		if parsed := internalHeartbeatParseTime(value); !parsed.IsZero() {
			return parsed
		}
	}
	return time.Time{}
}

func internalHeartbeatPatchQueueEvidenceRefs(item InternalHeartbeatPatchQueueSummary, extra ...string) []string {
	refs := []string{
		"patch_queue:" + sanitizeRefSegment(item.QueueID),
		"patch_item:" + sanitizeRefSegment(item.ItemID),
		"project:" + sanitizeRefSegment(item.ProjectID),
	}
	if strings.TrimSpace(item.BranchID) != "" {
		refs = append(refs, "branch:"+sanitizeRefSegment(item.BranchID))
	}
	if strings.TrimSpace(item.HeadSHA) != "" {
		refs = append(refs, "head:"+sanitizeRefSegment(item.HeadSHA))
	}
	for _, docKey := range []string{item.ReviewDocKey, item.EvidenceDocKey, item.DecisionDocKey} {
		if strings.TrimSpace(docKey) != "" {
			refs = append(refs, "doc:"+strings.TrimSpace(docKey))
		}
	}
	refs = append(refs, extra...)
	return uniqueTrimmedCSVStrings(refs)
}

func internalHeartbeatUsesProjectInitiativeSensor(packet InternalHeartbeatContextPacket) bool {
	if strings.EqualFold(strings.TrimSpace(packet.HeartbeatID), "project_role_initiative") {
		return true
	}
	if !packet.AllowTaskSubmit || packet.LocalOnly {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(packet.HeartbeatKind), "global_metacognition") {
		return false
	}
	return containsTrimmedString(packet.ContextSelectors, "service_pipeline") &&
		(containsTrimmedString(packet.OutputContracts, "initiative_proposal") ||
			containsTrimmedString(packet.OutputContracts, "deploy_readiness_note") ||
			containsTrimmedString(packet.OutputContracts, "bounded_task_if_unowned")) &&
		(containsTrimmedString(packet.PromotionSignals, "service_candidate_with_evidence") ||
			containsTrimmedString(packet.PromotionSignals, "deploy_smoke_gap") ||
			containsTrimmedString(packet.PromotionSignals, "monetization_readiness_gap") ||
			containsTrimmedString(packet.PromotionSignals, "policy_review_gap"))
}

func internalHeartbeatSpecUsesProjectInitiativePromotion(spec AgentHeartbeatSpec) bool {
	spec = normalizeAgentHeartbeatSpec(spec)
	if strings.EqualFold(strings.TrimSpace(spec.ID), "project_role_initiative") {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(spec.Kind), "global_metacognition") {
		return false
	}
	return containsTrimmedString(spec.ContextSelectors, "service_pipeline") &&
		(containsTrimmedString(spec.OutputContracts, "initiative_proposal") ||
			containsTrimmedString(spec.OutputContracts, "deploy_readiness_note") ||
			containsTrimmedString(spec.OutputContracts, "bounded_task_if_unowned")) &&
		(containsTrimmedString(spec.PromotionSignals, "service_candidate_with_evidence") ||
			containsTrimmedString(spec.PromotionSignals, "deploy_smoke_gap") ||
			containsTrimmedString(spec.PromotionSignals, "monetization_readiness_gap") ||
			containsTrimmedString(spec.PromotionSignals, "policy_review_gap"))
}

func internalHeartbeatPostMVPProjectInitiativeFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	if finding, ok := internalHeartbeatServicePipelineProjectInitiativeFinding(packet); ok {
		return finding, true
	}
	project, ok := internalHeartbeatSingleVisibleActiveProject(packet)
	if !ok {
		return InternalHeartbeatFinding{}, false
	}
	projectID := strings.TrimSpace(project.ProjectID)
	if projectID == "" || internalHeartbeatProjectHasOpenWork(packet, projectID) {
		return InternalHeartbeatFinding{}, false
	}
	terminalTasks := internalHeartbeatProjectTerminalTaskRefs(packet, projectID, 4)
	docRefs := internalHeartbeatProjectInitiativeDocRefs(packet, projectID, 4)
	explicitGapRefs := internalHeartbeatProjectInitiativeExplicitGapRefs(packet, projectID, 4)
	serviceGapRefs, serviceGapLane, serviceGapLabel, serviceGapKey := internalHeartbeatProjectInitiativeServiceGapRefs(packet, projectID, 4)
	ownerCoverageRefs := internalHeartbeatProjectInitiativeOwnerCoverageRefs(packet, projectID, 4)
	if len(terminalTasks) == 0 && len(docRefs) == 0 && len(explicitGapRefs) == 0 && len(serviceGapRefs) == 0 {
		return InternalHeartbeatFinding{}, false
	}
	evidence := uniqueTrimmedCSVStrings(append([]string{
		"project:" + projectID,
		"selector:workspace_state",
		"status:all_public_tasks_closed",
	}, append(append(append(append(terminalTasks, docRefs...), explicitGapRefs...), serviceGapRefs...), ownerCoverageRefs...)...))
	projectLabel := firstNonEmpty(project.Title, projectID)
	explicitGap := len(explicitGapRefs) > 0 || len(serviceGapRefs) > 0
	covered := len(ownerCoverageRefs) > 0
	title := "Possible post-MVP project quality loop is unowned"
	summary := fmt.Sprintf(
		"Project %s has no active public tasks after terminal delivery/contract signals; keep this as local sensemaking until explicit unresolved post-MVP work is visible.",
		projectLabel,
	)
	reason := "single active project has no open public work; local sensemaking only until an explicit gap signal exists"
	score := 68 + len(terminalTasks) + len(docRefs)
	if explicitGap {
		title = "Post-MVP project quality loop is unowned"
		summary = fmt.Sprintf(
			"Project %s has no active public tasks and explicit post-MVP/quality gap evidence; a strategist should create one bounded QA/reflection follow-up instead of idling.",
			projectLabel,
		)
		reason = "single active project has no open public work and explicit unresolved post-MVP quality evidence"
		score = 82 + len(terminalTasks) + len(docRefs) + len(explicitGapRefs)*2
	}
	if len(serviceGapRefs) > 0 {
		title = "Service run needs a next-stage owner"
		summary = fmt.Sprintf(
			"Project %s has no active public tasks, but service run %s still needs next-stage product/service evidence; a strategist should open one bounded follow-up instead of treating the pipeline as finished.",
			projectLabel,
			firstNonEmpty(serviceGapLabel, "service-run"),
		)
		reason = "single active project has no open public work and a nonterminal service run needs next-stage evidence"
		score = 86 + len(serviceGapRefs)*2 + len(terminalTasks) + len(docRefs)
	}
	if explicitGap && covered {
		title = "Post-MVP project quality loop already has fresh owner coverage"
		summary = fmt.Sprintf(
			"Project %s has explicit post-MVP/quality gap evidence, but recent role coverage already references that gap; keep this local and avoid creating a duplicate public follow-up.",
			projectLabel,
		)
		reason = "recent agent/update coverage already appears to own the explicit project gap"
		score = 72
	}
	dedupKey := "project-initiative:post-mvp-quality-loop:" + sanitizeRefSegment(projectID)
	kind := "project_post_mvp_quality_gap"
	if len(serviceGapRefs) > 0 && len(explicitGapRefs) == 0 {
		dedupKey = "project-initiative:service-pipeline:" + sanitizeRefSegment(projectID) + ":" + sanitizeRefSegment(firstNonEmpty(serviceGapKey, shortRefHash(strings.Join(serviceGapRefs, "\x00"))))
		kind = "service_pipeline_next_step"
	}
	return InternalHeartbeatFinding{
		DedupKey:     dedupKey,
		Kind:         kind,
		Source:       internalHeartbeatProjectInitiativeSensorSource,
		ProjectID:    projectID,
		ProjectLane:  firstNonEmpty(serviceGapLane, "qa"),
		Title:        title,
		Summary:      summary,
		Score:        internalHeartbeatClampScore(score),
		EvidenceRefs: evidence,
		BlockPromote: covered,
		Promote:      packet.AllowTaskSubmit && explicitGap && !covered,
		Reason:       reason,
	}, true
}

type internalHeartbeatProjectInitiativeServiceGap struct {
	ProjectID string
	Refs      []string
	Lane      string
	Label     string
	Key       string
}

func internalHeartbeatServicePipelineProjectInitiativeFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	gap, ok := internalHeartbeatProjectInitiativeServiceGapCandidate(packet, 4)
	if !ok {
		return InternalHeartbeatFinding{}, false
	}
	projectID := strings.TrimSpace(gap.ProjectID)
	if projectID == "" || internalHeartbeatProjectHasOpenWorkScoped(packet, projectID) {
		return InternalHeartbeatFinding{}, false
	}
	project := internalHeartbeatVisibleProjectByID(packet, projectID)
	terminalTasks := internalHeartbeatProjectTerminalTaskRefs(packet, projectID, 4)
	docRefs := internalHeartbeatProjectInitiativeDocRefs(packet, projectID, 4)
	ownerCoverageRefs := internalHeartbeatProjectInitiativeOwnerCoverageRefs(packet, projectID, 4)
	evidence := uniqueTrimmedCSVStrings(append([]string{
		"project:" + projectID,
		"selector:service_pipeline",
		"status:all_public_tasks_closed",
	}, append(append(terminalTasks, docRefs...), append(gap.Refs, ownerCoverageRefs...)...)...))
	projectLabel := firstNonEmpty(project.Title, gap.Label, projectID)
	covered := len(ownerCoverageRefs) > 0
	title := "Service run needs a next-stage owner"
	if covered {
		title = "Service run next-stage follow-up already has fresh owner coverage"
	}
	return InternalHeartbeatFinding{
		DedupKey:    "project-initiative:service-pipeline:" + sanitizeRefSegment(projectID) + ":" + sanitizeRefSegment(firstNonEmpty(gap.Key, shortRefHash(strings.Join(gap.Refs, "\x00")))),
		Kind:        "service_pipeline_next_step",
		Source:      internalHeartbeatProjectInitiativeSensorSource,
		ProjectID:   projectID,
		ProjectLane: firstNonEmpty(gap.Lane, "qa"),
		Title:       title,
		Summary: fmt.Sprintf(
			"Project %s has no active public tasks for service run %s, but the run still needs next-stage product/service evidence; open one bounded follow-up unless recent role coverage already owns it.",
			projectLabel,
			firstNonEmpty(gap.Label, "service-run"),
		),
		Score:        internalHeartbeatClampScore(86 + len(gap.Refs)*2 + len(terminalTasks) + len(docRefs)),
		EvidenceRefs: evidence,
		BlockPromote: covered,
		Promote:      packet.AllowTaskSubmit && !covered,
		Reason:       "project-scoped service run has no open public work and needs next-stage evidence",
	}, true
}

func internalHeartbeatSingleVisibleActiveProject(packet InternalHeartbeatContextPacket) (InternalHeartbeatProjectSummary, bool) {
	workspaceProjectCount := 0
	projects := map[string]InternalHeartbeatProjectSummary{}
	for _, payload := range packet.SelectorPayloads {
		if payload.Workspace.ProjectCount > workspaceProjectCount {
			workspaceProjectCount = payload.Workspace.ProjectCount
		}
		for _, project := range payload.Projects {
			projectID := strings.TrimSpace(project.ProjectID)
			if projectID == "" || !internalHeartbeatProjectLooksActive(project) {
				continue
			}
			if existing, ok := projects[projectID]; ok {
				if existing.OpenTaskCount == 0 && project.OpenTaskCount != 0 {
					existing.OpenTaskCount = project.OpenTaskCount
				}
				if existing.BlockedTaskCount == 0 && project.BlockedTaskCount != 0 {
					existing.BlockedTaskCount = project.BlockedTaskCount
				}
				if existing.ClaimedTaskCount == 0 && project.ClaimedTaskCount != 0 {
					existing.ClaimedTaskCount = project.ClaimedTaskCount
				}
				if strings.TrimSpace(existing.Title) == "" {
					existing.Title = strings.TrimSpace(project.Title)
				}
				projects[projectID] = existing
				continue
			}
			project.ProjectID = projectID
			project.Title = strings.TrimSpace(project.Title)
			project.Status = strings.TrimSpace(project.Status)
			projects[projectID] = project
		}
	}
	if workspaceProjectCount > 1 || len(projects) != 1 {
		return InternalHeartbeatProjectSummary{}, false
	}
	for _, project := range projects {
		return project, true
	}
	return InternalHeartbeatProjectSummary{}, false
}

func internalHeartbeatVisibleProjectByID(packet InternalHeartbeatContextPacket, projectID string) InternalHeartbeatProjectSummary {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return InternalHeartbeatProjectSummary{}
	}
	for _, payload := range packet.SelectorPayloads {
		for _, project := range payload.Projects {
			if strings.TrimSpace(project.ProjectID) != projectID {
				continue
			}
			project.ProjectID = projectID
			project.Title = strings.TrimSpace(project.Title)
			project.Status = strings.TrimSpace(project.Status)
			return project
		}
	}
	return InternalHeartbeatProjectSummary{ProjectID: projectID}
}

func internalHeartbeatProjectLooksActive(project InternalHeartbeatProjectSummary) bool {
	switch strings.ToUpper(strings.TrimSpace(project.Status)) {
	case "", "ACTIVE", "OPEN", "IN_PROGRESS", "RUNNING", "BUILDING", "REVIEW", "QA", "MVP":
		return true
	case "ARCHIVED", "CANCELLED", "CANCELED", "DELETED", "DONE", "COMPLETED", "CLOSED", "RESOLVED":
		return false
	default:
		return true
	}
}

func internalHeartbeatProjectHasOpenWork(packet InternalHeartbeatContextPacket, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return true
	}
	if strings.TrimSpace(packet.ActiveTaskID) != "" {
		return true
	}
	for _, payload := range packet.SelectorPayloads {
		if payload.Workspace.OpenTaskCount > 0 || payload.Workspace.BlockedTaskCount > 0 {
			return true
		}
		for _, project := range payload.Projects {
			if strings.TrimSpace(project.ProjectID) != projectID {
				continue
			}
			if project.OpenTaskCount > 0 || project.BlockedTaskCount > 0 {
				return true
			}
		}
		for _, task := range payload.Tasks {
			if strings.TrimSpace(task.ProjectID) != projectID {
				continue
			}
			if !taskSubmitTaskIsTerminalStatus(task.Status) || internalHeartbeatClaimLooksActive(task) {
				return true
			}
		}
	}
	return false
}

func internalHeartbeatProjectHasOpenWorkScoped(packet InternalHeartbeatContextPacket, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return true
	}
	activeTaskID := strings.TrimSpace(packet.ActiveTaskID)
	activeTaskSeen := false
	for _, payload := range packet.SelectorPayloads {
		for _, project := range payload.Projects {
			if strings.TrimSpace(project.ProjectID) != projectID {
				continue
			}
			if project.OpenTaskCount > 0 || project.BlockedTaskCount > 0 {
				return true
			}
		}
		for _, task := range payload.Tasks {
			taskID := strings.TrimSpace(task.TaskID)
			if activeTaskID != "" && taskID == activeTaskID {
				activeTaskSeen = true
			}
			if strings.TrimSpace(task.ProjectID) != projectID {
				continue
			}
			if activeTaskID != "" && taskID == activeTaskID {
				return true
			}
			if !taskSubmitTaskIsTerminalStatus(task.Status) || internalHeartbeatClaimLooksActive(task) {
				return true
			}
		}
	}
	if activeTaskID != "" && !activeTaskSeen {
		return true
	}
	return false
}

func internalHeartbeatProjectTerminalTaskRefs(packet InternalHeartbeatContextPacket, projectID string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	refs := []string{}
	seen := map[string]bool{}
	for _, task := range internalHeartbeatAllTaskSummaries(packet) {
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" || seen[taskID] || strings.TrimSpace(task.ProjectID) != strings.TrimSpace(projectID) {
			continue
		}
		if !taskSubmitTaskIsTerminalStatus(task.Status) {
			continue
		}
		seen[taskID] = true
		refs = append(refs, "terminal_task:"+taskID)
		if len(refs) >= limit {
			break
		}
	}
	return refs
}

func internalHeartbeatProjectInitiativeDocRefs(packet InternalHeartbeatContextPacket, projectID string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	refs := []string{}
	for _, doc := range internalHeartbeatAllDocSummaries(packet) {
		docKey := strings.TrimSpace(doc.DocKey)
		if docKey == "" {
			continue
		}
		text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, projectID}, " "))
		if !containsAny(text, "contract", "mvp", "quality", "qa", "review", "evidence", "roadmap", "post-mvp", "post_mvp", "visual", "plan") {
			continue
		}
		refs = append(refs, "doc:"+docKey)
		if len(refs) >= limit {
			break
		}
	}
	return uniqueTrimmedCSVStrings(refs)
}

func internalHeartbeatProjectInitiativeExplicitGapRefs(packet InternalHeartbeatContextPacket, projectID string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	refs := []string{}
	for _, doc := range internalHeartbeatAllDocSummaries(packet) {
		if docKey := strings.TrimSpace(doc.DocKey); docKey != "" && internalHeartbeatProjectInitiativeTextIsExplicitGap(strings.Join([]string{doc.DocKey, doc.Title}, " ")) {
			refs = append(refs, "explicit_gap_doc:"+docKey)
		}
		if len(refs) >= limit {
			return uniqueTrimmedCSVStrings(refs)
		}
	}
	for _, update := range internalHeartbeatAllRecentUpdates(packet) {
		if internalHeartbeatProjectInitiativeTextIsExplicitGap(strings.Join([]string{update.UpdateType, update.Summary, projectID}, " ")) {
			refs = append(refs, "explicit_gap_update:"+shortRefHash(strings.Join([]string{update.AgentID, update.UpdateType, update.CreatedAt, update.Summary}, "\x00")))
		}
		if len(refs) >= limit {
			return uniqueTrimmedCSVStrings(refs)
		}
	}
	for _, backlog := range packet.BacklogCandidates {
		if internalHeartbeatProjectInitiativeTextIsExplicitGap(strings.Join([]string{backlog.Kind, backlog.Title, backlog.DedupKey}, " ")) {
			refs = append(refs, "explicit_gap_backlog:"+backlog.ItemID)
		}
		if len(refs) >= limit {
			return uniqueTrimmedCSVStrings(refs)
		}
	}
	return uniqueTrimmedCSVStrings(refs)
}

func internalHeartbeatProjectInitiativeServiceGapRefs(packet InternalHeartbeatContextPacket, projectID string, limit int) ([]string, string, string, string) {
	gap, ok := internalHeartbeatProjectInitiativeServiceGapForProject(packet, projectID, limit)
	if !ok {
		return nil, "", "", ""
	}
	return gap.Refs, gap.Lane, gap.Label, gap.Key
}

func internalHeartbeatProjectInitiativeServiceGapForProject(packet InternalHeartbeatContextPacket, projectID string, limit int) (internalHeartbeatProjectInitiativeServiceGap, bool) {
	if limit <= 0 {
		return internalHeartbeatProjectInitiativeServiceGap{}, false
	}
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return internalHeartbeatProjectInitiativeServiceGap{}, false
	}
	refs := []string{}
	lane := ""
	label := ""
	key := ""
	now := internalHeartbeatParseTime(packet.Now)
	for _, run := range internalHeartbeatAllServiceRunSummaries(packet) {
		runProjectID := strings.TrimSpace(run.ProjectID)
		if runProjectID != "" && runProjectID != projectID {
			continue
		}
		if !internalHeartbeatServiceRunNeedsAction(run, now) {
			continue
		}
		runID := firstNonEmpty(run.RunID, run.Title, run.CandidateID)
		if runID == "" {
			continue
		}
		if label == "" {
			label = firstNonEmpty(run.Title, run.RunID, run.CandidateID)
			lane = internalHeartbeatServiceRunProjectLane(run.Status)
			key = runID
		}
		refs = append(refs,
			"service_run:"+sanitizeRefSegment(runID),
			"service_status:"+sanitizeRefSegment(firstNonEmpty(run.Status, "unknown")),
			"service_next:"+sanitizeRefSegment(firstNonEmpty(run.NextAction, "inspect service pipeline")),
		)
		if len(refs) >= limit {
			break
		}
	}
	refs = uniqueTrimmedCSVStrings(refs)
	if len(refs) == 0 {
		return internalHeartbeatProjectInitiativeServiceGap{}, false
	}
	return internalHeartbeatProjectInitiativeServiceGap{
		ProjectID: projectID,
		Refs:      refs,
		Lane:      lane,
		Label:     label,
		Key:       key,
	}, true
}

func internalHeartbeatProjectInitiativeServiceGapCandidate(packet InternalHeartbeatContextPacket, limit int) (internalHeartbeatProjectInitiativeServiceGap, bool) {
	if limit <= 0 {
		return internalHeartbeatProjectInitiativeServiceGap{}, false
	}
	now := internalHeartbeatParseTime(packet.Now)
	for _, run := range internalHeartbeatAllServiceRunSummaries(packet) {
		projectID := strings.TrimSpace(run.ProjectID)
		if projectID == "" || !internalHeartbeatServiceRunNeedsAction(run, now) {
			continue
		}
		if gap, ok := internalHeartbeatProjectInitiativeServiceGapForProject(packet, projectID, limit); ok {
			return gap, true
		}
	}
	return internalHeartbeatProjectInitiativeServiceGap{}, false
}

func internalHeartbeatServiceRunNeedsAction(run InternalHeartbeatServiceRunSummary, now time.Time) bool {
	status := strings.ToUpper(strings.TrimSpace(run.Status))
	if status == "" {
		return false
	}
	if !stringInSet(status, "PLANNED", "ACTIVE", "DEPLOYED", "MEASURING", "BLOCKED") {
		return false
	}
	if now.IsZero() {
		return status == "BLOCKED"
	}
	updatedAt := internalHeartbeatParseTime(run.UpdatedAt)
	if updatedAt.IsZero() || updatedAt.After(now.Add(5*time.Minute)) {
		return status == "BLOCKED"
	}
	age := now.Sub(updatedAt)
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "BLOCKED":
		return age >= 15*time.Minute
	case "PLANNED", "ACTIVE":
		return age >= 45*time.Minute
	case "DEPLOYED", "MEASURING":
		return age >= 90*time.Minute
	default:
		return false
	}
}

func internalHeartbeatServiceRunProjectLane(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "PLANNED", "BLOCKED":
		return "coordination"
	case "ACTIVE":
		return "implementation"
	case "DEPLOYED", "MEASURING":
		return "qa"
	default:
		return "coordination"
	}
}

func internalHeartbeatProjectInitiativeOwnerCoverageRefs(packet InternalHeartbeatContextPacket, projectID string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	now := internalHeartbeatParseTime(packet.Now)
	if now.IsZero() {
		return nil
	}
	refs := []string{}
	for _, update := range internalHeartbeatAllRecentUpdates(packet) {
		createdAt := internalHeartbeatParseTime(update.CreatedAt)
		if createdAt.IsZero() || now.Sub(createdAt) > 90*time.Minute || createdAt.After(now.Add(5*time.Minute)) {
			continue
		}
		if !internalHeartbeatProjectInitiativeUpdateTypeCanCoverGap(update.UpdateType) {
			continue
		}
		text := strings.ToLower(strings.Join([]string{update.AgentID, update.UpdateType, update.Summary}, " "))
		if !internalHeartbeatProjectInitiativeTextIsExplicitGap(text) {
			continue
		}
		if strings.TrimSpace(projectID) != "" && !strings.Contains(text, strings.ToLower(strings.TrimSpace(projectID))) && !containsAny(text, "project", "product", "service", "app", "tool") {
			continue
		}
		if !internalHeartbeatProjectInitiativeTextClaimsCoverage(text) {
			continue
		}
		agentID := sanitizeRefSegment(firstNonEmpty(update.AgentID, "unknown"))
		refs = append(refs, "owner_coverage_update:"+agentID+":"+shortRefHash(strings.Join([]string{update.AgentID, update.UpdateType, update.CreatedAt, update.Summary}, "\x00")))
		if len(refs) >= limit {
			return uniqueTrimmedCSVStrings(refs)
		}
	}
	for _, agent := range internalHeartbeatAllAgentSummaries(packet) {
		if !agent.Online {
			continue
		}
		seen := internalHeartbeatParseTime(agent.LastSeenAt)
		if seen.IsZero() || now.Sub(seen) > 90*time.Minute || seen.After(now.Add(5*time.Minute)) {
			continue
		}
		text := strings.ToLower(strings.Join([]string{agent.AgentID, agent.Role, agent.Status, agent.CurrentSessionStatus, agent.CurrentSessionSummary, strings.Join(agent.ActiveTaskIDs, " ")}, " "))
		if !internalHeartbeatProjectInitiativeTextIsExplicitGap(text) || !internalHeartbeatProjectInitiativeTextClaimsCoverage(text) {
			continue
		}
		refs = append(refs, "owner_coverage_agent:"+sanitizeRefSegment(firstNonEmpty(agent.AgentID, "unknown")))
		if len(refs) >= limit {
			return uniqueTrimmedCSVStrings(refs)
		}
	}
	return uniqueTrimmedCSVStrings(refs)
}

func internalHeartbeatProjectInitiativeTextIsExplicitGap(value string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	if text == "" {
		return false
	}
	return containsAny(text,
		"post-mvp", "post_mvp", "post mvp",
		"quality gap", "qa gap", "review gap", "ux gap", "visual gap",
		"unresolved", "unowned", "follow-up", "followup", "next step",
		"needs review", "needs qa", "needs validation", "missing evidence",
		"failing", "failure", "regression", "bug", "defect",
		"visual finding", "ux finding", "smoke gap", "test gap",
		"service run", "service pipeline", "deploy evidence", "deployment evidence",
		"analytics evidence", "measurement evidence", "public health", "launch evidence",
		"revenue evidence", "monetization evidence",
	)
}

func internalHeartbeatProjectInitiativeTextClaimsCoverage(value string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	if text == "" {
		return false
	}
	if containsAny(text, "unowned", "not owned", "no owner", "needs owner", "needs review", "needs qa", "needs validation", "missing evidence", "should create", "should assign") &&
		!containsAny(text, "working on", "owning", "i own", "i am handling", "handling", "covering", "assigned to me") {
		return false
	}
	return containsAny(text,
		"working on", "i am working", "i'm working", "work in progress",
		"taking ownership", "i own", "owning", "owner:", "assigned to", "assigned to me",
		"covered by", "coverage by", "i am covering", "covering",
		"reviewing", "auditing", "validating", "testing", "investigating",
		"started qa", "started review", "started validation", "in progress",
		"will handle", "i will handle", "handling",
	)
}

func internalHeartbeatProjectInitiativeUpdateTypeCanCoverGap(updateType string) bool {
	text := strings.ToLower(strings.TrimSpace(updateType))
	if text == "" {
		return false
	}
	return containsAny(text, "qa", "review", "progress", "coordination", "validation", "smoke", "implementation", "status")
}

func internalHeartbeatAllAgentSummaries(packet InternalHeartbeatContextPacket) []InternalHeartbeatAgentSummary {
	out := []InternalHeartbeatAgentSummary{}
	seen := map[string]bool{}
	for _, payload := range packet.SelectorPayloads {
		for _, agent := range payload.Agents {
			key := strings.TrimSpace(agent.AgentID)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, agent)
		}
	}
	return out
}

func internalHeartbeatAllServiceRunSummaries(packet InternalHeartbeatContextPacket) []InternalHeartbeatServiceRunSummary {
	out := []InternalHeartbeatServiceRunSummary{}
	seen := map[string]bool{}
	for _, payload := range packet.SelectorPayloads {
		for _, run := range payload.ServiceRuns {
			key := strings.TrimSpace(firstNonEmpty(run.RunID, run.ProjectID+"\x00"+run.CandidateID+"\x00"+run.Title))
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, run)
		}
	}
	return out
}

func internalHeartbeatAllPatchQueueSummaries(packet InternalHeartbeatContextPacket) []InternalHeartbeatPatchQueueSummary {
	out := []InternalHeartbeatPatchQueueSummary{}
	seen := map[string]bool{}
	for _, payload := range packet.SelectorPayloads {
		for _, item := range payload.PatchQueue {
			key := strings.TrimSpace(strings.Join([]string{item.QueueID, item.ItemID, item.BranchID, item.HeadSHA}, "\x00"))
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, item)
		}
	}
	return out
}

func internalHeartbeatDirtyCheckoutFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	checkout, ok := internalHeartbeatCurrentDirtyCheckout(packet)
	if !ok {
		return InternalHeartbeatFinding{}, false
	}
	now := internalHeartbeatParseTime(packet.Now)
	if now.IsZero() {
		return InternalHeartbeatFinding{}, false
	}
	anchor := internalHeartbeatCheckoutActivityTime(checkout)
	if !anchor.IsZero() && now.Sub(anchor) < 20*time.Minute {
		return InternalHeartbeatFinding{}, false
	}
	return InternalHeartbeatFinding{
		DedupKey: "self-check:dirty-checkout:" + sanitizeRefSegment(firstNonEmpty(checkout.CheckoutID, checkout.ActiveTaskID, checkout.BranchName)),
		Kind:     "self_check_dirty_checkout_no_commit",
		Source:   internalHeartbeatSelfCheckSensorSource,
		Title:    "Project checkout is dirty without commit evidence",
		Summary: fmt.Sprintf(
			"Checkout %s for task %s is marked dirty (%s) on branch %s; commit/submit patch evidence, publish a blocker, or clean up the checkout before the agent continues looping.",
			firstNonEmpty(checkout.CheckoutID, checkout.LocalPathRef, "checkout"),
			firstNonEmpty(checkout.ActiveTaskID, packet.ActiveTaskID, "unknown"),
			firstNonEmpty(checkout.DirtyState, "dirty"),
			firstNonEmpty(checkout.BranchName, "unknown"),
		),
		Score: 88,
		EvidenceRefs: uniqueTrimmedCSVStrings([]string{
			"checkout:" + checkout.CheckoutID,
			"task:" + firstNonEmpty(checkout.ActiveTaskID, packet.ActiveTaskID),
			"branch:" + checkout.BranchName,
			"dirty_state:" + firstNonEmpty(checkout.DirtyState, "dirty"),
			"status:" + checkout.Status,
			"local_path_ref:" + checkout.LocalPathRef,
		}),
		Promote: false,
		Reason:  "registered project checkout is dirty and not freshly updated",
	}, true
}

func internalHeartbeatStaleClaimFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	task, ok := internalHeartbeatCurrentClaimedTask(packet)
	if !ok {
		return InternalHeartbeatFinding{}, false
	}
	now := internalHeartbeatParseTime(packet.Now)
	if now.IsZero() {
		return InternalHeartbeatFinding{}, false
	}
	anchor := internalHeartbeatTaskActivityTime(task)
	if anchor.IsZero() || now.Sub(anchor) < 45*time.Minute {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatHasFreshOwnEvidence(packet, task, anchor, now) {
		return InternalHeartbeatFinding{}, false
	}
	ageMinutes := int(now.Sub(anchor).Minutes())
	return InternalHeartbeatFinding{
		DedupKey: "self-check:claimed-task-no-evidence:" + sanitizeRefSegment(task.TaskID),
		Kind:     "self_check_claimed_task_no_evidence",
		Source:   internalHeartbeatSelfCheckSensorSource,
		Title:    "Claimed task has no fresh evidence",
		Summary: fmt.Sprintf(
			"Task %s has been claimed or active for about %d minute(s) without a fresh update/evidence signal from this agent; inspect the checkout, publish evidence, or mark the task blocked instead of silently spinning.",
			task.TaskID,
			ageMinutes,
		),
		Score: internalHeartbeatClampScore(70 + ageMinutes/30),
		EvidenceRefs: uniqueTrimmedCSVStrings([]string{
			"task:" + task.TaskID,
			"claim_status:" + firstNonEmpty(task.ClaimStatus, task.Status),
			"claim_agent:" + task.ClaimAgentID,
			"claim_updated_at:" + task.ClaimUpdatedAt,
			"task_updated_at:" + task.UpdatedAt,
			"active_task:" + packet.ActiveTaskID,
		}),
		Promote: false,
		Reason:  "claimed task age exceeded local self-check threshold without fresh evidence",
	}, true
}

func internalHeartbeatImplementationMissingReviewFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	task, ok := internalHeartbeatTerminalImplementationTask(packet)
	if !ok {
		return InternalHeartbeatFinding{}, false
	}
	if internalHeartbeatHasReviewOrPatchEvidence(packet, task) {
		return InternalHeartbeatFinding{}, false
	}
	return InternalHeartbeatFinding{
		DedupKey: "self-check:implementation-missing-review:" + sanitizeRefSegment(task.TaskID),
		Kind:     "self_check_implementation_missing_review",
		Source:   internalHeartbeatSelfCheckSensorSource,
		Title:    "Implementation task lacks review or patch-queue evidence",
		Summary: fmt.Sprintf(
			"Task %s is terminal in an implementation lane, but the heartbeat context has no review, patch-queue, smoke, or verification evidence tied to it; publish or seek durable review/integration evidence before treating the work as done.",
			task.TaskID,
		),
		Score: 76,
		EvidenceRefs: uniqueTrimmedCSVStrings([]string{
			"task:" + task.TaskID,
			"status:" + task.Status,
			"lane:" + task.ProjectLane,
			"missing:review_or_patch_queue_evidence",
		}),
		Promote: false,
		Reason:  "terminal implementation work lacks durable review or patch queue evidence",
	}, true
}

func internalHeartbeatRepeatedFailureFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	type failureSignal struct {
		count     int
		heartbeat string
		outcome   string
		refs      []string
	}
	signals := map[string]failureSignal{}
	for _, session := range packet.RecentSessions {
		status := strings.ToLower(strings.TrimSpace(session.Status))
		if status != "failed" && status != "abandoned" && status != "blocked" {
			continue
		}
		heartbeatID := firstNonEmpty(strings.TrimSpace(session.HeartbeatID), "unknown")
		outcome := firstNonEmpty(strings.TrimSpace(session.Outcome), status)
		key := strings.ToLower(heartbeatID + "\x00" + outcome)
		signal := signals[key]
		signal.count++
		signal.heartbeat = heartbeatID
		signal.outcome = outcome
		if strings.TrimSpace(session.SessionID) != "" {
			signal.refs = append(signal.refs, "internal_session:"+strings.TrimSpace(session.SessionID))
		}
		if strings.TrimSpace(session.Summary) != "" {
			signal.refs = append(signal.refs, "summary:"+internalHeartbeatEvidenceText(session.Summary, 100))
		}
		signals[key] = signal
	}
	var best failureSignal
	for _, signal := range signals {
		if signal.count < 2 {
			continue
		}
		if signal.count > best.count || (signal.count == best.count && signal.heartbeat < best.heartbeat) {
			best = signal
		}
	}
	if best.count < 2 {
		return InternalHeartbeatFinding{}, false
	}
	return InternalHeartbeatFinding{
		DedupKey: "self-check:repeated-failure:" + sanitizeRefSegment(best.heartbeat) + ":" + shortRefHash(best.outcome),
		Kind:     "self_check_repeated_failure",
		Source:   internalHeartbeatSelfCheckSensorSource,
		Title:    "Repeated internal heartbeat failure needs self-repair",
		Summary: fmt.Sprintf(
			"The last internal session history shows %d repeated %s outcome(s) for heartbeat %s; the agent should inspect the loop before continuing normally.",
			best.count,
			firstNonEmpty(best.outcome, "failed"),
			best.heartbeat,
		),
		Score:        82,
		EvidenceRefs: uniqueTrimmedCSVStrings(append([]string{"heartbeat:" + best.heartbeat, "outcome:" + firstNonEmpty(best.outcome, "failed")}, internalHeartbeatLimit(best.refs, 4)...)),
		Promote:      false,
		Reason:       "same internal heartbeat failure repeated in local session history",
	}, true
}

func internalHeartbeatRepeatedBacklogFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	var best InternalHeartbeatBacklogSummary
	for _, item := range packet.BacklogCandidates {
		if strings.EqualFold(strings.TrimSpace(item.Status), "promoted") || strings.EqualFold(strings.TrimSpace(item.Status), "completed") {
			continue
		}
		if internalHeartbeatBacklogSummaryIsRoutingMetadata(item) {
			continue
		}
		if item.SeenCount < 3 && item.Score < 85 {
			continue
		}
		if best.ItemID == "" || item.Score > best.Score || (item.Score == best.Score && item.SeenCount > best.SeenCount) || (item.Score == best.Score && item.SeenCount == best.SeenCount && item.ItemID < best.ItemID) {
			best = item
		}
	}
	if strings.TrimSpace(best.ItemID) == "" {
		return InternalHeartbeatFinding{}, false
	}
	return InternalHeartbeatFinding{
		DedupKey: "self-check:repeated-local-backlog:" + sanitizeRefSegment(firstNonEmpty(best.DedupKey, best.ItemID)),
		Kind:     "self_check_repeated_local_backlog",
		Source:   internalHeartbeatSelfCheckSensorSource,
		Title:    "Repeated local backlog finding needs a decision",
		Summary: fmt.Sprintf(
			"Personal backlog item %s has been seen %d time(s) with score %d; suppress, complete, or let a bounded public-authority heartbeat promote it if it is still actionable.",
			firstNonEmpty(best.Title, best.ItemID),
			best.SeenCount,
			best.Score,
		),
		Score:        internalHeartbeatClampScore(best.Score + 5),
		EvidenceRefs: uniqueTrimmedCSVStrings([]string{"backlog_item:" + best.ItemID, "heartbeat:" + best.HeartbeatID, "dedup:" + best.DedupKey}),
		Promote:      false,
		Reason:       "same personal backlog candidate keeps recurring without resolution",
	}, true
}

func internalHeartbeatActionRequestRouteFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	var best InternalHeartbeatBacklogSummary
	for _, item := range packet.BacklogCandidates {
		if !internalHeartbeatBacklogSummaryIsOpen(item) {
			continue
		}
		if internalHeartbeatBacklogSummaryIsRoutingMetadata(item) {
			continue
		}
		if !internalHeartbeatBacklogSummaryIsActionRequest(item) {
			continue
		}
		if internalHeartbeatBacklogSummaryAlreadyArbiterRoute(packet, item) {
			continue
		}
		if best.ItemID == "" || item.Score > best.Score || (item.Score == best.Score && item.SeenCount > best.SeenCount) || (item.Score == best.Score && item.SeenCount == best.SeenCount && item.ItemID < best.ItemID) {
			best = item
		}
	}
	if strings.TrimSpace(best.ItemID) == "" {
		return InternalHeartbeatFinding{}, false
	}
	capability := firstNonEmpty(best.ActionCapability, "unspecified_capability")
	toolSuite := strings.TrimSpace(best.ActionToolSuite)
	refs := []string{
		"backlog_item:" + best.ItemID,
		"dedup:" + best.DedupKey,
		"heartbeat:" + best.HeartbeatID,
		"capability:" + sanitizeRefSegment(capability),
	}
	if toolSuite != "" {
		refs = append(refs, "tool_suite:"+sanitizeRefSegment(toolSuite))
	}
	if best.ActionRequiresTaskLoop {
		refs = append(refs, "requires:task_loop")
	}
	if best.ActionRequiresHumanInput {
		refs = append(refs, "requires:human_input")
	}
	summary := fmt.Sprintf(
		"Personal backlog action request %s asks for %s%s; keep it local until a heartbeat with matching authority can execute or a bounded public task can safely own it.",
		firstNonEmpty(best.Title, best.ItemID),
		capability,
		internalHeartbeatActionRouteToolSuiteSuffix(toolSuite),
	)
	if best.ActionRequiresHumanInput {
		summary = fmt.Sprintf(
			"Personal backlog action request %s needs human/operator input for %s%s; keep the blocker explicit and local unless a bounded coordination task is later allowed.",
			firstNonEmpty(best.Title, best.ItemID),
			capability,
			internalHeartbeatActionRouteToolSuiteSuffix(toolSuite),
		)
	}
	return InternalHeartbeatFinding{
		DedupKey:     "backlog-arbiter:action-route:" + sanitizeRefSegment(firstNonEmpty(best.DedupKey, best.ItemID)),
		Kind:         "personal_backlog_action_route",
		Source:       internalHeartbeatBacklogArbiterSource,
		ProjectID:    best.TargetProjectID,
		ProjectLane:  firstNonEmpty(best.TargetProjectLane, internalHeartbeatCapabilityDefaultLane(capability)),
		BlockPromote: true,
		Title:        "Route personal action request: " + capability,
		Summary:      summary,
		Score:        internalHeartbeatClampScore(best.Score + 6),
		EvidenceRefs: uniqueTrimmedCSVStrings(refs),
		Promote:      false,
		Reason:       "personal backlog arbiter routed an unresolved action_request into a local decision record",
	}, true
}

func internalHeartbeatStaleBacklogDecisionFinding(packet InternalHeartbeatContextPacket) (InternalHeartbeatFinding, bool) {
	var best InternalHeartbeatBacklogSummary
	for _, item := range packet.BacklogCandidates {
		if !internalHeartbeatBacklogSummaryIsOpen(item) {
			continue
		}
		if internalHeartbeatBacklogSummaryIsActionRequest(item) {
			continue
		}
		if internalHeartbeatBacklogSummaryIsRoutingMetadata(item) {
			continue
		}
		if item.SeenCount < 3 && item.Score < 90 {
			continue
		}
		if internalHeartbeatBacklogSummaryAlreadyArbiterRoute(packet, item) {
			continue
		}
		if best.ItemID == "" || item.Score > best.Score || (item.Score == best.Score && item.SeenCount > best.SeenCount) || (item.Score == best.Score && item.SeenCount == best.SeenCount && item.ItemID < best.ItemID) {
			best = item
		}
	}
	if strings.TrimSpace(best.ItemID) == "" {
		return InternalHeartbeatFinding{}, false
	}
	return InternalHeartbeatFinding{
		DedupKey:     "backlog-arbiter:decision-needed:" + sanitizeRefSegment(firstNonEmpty(best.DedupKey, best.ItemID)),
		Kind:         "personal_backlog_decision_needed",
		Source:       internalHeartbeatBacklogArbiterSource,
		ProjectID:    best.TargetProjectID,
		ProjectLane:  firstNonEmpty(best.TargetProjectLane, "coordination"),
		BlockPromote: true,
		Title:        "Personal backlog item needs triage decision",
		Summary: fmt.Sprintf(
			"Personal backlog item %s has been seen %d time(s) with score %d; the agent should suppress it, complete it, or wait for a bounded authority heartbeat instead of letting it remain inert.",
			firstNonEmpty(best.Title, best.ItemID),
			best.SeenCount,
			best.Score,
		),
		Score:        internalHeartbeatClampScore(best.Score + 4),
		EvidenceRefs: uniqueTrimmedCSVStrings([]string{"backlog_item:" + best.ItemID, "dedup:" + best.DedupKey, "heartbeat:" + best.HeartbeatID}),
		Promote:      false,
		Reason:       "personal backlog arbiter found a recurring local backlog item without a decision",
	}, true
}

func internalHeartbeatBacklogSummaryIsOpen(item InternalHeartbeatBacklogSummary) bool {
	status := strings.ToLower(strings.TrimSpace(item.Status))
	return status == "" || status == "open"
}

func internalHeartbeatBacklogSummaryIsActionRequest(item InternalHeartbeatBacklogSummary) bool {
	if internalHeartbeatBacklogSummaryIsRoutingMetadata(item) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(item.Kind), "heartbeat_action_request") ||
		strings.EqualFold(strings.TrimSpace(item.Source), internalHeartbeatActionRequestSource) ||
		strings.TrimSpace(item.ActionCapability) != "" ||
		strings.TrimSpace(item.ActionToolSuite) != ""
}

func internalHeartbeatBacklogSummaryIsRoutingMetadata(item InternalHeartbeatBacklogSummary) bool {
	source := strings.TrimSpace(item.Source)
	kind := strings.ToLower(strings.TrimSpace(item.Kind))
	return strings.EqualFold(source, internalHeartbeatBacklogArbiterSource) ||
		strings.EqualFold(source, internalHeartbeatCapabilitySessionSource) ||
		strings.EqualFold(source, internalHeartbeatSelfCheckSensorSource) ||
		strings.HasPrefix(kind, "personal_backlog_") ||
		strings.HasPrefix(kind, "capability_session_") ||
		strings.HasPrefix(kind, "self_check_")
}

func internalHeartbeatBacklogSummaryAlreadyArbiterRoute(packet InternalHeartbeatContextPacket, target InternalHeartbeatBacklogSummary) bool {
	targetKey := strings.TrimSpace(firstNonEmpty(target.DedupKey, target.ItemID))
	if targetKey == "" {
		return false
	}
	targetRef := "dedup:" + targetKey
	targetItemRef := "backlog_item:" + strings.TrimSpace(target.ItemID)
	for _, item := range packet.BacklogCandidates {
		if !internalHeartbeatBacklogSummaryIsOpen(item) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Source), internalHeartbeatBacklogArbiterSource) {
			continue
		}
		if strings.Contains(strings.TrimSpace(item.DedupKey), sanitizeRefSegment(targetKey)) {
			return true
		}
		for _, ref := range []string{targetRef, targetItemRef} {
			if ref != "" && strings.Contains(strings.ToLower(item.Title+" "+item.DedupKey), strings.ToLower(ref)) {
				return true
			}
		}
	}
	return false
}

func internalHeartbeatActionRouteToolSuiteSuffix(toolSuite string) string {
	toolSuite = strings.TrimSpace(toolSuite)
	if toolSuite == "" {
		return ""
	}
	return " via " + toolSuite
}

func internalHeartbeatCapabilityDefaultLane(capability string) string {
	capability = strings.ToLower(strings.TrimSpace(capability))
	switch {
	case containsAny(capability, "browser", "screenshot", "visual", "console", "accessibility"):
		return "qa"
	case containsAny(capability, "shell", "test", "build", "local_execution"):
		return "verification"
	case containsAny(capability, "human", "operator", "credential", "secret", "domain", "ads", "payment"):
		return "coordination"
	default:
		return "coordination"
	}
}

func internalHeartbeatCurrentClaimedTask(packet InternalHeartbeatContextPacket) (InternalHeartbeatTaskSummary, bool) {
	activeTaskID := strings.TrimSpace(packet.ActiveTaskID)
	for _, task := range internalHeartbeatAllTaskSummaries(packet) {
		if strings.TrimSpace(task.TaskID) == "" {
			continue
		}
		if activeTaskID != "" && strings.EqualFold(strings.TrimSpace(task.TaskID), activeTaskID) {
			return task, true
		}
		if strings.TrimSpace(task.ClaimAgentID) == strings.TrimSpace(packet.AgentID) && internalHeartbeatClaimLooksActive(task) {
			return task, true
		}
	}
	return InternalHeartbeatTaskSummary{}, false
}

func internalHeartbeatCurrentDirtyCheckout(packet InternalHeartbeatContextPacket) (InternalHeartbeatCheckoutSummary, bool) {
	activeTaskID := strings.TrimSpace(packet.ActiveTaskID)
	agentID := strings.TrimSpace(packet.AgentID)
	var fallback InternalHeartbeatCheckoutSummary
	for _, checkout := range internalHeartbeatAllCheckoutSummaries(packet) {
		if !internalHeartbeatCheckoutIsDirty(checkout) {
			continue
		}
		if activeTaskID != "" && (strings.TrimSpace(checkout.ActiveTaskID) == activeTaskID || strings.TrimSpace(checkout.ActiveClaimID) == activeTaskID) {
			return checkout, true
		}
		if agentID != "" && strings.TrimSpace(checkout.AgentID) == agentID {
			if fallback.CheckoutID == "" {
				fallback = checkout
			}
		}
	}
	if fallback.CheckoutID != "" || fallback.LocalPathRef != "" {
		return fallback, true
	}
	return InternalHeartbeatCheckoutSummary{}, false
}

func internalHeartbeatCheckoutIsDirty(checkout InternalHeartbeatCheckoutSummary) bool {
	dirty := strings.ToLower(strings.TrimSpace(checkout.DirtyState))
	if dirty == "" || dirty == "clean" || dirty == "none" || dirty == "unknown" {
		return false
	}
	status := strings.ToUpper(strings.TrimSpace(firstNonEmpty(checkout.Status, checkout.DerivedStatus)))
	if status == "ARCHIVED" || status == "INVALID" || status == "DELETED" {
		return false
	}
	return true
}

func internalHeartbeatTerminalImplementationTask(packet InternalHeartbeatContextPacket) (InternalHeartbeatTaskSummary, bool) {
	for _, task := range internalHeartbeatAllTaskSummaries(packet) {
		if strings.TrimSpace(task.TaskID) == "" {
			continue
		}
		if strings.TrimSpace(task.ClaimAgentID) != "" && strings.TrimSpace(packet.AgentID) != "" && strings.TrimSpace(task.ClaimAgentID) != strings.TrimSpace(packet.AgentID) {
			continue
		}
		if !taskSubmitTaskIsTerminalStatus(task.Status) {
			continue
		}
		text := strings.ToLower(strings.Join([]string{task.ProjectLane, task.TaskKind, task.Title}, " "))
		if !containsAny(text, "implementation", "implementer", "frontend", "backend", "algorithm", "feature", "code") {
			continue
		}
		return task, true
	}
	return InternalHeartbeatTaskSummary{}, false
}

func internalHeartbeatAllTaskSummaries(packet InternalHeartbeatContextPacket) []InternalHeartbeatTaskSummary {
	out := []InternalHeartbeatTaskSummary{}
	seen := map[string]bool{}
	for _, payload := range packet.SelectorPayloads {
		for _, task := range payload.Tasks {
			id := strings.TrimSpace(task.TaskID)
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, task)
		}
	}
	return out
}

func internalHeartbeatAllCheckoutSummaries(packet InternalHeartbeatContextPacket) []InternalHeartbeatCheckoutSummary {
	out := []InternalHeartbeatCheckoutSummary{}
	seen := map[string]bool{}
	for _, payload := range packet.SelectorPayloads {
		for _, checkout := range payload.Checkouts {
			key := strings.TrimSpace(firstNonEmpty(checkout.CheckoutID, checkout.LocalPathRef, checkout.ActiveTaskID, checkout.BranchName))
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, checkout)
		}
	}
	return out
}

func internalHeartbeatClaimLooksActive(task InternalHeartbeatTaskSummary) bool {
	status := strings.ToUpper(strings.TrimSpace(task.Status))
	claimStatus := strings.ToUpper(strings.TrimSpace(task.ClaimStatus))
	if taskSubmitTaskIsTerminalStatus(status) {
		return false
	}
	return claimStatus == "CLAIMED" || claimStatus == "RUNNING" || status == "RUNNING" || status == "IN_PROGRESS"
}

func taskSubmitTaskIsTerminalStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "DONE", "COMPLETED", "RESOLVED", "CANCELLED", "CLOSED", "ARCHIVED", "FAILED":
		return true
	default:
		return false
	}
}

func internalHeartbeatTaskActivityTime(task InternalHeartbeatTaskSummary) time.Time {
	for _, value := range []string{task.ClaimUpdatedAt, task.UpdatedAt} {
		if parsed := internalHeartbeatParseTime(value); !parsed.IsZero() {
			return parsed
		}
	}
	return time.Time{}
}

func internalHeartbeatCheckoutActivityTime(checkout InternalHeartbeatCheckoutSummary) time.Time {
	for _, value := range []string{checkout.UpdatedAt, checkout.LastSeenAt} {
		if parsed := internalHeartbeatParseTime(value); !parsed.IsZero() {
			return parsed
		}
	}
	return time.Time{}
}

func internalHeartbeatHasFreshOwnEvidence(packet InternalHeartbeatContextPacket, task InternalHeartbeatTaskSummary, since time.Time, now time.Time) bool {
	freshAfter := since
	if !now.IsZero() {
		recentFloor := now.Add(-30 * time.Minute)
		if recentFloor.After(freshAfter) {
			freshAfter = recentFloor
		}
	}
	for _, update := range internalHeartbeatAllRecentUpdates(packet) {
		if strings.TrimSpace(update.AgentID) != "" && strings.TrimSpace(packet.AgentID) != "" && strings.TrimSpace(update.AgentID) != strings.TrimSpace(packet.AgentID) {
			continue
		}
		createdAt := internalHeartbeatParseTime(update.CreatedAt)
		if createdAt.IsZero() || !createdAt.After(freshAfter) {
			continue
		}
		text := strings.ToLower(strings.Join([]string{update.UpdateType, update.Summary}, " "))
		if strings.TrimSpace(task.TaskID) != "" && strings.Contains(text, strings.ToLower(strings.TrimSpace(task.TaskID))) {
			return true
		}
		if containsAny(text, "evidence", "commit", "patch", "review", "smoke", "test", "blocked", "done", "completed", "implemented", "verified") {
			return true
		}
	}
	for _, doc := range internalHeartbeatAllDocSummaries(packet) {
		updatedAt := internalHeartbeatParseTime(doc.UpdatedAt)
		if updatedAt.IsZero() || !updatedAt.After(freshAfter) {
			continue
		}
		text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title}, " "))
		if strings.TrimSpace(task.TaskID) != "" && strings.Contains(text, strings.ToLower(strings.TrimSpace(task.TaskID))) {
			return true
		}
		if containsAny(text, "evidence", "commit", "patch", "review", "smoke", "test", "blocked", "done", "completed", "implemented", "verified") {
			return true
		}
	}
	return false
}

func internalHeartbeatHasReviewOrPatchEvidence(packet InternalHeartbeatContextPacket, task InternalHeartbeatTaskSummary) bool {
	for _, update := range internalHeartbeatAllRecentUpdates(packet) {
		text := strings.ToLower(strings.Join([]string{update.UpdateType, update.Summary}, " "))
		if !internalHeartbeatTextMatchesTaskOrProject(text, task) {
			continue
		}
		if containsAny(text, "patch_queue", "patch queue", "review", "review_ready", "accepted", "smoke", "verification", "qa", "test passed") {
			return true
		}
	}
	for _, doc := range internalHeartbeatAllDocSummaries(packet) {
		text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title}, " "))
		if !internalHeartbeatTextMatchesTaskOrProject(text, task) {
			continue
		}
		if containsAny(text, "patch_queue", "patch queue", "review", "review_ready", "accepted", "smoke", "verification", "qa", "test") {
			return true
		}
	}
	return false
}

func internalHeartbeatTextMatchesTaskOrProject(text string, task InternalHeartbeatTaskSummary) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	if taskID := strings.ToLower(strings.TrimSpace(task.TaskID)); taskID != "" && strings.Contains(text, taskID) {
		return true
	}
	if projectID := strings.ToLower(strings.TrimSpace(task.ProjectID)); projectID != "" && strings.Contains(text, projectID) {
		return true
	}
	return strings.TrimSpace(task.TaskID) == "" && strings.TrimSpace(task.ProjectID) == ""
}

func internalHeartbeatAllRecentUpdates(packet InternalHeartbeatContextPacket) []InternalHeartbeatRecentUpdateSummary {
	out := []InternalHeartbeatRecentUpdateSummary{}
	seen := map[string]bool{}
	for _, payload := range packet.SelectorPayloads {
		for _, update := range payload.RecentUpdates {
			key := strings.Join([]string{update.AgentID, update.UpdateType, update.CreatedAt, update.Summary}, "\x00")
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, update)
		}
	}
	return out
}

func internalHeartbeatAllDocSummaries(packet InternalHeartbeatContextPacket) []InternalHeartbeatDocSummary {
	out := []InternalHeartbeatDocSummary{}
	seen := map[string]bool{}
	for _, payload := range packet.SelectorPayloads {
		for _, doc := range payload.Docs {
			key := strings.TrimSpace(doc.DocKey)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, doc)
		}
	}
	return out
}

func internalHeartbeatParseTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func internalHeartbeatEvidenceText(value string, limit int) string {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return strings.TrimSpace(value[:limit])
}

func internalHeartbeatClampScore(score int) int {
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

func internalHeartbeatVisualProbeLocalResult(packet InternalHeartbeatContextPacket) (InternalHeartbeatLocalResult, bool) {
	if !strings.EqualFold(strings.TrimSpace(packet.HeartbeatID), "visual_product_audit") && !strings.EqualFold(strings.TrimSpace(packet.HeartbeatKind), "browser_critic") {
		return InternalHeartbeatLocalResult{}, false
	}
	findings := make([]InternalHeartbeatFinding, 0, 2)
	for _, payload := range packet.SelectorPayloads {
		if payload.Selector != "runnable_surface" {
			continue
		}
		projectID := internalHeartbeatVisualProbeProjectID(packet, payload)
		switch payload.Status {
		case "missing_surface":
			findings = append(findings, InternalHeartbeatFinding{
				DedupKey:     "visual:runnable-surface-missing",
				Kind:         "visual_surface_gap",
				Source:       internalHeartbeatVisualSensorSource,
				ProjectID:    projectID,
				Title:        "Runnable UI surface is missing",
				Summary:      "The UI critic heartbeat could not find a runnable URL, so it cannot truthfully inspect the product.",
				Score:        78,
				EvidenceRefs: []string{"selector:runnable_surface", "status:missing_surface"},
				Promote:      false,
				Reason:       "missing runnable surface blocks real visual audit",
			})
		case "surface_preflight_unverified", "probe_skipped":
			probe := internalHeartbeatMostActionableProbe(payload.BrowserProbes)
			summary := "The UI critic heartbeat found candidate runnable surfaces, but could not verify that any loaded the intended product."
			evidence := []string{"selector:runnable_surface", "status:" + payload.Status}
			if probe.URL != "" {
				summary = fmt.Sprintf("%s Candidate %s ended as %s.", summary, probe.URL, firstNonEmpty(probe.Status, "unverified"))
				evidence = append(evidence, "url:"+probe.URL, "browser_probe:"+firstNonEmpty(probe.Status, "unverified"))
			}
			findings = append(findings, InternalHeartbeatFinding{
				DedupKey:     "visual:runnable-surface-unverified",
				Kind:         "visual_surface_gap",
				Source:       internalHeartbeatVisualSensorSource,
				ProjectID:    projectID,
				Title:        "Runnable UI surface could not be verified",
				Summary:      summary,
				Score:        82,
				EvidenceRefs: evidence,
				Promote:      false,
				Reason:       "candidate surface failed product-marker preflight",
			})
		case "surface_preflight_verified":
			if payload.VisualAudit == nil || payload.VisualAudit.ExistingEvidenceSatisfied {
				continue
			}
			if len(payload.VisualAudit.BlockingEvidence) > 0 {
				probe, _ := internalHeartbeatVerifiedBrowserProbe(payload.BrowserProbes)
				evidence := []string{
					"selector:runnable_surface",
					"status:surface_preflight_verified",
				}
				evidence = append(evidence, internalHeartbeatVisualAuditEvidenceRefs(payload.VisualAudit)...)
				if probe.URL != "" {
					evidence = append(evidence, "url:"+probe.URL, "browser_probe:verified_product_marker")
				}
				for _, blocking := range internalHeartbeatLimit(payload.VisualAudit.BlockingEvidence, 4) {
					evidence = append(evidence, "blocking:"+blocking)
				}
				findings = append(findings, InternalHeartbeatFinding{
					DedupKey:     "visual:blocking-evidence",
					Kind:         "visual_blocking_finding",
					Source:       internalHeartbeatVisualSensorSource,
					ProjectID:    projectID,
					Title:        "Visual acceptance evidence contains blocking screenshot findings",
					Summary:      "The UI critic heartbeat found durable visual evidence, but screenshot inspection reported blocking visual failures that must not be treated as a pass.",
					Score:        90,
					EvidenceRefs: evidence,
					Promote:      packet.AllowTaskSubmit,
					Reason:       "visual artifact verification produced blocking findings",
				})
				break
			}
			evidence := []string{
				"selector:runnable_surface",
				"status:surface_preflight_verified",
			}
			evidence = append(evidence, internalHeartbeatVisualAuditEvidenceRefs(payload.VisualAudit)...)
			probe, _ := internalHeartbeatVerifiedBrowserProbe(payload.BrowserProbes)
			if probe.URL != "" {
				evidence = append(evidence, "url:"+probe.URL, "browser_probe:verified_product_marker")
			}
			for _, missing := range internalHeartbeatLimit(payload.VisualAudit.MissingEvidence, 3) {
				evidence = append(evidence, "missing:"+missing)
			}
			findings = append(findings, InternalHeartbeatFinding{
				DedupKey:     "visual:visual-evidence-required",
				Kind:         "visual_acceptance_gap",
				Source:       internalHeartbeatVisualSensorSource,
				ProjectID:    projectID,
				Title:        "Verified UI surface still needs visual acceptance evidence",
				Summary:      "The UI critic heartbeat verified the product surface, but no complete durable visual packet exists for desktop/narrow viewports and initial/primary/result states.",
				Score:        74,
				EvidenceRefs: evidence,
				Promote:      packet.AllowTaskSubmit,
				Reason:       "product-marker preflight is not screenshot or visual acceptance evidence",
			})
		}
		if len(findings) >= 2 {
			break
		}
	}
	if len(findings) == 0 {
		return InternalHeartbeatLocalResult{}, false
	}
	return normalizeInternalHeartbeatLocalResult(InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         "Typed visual browser preflight recorded a local UI surface finding before any public promotion.",
		BacklogItems:    findings,
	}), true
}

func internalHeartbeatVisualProbeProjectID(packet InternalHeartbeatContextPacket, payload InternalHeartbeatSelectorPacket) string {
	return firstNonEmpty(
		strings.TrimSpace(payload.TrustedScope.ProjectID),
		strings.TrimSpace(packet.TrustedScope.ProjectID),
	)
}

func internalHeartbeatMostActionableProbe(probes []InternalHeartbeatBrowserProbe) InternalHeartbeatBrowserProbe {
	if len(probes) == 0 {
		return InternalHeartbeatBrowserProbe{}
	}
	for _, status := range []string{"loaded_unverified", "http_error", "unreachable", "skipped", "failed"} {
		for _, probe := range probes {
			if probe.Status == status {
				return probe
			}
		}
	}
	return probes[0]
}

func internalHeartbeatVisualAuditEvidenceRefs(plan *InternalHeartbeatVisualAuditPlan) []string {
	if plan == nil {
		return nil
	}
	refs := []string{}
	for _, viewport := range plan.Viewports {
		if viewport.ID != "" {
			refs = append(refs, "viewport:"+viewport.ID)
		}
	}
	for _, scenario := range plan.Scenarios {
		if scenario.ID != "" {
			refs = append(refs, "scenario:"+scenario.ID)
		}
	}
	if plan.EvidenceRequired {
		refs = append(refs, "evidence_required")
		for _, request := range plan.EvidenceRequests {
			if request.RequestID != "" {
				refs = append(refs, "evidence_request:"+request.RequestID)
			}
			if request.Kind != "" && request.DimensionID != "" && request.StateID != "" {
				refs = append(refs, "evidence:"+request.Kind+":"+request.DimensionID+":"+request.StateID)
			}
		}
	}
	return uniqueTrimmedCSVStrings(refs)
}

func mergeInternalHeartbeatLocalResults(primary, sensor InternalHeartbeatLocalResult) InternalHeartbeatLocalResult {
	primary = normalizeInternalHeartbeatLocalResult(primary)
	sensor = normalizeInternalHeartbeatLocalResult(sensor)
	if len(sensor.BacklogItems) == 0 && len(sensor.ActionRequests) == 0 && len(sensor.ActiveMemory) == 0 && len(sensor.WillDirectives) == 0 {
		return primary
	}
	existing := map[string]bool{}
	mergedItems := make([]InternalHeartbeatFinding, 0, internalHeartbeatMaxBacklogWrites)
	for _, item := range sensor.BacklogItems {
		key := strings.TrimSpace(item.DedupKey)
		if key == "" || existing[key] {
			continue
		}
		existing[key] = true
		mergedItems = append(mergedItems, item)
		if len(mergedItems) >= internalHeartbeatMaxBacklogWrites {
			break
		}
	}
	for _, item := range primary.BacklogItems {
		key := strings.TrimSpace(item.DedupKey)
		if key == "" || existing[key] {
			continue
		}
		existing[key] = true
		mergedItems = append(mergedItems, item)
		if len(mergedItems) >= internalHeartbeatMaxBacklogWrites {
			break
		}
	}
	primary.BacklogItems = mergedItems
	primary.ActionRequests = mergeInternalHeartbeatActionRequests(primary.ActionRequests, sensor.ActionRequests)
	primary.ActiveMemory = mergeInternalHeartbeatActiveMemoryNotes(primary.ActiveMemory, sensor.ActiveMemory)
	primary.WillDirectives = mergeInternalHeartbeatWillDirectives(primary.WillDirectives, sensor.WillDirectives)
	if len(primary.BacklogItems) > 0 || len(primary.ActionRequests) > 0 {
		primary.Outcome = "backlog_recorded"
		primary.Summary = firstNonEmpty(primary.Summary, sensor.Summary)
	} else if len(primary.WillDirectives) > 0 {
		primary.Outcome = "will_updated"
		primary.Summary = firstNonEmpty(primary.Summary, sensor.Summary)
	}
	return normalizeInternalHeartbeatLocalResult(primary)
}

func mergeInternalHeartbeatActiveMemoryNotes(primary, sensor []InternalHeartbeatActiveMemoryNote) []InternalHeartbeatActiveMemoryNote {
	merged := append([]InternalHeartbeatActiveMemoryNote(nil), primary...)
	merged = append(merged, sensor...)
	return normalizeInternalHeartbeatActiveMemoryNotes(merged)
}

func mergeInternalHeartbeatWillDirectives(primary, sensor []InternalHeartbeatWillDirective) []InternalHeartbeatWillDirective {
	merged := append([]InternalHeartbeatWillDirective(nil), primary...)
	merged = append(merged, sensor...)
	return normalizeInternalHeartbeatWillDirectives(merged)
}

func mergeInternalHeartbeatActionRequests(primary, sensor []InternalHeartbeatActionRequest) []InternalHeartbeatActionRequest {
	existing := map[string]bool{}
	out := make([]InternalHeartbeatActionRequest, 0, internalHeartbeatMaxBacklogWrites)
	appendRequest := func(request InternalHeartbeatActionRequest) {
		if len(out) >= internalHeartbeatMaxBacklogWrites {
			return
		}
		request = normalizeInternalHeartbeatActionRequest(request)
		key := firstNonEmpty(request.RequestID, request.Capability+"\x00"+request.ToolSuite+"\x00"+request.Title+"\x00"+request.Summary)
		if strings.TrimSpace(key) == "" || existing[key] {
			return
		}
		existing[key] = true
		out = append(out, request)
	}
	for _, request := range sensor {
		appendRequest(request)
	}
	for _, request := range primary {
		appendRequest(request)
	}
	return out
}

func parseInternalHeartbeatLocalResult(raw string) (InternalHeartbeatLocalResult, error) {
	var result InternalHeartbeatLocalResult
	jsonBody, ok := extractInternalHeartbeatSingleJSONObject(raw)
	if !ok {
		jsonBody = strings.TrimSpace(raw)
	}
	decoder := json.NewDecoder(strings.NewReader(jsonBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return InternalHeartbeatLocalResult{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return InternalHeartbeatLocalResult{}, fmt.Errorf("internal heartbeat result must contain exactly one JSON object")
	}
	explicitVersion := strings.TrimSpace(result.ContractVersion)
	if explicitVersion != "" && explicitVersion != internalHeartbeatLocalResultContractVersion {
		return InternalHeartbeatLocalResult{}, fmt.Errorf("unsupported internal heartbeat result contract_version %q", explicitVersion)
	}
	return normalizeInternalHeartbeatLocalResult(result), nil
}

func decodeStrictJSONObject(data []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(strings.TrimSpace(string(data))))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("internal heartbeat nested result must contain exactly one JSON object")
	}
	return nil
}

func parseInternalHeartbeatScore(raw json.RawMessage) (int, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	text := strings.TrimSpace(string(raw))
	if text == "" || strings.EqualFold(text, "null") {
		return 0, nil
	}
	if strings.HasPrefix(text, `"`) {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, err
		}
		text = strings.TrimSpace(strings.TrimSuffix(s, "%"))
		if text == "" {
			return 0, nil
		}
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, fmt.Errorf("internal heartbeat score must be numeric: %w", err)
	}
	if value > 0 && value <= 1 {
		value *= 100
	}
	return int(math.Round(value)), nil
}

func extractInternalHeartbeatSingleJSONObject(raw string) (string, bool) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", false
	}
	start := strings.IndexByte(text, '{')
	if start < 0 {
		return "", false
	}
	inString := false
	escaped := false
	depth := 0
	for i := start; i < len(text); i++ {
		ch := text[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return text[start : i+1], true
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	return "", false
}

func normalizeInternalHeartbeatLocalResult(result InternalHeartbeatLocalResult) InternalHeartbeatLocalResult {
	result.ContractVersion = firstNonEmpty(strings.TrimSpace(result.ContractVersion), internalHeartbeatLocalResultContractVersion)
	result.Outcome = normalizeInternalHeartbeatLocalOutcome(result.Outcome)
	if result.Outcome == "" {
		result.Outcome = "no_action"
	}
	result.Summary = strings.TrimSpace(result.Summary)
	result.ActiveMemory = normalizeInternalHeartbeatActiveMemoryNotes(result.ActiveMemory)
	result.WillDirectives = normalizeInternalHeartbeatWillDirectives(result.WillDirectives)
	if len(result.BacklogItems) == 0 {
		result.ActionRequests = normalizeInternalHeartbeatActionRequests(result.ActionRequests)
		return result
	}
	out := make([]InternalHeartbeatFinding, 0, len(result.BacklogItems))
	for _, finding := range result.BacklogItems {
		finding = normalizeInternalHeartbeatFinding(finding)
		if strings.TrimSpace(firstNonEmpty(finding.Title, finding.Summary)) == "" {
			continue
		}
		out = append(out, finding)
		if len(out) >= internalHeartbeatMaxBacklogWrites {
			break
		}
	}
	result.BacklogItems = out
	result.ActionRequests = normalizeInternalHeartbeatActionRequests(result.ActionRequests)
	return result
}

func normalizeInternalHeartbeatLocalOutcome(outcome string) string {
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case "", "none", "noop", "no-op", "no_action":
		return "no_action"
	case "backlog_recorded", "recorded", "finding_recorded", "continue", "completed":
		return "backlog_recorded"
	case "will_updated", "will_directive", "will_directives", "replan", "steered":
		return "will_updated"
	case "blocked":
		return "blocked"
	case "failed", "error":
		return "failed"
	default:
		return "failed"
	}
}

func normalizeInternalHeartbeatFinding(finding InternalHeartbeatFinding) InternalHeartbeatFinding {
	finding.DedupKey = strings.ToLower(strings.TrimSpace(finding.DedupKey))
	finding.Kind = strings.TrimSpace(finding.Kind)
	finding.Source = strings.TrimSpace(finding.Source)
	finding.ProjectID = strings.TrimSpace(finding.ProjectID)
	finding.ProjectLane = strings.TrimSpace(finding.ProjectLane)
	finding.Title = strings.TrimSpace(finding.Title)
	finding.Summary = strings.TrimSpace(finding.Summary)
	if finding.Score < 0 {
		finding.Score = 0
	}
	if finding.Score > 100 {
		finding.Score = 100
	}
	if finding.Score == 0 {
		finding.Score = 50
	}
	finding.EvidenceRefs = uniqueTrimmedCSVStrings(finding.EvidenceRefs)
	finding.Reason = strings.TrimSpace(finding.Reason)
	if len(finding.Meta) > 0 {
		meta := map[string]string{}
		for key, value := range finding.Meta {
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			if key == "" || value == "" {
				continue
			}
			meta[key] = value
		}
		if len(meta) > 0 {
			finding.Meta = meta
		} else {
			finding.Meta = nil
		}
	}
	return finding
}

func normalizeInternalHeartbeatActionRequests(requests []InternalHeartbeatActionRequest) []InternalHeartbeatActionRequest {
	if len(requests) == 0 {
		return nil
	}
	out := make([]InternalHeartbeatActionRequest, 0, len(requests))
	for _, request := range requests {
		request = normalizeInternalHeartbeatActionRequest(request)
		if strings.TrimSpace(firstNonEmpty(request.Title, request.Summary, request.Capability)) == "" {
			continue
		}
		out = append(out, request)
		if len(out) >= internalHeartbeatMaxBacklogWrites {
			break
		}
	}
	return out
}

func normalizeInternalHeartbeatActionRequest(request InternalHeartbeatActionRequest) InternalHeartbeatActionRequest {
	request.RequestID = sanitizeRefSegment(strings.ToLower(strings.TrimSpace(request.RequestID)))
	request.Capability = sanitizeRefSegment(strings.ToLower(strings.TrimSpace(request.Capability)))
	request.ToolSuite = sanitizeRefSegment(strings.ToLower(strings.TrimSpace(request.ToolSuite)))
	request.ProjectID = strings.TrimSpace(request.ProjectID)
	request.ProjectLane = strings.TrimSpace(request.ProjectLane)
	request.Title = strings.TrimSpace(request.Title)
	request.Summary = strings.TrimSpace(request.Summary)
	if request.Score < 0 {
		request.Score = 0
	}
	if request.Score > 100 {
		request.Score = 100
	}
	if request.Score == 0 {
		request.Score = 60
	}
	request.EvidenceRefs = uniqueTrimmedCSVStrings(request.EvidenceRefs)
	request.Reason = strings.TrimSpace(request.Reason)
	return request
}

func normalizeInternalHeartbeatActiveMemoryNotes(notes []InternalHeartbeatActiveMemoryNote) []InternalHeartbeatActiveMemoryNote {
	if len(notes) == 0 {
		return nil
	}
	out := make([]InternalHeartbeatActiveMemoryNote, 0, len(notes))
	for _, note := range notes {
		note = normalizeInternalHeartbeatActiveMemoryNote(note)
		if strings.TrimSpace(note.Note) == "" {
			continue
		}
		out = append(out, note)
		if len(out) >= internalHeartbeatMaxBacklogWrites {
			break
		}
	}
	return out
}

func normalizeInternalHeartbeatActiveMemoryNote(note InternalHeartbeatActiveMemoryNote) InternalHeartbeatActiveMemoryNote {
	note.Lane = sanitizeRefSegment(strings.TrimSpace(note.Lane))
	note.Note = strings.TrimSpace(note.Note)
	if len(note.Note) > 500 {
		note.Note = strings.TrimSpace(note.Note[:500])
	}
	note.EvidenceRefs = uniqueTrimmedCSVStrings(note.EvidenceRefs)
	note.Tags = uniqueTrimmedCSVStrings(note.Tags)
	return note
}

func normalizeInternalHeartbeatWillDirectives(directives []InternalHeartbeatWillDirective) []InternalHeartbeatWillDirective {
	if len(directives) == 0 {
		return nil
	}
	out := make([]InternalHeartbeatWillDirective, 0, len(directives))
	seen := map[string]bool{}
	for _, directive := range directives {
		directive = normalizeInternalHeartbeatWillDirective(directive)
		if strings.TrimSpace(directive.Action) == "" || strings.TrimSpace(firstNonEmpty(directive.Summary, directive.Reason)) == "" {
			continue
		}
		key := firstNonEmpty(directive.DirectiveID, directive.Action+"\x00"+directive.TaskID+"\x00"+directive.Summary)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, directive)
		if len(out) >= internalHeartbeatMaxBacklogWrites {
			break
		}
	}
	return out
}

func normalizeInternalHeartbeatWillDirective(directive InternalHeartbeatWillDirective) InternalHeartbeatWillDirective {
	directive.DirectiveID = sanitizeRefSegment(strings.ToLower(strings.TrimSpace(directive.DirectiveID)))
	directive.Action = normalizeAgentHeartbeatWillAction(directive.Action)
	directive.TaskID = strings.TrimSpace(directive.TaskID)
	directive.SessionID = strings.TrimSpace(directive.SessionID)
	directive.Summary = strings.TrimSpace(directive.Summary)
	if len(directive.Summary) > 500 {
		directive.Summary = strings.TrimSpace(directive.Summary[:500])
	}
	directive.Reason = strings.TrimSpace(directive.Reason)
	if len(directive.Reason) > 500 {
		directive.Reason = strings.TrimSpace(directive.Reason[:500])
	}
	directive.Priority = strings.ToUpper(strings.TrimSpace(directive.Priority))
	directive.EvidenceRefs = uniqueTrimmedCSVStrings(directive.EvidenceRefs)
	return directive
}

func (r *Runtime) executeInternalHeartbeatTool(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
	if r != nil && r.client != nil && strings.TrimSpace(r.cfg.WorkspaceID) != "" && strings.TrimSpace(r.cfg.AgentID) != "" {
		return r.policyAwareToolExecutor(ctx, registry, call)
	}
	return defaultToolLoopExecutor(ctx, registry, call)
}

func internalHeartbeatCallWithDeterministicTaskID(policy InternalHeartbeatExecutionPolicy, call ToolCall) ToolCall {
	if !strings.EqualFold(strings.TrimSpace(call.Function.Name), "task_submit") {
		return call
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil || args == nil {
		return call
	}
	if strings.TrimSpace(stringArg(args, "task_id")) != "" {
		return call
	}
	scope := firstNonEmpty(
		strings.TrimSpace(stringArg(args, "project_id")),
		strings.TrimSpace(stringArg(args, "scope_id")),
		"workspace",
	)
	lane := firstNonEmpty(strings.TrimSpace(stringArg(args, "project_lane")), "follow-up")
	kind := firstNonEmpty(strings.TrimSpace(stringArg(args, "task_kind")), "EXECUTION")
	seed := strings.Join([]string{
		strings.TrimSpace(policy.HeartbeatID),
		strings.TrimSpace(policy.Kind),
		scope,
		lane,
		kind,
	}, "|")
	args["task_id"] = sanitizeRefSegment("task-internal-heartbeat-" + firstNonEmpty(policy.HeartbeatID, "heartbeat") + "-" + scope + "-" + shortHash(seed))
	raw, err := json.Marshal(args)
	if err != nil {
		return call
	}
	call.Function.Arguments = string(raw)
	return call
}

func (r *Runtime) executeDueInternalHeartbeatsLocal(ctx context.Context, now time.Time) ([]InternalHeartbeatExecutionResult, error) {
	return r.executeDueInternalHeartbeats(ctx, now, internalHeartbeatLeaseModeLocalOnly)
}

func (r *Runtime) executeDueInternalHeartbeatsLeased(ctx context.Context, now time.Time) ([]InternalHeartbeatExecutionResult, error) {
	return r.executeDueInternalHeartbeats(ctx, now, internalHeartbeatLeaseModeProbeRemote)
}

func (r *Runtime) executeDueInternalHeartbeatsCoordinated(ctx context.Context, now time.Time) ([]InternalHeartbeatExecutionResult, error) {
	return r.executeDueInternalHeartbeats(ctx, now, internalHeartbeatLeaseModeConfirmedRemote)
}

type internalHeartbeatLeaseMode int

const (
	internalHeartbeatLeaseModeLocalOnly internalHeartbeatLeaseMode = iota
	internalHeartbeatLeaseModeProbeRemote
	internalHeartbeatLeaseModeConfirmedRemote
)

func (r *Runtime) executeDueInternalHeartbeats(ctx context.Context, now time.Time, leaseMode internalHeartbeatLeaseMode) ([]InternalHeartbeatExecutionResult, error) {
	if r == nil {
		return nil, nil
	}
	if r.campaignBuildBreakActive() {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	anatomy := runtimeAnatomyConfig(r.cfg)
	r.mu.Lock()
	state := r.internalHeartbeatState
	state.LastRun = copyHeartbeatLastRun(state.LastRun)
	state.Running = copyHeartbeatLockSet(state.Running)
	state.HeldLocks = copyHeartbeatLockSet(state.HeldLocks)
	activeTask := r.activeTask != nil || strings.TrimSpace(r.scratch.ActiveTaskID) != ""
	paused := r.scratch.ControlPaused
	r.mu.Unlock()

	items := internalHeartbeatDueItemsWithState(anatomy, now, state, activeTask, paused)
	results := make([]InternalHeartbeatExecutionResult, 0)
	var firstErr error
	for _, item := range items {
		if !item.Due {
			continue
		}
		spec, ok := internalHeartbeatSpecByID(anatomy, item.ID)
		if !ok {
			continue
		}
		policy := internalHeartbeatExecutionPolicy(spec)
		if policy.RequiresTaskLoop {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(spec.ID), "action_request_promoter") && !r.actionRequestPromoterHasCandidate(now) {
			continue
		}
		if leaseMode == internalHeartbeatLeaseModeConfirmedRemote && !policy.LocalOnly {
			checked, _, probeErr := r.probeInternalHeartbeatRemoteLeaseSupport(ctx)
			if probeErr != nil && !checked && r.internalHeartbeatRemoteLeaseAvailable() && r.cfg.Mode == RuntimeModeDaemon {
				err := fmt.Errorf("internal_heartbeat %s durable lease support probe failed: %w", strings.TrimSpace(spec.ID), probeErr)
				r.recordLoopFailure("internal_heartbeat", err, now)
				result := r.recordInternalHeartbeatFailureCooldown(anatomy, spec, item.Reason, now, err)
				results = append(results, result)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		useRemoteLeaseForSpec := r.shouldUseRemoteLeaseForInternalHeartbeat(policy, leaseMode)
		lease, acquired := r.tryAcquireInternalHeartbeatRunLease(ctx, anatomy, spec, useRemoteLeaseForSpec)
		if !acquired {
			continue
		}
		baseCycleCtx, cancelBaseCycle := r.backgroundCycleContext(ctx)
		cycleCtx, cancelCycleCause := context.WithCancelCause(baseCycleCtx)
		r.recordLoopStarted("internal_heartbeat", now)
		stopLeaseRefresh := r.startInternalHeartbeatLeaseRefresh(lease, cancelCycleCause)
		result, err := r.recordTypedInternalHeartbeatLocalSession(cycleCtx, anatomy, spec, policy, item.Reason, now)
		cycleCause := context.Cause(cycleCtx)
		stopLeaseRefresh()
		timedOut := errors.Is(cycleCause, context.DeadlineExceeded) || errors.Is(baseCycleCtx.Err(), context.DeadlineExceeded)
		leaseLost := errors.Is(cycleCause, errInternalHeartbeatLeaseLost)
		if leaseLost && err == nil {
			err = errInternalHeartbeatLeaseLost
		}
		if cycleCause != nil && !errors.Is(cycleCause, context.Canceled) && !errors.Is(cycleCause, context.DeadlineExceeded) && !errors.Is(cycleCause, errInternalHeartbeatLeaseLost) && err == nil {
			err = cycleCause
		}
		cancelCycleCause(nil)
		cancelBaseCycle()
		r.releaseInternalHeartbeatRunLease(ctx, lease)
		if err != nil {
			if timedOut {
				err = fmt.Errorf("internal_heartbeat %s cycle timed out after %s: %w", strings.TrimSpace(spec.ID), runtimePlannerCycleTimeout(r.cfg), err)
			} else if leaseLost {
				err = fmt.Errorf("internal_heartbeat %s durable lease lost: %w", strings.TrimSpace(spec.ID), err)
			}
			result = r.recordInternalHeartbeatFailureCooldown(anatomy, spec, item.Reason, now, err)
			if firstErr == nil {
				firstErr = err
			}
		}
		if timedOut {
			timeoutErr := fmt.Errorf("internal_heartbeat %s cycle timed out after %s", strings.TrimSpace(spec.ID), runtimePlannerCycleTimeout(r.cfg))
			r.recordBackgroundCycleTimeoutSignal(ctx, "internal_heartbeat", "SYSTEM LIVENESS: internal_heartbeat_timeout heartbeat_id="+strings.TrimSpace(spec.ID), timeoutErr, now)
			if result.Status == "" || result.Status == "completed" {
				result.Status = "failed"
				result.Outcome = firstNonEmpty(result.Outcome, "cycle_timeout")
				result.Summary = firstNonEmpty(result.Summary, timeoutErr.Error())
			}
			if firstErr == nil {
				firstErr = timeoutErr
			}
		} else if strings.EqualFold(strings.TrimSpace(result.Status), "failed") {
			r.recordLoopFailure("internal_heartbeat", fmt.Errorf("internal heartbeat %s failed: %s", strings.TrimSpace(spec.ID), firstNonEmpty(result.Outcome, result.Summary)), now)
		} else {
			r.recordLoopSuccess("internal_heartbeat", now)
		}
		results = append(results, result)
	}
	return results, firstErr
}

func (r *Runtime) recordInternalHeartbeatFailureCooldown(anatomy AgentAnatomyConfig, spec AgentHeartbeatSpec, trigger string, now time.Time, failure error) InternalHeartbeatExecutionResult {
	if r == nil {
		return InternalHeartbeatExecutionResult{}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store := r.internalSessions
	if store == nil {
		var err error
		store, err = OpenAgentInternalSessionStore(r.cfg.WorkspaceID, r.cfg.AgentID)
		if err != nil {
			store = nil
		}
	}
	session := AgentInternalSessionRecord{}
	if store != nil {
		started := now
		if timeout := runtimePlannerCycleTimeout(r.cfg); timeout > 0 {
			started = now.Add(-timeout)
		}
		if created, err := store.BeginHeartbeatSession(spec, AgentAnatomyDigest(anatomy), firstNonEmpty(trigger, "typed_internal_heartbeat_failure"), started); err == nil {
			_ = store.CompleteSession(created.SessionID, "failed", "scheduler_failure", "Internal heartbeat failed before completing a local session.", nil, failure, now)
			if completed, ok := internalSessionRecordByID(store.Snapshot(), created.SessionID); ok {
				session = completed
			} else {
				session = created
			}
		}
	}
	// CA-30: a lost durable lease means a peer won the run, not that this node's
	// cadence window was satisfied. Advancing LastRun a full cadence would make the
	// lease loser self-cool as if it had run. Skip the advance on lease loss so the
	// heartbeat is due again promptly once the lease is reacquired; the per-item run
	// lease acquisition (which fails cheaply while the peer holds it) provides the
	// backoff and prevents a duplicate run.
	leaseLost := errors.Is(failure, errInternalHeartbeatLeaseLost)
	r.mu.Lock()
	if r.internalSessions == nil && store != nil {
		r.internalSessions = store
	}
	if r.internalHeartbeatState.LastRun == nil {
		r.internalHeartbeatState.LastRun = map[string]time.Time{}
	}
	if !leaseLost {
		r.internalHeartbeatState.LastRun[spec.ID] = now.UTC()
	}
	r.mu.Unlock()
	return InternalHeartbeatExecutionResult{
		SessionID:        strings.TrimSpace(session.SessionID),
		HeartbeatID:      strings.TrimSpace(spec.ID),
		Status:           "failed",
		Outcome:          "scheduler_failure",
		Summary:          firstNonEmpty(strings.TrimSpace(session.Summary), "Internal heartbeat failed before completing a local session."),
		Trigger:          firstNonEmpty(trigger, "typed_internal_heartbeat_failure"),
		PromotionBlocked: true,
		ToolSuites:       append([]string(nil), internalHeartbeatExecutionPolicy(spec).ToolSuites...),
		ContextSelectors: append([]string(nil), internalHeartbeatExecutionPolicy(spec).ContextSelectors...),
		OutputContracts:  append([]string(nil), internalHeartbeatExecutionPolicy(spec).OutputContracts...),
	}
}

func (r *Runtime) actionRequestPromoterHasCandidate(now time.Time) bool {
	if r == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store := r.internalSessions
	if store == nil {
		var err error
		store, err = OpenAgentInternalSessionStore(r.cfg.WorkspaceID, r.cfg.AgentID)
		if err != nil {
			return false
		}
		r.mu.Lock()
		if r.internalSessions == nil {
			r.internalSessions = store
		}
		r.mu.Unlock()
	}
	_, ok := internalHeartbeatActionRequestPromotionFinding(InternalHeartbeatContextPacket{
		HeartbeatID:       "action_request_promoter",
		Now:               now.Format(time.RFC3339Nano),
		BacklogCandidates: internalHeartbeatBacklogSummaries(store.ListBacklogPromotionCandidates(8, 0, now)),
	})
	return ok
}

func recordInternalHeartbeatActiveMemoryNotes(store *AgentInternalSessionStore, session AgentInternalSessionRecord, spec AgentHeartbeatSpec, result InternalHeartbeatLocalResult) (AgentInternalSessionRecord, error) {
	if store == nil || len(result.ActiveMemory) == 0 {
		return session, nil
	}
	notes := normalizeInternalHeartbeatActiveMemoryNotes(result.ActiveMemory)
	if len(notes) == 0 {
		return session, nil
	}
	if session.Meta == nil {
		session.Meta = map[string]string{}
	}
	policy := internalHeartbeatActiveMemoryPolicySummary(spec)
	session.Meta["active_memory_lane"] = firstNonEmpty(policy.Lane, spec.ID)
	session.Meta["active_memory_note_count"] = fmt.Sprint(len(notes))
	for idx, note := range notes {
		note.Lane = firstNonEmpty(policy.Lane, spec.ID)
		raw, err := json.Marshal(note)
		if err != nil {
			return session, err
		}
		session.Meta[fmt.Sprintf("active_memory_note_%d", idx+1)] = string(raw)
	}
	return store.RecordSession(session)
}

func (r *Runtime) applyInternalHeartbeatWillDirectives(ctx context.Context, session AgentInternalSessionRecord, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, result InternalHeartbeatLocalResult, now time.Time) ([]string, error) {
	if r == nil || !policy.AllowWillDirectives {
		return nil, nil
	}
	directives := normalizeInternalHeartbeatWillDirectives(result.WillDirectives)
	if len(directives) == 0 {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	allowed := map[string]bool{}
	for _, action := range policy.WillActions {
		if normalized := normalizeAgentHeartbeatWillAction(action); normalized != "" {
			allowed[normalized] = true
		}
	}
	if len(allowed) == 0 {
		allowed["advisory_signal"] = true
	}
	limit := policy.MaxWillDirectives
	if limit <= 0 {
		limit = 1
	}
	applied := make([]string, 0, limit)
	for _, directive := range directives {
		if len(applied) >= limit {
			break
		}
		action := normalizeAgentHeartbeatWillAction(directive.Action)
		if action == "" || !allowed[action] {
			continue
		}
		if policy.WillRequiresEvidence && action != "advisory_signal" && len(directive.EvidenceRefs) == 0 {
			continue
		}
		ref, err := r.applyInternalHeartbeatWillDirective(ctx, session, spec, policy, directive, action, now)
		if err != nil {
			return applied, err
		}
		if strings.TrimSpace(ref) != "" {
			applied = append(applied, ref)
		}
	}
	return applied, nil
}

func (r *Runtime) applyInternalHeartbeatWillDirective(ctx context.Context, session AgentInternalSessionRecord, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, directive InternalHeartbeatWillDirective, action string, now time.Time) (string, error) {
	summary := internalHeartbeatWillDirectiveSummary(spec, directive)
	ref := "will:" + strings.TrimSpace(session.SessionID) + ":" + action
	// CA-31: all will-directive notices route through appendInternalHeartbeatWillSignal,
	// which sends reflection-class heartbeats (metacognition/system_sensing/
	// strategy_synthesis) to the Layer B reflection channel and only falls back to the
	// 5-slot watchdog advisory ring for non-reflection heartbeats. This keeps reflective
	// course-corrections from evicting genuine system interventions. The actual
	// preemption (setPendingWorkTrigger) is unchanged.
	switch action {
	case "advisory_signal":
		return ref, r.appendInternalHeartbeatWillSignal(ctx, spec, directive, summary)
	case "request_resume":
		taskID, sessionID := r.internalHeartbeatWillTargetIDs(directive)
		if taskID == "" && sessionID == "" {
			return r.applyInternalHeartbeatTargetlessWillAdvisory(ctx, spec, directive, summary, action)
		}
		if err := r.setPendingWorkTrigger(ctx, "request_resume", taskID, sessionID); err != nil {
			return "", err
		}
		if err := r.appendInternalHeartbeatWillSignal(ctx, spec, directive, summary); err != nil {
			return "", err
		}
		return ref, nil
	case "runtime_switch_task":
		taskID := strings.TrimSpace(directive.TaskID)
		if taskID == "" {
			return "", r.appendInternalHeartbeatWillSignal(ctx, spec, directive, summary+" Ignored runtime_switch_task because no existing target task_id was provided.")
		}
		if err := r.setPendingWorkTrigger(ctx, "runtime_switch_task", taskID, strings.TrimSpace(directive.SessionID)); err != nil {
			return "", err
		}
		if err := r.appendInternalHeartbeatWillSignal(ctx, spec, directive, summary); err != nil {
			return "", err
		}
		return ref, nil
	case "replan_active_work":
		taskID, sessionID := r.internalHeartbeatWillTargetIDs(directive)
		if taskID == "" && sessionID == "" {
			return r.applyInternalHeartbeatTargetlessWillAdvisory(ctx, spec, directive, summary, action)
		}
		if err := r.setPendingWorkTrigger(ctx, "request_resume", taskID, sessionID); err != nil {
			return "", err
		}
		if err := r.appendInternalHeartbeatWillSignal(ctx, spec, directive, summary); err != nil {
			return "", err
		}
		return ref, nil
	case "publish_rhizome_update":
		if policy.LocalOnly || policy.WillPublishVisibility != "rhizome" {
			return "", r.appendInternalHeartbeatWillSignal(ctx, spec, directive, summary+" Public Rhizome update was blocked by this heartbeat's will_policy visibility.")
		}
		if err := r.publishInternalHeartbeatWillUpdate(ctx, session, spec, directive, summary, now); err != nil {
			return "", err
		}
		return ref, nil
	default:
		return "", nil
	}
}

func internalHeartbeatWillDirectiveSummary(spec AgentHeartbeatSpec, directive InternalHeartbeatWillDirective) string {
	heartbeatID := firstNonEmpty(spec.ID, "internal_heartbeat")
	summary := firstNonEmpty(directive.Summary, directive.Reason, "Heartbeat requested a planner course correction.")
	reason := strings.TrimSpace(directive.Reason)
	if reason != "" && !strings.Contains(summary, reason) {
		summary += " Reason: " + reason
	}
	return "SYSTEM HEARTBEAT WILL [" + heartbeatID + "]: " + summary
}

func (r *Runtime) applyInternalHeartbeatTargetlessWillAdvisory(ctx context.Context, spec AgentHeartbeatSpec, directive InternalHeartbeatWillDirective, summary, action string) (string, error) {
	notice := summary + " Targetless " + action + " was kept advisory-only because no active task/session or explicit target was available."
	if err := r.appendInternalHeartbeatWillSignal(ctx, spec, directive, notice); err != nil {
		return "", err
	}
	return "will:advisory_only:" + action, nil
}

func (r *Runtime) internalHeartbeatWillTargetIDs(directive InternalHeartbeatWillDirective) (string, string) {
	taskID := strings.TrimSpace(directive.TaskID)
	sessionID := strings.TrimSpace(directive.SessionID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if taskID == "" {
		taskID = firstNonEmpty(r.scratch.ActiveTaskID, runtimeActiveTaskIDLocked(r))
	}
	if sessionID == "" {
		sessionID = firstNonEmpty(r.scratch.ActiveSessionID, runtimeActiveSessionIDLocked(r))
	}
	return taskID, sessionID
}

func (r *Runtime) appendInternalHeartbeatWillAdvisory(ctx context.Context, signal string) error {
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		appendRuntimeAdvisorySignal(state, signal)
	})
}

// reflectionChannelCap returns the configured Layer B reflection-channel capacity
// (0 = disabled, the legacy advisory-ring fallback).
func (r *Runtime) reflectionChannelCap() int {
	if r == nil || r.agent == nil {
		return 0
	}
	return r.agent.Anatomy.Memory.ReflectionChannelCap
}

// heartbeatKindUsesReflectionChannel reports whether a heartbeat's self-authored
// advisory output is reflection (routed to the Layer B channel) rather than a
// system intervention (routed to the watchdog advisory ring).
func heartbeatKindUsesReflectionChannel(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "metacognition", "system_sensing", "strategy_synthesis":
		return true
	case heartbeatKindGlobalProgressReview:
		return true
	default:
		return false
	}
}

// appendInternalHeartbeatWillSignal routes an advisory_signal will-directive to
// the Layer B reflection channel when the writer is a reflection-class heartbeat
// and the channel is enabled; otherwise it falls back to the advisory ring. This
// is the routing split: reflection never competes with system interventions for
// the 5 advisory slots, but cap=0 preserves exact legacy behavior.
func (r *Runtime) appendInternalHeartbeatWillSignal(ctx context.Context, spec AgentHeartbeatSpec, directive InternalHeartbeatWillDirective, summary string) error {
	cap := r.reflectionChannelCap()
	if cap > 0 && heartbeatKindUsesReflectionChannel(spec.Kind) {
		kind := classifyReflectionKind(summary)
		// A P0/P1 directive elevates the entry to risk so it survives eviction.
		switch strings.ToUpper(strings.TrimSpace(directive.Priority)) {
		case "P0", "P1":
			kind = "risk"
		}
		source := firstNonEmpty(strings.TrimSpace(spec.ID), strings.TrimSpace(spec.Kind), "heartbeat")
		return r.updateScratch(ctx, func(state *RuntimeScratchState) {
			appendReflectionSignal(state, ReflectionSignal{
				Source: source,
				Kind:   kind,
				Text:   summary,
			}, cap)
		})
	}
	return r.appendInternalHeartbeatWillAdvisory(ctx, summary)
}

func (r *Runtime) publishInternalHeartbeatWillUpdate(ctx context.Context, session AgentInternalSessionRecord, spec AgentHeartbeatSpec, directive InternalHeartbeatWillDirective, summary string, now time.Time) error {
	if r == nil || r.client == nil || strings.TrimSpace(r.client.endpoint) == "" {
		return nil
	}
	payload := map[string]any{
		"contract_version":  internalHeartbeatLocalResultContractVersion,
		"heartbeat_id":      strings.TrimSpace(spec.ID),
		"heartbeat_kind":    strings.TrimSpace(spec.Kind),
		"session_id":        strings.TrimSpace(session.SessionID),
		"action":            strings.TrimSpace(directive.Action),
		"task_id":           strings.TrimSpace(directive.TaskID),
		"target_session_id": strings.TrimSpace(directive.SessionID),
		"priority":          strings.TrimSpace(directive.Priority),
		"evidence_refs":     uniqueTrimmedCSVStrings(directive.EvidenceRefs),
		"created_at":        now.UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	postCtx := ctx
	cancel := func() {}
	if postCtx == nil {
		postCtx = context.Background()
	}
	if _, ok := postCtx.Deadline(); !ok {
		postCtx, cancel = context.WithTimeout(postCtx, 5*time.Second)
	}
	defer cancel()
	return r.client.PostUpdate(postCtx, UpdatePostInput{
		WorkspaceID:   strings.TrimSpace(r.cfg.WorkspaceID),
		AgentID:       strings.TrimSpace(r.cfg.AgentID),
		UpdateType:    "internal_heartbeat_will",
		Summary:       internalHeartbeatSafePublicText(summary, 500),
		PayloadJSON:   string(raw),
		RequiresHuman: false,
	})
}

func (r *Runtime) recordTypedInternalHeartbeatLocalSession(ctx context.Context, anatomy AgentAnatomyConfig, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, trigger string, now time.Time) (InternalHeartbeatExecutionResult, error) {
	if r == nil {
		return InternalHeartbeatExecutionResult{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store := r.internalSessions
	if store == nil {
		var err error
		store, err = OpenAgentInternalSessionStore(r.cfg.WorkspaceID, r.cfg.AgentID)
		if err != nil {
			return InternalHeartbeatExecutionResult{}, err
		}
	}
	packet := r.buildInternalHeartbeatContextPacket(store, spec, policy, trigger, now)
	r.enrichInternalHeartbeatContextPacket(ctx, &packet, spec, policy, now)
	promptHash := shortRefHash(renderInternalHeartbeatPrompt(packet))
	packetRaw, _ := json.Marshal(packet)
	packetHash := shortRefHash(string(packetRaw))
	summary := "Typed internal heartbeat recorded local policy state without public side effects."
	if policy.AllowTaskSubmit {
		summary = "Typed internal heartbeat recorded bounded public-promotion policy."
	}
	session, err := store.BeginHeartbeatSession(spec, AgentAnatomyDigest(anatomy), firstNonEmpty(trigger, "typed_internal_heartbeat"), now)
	if err != nil {
		return InternalHeartbeatExecutionResult{}, err
	}
	session.PromotionBlocked = !policy.AllowTaskSubmit
	session.Meta = map[string]string{
		"local_only":              fmt.Sprint(policy.LocalOnly),
		"allow_public_docs":       fmt.Sprint(policy.AllowPublicDocs),
		"allow_task_submit":       fmt.Sprint(policy.AllowTaskSubmit),
		"tool_suites":             strings.Join(policy.ToolSuites, ","),
		"context_selectors":       strings.Join(policy.ContextSelectors, ","),
		"output_contracts":        strings.Join(policy.OutputContracts, ","),
		"promotion_signals":       strings.Join(policy.PromotionSignals, ","),
		"max_task_submits":        fmt.Sprint(policy.MaxTaskSubmits),
		"max_tool_iterations":     fmt.Sprint(policy.MaxToolIterations),
		"allow_llm":               fmt.Sprint(policy.AllowLLM),
		"allow_agent_request":     fmt.Sprint(policy.AllowAgentRequest),
		"require_session":         fmt.Sprint(policy.RequireSession),
		"expects_local_memory":    fmt.Sprint(policy.ExpectsLocalMemory),
		"allow_will_directives":   fmt.Sprint(policy.AllowWillDirectives),
		"will_actions":            strings.Join(policy.WillActions, ","),
		"max_will_directives":     fmt.Sprint(policy.MaxWillDirectives),
		"will_publish_visibility": policy.WillPublishVisibility,
		"context_packet_hash":     packetHash,
		"prompt_contract_hash":    promptHash,
	}
	if session, err = store.RecordSession(session); err != nil {
		return InternalHeartbeatExecutionResult{}, err
	}
	status := "completed"
	outcome := "typed_policy_recorded"
	var completionErr error
	var promotedRefs []string
	sensorResult, sensorHasResult := internalHeartbeatTypedSensorLocalResult(packet)
	var llmResult InternalHeartbeatLocalResult
	var llmAttempted bool
	var llmErr error
	if policy.AllowLLM {
		llmResult, llmAttempted, llmErr = r.runInternalHeartbeatLLM(ctx, packet, policy)
	}
	if !llmAttempted && sensorHasResult {
		llmResult = sensorResult
		llmAttempted = true
	}
	if llmAttempted {
		if llmErr != nil {
			status = "failed"
			outcome = "typed_result_failed"
			summary = "Typed internal heartbeat no-tool result failed; durable failed session recorded for cooldown."
			completionErr = llmErr
		} else {
			if sensorHasResult {
				llmResult = mergeInternalHeartbeatLocalResults(llmResult, sensorResult)
			}
			if updatedSession, err := recordInternalHeartbeatActiveMemoryNotes(store, session, spec, llmResult); err != nil {
				status = "failed"
				outcome = "active_memory_write_failed"
				summary = "Typed internal heartbeat could not persist private active-memory notes."
				completionErr = err
			} else {
				session = updatedSession
			}
			if completionErr == nil {
				if refs, err := r.applyInternalHeartbeatWillDirectives(ctx, session, spec, policy, llmResult, now); err != nil {
					status = "failed"
					outcome = "will_directive_failed"
					summary = "Typed internal heartbeat could not apply a configured will directive."
					completionErr = err
				} else if len(refs) > 0 && (llmResult.Outcome == "no_action" || llmResult.Outcome == "") {
					llmResult.Outcome = "will_updated"
					llmResult.Summary = firstNonEmpty(llmResult.Summary, fmt.Sprintf("Applied %d heartbeat will directive(s).", len(refs)))
				}
			}
		}
		if completionErr == nil {
			items, err := materializeInternalHeartbeatResultToBacklog(store, session, spec, policy, llmResult, now)
			if err != nil {
				status = "failed"
				outcome = "backlog_write_failed"
				summary = "Typed internal heartbeat could not persist local personal backlog candidates."
				completionErr = err
			} else {
				outcome = llmResult.Outcome
				summary = firstNonEmpty(llmResult.Summary, summary)
				if len(items) > 0 {
					outcome = "backlog_recorded"
					summary = firstNonEmpty(llmResult.Summary, fmt.Sprintf("Recorded %d local personal backlog item(s).", len(items)))
				}
				if strings.EqualFold(strings.TrimSpace(spec.ID), "personal_backlog_arbiter") {
					if refs, err := r.recordCapabilitySessionsForArbiterRoutes(ctx, store, anatomy, session, items, now); err != nil {
						status = "failed"
						outcome = "capability_session_failed"
						summary = "Typed internal heartbeat could not record local capability session result."
						completionErr = err
					} else if len(refs) > 0 {
						summary = firstNonEmpty(llmResult.Summary, fmt.Sprintf("Recorded %d local personal backlog item(s) and %d capability session result(s).", len(items), len(refs)))
					}
				}
				contracts, refs, err := r.promoteInternalHeartbeatBacklogCandidates(ctx, store, session.SessionID, spec, policy, items, now)
				if err != nil {
					status = "failed"
					outcome = "promotion_failed"
					summary = "Typed internal heartbeat could not promote bounded personal backlog candidate."
					completionErr = err
				} else if len(contracts) > 0 {
					outcome = "backlog_promoted"
					promotedRefs = refs
					summary = firstNonEmpty(llmResult.Summary, fmt.Sprintf("Promoted %d local personal backlog item(s).", len(contracts)))
				}
			}
		}
	}
	if err := store.CompleteSession(session.SessionID, status, outcome, summary, promotedRefs, completionErr, now); err != nil {
		return InternalHeartbeatExecutionResult{}, err
	}
	if completed, ok := internalSessionRecordByID(store.Snapshot(), session.SessionID); ok {
		session = completed
	}
	r.publishInternalHeartbeatSummary(ctx, anatomy, spec, policy, session)
	r.mu.Lock()
	if r.internalSessions == nil {
		r.internalSessions = store
	}
	if r.internalHeartbeatState.LastRun == nil {
		r.internalHeartbeatState.LastRun = map[string]time.Time{}
	}
	r.internalHeartbeatState.LastRun[spec.ID] = now.UTC()
	r.mu.Unlock()
	return InternalHeartbeatExecutionResult{
		SessionID:        session.SessionID,
		HeartbeatID:      spec.ID,
		Status:           session.Status,
		Outcome:          session.Outcome,
		Summary:          session.Summary,
		Trigger:          session.Trigger,
		PromotionBlocked: session.PromotionBlocked,
		ToolSuites:       append([]string(nil), policy.ToolSuites...),
		ContextSelectors: append([]string(nil), policy.ContextSelectors...),
		OutputContracts:  append([]string(nil), policy.OutputContracts...),
		PromotedRefs:     append([]string(nil), session.PromotedRefs...),
	}, nil
}

func (r *Runtime) publishInternalHeartbeatSummary(ctx context.Context, anatomy AgentAnatomyConfig, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, session AgentInternalSessionRecord) {
	if r == nil || r.client == nil || strings.TrimSpace(r.client.endpoint) == "" {
		return
	}
	workspaceID := strings.TrimSpace(r.cfg.WorkspaceID)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	if workspaceID == "" || agentID == "" || strings.TrimSpace(session.SessionID) == "" {
		return
	}
	payload := internalHeartbeatPublicSummaryPayload(r.cfg, anatomy, spec, policy, session)
	publishedAt := internalHeartbeatPublicSummaryReferenceTime(session)
	publishReason, ok := r.shouldPublishInternalHeartbeatSummary(payload, publishedAt)
	if !ok {
		return
	}
	payload.PublishReason = publishReason
	payload.Summary = internalHeartbeatPublicSummaryDeterministicText(payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	summary := internalHeartbeatPublicSummaryText(payload)
	if summary == "" {
		return
	}
	postCtx := ctx
	cancel := func() {}
	if ctx == nil {
		postCtx = context.Background()
	}
	if _, ok := postCtx.Deadline(); !ok {
		postCtx, cancel = context.WithTimeout(postCtx, 5*time.Second)
	}
	defer cancel()
	if err := r.client.PostUpdate(postCtx, UpdatePostInput{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "internal_heartbeat_summary",
		Summary:       summary,
		PayloadJSON:   string(raw),
		RequiresHuman: false,
	}); err == nil {
		r.recordInternalHeartbeatSummaryPublished(payload, publishedAt)
	}
}

func internalHeartbeatPublicSummaryPayload(cfg RuntimeConfig, anatomy AgentAnatomyConfig, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, session AgentInternalSessionRecord) InternalHeartbeatPublicSummaryPayload {
	spec = normalizeAgentHeartbeatSpec(spec)
	promotedRefs := publicBacklogEvidenceRefs(session.PromotedRefs)
	return InternalHeartbeatPublicSummaryPayload{
		ContractVersion:       internalHeartbeatSummaryContractVersion,
		WorkspaceID:           strings.TrimSpace(cfg.WorkspaceID),
		AgentID:               strings.TrimSpace(cfg.AgentID),
		SessionID:             strings.TrimSpace(session.SessionID),
		HeartbeatID:           strings.TrimSpace(firstNonEmpty(session.HeartbeatID, spec.ID, policy.HeartbeatID)),
		HeartbeatKind:         strings.TrimSpace(firstNonEmpty(session.HeartbeatKind, spec.Kind, policy.Kind)),
		Status:                strings.TrimSpace(session.Status),
		Outcome:               strings.TrimSpace(session.Outcome),
		Summary:               "",
		Trigger:               internalHeartbeatSafePublicText(session.Trigger, 120),
		ObservabilityOnly:     true,
		LocalOnly:             policy.LocalOnly,
		AllowTaskSubmit:       policy.AllowTaskSubmit,
		ToolSuites:            uniqueTrimmedCSVStrings(policy.ToolSuites),
		ContextSelectors:      uniqueTrimmedCSVStrings(policy.ContextSelectors),
		OutputContracts:       uniqueTrimmedCSVStrings(policy.OutputContracts),
		PromotedRefs:          promotedRefs,
		PromotedRefCount:      len(promotedRefs),
		AnatomyDigest:         AgentAnatomyDigest(anatomy),
		StartedAt:             strings.TrimSpace(session.StartedAt),
		EndedAt:               strings.TrimSpace(session.EndedAt),
		PrivateMemoryRedacted: true,
	}
}

func (r *Runtime) shouldPublishInternalHeartbeatSummary(payload InternalHeartbeatPublicSummaryPayload, at time.Time) (string, bool) {
	if strings.TrimSpace(payload.WorkspaceID) == "" || strings.TrimSpace(payload.AgentID) == "" || strings.TrimSpace(payload.SessionID) == "" || strings.TrimSpace(payload.HeartbeatID) == "" {
		return "", false
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	if internalHeartbeatPublicSummaryIsSyntheticCapabilitySession(payload) {
		return "", false
	}
	if len(payload.PromotedRefs) > 0 {
		return "promoted_refs", true
	}
	if strings.EqualFold(strings.TrimSpace(payload.Status), "failed") {
		return "failed", true
	}
	if payload.LocalOnly && strings.EqualFold(strings.TrimSpace(payload.HeartbeatID), "personal_backlog_arbiter") {
		return "", false
	}
	r.mu.Lock()
	if r.internalHeartbeatState.PublicSummary == nil {
		r.internalHeartbeatState.PublicSummary = map[string]internalHeartbeatPublicSummaryState{}
	}
	previous, hasPrevious := r.internalHeartbeatState.PublicSummary[strings.TrimSpace(payload.HeartbeatID)]
	r.mu.Unlock()
	if !hasPrevious || previous.PublishedAt.IsZero() {
		return "first_run", true
	}
	if !strings.EqualFold(strings.TrimSpace(previous.Status), strings.TrimSpace(payload.Status)) ||
		!strings.EqualFold(strings.TrimSpace(previous.Outcome), strings.TrimSpace(payload.Outcome)) {
		return "outcome_changed", true
	}
	if !at.Before(previous.PublishedAt.Add(internalHeartbeatPublicSummaryMinInterval)) {
		return "rate_limit_elapsed", true
	}
	return "", false
}

func internalHeartbeatPublicSummaryIsSyntheticCapabilitySession(payload InternalHeartbeatPublicSummaryPayload) bool {
	if !payload.LocalOnly {
		return false
	}
	heartbeatID := strings.ToLower(strings.TrimSpace(payload.HeartbeatID))
	kind := strings.ToLower(strings.TrimSpace(payload.HeartbeatKind))
	return strings.HasPrefix(heartbeatID, "capability_session_") || kind == "capability_session"
}

func (r *Runtime) recordInternalHeartbeatSummaryPublished(payload InternalHeartbeatPublicSummaryPayload, at time.Time) {
	if r == nil {
		return
	}
	heartbeatID := strings.TrimSpace(payload.HeartbeatID)
	if heartbeatID == "" {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.internalHeartbeatState.PublicSummary == nil {
		r.internalHeartbeatState.PublicSummary = map[string]internalHeartbeatPublicSummaryState{}
	}
	r.internalHeartbeatState.PublicSummary[heartbeatID] = internalHeartbeatPublicSummaryState{
		Status:      strings.TrimSpace(payload.Status),
		Outcome:     strings.TrimSpace(payload.Outcome),
		PublishedAt: at.UTC(),
	}
}

func internalHeartbeatPublicSummaryReferenceTime(session AgentInternalSessionRecord) time.Time {
	for _, raw := range []string{session.EndedAt, session.StartedAt} {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Now().UTC()
}

func internalHeartbeatPublicSummaryText(payload InternalHeartbeatPublicSummaryPayload) string {
	if strings.TrimSpace(payload.Summary) == "" {
		payload.Summary = internalHeartbeatPublicSummaryDeterministicText(payload)
	}
	heartbeatID := firstNonEmpty(payload.HeartbeatID, "heartbeat")
	outcome := firstNonEmpty(payload.Outcome, payload.Status, "recorded")
	summary := strings.TrimSpace(payload.Summary)
	if summary == "" || summary == "<redacted>" {
		summary = "compact internal heartbeat summary recorded"
	}
	text := fmt.Sprintf("Internal heartbeat %s %s: %s", heartbeatID, outcome, summary)
	return internalHeartbeatSurfaceField(text, 260)
}

func internalHeartbeatPublicSummaryDeterministicText(payload InternalHeartbeatPublicSummaryPayload) string {
	status := firstNonEmpty(payload.Status, "recorded")
	outcome := firstNonEmpty(payload.Outcome, "unknown_outcome")
	parts := []string{
		"status=" + status,
		"outcome=" + outcome,
	}
	if payload.LocalOnly {
		parts = append(parts, "local_only=true")
	}
	if payload.PromotedRefCount > 0 {
		parts = append(parts, fmt.Sprintf("promoted_refs=%d", payload.PromotedRefCount))
	}
	if payload.PublishReason != "" {
		parts = append(parts, "reason="+payload.PublishReason)
	}
	return strings.Join(parts, "; ")
}

func (r *Runtime) recordCapabilitySessionsForArbiterRoutes(ctx context.Context, store *AgentInternalSessionStore, anatomy AgentAnatomyConfig, parentSession AgentInternalSessionRecord, routes []AgentPersonalBacklogItem, now time.Time) ([]string, error) {
	if store == nil || len(routes) == 0 {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	refs := []string{}
	for _, route := range routes {
		if !internalHeartbeatBacklogItemIsArbiterRoute(route) {
			continue
		}
		ref, err := r.recordCapabilitySessionForRoute(ctx, store, anatomy, parentSession, route, now)
		if err != nil {
			return refs, err
		}
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	return uniqueTrimmedCSVStrings(refs), nil
}

func (r *Runtime) recordCapabilitySessionForRoute(ctx context.Context, store *AgentInternalSessionStore, anatomy AgentAnatomyConfig, parentSession AgentInternalSessionRecord, route AgentPersonalBacklogItem, now time.Time) (string, error) {
	if store == nil {
		return "", nil
	}
	plan := internalHeartbeatCapabilitySessionPlanForRoute(route)
	if r != nil && r.localToolBundleCapabilityAvailable(plan.Capability, plan.ToolSuite) {
		plan.Status = "ready"
		plan.Outcome = "capability_ready"
		plan.Kind = "capability_session_ready"
		plan.Title = "Capability session ready: " + plan.Capability
		plan.Supported = true
		plan.Summary = fmt.Sprintf("Capability %s%s is installed in this agent workdir as a local tool bundle; a bounded heartbeat can execute it with explicit arguments.", plan.Capability, internalHeartbeatActionRouteToolSuiteSuffix(plan.ToolSuite))
	}
	if strings.TrimSpace(plan.Capability) == "" {
		return "", nil
	}
	spec := internalHeartbeatCapabilitySessionSpec(plan)
	policy := internalHeartbeatExecutionPolicy(spec)
	session, err := store.BeginHeartbeatSession(spec, AgentAnatomyDigest(anatomy), "personal_backlog_arbiter:"+strings.TrimSpace(parentSession.SessionID), now)
	if err != nil {
		return "", err
	}
	session.PromotionBlocked = true
	session.Meta = map[string]string{
		"parent_session_id":    strings.TrimSpace(parentSession.SessionID),
		"route_backlog_item":   strings.TrimSpace(route.ItemID),
		"route_dedup_key":      strings.TrimSpace(route.DedupKey),
		"capability":           plan.Capability,
		"tool_suite":           plan.ToolSuite,
		"capability_status":    plan.Status,
		"supported":            fmt.Sprint(plan.Supported),
		"requires_task_loop":   fmt.Sprint(plan.RequiresTaskLoop),
		"requires_human_input": fmt.Sprint(plan.RequiresHumanInput),
	}
	if session, err = store.RecordSession(session); err != nil {
		return "", err
	}
	result := InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         plan.Summary,
		BacklogItems:    []InternalHeartbeatFinding{internalHeartbeatCapabilitySessionFinding(route, plan, session)},
	}
	if _, err := materializeInternalHeartbeatResultToBacklog(store, session, spec, policy, result, now); err != nil {
		_ = store.CompleteSession(session.SessionID, "failed", "capability_result_failed", "Capability session could not persist local result.", nil, err, now)
		return "internal_session:" + session.SessionID, err
	}
	if err := store.CompleteSession(session.SessionID, "completed", plan.Outcome, plan.Summary, nil, nil, now); err != nil {
		return "internal_session:" + session.SessionID, err
	}
	_ = ctx
	return "internal_session:" + session.SessionID, nil
}

func (r *Runtime) localToolBundleCapabilityAvailable(capability, toolSuite string) bool {
	capability = strings.ToLower(strings.TrimSpace(capability))
	toolSuite = strings.ToLower(strings.TrimSpace(toolSuite))
	if !stringInSet(toolSuite, "browser_unrestricted", "browser_interactive", "browser_read_only", "screenshot_capture") ||
		!stringInSet(capability, "browser_session", "browser-session", "browser_interaction", "browser-interaction", "browser_screenshot", "browser-screenshot", "screenshot_capture", "screenshot-capture", "browser_visual_probe", "browser-visual-probe", "browser_read_only", "browser-read-only") {
		return false
	}
	if r == nil || r.agent == nil || r.agent.registry == nil {
		return false
	}
	if stringInSet(capability, "browser_session", "browser-session", "browser_interaction", "browser-interaction") || stringInSet(toolSuite, "browser_unrestricted", "browser_interactive") {
		if _, ok := r.agent.registry.Get("browser_session"); ok {
			return true
		}
	}
	_, ok := r.agent.registry.Get("browser_visual_probe")
	return ok
}

type internalHeartbeatCapabilitySessionPlan struct {
	Capability         string
	ToolSuite          string
	Status             string
	Outcome            string
	Kind               string
	Title              string
	Summary            string
	Score              int
	Supported          bool
	RequiresTaskLoop   bool
	RequiresHumanInput bool
}

func internalHeartbeatCapabilitySessionPlanForRoute(route AgentPersonalBacklogItem) internalHeartbeatCapabilitySessionPlan {
	capability, toolSuite, requiresTaskLoop, requiresHumanInput := internalHeartbeatCapabilityMetadataFromBacklogItem(route)
	capability = firstNonEmpty(capability, "unspecified_capability")
	plan := internalHeartbeatCapabilitySessionPlan{
		Capability:         capability,
		ToolSuite:          toolSuite,
		Status:             "blocked",
		Outcome:            "capability_blocked",
		Kind:               "capability_session_blocked",
		Title:              "Capability session blocked: " + capability,
		Score:              internalHeartbeatClampScore(route.Score),
		RequiresTaskLoop:   requiresTaskLoop,
		RequiresHumanInput: requiresHumanInput,
	}
	switch {
	case requiresHumanInput || containsAny(capability, "human", "operator", "credential", "secret", "domain", "ads", "payment"):
		plan.Status = "human_input_required"
		plan.Outcome = "human_input_required"
		plan.Kind = "capability_session_human_input_required"
		plan.Title = "Capability session needs human input: " + capability
		plan.Summary = fmt.Sprintf("Capability %s%s requires human/operator input; the agent must keep this blocker explicit instead of fabricating completion.", capability, internalHeartbeatActionRouteToolSuiteSuffix(toolSuite))
	case internalHeartbeatCapabilityHasTypedExecutor(capability, toolSuite):
		plan.Status = "ready"
		plan.Outcome = "capability_ready"
		plan.Kind = "capability_session_ready"
		plan.Title = "Capability session ready: " + capability
		plan.Supported = true
		plan.Summary = fmt.Sprintf("Capability %s%s has a typed local/read-only executor path available; a future bounded heartbeat may execute it with explicit arguments.", capability, internalHeartbeatActionRouteToolSuiteSuffix(toolSuite))
	default:
		plan.Summary = fmt.Sprintf("Capability %s%s has no typed executor manifest in this runtime slice; keep the route local and blocked rather than pretending evidence exists.", capability, internalHeartbeatActionRouteToolSuiteSuffix(toolSuite))
	}
	if plan.Score == 0 {
		plan.Score = 70
	}
	return plan
}

func internalHeartbeatCapabilityHasTypedExecutor(capability, toolSuite string) bool {
	capability = strings.ToLower(strings.TrimSpace(capability))
	toolSuite = strings.ToLower(strings.TrimSpace(toolSuite))
	if stringInSet(toolSuite, "memory_and_docs_read", "workspace_docs_read", "local_log_read", "local_tests_read", "rhizome_read", "patch_queue_read") {
		return true
	}
	return stringInSet(capability, "workspace_doc_read", "workspace-doc-read", "memory_read", "memory-read", "memory_search", "memory-search", "local_log_read", "local-log-read", "patch_queue_read", "patch-queue-read")
}

func internalHeartbeatCapabilitySessionSpec(plan internalHeartbeatCapabilitySessionPlan) AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:               "capability_session_" + sanitizeRefSegment(firstNonEmpty(plan.Capability, "unknown")),
		Kind:             "capability_session",
		Cadence:          "every_10m",
		Priority:         10,
		Locks:            []string{"local_only"},
		ToolSuites:       []string{internalHeartbeatCapabilitySessionToolSuite(plan)},
		ContextSelectors: []string{"recent_internal_sessions", "local_memory"},
		OutputContracts:  []string{"local_memory", "capability_session_result"},
		Objective:        "Record a typed local capability-session outcome for one routed personal backlog action request.",
		Instructions: []string{
			"record whether this capability has a typed executor available in the current runtime",
			"do not execute unsupported browser, shell, or mutation tools from this local session",
			"write only local session/backlog state",
		},
		MemoryLanes: []string{"role_backlog", "working_notes"},
	})
}

func internalHeartbeatCapabilitySessionToolSuite(plan internalHeartbeatCapabilitySessionPlan) string {
	toolSuite := strings.TrimSpace(plan.ToolSuite)
	if plan.Supported && internalHeartbeatCapabilityHasTypedExecutor(plan.Capability, toolSuite) {
		if stringInSet(strings.ToLower(toolSuite), "memory_and_docs_read", "workspace_docs_read", "local_log_read", "local_tests_read", "rhizome_read", "patch_queue_read") {
			return toolSuite
		}
		if stringInSet(strings.ToLower(toolSuite), "browser_read_only", "screenshot_capture") {
			return toolSuite
		}
	}
	return "memory_and_docs_read"
}

func internalHeartbeatCapabilitySessionFinding(route AgentPersonalBacklogItem, plan internalHeartbeatCapabilitySessionPlan, session AgentInternalSessionRecord) InternalHeartbeatFinding {
	refs := []string{
		"backlog_item:" + strings.TrimSpace(route.ItemID),
		"dedup:" + strings.TrimSpace(route.DedupKey),
		"capability:" + sanitizeRefSegment(plan.Capability),
		"capability_status:" + sanitizeRefSegment(plan.Status),
		"internal_session:" + strings.TrimSpace(session.SessionID),
	}
	if strings.TrimSpace(plan.ToolSuite) != "" {
		refs = append(refs, "tool_suite:"+sanitizeRefSegment(plan.ToolSuite))
	}
	if plan.RequiresTaskLoop {
		refs = append(refs, "requires:task_loop")
	}
	if plan.RequiresHumanInput {
		refs = append(refs, "requires:human_input")
	}
	return InternalHeartbeatFinding{
		DedupKey:     "capability-session:" + sanitizeRefSegment(plan.Status) + ":" + sanitizeRefSegment(firstNonEmpty(route.DedupKey, route.ItemID)),
		Kind:         plan.Kind,
		Source:       internalHeartbeatCapabilitySessionSource,
		ProjectID:    strings.TrimSpace(route.Meta["target_project_id"]),
		ProjectLane:  firstNonEmpty(strings.TrimSpace(route.Meta["target_project_lane"]), internalHeartbeatCapabilityDefaultLane(plan.Capability)),
		BlockPromote: true,
		Title:        plan.Title,
		Summary:      plan.Summary,
		Score:        internalHeartbeatClampScore(plan.Score),
		EvidenceRefs: uniqueTrimmedCSVStrings(refs),
		Promote:      false,
		Reason:       "typed capability session recorded a local route outcome",
	}
}

func internalHeartbeatBacklogItemIsArbiterRoute(item AgentPersonalBacklogItem) bool {
	return normalizeAgentPersonalBacklogStatus(item.Status) == "open" &&
		strings.EqualFold(strings.TrimSpace(item.Kind), "personal_backlog_action_route") &&
		strings.EqualFold(strings.TrimSpace(item.Meta["finding_source"]), internalHeartbeatBacklogArbiterSource)
}

func internalHeartbeatCapabilityMetadataFromBacklogItem(item AgentPersonalBacklogItem) (capability, toolSuite string, requiresTaskLoop, requiresHumanInput bool) {
	capability = strings.TrimSpace(item.Meta["action_request_capability"])
	toolSuite = strings.TrimSpace(item.Meta["action_request_tool_suite"])
	requiresTaskLoop = strings.EqualFold(strings.TrimSpace(item.Meta["action_requires_task_loop"]), "true")
	requiresHumanInput = strings.EqualFold(strings.TrimSpace(item.Meta["action_requires_human_input"]), "true")
	if capability == "" || toolSuite == "" || !requiresTaskLoop || !requiresHumanInput {
		refCapability, refToolSuite, refRequiresTaskLoop, refRequiresHumanInput := internalHeartbeatActionMetadataFromEvidence(item.EvidenceRefs)
		capability = firstNonEmpty(capability, refCapability)
		toolSuite = firstNonEmpty(toolSuite, refToolSuite)
		requiresTaskLoop = requiresTaskLoop || refRequiresTaskLoop
		requiresHumanInput = requiresHumanInput || refRequiresHumanInput
	}
	return capability, toolSuite, requiresTaskLoop, requiresHumanInput
}

func internalHeartbeatActionMetadataFromEvidence(refs []string) (capability, toolSuite string, requiresTaskLoop, requiresHumanInput bool) {
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		lower := strings.ToLower(ref)
		switch {
		case strings.HasPrefix(lower, "capability:"):
			capability = firstNonEmpty(capability, strings.TrimSpace(ref[len("capability:"):]))
		case strings.HasPrefix(lower, "tool_suite:"):
			toolSuite = firstNonEmpty(toolSuite, strings.TrimSpace(ref[len("tool_suite:"):]))
		case lower == "requires:task_loop":
			requiresTaskLoop = true
		case lower == "requires:human_input":
			requiresHumanInput = true
		}
	}
	return capability, toolSuite, requiresTaskLoop, requiresHumanInput
}

func (r *Runtime) promoteInternalHeartbeatBacklogCandidates(ctx context.Context, store *AgentInternalSessionStore, sessionID string, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, items []AgentPersonalBacklogItem, now time.Time) ([]AgentBacklogPromotionContract, []string, error) {
	if r == nil || store == nil || !policy.AllowTaskSubmit || policy.LocalOnly {
		return nil, nil, nil
	}
	if r.client == nil {
		return nil, nil, nil
	}
	if strings.TrimSpace(r.client.endpoint) == "" {
		return nil, nil, nil
	}
	if internalHeartbeatProjectPromotionBlockedByCurrentSession(items, sessionID, spec) {
		return nil, nil, nil
	}
	candidates := internalHeartbeatPromotionCandidates(items, sessionID, spec, internalHeartbeatMinPromotionScore)
	if len(candidates) == 0 {
		return nil, nil, nil
	}
	limit := policy.MaxTaskSubmits
	if limit <= 0 || limit > 1 {
		limit = 1
	}
	contracts := make([]AgentBacklogPromotionContract, 0, limit)
	refs := make([]string, 0, limit*2)
	for _, item := range candidates {
		if len(contracts) >= limit {
			break
		}
		target, ok := r.internalHeartbeatBacklogPromotionTarget(spec, policy, item)
		if !ok {
			continue
		}
		contract, err := store.PromoteBacklogItem(ctx, r.client, item.ItemID, target, now)
		if err != nil {
			return contracts, refs, err
		}
		contracts = append(contracts, contract)
		refs = append(refs, "task:"+contract.TaskID, "doc:"+contract.DocKey)
	}
	return contracts, uniqueTrimmedCSVStrings(refs), nil
}

func internalHeartbeatPromotionCandidates(items []AgentPersonalBacklogItem, sessionID string, spec AgentHeartbeatSpec, minScore int) []AgentPersonalBacklogItem {
	sessionID = strings.TrimSpace(sessionID)
	spec = normalizeAgentHeartbeatSpec(spec)
	out := make([]AgentPersonalBacklogItem, 0, len(items))
	for _, item := range items {
		if item.Stale || normalizeAgentPersonalBacklogStatus(item.Status) != "open" {
			continue
		}
		if strings.TrimSpace(item.LastSessionID) == "" || strings.TrimSpace(item.LastSessionID) != sessionID {
			continue
		}
		if strings.TrimSpace(item.HeartbeatID) != strings.TrimSpace(spec.ID) {
			continue
		}
		if item.Score < minScore {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Meta["finding_promote"]), "true") {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(spec.ID), "visual_product_audit") &&
			!strings.EqualFold(strings.TrimSpace(item.Meta["finding_source"]), internalHeartbeatVisualSensorSource) {
			continue
		}
		if internalHeartbeatSpecUsesPatchQueueVigilancePromotion(spec) &&
			!strings.EqualFold(strings.TrimSpace(item.Meta["finding_source"]), internalHeartbeatPatchQueueVigilanceSource) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Meta["policy_allow_task_submit"]), "true") ||
			!strings.EqualFold(strings.TrimSpace(item.Meta["policy_local_only"]), "false") {
			continue
		}
		out = append(out, item)
	}
	if len(out) <= 1 {
		return out
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt < out[j].UpdatedAt
		}
		return out[i].ItemID < out[j].ItemID
	})
	return out
}

func internalHeartbeatProjectPromotionBlockedByCurrentSession(items []AgentPersonalBacklogItem, sessionID string, spec AgentHeartbeatSpec) bool {
	if !internalHeartbeatSpecUsesProjectInitiativePromotion(spec) {
		return false
	}
	sessionID = strings.TrimSpace(sessionID)
	blockedProjects := map[string]bool{}
	for _, item := range items {
		if item.Stale || normalizeAgentPersonalBacklogStatus(item.Status) != "open" {
			continue
		}
		if strings.TrimSpace(item.LastSessionID) == "" || strings.TrimSpace(item.LastSessionID) != sessionID {
			continue
		}
		if strings.TrimSpace(item.HeartbeatID) != strings.TrimSpace(spec.ID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Meta["block_promote"]), "true") {
			continue
		}
		projectID := strings.TrimSpace(item.Meta["target_project_id"])
		if projectID == "" {
			projectID = "*"
		}
		blockedProjects[projectID] = true
	}
	if len(blockedProjects) == 0 {
		return false
	}
	for _, item := range items {
		if item.Stale || normalizeAgentPersonalBacklogStatus(item.Status) != "open" {
			continue
		}
		if strings.TrimSpace(item.LastSessionID) == "" || strings.TrimSpace(item.LastSessionID) != sessionID {
			continue
		}
		if strings.TrimSpace(item.HeartbeatID) != strings.TrimSpace(spec.ID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Meta["finding_promote"]), "true") {
			continue
		}
		projectID := strings.TrimSpace(item.Meta["target_project_id"])
		if blockedProjects["*"] || blockedProjects[projectID] || (projectID == "" && len(blockedProjects) > 0) {
			return true
		}
	}
	return false
}

func (r *Runtime) internalHeartbeatBacklogPromotionTarget(spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, item AgentPersonalBacklogItem) (AgentBacklogPromotionTarget, bool) {
	var cfg RuntimeConfig
	if r != nil {
		cfg = r.cfg
	}
	projectID, projectLane := r.internalHeartbeatPromotionProjectScope()
	itemProjectID, itemProjectLane := internalHeartbeatBacklogPromotionItemScope(spec, item)
	if strings.TrimSpace(itemProjectID) != "" {
		projectID = itemProjectID
		projectLane = itemProjectLane
	} else if strings.TrimSpace(projectID) == "" {
		projectID, projectLane = itemProjectID, itemProjectLane
	}
	if internalHeartbeatBacklogItemIsActionRoutePromotion(spec, item) {
		projectLane = internalHeartbeatActionRoutePromotionLane(item.Meta["action_request_capability"], projectLane)
	}
	projectLane = firstNonEmpty(projectLane, internalHeartbeatDefaultProjectLane(spec, item))
	if internalHeartbeatSpecUsesProjectInitiativePromotion(spec) && strings.TrimSpace(projectID) == "" {
		return AgentBacklogPromotionTarget{}, false
	}
	if internalHeartbeatSpecUsesPatchQueueVigilancePromotion(spec) && strings.TrimSpace(projectID) == "" {
		return AgentBacklogPromotionTarget{}, false
	}
	taskKind := "EXECUTION"
	if strings.TrimSpace(projectID) == "" && strings.EqualFold(strings.TrimSpace(spec.Kind), "global_metacognition") {
		taskKind = "COORDINATION"
		projectLane = firstNonEmpty(projectLane, "coordination")
	}
	return AgentBacklogPromotionTarget{
		WorkspaceID:         strings.TrimSpace(cfg.WorkspaceID),
		AgentID:             strings.TrimSpace(cfg.AgentID),
		OwnerUserID:         strings.TrimSpace(cfg.OwnerUserID),
		ProjectID:           projectID,
		ProjectLane:         projectLane,
		TaskKind:            taskKind,
		TaskTemplate:        "generic",
		Priority:            backlogPriorityForScore(item.Score),
		WriteScopeHints:     internalHeartbeatPromotionWriteScopeHints(spec, item),
		TaskRequirements:    internalHeartbeatPromotionTaskRequirements(spec, item),
		Tags:                internalHeartbeatPromotionTags(spec, policy, item),
		Reason:              firstNonEmpty(item.Meta["finding_reason"], "promoted by internal heartbeat "+firstNonEmpty(spec.ID, policy.HeartbeatID)),
		RequiresProjectGate: nil,
	}, true
}

func (r *Runtime) internalHeartbeatPromotionProjectScope() (string, string) {
	if r == nil {
		return "", ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeTask != nil {
		return strings.TrimSpace(r.activeTask.ProjectID), strings.TrimSpace(r.activeTask.ProjectLane)
	}
	if r.activeWorkPacket != nil {
		return strings.TrimSpace(r.activeWorkPacket.ProjectID), strings.TrimSpace(r.activeWorkPacket.ProjectLane)
	}
	return "", ""
}

func internalHeartbeatBacklogItemIsActionRoutePromotion(spec AgentHeartbeatSpec, item AgentPersonalBacklogItem) bool {
	if strings.EqualFold(strings.TrimSpace(spec.ID), "action_request_promoter") {
		return true
	}
	source := strings.TrimSpace(item.Meta["finding_source"])
	return strings.EqualFold(source, internalHeartbeatActionRequestPromoterSource) ||
		strings.EqualFold(source, internalHeartbeatActionRequestSource) ||
		strings.TrimSpace(item.Meta["action_request_capability"]) != "" ||
		strings.TrimSpace(item.Meta["action_request_tool_suite"]) != ""
}

func internalHeartbeatPromotionLaneLooksImplementation(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "implementation", "implement", "coding", "code", "frontend", "front-end", "ui", "backend", "back-end", "api", "fullstack", "full-stack":
		return true
	default:
		return false
	}
}

func internalHeartbeatPromotionLaneLooksGenericActionHost(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "", "strategy", "strategic", "coordination", "coordinator", "planning", "plan", "spec", "design", "discovery", "research", "ops":
		return true
	default:
		return internalHeartbeatPromotionLaneLooksImplementation(lane)
	}
}

func internalHeartbeatActionRoutePromotionLane(capability, targetLane string) string {
	actionLane := internalHeartbeatCapabilityDefaultLane(capability)
	if internalHeartbeatPromotionLaneLooksGenericActionHost(targetLane) {
		return actionLane
	}
	return strings.TrimSpace(targetLane)
}

func internalHeartbeatBacklogPromotionItemScope(spec AgentHeartbeatSpec, item AgentPersonalBacklogItem) (string, string) {
	source := strings.TrimSpace(item.Meta["finding_source"])
	switch {
	case internalHeartbeatSpecUsesProjectInitiativePromotion(spec):
		if !strings.EqualFold(source, internalHeartbeatProjectInitiativeSensorSource) {
			return "", ""
		}
	case internalHeartbeatSpecUsesPatchQueueVigilancePromotion(spec):
		if !strings.EqualFold(source, internalHeartbeatPatchQueueVigilanceSource) {
			return "", ""
		}
	case strings.EqualFold(strings.TrimSpace(spec.ID), "action_request_promoter"):
		if !strings.EqualFold(source, internalHeartbeatActionRequestPromoterSource) {
			return "", ""
		}
	default:
		return "", ""
	}
	if projectID := strings.TrimSpace(item.Meta["target_project_id"]); projectID != "" {
		return projectID, strings.TrimSpace(item.Meta["target_project_lane"])
	}
	return "", ""
}

func internalHeartbeatPromotionWriteScopeHints(spec AgentHeartbeatSpec, item AgentPersonalBacklogItem) []string {
	hints := []string{"agent-personal-backlog:" + strings.TrimSpace(item.ItemID)}
	if heartbeatID := strings.TrimSpace(firstNonEmpty(item.HeartbeatID, spec.ID)); heartbeatID != "" {
		hints = append(hints, "internal-heartbeat:"+heartbeatID)
	}
	hints = append(hints, internalHeartbeatPatchQueueRefsForPromotion(item)...)
	return uniqueTrimmedCSVStrings(hints)
}

func internalHeartbeatPromotionTaskRequirements(spec AgentHeartbeatSpec, item AgentPersonalBacklogItem) map[string]any {
	if !internalHeartbeatSpecUsesPatchQueueVigilancePromotion(spec) ||
		!strings.EqualFold(strings.TrimSpace(item.Meta["finding_source"]), internalHeartbeatPatchQueueVigilanceSource) {
		return nil
	}
	record, followupKind, ok := internalHeartbeatPatchQueueRecordForPromotion(item)
	if !ok {
		return nil
	}
	requirements := projectPatchQueueFollowupTaskRequirements(record, followupKind)
	if requirements == nil {
		requirements = map[string]any{}
	}
	requirements["origin"] = "patch_queue_vigilance_backlog_promotion"
	requirements["source_backlog_item_id"] = strings.TrimSpace(item.ItemID)
	requirements["source_backlog_dedup_key"] = strings.TrimSpace(item.DedupKey)
	requirements["patch_queue_id"] = strings.TrimSpace(record.QueueID)
	if strings.EqualFold(strings.TrimSpace(followupKind), "integration") {
		requirements["required_project_role"] = "INTEGRATOR"
		requirements["integration_completion_gate"] = "canonical_target_build_and_verifier_mesh"
	}
	return requirements
}

func internalHeartbeatPatchQueueRecordForPromotion(item AgentPersonalBacklogItem) (ProjectPatchQueueItemRecord, string, bool) {
	refs := internalHeartbeatPatchQueueRefMap(item)
	meta := item.Meta
	record := ProjectPatchQueueItemRecord{
		QueueID:                 firstNonEmpty(strings.TrimSpace(refs["queue_id"]), strings.TrimSpace(meta["queue_id"])),
		ItemID:                  firstNonEmpty(strings.TrimSpace(refs["item_id"]), strings.TrimSpace(meta["item_id"])),
		ProjectID:               firstNonEmpty(strings.TrimSpace(meta["project_id"]), strings.TrimSpace(meta["target_project_id"])),
		RepoID:                  strings.TrimSpace(meta["repo_id"]),
		BranchID:                firstNonEmpty(strings.TrimSpace(refs["branch_id"]), strings.TrimSpace(meta["branch_id"])),
		State:                   strings.ToUpper(firstNonEmpty(strings.TrimSpace(meta["state"]), internalHeartbeatPatchQueueStateForFindingKind(item.Kind))),
		HeadSHA:                 firstNonEmpty(strings.TrimSpace(refs["head_sha"]), strings.TrimSpace(meta["head_sha"])),
		RepoAuthorityMode:       strings.TrimSpace(meta["repo_authority_mode"]),
		MaterializationAccepted: strings.EqualFold(strings.TrimSpace(meta["materialization_accepted"]), "true"),
		MaterializationSchema:   strings.TrimSpace(meta["materialization_schema"]),
		MaterializationDigest:   strings.TrimSpace(meta["materialization_digest"]),
		ReviewDocKey:            strings.TrimSpace(meta["review_doc_key"]),
		EvidenceDocKey:          strings.TrimSpace(meta["evidence_doc_key"]),
		DecisionDocKey:          strings.TrimSpace(meta["decision_doc_key"]),
		DecisionSummary:         strings.TrimSpace(meta["decision_summary"]),
		SupersedesQueueID:       strings.TrimSpace(meta["supersedes_queue_id"]),
		SupersedesItemID:        strings.TrimSpace(meta["supersedes_item_id"]),
	}
	if record.QueueID == "" || record.ItemID == "" || record.BranchID == "" {
		return ProjectPatchQueueItemRecord{}, "", false
	}
	kind := internalHeartbeatPatchQueueFollowupKindForPromotion(item, record)
	if strings.TrimSpace(kind) == "" {
		return ProjectPatchQueueItemRecord{}, "", false
	}
	return record, kind, true
}

func internalHeartbeatPatchQueueStateForFindingKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "patch_queue_accepted_integration_gap", "patch_queue_visual_evidence_gap":
		return "ACCEPTED"
	default:
		return ""
	}
}

func internalHeartbeatPatchQueueFollowupKindForPromotion(item AgentPersonalBacklogItem, record ProjectPatchQueueItemRecord) string {
	switch strings.TrimSpace(item.Kind) {
	case "patch_queue_accepted_integration_gap":
		return "integration"
	case "patch_queue_visual_evidence_gap":
		return "validation"
	case "patch_queue_review_owner_gap":
		return "review"
	case "patch_queue_convergence_gap":
		switch strings.ToUpper(strings.TrimSpace(record.State)) {
		case "BLOCKED":
			if containsAny(strings.ToLower(record.DecisionSummary), "validation", "verify", "test", "build") {
				return "validation"
			}
			return "revision"
		case "REVIEW_READY", "READY_FOR_REVIEW", "PENDING_REVIEW":
			return "review"
		case "CLAIMED", "ACCEPTED":
			return "integration"
		default:
			return "validation"
		}
	default:
		return ""
	}
}

func internalHeartbeatPromotionTags(spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, item AgentPersonalBacklogItem) []string {
	tags := []string{"internal-heartbeat", sanitizeRefSegment(firstNonEmpty(spec.ID, policy.HeartbeatID)), sanitizeRefSegment(firstNonEmpty(item.Kind, spec.Kind))}
	if capability := strings.TrimSpace(item.Meta["action_request_capability"]); capability != "" {
		tags = append(tags, "action-request", "capability-"+sanitizeRefSegment(capability))
		if strings.EqualFold(internalHeartbeatCapabilityDefaultLane(capability), "qa") {
			tags = append(tags, "validation", "visual-qa")
		}
	}
	if toolSuite := strings.TrimSpace(item.Meta["action_request_tool_suite"]); toolSuite != "" {
		tags = append(tags, "tool-suite-"+sanitizeRefSegment(toolSuite))
		if containsAny(toolSuite, "screenshot", "browser") {
			tags = append(tags, "browser-smoke")
		}
	}
	for _, ref := range internalHeartbeatPatchQueueRefsForPromotion(item) {
		tags = append(tags, sanitizeRefSegment(ref))
	}
	return uniqueTrimmedCSVStrings(tags)
}

func internalHeartbeatPatchQueueRefsForPromotion(item AgentPersonalBacklogItem) []string {
	if !strings.EqualFold(strings.TrimSpace(item.Meta["finding_source"]), internalHeartbeatPatchQueueVigilanceSource) {
		return nil
	}
	refs := []string{}
	for _, raw := range item.EvidenceRefs {
		raw = strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(raw, "patch_queue:"),
			strings.HasPrefix(raw, "patch_item:"),
			strings.HasPrefix(raw, "branch:"),
			strings.HasPrefix(raw, "head:"),
			strings.HasPrefix(raw, "integration_mode:"):
			refs = append(refs, raw)
		}
	}
	return uniqueTrimmedCSVStrings(refs)
}

func internalHeartbeatPatchQueueRefMap(item AgentPersonalBacklogItem) map[string]string {
	out := map[string]string{}
	for _, raw := range internalHeartbeatPatchQueueRefsForPromotion(item) {
		raw = strings.TrimSpace(raw)
		key, value, ok := strings.Cut(raw, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(key) {
		case "patch_queue":
			out["queue_id"] = value
		case "patch_item":
			out["item_id"] = value
		case "branch":
			out["branch_id"] = value
		case "head":
			out["head_sha"] = value
		case "integration_mode":
			out["integration_mode"] = value
		}
	}
	return out
}

func materializeInternalHeartbeatResultToBacklog(store *AgentInternalSessionStore, session AgentInternalSessionRecord, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, result InternalHeartbeatLocalResult, now time.Time) ([]AgentPersonalBacklogItem, error) {
	if store == nil {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	spec = normalizeAgentHeartbeatSpec(spec)
	result = normalizeInternalHeartbeatLocalResult(result)
	items := make([]AgentPersonalBacklogItem, 0, len(result.BacklogItems)+len(result.ActionRequests))
	for _, finding := range result.BacklogItems {
		if len(items) >= internalHeartbeatMaxBacklogWrites {
			break
		}
		item, ok := internalHeartbeatFindingBacklogItem(session, spec, policy, result, finding, now)
		if !ok {
			continue
		}
		upserted, err := store.UpsertBacklogItem(item)
		if err != nil {
			return items, err
		}
		items = append(items, upserted)
	}
	for _, request := range result.ActionRequests {
		if len(items) >= internalHeartbeatMaxBacklogWrites {
			break
		}
		item, ok := internalHeartbeatActionRequestBacklogItem(session, spec, policy, result, request, now)
		if !ok {
			continue
		}
		upserted, err := store.UpsertBacklogItem(item)
		if err != nil {
			return items, err
		}
		items = append(items, upserted)
	}
	return items, nil
}

func internalHeartbeatFindingBacklogItem(session AgentInternalSessionRecord, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, result InternalHeartbeatLocalResult, finding InternalHeartbeatFinding, now time.Time) (AgentPersonalBacklogItem, bool) {
	finding = normalizeInternalHeartbeatFinding(finding)
	title := firstNonEmpty(finding.Title, finding.Summary)
	if strings.TrimSpace(title) == "" {
		return AgentPersonalBacklogItem{}, false
	}
	summary := firstNonEmpty(finding.Summary, finding.Title, result.Summary)
	dedupKey := firstNonEmpty(finding.DedupKey, internalHeartbeatFindingDedupKey(spec, finding))
	evidence := append([]string(nil), finding.EvidenceRefs...)
	if strings.TrimSpace(session.SessionID) != "" {
		evidence = append(evidence, "internal_session:"+strings.TrimSpace(session.SessionID))
	}
	capability, toolSuite, requiresTaskLoop, requiresHumanInput := internalHeartbeatActionMetadataFromEvidence(evidence)
	meta := map[string]string{
		"heartbeat_kind":           firstNonEmpty(spec.Kind, session.HeartbeatKind),
		"finding_source":           strings.TrimSpace(finding.Source),
		"target_project_id":        strings.TrimSpace(finding.ProjectID),
		"target_project_lane":      strings.TrimSpace(finding.ProjectLane),
		"block_promote":            fmt.Sprint(finding.BlockPromote),
		"session_outcome":          strings.TrimSpace(session.Outcome),
		"result_outcome":           strings.TrimSpace(result.Outcome),
		"result_contract":          strings.TrimSpace(result.ContractVersion),
		"finding_reason":           strings.TrimSpace(finding.Reason),
		"finding_promote":          fmt.Sprint(finding.Promote),
		"policy_local_only":        fmt.Sprint(policy.LocalOnly),
		"policy_allow_task_submit": fmt.Sprint(policy.AllowTaskSubmit),
		"promotion_signals":        strings.Join(policy.PromotionSignals, ","),
		"output_contracts":         strings.Join(policy.OutputContracts, ","),
		"observed_at":              now.UTC().Format(time.RFC3339Nano),
	}
	for key, value := range finding.Meta {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		meta[key] = value
	}
	if strings.EqualFold(strings.TrimSpace(finding.Source), internalHeartbeatBacklogArbiterSource) || capability != "" || toolSuite != "" {
		meta["action_request_capability"] = capability
		meta["action_request_tool_suite"] = toolSuite
		meta["action_requires_task_loop"] = fmt.Sprint(requiresTaskLoop)
		meta["action_requires_human_input"] = fmt.Sprint(requiresHumanInput)
	}
	return AgentPersonalBacklogItem{
		DedupKey:      dedupKey,
		HeartbeatID:   spec.ID,
		Kind:          firstNonEmpty(finding.Kind, spec.Kind, "heartbeat_finding"),
		Status:        "open",
		Title:         title,
		Summary:       summary,
		Score:         finding.Score,
		EvidenceRefs:  evidence,
		LastSessionID: strings.TrimSpace(session.SessionID),
		Meta:          meta,
	}, true
}

func internalHeartbeatActionRequestBacklogItem(session AgentInternalSessionRecord, spec AgentHeartbeatSpec, policy InternalHeartbeatExecutionPolicy, result InternalHeartbeatLocalResult, request InternalHeartbeatActionRequest, now time.Time) (AgentPersonalBacklogItem, bool) {
	request = normalizeInternalHeartbeatActionRequest(request)
	capability := firstNonEmpty(request.Capability, "unspecified_capability")
	title := firstNonEmpty(request.Title, "Heartbeat needs "+capability)
	if strings.TrimSpace(title) == "" {
		return AgentPersonalBacklogItem{}, false
	}
	summary := firstNonEmpty(request.Summary, request.Reason, result.Summary, title)
	dedupKey := firstNonEmpty(request.RequestID, internalHeartbeatActionRequestDedupKey(spec, request))
	evidence := append([]string(nil), request.EvidenceRefs...)
	if strings.TrimSpace(session.SessionID) != "" {
		evidence = append(evidence, "internal_session:"+strings.TrimSpace(session.SessionID))
	}
	return AgentPersonalBacklogItem{
		DedupKey:      dedupKey,
		HeartbeatID:   spec.ID,
		Kind:          "heartbeat_action_request",
		Status:        "open",
		Title:         title,
		Summary:       summary,
		Score:         request.Score,
		EvidenceRefs:  evidence,
		LastSessionID: strings.TrimSpace(session.SessionID),
		Meta: map[string]string{
			"heartbeat_kind":              firstNonEmpty(spec.Kind, session.HeartbeatKind),
			"finding_source":              internalHeartbeatActionRequestSource,
			"action_request_capability":   capability,
			"action_request_tool_suite":   strings.TrimSpace(request.ToolSuite),
			"action_requires_task_loop":   fmt.Sprint(request.RequiresTaskLoop),
			"action_requires_human_input": fmt.Sprint(request.RequiresHumanInput),
			"target_project_id":           strings.TrimSpace(request.ProjectID),
			"target_project_lane":         strings.TrimSpace(request.ProjectLane),
			"block_promote":               fmt.Sprint(policy.LocalOnly || !policy.AllowTaskSubmit),
			"session_outcome":             strings.TrimSpace(session.Outcome),
			"result_outcome":              strings.TrimSpace(result.Outcome),
			"result_contract":             strings.TrimSpace(result.ContractVersion),
			"finding_reason":              strings.TrimSpace(request.Reason),
			"finding_promote":             fmt.Sprint(request.Promote),
			"policy_local_only":           fmt.Sprint(policy.LocalOnly),
			"policy_allow_task_submit":    fmt.Sprint(policy.AllowTaskSubmit),
			"promotion_signals":           strings.Join(policy.PromotionSignals, ","),
			"output_contracts":            strings.Join(policy.OutputContracts, ","),
			"observed_at":                 now.UTC().Format(time.RFC3339Nano),
		},
	}, true
}

func internalHeartbeatFindingDedupKey(spec AgentHeartbeatSpec, finding InternalHeartbeatFinding) string {
	seed := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(spec.ID)),
		strings.ToLower(strings.TrimSpace(spec.Kind)),
		strings.ToLower(strings.TrimSpace(finding.Kind)),
		strings.ToLower(strings.TrimSpace(finding.Title)),
		strings.ToLower(strings.TrimSpace(finding.Summary)),
	}, "\x00")
	return "heartbeat:" + sanitizeRefSegment(firstNonEmpty(spec.ID, "heartbeat")) + ":" + shortRefHash(seed)
}

func internalHeartbeatActionRequestDedupKey(spec AgentHeartbeatSpec, request InternalHeartbeatActionRequest) string {
	seed := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(spec.ID)),
		strings.ToLower(strings.TrimSpace(spec.Kind)),
		strings.ToLower(strings.TrimSpace(request.Capability)),
		strings.ToLower(strings.TrimSpace(request.ToolSuite)),
		strings.ToLower(strings.TrimSpace(request.Title)),
		strings.ToLower(strings.TrimSpace(request.Summary)),
		strings.ToLower(strings.TrimSpace(request.Reason)),
	}, "\x00")
	return "heartbeat-action:" + sanitizeRefSegment(firstNonEmpty(spec.ID, "heartbeat")) + ":" + shortRefHash(seed)
}

func internalHeartbeatSessionSummaries(sessions []AgentInternalSessionRecord, limit int) []InternalHeartbeatSessionSummary {
	if limit <= 0 || len(sessions) == 0 {
		return nil
	}
	start := 0
	if len(sessions) > limit {
		start = len(sessions) - limit
	}
	out := make([]InternalHeartbeatSessionSummary, 0, len(sessions)-start)
	for _, session := range sessions[start:] {
		out = append(out, InternalHeartbeatSessionSummary{
			SessionID:   strings.TrimSpace(session.SessionID),
			HeartbeatID: strings.TrimSpace(session.HeartbeatID),
			Status:      strings.TrimSpace(session.Status),
			Outcome:     strings.TrimSpace(session.Outcome),
			Summary:     strings.TrimSpace(session.Summary),
			StartedAt:   strings.TrimSpace(session.StartedAt),
			EndedAt:     strings.TrimSpace(session.EndedAt),
		})
	}
	return out
}

func internalHeartbeatBacklogSummaries(items []AgentPersonalBacklogItem) []InternalHeartbeatBacklogSummary {
	if len(items) == 0 {
		return nil
	}
	out := make([]InternalHeartbeatBacklogSummary, 0, len(items))
	for _, item := range items {
		out = append(out, InternalHeartbeatBacklogSummary{
			ItemID:                   strings.TrimSpace(item.ItemID),
			DedupKey:                 strings.TrimSpace(item.DedupKey),
			HeartbeatID:              strings.TrimSpace(item.HeartbeatID),
			Kind:                     strings.TrimSpace(item.Kind),
			Status:                   strings.TrimSpace(item.Status),
			Title:                    strings.TrimSpace(item.Title),
			Summary:                  strings.TrimSpace(item.Summary),
			Score:                    item.Score,
			SeenCount:                item.SeenCount,
			CreatedAt:                strings.TrimSpace(item.CreatedAt),
			LastSeenAt:               strings.TrimSpace(item.LastSeenAt),
			EvidenceRefs:             append([]string(nil), item.EvidenceRefs...),
			Source:                   internalHeartbeatSafeBacklogMeta(item.Meta, "finding_source", 80),
			ActionCapability:         internalHeartbeatSafeBacklogMeta(item.Meta, "action_request_capability", 80),
			ActionToolSuite:          internalHeartbeatSafeBacklogMeta(item.Meta, "action_request_tool_suite", 80),
			ActionRequiresTaskLoop:   strings.EqualFold(strings.TrimSpace(item.Meta["action_requires_task_loop"]), "true"),
			ActionRequiresHumanInput: strings.EqualFold(strings.TrimSpace(item.Meta["action_requires_human_input"]), "true"),
			TargetProjectID:          internalHeartbeatSafeBacklogMeta(item.Meta, "target_project_id", 80),
			TargetProjectLane:        internalHeartbeatSafeBacklogMeta(item.Meta, "target_project_lane", 80),
			PromotionBlocked:         strings.EqualFold(strings.TrimSpace(item.Meta["block_promote"]), "true"),
		})
	}
	return out
}

func internalHeartbeatSafeBacklogMeta(meta map[string]string, key string, limit int) string {
	if len(meta) == 0 {
		return ""
	}
	value := strings.TrimSpace(meta[key])
	if value == "" {
		return ""
	}
	return sanitizeRefSegment(internalHeartbeatSafePublicText(value, limit))
}

func internalHeartbeatSpecByID(anatomy AgentAnatomyConfig, id string) (AgentHeartbeatSpec, bool) {
	id = strings.TrimSpace(id)
	for _, spec := range anatomy.Heartbeats {
		if strings.TrimSpace(spec.ID) == id {
			return normalizeAgentHeartbeatSpec(spec), true
		}
	}
	return AgentHeartbeatSpec{}, false
}

func copyHeartbeatLastRun(in map[string]time.Time) map[string]time.Time {
	out := map[string]time.Time{}
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key != "" && !value.IsZero() {
			out[key] = value
		}
	}
	return out
}

func internalSessionRecordByID(state AgentInternalSessionState, sessionID string) (AgentInternalSessionRecord, bool) {
	sessionID = strings.TrimSpace(sessionID)
	for _, session := range state.Sessions {
		if strings.TrimSpace(session.SessionID) == sessionID {
			return session, true
		}
	}
	return AgentInternalSessionRecord{}, false
}

func stringInSet(value string, candidates ...string) bool {
	value = strings.TrimSpace(value)
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == value {
			return true
		}
	}
	return false
}
