package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

type RhizomeClient struct {
	endpoint string
	token    string
	client   *http.Client
}

const maxAgentRespondResponseBytes = 256 * 1024

func NewRhizomeClient(endpoint, token string) *RhizomeClient {
	return &RhizomeClient{
		endpoint: strings.TrimSpace(endpoint),
		token:    strings.TrimSpace(token),
		client:   &http.Client{Timeout: 2 * time.Minute},
	}
}

func (c *RhizomeClient) SetToken(token string) {
	if c == nil {
		return
	}
	c.token = strings.TrimSpace(token)
}

func (c *RhizomeClient) SetEndpoint(endpoint string) {
	if c == nil {
		return
	}
	c.endpoint = strings.TrimSpace(endpoint)
}

type rhizomeRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rhizomeRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

const (
	rhizomeRPCCodeDocumentConflict      = -32020
	rhizomeRPCCodeInvalidPollCursor     = -32021
	rhizomeRPCCodeOperatorQueueNotFound = -32022
)

var rhizomeWorkNextRetryDelay = func(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Second
	}
	delay := time.Second
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	if delay > 4*time.Second {
		return 4 * time.Second
	}
	return delay
}

type RhizomeRPCError struct {
	Method  string
	Code    int
	Message string
}

func (e *RhizomeRPCError) Error() string {
	if e == nil {
		return "<nil>"
	}
	method := strings.TrimSpace(e.Method)
	message := strings.TrimSpace(e.Message)
	if method == "" {
		if message == "" {
			return "rpc error"
		}
		return "rpc: " + message
	}
	if message == "" {
		return "rpc " + method
	}
	return fmt.Sprintf("rpc %s: %s", method, message)
}

func isRhizomeRPCErrorCode(err error, code int) bool {
	var rpcErr *RhizomeRPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == code
}

type AgentRegisterInput struct {
	WorkspaceID       string   `json:"workspace_id"`
	WorkspaceName     string   `json:"workspace_name,omitempty"`
	WorkspacePassword string   `json:"workspace_password,omitempty"`
	HostURL           string   `json:"host_url,omitempty"`
	AgentID           string   `json:"agent_id"`
	GroupID           string   `json:"group_id,omitempty"`
	DisplayName       string   `json:"display_name"`
	Role              string   `json:"role,omitempty"`
	OwnerUserID       string   `json:"owner_user_id"`
	Capabilities      []string `json:"capabilities,omitempty"`
	Status            string   `json:"status,omitempty"`
	ProtocolVersion   string   `json:"protocol_version,omitempty"`
	Summary           string   `json:"summary,omitempty"`
}

type AgentRegisterResult struct {
	Agent         AgentRecord `json:"agent"`
	AgentID       string      `json:"agent_id,omitempty"`
	DisplayName   string      `json:"display_name,omitempty"`
	Token         string      `json:"token,omitempty"`
	WorkspaceID   string      `json:"workspace_id,omitempty"`
	WorkspaceName string      `json:"workspace_name,omitempty"`
	HostURL       string      `json:"host_url,omitempty"`
}

type workspaceAuthAgentRegisterParams struct {
	WorkspaceID       string   `json:"workspace_id"`
	WorkspaceName     string   `json:"workspace_name,omitempty"`
	WorkspacePassword string   `json:"workspace_password"`
	HostURL           string   `json:"host_url,omitempty"`
	AgentID           string   `json:"agent_id"`
	AgentName         string   `json:"agent_name,omitempty"`
	DisplayName       string   `json:"display_name"`
	GroupID           string   `json:"group_id,omitempty"`
	OwnerUserID       string   `json:"owner_user_id"`
	Role              string   `json:"role,omitempty"`
	Status            string   `json:"status,omitempty"`
	ProtocolVersion   string   `json:"protocol_version,omitempty"`
	Capabilities      []string `json:"capabilities,omitempty"`
	Summary           string   `json:"summary,omitempty"`
}

type AgentHeartbeatInput struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Status      string `json:"status"`
	Summary     string `json:"summary,omitempty"`
}

type AgentProfileUpdateInput struct {
	WorkspaceID    string         `json:"workspace_id"`
	AgentID        string         `json:"agent_id"`
	ActorID        string         `json:"actor_id"`
	Bio            string         `json:"bio,omitempty"`
	Specialization string         `json:"specialization,omitempty"`
	OwnerName      string         `json:"owner_name,omitempty"`
	OwnerContact   string         `json:"owner_contact,omitempty"`
	AvatarURL      string         `json:"avatar_url,omitempty"`
	Links          []string       `json:"links,omitempty"`
	Tags           []string       `json:"tags,omitempty"`
	ToolsAccess    []string       `json:"tools_access,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type AgentRequestInput struct {
	WorkspaceID string `json:"workspace_id"`
	FromAgentID string `json:"from_agent_id"`
	ToAgentID   string `json:"to_agent_id"`
	Method      string `json:"method"`
	PayloadJSON string `json:"payload_json,omitempty"`
	TimeoutSec  int    `json:"timeout_sec,omitempty"`
}

type AgentRequestCreateResult struct {
	RequestID   string `json:"request_id"`
	WorkspaceID string `json:"workspace_id"`
	ToAgentID   string `json:"to_agent_id"`
	Status      string `json:"status"`
}

type ProjectCreateInput struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	CreatedBy   string `json:"created_by"`
}

type ProjectProfileUpdateInput struct {
	WorkspaceID             string `json:"workspace_id"`
	ProjectID               string `json:"project_id"`
	ActorID                 string `json:"actor_id"`
	Goal                    string `json:"goal,omitempty"`
	DesignDocID             string `json:"design_doc_id,omitempty"`
	ImplementationPlanDocID string `json:"implementation_plan_doc_id,omitempty"`
	RepoRequired            *bool  `json:"repo_required,omitempty"`
	RepoStatus              string `json:"repo_status,omitempty"`
	RepoURL                 string `json:"repo_url,omitempty"`
	RepoDefaultBranch       string `json:"repo_default_branch,omitempty"`
}

type ProjectRepositoryUpsertInput struct {
	WorkspaceID            string `json:"workspace_id"`
	ProjectID              string `json:"project_id"`
	ActorID                string `json:"actor_id"`
	RepoID                 string `json:"repo_id,omitempty"`
	RemoteURL              string `json:"remote_url,omitempty"`
	RemoteKind             string `json:"remote_kind,omitempty"`
	Owner                  string `json:"owner,omitempty"`
	Name                   string `json:"name,omitempty"`
	DefaultBranch          string `json:"default_branch,omitempty"`
	IntegrationBranch      string `json:"integration_branch,omitempty"`
	CredentialVaultEntryID string `json:"credential_vault_entry_id,omitempty"`
	RepoStatus             string `json:"repo_status,omitempty"`
	IsCanonical            *bool  `json:"is_canonical,omitempty"`
	CreatedByAgentID       string `json:"created_by_agent_id,omitempty"`
}

type ProjectLeadClaimInput struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	AgentID      string `json:"agent_id"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
	LeaseToken   string `json:"lease_token,omitempty"`
	Summary      string `json:"summary,omitempty"`
}

type ProjectPhaseTransitionInput struct {
	WorkspaceID      string `json:"workspace_id"`
	ProjectID        string `json:"project_id"`
	ActorID          string `json:"actor_id"`
	ToPhase          string `json:"to_phase"`
	Reason           string `json:"reason,omitempty"`
	CoordinationMode string `json:"coordination_mode,omitempty"`
}

type ProjectRoleAssignInput struct {
	WorkspaceID    string `json:"workspace_id"`
	ProjectID      string `json:"project_id"`
	ActorID        string `json:"actor_id"`
	AgentID        string `json:"agent_id"`
	RoleType       string `json:"role_type"`
	WriteScopeJSON string `json:"write_scope_json,omitempty"`
	Summary        string `json:"summary,omitempty"`
}

type ProjectGovernancePredicatesCheckInput struct {
	WorkspaceID       string   `json:"workspace_id"`
	ProjectID         string   `json:"project_id"`
	ChallengedAgentID string   `json:"challenged_agent_id"`
	StallPredicates   []string `json:"stall_predicates,omitempty"`
}

type ProjectGovernanceChallengeRaiseInput struct {
	WorkspaceID               string   `json:"workspace_id"`
	ProjectID                 string   `json:"project_id"`
	ActorID                   string   `json:"actor_id"`
	ChallengedAgentID         string   `json:"challenged_agent_id"`
	ChallengerAgentID         string   `json:"challenger_agent_id"`
	NominatedSuccessorAgentID string   `json:"nominated_successor_agent_id,omitempty"`
	StallPredicates           []string `json:"stall_predicates,omitempty"`
	EvidenceRefs              []string `json:"evidence_refs,omitempty"`
	ArgumentDocKey            string   `json:"argument_doc_key,omitempty"`
	TensionID                 string   `json:"tension_id,omitempty"`
	DefenseWindowSeconds      int      `json:"defense_window_seconds,omitempty"`
	VotingWindowSeconds       int      `json:"voting_window_seconds,omitempty"`
	MaxRounds                 int      `json:"max_rounds,omitempty"`
}

type ProjectGovernanceChallengeDefendInput struct {
	WorkspaceID         string `json:"workspace_id"`
	ActorID             string `json:"actor_id"`
	ChallengeID         string `json:"challenge_id"`
	Round               int    `json:"round,omitempty"`
	Stance              string `json:"stance"`
	DefenseDocKey       string `json:"defense_doc_key,omitempty"`
	VotingWindowSeconds int    `json:"voting_window_seconds,omitempty"`
}

type ProjectGovernanceVoteCastInput struct {
	WorkspaceID     string `json:"workspace_id"`
	ActorID         string `json:"actor_id"`
	ChallengeID     string `json:"challenge_id"`
	Round           int    `json:"round,omitempty"`
	VoterAgentID    string `json:"voter_agent_id"`
	Ballot          string `json:"ballot"`
	RationaleDocKey string `json:"rationale_doc_key,omitempty"`
}

type ProjectGovernanceChallengeTallyInput struct {
	WorkspaceID      string `json:"workspace_id"`
	ActorID          string `json:"actor_id"`
	ChallengeID      string `json:"challenge_id"`
	ReassignEnabled  bool   `json:"reassign_enabled,omitempty"`
	LeadLeaseSeconds int    `json:"lead_lease_seconds,omitempty"`
}

type ProjectGovernanceChallengeListInput struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id,omitempty"`
	State       string `json:"state,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type TaskProjectFieldsPutInput struct {
	WorkspaceID         string  `json:"workspace_id"`
	TaskID              string  `json:"task_id"`
	ProjectID           *string `json:"project_id,omitempty"`
	TaskKind            *string `json:"task_kind,omitempty"`
	ProjectLane         *string `json:"project_lane,omitempty"`
	RequiresProjectGate *bool   `json:"requires_project_gate,omitempty"`
	ActorID             string  `json:"actor_id"`
}

type SessionEventInput struct {
	WorkspaceID         string        `json:"workspace_id"`
	SessionID           string        `json:"session_id"`
	AgentID             string        `json:"agent_id"`
	TaskID              string        `json:"task_id,omitempty"`
	Summary             string        `json:"summary"`
	Status              string        `json:"status,omitempty"`
	OwnerScope          string        `json:"owner_scope,omitempty"`
	BlockedOn           []BlockedRef  `json:"blocked_on,omitempty"`
	DecisionNeededFrom  string        `json:"decision_needed_from,omitempty"`
	DecisionType        string        `json:"decision_type,omitempty"`
	KeepSessionActive   *bool         `json:"keep_session_active,omitempty"`
	HandoffTo           string        `json:"handoff_to,omitempty"`
	RelatedDocKeys      []string      `json:"related_doc_keys,omitempty"`
	RelatedArtifactRefs []ArtifactRef `json:"related_artifact_refs,omitempty"`
	Iterations          int           `json:"iterations,omitempty"`
	TotalInputTokens    int           `json:"total_input_tokens,omitempty"`
	TotalOutputTokens   int           `json:"total_output_tokens,omitempty"`
	ToolCalls           int           `json:"tool_calls,omitempty"`
	UpdatedAt           string        `json:"updated_at,omitempty"`
}

type TaskClaimInput struct {
	WorkspaceID          string `json:"workspace_id"`
	AgentID              string `json:"agent_id"`
	TaskID               string `json:"task_id"`
	ProjectRoleID        string `json:"project_role_id,omitempty"`
	RepoID               string `json:"repo_id,omitempty"`
	CheckoutID           string `json:"checkout_id,omitempty"`
	BranchID             string `json:"branch_id,omitempty"`
	WriteScopeJSON       string `json:"write_scope_json,omitempty"`
	CoordinationMode     string `json:"coordination_mode,omitempty"`
	Summary              string `json:"summary,omitempty"`
	SelectedFromFrontier bool   `json:"selected_from_frontier,omitempty"`
	FrontierGenerationID string `json:"frontier_generation_id,omitempty"`
	SelfFitSummary       string `json:"self_fit_summary,omitempty"`
}

type TaskFrontierDecisionInput struct {
	WorkspaceID          string `json:"workspace_id"`
	AgentID              string `json:"agent_id"`
	FrontierGenerationID string `json:"frontier_generation_id"`
	DecisionState        string `json:"decision_state"`
	SelectedTaskID       string `json:"selected_task_id,omitempty"`
	Summary              string `json:"summary,omitempty"`
}

type ProjectCheckoutRegisterInput struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	CheckoutID   string `json:"checkout_id,omitempty"`
	RepoID       string `json:"repo_id"`
	MachineID    string `json:"machine_id"`
	MachineLabel string `json:"machine_label,omitempty"`
	OwnerUserID  string `json:"owner_user_id,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	LocalPath    string `json:"local_path"`
	CheckoutKind string `json:"checkout_kind,omitempty"`
	BranchName   string `json:"branch_name,omitempty"`
	BaseBranch   string `json:"base_branch,omitempty"`
	HeadSHA      string `json:"head_sha,omitempty"`
	BaseSHA      string `json:"base_sha,omitempty"`
	DirtyState   string `json:"dirty_state,omitempty"`
	ActiveTaskID string `json:"active_task_id,omitempty"`
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID string `json:"active_claim_id,omitempty"`
	Status        string `json:"status,omitempty"`
	LastSeenAt    string `json:"last_seen_at,omitempty"`
}

type ProjectBranchRegisterInput struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	BranchID     string `json:"branch_id,omitempty"`
	RepoID       string `json:"repo_id"`
	CheckoutID   string `json:"checkout_id,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	ActiveTaskID string `json:"active_task_id,omitempty"`
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID  string `json:"active_claim_id,omitempty"`
	BranchName     string `json:"branch_name"`
	BranchKind     string `json:"branch_kind,omitempty"`
	BaseBranch     string `json:"base_branch,omitempty"`
	HeadSHA        string `json:"head_sha,omitempty"`
	BaseSHA        string `json:"base_sha,omitempty"`
	WriteScopeJSON string `json:"write_scope_json,omitempty"`
	ReviewDocKey   string `json:"review_doc_key,omitempty"`
	Status         string `json:"status,omitempty"`
}

type ProjectPatchQueueSubmitInput struct {
	WorkspaceID              string            `json:"workspace_id"`
	ProjectID                string            `json:"project_id"`
	ActorID                  string            `json:"actor_id"`
	QueueID                  string            `json:"queue_id,omitempty"`
	ItemID                   string            `json:"item_id,omitempty"`
	RepoID                   string            `json:"repo_id"`
	BranchID                 string            `json:"branch_id"`
	ReviewDocKey             string            `json:"review_doc_key,omitempty"`
	SupersedesQueueID        string            `json:"supersedes_queue_id,omitempty"`
	SupersedesItemID         string            `json:"supersedes_item_id,omitempty"`
	EvidenceDocKey           string            `json:"evidence_doc_key,omitempty"`
	RepoAuthorityMode        string            `json:"repo_authority_mode,omitempty"`
	PathsetJSON              string            `json:"pathset_json,omitempty"`
	BaseRef                  string            `json:"base_ref,omitempty"`
	BaseSHA                  string            `json:"base_sha,omitempty"`
	HeadSHA                  string            `json:"head_sha,omitempty"`
	AutoMerge                bool              `json:"auto_merge,omitempty"`
	TaskID                   string            `json:"task_id,omitempty"`
	SessionID                string            `json:"session_id,omitempty"`
	RunID                    string            `json:"run_id,omitempty"`
	AgentID                  string            `json:"agent_id,omitempty"`
	PrincipalType            string            `json:"principal_type,omitempty"`
	PrincipalID              string            `json:"principal_id,omitempty"`
	CapabilitySnapshotID     string            `json:"capability_snapshot_id,omitempty"`
	CapabilitySnapshotSchema string            `json:"capability_snapshot_schema,omitempty"`
	RepoRoot                 string            `json:"repo_root,omitempty"`
	BaseTreeHash             string            `json:"base_tree_hash,omitempty"`
	BaseFileHashes           map[string]string `json:"base_file_hashes,omitempty"`
	BaseFileHashesJSON       string            `json:"base_file_hashes_json,omitempty"`
	ContextDigest            string            `json:"context_digest,omitempty"`
	RepoLeaseID              string            `json:"repo_lease_id,omitempty"`
	LeaseTerm                int64             `json:"lease_term,omitempty"`
	OperationID              string            `json:"operation_id,omitempty"`
	OperationKind            string            `json:"operation_kind,omitempty"`
	MaxAttempts              int               `json:"max_attempts,omitempty"`
}

type ProjectPatchQueueSupersedeInput struct {
	WorkspaceID    string `json:"workspace_id"`
	ProjectID      string `json:"project_id"`
	ActorID        string `json:"actor_id"`
	QueueID        string `json:"queue_id"`
	ItemID         string `json:"item_id"`
	NewItemID      string `json:"new_item_id"`
	EvidenceDocKey string `json:"evidence_doc_key"`
	TaskID         string `json:"task_id,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	RunID          string `json:"run_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	PrincipalType  string `json:"principal_type,omitempty"`
	PrincipalID    string `json:"principal_id,omitempty"`
	MaxAttempts    int    `json:"max_attempts,omitempty"`
}

type ProjectPatchQueueListInput struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	RepoID      string `json:"repo_id,omitempty"`
	BranchID    string `json:"branch_id,omitempty"`
	State       string `json:"state,omitempty"`
}

type ProjectPatchQueueClaimInput struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	QueueID      string `json:"queue_id"`
	ItemID       string `json:"item_id"`
	ClaimToken   string `json:"claim_token,omitempty"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
}

type ProjectPatchQueueOperationBindInput struct {
	WorkspaceID       string `json:"workspace_id"`
	ProjectID         string `json:"project_id"`
	ActorID           string `json:"actor_id"`
	QueueID           string `json:"queue_id"`
	ItemID            string `json:"item_id"`
	OperationID       string `json:"operation_id,omitempty"`
	OperationKind     string `json:"operation_kind,omitempty"`
	MutationPathsJSON string `json:"mutation_paths_json,omitempty"`
	ClaimToken        string `json:"claim_token"`
}

type ProjectPatchQueueCASRecordInput struct {
	WorkspaceID  string                 `json:"workspace_id"`
	ProjectID    string                 `json:"project_id"`
	ActorID      string                 `json:"actor_id"`
	QueueID      string                 `json:"queue_id"`
	ItemID       string                 `json:"item_id"`
	CASResult    CASPatchApplyResult    `json:"cas_result"`
	TestEvidence PatchQueueTestEvidence `json:"test_evidence"`
	ClaimToken   string                 `json:"claim_token"`
}

type ProjectPatchQueueMaterializationRecordInput struct {
	WorkspaceID     string               `json:"workspace_id"`
	ProjectID       string               `json:"project_id"`
	ActorID         string               `json:"actor_id"`
	QueueID         string               `json:"queue_id"`
	ItemID          string               `json:"item_id"`
	Materialization PatchMaterialization `json:"materialization"`
	ClaimToken      string               `json:"claim_token"`
}

type ProjectPatchQueueRollbackRecordInput struct {
	WorkspaceID      string             `json:"workspace_id"`
	ProjectID        string             `json:"project_id"`
	ActorID          string             `json:"actor_id"`
	QueueID          string             `json:"queue_id"`
	ItemID           string             `json:"item_id"`
	RollbackEvidence PatchQueueRollback `json:"rollback_evidence"`
	ClaimToken       string             `json:"claim_token"`
}

type ProjectPatchQueueReviewerAdvisoryRecordInput struct {
	WorkspaceID      string                     `json:"workspace_id"`
	ProjectID        string                     `json:"project_id"`
	ActorID          string                     `json:"actor_id"`
	QueueID          string                     `json:"queue_id"`
	ItemID           string                     `json:"item_id"`
	ReviewerAdvisory PatchQueueReviewerAdvisory `json:"reviewer_advisory"`
	ClaimToken       string                     `json:"claim_token"`
}

type ProjectPatchQueueOperatorEnablementRecordInput struct {
	WorkspaceID        string                       `json:"workspace_id"`
	ProjectID          string                       `json:"project_id"`
	ActorID            string                       `json:"actor_id"`
	QueueID            string                       `json:"queue_id"`
	ItemID             string                       `json:"item_id"`
	OperatorEnablement PatchQueueOperatorEnablement `json:"operator_enablement"`
	ClaimToken         string                       `json:"claim_token"`
}

type ProjectPatchQueueReleaseInput struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	QueueID     string `json:"queue_id"`
	ItemID      string `json:"item_id"`
	ClaimToken  string `json:"claim_token"`
}

type ProjectPatchQueueDecisionInput struct {
	WorkspaceID          string   `json:"workspace_id"`
	ProjectID            string   `json:"project_id"`
	ActorID              string   `json:"actor_id"`
	QueueID              string   `json:"queue_id"`
	ItemID               string   `json:"item_id"`
	Decision             string   `json:"decision"`
	DecisionDocKey       string   `json:"decision_doc_key,omitempty"`
	DecisionSummary      string   `json:"decision_summary"`
	CheckedSourceDocKeys []string `json:"checked_source_doc_keys,omitempty"`
	ClaimToken           string   `json:"claim_token"`
}

type ProjectPatchQueueReviewTaskReconcileInput struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	QueueID     string `json:"queue_id"`
	ItemID      string `json:"item_id"`
}

type ProjectPatchQueueDecisionContinuationConsumeInput struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	OutboxID    string `json:"outbox_id,omitempty"`
	QueueID     string `json:"queue_id,omitempty"`
	ItemID      string `json:"item_id,omitempty"`
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

type TaskReleaseInput struct {
	WorkspaceID           string `json:"workspace_id"`
	AgentID               string `json:"agent_id"`
	TaskID                string `json:"task_id"`
	Reason                string `json:"reason,omitempty"`
	SessionTransitionKind string `json:"session_transition_kind,omitempty"`
}

type CoalitionOfferInput struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	Role        string `json:"role"`
}

type CoalitionLeaveInput struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	Reason      string `json:"reason,omitempty"`
}

type CoalitionSeekInput struct {
	WorkspaceID    string   `json:"workspace_id"`
	TaskID         string   `json:"task_id"`
	AgentID        string   `json:"agent_id"`
	RequiredSkills []string `json:"required_skills,omitempty"`
	Reason         string   `json:"reason,omitempty"`
}

type CoalitionInviteInput struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	ActorID     string `json:"actor_id"`
	AgentID     string `json:"agent_id"`  // inviter
	TargetID    string `json:"target_id"` // invitee
	Role        string `json:"role,omitempty"`
}

type CoalitionKickInput struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	ActorID     string `json:"actor_id"`
	AgentID     string `json:"agent_id"`  // kicker (steward/primary)
	TargetID    string `json:"target_id"` // kicked
	Reason      string `json:"reason,omitempty"`
}

type CoalitionStatusInput struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
}

type ReviewerRouteInput struct {
	WorkspaceID           string   `json:"workspace_id"`
	BundleID              string   `json:"bundle_id,omitempty"`
	GeneratorAgentID      string   `json:"generator_agent_id"`
	AvailableReviewers    []string `json:"available_reviewers"`
	IsMultiPatch          bool     `json:"is_multi_patch"`
	ImpactScore           float64  `json:"impact_score"`
	ContradictionPressure float64  `json:"contradiction_pressure"`
	HasActiveDissent      bool     `json:"has_active_dissent"`
	TouchesHardConstraint bool     `json:"touches_hard_constraint"`
	ClusterMode           string   `json:"cluster_mode"`
	MergeRisk             float64  `json:"merge_risk"`
}

type ReviewerScarcityInput struct {
	WorkspaceID string `json:"workspace_id"`
}

type TaskCompleteInput struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	TaskID      string `json:"task_id"`
	Summary     string `json:"summary,omitempty"`
}

type TaskBlockInput struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	TaskID      string `json:"task_id"`
	Reason      string `json:"reason,omitempty"`
}

type TaskCloseInput struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
	ActorID     string `json:"actor_id,omitempty"`
	Resolution  string `json:"resolution,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type TaskSubmitInput struct {
	WorkspaceID         string         `json:"workspace_id"`
	TaskID              string         `json:"task_id,omitempty"`
	OwnerUserID         string         `json:"owner_user_id,omitempty"`
	Priority            string         `json:"priority,omitempty"`
	Title               string         `json:"title"`
	Description         string         `json:"description"`
	TaskKind            string         `json:"task_kind,omitempty"`
	TaskTemplate        string         `json:"task_template,omitempty"`
	TaskClass           string         `json:"task_class,omitempty"`
	TaskClassSource     string         `json:"task_class_source,omitempty"`
	ProjectID           string         `json:"project_id,omitempty"`
	ProjectLane         string         `json:"project_lane,omitempty"`
	RequiresProjectGate *bool          `json:"requires_project_gate,omitempty"`
	DependencyTaskIDs   []string       `json:"dependency_task_ids,omitempty"`
	RelatedTaskIDs      []string       `json:"related_task_ids,omitempty"`
	WriteScopeHints     []string       `json:"write_scope_hints,omitempty"`
	TaskRequirements    map[string]any `json:"task_requirements,omitempty"`
	Graph               map[string]any `json:"graph,omitempty"`
	Tags                []string       `json:"tags,omitempty"`
	LinkedBy            string         `json:"linked_by,omitempty"`
}

type TaskSubmitResult struct {
	TaskID               string   `json:"task_id"`
	WorkspaceID          string   `json:"workspace_id"`
	Status               string   `json:"status"`
	TaskRequirementsJSON string   `json:"task_requirements_json,omitempty"`
	Suggested            []string `json:"suggested_agents,omitempty"`
}

type TaskHydrationInput struct {
	WorkspaceID      string   `json:"workspace_id,omitempty"`
	TaskID           string   `json:"task_id"`
	DocKeys          []string `json:"doc_keys,omitempty"`
	IncludeAllDocs   *bool    `json:"include_all_docs,omitempty"`
	UpdatesLimit     int      `json:"updates_limit,omitempty"`
	ArtifactLimit    int      `json:"artifact_limit,omitempty"`
	RelatedTaskLimit int      `json:"related_task_limit,omitempty"`
}

type WorkNextInput struct {
	WorkspaceID        string   `json:"workspace_id"`
	AgentID            string   `json:"agent_id"`
	IncludeHydration   *bool    `json:"include_hydration,omitempty"`
	IncludePacket      *bool    `json:"include_packet,omitempty"`
	IncludeAdvisory    *bool    `json:"include_advisory,omitempty"`
	EnableTaskFrontier *bool    `json:"enable_task_frontier,omitempty"`
	FrontierLimit      int      `json:"frontier_limit,omitempty"`
	DocKeys            []string `json:"doc_keys,omitempty"`
	IncludeAllDocs     *bool    `json:"include_all_docs,omitempty"`
	UpdatesLimit       int      `json:"updates_limit,omitempty"`
	ArtifactLimit      int      `json:"artifact_limit,omitempty"`
	RelatedTaskLimit   int      `json:"related_task_limit,omitempty"`
	SessionLimit       int      `json:"session_limit,omitempty"`
	Trigger            string   `json:"trigger,omitempty"`
	CandidateTaskID    string   `json:"candidate_task_id,omitempty"`
	CandidateSessionID string   `json:"candidate_session_id,omitempty"`
	CoordinationMode   string   `json:"coordination_mode,omitempty"`
}

type MemoryCoherenceInput struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
}

type MemoryPromotionInput struct {
	WorkspaceID string `json:"workspace_id"`
	Limit       int    `json:"limit,omitempty"`
}

type MessagePollInput struct {
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	AfterCreatedAt string `json:"after_created_at,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	TimeoutSec     int    `json:"timeout_sec,omitempty"`
	LookbackHours  int    `json:"lookback_hours,omitempty"`
}

type NewsPollInput struct {
	WorkspaceID    string `json:"workspace_id"`
	AfterCreatedAt string `json:"after_created_at,omitempty"`
	AfterNewsID    string `json:"after_news_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	LookbackHours  int    `json:"lookback_hours,omitempty"`
}

type ControlClusterInput struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id"`
}

type CorridorClusterInput struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id"`
}

type TensionFrontierInput struct {
	WorkspaceID    string `json:"workspace_id"`
	TensionType    string `json:"tension_type,omitempty"`
	LifecycleState string `json:"lifecycle_state,omitempty"`
	ReviewStatus   string `json:"review_status,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type TensionRefreshInput struct {
	WorkspaceID    string `json:"workspace_id"`
	ActorID        string `json:"actor_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	ClusterLimit   int    `json:"cluster_limit,omitempty"`
}

type LocusBundleInput struct {
	WorkspaceID    string   `json:"workspace_id"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	FrontierLimit  int      `json:"frontier_limit,omitempty"`
}

type MessageAckInput struct {
	WorkspaceID string   `json:"workspace_id"`
	AgentID     string   `json:"agent_id"`
	MessageIDs  []string `json:"message_ids"`
}

type UpdatePostInput struct {
	WorkspaceID   string `json:"workspace_id"`
	AgentID       string `json:"agent_id"`
	UpdateType    string `json:"update_type"`
	Summary       string `json:"summary"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	RequiresHuman bool   `json:"requires_human,omitempty"`
}

type WorkspaceDocPutInput struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	UpdatedBy   string `json:"updated_by"`
	ExpectedSHA string `json:"expected_sha,omitempty"`
}

type WorkspaceMemoryNodeTouchInput struct {
	WorkspaceID string `json:"workspace_id"`
	NodeID      string `json:"node_id"`
	Trusted     bool   `json:"trusted"`
}

type WorkspaceMemoryWriteInput struct {
	WorkspaceID string   `json:"workspace_id"`
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

type WorkspaceArtifactWriteInput struct {
	ArtifactID   string `json:"artifact_id,omitempty"`
	WorkspaceID  string `json:"workspace_id"`
	TaskID       string `json:"task_id,omitempty"`
	UpdateID     string `json:"update_id,omitempty"`
	Title        string `json:"title"`
	ArtifactRef  string `json:"artifact_ref"`
	Kind         string `json:"kind,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	CreatedBy    string `json:"created_by"`
	MetadataJSON string `json:"metadata_json,omitempty"`
}

type WorkspaceArtifactListInput struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id,omitempty"`
	UpdateID    string `json:"update_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type ExecutionRunWriteInput struct {
	WorkspaceID string `json:"workspace_id"`
	RunID       string `json:"run_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	Title       string `json:"title"`
	Summary     string `json:"summary,omitempty"`
	Status      string `json:"status,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
}

type ExecutionAgentRunsCancelInput struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Summary     string `json:"summary,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
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

type ExecutionStepWriteInput struct {
	WorkspaceID  string         `json:"workspace_id"`
	StepID       string         `json:"step_id,omitempty"`
	RunID        string         `json:"run_id"`
	ParentStepID string         `json:"parent_step_id,omitempty"`
	Phase        string         `json:"phase,omitempty"`
	Title        string         `json:"title"`
	Summary      string         `json:"summary,omitempty"`
	Status       string         `json:"status,omitempty"`
	SortOrder    int            `json:"sort_order,omitempty"`
	Evidence     []string       `json:"evidence,omitempty"`
	Verification map[string]any `json:"verification,omitempty"`
}

type PolicyCheckInput struct {
	WorkspaceID string `json:"workspace_id"`
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	Capability  string `json:"capability"`
	ToolID      string `json:"tool_id,omitempty"`
}

type OperatorQueueUpsertInput struct {
	WorkspaceID       string `json:"workspace_id"`
	QueueID           string `json:"queue_id,omitempty"`
	QueueKey          string `json:"queue_key"`
	QueueType         string `json:"queue_type,omitempty"`
	Title             string `json:"title"`
	Summary           string `json:"summary,omitempty"`
	Details           string `json:"details,omitempty"`
	AssignedTo        string `json:"assigned_to,omitempty"`
	Urgency           string `json:"urgency,omitempty"`
	SourceKind        string `json:"source_kind,omitempty"`
	SourceID          string `json:"source_id,omitempty"`
	TaskID            string `json:"task_id,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	AgentID           string `json:"agent_id,omitempty"`
	KeepSessionActive *bool  `json:"keep_session_active,omitempty"`
	DueAt             string `json:"due_at,omitempty"`
	CurrentRevision   int64  `json:"current_revision,omitempty"`
	CurrentUpdatedAt  string `json:"current_updated_at,omitempty"`
}

type ExternalGateRequestInput struct {
	WorkspaceID       string `json:"workspace_id"`
	RequestKey        string `json:"request_key"`
	GateType          string `json:"gate_type"`
	Title             string `json:"title"`
	Summary           string `json:"summary,omitempty"`
	Details           string `json:"details,omitempty"`
	AssignedTo        string `json:"assigned_to,omitempty"`
	Urgency           string `json:"urgency,omitempty"`
	SourceKind        string `json:"source_kind,omitempty"`
	SourceID          string `json:"source_id,omitempty"`
	TaskID            string `json:"task_id,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	AgentID           string `json:"agent_id,omitempty"`
	KeepSessionActive *bool  `json:"keep_session_active,omitempty"`
	DueAt             string `json:"due_at,omitempty"`
	CurrentRevision   int64  `json:"current_revision,omitempty"`
	CurrentUpdatedAt  string `json:"current_updated_at,omitempty"`
}

type OperatorQueueResolveInput struct {
	WorkspaceID      string `json:"workspace_id"`
	QueueID          string `json:"queue_id,omitempty"`
	QueueKey         string `json:"queue_key,omitempty"`
	Status           string `json:"status,omitempty"`
	ResolvedBy       string `json:"resolved_by"`
	Resolution       string `json:"resolution,omitempty"`
	CurrentRevision  int64  `json:"current_revision,omitempty"`
	CurrentUpdatedAt string `json:"current_updated_at,omitempty"`
}

type OperatorQueueRecord struct {
	QueueID     string `json:"queue_id"`
	WorkspaceID string `json:"workspace_id"`
	QueueKey    string `json:"queue_key"`
	Revision    int64  `json:"revision"`
	UpdatedAt   string `json:"updated_at"`
}

var errOperatorQueueLookupUnavailable = errors.New("operator queue lookup unavailable")

type KnowledgeClaimWriteInput struct {
	WorkspaceID       string   `json:"workspace_id"`
	ClaimID           string   `json:"claim_id,omitempty"`
	ClaimType         string   `json:"claim_type,omitempty"`
	Status            string   `json:"status,omitempty"`
	Subject           string   `json:"subject"`
	Body              string   `json:"body"`
	Summary           string   `json:"summary,omitempty"`
	Confidence        float64  `json:"confidence,omitempty"`
	SourceKind        string   `json:"source_kind,omitempty"`
	SourceID          string   `json:"source_id,omitempty"`
	MemoryID          string   `json:"memory_id,omitempty"`
	TaskID            string   `json:"task_id,omitempty"`
	SessionID         string   `json:"session_id,omitempty"`
	AgentID           string   `json:"agent_id,omitempty"`
	SupersedesClaimID string   `json:"supersedes_claim_id,omitempty"`
	ConflictsClaimID  string   `json:"conflicts_claim_id,omitempty"`
	Evidence          []string `json:"evidence,omitempty"`
	Tags              []string `json:"tags,omitempty"`
}

type MCPServerRecord struct {
	ServerID     string `json:"server_id"`
	WorkspaceID  string `json:"workspace_id"`
	DisplayName  string `json:"display_name"`
	Transport    string `json:"transport"`
	URL          string `json:"url,omitempty"`
	Command      string `json:"command,omitempty"`
	ArgsJSON     string `json:"args_json,omitempty"`
	EnvJSON      string `json:"env_json,omitempty"`
	HeadersJSON  string `json:"headers_json,omitempty"`
	Status       string `json:"status"`
	RegisteredBy string `json:"registered_by"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type MCPToolRecord struct {
	ServerID     string `json:"server_id"`
	ToolName     string `json:"tool_name"`
	Description  string `json:"description"`
	InputSchema  string `json:"input_schema"`
	DiscoveredAt string `json:"discovered_at"`
}

type MCPToolContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type MCPToolCallResult struct {
	ServerID string           `json:"server_id"`
	ToolName string           `json:"tool_name"`
	IsError  bool             `json:"is_error"`
	Content  []MCPToolContent `json:"content"`
}

type WorkspaceToolCallInput struct {
	ToolID              string         `json:"tool_id"`
	WorkspaceID         string         `json:"workspace_id"`
	Arguments           map[string]any `json:"arguments"`
	TimeoutSec          int            `json:"timeout_sec,omitempty"`
	ActorType           string         `json:"actor_type,omitempty"`
	ActorID             string         `json:"actor_id,omitempty"`
	RequestedCapability string         `json:"requested_capability,omitempty"`
	TaskID              string         `json:"task_id,omitempty"`
	SessionID           string         `json:"session_id,omitempty"`
	RunID               string         `json:"run_id,omitempty"`
}

type WorkspaceToolCallResult struct {
	ToolID     string           `json:"tool_id"`
	Stdout     string           `json:"stdout"`
	Stderr     string           `json:"stderr"`
	ExitCode   int              `json:"exit_code"`
	TimedOut   bool             `json:"timed_out"`
	RouterKind string           `json:"router_kind,omitempty"`
	IsError    bool             `json:"is_error,omitempty"`
	Content    []MCPToolContent `json:"content,omitempty"`
	ServerID   string           `json:"server_id,omitempty"`
	ToolName   string           `json:"tool_name,omitempty"`
}

// call performs an RPC, transparently retrying on HTTP 429 / "rate limit exceeded" with bounded
// exponential backoff. A 429 means the per-token /rpc token bucket was momentarily empty so the server
// REJECTED the request WITHOUT processing it; retrying is therefore safe for every method (idempotent
// reads and not-yet-applied writes alike). This makes startup (agent.bootstrap) and steady-state RPCs
// survive transient rate-limit contention instead of treating the first 429 as fatal. It raises no server
// limit and weakens no admission/security gate: a persistently rate-limited caller still surfaces the
// error after the bounded attempts.
func (c *RhizomeClient) call(ctx context.Context, method string, params any, out any) error {
	const maxRateLimitAttempts = 4
	var lastErr error
	for attempt := 0; attempt < maxRateLimitAttempts; attempt++ {
		err := c.callOnce(ctx, method, params, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == maxRateLimitAttempts-1 || !rhizomeRateLimitRetryableError(err) {
			return err
		}
		if !sleepContext(ctx, rhizomeWorkNextRetryDelay(attempt)) {
			return firstNonNilError(ctx.Err(), lastErr)
		}
	}
	return lastErr
}

// rhizomeRateLimitRetryableError reports whether an RPC error is a server rate-limit rejection (HTTP 429),
// which is always safe to retry because the request was not processed. Method-agnostic, unlike the older
// work.next-specific predicate.
func rhizomeRateLimitRetryableError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "http 429") || strings.Contains(message, "rate limit exceeded")
}

func (c *RhizomeClient) callOnce(ctx context.Context, method string, params any, out any) error {
	if c == nil {
		return errors.New("rhizome client is nil")
	}
	body, err := json.Marshal(rhizomeRPCRequest{
		JSONRPC: "2.0",
		ID:      newRuntimeID("rpc"),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("marshal rpc request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build rpc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return formatRhizomeRPCContextError(method, firstNonNilError(ctx.Err(), err))
		}
		return fmt.Errorf("rpc %s transport: %w", method, err)
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return formatRhizomeRPCContextError(method, firstNonNilError(ctx.Err(), err))
		}
		return fmt.Errorf("rpc %s read body: %w", method, err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("rpc %s http %d: %s", method, resp.StatusCode, strings.TrimSpace(string(rawBody)))
	}

	var envelope rhizomeRPCResponse
	if err := json.Unmarshal(rawBody, &envelope); err != nil {
		return fmt.Errorf("rpc %s decode envelope: %w", method, err)
	}
	if envelope.Error != nil {
		return &RhizomeRPCError{
			Method:  method,
			Code:    envelope.Error.Code,
			Message: strings.TrimSpace(envelope.Error.Message),
		}
	}
	if out == nil {
		return nil
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return nil
	}
	if err := json.Unmarshal(envelope.Result, out); err != nil {
		return fmt.Errorf("rpc %s decode result: %w", method, err)
	}
	return nil
}

func (c *RhizomeClient) callWorkNextWithRetry(ctx context.Context, params any, out any) error {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Use callOnce (not call) so work.next keeps its single dedicated retry loop and does not stack a
		// second generic 429 retry on top of call()'s.
		err := c.callOnce(ctx, "agent.work.next", params, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == maxAttempts-1 || !rhizomeWorkNextRetryableError(err) {
			return err
		}
		delay := rhizomeWorkNextRetryDelay(attempt)
		if !sleepContext(ctx, delay) {
			return firstNonNilError(ctx.Err(), lastErr)
		}
	}
	return lastErr
}

func rhizomeWorkNextRetryableError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "agent.work.next") &&
		(strings.Contains(message, "http 429") || strings.Contains(message, "rate limit exceeded"))
}

func formatRhizomeRPCContextError(method string, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("rpc %s timed out: %w", method, context.DeadlineExceeded)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("rpc %s canceled: %w", method, context.Canceled)
	default:
		return err
	}
}

func (c *RhizomeClient) RegisterAgent(ctx context.Context, input AgentRegisterInput) (AgentRegisterResult, error) {
	registerWorkspaceName := authoritativeRegisterWorkspaceName(input)

	if strings.TrimSpace(input.WorkspacePassword) != "" {
		if authBase := c.authAPIBaseURL(input.HostURL); authBase != "" {
			authResp, err := c.registerAgentViaHTTPAuth(ctx, authBase, input)
			if err == nil {
				return normalizeRegisterResult(authResp, input)
			}
			if !shouldFallbackHTTPAuthRegistration(err) {
				return AgentRegisterResult{}, err
			}
		}

		previousToken := c.token
		c.token = ""
		var authResp AgentRegisterResult
		err := c.call(ctx, "workspace.auth.agent.register", workspaceAuthAgentRegisterParams{
			WorkspaceID:       input.WorkspaceID,
			WorkspaceName:     registerWorkspaceName,
			WorkspacePassword: input.WorkspacePassword,
			HostURL:           input.HostURL,
			AgentID:           input.AgentID,
			AgentName:         firstNonEmpty(input.DisplayName, input.AgentID),
			DisplayName:       input.DisplayName,
			GroupID:           input.GroupID,
			OwnerUserID:       input.OwnerUserID,
			Role:              input.Role,
			Status:            input.Status,
			ProtocolVersion:   input.ProtocolVersion,
			Capabilities:      input.Capabilities,
			Summary:           input.Summary,
		}, &authResp)
		c.token = previousToken
		if err == nil {
			return normalizeRegisterResult(authResp, input)
		}
		if !shouldFallbackLegacyRegistration(err) {
			return AgentRegisterResult{}, err
		}
	}

	var resp AgentRegisterResult
	err := c.call(ctx, "agent.register", input, &resp)
	if err != nil {
		return AgentRegisterResult{}, err
	}
	return normalizeRegisterResult(resp, input)
}

func normalizeRegisterResult(result AgentRegisterResult, input AgentRegisterInput) (AgentRegisterResult, error) {
	if err := validateRegisterIdentity(result, input); err != nil {
		return AgentRegisterResult{}, err
	}
	result.WorkspaceID = firstNonEmpty(result.WorkspaceID, result.Agent.WorkspaceID, input.WorkspaceID)
	result.WorkspaceName = firstNonEmpty(result.WorkspaceName, input.WorkspaceName)
	result.HostURL = firstNonEmpty(result.HostURL, input.HostURL)
	result.AgentID = firstNonEmpty(result.AgentID, result.Agent.AgentID, input.AgentID)
	result.DisplayName = firstNonEmpty(result.DisplayName, result.Agent.DisplayName, input.DisplayName)
	return result, nil
}

func validateRegisterIdentity(result AgentRegisterResult, input AgentRegisterInput) error {
	explicitAgentID := strings.TrimSpace(result.Agent.AgentID)
	explicitWorkspaceID := strings.TrimSpace(result.Agent.WorkspaceID)
	topAgentID := strings.TrimSpace(result.AgentID)
	topWorkspaceID := strings.TrimSpace(result.WorkspaceID)
	requestAgentID := strings.TrimSpace(input.AgentID)
	requestWorkspaceID := strings.TrimSpace(input.WorkspaceID)

	if topAgentID != "" && explicitAgentID != "" && topAgentID != explicitAgentID {
		return fmt.Errorf("register agent response mismatch: top-level agent_id %q != agent.agent_id %q", topAgentID, explicitAgentID)
	}
	if topWorkspaceID != "" && explicitWorkspaceID != "" && topWorkspaceID != explicitWorkspaceID {
		return fmt.Errorf("register agent response mismatch: top-level workspace_id %q != agent.workspace_id %q", topWorkspaceID, explicitWorkspaceID)
	}
	if requestAgentID != "" && topAgentID != "" && requestAgentID != topAgentID {
		return fmt.Errorf("register agent response mismatch: requested agent_id %q != response agent_id %q", requestAgentID, topAgentID)
	}
	if requestWorkspaceID != "" && topWorkspaceID != "" && requestWorkspaceID != topWorkspaceID {
		return fmt.Errorf("register agent response mismatch: requested workspace_id %q != response workspace_id %q", requestWorkspaceID, topWorkspaceID)
	}
	if requestAgentID != "" && explicitAgentID != "" && requestAgentID != explicitAgentID {
		return fmt.Errorf("register agent response mismatch: requested agent_id %q != registered agent_id %q", requestAgentID, explicitAgentID)
	}
	if requestWorkspaceID != "" && explicitWorkspaceID != "" && requestWorkspaceID != explicitWorkspaceID {
		return fmt.Errorf("register agent response mismatch: requested workspace_id %q != registered workspace_id %q", requestWorkspaceID, explicitWorkspaceID)
	}
	if strings.TrimSpace(result.Token) == "" {
		return errors.New("register agent response missing token")
	}
	if strings.TrimSpace(result.Agent.AgentID) == "" {
		return errors.New("register agent response missing authoritative agent record")
	}
	return nil
}

func shouldFallbackLegacyRegistration(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "unknown method") || strings.Contains(msg, "method not found")
}

type agentRegisterHTTPResult struct {
	WorkspaceID   string      `json:"workspace_id,omitempty"`
	WorkspaceName string      `json:"workspace_name,omitempty"`
	AgentID       string      `json:"agent_id,omitempty"`
	DisplayName   string      `json:"display_name,omitempty"`
	AccessToken   string      `json:"access_token,omitempty"`
	Token         string      `json:"token,omitempty"`
	Agent         AgentRecord `json:"agent,omitempty"`
}

type httpAPIError struct {
	StatusCode int
	Message    string
}

func (e *httpAPIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("http %d: %s", e.StatusCode, strings.TrimSpace(e.Message))
}

func (c *RhizomeClient) authAPIBaseURL(inputHostURL string) string {
	if c == nil {
		return ""
	}
	endpoint := strings.TrimSpace(c.endpoint)
	if !strings.HasSuffix(endpoint, "/rpc") {
		return ""
	}
	return firstNonEmpty(strings.TrimSpace(inputHostURL), hostURLForRPC(endpoint))
}

func (c *RhizomeClient) registerAgentViaHTTPAuth(ctx context.Context, baseURL string, input AgentRegisterInput) (AgentRegisterResult, error) {
	payload := workspaceAuthAgentRegisterParams{
		WorkspaceID:       input.WorkspaceID,
		WorkspaceName:     authoritativeRegisterWorkspaceName(input),
		WorkspacePassword: input.WorkspacePassword,
		HostURL:           input.HostURL,
		AgentID:           input.AgentID,
		AgentName:         firstNonEmpty(input.DisplayName, input.AgentID),
		DisplayName:       input.DisplayName,
		GroupID:           input.GroupID,
		OwnerUserID:       input.OwnerUserID,
		Role:              input.Role,
		Status:            input.Status,
		ProtocolVersion:   input.ProtocolVersion,
		Capabilities:      input.Capabilities,
		Summary:           input.Summary,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return AgentRegisterResult{}, fmt.Errorf("marshal auth register request: %w", err)
	}

	url := strings.TrimRight(strings.TrimSpace(baseURL), "/") + "/api/auth/agent/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return AgentRegisterResult{}, fmt.Errorf("build auth register request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return AgentRegisterResult{}, fmt.Errorf("http auth register transport: %w", err)
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return AgentRegisterResult{}, fmt.Errorf("http auth register read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := strings.TrimSpace(string(rawBody))
		var apiErr struct {
			Message string `json:"message"`
			Error   string `json:"error"`
		}
		if err := json.Unmarshal(rawBody, &apiErr); err == nil {
			message = firstNonEmpty(apiErr.Message, apiErr.Error, message)
		}
		return AgentRegisterResult{}, &httpAPIError{
			StatusCode: resp.StatusCode,
			Message:    firstNonEmpty(message, http.StatusText(resp.StatusCode)),
		}
	}

	var result agentRegisterHTTPResult
	if err := json.Unmarshal(rawBody, &result); err != nil {
		return AgentRegisterResult{}, fmt.Errorf("http auth register decode result: %w", err)
	}
	return AgentRegisterResult{
		AgentID:       firstNonEmpty(result.AgentID, input.AgentID),
		DisplayName:   firstNonEmpty(result.DisplayName, input.DisplayName),
		Token:         firstNonEmpty(result.AccessToken, result.Token),
		WorkspaceID:   firstNonEmpty(result.WorkspaceID, input.WorkspaceID),
		WorkspaceName: firstNonEmpty(result.WorkspaceName, input.WorkspaceName),
		HostURL:       firstNonEmpty(input.HostURL, baseURL),
		Agent:         result.Agent,
	}, nil
}

func authoritativeRegisterWorkspaceName(input AgentRegisterInput) string {
	registerWorkspaceName := strings.TrimSpace(input.WorkspaceName)
	if strings.TrimSpace(input.WorkspaceID) != "" {
		// Treat workspace_id as the authoritative registration reference.
		// Legacy profiles may still carry a stale workspace_name, which can
		// make the backend reject the request as ambiguous.
		return ""
	}
	return registerWorkspaceName
}

func shouldFallbackHTTPAuthRegistration(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *httpAPIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func shouldFallbackNativeAgentWork(err error) bool {
	return shouldFallbackLegacyRegistration(err)
}

func (c *RhizomeClient) Heartbeat(ctx context.Context, input AgentHeartbeatInput) error {
	return c.call(ctx, "agent.heartbeat", input, nil)
}

func (c *RhizomeClient) UpdateAgentProfile(ctx context.Context, input AgentProfileUpdateInput) error {
	return c.call(ctx, "agent.profile.update", input, nil)
}

func (c *RhizomeClient) Bootstrap(ctx context.Context, workspaceID, agentID string, updatesLimit int) (BootstrapResult, error) {
	var resp BootstrapResult
	err := c.call(ctx, "agent.bootstrap", map[string]any{
		"workspace_id":  workspaceID,
		"agent_id":      agentID,
		"updates_limit": updatesLimit,
	}, &resp)
	return resp, err
}

func (c *RhizomeClient) WorkNext(ctx context.Context, input WorkNextInput) (AgentWorkNextResult, error) {
	var resp AgentWorkNextResult
	err := c.callWorkNextWithRetry(ctx, input, &resp)
	return resp, err
}

func (c *RhizomeClient) HydrateTask(ctx context.Context, input TaskHydrationInput) (TaskHydrationBundle, error) {
	var resp struct {
		Bundle TaskHydrationBundle `json:"bundle"`
	}
	err := c.call(ctx, "agent.task.hydrate", input, &resp)
	return resp.Bundle, err
}

func (c *RhizomeClient) RecordTaskFrontierDecision(ctx context.Context, input TaskFrontierDecisionInput) error {
	return c.call(ctx, "agent.task_frontier.decision", input, nil)
}

func (c *RhizomeClient) ListSessions(ctx context.Context, workspaceID string, activeOnly bool, limit int) ([]AgentSessionStateRecord, error) {
	var resp struct {
		Sessions []AgentSessionStateRecord `json:"sessions"`
	}
	err := c.call(ctx, "workspace.sessions.list", map[string]any{
		"workspace_id": workspaceID,
		"active_only":  activeOnly,
		"limit":        limit,
	}, &resp)
	return resp.Sessions, err
}

func (c *RhizomeClient) SessionEvent(ctx context.Context, method string, input SessionEventInput) (AgentSessionStateRecord, error) {
	var resp struct {
		State AgentSessionStateRecord `json:"state"`
	}
	err := c.call(ctx, method, input, &resp)
	return resp.State, err
}

func (c *RhizomeClient) ClaimTask(ctx context.Context, input TaskClaimInput) error {
	return c.call(ctx, "agent.task.claim", input, nil)
}

func (c *RhizomeClient) ListProjects(ctx context.Context, workspaceID string) ([]ProjectRecord, error) {
	var resp struct {
		Projects []ProjectRecord `json:"projects"`
	}
	err := c.call(ctx, "project.list", map[string]any{
		"workspace_id": workspaceID,
	}, &resp)
	return resp.Projects, err
}

func (c *RhizomeClient) ListRuntimeEvents(ctx context.Context, input RuntimeEventListInput) ([]RuntimeEventRecord, error) {
	var resp struct {
		Items []RuntimeEventRecord `json:"items"`
	}
	err := c.call(ctx, "workspace.events.list", input, &resp)
	return resp.Items, err
}

func (c *RhizomeClient) CreateProject(ctx context.Context, input ProjectCreateInput) (ProjectRecord, error) {
	var resp struct {
		Project ProjectRecord `json:"project"`
	}
	err := c.call(ctx, "project.create", input, &resp)
	return resp.Project, err
}

func (c *RhizomeClient) UpdateProjectProfile(ctx context.Context, input ProjectProfileUpdateInput) (ProjectProfileRecord, error) {
	params := map[string]any{
		"workspace_id": strings.TrimSpace(input.WorkspaceID),
		"project_id":   strings.TrimSpace(input.ProjectID),
		"actor_id":     strings.TrimSpace(input.ActorID),
	}
	if strings.TrimSpace(input.Goal) != "" {
		params["goal"] = strings.TrimSpace(input.Goal)
	}
	if strings.TrimSpace(input.DesignDocID) != "" {
		params["design_doc_id"] = strings.TrimSpace(input.DesignDocID)
	}
	if strings.TrimSpace(input.ImplementationPlanDocID) != "" {
		params["implementation_plan_doc_id"] = strings.TrimSpace(input.ImplementationPlanDocID)
	}
	if input.RepoRequired != nil {
		params["repo_required"] = *input.RepoRequired
	}
	if strings.TrimSpace(input.RepoStatus) != "" {
		params["repo_status"] = strings.TrimSpace(input.RepoStatus)
	}
	if strings.TrimSpace(input.RepoURL) != "" {
		params["repo_url"] = strings.TrimSpace(input.RepoURL)
	}
	if strings.TrimSpace(input.RepoDefaultBranch) != "" {
		params["repo_default_branch"] = strings.TrimSpace(input.RepoDefaultBranch)
	}
	var resp struct {
		Profile ProjectProfileRecord `json:"profile"`
	}
	err := c.call(ctx, "project.profile.update", params, &resp)
	return resp.Profile, err
}

func (c *RhizomeClient) ClaimProjectLead(ctx context.Context, input ProjectLeadClaimInput) (ProjectRoleRecord, error) {
	var resp struct {
		Role ProjectRoleRecord `json:"role"`
	}
	err := c.call(ctx, "project.lead.claim", input, &resp)
	return resp.Role, err
}

func (c *RhizomeClient) TransitionProjectPhase(ctx context.Context, input ProjectPhaseTransitionInput) (ProjectProfileRecord, error) {
	var resp struct {
		Profile ProjectProfileRecord `json:"profile"`
	}
	err := c.call(ctx, "project.phase.transition", input, &resp)
	return resp.Profile, err
}

func (c *RhizomeClient) AssignProjectRole(ctx context.Context, input ProjectRoleAssignInput) (ProjectRoleRecord, error) {
	resp, err := c.AssignProjectRoleWithResult(ctx, input)
	return resp.Role, err
}

func (c *RhizomeClient) AssignProjectRoleWithResult(ctx context.Context, input ProjectRoleAssignInput) (ProjectRoleAssignResult, error) {
	var resp ProjectRoleAssignResult
	err := c.call(ctx, "project.role.assign", input, &resp)
	return resp, err
}

func (c *RhizomeClient) PutTaskProjectFields(ctx context.Context, input TaskProjectFieldsPutInput) (WorkspaceTaskRecord, error) {
	var resp struct {
		Task WorkspaceTaskRecord `json:"task"`
	}
	err := c.call(ctx, "task.project_fields.put", input, &resp)
	return resp.Task, err
}

func (c *RhizomeClient) GetProjectCoordination(ctx context.Context, workspaceID, projectID string) (ProjectCoordinationRecord, error) {
	var resp struct {
		Coordination ProjectCoordinationRecord `json:"coordination"`
	}
	err := c.call(ctx, "project.coordination.get", map[string]any{
		"workspace_id": strings.TrimSpace(workspaceID),
		"project_id":   strings.TrimSpace(projectID),
	}, &resp)
	return resp.Coordination, err
}

func (c *RhizomeClient) CheckProjectGovernancePredicates(ctx context.Context, input ProjectGovernancePredicatesCheckInput) ([]GovernanceStallPredicateResult, bool, error) {
	var resp struct {
		PredicateResults []GovernanceStallPredicateResult `json:"predicate_results"`
		AllHold          bool                             `json:"all_hold"`
	}
	err := c.call(ctx, "project.governance.predicates.check", input, &resp)
	return resp.PredicateResults, resp.AllHold, err
}

func (c *RhizomeClient) RaiseProjectGovernanceChallenge(ctx context.Context, input ProjectGovernanceChallengeRaiseInput) (GovernanceChallengeRecord, error) {
	var resp struct {
		Challenge GovernanceChallengeRecord `json:"challenge"`
	}
	err := c.call(ctx, "project.governance.challenge.raise", input, &resp)
	return resp.Challenge, err
}

func (c *RhizomeClient) DefendProjectGovernanceChallenge(ctx context.Context, input ProjectGovernanceChallengeDefendInput) (GovernanceChallengeRecord, error) {
	var resp struct {
		Challenge GovernanceChallengeRecord `json:"challenge"`
	}
	err := c.call(ctx, "project.governance.challenge.defend", input, &resp)
	return resp.Challenge, err
}

func (c *RhizomeClient) CastProjectGovernanceVote(ctx context.Context, input ProjectGovernanceVoteCastInput) (GovernanceVoteRecord, error) {
	var resp struct {
		Vote GovernanceVoteRecord `json:"vote"`
	}
	err := c.call(ctx, "project.governance.vote.cast", input, &resp)
	return resp.Vote, err
}

func (c *RhizomeClient) TallyProjectGovernanceChallenge(ctx context.Context, input ProjectGovernanceChallengeTallyInput) (GovernanceTallyResult, error) {
	var resp struct {
		Tally GovernanceTallyResult `json:"tally"`
	}
	err := c.call(ctx, "project.governance.challenge.tally", input, &resp)
	return resp.Tally, err
}

func (c *RhizomeClient) GetProjectGovernanceChallenge(ctx context.Context, workspaceID, challengeID string, includeVotes bool) (GovernanceChallengeRecord, []GovernanceVoteRecord, error) {
	var resp struct {
		Challenge GovernanceChallengeRecord `json:"challenge"`
		Votes     []GovernanceVoteRecord    `json:"votes"`
	}
	err := c.call(ctx, "project.governance.challenge.get", map[string]any{
		"workspace_id":  strings.TrimSpace(workspaceID),
		"challenge_id":  strings.TrimSpace(challengeID),
		"include_votes": includeVotes,
	}, &resp)
	return resp.Challenge, resp.Votes, err
}

func (c *RhizomeClient) ListProjectGovernanceChallenges(ctx context.Context, input ProjectGovernanceChallengeListInput) ([]GovernanceChallengeRecord, error) {
	var resp struct {
		Challenges []GovernanceChallengeRecord `json:"challenges"`
	}
	err := c.call(ctx, "project.governance.challenge.list", input, &resp)
	return resp.Challenges, err
}

func (c *RhizomeClient) UpsertProjectRepository(ctx context.Context, input ProjectRepositoryUpsertInput) (ProjectRepositoryRecord, error) {
	var resp struct {
		Repository ProjectRepositoryRecord `json:"repository"`
	}
	err := c.call(ctx, "project.repository.upsert", input, &resp)
	return resp.Repository, err
}

func (c *RhizomeClient) ListProjectRepositories(ctx context.Context, workspaceID, projectID string, includeArchived bool) ([]ProjectRepositoryRecord, error) {
	var resp struct {
		Repositories []ProjectRepositoryRecord `json:"repositories"`
	}
	err := c.call(ctx, "project.repositories.list", map[string]any{
		"workspace_id":     strings.TrimSpace(workspaceID),
		"project_id":       strings.TrimSpace(projectID),
		"include_archived": includeArchived,
	}, &resp)
	return resp.Repositories, err
}

func (c *RhizomeClient) ListProjectBranches(ctx context.Context, workspaceID, projectID string, includeInactive bool) ([]ProjectBranchRecord, error) {
	var resp struct {
		Branches []ProjectBranchRecord `json:"branches"`
	}
	err := c.call(ctx, "project.branches.list", map[string]any{
		"workspace_id":     strings.TrimSpace(workspaceID),
		"project_id":       strings.TrimSpace(projectID),
		"include_inactive": includeInactive,
	}, &resp)
	return resp.Branches, err
}

func (c *RhizomeClient) RegisterProjectCheckout(ctx context.Context, input ProjectCheckoutRegisterInput) (ProjectCheckoutRecord, error) {
	var resp struct {
		Checkout ProjectCheckoutRecord `json:"checkout"`
	}
	err := c.call(ctx, "project.checkout.register", input, &resp)
	return resp.Checkout, err
}

func (c *RhizomeClient) RegisterProjectBranch(ctx context.Context, input ProjectBranchRegisterInput) (ProjectBranchRecord, error) {
	var resp struct {
		Branch ProjectBranchRecord `json:"branch"`
	}
	err := c.call(ctx, "project.branch.register", input, &resp)
	return resp.Branch, err
}

func (c *RhizomeClient) SubmitProjectPatchQueueItem(ctx context.Context, input ProjectPatchQueueSubmitInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.submit", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) SupersedeProjectPatchQueueItem(ctx context.Context, input ProjectPatchQueueSupersedeInput) (ProjectPatchQueueItemRecord, bool, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
		AlreadyQueued  bool                        `json:"already_queued"`
	}
	err := c.call(ctx, "project.patch_queue.supersede", input, &resp)
	return resp.PatchQueueItem, resp.AlreadyQueued, err
}

func (c *RhizomeClient) ListProjectPatchQueueItems(ctx context.Context, input ProjectPatchQueueListInput) ([]ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItems []ProjectPatchQueueItemRecord `json:"patch_queue_items"`
	}
	err := c.call(ctx, "project.patch_queue.list", input, &resp)
	return resp.PatchQueueItems, err
}

func (c *RhizomeClient) ClaimProjectPatchQueueItem(ctx context.Context, input ProjectPatchQueueClaimInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.claim", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) BindProjectPatchQueueMutationOperation(ctx context.Context, input ProjectPatchQueueOperationBindInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.operation_bind", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) RecordProjectPatchQueueCASEvidence(ctx context.Context, input ProjectPatchQueueCASRecordInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.cas_record", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) RecordProjectPatchQueueMaterialization(ctx context.Context, input ProjectPatchQueueMaterializationRecordInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.materialization_record", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) RecordProjectPatchQueueRollbackEvidence(ctx context.Context, input ProjectPatchQueueRollbackRecordInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.rollback_record", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) RecordProjectPatchQueueReviewerAdvisory(ctx context.Context, input ProjectPatchQueueReviewerAdvisoryRecordInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.reviewer_advisory_record", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) RecordProjectPatchQueueOperatorEnablement(ctx context.Context, input ProjectPatchQueueOperatorEnablementRecordInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.operator_enablement_record", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) ReleaseProjectPatchQueueClaim(ctx context.Context, input ProjectPatchQueueReleaseInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.release", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) DecideProjectPatchQueueItem(ctx context.Context, input ProjectPatchQueueDecisionInput) (ProjectPatchQueueItemRecord, error) {
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := c.call(ctx, "project.patch_queue.decision", input, &resp)
	return resp.PatchQueueItem, err
}

func (c *RhizomeClient) ReconcileProjectPatchQueueReviewTask(ctx context.Context, input ProjectPatchQueueReviewTaskReconcileInput) (TaskStatus, string, bool, error) {
	var resp struct {
		PatchQueueReviewTask TaskStatus `json:"patch_queue_review_task"`
		ReviewTaskEventID    string     `json:"review_task_event_id"`
		Repaired             bool       `json:"repaired"`
	}
	err := c.call(ctx, "project.patch_queue.review_task.reconcile", input, &resp)
	return resp.PatchQueueReviewTask, resp.ReviewTaskEventID, resp.Repaired, err
}

func (c *RhizomeClient) ConsumeProjectPatchQueueDecisionContinuation(ctx context.Context, input ProjectPatchQueueDecisionContinuationConsumeInput) (TaskStatus, ProjectPatchQueueDecisionContinuationRecord, bool, bool, error) {
	var resp struct {
		ContinuationTask               TaskStatus                                  `json:"continuation_task"`
		PatchQueueDecisionContinuation ProjectPatchQueueDecisionContinuationRecord `json:"patch_queue_decision_continuation"`
		Consumed                       bool                                        `json:"consumed"`
		Created                        bool                                        `json:"created"`
	}
	err := c.call(ctx, "project.patch_queue.decision_continuation.consume", input, &resp)
	return resp.ContinuationTask, resp.PatchQueueDecisionContinuation, resp.Consumed, resp.Created, err
}

func (c *RhizomeClient) CallServiceVenture(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	method = strings.TrimSpace(method)
	if method == "" {
		return nil, errors.New("service venture method is required")
	}
	var resp map[string]any
	err := c.call(ctx, method, params, &resp)
	return resp, err
}

func (c *RhizomeClient) ReleaseTask(ctx context.Context, input TaskReleaseInput) error {
	return c.call(ctx, "agent.task.release", input, nil)
}

func (c *RhizomeClient) CompleteTask(ctx context.Context, input TaskCompleteInput) error {
	return c.call(ctx, "agent.task.complete", input, nil)
}

func (c *RhizomeClient) BlockTask(ctx context.Context, input TaskBlockInput) error {
	return c.call(ctx, "agent.task.block", input, nil)
}

func (c *RhizomeClient) CloseTask(ctx context.Context, input TaskCloseInput) error {
	return c.call(ctx, "task.close", input, nil)
}

func (c *RhizomeClient) SubmitTask(ctx context.Context, input TaskSubmitInput) (TaskSubmitResult, error) {
	var resp TaskSubmitResult
	err := c.call(ctx, "task.submit", input, &resp)
	return resp, err
}

func (c *RhizomeClient) ListTasks(ctx context.Context, workspaceID string) ([]WorkspaceTaskRecord, error) {
	var resp struct {
		Tasks []WorkspaceTaskRecord `json:"tasks"`
	}
	err := c.call(ctx, "workspace.tasks.list", map[string]any{
		"workspace_id": workspaceID,
	}, &resp)
	return resp.Tasks, err
}

func (c *RhizomeClient) OfferCoalition(ctx context.Context, input CoalitionOfferInput) (TensionAgentMutationResult, error) {
	var resp TensionAgentMutationResult
	if err := c.call(ctx, "coalition.offer", input, &resp); err != nil {
		return resp, err
	}
	if err := validateTensionAgentMutationResult(resp, "tension.agent.attached"); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RhizomeClient) LeaveCoalition(ctx context.Context, input CoalitionLeaveInput) (TensionAgentMutationResult, error) {
	var resp TensionAgentMutationResult
	if err := c.call(ctx, "coalition.leave", input, &resp); err != nil {
		return resp, err
	}
	if err := validateTensionAgentMutationResult(resp, "tension.agent.detached"); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RhizomeClient) SeekCoalition(ctx context.Context, input CoalitionSeekInput) (json.RawMessage, error) {
	var resp json.RawMessage
	err := c.call(ctx, "coalition.seek", input, &resp)
	return resp, err
}

func (c *RhizomeClient) InviteCoalition(ctx context.Context, input CoalitionInviteInput) (TensionAgentMutationResult, error) {
	var resp TensionAgentMutationResult
	if err := c.call(ctx, "coalition.invite", input, &resp); err != nil {
		return resp, err
	}
	if err := validateTensionAgentMutationResult(resp, "tension.agent.attached"); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RhizomeClient) KickCoalition(ctx context.Context, input CoalitionKickInput) (TensionAgentMutationResult, error) {
	var resp TensionAgentMutationResult
	if err := c.call(ctx, "coalition.kick", input, &resp); err != nil {
		return resp, err
	}
	if err := validateTensionAgentMutationResult(resp, "tension.agent.detached"); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RhizomeClient) GetCoalitionStatus(ctx context.Context, input CoalitionStatusInput) (json.RawMessage, error) {
	var resp json.RawMessage
	err := c.call(ctx, "coalition.status", input, &resp)
	return resp, err
}

func (c *RhizomeClient) RouteReviewer(ctx context.Context, input ReviewerRouteInput) (json.RawMessage, error) {
	var resp json.RawMessage
	err := c.call(ctx, "reviewer.route", input, &resp)
	return resp, err
}

func (c *RhizomeClient) CheckReviewerScarcity(ctx context.Context, input ReviewerScarcityInput) (json.RawMessage, error) {
	var resp json.RawMessage
	err := c.call(ctx, "reviewer.scarcity", input, &resp)
	return resp, err
}

func (c *RhizomeClient) GetMemoryCoherence(ctx context.Context, input MemoryCoherenceInput) (json.RawMessage, error) {
	var resp json.RawMessage
	err := c.call(ctx, "workspace.memory.coherence.scope", input, &resp)
	return resp, err
}

func (c *RhizomeClient) ListPromotionRequests(ctx context.Context, input MemoryPromotionInput) (json.RawMessage, error) {
	var resp json.RawMessage
	err := c.call(ctx, "workspace.memory.promotion.list", input, &resp)
	return resp, err
}

func (c *RhizomeClient) PollMessages(ctx context.Context, input MessagePollInput) (PollMessagesResult, error) {
	var resp PollMessagesResult
	err := c.call(ctx, "agent.message.poll", input, &resp)
	return resp, err
}

func (c *RhizomeClient) PollNews(ctx context.Context, input NewsPollInput) (PollNewsResult, error) {
	var resp PollNewsResult
	err := c.call(ctx, "news.poll", input, &resp)
	return resp, err
}

func (c *RhizomeClient) AckMessages(ctx context.Context, input MessageAckInput) error {
	return c.call(ctx, "agent.message.ack", input, nil)
}

func (c *RhizomeClient) ListWorkspaceMessages(ctx context.Context, workspaceID, channel string, limit int) ([]MessageRecord, error) {
	var resp struct {
		Messages []MessageRecord `json:"messages"`
	}
	params := map[string]any{
		"workspace_id": workspaceID,
		"limit":        limit,
	}
	if strings.TrimSpace(channel) != "" {
		params["channel"] = channel
	}
	err := c.call(ctx, "workspace.messages.list", params, &resp)
	return resp.Messages, err
}

func (c *RhizomeClient) ListWorkspaceAgents(ctx context.Context, workspaceID string) ([]AgentRecord, error) {
	var resp struct {
		Agents []AgentRecord `json:"agents"`
	}
	err := c.call(ctx, "workspace.agents.list", map[string]any{
		"workspace_id": workspaceID,
	}, &resp)
	return resp.Agents, err
}

func (c *RhizomeClient) PostUpdate(ctx context.Context, input UpdatePostInput) error {
	return c.call(ctx, "agent.update.post", input, nil)
}

func (c *RhizomeClient) PostEpochTick(ctx context.Context, workspaceID string) error {
	return c.call(ctx, "workspace.control.epoch.tick", map[string]any{"workspace_id": workspaceID}, nil)
}

func (c *RhizomeClient) PutDoc(ctx context.Context, input WorkspaceDocPutInput) (string, error) {
	var resp struct {
		SHA string `json:"sha"`
	}
	err := c.call(ctx, "workspace.doc.put", input, &resp)
	return resp.SHA, err
}

func (c *RhizomeClient) ListDocs(ctx context.Context, workspaceID string, limit int) ([]WorkspaceDocRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	var resp struct {
		Docs []WorkspaceDocRecord `json:"docs"`
	}
	err := c.call(ctx, "workspace.doc.list", map[string]any{
		"workspace_id": strings.TrimSpace(workspaceID),
		"limit":        limit,
	}, &resp)
	return resp.Docs, err
}

func (c *RhizomeClient) GetDoc(ctx context.Context, workspaceID, docKey string) (WorkspaceDocRecord, bool, error) {
	var doc WorkspaceDocRecord
	err := c.call(ctx, "workspace.doc.get", map[string]any{
		"workspace_id": workspaceID,
		"doc_key":      docKey,
	}, &doc)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return WorkspaceDocRecord{}, false, nil
		}
		return WorkspaceDocRecord{}, false, err
	}
	return doc, true, nil
}

func (c *RhizomeClient) TouchMemoryNode(ctx context.Context, input WorkspaceMemoryNodeTouchInput) error {
	return c.call(ctx, "workspace.memory.node.touch", input, nil)
}

func (c *RhizomeClient) WriteMemory(ctx context.Context, input WorkspaceMemoryWriteInput) (WorkspaceMemoryRecord, error) {
	var resp struct {
		Memory WorkspaceMemoryRecord `json:"memory"`
	}
	err := c.call(ctx, "workspace.memory.write", input, &resp)
	return resp.Memory, err
}

func (c *RhizomeClient) WriteArtifact(ctx context.Context, input WorkspaceArtifactWriteInput) (WorkspaceArtifactRecord, error) {
	var resp struct {
		Artifact WorkspaceArtifactRecord `json:"artifact"`
	}
	err := c.call(ctx, "workspace.artifact.write", input, &resp)
	return resp.Artifact, err
}

func (c *RhizomeClient) ListArtifacts(ctx context.Context, input WorkspaceArtifactListInput) ([]WorkspaceArtifactRecord, error) {
	var resp struct {
		Items []WorkspaceArtifactRecord `json:"items"`
	}
	err := c.call(ctx, "workspace.artifact.list", input, &resp)
	return resp.Items, err
}

func (c *RhizomeClient) WriteExecutionRun(ctx context.Context, input ExecutionRunWriteInput) (ExecutionRunRecord, error) {
	var resp struct {
		Run ExecutionRunRecord `json:"run"`
	}
	err := c.call(ctx, "workspace.execution.run.write", input, &resp)
	return resp.Run, err
}

func (c *RhizomeClient) GetExecutionRun(ctx context.Context, workspaceID, runID string) (ExecutionRunDetail, error) {
	var resp struct {
		Detail ExecutionRunDetail `json:"detail"`
	}
	err := c.call(ctx, "workspace.execution.run.get", map[string]any{
		"workspace_id": strings.TrimSpace(workspaceID),
		"run_id":       strings.TrimSpace(runID),
	}, &resp)
	return resp.Detail, err
}

func (c *RhizomeClient) CancelExecutionRunsForAgentStop(ctx context.Context, input ExecutionAgentRunsCancelInput) (ExecutionAgentRunsCancelResult, error) {
	var resp struct {
		Result ExecutionAgentRunsCancelResult `json:"result"`
	}
	err := c.call(ctx, "workspace.execution.agent_runs.cancel", input, &resp)
	return resp.Result, err
}

func (c *RhizomeClient) WriteExecutionStep(ctx context.Context, input ExecutionStepWriteInput) (ExecutionStepRecord, error) {
	var resp struct {
		Step ExecutionStepRecord `json:"step"`
	}
	err := c.call(ctx, "workspace.execution.step.write", input, &resp)
	return resp.Step, err
}

func (c *RhizomeClient) CheckPolicy(ctx context.Context, input PolicyCheckInput) (CapabilityCheckResult, error) {
	var resp struct {
		Check CapabilityCheckResult `json:"check"`
	}
	err := c.call(ctx, "workspace.policy.check", input, &resp)
	return resp.Check, err
}

func (c *RhizomeClient) GetControlReport(ctx context.Context, workspaceID string, limit int) (ControlReport, error) {
	var resp struct {
		Report ControlReport `json:"report"`
	}
	err := c.call(ctx, "workspace.instrumentation.control.report", map[string]any{
		"workspace_id": workspaceID,
		"limit":        limit,
	}, &resp)
	return resp.Report, err
}

func (c *RhizomeClient) GetControlClusterDetail(ctx context.Context, input ControlClusterInput) (ControlClusterDetail, error) {
	var resp struct {
		Detail ControlClusterDetail `json:"detail"`
	}
	err := c.call(ctx, "workspace.instrumentation.control.cluster", input, &resp)
	return resp.Detail, err
}

func (c *RhizomeClient) GetCorridorClusterDetail(ctx context.Context, input CorridorClusterInput) (CorridorClusterDetail, error) {
	var resp struct {
		Detail CorridorClusterDetail `json:"detail"`
	}
	err := c.call(ctx, "workspace.instrumentation.corridor.cluster", input, &resp)
	return resp.Detail, err
}

func (c *RhizomeClient) GetLocusBundle(ctx context.Context, input LocusBundleInput) (InstrumentationLocusBundle, error) {
	var resp struct {
		Bundle InstrumentationLocusBundle `json:"bundle"`
	}
	err := c.call(ctx, "workspace.instrumentation.locus.bundle", input, &resp)
	return resp.Bundle, err
}

func (c *RhizomeClient) ListTensionFrontier(ctx context.Context, input TensionFrontierInput) ([]TensionFrontierItem, error) {
	var resp struct {
		Items []TensionFrontierItem `json:"items"`
	}
	err := c.call(ctx, "workspace.tension.frontier", input, &resp)
	return resp.Items, err
}

func (c *RhizomeClient) GetTensionFrontier(ctx context.Context, workspaceID string, limit int) ([]TensionFrontierItem, error) {
	return c.ListTensionFrontier(ctx, TensionFrontierInput{
		WorkspaceID: workspaceID,
		Limit:       limit,
	})
}

func (c *RhizomeClient) RefreshTensions(ctx context.Context, input TensionRefreshInput) error {
	var resp map[string]any
	return c.call(ctx, "workspace.tension.refresh", input, &resp)
}

type TensionAgentAttachInput struct {
	WorkspaceID      string `json:"workspace_id"`
	TensionID        string `json:"tension_id"`
	AgentID          string `json:"agent_id"`
	ActorID          string `json:"actor_id"`
	SuccessCriterion string `json:"success_criterion,omitempty"`
	Reason           string `json:"reason,omitempty"`
}

type TensionAgentDetachInput struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	Reason      string `json:"reason,omitempty"`
}

type TensionAgentMutationResult struct {
	Success     bool                        `json:"success"`
	Changed     bool                        `json:"changed"`
	CoalitionID string                      `json:"coalition_id,omitempty"`
	Coalition   WorkspaceCoalitionRecord    `json:"coalition,omitempty"`
	Event       TensionMutationRuntimeEvent `json:"event,omitempty"`
}

type TensionMutationRuntimeEvent struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	EntityType string `json:"entity_type,omitempty"`
	EntityID   string `json:"entity_id,omitempty"`
	ActorType  string `json:"actor_type,omitempty"`
	ActorID    string `json:"actor_id,omitempty"`
	AgentID    string `json:"agent_id,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
}

type CoalitionStatusResult struct {
	Coalitions []WorkspaceCoalitionRecord `json:"coalitions"`
}

type WorkspaceCoalitionRecord struct {
	CoalitionID      string                           `json:"coalition_id"`
	WorkspaceID      string                           `json:"workspace_id"`
	TensionID        string                           `json:"tension_id"`
	SuccessCriterion string                           `json:"success_criterion,omitempty"`
	Status           string                           `json:"status"`
	Members          []WorkspaceCoalitionMemberRecord `json:"members"`
}

type WorkspaceCoalitionMemberRecord struct {
	CoalitionID string `json:"coalition_id"`
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Role        string `json:"role,omitempty"`
}

type TensionLifecycleUpdateInput struct {
	WorkspaceID    string `json:"workspace_id"`
	TensionID      string `json:"tension_id"`
	LifecycleState string `json:"lifecycle_state"`
	UpdatedBy      string `json:"updated_by"`
	Reason         string `json:"reason,omitempty"`
}

func (c *RhizomeClient) AttachTensionAgent(ctx context.Context, input TensionAgentAttachInput) (TensionAgentMutationResult, error) {
	var resp TensionAgentMutationResult
	if err := c.call(ctx, "workspace.tension.agent.attach", input, &resp); err != nil {
		return resp, err
	}
	if err := validateTensionAgentMutationResult(resp, "tension.agent.attached"); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RhizomeClient) DetachTensionAgent(ctx context.Context, input TensionAgentDetachInput) (TensionAgentMutationResult, error) {
	var resp TensionAgentMutationResult
	if err := c.call(ctx, "workspace.tension.agent.detach", input, &resp); err != nil {
		return resp, err
	}
	if err := validateTensionAgentMutationResult(resp, "tension.agent.detached"); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RhizomeClient) UpdateTensionLifecycle(ctx context.Context, input TensionLifecycleUpdateInput) error {
	var resp struct{}
	return c.call(ctx, "workspace.tension.lifecycle.update", input, &resp)
}

func (c *RhizomeClient) GetCoalitionStatusTyped(ctx context.Context, input CoalitionStatusInput) (CoalitionStatusResult, error) {
	var resp CoalitionStatusResult
	err := c.call(ctx, "coalition.status", input, &resp)
	return resp, err
}

func (c *RhizomeClient) ResolveTensionAgentCoalition(ctx context.Context, workspaceID, tensionID, agentID string) (WorkspaceCoalitionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	tensionID = strings.TrimSpace(tensionID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || tensionID == "" || agentID == "" {
		return WorkspaceCoalitionRecord{}, errors.New("workspace_id, tension_id, and agent_id are required")
	}

	status, err := c.GetCoalitionStatusTyped(ctx, CoalitionStatusInput{WorkspaceID: workspaceID})
	if err != nil {
		return WorkspaceCoalitionRecord{}, err
	}
	matches := make([]WorkspaceCoalitionRecord, 0, 1)
	for _, coalition := range status.Coalitions {
		if strings.TrimSpace(coalition.WorkspaceID) != workspaceID {
			continue
		}
		if strings.TrimSpace(coalition.TensionID) != tensionID {
			continue
		}
		if !isLiveCoalitionStatus(coalition.Status) {
			continue
		}
		if !coalitionHasAgentMember(coalition, agentID) {
			continue
		}
		matches = append(matches, coalition)
	}
	switch len(matches) {
	case 0:
		return WorkspaceCoalitionRecord{}, fmt.Errorf("no live coalition membership found for tension %q and agent %q", tensionID, agentID)
	case 1:
		if strings.TrimSpace(matches[0].CoalitionID) == "" {
			return WorkspaceCoalitionRecord{}, fmt.Errorf("live coalition for tension %q has empty coalition_id", tensionID)
		}
		return matches[0], nil
	default:
		return WorkspaceCoalitionRecord{}, fmt.Errorf("ambiguous live coalition membership for tension %q and agent %q: %d matches", tensionID, agentID, len(matches))
	}
}

func isLiveCoalitionStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE", "FORMING":
		return true
	default:
		return false
	}
}

func coalitionHasAgentMember(coalition WorkspaceCoalitionRecord, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	for _, member := range coalition.Members {
		if strings.TrimSpace(member.AgentID) == agentID {
			return true
		}
	}
	return false
}

func validateTensionAgentMutationResult(result TensionAgentMutationResult, expectedEventType string) error {
	expectedEventType = strings.TrimSpace(expectedEventType)
	if !result.Success {
		return fmt.Errorf("%s RPC did not report success", expectedEventType)
	}
	if firstNonEmpty(result.CoalitionID, result.Coalition.CoalitionID) == "" {
		return fmt.Errorf("%s RPC returned success without coalition_id", expectedEventType)
	}
	if !result.Changed {
		return nil
	}
	if strings.TrimSpace(result.Event.EventID) == "" {
		return fmt.Errorf("%s RPC returned changed=true without runtime event id", expectedEventType)
	}
	if got := strings.TrimSpace(result.Event.EventType); got != expectedEventType {
		return fmt.Errorf("%s RPC returned unexpected event_type %q", expectedEventType, got)
	}
	return nil
}

func (c *RhizomeClient) GetTension(ctx context.Context, workspaceID, tensionID string) (TensionDetail, error) {
	var detail TensionDetail
	err := c.call(ctx, "workspace.tension.get", map[string]any{
		"workspace_id": workspaceID,
		"tension_id":   tensionID,
	}, &detail)
	return detail, err
}

func (c *RhizomeClient) UpsertOperatorQueue(ctx context.Context, input OperatorQueueUpsertInput) error {
	return c.call(ctx, "workspace.ops.upsert", input, nil)
}

func (c *RhizomeClient) GetOperatorQueue(ctx context.Context, workspaceID, queueID, queueKey string) (OperatorQueueRecord, bool, error) {
	var resp struct {
		Item OperatorQueueRecord `json:"item"`
	}
	err := c.call(ctx, "workspace.ops.get", map[string]any{
		"workspace_id": strings.TrimSpace(workspaceID),
		"queue_id":     strings.TrimSpace(queueID),
		"queue_key":    strings.TrimSpace(queueKey),
	}, &resp)
	if err != nil {
		if isRhizomeRPCErrorCode(err, rhizomeRPCCodeOperatorQueueNotFound) {
			return OperatorQueueRecord{}, false, nil
		}
		msg := strings.ToLower(strings.TrimSpace(err.Error()))
		if strings.Contains(msg, "operator queue item not found") || strings.Contains(msg, "not found") {
			return OperatorQueueRecord{}, false, nil
		}
		if strings.Contains(msg, "unknown method") || strings.Contains(msg, "method not found") {
			return OperatorQueueRecord{}, false, errOperatorQueueLookupUnavailable
		}
		return OperatorQueueRecord{}, false, err
	}
	resp.Item.QueueID = strings.TrimSpace(resp.Item.QueueID)
	resp.Item.WorkspaceID = strings.TrimSpace(resp.Item.WorkspaceID)
	resp.Item.QueueKey = strings.TrimSpace(resp.Item.QueueKey)
	resp.Item.UpdatedAt = strings.TrimSpace(resp.Item.UpdatedAt)
	if resp.Item.QueueID == "" && resp.Item.QueueKey == "" {
		return OperatorQueueRecord{}, false, nil
	}
	return resp.Item, true, nil
}

func (c *RhizomeClient) RequestExternalGate(ctx context.Context, input ExternalGateRequestInput) error {
	hydrated, err := c.hydrateOperatorQueueBaseVersion(ctx, strings.TrimSpace(input.WorkspaceID), "", externalGateQueueKey(strings.TrimSpace(input.GateType), strings.TrimSpace(input.RequestKey)), input.CurrentRevision, strings.TrimSpace(input.CurrentUpdatedAt))
	if err != nil {
		return err
	}
	input.CurrentRevision = hydrated.CurrentRevision
	input.CurrentUpdatedAt = hydrated.CurrentUpdatedAt
	err = c.call(ctx, "workspace.ops.request", input, nil)
	if err == nil || !shouldFallbackWorkspaceOpsRequest(err) {
		return err
	}
	return c.UpsertOperatorQueue(ctx, legacyOperatorQueueInputFromGate(input))
}

func (c *RhizomeClient) ResolveOperatorQueue(ctx context.Context, input OperatorQueueResolveInput) error {
	hydrated, err := c.hydrateOperatorQueueBaseVersion(ctx, strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.QueueID), strings.TrimSpace(input.QueueKey), input.CurrentRevision, strings.TrimSpace(input.CurrentUpdatedAt))
	if err != nil {
		return err
	}
	input.CurrentRevision = hydrated.CurrentRevision
	input.CurrentUpdatedAt = hydrated.CurrentUpdatedAt
	err = c.call(ctx, "workspace.ops.resolve", input, nil)
	if isOperatorQueueAlreadyClosedError(err) {
		return nil
	}
	return err
}

func isOperatorQueueAlreadyClosedError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "operator queue item is not open")
}

func (c *RhizomeClient) WriteClaim(ctx context.Context, input KnowledgeClaimWriteInput) error {
	return c.call(ctx, "workspace.claim.write", input, nil)
}

func (c *RhizomeClient) ListMCPServers(ctx context.Context, workspaceID string) ([]MCPServerRecord, error) {
	var resp struct {
		Servers []MCPServerRecord `json:"servers"`
	}
	err := c.call(ctx, "mcp.server.list", map[string]any{
		"workspace_id": workspaceID,
	}, &resp)
	return resp.Servers, err
}

func (c *RhizomeClient) ListMCPTools(ctx context.Context, workspaceID string) ([]MCPToolRecord, error) {
	var resp struct {
		Tools []MCPToolRecord `json:"tools"`
	}
	err := c.call(ctx, "mcp.tool.list", map[string]any{
		"workspace_id": workspaceID,
	}, &resp)
	return resp.Tools, err
}

func (c *RhizomeClient) ListWorkspaceTools(ctx context.Context, workspaceID string) ([]WorkspaceToolRecord, error) {
	var resp struct {
		Tools []WorkspaceToolRecord `json:"tools"`
	}
	err := c.call(ctx, "tool.list", map[string]any{
		"workspace_id": workspaceID,
	}, &resp)
	return resp.Tools, err
}

func (c *RhizomeClient) CallWorkspaceTool(ctx context.Context, input WorkspaceToolCallInput) (WorkspaceToolCallResult, error) {
	var resp WorkspaceToolCallResult
	err := c.call(ctx, "tool.call", input, &resp)
	return resp, err
}

func (c *RhizomeClient) CallMCPTool(ctx context.Context, serverID, toolName string, arguments map[string]any) (MCPToolCallResult, error) {
	var resp MCPToolCallResult
	err := c.call(ctx, "mcp.tool.call", map[string]any{
		"server_id": serverID,
		"tool_name": toolName,
		"arguments": arguments,
	}, &resp)
	return resp, err
}

func (c *RhizomeClient) StateGet(ctx context.Context, workspaceID, agentID, key string) (string, bool, error) {
	var resp struct {
		Value string `json:"value"`
	}
	err := c.call(ctx, "agent.state.get", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"key":          key,
	}, &resp)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return "", false, nil
		}
		return "", false, err
	}
	return resp.Value, true, nil
}

func (c *RhizomeClient) StateSet(ctx context.Context, workspaceID, agentID, key, value string) error {
	if strings.TrimSpace(key) == runtimeScratchStateKey && strings.TrimSpace(value) != "" {
		var state RuntimeScratchState
		if err := json.Unmarshal([]byte(value), &state); err == nil {
			state = normalizeScratchStateForPersist(clearConsumedPendingTriggerInState(state))
			if raw, err := json.Marshal(state); err == nil {
				value = string(raw)
			}
		}
	}
	return c.call(ctx, "agent.state.set", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"key":          key,
		"value":        value,
	}, nil)
}

func (c *RhizomeClient) StateDelete(ctx context.Context, workspaceID, agentID, key string) error {
	return c.call(ctx, "agent.state.delete", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"key":          key,
	}, nil)
}

func (c *RhizomeClient) StateList(ctx context.Context, workspaceID, agentID string) ([]AgentStateEntry, error) {
	var resp struct {
		Entries []AgentStateEntry `json:"entries"`
	}
	err := c.call(ctx, "agent.state.list", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
	}, &resp)
	return resp.Entries, err
}

func (c *RhizomeClient) DescribeRPCMethod(ctx context.Context, method string) (RPCMethodSchema, bool, error) {
	method = strings.TrimSpace(method)
	if method == "" {
		return RPCMethodSchema{}, false, errors.New("rpc method is required")
	}
	var resp RPCMethodSchema
	err := c.call(ctx, "rpc.describe", map[string]any{"method": method}, &resp)
	if err != nil {
		if rpcDescribeTargetMethodUnknown(err, method) {
			return RPCMethodSchema{}, false, nil
		}
		return RPCMethodSchema{}, false, err
	}
	if strings.TrimSpace(resp.Method) == "" {
		resp.Method = method
	}
	return resp, true, nil
}

func rpcDescribeTargetMethodUnknown(err error, method string) bool {
	if err == nil {
		return false
	}
	method = strings.ToLower(strings.TrimSpace(method))
	if method == "" {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unknown method: "+method) ||
		strings.Contains(message, "method not found: "+method)
}

func rpcDescribeMethodUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unknown method: rpc.describe") ||
		strings.Contains(message, "unexpected method rpc.describe") ||
		strings.Contains(message, "unexpected method: rpc.describe") ||
		strings.Contains(message, "method not found: rpc.describe") ||
		strings.Contains(message, "rpc.describe not implemented") ||
		strings.Contains(message, "not implemented: rpc.describe")
}

func (c *RhizomeClient) AcquireHeartbeatLease(ctx context.Context, input AgentHeartbeatLeaseAcquireInput) (AgentHeartbeatLeaseResult, error) {
	var resp AgentHeartbeatLeaseEnvelope
	err := c.call(ctx, "agent.heartbeat_lease.acquire", input, &resp)
	return resp.Lease, err
}

func (c *RhizomeClient) RefreshHeartbeatLease(ctx context.Context, input AgentHeartbeatLeaseAcquireInput) (AgentHeartbeatLeaseResult, error) {
	var resp AgentHeartbeatLeaseEnvelope
	err := c.call(ctx, "agent.heartbeat_lease.refresh", input, &resp)
	return resp.Lease, err
}

func (c *RhizomeClient) ReleaseHeartbeatLease(ctx context.Context, input AgentHeartbeatLeaseReleaseInput) (AgentHeartbeatLeaseReleaseResult, error) {
	var resp AgentHeartbeatLeaseReleaseResult
	err := c.call(ctx, "agent.heartbeat_lease.release", input, &resp)
	return resp, err
}

func (c *RhizomeClient) ListPendingRequests(ctx context.Context, workspaceID, agentID string) ([]AgentRequestRecord, error) {
	var resp struct {
		Requests []AgentRequestRecord `json:"requests"`
	}
	err := c.call(ctx, "agent.request.list", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
	}, &resp)
	return resp.Requests, err
}

func (c *RhizomeClient) ListOpenRequests(ctx context.Context, workspaceID, agentID string, limit int) ([]AgentRequestRecord, error) {
	var resp struct {
		Requests []AgentRequestRecord `json:"requests"`
	}
	err := c.call(ctx, "agent.request.open.list", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"limit":        limit,
	}, &resp)
	return resp.Requests, err
}

func (c *RhizomeClient) RespondRequest(ctx context.Context, requestID, response string) error {
	response = clampAgentRespondResponse(response)
	return c.call(ctx, "agent.respond", map[string]any{
		"request_id": requestID,
		"response":   response,
	}, nil)
}

func clampAgentRespondResponse(response string) string {
	if len(response) <= maxAgentRespondResponseBytes {
		return response
	}
	originalBytes := len(response)
	suffix := fmt.Sprintf("\n\n[Rhizome runtime truncated agent.respond response from %d bytes to stay under the RPC transport limit. Publish full details through workspace docs or artifacts instead.]", originalBytes)
	budget := maxAgentRespondResponseBytes - len(suffix)
	if budget < 0 {
		return truncateUTF8Bytes(suffix, maxAgentRespondResponseBytes)
	}
	return truncateUTF8Bytes(response, budget) + suffix
}

func truncateUTF8Bytes(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	if end <= 0 {
		return ""
	}
	return value[:end]
}

func (c *RhizomeClient) RequestAgent(ctx context.Context, input AgentRequestInput) (AgentRequestCreateResult, error) {
	var resp AgentRequestCreateResult
	err := c.call(ctx, "agent.request", map[string]any{
		"workspace_id":  input.WorkspaceID,
		"from_agent_id": input.FromAgentID,
		"to_agent_id":   input.ToAgentID,
		"method":        input.Method,
		"payload_json":  input.PayloadJSON,
		"timeout_sec":   input.TimeoutSec,
	}, &resp)
	if err != nil {
		return resp, err
	}
	resp.RequestID = strings.TrimSpace(resp.RequestID)
	resp.WorkspaceID = strings.TrimSpace(resp.WorkspaceID)
	resp.ToAgentID = strings.TrimSpace(resp.ToAgentID)
	resp.Status = strings.TrimSpace(resp.Status)
	if resp.RequestID == "" {
		return AgentRequestCreateResult{}, fmt.Errorf("agent.request returned partial result: missing request_id")
	}
	if resp.WorkspaceID != "" && strings.TrimSpace(input.WorkspaceID) != "" && resp.WorkspaceID != strings.TrimSpace(input.WorkspaceID) {
		return AgentRequestCreateResult{}, fmt.Errorf("agent.request returned mismatched workspace_id %q (wanted %q)", resp.WorkspaceID, strings.TrimSpace(input.WorkspaceID))
	}
	if resp.ToAgentID != "" && strings.TrimSpace(input.ToAgentID) != "" && resp.ToAgentID != strings.TrimSpace(input.ToAgentID) {
		return AgentRequestCreateResult{}, fmt.Errorf("agent.request returned mismatched to_agent_id %q (wanted %q)", resp.ToAgentID, strings.TrimSpace(input.ToAgentID))
	}
	return resp, nil
}

func (c *RhizomeClient) GetAgentRequestResult(ctx context.Context, workspaceID, requestID string) (AgentRequestRecord, error) {
	var resp AgentRequestRecord
	err := c.call(ctx, "agent.request.result", map[string]any{
		"workspace_id": workspaceID,
		"request_id":   requestID,
	}, &resp)
	if err != nil {
		return resp, err
	}
	resp.RequestID = strings.TrimSpace(resp.RequestID)
	resp.WorkspaceID = strings.TrimSpace(resp.WorkspaceID)
	resp.FromAgentID = strings.TrimSpace(resp.FromAgentID)
	resp.ToAgentID = strings.TrimSpace(resp.ToAgentID)
	resp.Method = strings.TrimSpace(resp.Method)
	resp.Payload = strings.TrimSpace(resp.Payload)
	resp.Status = strings.TrimSpace(resp.Status)
	resp.Response = strings.TrimSpace(resp.Response)
	resp.CreatedAt = strings.TrimSpace(resp.CreatedAt)
	resp.RespondedAt = strings.TrimSpace(resp.RespondedAt)
	requestID = strings.TrimSpace(requestID)
	workspaceID = strings.TrimSpace(workspaceID)
	if resp.RequestID == "" {
		return AgentRequestRecord{}, fmt.Errorf("agent.request.result returned partial result: missing request_id")
	}
	if requestID != "" && resp.RequestID != requestID {
		return AgentRequestRecord{}, fmt.Errorf("agent.request.result returned mismatched request_id %q (wanted %q)", resp.RequestID, requestID)
	}
	if resp.WorkspaceID != "" && workspaceID != "" && resp.WorkspaceID != workspaceID {
		return AgentRequestRecord{}, fmt.Errorf("agent.request.result returned mismatched workspace_id %q (wanted %q)", resp.WorkspaceID, workspaceID)
	}
	return resp, nil
}

func shouldFallbackWorkspaceOpsRequest(err error) bool {
	return shouldFallbackLegacyRegistration(err)
}

type operatorQueueBaseVersion struct {
	CurrentRevision  int64
	CurrentUpdatedAt string
}

func (c *RhizomeClient) hydrateOperatorQueueBaseVersion(ctx context.Context, workspaceID, queueID, queueKey string, currentRevision int64, currentUpdatedAt string) (operatorQueueBaseVersion, error) {
	hydrated := operatorQueueBaseVersion{
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: strings.TrimSpace(currentUpdatedAt),
	}
	if hydrated.CurrentRevision > 0 || hydrated.CurrentUpdatedAt != "" {
		return hydrated, nil
	}
	item, ok, err := c.GetOperatorQueue(ctx, workspaceID, queueID, queueKey)
	if err != nil || !ok {
		return hydrated, err
	}
	hydrated.CurrentRevision = item.Revision
	hydrated.CurrentUpdatedAt = strings.TrimSpace(item.UpdatedAt)
	return hydrated, nil
}

func legacyOperatorQueueInputFromGate(input ExternalGateRequestInput) OperatorQueueUpsertInput {
	queueType := "BLOCKER"
	if strings.EqualFold(strings.TrimSpace(input.GateType), "EXPLICIT_APPROVAL") {
		queueType = "DECISION"
	}
	return OperatorQueueUpsertInput{
		WorkspaceID:       strings.TrimSpace(input.WorkspaceID),
		QueueKey:          externalGateQueueKey(strings.TrimSpace(input.GateType), strings.TrimSpace(input.RequestKey)),
		QueueType:         queueType,
		Title:             strings.TrimSpace(input.Title),
		Summary:           strings.TrimSpace(input.Summary),
		Details:           strings.TrimSpace(input.Details),
		AssignedTo:        strings.TrimSpace(input.AssignedTo),
		Urgency:           strings.TrimSpace(input.Urgency),
		SourceKind:        firstNonEmpty(strings.TrimSpace(input.SourceKind), "external_gate"),
		SourceID:          firstNonEmpty(strings.TrimSpace(input.SourceID), strings.TrimSpace(input.RequestKey)),
		TaskID:            strings.TrimSpace(input.TaskID),
		SessionID:         strings.TrimSpace(input.SessionID),
		AgentID:           strings.TrimSpace(input.AgentID),
		KeepSessionActive: input.KeepSessionActive,
		DueAt:             strings.TrimSpace(input.DueAt),
		CurrentRevision:   input.CurrentRevision,
		CurrentUpdatedAt:  strings.TrimSpace(input.CurrentUpdatedAt),
	}
}

func externalGateQueueKey(gateType, requestKey string) string {
	normalizedGate := strings.ToLower(strings.TrimSpace(gateType))
	normalizedGate = strings.ReplaceAll(normalizedGate, " ", "_")
	normalizedGate = strings.ReplaceAll(normalizedGate, "-", "_")
	if normalizedGate == "" {
		normalizedGate = "external_gate"
	}
	requestKey = strings.TrimSpace(requestKey)
	if requestKey == "" {
		return "external_gate:" + normalizedGate
	}
	return "external_gate:" + normalizedGate + ":" + requestKey
}

type LimitGroupRecord struct {
	GroupID          string   `json:"group_id"`
	WorkspaceID      string   `json:"workspace_id"`
	Title            string   `json:"title"`
	OwnerName        string   `json:"owner_name"`
	SubscriptionTier string   `json:"subscription_tier"`
	DailyLimit       int      `json:"daily_limit"`
	WeeklyLimit      int      `json:"weekly_limit"`
	DailyRemaining   int      `json:"daily_remaining"`
	WeeklyRemaining  int      `json:"weekly_remaining"`
	LastReportedAt   string   `json:"last_reported_at,omitempty"`
	CreatedAt        string   `json:"created_at"`
	UpdatedAt        string   `json:"updated_at"`
	Agents           []string `json:"agents"`
}

func (c *RhizomeClient) GetAgentLimits(ctx context.Context, workspaceID, agentID string) (*LimitGroupRecord, error) {
	req := struct {
		WorkspaceID string `json:"workspace_id"`
		AgentID     string `json:"agent_id"`
	}{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	}
	var res LimitGroupRecord
	if err := c.call(ctx, "agent.limits.get", req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

type LimitReportInput struct {
	GroupID         string `json:"group_id"`
	AgentID         string `json:"agent_id"`
	DailyRemaining  int    `json:"daily_remaining"`
	WeeklyRemaining int    `json:"weekly_remaining"`
}

func (c *RhizomeClient) ReportLimits(ctx context.Context, input LimitReportInput) error {
	var res map[string]any
	return c.call(ctx, "limits.report", input, &res)
}

type BudgetAccountSnapshot struct {
	AccountID       string `json:"account_id"`
	PrincipalType   string `json:"principal_type"`
	PrincipalID     string `json:"principal_id"`
	WorkspaceID     string `json:"workspace_id"`
	Currency        string `json:"currency"`
	LimitMicros     int64  `json:"limit_micros"`
	ReservedMicros  int64  `json:"reserved_micros"`
	SpentMicros     int64  `json:"spent_micros"`
	RefundedMicros  int64  `json:"refunded_micros"`
	AvailableMicros int64  `json:"available_micros"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type BudgetAccountEnsureInput struct {
	AccountID     string `json:"account_id"`
	PrincipalType string `json:"principal_type"`
	PrincipalID   string `json:"principal_id"`
	WorkspaceID   string `json:"workspace_id"`
	Currency      string `json:"currency"`
	LimitMicros   int64  `json:"limit_micros"`
	Status        string `json:"status"`
}

type BudgetReserveInput struct {
	ReservationID  string `json:"reservation_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	AmountMicros   int64  `json:"amount_micros"`
	Reason         string `json:"reason"`
}

type BudgetSpendInput struct {
	EntryID        string `json:"entry_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	ReservationID  string `json:"reservation_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	AmountMicros   int64  `json:"amount_micros"`
	Reason         string `json:"reason"`
}

type BudgetReleaseInput struct {
	EntryID        string `json:"entry_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	ReservationID  string `json:"reservation_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	AmountMicros   int64  `json:"amount_micros"`
	Reason         string `json:"reason"`
}

type BudgetReservationListInput struct {
	AccountID   string `json:"account_id,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	RunID       string `json:"run_id,omitempty"`
	Status      string `json:"status,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type BudgetReservationRecord struct {
	ReservationID   string `json:"reservation_id"`
	IdempotencyKey  string `json:"idempotency_key"`
	AccountID       string `json:"account_id"`
	AmountMicros    int64  `json:"amount_micros"`
	SpentMicros     int64  `json:"spent_micros"`
	ReleasedMicros  int64  `json:"released_micros"`
	RemainingMicros int64  `json:"remaining_micros"`
	Status          string `json:"status"`
	WorkspaceID     string `json:"workspace_id"`
	AgentID         string `json:"agent_id"`
	TaskID          string `json:"task_id"`
	RunID           string `json:"run_id"`
	ProviderID      string `json:"provider_id"`
	Model           string `json:"model"`
	Reason          string `json:"reason,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type BudgetLedgerListInput struct {
	AccountID     string `json:"account_id,omitempty"`
	ReservationID string `json:"reservation_id,omitempty"`
	WorkspaceID   string `json:"workspace_id,omitempty"`
	TaskID        string `json:"task_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

type BudgetLedgerEntryRecord struct {
	EntryID        string `json:"entry_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	ReservationID  string `json:"reservation_id,omitempty"`
	EntryType      string `json:"entry_type"`
	AmountMicros   int64  `json:"amount_micros"`
	Currency       string `json:"currency"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	SourceEntryID  string `json:"source_entry_id,omitempty"`
	Reason         string `json:"reason,omitempty"`
	CreatedAt      string `json:"created_at"`
}

func (c *RhizomeClient) EnsureBudgetAccount(ctx context.Context, input BudgetAccountEnsureInput) (BudgetAccountSnapshot, error) {
	var resp struct {
		Account BudgetAccountSnapshot `json:"account"`
	}
	err := c.call(ctx, "budget.account.ensure", input, &resp)
	return resp.Account, err
}

func (c *RhizomeClient) ReserveBudget(ctx context.Context, input BudgetReserveInput) (BudgetAccountSnapshot, error) {
	var resp struct {
		Account BudgetAccountSnapshot `json:"account"`
	}
	err := c.call(ctx, "budget.reserve", input, &resp)
	return resp.Account, err
}

func (c *RhizomeClient) CaptureBudgetSpend(ctx context.Context, input BudgetSpendInput) (BudgetAccountSnapshot, error) {
	var resp struct {
		Account BudgetAccountSnapshot `json:"account"`
	}
	err := c.call(ctx, "budget.spend", input, &resp)
	return resp.Account, err
}

func (c *RhizomeClient) ReleaseBudget(ctx context.Context, input BudgetReleaseInput) (BudgetAccountSnapshot, error) {
	var resp struct {
		Account BudgetAccountSnapshot `json:"account"`
	}
	err := c.call(ctx, "budget.release", input, &resp)
	return resp.Account, err
}

func (c *RhizomeClient) ListBudgetReservations(ctx context.Context, input BudgetReservationListInput) ([]BudgetReservationRecord, error) {
	var resp struct {
		Reservations []BudgetReservationRecord `json:"reservations"`
	}
	err := c.call(ctx, "budget.reservations.list", input, &resp)
	return resp.Reservations, err
}

func (c *RhizomeClient) ListBudgetLedgerEntries(ctx context.Context, input BudgetLedgerListInput) ([]BudgetLedgerEntryRecord, error) {
	var resp struct {
		Entries []BudgetLedgerEntryRecord `json:"entries"`
	}
	err := c.call(ctx, "budget.ledger.list", input, &resp)
	return resp.Entries, err
}

func newRuntimeID(prefix string) string {
	now := time.Now().UTC().UnixNano()
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s-%d", prefix, now)
	}
	return fmt.Sprintf("%s-%d-%s", prefix, now, hex.EncodeToString(buf))
}

type RhizomeEvent struct {
	Type        string `json:"type"`
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id,omitempty"`
	Summary     string `json:"summary"`
	Timestamp   string `json:"timestamp"`
	PayloadJSON string `json:"payload_json,omitempty"`
}

func (c *RhizomeClient) SubscribeEvents(ctx context.Context, workspaceID string) (<-chan RhizomeEvent, error) {
	if c == nil {
		return nil, errors.New("rhizome client is nil")
	}
	if strings.TrimSpace(workspaceID) == "" {
		return nil, errors.New("workspace_id cannot be empty")
	}

	eventChan := make(chan RhizomeEvent, 100)

	go func() {
		defer close(eventChan)
		backoff := 1 * time.Second
		maxBackoff := 30 * time.Second

		for {
			if ctx.Err() != nil {
				return
			}

			reqURL := c.endpoint
			if idx := strings.LastIndex(reqURL, "/rpc"); idx != -1 {
				reqURL = reqURL[:idx] + "/events" + reqURL[idx+4:]
			} else if strings.HasSuffix(reqURL, "/") {
				reqURL += "events"
			} else {
				reqURL += "/events"
			}

			reqURL += "?workspace_id=" + workspaceID

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				continue
			}

			req.Header.Set("Accept", "text/event-stream")
			req.Header.Set("Cache-Control", "no-cache")
			req.Header.Set("Connection", "keep-alive")
			if c.token != "" {
				req.Header.Set("Authorization", "Bearer "+c.token)
			}

			resp, err := c.client.Do(req)
			if err != nil || resp.StatusCode >= 400 {
				if resp != nil && resp.Body != nil {
					resp.Body.Close()
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			backoff = 1 * time.Second
			reader := bufio.NewReader(resp.Body)
			var currentEvent RhizomeEvent
			var currentType string

			for {
				if ctx.Err() != nil {
					break
				}
				lineBytes, err := reader.ReadBytes('\n')
				if err != nil {
					break
				}

				line := string(lineBytes)
				line = strings.TrimSuffix(line, "\n")
				line = strings.TrimSuffix(line, "\r")

				// Server keep-alive comment or initial heartbeat
				if strings.HasPrefix(line, ":") {
					continue
				}

				if line == "" {
					if currentType != "" && currentEvent.Type == currentType {
						select {
						case eventChan <- currentEvent:
						case <-ctx.Done():
							resp.Body.Close()
							return
						}
					}
					currentEvent = RhizomeEvent{}
					currentType = ""
					continue
				}

				if strings.HasPrefix(line, "event: ") {
					currentType = strings.TrimPrefix(line, "event: ")
					currentEvent.Type = currentType
				} else if strings.HasPrefix(line, "data: ") {
					dataStr := strings.TrimPrefix(line, "data: ")
					if currentType != "" {
						_ = json.Unmarshal([]byte(dataStr), &currentEvent)
					}
				}
			}

			resp.Body.Close()
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}()

	return eventChan, nil
}
