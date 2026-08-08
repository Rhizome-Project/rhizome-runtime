package main

import "encoding/json"

type WorkspaceRecord struct {
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	CreatedBy   string `json:"created_by"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type WorkspaceDocRecord struct {
	DocKey     string  `json:"doc_key"`
	Title      string  `json:"title"`
	Content    string  `json:"content"`
	UpdatedBy  string  `json:"updated_by"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
	ArchivedAt *string `json:"archived_at,omitempty"`
	ArchivedBy *string `json:"archived_by,omitempty"`
	SHA        string  `json:"sha"`
}

type AgentCurrentTask struct {
	TaskID      string `json:"task_id"`
	ClaimStatus string `json:"claim_status"`
	Summary     string `json:"summary"`
}

type BlockedRef struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

type ArtifactRef struct {
	Ref         string `json:"ref"`
	Kind        string `json:"kind,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

type AgentSessionStateRecord struct {
	SessionID           string        `json:"session_id"`
	WorkspaceID         string        `json:"workspace_id"`
	AgentID             string        `json:"agent_id"`
	TaskID              string        `json:"task_id,omitempty"`
	Status              string        `json:"status"`
	Summary             string        `json:"summary"`
	OwnerScope          string        `json:"owner_scope,omitempty"`
	BlockedOn           []BlockedRef  `json:"blocked_on,omitempty"`
	DecisionNeededFrom  string        `json:"decision_needed_from,omitempty"`
	DecisionType        string        `json:"decision_type,omitempty"`
	KeepSessionActive   *bool         `json:"keep_session_active,omitempty"`
	HandoffTo           string        `json:"handoff_to,omitempty"`
	RelatedDocKeys      []string      `json:"related_doc_keys,omitempty"`
	RelatedArtifactRefs []ArtifactRef `json:"related_artifact_refs,omitempty"`
	UpdateType          string        `json:"update_type,omitempty"`
	UpdatedAt           string        `json:"updated_at"`
	StartedAt           string        `json:"started_at"`
	CompletedAt         string        `json:"completed_at,omitempty"`
}

type AgentRecord struct {
	AgentID         string                   `json:"agent_id"`
	WorkspaceID     string                   `json:"workspace_id"`
	OwnerUserID     string                   `json:"owner_user_id"`
	DisplayName     string                   `json:"display_name"`
	Role            string                   `json:"role"`
	Status          string                   `json:"status"`
	ProtocolVersion string                   `json:"protocol_version"`
	Capabilities    []string                 `json:"capabilities"`
	Summary         string                   `json:"summary"`
	CreatedAt       string                   `json:"created_at"`
	UpdatedAt       string                   `json:"updated_at"`
	LastSeenAt      *string                  `json:"last_seen_at,omitempty"`
	IsOnline        bool                     `json:"is_online"`
	ActiveTasks     []AgentCurrentTask       `json:"active_tasks"`
	CurrentSession  *AgentSessionStateRecord `json:"current_session,omitempty"`
}

type WorkspaceToolRecord struct {
	ToolID       string   `json:"tool_id"`
	WorkspaceID  string   `json:"workspace_id"`
	DisplayName  string   `json:"display_name"`
	Description  string   `json:"description"`
	OwnerUserID  string   `json:"owner_user_id"`
	OwnerAgentID string   `json:"owner_agent_id,omitempty"`
	Kind         string   `json:"kind"`
	Status       string   `json:"status"`
	Version      string   `json:"version"`
	AccessLevel  string   `json:"access_level"`
	Endpoint     string   `json:"endpoint,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	ManifestJSON string   `json:"manifest_json,omitempty"`
	CreatedAt    string   `json:"created_at"`
	UpdatedAt    string   `json:"updated_at"`
}

type WorkspaceTaskLinkRecord struct {
	WorkspaceID string `json:"workspace_id"`
	FromTaskID  string `json:"from_task_id"`
	ToTaskID    string `json:"to_task_id"`
	LinkType    string `json:"link_type"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   string `json:"created_at"`
}

type WorkspaceTaskRecord struct {
	TaskID               string   `json:"task_id"`
	Title                string   `json:"title,omitempty"`
	Description          string   `json:"description,omitempty"`
	OwnerUserID          string   `json:"owner_user_id"`
	Priority             string   `json:"priority"`
	Status               string   `json:"status"`
	TaskKind             string   `json:"task_kind"`
	TaskTemplate         string   `json:"task_template"`
	ProjectID            string   `json:"project_id,omitempty"`
	ProjectLane          string   `json:"project_lane,omitempty"`
	RequiresProjectGate  *bool    `json:"requires_project_gate,omitempty"`
	TaskRequirementsJSON string   `json:"task_requirements_json,omitempty"`
	WriteScopeHints      []string `json:"write_scope_hints,omitempty"`
	CloseReason          string   `json:"close_reason,omitempty"`
	Tags                 []string `json:"tags,omitempty"`
	LinkedBy             string   `json:"linked_by"`
	LinkedAt             string   `json:"linked_at"`
	UpdatedAt            string   `json:"updated_at,omitempty"`
	ClaimAgentID         *string  `json:"claim_agent_id,omitempty"`
	ClaimStatus          *string  `json:"claim_status,omitempty"`
	ClaimSummary         *string  `json:"claim_summary,omitempty"`
	ClaimUpdatedAt       *string  `json:"claim_updated_at,omitempty"`
	ClaimExpiresAt       *string  `json:"claim_expires_at,omitempty"`
	ClaimProjectRoleID   *string  `json:"claim_project_role_id,omitempty"`
	ClaimRepoID          *string  `json:"claim_repo_id,omitempty"`
	ClaimCheckoutID      *string  `json:"claim_checkout_id,omitempty"`
	ClaimBranchID        *string  `json:"claim_branch_id,omitempty"`
	ClaimWriteScopeJSON  *string  `json:"claim_write_scope_json,omitempty"`
}

type AgentUpdateRecord struct {
	UpdateID      string `json:"update_id"`
	AgentID       string `json:"agent_id"`
	AgentName     string `json:"agent_name"`
	UpdateType    string `json:"update_type"`
	Summary       string `json:"summary"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	RequiresHuman bool   `json:"requires_human"`
	CreatedAt     string `json:"created_at"`
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

type RuntimeEventListInput struct {
	WorkspaceID string `json:"workspace_id"`
	EventType   string `json:"event_type,omitempty"`
	EntityType  string `json:"entity_type,omitempty"`
	EntityID    string `json:"entity_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type AgentUpdateSideEffectV1 struct {
	Schema               string   `json:"schema"`
	SideEffectRef        string   `json:"side_effect_ref"`
	Actor                string   `json:"actor"`
	LaneRef              string   `json:"lane_ref"`
	TensionRef           string   `json:"tension_ref"`
	ArtifactRef          string   `json:"artifact_ref"`
	RegionRef            string   `json:"region_ref"`
	Operation            string   `json:"operation"`
	SourceKind           string   `json:"source_kind"`
	BoundaryRef          string   `json:"boundary_ref"`
	BoundaryRelation     string   `json:"boundary_relation"`
	MaterializationState string   `json:"materialization_state"`
	IntegrationIntent    string   `json:"integration_intent"`
	IntegrationStatus    string   `json:"integration_status"`
	Decision             string   `json:"decision"`
	Justification        string   `json:"justification"`
	DerivedRegionRefs    []string `json:"derived_region_refs"`
}

type NewsRecord struct {
	NewsID      string `json:"news_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	AuthorID    string `json:"author_id"`
	AuthorType  string `json:"author_type"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type WorkspaceMemoryRecord struct {
	MemoryID       string   `json:"memory_id"`
	WorkspaceID    string   `json:"workspace_id"`
	MemoryType     string   `json:"memory_type"`
	Title          string   `json:"title"`
	Body           string   `json:"body"`
	Summary        string   `json:"summary"`
	AgentID        string   `json:"agent_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	SourceKind     string   `json:"source_kind"`
	SourceID       string   `json:"source_id,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	Importance     float64  `json:"importance"`
	Confidence     float64  `json:"confidence"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
	ArchivedAt     *string  `json:"archived_at,omitempty"`
	ArchivedBy     *string  `json:"archived_by,omitempty"`
	ArchivedReason string   `json:"archived_reason,omitempty"`
}

type WorkspaceArtifactRecord struct {
	ArtifactID   string  `json:"artifact_id"`
	WorkspaceID  string  `json:"workspace_id"`
	TaskID       *string `json:"task_id,omitempty"`
	UpdateID     *string `json:"update_id,omitempty"`
	Title        string  `json:"title"`
	ArtifactRef  string  `json:"artifact_ref"`
	Kind         string  `json:"kind"`
	ContentType  string  `json:"content_type"`
	CreatedBy    string  `json:"created_by"`
	MetadataJSON string  `json:"metadata_json,omitempty"`
	CreatedAt    string  `json:"created_at"`
}

type MessageRecord struct {
	MessageID    string `json:"message_id"`
	WorkspaceID  string `json:"workspace_id"`
	FromAgentID  string `json:"from_agent_id"`
	ToAgentID    string `json:"to_agent_id"`
	Channel      string `json:"channel"`
	ContentType  string `json:"content_type"`
	Content      string `json:"content"`
	MetadataJSON string `json:"metadata_json"`
	CreatedAt    string `json:"created_at"`
	ReadAt       string `json:"read_at,omitempty"`
}

type ProjectRecord struct {
	ProjectID   string `json:"project_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Status      string `json:"status"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	TaskCount   int    `json:"task_count"`
}

type ProjectProfileRecord struct {
	WorkspaceID             string `json:"workspace_id"`
	ProjectID               string `json:"project_id"`
	Goal                    string `json:"goal"`
	CurrentPhase            string `json:"current_phase"`
	DesignDocID             string `json:"design_doc_id,omitempty"`
	ImplementationPlanDocID string `json:"implementation_plan_doc_id,omitempty"`
	RepoRequired            bool   `json:"repo_required"`
	RepoStatus              string `json:"repo_status"`
	RepoURL                 string `json:"repo_url,omitempty"`
	RepoDefaultBranch       string `json:"repo_default_branch,omitempty"`
	UpdatedBy               string `json:"updated_by,omitempty"`
	CreatedAt               string `json:"created_at"`
	UpdatedAt               string `json:"updated_at"`
}

type ProjectGateRecord struct {
	GateKey     string `json:"gate_key"`
	State       string `json:"state"`
	Required    bool   `json:"required"`
	Summary     string `json:"summary,omitempty"`
	EvidenceRef string `json:"evidence_ref,omitempty"`
	UpdatedBy   string `json:"updated_by,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	Source      string `json:"source"`
}

type ProjectGateStatusRecord struct {
	WorkspaceID         string              `json:"workspace_id"`
	ProjectID           string              `json:"project_id"`
	CurrentPhase        string              `json:"current_phase"`
	OverallState        string              `json:"overall_state"`
	ImplementationReady bool                `json:"implementation_ready"`
	Gates               []ProjectGateRecord `json:"gates"`
	UpdatedAt           string              `json:"updated_at"`
}

type ProjectRoleRecord struct {
	RoleID         string `json:"role_id"`
	WorkspaceID    string `json:"workspace_id"`
	ProjectID      string `json:"project_id"`
	AgentID        string `json:"agent_id"`
	RoleType       string `json:"role_type"`
	Status         string `json:"status"`
	WriteScopeJSON string `json:"write_scope_json,omitempty"`
	LeaseToken     string `json:"lease_token,omitempty"`
	LeaseExpiresAt string `json:"lease_expires_at,omitempty"`
	Summary        string `json:"summary,omitempty"`
	ClaimedAt      string `json:"claimed_at,omitempty"`
	ReleasedAt     string `json:"released_at,omitempty"`
	UpdatedBy      string `json:"updated_by,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type ProjectRoleAssignResult struct {
	Role              ProjectRoleRecord `json:"role"`
	ActiveClaimRebind map[string]any    `json:"active_claim_rebind,omitempty"`
}

type GovernanceStallPredicateResult struct {
	Name         string   `json:"name"`
	Holds        bool     `json:"holds"`
	Summary      string   `json:"summary,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type GovernanceChallengeRecord struct {
	ChallengeID               string                           `json:"challenge_id"`
	WorkspaceID               string                           `json:"workspace_id"`
	ProjectID                 string                           `json:"project_id"`
	ChallengedAgentID         string                           `json:"challenged_agent_id"`
	ChallengerAgentID         string                           `json:"challenger_agent_id"`
	NominatedSuccessorAgentID string                           `json:"nominated_successor_agent_id,omitempty"`
	LeadRoleID                string                           `json:"lead_role_id,omitempty"`
	TensionID                 string                           `json:"tension_id,omitempty"`
	State                     string                           `json:"state"`
	CurrentRound              int                              `json:"current_round"`
	MaxRounds                 int                              `json:"max_rounds"`
	StallPredicates           []string                         `json:"stall_predicates,omitempty"`
	PredicateResults          []GovernanceStallPredicateResult `json:"predicate_results,omitempty"`
	EvidenceRefs              []string                         `json:"evidence_refs,omitempty"`
	ArgumentDocKey            string                           `json:"argument_doc_key,omitempty"`
	DefenseDocKey             string                           `json:"defense_doc_key,omitempty"`
	DefenseStance             string                           `json:"defense_stance,omitempty"`
	RoundOpenedAt             string                           `json:"round_opened_at"`
	DefenseDeadlineAt         string                           `json:"defense_deadline_at,omitempty"`
	VotingDeadlineAt          string                           `json:"voting_deadline_at,omitempty"`
	CreatedAt                 string                           `json:"created_at"`
	UpdatedAt                 string                           `json:"updated_at"`
	ResolvedAt                string                           `json:"resolved_at,omitempty"`
	Resolution                string                           `json:"resolution,omitempty"`
}

type GovernanceVoteRecord struct {
	VoteID          string `json:"vote_id"`
	ChallengeID     string `json:"challenge_id"`
	WorkspaceID     string `json:"workspace_id"`
	Round           int    `json:"round"`
	VoterAgentID    string `json:"voter_agent_id"`
	Ballot          string `json:"ballot"`
	RationaleDocKey string `json:"rationale_doc_key,omitempty"`
	CastAt          string `json:"cast_at"`
}

type GovernanceTallyResult struct {
	Challenge       GovernanceChallengeRecord `json:"challenge"`
	LeadRole        *ProjectRoleRecord        `json:"lead_role,omitempty"`
	ElectorateCount int                       `json:"electorate_count"`
	QuorumThreshold int                       `json:"quorum_threshold"`
	UpholdVotes     int                       `json:"uphold_votes"`
	ReassignVotes   int                       `json:"reassign_votes"`
	AbstainVotes    int                       `json:"abstain_votes"`
}

type ProjectRepositoryRecord struct {
	RepoID                 string `json:"repo_id"`
	WorkspaceID            string `json:"workspace_id"`
	ProjectID              string `json:"project_id"`
	RemoteURL              string `json:"remote_url,omitempty"`
	RemoteKind             string `json:"remote_kind"`
	Owner                  string `json:"owner,omitempty"`
	Name                   string `json:"name,omitempty"`
	DefaultBranch          string `json:"default_branch,omitempty"`
	IntegrationBranch      string `json:"integration_branch,omitempty"`
	CredentialVaultEntryID string `json:"credential_vault_entry_id,omitempty"`
	RepoStatus             string `json:"repo_status"`
	IsCanonical            bool   `json:"is_canonical"`
	CreatedByAgentID       string `json:"created_by_agent_id,omitempty"`
	UpdatedBy              string `json:"updated_by,omitempty"`
	CreatedAt              string `json:"created_at"`
	UpdatedAt              string `json:"updated_at"`
}

type ProjectCheckoutRecord struct {
	CheckoutID   string `json:"checkout_id"`
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	RepoID       string `json:"repo_id"`
	MachineID    string `json:"machine_id"`
	MachineLabel string `json:"machine_label,omitempty"`
	OwnerUserID  string `json:"owner_user_id,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	LocalPath    string `json:"local_path"`
	CheckoutKind string `json:"checkout_kind"`
	BranchName   string `json:"branch_name,omitempty"`
	BaseBranch   string `json:"base_branch,omitempty"`
	HeadSHA      string `json:"head_sha,omitempty"`
	BaseSHA      string `json:"base_sha,omitempty"`
	DirtyState   string `json:"dirty_state"`
	ActiveTaskID string `json:"active_task_id,omitempty"`
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID string `json:"active_claim_id,omitempty"`
	Status        string `json:"status"`
	DerivedStatus string `json:"derived_status,omitempty"`
	LastSeenAt    string `json:"last_seen_at"`
	UpdatedBy     string `json:"updated_by,omitempty"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

type ProjectBranchRecord struct {
	BranchID     string `json:"branch_id"`
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	RepoID       string `json:"repo_id"`
	CheckoutID   string `json:"checkout_id,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	ActiveTaskID string `json:"active_task_id,omitempty"`
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID  string `json:"active_claim_id,omitempty"`
	BranchName     string `json:"branch_name"`
	BranchKind     string `json:"branch_kind"`
	BaseBranch     string `json:"base_branch,omitempty"`
	HeadSHA        string `json:"head_sha,omitempty"`
	BaseSHA        string `json:"base_sha,omitempty"`
	WriteScopeJSON string `json:"write_scope_json,omitempty"`
	ReviewDocKey   string `json:"review_doc_key,omitempty"`
	Status         string `json:"status"`
	UpdatedBy      string `json:"updated_by,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type ProjectPatchQueueItemRecord struct {
	QueueID                     string                       `json:"queue_id"`
	ItemID                      string                       `json:"item_id"`
	WorkspaceID                 string                       `json:"workspace_id"`
	ProjectID                   string                       `json:"project_id"`
	RepoID                      string                       `json:"repo_id"`
	BranchID                    string                       `json:"branch_id"`
	ReviewDocKey                string                       `json:"review_doc_key"`
	SupersedesQueueID           string                       `json:"supersedes_queue_id,omitempty"`
	SupersedesItemID            string                       `json:"supersedes_item_id,omitempty"`
	EvidenceDocKey              string                       `json:"evidence_doc_key,omitempty"`
	RepoAuthorityMode           string                       `json:"repo_authority_mode"`
	State                       string                       `json:"state"`
	Attempt                     int                          `json:"attempt"`
	MaxAttempts                 int                          `json:"max_attempts"`
	NextRetryAt                 string                       `json:"next_retry_at,omitempty"`
	DeadLetteredAt              string                       `json:"dead_lettered_at,omitempty"`
	Pathset                     []string                     `json:"pathset,omitempty"`
	PathsetJSON                 string                       `json:"pathset_json,omitempty"`
	BaseRef                     string                       `json:"base_ref,omitempty"`
	BaseSHA                     string                       `json:"base_sha,omitempty"`
	HeadSHA                     string                       `json:"head_sha,omitempty"`
	AutoMerge                   bool                         `json:"auto_merge"`
	SubmittedBy                 string                       `json:"submitted_by,omitempty"`
	TaskID                      string                       `json:"task_id,omitempty"`
	SessionID                   string                       `json:"session_id,omitempty"`
	RunID                       string                       `json:"run_id,omitempty"`
	AgentID                     string                       `json:"agent_id,omitempty"`
	PrincipalType               string                       `json:"principal_type,omitempty"`
	PrincipalID                 string                       `json:"principal_id,omitempty"`
	CapabilitySnapshotID        string                       `json:"capability_snapshot_id,omitempty"`
	CapabilitySnapshotSchema    string                       `json:"capability_snapshot_schema,omitempty"`
	RepoRoot                    string                       `json:"repo_root,omitempty"`
	BaseTreeHash                string                       `json:"base_tree_hash,omitempty"`
	BaseFileHashes              map[string]string            `json:"base_file_hashes,omitempty"`
	BaseFileHashesJSON          string                       `json:"base_file_hashes_json,omitempty"`
	ContextDigest               string                       `json:"context_digest,omitempty"`
	RepoLeaseID                 string                       `json:"repo_lease_id,omitempty"`
	LeaseTerm                   int64                        `json:"lease_term,omitempty"`
	OperationID                 string                       `json:"operation_id,omitempty"`
	OperationKind               string                       `json:"operation_kind,omitempty"`
	OperationBindingSchema      string                       `json:"operation_binding_schema,omitempty"`
	OperationBindingAccepted    bool                         `json:"operation_binding_accepted,omitempty"`
	OperationContextDigest      string                       `json:"operation_context_digest,omitempty"`
	OperationLeaseContextDigest string                       `json:"operation_lease_context_digest,omitempty"`
	OperationMutationPaths      []string                     `json:"operation_mutation_paths,omitempty"`
	OperationMutationPathsJSON  string                       `json:"operation_mutation_paths_json,omitempty"`
	OperationBoundBy            string                       `json:"operation_bound_by,omitempty"`
	OperationBoundAt            string                       `json:"operation_bound_at,omitempty"`
	CASEvidenceSchema           string                       `json:"cas_evidence_schema,omitempty"`
	CASEvidenceAccepted         bool                         `json:"cas_evidence_accepted,omitempty"`
	CASStatus                   string                       `json:"cas_status,omitempty"`
	CASPatchDigest              string                       `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest         string                       `json:"cas_evaluation_digest,omitempty"`
	CASResult                   CASPatchApplyResult          `json:"cas_result,omitempty"`
	CASResultJSON               string                       `json:"cas_result_json,omitempty"`
	CASTestEvidence             PatchQueueTestEvidence       `json:"cas_test_evidence,omitempty"`
	CASTestEvidenceJSON         string                       `json:"cas_test_evidence_json,omitempty"`
	CASTestEvidenceDigest       string                       `json:"cas_test_evidence_digest,omitempty"`
	CASRecordedBy               string                       `json:"cas_recorded_by,omitempty"`
	CASRecordedAt               string                       `json:"cas_recorded_at,omitempty"`
	MaterializationSchema       string                       `json:"materialization_schema,omitempty"`
	MaterializationAccepted     bool                         `json:"materialization_accepted,omitempty"`
	Materialization             PatchMaterialization         `json:"materialization,omitempty"`
	MaterializationJSON         string                       `json:"materialization_json,omitempty"`
	MaterializationDigest       string                       `json:"materialization_digest,omitempty"`
	MaterializationRecordedBy   string                       `json:"materialization_recorded_by,omitempty"`
	MaterializationRecordedAt   string                       `json:"materialization_recorded_at,omitempty"`
	RollbackEvidenceSchema      string                       `json:"rollback_evidence_schema,omitempty"`
	RollbackEvidenceAccepted    bool                         `json:"rollback_evidence_accepted,omitempty"`
	RollbackEvidence            PatchQueueRollback           `json:"rollback_evidence,omitempty"`
	RollbackEvidenceJSON        string                       `json:"rollback_evidence_json,omitempty"`
	RollbackEvidenceDigest      string                       `json:"rollback_evidence_digest,omitempty"`
	RollbackRecordedBy          string                       `json:"rollback_recorded_by,omitempty"`
	RollbackRecordedAt          string                       `json:"rollback_recorded_at,omitempty"`
	ReviewerAdvisorySchema      string                       `json:"reviewer_advisory_schema,omitempty"`
	ReviewerAdvisoryAccepted    bool                         `json:"reviewer_advisory_accepted,omitempty"`
	ReviewerAdvisory            PatchQueueReviewerAdvisory   `json:"reviewer_advisory,omitempty"`
	ReviewerAdvisoryJSON        string                       `json:"reviewer_advisory_json,omitempty"`
	ReviewerAdvisoryDigest      string                       `json:"reviewer_advisory_digest,omitempty"`
	ReviewerRecordedBy          string                       `json:"reviewer_recorded_by,omitempty"`
	ReviewerRecordedAt          string                       `json:"reviewer_recorded_at,omitempty"`
	OperatorEnablementSchema    string                       `json:"operator_enablement_schema,omitempty"`
	OperatorEnablementAccepted  bool                         `json:"operator_enablement_accepted,omitempty"`
	OperatorEnablement          PatchQueueOperatorEnablement `json:"operator_enablement,omitempty"`
	OperatorEnablementJSON      string                       `json:"operator_enablement_json,omitempty"`
	OperatorEnablementDigest    string                       `json:"operator_enablement_digest,omitempty"`
	OperatorEnabledBy           string                       `json:"operator_enabled_by,omitempty"`
	OperatorEnabledAt           string                       `json:"operator_enabled_at,omitempty"`
	ClaimedBy                   string                       `json:"claimed_by,omitempty"`
	ClaimToken                  string                       `json:"claim_token,omitempty"`
	ClaimedAt                   string                       `json:"claimed_at,omitempty"`
	ClaimExpiresAt              string                       `json:"claim_expires_at,omitempty"`
	DecisionDocKey              string                       `json:"decision_doc_key,omitempty"`
	DecisionSummary             string                       `json:"decision_summary,omitempty"`
	DecidedBy                   string                       `json:"decided_by,omitempty"`
	DecidedAt                   string                       `json:"decided_at,omitempty"`
	CreatedAt                   string                       `json:"created_at"`
	UpdatedAt                   string                       `json:"updated_at"`
	ReviewTaskID                string                       `json:"review_task_id,omitempty"`
	ReviewTaskStatus            string                       `json:"review_task_status,omitempty"`
	ReviewTaskEventID           string                       `json:"review_task_event_id,omitempty"`
	MissingReviewTask           bool                         `json:"missing_review_task,omitempty"`
}

type CASPatchApplyResult struct {
	Schema        string               `json:"schema"`
	Status        string               `json:"status"`
	PatchID       string               `json:"patch_id,omitempty"`
	PatchDigest   string               `json:"patch_digest,omitempty"`
	ContextDigest string               `json:"context_digest,omitempty"`
	Paths         []CASPatchPathResult `json:"paths,omitempty"`
	Issues        []CASPatchIssue      `json:"issues,omitempty"`
}

type CASPatchPathResult struct {
	Path          string `json:"path"`
	Status        string `json:"status"`
	ChangeKind    string `json:"change_kind,omitempty"`
	BaseHash      string `json:"base_hash,omitempty"`
	CurrentHash   string `json:"current_hash,omitempty"`
	CandidateHash string `json:"candidate_hash,omitempty"`
}

type CASPatchIssue struct {
	Status        string `json:"status"`
	Kind          string `json:"kind"`
	Path          string `json:"path,omitempty"`
	Message       string `json:"message"`
	ExpectedHash  string `json:"expected_hash,omitempty"`
	ActualHash    string `json:"actual_hash,omitempty"`
	CandidateHash string `json:"candidate_hash,omitempty"`
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

type PatchMaterialization struct {
	Schema                string                  `json:"schema,omitempty"`
	WorkspaceID           string                  `json:"workspace_id,omitempty"`
	ProjectID             string                  `json:"project_id,omitempty"`
	QueueID               string                  `json:"queue_id,omitempty"`
	ItemID                string                  `json:"item_id,omitempty"`
	OperationID           string                  `json:"operation_id,omitempty"`
	OperationKind         string                  `json:"operation_kind,omitempty"`
	CASPatchDigest        string                  `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest   string                  `json:"cas_evaluation_digest,omitempty"`
	Files                 []PatchMaterializedFile `json:"files,omitempty"`
	RecordedBy            string                  `json:"recorded_by,omitempty"`
	RecordedAt            string                  `json:"recorded_at,omitempty"`
	MaterializationDigest string                  `json:"materialization_digest,omitempty"`
}

type PatchMaterializedFile struct {
	Path            string `json:"path"`
	ChangeKind      string `json:"change_kind,omitempty"`
	BaseHash        string `json:"base_hash,omitempty"`
	CandidateHash   string `json:"candidate_hash,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	Content         string `json:"content"`
	ContentDigest   string `json:"content_digest,omitempty"`
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
	Schema                 string `json:"schema,omitempty"`
	Mode                   string `json:"mode,omitempty"`
	Verdict                string `json:"verdict,omitempty"`
	Scope                  string `json:"scope,omitempty"`
	HeadSHA                string `json:"head_sha,omitempty"`
	DefeatsAcceptance      bool   `json:"defeats_acceptance,omitempty"`
	ReviewerID             string `json:"reviewer_id,omitempty"`
	ReviewDocKey           string `json:"review_doc_key,omitempty"`
	OperationID            string `json:"operation_id,omitempty"`
	OperationKind          string `json:"operation_kind,omitempty"`
	CASPatchDigest         string `json:"cas_patch_digest,omitempty"`
	CASEvaluationDigest    string `json:"cas_evaluation_digest,omitempty"`
	RollbackEvidenceDigest string `json:"rollback_evidence_digest,omitempty"`
	Summary                string `json:"summary,omitempty"`
	RecordedAt             string `json:"recorded_at,omitempty"`
}

type PatchQueueOperatorEnablement struct {
	Schema                 string `json:"schema,omitempty"`
	Scope                  string `json:"scope,omitempty"`
	Enabled                bool   `json:"enabled,omitempty"`
	EnabledBy              string `json:"enabled_by,omitempty"`
	EnabledAt              string `json:"enabled_at,omitempty"`
	Reason                 string `json:"reason,omitempty"`
	WorkspaceID            string `json:"workspace_id,omitempty"`
	ProjectID              string `json:"project_id,omitempty"`
	QueueID                string `json:"queue_id,omitempty"`
	ItemID                 string `json:"item_id,omitempty"`
	OperationID            string `json:"operation_id,omitempty"`
	CASPatchDigest         string `json:"cas_patch_digest,omitempty"`
	RollbackEvidenceDigest string `json:"rollback_evidence_digest,omitempty"`
	ReviewerAdvisoryDigest string `json:"reviewer_advisory_digest,omitempty"`
}

type ProjectCoordinationRecord struct {
	SnapshotAt          string                        `json:"snapshot_at"`
	CoordinationVersion string                        `json:"coordination_version"`
	LatestEventID       string                        `json:"latest_event_id,omitempty"`
	SourceEventIDs      map[string]string             `json:"source_event_ids,omitempty"`
	Project             ProjectRecord                 `json:"project"`
	Profile             ProjectProfileRecord          `json:"profile"`
	GateStatus          ProjectGateStatusRecord       `json:"gate_status"`
	StrategicLead       *ProjectRoleRecord            `json:"strategic_lead,omitempty"`
	Roles               []ProjectRoleRecord           `json:"roles,omitempty"`
	Repositories        []ProjectRepositoryRecord     `json:"repositories,omitempty"`
	Checkouts           []ProjectCheckoutRecord       `json:"checkouts,omitempty"`
	Branches            []ProjectBranchRecord         `json:"branches,omitempty"`
	PatchQueueItems     []ProjectPatchQueueItemRecord `json:"patch_queue_items,omitempty"`
	ServiceRuns         []ServiceRunRecord            `json:"service_runs,omitempty"`
	Tasks               []WorkspaceTaskRecord         `json:"tasks,omitempty"`
	OpenTaskCount       int                           `json:"open_task_count"`
	TaskCountsByLane    map[string]int                `json:"task_counts_by_lane,omitempty"`
	TaskCountsByStatus  map[string]int                `json:"task_counts_by_status,omitempty"`
}

type ProtoClusterMetrics struct {
	EventCount                  int                `json:"event_count"`
	EventTypeCounts             map[string]int     `json:"event_type_counts,omitempty"`
	ActiveSessionCount          int                `json:"active_session_count"`
	OpenQueueCount              int                `json:"open_queue_count"`
	BlockerSignalCount          int                `json:"blocker_signal_count"`
	BlockerDensity              float64            `json:"blocker_density"`
	ActivityCountsByAgent       map[string]int     `json:"activity_counts_by_agent,omitempty"`
	ActivityShareByAgent        map[string]float64 `json:"activity_share_by_agent,omitempty"`
	MaxAgentActivityShare       float64            `json:"max_agent_activity_share"`
	CommunicationInByAgent      map[string]int     `json:"communication_in_by_agent,omitempty"`
	CommunicationOutByAgent     map[string]int     `json:"communication_out_by_agent,omitempty"`
	CommunicationCentralization float64            `json:"communication_centralization"`
	DuplicationSignalCount      int                `json:"duplication_signal_count"`
	DuplicationIndex            float64            `json:"duplication_index"`
	LastEventAt                 string             `json:"last_event_at,omitempty"`
}

type ControlReportWorkspace struct {
	HotClusterCount          int     `json:"hot_cluster_count"`
	AttentionClusterCount    int     `json:"attention_cluster_count"`
	HighestPressureClusterID string  `json:"highest_pressure_cluster_id"`
	HighestPressureScore     float64 `json:"highest_pressure_score"`
}

type ControlReportCluster struct {
	ProtoClusterID        string           `json:"proto_cluster_id"`
	Summary               string           `json:"summary"`
	TaskIDs               []string         `json:"task_ids"`
	SessionIDs            []string         `json:"session_ids"`
	DocKeys               []string         `json:"doc_keys"`
	AgentIDs              []string         `json:"agent_ids"`
	Signals               []map[string]any `json:"signals"`
	SuggestedControls     []map[string]any `json:"suggested_controls"`
	PendingTensionCount   int              `json:"pending_tension_count"`
	ConfirmedTensionCount int              `json:"confirmed_tension_count"`
}

func (c *ControlReportCluster) UnmarshalJSON(data []byte) error {
	type alias ControlReportCluster
	var raw struct {
		alias
		Signals           json.RawMessage `json:"signals"`
		SuggestedControls json.RawMessage `json:"suggested_controls"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*c = ControlReportCluster(raw.alias)
	c.Signals = decodeControlMapList(raw.Signals)
	c.SuggestedControls = decodeControlMapList(raw.SuggestedControls)
	return nil
}

func decodeControlMapList(raw json.RawMessage) []map[string]any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var list []map[string]any
	if err := json.Unmarshal(raw, &list); err == nil {
		return list
	}
	var single map[string]any
	if err := json.Unmarshal(raw, &single); err == nil && len(single) > 0 {
		return []map[string]any{single}
	}
	return nil
}

type ControlReport struct {
	WorkspaceID string                 `json:"workspace_id"`
	GeneratedAt string                 `json:"generated_at"`
	Workspace   ControlReportWorkspace `json:"workspace"`
	Clusters    []ControlReportCluster `json:"clusters"`
}

type ControlSignalVector struct {
	ThroughputPressure   int    `json:"throughput_pressure"`
	ReviewPressure       int    `json:"review_pressure"`
	CoordinationPressure int    `json:"coordination_pressure"`
	PressureScore        int    `json:"pressure_score"`
	AttentionBand        string `json:"attention_band"`
}

type ControlSuggestedControls struct {
	FanoutCap      int     `json:"fanout_cap"`
	ReviewDepth    int     `json:"review_depth"`
	ContextCap     int     `json:"context_cap"`
	BridgeQuota    int     `json:"bridge_quota"`
	MergeThreshold float64 `json:"merge_threshold"`
	PriorityFocus  string  `json:"priority_focus"`
}

type ControlClusterDetailRecord struct {
	ProtoClusterID        string                   `json:"proto_cluster_id"`
	ResolutionKind        string                   `json:"resolution_kind"`
	TaskIDs               []string                 `json:"task_ids,omitempty"`
	SessionIDs            []string                 `json:"session_ids,omitempty"`
	DocKeys               []string                 `json:"doc_keys,omitempty"`
	ArtifactRefs          []string                 `json:"artifact_refs,omitempty"`
	AgentIDs              []string                 `json:"agent_ids,omitempty"`
	MetricsMissing        bool                     `json:"metrics_missing,omitempty"`
	BasisStale            bool                     `json:"basis_stale,omitempty"`
	LastTensionBasisAt    string                   `json:"last_tension_basis_at,omitempty"`
	Metrics               ProtoClusterMetrics      `json:"metrics"`
	Signals               ControlSignalVector      `json:"signals"`
	SuggestedControls     ControlSuggestedControls `json:"suggested_controls"`
	ConfirmedTensionCount int                      `json:"confirmed_tension_count"`
	PendingTensionCount   int                      `json:"pending_tension_count"`
	ConfirmedCountsByType map[string]int           `json:"confirmed_counts_by_type,omitempty"`
	PendingCountsByType   map[string]int           `json:"pending_counts_by_type,omitempty"`
	ConfirmedTensionIDs   []string                 `json:"confirmed_tension_ids,omitempty"`
	PendingTensionIDs     []string                 `json:"pending_tension_ids,omitempty"`
	Summary               string                   `json:"summary,omitempty"`
}

type ControlClusterDetail struct {
	Cluster  ControlClusterDetailRecord `json:"cluster"`
	Tensions []TensionRecord            `json:"tensions,omitempty"`
}

type TensionFrontierItem struct {
	TensionID      string  `json:"tension_id"`
	ProtoClusterID string  `json:"proto_cluster_id"`
	TensionType    string  `json:"tension_type"`
	ReviewStatus   string  `json:"review_status"`
	Title          string  `json:"title"`
	Summary        string  `json:"summary"`
	SurfaceScore   float64 `json:"surface_score"`
	EvidenceCount  int     `json:"evidence_count"`
	LastSeenAt     string  `json:"last_seen_at"`
}

type TensionRecord struct {
	TensionID         string   `json:"tension_id"`
	WorkspaceID       string   `json:"workspace_id"`
	ProtoClusterID    string   `json:"proto_cluster_id"`
	TensionType       string   `json:"tension_type"`
	LifecycleState    string   `json:"lifecycle_state"`
	ReviewStatus      string   `json:"review_status"`
	Title             string   `json:"title"`
	Summary           string   `json:"summary,omitempty"`
	AnchorKind        string   `json:"anchor_kind"`
	AnchorRef         string   `json:"anchor_ref"`
	TaskIDs           []string `json:"task_ids,omitempty"`
	SessionIDs        []string `json:"session_ids,omitempty"`
	DocKeys           []string `json:"doc_keys,omitempty"`
	ArtifactRefs      []string `json:"artifact_refs,omitempty"`
	SegmentRefs       []string `json:"segment_refs,omitempty"`
	AgentIDs          []string `json:"agent_ids,omitempty"`
	ConstraintRefs    []string `json:"constraint_refs,omitempty"`
	BaseScore         int      `json:"base_score"`
	SurfaceScore      int      `json:"surface_score"`
	EvidenceCount     int      `json:"evidence_count"`
	LastSeenEventID   string   `json:"last_seen_event_id,omitempty"`
	LastSeenAt        string   `json:"last_seen_at,omitempty"`
	LastDetectedAt    string   `json:"last_detected_at,omitempty"`
	LastRefreshedAt   string   `json:"last_refreshed_at,omitempty"`
	StaleRefreshCount int      `json:"stale_refresh_count,omitempty"`
	ConfirmedBy       string   `json:"confirmed_by,omitempty"`
	ArchivedBy        string   `json:"archived_by,omitempty"`
	DismissedReason   string   `json:"dismissed_reason,omitempty"`
	CreatedAt         string   `json:"created_at"`
	UpdatedAt         string   `json:"updated_at"`
}

type TensionEvidenceRecord struct {
	TensionID    string `json:"tension_id"`
	WorkspaceID  string `json:"workspace_id"`
	EvidenceKind string `json:"evidence_kind"`
	EvidenceRef  string `json:"evidence_ref"`
	EventID      string `json:"event_id,omitempty"`
	Weight       int    `json:"weight"`
	Summary      string `json:"summary,omitempty"`
	CreatedAt    string `json:"created_at"`
}

type TensionDetail struct {
	Tension   TensionRecord             `json:"tension"`
	Evidence  []TensionEvidenceRecord   `json:"evidence,omitempty"`
	Docs      []WorkspaceDocRecord      `json:"docs,omitempty"`
	Artifacts []WorkspaceArtifactRecord `json:"artifacts,omitempty"`
}

type CorridorLookupRecord struct {
	LookupStatus    string   `json:"lookup_status"`
	CatalogKey      string   `json:"catalog_key,omitempty"`
	DisplayName     string   `json:"display_name,omitempty"`
	MatchSource     string   `json:"match_source,omitempty"`
	MatchConfidence float64  `json:"match_confidence"`
	MatchBasis      []string `json:"match_basis,omitempty"`
	Summary         string   `json:"summary,omitempty"`
}

type TaskClassHintRecord struct {
	TaskID             string               `json:"task_id"`
	Title              string               `json:"title,omitempty"`
	Status             string               `json:"status,omitempty"`
	TaskKind           string               `json:"task_kind"`
	TaskTemplate       string               `json:"task_template"`
	TaskClass          string               `json:"task_class,omitempty"`
	TaskClassSource    string               `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt string               `json:"task_class_updated_at,omitempty"`
	UpdatedAt          string               `json:"updated_at,omitempty"`
	BasisUpdatedAt     string               `json:"basis_updated_at,omitempty"`
	Tags               []string             `json:"tags,omitempty"`
	TaskClassHint      string               `json:"task_class_hint"`
	HintConfidence     float64              `json:"hint_confidence"`
	TaskClassBasis     []string             `json:"task_class_basis,omitempty"`
	CorridorHint       string               `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup     CorridorLookupRecord `json:"corridor_lookup,omitempty"`
	Summary            string               `json:"summary,omitempty"`
}

type CorridorClusterReport struct {
	ProtoClusterID      string               `json:"proto_cluster_id"`
	ResolutionKind      string               `json:"resolution_kind"`
	TaskIDs             []string             `json:"task_ids,omitempty"`
	SessionIDs          []string             `json:"session_ids,omitempty"`
	DocKeys             []string             `json:"doc_keys,omitempty"`
	ArtifactRefs        []string             `json:"artifact_refs,omitempty"`
	AgentIDs            []string             `json:"agent_ids,omitempty"`
	TaskClass           string               `json:"task_class,omitempty"`
	TaskClassSource     string               `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt  string               `json:"task_class_updated_at,omitempty"`
	TaskClassHint       string               `json:"task_class_hint"`
	CorridorCatalogHint string               `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup      CorridorLookupRecord `json:"corridor_lookup,omitempty"`
	TaskClassConfidence float64              `json:"task_class_confidence"`
	CorridorReadiness   string               `json:"corridor_readiness"`
	ReadinessConfidence float64              `json:"readiness_confidence"`
	TaskClassCounts     map[string]int       `json:"task_class_counts,omitempty"`
	UnknownTaskCount    int                  `json:"unknown_task_count"`
	MixedTaskClasses    bool                 `json:"mixed_task_classes,omitempty"`
	BasisStale          bool                 `json:"basis_stale,omitempty"`
	LastBasisEventAt    string               `json:"last_basis_event_at,omitempty"`
	TaskClassBasis      []string             `json:"task_class_basis,omitempty"`
	Metrics             ProtoClusterMetrics  `json:"metrics"`
	Summary             string               `json:"summary,omitempty"`
}

type CorridorClusterDetail struct {
	Cluster CorridorClusterReport `json:"cluster"`
	Tasks   []TaskClassHintRecord `json:"tasks,omitempty"`
}

type CorridorFitMetricRange struct {
	Metric     string   `json:"metric"`
	LowerBound *float64 `json:"lower_bound,omitempty"`
	UpperBound *float64 `json:"upper_bound,omitempty"`
}

type CorridorFitCatalogRangeCheck struct {
	CatalogKey   string                   `json:"catalog_key,omitempty"`
	DisplayName  string                   `json:"display_name,omitempty"`
	TaskClass    string                   `json:"task_class,omitempty"`
	MatchSource  string                   `json:"match_source,omitempty"`
	Ranges       []CorridorFitMetricRange `json:"ranges,omitempty"`
	BasisFresh   bool                     `json:"basis_fresh"`
	BasisSummary string                   `json:"basis_summary,omitempty"`
}

type CorridorFitMetricGap struct {
	Metric     string   `json:"metric"`
	Value      float64  `json:"value"`
	LowerBound *float64 `json:"lower_bound,omitempty"`
	UpperBound *float64 `json:"upper_bound,omitempty"`
	Delta      float64  `json:"delta"`
	Status     string   `json:"status"`
}

type CorridorFitClusterReport struct {
	ProtoClusterID        string                       `json:"proto_cluster_id"`
	ResolutionKind        string                       `json:"resolution_kind"`
	TaskIDs               []string                     `json:"task_ids,omitempty"`
	SessionIDs            []string                     `json:"session_ids,omitempty"`
	DocKeys               []string                     `json:"doc_keys,omitempty"`
	ArtifactRefs          []string                     `json:"artifact_refs,omitempty"`
	AgentIDs              []string                     `json:"agent_ids,omitempty"`
	TaskClass             string                       `json:"task_class,omitempty"`
	TaskClassSource       string                       `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt    string                       `json:"task_class_updated_at,omitempty"`
	TaskClassHint         string                       `json:"task_class_hint"`
	CorridorCatalogHint   string                       `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup        CorridorLookupRecord         `json:"corridor_lookup,omitempty"`
	CorridorReadiness     string                       `json:"corridor_readiness"`
	ReadinessConfidence   float64                      `json:"readiness_confidence"`
	BasisStale            bool                         `json:"basis_stale,omitempty"`
	LastBasisEventAt      string                       `json:"last_basis_event_at,omitempty"`
	MetricsMissing        bool                         `json:"metrics_missing,omitempty"`
	Metrics               ProtoClusterMetrics          `json:"metrics"`
	CatalogRangeCheck     CorridorFitCatalogRangeCheck `json:"catalog_range_check,omitempty"`
	MetricGapBreakdown    []CorridorFitMetricGap       `json:"metric_gap_breakdown,omitempty"`
	FitStatus             string                       `json:"fit_status"`
	FitConfidence         float64                      `json:"fit_confidence"`
	FitScore              int                          `json:"fit_score"`
	ConfirmedTensionCount int                          `json:"confirmed_tension_count"`
	ConfirmedCountsByType map[string]int               `json:"confirmed_counts_by_type,omitempty"`
	ConfirmedTensionIDs   []string                     `json:"confirmed_tension_ids,omitempty"`
	Summary               string                       `json:"summary,omitempty"`
}

type CorridorFitClusterDetail struct {
	Cluster           CorridorFitClusterReport `json:"cluster"`
	ConfirmedTensions []TensionRecord          `json:"confirmed_tensions,omitempty"`
}

type ClusterControlHeuristicProfileContext struct {
	Profile           string  `json:"profile"`
	ThroughputMin     float64 `json:"throughput_min"`
	ThroughputMax     float64 `json:"throughput_max"`
	ReviewMin         float64 `json:"review_min"`
	ReviewMax         float64 `json:"review_max"`
	CoordinationMin   float64 `json:"coordination_min"`
	CoordinationMax   float64 `json:"coordination_max"`
	CentralizationMax float64 `json:"centralization_max"`
	BlockerDensityMax float64 `json:"blocker_density_max"`
	DuplicationMin    float64 `json:"duplication_min"`
	DuplicationMax    float64 `json:"duplication_max"`
}

type ClusterControlSignalDeviationVector struct {
	Throughput     float64 `json:"throughput"`
	Review         float64 `json:"review"`
	Coordination   float64 `json:"coordination"`
	Centralization float64 `json:"centralization"`
	BlockerDensity float64 `json:"blocker_density"`
	Duplication    float64 `json:"duplication"`
	NoveltyGap     float64 `json:"novelty_gap"`
	SynergyGap     float64 `json:"synergy_gap"`
}

type ClusterControlStateRecord struct {
	WorkspaceID            string                              `json:"workspace_id"`
	ProtoClusterID         string                              `json:"proto_cluster_id"`
	ResolutionKind         string                              `json:"resolution_kind"`
	CorridorProfile        string                              `json:"heuristic_profile"`
	Epoch                  int                                 `json:"epoch"`
	CurrentMode            string                              `json:"stabilized_mode_hint"`
	CandidateMode          string                              `json:"candidate_mode_hint"`
	CandidateStreak        int                                 `json:"stability_streak"`
	DominantViolationKind  string                              `json:"dominant_signal_kind,omitempty"`
	DominantViolationScore float64                             `json:"dominant_signal_score"`
	AttentionBand          string                              `json:"attention_band"`
	PressureScore          int                                 `json:"pressure_score"`
	ConfirmedTensionCount  int                                 `json:"confirmed_tension_count"`
	PendingTensionCount    int                                 `json:"pending_tension_count"`
	TaskIDs                []string                            `json:"task_ids,omitempty"`
	SessionIDs             []string                            `json:"session_ids,omitempty"`
	DocKeys                []string                            `json:"doc_keys,omitempty"`
	ArtifactRefs           []string                            `json:"artifact_refs,omitempty"`
	AgentIDs               []string                            `json:"agent_ids,omitempty"`
	ConfirmedTensionIDs    []string                            `json:"confirmed_tension_ids,omitempty"`
	PendingTensionIDs      []string                            `json:"pending_tension_ids,omitempty"`
	OperatorHints          ControlSuggestedControls            `json:"operator_hints"`
	ViolationVector        ClusterControlSignalDeviationVector `json:"signal_deviation_vector"`
	Summary                string                              `json:"summary,omitempty"`
	LastBasisAt            string                              `json:"last_basis_at,omitempty"`
	LastTickEventID        string                              `json:"last_tick_event_id,omitempty"`
	LastTickAt             string                              `json:"last_tick_at,omitempty"`
	LastTransitionAt       string                              `json:"last_stabilized_at,omitempty"`
	CreatedAt              string                              `json:"created_at"`
	UpdatedAt              string                              `json:"updated_at"`
}

type ClusterControlStateCluster struct {
	ProtoClusterID        string                                `json:"proto_cluster_id"`
	ResolutionKind        string                                `json:"resolution_kind"`
	ProfileContext        ClusterControlHeuristicProfileContext `json:"heuristic_profile_context"`
	State                 ClusterControlStateRecord             `json:"state"`
	Metrics               ProtoClusterMetrics                   `json:"metrics"`
	Signals               ControlSignalVector                   `json:"signals"`
	SuggestedControls     ControlSuggestedControls              `json:"suggested_controls"`
	MetricsMissing        bool                                  `json:"metrics_missing,omitempty"`
	BasisStale            bool                                  `json:"basis_stale,omitempty"`
	LastTensionBasisAt    string                                `json:"last_tension_basis_at,omitempty"`
	ConfirmedCountsByType map[string]int                        `json:"confirmed_counts_by_type,omitempty"`
	PendingCountsByType   map[string]int                        `json:"pending_counts_by_type,omitempty"`
	Summary               string                                `json:"summary,omitempty"`
}

type ClusterControlStateDetail struct {
	Cluster  ControlClusterDetailRecord `json:"cluster"`
	State    ClusterControlStateCluster `json:"state"`
	Tensions []TensionRecord            `json:"tensions,omitempty"`
}

type InstrumentationLocusBundle struct {
	WorkspaceID     string                     `json:"workspace_id"`
	GeneratedAt     string                     `json:"generated_at"`
	Resolved        bool                       `json:"resolved"`
	ResolvedFrom    string                     `json:"resolved_from,omitempty"`
	MatchScore      int                        `json:"match_score,omitempty"`
	ProtoClusterID  string                     `json:"proto_cluster_id,omitempty"`
	Control         *ControlClusterDetail      `json:"control,omitempty"`
	ControlState    *ClusterControlStateDetail `json:"control_state,omitempty"`
	Corridor        *CorridorClusterDetail     `json:"corridor,omitempty"`
	CorridorFit     *CorridorFitClusterDetail  `json:"corridor_fit,omitempty"`
	Frontier        []TensionFrontierItem      `json:"frontier,omitempty"`
	DominantTension *TensionDetail             `json:"dominant_tension,omitempty"`
}

type TaskStatus struct {
	TaskID               string           `json:"task_id"`
	Title                string           `json:"title,omitempty"`
	Description          string           `json:"description,omitempty"`
	OwnerUserID          string           `json:"owner_user_id"`
	Priority             string           `json:"priority"`
	Status               string           `json:"status"`
	TaskKind             string           `json:"task_kind"`
	TaskTemplate         string           `json:"task_template"`
	ProjectID            string           `json:"project_id,omitempty"`
	ProjectLane          string           `json:"project_lane,omitempty"`
	RequiresProjectGate  *bool            `json:"requires_project_gate,omitempty"`
	TaskRequirementsJSON string           `json:"task_requirements_json,omitempty"`
	WriteScopeHints      []string         `json:"write_scope_hints,omitempty"`
	CreatedAt            string           `json:"created_at"`
	UpdatedAt            string           `json:"updated_at"`
	NodeCounts           map[string]int   `json:"node_counts"`
	Nodes                []TaskStatusNode `json:"nodes"`
}

type TaskStatusNode struct {
	NodeID       string   `json:"node_id"`
	Type         string   `json:"type"`
	Status       string   `json:"status"`
	AttemptCount int      `json:"attempt_count"`
	LastError    *string  `json:"last_error,omitempty"`
	DependsOn    []string `json:"depends_on"`
}

type TaskHydrationBundle struct {
	GeneratedAt   string                    `json:"generated_at"`
	Workspace     *WorkspaceRecord          `json:"workspace,omitempty"`
	WorkspaceTask *WorkspaceTaskRecord      `json:"workspace_task,omitempty"`
	Task          TaskStatus                `json:"task"`
	Docs          []WorkspaceDocRecord      `json:"docs"`
	TaskLinks     []WorkspaceTaskLinkRecord `json:"task_links"`
	RelatedTasks  []TaskStatus              `json:"related_tasks"`
	Artifacts     []WorkspaceArtifactRecord `json:"artifacts"`
	Updates       []AgentUpdateRecord       `json:"updates"`
	SideEffects   []AgentUpdateSideEffectV1 `json:"side_effects,omitempty"`
}

type AgentWorkNextResult struct {
	GeneratedAt                string                   `json:"generated_at"`
	WorkspaceID                string                   `json:"workspace_id"`
	AgentID                    string                   `json:"agent_id"`
	HasWork                    bool                     `json:"has_work"`
	Reason                     string                   `json:"reason"`
	Trigger                    string                   `json:"trigger,omitempty"`
	ClaimAction                string                   `json:"claim_action,omitempty"`
	SessionAction              string                   `json:"session_action,omitempty"`
	ResumeSummary              string                   `json:"resume_summary,omitempty"`
	ProjectID                  string                   `json:"project_id,omitempty"`
	TaskKind                   string                   `json:"task_kind,omitempty"`
	ProjectLane                string                   `json:"project_lane,omitempty"`
	RequiresProjectGate        *bool                    `json:"requires_project_gate,omitempty"`
	ProjectGateBlock           json.RawMessage          `json:"project_gate_block,omitempty"`
	ProjectCoordination        json.RawMessage          `json:"project_coordination,omitempty"`
	AutonomousExecutionAllowed bool                     `json:"autonomous_execution_allowed"`
	ProfileGateReason          string                   `json:"profile_gate_reason,omitempty"`
	ProfileGateSummary         string                   `json:"profile_gate_summary,omitempty"`
	ProfileGateBlockedWork     bool                     `json:"profile_gate_blocked_work,omitempty"`
	Packet                     *AgentWorkPacket         `json:"packet,omitempty"`
	Task                       *WorkspaceTaskRecord     `json:"task,omitempty"`
	Session                    *AgentSessionStateRecord `json:"session,omitempty"`
	Hydration                  *TaskHydrationBundle     `json:"hydration,omitempty"`
}

type AgentWorkPacket struct {
	WorkType            string                        `json:"work_type"`
	ClaimAction         string                        `json:"claim_action,omitempty"`
	SessionAction       string                        `json:"session_action,omitempty"`
	CoordinationState   string                        `json:"coordination_state,omitempty"`
	PreferredTransition string                        `json:"preferred_transition,omitempty"`
	WhyNow              string                        `json:"why_now,omitempty"`
	ProjectID           string                        `json:"project_id,omitempty"`
	TaskKind            string                        `json:"task_kind,omitempty"`
	ProjectLane         string                        `json:"project_lane,omitempty"`
	RequiresProjectGate *bool                         `json:"requires_project_gate,omitempty"`
	ProjectGateBlock    json.RawMessage               `json:"project_gate_block,omitempty"`
	ProjectCoordination json.RawMessage               `json:"project_coordination,omitempty"`
	Resume              *AgentWorkResume              `json:"resume,omitempty"`
	Decision            *AgentWorkDecision            `json:"decision,omitempty"`
	Gate                *AgentWorkGate                `json:"gate,omitempty"`
	Unblock             *AgentWorkUnblock             `json:"unblock,omitempty"`
	Handoff             *AgentWorkHandoff             `json:"handoff,omitempty"`
	Blockers            []BlockedRef                  `json:"blockers,omitempty"`
	HandoffToAgentID    string                        `json:"handoff_to_agent_id,omitempty"`
	ContextHints        AgentWorkContextHints         `json:"context_hints,omitempty"`
	OwnerBound          *AgentWorkOwnerBound          `json:"owner_bound,omitempty"`
	PatchQueueSupersede *AgentWorkPatchQueueSupersede `json:"patch_queue_supersede,omitempty"`
	PatchQueueClaim     *AgentWorkPatchQueueClaim     `json:"patch_queue_claim_stewardship,omitempty"`
	Frontier            *AgentWorkTaskFrontier        `json:"frontier,omitempty"`
	Advisory            *AgentWorkAdvisory            `json:"advisory,omitempty"`
}

type AgentWorkTaskFrontier struct {
	GenerationID   string                           `json:"generation_id"`
	GeneratedAt    string                           `json:"generated_at"`
	SelectionMode  string                           `json:"selection_mode"`
	Summary        string                           `json:"summary,omitempty"`
	Candidates     []AgentWorkTaskFrontierCandidate `json:"candidates,omitempty"`
	Roster         []AgentWorkRosterAgent           `json:"roster,omitempty"`
	SelectedTaskID string                           `json:"selected_task_id,omitempty"`
	SelfFitSummary string                           `json:"self_fit_summary,omitempty"`
	DeclineSummary string                           `json:"decline_summary,omitempty"`
}

type AgentWorkTaskFrontierCandidate struct {
	Task           WorkspaceTaskRecord `json:"task"`
	Fit            AgentWorkTaskFit    `json:"fit"`
	ClaimAction    string              `json:"claim_action,omitempty"`
	SessionAction  string              `json:"session_action,omitempty"`
	Blocked        bool                `json:"blocked,omitempty"`
	BlockReason    string              `json:"block_reason,omitempty"`
	BlockSummary   string              `json:"block_summary,omitempty"`
	AdvisoryReason string              `json:"advisory_reason,omitempty"`
}

type AgentWorkTaskFit struct {
	Level              string   `json:"level"`
	Score              int      `json:"score"`
	Reasons            []string `json:"reasons,omitempty"`
	RequiredWorkModes  []string `json:"required_work_modes,omitempty"`
	PreferredWorkModes []string `json:"preferred_work_modes,omitempty"`
	PreferredSkills    []string `json:"preferred_skills,omitempty"`
	PreferredTools     []string `json:"preferred_tools,omitempty"`
	AdvisoryRoleTypes  []string `json:"advisory_role_types,omitempty"`
}

type AgentWorkRosterAgent struct {
	AgentID               string             `json:"agent_id"`
	DisplayName           string             `json:"display_name,omitempty"`
	Role                  string             `json:"role,omitempty"`
	Status                string             `json:"status,omitempty"`
	IsOnline              bool               `json:"is_online"`
	LastSeenAt            *string            `json:"last_seen_at,omitempty"`
	ActiveTaskCount       int                `json:"active_task_count"`
	CurrentSessionID      string             `json:"current_session_id,omitempty"`
	CurrentTaskIDs        []string           `json:"current_task_ids,omitempty"`
	Capabilities          []string           `json:"capabilities,omitempty"`
	ProfileSpecialization string             `json:"profile_specialization,omitempty"`
	ProfileTags           []string           `json:"profile_tags,omitempty"`
	ToolsAccess           []string           `json:"tools_access,omitempty"`
	Busyness              string             `json:"busyness,omitempty"`
	ActiveTasks           []AgentCurrentTask `json:"active_tasks,omitempty"`
}

type AgentWorkResume struct {
	SessionID string `json:"session_id"`
	Summary   string `json:"summary,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type AgentWorkDecision struct {
	NeededFrom   string `json:"needed_from,omitempty"`
	DecisionType string `json:"decision_type,omitempty"`
}

type AgentWorkGate struct {
	GateState  string `json:"gate_state,omitempty"`
	GateType   string `json:"gate_type,omitempty"`
	NeededFrom string `json:"needed_from,omitempty"`
	Summary    string `json:"summary,omitempty"`
}

type AgentWorkUnblock struct {
	UnblockState string   `json:"unblock_state,omitempty"`
	Trigger      string   `json:"trigger,omitempty"`
	BlockerKinds []string `json:"blocker_kinds,omitempty"`
	Summary      string   `json:"summary,omitempty"`
}

type AgentWorkHandoff struct {
	HandoffState string `json:"handoff_state,omitempty"`
	ToAgentID    string `json:"to_agent_id,omitempty"`
	Summary      string `json:"summary,omitempty"`
}

type AgentWorkContextHints struct {
	SuggestedDocKeys      []string `json:"suggested_doc_keys,omitempty"`
	RelatedArtifactRefs   []string `json:"related_artifact_refs,omitempty"`
	AnchorTaskIDs         []string `json:"anchor_task_ids,omitempty"`
	AnchorConflictTaskIDs []string `json:"anchor_conflict_task_ids,omitempty"`
	AnchorBranchIDs       []string `json:"anchor_branch_ids,omitempty"`
	AnchorSessionIDs      []string `json:"anchor_session_ids,omitempty"`
}

type AgentWorkOwnerBound struct {
	Kind            string `json:"kind,omitempty"`
	RequiredAgentID string `json:"required_agent_id,omitempty"`
	BranchID        string `json:"branch_id,omitempty"`
	BranchName      string `json:"branch_name,omitempty"`
	HeadSHA         string `json:"head_sha,omitempty"`
	ReviewDocKey    string `json:"review_doc_key,omitempty"`
	QueueID         string `json:"queue_id,omitempty"`
	ItemID          string `json:"item_id,omitempty"`
	RepairNeeded    bool   `json:"repair_needed,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

type AgentWorkPatchQueueSupersede struct {
	ProjectID      string `json:"project_id,omitempty"`
	QueueID        string `json:"queue_id,omitempty"`
	ItemID         string `json:"item_id,omitempty"`
	BranchID       string `json:"branch_id,omitempty"`
	BranchName     string `json:"branch_name,omitempty"`
	HeadSHA        string `json:"head_sha,omitempty"`
	NewItemID      string `json:"new_item_id,omitempty"`
	EvidenceDocKey string `json:"evidence_doc_key,omitempty"`
	DecisionDocKey string `json:"decision_doc_key,omitempty"`
	ReviewDocKey   string `json:"review_doc_key,omitempty"`
	Summary        string `json:"summary,omitempty"`
}

type AgentWorkPatchQueueClaim struct {
	ProjectID               string   `json:"project_id,omitempty"`
	QueueID                 string   `json:"queue_id,omitempty"`
	ItemID                  string   `json:"item_id,omitempty"`
	BranchID                string   `json:"branch_id,omitempty"`
	BranchName              string   `json:"branch_name,omitempty"`
	HeadSHA                 string   `json:"head_sha,omitempty"`
	State                   string   `json:"state,omitempty"`
	ClaimedBy               string   `json:"claimed_by,omitempty"`
	ClaimExpiresAt          string   `json:"claim_expires_at,omitempty"`
	ClaimActive             bool     `json:"claim_active,omitempty"`
	OperationBindingPresent bool     `json:"operation_binding_present,omitempty"`
	ReviewDocKey            string   `json:"review_doc_key,omitempty"`
	EvidenceDocKey          string   `json:"evidence_doc_key,omitempty"`
	DecisionDocKey          string   `json:"decision_doc_key,omitempty"`
	AllowedActions          []string `json:"allowed_actions,omitempty"`
	Summary                 string   `json:"summary,omitempty"`
}

type AgentWorkAdvisory struct {
	ProtoClusterID string                     `json:"proto_cluster_id,omitempty"`
	Control        *AgentWorkControlAdvisory  `json:"control,omitempty"`
	Corridor       *AgentWorkCorridorAdvisory `json:"corridor,omitempty"`
	Frontier       []TensionFrontierItem      `json:"frontier,omitempty"`
}

type AgentWorkControlAdvisory struct {
	AttentionBand string `json:"attention_band,omitempty"`
	PressureScore int    `json:"pressure_score,omitempty"`
	Summary       string `json:"summary,omitempty"`
	BasisStale    bool   `json:"basis_stale,omitempty"`
}

type AgentWorkCorridorAdvisory struct {
	CorridorReadiness   string `json:"corridor_readiness,omitempty"`
	TaskClassHint       string `json:"task_class_hint,omitempty"`
	CorridorCatalogHint string `json:"corridor_catalog_hint,omitempty"`
	Summary             string `json:"summary,omitempty"`
	BasisStale          bool   `json:"basis_stale,omitempty"`
}

type WorkspaceSnapshot struct {
	Workspace       WorkspaceRecord           `json:"workspace"`
	Docs            []WorkspaceDocRecord      `json:"docs"`
	Agents          []AgentRecord             `json:"agents"`
	Sessions        []AgentSessionStateRecord `json:"sessions"`
	Tools           []WorkspaceToolRecord     `json:"tools"`
	Tasks           []WorkspaceTaskRecord     `json:"tasks"`
	TaskLinks       []WorkspaceTaskLinkRecord `json:"task_links"`
	RecentMemory    []WorkspaceMemoryRecord   `json:"recent_memory"`
	RecentArtifacts []WorkspaceArtifactRecord `json:"recent_artifacts"`
	RecentUpdates   []AgentUpdateRecord       `json:"recent_updates"`
	RecentMessages  []MessageRecord           `json:"recent_messages"`
	Projects        []ProjectRecord           `json:"projects"`
}

type BootstrapResult struct {
	GeneratedAt string            `json:"generated_at"`
	Agent       AgentRecord       `json:"agent"`
	Snapshot    WorkspaceSnapshot `json:"snapshot"`
}

type PollMessagesResult struct {
	Messages   []MessageRecord `json:"messages"`
	Count      int             `json:"count"`
	NextCursor string          `json:"next_cursor"`
}

type PollNewsResult struct {
	Items               []NewsRecord `json:"items"`
	Count               int          `json:"count"`
	NextCursorCreatedAt string       `json:"next_cursor_created_at"`
	NextCursorNewsID    string       `json:"next_cursor_news_id"`
}

type AgentRequestRecord struct {
	RequestID   string `json:"request_id"`
	WorkspaceID string `json:"workspace_id"`
	FromAgentID string `json:"from_agent_id"`
	ToAgentID   string `json:"to_agent_id"`
	Method      string `json:"method"`
	Payload     string `json:"payload"`
	Status      string `json:"status"`
	Response    string `json:"response,omitempty"`
	CreatedAt   string `json:"created_at"`
	RespondedAt string `json:"responded_at,omitempty"`
	TimeoutSec  int    `json:"timeout_sec"`
}

type ExecutionRunRecord struct {
	RunID       string  `json:"run_id"`
	WorkspaceID string  `json:"workspace_id"`
	TaskID      string  `json:"task_id,omitempty"`
	SessionID   string  `json:"session_id,omitempty"`
	AgentID     string  `json:"agent_id,omitempty"`
	Title       string  `json:"title"`
	Summary     string  `json:"summary,omitempty"`
	Status      string  `json:"status"`
	Outcome     string  `json:"outcome,omitempty"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
	ClosedAt    *string `json:"closed_at,omitempty"`
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
}

type ExecutionRunDetail struct {
	Run   ExecutionRunRecord    `json:"run"`
	Steps []ExecutionStepRecord `json:"steps"`
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

type AgentStateEntry struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	UpdatedAt string `json:"updated_at"`
}

type RPCParamSchema struct {
	Type        string   `json:"type"`
	Required    bool     `json:"required"`
	Description string   `json:"description,omitempty"`
	Enum        []string `json:"enum,omitempty"`
	Default     string   `json:"default,omitempty"`
}

type RPCMethodSchema struct {
	Method      string                    `json:"method"`
	Description string                    `json:"description,omitempty"`
	Params      map[string]RPCParamSchema `json:"params,omitempty"`
}

type AgentHeartbeatLeaseAcquireInput struct {
	WorkspaceID string   `json:"workspace_id"`
	AgentID     string   `json:"agent_id"`
	HeartbeatID string   `json:"heartbeat_id"`
	OwnerID     string   `json:"owner_id"`
	LeaseToken  string   `json:"lease_token"`
	Locks       []string `json:"locks,omitempty"`
	TTLSec      int      `json:"ttl_sec,omitempty"`
}

type AgentHeartbeatLeaseReleaseInput struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	HeartbeatID string `json:"heartbeat_id"`
	LeaseToken  string `json:"lease_token"`
}

type AgentHeartbeatLeaseResult struct {
	Acquired            bool     `json:"acquired"`
	WorkspaceID         string   `json:"workspace_id,omitempty"`
	AgentID             string   `json:"agent_id,omitempty"`
	HeartbeatID         string   `json:"heartbeat_id,omitempty"`
	OwnerID             string   `json:"owner_id,omitempty"`
	LeaseToken          string   `json:"lease_token,omitempty"`
	Locks               []string `json:"locks,omitempty"`
	AcquiredAt          string   `json:"acquired_at,omitempty"`
	ExpiresAt           string   `json:"expires_at,omitempty"`
	ConflictReason      string   `json:"conflict_reason,omitempty"`
	ConflictHeartbeatID string   `json:"conflict_heartbeat_id,omitempty"`
	ConflictLock        string   `json:"conflict_lock,omitempty"`
	ConflictOwnerID     string   `json:"conflict_owner_id,omitempty"`
	ConflictLeaseToken  string   `json:"conflict_lease_token,omitempty"`
	ConflictExpiresAt   string   `json:"conflict_expires_at,omitempty"`
}

type AgentHeartbeatLeaseEnvelope struct {
	Status string                    `json:"status,omitempty"`
	Lease  AgentHeartbeatLeaseResult `json:"lease"`
}

type AgentHeartbeatLeaseReleaseResult struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	HeartbeatID string `json:"heartbeat_id,omitempty"`
	Released    bool   `json:"released"`
	Status      string `json:"status,omitempty"`
}

type TaskMaterialization struct {
	DocKey     string `json:"doc_key,omitempty"`
	DocTitle   string `json:"doc_title,omitempty"`
	DocContent string `json:"doc_content,omitempty"`
}

type StructuredTaskResult struct {
	Outcome       string               `json:"outcome"`
	Summary       string               `json:"summary"`
	Details       string               `json:"details,omitempty"`
	NextAction    string               `json:"next_action,omitempty"`
	Reflection    *TaskCycleReflection `json:"reflection,omitempty"`
	RequiresHuman bool                 `json:"requires_human,omitempty"`
	OwnerAction   string               `json:"owner_action,omitempty"`
	HumanReason   string               `json:"human_reason,omitempty"`
	DecisionType  string               `json:"decision_type,omitempty"`
	BlockedOn     []BlockedRef         `json:"blocked_on,omitempty"`
	MemoryTitle   string               `json:"memory_title,omitempty"`
	MemoryBody    string               `json:"memory_body,omitempty"`
	MemoryType    string               `json:"memory_type,omitempty"`
	Materialize   TaskMaterialization  `json:"materialize,omitempty"`
}

type TaskCycleReflection struct {
	CurrentIntent    string `json:"current_intent,omitempty"`
	FreshEvidence    string `json:"fresh_evidence,omitempty"`
	BlockerFreshness string `json:"blocker_freshness,omitempty"`
	NextUsefulMove   string `json:"next_useful_move,omitempty"`
}

type TaskRunTrace struct {
	AssistantTurns      int
	TotalInputTokens    int
	TotalOutputTokens   int
	ToolCalls           []string
	SuccessfulToolCalls []string
	FailedToolCalls     []string
	ToolReceipts        []TaskRunToolReceipt
	// PromptSectionBytes holds per-iteration prompt composition byte
	// breakdowns (TE-02 prompt telemetry), appended in tool-loop order. The
	// last entry is the most recent LLM call's prompt shape.
	PromptSectionBytes []PromptSectionBytes
}

type TaskRunToolReceipt struct {
	ToolName string
	IsError  bool
	Output   string
}
