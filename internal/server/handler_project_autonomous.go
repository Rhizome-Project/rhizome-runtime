package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type projectProfileGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
}

type projectProfileUpdateParams struct {
	WorkspaceID             string  `json:"workspace_id"`
	ProjectID               string  `json:"project_id"`
	ActorID                 string  `json:"actor_id"`
	Goal                    *string `json:"goal"`
	DesignDocID             *string `json:"design_doc_id"`
	ImplementationPlanDocID *string `json:"implementation_plan_doc_id"`
	RepoRequired            *bool   `json:"repo_required"`
	RepoStatus              *string `json:"repo_status"`
	RepoURL                 *string `json:"repo_url"`
	RepoDefaultBranch       *string `json:"repo_default_branch"`
}

type projectPhaseTransitionParams struct {
	WorkspaceID      string `json:"workspace_id"`
	ProjectID        string `json:"project_id"`
	ActorID          string `json:"actor_id"`
	ToPhase          string `json:"to_phase"`
	Reason           string `json:"reason"`
	CoordinationMode string `json:"coordination_mode"`
}

type projectGatesStatusParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
}

type projectCoordinationGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
}

type projectLeadClaimParams struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	AgentID      string `json:"agent_id"`
	LeaseSeconds int    `json:"lease_seconds"`
	LeaseToken   string `json:"lease_token"`
	Summary      string `json:"summary"`
}

type projectLeadRenewParams struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	RoleID       string `json:"role_id"`
	LeaseSeconds int    `json:"lease_seconds"`
	LeaseToken   string `json:"lease_token"`
	Summary      string `json:"summary"`
}

type projectLeadReleaseParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	RoleID      string `json:"role_id"`
	LeaseToken  string `json:"lease_token"`
	Summary     string `json:"summary"`
}

type projectLeadTransferParams struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	RoleID       string `json:"role_id"`
	ToAgentID    string `json:"to_agent_id"`
	LeaseSeconds int    `json:"lease_seconds"`
	LeaseToken   string `json:"lease_token"`
	Summary      string `json:"summary"`
}

type projectRoleAssignParams struct {
	WorkspaceID    string `json:"workspace_id"`
	ProjectID      string `json:"project_id"`
	ActorID        string `json:"actor_id"`
	AgentID        string `json:"agent_id"`
	RoleType       string `json:"role_type"`
	WriteScopeJSON string `json:"write_scope_json"`
	Summary        string `json:"summary"`
}

type projectRolesListParams struct {
	WorkspaceID     string `json:"workspace_id"`
	ProjectID       string `json:"project_id"`
	IncludeInactive bool   `json:"include_inactive"`
}

type projectRepositoryUpsertParams struct {
	WorkspaceID            string `json:"workspace_id"`
	ProjectID              string `json:"project_id"`
	ActorID                string `json:"actor_id"`
	RepoID                 string `json:"repo_id"`
	RemoteURL              string `json:"remote_url"`
	RemoteKind             string `json:"remote_kind"`
	Owner                  string `json:"owner"`
	Name                   string `json:"name"`
	DefaultBranch          string `json:"default_branch"`
	IntegrationBranch      string `json:"integration_branch"`
	CredentialVaultEntryID string `json:"credential_vault_entry_id"`
	RepoStatus             string `json:"repo_status"`
	IsCanonical            bool   `json:"is_canonical"`
	CreatedByAgentID       string `json:"created_by_agent_id"`
}

type projectRepositoriesListParams struct {
	WorkspaceID     string `json:"workspace_id"`
	ProjectID       string `json:"project_id"`
	IncludeArchived bool   `json:"include_archived"`
}

type projectCheckoutRegisterParams struct {
	WorkspaceID   string `json:"workspace_id"`
	ProjectID     string `json:"project_id"`
	ActorID       string `json:"actor_id"`
	CheckoutID    string `json:"checkout_id"`
	RepoID        string `json:"repo_id"`
	MachineID     string `json:"machine_id"`
	MachineLabel  string `json:"machine_label"`
	OwnerUserID   string `json:"owner_user_id"`
	AgentID       string `json:"agent_id"`
	LocalPath     string `json:"local_path"`
	CheckoutKind  string `json:"checkout_kind"`
	BranchName    string `json:"branch_name"`
	BaseBranch    string `json:"base_branch"`
	HeadSHA       string `json:"head_sha"`
	BaseSHA       string `json:"base_sha"`
	DirtyState    string `json:"dirty_state"`
	ActiveTaskID  string `json:"active_task_id"`
	ActiveClaimID string `json:"active_claim_id"`
	Status        string `json:"status"`
	LastSeenAt    string `json:"last_seen_at"`
}

type projectCheckoutsListParams struct {
	WorkspaceID        string `json:"workspace_id"`
	ProjectID          string `json:"project_id"`
	RepoID             string `json:"repo_id"`
	AgentID            string `json:"agent_id"`
	IncludeInactive    bool   `json:"include_inactive"`
	StaleAfterSeconds  int    `json:"stale_after_seconds"`
	ReferenceTimestamp string `json:"reference_timestamp"`
}

type projectBranchRegisterParams struct {
	WorkspaceID    string `json:"workspace_id"`
	ProjectID      string `json:"project_id"`
	ActorID        string `json:"actor_id"`
	BranchID       string `json:"branch_id"`
	RepoID         string `json:"repo_id"`
	CheckoutID     string `json:"checkout_id"`
	AgentID        string `json:"agent_id"`
	ActiveTaskID   string `json:"active_task_id"`
	ActiveClaimID  string `json:"active_claim_id"`
	BranchName     string `json:"branch_name"`
	BranchKind     string `json:"branch_kind"`
	BaseBranch     string `json:"base_branch"`
	HeadSHA        string `json:"head_sha"`
	BaseSHA        string `json:"base_sha"`
	WriteScopeJSON string `json:"write_scope_json"`
	ReviewDocKey   string `json:"review_doc_key"`
	Status         string `json:"status"`
}

type projectBranchesListParams struct {
	WorkspaceID     string `json:"workspace_id"`
	ProjectID       string `json:"project_id"`
	RepoID          string `json:"repo_id"`
	AgentID         string `json:"agent_id"`
	ActiveTaskID    string `json:"active_task_id"`
	IncludeInactive bool   `json:"include_inactive"`
}

type projectPatchQueueSubmitParams struct {
	WorkspaceID              string            `json:"workspace_id"`
	ProjectID                string            `json:"project_id"`
	ActorID                  string            `json:"actor_id"`
	QueueID                  string            `json:"queue_id"`
	ItemID                   string            `json:"item_id"`
	RepoID                   string            `json:"repo_id"`
	BranchID                 string            `json:"branch_id"`
	ReviewDocKey             string            `json:"review_doc_key"`
	SupersedesQueueID        string            `json:"supersedes_queue_id"`
	SupersedesItemID         string            `json:"supersedes_item_id"`
	EvidenceDocKey           string            `json:"evidence_doc_key"`
	RepoAuthorityMode        string            `json:"repo_authority_mode"`
	PathsetJSON              string            `json:"pathset_json"`
	BaseRef                  string            `json:"base_ref"`
	BaseSHA                  string            `json:"base_sha"`
	HeadSHA                  string            `json:"head_sha"`
	AutoMerge                bool              `json:"auto_merge"`
	TaskID                   string            `json:"task_id"`
	SessionID                string            `json:"session_id"`
	RunID                    string            `json:"run_id"`
	AgentID                  string            `json:"agent_id"`
	PrincipalType            string            `json:"principal_type"`
	PrincipalID              string            `json:"principal_id"`
	CapabilitySnapshotID     string            `json:"capability_snapshot_id"`
	CapabilitySnapshotSchema string            `json:"capability_snapshot_schema"`
	RepoRoot                 string            `json:"repo_root"`
	BaseTreeHash             string            `json:"base_tree_hash"`
	BaseFileHashes           map[string]string `json:"base_file_hashes"`
	BaseFileHashesJSON       string            `json:"base_file_hashes_json"`
	ContextDigest            string            `json:"context_digest"`
	RepoLeaseID              string            `json:"repo_lease_id"`
	LeaseTerm                int64             `json:"lease_term"`
	OperationID              string            `json:"operation_id"`
	OperationKind            string            `json:"operation_kind"`
	MaxAttempts              int               `json:"max_attempts"`
}

type projectPatchQueueSupersedeParams struct {
	WorkspaceID              string            `json:"workspace_id"`
	ProjectID                string            `json:"project_id"`
	ActorID                  string            `json:"actor_id"`
	QueueID                  string            `json:"queue_id"`
	ItemID                   string            `json:"item_id"`
	NewItemID                string            `json:"new_item_id"`
	EvidenceDocKey           string            `json:"evidence_doc_key"`
	TaskID                   string            `json:"task_id"`
	SessionID                string            `json:"session_id"`
	RunID                    string            `json:"run_id"`
	AgentID                  string            `json:"agent_id"`
	PrincipalType            string            `json:"principal_type"`
	PrincipalID              string            `json:"principal_id"`
	CapabilitySnapshotID     string            `json:"capability_snapshot_id"`
	CapabilitySnapshotSchema string            `json:"capability_snapshot_schema"`
	RepoRoot                 string            `json:"repo_root"`
	BaseTreeHash             string            `json:"base_tree_hash"`
	BaseFileHashes           map[string]string `json:"base_file_hashes"`
	BaseFileHashesJSON       string            `json:"base_file_hashes_json"`
	ContextDigest            string            `json:"context_digest"`
	RepoLeaseID              string            `json:"repo_lease_id"`
	LeaseTerm                int64             `json:"lease_term"`
	MaxAttempts              int               `json:"max_attempts"`
}

type projectPatchQueueListParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	RepoID      string `json:"repo_id"`
	BranchID    string `json:"branch_id"`
	State       string `json:"state"`
}

type projectPatchQueueClaimParams struct {
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	ActorID      string `json:"actor_id"`
	QueueID      string `json:"queue_id"`
	ItemID       string `json:"item_id"`
	ClaimToken   string `json:"claim_token"`
	LeaseSeconds int    `json:"lease_seconds"`
}

type projectPatchQueueOperationBindParams struct {
	WorkspaceID       string `json:"workspace_id"`
	ProjectID         string `json:"project_id"`
	ActorID           string `json:"actor_id"`
	QueueID           string `json:"queue_id"`
	ItemID            string `json:"item_id"`
	OperationID       string `json:"operation_id"`
	OperationKind     string `json:"operation_kind"`
	MutationPathsJSON string `json:"mutation_paths_json"`
	ClaimToken        string `json:"claim_token"`
}

type projectPatchQueueCASRecordParams struct {
	WorkspaceID  string                               `json:"workspace_id"`
	ProjectID    string                               `json:"project_id"`
	ActorID      string                               `json:"actor_id"`
	QueueID      string                               `json:"queue_id"`
	ItemID       string                               `json:"item_id"`
	CASResult    repoauthority.CASPatchApplyResult    `json:"cas_result"`
	TestEvidence repoauthority.PatchQueueTestEvidence `json:"test_evidence"`
	ClaimToken   string                               `json:"claim_token"`
}

type projectPatchQueueMaterializationRecordParams struct {
	WorkspaceID     string                             `json:"workspace_id"`
	ProjectID       string                             `json:"project_id"`
	ActorID         string                             `json:"actor_id"`
	QueueID         string                             `json:"queue_id"`
	ItemID          string                             `json:"item_id"`
	Materialization repoauthority.PatchMaterialization `json:"materialization"`
	ClaimToken      string                             `json:"claim_token"`
}

type projectPatchQueueRollbackRecordParams struct {
	WorkspaceID      string                           `json:"workspace_id"`
	ProjectID        string                           `json:"project_id"`
	ActorID          string                           `json:"actor_id"`
	QueueID          string                           `json:"queue_id"`
	ItemID           string                           `json:"item_id"`
	RollbackEvidence repoauthority.PatchQueueRollback `json:"rollback_evidence"`
	ClaimToken       string                           `json:"claim_token"`
}

type projectPatchQueueIntegrationRecordParams struct {
	WorkspaceID           string `json:"workspace_id"`
	ProjectID             string `json:"project_id"`
	ActorID               string `json:"actor_id"`
	QueueID               string `json:"queue_id"`
	ItemID                string `json:"item_id"`
	Outcome               string `json:"outcome"`
	IntegrationMode       string `json:"integration_mode"`
	AuthorityMode         string `json:"authority_mode"`
	RepoID                string `json:"repo_id"`
	SourceBranchID        string `json:"source_branch_id"`
	SourceHeadSHA         string `json:"source_head_sha"`
	TargetBranch          string `json:"target_branch"`
	TargetHeadBefore      string `json:"target_head_before"`
	TargetHeadAfter       string `json:"target_head_after"`
	RemoteTargetHeadAfter string `json:"remote_target_head_after"`
	MergePerformed        bool   `json:"merge_performed"`
	PushAttempted         bool   `json:"push_attempted"`
	PushSucceeded         bool   `json:"push_succeeded"`
	AlreadyIntegrated     bool   `json:"already_integrated"`
	RepairReason          string `json:"repair_reason"`
}

type projectPatchQueueReviewerAdvisoryRecordParams struct {
	WorkspaceID      string                                   `json:"workspace_id"`
	ProjectID        string                                   `json:"project_id"`
	ActorID          string                                   `json:"actor_id"`
	QueueID          string                                   `json:"queue_id"`
	ItemID           string                                   `json:"item_id"`
	ReviewerAdvisory repoauthority.PatchQueueReviewerAdvisory `json:"reviewer_advisory"`
	ClaimToken       string                                   `json:"claim_token"`
}

type projectPatchQueueOperatorEnablementRecordParams struct {
	WorkspaceID        string                                     `json:"workspace_id"`
	ProjectID          string                                     `json:"project_id"`
	ActorID            string                                     `json:"actor_id"`
	QueueID            string                                     `json:"queue_id"`
	ItemID             string                                     `json:"item_id"`
	OperatorEnablement repoauthority.PatchQueueOperatorEnablement `json:"operator_enablement"`
	ClaimToken         string                                     `json:"claim_token"`
}

type operatorPatchQueueEnableParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	QueueID     string `json:"queue_id"`
	ItemID      string `json:"item_id"`
	ClaimToken  string `json:"claim_token"`
	Reason      string `json:"reason"`
}

type projectPatchQueueReleaseParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	QueueID     string `json:"queue_id"`
	ItemID      string `json:"item_id"`
	ClaimToken  string `json:"claim_token"`
}

type projectPatchQueueDecisionParams struct {
	WorkspaceID          string   `json:"workspace_id"`
	ProjectID            string   `json:"project_id"`
	ActorID              string   `json:"actor_id"`
	QueueID              string   `json:"queue_id"`
	ItemID               string   `json:"item_id"`
	Decision             string   `json:"decision"`
	DecisionDocKey       string   `json:"decision_doc_key"`
	DecisionSummary      string   `json:"decision_summary"`
	CheckedSourceDocKeys []string `json:"checked_source_doc_keys"`
	ClaimToken           string   `json:"claim_token"`
}

type projectPatchQueueReviewTaskReconcileParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	QueueID     string `json:"queue_id"`
	ItemID      string `json:"item_id"`
}

type projectPatchQueueDecisionContinuationConsumeParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
	OutboxID    string `json:"outbox_id"`
	QueueID     string `json:"queue_id"`
	ItemID      string `json:"item_id"`
}

func (h *Handler) projectProfileGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectProfileGetParams
	workspaceID, projectID, rpcErr := h.decodeProjectProfileGetParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	profile, err := h.store.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"profile": profile}, nil
}

func (h *Handler) projectProfileUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectProfileUpdateParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectProfileUpdateParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if !projectProfileUpdateHasChanges(p) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "at least one project profile field is required"}
	}
	profile, event, err := h.store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		Goal:                    p.Goal,
		DesignDocID:             p.DesignDocID,
		ImplementationPlanDocID: p.ImplementationPlanDocID,
		RepoRequired:            p.RepoRequired,
		RepoStatus:              p.RepoStatus,
		RepoURL:                 p.RepoURL,
		RepoDefaultBranch:       p.RepoDefaultBranch,
		ActorID:                 actorID,
		ActorType:               principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.profile.update", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.profile.update",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.profile.update")
	}
	h.publishRuntimeEventRecord(event, projectID)
	return map[string]any{"profile": profile}, nil
}

func (h *Handler) projectPhaseTransition(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPhaseTransitionParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPhaseTransitionParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	toPhase := strings.TrimSpace(p.ToPhase)
	if rpcErr := requireTrimmedParam(toPhase, "to_phase"); rpcErr != nil {
		return nil, rpcErr
	}
	profile, history, event, err := h.store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		ToPhase:          toPhase,
		Reason:           p.Reason,
		ActorID:          actorID,
		ActorType:        principal.PrincipalType,
		CoordinationMode: strings.TrimSpace(p.CoordinationMode),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.phase.transition", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
			"to_phase":   toPhase,
		}),
		PromptContextSurface: "project.phase.transition",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.phase.transition")
	}
	h.publishRuntimeEventRecord(event, projectID, history.ToPhase)
	return map[string]any{
		"profile": profile,
		"history": history,
	}, nil
}

func (h *Handler) projectGatesStatus(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGatesStatusParams
	workspaceID, projectID, rpcErr := h.decodeProjectGatesStatusParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	status, err := h.store.GetProjectGateStatus(ctx, workspaceID, projectID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"gate_status": status}, nil
}

func (h *Handler) projectCoordinationGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectCoordinationGetParams
	workspaceID, projectID, rpcErr := h.decodeProjectCoordinationGetParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	coordination, err := h.store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"coordination": coordination}, nil
}

func (h *Handler) projectLeadClaim(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectLeadClaimParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectLeadClaimParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	agentID := strings.TrimSpace(p.AgentID)
	role, event, err := h.store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		ActorType:    principal.PrincipalType,
		AgentID:      agentID,
		LeaseSeconds: p.LeaseSeconds,
		LeaseToken:   strings.TrimSpace(p.LeaseToken),
		Summary:      strings.TrimSpace(p.Summary),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.lead.claim", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
			"agent_id":   agentID,
		}),
		PromptContextSurface: "project.lead.claim",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.lead.claim")
	}
	h.publishRuntimeEventRecord(event, projectID, agentID)
	h.publishRuntimeEventRecordAs(event, "project.lead.changed", projectID, agentID)
	return map[string]any{"role": role}, nil
}

func (h *Handler) projectLeadRenew(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectLeadRenewParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectLeadRenewParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	roleID := strings.TrimSpace(p.RoleID)
	role, event, err := h.store.RenewProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadRenewInput{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		ActorType:    principal.PrincipalType,
		RoleID:       roleID,
		LeaseSeconds: p.LeaseSeconds,
		LeaseToken:   strings.TrimSpace(p.LeaseToken),
		Summary:      strings.TrimSpace(p.Summary),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.lead.renew", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
			"role_id":    roleID,
		}),
		PromptContextSurface: "project.lead.renew",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.lead.renew")
	}
	h.publishRuntimeEventRecord(event, projectID, roleID)
	h.publishRuntimeEventRecordAs(event, "project.lead.changed", projectID, roleID)
	return map[string]any{"role": role}, nil
}

func (h *Handler) projectLeadRelease(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectLeadReleaseParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectLeadReleaseParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	roleID := strings.TrimSpace(p.RoleID)
	role, event, err := h.store.ReleaseProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadReleaseInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
		RoleID:      roleID,
		LeaseToken:  strings.TrimSpace(p.LeaseToken),
		Summary:     strings.TrimSpace(p.Summary),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.lead.release", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
			"role_id":    roleID,
		}),
		PromptContextSurface: "project.lead.release",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.lead.release")
	}
	h.publishRuntimeEventRecord(event, projectID, roleID)
	h.publishRuntimeEventRecordAs(event, "project.lead.changed", projectID, roleID)
	return map[string]any{"role": role}, nil
}

func (h *Handler) projectLeadTransfer(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectLeadTransferParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectLeadTransferParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	roleID := strings.TrimSpace(p.RoleID)
	toAgentID := strings.TrimSpace(p.ToAgentID)
	role, event, err := h.store.TransferProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadTransferInput{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		ActorType:    principal.PrincipalType,
		RoleID:       roleID,
		ToAgentID:    toAgentID,
		LeaseSeconds: p.LeaseSeconds,
		LeaseToken:   strings.TrimSpace(p.LeaseToken),
		Summary:      strings.TrimSpace(p.Summary),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.lead.transfer", map[string]string{
			"project_id":   projectID,
			"actor_id":     actorID,
			"from_role_id": roleID,
			"to_agent_id":  toAgentID,
		}),
		PromptContextSurface: "project.lead.transfer",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.lead.transfer")
	}
	h.publishRuntimeEventRecord(event, projectID, roleID, toAgentID)
	h.publishRuntimeEventRecordAs(event, "project.lead.changed", projectID, roleID, toAgentID)
	return map[string]any{"role": role}, nil
}

func (h *Handler) projectRoleAssign(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectRoleAssignParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectRoleAssignParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	agentID := strings.TrimSpace(p.AgentID)
	roleType := strings.TrimSpace(p.RoleType)
	role, event, err := h.store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		ActorType:      principal.PrincipalType,
		AgentID:        agentID,
		RoleType:       roleType,
		WriteScopeJSON: strings.TrimSpace(p.WriteScopeJSON),
		Summary:        strings.TrimSpace(p.Summary),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.role.assign", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
			"agent_id":   agentID,
			"role_type":  roleType,
		}),
		PromptContextSurface: "project.role.assign",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.role.assign")
	}
	h.publishRuntimeEventRecord(event, projectID, agentID, roleType)
	h.publishRuntimeEventRecordAs(event, "project.role.changed", projectID, agentID, roleType)
	response := map[string]any{"role": role}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err == nil {
		if rebind, ok := payload["active_claim_rebind"]; ok {
			response["active_claim_rebind"] = rebind
		}
	}
	return response, nil
}

func (h *Handler) projectRolesList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectRolesListParams
	workspaceID, projectID, rpcErr := h.decodeProjectRolesListParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetProject(ctx, workspaceID, projectID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	roles, err := h.store.ListProjectRoles(ctx, workspaceID, projectID, p.IncludeInactive)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"roles": roles,
		"count": len(roles),
	}, nil
}

func (h *Handler) projectRepositoryUpsert(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectRepositoryUpsertParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectRepositoryUpsertParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	repo, event, err := h.store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:            workspaceID,
		ProjectID:              projectID,
		ActorID:                actorID,
		ActorType:              principal.PrincipalType,
		RepoID:                 strings.TrimSpace(p.RepoID),
		RemoteURL:              strings.TrimSpace(p.RemoteURL),
		RemoteKind:             strings.TrimSpace(p.RemoteKind),
		Owner:                  strings.TrimSpace(p.Owner),
		Name:                   strings.TrimSpace(p.Name),
		DefaultBranch:          strings.TrimSpace(p.DefaultBranch),
		IntegrationBranch:      strings.TrimSpace(p.IntegrationBranch),
		CredentialVaultEntryID: strings.TrimSpace(p.CredentialVaultEntryID),
		RepoStatus:             strings.TrimSpace(p.RepoStatus),
		IsCanonical:            p.IsCanonical,
		CreatedByAgentID:       strings.TrimSpace(p.CreatedByAgentID),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.repository.upsert", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
			"repo_id":    strings.TrimSpace(p.RepoID),
		}),
		PromptContextSurface: "project.repository.upsert",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.repository.upsert")
	}
	h.publishRuntimeEventRecord(event, projectID, repo.RepoID)
	h.publishRuntimeEventRecordAs(event, "project.repository.changed", projectID, repo.RepoID)
	return map[string]any{"repository": repo}, nil
}

func (h *Handler) projectRepositoriesList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectRepositoriesListParams
	workspaceID, projectID, rpcErr := h.decodeProjectRepositoriesListParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetProject(ctx, workspaceID, projectID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	repos, err := h.store.ListProjectRepositories(ctx, sqlite.ProjectRepositoryListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		IncludeArchived: p.IncludeArchived,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"repositories": repos, "count": len(repos)}, nil
}

func (h *Handler) projectCheckoutRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectCheckoutRegisterParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectCheckoutRegisterParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	checkout, event, err := h.store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:   workspaceID,
		ProjectID:     projectID,
		ActorID:       actorID,
		ActorType:     principal.PrincipalType,
		CheckoutID:    strings.TrimSpace(p.CheckoutID),
		RepoID:        strings.TrimSpace(p.RepoID),
		MachineID:     strings.TrimSpace(p.MachineID),
		MachineLabel:  strings.TrimSpace(p.MachineLabel),
		OwnerUserID:   strings.TrimSpace(p.OwnerUserID),
		AgentID:       strings.TrimSpace(p.AgentID),
		LocalPath:     strings.TrimSpace(p.LocalPath),
		CheckoutKind:  strings.TrimSpace(p.CheckoutKind),
		BranchName:    strings.TrimSpace(p.BranchName),
		BaseBranch:    strings.TrimSpace(p.BaseBranch),
		HeadSHA:       strings.TrimSpace(p.HeadSHA),
		BaseSHA:       strings.TrimSpace(p.BaseSHA),
		DirtyState:    strings.TrimSpace(p.DirtyState),
		ActiveTaskID:  strings.TrimSpace(p.ActiveTaskID),
		ActiveClaimID: strings.TrimSpace(p.ActiveClaimID),
		Status:        strings.TrimSpace(p.Status),
		LastSeenAt:    strings.TrimSpace(p.LastSeenAt),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.checkout.register", map[string]string{
			"project_id":  projectID,
			"actor_id":    actorID,
			"repo_id":     strings.TrimSpace(p.RepoID),
			"checkout_id": strings.TrimSpace(p.CheckoutID),
		}),
		PromptContextSurface: "project.checkout.register",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.checkout.register")
	}
	h.publishRuntimeEventRecord(event, projectID, checkout.CheckoutID)
	h.publishRuntimeEventRecordAs(event, "project.checkout.changed", projectID, checkout.CheckoutID)
	return map[string]any{"checkout": checkout}, nil
}

func (h *Handler) projectCheckoutsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectCheckoutsListParams
	workspaceID, projectID, rpcErr := h.decodeProjectCheckoutsListParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetProject(ctx, workspaceID, projectID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	checkouts, err := h.store.ListProjectCheckouts(ctx, sqlite.ProjectCheckoutListFilter{
		WorkspaceID:        workspaceID,
		ProjectID:          projectID,
		RepoID:             strings.TrimSpace(p.RepoID),
		AgentID:            strings.TrimSpace(p.AgentID),
		IncludeInactive:    p.IncludeInactive,
		StaleAfterSeconds:  p.StaleAfterSeconds,
		ReferenceTimestamp: strings.TrimSpace(p.ReferenceTimestamp),
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"checkouts": checkouts, "count": len(checkouts)}, nil
}

func (h *Handler) projectBranchRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectBranchRegisterParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectBranchRegisterParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	branch, event, err := h.store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		ActorType:      principal.PrincipalType,
		BranchID:       strings.TrimSpace(p.BranchID),
		RepoID:         strings.TrimSpace(p.RepoID),
		CheckoutID:     strings.TrimSpace(p.CheckoutID),
		AgentID:        strings.TrimSpace(p.AgentID),
		ActiveTaskID:   strings.TrimSpace(p.ActiveTaskID),
		ActiveClaimID:  strings.TrimSpace(p.ActiveClaimID),
		BranchName:     strings.TrimSpace(p.BranchName),
		BranchKind:     strings.TrimSpace(p.BranchKind),
		BaseBranch:     strings.TrimSpace(p.BaseBranch),
		HeadSHA:        strings.TrimSpace(p.HeadSHA),
		BaseSHA:        strings.TrimSpace(p.BaseSHA),
		WriteScopeJSON: strings.TrimSpace(p.WriteScopeJSON),
		ReviewDocKey:   strings.TrimSpace(p.ReviewDocKey),
		Status:         strings.TrimSpace(p.Status),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.branch.register", map[string]string{
			"project_id":  projectID,
			"actor_id":    actorID,
			"repo_id":     strings.TrimSpace(p.RepoID),
			"branch_id":   strings.TrimSpace(p.BranchID),
			"branch_name": strings.TrimSpace(p.BranchName),
		}),
		PromptContextSurface: "project.branch.register",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.branch.register")
	}
	h.publishRuntimeEventRecord(event, projectID, branch.BranchID, branch.BranchName)
	h.publishRuntimeEventRecordAs(event, "project.branch.changed", projectID, branch.BranchID, branch.BranchName)
	result := map[string]any{"branch": branch}
	if branch.Status == sqlite.ProjectBranchStatusReadyForReview {
		result["receipt_state"] = "branch_registry_ready_for_review"
		result["mandatory_next_tool"] = "project_patch_queue_submit"
		result["patch_queue_item_receipt"] = "not_created_by_project_branch_register"
		result["patch_queue_auto_submitted"] = false
		result["patch_queue_review_task_created"] = false
	}
	return result, nil
}

func projectPatchQueueItemIsLive(item sqlite.ProjectPatchQueueItemRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(item.State)) {
	case sqlite.ProjectPatchQueueStateProposed, sqlite.ProjectPatchQueueStateClaimed:
		return true
	default:
		return false
	}
}

func (h *Handler) ensureProjectPatchQueueReviewTask(ctx context.Context, workspaceID, projectID, actorID, actorType string, item sqlite.ProjectPatchQueueItemRecord, branch sqlite.ProjectBranchRecord) (sqlite.TaskStatus, bool, *RPCError) {
	items, err := h.store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		return sqlite.TaskStatus{}, false, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	for _, candidate := range items {
		if strings.TrimSpace(candidate.QueueID) != strings.TrimSpace(item.QueueID) || strings.TrimSpace(candidate.ItemID) != strings.TrimSpace(item.ItemID) {
			continue
		}
		if strings.TrimSpace(candidate.ReviewTaskID) == "" || strings.TrimSpace(candidate.ReviewTaskEventID) == "" {
			return sqlite.TaskStatus{}, false, nil
		}
		status, err := h.store.GetTaskStatus(ctx, workspaceID, candidate.ReviewTaskID)
		if err != nil {
			return sqlite.TaskStatus{}, false, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("patch queue review task readback failed: %s", err.Error())}
		}
		return status, false, nil
	}
	return sqlite.TaskStatus{}, false, nil
}

func projectPatchQueueReviewTaskID(item sqlite.ProjectPatchQueueItemRecord) string {
	return sqlite.ProjectPatchQueueReviewTaskID(item)
}

func projectPatchQueueReviewTaskIDWithAttempt(base string, attempt int) string {
	return sqlite.ProjectPatchQueueReviewTaskIDWithAttempt(base, attempt)
}

func projectPatchQueueReviewTaskReusable(status sqlite.TaskStatus, projectID string) bool {
	if strings.TrimSpace(status.ProjectID) != strings.TrimSpace(projectID) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(status.ProjectLane), "review") {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(status.Status)) {
	case "RESOLVED", "FAILED", "CANCELLED":
		return false
	default:
		return true
	}
}

func (h *Handler) projectBranchesList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectBranchesListParams
	workspaceID, projectID, rpcErr := h.decodeProjectBranchesListParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetProject(ctx, workspaceID, projectID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	branches, err := h.store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		RepoID:          strings.TrimSpace(p.RepoID),
		AgentID:         strings.TrimSpace(p.AgentID),
		ActiveTaskID:    strings.TrimSpace(p.ActiveTaskID),
		IncludeInactive: p.IncludeInactive,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"branches": branches, "count": len(branches)}, nil
}

func (h *Handler) projectPatchQueueSubmit(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueSubmitParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueSubmitParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	repoAuthorityMode := firstNonEmpty(strings.TrimSpace(p.RepoAuthorityMode), sqlite.ProjectPatchQueueAuthorityModeControlledQueue)
	item, event, err := h.store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  actorID,
		ActorType:                principal.PrincipalType,
		QueueID:                  strings.TrimSpace(p.QueueID),
		ItemID:                   strings.TrimSpace(p.ItemID),
		RepoID:                   strings.TrimSpace(p.RepoID),
		BranchID:                 strings.TrimSpace(p.BranchID),
		ReviewDocKey:             strings.TrimSpace(p.ReviewDocKey),
		SupersedesQueueID:        strings.TrimSpace(p.SupersedesQueueID),
		SupersedesItemID:         strings.TrimSpace(p.SupersedesItemID),
		EvidenceDocKey:           strings.TrimSpace(p.EvidenceDocKey),
		RepoAuthorityMode:        repoAuthorityMode,
		PathsetJSON:              strings.TrimSpace(p.PathsetJSON),
		BaseRef:                  strings.TrimSpace(p.BaseRef),
		BaseSHA:                  strings.TrimSpace(p.BaseSHA),
		HeadSHA:                  strings.TrimSpace(p.HeadSHA),
		AutoMerge:                p.AutoMerge,
		TaskID:                   strings.TrimSpace(p.TaskID),
		SessionID:                strings.TrimSpace(p.SessionID),
		RunID:                    strings.TrimSpace(p.RunID),
		AgentID:                  strings.TrimSpace(p.AgentID),
		PrincipalType:            principal.PrincipalType,
		PrincipalID:              actorID,
		CapabilitySnapshotID:     strings.TrimSpace(p.CapabilitySnapshotID),
		CapabilitySnapshotSchema: strings.TrimSpace(p.CapabilitySnapshotSchema),
		RepoRoot:                 strings.TrimSpace(p.RepoRoot),
		BaseTreeHash:             strings.TrimSpace(p.BaseTreeHash),
		BaseFileHashes:           p.BaseFileHashes,
		BaseFileHashesJSON:       strings.TrimSpace(p.BaseFileHashesJSON),
		ContextDigest:            strings.TrimSpace(p.ContextDigest),
		RepoLeaseID:              strings.TrimSpace(p.RepoLeaseID),
		LeaseTerm:                p.LeaseTerm,
		OperationID:              strings.TrimSpace(p.OperationID),
		OperationKind:            strings.TrimSpace(p.OperationKind),
		MaxAttempts:              p.MaxAttempts,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.submit", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
			"repo_id":    strings.TrimSpace(p.RepoID),
			"branch_id":  strings.TrimSpace(p.BranchID),
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
		}),
		PromptContextSurface: "project.patch_queue.submit",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.submit")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID)
	result := map[string]any{"patch_queue_item": item}
	if branch, ok, rpcErr := h.projectBranchForPatchQueueItem(ctx, workspaceID, projectID, item); rpcErr != nil {
		return nil, rpcErr
	} else if ok {
		reviewTask, created, rpcErr := h.ensureProjectPatchQueueReviewTask(ctx, workspaceID, projectID, actorID, principal.PrincipalType, item, branch)
		if rpcErr != nil {
			return nil, rpcErr
		}
		result["patch_queue_review_task"] = reviewTask
		result["patch_queue_review_task_created"] = created
	}
	return result, nil
}

func (h *Handler) projectPatchQueueSupersede(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueSupersedeParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueSupersedeParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	principalType := ""
	principalID := ""
	if projectPatchQueueSupersedeParamsHasNonPrincipalBindingRefs(p) {
		principalType = principal.PrincipalType
		principalID = actorID
	}
	item, event, alreadyQueued, err := h.store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  actorID,
		ActorType:                principal.PrincipalType,
		QueueID:                  strings.TrimSpace(p.QueueID),
		ItemID:                   strings.TrimSpace(p.ItemID),
		NewItemID:                strings.TrimSpace(p.NewItemID),
		EvidenceDocKey:           strings.TrimSpace(p.EvidenceDocKey),
		TaskID:                   strings.TrimSpace(p.TaskID),
		SessionID:                strings.TrimSpace(p.SessionID),
		RunID:                    strings.TrimSpace(p.RunID),
		AgentID:                  strings.TrimSpace(p.AgentID),
		PrincipalType:            principalType,
		PrincipalID:              principalID,
		CapabilitySnapshotID:     strings.TrimSpace(p.CapabilitySnapshotID),
		CapabilitySnapshotSchema: strings.TrimSpace(p.CapabilitySnapshotSchema),
		RepoRoot:                 strings.TrimSpace(p.RepoRoot),
		BaseTreeHash:             strings.TrimSpace(p.BaseTreeHash),
		BaseFileHashes:           p.BaseFileHashes,
		BaseFileHashesJSON:       strings.TrimSpace(p.BaseFileHashesJSON),
		ContextDigest:            strings.TrimSpace(p.ContextDigest),
		RepoLeaseID:              strings.TrimSpace(p.RepoLeaseID),
		LeaseTerm:                p.LeaseTerm,
		MaxAttempts:              p.MaxAttempts,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.supersede", map[string]string{
			"project_id":          projectID,
			"actor_id":            actorID,
			"queue_id":            strings.TrimSpace(p.QueueID),
			"item_id":             strings.TrimSpace(p.NewItemID),
			"new_item_id":         strings.TrimSpace(p.NewItemID),
			"supersedes_queue_id": strings.TrimSpace(p.QueueID),
			"supersedes_item_id":  strings.TrimSpace(p.ItemID),
			"evidence_doc_key":    strings.TrimSpace(p.EvidenceDocKey),
		}),
		PromptContextSurface: "project.patch_queue.supersede",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.supersede")
	}
	if strings.TrimSpace(event.EventID) != "" {
		h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID)
		h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID)
	}
	result := map[string]any{"patch_queue_item": item, "already_queued": alreadyQueued}
	if branch, ok, rpcErr := h.projectBranchForPatchQueueItem(ctx, workspaceID, projectID, item); rpcErr != nil {
		return nil, rpcErr
	} else if ok {
		reviewTask, created, rpcErr := h.ensureProjectPatchQueueReviewTask(ctx, workspaceID, projectID, actorID, principal.PrincipalType, item, branch)
		if rpcErr != nil {
			return nil, rpcErr
		}
		result["patch_queue_review_task"] = reviewTask
		result["patch_queue_review_task_created"] = created
	}
	return result, nil
}

func projectPatchQueueSupersedeParamsHasNonPrincipalBindingRefs(p projectPatchQueueSupersedeParams) bool {
	if p.LeaseTerm != 0 || len(p.BaseFileHashes) > 0 {
		return true
	}
	for _, value := range []string{
		p.TaskID,
		p.SessionID,
		p.RunID,
		p.AgentID,
		p.CapabilitySnapshotID,
		p.CapabilitySnapshotSchema,
		p.RepoRoot,
		p.BaseTreeHash,
		p.BaseFileHashesJSON,
		p.ContextDigest,
		p.RepoLeaseID,
	} {
		value = strings.TrimSpace(value)
		if value != "" && value != "{}" {
			return true
		}
	}
	return false
}

func (h *Handler) projectBranchForPatchQueueItem(ctx context.Context, workspaceID, projectID string, item sqlite.ProjectPatchQueueItemRecord) (sqlite.ProjectBranchRecord, bool, *RPCError) {
	branches, err := h.store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		RepoID:          item.RepoID,
		IncludeInactive: true,
	})
	if err != nil {
		return sqlite.ProjectBranchRecord{}, false, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) == strings.TrimSpace(item.BranchID) {
			return branch, true, nil
		}
	}
	return sqlite.ProjectBranchRecord{}, false, nil
}

func (h *Handler) projectPatchQueueList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueListParams
	workspaceID, projectID, rpcErr := h.decodeProjectPatchQueueListParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetProject(ctx, workspaceID, projectID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	items, err := h.store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      strings.TrimSpace(p.RepoID),
		BranchID:    strings.TrimSpace(p.BranchID),
		State:       strings.TrimSpace(p.State),
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"patch_queue_items": items, "count": len(items)}, nil
}

func (h *Handler) projectPatchQueueClaim(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueClaimParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueClaimParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		QueueID:      strings.TrimSpace(p.QueueID),
		ItemID:       strings.TrimSpace(p.ItemID),
		ClaimToken:   strings.TrimSpace(p.ClaimToken),
		LeaseSeconds: p.LeaseSeconds,
		ActorID:      actorID,
		ActorType:    principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.claim", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.claim",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.claim")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueOperationBind(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueOperationBindParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueOperationBindParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		QueueID:           strings.TrimSpace(p.QueueID),
		ItemID:            strings.TrimSpace(p.ItemID),
		OperationID:       strings.TrimSpace(p.OperationID),
		OperationKind:     strings.TrimSpace(p.OperationKind),
		MutationPathsJSON: strings.TrimSpace(p.MutationPathsJSON),
		ClaimToken:        strings.TrimSpace(p.ClaimToken),
		ActorID:           actorID,
		ActorType:         principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.operation_bind", map[string]string{
			"project_id":     projectID,
			"queue_id":       strings.TrimSpace(p.QueueID),
			"item_id":        strings.TrimSpace(p.ItemID),
			"operation_id":   strings.TrimSpace(p.OperationID),
			"operation_kind": strings.TrimSpace(p.OperationKind),
			"actor_id":       actorID,
		}),
		PromptContextSurface: "project.patch_queue.operation_bind",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.operation_bind")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueCASRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueCASRecordParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueCASRecordParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		QueueID:      strings.TrimSpace(p.QueueID),
		ItemID:       strings.TrimSpace(p.ItemID),
		CASResult:    p.CASResult,
		TestEvidence: p.TestEvidence,
		ClaimToken:   strings.TrimSpace(p.ClaimToken),
		ActorID:      actorID,
		ActorType:    principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.cas_record", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.cas_record",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.cas_record")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.CASPatchDigest)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.CASPatchDigest)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueMaterializationRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueMaterializationRecordParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueMaterializationRecordParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		QueueID:         strings.TrimSpace(p.QueueID),
		ItemID:          strings.TrimSpace(p.ItemID),
		Materialization: p.Materialization,
		ClaimToken:      strings.TrimSpace(p.ClaimToken),
		ActorID:         actorID,
		ActorType:       principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.materialization_record", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.materialization_record",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.materialization_record")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.MaterializationDigest)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.MaterializationDigest)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueRollbackRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueRollbackRecordParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueRollbackRecordParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.RecordProjectPatchQueueRollbackEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueRollbackRecordInput{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		QueueID:          strings.TrimSpace(p.QueueID),
		ItemID:           strings.TrimSpace(p.ItemID),
		RollbackEvidence: p.RollbackEvidence,
		ClaimToken:       strings.TrimSpace(p.ClaimToken),
		ActorID:          actorID,
		ActorType:        principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.rollback_record", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.rollback_record",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.rollback_record")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.RollbackEvidenceDigest)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.RollbackEvidenceDigest)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueIntegrationRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.projectPatchQueueIntegrationRecordWithMethod(ctx, raw, "project.patch_queue.integration_record", "")
}

func (h *Handler) projectPatchQueueIntegrationRepair(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.projectPatchQueueIntegrationRecordWithMethod(ctx, raw, "project.patch_queue.integration_repair", sqlite.ProjectPatchQueueIntegrationOutcomeRepair)
}

func (h *Handler) projectPatchQueueIntegrationRecordWithMethod(ctx context.Context, raw json.RawMessage, method string, defaultOutcome string) (any, *RPCError) {
	var p projectPatchQueueIntegrationRecordParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueIntegrationRecordParams(ctx, raw, &p, defaultOutcome)
	if rpcErr != nil {
		return nil, rpcErr
	}
	outcome := firstNonEmpty(strings.TrimSpace(p.Outcome), strings.TrimSpace(defaultOutcome))
	item, event, err := h.store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               strings.TrimSpace(p.QueueID),
		ItemID:                strings.TrimSpace(p.ItemID),
		ActorID:               actorID,
		ActorType:             principal.PrincipalType,
		Outcome:               outcome,
		IntegrationMode:       strings.TrimSpace(p.IntegrationMode),
		RepoID:                strings.TrimSpace(p.RepoID),
		SourceBranchID:        strings.TrimSpace(p.SourceBranchID),
		SourceHeadSHA:         strings.TrimSpace(p.SourceHeadSHA),
		TargetBranch:          strings.TrimSpace(p.TargetBranch),
		TargetHeadBefore:      strings.TrimSpace(p.TargetHeadBefore),
		TargetHeadAfter:       strings.TrimSpace(p.TargetHeadAfter),
		RemoteTargetHeadAfter: strings.TrimSpace(p.RemoteTargetHeadAfter),
		MergePerformed:        p.MergePerformed,
		PushAttempted:         p.PushAttempted,
		PushSucceeded:         p.PushSucceeded,
		AlreadyIntegrated:     p.AlreadyIntegrated,
		RepairReason:          strings.TrimSpace(p.RepairReason),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, method, map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"outcome":    outcome,
			"actor_id":   actorID,
		}),
		PromptContextSurface: method,
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, method)
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.MaterializationDigest)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.MaterializationDigest)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueReviewerAdvisoryRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueReviewerAdvisoryRecordParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueReviewerAdvisoryRecordParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx, sqlite.ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		QueueID:          strings.TrimSpace(p.QueueID),
		ItemID:           strings.TrimSpace(p.ItemID),
		ReviewerAdvisory: p.ReviewerAdvisory,
		ClaimToken:       strings.TrimSpace(p.ClaimToken),
		ActorID:          actorID,
		ActorType:        principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.reviewer_advisory_record", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.reviewer_advisory_record",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.reviewer_advisory_record")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.ReviewerAdvisoryDigest)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.ReviewerAdvisoryDigest)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueOperatorEnablementRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueOperatorEnablementRecordParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueOperatorEnablementRecordParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID:        workspaceID,
		ProjectID:          projectID,
		QueueID:            strings.TrimSpace(p.QueueID),
		ItemID:             strings.TrimSpace(p.ItemID),
		OperatorEnablement: p.OperatorEnablement,
		ClaimToken:         strings.TrimSpace(p.ClaimToken),
		ActorID:            actorID,
		ActorType:          principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.operator_enablement_record", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.operator_enablement_record",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.operator_enablement_record")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.OperatorEnablementDigest)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.OperatorEnablementDigest)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) operatorPatchQueueEnable(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p operatorPatchQueueEnableParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeOperatorPatchQueueEnableParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     strings.TrimSpace(p.QueueID),
		ItemID:      strings.TrimSpace(p.ItemID),
		OperatorEnablement: repoauthority.PatchQueueOperatorEnablement{
			Enabled: true,
			Reason:  strings.TrimSpace(p.Reason),
		},
		ClaimToken: strings.TrimSpace(p.ClaimToken),
		ActorID:    actorID,
		ActorType:  principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "operator.patch_queue.enable", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "operator.patch_queue.enable",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "operator.patch_queue.enable")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.OperatorEnablementDigest)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID, item.OperationID, item.OperatorEnablementDigest)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueRelease(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueReleaseParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueReleaseParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     strings.TrimSpace(p.QueueID),
		ItemID:      strings.TrimSpace(p.ItemID),
		ClaimToken:  strings.TrimSpace(p.ClaimToken),
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.release", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.release",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.release")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueDecision(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueDecisionParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueDecisionParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, event, err := h.store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:          workspaceID,
		ProjectID:            projectID,
		QueueID:              strings.TrimSpace(p.QueueID),
		ItemID:               strings.TrimSpace(p.ItemID),
		Decision:             strings.TrimSpace(p.Decision),
		DecisionDocKey:       strings.TrimSpace(p.DecisionDocKey),
		DecisionSummary:      strings.TrimSpace(p.DecisionSummary),
		CheckedSourceDocKeys: p.CheckedSourceDocKeys,
		ClaimToken:           strings.TrimSpace(p.ClaimToken),
		ActorID:              actorID,
		ActorType:            principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.patch_queue.decision", map[string]string{
			"project_id": projectID,
			"queue_id":   strings.TrimSpace(p.QueueID),
			"item_id":    strings.TrimSpace(p.ItemID),
			"decision":   strings.TrimSpace(p.Decision),
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.patch_queue.decision",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.patch_queue.decision")
	}
	h.publishRuntimeEventRecord(event, projectID, item.QueueID, item.ItemID, item.BranchID)
	h.publishRuntimeEventRecordAs(event, "project.patch_queue.changed", projectID, item.QueueID, item.ItemID, item.BranchID)
	return map[string]any{"patch_queue_item": item}, nil
}

func (h *Handler) projectPatchQueueReviewTaskReconcile(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueReviewTaskReconcileParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueReviewTaskReconcileParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	status, eventID, repaired, err := h.store.ReconcileProjectPatchQueueReviewTaskReceipt(ctx, sqlite.ProjectPatchQueueReviewTaskReconcileInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     strings.TrimSpace(p.QueueID),
		ItemID:      strings.TrimSpace(p.ItemID),
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
	})
	if err != nil {
		return nil, rpcErrorFromProjectPatchQueueStore(err, "project.patch_queue.review_task.reconcile")
	}
	return map[string]any{
		"patch_queue_review_task": status,
		"review_task_event_id":    eventID,
		"repaired":                repaired,
	}, nil
}

func (h *Handler) projectPatchQueueDecisionContinuationConsume(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectPatchQueueDecisionContinuationConsumeParams
	workspaceID, projectID, actorID, principal, rpcErr := h.decodeProjectPatchQueueDecisionContinuationConsumeParams(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	status, continuation, created, err := h.store.ConsumeProjectPatchQueueDecisionContinuation(ctx, sqlite.ProjectPatchQueueDecisionContinuationConsumeInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		OutboxID:    strings.TrimSpace(p.OutboxID),
		QueueID:     strings.TrimSpace(p.QueueID),
		ItemID:      strings.TrimSpace(p.ItemID),
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
	})
	if err != nil {
		return nil, rpcErrorFromProjectPatchQueueStore(err, "project.patch_queue.decision_continuation.consume")
	}
	return map[string]any{
		"patch_queue_decision_continuation": continuation,
		"continuation_task":                 status,
		"consumed":                          strings.EqualFold(strings.TrimSpace(continuation.State), "CONSUMED"),
		"created":                           created,
	}, nil
}

func (h *Handler) decodeProjectProfileGetParams(ctx context.Context, raw json.RawMessage, p *projectProfileGetParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) decodeProjectGatesStatusParams(ctx context.Context, raw json.RawMessage, p *projectGatesStatusParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) decodeProjectCoordinationGetParams(ctx context.Context, raw json.RawMessage, p *projectCoordinationGetParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) decodeProjectRolesListParams(ctx context.Context, raw json.RawMessage, p *projectRolesListParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) decodeProjectRepositoriesListParams(ctx context.Context, raw json.RawMessage, p *projectRepositoriesListParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) decodeProjectCheckoutsListParams(ctx context.Context, raw json.RawMessage, p *projectCheckoutsListParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) decodeProjectBranchesListParams(ctx context.Context, raw json.RawMessage, p *projectBranchesListParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) decodeProjectPatchQueueListParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueListParams) (string, string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return h.validateProjectRead(ctx, p.WorkspaceID, p.ProjectID)
}

func (h *Handler) validateProjectRead(ctx context.Context, workspaceID, projectID string) (string, string, *RPCError) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return "", "", rpcErr
	}
	if rpcErr := requireTrimmedParam(projectID, "project_id"); rpcErr != nil {
		return "", "", rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return "", "", rpcErr
	}
	canonicalProjectID, rpcErr := h.resolveProjectIDForRPC(ctx, workspaceID, projectID)
	if rpcErr != nil {
		return "", "", rpcErr
	}
	projectID = canonicalProjectID
	return workspaceID, projectID, nil
}

func (h *Handler) resolveProjectIDForRPC(ctx context.Context, workspaceID, projectID string) (string, *RPCError) {
	canonicalProjectID, err := h.store.ResolveProjectID(ctx, workspaceID, projectID)
	if err != nil {
		return "", &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return canonicalProjectID, nil
}

func (h *Handler) resolveProjectActorForRPC(ctx context.Context, workspaceID, projectID, actorID string, principal AuthPrincipal) (string, string, string, AuthPrincipal, *RPCError) {
	canonicalProjectID, rpcErr := h.resolveProjectIDForRPC(ctx, workspaceID, projectID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return workspaceID, canonicalProjectID, actorID, principal, nil
}

func (h *Handler) decodeProjectProfileUpdateParams(ctx context.Context, raw json.RawMessage, p *projectProfileUpdateParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPhaseTransitionParams(ctx context.Context, raw json.RawMessage, p *projectPhaseTransitionParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectLeadClaimParams(ctx context.Context, raw json.RawMessage, p *projectLeadClaimParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.AgentID), "agent_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if principal.PrincipalType == "agent" && strings.TrimSpace(p.AgentID) != actorID {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodePermissionDenied, Message: "agent principal can only claim project lead for itself"}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectLeadRenewParams(ctx context.Context, raw json.RawMessage, p *projectLeadRenewParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RoleID), "role_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectLeadReleaseParams(ctx context.Context, raw json.RawMessage, p *projectLeadReleaseParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RoleID), "role_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectLeadTransferParams(ctx context.Context, raw json.RawMessage, p *projectLeadTransferParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RoleID), "role_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ToAgentID), "to_agent_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectRoleAssignParams(ctx context.Context, raw json.RawMessage, p *projectRoleAssignParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.AgentID), "agent_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RoleType), "role_type"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	writeScopeJSON := strings.TrimSpace(p.WriteScopeJSON)
	if writeScopeJSON != "" && !json.Valid([]byte(writeScopeJSON)) {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: "write_scope_json must be valid JSON"}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectRepositoryUpsertParams(ctx context.Context, raw json.RawMessage, p *projectRepositoryUpsertParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := h.requireProjectRepositoryMutationPrincipal(ctx, workspaceID, projectID, actorID, principal); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectCheckoutRegisterParams(ctx context.Context, raw json.RawMessage, p *projectCheckoutRegisterParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RepoID), "repo_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.MachineID), "machine_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.LocalPath), "local_path"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := normalizeProjectCheckoutAgentForPrincipal(p, actorID, principal); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectBranchRegisterParams(ctx context.Context, raw json.RawMessage, p *projectBranchRegisterParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RepoID), "repo_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.BranchName), "branch_name"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := normalizeProjectBranchAgentForPrincipal(p, actorID, principal); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	writeScopeJSON := strings.TrimSpace(p.WriteScopeJSON)
	if writeScopeJSON != "" && !json.Valid([]byte(writeScopeJSON)) {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: "write_scope_json must be valid JSON"}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueSubmitParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueSubmitParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RepoID), "repo_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.BranchID), "branch_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	pathsetJSON := strings.TrimSpace(p.PathsetJSON)
	if pathsetJSON != "" && !json.Valid([]byte(pathsetJSON)) {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: "pathset_json must be valid JSON"}
	}
	baseFileHashesJSON := strings.TrimSpace(p.BaseFileHashesJSON)
	if baseFileHashesJSON != "" && !json.Valid([]byte(baseFileHashesJSON)) {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: "base_file_hashes_json must be valid JSON"}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueSupersedeParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueSupersedeParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"new_item_id", p.NewItemID},
		{"evidence_doc_key", p.EvidenceDocKey},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueClaimParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueClaimParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.QueueID), "queue_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ItemID), "item_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueOperationBindParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueOperationBindParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"claim_token", p.ClaimToken},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	if operationKind := strings.TrimSpace(p.OperationKind); operationKind != "" && operationKind != sqlite.ProjectPatchQueueOperationKindRepoPatchApply {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: "operation_kind must be repo_patch_apply when supplied"}
	}
	mutationPathsJSON := strings.TrimSpace(p.MutationPathsJSON)
	if mutationPathsJSON != "" && !json.Valid([]byte(mutationPathsJSON)) {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: "mutation_paths_json must be valid JSON"}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueCASRecordParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueCASRecordParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"claim_token", p.ClaimToken},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueMaterializationRecordParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueMaterializationRecordParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"claim_token", p.ClaimToken},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueRollbackRecordParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueRollbackRecordParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"claim_token", p.ClaimToken},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueIntegrationRecordParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueIntegrationRecordParams, defaultOutcome string) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if strings.TrimSpace(p.Outcome) == "" {
		p.Outcome = strings.TrimSpace(defaultOutcome)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"outcome", p.Outcome},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	if strings.EqualFold(strings.TrimSpace(p.Outcome), sqlite.ProjectPatchQueueIntegrationOutcomeRepair) {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RepairReason), "repair_reason"); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	} else {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(p.TargetBranch), "target_branch"); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueReviewerAdvisoryRecordParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueReviewerAdvisoryRecordParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"claim_token", p.ClaimToken},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueOperatorEnablementRecordParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueOperatorEnablementRecordParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActorIdentity(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requirePatchQueueOperatorPrincipal(actorID, principal); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	projectID, rpcErr = h.resolveProjectIDForRPC(ctx, workspaceID, projectID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"claim_token", p.ClaimToken},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeOperatorPatchQueueEnableParams(ctx context.Context, raw json.RawMessage, p *operatorPatchQueueEnableParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActorIdentity(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requirePatchQueueOperatorPrincipal(actorID, principal); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	projectID, rpcErr = h.resolveProjectIDForRPC(ctx, workspaceID, projectID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"queue_id", p.QueueID},
		{"item_id", p.ItemID},
		{"claim_token", p.ClaimToken},
	} {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(field.value), field.name); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueReleaseParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueReleaseParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.QueueID), "queue_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ItemID), "item_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ClaimToken), "claim_token"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueDecisionParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueDecisionParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.QueueID), "queue_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ItemID), "item_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.Decision), "decision"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.DecisionSummary), "decision_summary"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ClaimToken), "claim_token"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueReviewTaskReconcileParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueReviewTaskReconcileParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.QueueID), "queue_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ItemID), "item_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func (h *Handler) decodeProjectPatchQueueDecisionContinuationConsumeParams(ctx context.Context, raw json.RawMessage, p *projectPatchQueueDecisionContinuationConsumeParams) (string, string, string, AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", "", "", AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if strings.TrimSpace(p.OutboxID) == "" {
		if rpcErr := requireTrimmedParam(strings.TrimSpace(p.QueueID), "queue_id"); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
		if rpcErr := requireTrimmedParam(strings.TrimSpace(p.ItemID), "item_id"); rpcErr != nil {
			return "", "", "", AuthPrincipal{}, rpcErr
		}
	}
	return h.resolveProjectActorForRPC(ctx, workspaceID, projectID, actorID, principal)
}

func requirePatchQueueOperatorPrincipal(actorID string, principal AuthPrincipal) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "human") {
		return &RPCError{Code: errCodePermissionDenied, Message: "human operator principal required for patch queue enablement"}
	}
	operators := parsePatchQueueOperatorIDs(os.Getenv("RHIZOME_OPERATOR_IDS"))
	if len(operators) == 0 {
		return &RPCError{Code: errCodePermissionDenied, Message: "RHIZOME_OPERATOR_IDS is required for patch queue operator enablement"}
	}
	if _, ok := operators[strings.TrimSpace(actorID)]; !ok {
		return &RPCError{Code: errCodePermissionDenied, Message: "actor is not in RHIZOME_OPERATOR_IDS"}
	}
	return nil
}

func parsePatchQueueOperatorIDs(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

func (h *Handler) requireProjectRepositoryMutationPrincipal(ctx context.Context, workspaceID, projectID, actorID string, principal AuthPrincipal) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	lead, ok, err := h.store.GetActiveProjectStrategicLead(ctx, workspaceID, projectID)
	if err != nil {
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if !ok || strings.TrimSpace(lead.AgentID) != strings.TrimSpace(actorID) {
		return &RPCError{Code: errCodePermissionDenied, Message: "project repository mutation requires active strategic lead"}
	}
	return nil
}

func normalizeProjectCheckoutAgentForPrincipal(p *projectCheckoutRegisterParams, actorID string, principal AuthPrincipal) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	actorID = strings.TrimSpace(actorID)
	agentID := strings.TrimSpace(p.AgentID)
	if agentID == "" {
		p.AgentID = actorID
		return nil
	}
	if agentID != actorID {
		return &RPCError{Code: errCodePermissionDenied, Message: "agent principals may only register their own project checkout"}
	}
	p.AgentID = agentID
	return nil
}

func normalizeProjectBranchAgentForPrincipal(p *projectBranchRegisterParams, actorID string, principal AuthPrincipal) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	actorID = strings.TrimSpace(actorID)
	agentID := strings.TrimSpace(p.AgentID)
	if agentID == "" {
		p.AgentID = actorID
		return nil
	}
	if agentID != actorID && !(strings.TrimSpace(p.BranchID) != "" && strings.ToUpper(strings.TrimSpace(p.Status)) == sqlite.ProjectBranchStatusMerged) {
		return &RPCError{Code: errCodePermissionDenied, Message: "agent principals may only register their own project branch"}
	}
	p.AgentID = agentID
	return nil
}

func (h *Handler) validateProjectActor(ctx context.Context, workspaceID, projectID, actorID string) (string, string, string, AuthPrincipal, *RPCError) {
	return h.validateProjectActorIdentity(ctx, workspaceID, projectID, actorID)
}

func (h *Handler) validateProjectActorIdentity(ctx context.Context, workspaceID, projectID, actorID string) (string, string, string, AuthPrincipal, *RPCError) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	actorID = strings.TrimSpace(actorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(projectID, "project_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return "", "", "", AuthPrincipal{}, rpcErr
	}
	return workspaceID, projectID, actorID, principal, nil
}

func projectProfileUpdateHasChanges(p projectProfileUpdateParams) bool {
	return p.Goal != nil ||
		p.DesignDocID != nil ||
		p.ImplementationPlanDocID != nil ||
		p.RepoRequired != nil ||
		p.RepoStatus != nil ||
		p.RepoURL != nil ||
		p.RepoDefaultBranch != nil
}
