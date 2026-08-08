package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceEventsListParams struct {
	WorkspaceID string `json:"workspace_id"`
	EventType   string `json:"event_type,omitempty"`
	EntityType  string `json:"entity_type,omitempty"`
	EntityID    string `json:"entity_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceEventsReplayParams struct {
	WorkspaceID   string `json:"workspace_id"`
	AgentID       string `json:"agent_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	TaskID        string `json:"task_id,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	IncludeEvents bool   `json:"include_events,omitempty"`
}

type workspaceAuthorityStatusParams struct {
	WorkspaceID string `json:"workspace_id"`
	Scope       string `json:"scope,omitempty"`
}

type workspaceAuthorityEnsureLocalParams struct {
	WorkspaceID string `json:"workspace_id"`
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
}

type workspaceAuthorityForceBreakParams struct {
	WorkspaceID string `json:"workspace_id"`
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
}

type workspaceOpsUpsertParams struct {
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

type workspaceOpsRequestParams struct {
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

type workspaceOpsListParams struct {
	WorkspaceID string `json:"workspace_id"`
	QueueType   string `json:"queue_type,omitempty"`
	Status      string `json:"status,omitempty"`
	AssignedTo  string `json:"assigned_to,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceOpsGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	QueueID     string `json:"queue_id,omitempty"`
	QueueKey    string `json:"queue_key,omitempty"`
}

type workspaceOpsResolveParams struct {
	WorkspaceID      string `json:"workspace_id"`
	QueueID          string `json:"queue_id,omitempty"`
	QueueKey         string `json:"queue_key,omitempty"`
	Status           string `json:"status,omitempty"`
	ResolvedBy       string `json:"resolved_by"`
	Resolution       string `json:"resolution,omitempty"`
	CurrentRevision  int64  `json:"current_revision,omitempty"`
	CurrentUpdatedAt string `json:"current_updated_at,omitempty"`
}

type workspaceOpsEscalateParams struct {
	WorkspaceID      string `json:"workspace_id"`
	QueueID          string `json:"queue_id,omitempty"`
	QueueKey         string `json:"queue_key,omitempty"`
	EscalatedBy      string `json:"escalated_by"`
	Reason           string `json:"reason,omitempty"`
	AssignedTo       string `json:"assigned_to,omitempty"`
	Urgency          string `json:"urgency,omitempty"`
	DueAt            string `json:"due_at,omitempty"`
	CurrentRevision  int64  `json:"current_revision,omitempty"`
	CurrentUpdatedAt string `json:"current_updated_at,omitempty"`
}

type workspaceClaimWriteParams struct {
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

type workspaceClaimListParams struct {
	WorkspaceID     string `json:"workspace_id"`
	ClaimType       string `json:"claim_type,omitempty"`
	Status          string `json:"status,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	MemoryID        string `json:"memory_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

type workspaceClaimLinksListParams struct {
	WorkspaceID  string `json:"workspace_id"`
	ClaimID      string `json:"claim_id,omitempty"`
	FromClaimID  string `json:"from_claim_id,omitempty"`
	ToClaimID    string `json:"to_claim_id,omitempty"`
	RelationType string `json:"relation_type,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

type workspaceClaimSearchParams struct {
	WorkspaceID     string `json:"workspace_id"`
	Query           string `json:"query"`
	ClaimType       string `json:"claim_type,omitempty"`
	Status          string `json:"status,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	MemoryID        string `json:"memory_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

type workspaceClaimArchiveParams struct {
	WorkspaceID string `json:"workspace_id"`
	ClaimID     string `json:"claim_id"`
	ArchivedBy  string `json:"archived_by"`
	Reason      string `json:"reason,omitempty"`
}

type workspaceClaimLifecycleParams struct {
	WorkspaceID        string `json:"workspace_id"`
	ClaimID            string `json:"claim_id"`
	ActorID            string `json:"actor_id"`
	Reason             string `json:"reason,omitempty"`
	DueAt              string `json:"due_at,omitempty"`
	AssignedTo         string `json:"assigned_to,omitempty"`
	Urgency            string `json:"urgency,omitempty"`
	SupersedingClaimID string `json:"superseding_claim_id,omitempty"`
	ConflictsClaimID   string `json:"conflicts_claim_id,omitempty"`
}

type workspaceExecutionRunWriteParams struct {
	WorkspaceID  string         `json:"workspace_id"`
	RunID        string         `json:"run_id,omitempty"`
	TaskID       string         `json:"task_id,omitempty"`
	SessionID    string         `json:"session_id,omitempty"`
	AgentID      string         `json:"agent_id,omitempty"`
	Title        string         `json:"title"`
	Summary      string         `json:"summary,omitempty"`
	Status       string         `json:"status,omitempty"`
	Outcome      string         `json:"outcome,omitempty"`
	Verification map[string]any `json:"verification,omitempty"`
}

type workspaceExecutionRunListParams struct {
	WorkspaceID string `json:"workspace_id"`
	Status      string `json:"status,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceExecutionAgentRunsCancelParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Summary     string `json:"summary,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
}

type workspaceExecutionRunGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	RunID       string `json:"run_id"`
}

type workspaceExecutionStepWriteParams struct {
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

type workspacePolicyPutParams struct {
	WorkspaceID string `json:"workspace_id"`
	PolicyID    string `json:"policy_id,omitempty"`
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id,omitempty"`
	Capability  string `json:"capability,omitempty"`
	ToolID      string `json:"tool_id,omitempty"`
	Effect      string `json:"effect,omitempty"`
	Reason      string `json:"reason,omitempty"`
	CreatedBy   string `json:"created_by"`
}

type workspacePolicyListParams struct {
	WorkspaceID string `json:"workspace_id"`
	SubjectType string `json:"subject_type,omitempty"`
	SubjectID   string `json:"subject_id,omitempty"`
	Capability  string `json:"capability,omitempty"`
	ToolID      string `json:"tool_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspacePolicyCheckParams struct {
	WorkspaceID string `json:"workspace_id"`
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	Capability  string `json:"capability"`
	ToolID      string `json:"tool_id,omitempty"`
}

type workspaceControlCommandRequestParams struct {
	WorkspaceID    string   `json:"workspace_id"`
	CommandID      string   `json:"command_id,omitempty"`
	CommandType    string   `json:"command_type"`
	Scope          string   `json:"scope,omitempty"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	TensionID      string   `json:"tension_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	TargetMode     string   `json:"target_mode,omitempty"`
	TTLSeconds     int      `json:"ttl_seconds,omitempty"`
	Reason         string   `json:"reason,omitempty"`
	RequestedBy    string   `json:"requested_by"`
	ActorType      string   `json:"actor_type,omitempty"`
	ParentRefs     []string `json:"parent_refs,omitempty"`
}

func (h *Handler) workspaceEventsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceEventsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: p.WorkspaceID,
		EventType:   p.EventType,
		EntityType:  p.EntityType,
		EntityID:    p.EntityID,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		TaskID:      p.TaskID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.execution.run.write")
	}
	authority, rpcErr := h.loadWorkspaceTimeAuthority(ctx, p.WorkspaceID, "workspace.events.list")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(p.WorkspaceID),
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceEventsReplay(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceEventsReplayParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		TaskID:      p.TaskID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.execution.run.write")
	}
	if !p.IncludeEvents {
		report.Events = nil
	}
	return map[string]any{
		"workspace_id": report.WorkspaceID,
		"report":       report,
	}, nil
}

func (h *Handler) workspaceEventsEvaluate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceEventsReplayParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		TaskID:      p.TaskID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.execution.run.write")
	}
	return map[string]any{
		"workspace_id":            report.WorkspaceID,
		"time_authority":          report.TimeAuthority,
		"truncated":               report.Truncated,
		"window_incomplete":       report.WindowIncomplete,
		"filter":                  report.Filter,
		"scope":                   report.Scope,
		"replica_coverage":        report.ReplicaCoverage,
		"replica_freshness":       report.ReplicaFreshness,
		"local_replica_freshness": report.LocalReplicaFreshness,
		"missing_parent_refs":     report.MissingParentRefs,
		"metrics":                 report.Metrics,
		"evaluation":              report.Evaluation,
		"counts": map[string]int{
			"replicas":       len(report.ReplicaFreshness),
			"sessions":       len(report.Sessions),
			"queues":         len(report.Queues),
			"claims":         len(report.Claims),
			"execution_runs": len(report.ExecutionRuns),
			"events":         len(report.Events),
		},
	}, nil
}

func (h *Handler) workspaceAuthorityStatus(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAuthorityStatusParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	status, err := h.store.GetLocalWorkspaceAuthorityStatus(ctx, p.WorkspaceID, p.Scope)
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "workspace.authority.status")
	}
	return status, nil
}

func (h *Handler) workspaceAuthorityEnsureLocal(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAuthorityEnsureLocalParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: p.WorkspaceID,
		Scope:       p.Scope,
		ActorType:   p.ActorType,
		ActorID:     p.ActorID,
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "workspace.authority.ensure_local")
	}
	return result, nil
}

func (h *Handler) workspaceAuthorityForceBreak(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAuthorityForceBreakParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.ForceBreakWorkspaceAuthority(ctx, sqlite.ForceBreakWorkspaceAuthorityInput{
		WorkspaceID: p.WorkspaceID,
		Scope:       p.Scope,
		ActorType:   p.ActorType,
		ActorID:     p.ActorID,
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "workspace.authority.force_break")
	}
	return result, nil
}

func (h *Handler) workspaceOpsUpsert(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceOpsUpsertParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && strings.TrimSpace(p.AgentID) != "" && strings.TrimSpace(p.AgentID) != strings.TrimSpace(principal.PrincipalID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && strings.EqualFold(strings.TrimSpace(p.SourceKind), "agent") && strings.TrimSpace(p.SourceID) != "" && strings.TrimSpace(p.SourceID) != strings.TrimSpace(principal.PrincipalID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "source scope mismatch: token identity does not match source_id"}
	}
	if rpcErr := requireTrimmedParam(p.QueueKey, "queue_key"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.rejectWorkflowQueueUpsertWorkflowMutation(
		ctx,
		strings.TrimSpace(p.WorkspaceID),
		strings.TrimSpace(p.QueueID),
		strings.TrimSpace(p.QueueKey),
		strings.TrimSpace(p.AssignedTo),
		strings.TrimSpace(p.SourceKind),
		strings.TrimSpace(p.SourceID),
		strings.TrimSpace(p.TaskID),
		strings.TrimSpace(p.AgentID),
	); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireOperatorQueueBaseVersionForAdvancedRevision(ctx, strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(p.QueueID), strings.TrimSpace(p.QueueKey), p.CurrentRevision, strings.TrimSpace(p.CurrentUpdatedAt), "workspace.ops.upsert"); rpcErr != nil {
		return nil, rpcErr
	}
	keepAlive := true
	if p.KeepSessionActive != nil {
		keepAlive = *p.KeepSessionActive
	}
	if h.beforeWorkspaceOpsUpsertStoreOverride != nil {
		h.beforeWorkspaceOpsUpsertStoreOverride(ctx)
	}
	record, event, err := h.store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:                 p.QueueID,
		WorkspaceID:             p.WorkspaceID,
		QueueKey:                p.QueueKey,
		QueueType:               p.QueueType,
		Title:                   p.Title,
		Summary:                 p.Summary,
		Details:                 p.Details,
		AssignedTo:              p.AssignedTo,
		Urgency:                 p.Urgency,
		SourceKind:              p.SourceKind,
		SourceID:                p.SourceID,
		TaskID:                  p.TaskID,
		SessionID:               p.SessionID,
		AgentID:                 p.AgentID,
		KeepSessionActive:       keepAlive,
		DueAt:                   p.DueAt,
		RequireCurrentRevision:  p.CurrentRevision,
		RequireCurrentUpdatedAt: strings.TrimSpace(p.CurrentUpdatedAt),
		PromptContextEnvelope:   h.operatorQueuePromptContextEnvelope(ctx, p.WorkspaceID, "workspace.ops.upsert"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.ops.upsert"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishOperatorQueueEventRecord(event, "workspace.ops.updated", record)
	return map[string]any{"item": record, "status": "UPSERTED"}, nil
}

func describeWorkflowOwnedQueue(queue sqlite.OperatorQueueRecord) string {
	if strings.EqualFold(strings.TrimSpace(queue.SourceKind), "human_action") {
		return "human action queue"
	}
	if rollbackPayload, rollbackErr := actionCreateDecodeRollbackFailurePayload(queue.PayloadJSON); rollbackErr == nil && rollbackPayload.IsRollbackFailure(queue.QueueKey) {
		return "rollback-failure follow-up"
	}
	if payload, payloadErr := actionCreateDecodeQueuePayload(queue.PayloadJSON); payloadErr == nil && payload.IsRebaseFollowup(queue.QueueKey) {
		return "linked rebase follow-up"
	}
	return ""
}

func isOperatorQueueItemNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sqlite.ErrOperatorQueueItemNotFound) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "operator queue item not found")
}

func (h *Handler) lookupOperatorQueueMutationTargets(ctx context.Context, workspaceID, queueID, queueKey string) (*sqlite.OperatorQueueRecord, *sqlite.OperatorQueueRecord, *RPCError) {
	if h == nil {
		return nil, nil, nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	queueID = strings.TrimSpace(queueID)
	queueKey = strings.TrimSpace(queueKey)
	var byID *sqlite.OperatorQueueRecord
	if queueID != "" {
		record, err := h.store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
		if err != nil {
			if !isOperatorQueueItemNotFoundErr(err) {
				return nil, nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
			}
		} else {
			recordCopy := record
			byID = &recordCopy
		}
	}
	var byKey *sqlite.OperatorQueueRecord
	if queueKey != "" {
		record, err := h.store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey)
		if err != nil {
			if !isOperatorQueueItemNotFoundErr(err) {
				return nil, nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
			}
		} else {
			recordCopy := record
			byKey = &recordCopy
		}
	}
	return byID, byKey, nil
}

func (h *Handler) requireOperatorQueueBaseVersionForAdvancedRevision(ctx context.Context, workspaceID, queueID, queueKey string, currentRevision int64, currentUpdatedAt, method string) *RPCError {
	if h == nil {
		return nil
	}
	queueID = strings.TrimSpace(queueID)
	queueKey = strings.TrimSpace(queueKey)
	byID, byKey, rpcErr := h.lookupOperatorQueueMutationTargets(ctx, workspaceID, queueID, queueKey)
	if rpcErr != nil {
		return rpcErr
	}
	if byID != nil && queueKey != "" && byID.QueueKey != queueKey {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: fmt.Sprintf("%s requires queue_id and queue_key to refer to the same queue item", method),
		}
	}
	if byKey != nil && queueID != "" && byKey.QueueID != queueID {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: fmt.Sprintf("%s requires queue_id and queue_key to refer to the same queue item", method),
		}
	}
	if currentRevision > 0 || strings.TrimSpace(currentUpdatedAt) != "" {
		return nil
	}
	queue := byID
	if queue == nil {
		queue = byKey
	}
	if queue == nil {
		return nil
	}
	if queue.Revision <= 1 {
		return nil
	}
	return &RPCError{
		Code:    errCodeInvalidParams,
		Message: fmt.Sprintf("%s requires current_revision (preferred) or current_updated_at once queue revision has advanced beyond its initial create", method),
	}
}

func (h *Handler) rejectWorkflowQueueUpsertWorkflowMutation(ctx context.Context, workspaceID, queueID, queueKey, requestedAssignedTo, requestedSourceKind, requestedSourceID, requestedTaskID, requestedAgentID string) *RPCError {
	if h == nil {
		return nil
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, queueID, queueKey)
	if err != nil {
		if isOperatorQueueItemNotFoundErr(err) {
			return nil
		}
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	workflowLabel := describeWorkflowOwnedQueue(queue)
	if workflowLabel == "" {
		return nil
	}

	holder := strings.TrimSpace(queue.AssignedTo)
	if holder != strings.TrimSpace(requestedAssignedTo) {
		message := fmt.Sprintf("%s ownership is workflow-managed; use workspace.ops.escalate", workflowLabel)
		if holder != "" {
			message = fmt.Sprintf("%s is assigned to %s; use workspace.ops.escalate", workflowLabel, holder)
		}
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: message,
		}
	}

	if strings.TrimSpace(queue.SourceKind) != strings.TrimSpace(requestedSourceKind) || strings.TrimSpace(queue.SourceID) != strings.TrimSpace(requestedSourceID) {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: fmt.Sprintf("%s source identity is workflow-managed and cannot be changed via workspace.ops.upsert", workflowLabel),
		}
	}

	if strings.TrimSpace(queue.TaskID) != strings.TrimSpace(requestedTaskID) || strings.TrimSpace(queue.AgentID) != strings.TrimSpace(requestedAgentID) {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: fmt.Sprintf("%s task/agent context is workflow-managed and cannot be changed via workspace.ops.upsert", workflowLabel),
		}
	}

	return nil
}

func requirePolicySubjectPrincipal(ctx context.Context, workspaceID, subjectType, subjectID string) *RPCError {
	principal, rpcErr := requireWorkspacePrincipal(ctx, workspaceID)
	if rpcErr != nil {
		return rpcErr
	}
	return requirePolicySubjectMatchesPrincipal(principal, subjectType, subjectID)
}

func requirePolicySubjectMatchesPrincipal(principal AuthPrincipal, subjectType, subjectID string) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(subjectType), "agent") || strings.TrimSpace(subjectID) != strings.TrimSpace(principal.PrincipalID) {
		return &RPCError{Code: errCodePermissionDenied, Message: "subject scope mismatch: token identity does not match subject_id"}
	}
	return nil
}

func (h *Handler) workspaceOpsRequest(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceOpsRequestParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && strings.TrimSpace(p.AgentID) != "" && strings.TrimSpace(p.AgentID) != strings.TrimSpace(principal.PrincipalID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && strings.EqualFold(strings.TrimSpace(p.SourceKind), "agent") && strings.TrimSpace(p.SourceID) != "" && strings.TrimSpace(p.SourceID) != strings.TrimSpace(principal.PrincipalID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "source scope mismatch: token identity does not match source_id"}
	}
	if rpcErr := requireTrimmedParam(p.RequestKey, "request_key"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.GateType, "gate_type"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	gateType, queueType, ok := normalizeExternalGateType(p.GateType)
	if !ok {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "gate_type must be one of CREDENTIAL_AUTH, PAYMENT_BILLING, or EXPLICIT_APPROVAL"}
	}
	keepAlive := true
	if p.KeepSessionActive != nil {
		keepAlive = *p.KeepSessionActive
	}
	urgency := strings.TrimSpace(p.Urgency)
	if urgency == "" {
		urgency = defaultExternalGateUrgency(gateType)
	}
	sourceKind := firstNonEmpty(strings.TrimSpace(p.SourceKind), "external_gate")
	sourceID := firstNonEmpty(strings.TrimSpace(p.SourceID), strings.TrimSpace(p.RequestKey))
	queueKey := externalGateQueueKey(gateType, p.RequestKey)
	payload := map[string]any{
		"request_key":         strings.TrimSpace(p.RequestKey),
		"gate_type":           gateType,
		"queue_key":           queueKey,
		"queue_type":          queueType,
		"title":               strings.TrimSpace(p.Title),
		"summary":             strings.TrimSpace(p.Summary),
		"details":             strings.TrimSpace(p.Details),
		"assigned_to":         strings.TrimSpace(p.AssignedTo),
		"urgency":             urgency,
		"source_kind":         sourceKind,
		"source_id":           sourceID,
		"task_id":             strings.TrimSpace(p.TaskID),
		"session_id":          strings.TrimSpace(p.SessionID),
		"agent_id":            strings.TrimSpace(p.AgentID),
		"keep_session_active": keepAlive,
		"due_at":              strings.TrimSpace(p.DueAt),
	}
	if rpcErr := h.requireOperatorQueueBaseVersionForAdvancedRevision(ctx, strings.TrimSpace(p.WorkspaceID), "", queueKey, p.CurrentRevision, strings.TrimSpace(p.CurrentUpdatedAt), "workspace.ops.request"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             p.WorkspaceID,
		QueueKey:                queueKey,
		QueueType:               queueType,
		Title:                   p.Title,
		Summary:                 p.Summary,
		Details:                 p.Details,
		PayloadJSON:             string(mustJSON(payload)),
		AssignedTo:              p.AssignedTo,
		Urgency:                 urgency,
		SourceKind:              sourceKind,
		SourceID:                sourceID,
		TaskID:                  p.TaskID,
		SessionID:               p.SessionID,
		AgentID:                 p.AgentID,
		KeepSessionActive:       keepAlive,
		DueAt:                   p.DueAt,
		RequireCurrentRevision:  p.CurrentRevision,
		RequireCurrentUpdatedAt: strings.TrimSpace(p.CurrentUpdatedAt),
		PromptContextEnvelope:   h.operatorQueuePromptContextEnvelope(ctx, p.WorkspaceID, "workspace.ops.request"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.ops.request"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishOperatorQueueEventRecord(event, "workspace.ops.updated", record)
	return map[string]any{
		"item": record,
		"request": map[string]any{
			"workspace_id": record.WorkspaceID,
			"request_key":  strings.TrimSpace(p.RequestKey),
			"gate_type":    gateType,
			"queue_key":    queueKey,
			"queue_type":   queueType,
		},
		"status": "REQUESTED",
	}, nil
}

func (h *Handler) workspaceOpsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceOpsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: p.WorkspaceID,
		QueueType:   p.QueueType,
		Status:      p.Status,
		AssignedTo:  p.AssignedTo,
		SessionID:   p.SessionID,
		TaskID:      p.TaskID,
		AgentID:     p.AgentID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.execution.run.write")
	}
	authority, rpcErr := h.loadWorkspaceTimeAuthority(ctx, p.WorkspaceID, "workspace.ops.list")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(p.WorkspaceID),
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceOpsGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceOpsGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.QueueID) == "" && strings.TrimSpace(p.QueueKey) == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "queue_id or queue_key is required"}
	}
	item, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(p.QueueID), strings.TrimSpace(p.QueueKey))
	if err != nil {
		if isOperatorQueueItemNotFoundErr(err) {
			return nil, &RPCError{Code: errCodeOperatorQueueNotFound, Message: err.Error()}
		}
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"item": item,
	}, nil
}

func (h *Handler) workspaceOpsResolve(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceOpsResolveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.QueueID) == "" && strings.TrimSpace(p.QueueKey) == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "queue_id or queue_key is required"}
	}
	if rpcErr := requireTrimmedParam(p.ResolvedBy, "resolved_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ResolvedBy, "resolved_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.rejectPendingActionQueueResolve(ctx, strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(p.QueueID), strings.TrimSpace(p.QueueKey)); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireOperatorQueueBaseVersionForAdvancedRevision(ctx, strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(p.QueueID), strings.TrimSpace(p.QueueKey), p.CurrentRevision, strings.TrimSpace(p.CurrentUpdatedAt), "workspace.ops.resolve"); rpcErr != nil {
		return nil, rpcErr
	}
	status := strings.TrimSpace(p.Status)
	if status == "" {
		status = "RESOLVED"
	}
	if h.beforeWorkspaceOpsResolveStoreOverride != nil {
		h.beforeWorkspaceOpsResolveStoreOverride(ctx)
	}
	record, event, err := h.store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID:             p.WorkspaceID,
		QueueID:                 p.QueueID,
		QueueKey:                p.QueueKey,
		Status:                  status,
		ResolvedBy:              p.ResolvedBy,
		Resolution:              p.Resolution,
		RequireCurrentRevision:  p.CurrentRevision,
		RequireCurrentUpdatedAt: strings.TrimSpace(p.CurrentUpdatedAt),
		PromptContextEnvelope:   h.operatorQueuePromptContextEnvelope(ctx, p.WorkspaceID, "workspace.ops.resolve"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.ops.resolve"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if event.EventID != "" {
		liveType := operatorQueueRuntimeEventLiveType(event.EventType)
		if liveType == "" {
			liveType = "workspace.ops.resolved"
		}
		h.publishOperatorQueueEventRecord(event, liveType, record)
	}
	return map[string]any{"item": record, "status": record.Status}, nil
}

func (h *Handler) rejectPendingActionQueueResolve(ctx context.Context, workspaceID, queueID, queueKey string) *RPCError {
	if h == nil {
		return nil
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, queueID, queueKey)
	if err != nil {
		if isOperatorQueueItemNotFoundErr(err) {
			return nil
		}
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	linkedActionID := ""
	actionQueue := false
	if strings.EqualFold(strings.TrimSpace(queue.SourceKind), "human_action") {
		linkedActionID = strings.TrimSpace(queue.SourceID)
		actionQueue = linkedActionID != ""
	}
	if rollbackPayload, rollbackErr := actionCreateDecodeRollbackFailurePayload(queue.PayloadJSON); rollbackErr == nil && rollbackPayload.IsRollbackFailure(queue.QueueKey) {
		linkedActionID = strings.TrimSpace(rollbackPayload.FollowupActionID)
	} else if payload, payloadErr := actionCreateDecodeQueuePayload(queue.PayloadJSON); payloadErr == nil && payload.IsRebaseFollowup(queue.QueueKey) {
		linkedActionID = strings.TrimSpace(payload.ActionID)
	}
	if linkedActionID == "" {
		return nil
	}

	action, err := h.store.GetHumanAction(ctx, linkedActionID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "action not found") {
			return nil
		}
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if strings.ToUpper(strings.TrimSpace(action.Status)) != humanActionStatusPending {
		return nil
	}
	return &RPCError{
		Code: errCodeInvalidParams,
		Message: func() string {
			if actionQueue {
				return fmt.Sprintf("action queue is linked to pending action %s; use action.resolve", strings.TrimSpace(action.ActionID))
			}
			return fmt.Sprintf("source queue is linked to pending action %s; use action.resolve or workspace.ops.escalate", strings.TrimSpace(action.ActionID))
		}(),
	}
}

func (h *Handler) rejectLinkedActionQueueEscalate(ctx context.Context, workspaceID, queueID, queueKey string) *RPCError {
	if h == nil {
		return nil
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, queueID, queueKey)
	if err != nil {
		if isOperatorQueueItemNotFoundErr(err) {
			return nil
		}
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if !strings.EqualFold(strings.TrimSpace(queue.SourceKind), "human_action") {
		return nil
	}
	payload, err := actionCreateDecodeQueuePayload(queue.PayloadJSON)
	if err != nil {
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if strings.TrimSpace(payload.SourceQueueID) == "" && strings.TrimSpace(payload.SourceQueueKey) == "" {
		return nil
	}
	return &RPCError{
		Code:    errCodeInvalidParams,
		Message: fmt.Sprintf("action queue is linked to source queue %s; use workspace.ops.escalate on the source queue", firstNonEmpty(strings.TrimSpace(payload.SourceQueueID), strings.TrimSpace(payload.SourceQueueKey))),
	}
}

func (h *Handler) workspaceOpsEscalate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceOpsEscalateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.QueueID) == "" && strings.TrimSpace(p.QueueKey) == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "queue_id or queue_key is required"}
	}
	if rpcErr := requireTrimmedParam(p.EscalatedBy, "escalated_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.EscalatedBy, "escalated_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.rejectLinkedActionQueueEscalate(ctx, strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(p.QueueID), strings.TrimSpace(p.QueueKey)); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireOperatorQueueBaseVersionForAdvancedRevision(ctx, strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(p.QueueID), strings.TrimSpace(p.QueueKey), p.CurrentRevision, strings.TrimSpace(p.CurrentUpdatedAt), "workspace.ops.escalate"); rpcErr != nil {
		return nil, rpcErr
	}
	if h.beforeWorkspaceOpsEscalateStoreOverride != nil {
		h.beforeWorkspaceOpsEscalateStoreOverride(ctx)
	}
	record, event, linkedActionQueueEvent, err := h.store.EscalateOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID:             p.WorkspaceID,
		QueueID:                 p.QueueID,
		QueueKey:                p.QueueKey,
		EscalatedBy:             p.EscalatedBy,
		Reason:                  p.Reason,
		AssignedTo:              p.AssignedTo,
		Urgency:                 p.Urgency,
		DueAt:                   p.DueAt,
		RequireCurrentRevision:  p.CurrentRevision,
		RequireCurrentUpdatedAt: strings.TrimSpace(p.CurrentUpdatedAt),
		PromptContextEnvelope:   h.operatorQueuePromptContextEnvelope(ctx, p.WorkspaceID, "workspace.ops.escalate"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.ops.escalate")
	}
	if event.EventID != "" {
		h.publishOperatorQueueEventRecord(event, "workspace.ops.escalated", record)
	}
	if linkedActionQueueEvent != nil && strings.TrimSpace(linkedActionQueueEvent.Event.EventID) != "" {
		h.publishOperatorQueueEventRecord(linkedActionQueueEvent.Event, "workspace.ops.updated", linkedActionQueueEvent.Record)
	}
	return map[string]any{"item": record, "status": record.Status}, nil
}

func (h *Handler) workspaceClaimWrite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceClaimWriteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, principalErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if principalErr != nil {
		return nil, principalErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		if strings.TrimSpace(p.AgentID) != "" && strings.TrimSpace(p.AgentID) != strings.TrimSpace(principal.PrincipalID) {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
		}
		if strings.EqualFold(strings.TrimSpace(p.SourceKind), "agent") && strings.TrimSpace(p.SourceID) != "" && strings.TrimSpace(p.SourceID) != strings.TrimSpace(principal.PrincipalID) {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "source scope mismatch: token identity does not match source_id"}
		}
	}
	if rpcErr := requireTrimmedParam(p.Subject, "subject"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Body, "body"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, invalidationEvents, err := h.store.RecordKnowledgeClaimWithAuthorityEffects(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:               p.ClaimID,
		WorkspaceID:           p.WorkspaceID,
		ClaimType:             p.ClaimType,
		Status:                p.Status,
		Subject:               p.Subject,
		Body:                  p.Body,
		Summary:               p.Summary,
		Confidence:            p.Confidence,
		SourceKind:            p.SourceKind,
		SourceID:              p.SourceID,
		MemoryID:              p.MemoryID,
		TaskID:                p.TaskID,
		SessionID:             p.SessionID,
		AgentID:               p.AgentID,
		SupersedesClaimID:     p.SupersedesClaimID,
		ConflictsClaimID:      p.ConflictsClaimID,
		Evidence:              p.Evidence,
		Tags:                  p.Tags,
		PromptContextEnvelope: h.knowledgeClaimPromptContextEnvelope(ctx, p.WorkspaceID, "workspace.claim.write"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.claim.write")
	}
	if strings.TrimSpace(record.AgentID) != "" {
		h.touchAgentActivity(ctx, record.WorkspaceID, record.AgentID)
	}
	actions := []runtimeEventPublishAction{
		{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishKnowledgeClaimEventRecord(runtimeEvent, "workspace.claim.written", record)
			},
		},
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, record.Subject, record.ClaimID)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)
	return map[string]any{"claim": record, "status": "RECORDED"}, nil
}

func (h *Handler) workspaceClaimList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceClaimListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID:     p.WorkspaceID,
		ClaimType:       p.ClaimType,
		Status:          p.Status,
		AgentID:         p.AgentID,
		SessionID:       p.SessionID,
		TaskID:          p.TaskID,
		MemoryID:        p.MemoryID,
		SourceKind:      p.SourceKind,
		IncludeArchived: p.IncludeArchived,
		Limit:           p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, rpcErr := h.loadWorkspaceTimeAuthority(ctx, p.WorkspaceID, "workspace.claim.list")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(p.WorkspaceID),
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceClaimLinksList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceClaimLinksListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListKnowledgeClaimRelations(ctx, sqlite.KnowledgeClaimRelationFilter{
		WorkspaceID:  p.WorkspaceID,
		ClaimID:      p.ClaimID,
		FromClaimID:  p.FromClaimID,
		ToClaimID:    p.ToClaimID,
		RelationType: p.RelationType,
		Limit:        p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, rpcErr := h.loadWorkspaceTimeAuthority(ctx, p.WorkspaceID, "workspace.claim.links.list")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(p.WorkspaceID),
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceClaimSearch(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceClaimSearchParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Query, "query"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.SearchKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID:     p.WorkspaceID,
		Query:           p.Query,
		ClaimType:       p.ClaimType,
		Status:          p.Status,
		AgentID:         p.AgentID,
		SessionID:       p.SessionID,
		TaskID:          p.TaskID,
		MemoryID:        p.MemoryID,
		SourceKind:      p.SourceKind,
		IncludeArchived: p.IncludeArchived,
		Limit:           p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": strings.TrimSpace(p.WorkspaceID),
		"query":        strings.TrimSpace(p.Query),
		"items":        items,
		"count":        len(items),
	}, nil
}

func (h *Handler) workspaceClaimArchive(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceClaimArchiveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ClaimID, "claim_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ArchivedBy, "archived_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ArchivedBy, "archived_by"); rpcErr != nil {
		return nil, rpcErr
	}
	record, queueRecord, event, queueEvent, invalidationEvents, err := h.store.ArchiveKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID:           p.WorkspaceID,
		ClaimID:               p.ClaimID,
		ArchivedBy:            p.ArchivedBy,
		Reason:                p.Reason,
		PromptContextEnvelope: h.knowledgeClaimPromptContextEnvelope(ctx, p.WorkspaceID, "workspace.claim.archive"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.claim.archive")
	}
	actions := []runtimeEventPublishAction{
		{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishKnowledgeClaimEventRecord(runtimeEvent, "workspace.claim.archived", record)
			},
		},
	}
	if queueEvent.EventID != "" {
		queueLiveType := operatorQueueRuntimeEventLiveType(queueEvent.EventType)
		if queueLiveType == "" {
			queueLiveType = "workspace.ops.updated"
		}
		actions = append(actions, runtimeEventPublishAction{
			Event: queueEvent,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishOperatorQueueEventRecord(runtimeEvent, queueLiveType, queueRecord)
			},
		})
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, record.Subject, record.ClaimID)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)
	return map[string]any{"claim": record, "status": "ARCHIVED"}, nil
}

func (h *Handler) workspaceClaimReview(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceClaimLifecycle(ctx, raw, "REVIEW")
}

func (h *Handler) workspaceClaimConfirm(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceClaimLifecycle(ctx, raw, "CONFIRMED")
}

func (h *Handler) workspaceClaimDispute(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceClaimLifecycle(ctx, raw, "DISPUTED")
}

func (h *Handler) workspaceClaimSupersede(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceClaimLifecycle(ctx, raw, "SUPERSEDED")
}

func (h *Handler) workspaceClaimStale(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceClaimLifecycle(ctx, raw, "STALE")
}

func (h *Handler) workspaceClaimEscalate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceClaimLifecycleParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ClaimID, "claim_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	record, claimEvent, queueEvent, invalidationEvents, err := h.store.EscalateKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           p.WorkspaceID,
		ClaimID:               p.ClaimID,
		ActorID:               p.ActorID,
		Reason:                p.Reason,
		ReviewDueAt:           p.DueAt,
		AssignedTo:            p.AssignedTo,
		Urgency:               p.Urgency,
		PromptContextEnvelope: h.knowledgeClaimPromptContextEnvelope(ctx, p.WorkspaceID, "workspace.claim.escalate"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.claim.escalate")
	}
	actions := []runtimeEventPublishAction{
		runtimeEventPublishAction{
			Event: claimEvent,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishKnowledgeClaimEventRecord(event, "workspace.claim.review_escalated", record.Claim)
			},
		},
		runtimeEventPublishAction{
			Event: queueEvent,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishOperatorQueueEventRecord(event, "workspace.ops.escalated", record.Queue)
			},
		},
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(event, record.Claim.Subject, record.Claim.ClaimID)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)
	return map[string]any{
		"claim":  record.Claim,
		"queue":  record.Queue,
		"status": record.Claim.Status,
	}, nil
}

func (h *Handler) workspaceClaimLifecycle(ctx context.Context, raw json.RawMessage, action string) (any, *RPCError) {
	var p workspaceClaimLifecycleParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ClaimID, "claim_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	input := sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:           p.WorkspaceID,
		ClaimID:               p.ClaimID,
		ActorID:               p.ActorID,
		Reason:                p.Reason,
		ReviewDueAt:           p.DueAt,
		AssignedTo:            p.AssignedTo,
		Urgency:               p.Urgency,
		SupersedingClaimID:    p.SupersedingClaimID,
		ConflictsClaimID:      p.ConflictsClaimID,
		PromptContextEnvelope: h.knowledgeClaimPromptContextEnvelope(ctx, p.WorkspaceID, workspaceClaimLifecycleSurface(action)),
	}
	var (
		record             sqlite.KnowledgeClaimRecord
		queueRecord        sqlite.OperatorQueueRecord
		event              sqlite.RuntimeEventRecord
		queueEvent         sqlite.RuntimeEventRecord
		invalidationEvents []sqlite.RuntimeEventRecord
		err                error
	)
	switch action {
	case "REVIEW":
		record, queueRecord, event, queueEvent, invalidationEvents, err = h.store.RequestKnowledgeClaimReviewWithEffects(ctx, input)
	case "CONFIRMED":
		record, queueRecord, event, queueEvent, invalidationEvents, err = h.store.ConfirmKnowledgeClaimWithEffects(ctx, input)
	case "DISPUTED":
		record, queueRecord, event, queueEvent, invalidationEvents, err = h.store.DisputeKnowledgeClaimWithEffects(ctx, input)
	case "SUPERSEDED":
		record, queueRecord, event, queueEvent, invalidationEvents, err = h.store.SupersedeKnowledgeClaimWithEffects(ctx, input)
	case "STALE":
		record, queueRecord, event, queueEvent, invalidationEvents, err = h.store.MarkKnowledgeClaimStaleWithEffects(ctx, input)
	default:
		err = errors.New("unsupported knowledge claim action")
	}
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		surface := map[string]string{
			"REVIEW":     "workspace.claim.review",
			"CONFIRMED":  "workspace.claim.confirm",
			"DISPUTED":   "workspace.claim.dispute",
			"SUPERSEDED": "workspace.claim.supersede",
			"STALE":      "workspace.claim.stale",
		}[action]
		if surface == "" {
			surface = "workspace.claim.lifecycle"
		}
		return nil, rpcErrorFromStoreAuthority(err, surface)
	}
	eventType := map[string]string{
		"REVIEW":     "workspace.claim.review_requested",
		"CONFIRMED":  "workspace.claim.confirmed",
		"DISPUTED":   "workspace.claim.disputed",
		"SUPERSEDED": "workspace.claim.superseded",
		"STALE":      "workspace.claim.stale",
	}[action]
	actions := []runtimeEventPublishAction{
		{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishKnowledgeClaimEventRecord(runtimeEvent, eventType, record)
			},
		},
	}
	if queueEvent.EventID != "" {
		queueLiveType := operatorQueueRuntimeEventLiveType(queueEvent.EventType)
		if queueLiveType == "" {
			queueLiveType = "workspace.ops.updated"
		}
		actions = append(actions, runtimeEventPublishAction{
			Event: queueEvent,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishOperatorQueueEventRecord(runtimeEvent, queueLiveType, queueRecord)
			},
		})
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, record.Subject, record.ClaimID)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)
	return map[string]any{"claim": record, "status": record.Status}, nil
}

func (h *Handler) promptContextPrincipal(ctx context.Context) (string, string) {
	principalType := "system"
	principalID := "server_rpc"
	if principal, ok := authPrincipalFromContext(ctx); ok {
		if trimmed := strings.TrimSpace(principal.PrincipalType); trimmed != "" {
			principalType = trimmed
		}
		if trimmed := strings.TrimSpace(principal.PrincipalID); trimmed != "" {
			principalID = trimmed
		}
	}
	return principalType, principalID
}

func (h *Handler) executionWritePromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildExecutionPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) operatorQueuePromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildOperatorQueuePromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) knowledgeClaimPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildKnowledgeClaimPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) capabilityPolicyPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildCapabilityPolicyPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) controlCommandPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildControlCommandPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) workspaceDocPromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildWorkspaceDocPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value = strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) agentUpdatePromptContextEnvelope(ctx context.Context, workspaceID, updateID, agentID, updateType, summary string, requiresHuman bool) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildAgentUpdatePromptContextEnvelope("agent.update.post", "server_rpc", workspaceID, principalType, principalID)
	envelope["update_id"] = strings.TrimSpace(updateID)
	envelope["agent_id"] = strings.TrimSpace(agentID)
	envelope["actor_agent_id"] = strings.TrimSpace(agentID)
	envelope["update_type"] = strings.TrimSpace(updateType)
	envelope["summary"] = strings.TrimSpace(summary)
	envelope["requires_human"] = fmt.Sprintf("%t", requiresHuman)
	return envelope
}

func workspaceClaimLifecycleSurface(action string) string {
	switch strings.TrimSpace(action) {
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

func (h *Handler) workspaceExecutionRunWrite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceExecutionRunWriteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		RunID:                 p.RunID,
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		SessionID:             p.SessionID,
		AgentID:               p.AgentID,
		Title:                 p.Title,
		Summary:               p.Summary,
		Status:                p.Status,
		Outcome:               p.Outcome,
		Verification:          p.Verification,
		PromptContextEnvelope: h.executionWritePromptContextEnvelope(ctx, p.WorkspaceID, "workspace.execution.run.write"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.execution.run.write")
	}
	if strings.TrimSpace(record.AgentID) != "" {
		h.touchAgentActivity(ctx, record.WorkspaceID, record.AgentID)
	}
	h.publishRuntimeEventRecordEnvelopeAs(event, "workspace.execution.run", string(mustJSON(record)), firstNonEmpty(record.Summary, record.Title, record.RunID))
	runLineage := runtimeLineageFromRuntimeEvent(event)
	if rpcErr := h.rollbackLinkedRebaseFollowupForFailedExecutionRun(ctx, record, runLineage); rpcErr != nil {
		h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
			WorkspaceID:     record.WorkspaceID,
			FailureScope:    "execution_run",
			FailureTrigger:  "execution_run_verifier_failed",
			FailureMessage:  rpcErr.Message,
			SourceID:        record.RunID,
			RunID:           record.RunID,
			TaskID:          record.TaskID,
			SessionID:       record.SessionID,
			AgentID:         record.AgentID,
			ActionID:        executionVerifierString(record.VerificationJSON, "action_id"),
			SourceQueueID:   executionVerifierString(record.VerificationJSON, "source_queue_id"),
			SourceQueueKey:  executionVerifierString(record.VerificationJSON, "source_queue_key"),
			RepairTensionID: executionVerifierString(record.VerificationJSON, "repair_tension_id"),
			Lineage:         runLineage,
		})
		log.Printf("[control-plane] execution run verifier rollback failed workspace=%s run=%s: %s", record.WorkspaceID, record.RunID, rpcErr.Message)
	}
	return map[string]any{"run": record, "status": "RECORDED"}, nil
}

func (h *Handler) workspaceExecutionRunList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceExecutionRunListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{
		WorkspaceID: p.WorkspaceID,
		Status:      p.Status,
		TaskID:      p.TaskID,
		SessionID:   p.SessionID,
		AgentID:     p.AgentID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, rpcErr := h.loadWorkspaceTimeAuthority(ctx, p.WorkspaceID, "workspace.execution.run.list")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(p.WorkspaceID),
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceExecutionRunGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceExecutionRunGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.RunID, "run_id"); rpcErr != nil {
		return nil, rpcErr
	}
	detail, err := h.store.GetExecutionRun(ctx, p.WorkspaceID, p.RunID)
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.policy.put")
	}
	return map[string]any{"detail": detail}, nil
}

func (h *Handler) workspaceExecutionAgentRunsCancel(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceExecutionAgentRunsCancelParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principalType, principalID := h.promptContextPrincipal(ctx)
	result, event, err := h.store.CancelExecutionRunsForAgentStopWithEvent(ctx, sqlite.ExecutionAgentRunsCancelInput{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		ActorType:   principalType,
		ActorID:     principalID,
		Summary:     p.Summary,
		Outcome:     p.Outcome,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.execution.agent_runs.cancel")
	}
	if result.RunsCancelled > 0 || result.StepsCancelled > 0 {
		h.touchAgentActivity(ctx, result.WorkspaceID, result.AgentID)
	}
	h.publishRuntimeEventRecordAs(event, "workspace.execution.agent_runs.cancelled", result.AgentID, result.Summary)
	return map[string]any{"result": result, "status": "RECORDED"}, nil
}

func (h *Handler) workspaceExecutionStepWrite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceExecutionStepWriteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.RunID, "run_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:                p.StepID,
		RunID:                 p.RunID,
		WorkspaceID:           p.WorkspaceID,
		ParentStepID:          p.ParentStepID,
		Phase:                 p.Phase,
		Title:                 p.Title,
		Summary:               p.Summary,
		Status:                p.Status,
		SortOrder:             p.SortOrder,
		Evidence:              p.Evidence,
		Verification:          p.Verification,
		PromptContextEnvelope: h.executionWritePromptContextEnvelope(ctx, p.WorkspaceID, "workspace.execution.step.write"),
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.execution.step.write")
	}
	h.publishRuntimeEventRecordEnvelopeAs(event, "workspace.execution.step", string(mustJSON(record)), firstNonEmpty(record.Summary, record.Title, record.StepID))
	stepLineage := runtimeLineageFromRuntimeEvent(event)
	if rpcErr := h.rollbackLinkedRebaseFollowupForFailedVerifyStep(ctx, record, stepLineage); rpcErr != nil {
		h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
			WorkspaceID:     record.WorkspaceID,
			FailureScope:    "execution_step",
			FailureTrigger:  "execution_verifier_failed",
			FailureMessage:  rpcErr.Message,
			SourceID:        record.StepID,
			RunID:           record.RunID,
			StepID:          record.StepID,
			ActionID:        executionVerifierString(record.VerificationJSON, "action_id"),
			SourceQueueID:   executionVerifierString(record.VerificationJSON, "source_queue_id"),
			SourceQueueKey:  executionVerifierString(record.VerificationJSON, "source_queue_key"),
			RepairTensionID: executionVerifierString(record.VerificationJSON, "repair_tension_id"),
			Lineage:         stepLineage,
		})
		log.Printf("[control-plane] execution verifier rollback failed workspace=%s run=%s step=%s: %s", record.WorkspaceID, record.RunID, record.StepID, rpcErr.Message)
	}
	return map[string]any{"step": record, "status": "RECORDED"}, nil
}

func (h *Handler) rollbackLinkedRebaseFollowupForFailedVerifyStep(ctx context.Context, step sqlite.ExecutionStepRecord, lineage rebaseRuntimeLineage) *RPCError {
	if strings.ToUpper(strings.TrimSpace(step.Phase)) != "VERIFY" || strings.ToUpper(strings.TrimSpace(step.Status)) != "FAILED" {
		return nil
	}
	target, ok, err := h.executionVerifierRollbackTargetFromLinkage(ctx, step.WorkspaceID, step.RunID, "", step.VerificationJSON, "failed verify step")
	if err != nil {
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if !ok {
		return nil
	}
	comment := "Execution verifier late fail during step " + firstNonEmpty(strings.TrimSpace(step.StepID), strings.TrimSpace(step.Title), strings.TrimSpace(step.RunID))
	if summary := strings.TrimSpace(firstNonEmpty(step.Summary, step.Title)); summary != "" {
		comment = "Execution verifier late fail: " + summary
	}
	resolveParams := actionResolveParams{
		ActionID:   target.action.ActionID,
		Resolution: humanActionStatusFailed,
		Comment:    comment,
		ResolvedBy: "system:execution_verifier",
	}
	resolveOpts := actionResolveOptions{RollbackReason: "execution_verifier_failed", Lineage: lineage}
	_, rpcErr := h.resolveActionWithEffects(ctx, target.action, resolveParams, resolveOpts)
	if isHumanActionWorkflowConflictRPCError(rpcErr) {
		if h.humanActionRollbackConflictAlreadyHandled(ctx, target) {
			return nil
		}
		return h.retryHumanActionRollbackOnCurrentCarrier(ctx, target, resolveParams, resolveOpts)
	}
	return rpcErr
}

type executionVerifierRollbackTarget struct {
	queue  sqlite.OperatorQueueRecord
	action sqlite.HumanActionRecord
}

type rebaseRollbackFailureInput struct {
	WorkspaceID                 string
	FailureScope                string
	FailureTrigger              string
	FailureMessage              string
	EventID                     string
	SourceID                    string
	RunID                       string
	StepID                      string
	EntityID                    string
	Family                      string
	TaskID                      string
	SessionID                   string
	AgentID                     string
	ActionID                    string
	SourceQueueID               string
	SourceQueueKey              string
	RepairTensionID             string
	Lineage                     rebaseRuntimeLineage
	DisableCurrentCreateContext bool
}

func (h *Handler) queueRebaseRollbackFailure(ctx context.Context, input rebaseRollbackFailureInput) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return
	}
	h.fillRebaseRollbackFailureRunContext(ctx, &input)
	payload := rebaseRollbackFailurePayload(input)
	queueKey := rebaseRollbackFailureQueueKey(input)
	if queueKey == "" {
		return
	}
	requireCurrentStatus := ""
	requireCurrentRevision := int64(0)
	requireCurrentUpdatedAt := ""
	requireMissing := false
	sourceID := firstNonEmpty(input.SourceID, input.RunID, input.StepID, input.EntityID, input.SourceQueueID, input.ActionID, input.RepairTensionID)
	existing, err := h.store.GetOperatorQueueItem(ctx, input.WorkspaceID, "", queueKey)
	switch {
	case err == nil:
		requireCurrentStatus = strings.TrimSpace(existing.Status)
		requireCurrentRevision = existing.Revision
		if requireCurrentRevision <= 0 {
			requireCurrentUpdatedAt = strings.TrimSpace(existing.UpdatedAt)
		}
		payload = rebaseRollbackFailurePayloadWithExistingFollowupState(existing, payload)
	case err != nil && isOperatorQueueItemNotFoundErr(err):
		requireMissing = true
	case err != nil && !isOperatorQueueItemNotFoundErr(err):
		log.Printf("[control-plane] rebase rollback failure queue lookup failed workspace=%s key=%s err=%v", input.WorkspaceID, queueKey, err)
	}
	useStoreCurrentCreateLineage := requireMissing && (strings.TrimSpace(input.SourceQueueID) != "" || strings.TrimSpace(input.SourceQueueKey) != "")
	useStoreCurrentActionCreateContext := requireMissing && !useStoreCurrentCreateLineage && strings.TrimSpace(input.ActionID) != ""
	useStoreCurrentRepairCreateContext := requireMissing && !useStoreCurrentCreateLineage && !useStoreCurrentActionCreateContext && strings.TrimSpace(input.RepairTensionID) != ""
	useStoreCurrentTaskCreateContext := requireMissing && !useStoreCurrentCreateLineage && !useStoreCurrentActionCreateContext && !useStoreCurrentRepairCreateContext && (strings.TrimSpace(input.TaskID) != "" || strings.TrimSpace(input.RunID) != "")
	if input.DisableCurrentCreateContext {
		useStoreCurrentCreateLineage = false
		useStoreCurrentActionCreateContext = false
		useStoreCurrentRepairCreateContext = false
		useStoreCurrentTaskCreateContext = false
	}
	if requireMissing && !input.DisableCurrentCreateContext && !useStoreCurrentCreateLineage && !useStoreCurrentActionCreateContext && !useStoreCurrentRepairCreateContext && !useStoreCurrentTaskCreateContext {
		payload = h.rebaseRollbackFailurePayloadWithCurrentCreateLineage(ctx, input, payload)
	}
	if requireMissing && !useStoreCurrentCreateLineage && !useStoreCurrentActionCreateContext && !useStoreCurrentRepairCreateContext && !useStoreCurrentTaskCreateContext {
		clearRebaseRollbackFailurePayloadLineage(&payload)
		payload.Normalize()
	}
	payloadJSON := string(mustJSON(payload))
	if err == nil && rebaseRollbackFailureQueueNoopOnExistingReplay(existing, input, sourceID, payloadJSON) {
		return
	}
	if err == nil && strings.ToUpper(strings.TrimSpace(existing.Status)) != "OPEN" {
		return
	}
	if err == nil && h.beforeRebaseRollbackFailureUpsertOverride != nil {
		h.beforeRebaseRollbackFailureUpsertOverride(ctx, existing)
	}
	if requireMissing && h.beforeRebaseRollbackFailureCreateOverride != nil {
		h.beforeRebaseRollbackFailureCreateOverride(ctx, queueKey)
	}
	upsertInput := sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             input.WorkspaceID,
		QueueKey:                queueKey,
		QueueType:               "FOLLOW_UP",
		Title:                   "Repair automatic rebase rollback failure",
		Summary:                 rebaseRollbackFailureSummary(input),
		Details:                 rebaseRollbackFailureDetails(input),
		PayloadJSON:             payloadJSON,
		Urgency:                 "HIGH",
		SourceKind:              strings.TrimSpace(input.FailureScope),
		SourceID:                sourceID,
		TaskID:                  strings.TrimSpace(input.TaskID),
		SessionID:               strings.TrimSpace(input.SessionID),
		AgentID:                 strings.TrimSpace(input.AgentID),
		KeepSessionActive:       true,
		RequireMissing:          requireMissing,
		RequireCurrentStatus:    requireCurrentStatus,
		RequireCurrentRevision:  requireCurrentRevision,
		RequireCurrentUpdatedAt: requireCurrentUpdatedAt,
	}
	var record sqlite.OperatorQueueRecord
	var event sqlite.RuntimeEventRecord
	if useStoreCurrentCreateLineage {
		record, event, err = h.store.UpsertRollbackFailureQueueItemWithCurrentCreateLineage(ctx, upsertInput, input.SourceQueueID, input.SourceQueueKey)
	} else if useStoreCurrentActionCreateContext {
		record, event, err = h.store.UpsertRollbackFailureQueueItemWithCurrentLinkedActionCreateContext(ctx, upsertInput, input.ActionID)
	} else if useStoreCurrentRepairCreateContext {
		record, event, err = h.store.UpsertRollbackFailureQueueItemWithCurrentLinkedRepairCreateContext(ctx, upsertInput, input.RepairTensionID)
	} else if useStoreCurrentTaskCreateContext {
		record, event, err = h.store.UpsertRollbackFailureQueueItemWithCurrentTaskCreateContext(ctx, upsertInput, input.TaskID, input.RunID)
	} else {
		record, event, err = h.store.UpsertOperatorQueueItemWithEvent(ctx, upsertInput)
	}
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "updated concurrently") {
			if latest, latestErr := h.store.GetOperatorQueueItem(ctx, input.WorkspaceID, "", queueKey); latestErr == nil {
				noopPayloadJSON := payloadJSON
				if useStoreCurrentCreateLineage {
					currentPayload := h.rebaseRollbackFailurePayloadWithCurrentCreateLineage(ctx, input, rebaseRollbackFailurePayload(input))
					noopPayloadJSON = string(mustJSON(currentPayload))
				} else if useStoreCurrentActionCreateContext {
					currentPayload := h.rebaseRollbackFailurePayloadWithCurrentActionCreateContext(ctx, input, rebaseRollbackFailurePayload(input))
					noopPayloadJSON = string(mustJSON(currentPayload))
				} else if useStoreCurrentRepairCreateContext {
					currentPayload := h.rebaseRollbackFailurePayloadWithCurrentRepairTensionCreateContext(ctx, input, rebaseRollbackFailurePayload(input))
					noopPayloadJSON = string(mustJSON(currentPayload))
				} else if useStoreCurrentTaskCreateContext {
					currentPayload := h.rebaseRollbackFailurePayloadWithCurrentTaskOrRunCreateContext(ctx, input, rebaseRollbackFailurePayload(input))
					noopPayloadJSON = string(mustJSON(currentPayload))
				}
				if rebaseRollbackFailureQueueNoopOnExistingReplay(latest, input, sourceID, noopPayloadJSON) {
					return
				}
			}
		}
		log.Printf("[control-plane] failed to persist rebase rollback failure queue workspace=%s key=%s err=%v", input.WorkspaceID, queueKey, err)
		return
	}
	liveType := operatorQueueRuntimeEventLiveType(event.EventType)
	if liveType == "" {
		liveType = "workspace.ops.updated"
	}
	h.publishOperatorQueueEventRecord(event, liveType, record)
}

func rollbackFailureLineagePresent(lineage rebaseRuntimeLineage) bool {
	lineage = normalizeRuntimeLineage(lineage)
	return lineage.RootCauseID != "" || lineage.ProvenanceGroupID != "" || len(lineage.ParentRefsJSON) > 0
}

func clearRebaseRollbackFailurePayloadLineage(payload *model.RebaseRollbackFailurePayload) {
	if payload == nil {
		return
	}
	payload.RootCauseID = ""
	payload.ProvenanceGroupID = ""
	payload.ParentRefsJSON = nil
}

func (h *Handler) rebaseRollbackFailurePayloadWithCurrentCreateLineage(ctx context.Context, input rebaseRollbackFailureInput, payload model.RebaseRollbackFailurePayload) model.RebaseRollbackFailurePayload {
	lineage, ok := h.currentRebaseRollbackFailureCreateLineage(ctx, input)
	if !ok {
		return payload
	}
	applyRebaseRollbackFailurePayloadLineage(&payload, lineage)
	payload.Normalize()
	return payload
}

func (h *Handler) rebaseRollbackFailurePayloadWithCurrentActionCreateContext(ctx context.Context, input rebaseRollbackFailureInput, payload model.RebaseRollbackFailurePayload) model.RebaseRollbackFailurePayload {
	resolvedInput, ok := h.currentRebaseRollbackFailureCreateInputFromAction(ctx, input)
	if !ok {
		return payload
	}
	payload.ActionID = strings.TrimSpace(resolvedInput.ActionID)
	payload.SourceQueueID = strings.TrimSpace(resolvedInput.SourceQueueID)
	payload.SourceQueueKey = strings.TrimSpace(resolvedInput.SourceQueueKey)
	if strings.TrimSpace(payload.RepairTensionID) == "" {
		payload.RepairTensionID = strings.TrimSpace(resolvedInput.RepairTensionID)
	}
	lineage, ok := h.currentRebaseRollbackFailureCreateLineage(ctx, resolvedInput)
	if ok {
		applyRebaseRollbackFailurePayloadLineage(&payload, lineage)
	}
	payload.Normalize()
	return payload
}

func (h *Handler) rebaseRollbackFailurePayloadWithCurrentRepairTensionCreateContext(ctx context.Context, input rebaseRollbackFailureInput, payload model.RebaseRollbackFailurePayload) model.RebaseRollbackFailurePayload {
	resolvedInput, ok := h.currentRebaseRollbackFailureCreateInputFromRepairTension(ctx, input)
	if !ok {
		return payload
	}
	payload.RepairTensionID = strings.TrimSpace(resolvedInput.RepairTensionID)
	payload.ActionID = strings.TrimSpace(resolvedInput.ActionID)
	payload.SourceQueueID = strings.TrimSpace(resolvedInput.SourceQueueID)
	payload.SourceQueueKey = strings.TrimSpace(resolvedInput.SourceQueueKey)
	lineage, ok := h.currentRebaseRollbackFailureCreateLineage(ctx, resolvedInput)
	if ok {
		applyRebaseRollbackFailurePayloadLineage(&payload, lineage)
	}
	payload.Normalize()
	return payload
}

func (h *Handler) rebaseRollbackFailurePayloadWithCurrentTaskOrRunCreateContext(ctx context.Context, input rebaseRollbackFailureInput, payload model.RebaseRollbackFailurePayload) model.RebaseRollbackFailurePayload {
	resolvedInput, ok := h.currentRebaseRollbackFailureCreateInputFromTaskOrRun(ctx, input)
	if !ok {
		return payload
	}
	payload.TaskID = firstNonEmpty(strings.TrimSpace(payload.TaskID), strings.TrimSpace(resolvedInput.TaskID))
	payload.ActionID = firstNonEmpty(strings.TrimSpace(payload.ActionID), strings.TrimSpace(resolvedInput.ActionID))
	payload.RepairTensionID = firstNonEmpty(strings.TrimSpace(payload.RepairTensionID), strings.TrimSpace(resolvedInput.RepairTensionID))
	payload.SourceQueueID = strings.TrimSpace(resolvedInput.SourceQueueID)
	payload.SourceQueueKey = strings.TrimSpace(resolvedInput.SourceQueueKey)
	lineage, ok := h.currentRebaseRollbackFailureCreateLineage(ctx, resolvedInput)
	if ok {
		applyRebaseRollbackFailurePayloadLineage(&payload, lineage)
	}
	payload.Normalize()
	return payload
}

func (h *Handler) currentRebaseRollbackFailureCreateInputFromAction(ctx context.Context, input rebaseRollbackFailureInput) (rebaseRollbackFailureInput, bool) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	actionID := strings.TrimSpace(input.ActionID)
	if workspaceID == "" || actionID == "" {
		return input, false
	}
	actionQueue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		return input, false
	}
	actionPayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		return input, false
	}
	sourceQueueID := strings.TrimSpace(actionPayload.SourceQueueID)
	sourceQueueKey := strings.TrimSpace(actionPayload.SourceQueueKey)
	if sourceQueueID == "" && sourceQueueKey == "" {
		return input, false
	}
	sourceQueue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, sourceQueueKey)
	if err != nil {
		return input, false
	}
	resolved := input
	resolved.ActionID = actionID
	resolved.SourceQueueID = strings.TrimSpace(sourceQueue.QueueID)
	resolved.SourceQueueKey = strings.TrimSpace(sourceQueue.QueueKey)
	if strings.TrimSpace(resolved.RepairTensionID) == "" {
		resolved.RepairTensionID = strings.TrimSpace(actionPayload.RepairTensionID)
	}
	return resolved, true
}

func (h *Handler) currentRebaseRollbackFailureCreateInputFromTaskOrRun(ctx context.Context, input rebaseRollbackFailureInput) (rebaseRollbackFailureInput, bool) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	taskID := strings.TrimSpace(input.TaskID)
	runID := strings.TrimSpace(input.RunID)
	if workspaceID == "" || (taskID == "" && runID == "") {
		return input, false
	}
	sourceQueueID, sourceQueueKey, actionID, repairTensionID, ok, err := h.store.CurrentRollbackFailureCreateSourceQueueFromTaskOrRun(ctx, workspaceID, taskID, runID)
	if err != nil || !ok {
		return input, false
	}
	resolved := input
	resolved.TaskID = firstNonEmpty(strings.TrimSpace(input.TaskID), taskID)
	resolved.ActionID = firstNonEmpty(strings.TrimSpace(input.ActionID), actionID)
	resolved.RepairTensionID = firstNonEmpty(strings.TrimSpace(input.RepairTensionID), repairTensionID)
	resolved.SourceQueueID = sourceQueueID
	resolved.SourceQueueKey = sourceQueueKey
	return resolved, true
}

func (h *Handler) currentRebaseRollbackFailureCreateInputFromRepairTension(ctx context.Context, input rebaseRollbackFailureInput) (rebaseRollbackFailureInput, bool) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	repairTensionID := strings.TrimSpace(input.RepairTensionID)
	if workspaceID == "" || repairTensionID == "" {
		return input, false
	}
	sourceQueue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, "", model.RebaseFollowupQueueKeyPrefix+repairTensionID)
	if err != nil {
		return input, false
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		return input, false
	}
	if strings.TrimSpace(sourcePayload.RepairTensionID) != repairTensionID {
		return input, false
	}
	resolved := input
	resolved.RepairTensionID = repairTensionID
	resolved.ActionID = firstNonEmpty(strings.TrimSpace(input.ActionID), strings.TrimSpace(sourcePayload.ActionID))
	resolved.SourceQueueID = strings.TrimSpace(sourceQueue.QueueID)
	resolved.SourceQueueKey = strings.TrimSpace(sourceQueue.QueueKey)
	return resolved, true
}

func (h *Handler) currentRebaseRollbackFailureCreateLineage(ctx context.Context, input rebaseRollbackFailureInput) (rebaseRuntimeLineage, bool) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return rebaseRuntimeLineage{}, false
	}
	sourceQueueID := strings.TrimSpace(input.SourceQueueID)
	sourceQueueKey := strings.TrimSpace(input.SourceQueueKey)
	if sourceQueueID == "" && sourceQueueKey == "" {
		return rebaseRuntimeLineage{}, false
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, sourceQueueKey)
	if err != nil {
		return rebaseRuntimeLineage{}, false
	}
	queueKey := strings.TrimSpace(queue.QueueKey)
	if strings.HasPrefix(queueKey, model.RebaseRollbackFailureQueueKeyPrefix) {
		payload, err := actionCreateDecodeRollbackFailurePayload(queue.PayloadJSON)
		if err != nil {
			return rebaseRuntimeLineage{}, false
		}
		lineage := rebaseRollbackFailurePayloadLineage(payload)
		return lineage, rollbackFailureLineagePresent(lineage)
	}
	payload, err := actionCreateDecodeQueuePayload(queue.PayloadJSON)
	if err != nil {
		return rebaseRuntimeLineage{}, false
	}
	lineage := rebaseFollowupPayloadLineage(payload)
	if rollbackFailureLineagePresent(lineage) {
		return lineage, true
	}
	actionID := strings.TrimSpace(payload.ActionID)
	if actionID == "" {
		actionID = strings.TrimSpace(payload.LastFailedActionID)
	}
	return h.latestHumanActionLineage(ctx, workspaceID, actionID)
}

func (h *Handler) latestHumanActionLineage(ctx context.Context, workspaceID, actionID string) (rebaseRuntimeLineage, bool) {
	workspaceID = strings.TrimSpace(workspaceID)
	actionID = strings.TrimSpace(actionID)
	if workspaceID == "" || actionID == "" {
		return rebaseRuntimeLineage{}, false
	}
	events, err := h.store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       1,
	})
	if err != nil || len(events) == 0 {
		return rebaseRuntimeLineage{}, false
	}
	lineage := runtimeLineageFromRuntimeEvent(events[0])
	return lineage, rollbackFailureLineagePresent(lineage)
}

func rebaseRollbackFailureQueueNoopOnExistingReplay(existing sqlite.OperatorQueueRecord, input rebaseRollbackFailureInput, sourceID, payloadJSON string) bool {
	if strings.TrimSpace(existing.QueueType) != "FOLLOW_UP" {
		return false
	}
	if strings.TrimSpace(existing.Title) != "Repair automatic rebase rollback failure" {
		return false
	}
	if strings.TrimSpace(existing.Urgency) != "HIGH" || !existing.KeepSessionActive {
		return false
	}
	if strings.TrimSpace(existing.SourceKind) != strings.TrimSpace(input.FailureScope) {
		return false
	}
	if strings.TrimSpace(existing.SourceID) != strings.TrimSpace(sourceID) {
		return false
	}
	if strings.TrimSpace(existing.TaskID) != strings.TrimSpace(input.TaskID) {
		return false
	}
	if strings.TrimSpace(existing.SessionID) != strings.TrimSpace(input.SessionID) {
		return false
	}
	if strings.TrimSpace(existing.AgentID) != strings.TrimSpace(input.AgentID) {
		return false
	}
	return strings.TrimSpace(existing.PayloadJSON) == strings.TrimSpace(payloadJSON)
}

func rebaseRollbackFailurePayloadWithExistingFollowupState(existing sqlite.OperatorQueueRecord, payload model.RebaseRollbackFailurePayload) model.RebaseRollbackFailurePayload {
	if strings.TrimSpace(existing.PayloadJSON) == "" {
		payload.Normalize()
		return payload
	}
	existingPayload, err := actionCreateDecodeRollbackFailurePayload(existing.PayloadJSON)
	if err != nil || !existingPayload.IsRollbackFailure(existing.QueueKey) {
		payload.Normalize()
		return payload
	}
	if strings.TrimSpace(payload.FollowupActionID) == "" {
		payload.FollowupActionID = strings.TrimSpace(existingPayload.FollowupActionID)
	}
	if strings.TrimSpace(payload.FollowupActionQueueKey) == "" {
		payload.FollowupActionQueueKey = strings.TrimSpace(existingPayload.FollowupActionQueueKey)
	}
	if strings.TrimSpace(payload.FollowupActionStatus) == "" {
		payload.FollowupActionStatus = strings.TrimSpace(existingPayload.FollowupActionStatus)
	}
	if strings.TrimSpace(payload.LastFailedFollowupActionID) == "" {
		payload.LastFailedFollowupActionID = strings.TrimSpace(existingPayload.LastFailedFollowupActionID)
	}
	if strings.TrimSpace(payload.LastFailedFollowupActionStatus) == "" {
		payload.LastFailedFollowupActionStatus = strings.TrimSpace(existingPayload.LastFailedFollowupActionStatus)
	}
	payload = rebaseRollbackFailurePayloadWithExistingLineage(existingPayload, payload)
	payload.Normalize()
	return payload
}

func rebaseRollbackFailurePayloadWithExistingLineage(existingPayload, payload model.RebaseRollbackFailurePayload) model.RebaseRollbackFailurePayload {
	existingLineage := rebaseRollbackFailurePayloadLineage(existingPayload)
	if existingLineage.RootCauseID == "" && existingLineage.ProvenanceGroupID == "" && len(existingLineage.ParentRefsJSON) == 0 {
		return payload
	}
	payload.Normalize()
	incomingLineage := rebaseRollbackFailurePayloadLineage(payload)
	providedRoot := strings.TrimSpace(payload.RootCauseID) != ""
	providedProvenance := strings.TrimSpace(payload.ProvenanceGroupID) != ""
	providedParents := len(payload.ParentRefsJSON) > 0
	if !providedRoot && !providedProvenance && !providedParents {
		payload.RootCauseID = existingLineage.RootCauseID
		payload.ProvenanceGroupID = existingLineage.ProvenanceGroupID
		if len(existingLineage.ParentRefsJSON) > 0 {
			payload.ParentRefsJSON = append([]string(nil), existingLineage.ParentRefsJSON...)
		}
		return payload
	}
	if (providedRoot && incomingLineage.RootCauseID != existingLineage.RootCauseID) ||
		(providedProvenance && incomingLineage.ProvenanceGroupID != existingLineage.ProvenanceGroupID) ||
		(providedParents && !reflect.DeepEqual(incomingLineage.ParentRefsJSON, existingLineage.ParentRefsJSON)) {
		payload.RootCauseID = existingLineage.RootCauseID
		payload.ProvenanceGroupID = existingLineage.ProvenanceGroupID
		payload.ParentRefsJSON = append([]string(nil), existingLineage.ParentRefsJSON...)
		return payload
	}
	if !providedRoot {
		payload.RootCauseID = existingLineage.RootCauseID
	}
	if !providedProvenance {
		payload.ProvenanceGroupID = existingLineage.ProvenanceGroupID
	}
	if !providedParents && len(existingLineage.ParentRefsJSON) > 0 {
		payload.ParentRefsJSON = append([]string(nil), existingLineage.ParentRefsJSON...)
	}
	return payload
}

func (h *Handler) rollbackLinkedRebaseFollowupForFailedExecutionRun(ctx context.Context, run sqlite.ExecutionRunRecord, lineage rebaseRuntimeLineage) *RPCError {
	if strings.ToUpper(strings.TrimSpace(run.Status)) != "FAILED" {
		return nil
	}
	target, ok, err := h.executionVerifierRollbackTargetFromLinkage(ctx, run.WorkspaceID, run.RunID, run.TaskID, run.VerificationJSON, "failed execution run")
	if err != nil {
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if !ok {
		return nil
	}
	comment := "Execution verifier late fail during run " + firstNonEmpty(strings.TrimSpace(run.RunID), strings.TrimSpace(run.Title))
	if summary := strings.TrimSpace(firstNonEmpty(run.Outcome, run.Summary, run.Title)); summary != "" {
		comment = "Execution verifier late fail: " + summary
	}
	resolveParams := actionResolveParams{
		ActionID:   target.action.ActionID,
		Resolution: humanActionStatusFailed,
		Comment:    comment,
		ResolvedBy: "system:execution_verifier",
	}
	resolveOpts := actionResolveOptions{RollbackReason: "execution_run_verifier_failed", Lineage: lineage}
	_, rpcErr := h.resolveActionWithEffects(ctx, target.action, resolveParams, resolveOpts)
	if isHumanActionWorkflowConflictRPCError(rpcErr) {
		if h.humanActionRollbackConflictAlreadyHandled(ctx, target) {
			return nil
		}
		return h.retryHumanActionRollbackOnCurrentCarrier(ctx, target, resolveParams, resolveOpts)
	}
	return rpcErr
}

func (h *Handler) humanActionRollbackAlreadyWon(ctx context.Context, actionID string) bool {
	if h == nil {
		return false
	}
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return false
	}
	currentAction, err := h.store.GetHumanAction(ctx, actionID)
	if err != nil {
		return false
	}
	return strings.ToUpper(strings.TrimSpace(currentAction.Status)) != humanActionStatusPending
}

func (h *Handler) humanActionRollbackConflictAlreadyHandled(ctx context.Context, target executionVerifierRollbackTarget) bool {
	if h == nil {
		return false
	}
	actionID := strings.TrimSpace(target.action.ActionID)
	if actionID == "" {
		return false
	}
	currentAction, err := h.store.GetHumanAction(ctx, actionID)
	if err != nil {
		return false
	}
	if strings.ToUpper(strings.TrimSpace(currentAction.Status)) != humanActionStatusPending {
		return true
	}
	expectedAssignedTo := strings.TrimSpace(target.action.AssignedTo)
	if expectedAssignedTo == "" {
		return h.executionVerifierRollbackQueueConflictAlreadyHandled(ctx, target)
	}
	if strings.TrimSpace(currentAction.AssignedTo) != expectedAssignedTo {
		return true
	}
	return h.executionVerifierRollbackQueueConflictAlreadyHandled(ctx, target)
}

func (h *Handler) retryHumanActionRollbackOnCurrentCarrier(ctx context.Context, target executionVerifierRollbackTarget, params actionResolveParams, opts actionResolveOptions) *RPCError {
	if h == nil {
		return nil
	}
	actionID := strings.TrimSpace(target.action.ActionID)
	if actionID == "" {
		return nil
	}
	currentAction, err := h.store.GetHumanAction(ctx, actionID)
	if err != nil {
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if strings.ToUpper(strings.TrimSpace(currentAction.Status)) != humanActionStatusPending {
		return nil
	}
	_, rpcErr := h.resolveActionWithEffects(ctx, currentAction, params, opts)
	if isHumanActionWorkflowConflictRPCError(rpcErr) {
		if refreshedTarget, ok, err := h.executionVerifierRollbackTargetFromAction(ctx, currentAction, ""); err == nil && ok && h.humanActionRollbackConflictAlreadyHandled(ctx, refreshedTarget) {
			return nil
		}
	}
	return rpcErr
}

func (h *Handler) executionVerifierRollbackQueueConflictAlreadyHandled(ctx context.Context, target executionVerifierRollbackTarget) bool {
	if h == nil {
		return false
	}
	workspaceID := strings.TrimSpace(target.queue.WorkspaceID)
	queueID := strings.TrimSpace(target.queue.QueueID)
	if workspaceID == "" || queueID == "" {
		return false
	}
	snapshotPayload, err := actionCreateDecodeQueuePayload(target.queue.PayloadJSON)
	if err != nil {
		return false
	}
	currentQueue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
	if err != nil {
		return false
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		return false
	}
	if strings.TrimSpace(currentPayload.ActionID) != strings.TrimSpace(target.action.ActionID) {
		return false
	}
	return strings.TrimSpace(currentPayload.RebaseWorkflowState) != strings.TrimSpace(snapshotPayload.RebaseWorkflowState) ||
		strings.TrimSpace(currentPayload.RebaseWorkflowStep) != strings.TrimSpace(snapshotPayload.RebaseWorkflowStep)
}

func (h *Handler) executionVerifierRollbackTargetFromLinkage(ctx context.Context, workspaceID, runID, taskID string, verification map[string]any, scope string) (executionVerifierRollbackTarget, bool, error) {
	actionID := executionVerifierString(verification, "action_id")
	sourceQueueID := executionVerifierString(verification, "source_queue_id")
	sourceQueueKey := executionVerifierString(verification, "source_queue_key")
	repairTensionID := executionVerifierString(verification, "repair_tension_id")
	if actionID == "" && sourceQueueID == "" && sourceQueueKey == "" && repairTensionID == "" {
		return executionVerifierRollbackTarget{}, false, nil
	}
	if sourceQueueID != "" || sourceQueueKey != "" {
		queue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, sourceQueueKey)
		if err != nil {
			return executionVerifierRollbackTarget{}, false, err
		}
		if sourceQueueID != "" && sourceQueueKey != "" && strings.TrimSpace(queue.QueueKey) != sourceQueueKey {
			return executionVerifierRollbackTarget{}, false, nil
		}
		return h.executionVerifierRollbackTargetFromQueue(ctx, queue, actionID, repairTensionID)
	}
	if actionID != "" {
		action, err := h.store.GetHumanAction(ctx, actionID)
		if err != nil {
			return executionVerifierRollbackTarget{}, false, err
		}
		return h.executionVerifierRollbackTargetFromAction(ctx, action, repairTensionID)
	}
	if taskID == "" {
		if runID == "" {
			return executionVerifierRollbackTarget{}, false, nil
		}
		runDetail, err := h.store.GetExecutionRun(ctx, workspaceID, runID)
		if err != nil {
			return executionVerifierRollbackTarget{}, false, err
		}
		taskID = strings.TrimSpace(runDetail.Run.TaskID)
		if taskID == "" {
			return executionVerifierRollbackTarget{}, false, nil
		}
	}
	items, err := h.store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		TaskID:      taskID,
		Limit:       50,
	})
	if err != nil {
		return executionVerifierRollbackTarget{}, false, err
	}
	matches := make([]executionVerifierRollbackTarget, 0, 1)
	for _, item := range items {
		target, ok, err := h.executionVerifierRollbackTargetFromQueue(ctx, item, "", repairTensionID)
		if err != nil {
			return executionVerifierRollbackTarget{}, false, err
		}
		if ok {
			matches = append(matches, target)
		}
	}
	switch len(matches) {
	case 0:
		return executionVerifierRollbackTarget{}, false, nil
	case 1:
		return matches[0], true, nil
	default:
		return executionVerifierRollbackTarget{}, false, errors.New("multiple active linked rebase follow-up queues for " + scope)
	}
}

func (h *Handler) executionVerifierRollbackTargetFromAction(ctx context.Context, action sqlite.HumanActionRecord, repairTensionID string) (executionVerifierRollbackTarget, bool, error) {
	if strings.ToUpper(strings.TrimSpace(action.Status)) != humanActionStatusPending {
		return executionVerifierRollbackTarget{}, false, nil
	}
	linkedQueues, err := h.listLinkedRebaseFollowupQueuesForAction(ctx, action)
	if err != nil {
		return executionVerifierRollbackTarget{}, false, err
	}
	matches := make([]executionVerifierRollbackTarget, 0, 1)
	for _, linkedQueue := range linkedQueues {
		if !executionVerifierRollbackQueueIsActive(linkedQueue.queue, linkedQueue.payload) {
			continue
		}
		if repairTensionID != "" && strings.TrimSpace(linkedQueue.payload.RepairTensionID) != repairTensionID {
			continue
		}
		matches = append(matches, executionVerifierRollbackTarget{
			queue:  linkedQueue.queue,
			action: action,
		})
	}
	switch len(matches) {
	case 0:
		return executionVerifierRollbackTarget{}, false, nil
	case 1:
		return matches[0], true, nil
	default:
		return executionVerifierRollbackTarget{}, false, errors.New("multiple active linked rebase follow-up queues for failed verify step action")
	}
}

func (h *Handler) executionVerifierRollbackTargetFromQueue(ctx context.Context, queue sqlite.OperatorQueueRecord, actionID, repairTensionID string) (executionVerifierRollbackTarget, bool, error) {
	payload, err := actionCreateDecodeQueuePayload(queue.PayloadJSON)
	if err != nil {
		return executionVerifierRollbackTarget{}, false, err
	}
	if !executionVerifierRollbackQueueIsActive(queue, payload) {
		return executionVerifierRollbackTarget{}, false, nil
	}
	if executionVerifierRollbackLinkageRequiresExplicitActionID(payload, actionID) {
		return executionVerifierRollbackTarget{}, false, errors.New("ambiguous rebase rollback linkage after prior retry: action_id is required")
	}
	if actionID != "" && strings.TrimSpace(payload.ActionID) != actionID {
		return executionVerifierRollbackTarget{}, false, nil
	}
	if repairTensionID != "" && strings.TrimSpace(payload.RepairTensionID) != repairTensionID {
		return executionVerifierRollbackTarget{}, false, nil
	}
	action, err := h.store.GetHumanAction(ctx, payload.ActionID)
	if err != nil {
		return executionVerifierRollbackTarget{}, false, err
	}
	if strings.ToUpper(strings.TrimSpace(action.Status)) != humanActionStatusPending {
		return executionVerifierRollbackTarget{}, false, nil
	}
	return executionVerifierRollbackTarget{
		queue:  queue,
		action: action,
	}, true, nil
}

func executionVerifierRollbackLinkageRequiresExplicitActionID(payload model.RebaseFollowupPayload, actionID string) bool {
	return strings.TrimSpace(actionID) == "" && strings.TrimSpace(payload.LastFailedActionID) != ""
}

func executionVerifierRollbackQueueIsActive(queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload) bool {
	if strings.ToUpper(strings.TrimSpace(queue.Status)) != "OPEN" {
		return false
	}
	if !payload.IsRebaseFollowup(queue.QueueKey) || !payload.LinkedActionExists() {
		return false
	}
	return strings.TrimSpace(payload.RebaseWorkflowState) == linkedActionSourceQueueStartedState() &&
		strings.TrimSpace(payload.RebaseWorkflowStep) == linkedActionSourceQueueStartedStep()
}

func executionVerifierString(values map[string]any, key string) string {
	if len(values) == 0 {
		return ""
	}
	raw, ok := values[key]
	if !ok {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(firstNonEmpty(strings.TrimSpace(mustJSONString(v)), ""))
	}
}

func (h *Handler) fillRebaseRollbackFailureRunContext(ctx context.Context, input *rebaseRollbackFailureInput) {
	if input == nil || strings.TrimSpace(input.RunID) == "" {
		return
	}
	if strings.TrimSpace(input.TaskID) != "" && strings.TrimSpace(input.SessionID) != "" && strings.TrimSpace(input.AgentID) != "" {
		return
	}
	runDetail, err := h.store.GetExecutionRun(ctx, strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.RunID))
	if err != nil {
		return
	}
	if strings.TrimSpace(input.TaskID) == "" {
		input.TaskID = strings.TrimSpace(runDetail.Run.TaskID)
	}
	if strings.TrimSpace(input.SessionID) == "" {
		input.SessionID = strings.TrimSpace(runDetail.Run.SessionID)
	}
	if strings.TrimSpace(input.AgentID) == "" {
		input.AgentID = strings.TrimSpace(runDetail.Run.AgentID)
	}
}

func rebaseRollbackFailurePayload(input rebaseRollbackFailureInput) model.RebaseRollbackFailurePayload {
	payload := model.RebaseRollbackFailurePayload{
		Kind:            model.RebaseRollbackFailureKind,
		FailureScope:    strings.TrimSpace(input.FailureScope),
		FailureTrigger:  strings.TrimSpace(input.FailureTrigger),
		FailureMessage:  strings.TrimSpace(input.FailureMessage),
		EventID:         strings.TrimSpace(input.EventID),
		RunID:           strings.TrimSpace(input.RunID),
		StepID:          strings.TrimSpace(input.StepID),
		EntityID:        strings.TrimSpace(input.EntityID),
		Family:          strings.TrimSpace(input.Family),
		TaskID:          strings.TrimSpace(input.TaskID),
		SessionID:       strings.TrimSpace(input.SessionID),
		AgentID:         strings.TrimSpace(input.AgentID),
		ActionID:        strings.TrimSpace(input.ActionID),
		SourceQueueID:   strings.TrimSpace(input.SourceQueueID),
		SourceQueueKey:  strings.TrimSpace(input.SourceQueueKey),
		RepairTensionID: strings.TrimSpace(input.RepairTensionID),
	}
	applyRebaseRollbackFailurePayloadLineage(&payload, input.Lineage)
	payload.Normalize()
	return payload
}

func rebaseRollbackFailureQueueKey(input rebaseRollbackFailureInput) string {
	scope := strings.TrimSpace(input.FailureScope)
	eventID := strings.TrimSpace(input.EventID)
	identity := firstNonEmpty(
		strings.TrimSpace(input.SourceQueueID),
		strings.TrimSpace(input.SourceQueueKey),
		strings.TrimSpace(input.ActionID),
		strings.TrimSpace(input.RepairTensionID),
		strings.TrimSpace(input.RunID),
		strings.TrimSpace(input.StepID),
		func() string {
			if scope == "rsp_anomaly_list" {
				return eventID
			}
			return ""
		}(),
		strings.TrimSpace(input.EntityID),
		strings.TrimSpace(input.SourceID),
		strings.TrimSpace(input.TaskID),
	)
	if scope == "" || identity == "" {
		return ""
	}
	return model.RebaseRollbackFailureQueueKeyPrefix + scope + ":" + identity
}

func rebaseRollbackFailureSummary(input rebaseRollbackFailureInput) string {
	scopeLabel := strings.ReplaceAll(strings.TrimSpace(input.FailureScope), "_", " ")
	if scopeLabel == "" {
		scopeLabel = "rebase rollback"
	}
	target := firstNonEmpty(strings.TrimSpace(input.RunID), strings.TrimSpace(input.StepID), strings.TrimSpace(input.EntityID), strings.TrimSpace(input.SourceQueueID), strings.TrimSpace(input.ActionID), strings.TrimSpace(input.RepairTensionID))
	if target == "" {
		return "Automatic " + scopeLabel + " rollback failed"
	}
	return "Automatic " + scopeLabel + " rollback failed for " + target
}

func rebaseRollbackFailureDetails(input rebaseRollbackFailureInput) string {
	lines := []string{
		"Failure scope: " + firstNonEmpty(strings.TrimSpace(input.FailureScope), "unknown"),
		"Trigger: " + firstNonEmpty(strings.TrimSpace(input.FailureTrigger), "unknown"),
		"Failure: " + firstNonEmpty(strings.TrimSpace(input.FailureMessage), "unknown"),
	}
	if trimmed := strings.TrimSpace(input.RunID); trimmed != "" {
		lines = append(lines, "Run: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.StepID); trimmed != "" {
		lines = append(lines, "Step: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.EntityID); trimmed != "" {
		lines = append(lines, "Entity: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.EventID); trimmed != "" {
		lines = append(lines, "Event: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.Family); trimmed != "" {
		lines = append(lines, "Family: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.ActionID); trimmed != "" {
		lines = append(lines, "Linked action: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.SourceQueueID); trimmed != "" {
		lines = append(lines, "Linked source queue id: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.SourceQueueKey); trimmed != "" {
		lines = append(lines, "Linked source queue key: "+trimmed)
	}
	if trimmed := strings.TrimSpace(input.RepairTensionID); trimmed != "" {
		lines = append(lines, "Repair tension: "+trimmed)
	}
	lines = append(lines, "Next action: inspect the linked rebase action and source queue, then restore the retry path manually if it is still valid.")
	return strings.Join(lines, "\n")
}

func mustJSONString(v any) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return strings.Trim(strings.TrimSpace(string(encoded)), "\"")
}

func (h *Handler) loadWorkspaceOperatorWriteAuthority(ctx context.Context, workspaceID, surface string) (sqlite.WorkspaceAuthorityRecord, *RPCError) {
	workspaceID = strings.TrimSpace(workspaceID)
	node, err := h.store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return sqlite.WorkspaceAuthorityRecord{}, rpcErrorFromStoreAuthority(err, surface)
	}
	authority, err := h.store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sqlite.WorkspaceAuthorityRecord{}, authorityRejectRPCError(&sqlite.AuthorityRejectError{
				RejectCode:              sqlite.AuthorityRejectMissing,
				RejectMessage:           "workspace authority is missing",
				WorkspaceID:             workspaceID,
				Scope:                   "workspace",
				ExpectedAuthorityNodeID: node.AuthorityNodeID,
				ReferenceAt:             time.Now().UTC().Format(time.RFC3339Nano),
				Retryable:               true,
				Surface:                 surface,
			}, surface)
		}
		return sqlite.WorkspaceAuthorityRecord{}, rpcErrorFromStoreAuthority(err, surface)
	}
	if strings.TrimSpace(authority.HolderAuthorityNodeID) != strings.TrimSpace(node.AuthorityNodeID) {
		return sqlite.WorkspaceAuthorityRecord{}, authorityRejectRPCError(&sqlite.AuthorityRejectError{
			RejectCode:               sqlite.AuthorityRejectStale,
			RejectMessage:            "workspace authority holder does not match local authority node",
			WorkspaceID:              authority.WorkspaceID,
			Scope:                    authority.Scope,
			HolderAuthorityNodeID:    authority.HolderAuthorityNodeID,
			ExpectedAuthorityNodeID:  node.AuthorityNodeID,
			Term:                     authority.Term,
			CommitWatermark:          authority.CommitWatermark,
			AppliedWatermark:         authority.AppliedWatermark,
			AuthorityStatus:          authority.Status,
			AuthorityRecordUpdatedAt: authority.UpdatedAt,
			ReferenceAt:              time.Now().UTC().Format(time.RFC3339Nano),
			Retryable:                true,
			Surface:                  surface,
		}, surface)
	}
	if strings.TrimSpace(authority.LeaseToken) == "" || authority.Term <= 0 {
		return sqlite.WorkspaceAuthorityRecord{}, authorityRejectRPCError(&sqlite.AuthorityRejectError{
			RejectCode:               sqlite.AuthorityRejectStale,
			RejectMessage:            "workspace authority row is incomplete for fenced write",
			WorkspaceID:              authority.WorkspaceID,
			Scope:                    authority.Scope,
			HolderAuthorityNodeID:    authority.HolderAuthorityNodeID,
			ExpectedAuthorityNodeID:  node.AuthorityNodeID,
			Term:                     authority.Term,
			CommitWatermark:          authority.CommitWatermark,
			AppliedWatermark:         authority.AppliedWatermark,
			AuthorityStatus:          authority.Status,
			AuthorityRecordUpdatedAt: authority.UpdatedAt,
			ReferenceAt:              time.Now().UTC().Format(time.RFC3339Nano),
			Retryable:                true,
			Surface:                  surface,
		}, surface)
	}
	leaseExpiresAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(authority.LeaseExpiresAt))
	if err != nil || !leaseExpiresAt.After(time.Now().UTC()) {
		return sqlite.WorkspaceAuthorityRecord{}, authorityRejectRPCError(&sqlite.AuthorityRejectError{
			RejectCode:               sqlite.AuthorityRejectLeaseExpired,
			RejectMessage:            "workspace authority lease is expired or invalid for fenced write",
			WorkspaceID:              authority.WorkspaceID,
			Scope:                    authority.Scope,
			HolderAuthorityNodeID:    authority.HolderAuthorityNodeID,
			ExpectedAuthorityNodeID:  node.AuthorityNodeID,
			Term:                     authority.Term,
			LeaseExpiresAt:           strings.TrimSpace(authority.LeaseExpiresAt),
			CommitWatermark:          authority.CommitWatermark,
			AppliedWatermark:         authority.AppliedWatermark,
			AuthorityStatus:          authority.Status,
			AuthorityRecordUpdatedAt: authority.UpdatedAt,
			ReferenceAt:              time.Now().UTC().Format(time.RFC3339Nano),
			Retryable:                true,
			Surface:                  surface,
		}, surface)
	}
	if authority.Status != sqlite.WorkspaceAuthorityStatusActive {
		rejectCode := sqlite.AuthorityRejectStale
		switch authority.Status {
		case sqlite.WorkspaceAuthorityStatusExpired:
			rejectCode = sqlite.AuthorityRejectLeaseExpired
		case sqlite.WorkspaceAuthorityStatusRejected:
			rejectCode = sqlite.AuthorityRejectRejoinRejected
		}
		return sqlite.WorkspaceAuthorityRecord{}, authorityRejectRPCError(&sqlite.AuthorityRejectError{
			RejectCode:               rejectCode,
			RejectMessage:            "workspace authority is not active for fenced write",
			WorkspaceID:              authority.WorkspaceID,
			Scope:                    authority.Scope,
			HolderAuthorityNodeID:    authority.HolderAuthorityNodeID,
			ExpectedAuthorityNodeID:  node.AuthorityNodeID,
			Term:                     authority.Term,
			LeaseExpiresAt:           strings.TrimSpace(authority.LeaseExpiresAt),
			CommitWatermark:          authority.CommitWatermark,
			AppliedWatermark:         authority.AppliedWatermark,
			AuthorityStatus:          authority.Status,
			AuthorityRecordUpdatedAt: authority.UpdatedAt,
			ReferenceAt:              time.Now().UTC().Format(time.RFC3339Nano),
			Retryable:                true,
			Surface:                  surface,
		}, surface)
	}
	return authority, nil
}

func (h *Handler) workspacePolicyPut(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspacePolicyPutParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SubjectType, "subject_type"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.CreatedBy, "created_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.CreatedBy, "created_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requirePolicySubjectPrincipal(ctx, p.WorkspaceID, p.SubjectType, p.SubjectID); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		PolicyID:              p.PolicyID,
		WorkspaceID:           p.WorkspaceID,
		SubjectType:           p.SubjectType,
		SubjectID:             p.SubjectID,
		Capability:            p.Capability,
		ToolID:                p.ToolID,
		Effect:                p.Effect,
		Reason:                p.Reason,
		CreatedBy:             p.CreatedBy,
		PromptContextEnvelope: h.capabilityPolicyPromptContextEnvelope(ctx, p.WorkspaceID, "workspace.policy.put"),
		PromptContextSurface:  "workspace.policy.put",
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.policy.put")
	}
	h.publishRuntimeEventRecordEnvelopeAs(event, "workspace.policy.put", event.PayloadJSON, record.SubjectType+":"+record.SubjectID+" -> "+record.Effect)
	return map[string]any{"policy": record, "status": "RECORDED"}, nil
}

func (h *Handler) workspacePolicyList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspacePolicyListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.SubjectID) != "" {
		if rpcErr := requirePolicySubjectMatchesPrincipal(principal, p.SubjectType, p.SubjectID); rpcErr != nil {
			return nil, rpcErr
		}
	} else if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "subject scope mismatch: subject_id is required for agent principal"}
	}
	items, err := h.store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{
		WorkspaceID: p.WorkspaceID,
		SubjectType: p.SubjectType,
		SubjectID:   p.SubjectID,
		Capability:  p.Capability,
		ToolID:      p.ToolID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"workspace_id": strings.TrimSpace(p.WorkspaceID), "items": items, "count": len(items)}, nil
}

func (h *Handler) workspacePolicyCheck(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspacePolicyCheckParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SubjectType, "subject_type"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SubjectID, "subject_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Capability, "capability"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requirePolicySubjectPrincipal(ctx, p.WorkspaceID, p.SubjectType, p.SubjectID); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
		WorkspaceID: p.WorkspaceID,
		SubjectType: p.SubjectType,
		SubjectID:   p.SubjectID,
		Capability:  p.Capability,
		ToolID:      p.ToolID,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{"check": result}, nil
}

func (h *Handler) workspaceControlCommandRequest(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceControlCommandRequestParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.CommandType, "command_type"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.RequestedBy, "requested_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.RequestedBy, "requested_by"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.RequestControlCommandWithEvent(ctx, sqlite.ControlCommandInput{
		CommandID:             p.CommandID,
		WorkspaceID:           p.WorkspaceID,
		CommandType:           p.CommandType,
		Scope:                 p.Scope,
		ProtoClusterID:        p.ProtoClusterID,
		TensionID:             p.TensionID,
		AgentID:               p.AgentID,
		TargetMode:            p.TargetMode,
		TTLSeconds:            p.TTLSeconds,
		Reason:                p.Reason,
		RequestedBy:           p.RequestedBy,
		ActorType:             p.ActorType,
		ParentRefs:            p.ParentRefs,
		PromptContextEnvelope: h.controlCommandPromptContextEnvelope(ctx, p.WorkspaceID, "workspace.control.command.request"),
		PromptContextSurface:  "workspace.control.command.request",
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.control.command.request")
	}
	h.publishRuntimeEventRecordEnvelopeAs(event, "workspace.control.command.request", event.PayloadJSON, record.CommandType)
	return map[string]any{"command": record, "status": "RECORDED"}, nil
}

func normalizeExternalGateType(gateType string) (string, string, bool) {
	switch strings.ToUpper(strings.TrimSpace(gateType)) {
	case "CREDENTIAL_AUTH":
		return "CREDENTIAL_AUTH", "BLOCKER", true
	case "PAYMENT_BILLING":
		return "PAYMENT_BILLING", "BLOCKER", true
	case "EXPLICIT_APPROVAL":
		return "EXPLICIT_APPROVAL", "DECISION", true
	default:
		return "", "", false
	}
}

func defaultExternalGateUrgency(gateType string) string {
	switch gateType {
	case "EXPLICIT_APPROVAL":
		return "NORMAL"
	default:
		return "HIGH"
	}
}

func externalGateQueueKey(gateType, requestKey string) string {
	gateType = strings.ToLower(strings.TrimSpace(gateType))
	requestKey = strings.TrimSpace(requestKey)
	if gateType == "" {
		gateType = "external_gate"
	}
	if requestKey == "" {
		return "external_gate:" + gateType
	}
	return "external_gate:" + gateType + ":" + requestKey
}

func (h *Handler) publishOperatorQueueEventRecord(event sqlite.RuntimeEventRecord, eventType string, record sqlite.OperatorQueueRecord) {
	h.publishRuntimeEventRecordEnvelopeAs(event, eventType, string(mustJSON(record)), firstNonEmpty(record.Summary, record.Title, record.QueueID))
}

func (h *Handler) publishKnowledgeClaimEventRecord(event sqlite.RuntimeEventRecord, eventType string, record sqlite.KnowledgeClaimRecord) {
	h.publishRuntimeEventRecordEnvelopeAs(event, eventType, string(mustJSON(record)), firstNonEmpty(record.Summary, record.Subject, record.ClaimID))
}

func isControlPlaneValidationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) {
		return true
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return false
	}
	return strings.Contains(msg, " is required") ||
		strings.Contains(msg, " is invalid") ||
		strings.Contains(msg, " has invalid value") ||
		strings.Contains(msg, "updated concurrently") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "requires archive flow") ||
		strings.Contains(msg, "reviewer scarcity is saturated") ||
		strings.Contains(msg, "task is not attached to workspace") ||
		strings.Contains(msg, "session not found") ||
		strings.Contains(msg, "operator queue item is not open") ||
		strings.Contains(msg, "knowledge claim is not in review workflow") ||
		strings.Contains(msg, "knowledge claim is archived") ||
		strings.Contains(msg, "prompt-convergence") ||
		strings.Contains(msg, "prompt evidence") ||
		strings.Contains(msg, "prompt_capability_evidence") ||
		strings.Contains(msg, "prompt_compiler_status") ||
		strings.Contains(msg, "projection_digest") ||
		strings.Contains(msg, "capability_snapshot_ref")
}
