package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceTensionRefreshParams struct {
	WorkspaceID    string `json:"workspace_id"`
	ActorID        string `json:"actor_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	ClusterLimit   int    `json:"cluster_limit,omitempty"`
}

type workspaceTensionListParams struct {
	WorkspaceID    string `json:"workspace_id"`
	TensionType    string `json:"tension_type,omitempty"`
	LifecycleState string `json:"lifecycle_state,omitempty"`
	ReviewStatus   string `json:"review_status,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type workspaceTensionFrontierParams struct {
	WorkspaceID    string `json:"workspace_id"`
	TensionType    string `json:"tension_type,omitempty"`
	LifecycleState string `json:"lifecycle_state,omitempty"`
	ReviewStatus   string `json:"review_status,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type workspaceTensionGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	TensionID   string `json:"tension_id"`
}

type workspaceTensionLifecycleParams struct {
	WorkspaceID string `json:"workspace_id"`
	TensionID   string `json:"tension_id"`
	ActorID     string `json:"actor_id"`
	Reason      string `json:"reason,omitempty"`
}

type workspaceTensionLifecycleUpdateParams struct {
	WorkspaceID    string `json:"workspace_id"`
	TensionID      string `json:"tension_id"`
	LifecycleState string `json:"lifecycle_state"`
	UpdatedBy      string `json:"updated_by"`
	Reason         string `json:"reason,omitempty"`
}

type workspaceTensionMutationParams = workspaceTensionLifecycleParams

type workspaceTensionCondenseParams struct {
	WorkspaceID string `json:"workspace_id"`
	ActorID     string `json:"actor_id"`
	Reason      string `json:"reason,omitempty"`
}

func (h *Handler) workspaceTensionRefresh(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionRefreshParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID:                p.WorkspaceID,
		ActorID:                    p.ActorID,
		ProtoClusterID:             p.ProtoClusterID,
		Limit:                      p.Limit,
		ClusterLimit:               p.ClusterLimit,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.refresh", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "workspace.tension.refresh",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.tension.refresh"); rpcErr != nil {
			return nil, rpcErr
		}
		if isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	for _, event := range result.Events {
		h.publishTensionRuntimeEvent(event)
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(result.WorkspaceID),
		"time_authority": result.TimeAuthority,
		"refresh":        result,
	}, nil
}

func (h *Handler) workspaceTensionList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID:    p.WorkspaceID,
		TensionType:    p.TensionType,
		LifecycleState: p.LifecycleState,
		ReviewStatus:   p.ReviewStatus,
		ProtoClusterID: p.ProtoClusterID,
		TaskID:         p.TaskID,
		AgentID:        p.AgentID,
		Limit:          p.Limit,
	})
	if err != nil {
		if isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, err := h.store.GetWorkspaceTimeAuthority(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceTensionGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TensionID, "tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	detail, err := h.store.GetTension(ctx, p.WorkspaceID, p.TensionID)
	if err != nil {
		if isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return detail, nil
}

func (h *Handler) workspaceTensionFrontier(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionFrontierParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListTensionFrontier(ctx, sqlite.TensionFilter{
		WorkspaceID:    p.WorkspaceID,
		TensionType:    p.TensionType,
		LifecycleState: p.LifecycleState,
		ReviewStatus:   p.ReviewStatus,
		ProtoClusterID: p.ProtoClusterID,
		TaskID:         p.TaskID,
		AgentID:        p.AgentID,
		Limit:          p.Limit,
	})
	if err != nil {
		if isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, err := h.store.GetWorkspaceTimeAuthority(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceTensionConfirm(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceTensionMutate(ctx, raw, "workspace.tension.confirm", func(ctx context.Context, input sqlite.TensionMutationInput) (sqlite.TensionMutationResult, error) {
		return h.store.ConfirmTension(ctx, input)
	})
}

func (h *Handler) workspaceTensionDiscard(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceTensionMutate(ctx, raw, "workspace.tension.discard", func(ctx context.Context, input sqlite.TensionMutationInput) (sqlite.TensionMutationResult, error) {
		return h.store.DiscardTension(ctx, input)
	})
}

func (h *Handler) workspaceTensionArchive(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceTensionMutate(ctx, raw, "workspace.tension.archive", func(ctx context.Context, input sqlite.TensionMutationInput) (sqlite.TensionMutationResult, error) {
		return h.store.ArchiveTension(ctx, input)
	})
}

func (h *Handler) workspaceTensionResolve(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceTensionMutate(ctx, raw, "workspace.tension.resolve", func(ctx context.Context, input sqlite.TensionMutationInput) (sqlite.TensionMutationResult, error) {
		return h.store.ResolveTension(ctx, input)
	})
}

func (h *Handler) workspaceTensionDormant(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.workspaceTensionMutate(ctx, raw, "workspace.tension.dormant", func(ctx context.Context, input sqlite.TensionMutationInput) (sqlite.TensionMutationResult, error) {
		return h.store.DormantTension(ctx, input)
	})
}

func (h *Handler) workspaceTensionMutate(ctx context.Context, raw json.RawMessage, surface string, fn func(context.Context, sqlite.TensionMutationInput) (sqlite.TensionMutationResult, error)) (any, *RPCError) {
	var p workspaceTensionLifecycleParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TensionID, "tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	result, err := fn(ctx, sqlite.TensionMutationInput{
		WorkspaceID:                p.WorkspaceID,
		TensionID:                  p.TensionID,
		ActorID:                    p.ActorID,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope(surface, "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       surface,
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, surface); rpcErr != nil {
			return nil, rpcErr
		}
		if isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishTensionRuntimeEvent(result.Event)
	return map[string]any{
		"tension": result.Tension,
		"event":   result.Event,
	}, nil
}

func (h *Handler) publishTensionRuntimeEvent(event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecord(event)
}

func isTensionValidationError(err error) bool {
	if err == nil {
		return false
	}
	if isControlPlaneValidationError(err) {
		return true
	}
	msg := strings.TrimSpace(err.Error())
	return strings.Contains(msg, "tension is not archived") ||
		strings.Contains(msg, "scan tension record")
}

type workspaceTensionDependencyParams struct {
	WorkspaceID        string `json:"workspace_id"`
	TensionID          string `json:"tension_id"`
	DependsOnTensionID string `json:"depends_on_tension_id"`
	DependencyType     string `json:"dependency_type,omitempty"`
	ActorID            string `json:"actor_id"`
	Reason             string `json:"reason,omitempty"`
}

func (h *Handler) workspaceTensionAddDependency(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionDependencyParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TensionID, "tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.DependsOnTensionID, "depends_on_tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.AddTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                p.WorkspaceID,
		TensionID:                  p.TensionID,
		DependsOnTensionID:         p.DependsOnTensionID,
		DependencyType:             p.DependencyType,
		ActorID:                    p.ActorID,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.add.dependency", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "workspace.tension.add.dependency",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.tension.add.dependency"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if result.Changed {
		h.publishTensionRuntimeEvent(result.Event)
	}
	return map[string]any{"success": true, "changed": result.Changed, "edge": result.Edge, "event": result.Event}, nil
}

func (h *Handler) workspaceTensionRemoveDependency(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionDependencyParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TensionID, "tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.DependsOnTensionID, "depends_on_tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.RemoveTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                p.WorkspaceID,
		TensionID:                  p.TensionID,
		DependsOnTensionID:         p.DependsOnTensionID,
		ActorID:                    p.ActorID,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.remove.dependency", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "workspace.tension.remove.dependency",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.tension.remove.dependency"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if result.Changed {
		h.publishTensionRuntimeEvent(result.Event)
	}
	return map[string]any{"success": true, "changed": result.Changed, "edge": result.Edge, "event": result.Event}, nil
}

func (h *Handler) workspaceTensionCondense(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionCondenseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.RefreshMetaTensionsWithContext(ctx, sqlite.TensionCondenseInput{
		WorkspaceID:                p.WorkspaceID,
		ActorID:                    p.ActorID,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.tension.condense"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	for _, event := range result.Events {
		h.publishTensionRuntimeEvent(event)
	}
	return map[string]any{"success": true, "result": result, "events": result.Events}, nil
}

type workspaceTensionAttachableParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
}

func (h *Handler) workspaceTensionListAttachable(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionAttachableParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	scored, err := h.store.ListAgentAvailableTensionsScored(ctx, p.WorkspaceID, p.AgentID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"agent_id":     p.AgentID,
		"items":        scored,
		"count":        len(scored),
	}, nil
}

type workspaceTensionAgentActionParams struct {
	WorkspaceID      string `json:"workspace_id"`
	TensionID        string `json:"tension_id"`
	AgentID          string `json:"agent_id"`
	ActorID          string `json:"actor_id"`
	SuccessCriterion string `json:"success_criterion,omitempty"`
	Reason           string `json:"reason,omitempty"`
}

func (h *Handler) workspaceTensionAttachAgent(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionAgentActionParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TensionID, "tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}

	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if principal.PrincipalType == "agent" {
		if strings.TrimSpace(p.AgentID) != principal.PrincipalID {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "agent_id spoofing denied"}
		}
	}

	result, err := h.store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                p.WorkspaceID,
		TensionID:                  p.TensionID,
		AgentID:                    p.AgentID,
		ActorID:                    p.ActorID,
		SuccessCriterion:           p.SuccessCriterion,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.tension.agent.attach"); rpcErr != nil {
			return nil, rpcErr
		}
		if coalitionClientStateError(err) || isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("failed to attach to coalition: %v", err)}
	}
	if result.Changed {
		h.publishTensionRuntimeEvent(result.Event)
	}
	return map[string]any{"success": true, "changed": result.Changed, "coalition_id": result.Coalition.CoalitionID, "coalition": result.Coalition, "event": result.Event}, nil
}

type workspaceTensionDetachParams struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	Reason      string `json:"reason,omitempty"`
}

func (h *Handler) workspaceTensionDetachAgent(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionDetachParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.CoalitionID, "coalition_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}

	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if principal.PrincipalType == "agent" {
		if strings.TrimSpace(p.AgentID) != principal.PrincipalID {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "agent_id spoofing denied"}
		}
	}

	result, err := h.store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                p.WorkspaceID,
		CoalitionID:                p.CoalitionID,
		AgentID:                    p.AgentID,
		ActorID:                    p.ActorID,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.detach", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "workspace.tension.agent.detach",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.tension.agent.detach"); rpcErr != nil {
			return nil, rpcErr
		}
		if coalitionClientStateError(err) || isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("failed to detach from coalition: %v", err)}
	}
	if result.Changed {
		h.publishTensionRuntimeEvent(result.Event)
	}
	return map[string]any{"success": true, "changed": result.Changed, "coalition": result.Coalition, "event": result.Event}, nil
}

func (h *Handler) workspaceTensionLifecycleUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTensionLifecycleUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TensionID, "tension_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.LifecycleState, "lifecycle_state"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.UpdatedBy, "updated_by"); rpcErr != nil {
		return nil, rpcErr
	}

	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.UpdatedBy, "updated_by")
	if rpcErr != nil {
		return nil, rpcErr
	}

	state := strings.ToUpper(strings.TrimSpace(p.LifecycleState))
	var res sqlite.TensionMutationResult
	var err error

	input := sqlite.TensionMutationInput{
		WorkspaceID:                       p.WorkspaceID,
		TensionID:                         p.TensionID,
		ActorID:                           p.UpdatedBy,
		Reason:                            p.Reason,
		PromptContextEnvelope:             sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.lifecycle.update", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:              "workspace.tension.lifecycle.update",
		PromptContextPrincipalType:        principal.PrincipalType,
		PromptContextPrincipalID:          principal.PrincipalID,
		PromptContextAllowLifecycleUpdate: true,
	}

	switch state {
	case "RESOLVED":
		res, err = h.store.ResolveTension(ctx, input)
	case "DISCARDED":
		res, err = h.store.DiscardTension(ctx, input)
	case "ARCHIVED":
		res, err = h.store.ArchiveTension(ctx, input)
	default:
		return nil, &RPCError{Code: errCodeInvalidParams, Message: fmt.Sprintf("unsupported lifecycle state update: %s", state)}
	}

	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.tension.lifecycle.update"); rpcErr != nil {
			return nil, rpcErr
		}
		if isTensionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	if res.Event.EventID != "" {
		h.publishTensionRuntimeEvent(res.Event)
	}
	return map[string]any{"success": true}, nil
}
