package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const (
	coalitionRequestedRoleSemantics = "advisory_system_normalized"
	coalitionKickReasonSemantics    = "operator_note_no_policy_effect"
)

type coalitionOfferParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	Role        string `json:"role"`
}

func (h *Handler) coalitionOffer(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p coalitionOfferParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	if p.WorkspaceID == "" || p.TaskID == "" || p.AgentID == "" || p.ActorID == "" || p.Role == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id, task_id, agent_id, actor_id, and role are required"}
	}
	if strings.TrimSpace(p.ActorID) != strings.TrimSpace(p.AgentID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "actor_id must match agent_id for coalition.offer"}
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	result, err := h.store.CoalitionJoinOfferWithContext(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID:                p.WorkspaceID,
		TaskID:                     p.TaskID,
		AgentID:                    p.AgentID,
		ActorID:                    p.ActorID,
		Role:                       p.Role,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("coalition.offer", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "coalition.offer",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		return nil, coalitionRPCError("offer coalition", err)
	}
	publishCoalitionMutationEvent(h, result)

	return result, nil
}

type coalitionLeaveParams struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	Reason      string `json:"reason"`
}

func (h *Handler) coalitionLeave(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p coalitionLeaveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	if p.WorkspaceID == "" || p.CoalitionID == "" || p.AgentID == "" || p.ActorID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id, coalition_id, agent_id, and actor_id are required"}
	}
	if strings.TrimSpace(p.ActorID) != strings.TrimSpace(p.AgentID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "actor_id must match agent_id for coalition.leave"}
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	result, err := h.store.CoalitionJoinLeaveWithContext(ctx, sqlite.CoalitionJoinLeaveInput{
		WorkspaceID:                p.WorkspaceID,
		CoalitionID:                p.CoalitionID,
		AgentID:                    p.AgentID,
		ActorID:                    p.ActorID,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("coalition.leave", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "coalition.leave",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		return nil, coalitionRPCError("leave coalition", err)
	}
	publishCoalitionMutationEvent(h, result)

	return result, nil
}

type coalitionStatusParams struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id,omitempty"`
}

func (h *Handler) coalitionStatus(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p coalitionStatusParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	if p.WorkspaceID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id is required"}
	}

	res, err := h.store.GetCoalitionByWorkspace(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: "failed to get coalition status", Details: err.Error()}
	}

	if p.CoalitionID != "" {
		coalitions, ok := res["coalitions"].([]sqlite.WorkspaceCoalition)
		if !ok {
			return nil, &RPCError{Code: errCodeInternal, Message: "failed to decode coalition status"}
		}
		filtered := make([]sqlite.WorkspaceCoalition, 0, 1)
		for _, coalition := range coalitions {
			if coalition.CoalitionID == p.CoalitionID {
				filtered = append(filtered, coalition)
				break
			}
		}
		res["coalitions"] = filtered
		if integrity, ok := res["integrity"].(sqlite.WorkspaceCoalitionIntegrityReport); ok {
			integrity.Items = filterCoalitionIntegrityItems(integrity.Items, p.CoalitionID)
			integrity.IssueCodes = filterCoalitionIntegrityIssueCodes(integrity.Items)
			if len(integrity.Items) == 0 {
				integrity.State = sqlite.WorkspaceCoalitionIntegrityCurrent
				integrity.Summary = "no coalition integrity drift detected for the requested coalition"
			} else {
				integrity.State, integrity.Summary = classifyCoalitionIntegrityReportForHandler(integrity.Items)
			}
			res["integrity"] = integrity
		}
	}

	return res, nil
}

func filterCoalitionIntegrityItems(items []sqlite.WorkspaceCoalitionIntegrityItem, coalitionID string) []sqlite.WorkspaceCoalitionIntegrityItem {
	coalitionID = strings.TrimSpace(coalitionID)
	if coalitionID == "" || len(items) == 0 {
		return items
	}
	filtered := make([]sqlite.WorkspaceCoalitionIntegrityItem, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.CanonicalCoalitionID) == coalitionID {
			filtered = append(filtered, item)
			continue
		}
		for _, shadowID := range item.ShadowCoalitionIDs {
			if strings.TrimSpace(shadowID) == coalitionID {
				filtered = append(filtered, item)
				break
			}
		}
	}
	return filtered
}

func filterCoalitionIntegrityIssueCodes(items []sqlite.WorkspaceCoalitionIntegrityItem) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, item := range items {
		for _, code := range item.IssueCodes {
			code = strings.TrimSpace(code)
			if code == "" {
				continue
			}
			if _, ok := seen[code]; ok {
				continue
			}
			seen[code] = struct{}{}
			out = append(out, code)
		}
	}
	return out
}

func classifyCoalitionIntegrityReportForHandler(items []sqlite.WorkspaceCoalitionIntegrityItem) (string, string) {
	hasDrift := false
	hasUnknown := false
	for _, item := range items {
		switch strings.TrimSpace(item.State) {
		case sqlite.WorkspaceCoalitionIntegrityDrift:
			hasDrift = true
		case sqlite.WorkspaceCoalitionIntegrityUnknown:
			hasUnknown = true
		}
	}
	switch {
	case hasDrift:
		return sqlite.WorkspaceCoalitionIntegrityDrift, "requested coalition integrity drift detected"
	case hasUnknown:
		return sqlite.WorkspaceCoalitionIntegrityUnknown, "requested coalition integrity is only partially trustworthy"
	default:
		return sqlite.WorkspaceCoalitionIntegrityCurrent, "requested coalition integrity invariants hold"
	}
}

type coalitionSeekParams struct {
	WorkspaceID    string   `json:"workspace_id"`
	TaskID         string   `json:"task_id,omitempty"`
	AgentID        string   `json:"agent_id"`
	Role           string   `json:"role,omitempty"`
	RequiredSkills []string `json:"required_skills,omitempty"`
	Reason         string   `json:"reason,omitempty"`
	Limit          int      `json:"limit"`
}

func (h *Handler) coalitionSeek(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p coalitionSeekParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	if p.WorkspaceID == "" || p.AgentID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id and agent_id are required"}
	}

	limit := p.Limit
	if limit <= 0 {
		limit = 20
	}

	res, err := h.store.CoalitionSeekQuery(ctx, sqlite.CoalitionSeekQueryInput{
		WorkspaceID:    p.WorkspaceID,
		TaskID:         p.TaskID,
		AgentID:        p.AgentID,
		Role:           p.Role,
		RequiredSkills: append([]string{}, p.RequiredSkills...),
		Reason:         p.Reason,
		Limit:          limit,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: "failed to seek coalitions", Details: err.Error()}
	}

	return res, nil
}

type coalitionInviteParams struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	ActorID     string `json:"actor_id"`
	AgentID     string `json:"agent_id,omitempty"`
	TargetID    string `json:"target_id,omitempty"`
	InvitedBy   string `json:"invited_by,omitempty"`
	Role        string `json:"role,omitempty"`
}

func (h *Handler) coalitionInvite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p coalitionInviteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	targetID := p.TargetID
	if targetID == "" {
		targetID = p.AgentID
	}
	invitedBy := p.InvitedBy
	if invitedBy == "" {
		invitedBy = p.AgentID
	}
	if p.WorkspaceID == "" || p.CoalitionID == "" || p.ActorID == "" || targetID == "" || invitedBy == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id, coalition_id, actor_id, inviter, and target are required"}
	}
	if strings.TrimSpace(p.ActorID) != strings.TrimSpace(invitedBy) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "actor_id must match inviter for coalition.invite"}
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	result, err := h.store.CoalitionInviteEventWithContext(ctx, sqlite.CoalitionInviteEventInput{
		WorkspaceID:                p.WorkspaceID,
		CoalitionID:                p.CoalitionID,
		AgentID:                    targetID,
		InvitedBy:                  invitedBy,
		Role:                       p.Role,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("coalition.invite", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "coalition.invite",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		return nil, coalitionRPCError("invite to coalition", err)
	}
	publishCoalitionMutationEvent(h, result)

	return result, nil
}

type coalitionKickParams struct {
	WorkspaceID string `json:"workspace_id"`
	CoalitionID string `json:"coalition_id"`
	ActorID     string `json:"actor_id"`
	AgentID     string `json:"agent_id,omitempty"`
	TargetID    string `json:"target_id,omitempty"`
	KickedBy    string `json:"kicked_by,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

func (h *Handler) coalitionKick(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p coalitionKickParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	targetID := p.TargetID
	if targetID == "" {
		targetID = p.AgentID
	}
	kickedBy := p.KickedBy
	if kickedBy == "" {
		kickedBy = p.AgentID
	}
	if p.WorkspaceID == "" || p.CoalitionID == "" || p.ActorID == "" || targetID == "" || kickedBy == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id, coalition_id, actor_id, kicker, and target are required"}
	}
	if strings.TrimSpace(p.ActorID) != strings.TrimSpace(kickedBy) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "actor_id must match kicker for coalition.kick"}
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.ActorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	result, err := h.store.CoalitionKickEventWithContext(ctx, sqlite.CoalitionKickEventInput{
		WorkspaceID:                p.WorkspaceID,
		CoalitionID:                p.CoalitionID,
		AgentID:                    targetID,
		KickedBy:                   kickedBy,
		Reason:                     p.Reason,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("coalition.kick", "server_rpc", p.WorkspaceID, principal.PrincipalType, principal.PrincipalID),
		PromptContextSurface:       "coalition.kick",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		return nil, coalitionRPCError("kick from coalition", err)
	}
	publishCoalitionMutationEvent(h, result)

	return result, nil
}

func publishCoalitionMutationEvent(h *Handler, result map[string]any) {
	changed, _ := result["changed"].(bool)
	if !changed {
		return
	}
	event, ok := result["event"].(sqlite.RuntimeEventRecord)
	if !ok || strings.TrimSpace(event.EventID) == "" {
		return
	}
	h.publishTensionRuntimeEvent(event)
}

func coalitionRPCError(action string, err error) *RPCError {
	if coalitionClientStateError(err) {
		return &RPCError{Code: errCodeInvalidParams, Message: "failed to " + action, Details: err.Error()}
	}
	return &RPCError{Code: errCodeInternal, Message: "failed to " + action, Details: err.Error()}
}

func coalitionClientStateError(err error) bool {
	return errors.Is(err, sqlite.ErrCoalitionExpired) ||
		errors.Is(err, sqlite.ErrCoalitionActorNotMember) ||
		errors.Is(err, sqlite.ErrCoalitionTargetNotMember) ||
		errors.Is(err, sqlite.ErrCoalitionSelfKick) ||
		errors.Is(err, sqlite.ErrCoalitionTargetNotFound) ||
		errors.Is(err, sqlite.ErrCoalitionTargetAmbiguous) ||
		errors.Is(err, sqlite.ErrCoalitionMinimumTenureNotMet) ||
		errors.Is(err, sqlite.ErrCoalitionCapacityReached) ||
		errors.Is(err, sqlite.ErrCoalitionAttachmentRejected)
}
