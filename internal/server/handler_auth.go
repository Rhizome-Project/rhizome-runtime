package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

var errWorkspaceAccessDenied = errors.New("workspace access denied")

type workspaceAgentRegisterParams struct {
	WorkspaceID       string          `json:"workspace_id"`
	WorkspaceName     string          `json:"workspace_name"`
	WorkspacePassword string          `json:"workspace_password"`
	AgentID           string          `json:"agent_id"`
	AgentName         string          `json:"agent_name"`
	DisplayName       string          `json:"display_name"`
	GroupID           string          `json:"group_id,omitempty"`
	OwnerUserID       string          `json:"owner_user_id"`
	Role              string          `json:"role,omitempty"`
	ProtocolVersion   string          `json:"protocol_version,omitempty"`
	Capabilities      json.RawMessage `json:"capabilities,omitempty"`
	Summary           string          `json:"summary,omitempty"`
	Notes             string          `json:"notes,omitempty"`
	HostURL           string          `json:"host_url,omitempty"`
}

type workspaceHumanRegisterParams struct {
	WorkspaceID       string `json:"workspace_id"`
	WorkspaceName     string `json:"workspace_name"`
	WorkspacePassword string `json:"workspace_password"`
	Username          string `json:"username,omitempty"`
	LoginName         string `json:"login_name,omitempty"`
	DisplayName       string `json:"display_name,omitempty"`
	Name              string `json:"name,omitempty"`
	Password          string `json:"password"`
}

type workspaceHumanLoginParams struct {
	WorkspaceID   string `json:"workspace_id"`
	WorkspaceName string `json:"workspace_name"`
	Username      string `json:"username,omitempty"`
	LoginName     string `json:"login_name,omitempty"`
	Name          string `json:"name,omitempty"`
	Password      string `json:"password"`
}

type workspaceSecurityGetParams struct {
	WorkspaceID   string `json:"workspace_id"`
	WorkspaceName string `json:"workspace_name"`
}

type workspaceSecurityUpdateParams struct {
	WorkspaceID       string `json:"workspace_id"`
	WorkspaceName     string `json:"workspace_name"`
	WorkspacePassword string `json:"workspace_password"`
	Password          string `json:"password,omitempty"`
	Description       string `json:"description,omitempty"`
	UpdatedBy         string `json:"updated_by,omitempty"`
}

type workspaceSecurityPasswordUpdateParams = workspaceSecurityUpdateParams

type workspaceSecurityAuditListParams struct {
	WorkspaceID   string `json:"workspace_id"`
	WorkspaceName string `json:"workspace_name"`
	Limit         int    `json:"limit,omitempty"`
}

type workspaceHumanProfileUpdateParams struct {
	DisplayName    string `json:"display_name,omitempty"`
	Password       string `json:"password,omitempty"`
	TelegramUserID *int64 `json:"telegram_user_id,omitempty"`
}

type workspaceHumanSessionsRevokeParams struct {
	TokenID          string `json:"token_id,omitempty"`
	AllOtherSessions bool   `json:"all_other_sessions,omitempty"`
	Reason           string `json:"reason,omitempty"`
}

type workspaceAgentTokenRotateParams struct {
	AgentID string `json:"agent_id"`
}

type workspaceAgentUpdateParams struct {
	AgentID         string          `json:"agent_id"`
	DisplayName     string          `json:"display_name,omitempty"`
	Role            string          `json:"role,omitempty"`
	ProtocolVersion string          `json:"protocol_version,omitempty"`
	Capabilities    json.RawMessage `json:"capabilities,omitempty"`
}

func normalizedHumanUsername(p workspaceHumanRegisterParams) string {
	return firstNonEmptyString(strings.TrimSpace(p.Username), strings.TrimSpace(p.LoginName), strings.TrimSpace(p.Name))
}

func normalizedHumanDisplayName(p workspaceHumanRegisterParams) string {
	return firstNonEmptyString(strings.TrimSpace(p.DisplayName), strings.TrimSpace(p.Name), normalizedHumanUsername(p))
}

func normalizedHumanLoginName(p workspaceHumanLoginParams) string {
	return firstNonEmptyString(strings.TrimSpace(p.Username), strings.TrimSpace(p.LoginName), strings.TrimSpace(p.Name))
}

func (h *Handler) workspaceAuthAgentRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAgentRegisterParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	meta, _ := requestMetadataFromContext(ctx)
	workspace, err := h.resolveWorkspace(ctx, p.WorkspaceID, p.WorkspaceName)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	capabilities := flexParseCapabilities(p.Capabilities)
	result, err := h.store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       workspace.WorkspaceID,
		WorkspacePassword: strings.TrimSpace(p.WorkspacePassword),
		AgentID:           normalizedAgentID(firstNonEmptyString(strings.TrimSpace(p.AgentID), strings.TrimSpace(p.AgentName), strings.TrimSpace(p.DisplayName))),
		DisplayName:       firstNonEmptyString(strings.TrimSpace(p.DisplayName), strings.TrimSpace(p.AgentName)),
		OwnerUserID:       strings.TrimSpace(p.OwnerUserID),
		Role:              strings.TrimSpace(p.Role),
		ProtocolVersion:   strings.TrimSpace(p.ProtocolVersion),
		Capabilities:      capabilities,
		Summary:           firstNonEmptyString(strings.TrimSpace(p.Summary), strings.TrimSpace(p.Notes)),
		IPAddress:         meta.ClientIP,
		UserAgent:         meta.UserAgent,
	})
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	if _, err := h.store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspace.WorkspaceID,
		ActorType:   "system",
		ActorID:     "workspace.auth.agent.register",
	}); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	groupID := strings.TrimSpace(p.GroupID)
	switch {
	case groupID != "":
		if err := h.store.AssignAgentLimitGroup(ctx, workspace.WorkspaceID, result.Agent.AgentID, groupID, groupID); err != nil {
			return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
		}
	default:
		_ = h.store.EnsureAgentLimitGroup(ctx, workspace.WorkspaceID, result.Agent.AgentID, result.Agent.DisplayName)
	}
	return result, nil
}

func (h *Handler) workspaceAuthHumanRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceHumanRegisterParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	meta, _ := requestMetadataFromContext(ctx)
	workspace, err := h.resolveWorkspace(ctx, p.WorkspaceID, p.WorkspaceName)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	result, err := h.store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       workspace.WorkspaceID,
		WorkspacePassword: strings.TrimSpace(p.WorkspacePassword),
		Username:          normalizedHumanUsername(p),
		DisplayName:       normalizedHumanDisplayName(p),
		Password:          p.Password,
		IPAddress:         meta.ClientIP,
		UserAgent:         meta.UserAgent,
	})
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	return result, nil
}

func (h *Handler) workspaceAuthHumanLogin(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceHumanLoginParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	meta, _ := requestMetadataFromContext(ctx)
	workspace, err := h.resolveWorkspace(ctx, p.WorkspaceID, p.WorkspaceName)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	result, err := h.store.LoginHuman(ctx, sqlite.HumanLoginInput{
		WorkspaceID: workspace.WorkspaceID,
		Username:    normalizedHumanLoginName(p),
		Password:    p.Password,
		IPAddress:   meta.ClientIP,
		UserAgent:   meta.UserAgent,
	})
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	return result, nil
}

func (h *Handler) workspaceSecurityPasswordUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceSecurityUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspace, actorType, actorID, err := h.resolveSecurityContext(ctx, p.WorkspaceID, p.WorkspaceName, p.UpdatedBy)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	meta, _ := requestMetadataFromContext(ctx)
	settings, err := h.store.UpdateWorkspaceSecuritySettings(ctx, sqlite.WorkspaceSecuritySettingsInput{
		WorkspaceID:       workspace.WorkspaceID,
		Title:             workspace.Title,
		Description:       workspace.Description,
		WorkspacePassword: normalizedWorkspacePassword(p),
		UpdatedByType:     actorType,
		UpdatedByID:       actorID,
		IPAddress:         meta.ClientIP,
		UserAgent:         meta.UserAgent,
	})
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	return map[string]any{
		"workspace_id":   settings.WorkspaceID,
		"workspace_name": settings.Title,
		"status":         "UPDATED",
		"settings":       workspaceSecurityPayload(settings),
	}, nil
}

func (h *Handler) workspaceSecurityAuditList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceSecurityAuditListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspace, _, _, err := h.resolveSecurityContext(ctx, p.WorkspaceID, p.WorkspaceName, "")
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	items, err := h.listSecurityLogItems(ctx, workspace.WorkspaceID, p.Limit)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	return map[string]any{
		"items": items,
		"count": len(items),
	}, nil
}

func (h *Handler) workspaceAuthHumanProfileGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	if len(raw) > 0 && string(raw) != "null" {
		var p map[string]any
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	workspace, userID, err := h.resolveHumanProfileContext(ctx)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	profile, err := h.store.GetHumanProfile(ctx, workspace.WorkspaceID, userID)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	return h.humanProfilePayload(ctx, workspace, profile), nil
}

func (h *Handler) workspaceAuthHumanProfileUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceHumanProfileUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspace, userID, err := h.resolveHumanProfileContext(ctx)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	meta, _ := requestMetadataFromContext(ctx)
	profile, err := h.store.UpdateHumanProfile(ctx, sqlite.HumanProfileUpdateInput{
		WorkspaceID:    workspace.WorkspaceID,
		UserID:         userID,
		DisplayName:    strings.TrimSpace(p.DisplayName),
		Password:       p.Password,
		IPAddress:      meta.ClientIP,
		UserAgent:      meta.UserAgent,
		TelegramUserID: p.TelegramUserID,
	})
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	return h.humanProfilePayload(ctx, workspace, profile), nil
}

func (h *Handler) workspaceAuthHumanSessionsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	if len(raw) > 0 && string(raw) != "null" {
		var p map[string]any
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	workspace, principal, err := h.resolveHumanAuthContext(ctx)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	tokens, err := h.store.ListAccessTokens(ctx, workspace.WorkspaceID, "human", principal.PrincipalID, true, 20)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	items := make([]map[string]any, 0, len(tokens))
	for _, token := range tokens {
		items = append(items, authTokenPayload(token, token.TokenID == principal.TokenID))
	}
	return map[string]any{
		"sessions":         items,
		"count":            len(items),
		"current_token_id": principal.TokenID,
	}, nil
}

func (h *Handler) workspaceAuthHumanSessionsRevoke(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceHumanSessionsRevokeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspace, principal, err := h.resolveHumanAuthContext(ctx)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	meta, _ := requestMetadataFromContext(ctx)
	if p.AllOtherSessions {
		revoked, err := h.store.RevokeOtherAccessTokens(ctx, workspace.WorkspaceID, "human", principal.PrincipalID, principal.TokenID, p.Reason, principal.PrincipalType, principal.PrincipalID, meta.ClientIP, meta.UserAgent)
		if err != nil {
			return nil, h.rpcErrorFor(err)
		}
		return map[string]any{
			"status":        "REVOKED",
			"revoked_count": revoked,
		}, nil
	}
	tokenID := strings.TrimSpace(p.TokenID)
	if tokenID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "token_id is required"}
	}
	revoked, err := h.store.RevokeAccessToken(ctx, workspace.WorkspaceID, "human", principal.PrincipalID, tokenID, p.Reason, principal.PrincipalType, principal.PrincipalID, meta.ClientIP, meta.UserAgent)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	if !revoked {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "auth token not found"}
	}
	return map[string]any{
		"status":   "REVOKED",
		"token_id": tokenID,
	}, nil
}

func (h *Handler) workspaceAuthAgentTokenRotate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAgentTokenRotateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspace, principal, err := h.resolveHumanAuthContext(ctx)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	agent, err := h.store.GetAgent(ctx, workspace.WorkspaceID, p.AgentID)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	if strings.TrimSpace(agent.OwnerUserID) != principal.PrincipalID {
		return nil, h.rpcErrorFor(errWorkspaceAccessDenied)
	}
	meta, _ := requestMetadataFromContext(ctx)
	result, err := h.store.RotateAgentAccessToken(ctx, sqlite.AgentTokenRotateInput{
		WorkspaceID: workspace.WorkspaceID,
		AgentID:     agent.AgentID,
		ActorType:   principal.PrincipalType,
		ActorID:     principal.PrincipalID,
		IPAddress:   meta.ClientIP,
		UserAgent:   meta.UserAgent,
	})
	if err != nil {
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.auth.agent_token.rotate"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, h.rpcErrorFor(err)
	}
	liveAgentID := agent.AgentID
	h.publishRuntimeEventRecordAlias(result.MessageEvent, "agent.message", &liveAgentID, "", "Security notice for "+agent.AgentID)
	return map[string]any{
		"workspace_id":  result.WorkspaceID,
		"agent_id":      result.AgentID,
		"display_name":  result.DisplayName,
		"access_token":  result.Token,
		"token":         result.Token,
		"message_id":    result.MessageID,
		"delivery":      "agent_message",
		"rotation_mode": "grace_until_first_use",
	}, nil
}

func (h *Handler) workspaceAuthAgentUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAgentUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawFields); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspace, principal, err := h.resolveHumanAuthContext(ctx)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	agentID := strings.TrimSpace(p.AgentID)
	if agentID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "agent_id is required"}
	}
	agent, err := h.store.GetAgent(ctx, workspace.WorkspaceID, agentID)
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	if strings.TrimSpace(agent.OwnerUserID) != principal.PrincipalID {
		return nil, h.rpcErrorFor(errWorkspaceAccessDenied)
	}
	updated, err := h.store.UpdateAgentMetadataPreservingOmitted(ctx, sqlite.AgentMetadataPatchInput{
		WorkspaceID:     workspace.WorkspaceID,
		AgentID:         agent.AgentID,
		DisplayName:     optionalRegisterStringField(rawFields, "display_name", p.DisplayName),
		Role:            optionalRegisterStringField(rawFields, "role", p.Role),
		ProtocolVersion: optionalRegisterStringField(rawFields, "protocol_version", p.ProtocolVersion),
		Capabilities:    optionalRegisterCapabilitiesField(rawFields, "capabilities"),
		UpdatedBy:       principal.PrincipalID,
	})
	if err != nil {
		return nil, h.rpcErrorFor(err)
	}
	return map[string]any{
		"agent": updated,
	}, nil
}

func (h *Handler) ServeHumanRegisterHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceHumanRegisterParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, rpcErr := h.workspaceAuthHumanRegister(ctx, mustJSONRaw(p))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		auth, ok := result.(sqlite.HumanAuthResult)
		if !ok {
			writeAPIError(w, http.StatusInternalServerError, "unexpected auth response")
			return
		}
		workspace, err := h.resolveWorkspace(ctx, p.WorkspaceID, p.WorkspaceName)
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		writeAPIJSON(w, http.StatusOK, humanAuthPayload(workspace, auth))
	}
}

func (h *Handler) ServeHumanProfileGetHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		result, rpcErr := h.workspaceAuthHumanProfileGet(ctx, mustJSONRaw(map[string]any{}))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"profile": result,
		})
	}
}

func (h *Handler) ServeHumanProfileUpdateHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceHumanProfileUpdateParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, rpcErr := h.workspaceAuthHumanProfileUpdate(ctx, mustJSONRaw(p))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"profile": result,
		})
	}
}

func (h *Handler) ServeHumanSessionsListHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		result, rpcErr := h.workspaceAuthHumanSessionsList(ctx, mustJSONRaw(map[string]any{}))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		writeAPIJSON(w, http.StatusOK, result)
	}
}

func (h *Handler) ServeHumanSessionsRevokeHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceHumanSessionsRevokeParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, rpcErr := h.workspaceAuthHumanSessionsRevoke(ctx, mustJSONRaw(p))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		writeAPIJSON(w, http.StatusOK, result)
	}
}

func (h *Handler) ServeAgentTokenRotateHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceAgentTokenRotateParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, rpcErr := h.workspaceAuthAgentTokenRotate(ctx, mustJSONRaw(p))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		writeAPIJSON(w, http.StatusOK, result)
	}
}

func (h *Handler) ServeAgentUpdateHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceAgentUpdateParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, rpcErr := h.workspaceAuthAgentUpdate(ctx, mustJSONRaw(p))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		writeAPIJSON(w, http.StatusOK, result)
	}
}

func (h *Handler) ServeHumanLoginHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceHumanLoginParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, rpcErr := h.workspaceAuthHumanLogin(ctx, mustJSONRaw(p))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		auth, ok := result.(sqlite.HumanAuthResult)
		if !ok {
			writeAPIError(w, http.StatusInternalServerError, "unexpected auth response")
			return
		}
		workspace, err := h.resolveWorkspace(ctx, p.WorkspaceID, p.WorkspaceName)
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		writeAPIJSON(w, http.StatusOK, humanAuthPayload(workspace, auth))
	}
}

func (h *Handler) ServeAgentRegisterHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceAgentRegisterParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		result, rpcErr := h.workspaceAuthAgentRegister(ctx, mustJSONRaw(p))
		if rpcErr != nil {
			writeAPIError(w, statusForError(errors.New(rpcErr.Message)), rpcErr.Message)
			return
		}
		auth, ok := result.(sqlite.AgentRegistrationResult)
		if !ok {
			writeAPIError(w, http.StatusInternalServerError, "unexpected auth response")
			return
		}
		workspace, err := h.resolveWorkspace(ctx, p.WorkspaceID, p.WorkspaceName)
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		writeAPIJSON(w, http.StatusOK, agentAuthPayload(workspace, auth))
	}
}

func (h *Handler) ServeWorkspaceSecurityGetHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceSecurityGetParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		workspace, _, _, err := h.resolveSecurityContext(ctx, p.WorkspaceID, p.WorkspaceName, "")
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		settings, err := h.store.GetWorkspaceSecuritySettings(ctx, workspace.WorkspaceID)
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"settings": workspaceSecurityPayload(settings),
		})
	}
}

func (h *Handler) ServeWorkspaceSecurityUpdateHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceSecurityUpdateParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		workspace, actorType, actorID, err := h.resolveSecurityContext(ctx, p.WorkspaceID, "", "")
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		settings, err := h.store.UpdateWorkspaceSecuritySettings(ctx, sqlite.WorkspaceSecuritySettingsInput{
			WorkspaceID:       workspace.WorkspaceID,
			Title:             firstNonEmptyString(strings.TrimSpace(p.WorkspaceName), workspace.Title),
			Description:       strings.TrimSpace(p.Description),
			WorkspacePassword: normalizedWorkspacePassword(p),
			UpdatedByType:     actorType,
			UpdatedByID:       actorID,
			IPAddress:         requestClientIP(r),
			UserAgent:         strings.TrimSpace(r.UserAgent()),
		})
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"workspace_id":   settings.WorkspaceID,
			"workspace_name": settings.Title,
			"status":         "UPDATED",
			"settings":       workspaceSecurityPayload(settings),
		})
	}
}

func (h *Handler) ServeWorkspaceSecurityLogsHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := h.prepareHTTPContext(w, r)
		if !ok {
			return
		}
		var p workspaceSecurityAuditListParams
		if err := decodeJSONBody(r, &p); err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		workspace, _, _, err := h.resolveSecurityContext(ctx, p.WorkspaceID, p.WorkspaceName, "")
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		items, err := h.listSecurityLogItems(ctx, workspace.WorkspaceID, p.Limit)
		if err != nil {
			writeAPIError(w, statusForError(err), err.Error())
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"items": items,
			"count": len(items),
		})
	}
}

func (h *Handler) prepareHTTPContext(w http.ResponseWriter, r *http.Request) (context.Context, bool) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return nil, false
	}
	return context.WithValue(r.Context(), requestMetadataContextKey{}, RequestMetadata{
		ClientIP:  requestClientIP(r),
		UserAgent: strings.TrimSpace(r.UserAgent()),
	}), true
}

func (h *Handler) resolveWorkspace(ctx context.Context, workspaceID, workspaceName string) (sqlite.WorkspaceRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	workspaceName = strings.TrimSpace(workspaceName)

	// If the client provided the exact same string for both, it's likely a unified "workspace ref" input.
	if workspaceID != "" && workspaceID == workspaceName {
		workspace, err := h.store.GetWorkspace(ctx, workspaceID)
		if err == nil {
			return workspace, nil
		}
		if errors.Is(err, sqlite.ErrWorkspaceNotFound) {
			return h.store.GetWorkspaceByTitle(ctx, workspaceName)
		}
		return sqlite.WorkspaceRecord{}, err
	}

	if workspaceID != "" {
		workspace, err := h.store.GetWorkspace(ctx, workspaceID)
		if err != nil {
			return sqlite.WorkspaceRecord{}, err
		}
		if workspaceName != "" && !strings.EqualFold(workspaceName, workspace.Title) {
			return sqlite.WorkspaceRecord{}, sqlite.ErrWorkspaceRefAmbiguous
		}
		return workspace, nil
	}

	if workspaceName != "" {
		return h.store.GetWorkspaceByTitle(ctx, workspaceName)
	}
	return sqlite.WorkspaceRecord{}, errors.New("workspace_id or workspace_name is required")
}

func (h *Handler) resolveSecurityContext(ctx context.Context, workspaceID, workspaceName, fallbackActorID string) (sqlite.WorkspaceRecord, string, string, error) {
	if principal, ok := authPrincipalFromContext(ctx); ok {
		if strings.TrimSpace(principal.WorkspaceID) == "" {
			return sqlite.WorkspaceRecord{}, "", "", errWorkspaceAccessDenied
		}
		if principal.PrincipalType != "human" {
			return sqlite.WorkspaceRecord{}, "", "", errWorkspaceAccessDenied
		}
		workspace, err := h.store.GetWorkspace(ctx, principal.WorkspaceID)
		if err != nil {
			return sqlite.WorkspaceRecord{}, "", "", err
		}
		if requested := strings.TrimSpace(workspaceID); requested != "" && !strings.EqualFold(requested, workspace.WorkspaceID) {
			return sqlite.WorkspaceRecord{}, "", "", errWorkspaceAccessDenied
		}
		if requested := strings.TrimSpace(workspaceName); requested != "" && !strings.EqualFold(requested, workspace.WorkspaceID) && !strings.EqualFold(requested, workspace.Title) {
			return sqlite.WorkspaceRecord{}, "", "", errWorkspaceAccessDenied
		}
		return workspace, principal.PrincipalType, principal.PrincipalID, nil
	}

	return sqlite.WorkspaceRecord{}, "", "", errWorkspaceAccessDenied
}

func (h *Handler) resolveHumanAuthContext(ctx context.Context) (sqlite.WorkspaceRecord, AuthPrincipal, error) {
	principal, ok := authPrincipalFromContext(ctx)
	if !ok || strings.TrimSpace(principal.WorkspaceID) == "" || principal.PrincipalType != "human" {
		return sqlite.WorkspaceRecord{}, AuthPrincipal{}, errWorkspaceAccessDenied
	}
	workspace, err := h.store.GetWorkspace(ctx, principal.WorkspaceID)
	if err != nil {
		return sqlite.WorkspaceRecord{}, AuthPrincipal{}, err
	}
	return workspace, principal, nil
}

func (h *Handler) resolveHumanProfileContext(ctx context.Context) (sqlite.WorkspaceRecord, string, error) {
	workspace, principal, err := h.resolveHumanAuthContext(ctx)
	if err != nil {
		return sqlite.WorkspaceRecord{}, "", err
	}
	return workspace, principal.PrincipalID, nil
}

func (h *Handler) listSecurityLogItems(ctx context.Context, workspaceID string, limit int) ([]map[string]any, error) {
	events, err := h.store.ListSecurityEvents(ctx, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(events))
	for _, event := range events {
		items = append(items, securityLogPayload(event))
	}
	return items, nil
}

func authTokenPayload(token sqlite.AuthTokenRecord, current bool) map[string]any {
	return map[string]any{
		"token_id":       token.TokenID,
		"subject_type":   token.SubjectType,
		"subject_id":     token.SubjectID,
		"subject_label":  token.SubjectLabel,
		"token_prefix":   token.TokenPrefix,
		"issued_by":      token.IssuedBy,
		"issued_at":      token.IssuedAt,
		"last_used_at":   token.LastUsedAt,
		"revoked_at":     token.RevokedAt,
		"revoked_reason": token.RevokedReason,
		"metadata_json":  token.MetadataJSON,
		"current":        current,
	}
}

func humanProfileAgentLivenessStatus(agent sqlite.AgentRecord) string {
	if agent.IsOnline {
		return "ONLINE"
	}
	return "REGISTERED_OFFLINE"
}

func (h *Handler) humanProfilePayload(ctx context.Context, workspace sqlite.WorkspaceRecord, profile sqlite.HumanProfileRecord) map[string]any {
	agents := make([]map[string]any, 0, len(profile.Agents))
	for _, agent := range profile.Agents {
		item := map[string]any{
			"agent_id":         agent.AgentID,
			"workspace_id":     agent.WorkspaceID,
			"owner_user_id":    agent.OwnerUserID,
			"display_name":     agent.DisplayName,
			"role":             agent.Role,
			"status":           agent.Status,
			"protocol_version": agent.ProtocolVersion,
			"capabilities":     agent.Capabilities,
			"summary":          agent.Summary,
			"created_at":       agent.CreatedAt,
			"updated_at":       agent.UpdatedAt,
			"last_seen_at":     agent.LastSeenAt,
			"is_online":        agent.IsOnline,
			"liveness_status":  humanProfileAgentLivenessStatus(agent),
			"active_tasks":     agent.ActiveTasks,
			"current_session":  agent.CurrentSession,
		}
		if token, err := h.store.GetLatestAccessToken(ctx, workspace.WorkspaceID, "agent", agent.AgentID); err == nil {
			item["auth_token"] = authTokenPayload(token, false)
		}
		agents = append(agents, item)
	}
	return map[string]any{
		"workspace_id":     workspace.WorkspaceID,
		"workspace_name":   workspace.Title,
		"user_id":          profile.UserID,
		"username":         profile.Username,
		"display_name":     profile.DisplayName,
		"actor_id":         profile.UserID,
		"actor_type":       "human",
		"actor_name":       profile.DisplayName,
		"status":           profile.Status,
		"created_at":       profile.CreatedAt,
		"updated_at":       profile.UpdatedAt,
		"last_login_at":    profile.LastLoginAt,
		"telegram_user_id": profile.TelegramUserID,
		"agents":           agents,
		"agent_count":      profile.AgentCount,
	}
}

func (h *Handler) rpcErrorFor(err error) *RPCError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errWorkspaceAccessDenied):
		return &RPCError{Code: errCodePermissionDenied, Message: err.Error()}
	case errors.Is(err, sqlite.ErrWorkspaceNotFound):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case errors.Is(err, sqlite.ErrHumanUsernameConflict):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case errors.Is(err, sqlite.ErrHumanDisplayNameConflict):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case errors.Is(err, sqlite.ErrHumanTelegramUserIDConflict):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case errors.Is(err, sqlite.ErrHumanNotFound):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case errors.Is(err, sqlite.ErrAuthTokenNotFound):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case errors.Is(err, sqlite.ErrWorkspaceRefAmbiguous):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	default:
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
}

func decodeJSONBody(r *http.Request, dst any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return errors.New("failed to read request body")
	}
	if len(body) == 0 {
		return errors.New("empty request body")
	}
	if err := checkJSONDepth(body, 100); err != nil {
		return err
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return errors.New("invalid JSON")
	}
	return nil
}

func writeAPIJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeAPIJSON(w, status, map[string]any{
		"message": message,
	})
}

func humanAuthPayload(workspace sqlite.WorkspaceRecord, result sqlite.HumanAuthResult) map[string]any {
	return map[string]any{
		"workspace_id":   workspace.WorkspaceID,
		"workspace_name": workspace.Title,
		"user_id":        result.UserID,
		"username":       result.Username,
		"display_name":   result.DisplayName,
		"actor_id":       result.UserID,
		"actor_type":     "human",
		"actor_name":     result.DisplayName,
		"access_token":   result.Token,
		"token":          result.Token,
	}
}

func agentAuthPayload(workspace sqlite.WorkspaceRecord, result sqlite.AgentRegistrationResult) map[string]any {
	return map[string]any{
		"workspace_id":   workspace.WorkspaceID,
		"workspace_name": workspace.Title,
		"agent_id":       result.AgentID,
		"display_name":   result.DisplayName,
		"agent":          result.Agent,
		"actor_id":       result.AgentID,
		"actor_type":     "agent",
		"actor_name":     result.DisplayName,
		"access_token":   result.Token,
		"token":          result.Token,
	}
}

func workspaceSecurityPayload(settings sqlite.WorkspaceSecuritySettingsRecord) map[string]any {
	return map[string]any{
		"workspace_id":        settings.WorkspaceID,
		"workspace_name":      settings.Title,
		"title":               settings.Title,
		"description":         settings.Description,
		"password_updated_by": settings.PasswordUpdatedBy,
		"password_updated_at": settings.PasswordUpdatedAt,
		"human_count":         settings.HumanCount,
		"agent_count":         settings.AgentCount,
		"created_at":          settings.CreatedAt,
		"updated_at":          settings.UpdatedAt,
		"last_security_event": settings.LastSecurityEvent,
		"status":              "ready",
	}
}

func securityLogPayload(event sqlite.SecurityEventRecord) map[string]any {
	return map[string]any{
		"event_id":     event.EventID,
		"event_type":   event.EventType,
		"actor_type":   event.ActorType,
		"actor_id":     event.ActorID,
		"actor":        event.ActorID,
		"actor_name":   securityActorName(event),
		"subject_type": event.SubjectType,
		"subject_id":   event.SubjectID,
		"ip_address":   event.IPAddress,
		"user_agent":   event.UserAgent,
		"detail_json":  event.DetailJSON,
		"created_at":   event.CreatedAt,
		"status":       "ok",
		"success":      true,
	}
}

func securityActorName(event sqlite.SecurityEventRecord) string {
	if event.DetailJSON != "" {
		var detail map[string]any
		if err := json.Unmarshal([]byte(event.DetailJSON), &detail); err == nil {
			if displayName, _ := detail["display_name"].(string); strings.TrimSpace(displayName) != "" {
				return strings.TrimSpace(displayName)
			}
		}
	}
	return firstNonEmptyString(strings.TrimSpace(event.ActorID), strings.TrimSpace(event.ActorType), "system")
}

func statusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, errWorkspaceAccessDenied):
		return http.StatusForbidden
	case errors.Is(err, sqlite.ErrWorkspaceNotFound):
		return http.StatusNotFound
	case errors.Is(err, sqlite.ErrWorkspaceRefAmbiguous):
		return http.StatusBadRequest
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "invalid workspace password"),
		strings.Contains(message, "invalid human credentials"),
		strings.Contains(message, "invalid token"):
		return http.StatusUnauthorized
	case strings.Contains(message, "access denied"),
		strings.Contains(message, "forbidden"):
		return http.StatusForbidden
	case strings.Contains(message, "not found"):
		return http.StatusNotFound
	case strings.Contains(message, "required"),
		strings.Contains(message, "invalid"),
		strings.Contains(message, "already"),
		strings.Contains(message, "ambiguous"):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func normalizedWorkspacePassword(p workspaceSecurityUpdateParams) string {
	return firstNonEmptyString(strings.TrimSpace(p.WorkspacePassword), strings.TrimSpace(p.Password))
}

func normalizedAgentID(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "agent"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case !lastDash:
			b.WriteByte('-')
			lastDash = true
		}
	}
	id := strings.Trim(b.String(), "-")
	if id == "" {
		return "agent"
	}
	return id
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func mustJSONRaw(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}
