package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryGraphListParams struct {
	WorkspaceID     string `json:"workspace_id"`
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

type workspaceMemoryGraphGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	MemoryID    string `json:"memory_id"`
}

type workspaceMemoryGraphSyncParams struct {
	WorkspaceID string `json:"workspace_id"`
}

type workspaceMemoryGraphRepairParams struct {
	WorkspaceID string `json:"workspace_id"`
	MemoryID    string `json:"memory_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceMemoryGraphAtlasParams struct {
	WorkspaceID     string  `json:"workspace_id"`
	CenterMemoryID  string  `json:"center_memory_id,omitempty"`
	Query           string  `json:"query,omitempty"`
	MemoryType      string  `json:"memory_type,omitempty"`
	MemoryLayer     string  `json:"memory_layer,omitempty"`
	Visibility      string  `json:"visibility,omitempty"`
	EpistemicStatus string  `json:"epistemic_status,omitempty"`
	LifecycleState  string  `json:"lifecycle_state,omitempty"`
	OriginKind      string  `json:"origin_kind,omitempty"`
	IncludeAnchors  bool    `json:"include_anchors,omitempty"`
	IncludeArchived bool    `json:"include_archived,omitempty"`
	CanonicalOnly   bool    `json:"canonical_only,omitempty"`
	Depth           int     `json:"depth,omitempty"`
	LimitNodes      int     `json:"limit_nodes,omitempty"`
	LimitEdges      int     `json:"limit_edges,omitempty"`
	MinImportance   float64 `json:"min_importance,omitempty"`
	MinActivation   float64 `json:"min_activation,omitempty"`
}

func (h *Handler) workspaceMemoryGraphList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryGraphListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListMemoryGraphNodes(ctx, sqlite.MemoryGraphNodeFilter{
		WorkspaceID:     p.WorkspaceID,
		MemoryType:      p.MemoryType,
		MemoryLayer:     p.MemoryLayer,
		Visibility:      p.Visibility,
		EpistemicStatus: p.EpistemicStatus,
		LifecycleState:  p.LifecycleState,
		OriginKind:      p.OriginKind,
		OriginID:        p.OriginID,
		SourceKind:      p.SourceKind,
		AgentID:         p.AgentID,
		SessionID:       p.SessionID,
		TaskID:          p.TaskID,
		IncludeArchived: p.IncludeArchived,
		Limit:           p.Limit,
	})
	if err != nil {
		if isMemoryGraphValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	boundary, err := h.store.MemoryGraphBoundaryContractForWorkspace(ctx, p.WorkspaceID, p.OriginKind)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, rpcErr := h.loadWorkspaceTimeAuthority(ctx, p.WorkspaceID, "workspace.memory.graph.list")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":      p.WorkspaceID,
		"time_authority":    authority,
		"boundary_contract": boundary,
		"items":             items,
		"count":             len(items),
	}, nil
}

func (h *Handler) workspaceMemoryGraphGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryGraphGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.MemoryID, "memory_id"); rpcErr != nil {
		return nil, rpcErr
	}
	detail, err := h.store.GetMemoryGraphNode(ctx, p.WorkspaceID, p.MemoryID)
	if err != nil {
		if isMemoryGraphValidationError(err) {
			rpcErr := &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
			if details, detailsErr := h.memoryGraphProjectionMissingDetails(ctx, p.WorkspaceID, p.MemoryID); detailsErr == nil && details != nil {
				rpcErr.Details = details
			}
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return detail, nil
}

func (h *Handler) workspaceMemoryGraphSync(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryGraphSyncParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.SyncMemoryGraphWorkspace(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"status":            "SYNCED",
		"boundary_contract": sqlite.DefaultMemoryShapeBoundaryContract(),
		"sync":              result,
	}, nil
}

func (h *Handler) workspaceMemoryGraphRepair(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryGraphRepairParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	filter := sqlite.WorkspaceMemoryProjectionRepairFilter{
		WorkspaceID: p.WorkspaceID,
		Limit:       p.Limit,
	}
	if trimmed := strings.TrimSpace(p.MemoryID); trimmed != "" {
		filter.MemoryIDs = []string{trimmed}
		if filter.Limit <= 0 {
			filter.Limit = 1
		}
	}
	result, err := h.store.RepairWorkspaceMemoryProjectionWorkspace(ctx, filter)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"status":            "REPAIRED",
		"boundary_contract": sqlite.DefaultMemoryShapeBoundaryContract(),
		"repair":            result,
	}, nil
}

func (h *Handler) workspaceMemoryGraphAtlas(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryGraphAtlasParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	if trimmed := strings.TrimSpace(p.MemoryLayer); trimmed != "" {
		if err := sqlite.ValidateOptionalMemoryGraphLayer(trimmed); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	if trimmed := strings.TrimSpace(p.MemoryType); trimmed != "" {
		if err := sqlite.ValidateOptionalMemoryGraphType(trimmed); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	if trimmed := strings.TrimSpace(p.Visibility); trimmed != "" {
		if err := sqlite.ValidateOptionalMemoryGraphVisibility(trimmed); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	if trimmed := strings.TrimSpace(p.EpistemicStatus); trimmed != "" {
		if err := sqlite.ValidateOptionalMemoryGraphEpistemicStatus(trimmed); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	if trimmed := strings.TrimSpace(p.LifecycleState); trimmed != "" {
		if err := sqlite.ValidateOptionalMemoryGraphLifecycleState(trimmed); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	if trimmed := strings.TrimSpace(p.OriginKind); trimmed != "" {
		if err := sqlite.ValidateOptionalMemoryGraphOriginKind(trimmed); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	if p.CanonicalOnly && strings.TrimSpace(p.OriginKind) != "" && !strings.EqualFold(strings.TrimSpace(p.OriginKind), "workspace_memory") {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "canonical_only requires origin_kind to be empty or workspace_memory"}
	}
	snapshot, err := h.store.GetMemoryGraphAtlas(ctx, sqlite.MemoryGraphAtlasRequest{
		WorkspaceID:     p.WorkspaceID,
		CenterMemoryID:  p.CenterMemoryID,
		Query:           p.Query,
		MemoryType:      p.MemoryType,
		MemoryLayer:     p.MemoryLayer,
		Visibility:      p.Visibility,
		EpistemicStatus: p.EpistemicStatus,
		LifecycleState:  p.LifecycleState,
		OriginKind:      p.OriginKind,
		IncludeAnchors:  p.IncludeAnchors,
		IncludeArchived: p.IncludeArchived,
		CanonicalOnly:   p.CanonicalOnly,
		Depth:           p.Depth,
		LimitNodes:      p.LimitNodes,
		LimitEdges:      p.LimitEdges,
		MinImportance:   p.MinImportance,
		MinActivation:   p.MinActivation,
	})
	if err != nil {
		if isMemoryGraphValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return snapshot, nil
}

func isMemoryGraphValidationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return false
	}
	return errors.Is(err, sqlite.ErrWorkspaceNotFound) ||
		strings.Contains(msg, " is required") ||
		strings.Contains(msg, "must be one of") ||
		strings.Contains(strings.ToLower(msg), "not found") ||
		strings.Contains(strings.ToLower(msg), "invalid memory")
}

func (h *Handler) memoryGraphProjectionMissingDetails(ctx context.Context, workspaceID, memoryID string) (map[string]any, error) {
	originKind, originID, ok := parseMemoryGraphProjectionMemoryID(memoryID)
	if !ok || originKind != "knowledge_claim" {
		return nil, nil
	}
	if _, err := h.store.GetKnowledgeClaim(ctx, workspaceID, originID); err != nil {
		return nil, nil
	}
	boundary, err := h.store.MemoryGraphBoundaryContractForMemoryID(ctx, workspaceID, memoryID)
	if err != nil {
		return nil, err
	}
	reason := "KNOWLEDGE_CLAIM_PROJECTION_MISSING"
	if strings.EqualFold(boundary.ProjectionCoverage, "PARTIAL") || !strings.EqualFold(boundary.ProjectionLagState, "ok") {
		reason = "KNOWLEDGE_CLAIM_PROJECTION_PENDING"
	}
	return map[string]any{
		"projection_missing_reason": reason,
		"canonical_authority":       "knowledge_claim",
		"surface_authority":         "compatibility_only",
		"surface_role":              "derived_compatibility_projection",
		"compatibility_only":        true,
		"workspace_id":              workspaceID,
		"memory_id":                 memoryID,
		"origin_kind":               originKind,
		"origin_id":                 originID,
		"boundary_contract":         boundary,
	}, nil
}

func parseMemoryGraphProjectionMemoryID(memoryID string) (originKind, originID string, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(memoryID), ":", 3)
	if len(parts) != 3 || parts[0] != "memnode" {
		return "", "", false
	}
	originKind = strings.TrimSpace(parts[1])
	originID = strings.TrimSpace(parts[2])
	if originKind == "" || originID == "" {
		return "", "", false
	}
	return originKind, originID, true
}
