package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceSegmentListParams struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key,omitempty"`
	ArtifactRef string `json:"artifact_ref,omitempty"`
	SegmentRef  string `json:"segment_ref,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceSegmentGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	SegmentRef  string `json:"segment_ref"`
}

func (h *Handler) workspaceSegmentList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceSegmentListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.SegmentRef) != "" && (strings.TrimSpace(p.DocKey) != "" || strings.TrimSpace(p.ArtifactRef) != "") {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "segment_ref cannot be combined with doc_key or artifact_ref"}
	}
	report, err := h.store.BuildWorkspaceSegmentReport(ctx, sqlite.WorkspaceSegmentFilter{
		WorkspaceID: p.WorkspaceID,
		DocKey:      p.DocKey,
		ArtifactRef: p.ArtifactRef,
		SegmentRef:  p.SegmentRef,
		Limit:       p.Limit,
	})
	if err != nil {
		if isWorkspaceSegmentValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": strings.TrimSpace(report.WorkspaceID),
		"report":       report,
		"items":        report.Segments,
		"count":        len(report.Segments),
	}, nil
}

func (h *Handler) workspaceSegmentGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceSegmentGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SegmentRef, "segment_ref"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildWorkspaceSegmentReport(ctx, sqlite.WorkspaceSegmentFilter{
		WorkspaceID: p.WorkspaceID,
		SegmentRef:  p.SegmentRef,
		Limit:       1,
	})
	if err != nil {
		if isWorkspaceSegmentValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if len(report.Segments) == 0 {
		return nil, &RPCError{Code: errCodeInternal, Message: "segment not found"}
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(report.WorkspaceID),
		"time_authority": report.TimeAuthority,
		"segment":        report.Segments[0],
	}, nil
}

func isWorkspaceSegmentValidationError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, sqlite.ErrWorkspaceNotFound) ||
		strings.Contains(err.Error(), " is required") ||
		strings.Contains(err.Error(), "mutually exclusive") ||
		strings.Contains(strings.ToLower(err.Error()), "not found") ||
		strings.Contains(strings.ToLower(err.Error()), "invalid segment_ref")
}
