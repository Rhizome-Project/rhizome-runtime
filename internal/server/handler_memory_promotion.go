package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryPromotionEnqueueParams struct {
	PromotionID   string   `json:"promotion_id,omitempty"`
	WorkspaceID   string   `json:"workspace_id"`
	CandidateKind string   `json:"candidate_kind,omitempty"`
	MemoryType    string   `json:"memory_type,omitempty"`
	Title         string   `json:"title,omitempty"`
	Body          string   `json:"body"`
	Summary       string   `json:"summary,omitempty"`
	AgentID       string   `json:"agent_id,omitempty"`
	SessionID     string   `json:"session_id,omitempty"`
	TaskID        string   `json:"task_id,omitempty"`
	SourceKind    string   `json:"source_kind"`
	SourceID      string   `json:"source_id"`
	Tags          []string `json:"tags,omitempty"`
	Importance    float64  `json:"importance,omitempty"`
	Confidence    float64  `json:"confidence,omitempty"`
	BasisDigest   string   `json:"basis_digest"`
	BasisRefs     []string `json:"basis_refs,omitempty"`
	ProposedBy    string   `json:"proposed_by"`
}

type workspaceMemoryPromotionResolveParams struct {
	WorkspaceID    string `json:"workspace_id"`
	PromotionID    string `json:"promotion_id,omitempty"`
	QueueKey       string `json:"queue_key,omitempty"`
	Resolution     string `json:"resolution"`
	ResolutionNote string `json:"resolution_note,omitempty"`
	ResolvedBy     string `json:"resolved_by"`
}

type workspaceMemoryPromotionGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	PromotionID string `json:"promotion_id"`
}

type workspaceMemoryPromotionListParams struct {
	WorkspaceID   string `json:"workspace_id"`
	State         string `json:"state,omitempty"`
	CandidateKind string `json:"candidate_kind,omitempty"`
	CandidateType string `json:"candidate_type,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

func (h *Handler) workspaceMemoryPromotionEnqueue(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPromotionEnqueueParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Body, "body"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SourceKind, "source_kind"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SourceID, "source_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.BasisDigest, "basis_digest"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ProposedBy, "proposed_by"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.EnqueueMemoryPromotionWithEvent(ctx, sqlite.MemoryPromotionEnqueueInput{
		PromotionID:   p.PromotionID,
		WorkspaceID:   p.WorkspaceID,
		CandidateKind: p.CandidateKind,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: p.MemoryType,
			Title:      p.Title,
			Body:       p.Body,
			Summary:    p.Summary,
			AgentID:    p.AgentID,
			SessionID:  p.SessionID,
			TaskID:     p.TaskID,
			SourceKind: p.SourceKind,
			SourceID:   p.SourceID,
			Tags:       p.Tags,
			Importance: p.Importance,
			Confidence: p.Confidence,
		},
		BasisDigest: p.BasisDigest,
		BasisRefs:   p.BasisRefs,
		ProposedBy:  p.ProposedBy,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.memory.promotion.enqueue"); rpcErr != nil {
			return nil, rpcErr
		}
		if isWorkspaceMemoryValidationError(err) ||
			strings.Contains(strings.ToLower(err.Error()), "invalid evidence") ||
			strings.Contains(strings.ToLower(err.Error()), "cross-episodic evidence") ||
			strings.Contains(strings.ToLower(err.Error()), "candidate_kind") ||
			strings.Contains(strings.ToLower(err.Error()), "basis_digest") ||
			strings.Contains(strings.ToLower(err.Error()), "proposed_by") ||
			strings.Contains(strings.ToLower(err.Error()), "source_kind") ||
			strings.Contains(strings.ToLower(err.Error()), "source_id") ||
			strings.Contains(strings.ToLower(err.Error()), "promotion_id") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if strings.TrimSpace(record.Candidate.AgentID) != "" {
		h.touchAgentActivity(ctx, record.WorkspaceID, record.Candidate.AgentID)
	}
	if event.EventID != "" {
		h.publishRuntimeEventRecord(event, "memory promotion enqueue")
	}
	return record, nil
}

func (h *Handler) workspaceMemoryPromotionResolve(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPromotionResolveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Resolution, "resolution"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ResolvedBy, "resolved_by"); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
		WorkspaceID:    p.WorkspaceID,
		PromotionID:    p.PromotionID,
		QueueKey:       p.QueueKey,
		Resolution:     p.Resolution,
		ResolutionNote: p.ResolutionNote,
		ResolvedBy:     p.ResolvedBy,
	})
	if err != nil {
		var deferredErr *sqlite.MemoryPromotionDeferredAcceptError
		if errors.As(err, &deferredErr) {
			if deferredErr.Queue != nil && deferredErr.QueueEvent != nil && strings.TrimSpace(deferredErr.QueueEvent.EventID) != "" {
				h.publishOperatorQueueEventRecord(*deferredErr.QueueEvent, "workspace.ops.updated", *deferredErr.Queue)
			}
		}
		if rpcErr := authorityRejectRPCError(err, "workspace.memory.promotion.resolve"); rpcErr != nil {
			return nil, rpcErr
		}
		if isWorkspaceMemoryValidationError(err) ||
			strings.Contains(strings.ToLower(err.Error()), "invalid evidence") ||
			strings.Contains(strings.ToLower(err.Error()), "cross-episodic evidence") ||
			strings.Contains(strings.ToLower(err.Error()), "coherence gate requires deferred accept") ||
			strings.Contains(strings.ToLower(err.Error()), "promotion_id") ||
			strings.Contains(strings.ToLower(err.Error()), "queue_key") ||
			strings.Contains(strings.ToLower(err.Error()), "resolution") ||
			strings.Contains(strings.ToLower(err.Error()), "resolved_by") ||
			strings.Contains(strings.ToLower(err.Error()), "already resolved") ||
			strings.Contains(strings.ToLower(err.Error()), "not found") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	actions := make([]runtimeEventPublishAction, 0, 4)
	if result.Event != nil && result.Event.EventID != "" {
		actions = append(actions, runtimeEventPublishAction{
			Event: *result.Event,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(event, "memory promotion resolve")
			},
		})
	}
	if result.AppliedMemory != nil && strings.TrimSpace(result.AppliedMemory.Record.AgentID) != "" {
		h.touchAgentActivity(ctx, result.AppliedMemory.Record.WorkspaceID, result.AppliedMemory.Record.AgentID)
	}
	if result.AppliedMemory != nil && result.AppliedMemory.Event.EventID != "" {
		record := result.AppliedMemory.Record
		actions = append(actions, runtimeEventPublishAction{
			Event: result.AppliedMemory.Event,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishWorkspaceMemoryRecordedEvent(record, event)
			},
		})
	}
	if result.AppliedMemory != nil {
		actions = append(actions, h.promotedKnowledgeClaimSyncActions(result.AppliedMemory.PromotedClaimEffects)...)
	}
	h.publishRuntimeEventActionsChronological(actions...)
	return result, nil
}

func (h *Handler) workspaceMemoryPromotionGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPromotionGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.PromotionID, "promotion_id"); rpcErr != nil {
		return nil, rpcErr
	}
	record, err := h.store.GetMemoryPromotion(ctx, p.WorkspaceID, p.PromotionID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") || strings.Contains(strings.ToLower(err.Error()), " is required") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return record, nil
}

func (h *Handler) workspaceMemoryPromotionList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPromotionListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListMemoryPromotions(ctx, sqlite.MemoryPromotionFilter{
		WorkspaceID:   p.WorkspaceID,
		State:         p.State,
		CandidateKind: p.CandidateKind,
		CandidateType: p.CandidateType,
		Limit:         p.Limit,
	})
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "workspace_id") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": strings.TrimSpace(p.WorkspaceID),
		"items":        items,
		"count":        len(items),
	}, nil
}
