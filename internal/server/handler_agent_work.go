package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type agentTaskHydrateParams struct {
	WorkspaceID      string   `json:"workspace_id"`
	TaskID           string   `json:"task_id"`
	DocKeys          []string `json:"doc_keys"`
	IncludeAllDocs   *bool    `json:"include_all_docs"`
	UpdatesLimit     int      `json:"updates_limit"`
	ArtifactLimit    int      `json:"artifact_limit"`
	RelatedTaskLimit int      `json:"related_task_limit"`
}

type agentWorkNextParams struct {
	WorkspaceID        string   `json:"workspace_id"`
	AgentID            string   `json:"agent_id"`
	IncludeHydration   *bool    `json:"include_hydration"`
	IncludePacket      *bool    `json:"include_packet"`
	IncludeAdvisory    *bool    `json:"include_advisory"`
	EnableTaskFrontier *bool    `json:"enable_task_frontier"`
	FrontierLimit      int      `json:"frontier_limit"`
	DocKeys            []string `json:"doc_keys"`
	IncludeAllDocs     *bool    `json:"include_all_docs"`
	UpdatesLimit       int      `json:"updates_limit"`
	ArtifactLimit      int      `json:"artifact_limit"`
	RelatedTaskLimit   int      `json:"related_task_limit"`
	SessionLimit       int      `json:"session_limit"`
	Trigger            string   `json:"trigger"`
	CandidateTaskID    string   `json:"candidate_task_id"`
	CandidateSessionID string   `json:"candidate_session_id"`
	CoordinationMode   string   `json:"coordination_mode"`
}

func (h *Handler) agentTaskHydrate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentTaskHydrateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.TaskID, "task_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	includeAllDocs := true
	if p.IncludeAllDocs != nil {
		includeAllDocs = *p.IncludeAllDocs
	} else if len(p.DocKeys) > 0 {
		includeAllDocs = false
	}

	bundle, err := h.store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:           p.TaskID,
		WorkspaceID:      p.WorkspaceID,
		DocKeys:          append([]string(nil), p.DocKeys...),
		IncludeAllDocs:   includeAllDocs,
		UpdatesLimit:     p.UpdatesLimit,
		ArtifactLimit:    p.ArtifactLimit,
		RelatedTaskLimit: p.RelatedTaskLimit,
	})
	if err != nil {
		return nil, hydrationRPCError(err)
	}

	return map[string]any{
		"bundle": bundle,
	}, nil
}

func (h *Handler) agentWorkNext(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentWorkNextParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	includeHydration := false
	if p.IncludeHydration != nil {
		includeHydration = *p.IncludeHydration
	}
	includePacket := false
	if p.IncludePacket != nil {
		includePacket = *p.IncludePacket
	}
	includeAdvisory := false
	if p.IncludeAdvisory != nil {
		includeAdvisory = *p.IncludeAdvisory
	}
	enableTaskFrontier := false
	if p.EnableTaskFrontier != nil {
		enableTaskFrontier = *p.EnableTaskFrontier
	}
	includeAllDocs := true
	if p.IncludeAllDocs != nil {
		includeAllDocs = *p.IncludeAllDocs
	} else if len(p.DocKeys) > 0 {
		includeAllDocs = false
	}

	result, err := h.store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        p.WorkspaceID,
		AgentID:            p.AgentID,
		IncludeHydration:   includeHydration,
		IncludePacket:      includePacket,
		IncludeAdvisory:    includeAdvisory,
		EnableTaskFrontier: enableTaskFrontier,
		FrontierLimit:      p.FrontierLimit,
		DocKeys:            append([]string(nil), p.DocKeys...),
		IncludeAllDocs:     includeAllDocs,
		UpdatesLimit:       p.UpdatesLimit,
		ArtifactLimit:      p.ArtifactLimit,
		RelatedTaskLimit:   p.RelatedTaskLimit,
		SessionLimit:       p.SessionLimit,
		Trigger:            p.Trigger,
		CandidateTaskID:    p.CandidateTaskID,
		CandidateSessionID: p.CandidateSessionID,
		CoordinationMode:   p.CoordinationMode,
	})
	if err != nil {
		return nil, hydrationRPCError(err)
	}

	h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
	return result, nil
}

func hydrationRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, sqlite.ErrWorkspaceNotFound),
		errors.Is(err, sqlite.ErrAgentNotFound),
		errors.Is(err, sqlite.ErrTaskNotFound),
		errors.Is(err, sqlite.ErrWorkspaceTaskAbsent):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	default:
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
}
