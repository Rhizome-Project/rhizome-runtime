package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceArtifactWriteParams struct {
	ArtifactID   string `json:"artifact_id,omitempty"`
	WorkspaceID  string `json:"workspace_id"`
	TaskID       string `json:"task_id,omitempty"`
	UpdateID     string `json:"update_id,omitempty"`
	Title        string `json:"title"`
	ArtifactRef  string `json:"artifact_ref"`
	Kind         string `json:"kind,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	CreatedBy    string `json:"created_by"`
	MetadataJSON string `json:"metadata_json,omitempty"`
}

type workspaceArtifactListParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id,omitempty"`
	UpdateID    string `json:"update_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

func (h *Handler) workspaceArtifactWrite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceArtifactWriteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ArtifactRef, "artifact_ref"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.CreatedBy, "created_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.CreatedBy, "created_by"); rpcErr != nil {
		return nil, rpcErr
	}

	record, event, invalidationEvents, err := h.store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:            p.ArtifactID,
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		UpdateID:              p.UpdateID,
		Title:                 p.Title,
		ArtifactRef:           p.ArtifactRef,
		Kind:                  p.Kind,
		ContentType:           p.ContentType,
		CreatedBy:             p.CreatedBy,
		MetadataJSON:          p.MetadataJSON,
		PromptContextEnvelope: h.workspaceArtifactPromptContextEnvelope(ctx, p.WorkspaceID, "workspace.artifact.write"),
		PromptContextSurface:  "workspace.artifact.write",
	})
	if err != nil {
		if isWorkspaceArtifactValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.artifact.write"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	h.touchAgentActivity(ctx, record.WorkspaceID, record.CreatedBy)
	actions := []runtimeEventPublishAction{
		{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, record.Title, record.ArtifactRef, record.ArtifactID)
			},
		},
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, record.Title, record.ArtifactRef, record.ArtifactID)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)

	return map[string]any{
		"artifact": record,
		"status":   "RECORDED",
	}, nil
}

func (h *Handler) workspaceArtifactList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceArtifactListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	items, err := h.store.ListWorkspaceArtifacts(ctx, sqlite.WorkspaceArtifactFilter{
		WorkspaceID: p.WorkspaceID,
		TaskID:      p.TaskID,
		UpdateID:    p.UpdateID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isWorkspaceArtifactValidationError(err) {
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

func isWorkspaceArtifactValidationError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, sqlite.ErrWorkspaceNotFound) ||
		errors.Is(err, sqlite.ErrWorkspaceTaskAbsent) ||
		strings.Contains(err.Error(), " is required") ||
		strings.Contains(err.Error(), "not found") ||
		strings.Contains(err.Error(), "metadata_json") ||
		strings.Contains(err.Error(), "prompt_context_envelope") ||
		strings.Contains(err.Error(), "prompt_context_envelope.v1")
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func (h *Handler) workspaceArtifactPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildWorkspaceArtifactPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}
