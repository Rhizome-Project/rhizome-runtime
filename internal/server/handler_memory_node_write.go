package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryNodeWriteParams struct {
	WorkspaceID string   `json:"workspace_id"`
	NodeID      string   `json:"node_id,omitempty"`
	MemoryID    string   `json:"memory_id,omitempty"`
	MemoryType  string   `json:"memory_type,omitempty"`
	Title       string   `json:"title,omitempty"`
	Body        string   `json:"body"`
	Summary     string   `json:"summary,omitempty"`
	AgentID     string   `json:"agent_id,omitempty"`
	SessionID   string   `json:"session_id,omitempty"`
	TaskID      string   `json:"task_id,omitempty"`
	SourceKind  string   `json:"source_kind,omitempty"`
	SourceID    string   `json:"source_id,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Importance  float64  `json:"importance,omitempty"`
	Confidence  float64  `json:"confidence,omitempty"`
}

func (h *Handler) workspaceMemoryNodeWrite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	if err := rejectWorkspaceMemoryNodeAuthorityFields(raw); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	var p workspaceMemoryNodeWriteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Body, "body"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	result, err := h.store.WriteMemoryNode(ctx, sqlite.MemoryNodeWriteInput{
		WorkspaceID:           workspaceID,
		NodeID:                p.NodeID,
		MemoryID:              p.MemoryID,
		MemoryType:            p.MemoryType,
		Title:                 p.Title,
		Body:                  p.Body,
		Summary:               p.Summary,
		AgentID:               p.AgentID,
		SessionID:             p.SessionID,
		TaskID:                p.TaskID,
		SourceKind:            p.SourceKind,
		SourceID:              p.SourceID,
		Tags:                  p.Tags,
		Importance:            p.Importance,
		Confidence:            p.Confidence,
		PromptContextEnvelope: h.workspaceMemoryPromptContextEnvelope(ctx, workspaceID, "workspace.memory.node.write"),
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) || isMemoryGraphValidationError(err) || strings.Contains(strings.ToLower(err.Error()), "node_id") || strings.Contains(strings.ToLower(err.Error()), "memory_id") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	if strings.TrimSpace(result.Record.AgentID) != "" {
		h.touchAgentActivity(ctx, result.Record.WorkspaceID, result.Record.AgentID)
	}
	h.publishRuntimeEventActionsChronological(h.workspaceMemoryEventActions("workspace.memory.recorded", result.Record, result.Event, result.PromotedClaimEffects)...)
	return result, nil
}

func rejectWorkspaceMemoryNodeAuthorityFields(raw json.RawMessage) error {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	prohibited := []string{
		"activation",
		"archived_at",
		"claim_object",
		"claim_predicate",
		"claim_subject",
		"compat_type",
		"drift",
		"drift_score",
		"edges",
		"epistemic_status",
		"inbound_edges",
		"lifecycle_state",
		"memory_layer",
		"metrics",
		"origin_id",
		"origin_kind",
		"outbound_edges",
		"pin_strength",
		"provenance",
		"refs",
		"source_set",
		"temperature",
		"versions",
		"visibility",
		"volatility",
	}
	found := make([]string, 0, 4)
	for _, field := range prohibited {
		if _, ok := payload[field]; ok {
			found = append(found, field)
		}
	}
	if len(found) == 0 {
		return nil
	}
	sort.Strings(found)
	return fmt.Errorf("workspace.memory.node.write does not accept direct graph authority fields: %s", strings.Join(found, ", "))
}
