package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryPackWriteParams struct {
	SnapshotID             string `json:"snapshot_id,omitempty"`
	WorkspaceID            string `json:"workspace_id"`
	SessionID              string `json:"session_id"`
	AgentID                string `json:"agent_id,omitempty"`
	TriggerKind            string `json:"trigger_kind,omitempty"`
	PackMode               string `json:"pack_mode,omitempty"`
	SourceWindowDigest     string `json:"source_window_digest,omitempty"`
	TokenBudget            int    `json:"token_budget,omitempty"`
	MessageCountBefore     int    `json:"message_count_before,omitempty"`
	MessageCountAfter      int    `json:"message_count_after,omitempty"`
	MessageTokensBefore    int    `json:"message_tokens_before,omitempty"`
	MessageTokensAfter     int    `json:"message_tokens_after,omitempty"`
	TotalInputTokens       int    `json:"total_input_tokens,omitempty"`
	TotalOutputTokens      int    `json:"total_output_tokens,omitempty"`
	SummaryText            string `json:"summary_text,omitempty"`
	SummaryWorkspaceMemory string `json:"summary_workspace_memory,omitempty"`
}

func (h *Handler) workspaceMemoryPackWrite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	if err := rejectWorkspaceMemoryPackAuthorityFields(raw); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	var p workspaceMemoryPackWriteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SessionID, "session_id"); rpcErr != nil {
		return nil, rpcErr
	}

	result, err := h.store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:             p.SnapshotID,
		WorkspaceID:            p.WorkspaceID,
		SessionID:              p.SessionID,
		AgentID:                p.AgentID,
		TriggerKind:            p.TriggerKind,
		PackMode:               p.PackMode,
		SourceWindowDigest:     p.SourceWindowDigest,
		TokenBudget:            p.TokenBudget,
		MessageCountBefore:     p.MessageCountBefore,
		MessageCountAfter:      p.MessageCountAfter,
		MessageTokensBefore:    p.MessageTokensBefore,
		MessageTokensAfter:     p.MessageTokensAfter,
		TotalInputTokens:       p.TotalInputTokens,
		TotalOutputTokens:      p.TotalOutputTokens,
		SummaryText:            p.SummaryText,
		SummaryWorkspaceMemory: p.SummaryWorkspaceMemory,
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) || strings.Contains(strings.ToLower(err.Error()), "snapshot_id") || strings.Contains(strings.ToLower(err.Error()), "session_id") || strings.Contains(strings.ToLower(err.Error()), "agent_id") || strings.Contains(strings.ToLower(err.Error()), "pack_mode") || strings.Contains(strings.ToLower(err.Error()), "summary_workspace_memory") || strings.Contains(strings.ToLower(err.Error()), "trigger_kind") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if strings.TrimSpace(result.Snapshot.AgentID) != "" {
		h.touchAgentActivity(ctx, result.Snapshot.WorkspaceID, result.Snapshot.AgentID)
	}
	return result, nil
}

func rejectWorkspaceMemoryPackAuthorityFields(raw json.RawMessage) error {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	prohibited := []string{
		"artifact_delta_ledger",
		"blocker_ledger",
		"canonical_memory_id",
		"decision_ledger",
		"dissent_set",
		"dissent_state",
		"fact_candidates",
		"failure_repair_chain",
		"hypothesis_candidates",
		"lifecycle_event_id",
		"lineage_session_id",
		"open_loops",
		"pack_id",
		"pack_key",
		"pack_source",
		"pack_type",
		"provenance_refs",
		"schema_version",
		"task_id",
	}
	allowed := map[string]struct{}{
		"agent_id":                 {},
		"message_count_after":      {},
		"message_count_before":     {},
		"message_tokens_after":     {},
		"message_tokens_before":    {},
		"pack_mode":                {},
		"session_id":               {},
		"snapshot_id":              {},
		"source_window_digest":     {},
		"summary_text":             {},
		"summary_workspace_memory": {},
		"token_budget":             {},
		"total_input_tokens":       {},
		"total_output_tokens":      {},
		"trigger_kind":             {},
		"workspace_id":             {},
	}
	found := make([]string, 0, 4)
	unknown := make([]string, 0, 4)
	for _, field := range prohibited {
		if _, ok := payload[field]; ok {
			found = append(found, field)
		}
	}
	for field := range payload {
		if _, ok := allowed[field]; ok {
			continue
		}
		isProhibited := false
		for _, prohibitedField := range prohibited {
			if field == prohibitedField {
				isProhibited = true
				break
			}
		}
		if !isProhibited {
			unknown = append(unknown, field)
		}
	}
	if len(found) > 0 {
		sort.Strings(found)
		return fmt.Errorf("workspace.memory.pack.write does not accept direct pack authority fields: %s", strings.Join(found, ", "))
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("workspace.memory.pack.write does not accept unknown fields: %s", strings.Join(unknown, ", "))
	}
	return nil
}
