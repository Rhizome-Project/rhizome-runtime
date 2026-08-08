package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryWriteParams struct {
	WorkspaceID string   `json:"workspace_id"`
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

type workspaceMemoryListParams struct {
	WorkspaceID     string `json:"workspace_id"`
	MemoryType      string `json:"memory_type,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

type workspaceMemorySearchParams struct {
	WorkspaceID     string `json:"workspace_id"`
	Query           string `json:"query"`
	MemoryType      string `json:"memory_type,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

type workspaceMemoryRemoveParams struct {
	WorkspaceID string `json:"workspace_id"`
	MemoryID    string `json:"memory_id"`
	RemovedBy   string `json:"removed_by"`
	Reason      string `json:"reason,omitempty"`
}

type workspaceMemoryRestoreParams struct {
	WorkspaceID    string `json:"workspace_id"`
	MemoryID       string `json:"memory_id"`
	RestoredBy     string `json:"restored_by"`
	RecoveryReason string `json:"recovery_reason,omitempty"`
}

type workspaceCompactionCandidatesParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id,omitempty"`
	ActiveOnly  *bool  `json:"active_only,omitempty"`
	MinMessages int    `json:"min_messages,omitempty"`
	MinTokens   int    `json:"min_tokens,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceCompactionSnapshotsParams struct {
	WorkspaceID string `json:"workspace_id"`
	SessionID   string `json:"session_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceMemoryView struct {
	Record sqlite.WorkspaceMemoryRecord `json:"record"`
	Meta   workspaceMemoryViewMeta      `json:"meta"`
}

type workspaceMemoryViewMeta struct {
	Headline                   string   `json:"headline"`
	State                      string   `json:"state"`
	TypeLabel                  string   `json:"type_label"`
	SourceLabel                string   `json:"source_label"`
	Context                    string   `json:"context,omitempty"`
	Provenance                 string   `json:"provenance"`
	Derived                    bool     `json:"derived"`
	LiveSignal                 bool     `json:"live_signal"`
	RequiresAttention          bool     `json:"requires_attention"`
	AttentionLabel             string   `json:"attention_label,omitempty"`
	CanonicalAuthority         string   `json:"canonical_authority"`
	AnchorAuthority            string   `json:"anchor_authority,omitempty"`
	AnchorStatus               string   `json:"anchor_status,omitempty"`
	AnchorStatusReason         string   `json:"anchor_status_reason,omitempty"`
	AnchorSignalState          string   `json:"anchor_signal_state,omitempty"`
	AnchorInvariantState       string   `json:"anchor_invariant_state,omitempty"`
	AnchorInvariantSummary     string   `json:"anchor_invariant_summary,omitempty"`
	AnchorInvariantIssueCodes  []string `json:"anchor_invariant_issue_codes,omitempty"`
	AnchorProjectionLagState   string   `json:"anchor_projection_lag_state,omitempty"`
	AnchorProjectionLagMessage string   `json:"anchor_projection_lag_message,omitempty"`
	AnchorSemanticLineageID    string   `json:"anchor_semantic_lineage_id,omitempty"`
	AnchorRevision             int      `json:"anchor_revision,omitempty"`
	AnchorProtect              *bool    `json:"anchor_protect,omitempty"`
	AnchorUnresolved           *bool    `json:"anchor_unresolved,omitempty"`
	AnchorLastAnyAccess        *string  `json:"anchor_last_any_access,omitempty"`
	AnchorLastTrustedAccess    *string  `json:"anchor_last_trusted_access,omitempty"`
	AnchorTLife                *float64 `json:"anchor_t_life,omitempty"`
	RetentionBand              string   `json:"retention_band,omitempty"`
	RetentionPrunable          bool     `json:"retention_prunable"`
	RetentionGuardReason       string   `json:"retention_guard_reason,omitempty"`
	RetentionHotUntil          *string  `json:"retention_hot_until,omitempty"`
	RetentionWarmUntil         *string  `json:"retention_warm_until,omitempty"`
	RetentionExpiresAt         *string  `json:"retention_expires_at,omitempty"`
	RecoveryCandidate          bool     `json:"recovery_candidate"`
	RecoveryTriggerCount       int      `json:"recovery_trigger_count,omitempty"`
	RecoveryTriggerKinds       []string `json:"recovery_trigger_kinds,omitempty"`
	RecoveryGuardReason        string   `json:"recovery_guard_reason,omitempty"`
	SalienceA                  float64  `json:"salience_a,omitempty"`
	SalienceTStar              string   `json:"salience_t_star,omitempty"`
	SalienceH                  float64  `json:"salience_h,omitempty"`
	SalienceN                  int      `json:"salience_n,omitempty"`
}

type workspaceMemorySummary struct {
	ActiveCount     int            `json:"active_count"`
	ArchivedCount   int            `json:"archived_count"`
	DerivedCount    int            `json:"derived_count"`
	LiveSignalCount int            `json:"live_signal_count"`
	AttentionCount  int            `json:"attention_count"`
	ByType          map[string]int `json:"by_type,omitempty"`
	BySource        map[string]int `json:"by_source,omitempty"`
}

type workspaceMemoryAnchorCompatibility struct {
	Node                 sqlite.MemoryGraphNodeRecord
	Status               string
	StatusReason         string
	SignalState          string
	InvariantState       string
	InvariantSummary     string
	InvariantIssueCodes  []string
	ProjectionLagState   string
	ProjectionLagMessage string
	Usable               bool
}

var currentDirectWorkspaceMemoryTypes = []string{
	"NOTE",
	"LESSON",
	"DECISION",
	"PROCEDURE",
	"ANTI_PROCEDURE",
	"INCIDENT",
	"ENTITY",
	"EXPERIENCE",
	"UPDATE_DIGEST",
	"SUMMARY",
	"SELF_MODEL",
	"GOAL_COMMITMENT",
	"POLICY_TRACE",
}

var currentDirectWorkspaceMemoryTypeSet = func() map[string]struct{} {
	out := make(map[string]struct{}, len(currentDirectWorkspaceMemoryTypes))
	for _, memoryType := range currentDirectWorkspaceMemoryTypes {
		out[memoryType] = struct{}{}
	}
	return out
}()

func validateCurrentDirectWorkspaceMemoryType(raw string) error {
	memoryType := strings.ToUpper(strings.TrimSpace(raw))
	if memoryType == "" {
		return nil
	}
	if _, ok := currentDirectWorkspaceMemoryTypeSet[memoryType]; ok {
		return nil
	}
	return fmt.Errorf("memory_type must be one of: %s", strings.Join(currentDirectWorkspaceMemoryTypes, ", "))
}

func (h *Handler) workspaceMemoryWrite(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryWriteParams
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
	if err := validateCurrentDirectWorkspaceMemoryType(p.MemoryType); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	record, event, effects, err := h.store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		MemoryID:              p.MemoryID,
		WorkspaceID:           workspaceID,
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
		PromptContextEnvelope: h.workspaceMemoryPromptContextEnvelope(ctx, workspaceID, "workspace.memory.write"),
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.write")
	}

	if strings.TrimSpace(record.AgentID) != "" {
		h.touchAgentActivity(ctx, record.WorkspaceID, record.AgentID)
	}
	h.publishRuntimeEventActionsChronological(h.workspaceMemoryEventActions("workspace.memory.recorded", record, event, effects)...)

	salience := h.loadWorkspaceMemorySalience(ctx, record.WorkspaceID, record.MemoryID)
	anchor := h.loadWorkspaceMemoryAnchorState(ctx, record, h.loadWorkspaceMemoryProjectionLag(ctx))
	authority, rpcErr := h.loadWorkspaceMemoryTimeAuthority(ctx, record.WorkspaceID, "workspace.memory.write")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"memory":                 record,
		"entry":                  buildWorkspaceMemoryView(record, salience, anchor),
		"promoted_claim_effects": effects,
		"time_authority":         authority,
		"status":                 "RECORDED",
	}, nil
}

func (h *Handler) workspaceMemoryList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	items, err := h.store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID:     workspaceID,
		MemoryType:      p.MemoryType,
		AgentID:         p.AgentID,
		SessionID:       p.SessionID,
		TaskID:          p.TaskID,
		SourceKind:      p.SourceKind,
		IncludeArchived: p.IncludeArchived,
		Limit:           p.Limit,
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	saliences := h.loadWorkspaceMemorySaliences(ctx, workspaceID, items)
	anchors := h.loadWorkspaceMemoryAnchorStates(ctx, workspaceID, items)
	entries := buildWorkspaceMemoryViews(items, saliences, anchors)
	authority, rpcErr := h.loadWorkspaceMemoryTimeAuthority(ctx, workspaceID, "workspace.memory.list")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   workspaceID,
		"time_authority": authority,
		"items":          items,
		"entries":        entries,
		"summary":        summarizeWorkspaceMemoryViews(entries),
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceMemorySearch(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemorySearchParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Query, "query"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	items, err := h.store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID:     workspaceID,
		Query:           p.Query,
		MemoryType:      p.MemoryType,
		AgentID:         p.AgentID,
		SessionID:       p.SessionID,
		TaskID:          p.TaskID,
		SourceKind:      p.SourceKind,
		IncludeArchived: p.IncludeArchived,
		Limit:           p.Limit,
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	saliences := h.loadWorkspaceMemorySaliences(ctx, p.WorkspaceID, items)
	anchors := h.loadWorkspaceMemoryAnchorStates(ctx, workspaceID, items)
	entries := buildWorkspaceMemoryViews(items, saliences, anchors)
	authority, rpcErr := h.loadWorkspaceMemoryTimeAuthority(ctx, workspaceID, "workspace.memory.search")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   workspaceID,
		"time_authority": authority,
		"query":          strings.TrimSpace(p.Query),
		"items":          items,
		"entries":        entries,
		"summary":        summarizeWorkspaceMemoryViews(entries),
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceMemoryRemove(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryRemoveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.MemoryID, "memory_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.RemovedBy, "removed_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	record, event, effects, err := h.store.ArchiveWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID:           workspaceID,
		MemoryID:              p.MemoryID,
		ArchivedBy:            p.RemovedBy,
		Reason:                p.Reason,
		PromptContextEnvelope: h.workspaceMemoryPromptContextEnvelope(ctx, workspaceID, "workspace.memory.remove"),
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) || strings.Contains(err.Error(), "not found") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.remove")
	}

	if strings.TrimSpace(record.AgentID) != "" {
		h.touchAgentActivity(ctx, record.WorkspaceID, record.AgentID)
	}
	h.publishRuntimeEventActionsChronological(h.workspaceMemoryEventActions("workspace.memory.removed", record, event, effects)...)

	salience := h.loadWorkspaceMemorySalience(ctx, record.WorkspaceID, record.MemoryID)
	anchor := h.loadWorkspaceMemoryAnchorState(ctx, record, h.loadWorkspaceMemoryProjectionLag(ctx))
	authority, rpcErr := h.loadWorkspaceMemoryTimeAuthority(ctx, record.WorkspaceID, "workspace.memory.remove")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"memory":         record,
		"entry":          buildWorkspaceMemoryView(record, salience, anchor),
		"time_authority": authority,
		"status":         "ARCHIVED",
	}, nil
}

func (h *Handler) workspaceMemoryRestore(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryRestoreParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.MemoryID, "memory_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.RestoredBy, "restored_by"); rpcErr != nil {
		return nil, rpcErr
	}

	record, event, effects, err := h.store.RestoreWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID:           workspaceID,
		MemoryID:              p.MemoryID,
		RestoredBy:            p.RestoredBy,
		RecoveryReason:        p.RecoveryReason,
		PromptContextEnvelope: h.workspaceMemoryPromptContextEnvelope(ctx, workspaceID, "workspace.memory.restore"),
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "active duplicate") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.restore")
	}

	if strings.TrimSpace(record.AgentID) != "" {
		h.touchAgentActivity(ctx, record.WorkspaceID, record.AgentID)
	}
	h.publishRuntimeEventActionsChronological(h.workspaceMemoryEventActions("workspace.memory.restored", record, event, effects)...)

	salience := h.loadWorkspaceMemorySalience(ctx, record.WorkspaceID, record.MemoryID)
	anchor := h.loadWorkspaceMemoryAnchorState(ctx, record, h.loadWorkspaceMemoryProjectionLag(ctx))
	authority, rpcErr := h.loadWorkspaceMemoryTimeAuthority(ctx, record.WorkspaceID, "workspace.memory.restore")
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"memory":         record,
		"entry":          buildWorkspaceMemoryView(record, salience, anchor),
		"time_authority": authority,
		"status":         "RESTORED",
	}, nil
}

func (h *Handler) workspaceCompactionCandidates(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceCompactionCandidatesParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	activeOnly := true
	if p.ActiveOnly != nil {
		activeOnly = *p.ActiveOnly
	}
	minMessages := p.MinMessages
	if minMessages <= 0 {
		minMessages = model.DefaultSessionCompactionMinMessages
	}
	minTokens := p.MinTokens
	if minTokens <= 0 {
		minTokens = model.DefaultSessionCompactionMinTokens
	}

	items, err := h.store.ListSessionCompactionCandidates(ctx, sqlite.SessionCompactionFilter{
		WorkspaceID: workspaceID,
		AgentID:     p.AgentID,
		ActiveOnly:  activeOnly,
		MinMessages: minMessages,
		MinTokens:   minTokens,
		Limit:       p.Limit,
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     strings.TrimSpace(p.AgentID),
		"active_only":  activeOnly,
		"min_messages": minMessages,
		"min_tokens":   minTokens,
		"items":        items,
		"count":        len(items),
	}, nil
}

func (h *Handler) workspaceCompactionSnapshots(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceCompactionSnapshotsParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	items, err := h.store.ListSessionCompactionSnapshots(ctx, sqlite.SessionCompactionSnapshotFilter{
		WorkspaceID: workspaceID,
		SessionID:   p.SessionID,
		AgentID:     p.AgentID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isWorkspaceMemoryValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{
		"workspace_id": workspaceID,
		"session_id":   strings.TrimSpace(p.SessionID),
		"agent_id":     strings.TrimSpace(p.AgentID),
		"items":        items,
		"count":        len(items),
	}, nil
}

func isWorkspaceMemoryValidationError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, sqlite.ErrWorkspaceNotFound) ||
		errors.Is(err, sqlite.ErrAgentNotFound) ||
		errors.Is(err, sqlite.ErrSessionNotFound) ||
		errors.Is(err, sqlite.ErrWorkspaceTaskAbsent) ||
		strings.Contains(err.Error(), " is required") ||
		strings.Contains(err.Error(), "active duplicate")
}

func workspaceMemoryEventSummary(record sqlite.WorkspaceMemoryRecord) string {
	switch {
	case strings.TrimSpace(record.Title) != "":
		return record.Title
	case strings.TrimSpace(record.Summary) != "":
		return record.Summary
	default:
		return record.MemoryType
	}
}

func buildWorkspaceMemoryViews(items []sqlite.WorkspaceMemoryRecord, saliences map[string]sqlite.MemoryNodeSalienceRecord, anchors map[string]workspaceMemoryAnchorCompatibility) []workspaceMemoryView {
	views := make([]workspaceMemoryView, 0, len(items))
	for _, item := range items {
		views = append(views, buildWorkspaceMemoryView(item, saliences[item.MemoryID], anchors[item.MemoryID]))
	}
	return views
}

func buildWorkspaceMemoryView(record sqlite.WorkspaceMemoryRecord, salience sqlite.MemoryNodeSalienceRecord, anchor workspaceMemoryAnchorCompatibility) workspaceMemoryView {
	return workspaceMemoryView{
		Record: record,
		Meta:   buildWorkspaceMemoryViewMeta(record, salience, anchor),
	}
}

func buildWorkspaceMemoryViewMeta(record sqlite.WorkspaceMemoryRecord, salience sqlite.MemoryNodeSalienceRecord, anchor workspaceMemoryAnchorCompatibility) workspaceMemoryViewMeta {
	state := "ACTIVE"
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		state = "ARCHIVED"
	}
	derived := strings.TrimSpace(record.SourceKind) != "" && !strings.EqualFold(strings.TrimSpace(record.SourceKind), "manual")
	liveSignal := state == "ACTIVE" && strings.EqualFold(strings.TrimSpace(record.SourceKind), "session_event")
	attentionLabel := workspaceMemoryAttentionLabel(record)
	meta := workspaceMemoryViewMeta{
		Headline:                   workspaceMemoryHeadline(record),
		State:                      state,
		TypeLabel:                  workspaceMemoryTypeLabel(record.MemoryType),
		SourceLabel:                workspaceMemorySourceLabel(record.SourceKind),
		Context:                    workspaceMemoryContext(record),
		Provenance:                 workspaceMemoryProvenance(record, derived, state),
		Derived:                    derived,
		LiveSignal:                 liveSignal,
		RequiresAttention:          attentionLabel != "",
		AttentionLabel:             attentionLabel,
		CanonicalAuthority:         "workspace_memory",
		AnchorAuthority:            "compatibility_only",
		AnchorStatus:               firstNonEmpty(strings.TrimSpace(anchor.Status), "DERIVED_UNKNOWN"),
		AnchorStatusReason:         strings.TrimSpace(anchor.StatusReason),
		AnchorSignalState:          firstNonEmpty(strings.TrimSpace(anchor.SignalState), "UNAVAILABLE"),
		AnchorInvariantState:       strings.TrimSpace(anchor.InvariantState),
		AnchorInvariantSummary:     strings.TrimSpace(anchor.InvariantSummary),
		AnchorInvariantIssueCodes:  append([]string{}, anchor.InvariantIssueCodes...),
		AnchorProjectionLagState:   strings.TrimSpace(anchor.ProjectionLagState),
		AnchorProjectionLagMessage: strings.TrimSpace(anchor.ProjectionLagMessage),
		SalienceA:                  salience.A_i,
		SalienceTStar:              salience.T_i_star,
		SalienceH:                  salience.H_i,
		SalienceN:                  salience.N_i,
	}
	if !anchor.Usable || strings.TrimSpace(anchor.Node.MemoryID) == "" {
		return meta
	}
	meta.AnchorSemanticLineageID = strings.TrimSpace(anchor.Node.SemanticLineageID)
	meta.AnchorRevision = anchor.Node.Revision
	meta.AnchorProtect = workspaceMemoryBoolPtr(anchor.Node.Protect)
	meta.AnchorUnresolved = workspaceMemoryBoolPtr(anchor.Node.Unresolved)
	meta.AnchorLastAnyAccess = anchor.Node.LastAnyAccess
	meta.AnchorLastTrustedAccess = anchor.Node.LastTrustedAccess
	meta.RetentionBand = strings.TrimSpace(anchor.Node.RetentionBand)
	meta.RetentionPrunable = anchor.Node.RetentionPrunable
	meta.RetentionGuardReason = strings.TrimSpace(anchor.Node.RetentionGuardReason)
	meta.RetentionHotUntil = anchor.Node.RetentionHotUntil
	meta.RetentionWarmUntil = anchor.Node.RetentionWarmUntil
	meta.RetentionExpiresAt = anchor.Node.RetentionExpiresAt
	meta.RecoveryCandidate = anchor.Node.RecoveryCandidate
	meta.RecoveryTriggerCount = anchor.Node.RecoveryTriggerCount
	meta.RecoveryTriggerKinds = append([]string{}, anchor.Node.RecoveryTriggerKinds...)
	meta.RecoveryGuardReason = strings.TrimSpace(anchor.Node.RecoveryGuardReason)
	if !workspaceMemoryAnchorSignalsSettled(anchor) {
		meta.RetentionPrunable = false
		if strings.EqualFold(strings.TrimSpace(meta.RetentionBand), "PRUNABLE") {
			meta.RetentionGuardReason = firstNonEmpty(meta.RetentionGuardReason, "PROJECTION_NOT_SETTLED")
		}
		if workspaceMemoryRecoverySignalsInspectable(anchor.Node) {
			meta.RecoveryCandidate = false
			meta.RecoveryTriggerCount = 0
			meta.RecoveryTriggerKinds = nil
			meta.RecoveryGuardReason = "PROJECTION_NOT_SETTLED"
		}
	}
	if anchor.Node.LastAnyAccess != nil || anchor.Node.LastTrustedAccess != nil || anchor.Node.TLife > 0 {
		meta.AnchorTLife = workspaceMemoryFloat64Ptr(anchor.Node.TLife)
	}
	return meta
}

func (h *Handler) loadWorkspaceMemoryAnchorStates(ctx context.Context, workspaceID string, items []sqlite.WorkspaceMemoryRecord) map[string]workspaceMemoryAnchorCompatibility {
	anchors := make(map[string]workspaceMemoryAnchorCompatibility, len(items))
	lag := h.loadWorkspaceMemoryProjectionLag(ctx)
	for _, item := range items {
		anchors[item.MemoryID] = h.loadWorkspaceMemoryAnchorState(ctx, item, lag)
	}
	return anchors
}

func (h *Handler) loadWorkspaceMemorySaliences(ctx context.Context, workspaceID string, items []sqlite.WorkspaceMemoryRecord) map[string]sqlite.MemoryNodeSalienceRecord {
	nodeToMemory := make(map[string]string, len(items))
	nodeIDs := make([]string, 0, len(items))
	for _, item := range items {
		memoryID := strings.TrimSpace(item.MemoryID)
		if memoryID == "" {
			continue
		}
		nodeID := "memnode:workspace_memory:" + memoryID
		nodeToMemory[nodeID] = memoryID
		nodeIDs = append(nodeIDs, nodeID)
	}
	if len(nodeIDs) == 0 {
		return map[string]sqlite.MemoryNodeSalienceRecord{}
	}
	batch, err := h.store.GetMemoryNodeSalienceBatch(ctx, strings.TrimSpace(workspaceID), nodeIDs)
	if err != nil {
		return map[string]sqlite.MemoryNodeSalienceRecord{}
	}
	saliences := make(map[string]sqlite.MemoryNodeSalienceRecord, len(batch))
	for nodeID, record := range batch {
		if memoryID, ok := nodeToMemory[nodeID]; ok {
			saliences[memoryID] = record
		}
	}
	return saliences
}

func (h *Handler) loadWorkspaceMemorySalience(ctx context.Context, workspaceID, memoryID string) sqlite.MemoryNodeSalienceRecord {
	return h.loadWorkspaceMemorySaliences(ctx, workspaceID, []sqlite.WorkspaceMemoryRecord{{MemoryID: memoryID}})[strings.TrimSpace(memoryID)]
}

func (h *Handler) loadWorkspaceMemoryProjectionLag(ctx context.Context) sqlite.MemoryProjectionLagSnapshot {
	snapshot, err := h.store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		return sqlite.MemoryProjectionLagSnapshot{
			State:   "unknown",
			Message: "memory projection lag snapshot unavailable",
			Error:   err.Error(),
		}
	}
	if strings.TrimSpace(snapshot.State) == "" {
		snapshot.State = "unknown"
	}
	return snapshot
}

func (h *Handler) loadWorkspaceMemoryAnchorState(ctx context.Context, record sqlite.WorkspaceMemoryRecord, lag sqlite.MemoryProjectionLagSnapshot) workspaceMemoryAnchorCompatibility {
	workspaceID := strings.TrimSpace(record.WorkspaceID)
	memoryID := strings.TrimSpace(record.MemoryID)
	if workspaceID == "" || memoryID == "" {
		return workspaceMemoryAnchorCompatibility{
			Status:               "DERIVED_UNKNOWN",
			StatusReason:         "derived anchor could not be resolved because workspace_id or memory_id is missing",
			SignalState:          "UNAVAILABLE",
			InvariantState:       sqlite.WorkspaceMemoryProjectionInvariantUnknown,
			InvariantSummary:     "workspace memory projection invariants could not be evaluated because workspace_id or memory_id is missing",
			ProjectionLagState:   firstNonEmpty(strings.TrimSpace(lag.State), "unknown"),
			ProjectionLagMessage: strings.TrimSpace(lag.Message),
		}
	}
	invariant, err := h.store.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, lag)
	if err != nil {
		return workspaceMemoryAnchorCompatibility{
			Status:               "DERIVED_UNKNOWN",
			StatusReason:         "derived anchor invariants could not be evaluated; canonical workspace_memory remains authoritative",
			SignalState:          "UNAVAILABLE",
			InvariantState:       sqlite.WorkspaceMemoryProjectionInvariantUnknown,
			InvariantSummary:     "workspace memory projection invariants are unavailable",
			ProjectionLagState:   firstNonEmpty(strings.TrimSpace(lag.State), "unknown"),
			ProjectionLagMessage: firstNonEmpty(strings.TrimSpace(lag.Message), err.Error()),
		}
	}
	anchor := workspaceMemoryAnchorCompatibility{
		SignalState:          workspaceMemoryAnchorSignalState(invariant),
		InvariantState:       firstNonEmpty(strings.TrimSpace(invariant.State), sqlite.WorkspaceMemoryProjectionInvariantUnknown),
		InvariantSummary:     strings.TrimSpace(invariant.Summary),
		InvariantIssueCodes:  workspaceMemoryInvariantIssueCodes(invariant.Issues),
		ProjectionLagState:   firstNonEmpty(strings.TrimSpace(invariant.ProjectionLagState), "unknown"),
		ProjectionLagMessage: strings.TrimSpace(invariant.ProjectionLagMessage),
	}
	if !invariant.NodePresent {
		switch anchor.InvariantState {
		case sqlite.WorkspaceMemoryProjectionInvariantLagging:
			anchor.Status = "DERIVED_STALE"
			anchor.StatusReason = "derived anchor is unavailable while projection reconciliation is lagging; canonical workspace_memory remains authoritative"
		case sqlite.WorkspaceMemoryProjectionInvariantDrift:
			anchor.Status = "DERIVED_MISSING"
			anchor.StatusReason = "derived anchor is missing after projection backlog settled; canonical workspace_memory remains authoritative"
		default:
			anchor.Status = "DERIVED_UNKNOWN"
			anchor.StatusReason = "derived anchor is unavailable and projection invariants are not trustworthy; canonical workspace_memory remains authoritative"
		}
		return anchor
	}

	anchor.Node = invariant.Node
	switch anchor.InvariantState {
	case sqlite.WorkspaceMemoryProjectionInvariantDrift:
		return workspaceMemoryAnchorCompatibility{
			Node:                 invariant.Node,
			Status:               "DERIVED_CONFLICT",
			StatusReason:         "derived anchor violates canonical workspace_memory invariants; canonical workspace_memory remains authoritative",
			SignalState:          anchor.SignalState,
			InvariantState:       anchor.InvariantState,
			InvariantSummary:     anchor.InvariantSummary,
			InvariantIssueCodes:  anchor.InvariantIssueCodes,
			ProjectionLagState:   anchor.ProjectionLagState,
			ProjectionLagMessage: anchor.ProjectionLagMessage,
		}
	case sqlite.WorkspaceMemoryProjectionInvariantLagging:
		return workspaceMemoryAnchorCompatibility{
			Node:                 invariant.Node,
			Status:               "DERIVED_STALE",
			StatusReason:         "derived anchor is compatibility-only and may lag canonical workspace_memory while projection reconciliation is active",
			SignalState:          anchor.SignalState,
			InvariantState:       anchor.InvariantState,
			InvariantSummary:     anchor.InvariantSummary,
			InvariantIssueCodes:  anchor.InvariantIssueCodes,
			ProjectionLagState:   anchor.ProjectionLagState,
			ProjectionLagMessage: anchor.ProjectionLagMessage,
			Usable:               true,
		}
	case sqlite.WorkspaceMemoryProjectionInvariantCurrent:
		return workspaceMemoryAnchorCompatibility{
			Node:                 invariant.Node,
			Status:               "DERIVED_READY",
			StatusReason:         "derived anchor is compatibility-only over canonical workspace_memory",
			SignalState:          anchor.SignalState,
			InvariantState:       anchor.InvariantState,
			InvariantSummary:     anchor.InvariantSummary,
			InvariantIssueCodes:  anchor.InvariantIssueCodes,
			ProjectionLagState:   anchor.ProjectionLagState,
			ProjectionLagMessage: anchor.ProjectionLagMessage,
			Usable:               true,
		}
	default:
		return workspaceMemoryAnchorCompatibility{
			Node:                 invariant.Node,
			Status:               "DERIVED_UNKNOWN",
			StatusReason:         "derived anchor is compatibility-only, but projection invariants are not fully trustworthy",
			SignalState:          anchor.SignalState,
			InvariantState:       anchor.InvariantState,
			InvariantSummary:     anchor.InvariantSummary,
			InvariantIssueCodes:  anchor.InvariantIssueCodes,
			ProjectionLagState:   anchor.ProjectionLagState,
			ProjectionLagMessage: anchor.ProjectionLagMessage,
			Usable:               true,
		}
	}
}

func workspaceMemoryInvariantIssueCodes(items []sqlite.WorkspaceMemoryProjectionInvariantIssue) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		code := strings.TrimSpace(item.Code)
		if code == "" {
			continue
		}
		out = append(out, code)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workspaceMemoryAnchorSignalState(invariant sqlite.WorkspaceMemoryProjectionInvariant) string {
	if !invariant.NodePresent {
		return "UNAVAILABLE"
	}
	if strings.EqualFold(strings.TrimSpace(invariant.State), sqlite.WorkspaceMemoryProjectionInvariantCurrent) {
		return "SETTLED"
	}
	return "INSPECTABLE_ONLY"
}

func workspaceMemoryAnchorSignalsSettled(anchor workspaceMemoryAnchorCompatibility) bool {
	return anchor.Usable &&
		strings.EqualFold(strings.TrimSpace(anchor.Status), "DERIVED_READY") &&
		strings.EqualFold(strings.TrimSpace(anchor.SignalState), "SETTLED") &&
		strings.EqualFold(strings.TrimSpace(anchor.InvariantState), sqlite.WorkspaceMemoryProjectionInvariantCurrent)
}

func workspaceMemoryRecoverySignalsInspectable(node sqlite.MemoryGraphNodeRecord) bool {
	if node.RecoveryCandidate || node.RecoveryTriggerCount > 0 || len(node.RecoveryTriggerKinds) > 0 || strings.TrimSpace(node.RecoveryGuardReason) != "" {
		return true
	}
	return node.ArchivedAt != nil &&
		strings.TrimSpace(*node.ArchivedAt) != "" &&
		strings.EqualFold(strings.TrimSpace(node.OriginKind), "workspace_memory") &&
		strings.TrimSpace(node.ArchivedReason) == "rmp_gc_expired"
}

func (h *Handler) loadWorkspaceMemoryTimeAuthority(ctx context.Context, workspaceID, surface string) (sqlite.WorkspaceTimeAuthority, *RPCError) {
	return h.loadWorkspaceTimeAuthority(ctx, workspaceID, surface)
}

func (h *Handler) workspaceMemoryPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildWorkspaceMemoryPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func workspaceMemoryBoolPtr(value bool) *bool {
	v := value
	return &v
}

func workspaceMemoryFloat64Ptr(value float64) *float64 {
	v := value
	return &v
}

func summarizeWorkspaceMemoryViews(entries []workspaceMemoryView) workspaceMemorySummary {
	summary := workspaceMemorySummary{
		ByType:   make(map[string]int),
		BySource: make(map[string]int),
	}
	for _, entry := range entries {
		if entry.Meta.State == "ARCHIVED" {
			summary.ArchivedCount++
		} else {
			summary.ActiveCount++
		}
		if entry.Meta.Derived {
			summary.DerivedCount++
		}
		if entry.Meta.LiveSignal {
			summary.LiveSignalCount++
		}
		if entry.Meta.RequiresAttention {
			summary.AttentionCount++
		}
		summary.ByType[entry.Record.MemoryType]++
		summary.BySource[entry.Record.SourceKind]++
	}
	return summary
}

func workspaceMemoryHeadline(record sqlite.WorkspaceMemoryRecord) string {
	switch {
	case strings.TrimSpace(record.Title) != "":
		return strings.TrimSpace(record.Title)
	case strings.TrimSpace(record.Summary) != "":
		return strings.TrimSpace(record.Summary)
	default:
		return workspaceMemoryTypeLabel(record.MemoryType)
	}
}

func workspaceMemoryTypeLabel(raw string) string {
	return workspaceMemoryLabel(raw)
}

func workspaceMemorySourceLabel(raw string) string {
	return workspaceMemoryLabel(raw)
}

func workspaceMemoryLabel(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "Unknown"
	}
	parts := strings.FieldsFunc(strings.ToLower(trimmed), func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	for i, part := range parts {
		if len(part) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func workspaceMemoryContext(record sqlite.WorkspaceMemoryRecord) string {
	parts := make([]string, 0, 3)
	if trimmed := strings.TrimSpace(record.AgentID); trimmed != "" {
		parts = append(parts, "Agent "+trimmed)
	}
	if trimmed := strings.TrimSpace(record.TaskID); trimmed != "" {
		parts = append(parts, "Task "+trimmed)
	}
	if trimmed := strings.TrimSpace(record.SessionID); trimmed != "" {
		parts = append(parts, "Session "+trimmed)
	}
	return strings.Join(parts, " · ")
}

func workspaceMemoryProvenance(record sqlite.WorkspaceMemoryRecord, derived bool, state string) string {
	sourceLabel := workspaceMemorySourceLabel(record.SourceKind)
	parts := make([]string, 0, 3)
	if derived {
		parts = append(parts, "Derived from "+sourceLabel)
	} else {
		parts = append(parts, "Recorded manually")
	}
	if sourceID := strings.TrimSpace(record.SourceID); sourceID != "" &&
		sourceID != strings.TrimSpace(record.SessionID) &&
		sourceID != strings.TrimSpace(record.AgentID) {
		parts = append(parts, "Source "+sourceID)
	}
	if state == "ARCHIVED" {
		parts = append(parts, "Archived")
	}
	return strings.Join(parts, " · ")
}

func workspaceMemoryAttentionLabel(record sqlite.WorkspaceMemoryRecord) string {
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		return ""
	}
	if !strings.EqualFold(strings.TrimSpace(record.SourceKind), "session_event") {
		return ""
	}
	switch {
	case workspaceMemoryHasTag(record.Tags, "decision_needed"):
		return "Decision needed"
	case workspaceMemoryHasTag(record.Tags, "blocked"):
		return "Blocked"
	default:
		return ""
	}
}

func workspaceMemoryHasTag(tags []string, needle string) bool {
	normalizedNeedle := strings.TrimSpace(strings.ToLower(needle))
	for _, tag := range tags {
		if strings.TrimSpace(strings.ToLower(tag)) == normalizedNeedle {
			return true
		}
	}
	return false
}
