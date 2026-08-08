package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type MemoryNodeWriteInput struct {
	WorkspaceID           string
	NodeID                string
	MemoryID              string
	MemoryType            string
	Title                 string
	Body                  string
	Summary               string
	AgentID               string
	SessionID             string
	TaskID                string
	SourceKind            string
	SourceID              string
	Tags                  []string
	Importance            float64
	Confidence            float64
	PromptContextEnvelope map[string]any
}

type MemoryNodeWriteResult struct {
	WorkspaceID          string                             `json:"workspace_id"`
	NodeID               string                             `json:"node_id"`
	MemoryID             string                             `json:"memory_id"`
	OriginKind           string                             `json:"origin_kind"`
	OriginID             string                             `json:"origin_id"`
	Status               string                             `json:"status"`
	Record               WorkspaceMemoryRecord              `json:"record"`
	Event                RuntimeEventRecord                 `json:"event"`
	Node                 MemoryGraphNodeRecord              `json:"node"`
	PromotedClaimEffects *PromotedKnowledgeClaimSyncEffects `json:"promoted_claim_effects,omitempty"`
}

func (s *Store) WriteMemoryNode(ctx context.Context, input MemoryNodeWriteInput) (MemoryNodeWriteResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return MemoryNodeWriteResult{}, errors.New("workspace_id is required")
	}
	if strings.TrimSpace(input.Body) == "" {
		return MemoryNodeWriteResult{}, errors.New("body is required")
	}
	if err := validateMemoryNodeWriteType(input.MemoryType); err != nil {
		return MemoryNodeWriteResult{}, err
	}

	nodeBackingID, err := normalizeWorkspaceMemoryNodeWriteID(input.NodeID, "node_id")
	if err != nil {
		return MemoryNodeWriteResult{}, err
	}
	memoryBackingID, err := normalizeWorkspaceMemoryNodeWriteID(input.MemoryID, "memory_id")
	if err != nil {
		return MemoryNodeWriteResult{}, err
	}
	if nodeBackingID != "" && memoryBackingID != "" && nodeBackingID != memoryBackingID {
		return MemoryNodeWriteResult{}, fmt.Errorf("node_id and memory_id must reference the same workspace_memory origin")
	}
	backingID := firstNonEmpty(memoryBackingID, nodeBackingID)
	if err := s.ensureMemoryNodeWriteOwnership(ctx, workspaceID, backingID); err != nil {
		return MemoryNodeWriteResult{}, err
	}
	if err := s.normalizeMemoryNodeWriteAnchors(ctx, &input); err != nil {
		return MemoryNodeWriteResult{}, err
	}

	record, event, effects, err := s.RecordWorkspaceMemoryWithEffects(ctx, WorkspaceMemoryInput{
		MemoryID:              backingID,
		WorkspaceID:           workspaceID,
		MemoryType:            strings.TrimSpace(input.MemoryType),
		Title:                 strings.TrimSpace(input.Title),
		Body:                  strings.TrimSpace(input.Body),
		Summary:               strings.TrimSpace(input.Summary),
		AgentID:               strings.TrimSpace(input.AgentID),
		SessionID:             strings.TrimSpace(input.SessionID),
		TaskID:                strings.TrimSpace(input.TaskID),
		SourceKind:            strings.TrimSpace(input.SourceKind),
		SourceID:              strings.TrimSpace(input.SourceID),
		Tags:                  append([]string(nil), input.Tags...),
		Importance:            input.Importance,
		Confidence:            input.Confidence,
		PromptContextEnvelope: input.PromptContextEnvelope,
	})
	if err != nil {
		return MemoryNodeWriteResult{}, err
	}

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	detail, err := s.GetMemoryGraphNode(ctx, record.WorkspaceID, nodeID)
	if err != nil {
		return MemoryNodeWriteResult{}, err
	}

	return MemoryNodeWriteResult{
		WorkspaceID:          record.WorkspaceID,
		NodeID:               nodeID,
		MemoryID:             record.MemoryID,
		OriginKind:           "workspace_memory",
		OriginID:             record.MemoryID,
		Status:               "RECORDED",
		Record:               record,
		Event:                event,
		Node:                 detail.Node,
		PromotedClaimEffects: effects,
	}, nil
}

func (s *Store) ensureMemoryNodeWriteOwnership(ctx context.Context, workspaceID, memoryID string) error {
	memoryID = strings.TrimSpace(memoryID)
	if memoryID == "" {
		return nil
	}
	var existingWorkspaceID string
	err := s.db.QueryRowContext(ctx, `SELECT workspace_id FROM workspace_memory WHERE memory_id = ?`, memoryID).Scan(&existingWorkspaceID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("lookup workspace memory id: %w", err)
	case strings.TrimSpace(existingWorkspaceID) != strings.TrimSpace(workspaceID):
		return fmt.Errorf("memory_id already belongs to workspace %s", strings.TrimSpace(existingWorkspaceID))
	default:
		return nil
	}
}

func (s *Store) normalizeMemoryNodeWriteAnchors(ctx context.Context, input *MemoryNodeWriteInput) error {
	if input == nil {
		return nil
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	sessionID := strings.TrimSpace(input.SessionID)
	if sessionID == "" {
		return nil
	}
	state, err := s.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(input.AgentID)
	taskID := strings.TrimSpace(input.TaskID)
	if agentID != "" && agentID != strings.TrimSpace(state.AgentID) {
		return fmt.Errorf("session_id does not belong to agent_id")
	}
	if taskID != "" {
		if strings.TrimSpace(state.TaskID) == "" || taskID != strings.TrimSpace(state.TaskID) {
			return fmt.Errorf("session_id does not belong to task_id")
		}
	}
	if agentID == "" {
		input.AgentID = strings.TrimSpace(state.AgentID)
	}
	if taskID == "" && strings.TrimSpace(state.TaskID) != "" {
		input.TaskID = strings.TrimSpace(state.TaskID)
	}
	return nil
}

func normalizeWorkspaceMemoryNodeWriteID(raw, fieldName string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if !strings.HasPrefix(trimmed, "memnode:") {
		return trimmed, nil
	}
	parts := strings.SplitN(trimmed, ":", 3)
	if len(parts) != 3 || strings.TrimSpace(parts[2]) == "" {
		return "", fmt.Errorf("%s must be a valid memnode id or backing workspace memory id", fieldName)
	}
	if strings.TrimSpace(parts[1]) != "workspace_memory" {
		return "", fmt.Errorf("%s must reference workspace_memory origin", fieldName)
	}
	return strings.TrimSpace(parts[2]), nil
}

func validateMemoryNodeWriteType(raw string) error {
	memoryType := strings.ToUpper(strings.TrimSpace(raw))
	if memoryType == "" {
		return nil
	}
	if _, ok := allowedMemoryNodeWriteTypes[memoryType]; ok {
		return nil
	}
	keys := make([]string, 0, len(allowedMemoryNodeWriteTypes))
	for key := range allowedMemoryNodeWriteTypes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return fmt.Errorf("memory_type must be one of: %s", strings.Join(keys, ", "))
}

var allowedMemoryNodeWriteTypes = map[string]struct{}{
	"ANTI_PROCEDURE":  {},
	"DECISION":        {},
	"ENTITY":          {},
	"EXPERIENCE":      {},
	"GOAL_COMMITMENT": {},
	"INCIDENT":        {},
	"LESSON":          {},
	"NOTE":            {},
	"POLICY_TRACE":    {},
	"PROCEDURE":       {},
	"SELF_MODEL":      {},
	"SUMMARY":         {},
	"UPDATE_DIGEST":   {},
}
