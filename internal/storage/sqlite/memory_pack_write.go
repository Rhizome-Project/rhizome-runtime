package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type MemoryPackWriteInput struct {
	SnapshotID             string
	WorkspaceID            string
	SessionID              string
	AgentID                string
	TriggerKind            string
	PackMode               string
	SourceWindowDigest     string
	TokenBudget            int
	MessageCountBefore     int
	MessageCountAfter      int
	MessageTokensBefore    int
	MessageTokensAfter     int
	TotalInputTokens       int
	TotalOutputTokens      int
	SummaryText            string
	SummaryWorkspaceMemory string
}

type MemoryPackWriteResult struct {
	WorkspaceID string                          `json:"workspace_id"`
	PackSource  string                          `json:"pack_source"`
	Status      string                          `json:"status"`
	Snapshot    SessionCompactionSnapshotRecord `json:"snapshot"`
	Pack        EpisodePackRecord               `json:"pack"`
}

func (s *Store) WriteMemoryPack(ctx context.Context, input MemoryPackWriteInput) (MemoryPackWriteResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	sessionID := strings.TrimSpace(input.SessionID)
	if workspaceID == "" {
		return MemoryPackWriteResult{}, errors.New("workspace_id is required")
	}
	if sessionID == "" {
		return MemoryPackWriteResult{}, errors.New("session_id is required")
	}
	if err := validateEpisodePackModeFilter(input.PackMode); err != nil {
		return MemoryPackWriteResult{}, err
	}
	if err := validateMemoryPackWriteTriggerKind(input.TriggerKind); err != nil {
		return MemoryPackWriteResult{}, err
	}
	existing, err := s.lookupMemoryPackExistingSnapshot(ctx, workspaceID, strings.TrimSpace(input.SnapshotID))
	if err != nil {
		return MemoryPackWriteResult{}, err
	}
	if existing != nil {
		if err := validateMemoryPackReplayInput(*existing, input); err != nil {
			return MemoryPackWriteResult{}, err
		}
		if strings.TrimSpace(existing.SessionID) != sessionID {
			return MemoryPackWriteResult{}, fmt.Errorf("snapshot_id does not belong to session_id")
		}
		if agentID := strings.TrimSpace(input.AgentID); agentID != "" && agentID != strings.TrimSpace(existing.AgentID) {
			return MemoryPackWriteResult{}, fmt.Errorf("snapshot_id does not belong to agent_id")
		}
		pack, err := s.GetEpisodePack(ctx, existing.WorkspaceID, existing.EpisodePackID)
		if err != nil {
			return MemoryPackWriteResult{}, err
		}
		return MemoryPackWriteResult{
			WorkspaceID: existing.WorkspaceID,
			PackSource:  "episode_pack",
			Status:      "RECORDED",
			Snapshot:    *existing,
			Pack:        pack,
		}, nil
	}
	if err := validateMemoryPackWriteCanonicalFields(input); err != nil {
		return MemoryPackWriteResult{}, err
	}
	if err := s.normalizeMemoryPackWriteAnchors(ctx, &input); err != nil {
		return MemoryPackWriteResult{}, err
	}
	if err := s.validateMemoryPackSummaryWorkspaceMemory(ctx, workspaceID, strings.TrimSpace(input.SummaryWorkspaceMemory)); err != nil {
		return MemoryPackWriteResult{}, err
	}

	snapshot, err := s.RecordSessionCompactionSnapshot(ctx, SessionCompactionSnapshotInput{
		SnapshotID:             strings.TrimSpace(input.SnapshotID),
		SessionID:              sessionID,
		WorkspaceID:            workspaceID,
		AgentID:                strings.TrimSpace(input.AgentID),
		TriggerKind:            strings.TrimSpace(input.TriggerKind),
		PackMode:               strings.TrimSpace(input.PackMode),
		SourceWindowDigest:     strings.TrimSpace(input.SourceWindowDigest),
		TokenBudget:            input.TokenBudget,
		MessageCountBefore:     input.MessageCountBefore,
		MessageCountAfter:      input.MessageCountAfter,
		MessageTokensBefore:    input.MessageTokensBefore,
		MessageTokensAfter:     input.MessageTokensAfter,
		TotalInputTokens:       input.TotalInputTokens,
		TotalOutputTokens:      input.TotalOutputTokens,
		SummaryText:            strings.TrimSpace(input.SummaryText),
		SummaryWorkspaceMemory: strings.TrimSpace(input.SummaryWorkspaceMemory),
	})
	if err != nil {
		return MemoryPackWriteResult{}, err
	}
	pack, err := s.GetEpisodePack(ctx, snapshot.WorkspaceID, snapshot.EpisodePackID)
	if err != nil {
		return MemoryPackWriteResult{}, err
	}
	return MemoryPackWriteResult{
		WorkspaceID: snapshot.WorkspaceID,
		PackSource:  "episode_pack",
		Status:      "RECORDED",
		Snapshot:    snapshot,
		Pack:        pack,
	}, nil
}

func validateMemoryPackWriteCanonicalFields(input MemoryPackWriteInput) error {
	if strings.EqualFold(strings.TrimSpace(input.PackMode), episodePackModeComplete) && episodePackLooksFallback(strings.TrimSpace(input.SummaryText)) {
		return errors.New("pack_mode COMPLETE is incompatible with fallback summary_text")
	}
	return nil
}

func (s *Store) normalizeMemoryPackWriteAnchors(ctx context.Context, input *MemoryPackWriteInput) error {
	if input == nil {
		return nil
	}
	state, err := s.GetAgentSessionState(ctx, strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.SessionID))
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID != "" && agentID != strings.TrimSpace(state.AgentID) {
		return fmt.Errorf("session_id does not belong to agent_id")
	}
	if agentID == "" {
		input.AgentID = strings.TrimSpace(state.AgentID)
	}
	return nil
}

func (s *Store) lookupMemoryPackExistingSnapshot(ctx context.Context, workspaceID, snapshotID string) (*SessionCompactionSnapshotRecord, error) {
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return nil, nil
	}
	var existingWorkspaceID string
	err := s.db.QueryRowContext(ctx, `SELECT workspace_id FROM session_compaction_snapshots WHERE snapshot_id = ?`, snapshotID).Scan(&existingWorkspaceID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		var existingPackWorkspaceID string
		err = s.db.QueryRowContext(ctx, `SELECT workspace_id FROM episode_packs WHERE pack_id = ?`, snapshotID).Scan(&existingPackWorkspaceID)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, nil
		case err != nil:
			return nil, fmt.Errorf("lookup episode pack id: %w", err)
		default:
			return nil, fmt.Errorf("snapshot_id collides with canonical episode pack in workspace %s", strings.TrimSpace(existingPackWorkspaceID))
		}
	case err != nil:
		return nil, fmt.Errorf("lookup compaction snapshot id: %w", err)
	case strings.TrimSpace(existingWorkspaceID) != strings.TrimSpace(workspaceID):
		return nil, fmt.Errorf("snapshot_id already belongs to workspace %s", strings.TrimSpace(existingWorkspaceID))
	default:
		record, err := s.getSessionCompactionSnapshot(ctx, workspaceID, snapshotID)
		if err != nil {
			return nil, err
		}
		return &record, nil
	}
}

func (s *Store) validateMemoryPackSummaryWorkspaceMemory(ctx context.Context, workspaceID, memoryID string) error {
	memoryID = strings.TrimSpace(memoryID)
	if memoryID == "" {
		return nil
	}
	record, err := s.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "workspace memory not found") {
			return fmt.Errorf("summary_workspace_memory must reference an existing workspace memory in workspace_id")
		}
		return err
	}
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		return fmt.Errorf("summary_workspace_memory is archived: %s", memoryID)
	}
	if normalizeWorkspaceMemorySourceKind(record.SourceKind) != "compaction" {
		return fmt.Errorf("summary_workspace_memory must reference compaction workspace memory")
	}
	return nil
}

func validateMemoryPackReplayInput(existing SessionCompactionSnapshotRecord, input MemoryPackWriteInput) error {
	mismatches := make([]string, 0, 8)
	if trimmed := strings.TrimSpace(input.TriggerKind); trimmed != "" && trimmed != strings.TrimSpace(existing.TriggerKind) {
		mismatches = append(mismatches, "trigger_kind")
	}
	if trimmed := strings.TrimSpace(input.PackMode); trimmed != "" && trimmed != strings.TrimSpace(existing.PackMode) {
		mismatches = append(mismatches, "pack_mode")
	}
	if trimmed := strings.TrimSpace(input.SourceWindowDigest); trimmed != "" && trimmed != strings.TrimSpace(existing.SourceWindowDigest) {
		mismatches = append(mismatches, "source_window_digest")
	}
	if trimmed := strings.TrimSpace(input.SummaryText); trimmed != "" && trimmed != strings.TrimSpace(existing.SummaryText) {
		mismatches = append(mismatches, "summary_text")
	}
	if trimmed := strings.TrimSpace(input.SummaryWorkspaceMemory); trimmed != "" && trimmed != strings.TrimSpace(existing.SummaryWorkspaceMemory) {
		mismatches = append(mismatches, "summary_workspace_memory")
	}
	if input.TokenBudget != 0 && input.TokenBudget != existing.TokenBudget {
		mismatches = append(mismatches, "token_budget")
	}
	if input.MessageCountBefore != 0 && input.MessageCountBefore != existing.MessageCountBefore {
		mismatches = append(mismatches, "message_count_before")
	}
	if input.MessageCountAfter != 0 && input.MessageCountAfter != existing.MessageCountAfter {
		mismatches = append(mismatches, "message_count_after")
	}
	if input.MessageTokensBefore != 0 && input.MessageTokensBefore != existing.MessageTokensBefore {
		mismatches = append(mismatches, "message_tokens_before")
	}
	if input.MessageTokensAfter != 0 && input.MessageTokensAfter != existing.MessageTokensAfter {
		mismatches = append(mismatches, "message_tokens_after")
	}
	if input.TotalInputTokens != 0 && input.TotalInputTokens != existing.TotalInputTokens {
		mismatches = append(mismatches, "total_input_tokens")
	}
	if input.TotalOutputTokens != 0 && input.TotalOutputTokens != existing.TotalOutputTokens {
		mismatches = append(mismatches, "total_output_tokens")
	}
	if len(mismatches) == 0 {
		return nil
	}
	sort.Strings(mismatches)
	return fmt.Errorf("snapshot_id replay payload does not match existing snapshot: %s", strings.Join(mismatches, ", "))
}

func validateMemoryPackWriteTriggerKind(raw string) error {
	triggerKind := strings.TrimSpace(raw)
	if triggerKind == "" {
		return nil
	}
	if _, ok := allowedMemoryPackWriteTriggerKinds[triggerKind]; ok {
		return nil
	}
	keys := make([]string, 0, len(allowedMemoryPackWriteTriggerKinds))
	for key := range allowedMemoryPackWriteTriggerKinds {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return fmt.Errorf("trigger_kind must be one of: %s", strings.Join(keys, ", "))
}

var allowedMemoryPackWriteTriggerKinds = map[string]struct{}{
	"manual_compaction":     {},
	"token_budget_exceeded": {},
}
