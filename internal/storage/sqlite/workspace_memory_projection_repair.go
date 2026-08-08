package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	workspaceMemoryProjectionRepairActionRepaired         = "REPAIRED"
	workspaceMemoryProjectionRepairActionSkipCurrent      = "SKIP_CURRENT"
	workspaceMemoryProjectionRepairActionSkipBacklogOnly  = "SKIP_BACKLOG_ONLY"
	workspaceMemoryProjectionRepairActionSkipUntrustedLag = "SKIP_UNTRUSTED_LAG"
	workspaceMemoryProjectionRepairActionSkipUnknown      = "SKIP_UNKNOWN"
	workspaceMemoryProjectionDefaultRepairLimit           = 25
)

type WorkspaceMemoryProjectionRepairFilter struct {
	WorkspaceID string   `json:"workspace_id"`
	MemoryIDs   []string `json:"memory_ids,omitempty"`
	Limit       int      `json:"limit,omitempty"`
}

type WorkspaceMemoryProjectionRepairItem struct {
	MemoryID                string   `json:"memory_id"`
	InvariantState          string   `json:"invariant_state"`
	IssueCodes              []string `json:"issue_codes,omitempty"`
	Action                  string   `json:"action"`
	DeletedCompetingAnchors int      `json:"deleted_competing_anchors,omitempty"`
}

type WorkspaceMemoryProjectionRepairResult struct {
	WorkspaceID             string                                `json:"workspace_id"`
	Limit                   int                                   `json:"limit"`
	Examined                int                                   `json:"examined"`
	Repaired                int                                   `json:"repaired"`
	SkippedCurrent          int                                   `json:"skipped_current"`
	SkippedBacklogOnly      int                                   `json:"skipped_backlog_only"`
	SkippedUntrustedLag     int                                   `json:"skipped_untrusted_lag"`
	SkippedUnknown          int                                   `json:"skipped_unknown"`
	DeletedCompetingAnchors int                                   `json:"deleted_competing_anchors"`
	Items                   []WorkspaceMemoryProjectionRepairItem `json:"items,omitempty"`
}

func (s *Store) RepairWorkspaceMemoryProjectionWorkspace(ctx context.Context, filter WorkspaceMemoryProjectionRepairFilter) (WorkspaceMemoryProjectionRepairResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return WorkspaceMemoryProjectionRepairResult{}, errors.New("workspace_id is required")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = workspaceMemoryProjectionDefaultRepairLimit
	}
	targets, err := s.listWorkspaceMemoryProjectionRepairTargets(ctx, workspaceID, filter.MemoryIDs, limit)
	if err != nil {
		return WorkspaceMemoryProjectionRepairResult{}, err
	}
	lag, err := s.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		return WorkspaceMemoryProjectionRepairResult{}, fmt.Errorf("load memory projection lag snapshot: %w", err)
	}
	result := WorkspaceMemoryProjectionRepairResult{
		WorkspaceID: workspaceID,
		Limit:       limit,
		Items:       make([]WorkspaceMemoryProjectionRepairItem, 0, len(targets)),
	}
	for _, record := range targets {
		invariant, err := s.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, lag)
		if err != nil {
			return result, fmt.Errorf("evaluate workspace memory projection invariant for %s: %w", record.MemoryID, err)
		}
		item := WorkspaceMemoryProjectionRepairItem{
			MemoryID:       record.MemoryID,
			InvariantState: invariant.State,
			IssueCodes:     workspaceMemoryProjectionIssueCodes(invariant.Issues),
		}
		result.Examined++
		switch action := workspaceMemoryProjectionRepairActionFor(invariant); action {
		case workspaceMemoryProjectionRepairActionSkipCurrent:
			item.Action = action
			result.SkippedCurrent++
		case workspaceMemoryProjectionRepairActionSkipBacklogOnly:
			item.Action = action
			result.SkippedBacklogOnly++
		case workspaceMemoryProjectionRepairActionSkipUntrustedLag:
			item.Action = action
			result.SkippedUntrustedLag++
		case workspaceMemoryProjectionRepairActionSkipUnknown:
			item.Action = action
			result.SkippedUnknown++
		case workspaceMemoryProjectionRepairActionRepaired:
			deleted, err := s.repairWorkspaceMemoryProjectionRecord(ctx, record)
			if err != nil {
				return result, fmt.Errorf("repair workspace memory projection %s: %w", record.MemoryID, err)
			}
			item.Action = action
			item.DeletedCompetingAnchors = deleted
			result.Repaired++
			result.DeletedCompetingAnchors += deleted
		default:
			return result, fmt.Errorf("unsupported workspace memory projection repair action %q", action)
		}
		result.Items = append(result.Items, item)
	}
	return result, nil
}

func (s *Store) listWorkspaceMemoryProjectionRepairTargets(ctx context.Context, workspaceID string, memoryIDs []string, limit int) ([]WorkspaceMemoryRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = workspaceMemoryProjectionDefaultRepairLimit
	}
	trimmedIDs := make([]string, 0, len(memoryIDs))
	seen := make(map[string]struct{}, len(memoryIDs))
	for _, raw := range memoryIDs {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		trimmedIDs = append(trimmedIDs, trimmed)
	}

	query := strings.Builder{}
	query.WriteString(`SELECT memory_id, workspace_id, memory_type, title, body, summary,
	        COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
	        source_kind, source_id, tags_json, importance, confidence,
	        created_at, updated_at, archived_at, archived_by, archived_reason, recovery_reason
	   FROM workspace_memory
	  WHERE workspace_id = ?`)
	args := make([]any, 0, 2+len(trimmedIDs))
	args = append(args, workspaceID)
	if len(trimmedIDs) > 0 {
		query.WriteString(` AND memory_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(trimmedIDs)), ",") + `)`)
		for _, memoryID := range trimmedIDs {
			args = append(args, memoryID)
		}
	}
	query.WriteString(` ORDER BY updated_at DESC, importance DESC, memory_id DESC LIMIT ?`)
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list workspace memory projection repair targets: %w", err)
	}
	defer rows.Close()
	return collectWorkspaceMemoryRows(rows)
}

func workspaceMemoryProjectionIssuesRepairable(issues []WorkspaceMemoryProjectionInvariantIssue) bool {
	if len(issues) == 0 {
		return false
	}
	for _, issue := range issues {
		switch strings.TrimSpace(issue.Code) {
		case "MISSING_ANCHOR",
			"DUPLICATE_ANCHOR",
			"CONFLICTING_NODE_ID",
			"ORIGIN_KIND_MISMATCH",
			"ORIGIN_ID_MISMATCH",
			"ANCHOR_NODE_ID_MISMATCH",
			"SEMANTIC_LINEAGE_MISMATCH",
			"MEMORY_TYPE_MISMATCH",
			"COMPAT_TYPE_MISMATCH",
			"LIFECYCLE_MISMATCH",
			"MEMORY_LAYER_MISMATCH",
			"SOURCE_KIND_MISMATCH",
			"SOURCE_ID_MISMATCH",
			"TITLE_MISMATCH",
			"BODY_MISMATCH",
			"SUMMARY_MISMATCH",
			"ARCHIVE_STATE_MISMATCH",
			"ARCHIVED_REASON_MISMATCH",
			"RECOVERY_REASON_MISMATCH":
			continue
		default:
			return false
		}
	}
	return true
}

func workspaceMemoryProjectionRepairActionFor(invariant WorkspaceMemoryProjectionInvariant) string {
	if len(invariant.Issues) == 0 {
		if invariant.State == WorkspaceMemoryProjectionInvariantCurrent {
			return workspaceMemoryProjectionRepairActionSkipCurrent
		}
		return workspaceMemoryProjectionRepairActionSkipBacklogOnly
	}
	if firstNonEmpty(strings.TrimSpace(invariant.ProjectionLagState), "unknown") != "ok" {
		return workspaceMemoryProjectionRepairActionSkipUntrustedLag
	}
	if !workspaceMemoryProjectionIssuesRepairable(invariant.Issues) {
		return workspaceMemoryProjectionRepairActionSkipUnknown
	}
	return workspaceMemoryProjectionRepairActionRepaired
}

func workspaceMemoryProjectionIssueCodes(items []WorkspaceMemoryProjectionInvariantIssue) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if code := strings.TrimSpace(item.Code); code != "" {
			out = append(out, code)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *Store) repairWorkspaceMemoryProjectionRecord(ctx context.Context, record WorkspaceMemoryRecord) (int, error) {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin workspace memory projection repair tx: %w", err)
	}
	canonical, err := loadWorkspaceMemoryRecordTx(ctx, tx, record.WorkspaceID, record.MemoryID)
	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("canonical workspace memory %s not found", record.MemoryID)
		}
		return 0, fmt.Errorf("load canonical workspace memory: %w", err)
	}
	deleted, err := s.deleteWorkspaceMemoryProjectionCompetingNodesTx(ctx, tx, canonical.WorkspaceID, canonical.MemoryID)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if err := s.syncWorkspaceMemoryGraphTx(ctx, tx, canonical); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("sync canonical workspace memory projection: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit workspace memory projection repair tx: %w", err)
	}
	return deleted, nil
}

func (s *Store) deleteWorkspaceMemoryProjectionCompetingNodesTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) (int, error) {
	expectedNodeID := memoryGraphNodeID("workspace_memory", memoryID)
	rows, err := tx.QueryContext(
		ctx,
		`SELECT memory_id
		   FROM memory_nodes
		  WHERE workspace_id = ?
		    AND origin_kind = 'workspace_memory'
		    AND origin_id = ?
		    AND memory_id <> ?
		  ORDER BY updated_at DESC, memory_id DESC`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(memoryID),
		expectedNodeID,
	)
	if err != nil {
		return 0, fmt.Errorf("list competing workspace memory projection anchors: %w", err)
	}
	defer rows.Close()

	var strayIDs []string
	for rows.Next() {
		var strayID string
		if err := rows.Scan(&strayID); err != nil {
			return 0, fmt.Errorf("scan competing workspace memory projection anchor: %w", err)
		}
		if trimmed := strings.TrimSpace(strayID); trimmed != "" {
			strayIDs = append(strayIDs, trimmed)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate competing workspace memory projection anchors: %w", err)
	}

	deleted := 0
	for _, strayID := range strayIDs {
		if err := deleteMemoryGraphNodeWithEdgesTx(ctx, tx, workspaceID, strayID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
