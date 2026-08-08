package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	memoryProjectionKindWorkspaceMemory = "WORKSPACE_MEMORY"
	memoryProjectionKindKnowledgeClaim  = "KNOWLEDGE_CLAIM"
	memoryProjectionKindEpisodePack     = "EPISODE_PACK"

	memoryProjectionStatusPending    = "PENDING"
	memoryProjectionStatusProcessing = "PROCESSING"
	memoryProjectionStatusFailed     = "FAILED"
	memoryProjectionStatusDone       = "DONE"

	memoryProjectionDefaultReconcileLimit = 64
	memoryProjectionEnqueueBatchSize      = 128
	memoryProjectionRetryBaseDelay        = 5 * time.Second
	memoryProjectionRetryMaxDelay         = 5 * time.Minute
	memoryProjectionProcessingStaleAfter  = 2 * time.Minute
)

type MemoryProjectionOutboxRecord struct {
	ProjectionID   string  `json:"projection_id"`
	WorkspaceID    string  `json:"workspace_id"`
	ProjectionKind string  `json:"projection_kind"`
	OriginID       string  `json:"origin_id"`
	Status         string  `json:"status"`
	AttemptCount   int     `json:"attempt_count"`
	LastError      string  `json:"last_error,omitempty"`
	AvailableAt    string  `json:"available_at"`
	EnqueuedAt     string  `json:"enqueued_at"`
	StartedAt      *string `json:"started_at,omitempty"`
	CompletedAt    *string `json:"completed_at,omitempty"`
	UpdatedAt      string  `json:"updated_at"`
}

type MemoryProjectionReconcileResult struct {
	WorkspaceID string `json:"workspace_id"`
	Processed   int    `json:"processed"`
	Failed      int    `json:"failed"`
}

type MemoryProjectionRebuildFilter struct {
	WorkspaceID string   `json:"workspace_id"`
	Kinds       []string `json:"kinds,omitempty"`
	Limit       int      `json:"limit"`
}

type MemoryProjectionRebuildResult struct {
	WorkspaceID   string   `json:"workspace_id"`
	Kinds         []string `json:"kinds"`
	TargetsQueued int      `json:"targets_queued"`
	Processed     int      `json:"processed"`
	Failed        int      `json:"failed"`
	Pending       int      `json:"pending"`
	Processing    int      `json:"processing"`
	FailedPending int      `json:"failed_pending"`
	Done          int      `json:"done"`
}

type MemoryProjectionLagSnapshot struct {
	State                   string `json:"state"`
	Message                 string `json:"message,omitempty"`
	PendingCount            int    `json:"pending_count"`
	ProcessingCount         int    `json:"processing_count"`
	FailedCount             int    `json:"failed_count"`
	OldestPendingAt         string `json:"oldest_pending_at,omitempty"`
	OldestPendingAgeSeconds *int64 `json:"oldest_pending_age_seconds,omitempty"`
	Error                   string `json:"error,omitempty"`
}

func normalizeMemoryProjectionKind(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case memoryProjectionKindWorkspaceMemory:
		return memoryProjectionKindWorkspaceMemory
	case memoryProjectionKindKnowledgeClaim:
		return memoryProjectionKindKnowledgeClaim
	case memoryProjectionKindEpisodePack:
		return memoryProjectionKindEpisodePack
	default:
		return ""
	}
}

func normalizeMemoryProjectionKinds(raw []string) []string {
	if len(raw) == 0 {
		return []string{
			memoryProjectionKindWorkspaceMemory,
			memoryProjectionKindKnowledgeClaim,
			memoryProjectionKindEpisodePack,
		}
	}
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		kind := normalizeMemoryProjectionKind(item)
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		out = append(out, kind)
	}
	if len(out) == 0 {
		return []string{
			memoryProjectionKindWorkspaceMemory,
			memoryProjectionKindKnowledgeClaim,
			memoryProjectionKindEpisodePack,
		}
	}
	return out
}

func memoryProjectionOriginKind(projectionKind string) string {
	switch normalizeMemoryProjectionKind(projectionKind) {
	case memoryProjectionKindWorkspaceMemory:
		return "workspace_memory"
	case memoryProjectionKindKnowledgeClaim:
		return "knowledge_claim"
	case memoryProjectionKindEpisodePack:
		return "episode_pack"
	default:
		return ""
	}
}

func memoryProjectionOutboxID(workspaceID, projectionKind, originID string) string {
	return strings.Join([]string{
		"mproj",
		strings.TrimSpace(workspaceID),
		strings.ToLower(strings.TrimSpace(projectionKind)),
		strings.TrimSpace(originID),
	}, ":")
}

func memoryProjectionOriginMemoryID(projectionKind, originID string) string {
	switch normalizeMemoryProjectionKind(projectionKind) {
	case memoryProjectionKindWorkspaceMemory:
		return memoryGraphNodeID("workspace_memory", originID)
	case memoryProjectionKindKnowledgeClaim:
		return memoryGraphNodeID("knowledge_claim", originID)
	case memoryProjectionKindEpisodePack:
		return memoryGraphNodeID("episode_pack", originID)
	default:
		return ""
	}
}

func (s *Store) enqueueMemoryProjectionOutboxTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionKind, originID, now string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	projectionKind = normalizeMemoryProjectionKind(projectionKind)
	originID = strings.TrimSpace(originID)
	now = memoryProjectionClampOperationalNow(now)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if projectionKind == "" {
		return errors.New("projection_kind is required")
	}
	if originID == "" {
		return errors.New("origin_id is required")
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO memory_projection_outbox(
		    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		    available_at, enqueued_at, started_at, completed_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, 0, '', ?, ?, NULL, NULL, ?)
		  ON CONFLICT(workspace_id, projection_kind, origin_id) DO UPDATE SET
		    status = excluded.status,
		    last_error = '',
		    available_at = excluded.available_at,
		    started_at = NULL,
		    completed_at = NULL,
		    updated_at = excluded.updated_at`,
		memoryProjectionOutboxID(workspaceID, projectionKind, originID),
		workspaceID,
		projectionKind,
		originID,
		memoryProjectionStatusPending,
		now,
		now,
		now,
	); err != nil {
		return fmt.Errorf("enqueue memory projection outbox: %w", err)
	}
	return nil
}

func (s *Store) enqueueMemoryProjectionOutboxBatchTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionKind string, originIDs []string, now string) (int, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectionKind = normalizeMemoryProjectionKind(projectionKind)
	now = memoryProjectionClampOperationalNow(now)
	if workspaceID == "" {
		return 0, errors.New("workspace_id is required")
	}
	if projectionKind == "" {
		return 0, errors.New("projection_kind is required")
	}
	if len(originIDs) == 0 {
		return 0, nil
	}

	clean := normalizeMemoryProjectionOriginIDs(originIDs)
	if len(clean) == 0 {
		return 0, nil
	}

	queued := 0
	for start := 0; start < len(clean); start += memoryProjectionEnqueueBatchSize {
		end := start + memoryProjectionEnqueueBatchSize
		if end > len(clean) {
			end = len(clean)
		}
		if err := s.enqueueMemoryProjectionOutboxChunkTx(ctx, tx, workspaceID, projectionKind, clean[start:end], now); err != nil {
			return queued, err
		}
		queued += end - start
	}
	return queued, nil
}

func (s *Store) enqueueMemoryProjectionOutboxChunkTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionKind string, originIDs []string, now string) error {
	var query strings.Builder
	args := make([]any, 0, len(originIDs)*8)
	query.WriteString(`INSERT INTO memory_projection_outbox(
		    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		    available_at, enqueued_at, started_at, completed_at, updated_at
		  ) VALUES `)
	for idx, originID := range originIDs {
		if idx > 0 {
			query.WriteString(", ")
		}
		query.WriteString("(?, ?, ?, ?, ?, 0, '', ?, ?, NULL, NULL, ?)")
		args = append(args,
			memoryProjectionOutboxID(workspaceID, projectionKind, originID),
			workspaceID,
			projectionKind,
			originID,
			memoryProjectionStatusPending,
			now,
			now,
			now,
		)
	}
	query.WriteString(`
		  ON CONFLICT(workspace_id, projection_kind, origin_id) DO UPDATE SET
		    status = excluded.status,
		    last_error = '',
		    available_at = excluded.available_at,
		    started_at = NULL,
		    completed_at = NULL,
		    updated_at = excluded.updated_at`)
	if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("enqueue memory projection outbox batch: %w", err)
	}
	return nil
}

func normalizeMemoryProjectionOriginIDs(originIDs []string) []string {
	if len(originIDs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(originIDs))
	out := make([]string, 0, len(originIDs))
	for _, raw := range originIDs {
		originID := strings.TrimSpace(raw)
		if originID == "" {
			continue
		}
		if _, ok := seen[originID]; ok {
			continue
		}
		seen[originID] = struct{}{}
		out = append(out, originID)
	}
	sort.Strings(out)
	return out
}

func (s *Store) listMemoryProjectionOutbox(ctx context.Context, workspaceID string) ([]MemoryProjectionOutboxRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		        available_at, enqueued_at, started_at, completed_at, updated_at
		   FROM memory_projection_outbox
		  WHERE workspace_id = ?
		  ORDER BY updated_at ASC, projection_id ASC`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory projection outbox: %w", err)
	}
	defer rows.Close()
	return collectMemoryProjectionOutboxRows(rows)
}

func (s *Store) listPendingMemoryProjectionOutbox(ctx context.Context, workspaceID string, limit int) ([]MemoryProjectionOutboxRecord, error) {
	return s.listPendingMemoryProjectionOutboxKinds(ctx, workspaceID, nil, limit)
}

func (s *Store) ListMemoryProjectionPendingWorkspaces(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 16
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT workspace_id
		   FROM memory_projection_outbox
		  WHERE status IN (?, ?)
		    AND available_at GLOB '????-??-??T??:??:??*Z'
		    AND available_at <= ?
		  GROUP BY workspace_id
		  ORDER BY MIN(updated_at) ASC, workspace_id ASC
		  LIMIT ?`,
		memoryProjectionStatusPending,
		memoryProjectionStatusFailed,
		now,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending memory projection workspaces: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			return nil, fmt.Errorf("scan pending memory projection workspace: %w", err)
		}
		workspaceID = strings.TrimSpace(workspaceID)
		if workspaceID == "" {
			continue
		}
		out = append(out, workspaceID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("collect pending memory projection workspaces: %w", err)
	}
	return out, nil
}

func (s *Store) ReclaimStaleMemoryProjectionProcessing(ctx context.Context, staleAfter time.Duration, limit int) (int, error) {
	if staleAfter <= 0 {
		staleAfter = memoryProjectionProcessingStaleAfter
	}
	if limit <= 0 {
		limit = memoryProjectionDefaultReconcileLimit
	}
	quarantined, err := s.quarantineMalformedMemoryProjectionAvailableAt(ctx, "", nil, limit)
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().UTC().Add(-staleAfter)
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT projection_id, workspace_id, started_at, updated_at, enqueued_at, available_at
		   FROM memory_projection_outbox
		  WHERE status = ?
		  ORDER BY COALESCE(started_at, updated_at, enqueued_at, available_at) ASC, updated_at ASC, projection_id ASC
		  LIMIT ?`,
		memoryProjectionStatusProcessing,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("list stale processing memory projection rows: %w", err)
	}
	defer rows.Close()

	type staleRow struct {
		projectionID string
		workspaceID  string
		reason       string
	}
	reclaim := make([]staleRow, 0, limit)
	for rows.Next() {
		var (
			row                                staleRow
			startedAt                          sql.NullString
			updatedAt, enqueuedAt, availableAt string
		)
		if err := rows.Scan(&row.projectionID, &row.workspaceID, &startedAt, &updatedAt, &enqueuedAt, &availableAt); err != nil {
			return 0, fmt.Errorf("scan stale processing memory projection row: %w", err)
		}
		referenceTS, hasReference, malformedStartedAt := memoryProjectionProcessingReferenceTime(startedAt, updatedAt, enqueuedAt, availableAt)
		if hasReference && referenceTS.After(cutoff) {
			continue
		}
		row.reason = "processing row reclaimed after stale timeout"
		if malformedStartedAt {
			row.reason = "processing row reclaimed after stale timeout; malformed started_at"
		}
		if !hasReference {
			row.reason = "processing row reclaimed after malformed timestamps"
		}
		reclaim = append(reclaim, row)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate stale processing memory projection rows: %w", err)
	}
	if len(reclaim) == 0 {
		return quarantined, nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin stale memory projection reclaim tx: %w", err)
	}
	reclaimed := 0
	for _, row := range reclaim {
		result, execErr := tx.ExecContext(
			ctx,
			`UPDATE memory_projection_outbox
			    SET status = ?, available_at = ?, started_at = NULL, completed_at = NULL, updated_at = ?, last_error = ?
			  WHERE projection_id = ? AND workspace_id = ? AND status = ?`,
			memoryProjectionStatusFailed,
			now,
			now,
			row.reason,
			row.projectionID,
			row.workspaceID,
			memoryProjectionStatusProcessing,
		)
		if execErr != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("reclaim stale memory projection row %s/%s: %w", row.workspaceID, row.projectionID, execErr)
		}
		if affected, _ := result.RowsAffected(); affected > 0 {
			reclaimed++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit stale memory projection reclaim tx: %w", err)
	}
	return reclaimed + quarantined, nil
}

func (s *Store) quarantineMalformedMemoryProjectionAvailableAt(ctx context.Context, workspaceID string, kinds []string, limit int) (int, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if limit <= 0 {
		limit = memoryProjectionDefaultReconcileLimit
	}
	filterKinds := normalizeMemoryProjectionKinds(kinds)
	args := make([]any, 0, 5+len(filterKinds))
	args = append(args, memoryProjectionStatusPending, memoryProjectionStatusFailed)
	query := `SELECT projection_id, workspace_id, available_at
		   FROM memory_projection_outbox
		  WHERE status IN (?, ?)`
	if workspaceID != "" {
		query += "\n\t\t    AND workspace_id = ?"
		args = append(args, workspaceID)
	}
	if len(kinds) > 0 {
		placeholders := make([]string, 0, len(filterKinds))
		for _, kind := range filterKinds {
			placeholders = append(placeholders, "?")
			args = append(args, kind)
		}
		query += fmt.Sprintf("\n\t\t    AND projection_kind IN (%s)", strings.Join(placeholders, ", "))
	}
	query += `
		  ORDER BY CASE
		             WHEN TRIM(available_at) = '' OR available_at NOT GLOB '????-??-??T??:??:??*Z' THEN 0
		             ELSE 1
		           END,
		           updated_at ASC, projection_id ASC
		  LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("list malformed memory projection timestamps: %w", err)
	}
	defer rows.Close()

	type malformedRow struct {
		projectionID string
		workspaceID  string
	}
	quarantine := make([]malformedRow, 0, limit)
	for rows.Next() {
		var row malformedRow
		var availableAt string
		if err := rows.Scan(&row.projectionID, &row.workspaceID, &availableAt); err != nil {
			return 0, fmt.Errorf("scan malformed memory projection timestamp row: %w", err)
		}
		if _, ok := memoryProjectionQueueTimestamp(availableAt); ok {
			continue
		}
		quarantine = append(quarantine, row)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate malformed memory projection timestamp rows: %w", err)
	}
	if len(quarantine) == 0 {
		return 0, nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin malformed memory projection quarantine tx: %w", err)
	}
	quarantined := 0
	for _, row := range quarantine {
		result, execErr := tx.ExecContext(
			ctx,
			`UPDATE memory_projection_outbox
			    SET status = ?, available_at = ?, started_at = NULL, completed_at = NULL, updated_at = ?, last_error = ?
			  WHERE projection_id = ? AND workspace_id = ? AND status IN (?, ?)`,
			memoryProjectionStatusFailed,
			now,
			now,
			"malformed projection available_at quarantined",
			row.projectionID,
			row.workspaceID,
			memoryProjectionStatusPending,
			memoryProjectionStatusFailed,
		)
		if execErr != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("quarantine malformed memory projection timestamp %s/%s: %w", row.workspaceID, row.projectionID, execErr)
		}
		if affected, _ := result.RowsAffected(); affected > 0 {
			quarantined++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit malformed memory projection quarantine tx: %w", err)
	}
	return quarantined, nil
}

func (s *Store) listPendingMemoryProjectionOutboxKinds(ctx context.Context, workspaceID string, kinds []string, limit int) ([]MemoryProjectionOutboxRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = memoryProjectionDefaultReconcileLimit
	}
	filterKinds := normalizeMemoryProjectionKinds(kinds)
	now := time.Now().UTC()
	args := make([]any, 0, 5+len(filterKinds))
	args = append(args, workspaceID, memoryProjectionStatusPending, memoryProjectionStatusFailed, now.Format(time.RFC3339Nano))
	query := `SELECT projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		        available_at, enqueued_at, started_at, completed_at, updated_at
		   FROM memory_projection_outbox
		  WHERE workspace_id = ?
		    AND status IN (?, ?)
		    AND available_at GLOB '????-??-??T??:??:??*Z'
		    AND available_at <= ?`
	if len(kinds) > 0 {
		placeholders := make([]string, 0, len(filterKinds))
		for _, kind := range filterKinds {
			placeholders = append(placeholders, "?")
			args = append(args, kind)
		}
		query += fmt.Sprintf("\n\t\t    AND projection_kind IN (%s)", strings.Join(placeholders, ", "))
	}
	query += `
		  ORDER BY CASE status WHEN ? THEN 0 ELSE 1 END, available_at ASC, updated_at ASC, projection_id ASC
		  LIMIT ?`
	args = append(args, memoryProjectionStatusPending, limit)
	rows, err := s.db.QueryContext(
		ctx,
		query,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending memory projection outbox: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryProjectionOutboxRows(rows)
	if err != nil {
		return nil, err
	}
	eligible := make([]MemoryProjectionOutboxRecord, 0, len(items))
	for _, item := range items {
		if !memoryProjectionAvailableBy(item.AvailableAt, now) {
			continue
		}
		eligible = append(eligible, item)
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		left := eligible[i]
		right := eligible[j]
		leftPriority := 1
		if left.Status == memoryProjectionStatusPending {
			leftPriority = 0
		}
		rightPriority := 1
		if right.Status == memoryProjectionStatusPending {
			rightPriority = 0
		}
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		leftAvailable := memoryProjectionTimeOrZero(left.AvailableAt)
		rightAvailable := memoryProjectionTimeOrZero(right.AvailableAt)
		if !leftAvailable.Equal(rightAvailable) {
			return leftAvailable.Before(rightAvailable)
		}
		leftUpdated := memoryProjectionTimeOrZero(left.UpdatedAt)
		rightUpdated := memoryProjectionTimeOrZero(right.UpdatedAt)
		if !leftUpdated.Equal(rightUpdated) {
			return leftUpdated.Before(rightUpdated)
		}
		return left.ProjectionID < right.ProjectionID
	})
	if len(eligible) > limit {
		eligible = eligible[:limit]
	}
	return eligible, nil
}

func (s *Store) MemoryProjectionLagSnapshot(ctx context.Context) (MemoryProjectionLagSnapshot, error) {
	var snapshot MemoryProjectionLagSnapshot
	var oldestPending sql.NullString
	row := s.db.QueryRowContext(
		ctx,
		`SELECT
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS pending_count,
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS processing_count,
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS failed_count,
		    MIN(CASE WHEN status = ? THEN available_at ELSE NULL END) AS oldest_pending_at
		   FROM memory_projection_outbox`,
		memoryProjectionStatusPending,
		memoryProjectionStatusProcessing,
		memoryProjectionStatusFailed,
		memoryProjectionStatusPending,
	)
	if err := row.Scan(&snapshot.PendingCount, &snapshot.ProcessingCount, &snapshot.FailedCount, &oldestPending); err != nil {
		return MemoryProjectionLagSnapshot{}, fmt.Errorf("collect memory projection lag snapshot: %w", err)
	}

	if snapshot.PendingCount == 0 && snapshot.ProcessingCount == 0 && snapshot.FailedCount == 0 {
		snapshot.State = "ok"
		snapshot.Message = "memory projection outbox is empty"
		return snapshot, nil
	}

	snapshot.State = "degraded"
	reasons := make([]string, 0, 2)
	if snapshot.PendingCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d pending projection(s)", snapshot.PendingCount))
	}
	if snapshot.ProcessingCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d processing projection(s)", snapshot.ProcessingCount))
	}
	if snapshot.FailedCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d failed projection(s)", snapshot.FailedCount))
	}

	if oldestPending.Valid {
		snapshot.OldestPendingAt = oldestPending.String
		if ts, err := time.Parse(time.RFC3339Nano, oldestPending.String); err == nil {
			age := time.Since(ts).Round(time.Second)
			ageSeconds := int64(age / time.Second)
			snapshot.OldestPendingAgeSeconds = &ageSeconds
		} else {
			reasons = append(reasons, "oldest pending timestamp could not be parsed")
		}
	}

	if len(reasons) == 0 {
		reasons = append(reasons, "memory projection outbox has backlog")
	}
	snapshot.Message = strings.Join(reasons, "; ")
	return snapshot, nil
}

func (s *Store) memoryProjectionLagSnapshotForWorkspaceKinds(ctx context.Context, workspaceID string, kinds []string) (MemoryProjectionLagSnapshot, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryProjectionLagSnapshot{}, errors.New("workspace_id is required")
	}
	kinds = normalizeMemoryProjectionKinds(kinds)
	placeholders := make([]string, 0, len(kinds))
	args := make([]any, 0, 5+len(kinds))
	args = append(args, memoryProjectionStatusPending, memoryProjectionStatusProcessing, memoryProjectionStatusFailed, memoryProjectionStatusPending, workspaceID)
	for _, kind := range kinds {
		placeholders = append(placeholders, "?")
		args = append(args, kind)
	}

	var snapshot MemoryProjectionLagSnapshot
	var oldestPending sql.NullString
	query := fmt.Sprintf(
		`SELECT
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS pending_count,
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS processing_count,
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS failed_count,
		    MIN(CASE WHEN status = ? THEN available_at ELSE NULL END) AS oldest_pending_at
		   FROM memory_projection_outbox
		  WHERE workspace_id = ?
		    AND projection_kind IN (%s)`,
		strings.Join(placeholders, ", "),
	)
	row := s.db.QueryRowContext(ctx, query, args...)
	if err := row.Scan(&snapshot.PendingCount, &snapshot.ProcessingCount, &snapshot.FailedCount, &oldestPending); err != nil {
		return MemoryProjectionLagSnapshot{}, fmt.Errorf("collect memory projection lag snapshot for workspace kinds: %w", err)
	}

	if snapshot.PendingCount == 0 && snapshot.ProcessingCount == 0 && snapshot.FailedCount == 0 {
		snapshot.State = "ok"
		snapshot.Message = "projection surface is current"
		return snapshot, nil
	}

	snapshot.State = "degraded"
	reasons := make([]string, 0, 3)
	if snapshot.PendingCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d pending projection(s)", snapshot.PendingCount))
	}
	if snapshot.ProcessingCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d processing projection(s)", snapshot.ProcessingCount))
	}
	if snapshot.FailedCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d failed projection(s)", snapshot.FailedCount))
	}
	if oldestPending.Valid {
		snapshot.OldestPendingAt = oldestPending.String
		if ts, err := time.Parse(time.RFC3339Nano, oldestPending.String); err == nil {
			age := time.Since(ts).Round(time.Second)
			ageSeconds := int64(age / time.Second)
			snapshot.OldestPendingAgeSeconds = &ageSeconds
		} else {
			reasons = append(reasons, "oldest pending timestamp could not be parsed")
		}
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "projection surface has backlog")
	}
	snapshot.Message = strings.Join(reasons, "; ")
	return snapshot, nil
}

func collectMemoryProjectionOutboxRows(rows *sql.Rows) ([]MemoryProjectionOutboxRecord, error) {
	out := make([]MemoryProjectionOutboxRecord, 0)
	for rows.Next() {
		record, err := scanMemoryProjectionOutboxRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory projection outbox: %w", err)
	}
	return out, nil
}

func scanMemoryProjectionOutboxRecord(scanner interface{ Scan(dest ...any) error }) (MemoryProjectionOutboxRecord, error) {
	var record MemoryProjectionOutboxRecord
	var startedAt, completedAt sql.NullString
	if err := scanner.Scan(
		&record.ProjectionID,
		&record.WorkspaceID,
		&record.ProjectionKind,
		&record.OriginID,
		&record.Status,
		&record.AttemptCount,
		&record.LastError,
		&record.AvailableAt,
		&record.EnqueuedAt,
		&startedAt,
		&completedAt,
		&record.UpdatedAt,
	); err != nil {
		return MemoryProjectionOutboxRecord{}, err
	}
	if startedAt.Valid {
		record.StartedAt = &startedAt.String
	}
	if completedAt.Valid {
		record.CompletedAt = &completedAt.String
	}
	return record, nil
}

func (s *Store) bestEffortReconcileMemoryProjectionWorkspace(ctx context.Context, workspaceID string) {
	_, _ = s.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, memoryProjectionDefaultReconcileLimit)
}

func (s *Store) RebuildMemoryProjectionWorkspace(ctx context.Context, filter MemoryProjectionRebuildFilter) (MemoryProjectionRebuildResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return MemoryProjectionRebuildResult{}, errors.New("workspace_id is required")
	}
	kinds := normalizeMemoryProjectionKinds(filter.Kinds)
	result := MemoryProjectionRebuildResult{
		WorkspaceID: workspaceID,
		Kinds:       append([]string(nil), kinds...),
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return result, fmt.Errorf("begin memory projection rebuild tx: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return result, err
	}
	targets, err := s.collectMemoryProjectionRebuildTargetsTx(ctx, tx, workspaceID, kinds)
	if err != nil {
		_ = tx.Rollback()
		return result, err
	}
	for _, kind := range kinds {
		origins := targets[kind]
		queued, err := s.enqueueMemoryProjectionOutboxBatchTx(ctx, tx, workspaceID, kind, origins, now)
		if err != nil {
			_ = tx.Rollback()
			return result, err
		}
		result.TargetsQueued += queued
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit memory projection rebuild tx: %w", err)
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = maxInt(result.TargetsQueued, memoryProjectionDefaultReconcileLimit)
	}
	reconcileResult, reconcileErr := s.reconcileMemoryProjectionWorkspaceKinds(ctx, workspaceID, kinds, limit)
	result.Processed = reconcileResult.Processed
	result.Failed = reconcileResult.Failed

	pending, processing, failed, done, summaryErr := s.summarizeMemoryProjectionOutbox(ctx, workspaceID, kinds)
	result.Pending = pending
	result.Processing = processing
	result.FailedPending = failed
	result.Done = done

	if reconcileErr != nil && summaryErr != nil {
		return result, fmt.Errorf("%v; %w", reconcileErr, summaryErr)
	}
	if reconcileErr != nil {
		return result, reconcileErr
	}
	if summaryErr != nil {
		return result, summaryErr
	}
	return result, nil
}

func (s *Store) collectMemoryProjectionRebuildTargetsTx(ctx context.Context, tx *sql.Tx, workspaceID string, kinds []string) (map[string][]string, error) {
	targets := make(map[string][]string, len(kinds))
	for _, kind := range kinds {
		seen := make(map[string]struct{})
		sourceIDs, err := s.listMemoryProjectionSourceIDsTx(ctx, tx, workspaceID, kind)
		if err != nil {
			return nil, err
		}
		for _, id := range sourceIDs {
			seen[id] = struct{}{}
		}
		graphIDs, err := s.listMemoryProjectionGraphOriginIDsTx(ctx, tx, workspaceID, kind)
		if err != nil {
			return nil, err
		}
		for _, id := range graphIDs {
			seen[id] = struct{}{}
		}
		outboxIDs, err := s.listMemoryProjectionOutboxOriginIDsTx(ctx, tx, workspaceID, kind)
		if err != nil {
			return nil, err
		}
		for _, id := range outboxIDs {
			seen[id] = struct{}{}
		}
		targets[kind] = normalizeMemoryProjectionOriginIDs(memoryProjectionMapKeys(seen))
	}
	return targets, nil
}

func memoryProjectionMapKeys(items map[string]struct{}) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for item := range items {
		out = append(out, item)
	}
	return out
}

func (s *Store) listMemoryProjectionSourceIDsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionKind string) ([]string, error) {
	var query string
	switch normalizeMemoryProjectionKind(projectionKind) {
	case memoryProjectionKindWorkspaceMemory:
		query = `SELECT memory_id FROM workspace_memory WHERE workspace_id = ? ORDER BY memory_id ASC`
	case memoryProjectionKindKnowledgeClaim:
		query = `SELECT claim_id FROM knowledge_claims WHERE workspace_id = ? ORDER BY claim_id ASC`
	case memoryProjectionKindEpisodePack:
		query = `SELECT pack_id FROM episode_packs WHERE workspace_id = ? ORDER BY pack_id ASC`
	default:
		return nil, fmt.Errorf("unsupported projection kind: %s", projectionKind)
	}
	rows, err := tx.QueryContext(ctx, query, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list projection source ids for %s: %w", projectionKind, err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var originID string
		if err := rows.Scan(&originID); err != nil {
			return nil, fmt.Errorf("scan projection source id for %s: %w", projectionKind, err)
		}
		originID = strings.TrimSpace(originID)
		if originID == "" {
			continue
		}
		out = append(out, originID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate projection source ids for %s: %w", projectionKind, err)
	}
	return out, nil
}

func (s *Store) listMemoryProjectionGraphOriginIDsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionKind string) ([]string, error) {
	originKind := memoryProjectionOriginKind(projectionKind)
	if originKind == "" {
		return nil, fmt.Errorf("unsupported projection kind: %s", projectionKind)
	}
	rows, err := tx.QueryContext(
		ctx,
		`SELECT DISTINCT origin_id
		   FROM memory_nodes
		  WHERE workspace_id = ?
		    AND origin_kind = ?
		    AND TRIM(origin_id) <> ''
		  ORDER BY origin_id ASC`,
		workspaceID,
		originKind,
	)
	if err != nil {
		return nil, fmt.Errorf("list projection graph origins for %s: %w", projectionKind, err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var originID string
		if err := rows.Scan(&originID); err != nil {
			return nil, fmt.Errorf("scan projection graph origin for %s: %w", projectionKind, err)
		}
		originID = strings.TrimSpace(originID)
		if originID == "" {
			continue
		}
		out = append(out, originID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate projection graph origins for %s: %w", projectionKind, err)
	}
	return out, nil
}

func (s *Store) listMemoryProjectionOutboxOriginIDsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionKind string) ([]string, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT DISTINCT origin_id
		   FROM memory_projection_outbox
		  WHERE workspace_id = ?
		    AND projection_kind = ?
		    AND TRIM(origin_id) <> ''
		  ORDER BY origin_id ASC`,
		workspaceID,
		normalizeMemoryProjectionKind(projectionKind),
	)
	if err != nil {
		return nil, fmt.Errorf("list projection outbox origins for %s: %w", projectionKind, err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var originID string
		if err := rows.Scan(&originID); err != nil {
			return nil, fmt.Errorf("scan projection outbox origin for %s: %w", projectionKind, err)
		}
		originID = strings.TrimSpace(originID)
		if originID == "" {
			continue
		}
		out = append(out, originID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate projection outbox origins for %s: %w", projectionKind, err)
	}
	return out, nil
}

func (s *Store) summarizeMemoryProjectionOutbox(ctx context.Context, workspaceID string, kinds []string) (pending int, processing int, failed int, done int, err error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return 0, 0, 0, 0, errors.New("workspace_id is required")
	}
	kinds = normalizeMemoryProjectionKinds(kinds)
	placeholders := make([]string, 0, len(kinds))
	args := make([]any, 0, 1+len(kinds))
	args = append(args, workspaceID)
	for _, kind := range kinds {
		placeholders = append(placeholders, "?")
		args = append(args, kind)
	}
	query := fmt.Sprintf(
		`SELECT status, COUNT(1)
		   FROM memory_projection_outbox
		  WHERE workspace_id = ?
		    AND projection_kind IN (%s)
		  GROUP BY status`,
		strings.Join(placeholders, ", "),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("summarize memory projection outbox: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("scan memory projection outbox summary: %w", err)
		}
		switch strings.TrimSpace(status) {
		case memoryProjectionStatusPending:
			pending = count
		case memoryProjectionStatusProcessing:
			processing = count
		case memoryProjectionStatusFailed:
			failed = count
		case memoryProjectionStatusDone:
			done = count
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("iterate memory projection outbox summary: %w", err)
	}
	return pending, processing, failed, done, nil
}

func (s *Store) ReconcileMemoryProjectionWorkspace(ctx context.Context, workspaceID string, limit int) (MemoryProjectionReconcileResult, error) {
	return s.reconcileMemoryProjectionWorkspaceKinds(ctx, workspaceID, nil, limit)
}

func (s *Store) reconcileMemoryProjectionWorkspaceKinds(ctx context.Context, workspaceID string, kinds []string, limit int) (MemoryProjectionReconcileResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryProjectionReconcileResult{}, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = memoryProjectionDefaultReconcileLimit
	}
	result := MemoryProjectionReconcileResult{WorkspaceID: workspaceID}
	if _, err := s.quarantineMalformedMemoryProjectionAvailableAt(ctx, workspaceID, kinds, limit); err != nil {
		return result, err
	}
	rows, err := s.listPendingMemoryProjectionOutboxKinds(ctx, workspaceID, kinds, limit)
	if err != nil {
		return result, err
	}
	var errs []string
	for _, row := range rows {
		if err := s.processMemoryProjectionOutboxRecord(ctx, row); err != nil {
			result.Failed++
			errs = append(errs, err.Error())
			continue
		}
		result.Processed++
	}
	if len(errs) > 0 {
		return result, errors.New(strings.Join(errs, "; "))
	}
	return result, nil
}

func (s *Store) processMemoryProjectionOutboxRecord(ctx context.Context, row MemoryProjectionOutboxRecord) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin memory projection reconcile tx: %w", err)
	}
	claimed, err := s.claimMemoryProjectionOutboxTx(ctx, tx, row, now)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if !claimed {
		_ = tx.Rollback()
		return nil
	}
	if err := s.rebuildMemoryProjectionTx(ctx, tx, row); err != nil {
		_ = tx.Rollback()
		s.markMemoryProjectionOutboxFailed(ctx, row, now, err)
		return fmt.Errorf("rebuild memory projection %s/%s: %w", row.ProjectionKind, row.OriginID, err)
	}
	if err := s.markMemoryProjectionOutboxDoneTx(ctx, tx, row, now); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit memory projection reconcile tx: %w", err)
	}
	return nil
}

func (s *Store) claimMemoryProjectionOutboxTx(ctx context.Context, tx *sql.Tx, row MemoryProjectionOutboxRecord, now string) (bool, error) {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE memory_projection_outbox
		    SET status = ?, attempt_count = attempt_count + 1, started_at = ?, updated_at = ?, last_error = ''
		  WHERE projection_id = ?
		    AND workspace_id = ?
		    AND status IN (?, ?)`,
		memoryProjectionStatusProcessing,
		now,
		now,
		row.ProjectionID,
		row.WorkspaceID,
		memoryProjectionStatusPending,
		memoryProjectionStatusFailed,
	)
	if err != nil {
		return false, fmt.Errorf("claim memory projection outbox: %w", err)
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func (s *Store) markMemoryProjectionOutboxDoneTx(ctx context.Context, tx *sql.Tx, row MemoryProjectionOutboxRecord, now string) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE memory_projection_outbox
		    SET status = ?, completed_at = ?, updated_at = ?, last_error = ''
		  WHERE projection_id = ? AND workspace_id = ?`,
		memoryProjectionStatusDone,
		now,
		now,
		row.ProjectionID,
		row.WorkspaceID,
	); err != nil {
		return fmt.Errorf("mark memory projection outbox done: %w", err)
	}
	return nil
}

func (s *Store) markMemoryProjectionOutboxFailed(ctx context.Context, row MemoryProjectionOutboxRecord, now string, cause error) {
	if s == nil {
		return
	}
	nextAvailableAt := memoryProjectionRetryAt(now, row.AttemptCount+1)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return
	}
	if _, execErr := tx.ExecContext(
		ctx,
		`UPDATE memory_projection_outbox
		    SET status = ?, attempt_count = attempt_count + 1, last_error = ?, available_at = ?, started_at = NULL, completed_at = NULL, updated_at = ?
		  WHERE projection_id = ? AND workspace_id = ?`,
		memoryProjectionStatusFailed,
		truncateMessage(strings.TrimSpace(cause.Error()), 512),
		nextAvailableAt,
		now,
		row.ProjectionID,
		row.WorkspaceID,
	); execErr != nil {
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()
}

func (s *Store) rebuildMemoryProjectionTx(ctx context.Context, tx *sql.Tx, row MemoryProjectionOutboxRecord) error {
	switch normalizeMemoryProjectionKind(row.ProjectionKind) {
	case memoryProjectionKindKnowledgeClaim:
		record, err := s.loadKnowledgeClaimRecordTx(ctx, tx, row.WorkspaceID, row.OriginID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return s.deleteMemoryProjectionCompatibilityNodeTx(ctx, tx, row.WorkspaceID, row.ProjectionKind, row.OriginID)
			}
			return err
		}
		return s.syncKnowledgeClaimGraphTx(ctx, tx, record)
	case memoryProjectionKindEpisodePack:
		record, err := s.loadEpisodePackTx(ctx, tx, row.WorkspaceID, row.OriginID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return s.deleteMemoryProjectionCompatibilityNodeTx(ctx, tx, row.WorkspaceID, row.ProjectionKind, row.OriginID)
			}
			return err
		}
		return s.syncEpisodePackGraphProjectionTx(ctx, tx, record)
	case memoryProjectionKindWorkspaceMemory:
		record, err := loadWorkspaceMemoryRecordTx(ctx, tx, row.WorkspaceID, row.OriginID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return s.deleteMemoryProjectionCompatibilityNodeTx(ctx, tx, row.WorkspaceID, row.ProjectionKind, row.OriginID)
			}
			return err
		}
		return s.syncWorkspaceMemoryGraphTx(ctx, tx, record)
	default:
		return fmt.Errorf("unsupported projection kind: %s", row.ProjectionKind)
	}
}

func (s *Store) deleteMemoryProjectionCompatibilityNodeTx(ctx context.Context, tx *sql.Tx, workspaceID, projectionKind, originID string) error {
	memoryID := strings.TrimSpace(memoryProjectionOriginMemoryID(projectionKind, originID))
	if memoryID == "" {
		return fmt.Errorf("unknown projection memory id for kind %s", projectionKind)
	}
	return deleteMemoryGraphNodeWithEdgesTx(ctx, tx, workspaceID, memoryID)
}

func deleteMemoryGraphNodeWithEdgesTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM memory_edges
		  WHERE workspace_id = ?
		    AND (from_memory_id = ? OR to_memory_id = ?)`,
		workspaceID,
		memoryID,
		memoryID,
	); err != nil {
		return fmt.Errorf("delete memory projection edges: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, memoryID); err != nil {
		return fmt.Errorf("delete memory projection node: %w", err)
	}
	return nil
}

func truncateMessage(message string, limit int) string {
	message = strings.TrimSpace(message)
	if limit <= 0 || len(message) <= limit {
		return message
	}
	return message[:limit]
}

func memoryProjectionRetryAt(now string, attempt int) string {
	baseTS, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(now))
	if err != nil {
		baseTS = time.Now().UTC()
	}
	delay := time.Duration(maxInt(attempt, 1)) * memoryProjectionRetryBaseDelay
	if delay > memoryProjectionRetryMaxDelay {
		delay = memoryProjectionRetryMaxDelay
	}
	return baseTS.Add(delay).UTC().Format(time.RFC3339Nano)
}

func memoryProjectionClampOperationalNow(raw string) string {
	actualNow := time.Now().UTC()
	requested := strings.TrimSpace(raw)
	if requested == "" {
		return actualNow.Format(time.RFC3339Nano)
	}
	requestedTS, err := time.Parse(time.RFC3339Nano, requested)
	if err != nil {
		return actualNow.Format(time.RFC3339Nano)
	}
	if requestedTS.After(actualNow) {
		return actualNow.Format(time.RFC3339Nano)
	}
	return requestedTS.UTC().Format(time.RFC3339Nano)
}

func memoryProjectionAvailableBy(raw string, reference time.Time) bool {
	availableAt, ok := memoryProjectionQueueTimestamp(raw)
	if !ok {
		return false
	}
	return !availableAt.After(reference)
}

func memoryProjectionTimeOrZero(raw string) time.Time {
	ts, ok := memoryProjectionParseTimestamp(raw)
	if !ok {
		return time.Time{}
	}
	return ts
}

func memoryProjectionParseTimestamp(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return ts.UTC(), true
}

func memoryProjectionQueueTimestamp(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasSuffix(raw, "Z") {
		return time.Time{}, false
	}
	return memoryProjectionParseTimestamp(raw)
}

func memoryProjectionProcessingReferenceTime(startedAt sql.NullString, updatedAt, enqueuedAt, availableAt string) (time.Time, bool, bool) {
	if startedAt.Valid && strings.TrimSpace(startedAt.String) != "" {
		if ts, ok := memoryProjectionParseTimestamp(startedAt.String); ok {
			return ts, true, false
		}
	} else if !startedAt.Valid {
		return memoryProjectionProcessingFallbackReferenceTime(updatedAt, enqueuedAt, availableAt, false)
	}
	return memoryProjectionProcessingFallbackReferenceTime(updatedAt, enqueuedAt, availableAt, true)
}

func memoryProjectionProcessingFallbackReferenceTime(updatedAt, enqueuedAt, availableAt string, malformedStartedAt bool) (time.Time, bool, bool) {
	for _, raw := range []string{updatedAt, enqueuedAt, availableAt} {
		if ts, ok := memoryProjectionParseTimestamp(raw); ok {
			return ts, true, malformedStartedAt
		}
	}
	return time.Time{}, false, true
}
