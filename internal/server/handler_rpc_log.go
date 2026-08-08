package server

import (
	"context"
	"encoding/json"
	"time"
)

type rpcLogEntry struct {
	ID          int64  `json:"id"`
	Method      string `json:"method"`
	WorkspaceID string `json:"workspace_id"`
	Actor       string `json:"actor"`
	Status      string `json:"status"`
	ErrorMsg    string `json:"error_msg,omitempty"`
	LatencyMs   int64  `json:"latency_ms"`
	CreatedAt   string `json:"created_at"`
}

type rpcLogsListParams struct {
	Limit      int    `json:"limit"`
	Method     string `json:"method"`
	Status     string `json:"status"`
	BeforeID   int64  `json:"before_id"`
	SinceHours int    `json:"since_hours"`
}

func (h *Handler) rpcLogsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p rpcLogsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	if p.Limit <= 0 || p.Limit > 500 {
		p.Limit = 50
	}
	if p.SinceHours <= 0 {
		p.SinceHours = 24
	}

	query := `SELECT id, method, workspace_id, actor, status, error_msg, latency_ms, created_at
	          FROM rpc_access_log`
	args := []any{}
	where := []string{}

	where = append(where, "created_at > ?")
	args = append(args, time.Now().UTC().Add(-time.Duration(p.SinceHours)*time.Hour).Format(time.RFC3339Nano))

	if p.BeforeID > 0 {
		where = append(where, "id < ?")
		args = append(args, p.BeforeID)
	}
	if p.Method != "" {
		where = append(where, "method = ?")
		args = append(args, p.Method)
	}
	if p.Status != "" {
		where = append(where, "status = ?")
		args = append(args, p.Status)
	}

	if len(where) > 0 {
		for i, w := range where {
			if i == 0 {
				query += " WHERE " + w
			} else {
				query += " AND " + w
			}
		}
	}

	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, p.Limit)

	rows, err := h.store.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, &RPCError{Code: -32000, Message: err.Error()}
	}
	defer rows.Close()

	var entries []rpcLogEntry
	for rows.Next() {
		var e rpcLogEntry
		if err := rows.Scan(&e.ID, &e.Method, &e.WorkspaceID, &e.Actor, &e.Status, &e.ErrorMsg, &e.LatencyMs, &e.CreatedAt); err != nil {
			return nil, &RPCError{Code: -32000, Message: err.Error()}
		}
		entries = append(entries, e)
	}

	// Compute stats
	var totalCalls, errorCount, avgLatency int64
	row := h.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(CASE WHEN status='error' THEN 1 ELSE 0 END),0), COALESCE(AVG(latency_ms),0)
		 FROM rpc_access_log WHERE created_at > ?`,
		time.Now().UTC().Add(-24*time.Hour).Format(time.RFC3339Nano))
	_ = row.Scan(&totalCalls, &errorCount, &avgLatency)

	// Check if there are more entries
	hasMore := len(entries) == p.Limit

	return map[string]any{
		"entries":  entries,
		"count":    len(entries),
		"has_more": hasMore,
		"stats": map[string]any{
			"total_24h":      totalCalls,
			"errors_24h":     errorCount,
			"avg_latency_ms": avgLatency,
		},
	}, nil
}
