package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRPCLogsListDefaultsToRecentWindow(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	recentCreatedAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	oldCreatedAt := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339Nano)

	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rpc_access_log (method, workspace_id, actor, status, error_msg, latency_ms, created_at) VALUES (?,?,?,?,?,?,?)`,
		"agent.register", "rhizome-main", "agent-recent", "error", "recent failure", 0, recentCreatedAt,
	); err != nil {
		t.Fatalf("insert recent rpc log: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rpc_access_log (method, workspace_id, actor, status, error_msg, latency_ms, created_at) VALUES (?,?,?,?,?,?,?)`,
		"agent.heartbeat", "rhizome-main", "agent-old", "error", "old failure", 0, oldCreatedAt,
	); err != nil {
		t.Fatalf("insert old rpc log: %v", err)
	}

	result, rpcErr := h.rpcLogsList(ctx, json.RawMessage(`{}`))
	if rpcErr != nil {
		t.Fatalf("rpcLogsList() rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	entries, ok := payload["entries"].([]rpcLogEntry)
	if !ok {
		t.Fatalf("unexpected entries type %T", payload["entries"])
	}
	if len(entries) != 1 {
		t.Fatalf("expected only recent entry in default window, got %+v", entries)
	}
	if entries[0].Actor != "agent-recent" {
		t.Fatalf("expected recent entry to remain visible, got %+v", entries[0])
	}

	result, rpcErr = h.rpcLogsList(ctx, json.RawMessage(`{"since_hours":168}`))
	if rpcErr != nil {
		t.Fatalf("rpcLogsList(since_hours) rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected widened result type %T", result)
	}
	entries, ok = payload["entries"].([]rpcLogEntry)
	if !ok {
		t.Fatalf("unexpected widened entries type %T", payload["entries"])
	}
	if len(entries) != 2 {
		t.Fatalf("expected widened window to include historical entries, got %+v", entries)
	}
}
