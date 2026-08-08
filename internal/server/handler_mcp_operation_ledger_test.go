package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMCPServerRegisterRecordsDurableOperationLedgerLifecycle(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-mcp-register-ledger")
	serverID := uniqueOperationLedgerTestID("mcp-register-ledger")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP register ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(mcpServerRegisterParams{
		ServerID:     serverID,
		WorkspaceID:  workspaceID,
		DisplayName:  "Register ledger server",
		Transport:    "streamable-http",
		URL:          "https://example.invalid/mcp",
		EnvJSON:      `{"TOKEN":"super-secret-register-token"}`,
		HeadersJSON:  `{"Authorization":"Bearer super-secret-header-token"}`,
		RegisteredBy: "human:developer",
	})
	if err != nil {
		t.Fatalf("marshal register params: %v", err)
	}
	result, rpcErr := h.mcpServerRegister(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("mcp register failed: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected register result type %T", result)
	}
	operationID, ok := resultMap["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("missing operation_id in register result %+v", resultMap)
	}
	if got := strings.TrimSpace(resultMap["status"].(string)); got != "REGISTERED" {
		t.Fatalf("register status = %q", got)
	}

	run := latestExecutionRunWithPrefix(t, ctx, store, workspaceID, "mcpregister_")
	if run.RunID != operationID {
		t.Fatalf("run id = %q, want response operation_id %q", run.RunID, operationID)
	}
	if run.Status != "COMPLETED" || run.Outcome != "COMPLETED" {
		t.Fatalf("unexpected register run terminal state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	assertOperationLedgerPromptContext(t, run, "mcp.server.register", workspaceID, "human", "developer")
	assertMCPServerLifecycleEventCount(t, ctx, store, workspaceID, operationID, "mcp.server.register")

	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "mcp_server_register" {
		t.Fatalf("operation_kind = %q", got)
	}
	if got := stringLedgerField(t, ledger, "status"); got != "completed" {
		t.Fatalf("ledger status = %q", got)
	}
	capability := nestedLedgerMap(t, ledger, "capability_snapshot")
	if got := stringLedgerField(t, capability, "requested_capability"); got != "mcp.server.register" {
		t.Fatalf("requested_capability = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	if got := stringLedgerField(t, requestLedger, "redaction_policy"); got != "mcp-lifecycle-secrets-redacted-v1" {
		t.Fatalf("redaction_policy = %q", got)
	}
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("mcp_transport = %q", got)
	}
	if envPresent, ok := details["env_present"].(bool); !ok || !envPresent {
		t.Fatalf("expected env_present=true, got %+v", details["env_present"])
	}
	if headersPresent, ok := details["headers_present"].(bool); !ok || !headersPresent {
		t.Fatalf("expected headers_present=true, got %+v", details["headers_present"])
	}
	encodedLedger, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	if strings.Contains(string(encodedLedger), "super-secret-register-token") ||
		strings.Contains(string(encodedLedger), "super-secret-header-token") {
		t.Fatalf("register ledger leaked raw MCP secrets: %s", string(encodedLedger))
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != operationID {
		t.Fatalf("ledger result operation_id = %q, want %q", got, operationID)
	}
	if got := stringLedgerField(t, resultLedger, "status"); got != "REGISTERED" {
		t.Fatalf("ledger result status = %q", got)
	}
}

func TestMCPServerRemoveRecordsDurableOperationLedgerLifecycle(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := uniqueOperationLedgerTestID("ws-mcp-remove-ledger")
	serverID := uniqueOperationLedgerTestID("mcp-remove-ledger")
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP remove ledger",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	registerRaw, err := json.Marshal(mcpServerRegisterParams{
		ServerID:     serverID,
		WorkspaceID:  workspaceID,
		DisplayName:  "Remove ledger server",
		Transport:    "streamable-http",
		URL:          "https://example.invalid/remove-mcp",
		RegisteredBy: "human:developer",
	})
	if err != nil {
		t.Fatalf("marshal register params: %v", err)
	}
	if _, rpcErr := h.mcpServerRegister(ctx, registerRaw); rpcErr != nil {
		t.Fatalf("seed mcp register failed: %+v", rpcErr)
	}

	removeRaw, err := json.Marshal(mcpServerRemoveParams{
		WorkspaceID: workspaceID,
		ServerID:    serverID,
	})
	if err != nil {
		t.Fatalf("marshal remove params: %v", err)
	}
	result, rpcErr := h.mcpServerRemove(ctx, removeRaw)
	if rpcErr != nil {
		t.Fatalf("mcp remove failed: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected remove result type %T", result)
	}
	operationID, ok := resultMap["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("missing operation_id in remove result %+v", resultMap)
	}
	if got := strings.TrimSpace(resultMap["status"].(string)); got != "REMOVED" {
		t.Fatalf("remove status = %q", got)
	}

	run := latestExecutionRunWithPrefix(t, ctx, store, workspaceID, "mcpremove_")
	if run.RunID != operationID {
		t.Fatalf("run id = %q, want response operation_id %q", run.RunID, operationID)
	}
	if run.Status != "COMPLETED" || run.Outcome != "COMPLETED" {
		t.Fatalf("unexpected remove run terminal state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	assertOperationLedgerPromptContext(t, run, "mcp.server.remove", workspaceID, "human", "developer")
	assertMCPServerLifecycleEventCount(t, ctx, store, workspaceID, operationID, "mcp.server.remove")

	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "mcp_server_remove" {
		t.Fatalf("operation_kind = %q", got)
	}
	capability := nestedLedgerMap(t, ledger, "capability_snapshot")
	if got := stringLedgerField(t, capability, "requested_capability"); got != "mcp.server.remove" {
		t.Fatalf("requested_capability = %q", got)
	}
	requestLedger := nestedLedgerMap(t, ledger, "request")
	details := nestedLedgerMap(t, requestLedger, "details")
	if got := stringLedgerField(t, details, "server_status_before"); got != "ACTIVE" {
		t.Fatalf("server_status_before = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != operationID {
		t.Fatalf("ledger result operation_id = %q, want %q", got, operationID)
	}
	if got := stringLedgerField(t, resultLedger, "status"); got != "REMOVED" {
		t.Fatalf("ledger result status = %q", got)
	}
	if reset, ok := resultLedger["discover_state_reset"].(bool); !ok || !reset {
		t.Fatalf("expected discover_state_reset=true, got %+v", resultLedger["discover_state_reset"])
	}
}

func assertMCPServerLifecycleEventCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, operationID, wantSurface string) {
	t.Helper()
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    operationID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list operation runtime events: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected start and terminal execution_run.written events, got %d", len(events))
	}
	statuses := map[string]bool{}
	for _, event := range events {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode operation runtime event payload: %v", err)
		}
		if status, _ := payload["status"].(string); strings.TrimSpace(status) != "" {
			statuses[strings.TrimSpace(status)] = true
		}
		verification, ok := payload["verification"].(map[string]any)
		if !ok {
			t.Fatalf("expected execution_run.written verification payload, got %+v", payload)
		}
		envelope, ok := verification["prompt_context_envelope"].(map[string]any)
		if !ok {
			t.Fatalf("expected runtime event prompt_context_envelope, got %+v", verification)
		}
		if got := envelope["surface"]; got != wantSurface {
			t.Fatalf("runtime event prompt_context_envelope surface = %v, want %s in %+v", got, wantSurface, envelope)
		}
		if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
			t.Fatalf("runtime event prompt_context_envelope must not claim daemon convergence: %+v", envelope)
		}
	}
	if !statuses["ACTIVE"] || !statuses["COMPLETED"] {
		t.Fatalf("expected ACTIVE and COMPLETED runtime events, got statuses=%+v", statuses)
	}
}
