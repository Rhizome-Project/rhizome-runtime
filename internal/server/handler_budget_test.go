package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestBudgetLedgerRPCReserveSpendRefundAndList(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	result := callBudgetRPC(t, ctx, h, "budget.account.ensure", map[string]any{
		"account_id":     "budget-rpc-account",
		"principal_type": "human",
		"principal_id":   "developer",
		"workspace_id":   "ws-budget-rpc",
		"limit_micros":   1000,
	})
	account := mapFromResult(t, result, "account")
	if got := int64FromJSON(t, account["available_micros"]); got != 1000 {
		t.Fatalf("expected initial available budget 1000, got %d from %+v", got, account)
	}

	result = callBudgetRPC(t, ctx, h, "budget.reserve", budgetRPCReservationParams("budget-rpc-account", "budget-rpc-reservation", "idem-budget-rpc-reserve", 600))
	account = mapFromResult(t, result, "account")
	if got := int64FromJSON(t, account["reserved_micros"]); got != 600 {
		t.Fatalf("expected reserved budget 600, got %d from %+v", got, account)
	}

	result = callBudgetRPC(t, ctx, h, "budget.spend", map[string]any{
		"entry_id":        "budget-rpc-spend",
		"idempotency_key": "idem-budget-rpc-spend",
		"account_id":      "budget-rpc-account",
		"reservation_id":  "budget-rpc-reservation",
		"workspace_id":    "ws-budget-rpc",
		"agent_id":        "agent-rpc",
		"task_id":         "task-rpc",
		"run_id":          "run-rpc",
		"provider_id":     "openai",
		"model":           "gpt-test",
		"amount_micros":   400,
	})
	account = mapFromResult(t, result, "account")
	if got := int64FromJSON(t, account["spent_micros"]); got != 400 {
		t.Fatalf("expected spent budget 400, got %d from %+v", got, account)
	}

	result = callBudgetRPC(t, ctx, h, "budget.refund", map[string]any{
		"entry_id":        "budget-rpc-refund",
		"idempotency_key": "idem-budget-rpc-refund",
		"account_id":      "budget-rpc-account",
		"workspace_id":    "ws-budget-rpc",
		"agent_id":        "agent-rpc",
		"task_id":         "task-rpc",
		"run_id":          "run-rpc",
		"provider_id":     "openai",
		"model":           "gpt-test",
		"source_entry_id": "budget-rpc-spend",
		"amount_micros":   100,
	})
	account = mapFromResult(t, result, "account")
	if got := int64FromJSON(t, account["available_micros"]); got != 500 {
		t.Fatalf("expected available budget 500 after refund, got %d from %+v", got, account)
	}

	result = callBudgetRPC(t, ctx, h, "budget.release", map[string]any{
		"entry_id":        "budget-rpc-release",
		"idempotency_key": "idem-budget-rpc-release",
		"account_id":      "budget-rpc-account",
		"reservation_id":  "budget-rpc-reservation",
		"workspace_id":    "ws-budget-rpc",
		"agent_id":        "agent-rpc",
		"task_id":         "task-rpc",
		"run_id":          "run-rpc",
		"provider_id":     "openai",
		"model":           "gpt-test",
		"amount_micros":   200,
	})
	account = mapFromResult(t, result, "account")
	if got := int64FromJSON(t, account["reserved_micros"]); got != 0 {
		t.Fatalf("expected reserved budget 0 after release, got %d from %+v", got, account)
	}

	result = callBudgetRPC(t, ctx, h, "budget.ledger.list", map[string]any{"account_id": "budget-rpc-account", "workspace_id": "ws-budget-rpc"})
	if got := int64FromJSON(t, result["count"]); got != 4 {
		t.Fatalf("expected 4 budget ledger entries, got %d from %+v", got, result)
	}

	result = callBudgetRPC(t, ctx, h, "budget.reservations.list", map[string]any{
		"account_id":   "budget-rpc-account",
		"workspace_id": "ws-budget-rpc",
		"agent_id":     "agent-rpc",
		"status":       "CLOSED",
	})
	reservations := sliceFromResult(t, result, "reservations")
	if len(reservations) != 1 {
		t.Fatalf("expected one closed budget reservation, got %+v", result)
	}
	closedReservation := reservations[0]
	if got := closedReservation["reservation_id"]; got != "budget-rpc-reservation" {
		t.Fatalf("unexpected closed reservation record: %+v", closedReservation)
	}
	if got := int64FromJSON(t, closedReservation["remaining_micros"]); got != 0 {
		t.Fatalf("expected closed reservation remaining 0, got %d from %+v", got, closedReservation)
	}

	result = callBudgetRPC(t, ctx, h, "budget.reservations.list", map[string]any{
		"workspace_id": "ws-budget-rpc",
		"agent_id":     "agent-rpc",
		"status":       "OPEN",
	})
	if got := int64FromJSON(t, result["count"]); got != 0 {
		t.Fatalf("expected zero open budget reservations after release, got %d from %+v", got, result)
	}

	result = callBudgetRPC(t, ctx, h, "budget.health", map[string]any{"stale_after_sec": 3600})
	health := mapFromResult(t, result, "budget_ledger")
	if got := health["status"]; got != "ok" {
		t.Fatalf("expected budget health ok, got %+v", health)
	}
}

func TestBudgetLedgerRPCRejectsMismatchedIdempotencyReplay(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	callBudgetRPC(t, ctx, h, "budget.account.ensure", map[string]any{
		"account_id":     "budget-rpc-idem",
		"principal_type": "human",
		"principal_id":   "developer",
		"workspace_id":   "ws-budget-rpc",
		"limit_micros":   1000,
	})
	callBudgetRPC(t, ctx, h, "budget.reserve", budgetRPCReservationParams("budget-rpc-idem", "budget-rpc-idem-reservation", "idem-budget-rpc-reserve", 600))

	params := budgetRPCReservationParams("budget-rpc-idem", "budget-rpc-idem-reservation", "idem-budget-rpc-reserve", 700)
	authCtx := testAuthContext("ws-budget-rpc", "human", "developer")
	_, rpcErr := h.dispatch(authCtx, "budget.reserve", mustRawJSON(t, params))
	if rpcErr == nil {
		t.Fatal("expected mismatched budget idempotency replay to fail")
	}

	snapshot, err := store.GetBudgetAccount(ctx, "budget-rpc-idem")
	if err != nil {
		t.Fatalf("get budget account: %v", err)
	}
	if snapshot.ReservedMicros != 600 || snapshot.AvailableMicros != 400 {
		t.Fatalf("mismatched replay changed budget snapshot: %+v", snapshot)
	}
}

func TestBudgetLedgerRPCEnsureReusesExistingPrincipalIdentity(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	callBudgetRPC(t, ctx, h, "budget.account.ensure", map[string]any{
		"account_id":     "pilot-agent-alpha",
		"principal_type": "agent",
		"principal_id":   "alpha",
		"workspace_id":   "ws-budget-rpc-identity",
		"limit_micros":   1000,
	})
	result := callBudgetRPC(t, ctx, h, "budget.account.ensure", map[string]any{
		"account_id":     "llm-budget-alpha",
		"principal_type": "agent",
		"principal_id":   "alpha",
		"workspace_id":   "ws-budget-rpc-identity",
		"limit_micros":   2000,
	})
	account := mapFromResult(t, result, "account")
	if got := account["account_id"]; got != "pilot-agent-alpha" {
		t.Fatalf("expected canonical existing account id, got %+v", account)
	}
	if got := int64FromJSON(t, account["limit_micros"]); got != 2000 {
		t.Fatalf("expected updated limit 2000, got %d from %+v", got, account)
	}
}

func TestBudgetLedgerRPCRejectsUnsupportedAccountStatus(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	authCtx := testAuthContext("ws-budget-rpc", "human", "developer")
	_, rpcErr := h.dispatch(authCtx, "budget.account.ensure", mustRawJSON(t, map[string]any{
		"account_id":     "budget-rpc-paused",
		"principal_type": "human",
		"principal_id":   "developer",
		"workspace_id":   "ws-budget-rpc",
		"limit_micros":   1000,
		"status":         "PAUSED",
	}))
	if rpcErr == nil {
		t.Fatal("expected unsupported budget account status to fail")
	}
}

func TestBudgetLedgerRPCAgentBudgetAuthorityIsAccountBounded(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	agentCtx := testAuthContext("ws-budget-agent-auth", "agent", "agent-alpha")
	managedAccountID := serverManagedAgentBudgetAccountID("agent-alpha")

	result, rpcErr := h.dispatch(agentCtx, "budget.account.ensure", mustRawJSON(t, map[string]any{
		"account_id":     managedAccountID,
		"principal_type": "agent",
		"principal_id":   "agent-alpha",
		"workspace_id":   "ws-budget-agent-auth",
		"limit_micros":   1000,
	}))
	if rpcErr != nil {
		t.Fatalf("agent managed budget account create failed: %+v", rpcErr)
	}
	raw, _ := json.Marshal(result)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode managed account result: %v", err)
	}
	if got := mapFromResult(t, out, "account")["account_id"]; got != managedAccountID {
		t.Fatalf("unexpected managed account result: %+v", out)
	}
	if _, rpcErr := h.dispatch(agentCtx, "budget.account.ensure", mustRawJSON(t, map[string]any{
		"account_id":     "budget-arbitrary-alpha",
		"principal_type": "agent",
		"principal_id":   "agent-alpha",
		"workspace_id":   "ws-budget-agent-auth",
		"limit_micros":   1000,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected arbitrary agent budget account create denial, got %+v", rpcErr)
	}
	if _, rpcErr := h.dispatch(agentCtx, "budget.account.ensure", mustRawJSON(t, map[string]any{
		"account_id":     managedAccountID,
		"principal_type": "agent",
		"principal_id":   "agent-alpha",
		"workspace_id":   "ws-budget-agent-auth",
		"limit_micros":   1500,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected agent budget limit raise denial, got %+v", rpcErr)
	}

	callBudgetRPC(t, ctx, h, "budget.account.ensure", map[string]any{
		"account_id":     "budget-beta",
		"principal_type": "agent",
		"principal_id":   "agent-beta",
		"workspace_id":   "ws-budget-agent-auth",
		"limit_micros":   1000,
	})
	if _, rpcErr := h.dispatch(agentCtx, "budget.reserve", mustRawJSON(t, map[string]any{
		"reservation_id":  "reservation-alpha-on-beta",
		"idempotency_key": "reservation-alpha-on-beta",
		"account_id":      "budget-beta",
		"workspace_id":    "ws-budget-agent-auth",
		"agent_id":        "agent-alpha",
		"task_id":         "task-alpha",
		"run_id":          "run-alpha",
		"provider_id":     "openai",
		"model":           "gpt-test",
		"amount_micros":   1,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected agent cross-account reserve denial, got %+v", rpcErr)
	}
}

func callBudgetRPC(t *testing.T, ctx context.Context, h *Handler, method string, params map[string]any) map[string]any {
	t.Helper()
	if _, ok := authPrincipalFromContext(ctx); !ok {
		if workspaceID, _ := params["workspace_id"].(string); strings.TrimSpace(workspaceID) != "" {
			ctx = testAuthContext(strings.TrimSpace(workspaceID), "human", "developer")
		}
	}
	result, rpcErr := h.dispatch(ctx, method, mustRawJSON(t, params))
	if rpcErr != nil {
		t.Fatalf("%s returned RPC error: %+v", method, rpcErr)
	}
	var out map[string]any
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal %s result: %v", method, err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal %s result: %v", method, err)
	}
	return out
}

func sliceFromResult(t *testing.T, result map[string]any, key string) []map[string]any {
	t.Helper()
	rawItems, ok := result[key].([]any)
	if !ok {
		t.Fatalf("result key %q is not an array: %+v", key, result)
	}
	items := make([]map[string]any, 0, len(rawItems))
	for _, rawItem := range rawItems {
		item, ok := rawItem.(map[string]any)
		if !ok {
			t.Fatalf("result key %q contains non-object item: %+v", key, rawItem)
		}
		items = append(items, item)
	}
	return items
}

func budgetRPCReservationParams(accountID, reservationID, idempotencyKey string, amountMicros int64) map[string]any {
	return map[string]any{
		"reservation_id":  reservationID,
		"idempotency_key": idempotencyKey,
		"account_id":      accountID,
		"workspace_id":    "ws-budget-rpc",
		"agent_id":        "agent-rpc",
		"task_id":         "task-rpc",
		"run_id":          "run-rpc",
		"provider_id":     "openai",
		"model":           "gpt-test",
		"amount_micros":   amountMicros,
	}
}

func mapFromResult(t *testing.T, result map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := result[key].(map[string]any)
	if !ok {
		t.Fatalf("result key %q is not an object: %+v", key, result)
	}
	return value
}

func int64FromJSON(t *testing.T, value any) int64 {
	t.Helper()
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		t.Fatalf("value is not numeric: %T %v", value, value)
	}
	return 0
}

func mustRawJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal raw json: %v", err)
	}
	return raw
}
