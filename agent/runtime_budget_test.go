package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type budgetTestLLM struct {
	calls    int
	response *LLMResponse
	err      error
	onChat   func()
	waitCtx  bool
}

func (l *budgetTestLLM) Chat(ctx context.Context, _ []Message, _ []ToolDef) (*LLMResponse, error) {
	l.calls++
	if l.onChat != nil {
		l.onChat()
	}
	if l.waitCtx {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if l.err != nil {
		return l.response, l.err
	}
	if l.response != nil {
		return l.response, nil
	}
	return &LLMResponse{Content: "ok"}, nil
}

func TestRuntimeBudgetedLLMBlocksWhenLocalTokenBudgetExhausted(t *testing.T) {
	llm := &budgetTestLLM{}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-budget-local",
		AgentID:     "agent-budget-local",
	}, llm)

	runtime.tokenMu.Lock()
	runtime.limitsGroupID = "limit-group-1"
	runtime.limitsLoaded = true
	runtime.dailyRemaining = 0
	runtime.weeklyRemaining = 100
	runtime.tokenMu.Unlock()

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, errRuntimeBudgetExhausted) {
		t.Fatalf("expected runtime budget exhaustion, got %v", err)
	}
	if llm.calls != 0 {
		t.Fatalf("expected provider not to be called after exhausted local budget, got %d calls", llm.calls)
	}
}

func TestRuntimeBudgetedLLMBlocksWhenSoakStopFileExists(t *testing.T) {
	llm := &budgetTestLLM{}
	workdir := t.TempDir()
	stopFile := filepath.Join(workdir, "STOP_REAL_LLM_SOAK")
	if err := os.WriteFile(stopFile, []byte("stop"), 0o600); err != nil {
		t.Fatalf("WriteFile(stopFile) error = %v", err)
	}
	runtime := NewRuntime(RuntimeConfig{
		Mode:                 RuntimeModeDaemon,
		Workdir:              workdir,
		WorkspaceID:          "ws-soak-stop",
		AgentID:              "agent-soak-stop",
		RealLLMPilot:         true,
		SoakStopFile:         stopFile,
		SoakRuntimeLimit:     time.Hour,
		SoakMaxProviderCalls: 1,
	}, llm)

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, errRuntimeSoakGuard) || !strings.Contains(err.Error(), "stop-file exists") {
		t.Fatalf("expected stop-file soak guard error, got %v", err)
	}
	if llm.calls != 0 {
		t.Fatalf("expected provider not to be called after stop-file, got %d calls", llm.calls)
	}
}

func TestRuntimeBudgetedLLMBlocksWhenSoakProviderCallLimitReached(t *testing.T) {
	llm := &budgetTestLLM{}
	runtime := NewRuntime(RuntimeConfig{
		Mode:                 RuntimeModeDaemon,
		Workdir:              t.TempDir(),
		WorkspaceID:          "ws-soak-limit",
		AgentID:              "agent-soak-limit",
		RealLLMPilot:         true,
		SoakStopFile:         filepath.Join(t.TempDir(), "STOP_REAL_LLM_SOAK"),
		SoakRuntimeLimit:     time.Hour,
		SoakMaxProviderCalls: 1,
	}, llm)

	if _, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "one"}}, nil); err != nil {
		t.Fatalf("first Chat() error = %v", err)
	}
	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "two"}}, nil)
	if !errors.Is(err, errRuntimeSoakGuard) || !strings.Contains(err.Error(), "provider-call limit reached") {
		t.Fatalf("expected provider-call limit soak guard error, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected only first provider call to pass, got %d calls", llm.calls)
	}
}

func TestRuntimeBudgetedLLMDoesNotReserveWhenSoakProviderCallLimitReached(t *testing.T) {
	llm := &budgetTestLLM{}
	reserveCalls := 0
	methods := []string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "budget.account.ensure":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-soak-limit", "status": "ACTIVE"}})
		case "budget.reserve":
			reserveCalls++
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-soak-limit", "status": "ACTIVE"}})
		case "budget.spend":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-soak-limit", "status": "ACTIVE"}})
		case "budget.release":
			t.Fatalf("unexpected budget.release after local soak provider-call limit")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-soak-budget-limit",
		AgentID:               "agent-soak-budget-limit",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		RealLLMPilot:          true,
		SoakStopFile:          filepath.Join(t.TempDir(), "STOP_REAL_LLM_SOAK"),
		SoakRuntimeLimit:      time.Hour,
		SoakMaxProviderCalls:  1,
		BudgetAccountID:       "acct-soak-limit",
		BudgetHardLimitMicros: 10,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-soak-budget-limit"
	runtime.scratch.ActiveSessionID = "session-soak-budget-limit"
	runtime.scratch.ActiveRunID = "run-soak-budget-limit"

	if _, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "one"}}, nil); err != nil {
		t.Fatalf("first Chat() error = %v", err)
	}
	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "two"}}, nil)
	if !errors.Is(err, errRuntimeSoakGuard) || !strings.Contains(err.Error(), "provider-call limit reached") {
		t.Fatalf("expected provider-call limit soak guard error, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected only first provider call to pass, got %d calls", llm.calls)
	}
	if reserveCalls != 1 {
		t.Fatalf("expected blocked second call to create no extra budget reservation, got %d reserve calls; methods=%v", reserveCalls, methods)
	}
	if want := []string{"budget.account.ensure", "budget.reserve", "budget.spend"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("unexpected methods: got %#v want %#v", methods, want)
	}
}

func TestRuntimeBudgetedLLMBlocksProviderWhenLedgerReserveFails(t *testing.T) {
	llm := &budgetTestLLM{}
	methods := []string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "budget.account.ensure":
			writeRPCResult(w, req, map[string]any{
				"account": map[string]any{
					"account_id":       rpcString(req.Params, "account_id"),
					"available_micros": 0,
					"status":           "ACTIVE",
				},
			})
		case "budget.reserve":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32602,
					"message": "budget exceeded",
				},
			})
		case "budget.spend", "budget.release":
			t.Fatalf("unexpected %s after failed reserve", req.Method)
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-denied",
		AgentID:               "agent-budget-denied",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-denied",
		BudgetHardLimitMicros: 10,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-denied"
	runtime.scratch.ActiveSessionID = "session-budget-denied"
	runtime.scratch.ActiveRunID = "run-budget-denied"

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, errRuntimeBudgetExhausted) || !strings.Contains(err.Error(), "reserve llm provider budget") {
		t.Fatalf("expected reserve budget exhaustion, got %v", err)
	}
	if llm.calls != 0 {
		t.Fatalf("expected provider not to be called after failed reserve, got %d calls", llm.calls)
	}
	if want := []string{"budget.account.ensure", "budget.reserve"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("unexpected methods: got %#v want %#v", methods, want)
	}
}

func TestRuntimeBudgetedLLMSpendsAndReleasesLedgerReservation(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage: TokenUsage{
			PromptTokens:     1,
			CompletionTokens: 2,
			TotalTokens:      3,
		},
	}}
	methods := []string{}
	spendMicros := int64(0)
	releaseMicros := int64(0)
	ensurePrincipalType := ""
	ensurePrincipalID := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "budget.account.ensure":
			if got := rpcString(req.Params, "account_id"); got != "acct-budget-happy" {
				t.Fatalf("unexpected account id %q", got)
			}
			ensurePrincipalType = rpcString(req.Params, "principal_type")
			ensurePrincipalID = rpcString(req.Params, "principal_id")
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-happy", "status": "ACTIVE"}})
		case "budget.reserve":
			if got := int64(req.Params["amount_micros"].(float64)); got != 10 {
				t.Fatalf("reserve amount = %d, want 10", got)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-budget-happy" {
				t.Fatalf("reserve task id = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-happy", "status": "ACTIVE"}})
		case "budget.spend":
			spendMicros = int64(req.Params["amount_micros"].(float64))
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-happy", "status": "ACTIVE"}})
		case "budget.release":
			releaseMicros = int64(req.Params["amount_micros"].(float64))
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-happy", "status": "ACTIVE"}})
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-happy",
		AgentID:               "agent-budget-happy",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		GroupID:               "group-budget",
		BudgetAccountID:       "acct-budget-happy",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  2,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-happy"
	runtime.scratch.ActiveSessionID = "session-budget-happy"
	runtime.scratch.ActiveRunID = "run-budget-happy"

	resp, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if resp.Content != "ok" || llm.calls != 1 {
		t.Fatalf("unexpected provider result=%+v calls=%d", resp, llm.calls)
	}
	if spendMicros != 6 {
		t.Fatalf("spend amount = %d, want 6", spendMicros)
	}
	if releaseMicros != 4 {
		t.Fatalf("release amount = %d, want 4", releaseMicros)
	}
	if ensurePrincipalType != "agent" || ensurePrincipalID != "agent-budget-happy" {
		t.Fatalf("budget account principal = %s/%s, want agent/agent-budget-happy", ensurePrincipalType, ensurePrincipalID)
	}
	if want := []string{"budget.account.ensure", "budget.reserve", "budget.spend", "budget.release"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("unexpected methods: got %#v want %#v", methods, want)
	}
}

func TestRuntimeBudgetedLLMUsesCanonicalAccountReturnedByLedger(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage:   TokenUsage{TotalTokens: 10},
	}}
	methods := []string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "budget.account.ensure":
			if got := rpcString(req.Params, "account_id"); got != "llm-budget-alpha" {
				t.Fatalf("ensure account id = %q, want llm-budget-alpha", got)
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "pilot-agent-alpha", "status": "ACTIVE"}})
		case "budget.reserve":
			if got := rpcString(req.Params, "account_id"); got != "pilot-agent-alpha" {
				t.Fatalf("reserve account id = %q, want canonical pilot-agent-alpha", got)
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "pilot-agent-alpha", "status": "ACTIVE"}})
		case "budget.spend":
			if got := rpcString(req.Params, "account_id"); got != "pilot-agent-alpha" {
				t.Fatalf("spend account id = %q, want canonical pilot-agent-alpha", got)
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "pilot-agent-alpha", "status": "ACTIVE"}})
		case "budget.release":
			t.Fatalf("unexpected release when spend consumes full reservation")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-canonical",
		AgentID:               "alpha",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "llm-budget-alpha",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-canonical"
	runtime.scratch.ActiveSessionID = "session-budget-canonical"
	runtime.scratch.ActiveRunID = "run-budget-canonical"

	resp, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if resp.Content != "ok" || llm.calls != 1 {
		t.Fatalf("unexpected provider result=%+v calls=%d", resp, llm.calls)
	}
	if runtime.cfg.BudgetAccountID != "pilot-agent-alpha" {
		t.Fatalf("runtime budget account id = %q, want canonical pilot-agent-alpha", runtime.cfg.BudgetAccountID)
	}
	if want := []string{"budget.account.ensure", "budget.reserve", "budget.spend"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("unexpected methods: got %#v want %#v", methods, want)
	}
}

func TestRuntimeBudgetedLLMKeepsReservationUnresolvedWhenSpendCaptureFailsAfterProviderSuccess(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage:   TokenUsage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
	}}
	methods := []string{}
	var persistedScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-spend-fail", "status": "ACTIVE"}})
		case "budget.spend":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32000,
					"message": "spend capture unavailable",
				},
			})
		case "budget.release":
			t.Fatalf("unexpected budget.release after provider success and spend capture failure")
		case "agent.state.set":
			if got := rpcString(req.Params, "key"); got != runtimeScratchStateKey {
				t.Fatalf("unexpected state key %q", got)
			}
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &persistedScratch); err != nil {
				t.Fatalf("decode persisted scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-spend-fail",
		AgentID:               "agent-budget-spend-fail",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-spend-fail",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  2,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-spend-fail"
	runtime.scratch.ActiveSessionID = "session-budget-spend-fail"
	runtime.scratch.ActiveRunID = "run-budget-spend-fail"
	runtime.tokenMu.Lock()
	runtime.limitsGroupID = "limit-group-spend-fail"
	runtime.limitsLoaded = true
	runtime.dailyRemaining = 100
	runtime.weeklyRemaining = 100
	runtime.tokenMu.Unlock()

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, errRuntimeBudgetExhausted) || !strings.Contains(err.Error(), runtimeBudgetCaptureFailedAfterProviderSuccessState) {
		t.Fatalf("expected spend capture budget error, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected provider to be called once before spend failure, got %d", llm.calls)
	}
	runtime.tokenMu.Lock()
	daily := runtime.dailyRemaining
	weekly := runtime.weeklyRemaining
	runtime.tokenMu.Unlock()
	if daily != 97 || weekly != 97 {
		t.Fatalf("expected local token budget charged before spend failure return, got daily=%d weekly=%d", daily, weekly)
	}
	if persistedScratch.LastWakeReason != runtimeBudgetCaptureFailedAfterProviderSuccessState {
		t.Fatalf("expected persisted capture-failure state, got %+v", persistedScratch)
	}
	if !strings.Contains(persistedScratch.LastSummary, runtimeBudgetCaptureFailedAfterProviderSuccessState) ||
		!strings.Contains(persistedScratch.LastSummary, "spend capture unavailable") {
		t.Fatalf("expected persisted capture-failure summary, got %+v", persistedScratch)
	}
	if want := []string{"budget.account.ensure", "budget.reserve", "budget.spend", "agent.state.set"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("unexpected methods: got %#v want %#v", methods, want)
	}
}

func TestRuntimeBudgetedLLMRetriesTransientSpendCaptureAfterProviderSuccess(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{Content: "ok"}}
	methods := []string{}
	spendCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-spend-retry", "status": "ACTIVE"}})
		case "budget.spend":
			spendCalls++
			if spendCalls == 1 {
				http.Error(w, `{"error":"temporary gateway"}`, http.StatusBadGateway)
				return
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-spend-retry", "status": "ACTIVE"}})
		case "budget.release":
			t.Fatalf("unexpected budget.release after full reservation spend")
		case "agent.state.set":
			t.Fatalf("transient spend capture retry should not persist capture-failure state")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-spend-retry",
		AgentID:               "agent-budget-spend-retry",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-spend-retry",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-spend-retry"
	runtime.scratch.ActiveSessionID = "session-budget-spend-retry"
	runtime.scratch.ActiveRunID = "run-budget-spend-retry"

	resp, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if resp.Content != "ok" || llm.calls != 1 {
		t.Fatalf("unexpected provider result=%+v calls=%d", resp, llm.calls)
	}
	if spendCalls != 2 {
		t.Fatalf("expected one transient spend failure plus one retry success, got %d spend calls", spendCalls)
	}
	if want := []string{"budget.account.ensure", "budget.reserve", "budget.spend", "budget.spend"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("unexpected methods: got %#v want %#v", methods, want)
	}
	if runtime.scratch.LastWakeReason == runtimeBudgetCaptureFailedAfterProviderSuccessState {
		t.Fatalf("transient retry should not leave capture-failure scratch state: %+v", runtime.scratch)
	}
}

func TestRuntimeBudgetedLLMPersistsCaptureFailureWithFreshContextAfterSpendTimeout(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage:   TokenUsage{TotalTokens: 3},
	}}
	methods := []string{}
	var persistedScratch RuntimeScratchState
	var cancelSpendContext context.CancelFunc

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-spend-timeout", "status": "ACTIVE"}})
		case "budget.spend":
			if cancelSpendContext == nil {
				t.Fatal("spend cancellation hook was not configured")
			}
			cancelSpendContext()
			<-r.Context().Done()
		case "budget.release":
			t.Fatalf("unexpected budget.release after provider success and spend capture timeout")
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &persistedScratch); err != nil {
				t.Fatalf("decode persisted scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-spend-timeout",
		AgentID:               "agent-budget-spend-timeout",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-spend-timeout",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  2,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-spend-timeout"
	runtime.scratch.ActiveSessionID = "session-budget-spend-timeout"
	runtime.scratch.ActiveRunID = "run-budget-spend-timeout"

	ctx, cancel := context.WithCancel(context.Background())
	cancelSpendContext = cancel
	defer cancel()
	_, err := runtime.agent.LLM.Chat(ctx, []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, errRuntimeBudgetExhausted) || !strings.Contains(err.Error(), runtimeBudgetCaptureFailedAfterProviderSuccessState) {
		t.Fatalf("expected spend timeout capture-failure budget error, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected provider to be called once before spend timeout, got %d", llm.calls)
	}
	if persistedScratch.LastWakeReason != runtimeBudgetCaptureFailedAfterProviderSuccessState {
		t.Fatalf("expected persisted capture-failure state after spend timeout, got %+v", persistedScratch)
	}
	if want := []string{"budget.account.ensure", "budget.reserve", "budget.spend", "agent.state.set"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("unexpected methods: got %#v want %#v", methods, want)
	}
}

func TestRuntimeBudgetedLLMChargesLocalTokensWhenUnusedReleaseFails(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage:   TokenUsage{TotalTokens: 3},
	}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve", "budget.spend":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-release-fail", "status": "ACTIVE"}})
		case "budget.release":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32000,
					"message": "release unavailable",
				},
			})
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-release-fail",
		AgentID:               "agent-budget-release-fail",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-release-fail",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-release-fail"
	runtime.scratch.ActiveSessionID = "session-budget-release-fail"
	runtime.scratch.ActiveRunID = "run-budget-release-fail"
	runtime.tokenMu.Lock()
	runtime.limitsGroupID = "limit-group-release-fail"
	runtime.limitsLoaded = true
	runtime.dailyRemaining = 100
	runtime.weeklyRemaining = 100
	runtime.tokenMu.Unlock()

	resp, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if err != nil {
		t.Fatalf("unused release failure must not fail a paid provider response: %v", err)
	}
	if resp == nil || resp.Content != "ok" {
		t.Fatalf("unexpected response after deferred release cleanup: %+v", resp)
	}
	runtime.tokenMu.Lock()
	daily := runtime.dailyRemaining
	weekly := runtime.weeklyRemaining
	runtime.tokenMu.Unlock()
	if daily != 97 || weekly != 97 {
		t.Fatalf("expected local token budget charged before release failure return, got daily=%d weekly=%d", daily, weekly)
	}
}

func TestRuntimeBudgetedLLMFailsClosedWhenUsageExceedsReservation(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage:   TokenUsage{TotalTokens: 20},
	}}
	spendMicros := int64(0)
	releaseCalled := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-overspend", "status": "ACTIVE"}})
		case "budget.spend":
			spendMicros = int64(req.Params["amount_micros"].(float64))
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-overspend", "status": "ACTIVE"}})
		case "budget.release":
			releaseCalled = true
			t.Fatalf("unexpected release after overspend consumed full reservation")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-overspend",
		AgentID:               "agent-budget-overspend",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-overspend",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-overspend"
	runtime.scratch.ActiveSessionID = "session-budget-overspend"
	runtime.scratch.ActiveRunID = "run-budget-overspend"
	runtime.tokenMu.Lock()
	runtime.limitsGroupID = "limit-group-overspend"
	runtime.limitsLoaded = true
	runtime.dailyRemaining = 100
	runtime.weeklyRemaining = 100
	runtime.tokenMu.Unlock()

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, errRuntimeBudgetExhausted) || !strings.Contains(err.Error(), "exceeded reserved budget") {
		t.Fatalf("expected overspend budget exhaustion, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected provider to be called once before overspend detection, got %d", llm.calls)
	}
	if spendMicros != 10 {
		t.Fatalf("spend amount = %d, want capped full reservation 10", spendMicros)
	}
	if releaseCalled {
		t.Fatal("release should not be called after full reservation spend")
	}
	runtime.tokenMu.Lock()
	daily := runtime.dailyRemaining
	weekly := runtime.weeklyRemaining
	runtime.tokenMu.Unlock()
	if daily != 80 || weekly != 80 {
		t.Fatalf("expected local token budget charged before overspend return, got daily=%d weekly=%d", daily, weekly)
	}
}

func TestRuntimeBudgetedLLMReleasesReservationOnProviderError(t *testing.T) {
	llm := &budgetTestLLM{err: errors.New("provider down")}
	releaseMicros := int64(0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-error", "status": "ACTIVE"}})
		case "budget.release":
			releaseMicros = int64(req.Params["amount_micros"].(float64))
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-error", "status": "ACTIVE"}})
		case "budget.spend":
			t.Fatalf("unexpected spend after provider error")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-error",
		AgentID:               "agent-budget-error",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-error",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  2,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-error"
	runtime.scratch.ActiveSessionID = "session-budget-error"
	runtime.scratch.ActiveRunID = "run-budget-error"

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "provider down") {
		t.Fatalf("expected provider error, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected provider to be called once after successful reserve, got %d", llm.calls)
	}
	if releaseMicros != 10 {
		t.Fatalf("release amount = %d, want 10", releaseMicros)
	}
}

func TestRuntimeBudgetedLLMRetriesTransientReleaseAfterProviderError(t *testing.T) {
	llm := &budgetTestLLM{err: errors.New("provider down")}
	releaseCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-release-retry", "status": "ACTIVE"}})
		case "budget.release":
			releaseCalls++
			if releaseCalls == 1 {
				http.Error(w, `{"error":"temporary unavailable"}`, http.StatusServiceUnavailable)
				return
			}
			if got := int64(req.Params["amount_micros"].(float64)); got != 10 {
				t.Fatalf("release amount = %d, want 10", got)
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-release-retry", "status": "ACTIVE"}})
		case "budget.spend":
			t.Fatalf("unexpected spend after provider error")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-release-retry",
		AgentID:               "agent-budget-release-retry",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-release-retry",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-release-retry"
	runtime.scratch.ActiveSessionID = "session-budget-release-retry"
	runtime.scratch.ActiveRunID = "run-budget-release-retry"

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "provider down") {
		t.Fatalf("expected original provider error after release retry, got %v", err)
	}
	if strings.Contains(err.Error(), "budget release failed") {
		t.Fatalf("transient release retry should not mask provider error, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected provider to be called once after reserve, got %d", llm.calls)
	}
	if releaseCalls != 2 {
		t.Fatalf("expected one transient release failure plus one retry success, got %d", releaseCalls)
	}
}

func TestRuntimeBudgetedLLMProviderErrorDoesNotBecomeBudgetErrorWhenReleaseFails(t *testing.T) {
	llm := &budgetTestLLM{err: errors.New("provider down")}
	releaseCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-release-hard-fail", "status": "ACTIVE"}})
		case "budget.release":
			releaseCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32000,
					"message": "release unavailable",
				},
			})
		case "budget.spend":
			t.Fatalf("unexpected spend after provider error")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-release-hard-fail",
		AgentID:               "agent-budget-release-hard-fail",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-release-hard-fail",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-release-hard-fail"
	runtime.scratch.ActiveSessionID = "session-budget-release-hard-fail"
	runtime.scratch.ActiveRunID = "run-budget-release-hard-fail"

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "provider down") {
		t.Fatalf("expected original provider error, got %v", err)
	}
	if errors.Is(err, errRuntimeBudgetExhausted) || strings.Contains(err.Error(), "release llm provider budget") {
		t.Fatalf("release cleanup failure must not mask provider error, got %v", err)
	}
	if releaseCalls != 1 {
		t.Fatalf("expected one best-effort release attempt, got %d", releaseCalls)
	}
}

func TestRuntimeBudgetedLLMUsesDetachedCleanupContextAfterProviderContextCancel(t *testing.T) {
	var cancel context.CancelFunc
	llm := &budgetTestLLM{
		err: errors.New("provider canceled"),
		onChat: func() {
			cancel()
		},
	}
	released := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-cancel", "status": "ACTIVE"}})
		case "budget.release":
			if err := r.Context().Err(); err != nil {
				t.Fatalf("release should use detached cleanup context, got request context error %v", err)
			}
			released = true
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-cancel", "status": "ACTIVE"}})
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-cancel",
		AgentID:               "agent-budget-cancel",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-cancel",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-cancel"
	runtime.scratch.ActiveSessionID = "session-budget-cancel"
	runtime.scratch.ActiveRunID = "run-budget-cancel"

	ctx, ctxCancel := context.WithCancel(context.Background())
	cancel = ctxCancel
	_, err := runtime.agent.LLM.Chat(ctx, []Message{{Role: "user", Content: "go"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "provider canceled") {
		t.Fatalf("expected provider error, got %v", err)
	}
	if !released {
		t.Fatal("expected budget.release to be sent even after provider context cancellation")
	}
}

func TestRuntimeBudgetedLLMReleasesReservationOnProviderTimeout(t *testing.T) {
	llm := &budgetTestLLM{waitCtx: true}
	releaseMicros := int64(0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-timeout", "status": "ACTIVE"}})
		case "budget.release":
			releaseMicros = int64(req.Params["amount_micros"].(float64))
			if got := rpcString(req.Params, "reason"); got != "llm_provider_error" {
				t.Fatalf("release reason = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-timeout", "status": "ACTIVE"}})
		case "budget.spend":
			t.Fatalf("unexpected spend after provider timeout")
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-timeout",
		AgentID:               "agent-budget-timeout",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-timeout",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
		ProviderCallTimeout:   5 * time.Millisecond,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-timeout"
	runtime.scratch.ActiveSessionID = "session-budget-timeout"
	runtime.scratch.ActiveRunID = "run-budget-timeout"

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "llm provider call timed out") {
		t.Fatalf("expected provider timeout error, got %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("expected provider to be called once after reserve, got %d", llm.calls)
	}
	if releaseMicros != 10 {
		t.Fatalf("release amount = %d, want 10", releaseMicros)
	}
}

func TestRuntimeBudgetedLLMSpendsPartialUsageOnProviderTimeout(t *testing.T) {
	llm := &budgetTestLLM{
		response: &LLMResponse{Usage: TokenUsage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7}},
		err:      context.DeadlineExceeded,
	}
	spendMicros := int64(0)
	releaseMicros := int64(0)
	releaseReason := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "budget.account.ensure", "budget.reserve":
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-partial-timeout", "status": "ACTIVE"}})
		case "budget.spend":
			spendMicros = int64(req.Params["amount_micros"].(float64))
			if got := rpcString(req.Params, "reason"); got != "runtime_llm_provider_usage" {
				t.Fatalf("spend reason = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-partial-timeout", "status": "ACTIVE"}})
		case "budget.release":
			releaseMicros = int64(req.Params["amount_micros"].(float64))
			releaseReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, map[string]any{"account": map[string]any{"account_id": "acct-budget-partial-timeout", "status": "ACTIVE"}})
		default:
			t.Fatalf("unexpected budget rpc method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            server.URL,
		RhizomeToken:          "token",
		WorkspaceID:           "ws-budget-partial-timeout",
		AgentID:               "agent-budget-partial-timeout",
		ProviderID:            "provider-budget",
		Model:                 "gpt-budget",
		BudgetAccountID:       "acct-budget-partial-timeout",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   10,
		BudgetMicrosPerToken:  1,
		ProviderCallTimeout:   time.Second,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch.ActiveTaskID = "task-budget-partial-timeout"
	runtime.scratch.ActiveSessionID = "session-budget-partial-timeout"
	runtime.scratch.ActiveRunID = "run-budget-partial-timeout"

	_, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected provider timeout error, got %v", err)
	}
	if spendMicros != 7 {
		t.Fatalf("spend amount = %d, want 7", spendMicros)
	}
	if releaseMicros != 3 || releaseReason != "runtime_llm_provider_unused_reservation" {
		t.Fatalf("release amount/reason = %d/%q, want 3/runtime_llm_provider_unused_reservation", releaseMicros, releaseReason)
	}
}

func TestRuntimeBudgetedLLMRecordsTokenUsageOutsideTaskLoopObserver(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage: TokenUsage{
			PromptTokens:     2,
			CompletionTokens: 3,
			TotalTokens:      5,
		},
	}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-budget-usage",
		AgentID:     "agent-budget-usage",
	}, llm)
	runtime.tokenMu.Lock()
	runtime.limitsGroupID = "limit-group-usage"
	runtime.limitsLoaded = true
	runtime.dailyRemaining = 10
	runtime.weeklyRemaining = 20
	runtime.tokenMu.Unlock()

	if _, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil); err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	runtime.tokenMu.Lock()
	daily := runtime.dailyRemaining
	weekly := runtime.weeklyRemaining
	runtime.tokenMu.Unlock()
	if daily != 5 || weekly != 15 {
		t.Fatalf("expected token usage to decrement local limits to daily=5 weekly=15, got daily=%d weekly=%d", daily, weekly)
	}
}

func TestRuntimeBudgetedLLMRecordsTotalTokenUsageWhenBreakdownMissing(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage:   TokenUsage{TotalTokens: 7},
	}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-budget-total-usage",
		AgentID:     "agent-budget-total-usage",
	}, llm)
	runtime.tokenMu.Lock()
	runtime.limitsGroupID = "limit-group-total-usage"
	runtime.limitsLoaded = true
	runtime.dailyRemaining = 10
	runtime.weeklyRemaining = 20
	runtime.tokenMu.Unlock()

	if _, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil); err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	runtime.tokenMu.Lock()
	daily := runtime.dailyRemaining
	weekly := runtime.weeklyRemaining
	runtime.tokenMu.Unlock()
	if daily != 3 || weekly != 13 {
		t.Fatalf("expected total token usage fallback to decrement local limits to daily=3 weekly=13, got daily=%d weekly=%d", daily, weekly)
	}
}

func TestRuntimeBudgetedLLMRecordsBreakdownWhenTotalTokenUsageMissing(t *testing.T) {
	llm := &budgetTestLLM{response: &LLMResponse{
		Content: "ok",
		Usage: TokenUsage{
			PromptTokens:     2,
			CompletionTokens: 3,
		},
	}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-budget-breakdown-usage",
		AgentID:     "agent-budget-breakdown-usage",
	}, llm)
	runtime.tokenMu.Lock()
	runtime.limitsGroupID = "limit-group-breakdown-usage"
	runtime.limitsLoaded = true
	runtime.dailyRemaining = 10
	runtime.weeklyRemaining = 20
	runtime.tokenMu.Unlock()

	if _, err := runtime.agent.LLM.Chat(context.Background(), []Message{{Role: "user", Content: "go"}}, nil); err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	runtime.tokenMu.Lock()
	daily := runtime.dailyRemaining
	weekly := runtime.weeklyRemaining
	runtime.tokenMu.Unlock()
	if daily != 5 || weekly != 15 {
		t.Fatalf("expected prompt/completion fallback to decrement local limits to daily=5 weekly=15, got daily=%d weekly=%d", daily, weekly)
	}
}
