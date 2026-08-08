package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWaitForManagedAgentRequestResultSurfacesTimeoutWithLastStatus(t *testing.T) {
	origPoll := managedAgentRequestPollInterval
	managedAgentRequestPollInterval = 1 * time.Millisecond
	defer func() { managedAgentRequestPollInterval = origPoll }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request.result" {
			t.Fatalf("unexpected method %q", method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"request_id":   "req-timeout",
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-1",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	record, err := waitForManagedAgentRequestResult(context.Background(), NewRhizomeClient(server.URL, ""), "ws-1", "req-timeout", 5*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if record.RequestID != "req-timeout" || record.Status != "PENDING" {
		t.Fatalf("expected last pending record on timeout, got %+v", record)
	}
	if !strings.Contains(err.Error(), "timed out waiting for request req-timeout after 5ms") || !strings.Contains(err.Error(), "last status: PENDING") {
		t.Fatalf("expected explicit timeout error, got %v", err)
	}
}

func TestManagedAgentControlTokenPrefersLocalAgentToken(t *testing.T) {
	token := managedAgentControlToken(
		RhizomeConnectionProfile{AgentToken: "global-stale-token"},
		LocalRuntimeProfile{AgentToken: "local-live-token"},
	)
	if token != "local-live-token" {
		t.Fatalf("expected local token to win, got %q", token)
	}
}

func TestManagedAgentControlAuthPairsLocalTokenWithLocalAgentID(t *testing.T) {
	token, fromAgentID := managedAgentControlAuth(
		RhizomeConnectionProfile{AgentID: "global-manager", AgentToken: "global-stale-token"},
		LocalRuntimeProfile{
			AgentID:    "stale-local-agent",
			AgentToken: "local-live-token",
			RegisteredExecutor: RegisteredExecutorIdentity{
				AgentID: "alpha",
			},
		},
		ManagedAgentRecord{AgentID: "alpha"},
	)
	if token != "local-live-token" {
		t.Fatalf("expected local token to win, got %q", token)
	}
	if fromAgentID != "alpha" {
		t.Fatalf("expected from_agent_id to match local token principal, got %q", fromAgentID)
	}
}

func TestManagedAgentControlAuthPairsGlobalTokenWithGlobalAgentID(t *testing.T) {
	token, fromAgentID := managedAgentControlAuth(
		RhizomeConnectionProfile{AgentID: "manager-agent", AgentToken: "global-token"},
		LocalRuntimeProfile{AgentID: "alpha"},
		ManagedAgentRecord{AgentID: "alpha"},
	)
	if token != "global-token" {
		t.Fatalf("expected global token, got %q", token)
	}
	if fromAgentID != "manager-agent" {
		t.Fatalf("expected global agent id for global token, got %q", fromAgentID)
	}
}

func TestManagedAgentControlAuthFallsBackToRecordAgentForLocalToken(t *testing.T) {
	token, fromAgentID := managedAgentControlAuth(
		RhizomeConnectionProfile{AgentID: "global-manager", AgentToken: "global-stale-token"},
		LocalRuntimeProfile{AgentToken: "local-live-token"},
		ManagedAgentRecord{AgentID: "alpha"},
	)
	if token != "local-live-token" {
		t.Fatalf("expected local token, got %q", token)
	}
	if fromAgentID != "alpha" {
		t.Fatalf("expected managed record agent id for local token, got %q", fromAgentID)
	}
}

func TestWaitForManagedAgentRequestResultSurfacesCancellationExplicitly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := waitForManagedAgentRequestResult(ctx, NewRhizomeClient("http://127.0.0.1:1", ""), "ws-1", "req-canceled", time.Second)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "request req-canceled canceled while waiting for result") {
		t.Fatalf("expected explicit canceled error, got %v", err)
	}
}

func TestWaitForManagedAgentRequestResultSurfacesParentDeadlineExplicitly(t *testing.T) {
	origPoll := managedAgentRequestPollInterval
	managedAgentRequestPollInterval = 1 * time.Millisecond
	defer func() { managedAgentRequestPollInterval = origPoll }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request.result" {
			t.Fatalf("unexpected method %q", method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"request_id":   "req-deadline",
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-1",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := waitForManagedAgentRequestResult(ctx, NewRhizomeClient(server.URL, ""), "ws-1", "req-deadline", time.Second)
	if err == nil {
		t.Fatal("expected parent deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded classification, got %v", err)
	}
	if !strings.Contains(err.Error(), "request req-deadline timed out while waiting for result") {
		t.Fatalf("expected explicit deadline error, got %v", err)
	}
}

func TestWaitForManagedAgentRequestResultIgnoresNonTerminalResponseUntilCompleted(t *testing.T) {
	origPoll := managedAgentRequestPollInterval
	managedAgentRequestPollInterval = 1 * time.Millisecond
	defer func() { managedAgentRequestPollInterval = origPoll }()

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request.result" {
			t.Fatalf("unexpected method %q", method)
		}
		callCount++
		status := "RUNNING"
		response := `{"status":"warming_up"}`
		if callCount >= 2 {
			status = "COMPLETED"
			response = `{"status":"ok"}`
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"request_id":   "req-running-response",
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-1",
				"status":       status,
				"response":     response,
			},
		})
	}))
	defer server.Close()

	record, err := waitForManagedAgentRequestResult(context.Background(), NewRhizomeClient(server.URL, ""), "ws-1", "req-running-response", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForManagedAgentRequestResult() error: %v", err)
	}
	if callCount < 2 {
		t.Fatalf("expected helper wait to keep polling past non-terminal response, got %d call(s)", callCount)
	}
	if record.Status != "COMPLETED" || record.Response != `{"status":"ok"}` {
		t.Fatalf("expected terminal completed record, got %+v", record)
	}
}
