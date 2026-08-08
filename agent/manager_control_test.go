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

func TestManagedRuntimeControlClientPauseRequestsAndWaitsForResult(t *testing.T) {
	var methods []string
	var requestParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)
		switch method {
		case "agent.request":
			requestParams, _ = req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-1",
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-1",
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "COMPLETED",
					"response":     `{"status":"ok","control":{"paused":true}}`,
					"responded_at": "2026-03-27T09:00:00Z",
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	record, err := client.Pause(context.Background(), "manual pause", 10*time.Second)
	if err != nil {
		t.Fatalf("Pause() error: %v", err)
	}
	if len(methods) != 2 || methods[0] != "agent.request" || methods[1] != "agent.request.result" {
		t.Fatalf("unexpected method sequence: %#v", methods)
	}
	if requestParams["method"] != "runtime.pause" {
		t.Fatalf("expected runtime.pause method, got %+v", requestParams)
	}
	if requestParams["from_agent_id"] != "manager" || requestParams["to_agent_id"] != "agent-1" {
		t.Fatalf("unexpected request params: %+v", requestParams)
	}
	if record.RequestID != "req-1" || record.Status != "COMPLETED" {
		t.Fatalf("unexpected final request record: %+v", record)
	}
}

func TestManagedRuntimeControlClientWaitSurfacesTimeoutWithLastStatus(t *testing.T) {
	origPoll := managedRuntimeControlPollInterval
	managedRuntimeControlPollInterval = 1 * time.Millisecond
	defer func() { managedRuntimeControlPollInterval = origPoll }()

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

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	record, err := client.Wait(context.Background(), "req-timeout", 5*time.Millisecond)
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

func TestManagedRuntimeControlClientWaitSurfacesCancellationExplicitly(t *testing.T) {
	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient("http://127.0.0.1:1", ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Wait(ctx, "req-canceled", time.Second)
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

func TestManagedRuntimeControlClientWaitSurfacesParentDeadlineExplicitly(t *testing.T) {
	origPoll := managedRuntimeControlPollInterval
	managedRuntimeControlPollInterval = 1 * time.Millisecond
	defer func() { managedRuntimeControlPollInterval = origPoll }()

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

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := client.Wait(ctx, "req-deadline", time.Second)
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

func TestManagedRuntimeControlClientRequestRejectsPartialCreateTruth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		if method != "agent.request" {
			t.Fatalf("unexpected method %q", method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-1",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	_, err := client.Pause(context.Background(), "manual pause", time.Second)
	if err == nil {
		t.Fatal("expected partial create truth error")
	}
	if !strings.Contains(err.Error(), "agent.request returned partial result: missing request_id") {
		t.Fatalf("expected explicit partial create error, got %v", err)
	}
}

func TestManagedRuntimeControlClientRequestSurfacesTerminalFailureExplicitly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "agent.request":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-failed",
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-failed",
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "FAILED",
					"response":     `{"error":"runtime paused and cannot switch task"}`,
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	record, err := client.SwitchTask(context.Background(), "task-2", "", "reassign", time.Second)
	if err == nil {
		t.Fatal("expected terminal failure error")
	}
	if record.RequestID != "req-failed" || record.Status != "FAILED" {
		t.Fatalf("expected failed record to be preserved, got %+v", record)
	}
	if !strings.Contains(err.Error(), "request req-failed finished with status FAILED") || !strings.Contains(err.Error(), `"error": "runtime paused and cannot switch task"`) {
		t.Fatalf("expected explicit terminal failure detail, got %v", err)
	}
}

func TestManagedRuntimeControlClientWaitSurfacesRequestResultTruthErrorImmediately(t *testing.T) {
	origPoll := managedRuntimeControlPollInterval
	managedRuntimeControlPollInterval = 1 * time.Millisecond
	defer func() { managedRuntimeControlPollInterval = origPoll }()

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
				"request_id":   "req-other",
				"workspace_id": "ws-1",
				"status":       "PENDING",
			},
		})
	}))
	defer server.Close()

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	_, err := client.Wait(context.Background(), "req-expected", time.Second)
	if err == nil {
		t.Fatal("expected request-result truth error")
	}
	if !strings.Contains(err.Error(), `agent.request.result returned mismatched request_id "req-other" (wanted "req-expected")`) {
		t.Fatalf("expected immediate request-result truth error, got %v", err)
	}
}

func TestManagedRuntimeControlClientWaitDoesNotTreatNonTerminalStatusAsCompletion(t *testing.T) {
	origPoll := managedRuntimeControlPollInterval
	managedRuntimeControlPollInterval = 1 * time.Millisecond
	defer func() { managedRuntimeControlPollInterval = origPoll }()

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
				"request_id":   "req-running",
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-1",
				"status":       "RUNNING",
			},
		})
	}))
	defer server.Close()

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	record, err := client.Wait(context.Background(), "req-running", 5*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if record.RequestID != "req-running" || record.Status != "RUNNING" {
		t.Fatalf("expected last running record on timeout, got %+v", record)
	}
	if !strings.Contains(err.Error(), "timed out waiting for request req-running after 5ms") || !strings.Contains(err.Error(), "last status: RUNNING") {
		t.Fatalf("expected explicit timeout with running status, got %v", err)
	}
}

func TestManagedRuntimeControlClientWaitIgnoresNonTerminalResponseUntilCompleted(t *testing.T) {
	origPoll := managedRuntimeControlPollInterval
	managedRuntimeControlPollInterval = 1 * time.Millisecond
	defer func() { managedRuntimeControlPollInterval = origPoll }()

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

	client := ManagedRuntimeControlClient{
		Client:      NewRhizomeClient(server.URL, ""),
		WorkspaceID: "ws-1",
		FromAgentID: "manager",
		ToAgentID:   "agent-1",
	}

	record, err := client.Wait(context.Background(), "req-running-response", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	if callCount < 2 {
		t.Fatalf("expected wait to keep polling past non-terminal response, got %d call(s)", callCount)
	}
	if record.Status != "COMPLETED" || record.Response != `{"status":"ok"}` {
		t.Fatalf("expected terminal completed record, got %+v", record)
	}
}
