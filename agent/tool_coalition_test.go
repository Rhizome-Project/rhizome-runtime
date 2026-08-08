package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCoalitionOfferToolSendsActorIDAndRequiresDurableEvent(t *testing.T) {
	var offerParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "coalition.offer" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		offerParams = req.Params
		if got := rpcString(req.Params, "workspace_id"); got != "ws-1" {
			t.Fatalf("workspace_id = %q", got)
		}
		if got := rpcString(req.Params, "task_id"); got != "task-1" {
			t.Fatalf("task_id = %q", got)
		}
		if got := rpcString(req.Params, "agent_id"); got != "agent-1" {
			t.Fatalf("agent_id = %q", got)
		}
		if got := rpcString(req.Params, "actor_id"); got != "agent-1" {
			t.Fatalf("actor_id = %q", got)
		}
		if _, ok := req.Params["prompt_context_surface"]; ok {
			t.Fatalf("model-controlled prompt context leaked into coalition offer payload: %+v", req.Params)
		}
		writeRPCResult(w, req, map[string]any{
			"success":      true,
			"changed":      true,
			"coalition_id": "coalition-live",
			"coalition": map[string]any{
				"coalition_id": "coalition-live",
				"workspace_id": "ws-1",
				"tension_id":   "tension-1",
				"status":       "FORMING",
			},
			"event": map[string]any{
				"event_id":   "rtev-offer",
				"event_type": "tension.agent.attached",
			},
		})
	}))
	defer server.Close()

	tool := NewCoalitionOfferTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":                " task-1 ",
		"role":                   " reviewer ",
		"actor_id":               "agent-other",
		"prompt_context_surface": "forged.surface",
	})
	if result.IsError {
		t.Fatalf("coalition offer returned error: %s", result.Output)
	}
	if !strings.Contains(result.Output, "rtev-offer") {
		t.Fatalf("expected coalition offer output to include event id, got %q", result.Output)
	}
	if strings.Contains(result.Output, "agent-other") {
		t.Fatalf("offer output leaked forged actor identity: %q", result.Output)
	}
	if offerParams == nil {
		t.Fatal("expected offer params to be captured")
	}
}

func TestCoalitionOfferToolRejectsChangedSuccessWithoutRuntimeEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "coalition.offer" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"success":      true,
			"changed":      true,
			"coalition_id": "coalition-live",
		})
	}))
	defer server.Close()

	tool := NewCoalitionOfferTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{"task_id": "task-1", "role": "reviewer"})
	if !result.IsError {
		t.Fatalf("expected missing event handle to fail coalition offer, got %q", result.Output)
	}
	if !strings.Contains(strings.ToLower(result.Output), "runtime event") {
		t.Fatalf("expected runtime event error, got %q", result.Output)
	}
}

func TestCoalitionOfferToolTreatsRPCFailureAsAdvisoryFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "coalition.offer" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		writeRPCError(w, req, -32000, "no coalition anchor for task")
	}))
	defer server.Close()

	tool := NewCoalitionOfferTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{"task_id": "task-1", "role": "reviewer"})
	if result == nil || result.IsError {
		t.Fatalf("expected advisory non-error fallback, got %+v", result)
	}
	for _, want := range []string{"coalition_offer unavailable", "do not retry coalition_offer blindly", "agent_request"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestCoalitionSeekToolGuidesDirectPeerRequestWhenNoMatches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "coalition.seek" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"matches": []any{},
			"target_resolution": map[string]any{
				"status":  "not_found",
				"task_id": "task-1",
				"error":   "no coalition-eligible tension for task-1",
			},
			"task_id": "task-1",
		})
	}))
	defer server.Close()

	tool := NewCoalitionSeekTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":         "task-1",
		"required_skills": []any{"review"},
		"reason":          "need review",
	})
	if result.IsError {
		t.Fatalf("expected no-match seek to be actionable, not fatal, got %q", result.Output)
	}
	if !strings.Contains(result.Output, "No coalition matches") || !strings.Contains(result.Output, "agent_request") {
		t.Fatalf("expected direct peer request fallback guidance, got %q", result.Output)
	}
}

func TestCoalitionSeekToolGuidesDirectPeerRequestOnRPCError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "coalition.seek" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		writeRPCError(w, req, -32000, "failed to seek coalitions")
	}))
	defer server.Close()

	tool := NewCoalitionSeekTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{"task_id": "task-1"})
	if !result.IsError {
		t.Fatalf("expected coalition seek rpc error, got %q", result.Output)
	}
	if !strings.Contains(result.Output, "agent_request") || !strings.Contains(result.Output, "fall back") {
		t.Fatalf("expected rpc error fallback guidance, got %q", result.Output)
	}
}

func TestCoalitionKickToolSendsActorIDAndRequiresDetachEvent(t *testing.T) {
	var kickParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "coalition.kick" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		kickParams = req.Params
		if got := rpcString(req.Params, "actor_id"); got != "agent-1" {
			t.Fatalf("actor_id = %q", got)
		}
		if got := rpcString(req.Params, "agent_id"); got != "agent-1" {
			t.Fatalf("agent_id = %q", got)
		}
		if got := rpcString(req.Params, "target_id"); got != "agent-2" {
			t.Fatalf("target_id = %q", got)
		}
		writeRPCResult(w, req, map[string]any{
			"success": true,
			"changed": true,
			"coalition": map[string]any{
				"coalition_id": "coalition-live",
				"workspace_id": "ws-1",
				"tension_id":   "tension-1",
				"status":       "ACTIVE",
			},
			"event": map[string]any{
				"event_id":   "rtev-kick",
				"event_type": "tension.agent.detached",
			},
		})
	}))
	defer server.Close()

	tool := NewCoalitionKickTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"coalition_id": "coalition-live",
		"target_id":    "agent-2",
		"actor_id":     "agent-other",
	})
	if result.IsError {
		t.Fatalf("coalition kick returned error: %s", result.Output)
	}
	if !strings.Contains(result.Output, "rtev-kick") {
		t.Fatalf("expected coalition kick output to include event id, got %q", result.Output)
	}
	if kickParams == nil {
		t.Fatal("expected kick params to be captured")
	}
}
