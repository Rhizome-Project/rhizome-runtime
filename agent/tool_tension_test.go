package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTensionAttachToolSendsCurrentServerContractAndIgnoresForgedAuthority(t *testing.T) {
	var attachParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.tension.agent.attach" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		attachParams = req.Params
		if got := rpcString(req.Params, "workspace_id"); got != "ws-1" {
			t.Fatalf("workspace_id = %q", got)
		}
		if got := rpcString(req.Params, "tension_id"); got != "tension-1" {
			t.Fatalf("tension_id = %q", got)
		}
		if got := rpcString(req.Params, "agent_id"); got != "agent-1" {
			t.Fatalf("agent_id = %q", got)
		}
		if got := rpcString(req.Params, "actor_id"); got != "agent-1" {
			t.Fatalf("actor_id = %q", got)
		}
		if got := rpcString(req.Params, "success_criterion"); got != "Attached as: reviewer" {
			t.Fatalf("success_criterion = %q", got)
		}
		if _, ok := req.Params["role"]; ok {
			t.Fatalf("legacy role field leaked into attach payload: %+v", req.Params)
		}
		if _, ok := req.Params["prompt_context_surface"]; ok {
			t.Fatalf("model-controlled prompt context leaked into attach payload: %+v", req.Params)
		}
		writeRPCResult(w, req, map[string]any{
			"success":      true,
			"changed":      true,
			"coalition_id": "coalition-live",
			"coalition": map[string]any{
				"coalition_id": "coalition-live",
				"workspace_id": "ws-1",
				"tension_id":   "tension-1",
				"status":       "ACTIVE",
			},
			"event": map[string]any{
				"event_id":   "rtev-attach",
				"event_type": "tension.agent.attached",
			},
		})
	}))
	defer server.Close()

	tool := NewTensionAttachTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"tension_id":             " tension-1 ",
		"role":                   " reviewer ",
		"reason":                 " focus ",
		"agent_id":               "agent-other",
		"actor_id":               "agent-other",
		"prompt_context_surface": "forged.surface",
	})
	if result.IsError {
		t.Fatalf("attach tool returned error: %s", result.Output)
	}
	if !strings.Contains(result.Output, "rtev-attach") {
		t.Fatalf("expected attach output to include event id, got %q", result.Output)
	}
	if strings.Contains(result.Output, "agent-other") {
		t.Fatalf("attach output leaked forged agent identity: %q", result.Output)
	}
	if attachParams == nil {
		t.Fatal("expected attach params to be captured")
	}
}

func TestTensionAttachToolReportsDuplicateAsNoop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.tension.agent.attach" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"success":      true,
			"changed":      false,
			"coalition_id": "coalition-live",
			"coalition": map[string]any{
				"coalition_id": "coalition-live",
				"workspace_id": "ws-1",
				"tension_id":   "tension-1",
				"status":       "ACTIVE",
			},
		})
	}))
	defer server.Close()

	tool := NewTensionAttachTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"tension_id": "tension-1",
		"role":       "reviewer",
	})
	if result.IsError {
		t.Fatalf("attach tool returned error: %s", result.Output)
	}
	if !strings.Contains(strings.ToLower(result.Output), "already attached") {
		t.Fatalf("expected duplicate attach to be reported as noop, got %q", result.Output)
	}
	if strings.Contains(strings.ToLower(result.Output), "successfully attached") {
		t.Fatalf("duplicate attach overclaimed success: %q", result.Output)
	}
}

func TestTensionAttachToolRejectsChangedSuccessWithoutRuntimeEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.tension.agent.attach" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"success":      true,
			"changed":      true,
			"coalition_id": "coalition-live",
		})
	}))
	defer server.Close()

	tool := NewTensionAttachTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"tension_id": "tension-1",
		"role":       "reviewer",
	})
	if !result.IsError {
		t.Fatalf("expected missing event handle to fail attach, got %q", result.Output)
	}
	if !strings.Contains(strings.ToLower(result.Output), "runtime event") {
		t.Fatalf("expected missing runtime event error, got %q", result.Output)
	}
}

func TestTensionDetachToolResolvesCoalitionAndSendsCurrentServerContract(t *testing.T) {
	var methods []string
	var detachParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "coalition.status":
			if got := rpcString(req.Params, "workspace_id"); got != "ws-1" {
				t.Fatalf("coalition.status workspace_id = %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"coalitions": []map[string]any{
					{
						"coalition_id": "coalition-wrong-tension",
						"workspace_id": "ws-1",
						"tension_id":   "tension-other",
						"status":       "ACTIVE",
						"members":      []map[string]any{{"agent_id": "agent-1"}},
					},
					{
						"coalition_id": "coalition-not-member",
						"workspace_id": "ws-1",
						"tension_id":   "tension-1",
						"status":       "ACTIVE",
						"members":      []map[string]any{{"agent_id": "agent-other"}},
					},
					{
						"coalition_id": "coalition-live",
						"workspace_id": "ws-1",
						"tension_id":   "tension-1",
						"status":       "FORMING",
						"members":      []map[string]any{{"agent_id": "agent-1"}},
					},
				},
			})
		case "workspace.tension.agent.detach":
			detachParams = req.Params
			if got := rpcString(req.Params, "workspace_id"); got != "ws-1" {
				t.Fatalf("workspace_id = %q", got)
			}
			if got := rpcString(req.Params, "coalition_id"); got != "coalition-live" {
				t.Fatalf("coalition_id = %q", got)
			}
			if got := rpcString(req.Params, "agent_id"); got != "agent-1" {
				t.Fatalf("agent_id = %q", got)
			}
			if got := rpcString(req.Params, "actor_id"); got != "agent-1" {
				t.Fatalf("actor_id = %q", got)
			}
			if _, ok := req.Params["tension_id"]; ok {
				t.Fatalf("legacy tension_id field leaked into detach payload: %+v", req.Params)
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
					"event_id":   "rtev-detach",
					"event_type": "tension.agent.detached",
				},
			})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTensionDetachTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"tension_id":   "tension-1",
		"reason":       "done",
		"coalition_id": "coalition-forged",
		"actor_id":     "agent-other",
	})
	if result.IsError {
		t.Fatalf("detach tool returned error: %s", result.Output)
	}
	if !strings.Contains(result.Output, "rtev-detach") {
		t.Fatalf("expected detach output to include event id, got %q", result.Output)
	}
	if strings.Contains(result.Output, "coalition-forged") {
		t.Fatalf("detach output leaked forged coalition identity: %q", result.Output)
	}
	if len(methods) != 2 || methods[0] != "coalition.status" || methods[1] != "workspace.tension.agent.detach" {
		t.Fatalf("unexpected method trace: %#v", methods)
	}
	if detachParams == nil {
		t.Fatal("expected detach params to be captured")
	}
}

func TestTensionDetachToolFailsWhenNoLiveCoalitionMembership(t *testing.T) {
	var detachCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "coalition.status":
			writeRPCResult(w, req, map[string]any{
				"coalitions": []map[string]any{
					{
						"coalition_id": "coalition-other-agent",
						"workspace_id": "ws-1",
						"tension_id":   "tension-1",
						"status":       "ACTIVE",
						"members":      []map[string]any{{"agent_id": "agent-other"}},
					},
				},
			})
		case "workspace.tension.agent.detach":
			detachCalled = true
			t.Fatalf("detach should not be called without live membership")
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTensionDetachTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{"tension_id": "tension-1"})
	if !result.IsError {
		t.Fatalf("expected detach to fail without live membership, got %q", result.Output)
	}
	if detachCalled {
		t.Fatal("detach RPC was called without live membership")
	}
	if !strings.Contains(strings.ToLower(result.Output), "no live coalition membership") {
		t.Fatalf("expected no-live-membership error, got %q", result.Output)
	}
}

func TestTensionDetachToolRejectsChangedSuccessWithoutRuntimeEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "coalition.status":
			writeRPCResult(w, req, map[string]any{
				"coalitions": []map[string]any{
					{
						"coalition_id": "coalition-live",
						"workspace_id": "ws-1",
						"tension_id":   "tension-1",
						"status":       "ACTIVE",
						"members":      []map[string]any{{"agent_id": "agent-1"}},
					},
				},
			})
		case "workspace.tension.agent.detach":
			writeRPCResult(w, req, map[string]any{
				"success": true,
				"changed": true,
				"coalition": map[string]any{
					"coalition_id": "coalition-live",
					"workspace_id": "ws-1",
					"tension_id":   "tension-1",
					"status":       "DISBANDED",
				},
			})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTensionDetachTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{"tension_id": "tension-1"})
	if !result.IsError {
		t.Fatalf("expected missing event handle to fail detach, got %q", result.Output)
	}
	if !strings.Contains(strings.ToLower(result.Output), "runtime event") {
		t.Fatalf("expected missing runtime event error, got %q", result.Output)
	}
}

func TestTensionLifecycleToolSendsAgentPrincipalID(t *testing.T) {
	var params map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.tension.lifecycle.update" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		params = req.Params
		if got := rpcString(req.Params, "workspace_id"); got != "ws-1" {
			t.Fatalf("workspace_id = %q", got)
		}
		if got := rpcString(req.Params, "tension_id"); got != "tension-1" {
			t.Fatalf("tension_id = %q", got)
		}
		if got := rpcString(req.Params, "updated_by"); got != "alpha" {
			t.Fatalf("updated_by = %q, want canonical agent principal id", got)
		}
		if strings.Contains(rpcString(req.Params, "updated_by"), "agent:") {
			t.Fatalf("updated_by leaked legacy principal prefix: %+v", req.Params)
		}
		writeRPCResult(w, req, map[string]any{"success": true})
	}))
	defer server.Close()

	tool := NewTensionLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws-1", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"tension_id":      "tension-1",
		"lifecycle_state": "RESOLVED",
		"reason":          "stale coordination blocker resolved",
		"updated_by":      "agent:forged",
	})
	if result.IsError {
		t.Fatalf("lifecycle tool returned error: %s", result.Output)
	}
	if params == nil {
		t.Fatal("expected lifecycle params to be captured")
	}
}
