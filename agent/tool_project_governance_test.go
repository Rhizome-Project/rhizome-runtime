package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectGovernanceToolRegistrationIsEnvGated(t *testing.T) {
	t.Setenv("RHIZOME_ENABLE_PROJECT_GOVERNANCE_TOOLS", "")
	agent := &Agent{
		Workdir:     t.TempDir(),
		Client:      NewRhizomeClient("http://127.0.0.1:1", "token"),
		WorkspaceID: "ws",
		AgentID:     "agent-alpha",
	}
	agent.Init()
	if _, ok := agent.registry.Get("project_governance_challenge"); ok {
		t.Fatalf("project governance tool registered while env gate is off")
	}

	t.Setenv("RHIZOME_ENABLE_PROJECT_GOVERNANCE_TOOLS", "true")
	agent.Init()
	if _, ok := agent.registry.Get("project_governance_challenge"); !ok {
		t.Fatalf("project governance tool was not registered while env gate is on")
	}
}

func TestProjectGovernanceToolRejectsCrossProjectRuntimeBindingBeforeRPC(t *testing.T) {
	tool := NewProjectGovernanceTool(NewRhizomeClient("http://127.0.0.1:1", "token"), "ws", "agent-beta").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-current", TaskID: "task-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"action":              "check",
		"project_id":          "project-old",
		"challenged_agent_id": "agent-alpha",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "active task is bound to project_id project-current") {
		t.Fatalf("expected active-project binding rejection before RPC, got %+v", result)
	}
}

func TestProjectGovernanceToolRaiseStopsWhenPrecheckFails(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     any             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		methods = append(methods, req.Method)
		if req.Method != "project.governance.predicates.check" {
			t.Fatalf("unexpected method after failed precheck: %s", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"all_hold": false,
				"predicate_results": []map[string]any{
					{"name": "fanout_absent", "holds": false, "evidence": "open implementation task exists"},
				},
			},
		})
	}))
	defer server.Close()

	tool := NewProjectGovernanceTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta")
	result := tool.Execute(context.Background(), map[string]any{
		"action":              "raise",
		"project_id":          "project-1",
		"challenged_agent_id": "agent-alpha",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected failed precheck tool error, got %+v", result)
	}
	if !strings.Contains(result.Output, "stall_predicates_not_satisfied") {
		t.Fatalf("expected failed precheck output, got %s", result.Output)
	}
	if len(methods) != 1 || methods[0] != "project.governance.predicates.check" {
		t.Fatalf("expected only predicate check call, got %+v", methods)
	}
}

func TestProjectGovernanceToolListsOpenChallenges(t *testing.T) {
	var gotMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     any             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod = req.Method
		if req.Method != "project.governance.challenge.list" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"challenges": []map[string]any{{
					"challenge_id":        "govchal-1",
					"workspace_id":        "ws",
					"project_id":          "project-1",
					"challenged_agent_id": "agent-alpha",
					"challenger_agent_id": "agent-beta",
					"state":               "DEFENSE_OPEN",
					"current_round":       1,
					"max_rounds":          3,
					"round_opened_at":     "2026-05-31T00:00:00Z",
					"created_at":          "2026-05-31T00:00:00Z",
					"updated_at":          "2026-05-31T00:00:00Z",
				}},
			},
		})
	}))
	defer server.Close()

	tool := NewProjectGovernanceTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta")
	result := tool.Execute(context.Background(), map[string]any{
		"action":     "list",
		"project_id": "project-1",
		"state":      "DEFENSE_OPEN",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected list success, got %+v", result)
	}
	if gotMethod != "project.governance.challenge.list" || !strings.Contains(result.Output, "govchal-1") {
		t.Fatalf("unexpected list output method=%s output=%s", gotMethod, result.Output)
	}
}
