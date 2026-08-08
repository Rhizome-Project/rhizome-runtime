package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestAgentRequestHydrationClient(t *testing.T, response map[string]any) *RhizomeClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.task.hydrate" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, response)
	}))
	t.Cleanup(server.Close)
	return NewRhizomeClient(server.URL, "token")
}

func TestAgentRequestToolWaitsForPeerResponse(t *testing.T) {
	var methods []string
	var captured map[string]any

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
			captured, _ = req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "areq-1",
					"workspace_id": "ws-test",
					"to_agent_id":  "worker-neo",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":    "areq-1",
					"workspace_id":  "ws-test",
					"from_agent_id": "observer",
					"to_agent_id":   "worker-neo",
					"method":        "model.ask",
					"payload":       `{"prompt":"please inspect the task graph"}`,
					"status":        "COMPLETED",
					"response":      `{"summary":"worker checked the task graph"}`,
					"created_at":    "2026-04-17T20:00:00Z",
					"responded_at":  "2026-04-17T20:00:01Z",
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "observer")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id": "worker-neo",
		"prompt":      "please inspect the task graph",
		"timeout_sec": 30,
	})
	if result == nil {
		t.Fatal("expected tool result")
	}
	if result.IsError {
		t.Fatalf("expected success result, got %+v", result)
	}
	if len(methods) < 2 || methods[0] != "agent.request" || methods[1] != "agent.request.result" {
		t.Fatalf("unexpected method flow: %+v", methods)
	}
	if captured["method"] != "model.ask" || captured["to_agent_id"] != "worker-neo" {
		t.Fatalf("unexpected request payload: %+v", captured)
	}
	if !strings.Contains(result.Output, "peer response from worker-neo") || !strings.Contains(result.Output, "worker checked the task graph") {
		t.Fatalf("unexpected tool output: %q", result.Output)
	}
}

func TestAgentRequestToolEncodesDelegatedTaskPayload(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":       "task-impl-1",
				"owner_user_id": "owner",
				"priority":      "normal",
				"status":        "PENDING",
				"task_kind":     "EXECUTION",
				"task_template": "generic",
			}}})
		case "agent.request":
			captured = req.Params
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-delegate",
				"workspace_id": "ws-test",
				"to_agent_id":  "beta",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "beta",
		"request_kind":      "delegate_task",
		"task_id":           "task-impl-1",
		"prompt":            "Please claim task-impl-1 and implement the image pipeline lane.",
		"wait_for_response": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected delegated request to queue, got %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(rpcString(captured, "payload_json")), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["request_kind"] != "delegate_task" || payload["task_id"] != "task-impl-1" {
		t.Fatalf("expected delegated task payload, got %+v", payload)
	}
}

func TestAgentRequestToolAuthorityTransitionRequiresTaskID(t *testing.T) {
	tool := NewAgentRequestTool(NewRhizomeClient("http://example.com", "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "beta",
		"request_kind": "authority_transition",
		"prompt":       "Please approve the scope expansion.",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "authority_transition requires task_id") {
		t.Fatalf("expected authority transition task_id guard, got %+v", result)
	}
}

func TestAgentRequestToolEncodesAuthorityTransitionPayload(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":       "task-role-scope-gamma",
				"title":         "Resolve project role/scope request for gamma",
				"description":   "# Strategic Lead Role/Scope Request",
				"owner_user_id": "gamma",
				"priority":      "high",
				"status":        "PENDING",
				"task_kind":     "COORDINATION",
				"task_template": "generic",
				"project_id":    "project-clearpress",
				"project_lane":  "coordination",
				"tags":          []any{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
			}}})
		case "agent.request":
			captured = req.Params
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-authority",
				"workspace_id": "ws-test",
				"to_agent_id":  "alpha",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "alpha",
		"request_kind":      "authority_transition",
		"task_id":           "task-role-scope-gamma",
		"prompt":            "Please run project_role_assign for task-role-scope-gamma.",
		"wait_for_response": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected authority transition request to queue, got %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(rpcString(captured, "payload_json")), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["request_kind"] != "authority_transition" || payload["task_id"] != "task-role-scope-gamma" {
		t.Fatalf("expected authority transition payload, got %+v", payload)
	}
}

func TestAgentRequestToolDelegateTaskWaitsForClaimEvidence(t *testing.T) {
	var methods []string
	hydrateCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-delegate",
				"workspace_id": "ws-test",
				"to_agent_id":  "beta",
				"status":       "PENDING",
			})
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":    "areq-delegate",
				"workspace_id":  "ws-test",
				"from_agent_id": "alpha",
				"to_agent_id":   "beta",
				"method":        "model.ask",
				"payload":       `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`,
				"status":        "COMPLETED",
				"response":      `Queued runtime_switch_task for delegated task task-impl-1 from alpha.`,
				"created_at":    "2026-04-17T20:00:00Z",
				"responded_at":  "2026-04-17T20:00:01Z",
			})
		case "agent.task.hydrate":
			if got := rpcString(req.Params, "task_id"); got != "task-impl-1" {
				t.Fatalf("hydrate task_id=%q, want task-impl-1", got)
			}
			hydrateCount++
			task := map[string]any{
				"task_id":       "task-impl-1",
				"owner_user_id": "owner",
				"priority":      "normal",
				"status":        "PENDING",
				"task_kind":     "EXECUTION",
				"task_template": "generic",
				"linked_by":     "alpha",
			}
			if hydrateCount > 1 {
				task["status"] = "RUNNING"
				task["claim_agent_id"] = "beta"
				task["claim_status"] = "CLAIMED"
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": task}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "beta",
		"request_kind": "delegate_task",
		"task_id":      "task-impl-1",
		"prompt":       "Please claim task-impl-1 and implement the image pipeline lane.",
		"timeout_sec":  5,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected delegated request with claim evidence to succeed, got %+v", result)
	}
	if !strings.Contains(result.Output, "Delegated task claim confirmed") || !strings.Contains(result.Output, "claim_agent_id=beta") {
		t.Fatalf("expected claim confirmation in output, got %q", result.Output)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.request,agent.request.result,agent.task.hydrate" {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestAgentRequestToolAuthorityTransitionWaitsForClaimEvidence(t *testing.T) {
	var methods []string
	hydrateCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			hydrateCount++
			task := map[string]any{
				"task_id":       "task-role-scope-gamma",
				"title":         "Resolve project role/scope request for gamma",
				"description":   "# Strategic Lead Role/Scope Request\n\nRun project_role_assign.",
				"owner_user_id": "gamma",
				"priority":      "high",
				"status":        "PENDING",
				"task_kind":     "COORDINATION",
				"task_template": "generic",
				"project_id":    "project-clearpress",
				"project_lane":  "coordination",
				"tags":          []any{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
			}
			if hydrateCount > 1 {
				task["status"] = "RUNNING"
				task["claim_agent_id"] = "alpha"
				task["claim_status"] = "CLAIMED"
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": task}})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-authority",
				"workspace_id": "ws-test",
				"to_agent_id":  "alpha",
				"status":       "PENDING",
			})
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":    "areq-authority",
				"workspace_id":  "ws-test",
				"from_agent_id": "gamma",
				"to_agent_id":   "alpha",
				"method":        "model.ask",
				"payload":       `{"request_kind":"authority_transition","task_id":"task-role-scope-gamma","prompt":"Please run project_role_assign."}`,
				"status":        "COMPLETED",
				"response":      `Queued runtime_switch_task for delegated task task-role-scope-gamma from gamma.`,
				"created_at":    "2026-04-17T20:00:00Z",
				"responded_at":  "2026-04-17T20:00:01Z",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "alpha",
		"request_kind": "authority_transition",
		"task_id":      "task-role-scope-gamma",
		"prompt":       "Please run project_role_assign for task-role-scope-gamma.",
		"timeout_sec":  5,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected authority transition request with admission evidence to succeed, got %+v", result)
	}
	if !strings.Contains(result.Output, "Authority transition claim confirmed") ||
		!strings.Contains(result.Output, "project_role_assign applied/denied receipt") {
		t.Fatalf("expected authority transition admission warning in output, got %q", result.Output)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.request,agent.request.result,agent.task.hydrate" {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestAgentRequestToolDelegateProjectTaskRequiresBranchBoundClaim(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request":
			writeRPCResult(w, req, map[string]any{"request_id": "areq-delegate", "workspace_id": "ws-test", "to_agent_id": "beta", "status": "PENDING"})
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":    "areq-delegate",
				"workspace_id":  "ws-test",
				"from_agent_id": "alpha",
				"to_agent_id":   "beta",
				"method":        "model.ask",
				"status":        "COMPLETED",
				"response":      `Queued runtime_switch_task for delegated task task-impl-1 from alpha.`,
			})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":        "task-impl-1",
				"owner_user_id":  "owner",
				"priority":       "normal",
				"status":         "RUNNING",
				"task_kind":      "EXECUTION",
				"task_template":  "generic",
				"project_id":     "project-icons",
				"project_lane":   "implementation",
				"claim_agent_id": "beta",
				"claim_status":   "CLAIMED",
			}}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "beta",
		"request_kind": "delegate_task",
		"task_id":      "task-impl-1",
		"prompt":       "Please claim task-impl-1 and implement the image pipeline lane.",
		"timeout_sec":  5,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected project implementation delegation without branch binding to fail, got %+v", result)
	}
	for _, want := range []string{"not sufficient coverage", "branch_bound", "checkout_bound", "write_scope"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestAgentRequestToolDelegateTaskReportsDelegationBlockerUpdate(t *testing.T) {
	_, err := waitForDelegatedTaskClaimEvidence(context.Background(), newTestAgentRequestHydrationClient(t, map[string]any{
		"bundle": map[string]any{
			"workspace_task": map[string]any{
				"task_id":       "task-impl-1",
				"owner_user_id": "owner",
				"priority":      "normal",
				"status":        "PENDING",
				"task_kind":     "EXECUTION",
				"task_template": "generic",
			},
			"updates": []map[string]any{{
				"update_id":    "upd-delegation-block",
				"agent_id":     "beta",
				"update_type":  "coordination",
				"summary":      "Delegated task task-impl-1 blocked by unresolved dependency task-root.",
				"payload_json": `{"delegation_state":"blocked_dependency","task_id":"task-impl-1","to_agent_id":"beta","work_reason":"task_dependency_blocked","gate_summary":"task task-impl-1 is blocked by unresolved dependency task-root"}`,
			}},
		},
	}), "ws-test", "task-impl-1", "beta", 20*time.Millisecond, false)
	if err == nil {
		t.Fatal("expected delegated blocker update to fail claim wait")
	}
	for _, want := range []string{"delegation_state=blocked_dependency", "task_dependency_blocked", "last_state"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to contain %q, got %v", want, err)
		}
	}
}

func TestAgentRequestToolDelegateTaskReportsProjectClaimOverlapUpdate(t *testing.T) {
	_, err := waitForDelegatedTaskClaimEvidence(context.Background(), newTestAgentRequestHydrationClient(t, map[string]any{
		"bundle": map[string]any{
			"workspace_task": map[string]any{
				"task_id":       "task-impl-1",
				"owner_user_id": "owner",
				"priority":      "normal",
				"status":        "PENDING",
				"task_kind":     "EXECUTION",
				"task_template": "generic",
				"project_id":    "project-ui",
				"project_lane":  "implementation",
			},
			"updates": []map[string]any{{
				"update_id":    "upd-project-overlap",
				"agent_id":     "beta",
				"update_type":  "coordination",
				"summary":      "Deferring project claim for task task-impl-1 because an active overlapping implementation claim already owns this write scope.",
				"payload_json": `{"delegation_state":"delegation_project_claim_overlap","task_id":"task-impl-1","blocked_task_id":"task-impl-1","to_agent_id":"beta","hold_kind":"project_claim_overlap","coverage_state":"covered_by_active_overlapping_claim","conflict_task_id":"task-active","conflict_branch_id":"branch-active","recommended_next_action":"wait for the active overlapping lane"}`,
			}},
		},
	}), "ws-test", "task-impl-1", "beta", 20*time.Millisecond, false)
	if err == nil {
		t.Fatal("expected project claim overlap update to fail claim wait")
	}
	for _, want := range []string{"delegation_project_claim_overlap", "covered_by_active_overlapping_claim", "conflict_task_id=task-active", "last_state"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to contain %q, got %v", want, err)
		}
	}
}

func TestAgentRequestToolDelegateTaskAcceptsActiveLaneResumeCoverage(t *testing.T) {
	summary, err := waitForDelegatedTaskClaimEvidence(context.Background(), newTestAgentRequestHydrationClient(t, map[string]any{
		"bundle": map[string]any{
			"workspace_task": map[string]any{
				"task_id":       "task-clearpress-provenance-publication",
				"owner_user_id": "owner",
				"priority":      "high",
				"status":        "PENDING",
				"task_kind":     "COORDINATION",
				"task_template": "generic",
				"project_id":    "project-clearpress",
				"project_lane":  "coordination",
			},
			"updates": []map[string]any{{
				"update_id":    "upd-active-lane-resume",
				"agent_id":     "gamma",
				"update_type":  "coordination",
				"summary":      "Delegated same-project lane nudge resumed active task task-clearpress-editor for gamma.",
				"payload_json": `{"delegation_state":"active_lane_resume_queued","delegated_task_id":"task-clearpress-provenance-publication","to_agent_id":"gamma","active_task_id":"task-clearpress-editor","active_session_id":"session-clearpress-editor","coverage_state":"active_task_resume_only"}`,
			}},
		},
	}), "ws-test", "task-clearpress-provenance-publication", "gamma", 20*time.Millisecond, false)
	if err != nil {
		t.Fatalf("expected active-lane resume coverage to satisfy delegate wait, got %v", err)
	}
	for _, want := range []string{"active_lane_resume:", "active_task_id=task-clearpress-editor", "active_session_id=session-clearpress-editor", "coverage_state=active_task_resume_only"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("expected summary to contain %q, got %q", want, summary)
		}
	}
}

func TestAgentRequestToolDelegateTaskPreflightSuppressesActiveLaneResumeRetry(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_task": map[string]any{
						"task_id":       "task-clearpress-provenance-publication",
						"owner_user_id": "owner",
						"priority":      "high",
						"status":        "PENDING",
						"task_kind":     "COORDINATION",
						"task_template": "generic",
						"project_id":    "project-clearpress",
						"project_lane":  "coordination",
					},
					"updates": []map[string]any{{
						"update_id":    "upd-active-lane-resume",
						"agent_id":     "gamma",
						"update_type":  "coordination",
						"summary":      "Delegated same-project lane nudge resumed active task task-clearpress-editor for gamma.",
						"payload_json": `{"delegation_state":"active_lane_resume_queued","delegated_task_id":"task-clearpress-provenance-publication","to_agent_id":"gamma","active_task_id":"task-clearpress-editor","active_session_id":"session-clearpress-editor","coverage_state":"active_task_resume_only"}`,
					}},
				},
			})
		case "agent.request":
			t.Fatalf("preflight should suppress retry before creating agent.request")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "gamma",
		"request_kind":      "delegate_task",
		"task_id":           "task-clearpress-provenance-publication",
		"prompt":            "Please claim task-clearpress-provenance-publication and publish lane provenance.",
		"wait_for_response": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected active-lane preflight suppression to succeed, got %+v", result)
	}
	for _, want := range []string{"already routed to active lane", "active_task_id=task-clearpress-editor", "Do not retry the sidecar"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestAgentRequestToolBlocksNoPathSideEffectFoundationDelegation(t *testing.T) {
	var methods []string
	requirements, _ := json.Marshal(map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_foundation",
		"action_kind":                     "split_foundation_bucket",
		"successor_key":                   "abpc-resolution-successor:stale-lua",
		"resolution_saga_key":             "abpc-resolution-saga:stale-lua",
		"decision":                        "split_tension",
		"project_id":                      "project-lua",
		"branch_id":                       "projbranch-old",
		"active_task_id":                  "task-lua-lexer",
		"side_effect_refs":                []string{"side-effect:ws:project-lua:projbranch-old:cmd-glua-main.go"},
		"dirty_paths":                     []string{},
		"path_bucket":                     []string{},
		"write_scope_hints":               []string{},
		"write_scope_hints_authoritative": true,
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_task": map[string]any{
						"task_id":                "task-side-effect-aea7fd1cdc",
						"owner_user_id":          "owner",
						"priority":               "high",
						"status":                 "PENDING",
						"task_kind":              "EXECUTION",
						"task_template":          "generic",
						"project_id":             "project-lua",
						"project_lane":           "implementation",
						"task_requirements_json": string(requirements),
						"write_scope_hints":      []string{},
						"updated_at":             "2026-06-17T01:30:31Z",
					},
				},
			})
		case "agent.request":
			t.Fatalf("no-path side-effect foundation carrier should be blocked before agent.request")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "zeta",
		"request_kind":      "delegate_task",
		"task_id":           "task-side-effect-aea7fd1cdc",
		"prompt":            "Please claim task-side-effect-aea7fd1cdc and resolve the split foundation bucket.",
		"wait_for_response": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected no-path side-effect carrier preflight to block, got %+v", result)
	}
	for _, want := range []string{"no-path ABPC side-effect foundation carrier", "no dirty-path/write-scope identity", "typed terminal blocker"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only preflight hydrate, got %s", got)
	}
}

func TestAgentRequestToolBlocksSideEffectTerminalCarrierDelegation(t *testing.T) {
	const taskID = "task-side-effect-terminal-r18"
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedSideEffectSuccessorHydrationBundle(taskID, "project-lua", "zeta", "BLOCKED")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["claim_summary"] = "typed terminal blocker: runtime_switch_terminal_blocker.v1 carrier_kind=side_effect_resolution_successor task_id=" + taskID + " blocker_kind=no_fresh_claimable_side_effect_successor_path"
			}
			writeRPCResult(w, req, bundle)
		case "agent.request":
			t.Fatalf("terminal side-effect carrier should be blocked before agent.request")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "zeta",
		"request_kind":      "delegate_task",
		"task_id":           taskID,
		"prompt":            "Please claim " + taskID + " and continue the side-effect successor.",
		"wait_for_response": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected terminal side-effect carrier preflight to block, got %+v", result)
	}
	for _, want := range []string{"terminal blocker", "runtime switch carrier task " + taskID, "side_effect_resolution_successor", "do not send another runtime_switch_task wake"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only preflight hydrate, got %s", got)
	}
}

func TestAgentRequestToolDelegateTaskPreflightBlocksRecentProjectClaimOverlap(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_task": map[string]any{
						"task_id":       "task-impl-1",
						"owner_user_id": "owner",
						"priority":      "normal",
						"status":        "PENDING",
						"task_kind":     "EXECUTION",
						"task_template": "generic",
						"project_id":    "project-ui",
						"project_lane":  "implementation",
					},
					"updates": []map[string]any{{
						"update_id":    "upd-legacy-overlap",
						"agent_id":     "gamma",
						"update_type":  "coordination",
						"summary":      "Deferring project claim for task task-impl-1 because an active overlapping implementation claim already owns this write scope.",
						"payload_json": `{"project_id":"project-ui","task_id":"task-impl-1","hold_kind":"project_claim_overlap","coordination_mode":"trust_first"}`,
					}},
				},
			})
		case "agent.request":
			t.Fatalf("preflight should block before creating agent.request")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "beta",
		"request_kind":      "delegate_task",
		"task_id":           "task-impl-1",
		"prompt":            "Please claim task-impl-1.",
		"wait_for_response": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected project overlap preflight to block, got %+v", result)
	}
	for _, want := range []string{"recent project-claim overlap", "project_claim_overlap", "Do not retry"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestAgentRequestToolDelegateTaskPreflightIgnoresExpiredProjectClaimOverlap(t *testing.T) {
	var methods []string
	expiredAt := time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_task": map[string]any{
						"task_id":       "task-impl-1",
						"owner_user_id": "owner",
						"priority":      "normal",
						"status":        "PENDING",
						"task_kind":     "EXECUTION",
						"task_template": "generic",
						"project_id":    "project-ui",
						"project_lane":  "implementation",
					},
					"updates": []map[string]any{{
						"update_id":    "upd-expired-overlap",
						"agent_id":     "gamma",
						"update_type":  "coordination",
						"summary":      "Deferring project claim for task task-impl-1 because an active overlapping implementation claim already owns this write scope.",
						"created_at":   time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano),
						"payload_json": `{"project_id":"project-ui","task_id":"task-impl-1","hold_kind":"project_claim_overlap","coordination_mode":"trust_first","expires_at":"` + expiredAt + `"}`,
					}},
				},
			})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-delegate",
				"workspace_id": "ws-test",
				"to_agent_id":  "beta",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "beta",
		"request_kind":      "delegate_task",
		"task_id":           "task-impl-1",
		"prompt":            "Please claim task-impl-1.",
		"wait_for_response": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected expired overlap to allow request queueing, got %+v", result)
	}
	if !strings.Contains(result.Output, "peer request queued") {
		t.Fatalf("unexpected output: %q", result.Output)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.request" {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestDelegatedTaskProjectOverlapLegacyUpdateExpiresByCreatedAt(t *testing.T) {
	update := AgentUpdateRecord{
		UpdateID:  "upd-legacy-overlap",
		AgentID:   "gamma",
		CreatedAt: time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano),
	}
	payload := map[string]any{
		"task_id":   "task-impl-1",
		"hold_kind": "project_claim_overlap",
	}
	if delegatedTaskProjectClaimOverlapCurrent(update, payload, time.Now().UTC()) {
		t.Fatal("expected old legacy project overlap update to expire by created_at")
	}
	update.CreatedAt = time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	if !delegatedTaskProjectClaimOverlapCurrent(update, payload, time.Now().UTC()) {
		t.Fatal("expected fresh legacy project overlap update to remain current")
	}
}

func TestAgentRequestToolDelegateTaskAcceptsFastTerminalClaimEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-delegate",
				"workspace_id": "ws-test",
				"to_agent_id":  "beta",
				"status":       "PENDING",
			})
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":    "areq-delegate",
				"workspace_id":  "ws-test",
				"from_agent_id": "alpha",
				"to_agent_id":   "beta",
				"method":        "model.ask",
				"payload":       `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`,
				"status":        "COMPLETED",
				"response":      `Queued runtime_switch_task for delegated task task-impl-1 from alpha.`,
				"created_at":    "2026-04-17T20:00:00Z",
				"responded_at":  "2026-04-17T20:00:01Z",
			})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_task": map[string]any{
						"task_id":        "task-impl-1",
						"owner_user_id":  "owner",
						"priority":       "normal",
						"status":         "DONE",
						"task_kind":      "EXECUTION",
						"task_template":  "generic",
						"linked_by":      "alpha",
						"claim_agent_id": "beta",
						"claim_status":   "COMPLETED",
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "beta",
		"request_kind": "delegate_task",
		"task_id":      "task-impl-1",
		"prompt":       "Please claim task-impl-1 and implement the image pipeline lane.",
		"timeout_sec":  5,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected fast terminal ownership evidence to satisfy delegated claim wait, got %+v", result)
	}
	if !strings.Contains(result.Output, "claim_status=COMPLETED") || !strings.Contains(result.Output, "task_status=DONE") {
		t.Fatalf("expected terminal claim evidence in output, got %q", result.Output)
	}
}

func TestAgentRequestToolDelegateTaskRequiresTaskID(t *testing.T) {
	tool := NewAgentRequestTool(NewRhizomeClient("http://example.com", "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "beta",
		"request_kind": "delegate_task",
		"prompt":       "Please take the implementation lane.",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "requires task_id") {
		t.Fatalf("expected missing task_id rejection, got %+v", result)
	}
}

func TestAgentRequestToolRejectsReviewThatRequiresWorkLoop(t *testing.T) {
	serverCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalled = true
		t.Fatalf("work-loop review should be rejected before RPC")
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "beta")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "theta",
		"request_kind": "review",
		"task_id":      "task-visual-qa",
		"prompt":       "Please run browser smoke/visual QA, start the app on a high port, capture screenshot refs, and publish evidence.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected read-only work-loop request rejection, got %+v", result)
	}
	for _, want := range []string{"model.ask is read-only", "task_submit", "delegate_task"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejection to contain %q, got %q", want, result.Output)
		}
	}
	if serverCalled {
		t.Fatal("server should not have been called")
	}
}

func TestAgentRequestRequiresWorkLoopForEnglishScreenshotActions(t *testing.T) {
	for _, prompt := range []string{
		"Please take a screenshot of the app.",
		"Take screenshot evidence from the browser.",
		"Take an app screenshot after launch.",
		"Grab a screenshot of the app.",
		"Capture screenshot evidence from the result page.",
		"Take the screenshot after launch.",
		"Take app screenshot evidence.",
		"Snap a screenshot of the app.",
		"Take a screen shot of the app.",
		"Snap the screenshot of the app.",
		"Grab an app screenshot.",
		"Please screenshot the app.",
		"Can you screenshot the app?",
		"Can you take another screenshot?",
	} {
		if !agentRequestRequiresWorkLoop(prompt) {
			t.Fatalf("expected screenshot action to require a work loop: %q", prompt)
		}
	}
	if agentRequestRequiresWorkLoop("Please take a look at the screenshot.") {
		t.Fatal("a request to inspect an existing screenshot must not imply an execution work loop")
	}
	if agentRequestRequiresWorkLoop("Please do not take a screenshot; review the existing image.") {
		t.Fatal("an explicitly negated screenshot action must not imply an execution work loop")
	}
	if agentRequestRequiresWorkLoop("Please don't take a screenshot; review the existing image.") {
		t.Fatal("a contracted screenshot negation must not imply an execution work loop")
	}
}

func TestAgentRequestToolRejectsDelegatingCurrentAgentsClaim(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":        "task-visual-qa",
				"owner_user_id":  "owner",
				"priority":       "high",
				"status":         "RUNNING",
				"task_kind":      "EXECUTION",
				"task_template":  "generic",
				"claim_agent_id": "beta",
				"claim_status":   "CLAIMED",
			}}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "beta")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "theta",
		"request_kind": "delegate_task",
		"task_id":      "task-visual-qa",
		"prompt":       "Please claim task-visual-qa and run browser smoke evidence.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected self-claim delegation rejection, got %+v", result)
	}
	for _, want := range []string{"already claimed by the current agent", "task_submit", "finish or block"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejection to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only preflight hydrate, got %s", got)
	}
}

func TestAgentRequestToolRejectsAuthorityTransitionForCurrentAgentsClaim(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":        "task-role-scope-beta",
				"owner_user_id":  "owner",
				"priority":       "high",
				"status":         "RUNNING",
				"task_kind":      "COORDINATION",
				"task_template":  "generic",
				"project_id":     "project-rq",
				"project_lane":   "coordination",
				"claim_agent_id": "beta",
				"claim_status":   "CLAIMED",
				"tags":           []any{"project-role-scope", "strategic-lead", "coordination"},
			}}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "beta")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "alpha",
		"request_kind": "authority_transition",
		"task_id":      "task-role-scope-beta",
		"prompt":       "Please perform the lead-level authority transition for task-role-scope-beta.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected self-claimed authority transition rejection, got %+v", result)
	}
	for _, want := range []string{"authority_transition blocked", "already claimed by the current agent", "dedicated lead-owned authority task", "project_role_assign"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejection to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only preflight hydrate, got %s", got)
	}
}

func TestAgentRequestToolRejectsBlockedAuthorityTransitionCarrierAsTerminalBlocker(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":        "task-role-scope-beta",
				"owner_user_id":  "owner",
				"priority":       "high",
				"status":         "RUNNING",
				"task_kind":      "COORDINATION",
				"task_template":  "generic",
				"project_id":     "project-rq",
				"project_lane":   "coordination",
				"claim_agent_id": "alpha",
				"claim_status":   "BLOCKED",
				"claim_summary":  "typed terminal blocker: no authority-bearing admission path is available",
				"tags":           []any{"project-role-scope", "strategic-lead", "coordination"},
			}}})
		default:
			t.Fatalf("blocked authority carrier must not send agent.request; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "alpha",
		"request_kind": "authority_transition",
		"task_id":      "task-role-scope-beta",
		"prompt":       "Please perform the lead-level authority transition for task-role-scope-beta.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected blocked authority carrier rejection, got %+v", result)
	}
	for _, want := range []string{"authority_transition blocked", "terminal blocker", "claim_status=BLOCKED", "do not send another authority_transition wake"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejection to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only preflight hydrate, got %s", got)
	}
}

func TestAgentRequestToolAllowsExpiredPeerClaimedAuthorityTransitionCarrier(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":          "task-role-scope-beta",
				"owner_user_id":    "owner",
				"priority":         "high",
				"status":           "RUNNING",
				"task_kind":        "COORDINATION",
				"task_template":    "generic",
				"project_id":       "project-rq",
				"project_lane":     "coordination",
				"claim_agent_id":   "beta",
				"claim_status":     "CLAIMED",
				"claim_expires_at": "2000-01-01T00:00:00Z",
				"tags":             []any{"project-role-scope", "strategic-lead", "coordination"},
			}}})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-authority-expired",
				"workspace_id": "ws-test",
				"to_agent_id":  "alpha",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":        "alpha",
		"request_kind":       "authority_transition",
		"task_id":            "task-role-scope-beta",
		"prompt":             "Please perform the lead-level authority transition for task-role-scope-beta.",
		"wait_for_response":  false,
		"wait_for_claim":     false,
		"authority_carrier":  true,
		"transition_surface": "project_role_assign",
	})
	if result == nil || result.IsError {
		t.Fatalf("expired peer-claimed authority carrier should be queued, got %+v", result)
	}
	if !strings.Contains(result.Output, "peer request queued") || !strings.Contains(result.Output, "authority_transition is not covered") {
		t.Fatalf("expected queued authority transition output, got %q", result.Output)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.request" {
		t.Fatalf("expected preflight then request, got %s", got)
	}
}

func TestAgentRequestToolRejectsUnexpiredPeerClaimedAuthorityTransitionCarrier(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":          "task-role-scope-beta",
				"owner_user_id":    "owner",
				"priority":         "high",
				"status":           "RUNNING",
				"task_kind":        "COORDINATION",
				"task_template":    "generic",
				"project_id":       "project-rq",
				"project_lane":     "coordination",
				"claim_agent_id":   "beta",
				"claim_status":     "CLAIMED",
				"claim_expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
				"tags":             []any{"project-role-scope", "strategic-lead", "coordination"},
			}}})
		case "agent.request":
			t.Fatalf("unexpired peer-claimed authority carrier should be blocked before agent.request")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "alpha",
		"request_kind": "authority_transition",
		"task_id":      "task-role-scope-beta",
		"prompt":       "Please perform the lead-level authority transition for task-role-scope-beta.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected unexpired peer-claimed authority transition rejection, got %+v", result)
	}
	for _, want := range []string{"authority_transition blocked", "already claimed by beta", "not alpha"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejection to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only preflight hydrate, got %s", got)
	}
}

func TestAgentRequestToolRejectsAuthorityTransitionOnProductTaskCarrier(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":       "task-eval-impl",
				"owner_user_id": "owner",
				"priority":      "high",
				"status":        "PENDING",
				"task_kind":     "EXECUTION",
				"task_template": "generic",
				"project_id":    "project-rq",
				"project_lane":  "implementation",
			}}})
		case "agent.request":
			t.Fatalf("authority_transition with product task carrier should be rejected before agent.request")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "eta")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":  "alpha",
		"request_kind": "authority_transition",
		"task_id":      "task-eval-impl",
		"prompt":       "Please perform the required authority transition for task-eval-impl.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected authority product-task carrier rejection, got %+v", result)
	}
	for _, want := range []string{"authority_transition blocked", "not a dedicated authority transition task", "task-role-scope-*"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejection to contain %q, got %q", want, result.Output)
		}
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only preflight hydrate, got %s", got)
	}
}

func TestAgentRequestToolAllowsDelegatingReleasedPriorClaim(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":        "task-visual-qa",
				"owner_user_id":  "owner",
				"priority":       "high",
				"status":         "PENDING",
				"task_kind":      "EXECUTION",
				"task_template":  "generic",
				"claim_agent_id": "beta",
				"claim_status":   "RELEASED",
			}}})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-released-prior",
				"workspace_id": "ws-test",
				"to_agent_id":  "theta",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "beta")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "theta",
		"request_kind":      "delegate_task",
		"task_id":           "task-visual-qa",
		"prompt":            "Please claim task-visual-qa and run browser smoke evidence.",
		"wait_for_response": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected released prior claim to allow delegate request queueing, got %+v", result)
	}
	if !strings.Contains(result.Output, "peer request queued") {
		t.Fatalf("unexpected output: %q", result.Output)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.request" {
		t.Fatalf("expected hydrate then request, got %s", got)
	}
}

func TestAgentRequestToolDoesNotPreflightReleasedTargetWithSenderIdentity(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":        "task-visual-qa",
				"owner_user_id":  "owner",
				"priority":       "high",
				"status":         "PENDING",
				"task_kind":      "EXECUTION",
				"task_template":  "generic",
				"project_id":     "project-clearpress",
				"project_lane":   "validation",
				"claim_agent_id": "theta",
				"claim_status":   "RELEASED",
				"title":          "Clearpress exact-head visual acceptance",
				"description":    "Run browser smoke and visual acceptance evidence for patch queue candidate.",
			}}})
		case "agent.work.next":
			t.Fatalf("sender-side released-task delegation must not call target-scoped agent.work.next with the sender token: %+v", req.Params)
		case "agent.request":
			if rpcString(req.Params, "from_agent_id") != "beta" || rpcString(req.Params, "to_agent_id") != "theta" {
				t.Fatalf("unexpected request actors: %+v", req.Params)
			}
			payload := rpcString(req.Params, "payload_json")
			if !strings.Contains(payload, `"request_kind":"delegate_task"`) || !strings.Contains(payload, `"task_id":"task-visual-qa"`) {
				t.Fatalf("delegated request payload did not preserve executable wake intent: %s", payload)
			}
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-released-target",
				"workspace_id": "ws-test",
				"to_agent_id":  "theta",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "beta")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "theta",
		"request_kind":      "delegate_task",
		"task_id":           "task-visual-qa",
		"prompt":            "Please claim task-visual-qa and publish exact browser visual evidence.",
		"wait_for_response": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected released target delegation to queue for target-owned admission, got %+v", result)
	}
	if !strings.Contains(result.Output, "peer request queued") || !strings.Contains(result.Output, "delegate_task is not covered") {
		t.Fatalf("unexpected output: %q", result.Output)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.request" {
		t.Fatalf("expected hydrate then request, got %s", got)
	}
}

func TestAgentRequestToolWaitForClaimIgnoresReleasedPriorHolder(t *testing.T) {
	_, err := waitForDelegatedTaskClaimEvidence(context.Background(), newTestAgentRequestHydrationClient(t, map[string]any{
		"bundle": map[string]any{"workspace_task": map[string]any{
			"task_id":        "task-visual-qa",
			"owner_user_id":  "owner",
			"priority":       "high",
			"status":         "PENDING",
			"task_kind":      "EXECUTION",
			"task_template":  "generic",
			"claim_agent_id": "beta",
			"claim_status":   "RELEASED",
		}},
	}), "ws-test", "task-visual-qa", "theta", 20*time.Millisecond, false)
	if err == nil {
		t.Fatal("expected timeout while waiting for theta claim")
	}
	if strings.Contains(err.Error(), "claimed by beta instead of theta") {
		t.Fatalf("released prior holder must not be treated as active conflicting claim: %v", err)
	}
	if !strings.Contains(err.Error(), "timed out waiting for claim_admitted") {
		t.Fatalf("expected wait timeout, got %v", err)
	}
}

func TestAgentRequestToolRejectsUnresolvedTaskPlaceholder(t *testing.T) {
	tool := NewAgentRequestTool(NewRhizomeClient("http://example.com", "token"), "ws-test", "alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "beta",
		"request_kind":      "delegate_task",
		"task_id":           "__FROM_PREVIOUS_task_submit_RESULT__",
		"prompt":            "Please claim __FROM_PREVIOUS_task_submit_RESULT__ and build it.",
		"wait_for_response": false,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "unresolved tool-result placeholder") {
		t.Fatalf("expected unresolved placeholder rejection, got %+v", result)
	}
}

func TestAgentRequestToolRejectsSelfTarget(t *testing.T) {
	tool := NewAgentRequestTool(NewRhizomeClient("http://example.com", "token"), "ws-test", "observer")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id": "observer",
		"prompt":      "do the thing",
	})
	if result == nil {
		t.Fatal("expected tool result")
	}
	if !result.IsError || !strings.Contains(result.Output, "cannot target the current agent") {
		t.Fatalf("expected self-target rejection, got %+v", result)
	}
}

func TestAgentRequestToolRejectsPrivateLocalArtifactWithoutSharedContext(t *testing.T) {
	serverCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalled = true
		t.Fatalf("agent_request should block private local artifact prompts before RPC")
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "observer")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id": "reviewer",
		"prompt":      "Please review subpixel-art-lab/index.html and verify the dashboard.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected private local artifact request to be rejected, got %+v", result)
	}
	for _, want := range []string{"private local file path", "Peer agents cannot read your workdir", "workspace_doc_put"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejection to contain %q, got %q", want, result.Output)
		}
	}
	if serverCalled {
		t.Fatal("server should not have been called")
	}
}

func TestAgentRequestArtifactGuardHandlesWeakContextAndPOSIXPaths(t *testing.T) {
	for _, prompt := range []string{
		"Please review subpixel-art-lab/index.html. content: check rendering.",
		"Please review subpixel-art-lab/index.html.\n```",
		"Please review subpixel-art-lab/index.html.\n```\n \n```",
		"Please review /tmp/build/subpixel-art-lab/index.html and verify it opens.",
		"Please inspect /home/agent/work/output.go for correctness.",
	} {
		if !agentRequestNeedsSharedArtifact(prompt) {
			t.Fatalf("expected prompt to need shared artifact context: %q", prompt)
		}
	}
	for _, prompt := range []string{
		"Please review https://example.com/subpixel-art-lab/index.html.",
		"Please review subpixel-art-lab/index.html. doc_key=task.demo.result contains the shared artifact.",
		"Please review subpixel-art-lab/index.html. file content: " + strings.Repeat("x", 140),
		"Please review subpixel-art-lab/index.html.\n```\n" + strings.Repeat("x", 100) + "\n```",
	} {
		if agentRequestNeedsSharedArtifact(prompt) {
			t.Fatalf("expected prompt to have adequate shared artifact context: %q", prompt)
		}
	}
}

func TestAgentRequestToolAllowsLocalArtifactWhenSharedContextIsIncluded(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.request" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		captured = req.Params
		writeRPCResult(w, req, map[string]any{
			"request_id":   "areq-shared",
			"workspace_id": "ws-test",
			"to_agent_id":  "reviewer",
			"status":       "PENDING",
		})
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "observer")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id":       "reviewer",
		"prompt":            "Please review subpixel-art-lab/index.html. doc_key=task.demo.result contains the shared artifact.",
		"wait_for_response": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected shared artifact request to be queued, got %+v", result)
	}
	if captured == nil || captured["to_agent_id"] != "reviewer" {
		t.Fatalf("expected request to reach RPC, captured=%+v", captured)
	}
	if !strings.Contains(result.Output, "peer request queued") {
		t.Fatalf("unexpected result output: %q", result.Output)
	}
}

func TestAgentRequestToolSurfacesFailedPeerResult(t *testing.T) {
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
					"request_id":   "areq-2",
					"workspace_id": "ws-test",
					"to_agent_id":  "synthesizer",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":    "areq-2",
					"workspace_id":  "ws-test",
					"from_agent_id": "observer",
					"to_agent_id":   "synthesizer",
					"method":        "model.ask",
					"status":        "FAILED",
					"response":      `{"error":"peer runtime unavailable"}`,
					"created_at":    "2026-04-17T20:00:00Z",
					"responded_at":  "2026-04-17T20:00:01Z",
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	tool := NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-test", "observer")
	result := tool.Execute(context.Background(), map[string]any{
		"to_agent_id": "synthesizer",
		"prompt":      "draft a decomposition plan",
	})
	if result == nil {
		t.Fatal("expected tool result")
	}
	if !result.IsError || !strings.Contains(result.Output, "FAILED") || !strings.Contains(result.Output, "peer runtime unavailable") {
		t.Fatalf("expected failed peer result to surface clearly, got %+v", result)
	}
}
