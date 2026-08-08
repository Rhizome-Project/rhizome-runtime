package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectRoleAssignToolAssignsImplementerScope(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase: "SPEC",
				LeadAgentID:  "agent-alpha",
			}))
			return
		case 2:
			if req.Method != "project.role.assign" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "actor_id"); got != "agent-alpha" {
				t.Fatalf("actor_id = %q", got)
			}
			if got := rpcString(req.Params, "agent_id"); got != "agent-beta" {
				t.Fatalf("agent_id = %q", got)
			}
			if got := rpcString(req.Params, "role_type"); got != "IMPLEMENTER" {
				t.Fatalf("role_type = %q", got)
			}
			if got := rpcString(req.Params, "write_scope_json"); got != `{"paths":["web/**"]}` {
				t.Fatalf("write_scope_json = %q", got)
			}
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, map[string]any{"role": map[string]any{
			"role_id":          "role-beta",
			"workspace_id":     "ws",
			"project_id":       "project-demo",
			"agent_id":         "agent-beta",
			"role_type":        "IMPLEMENTER",
			"status":           "ACTIVE",
			"write_scope_json": `{"paths":["web/**"]}`,
			"claimed_at":       "2026-05-04T00:00:00Z",
			"created_at":       "2026-05-04T00:00:00Z",
			"updated_at":       "2026-05-04T00:00:00Z",
		}})
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-beta",
		"role_type":        "implementer",
		"write_scope_json": `{"paths":["web/**"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful role assignment, got %+v", result)
	}
	if !strings.Contains(result.Output, `"role_id": "role-beta"`) || !strings.Contains(result.Output, `"agent_id": "agent-beta"`) {
		t.Fatalf("unexpected output %q", result.Output)
	}
	if calls != 2 {
		t.Fatalf("expected two RPC calls, got %d", calls)
	}
}

func TestProjectRoleAssignToolSurfacesSkippedActiveClaimRebind(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase: "IMPLEMENTATION",
				LeadAgentID:  "agent-alpha",
			}))
		case 2:
			if req.Method != "project.role.assign" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-beta",
					"workspace_id":     "ws",
					"project_id":       "project-demo",
					"agent_id":         "agent-beta",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": `{"paths":["src/editor/**"]}`,
					"claimed_at":       "2026-05-04T00:00:00Z",
					"created_at":       "2026-05-04T00:00:00Z",
					"updated_at":       "2026-05-04T00:00:00Z",
				},
				"active_claim_rebind": map[string]any{
					"state":     "skipped_missing_branch",
					"task_id":   "task-impl",
					"branch_id": "branch-stale",
				},
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": `{"paths":["src/editor/**"]}`,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected skipped rebind to be surfaced as unresolved repair, got %+v", result)
	}
	if !strings.Contains(result.Output, `"active_claim_rebind"`) || !strings.Contains(result.Output, "repair as unresolved") {
		t.Fatalf("expected active_claim_rebind guidance in output, got %q", result.Output)
	}
}

func TestProjectRoleAssignToolBlocksReadyForReviewBranchRebindBeforeRoleMutation(t *testing.T) {
	calls := 0
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
					"branches": []map[string]any{{
						"branch_id":        "branch-beta-ready",
						"project_id":       "project-demo",
						"agent_id":         "agent-beta",
						"active_task_id":   "task-editor",
						"branch_name":      "beta/task-editor",
						"status":           "READY_FOR_REVIEW",
						"write_scope_json": `{"paths":["src/editor/**","tests/editor/**"]}`,
					}},
				},
			})
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected denial update before any role mutation, got %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": `{"paths":["src/**","tests/**","package.json","package-lock.json"]}`,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected ready-for-review rebind denial, got %+v", result)
	}
	for _, want := range []string{"ready_for_review_branch_rebind_blocked", "project_patch_queue_followup"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
		if !strings.Contains(updatePayload, want) {
			t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
		}
	}
	if !strings.Contains(result.Output, `"denial_recorded": true`) || !strings.Contains(updatePayload, `"denial_recorded":true`) {
		t.Fatalf("expected denial_recorded in output/update, output=%s update=%s", result.Output, updatePayload)
	}
	if calls != 2 {
		t.Fatalf("expected coordination read + denial update only, got %d calls", calls)
	}
}

func TestProjectRoleAssignToolBlocksReadyForReviewBranchWhenClaimScopeIsNarrower(t *testing.T) {
	calls := 0
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
					"branches": []map[string]any{{
						"branch_id":        "branch-beta-ready",
						"project_id":       "project-demo",
						"agent_id":         "agent-beta",
						"active_task_id":   "task-editor",
						"branch_name":      "beta/task-editor",
						"status":           "READY_FOR_REVIEW",
						"write_scope_json": `{"paths":["src/**","tests/**"]}`,
					}},
					"tasks": []map[string]any{{
						"task_id":                "task-editor",
						"status":                 "RUNNING",
						"claim_agent_id":         "agent-beta",
						"claim_status":           "BLOCKED",
						"claim_branch_id":        "branch-beta-ready",
						"claim_write_scope_json": `{"paths":["src/App.*"]}`,
					}},
				},
			})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("expected applied receipt lookup before denial, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-beta-ready",
					"claim_write_scope_json": `{"paths":["src/App.*"]}`,
				},
			}})
		case 3:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected stale-claim denial update before role mutation, got %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-demo",
		"agent_id":                "agent-beta",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        `{"paths":["src/**","tests/**"]}`,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta-ready",
		"boundary_transition_key": "abpc-boundary-transition:ready-stale-claim",
		"side_effect_refs":        []string{"side-effect:src-test"},
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected ready branch stale-claim rebind denial, got %+v", result)
	}
	for _, want := range []string{"ready_for_review_branch_rebind_blocked", "project_patch_queue_followup", "abpc-boundary-transition:ready-stale-claim"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
		if !strings.Contains(updatePayload, want) {
			t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
		}
	}
	if calls != 3 {
		t.Fatalf("expected coordination read + applied receipt lookup + denial update only, got %d calls", calls)
	}
}

func TestProjectRoleAssignToolRequiresImplementerWriteScope(t *testing.T) {
	tool := NewProjectRoleAssignTool(NewRhizomeClient("http://127.0.0.1:1", "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"agent_id":   "agent-beta",
		"role_type":  "IMPLEMENTER",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "write_scope_json") {
		t.Fatalf("expected write_scope_json error, got %+v", result)
	}
}

func TestProjectRoleAssignToolDelegatesNonLeadRoleChangeToStrategicLead(t *testing.T) {
	calls := 0
	var taskParams map[string]any
	var requestParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase: "IMPLEMENTATION",
				LeadAgentID:  "agent-alpha",
			}))
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			taskParams = req.Params
			if got := rpcString(req.Params, "project_id"); got != "project-demo" {
				t.Fatalf("task project_id = %q", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "coordination" {
				t.Fatalf("task project_lane = %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "agent.request" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			requestParams = req.Params
			if got := rpcString(req.Params, "to_agent_id"); got != "agent-alpha" {
				t.Fatalf("to_agent_id = %q", got)
			}
			if got := rpcString(req.Params, "method"); got != "model.ask" {
				t.Fatalf("method = %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-lead-role",
				"workspace_id": "ws",
				"to_agent_id":  "agent-alpha",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": `{"paths":["src/pipeline/**","tests/**"]}`,
		"summary":          "beta is blocked by a role/task mismatch and needs the algorithm scope",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected non-lead request to be converted into lead coordination, got %+v", result)
	}
	for _, want := range []string{`"delegated_to_lead": false`, `"lead_notified": true`, `"do_not_retry": true`, `"lead_agent_id": "agent-alpha"`, `"lead_request_id": "areq-lead-role"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if taskParams == nil || requestParams == nil {
		t.Fatalf("expected task submit and lead request params")
	}
	taskID := rpcString(taskParams, "task_id")
	if taskID == "" {
		t.Fatalf("expected deterministic lead task id in params: %+v", taskParams)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(rpcString(requestParams, "payload_json")), &payload); err != nil {
		t.Fatalf("unmarshal request payload: %v", err)
	}
	if payload["request_kind"] != "authority_transition" || payload["lead_task_id"] != taskID || payload["task_id"] != taskID {
		t.Fatalf("expected authority transition lead notice payload for task %s, got %+v", taskID, payload)
	}
	if calls != 3 {
		t.Fatalf("expected three RPC calls, got %d", calls)
	}
}

func TestProjectRoleAssignToolNonLeadNoopsWhenRequestedRoleAlreadyActive(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectRoleAssignCoordinationResultWithRoles("agent-alpha", []map[string]any{{
			"role_id":          "role-gamma",
			"workspace_id":     "ws",
			"project_id":       "project-demo",
			"agent_id":         "agent-gamma",
			"role_type":        "IMPLEMENTER",
			"status":           "ACTIVE",
			"write_scope_json": `{"paths":["src/**","tests/**","package.json"]}`,
			"created_at":       "2026-05-04T00:00:00Z",
			"updated_at":       "2026-05-04T00:00:00Z",
		}}))
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-gamma",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": `{"paths":["src/preview/**","tests/preview/**"]}`,
		"summary":          "same role request after stale local memory",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected idempotent already-satisfied result, got %+v", result)
	}
	for _, want := range []string{`"already_satisfied": true`, `"do_not_retry": true`, `"role_id": "role-gamma"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup, got %d calls", calls)
	}
}

func TestProjectRoleAssignToolNonLeadQueuesBoundaryRequestWhenRoleCoversButBindingDoesNot(t *testing.T) {
	calls := 0
	listCalls := 0
	submitCalls := 0
	requestCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			result := projectRoleAssignCoordinationResultWithRoles("agent-alpha", []map[string]any{{
				"role_id":          "role-gamma",
				"workspace_id":     "ws",
				"project_id":       "project-demo",
				"agent_id":         "agent-gamma",
				"role_type":        "IMPLEMENTER",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
				"created_at":       "2026-05-04T00:00:00Z",
				"updated_at":       "2026-05-04T00:00:00Z",
			}})
			coordination := result["coordination"].(map[string]any)
			coordination["branches"] = []any{map[string]any{
				"branch_id":        "branch-gamma",
				"agent_id":         "agent-gamma",
				"active_task_id":   "task-cli",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**"]}`,
			}}
			writeRPCResult(w, req, result)
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-cli",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-gamma",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-gamma",
					"claim_write_scope_json": `{"paths":["cmd/**"]}`,
				},
			}})
		case "task.submit":
			submitCalls++
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements["boundary_transition_key"] == "" || requirements["active_task_id"] != "task-cli" || requirements["branch_id"] != "branch-gamma" {
				t.Fatalf("expected boundary transition metadata in lead task requirements, got %+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			requestCalls++
			writeRPCResult(w, req, map[string]any{
				"request_id": "areq-boundary",
				"status":     "PENDING",
			})
		default:
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-demo",
		"agent_id":                "agent-gamma",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        `{"paths":["cmd/**","go.mod","README.md"]}`,
		"side_effect_refs":        []string{"side-effect:go-mod"},
		"active_task_id":          "task-cli",
		"branch_id":               "branch-gamma",
		"boundary_transition_key": "abpc-boundary-transition:go-mod",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected non-lead boundary request to queue for lead, got %+v", result)
	}
	for _, want := range []string{`"lead_task_id"`, `"do_not_retry": true`, `"lead_request_id": "areq-boundary"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"already_satisfied": true`) {
		t.Fatalf("branch-bound side effect must not collapse to generic already_satisfied without binding evidence: %s", result.Output)
	}
	if listCalls < 1 || submitCalls != 1 || requestCalls != 1 {
		t.Fatalf("expected binding check then lead queue, list=%d submit=%d request=%d calls=%d", listCalls, submitCalls, requestCalls, calls)
	}
}

func TestProjectRoleAssignToolNonLeadReturnsAlreadyAppliedBoundaryReceiptWhenBindingCovers(t *testing.T) {
	calls := 0
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			result := projectRoleAssignCoordinationResultWithRoles("agent-alpha", []map[string]any{{
				"role_id":          "role-gamma",
				"workspace_id":     "ws",
				"project_id":       "project-demo",
				"agent_id":         "agent-gamma",
				"role_type":        "IMPLEMENTER",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
				"created_at":       "2026-05-04T00:00:00Z",
				"updated_at":       "2026-05-04T00:00:00Z",
			}})
			coordination := result["coordination"].(map[string]any)
			coordination["branches"] = []any{map[string]any{
				"branch_id":        "branch-gamma",
				"agent_id":         "agent-gamma",
				"active_task_id":   "task-cli",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
			}}
			writeRPCResult(w, req, result)
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-cli",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-gamma",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-gamma",
					"claim_write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
				},
			}})
		default:
			t.Fatalf("already-applied boundary receipt must not queue or mutate; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-demo",
		"agent_id":                "agent-gamma",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        `{"paths":["cmd/**","go.mod","README.md"]}`,
		"side_effect_refs":        []string{"side-effect:go-mod"},
		"active_task_id":          "task-cli",
		"branch_id":               "branch-gamma",
		"boundary_transition_key": "abpc-boundary-transition:go-mod",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected already-applied boundary receipt, got %+v", result)
	}
	for _, want := range []string{`"already_satisfied": true`, `"active_claim_rebind"`, `"already_applied"`, `"role_scope_already_satisfied"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 2 || listCalls != 1 {
		t.Fatalf("expected coordination and binding task lookup only, calls=%d lists=%d", calls, listCalls)
	}
}

func TestProjectRoleAssignToolNonLeadDoesNotTreatReleasedClaimAsLiveBoundaryBinding(t *testing.T) {
	calls := 0
	listCalls := 0
	submitCalls := 0
	requestCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			result := projectRoleAssignCoordinationResultWithRoles("agent-alpha", []map[string]any{{
				"role_id":          "role-gamma",
				"workspace_id":     "ws",
				"project_id":       "project-demo",
				"agent_id":         "agent-gamma",
				"role_type":        "IMPLEMENTER",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
				"created_at":       "2026-05-04T00:00:00Z",
				"updated_at":       "2026-05-04T00:00:00Z",
			}})
			coordination := result["coordination"].(map[string]any)
			coordination["branches"] = []any{map[string]any{
				"branch_id":        "branch-gamma",
				"agent_id":         "agent-gamma",
				"active_task_id":   "task-cli",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
			}}
			writeRPCResult(w, req, result)
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-cli",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-gamma",
					"claim_status":           "RELEASED",
					"claim_branch_id":        "branch-gamma",
					"claim_write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
				},
			}})
		case "task.submit":
			submitCalls++
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			requestCalls++
			writeRPCResult(w, req, map[string]any{
				"request_id": "areq-boundary",
				"status":     "PENDING",
			})
		default:
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-demo",
		"agent_id":                "agent-gamma",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        `{"paths":["cmd/**","go.mod","README.md"]}`,
		"side_effect_refs":        []string{"side-effect:go-mod"},
		"active_task_id":          "task-cli",
		"branch_id":               "branch-gamma",
		"boundary_transition_key": "abpc-boundary-transition:released-claim",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected released claim binding to queue lead request, got %+v", result)
	}
	for _, want := range []string{`"lead_task_id"`, `"do_not_retry": true`, `"lead_request_id": "areq-boundary"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, "already_applied") || strings.Contains(result.Output, "role_scope_already_satisfied") {
		t.Fatalf("released claim must not produce already-applied live boundary receipt: %s", result.Output)
	}
	if listCalls < 1 || submitCalls != 1 || requestCalls != 1 {
		t.Fatalf("expected binding check then lead queue, list=%d submit=%d request=%d calls=%d", listCalls, submitCalls, requestCalls, calls)
	}
}

func TestProjectRoleAssignToolStableLeadTaskIDIgnoresSummaryAndScopeOrder(t *testing.T) {
	var taskIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectRoleAssignCoordinationResultWithRoles("agent-alpha", nil))
		case "task.submit":
			taskIDs = append(taskIDs, rpcString(req.Params, "task_id"))
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-lead-role",
				"workspace_id": "ws",
				"to_agent_id":  "agent-alpha",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	firstTool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma")
	secondTool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta")
	first := firstTool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-gamma",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": `{"paths":["src/preview/**","tests/preview/**"]}`,
		"summary":          "first phrasing",
	})
	second := secondTool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-gamma",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": `{"paths":["tests/preview/**","src/preview/**"]}`,
		"summary":          "second phrasing",
	})
	if first == nil || first.IsError || second == nil || second.IsError {
		t.Fatalf("expected both non-lead requests to queue, got first=%+v second=%+v", first, second)
	}
	if len(taskIDs) != 2 {
		t.Fatalf("expected two task submit attempts, got %+v", taskIDs)
	}
	if taskIDs[0] == "" || taskIDs[0] != taskIDs[1] {
		t.Fatalf("expected stable lead task id across summary/scope order changes, got %+v", taskIDs)
	}
}

func TestProjectRoleAssignToolBoundaryTransitionKeySuppressesDuplicateLeadWake(t *testing.T) {
	taskSubmitCalls := 0
	agentRequestCalls := 0
	var taskIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectRoleAssignCoordinationResultWithRoles("agent-alpha", nil))
		case "task.submit":
			taskSubmitCalls++
			taskIDs = append(taskIDs, rpcString(req.Params, "task_id"))
			rawRequirements, _ := json.Marshal(req.Params["task_requirements"])
			if payload := string(rawRequirements); !strings.Contains(payload, "project_role_scope_authority_transition.v1") || !strings.Contains(payload, "abpc-boundary-transition:stable") {
				t.Fatalf("expected boundary transition requirements, got %s", payload)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"boundary_transition_key: abpc-boundary-transition:stable", "side_effect_refs: side-effect:branch:pathset", "active_task_id: task-impl", "branch_id: branch-beta"} {
				if !strings.Contains(description, want) {
					t.Fatalf("expected role/scope task description to carry transition metadata %q, got %s", want, description)
				}
			}
			if taskSubmitCalls == 1 {
				writeRPCResult(w, req, map[string]any{
					"task_id":      rpcString(req.Params, "task_id"),
					"workspace_id": "ws",
					"status":       "PENDING",
				})
				return
			}
			writeRPCError(w, req, -32602, "workspace task already exists")
		case "agent.request":
			agentRequestCalls++
			if agentRequestCalls > 1 {
				t.Fatalf("duplicate boundary transition must not re-send lead wake request")
			}
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-lead-role",
				"workspace_id": "ws",
				"to_agent_id":  "agent-alpha",
				"status":       "PENDING",
			})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta")
	args := map[string]any{
		"project_id":              "project-demo",
		"agent_id":                "agent-beta",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        `{"paths":["src/App.tsx","package.json"]}`,
		"summary":                 "boundary expansion for the same side effect",
		"boundary_transition_key": "abpc-boundary-transition:stable",
		"side_effect_refs":        []string{"side-effect:branch:pathset"},
		"active_task_id":          "task-impl",
		"branch_id":               "branch-beta",
	}
	first := tool.Execute(context.Background(), args)
	second := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-demo",
		"agent_id":                "agent-beta",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        `{"paths":["src/App.tsx","package.json","index.html"]}`,
		"summary":                 "same side effect with a slightly different requested boundary",
		"boundary_transition_key": "abpc-boundary-transition:stable",
		"side_effect_refs":        []string{"side-effect:branch:pathset"},
		"active_task_id":          "task-impl",
		"branch_id":               "branch-beta",
	})
	if first == nil || first.IsError {
		t.Fatalf("expected first transition to queue, got %+v", first)
	}
	if second == nil || second.IsError {
		t.Fatalf("expected duplicate transition to reuse existing task without hard failure, got %+v", second)
	}
	if len(taskIDs) != 2 || taskIDs[0] == "" || taskIDs[0] != taskIDs[1] {
		t.Fatalf("expected stable role-scope task id for same boundary transition, got %+v", taskIDs)
	}
	if agentRequestCalls != 1 {
		t.Fatalf("expected only one lead wake request, got %d", agentRequestCalls)
	}
	for _, want := range []string{`"transition_already_pending": true`, `"existing_task_id"`, `"lead_notified": false`, "wait_or_assist_existing_authority_transition"} {
		if !strings.Contains(second.Output, want) {
			t.Fatalf("expected duplicate output to contain %q, got %s", want, second.Output)
		}
	}
}

func TestProjectRoleAssignToolRecognizesAppliedAuthorityReceiptWithoutCurrentTaskKey(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	expandedScope := `{"paths":["src/App.*","src/clearpress.ts","src/test/setup.ts"]}`
	authorityReq, _ := json.Marshal(map[string]any{
		"schema":                  "project_role_scope_authority_transition.v1",
		"boundary_transition_key": "abpc-boundary-transition:applied",
		"dedup_key":               "abpc-boundary-transition:applied",
		"project_id":              "project-clearpress",
		"target_agent_id":         "agent-beta",
		"role_type":               "IMPLEMENTER",
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
		"write_scope_json":        expandedScope,
	})
	staleReq, _ := json.Marshal(map[string]any{
		"schema":           "project_role_scope_authority_transition.v1",
		"project_id":       "project-clearpress",
		"target_agent_id":  "agent-beta",
		"role_type":        "IMPLEMENTER",
		"side_effect_refs": refs,
		"active_task_id":   "task-editor",
		"branch_id":        "branch-beta",
		"write_scope_json": expandedScope,
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{"agent_id": "agent-alpha", "role_type": "STRATEGIC_LEAD", "status": "ACTIVE"},
					"branches": []any{map[string]any{
						"branch_id":        "branch-beta",
						"agent_id":         "agent-beta",
						"status":           "READY_FOR_REVIEW",
						"write_scope_json": expandedScope,
					}},
				},
			})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-stale",
					"status":                 "PENDING",
					"task_requirements_json": string(staleReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "RELEASED",
				},
				map[string]any{
					"task_id":                "task-role-scope-applied",
					"status":                 "RESOLVED",
					"task_requirements_json": string(authorityReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "COMPLETED",
				},
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-beta",
					"claim_write_scope_json": expandedScope,
				},
			}})
		default:
			t.Fatalf("applied receipt should skip duplicate project.role.assign; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-clearpress",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": expandedScope,
		"side_effect_refs": refs,
		"active_task_id":   "task-editor",
		"branch_id":        "branch-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected applied receipt to satisfy duplicate role-scope task, got %+v", result)
	}
	for _, want := range []string{"authority_transition_applied", "task-role-scope-applied", "already_satisfied", "active_claim_rebind"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected applied receipt output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 2 {
		t.Fatalf("expected coordination and task-list receipt lookup only, got %d calls", calls)
	}
}

func TestProjectRoleAssignToolRecognizesAppliedAuthorityReceiptWithReleasedOwnerClaim(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	expandedScope := `{"paths":["src/App.*","src/clearpress.ts","src/test/setup.ts"]}`
	authorityReq, _ := json.Marshal(map[string]any{
		"schema":                  "project_role_scope_authority_transition.v1",
		"boundary_transition_key": "abpc-boundary-transition:released-owner",
		"dedup_key":               "abpc-boundary-transition:released-owner",
		"project_id":              "project-clearpress",
		"target_agent_id":         "agent-beta",
		"role_type":               "IMPLEMENTER",
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
		"write_scope_json":        expandedScope,
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{"agent_id": "agent-alpha", "role_type": "STRATEGIC_LEAD", "status": "ACTIVE"},
					"branches": []any{map[string]any{
						"branch_id":        "branch-beta",
						"agent_id":         "agent-beta",
						"active_task_id":   "task-editor",
						"status":           "ACTIVE",
						"write_scope_json": expandedScope,
					}},
				},
			})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-applied",
					"status":                 "RESOLVED",
					"task_requirements_json": string(authorityReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "COMPLETED",
				},
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "PENDING",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "RELEASED",
					"claim_branch_id":        "branch-beta",
					"claim_write_scope_json": expandedScope,
				},
			}})
		default:
			t.Fatalf("released owner claim already carries the applied boundary; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-clearpress",
		"agent_id":                "agent-beta",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        expandedScope,
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
		"boundary_transition_key": "abpc-boundary-transition:released-owner",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected released owner claim to satisfy applied authority receipt, got %+v", result)
	}
	for _, want := range []string{"authority_transition_applied", "task-role-scope-applied", "already_satisfied", "already_applied"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected released-claim receipt output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 2 {
		t.Fatalf("expected coordination and task-list receipt lookup only, got %d calls", calls)
	}
}

func TestProjectRoleAssignToolRecognizesAppliedAuthorityReceiptWithLeadKey(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	expandedScope := `{"paths":["src/App.*","src/clearpress.ts","src/test/setup.ts"]}`
	authorityReq, _ := json.Marshal(map[string]any{
		"schema":                  "project_role_scope_authority_transition.v1",
		"boundary_transition_key": "abpc-boundary-transition:applied",
		"dedup_key":               "abpc-boundary-transition:applied",
		"project_id":              "project-clearpress",
		"target_agent_id":         "agent-beta",
		"role_type":               "IMPLEMENTER",
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
		"write_scope_json":        expandedScope,
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{"agent_id": "agent-alpha", "role_type": "STRATEGIC_LEAD", "status": "ACTIVE"},
					"branches": []any{map[string]any{
						"branch_id":        "branch-beta",
						"agent_id":         "agent-beta",
						"active_task_id":   "task-editor",
						"status":           "ACTIVE",
						"write_scope_json": expandedScope,
					}},
				},
			})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-applied",
					"status":                 "RESOLVED",
					"task_requirements_json": string(authorityReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "COMPLETED",
				},
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-beta",
					"claim_write_scope_json": expandedScope,
				},
			}})
		default:
			t.Fatalf("lead with applied key must not call project.role.assign; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-clearpress",
		"agent_id":                "agent-beta",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        expandedScope,
		"boundary_transition_key": "abpc-boundary-transition:applied",
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected keyed applied receipt to satisfy lead rerun, got %+v", result)
	}
	for _, want := range []string{"authority_transition_applied", "abpc-boundary-transition:applied", "task-role-scope-applied"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 2 {
		t.Fatalf("expected coordination and task-list receipt lookup only, got %d calls", calls)
	}
}

func TestProjectRoleAssignToolReusesLiveNoKeyAuthorityTransitionBeforeSubmit(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	oldScope := `{"paths":["src/App.*","src/clearpress.ts"]}`
	requestedScope := `{"paths":["src/App.*","src/clearpress.ts","src/test/setup.ts"]}`
	liveReq, _ := json.Marshal(map[string]any{
		"schema":           "project_role_scope_authority_transition.v1",
		"project_id":       "project-clearpress",
		"target_agent_id":  "agent-beta",
		"role_type":        "IMPLEMENTER",
		"side_effect_refs": refs,
		"active_task_id":   "task-editor",
		"branch_id":        "branch-beta",
		"write_scope_json": oldScope,
	})
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectRoleAssignCoordinationResultWithRoles("agent-alpha", nil))
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-old-live",
					"status":                 "PENDING",
					"task_requirements_json": string(liveReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "RELEASED",
				},
			}})
		default:
			t.Fatalf("live no-key duplicate must not create another task or wake lead; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-eta")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-clearpress",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": requestedScope,
		"side_effect_refs": refs,
		"active_task_id":   "task-editor",
		"branch_id":        "branch-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected live no-key authority transition reuse, got %+v", result)
	}
	for _, want := range []string{"transition_already_pending", "task-role-scope-old-live", "wait_or_assist_existing_authority_transition"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if listCalls == 0 {
		t.Fatalf("expected tasks.list lookup before submit")
	}
}

func TestProjectRoleAssignToolTreatsBlockedBoundaryAuthorityTaskAsTerminalBlocker(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	requestedScope := `{"paths":["src/App.*","src/clearpress.ts","src/test/setup.ts"]}`
	authorityReq, _ := json.Marshal(map[string]any{
		"schema":                  "project_role_scope_authority_transition.v1",
		"boundary_transition_key": "abpc-boundary-transition:blocked",
		"project_id":              "project-clearpress",
		"target_agent_id":         "agent-beta",
		"role_type":               "IMPLEMENTER",
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
		"write_scope_json":        requestedScope,
	})
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectRoleAssignCoordinationResultWithRoles("agent-alpha", nil))
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-blocked",
					"status":                 "RUNNING",
					"task_kind":              "COORDINATION",
					"project_id":             "project-clearpress",
					"project_lane":           "coordination",
					"tags":                   []any{"project-role-scope", "strategic-lead", "coordination"},
					"task_requirements_json": string(authorityReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "BLOCKED",
					"claim_summary":          "typed terminal blocker: no authority-bearing admission path is available",
				},
			}})
		default:
			t.Fatalf("blocked boundary authority task must not be recreated or re-woken; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-eta")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":              "project-clearpress",
		"agent_id":                "agent-beta",
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        requestedScope,
		"side_effect_refs":        refs,
		"boundary_transition_key": "abpc-boundary-transition:blocked",
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected terminal-blocker receipt without duplicate wake, got %+v", result)
	}
	for _, want := range []string{`"transition_terminal_blocker": true`, `"claim_status": "BLOCKED"`, "inspect_existing_authority_terminal_blocker"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected terminal blocker output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"transition_already_pending": true`) {
		t.Fatalf("blocked authority carrier must not be reported as live pending, got %s", result.Output)
	}
	if listCalls == 0 {
		t.Fatalf("expected tasks.list lookup before terminal blocker receipt")
	}
}

func TestProjectRoleAssignToolDoesNotCollapseAppliedAuthorityWhenBoundaryNoLongerCovers(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	requestedScope := `{"paths":["src/App.*","src/clearpress.ts","src/test/setup.ts"]}`
	narrowScope := `{"paths":["src/App.*"]}`
	authorityReq, _ := json.Marshal(map[string]any{
		"schema":                  "project_role_scope_authority_transition.v1",
		"boundary_transition_key": "abpc-boundary-transition:old",
		"project_id":              "project-clearpress",
		"target_agent_id":         "agent-beta",
		"role_type":               "IMPLEMENTER",
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
		"write_scope_json":        requestedScope,
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{"agent_id": "agent-alpha", "role_type": "STRATEGIC_LEAD", "status": "ACTIVE"},
					"branches": []any{map[string]any{
						"branch_id":        "branch-beta",
						"agent_id":         "agent-beta",
						"status":           "ACTIVE",
						"write_scope_json": narrowScope,
					}},
				},
			})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-old",
					"status":                 "RESOLVED",
					"task_requirements_json": string(authorityReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "COMPLETED",
				},
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-beta",
					"claim_write_scope_json": narrowScope,
				},
			}})
		case 3:
			if req.Method != "project.role.assign" {
				t.Fatalf("expected fresh assignment after uncovered old receipt, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-beta",
					"workspace_id":     "ws",
					"project_id":       "project-clearpress",
					"agent_id":         "agent-beta",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": requestedScope,
				},
				"active_claim_rebind": map[string]any{
					"state":                   "updated",
					"task_id":                 "task-editor",
					"branch_id":               "branch-beta",
					"claim_write_scope_json":  requestedScope,
					"branch_write_scope_json": requestedScope,
				},
			})
		default:
			t.Fatalf("unexpected extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-clearpress",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": requestedScope,
		"side_effect_refs": refs,
		"active_task_id":   "task-editor",
		"branch_id":        "branch-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected uncovered stale receipt to fall through to fresh assignment, got %+v", result)
	}
	if strings.Contains(result.Output, "authority_transition_applied") {
		t.Fatalf("uncovered old authority task must not be treated as applied receipt, got %s", result.Output)
	}
	if !strings.Contains(result.Output, `"role_id": "role-beta"`) {
		t.Fatalf("expected fresh role assignment output, got %s", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected coordination, task list, and role assignment calls, got %d", calls)
	}
}

func TestProjectRoleAssignToolDoesNotCollapseAppliedAuthorityAcrossBranchTaskMismatch(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	requestedScope := `{"paths":["src/App.*","src/clearpress.ts","src/test/setup.ts"]}`
	authorityReq, _ := json.Marshal(map[string]any{
		"schema":                  "project_role_scope_authority_transition.v1",
		"boundary_transition_key": "abpc-boundary-transition:old",
		"project_id":              "project-clearpress",
		"target_agent_id":         "agent-beta",
		"role_type":               "IMPLEMENTER",
		"side_effect_refs":        refs,
		"active_task_id":          "task-editor",
		"branch_id":               "branch-beta",
		"write_scope_json":        requestedScope,
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{"agent_id": "agent-alpha", "role_type": "STRATEGIC_LEAD", "status": "ACTIVE"},
					"branches": []any{map[string]any{
						"branch_id":        "branch-beta",
						"agent_id":         "agent-beta",
						"active_task_id":   "task-other",
						"status":           "ACTIVE",
						"write_scope_json": requestedScope,
					}},
				},
			})
		case 2:
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-old",
					"status":                 "RESOLVED",
					"task_requirements_json": string(authorityReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "COMPLETED",
				},
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-beta",
					"claim_write_scope_json": requestedScope,
				},
			}})
		case 3:
			if req.Method != "project.role.assign" {
				t.Fatalf("expected fresh assignment after branch/task mismatch, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-beta",
					"workspace_id":     "ws",
					"project_id":       "project-clearpress",
					"agent_id":         "agent-beta",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": requestedScope,
				},
				"active_claim_rebind": map[string]any{
					"state":                   "updated",
					"task_id":                 "task-editor",
					"branch_id":               "branch-beta",
					"claim_write_scope_json":  requestedScope,
					"branch_write_scope_json": requestedScope,
				},
			})
		default:
			t.Fatalf("unexpected extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-clearpress",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": requestedScope,
		"side_effect_refs": refs,
		"active_task_id":   "task-editor",
		"branch_id":        "branch-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected branch/task mismatch to fall through to fresh assignment, got %+v", result)
	}
	if strings.Contains(result.Output, "authority_transition_applied") {
		t.Fatalf("mismatched branch active task must not be treated as applied receipt, got %s", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected coordination, task list, and role assignment calls, got %d", calls)
	}
}

func TestProjectRoleAssignToolStructuresOverlapDenial(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			writeRPCResult(w, req, projectRoleAssignCoordinationResultWithRoles("agent-alpha", nil))
		case 2:
			if req.Method != "project.role.assign" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "project role write scope conflict: write_scope_json overlaps active claim task_id=task-gamma agent_id=gamma branch_id=branch-gamma; request the active owner to commit/publish")
		case 3:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"project_role_scope_authority_denial.v1", "boundary_expansion_denied_overlap", "overlaps_live_owner_lane", "task-gamma", "gamma"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected durable overlap denial update to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRoleAssignTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-demo",
		"agent_id":         "agent-beta",
		"role_type":        "IMPLEMENTER",
		"write_scope_json": `{"paths":["package.json","src/App.tsx"]}`,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected overlap denial, got %+v", result)
	}
	for _, want := range []string{"boundary_expansion_denied_overlap", "denial_recorded", "conflicting_task_id", "task-gamma", "conflicting_owner_agent_id", "gamma", "wait_or_split_existing_owner_lane"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected structured overlap output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected coordination, role assign, and denial update calls, got %d", calls)
	}
}

func TestProjectRoleAssignScopeCoversGlobPattern(t *testing.T) {
	if !projectRoleAssignScopeCovers(
		projectRoleAssignNormalizedWriteScopePaths(`{"paths":["src/App.*","src/lib/**"]}`),
		projectRoleAssignNormalizedWriteScopePaths(`{"paths":["src/App.tsx","src/lib/render/canvas.ts"]}`),
	) {
		t.Fatal("expected active glob/prefix scope to cover requested concrete paths")
	}
	if projectRoleAssignScopeCovers(
		projectRoleAssignNormalizedWriteScopePaths(`{"paths":["src/*"]}`),
		projectRoleAssignNormalizedWriteScopePaths(`{"paths":["src/deep/file.ts"]}`),
	) {
		t.Fatal("expected src/* to cover only one path segment, not recursive descendants")
	}
}

func TestProjectRoleAssignWriteScopeEmptyRejectsBlankPathsets(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "empty object", raw: `{}`, want: true},
		{name: "blank paths", raw: `{"paths":["","   ","."]}`, want: true},
		{name: "blank array", raw: `["","/"]`, want: true},
		{name: "paths", raw: `{"paths":["web/**"]}`, want: false},
		{name: "files", raw: `{"files":["README.md"]}`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := projectRoleAssignWriteScopeEmpty(tt.raw); got != tt.want {
				t.Fatalf("projectRoleAssignWriteScopeEmpty(%s) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func projectRoleAssignCoordinationResultWithRoles(leadAgentID string, roles []map[string]any) map[string]any {
	result := projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
		CurrentPhase: "IMPLEMENTATION",
		LeadAgentID:  leadAgentID,
	})
	coordination := result["coordination"].(map[string]any)
	if roles != nil {
		coordination["roles"] = roles
	}
	return result
}
