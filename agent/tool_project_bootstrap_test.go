package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectBootstrapSpecTaskNamesCanonicalDesignAndPlanDoc(t *testing.T) {
	description := renderProjectBootstrapSpecTaskDescription("project-demo", "Demo", "task-root", nil)
	for _, want := range []string{
		"project.project-demo.design_and_plan",
		"project.project-demo.design",
		"project.project-demo.implementation_plan",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("expected spec task description to mention %q, got:\n%s", want, description)
		}
	}
}

func TestProjectBootstrapSpecTaskNamesSourceDocKeys(t *testing.T) {
	description := renderProjectBootstrapSpecTaskDescription("project-demo", "Demo", "task-root", []string{"run.operator-spec"})
	for _, want := range []string{
		"run.operator-spec",
		"project.project-demo.source_requirements_trace",
		"rhizome_source_requirements_trace_v1",
		"non-droppable acceptance-critical anchors",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("expected source fidelity description to mention %q, got:\n%s", want, description)
		}
	}
}

func TestProjectBootstrapToolCreatesLeadSpecAndRootLink(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-root" {
				t.Fatalf("preflight task_id = %q, want task-root", got)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"task": map[string]any{"task_id": "task-root"},
			}})
		case 2:
			if req.Method != "project.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"projects": []map[string]any{}})
		case 3:
			if req.Method != "project.create" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			for key, want := range map[string]string{
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"title":        "Subpixel Pattern Lab",
				"description":  "Build a local dashboard and generator with coordinated design first.",
				"created_by":   "agent-alpha",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"project": map[string]any{
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"title":        "Subpixel Pattern Lab",
				"description":  "Build a local dashboard and generator with coordinated design first.",
				"status":       "ACTIVE",
				"created_by":   "agent-alpha",
			}})
		case 4:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-subpixel.intake" {
				t.Fatalf("intake doc_key = %q, want project.project-subpixel.intake", got)
			}
			content := rpcString(req.Params, "content")
			for _, want := range []string{"Project Intake", "root_task_id: task-root", "repo_required: true", "project.project-subpixel.acceptance_criteria", "Acceptance criteria should use stable IDs", "Do not begin implementation"} {
				if !strings.Contains(content, want) {
					t.Fatalf("intake doc missing %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-intake"})
		case 5:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			if got := rpcString(req.Params, "actor_id"); got != "agent-alpha" {
				t.Fatalf("actor_id = %q, want agent-alpha", got)
			}
			if got, ok := req.Params["repo_required"].(bool); !ok || !got {
				t.Fatalf("repo_required = %#v, want true; params=%+v", req.Params["repo_required"], req.Params)
			}
			if got := rpcString(req.Params, "repo_status"); got != "MISSING" {
				t.Fatalf("repo_status = %q, want MISSING", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"goal":          "Build a local dashboard and generator with coordinated design first.",
				"current_phase": "INTAKE",
				"repo_required": true,
				"repo_status":   "MISSING",
				"updated_by":    "agent-alpha",
			}})
		case 6:
			if req.Method != "project.lead.claim" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			if got := rpcString(req.Params, "agent_id"); got != "agent-alpha" {
				t.Fatalf("agent_id = %q, want agent-alpha", got)
			}
			writeRPCResult(w, req, map[string]any{"role": map[string]any{
				"role_id":      "projrole-alpha",
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"agent_id":     "agent-alpha",
				"role_type":    "STRATEGIC_LEAD",
				"status":       "ACTIVE",
			}})
		case 7:
			if req.Method != "project.phase.transition" {
				t.Fatalf("unexpected seventh method %q", req.Method)
			}
			if got := rpcString(req.Params, "to_phase"); got != "SPEC" {
				t.Fatalf("to_phase = %q, want SPEC", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"goal":          "Build a local dashboard and generator with coordinated design first.",
				"current_phase": "SPEC",
				"repo_required": true,
				"repo_status":   "MISSING",
				"updated_by":    "agent-alpha",
			}})
		case 8:
			if req.Method != "task.project_fields.put" {
				t.Fatalf("unexpected eighth method %q", req.Method)
			}
			for key, want := range map[string]string{
				"task_id":      "task-root",
				"project_id":   "project-subpixel",
				"task_kind":    "COORDINATION",
				"project_lane": "strategy",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			if got, ok := req.Params["requires_project_gate"].(bool); !ok || got {
				t.Fatalf("requires_project_gate = %#v, want false", req.Params["requires_project_gate"])
			}
			writeRPCResult(w, req, map[string]any{"task": map[string]any{
				"task_id":               "task-root",
				"project_id":            "project-subpixel",
				"task_kind":             "COORDINATION",
				"project_lane":          "strategy",
				"requires_project_gate": false,
			}})
		case 9:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected ninth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 10:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected tenth method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-project-subpixel-spec" {
				t.Fatalf("spec task_id = %q, want task-project-subpixel-spec", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "spec" {
				t.Fatalf("spec project_lane = %q, want spec", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"acceptance criteria", "stable IDs", "map each proposed implementation, review, validation, and post-MVP lane"} {
				if !strings.Contains(description, want) {
					t.Fatalf("spec task description missing %q:\n%s", want, description)
				}
			}
			if got := rpcStringSlice(req.Params, "dependency_task_ids"); strings.Join(got, ",") != "task-root" {
				t.Fatalf("dependency_task_ids = %#v, want task-root", got)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-project-subpixel-spec",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 11:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected eleventh method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "task.task-project-subpixel-spec" {
				t.Fatalf("spec doc_key = %q, want task.task-project-subpixel-spec", got)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-spec-task"})
		case 12:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"coordination_version": "v1",
				"project": map[string]any{
					"workspace_id": "ws",
					"project_id":   "project-subpixel",
				},
				"profile": map[string]any{
					"workspace_id":  "ws",
					"project_id":    "project-subpixel",
					"current_phase": "SPEC",
					"repo_required": true,
					"repo_status":   "MISSING",
				},
				"gate_status": map[string]any{
					"workspace_id":         "ws",
					"project_id":           "project-subpixel",
					"overall_state":        "BLOCKED",
					"implementation_ready": false,
				},
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":   "project-subpixel",
		"title":        "Subpixel Pattern Lab",
		"goal":         "Build a local dashboard and generator with coordinated design first.",
		"root_task_id": "task-root",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful project_bootstrap, got %+v", result)
	}
	for _, want := range []string{"project-subpixel", "projrole-alpha", "task-project-subpixel-spec", "implementation_ready", "false"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 12 {
		t.Fatalf("expected 12 calls, got %d", calls)
	}
}

func TestProjectBootstrapToolRejectsRootTaskClaimedByAnotherAgent(t *testing.T) {
	calls := 0
	claimAgent := "agent-alpha"
	claimStatus := "CLAIMED"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "agent.task.hydrate" {
			t.Fatalf("unexpected RPC method %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
			"task": map[string]any{"task_id": "task-root"},
			"workspace_task": map[string]any{
				"task_id":        "task-root",
				"claim_agent_id": claimAgent,
				"claim_status":   claimStatus,
			},
		}})
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":   "project-subpixel",
		"title":        "Subpixel Pattern Lab",
		"goal":         "Build a local dashboard and generator with coordinated design first.",
		"root_task_id": "task-root",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected bootstrap to reject foreign claimed root task, got %+v", result)
	}
	if !strings.Contains(result.Output, "already actively claimed by agent-alpha") {
		t.Fatalf("expected foreign claim blocker in output, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected preflight-only call, got %d", calls)
	}
}

func TestProjectBootstrapToolRejectsActiveProjectImplementationTaskRelink(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "agent.task.hydrate" {
			t.Fatalf("unexpected RPC method %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
			"workspace_task": map[string]any{
				"task_id":               "task-impl",
				"project_id":            "project-existing",
				"task_kind":             "EXECUTION",
				"project_lane":          "implementation",
				"requires_project_gate": true,
			},
		}})
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-impl", ProjectID: "project-existing", SessionID: "session-1"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"title":        "Existing Product",
		"goal":         "Continue the product implementation.",
		"root_task_id": "task-impl",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected active implementation bootstrap guard, got %+v", result)
	}
	for _, want := range []string{"project_bootstrap blocked active project-bound product task", "project_branch_commit", "implementation"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected hydrate-only preflight, got %d calls", calls)
	}
}

func TestProjectBootstrapToolReusesExistingProjectAndSpecTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-root-existing" {
				t.Fatalf("preflight task_id = %q, want task-root-existing", got)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"workspace_task": map[string]any{"task_id": "task-root-existing"},
			}})
		case 2:
			if req.Method != "project.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"projects": []map[string]any{{
				"workspace_id": "ws",
				"project_id":   "project-existing",
				"title":        "Subpixel Pattern Lab",
				"description":  "existing",
				"status":       "ACTIVE",
				"created_by":   "agent-beta",
			}}})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-intake"})
		case 4:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-existing",
				"goal":          "Goal",
				"current_phase": "SPEC",
				"repo_required": false,
				"repo_status":   "NOT_REQUIRED",
			}})
		case 5:
			if req.Method != "project.lead.claim" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"role": map[string]any{
				"role_id":    "projrole-alpha",
				"project_id": "project-existing",
				"agent_id":   "agent-alpha",
				"role_type":  "STRATEGIC_LEAD",
				"status":     "ACTIVE",
			}})
		case 6:
			if req.Method != "task.project_fields.put" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			for key, want := range map[string]string{
				"task_id":      "task-root-existing",
				"project_id":   "project-existing",
				"task_kind":    "COORDINATION",
				"project_lane": "strategy",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"task": map[string]any{
				"task_id":               "task-root-existing",
				"project_id":            "project-existing",
				"task_kind":             "COORDINATION",
				"project_lane":          "strategy",
				"requires_project_gate": false,
			}})
		case 7:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected seventh method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{{
				"task_id":      "task-existing-spec",
				"title":        "Prepare design spec",
				"description":  "Create design document",
				"status":       "PENDING",
				"project_id":   "project-existing",
				"project_lane": "spec",
			}}})
		case 8:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected eighth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"coordination_version": "v2",
				"project":              map[string]any{"workspace_id": "ws", "project_id": "project-existing"},
				"profile":              map[string]any{"workspace_id": "ws", "project_id": "project-existing", "current_phase": "SPEC", "repo_status": "NOT_REQUIRED"},
				"gate_status":          map[string]any{"workspace_id": "ws", "project_id": "project-existing", "overall_state": "BLOCKED", "implementation_ready": false},
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"title":         "Subpixel Pattern Lab",
		"goal":          "Goal",
		"root_task_id":  "task-root-existing",
		"repo_required": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful project_bootstrap reuse, got %+v", result)
	}
	for _, want := range []string{`"created": false`, "project-existing", "task-existing-spec"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 8 {
		t.Fatalf("expected 8 calls, got %d", calls)
	}
}

func TestProjectBootstrapToolUsesRuntimeProjectBindingWhenProjectIDOmitted(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-root-bound" {
				t.Fatalf("preflight task_id = %q, want task-root-bound", got)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"workspace_task": map[string]any{
					"task_id":    "task-root-bound",
					"project_id": "project-existing-binding",
				},
			}})
		case 2:
			if req.Method != "project.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"projects": []map[string]any{{
				"workspace_id": "ws",
				"project_id":   "project-existing-binding",
				"title":        "Decision Ledger",
				"description":  "preseeded operator project",
				"status":       "ACTIVE",
				"created_by":   "operator",
			}}})
		case 3:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-existing-binding" {
				t.Fatalf("preserve profile project_id = %q, want project-existing-binding", got)
			}
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"coordination_version": "v-before",
				"project":              map[string]any{"workspace_id": "ws", "project_id": "project-existing-binding"},
				"profile": map[string]any{
					"workspace_id":         "ws",
					"project_id":           "project-existing-binding",
					"current_phase":        "INTAKE",
					"repo_required":        true,
					"repo_status":          "READY",
					"repo_url":             "file:///tmp/decision-ledger.git",
					"repo_default_branch":  "main",
					"implementation_ready": false,
				},
				"gate_status": map[string]any{"workspace_id": "ws", "project_id": "project-existing-binding", "overall_state": "BLOCKED", "implementation_ready": false},
			}})
		case 4:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-existing-binding.intake" {
				t.Fatalf("intake doc_key = %q, want project.project-existing-binding.intake", got)
			}
			content := rpcString(req.Params, "content")
			for _, want := range []string{"project_id: project-existing-binding", "root_task_id: task-root-bound"} {
				if !strings.Contains(content, want) {
					t.Fatalf("intake doc missing %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-intake"})
		case 5:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-existing-binding" {
				t.Fatalf("profile project_id = %q, want project-existing-binding", got)
			}
			if got, ok := req.Params["repo_required"].(bool); !ok || !got {
				t.Fatalf("repo_required = %#v, want preserved true", req.Params["repo_required"])
			}
			if got := rpcString(req.Params, "repo_status"); got != "READY" {
				t.Fatalf("repo_status = %q, want preserved READY", got)
			}
			if got := rpcString(req.Params, "repo_url"); got != "file:///tmp/decision-ledger.git" {
				t.Fatalf("repo_url = %q, want preserved canonical URL", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-existing-binding",
				"goal":          "Keep the operator-created project as canonical.",
				"current_phase": "INTAKE",
				"repo_required": true,
				"repo_status":   "READY",
				"repo_url":      "file:///tmp/decision-ledger.git",
			}})
		case 6:
			if req.Method != "project.lead.claim" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-existing-binding" {
				t.Fatalf("lead project_id = %q, want project-existing-binding", got)
			}
			writeRPCResult(w, req, map[string]any{"role": map[string]any{
				"role_id":    "projrole-bound-alpha",
				"project_id": "project-existing-binding",
				"agent_id":   "agent-alpha",
				"role_type":  "STRATEGIC_LEAD",
				"status":     "ACTIVE",
			}})
		case 7:
			if req.Method != "task.project_fields.put" {
				t.Fatalf("unexpected seventh method %q", req.Method)
			}
			for key, want := range map[string]string{
				"task_id":    "task-root-bound",
				"project_id": "project-existing-binding",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"task": map[string]any{
				"task_id":               "task-root-bound",
				"project_id":            "project-existing-binding",
				"task_kind":             "COORDINATION",
				"project_lane":          "strategy",
				"requires_project_gate": false,
			}})
		case 8:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected eighth method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-existing-binding" {
				t.Fatalf("coordination project_id = %q, want project-existing-binding", got)
			}
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"coordination_version": "v-bound",
				"project":              map[string]any{"workspace_id": "ws", "project_id": "project-existing-binding"},
				"profile":              map[string]any{"workspace_id": "ws", "project_id": "project-existing-binding", "current_phase": "INTAKE", "repo_status": "READY"},
				"gate_status":          map[string]any{"workspace_id": "ws", "project_id": "project-existing-binding", "overall_state": "BLOCKED", "implementation_ready": false},
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-root-bound", ProjectID: "project-existing-binding"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"title":            "Decision Ledger",
		"goal":             "Keep the operator-created project as canonical.",
		"root_task_id":     "task-root-bound",
		"desired_phase":    "INTAKE",
		"create_spec_task": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful project_bootstrap with runtime-bound project, got %+v", result)
	}
	for _, want := range []string{`"created": false`, "project-existing-binding", `"root_task_linked": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 8 {
		t.Fatalf("expected 8 calls, got %d", calls)
	}
}

func TestProjectBootstrapToolCreatesRuntimeProjectInsteadOfTitleFallback(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"workspace_task": map[string]any{
					"task_id":    "task-root-bound",
					"project_id": "project-runtime-canonical",
				},
			}})
		case 2:
			if req.Method != "project.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"projects": []map[string]any{{
				"workspace_id": "ws",
				"project_id":   "project-foreign-same-title",
				"title":        "Decision Ledger",
				"status":       "ACTIVE",
				"created_by":   "agent-beta",
			}}})
		case 3:
			if req.Method != "project.create" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-runtime-canonical" {
				t.Fatalf("created project_id = %q, want project-runtime-canonical", got)
			}
			writeRPCResult(w, req, map[string]any{"project": map[string]any{
				"workspace_id": "ws",
				"project_id":   "project-runtime-canonical",
				"title":        "Decision Ledger",
				"description":  "Create the runtime-bound project, not the same-title foreign project.",
				"status":       "ACTIVE",
				"created_by":   "agent-alpha",
			}})
		case 4:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-runtime-canonical.intake" {
				t.Fatalf("intake doc_key = %q, want project.project-runtime-canonical.intake", got)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-intake"})
		case 5:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-runtime-canonical" {
				t.Fatalf("profile project_id = %q, want project-runtime-canonical", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-runtime-canonical",
				"current_phase": "INTAKE",
				"repo_required": false,
				"repo_status":   "NOT_REQUIRED",
			}})
		case 6:
			if req.Method != "project.lead.claim" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"role": map[string]any{
				"role_id":    "projrole-runtime-alpha",
				"project_id": "project-runtime-canonical",
				"agent_id":   "agent-alpha",
				"role_type":  "STRATEGIC_LEAD",
				"status":     "ACTIVE",
			}})
		case 7:
			if req.Method != "task.project_fields.put" {
				t.Fatalf("unexpected seventh method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-runtime-canonical" {
				t.Fatalf("task project_id = %q, want project-runtime-canonical", got)
			}
			writeRPCResult(w, req, map[string]any{"task": map[string]any{
				"task_id":               "task-root-bound",
				"project_id":            "project-runtime-canonical",
				"task_kind":             "COORDINATION",
				"project_lane":          "strategy",
				"requires_project_gate": false,
			}})
		case 8:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected eighth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"coordination_version": "v-runtime",
				"project":              map[string]any{"workspace_id": "ws", "project_id": "project-runtime-canonical"},
				"profile":              map[string]any{"workspace_id": "ws", "project_id": "project-runtime-canonical", "current_phase": "INTAKE", "repo_status": "NOT_REQUIRED"},
				"gate_status":          map[string]any{"workspace_id": "ws", "project_id": "project-runtime-canonical", "overall_state": "BLOCKED", "implementation_ready": false},
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-root-bound", ProjectID: "project-runtime-canonical"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"title":            "Decision Ledger",
		"goal":             "Create the runtime-bound project, not the same-title foreign project.",
		"root_task_id":     "task-root-bound",
		"repo_required":    false,
		"desired_phase":    "INTAKE",
		"create_spec_task": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful runtime-bound project create, got %+v", result)
	}
	for _, want := range []string{`"created": true`, "project-runtime-canonical"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if strings.Contains(result.Output, "project-foreign-same-title") {
		t.Fatalf("runtime-bound bootstrap attached to foreign same-title project: %s", result.Output)
	}
	if calls != 8 {
		t.Fatalf("expected 8 calls, got %d", calls)
	}
}

func TestProjectBootstrapToolRecoversConcurrentProjectCreate(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-root-race" {
				t.Fatalf("preflight task_id = %q, want task-root-race", got)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"task": map[string]any{"task_id": "task-root-race"},
			}})
		case 2:
			if req.Method != "project.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"projects": []map[string]any{}})
		case 3:
			if req.Method != "project.create" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-task-root-race" {
				t.Fatalf("project_id = %q, want project-task-root-race", got)
			}
			writeRPCError(w, req, -32000, "project already exists")
		case 4:
			if req.Method != "project.list" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"projects": []map[string]any{{
				"workspace_id": "ws",
				"project_id":   "project-task-root-race",
				"title":        "Race Project",
				"status":       "ACTIVE",
				"created_by":   "agent-beta",
			}}})
		case 5:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-intake"})
		case 6:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-task-root-race",
				"current_phase": "INTAKE",
				"repo_required": true,
				"repo_status":   "MISSING",
			}})
		case 7:
			if req.Method != "project.lead.claim" {
				t.Fatalf("unexpected seventh method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"role": map[string]any{
				"role_id":    "projrole-race",
				"project_id": "project-task-root-race",
				"agent_id":   "agent-alpha",
				"role_type":  "STRATEGIC_LEAD",
				"status":     "ACTIVE",
			}})
		case 8:
			if req.Method != "task.project_fields.put" {
				t.Fatalf("unexpected eighth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"task": map[string]any{
				"task_id":               "task-root-race",
				"project_id":            "project-task-root-race",
				"task_kind":             "COORDINATION",
				"project_lane":          "strategy",
				"requires_project_gate": false,
			}})
		case 9:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected ninth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"coordination_version": "v-race",
				"project":              map[string]any{"workspace_id": "ws", "project_id": "project-task-root-race"},
				"profile":              map[string]any{"workspace_id": "ws", "project_id": "project-task-root-race", "current_phase": "INTAKE", "repo_status": "MISSING"},
				"gate_status":          map[string]any{"workspace_id": "ws", "project_id": "project-task-root-race", "overall_state": "BLOCKED", "implementation_ready": false},
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"title":            "Race Project",
		"goal":             "Converge two simultaneous bootstrap attempts.",
		"root_task_id":     "task-root-race",
		"desired_phase":    "INTAKE",
		"create_spec_task": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful project_bootstrap race recovery, got %+v", result)
	}
	for _, want := range []string{`"created": false`, "project-task-root-race", `"root_task_linked": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 9 {
		t.Fatalf("expected 9 calls, got %d", calls)
	}
}

func TestProjectBootstrapToolRejectsRuntimeBindingMismatchBeforeRPC(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("project_bootstrap should reject runtime binding mismatches before RPC; got %s", r.URL.Path)
	}))
	defer server.Close()

	baseTool := func() *ProjectBootstrapTool {
		return NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1").
			WithRuntimeBinding(func() AgentRuntimeBinding {
				return AgentRuntimeBinding{TaskID: "task-root-current", ProjectID: "project-current"}
			})
	}

	for name, tc := range map[string]struct {
		args     map[string]any
		contains []string
	}{
		"project_id_mismatch": {
			args: map[string]any{
				"project_id":   "project-other",
				"title":        "Decision Ledger",
				"goal":         "Should keep the runtime-bound project.",
				"root_task_id": "task-root-current",
			},
			contains: []string{"project_id mismatch", "project-current", "project-other"},
		},
		"root_task_id_mismatch": {
			args: map[string]any{
				"title":        "Decision Ledger",
				"goal":         "Should keep the runtime-bound root task.",
				"root_task_id": "task-root-other",
			},
			contains: []string{"root_task_id task-root-other", "currently bound to task task-root-current"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			result := baseTool().Execute(context.Background(), tc.args)
			if result == nil || !result.IsError {
				t.Fatalf("expected runtime binding rejection, got %+v", result)
			}
			for _, want := range tc.contains {
				if !strings.Contains(result.Output, want) {
					t.Fatalf("expected output to contain %q, got %q", want, result.Output)
				}
			}
		})
	}
}

func TestProjectBootstrapToolRejectsUnsafeInputsBeforeRPC(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("project_bootstrap should reject invalid input before RPC; got %s", r.URL.Path)
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	for name, args := range map[string]map[string]any{
		"missing_root_task_id": {
			"title": "Unscoped broad project",
			"goal":  "Should not reach the server.",
		},
		"implementation_phase": {
			"title":         "Unsafe phase jump",
			"goal":          "Should not bypass design/spec gates.",
			"root_task_id":  "task-root",
			"desired_phase": "IMPLEMENTATION",
		},
	} {
		t.Run(name, func(t *testing.T) {
			result := tool.Execute(context.Background(), args)
			if result == nil || !result.IsError {
				t.Fatalf("expected project_bootstrap input rejection, got %+v", result)
			}
		})
	}
}

func TestProjectBootstrapToolFailsBeforeProjectCreateWhenRootTaskMissing(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "agent.task.hydrate" {
			t.Fatalf("unexpected method before root task preflight failure: %s", req.Method)
		}
		writeRPCError(w, req, -32000, "workspace task not found")
	}))
	defer server.Close()

	tool := NewProjectBootstrapTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"title":        "Missing Root Task Project",
		"goal":         "Should not create any project-side records.",
		"root_task_id": "task-missing",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected root task preflight error, got %+v", result)
	}
	if !strings.Contains(result.Output, "root task preflight failed") {
		t.Fatalf("expected preflight failure output, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only one preflight call, got %d", calls)
	}
}

func TestProjectBootstrapDefaultsAreFailClosedAndDeterministic(t *testing.T) {
	if got := projectBootstrapIDFromRootTask("task-root"); got != "project-task-root" {
		t.Fatalf("project id = %q, want project-task-root", got)
	}
	if got := projectBootstrapSpecTaskID("project-task-root"); got != "task-project-task-root-spec" {
		t.Fatalf("spec task id = %q, want task-project-task-root-spec", got)
	}
	if got := bootstrapRepoRequired(map[string]any{}, "Write a poem", "This might still need project materialization."); !got {
		t.Fatalf("repo_required default = false, want conservative true")
	}
	if got := bootstrapRepoRequired(map[string]any{"repo_required": false}, "Write a poem", "No repo."); got {
		t.Fatalf("explicit repo_required=false was not preserved")
	}
	if got, err := normalizeProjectBootstrapDesiredPhase(""); err != nil || got != "SPEC" {
		t.Fatalf("default desired phase = %q, %v; want SPEC", got, err)
	}
	if got, err := normalizeProjectBootstrapDesiredPhase("intake"); err != nil || got != "INTAKE" {
		t.Fatalf("intake desired phase = %q, %v; want INTAKE", got, err)
	}
	if _, err := normalizeProjectBootstrapDesiredPhase("IMPLEMENTATION"); err == nil {
		t.Fatalf("expected IMPLEMENTATION phase to be rejected")
	}
}
