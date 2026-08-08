package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTaskSubmitToolCreatesWorkspaceTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			for key, want := range map[string]string{
				"workspace_id":  "ws",
				"task_id":       "task-build-backend",
				"owner_user_id": "owner-1",
				"priority":      "high",
				"title":         "Build backend",
				"description":   "Implement generator backend",
				"task_kind":     "EXECUTION",
				"task_template": "generic",
				"project_id":    "project-alpha",
				"project_lane":  "implementation",
				"linked_by":     "agent-alpha",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			if got, ok := req.Params["requires_project_gate"].(bool); !ok || !got {
				t.Fatalf("requires_project_gate = %#v, want true; params=%+v", req.Params["requires_project_gate"], req.Params)
			}
			if got := rpcStringSlice(req.Params, "dependency_task_ids"); strings.Join(got, ",") != "task-api,task-schema" {
				t.Fatalf("dependency_task_ids = %#v, want task-api/task-schema; params=%+v", got, req.Params)
			}
			if got := rpcStringSlice(req.Params, "write_scope_hints"); strings.Join(got, ",") != "agent/**,docs/rhizome.md" {
				t.Fatalf("write_scope_hints = %#v, want agent/docs hints; params=%+v", got, req.Params)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok || len(requirements) == 0 {
				t.Fatalf("expected task_requirements object in params, got %+v", req.Params)
			}
			if got := strings.Join(taskSubmitTestStringSlice(requirements["required_work_modes"]), ","); got != "implementation,visual-qa" {
				t.Fatalf("required_work_modes = %q; requirements=%+v", got, requirements)
			}
			if got := strings.Join(taskSubmitTestStringSlice(requirements["preferred_tools"]), ","); got != "browser,chrome-devtools" {
				t.Fatalf("preferred_tools = %q; requirements=%+v", got, requirements)
			}
			if got := strings.Join(taskSubmitTestStringSlice(requirements["write_scope_hints"]), ","); got != "agent/**,docs/rhizome.md" {
				t.Fatalf("task_requirements.write_scope_hints = %q; requirements=%+v", got, requirements)
			}
			graph, ok := req.Params["graph"].(map[string]any)
			if !ok || len(graph) == 0 {
				t.Fatalf("expected graph object in params, got %+v", req.Params)
			}
			nodes, ok := graph["nodes"].([]any)
			if !ok || len(nodes) != 2 {
				t.Fatalf("expected graph nodes in params, got %+v", graph)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-build-backend",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			for key, want := range map[string]string{
				"workspace_id": "ws",
				"doc_key":      "task.task-build-backend",
				"title":        "Task Brief - Build backend",
				"updated_by":   "agent-alpha",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			content := rpcString(req.Params, "content")
			for _, want := range []string{"# Task Brief - Build backend", "- task_id: task-build-backend", "- project_id: project-alpha", "- project_lane: implementation", "- requires_project_gate: true", "- dependency_task_ids: task-api, task-schema", "- write_scope_hints: agent/**, docs/rhizome.md", "- task_requirements_schema: task_requirements.v1", "Implement generator backend", "## Task Fit Requirements", `"required_work_modes"`, `"preferred_tools"`, `"browser"`, "## Spec Fidelity", "project.project-alpha.acceptance_criteria", "project.project-alpha.product_contract", "project.project-alpha.plan_review", "core_user_promise", "adjacent wrong product", "acceptance-criteria IDs", "workspace docs"} {
				if !strings.Contains(content, want) {
					t.Fatalf("canonical doc content missing %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":               "task-build-backend",
		"title":                 "Build backend",
		"description":           "Implement generator backend",
		"priority":              "high",
		"task_kind":             "execution",
		"task_template":         "generic",
		"project_id":            "project-alpha",
		"project_lane":          "implementation",
		"requires_project_gate": true,
		"dependency_task_ids":   []any{"task-api", "task-schema"},
		"write_scope_hints":     []any{"agent/**", "docs/rhizome.md"},
		"required_work_modes":   []any{"implementation", "visual-qa"},
		"preferred_skills":      []any{"frontend", "browser-verification"},
		"preferred_tools":       []any{"browser", "chrome-devtools"},
		"graph": map[string]any{
			"nodes": []any{
				map[string]any{"node_id": "api", "type": "implementation"},
				map[string]any{"node_id": "review", "type": "review", "depends_on": []any{"api"}},
			},
		},
		"tags": []any{"backend", "generator"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	for _, want := range []string{"task-build-backend", "PENDING", "agent-alpha"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	for _, want := range []string{"project-alpha", "implementation", "requires_project_gate", "dependency_task_ids", "write_scope_hints", "task_requirements", "required_work_modes", "preferred_tools"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain project-scoped field %q, got %q", want, result.Output)
		}
	}
	for _, want := range []string{"selection_guidance", "frontier self-selection", "delegate_request_template", "Please inspect task task-build-backend", "project acceptance criteria"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain delegation guidance %q, got %q", want, result.Output)
		}
	}
	for _, want := range []string{"task.task-build-backend", "SAVED", "sha-task-doc"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain canonical doc %q, got %q", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected list, task submit, and canonical doc put, got %d calls", calls)
	}
}

func TestTaskSubmitSemanticWriteScopeHintsNarrowsLuaLexerAtBirth(t *testing.T) {
	requirements := taskSubmitRequirementsFromArgs(map[string]any{
		"task_requirements": map[string]any{
			"schema":                   "task_requirements.v1",
			"acceptance_criteria_refs": []any{"AC-LUA-LEX-01"},
		},
	}, []string{"cmd/**", "internal/**", "testdata/**", "tools/oracle/**", "scripts/**", "README.md"})

	got := taskSubmitSemanticWriteScopeHints(
		"task-1781622429496831800-2dab1f83",
		"Implement AC-LUA-LEX-01: Lua lexer subset",
		"Lex Lua 5.1 subset tokens and source positions.",
		"EXECUTION",
		"project-signal01-lua-capability",
		"implementation",
		[]string{"lua", "implementation"},
		requirements,
		[]string{"cmd/**", "internal/**", "testdata/**", "tools/oracle/**", "scripts/**", "README.md"},
	)
	for _, want := range []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**"} {
		if !stringSliceContainsFold(got, want) {
			t.Fatalf("expected Lua lexer task_submit scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"cmd/**", "internal/**", "testdata/**", "tools/oracle/**", "scripts/**", "README.md"} {
		if stringSliceContainsFold(got, forbidden) {
			t.Fatalf("Lua lexer task_submit scope must not keep broad/harness path %q, got %+v", forbidden, got)
		}
	}
}

func TestTaskSubmitToolRejectsProseWriteScopeHints(t *testing.T) {
	tool := NewTaskSubmitTool(NewRhizomeClient("http://127.0.0.1:1/rpc", "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":           "task-prose-scope",
		"title":             "Revise and publish the Clearpress shell/workspace candidate",
		"description":       "Operate on the existing Clearpress shell/workspace product slice.",
		"project_id":        "project-clearpress",
		"project_lane":      "implementation",
		"write_scope_hints": []any{"existing Clearpress shell/workspace checkout", "app shell", "routing", "review-ready evidence for the article workspace slice"},
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "not prose") {
		t.Fatalf("expected prose write_scope_hints rejection, got %+v", result)
	}
}

func TestTaskSubmitToolIgnoresProseWriteScopeForValidationDocTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_lane"); got != "validation" {
				t.Fatalf("project_lane = %q, want validation", got)
			}
			if got := rpcStringSlice(req.Params, "write_scope_hints"); len(got) != 0 {
				t.Fatalf("write_scope_hints = %#v, want empty after prose-only normalization", got)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("expected task_requirements object, got %+v", req.Params)
			}
			ignored := strings.Join(taskSubmitTestStringSlice(requirements["ignored_write_scope_hints"]), "\n")
			if !strings.Contains(ignored, "workspace docs only unless repair is proven necessary") || !strings.Contains(ignored, "project coordination docs only") {
				t.Fatalf("ignored_write_scope_hints missing prose diagnostics: %+v", requirements)
			}
			if got, _ := requirements["write_scope_hints_normalization"].(string); got != "ignored_prose_for_non_mutating_task" {
				t.Fatalf("write_scope_hints_normalization = %q; requirements=%+v", got, requirements)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-visual-evidence", "workspace_id": "ws", "status": "PENDING"})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "ignored_prose_for_non_mutating_task") {
				t.Fatalf("canonical doc missing ignored write-scope marker:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":           "task-visual-evidence",
		"title":             "Publish exact visual blocker packet",
		"description":       "Create workspace docs only evidence for the current visual validation blocker; no repo mutation is expected.",
		"priority":          "high",
		"task_kind":         "execution",
		"project_id":        "project-clearpress",
		"project_lane":      "validation",
		"write_scope_hints": []any{"workspace docs only unless repair is proven necessary", "project coordination docs only"},
		"tags":              []any{"validation", "visual", "evidence"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected validation docs task to ignore prose write scope, got %+v", result)
	}
	if !strings.Contains(result.Output, `"status": "PENDING"`) {
		t.Fatalf("unexpected result: %s", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and canonical doc put, got %d calls", calls)
	}
}

func TestTaskSubmitToolPersistsPatchQueueValidationLineage(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "workspace.doc.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			for key, want := range map[string]string{
				"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
				"patch_queue_task_kind":     "validation",
				"queue_id":                  "patchq-clearpress",
				"item_id":                   "patchitem-projbranch-123",
				"branch_id":                 "projbranch-123",
				"head_sha":                  "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f",
			} {
				if got, _ := requirements[key].(string); strings.TrimSpace(got) != want {
					t.Fatalf("requirements[%s]=%q want %q; requirements=%+v", key, got, want, requirements)
				}
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-visual", "workspace_id": "ws", "status": "PENDING"})
		case 4:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			for _, want := range []string{"patch_queue_task_identity", "patchitem-projbranch-123", "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"} {
				if !strings.Contains(content, want) {
					t.Fatalf("canonical doc missing %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-visual",
		"title":        "Publish exact visual evidence for head 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f",
		"description":  "Produce validation for queue_id: patchq-clearpress and patch candidate `patchitem-projbranch-123` on branch_id: projbranch-123 at head 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f.",
		"project_id":   "project-clearpress",
		"project_lane": "validation",
		"tags":         []any{"patch-queue", "visual"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected list/probe/submit/doc calls, got %d", calls)
	}
}

func TestTaskSubmitToolMarksCanonicalIntegrationValidationAsPatchQueueIntegration(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			for key, want := range map[string]string{
				"required_tool":         "project_patch_queue_integrate",
				"required_transition":   "project_patch_queue_integrate_then_full_product_verify",
				"patch_queue_task_kind": "integration",
				"integration_contract":  "canonical_integration_before_full_product_validation.v1",
			} {
				if got, _ := requirements[key].(string); strings.TrimSpace(got) != want {
					t.Fatalf("requirements[%s]=%q want %q; requirements=%+v", key, got, want, requirements)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-rq-canonical-integration",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			for _, want := range []string{"project_patch_queue_integrate", "canonical_integration_before_full_product_validation.v1", "patch_queue_task_kind"} {
				if !strings.Contains(content, want) {
					t.Fatalf("canonical doc missing %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "theta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-rq-canonical-integration",
		"title":        "Validate canonical rq integration and full-product evidence",
		"description":  "Verify exact branch/head provenance, run bounded build/test/smoke checks on the canonical target, and publish durable evidence of the assembled product state.",
		"task_kind":    "COORDINATION",
		"project_id":   "project-signal01-rq-root",
		"project_lane": "integration",
		"task_requirements": map[string]any{
			"evidence_needed":     []any{"exact branch/head SHA", "checkout path", "go build ./...", "go test ./..."},
			"preferred_tools":     []any{"shell", "browser_session"},
			"required_work_modes": []any{"validation", "review"},
		},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected canonical integration validation task submit to pass, got %+v", result)
	}
	if calls != 3 {
		t.Fatalf("expected list/submit/doc calls, got %d", calls)
	}
	for _, want := range []string{`"required_tool": "project_patch_queue_integrate"`, `"patch_queue_task_kind": "integration"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("output missing %q:\n%s", want, result.Output)
		}
	}
}

func TestTaskSubmitToolDoesNotMarkClaimStewardshipAsIntegration(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			for key, want := range map[string]string{
				"patch_queue_task_kind": "claim_stewardship",
				"queue_id":              "patchq-project-signal01-rq-p",
				"item_id":               "patchitem-projbranch-1781142360396584010-28",
				"branch_id":             "projbranch-1781142360396584010-28",
			} {
				if got, _ := requirements[key].(string); strings.TrimSpace(got) != want {
					t.Fatalf("requirements[%s]=%q want %q; requirements=%+v", key, got, want, requirements)
				}
			}
			if got, _ := requirements["required_tool"].(string); strings.TrimSpace(got) != "" {
				t.Fatalf("claim stewardship must not require integration tool, got requirements=%+v", requirements)
			}
			if got, _ := requirements["required_transition"].(string); strings.TrimSpace(got) == "project_patch_queue_integrate_then_full_product_verify" {
				t.Fatalf("claim stewardship must not carry integration transition, got requirements=%+v", requirements)
			}
			if got, _ := requirements["integration_contract"].(string); strings.TrimSpace(got) != "" {
				t.Fatalf("claim stewardship must not carry integration contract, got requirements=%+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-patchq-claim-stewardship-r58",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			if strings.Contains(content, "project_patch_queue_integrate") ||
				strings.Contains(content, "canonical_integration_before_full_product_validation.v1") {
				t.Fatalf("task doc must not restate integration-only gate for claim stewardship:\n%s", content)
			}
			if !strings.Contains(content, "claim_stewardship") {
				t.Fatalf("task doc missing structural claim stewardship kind:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "zeta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-patchq-claim-stewardship-r58",
		"title":        "Resolve claimed patch queue item lifecycle",
		"description":  "Patch queue claim stewardship task created from agent.work.next frontier.\n\nqueue_id: patchq-project-signal01-rq-p\nitem_id: patchitem-projbranch-1781142360396584010-28\nbranch_id: projbranch-1781142360396584010-28\nhead_sha: de68fb4c04db",
		"task_kind":    "EXECUTION",
		"project_id":   "project-signal01-rq-product-first",
		"project_lane": "integration",
		"tags":         []any{"project", "patch-queue", "integration", "queue-stewardship", "claim-stewardship", "claimed-decision"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected claim stewardship task submit to pass, got %+v", result)
	}
	if calls != 3 {
		t.Fatalf("expected list/submit/doc calls, got %d", calls)
	}
	if strings.Contains(result.Output, "project_patch_queue_integrate") {
		t.Fatalf("output must not advertise integration required_tool for claim stewardship: %s", result.Output)
	}
}

func TestTaskSubmitToolDoesNotMarkBlockedPatchQueueValidationAsIntegration(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "workspace.doc.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			for key, want := range map[string]string{
				"patch_queue_task_kind":       "validation",
				"state":                       "BLOCKED",
				"required_transition":         "project_patch_queue_submit_or_revision_followup",
				"blocked_validation_recovery": "same_head_evidence_or_bounded_revision",
			} {
				if got, _ := requirements[key].(string); strings.TrimSpace(got) != want {
					t.Fatalf("requirements[%s]=%q want %q; requirements=%+v", key, got, want, requirements)
				}
			}
			if got, _ := requirements["required_tool"].(string); strings.TrimSpace(got) != "" {
				t.Fatalf("blocked validation follow-up must not require integration tool, got requirements=%+v", requirements)
			}
			if got, _ := requirements["integration_contract"].(string); strings.TrimSpace(got) != "" {
				t.Fatalf("blocked validation follow-up must not carry canonical integration contract, got requirements=%+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-patchq-validation-rq",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 4:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			if strings.Contains(content, "project_patch_queue_integrate") ||
				strings.Contains(content, "canonical_integration_before_full_product_validation.v1") {
				t.Fatalf("task doc must not restate integration-only gate for blocked validation follow-up:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "zeta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-patchq-validation-rq",
		"title":        "Validate blocked integration candidate projbranch-1780945180974806609-111",
		"description":  "Patch queue decision follow-up for a BLOCKED item. Produce fresh exact branch/head validation evidence, then use project_patch_queue_lifecycle supersede/requeue if the blocked item can be reconsidered.",
		"task_kind":    "EXECUTION",
		"project_id":   "project-signal01-rq-product-first",
		"project_lane": "validation",
		"tags":         []any{"project", "patch-queue", "validation", "blocked"},
		"task_requirements": map[string]any{
			"schema":                      "task_requirements.v1",
			"patch_queue_task_identity":   "rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":       "validation",
			"repo_id":                     "repo-signal01-rq-core",
			"branch_id":                   "projbranch-1780945180974806609-111",
			"queue_id":                    "patchq-project-signal01-rq-product-first-repo-signal01-rq-core",
			"item_id":                     "patchitem-projbranch-1780945180974806609-111",
			"head_sha":                    "186def09f4dd88cf6375166e1fe26884db681a34",
			"state":                       "BLOCKED",
			"required_transition":         "project_patch_queue_submit_or_revision_followup",
			"blocked_validation_recovery": "same_head_evidence_or_bounded_revision",
			"write_scope_hints":           []any{"internal/eval/**"},
		},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected blocked validation task submit to pass, got %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected list/probe/submit/doc calls, got %d", calls)
	}
	if strings.Contains(result.Output, "project_patch_queue_integrate") {
		t.Fatalf("output must not advertise integration required_tool for blocked validation: %s", result.Output)
	}
}

func TestTaskSubmitToolDoesNotMarkAcceptedPatchQueueValidationAsIntegration(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "workspace.doc.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			for key, want := range map[string]string{
				"patch_queue_task_kind": "validation",
				"state":                 "ACCEPTED",
			} {
				if got, _ := requirements[key].(string); strings.TrimSpace(got) != want {
					t.Fatalf("requirements[%s]=%q want %q; requirements=%+v", key, got, want, requirements)
				}
			}
			if got, _ := requirements["required_tool"].(string); strings.TrimSpace(got) != "" {
				t.Fatalf("accepted validation follow-up must not require integration tool, got requirements=%+v", requirements)
			}
			if got, _ := requirements["required_transition"].(string); strings.TrimSpace(got) == "project_patch_queue_integrate_then_full_product_verify" {
				t.Fatalf("accepted validation follow-up must not carry integration-only transition, got requirements=%+v", requirements)
			}
			if got, _ := requirements["integration_contract"].(string); strings.TrimSpace(got) != "" {
				t.Fatalf("accepted validation follow-up must not carry canonical integration contract, got requirements=%+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-patchq-validation-rq",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 4:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			if strings.Contains(content, "project_patch_queue_integrate") ||
				strings.Contains(content, "canonical_integration_before_full_product_validation.v1") {
				t.Fatalf("task doc must not restate integration-only gate for accepted validation follow-up:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-patchq-validation-rq",
		"title":        "Validate accepted integration candidate projbranch-1780924866614389862-15",
		"description":  "Patch queue decision follow-up for an ACCEPTED item. Publish post-integration build/test evidence for the assembled product.",
		"task_kind":    "EXECUTION",
		"project_id":   "project-signal01-rq-product-first",
		"project_lane": "validation",
		"tags":         []any{"project", "patch-queue", "validation", "accepted"},
		"task_requirements": map[string]any{
			"schema":                    "task_requirements.v1",
			"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":     "validation",
			"repo_id":                   "repo-signal01-rq-core",
			"branch_id":                 "projbranch-1780924866614389862-15",
			"queue_id":                  "patchq-project-signal01-rq-product-first-repo-signal01-rq-core",
			"item_id":                   "patchitem-projbranch-1780924866614389862-15",
			"head_sha":                  "9415f3e2c0216738debfbdc620019fa4ef493328",
			"state":                     "ACCEPTED",
			"write_scope_hints":         []any{"internal/eval/**"},
		},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected accepted validation task submit to pass, got %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected list/probe/submit/doc calls, got %d", calls)
	}
	if strings.Contains(result.Output, "project_patch_queue_integrate") {
		t.Fatalf("output must not advertise integration required_tool for accepted validation: %s", result.Output)
	}
}

func TestTaskSubmitToolSuppressesDuplicatePatchQueueValidationTask(t *testing.T) {
	requirementsRaw, _ := json.Marshal(map[string]any{
		"schema":                    "task_requirements.v1",
		"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
		"patch_queue_task_kind":     "validation",
		"queue_id":                  "patchq-clearpress",
		"item_id":                   "patchitem-projbranch-123",
		"branch_id":                 "projbranch-123",
		"head_sha":                  "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f",
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{{
				"task_id":                "task-existing-visual",
				"title":                  "Validate blocked integration candidate projbranch-123",
				"status":                 "RUNNING",
				"project_id":             "project-clearpress",
				"project_lane":           "validation",
				"task_requirements_json": string(requirementsRaw),
			}}})
		default:
			t.Fatalf("duplicate patch queue validation task should stop before %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-new-sidecar",
		"title":        "Capture browser and source-fidelity evidence for Clearpress 4dc89d3",
		"description":  "Produce another visual packet for queue_id: patchq-clearpress, item_id: patchitem-projbranch-123, branch_id: projbranch-123, head_sha: 4dc89d3.",
		"project_id":   "project-clearpress",
		"project_lane": "validation",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected duplicate gate result, got %+v", result)
	}
	for _, want := range []string{"patch_queue_identity_duplicate", "task-existing-visual", "4dc89d3"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("duplicate result missing %q:\n%s", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only workspace task list call, got %d", calls)
	}
}

func TestTaskSubmitToolSuppressesGenericPatchQueueValidationAfterTerminalSameHeadTask(t *testing.T) {
	requirementsRaw, _ := json.Marshal(map[string]any{
		"schema":                    "task_requirements.v1",
		"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
		"patch_queue_task_kind":     "validation",
		"queue_id":                  "patchq-clearpress",
		"item_id":                   "patchitem-projbranch-123",
		"branch_id":                 "projbranch-123",
		"head_sha":                  "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f",
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{{
				"task_id":                "task-existing-terminal-visual",
				"title":                  "Validate blocked integration candidate projbranch-123",
				"status":                 "RESOLVED",
				"project_id":             "project-clearpress",
				"project_lane":           "validation",
				"task_requirements_json": string(requirementsRaw),
			}}})
		default:
			t.Fatalf("terminal patch queue validation duplicate should stop before %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-new-generic-sidecar",
		"title":        "Run Clearpress source and browser validation for 4dc89d3",
		"description":  "Create another visual/source validation task for queue_id: patchq-clearpress, item_id: patchitem-projbranch-123, branch_id: projbranch-123, head_sha: 4dc89d3.",
		"project_id":   "project-clearpress",
		"project_lane": "validation",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected terminal duplicate gate result, got %+v", result)
	}
	for _, want := range []string{"patch_queue_identity_duplicate", "task-existing-terminal-visual", `"terminal_existing": true`, "project_patch_queue_followup"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("terminal duplicate result missing %q:\n%s", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only workspace task list call, got %d", calls)
	}
}

func TestTaskSubmitToolSuppressesPatchQueueValidationWhenVisualEvidenceExists(t *testing.T) {
	const headSHA = "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	const evidenceDocKey = "task.task-visual-refresh.visual_acceptance"
	calls := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Clearpress Visual Acceptance - beta head 4dc89d3c fail",
			}}})
		case "workspace.doc.get":
			if got := rpcString(req.Params, "doc_key"); got != evidenceDocKey {
				t.Fatalf("unexpected doc get %q", got)
			}
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Clearpress Visual Acceptance - beta head 4dc89d3c fail",
				Content: `schema: rhizome_visual_acceptance_v1
queue_id: patchq-clearpress
item_id: patchitem-projbranch-123
branch_id: projbranch-123
head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
visual_verdict: fail
severity: blocking`,
			})
		default:
			t.Fatalf("visual evidence receipt should stop before %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-new-generic-sidecar",
		"title":        "Produce Clearpress AC-14/AC-15 validation evidence or exact blocker for latest candidate",
		"description":  "Produce visual evidence for queue_id: patchq-clearpress, item_id: patchitem-projbranch-123, branch_id: projbranch-123, head_sha: " + headSHA + ".",
		"project_id":   "project-clearpress",
		"project_lane": "validation",
		"tags":         []any{"visual", "validation"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected visual evidence receipt gate result, got %+v", result)
	}
	for _, want := range []string{"patch_queue_visual_evidence_exists", evidenceDocKey, `"visual_verdict": "fail"`, `"required_transition": "project_patch_queue_followup"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("visual receipt result missing %q:\n%s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "workspace.tasks.list,workspace.doc.list,workspace.doc.get" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestTaskSubmitToolSuppressesPatchQueueValidationWithTaskBoundVisualEvidence(t *testing.T) {
	const headSHA = "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	const evidenceDocKey = "task.task-visual-refresh.visual_acceptance"
	calls := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Clearpress Visual Acceptance - beta head 4dc89d3c provisional fail",
			}}})
		case "workspace.doc.get":
			if got := rpcString(req.Params, "doc_key"); got != evidenceDocKey {
				t.Fatalf("unexpected doc get %q", got)
			}
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Clearpress Visual Acceptance - beta head 4dc89d3c provisional fail",
				Content: `# rhizome_visual_acceptance_v1

- task_id: task-clearpress-visual-acceptance-beta-head-4dc89d3c-refresh
- project_id: project-clearpress
- candidate_head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
- candidate_kind: provisional_non_canonical_validation_checkout

## Verdict
- visual_verdict: fail
- severity: blocking`,
			})
		default:
			t.Fatalf("visual evidence receipt should stop before %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-new-generic-sidecar",
		"title":        "Produce Clearpress AC-14/AC-15 validation evidence or exact blocker for latest candidate",
		"description":  "Produce visual evidence for project project-clearpress, branch_id: projbranch-123, head_sha: " + headSHA + ".",
		"project_id":   "project-clearpress",
		"project_lane": "validation",
		"tags":         []any{"visual", "validation"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected visual evidence receipt gate result, got %+v", result)
	}
	for _, want := range []string{"patch_queue_visual_evidence_exists", evidenceDocKey, `"visual_verdict": "fail"`, `"required_transition": "project_patch_queue_followup"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("visual receipt result missing %q:\n%s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "workspace.tasks.list,workspace.doc.list,workspace.doc.get" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestPatchQueueVisualEvidenceMatchesProjectBoundHead(t *testing.T) {
	identity := taskSubmitPatchQueueIdentity{
		ProjectID: "project-clearpress",
		BranchID:  "projbranch-123",
		HeadSHA:   "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f",
	}
	doc := WorkspaceDocRecord{
		DocKey: "task.task-visual-refresh.visual_acceptance",
		Title:  "Clearpress Visual Acceptance - beta head 4dc89d3c provisional fail",
		Content: `# rhizome_visual_acceptance_v1

- project_id: project-clearpress
- candidate_head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
- visual_verdict: fail
- severity: blocking`,
	}
	if receipt, ok := patchQueueVisualEvidenceReceiptFromDoc(doc, identity); !ok || !receipt.Blocking {
		t.Fatalf("expected project-bound exact-head visual receipt, got ok=%v receipt=%+v", ok, receipt)
	}
}

func TestTaskSubmitPatchQueueIdentityFromVisualEvidenceRequestCarriesProjectHead(t *testing.T) {
	requirements := taskSubmitRequirementsFromArgs(map[string]any{
		"project_id":   "project-clearpress",
		"project_lane": "validation",
		"tags":         []any{"visual", "validation"},
	}, nil)
	identity := taskSubmitPatchQueueIdentityFromInput(
		"Produce Clearpress AC-14/AC-15 validation evidence or exact blocker for latest candidate",
		"Produce visual evidence for project project-clearpress, branch_id: projbranch-123, head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f.",
		"validation",
		[]string{"visual", "validation"},
		requirements,
	)
	identity.ProjectID = firstNonEmpty(identity.ProjectID, "project-clearpress")
	identity = taskSubmitNormalizePatchQueueIdentity(identity)
	if identity.ProjectID != "project-clearpress" || identity.HeadSHA != "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f" || identity.Kind != "validation" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if !taskSubmitPatchQueueIdentityActionable(identity) {
		t.Fatalf("expected actionable identity: %+v", identity)
	}
}

func TestTaskSubmitToolDoesNotUseTaskBoundVisualEvidenceFromDifferentProject(t *testing.T) {
	const headSHA = "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	const evidenceDocKey = "task.task-visual-refresh.visual_acceptance"
	calls := make([]string, 0, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Other project visual acceptance 4dc89d3c fail",
			}}})
		case "workspace.doc.get":
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Other project visual acceptance 4dc89d3c fail",
				Content: `schema: rhizome_visual_acceptance_v1
project_id: other-project
candidate_head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
visual_verdict: fail
severity: blocking`,
			})
		case "task.submit":
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-new"})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-new-generic-sidecar",
		"title":        "Produce Clearpress visual evidence for latest candidate",
		"description":  "Produce visual evidence for project project-clearpress, branch_id: projbranch-123, head_sha: " + headSHA + ".",
		"project_id":   "project-clearpress",
		"project_lane": "validation",
		"tags":         []any{"visual", "validation"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected task creation, got %+v", result)
	}
	if strings.Contains(result.Output, "patch_queue_visual_evidence_exists") {
		t.Fatalf("different-project visual evidence must not suppress task creation:\n%s", result.Output)
	}
	if !strings.Contains(result.Output, `"task_id": "task-new-generic-sidecar"`) || !strings.Contains(result.Output, `"status": "PENDING"`) {
		t.Fatalf("expected task creation, got %s", result.Output)
	}
	if strings.Join(calls, ",") != "workspace.tasks.list,workspace.doc.list,workspace.doc.get,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestTaskSubmitToolAllowsCanonicalPatchQueueRetryAfterTerminalSameHeadTask(t *testing.T) {
	requirementsRaw, _ := json.Marshal(map[string]any{
		"schema":                    "task_requirements.v1",
		"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
		"patch_queue_task_kind":     "validation",
		"queue_id":                  "patchq-clearpress",
		"item_id":                   "patchitem-projbranch-123",
		"branch_id":                 "projbranch-123",
		"head_sha":                  "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f",
	})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{{
				"task_id":                "task-existing-terminal-visual",
				"title":                  "Validate blocked integration candidate projbranch-123",
				"status":                 "RESOLVED",
				"project_id":             "project-clearpress",
				"project_lane":           "validation",
				"task_requirements_json": string(requirementsRaw),
			}}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); !strings.Contains(got, "retry") {
				t.Fatalf("expected retry task id, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-patchq-clearpress-validation-retry-001",
		"title":        "Retry Clearpress source and browser validation for 4dc89d3",
		"description":  "retry_of_terminal_followup_task: task-existing-terminal-visual\nRetry queue_id: patchq-clearpress, item_id: patchitem-projbranch-123, branch_id: projbranch-123, head_sha: 4dc89d3.",
		"project_id":   "project-clearpress",
		"project_lane": "validation",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected canonical retry task submit, got %+v", result)
	}
	for _, want := range []string{"task-patchq-clearpress-validation-retry-001", "PENDING"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("retry output missing %q:\n%s", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected list/submit/doc calls, got %d", calls)
	}
}

func taskSubmitTestStringSlice(raw any) []string {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if str, ok := item.(string); ok {
			out = append(out, str)
		}
	}
	return out
}

func taskSubmitTestStringSliceContains(values []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	for _, value := range values {
		if strings.ToLower(strings.TrimSpace(value)) == want {
			return true
		}
	}
	return false
}

func TestTaskSubmitToolAutoDelegatesSuggestedImplementationTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":          "task-build-panel",
				"workspace_id":     "ws",
				"status":           "PENDING",
				"suggested_agents": []string{"agent-alpha", "agent-beta"},
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		case 4:
			if req.Method != "agent.request" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "from_agent_id"); got != "agent-alpha" {
				t.Fatalf("from_agent_id=%q, want agent-alpha; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "to_agent_id"); got != "agent-beta" {
				t.Fatalf("to_agent_id=%q, want first non-self suggestion agent-beta; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "method"); got != "model.ask" {
				t.Fatalf("method=%q, want model.ask", got)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"delegate_task", "task-build-panel", "task_submit.auto_delegate", "Please inspect task task-build-panel"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("delegation payload missing %q:\n%s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{"request_id": "req-delegate-1", "workspace_id": "ws", "to_agent_id": "agent-beta", "status": "PENDING"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":       "task-build-panel",
		"title":         "Build sprite preview panel",
		"description":   "Implement the preview panel against the project product contract.",
		"task_kind":     "execution",
		"project_id":    "project-sprite",
		"project_lane":  "implementation",
		"auto_delegate": true,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	for _, want := range []string{"auto_delegate", "QUEUED", "req-delegate-1", "agent-beta"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 4 {
		t.Fatalf("expected list, submit, doc put, and agent.request, got %d calls", calls)
	}
}

func TestTaskSubmitToolAutoDelegateCanBeDisabled(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":          "task-build-disabled",
				"workspace_id":     "ws",
				"status":           "PENDING",
				"suggested_agents": []string{"agent-beta"},
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		case 4:
			t.Fatalf("auto_delegate=false should not queue agent.request, got %s", req.Method)
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":       "task-build-disabled",
		"title":         "Build disabled delegation lane",
		"description":   "Implementation lane that the creating agent will delegate manually.",
		"task_kind":     "execution",
		"project_id":    "project-sprite",
		"project_lane":  "implementation",
		"auto_delegate": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	if strings.Contains(result.Output, "auto_delegate") {
		t.Fatalf("auto_delegate=false should omit auto_delegate result, got %q", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and doc put only, got %d calls", calls)
	}
}

func TestTaskSubmitToolReportsCanonicalDocFailureWithoutHidingCreatedTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-build-ui",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "doc write denied")
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":     "task-build-ui",
		"title":       "Build UI",
		"description": "Implement dashboard UI",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected partial failure result, got %+v", result)
	}
	for _, want := range []string{"task-build-ui", "canonical_doc_status", "FAILED", "do not retry task_submit blindly"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected list, task submit, and failed canonical doc put, got %d calls", calls)
	}
}

func TestTaskSubmitToolDropsRunningProjectRootContextDependency(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				{
					"task_id":       "root-icon-sprite-forge",
					"title":         "Icon Sprite Forge root",
					"status":        "RUNNING",
					"task_kind":     "COORDINATION",
					"task_template": "project",
					"project_id":    "icon-sprite-forge",
					"project_lane":  "strategy",
				},
				{
					"task_id":       "task-gamma-branch",
					"title":         "Gamma branch must finish first",
					"status":        "PENDING",
					"task_kind":     "EXECUTION",
					"task_template": "generic",
					"project_id":    "icon-sprite-forge",
					"project_lane":  "implementation",
				},
			}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcStringSlice(req.Params, "dependency_task_ids"); strings.Join(got, ",") != "task-gamma-branch" {
				t.Fatalf("dependency_task_ids = %#v, want only concrete dependency; params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-followup", "workspace_id": "ws", "status": "PENDING"})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			if strings.Contains(content, "root-icon-sprite-forge") {
				t.Fatalf("canonical task brief should not preserve dropped root dependency:\n%s", content)
			}
			if !strings.Contains(content, "- dependency_task_ids: task-gamma-branch") {
				t.Fatalf("canonical task brief should preserve concrete dependency:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-followup"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":             "task-followup",
		"title":               "Repair publication handoff",
		"description":         "Create a concrete publication repair lane.",
		"project_id":          "icon-sprite-forge",
		"project_lane":        "coordination",
		"dependency_task_ids": []any{"root-icon-sprite-forge", "task-gamma-branch"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	for _, want := range []string{"dropped_dependency_task_ids", "root-icon-sprite-forge", "context anchors"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain dropped dependency guidance %q, got %q", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolDemotesSiblingImplementationDependencyToAdvisory(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				{
					"task_id":       "task-core-parser",
					"title":         "Build core parser",
					"status":        "RUNNING",
					"task_kind":     "EXECUTION",
					"task_template": "generic",
					"project_id":    "project-rq",
					"project_lane":  "implementation",
				},
			}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcStringSlice(req.Params, "dependency_task_ids"); len(got) != 0 {
				t.Fatalf("dependency_task_ids = %#v, want no hard dependency; params=%+v", got, req.Params)
			}
			if got := rpcStringSlice(req.Params, "related_task_ids"); strings.Join(got, ",") != "task-core-parser" {
				t.Fatalf("related_task_ids = %#v, want non-blocking related link; params=%+v", got, req.Params)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("expected advisory task_requirements object, got %+v", req.Params)
			}
			if got := strings.Join(taskSubmitTestStringSlice(requirements["advisory_dependency_task_ids"]), ","); got != "task-core-parser" {
				t.Fatalf("advisory_dependency_task_ids = %q; requirements=%+v", got, requirements)
			}
			if got := strings.Join(taskSubmitTestStringSlice(requirements["demoted_dependency_task_ids"]), ","); got != "task-core-parser" {
				t.Fatalf("demoted_dependency_task_ids = %q; requirements=%+v", got, requirements)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-evaluator", "workspace_id": "ws", "status": "PENDING"})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			if strings.Contains(content, "- dependency_task_ids: task-core-parser") {
				t.Fatalf("canonical task brief should not preserve demoted sibling as hard dependency:\n%s", content)
			}
			for _, want := range []string{"advisory_dependency_task_ids", "demoted_dependency_task_ids", "task-core-parser"} {
				if !strings.Contains(content, want) {
					t.Fatalf("canonical task brief missing advisory dependency marker %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-evaluator"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":             "task-evaluator",
		"title":               "Build rq evaluator",
		"description":         "Implement evaluator against the parser interface; parser sequencing is preferred but evaluator can start from the contract.",
		"project_id":          "project-rq",
		"project_lane":        "implementation",
		"dependency_task_ids": []any{"task-core-parser"},
		"write_scope_hints":   []any{"internal/evaluator/**"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	for _, want := range []string{"advisory_dependency_task_ids", "related_task_ids", "demoted_dependency_task_ids", "task-core-parser", "hard_dependency_task_ids"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolKeepsExplicitHardSiblingImplementationDependency(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				{
					"task_id":       "task-core-parser",
					"title":         "Build core parser",
					"status":        "RUNNING",
					"task_kind":     "EXECUTION",
					"task_template": "generic",
					"project_id":    "project-rq",
					"project_lane":  "implementation",
				},
			}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcStringSlice(req.Params, "dependency_task_ids"); strings.Join(got, ",") != "task-core-parser" {
				t.Fatalf("dependency_task_ids = %#v, want explicit hard dependency; params=%+v", got, req.Params)
			}
			if got := rpcStringSlice(req.Params, "related_task_ids"); len(got) != 0 {
				t.Fatalf("related_task_ids = %#v, want no advisory link for explicit hard dependency; params=%+v", got, req.Params)
			}
			if requirements, ok := req.Params["task_requirements"].(map[string]any); ok {
				if got := taskSubmitTestStringSlice(requirements["demoted_dependency_task_ids"]); len(got) != 0 {
					t.Fatalf("explicit hard dependency should not be demoted; requirements=%+v", requirements)
				}
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-evaluator", "workspace_id": "ws", "status": "PENDING"})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "- dependency_task_ids: task-core-parser") {
				t.Fatalf("canonical task brief should preserve explicit hard dependency:\n%s", content)
			}
			if strings.Contains(content, "demoted_dependency_task_ids") {
				t.Fatalf("canonical task brief should not mark explicit hard dependency as demoted:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-evaluator"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":                  "task-evaluator",
		"title":                    "Build rq evaluator",
		"description":              "Implement evaluator only after the parser contract is complete.",
		"project_id":               "project-rq",
		"project_lane":             "implementation",
		"hard_dependency_task_ids": []any{"task-core-parser"},
		"write_scope_hints":        []any{"internal/evaluator/**"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	if !strings.Contains(result.Output, "dependency_task_ids") || strings.Contains(result.Output, "related_task_ids") || strings.Contains(result.Output, "demoted_dependency_task_ids") {
		t.Fatalf("expected hard dependency without demotion, got %q", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolMapsProjectPhaseTaskKindToLane(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_kind"); got != "EXECUTION" {
				t.Fatalf("task_kind = %q, want EXECUTION; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("project_lane = %q, want implementation; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-alpha" {
				t.Fatalf("project_id = %q, want project-alpha; params=%+v", got, req.Params)
			}
			if got, ok := req.Params["requires_project_gate"].(bool); !ok || !got {
				t.Fatalf("requires_project_gate = %#v, want auto-enforced true; params=%+v", req.Params["requires_project_gate"], req.Params)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-phase-kind", "workspace_id": "ws", "status": "PENDING"})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			content := rpcString(req.Params, "content")
			for _, want := range []string{"- task_kind: EXECUTION", "- project_id: project-alpha", "- project_lane: implementation", "- requires_project_gate: true"} {
				if !strings.Contains(content, want) {
					t.Fatalf("canonical doc content missing %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-phase-kind"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":               "task-phase-kind",
		"title":                 "Implement slice",
		"description":           "Build a bounded implementation slice.",
		"task_kind":             "implementation",
		"project_id":            "project-alpha",
		"requires_project_gate": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful task submit, got %+v", result)
	}
	for _, want := range []string{`"requires_project_gate": true`, `"project_gate_enforced": true`, "hard-gated"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain enforced gate marker %q, got %q", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected list, task submit, and canonical doc put, got %d calls", calls)
	}
}

func TestTaskSubmitToolRejectsImplementationLaneMetaTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Fatalf("task_submit meta-task validation should fail before RPC call")
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "beta", "owner-1")
	cases := []struct {
		name        string
		title       string
		description string
	}{
		{
			name:        "observed russian lane opener",
			title:       "Открыть implementation lane для локального sub-pixel image processor",
			description: "Создать implementation-shaped задачу, чтобы beta мог зарегистрировать checkout/branch и перейти к локальной реализации. Ожидаемый результат: CLAIMED implementation task с write scope app/**, src/**, tests/**; после этого beta должен вызвать project_checkout_materialize.",
		},
		{
			name:        "english gate opener",
			title:       "Open implementation lane for local image processor",
			description: "Set up implementation lane admission, materialize checkout, register branch evidence, and satisfy project gates before the real work starts.",
		},
		{
			name:        "candidate provenance without product work",
			title:       "Publish Clearpress runnable candidate provenance and review-ready evidence",
			description: "Create exact branch/head/checkout/command/server provenance so browser acceptance can run.",
		},
		{
			name:        "qa candidate publication without product work",
			title:       "Publish exact runnable Clearpress candidate for QA validation",
			description: "Confirm the current shared branch and publish candidate provenance for QA validation.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := tool.Execute(context.Background(), map[string]any{
				"task_id":      "task-open-implementation-lane",
				"title":        tc.title,
				"description":  tc.description,
				"task_kind":    "EXECUTION",
				"project_id":   "project-subpixel",
				"project_lane": "implementation",
			})
			if result == nil || !result.IsError {
				t.Fatalf("expected implementation-lane meta task rejection, got %+v", result)
			}
			if !strings.Contains(result.Output, "implementation-lane meta task") && !strings.Contains(result.Output, "publication/provenance-only implementation task") {
				t.Fatalf("expected implementation meta-task rejection, got %q", result.Output)
			}
			if !strings.Contains(result.Output, "product/code") {
				t.Fatalf("expected rejection output to mention product/code work, got %q", result.Output)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("expected no RPC calls after local validation, got %d", calls)
	}
}

func TestTaskSubmitToolAllowsProductImplementationTaskWithCheckoutProductWord(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "title"); got != "Implement checkout page preview UI" {
				t.Fatalf("title = %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-checkout-page",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-checkout-page"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-checkout-page",
		"title":        "Implement checkout page preview UI",
		"description":  "Build the checkout page preview UI with runnable component files and smoke evidence.",
		"task_kind":    "EXECUTION",
		"project_id":   "project-storefront",
		"project_lane": "implementation",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected product implementation task to pass, got %+v", result)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolAllowsProductImplementationTaskWithReviewEvidence(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-build-editor",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-build-editor"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-build-editor",
		"title":        "Build Clearpress editor shell and publish review-ready evidence",
		"description":  "Implement the editor UI, article route, and local persistence before publishing branch/head provenance for review.",
		"task_kind":    "EXECUTION",
		"project_id":   "project-clearpress",
		"project_lane": "implementation",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected product implementation task with evidence requirement to pass, got %+v", result)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolBlocksGenericCoordinationSplitWhenProductLaneOpen(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "workspace.tasks.list" {
			t.Fatalf("product-lane liveness gate should only list tasks, got %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"tasks": []map[string]any{{
				"task_id":       "task-rq-eval",
				"title":         "Repair rq internal/eval import resolution",
				"description":   "Fix the concrete rq internal/eval import resolution failure.",
				"status":        "PENDING",
				"priority":      "high",
				"task_kind":     "EXECUTION",
				"project_id":    "project-rq",
				"project_lane":  "implementation",
				"linked_by":     "gamma",
				"owner_user_id": "owner-1",
			}},
		})
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-rq-coordinate-more",
		"title":        "Coordinate rq repair follow-up",
		"description":  "Split another coordination lane for rq internal/eval import resolution before product work starts.",
		"priority":     "high",
		"task_kind":    "COORDINATION",
		"project_id":   "project-rq",
		"project_lane": "coordination",
		"tags":         []string{"coordination", "split"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected product-lane liveness gate to return non-error guidance, got %+v", result)
	}
	for _, want := range []string{`"task_submit_gate": "product_lane_liveness"`, `"task-rq-eval"`, `"required_transition": "claim_or_delegate_visible_product_lane"`, "Do not create another generic coordination split"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only task list call, got %d", calls)
	}
}

func TestTaskSubmitOpenProductTasksSkipsPathlessABPCRecoveryResidue(t *testing.T) {
	pathlessRequirements, _ := json.Marshal(map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_foundation",
		"action_kind":                     "split_foundation_bucket",
		"project_id":                      "project-lua",
		"branch_id":                       "projbranch-old",
		"active_task_id":                  "task-old",
		"side_effect_refs":                []string{"side-effect:ws:project-lua:projbranch-old:cmd-glua-main.go"},
		"dirty_paths":                     []string{},
		"path_bucket":                     []string{},
		"write_scope_hints":               []string{},
		"write_scope_hints_authoritative": true,
	})
	pathBoundRequirements, _ := json.Marshal(map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_foundation",
		"action_kind":                     "split_foundation_bucket",
		"project_id":                      "project-lua",
		"branch_id":                       "projbranch-current",
		"active_task_id":                  "task-current",
		"side_effect_refs":                []string{"side-effect:cmd-glua-main"},
		"dirty_paths":                     []string{"cmd/glua/main.go"},
		"path_bucket":                     []string{"cmd/glua/main.go"},
		"write_scope_hints":               []string{"cmd/glua/main.go"},
		"write_scope_hints_authoritative": true,
	})
	tasks := []WorkspaceTaskRecord{
		{
			TaskID:               "task-side-effect-pathless-r14",
			Status:               "PENDING",
			TaskKind:             "EXECUTION",
			ProjectID:            "project-lua",
			ProjectLane:          "implementation",
			TaskRequirementsJSON: string(pathlessRequirements),
		},
		{
			TaskID:               "task-side-effect-path-bound",
			Status:               "PENDING",
			TaskKind:             "EXECUTION",
			ProjectID:            "project-lua",
			ProjectLane:          "implementation",
			TaskRequirementsJSON: string(pathBoundRequirements),
		},
		{
			TaskID:      "task-lua-parser",
			Status:      "PENDING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-lua",
			ProjectLane: "implementation",
		},
	}

	open := taskSubmitOpenProductTasks(tasks, true, "project-lua")
	var ids []string
	for _, task := range open {
		ids = append(ids, strings.TrimSpace(task.TaskID))
	}
	joined := strings.Join(ids, ",")
	if strings.Contains(joined, "task-side-effect-pathless-r14") {
		t.Fatalf("pathless ABPC recovery residue must not create product-pressure, got %v", ids)
	}
	for _, want := range []string{"task-side-effect-path-bound", "task-lua-parser"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected product task %s to remain visible, got %v", want, ids)
		}
	}
}

func TestTaskSubmitToolAllowsDedicatedAuthorityCarrierUnderProductPressure(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"tasks": []map[string]any{{
					"task_id":       "task-rq-eval",
					"title":         "Repair rq internal/eval import resolution",
					"description":   "Fix the concrete rq internal/eval import resolution failure.",
					"status":        "PENDING",
					"priority":      "high",
					"task_kind":     "EXECUTION",
					"project_id":    "project-rq",
					"project_lane":  "implementation",
					"linked_by":     "gamma",
					"owner_user_id": "owner-1",
				}},
			})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-role-scope-beta" {
				t.Fatalf("task_id = %q, want task-role-scope-beta; params=%+v", got, req.Params)
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if got := taskSubmitRequirementString(requirements, "schema"); got != projectRoleScopeAuthorityTransitionSchema {
				t.Fatalf("schema = %q, want %q; requirements=%+v", got, projectRoleScopeAuthorityTransitionSchema, requirements)
			}
			if got := taskSubmitRequirementString(requirements, "required_transition"); got != projectRoleScopeAuthorityTransitionTool {
				t.Fatalf("required_transition = %q, want %q; requirements=%+v", got, projectRoleScopeAuthorityTransitionTool, requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-role-scope-beta",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-role-scope-beta",
		"title":        "Resolve project role/scope request for beta",
		"description":  "# Strategic lead role/scope request\nAssign beta the missing write scope needed to unblock the visible product lane.",
		"priority":     "high",
		"task_kind":    "COORDINATION",
		"project_id":   "project-rq",
		"project_lane": "coordination",
		"tags":         []string{"project-role-scope", "coordination"},
	})
	if result == nil || result.IsError {
		t.Fatalf("expected dedicated authority carrier to bypass product-lane gate, got %+v", result)
	}
	if strings.Contains(result.Output, "product_lane_liveness") {
		t.Fatalf("authority carrier should not be blocked by product-lane liveness gate: %s", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolCanonicalizesRoleScopeAuthorityCarrierFromActiveProject(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"tasks": []map[string]any{{
					"task_id":      "task-lua-parser-frontier",
					"title":        "Advance Lua parser front",
					"description":  "Implement the next parser increment.",
					"status":       "PENDING",
					"task_kind":    "EXECUTION",
					"project_id":   "project-lua",
					"project_lane": "implementation",
				}},
			})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-role-scope-lua-lexer-parser-front" {
				t.Fatalf("task_id = %q; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-lua" {
				t.Fatalf("project_id = %q, want project-lua; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "task_kind"); got != "COORDINATION" {
				t.Fatalf("task_kind = %q, want COORDINATION; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "project_lane"); got != "coordination" {
				t.Fatalf("project_lane = %q, want coordination; params=%+v", got, req.Params)
			}
			tags := rpcStringSlice(req.Params, "tags")
			for _, want := range []string{"project-role-scope", "authority-transition", "strategic-lead", "coordination", "blocker-unblock"} {
				if !taskSubmitTestStringSliceContains(tags, want) {
					t.Fatalf("tags missing %q: %#v", want, tags)
				}
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if got := taskSubmitRequirementString(requirements, "schema"); got != projectRoleScopeAuthorityTransitionSchema {
				t.Fatalf("schema = %q, want %q; requirements=%+v", got, projectRoleScopeAuthorityTransitionSchema, requirements)
			}
			if got := taskSubmitRequirementString(requirements, "project_id"); got != "project-lua" {
				t.Fatalf("requirements.project_id = %q, want project-lua; requirements=%+v", got, requirements)
			}
			if got := taskSubmitRequirementString(requirements, "required_transition"); got != projectRoleScopeAuthorityTransitionTool {
				t.Fatalf("required_transition = %q, want %q; requirements=%+v", got, projectRoleScopeAuthorityTransitionTool, requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-role-scope-lua-lexer-parser-front",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "gamma", "owner-1").WithRuntimeBinding(func() AgentRuntimeBinding {
		return AgentRuntimeBinding{ProjectID: "project-lua"}
	})
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-role-scope-lua-lexer-parser-front",
		"title":        "Repair Lua lexer/parser authority boundary",
		"description":  "Create the scope grant carrier needed after side-effect verification.",
		"priority":     "high",
		"task_kind":    "COORDINATION",
		"project_lane": "coordination",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected canonical role-scope carrier submit, got %+v", result)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolCanonicalizesProjectClaimRepairAuthorityCarrierFromActiveProject(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"tasks": []map[string]any{{
					"task_id":      "task-lua-parser-frontier",
					"title":        "Advance Lua parser front",
					"description":  "Implement the next parser increment.",
					"status":       "PENDING",
					"task_kind":    "EXECUTION",
					"project_id":   "project-lua",
					"project_lane": "implementation",
				}},
			})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-lua" {
				t.Fatalf("project_id = %q, want project-lua; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "task_kind"); got != "COORDINATION" {
				t.Fatalf("task_kind = %q, want COORDINATION; params=%+v", got, req.Params)
			}
			if got := rpcString(req.Params, "project_lane"); got != "strategy" {
				t.Fatalf("project_lane = %q, want strategy; params=%+v", got, req.Params)
			}
			tags := rpcStringSlice(req.Params, "tags")
			for _, want := range []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"} {
				if !taskSubmitTestStringSliceContains(tags, want) {
					t.Fatalf("tags missing %q: %#v", want, tags)
				}
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if got := taskSubmitRequirementString(requirements, "schema"); got != "project_claim_repair_task_v1" {
				t.Fatalf("schema = %q, want project_claim_repair_task_v1; requirements=%+v", got, requirements)
			}
			if got := taskSubmitRequirementString(requirements, "project_id"); got != "project-lua" {
				t.Fatalf("requirements.project_id = %q, want project-lua; requirements=%+v", got, requirements)
			}
			if got := taskSubmitRequirementString(requirements, "required_transition"); got != "project_claim_repair_receipt" {
				t.Fatalf("required_transition = %q, want project_claim_repair_receipt; requirements=%+v", got, requirements)
			}
			if got, ok := requirements["project_claim_repair"].(bool); !ok || !got {
				t.Fatalf("project_claim_repair = %#v, want true; requirements=%+v", requirements["project_claim_repair"], requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-project-claim-repair-lua-lexer-parser-front",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "gamma", "owner-1").WithRuntimeBinding(func() AgentRuntimeBinding {
		return AgentRuntimeBinding{ProjectID: "project-lua"}
	})
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":     "task-project-claim-repair-lua-lexer-parser-front",
		"title":       "Repair Lua lexer/parser project claim boundary",
		"description": "Open the project claim repair carrier needed after side-effect verification.",
		"priority":    "high",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected canonical project claim repair carrier submit, got %+v", result)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolRejectsAuthorityCarrierWithoutProjectContext(t *testing.T) {
	tool := NewTaskSubmitTool(NewRhizomeClient("http://127.0.0.1:1/rpc", "token"), "ws", "gamma", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":     "task-role-scope-lua-lexer-parser-front",
		"title":       "Repair Lua lexer/parser authority boundary",
		"description": "Create the scope grant carrier needed after side-effect verification.",
		"priority":    "high",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing project context rejection, got %+v", result)
	}
	for _, want := range []string{"authority_repair_requires_project", "project_id", `"created": false`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestTaskSubmitToolRejectsNewTaskDuringActiveProjectClaim(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Fatalf("task_submit should not issue RPC while active claim follow-through gate is closed")
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "delta", "owner-1").WithRuntimeBinding(func() AgentRuntimeBinding {
		return AgentRuntimeBinding{
			TaskID:              "task-active-eval",
			ProjectID:           "project-lua",
			ClaimRepoID:         "repo-lua",
			ClaimCheckoutID:     "checkout-active",
			ClaimBranchID:       "branch-active",
			ClaimWriteScopeJSON: `{"paths":["internal/eval/**","internal/runtime/**"]}`,
		}
	})
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":                      "task-followup-eval-split",
		"title":                        "Split eval runtime follow-up",
		"description":                  "Create a successor task instead of finishing the active eval claim.",
		"priority":                     "high",
		"task_kind":                    "EXECUTION",
		"project_id":                   "project-lua",
		"project_lane":                 "implementation",
		"advisory_dependency_task_ids": []any{"task-active-eval"},
		"write_scope_hints":            []any{"internal/eval/**"},
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected active claim follow-through rejection, got %+v", result)
	}
	for _, want := range []string{"active_claim_follow_through", "task-active-eval", "implement_current_claim_or_publish_terminal_blocker", `"created": false`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 0 {
		t.Fatalf("expected no RPC calls, got %d", calls)
	}
}

func TestTaskSubmitToolSkipsSimilarActiveTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "workspace.tasks.list" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"tasks": []map[string]any{{
				"task_id":       "task-existing-ui",
				"title":         "Build dashboard UI",
				"description":   "Implement local dashboard controls and preview panel",
				"status":        "PENDING",
				"priority":      "high",
				"task_kind":     "EXECUTION",
				"linked_by":     "alpha",
				"owner_user_id": "owner-1",
			}},
		})
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"title":       "Build dashboard UI",
		"description": "Create local dashboard controls and preview panel for the artifact.",
		"priority":    "high",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected duplicate guard to return non-error guidance, got %+v", result)
	}
	for _, want := range []string{"similar_active_task", "task-existing-ui", "Do not create a duplicate task", `"created": false`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only task list call, got %d", calls)
	}
}

func TestTaskSubmitToolExactTerminalTaskIDIsIdempotent(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "workspace.tasks.list" {
			t.Fatalf("exact task-id idempotence should only list tasks, got %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"tasks": []map[string]any{{
				"task_id":       "task-patchq-integration-existing",
				"title":         "Integrate accepted candidate",
				"description":   "Existing deterministic follow-up.",
				"status":        "RESOLVED",
				"priority":      "high",
				"task_kind":     "EXECUTION",
				"project_id":    "project-alpha",
				"project_lane":  "integration",
				"linked_by":     "beta",
				"owner_user_id": "owner-1",
			}},
		})
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":      "task-patchq-integration-existing",
		"title":        "Integrate accepted candidate",
		"description":  "Create the deterministic integration follow-up.",
		"priority":     "high",
		"project_id":   "project-alpha",
		"project_lane": "integration",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected exact terminal task id to return idempotent no-op, got %+v", result)
	}
	for _, want := range []string{`"created": false`, `"task_submit_gate": "exact_task_id_exists"`, `"existing_status": "RESOLVED"`, `"terminal_existing": true`, "Do not recreate or claim it"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only task list call, got %d", calls)
	}
}

func TestTaskSubmitToolDedupeIsProjectScoped(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"tasks": []map[string]any{{
					"task_id":       "task-existing-ui",
					"title":         "Build dashboard UI",
					"description":   "Implement local dashboard controls and preview panel",
					"status":        "PENDING",
					"priority":      "high",
					"task_kind":     "EXECUTION",
					"project_id":    "project-old",
					"linked_by":     "alpha",
					"owner_user_id": "owner-1",
				}},
			})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-new" {
				t.Fatalf("project_id = %q, want project-new; params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-new-ui",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-task-doc"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewTaskSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":     "task-new-ui",
		"title":       "Build dashboard UI",
		"description": "Create local dashboard controls and preview panel for the new project.",
		"priority":    "high",
		"project_id":  "project-new",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected project-scoped duplicate guard to allow new task, got %+v", result)
	}
	if strings.Contains(result.Output, "similar_active_task") {
		t.Fatalf("project-scoped duplicate guard should not suppress unrelated project work: %s", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected list, submit, and doc put calls, got %d", calls)
	}
}

func TestTaskSubmitToolValidatesRequiredFields(t *testing.T) {
	tool := NewTaskSubmitTool(NewRhizomeClient("http://127.0.0.1:1/rpc", "token"), "ws", "agent-alpha", "owner-1")
	for _, tc := range []struct {
		name string
		args map[string]any
		want string
	}{
		{name: "missing title", args: map[string]any{"description": "body"}, want: "title is required"},
		{name: "missing description", args: map[string]any{"title": "title"}, want: "description is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := tool.Execute(context.Background(), tc.args)
			if result == nil || !result.IsError || !strings.Contains(result.Output, tc.want) {
				t.Fatalf("expected validation error %q, got %+v", tc.want, result)
			}
		})
	}
}

func TestTaskSubmitToolParametersExposeProjectScopedFields(t *testing.T) {
	params := NewTaskSubmitTool(nil, "", "", "").Parameters()
	properties, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected properties map, got %+v", params)
	}
	for _, key := range []string{
		"project_id",
		"task_kind",
		"project_lane",
		"requires_project_gate",
		"dependency_task_ids",
		"hard_dependency_task_ids",
		"advisory_dependency_task_ids",
		"write_scope_hints",
		"graph",
	} {
		if _, ok := properties[key]; !ok {
			t.Fatalf("expected task_submit schema to expose %s, got %+v", key, properties)
		}
	}
	if propertyType(properties, "requires_project_gate") != "boolean" {
		t.Fatalf("requires_project_gate should be boolean, got %+v", properties["requires_project_gate"])
	}
	if propertyType(properties, "graph") != "object" {
		t.Fatalf("graph should be object, got %+v", properties["graph"])
	}
}

func propertyType(properties map[string]any, key string) string {
	property, _ := properties[key].(map[string]any)
	kind, _ := property["type"].(string)
	return kind
}
