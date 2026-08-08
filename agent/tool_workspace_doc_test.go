package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkspaceDocGetToolReadsCanonicalDoc(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		if got := rpcString(req.Params, "doc_key"); got != "pilot.spec" {
			t.Fatalf("doc_key = %q, want pilot.spec", got)
		}
		writeRPCResult(w, req, map[string]any{
			"doc_key":    "pilot.spec",
			"title":      "Pilot Spec",
			"content":    "canonical spec content",
			"updated_at": "2026-04-26T00:00:00Z",
		})
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws")
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "pilot.spec"})
	if result == nil || result.IsError {
		t.Fatalf("expected successful doc read, got %+v", result)
	}
	if !strings.Contains(result.Output, "canonical spec content") {
		t.Fatalf("expected doc content in output, got %q", result.Output)
	}
}

func TestWorkspaceDocGetToolCanonicalizesDocPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		if got := rpcString(req.Params, "doc_key"); got != "pilot.spec" {
			t.Fatalf("doc_key = %q, want pilot.spec", got)
		}
		writeRPCResult(w, req, map[string]any{
			"doc_key":    "pilot.spec",
			"title":      "Pilot Spec",
			"content":    "canonical spec content",
			"updated_at": "2026-04-26T00:00:00Z",
		})
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws")
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "doc:doc:pilot.spec"})
	if result == nil || result.IsError {
		t.Fatalf("expected successful doc read, got %+v", result)
	}
	for _, want := range []string{"pilot.spec", "canonicalized_from", "doc:doc:pilot.spec"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocGetToolFallsBackToLegacyDocPrefixedRow(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		docKey := rpcString(req.Params, "doc_key")
		calls = append(calls, docKey)
		switch docKey {
		case "pilot.spec":
			writeRPCError(w, req, -32000, "workspace doc not found: ws/pilot.spec")
		case "doc:pilot.spec":
			writeRPCResult(w, req, map[string]any{
				"doc_key":    "doc:pilot.spec",
				"title":      "Legacy Spec",
				"content":    "legacy prefixed content",
				"updated_at": "2026-04-26T00:00:00Z",
			})
		default:
			t.Fatalf("unexpected doc_key %q", docKey)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws")
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "doc:pilot.spec"})
	if result == nil || result.IsError {
		t.Fatalf("expected legacy prefixed doc read, got %+v", result)
	}
	if got := strings.Join(calls, ","); got != "pilot.spec,doc:pilot.spec" {
		t.Fatalf("unexpected doc lookup order: %s", got)
	}
	if !strings.Contains(result.Output, "legacy prefixed content") || !strings.Contains(result.Output, "canonicalized_from") {
		t.Fatalf("expected legacy doc output with canonicalization note, got %q", result.Output)
	}
}

func TestWorkspaceDocGetToolMissingDocHintsDependencyBlock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		writeRPCError(w, req, -32000, "workspace doc not found: ws/missing.doc")
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws")
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "missing.doc"})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing doc to be an error, got %+v", result)
	}
	for _, want := range []string{"workspace doc not found", "block on dependency", "retrying indefinitely"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocGetToolBlocksForeignProjectDocInActiveProject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"doc_key":    "task.task-old.impl",
			"title":      "Old implementation brief",
			"content":    "# Task Brief\n\n- task_id: task-old\n- project_id: project-old\n- project_lane: implementation\n",
			"updated_at": "2026-05-04T00:00:00Z",
		})
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-current", ProjectID: "project-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "task.task-old.impl"})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign project doc to be blocked, got %+v", result)
	}
	for _, want := range []string{"cross-project", "project-old", "project-current", "stale context"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocGetToolBlocksForeignProjectDocKeyWithoutHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"doc_key":    "project.project-old.product_contract",
			"title":      "Old Product Contract",
			"content":    "# Product Contract\n\nNo project header here.",
			"updated_at": "2026-05-04T00:00:00Z",
		})
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-current", ProjectID: "project-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "project.project-old.product_contract"})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign project doc key to be blocked, got %+v", result)
	}
	for _, want := range []string{"cross-project", "project-old", "project-current", "project docs"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocGetToolBlocksForeignTaskDocKeyWithoutProjectID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{
				"doc_key":    "task.task-old.blocker",
				"title":      "Old blocker without project header",
				"content":    "# Blocker\n\nThis old blocker forgot to declare project_id.\n",
				"updated_at": "2026-05-04T00:00:00Z",
			})
		case "agent.task.hydrate":
			if got := rpcString(req.Params, "task_id"); got != "task-old" {
				t.Fatalf("hydrate task_id = %q, want task-old", got)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"task": map[string]any{
						"task_id":       "task-old",
						"project_id":    "project-old",
						"owner_user_id": "owner",
						"priority":      "normal",
						"status":        "CANCELLED",
						"task_kind":     "EXECUTION",
						"task_template": "generic",
						"created_at":    "2026-05-04T00:00:00Z",
						"updated_at":    "2026-05-04T00:00:00Z",
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-current", ProjectID: "project-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "task.task-old.blocker"})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign task-keyed doc to be blocked, got %+v", result)
	}
	for _, want := range []string{"cross-project task document", "task-old", "project-old", "project-current"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocGetToolBlocksForeignContentTaskIDWithoutProjectID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{
				"doc_key":    "agent.beta.current_context",
				"title":      "Beta Current Context",
				"content":    "# Agent Current Context\n\n- agent_id: beta\n- task_id: task-old-impl\n- task_title: Old implementation lane\n",
				"updated_at": "2026-05-04T00:00:00Z",
			})
		case "agent.task.hydrate":
			if got := rpcString(req.Params, "task_id"); got != "task-old-impl" {
				t.Fatalf("hydrate task_id = %q, want task-old-impl", got)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"task": map[string]any{
						"task_id":       "task-old-impl",
						"project_id":    "project-old",
						"owner_user_id": "owner",
						"priority":      "normal",
						"status":        "RUNNING",
						"task_kind":     "EXECUTION",
						"task_template": "generic",
						"created_at":    "2026-05-04T00:00:00Z",
						"updated_at":    "2026-05-04T00:00:00Z",
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-current", ProjectID: "project-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "agent.beta.current_context"})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign content task-id doc to be blocked, got %+v", result)
	}
	for _, want := range []string{"cross-project task document", "task-old-impl", "project-old", "project-current", "stale context"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocGetToolAllowsActiveContentTaskIDWithoutHydration(t *testing.T) {
	hydrateCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{
				"doc_key":    "agent.alpha.current_context",
				"title":      "Alpha Current Context",
				"content":    "# Agent Current Context\n\n- agent_id: alpha\n- task_id: task-current\n- task_title: Current strategy lane\n",
				"updated_at": "2026-05-04T00:00:00Z",
			})
		case "agent.task.hydrate":
			hydrateCalled = true
			t.Fatalf("active content task_id should not require hydration")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-current", ProjectID: "project-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "agent.alpha.current_context"})
	if result == nil || result.IsError {
		t.Fatalf("expected active content task-id doc to be readable, got %+v", result)
	}
	if hydrateCalled {
		t.Fatal("hydrate should not have been called")
	}
	if !strings.Contains(result.Output, "Current strategy lane") {
		t.Fatalf("expected doc content, got %q", result.Output)
	}
}

func TestWorkspaceDocGetToolAllowsActiveTaskDocKeyWithoutHydration(t *testing.T) {
	hydrateCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{
				"doc_key":    "task.task-current.agent_response.req-1",
				"title":      "Current response",
				"content":    "# Current task response\n\nNo project header is required for the active task doc.",
				"updated_at": "2026-05-04T00:00:00Z",
			})
		case "agent.task.hydrate":
			hydrateCalled = true
			t.Fatalf("active task doc key should not require hydration")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocGetTool(NewRhizomeClient(server.URL, "token"), "ws").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-current", ProjectID: "project-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{"doc_key": "task.task-current.agent_response.req-1"})
	if result == nil || result.IsError {
		t.Fatalf("expected active task doc to be readable, got %+v", result)
	}
	if hydrateCalled {
		t.Fatal("hydrate should not have been called")
	}
	if !strings.Contains(result.Output, "Current task response") {
		t.Fatalf("expected doc content, got %q", result.Output)
	}
}

func TestWorkspaceDocPutToolWritesCanonicalDoc(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.put" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		for key, want := range map[string]string{
			"workspace_id": "ws",
			"doc_key":      "pilot.result",
			"title":        "Pilot Result",
			"content":      "durable result content",
			"updated_by":   "agent-1",
			"expected_sha": "sha-old",
		} {
			if got := rpcString(req.Params, key); got != want {
				t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
			}
		}
		writeRPCResult(w, req, map[string]any{"sha": "sha-new"})
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key":      "pilot.result",
		"title":        "Pilot Result",
		"content":      "durable result content",
		"expected_sha": "sha-old",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful doc write, got %+v", result)
	}
	for _, want := range []string{"pilot.result", "agent-1", "sha-new", "stored"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocPutToolCanonicalizesDocPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.put" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		if got := rpcString(req.Params, "doc_key"); got != "pilot.result" {
			t.Fatalf("doc_key = %q, want pilot.result", got)
		}
		writeRPCResult(w, req, map[string]any{"sha": "sha-new"})
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key": "doc:pilot.result",
		"title":   "Pilot Result",
		"content": "durable result content",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful doc write, got %+v", result)
	}
	for _, want := range []string{"pilot.result", "canonicalized_from", "doc:pilot.result"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocPutToolSyncsProjectDesignDocToProfile(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-alpha.design" {
				t.Fatalf("doc_key = %q, want project.project-alpha.design", got)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-design"})
		case 2:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			for key, want := range map[string]string{
				"workspace_id":  "ws",
				"project_id":    "project-alpha",
				"actor_id":      "agent-1",
				"design_doc_id": "project.project-alpha.design",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-alpha",
				"design_doc_id": "project.project-alpha.design",
				"updated_by":    "agent-1",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key": "project.project-alpha.design",
		"title":   "Project Alpha Design",
		"content": "Design content ready for implementation planning.",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful project design doc write, got %+v", result)
	}
	if calls != 2 {
		t.Fatalf("expected doc write plus profile sync, got %d calls", calls)
	}
	for _, want := range []string{"project_profile_sync", "design_doc_id", "synced"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocPutToolSyncsProjectProfileWithCanonicalProjectIDFromContent(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-plan"})
		case 2:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got, want := rpcString(req.Params, "project_id"), "project-causal-board-20260504T2015Z"; got != want {
				t.Fatalf("project_id = %q, want canonical %q; params=%+v", got, want, req.Params)
			}
			if got := rpcString(req.Params, "implementation_plan_doc_id"); got != "project.project-causal-board-20260504t2015z.implementation_plan" {
				t.Fatalf("implementation_plan_doc_id = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":                    "ws",
				"project_id":                      "project-causal-board-20260504T2015Z",
				"implementation_plan_doc_id":      "project.project-causal-board-20260504t2015z.implementation_plan",
				"implementation_plan_doc_updated": "ok",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "delta")
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key": "project.project-causal-board-20260504t2015z.implementation_plan",
		"title":   "Causal Board Implementation Plan",
		"content": "# Plan\n\n- project_id: project-causal-board-20260504T2015Z\n- phase: SPEC\n",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful project profile sync, got %+v", result)
	}
}

func TestWorkspaceDocPutToolSyncsCombinedProjectDesignAndPlan(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-alpha.design_and_plan" {
				t.Fatalf("doc_key = %q, want project.project-alpha.design_and_plan", got)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-combined"})
		case 2:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			for key, want := range map[string]string{
				"workspace_id":               "ws",
				"project_id":                 "project-alpha",
				"actor_id":                   "agent-1",
				"design_doc_id":              "project.project-alpha.design_and_plan",
				"implementation_plan_doc_id": "project.project-alpha.design_and_plan",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":               "ws",
				"project_id":                 "project-alpha",
				"design_doc_id":              "project.project-alpha.design_and_plan",
				"implementation_plan_doc_id": "project.project-alpha.design_and_plan",
				"updated_by":                 "agent-1",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key": "project.project-alpha.design_and_plan",
		"title":   "Project Alpha Design And Plan",
		"content": "# Design And Plan\n\n- project_id: project-alpha\n\nReady for implementation.",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful combined project doc sync, got %+v", result)
	}
	for _, want := range []string{"project_profile_sync", "design_and_plan_doc_ids", "synced"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 2 {
		t.Fatalf("expected doc write plus combined profile sync, got %d calls", calls)
	}
}

func TestWorkspaceDocPutToolSkipsDraftProjectDocProfileSync(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "workspace.doc.put" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{"sha": "sha-draft"})
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key": "project.project-alpha.design",
		"title":   "Draft Project Alpha Design",
		"content": "Not ready yet.",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful draft project doc write, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only doc write for draft project doc, got %d calls", calls)
	}
	for _, want := range []string{"project_profile_sync", "skipped_draft"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocPutToolRetriesDocumentConflictWithMergedContent(t *testing.T) {
	putCalls := 0
	getCalls := 0
	var mergedContent string
	var finalExpectedSHA string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.put":
			putCalls++
			if putCalls == 1 {
				writeRPCError(w, req, rhizomeRPCCodeDocumentConflict, "sha drifted")
				return
			}
			mergedContent = rpcString(req.Params, "content")
			finalExpectedSHA = rpcString(req.Params, "expected_sha")
			writeRPCResult(w, req, map[string]any{"sha": "sha-final"})
		case "workspace.doc.get":
			getCalls++
			writeRPCResult(w, req, map[string]any{
				"doc_key": "pilot.result",
				"title":   "Pilot Result",
				"content": "remote content",
				"sha":     "sha-remote",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1")
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key":      "pilot.result",
		"title":        "Pilot Result",
		"content":      "local update",
		"expected_sha": "sha-stale",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful retry, got %+v", result)
	}
	if putCalls != 2 || getCalls != 1 {
		t.Fatalf("expected one conflict retry, got puts=%d gets=%d", putCalls, getCalls)
	}
	if finalExpectedSHA != "sha-remote" {
		t.Fatalf("expected retry to use refreshed sha, got %q", finalExpectedSHA)
	}
	for _, want := range []string{"remote content", "Merged local update from agent-1", "local update"} {
		if !strings.Contains(mergedContent, want) {
			t.Fatalf("expected merged content to contain %q, got %q", want, mergedContent)
		}
	}
	if !strings.Contains(result.Output, "sha-final") {
		t.Fatalf("expected final sha in output, got %q", result.Output)
	}
}

func TestWorkspaceDocPutToolMarksFinalCandidateDraftWhileCoordinationGateActive(t *testing.T) {
	var storedTitle string
	var storedContent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.put" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		storedTitle = rpcString(req.Params, "title")
		storedContent = rpcString(req.Params, "content")
		writeRPCResult(w, req, map[string]any{"sha": "sha-draft"})
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1").WithDraftGate(func() WorkspaceDocDraftGateState {
		return WorkspaceDocDraftGateState{
			Active:    true,
			TaskID:    "task-1",
			RunID:     "run-1",
			PeerID:    "agent-beta",
			RequestID: "req-1",
			State:     completionCoordinationStateRequested,
			Reason:    "peer review pending",
		}
	})
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key": "task.task-1.result",
		"title":   "Final Result",
		"content": "ready for completion",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful doc write, got %+v", result)
	}
	if !strings.HasPrefix(storedTitle, "Draft - ") {
		t.Fatalf("expected draft title, got %q", storedTitle)
	}
	for _, want := range []string{"DRAFT PENDING PEER REVIEW", "peer review pending", "coordination_request_id: req-1", "ready for completion"} {
		if !strings.Contains(storedContent, want) {
			t.Fatalf("expected stored content to contain %q, got %q", want, storedContent)
		}
	}
	for _, want := range []string{"draft_pending_peer_review", "req-1", "agent-beta"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected tool output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestWorkspaceDocPutToolDoesNotDraftNonFinalDoc(t *testing.T) {
	var storedTitle string
	var storedContent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.doc.put" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		storedTitle = rpcString(req.Params, "title")
		storedContent = rpcString(req.Params, "content")
		writeRPCResult(w, req, map[string]any{"sha": "sha-note"})
	}))
	defer server.Close()

	tool := NewWorkspaceDocPutTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-1").WithDraftGate(func() WorkspaceDocDraftGateState {
		return WorkspaceDocDraftGateState{Active: true, TaskID: "task-1", State: "pre_review"}
	})
	result := tool.Execute(context.Background(), map[string]any{
		"doc_key": "task.task-1.notes",
		"title":   "Implementation Notes",
		"content": "Backend spike notes",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful doc write, got %+v", result)
	}
	if strings.HasPrefix(storedTitle, "Draft - ") || strings.Contains(storedContent, "DRAFT PENDING PEER REVIEW") {
		t.Fatalf("non-final doc should not be draft-marked, title=%q content=%q", storedTitle, storedContent)
	}
	if strings.Contains(result.Output, "draft_pending_peer_review") {
		t.Fatalf("non-final doc output should not report draft status: %q", result.Output)
	}
}

func TestWorkspaceDocPutToolValidatesRequiredFields(t *testing.T) {
	tool := NewWorkspaceDocPutTool(NewRhizomeClient("http://127.0.0.1:1/rpc", "token"), "ws", "agent-1")
	for _, tc := range []struct {
		name string
		args map[string]any
		want string
	}{
		{name: "missing doc key", args: map[string]any{"title": "Title", "content": "Body"}, want: "doc_key is required"},
		{name: "missing title", args: map[string]any{"doc_key": "doc", "content": "Body"}, want: "title is required"},
		{name: "missing content", args: map[string]any{"doc_key": "doc", "title": "Title"}, want: "content is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := tool.Execute(context.Background(), tc.args)
			if result == nil || !result.IsError || !strings.Contains(result.Output, tc.want) {
				t.Fatalf("expected validation error %q, got %+v", tc.want, result)
			}
		})
	}
}
