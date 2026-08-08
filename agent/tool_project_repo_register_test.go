package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectRepoRegisterToolRecordsReadyGitHubRemote(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
		case 2:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			for key, want := range map[string]string{
				"workspace_id":        "ws",
				"project_id":          "project-subpixel",
				"actor_id":            "agent-alpha",
				"repo_id":             "projrepo-project-subpixel-exampleorg-subpixel-lab",
				"remote_url":          "git@github.com:ExampleOrg/subpixel-lab.git",
				"remote_kind":         "github",
				"owner":               "ExampleOrg",
				"name":                "subpixel-lab",
				"default_branch":      "main",
				"repo_status":         "READY",
				"created_by_agent_id": "agent-alpha",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			if got, ok := req.Params["is_canonical"].(bool); !ok || !got {
				t.Fatalf("is_canonical = %#v, want true", req.Params["is_canonical"])
			}
			writeRPCResult(w, req, map[string]any{"repository": map[string]any{
				"workspace_id":        "ws",
				"project_id":          "project-subpixel",
				"repo_id":             "projrepo-project-subpixel-exampleorg-subpixel-lab",
				"remote_url":          "git@github.com:ExampleOrg/subpixel-lab.git",
				"remote_kind":         "github",
				"owner":               "ExampleOrg",
				"name":                "subpixel-lab",
				"default_branch":      "main",
				"repo_status":         "READY",
				"is_canonical":        true,
				"created_by_agent_id": "agent-alpha",
			}})
		case 3:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "READY" {
				t.Fatalf("profile repo_status = %q, want READY", got)
			}
			if got := rpcString(req.Params, "repo_url"); got != "git@github.com:ExampleOrg/subpixel-lab.git" {
				t.Fatalf("profile repo_url = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":        "ws",
				"project_id":          "project-subpixel",
				"current_phase":       "SPEC",
				"repo_required":       true,
				"repo_status":         "READY",
				"repo_url":            "git@github.com:ExampleOrg/subpixel-lab.git",
				"repo_default_branch": "main",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			repo := map[string]any{
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-project-subpixel-exampleorg-subpixel-lab",
				"remote_url":     "git@github.com:ExampleOrg/subpixel-lab.git",
				"remote_kind":    "github",
				"owner":          "ExampleOrg",
				"name":           "subpixel-lab",
				"default_branch": "main",
				"repo_status":    "READY",
				"is_canonical":   true,
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", []map[string]any{repo}, "PARTIAL", false))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":  "project-subpixel",
		"remote_url":  "git@github.com:ExampleOrg/subpixel-lab.git",
		"repo_status": "READY",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful project_repo_register, got %+v", result)
	}
	for _, want := range []string{"projrepo-project-subpixel-exampleorg-subpixel-lab", `"repo_status": "READY"`, `"no_git_mutation": true`, `"profile_updated": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectRepoRegisterToolRejectsCrossProjectRuntimeBindingBeforeRPC(t *testing.T) {
	tool := NewProjectRepoRegisterTool(NewRhizomeClient("http://127.0.0.1:1", "token"), "ws", "agent-alpha", "owner-1").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-current", TaskID: "task-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":  "project-old",
		"remote_url":  "https://github.com/ExampleOrg/old.git",
		"repo_status": "READY",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "active task is bound to project_id project-current") {
		t.Fatalf("expected active-project binding rejection before RPC, got %+v", result)
	}
}

func TestProjectRepoRegisterToolDefaultsRemoteToCreatedNotReady(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
		case 2:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "CREATED" {
				t.Fatalf("repo_status = %q, want CREATED by default for unverified remote", got)
			}
			writeRPCResult(w, req, map[string]any{"repository": map[string]any{
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-project-subpixel-exampleorg-subpixel-lab",
				"remote_url":     "https://github.com/ExampleOrg/subpixel-lab.git",
				"remote_kind":    "github",
				"owner":          "ExampleOrg",
				"name":           "subpixel-lab",
				"default_branch": "main",
				"repo_status":    "CREATED",
				"is_canonical":   true,
			}})
		case 3:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "BLOCKED" {
				t.Fatalf("profile repo_status = %q, want BLOCKED until explicit READY evidence", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"current_phase": "SPEC",
				"repo_required": true,
				"repo_status":   "BLOCKED",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"remote_url": "https://github.com/ExampleOrg/subpixel-lab.git",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful unverified remote registration, got %+v", result)
	}
	if !strings.Contains(result.Output, `"repo_status": "CREATED"`) || !strings.Contains(result.Output, `"implementation_ready": false`) {
		t.Fatalf("expected CREATED/non-ready evidence, got %q", result.Output)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectRepoRegisterToolRequestsHumanWhenRepositoryMissing(t *testing.T) {
	calls := 0
	var requestParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
		case 2:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "REQUESTED" {
				t.Fatalf("repo_status = %q, want REQUESTED", got)
			}
			if got := rpcString(req.Params, "repo_id"); got != "projrepo-project-subpixel-canonical" {
				t.Fatalf("repo_id = %q, want deterministic canonical id", got)
			}
			writeRPCResult(w, req, map[string]any{"repository": map[string]any{
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-project-subpixel-canonical",
				"remote_kind":    "unknown",
				"default_branch": "main",
				"repo_status":    "REQUESTED",
				"is_canonical":   true,
			}})
		case 3:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "BLOCKED" {
				t.Fatalf("profile repo_status = %q, want BLOCKED", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"current_phase": "SPEC",
				"repo_required": true,
				"repo_status":   "BLOCKED",
			}})
		case 4:
			if req.Method != "workspace.ops.get" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCError(w, req, -32602, "operator queue item not found")
		case 5:
			if req.Method != "workspace.ops.request" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			requestParams = req.Params
			writeRPCResult(w, req, map[string]any{"status": "REQUESTED"})
		case 6:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{"project_id": "project-subpixel"})
	if result == nil || result.IsError {
		t.Fatalf("expected successful missing-repo registration, got %+v", result)
	}
	if requestParams == nil {
		t.Fatalf("expected operator request params")
	}
	if requestParams["gate_type"] != "EXPLICIT_APPROVAL" || requestParams["source_kind"] != "project" || requestParams["source_id"] != "project-subpixel" {
		t.Fatalf("unexpected request params: %+v", requestParams)
	}
	if requestParams["request_key"] != "project.repo.project-subpixel.projrepo-project-subpixel-canonical" {
		t.Fatalf("unexpected request_key: %+v", requestParams)
	}
	if !strings.Contains(result.Output, `"human_request_created": true`) || !strings.Contains(result.Output, `"no_git_mutation": true`) {
		t.Fatalf("expected human request and no mutation evidence, got %q", result.Output)
	}
	if calls != 6 {
		t.Fatalf("expected 6 calls, got %d", calls)
	}
}

func TestProjectRepoRegisterToolRequestsHumanForCreatedWithoutRemote(t *testing.T) {
	calls := 0
	var requestParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
		case 2:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "CREATED" {
				t.Fatalf("repo_status = %q, want CREATED", got)
			}
			writeRPCResult(w, req, map[string]any{"repository": map[string]any{
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-project-subpixel-canonical",
				"remote_kind":    "unknown",
				"default_branch": "main",
				"repo_status":    "CREATED",
				"is_canonical":   true,
			}})
		case 3:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "BLOCKED" {
				t.Fatalf("profile repo_status = %q, want BLOCKED", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"current_phase": "SPEC",
				"repo_required": true,
				"repo_status":   "BLOCKED",
			}})
		case 4:
			if req.Method != "workspace.ops.get" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCError(w, req, -32602, "operator queue item not found")
		case 5:
			if req.Method != "workspace.ops.request" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			requestParams = req.Params
			writeRPCResult(w, req, map[string]any{"status": "REQUESTED"})
		case 6:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":  "project-subpixel",
		"repo_status": "CREATED",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful created-without-remote registration, got %+v", result)
	}
	if requestParams == nil || requestParams["gate_type"] != "EXPLICIT_APPROVAL" {
		t.Fatalf("expected explicit approval operator request, got %+v", requestParams)
	}
	if !strings.Contains(result.Output, `"human_request_created": true`) {
		t.Fatalf("expected human request evidence, got %q", result.Output)
	}
	if calls != 6 {
		t.Fatalf("expected 6 calls, got %d", calls)
	}
}

func TestProjectRepoRegisterToolReusesExistingCanonicalRepo(t *testing.T) {
	calls := 0
	existingRepo := map[string]any{
		"workspace_id":   "ws",
		"project_id":     "project-subpixel",
		"repo_id":        "projrepo-existing",
		"remote_url":     "https://github.com/ExampleOrg/subpixel-lab.git",
		"remote_kind":    "github",
		"default_branch": "main",
		"repo_status":    "READY",
		"is_canonical":   true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", []map[string]any{existingRepo}, "PARTIAL", false))
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{"project_id": "project-subpixel"})
	if result == nil || result.IsError {
		t.Fatalf("expected successful reuse, got %+v", result)
	}
	if !strings.Contains(result.Output, `"reused_existing": true`) || !strings.Contains(result.Output, "projrepo-existing") {
		t.Fatalf("expected existing repo reuse, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectRepoRegisterToolReusesExistingRepoForSameRemote(t *testing.T) {
	calls := 0
	existingRepo := map[string]any{
		"workspace_id":   "ws",
		"project_id":     "project-subpixel",
		"repo_id":        "projrepo-existing",
		"remote_url":     "https://github.com/ExampleOrg/subpixel-lab.git",
		"remote_kind":    "github",
		"owner":          "ExampleOrg",
		"name":           "subpixel-lab",
		"default_branch": "main",
		"repo_status":    "READY",
		"is_canonical":   true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", []map[string]any{existingRepo}, "PARTIAL", false))
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"remote_url": "https://github.com/ExampleOrg/subpixel-lab.git",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful same-remote reuse, got %+v", result)
	}
	if !strings.Contains(result.Output, `"repo_id": "projrepo-existing"`) || !strings.Contains(result.Output, `"reused_existing": true`) {
		t.Fatalf("expected existing repo id reuse, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectRepoRegisterToolUsesExistingMissingCanonicalRepoID(t *testing.T) {
	calls := 0
	existingRepo := map[string]any{
		"workspace_id":   "ws",
		"project_id":     "project-subpixel",
		"repo_id":        "projrepo-existing-missing",
		"remote_kind":    "unknown",
		"default_branch": "main",
		"repo_status":    "REQUESTED",
		"is_canonical":   true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", []map[string]any{existingRepo}, "BLOCKED", false))
		case 2:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_id"); got != "projrepo-existing-missing" {
				t.Fatalf("repo_id = %q, want existing canonical repo id", got)
			}
			if got := rpcString(req.Params, "repo_status"); got != "REQUESTED" {
				t.Fatalf("repo_status = %q, want REQUESTED", got)
			}
			writeRPCResult(w, req, map[string]any{"repository": existingRepo})
		case 3:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"current_phase": "SPEC",
				"repo_required": true,
				"repo_status":   "BLOCKED",
			}})
		case 4:
			if req.Method != "workspace.ops.get" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCError(w, req, -32602, "operator queue item not found")
		case 5:
			if req.Method != "workspace.ops.request" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"status": "REQUESTED"})
		case 6:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", []map[string]any{existingRepo}, "BLOCKED", false))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{"project_id": "project-subpixel"})
	if result == nil || result.IsError {
		t.Fatalf("expected successful existing missing repo handling, got %+v", result)
	}
	if !strings.Contains(result.Output, "projrepo-existing-missing") || !strings.Contains(result.Output, `"human_request_created": true`) {
		t.Fatalf("expected existing missing repo id and human request, got %q", result.Output)
	}
	if calls != 6 {
		t.Fatalf("expected 6 calls, got %d", calls)
	}
}

func TestProjectRepoRegisterToolMaterializesExistingRequestedCanonicalRepo(t *testing.T) {
	calls := 0
	existingRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          "project-subpixel",
		"repo_id":             "projrepo-existing-requested",
		"remote_kind":         "unknown",
		"default_branch":      "main",
		"repo_status":         "REQUESTED",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
	}
	readyRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          "project-subpixel",
		"repo_id":             "projrepo-existing-requested",
		"remote_url":          "file:///tmp/branch-choir.git",
		"remote_kind":         "local",
		"owner":               "tmp",
		"name":                "branch-choir",
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", []map[string]any{existingRepo}, "BLOCKED", false))
		case 2:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			for key, want := range map[string]string{
				"repo_id":     "projrepo-existing-requested",
				"remote_url":  "file:///tmp/branch-choir.git",
				"remote_kind": "local",
				"owner":       "tmp",
				"name":        "branch-choir",
				"repo_status": "READY",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"repository": readyRepo})
		case 3:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "READY" {
				t.Fatalf("profile repo_status = %q, want READY", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":        "ws",
				"project_id":          "project-subpixel",
				"current_phase":       "SPEC",
				"repo_required":       true,
				"repo_status":         "READY",
				"repo_url":            "file:///tmp/branch-choir.git",
				"repo_default_branch": "main",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", []map[string]any{readyRepo}, "PARTIAL", false))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":  "project-subpixel",
		"remote_url":  "file:///tmp/branch-choir.git",
		"repo_status": "READY",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful requested canonical materialization, got %+v", result)
	}
	if !strings.Contains(result.Output, `"repo_id": "projrepo-existing-requested"`) ||
		!strings.Contains(result.Output, `"repo_status": "READY"`) ||
		strings.Contains(result.Output, "project-subpixel-tmp-branch-choir") {
		t.Fatalf("expected materialized existing canonical repo id, got %q", result.Output)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectRepoRegisterToolRejectsUnsafeInputsBeforeRPC(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Fatalf("unexpected RPC call")
	}))
	defer server.Close()

	tool := NewProjectRepoRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":                "project-subpixel",
		"credential_vault_entry_id": "-----BEGIN PRIVATE KEY-----\nabc",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "vault entry id") {
		t.Fatalf("expected credential ref validation error, got %+v", result)
	}
	result = tool.Execute(context.Background(), map[string]any{
		"project_id":  "project-subpixel",
		"repo_status": "READY",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "READY requires remote_url") {
		t.Fatalf("expected READY-without-remote validation error, got %+v", result)
	}
	if calls != 0 {
		t.Fatalf("expected no RPC calls, got %d", calls)
	}
}

func projectRepoCoordinationResult(projectID string, repositories []map[string]any, overallState string, implementationReady bool) map[string]any {
	return map[string]any{"coordination": map[string]any{
		"coordination_version": "v1",
		"project": map[string]any{
			"workspace_id": "ws",
			"project_id":   projectID,
			"title":        "Subpixel Pattern Lab",
			"status":       "ACTIVE",
		},
		"profile": map[string]any{
			"workspace_id":  "ws",
			"project_id":    projectID,
			"current_phase": "SPEC",
			"repo_required": true,
			"repo_status":   "MISSING",
		},
		"gate_status": map[string]any{
			"workspace_id":         "ws",
			"project_id":           projectID,
			"overall_state":        overallState,
			"implementation_ready": implementationReady,
		},
		"repositories": repositories,
	}}
}
