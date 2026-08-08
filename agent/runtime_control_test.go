package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeHandleRequestRuntimeResumeQueuesPendingTrigger(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var savedScratch RuntimeScratchState
	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &savedScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseJSON = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during runtime.resume: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-1",
			TaskID:    "task-1",
			Status:    "ACTIVE",
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-resume", Method: "runtime.resume"}); err != nil {
		t.Fatalf("handleRequest(runtime.resume) error = %v", err)
	}

	if savedScratch.PendingTrigger != "runtime_resume" || savedScratch.PendingTriggerTask != "task-1" || savedScratch.PendingTriggerSession != "session-1" {
		t.Fatalf("expected runtime resume to queue trigger, got %+v", savedScratch)
	}
	if runtime.scratch.PendingTrigger != "runtime_resume" {
		t.Fatalf("expected runtime scratch to retain pending trigger, got %+v", runtime.scratch)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.resume response: %v", err)
	}
	if parsed["status"] != "ok" {
		t.Fatalf("unexpected response status: %#v", parsed)
	}
	if parsed["summary"] != "resume requested" {
		t.Fatalf("unexpected resume summary: %#v", parsed)
	}
	if parsed["method"] != "runtime.resume" {
		t.Fatalf("expected method to round-trip in response, got %#v", parsed["method"])
	}
	if parsed["session_id"] != "session-1" || parsed["task_id"] != "task-1" {
		t.Fatalf("expected current session/task in response, got %#v", parsed)
	}
}

func TestRuntimeStatusResponseCompactsOversizedPayload(t *testing.T) {
	var responseJSON string
	var bodyLen int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.respond" {
			t.Fatalf("unexpected method during runtime.status: %s", req.Method)
		}
		responseJSON = rpcString(req.Params, "response")
		raw, _ := json.Marshal(req)
		bodyLen = len(raw)
		writeRPCResult(w, req, nil)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:     map[string]string{},
			LastSummary: strings.Repeat("massive-status ", maxAgentRespondResponseBytes/2),
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-status", Method: "runtime.status"}); err != nil {
		t.Fatalf("handleRequest(runtime.status) error = %v", err)
	}
	if bodyLen >= 1<<20 {
		t.Fatalf("agent.respond request body should stay below default cap, got %d", bodyLen)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		end := len(responseJSON)
		if end > 200 {
			end = 200
		}
		t.Fatalf("compact runtime.status response must remain valid JSON: %v; response=%q", err, responseJSON[:end])
	}
	if parsed["response_truncated"] != true {
		t.Fatalf("expected response_truncated marker, got %+v", parsed)
	}
	if parsed["method"] != "runtime.status" || parsed["agent_id"] != "agent-1" {
		t.Fatalf("expected compact status identity fields, got %+v", parsed)
	}
	if len(responseJSON) > maxAgentRespondResponseBytes {
		t.Fatalf("compact runtime.status response len = %d, want <= %d", len(responseJSON), maxAgentRespondResponseBytes)
	}
}

func TestRuntimeStatusResponseCompactsToolBundleContourAsValidJSON(t *testing.T) {
	huge := strings.Repeat("oversized diagnostic ", maxAgentRespondResponseBytes/8)
	status := map[string]any{
		"status":     "ok",
		"agent_id":   "agent-tool",
		"method":     "runtime.status",
		"summary":    strings.Repeat("massive-status ", maxAgentRespondResponseBytes/2),
		"task_id":    "task-tool",
		"session_id": "session-tool",
		"tool_bundles": ToolBundleStatusSnapshot{
			Schema:           "tool_bundle_status/v1",
			Status:           "degraded",
			PromptVisible:    true,
			ToolVisible:      true,
			CopyInContract:   ".runtime-config/tool-bundles/<bundle>/tool.json or tools/<bundle>/tool.json",
			SuiteCount:       1,
			InstalledCount:   1,
			HealthcheckCount: 1,
			ErrorCount:       1,
			CollisionCount:   1,
			Suites: []ToolBundleSuiteReadinessItem{{
				Suite:            "browser_read_only",
				Status:           "ready",
				Required:         true,
				Heartbeats:       []string{"visual_product_audit"},
				ReadyBundles:     []string{"browser_visual_probe"},
				CandidateBundles: []string{"browser_visual_probe"},
				SuggestedBundles: []string{"browser_visual_probe"},
				SuggestedActions: []string{"inspect " + huge},
				Message:          huge,
			}},
			Installed: []ToolBundleStatusItem{{
				Name:              "browser_visual_probe",
				Version:           strings.Repeat("v", 512),
				CapabilitySuites:  []string{strings.Repeat("browser_read_only", 64)},
				ArtifactContracts: []string{strings.Repeat("probe_report:required", 64)},
				Dependencies:      []string{strings.Repeat("node:executable:required", 64)},
				TimeoutSeconds:    5,
				SourceDir:         huge,
			}},
			Healthchecks: []ToolBundleDiscoveryDiagnostic{{Code: "healthcheck_passed", Name: "browser_visual_probe", Message: huge}},
			Errors:       []ToolBundleDiscoveryDiagnostic{{Code: "malformed_manifest", Name: "broken", RootKind: "tools", Message: huge}},
			Collisions:   []ToolBundleDiscoveryDiagnostic{{Code: "duplicate_tool_registration", Name: "read_file", RootKind: "managed", Message: huge}},
		},
	}
	responseJSON := runtimeStatusResponseText(status, []byte(strings.Repeat("x", maxAgentRespondResponseBytes+1)))
	if len(responseJSON) > maxAgentRespondResponseBytes {
		t.Fatalf("compact runtime.status response len = %d, want <= %d", len(responseJSON), maxAgentRespondResponseBytes)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("compact runtime.status response must remain valid JSON: %v", err)
	}
	bundles, ok := parsed["tool_bundles"].(map[string]any)
	if !ok {
		t.Fatalf("expected compact tool_bundles object, got %+v", parsed["tool_bundles"])
	}
	if bundles["status"] != "degraded" || bundles["error_count"] != float64(1) || bundles["collision_count"] != float64(1) {
		t.Fatalf("expected compact tool bundle counts/status, got %+v", bundles)
	}
	if bundles["suite_count"] != float64(1) || bundles["suites"] == nil {
		t.Fatalf("expected compact tool bundle suites, got %+v", bundles)
	}
	if strings.Contains(responseJSON, huge) {
		t.Fatalf("compact runtime.status response leaked unbounded tool bundle diagnostic")
	}
}

func TestRuntimeHandleRequestRuntimeRefreshRebuildsBootstrapAndPersistsScratch(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var methods []string
	var savedScratch RuntimeScratchState
	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-27T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         "agent-1",
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-03-27T00:00:00Z",
					"updated_at":       "2026-03-27T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{
						"workspace_id": "ws-1",
						"title":        "Workspace One",
						"status":       "ACTIVE",
					},
					"docs":             []any{},
					"agents":           []any{},
					"sessions":         []any{},
					"tools":            []any{},
					"tasks":            []any{},
					"task_links":       []any{},
					"recent_memory":    []any{},
					"recent_artifacts": []any{},
					"recent_updates":   []any{},
					"recent_messages":  []any{},
					"projects":         []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &savedScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseJSON = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during runtime.refresh: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			LastSummary: "steady",
			DocSHAs:     map[string]string{"doc-1": "sha-old"},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-refresh", Method: "runtime.refresh"}); err != nil {
		t.Fatalf("handleRequest(runtime.refresh) error = %v", err)
	}

	if len(methods) != 4 || methods[0] != "agent.bootstrap" || methods[1] != "agent.limits.get" || methods[2] != "agent.state.set" || methods[3] != "agent.respond" {
		t.Fatalf("unexpected runtime.refresh call sequence: %#v", methods)
	}
	if runtime.lastBootstrap.IsZero() {
		t.Fatal("expected runtime.lastBootstrap to be updated")
	}
	if runtime.bootstrap.GeneratedAt != "2026-03-27T00:00:00Z" {
		t.Fatalf("expected bootstrap result to be stored, got %+v", runtime.bootstrap)
	}
	if savedScratch.LastSummary != "steady" || savedScratch.DocSHAs["doc-1"] != "sha-old" {
		t.Fatalf("expected scratch state to be persisted unchanged, got %+v", savedScratch)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.refresh response: %v", err)
	}
	if parsed["status"] != "ok" {
		t.Fatalf("unexpected response status: %#v", parsed)
	}
	if parsed["summary"] != "bootstrap refreshed" {
		t.Fatalf("unexpected refresh summary: %#v", parsed)
	}
	if !strings.Contains(strings.ToLower(rpcString(map[string]any{"method": parsed["method"]}, "method")), "runtime.refresh") {
		t.Fatalf("expected method to round-trip in response, got %#v", parsed["method"])
	}
}

func TestRuntimeHandleRequestRuntimePauseResumeAndSwitchTask(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var methods []string
	var savedStates []RuntimeScratchState
	var responses []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			var parsed map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "response")), &parsed); err != nil {
				t.Fatalf("decode response payload: %v", err)
			}
			responses = append(responses, parsed)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during runtime control: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{TaskID: "task-1", Title: "Task One", Status: "RUNNING"},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-1",
			TaskID:    "task-1",
			Status:    "ACTIVE",
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Tasks: []WorkspaceTaskRecord{
					{TaskID: "task-1", Title: "Task One", Status: "RUNNING"},
					{TaskID: "task-2", Title: "Task Two", Status: "PENDING"},
				},
				Sessions: []AgentSessionStateRecord{
					{SessionID: "session-1", AgentID: "agent-1", TaskID: "task-1", Status: "ACTIVE"},
				},
			},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-pause", Method: "runtime.pause", Payload: `{"reason":"manual pause"}`}); err != nil {
		t.Fatalf("handleRequest(runtime.pause) error = %v", err)
	}
	if len(savedStates) == 0 || !savedStates[len(savedStates)-1].ControlPaused || savedStates[len(savedStates)-1].ControlAction != "pause" {
		t.Fatalf("expected pause control state, got %+v", savedStates)
	}
	if runtime.runtimePaused() == false {
		t.Fatal("expected runtime to be paused after pause request")
	}

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-resume", Method: "runtime.resume", Payload: `{"reason":"resume now"}`}); err != nil {
		t.Fatalf("handleRequest(runtime.resume) error = %v", err)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "runtime_resume" || got.TaskID != "task-1" || got.SessionID != "session-1" {
		t.Fatalf("expected resume trigger, got %+v", got)
	}
	if runtime.runtimePaused() {
		t.Fatal("expected runtime to be live after resume request")
	}

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{RequestID: "req-switch-task", Method: "runtime.switch_task", Payload: `{"task_id":"task-2","reason":"reassign"}`}); err != nil {
		t.Fatalf("handleRequest(runtime.switch_task) error = %v", err)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "runtime_switch_task" || got.TaskID != "task-2" {
		t.Fatalf("expected switch task trigger, got %+v", got)
	}
	last := savedStates[len(savedStates)-1]
	if last.ControlTargetTaskID != "task-2" || last.ControlAction != "switch_task" || last.ControlMode != "task" {
		t.Fatalf("expected switch task control state, got %+v", last)
	}

	if len(responses) < 3 {
		t.Fatalf("expected three runtime control responses, got %d", len(responses))
	}
	if responses[0]["control"].(map[string]any)["paused"] != true {
		t.Fatalf("expected pause response to report paused=true, got %+v", responses[0])
	}
	if responses[1]["control"].(map[string]any)["paused"] != false {
		t.Fatalf("expected resume response to report paused=false, got %+v", responses[1])
	}
	if responses[2]["control"].(map[string]any)["target_task_id"] != "task-2" {
		t.Fatalf("expected switch_task response to include target task, got %+v", responses[2])
	}
}

func TestRuntimeHandleRequestRuntimeSwitchTensionAttachesAgent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var methods []string
	var attachParams map[string]any
	var savedStates []RuntimeScratchState
	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "workspace.tension.agent.attach":
			attachParams = req.Params
			if got := rpcString(req.Params, "actor_id"); got != "agent-1" {
				t.Fatalf("attach actor_id = %q", got)
			}
			if got := rpcString(req.Params, "success_criterion"); got != "Attached as: generalist" {
				t.Fatalf("attach success_criterion = %q", got)
			}
			if _, ok := req.Params["role"]; ok {
				t.Fatalf("legacy role field leaked into attach payload: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"success":      true,
				"changed":      true,
				"coalition_id": "coalition-live",
				"event": map[string]any{
					"event_id":   "rtev-attach",
					"event_type": "tension.agent.attached",
				},
			})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseJSON = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during runtime.switch_tension: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
			Role:         "generalist",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID: "req-switch-tension",
		Method:    "runtime.switch_tension",
		Payload:   `{"tension_id":"tension-1","reason":"focus the live attach","role":"generalist"}`,
	}); err != nil {
		t.Fatalf("handleRequest(runtime.switch_tension) error = %v", err)
	}
	if len(methods) != 3 || methods[0] != "workspace.tension.agent.attach" || methods[1] != "agent.state.set" || methods[2] != "agent.respond" {
		t.Fatalf("unexpected method trace: %#v", methods)
	}
	if attachParams["tension_id"] != "tension-1" || attachParams["agent_id"] != "agent-1" || attachParams["actor_id"] != "agent-1" {
		t.Fatalf("unexpected attach params: %+v", attachParams)
	}
	if len(savedStates) == 0 || savedStates[len(savedStates)-1].ControlTargetTensionID != "tension-1" || savedStates[len(savedStates)-1].ControlAction != "switch_tension" {
		t.Fatalf("expected tension switch to persist control state, got %+v", savedStates)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.switch_tension response: %v", err)
	}
	control, ok := parsed["control"].(map[string]any)
	if !ok || control["target_tension_id"] != "tension-1" {
		t.Fatalf("expected control target tension in response, got %#v", parsed["control"])
	}
	mutation, ok := parsed["tension_mutation"].(map[string]any)
	if !ok || mutation["runtime_event_id"] != "rtev-attach" || mutation["event_type"] != "tension.agent.attached" || mutation["coalition_id"] != "coalition-live" {
		t.Fatalf("expected attach mutation evidence in response, got %#v", parsed["tension_mutation"])
	}
}

func TestRuntimeHandleRequestRuntimeSwitchTensionDetachResolvesCoalition(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var methods []string
	var detachParams map[string]any
	var savedStates []RuntimeScratchState
	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "coalition.status":
			writeRPCResult(w, req, map[string]any{
				"coalitions": []map[string]any{
					{
						"coalition_id": "coalition-stale",
						"workspace_id": "ws-1",
						"tension_id":   "tension-1",
						"status":       "DISBANDED",
						"members":      []map[string]any{{"agent_id": "agent-1"}},
					},
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
			detachParams = req.Params
			if got := rpcString(req.Params, "coalition_id"); got != "coalition-live" {
				t.Fatalf("detach coalition_id = %q", got)
			}
			if got := rpcString(req.Params, "actor_id"); got != "agent-1" {
				t.Fatalf("detach actor_id = %q", got)
			}
			if got := rpcString(req.Params, "agent_id"); got != "agent-1" {
				t.Fatalf("detach agent_id = %q", got)
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
					"status":       "DISBANDED",
				},
				"event": map[string]any{
					"event_id":   "rtev-detach",
					"event_type": "tension.agent.detached",
				},
			})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseJSON = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during runtime.switch_tension detach: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
			Role:         "generalist",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID: "req-switch-tension-detach",
		Method:    "runtime.switch_tension",
		Payload:   `{"tension_id":"tension-1","action":"detach","reason":"done"}`,
	}); err != nil {
		t.Fatalf("handleRequest(runtime.switch_tension detach) error = %v", err)
	}
	if len(methods) != 4 || methods[0] != "coalition.status" || methods[1] != "workspace.tension.agent.detach" || methods[2] != "agent.state.set" || methods[3] != "agent.respond" {
		t.Fatalf("unexpected method trace: %#v", methods)
	}
	if detachParams == nil {
		t.Fatal("expected detach params to be captured")
	}
	last := savedStates[len(savedStates)-1]
	if last.ControlTargetTensionID != "tension-1" || last.ControlTensionAction != "detach" || last.ControlAction != "switch_tension" {
		t.Fatalf("expected detach tension switch state, got %+v", last)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.switch_tension detach response: %v", err)
	}
	control, ok := parsed["control"].(map[string]any)
	if !ok || control["target_tension_id"] != "tension-1" || control["target_tension_action"] != "detach" {
		t.Fatalf("expected detach control target tension in response, got %#v", parsed["control"])
	}
	mutation, ok := parsed["tension_mutation"].(map[string]any)
	if !ok || mutation["runtime_event_id"] != "rtev-detach" || mutation["event_type"] != "tension.agent.detached" || mutation["coalition_id"] != "coalition-live" {
		t.Fatalf("expected detach mutation evidence in response, got %#v", parsed["tension_mutation"])
	}
}

func TestRuntimeHandleRequestRuntimeSwitchTensionDoesNotPersistWithoutMutationEvent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var stateSetCalled bool
	var responseJSON string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.tension.agent.attach":
			writeRPCResult(w, req, map[string]any{
				"success":      true,
				"changed":      true,
				"coalition_id": "coalition-live",
			})
		case "agent.state.set":
			stateSetCalled = true
			t.Fatalf("runtime scratch must not persist after changed mutation without runtime event")
		case "agent.respond":
			responseJSON = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during runtime.switch_tension missing-event check: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
			Role:         "generalist",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID: "req-switch-tension-missing-event",
		Method:    "runtime.switch_tension",
		Payload:   `{"tension_id":"tension-1","reason":"focus"}`,
	}); err != nil {
		t.Fatalf("handleRequest(runtime.switch_tension missing event) error = %v", err)
	}
	if stateSetCalled {
		t.Fatal("runtime scratch persisted after missing mutation event")
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(responseJSON), &parsed); err != nil {
		t.Fatalf("decode runtime.switch_tension missing-event response: %v", err)
	}
	if parsed["status"] != "error" {
		t.Fatalf("expected error status for missing event, got %+v", parsed)
	}
	if !strings.Contains(strings.ToLower(rpcStringMap(parsed, "details")), "runtime event") {
		t.Fatalf("expected missing runtime event details, got %+v", parsed)
	}
	if _, ok := parsed["tension_mutation"]; ok {
		t.Fatalf("mutation evidence should not be reported for failed mutation evidence, got %+v", parsed["tension_mutation"])
	}
}
