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

func TestRuntimeHandleInboundMessageQueuesWorkTriggerWithoutDirectResume(t *testing.T) {
	var methods []string
	var savedStates []RuntimeScratchState
	var updatePayload map[string]any

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
		case "agent.update.post":
			if raw := rpcString(req.Params, "payload_json"); raw != "" {
				if err := json.Unmarshal([]byte(raw), &updatePayload); err != nil {
					t.Fatalf("decode update payload: %v", err)
				}
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-1",
			TaskID:    "task-1",
			Status:    "WAITING_DECISION",
		},
	}

	err := runtime.handleInboundMessage(context.Background(), MessageRecord{
		MessageID:    "msg-1",
		FromAgentID:  "agent-owner",
		ToAgentID:    "agent-1",
		Content:      "Approved, continue",
		MetadataJSON: `{"task_id":"task-1"}`,
	})
	if err != nil {
		t.Fatalf("handleInboundMessage() error = %v", err)
	}

	if len(methods) != 3 || methods[0] != "agent.state.set" || methods[1] != "agent.state.set" || methods[2] != "agent.update.post" {
		t.Fatalf("unexpected method sequence: %#v", methods)
	}
	if len(savedStates) == 0 {
		t.Fatal("expected scratch state writes")
	}
	last := savedStates[len(savedStates)-1]
	if last.PendingTrigger != "inbound_message" || last.PendingTriggerTask != "task-1" || last.PendingTriggerSession != "session-1" {
		t.Fatalf("expected queued work trigger in scratch, got %+v", last)
	}
	if last.ActiveTaskID != "" || last.ActiveSessionID != "" || last.ActiveRunID != "" {
		t.Fatalf("inbound focus must not become active tool/runtime binding, got %+v", last)
	}
	for _, method := range methods {
		if method == "agent.session.status" {
			t.Fatalf("listener path should no longer resume session directly: %#v", methods)
		}
	}
	if updatePayload["status"] != "INBOUND_MESSAGE" {
		t.Fatalf("expected inbound update payload, got %+v", updatePayload)
	}
}

func TestRuntimeToolExecutionContextIgnoresScratchTaskWithoutSession(t *testing.T) {
	runtime := &Runtime{
		scratch: RuntimeScratchState{
			ActiveTaskID: "task-legacy-focus",
			DocSHAs:      map[string]string{},
		},
	}

	taskID, sessionID, runID := runtime.currentToolExecutionContext()
	if taskID != "" || sessionID != "" || runID != "" {
		t.Fatalf("scratch task without durable session must not become tool binding, got task=%q session=%q run=%q", taskID, sessionID, runID)
	}
}

func TestRuntimeEnsureRunnableTaskUsesResumePacketInsteadOfStartingNewSession(t *testing.T) {
	var methods []string
	var statusCalls int
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "inbound_message" || rpcString(req.Params, "candidate_task_id") != "task-1" || rpcString(req.Params, "candidate_session_id") != "session-1" {
				t.Fatalf("expected work.next trigger hints, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"workspace_id":   "ws",
				"agent_id":       "agent-1",
				"has_work":       true,
				"reason":         "resume_session",
				"trigger":        "inbound_message",
				"claim_action":   "reuse_claim",
				"session_action": "resume_inactive",
				"resume_summary": "Decision context changed after inbound message; resume session",
				"task": map[string]any{
					"task_id":        "task-1",
					"title":          "Task One",
					"description":    "resume blocked work",
					"owner_user_id":  "owner-1",
					"priority":       "HIGH",
					"status":         "RUNNING",
					"task_kind":      "general",
					"task_template":  "default",
					"linked_by":      "system",
					"linked_at":      "2026-03-23T00:00:00Z",
					"claim_agent_id": "agent-1",
					"claim_status":   "BLOCKED",
				},
				"session": map[string]any{
					"session_id":   "session-1",
					"workspace_id": "ws",
					"agent_id":     "agent-1",
					"task_id":      "task-1",
					"status":       "WAITING_DECISION",
					"summary":      "need approval",
					"updated_at":   "2026-03-23T00:00:00Z",
					"started_at":   "2026-03-23T00:00:00Z",
				},
			})
		case "agent.task.claim":
			if rpcString(req.Params, "task_id") != "task-1" || rpcString(req.Params, "agent_id") != "agent-1" {
				t.Fatalf("unexpected canonical reclaim params: %+v", req.Params)
			}
			writeRPCResult(w, req, nil)
		case "agent.session.status":
			statusCalls++
			if rpcString(req.Params, "session_id") != "session-1" || rpcString(req.Params, "task_id") != "task-1" {
				t.Fatalf("unexpected session resume params: %+v", req.Params)
			}
			if rpcString(req.Params, "summary") != "Decision context changed after inbound message; resume session" {
				t.Fatalf("expected deterministic resume summary, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          "session-1",
					"workspace_id":        "ws",
					"agent_id":            "agent-1",
					"task_id":             "task-1",
					"status":              "ACTIVE",
					"summary":             rpcString(req.Params, "summary"),
					"updated_at":          "2026-03-23T00:00:01Z",
					"started_at":          "2026-03-23T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{
				"run": map[string]any{"run_id": "run-1"},
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-1"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			PendingTrigger:        "inbound_message",
			PendingTriggerTask:    "task-1",
			PendingTriggerSession: "session-1",
			DocSHAs:               map[string]string{},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task == nil || task.TaskID != "task-1" {
		t.Fatalf("unexpected selected task: %+v", task)
	}
	if statusCalls != 1 {
		t.Fatalf("expected one deterministic session resume, got %d", statusCalls)
	}
	if !strings.Contains(strings.Join(methods, ","), "agent.task.claim") {
		t.Fatalf("expected blocked task wake to canonically reclaim before execution, got %#v", methods)
	}
	if runtime.activeSession == nil || runtime.activeSession.Status != "ACTIVE" {
		t.Fatalf("expected active resumed session, got %+v", runtime.activeSession)
	}
	if runtime.activeRunID != "run-1" {
		t.Fatalf("expected run-1, got %q", runtime.activeRunID)
	}
	if lastScratch.PendingTrigger != "" || lastScratch.PendingTriggerTask != "" || lastScratch.PendingTriggerSession != "" {
		t.Fatalf("expected trigger to be cleared after selection, got %+v", lastScratch)
	}
	for _, method := range methods {
		if method == "agent.session.start" {
			t.Fatalf("resume packet should avoid session.start, got %#v", methods)
		}
	}
	if !strings.Contains(strings.Join(methods, ","), "agent.session.status") {
		t.Fatalf("expected session.status in method trace, got %#v", methods)
	}
}

func TestPrepareSessionForSelectedWorkStartsNewWhenReuseActiveSessionEnded(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.session.start":
			if got := rpcString(req.Params, "session_id"); got == "session-ended" {
				t.Fatalf("must not reuse ended session: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        "ws",
				"agent_id":            "agent-1",
				"task_id":             "task-1",
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-03-23T00:00:01Z",
				"started_at":          "2026-03-23T00:00:01Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			if rpcString(req.Params, "session_id") == "session-ended" {
				t.Fatalf("execution run should bind to fresh session: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-1",
		Title:       "Task One",
		Description: "fresh session after stale reuse packet",
	}
	session, runID, err := runtime.prepareSessionForSelectedWork(context.Background(), task, AgentWorkNextResult{
		SessionAction: "reuse_active",
		Session: &AgentSessionStateRecord{
			SessionID: "session-ended",
			TaskID:    "task-1",
			AgentID:   "agent-1",
			Status:    "ENDED",
		},
	})
	if err != nil {
		t.Fatalf("prepareSessionForSelectedWork() error = %v", err)
	}
	if session == nil || session.SessionID == "" || session.SessionID == "session-ended" || session.Status != "ACTIVE" {
		t.Fatalf("expected fresh active session, got %+v", session)
	}
	if runID == "" {
		t.Fatal("expected execution run id")
	}
	if strings.Join(methods, ",") != "agent.session.start,workspace.execution.run.write" {
		t.Fatalf("expected fresh start flow, got %#v", methods)
	}
}

func TestRuntimeEnsureRunnableTaskSkipsBlockedClaimWithoutExplicitWake(t *testing.T) {
	var methods []string
	var lastScratch RuntimeScratchState
	currentContextDocWrites := 0
	claimedWorkDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"workspace_id":   "ws",
				"agent_id":       "agent-1",
				"has_work":       true,
				"reason":         "resume_session",
				"claim_action":   "reuse_claim",
				"session_action": "resume_inactive",
				"task": map[string]any{
					"task_id":        "task-1",
					"title":          "Task One",
					"description":    "blocked work should stay parked",
					"owner_user_id":  "owner-1",
					"priority":       "HIGH",
					"status":         "RUNNING",
					"task_kind":      "general",
					"task_template":  "default",
					"linked_by":      "system",
					"linked_at":      "2026-03-23T00:00:00Z",
					"claim_agent_id": "agent-1",
					"claim_status":   "BLOCKED",
				},
				"session": map[string]any{
					"session_id":   "session-1",
					"workspace_id": "ws",
					"agent_id":     "agent-1",
					"task_id":      "task-1",
					"status":       "BLOCKED",
					"summary":      "need dependency",
					"updated_at":   "2026-03-23T00:00:00Z",
					"started_at":   "2026-03-23T00:00:00Z",
				},
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey("agent-1"):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected blocked parked claim to clear current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-blocked-context-cleared"})
			case claimedWorkDocKey("agent-1"):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected blocked parked claim to clear claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-blocked-claimed-cleared"})
			default:
				t.Fatalf("blocked claim without wake should not materialize doc %q: %+v", rpcString(req.Params, "doc_key"), req.Params)
			}
		default:
			t.Fatalf("blocked claim without wake should not call %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client:  NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}, ActiveTaskID: "task-1", ActiveSessionID: "session-1", ActiveRunID: "run-1"},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("expected blocked task without explicit wake to stay idle, got %+v", task)
	}
	trace := strings.Join(methods, ",")
	for _, forbidden := range []string{"agent.task.claim", "agent.session.status", "workspace.execution.run.write"} {
		if strings.Contains(trace, forbidden) {
			t.Fatalf("expected no %s for parked blocked claim, got %#v", forbidden, methods)
		}
	}
	if lastScratch.ActiveTaskID != "" || lastScratch.ActiveSessionID != "" || lastScratch.ActiveRunID != "" {
		t.Fatalf("expected parked blocked claim to clear active scratch, got %+v", lastScratch)
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected parked blocked claim to publish one current-context and one claimed-work cleanup doc, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
}

func TestRuntimeEnsureRunnableTaskSelfSelectsFromTaskFrontier(t *testing.T) {
	var methods []string
	var claimPayload map[string]any

	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-ui","self_fit_summary":"browser and visual QA tools fit this UI task","reason":"frontier fit is recommended"}`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			if got, _ := req.Params["enable_task_frontier"].(bool); !got {
				t.Fatalf("expected runtime to request task frontier in trust_first mode, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-23T00:00:00Z",
				"workspace_id": "ws",
				"agent_id":     "agent-ui",
				"has_work":     true,
				"reason":       "task_frontier_available",
				"packet": map[string]any{
					"work_type":            "task_frontier_available",
					"coordination_state":   "frontier_available",
					"preferred_transition": "self_select_task",
					"frontier": map[string]any{
						"generation_id":  "frontier-1",
						"generated_at":   "2026-03-23T00:00:00Z",
						"selection_mode": "agent_self_select",
						"candidates": []map[string]any{
							{
								"task": map[string]any{
									"task_id":                "task-ui",
									"title":                  "Inspect web app UI",
									"description":            "Use browser visual QA tools.",
									"owner_user_id":          "owner-1",
									"priority":               "HIGH",
									"status":                 "PENDING",
									"task_kind":              "EXECUTION",
									"task_template":          "generic",
									"linked_by":              "system",
									"linked_at":              "2026-03-23T00:00:00Z",
									"requires_project_gate":  false,
									"task_requirements_json": `{"schema":"task_requirements.v1","write_scope_hints":["src/ui/**"]}`,
									"write_scope_hints":      []string{"src/ui/**"},
									"tags":                   []string{"ui", "browser", "visual-qa"},
								},
								"fit": map[string]any{
									"level":            "recommended",
									"score":            91,
									"reasons":          []string{"profile work mode matches task mode", "tool access matches task hints"},
									"preferred_tools":  []string{"browser"},
									"preferred_skills": []string{"ui", "visual-qa"},
								},
								"claim_action":   "claim_required",
								"session_action": "start_new",
							},
							{
								"task": map[string]any{
									"task_id":       "task-blocked",
									"title":         "Blocked candidate",
									"owner_user_id": "owner-1",
									"priority":      "NORMAL",
									"status":        "PENDING",
									"task_kind":     "EXECUTION",
									"task_template": "generic",
									"linked_by":     "system",
									"linked_at":     "2026-03-23T00:00:00Z",
								},
								"fit":          map[string]any{"level": "blocked", "score": 0},
								"blocked":      true,
								"block_reason": "task_dependency_blocked",
							},
						},
						"roster": []map[string]any{
							{"agent_id": "agent-ui", "role": "ui/ux", "busyness": "idle", "tools_access": []string{"browser"}},
						},
					},
				},
			})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-ui" {
				t.Fatalf("expected hydration for selected task-ui, got %+v", req.Params)
			}
			docKeys := rpcStringSlice(req.Params, "doc_keys")
			if !containsAll(docKeys, []string{"task.task-ui", "task.task-ui.result", "current_context"}) {
				t.Fatalf("expected selected frontier hydration to include task doc keys, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-ui", "write_scope_hints": []string{"src/ui/**"}},
				"task":           map[string]any{"task_id": "task-ui", "write_scope_hints": []string{"src/ui/**"}},
			}})
		case "agent.task_frontier.decision":
			if rpcString(req.Params, "frontier_generation_id") != "frontier-1" || rpcString(req.Params, "decision_state") != "selected" || rpcString(req.Params, "selected_task_id") != "task-ui" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.claim":
			claimPayload = req.Params
			if rpcString(req.Params, "task_id") != "task-ui" || rpcString(req.Params, "frontier_generation_id") != "frontier-1" {
				t.Fatalf("unexpected frontier claim params: %+v", req.Params)
			}
			if selected, _ := req.Params["selected_from_frontier"].(bool); !selected {
				t.Fatalf("expected selected_from_frontier claim evidence, got %+v", req.Params)
			}
			if !strings.Contains(rpcString(req.Params, "self_fit_summary"), "browser") {
				t.Fatalf("expected self_fit_summary evidence, got %+v", req.Params)
			}
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			sessionID := rpcString(req.Params, "session_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          sessionID,
				"workspace_id":        "ws",
				"agent_id":            "agent-ui",
				"task_id":             "task-ui",
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-03-23T00:00:01Z",
				"started_at":          "2026-03-23T00:00:01Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-ui",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
			Workdir:          t.TempDir(),
		},
		client:  NewRhizomeClient(server.URL, "token"),
		agent:   &Agent{LLM: llm},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task == nil || task.TaskID != "task-ui" {
		t.Fatalf("expected self-selected task-ui, got %+v", task)
	}
	if strings.Join(task.WriteScopeHints, ",") != "src/ui/**" {
		t.Fatalf("expected selected task to retain write_scope_hints, got %+v", task)
	}
	if claimPayload == nil {
		t.Fatal("expected claim payload")
	}
	if llm.callCount() != 1 {
		t.Fatalf("expected one frontier choice LLM call, got %d", llm.callCount())
	}
	if !containsAll(methods, []string{"agent.work.next", "agent.task.hydrate", "agent.task.claim", "agent.session.start", "workspace.execution.run.write"}) {
		t.Fatalf("expected frontier self-selection activation flow, got %#v", methods)
	}
}

func TestRuntimeTaskFrontierPreemptsRootWithProjectClaimRepair(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-clearpress-root","self_fit_summary":"root strategy looks active","reason":"root is mine"}`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			if rpcString(req.Params, "decision_state") != "selected" || rpcString(req.Params, "selected_task_id") != "task-project-claim-repair-abc123" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-project-claim-repair-abc123" {
				t.Fatalf("expected hydration for project claim repair task, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-05-24T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-project-claim-repair-abc123"},
				"task":           map[string]any{"task_id": "task-project-claim-repair-abc123"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{LLM: llm},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-claim-repair",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:      "task-clearpress-root",
							Title:       "Clearpress autonomous product MVP run",
							OwnerUserID: "owner-1",
							Priority:    "HIGH",
							Status:      "PENDING",
							TaskKind:    "COORDINATION",
							ProjectID:   "project-clearpress",
							ProjectLane: "strategy",
							LinkedBy:    "system",
							LinkedAt:    "2026-05-24T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:      "task-project-claim-repair-abc123",
							Title:       "Repair project claim scope conflict",
							Description: "A project implementation lane is blocked by an overlapping write scope. Claim this task as the active strategic lead and repair the project coordination state instead of waiting.",
							OwnerUserID: "owner-1",
							Priority:    "HIGH",
							Status:      "PENDING",
							TaskKind:    "COORDINATION",
							ProjectID:   "project-clearpress",
							ProjectLane: "strategy",
							LinkedBy:    "system",
							LinkedAt:    "2026-05-24T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "plausible", Score: 70},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-project-claim-repair-abc123" {
		t.Fatalf("expected project claim repair to preempt root frontier choice, selected=%t work=%+v", selected, work)
	}
	if got := work.Packet.Frontier.SelectedTaskID; got != "task-project-claim-repair-abc123" {
		t.Fatalf("selected frontier task = %q", got)
	}
	if work.Packet.Gate == nil || work.Packet.Gate.GateType != "project_claim_repair_authority_transition" {
		t.Fatalf("expected project claim repair authority gate, got %+v", work.Packet)
	}
	if work.Packet.PreferredTransition != "project_claim_repair_receipt" || work.Packet.Gate.NeededFrom != "project_claim_repair_receipt" {
		t.Fatalf("expected project claim repair receipt transition, packet=%+v", work.Packet)
	}
	if llm.callCount() != 0 {
		t.Fatalf("authority repair frontier selection should not consult LLM, got %d calls", llm.callCount())
	}
}

func TestRuntimeTaskFrontierProductPressurePreemptsCoordinationChoice(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-rq-coordinate","self_fit_summary":"coordination looks easy","reason":"fresh split"}`}
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			if rpcString(req.Params, "project_id") != "project-rq" {
				t.Fatalf("expected project-rq coordination lookup, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"project": map[string]any{"project_id": "project-rq"},
				"branches": []map[string]any{{
					"branch_id":  "branch-gamma",
					"project_id": "project-rq",
					"status":     "READY_FOR_REVIEW",
				}},
				"patch_queue_items": []map[string]any{{
					"queue_id":   "queue-rq",
					"item_id":    "item-rq",
					"project_id": "project-rq",
					"state":      "BLOCKED",
				}},
			}})
		case "agent.task_frontier.decision":
			if rpcString(req.Params, "decision_state") != "selected" || rpcString(req.Params, "selected_task_id") != "task-rq-eval" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-rq-eval" {
				t.Fatalf("expected hydration for product task, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-06-06T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-rq-eval", "project_id": "project-rq", "project_lane": "implementation"},
				"task":           map[string]any{"task_id": "task-rq-eval", "project_id": "project-rq", "project_lane": "implementation"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{LLM: llm},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID:  "frontier-product-pressure",
				GeneratedAt:   "2026-06-06T00:00:00Z",
				SelectionMode: "agent_self_select",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:      "task-rq-coordinate",
							Title:       "Coordinate rq repair split",
							Description: "Create another coordination split for rq internal/eval import resolution.",
							OwnerUserID: "owner-1",
							Priority:    "HIGH",
							Status:      "PENDING",
							TaskKind:    "COORDINATION",
							ProjectID:   "project-rq",
							ProjectLane: "coordination",
							LinkedBy:    "system",
							LinkedAt:    "2026-06-06T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 98},
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:          "task-rq-eval",
							Title:           "Repair rq internal/eval import resolution",
							Description:     "Fix the concrete rq internal/eval import resolution failure.",
							OwnerUserID:     "owner-1",
							Priority:        "HIGH",
							Status:          "PENDING",
							TaskKind:        "EXECUTION",
							ProjectID:       "project-rq",
							ProjectLane:     "implementation",
							WriteScopeHints: []string{"internal/eval/**", "go.mod"},
							LinkedBy:        "system",
							LinkedAt:        "2026-06-06T00:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "plausible", Score: 61},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-rq-eval" {
		t.Fatalf("expected product task to preempt coordination choice, selected=%t work=%+v", selected, work)
	}
	if got := work.Packet.Frontier.SelectedTaskID; got != "task-rq-eval" {
		t.Fatalf("selected frontier task = %q", got)
	}
	if llm.callCount() != 0 {
		t.Fatalf("product pressure frontier selection should not consult LLM, got %d calls", llm.callCount())
	}
	if !containsAll(methods, []string{"project.coordination.get", "agent.task_frontier.decision", "agent.task.hydrate"}) {
		t.Fatalf("expected pressure lookup, decision, and hydration, got %#v", methods)
	}
}

func TestRuntimeTaskFrontierDeclineLeavesTaskUnselected(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"decline","task_id":"","self_fit_summary":"","reason":"browser-only candidate belongs to a peer"}`}
	runtime := &Runtime{agent: &Agent{LLM: llm}}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-decline",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-peer",
							Title:        "Peer-shaped work",
							OwnerUserID:  "owner-1",
							Priority:     "NORMAL",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "weak_fit", Score: 20},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.Task != nil {
		t.Fatalf("expected frontier decline to leave task unselected, selected=%t work=%+v", selected, work)
	}
	if work.Packet.Frontier.DeclineSummary == "" {
		t.Fatalf("expected decline summary to be recorded, got %+v", work.Packet.Frontier)
	}
	if llm.callCount() != 1 {
		t.Fatalf("expected one LLM decline call, got %d", llm.callCount())
	}
}

func TestRuntimeTaskFrontierNoSelectableCandidatesDeclinesWithoutModel(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-blocked","self_fit_summary":"bad","reason":"bad"}`}
	runtime := &Runtime{agent: &Agent{LLM: llm}}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-blocked-only",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:      "task-blocked",
							Title:       "Blocked implementation",
							OwnerUserID: "owner-1",
							Priority:    "HIGH",
							Status:      "PENDING",
							TaskKind:    "EXECUTION",
							ProjectID:   "project-rq",
							ProjectLane: "implementation",
							LinkedBy:    "system",
							LinkedAt:    "2026-06-17T00:00:00Z",
						},
						Fit:          AgentWorkTaskFit{Level: "blocked", Score: 0},
						Blocked:      true,
						BlockReason:  "project_claim_scope_busy",
						BlockSummary: "Project implementation write scope is busy.",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.HasWork || work.Task != nil {
		t.Fatalf("expected blocked-only frontier to decline without runnable work, selected=%t work=%+v", selected, work)
	}
	if work.Reason != "task_frontier_no_selectable_candidate" {
		t.Fatalf("work reason = %q, want task_frontier_no_selectable_candidate", work.Reason)
	}
	if work.Packet.Frontier.DeclineSummary != "frontier contained no selectable candidates" {
		t.Fatalf("decline summary = %q", work.Packet.Frontier.DeclineSummary)
	}
	if llm.callCount() != 0 {
		t.Fatalf("blocked-only frontier must not consult LLM, got %d calls", llm.callCount())
	}
}

func TestRuntimeTaskFrontierDoesNotSelectForeignAssignedPatchQueueValidation(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-patchq-validation-tokenizer","self_fit_summary":"I can validate it","reason":"validation task is visible"}`}
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			t.Fatalf("foreign assigned validation task must not hydrate before claim: %+v", req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "zeta"},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{LLM: llm},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-foreign-validation",
				Roster: []AgentWorkRosterAgent{
					{AgentID: "epsilon", Status: "ACTIVE", IsOnline: true},
					{AgentID: "zeta", Status: "ACTIVE", IsOnline: true},
				},
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:               "task-patchq-validation-tokenizer",
							Title:                "Validate blocked tokenizer candidate",
							OwnerUserID:          "epsilon",
							Priority:             "HIGH",
							Status:               "PENDING",
							TaskKind:             "EXECUTION",
							TaskTemplate:         "generic",
							ProjectID:            "project-signal01-rq-s1",
							ProjectLane:          "validation",
							TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_kind":"validation","queue_id":"patchq-project-signal01-rq-s1-repo-signal01-rq-core","item_id":"patchitem-projbranch-1781210598654043671-518","branch_id":"projbranch-1781210598654043671-518","head_sha":"598afe2690eede9258bcc2c9f7193cbf8aa7511e"}`,
							LinkedBy:             "system",
							LinkedAt:             "2026-06-11T21:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "recommended", Score: 95},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.Task != nil {
		t.Fatalf("foreign assigned validation task must not be selected, selected=%t work=%+v", selected, work)
	}
	if len(decisions) != 1 || rpcString(decisions[0], "decision_state") != "declined" {
		t.Fatalf("expected declined frontier decision, got %+v", decisions)
	}
	if llm.callCount() != 1 {
		t.Fatalf("expected one LLM choice attempt before local hard-assignment guard, got %d", llm.callCount())
	}
}

func TestRuntimeTaskFrontierAllowsOperatorOwnedPatchQueueValidation(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-patchq-validation-operator","self_fit_summary":"I can validate it","reason":"operator-owned validation is not hard-routed to a peer agent"}`}
	hydrated := false
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			hydrated = true
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at": "2026-06-11T21:00:00Z",
				"task": map[string]any{
					"task_id":       "task-patchq-validation-operator",
					"title":         "Validate blocked tokenizer candidate",
					"owner_user_id": "developer",
					"priority":      "HIGH",
					"status":        "PENDING",
					"task_kind":     "EXECUTION",
					"task_template": "generic",
					"project_id":    "project-signal01-rq-s1",
					"project_lane":  "validation",
					"linked_by":     "system",
					"linked_at":     "2026-06-11T21:00:00Z",
				},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "zeta"},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{LLM: llm},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-operator-validation",
				Roster: []AgentWorkRosterAgent{
					{AgentID: "epsilon", Status: "ACTIVE", IsOnline: true},
					{AgentID: "zeta", Status: "ACTIVE", IsOnline: true},
				},
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:               "task-patchq-validation-operator",
							Title:                "Validate blocked tokenizer candidate",
							OwnerUserID:          "developer",
							Priority:             "HIGH",
							Status:               "PENDING",
							TaskKind:             "EXECUTION",
							TaskTemplate:         "generic",
							ProjectID:            "project-signal01-rq-s1",
							ProjectLane:          "validation",
							TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_kind":"validation","queue_id":"patchq-project-signal01-rq-s1-repo-signal01-rq-core","item_id":"patchitem-projbranch-1781210598654043671-518","branch_id":"projbranch-1781210598654043671-518","head_sha":"598afe2690eede9258bcc2c9f7193cbf8aa7511e"}`,
							LinkedBy:             "system",
							LinkedAt:             "2026-06-11T21:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "recommended", Score: 95},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-patchq-validation-operator" || !hydrated {
		t.Fatalf("operator-owned validation task should remain selectable, selected=%t hydrated=%t work=%+v", selected, hydrated, work)
	}
	if len(decisions) != 1 || rpcString(decisions[0], "decision_state") != "selected" {
		t.Fatalf("expected selected frontier decision, got %+v", decisions)
	}
}

func TestRuntimeTaskFrontierDeclineFallsBackToBoundedProductCandidate(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"decline","task_id":"","self_fit_summary":"","reason":"uncertain fit despite product lane"}`}
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-editor" {
				t.Fatalf("expected hydration for task-editor, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-05-31T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-editor", "project_id": "project-alpha", "project_lane": "implementation", "write_scope_hints": []string{"src/editor/**"}},
				"task":           map[string]any{"task_id": "task-editor", "project_id": "project-alpha", "project_lane": "implementation", "write_scope_hints": []string{"src/editor/**"}},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-editor"},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{LLM: llm},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID:  "frontier-product",
				GeneratedAt:   "2026-05-31T00:00:00Z",
				SelectionMode: "agent_self_select",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:              "task-editor",
							Title:               "Implement editor shortcuts",
							OwnerUserID:         "owner-1",
							Priority:            "HIGH",
							Status:              "PENDING",
							TaskKind:            "EXECUTION",
							TaskTemplate:        "generic",
							ProjectID:           "project-alpha",
							ProjectLane:         "implementation",
							RequiresProjectGate: boolPtr(true),
							WriteScopeHints:     []string{"src/editor/**"},
							LinkedBy:            "system",
							LinkedAt:            "2026-05-31T00:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "recommended", Score: 92, Reasons: []string{"profile and write scope match"}},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-editor" {
		t.Fatalf("expected bounded product fallback selection, selected=%t work=%+v", selected, work)
	}
	if work.Packet.Frontier.SelectedTaskID != "task-editor" || !strings.Contains(work.Packet.Frontier.SelfFitSummary, "bounded product candidate") {
		t.Fatalf("unexpected frontier selection evidence: %+v", work.Packet.Frontier)
	}
	if len(decisions) != 1 || rpcString(decisions[0], "decision_state") != "selected" || rpcString(decisions[0], "selected_task_id") != "task-editor" {
		t.Fatalf("unexpected frontier decisions: %+v", decisions)
	}
}

func TestRuntimeTaskFrontierModelFailureDeclinesWithoutDeterministicClaim(t *testing.T) {
	runtime := &Runtime{}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-model-failure",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-ui",
							Title:        "Inspect web app UI",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.Task != nil {
		t.Fatalf("model failure must not deterministically claim frontier work, selected=%t work=%+v", selected, work)
	}
	if !strings.Contains(work.Packet.Frontier.DeclineSummary, "model choice failed") {
		t.Fatalf("expected model failure decline evidence, got %+v", work.Packet.Frontier)
	}
}

func TestRuntimeTaskFrontierHydrationFailureFailsClosed(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-ui","self_fit_summary":"browser and visual QA tools fit this UI task","reason":"frontier fit is recommended"}`}
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-ui" {
				t.Fatalf("expected hydration for task-ui, got %+v", req.Params)
			}
			writeRPCError(w, req, -32603, "hydration unavailable")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "agent-ui"},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{LLM: llm},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID:  "frontier-hydration-failed",
				GeneratedAt:   "2026-05-26T00:00:00Z",
				SelectionMode: "agent_self_select",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-ui",
							Title:        "Inspect UI",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							LinkedBy:     "system",
							LinkedAt:     "2026-05-26T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 91},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.HasWork || work.Task != nil || work.Reason != "task_frontier_hydration_failed" {
		t.Fatalf("hydration failure must fail closed without runnable work, selected=%t work=%+v", selected, work)
	}
	if work.Packet == nil || work.Packet.Frontier == nil || !strings.Contains(work.Packet.Frontier.DeclineSummary, "failed hydration") {
		t.Fatalf("expected hydration failure decline summary, got %+v", work.Packet)
	}
	if len(decisions) != 2 {
		t.Fatalf("expected selected and hydration_failed decision receipts, got %+v", decisions)
	}
	if rpcString(decisions[0], "decision_state") != "selected" || rpcString(decisions[0], "selected_task_id") != "task-ui" {
		t.Fatalf("unexpected selected decision receipt: %+v", decisions[0])
	}
	if rpcString(decisions[1], "decision_state") != "hydration_failed" || rpcString(decisions[1], "selected_task_id") != "task-ui" {
		t.Fatalf("unexpected hydration failure receipt: %+v", decisions[1])
	}
}

func TestRuntimeTaskFrontierModelFailureReclaimsReleasedSameAgentTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			if rpcString(req.Params, "decision_state") != "selected" || rpcString(req.Params, "selected_task_id") != "task-released-validation" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-released-validation" {
				t.Fatalf("expected hydration for released owned task, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-released-validation"},
				"task":           map[string]any{"task_id": "task-released-validation"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	claimAgent := "delta"
	claimStatus := "RELEASED"
	runtime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "delta"}, client: NewRhizomeClient(server.URL, "token")}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-released-owner",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-general",
							Title:        "General implementation work",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-released-validation",
							Title:        "Capture browser evidence",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ClaimAgentID: &claimAgent,
							ClaimStatus:  &claimStatus,
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "plausible", Score: 60},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-released-validation" {
		t.Fatalf("expected released same-agent task reclaim fallback, selected=%t work=%+v", selected, work)
	}
}

func TestRuntimeTaskFrontierModelFailureDoesNotReclaimForeignReleasedTask(t *testing.T) {
	claimAgent := "delta"
	claimStatus := "RELEASED"
	runtime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "theta"}}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-foreign-released-owner",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-released-validation",
							Title:        "Capture browser evidence",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ClaimAgentID: &claimAgent,
							ClaimStatus:  &claimStatus,
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.Task != nil {
		t.Fatalf("foreign released claim must not become deterministic local reclaim, selected=%t work=%+v", selected, work)
	}
	if !strings.Contains(work.Packet.Frontier.DeclineSummary, "model choice failed") {
		t.Fatalf("expected model failure decline evidence, got %+v", work.Packet.Frontier)
	}
}

func TestRuntimeTaskFrontierDeclineFallsBackToRequiredOwnerBoundTask(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"decline","task_id":"","self_fit_summary":"","reason":"revision work looks too broad"}`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			if state := rpcString(req.Params, "decision_state"); state == "selected" {
				if rpcString(req.Params, "selected_task_id") != "task-owner-revision" {
					t.Fatalf("unexpected selected frontier decision params: %+v", req.Params)
				}
			} else if state != "model_failed" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-owner-revision" {
				t.Fatalf("expected hydration for owner-bound task, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-owner-revision"},
				"task":           map[string]any{"task_id": "task-owner-revision"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "eta"},
		agent:  &Agent{LLM: llm},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-owner-bound",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-peer",
							Title:        "General implementation work",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-owner-revision",
							Title:        "Unblock integration candidate branch-1",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "required-agent:eta", "owner-agent:eta"},
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "plausible", Score: 60},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-owner-revision" {
		t.Fatalf("expected required owner-bound fallback selection, selected=%t work=%+v", selected, work)
	}
	if got := work.Packet.Frontier.SelectedTaskID; got != "task-owner-revision" {
		t.Fatalf("selected task id = %q, want owner-bound revision", got)
	}
	if !strings.Contains(work.Packet.Frontier.SelfFitSummary, "owner-bound") {
		t.Fatalf("expected owner-bound self-fit summary, got %+v", work.Packet.Frontier)
	}
	if llm.callCount() != 1 {
		t.Fatalf("expected one LLM choice attempt, got %d", llm.callCount())
	}
}

func TestRuntimeTaskFrontierHeldOrdinaryChoiceFallsBackToRequiredOwnerBoundTask(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-stale-sidecar","self_fit_summary":"I can publish provenance","reason":"looks like implementation follow-up"}`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			if rpcString(req.Params, "decision_state") != "selected" || rpcString(req.Params, "selected_task_id") != "task-owner-revision" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-owner-revision" {
				t.Fatalf("expected hydration for owner-bound revision, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-owner-revision"},
				"task":           map[string]any{"task_id": "task-owner-revision"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		agent:  &Agent{LLM: llm},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			ProjectClaimHoldTaskID:    "task-stale-sidecar",
			ProjectClaimHoldProjectID: "project-clearpress",
			ProjectClaimHoldUntil:     time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
		},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-owner-bound-shadowed",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-stale-sidecar",
							Title:        "Make Clearpress MVP reviewable and publish runnable evidence",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ProjectID:    "project-clearpress",
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-owner-revision",
							Title:        "Unblock integration candidate branch-1",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ProjectID:    "project-clearpress",
							Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "required-agent:beta", "owner-agent:beta"},
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "plausible", Score: 70},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-owner-revision" {
		t.Fatalf("expected held stale sidecar to fall back to owner-bound revision, selected=%t work=%+v", selected, work)
	}
	if got := work.Packet.Frontier.SelectedTaskID; got != "task-owner-revision" {
		t.Fatalf("selected task id = %q, want owner-bound revision", got)
	}
	if llm.callCount() != 1 {
		t.Fatalf("expected one LLM choice attempt before local hold fallback, got %d", llm.callCount())
	}
}

func TestRuntimeTaskFrontierSelectionMarksNoWorkPacketRunnable(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"decline","task_id":"","self_fit_summary":"","reason":"model missed owner-bound task"}`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			if rpcString(req.Params, "decision_state") != "selected" || rpcString(req.Params, "selected_task_id") != "task-owner-revision" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-owner-revision" {
				t.Fatalf("expected hydration for owner revision, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-owner-revision"},
				"task":           map[string]any{"task_id": "task-owner-revision"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		agent:  &Agent{LLM: llm},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := AgentWorkNextResult{
		HasWork: false,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-no-work-owner-bound",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-owner-revision",
							Title:        "Unblock integration candidate branch-1",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ProjectID:    "project-clearpress",
							ProjectLane:  "implementation",
							Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "required-agent:beta", "owner-agent:beta"},
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "recommended", Score: 95},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || !work.HasWork || work.Task == nil || work.Task.TaskID != "task-owner-revision" {
		t.Fatalf("selected frontier task must become runnable work, selected=%t work=%+v", selected, work)
	}
	if work.ClaimAction != "claim_required" || work.SessionAction != "start_new" {
		t.Fatalf("expected claim/session actions after no-work frontier selection, got claim=%q session=%q", work.ClaimAction, work.SessionAction)
	}
}

func TestRuntimeTaskFrontierAllBlockedProductPressureMaterializesClaimRepairPacket(t *testing.T) {
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			if rpcString(req.Params, "decision_state") != "admission_failed" || rpcString(req.Params, "selected_task_id") != "task-rq-eval" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	coordination := json.RawMessage(`{"project":{"project_id":"project-rq"},"patch_queue_items":[{"state":"BLOCKED"}]}`)
	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := AgentWorkNextResult{
		HasWork:             true,
		Reason:              "task_frontier_available",
		ProjectCoordination: coordination,
		Packet: &AgentWorkPacket{
			WorkType:            "task_frontier_available",
			ProjectCoordination: coordination,
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-blocked-product-pressure",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:              "task-rq-eval",
							Title:               "Repair rq evaluator",
							OwnerUserID:         "owner-1",
							Priority:            "HIGH",
							Status:              "PENDING",
							TaskKind:            "EXECUTION",
							TaskTemplate:        "generic",
							ProjectID:           "project-rq",
							ProjectLane:         "implementation",
							RequiresProjectGate: boolPtr(true),
							WriteScopeHints:     []string{"internal/eval/**"},
							LinkedBy:            "system",
							LinkedAt:            "2026-03-23T00:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "blocked", Score: 0},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
						Blocked:       true,
						BlockReason:   "project_claim_scope_busy",
						BlockSummary:  "Project implementation write scope is busy.",
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:      "task-coordinate",
							Title:       "Coordinate rq split",
							OwnerUserID: "owner-1",
							Priority:    "HIGH",
							Status:      "PENDING",
							TaskKind:    "COORDINATION",
							ProjectID:   "project-rq",
							ProjectLane: "coordination",
							LinkedBy:    "system",
							LinkedAt:    "2026-03-23T00:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "blocked", Score: 0},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
						Blocked:       true,
						BlockReason:   "product_lane_pressure",
						BlockSummary:  "Project has terminal patch queue pressure and pending product-lane work.",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.HasWork || work.Task != nil || work.Reason != "project_claim_scope_busy" {
		t.Fatalf("blocked product frontier should become no-work claim-repair packet, selected=%t work=%+v", selected, work)
	}
	if work.Packet == nil || work.Packet.WorkType != "project_claim_scope_busy" || work.Packet.CoordinationState != "project_claim_scope_busy" {
		t.Fatalf("expected project_claim_scope_busy packet, got %+v", work.Packet)
	}
	if work.Packet.ProjectID != "project-rq" || work.Packet.ProjectLane != "implementation" || work.Packet.RequiresProjectGate == nil || !*work.Packet.RequiresProjectGate {
		t.Fatalf("packet should carry blocked product digest, got %+v", work.Packet)
	}
	if len(work.Packet.ContextHints.AnchorTaskIDs) != 1 || work.Packet.ContextHints.AnchorTaskIDs[0] != "task-rq-eval" {
		t.Fatalf("expected blocked task anchor, got %+v", work.Packet.ContextHints.AnchorTaskIDs)
	}
	if len(decisions) != 1 {
		t.Fatalf("expected one admission_failed frontier decision, got %+v", decisions)
	}
}

func TestRuntimeTaskFrontierAllBlockedProductPressureMaterializesProfileDelegationPacket(t *testing.T) {
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			if rpcString(req.Params, "decision_state") != "admission_failed" || rpcString(req.Params, "selected_task_id") != "task-rq-import" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	coordination := json.RawMessage(`{"project":{"project_id":"project-rq"},"patch_queue_items":[{"state":"BLOCKED"}]}`)
	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "iota"},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := AgentWorkNextResult{
		HasWork:             true,
		Reason:              "task_frontier_available",
		ProjectCoordination: coordination,
		Packet: &AgentWorkPacket{
			WorkType:            "task_frontier_available",
			ProjectCoordination: coordination,
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-profile-blocked-product-pressure",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:              "task-rq-import",
							Title:               "Repair rq internal eval import resolution",
							OwnerUserID:         "owner-1",
							Priority:            "HIGH",
							Status:              "PENDING",
							TaskKind:            "EXECUTION",
							TaskTemplate:        "generic",
							ProjectID:           "project-rq",
							ProjectLane:         "implementation",
							RequiresProjectGate: boolPtr(true),
							WriteScopeHints:     []string{"internal/eval/**"},
							LinkedBy:            "system",
							LinkedAt:            "2026-03-23T00:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "blocked", Score: 0},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
						Blocked:       true,
						BlockReason:   "profile_task_mode_mismatch",
						BlockSummary:  "Agent fresh-selection mode review is not eligible for pure implementation work.",
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:      "task-coordinate",
							Title:       "Coordinate rq split",
							OwnerUserID: "owner-1",
							Priority:    "HIGH",
							Status:      "PENDING",
							TaskKind:    "COORDINATION",
							ProjectID:   "project-rq",
							ProjectLane: "coordination",
							LinkedBy:    "system",
							LinkedAt:    "2026-03-23T00:00:00Z",
						},
						Fit:           AgentWorkTaskFit{Level: "blocked", Score: 0},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
						Blocked:       true,
						BlockReason:   "product_lane_pressure",
						BlockSummary:  "Project has terminal patch queue pressure and pending product-lane work.",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.HasWork || work.Task != nil || work.Reason != "project_targeted_delegation_required" {
		t.Fatalf("profile-blocked product frontier should become targeted delegation packet, selected=%t work=%+v", selected, work)
	}
	if work.Packet == nil || work.Packet.WorkType != "project_targeted_delegation_required" || work.Packet.PreferredTransition != "runtime_switch_task" {
		t.Fatalf("expected targeted delegation runtime-switch packet, got %+v", work.Packet)
	}
	if work.Packet.ProjectID != "project-rq" || work.Packet.ProjectLane != "implementation" || work.Packet.RequiresProjectGate == nil || !*work.Packet.RequiresProjectGate {
		t.Fatalf("packet should carry blocked product digest, got %+v", work.Packet)
	}
	if len(work.Packet.ContextHints.AnchorTaskIDs) != 1 || work.Packet.ContextHints.AnchorTaskIDs[0] != "task-rq-import" {
		t.Fatalf("expected blocked task anchor, got %+v", work.Packet.ContextHints.AnchorTaskIDs)
	}
	if len(decisions) != 1 {
		t.Fatalf("expected one admission_failed frontier decision, got %+v", decisions)
	}
}

func TestRuntimeTaskFrontierNoHoldKeepsModelOrdinaryChoice(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-ordinary","self_fit_summary":"ordinary task fits now","reason":"no local hold blocks it"}`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			if rpcString(req.Params, "decision_state") != "selected" || rpcString(req.Params, "selected_task_id") != "task-ordinary" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-ordinary" {
				t.Fatalf("expected hydration for ordinary model choice, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-ordinary"},
				"task":           map[string]any{"task_id": "task-ordinary"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		agent:  &Agent{LLM: llm},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-owner-bound-no-hold",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-ordinary",
							Title:        "Ordinary runnable implementation follow-up",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ProjectID:    "project-clearpress",
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-owner-revision",
							Title:        "Unblock integration candidate branch-1",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ProjectID:    "project-clearpress",
							Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "required-agent:beta", "owner-agent:beta"},
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "plausible", Score: 70},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-ordinary" {
		t.Fatalf("expected ordinary model choice when no hold is active, selected=%t work=%+v", selected, work)
	}
}

func TestRuntimeTaskFrontierHeldOrdinaryDoesNotFallbackToOtherAgentOwnerBoundTask(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-stale-sidecar","self_fit_summary":"I can publish provenance","reason":"looks like implementation follow-up"}`}
	runtime := &Runtime{
		cfg:   RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		agent: &Agent{LLM: llm},
		scratch: RuntimeScratchState{
			ProjectClaimHoldTaskID:    "task-stale-sidecar",
			ProjectClaimHoldProjectID: "project-clearpress",
			ProjectClaimHoldUntil:     time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
		},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID: "frontier-owner-bound-other-agent",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-stale-sidecar",
							Title:        "Make Clearpress MVP reviewable and publish runnable evidence",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ProjectID:    "project-clearpress",
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 95},
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:       "task-eta-revision",
							Title:        "Unblock integration candidate branch-eta",
							OwnerUserID:  "owner-1",
							Priority:     "HIGH",
							Status:       "PENDING",
							TaskKind:     "EXECUTION",
							TaskTemplate: "generic",
							ProjectID:    "project-clearpress",
							Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "required-agent:eta", "owner-agent:eta"},
							LinkedBy:     "system",
							LinkedAt:     "2026-03-23T00:00:00Z",
						},
						Fit: AgentWorkTaskFit{Level: "recommended", Score: 90},
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if selected || work.Task != nil {
		t.Fatalf("must not claim held ordinary or another agent's owner-bound task, selected=%t work=%+v", selected, work)
	}
}

func TestRuntimeTaskFrontierModelFailureFallsBackToRequiredOwnerBoundTaskOnlyForSelf(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			if state := rpcString(req.Params, "decision_state"); state == "selected" {
				if rpcString(req.Params, "selected_task_id") != "task-owner-revision" {
					t.Fatalf("unexpected selected frontier decision params: %+v", req.Params)
				}
			} else if state != "model_failed" {
				t.Fatalf("unexpected frontier decision params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-owner-revision" {
				t.Fatalf("expected hydration for owner-bound task, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-owner-revision"},
				"task":           map[string]any{"task_id": "task-owner-revision"},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	baseWork := func(requiredAgent string) AgentWorkNextResult {
		return AgentWorkNextResult{
			HasWork: true,
			Reason:  "task_frontier_available",
			Packet: &AgentWorkPacket{
				WorkType: "task_frontier_available",
				Frontier: &AgentWorkTaskFrontier{
					GenerationID: "frontier-owner-bound-model-failure",
					Candidates: []AgentWorkTaskFrontierCandidate{
						{
							Task: WorkspaceTaskRecord{
								TaskID:       "task-owner-revision",
								Title:        "Unblock integration candidate branch-1",
								OwnerUserID:  "owner-1",
								Priority:     "HIGH",
								Status:       "PENDING",
								TaskKind:     "EXECUTION",
								TaskTemplate: "generic",
								Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "required-agent:" + requiredAgent},
								LinkedBy:     "system",
								LinkedAt:     "2026-03-23T00:00:00Z",
							},
							Fit: AgentWorkTaskFit{Level: "recommended", Score: 90},
						},
					},
				},
			},
		}
	}

	selfRuntime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "eta"}, client: NewRhizomeClient(server.URL, "token")}
	selfWork := baseWork("eta")
	selected, err := selfRuntime.selectTaskFromFrontier(context.Background(), &selfWork)
	if err != nil {
		t.Fatalf("self selectTaskFromFrontier() error = %v", err)
	}
	if !selected || selfWork.Task == nil || selfWork.Task.TaskID != "task-owner-revision" {
		t.Fatalf("expected self owner-bound model-failure fallback, selected=%t work=%+v", selected, selfWork)
	}

	peerRuntime := &Runtime{cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "gamma"}, client: NewRhizomeClient(server.URL, "token")}
	peerWork := baseWork("eta")
	selected, err = peerRuntime.selectTaskFromFrontier(context.Background(), &peerWork)
	if err != nil {
		t.Fatalf("peer selectTaskFromFrontier() error = %v", err)
	}
	if selected || peerWork.Task != nil {
		t.Fatalf("peer must not fallback-claim another agent's owner-bound task, selected=%t work=%+v", selected, peerWork)
	}
	if !strings.Contains(peerWork.Packet.Frontier.DeclineSummary, "model choice failed") {
		t.Fatalf("expected ordinary model-failure decline for peer, got %+v", peerWork.Packet.Frontier)
	}
}

func TestRuntimeEnsureRunnableTaskRuntimeSwitchTaskWakesBlockedClaim(t *testing.T) {
	var methods []string
	var lastScratch RuntimeScratchState
	var claimedTaskID string
	var sessionStartTaskID string
	var runWriteTaskID string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-2" {
				t.Fatalf("expected runtime switch-task work.next hints, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":   "2026-03-23T00:00:00Z",
				"workspace_id":   "ws",
				"agent_id":       "agent-1",
				"has_work":       true,
				"reason":         "resume_claim",
				"trigger":        "runtime_switch_task",
				"claim_action":   "claim_required",
				"session_action": "start_new",
				"task": map[string]any{
					"task_id":        "task-2",
					"title":          "Task Two",
					"description":    "blocked claim explicitly selected from dashboard",
					"owner_user_id":  "owner-1",
					"priority":       "HIGH",
					"status":         "RUNNING",
					"task_kind":      "general",
					"task_template":  "default",
					"linked_by":      "system",
					"linked_at":      "2026-03-23T00:00:00Z",
					"claim_agent_id": "agent-1",
					"claim_status":   "BLOCKED",
				},
			})
		case "agent.task.claim":
			claimedTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			sessionStartTaskID = rpcString(req.Params, "task_id")
			sessionID := rpcString(req.Params, "session_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          sessionID,
				"workspace_id":        "ws",
				"agent_id":            "agent-1",
				"task_id":             sessionStartTaskID,
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-03-23T00:00:01Z",
				"started_at":          "2026-03-23T00:00:01Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			runWriteTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-2",
			DocSHAs:            map[string]string{},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task == nil || task.TaskID != "task-2" {
		t.Fatalf("expected switched task to become runnable, got %+v", task)
	}
	if claimedTaskID != "task-2" || sessionStartTaskID != "task-2" || runWriteTaskID != "task-2" {
		t.Fatalf("expected task-2 through claim/session/run, claim=%q session=%q run=%q methods=%#v", claimedTaskID, sessionStartTaskID, runWriteTaskID, methods)
	}
	if lastScratch.PendingTrigger != "" || lastScratch.PendingTriggerTask != "" || lastScratch.PendingTriggerSession != "" {
		t.Fatalf("expected runtime switch trigger to be cleared after selection, got %+v", lastScratch)
	}
	if lastScratch.ActiveTaskID != "task-2" || lastScratch.ActiveSessionID == "" || lastScratch.ActiveRunID == "" {
		t.Fatalf("expected switched task to publish active scratch, got %+v", lastScratch)
	}
	if !containsAll(methods, []string{"agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.state.set"}) {
		t.Fatalf("expected full switched blocked-claim activation path, got %#v", methods)
	}
}

func TestRuntimeEnsureRunnableTaskDefersActiveContinuationHold(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:       "task-continue",
		Title:        "Continue carefully",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "RUNNING",
		TaskKind:     "general",
		TaskTemplate: "default",
		LinkedBy:     "system",
		LinkedAt:     "2026-03-23T00:00:00Z",
		ClaimAgentID: stringPtr("agent-1"),
		ClaimStatus:  stringPtr("CLAIMED"),
	}
	session := AgentSessionStateRecord{
		SessionID: "session-continue",
		AgentID:   "agent-1",
		TaskID:    task.TaskID,
		Status:    "ACTIVE",
		Summary:   "continue later",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:     "ws",
			AgentID:         "agent-1",
			PlannerEvery:    45 * time.Second,
			RhizomeRPC:      "http://127.0.0.1/unused",
			RhizomeToken:    "token",
			OwnerUserID:     "owner-1",
			DisplayName:     "Agent One",
			ProtocolVersion: "rnar/v1",
		},
		scratch: RuntimeScratchState{
			ActiveTaskID:              task.TaskID,
			ActiveSessionID:           session.SessionID,
			ActiveRunID:               "run-continue",
			ContinuationHoldTaskID:    task.TaskID,
			ContinuationHoldSessionID: session.SessionID,
			ContinuationHoldRunID:     "run-continue",
			ContinuationHoldUntil:     time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano),
			ContinuationHoldSummary:   "continue later",
			ContinuationHoldCount:     1,
			DocSHAs:                   map[string]string{},
		},
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-continue",
	}

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got != nil {
		t.Fatalf("expected continuation hold to defer active task, got %+v", got)
	}

	runtime.scratch.ContinuationHoldUntil = time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	got, err = runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() after hold expiry error = %v", err)
	}
	if got == nil || got.TaskID != task.TaskID {
		t.Fatalf("expected expired continuation hold to allow active task, got %+v", got)
	}
}

func TestRuntimeContinueTaskCycleSetsContinuationHold(t *testing.T) {
	var saved RuntimeScratchState
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.session.status":
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          "session-continue",
					"workspace_id":        "ws",
					"agent_id":            "agent-1",
					"task_id":             "task-continue",
					"status":              "ACTIVE",
					"summary":             rpcString(req.Params, "summary"),
					"updated_at":          "2026-03-23T00:00:01Z",
					"started_at":          "2026-03-23T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{
				"run": map[string]any{"run_id": rpcString(req.Params, "run_id")},
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{TaskID: "task-continue", Title: "Continue carefully", Status: "RUNNING"}
	session := AgentSessionStateRecord{SessionID: "session-continue", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:  "ws",
			AgentID:      "agent-1",
			PlannerEvery: 45 * time.Second,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-continue", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Made one useful checkpoint",
		NextAction: "Wait for the planner cooldown before another cycle",
	}, nil)
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if !containsAll(methods, []string{"agent.session.status", "workspace.execution.run.write", "agent.state.set"}) {
		t.Fatalf("expected continuation persistence methods, got %#v", methods)
	}
	if saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID || saved.ContinuationHoldRunID != "run-continue" {
		t.Fatalf("expected continuation hold identity in scratch, got %+v", saved)
	}
	if saved.ContinuationHoldCount != 1 {
		t.Fatalf("expected first continuation hold count, got %+v", saved)
	}
	holdUntil, ok := parseRFC3339Nano(saved.ContinuationHoldUntil)
	if !ok || holdUntil == nil || !holdUntil.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("expected durable future continuation hold, got %+v", saved)
	}
	if runtime.activeTask == nil || runtime.activeTask.TaskID != task.TaskID {
		t.Fatalf("expected continuation to keep task visible as active, got %+v", runtime.activeTask)
	}
}

func TestRuntimeContinueTaskCycleTrustFirstDurableProjectProgressQueuesImmediateResume(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:      "ws",
		AgentID:          "agent-1",
		PlannerEvery:     45 * time.Second,
		CoordinationMode: CoordinationModeTrustFirst,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{
		TaskID:      "task-impl",
		Title:       "Implement product slice",
		Status:      "RUNNING",
		ProjectID:   "project-1",
		ProjectLane: "implementation",
	}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-impl", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Implemented the first slice in the owned checkout",
		NextAction: "Run npm run build, then call project_branch_commit with push=true and project_branch_review_ready for the current HEAD.",
	}, &TaskRunTrace{ToolCalls: []string{"project_branch_commit"}, SuccessfulToolCalls: []string{"project_branch_commit"}})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != task.TaskID || saved.PendingTriggerSession != session.SessionID {
		t.Fatalf("expected immediate request_resume for project continuation, got %+v", *saved)
	}
	if saved.ContinuationHoldTaskID != "" || saved.ContinuationHoldSessionID != "" || saved.ContinuationHoldRunID != "" || saved.ContinuationHoldUntil != "" {
		t.Fatalf("expected immediate project continuation to clear hold, got %+v", *saved)
	}
	if saved.ImmediateProjectResumeTaskID != task.TaskID || saved.ImmediateProjectResumeSessionID != session.SessionID || saved.ImmediateProjectResumeRunID != "run-impl" || saved.ImmediateProjectResumeSignature == "" {
		t.Fatalf("expected immediate project resume throttle state, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleTrustFirstShellOnlyProgressKeepsContinuationHold(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:      "ws",
		AgentID:          "agent-1",
		PlannerEvery:     45 * time.Second,
		CoordinationMode: CoordinationModeTrustFirst,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{
		TaskID:      "task-impl",
		Title:       "Implement product slice",
		Status:      "RUNNING",
		ProjectID:   "project-1",
		ProjectLane: "implementation",
	}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-impl", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Inspected the checkout",
		NextAction: "Run npm run build, then call project_branch_commit with push=true and project_branch_review_ready for the current HEAD.",
	}, &TaskRunTrace{ToolCalls: []string{"shell"}, SuccessfulToolCalls: []string{"shell"}})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.PendingTriggerSession != "" {
		t.Fatalf("shell-only project continuation should not queue immediate resume, got %+v", *saved)
	}
	if saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID {
		t.Fatalf("shell-only project continuation should keep hold, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleTrustFirstRealityCheckRepairQueuesImmediateResume(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:      "ws",
		AgentID:          "agent-1",
		PlannerEvery:     45 * time.Second,
		CoordinationMode: CoordinationModeTrustFirst,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{
		TaskID:      "task-impl",
		Title:       "Implement product slice",
		Status:      "RUNNING",
		ProjectID:   "project-1",
		ProjectLane: "implementation",
	}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-reality", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Published artifact reality check",
		NextAction: "Replace the stock starter with the smallest complete product slice.",
		Materialize: TaskMaterialization{
			DocKey:     "task.task-impl.artifact_reality_check",
			DocTitle:   "Artifact Reality Check",
			DocContent: "The checkout is stock starter content and not review-ready. Smallest repair direction: implement the product slice, run build, commit, and publish review-ready evidence.",
		},
	}, &TaskRunTrace{})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != task.TaskID || saved.PendingTriggerSession != session.SessionID {
		t.Fatalf("expected self-actionable reality check to queue immediate resume, got %+v", *saved)
	}
	if saved.ContinuationHoldTaskID != "" || saved.ContinuationHoldSessionID != "" {
		t.Fatalf("expected immediate reality-check continuation to clear hold, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleStrictProjectProgressKeepsContinuationHold(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:  "ws",
		AgentID:      "agent-1",
		PlannerEvery: 45 * time.Second,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{TaskID: "task-impl", Status: "RUNNING", ProjectID: "project-1", ProjectLane: "implementation"}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-impl", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Implemented the first slice",
		NextAction: "Run npm run build, project_branch_commit, then project_branch_review_ready.",
	}, &TaskRunTrace{ToolCalls: []string{"shell"}, SuccessfulToolCalls: []string{"shell"}})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.PendingTriggerSession != "" {
		t.Fatalf("strict project continuation should not queue immediate resume, got %+v", *saved)
	}
	if saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID {
		t.Fatalf("strict project continuation should keep hold, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleTrustFirstGenericCommitWordsKeepContinuationHold(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:      "ws",
		AgentID:          "agent-1",
		PlannerEvery:     45 * time.Second,
		CoordinationMode: CoordinationModeTrustFirst,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{TaskID: "task-generic", Status: "RUNNING", TaskKind: "general"}
	session := AgentSessionStateRecord{SessionID: "session-generic", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-generic", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Prepared generic work",
		NextAction: "Commit the result after one more check.",
	}, &TaskRunTrace{ToolCalls: []string{"shell"}, SuccessfulToolCalls: []string{"shell"}})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID {
		t.Fatalf("generic task should keep continuation hold without immediate resume, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleTrustFirstFailedProgressKeepsContinuationHold(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:      "ws",
		AgentID:          "agent-1",
		PlannerEvery:     45 * time.Second,
		CoordinationMode: CoordinationModeTrustFirst,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{TaskID: "task-impl", Status: "RUNNING", ProjectID: "project-1", ProjectLane: "implementation"}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-impl", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Build did not finish",
		NextAction: "Run project_branch_commit after the build is fixed.",
	}, &TaskRunTrace{ToolCalls: []string{"shell"}, FailedToolCalls: []string{"shell"}})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID {
		t.Fatalf("failed/no-op progress should keep continuation hold, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleTrustFirstAdvisoryNextMoveKeepsContinuationHold(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:      "ws",
		AgentID:          "agent-1",
		PlannerEvery:     45 * time.Second,
		CoordinationMode: CoordinationModeTrustFirst,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{TaskID: "task-impl", Status: "RUNNING", ProjectID: "project-1", ProjectLane: "implementation"}
	session := AgentSessionStateRecord{SessionID: "session-impl", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-impl", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Recorded advisory gap",
		NextAction: "Keep moving with product, review, or integration work; publish branch/review-ready evidence when useful.",
	}, &TaskRunTrace{ToolCalls: []string{"shell"}, SuccessfulToolCalls: []string{"shell"}})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID {
		t.Fatalf("advisory next action should keep continuation hold, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleTrustFirstImmediateResumeThrottlesRepeatedSignature(t *testing.T) {
	runtime, saved, _, cleanup := newContinueTaskCycleHarness(t, RuntimeConfig{
		WorkspaceID:      "ws",
		AgentID:          "agent-1",
		PlannerEvery:     45 * time.Second,
		CoordinationMode: CoordinationModeTrustFirst,
	})
	defer cleanup()

	task := WorkspaceTaskRecord{TaskID: "task-rebuild", Status: "RUNNING", ProjectID: "project-1", ProjectLane: "rebuild"}
	session := AgentSessionStateRecord{SessionID: "session-rebuild", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Rebuilt the candidate",
		NextAction: "Run build, then project_branch_commit and project_branch_review_ready.",
	}
	trace := &TaskRunTrace{ToolCalls: []string{"project_branch_commit"}, SuccessfulToolCalls: []string{"project_branch_commit"}}

	if err := runtime.continueTaskCycle(context.Background(), task, session, "run-rebuild", result, trace); err != nil {
		t.Fatalf("first continueTaskCycle() error = %v", err)
	}
	firstSignature := saved.ImmediateProjectResumeSignature
	if firstSignature == "" || saved.PendingTrigger != "request_resume" {
		t.Fatalf("expected first cycle to queue immediate resume, got %+v", *saved)
	}
	runtime.scratch = *saved
	runtime.scratch.PendingTrigger = ""
	runtime.scratch.PendingTriggerTask = ""
	runtime.scratch.PendingTriggerSession = ""
	runtime.scratch.PendingTriggerAt = ""

	if err := runtime.continueTaskCycle(context.Background(), task, session, "run-rebuild", result, trace); err != nil {
		t.Fatalf("second continueTaskCycle() error = %v", err)
	}
	if saved.ImmediateProjectResumeSignature != firstSignature || saved.PendingTrigger != "" {
		t.Fatalf("repeated identical project continuation should be throttled, got %+v", *saved)
	}
	if saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID {
		t.Fatalf("throttled continuation should fall back to hold, got %+v", *saved)
	}
}

func TestRuntimeContinueTaskCycleParksAfterPeerRequest(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.session.status":
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          "session-peer",
					"workspace_id":        "ws",
					"agent_id":            "agent-1",
					"task_id":             "task-peer",
					"status":              "ACTIVE",
					"summary":             rpcString(req.Params, "summary"),
					"updated_at":          "2026-03-23T00:00:01Z",
					"started_at":          "2026-03-23T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{TaskID: "task-peer", Title: "Peer-reviewed task", Status: "RUNNING"}
	session := AgentSessionStateRecord{SessionID: "session-peer", AgentID: "agent-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:  "ws",
			AgentID:      "agent-1",
			PlannerEvery: 45 * time.Second,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-peer", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Peer review has been incorporated",
		NextAction: "Return final completion if no blockers remain",
	}, &TaskRunTrace{ToolCalls: []string{"agent_request"}})
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if saved.ContinuationHoldTaskID != task.TaskID || saved.ContinuationHoldSessionID != session.SessionID {
		t.Fatalf("peer-review continuation should set a cooldown hold, got %+v", saved)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.PendingTriggerSession != "" {
		t.Fatalf("peer-review continuation should not queue immediate request_resume, got %+v", saved)
	}
}

func newContinueTaskCycleHarness(t *testing.T, cfg RuntimeConfig) (*Runtime, *RuntimeScratchState, *[]string, func()) {
	t.Helper()
	saved := &RuntimeScratchState{}
	methods := &[]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		*methods = append(*methods, req.Method)

		switch req.Method {
		case "agent.session.status":
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          rpcString(req.Params, "session_id"),
					"workspace_id":        rpcString(req.Params, "workspace_id"),
					"agent_id":            rpcString(req.Params, "agent_id"),
					"task_id":             rpcString(req.Params, "task_id"),
					"status":              rpcString(req.Params, "status"),
					"summary":             rpcString(req.Params, "summary"),
					"updated_at":          "2026-03-23T00:00:01Z",
					"started_at":          "2026-03-23T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{
				"run": map[string]any{"run_id": rpcString(req.Params, "run_id")},
			})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"coordination_version": "v1",
				"project": map[string]any{
					"workspace_id": rpcString(req.Params, "workspace_id"),
					"project_id":   rpcString(req.Params, "project_id"),
					"status":       "ACTIVE",
				},
				"branches":          []any{},
				"checkouts":         []any{},
				"repositories":      []any{},
				"patch_queue_items": []any{},
			}})
		case "agent.state.set":
			*saved = RuntimeScratchState{}
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	runtime := &Runtime{
		cfg:    cfg,
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	return runtime, saved, methods, server.Close
}

func TestRuntimeTaskFrontierClaimFailureTerminalizesSelection(t *testing.T) {
	llm := &ambientRecordingLLM{content: `{"decision":"claim","task_id":"task-frontier","self_fit_summary":"fits local runtime tools","reason":"recommended frontier candidate"}`}
	var methods []string
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-28T00:00:00Z",
				"workspace_id": "ws",
				"agent_id":     "agent-frontier",
				"has_work":     true,
				"reason":       "task_frontier_available",
				"packet": map[string]any{
					"work_type": "task_frontier_available",
					"frontier": map[string]any{
						"generation_id":  "frontier-terminalize",
						"generated_at":   "2026-05-28T00:00:00Z",
						"selection_mode": "agent_self_select",
						"candidates": []map[string]any{{
							"task": map[string]any{
								"task_id":       "task-frontier",
								"title":         "Frontier task",
								"description":   "Selected task must terminalize if claim fails.",
								"owner_user_id": "owner-1",
								"priority":      "HIGH",
								"status":        "PENDING",
								"task_kind":     "EXECUTION",
								"task_template": "generic",
								"linked_by":     "system",
								"linked_at":     "2026-05-28T00:00:00Z",
							},
							"fit":            map[string]any{"level": "recommended", "score": 91},
							"claim_action":   "claim_required",
							"session_action": "start_new",
						}},
					},
				},
			})
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-05-28T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": "task-frontier"},
				"task":           map[string]any{"task_id": "task-frontier"},
			}})
		case "agent.task.claim":
			writeRPCError(w, req, -32603, "claim receipt write failed")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-frontier",
			CoordinationMode: CoordinationModeTrustFirst,
			Workdir:          t.TempDir(),
		},
		client:  NewRhizomeClient(server.URL, "token"),
		agent:   &Agent{LLM: llm},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err == nil {
		t.Fatalf("expected claim failure, got task=%+v", task)
	}
	if len(decisions) != 2 {
		t.Fatalf("expected selected and terminal frontier decisions, got %+v", decisions)
	}
	if rpcString(decisions[0], "decision_state") != "selected" || rpcString(decisions[0], "selected_task_id") != "task-frontier" {
		t.Fatalf("unexpected selected decision: %+v", decisions[0])
	}
	if rpcString(decisions[1], "decision_state") != "claim_failed" || rpcString(decisions[1], "selected_task_id") != "task-frontier" {
		t.Fatalf("expected terminal claim_failed frontier decision, got %+v", decisions[1])
	}
	if !strings.Contains(rpcString(decisions[1], "summary"), "claim receipt write failed") {
		t.Fatalf("expected terminal decision summary to include failure cause, got %+v", decisions[1])
	}
	if containsAll(methods, []string{"agent.session.start"}) {
		t.Fatalf("session must not start after failed frontier claim, methods=%#v", methods)
	}
}

func TestRuntimeTaskFrontierClaimConflictRetriesFreshWorkNext(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"decision":"claim","task_id":"task-lexer","self_fit_summary":"lexer fit","reason":"recommended frontier candidate"}`},
		{Content: `{"decision":"claim","task_id":"task-parser","self_fit_summary":"parser fit","reason":"fresh frontier candidate"}`},
	}}
	var methods []string
	var decisions []map[string]any
	workNextCalls := 0
	claimCalls := 0
	sessionTaskIDs := []string{}
	frontierWork := func(generationID, taskID, title string) map[string]any {
		return map[string]any{
			"generated_at": "2026-05-31T00:00:00Z",
			"workspace_id": "ws",
			"agent_id":     "agent-frontier",
			"has_work":     true,
			"reason":       "task_frontier_available",
			"packet": map[string]any{
				"work_type": "task_frontier_available",
				"frontier": map[string]any{
					"generation_id":  generationID,
					"generated_at":   "2026-05-31T00:00:00Z",
					"selection_mode": "agent_self_select",
					"candidates": []map[string]any{{
						"task": map[string]any{
							"task_id":       taskID,
							"title":         title,
							"description":   "Fresh frontier candidate.",
							"owner_user_id": "owner-1",
							"priority":      "HIGH",
							"status":        "PENDING",
							"task_kind":     "EXECUTION",
							"task_template": "generic",
							"linked_by":     "system",
							"linked_at":     "2026-05-31T00:00:00Z",
						},
						"fit":            map[string]any{"level": "recommended", "score": 91},
						"claim_action":   "claim_required",
						"session_action": "start_new",
					}},
				},
			},
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			if workNextCalls == 1 {
				writeRPCResult(w, req, frontierWork("frontier-lexer", "task-lexer", "Implement rq lexer"))
				return
			}
			if workNextCalls == 2 {
				writeRPCResult(w, req, frontierWork("frontier-parser", "task-parser", "Implement rq parser"))
				return
			}
			t.Fatalf("unexpected extra agent.work.next call %d", workNextCalls)
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			taskID := rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-05-31T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": taskID},
				"task":           map[string]any{"task_id": taskID},
			}})
		case "agent.task.claim":
			claimCalls++
			if claimCalls == 1 {
				if rpcString(req.Params, "task_id") != "task-lexer" {
					t.Fatalf("expected first claim for task-lexer, got %+v", req.Params)
				}
				writeRPCError(w, req, -32603, "task claim conflict")
				return
			}
			if claimCalls == 2 {
				if rpcString(req.Params, "task_id") != "task-parser" || rpcString(req.Params, "frontier_generation_id") != "frontier-parser" {
					t.Fatalf("expected second claim for fresh parser frontier, got %+v", req.Params)
				}
				writeRPCResult(w, req, nil)
				return
			}
			t.Fatalf("unexpected extra claim call %d", claimCalls)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-31T00:00:00Z",
				"agent": map[string]any{
					"agent_id":      "agent-frontier",
					"workspace_id":  "ws",
					"owner_user_id": "owner-1",
					"display_name":  "agent-frontier",
					"status":        "ACTIVE",
					"active_tasks":  []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws", "title": "Workspace", "status": "ACTIVE"},
					"tasks":     []any{},
					"sessions":  []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.session.start":
			taskID := rpcString(req.Params, "task_id")
			sessionTaskIDs = append(sessionTaskIDs, taskID)
			sessionID := rpcString(req.Params, "session_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          sessionID,
				"workspace_id":        "ws",
				"agent_id":            "agent-frontier",
				"task_id":             taskID,
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-05-31T00:00:01Z",
				"started_at":          "2026-05-31T00:00:01Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-frontier",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
			Workdir:          t.TempDir(),
		},
		client:  NewRhizomeClient(server.URL, "token"),
		agent:   &Agent{LLM: llm},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task == nil || task.TaskID != "task-parser" {
		t.Fatalf("expected retry to activate task-parser, got %+v", task)
	}
	if workNextCalls != 2 || claimCalls != 2 {
		t.Fatalf("expected one conflict then one fresh retry, work_next=%d claim=%d methods=%#v", workNextCalls, claimCalls, methods)
	}
	if len(sessionTaskIDs) != 1 || sessionTaskIDs[0] != "task-parser" {
		t.Fatalf("session must only start for fresh parser task, got %+v", sessionTaskIDs)
	}
	if len(decisions) != 3 {
		t.Fatalf("expected selected/claim_failed/selected decisions, got %+v", decisions)
	}
	if rpcString(decisions[0], "decision_state") != "selected" || rpcString(decisions[0], "selected_task_id") != "task-lexer" {
		t.Fatalf("unexpected first selected decision: %+v", decisions[0])
	}
	if rpcString(decisions[1], "decision_state") != "claim_failed" || rpcString(decisions[1], "selected_task_id") != "task-lexer" {
		t.Fatalf("expected terminal claim_failed for stale lexer selection, got %+v", decisions[1])
	}
	if rpcString(decisions[2], "decision_state") != "selected" || rpcString(decisions[2], "selected_task_id") != "task-parser" {
		t.Fatalf("expected fresh selected parser decision, got %+v", decisions[2])
	}
}

func TestRuntimeTaskFrontierClaimConflictHoldsLoserAndSelectsNextCandidate(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"decision":"claim","task_id":"task-lexer","self_fit_summary":"still looks best","reason":"stale self-selection"}`}}}
	var decisions []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			taskID := rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-05-31T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": taskID},
				"task":           map[string]any{"task_id": taskID},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-frontier",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		agent:  &Agent{LLM: llm},
		scratch: RuntimeScratchState{
			ProjectClaimHoldTaskID:    "task-lexer",
			ProjectClaimHoldProjectID: "project-rq",
			ProjectClaimHoldUntil:     time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
			DocSHAs:                   map[string]string{},
		},
	}
	work := AgentWorkNextResult{
		HasWork: true,
		Reason:  "task_frontier_available",
		Packet: &AgentWorkPacket{
			WorkType: "task_frontier_available",
			Frontier: &AgentWorkTaskFrontier{
				GenerationID:  "frontier-2",
				SelectionMode: "agent_self_select",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{
						Task: WorkspaceTaskRecord{
							TaskID:              "task-lexer",
							Title:               "Implement rq lexer",
							ProjectID:           "project-rq",
							ProjectLane:         "implementation",
							RequiresProjectGate: boolPtr(true),
							WriteScopeHints:     []string{"internal/lexer/**"},
						},
						Fit:           AgentWorkTaskFit{Level: "recommended", Score: 99},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
					},
					{
						Task: WorkspaceTaskRecord{
							TaskID:              "task-parser",
							Title:               "Implement rq parser",
							ProjectID:           "project-rq",
							ProjectLane:         "implementation",
							RequiresProjectGate: boolPtr(true),
							WriteScopeHints:     []string{"internal/parser/**", "internal/ast/**"},
						},
						Fit:           AgentWorkTaskFit{Level: "recommended", Score: 91},
						ClaimAction:   "claim_required",
						SessionAction: "start_new",
					},
				},
			},
		},
	}

	selected, err := runtime.selectTaskFromFrontier(context.Background(), &work)
	if err != nil {
		t.Fatalf("selectTaskFromFrontier() error = %v", err)
	}
	if !selected || work.Task == nil || work.Task.TaskID != "task-parser" {
		t.Fatalf("expected held loser task to reroute to parser, selected=%v work=%+v", selected, work.Task)
	}
	if len(decisions) != 1 || rpcString(decisions[0], "selected_task_id") != "task-parser" {
		t.Fatalf("expected frontier decision to record parser after stale lexer choice, decisions=%+v", decisions)
	}
}

func TestRuntimeTaskFrontierClaimConflictRetryDoesNotLoop(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"decision":"claim","task_id":"task-lexer","self_fit_summary":"lexer fit","reason":"recommended frontier candidate"}`},
		{Content: `{"decision":"claim","task_id":"task-parser","self_fit_summary":"parser fit","reason":"fresh frontier candidate"}`},
	}}
	var methods []string
	var decisions []map[string]any
	workNextCalls := 0
	claimCalls := 0
	frontierWork := func(generationID, taskID string) map[string]any {
		return map[string]any{
			"generated_at": "2026-05-31T00:00:00Z",
			"workspace_id": "ws",
			"agent_id":     "agent-frontier",
			"has_work":     true,
			"reason":       "task_frontier_available",
			"packet": map[string]any{
				"work_type": "task_frontier_available",
				"frontier": map[string]any{
					"generation_id":  generationID,
					"generated_at":   "2026-05-31T00:00:00Z",
					"selection_mode": "agent_self_select",
					"candidates": []map[string]any{{
						"task": map[string]any{
							"task_id":       taskID,
							"title":         "Frontier candidate",
							"owner_user_id": "owner-1",
							"priority":      "HIGH",
							"status":        "PENDING",
							"task_kind":     "EXECUTION",
							"task_template": "generic",
							"linked_by":     "system",
							"linked_at":     "2026-05-31T00:00:00Z",
						},
						"fit":            map[string]any{"level": "recommended", "score": 91},
						"claim_action":   "claim_required",
						"session_action": "start_new",
					}},
				},
			},
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			switch workNextCalls {
			case 1:
				writeRPCResult(w, req, frontierWork("frontier-lexer", "task-lexer"))
			case 2:
				writeRPCResult(w, req, frontierWork("frontier-parser", "task-parser"))
			default:
				t.Fatalf("retry must be bounded to one fresh work-next, got call %d", workNextCalls)
			}
		case "agent.task_frontier.decision":
			decisions = append(decisions, req.Params)
			writeRPCResult(w, req, map[string]any{"status": "RECORDED"})
		case "agent.task.hydrate":
			taskID := rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-05-31T00:00:00Z",
				"docs":           []any{},
				"updates":        []any{},
				"artifacts":      []any{},
				"related_tasks":  []any{},
				"task_links":     []any{},
				"workspace_task": map[string]any{"task_id": taskID},
				"task":           map[string]any{"task_id": taskID},
			}})
		case "agent.task.claim":
			claimCalls++
			writeRPCError(w, req, -32603, "task claim conflict")
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-31T00:00:00Z",
				"agent": map[string]any{
					"agent_id":      "agent-frontier",
					"workspace_id":  "ws",
					"owner_user_id": "owner-1",
					"display_name":  "agent-frontier",
					"status":        "ACTIVE",
					"active_tasks":  []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws", "title": "Workspace", "status": "ACTIVE"},
					"tasks":     []any{},
					"sessions":  []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "agent-frontier",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
			Workdir:          t.TempDir(),
		},
		client:  NewRhizomeClient(server.URL, "token"),
		agent:   &Agent{LLM: llm},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("second conflict should stop without runnable task, got %+v", task)
	}
	if workNextCalls != 2 || claimCalls != 2 {
		t.Fatalf("expected one bounded retry, work_next=%d claim=%d methods=%#v", workNextCalls, claimCalls, methods)
	}
	if containsAll(methods, []string{"agent.session.start"}) {
		t.Fatalf("session must not start when both frontier claims conflict, methods=%#v", methods)
	}
	if len(decisions) != 4 {
		t.Fatalf("expected selected/claim_failed for both frontier generations, got %+v", decisions)
	}
	if rpcString(decisions[1], "decision_state") != "claim_failed" || rpcString(decisions[3], "decision_state") != "claim_failed" {
		t.Fatalf("expected both selected generations to terminalize as claim_failed, got %+v", decisions)
	}
}

func TestRuntimePostClaimSessionFailureBlocksClaimAndClearsPendingTrigger(t *testing.T) {
	var methods []string
	var blockReason string
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-session-fail" {
				t.Fatalf("expected pending switch hints, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":   "2026-05-28T00:00:00Z",
				"workspace_id":   "ws",
				"agent_id":       "agent-session",
				"has_work":       true,
				"reason":         "trigger_task_ready",
				"trigger":        "runtime_switch_task",
				"claim_action":   "claim_required",
				"session_action": "start_new",
				"task": map[string]any{
					"task_id":       "task-session-fail",
					"title":         "Session materialization failure",
					"description":   "Claim should be repaired if session cannot start.",
					"owner_user_id": "owner-1",
					"priority":      "HIGH",
					"status":        "PENDING",
					"task_kind":     "EXECUTION",
					"task_template": "generic",
					"linked_by":     "system",
					"linked_at":     "2026-05-28T00:00:00Z",
				},
			})
		case "agent.task.claim":
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			writeRPCError(w, req, -32603, "session backend unavailable")
		case "agent.task.block":
			blockReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-session",
			Workdir:     t.TempDir(),
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-session-fail",
			DocSHAs:            map[string]string{},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err == nil {
		t.Fatalf("expected session failure, got task=%+v", task)
	}
	if !containsAll(methods, []string{"agent.task.claim", "agent.session.start", "agent.task.block", "agent.state.set"}) {
		t.Fatalf("expected claim, failed session, repair block, and scratch clear, got %#v", methods)
	}
	if !strings.Contains(blockReason, "post-claim session materialization failed") || !strings.Contains(blockReason, "session backend unavailable") {
		t.Fatalf("expected post-claim repair block reason, got %q", blockReason)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "" || saved.ActiveSessionID != "" || saved.ActiveRunID != "" {
		t.Fatalf("expected post-claim repair to clear pending/active scratch, got %+v", saved)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected no local active binding after session failure, task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestRuntimePostClaimExecutionRunFailureEndsSessionAndBlocksClaim(t *testing.T) {
	var methods []string
	var endSessionID, blockReason string
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at":   "2026-05-28T00:00:00Z",
				"workspace_id":   "ws",
				"agent_id":       "agent-session",
				"has_work":       true,
				"reason":         "trigger_task_ready",
				"trigger":        "runtime_switch_task",
				"claim_action":   "claim_required",
				"session_action": "start_new",
				"task": map[string]any{
					"task_id":       "task-run-fail",
					"title":         "Execution run materialization failure",
					"description":   "Claim should be repaired if execution run cannot be recorded.",
					"owner_user_id": "owner-1",
					"priority":      "HIGH",
					"status":        "PENDING",
					"task_kind":     "EXECUTION",
					"task_template": "generic",
					"linked_by":     "system",
					"linked_at":     "2026-05-28T00:00:00Z",
				},
			})
		case "agent.task.claim":
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			writeRPCResult(w, req, map[string]any{"state": AgentSessionStateRecord{
				SessionID:   rpcString(req.Params, "session_id"),
				WorkspaceID: "ws",
				AgentID:     "agent-session",
				TaskID:      "task-run-fail",
				Status:      "ACTIVE",
			}})
		case "workspace.execution.run.write":
			writeRPCError(w, req, -32603, "execution run ledger unavailable")
		case "agent.session.end":
			endSessionID = rpcString(req.Params, "session_id")
			if rpcString(req.Params, "status") != "ENDED" {
				t.Fatalf("expected session end status ENDED, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"state": AgentSessionStateRecord{
				SessionID:   endSessionID,
				WorkspaceID: "ws",
				AgentID:     "agent-session",
				TaskID:      "task-run-fail",
				Status:      "ENDED",
			}})
		case "agent.task.block":
			blockReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-session",
			Workdir:     t.TempDir(),
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-run-fail",
			DocSHAs:            map[string]string{},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err == nil {
		t.Fatalf("expected execution run failure, got task=%+v", task)
	}
	if !containsAll(methods, []string{"agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.session.end", "agent.task.block", "agent.state.set"}) {
		t.Fatalf("expected claim, session start, run failure, session end, block, and scratch clear, got %#v", methods)
	}
	if endSessionID == "" {
		t.Fatalf("expected durable session end after execution run failure, methods=%#v", methods)
	}
	if !strings.Contains(blockReason, "post-claim session materialization failed") || !strings.Contains(blockReason, "execution run ledger unavailable") {
		t.Fatalf("expected repair block reason to include run failure, got %q", blockReason)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "" || saved.ActiveSessionID != "" || saved.ActiveRunID != "" {
		t.Fatalf("expected repair to clear pending/active scratch, got %+v", saved)
	}
}

func TestRuntimeExistingOwnedClaimExecutionRunFailureRepairsOwnership(t *testing.T) {
	var methods []string
	var endCalled, blockCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			claimAgent := "agent-session"
			claimStatus := "CLAIMED"
			writeRPCResult(w, req, map[string]any{
				"generated_at":   "2026-05-28T00:00:00Z",
				"workspace_id":   "ws",
				"agent_id":       "agent-session",
				"has_work":       true,
				"reason":         "reuse_existing_claim",
				"claim_action":   "reuse_claim",
				"session_action": "start_new",
				"task": map[string]any{
					"task_id":        "task-existing-run-fail",
					"title":          "Existing claim run failure",
					"owner_user_id":  "owner-1",
					"priority":       "HIGH",
					"status":         "CLAIMED",
					"task_kind":      "EXECUTION",
					"task_template":  "generic",
					"linked_by":      "system",
					"linked_at":      "2026-05-28T00:00:00Z",
					"claim_agent_id": claimAgent,
					"claim_status":   claimStatus,
				},
			})
		case "agent.session.start":
			writeRPCResult(w, req, map[string]any{"state": AgentSessionStateRecord{
				SessionID:   rpcString(req.Params, "session_id"),
				WorkspaceID: "ws",
				AgentID:     "agent-session",
				TaskID:      "task-existing-run-fail",
				Status:      "ACTIVE",
			}})
		case "workspace.execution.run.write":
			writeRPCError(w, req, -32603, "execution binding rejected")
		case "agent.session.end":
			endCalled = true
			writeRPCResult(w, req, map[string]any{"state": AgentSessionStateRecord{
				SessionID:   rpcString(req.Params, "session_id"),
				WorkspaceID: "ws",
				AgentID:     "agent-session",
				TaskID:      "task-existing-run-fail",
				Status:      "ENDED",
			}})
		case "agent.task.block":
			blockCalled = true
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-session",
			Workdir:     t.TempDir(),
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err == nil {
		t.Fatalf("expected existing-owned claim materialization failure, got task=%+v", task)
	}
	if containsAll(methods, []string{"agent.task.claim"}) {
		t.Fatalf("existing-owned claim should not be re-claimed, methods=%#v", methods)
	}
	if !endCalled || !blockCalled {
		t.Fatalf("expected existing-owned claim repair to end session and block task, end=%v block=%v methods=%#v", endCalled, blockCalled, methods)
	}
}
