package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRuntimeHandleDelegatedModelAskQueuesSwitchTask(t *testing.T) {
	var saved RuntimeScratchState
	var savedStates []RuntimeScratchState
	var responded string
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedHydrationBundle("task-impl-1", "PENDING", "", ""))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("beta", "task-impl-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "beta", "task-impl-1") {
				return
			}
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			saved = state
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "workspace.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1 and implement the image pipeline lane."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-impl-1" || saved.ActiveSessionID == "" {
		t.Fatalf("expected delegated request to materialize claim/session and clear trigger, got %+v trace=%s", saved, compactScratchTriggerTrace(savedStates))
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-impl-1") {
		t.Fatalf("unexpected delegated response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected delegated task request to wake planner")
	}
	if !containsAll(methods, []string{"agent.state.set", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected response evidence methods, got %#v", methods)
	}
	if stringIndex(methods, "agent.task.claim") > stringIndex(methods, "agent.respond") ||
		stringIndex(methods, "agent.session.start") > stringIndex(methods, "agent.respond") {
		t.Fatalf("delegated task should materialize before ack, got method order %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskReconcilesDuplicateClaimReceipt(t *testing.T) {
	var savedStates []RuntimeScratchState
	var responded string
	var methods []string
	var hydrateCalls int
	var sessionsListCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			hydrateCalls++
			if hydrateCalls == 1 {
				writeRPCResult(w, req, delegatedHydrationBundle("task-impl-1", "PENDING", "", ""))
				return
			}
			writeRPCResult(w, req, delegatedHydrationBundle("task-impl-1", "RUNNING", "beta", "CLAIMED"))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("beta", "task-impl-1"))
		case "agent.task.claim":
			if got := rpcString(req.Params, "task_id"); got != "task-impl-1" {
				t.Fatalf("expected claim task task-impl-1, got %q", got)
			}
			writeRPCError(w, req, -32000, "task claim transition is stale or duplicate")
		case "agent.state.set":
			savedStates = append(savedStates, decodeRuntimeScratchStateRPC(t, req))
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, BootstrapResult{Snapshot: WorkspaceSnapshot{}})
		case "agent.limits.get":
			writeRPCResult(w, req, nil)
		case "workspace.sessions.list":
			sessionsListCalls++
			writeRPCResult(w, req, map[string]any{"sessions": []map[string]any{{
				"session_id":          "session-duplicate-claim",
				"workspace_id":        "ws",
				"agent_id":            "beta",
				"task_id":             "task-impl-1",
				"status":              "ACTIVE",
				"summary":             "already materialized by planner wake",
				"updated_at":          "2026-06-05T20:52:33Z",
				"started_at":          "2026-06-05T20:52:33Z",
				"keep_session_active": true,
			}}})
		case "workspace.execution.run.write":
			if got := rpcString(req.Params, "task_id"); got != "task-impl-1" {
				t.Fatalf("expected reconciled execution run task task-impl-1, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"run": map[string]any{
				"run_id":       firstNonEmpty(rpcString(req.Params, "run_id"), "run-duplicate-claim"),
				"workspace_id": "ws",
				"agent_id":     "beta",
				"task_id":      "task-impl-1",
				"session_id":   rpcString(req.Params, "session_id"),
				"status":       "ACTIVE",
			}})
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-duplicate-claim"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-duplicate-claim"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 4),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate-duplicate-claim",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(responded, "Cannot accept delegated task") ||
		!strings.Contains(responded, "Delegated task claim_admitted for task task-impl-1") ||
		!strings.Contains(responded, "receipt_source=reconciled") {
		t.Fatalf("duplicate claim should reconcile to claim_admitted, got %q", responded)
	}
	if hydrateCalls < 2 || sessionsListCalls != 1 {
		t.Fatalf("expected hydration/session reconciliation, hydrate_calls=%d sessions_list_calls=%d methods=%#v", hydrateCalls, sessionsListCalls, methods)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected scratch persistence, methods=%#v", methods)
	}
	last := savedStates[len(savedStates)-1]
	if last.PendingTrigger != "" || last.PendingTriggerTask != "" || last.ActiveTaskID != "task-impl-1" || last.ActiveSessionID != "session-duplicate-claim" {
		t.Fatalf("expected reconciled active scratch and cleared trigger, got %+v trace=%s", last, compactScratchTriggerTrace(savedStates))
	}
	if last.ProjectClaimHoldKind != "" || last.ProjectClaimHoldTaskID != "" {
		t.Fatalf("successful duplicate-claim reconciliation should clear stale conflict hold, got %+v", last)
	}
	if !containsAll(methods, []string{"agent.task.claim", "workspace.sessions.list", "workspace.execution.run.write", "agent.respond"}) {
		t.Fatalf("expected duplicate-claim reconciliation flow, got %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskBlocksSecondRuntimeSwitchCarrierWhileFirstActive(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-project-claim-repair-next", "PENDING", "", "", "project-alpha", "strategy")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Repair project claim handoff"
				task["description"] = "Claim this project-claim-repair carrier only after the current runtime-switch carrier is terminal."
				task["task_kind"] = "COORDINATION"
				task["tags"] = []any{"project-claim-repair", "strategic-lead"}
			}
			writeRPCResult(w, req, bundle)
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("theta", "task-project-claim-repair-next"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-single-active"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-single-active"})
		case "agent.state.set", "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("second runtime-switch carrier must decline before durable ownership, method=%q", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	self := "theta"
	claimed := "CLAIMED"
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "theta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs:           map[string]string{},
			ActiveTaskID:      "task-project-claim-repair-current",
			ActiveSessionID:   "session-current-carrier",
			LastWakeTrigger:   "runtime_switch_task",
			LastWakeTaskID:    "task-project-claim-repair-current",
			LastWakeSessionID: "session-current-carrier",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "task-project-claim-repair-current",
			Title:        "Current project claim repair",
			Status:       "RUNNING",
			TaskKind:     "COORDINATION",
			TaskTemplate: "generic",
			ProjectID:    "project-alpha",
			ProjectLane:  "strategy",
			ClaimAgentID: &self,
			ClaimStatus:  &claimed,
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-current-carrier",
			TaskID:    "task-project-claim-repair-current",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-project-claim-repair-next","prompt":"Please claim task-project-claim-repair-next."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-second-carrier",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "theta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-project-claim-repair-next") ||
		!strings.Contains(responded, "single-active follow-through invariant") ||
		!strings.Contains(responded, "task-project-claim-repair-current/session session-current-carrier") {
		t.Fatalf("expected single-active decline, got %q", responded)
	}
	if strings.Contains(responded, "claim_admitted") || strings.Contains(responded, "Queued runtime_switch_task") {
		t.Fatalf("single-active decline must not advertise queued/claimed progress, got %q", responded)
	}
	for _, forbidden := range []string{"agent.state.set", "agent.task.claim", "agent.session.start", "workspace.execution.run.write"} {
		if containsAll(methods, []string{forbidden}) {
			t.Fatalf("single-active decline must not create durable ownership via %s, methods=%#v", forbidden, methods)
		}
	}
}

func TestRuntimeAuthorityTransitionBlocksWhileRuntimeSwitchPending(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedRoleScopeHydrationBundle("task-role-scope-beta", "project-alpha"))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("theta", "task-role-scope-beta"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-pending-carrier"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-pending-carrier"})
		case "agent.state.set", "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("pending runtime-switch carrier must decline before durable ownership, method=%q", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "theta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-project-claim-repair-current",
			PendingTriggerAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-beta","prompt":"Perform the lead-level authority transition for task-role-scope-beta."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-pending-carrier",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "theta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept authority transition task task-role-scope-beta") ||
		!strings.Contains(responded, "pending runtime_switch_task for task task-project-claim-repair-current") ||
		!strings.Contains(responded, "single-active follow-through invariant") {
		t.Fatalf("expected pending-carrier decline, got %q", responded)
	}
	if strings.Contains(responded, "claim_admitted") || strings.Contains(responded, "Queued runtime_switch_task") {
		t.Fatalf("pending-carrier decline must not advertise queued/claimed progress, got %q", responded)
	}
	for _, forbidden := range []string{"agent.state.set", "agent.task.claim", "agent.session.start", "workspace.execution.run.write"} {
		if containsAll(methods, []string{forbidden}) {
			t.Fatalf("pending-carrier decline must not create durable ownership via %s, methods=%#v", forbidden, methods)
		}
	}
}

func TestRuntimeAuthorityTransitionActiveCarrierWithoutTerminalBlockerStillBlocksNextSwitch(t *testing.T) {
	self := "alpha"
	claimed := "CLAIMED"
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     self,
		},
		scratch: RuntimeScratchState{
			ActiveTaskID:      "task-role-scope-live",
			ActiveSessionID:   "session-role-scope-live",
			LastWakeTrigger:   "runtime_switch_task",
			LastWakeTaskID:    "task-role-scope-live",
			LastWakeSessionID: "session-role-scope-live",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:               "task-role-scope-live",
			ProjectID:            "project-rq",
			Status:               "RUNNING",
			TaskKind:             "COORDINATION",
			TaskTemplate:         "generic",
			ProjectLane:          "coordination",
			Tags:                 []string{"project-role-scope", "strategic-lead", "coordination"},
			TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","project_id":"project-rq","target_agent_id":"beta","role_type":"IMPLEMENTER","required_transition":"project_role_assign"}`,
			ClaimAgentID:         &self,
			ClaimStatus:          &claimed,
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-role-scope-live",
			TaskID:    "task-role-scope-live",
			AgentID:   self,
			Status:    "ACTIVE",
		},
	}

	blocker := runtime.runtimeSwitchAdmissionFollowThroughBlocker("task-role-scope-next")
	if !strings.Contains(blocker, "active runtime_switch_task carrier task task-role-scope-live/session session-role-scope-live is still runnable") ||
		!strings.Contains(blocker, "cannot be accepted until the active carrier ends, yields, or publishes a terminal blocker") {
		t.Fatalf("live authority carrier without receipt/blocker must still block next switch, got %q", blocker)
	}
}

func TestRuntimeDelegatedModelAskDeclinesResolvedTask(t *testing.T) {
	var responded string
	var methods []string
	var docKey string
	var docContent string
	var updateSummaries []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedHydrationBundle("task-smoke", "RESOLVED", "theta", "COMPLETED"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			docKey = rpcString(req.Params, "doc_key")
			docContent = rpcString(req.Params, "content")
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			updateSummaries = append(updateSummaries, rpcString(req.Params, "summary"))
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "theta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-smoke","prompt":"Please claim task-smoke and publish browser smoke evidence."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate-terminal",
		WorkspaceID: "ws",
		FromAgentID: "epsilon",
		ToAgentID:   "theta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "already terminal with status RESOLVED") || !strings.Contains(responded, "Inspect the terminal task result doc") {
		t.Fatalf("unexpected terminal delegation response: %q", responded)
	}
	if strings.HasPrefix(docKey, "task.") || !strings.HasPrefix(docKey, "agent_response.") {
		t.Fatalf("terminal delegated task decline should publish coordination evidence outside task doc namespace, got %q", docKey)
	}
	if !strings.Contains(docContent, "evidence_scope: coordination_ack_not_validation") {
		t.Fatalf("terminal delegated task decline must not look like validation evidence, got:\n%s", docContent)
	}
	for _, summary := range updateSummaries {
		if strings.Contains(summary, "switch_queued") {
			t.Fatalf("terminal delegated task should not post switch_queued update, got summaries %#v", updateSummaries)
		}
	}
	if containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("terminal delegated task should not queue switch, got %#v", methods)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("terminal delegated task request should not wake planner")
	default:
	}
}

func TestDelegatedAgentTaskRequestDetectionDoesNotStealReviewRequests(t *testing.T) {
	reviewPayload := `{"prompt":"Review this task before terminal completion. Give bounded, concrete feedback.\n- task_id: task-critical\nReturn approve/blockers and missing tests."}`
	for _, payload := range []string{
		reviewPayload,
		`{"prompt":"Please review deliverable task-critical and verify it before terminal completion."}`,
	} {
		if delegated, ok := delegatedAgentTaskRequestFromRecord(AgentRequestRecord{
			RequestID: "areq-review",
			Method:    "model.ask",
			Payload:   payload,
		}); ok {
			t.Fatalf("review request should stay read-only, got delegated %+v payload=%s", delegated, payload)
		}
	}
}

func TestDelegatedAgentTaskRequestDetectionFallsBackForLegacyPrompt(t *testing.T) {
	for _, prompt := range []string{
		"Please claim task-impl-2 and work on task-impl-2 using your normal planner loop.",
		"Please claim the implementation task task-impl-2.",
		"Please take the implementation task task-impl-2.",
		"Please handle task-impl-2 as your bounded implementation lane.",
		"Please claim task-impl-2 and verify the implementation in your planner loop.",
	} {
		delegated, ok := delegatedAgentTaskRequestFromRecord(AgentRequestRecord{
			RequestID: "areq-legacy",
			Method:    "model.ask",
			Payload:   `{"prompt":` + strconv.Quote(prompt) + `}`,
		})
		if !ok || delegated.TaskID != "task-impl-2" {
			t.Fatalf("expected legacy claim prompt to become delegated task, got ok=%v delegated=%+v prompt=%q", ok, delegated, prompt)
		}
	}
}

func TestRuntimeDelegatedModelAskQueuesBeforeResponse(t *testing.T) {
	var savedStates []RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedHydrationBundle("task-impl-1", "PENDING", "", ""))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("beta", "task-impl-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "beta", "task-impl-1") {
				return
			}
		case "agent.state.set":
			saved := decodeRuntimeScratchStateRPC(t, req)
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			http.Error(w, "response failed", http.StatusInternalServerError)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	})
	if err == nil {
		t.Fatal("expected response failure to surface")
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("delegated request should wake planner before response ack")
	}
	if len(savedStates) == 0 {
		t.Fatal("expected delegated request to persist materialized claim before response")
	}
	last := savedStates[len(savedStates)-1]
	if last.PendingTrigger != "" || last.PendingTriggerTask != "" || last.ActiveTaskID != "task-impl-1" || last.ActiveSessionID == "" {
		t.Fatalf("response failure should still leave delegated claim/session materialized, got %+v trace=%s", last, compactScratchTriggerTrace(savedStates))
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.state.set", "agent.respond"}) {
		t.Fatalf("expected materialization before response failure, got %#v", methods)
	}
	if stringIndex(methods, "agent.task.claim") > stringIndex(methods, "agent.respond") ||
		stringIndex(methods, "agent.session.start") > stringIndex(methods, "agent.respond") {
		t.Fatalf("delegated claim/session should materialize before response ack, got method order %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskDeclinesDurableSwitchWhenScratchSaveFails(t *testing.T) {
	var methods []string
	var responded string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedHydrationBundle("task-impl-1", "PENDING", "", ""))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("beta", "task-impl-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "beta", "task-impl-1") {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.state.set":
			http.Error(w, "scratch save failed", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest should publish durable-queue decline, got %v", err)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("durable queue failure must not retain executable local wake fallback")
	default:
	}
	runtime.mu.Lock()
	trigger := runtime.scratch.PendingTrigger
	taskID := runtime.scratch.PendingTriggerTask
	runtime.mu.Unlock()
	if trigger != "" || taskID != "" {
		t.Fatalf("expected no local pending trigger after scratch save failure, got trigger=%q task=%q", trigger, taskID)
	}
	if strings.Contains(responded, "Queued runtime_switch_task") || !strings.Contains(responded, "could not durably queue runtime_switch_task") {
		t.Fatalf("scratch save failure must not be acknowledged as durable switch_queued, got %q", responded)
	}
	if stringIndex(methods, "agent.state.set") > stringIndex(methods, "agent.respond") {
		t.Fatalf("durable queue attempt should happen before response ack, got methods %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskDeclinesAlreadyClaimedTask(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedHydrationBundle("task-impl-1", "PENDING", "delta", "CLAIMED"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.state.set":
			t.Fatalf("declined delegated task must not queue scratch trigger")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-impl-1") || !strings.Contains(responded, "already claimed by delta") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined delegated task should not wake planner")
	default:
	}
	if containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("declined delegated task must not save pending trigger, got methods %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskDeclinesProjectTaskForWrongRole(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle("task-impl-1", "PENDING", "", "", "project-alpha", "implementation"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedProjectCoordination([]map[string]any{
				{"role_id": "role-beta", "workspace_id": "ws", "project_id": "project-alpha", "agent_id": "beta", "role_type": "IMPLEMENTER", "status": "ACTIVE", "write_scope_json": `{"paths":["src/**"]}`},
				{"role_id": "role-epsilon", "workspace_id": "ws", "project_id": "project-alpha", "agent_id": "epsilon", "role_type": "REVIEWER", "status": "ACTIVE"},
			}))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.state.set":
			t.Fatalf("wrong-role delegated task must not queue scratch trigger")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "epsilon",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "epsilon",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-impl-1") || !strings.Contains(responded, "not role-matched") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("wrong-role delegated task should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydration, coordination, and decline evidence methods, got %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskTrustFirstQueuesProjectTaskForWrongRole(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle("task-impl-1", "PENDING", "", "", "project-alpha", "implementation"))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("epsilon", "task-impl-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "epsilon", "task-impl-1") {
				return
			}
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "project.coordination.get":
			t.Fatalf("trust_first delegated implementation handoff should not hard-check project role fit")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "epsilon",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "epsilon",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-impl-1" || saved.ActiveSessionID == "" {
		t.Fatalf("expected trust_first wrong-role delegation to materialize claim/session, got %+v", saved)
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-impl-1") ||
		!strings.Contains(responded, "Work is not complete until") {
		t.Fatalf("unexpected delegated response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected trust_first delegated project task to wake planner")
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond", "workspace.doc.put", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected hydration and delegation evidence methods, got %#v", methods)
	}
}

func TestRuntimeDeclinesDelegatedOwnerBoundTaskForNonOwner(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-submit", "PENDING", "", "", "project-alpha", "integration")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Owner-only project_patch_queue_submit"
				task["description"] = "Owner-only submit.\n\n- branch_id: branch-gamma"
				task["tags"] = []any{"owner-bound", "owner-bound-kind:patch_queue_submit", "owner-branch:branch-gamma", "required-agent:gamma"}
			}
			writeRPCResult(w, req, bundle)
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedOwnerBoundProjectCoordination("gamma"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "iota",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-submit","prompt":"Please claim task-submit."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-owner-bound",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "iota",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(strings.Join(methods, ","), "agent.state.set") {
		t.Fatalf("owner-bound decline must not queue runtime_switch_task, methods=%#v", methods)
	}
	if !strings.Contains(responded, "requires branch owner gamma") || !strings.Contains(responded, "delegate to that agent") {
		t.Fatalf("unexpected owner-bound decline response: %q", responded)
	}
}

func TestRuntimeDeclinesDelegatedStalePatchQueueSupersedeTask(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-patchq-supersede-project-alpha-olditem", "PENDING", "", "", "project-alpha", "integration")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Supersede blocked patch queue item after fresh evidence"
				task["description"] = "Patch queue stewardship.\n\n- queue_id: oldq\n- item_id: olditem\n- branch_id: branch-old\n- head_sha: " + strings.Repeat("a", 40)
				task["tags"] = []any{"project", "patch-queue", "supersede", "queue:oldq", "item:olditem"}
			}
			writeRPCResult(w, req, bundle)
		case "project.coordination.get":
			blocked := patchQueueLifecycleItem(map[string]any{
				"queue_id":         "oldq",
				"item_id":          "olditem",
				"project_id":       "project-alpha",
				"branch_id":        "branch-old",
				"state":            "BLOCKED",
				"head_sha":         strings.Repeat("a", 40),
				"updated_at":       "2026-05-13T18:00:00Z",
				"decided_at":       "2026-05-13T18:00:00Z",
				"decided_by":       "epsilon",
				"pathset":          []string{"src/**"},
				"decision_summary": "Blocked pending fresh evidence.",
			})
			accepted := patchQueueLifecycleItem(map[string]any{
				"queue_id":            "newq",
				"item_id":             "newitem",
				"project_id":          "project-alpha",
				"branch_id":           "branch-new",
				"state":               "ACCEPTED",
				"head_sha":            strings.Repeat("b", 40),
				"supersedes_queue_id": "oldq",
				"supersedes_item_id":  "olditem",
				"updated_at":          "2026-05-13T19:00:00Z",
				"decided_at":          "2026-05-13T19:00:00Z",
				"decided_by":          "epsilon",
				"pathset":             []string{"src/**"},
				"decision_summary":    "Accepted successor with fresh evidence.",
			})
			writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{blocked, accepted}))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-stale-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-stale-decline"})
		case "agent.state.set":
			t.Fatalf("stale patch queue delegation must not queue runtime_switch_task")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-patchq-supersede-project-alpha-olditem","prompt":"Please claim task-patchq-supersede-project-alpha-olditem."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-stale-patchq",
		WorkspaceID: "ws",
		FromAgentID: "epsilon",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-patchq-supersede-project-alpha-olditem") ||
		!strings.Contains(responded, "stale in current project coordination") ||
		!strings.Contains(responded, "Do not delegate or queue runtime_switch_task") {
		t.Fatalf("unexpected stale decline response: %q", responded)
	}
	if strings.Contains(strings.Join(methods, ","), "agent.state.set") {
		t.Fatalf("stale delegation must not queue runtime_switch_task, methods=%#v", methods)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("stale delegated patch queue task should not wake planner")
	default:
	}
}

func TestRuntimeDeclinesStalePatchQueueTaskInsteadOfActiveLaneNudge(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-patchq-supersede-project-alpha-olditem", "PENDING", "", "", "project-alpha", "integration")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Publish candidate evidence for stale patch queue item"
				task["description"] = "Patch queue stewardship.\n\n- queue_id: oldq\n- item_id: olditem\n- branch_id: branch-old\n- state: BLOCKED\n- head_sha: " + strings.Repeat("a", 40)
				task["tags"] = []any{"project", "patch-queue", "supersede", "blocked", "queue:oldq", "item:olditem"}
			}
			writeRPCResult(w, req, bundle)
		case "project.coordination.get":
			blocked := patchQueueLifecycleItem(map[string]any{
				"queue_id":         "oldq",
				"item_id":          "olditem",
				"project_id":       "project-alpha",
				"branch_id":        "branch-old",
				"state":            "BLOCKED",
				"head_sha":         strings.Repeat("a", 40),
				"updated_at":       "2026-05-13T18:00:00Z",
				"decided_at":       "2026-05-13T18:00:00Z",
				"decided_by":       "epsilon",
				"pathset":          []string{"src/**"},
				"decision_summary": "Blocked pending fresh evidence.",
			})
			accepted := patchQueueLifecycleItem(map[string]any{
				"queue_id":            "newq",
				"item_id":             "newitem",
				"project_id":          "project-alpha",
				"branch_id":           "branch-new",
				"state":               "ACCEPTED",
				"head_sha":            strings.Repeat("b", 40),
				"supersedes_queue_id": "oldq",
				"supersedes_item_id":  "olditem",
				"updated_at":          "2026-05-13T19:00:00Z",
				"decided_at":          "2026-05-13T19:00:00Z",
				"decided_by":          "epsilon",
				"pathset":             []string{"src/**"},
				"decision_summary":    "Accepted successor with fresh evidence.",
			})
			writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{blocked, accepted}))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-stale-active-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-stale-active-decline"})
		case "agent.state.set":
			t.Fatalf("stale same-project patch queue task must not queue request_resume or runtime_switch_task")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-active-implementation",
			Title:       "Build active project lane",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-alpha",
			ProjectLane: "implementation",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-active-implementation",
			TaskID:    "task-active-implementation",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-patchq-supersede-project-alpha-olditem","prompt":"Please claim task-patchq-supersede-project-alpha-olditem and publish evidence."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-stale-patchq-active",
		WorkspaceID: "ws",
		FromAgentID: "epsilon",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(responded, "Queued request_resume") ||
		!strings.Contains(responded, "stale in current project coordination") ||
		!strings.Contains(responded, "Do not delegate or queue runtime_switch_task") {
		t.Fatalf("stale patch queue task should decline instead of nudging active lane, got %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("stale delegated patch queue task should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected stale patch queue blocker path, got %#v", methods)
	}
}

func TestRuntimeQueuesDelegatedOwnerBoundTaskForOwnerWithProseBranchMention(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-submit", "PENDING", "", "", "project-alpha", "integration")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Beta owner requeue submit for validated branch"
				task["description"] = "Create a precise owner lane for branch `branch-gamma` / `agent/gamma/owner-bound-submit` without changing branch contents."
				task["tags"] = []any{"project", "patch-queue", "requeue", "coordination", "owner-only", "beta"}
			}
			writeRPCResult(w, req, bundle)
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedProjectCoordination(nil))
		case "project.branches.list":
			writeRPCResult(w, req, map[string]any{
				"branches": delegatedOwnerBoundProjectCoordination("beta")["coordination"].(map[string]any)["branches"],
				"count":    1,
			})
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("beta", "task-submit"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "beta", "task-submit") {
				return
			}
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-owner-bound-queued"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-owner-bound-queued"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "beta",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-submit","prompt":"Please claim task-submit."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-owner-bound-owner",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-submit" || saved.ActiveSessionID == "" {
		t.Fatalf("expected owner-bound prose task to materialize claim/session, got %+v", saved)
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-submit") {
		t.Fatalf("unexpected owner-bound queued response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected owner-bound delegated task to wake planner")
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "project.branches.list", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.state.set", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydration, coordination, queue, and evidence methods, got %#v", methods)
	}
}

func TestRuntimePostsDelegatedSwitchDependencyBlockerEvidence(t *testing.T) {
	var payload map[string]any
	var summary string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.update.post":
			summary = rpcString(req.Params, "summary")
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-blocked"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "zeta",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	err := runtime.maybePostDelegatedSwitchBlockedUpdate(context.Background(), AgentWorkNextResult{
		WorkspaceID: "ws",
		AgentID:     "zeta",
		Reason:      "task_dependency_blocked",
		Packet: &AgentWorkPacket{
			Gate: &AgentWorkGate{Summary: "task task-integrate is blocked by unresolved dependency task-root"},
			ContextHints: AgentWorkContextHints{
				AnchorConflictTaskIDs: []string{"task-root"},
			},
		},
	}, pendingWorkTrigger{Trigger: "runtime_switch_task", TaskID: "task-integrate"})
	if err != nil {
		t.Fatalf("post delegated switch blocker: %v", err)
	}
	if payload["delegation_state"] != "blocked_dependency" || payload["task_id"] != "task-integrate" || payload["to_agent_id"] != "zeta" {
		t.Fatalf("unexpected delegated blocker payload: %+v", payload)
	}
	if payload["suggested_route"] != "route_to_dependency_resolution_before_redelegation" || payload["preferred_transition"] != "resolve_dependency" {
		t.Fatalf("dependency blocker should publish executable route, got %+v", payload)
	}
	gateSummary, _ := payload["gate_summary"].(string)
	if !strings.Contains(summary, "could not reach claim_admitted") || !strings.Contains(gateSummary, "task-root") {
		t.Fatalf("expected dependency blocker summary, summary=%q payload=%+v", summary, payload)
	}
}

func TestRuntimePostsDelegatedSwitchProfileBlockerRoute(t *testing.T) {
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.update.post":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-profile-blocked"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "epsilon",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	err := runtime.maybePostDelegatedSwitchBlockedUpdate(context.Background(), AgentWorkNextResult{
		WorkspaceID:                "ws",
		AgentID:                    "epsilon",
		Reason:                     "trigger_no_work",
		ProfileGateBlockedWork:     true,
		ProfileGateReason:          "profile_task_mode_mismatch",
		ProfileGateSummary:         "Agent fresh-selection mode review is not eligible for triggered task task-design-doc.",
		AutonomousExecutionAllowed: true,
	}, pendingWorkTrigger{Trigger: "runtime_switch_task", TaskID: "task-design-doc"})
	if err != nil {
		t.Fatalf("post delegated profile blocker: %v", err)
	}
	if payload["delegation_state"] != "blocked_profile" || payload["coverage_state"] != "not_claimed" {
		t.Fatalf("unexpected profile blocker payload: %+v", payload)
	}
	if payload["suggested_route"] != "assign_to_eligible_profile_or_leave_for_frontier_self_selection" || payload["preferred_transition"] != "route_to_eligible_profile" {
		t.Fatalf("profile blocker should publish actionable routing evidence, got %+v", payload)
	}
}

func TestRuntimeDeclinesDelegatedValidationTaskWhenWorkNextNeedsArtifact(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle("task-visual", "PENDING", "", "", "project-alpha", "verification"))
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-visual" {
				t.Fatalf("expected delegated runtime switch gate check, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"workspace_id": "ws",
				"agent_id":     "theta",
				"has_work":     false,
				"reason":       "project_validation_artifact_missing",
				"packet": map[string]any{
					"work_type": "project_validation_artifact_missing",
					"gate":      map[string]any{"summary": "Project validation work is waiting for a reviewable artifact."},
				},
			})
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "theta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-visual","prompt":"Please verify the public article page visually."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-visual",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "theta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-visual") || !strings.Contains(responded, "reviewable artifact") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	if containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("validation artifact blocker must not queue runtime_switch_task, methods=%#v", methods)
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrate/work-next/decline evidence methods, got %#v", methods)
	}
}

func TestRuntimeDeclinesDelegatedProjectSpecTaskWhenWorkNextProfileGateBlocks(t *testing.T) {
	var responded string
	var declinePayload map[string]any
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle("task-design-doc", "PENDING", "", "", "project-alpha", "spec"))
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-design-doc" {
				t.Fatalf("expected delegated runtime switch gate check, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"workspace_id":              "ws",
				"agent_id":                  "epsilon",
				"has_work":                  false,
				"reason":                    "trigger_no_work",
				"trigger":                   "runtime_switch_task",
				"profile_gate_blocked_work": true,
				"profile_gate_reason":       "profile_task_mode_mismatch",
				"profile_gate_summary":      "Agent fresh-selection mode review is not eligible for triggered task task-design-doc.",
			})
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil && payload["delegation_state"] == "declined" {
				declinePayload = payload
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.state.set":
			t.Fatalf("profile-blocked delegated switch must not be queued")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "epsilon",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-design-doc","prompt":"Please claim the project design doc task."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-design-doc",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "epsilon",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-design-doc") || !strings.Contains(responded, "fresh-selection mode review is not eligible") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	if declinePayload["suggested_route"] != "assign_to_eligible_profile_or_leave_for_frontier_self_selection" || declinePayload["preferred_transition"] != "route_to_eligible_profile" {
		t.Fatalf("profile-blocked decline should publish actionable routing evidence, got %+v", declinePayload)
	}
	if containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("profile-blocked delegated switch must not queue runtime_switch_task, methods=%#v", methods)
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrate/work-next/decline evidence methods, got %#v", methods)
	}
}

func TestRuntimeDelegatedSwitchNoWorkClearsPendingWithoutProfileFalseBlock(t *testing.T) {
	var payload map[string]any
	var saved RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-done" {
				t.Fatalf("expected delegated runtime switch hints, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-05-12T00:00:00Z",
				"workspace_id":                 "ws",
				"agent_id":                     "gamma",
				"has_work":                     false,
				"reason":                       "trigger_task_terminal",
				"trigger":                      "runtime_switch_task",
				"autonomous_execution_allowed": true,
				"profile_gate_reason":          "profile_allows_autonomous_execution",
				"profile_gate_summary":         "Agent profile allows autonomous work selection.",
			})
		case "agent.update.post":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err != nil {
				t.Fatalf("decode update payload: %v", err)
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-terminal"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-done",
			DocSHAs:            map[string]string{},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("expected terminal delegated switch to stay idle, got %+v", task)
	}
	if payload["delegation_state"] != "trigger_task_terminal" || payload["profile_gate_reason"] != "profile_allows_autonomous_execution" {
		t.Fatalf("expected terminal state without false profile blocker, got %+v", payload)
	}
	payloadSummary, _ := payload["summary"].(string)
	if strings.Contains(payloadSummary, "Agent profile allows autonomous") {
		t.Fatalf("summary should use terminal trigger reason, got %+v", payload)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" {
		t.Fatalf("expected no-work delegated switch to clear pending trigger, got %+v", saved)
	}
	if !containsAll(methods, []string{"agent.work.next", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected work, evidence, and trigger clear path, got %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskQueuesProjectTaskForMatchingImplementerRole(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle("task-impl-1", "PENDING", "", "", "project-alpha", "implementation"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedProjectCoordination([]map[string]any{
				{"role_id": "role-beta", "workspace_id": "ws", "project_id": "project-alpha", "agent_id": "beta", "role_type": "IMPLEMENTER", "status": "ACTIVE", "write_scope_json": `{"paths":["src/**"]}`},
				{"role_id": "role-epsilon", "workspace_id": "ws", "project_id": "project-alpha", "agent_id": "epsilon", "role_type": "REVIEWER", "status": "ACTIVE"},
			}))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("beta", "task-impl-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "beta", "task-impl-1") {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-impl-1" || saved.ActiveSessionID == "" {
		t.Fatalf("expected matching role to materialize claim/session, got %+v", saved)
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-impl-1") ||
		!strings.Contains(responded, "Work is not complete until") {
		t.Fatalf("unexpected delegated response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected matching delegated project task to wake planner")
	}
}

func TestRuntimeDelegatedModelAskQueuesReviewProjectGateWithoutImplementerRole(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-review-1", "PENDING", "", "", "project-alpha", "review")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["requires_project_gate"] = true
			}
			writeRPCResult(w, req, bundle)
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("epsilon", "task-review-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "epsilon", "task-review-1") {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "project.coordination.get":
			t.Fatalf("review delegated task should not require implementer role coordination")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "epsilon",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-review-1","prompt":"Please claim task-review-1 and review the implementation lane."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "epsilon",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-review-1" || saved.ActiveSessionID == "" {
		t.Fatalf("expected review task delegation to materialize claim/session, got %+v", saved)
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-review-1") ||
		!strings.Contains(responded, "Work is not complete until") {
		t.Fatalf("unexpected delegated response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected delegated review project task to wake planner")
	}
	if containsAll(methods, []string{"project.coordination.get"}) {
		t.Fatalf("review project gate should not fetch implementer-role coordination, got %#v", methods)
	}
}

func TestRuntimeDelegatedStructuredPlanningEvidenceTaskBypassesImplementationAdmission(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedStructuredPlanningEvidenceHydrationBundle("task-ambient-project-signal01-rq-s1-84dc75ae6d755732", "project-signal01-rq-s1"))
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-ambient-project-signal01-rq-s1-84dc75ae6d755732" {
				t.Fatalf("expected structured planning evidence runtime_switch_task gate, got %+v", req.Params)
			}
			writeRPCResult(w, req, delegatedStructuredPlanningEvidenceWorkNextResult("epsilon", "task-ambient-project-signal01-rq-s1-84dc75ae6d755732", "project-signal01-rq-s1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "epsilon", "task-ambient-project-signal01-rq-s1-84dc75ae6d755732") {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "project.coordination.get":
			t.Fatalf("structured planning evidence task should not require implementer-role coordination")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "epsilon",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-ambient-project-signal01-rq-s1-84dc75ae6d755732","prompt":"Please materialize the project product contract and plan review docs."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-structured-planning",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "epsilon",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-ambient-project-signal01-rq-s1-84dc75ae6d755732" || saved.ActiveSessionID == "" {
		t.Fatalf("expected structured planning evidence delegation to materialize claim/session, got %+v", saved)
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-ambient-project-signal01-rq-s1-84dc75ae6d755732") ||
		strings.Contains(responded, "Cannot accept delegated task") ||
		strings.Contains(responded, "branch_bound") {
		t.Fatalf("unexpected delegated response: %q", responded)
	}
	if containsAll(methods, []string{"project.coordination.get"}) {
		t.Fatalf("structured planning evidence task should not fetch implementer-role coordination, got %#v", methods)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected structured planning evidence delegated task to wake planner")
	}
}

func TestRuntimeDelegatedModelAskDeclinesMissingTask(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at":   "2026-05-07T00:00:00Z",
				"workspace_task": nil,
				"docs":           []any{},
				"task_links":     []any{},
				"related_tasks":  []any{},
				"artifacts":      []any{},
				"updates":        []any{},
			}})
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.state.set":
			t.Fatalf("missing delegated task must not queue scratch trigger")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-does-not-exist","prompt":"Please claim task-does-not-exist."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-does-not-exist") || !strings.Contains(responded, "does not exist") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined missing delegated task should not wake planner")
	default:
	}
	if containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("missing delegated task must not save pending trigger, got methods %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskCanPreemptIdleReflection(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedHydrationBundle("task-impl-1", "PENDING", "", ""))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("beta", "task-impl-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "beta", "task-impl-1") {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID: "task-idle-reflection-workspace-ws-20260508-10",
			Status: "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-idle",
			TaskID:    "task-idle-reflection-workspace-ws-20260508-10",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-impl-1","prompt":"Please claim task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-impl-1" || saved.ActiveSessionID == "" {
		t.Fatalf("expected delegated task to preempt idle reflection and materialize claim/session, got %+v", saved)
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-impl-1") ||
		!strings.Contains(responded, "Work is not complete until") {
		t.Fatalf("unexpected delegated response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected idle reflection preemption to wake planner")
	}
}

func TestRuntimeDelegatedModelAskCanPreemptSameProjectUmbrella(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-project-claim-repair-1", "PENDING", "", "", "project-icon-sprite-forge", "strategy")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Repair project claim scope conflict"
				task["description"] = "Claim this task as the active strategic lead and unblock the overlapping implementation lane with fresh branch evidence."
				task["task_kind"] = "COORDINATION"
				task["task_template"] = "generic"
				task["tags"] = []any{"project-claim-repair", "strategic-lead"}
			}
			writeRPCResult(w, req, bundle)
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("alpha", "task-project-claim-repair-1"))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "alpha", "task-project-claim-repair-1") {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "root-icon-sprite-forge-cleanroom-20260512",
			Title:        "Clean-room project root",
			Status:       "RUNNING",
			TaskKind:     "COORDINATION",
			TaskTemplate: "project",
			ProjectID:    "project-icon-sprite-forge",
			ProjectLane:  "strategy",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-root",
			TaskID:    "root-icon-sprite-forge-cleanroom-20260512",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-project-claim-repair-1","prompt":"Please claim task-project-claim-repair-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "iota",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "task-project-claim-repair-1" || saved.ActiveSessionID == "" {
		t.Fatalf("expected delegated repair task to preempt project umbrella and materialize claim/session, got %+v", saved)
	}
	if !strings.Contains(responded, "Delegated task claim_admitted for task task-project-claim-repair-1") {
		t.Fatalf("unexpected delegated response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected project umbrella preemption to wake planner")
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond", "workspace.doc.put", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected hydrated accepted delegation path, got %#v", methods)
	}
}

func TestRuntimeDelegatedRepairTaskNudgesSameProjectActiveImplementationTask(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-project-claim-repair-1", "PENDING", "", "", "project-icon-sprite-forge", "strategy")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Gather fresh owner and branch-head evidence"
				task["description"] = "Gather current implementation state, live owner, branch, HEAD SHA, and blocker evidence for the active project lane."
				task["task_kind"] = "COORDINATION"
				task["task_template"] = "generic"
				task["tags"] = []any{"project-claim-repair", "strategic-lead"}
			}
			writeRPCResult(w, req, bundle)
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("active implementation must not be preempted by claim-repair ownership, method=%q", req.Method)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-delegate"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-delegate"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := runtimeWithActiveProjectUmbrella(server.URL)
	runtime.activeTask = &WorkspaceTaskRecord{
		TaskID:      "task-current-active",
		Title:       "Implement Service Cartography Studio MVP dashboard",
		Status:      "RUNNING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-icon-sprite-forge",
		ProjectLane: "implementation",
	}
	runtime.activeSession = &AgentSessionStateRecord{
		SessionID: "session-current-active",
		TaskID:    "task-current-active",
		Status:    "ACTIVE",
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-project-claim-repair-1","prompt":"Please claim task-project-claim-repair-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "iota",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-current-active" || saved.PendingTriggerSession != "session-current-active" {
		t.Fatalf("expected same-project repair task to resume the active implementation lane, got %+v", saved)
	}
	if !strings.Contains(responded, "Queued request_resume on active task task-current-active") || strings.Contains(responded, "Delegated task claim_admitted") {
		t.Fatalf("repair task must nudge active implementation, got %q", responded)
	}
	for _, forbidden := range []string{"agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write"} {
		if containsAll(methods, []string{forbidden}) {
			t.Fatalf("active implementation nudge must not materialize second ownership via %s, methods=%#v", forbidden, methods)
		}
	}
}

func TestRuntimeDelegatedModelAskDeclinesForeignProjectWhileOnUmbrella(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle("task-project-claim-repair-1", "PENDING", "", "", "project-other", "qa"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.state.set":
			t.Fatalf("foreign-project delegated task must not queue scratch trigger")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := runtimeWithActiveProjectUmbrella(server.URL)
	payload := `{"request_kind":"delegate_task","task_id":"task-project-claim-repair-1","prompt":"Please claim task-project-claim-repair-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "iota",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-project-claim-repair-1") ||
		!strings.Contains(responded, "already actively working on task root-icon-sprite-forge-cleanroom-20260512") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined foreign-project delegated task should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrated decline path, got %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskDeclinesUmbrellaToUmbrellaPreemption(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("root-icon-sprite-followup", "PENDING", "", "", "project-icon-sprite-forge", "strategy")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["task_kind"] = "COORDINATION"
				task["task_template"] = "project"
				task["title"] = "Project strategy umbrella follow-up"
			}
			writeRPCResult(w, req, bundle)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.state.set":
			t.Fatalf("umbrella-to-umbrella delegated task must not queue scratch trigger")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := runtimeWithActiveProjectUmbrella(server.URL)
	payload := `{"request_kind":"delegate_task","task_id":"root-icon-sprite-followup","prompt":"Please claim root-icon-sprite-followup."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "iota",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task root-icon-sprite-followup") ||
		!strings.Contains(responded, "already actively working on task root-icon-sprite-forge-cleanroom-20260512") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined umbrella-to-umbrella delegated task should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrated decline path, got %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskDeclinesRootPrefixWithoutProjectUmbrellaFields(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.task.hydrate", "agent.state.set":
			t.Fatalf("non-umbrella root-prefix active task should not hydrate or queue scratch: %s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "root-but-actually-execution",
			Status:       "RUNNING",
			TaskKind:     "EXECUTION",
			TaskTemplate: "generic",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-root-ish",
			TaskID:    "root-but-actually-execution",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-project-claim-repair-1","prompt":"Please claim task-project-claim-repair-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "iota",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-project-claim-repair-1") ||
		!strings.Contains(responded, "already actively working on task root-but-actually-execution") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined non-umbrella root-prefix delegated task should not wake planner")
	default:
	}
	if containsAll(methods, []string{"agent.task.hydrate", "agent.state.set"}) {
		t.Fatalf("unexpected hydrate/scratch path, got %#v", methods)
	}
}

func TestRuntimeDelegatedModelAskDeclinesWhileActiveOnDifferentTask(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-decline"})
		case "agent.task.hydrate", "agent.state.set":
			t.Fatalf("active-task decline should not hydrate remote task or queue scratch: %s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "delta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID: "task-review",
			Status: "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-review",
			TaskID:    "task-review",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-pipeline","prompt":"Please claim task-pipeline."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "delta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Cannot accept delegated task task-pipeline") || !strings.Contains(responded, "already actively working on task task-review") {
		t.Fatalf("unexpected decline response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined delegated task should not wake planner")
	default:
	}
}

func TestRuntimeDelegatedModelAskQueuesActiveLaneResumeForSameProjectStatusNudge(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var updateSummary string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-frontend-status", "PENDING", "", "", "project-image-lab", "frontend")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Publish frontend lane status and candidate evidence"
				task["description"] = "Summarize current implementation state and evidence for the active frontend lane."
			}
			writeRPCResult(w, req, bundle)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-active-lane-nudge"})
		case "agent.update.post":
			updateSummary = rpcString(req.Params, "summary")
			writeRPCResult(w, req, map[string]any{"update_id": "upd-active-lane-nudge"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-build-frontend",
			Title:       "Build frontend shell",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-image-lab",
			ProjectLane: "frontend",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-frontend",
			TaskID:    "task-build-frontend",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-frontend-status","prompt":"Please claim task-frontend-status and publish current frontend lane status."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-delegate-status",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "beta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-build-frontend" || saved.PendingTriggerSession != "session-frontend" {
		t.Fatalf("same-project status nudge should resume active lane, got %+v", saved)
	}
	if !strings.Contains(responded, "Queued request_resume on active task task-build-frontend") ||
		!strings.Contains(responded, "does not claim the sidecar task") {
		t.Fatalf("unexpected active-lane response: %q", responded)
	}
	if !strings.Contains(updateSummary, "resumed active task task-build-frontend") {
		t.Fatalf("expected active-lane update summary, got %q", updateSummary)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected active-lane nudge to wake planner")
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "agent.respond", "workspace.doc.put", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected active-lane nudge methods, got %#v", methods)
	}
}

func TestRuntimeDelegatedPublicationTaskUsesStoreAuthoritativeActiveLaneNudge(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var updateSummary string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-clearpress-provenance-publication" {
				t.Fatalf("expected store-authoritative runtime switch probe, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"workspace_id":                 "ws",
				"agent_id":                     "eta",
				"has_work":                     true,
				"reason":                       "resume_session",
				"trigger":                      "runtime_switch_task",
				"claim_action":                 "reuse_claim",
				"session_action":               "reuse_active",
				"resume_summary":               "publication sidecar rerouted to active implementation lane",
				"project_id":                   "project-clearpress",
				"project_lane":                 "implementation",
				"task_kind":                    "EXECUTION",
				"requires_project_gate":        true,
				"autonomous_execution_allowed": true,
				"task": map[string]any{
					"task_id":        "task-clearpress-build-shell",
					"title":          "Implement Clearpress app shell",
					"owner_user_id":  "owner-1",
					"priority":       "high",
					"status":         "RUNNING",
					"task_kind":      "EXECUTION",
					"task_template":  "generic",
					"project_id":     "project-clearpress",
					"project_lane":   "implementation",
					"claim_agent_id": "eta",
					"claim_status":   "CLAIMED",
					"linked_by":      "alpha",
					"linked_at":      "2026-05-21T00:00:00Z",
				},
				"session": map[string]any{
					"session_id":   "session-clearpress-build-shell",
					"workspace_id": "ws",
					"agent_id":     "eta",
					"task_id":      "task-clearpress-build-shell",
					"status":       "ACTIVE",
					"summary":      "implementation active",
					"updated_at":   "2026-05-21T00:00:00Z",
					"started_at":   "2026-05-21T00:00:00Z",
				},
			})
		case "agent.task.hydrate":
			bundle := delegatedProjectHydrationBundle("task-clearpress-provenance-publication", "PENDING", "", "", "project-clearpress", "coordination")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Publish exact Clearpress runnable candidate provenance"
				task["description"] = "Publish branch/head/checkout provenance for the current active implementation lane."
				task["task_kind"] = "COORDINATION"
			}
			writeRPCResult(w, req, bundle)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-store-active-lane-nudge"})
		case "agent.update.post":
			updateSummary = rpcString(req.Params, "summary")
			writeRPCResult(w, req, map[string]any{"update_id": "upd-store-active-lane-nudge"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "eta",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	payload := `{"request_kind":"delegate_task","task_id":"task-clearpress-provenance-publication","prompt":"Please claim task-clearpress-provenance-publication and publish exact candidate provenance."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-store-active-lane",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "eta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-clearpress-build-shell" || saved.PendingTriggerSession != "session-clearpress-build-shell" {
		t.Fatalf("store authoritative nudge should resume active implementation lane, got %+v", saved)
	}
	if !strings.Contains(responded, "Queued request_resume on active task task-clearpress-build-shell") ||
		!strings.Contains(responded, "does not claim the sidecar task") {
		t.Fatalf("unexpected store active-lane response: %q", responded)
	}
	if !strings.Contains(updateSummary, "resumed active task task-clearpress-build-shell") {
		t.Fatalf("expected active-lane update summary, got %q", updateSummary)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected store active-lane nudge to wake planner")
	}
	if !containsAll(methods, []string{"agent.work.next", "agent.task.hydrate", "agent.respond", "workspace.doc.put", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected store active-lane nudge methods, got %#v", methods)
	}
}

func TestRuntimeDelegatedPatchQueueRevisionPreemptsActivePublicationForOwner(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	taskID := "task-1779057242625265200-40121fce"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedPatchQueueRevisionHydrationBundle(taskID, "project-alpha", "gamma"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedPatchQueueRevisionCoordination("gamma"))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("gamma", taskID))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "gamma", taskID) {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-patchq-revision"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-patchq-revision"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-service-cartography-studio-review-ready-publication-20260518",
			Title:       "Publish review-ready provenance for Service Cartography Studio",
			Description: "Publish branch state, review packet links, and candidate evidence for the current project lane.",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-alpha",
			ProjectLane: "coordination",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-publication",
			TaskID:    "task-service-cartography-studio-review-ready-publication-20260518",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and revise the blocked patch queue candidate."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-patchq-revision",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != taskID || saved.ActiveSessionID == "" {
		t.Fatalf("expected patch queue revision to materialize claim/session, got %+v", saved)
	}
	if strings.Contains(responded, "Queued request_resume") || !strings.Contains(responded, "Delegated task claim_admitted for task "+taskID) {
		t.Fatalf("patch queue revision must not become active-lane request_resume, got %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected patch queue revision preemption to wake planner")
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond", "workspace.doc.put", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected hydrated owner-bound switch path, got %#v", methods)
	}
	if stringIndex(methods, "agent.task.claim") > stringIndex(methods, "agent.respond") ||
		stringIndex(methods, "agent.session.start") > stringIndex(methods, "agent.respond") {
		t.Fatalf("patch queue revision should materialize before ack, got method order %#v", methods)
	}
}

func TestRuntimeDelegatedPatchQueueRevisionIgnoresSameHeadAcceptedSiblingForOwner(t *testing.T) {
	var saved RuntimeScratchState
	var responded string
	var methods []string
	taskID := "task-patchq-revision-with-accepted-sibling"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedPatchQueueRevisionHydrationBundle(taskID, "project-alpha", "gamma"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedPatchQueueRevisionCoordinationWithAcceptedSameHead("gamma"))
		case "agent.work.next":
			writeRPCResult(w, req, delegatedAcceptedWorkNextResult("gamma", taskID))
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			if handleDelegatedTaskMaterializationRPC(t, w, req, "gamma", taskID) {
				return
			}
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-patchq-revision-sibling"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-patchq-revision-sibling"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-service-cartography-studio-review-ready-publication-20260518",
			Title:       "Publish review-ready provenance for Service Cartography Studio",
			Description: "Publish branch state, review packet links, and candidate evidence for the current project lane.",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-alpha",
			ProjectLane: "coordination",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-publication",
			TaskID:    "task-service-cartography-studio-review-ready-publication-20260518",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and revise the blocked patch queue candidate."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-patchq-revision-sibling",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != taskID || saved.ActiveSessionID == "" {
		t.Fatalf("same-head accepted sibling must not stale-block patch queue revision; expected claim/session, got %+v response=%q", saved, responded)
	}
	if strings.Contains(responded, "already has an ACCEPTED patch queue decision") || !strings.Contains(responded, "Delegated task claim_admitted for task "+taskID) {
		t.Fatalf("unexpected patch queue revision response: %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected patch queue revision with accepted sibling to wake planner")
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond", "workspace.doc.put", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected hydrated owner-bound switch path, got %#v", methods)
	}
}

func TestRuntimeDelegatedPatchQueueRevisionDeclinesNonOwnerInsteadOfNudge(t *testing.T) {
	var responded string
	var methods []string
	taskID := "task-1779057242625265200-40121fce"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedPatchQueueRevisionHydrationBundle(taskID, "project-alpha", "gamma"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedPatchQueueRevisionCoordination("gamma"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-patchq-nonowner"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-patchq-nonowner"})
		case "agent.state.set":
			t.Fatalf("non-owner patch queue revision must not queue scratch trigger")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "eta",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-implementation-interactions",
			Title:       "Implement Service Cartography Studio interactions",
			Description: "Build core app interactions in the active implementation checkout.",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-alpha",
			ProjectLane: "implementation",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-implementation",
			TaskID:    "task-implementation-interactions",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and revise the blocked patch queue candidate."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-patchq-revision-nonowner",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "eta",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(responded, "Queued request_resume") || !strings.Contains(responded, "requires branch owner gamma") {
		t.Fatalf("non-owner patch queue revision should decline as owner-bound, got %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("non-owner patch queue revision decline should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrated owner-bound decline path, got %#v", methods)
	}
}

func TestRuntimeDelegatedPatchQueueRevisionRequirementsOnlyDeclinesNonOwner(t *testing.T) {
	var responded string
	var methods []string
	taskID := "task-patchq-revision-project-alpha-ecb59c61493d"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedPatchQueueRevisionRequirementsOnlyHydrationBundle(taskID, "project-alpha", "delta"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedPatchQueueRevisionCoordination("delta"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-patchq-requirements-owner"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-patchq-requirements-owner"})
		case "agent.work.next", "agent.state.set":
			t.Fatalf("requirements-only owner-bound revision must decline before runtime_switch_task admission, got %s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and revise the blocked patch queue candidate."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-patchq-requirements-owner",
		WorkspaceID: "ws",
		FromAgentID: "alpha",
		ToAgentID:   "gamma",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(responded, "claim_admitted") ||
		strings.Contains(responded, "terminal blocker") ||
		!strings.Contains(responded, "requires branch owner delta") {
		t.Fatalf("requirements-only revision should decline to branch owner, got %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("non-owner requirements-only revision decline should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrated owner-bound decline path, got %#v", methods)
	}
}

func TestRuntimeDelegatedPatchQueueRevisionDoesNotPreemptActiveImplementation(t *testing.T) {
	var responded string
	var methods []string
	taskID := "task-patchq-revision-project-alpha-item-blocked"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedPatchQueueRevisionHydrationBundle(taskID, "project-alpha", "gamma"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedPatchQueueRevisionCoordination("gamma"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-patchq-active-impl"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-patchq-active-impl"})
		case "agent.state.set":
			t.Fatalf("patch queue revision must not preempt active implementation task")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-implementation-work",
			Title:       "Implement Service Cartography Studio order status screen",
			Description: "Edit the production app source and wire the map interaction model.",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-alpha",
			ProjectLane: "implementation",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-active-impl",
			TaskID:    "task-implementation-work",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and revise the blocked patch queue candidate."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-patchq-active-impl",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(responded, "Queued request_resume") || !strings.Contains(responded, "already actively working on task task-implementation-work") {
		t.Fatalf("active implementation should keep running, got %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined active implementation preemption should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrated active implementation decline path, got %#v", methods)
	}
}

func TestRuntimeDelegatedPatchQueueRevisionDoesNotPreemptActiveStrategyWork(t *testing.T) {
	var responded string
	var methods []string
	taskID := "task-patchq-revision-project-alpha-item-blocked"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedPatchQueueRevisionHydrationBundle(taskID, "project-alpha", "gamma"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedPatchQueueRevisionCoordination("gamma"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-patchq-active-strategy"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-patchq-active-strategy"})
		case "agent.state.set":
			t.Fatalf("patch queue revision must not preempt non-sidecar active strategy work")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-strategy-work",
			Title:       "Plan next implementation contour",
			Description: "Resolve product sequencing and next task split before handing work to peers.",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-alpha",
			ProjectLane: "strategy",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-active-strategy",
			TaskID:    "task-strategy-work",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and revise the blocked patch queue candidate."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-patchq-active-strategy",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(responded, "Queued request_resume") || !strings.Contains(responded, "already actively working on task task-strategy-work") {
		t.Fatalf("active strategy work should keep running, got %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined active strategy preemption should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrated active strategy decline path, got %#v", methods)
	}
}

func TestRuntimeDelegatedPatchQueueRevisionDoesNotPreemptProjectUmbrellaWithoutPublicationSignal(t *testing.T) {
	var responded string
	var methods []string
	taskID := "task-patchq-revision-project-alpha-item-blocked"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedPatchQueueRevisionHydrationBundle(taskID, "project-alpha", "gamma"))
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedPatchQueueRevisionCoordination("gamma"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-patchq-active-umbrella"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-patchq-active-umbrella"})
		case "agent.state.set":
			t.Fatalf("patch queue revision must not preempt active project umbrella without light publication signal")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "root-service-cartography",
			Title:        "Service Cartography Studio project root",
			Description:  "Coordinate project scope, task sequencing, and peer work allocation.",
			Status:       "RUNNING",
			TaskKind:     "COORDINATION",
			TaskTemplate: "project",
			ProjectID:    "project-alpha",
			ProjectLane:  "strategy",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-active-umbrella",
			TaskID:    "root-service-cartography",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and revise the blocked patch queue candidate."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-patchq-active-umbrella",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if strings.Contains(responded, "Queued request_resume") || !strings.Contains(responded, "already actively working on task root-service-cartography") {
		t.Fatalf("active project umbrella should keep running, got %q", responded)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("declined active umbrella preemption should not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.task.hydrate", "project.coordination.get", "agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected hydrated active umbrella decline path, got %#v", methods)
	}
}

func TestDelegatedAgentTaskRequestDetectionKeepsFixQuestionsReadOnly(t *testing.T) {
	for _, prompt := range []string{
		"Please review how to fix task-impl-2.",
		"Can you answer how to fix task-impl-2 without claiming it?",
		"Can you answer whether I should implement task-impl-2?",
		"Please review how to run task-impl-2.",
	} {
		if delegated, ok := delegatedAgentTaskRequestFromRecord(AgentRequestRecord{
			RequestID: "areq-question",
			Method:    "model.ask",
			Payload:   `{"prompt":` + strconv.Quote(prompt) + `}`,
		}); ok {
			t.Fatalf("fix question should stay read-only, got delegated %+v prompt=%q", delegated, prompt)
		}
	}
}

func TestAuthorityTransitionRequestQueuesTaskSwitch(t *testing.T) {
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-gamma","prompt":"Run the strategic lead boundary transition."}`
	delegated, ok := delegatedAgentTaskRequestFromRecord(AgentRequestRecord{
		RequestID: "areq-authority",
		Method:    "model.ask",
		Payload:   payload,
	})
	if !ok {
		t.Fatal("expected authority transition request to route through the executable task switch path")
	}
	if delegated.RequestKind != agentRequestKindAuthorityTransition || delegated.TaskID != "task-role-scope-gamma" {
		t.Fatalf("unexpected authority transition request: %+v", delegated)
	}
	if !delegatedAgentTaskIDLooksStrategicRepair(delegated.TaskID) {
		t.Fatalf("role/scope authority task should be treated as strategic repair: %+v", delegated)
	}
}

func TestRuntimeAuthorityTransitionDoesNotDowngradeToActiveLaneNudge(t *testing.T) {
	var savedStates []RuntimeScratchState
	var responded string
	var claimTaskID string
	var sessionTaskID string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedRoleScopeHydrationBundle("task-role-scope-gamma", "project-alpha"))
		case "agent.work.next":
			writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-gamma"))
		case "agent.task.claim":
			claimTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			sessionTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        "ws",
				"agent_id":            "alpha",
				"task_id":             rpcString(req.Params, "task_id"),
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-06-05T12:00:00Z",
				"started_at":          "2026-06-05T12:00:00Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": "run-authority"}})
		case "agent.state.set":
			saved := decodeRuntimeScratchStateRPC(t, req)
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-authority"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-active-impl",
			Title:       "Active implementation",
			Status:      "RUNNING",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-alpha",
			ProjectLane: "implementation",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-active-impl",
			TaskID:    "task-active-impl",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-gamma","prompt":"Run project_role_assign for task-role-scope-gamma."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-authority",
		WorkspaceID: "ws",
		FromAgentID: "gamma",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Authority transition claim_admitted") || strings.Contains(responded, "Queued request_resume") || strings.Contains(responded, "Queued runtime_switch_task") {
		t.Fatalf("authority transition should materialize claim before ack, got %q", responded)
	}
	if claimTaskID != "task-role-scope-gamma" || sessionTaskID != "task-role-scope-gamma" {
		t.Fatalf("expected authority transition claim/session materialization, claim=%q session_task=%q", claimTaskID, sessionTaskID)
	}
	if len(savedStates) == 0 {
		t.Fatal("expected scratch persistence for authority transition")
	}
	lastSaved := savedStates[len(savedStates)-1]
	if lastSaved.PendingTrigger != "" || lastSaved.PendingTriggerTask != "" || lastSaved.ActiveTaskID != "task-role-scope-gamma" {
		t.Fatalf("expected authority trigger to clear after claim_admitted, got %+v trace=%s", lastSaved, compactScratchTriggerTrace(savedStates))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected authority transition to wake planner")
	}
	if got := strings.Join(methods, ","); !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond"}) {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestRuntimeAuthorityTransitionQueuesBehindAmbientCoordinationTask(t *testing.T) {
	var savedStates []RuntimeScratchState
	var responded string
	var claimTaskID string
	var sessionTaskID string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedRoleScopeHydrationBundle("task-role-scope-gamma", "project-rq"))
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-role-scope-gamma" {
				t.Fatalf("expected authority task runtime_switch_task gate, got %+v", req.Params)
			}
			writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-gamma"))
		case "agent.task.claim":
			claimTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			sessionTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        "ws",
				"agent_id":            "alpha",
				"task_id":             rpcString(req.Params, "task_id"),
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-06-05T12:00:00Z",
				"started_at":          "2026-06-05T12:00:00Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": "run-authority"}})
		case "agent.state.set":
			saved := decodeRuntimeScratchStateRPC(t, req)
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-authority"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-ambient-project-task-signal01-rq-root-5d744c0001395a6b",
			Title:       "Publish canonical rq interpreter coordination docs",
			Status:      "RUNNING",
			TaskKind:    "COORDINATION",
			ProjectLane: "coordination",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-ambient-alpha",
			TaskID:    "task-ambient-project-task-signal01-rq-root-5d744c0001395a6b",
			Status:    "ACTIVE",
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-gamma","prompt":"Run project_role_assign for task-role-scope-gamma."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-authority-ambient",
		WorkspaceID: "ws",
		FromAgentID: "gamma",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Authority transition claim_admitted") ||
		strings.Contains(responded, "already actively working") ||
		strings.Contains(responded, "Queued request_resume") ||
		strings.Contains(responded, "Queued runtime_switch_task") {
		t.Fatalf("authority transition should claim dedicated task behind ambient coordination before ack, got %q", responded)
	}
	if claimTaskID != "task-role-scope-gamma" || sessionTaskID != "task-role-scope-gamma" {
		t.Fatalf("expected authority transition claim/session materialization, claim=%q session_task=%q", claimTaskID, sessionTaskID)
	}
	if len(savedStates) == 0 {
		t.Fatal("expected scratch persistence for authority transition")
	}
	lastSaved := savedStates[len(savedStates)-1]
	if lastSaved.PendingTrigger != "" || lastSaved.PendingTriggerTask != "" || lastSaved.ActiveTaskID != "task-role-scope-gamma" {
		t.Fatalf("expected authority trigger to clear after claim_admitted, got %+v trace=%s", lastSaved, compactScratchTriggerTrace(savedStates))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected authority transition to wake planner")
	}
	if got := strings.Join(methods, ","); !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond"}) {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestRuntimeAuthorityTransitionRejectsProductTaskCarrier(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle("task-impl-1", "PENDING", "", "", "project-rq", "implementation"))
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority-decline"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-authority-decline"})
		case "agent.work.next", "agent.state.set":
			t.Fatalf("non-dedicated authority carrier must not queue runtime switch, method=%s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-impl-1","prompt":"Please perform the required authority transition for task-impl-1."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-authority-product-task",
		WorkspaceID: "ws",
		FromAgentID: "eta",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	for _, want := range []string{"Cannot accept authority transition task task-impl-1", "not a dedicated authority transition task", "task-role-scope-*"} {
		if !strings.Contains(responded, want) {
			t.Fatalf("expected response to contain %q, got %q", want, responded)
		}
	}
	if strings.Contains(responded, "Queued runtime_switch_task") || strings.Contains(responded, "Cannot accept delegated task") {
		t.Fatalf("authority product-task carrier should be a typed authority decline, got %q", responded)
	}
	if containsAll(methods, []string{"agent.work.next"}) || containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("non-dedicated authority carrier must not queue runtime switch, methods=%#v", methods)
	}
}

func TestRuntimeAuthorityTransitionRejectsBlockedCarrierTerminalBlocker(t *testing.T) {
	var responded string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedRoleScopeHydrationBundle("task-role-scope-beta", "project-rq")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["status"] = "RUNNING"
				task["claim_agent_id"] = "alpha"
				task["claim_status"] = "BLOCKED"
				task["claim_summary"] = "typed terminal blocker: no authority-bearing admission path is available"
			}
			writeRPCResult(w, req, bundle)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority-blocked"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-authority-blocked"})
		default:
			t.Fatalf("blocked authority carrier must not queue runtime switch; unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-beta","prompt":"Perform the lead-level authority transition for task-role-scope-beta."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-authority-blocked",
		WorkspaceID: "ws",
		FromAgentID: "eta",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	for _, want := range []string{"Cannot accept authority transition task task-role-scope-beta", "terminal blocker", "claim_status=BLOCKED"} {
		if !strings.Contains(responded, want) {
			t.Fatalf("expected response to contain %q, got %q", want, responded)
		}
	}
	if strings.Contains(responded, "Queued runtime_switch_task") {
		t.Fatalf("blocked authority carrier must not queue runtime switch, got %q", responded)
	}
	if containsAll(methods, []string{"agent.work.next"}) || containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("blocked authority carrier must not queue runtime switch, methods=%#v", methods)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("blocked authority carrier must not wake planner")
	default:
	}
}

func TestRuntimeAuthorityTransitionAllowsExpiredPeerClaimedCarrier(t *testing.T) {
	var savedStates []RuntimeScratchState
	var responded string
	var claimTaskID string
	var sessionTaskID string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedRoleScopeHydrationBundle("task-role-scope-beta", "project-rq")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["status"] = "RUNNING"
				task["claim_agent_id"] = "beta"
				task["claim_status"] = "CLAIMED"
				task["claim_expires_at"] = "2000-01-01T00:00:00Z"
			}
			writeRPCResult(w, req, bundle)
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-role-scope-beta" {
				t.Fatalf("expected authority task runtime_switch_task gate, got %+v", req.Params)
			}
			writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-beta"))
		case "agent.task.claim":
			claimTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			sessionTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        "ws",
				"agent_id":            "alpha",
				"task_id":             rpcString(req.Params, "task_id"),
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-06-05T12:00:00Z",
				"started_at":          "2026-06-05T12:00:00Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": "run-authority"}})
		case "agent.state.set":
			saved := decodeRuntimeScratchStateRPC(t, req)
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority-expired"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-authority-expired"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-beta","prompt":"Perform the lead-level authority transition for task-role-scope-beta."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-authority-expired-owned",
		WorkspaceID: "ws",
		FromAgentID: "gamma",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	if !strings.Contains(responded, "Authority transition claim_admitted") ||
		strings.Contains(responded, "already claimed by beta") ||
		strings.Contains(responded, "Queued runtime_switch_task") {
		t.Fatalf("expired peer claim should not hard-decline authority transition, got %q", responded)
	}
	if claimTaskID != "task-role-scope-beta" || sessionTaskID != "task-role-scope-beta" {
		t.Fatalf("expected expired-claim authority transition claim/session materialization, claim=%q session_task=%q", claimTaskID, sessionTaskID)
	}
	if len(savedStates) == 0 {
		t.Fatal("expected scratch persistence for authority transition")
	}
	lastSaved := savedStates[len(savedStates)-1]
	if lastSaved.PendingTrigger != "" || lastSaved.PendingTriggerTask != "" || lastSaved.ActiveTaskID != "task-role-scope-beta" {
		t.Fatalf("expected expired-claim authority trigger to clear after claim_admitted, got %+v trace=%s", lastSaved, compactScratchTriggerTrace(savedStates))
	}
	if got := strings.Join(methods, ","); !containsAll(methods, []string{"agent.task.hydrate", "agent.work.next", "agent.task.claim", "agent.session.start", "workspace.execution.run.write", "agent.respond"}) {
		t.Fatalf("unexpected method flow: %s", got)
	}
}

func TestRuntimeAuthorityTransitionPublishesTerminalBlockerWhenClaimAdmissionHasNoPath(t *testing.T) {
	var responded string
	var workNextCalls int
	var declinedPayload map[string]any
	var terminalPayload map[string]any
	var blockReason string
	var saved RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedRoleScopeHydrationBundle("task-role-scope-beta", "project-rq"))
		case "agent.work.next":
			workNextCalls++
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-role-scope-beta" {
				t.Fatalf("expected authority task runtime_switch_task gate, got %+v", req.Params)
			}
			if workNextCalls == 1 {
				writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-beta"))
				return
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-06-05T12:00:00Z",
				"workspace_id":                 "ws",
				"agent_id":                     "alpha",
				"has_work":                     false,
				"reason":                       "task_dependency_blocked",
				"trigger":                      "runtime_switch_task",
				"autonomous_execution_allowed": true,
				"packet": map[string]any{
					"work_type": "task_dependency_blocked",
					"gate":      map[string]any{"summary": "authority task still blocked by missing role scope evidence"},
				},
			})
		case "agent.task.block":
			if got := rpcString(req.Params, "task_id"); got != "task-role-scope-beta" {
				t.Fatalf("expected authority task block, got %+v", req.Params)
			}
			blockReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority-admission-decline"})
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil {
				switch payload["delegation_state"] {
				case "declined":
					declinedPayload = payload
				case "terminal_blocker":
					terminalPayload = payload
				}
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-authority-admission-decline"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-beta","prompt":"Perform the lead-level authority transition for task-role-scope-beta."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-authority-no-admission",
		WorkspaceID: "ws",
		FromAgentID: "gamma",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	for _, want := range []string{"Cannot accept authority transition task task-role-scope-beta", "typed terminal blocker", "authority_transition_terminal_blocker.v1", "no_completion_without_project_role_assign_receipt"} {
		if !strings.Contains(responded, want) {
			t.Fatalf("expected response to contain %q, got %q", want, responded)
		}
	}
	if strings.Contains(responded, "Queued runtime_switch_task") || strings.Contains(responded, "claim_admitted") {
		t.Fatalf("non-materialized authority transition must not ack queued/claimed, got %q", responded)
	}
	if workNextCalls != 2 {
		t.Fatalf("expected preflight and materialization work.next calls, got %d", workNextCalls)
	}
	if !strings.Contains(blockReason, "authority_transition_terminal_blocker.v1") ||
		!strings.Contains(blockReason, "required_transition=project_role_assign") {
		t.Fatalf("expected typed terminal blocker block reason, got %q", blockReason)
	}
	if terminalPayload == nil || terminalPayload["schema"] != "authority_transition_terminal_blocker.v1" ||
		terminalPayload["terminal_blocker"] != true ||
		terminalPayload["required_transition"] != projectRoleScopeAuthorityTransitionTool {
		t.Fatalf("expected terminal blocker update payload, got %#v methods=%#v", terminalPayload, methods)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "" || saved.ActiveSessionID != "" {
		t.Fatalf("terminal blocker should clear authority switch scratch, got %+v", saved)
	}
	if declinedPayload == nil || declinedPayload["delegation_state"] != "declined" || declinedPayload["request_kind"] != agentRequestKindAuthorityTransition {
		t.Fatalf("expected typed authority decline payload, got %#v methods=%#v", declinedPayload, methods)
	}
	if !containsAll(methods, []string{"agent.task.block", "agent.state.set", "agent.respond"}) {
		t.Fatalf("expected terminal blocker materialization flow, methods=%#v", methods)
	}
}

func TestRuntimeAuthorityTransitionClosesUnclaimedCarrierWhenBlockNeedsClaim(t *testing.T) {
	const taskID = "task-role-scope-6add9cf555"
	var blockReason string
	var closeReason string
	var closeResolution string
	var terminalPayload map[string]any
	var saved RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedRoleScopeHydrationBundle(taskID, "project-signal01-lua-capability"))
		case "agent.task.block":
			if got := rpcString(req.Params, "task_id"); got != taskID {
				t.Fatalf("expected authority carrier block, got %+v", req.Params)
			}
			blockReason = rpcString(req.Params, "reason")
			writeRPCError(w, req, -32000, "task claim not found")
		case "task.close":
			if got := rpcString(req.Params, "task_id"); got != taskID {
				t.Fatalf("expected authority carrier close, got %+v", req.Params)
			}
			closeResolution = rpcString(req.Params, "resolution")
			closeReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, map[string]any{"task_id": taskID, "status": "CANCELLED"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil && payload["delegation_state"] == "terminal_blocker" {
				terminalPayload = payload
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-r24-authority-terminal"})
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("unclaimed authority terminal blocker path must not create ownership, method=%s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "zeta",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: taskID,
			ActiveTaskID:       taskID,
			ActiveSessionID:    "session-authority-r24",
		},
	}
	reason, ok, err := runtime.publishAuthorityTransitionTerminalBlocker(context.Background(), delegatedAgentTaskRequest{
		TaskID:      taskID,
		RequestKind: agentRequestKindAuthorityTransition,
		Prompt:      "take the dedicated role-scope authority transition carrier",
	}, "agent.work.next rejected runtime_switch_task for task "+taskID+" before acceptance: no fresh claimable authority path")
	if err != nil {
		t.Fatalf("publishAuthorityTransitionTerminalBlocker() error = %v", err)
	}
	if !ok {
		t.Fatal("expected authority terminal blocker publication")
	}
	for _, gotReason := range []string{reason, blockReason, closeReason} {
		if !strings.Contains(gotReason, authorityTransitionTerminalBlockerSchema) ||
			!strings.Contains(gotReason, "required_transition=project_role_assign") ||
			!strings.Contains(gotReason, "no_completion_without_project_role_assign_receipt") {
			t.Fatalf("expected schema-stamped R24 authority terminal blocker reason, reason=%q block=%q close=%q", reason, blockReason, closeReason)
		}
	}
	if closeResolution != "CANCELLED" {
		t.Fatalf("expected unclaimed authority carrier to close as CANCELLED, got %q", closeResolution)
	}
	if stringIndex(methods, "agent.task.block") < 0 || stringIndex(methods, "task.close") < 0 || stringIndex(methods, "agent.task.block") > stringIndex(methods, "task.close") {
		t.Fatalf("expected missing-claim block to fall back to task.close, methods=%#v", methods)
	}
	if terminalPayload == nil ||
		terminalPayload["schema"] != authorityTransitionTerminalBlockerSchema ||
		terminalPayload["task_id"] != taskID ||
		terminalPayload["required_transition"] != projectRoleScopeAuthorityTransitionTool {
		t.Fatalf("expected R24 authority terminal payload, got %#v methods=%#v", terminalPayload, methods)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "" || saved.ActiveSessionID != "" {
		t.Fatalf("terminal blocker should clear authority switch scratch, got %+v", saved)
	}
	closedTask := WorkspaceTaskRecord{
		TaskID:               taskID,
		Status:               "CANCELLED",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectID:            "project-signal01-lua-capability",
		ProjectLane:          "coordination",
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","project_id":"project-signal01-lua-capability","target_agent_id":"gamma","role_type":"IMPLEMENTER","required_transition":"project_role_assign"}`,
		CloseReason:          closeReason,
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
	}
	if !authorityTransitionTaskHasTerminalBlocker(closedTask) || !runtimeSwitchTaskHasTerminalBlocker(closedTask) {
		t.Fatalf("closed typed authority carrier should be recognized as terminal blocker, task=%+v", closedTask)
	}
}

func TestRuntimeAuthorityTransitionPublishesTerminalBlockerForGenericCarrierAdmissionNoPath(t *testing.T) {
	var responded string
	var blockReason string
	var terminalPayload map[string]any
	var saved RuntimeScratchState
	var methods []string
	const taskID = "task-1781703325719662100-4df0c277"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedGenericAuthorityTransitionHydrationBundle(taskID, "project-signal01-lua-capability"))
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != taskID {
				t.Fatalf("expected generic authority task runtime_switch_task gate, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-06-17T13:51:00Z",
				"workspace_id":                 "ws",
				"agent_id":                     "zeta",
				"has_work":                     false,
				"reason":                       "profile_gate",
				"trigger":                      "runtime_switch_task",
				"profile_gate_summary":         "Agent profile allows autonomous work selection.",
				"autonomous_execution_allowed": true,
			})
		case "agent.task.block":
			if got := rpcString(req.Params, "task_id"); got != taskID {
				t.Fatalf("expected generic authority task block, got %+v", req.Params)
			}
			blockReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-generic-authority-terminal"})
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil && payload["delegation_state"] == "terminal_blocker" {
				terminalPayload = payload
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-generic-authority-terminal"})
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("admission no-path must not claim generic authority carrier, method=%s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "zeta",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"` + taskID + `","prompt":"Own ` + taskID + ` as the authority-transition carrier."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-generic-authority-no-path",
		WorkspaceID: "ws",
		FromAgentID: "epsilon",
		ToAgentID:   "zeta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	for _, want := range []string{"Cannot accept authority transition task " + taskID, "typed terminal blocker", authorityTransitionTerminalBlockerSchema, "no_completion_without_project_role_assign_receipt"} {
		if !strings.Contains(responded, want) {
			t.Fatalf("expected response to contain %q, got %q", want, responded)
		}
	}
	if !strings.Contains(blockReason, authorityTransitionTerminalBlockerSchema) ||
		!strings.Contains(blockReason, "required_transition=project_role_assign") {
		t.Fatalf("expected schema-stamped terminal blocker reason, got %q", blockReason)
	}
	if terminalPayload == nil ||
		terminalPayload["task_id"] != taskID ||
		terminalPayload["schema"] != authorityTransitionTerminalBlockerSchema ||
		terminalPayload["required_transition"] != projectRoleScopeAuthorityTransitionTool {
		t.Fatalf("expected terminal blocker payload for generic authority carrier, got %#v methods=%#v", terminalPayload, methods)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "" || saved.ActiveSessionID != "" {
		t.Fatalf("terminal blocker should clear generic authority scratch, got %+v", saved)
	}
	if containsAll(methods, []string{"agent.task.claim"}) || containsAll(methods, []string{"agent.session.start"}) {
		t.Fatalf("terminal blocker path must not create task ownership, methods=%#v", methods)
	}
}

func TestRuntimeDelegatedSideEffectSuccessorPublishesTerminalBlockerWhenClaimAdmissionHasNoPath(t *testing.T) {
	const taskID = "task-side-effect-1837f52144"
	var responded string
	var blockReason string
	var terminalPayload map[string]any
	var declinedPayload map[string]any
	var saved RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedSideEffectSuccessorHydrationBundle(taskID, "project-lua", "", ""))
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != taskID {
				t.Fatalf("expected side-effect successor runtime_switch_task gate, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-06-18T02:57:49Z",
				"workspace_id":                 "ws",
				"agent_id":                     "zeta",
				"has_work":                     false,
				"reason":                       "project_targeted_delegation_required",
				"trigger":                      "runtime_switch_task",
				"autonomous_execution_allowed": true,
				"packet": map[string]any{
					"work_type":          "project_targeted_delegation_required",
					"coordination_state": "side_effect_resolution_successor",
					"gate": map[string]any{
						"summary": "side-effect successor has no fresh claimable role/scope path for this agent",
					},
				},
			})
		case "agent.task.block":
			if got := rpcString(req.Params, "task_id"); got != taskID {
				t.Fatalf("expected side-effect successor block, got %+v", req.Params)
			}
			blockReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-side-effect-terminal"})
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil {
				switch payload["delegation_state"] {
				case "terminal_blocker":
					terminalPayload = payload
				case "declined":
					declinedPayload = payload
				}
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-side-effect-terminal"})
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("admission no-path must not claim side-effect successor, method=%s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "zeta",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and resolve the Lua side-effect successor."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-side-effect-no-path",
		WorkspaceID: "ws",
		FromAgentID: "delta",
		ToAgentID:   "zeta",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	for _, want := range []string{"Cannot accept delegated task " + taskID, "typed terminal blocker", runtimeSwitchTerminalBlockerSchema, "no_completion_without_side_effect_resolution_receipt_or_claim_admitted_successor"} {
		if !strings.Contains(responded, want) {
			t.Fatalf("expected response to contain %q, got %q", want, responded)
		}
	}
	if strings.Contains(responded, "Queued runtime_switch_task") || strings.Contains(responded, "Delegated task claim_admitted") {
		t.Fatalf("non-materialized side-effect successor must not ack queued/claimed, got %q", responded)
	}
	if !strings.Contains(blockReason, runtimeSwitchTerminalBlockerSchema) ||
		!strings.Contains(blockReason, "carrier_kind=side_effect_resolution_successor") ||
		!strings.Contains(blockReason, "blocker_kind=no_fresh_claimable_side_effect_successor_path") {
		t.Fatalf("expected schema-stamped side-effect terminal blocker reason, got %q", blockReason)
	}
	if terminalPayload == nil ||
		terminalPayload["task_id"] != taskID ||
		terminalPayload["schema"] != runtimeSwitchTerminalBlockerSchema ||
		terminalPayload["carrier_kind"] != "side_effect_resolution_successor" ||
		terminalPayload["terminal_blocker"] != true {
		t.Fatalf("expected side-effect terminal blocker payload, got %#v methods=%#v", terminalPayload, methods)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "" || saved.ActiveSessionID != "" {
		t.Fatalf("terminal blocker should clear side-effect switch scratch, got %+v", saved)
	}
	if declinedPayload == nil || declinedPayload["delegation_state"] != "declined" || declinedPayload["request_kind"] != agentRequestKindDelegateTask {
		t.Fatalf("expected declined coordination payload after terminal blocker, got %#v methods=%#v", declinedPayload, methods)
	}
	if containsAll(methods, []string{"agent.task.claim"}) || containsAll(methods, []string{"agent.session.start"}) {
		t.Fatalf("terminal blocker path must not create side-effect ownership, methods=%#v", methods)
	}
}

func TestRuntimeDelegatedSideEffectSuccessorClosesUnclaimedCarrierWhenBlockNeedsClaim(t *testing.T) {
	const taskID = "task-side-effect-46a2109a22"
	var responded string
	var blockReason string
	var closeReason string
	var closeResolution string
	var terminalPayload map[string]any
	var saved RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedSideEffectSuccessorHydrationBundle(taskID, "project-lua", "", "")
			root := bundle["bundle"].(map[string]any)
			requirements := map[string]any{
				"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
				"admission_kind":                  "abpc_recovery_action",
				"abpc_task_class":                 "side_effect_foundation",
				"action_kind":                     "split_foundation_bucket",
				"successor_key":                   "abpc-resolution-successor:r23",
				"resolution_saga_key":             "abpc-resolution-saga:r23",
				"decision":                        "split_tension",
				"integration_status":              "foundation_requested",
				"project_id":                      "project-lua",
				"branch_id":                       "projbranch-r23-delta",
				"branch_name":                     "agent/delta/lua-lexer-parser-front",
				"owner_agent_id":                  "delta",
				"target_agent_id":                 "alpha",
				"active_task_id":                  "task-signal01-lua-root-capability",
				"classification_task_id":          "task-side-effect-classify-r23",
				"parent_classifier_task_id":       "task-side-effect-classify-r23",
				"side_effect_refs":                []string{"side-effect:r23:lexer-parser-front"},
				"dirty_paths":                     []string{"internal/lexer/lexer_test.go", "internal/token/token.go"},
				"path_bucket":                     []string{"internal/lexer/**", "internal/token/**"},
				"write_scope_hints":               []string{"internal/lexer/lexer_test.go", "internal/token/token.go"},
				"preserve_write_scope_hints":      true,
				"write_scope_hints_authoritative": true,
				"next_transition":                 "create_foundation_lane",
			}
			raw, _ := json.Marshal(requirements)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["title"] = "Resolve Lua lexer/parser foundation side-effect bucket"
				task["description"] = "Side Effect Resolution Path for the Lua lexer/parser foundation bucket."
				task["task_kind"] = "EXECUTION"
				task["task_template"] = "generic"
				task["project_lane"] = "implementation"
				task["tags"] = []any{"side-effect-resolution", "foundation-effect", "operational-boundary", "abpc"}
				task["task_requirements_json"] = string(raw)
				task["write_scope_hints"] = []any{"internal/lexer/lexer_test.go", "internal/token/token.go"}
			}
			writeRPCResult(w, req, bundle)
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != taskID {
				t.Fatalf("expected side-effect successor runtime_switch_task gate, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-06-18T05:07:06Z",
				"workspace_id":                 "ws",
				"agent_id":                     "alpha",
				"has_work":                     false,
				"reason":                       "project_claim_scope_busy",
				"trigger":                      "runtime_switch_task",
				"autonomous_execution_allowed": true,
				"packet": map[string]any{
					"work_type":          "project_targeted_delegation_required",
					"coordination_state": "side_effect_resolution_successor",
					"gate": map[string]any{
						"summary": "agent.work.next rejected runtime_switch_task for task-side-effect-46a2109a22 before acceptance: project claim is busy",
					},
				},
			})
		case "agent.task.block":
			if got := rpcString(req.Params, "task_id"); got != taskID {
				t.Fatalf("expected side-effect successor block, got %+v", req.Params)
			}
			blockReason = rpcString(req.Params, "reason")
			writeRPCError(w, req, -32000, "task claim not found")
		case "task.close":
			if got := rpcString(req.Params, "task_id"); got != taskID {
				t.Fatalf("expected side-effect successor close, got %+v", req.Params)
			}
			closeResolution = rpcString(req.Params, "resolution")
			closeReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, map[string]any{"task_id": taskID, "status": "CANCELLED"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-r23-side-effect-terminal"})
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil && payload["delegation_state"] == "terminal_blocker" {
				terminalPayload = payload
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-r23-side-effect-terminal"})
		case "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("unclaimed terminal blocker path must not create ownership, method=%s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"delegate_task","task_id":"` + taskID + `","prompt":"Please claim ` + taskID + ` and split the Lua foundation side-effect bucket."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-r23-side-effect-no-path",
		WorkspaceID: "ws",
		FromAgentID: "delta",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	for _, want := range []string{"Cannot accept delegated task " + taskID, "typed terminal blocker", runtimeSwitchTerminalBlockerSchema, "no_fresh_claimable_side_effect_successor_path"} {
		if !strings.Contains(responded, want) {
			t.Fatalf("expected response to contain %q, got %q", want, responded)
		}
	}
	for _, reason := range []string{blockReason, closeReason} {
		if !strings.Contains(reason, runtimeSwitchTerminalBlockerSchema) ||
			!strings.Contains(reason, "carrier_kind=side_effect_resolution_successor") ||
			!strings.Contains(reason, "required_transition=side_effect_resolution_followup:split_foundation_bucket") {
			t.Fatalf("expected schema-stamped R23 side-effect terminal blocker reason, block=%q close=%q", blockReason, closeReason)
		}
	}
	if closeResolution != "CANCELLED" {
		t.Fatalf("expected unclaimed terminal carrier to close as CANCELLED, got %q", closeResolution)
	}
	if stringIndex(methods, "agent.task.block") < 0 || stringIndex(methods, "task.close") < 0 || stringIndex(methods, "agent.task.block") > stringIndex(methods, "task.close") {
		t.Fatalf("expected missing-claim block to fall back to task.close, methods=%#v", methods)
	}
	if terminalPayload == nil ||
		terminalPayload["task_id"] != taskID ||
		terminalPayload["carrier_kind"] != "side_effect_resolution_successor" ||
		terminalPayload["required_transition"] != "side_effect_resolution_followup:split_foundation_bucket" {
		t.Fatalf("expected R23 side-effect terminal payload, got %#v methods=%#v", terminalPayload, methods)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.ActiveTaskID != "" || saved.ActiveSessionID != "" {
		t.Fatalf("terminal blocker should clear side-effect switch scratch, got %+v", saved)
	}
}

func TestRuntimeSwitchTerminalBlockerDoesNotCloseCarrierForNonClaimBlockError(t *testing.T) {
	const taskID = "task-side-effect-block-forbidden"
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedSideEffectSuccessorHydrationBundle(taskID, "project-lua", "", ""))
		case "agent.task.block":
			writeRPCError(w, req, -32000, "permission denied")
		case "task.close":
			t.Fatalf("non-claim block errors must not close runtime switch carriers")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "zeta",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	reason, ok, err := runtime.publishRuntimeSwitchTerminalBlocker(context.Background(), delegatedAgentTaskRequest{
		TaskID:      taskID,
		RequestKind: agentRequestKindDelegateTask,
		Prompt:      "claim side-effect successor",
	}, "agent.work.next rejected runtime_switch_task for task "+taskID+" before acceptance: no path")
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected non-claim block error to propagate, reason=%q ok=%t err=%v", reason, ok, err)
	}
	if ok || reason != "" {
		t.Fatalf("non-claim block error must not publish a terminal blocker, ok=%t reason=%q", ok, reason)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.task.block" {
		t.Fatalf("expected hydrate then block only, got %s", got)
	}
}

func TestRuntimeAuthorityTransitionTerminalBlockerDoesNotCloseCarrierForNonClaimBlockError(t *testing.T) {
	const taskID = "task-role-scope-block-forbidden"
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedRoleScopeHydrationBundle(taskID, "project-lua"))
		case "agent.task.block":
			writeRPCError(w, req, -32000, "permission denied")
		case "task.close":
			t.Fatalf("non-claim block errors must not close authority transition carriers")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "zeta",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	reason, ok, err := runtime.publishAuthorityTransitionTerminalBlocker(context.Background(), delegatedAgentTaskRequest{
		TaskID:      taskID,
		RequestKind: agentRequestKindAuthorityTransition,
		Prompt:      "claim authority transition",
	}, "agent.work.next rejected runtime_switch_task for task "+taskID+" before acceptance: no path")
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected non-claim block error to propagate, reason=%q ok=%v err=%v methods=%#v", reason, ok, err, methods)
	}
	if ok || reason != "" {
		t.Fatalf("non-claim block error must not publish authority terminal blocker, reason=%q ok=%v", reason, ok)
	}
	if stringIndex(methods, "task.close") >= 0 {
		t.Fatalf("non-claim block error must not close authority carrier, methods=%#v", methods)
	}
}

func TestRuntimeSwitchTerminalBlockerDoesNotBlockLiveClaimedSideEffectSuccessor(t *testing.T) {
	const taskID = "task-side-effect-live-claim"
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedSideEffectSuccessorHydrationBundle(taskID, "project-lua", "zeta", "CLAIMED"))
		case "agent.task.block", "agent.state.set", "agent.update.post":
			t.Fatalf("live claimed side-effect successor must not publish terminal blocker via %s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "zeta",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	reason, ok, err := runtime.publishRuntimeSwitchTerminalBlocker(context.Background(), delegatedAgentTaskRequest{
		TaskID:      taskID,
		RequestKind: agentRequestKindDelegateTask,
		Prompt:      "claim side-effect successor",
	}, "agent.work.next rejected runtime_switch_task for task "+taskID+" before acceptance: no path")
	if err != nil {
		t.Fatalf("publishRuntimeSwitchTerminalBlocker() error = %v", err)
	}
	if ok || reason != "" {
		t.Fatalf("live claimed side-effect successor must remain live, ok=%t reason=%q methods=%#v", ok, reason, methods)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate" {
		t.Fatalf("expected only hydrate for live claimed successor, got %s", got)
	}
}

func TestRuntimeAuthorityTransitionPublishesTerminalBlockerForUnstampedRoleScopeCarrier(t *testing.T) {
	var blockReason string
	var terminalPayload map[string]any
	var saved RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedUnstampedRoleScopeHydrationBundle("task-role-scope-cli-publication-ownership", "project-lua"))
		case "agent.task.block":
			if got := rpcString(req.Params, "task_id"); got != "task-role-scope-cli-publication-ownership" {
				t.Fatalf("expected unstamped authority carrier block, got %+v", req.Params)
			}
			blockReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-unstamped-authority-terminal"})
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil {
				terminalPayload = payload
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-unstamped-authority-terminal"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			ActiveTaskID:       "task-role-scope-cli-publication-ownership",
			ActiveSessionID:    "session-authority",
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-role-scope-cli-publication-ownership",
		},
	}

	reason, ok, err := runtime.publishAuthorityTransitionTerminalBlocker(context.Background(), delegatedAgentTaskRequest{
		TaskID:      "task-role-scope-cli-publication-ownership",
		RequestKind: agentRequestKindAuthorityTransition,
	}, "agent.work.next rejected runtime_switch_task before acceptance")
	if err != nil {
		t.Fatalf("publishAuthorityTransitionTerminalBlocker() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected unstamped dedicated role-scope carrier to publish terminal blocker; methods=%#v", methods)
	}
	if reason != blockReason || !strings.Contains(blockReason, "authority_transition_terminal_blocker.v1") ||
		!strings.Contains(blockReason, "no_completion_without_project_role_assign_receipt") {
		t.Fatalf("expected typed terminal blocker reason, reason=%q block=%q", reason, blockReason)
	}
	if terminalPayload == nil || terminalPayload["schema"] != "authority_transition_terminal_blocker.v1" ||
		terminalPayload["task_id"] != "task-role-scope-cli-publication-ownership" {
		t.Fatalf("expected terminal blocker update payload, got %#v", terminalPayload)
	}
	if saved.ActiveTaskID != "" || saved.ActiveSessionID != "" || saved.PendingTrigger != "" || saved.PendingTriggerTask != "" {
		t.Fatalf("terminal blocker should clear active authority scratch, got %+v", saved)
	}
}

func TestRuntimeAuthorityTransitionDeclineKeepsTypedRepairRoute(t *testing.T) {
	var responded string
	var declinedPayload map[string]any
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			bundle := delegatedRoleScopeHydrationBundle("task-role-scope-beta", "project-rq")
			root := bundle["bundle"].(map[string]any)
			for _, key := range []string{"workspace_task", "task"} {
				task := root[key].(map[string]any)
				task["status"] = "RUNNING"
				task["claim_agent_id"] = "beta"
				task["claim_status"] = "CLAIMED"
				task["claim_expires_at"] = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
			}
			writeRPCResult(w, req, bundle)
		case "agent.respond":
			responded = rpcString(req.Params, "response")
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority-decline"})
		case "agent.update.post":
			var payload map[string]any
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err == nil && payload["delegation_state"] == "declined" {
				declinedPayload = payload
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-authority-decline"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "alpha",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	payload := `{"request_kind":"authority_transition","task_id":"task-role-scope-beta","prompt":"Perform the lead-level authority transition for task-role-scope-beta."}`
	if err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-authority-owned",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "alpha",
		Method:      "model.ask",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}
	for _, want := range []string{"Cannot accept authority transition task task-role-scope-beta", "already claimed by beta", "dedicated authority task"} {
		if !strings.Contains(responded, want) {
			t.Fatalf("expected authority decline response to contain %q, got %q", want, responded)
		}
	}
	if strings.Contains(responded, "Cannot accept delegated task") || strings.Contains(responded, "Queued request_resume") {
		t.Fatalf("authority decline was downgraded, got %q", responded)
	}
	if declinedPayload == nil {
		t.Fatalf("expected declined coordination update, methods=%#v", methods)
	}
	if declinedPayload["request_kind"] != agentRequestKindAuthorityTransition {
		t.Fatalf("decline update lost authority request_kind: %+v", declinedPayload)
	}
	if declinedPayload["preferred_transition"] == "request_resume" {
		t.Fatalf("authority decline must not route to request_resume: %+v", declinedPayload)
	}
	if declinedPayload["suggested_route"] != "create_or_reuse_dedicated_authority_transition_task" ||
		declinedPayload["preferred_transition"] != "authority_transition_task_claim_repair" {
		t.Fatalf("unexpected authority decline route: %+v", declinedPayload)
	}
	if containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("declined authority task must not queue runtime_switch_task, methods=%#v", methods)
	}
}

func runtimeWithActiveProjectUmbrella(serverURL string) *Runtime {
	return &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "alpha",
		},
		client:           NewRhizomeClient(serverURL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "root-icon-sprite-forge-cleanroom-20260512",
			Title:        "Clean-room project root",
			Status:       "RUNNING",
			TaskKind:     "COORDINATION",
			TaskTemplate: "project",
			ProjectID:    "project-icon-sprite-forge",
			ProjectLane:  "strategy",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-root",
			TaskID:    "root-icon-sprite-forge-cleanroom-20260512",
			Status:    "ACTIVE",
		},
	}
}

func stringIndex(values []string, target string) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
}

func delegatedRoleScopeHydrationBundle(taskID, projectID string) map[string]any {
	bundle := delegatedProjectHydrationBundle(taskID, "PENDING", "", "", projectID, "coordination")
	root := bundle["bundle"].(map[string]any)
	for _, key := range []string{"workspace_task", "task"} {
		task := root[key].(map[string]any)
		task["title"] = "Resolve project role/scope request for gamma"
		task["description"] = "# Strategic Lead Role/Scope Request\n\n- requested_role_type: IMPLEMENTER\n- requested_write_scope_json: {\"paths\":[\"src/**\",\"tests/**\"]}\n\n## Required Lead Action\nRun project_role_assign."
		task["task_kind"] = "COORDINATION"
		task["task_template"] = "generic"
		task["tags"] = []any{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"}
		task["task_requirements_json"] = `{"schema":"project_role_scope_authority_transition.v1","project_id":"` + projectID + `","target_agent_id":"gamma","role_type":"IMPLEMENTER","required_transition":"project_role_assign"}`
	}
	return bundle
}

func delegatedUnstampedRoleScopeHydrationBundle(taskID, projectID string) map[string]any {
	bundle := delegatedProjectHydrationBundle(taskID, "PENDING", "", "", projectID, "coordination")
	root := bundle["bundle"].(map[string]any)
	for _, key := range []string{"workspace_task", "task"} {
		task := root[key].(map[string]any)
		task["title"] = "Role-scope repair for overlapping CLI publication ownership"
		task["description"] = "Create the canonical coordination lane to resolve overlapping write ownership. This task exists as the authority-transition carrier."
		task["task_kind"] = "COORDINATION"
		task["task_template"] = "generic"
		task["tags"] = []any{"cli", "coordination", "ownership", "publication"}
		task["task_requirements_json"] = `{"schema":"task_requirements.v1","write_scope_hints":["README.md","internal/runner/**","scripts/**","testdata/smoke/**"]}`
	}
	return bundle
}

func delegatedGenericAuthorityTransitionHydrationBundle(taskID, projectID string) map[string]any {
	bundle := delegatedProjectHydrationBundle(taskID, "PENDING", "", "", projectID, "coordination")
	root := bundle["bundle"].(map[string]any)
	for _, key := range []string{"workspace_task", "task"} {
		task := root[key].(map[string]any)
		task["title"] = "Repair task-role-scope authority transition for CLI publication lane"
		task["description"] = "Dedicated authority-transition carrier for publication ownership repair. It must either publish a durable project_role_assign receipt or a typed terminal blocker."
		task["task_kind"] = "COORDINATION"
		task["task_template"] = "generic"
		task["tags"] = []any{"cli", "publication", "claim", "ownership", "authority-transition", "coordination"}
		task["task_requirements_json"] = `{}`
	}
	return bundle
}

func delegatedSideEffectSuccessorHydrationBundle(taskID, projectID, claimAgentID, claimStatus string) map[string]any {
	bundle := delegatedProjectHydrationBundle(taskID, "PENDING", claimAgentID, claimStatus, projectID, "implementation")
	root := bundle["bundle"].(map[string]any)
	requirements := map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_verification",
		"action_kind":                     "verify_bucket",
		"successor_key":                   "abpc-resolution-successor:r18",
		"resolution_saga_key":             "abpc-resolution-saga:r18",
		"decision":                        "request_verification",
		"integration_status":              "verification_requested",
		"project_id":                      projectID,
		"branch_id":                       "projbranch-lua-delta",
		"branch_name":                     "agent/delta/lua-runtime",
		"owner_agent_id":                  "delta",
		"target_agent_id":                 "zeta",
		"active_task_id":                  "task-signal01-lua-runtime-values",
		"classification_task_id":          "task-side-effect-classify-r18",
		"parent_classifier_task_id":       "task-side-effect-classify-r18",
		"side_effect_refs":                []string{"side-effect:r18:lua-runtime"},
		"dirty_paths":                     []string{"internal/runtime/**"},
		"path_bucket":                     []string{"internal/runtime/**"},
		"write_scope_hints":               []string{"internal/runtime/**"},
		"preserve_write_scope_hints":      true,
		"write_scope_hints_authoritative": true,
		"next_transition":                 "route_to_verifier",
	}
	raw, _ := json.Marshal(requirements)
	for _, key := range []string{"workspace_task", "task"} {
		task := root[key].(map[string]any)
		task["title"] = "Verify Lua runtime side-effect bucket"
		task["description"] = "Side Effect Resolution Path for the Lua runtime branch. Verify whether the materialized side effect is valid before the original lane resumes."
		task["task_kind"] = "EXECUTION"
		task["task_template"] = "generic"
		task["project_lane"] = "implementation"
		task["tags"] = []any{"side-effect", "abpc", "verification", "runtime-switch-carrier"}
		task["task_requirements_json"] = string(raw)
		task["write_scope_hints"] = []any{"internal/runtime/**"}
	}
	return bundle
}

func delegatedHydrationBundle(taskID, status, claimAgentID, claimStatus string) map[string]any {
	task := map[string]any{
		"task_id":       taskID,
		"title":         "Delegated task",
		"description":   "Task for delegated request tests",
		"owner_user_id": "owner-1",
		"priority":      "high",
		"status":        status,
		"task_kind":     "EXECUTION",
		"task_template": "generic",
		"linked_by":     "alpha",
		"linked_at":     "2026-05-07T00:00:00Z",
	}
	if strings.TrimSpace(claimAgentID) != "" {
		task["claim_agent_id"] = strings.TrimSpace(claimAgentID)
	}
	if strings.TrimSpace(claimStatus) != "" {
		task["claim_status"] = strings.TrimSpace(claimStatus)
	}
	return map[string]any{
		"bundle": map[string]any{
			"generated_at":   "2026-05-07T00:00:00Z",
			"workspace_task": task,
			"task":           task,
			"docs":           []any{},
			"task_links":     []any{},
			"related_tasks":  []any{},
			"artifacts":      []any{},
			"updates":        []any{},
		},
	}
}

func delegatedAcceptedWorkNextResult(agentID, taskID string) map[string]any {
	return map[string]any{
		"workspace_id": "ws",
		"agent_id":     strings.TrimSpace(agentID),
		"has_work":     true,
		"reason":       "triggered_task",
		"trigger":      "runtime_switch_task",
		"task": map[string]any{
			"task_id":       strings.TrimSpace(taskID),
			"title":         "Delegated task",
			"description":   "Task accepted by runtime_switch_task preflight",
			"owner_user_id": "owner-1",
			"priority":      "high",
			"status":        "PENDING",
			"task_kind":     "EXECUTION",
			"task_template": "generic",
		},
	}
}

func delegatedStructuredPlanningEvidenceHydrationBundle(taskID, projectID string) map[string]any {
	bundle := delegatedProjectHydrationBundle(taskID, "PENDING", "", "", projectID, "implementation")
	root := bundle["bundle"].(map[string]any)
	for _, key := range []string{"workspace_task", "task"} {
		applyDelegatedStructuredPlanningEvidenceFields(root[key].(map[string]any), projectID)
	}
	return bundle
}

func delegatedStructuredPlanningEvidenceWorkNextResult(agentID, taskID, projectID string) map[string]any {
	result := delegatedAcceptedWorkNextResult(agentID, taskID)
	task := result["task"].(map[string]any)
	applyDelegatedStructuredPlanningEvidenceFields(task, projectID)
	return result
}

func applyDelegatedStructuredPlanningEvidenceFields(task map[string]any, projectID string) {
	task["project_id"] = strings.TrimSpace(projectID)
	task["project_lane"] = "implementation"
	task["requires_project_gate"] = true
	task["title"] = "Materialize product contract and plan review"
	task["description"] = "Create project product_contract and plan_review docs, then compare shipped evidence against them."
	task["tags"] = []any{"docs", "review", "spec-fidelity"}
	task["task_requirements_json"] = `{"schema":"task_requirements.v1","preferred_tools":["workspace_doc_get","project_patch_queue_list"],"required_work_modes":["implementation","review","synthesis"]}`
	task["write_scope_hints"] = []any{}
}

func handleDelegatedTaskMaterializationRPC(t *testing.T, w http.ResponseWriter, req rpcRequest, agentID, taskID string) bool {
	t.Helper()
	agentID = strings.TrimSpace(agentID)
	taskID = strings.TrimSpace(taskID)
	switch req.Method {
	case "agent.work.next":
		writeRPCResult(w, req, delegatedAcceptedWorkNextResult(agentID, taskID))
	case "agent.task.claim":
		if got := rpcString(req.Params, "task_id"); got != taskID {
			t.Fatalf("expected claim task %q, got %q", taskID, got)
		}
		writeRPCResult(w, req, nil)
	case "agent.session.start":
		if got := rpcString(req.Params, "task_id"); got != taskID {
			t.Fatalf("expected session task %q, got %q", taskID, got)
		}
		writeRPCResult(w, req, map[string]any{"state": map[string]any{
			"session_id":          rpcString(req.Params, "session_id"),
			"workspace_id":        "ws",
			"agent_id":            agentID,
			"task_id":             taskID,
			"status":              "ACTIVE",
			"summary":             rpcString(req.Params, "summary"),
			"updated_at":          "2026-06-05T12:00:00Z",
			"started_at":          "2026-06-05T12:00:00Z",
			"keep_session_active": true,
		}})
	case "workspace.execution.run.write":
		if got := rpcString(req.Params, "task_id"); got != taskID {
			t.Fatalf("expected execution run task %q, got %q", taskID, got)
		}
		writeRPCResult(w, req, map[string]any{"run": map[string]any{
			"run_id":       firstNonEmpty(rpcString(req.Params, "run_id"), "run-"+taskID),
			"workspace_id": "ws",
			"agent_id":     agentID,
			"task_id":      taskID,
			"session_id":   rpcString(req.Params, "session_id"),
			"status":       "ACTIVE",
		}})
	default:
		return false
	}
	return true
}

func decodeRuntimeScratchStateRPC(t *testing.T, req rpcRequest) RuntimeScratchState {
	t.Helper()
	var state RuntimeScratchState
	if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
		t.Fatalf("decode scratch: %v", err)
	}
	return state
}

func delegatedProjectHydrationBundle(taskID, status, claimAgentID, claimStatus, projectID, projectLane string) map[string]any {
	bundle := delegatedHydrationBundle(taskID, status, claimAgentID, claimStatus)
	root := bundle["bundle"].(map[string]any)
	for _, key := range []string{"workspace_task", "task"} {
		task := root[key].(map[string]any)
		task["project_id"] = projectID
		task["project_lane"] = projectLane
		task["requires_project_gate"] = false
	}
	return bundle
}

func delegatedProjectCoordination(roles []map[string]any) map[string]any {
	return map[string]any{
		"coordination": map[string]any{
			"snapshot_at":           "2026-05-07T00:00:00Z",
			"coordination_version":  "test",
			"project":               map[string]any{"project_id": "project-alpha", "workspace_id": "ws", "status": "ACTIVE"},
			"profile":               map[string]any{"project_id": "project-alpha", "workspace_id": "ws", "current_phase": "IMPLEMENTATION"},
			"roles":                 roles,
			"repositories":          []any{},
			"tasks":                 []any{},
			"open_task_count":       1,
			"task_counts_by_lane":   map[string]any{},
			"task_counts_by_status": map[string]any{},
		},
	}
}

func delegatedOwnerBoundProjectCoordination(ownerAgentID string) map[string]any {
	result := delegatedProjectCoordination(nil)
	coordination := result["coordination"].(map[string]any)
	coordination["branches"] = []any{
		map[string]any{
			"branch_id":      "branch-gamma",
			"workspace_id":   "ws",
			"project_id":     "project-alpha",
			"repo_id":        "repo-main",
			"agent_id":       strings.TrimSpace(ownerAgentID),
			"branch_name":    "agent/gamma/owner-bound-submit",
			"branch_kind":    "feature",
			"head_sha":       strings.Repeat("b", 40),
			"status":         "READY_FOR_REVIEW",
			"review_doc_key": "project.project-alpha.branch.branch-gamma.review",
			"created_at":     "2026-05-07T00:00:00Z",
			"updated_at":     "2026-05-07T00:00:00Z",
		},
	}
	return result
}

func delegatedPatchQueueRevisionHydrationBundle(taskID, projectID, ownerAgentID string) map[string]any {
	bundle := delegatedProjectHydrationBundle(taskID, "PENDING", "", "", projectID, "implementation")
	root := bundle["bundle"].(map[string]any)
	for _, key := range []string{"workspace_task", "task"} {
		task := root[key].(map[string]any)
		task["title"] = "Revise blocked patch queue candidate"
		task["description"] = "Patch queue decision follow-up.\n\n- queue_id: patchq-project-alpha\n- item_id: patchitem-blocked\n- branch_id: branch-gamma\n- state: BLOCKED\n- head_sha: " + strings.Repeat("b", 40) + "\n\nRequired work:\n- Revise or unblock the candidate according to the decision evidence.\n- Publish new implementation/review evidence before resubmitting to the patch queue."
		task["tags"] = []any{
			"project",
			"patch-queue",
			"revision",
			"blocked",
			"owner-bound",
			"owner-bound-kind:patch_queue_revision",
			"owner-branch:branch-gamma",
			"owner-agent:" + strings.TrimSpace(ownerAgentID),
			"required-agent:" + strings.TrimSpace(ownerAgentID),
			"queue:patchq-project-alpha",
			"item:patchitem-blocked",
		}
	}
	return bundle
}

func delegatedPatchQueueRevisionRequirementsOnlyHydrationBundle(taskID, projectID, ownerAgentID string) map[string]any {
	bundle := delegatedProjectHydrationBundle(taskID, "PENDING", "", "", projectID, "implementation")
	root := bundle["bundle"].(map[string]any)
	requirements := map[string]any{
		"branch_id":                       "branch-gamma",
		"decision":                        "BLOCKED",
		"decisive_path_kind":              "patch_queue_revision_followup",
		"head_sha":                        strings.Repeat("b", 40),
		"item_id":                         "patchitem-blocked",
		"patch_queue_task_kind":           "revision",
		"project_id":                      projectID,
		"queue_id":                        "patchq-project-alpha",
		"required_terminal_tool":          "project_patch_queue_submit",
		"required_transition":             "project_patch_queue_revision_commit_review_submit",
		"required_tool_sequence":          []string{"project_branch_commit", "project_branch_review_ready", "project_patch_queue_submit"},
		"historical_source_branch_role":   "read_only_defeated_source_branch_evidence",
		"live_repair_branch_required":     true,
		"candidate_pathset_role":          "historical_changed_path_evidence_not_claim_scope",
		"required_first_publication_tool": "project_branch_commit",
	}
	raw, _ := json.Marshal(requirements)
	for _, key := range []string{"workspace_task", "task"} {
		task := root[key].(map[string]any)
		task["owner_user_id"] = strings.TrimSpace(ownerAgentID)
		task["title"] = "Revise patch queue candidate for " + projectID
		task["description"] = "Continue a terminal patch queue decision through a visible task instead of private coordination chatter.\n\nProject: " + projectID + "\nPatch queue: patchq-project-alpha/patchitem-blocked\nBranch ID: branch-gamma\nHead SHA: " + strings.Repeat("b", 40) + "\nDecision: BLOCKED\nFollow-up kind: revision\n\nRequired transition:\nRevise the referenced branch/head or create a successor candidate tied to this queue/item."
		task["tags"] = []any{"project", "patch_queue", "revision", "decision_continuation"}
		task["task_requirements_json"] = string(raw)
	}
	return bundle
}

func delegatedPatchQueueRevisionCoordination(ownerAgentID string) map[string]any {
	result := delegatedOwnerBoundProjectCoordination(ownerAgentID)
	coordination := result["coordination"].(map[string]any)
	coordination["patch_queue_items"] = []any{
		map[string]any{
			"queue_id":         "patchq-project-alpha",
			"item_id":          "patchitem-blocked",
			"workspace_id":     "ws",
			"project_id":       "project-alpha",
			"repo_id":          "repo-main",
			"branch_id":        "branch-gamma",
			"review_doc_key":   "project.project-alpha.branch.branch-gamma.review",
			"decision_doc_key": "project.project-alpha.patchq.patchitem-blocked.decision",
			"state":            "BLOCKED",
			"pathset":          []string{"src/**"},
			"base_ref":         "main",
			"base_sha":         strings.Repeat("a", 40),
			"head_sha":         strings.Repeat("b", 40),
			"updated_at":       "2026-05-17T22:00:00Z",
			"decided_at":       "2026-05-17T22:00:00Z",
			"decided_by":       "epsilon",
			"decision_summary": "Blocked pending product completeness revision and fresh browser evidence.",
		},
	}
	return result
}

func delegatedPatchQueueRevisionCoordinationWithAcceptedSameHead(ownerAgentID string) map[string]any {
	result := delegatedPatchQueueRevisionCoordination(ownerAgentID)
	coordination := result["coordination"].(map[string]any)
	items := coordination["patch_queue_items"].([]any)
	blocked := items[0].(map[string]any)
	accepted := map[string]any{}
	for key, value := range blocked {
		accepted[key] = value
	}
	accepted["item_id"] = "patchitem-accepted"
	accepted["state"] = "ACCEPTED"
	accepted["decision_doc_key"] = "project.project-alpha.patchq.patchitem-accepted.decision"
	accepted["decision_summary"] = "Earlier accepted queue item for the same branch head."
	coordination["patch_queue_items"] = append([]any{accepted}, items...)
	return result
}

func TestRuntimeDelegatedSwitchDependencyBlockerClearsPendingTrigger(t *testing.T) {
	var methods []string
	var payload map[string]any
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			if rpcString(req.Params, "trigger") != "runtime_switch_task" || rpcString(req.Params, "candidate_task_id") != "task-integrate" {
				t.Fatalf("expected delegated switch hints, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at":                 "2026-05-28T00:00:00Z",
				"workspace_id":                 "ws",
				"agent_id":                     "zeta",
				"has_work":                     false,
				"reason":                       "task_dependency_blocked",
				"trigger":                      "runtime_switch_task",
				"autonomous_execution_allowed": true,
				"packet": map[string]any{
					"work_type": "task_dependency_blocked",
					"context_hints": map[string]any{
						"anchor_conflict_task_ids": []string{"task-root"},
					},
				},
			})
		case "agent.update.post":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "payload_json")), &payload); err != nil {
				t.Fatalf("decode update payload: %v", err)
			}
			writeRPCResult(w, req, map[string]any{"update_id": "upd-dependency-blocked"})
		case "agent.state.set":
			saved = decodeRuntimeScratchStateRPC(t, req)
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "zeta",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-integrate",
			DocSHAs:            map[string]string{},
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("expected dependency-blocked delegated switch to stay idle, got %+v", task)
	}
	if payload["delegation_state"] != "blocked_dependency" || payload["task_id"] != "task-integrate" {
		t.Fatalf("expected dependency blocker evidence, got %+v", payload)
	}
	if saved.PendingTrigger != "" || saved.PendingTriggerTask != "" || saved.PendingTriggerSession != "" {
		t.Fatalf("dependency-blocked delegated switch must park and clear pending trigger, got %+v", saved)
	}
	if !containsAll(methods, []string{"agent.work.next", "agent.update.post", "agent.state.set"}) {
		t.Fatalf("expected work, blocker evidence, and pending clear, got %#v", methods)
	}
}
