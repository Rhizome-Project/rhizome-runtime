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

func TestCompletionCoordinationTaskRequiresExplicitProjectGate(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	task := WorkspaceTaskRecord{
		TaskID:              "task-project-gated",
		Title:               "Small implementation",
		Description:         "No heuristic review words here.",
		Priority:            "normal",
		TaskKind:            "EXECUTION",
		RequiresProjectGate: boolPtr(true),
	}
	if !runtime.completionCoordinationTaskRequiresGate(task) {
		t.Fatal("expected explicit requires_project_gate=true to force completion coordination")
	}
}

func TestCompletionCoordinationExplicitFalseDisablesHeuristicGate(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	task := WorkspaceTaskRecord{
		TaskID:              "task-broad-coordination",
		Title:               "High priority autonomous coordination review",
		Description:         "Coordinate peer review for a broad multi-agent handoff.",
		Priority:            "high",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		RequiresProjectGate: boolPtr(false),
	}
	if runtime.completionCoordinationTaskRequiresGate(task) {
		t.Fatal("expected explicit requires_project_gate=false to disable heuristic completion coordination")
	}
	task.RequiresProjectGate = nil
	if !runtime.completionCoordinationTaskRequiresGate(task) {
		t.Fatal("expected nil requires_project_gate to keep existing high-priority coordination heuristic")
	}
}

func TestCompletionCoordinationMissingReviewLanguageDoesNotApproveSoloExecution(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	for _, description := range []string{
		"Do not complete without review.",
		"No review has completed yet; peer review is required.",
		"Cannot proceed without coordination.",
		"solo_ok=false",
		"Solo allowed: false.",
		"Review not required: false.",
		"Review not required?",
		"This does not mean review not required; review is mandatory.",
		"Solo execution approved? No.",
		"A single agent must not complete this without peer review.",
		"Single agent must not complete this task.",
		"Single-agent execution is not approved.",
		"Solo allowed for this task: false.",
		"Review not required for this task: false.",
		"Single agent. Do not complete without peer review.",
		"Solo allowed for planning only; peer review before completion.",
		"Solo allowed for planning only; review must happen before completion.",
		"Single-agent execution is not permitted.",
	} {
		task := WorkspaceTaskRecord{
			TaskID:      "task-review-required",
			Title:       "High-priority handoff",
			Description: description,
			Priority:    "high",
			TaskKind:    "COORDINATION",
		}
		if !runtime.completionCoordinationTaskRequiresGate(task) {
			t.Fatalf("missing-review language must not approve solo execution: %q", description)
		}
	}
}

func TestCompletionCoordinationExplicitEnglishSoloApprovalDisablesHeuristicGate(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	for _, description := range []string{
		"Review not required; solo execution approved.",
		"Single-agent implementation.",
		"Solo allowed for this bounded task.",
		"Review not required for this task.",
		"Single-agent implementation must preserve the API.",
		"Single-agent implementation without external dependencies.",
		"Single-agent implementation of required documentation.",
		"Single-agent implementation without peer review.",
		"Single-agent implementation with no external dependencies.",
		"Single-agent implementation, not a multi-agent effort.",
	} {
		task := WorkspaceTaskRecord{
			TaskID:      "task-solo-approved",
			Title:       "High-priority implementation",
			Description: description,
			Priority:    "high",
			TaskKind:    "EXECUTION",
		}
		if runtime.completionCoordinationTaskRequiresGate(task) {
			t.Fatalf("explicit English solo approval should disable the heuristic gate: %q", description)
		}
	}
}

func TestCompletionCoordinationDeniedReviewQuestionAllowsExplicitSolo(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	task := WorkspaceTaskRecord{
		TaskID:      "task-review-question-denied",
		Title:       "High-priority implementation",
		Description: "Review required? Not for this task, solo allowed.",
		Priority:    "high",
		TaskKind:    "EXECUTION",
	}
	if runtime.completionCoordinationTaskRequiresGate(task) {
		t.Fatal("an explicitly denied review requirement must not override solo approval")
	}
}

func TestCompletionCoordinationExplicitReviewRequirementOverridesSoloLanguage(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	task := WorkspaceTaskRecord{
		TaskID:      "task-review-required",
		Title:       "Single agent",
		Description: "Peer review is required.",
		Priority:    "normal",
		TaskKind:    "EXECUTION",
	}
	if !runtime.completionCoordinationTaskRequiresGate(task) {
		t.Fatal("an explicit review requirement must override solo language")
	}
}

func TestCompletionCoordinationSkipsStrategicLeadCorrectionTask(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-abc123",
		Title:                "Adjust gamma lane authority",
		Description:          "Fresh phrasing without the legacy role/scope header.",
		Priority:             "high",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectID:            "project-demo",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign"}`,
	}
	if runtime.completionCoordinationTaskRequiresGate(task) {
		t.Fatal("expected structured strategic-lead role/scope service task to bypass automatic completion review")
	}
}

func TestCompletionCoordinationDoesNotBypassArbitraryTaggedCoordinationTask(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	task := WorkspaceTaskRecord{
		TaskID:       "task-custom-role-scope",
		Title:        "Review broad coordination plan",
		Description:  "This is a broad project coordination task with review language.",
		Priority:     "high",
		TaskKind:     "COORDINATION",
		TaskTemplate: "generic",
		ProjectID:    "project-demo",
		ProjectLane:  "coordination",
		Tags:         []string{"project-role-scope"},
	}
	if !runtime.completionCoordinationTaskRequiresGate(task) {
		t.Fatal("expected arbitrary project-role-scope tag without canonical task shape not to bypass completion coordination")
	}
}

func TestExplicitProjectGateDefersCompletionWhenNoPeerAvailable(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
	}}
	task := WorkspaceTaskRecord{
		TaskID:              "task-project-gated-no-peer",
		Title:               "Small implementation",
		Priority:            "normal",
		TaskKind:            "EXECUTION",
		RequiresProjectGate: boolPtr(true),
	}
	session := AgentSessionStateRecord{SessionID: "session-alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceCompletionCoordinationGate(context.Background(), task, session, "run-alpha", result, nil, nil)
	if err != nil {
		t.Fatalf("enforce completion coordination gate: %v", err)
	}
	if !applied {
		t.Fatal("expected explicit project gate to apply even without an eligible peer")
	}
	if normalizeOutcome(gated.Outcome) == "completed" || !strings.Contains(gated.Summary, "no peer available") {
		t.Fatalf("expected completion to be deferred without peer, got %+v", gated)
	}
}

func TestTrustFirstCompletionCoordinationGateIsAdvisory(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		CoordinationMode: CoordinationModeTrustFirst,
	}}
	task := WorkspaceTaskRecord{
		TaskID:              "task-project-gated-trust-first",
		Title:               "Small implementation",
		Priority:            "normal",
		TaskKind:            "EXECUTION",
		RequiresProjectGate: boolPtr(true),
	}
	session := AgentSessionStateRecord{SessionID: "session-alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	result := StructuredTaskResult{Outcome: "completed", Summary: "done"}

	gated, applied, err := runtime.enforceCompletionCoordinationGate(context.Background(), task, session, "run-alpha", result, nil, nil)
	if err != nil {
		t.Fatalf("enforce completion coordination gate: %v", err)
	}
	if applied {
		t.Fatalf("trust-first should keep peer coordination advisory, got applied=%v result=%+v", applied, gated)
	}
	if normalizeOutcome(gated.Outcome) != "completed" {
		t.Fatalf("trust-first should preserve completion, got %+v", gated)
	}
}

func TestProjectRootCompletionLivenessBlocksOpenImplementationTaskInTrustFirst(t *testing.T) {
	root := WorkspaceTaskRecord{
		TaskID:              "root-project-ui",
		Title:               "Autonomous coordination root",
		Description:         "Create any needed subtasks and coordinate builders.",
		Priority:            "high",
		Status:              "RUNNING",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "project",
		ProjectID:           "project-ui",
		ProjectLane:         "strategy",
		RequiresProjectGate: boolPtr(false),
	}
	session := AgentSessionStateRecord{SessionID: "session-root", TaskID: root.TaskID, Status: "ACTIVE"}
	var gateSteps int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": ProjectCoordinationRecord{
				Project: ProjectRecord{ProjectID: "project-ui", WorkspaceID: "ws-1"},
				Tasks: []WorkspaceTaskRecord{
					root,
					{TaskID: "task-build-ui", Title: "Build UI", Status: "PENDING", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
				},
				OpenTaskCount:      2,
				TaskCountsByStatus: map[string]int{"RUNNING": 1, "PENDING": 1},
			}})
		case "workspace.execution.step.write":
			gateSteps++
			if got := rpcString(req.Params, "title"); !strings.Contains(got, "project liveness") {
				t.Fatalf("unexpected gate step title %q", got)
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-project-liveness"}})
		case "agent.request":
			t.Fatalf("project liveness should block before trust-first peer advisory/request")
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		CoordinationMode: CoordinationModeTrustFirst,
	}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	t.Cleanup(func() { _ = runtime.Close() })

	gated, applied, err := runtime.enforceCompletionCoordinationGate(context.Background(), root, session, "run-root", StructuredTaskResult{Outcome: "completed", Summary: "done"}, nil, nil)
	if err != nil {
		t.Fatalf("enforceCompletionCoordinationGate() error = %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected root completion to be demoted, applied=%v result=%+v", applied, gated)
	}
	for _, want := range []string{"task-build-ui", "implementation", "Project completion deferred"} {
		if !strings.Contains(gated.Summary+"\n"+gated.Details+"\n"+gated.NextAction, want) {
			t.Fatalf("expected gated result to mention %q, got %+v", want, gated)
		}
	}
	if gateSteps != 1 {
		t.Fatalf("expected one project liveness gate step, got %d", gateSteps)
	}
}

func TestProjectRootCompletionLivenessIgnoresCoordinationDebtSiblings(t *testing.T) {
	root := WorkspaceTaskRecord{
		TaskID:      "root-project-rq",
		Status:      "RUNNING",
		TaskKind:    "COORDINATION",
		ProjectID:   "project-rq",
		ProjectLane: "strategy",
	}
	detail, blocked := projectCoordinationCompletionLivenessBlocker(root, ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-rq", WorkspaceID: "ws-1"},
		Tasks: []WorkspaceTaskRecord{
			root,
			{TaskID: "task-project-claim-repair-beta", Status: "PENDING", TaskKind: "COORDINATION", ProjectID: "project-rq", ProjectLane: "coordination", Tags: []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"}},
			{TaskID: "task-role-scope-gamma", Status: "RUNNING", TaskKind: "COORDINATION", ProjectID: "project-rq", ProjectLane: "coordination", Tags: []string{"project-role-scope", "authority-transition", "strategic-lead", "coordination"}, TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign"}`},
			{TaskID: "task-side-effect-classify", Status: "PENDING", TaskKind: "COORDINATION", ProjectID: "project-rq", ProjectLane: "strategy", Tags: []string{"side-effect", "classification", "coordination"}},
		},
	})
	if blocked || strings.TrimSpace(detail) != "" {
		t.Fatalf("expected coordination-only sibling debt not to block root completion liveness, blocked=%v detail=%q", blocked, detail)
	}
}

func TestProjectRootCompletionLivenessBlocksReviewAndValidationSiblings(t *testing.T) {
	root := WorkspaceTaskRecord{
		TaskID:      "root-project-review",
		Status:      "RUNNING",
		TaskKind:    "COORDINATION",
		ProjectID:   "project-review",
		ProjectLane: "strategy",
	}
	detail, blocked := projectCoordinationCompletionLivenessBlocker(root, ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-review", WorkspaceID: "ws-1"},
		Tasks: []WorkspaceTaskRecord{
			root,
			{TaskID: "task-visual-review", Status: "PENDING", TaskKind: "COORDINATION", ProjectID: "project-review", ProjectLane: "review"},
			{TaskID: "task-final-qa", Status: "RUNNING", TaskKind: "COORDINATION", ProjectID: "project-review", ProjectLane: "validation"},
		},
	})
	if !blocked {
		t.Fatal("expected review/validation sibling work to block root completion liveness")
	}
	for _, want := range []string{"task-visual-review", "task-final-qa", "review", "validation"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("expected blocker detail to mention %q, got %q", want, detail)
		}
	}
}

func TestProjectRootCompletionLivenessBlocksReadyBranchWithoutIntegration(t *testing.T) {
	root := WorkspaceTaskRecord{
		TaskID:       "root-project-tools",
		Title:        "Autonomous coordination root",
		Description:  "Strategic agent should coordinate builders and task decomposition.",
		Priority:     "high",
		Status:       "RUNNING",
		TaskKind:     "COORDINATION",
		TaskTemplate: "project",
		ProjectID:    "project-tools",
		ProjectLane:  "strategy",
	}
	session := AgentSessionStateRecord{SessionID: "session-root", TaskID: root.TaskID, Status: "ACTIVE"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": ProjectCoordinationRecord{
				Project: ProjectRecord{ProjectID: "project-tools", WorkspaceID: "ws-1"},
				Tasks: []WorkspaceTaskRecord{
					root,
					{TaskID: "task-build", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-tools", ProjectLane: "implementation"},
				},
				Branches: []ProjectBranchRecord{
					{BranchID: "branch-build", BranchName: "agent/build", Status: "READY_FOR_REVIEW", ActiveTaskID: "task-build", ProjectID: "project-tools"},
				},
			}})
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-project-liveness"}})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		CoordinationMode: CoordinationModeTrustFirst,
	}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	t.Cleanup(func() { _ = runtime.Close() })

	gated, applied, err := runtime.enforceCompletionCoordinationGate(context.Background(), root, session, "run-root", StructuredTaskResult{Outcome: "completed", Summary: "done"}, nil, nil)
	if err != nil {
		t.Fatalf("enforceCompletionCoordinationGate() error = %v", err)
	}
	if !applied || normalizeOutcome(gated.Outcome) != "continue" {
		t.Fatalf("expected ready-for-review branch to keep root open, applied=%v result=%+v", applied, gated)
	}
	if !strings.Contains(gated.Details, "branch-build") || !strings.Contains(gated.Details, "READY_FOR_REVIEW") {
		t.Fatalf("expected branch blocker in details, got %+v", gated)
	}
}

func TestTrustFirstWorkspaceDocDraftGateInactiveBeforeReview(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := NewRuntime(RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		CoordinationMode: CoordinationModeTrustFirst,
	}, &sequenceLLM{})
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    task.TaskID,
		ActiveSessionID: session.SessionID,
		ActiveRunID:     "run-1",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if state := runtime.workspaceDocDraftGateState(); state.Active {
		t.Fatalf("trust-first should not draft-mark workspace docs before peer review, got %+v", state)
	}
}

func TestExecuteTaskCycleDefersCriticalCompletionUntilPeerReview(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-alpha"
	peer := "agent-beta"
	claimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-critical",
		Title:        "Autonomous coding kata",
		Description:  "Do the work and decide review/tests autonomously",
		Priority:     "critical",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-alpha",
		TaskID:    task.TaskID,
		Status:    "ACTIVE",
	}

	var (
		taskComplete   int
		sessionEnd     int
		agentRequests  []map[string]any
		docKeys        []string
		savedScratches []RuntimeScratchState
		stepTitles     []string
		runStatuses    []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.execution.step.write":
			stepTitles = append(stepTitles, rpcString(req.Params, "title"))
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.execution.run.write":
			runStatuses = append(runStatuses, rpcString(req.Params, "status"))
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"generated_at": "2026-04-27T00:00:00Z", "resolved": true}})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "agent.request":
			agentRequests = append(agentRequests, req.Params)
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-review-1",
				"workspace_id": "ws-1",
				"to_agent_id":  peer,
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			if docKey == "task.final" {
				t.Fatalf("coordination gate must not materialize final doc before peer review")
			}
			docKeys = append(docKeys, docKey)
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + docKey})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update": map[string]any{"update_id": "update-1"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "agent.session.status":
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id": rpcString(req.Params, "session_id"),
				"task_id":    rpcString(req.Params, "task_id"),
				"status":     rpcString(req.Params, "status"),
			}})
		case "agent.task.complete":
			taskComplete++
			writeRPCResult(w, req, nil)
		case "agent.session.end":
			sessionEnd++
			writeRPCResult(w, req, map[string]any{"state": map[string]any{"session_id": rpcString(req.Params, "session_id"), "status": rpcString(req.Params, "status")}})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedScratches = append(savedScratches, state)
			writeRPCResult(w, req, nil)
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method in coordination gate path: %s", req.Method)
		}
	}))
	defer server.Close()

	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"outcome":"completed","summary":"done solo","details":"implementation ready","blocked_on":[],"materialize":{"doc_key":"task.final","doc_title":"Final","doc_content":"final answer"}}`}}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      self,
		DisplayName:  "Agent Alpha",
		OwnerUserID:  "owner-1",
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-alpha"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    task.TaskID,
		ActiveSessionID: session.SessionID,
		ActiveRunID:     "run-alpha",
		DocSHAs:         map[string]string{},
	}
	runtime.bootstrap.Snapshot.Agents = []AgentRecord{
		{AgentID: self, DisplayName: "Alpha", Role: "builder", Status: "ACTIVE", IsOnline: true},
		{AgentID: peer, DisplayName: "Beta", Role: "reviewer", Status: "ACTIVE", IsOnline: true},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.executeTaskCycle(context.Background(), task); err != nil {
		t.Fatalf("executeTaskCycle() error = %v", err)
	}

	if taskComplete != 0 || sessionEnd != 0 {
		t.Fatalf("completion gate should defer terminal transition, complete=%d session_end=%d", taskComplete, sessionEnd)
	}
	if len(agentRequests) != 1 {
		t.Fatalf("expected one peer review request, got %d", len(agentRequests))
	}
	if got := rpcString(agentRequests[0], "to_agent_id"); got != peer {
		t.Fatalf("peer request target = %q, want %q", got, peer)
	}
	if !strings.Contains(rpcString(agentRequests[0], "payload_json"), "Review this task before terminal completion") {
		t.Fatalf("peer request payload did not include review prompt: %+v", agentRequests[0])
	}
	if containsTrimmed(docKeys, "task.final") {
		t.Fatalf("final doc was materialized before peer review: %+v", docKeys)
	}
	if !containsTrimmed(stepTitles, "Defer completion pending peer coordination") {
		t.Fatalf("expected coordination gate execution step, got %+v", stepTitles)
	}
	if !containsTrimmed(runStatuses, "ACTIVE") {
		t.Fatalf("expected active run continuation, got %+v", runStatuses)
	}
	if len(savedScratches) == 0 {
		t.Fatal("expected scratch state to be saved")
	}
	last := savedScratches[len(savedScratches)-1]
	if last.CompletionCoordinationReqID != "req-review-1" || last.CompletionCoordinationPeerID != peer || last.CompletionCoordinationState != completionCoordinationStateRequested {
		t.Fatalf("unexpected coordination scratch: %+v", last)
	}
	if last.CompletionCoordinationSessionID != session.SessionID {
		t.Fatalf("expected coordination scratch to preserve session id %q, got %+v", session.SessionID, last)
	}
	if len(last.AdvisorySignals) == 0 || !strings.Contains(last.AdvisorySignals[len(last.AdvisorySignals)-1], "SYSTEM COORDINATION GATE") {
		t.Fatalf("expected coordination advisory signal, got %+v", last.AdvisorySignals)
	}
}

func TestCompletionCoordinationGateDoesNotQueueDuplicateRequestWhilePending(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}
	var agentRequestCalls int
	var gateSteps int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":    "req-review-1",
				"workspace_id":  "ws-1",
				"from_agent_id": "agent-alpha",
				"to_agent_id":   "agent-beta",
				"method":        "model.ask",
				"status":        "PENDING",
			})
		case "workspace.execution.step.write":
			gateSteps++
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-gate"}})
		case "agent.request":
			agentRequestCalls++
			writeRPCResult(w, req, map[string]any{"request_id": "unexpected", "workspace_id": "ws-1", "to_agent_id": "agent-beta", "status": "PENDING"})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{
		CompletionCoordinationTaskID: task.TaskID,
		CompletionCoordinationRunID:  "run-1",
		CompletionCoordinationPeerID: "agent-beta",
		CompletionCoordinationReqID:  "req-review-1",
		CompletionCoordinationState:  completionCoordinationStateRequested,
		DocSHAs:                      map[string]string{},
	}
	runtime.bootstrap.Snapshot.Agents = []AgentRecord{{AgentID: "agent-beta", Status: "ACTIVE", IsOnline: true}}
	t.Cleanup(func() { _ = runtime.Close() })

	gated, applied, err := runtime.enforceCompletionCoordinationGate(context.Background(), task, session, "run-1", StructuredTaskResult{Outcome: "completed", Summary: "done"}, nil, nil)
	if err != nil {
		t.Fatalf("enforceCompletionCoordinationGate() error = %v", err)
	}
	if !applied || gated.Outcome != "continue" {
		t.Fatalf("expected pending request to keep completion gated, applied=%v result=%+v", applied, gated)
	}
	if agentRequestCalls != 0 {
		t.Fatalf("pending coordination request should not queue duplicate agent.request, got %d", agentRequestCalls)
	}
	if gateSteps != 1 {
		t.Fatalf("expected one gate step, got %d", gateSteps)
	}
}

func TestCompletionCoordinationRequestDoesNotBypassContinuationHold(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:                 task.TaskID,
		ActiveSessionID:              session.SessionID,
		CompletionCoordinationTaskID: task.TaskID,
		CompletionCoordinationReqID:  "req-review-1",
		CompletionCoordinationState:  completionCoordinationStateRequested,
		ContinuationHoldTaskID:       task.TaskID,
		ContinuationHoldSessionID:    session.SessionID,
		ContinuationHoldUntil:        time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		DocSHAs:                      map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	runtime.mu.Lock()
	held := runtime.continuationHoldActiveLocked(time.Now().UTC())
	runtime.mu.Unlock()
	if !held {
		t.Fatal("pending completion coordination request should stay parked until an agent.response queues request_resume")
	}
}

func TestCompletionCoordinationGateClearsStaleTaskScratchBeforeSelectingPeer(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-new", Title: "New critical task", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-new", TaskID: task.TaskID, Status: "ACTIVE"}
	var requestTarget string
	var savedScratches []RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedScratches = append(savedScratches, state)
			writeRPCResult(w, req, nil)
		case "agent.request":
			requestTarget = rpcString(req.Params, "to_agent_id")
			writeRPCResult(w, req, map[string]any{"request_id": "req-new", "workspace_id": "ws-1", "to_agent_id": requestTarget, "status": "PENDING"})
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-gate"}})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{
		CompletionCoordinationTaskID: "task-old",
		CompletionCoordinationPeerID: "agent-beta",
		CompletionCoordinationReqID:  "req-old",
		CompletionCoordinationState:  completionCoordinationStateRequested,
		DocSHAs:                      map[string]string{},
	}
	runtime.bootstrap.Snapshot.Agents = []AgentRecord{
		{AgentID: "agent-beta", Role: "reviewer", Status: "ACTIVE", IsOnline: true},
		{AgentID: "agent-gamma", Role: "synthesizer", Status: "ACTIVE", IsOnline: true},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	gated, applied, err := runtime.enforceCompletionCoordinationGate(context.Background(), task, session, "run-new", StructuredTaskResult{Outcome: "completed", Summary: "done"}, nil, nil)
	if err != nil {
		t.Fatalf("enforceCompletionCoordinationGate() error = %v", err)
	}
	if !applied || gated.Outcome != "continue" {
		t.Fatalf("expected completion to be gated for new task, applied=%v result=%+v", applied, gated)
	}
	if requestTarget != "agent-beta" {
		t.Fatalf("stale peer exclusion leaked from old task; request target=%q, want agent-beta", requestTarget)
	}
	if len(savedScratches) < 2 {
		t.Fatalf("expected scratch clear and new coordination save, got %+v", savedScratches)
	}
	last := savedScratches[len(savedScratches)-1]
	if last.CompletionCoordinationTaskID != task.TaskID || last.CompletionCoordinationReqID != "req-new" {
		t.Fatalf("expected new task coordination scratch, got %+v", last)
	}
}

func TestCompletionCoordinationReviewReadyQueuesResumeTrigger(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-review-1",
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-beta",
				"status":       "PENDING",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.setCompletionCoordinationScratch(context.Background(), task, session, "run-1", "agent-beta", "req-review-1", completionCoordinationStateReviewReady, "looks good", "review ready"); err != nil {
		t.Fatalf("setCompletionCoordinationScratch() error = %v", err)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != task.TaskID || saved.PendingTriggerSession != session.SessionID {
		t.Fatalf("review-ready coordination should queue request_resume trigger, got %+v", saved)
	}
	if saved.CompletionCoordinationSessionID != session.SessionID {
		t.Fatalf("review-ready coordination should preserve session id, got %+v", saved)
	}
}

func TestWorkspaceDocDraftGateInactiveAfterReviewReady(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:                 task.TaskID,
		ActiveSessionID:              session.SessionID,
		ActiveRunID:                  "run-1",
		CompletionCoordinationTaskID: task.TaskID,
		CompletionCoordinationRunID:  "run-1",
		CompletionCoordinationReqID:  "req-review-1",
		CompletionCoordinationState:  completionCoordinationStateReviewReady,
		DocSHAs:                      map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if state := runtime.workspaceDocDraftGateState(); state.Active {
		t.Fatalf("review-ready task should allow final doc writes without draft marker, got %+v", state)
	}
}

func TestWorkspaceDocDraftGateActiveBeforeReview(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    task.TaskID,
		ActiveSessionID: session.SessionID,
		ActiveRunID:     "run-1",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	state := runtime.workspaceDocDraftGateState()
	if !state.Active || state.State != "pre_review" || state.TaskID != task.TaskID || state.RunID != "run-1" {
		t.Fatalf("critical task should draft-mark final docs before peer review, got %+v", state)
	}
}

func TestAgentResponseRuntimeEventQueuesRequestResumeTrigger(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-review-1",
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-epsilon",
				"status":       "PROCESSING",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:                 task.TaskID,
		ActiveSessionID:              session.SessionID,
		CompletionCoordinationTaskID: task.TaskID,
		CompletionCoordinationReqID:  "req-review-1",
		CompletionCoordinationState:  completionCoordinationStateRequested,
		ContinuationHoldTaskID:       task.TaskID,
		ContinuationHoldSessionID:    session.SessionID,
		ContinuationHoldUntil:        time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		DocSHAs:                      map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	runtime.handleAgentResponseRuntimeEvent(context.Background(), RhizomeEvent{Type: "agent.response", WorkspaceID: "ws-1", AgentID: "agent-alpha", Summary: "response received"})

	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != task.TaskID || saved.PendingTriggerSession != session.SessionID {
		t.Fatalf("expected agent.response to queue request_resume trigger, got %+v", saved)
	}
	select {
	case <-runtime.eventWakePlanner:
	case <-time.After(time.Second):
		t.Fatal("expected agent.response trigger to wake planner")
	}
}

func TestAgentResponseRuntimeEventQueuesRequestResumeFromCoordinationScratch(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request.result":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-review-1",
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-epsilon",
				"status":       "PROCESSING",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{
		CompletionCoordinationTaskID:    "task-critical",
		CompletionCoordinationSessionID: "session-alpha",
		CompletionCoordinationReqID:     "req-review-1",
		CompletionCoordinationState:     completionCoordinationStateRequested,
		ContinuationHoldTaskID:          "task-critical",
		ContinuationHoldSessionID:       "session-alpha",
		ContinuationHoldUntil:           time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		DocSHAs:                         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	payload, err := json.Marshal(map[string]any{
		"request_id":    "req-review-1",
		"from_agent_id": "agent-alpha",
		"to_agent_id":   "agent-epsilon",
		"task_id":       "task-critical",
		"session_id":    "session-alpha",
	})
	if err != nil {
		t.Fatalf("marshal event payload: %v", err)
	}
	runtime.handleAgentResponseRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "agent.response",
		WorkspaceID: "ws-1",
		AgentID:     "agent-epsilon",
		Summary:     "response received",
		PayloadJSON: string(payload),
	})

	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-critical" || saved.PendingTriggerSession != "session-alpha" {
		t.Fatalf("expected response payload to wake coordination session, got %+v", saved)
	}
	select {
	case <-runtime.eventWakePlanner:
	case <-time.After(time.Second):
		t.Fatal("expected response payload trigger to wake planner")
	}
}

func TestAgentResponseRuntimeEventCapturesCompletedCoordinationResponse(t *testing.T) {
	var saved RuntimeScratchState
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.request.result":
			if got := rpcString(req.Params, "request_id"); got != "req-review-1" {
				t.Fatalf("unexpected request_id %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"request_id":    "req-review-1",
				"workspace_id":  "ws-1",
				"from_agent_id": "agent-alpha",
				"to_agent_id":   "agent-gamma",
				"method":        "model.ask",
				"status":        "COMPLETED",
				"response":      "approve after narrowing the smoke gate",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{
		CompletionCoordinationTaskID:    "task-critical",
		CompletionCoordinationRunID:     "run-alpha",
		CompletionCoordinationSessionID: "session-alpha",
		CompletionCoordinationPeerID:    "agent-gamma",
		CompletionCoordinationReqID:     "req-review-1",
		CompletionCoordinationState:     completionCoordinationStateRequested,
		DocSHAs:                         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	payload, err := json.Marshal(map[string]any{
		"request_id":    "req-review-1",
		"from_agent_id": "agent-alpha",
		"to_agent_id":   "agent-gamma",
		"task_id":       "task-critical",
		"session_id":    "session-alpha",
	})
	if err != nil {
		t.Fatalf("marshal event payload: %v", err)
	}
	runtime.handleAgentResponseRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "agent.response",
		WorkspaceID: "ws-1",
		AgentID:     "agent-gamma",
		PayloadJSON: string(payload),
	})

	if got := strings.Join(methods, ","); got != "agent.request.result,agent.state.set" {
		t.Fatalf("expected response read then state save, got %s", got)
	}
	if saved.CompletionCoordinationState != completionCoordinationStateReviewReady {
		t.Fatalf("expected completed response to mark review ready, got %+v", saved)
	}
	if saved.CompletionCoordinationReply != "approve after narrowing the smoke gate" {
		t.Fatalf("expected review reply persisted in scratch, got %+v", saved)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-critical" || saved.PendingTriggerSession != "session-alpha" {
		t.Fatalf("expected review-ready response to queue resume trigger, got %+v", saved)
	}
	if len(saved.AdvisorySignals) == 0 || !strings.Contains(saved.AdvisorySignals[len(saved.AdvisorySignals)-1], "approve after narrowing") {
		t.Fatalf("expected advisory signal with review response, got %+v", saved.AdvisorySignals)
	}
	select {
	case <-runtime.eventWakePlanner:
	case <-time.After(time.Second):
		t.Fatal("expected completed response capture to wake planner")
	}
}

func TestRefreshCompletionCoordinationResponseCapturesMissedCompletedResponse(t *testing.T) {
	var saved RuntimeScratchState
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.request.result":
			if got := rpcString(req.Params, "request_id"); got != "req-review-1" {
				t.Fatalf("unexpected request_id %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"request_id":    "req-review-1",
				"workspace_id":  "ws-1",
				"from_agent_id": "agent-alpha",
				"to_agent_id":   "agent-delta",
				"method":        "model.ask",
				"status":        "COMPLETED",
				"response":      "review is complete; clear the stale blocker and finish the handoff",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{TaskID: "task-critical", Title: "Critical", Priority: "critical"}
	session := AgentSessionStateRecord{SessionID: "session-alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{
		CompletionCoordinationTaskID:    task.TaskID,
		CompletionCoordinationRunID:     "run-old",
		CompletionCoordinationSessionID: session.SessionID,
		CompletionCoordinationPeerID:    "agent-delta",
		CompletionCoordinationReqID:     "req-review-1",
		CompletionCoordinationState:     completionCoordinationStateRequested,
		DocSHAs:                         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.refreshCompletionCoordinationResponse(context.Background(), task, session, "run-new"); err != nil {
		t.Fatalf("refreshCompletionCoordinationResponse() error = %v", err)
	}

	if got := strings.Join(methods, ","); got != "agent.request.result,agent.state.set" {
		t.Fatalf("expected response read then state save, got %s", got)
	}
	if saved.CompletionCoordinationState != completionCoordinationStateReviewReady {
		t.Fatalf("expected missed response to mark review ready, got %+v", saved)
	}
	if saved.CompletionCoordinationRunID != "run-new" {
		t.Fatalf("expected refreshed run id to be persisted, got %+v", saved)
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != task.TaskID || saved.PendingTriggerSession != session.SessionID {
		t.Fatalf("expected refresh to queue resume trigger, got %+v", saved)
	}
	if len(saved.AdvisorySignals) == 0 || !strings.Contains(saved.AdvisorySignals[len(saved.AdvisorySignals)-1], "clear the stale blocker") {
		t.Fatalf("expected advisory signal with missed review response, got %+v", saved.AdvisorySignals)
	}
	select {
	case <-runtime.eventWakePlanner:
	case <-time.After(time.Second):
		t.Fatal("expected missed response capture to wake planner")
	}
}

func TestAgentResponseRuntimeEventQueuesRequestResumeForGenericContinuationHold(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.state.set" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
			t.Fatalf("decode scratch state: %v", err)
		}
		writeRPCResult(w, req, nil)
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:              "task-generic",
		ActiveSessionID:           "session-generic",
		ContinuationHoldTaskID:    "task-generic",
		ContinuationHoldSessionID: "session-generic",
		ContinuationHoldUntil:     time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		DocSHAs:                   map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	payload, err := json.Marshal(map[string]any{
		"request_id":    "req-generic-1",
		"from_agent_id": "agent-alpha",
		"to_agent_id":   "agent-delta",
	})
	if err != nil {
		t.Fatalf("marshal event payload: %v", err)
	}
	runtime.handleAgentResponseRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "agent.response",
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
		Summary:     "response received",
		PayloadJSON: string(payload),
	})

	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-generic" || saved.PendingTriggerSession != "session-generic" {
		t.Fatalf("expected generic response to wake held session, got %+v", saved)
	}
	select {
	case <-runtime.eventWakePlanner:
	case <-time.After(time.Second):
		t.Fatal("expected generic response trigger to wake planner")
	}
}

func TestAgentResponseRuntimeEventIgnoresUnrelatedRequestID(t *testing.T) {
	var stateSetCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method == "agent.state.set" {
			stateSetCalls++
			t.Fatalf("unrelated response should not persist pending trigger")
		}
		t.Fatalf("unexpected method: %s", req.Method)
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "agent-alpha"}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{
		CompletionCoordinationTaskID:    "task-critical",
		CompletionCoordinationSessionID: "session-alpha",
		CompletionCoordinationReqID:     "req-review-1",
		CompletionCoordinationState:     completionCoordinationStateRequested,
		DocSHAs:                         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	payload, err := json.Marshal(map[string]any{
		"request_id":    "req-other",
		"from_agent_id": "agent-alpha",
		"to_agent_id":   "agent-epsilon",
		"task_id":       "task-critical",
		"session_id":    "session-alpha",
	})
	if err != nil {
		t.Fatalf("marshal event payload: %v", err)
	}
	runtime.handleAgentResponseRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "agent.response",
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
		Summary:     "other response received",
		PayloadJSON: string(payload),
	})

	if stateSetCalls != 0 {
		t.Fatalf("expected no pending trigger writes, got %d", stateSetCalls)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("unrelated response should not wake planner")
	default:
	}
}
