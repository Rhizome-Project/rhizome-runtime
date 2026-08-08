package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type ambientRecordingLLM struct {
	mu       sync.Mutex
	calls    int
	messages [][]Message
	tools    [][]ToolDef
	content  string
}

func (l *ambientRecordingLLM) Chat(_ context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.messages = append(l.messages, append([]Message(nil), messages...))
	l.tools = append(l.tools, append([]ToolDef(nil), tools...))
	return &LLMResponse{
		Content: firstNonEmpty(l.content, `{"outcome":"completed","summary":"ambient heartbeat completed","details":"ambient no-task cycle inspected visible state","reflection":{"current_intent":"inspect idle state","fresh_evidence":"agent.work.next returned no work","blocker_freshness":"no blocker","next_useful_move":"record heartbeat evidence"},"materialize":{"doc_key":"agent.agent-alpha.ambient.heartbeat","doc_title":"Ambient Heartbeat","doc_content":"# Ambient Heartbeat\n\nFresh no-task inspection completed."},"memory_title":"Ambient heartbeat","memory_body":"No-task autonomous heartbeat ran and recorded durable evidence.","memory_type":"DECISION"}`),
		Usage:   TokenUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
	}, nil
}

func (l *ambientRecordingLLM) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (l *ambientRecordingLLM) promptText() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	for _, messageSet := range l.messages {
		for _, msg := range messageSet {
			b.WriteString(msg.Content)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func TestRuntimeNoWorkRunsAmbientAutonomyThenEscalatesToDutyTaskWithoutFollowup(t *testing.T) {
	llm := &ambientRecordingLLM{}
	var workNextCalls int
	var taskSubmitCalls int
	var submittedTask map[string]any
	var updateTypes []string
	materializedDocs := map[string]string{}
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-10T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-alpha",
				"has_work":     false,
				"reason":       "idle",
				"packet": map[string]any{
					"work_type":          "no_work_ambient_context",
					"coordination_state": "idle",
					"why_now":            "scheduler says inspect UX lag evidence before declaring complete",
					"project_id":         "project-workflow",
					"context_hints": map[string]any{
						"suggested_doc_keys": []any{"project.project-workflow.reflection_board"},
					},
				},
			})
		case "task.submit":
			taskSubmitCalls++
			submittedTask = req.Params
			if got := rpcString(req.Params, "task_id"); !strings.HasPrefix(got, "task-idle-reflection-") {
				t.Fatalf("ambient fallback should create deterministic idle task, got %q", got)
			}
			if got := rpcString(req.Params, "task_kind"); got != "EXECUTION" {
				t.Fatalf("task_kind = %q, want EXECUTION", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "qa" {
				t.Fatalf("project_lane = %q, want qa", got)
			}
			if !strings.Contains(rpcString(req.Params, "description"), "product-quality iteration") {
				t.Fatalf("fallback task description missing duty guidance: %q", rpcString(req.Params, "description"))
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws-1", "status": "PENDING"})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{"workspace_id": "ws-1"}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{"doc_key": rpcString(req.Params, "doc_key"), "content": ""})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{"project_id": rpcString(req.Params, "project_id")}})
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			materializedDocs[docKey] = rpcString(req.Params, "content")
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + strings.ReplaceAll(docKey, ".", "-")})
		case "agent.update.post":
			updateTypes = append(updateTypes, rpcString(req.Params, "update_type"))
			writeRPCResult(w, req, map[string]any{"ok": true})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during ambient autonomy test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		OwnerUserID:      "owner-1",
		Role:             "strategist",
		CoordinationMode: CoordinationModeTrustFirst,
	}, llm)
	runtime.bootstrap = BootstrapResult{Snapshot: WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-workflow", WorkspaceID: "ws-1", Title: "Workflow Runner", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:       "task-root",
			ProjectID:    "project-workflow",
			Title:        "Headless Workflow Runner",
			OwnerUserID:  "owner-1",
			Priority:     "HIGH",
			Status:       "RESOLVED",
			TaskKind:     "COORDINATION",
			TaskTemplate: "generic",
		}},
	}}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold; i++ {
		task, err := runtime.ensureRunnableTask(context.Background())
		if err != nil {
			t.Fatalf("ensureRunnableTask tick %d error: %v", i+1, err)
		}
		if task != nil {
			t.Fatalf("ambient autonomy should not directly select work on tick %d, got %+v", i+1, task)
		}
	}

	if workNextCalls != idleReflectionNoWorkThreshold {
		t.Fatalf("work.next calls = %d, want %d", workNextCalls, idleReflectionNoWorkThreshold)
	}
	if taskSubmitCalls != 1 {
		t.Fatalf("task.submit calls = %d, want 1", taskSubmitCalls)
	}
	taskID := rpcString(submittedTask, "task_id")
	if taskID == "" {
		t.Fatalf("fallback task_submit params missing task_id: %+v", submittedTask)
	}
	if got := llm.callCount(); got != 1 {
		t.Fatalf("ambient LLM calls = %d, want 1", got)
	}
	if prompt := llm.promptText(); !strings.Contains(prompt, "scheduler says inspect UX lag evidence") || !strings.Contains(prompt, "project.project-workflow.reflection_board") {
		t.Fatalf("ambient prompt did not preserve no-work packet hints:\n%s", prompt)
	}
	if len(llm.tools) == 0 {
		t.Fatal("expected ambient LLM call to receive a filtered tool surface")
	}
	for _, hidden := range []string{"shell", "project_checkout_materialize", "project_patch_queue_materialize", "project_branch_review_ready"} {
		if containsToolDef(llm.tools[0], hidden) {
			t.Fatalf("ambient no-task tool surface should hide %s", hidden)
		}
	}
	if !containsToolDef(llm.tools[0], "workspace_doc_get") {
		t.Fatalf("ambient tool surface should still include read/coordination tools")
	}
	if !containsToolDef(llm.tools[0], "task_submit") {
		t.Fatalf("ambient tool surface should include task_submit for bounded autonomous follow-ups")
	}
	if !containsAmbientString(updateTypes, "ambient_reflection") {
		t.Fatalf("ambient_reflection update not posted, got %v", updateTypes)
	}
	if content := materializedDocs["agent.agent-alpha.ambient.heartbeat"]; !strings.Contains(content, "Fresh no-task inspection completed") {
		t.Fatalf("ambient materialized doc missing expected evidence: %q", content)
	}
	if lastScratch.AmbientAutonomyKey == "" || lastScratch.AmbientAutonomyOutcome != "completed" || lastScratch.AmbientAutonomyDocKey != "agent.agent-alpha.ambient.heartbeat" {
		t.Fatalf("ambient autonomy scratch not persisted: %+v", lastScratch)
	}
	if lastScratch.PendingTrigger != "request_resume" || lastScratch.PendingTriggerTask != taskID {
		t.Fatalf("fallback idle task was not queued for planner wake: %+v", lastScratch)
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask cooldown tick error: %v", err)
	}
	if task != nil {
		t.Fatalf("ambient cooldown should not directly select work, got %+v", task)
	}
	if got := llm.callCount(); got != 1 {
		t.Fatalf("ambient cooldown should suppress repeated LLM calls, got %d", got)
	}
	if taskSubmitCalls != 1 {
		t.Fatalf("ambient cooldown should not create another fallback task, got %d", taskSubmitCalls)
	}
}

func TestRuntimeAmbientTaskSubmitQueuesPlannerWake(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{
		{
			Content: "I found a concrete follow-up.",
			ToolCalls: []ToolCall{{
				ID:   "call-task-submit",
				Type: "function",
				Function: FunctionCall{
					Name:      "task_submit",
					Arguments: `{"title":"Repair stale integration path","description":"Inspect the accepted patch queue item and disposition the stale blocked candidate before integration.","priority":"high"}`,
				},
			}},
		},
		{Content: `{"outcome":"completed","summary":"created bounded repair task","details":"ambient pass submitted a concrete follow-up","reflection":{"current_intent":"unblock repair","fresh_evidence":"task_submit returned a task id","blocker_freshness":"fresh","next_useful_move":"claim created task"}}`},
	}}
	var submittedTask map[string]any
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{"workspace_id": "ws-1"}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{"doc_key": rpcString(req.Params, "doc_key"), "content": ""})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{"project_id": rpcString(req.Params, "project_id")}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case "workspace.policy.check":
			writeRPCResult(w, req, map[string]any{"check": map[string]any{
				"workspace_id": rpcString(req.Params, "workspace_id"),
				"subject_type": rpcString(req.Params, "subject_type"),
				"subject_id":   rpcString(req.Params, "subject_id"),
				"capability":   rpcString(req.Params, "capability"),
				"tool_id":      rpcString(req.Params, "tool_id"),
				"verdict":      "ALLOW",
			}})
		case "task.submit":
			submittedTask = req.Params
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws-1", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-doc"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during ambient task-submit wake test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		OwnerUserID:      "owner-1",
		Role:             "strategist",
		CoordinationMode: CoordinationModeTrustFirst,
	}, llm)
	runtime.bootstrap = BootstrapResult{Snapshot: WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-workflow", WorkspaceID: "ws-1", Title: "Workflow Runner", Status: "ACTIVE"}},
	}}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	t.Cleanup(func() { _ = runtime.Close() })

	target := idleReflectionTarget{
		Key:              "ambient:agent-alpha-project-workflow",
		ScopeKind:        "project",
		ScopeID:          "project-workflow",
		ProjectID:        "project-workflow",
		ProjectLane:      "coordination",
		ReflectionScope:  reflectionScopeProject,
		IdleActionPolicy: idlePolicyOpenUncoveredDirection,
		Title:            "Project ambient heartbeat",
		Description:      "Inspect stale integration repair state.",
	}
	disposition, err := runtime.maybeRunAmbientAutonomy(context.Background(), target, AgentWorkNextResult{WorkspaceID: "ws-1", AgentID: "agent-alpha", HasWork: false, Reason: "idle"}, pendingWorkTrigger{}, time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("maybeRunAmbientAutonomy() error: %v", err)
	}
	taskID := rpcString(submittedTask, "task_id")
	if taskID == "" || !strings.HasPrefix(taskID, "task-ambient-project-workflow-") {
		t.Fatalf("expected deterministic ambient task id, got params %+v", submittedTask)
	}
	if got := rpcString(submittedTask, "project_id"); got != "project-workflow" {
		t.Fatalf("expected target project_id default, got %q", got)
	}
	if got := rpcString(submittedTask, "project_lane"); got != "coordination" {
		t.Fatalf("expected target project_lane default, got %q", got)
	}
	if !disposition.SubmittedTask || len(disposition.SubmittedTaskIDs) != 1 || disposition.SubmittedTaskIDs[0] != taskID {
		t.Fatalf("expected submitted task disposition, got %+v", disposition)
	}
	if lastScratch.PendingTrigger != "request_resume" || lastScratch.PendingTriggerTask != taskID {
		t.Fatalf("ambient task_submit should queue planner wake for created task, got %+v", lastScratch)
	}
}

func TestRuntimeIdleReflectionCooldownBypassedByFreshProjectEvidence(t *testing.T) {
	llm := &ambientRecordingLLM{}
	now := time.Now().UTC()
	cooldownStarted := now.Add(-2 * time.Minute)
	evidenceKey := "task.task-validate-project-workflow-main.browser_smoke_evidence"
	var submittedTask map[string]any
	materializedDocs := map[string]string{}
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []any{
				map[string]any{
					"doc_key":    evidenceKey,
					"title":      "Workflow Runner Browser Smoke Evidence",
					"updated_by": "agent-alpha",
					"updated_at": now.Add(-30 * time.Second).Format(time.RFC3339Nano),
				},
				map[string]any{
					"doc_key":    "task.old-project-workflow.browser_smoke_evidence",
					"title":      "Workflow Runner Old Evidence",
					"updated_by": "agent-alpha",
					"updated_at": cooldownStarted.Add(-time.Minute).Format(time.RFC3339Nano),
				},
			}})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{"workspace_id": "ws-1"}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.doc.get":
			docKey := rpcString(req.Params, "doc_key")
			content := ""
			if docKey == evidenceKey {
				content = "# Browser Smoke Evidence\n\nResult: canonical main rendered Purple Deception Analyst Console instead of Workflow Runner."
			}
			writeRPCResult(w, req, map[string]any{"doc_key": docKey, "title": docKey, "content": content})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{"project_id": rpcString(req.Params, "project_id")}})
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			materializedDocs[docKey] = rpcString(req.Params, "content")
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + strings.ReplaceAll(docKey, ".", "-")})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "task.submit":
			submittedTask = req.Params
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws-1", "status": "PENDING"})
		default:
			t.Fatalf("unexpected method during fresh-evidence cooldown bypass test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-beta",
		OwnerUserID:      "owner-1",
		Role:             "strategist",
		CoordinationMode: CoordinationModeTrustFirst,
	}, llm)
	runtime.bootstrap = BootstrapResult{Snapshot: WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-workflow", WorkspaceID: "ws-1", Title: "Workflow Runner", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:       "task-root",
			ProjectID:    "project-workflow",
			Title:        "Workflow Runner",
			Status:       "RESOLVED",
			TaskKind:     "COORDINATION",
			TaskTemplate: "generic",
		}},
	}}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	runtime.idleNoWorkKey = "project:project-workflow"
	runtime.idleNoWorkCount = idleReflectionNoWorkThreshold - 1
	runtime.idleReflectionKey = "project:project-workflow"
	runtime.idleReflectionAt = cooldownStarted
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.maybeMaterializeIdleReflection(context.Background(), AgentWorkNextResult{WorkspaceID: "ws-1", AgentID: "agent-beta", HasWork: false, Reason: "idle"}, pendingWorkTrigger{}); err != nil {
		t.Fatalf("maybeMaterializeIdleReflection() error: %v", err)
	}

	if got := llm.callCount(); got != 1 {
		t.Fatalf("fresh evidence should bypass idle cooldown and run ambient LLM once, got %d", got)
	}
	if prompt := llm.promptText(); !strings.Contains(prompt, "Purple Deception Analyst Console") || !strings.Contains(prompt, evidenceKey) {
		t.Fatalf("ambient prompt did not include fresh evidence doc content:\n%s", prompt)
	}
	taskID := rpcString(submittedTask, "task_id")
	if taskID == "" {
		t.Fatalf("expected fallback idle task after fresh evidence, got %+v", submittedTask)
	}
	ordinaryID := idleReflectionTaskID("project:project-workflow", now)
	if taskID == ordinaryID {
		t.Fatalf("fresh evidence task should not collide with ordinary cooldown bucket id %q", ordinaryID)
	}
	taskDoc := materializedDocs["task."+taskID]
	if !strings.Contains(taskDoc, "fresh_evidence_doc_keys: "+evidenceKey) {
		t.Fatalf("fresh evidence keys not carried into task doc:\n%s", taskDoc)
	}
	if lastScratch.PendingTrigger != "request_resume" || lastScratch.PendingTriggerTask != taskID {
		t.Fatalf("fresh evidence fallback task should wake planner, got %+v", lastScratch)
	}
}

func TestRuntimeIdleReflectionFreshEvidenceDoesNotBypassActiveProjectWork(t *testing.T) {
	llm := &ambientRecordingLLM{}
	now := time.Now().UTC()
	cooldownStarted := now.Add(-2 * time.Minute)
	claimed := "CLAIMED"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		t.Fatalf("fresh evidence cooldown should not query or mutate RPC while project work is actively owned, got %s", req.Method)
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-beta",
		OwnerUserID:      "owner-1",
		Role:             "strategist",
		CoordinationMode: CoordinationModeTrustFirst,
	}, llm)
	runtime.bootstrap = BootstrapResult{Snapshot: WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-workflow", WorkspaceID: "ws-1", Title: "Workflow Runner", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:       "task-workflow-repair",
			ProjectID:    "project-workflow",
			Title:        "Repair Workflow Runner MVP gap",
			Status:       "RUNNING",
			TaskKind:     "EXECUTION",
			TaskTemplate: "generic",
			ClaimAgentID: stringPtr("agent-beta"),
			ClaimStatus:  &claimed,
		}, {
			TaskID:       "task-workflow-validation-blocked",
			ProjectID:    "project-workflow",
			Title:        "Validate Workflow Runner repair",
			Status:       "BLOCKED",
			TaskKind:     "EXECUTION",
			TaskTemplate: "generic",
		}},
	}}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	runtime.idleNoWorkKey = "project:project-workflow"
	runtime.idleNoWorkCount = idleReflectionNoWorkThreshold - 1
	runtime.idleReflectionKey = "project:project-workflow"
	runtime.idleReflectionAt = cooldownStarted
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.maybeMaterializeIdleReflection(context.Background(), AgentWorkNextResult{WorkspaceID: "ws-1", AgentID: "agent-beta", HasWork: false, Reason: "idle"}, pendingWorkTrigger{}); err != nil {
		t.Fatalf("maybeMaterializeIdleReflection() error: %v", err)
	}
	if got := llm.callCount(); got != 0 {
		t.Fatalf("active project work should suppress fresh-evidence idle bypass, llm calls=%d", got)
	}
}

func TestRuntimeAmbientAutonomySalvagesFencedTrailingStructuredResult(t *testing.T) {
	llm := &ambientRecordingLLM{content: "```json\n" +
		`{"outcome":"completed","summary":"salvaged heartbeat","details":"parsed from fenced JSON","reflection":{"current_intent":"inspect","fresh_evidence":"docs checked","blocker_freshness":"none","next_useful_move":"stop"}}` +
		"\n```\nextra note"}
	var lastScratch RuntimeScratchState
	var payloads []string
	var taskSubmitCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{"workspace_id": "ws-1"}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{"doc_key": rpcString(req.Params, "doc_key"), "content": ""})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{"project_id": rpcString(req.Params, "project_id")}})
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + strings.ReplaceAll(docKey, ".", "-")})
		case "agent.update.post":
			payloads = append(payloads, rpcString(req.Params, "payload_json"))
			writeRPCResult(w, req, map[string]any{"ok": true})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "task.submit":
			taskSubmitCalls++
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws-1", "status": "PENDING"})
		default:
			t.Fatalf("unexpected method during ambient salvage test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		OwnerUserID:      "owner-1",
		Role:             "strategist",
		CoordinationMode: CoordinationModeTrustFirst,
	}, llm)
	runtime.bootstrap = BootstrapResult{Snapshot: WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-workflow", WorkspaceID: "ws-1", Title: "Workflow Runner", Status: "ACTIVE"}},
	}}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold; i++ {
		if err := runtime.maybeMaterializeIdleReflection(context.Background(), AgentWorkNextResult{WorkspaceID: "ws-1", AgentID: "agent-alpha", HasWork: false, Reason: "idle"}, pendingWorkTrigger{}); err != nil {
			t.Fatalf("maybeMaterializeIdleReflection tick %d error: %v", i+1, err)
		}
	}
	if lastScratch.AmbientAutonomyOutcome != "completed" || lastScratch.AmbientAutonomySummary != "salvaged heartbeat" {
		t.Fatalf("expected salvaged result to complete, got %+v", lastScratch)
	}
	if len(payloads) == 0 || !strings.Contains(payloads[len(payloads)-1], `"parse_salvaged":true`) {
		t.Fatalf("expected salvage telemetry in ambient update payloads, got %v", payloads)
	}
	if taskSubmitCalls != 1 || lastScratch.PendingTrigger == "" {
		t.Fatalf("expected salvaged reflection without task_submit to escalate into fallback task, calls=%d scratch=%+v", taskSubmitCalls, lastScratch)
	}
}

func TestRuntimeClosedGateStillRunsAmbientAutonomy(t *testing.T) {
	llm := &ambientRecordingLLM{}
	var updateTypes []string
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{"workspace_id": "ws-1"}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{"doc_key": rpcString(req.Params, "doc_key"), "content": ""})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{"project_id": rpcString(req.Params, "project_id")}})
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + strings.ReplaceAll(docKey, ".", "-")})
		case "agent.update.post":
			updateTypes = append(updateTypes, rpcString(req.Params, "update_type"))
			writeRPCResult(w, req, map[string]any{"ok": true})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "task.submit":
			t.Fatalf("closed gate ambient autonomy must not fall back to singleton idle task")
		default:
			t.Fatalf("unexpected method during closed-gate ambient test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		OwnerUserID:      "owner-1",
		Role:             "strategist",
		CoordinationMode: CoordinationModeTrustFirst,
	}, llm)
	runtime.bootstrap = BootstrapResult{Snapshot: WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-workflow", WorkspaceID: "ws-1", Title: "Workflow Runner", Status: "ACTIVE"}},
	}}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	t.Cleanup(func() { _ = runtime.Close() })

	work := AgentWorkNextResult{
		WorkspaceID: "ws-1",
		AgentID:     "agent-alpha",
		HasWork:     false,
		Reason:      "profile_gate_closed",
		Packet: &AgentWorkPacket{
			Gate: &AgentWorkGate{GateState: "closed", GateType: "profile_autonomous_execution", Summary: "no runnable profile lane"},
		},
	}
	for i := 0; i < idleReflectionNoWorkThreshold; i++ {
		if err := runtime.maybeMaterializeIdleReflection(context.Background(), work, pendingWorkTrigger{}); err != nil {
			t.Fatalf("maybeMaterializeIdleReflection tick %d error: %v", i+1, err)
		}
	}
	if got := llm.callCount(); got != 1 {
		t.Fatalf("closed gate ambient LLM calls = %d, want 1", got)
	}
	if !containsAmbientString(updateTypes, "ambient_reflection") {
		t.Fatalf("closed gate should still post ambient update, got %v", updateTypes)
	}
	if lastScratch.AmbientAutonomyKey == "" || lastScratch.AmbientAutonomyOutcome != "completed" {
		t.Fatalf("closed gate should persist ambient scratch, got %+v", lastScratch)
	}
	if runtime.idleNoWorkCount == 0 {
		t.Fatalf("closed gate should contribute to ambient idle counters")
	}
}

func TestAmbientAutonomyToolExecutorBlocksAuthorityBearingMutation(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{Mode: RuntimeModeDaemon, WorkspaceID: "ws-1", AgentID: "agent-alpha"}}
	for _, toolName := range []string{"shell", "write_file", "project_bootstrap", "project_role_assign", "project_phase_transition", "service_direction_upsert", "service_run_start", "budget_account_ensure", "project_checkout_materialize", "project_patch_queue_materialize", "project_patch_queue_cas_record"} {
		result := runtime.ambientAutonomyToolExecutor(context.Background(), NewToolRegistry(), ToolCall{
			ID:   "call-1",
			Type: "function",
			Function: FunctionCall{
				Name:      toolName,
				Arguments: `{}`,
			},
		})
		if !result.IsError || !strings.Contains(result.Output, "Create or claim a concrete task") {
			t.Fatalf("%s should be blocked in ambient autonomy, got %+v", toolName, result)
		}
	}
}

func TestAmbientAutonomyBlocksTaskSubmitWhileProjectWorkActive(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{Mode: RuntimeModeDaemon, WorkspaceID: "ws-1", AgentID: "agent-alpha", Role: "strategist", CoordinationMode: CoordinationModeTrustFirst}}
	result := runtime.ambientAutonomyToolExecutorWithLimits(context.Background(), NewToolRegistry(), ToolCall{
		ID:   "call-task-submit",
		Type: "function",
		Function: FunctionCall{
			Name:      "task_submit",
			Arguments: `{"title":"Duplicate polish lane","description":"Should be blocked while implementation is active."}`,
		},
	}, idleReflectionTarget{Key: "project:project-workflow", ProjectID: "project-workflow", ActiveTaskIDs: []string{"task-active-implementation"}}, metacognitionPolicyForRuntimeConfig(runtime.cfg), new(int))
	if !result.IsError || !strings.Contains(result.Output, "active non-idle project work already has ownership") {
		t.Fatalf("ambient task_submit should be blocked by active project work, got %+v", result)
	}
}

func TestAmbientAutonomyTargetSuppressesWorkspaceFallbackWhenUnprojectedRootWorkOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("ambient target selection should not call RPC, got %s", r.URL.Path)
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-alpha",
		OwnerUserID:      "owner-1",
		Role:             "strategist",
		CoordinationMode: CoordinationModeTrustFirst,
	}, &ambientRecordingLLM{})
	t.Cleanup(func() { _ = runtime.Close() })

	target := runtime.ambientAutonomyTarget(idleReflectionTarget{}, WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:       "task-clearpress-root",
			Title:        "Clearpress autonomous MVP deployment run",
			Priority:     "HIGH",
			Status:       "PENDING",
			TaskKind:     "COORDINATION",
			TaskTemplate: "generic",
		}},
	}, "ws-1", time.Date(2026, 5, 21, 20, 30, 0, 0, time.UTC), AgentMetacognitionPolicy{
		ReflectionScope:         reflectionScopeGlobal,
		IdleActionPolicy:        idlePolicyOpenUncoveredDirection,
		CanOpenReflectionTasks:  true,
		MaxNewTasksPerIdleCycle: 1,
	})
	if strings.TrimSpace(target.Key) != "" {
		t.Fatalf("ambient workspace fallback should be suppressed while unprojected root work is open, got %+v", target)
	}
}

func TestAmbientAutonomyBlocksBroadAgentRequest(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{Mode: RuntimeModeDaemon, WorkspaceID: "ws-1", AgentID: "agent-alpha", Role: "strategist", CoordinationMode: CoordinationModeTrustFirst}}
	result := runtime.ambientAutonomyToolExecutorWithLimits(context.Background(), NewToolRegistry(), ToolCall{
		ID:   "call-agent-request",
		Type: "function",
		Function: FunctionCall{
			Name:      "agent_request",
			Arguments: `{"target_agent_id":"beta","request_kind":"question","prompt":"What should we do next?"}`,
		},
	}, idleReflectionTarget{Key: "project:project-workflow", ProjectID: "project-workflow"}, metacognitionPolicyForRuntimeConfig(runtime.cfg), nil)
	if !result.IsError || !strings.Contains(result.Output, "blocked broad agent_request") {
		t.Fatalf("ambient broad agent_request should be blocked, got %+v", result)
	}
}

func TestAmbientAutonomyAllowsDelegateTaskAgentRequest(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"workspace_task": map[string]any{
				"task_id":       "task-repair",
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
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-beta",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	registry := NewToolRegistry()
	registry.Register(NewAgentRequestTool(NewRhizomeClient(server.URL, "token"), "ws-1", "agent-alpha"))
	runtime := &Runtime{cfg: RuntimeConfig{Mode: RuntimeModeDaemon, WorkspaceID: "ws-1", AgentID: "agent-alpha", Role: "strategist", CoordinationMode: CoordinationModeTrustFirst}}
	result := runtime.ambientAutonomyToolExecutorWithLimits(context.Background(), registry, ToolCall{
		ID:   "call-agent-request-delegate",
		Type: "function",
		Function: FunctionCall{
			Name:      "agent_request",
			Arguments: `{"to_agent_id":"agent-beta","request_kind":"delegate_task","task_id":"task-repair","prompt":"Please claim and execute task task-repair through your normal planner loop.","wait_for_response":false}`,
		},
	}, idleReflectionTarget{Key: "project:project-workflow", ProjectID: "project-workflow"}, metacognitionPolicyForRuntimeConfig(runtime.cfg), nil)
	if result.IsError || !strings.Contains(result.Output, "peer request queued") {
		t.Fatalf("ambient delegate_task agent_request should pass through executor, got %+v", result)
	}
	if captured == nil || rpcString(captured, "to_agent_id") != "agent-beta" {
		t.Fatalf("expected delegate request to reach RPC, captured=%+v", captured)
	}
	if payload := rpcString(captured, "payload_json"); !strings.Contains(payload, `"request_kind":"delegate_task"`) || !strings.Contains(payload, `"task_id":"task-repair"`) {
		t.Fatalf("delegate request payload missing bounded task metadata: %s", payload)
	}
}

func TestAmbientAutonomyInjectsDeterministicTaskID(t *testing.T) {
	target := idleReflectionTarget{Key: "project:project-workflow", ScopeKind: "project", ScopeID: "project-workflow", ProjectID: "project-workflow"}
	call := ToolCall{Function: FunctionCall{Name: "task_submit", Arguments: `{"title":"Inspect subpixel preview sizing","description":"Verify output sizing against acceptance criteria.","project_lane":"qa"}`}}

	first := ambientAutonomyCallWithDeterministicTaskID(target, call)
	second := ambientAutonomyCallWithDeterministicTaskID(target, call)
	var firstArgs map[string]any
	var secondArgs map[string]any
	if err := json.Unmarshal([]byte(first.Function.Arguments), &firstArgs); err != nil {
		t.Fatalf("decode first args: %v", err)
	}
	if err := json.Unmarshal([]byte(second.Function.Arguments), &secondArgs); err != nil {
		t.Fatalf("decode second args: %v", err)
	}
	firstID := rpcString(firstArgs, "task_id")
	secondID := rpcString(secondArgs, "task_id")
	if firstID == "" || firstID != secondID || !strings.HasPrefix(firstID, "task-ambient-project-workflow-") {
		t.Fatalf("expected stable ambient task id, got first=%q second=%q", firstID, secondID)
	}

	explicit := ambientAutonomyCallWithDeterministicTaskID(target, ToolCall{Function: FunctionCall{Name: "task_submit", Arguments: `{"task_id":"task-explicit","title":"Keep id","description":"Preserve caller id."}`}})
	var explicitArgs map[string]any
	if err := json.Unmarshal([]byte(explicit.Function.Arguments), &explicitArgs); err != nil {
		t.Fatalf("decode explicit args: %v", err)
	}
	if got := rpcString(explicitArgs, "task_id"); got != "task-explicit" {
		t.Fatalf("explicit task_id = %q, want task-explicit", got)
	}
}

func TestBuildAmbientAutonomyObjectiveNamesBoundedNoTaskAuthority(t *testing.T) {
	objective := buildAmbientAutonomyObjective(RuntimeConfig{CoordinationMode: CoordinationModeTrustFirst}, idleReflectionTarget{Key: "ambient:alpha", ScopeKind: "project", ScopeID: "project-1"}, AgentWorkNextResult{}, time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC))
	for _, want := range []string{"one concrete outcome", "do not bootstrap projects", "Do not create, raise, reserve, spend, release, or refund budgets", "Generic host shell is not part", "materialize/register project checkouts", "duty-bearing idle task", "after roughly 4-6 useful tool calls", "Return a strict structured task result JSON object"} {
		if !strings.Contains(objective, want) {
			t.Fatalf("ambient objective missing %q:\n%s", want, objective)
		}
	}
	if strings.Contains(objective, "You may use shell") {
		t.Fatalf("ambient objective should not invite shell use:\n%s", objective)
	}
}

func containsAmbientString(items []string, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item) == want {
			return true
		}
	}
	return false
}

func containsToolDef(tools []ToolDef, name string) bool {
	for _, tool := range tools {
		if strings.TrimSpace(tool.Function.Name) == name {
			return true
		}
	}
	return false
}
