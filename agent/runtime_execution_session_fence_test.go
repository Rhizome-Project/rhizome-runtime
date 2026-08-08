package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTaskLoopObserverDoesNotKeepSessionAliveAfterContentOnlyLLMResponse(t *testing.T) {
	keepaliveSummaries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.session.keepalive":
			keepaliveSummaries = append(keepaliveSummaries, rpcString(req.Params, "summary"))
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"workspace_id": "ws-1",
				"session_id":   "session-1",
				"agent_id":     "agent-1",
				"task_id":      "task-1",
				"status":       "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			RealLLMPilot: true,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		activeSession: &AgentSessionStateRecord{
			WorkspaceID: "ws-1",
			SessionID:   "session-1",
			AgentID:     "agent-1",
			TaskID:      "task-1",
			Status:      "ACTIVE",
		},
	}
	trace := &TaskRunTrace{}

	observer := &taskLoopObserver{ctx: context.Background(), trace: trace, runtime: runtime}
	observer.OnLLMResponse(0, &LLMResponse{Content: "working"})

	if trace.AssistantTurns != 1 {
		t.Fatalf("expected assistant turn to be recorded, got %d", trace.AssistantTurns)
	}
	if len(keepaliveSummaries) != 0 {
		t.Fatalf("content-only response should not keep session alive, got %#v", keepaliveSummaries)
	}
}

func TestTaskLoopObserverRecordsTokenUsageForSessionMetrics(t *testing.T) {
	trace := &TaskRunTrace{}
	observer := &taskLoopObserver{ctx: context.Background(), trace: trace}

	observer.OnLLMResponse(0, &LLMResponse{
		Content: "working",
		Usage: TokenUsage{
			PromptTokens:     11,
			CompletionTokens: 7,
			TotalTokens:      18,
		},
	})
	observer.OnToolResult(0, ToolCall{
		ID:   "call-1",
		Type: "function",
		Function: FunctionCall{
			Name:      "workspace_doc_put",
			Arguments: "{}",
		},
	}, ToolResult{Output: `{"ok":true}`})

	if trace.AssistantTurns != 1 || trace.TotalInputTokens != 11 || trace.TotalOutputTokens != 7 || len(trace.ToolCalls) != 1 {
		t.Fatalf("unexpected trace metrics: %+v", trace)
	}
	input := sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID: "ws-1",
		SessionID:   "session-1",
		AgentID:     "agent-1",
		TaskID:      "task-1",
		Summary:     "done",
		Status:      "ENDED",
	}, trace)
	if input.Iterations != 1 || input.TotalInputTokens != 11 || input.TotalOutputTokens != 7 || input.ToolCalls != 1 {
		t.Fatalf("session event metrics = %+v", input)
	}
}

func TestTaskLoopObserverKeepsSessionAliveAfterToolCallLLMResponse(t *testing.T) {
	keepaliveSummaries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.session.keepalive":
			keepaliveSummaries = append(keepaliveSummaries, rpcString(req.Params, "summary"))
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"workspace_id": "ws-1",
				"session_id":   "session-1",
				"agent_id":     "agent-1",
				"task_id":      "task-1",
				"status":       "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			RealLLMPilot: true,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		activeSession: &AgentSessionStateRecord{
			WorkspaceID: "ws-1",
			SessionID:   "session-1",
			AgentID:     "agent-1",
			TaskID:      "task-1",
			Status:      "ACTIVE",
		},
	}
	trace := &TaskRunTrace{}

	observer := &taskLoopObserver{ctx: context.Background(), trace: trace, runtime: runtime}
	observer.OnLLMResponse(0, &LLMResponse{ToolCalls: []ToolCall{{
		ID:   "call-1",
		Type: "function",
		Function: FunctionCall{
			Name:      "project_status_probe",
			Arguments: "{}",
		},
	}}})

	if trace.AssistantTurns != 1 {
		t.Fatalf("expected assistant turn to be recorded, got %d", trace.AssistantTurns)
	}
	if len(keepaliveSummaries) != 1 || !strings.Contains(keepaliveSummaries[0], "llm_response") {
		t.Fatalf("expected tool-call llm_response keepalive, got %#v", keepaliveSummaries)
	}
}

func TestTaskCycleToolExecutorKeepsSessionAliveAroundTool(t *testing.T) {
	keepaliveSummaries := []string{}
	policyCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.session.keepalive":
			keepaliveSummaries = append(keepaliveSummaries, rpcString(req.Params, "summary"))
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"workspace_id": "ws-1",
				"session_id":   "session-1",
				"agent_id":     "agent-1",
				"task_id":      "task-1",
				"status":       "ACTIVE",
			}})
		case "workspace.policy.check":
			policyCalls++
			writeRPCResult(w, req, map[string]any{"check": map[string]any{
				"workspace_id": rpcString(req.Params, "workspace_id"),
				"subject_type": rpcString(req.Params, "subject_type"),
				"subject_id":   rpcString(req.Params, "subject_id"),
				"capability":   rpcString(req.Params, "capability"),
				"tool_id":      rpcString(req.Params, "tool_id"),
				"verdict":      "ALLOW",
			}})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := runtimeWithActiveSessionFenceTestState(t, server.URL)
	tool := &sessionFenceCountingTool{name: "project_status_probe"}
	registry := NewToolRegistry()
	registry.Register(tool)

	result := runtime.taskCycleToolExecutor(context.Background(), registry, ToolCall{
		ID:   "call-1",
		Type: "function",
		Function: FunctionCall{
			Name:      tool.Name(),
			Arguments: "{}",
		},
	})

	if result.IsError || !strings.Contains(result.Output, "executed") {
		t.Fatalf("expected successful tool execution, got %+v", result)
	}
	if tool.calls != 1 {
		t.Fatalf("expected tool to execute once, got %d", tool.calls)
	}
	if policyCalls != 1 {
		t.Fatalf("expected one policy call for tool.call capability, got %d", policyCalls)
	}
	if len(keepaliveSummaries) != 2 {
		t.Fatalf("expected before/after keepalives, got %#v", keepaliveSummaries)
	}
	if !strings.Contains(keepaliveSummaries[0], "before_tool") || !strings.Contains(keepaliveSummaries[1], "after_tool") {
		t.Fatalf("unexpected keepalive summaries: %#v", keepaliveSummaries)
	}
}

func TestTaskCycleToolExecutorBlocksToolWhenSessionEnded(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	keepaliveCalls := 0
	policyCalls := 0
	savedStates := []RuntimeScratchState{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.session.keepalive":
			keepaliveCalls++
			writeRPCErrorForSessionFenceTest(w, req, "session is not active: session-1 (ENDED)")
		case "workspace.policy.check":
			policyCalls++
			writeRPCResult(w, req, map[string]any{"check": map[string]any{"verdict": "ALLOW"}})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := runtimeWithActiveSessionFenceTestState(t, server.URL)
	tool := &sessionFenceCountingTool{name: "project_status_probe"}
	registry := NewToolRegistry()
	registry.Register(tool)

	result := runtime.taskCycleToolExecutor(context.Background(), registry, ToolCall{
		ID:   "call-1",
		Type: "function",
		Function: FunctionCall{
			Name:      tool.Name(),
			Arguments: "{}",
		},
	})

	if !result.IsError {
		t.Fatalf("expected session fence error, got %+v", result)
	}
	for _, want := range []string{"session fence", "no longer active", "Stop local work"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if tool.calls != 0 {
		t.Fatalf("tool executed despite inactive session fence, calls=%d", tool.calls)
	}
	if policyCalls != 0 {
		t.Fatalf("policy should not be checked after inactive session fence, got %d calls", policyCalls)
	}
	if keepaliveCalls != 1 {
		t.Fatalf("expected one keepalive before blocking, got %d", keepaliveCalls)
	}
	runtime.mu.Lock()
	activeTask := runtime.activeTask
	activeSession := runtime.activeSession
	scratch := runtime.scratch
	runtime.mu.Unlock()
	if activeTask != nil || activeSession != nil {
		t.Fatalf("expected inactive session handling to clear active state, got task=%+v session=%+v", activeTask, activeSession)
	}
	if scratch.ActiveTaskID != "" || scratch.ActiveSessionID != "" || scratch.ActiveRunID != "" {
		t.Fatalf("expected scratch active ids cleared, got %+v", scratch)
	}
	if len(savedStates) == 0 {
		t.Fatal("expected inactive session handling to persist cleared scratch state")
	}
}

func runtimeWithActiveSessionFenceTestState(t *testing.T, serverURL string) *Runtime {
	t.Helper()
	task := WorkspaceTaskRecord{
		TaskID: "task-1",
		Title:  "Task One",
		Status: "RUNNING",
	}
	session := AgentSessionStateRecord{
		WorkspaceID: "ws-1",
		SessionID:   "session-1",
		AgentID:     "agent-1",
		TaskID:      "task-1",
		Status:      "ACTIVE",
	}
	return &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   serverURL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client:        NewRhizomeClient(serverURL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-1",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-1",
			ActiveSessionID: "session-1",
			ActiveRunID:     "run-1",
			DocSHAs:         map[string]string{},
		},
	}
}

func writeRPCErrorForSessionFenceTest(w http.ResponseWriter, req rpcRequest, message string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"error": map[string]any{
			"code":    -32000,
			"message": message,
		},
	})
}

type sessionFenceCountingTool struct {
	name  string
	calls int
}

func (t *sessionFenceCountingTool) Name() string { return t.name }

func (t *sessionFenceCountingTool) Description() string { return "session fence counting tool" }

func (t *sessionFenceCountingTool) Parameters() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

func (t *sessionFenceCountingTool) Execute(context.Context, map[string]any) *ToolResult {
	t.calls++
	return &ToolResult{Output: "executed"}
}
