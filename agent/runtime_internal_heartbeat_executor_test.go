package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type blockingHeartbeatLLM struct{}

func (blockingHeartbeatLLM) Chat(ctx context.Context, _ []Message, _ []ToolDef) (*LLMResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func installTestToolBundle(t *testing.T, workdir, name string) {
	t.Helper()
	dir := filepath.Join(workdir, ".runtime-config", "tool-bundles", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := map[string]any{
		"name":        name,
		"description": "test installed tool bundle",
		"command":     []string{"test-tool-binary"},
		"parameters":  map[string]any{"type": "object"},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestInternalHeartbeatPolicyBlocksLocalOnlyPublicTools(t *testing.T) {
	policy := internalHeartbeatExecutionPolicy(defaultLoopSelfCheckHeartbeat())
	if !policy.LocalOnly || policy.AllowTaskSubmit || policy.AllowPublicDocs || !policy.RequireSession || !policy.ExpectsLocalMemory {
		t.Fatalf("loop_self_check should be local-only without public authority: %+v", policy)
	}
	runtime := &Runtime{}
	registry := NewToolRegistry()
	for _, toolName := range []string{
		"workspace_doc_put",
		"task_submit",
		"agent_request",
		"service_direction_upsert",
		"budget_account_ensure",
		"project_patch_queue_cas_record",
		"project_checkout_materialize",
		"shell",
		"write_file",
	} {
		registry.Register(staticTool{name: toolName, output: "should not execute"})
		result := runtime.internalHeartbeatToolExecutorWithPolicy(context.Background(), registry, ToolCall{
			ID:   "call-" + toolName,
			Type: "function",
			Function: FunctionCall{
				Name:      toolName,
				Arguments: `{}`,
			},
		}, policy, nil)
		if !result.IsError || !strings.Contains(result.Output, "blocked tool") {
			t.Fatalf("%s should be blocked by local-only heartbeat policy, got %+v", toolName, result)
		}
	}
}

func TestInternalHeartbeatPolicyLocalOnlyWinsOverContradictorySuites(t *testing.T) {
	policy := internalHeartbeatExecutionPolicy(AgentHeartbeatSpec{
		ID:         "contradictory",
		Kind:       "metacognition",
		Cadence:    "every_5m",
		Priority:   20,
		Locks:      []string{"local_only"},
		ToolSuites: []string{"bounded_task_submit", "task_authority", "workspace_tools"},
	})
	if !policy.LocalOnly || policy.AllowTaskSubmit || policy.AllowAgentRequest || policy.AllowPublicDocs || policy.RequiresTaskLoop {
		t.Fatalf("local_only should override contradictory public-authority suites: %+v", policy)
	}
	runtime := &Runtime{}
	for _, toolName := range []string{"task_submit", "agent_request", "workspace_doc_put", "project_phase_transition", "shell", "write_file"} {
		result := runtime.internalHeartbeatToolExecutorWithPolicy(context.Background(), NewToolRegistry(), ToolCall{
			ID:   "call-" + toolName,
			Type: "function",
			Function: FunctionCall{
				Name:      toolName,
				Arguments: `{}`,
			},
		}, policy, nil)
		if !result.IsError {
			t.Fatalf("%s should be blocked even with contradictory suites, got %+v", toolName, result)
		}
	}
}

func TestGlobalProgressReviewPolicyAllowsGovernanceToolWithoutTaskSubmit(t *testing.T) {
	policy := internalHeartbeatExecutionPolicy(defaultGlobalProgressReviewHeartbeat("reviewer_qa"))
	if policy.LocalOnly || policy.AllowTaskSubmit || policy.AllowPublicDocs || policy.AllowAgentRequest {
		t.Fatalf("global progress review should be public governance-only without task/doc/delegate authority: %+v", policy)
	}
	if !policy.AllowsTool("project_governance_challenge") || !internalHeartbeatReadOnlyToolLoopAllows(policy, "project_governance_challenge") {
		t.Fatalf("global progress review should expose governed challenge tool when registered: %+v", policy)
	}
	if policy.AllowsTool("task_submit") || internalHeartbeatReadOnlyToolLoopAllows(policy, "task_submit") {
		t.Fatalf("global progress review must not expose task_submit: %+v", policy)
	}
	if !heartbeatKindUsesReflectionChannel(heartbeatKindGlobalProgressReview) {
		t.Fatalf("global progress review should route observations through reflection channel")
	}

	registry := NewToolRegistry()
	registry.Register(staticTool{name: "project_governance_challenge", output: `{"action":"check","all_hold":false}`})
	result := (&Runtime{}).internalHeartbeatToolExecutorWithPolicy(context.Background(), registry, ToolCall{
		ID:   "call-governance",
		Type: "function",
		Function: FunctionCall{
			Name:      "project_governance_challenge",
			Arguments: `{"action":"check"}`,
		},
	}, policy, nil)
	if result.IsError || !strings.Contains(result.Output, `"all_hold":false`) {
		t.Fatalf("governance tool should pass through governed heartbeat policy, got %+v", result)
	}
}

func TestInternalHeartbeatPolicyAllowsReadOnlyMemoryAndDocs(t *testing.T) {
	policy := internalHeartbeatExecutionPolicy(defaultLoopSelfCheckHeartbeat())
	runtime := &Runtime{}
	registry := NewToolRegistry()
	registry.Register(staticTool{name: "workspace_doc_get", output: "doc"})
	registry.Register(staticTool{name: "memory_search", output: "memory"})
	for _, toolName := range []string{"workspace_doc_get", "memory_search"} {
		result := runtime.internalHeartbeatToolExecutorWithPolicy(context.Background(), registry, ToolCall{
			ID:   "call-" + toolName,
			Type: "function",
			Function: FunctionCall{
				Name:      toolName,
				Arguments: `{}`,
			},
		}, policy, nil)
		if result.IsError || strings.TrimSpace(result.Output) == "" {
			t.Fatalf("%s should pass through local read policy, got %+v", toolName, result)
		}
	}
}

func TestInternalHeartbeatReadOnlyToolLoopAllowlistIsConservative(t *testing.T) {
	spec := AgentHeartbeatSpec{
		ID:         "read_only_probe",
		Kind:       "metacognition",
		Cadence:    "every_5m",
		Priority:   20,
		Locks:      []string{"local_only"},
		ToolSuites: []string{"memory_and_docs_read", "workspace_docs_read", "local_log_read", "browser_read_only", "screenshot_capture", "console_read", "custom:danger"},
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	for _, toolName := range []string{"workspace_doc_get", "memory_read", "memory_search", "read_file", "list_directory", "browser_visual_probe"} {
		if !internalHeartbeatReadOnlyToolLoopAllows(policy, toolName) {
			t.Fatalf("%s should be allowed by conservative read-only heartbeat tool loop, policy=%+v", toolName, policy)
		}
	}
	for _, toolName := range []string{
		"memory_reinforce",
		"memory_write",
		"workspace_doc_put",
		"task_submit",
		"agent_request",
		"shell",
		"write_file",
		"browser_click",
		"browser_type",
		"chrome_navigate",
		"screenshot_capture",
		"coalition_seek",
	} {
		if internalHeartbeatReadOnlyToolLoopAllows(policy, toolName) {
			t.Fatalf("%s should be blocked by conservative read-only heartbeat tool loop, policy=%+v", toolName, policy)
		}
	}
}

func TestInternalHeartbeatBoundedTaskSubmitAllowsOnePerCycle(t *testing.T) {
	policy := internalHeartbeatExecutionPolicy(defaultProjectRoleInitiativeHeartbeat("strategist"))
	if policy.LocalOnly || !policy.AllowTaskSubmit || policy.MaxTaskSubmits != 1 {
		t.Fatalf("project initiative policy should allow one bounded task submit: %+v", policy)
	}
	if policy.AllowPublicDocs {
		t.Fatalf("bounded task submit should not imply arbitrary workspace_doc_put authority: %+v", policy)
	}
	runtime := &Runtime{}
	registry := NewToolRegistry()
	registry.Register(staticTool{name: "task_submit", output: `{"task_id":"task-one"}`})
	count := 0
	call := ToolCall{
		ID:   "call-task-submit",
		Type: "function",
		Function: FunctionCall{
			Name:      "task_submit",
			Arguments: `{"title":"One bounded task","description":"Exactly one task submit is allowed."}`,
		},
	}
	first := runtime.internalHeartbeatToolExecutorWithPolicy(context.Background(), registry, call, policy, &count)
	if first.IsError || count != 1 {
		t.Fatalf("first bounded task_submit should pass and count once, result=%+v count=%d", first, count)
	}
	second := runtime.internalHeartbeatToolExecutorWithPolicy(context.Background(), registry, call, policy, &count)
	if !second.IsError || !strings.Contains(second.Output, "max_task_submits=1") || count != 1 {
		t.Fatalf("second bounded task_submit should be blocked without incrementing, result=%+v count=%d", second, count)
	}
}

func TestInternalHeartbeatTaskSubmitInjectsStableTaskIDByHeartbeatScope(t *testing.T) {
	policy := internalHeartbeatExecutionPolicy(defaultProjectRoleInitiativeHeartbeat("strategist"))
	first := internalHeartbeatCallWithDeterministicTaskID(policy, ToolCall{Function: FunctionCall{Name: "task_submit", Arguments: `{"title":"Fix obvious UI bug","description":"One wording" ,"project_id":"project-ui","project_lane":"qa"}`}})
	second := internalHeartbeatCallWithDeterministicTaskID(policy, ToolCall{Function: FunctionCall{Name: "task_submit", Arguments: `{"title":"Repair the same visible UI issue","description":"Different wording","project_id":"project-ui","project_lane":"qa"}`}})
	var firstArgs map[string]any
	var secondArgs map[string]any
	if err := jsonUnmarshalTest(first.Function.Arguments, &firstArgs); err != nil {
		t.Fatal(err)
	}
	if err := jsonUnmarshalTest(second.Function.Arguments, &secondArgs); err != nil {
		t.Fatal(err)
	}
	firstID := stringArg(firstArgs, "task_id")
	secondID := stringArg(secondArgs, "task_id")
	if firstID == "" || firstID != secondID || !strings.HasPrefix(firstID, "task-internal-heartbeat-project_role_initiative-project-ui-") {
		t.Fatalf("expected stable heartbeat/scope task id, first=%q second=%q", firstID, secondID)
	}
	explicit := internalHeartbeatCallWithDeterministicTaskID(policy, ToolCall{Function: FunctionCall{Name: "task_submit", Arguments: `{"task_id":"task-explicit","title":"Keep explicit","description":"Keep explicit."}`}})
	var explicitArgs map[string]any
	if err := jsonUnmarshalTest(explicit.Function.Arguments, &explicitArgs); err != nil {
		t.Fatal(err)
	}
	if got := stringArg(explicitArgs, "task_id"); got != "task-explicit" {
		t.Fatalf("explicit task_id should be preserved, got %q", got)
	}
}

func TestExecuteDueInternalHeartbeatsLocalRecordsSessionLifecycle(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	results, err := runtime.executeDueInternalHeartbeatsLocal(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].HeartbeatID != "loop_self_check" || !results[0].PromotionBlocked {
		t.Fatalf("expected one completed local loop_self_check heartbeat, got %+v", results)
	}
	state := LoadAgentInternalSessionState("ws-1", "sigma")
	if len(state.Sessions) != 1 {
		t.Fatalf("expected one persisted internal session, got %+v", state)
	}
	session := state.Sessions[0]
	if session.Status != "completed" || session.Outcome != "typed_policy_recorded" || session.DurationMillis != 0 {
		t.Fatalf("expected completed typed policy session, got %+v", session)
	}
	if session.Meta["local_only"] != "true" || session.Meta["allow_task_submit"] != "false" || session.Meta["expects_local_memory"] != "true" {
		t.Fatalf("expected local-only policy metadata, got %+v", session.Meta)
	}
	snapshot := runtime.internalHeartbeatStatusSnapshot(now.Add(time.Minute))
	assertHeartbeatState(t, snapshot.Heartbeats, "loop_self_check", false, "cooldown")
}

func TestInternalHeartbeatFailureCooldownWaitsInMemoryAndAfterRestart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-failure-cooldown",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, &sequenceLLM{responses: []*LLMResponse{{Content: `not-json`}}})
	now := time.Date(2026, 5, 16, 11, 0, 0, 0, time.UTC)
	results, err := runtime.executeDueInternalHeartbeatsLocal(context.Background(), now)
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsLocal() error = %v", err)
	}
	if len(results) == 0 || results[0].Status != "failed" {
		t.Fatalf("expected failed heartbeat result for cooldown, got %+v", results)
	}
	assertHeartbeatState(t, runtime.internalHeartbeatStatusSnapshot(now.Add(time.Minute)).Heartbeats, "loop_self_check", false, "cooldown")

	restarted := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-failure-cooldown",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	restarted.internalHeartbeatState.LastRun = internalHeartbeatCooldownsFromSessions(LoadAgentInternalSessionState("ws-failure-cooldown", "sigma"), runtimeAnatomyConfig(restarted.cfg))
	assertHeartbeatState(t, restarted.internalHeartbeatStatusSnapshot(now.Add(2*time.Minute)).Heartbeats, "loop_self_check", false, "cooldown")
}

func TestInternalHeartbeatPerCycleTimeoutRecordsDurableScratchAndLoopHealth(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var savedScratch RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &savedScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			t.Fatalf("unexpected method during heartbeat timeout test: %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID:         "ws-timeout",
		AgentID:             "sigma",
		DisplayName:         "Sigma",
		Workdir:             workdir,
		RhizomeRPC:          server.URL,
		PlannerCycleTimeout: 10 * time.Millisecond,
	}, blockingHeartbeatLLM{})
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	results, err := runtime.executeDueInternalHeartbeatsLocal(context.Background(), now)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "internal_heartbeat") {
		t.Fatalf("expected internal heartbeat timeout error, got %v", err)
	}
	if len(results) == 0 || results[0].Status != "failed" {
		t.Fatalf("expected failed timeout result, got %+v", results)
	}
	state := LoadAgentInternalSessionState("ws-timeout", "sigma")
	if len(state.Sessions) == 0 || state.Sessions[0].Status != "failed" {
		t.Fatalf("expected durable failed heartbeat session, got %+v", state)
	}
	if !strings.Contains(strings.Join(runtime.scratch.AdvisorySignals, "\n"), "internal_heartbeat_timeout") ||
		!strings.Contains(strings.Join(savedScratch.AdvisorySignals, "\n"), "internal_heartbeat_timeout") {
		t.Fatalf("expected scratch timeout signal in memory and persisted state, runtime=%+v saved=%+v", runtime.scratch, savedScratch)
	}
	snapshot := runtime.watchdogSnapshot(now)
	if snapshot.InternalHeartbeatState != "degraded" || snapshot.InternalHeartbeatFailures == 0 {
		t.Fatalf("expected loop-health degraded internal heartbeat signal, got %+v", snapshot)
	}
	assertHeartbeatState(t, runtime.internalHeartbeatStatusSnapshot(now.Add(time.Minute)).Heartbeats, "loop_self_check", false, "cooldown")
}

func TestRecordTypedInternalHeartbeatLocalSessionUsesConfiguredToolLoopAndBacklog(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"A recurring no-work loop should become a local candidate first.",
		"backlog_items":[
			{"dedup_key":"loop:no-work-self-check","kind":"metacognition","title":"No-work loop needs self-check","summary":"The agent repeatedly wakes with no public task and should inspect its own recent sessions before asking for public work.","score":75,"evidence_refs":["session:prior"],"promote":false}
		]
	}`}}}
	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, llm)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(DefaultAgentProfile("sigma", "Sigma", "local self checker")), spec, policy, "repeated_no_work", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || result.Summary == "" {
		t.Fatalf("expected completed backlog-recorded heartbeat, got %+v", result)
	}
	if len(llm.calls) != 1 || len(llm.tools) != 1 || len(llm.tools[0]) == 0 {
		t.Fatalf("expected exactly one tool-enabled local heartbeat LLM call, calls=%d tools=%+v", len(llm.calls), llm.tools)
	}
	state := LoadAgentInternalSessionState("ws-1", "sigma")
	if len(state.Sessions) != 1 || len(state.Backlog) != 1 {
		t.Fatalf("expected one session and one local backlog item, got %+v", state)
	}
	item := state.Backlog[0]
	if item.DedupKey != "loop:no-work-self-check" || item.HeartbeatID != "loop_self_check" || item.LastSessionID != result.SessionID {
		t.Fatalf("unexpected local backlog item: %+v", item)
	}
	if len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 || len(item.PromotionRefs) != 0 {
		t.Fatalf("internal heartbeat must not materialize public task/doc refs, got %+v", item)
	}
	snapshot := runtime.internalHeartbeatStatusSnapshot(now.Add(time.Minute))
	assertHeartbeatState(t, snapshot.Heartbeats, "loop_self_check", false, "cooldown")
}

func TestTypedInternalHeartbeatPublishesCompactPublicSummary(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"SECRET_TOKEN_SHOULD_NOT_LEAK stayed in the private heartbeat notes.",
		"backlog_items":[
			{"dedup_key":"loop:private-public-summary","kind":"metacognition","title":"PRIVATE_BACKLOG_TITLE","summary":"PRIVATE_BACKLOG_SUMMARY should remain local-only.","score":81,"evidence_refs":["internal_session:secret","workspace_doc:public-safe"],"promote":false}
		]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 12, 30, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "repeated_no_work", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" {
		t.Fatalf("expected completed local heartbeat, got %+v", result)
	}
	if got := server.postUpdateCount(); got != 1 {
		t.Fatalf("expected one public heartbeat summary update, got %d", got)
	}
	update := server.lastUpdate()
	if update.WorkspaceID != "ws-1" || update.AgentID != "sigma" || update.UpdateType != "internal_heartbeat_summary" || update.RequiresHuman {
		t.Fatalf("unexpected update envelope: %+v", update)
	}
	published := update.Summary + "\n" + update.PayloadJSON
	for _, forbidden := range []string{
		"SECRET_TOKEN_SHOULD_NOT_LEAK",
		"PRIVATE_BACKLOG_TITLE",
		"PRIVATE_BACKLOG_SUMMARY",
		"backlog_items",
		"context_packet",
		"internal_session:secret",
	} {
		if strings.Contains(published, forbidden) {
			t.Fatalf("public heartbeat summary leaked %q: %s", forbidden, published)
		}
	}
	var payload InternalHeartbeatPublicSummaryPayload
	if err := json.Unmarshal([]byte(update.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode public summary payload: %v; raw=%s", err, update.PayloadJSON)
	}
	if payload.ContractVersion != internalHeartbeatSummaryContractVersion ||
		payload.WorkspaceID != "ws-1" ||
		payload.AgentID != "sigma" ||
		payload.SessionID != result.SessionID ||
		payload.HeartbeatID != "loop_self_check" ||
		payload.Status != "completed" ||
		payload.Outcome != "backlog_recorded" ||
		payload.PublishReason != "first_run" ||
		!payload.ObservabilityOnly ||
		!payload.LocalOnly ||
		payload.AllowTaskSubmit ||
		!payload.PrivateMemoryRedacted {
		t.Fatalf("unexpected public summary payload: %+v", payload)
	}
	if !strings.Contains(payload.Summary, "status=completed") || !strings.Contains(payload.Summary, "outcome=backlog_recorded") {
		t.Fatalf("public summary should be deterministic status/outcome text, got %+v", payload)
	}
	if payload.AnatomyDigest == "" || len(payload.ContextSelectors) == 0 || len(payload.OutputContracts) == 0 {
		t.Fatalf("summary should expose compact public anatomy/context metadata, got %+v", payload)
	}
}

func TestTypedInternalHeartbeatPublicSummaryCoalescesRoutineRepeats(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Routine no action.","backlog_items":[]}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Routine no action again.","backlog_items":[]}`},
	}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 12, 35, 0, 0, time.UTC)
	if _, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "repeated_no_work", now); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "repeated_no_work", now.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := server.postUpdateCount(); got != 1 {
		t.Fatalf("routine repeated internal heartbeat summaries should coalesce, got %d public updates", got)
	}
}

func TestPersonalBacklogArbiterRoutineSummaryDoesNotPublish(t *testing.T) {
	payload := InternalHeartbeatPublicSummaryPayload{
		ContractVersion:   internalHeartbeatSummaryContractVersion,
		WorkspaceID:       "ws-1",
		AgentID:           "sigma",
		SessionID:         "session-arbiter",
		HeartbeatID:       "personal_backlog_arbiter",
		Status:            "completed",
		Outcome:           "backlog_recorded",
		LocalOnly:         true,
		ObservabilityOnly: true,
	}
	runtime := &Runtime{}
	if reason, ok := runtime.shouldPublishInternalHeartbeatSummary(payload, time.Date(2026, 5, 15, 11, 30, 0, 0, time.UTC)); ok || reason != "" {
		t.Fatalf("routine local-only arbiter summaries should not publish, reason=%q ok=%v", reason, ok)
	}
	payload.Status = "failed"
	if reason, ok := runtime.shouldPublishInternalHeartbeatSummary(payload, time.Date(2026, 5, 15, 11, 31, 0, 0, time.UTC)); !ok || reason != "failed" {
		t.Fatalf("failed arbiter summary should still publish, reason=%q ok=%v", reason, ok)
	}
	payload.HeartbeatID = "capability_session_browser_screenshot"
	payload.HeartbeatKind = "capability_session"
	if reason, ok := runtime.shouldPublishInternalHeartbeatSummary(payload, time.Date(2026, 5, 15, 11, 32, 0, 0, time.UTC)); ok || reason != "" {
		t.Fatalf("synthetic local capability summaries should never publish, reason=%q ok=%v", reason, ok)
	}
}

func TestTypedInternalHeartbeatSummaryPublishFailureDoesNotFailSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	server.setFailNextUpdate(true)
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"no_action",
		"summary":"No public action is warranted after this local self-check.",
		"backlog_items":[]
	}`}, {Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"no_action",
		"summary":"No public action is warranted after the retry either.",
		"backlog_items":[]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 12, 45, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "repeated_no_work", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "no_action" {
		t.Fatalf("public summary publish failure must not fail local session, got %+v", result)
	}
	if got := server.postUpdateCount(); got != 1 {
		t.Fatalf("expected one attempted public summary update, got %d", got)
	}
	if _, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "repeated_no_work", now.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := server.postUpdateCount(); got != 2 {
		t.Fatalf("failed summary publication should not mark the heartbeat as publicly summarized, got %d attempts", got)
	}
}

func TestLoopSelfCheckSensorRecordsRepeatedFailureWithoutLLM(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 0, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	for idx := 0; idx < 2; idx++ {
		started := now.Add(time.Duration(-20+idx) * time.Minute).Format(time.RFC3339Nano)
		ended := now.Add(time.Duration(-19+idx) * time.Minute).Format(time.RFC3339Nano)
		if _, err := store.RecordSession(AgentInternalSessionRecord{
			SessionID:      "prior-failed-" + string(rune('a'+idx)),
			HeartbeatID:    "project_role_initiative",
			HeartbeatKind:  "global_metacognition",
			Status:         "failed",
			Outcome:        "typed_result_failed",
			Summary:        "LLM returned invalid heartbeat JSON",
			StartedAt:      started,
			EndedAt:        ended,
			DurationMillis: 60000,
		}); err != nil {
			t.Fatal(err)
		}
	}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "same_blocker_repeated", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("expected local self-check backlog without public promotion, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "self_check_repeated_failure")
	if item.Meta["finding_source"] != internalHeartbeatSelfCheckSensorSource || item.HeartbeatID != "loop_self_check" || item.LastSessionID != result.SessionID {
		t.Fatalf("unexpected self-check repeated failure item: %+v", item)
	}
	if len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 || len(item.PromotionRefs) != 0 {
		t.Fatalf("self-check sensor must stay local-only, got %+v", item)
	}
	for _, want := range []string{"heartbeat:project_role_initiative", "outcome:typed_result_failed"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("self-check evidence missing %q in %+v", want, item.EvidenceRefs)
		}
	}
}

func TestLoopSelfCheckSensorRecordsRepeatedBacklogWithoutLLM(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 5, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "role:unresolved-gap",
		HeartbeatID: "project_role_initiative",
		Kind:        "strategic_gap",
		Status:      "open",
		Title:       "Unresolved project gap",
		Summary:     "This local finding keeps reappearing without suppression, completion, or promotion.",
		Score:       86,
		SeenCount:   3,
		UpdatedAt:   now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
		LastSeenAt:  now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "repeated_no_work", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("expected repeated backlog to stay local, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "self_check_repeated_local_backlog")
	if item.Meta["finding_source"] != internalHeartbeatSelfCheckSensorSource || item.Score < seed.Score {
		t.Fatalf("unexpected repeated backlog self-check item: %+v seed=%+v", item, seed)
	}
	if !containsAnatomyTestString(item.EvidenceRefs, "backlog_item:"+seed.ItemID) || !containsAnatomyTestString(item.EvidenceRefs, "dedup:"+seed.DedupKey) {
		t.Fatalf("self-check repeated backlog should reference original item, got %+v", item.EvidenceRefs)
	}
}

func TestLoopSelfCheckIgnoresArbiterRoutingMetadata(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 10, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "backlog-arbiter:action-route:needs-browser-shot",
		HeartbeatID: "personal_backlog_arbiter",
		Kind:        "personal_backlog_action_route",
		Status:      "open",
		Title:       "Route personal action request: browser_screenshot",
		Summary:     "Routing metadata should not itself trigger a self-check repeated-backlog loop.",
		Score:       96,
		SeenCount:   5,
		LastSeenAt:  now.Add(-time.Minute).Format(time.RFC3339Nano),
		Meta: map[string]string{
			"finding_source": internalHeartbeatBacklogArbiterSource,
		},
	}); err != nil {
		t.Fatal(err)
	}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "repeated_no_work", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome == "backlog_recorded" {
		t.Fatalf("self-check should ignore arbiter routing metadata, got %+v", result)
	}
	if backlogHasKind(runtime.internalSessions, "self_check_repeated_local_backlog") {
		t.Fatalf("routing metadata should not create repeated-backlog self-check item: %+v", runtime.internalSessions.Snapshot().Backlog)
	}
}

func TestLoopSelfCheckSensorRecordsStaleClaimWithoutEvidence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 20, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "frontend implementer")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	claimed := "CLAIMED"
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.scratch.ActiveTaskID = "task-stale"
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Autonomy test"},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:         "task-stale",
			Title:          "Implement converter",
			Status:         "RUNNING",
			ProjectLane:    "implementation",
			ClaimAgentID:   stringPtr("sigma"),
			ClaimStatus:    &claimed,
			ClaimUpdatedAt: stringPtr(now.Add(-2 * time.Hour).Format(time.RFC3339Nano)),
			UpdatedAt:      now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		}},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:   "old-update",
			AgentID:    "sigma",
			UpdateType: "progress",
			Summary:    "Started task-stale",
			CreatedAt:  now.Add(-119 * time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "long_session_no_evidence", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("expected stale claim to become local self-check backlog, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "self_check_claimed_task_no_evidence")
	if item.Meta["finding_source"] != internalHeartbeatSelfCheckSensorSource || item.HeartbeatID != "loop_self_check" {
		t.Fatalf("unexpected stale-claim self-check item: %+v", item)
	}
	for _, want := range []string{"task:task-stale", "claim_status:CLAIMED", "claim_agent:sigma"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("stale claim evidence missing %q in %+v", want, item.EvidenceRefs)
		}
	}
	if len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 || len(item.PromotionRefs) != 0 {
		t.Fatalf("stale claim self-check must remain local-only, got %+v", item)
	}
}

func TestLoopSelfCheckSensorSuppressesStaleClaimWithFreshEvidence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 25, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "frontend implementer")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	claimed := "CLAIMED"
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "sigma", DisplayName: "Sigma", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.scratch.ActiveTaskID = "task-fresh"
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Autonomy test"},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:         "task-fresh",
			Title:          "Implement converter",
			Status:         "RUNNING",
			ProjectLane:    "implementation",
			ClaimAgentID:   stringPtr("sigma"),
			ClaimStatus:    &claimed,
			ClaimUpdatedAt: stringPtr(now.Add(-2 * time.Hour).Format(time.RFC3339Nano)),
			UpdatedAt:      now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		}},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:   "fresh-evidence",
			AgentID:    "sigma",
			UpdateType: "evidence",
			Summary:    "task-fresh tests passed and patch evidence was published",
			CreatedAt:  now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "long_session_no_evidence", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "typed_policy_recorded" {
		t.Fatalf("fresh evidence should suppress stale-claim sensor, got %+v", result)
	}
	if backlogHasKind(runtime.internalSessions, "self_check_claimed_task_no_evidence") {
		t.Fatalf("fresh evidence should prevent stale-claim backlog, got %+v", runtime.internalSessions.Snapshot().Backlog)
	}
}

func TestLoopSelfCheckSensorRecordsTerminalImplementationWithoutReviewEvidence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 30, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "frontend implementer")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	claimed := "CLAIMED"
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "sigma", DisplayName: "Sigma", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Autonomy test"},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:         "task-impl-done",
			Title:          "Frontend implementation",
			Status:         "COMPLETED",
			ProjectID:      "project-ui",
			ProjectLane:    "implementation",
			ClaimAgentID:   stringPtr("sigma"),
			ClaimStatus:    &claimed,
			ClaimUpdatedAt: stringPtr(now.Add(-10 * time.Minute).Format(time.RFC3339Nano)),
			UpdatedAt:      now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
		}},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:   "impl-complete",
			AgentID:    "sigma",
			UpdateType: "implementation",
			Summary:    "task-impl-done implementation completed",
			CreatedAt:  now.Add(-9 * time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "long_session_no_evidence", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("terminal implementation without review evidence should become local backlog, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "self_check_implementation_missing_review")
	if item.Meta["finding_source"] != internalHeartbeatSelfCheckSensorSource {
		t.Fatalf("unexpected missing-review self-check item: %+v", item)
	}
	for _, want := range []string{"task:task-impl-done", "missing:review_or_patch_queue_evidence"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("missing review evidence refs missing %q in %+v", want, item.EvidenceRefs)
		}
	}
}

func TestLoopSelfCheckSensorRecordsDirtyCheckoutWithoutCommitEvidence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 40, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "frontend implementer")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	claimed := "CLAIMED"
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "UI tool"},
		Checkouts: []ProjectCheckoutRecord{{
			CheckoutID:   "checkout-dirty",
			WorkspaceID:  "ws-1",
			ProjectID:    "project-ui",
			RepoID:       "repo-ui",
			AgentID:      "sigma",
			LocalPath:    `C:\fixtures\agents\sigma\project-checkouts\secret-ui-tool`,
			CheckoutKind: "clone",
			BranchName:   "agent/sigma/project-ui/task-dirty",
			DirtyState:   "modified",
			ActiveTaskID: "task-dirty",
			Status:       "ACTIVE",
			LastSeenAt:   now.Add(-40 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:    now.Add(-40 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "sigma", DisplayName: "Sigma", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.scratch.ActiveTaskID = "task-dirty"
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectID: "project-ui", ProjectLane: "implementation", ProjectCoordination: coordinationRaw}
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Autonomy test"},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:         "task-dirty",
			Title:          "Implement dirty checkout feature",
			Status:         "RUNNING",
			ProjectID:      "project-ui",
			ProjectLane:    "implementation",
			ClaimAgentID:   stringPtr("sigma"),
			ClaimStatus:    &claimed,
			ClaimUpdatedAt: stringPtr(now.Add(-40 * time.Minute).Format(time.RFC3339Nano)),
			UpdatedAt:      now.Add(-40 * time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(nil, spec, policy, "dirty_checkout_without_progress", now)
	active := findSelectorPayload(t, packet, "active_task_state")
	if len(active.Checkouts) != 1 || active.Checkouts[0].LocalPathRef == "" || strings.Contains(active.Checkouts[0].LocalPathRef, "Users") || strings.Contains(active.Checkouts[0].LocalPathRef, "developer") {
		t.Fatalf("checkout summary should expose compact dirty metadata without raw local path, got %+v", active.Checkouts)
	}
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "dirty_checkout_without_progress", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("dirty checkout should become local self-check backlog, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "self_check_dirty_checkout_no_commit")
	if item.Meta["finding_source"] != internalHeartbeatSelfCheckSensorSource || item.HeartbeatID != "loop_self_check" {
		t.Fatalf("unexpected dirty-checkout self-check item: %+v", item)
	}
	for _, want := range []string{"checkout:checkout-dirty", "task:task-dirty", "dirty_state:modified"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("dirty checkout evidence missing %q in %+v", want, item.EvidenceRefs)
		}
	}
	for _, ref := range item.EvidenceRefs {
		if strings.Contains(ref, `C:\Users`) || strings.Contains(ref, "developer") {
			t.Fatalf("dirty checkout evidence must not leak raw local path, got %+v", item.EvidenceRefs)
		}
	}
}

func TestLoopSelfCheckSensorSuppressesFreshDirtyCheckout(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 16, 45, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "frontend implementer")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "UI tool"},
		Checkouts: []ProjectCheckoutRecord{{
			CheckoutID:   "checkout-fresh-dirty",
			ProjectID:    "project-ui",
			RepoID:       "repo-ui",
			AgentID:      "sigma",
			LocalPath:    filepath.Join(workdir, "project-checkouts", "fresh-ui-tool"),
			CheckoutKind: "clone",
			BranchName:   "agent/sigma/project-ui/task-fresh-dirty",
			DirtyState:   "modified",
			ActiveTaskID: "task-fresh-dirty",
			Status:       "ACTIVE",
			LastSeenAt:   now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:    now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "sigma", DisplayName: "Sigma", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.scratch.ActiveTaskID = "task-fresh-dirty"
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectID: "project-ui", ProjectLane: "implementation", ProjectCoordination: coordinationRaw}
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Autonomy test"},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-fresh-dirty",
			Title:       "Implement fresh dirty checkout feature",
			Status:      "RUNNING",
			ProjectID:   "project-ui",
			ProjectLane: "implementation",
		}},
	}
	runtime.mu.Unlock()

	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "dirty_checkout_without_progress", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "typed_policy_recorded" {
		t.Fatalf("fresh dirty checkout should not be treated as stuck yet, got %+v", result)
	}
	if backlogHasKind(runtime.internalSessions, "self_check_dirty_checkout_no_commit") {
		t.Fatalf("fresh dirty checkout should not create backlog, got %+v", runtime.internalSessions.Snapshot().Backlog)
	}
}

func TestRecordTypedInternalHeartbeatToolLoopUsesPolicyFilteredToolsAndBacklog(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	llm := &sequenceLLM{responses: []*LLMResponse{
		{
			Content: "Need to inspect a public workspace doc before making a private note.",
			ToolCalls: []ToolCall{{
				ID:   "call-doc-get",
				Type: "function",
				Function: FunctionCall{
					Name:      "workspace_doc_get",
					Arguments: `{"doc_key":"project.contract"}`,
				},
			}},
		},
		{Content: `{
			"contract_version":"internal-heartbeat-local-result/v1",
			"outcome":"backlog_recorded",
			"summary":"Tool-enabled heartbeat inspected allowed read context and recorded a local finding.",
			"backlog_items":[
				{"dedup_key":"tool-loop:allowed-doc-read","kind":"metacognition","title":"Allowed doc read informed local heartbeat","summary":"The generic heartbeat tool loop used only policy-allowed tools before writing local backlog.","score":72,"promote":false}
			]
		}`},
	}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, llm)
	registry := NewToolRegistry()
	registry.Register(staticTool{name: "workspace_doc_get", output: `{"doc_key":"project.contract","title":"Contract"}`})
	registry.Register(staticTool{name: "workspace_doc_put", output: "should not be visible"})
	runtime.agent.registry = registry

	spec := defaultLoopSelfCheckHeartbeat()
	spec.MaxToolIterations = 2
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 14, 20, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "tool_enabled_probe", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" {
		t.Fatalf("expected completed tool-enabled local backlog record, got %+v", result)
	}
	if len(llm.calls) != 2 {
		t.Fatalf("expected two LLM turns through tool loop, got %d", len(llm.calls))
	}
	if got := testToolDefNames(llm.tools[0]); len(got) != 1 || got[0] != "workspace_doc_get" {
		t.Fatalf("tool-enabled heartbeat should expose only policy-filtered tools, got %+v", got)
	}
	secondPrompt := messagesText(llm.calls[1])
	if !strings.Contains(secondPrompt, `"doc_key":"project.contract"`) || strings.Contains(secondPrompt, "should not be visible") {
		t.Fatalf("second tool-loop turn should contain allowed tool output only, got:\n%s", secondPrompt)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "tool-loop:allowed-doc-read")
	if item.Status != "open" || item.LastSessionID != result.SessionID {
		t.Fatalf("expected local backlog item from tool-enabled heartbeat, got %+v", item)
	}
}

func TestRecordTypedInternalHeartbeatToolLoopBlocksDisallowedPublicToolCalls(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	llm := &sequenceLLM{responses: []*LLMResponse{
		{
			Content: "Attempting a forbidden public write despite the local-only heartbeat policy.",
			ToolCalls: []ToolCall{{
				ID:   "call-doc-put",
				Type: "function",
				Function: FunctionCall{
					Name:      "workspace_doc_put",
					Arguments: `{"doc_key":"agent.sigma.bad","title":"Bad","content":"must not write"}`,
				},
			}},
		},
		{Content: `{
			"contract_version":"internal-heartbeat-local-result/v1",
			"outcome":"no_action",
			"summary":"Forbidden public write was blocked by policy."
		}`},
	}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "local self checker")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, llm)
	registry := NewToolRegistry()
	registry.Register(staticTool{name: "workspace_doc_put", output: "PUBLIC_WRITE_SHOULD_NOT_EXECUTE"})
	runtime.agent.registry = registry

	spec := AgentHeartbeatSpec{
		ID:                "local_public_write_probe",
		Kind:              "metacognition",
		Cadence:           "every_15m",
		Priority:          30,
		Locks:             []string{"local_only"},
		ToolSuites:        []string{"workspace_tools"},
		OutputContracts:   []string{"local_memory"},
		MaxToolIterations: 2,
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	if !policy.LocalOnly || len(policy.ToolSuites) != 0 {
		t.Fatalf("local-only policy should strip public authority suites, got %+v", policy)
	}
	now := time.Date(2026, 5, 14, 14, 25, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "tool_enabled_probe", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "no_action" {
		t.Fatalf("expected completed no-action after blocked tool call, got %+v", result)
	}
	secondPrompt := messagesText(llm.calls[1])
	if !strings.Contains(secondPrompt, "blocked tool workspace_doc_put") {
		t.Fatalf("tool loop should feed blocked public-tool result back to the model, got:\n%s", secondPrompt)
	}
	if strings.Contains(secondPrompt, "PUBLIC_WRITE_SHOULD_NOT_EXECUTE") {
		t.Fatalf("disallowed public tool should not execute, got:\n%s", secondPrompt)
	}
}

func TestRecordTypedInternalHeartbeatLocalSessionPromotesOneBoundedCandidate(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"A strategist found two possible gaps but should promote only the highest-value one.",
		"backlog_items":[
			{"dedup_key":"strategy:high-gap","kind":"strategic_gap","title":"High-value unowned quality gap","summary":"A post-MVP quality gap is unowned and needs a bounded public follow-up.","score":95,"evidence_refs":["doc:project.contract"],"promote":true,"reason":"unowned high-value gap"},
			{"dedup_key":"strategy:medium-gap","kind":"strategic_gap","title":"Medium-value follow-up","summary":"Useful but lower priority.","score":80,"evidence_refs":["doc:project.notes"],"promote":true}
		]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	runtime.mu.Lock()
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectID: "project-ui", ProjectLane: "qa"}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	if policy.LocalOnly || !policy.AllowTaskSubmit || policy.MaxTaskSubmits != 1 {
		t.Fatalf("expected bounded public promotion policy, got %+v", policy)
	}
	now := time.Date(2026, 5, 14, 13, 0, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected one bounded promotion in completed session, got %+v", result)
	}
	if server.putDocCount() != 1 || server.submitTaskCount() != 1 {
		t.Fatalf("expected one promotion doc and one task, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	state := LoadAgentInternalSessionState("ws-1", "alpha")
	if len(state.Backlog) != 2 {
		t.Fatalf("expected two local backlog items, got %+v", state.Backlog)
	}
	high := findBacklogByDedup(t, runtime.internalSessions, "strategy:high-gap")
	medium := findBacklogByDedup(t, runtime.internalSessions, "strategy:medium-gap")
	if high.Status != "promoted" || medium.Status != "open" {
		t.Fatalf("expected only high candidate promoted, high=%+v medium=%+v", high, medium)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	docContent := server.lastDocIn.Content
	server.mu.Unlock()
	if submitted.TaskID == "" || submitted.LinkedBy != "alpha" || submitted.OwnerUserID != "owner-1" || !containsTrimmedString(submitted.Tags, "internal-heartbeat") {
		t.Fatalf("unexpected submitted task: %+v", submitted)
	}
	if submitted.ProjectID != "project-ui" || submitted.ProjectLane != "qa" {
		t.Fatalf("promotion target should use trusted runtime project scope, got %+v", submitted)
	}
	if strings.Contains(docContent, "internal_session:") || !strings.Contains(docContent, "doc:project.contract") {
		t.Fatalf("promotion doc should keep public evidence and filter private session refs: %s", docContent)
	}
}

func TestProjectRoleInitiativeSensorPromotesPostMVPFollowupWhenSingleProjectClosed(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 14, 13, 5, 0, 0, time.UTC)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.post-mvp-quality-gap", Title: "Post-MVP quality gap", UpdatedBy: "planner", UpdatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano)},
		},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected typed strategist sensor to promote one bounded follow-up, got %+v", result)
	}
	if server.putDocCount() != 1 || server.submitTaskCount() != 1 {
		t.Fatalf("expected one promotion doc and one task, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "project-initiative:post-mvp-quality-loop:project-ui")
	if item.Status != "promoted" || item.Meta["finding_source"] != internalHeartbeatProjectInitiativeSensorSource || item.Meta["target_project_id"] != "project-ui" || item.Meta["target_project_lane"] != "qa" {
		t.Fatalf("expected promoted typed project initiative backlog item with target scope, got %+v", item)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	docContent := server.lastDocIn.Content
	server.mu.Unlock()
	if submitted.ProjectID != "project-ui" || submitted.ProjectLane != "qa" || submitted.LinkedBy != "alpha" || !containsTrimmedString(submitted.Tags, "project_role_initiative") {
		t.Fatalf("unexpected submitted task target: %+v", submitted)
	}
	for _, want := range []string{"project:project-ui", "status:all_public_tasks_closed", "terminal_task:task-mvp", "doc:project.post-mvp-quality-gap", "explicit_gap_doc:project.post-mvp-quality-gap"} {
		if !strings.Contains(docContent, want) {
			t.Fatalf("promotion doc missing evidence ref %q:\n%s", want, docContent)
		}
	}
	if strings.Contains(docContent, "internal_session:") {
		t.Fatalf("promotion doc should filter private session refs: %s", docContent)
	}
}

func TestProjectRoleInitiativeSensorRecordsLocalOnlyWithoutExplicitGap(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 14, 13, 5, 30, 0, time.UTC)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.contract", Title: "Product contract", UpdatedBy: "planner", UpdatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano)},
		},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("expected local sensemaking only without explicit gap evidence, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("generic contract/completed-task signals must not create public task, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "project-initiative:post-mvp-quality-loop:project-ui")
	if item.Status != "open" || item.Meta["finding_promote"] != "false" || item.Meta["target_project_id"] != "project-ui" {
		t.Fatalf("expected open local-only initiative finding with target metadata, got %+v", item)
	}
}

func TestProjectRoleInitiativeTypedSensorSurvivesLLMBacklogCap(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"The model found too many generic candidates.",
		"backlog_items":[
			{"dedup_key":"strategy:llm-1","kind":"strategic_gap","title":"Generic gap one","summary":"No trusted project scope.","score":99,"promote":true},
			{"dedup_key":"strategy:llm-2","kind":"strategic_gap","title":"Generic gap two","summary":"No trusted project scope.","score":98,"promote":true},
			{"dedup_key":"strategy:llm-3","kind":"strategic_gap","title":"Generic gap three","summary":"No trusted project scope.","score":97,"promote":true}
		]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 14, 13, 5, 45, 0, time.UTC)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.post-mvp-quality-gap", Title: "Post-MVP quality gap", UpdatedBy: "planner", UpdatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano)},
		},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || server.submitTaskCount() != 1 {
		t.Fatalf("expected sensor-backed initiative to survive LLM cap and promote, result=%+v task_count=%d", result, server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "project-initiative:post-mvp-quality-loop:project-ui")
	if item.Status != "promoted" {
		t.Fatalf("expected typed sensor finding promoted, got %+v", item)
	}
	state := LoadAgentInternalSessionState("ws-1", "alpha")
	if len(state.Backlog) != internalHeartbeatMaxBacklogWrites {
		t.Fatalf("expected capped backlog to stay at %d, got %+v", internalHeartbeatMaxBacklogWrites, state.Backlog)
	}
}

func TestProjectRoleInitiativeSensorDoesNotPromoteWhenOpenWorkExists(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 2}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-open-qa", Title: "Review visual evidence", Status: "OPEN", TaskKind: "QA", ProjectID: "project-ui", ProjectLane: "qa"},
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 13, 6, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "typed_policy_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("expected no sensor action while public work remains open, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("open work should block public initiative writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	state := LoadAgentInternalSessionState("ws-1", "alpha")
	if len(state.Backlog) != 0 {
		t.Fatalf("open work should not create strategist initiative backlog, got %+v", state.Backlog)
	}
}

func TestProjectRoleInitiativeSensorRecordsLocalWhenRecentOwnerCoverageExists(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 14, 13, 7, 0, 0, time.UTC)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.post-mvp-quality-gap", Title: "Post-MVP quality gap", UpdatedBy: "planner", UpdatedAt: now.Add(-10 * time.Minute).Format(time.RFC3339Nano)},
		},
		Agents: []AgentRecord{{
			AgentID:     "epsilon",
			DisplayName: "Epsilon",
			Role:        "adversarial reviewer and QA critic",
			Status:      "ACTIVE",
			IsOnline:    true,
			LastSeenAt:  stringPtr(now.Add(-2 * time.Minute).Format(time.RFC3339Nano)),
			CurrentSession: &AgentSessionStateRecord{
				SessionID: "session-epsilon",
				Status:    "ACTIVE",
				Summary:   "Working on project-ui post-MVP quality gap validation.",
				UpdatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
			},
		}},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:   "upd-coverage",
			AgentID:    "epsilon",
			UpdateType: "qa",
			Summary:    "Working on project-ui post-MVP quality gap validation.",
			CreatedAt:  now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("expected local record only when fresh owner coverage exists, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("fresh owner coverage should suppress public promotion, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "project-initiative:post-mvp-quality-loop:project-ui")
	if item.Status != "open" || item.Meta["finding_promote"] != "false" {
		t.Fatalf("expected open local backlog item with promotion blocked by coverage, got %+v", item)
	}
	if !containsAny(strings.Join(item.EvidenceRefs, " "), "owner_coverage_update:epsilon", "owner_coverage_agent:epsilon") {
		t.Fatalf("expected owner coverage evidence ref, got %+v", item.EvidenceRefs)
	}
}

func TestProjectRoleInitiativeOwnerCoverageBlocksLLMPromotionToo(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"The model tries to promote a duplicate gap despite owner coverage.",
		"backlog_items":[
			{"dedup_key":"strategy:duplicate-gap","kind":"strategic_gap","project_id":"project-ui","project_lane":"qa","title":"Duplicate post-MVP quality gap","summary":"Should not promote because coverage exists.","score":99,"promote":true}
		]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 14, 13, 7, 30, 0, time.UTC)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.post-mvp-quality-gap", Title: "Post-MVP quality gap", UpdatedBy: "planner", UpdatedAt: now.Add(-10 * time.Minute).Format(time.RFC3339Nano)},
		},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:   "upd-coverage",
			AgentID:    "epsilon",
			UpdateType: "qa",
			Summary:    "Working on project-ui post-MVP quality gap validation.",
			CreatedAt:  now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("expected owner coverage to block all current-session promotions, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("owner coverage should block LLM promotion too, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	duplicate := findBacklogByDedup(t, runtime.internalSessions, "strategy:duplicate-gap")
	if duplicate.Status != "open" || duplicate.Meta["finding_promote"] != "true" {
		t.Fatalf("LLM finding should remain local/open, got %+v", duplicate)
	}
}

func TestProjectRoleInitiativeGapReportDoesNotCountAsOwnerCoverage(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 14, 13, 7, 45, 0, time.UTC)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.post-mvp-quality-gap", Title: "Post-MVP quality gap", UpdatedBy: "planner", UpdatedAt: now.Add(-10 * time.Minute).Format(time.RFC3339Nano)},
		},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:   "upd-gap-report",
			AgentID:    "epsilon",
			UpdateType: "review",
			Summary:    "Unowned project-ui QA gap needs review follow-up and missing evidence.",
			CreatedAt:  now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || server.submitTaskCount() != 1 {
		t.Fatalf("gap report without ownership should still allow bounded promotion, result=%+v task_count=%d", result, server.submitTaskCount())
	}
}

func TestProjectRoleInitiativeSensorIgnoresStaleOwnerCoverage(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 14, 13, 8, 0, 0, time.UTC)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Single service project"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Export Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-mvp", Title: "Build MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.post-mvp-quality-gap", Title: "Post-MVP quality gap", UpdatedBy: "planner", UpdatedAt: now.Add(-10 * time.Minute).Format(time.RFC3339Nano)},
		},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:   "upd-stale-coverage",
			AgentID:    "epsilon",
			UpdateType: "qa",
			Summary:    "Working on project-ui post-MVP quality gap validation.",
			CreatedAt:  now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || server.submitTaskCount() != 1 {
		t.Fatalf("expected stale coverage to allow bounded promotion, result=%+v task_count=%d", result, server.submitTaskCount())
	}
}

func TestRecordTypedInternalHeartbeatLocalSessionDoesNotPromoteLocalOnlyCandidate(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"Local self-check found something tempting but must stay private.",
		"backlog_items":[
			{"dedup_key":"loop:private-gap","kind":"metacognition","title":"Private loop observation","summary":"This should remain local because loop_self_check is local-only.","score":99,"evidence_refs":["internal:trace"],"promote":true}
		]
	}`}}}
	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 13, 10, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(DefaultAgentProfile("sigma", "Sigma", "local self checker")), spec, policy, "repeated_no_work", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("expected local-only backlog record without promotion, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("local-only heartbeat must not write public docs/tasks, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "loop:private-gap")
	if item.Status != "open" || len(item.PromotionRefs) != 0 {
		t.Fatalf("expected open private backlog item, got %+v", item)
	}
}

func TestInternalHeartbeatPromptIsLocalOnlyAndTyped(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
	}, nil)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "repeated_no_work", time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC))
	prompt := renderInternalHeartbeatPrompt(packet)
	for _, want := range []string{
		"LOCAL-ONLY",
		"do not submit tasks",
		"write workspace docs",
		"request agents",
		"InternalHeartbeatLocalResult",
		"agent-local personal backlog candidates",
		"action_requests",
		"missing capabilities",
		"internal-heartbeat-local-result/v1",
		"repeated_no_work",
		"Heartbeat objective:",
		"compare the last few internal sessions",
		"self_check, working_notes",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	if !packet.LocalOnly || packet.AllowTaskSubmit || len(packet.PolicyInstructions) == 0 || packet.HeartbeatObjective == "" || len(packet.Instructions) == 0 || !containsAnatomyTestString(packet.MemoryLanes, "self_check") {
		t.Fatalf("expected local-only typed packet, got %+v", packet)
	}
	if packet.ActionPolicy.AuthorityBoundary != "local_only" ||
		!containsTrimmedString(packet.ActionPolicy.AllowedCapabilities, "local_personal_backlog") ||
		!containsTrimmedString(packet.ActionPolicy.BlockedCapabilities, "public_task_submit") ||
		!strings.Contains(packet.ActionPolicy.ActionRequestFormat, "action_requests") {
		t.Fatalf("expected explicit local action policy in packet, got %+v", packet.ActionPolicy)
	}
}

func TestInternalHeartbeatContextPacketHydratesSelectorsWithoutPrivateContent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 14, 0, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		ItemID:      "item-visual-overlap",
		DedupKey:    "visual:first-viewport-overlap",
		HeartbeatID: "visual_product_audit",
		Kind:        "visual_finding",
		Status:      "open",
		Title:       "Viewport layout overlap still hurts users",
		Summary:     "The first viewport still needs visual repair.",
		Score:       92,
		SeenCount:   1,
		LastSeenAt:  now.Add(-time.Minute).Format(time.RFC3339Nano),
		UpdatedAt:   now.Add(-time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Service factory"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite tool", Status: "ACTIVE", TaskCount: 2}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-visual-gap", Title: "Fix obvious UI overlap", Status: "OPEN", Priority: "HIGH", TaskKind: "QA", ProjectID: "project-ui", ProjectLane: "qa", Tags: []string{"visual"}},
			{TaskID: "task-done", Title: "Completed implementation", Status: "COMPLETED", Priority: "LOW", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.contract", Title: "Product contract", Content: "DOC_SECRET_SHOULD_NOT_LEAK", UpdatedBy: "planner", UpdatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano)},
			{DocKey: "project.visual-evidence", Title: "Visual evidence packet", Content: "SCREENSHOT_SECRET_SHOULD_NOT_LEAK", UpdatedBy: "critic", UpdatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano)},
		},
		Agents: []AgentRecord{{
			AgentID:     "critic",
			DisplayName: "Critic",
			Role:        "visual QA critic",
			Status:      "ACTIVE",
			IsOnline:    true,
			LastSeenAt:  stringPtr(now.Add(-30 * time.Second).Format(time.RFC3339Nano)),
		}},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:    "upd-1",
			AgentID:     "critic",
			UpdateType:  "review",
			Summary:     "Layout still overlaps.",
			PayloadJSON: `{"secret":"PAYLOAD_SECRET_SHOULD_NOT_LEAK"}`,
			CreatedAt:   now.Add(-30 * time.Second).Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "all_public_tasks_closed", now)
	if packet.TrustedScope.ProjectID != "" || packet.TrustedScope.Source != "workspace_snapshot_context" {
		t.Fatalf("snapshot should hydrate context but not grant project authority, got %+v", packet.TrustedScope)
	}
	workspaceState := findSelectorPayload(t, packet, "workspace_state")
	if workspaceState.Status != "hydrated" || workspaceState.Workspace.ProjectCount != 1 || len(workspaceState.Projects) != 1 || len(workspaceState.Tasks) == 0 {
		t.Fatalf("workspace selector was not hydrated from snapshot: %+v", workspaceState)
	}
	quality := findSelectorPayload(t, packet, "open_quality_gaps")
	if quality.Status != "hydrated" || len(quality.Tasks) != 1 || len(quality.Backlog) != 1 {
		t.Fatalf("quality selector should include task metadata and local backlog metadata, got %+v", quality)
	}
	roleMemory := findSelectorPayload(t, packet, "role_memory")
	if roleMemory.Status != "metadata_only" || len(roleMemory.Backlog) != 1 {
		t.Fatalf("role memory should expose backlog metadata only, got %+v", roleMemory)
	}
	roleCoverage := findSelectorPayload(t, packet, "role_coverage")
	if roleCoverage.Status != "hydrated" || len(roleCoverage.Agents) != 1 || len(roleCoverage.RecentUpdates) != 1 || roleCoverage.Agents[0].AgentID != "critic" {
		t.Fatalf("role coverage should expose compact agent/update metadata, got %+v", roleCoverage)
	}
	rawPacket, _ := json.Marshal(packet)
	prompt := renderInternalHeartbeatPrompt(packet)
	longUpdate := strings.Repeat("x", 400)
	capped := internalHeartbeatRecentUpdateSummaries([]AgentUpdateRecord{{UpdateID: "long", Summary: longUpdate}}, 1)
	if len(capped) != 1 || len(capped[0].Summary) >= len(longUpdate) {
		t.Fatalf("recent update summaries should be capped, got %+v", capped)
	}
	for _, forbidden := range []string{
		"DOC_SECRET_SHOULD_NOT_LEAK",
		"SCREENSHOT_SECRET_SHOULD_NOT_LEAK",
		"PAYLOAD_SECRET_SHOULD_NOT_LEAK",
	} {
		if strings.Contains(string(rawPacket), forbidden) || strings.Contains(prompt, forbidden) {
			t.Fatalf("private sentinel %q leaked into heartbeat context:\n%s", forbidden, prompt)
		}
	}
}

func TestInternalHeartbeatRoleCoverageSelectorSummarizesProjectRolesWithoutSecrets(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Tool", Status: "ACTIVE"},
		StrategicLead: &ProjectRoleRecord{
			RoleID:         "role-lead",
			WorkspaceID:    "ws-1",
			ProjectID:      "project-ui",
			AgentID:        "alpha",
			RoleType:       "strategic_lead",
			Status:         "ACTIVE",
			WriteScopeJSON: `{"paths":["SECRET_WRITE_SCOPE_SHOULD_NOT_LEAK"]}`,
			LeaseToken:     "LEASE_SECRET_SHOULD_NOT_LEAK",
			Summary:        "Project lead coordinating quality follow-up.",
			UpdatedAt:      now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		},
		Roles: []ProjectRoleRecord{
			{
				RoleID:         "role-qa",
				WorkspaceID:    "ws-1",
				ProjectID:      "project-ui",
				AgentID:        "epsilon",
				RoleType:       "qa",
				Status:         "ACTIVE",
				WriteScopeJSON: `{"paths":["SECRET_QA_SCOPE_SHOULD_NOT_LEAK"]}`,
				LeaseToken:     "LEASE_SECRET_2_SHOULD_NOT_LEAK",
				Summary:        strings.Repeat("QA role summary ", 30),
				UpdatedAt:      now.Add(-1 * time.Minute).Format(time.RFC3339Nano),
			},
			{
				RoleID:    "role-old",
				ProjectID: "project-ui",
				AgentID:   "beta",
				RoleType:  "implementation",
				Status:    "RELEASED",
				Summary:   "Released implementation role.",
				UpdatedAt: now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Role coverage workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "Sprite Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks:     []WorkspaceTaskRecord{{TaskID: "task-done", Status: "COMPLETED", ProjectID: "project-ui", ProjectLane: "implementation"}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectID: "project-ui", ProjectLane: "qa", ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(nil, spec, policy, "role_coverage_probe", now)
	roleCoverage := findSelectorPayload(t, packet, "role_coverage")
	if roleCoverage.Status != "hydrated" || len(roleCoverage.Roles) != 3 {
		t.Fatalf("expected compact role summaries, got %+v", roleCoverage)
	}
	if roleCoverage.Roles[0].RoleID != "role-qa" || roleCoverage.Roles[1].RoleID != "role-lead" || roleCoverage.Roles[2].RoleID != "role-old" {
		t.Fatalf("expected active roles before released roles, got %+v", roleCoverage.Roles)
	}
	if roleCoverage.Roles[0].AgentID != "epsilon" || roleCoverage.Roles[0].RoleType != "qa" || len(roleCoverage.Roles[0].Summary) >= len(strings.Repeat("QA role summary ", 30)) {
		t.Fatalf("role summary should keep safe fields and cap summary text, got %+v", roleCoverage.Roles[0])
	}
	rawPacket, _ := json.Marshal(packet)
	prompt := renderInternalHeartbeatPrompt(packet)
	for _, forbidden := range []string{"LEASE_SECRET_SHOULD_NOT_LEAK", "LEASE_SECRET_2_SHOULD_NOT_LEAK", "SECRET_WRITE_SCOPE_SHOULD_NOT_LEAK", "SECRET_QA_SCOPE_SHOULD_NOT_LEAK", "lease_token", "write_scope_json", "payload_json"} {
		if strings.Contains(string(rawPacket), forbidden) || strings.Contains(prompt, forbidden) {
			t.Fatalf("coordination secret %q leaked into heartbeat role coverage:\n%s", forbidden, prompt)
		}
	}
}

func TestInternalHeartbeatRoleOnlyCoordinationDoesNotGrantProjectAuthority(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 9, 10, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		Roles: []ProjectRoleRecord{{
			RoleID:         "role-qa",
			WorkspaceID:    "ws-1",
			ProjectID:      "project-ui",
			AgentID:        "epsilon",
			RoleType:       "qa",
			Status:         "ACTIVE",
			WriteScopeJSON: `{"paths":["SECRET_ROLE_ONLY_SCOPE_SHOULD_NOT_LEAK"]}`,
			LeaseToken:     "ROLE_ONLY_LEASE_SECRET_SHOULD_NOT_LEAK",
			Summary:        "QA role exists, but this packet has no active project authority.",
			UpdatedAt:      now.Add(-time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
	}, nil)
	runtime.mu.Lock()
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(nil, spec, policy, "role_only_coordination_probe", now)
	if packet.TrustedScope.ProjectID != "" || packet.TrustedScope.Source != "" {
		t.Fatalf("role-only coordination should not grant trusted project authority, got %+v", packet.TrustedScope)
	}
	roleCoverage := findSelectorPayload(t, packet, "role_coverage")
	if roleCoverage.Status != "hydrated" || len(roleCoverage.Roles) != 1 || roleCoverage.Roles[0].RoleID != "role-qa" {
		t.Fatalf("role-only coordination should still hydrate safe role coverage, got %+v", roleCoverage)
	}
	rawPacket, _ := json.Marshal(packet)
	prompt := renderInternalHeartbeatPrompt(packet)
	for _, forbidden := range []string{"ROLE_ONLY_LEASE_SECRET_SHOULD_NOT_LEAK", "SECRET_ROLE_ONLY_SCOPE_SHOULD_NOT_LEAK", "lease_token", "write_scope_json", "payload_json"} {
		if strings.Contains(string(rawPacket), forbidden) || strings.Contains(prompt, forbidden) {
			t.Fatalf("role-only coordination secret %q leaked into heartbeat role coverage:\n%s", forbidden, prompt)
		}
	}
}

func TestInternalHeartbeatServicePipelineSelectorSummarizesServiceRunsWithoutSecrets(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "PDF Tools", Status: "ACTIVE"},
		ServiceRuns: []ServiceRunRecord{{
			RunID:            "run-1",
			IdempotencyKey:   "IDEMPOTENCY_SECRET_SHOULD_NOT_LEAK",
			WorkspaceID:      "ws-1",
			CandidateID:      "candidate-1",
			ProjectID:        "project-svc",
			Title:            "PDF Merge SECRET_TITLE_SHOULD_NOT_LEAK",
			Status:           "DEPLOYED",
			DeployTarget:     "vercel SECRET_DEPLOY_SHOULD_NOT_LEAK",
			PublicURL:        "https://tools.example/SECRET_PATH/pdf?token=PUBLIC_URL_SECRET_SHOULD_NOT_LEAK#debug",
			HealthCheckURL:   "https://tools.example/pdf/health?secret=HEALTH_SECRET_SHOULD_NOT_LEAK",
			BudgetAccountID:  "BUDGET_ACCOUNT_SECRET_SHOULD_NOT_LEAK",
			CredentialPolicy: "FREE_TIER_ONLY",
			UpdatedAt:        now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Service workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "PDF Tools", Status: "ACTIVE", TaskCount: 1}},
		Docs: []WorkspaceDocRecord{{
			DocKey:    "project.project-svc.reflection_board",
			Title:     "Reflection board",
			Content:   "RAW_REFLECTION_SECRET_SHOULD_NOT_LEAK",
			UpdatedBy: "alpha",
			UpdatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(nil, spec, policy, "service_pipeline_probe", now)
	servicePipeline := findSelectorPayload(t, packet, "service_pipeline")
	if servicePipeline.Status != "hydrated" || len(servicePipeline.ServiceRuns) != 1 {
		t.Fatalf("expected one service run summary, got %+v", servicePipeline)
	}
	run := servicePipeline.ServiceRuns[0]
	if run.RunID != "run-1" || run.ProjectID != "project-svc" || run.Status != "DEPLOYED" || run.NextAction == "" {
		t.Fatalf("unexpected service run summary: %+v", run)
	}
	if run.Title != "<redacted>" || run.DeployTarget != "<redacted>" {
		t.Fatalf("secret-bearing service fields should be redacted, got %+v", run)
	}
	if run.PublicURL != "https://tools.example" || run.HealthCheckURL != "https://tools.example/pdf/health" {
		t.Fatalf("service URLs should be sanitized, got public=%q health=%q", run.PublicURL, run.HealthCheckURL)
	}
	reflection := findSelectorPayload(t, packet, "reflection_boards")
	if reflection.Status != "hydrated" || len(reflection.Docs) != 1 || reflection.Docs[0].DocKey != "project.project-svc.reflection_board" {
		t.Fatalf("expected reflection-board metadata only, got %+v", reflection)
	}
	rawPacket, _ := json.Marshal(packet)
	prompt := renderInternalHeartbeatPrompt(packet)
	for _, forbidden := range []string{
		"IDEMPOTENCY_SECRET_SHOULD_NOT_LEAK",
		"PUBLIC_URL_SECRET_SHOULD_NOT_LEAK",
		"SECRET_PATH",
		"SECRET_TITLE_SHOULD_NOT_LEAK",
		"SECRET_DEPLOY_SHOULD_NOT_LEAK",
		"HEALTH_SECRET_SHOULD_NOT_LEAK",
		"BUDGET_ACCOUNT_SECRET_SHOULD_NOT_LEAK",
		"RAW_REFLECTION_SECRET_SHOULD_NOT_LEAK",
		"idempotency_key",
		"budget_account_id",
		"payload_json",
	} {
		if strings.Contains(string(rawPacket), forbidden) || strings.Contains(prompt, forbidden) {
			t.Fatalf("service/reflection secret %q leaked into heartbeat context:\n%s", forbidden, prompt)
		}
	}
}

func TestProjectRoleInitiativeSensorPromotesServiceRunFollowupWhenPipelineIdle(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 10, 15, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		ServiceRuns: []ServiceRunRecord{{
			RunID:            "run-deployed",
			WorkspaceID:      "ws-1",
			CandidateID:      "candidate-pdf",
			ProjectID:        "project-svc",
			Title:            "PDF Merge Utility",
			Status:           "DEPLOYED",
			DeployTarget:     "vercel",
			PublicURL:        "https://tools.example/pdf",
			CredentialPolicy: "FREE_TIER_ONLY",
			UpdatedAt:        now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Service workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "PDF Tools", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-mvp",
			Title:       "Build PDF merge MVP",
			Status:      "COMPLETED",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-svc",
			ProjectLane: "implementation",
		}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "service_pipeline_idle", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected service-run idle sensor to promote one follow-up, got %+v", result)
	}
	if server.putDocCount() != 1 || server.submitTaskCount() != 1 {
		t.Fatalf("expected one promotion doc and one task, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedupPrefix(t, runtime.internalSessions, "project-initiative:service-pipeline:project-svc:")
	if item.Status != "promoted" || item.Kind != "service_pipeline_next_step" || item.Meta["target_project_lane"] != "qa" {
		t.Fatalf("expected promoted service pipeline backlog item with qa lane, got %+v", item)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	docContent := server.lastDocIn.Content
	server.mu.Unlock()
	if submitted.ProjectID != "project-svc" || submitted.ProjectLane != "qa" || submitted.LinkedBy != "alpha" {
		t.Fatalf("unexpected service follow-up target: %+v", submitted)
	}
	for _, want := range []string{"service_run:run-deployed", "service_status:deployed", "service_next:collect-public-health-analytics-spend-evidence"} {
		if !strings.Contains(docContent, want) {
			t.Fatalf("promotion doc missing service evidence ref %q:\n%s", want, docContent)
		}
	}
}

func TestServiceFactoryHeartbeatUsesServicePipelineTypedPromotion(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("rho", "Rho", "service scout deploy monetization operator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 11, 15, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		ServiceRuns: []ServiceRunRecord{{
			RunID:       "run-service-factory",
			WorkspaceID: "ws-1",
			CandidateID: "candidate-image-tools",
			ProjectID:   "project-svc",
			Title:       "Image Tools Utility",
			Status:      "DEPLOYED",
			PublicURL:   "https://tools.example/image",
			UpdatedAt:   now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "rho",
		DisplayName: "Rho",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Service workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Image Tools", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-mvp",
			Title:       "Build image tools MVP",
			Status:      "COMPLETED",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-svc",
			ProjectLane: "implementation",
		}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "service_factory_operator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "deploy_monetization_vigilance")
	if !ok {
		t.Fatalf("deploy_monetization_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "service_pipeline_idle", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected service factory typed promotion, got %+v", result)
	}
	item := findBacklogByDedupPrefix(t, runtime.internalSessions, "project-initiative:service-pipeline:project-svc:")
	if item.Status != "promoted" || item.HeartbeatID != "deploy_monetization_vigilance" || item.Meta["finding_source"] != internalHeartbeatProjectInitiativeSensorSource {
		t.Fatalf("expected service factory heartbeat to promote typed service finding, got %+v", item)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	server.mu.Unlock()
	if submitted.ProjectID != "project-svc" || submitted.ProjectLane != "qa" || !strings.Contains(submitted.TaskID, "task-agent-backlog-project-role-project-svc-") {
		t.Fatalf("unexpected service factory promotion target: %+v", submitted)
	}
}

func TestServiceFactoryHeartbeatPromotesServicePipelineInMultiProjectWorkspace(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("rho", "Rho", "service scout deploy monetization operator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		ServiceRuns: []ServiceRunRecord{{
			RunID:       "run-multiproject-service",
			WorkspaceID: "ws-1",
			CandidateID: "candidate-image-tools",
			ProjectID:   "project-svc",
			Title:       "Image Tools Utility",
			Status:      "DEPLOYED",
			PublicURL:   "https://tools.example/image",
			UpdatedAt:   now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "rho",
		DisplayName: "Rho",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Service portfolio"},
		Projects: []ProjectRecord{
			{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Image Tools", Status: "ACTIVE", TaskCount: 1},
			{ProjectID: "project-pdf", WorkspaceID: "ws-1", Title: "PDF Tools", Status: "ACTIVE", TaskCount: 1},
		},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-svc-mvp", Title: "Build image tools MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"},
			{TaskID: "task-pdf-mvp", Title: "Build PDF tools MVP", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-pdf", ProjectLane: "implementation"},
		},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "service_factory_operator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "deploy_monetization_vigilance")
	if !ok {
		t.Fatalf("deploy_monetization_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "service_pipeline_idle", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected multiproject service-pipeline promotion, got %+v", result)
	}
	item := findBacklogByDedupPrefix(t, runtime.internalSessions, "project-initiative:service-pipeline:project-svc:")
	if item.Status != "promoted" || item.Meta["target_project_id"] != "project-svc" {
		t.Fatalf("expected promoted project-scoped service finding, got %+v", item)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	server.mu.Unlock()
	if submitted.ProjectID != "project-svc" || submitted.ProjectLane != "qa" {
		t.Fatalf("unexpected multiproject service promotion target: %+v", submitted)
	}
}

func TestPatchQueueVigilancePromotesAcceptedCandidateToIntegration(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 13, 0, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-accepted",
			ProjectID:  "project-svc",
			BranchName: "agent/zeta/accepted",
			HeadSHA:    "abcdef1234567890",
			Status:     "review_ready",
			UpdatedAt:  now.Add(-45 * time.Minute).Format(time.RFC3339Nano),
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:           "queue-svc",
			ItemID:            "item-accepted",
			ProjectID:         "project-svc",
			BranchID:          "branch-accepted",
			State:             "ACCEPTED",
			HeadSHA:           "abcdef1234567890",
			RepoAuthorityMode: "repoauthority_controlled_queue",
			DecisionDocKey:    "decision.accepted",
			DecisionSummary:   "Accepted non-UI backend candidate after tests.",
			DecidedAt:         now.Add(-40 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:         now.Add(-40 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "zeta",
		DisplayName: "Zeta",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-impl",
			Title:       "Implementation complete",
			Status:      "COMPLETED",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-svc",
			ProjectLane: "implementation",
		}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "integrator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "accepted_patch_waiting", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected accepted patch queue item to promote integration follow-up, got %+v", result)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "patch-queue-vigilance:accepted-integration:project-svc-queue-svc-item-accepted-branch-accepted-abcdef123456")
	if item.Status != "promoted" || item.Meta["finding_source"] != internalHeartbeatPatchQueueVigilanceSource || item.Meta["target_project_id"] != "project-svc" || item.Meta["target_project_lane"] != "integration" {
		t.Fatalf("expected promoted typed patch queue backlog item, got %+v", item)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	docContent := server.lastDocIn.Content
	server.mu.Unlock()
	if submitted.ProjectID != "project-svc" || submitted.ProjectLane != "integration" || submitted.LinkedBy != "zeta" {
		t.Fatalf("unexpected patch queue promotion target: %+v", submitted)
	}
	requirements := submitted.TaskRequirements
	for key, want := range map[string]string{
		"schema":                    "task_requirements.v1",
		"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
		"patch_queue_task_kind":     "integration",
		"required_project_role":     "INTEGRATOR",
		"patch_queue_id":            "queue-svc",
		"queue_id":                  "queue-svc",
		"item_id":                   "item-accepted",
		"branch_id":                 "branch-accepted",
		"head_sha":                  "abcdef123456",
		"required_tool":             "project_patch_queue_integrate",
		"required_transition":       "project_patch_queue_integrate_then_full_product_verify",
		"integration_mode":          "direct_merge",
		"integration_authorization": "direct_merge_for_accepted_unmaterialized_controlled_queue",
	} {
		if got := strings.TrimSpace(fmt.Sprint(requirements[key])); got != want {
			t.Fatalf("patch queue promotion task requirement %s=%q want %q; requirements=%+v", key, got, want, requirements)
		}
	}
	args, ok := requirements["required_tool_args"].(map[string]any)
	if !ok || args["integration_mode"] != "direct_merge" {
		t.Fatalf("patch queue promotion should carry direct_merge tool args, got %#v", requirements["required_tool_args"])
	}
	for _, want := range []string{"patch_queue:queue-svc", "patch_item:item-accepted", "missing:integration_owner", "integration_mode:direct_merge"} {
		if !strings.Contains(docContent, want) {
			t.Fatalf("promotion doc missing patch queue evidence %q:\n%s", want, docContent)
		}
	}
}

func TestPatchQueueVigilancePromotionRequirementsUseStructuredMetaWithoutIntegrationRef(t *testing.T) {
	spec := AgentHeartbeatSpec{ID: "patch_queue_vigilance"}
	item := AgentPersonalBacklogItem{
		ItemID:   "backlog-direct",
		DedupKey: "patch-queue-vigilance:accepted-integration:project-svc-queue-svc-item-accepted-branch-accepted-abcdef123456",
		Kind:     "patch_queue_accepted_integration_gap",
		Meta: map[string]string{
			"finding_source":           internalHeartbeatPatchQueueVigilanceSource,
			"target_project_id":        "project-svc",
			"queue_id":                 "queue-svc",
			"item_id":                  "item-accepted",
			"branch_id":                "branch-accepted",
			"head_sha":                 "abcdef123456",
			"state":                    "ACCEPTED",
			"repo_authority_mode":      "repoauthority_controlled_queue",
			"materialization_accepted": "false",
		},
		EvidenceRefs: []string{"patch_queue:queue-svc", "patch_item:item-accepted", "branch:branch-accepted", "head:abcdef123456"},
	}
	requirements := internalHeartbeatPromotionTaskRequirements(spec, item)
	if requirements == nil {
		t.Fatalf("expected structured patch queue requirements")
	}
	if got := strings.TrimSpace(fmt.Sprint(requirements["integration_mode"])); got != "direct_merge" {
		t.Fatalf("expected direct_merge reconstructed from structured meta, got %q in %+v", got, requirements)
	}
	args, ok := requirements["required_tool_args"].(map[string]any)
	if !ok || args["integration_mode"] != "direct_merge" {
		t.Fatalf("expected direct_merge tool args from structured meta, got %#v", requirements["required_tool_args"])
	}
}

func TestPatchQueueVigilancePromotionRequirementsCoverConvergenceAndReview(t *testing.T) {
	spec := AgentHeartbeatSpec{ID: "patch_queue_vigilance"}
	cases := []struct {
		name      string
		kind      string
		state     string
		summary   string
		wantKind  string
		wantTrans string
	}{
		{
			name:      "blocked-revision",
			kind:      "patch_queue_convergence_gap",
			state:     "BLOCKED",
			wantKind:  "revision",
			wantTrans: "project_patch_queue_revision_commit_review_submit",
		},
		{
			name:     "review-owner",
			kind:     "patch_queue_review_owner_gap",
			state:    "REVIEW_READY",
			wantKind: "review",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item := AgentPersonalBacklogItem{
				ItemID:   "backlog-" + tc.name,
				DedupKey: "patch-queue-vigilance:" + tc.name,
				Kind:     tc.kind,
				Meta: map[string]string{
					"finding_source":    internalHeartbeatPatchQueueVigilanceSource,
					"target_project_id": "project-svc",
					"queue_id":          "queue-svc",
					"item_id":           "item-" + tc.name,
					"branch_id":         "branch-" + tc.name,
					"head_sha":          "abcdef123456",
					"state":             tc.state,
					"decision_summary":  tc.summary,
				},
			}
			requirements := internalHeartbeatPromotionTaskRequirements(spec, item)
			if requirements == nil {
				t.Fatalf("expected requirements for %s", tc.kind)
			}
			if got := strings.TrimSpace(fmt.Sprint(requirements["patch_queue_task_kind"])); got != tc.wantKind {
				t.Fatalf("patch_queue_task_kind=%q want %q; requirements=%+v", got, tc.wantKind, requirements)
			}
			if tc.wantTrans != "" {
				if got := strings.TrimSpace(fmt.Sprint(requirements["required_transition"])); got != tc.wantTrans {
					t.Fatalf("required_transition=%q want %q; requirements=%+v", got, tc.wantTrans, requirements)
				}
			}
		})
	}
}

func TestPatchQueueVigilanceDoesNotDuplicateOpenIntegrationFollowup(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 13, 20, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-svc",
			ItemID:          "item-accepted",
			ProjectID:       "project-svc",
			BranchID:        "branch-accepted",
			State:           "ACCEPTED",
			HeadSHA:         "abcdef1234567890",
			DecisionSummary: "Accepted backend candidate.",
			DecidedAt:       now.Add(-40 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:       now.Add(-40 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "zeta",
		DisplayName: "Zeta",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: 2}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-impl", Title: "Implementation complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"},
			{
				TaskID:      "task-agent-backlog-project-role-project-svc-existing",
				Title:       "Address: Accepted patch queue candidate needs integration ownership",
				Description: "Patch queue refs from promoted heartbeat:\n- patch_queue:queue-svc\n- patch_item:item-accepted\n- branch:branch-accepted\n- head:abcdef123456",
				Status:      "PENDING",
				TaskKind:    "EXECUTION",
				ProjectID:   "project-svc",
				ProjectLane: "integration",
				Tags:        []string{"internal-heartbeat", "patch_queue_vigilance", "patch_queue_accepted_integration_gap", "patch_queue_queue-svc", "patch_item_item-accepted"},
			},
		},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "integrator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "accepted_patch_waiting", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.PromotedRefs) != 0 {
		t.Fatalf("expected no duplicate patch queue promotion, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("open integration follow-up should suppress public writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
}

func TestPatchQueueVigilanceSeesExactBlockedFollowupPastCompactTaskLimit(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("epsilon", "Epsilon", "reviewer QA critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 5, 9, 30, 0, 0, time.UTC)
	headSHA := strings.Repeat("a", 40)
	blockedItem := ProjectPatchQueueItemRecord{
		QueueID:         "queue-svc",
		ItemID:          "item-blocked",
		ProjectID:       "project-svc",
		BranchID:        "branch-blocked",
		State:           "BLOCKED",
		HeadSHA:         headSHA,
		DecisionSummary: "BLOCKED: parser implementation remains missing; publish a revision before integration.",
		DecidedAt:       now.Add(-45 * time.Minute).Format(time.RFC3339Nano),
		UpdatedAt:       now.Add(-45 * time.Minute).Format(time.RFC3339Nano),
	}
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{blockedItem},
	})
	if err != nil {
		t.Fatal(err)
	}
	tasks := []WorkspaceTaskRecord{{TaskID: "task-impl", Title: "Implementation complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"}}
	for i := 0; i < 10; i++ {
		tasks = append(tasks, WorkspaceTaskRecord{
			TaskID:      fmt.Sprintf("task-noisy-patchq-%02d", i),
			Title:       fmt.Sprintf("Patch queue noisy coordination %02d", i),
			Description: "Patch queue branch coordination item unrelated to the blocked queue/item.",
			Status:      "PENDING",
			Priority:    "critical",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-svc",
			ProjectLane: "coordination",
			Tags:        []string{"patch_queue", "coordination"},
		})
	}
	tasks = append(tasks, WorkspaceTaskRecord{
		TaskID:      "task-patchq-revision-project-svc-queue-svc-item-blocked",
		Title:       "Unblock integration candidate branch-blocked",
		Description: "Patch queue decision follow-up for the exact blocked item.",
		Status:      "PENDING",
		Priority:    "high",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-svc",
		ProjectLane: "implementation",
		Tags:        []string{"project", "patch-queue", "revision", "blocked", "owner-bound-kind:patch_queue_revision"},
		TaskRequirementsJSON: `{
			"schema":"task_requirements.v1",
			"patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":"revision",
			"queue_id":"queue-svc",
			"item_id":"item-blocked",
			"branch_id":"branch-blocked",
			"head_sha":"` + headSHA + `",
			"required_transition":"project_patch_queue_revision_commit_review_submit"
		}`,
	})
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "epsilon",
		DisplayName: "Epsilon",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: len(tasks)}},
		Tasks:     tasks,
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "reviewer_qa")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "blocked_patch_waiting", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.PromotedRefs) != 0 {
		t.Fatalf("exact blocked revision follow-up should suppress backlog promotion despite compact task noise, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("exact blocked revision follow-up should suppress public writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
}

func TestPatchQueueVigilanceSeesServerDecisionContinuationTaskRequirements(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("epsilon", "Epsilon", "reviewer QA critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-svc",
			ItemID:          "item-accepted",
			ProjectID:       "project-svc",
			BranchID:        "branch-accepted",
			State:           "ACCEPTED",
			HeadSHA:         "abcdef1234567890",
			DecisionSummary: "Accepted repaired candidate.",
			DecidedAt:       now.Add(-45 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:       now.Add(-45 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "epsilon",
		DisplayName: "Epsilon",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: 2}},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-impl", Title: "Implementation complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"},
			{
				TaskID:      "task-server-created-continuation",
				Title:       "Continue patch queue decision",
				Description: "Server-created continuation task.",
				Status:      "PENDING",
				TaskKind:    "EXECUTION",
				ProjectID:   "project-svc",
				ProjectLane: "integration",
				TaskRequirementsJSON: `{
					"schema":"task_requirements.v1",
					"patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1",
					"patch_queue_task_kind":"integration",
					"required_project_role":"INTEGRATOR",
					"queue_id":"queue-svc",
					"item_id":"item-accepted",
					"branch_id":"branch-accepted",
					"head_sha":"abcdef1234567890",
					"required_tool":"project_patch_queue_integrate",
					"required_transition":"project_patch_queue_integrate_then_full_product_verify"
				}`,
			},
		},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "reviewer_qa")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "accepted_patch_waiting", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.PromotedRefs) != 0 {
		t.Fatalf("server continuation task should suppress reviewer integration backlog promotion, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("server continuation task should suppress public writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
}

func TestPatchQueueVigilanceIgnoresMergedAcceptedBranch(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 13, 25, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-merged",
			ProjectID:  "project-svc",
			BranchName: "agent/zeta/merged",
			HeadSHA:    "deadbeef12345678",
			Status:     "MERGED",
			UpdatedAt:  now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-svc",
			ItemID:          "item-merged",
			ProjectID:       "project-svc",
			BranchID:        "branch-merged",
			State:           "ACCEPTED",
			HeadSHA:         "deadbeef12345678",
			DecisionSummary: "Accepted candidate already merged.",
			DecidedAt:       now.Add(-60 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:       now.Add(-60 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "zeta",
		DisplayName: "Zeta",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks:     []WorkspaceTaskRecord{{TaskID: "task-impl", Title: "Implementation complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "integrator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "accepted_patch_waiting", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.PromotedRefs) != 0 {
		t.Fatalf("merged accepted branch should not resurrect integration debt, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("merged accepted branch should not create public writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
}

func TestPatchQueueVigilanceIgnoresIntegratedItem(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 13, 27, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-svc",
			ItemID:          "item-integrated",
			ProjectID:       "project-svc",
			BranchID:        "branch-integrated",
			State:           "INTEGRATED",
			HeadSHA:         "f00dbabe12345678",
			DecisionSummary: "Accepted candidate already has a durable integrated receipt.",
			DecidedAt:       now.Add(-60 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:       now.Add(-60 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "zeta",
		DisplayName: "Zeta",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks:     []WorkspaceTaskRecord{{TaskID: "task-impl", Title: "Implementation complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "integrator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "accepted_patch_waiting", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.PromotedRefs) != 0 {
		t.Fatalf("integrated item should not resurrect integration debt, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("integrated item should not create public writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
}

func TestPatchQueueVigilancePromotesStaleClaimedCandidateToStewardship(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 13, 30, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:        "queue-svc",
			ItemID:         "item-claimed",
			ProjectID:      "project-svc",
			BranchID:       "branch-claimed",
			State:          "CLAIMED",
			ClaimedBy:      "old-integrator",
			ClaimExpiresAt: now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:      now.Add(-60 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "zeta",
		DisplayName: "Zeta",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks:     []WorkspaceTaskRecord{{TaskID: "task-impl", Title: "Implementation complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "integrator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "owner_bound_queue_stale", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected stale claimed item to promote stewardship follow-up, got %+v", result)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "patch-queue-vigilance:patch_queue_convergence_gap:project-svc-queue-svc-item-claimed-branch-claimed")
	if item.Status != "promoted" || item.Kind != "patch_queue_convergence_gap" || item.Meta["target_project_lane"] != "integration" {
		t.Fatalf("expected promoted stale claim stewardship item, got %+v", item)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	server.mu.Unlock()
	if submitted.ProjectID != "project-svc" || submitted.ProjectLane != "integration" {
		t.Fatalf("unexpected stale claim promotion target: %+v", submitted)
	}
}

func TestPatchQueueVigilanceDoesNotPromoteUnexpiredClaim(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 13, 35, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:        "queue-svc",
			ItemID:         "item-claimed",
			ProjectID:      "project-svc",
			BranchID:       "branch-claimed",
			State:          "CLAIMED",
			ClaimedBy:      "active-integrator",
			ClaimExpiresAt: now.Add(30 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:      now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "zeta",
		DisplayName: "Zeta",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Patch queue workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "Service Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks:     []WorkspaceTaskRecord{{TaskID: "task-impl", Title: "Implementation complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-svc", ProjectLane: "implementation"}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "integrator")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "owner_bound_queue_stale", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.PromotedRefs) != 0 {
		t.Fatalf("unexpired claim should not promote stewardship work, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("unexpired claim should not create public writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
}

func TestPatchQueueVigilancePromotesUIAcceptedCandidateToVisualEvidence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("kappa", "Kappa", "adversarial reviewer qa")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 13, 40, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-ui",
			ItemID:          "item-ui",
			ProjectID:       "project-ui",
			BranchID:        "branch-ui",
			State:           "ACCEPTED",
			HeadSHA:         "123456abcdef7890",
			Pathset:         []string{"src/App.tsx", "src/styles.css"},
			DecisionSummary: "Accepted frontend layout update.",
			DecidedAt:       now.Add(-20 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:       now.Add(-20 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "kappa",
		DisplayName: "Kappa",
		OwnerUserID: "owner-1",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "UI workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "UI Tool", Status: "ACTIVE", TaskCount: 1}},
		Tasks:     []WorkspaceTaskRecord{{TaskID: "task-impl", Title: "Frontend MVP complete", Status: "COMPLETED", TaskKind: "EXECUTION", ProjectID: "project-ui", ProjectLane: "implementation"}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "reviewer_qa")
	spec, ok := internalHeartbeatSpecByID(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("patch_queue_vigilance heartbeat missing")
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "integration_candidate_without_smoke", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || len(result.PromotedRefs) != 2 {
		t.Fatalf("expected UI patch queue visual evidence follow-up, got %+v", result)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "patch-queue-vigilance:visual-evidence:project-ui-queue-ui-item-ui-branch-ui-123456abcdef")
	if item.Status != "promoted" || item.Kind != "patch_queue_visual_evidence_gap" || item.Meta["target_project_lane"] != "qa" {
		t.Fatalf("expected promoted visual evidence gap, got %+v", item)
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	server.mu.Unlock()
	if submitted.ProjectID != "project-ui" || submitted.ProjectLane != "qa" {
		t.Fatalf("unexpected visual evidence promotion target: %+v", submitted)
	}
}

func TestPatchQueueVigilanceRedactsDecisionSummarySecrets(t *testing.T) {
	now := time.Date(2026, 5, 15, 13, 50, 0, 0, time.UTC)
	summaries := internalHeartbeatPatchQueueSummaries([]ProjectPatchQueueItemRecord{{
		QueueID:         "queue-secret",
		ItemID:          "item-secret",
		ProjectID:       "project-secret",
		BranchID:        "branch-secret",
		State:           "ACCEPTED",
		DecisionSummary: "Accepted; smoke URL https://example.com/result?token=SECRET_TOKEN and client_secret=SECRET_VALUE",
		UpdatedAt:       now.Format(time.RFC3339Nano),
	}}, nil, 4)
	if len(summaries) != 1 {
		t.Fatalf("expected one patch queue summary, got %+v", summaries)
	}
	if strings.Contains(strings.ToLower(summaries[0].DecisionSummary), "secret") || strings.Contains(summaries[0].DecisionSummary, "TOKEN") || strings.Contains(summaries[0].DecisionSummary, "?token=") {
		t.Fatalf("decision summary should be redacted/sanitized before LLM context: %+v", summaries[0])
	}
}

func TestProjectRoleInitiativeSensorDoesNotPromoteFreshServiceRun(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 15, 10, 20, 0, 0, time.UTC)
	coordinationRaw, err := json.Marshal(ProjectCoordinationRecord{
		ServiceRuns: []ServiceRunRecord{{
			RunID:       "run-fresh",
			WorkspaceID: "ws-1",
			CandidateID: "candidate-pdf",
			ProjectID:   "project-svc",
			Title:       "PDF Merge Utility",
			Status:      "DEPLOYED",
			UpdatedAt:   now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Service workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-svc", WorkspaceID: "ws-1", Title: "PDF Tools", Status: "ACTIVE", TaskCount: 1}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-mvp",
			Title:       "Build PDF merge MVP",
			Status:      "COMPLETED",
			TaskKind:    "EXECUTION",
			ProjectID:   "project-svc",
			ProjectLane: "implementation",
		}},
	}
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectCoordination: coordinationRaw}
	runtime.mu.Unlock()

	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "service_pipeline_fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("fresh service run should not trigger public promotion, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("fresh service run should not submit public work, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "project-initiative:post-mvp-quality-loop:project-svc")
	if item.Status != "open" || item.Meta["finding_promote"] != "false" {
		t.Fatalf("expected only local generic post-MVP sensemaking, got %+v", item)
	}
}

func findBacklogByDedupPrefix(t *testing.T, store *AgentInternalSessionStore, prefix string) AgentPersonalBacklogItem {
	t.Helper()
	for _, item := range store.Snapshot().Backlog {
		if strings.HasPrefix(item.DedupKey, prefix) {
			return item
		}
	}
	t.Fatalf("backlog item with dedup prefix %s not found", prefix)
	return AgentPersonalBacklogItem{}
}

func TestInternalHeartbeatRunnableSurfaceSelectorExtractsSanitizedURLs(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 14, 10, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "iota")
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "iota",
		DisplayName: "Iota",
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "UI run"},
		Docs: []WorkspaceDocRecord{{
			DocKey:    "project.runbook",
			Title:     "Runnable preview",
			Content:   "Preview URL: http://127.0.0.1:5173/?token=SECRET_TOKEN#debug and deployment https://sprite.example/app?secret=DEPLOY_SECRET. Vite internals: http://localhost:5173/@fs/C:/workspace/fixtures/SECRET_PATH/module.js",
			UpdatedBy: "zeta",
			UpdatedAt: now.Format(time.RFC3339Nano),
		}, {
			DocKey:    "private.note",
			Title:     "Private note",
			Content:   "Unrelated secret URL should not be scanned: https://private.example/token/PRIVATE_NOTE_SECRET",
			UpdatedBy: "alpha",
			UpdatedAt: now.Format(time.RFC3339Nano),
		}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-smoke",
			Title:       "Smoke the local web app",
			Description: "Manual browser smoke should use http://localhost:3000/workflow?api_key=TASK_SECRET.",
			Status:      "OPEN",
			ProjectLane: "qa",
		}},
		RecentUpdates: []AgentUpdateRecord{{
			UpdateID:    "upd-surface",
			AgentID:     "theta",
			UpdateType:  "smoke",
			Summary:     "Latest public preview at https://preview.example/result?run=RUN_SECRET",
			PayloadJSON: `{"private_url":"http://localhost:9999/private?token=PAYLOAD_ONLY_SECRET"}`,
			CreatedAt:   now.Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()

	spec := AgentHeartbeatSpec{
		ID:               "surface_probe",
		Kind:             "browser_critic",
		Cadence:          "every_10m",
		Priority:         20,
		Locks:            []string{"local_only"},
		ToolSuites:       []string{"workspace_docs_read"},
		ContextSelectors: []string{"runnable_surface"},
		OutputContracts:  []string{"local_memory"},
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "project_has_ui_surface", now)
	payload := findSelectorPayload(t, packet, "runnable_surface")
	if payload.Status != "hydrated" || len(payload.Surfaces) != 5 {
		t.Fatalf("expected five sanitized runnable surface candidates, got %+v", payload)
	}
	gotURLs := map[string]bool{}
	for _, surface := range payload.Surfaces {
		gotURLs[surface.URL] = true
		if strings.Contains(surface.URL, "?") || strings.Contains(surface.URL, "#") || strings.Contains(surface.URL, "SECRET") {
			t.Fatalf("surface URL should be sanitized, got %+v", surface)
		}
		if surface.Localhost && surface.Confidence > 55 {
			t.Fatalf("localhost surface should stay unverified and lower-confidence before browser marker checks, got %+v", surface)
		}
		if !surface.VerificationRequired {
			t.Fatalf("surface candidates should require browser/product verification, got %+v", surface)
		}
	}
	for _, want := range []string{
		"http://127.0.0.1:5173/",
		"http://localhost:5173",
		"http://localhost:3000/workflow",
		"https://sprite.example/app",
		"https://preview.example/result",
	} {
		if !gotURLs[want] {
			t.Fatalf("missing sanitized surface URL %q in %+v", want, payload.Surfaces)
		}
	}
	rawPacket, _ := json.Marshal(packet)
	prompt := renderInternalHeartbeatPrompt(packet)
	for _, forbidden := range []string{
		"SECRET_TOKEN",
		"DEPLOY_SECRET",
		"TASK_SECRET",
		"RUN_SECRET",
		"PAYLOAD_ONLY_SECRET",
		"SECRET_PATH",
		"PRIVATE_NOTE_SECRET",
		"C:/Users",
		"localhost:9999/private",
		"private.example",
	} {
		if strings.Contains(string(rawPacket), forbidden) || strings.Contains(prompt, forbidden) {
			t.Fatalf("surface selector leaked private content %q:\n%s", forbidden, prompt)
		}
	}
}

func TestInternalHeartbeatRunnableSurfaceSelectorReportsMissingSurface(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "iota")
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota"}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "UI run"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.visual-evidence",
			Title:   "Visual evidence request",
			Content: "UI project exists, but no preview URL or deployment endpoint has been published.",
		}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-ui-review",
			Title:       "Review the UI once a preview exists",
			Description: "No runnable surface is currently known.",
			Status:      "OPEN",
			ProjectLane: "qa",
		}},
	}
	runtime.mu.Unlock()

	spec := AgentHeartbeatSpec{
		ID:               "surface_probe",
		Kind:             "browser_critic",
		Cadence:          "every_10m",
		Priority:         20,
		Locks:            []string{"local_only"},
		ToolSuites:       []string{"workspace_docs_read"},
		ContextSelectors: []string{"runnable_surface"},
		OutputContracts:  []string{"local_memory"},
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "project_has_ui_surface", time.Date(2026, 5, 14, 14, 15, 0, 0, time.UTC))
	payload := findSelectorPayload(t, packet, "runnable_surface")
	if payload.Status != "missing_surface" || len(payload.Surfaces) != 0 {
		t.Fatalf("expected explicit missing runnable surface state, got %+v", payload)
	}
	if !strings.Contains(payload.Summary, "missing surface") && !strings.Contains(payload.Summary, "No runnable URL") {
		t.Fatalf("missing surface summary should prevent fake visual pass, got %+v", payload)
	}
}

func TestVisualAuditHeartbeatEnrichesVerifiedBrowserPreflight(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			t.Fatalf("browser preflight should receive sanitized URL without query, got %q", r.URL.String())
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body><main>Sprite Export Tool converts icons.</main></body></html>`))
	}))
	defer server.Close()

	now := time.Date(2026, 5, 14, 15, 0, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "iota")
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota"}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:    "project.runbook",
			Title:     "Sprite Export Tool runnable surface",
			Content:   "Preview " + server.URL + "?token=SECRET_TOKEN#debug",
			UpdatedBy: "zeta",
			UpdatedAt: now.Format(time.RFC3339Nano),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "project_has_ui_surface", now)
	runtime.enrichInternalHeartbeatContextPacket(context.Background(), &packet, spec, policy, now)

	payload := findSelectorPayload(t, packet, "runnable_surface")
	if payload.Status != "surface_preflight_verified" || len(payload.BrowserProbes) != 1 {
		t.Fatalf("expected verified typed browser preflight, got %+v", payload)
	}
	if payload.VisualAudit == nil || payload.VisualAudit.Status != "needs_visual_evidence" || payload.VisualAudit.VisualVerdictAllowed {
		t.Fatalf("verified preflight without visual packet should still require visual evidence, got %+v", payload.VisualAudit)
	}
	if len(packet.RequiredToolArtifacts) != 1 || packet.RequiredToolArtifacts[0].Tool != "browser_visual_probe" || !packet.RequiredToolArtifacts[0].RequiredNow {
		t.Fatalf("verified runnable surface should require browser visual probe artifact, got %+v", packet.RequiredToolArtifacts)
	}
	if prompt := renderInternalHeartbeatPrompt(packet); !strings.Contains(prompt, "Required tool artifact contract") || !strings.Contains(prompt, "build/source/doc review cannot replace") {
		t.Fatalf("prompt should make required tool artifact contract explicit, got:\n%s", prompt)
	}
	if len(payload.VisualAudit.Viewports) < 2 || len(payload.VisualAudit.Scenarios) < 3 {
		t.Fatalf("visual audit plan should include desktop/narrow viewports and state scenarios, got %+v", payload.VisualAudit)
	}
	if !payload.VisualAudit.EvidenceRequired || len(payload.VisualAudit.EvidenceRequests) < 6 {
		t.Fatalf("visual audit plan should request evidence matrix, got %+v", payload.VisualAudit)
	}
	for _, request := range payload.VisualAudit.EvidenceRequests {
		if request.SurfaceURL == "" || strings.Contains(request.SurfaceURL, "?") || strings.Contains(request.SurfaceURL, "#") || strings.Contains(request.SurfaceURL, "SECRET_TOKEN") {
			t.Fatalf("evidence request should carry sanitized surface URL, got %+v", request)
		}
		if request.Kind != "screenshot" || request.DimensionID == "" || request.StateID == "" || request.Width <= 0 || request.Height <= 0 || request.ArtifactRefHint == "" {
			t.Fatalf("evidence request should include kind, dimension/state, dimensions, and artifact hint, got %+v", request)
		}
	}
	probe := payload.BrowserProbes[0]
	if !probe.ProductMarkerVerified || probe.MatchedMarker == "" || !probe.VisualVerificationRequired {
		t.Fatalf("probe should verify product marker while still requiring visual evidence, got %+v", probe)
	}
	if strings.Contains(probe.URL, "?") || strings.Contains(probe.URL, "#") || strings.Contains(probe.URL, "SECRET_TOKEN") {
		t.Fatalf("probe URL should stay sanitized, got %+v", probe)
	}
	rawPacket, _ := json.Marshal(packet)
	prompt := renderInternalHeartbeatPrompt(packet)
	for _, forbidden := range []string{"SECRET_TOKEN"} {
		if strings.Contains(string(rawPacket), forbidden) || strings.Contains(prompt, forbidden) {
			t.Fatalf("browser preflight leaked or overclaimed %q:\n%s", forbidden, prompt)
		}
	}
}

func TestVisualAuditHeartbeatRecordsLocalBacklogForVerifiedSurfaceWithoutVisualPacket(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body>Sprite Export Tool</body></html>`))
	}))
	defer server.Close()

	now := time.Date(2026, 5, 14, 15, 3, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + server.URL,
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "backlog_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("verified preflight without visual packet should become local backlog only, got %+v", result)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "visual:visual-evidence-required")
	if item.Kind != "visual_acceptance_gap" || len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 {
		t.Fatalf("unexpected visual evidence backlog item: %+v", item)
	}
	for _, want := range []string{"viewport:desktop", "viewport:narrow", "scenario:initial_state", "scenario:primary_flow", "scenario:result_state", "evidence_required", "evidence:screenshot:desktop:initial_state", "evidence:screenshot:narrow:result_state", "browser_probe:verified_product_marker"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("visual evidence backlog missing %q in %+v", want, item.EvidenceRefs)
		}
	}
	if item.Meta["finding_source"] != internalHeartbeatVisualSensorSource || item.Meta["finding_promote"] != "true" {
		t.Fatalf("visual evidence sensor finding should be promotion-eligible when a bounded client is configured, got meta %+v", item.Meta)
	}
}

func TestVisualAuditHeartbeatRecordsActionRequestWhenRequiredToolArtifactMissing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body>Sprite Export Tool</body></html>`))
	}))
	defer server.Close()

	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"source/build inspection did not find anything"}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota", Workdir: workdir}, llm)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + server.URL,
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", time.Date(2026, 5, 14, 15, 4, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "backlog_recorded" {
		t.Fatalf("missing required tool artifact should record backlog/action request, got %+v", result)
	}
	var found AgentPersonalBacklogItem
	for _, item := range runtime.internalSessions.Snapshot().Backlog {
		if item.Kind == "heartbeat_action_request" && strings.Contains(item.Title, "browser_visual_probe") {
			found = item
			break
		}
	}
	if strings.TrimSpace(found.ItemID) == "" {
		t.Fatalf("expected heartbeat action request for missing browser_visual_probe artifact, backlog=%+v", runtime.internalSessions.Snapshot().Backlog)
	}
	if found.Meta["action_request_capability"] != "browser_screenshot" || found.Meta["action_request_tool_suite"] != "screenshot_capture" || found.Meta["action_requires_task_loop"] != "true" {
		t.Fatalf("missing artifact action request should preserve capability routing metadata, got %+v", found.Meta)
	}
	for _, want := range []string{"required_tool:browser_visual_probe", "missing_tool_call"} {
		if !containsAnatomyTestString(found.EvidenceRefs, want) {
			t.Fatalf("missing artifact action request lacks %q in %+v", want, found.EvidenceRefs)
		}
	}
	if len(llm.calls) == 0 || !strings.Contains(llm.calls[0][1].Content, "Required tool artifact contract") {
		t.Fatalf("LLM prompt should expose required artifact contract, calls=%+v", llm.calls)
	}
}

func TestVisualAuditHeartbeatRequiredToolArtifactRejectsWrongContractTrace(t *testing.T) {
	packet, policy, messages := visualRequiredToolArtifactEnforcementFixture()
	result := enforceInternalHeartbeatRequiredToolArtifacts(packet, policy, messages, []ToolLoopToolResult{{
		ToolCallID:      "call-probe",
		ToolName:        "browser_visual_probe",
		Status:          "pass",
		ContractVersion: "other_contract_v1",
	}}, InternalHeartbeatLocalResult{ContractVersion: internalHeartbeatLocalResultContractVersion, Outcome: "no_action", Summary: "no action"})

	if result.Outcome != "backlog_recorded" || len(result.ActionRequests) != 1 {
		t.Fatalf("wrong contract should produce one action request, got %+v", result)
	}
	request := result.ActionRequests[0]
	if !strings.Contains(request.Reason, "browser_visual_probe_result_v1") || !containsAnatomyTestString(request.EvidenceRefs, "missing_contract_version") {
		t.Fatalf("wrong-contract request should preserve contract evidence, got %+v", request)
	}
}

func TestVisualAuditHeartbeatRequiredToolArtifactRejectsWarnStatus(t *testing.T) {
	packet, policy, messages := visualRequiredToolArtifactEnforcementFixture()
	path := writeRequiredToolArtifactTraceFile(t)
	result := enforceInternalHeartbeatRequiredToolArtifacts(packet, policy, messages, []ToolLoopToolResult{{
		ToolCallID:      "call-probe",
		ToolName:        "browser_visual_probe",
		Status:          "warn",
		ContractVersion: "browser_visual_probe_result_v1",
		ArtifactPaths:   []string{path},
		ArtifactHashes:  []string{"sha256:abc"},
		ScenarioIDs:     []string{"visual_audit_probe"},
		StateIDs:        []string{"observed_surface"},
		ViewportIDs:     []string{"desktop"},
	}}, InternalHeartbeatLocalResult{ContractVersion: internalHeartbeatLocalResultContractVersion, Outcome: "no_action", Summary: "no action"})

	if result.Outcome != "backlog_recorded" || len(result.ActionRequests) != 1 {
		t.Fatalf("warn status should produce one action request, got %+v", result)
	}
	request := result.ActionRequests[0]
	if !strings.Contains(request.Reason, "warning status") || !containsAnatomyTestString(request.EvidenceRefs, "status:warn") {
		t.Fatalf("warn-status request should preserve status evidence, got %+v", request)
	}
}

func TestVisualAuditHeartbeatRequiredToolArtifactRejectsMissingHashAndStalePath(t *testing.T) {
	packet, policy, messages := visualRequiredToolArtifactEnforcementFixture()
	result := enforceInternalHeartbeatRequiredToolArtifacts(packet, policy, messages, []ToolLoopToolResult{{
		ToolCallID:      "call-probe",
		ToolName:        "browser_visual_probe",
		Status:          "pass",
		ContractVersion: "browser_visual_probe_result_v1",
		ArtifactPaths:   []string{filepath.Join(t.TempDir(), "missing.png")},
		ScenarioIDs:     []string{"visual_audit_probe"},
		StateIDs:        []string{"observed_surface"},
		ViewportIDs:     []string{"desktop"},
	}}, InternalHeartbeatLocalResult{ContractVersion: internalHeartbeatLocalResultContractVersion, Outcome: "no_action", Summary: "no action"})

	if result.Outcome != "backlog_recorded" || len(result.ActionRequests) != 1 {
		t.Fatalf("missing hash should produce one action request, got %+v", result)
	}
	if !containsAnatomyTestString(result.ActionRequests[0].EvidenceRefs, "missing_artifact_hash") {
		t.Fatalf("expected missing hash evidence, got %+v", result.ActionRequests[0])
	}

	result = enforceInternalHeartbeatRequiredToolArtifacts(packet, policy, messages, []ToolLoopToolResult{{
		ToolCallID:      "call-probe",
		ToolName:        "browser_visual_probe",
		Status:          "pass",
		ContractVersion: "browser_visual_probe_result_v1",
		ArtifactPaths:   []string{filepath.Join(t.TempDir(), "missing.png")},
		ArtifactHashes:  []string{"sha256:abc"},
		ScenarioIDs:     []string{"visual_audit_probe"},
		StateIDs:        []string{"observed_surface"},
		ViewportIDs:     []string{"desktop"},
	}}, InternalHeartbeatLocalResult{ContractVersion: internalHeartbeatLocalResultContractVersion, Outcome: "no_action", Summary: "no action"})
	if !containsAnatomyTestString(result.ActionRequests[0].EvidenceRefs, "stale_artifact_path") {
		t.Fatalf("expected stale artifact path evidence, got %+v", result.ActionRequests[0])
	}
}

func TestVisualAuditHeartbeatRequiredToolArtifactAcceptsPassTrace(t *testing.T) {
	packet, policy, messages := visualRequiredToolArtifactEnforcementFixture()
	path := writeRequiredToolArtifactTraceFile(t)
	result := enforceInternalHeartbeatRequiredToolArtifacts(packet, policy, messages, []ToolLoopToolResult{{
		ToolCallID:      "call-probe",
		ToolName:        "browser_visual_probe",
		Status:          "pass",
		ContractVersion: "browser_visual_probe_result_v1",
		ArtifactPaths:   []string{path},
		ArtifactHashes:  []string{"sha256:abc"},
		ScenarioIDs:     []string{"visual_audit_probe"},
		StateIDs:        []string{"observed_surface"},
		ViewportIDs:     []string{"desktop"},
	}}, InternalHeartbeatLocalResult{ContractVersion: internalHeartbeatLocalResultContractVersion, Outcome: "no_action", Summary: "no action"})

	if result.Outcome != "no_action" || len(result.ActionRequests) != 0 {
		t.Fatalf("valid pass trace should satisfy required tool artifact, got %+v", result)
	}
}

func visualRequiredToolArtifactEnforcementFixture() (InternalHeartbeatContextPacket, InternalHeartbeatExecutionPolicy, []Message) {
	packet := InternalHeartbeatContextPacket{
		HeartbeatID: "visual_product_audit",
		RequiredToolArtifacts: []InternalHeartbeatRequiredToolArtifact{{
			Tool:            "browser_visual_probe",
			ContractVersion: "browser_visual_probe_result_v1",
			Capability:      "browser_screenshot",
			ToolSuite:       "screenshot_capture",
			When:            "runnable_surface_present",
			RequiredNow:     true,
			Reason:          "runnable surface present",
		}},
	}
	policy := InternalHeartbeatExecutionPolicy{HeartbeatID: "visual_product_audit", AllowTaskSubmit: true}
	messages := []Message{{
		Role: "assistant",
		ToolCalls: []ToolCall{{
			ID:       "call-probe",
			Type:     "function",
			Function: FunctionCall{Name: "browser_visual_probe", Arguments: `{}`},
		}},
	}}
	return packet, policy, messages
}

func writeRequiredToolArtifactTraceFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "desktop.png")
	if err := os.WriteFile(path, []byte("png-ish"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestVisualAuditHeartbeatPromotesSensorVisualEvidenceGapWithBoundedAuthority(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	surface := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body>Sprite Export Tool</body></html>`))
	}))
	defer surface.Close()
	rpc := newBacklogPromotionTestServer(t, false)
	defer rpc.Close()

	now := time.Date(2026, 5, 14, 15, 3, 15, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		RhizomeRPC:  rpc.URL,
		WorkspaceID: "ws-1",
		AgentID:     "iota",
		DisplayName: "Iota",
		OwnerUserID: "operator",
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + surface.URL,
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	if policy.LocalOnly || !policy.AllowTaskSubmit {
		t.Fatalf("default visual audit heartbeat should have bounded public promotion authority, got %+v", policy)
	}
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "backlog_promoted" || len(result.PromotedRefs) == 0 {
		t.Fatalf("verified visual evidence gap should promote one bounded task, got %+v", result)
	}
	if rpc.submitTaskCount() != 1 || rpc.putDocCount() != 1 {
		t.Fatalf("expected one promoted task/doc, submit=%d putDoc=%d", rpc.submitTaskCount(), rpc.putDocCount())
	}
	rpc.mu.Lock()
	task := rpc.lastTaskIn
	doc := rpc.lastDocIn
	rpc.mu.Unlock()
	if task.ProjectLane != "qa" || task.Priority == "" || !containsAnatomyTestString(task.Tags, "visual_product_audit") {
		t.Fatalf("promoted visual task should be scoped as qa heartbeat work, got %+v", task)
	}
	if !strings.Contains(task.Title, "Verified UI surface still needs visual acceptance evidence") || !strings.Contains(doc.Content, "browser_probe:verified_product_marker") || !strings.Contains(doc.Content, "evidence:screenshot:desktop:initial_state") {
		t.Fatalf("promoted visual task/doc should preserve actionable evidence, task=%+v doc=%+v", task, doc)
	}
}

func TestInternalHeartbeatPromotionCandidatesRequireSensorSourceForVisualAudit(t *testing.T) {
	spec := defaultVisualProductAuditHeartbeat()
	base := AgentPersonalBacklogItem{
		ItemID:        "item-1",
		DedupKey:      "visual:test",
		HeartbeatID:   "visual_product_audit",
		Kind:          "visual_acceptance_gap",
		Status:        "open",
		Title:         "Visual gap",
		Summary:       "Visual gap",
		Score:         90,
		LastSessionID: "session-1",
		Meta: map[string]string{
			"finding_promote":          "true",
			"policy_allow_task_submit": "true",
			"policy_local_only":        "false",
		},
	}
	if got := internalHeartbeatPromotionCandidates([]AgentPersonalBacklogItem{base}, "session-1", spec, 70); len(got) != 0 {
		t.Fatalf("visual audit LLM-like backlog item without typed sensor source must not promote, got %+v", got)
	}
	base.Meta["finding_source"] = internalHeartbeatVisualSensorSource
	if got := internalHeartbeatPromotionCandidates([]AgentPersonalBacklogItem{base}, "session-1", spec, 70); len(got) != 1 {
		t.Fatalf("typed visual sensor finding should remain promotion-eligible, got %+v", got)
	}
}

func TestVisualAuditHeartbeatUsesConfiguredVisualAuditContract(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body>Sprite Export Tool</body></html>`))
	}))
	defer server.Close()

	now := time.Date(2026, 5, 14, 15, 3, 30, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + server.URL,
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	spec.VisualAudit = &AgentHeartbeatVisualAuditSpec{
		Viewports: []AgentHeartbeatVisualAuditViewportSpec{{ID: "wide", Width: 1600, Height: 1000, Purpose: "wide production viewport"}},
		Scenarios: []AgentHeartbeatVisualAuditScenarioSpec{{
			ID:                   "export_flow",
			Label:                "Export flow",
			RequiredState:        "after export",
			ScreenshotRequired:   boolPtr(true),
			RealUserQuestion:     "Can the user download the generated bundle?",
			ExpectedEvidenceKind: "export screenshot",
		}},
		Checks:               []string{"text_fit", "export_controls"},
		ArtifactRequirements: []string{"wide and export screenshots are locally decodable"},
	}
	spec = normalizeAgentHeartbeatSpec(spec)
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "backlog_recorded" {
		t.Fatalf("custom visual contract should still create local evidence gap, got %+v", result)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "visual:visual-evidence-required")
	for _, want := range []string{"viewport:wide", "scenario:export_flow", "evidence_required", "evidence:screenshot:wide:export_flow"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("configured visual contract missing evidence ref %q in %+v", want, item.EvidenceRefs)
		}
	}
	for _, forbidden := range []string{"viewport:desktop", "scenario:initial_state", "evidence:screenshot:desktop:initial_state"} {
		if containsAnatomyTestString(item.EvidenceRefs, forbidden) {
			t.Fatalf("configured visual contract should override fallback ref %q in %+v", forbidden, item.EvidenceRefs)
		}
	}
}

func TestVisualAuditHeartbeatDoesNotRecordEvidenceGapWhenVisualPacketExists(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body>Sprite Export Tool</body></html>`))
	}))
	defer server.Close()

	now := time.Date(2026, 5, 14, 15, 4, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + server.URL,
		}, {
			DocKey:  "project.visual_acceptance",
			Title:   "Visual Acceptance Packet",
			Content: completeStructuredVisualPacketWithRealScreenshots(t),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "typed_policy_recorded" {
		t.Fatalf("complete visual packet should avoid sensor backlog without LLM, got %+v", result)
	}
	if backlogHasDedup(runtime.internalSessions, "visual:visual-evidence-required") {
		t.Fatalf("complete visual packet should suppress visual evidence backlog")
	}
}

func TestVisualAuditHeartbeatRejectsTextOnlyVisualPacketWithMissingScreenshots(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body>Sprite Export Tool</body></html>`))
	}))
	defer server.Close()

	now := time.Date(2026, 5, 14, 15, 4, 30, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + server.URL,
		}, {
			DocKey:  "project.visual_acceptance",
			Title:   "Visual Acceptance Packet",
			Content: completeStructuredVisualPacket(),
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(runtime.internalSessions, spec, policy, "project_has_ui_surface", now)
	runtime.enrichInternalHeartbeatContextPacket(context.Background(), &packet, spec, policy, now)
	payload := findSelectorPayload(t, packet, "runnable_surface")
	if payload.VisualAudit == nil || payload.VisualAudit.Status != "needs_visual_evidence" || payload.VisualAudit.VisualVerdictAllowed {
		t.Fatalf("text-only visual packet with missing screenshots must not satisfy heartbeat visual evidence, got %+v", payload.VisualAudit)
	}
	if !containsAnySignal(strings.Join(payload.VisualAudit.MissingEvidence, "\n"), []string{"local screenshot artifact missing"}) {
		t.Fatalf("expected missing screenshot artifact evidence, got %+v", payload.VisualAudit.MissingEvidence)
	}
	if !payload.VisualAudit.EvidenceRequired || len(payload.VisualAudit.EvidenceRequests) == 0 {
		t.Fatalf("missing screenshot artifacts should request evidence capture, got %+v", payload.VisualAudit)
	}

	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "backlog_recorded" {
		t.Fatalf("text-only visual packet should create local backlog gap, got %+v", result)
	}
	if !backlogHasDedup(runtime.internalSessions, "visual:visual-evidence-required") {
		t.Fatalf("expected local visual evidence gap backlog")
	}
}

func TestVisualAuditHeartbeatRecordsBlockingFindingForBadScreenshotPacket(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Sprite Export Tool</title></head><body>Sprite Export Tool</body></html>`))
	}))
	defer server.Close()

	now := time.Date(2026, 5, 14, 15, 4, 45, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	screenshotDir := filepath.Join(t.TempDir(), "screenshot dir with spaces")
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		writeVisualAcceptanceLowContrastHeroPNG(t, filepath.Join(screenshotDir, "initial-empty-desktop.png")),
		writeVisualAcceptanceTestPNG(t, filepath.Join(screenshotDir, "mobile-initial-empty.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(screenshotDir, "primary-upload-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(screenshotDir, "result-export-desktop.png"), false),
	)
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + server.URL,
		}, {
			DocKey:  "project.visual_acceptance",
			Title:   "Visual Acceptance Packet",
			Content: packet,
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "backlog_recorded" {
		t.Fatalf("bad screenshot visual packet should create local blocking backlog, got %+v", result)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "visual:blocking-evidence")
	if item.Kind != "visual_blocking_finding" || item.Score < 85 {
		t.Fatalf("unexpected blocking screenshot backlog item: %+v", item)
	}
	if !containsAnySignal(strings.Join(item.EvidenceRefs, "\n"), []string{"blocking:screenshot first viewport low-contrast composition"}) {
		t.Fatalf("blocking backlog should name screenshot inspection failure, got %+v", item.EvidenceRefs)
	}
	joinedEvidence := strings.Join(item.EvidenceRefs, "\n")
	if strings.Contains(joinedEvidence, screenshotDir) || strings.Contains(joinedEvidence, `C:\`) || strings.Contains(strings.ToLower(joinedEvidence), "/users/") {
		t.Fatalf("blocking visual evidence should not leak local screenshot paths, got %q", joinedEvidence)
	}
	if len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 {
		t.Fatalf("blocking visual finding should remain local-only without public refs, got %+v", item)
	}
}

func TestInternalHeartbeatSanitizeVisualEvidenceTextRedactsLocalImagePathsWithSpaces(t *testing.T) {
	input := `screenshot artifact unreadable: C:\Users\Jane Doe\AppData\Local\Temp\bad hero.png and /Users/Jane Doe/tmp/mobile bad.webp plus @fs/C:/Users/Jane Doe/project/screen shot.png plus \\server\share\bad capture.jpg`
	got := internalHeartbeatSanitizeVisualEvidenceText(input)
	if strings.Count(got, "<local-path>") != 4 {
		t.Fatalf("expected all four local paths to be redacted, got %q", got)
	}
	for _, leaked := range []string{"Jane Doe", "Users", "AppData", "server", "share", "bad hero", "mobile bad", "screen shot", "bad capture"} {
		if strings.Contains(strings.ToLower(got), strings.ToLower(leaked)) {
			t.Fatalf("sanitized visual evidence leaked %q in %q", leaked, got)
		}
	}
	if !strings.Contains(got, "screenshot artifact unreadable") {
		t.Fatalf("sanitizer should preserve the actionable finding text, got %q", got)
	}
}

func TestVisualAuditHeartbeatRejectsWrongLocalhostProduct(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Unrelated Admin Console</title></head><body>Nothing about the target product.</body></html>`))
	}))
	defer server.Close()

	now := time.Date(2026, 5, 14, 15, 5, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "iota")
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota"}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.runbook",
			Title:   "Sprite Export Tool runnable surface",
			Content: "Preview " + server.URL,
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "project_has_ui_surface", now)
	runtime.enrichInternalHeartbeatContextPacket(context.Background(), &packet, spec, policy, now)

	payload := findSelectorPayload(t, packet, "runnable_surface")
	if payload.Status != "surface_preflight_unverified" || len(payload.BrowserProbes) != 1 {
		t.Fatalf("expected wrong-product surface to remain unverified, got %+v", payload)
	}
	probe := payload.BrowserProbes[0]
	if probe.ProductMarkerVerified || probe.Status != "loaded_unverified" || !strings.Contains(probe.Error, "no product marker") {
		t.Fatalf("wrong localhost product should not be accepted as visual evidence, got %+v", probe)
	}
	if !probe.Localhost || !probe.VisualVerificationRequired {
		t.Fatalf("localhost probe should stay marked local and visually unverified, got %+v", probe)
	}
}

func TestVisualAuditHeartbeatRecordsLocalBacklogForMissingSurfaceWithoutLLM(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 14, 15, 8, 0, 0, time.UTC)
	workdir := t.TempDir()
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX evil reality critic")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "iota", DisplayName: "Iota", Workdir: workdir}, nil)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Sprite Export Tool"},
		Docs: []WorkspaceDocRecord{{
			DocKey:  "project.visual-evidence",
			Title:   "Visual evidence request",
			Content: "UI project exists, but no preview URL or deployment endpoint has been published.",
		}},
	}
	runtime.mu.Unlock()

	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "project_has_ui_surface", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "backlog_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("missing surface should become local backlog only, got %+v", result)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "visual:runnable-surface-missing")
	if item.HeartbeatID != "visual_product_audit" || item.Kind != "visual_surface_gap" || len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 {
		t.Fatalf("unexpected missing-surface backlog item: %+v", item)
	}
}

func TestVisualAuditHeartbeatBrowserProbeDoesNotFollowSecretRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://localhost:9999/private?token=SECRET_REDIRECT", http.StatusFound)
	}))
	defer server.Close()

	probe := internalHeartbeatProbeRunnableSurface(context.Background(), InternalHeartbeatRunnableSurface{URL: server.URL + "?api_key=SECRET_QUERY"}, []string{"Sprite Export Tool|test"}, time.Date(2026, 5, 14, 15, 10, 0, 0, time.UTC))
	if probe.Status != "http_error" || probe.HTTPStatus != http.StatusFound {
		t.Fatalf("redirect should be retained as an unverified HTTP error, got %+v", probe)
	}
	raw, _ := json.Marshal(probe)
	for _, forbidden := range []string{"SECRET_QUERY", "SECRET_REDIRECT", "localhost:9999/private"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("redirect probe leaked secret target %q in %+v", forbidden, probe)
		}
	}
}

func TestVisualAuditHeartbeatDoesNotEnableGenericBrowserTools(t *testing.T) {
	spec := defaultVisualProductAuditHeartbeat()
	if spec.MaxToolIterations <= 0 {
		t.Fatalf("visual audit heartbeat should enable a bounded typed browser tool loop, got %+v", spec)
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	if !policy.AllowsTool("browser_visual_probe") || !internalHeartbeatReadOnlyToolLoopAllows(policy, "browser_visual_probe") {
		t.Fatalf("browser_visual_probe should be available to visual audit heartbeat, policy=%+v", policy)
	}
	for _, toolName := range []string{"browser_click", "browser_type", "chrome_navigate", "browser_screenshot", "console_read"} {
		if policy.AllowsTool(toolName) || internalHeartbeatReadOnlyToolLoopAllows(policy, toolName) {
			t.Fatalf("%s should stay blocked before typed browser-surface execution, policy=%+v", toolName, policy)
		}
	}
}

func TestInternalHeartbeatAllowsInstalledBundleByManifestSuite(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&InstalledToolBundleTool{
		workdir: t.TempDir(),
		dir:     t.TempDir(),
		manifest: InstalledToolBundleManifest{
			Name:             "custom_visual_probe",
			Description:      "custom screenshot capture",
			Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
			Parameters:       map[string]any{"type": "object"},
			CapabilitySuites: []string{"screenshot_capture"},
		},
	})
	policy := internalHeartbeatExecutionPolicy(AgentHeartbeatSpec{
		ID:                "custom_visual_audit",
		Kind:              "browser_critic",
		ToolSuites:        []string{"screenshot_capture"},
		MaxToolIterations: 1,
	})

	if !internalHeartbeatReadOnlyToolLoopAllowsWithRegistry(policy, registry, "custom_visual_probe") {
		t.Fatalf("manifest-declared screenshot_capture bundle should be available to heartbeat policy")
	}
	if !internalHeartbeatRegistryToolAllowedByManifest(policy, registry, "custom_visual_probe") {
		t.Fatalf("manifest suite should satisfy tool policy")
	}
	if internalHeartbeatReadOnlyToolLoopAllowsWithRegistry(policy, registry, "unregistered_visual_probe") {
		t.Fatalf("unregistered bundle must not be allowed from suite name alone")
	}
}

func TestInternalHeartbeatContextPacketReportsUnknownSelectors(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	spec := AgentHeartbeatSpec{
		ID:               "custom_reflection",
		Kind:             "metacognition",
		Cadence:          "every_15m",
		Locks:            []string{"local_only"},
		ToolSuites:       []string{"memory_and_docs_read"},
		ContextSelectors: []string{"unsupported_future_selector"},
		OutputContracts:  []string{"local_memory"},
	}
	policy := internalHeartbeatExecutionPolicy(spec)
	runtime := NewRuntime(RuntimeConfig{WorkspaceID: "ws-1", AgentID: "sigma", DisplayName: "Sigma"}, nil)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "test", time.Date(2026, 5, 14, 14, 5, 0, 0, time.UTC))
	payload := findSelectorPayload(t, packet, "unsupported_future_selector")
	if payload.Status != "not_hydrated" {
		t.Fatalf("unknown selector should be explicit not_hydrated, got %+v", payload)
	}
}

func TestInternalHeartbeatResultMaterializesPersonalBacklogOnly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	spec := defaultVisualProductAuditHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	session, err := store.BeginHeartbeatSession(spec, "digest", "post_mvp_no_visual_evidence", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteSession(session.SessionID, "completed", "finding_recorded", "Visible layout failure found", nil, nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	session, _ = internalSessionRecordByID(store.Snapshot(), session.SessionID)

	first, err := materializeInternalHeartbeatResultToBacklog(store, session, spec, policy, InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		Summary:         "The product still has an obvious first-viewport layout failure.",
		BacklogItems: []InternalHeartbeatFinding{
			{
				DedupKey:     "visual:first-viewport-overlap",
				Kind:         "visual_finding",
				Title:        "First viewport typography overlaps controls",
				Summary:      "The heading and drop zone text collide in the first viewport.",
				Score:        90,
				EvidenceRefs: []string{"screenshot:desktop"},
				Promote:      true,
				Reason:       "visible user harm",
			},
		},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("expected one backlog item, got %+v", first)
	}
	if _, err := materializeInternalHeartbeatResultToBacklog(store, session, spec, policy, InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "backlog_recorded",
		BacklogItems: []InternalHeartbeatFinding{
			{
				DedupKey:     "visual:first-viewport-overlap",
				Title:        "Duplicate visible viewport overlap",
				Summary:      "The first viewport heading, drop copy, and controls still collide on desktop and narrow viewport.",
				Score:        80,
				EvidenceRefs: []string{"screenshot:narrow"},
				Promote:      true,
			},
		},
	}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	state := store.Snapshot()
	if len(state.Backlog) != 1 {
		t.Fatalf("expected duplicate heartbeat finding to merge into one personal backlog item, got %+v", state.Backlog)
	}
	item := state.Backlog[0]
	if item.Status != "open" || item.Score != 90 || item.SeenCount < 2 {
		t.Fatalf("unexpected merged backlog item: %+v", item)
	}
	for _, ref := range []string{"screenshot:desktop", "screenshot:narrow", "internal_session:" + session.SessionID} {
		if !containsTrimmedString(item.EvidenceRefs, ref) {
			t.Fatalf("expected evidence ref %s in %+v", ref, item.EvidenceRefs)
		}
	}
	if len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 || len(item.PromotionRefs) != 0 {
		t.Fatalf("local heartbeat materialization must not create public refs, got %+v", item)
	}
	if item.Meta["policy_local_only"] != "false" || item.Meta["policy_allow_task_submit"] != "true" || item.Meta["finding_promote"] != "true" || item.Meta["finding_source"] != "" || item.LastSessionID != session.SessionID {
		t.Fatalf("expected policy/session metadata, got %+v", item)
	}
}

func TestInternalHeartbeatActionRequestsMaterializeAsLocalBacklog(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	session, err := store.BeginHeartbeatSession(spec, "digest", "needs_unavailable_evidence", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteSession(session.SessionID, "completed", "blocked", "Needs a capability outside this heartbeat.", nil, nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	session, _ = internalSessionRecordByID(store.Snapshot(), session.SessionID)

	items, err := materializeInternalHeartbeatResultToBacklog(store, session, spec, policy, InternalHeartbeatLocalResult{
		ContractVersion: internalHeartbeatLocalResultContractVersion,
		Outcome:         "blocked",
		Summary:         "Cannot verify the claim without a browser screenshot.",
		ActionRequests: []InternalHeartbeatActionRequest{
			{
				RequestID:        "needs-browser-shot",
				Capability:       "browser_screenshot",
				ToolSuite:        "screenshot_capture",
				Title:            "Capture real product screenshot before judging UX",
				Summary:          "The heartbeat can see a possible visual claim but lacks screenshot authority in this local-only cycle.",
				Score:            82,
				EvidenceRefs:     []string{"selector:runnable_surface"},
				Promote:          true,
				Reason:           "visual judgment would be fake without screenshot evidence",
				RequiresTaskLoop: true,
			},
		},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one action request backlog item, got %+v", items)
	}
	item := items[0]
	if item.Kind != "heartbeat_action_request" || item.DedupKey != "needs-browser-shot" || item.Meta["finding_source"] != internalHeartbeatActionRequestSource {
		t.Fatalf("unexpected action request item: %+v", item)
	}
	if item.Meta["action_request_capability"] != "browser_screenshot" || item.Meta["action_request_tool_suite"] != "screenshot_capture" || item.Meta["action_requires_task_loop"] != "true" {
		t.Fatalf("action request metadata was not preserved: %+v", item.Meta)
	}
	if item.Meta["policy_local_only"] != "true" || item.Meta["policy_allow_task_submit"] != "false" || item.Meta["finding_promote"] != "true" {
		t.Fatalf("expected local policy metadata without public refs, got %+v", item.Meta)
	}
	if len(item.PromotionRefs) != 0 || len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 {
		t.Fatalf("action request must remain local until bounded promotion policy exists, got %+v", item)
	}
}

func TestPersonalBacklogArbiterRoutesActionRequestWithoutPublicPromotion(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 11, 0, 0, 0, time.UTC)
	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "needs-browser-shot",
		HeartbeatID: "visual_product_audit",
		Kind:        "heartbeat_action_request",
		Status:      "open",
		Title:       "Capture real product screenshot before judging UX",
		Summary:     "The critic needs screenshot authority before making a visual judgment.",
		Score:       84,
		EvidenceRefs: []string{
			"selector:runnable_surface",
		},
		LastSeenAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		Meta: map[string]string{
			"finding_source":              internalHeartbeatActionRequestSource,
			"action_request_capability":   "browser_screenshot",
			"action_request_tool_suite":   "screenshot_capture",
			"action_requires_task_loop":   "true",
			"action_requires_human_input": "false",
			"target_project_id":           "project-ui",
			"target_project_lane":         "qa",
			"block_promote":               "true",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "generalist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	spec := defaultPersonalBacklogArbiterHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "action_request_pending", now)
	if len(packet.BacklogCandidates) == 0 || packet.BacklogCandidates[0].ActionCapability != "browser_screenshot" || packet.BacklogCandidates[0].ActionToolSuite != "screenshot_capture" || !packet.BacklogCandidates[0].ActionRequiresTaskLoop {
		t.Fatalf("arbiter packet should expose safe action request metadata, got %+v", packet.BacklogCandidates)
	}
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "action_request_pending", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("expected local arbiter route, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "personal_backlog_action_route")
	if item.Meta["finding_source"] != internalHeartbeatBacklogArbiterSource || item.Meta["target_project_id"] != "project-ui" || item.Meta["target_project_lane"] != "qa" {
		t.Fatalf("unexpected arbiter route item: %+v", item)
	}
	for _, want := range []string{"backlog_item:" + seed.ItemID, "dedup:needs-browser-shot", "capability:browser_screenshot", "tool_suite:screenshot_capture", "requires:task_loop"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("arbiter route missing evidence ref %q in %+v", want, item.EvidenceRefs)
		}
	}
	if len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 || len(item.PromotionRefs) != 0 || item.Meta["finding_promote"] != "false" {
		t.Fatalf("arbiter route must remain private/local, got %+v", item)
	}
	capabilityItem := findBacklogByKind(t, runtime.internalSessions, "capability_session_blocked")
	if capabilityItem.Meta["finding_source"] != internalHeartbeatCapabilitySessionSource || capabilityItem.Meta["action_request_capability"] != "browser_screenshot" || capabilityItem.Meta["action_request_tool_suite"] != "screenshot_capture" {
		t.Fatalf("unexpected local capability session item: %+v", capabilityItem)
	}
	for _, want := range []string{"backlog_item:" + item.ItemID, "dedup:" + item.DedupKey, "capability:browser_screenshot", "tool_suite:screenshot_capture", "requires:task_loop", "capability_status:blocked"} {
		if !containsAnatomyTestString(capabilityItem.EvidenceRefs, want) {
			t.Fatalf("capability session missing evidence ref %q in %+v", want, capabilityItem.EvidenceRefs)
		}
	}
	if len(capabilityItem.TaskIDs) != 0 || len(capabilityItem.DocKeys) != 0 || len(capabilityItem.PromotionRefs) != 0 || capabilityItem.Meta["finding_promote"] != "false" {
		t.Fatalf("capability session result must remain private/local, got %+v", capabilityItem)
	}
	foundCapabilitySession := false
	for _, session := range runtime.internalSessions.Snapshot().Sessions {
		if session.HeartbeatID == "capability_session_browser_screenshot" {
			foundCapabilitySession = true
			if session.Status != "completed" || session.Outcome != "capability_blocked" || !session.PromotionBlocked {
				t.Fatalf("unexpected synthetic capability session: %+v", session)
			}
			if session.Meta["capability"] != "browser_screenshot" || session.Meta["tool_suite"] != "screenshot_capture" || session.Meta["capability_status"] != "blocked" {
				t.Fatalf("synthetic capability session lost route metadata: %+v", session)
			}
		}
	}
	if !foundCapabilitySession {
		t.Fatalf("expected synthetic capability session, got %+v", runtime.internalSessions.Snapshot().Sessions)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 || server.postUpdateCount() != 0 {
		t.Fatalf("arbiter plus synthetic capability session must not write public state, doc=%d task=%d update=%d", server.putDocCount(), server.submitTaskCount(), server.postUpdateCount())
	}

	second, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "action_request_pending", now.Add(11*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if second.Outcome == "backlog_recorded" {
		t.Fatalf("arbiter should not duplicate an already routed action request, got %+v", second)
	}
	if count := countBacklogByKind(runtime.internalSessions, "personal_backlog_action_route"); count != 1 {
		t.Fatalf("expected exactly one arbiter route item, got %d", count)
	}
	if count := countBacklogByKind(runtime.internalSessions, "capability_session_blocked"); count != 1 {
		t.Fatalf("expected exactly one local capability result item, got %d", count)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 || server.postUpdateCount() != 0 {
		t.Fatalf("second arbiter run must still not write public state, doc=%d task=%d update=%d", server.putDocCount(), server.submitTaskCount(), server.postUpdateCount())
	}
}

func TestActionRequestPromotionFindingRequiresRepeatedAgedRouteAndUsesCapabilityLane(t *testing.T) {
	now := time.Date(2026, 5, 15, 11, 30, 0, 0, time.UTC)
	candidate := InternalHeartbeatBacklogSummary{
		ItemID:                 "route-1",
		DedupKey:               "visual-audit-runnable-surface",
		Kind:                   "personal_backlog_action_route",
		Source:                 internalHeartbeatBacklogArbiterSource,
		Title:                  "Route personal action request: browser_screenshot",
		Score:                  99,
		SeenCount:              1,
		CreatedAt:              now.Add(-25 * time.Minute).Format(time.RFC3339Nano),
		LastSeenAt:             now.Add(-25 * time.Minute).Format(time.RFC3339Nano),
		ActionCapability:       "browser_screenshot",
		ActionToolSuite:        "screenshot_capture",
		TargetProjectID:        "project-ui",
		TargetProjectLane:      "strategy",
		PromotionBlocked:       true,
		ActionRequiresTaskLoop: true,
	}
	packet := InternalHeartbeatContextPacket{
		HeartbeatID:       "action_request_promoter",
		Now:               now.Format(time.RFC3339Nano),
		BacklogCandidates: []InternalHeartbeatBacklogSummary{candidate},
	}
	if finding, ok := internalHeartbeatActionRequestPromotionFinding(packet); ok {
		t.Fatalf("single-observation high-score visual route must stay local, got %+v", finding)
	}

	candidate.SeenCount = 2
	candidate.CreatedAt = now.Add(-5 * time.Minute).Format(time.RFC3339Nano)
	candidate.LastSeenAt = now.Add(-5 * time.Minute).Format(time.RFC3339Nano)
	packet.BacklogCandidates = []InternalHeartbeatBacklogSummary{candidate}
	if finding, ok := internalHeartbeatActionRequestPromotionFinding(packet); ok {
		t.Fatalf("fresh repeated visual route must stay local until it ages, got %+v", finding)
	}

	candidate.CreatedAt = now.Add(-25 * time.Minute).Format(time.RFC3339Nano)
	candidate.LastSeenAt = now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	packet.BacklogCandidates = []InternalHeartbeatBacklogSummary{candidate}
	if finding, ok := internalHeartbeatActionRequestPromotionFinding(packet); ok {
		t.Fatalf("aged visual route without verified runnable surface must stay local, got %+v", finding)
	}
	probeCandidate := candidate
	probeCandidate.ItemID = "route-browser-probe"
	probeCandidate.DedupKey = "browser-visual-probe-route"
	probeCandidate.Title = "Route personal action request: capture screenshot"
	probeCandidate.ActionCapability = "browser_visual_probe"
	probeCandidate.ActionToolSuite = "browser_session"
	packet.BacklogCandidates = []InternalHeartbeatBacklogSummary{probeCandidate}
	if finding, ok := internalHeartbeatActionRequestPromotionFinding(packet); ok {
		t.Fatalf("browser_visual_probe route without verified runnable surface must stay local, got %+v", finding)
	}

	packet.SelectorPayloads = []InternalHeartbeatSelectorPacket{{
		Selector:     "runnable_surface",
		Status:       "surface_preflight_verified",
		Summary:      "Typed browser preflight verified a runnable project surface.",
		TrustedScope: InternalHeartbeatTrustedScope{ProjectID: "project-other"},
	}}
	if finding, ok := internalHeartbeatActionRequestPromotionFinding(packet); ok {
		t.Fatalf("verified runnable surface from another project must not promote route, got %+v", finding)
	}

	packet.SelectorPayloads[0].TrustedScope.ProjectID = "project-ui"
	finding, ok := internalHeartbeatActionRequestPromotionFinding(packet)
	if !ok {
		t.Fatal("expected repeated aged visual route to become promotable after verified surface evidence")
	}
	if finding.ProjectLane != "qa" {
		t.Fatalf("browser/screenshot route should promote into capability lane qa instead of inherited strategy lane, got %+v", finding)
	}
}

func TestActionRequestPromotionFindingUsesProjectScopedPersistedVisualSurfaceEvidence(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 15, 0, 0, time.UTC)
	route := InternalHeartbeatBacklogSummary{
		ItemID:                 "route-visual-probe",
		DedupKey:               "browser-visual-probe-route",
		Kind:                   "personal_backlog_action_route",
		Source:                 internalHeartbeatBacklogArbiterSource,
		Title:                  "Route personal action request: browser_visual_probe",
		Score:                  96,
		SeenCount:              2,
		CreatedAt:              now.Add(-20 * time.Minute).Format(time.RFC3339Nano),
		LastSeenAt:             now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		ActionCapability:       "browser_visual_probe",
		ActionToolSuite:        "browser_session",
		TargetProjectID:        "project-ui",
		ActionRequiresTaskLoop: true,
	}
	visualEvidence := InternalHeartbeatBacklogSummary{
		ItemID:          "visual-surface",
		Kind:            "visual_acceptance_gap",
		Source:          internalHeartbeatVisualSensorSource,
		Status:          "open",
		TargetProjectID: "project-other",
		EvidenceRefs:    []string{"selector:runnable_surface", "status:surface_preflight_verified", "browser_probe:verified_product_marker"},
	}
	packet := InternalHeartbeatContextPacket{
		HeartbeatID:       "action_request_promoter",
		Now:               now.Format(time.RFC3339Nano),
		BacklogCandidates: []InternalHeartbeatBacklogSummary{route, visualEvidence},
	}
	if finding, ok := internalHeartbeatActionRequestPromotionFinding(packet); ok {
		t.Fatalf("persisted verified surface from another project must not promote route, got %+v", finding)
	}

	visualEvidence.TargetProjectID = "project-ui"
	packet.BacklogCandidates = []InternalHeartbeatBacklogSummary{route, visualEvidence}
	if finding, ok := internalHeartbeatActionRequestPromotionFinding(packet); !ok || finding.ProjectID != "project-ui" {
		t.Fatalf("expected project-matched persisted visual surface to promote route, got ok=%v finding=%+v", ok, finding)
	}
}

func TestActionRequestPromoterPrecheckRespectsFreshRouteAge(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 11, 45, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-promoter-precheck", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "backlog-arbiter:action-route:fresh-browser-shot",
		HeartbeatID: "personal_backlog_arbiter",
		Kind:        "personal_backlog_action_route",
		Status:      "open",
		Title:       "Route personal action request: browser_screenshot",
		Summary:     "A UI critic needs browser screenshot evidence before judging product quality.",
		Score:       99,
		SeenCount:   2,
		CreatedAt:   now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		LastSeenAt:  now.Add(-1 * time.Minute).Format(time.RFC3339Nano),
		Meta: map[string]string{
			"finding_source":              internalHeartbeatBacklogArbiterSource,
			"action_request_capability":   "browser_screenshot",
			"action_request_tool_suite":   "screenshot_capture",
			"action_requires_task_loop":   "true",
			"action_requires_human_input": "false",
			"target_project_id":           "project-ui",
			"target_project_lane":         "strategy",
		},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws-promoter-precheck", AgentID: "sigma"},
		internalSessions: store,
	}
	if runtime.actionRequestPromoterHasCandidate(now) {
		t.Fatal("fresh repeated action route should not schedule the public promoter precheck")
	}
}

func TestActionRequestPromoterTurnsStalePrivateRouteIntoBoundedPublicTask(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 11, 30, 0, 0, time.UTC)
	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	route, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "backlog-arbiter:action-route:needs-build-smoke",
		HeartbeatID: "personal_backlog_arbiter",
		Kind:        "personal_backlog_action_route",
		Status:      "open",
		Title:       "Route personal action request: shell_execution",
		Summary:     "A local build/test smoke route needs a bounded public owner instead of staying private.",
		Score:       88,
		SeenCount:   2,
		EvidenceRefs: []string{
			"backlog_item:seed-action-request",
			"dedup:needs-build-smoke",
			"capability:shell_execution",
			"tool_suite:shell",
			"requires:task_loop",
		},
		CreatedAt:  now.Add(-25 * time.Minute).Format(time.RFC3339Nano),
		LastSeenAt: now.Add(-25 * time.Minute).Format(time.RFC3339Nano),
		Meta: map[string]string{
			"finding_source":              internalHeartbeatBacklogArbiterSource,
			"action_request_capability":   "shell_execution",
			"action_request_tool_suite":   "shell",
			"action_requires_task_loop":   "true",
			"action_requires_human_input": "false",
			"target_project_id":           "project-ui",
			"target_project_lane":         "implementation",
			"block_promote":               "true",
			"finding_promote":             "false",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "generalist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, nil)
	runtime.mu.Lock()
	runtime.activeTask = &WorkspaceTaskRecord{
		TaskID:      "task-active-ui",
		Title:       "Active implementation lane",
		Status:      "RUNNING",
		ProjectID:   "project-ui",
		ProjectLane: "implementation",
	}
	runtime.mu.Unlock()
	spec := defaultActionRequestPromoterHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	if policy.LocalOnly || !policy.AllowTaskSubmit {
		t.Fatalf("action_request_promoter should have bounded public promotion authority, got %+v", policy)
	}
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "personal_backlog_action_route_stale", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_promoted" || server.putDocCount() != 1 || server.submitTaskCount() != 1 {
		t.Fatalf("expected bounded public promotion, result=%+v doc=%d task=%d", result, server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByKind(t, runtime.internalSessions, "action_request_public_followup")
	if item.Status != "promoted" || item.Meta["finding_source"] != internalHeartbeatActionRequestPromoterSource || item.Meta["target_project_id"] != "project-ui" {
		t.Fatalf("unexpected promoted action-request item: %+v", item)
	}
	for _, want := range []string{"backlog_item:" + route.ItemID, "capability:shell_execution", "target_project:project-ui"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("promoted item missing evidence ref %q in %+v", want, item.EvidenceRefs)
		}
	}
	server.mu.Lock()
	submitted := server.lastTaskIn
	docContent := server.lastDocIn.Content
	server.mu.Unlock()
	if submitted.ProjectID != "project-ui" || submitted.ProjectLane != "verification" || !containsAnatomyTestString(submitted.Tags, "agent-backlog") || !containsAnatomyTestString(submitted.Tags, "capability-shell_execution") || !containsAnatomyTestString(submitted.Tags, "tool-suite-shell") {
		t.Fatalf("unexpected promoted task scope: %+v", submitted)
	}
	if strings.Contains(docContent, "C:\\") || strings.Contains(docContent, "/Users/") {
		t.Fatalf("promotion doc should not leak local paths: %s", docContent)
	}
}

func TestPersonalBacklogArbiterRecordsReadyInstalledBrowserCapabilitySession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 11, 15, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "needs-installed-browser-shot",
		HeartbeatID: "visual_product_audit",
		Kind:        "heartbeat_action_request",
		Status:      "open",
		Title:       "Capture real product screenshot before judging UX",
		Summary:     "The critic needs screenshot authority before making a visual judgment.",
		Score:       84,
		EvidenceRefs: []string{
			"selector:runnable_surface",
		},
		LastSeenAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		Meta: map[string]string{
			"finding_source":              internalHeartbeatActionRequestSource,
			"action_request_capability":   "browser_screenshot",
			"action_request_tool_suite":   "screenshot_capture",
			"action_requires_task_loop":   "true",
			"action_requires_human_input": "false",
			"target_project_id":           "project-ui",
			"target_project_lane":         "qa",
			"block_promote":               "true",
		},
	}); err != nil {
		t.Fatal(err)
	}

	workdir := t.TempDir()
	installTestToolBundle(t, workdir, "browser_visual_probe")
	profile := DefaultAgentProfile("sigma", "Sigma", "generalist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	spec := defaultPersonalBacklogArbiterHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "action_request_pending", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("expected local arbiter route, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "capability_session_ready")
	if item.Meta["finding_source"] != internalHeartbeatCapabilitySessionSource || item.Meta["action_request_capability"] != "browser_screenshot" || item.Meta["action_request_tool_suite"] != "screenshot_capture" {
		t.Fatalf("unexpected ready installed capability item: %+v", item)
	}
	for _, want := range []string{"capability:browser_screenshot", "tool_suite:screenshot_capture", "capability_status:ready"} {
		if !containsAnatomyTestString(item.EvidenceRefs, want) {
			t.Fatalf("ready installed capability item missing evidence ref %q in %+v", want, item.EvidenceRefs)
		}
	}
	for _, session := range runtime.internalSessions.Snapshot().Sessions {
		if session.HeartbeatID == "capability_session_browser_screenshot" {
			if session.Status != "completed" || session.Outcome != "capability_ready" || session.Meta["supported"] != "true" {
				t.Fatalf("unexpected installed capability session: %+v", session)
			}
			return
		}
	}
	t.Fatalf("expected installed capability session, got %+v", runtime.internalSessions.Snapshot().Sessions)
}

func TestPersonalBacklogArbiterRecordsReadyReadOnlyCapabilitySession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 15, 11, 30, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-1", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "needs-doc-read",
		HeartbeatID: "project_role_initiative",
		Kind:        "heartbeat_action_request",
		Status:      "open",
		Title:       "Read product contract before planning",
		Summary:     "A strategy heartbeat wants bounded local document context.",
		Score:       82,
		LastSeenAt:  now.Add(-time.Minute).Format(time.RFC3339Nano),
		Meta: map[string]string{
			"finding_source":              internalHeartbeatActionRequestSource,
			"action_request_capability":   "workspace_doc_read",
			"action_request_tool_suite":   "workspace_docs_read",
			"action_requires_task_loop":   "false",
			"action_requires_human_input": "false",
			"target_project_id":           "project-docs",
			"target_project_lane":         "strategy",
			"block_promote":               "true",
		},
	}); err != nil {
		t.Fatal(err)
	}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("sigma", "Sigma", "generalist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	spec := defaultPersonalBacklogArbiterHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(profile), spec, policy, "action_request_pending", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || !result.PromotionBlocked {
		t.Fatalf("expected local arbiter route, got %+v", result)
	}
	item := findBacklogByKind(t, runtime.internalSessions, "capability_session_ready")
	if item.Meta["finding_source"] != internalHeartbeatCapabilitySessionSource || item.Meta["action_request_capability"] != "workspace_doc_read" || item.Meta["action_request_tool_suite"] != "workspace_docs_read" {
		t.Fatalf("unexpected ready capability session item: %+v", item)
	}
	if len(item.TaskIDs) != 0 || len(item.DocKeys) != 0 || len(item.PromotionRefs) != 0 || item.Meta["finding_promote"] != "false" {
		t.Fatalf("ready capability session still must stay local-only, got %+v", item)
	}
	for _, session := range runtime.internalSessions.Snapshot().Sessions {
		if session.HeartbeatID == "capability_session_workspace_doc_read" {
			if session.Outcome != "capability_ready" || session.Meta["supported"] != "true" || session.Meta["tool_suite"] != "workspace_docs_read" {
				t.Fatalf("unexpected ready capability session record: %+v", session)
			}
			return
		}
	}
	t.Fatalf("expected ready synthetic capability session, got %+v", runtime.internalSessions.Snapshot().Sessions)
}

func TestInternalHeartbeatPromotionCandidatesRequireCurrentSessionAndPolicy(t *testing.T) {
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	current := AgentPersonalBacklogItem{
		ItemID:        "item-current",
		DedupKey:      "strategy:current",
		HeartbeatID:   spec.ID,
		Kind:          "strategic_gap",
		Status:        "open",
		Title:         "Current candidate",
		Score:         90,
		LastSessionID: "session-current",
		Meta: map[string]string{
			"finding_promote":          "true",
			"policy_allow_task_submit": "true",
			"policy_local_only":        "false",
		},
	}
	got := internalHeartbeatPromotionCandidates([]AgentPersonalBacklogItem{
		current,
		{
			ItemID:        "item-old",
			DedupKey:      "strategy:old",
			HeartbeatID:   spec.ID,
			Status:        "open",
			Title:         "Old candidate",
			Score:         99,
			LastSessionID: "session-old",
			Meta:          current.Meta,
		},
		{
			ItemID:        "item-local",
			DedupKey:      "strategy:local",
			HeartbeatID:   spec.ID,
			Status:        "open",
			Title:         "Local policy candidate",
			Score:         98,
			LastSessionID: "session-current",
			Meta: map[string]string{
				"finding_promote":          "true",
				"policy_allow_task_submit": "false",
				"policy_local_only":        "true",
			},
		},
		{
			ItemID:        "item-wrong-heartbeat",
			DedupKey:      "strategy:wrong-heartbeat",
			HeartbeatID:   "loop_self_check",
			Status:        "open",
			Title:         "Wrong heartbeat candidate",
			Score:         97,
			LastSessionID: "session-current",
			Meta:          current.Meta,
		},
	}, "session-current", spec, 70)
	if len(got) != 1 || got[0].ItemID != "item-current" {
		t.Fatalf("expected only current-session public-policy candidate, got %+v", got)
	}
}

func TestParseInternalHeartbeatLocalResultNormalizesFindings(t *testing.T) {
	result, err := parseInternalHeartbeatLocalResult(`{
		"contract_version":"internal-heartbeat-local-result/v1",
		"summary":"checked",
		"backlog_items":[
			{"title":"  Keep me  ","score":500,"evidence_refs":[" a ","a"]},
			{"title":"Scale fractional priority","score":0.82},
			{"summary":"   "}
		],
		"active_memory":[
			{"lane":" Self Check ","note":"  remember this course correction  ","evidence_refs":[" e ","e"],"tags":[" loop ","loop"]}
		],
		"action_requests":[
			{"request_id":" Needs Browser ","capability":" Browser Screenshot ","tool_suite":" Screenshot Capture ","title":"Need evidence","score":"0.72","evidence_refs":[" e ","e"]}
		],
		"will_directives":[
			{"directive_id":" Replan Now ","action":"abandon_current_contour","task_id":"task-1","summary":"Change course","evidence_refs":[" e ","e"]}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "no_action" || len(result.BacklogItems) != 2 || len(result.ActionRequests) != 1 || len(result.ActiveMemory) != 1 || len(result.WillDirectives) != 1 {
		t.Fatalf("unexpected parsed result: %+v", result)
	}
	if result.BacklogItems[0].Title != "Keep me" || result.BacklogItems[0].Score != 100 || len(result.BacklogItems[0].EvidenceRefs) != 1 {
		t.Fatalf("finding was not normalized: %+v", result.BacklogItems[0])
	}
	if result.BacklogItems[1].Score != 82 {
		t.Fatalf("fractional finding score was not scaled: %+v", result.BacklogItems[1])
	}
	if got := result.ActionRequests[0]; got.RequestID != "needs-browser" || got.Capability != "browser-screenshot" || got.ToolSuite != "screenshot-capture" || got.Score != 72 || len(got.EvidenceRefs) != 1 {
		t.Fatalf("action request was not normalized: %+v", got)
	}
	if got := result.ActiveMemory[0]; got.Lane != "self-check" || got.Note != "remember this course correction" || len(got.EvidenceRefs) != 1 || len(got.Tags) != 1 {
		t.Fatalf("active memory note was not normalized: %+v", got)
	}
	if got := result.WillDirectives[0]; got.DirectiveID != "replan-now" || got.Action != "replan_active_work" || got.TaskID != "task-1" || len(got.EvidenceRefs) != 1 {
		t.Fatalf("will directive was not normalized: %+v", got)
	}
	if _, err := parseInternalHeartbeatLocalResult(`{"summary":"bad","memory_body":"must stay private"}`); err == nil {
		t.Fatalf("expected unknown public-looking task-cycle field to be rejected")
	}
}

func TestParseInternalHeartbeatLocalResultExtractsObjectFromLLMWrapper(t *testing.T) {
	result, err := parseInternalHeartbeatLocalResult("Here is the JSON:\n```json\n{\"contract_version\":\"internal-heartbeat-local-result/v1\",\"summary\":\"wrapped\",\"backlog_items\":[{\"title\":\"Captured despite wrapper\",\"score\":\"84%\"}]}\n```\nDone.")
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary != "wrapped" || len(result.BacklogItems) != 1 || result.BacklogItems[0].Score != 84 {
		t.Fatalf("wrapped JSON was not parsed and normalized: %+v", result)
	}
}

func TestApplyInternalHeartbeatWillDirectivesReplansActiveWork(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-will",
			AgentID:     "sigma",
		},
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-active",
			ActiveSessionID: "sess-active",
			DocSHAs:         map[string]string{},
		},
	}
	spec := normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "loop_self_check",
		Kind:     "metacognition",
		Cadence:  "every_15m",
		Priority: 50,
		Locks:    []string{"local_only"},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions: []string{"replan_active_work"},
			MaxDirectives:  1,
		},
	})
	policy := internalHeartbeatExecutionPolicy(spec)
	refs, err := runtime.applyInternalHeartbeatWillDirectives(context.Background(), AgentInternalSessionRecord{SessionID: "ihb-1"}, spec, policy, InternalHeartbeatLocalResult{
		WillDirectives: []InternalHeartbeatWillDirective{
			{Action: "abandon_current_contour", Summary: "Current loop is stale; resume with a different plan.", EvidenceRefs: []string{"session:prior"}},
		},
	}, time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected one applied will directive, got %+v", refs)
	}
	if runtime.scratch.PendingTrigger != "request_resume" || runtime.scratch.PendingTriggerTask != "task-active" || runtime.scratch.PendingTriggerSession != "sess-active" {
		t.Fatalf("expected active work replan trigger, got %+v", runtime.scratch)
	}
	if len(runtime.scratch.AdvisorySignals) == 0 || !strings.Contains(runtime.scratch.AdvisorySignals[len(runtime.scratch.AdvisorySignals)-1], "Current loop is stale") {
		t.Fatalf("expected heartbeat advisory signal, got %+v", runtime.scratch.AdvisorySignals)
	}
}

// TestApplyInternalHeartbeatWillDirectivesRouteReflectionClassToChannel is the
// CA-31 regression: when the reflection channel is enabled, a reflection-class
// (metacognition) heartbeat's preemptive will-directive still sets the pending
// work trigger, but its advisory NOTICE must land in the Layer B reflection
// channel rather than evicting a slot from the 5-slot watchdog advisory ring.
func TestApplyInternalHeartbeatWillDirectivesRouteReflectionClassToChannel(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws-will-reflect", AgentID: "sigma"},
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-active",
			ActiveSessionID: "sess-active",
			DocSHAs:         map[string]string{},
		},
	}
	runtime.agent = &Agent{}
	runtime.agent.Anatomy.Memory.ReflectionChannelCap = 8

	spec := normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "loop_self_check",
		Kind:     "metacognition",
		Cadence:  "every_15m",
		Priority: 50,
		Locks:    []string{"local_only"},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions: []string{"replan_active_work"},
			MaxDirectives:  1,
		},
	})
	policy := internalHeartbeatExecutionPolicy(spec)
	if _, err := runtime.applyInternalHeartbeatWillDirectives(context.Background(), AgentInternalSessionRecord{SessionID: "ihb-reflect"}, spec, policy, InternalHeartbeatLocalResult{
		WillDirectives: []InternalHeartbeatWillDirective{
			{Action: "replan_active_work", Summary: "Reflective course correction is warranted.", EvidenceRefs: []string{"session:prior"}},
		},
	}, time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if runtime.scratch.PendingTrigger != "request_resume" {
		t.Fatalf("expected reflection-class replan to still set the pending work trigger, got %q", runtime.scratch.PendingTrigger)
	}
	if len(runtime.scratch.AdvisorySignals) != 0 {
		t.Fatalf("CA-31: reflection-class notice must not enter the watchdog advisory ring, got %+v", runtime.scratch.AdvisorySignals)
	}
	if len(runtime.scratch.ReflectionSignals) == 0 {
		t.Fatalf("CA-31: expected the will-directive notice to route to the reflection channel, got none")
	}
}

func TestApplyInternalHeartbeatWillDirectivesRequiresEvidenceForMutatingActions(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws-will-evidence", AgentID: "sigma"},
		scratch: RuntimeScratchState{
			ActiveTaskID: "task-active",
			DocSHAs:      map[string]string{},
		},
	}
	spec := normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:         "design_global_reflection",
		Kind:       "global_metacognition",
		Cadence:    "every_30m",
		Priority:   60,
		ToolSuites: []string{"bounded_task_submit"},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions:    []string{"replan_active_work"},
			MaxDirectives:     1,
			RequiresEvidence:  boolPtr(true),
			PublishVisibility: "rhizome",
		},
	})
	policy := internalHeartbeatExecutionPolicy(spec)
	refs, err := runtime.applyInternalHeartbeatWillDirectives(context.Background(), AgentInternalSessionRecord{SessionID: "ihb-1"}, spec, policy, InternalHeartbeatLocalResult{
		WillDirectives: []InternalHeartbeatWillDirective{
			{Action: "replan_active_work", Summary: "Reason alone should not mutate planner.", Reason: "sounds convincing"},
		},
	}, time.Date(2026, 5, 17, 10, 5, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 || runtime.scratch.PendingTrigger != "" {
		t.Fatalf("expected evidence-less mutating directive to be ignored, refs=%+v scratch=%+v", refs, runtime.scratch)
	}
}

func TestApplyInternalHeartbeatWillDirectivesTargetlessReplanIsAdvisoryOnly(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws-targetless", AgentID: "sigma"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	spec := normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "loop_self_check",
		Kind:     "metacognition",
		Cadence:  "every_15m",
		Priority: 50,
		Locks:    []string{"local_only"},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions: []string{"replan_active_work"},
			MaxDirectives:  1,
		},
	})
	policy := internalHeartbeatExecutionPolicy(spec)
	refs, err := runtime.applyInternalHeartbeatWillDirectives(context.Background(), AgentInternalSessionRecord{SessionID: "ihb-1"}, spec, policy, InternalHeartbeatLocalResult{
		WillDirectives: []InternalHeartbeatWillDirective{
			{Action: "replan_active_work", Summary: "No active work exists yet."},
		},
	}, time.Date(2026, 5, 17, 10, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || !strings.Contains(refs[0], "advisory_only") {
		t.Fatalf("expected advisory-only ref for targetless replan, got %+v", refs)
	}
	if runtime.scratch.PendingTrigger != "" {
		t.Fatalf("targetless replan must not queue pending trigger, got %+v", runtime.scratch)
	}
	if len(runtime.scratch.AdvisorySignals) == 0 || !strings.Contains(runtime.scratch.AdvisorySignals[len(runtime.scratch.AdvisorySignals)-1], "Targetless replan_active_work") {
		t.Fatalf("expected advisory-only signal, got %+v", runtime.scratch.AdvisorySignals)
	}
}

func TestApplyInternalHeartbeatWillDirectivesBlocksPrivateVisibilityPublish(t *testing.T) {
	runtime := &Runtime{
		cfg:     RuntimeConfig{WorkspaceID: "ws-private-publish", AgentID: "sigma"},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	spec := normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:         "private_reflection",
		Kind:       "metacognition",
		Cadence:    "every_30m",
		Priority:   40,
		ToolSuites: []string{"bounded_task_submit"},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions: []string{"publish_rhizome_update"},
			MaxDirectives:  1,
		},
	})
	policy := internalHeartbeatExecutionPolicy(spec)
	refs, err := runtime.applyInternalHeartbeatWillDirectives(context.Background(), AgentInternalSessionRecord{SessionID: "ihb-1"}, spec, policy, InternalHeartbeatLocalResult{
		WillDirectives: []InternalHeartbeatWillDirective{
			{Action: "publish_rhizome_update", Summary: "Should stay private."},
		},
	}, time.Date(2026, 5, 17, 10, 15, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Fatalf("private visibility publish should not apply public ref, got %+v", refs)
	}
	if len(runtime.scratch.AdvisorySignals) == 0 || !strings.Contains(runtime.scratch.AdvisorySignals[len(runtime.scratch.AdvisorySignals)-1], "blocked by this heartbeat's will_policy visibility") {
		t.Fatalf("expected blocked visibility advisory, got %+v", runtime.scratch.AdvisorySignals)
	}
}

func TestRecordTypedInternalHeartbeatLocalSessionPersistsActiveMemoryNotes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"no_action",
		"summary":"Local reflection adjusted its short-term context.",
		"active_memory":[{"lane":"wrong_lane","note":"Need to stop retrying the same visual route until runnable evidence exists.","evidence_refs":["session:prior"],"tags":["stuck"]}]
	}`}}}
	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-active-memory",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, llm)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	anatomy := DefaultAgentAnatomyConfig(DefaultAgentProfile("sigma", "Sigma", "local self checker"))
	now := time.Date(2026, 5, 17, 11, 0, 0, 0, time.UTC)
	if _, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), anatomy, spec, policy, "same_blocker_repeated", now); err != nil {
		t.Fatal(err)
	}
	store, err := OpenAgentInternalSessionStore("ws-active-memory", "sigma")
	if err != nil {
		t.Fatal(err)
	}
	state := store.Snapshot()
	if len(state.Sessions) != 1 || state.Sessions[0].Meta["active_memory_note_count"] != "1" {
		t.Fatalf("expected active memory note in session meta, got %+v", state.Sessions)
	}
	if !strings.Contains(state.Sessions[0].Meta["active_memory_note_1"], `"lane":"self_check"`) {
		t.Fatalf("expected configured active-memory lane to override model lane, got %+v", state.Sessions[0].Meta)
	}
	packet := runtime.buildInternalHeartbeatContextPacket(store, spec, policy, "cadence_elapsed", now.Add(time.Hour))
	if len(packet.ActiveMemory) == 0 || !strings.Contains(packet.ActiveMemory[0].Summary, "runnable evidence") {
		t.Fatalf("expected next heartbeat packet to hydrate active memory, got %+v", packet.ActiveMemory)
	}
}

func TestRecordTypedInternalHeartbeatLocalSessionRecordsFailedSessionOnBadLLMResult(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"summary":"bad","materialize":{"doc_key":"leak"}}`}}}
	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, llm)
	spec := defaultLoopSelfCheckHeartbeat()
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfig(DefaultAgentProfile("sigma", "Sigma", "local self checker")), spec, policy, "repeated_no_work", now)
	if err != nil {
		t.Fatal(err)
	}
	state := LoadAgentInternalSessionState("ws-1", "sigma")
	if len(state.Sessions) != 1 || state.Sessions[0].Status != "failed" || state.Sessions[0].Outcome != "typed_result_failed" {
		t.Fatalf("expected durable failed heartbeat session, result=%+v state=%+v", result, state)
	}
	if len(state.Backlog) != 0 {
		t.Fatalf("bad heartbeat result should not create local backlog items: %+v", state.Backlog)
	}
	snapshot := runtime.internalHeartbeatStatusSnapshot(now.Add(time.Minute))
	assertHeartbeatState(t, snapshot.Heartbeats, "loop_self_check", false, "cooldown")
}

func TestRecordTypedInternalHeartbeatLocalSessionPromotionFailureFailsSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	server.setFailTaskList(true)
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"A public follow-up is needed.",
		"backlog_items":[
			{"dedup_key":"strategy:list-fail","kind":"strategic_gap","title":"Promotion should fail closed","summary":"Task list failure must not create a public task or mark the item promoted.","score":95,"promote":true}
		]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	runtime.mu.Lock()
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectID: "project-ui", ProjectLane: "qa"}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 13, 20, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "failed" || result.Outcome != "promotion_failed" {
		t.Fatalf("expected failed durable promotion session, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("task-list failure must fail before public writes, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "strategy:list-fail")
	if item.Status != "open" || len(item.PromotionRefs) != 0 {
		t.Fatalf("failed promotion should leave backlog open and unpromoted, got %+v", item)
	}
	snapshot := runtime.internalHeartbeatStatusSnapshot(now.Add(time.Minute))
	assertHeartbeatState(t, snapshot.Heartbeats, "project_role_initiative", false, "cooldown")
}

func TestRecordTypedInternalHeartbeatLocalSessionRequiresTrustedProjectScopeForProjectInitiative(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"A project initiative lacks trusted project context and should stay local.",
		"backlog_items":[
			{"dedup_key":"strategy:no-project","kind":"strategic_gap","title":"No trusted project context","summary":"Do not publish a project-role initiative task without runtime project scope.","score":95,"promote":true}
		]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		RhizomeRPC:  server.URL,
		Workdir:     workdir,
	}, llm)
	runtime.mu.Lock()
	runtime.bootstrap.Snapshot = WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Two visible projects"},
		Projects: []ProjectRecord{
			{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "UI tool", Status: "ACTIVE", TaskCount: 0},
			{ProjectID: "project-pdf", WorkspaceID: "ws-1", Title: "PDF tool", Status: "ACTIVE", TaskCount: 0},
		},
		Tasks: []WorkspaceTaskRecord{
			{TaskID: "task-closed", Title: "Closed old task", Status: "COMPLETED", ProjectID: "project-ui", ProjectLane: "implementation"},
		},
	}
	runtime.mu.Unlock()
	spec := defaultProjectRoleInitiativeHeartbeat("strategist")
	policy := internalHeartbeatExecutionPolicy(spec)
	now := time.Date(2026, 5, 14, 13, 30, 0, 0, time.UTC)
	result, err := runtime.recordTypedInternalHeartbeatLocalSession(context.Background(), DefaultAgentAnatomyConfigForPreset(profile, "strategist"), spec, policy, "all_public_tasks_closed", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Outcome != "backlog_recorded" || len(result.PromotedRefs) != 0 {
		t.Fatalf("expected local backlog only without active project authority, got %+v", result)
	}
	if server.putDocCount() != 0 || server.submitTaskCount() != 0 {
		t.Fatalf("missing project scope should not write public docs/tasks, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "strategy:no-project")
	if item.Status != "open" {
		t.Fatalf("candidate should remain open local backlog, got %+v", item)
	}
}

func TestRecordInternalHeartbeatObservationDoesNotAdvanceOnStoreFailure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	rootFile := filepath.Join(home, "runtime-config-file")
	if err := os.WriteFile(rootFile, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(managedAgentConfigRootFlag, rootFile)

	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	runtime.recordInternalHeartbeatObservation("loop_self_check", now)
	snapshot := runtime.internalHeartbeatStatusSnapshot(now.Add(time.Minute))
	assertHeartbeatState(t, snapshot.Heartbeats, "loop_self_check", true, "never_ran")
}

type staticTool struct {
	name   string
	output string
}

func (t staticTool) Name() string { return t.name }

func (t staticTool) Description() string { return "static test tool" }

func (t staticTool) Parameters() map[string]any { return map[string]any{"type": "object"} }

func (t staticTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	return &ToolResult{Output: t.output}
}

func jsonUnmarshalTest(raw string, out any) error {
	return json.Unmarshal([]byte(raw), out)
}

func findSelectorPayload(t *testing.T, packet InternalHeartbeatContextPacket, selector string) InternalHeartbeatSelectorPacket {
	t.Helper()
	for _, payload := range packet.SelectorPayloads {
		if payload.Selector == selector {
			return payload
		}
	}
	t.Fatalf("selector %q not found in %+v", selector, packet.SelectorPayloads)
	return InternalHeartbeatSelectorPacket{}
}

func backlogHasDedup(store *AgentInternalSessionStore, dedupKey string) bool {
	if store == nil {
		return false
	}
	for _, item := range store.Snapshot().Backlog {
		if item.DedupKey == dedupKey {
			return true
		}
	}
	return false
}

func backlogHasKind(store *AgentInternalSessionStore, kind string) bool {
	if store == nil {
		return false
	}
	for _, item := range store.Snapshot().Backlog {
		if item.Kind == kind {
			return true
		}
	}
	return false
}

func countBacklogByKind(store *AgentInternalSessionStore, kind string) int {
	if store == nil {
		return 0
	}
	count := 0
	for _, item := range store.Snapshot().Backlog {
		if item.Kind == kind {
			count++
		}
	}
	return count
}

func findBacklogByKind(t *testing.T, store *AgentInternalSessionStore, kind string) AgentPersonalBacklogItem {
	t.Helper()
	for _, item := range store.Snapshot().Backlog {
		if item.Kind == kind {
			return item
		}
	}
	t.Fatalf("backlog item with kind %s not found", kind)
	return AgentPersonalBacklogItem{}
}

func testToolDefNames(tools []ToolDef) []string {
	out := make([]string, 0, len(tools))
	for _, tool := range tools {
		out = append(out, strings.TrimSpace(tool.Function.Name))
	}
	return out
}

func messagesText(messages []Message) string {
	var b strings.Builder
	for _, message := range messages {
		b.WriteString(message.Role)
		b.WriteString(": ")
		b.WriteString(message.Content)
		b.WriteString("\n")
		for _, call := range message.ToolCalls {
			b.WriteString("tool_call: ")
			b.WriteString(call.Function.Name)
			b.WriteString(" ")
			b.WriteString(call.Function.Arguments)
			b.WriteString("\n")
		}
	}
	return b.String()
}
