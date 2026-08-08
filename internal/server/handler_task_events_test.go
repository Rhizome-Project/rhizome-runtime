package server

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentUpdatePostPublishesSSEMirrorWithRuntimePayload(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-agent-update-sse"
		agentID     = "agent-update-sse"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, "task-agent-update-sse")

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(agentUpdatePostParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "progress",
		Summary:       "journal mirror ready",
		PayloadJSON:   `{"phase":"instrumentation"}`,
		RequiresHuman: true,
	})
	if err != nil {
		t.Fatalf("marshal update params: %v", err)
	}

	result, rpcErr := h.agentUpdatePost(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentUpdatePost rpc error: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if resultMap["status"] != "RECORDED" {
		t.Fatalf("expected RECORDED status, got %+v", resultMap)
	}

	event := nextEvent(t, ch)
	if event.WorkspaceID != workspaceID || event.AgentID != agentID || event.Summary != "journal mirror ready" {
		t.Fatalf("unexpected event envelope %+v", event)
	}

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	if payload["workspace_id"] != workspaceID {
		t.Fatalf("expected workspace_id %q, got %+v", workspaceID, payload)
	}
	if payload["agent_id"] != agentID {
		t.Fatalf("expected agent_id %q, got %+v", agentID, payload)
	}
	if payload["update_type"] != "progress" || payload["summary"] != "journal mirror ready" {
		t.Fatalf("unexpected event payload %+v", payload)
	}
	requiresHuman, ok := payload["requires_human"].(bool)
	if !ok || !requiresHuman {
		t.Fatalf("expected requires_human=true, got %+v", payload)
	}
	updateID, ok := payload["update_id"].(string)
	if !ok || updateID == "" {
		t.Fatalf("expected non-empty update_id in payload, got %+v", payload)
	}

	runtimeEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		EntityID:    updateID,
		AgentID:     agentID,
		Limit:       1,
	})
	if runtimeEvent.EntityID != updateID {
		t.Fatalf("expected runtime event entity %q, got %+v", updateID, runtimeEvent)
	}
	assertLiveEventMirrorsRuntimeEvent(t, event, runtimeEvent, "agent.update")
	assertPayloadMatchesRuntimeEvent(t, payload, runtimeEvent.PayloadJSON)
	assertServerAgentUpdateRuntimePromptContext(t, runtimeEvent, "agent.update.post", workspaceID, updateID, agentID, map[string]string{
		"update_type":    "progress",
		"summary":        "journal mirror ready",
		"requires_human": "true",
	})
}

func TestAgentUpdatePostInternalHeartbeatSummaryDoesNotTouchActivity(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-agent-update-internal-summary"
		agentID     = "agent-internal-summary"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Internal Summary Activity",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Internal Summary Agent",
		Role:        "strategist",
		Status:      "registered",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	beforeLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)
	if beforeLastSeenAt != "" {
		t.Fatalf("expected registered agent to start without last_seen_at, got %q", beforeLastSeenAt)
	}

	raw, err := json.Marshal(agentUpdatePostParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "internal_heartbeat_summary",
		Summary:       "Internal heartbeat loop_self_check no_action: status=completed; outcome=no_action; local_only=true",
		PayloadJSON:   `{"contract_version":"internal-heartbeat-summary/v1","observability_only":true}`,
		RequiresHuman: false,
	})
	if err != nil {
		t.Fatalf("marshal update params: %v", err)
	}
	if _, rpcErr := h.agentUpdatePost(ctx, raw); rpcErr != nil {
		t.Fatalf("agentUpdatePost rpc error: %+v", rpcErr)
	}
	if afterLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID); afterLastSeenAt != beforeLastSeenAt {
		t.Fatalf("internal heartbeat summary should not refresh liveness, before=%q after=%q", beforeLastSeenAt, afterLastSeenAt)
	}
	updates, err := store.ListAgentUpdatesAfter(ctx, workspaceID, "", "", 10)
	if err != nil {
		t.Fatalf("list updates: %v", err)
	}
	if len(updates) != 1 || updates[0].UpdateType != "internal_heartbeat_summary" {
		t.Fatalf("expected durable internal heartbeat summary update, got %+v", updates)
	}
}

func TestAgentUpdatePostReservedInternalHeartbeatTypeRequiresSummaryContractToSkipActivity(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-agent-update-internal-summary-contract"
		agentID     = "agent-internal-summary-contract"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Internal Summary Contract",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Internal Summary Contract Agent",
		Role:        "strategist",
		Status:      "registered",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	beforeLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)
	raw, err := json.Marshal(agentUpdatePostParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "internal_heartbeat_summary",
		Summary:       "This reserved update type is missing the heartbeat summary contract.",
		PayloadJSON:   `{"step":"ordinary-progress"}`,
		RequiresHuman: false,
	})
	if err != nil {
		t.Fatalf("marshal update params: %v", err)
	}
	if _, rpcErr := h.agentUpdatePost(ctx, raw); rpcErr != nil {
		t.Fatalf("agentUpdatePost rpc error: %+v", rpcErr)
	}
	afterLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)
	if afterLastSeenAt == beforeLastSeenAt || strings.TrimSpace(afterLastSeenAt) == "" {
		t.Fatalf("reserved update type without summary contract should refresh liveness, before=%q after=%q", beforeLastSeenAt, afterLastSeenAt)
	}
}

func assertServerAgentUpdateRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantUpdateID, wantAgentID string, extra map[string]string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent update prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_agent_update_write",
		"surface":                            wantSurface,
		"origin":                             "server_rpc",
		"workspace_id":                       wantWorkspaceID,
		"update_id":                          wantUpdateID,
		"agent_id":                           wantAgentID,
		"actor_agent_id":                     wantAgentID,
		"principal_type":                     "agent",
		"principal_id":                       wantAgentID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("agent update prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("agent update prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}

func TestAgentUpdatePostRejectsMismatchedAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-agent-update-spoof"
		agentID     = "agent-update-spoof"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Update Spoof",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Spoof Target",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(agentUpdatePostParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-other",
		UpdateType:    "progress",
		Summary:       "spoofed agent update should fail closed",
		PayloadJSON:   `{"step":"spoof"}`,
		RequiresHuman: true,
	})
	if err != nil {
		t.Fatalf("marshal agent update params: %v", err)
	}

	result, rpcErr := h.agentUpdatePost(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected spoofed agent update to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on spoofed update, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected spoofed update error %+v", rpcErr)
	}
}

func TestTaskSubmitPublishesTaskCreatedRuntimeEventWithPromptContext(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-created-live", "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-task-created-live",
		Title:       "Task Created Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	ch := h.GetEventBus().Subscribe("ws-task-created-live")
	defer h.GetEventBus().Unsubscribe("ws-task-created-live", ch)

	raw, err := json.Marshal(taskSubmitParams{
		TaskID:      "task-created-live",
		OwnerUserID: "developer",
		Priority:    "HIGH",
		Title:       "Live graph growth",
		Description: "Verify task.created reaches the dashboard graph.",
		TaskKind:    "EXECUTION",
		WorkspaceID: "ws-task-created-live",
		LinkedBy:    "dashboard",
		Tags:        []string{"graph", "live"},
	})
	if err != nil {
		t.Fatalf("marshal task submit params: %v", err)
	}

	result, rpcErr := h.taskSubmit(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("taskSubmit rpc error: %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if got, _ := resultMap["task_id"].(string); got != "task-created-live" {
		t.Fatalf("taskSubmit returned unexpected result %+v", resultMap)
	}

	live := nextEventOfType(t, ch, "task.created")
	if live.WorkspaceID != "ws-task-created-live" || live.EntityType != "task" || live.EntityID != "task-created-live" {
		t.Fatalf("unexpected task.created envelope %+v", live)
	}
	if live.CanonicalEventType != "task.created" {
		t.Fatalf("expected canonical_event_type task.created, got %+v", live)
	}
	if live.Summary != "Task created: Live graph growth" {
		t.Fatalf("unexpected summary %+v", live)
	}

	payload := decodeEventPayloadMap(t, live.PayloadJSON)
	if payload["workspace_id"] != "ws-task-created-live" || payload["task_id"] != "task-created-live" {
		t.Fatalf("unexpected task.created payload %+v", payload)
	}
	if payload["title"] != "Live graph growth" || payload["status"] != "PENDING" {
		t.Fatalf("unexpected task.created payload %+v", payload)
	}
	if payload["summary"] != "Task created: Live graph growth" {
		t.Fatalf("unexpected task.created summary payload %+v", payload)
	}

	persisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-task-created-live",
		EventType:   "task.created",
		EntityType:  "task",
		EntityID:    "task-created-live",
		TaskID:      "task-created-live",
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, live, persisted, "")
	assertPayloadMatchesRuntimeEvent(t, payload, persisted.PayloadJSON)
	assertTaskRuntimePromptContext(t, persisted, "task.submit", "ws-task-created-live", "human", "developer")
}

func TestTaskSubmitMissingWorkspaceDoesNotCreateOrphanTask(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-submit-missing", "human", "developer")

	ch := h.GetEventBus().Subscribe("ws-task-submit-missing")
	defer h.GetEventBus().Unsubscribe("ws-task-submit-missing", ch)

	raw, err := json.Marshal(taskSubmitParams{
		TaskID:      "task-submit-missing-workspace",
		OwnerUserID: "developer",
		Priority:    "HIGH",
		Title:       "Should not become orphaned",
		Description: "Missing workspace must fail before task creation.",
		TaskKind:    model.TaskKindExecution,
		WorkspaceID: "ws-task-submit-missing",
		LinkedBy:    "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal task submit params: %v", err)
	}

	result, rpcErr := h.taskSubmit(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected taskSubmit to reject missing workspace before task creation")
	}
	if result != nil {
		t.Fatalf("expected no result on missing workspace, got %+v", result)
	}
	if _, err := store.GetTaskStatus(ctx, "", "task-submit-missing-workspace"); err == nil {
		t.Fatal("expected missing-workspace submit to leave no orphan task")
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)
}

func TestTaskSubmitRequiresWorkspaceIDBeforeTaskCreation(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-submit-required", "human", "developer")

	ch := h.GetEventBus().Subscribe("ws-task-submit-required")
	defer h.GetEventBus().Unsubscribe("ws-task-submit-required", ch)

	cases := []struct {
		name      string
		taskID    string
		workspace any
		omit      bool
	}{
		{name: "omitted", taskID: "task-submit-workspace-omitted", omit: true},
		{name: "empty", taskID: "task-submit-workspace-empty", workspace: ""},
		{name: "whitespace", taskID: "task-submit-workspace-space", workspace: "   "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]any{
				"task_id":       tc.taskID,
				"owner_user_id": "developer",
				"priority":      "HIGH",
				"title":         "Workspace required",
				"description":   "Submit must not create a global orphan task.",
				"task_kind":     model.TaskKindExecution,
				"linked_by":     "dashboard",
			}
			if !tc.omit {
				params["workspace_id"] = tc.workspace
			}
			raw, err := json.Marshal(params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}

			result, rpcErr := h.taskSubmit(ctx, raw)
			if rpcErr == nil {
				t.Fatal("expected missing workspace_id to fail before task creation")
			}
			if rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params for missing workspace_id, got %+v", rpcErr)
			}
			if result != nil {
				t.Fatalf("expected no result on missing workspace_id, got %+v", result)
			}
			if _, err := store.GetTaskStatus(ctx, "", tc.taskID); err == nil {
				t.Fatalf("expected %s to leave no orphan task", tc.taskID)
			}
		})
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)
}

func TestTaskSubmitSuggestionContentIsValidJSONWithQuotedTitleAndTags(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-suggestion-json", "human", "developer")

	const (
		workspaceID = "ws-task-suggestion-json"
		agentID     = "agent-suggestion-json"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Suggestion JSON",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "system",
		OwnerUserID: "developer",
		DisplayName: "System",
	}); err != nil {
		t.Fatalf("register system agent: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Suggestion Agent",
	}); err != nil {
		t.Fatalf("register suggestion agent: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Tags:        []string{"quote-test", `quoted "tag"`},
	}); err != nil {
		t.Fatalf("upsert suggestion agent profile: %v", err)
	}

	taskTags := []string{"quote-test", `quoted "tag"`, `slash\tag`}
	raw, err := json.Marshal(taskSubmitParams{
		WorkspaceID: workspaceID,
		TaskID:      "task-suggestion-json",
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       `Quoted "title" stays JSON-safe`,
		Description: "Suggestion messages must remain parseable.",
		TaskKind:    model.TaskKindExecution,
		LinkedBy:    "dashboard",
		Tags:        taskTags,
	})
	if err != nil {
		t.Fatalf("marshal task submit params: %v", err)
	}
	if _, rpcErr := h.taskSubmit(ctx, raw); rpcErr != nil {
		t.Fatalf("taskSubmit rpc error: %+v", rpcErr)
	}

	messages, err := store.ListWorkspaceMessages(ctx, workspaceID, "task-suggestion", 10)
	if err != nil {
		t.Fatalf("list suggestion messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one task suggestion message, got %+v", messages)
	}
	if messages[0].FromAgentID != "system" || messages[0].ToAgentID != agentID || messages[0].Channel != "task-suggestion" || messages[0].ContentType != "application/json" {
		t.Fatalf("unexpected suggestion message envelope %+v", messages[0])
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(messages[0].Content), &payload); err != nil {
		t.Fatalf("task suggestion content is not valid JSON: %q: %v", messages[0].Content, err)
	}
	if payload["type"] != "task_suggestion" || payload["title"] != `Quoted "title" stays JSON-safe` {
		t.Fatalf("unexpected suggestion payload %+v", payload)
	}
	if !reflect.DeepEqual(payload["tags"], []any{"quote-test", `quoted "tag"`, `slash\tag`}) {
		t.Fatalf("unexpected tags in suggestion payload %+v", payload)
	}
	matched, ok := payload["matched_tags"].([]any)
	if !ok || !reflect.DeepEqual(matched, []any{"quote-test", `quoted "tag"`}) {
		t.Fatalf("unexpected matched_tags in suggestion payload %+v", payload)
	}
	messageEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messages[0].MessageID,
		Limit:       1,
	})
	messagePayload := decodeEventPayloadMap(t, messageEvent.PayloadJSON)
	if messagePayload["from_agent_id"] != "system" || messagePayload["to_agent_id"] != agentID || messagePayload["channel"] != "task-suggestion" || messagePayload["content_type"] != "application/json" || messagePayload["status"] != "SENT" {
		t.Fatalf("unexpected suggestion runtime event payload %+v", messagePayload)
	}
}

func TestTaskSubmitRejectsPatchQueueDuplicateButAllowsNonPatchReview(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-task-submit-patchq-rpc-gate"
		projectID   = "project-task-submit-patchq-rpc-gate"
		queueID     = "patchq-rpc"
		itemID      = "patchitem-rpc"
		branchID    = "projbranch-rpc"
		headSHA     = "1234567890abcdef1234567890abcdef12345678"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Patch Queue RPC Gate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	createServerProjectForTaskProjectFields(t, ctx, store, workspaceID, projectID)
	requiresProjectGate := true

	baseParams := taskSubmitParams{
		WorkspaceID:         workspaceID,
		TaskID:              "task-patchq-rpc-existing",
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Validate patch queue RPC candidate",
		Description:         "Direct RPC validation for a queue-bound candidate.",
		TaskKind:            model.TaskKindExecution,
		TaskTemplate:        model.TaskTemplateGeneric,
		ProjectID:           projectID,
		ProjectLane:         "validation",
		RequiresProjectGate: &requiresProjectGate,
		Tags:                []string{"patch-queue", "validation"},
		TaskRequirements: map[string]any{
			"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
			"patch_queue_task_kind":     "validation",
			"queue_id":                  queueID,
			"item_id":                   itemID,
			"branch_id":                 branchID,
			"head_sha":                  headSHA,
		},
		LinkedBy: "dashboard",
		Graph:    dag.DefaultGraph(),
	}
	raw, err := json.Marshal(baseParams)
	if err != nil {
		t.Fatalf("marshal existing params: %v", err)
	}
	if _, rpcErr := h.taskSubmit(ctx, raw); rpcErr != nil {
		t.Fatalf("create existing patch queue task: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	duplicateParams := baseParams
	duplicateParams.TaskID = "task-patchq-rpc-duplicate"
	raw, err = json.Marshal(duplicateParams)
	if err != nil {
		t.Fatalf("marshal duplicate params: %v", err)
	}
	result, rpcErr := h.taskSubmit(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "patch_queue_identity_duplicate") {
		t.Fatalf("expected invalid params patch queue duplicate, result=%+v err=%+v", result, rpcErr)
	}
	if _, err := store.GetTaskStatus(ctx, workspaceID, duplicateParams.TaskID); err == nil {
		t.Fatalf("duplicate RPC task should not be created")
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)

	requirementsOnlyProjectParams := baseParams
	requirementsOnlyProjectParams.TaskID = "task-patchq-rpc-requirements-project-duplicate"
	requirementsOnlyProjectParams.ProjectID = ""
	requirementsOnlyProjectParams.TaskRequirements = map[string]any{
		"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
		"patch_queue_task_kind":     "validation",
		"project_id":                projectID,
		"queue_id":                  queueID,
		"item_id":                   itemID,
		"branch_id":                 branchID,
		"head_sha":                  headSHA,
	}
	raw, err = json.Marshal(requirementsOnlyProjectParams)
	if err != nil {
		t.Fatalf("marshal requirements-only project duplicate params: %v", err)
	}
	result, rpcErr = h.taskSubmit(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "patch_queue_identity_duplicate") {
		t.Fatalf("expected requirements-only project identity to be gated, result=%+v err=%+v", result, rpcErr)
	}
	if _, err := store.GetTaskStatus(ctx, workspaceID, requirementsOnlyProjectParams.TaskID); err == nil {
		t.Fatalf("requirements-only project duplicate RPC task should not be created")
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)

	activeRetryParams := baseParams
	activeRetryParams.TaskID = "task-patchq-rpc-active-retry-marker"
	activeRetryParams.Title = "retry_of_terminal_followup_task patchq validation retry"
	activeRetryParams.Tags = []string{"patch-queue", "validation", "retry_of_terminal_followup_task"}
	raw, err = json.Marshal(activeRetryParams)
	if err != nil {
		t.Fatalf("marshal active retry marker params: %v", err)
	}
	result, rpcErr = h.taskSubmit(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "patch_queue_identity_duplicate") {
		t.Fatalf("retry marker must not bypass active duplicate, result=%+v err=%+v", result, rpcErr)
	}
	if _, err := store.GetTaskStatus(ctx, workspaceID, activeRetryParams.TaskID); err == nil {
		t.Fatalf("active retry marker RPC task should not be created")
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)

	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusResolved, baseParams.TaskID); err != nil {
		t.Fatalf("mark existing patch queue task terminal: %v", err)
	}
	terminalDuplicateParams := baseParams
	terminalDuplicateParams.TaskID = "task-patchq-rpc-terminal-duplicate"
	raw, err = json.Marshal(terminalDuplicateParams)
	if err != nil {
		t.Fatalf("marshal terminal duplicate params: %v", err)
	}
	result, rpcErr = h.taskSubmit(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "required_transition=project_patch_queue_followup") {
		t.Fatalf("expected terminal duplicate to route through followup, result=%+v err=%+v", result, rpcErr)
	}
	if _, err := store.GetTaskStatus(ctx, workspaceID, terminalDuplicateParams.TaskID); err == nil {
		t.Fatalf("terminal duplicate RPC task should not be created without canonical retry marker")
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)

	retryParams := baseParams
	retryParams.TaskID = "task-patchq-rpc-canonical-retry"
	retryParams.Title = "retry_of_terminal_followup_task patchq validation retry"
	retryParams.Tags = []string{"patch-queue", "validation", "retry_of_terminal_followup_task"}
	raw, err = json.Marshal(retryParams)
	if err != nil {
		t.Fatalf("marshal canonical retry params: %v", err)
	}
	if _, rpcErr := h.taskSubmit(ctx, raw); rpcErr != nil {
		t.Fatalf("canonical terminal retry should be allowed: %+v", rpcErr)
	}

	for _, taskID := range []string{"task-human-review-one", "task-human-review-two"} {
		raw, err = json.Marshal(taskSubmitParams{
			WorkspaceID:  workspaceID,
			TaskID:       taskID,
			OwnerUserID:  "developer",
			Priority:     "normal",
			Title:        "Review customer-facing wording",
			Description:  "Ordinary human review without patch queue identity.",
			TaskKind:     model.TaskKindCoordination,
			TaskTemplate: model.TaskTemplateGeneric,
			LinkedBy:     "dashboard",
			Graph:        dag.DefaultGraph(),
		})
		if err != nil {
			t.Fatalf("marshal non-patch params: %v", err)
		}
		if _, rpcErr := h.taskSubmit(ctx, raw); rpcErr != nil {
			t.Fatalf("non-patch review task %s should be allowed: %+v", taskID, rpcErr)
		}
	}
}

func TestTaskSubmitRuntimeEventFailureDoesNotPublishLiveTaskCreated(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-created-runtime-fail", "agent", "agent-missing-runtime")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-task-created-runtime-fail",
		Title:       "Task Created Runtime Failure",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	ch := h.GetEventBus().Subscribe("ws-task-created-runtime-fail")
	defer h.GetEventBus().Unsubscribe("ws-task-created-runtime-fail", ch)

	raw, err := json.Marshal(taskSubmitParams{
		TaskID:      "task-created-runtime-fail",
		OwnerUserID: "developer",
		Priority:    "HIGH",
		Title:       "No live false green",
		Description: "Runtime event append failure must not publish task.created.",
		TaskKind:    model.TaskKindExecution,
		WorkspaceID: "ws-task-created-runtime-fail",
		LinkedBy:    "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal task submit params: %v", err)
	}

	result, rpcErr := h.taskSubmit(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected taskSubmit to fail when durable runtime event cannot be appended")
	}
	if result != nil {
		t.Fatalf("expected no result on runtime event failure, got %+v", result)
	}
	if rpcErr.Message == "" {
		t.Fatalf("expected runtime append error message, got %+v", rpcErr)
	}
	if _, err := store.GetTaskStatus(ctx, "", "task-created-runtime-fail"); err == nil {
		t.Fatal("expected task create runtime failure to roll back task creation")
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)
}

func TestTaskClosePublishesTaskClosedRuntimeEventWithPromptContext(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-closed-live", "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-task-closed-live",
		Title:       "Task Closed Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, "ws-task-closed-live")
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-task-closed-live", Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate close fixture graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       "task-closed-live",
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Close live graph",
		Description:  "Verify task.closed reaches durable runtime.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create close task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-task-closed-live",
		TaskID:      "task-closed-live",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach close task: %v", err)
	}

	ch := h.GetEventBus().Subscribe("ws-task-closed-live")
	defer h.GetEventBus().Unsubscribe("ws-task-closed-live", ch)

	raw, err := json.Marshal(taskCloseParams{
		WorkspaceID: "ws-task-closed-live",
		TaskID:      "task-closed-live",
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "coordination done",
	})
	if err != nil {
		t.Fatalf("marshal task close params: %v", err)
	}
	if _, rpcErr := h.taskClose(ctx, raw); rpcErr != nil {
		t.Fatalf("taskClose rpc error: %+v", rpcErr)
	}

	live := nextEventOfType(t, ch, "task.closed")
	if live.WorkspaceID != "ws-task-closed-live" || live.EntityType != "task" || live.EntityID != "task-closed-live" {
		t.Fatalf("unexpected task.closed envelope %+v", live)
	}
	if live.CanonicalEventType != "task.closed" {
		t.Fatalf("expected canonical_event_type task.closed, got %+v", live)
	}
	if live.Summary != "Task closed: task-closed-live" {
		t.Fatalf("unexpected task.closed summary %+v", live)
	}

	payload := decodeEventPayloadMap(t, live.PayloadJSON)
	if payload["workspace_id"] != "ws-task-closed-live" || payload["task_id"] != "task-closed-live" {
		t.Fatalf("unexpected task.closed payload %+v", payload)
	}
	if payload["status"] != model.TaskStatusResolved || payload["reason"] != "coordination done" {
		t.Fatalf("unexpected task.closed payload %+v", payload)
	}

	persisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-task-closed-live",
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    "task-closed-live",
		TaskID:      "task-closed-live",
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, live, persisted, "")
	assertPayloadMatchesRuntimeEvent(t, payload, persisted.PayloadJSON)
	assertTaskRuntimePromptContext(t, persisted, "task.close", "ws-task-closed-live", "human", "developer")
	if persisted.AuthorityHolderNodeID == "" || persisted.AuthorityTerm <= 0 || persisted.AuthorityLeaseTokenFingerprint == "" {
		t.Fatalf("task.closed must carry authority metadata, got %+v", persisted)
	}
}

func TestTaskCloseRuntimeEventFailureDoesNotPublishLiveTaskClosed(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-closed-runtime-fail", "agent", "agent-missing-runtime")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-task-closed-runtime-fail",
		Title:       "Task Closed Runtime Failure",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-task-closed-runtime-fail", Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate close runtime failure graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       "task-closed-runtime-fail",
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Close runtime failure",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create close runtime failure task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-task-closed-runtime-fail",
		TaskID:      "task-closed-runtime-fail",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach close runtime failure task: %v", err)
	}

	ch := h.GetEventBus().Subscribe("ws-task-closed-runtime-fail")
	defer h.GetEventBus().Unsubscribe("ws-task-closed-runtime-fail", ch)

	raw, err := json.Marshal(taskCloseParams{
		WorkspaceID: "ws-task-closed-runtime-fail",
		TaskID:      "task-closed-runtime-fail",
		ActorID:     "agent-missing-runtime",
		Resolution:  model.TaskStatusResolved,
		Reason:      "runtime append should fail",
	})
	if err != nil {
		t.Fatalf("marshal task close params: %v", err)
	}

	result, rpcErr := h.taskClose(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected taskClose to fail when durable runtime event cannot be appended")
	}
	if result != nil {
		t.Fatalf("expected no result on runtime event failure, got %+v", result)
	}
	if rpcErr.Message == "" {
		t.Fatalf("expected runtime append error message, got %+v", rpcErr)
	}
	status, err := store.GetTaskStatus(ctx, "ws-task-closed-runtime-fail", "task-closed-runtime-fail")
	if err != nil {
		t.Fatalf("get task status after rolled-back close: %v", err)
	}
	if status.Status != model.TaskStatusPending {
		t.Fatalf("expected task close runtime failure to roll back status to PENDING, got %+v", status)
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)
}

func TestTaskCloseRejectsActorAuthMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-close-actor-mismatch", "human", "developer")

	const (
		workspaceID = "ws-task-close-actor-mismatch"
		taskID      = "task-close-actor-mismatch"
	)
	createCloseableCoordinationTaskFixture(t, ctx, store, workspaceID, taskID)

	raw, err := json.Marshal(taskCloseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "other-human",
		Resolution:  model.TaskStatusResolved,
		Reason:      "wrong actor",
	})
	if err != nil {
		t.Fatalf("marshal close params: %v", err)
	}
	result, rpcErr := h.taskClose(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected task.close actor mismatch permission error, result=%+v err=%+v", result, rpcErr)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status after rejected actor mismatch: %v", err)
	}
	if status.Status != model.TaskStatusPending {
		t.Fatalf("actor mismatch should not mutate task status, got %+v", status)
	}
}

func TestTaskCloseSystemPrincipalUsesAuthenticatedActorForDurableProvenance(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-task-close-system-actor"
		taskID      = "task-close-system-actor"
	)
	ctx := testAuthContext(workspaceID, "system", "system-control-plane")
	createCloseableCoordinationTaskFixture(t, ctx, store, workspaceID, taskID)

	raw, err := json.Marshal(taskCloseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "body-spoof",
		Resolution:  model.TaskStatusResolved,
		Reason:      "system close should use token actor",
	})
	if err != nil {
		t.Fatalf("marshal close params: %v", err)
	}
	if _, rpcErr := h.taskClose(ctx, raw); rpcErr != nil {
		t.Fatalf("taskClose system rpc error: %+v", rpcErr)
	}

	event := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       1,
	})
	if event.ActorID != "system-control-plane" {
		t.Fatalf("expected auth-derived event actor, got %+v", event)
	}
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	if payload["actor_id"] != "system-control-plane" {
		t.Fatalf("expected auth-derived payload actor, got %+v", payload)
	}
	var nodeActor string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT actor_id FROM node_state_transitions WHERE task_id = ? ORDER BY created_at DESC LIMIT 1`,
		taskID,
	).Scan(&nodeActor); err != nil {
		t.Fatalf("query node transition actor: %v", err)
	}
	if nodeActor != "system-control-plane" {
		t.Fatalf("expected auth-derived node transition actor, got %q", nodeActor)
	}
}

func TestTaskCloseRejectsTerminalStatusRewrite(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-close-terminal-rewrite", "human", "developer")

	const (
		workspaceID = "ws-task-close-terminal-rewrite"
		taskID      = "task-close-terminal-rewrite"
	)
	createCloseableCoordinationTaskFixture(t, ctx, store, workspaceID, taskID)

	firstRaw, err := json.Marshal(taskCloseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "initial close",
	})
	if err != nil {
		t.Fatalf("marshal first close params: %v", err)
	}
	if _, rpcErr := h.taskClose(ctx, firstRaw); rpcErr != nil {
		t.Fatalf("first taskClose rpc error: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	secondRaw, err := json.Marshal(taskCloseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusCancelled,
		Reason:      "should not rewrite terminal truth",
	})
	if err != nil {
		t.Fatalf("marshal second close params: %v", err)
	}
	result, rpcErr := h.taskClose(ctx, secondRaw)
	if rpcErr == nil {
		t.Fatal("expected terminal rewrite to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on terminal rewrite, got %+v", result)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status after rejected rewrite: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("expected terminal status to remain RESOLVED, got %+v", status)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events after rejected rewrite: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected only the original task.closed event, got %+v", events)
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)
}

func TestTaskCloseRepeatedSameResolutionDoesNotPublishLiveTaskClosed(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-task-close-repeat", "human", "developer")

	const (
		workspaceID = "ws-task-close-repeat"
		taskID      = "task-close-repeat"
	)
	createCloseableCoordinationTaskFixture(t, ctx, store, workspaceID, taskID)

	raw, err := json.Marshal(taskCloseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "done once",
	})
	if err != nil {
		t.Fatalf("marshal first close params: %v", err)
	}
	if _, rpcErr := h.taskClose(ctx, raw); rpcErr != nil {
		t.Fatalf("first taskClose rpc error: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	repeatRaw, err := json.Marshal(taskCloseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "same resolution replay",
	})
	if err != nil {
		t.Fatalf("marshal repeat close params: %v", err)
	}
	result, rpcErr := h.taskClose(ctx, repeatRaw)
	if rpcErr != nil {
		t.Fatalf("repeat same-resolution taskClose should be idempotent, got %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok || resultMap["status"] != "CLOSED" {
		t.Fatalf("unexpected repeat close result %+v", result)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events after repeat close: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected repeat close to avoid duplicate runtime event, got %+v", events)
	}
	assertNoEventWithin(t, ch, 20*time.Millisecond)
}

func TestAgentTaskLifecyclePublishesSSEMirrorsRuntimePayload(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID  = "ws-task-lifecycle-sse"
		agentID      = "agent-task-lifecycle-sse"
		claimTaskID  = "task-claim-release-sse"
		completeTask = "task-complete-sse"
		blockTask    = "task-block-sse"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, claimTaskID, completeTask, blockTask)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      completeTask,
		AgentID:     agentID,
		Summary:     "claim before complete",
	}); err != nil {
		t.Fatalf("claim complete task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      blockTask,
		AgentID:     agentID,
		Summary:     "claim before block",
	}); err != nil {
		t.Fatalf("claim block task: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawClaim, err := json.Marshal(agentTaskClaimParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      claimTaskID,
		Summary:     "starting runtime follow-up",
	})
	if err != nil {
		t.Fatalf("marshal claim params: %v", err)
	}
	if _, rpcErr := h.agentTaskClaim(ctx, rawClaim); rpcErr != nil {
		t.Fatalf("agentTaskClaim rpc error: %+v", rpcErr)
	}
	claimEvent := nextEvent(t, ch)
	claimPayload := decodeEventPayloadMap(t, claimEvent.PayloadJSON)
	assertTaskLifecyclePayload(t, claimPayload, workspaceID, claimTaskID, agentID)
	if claimPayload["summary"] != "starting runtime follow-up" {
		t.Fatalf("unexpected claim payload %+v", claimPayload)
	}
	claimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.claimed",
		EntityType:  "task",
		EntityID:    claimTaskID,
		TaskID:      claimTaskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, claimEvent, claimPersisted, "")
	assertPayloadMatchesRuntimeEvent(t, claimPayload, claimPersisted.PayloadJSON)
	assertAgentTaskRuntimePromptContext(t, claimPersisted, "agent.task.claim", workspaceID, claimTaskID, agentID)

	rawRelease, err := json.Marshal(agentTaskReleaseParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      claimTaskID,
		Reason:      "waiting on docs",
	})
	if err != nil {
		t.Fatalf("marshal release params: %v", err)
	}
	if _, rpcErr := h.agentTaskRelease(ctx, rawRelease); rpcErr != nil {
		t.Fatalf("agentTaskRelease rpc error: %+v", rpcErr)
	}
	releaseEvent := nextEvent(t, ch)
	releasePayload := decodeEventPayloadMap(t, releaseEvent.PayloadJSON)
	assertTaskLifecyclePayload(t, releasePayload, workspaceID, claimTaskID, agentID)
	if releasePayload["reason"] != "waiting on docs" {
		t.Fatalf("unexpected release payload %+v", releasePayload)
	}
	releasePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.released",
		EntityType:  "task",
		EntityID:    claimTaskID,
		TaskID:      claimTaskID,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, releaseEvent, releasePersisted, "")
	assertPayloadMatchesRuntimeEvent(t, releasePayload, releasePersisted.PayloadJSON)
	assertAgentTaskRuntimePromptContext(t, releasePersisted, "agent.task.release", workspaceID, claimTaskID, agentID)

	rawComplete, err := json.Marshal(taskCompleteParams{
		WorkspaceID: workspaceID,
		TaskID:      completeTask,
		AgentID:     agentID,
		Summary:     "done and verified",
	})
	if err != nil {
		t.Fatalf("marshal complete params: %v", err)
	}
	if _, rpcErr := h.agentTaskComplete(ctx, rawComplete); rpcErr != nil {
		t.Fatalf("agentTaskComplete rpc error: %+v", rpcErr)
	}
	completeEvent := nextEvent(t, ch)
	completePayload := decodeEventPayloadMap(t, completeEvent.PayloadJSON)
	assertTaskLifecyclePayload(t, completePayload, workspaceID, completeTask, agentID)
	if completePayload["claim_status"] != "COMPLETED" || completePayload["summary"] != "done and verified" {
		t.Fatalf("unexpected complete payload %+v", completePayload)
	}
	completePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.completed",
		EntityType:  "task",
		EntityID:    completeTask,
		TaskID:      completeTask,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, completeEvent, completePersisted, "")
	assertPayloadMatchesRuntimeEvent(t, completePayload, completePersisted.PayloadJSON)
	assertAgentTaskRuntimePromptContext(t, completePersisted, "agent.task.complete", workspaceID, completeTask, agentID)

	rawBlock, err := json.Marshal(taskBlockParams{
		WorkspaceID: workspaceID,
		TaskID:      blockTask,
		AgentID:     agentID,
		Reason:      "need product decision",
	})
	if err != nil {
		t.Fatalf("marshal block params: %v", err)
	}
	if _, rpcErr := h.agentTaskBlock(ctx, rawBlock); rpcErr != nil {
		t.Fatalf("agentTaskBlock rpc error: %+v", rpcErr)
	}
	blockEvent := nextEvent(t, ch)
	blockPayload := decodeEventPayloadMap(t, blockEvent.PayloadJSON)
	assertTaskLifecyclePayload(t, blockPayload, workspaceID, blockTask, agentID)
	if blockPayload["claim_status"] != "BLOCKED" || blockPayload["reason"] != "need product decision" {
		t.Fatalf("unexpected block payload %+v", blockPayload)
	}
	blockPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.blocked",
		EntityType:  "task",
		EntityID:    blockTask,
		TaskID:      blockTask,
		AgentID:     agentID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, blockEvent, blockPersisted, "")
	assertPayloadMatchesRuntimeEvent(t, blockPayload, blockPersisted.PayloadJSON)
	assertAgentTaskRuntimePromptContext(t, blockPersisted, "agent.task.block", workspaceID, blockTask, agentID)
}

func createAgentTaskLifecycleFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string, taskIDs ...string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Lifecycle SSE",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Lifecycle Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	for _, taskID := range taskIDs {
		graph := dag.NormalizeGraph(dag.Graph{
			Nodes: []dag.NodeSpec{{NodeID: "node-task-sse-" + taskID, Type: "generic"}},
		})
		if err := dag.ValidateGraph(graph); err != nil {
			t.Fatalf("validate graph for %s: %v", taskID, err)
		}
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			TaskID:      taskID,
			OwnerUserID: "developer",
			Priority:    "normal",
			Title:       "Task Lifecycle SSE",
			Description: "fixture",
			ProjectID:   "",
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
}

func createCloseableCoordinationTaskFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Closeable Coordination Task",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate closeable graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Closeable coordination task",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create closeable task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach closeable task: %v", err)
	}
}

func mustRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, filter sqlite.RuntimeEventFilter) sqlite.RuntimeEventRecord {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	if len(events) == 0 {
		t.Fatalf("expected runtime event for filter %+v", filter)
	}
	return events[0]
}

func decodeEventPayloadMap(t *testing.T, payload string) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("decode payload %q: %v", payload, err)
	}
	return decoded
}

func assertPayloadMatchesRuntimeEvent(t *testing.T, livePayload map[string]any, runtimePayloadJSON string) {
	t.Helper()

	runtimePayload := decodeEventPayloadMap(t, runtimePayloadJSON)
	if !reflect.DeepEqual(livePayload, runtimePayload) {
		t.Fatalf("expected live payload %+v to match runtime payload %+v", livePayload, runtimePayload)
	}
}

func assertTaskLifecyclePayload(t *testing.T, payload map[string]any, workspaceID, taskID, agentID string) {
	t.Helper()

	if payload["workspace_id"] != workspaceID || payload["task_id"] != taskID || payload["agent_id"] != agentID {
		t.Fatalf("unexpected task lifecycle payload %+v", payload)
	}
}

func assertTaskRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantPrincipalType, wantPrincipalID string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected task prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	assertTaskPromptContextField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertTaskPromptContextField(t, envelope, "context_kind", "authority_bearing_task_write")
	assertTaskPromptContextField(t, envelope, "surface", wantSurface)
	assertTaskPromptContextField(t, envelope, "origin", "server_rpc")
	assertTaskPromptContextField(t, envelope, "workspace_id", wantWorkspaceID)
	assertTaskPromptContextField(t, envelope, "principal_type", wantPrincipalType)
	assertTaskPromptContextField(t, envelope, "principal_id", wantPrincipalID)
	assertTaskPromptContextField(t, envelope, "authority_model", "workspace_authority")
	assertTaskPromptContextField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertTaskPromptContextField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertTaskPromptContextField(t, envelope, "prompt_capability_evidence", "not_present")
}

func assertAgentTaskRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantTaskID, wantAgentID string) {
	t.Helper()

	assertTaskRuntimePromptContext(t, event, wantSurface, wantWorkspaceID, "agent", wantAgentID)
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope := payload["prompt_context_envelope"].(map[string]any)
	assertTaskPromptContextField(t, envelope, "actor_agent_id", wantAgentID)
	assertTaskPromptContextField(t, envelope, "agent_id", wantAgentID)
	assertTaskPromptContextField(t, envelope, "task_id", wantTaskID)
	wantClaimStatus := agentTaskClaimStatusForSurface(wantSurface)
	if payload["claim_status"] != wantClaimStatus {
		t.Fatalf("task runtime payload claim_status = %v, want %s in %+v", payload["claim_status"], wantClaimStatus, payload)
	}
	assertTaskPromptContextField(t, envelope, "claim_status", wantClaimStatus)
}

func agentTaskClaimStatusForSurface(surface string) string {
	switch surface {
	case "agent.task.claim":
		return model.TaskClaimStatusClaimed
	case "agent.task.release":
		return model.TaskClaimStatusReleased
	case "agent.task.complete":
		return model.TaskClaimStatusCompleted
	case "agent.task.block":
		return model.TaskClaimStatusBlocked
	default:
		return ""
	}
}

func assertTaskPromptContextField(t *testing.T, envelope map[string]any, key, want string) {
	t.Helper()

	got, ok := envelope[key].(string)
	if !ok {
		t.Fatalf("task prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
	}
	if got != want {
		t.Fatalf("task prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
	}
}
