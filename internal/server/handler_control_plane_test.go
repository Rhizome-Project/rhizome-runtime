package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/sessionmemory"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestToolCallHonorsCapabilityPolicy(t *testing.T) {
	testCases := []struct {
		name      string
		effect    string
		wantEvent string
	}{
		{name: "deny", effect: "DENY", wantEvent: "tool.call.denied"},
		{name: "require approval", effect: "REQUIRE_APPROVAL", wantEvent: "tool.call.approval_required"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newServerTestStore(t)
			h := NewHandler(store)

			workspaceID := "ws-tool-policy-" + tc.effect
			ctx := testAuthContext(workspaceID, "agent", "agent-a")
			if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
				WorkspaceID: workspaceID,
				Title:       "Tool Policy",
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
			if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
				WorkspaceID: workspaceID,
				SubjectType: "agent",
				SubjectID:   "agent-a",
				Capability:  "tool.call",
				ToolID:      "dangerous-tool",
				Effect:      tc.effect,
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("put capability policy: %v", err)
			}

			raw, err := json.Marshal(toolCallParams{
				ToolID:      "dangerous-tool",
				WorkspaceID: workspaceID,
				ActorType:   "agent",
				ActorID:     "agent-a",
			})
			if err != nil {
				t.Fatalf("marshal tool call params: %v", err)
			}

			ch := h.GetEventBus().Subscribe(workspaceID)
			defer h.GetEventBus().Unsubscribe(workspaceID, ch)
			runtimeFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   tc.wantEvent,
				EntityType:  "tool",
				EntityID:    "dangerous-tool",
				Limit:       10,
			}

			if _, rpcErr := h.toolCall(ctx, raw); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
				t.Fatalf("expected permission denied rpc error, got %+v", rpcErr)
			}
			firstRuntime := mustRuntimeEvent(t, ctx, store, runtimeFilter)
			assertToolCallRuntimePromptContext(t, firstRuntime, tc.wantEvent, workspaceID, "agent", "agent-a", "agent", "agent-a", "dangerous-tool", "tool.call", "")
			live := nextEvent(t, ch)
			assertValidEventTimestamp(t, live.Timestamp)
			assertLiveEventMirrorsRuntimeEvent(t, live, firstRuntime, tc.wantEvent)
			assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), firstRuntime.PayloadJSON)

			seenEventIDs := snapshotRuntimeEventIDs(t, ctx, store, runtimeFilter)
			if _, rpcErr := h.toolCall(ctx, raw); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
				t.Fatalf("expected second permission denied rpc error, got %+v", rpcErr)
			}
			secondRuntime := mustNewRuntimeEvent(t, ctx, store, runtimeFilter, seenEventIDs)
			assertToolCallRuntimePromptContext(t, secondRuntime, tc.wantEvent, workspaceID, "agent", "agent-a", "agent", "agent-a", "dangerous-tool", "tool.call", "")
			secondLive := nextEvent(t, ch)
			assertValidEventTimestamp(t, secondLive.Timestamp)
			assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondRuntime, tc.wantEvent)
			assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondLive.PayloadJSON), secondRuntime.PayloadJSON)
			if secondRuntime.EventID == firstRuntime.EventID || secondRuntime.IngestSeq <= firstRuntime.IngestSeq {
				t.Fatalf("expected second runtime event to advance beyond first, got first=%+v second=%+v", firstRuntime, secondRuntime)
			}
		})
	}
}

func TestWorkspaceExecutionWriteAddsPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-exec-context-rpc"
	ctx := testAuthContext(workspaceID, "human", "operator-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Context RPC",
		CreatedBy:   "operator-a",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, mustJSONRaw(workspaceExecutionRunWriteParams{
		WorkspaceID: workspaceID,
		RunID:       "run-rpc-context",
		Title:       "RPC execution run",
		Status:      "ACTIVE",
	}))
	if rpcErr != nil {
		t.Fatalf("write execution run: %+v", rpcErr)
	}
	run := runAny.(map[string]any)["run"].(sqlite.ExecutionRunRecord)
	runEnvelope := run.VerificationJSON["prompt_context_envelope"].(map[string]any)
	if got := runEnvelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected run context contract: %v", got)
	}
	if got := runEnvelope["surface"]; got != "workspace.execution.run.write" {
		t.Fatalf("unexpected run context surface: %v", got)
	}
	if got := runEnvelope["origin"]; got != "server_rpc" {
		t.Fatalf("unexpected run context origin: %v", got)
	}
	if got := runEnvelope["principal_id"]; got != "operator-a" {
		t.Fatalf("unexpected run context principal: %+v", runEnvelope)
	}
	runEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    run.RunID,
		Limit:       1,
	})
	runEventVerification := decodeEventPayloadMap(t, runEvent.PayloadJSON)["verification"].(map[string]any)
	if got := runEventVerification["prompt_context_envelope"].(map[string]any)["surface"]; got != "workspace.execution.run.write" {
		t.Fatalf("unexpected run event context surface: %v", got)
	}

	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, mustJSONRaw(workspaceExecutionStepWriteParams{
		WorkspaceID: workspaceID,
		RunID:       "run-rpc-context",
		Phase:       "VERIFY",
		Title:       "RPC execution step",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"gate": "pass",
		},
	}))
	if rpcErr != nil {
		t.Fatalf("write execution step: %+v", rpcErr)
	}
	step := stepAny.(map[string]any)["step"].(sqlite.ExecutionStepRecord)
	if step.VerificationJSON["gate"] != "pass" {
		t.Fatalf("expected caller verification to be preserved, got %+v", step.VerificationJSON)
	}
	stepEnvelope := step.VerificationJSON["prompt_context_envelope"].(map[string]any)
	if got := stepEnvelope["surface"]; got != "workspace.execution.step.write" {
		t.Fatalf("unexpected step context surface: %v", got)
	}
	if got := stepEnvelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("manual RPC context must not claim daemon convergence: %+v", stepEnvelope)
	}
	stepEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    step.StepID,
		Limit:       1,
	})
	stepEventVerification := decodeEventPayloadMap(t, stepEvent.PayloadJSON)["verification"].(map[string]any)
	if stepEventVerification["gate"] != "pass" {
		t.Fatalf("expected step runtime event to preserve caller verification, got %+v", stepEventVerification)
	}
	if got := stepEventVerification["prompt_context_envelope"].(map[string]any)["surface"]; got != "workspace.execution.step.write" {
		t.Fatalf("unexpected step event context surface: %v", got)
	}
}

func TestWorkspaceOpsWritesAddPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-ops-context-rpc"
	ctx := testAuthContext(workspaceID, "human", "operator-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Context RPC",
		CreatedBy:   "operator-a",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	upsertAny, rpcErr := h.workspaceOpsUpsert(ctx, mustJSONRaw(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:handoff",
		QueueType:   "HANDOFF",
		Title:       "Manual handoff",
		Summary:     "Need operator action",
		SourceKind:  "manual",
		SourceID:    "operator-a",
	}))
	if rpcErr != nil {
		t.Fatalf("upsert operator queue: %+v", rpcErr)
	}
	upserted := upsertAny.(map[string]any)["item"].(sqlite.OperatorQueueRecord)
	assertOperatorQueuePromptContextSurface(t, upserted.PayloadJSON, "workspace.ops.upsert", "operator-a")

	escalateAny, rpcErr := h.workspaceOpsEscalate(ctx, mustJSONRaw(workspaceOpsEscalateParams{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:handoff",
		EscalatedBy: "operator-a",
		Reason:      "needs a tighter handoff",
		Urgency:     "HIGH",
	}))
	if rpcErr != nil {
		t.Fatalf("escalate operator queue: %+v", rpcErr)
	}
	escalated := escalateAny.(map[string]any)["item"].(sqlite.OperatorQueueRecord)
	assertOperatorQueuePromptContextSurface(t, escalated.PayloadJSON, "workspace.ops.escalate", "operator-a")

	resolveAny, rpcErr := h.workspaceOpsResolve(ctx, mustJSONRaw(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueKey:         "manual:handoff",
		ResolvedBy:       "operator-a",
		Resolution:       "accepted",
		CurrentRevision:  escalated.Revision,
		CurrentUpdatedAt: escalated.UpdatedAt,
	}))
	if rpcErr != nil {
		t.Fatalf("resolve operator queue: %+v", rpcErr)
	}
	resolved := resolveAny.(map[string]any)["item"].(sqlite.OperatorQueueRecord)
	assertOperatorQueuePromptContextSurface(t, resolved.PayloadJSON, "workspace.ops.resolve", "operator-a")
}

func assertOperatorQueuePromptContextSurface(t *testing.T, payloadJSON, wantSurface, wantPrincipalID string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode operator queue payload_json: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected operator queue prompt context envelope, got %+v", payload)
	}
	if got := envelope["surface"]; got != wantSurface {
		t.Fatalf("unexpected operator queue context surface: got %v want %s", got, wantSurface)
	}
	if got := envelope["origin"]; got != "server_rpc" {
		t.Fatalf("unexpected operator queue context origin: %v", got)
	}
	if got := envelope["principal_id"]; got != wantPrincipalID {
		t.Fatalf("unexpected operator queue context principal: %+v", envelope)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_operator_queue_write" {
		t.Fatalf("unexpected operator queue context kind: %v", got)
	}
}

func TestWorkspaceOpsRequestCreatesTypedGateQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-typed-gate-request"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Typed Gate Request",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-gate",
		OwnerUserID: "developer",
		DisplayName: "Gate Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	cases := []struct {
		name               string
		gateType           string
		wantQueueType      string
		wantDefaultUrgency string
	}{
		{name: "payment billing", gateType: "PAYMENT_BILLING", wantQueueType: "BLOCKER", wantDefaultUrgency: "HIGH"},
		{name: "explicit approval", gateType: "EXPLICIT_APPROVAL", wantQueueType: "DECISION", wantDefaultUrgency: "NORMAL"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requestKey := "request-" + strings.ToLower(strings.ReplaceAll(tc.gateType, "_", "-"))
			raw, err := json.Marshal(workspaceOpsRequestParams{
				WorkspaceID: workspaceID,
				RequestKey:  requestKey,
				GateType:    tc.gateType,
				Title:       "Human gate for " + tc.gateType,
				Summary:     "Typed gate request",
				Details:     "Please unblock the flow",
				AssignedTo:  "operator-queue",
				AgentID:     "agent-gate",
			})
			if err != nil {
				t.Fatalf("marshal request params: %v", err)
			}

			result, rpcErr := h.workspaceOpsRequest(testAuthContext(workspaceID, "system", "tests"), raw)
			if rpcErr != nil {
				t.Fatalf("workspaceOpsRequest rpc error: %+v", rpcErr)
			}
			payload, ok := result.(map[string]any)
			if !ok {
				t.Fatalf("unexpected result type %T", result)
			}
			item, ok := payload["item"].(sqlite.OperatorQueueRecord)
			if !ok {
				t.Fatalf("unexpected item type %T", payload["item"])
			}
			if item.QueueType != tc.wantQueueType || item.Status != "OPEN" {
				t.Fatalf("unexpected queue item %+v", item)
			}
			if item.TimeAuthority.WorkspaceID != workspaceID || item.TimeAuthority.ReferenceAt == "" {
				t.Fatalf("expected queue item time authority, got %+v", item.TimeAuthority)
			}
			if item.PayloadJSON == "" {
				t.Fatal("expected typed payload_json to be persisted")
			}
			if item.Urgency != tc.wantDefaultUrgency {
				t.Fatalf("expected default urgency %q, got %+v", tc.wantDefaultUrgency, item)
			}
			var envelope map[string]any
			if err := json.Unmarshal([]byte(item.PayloadJSON), &envelope); err != nil {
				t.Fatalf("unmarshal payload_json: %v", err)
			}
			if envelope["gate_type"] != tc.gateType {
				t.Fatalf("unexpected gate_type envelope %+v", envelope)
			}
			if envelope["queue_type"] != tc.wantQueueType {
				t.Fatalf("unexpected queue_type envelope %+v", envelope)
			}
			if envelope["request_key"] != requestKey {
				t.Fatalf("unexpected request_key envelope %+v", envelope)
			}
			if envelope["source_kind"] != "external_gate" {
				t.Fatalf("unexpected source_kind envelope %+v", envelope)
			}
			if envelope["keep_session_active"] != true {
				t.Fatalf("expected keep_session_active default to be true, got %+v", envelope)
			}
			assertOperatorQueuePromptContextSurface(t, item.PayloadJSON, "workspace.ops.request", "tests")

			listRaw, err := json.Marshal(workspaceOpsListParams{
				WorkspaceID: workspaceID,
				QueueType:   tc.wantQueueType,
				AgentID:     "agent-gate",
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("marshal list params: %v", err)
			}
			listResult, rpcErr := h.workspaceOpsList(testAuthContext(workspaceID, "system", "tests"), listRaw)
			if rpcErr != nil {
				t.Fatalf("workspaceOpsList rpc error: %+v", rpcErr)
			}
			listPayload, ok := listResult.(map[string]any)
			if !ok {
				t.Fatalf("unexpected list result type %T", listResult)
			}
			listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
			if !ok || listAuthority.WorkspaceID != workspaceID || listAuthority.ReferenceAt == "" {
				t.Fatalf("expected list time authority, got %+v", listPayload["time_authority"])
			}
			items, ok := listPayload["items"].([]sqlite.OperatorQueueRecord)
			if !ok {
				t.Fatalf("unexpected list items type %T", listPayload["items"])
			}
			if len(items) != 1 || items[0].QueueKey != item.QueueKey || items[0].PayloadJSON == "" {
				t.Fatalf("unexpected listed queue items %+v", items)
			}
			if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
				t.Fatalf("expected listed queue time authority, got %+v", items[0].TimeAuthority)
			}

			live := nextEvent(t, ch)
			persisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EntityType:  "operator_queue",
				EntityID:    item.QueueID,
				Limit:       1,
			})
			assertLiveEventMirrorsRuntimeEvent(t, live, persisted, "workspace.ops.updated")
		})
	}
}

func TestWorkspaceOpsUpsertAndRequestMirrorNewPersistedRowsForRepeatedQueueKey(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-ops-repeat-runtime"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Repeated Queue Mirror",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	t.Run("workspace ops upsert", func(t *testing.T) {
		ch := h.GetEventBus().Subscribe(workspaceID)
		defer h.GetEventBus().Unsubscribe(workspaceID, ch)

		firstRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID: workspaceID,
			QueueKey:    "manual:repeat-follow-up",
			QueueType:   "FOLLOW_UP",
			Title:       "Repeatable queue",
			Summary:     "First queue state",
			AssignedTo:  "operator-a",
			Urgency:     "HIGH",
			SourceKind:  "manual",
			SourceID:    "developer",
		})
		if err != nil {
			t.Fatalf("marshal first ops upsert params: %v", err)
		}
		firstAny, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), firstRaw)
		if rpcErr != nil {
			t.Fatalf("workspaceOpsUpsert first rpc error: %+v", rpcErr)
		}
		firstPayload, ok := firstAny.(map[string]any)
		if !ok {
			t.Fatalf("unexpected first ops upsert result type %T", firstAny)
		}
		firstItem, ok := firstPayload["item"].(sqlite.OperatorQueueRecord)
		if !ok {
			t.Fatalf("unexpected first ops upsert item type %T", firstPayload["item"])
		}
		queueFilter := sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EntityType:  "operator_queue",
			EntityID:    firstItem.QueueID,
			Limit:       10,
		}
		firstLive := nextEventOfType(t, ch, "workspace.ops.updated")
		firstPersisted := mustRuntimeEvent(t, ctx, store, queueFilter)
		assertLiveEventMirrorsRuntimeEvent(t, firstLive, firstPersisted, "workspace.ops.updated")

		seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
		secondRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID: workspaceID,
			QueueKey:    "manual:repeat-follow-up",
			QueueType:   "FOLLOW_UP",
			Title:       "Repeatable queue",
			Summary:     "Second queue state",
			AssignedTo:  "operator-b",
			Urgency:     "CRITICAL",
			SourceKind:  "manual",
			SourceID:    "developer",
		})
		if err != nil {
			t.Fatalf("marshal second ops upsert params: %v", err)
		}
		secondAny, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), secondRaw)
		if rpcErr != nil {
			t.Fatalf("workspaceOpsUpsert second rpc error: %+v", rpcErr)
		}
		secondPayload, ok := secondAny.(map[string]any)
		if !ok {
			t.Fatalf("unexpected second ops upsert result type %T", secondAny)
		}
		secondItem, ok := secondPayload["item"].(sqlite.OperatorQueueRecord)
		if !ok {
			t.Fatalf("unexpected second ops upsert item type %T", secondPayload["item"])
		}
		if secondItem.QueueID != firstItem.QueueID {
			t.Fatalf("expected repeated queue upsert to preserve queue_id %q, got %+v", firstItem.QueueID, secondItem)
		}
		secondLive := nextEventOfType(t, ch, "workspace.ops.updated")
		secondPersisted := mustNewRuntimeEvent(t, ctx, store, queueFilter, seenQueueEvents)
		assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondPersisted, "workspace.ops.updated")
		if secondPersisted.EventID == firstPersisted.EventID || secondPersisted.IngestSeq <= firstPersisted.IngestSeq {
			t.Fatalf("expected second queue runtime event to advance beyond first, got first=%+v second=%+v", firstPersisted, secondPersisted)
		}
		var liveQueue sqlite.OperatorQueueRecord
		if err := json.Unmarshal([]byte(secondLive.PayloadJSON), &liveQueue); err != nil {
			t.Fatalf("decode second queue live payload: %v", err)
		}
		if liveQueue.QueueID != firstItem.QueueID || liveQueue.AssignedTo != "operator-b" || liveQueue.Urgency != "CRITICAL" || liveQueue.Summary != "Second queue state" {
			t.Fatalf("unexpected second queue live payload %+v", liveQueue)
		}
	})

	t.Run("workspace ops request", func(t *testing.T) {
		ch := h.GetEventBus().Subscribe(workspaceID)
		defer h.GetEventBus().Unsubscribe(workspaceID, ch)

		firstRaw, err := json.Marshal(workspaceOpsRequestParams{
			WorkspaceID: workspaceID,
			RequestKey:  "repeat-review",
			GateType:    "EXPLICIT_APPROVAL",
			Title:       "Review deploy gate",
			Summary:     "First request state",
			Details:     "First typed gate request",
			AssignedTo:  "reviewer-a",
			Urgency:     "NORMAL",
		})
		if err != nil {
			t.Fatalf("marshal first ops request params: %v", err)
		}
		firstAny, rpcErr := h.workspaceOpsRequest(testAuthContext(workspaceID, "system", "tests"), firstRaw)
		if rpcErr != nil {
			t.Fatalf("workspaceOpsRequest first rpc error: %+v", rpcErr)
		}
		firstPayload, ok := firstAny.(map[string]any)
		if !ok {
			t.Fatalf("unexpected first ops request result type %T", firstAny)
		}
		firstItem, ok := firstPayload["item"].(sqlite.OperatorQueueRecord)
		if !ok {
			t.Fatalf("unexpected first ops request item type %T", firstPayload["item"])
		}
		if firstItem.Revision != 1 {
			t.Fatalf("expected first ops request revision 1, got %+v", firstItem)
		}
		queueFilter := sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EntityType:  "operator_queue",
			EntityID:    firstItem.QueueID,
			Limit:       10,
		}
		firstLive := nextEventOfType(t, ch, "workspace.ops.updated")
		firstPersisted := mustRuntimeEvent(t, ctx, store, queueFilter)
		assertLiveEventMirrorsRuntimeEvent(t, firstLive, firstPersisted, "workspace.ops.updated")

		seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
		secondRaw, err := json.Marshal(workspaceOpsRequestParams{
			WorkspaceID:      workspaceID,
			RequestKey:       "repeat-review",
			GateType:         "EXPLICIT_APPROVAL",
			Title:            "Review deploy gate",
			Summary:          "Second request state",
			Details:          "Escalated typed gate request",
			AssignedTo:       "reviewer-b",
			Urgency:          "CRITICAL",
			CurrentRevision:  firstItem.Revision,
			CurrentUpdatedAt: firstItem.UpdatedAt,
		})
		if err != nil {
			t.Fatalf("marshal second ops request params: %v", err)
		}
		secondAny, rpcErr := h.workspaceOpsRequest(testAuthContext(workspaceID, "system", "tests"), secondRaw)
		if rpcErr != nil {
			t.Fatalf("workspaceOpsRequest second rpc error: %+v", rpcErr)
		}
		secondPayload, ok := secondAny.(map[string]any)
		if !ok {
			t.Fatalf("unexpected second ops request result type %T", secondAny)
		}
		secondItem, ok := secondPayload["item"].(sqlite.OperatorQueueRecord)
		if !ok {
			t.Fatalf("unexpected second ops request item type %T", secondPayload["item"])
		}
		if secondItem.QueueID != firstItem.QueueID {
			t.Fatalf("expected repeated request to preserve queue_id %q, got %+v", firstItem.QueueID, secondItem)
		}
		if secondItem.Revision != firstItem.Revision+1 {
			t.Fatalf("expected repeated request to advance queue revision from %d to %d, got %+v", firstItem.Revision, firstItem.Revision+1, secondItem)
		}
		secondLive := nextEventOfType(t, ch, "workspace.ops.updated")
		secondPersisted := mustNewRuntimeEvent(t, ctx, store, queueFilter, seenQueueEvents)
		assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondPersisted, "workspace.ops.updated")
		if secondPersisted.EventID == firstPersisted.EventID || secondPersisted.IngestSeq <= firstPersisted.IngestSeq {
			t.Fatalf("expected second request runtime event to advance beyond first, got first=%+v second=%+v", firstPersisted, secondPersisted)
		}
		var liveQueue sqlite.OperatorQueueRecord
		if err := json.Unmarshal([]byte(secondLive.PayloadJSON), &liveQueue); err != nil {
			t.Fatalf("decode second request live payload: %v", err)
		}
		if liveQueue.QueueID != firstItem.QueueID || liveQueue.AssignedTo != "reviewer-b" || liveQueue.Urgency != "CRITICAL" || liveQueue.Summary != "Second request state" {
			t.Fatalf("unexpected second request live payload %+v", liveQueue)
		}

		thirdRaw, err := json.Marshal(workspaceOpsRequestParams{
			WorkspaceID: workspaceID,
			RequestKey:  "repeat-review",
			GateType:    "EXPLICIT_APPROVAL",
			Title:       "Review deploy gate",
			Summary:     "Blind third request should fail",
			Details:     "Blind request without current revision",
			AssignedTo:  "reviewer-c",
			Urgency:     "HIGH",
		})
		if err != nil {
			t.Fatalf("marshal third ops request params: %v", err)
		}
		if _, rpcErr := h.workspaceOpsRequest(testAuthContext(workspaceID, "system", "tests"), thirdRaw); rpcErr == nil {
			t.Fatal("expected repeated request without queue base-version to reject after revision advanced")
		} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "current_revision") {
			t.Fatalf("expected invalid params current_revision guidance on blind repeated request, got %+v", rpcErr)
		}
		if got := snapshotRuntimeEventIDs(t, ctx, store, queueFilter); len(got) != len(seenQueueEvents)+1 {
			t.Fatalf("blind repeated request should not append runtime rows, before=%v after=%v", seenQueueEvents, got)
		}
	})
}

func TestWorkspaceOpsUpsertAcceptsFreshQueueRevisionToken(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-ops-upsert-fresh-revision"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Ops Upsert Fresh Revision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	firstRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:fresh-revision-follow-up",
		QueueType:   "FOLLOW_UP",
		Title:       "Fresh revision queue",
		Summary:     "Initial queue state",
		AssignedTo:  "operator-a",
		Urgency:     "NORMAL",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("marshal first ops upsert params: %v", err)
	}
	firstAny, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), firstRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsUpsert first rpc error: %+v", rpcErr)
	}
	firstPayload, ok := firstAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first ops upsert result type %T", firstAny)
	}
	firstItem, ok := firstPayload["item"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected first ops upsert item type %T", firstPayload["item"])
	}
	if firstItem.Revision != 1 {
		t.Fatalf("expected first queue revision 1, got %+v", firstItem)
	}
	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    firstItem.QueueID,
		Limit:       10,
	}
	firstLive := nextEventOfType(t, ch, "workspace.ops.updated")
	firstPersisted := mustRuntimeEvent(t, ctx, store, queueFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstLive, firstPersisted, "workspace.ops.updated")

	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)
	secondRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          firstItem.QueueID,
		QueueKey:         firstItem.QueueKey,
		QueueType:        firstItem.QueueType,
		Title:            firstItem.Title,
		Summary:          "Fresh guarded queue state",
		AssignedTo:       "operator-b",
		Urgency:          "HIGH",
		SourceKind:       firstItem.SourceKind,
		SourceID:         firstItem.SourceID,
		CurrentRevision:  firstItem.Revision,
		CurrentUpdatedAt: firstItem.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal second ops upsert params: %v", err)
	}
	secondAny, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), secondRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsUpsert second rpc error: %+v", rpcErr)
	}
	secondPayload, ok := secondAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second ops upsert result type %T", secondAny)
	}
	secondItem, ok := secondPayload["item"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected second ops upsert item type %T", secondPayload["item"])
	}
	if secondItem.QueueID != firstItem.QueueID {
		t.Fatalf("fresh guarded upsert should preserve queue_id %q, got %+v", firstItem.QueueID, secondItem)
	}
	if secondItem.Summary != "Fresh guarded queue state" || secondItem.AssignedTo != "operator-b" || secondItem.Urgency != "HIGH" {
		t.Fatalf("fresh guarded upsert did not persist edits: %+v", secondItem)
	}
	if secondItem.UpdatedAt == firstItem.UpdatedAt {
		t.Fatalf("fresh guarded upsert should advance updated_at, got %q", secondItem.UpdatedAt)
	}
	if secondItem.Revision != firstItem.Revision+1 {
		t.Fatalf("fresh guarded upsert should advance revision from %d to %d, got %+v", firstItem.Revision, firstItem.Revision+1, secondItem)
	}

	secondLive := nextEventOfType(t, ch, "workspace.ops.updated")
	secondPersisted := mustNewRuntimeEvent(t, ctx, store, queueFilter, seenQueueEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondPersisted, "workspace.ops.updated")
}

func TestWorkspaceOpsUpsertRejectsStaleQueueSnapshot(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-ops-upsert-stale-revision"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Ops Upsert Stale Revision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	firstRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:stale-revision-follow-up",
		QueueType:   "FOLLOW_UP",
		Title:       "Stale revision queue",
		Summary:     "Initial queue state",
		AssignedTo:  "operator-a",
		Urgency:     "NORMAL",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("marshal first ops upsert params: %v", err)
	}
	firstAny, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), firstRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsUpsert first rpc error: %+v", rpcErr)
	}
	firstPayload, ok := firstAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first ops upsert result type %T", firstAny)
	}
	firstItem, ok := firstPayload["item"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected first ops upsert item type %T", firstPayload["item"])
	}
	staleRevision := firstItem.Revision
	staleUpdatedAt := firstItem.UpdatedAt

	freshRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          firstItem.QueueID,
		QueueKey:         firstItem.QueueKey,
		QueueType:        firstItem.QueueType,
		Title:            firstItem.Title,
		Summary:          "Fresh queue state before stale reject",
		AssignedTo:       "operator-fresh",
		Urgency:          "CRITICAL",
		SourceKind:       firstItem.SourceKind,
		SourceID:         firstItem.SourceID,
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal fresh ops upsert params: %v", err)
	}
	freshAny, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), freshRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsUpsert fresh rpc error: %+v", rpcErr)
	}
	freshPayload, ok := freshAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected fresh ops upsert result type %T", freshAny)
	}
	freshItem, ok := freshPayload["item"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected fresh ops upsert item type %T", freshPayload["item"])
	}
	if freshItem.Revision != staleRevision+1 {
		t.Fatalf("fresh guarded upsert should advance revision from %d to %d, got %+v", staleRevision, staleRevision+1, freshItem)
	}
	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    firstItem.QueueID,
		Limit:       10,
	}
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)

	blindRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueID:     firstItem.QueueID,
		QueueKey:    firstItem.QueueKey,
		QueueType:   firstItem.QueueType,
		Title:       firstItem.Title,
		Summary:     "Blind queue state should fail",
		AssignedTo:  "operator-blind",
		Urgency:     "LOW",
		SourceKind:  firstItem.SourceKind,
		SourceID:    firstItem.SourceID,
	})
	if err != nil {
		t.Fatalf("marshal blind ops upsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), blindRaw); rpcErr == nil {
		t.Fatal("expected blind workspaceOpsUpsert to reject missing queue base-version after revision advanced")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "current_revision") {
		t.Fatalf("expected invalid params current_revision guidance on blind upsert, got %+v", rpcErr)
	}
	mismatchRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueID:     "opq-bogus",
		QueueKey:    firstItem.QueueKey,
		QueueType:   firstItem.QueueType,
		Title:       firstItem.Title,
		Summary:     "Mismatched queue identity should fail",
		AssignedTo:  "operator-mismatch",
		Urgency:     "LOW",
		SourceKind:  firstItem.SourceKind,
		SourceID:    firstItem.SourceID,
	})
	if err != nil {
		t.Fatalf("marshal mismatched ops upsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), mismatchRaw); rpcErr == nil {
		t.Fatal("expected mismatched workspaceOpsUpsert queue identity to reject")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "queue_id and queue_key") {
		t.Fatalf("expected invalid params queue identity guidance on mismatched upsert, got %+v", rpcErr)
	}

	staleRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          firstItem.QueueID,
		QueueKey:         firstItem.QueueKey,
		QueueType:        firstItem.QueueType,
		Title:            firstItem.Title,
		Summary:          "Stale queue state should fail",
		AssignedTo:       "operator-stale",
		Urgency:          "LOW",
		SourceKind:       firstItem.SourceKind,
		SourceID:         firstItem.SourceID,
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale ops upsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), staleRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to reject outdated queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale upsert, got %+v", rpcErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, firstItem.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale upsert reject: %v", err)
	}
	if current.Summary != freshItem.Summary || current.AssignedTo != freshItem.AssignedTo || current.Urgency != freshItem.Urgency || current.UpdatedAt != freshItem.UpdatedAt {
		t.Fatalf("stale upsert mutated refreshed queue state: got %+v want summary=%q assigned_to=%q urgency=%q updated_at=%q", current, freshItem.Summary, freshItem.AssignedTo, freshItem.Urgency, freshItem.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, queueFilter); len(got) != len(seenQueueEvents) {
		t.Fatalf("stale upsert should not append updated runtime rows, before=%v after=%v", seenQueueEvents, got)
	}
}

func TestWorkspaceOpsUpsertRejectsInterleavingRetryCreateWinnerOnReopenedFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-interleaving-retry-create-winner"
		taskID      = "task-ops-upsert-interleaving-retry-create-winner"
		agentID     = "agent-ops-upsert-interleaving-retry-create-winner"
		repairID    = "tens-repair-ops-upsert-interleaving-retry-create-winner"
		runID       = "run-ops-upsert-interleaving-retry-create-winner"
	)

	firstActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier late fail reopens retry path before stale upsert",
		Summary:     "seed reopened retry carrier before workspace.ops.upsert races a retry winner",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
			"action_id":       firstActionID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	sourceQueueBeforeCreate, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before retry create): %v", err)
	}

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBeforeCreate.QueueID,
		QueueKey:         sourceQueueBeforeCreate.QueueKey,
		QueueType:        sourceQueueBeforeCreate.QueueType,
		Title:            sourceQueueBeforeCreate.Title,
		Summary:          "stale manual note should lose to retry winner",
		Details:          "stale upsert on reopened retry carrier should fail closed",
		AssignedTo:       sourceQueueBeforeCreate.AssignedTo,
		Urgency:          sourceQueueBeforeCreate.Urgency,
		SourceKind:       sourceQueueBeforeCreate.SourceKind,
		SourceID:         sourceQueueBeforeCreate.SourceID,
		TaskID:           sourceQueueBeforeCreate.TaskID,
		AgentID:          sourceQueueBeforeCreate.AgentID,
		CurrentRevision:  sourceQueueBeforeCreate.Revision,
		CurrentUpdatedAt: sourceQueueBeforeCreate.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	var (
		hookErr        error
		winnerActionID string
	)
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		createAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
		if rpcErr != nil {
			hookErr = fmt.Errorf("interleaving retry actionCreate rpc error: %+v", rpcErr)
			return
		}
		createResp, ok := createAny.(map[string]any)
		if !ok {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response type %T", createAny)
			return
		}
		winnerActionID, _ = createResp["action_id"].(string)
		if strings.TrimSpace(winnerActionID) == "" {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response %+v", createResp)
		}
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to fail after interleaving retry winner linked the reopened source queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale upsert after retry create winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving retry create hook: %v", hookErr)
	}
	if winnerActionID == "" || winnerActionID == firstActionID {
		t.Fatalf("unexpected winner action id %q after interleaving retry create", winnerActionID)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated)+1 {
		t.Fatalf("interleaving retry create winner should append exactly one action.created row, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving retry create winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale interleaving upsert): %v", err)
	}
	if currentQueue.Summary == "stale manual note should lose to retry winner" || strings.Contains(currentQueue.Details, "stale upsert on reopened retry carrier should fail closed") || currentQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("stale interleaving upsert mutated reopened retry queue truth = %+v", currentQueue)
	}
	action, err := store.GetHumanAction(ctx, winnerActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(winner): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending {
		t.Fatalf("stale interleaving upsert mutated retry winner action truth = %+v, want assigned_to reviewer-a and pending status", action)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, winnerActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestWorkspaceOpsUpsertRejectsInterleavingFailedResolveWinnerOnLinkedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-interleaving-failed-resolve-winner"
		taskID      = "task-ops-upsert-interleaving-failed-resolve-winner"
		agentID     = "agent-ops-upsert-interleaving-failed-resolve-winner"
		repairID    = "tens-repair-ops-upsert-interleaving-failed-resolve-winner"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving failed resolve): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		QueueKey:         sourceQueueBefore.QueueKey,
		QueueType:        sourceQueueBefore.QueueType,
		Title:            sourceQueueBefore.Title,
		Summary:          "stale manual note should lose to failed resolve winner",
		Details:          "stale upsert on linked rebase carrier should fail after failed resolve",
		AssignedTo:       sourceQueueBefore.AssignedTo,
		Urgency:          sourceQueueBefore.Urgency,
		SourceKind:       sourceQueueBefore.SourceKind,
		SourceID:         sourceQueueBefore.SourceID,
		TaskID:           sourceQueueBefore.TaskID,
		AgentID:          sourceQueueBefore.AgentID,
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	var hookErr error
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "interleaving failed resolve winner should beat stale upsert",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving actionResolve rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to fail after interleaving failed resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale upsert after failed resolve winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving failed resolve hook: %v", hookErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale interleaving upsert): %v", err)
	}
	if currentQueue.Summary == "stale manual note should lose to failed resolve winner" || strings.Contains(currentQueue.Details, "stale upsert on linked rebase carrier should fail after failed resolve") {
		t.Fatalf("stale interleaving upsert smeared stale manual text onto failed-resolve queue truth = %+v", currentQueue)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionID != "" || currentPayload.ActionQueueKey != "" || currentPayload.ActionStatus != "" || currentPayload.ActionAssignedTo != "" {
		t.Fatalf("current source queue should clear active action linkage after failed resolve winner, got %+v", currentPayload)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateClaimed || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("current source queue workflow after stale interleaving upsert = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	}
	if currentPayload.LastFailedActionID != actionID || currentPayload.LastFailedStatus != humanActionStatusFailed {
		t.Fatalf("current source queue failed lineage after stale interleaving upsert = action=%q status=%q, want (%q,%q)", currentPayload.LastFailedActionID, currentPayload.LastFailedStatus, actionID, humanActionStatusFailed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusFailed {
		t.Fatalf("stale interleaving upsert mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale interleaving upsert mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving failed resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("interleaving failed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestWorkspaceOpsUpsertRejectsStaleSnapshotAfterLinkedRebaseActionStart(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-stale-after-rebase-start"
		taskID      = "task-ops-upsert-stale-after-rebase-start"
		agentID     = "agent-ops-upsert-stale-after-rebase-start"
		repairID    = "tens-repair-ops-upsert-stale-after-rebase-start"
	)

	actionID, sourceQueueID := createPendingRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before start): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		QueueKey:         sourceQueueBefore.QueueKey,
		QueueType:        sourceQueueBefore.QueueType,
		Title:            sourceQueueBefore.Title,
		Summary:          "stale manual note should fail after start",
		Details:          "stale upsert should not survive linked rebase start",
		AssignedTo:       sourceQueueBefore.AssignedTo,
		Urgency:          sourceQueueBefore.Urgency,
		SourceKind:       sourceQueueBefore.SourceKind,
		SourceID:         sourceQueueBefore.SourceID,
		TaskID:           sourceQueueBefore.TaskID,
		AgentID:          sourceQueueBefore.AgentID,
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "winner start should beat stale manual note",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to reject outdated queue revision after start")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale upsert after start, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale upsert): %v", err)
	}
	if currentQueue.Summary == "stale manual note should fail after start" || strings.Contains(currentQueue.Details, "stale upsert should not survive linked rebase start") {
		t.Fatalf("stale upsert smeared manual text onto started queue truth = %+v", currentQueue)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("current source queue workflow after stale upsert = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionQueueKey == "" || currentPayload.ActionStatus != humanActionStatusPending || currentPayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("current source queue active action linkage after stale upsert = %+v", currentPayload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending {
		t.Fatalf("stale upsert mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "OPEN" {
		t.Fatalf("stale upsert mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("start winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("stale upsert after start should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestWorkspaceOpsUpsertRejectsInterleavingEscalateWinnerOnStartedLinkedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-interleaving-escalate-winner-started-rebase"
		taskID      = "task-ops-upsert-interleaving-escalate-winner-started-rebase"
		agentID     = "agent-ops-upsert-interleaving-escalate-winner-started-rebase"
		repairID    = "tens-repair-ops-upsert-interleaving-escalate-winner-started-rebase"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving handoff): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		QueueKey:         sourceQueueBefore.QueueKey,
		QueueType:        sourceQueueBefore.QueueType,
		Title:            sourceQueueBefore.Title,
		Summary:          "stale manual note should lose to handoff winner",
		Details:          "stale upsert on started linked rebase carrier should fail after interleaving escalate winner",
		AssignedTo:       sourceQueueBefore.AssignedTo,
		Urgency:          sourceQueueBefore.Urgency,
		SourceKind:       sourceQueueBefore.SourceKind,
		SourceID:         sourceQueueBefore.SourceID,
		TaskID:           sourceQueueBefore.TaskID,
		AgentID:          sourceQueueBefore.AgentID,
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	var (
		hookErr     error
		winnerQueue sqlite.OperatorQueueRecord
	)
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		winnerQueue, hookErr = interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueueID, "lead-b", "reviewer-b", "interleaving handoff winner should beat stale upsert")
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to fail after interleaving escalate winner on started linked rebase")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale upsert after interleaving escalate winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving escalate hook: %v", hookErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale interleaving upsert): %v", err)
	}
	if currentQueue.Summary == "stale manual note should lose to handoff winner" || strings.Contains(currentQueue.Details, "stale upsert on started linked rebase carrier should fail after interleaving escalate winner") {
		t.Fatalf("stale interleaving upsert smeared stale manual text onto handed-off queue truth = %+v", currentQueue)
	}
	if currentQueue.AssignedTo != winnerQueue.AssignedTo || currentQueue.UpdatedAt != winnerQueue.UpdatedAt {
		t.Fatalf("stale interleaving upsert mutated handed-off source queue state: got %+v want assigned_to=%q updated_at=%q", currentQueue, winnerQueue.AssignedTo, winnerQueue.UpdatedAt)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("current source queue payload action_assigned_to after handoff winner = %q, want reviewer-b", currentPayload.ActionAssignedTo)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("current source queue workflow after stale interleaving upsert = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-b" || action.Status != humanActionStatusPending {
		t.Fatalf("stale interleaving upsert mutated linked action truth = %+v, want assigned_to reviewer-b and pending status", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != "reviewer-b" || actionQueue.UpdatedAt == actionQueueBefore.UpdatedAt {
		t.Fatalf("expected exactly one winning action-queue reassignment to reviewer-b, got %+v (before updated_at=%q)", actionQueue, actionQueueBefore.UpdatedAt)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("action queue payload action_assigned_to after interleaving handoff winner = %q, want reviewer-b", actionQueuePayload.ActionAssignedTo)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated)+1 {
		t.Fatalf("interleaving handoff winner should append exactly one source queue escalated row, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("interleaving handoff winner should append exactly one action queue updated row, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsInterleavingUpsertWinnerOnStartedLinkedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-interleaving-upsert-winner-started-rebase"
		taskID      = "task-ops-escalate-interleaving-upsert-winner-started-rebase"
		agentID     = "agent-ops-escalate-interleaving-upsert-winner-started-rebase"
		repairID    = "tens-repair-ops-escalate-interleaving-upsert-winner-started-rebase"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving manual edit): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-stale",
		Reason:           "stale handoff should lose to manual edit winner",
		AssignedTo:       "reviewer-b",
		Urgency:          "LOW",
		DueAt:            "2099-03-01T00:00:00Z",
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsEscalateStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsEscalateStoreOverride = nil
		hookRan = true
		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          sourceQueueBefore.QueueID,
			QueueKey:         sourceQueueBefore.QueueKey,
			QueueType:        sourceQueueBefore.QueueType,
			Title:            sourceQueueBefore.Title,
			Summary:          "winner note should beat stale handoff",
			Details:          "winner workspace.ops.upsert should block stale handoff on started linked rebase carrier",
			AssignedTo:       sourceQueueBefore.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       sourceQueueBefore.SourceKind,
			SourceID:         sourceQueueBefore.SourceID,
			TaskID:           sourceQueueBefore.TaskID,
			AgentID:          sourceQueueBefore.AgentID,
			DueAt:            "2099-02-01T00:00:00Z",
			CurrentRevision:  sourceQueueBefore.Revision,
			CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to fail after interleaving upsert winner on started linked rebase")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale escalate after interleaving upsert winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsEscalateStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving upsert hook: %v", hookErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale interleaving escalate): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("stale interleaving escalate mutated source queue status to %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != sourceQueueBefore.AssignedTo || currentQueue.Urgency != "CRITICAL" || derefString(currentQueue.DueAt) != "2099-02-01T00:00:00Z" {
		t.Fatalf("stale interleaving escalate smeared loser handoff onto winner-owned source queue: got %+v want assigned_to=%q urgency=%q due_at=%q", currentQueue, sourceQueueBefore.AssignedTo, "CRITICAL", "2099-02-01T00:00:00Z")
	}
	if currentQueue.EscalationCount != sourceQueueBefore.EscalationCount {
		t.Fatalf("stale interleaving escalate mutated escalation_count to %d, want %d", currentQueue.EscalationCount, sourceQueueBefore.EscalationCount)
	}
	if currentQueue.Summary != "winner note should beat stale handoff" || !strings.Contains(currentQueue.Details, "winner workspace.ops.upsert should block stale handoff on started linked rebase carrier") {
		t.Fatalf("winner manual edit did not survive stale interleaving handoff loser: %+v", currentQueue)
	}
	if strings.TrimSpace(currentQueue.Resolution) != "" || derefString(currentQueue.ResolvedBy) != "" {
		t.Fatalf("stale interleaving escalate smeared terminal fields onto open source queue: resolution=%q resolved_by=%q", currentQueue.Resolution, derefString(currentQueue.ResolvedBy))
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionStatus != humanActionStatusPending || currentPayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("current source queue active action linkage after stale interleaving escalate = %+v", currentPayload)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("current source queue workflow after stale interleaving escalate = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending {
		t.Fatalf("stale interleaving escalate mutated linked action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.UpdatedAt != actionQueueBefore.UpdatedAt || actionQueue.Status != "OPEN" {
		t.Fatalf("stale interleaving escalate mutated action queue state = %+v", actionQueue)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("action queue payload action_assigned_to after stale interleaving escalate = %q, want reviewer-a", actionQueuePayload.ActionAssignedTo)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving upsert winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated) {
		t.Fatalf("stale interleaving escalate loser should not append source queue escalated rows, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("interleaving upsert winner should not append linked action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceOpsUpsertRejectsStaleSnapshotAfterLinkedRebaseActionPause(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-stale-after-rebase-pause"
		taskID      = "task-ops-upsert-stale-after-rebase-pause"
		agentID     = "agent-ops-upsert-stale-after-rebase-pause"
		repairID    = "tens-repair-ops-upsert-stale-after-rebase-pause"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before pause): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		QueueKey:         sourceQueueBefore.QueueKey,
		QueueType:        sourceQueueBefore.QueueType,
		Title:            sourceQueueBefore.Title,
		Summary:          "stale manual note should fail after pause",
		Details:          "stale upsert should not survive linked rebase pause",
		AssignedTo:       sourceQueueBefore.AssignedTo,
		Urgency:          sourceQueueBefore.Urgency,
		SourceKind:       sourceQueueBefore.SourceKind,
		SourceID:         sourceQueueBefore.SourceID,
		TaskID:           sourceQueueBefore.TaskID,
		AgentID:          sourceQueueBefore.AgentID,
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "winner pause should beat stale manual note",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to reject outdated queue revision after pause")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale upsert after pause, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale upsert): %v", err)
	}
	if currentQueue.Summary == "stale manual note should fail after pause" || strings.Contains(currentQueue.Details, "stale upsert should not survive linked rebase pause") {
		t.Fatalf("stale upsert smeared manual text onto paused queue truth = %+v", currentQueue)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateClaimed || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("current source queue workflow after stale upsert = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionQueueKey == "" || currentPayload.ActionStatus != humanActionStatusPending || currentPayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("current source queue active action linkage after stale upsert = %+v", currentPayload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending {
		t.Fatalf("stale upsert mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "OPEN" {
		t.Fatalf("stale upsert mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("pause winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("stale upsert after pause should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestWorkspaceOpsUpsertRejectsStaleSnapshotAfterRollbackFailureFailedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-stale-after-rollback-failed-resolve"
		taskID      = "task-ops-upsert-stale-after-rollback-failed-resolve"
		agentID     = "agent-ops-upsert-stale-after-rollback-failed-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-upsert-stale-after-rollback-failed-resolve"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "ops-upsert-stale-after-rollback-failed-resolve")
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		QueueKey:         sourceQueue.QueueKey,
		QueueType:        sourceQueue.QueueType,
		Title:            sourceQueue.Title,
		Summary:          "stale rollback-failure note should fail after failed followup resolve",
		Details:          "stale upsert should not survive rollback-failure failed resolve",
		AssignedTo:       sourceQueue.AssignedTo,
		Urgency:          sourceQueue.Urgency,
		SourceKind:       sourceQueue.SourceKind,
		SourceID:         sourceQueue.SourceID,
		TaskID:           sourceQueue.TaskID,
		AgentID:          sourceQueue.AgentID,
		CurrentRevision:  sourceQueue.Revision,
		CurrentUpdatedAt: sourceQueue.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "winner failed followup resolve should beat stale rollback-failure upsert",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to reject outdated rollback-failure queue revision after failed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale rollback-failure upsert, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale rollback-failure upsert): %v", err)
	}
	if currentQueue.Summary == "stale rollback-failure note should fail after failed followup resolve" || strings.Contains(currentQueue.Details, "stale upsert should not survive rollback-failure failed resolve") {
		t.Fatalf("stale rollback-failure upsert smeared stale manual text onto queue truth = %+v", currentQueue)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale upsert reject: %v", err)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" {
		t.Fatalf("active followup link should be cleared after failed resolve winner, got %+v", payload)
	}
	if payload.LastFailedFollowupActionID != actionID || payload.LastFailedFollowupActionStatus != humanActionStatusFailed {
		t.Fatalf("rollback-failure failed lineage after stale upsert reject = %+v, want last_failed_followup_action_id=%q status=%q", payload, actionID, humanActionStatusFailed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusFailed {
		t.Fatalf("stale rollback-failure upsert mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure upsert mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestWorkspaceOpsUpsertRejectsStaleSnapshotAfterRollbackFailureCompletedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-stale-after-rollback-completed-resolve"
		taskID      = "task-ops-upsert-stale-after-rollback-completed-resolve"
		agentID     = "agent-ops-upsert-stale-after-rollback-completed-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-upsert-stale-after-rollback-completed-resolve"
	)

	sourceQueueSeed, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "ops-upsert-stale-after-rollback-completed-resolve")
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueSeed.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before completed resolve): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		QueueKey:         sourceQueueBefore.QueueKey,
		QueueType:        sourceQueueBefore.QueueType,
		Title:            sourceQueueBefore.Title,
		Summary:          "stale note should not reopen completed rollback-failure carrier",
		Details:          "stale upsert should not wipe terminal rollback-failure truth",
		AssignedTo:       sourceQueueBefore.AssignedTo,
		Urgency:          sourceQueueBefore.Urgency,
		SourceKind:       sourceQueueBefore.SourceKind,
		SourceID:         sourceQueueBefore.SourceID,
		TaskID:           sourceQueueBefore.TaskID,
		AgentID:          sourceQueueBefore.AgentID,
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		hookRan = true
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusCompleted,
			Comment:    "winner completed followup resolve should beat stale rollback-failure upsert",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("actionResolve rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to reject outdated rollback-failure queue revision after completed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale rollback-failure upsert after completed resolve, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsUpsertStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving completed resolve hook: %v", hookErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale rollback-failure upsert): %v", err)
	}
	if currentQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure upsert mutated source queue status to %q, want RESOLVED", currentQueue.Status)
	}
	if currentQueue.Resolution != "followup_action_completed:"+actionID {
		t.Fatalf("stale rollback-failure upsert mutated source queue resolution to %q, want followup_action_completed:%s", currentQueue.Resolution, actionID)
	}
	if derefString(currentQueue.ResolvedBy) != "reviewer-a" {
		t.Fatalf("stale rollback-failure upsert mutated source queue resolved_by to %q, want reviewer-a", derefString(currentQueue.ResolvedBy))
	}
	if currentQueue.Summary == "stale note should not reopen completed rollback-failure carrier" || strings.Contains(currentQueue.Details, "stale upsert should not wipe terminal rollback-failure truth") {
		t.Fatalf("stale rollback-failure upsert smeared stale manual text onto completed queue truth = %+v", currentQueue)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale upsert reject: %v", err)
	}
	if payload.FollowupActionID != actionID || payload.FollowupActionQueueKey != "action:"+actionID || payload.FollowupActionStatus != humanActionStatusCompleted {
		t.Fatalf("completed rollback-failure payload after stale upsert reject = %+v, want active completed followup linkage for %q", payload, actionID)
	}
	if payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("stale completed rollback-failure upsert should not mint failed lineage, got %+v", payload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusCompleted || action.ResolvedBy != "reviewer-a" {
		t.Fatalf("stale rollback-failure upsert mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure upsert mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("completed rollback-failure upsert loser should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one source queue resolved row, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("completed rollback-failure upsert loser should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsStaleSnapshotAfterRollbackFailureCompletedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-stale-after-rollback-completed-resolve"
		taskID      = "task-ops-resolve-stale-after-rollback-completed-resolve"
		agentID     = "agent-ops-resolve-stale-after-rollback-completed-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-resolve-stale-after-rollback-completed-resolve"
	)

	sourceQueueBefore, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "ops-resolve-stale-after-rollback-completed-resolve")
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "winner completed followup resolve should beat stale rollback-failure manual close",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	resolveQueueRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale rollback-failure manual close should fail after completed followup resolve",
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveQueueRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to reject outdated rollback-failure queue snapshot after completed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "operator queue item is not open") {
		t.Fatalf("expected invalid params not-open error on stale rollback-failure resolve after completed resolve, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale rollback-failure resolve): %v", err)
	}
	if currentQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure resolve mutated source queue status to %q, want RESOLVED", currentQueue.Status)
	}
	if currentQueue.Resolution != "followup_action_completed:"+actionID {
		t.Fatalf("stale rollback-failure resolve mutated source queue resolution to %q, want followup_action_completed:%s", currentQueue.Resolution, actionID)
	}
	if derefString(currentQueue.ResolvedBy) != "reviewer-a" {
		t.Fatalf("stale rollback-failure resolve mutated source queue resolved_by to %q, want reviewer-a", derefString(currentQueue.ResolvedBy))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale resolve reject: %v", err)
	}
	if payload.FollowupActionID != actionID || payload.FollowupActionQueueKey != "action:"+actionID || payload.FollowupActionStatus != humanActionStatusCompleted {
		t.Fatalf("completed rollback-failure payload after stale resolve reject = %+v, want active completed followup linkage for %q", payload, actionID)
	}
	if payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("stale completed rollback-failure resolve should not mint failed lineage, got %+v", payload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusCompleted || action.ResolvedBy != "reviewer-a" {
		t.Fatalf("stale rollback-failure resolve mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure resolve mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("completed rollback-failure resolve loser should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one source queue resolved row, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
}

func TestWorkspaceOpsEscalateRejectsStaleSnapshotAfterRollbackFailureCompletedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-stale-after-rollback-completed-resolve"
		taskID      = "task-ops-escalate-stale-after-rollback-completed-resolve"
		agentID     = "agent-ops-escalate-stale-after-rollback-completed-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-escalate-stale-after-rollback-completed-resolve"
	)

	sourceQueueBefore, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "ops-escalate-stale-after-rollback-completed-resolve")
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		EscalatedBy:      "lead-stale",
		Reason:           "stale rollback-failure handoff should fail after completed followup resolve",
		AssignedTo:       "reviewer-b",
		Urgency:          "CRITICAL",
		DueAt:            "2099-08-01T00:00:00Z",
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsEscalateStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsEscalateStoreOverride = nil
		hookRan = true
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusCompleted,
			Comment:    "winner completed followup resolve should beat stale rollback-failure handoff",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("actionResolve rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to reject outdated rollback-failure queue snapshot after completed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "operator queue item is not open") {
		t.Fatalf("expected invalid params not-open error on stale rollback-failure escalate after completed resolve, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsEscalateStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving completed resolve hook: %v", hookErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale rollback-failure escalate): %v", err)
	}
	if currentQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure escalate mutated source queue status to %q, want RESOLVED", currentQueue.Status)
	}
	if currentQueue.Resolution != "followup_action_completed:"+actionID {
		t.Fatalf("stale rollback-failure escalate mutated source queue resolution to %q, want followup_action_completed:%s", currentQueue.Resolution, actionID)
	}
	if derefString(currentQueue.ResolvedBy) != "reviewer-a" {
		t.Fatalf("stale rollback-failure escalate mutated source queue resolved_by to %q, want reviewer-a", derefString(currentQueue.ResolvedBy))
	}
	if currentQueue.AssignedTo != sourceQueueBefore.AssignedTo || currentQueue.Urgency != sourceQueueBefore.Urgency || derefString(currentQueue.DueAt) != derefString(sourceQueueBefore.DueAt) || currentQueue.EscalationCount != sourceQueueBefore.EscalationCount {
		t.Fatalf("stale rollback-failure escalate smeared loser handoff onto completed source queue: got %+v want assigned_to=%q urgency=%q due_at=%q escalation_count=%d", currentQueue, sourceQueueBefore.AssignedTo, sourceQueueBefore.Urgency, derefString(sourceQueueBefore.DueAt), sourceQueueBefore.EscalationCount)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale escalate reject: %v", err)
	}
	if payload.FollowupActionID != actionID || payload.FollowupActionQueueKey != "action:"+actionID || payload.FollowupActionStatus != humanActionStatusCompleted {
		t.Fatalf("completed rollback-failure payload after stale escalate reject = %+v, want active completed followup linkage for %q", payload, actionID)
	}
	if payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("stale completed rollback-failure escalate should not mint failed lineage, got %+v", payload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusCompleted || action.ResolvedBy != "reviewer-a" {
		t.Fatalf("stale rollback-failure escalate mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure escalate mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("completed rollback-failure escalate loser should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one source queue resolved row, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated) {
		t.Fatalf("completed rollback-failure escalate loser should not append source queue escalated rows, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("completed rollback-failure escalate loser should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("rollback-failure completed resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsStaleSnapshotAfterRollbackFailureFailedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-stale-after-rollback-failed-resolve"
		taskID      = "task-ops-resolve-stale-after-rollback-failed-resolve"
		agentID     = "agent-ops-resolve-stale-after-rollback-failed-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-resolve-stale-after-rollback-failed-resolve"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "ops-resolve-stale-after-rollback-failed-resolve")
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	sourceQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceQueueResolvedFilter)

	resolveQueueRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale rollback-failure manual close should fail after failed followup resolve",
		CurrentRevision:  sourceQueue.Revision,
		CurrentUpdatedAt: sourceQueue.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "winner failed followup resolve should beat stale rollback-failure manual close",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveQueueRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to reject outdated rollback-failure queue revision after failed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale rollback-failure resolve, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale rollback-failure resolve): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("stale rollback-failure resolve mutated source queue status to %q, want OPEN", currentQueue.Status)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale resolve reject: %v", err)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" {
		t.Fatalf("active followup link should be cleared after failed resolve winner, got %+v", payload)
	}
	if payload.LastFailedFollowupActionID != actionID || payload.LastFailedFollowupActionStatus != humanActionStatusFailed {
		t.Fatalf("rollback-failure failed lineage after stale resolve reject = %+v, want last_failed_followup_action_id=%q status=%q", payload, actionID, humanActionStatusFailed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusFailed {
		t.Fatalf("stale rollback-failure resolve mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure resolve mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceQueueResolvedFilter); len(got) != len(seenSourceQueueResolved) {
		t.Fatalf("stale rollback-failure resolve loser should not append source queue resolved rows, before=%v after=%v", seenSourceQueueResolved, got)
	}
}

func TestWorkspaceOpsEscalateRejectsStaleSnapshotAfterRollbackFailureFailedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-stale-after-rollback-failed-resolve"
		taskID      = "task-ops-escalate-stale-after-rollback-failed-resolve"
		agentID     = "agent-ops-escalate-stale-after-rollback-failed-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-escalate-stale-after-rollback-failed-resolve"
	)

	sourceQueue, actionID := createLinkedRollbackFailureActionForTest(t, ctx, h, store, workspaceID, taskID, agentID, queueKey, "ops-escalate-stale-after-rollback-failed-resolve")
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		EscalatedBy:      "lead-stale",
		Reason:           "stale rollback-failure handoff should fail after failed followup resolve",
		AssignedTo:       "reviewer-b",
		Urgency:          "CRITICAL",
		DueAt:            "2099-08-01T00:00:00Z",
		CurrentRevision:  sourceQueue.Revision,
		CurrentUpdatedAt: sourceQueue.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsEscalateStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsEscalateStoreOverride = nil
		hookRan = true
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "winner failed followup resolve should beat stale rollback-failure handoff",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("actionResolve rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to reject outdated rollback-failure queue revision after failed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale rollback-failure escalate, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsEscalateStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving failed resolve hook: %v", hookErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale rollback-failure escalate): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("stale rollback-failure escalate mutated source queue status to %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != sourceQueue.AssignedTo || currentQueue.Urgency != sourceQueue.Urgency || derefString(currentQueue.DueAt) != derefString(sourceQueue.DueAt) || currentQueue.EscalationCount != sourceQueue.EscalationCount {
		t.Fatalf("stale rollback-failure escalate smeared loser handoff onto source queue: got %+v want assigned_to=%q urgency=%q due_at=%q escalation_count=%d", currentQueue, sourceQueue.AssignedTo, sourceQueue.Urgency, derefString(sourceQueue.DueAt), sourceQueue.EscalationCount)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale escalate reject: %v", err)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" {
		t.Fatalf("active followup link should stay cleared after failed resolve winner, got %+v", payload)
	}
	if payload.LastFailedFollowupActionID != actionID || payload.LastFailedFollowupActionStatus != humanActionStatusFailed {
		t.Fatalf("rollback-failure failed lineage after stale escalate reject = %+v, want last_failed_followup_action_id=%q status=%q", payload, actionID, humanActionStatusFailed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusFailed {
		t.Fatalf("stale rollback-failure escalate mutated action truth = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale rollback-failure escalate mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("stale rollback-failure escalate loser should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("rollback-failure failed resolve winner should append exactly one action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated) {
		t.Fatalf("stale rollback-failure escalate loser should not append source queue escalated rows, before=%v after=%v", seenSourceEscalated, got)
	}
}

func TestWorkspaceOpsUpsertRejectsReservedSessionQueueNamespace(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-reserved-session"
		queueKey    = "session:sess-upsert-reserved:blocker"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Ops Upsert Reserved Session Namespace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    queueKey,
		QueueType:   "BLOCKER",
		Title:       "Spoofed session blocker",
		Summary:     "Public queue upsert must not create reserved session queues.",
		SourceKind:  "session_event",
		SourceID:    "sess-upsert-reserved",
		SessionID:   "sess-upsert-reserved",
		AgentID:     "agent-upsert-reserved",
	})
	if err != nil {
		t.Fatalf("marshal reserved session ops upsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), raw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject reserved session queue namespace")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "queue_key has invalid value") {
		t.Fatalf("expected invalid params reserved session namespace reject, got %+v", rpcErr)
	}

	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected no persisted queue row for reserved session namespace %s", queueKey)
	} else if !strings.Contains(strings.ToLower(err.Error()), "not found") {
		t.Fatalf("get queue after reserved session reject: %v", err)
	}
}

func TestWorkspaceOpsUpsertRejectsUpdatesToExistingReservedSessionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-reserved-session-update"
		agentID     = "agent-ops-upsert-reserved-session-update"
		sessionID   = "sess-ops-upsert-reserved-session-update"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Ops Upsert Reserved Session Update",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Reserved Session Update Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Blocked session queue should remain workflow-managed",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve rollout"}},
	})
	if err != nil {
		t.Fatalf("record blocked session state: %v", err)
	}
	syncResult, err := store.SyncOperatorQueueFromSessionState(ctx, state)
	if err != nil {
		t.Fatalf("sync operator queue from session state: %v", err)
	}
	if len(syncResult.Opened) != 1 {
		t.Fatalf("expected exactly one synced session queue, got %+v", syncResult)
	}
	queue := syncResult.Opened[0].Record
	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)

	raw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		QueueKey:    queue.QueueKey,
		QueueType:   queue.QueueType,
		Title:       queue.Title,
		Summary:     "tampered summary",
		AssignedTo:  "operator-x",
		SourceKind:  "session_event",
		SourceID:    sessionID,
		SessionID:   sessionID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("marshal reserved session update params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), raw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject updates to existing reserved session queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "queue_key has invalid value") {
		t.Fatalf("expected invalid params reserved session queue update reject, got %+v", rpcErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after reserved update reject: %v", err)
	}
	if current.Summary != queue.Summary || current.AssignedTo != queue.AssignedTo || current.SourceKind != queue.SourceKind || current.SourceID != queue.SourceID || current.UpdatedAt != queue.UpdatedAt {
		t.Fatalf("reserved session queue changed after reject: got %+v want %+v", current, queue)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, queueFilter); len(got) != len(seenQueueEvents) {
		t.Fatalf("reserved session update reject should not append runtime rows, before=%v after=%v", seenQueueEvents, got)
	}
}

func TestWorkspaceOpsUpsertRejectsStaleSnapshotAfterResolveRace(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-ops-resolve-vs-upsert-race"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Resolve vs Upsert Race",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	// Step 1: Create a generic (non-workflow) queue.
	firstRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:resolve-vs-upsert-race",
		QueueType:   "FOLLOW_UP",
		Title:       "Resolve vs upsert race queue",
		Summary:     "Initial queue state",
		AssignedTo:  "operator-a",
		Urgency:     "NORMAL",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("marshal first ops upsert params: %v", err)
	}
	firstAny, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), firstRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsUpsert first rpc error: %+v", rpcErr)
	}
	firstPayload, ok := firstAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first ops upsert result type %T", firstAny)
	}
	firstItem, ok := firstPayload["item"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected first ops upsert item type %T", firstPayload["item"])
	}
	staleRevision := firstItem.Revision
	staleUpdatedAt := firstItem.UpdatedAt

	queueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    firstItem.QueueID,
		Limit:       20,
	}

	// Step 2: Reader B resolves the queue (simulating a concurrent writer winning the race).
	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          firstItem.QueueID,
		ResolvedBy:       "operator-b",
		Resolution:       "resolved by concurrent winner",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsResolve rpc error: %+v", rpcErr)
	}
	seenEventsAfterResolve := snapshotRuntimeEventIDs(t, ctx, store, queueFilter)

	// Step 3: Reader A tries to upsert with stale updated_at from before the resolve.
	// This must be rejected.
	staleUpsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          firstItem.QueueID,
		QueueKey:         firstItem.QueueKey,
		QueueType:        firstItem.QueueType,
		Title:            firstItem.Title,
		Summary:          "Stale update from reader A after resolve race",
		AssignedTo:       "operator-stale",
		Urgency:          "CRITICAL",
		SourceKind:       firstItem.SourceKind,
		SourceID:         firstItem.SourceID,
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale upsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), staleUpsertRaw); rpcErr == nil {
		t.Fatal("expected stale upsert after resolve race to be rejected")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale upsert after resolve, got %+v", rpcErr)
	}

	// Step 4: Validate state integrity - queue stays RESOLVED, no phantom mutation.
	current, err := store.GetOperatorQueueItem(ctx, workspaceID, firstItem.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale upsert reject: %v", err)
	}
	if strings.ToUpper(strings.TrimSpace(current.Status)) != "RESOLVED" {
		t.Fatalf("expected queue to stay RESOLVED after stale upsert reject, got status=%s", current.Status)
	}
	if current.AssignedTo == "operator-stale" || current.Urgency == "CRITICAL" {
		t.Fatalf("stale upsert after resolve race mutated queue state: got assigned_to=%s urgency=%s", current.AssignedTo, current.Urgency)
	}

	// Step 5: No phantom runtime events from the rejected stale upsert.
	if got := snapshotRuntimeEventIDs(t, ctx, store, queueFilter); len(got) != len(seenEventsAfterResolve) {
		t.Fatalf("stale upsert after resolve race should not append runtime rows, before=%v after=%v", seenEventsAfterResolve, got)
	}
}

func TestAgentSessionEventCreatesExecutionRun(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-session-execution"
		taskID      = "task-session-execution"
		sessionID   = "sess-session-execution"
	)
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Execution",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Summary:     "claim before task-bound session start",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}

	startRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Start runtime control plane work",
	})
	if err != nil {
		t.Fatalf("marshal session start params: %v", err)
	}
	if _, rpcErr := callAgentSessionStartRaw(t, h, ctx, startRaw); rpcErr != nil {
		t.Fatalf("agentSessionStart rpc error: %+v", rpcErr)
	}
	runFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "session:" + sessionID,
		Limit:       10,
	}
	stepFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		Limit:       20,
	}
	startStatusFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStart,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	}
	startStatusPersisted := mustRuntimeEvent(t, ctx, store, startStatusFilter)
	startRunPersisted := mustRuntimeEvent(t, ctx, store, runFilter)
	startStepPersisted := mustRuntimeEvent(t, ctx, store, stepFilter)
	startDetail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get execution run after start: %v", err)
	}
	startStepRecord := mustExecutionStepFromDetail(t, startDetail, startStepPersisted.EntityID)
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: startStatusPersisted, Type: "agent.session.start"},
		runtimeEventExpectation{Event: startRunPersisted, Type: "workspace.execution.run"},
		runtimeEventExpectation{Event: startStepPersisted, Type: "workspace.execution.step"},
	)
	for i, expectation := range ordered {
		switch expectation.Type {
		case "agent.session.start":
			assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
		case "workspace.execution.run":
			var liveRun sqlite.ExecutionRunRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveRun); err != nil {
				t.Fatalf("decode session-derived execution run payload: %v", err)
			}
			if liveRun.RunID != startDetail.Run.RunID || liveRun.Status != startDetail.Run.Status || liveRun.Summary != startDetail.Run.Summary {
				t.Fatalf("unexpected session-derived execution run live payload %+v / detail %+v", liveRun, startDetail.Run)
			}
		case "workspace.execution.step":
			var liveStep sqlite.ExecutionStepRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveStep); err != nil {
				t.Fatalf("decode session-derived execution step payload: %v", err)
			}
			if liveStep.StepID != startStepRecord.StepID || liveStep.Status != startStepRecord.Status || liveStep.Title != startStepRecord.Title {
				t.Fatalf("unexpected session-derived execution step live payload %+v / detail %+v", liveStep, startStepRecord)
			}
		}
	}
	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, stepFilter)

	blockedRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Waiting for operator sign-off",
		Status:      model.SessionStatusBlocked,
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve deploy gate"}},
	})
	if err != nil {
		t.Fatalf("marshal session blocked params: %v", err)
	}
	if _, rpcErr := callAgentSessionBlockedRaw(t, h, ctx, blockedRaw); rpcErr != nil {
		t.Fatalf("agentSessionBlocked rpc error: %+v", rpcErr)
	}

	detail, err := store.GetExecutionRun(ctx, workspaceID, "session:"+sessionID)
	if err != nil {
		t.Fatalf("get execution run: %v", err)
	}
	if detail.Run.Status != "BLOCKED" {
		t.Fatalf("expected blocked execution run, got %+v", detail.Run)
	}
	if len(detail.Steps) < 2 {
		t.Fatalf("expected execution run steps from session events, got %+v", detail.Steps)
	}
	foundBlocked := false
	for _, step := range detail.Steps {
		if step.Title == "Session blocked" && step.Status == "BLOCKED" {
			foundBlocked = true
			break
		}
	}
	if !foundBlocked {
		t.Fatalf("expected blocked execution step, got %+v", detail.Steps)
	}
	blockedStatusPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventBlocked,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	blockedMemoryPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	})
	blockedClaimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + sessionmemory.StateMemoryID(sessionID),
		Limit:       10,
	})
	blockerQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "BLOCKER",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocker queues after blocked session execution event: %v", err)
	}
	if len(blockerQueues) != 1 {
		t.Fatalf("expected one blocker queue after blocked session execution event, got %+v", blockerQueues)
	}
	blockedQueuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    blockerQueues[0].QueueID,
		Limit:       10,
	})
	blockedRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	blockedStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	blockedStepRecord := mustExecutionStepFromDetail(t, detail, blockedStepPersisted.EntityID)
	ordered, liveEvents = nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: blockedStatusPersisted, Type: "agent.session.blocked"},
		runtimeEventExpectation{Event: blockedMemoryPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: blockedClaimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: blockedQueuePersisted, Type: "workspace.ops.updated"},
		runtimeEventExpectation{Event: blockedRunPersisted, Type: "workspace.execution.run"},
		runtimeEventExpectation{Event: blockedStepPersisted, Type: "workspace.execution.step"},
	)
	for i, expectation := range ordered {
		switch expectation.Type {
		case "agent.session.blocked", "workspace.memory.recorded":
			assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvents[i].PayloadJSON), expectation.Event.PayloadJSON)
		case "workspace.claim.written":
			assertLiveEventMirrorsRuntimeEvent(t, liveEvents[i], expectation.Event, expectation.Type)
		case "workspace.execution.run":
			var liveRun sqlite.ExecutionRunRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveRun); err != nil {
				t.Fatalf("decode blocked session-derived execution run payload: %v", err)
			}
			if liveRun.RunID != detail.Run.RunID || liveRun.Status != detail.Run.Status || liveRun.Summary != detail.Run.Summary {
				t.Fatalf("unexpected blocked session-derived execution run live payload %+v / detail %+v", liveRun, detail.Run)
			}
		case "workspace.execution.step":
			var liveStep sqlite.ExecutionStepRecord
			if err := json.Unmarshal([]byte(liveEvents[i].PayloadJSON), &liveStep); err != nil {
				t.Fatalf("decode blocked session-derived execution step payload: %v", err)
			}
			if liveStep.StepID != blockedStepRecord.StepID || liveStep.Status != blockedStepRecord.Status || liveStep.Title != blockedStepRecord.Title {
				t.Fatalf("unexpected blocked session-derived execution step live payload %+v / detail %+v", liveStep, blockedStepRecord)
			}
		}
	}
	if blockedRunPersisted.IngestSeq <= startRunPersisted.IngestSeq {
		t.Fatalf("expected blocked session execution run event to advance beyond start, start=%+v blocked=%+v", startRunPersisted, blockedRunPersisted)
	}
	if blockedStepPersisted.IngestSeq <= startStepPersisted.IngestSeq {
		t.Fatalf("expected blocked session execution step event to advance beyond start, start=%+v blocked=%+v", startStepPersisted, blockedStepPersisted)
	}
}

func TestWorkspaceExecutionRunWriteListAndGet(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-execution-run"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Run RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	write := func(runID, status, title string) sqlite.ExecutionRunRecord {
		raw, err := json.Marshal(workspaceExecutionRunWriteParams{
			WorkspaceID: workspaceID,
			RunID:       runID,
			AgentID:     "agent-a",
			Title:       title,
			Summary:     "Summarize " + title,
			Status:      status,
		})
		if err != nil {
			t.Fatalf("marshal execution run params: %v", err)
		}
		result, rpcErr := h.workspaceExecutionRunWrite(ctx, raw)
		if rpcErr != nil {
			t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
		}
		payload, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("unexpected execution run write result type %T", result)
		}
		run, ok := payload["run"].(sqlite.ExecutionRunRecord)
		if !ok {
			t.Fatalf("unexpected execution run payload type %T", payload["run"])
		}
		if payload["status"] != "RECORDED" || run.RunID != runID || run.AgentID != "agent-a" {
			t.Fatalf("unexpected execution run write payload %+v", payload)
		}
		live := nextEvent(t, ch)
		persisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EntityType:  "execution_run",
			EntityID:    run.RunID,
			Limit:       1,
		})
		assertLiveEventMirrorsRuntimeEvent(t, live, persisted, "workspace.execution.run")
		return run
	}

	runA := write("run-a", "ACTIVE", "Bridge recovery rollout")
	runB := write("run-b", "BLOCKED", "Bridge stabilization audit")

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		WorkspaceID: workspaceID,
		RunID:       runA.RunID,
		Phase:       "EXECUTE",
		Title:       "Touch run a to refresh sort order",
		Status:      "ACTIVE",
		SortOrder:   1,
	})
	if err != nil {
		t.Fatalf("marshal execution step params: %v", err)
	}
	stepResult, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}
	stepPayload, ok := stepResult.(map[string]any)
	if !ok || stepPayload["status"] != "RECORDED" {
		t.Fatalf("unexpected execution step payload %+v", stepResult)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected execution step record type %T", stepPayload["step"])
	}
	stepLive := nextEvent(t, ch)
	stepPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "execution_step",
		EntityID:    stepRecord.StepID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, stepLive, stepPersisted, "workspace.execution.step")

	listRaw, err := json.Marshal(workspaceExecutionRunListParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal execution run list params: %v", err)
	}
	listResult, rpcErr := h.workspaceExecutionRunList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected execution run list result type %T", listResult)
	}
	listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected execution run list time authority, got %+v", listPayload["time_authority"])
	}
	items, ok := listPayload["items"].([]sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected execution run items type %T", listPayload["items"])
	}
	if len(items) != 2 || items[0].RunID != runA.RunID || items[1].RunID != runB.RunID {
		t.Fatalf("expected most recent execution run first, got %+v", items)
	}

	blockedRaw, err := json.Marshal(workspaceExecutionRunListParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		Status:      "BLOCKED",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal blocked run list params: %v", err)
	}
	blockedResult, rpcErr := h.workspaceExecutionRunList(ctx, blockedRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunList blocked rpc error: %+v", rpcErr)
	}
	blockedPayload, ok := blockedResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected blocked execution run list result type %T", blockedResult)
	}
	blockedItems, ok := blockedPayload["items"].([]sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected blocked execution run items type %T", blockedPayload["items"])
	}
	if len(blockedItems) != 1 || blockedItems[0].RunID != runB.RunID {
		t.Fatalf("expected blocked execution run filter to return run-b, got %+v", blockedItems)
	}

	getRaw, err := json.Marshal(workspaceExecutionRunGetParams{
		WorkspaceID: workspaceID,
		RunID:       runA.RunID,
	})
	if err != nil {
		t.Fatalf("marshal execution run get params: %v", err)
	}
	getResult, rpcErr := h.workspaceExecutionRunGet(ctx, getRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunGet rpc error: %+v", rpcErr)
	}
	getPayload, ok := getResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected execution run get result type %T", getResult)
	}
	detail, ok := getPayload["detail"].(sqlite.ExecutionRunDetail)
	if !ok {
		t.Fatalf("unexpected execution run detail type %T", getPayload["detail"])
	}
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected execution run detail time authority, got %+v", detail.TimeAuthority)
	}
	if detail.Run.RunID != runA.RunID || len(detail.Steps) != 1 {
		t.Fatalf("unexpected execution run detail %+v", detail)
	}
}

func TestWorkspaceExecutionWriteRejectsPromptConvergenceFalseGreen(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-execution-prompt-proof"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Prompt Proof",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	legacyRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		WorkspaceID: workspaceID,
		RunID:       "run-legacy-prompt",
		Title:       "Legacy prompt proof",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"prompt_compiler_status": "legacy_non_converged",
			"c2_1_convergence":       "excluded_until_migrated",
			"deployment_evidence":    "not_accepted_for_daemon_prompt_compiler_convergence",
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, legacyRunRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for legacy prompt proof, got %+v", rpcErr)
	}

	validRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		WorkspaceID: workspaceID,
		RunID:       "run-partial-prompt",
		Title:       "Partial prompt proof carrier",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("marshal valid run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, validRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite carrier rpc error: %+v", rpcErr)
	}

	partialStepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		WorkspaceID: workspaceID,
		RunID:       "run-partial-prompt",
		Phase:       "VERIFY",
		Title:       "Partial prompt proof",
		Status:      "COMPLETED",
		Verification: map[string]any{
			"prompt_capability_evidence": map[string]any{
				"c2_1_convergence": "daemon_prompt_compiler_converged",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal partial step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, partialStepRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for partial prompt proof, got %+v", rpcErr)
	}
}

func TestWorkspaceExecutionRunAndStepMirrorNewPersistedRows(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-runtime-repeat"
		agentID     = "agent-execution-repeat"
		runID       = "run-repeat"
		stepID      = "step-repeat"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Runtime Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Execution Repeat Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	runFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    runID,
		Limit:       10,
	}
	firstRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Title:       "Repeatable execution run",
		Summary:     "First execution run state",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("marshal first run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, firstRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite first rpc error: %+v", rpcErr)
	}
	firstRunLive := nextEvent(t, ch)
	firstRunPersisted := mustRuntimeEvent(t, ctx, store, runFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstRunLive, firstRunPersisted, "workspace.execution.run")

	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, runFilter)
	secondRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Title:       "Repeatable execution run",
		Summary:     "Second execution run state",
		Status:      "FAILED",
		Outcome:     "verification mismatch",
	})
	if err != nil {
		t.Fatalf("marshal second run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, secondRunRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite second rpc error: %+v", rpcErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected run result type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected run payload type %T", runPayload["run"])
	}
	secondRunLive := nextEvent(t, ch)
	secondRunPersisted := mustNewRuntimeEvent(t, ctx, store, runFilter, seenRunEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondRunLive, secondRunPersisted, "workspace.execution.run")
	if secondRunPersisted.EventID == firstRunPersisted.EventID || secondRunPersisted.IngestSeq <= firstRunPersisted.IngestSeq {
		t.Fatalf("expected second run runtime event to advance beyond first, got first=%+v second=%+v", firstRunPersisted, secondRunPersisted)
	}
	var runEnvelope sqlite.ExecutionRunRecord
	if err := json.Unmarshal([]byte(secondRunLive.PayloadJSON), &runEnvelope); err != nil {
		t.Fatalf("decode second run live payload: %v", err)
	}
	if runEnvelope.RunID != runID || runEnvelope.Status != "FAILED" || runEnvelope.Outcome != "verification mismatch" || runRecord.RunID != runID {
		t.Fatalf("unexpected second run live envelope %+v / rpc record %+v", runEnvelope, runRecord)
	}

	stepFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    stepID,
		Limit:       10,
	}
	firstStepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		StepID:      stepID,
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "EXECUTE",
		Title:       "Repeatable execution step",
		Summary:     "First step state",
		Status:      "ACTIVE",
		SortOrder:   1,
	})
	if err != nil {
		t.Fatalf("marshal first step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, firstStepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite first rpc error: %+v", rpcErr)
	}
	firstStepLive := nextEvent(t, ch)
	firstStepPersisted := mustRuntimeEvent(t, ctx, store, stepFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstStepLive, firstStepPersisted, "workspace.execution.step")

	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, stepFilter)
	secondStepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		StepID:      stepID,
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Repeatable execution step",
		Summary:     "Second step state",
		Status:      "COMPLETED",
		SortOrder:   2,
	})
	if err != nil {
		t.Fatalf("marshal second step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, secondStepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite second rpc error: %+v", rpcErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected step result type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected step payload type %T", stepPayload["step"])
	}
	secondStepLive := nextEvent(t, ch)
	secondStepPersisted := mustNewRuntimeEvent(t, ctx, store, stepFilter, seenStepEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondStepLive, secondStepPersisted, "workspace.execution.step")
	if secondStepPersisted.EventID == firstStepPersisted.EventID || secondStepPersisted.IngestSeq <= firstStepPersisted.IngestSeq {
		t.Fatalf("expected second step runtime event to advance beyond first, got first=%+v second=%+v", firstStepPersisted, secondStepPersisted)
	}
	var stepEnvelope sqlite.ExecutionStepRecord
	if err := json.Unmarshal([]byte(secondStepLive.PayloadJSON), &stepEnvelope); err != nil {
		t.Fatalf("decode second step live payload: %v", err)
	}
	if stepEnvelope.StepID != stepID || stepEnvelope.Status != "COMPLETED" || stepEnvelope.Phase != "VERIFY" || stepRecord.StepID != stepID {
		t.Fatalf("unexpected second step live envelope %+v / rpc record %+v", stepEnvelope, stepRecord)
	}
}

func TestWorkspacePolicyPutMirrorsNewPersistedRowForRepeatedMatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-policy-repeat"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Repeated Policy Mirror",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	firstRaw, err := json.Marshal(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-repeat",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
		Effect:      "REQUIRE_APPROVAL",
		Reason:      "first policy version",
		CreatedBy:   "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal first policy params: %v", err)
	}
	firstAny, rpcErr := h.workspacePolicyPut(testAuthContext(workspaceID, "human", "dashboard"), firstRaw)
	if rpcErr != nil {
		t.Fatalf("workspacePolicyPut first rpc error: %+v", rpcErr)
	}
	firstPayload, ok := firstAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first policy result type %T", firstAny)
	}
	firstPolicy, ok := firstPayload["policy"].(sqlite.CapabilityPolicyRecord)
	if !ok {
		t.Fatalf("unexpected first policy payload type %T", firstPayload["policy"])
	}
	if firstPolicy.PolicyID == "" {
		t.Fatalf("expected first policy id, got %+v", firstPolicy)
	}
	policyFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    firstPolicy.PolicyID,
		Limit:       10,
	}
	firstLive := nextEvent(t, ch)
	firstPersisted := mustRuntimeEvent(t, ctx, store, policyFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstLive, firstPersisted, "workspace.policy.put")

	seenPolicyEvents := snapshotRuntimeEventIDs(t, ctx, store, policyFilter)
	secondRaw, err := json.Marshal(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-repeat",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
		Effect:      "DENY",
		Reason:      "second policy version",
		CreatedBy:   "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal second policy params: %v", err)
	}
	secondAny, rpcErr := h.workspacePolicyPut(testAuthContext(workspaceID, "human", "dashboard"), secondRaw)
	if rpcErr != nil {
		t.Fatalf("workspacePolicyPut second rpc error: %+v", rpcErr)
	}
	secondPayload, ok := secondAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second policy result type %T", secondAny)
	}
	secondPolicy, ok := secondPayload["policy"].(sqlite.CapabilityPolicyRecord)
	if !ok {
		t.Fatalf("unexpected second policy payload type %T", secondPayload["policy"])
	}
	if secondPolicy.PolicyID != firstPolicy.PolicyID {
		t.Fatalf("expected repeated policy put to preserve policy_id %q, got %+v", firstPolicy.PolicyID, secondPolicy)
	}
	secondLive := nextEvent(t, ch)
	secondPersisted := mustNewRuntimeEvent(t, ctx, store, policyFilter, seenPolicyEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondPersisted, "workspace.policy.put")
	if secondPersisted.EventID == firstPersisted.EventID || secondPersisted.IngestSeq <= firstPersisted.IngestSeq {
		t.Fatalf("expected second policy runtime event to advance beyond first, got first=%+v second=%+v", firstPersisted, secondPersisted)
	}
	var livePolicy sqlite.CapabilityPolicyRecord
	if err := json.Unmarshal([]byte(secondLive.PayloadJSON), &livePolicy); err != nil {
		t.Fatalf("decode second policy live payload: %v", err)
	}
	if livePolicy.PolicyID != firstPolicy.PolicyID || livePolicy.Effect != "DENY" || secondPolicy.Effect != "DENY" {
		t.Fatalf("unexpected second policy live payload %+v / rpc policy %+v", livePolicy, secondPolicy)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunRollsBackLinkedRebaseFollowupBySourceQueueID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-rebase-source-queue"
		taskID      = "task-execution-run-rebase-source-queue"
		agentID     = "agent-execution-run-rebase-source-queue"
		repairID    = "tens-repair-execution-run-rebase-source-queue"
		runID       = "run-execution-run-rebase-source-queue"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed",
		Summary:     "run-level verification mismatch on rebase follow-up",
		Status:      "FAILED",
		Outcome:     "verify stage failed after overlap trim",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.ResolvedBy != "system:execution_verifier" {
		t.Fatalf("action resolved_by = %q, want system:execution_verifier", action.ResolvedBy)
	}
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	payload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload: %v", err)
	}
	if payload.RollbackReason != "execution_run_verifier_failed" {
		t.Fatalf("rollback_reason = %q, want execution_run_verifier_failed", payload.RollbackReason)
	}
	runDetail, err := store.GetExecutionRun(ctx, workspaceID, runID)
	if err != nil {
		t.Fatalf("GetExecutionRun(%s): %v", runID, err)
	}
	if runDetail.Run.VerificationJSON["source_queue_id"] != sourceQueueID {
		t.Fatalf("unexpected execution run verification %+v", runDetail.Run.VerificationJSON)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunPreservesEarlierRebaseLinkageWhenVerificationOmitted(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-preserve-linkage"
		taskID      = "task-execution-run-preserve-linkage"
		agentID     = "agent-execution-run-preserve-linkage"
		repairID    = "tens-repair-execution-run-preserve-linkage"
		runID       = "run-execution-run-preserve-linkage"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	firstRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run active",
		Summary:     "seed persisted rebase linkage on the execution run",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal first execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, firstRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite first rpc error: %+v", rpcErr)
	}

	secondRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed",
		Summary:     "verification omitted on repeat write should preserve linkage",
		Status:      "FAILED",
	})
	if err != nil {
		t.Fatalf("marshal second execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, secondRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite second rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	runDetail, err := store.GetExecutionRun(ctx, workspaceID, runID)
	if err != nil {
		t.Fatalf("GetExecutionRun(%s): %v", runID, err)
	}
	if runDetail.Run.VerificationJSON["source_queue_id"] != sourceQueueID {
		t.Fatalf("expected repeated run write to preserve verification linkage, got %+v", runDetail.Run.VerificationJSON)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunSuppressesRollbackFailureWhenConcurrentWinnerCompletesAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-concurrent-winner-complete"
		taskID      = "task-execution-run-concurrent-winner-complete"
		agentID     = "agent-execution-run-concurrent-winner-complete"
		repairID    = "tens-repair-execution-run-concurrent-winner-complete"
		runID       = "run-execution-run-concurrent-winner-complete"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusCompleted,
			Comment:    "Concurrent winner completed before run-level verifier rollback applied.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent winner actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner actionResolve rpc error: %+v", rpcErr)
		}
	}

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run arrived after concurrent completion",
		Summary:     "late failed run must not manufacture rollback recovery once a winner already completed the action",
		Status:      "FAILED",
		Outcome:     "run-level verifier arrived after concurrent completion",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner resolve hook: %v", hookErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite response type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite run payload type %T", runPayload["run"])
	}
	if strings.ToUpper(strings.TrimSpace(runRecord.Status)) != "FAILED" {
		t.Fatalf("run status = %q, want FAILED record preserved", runRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusCompleted {
		t.Fatalf("action status = %q, want %q after concurrent winner", action.Status, humanActionStatusCompleted)
	}
	if action.ResolvedBy != "reviewer-a" {
		t.Fatalf("action resolved_by = %q, want reviewer-a", action.ResolvedBy)
	}
	if strings.Contains(action.ResolutionComment, "execution verifier") {
		t.Fatalf("action resolution comment should keep winner resolution, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.Status != "RESOLVED" {
		t.Fatalf("source queue status = %q, want RESOLVED", sourceQueue.Status)
	}
	if sourceQueue.Resolution != "linked_action_completed:"+actionID {
		t.Fatalf("source queue resolution = %q, want linked_action_completed:%s", sourceQueue.Resolution, actionID)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent winner completion")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("expected exactly one winning action.resolved event, before=%v after=%v", seenActionResolved, got)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunSuppressesRollbackFailureWhenConcurrentWinnerFailsAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-concurrent-winner-failed"
		taskID      = "task-execution-run-concurrent-winner-failed"
		agentID     = "agent-execution-run-concurrent-winner-failed"
		repairID    = "tens-repair-execution-run-concurrent-winner-failed"
		runID       = "run-execution-run-concurrent-winner-failed"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "Concurrent winner failed before run-level verifier rollback applied.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent failed winner actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent failed winner actionResolve rpc error: %+v", rpcErr)
		}
	}

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run arrived after concurrent failure",
		Summary:     "late failed run must not manufacture rollback recovery once a winner already failed the action",
		Status:      "FAILED",
		Outcome:     "run-level verifier arrived after concurrent failure",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent failed winner resolve hook: %v", hookErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite response type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite run payload type %T", runPayload["run"])
	}
	if strings.ToUpper(strings.TrimSpace(runRecord.Status)) != "FAILED" {
		t.Fatalf("run status = %q, want FAILED record preserved", runRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q after concurrent failed winner", action.Status, humanActionStatusFailed)
	}
	if action.ResolvedBy != "reviewer-a" {
		t.Fatalf("action resolved_by = %q, want reviewer-a", action.ResolvedBy)
	}
	if strings.Contains(action.ResolutionComment, "execution verifier") {
		t.Fatalf("action resolution comment should keep failed winner resolution, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent failed winner")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("expected exactly one winning action.resolved event, before=%v after=%v", seenActionResolved, got)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunSuppressesRollbackFailureWhenConcurrentWinnerEscalatesAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-concurrent-winner-escalate"
		taskID      = "task-execution-run-concurrent-winner-escalate"
		agentID     = "agent-execution-run-concurrent-winner-escalate"
		repairID    = "tens-repair-execution-run-concurrent-winner-escalate"
		runID       = "run-execution-run-concurrent-winner-escalate"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		_, hookErr = interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueueID, "lead-b", "reviewer-b", "Concurrent winner escalated before run-level verifier rollback applied.")
	}

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run arrived after concurrent handoff",
		Summary:     "late failed run must not manufacture rollback recovery once a handoff winner already reassigned the action",
		Status:      "FAILED",
		Outcome:     "run-level verifier arrived after concurrent handoff",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner escalate hook: %v", hookErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite response type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite run payload type %T", runPayload["run"])
	}
	if strings.ToUpper(strings.TrimSpace(runRecord.Status)) != "FAILED" {
		t.Fatalf("run status = %q, want FAILED record preserved", runRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.AssignedTo != "reviewer-b" {
		t.Fatalf("action truth after concurrent handoff = %+v, want pending reviewer-b", action)
	}
	if strings.Contains(strings.ToLower(action.ResolutionComment), "execution verifier") {
		t.Fatalf("action should not gain verifier resolution comment after concurrent handoff, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.Status != "OPEN" || sourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("source queue after concurrent handoff = %+v, want OPEN reviewer-b", sourceQueue)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload: %v", err)
	}
	if sourcePayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("source queue payload action_assigned_to = %q, want reviewer-b", sourcePayload.ActionAssignedTo)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent handoff winner")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("expected no action.resolved rows after concurrent handoff winner, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated)+1 {
		t.Fatalf("expected exactly one winning operator_queue.escalated row, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("expected exactly one winning linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunReusesCurrentStartedCarrierAfterConcurrentUpsertWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-execution-run-concurrent-winner-upsert"
		taskID        = "task-execution-run-concurrent-winner-upsert"
		agentID       = "agent-execution-run-concurrent-winner-upsert"
		repairID      = "tens-repair-execution-run-concurrent-winner-upsert"
		runID         = "run-execution-run-concurrent-winner-upsert"
		winnerSummary = "winner started-carrier note should survive verifier late fail"
		winnerDetails = "winner workspace.ops.upsert should not force false rollback-failure recovery after verifier late fail"
		winnerDueAt   = "2099-09-01T00:00:00Z"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving manual edit): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil

		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          sourceQueueBefore.QueueID,
			QueueKey:         sourceQueueBefore.QueueKey,
			QueueType:        sourceQueueBefore.QueueType,
			Title:            sourceQueueBefore.Title,
			Summary:          winnerSummary,
			Details:          winnerDetails,
			AssignedTo:       sourceQueueBefore.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       sourceQueueBefore.SourceKind,
			SourceID:         sourceQueueBefore.SourceID,
			TaskID:           sourceQueueBefore.TaskID,
			AgentID:          sourceQueueBefore.AgentID,
			DueAt:            winnerDueAt,
			CurrentRevision:  sourceQueueBefore.Revision,
			CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent winner workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run arrived after concurrent manual edit",
		Summary:     "late failed run should still land on the current started carrier after a concurrent manual edit",
		Status:      "FAILED",
		Outcome:     "run-level verifier arrived after concurrent started-carrier manual edit",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner workspaceOpsUpsert hook: %v", hookErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite response type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite run payload type %T", runPayload["run"])
	}
	if strings.ToUpper(strings.TrimSpace(runRecord.Status)) != "FAILED" {
		t.Fatalf("run status = %q, want FAILED record preserved", runRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q after verifier late fail on current carrier", action.Status, humanActionStatusFailed)
	}
	if action.ResolvedBy != "system:execution_verifier" {
		t.Fatalf("action resolved_by = %q, want system:execution_verifier", action.ResolvedBy)
	}
	if !strings.Contains(strings.ToLower(action.ResolutionComment), "execution verifier late fail") {
		t.Fatalf("action resolution comment should reflect verifier late fail, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.Status != "OPEN" {
		t.Fatalf("source queue status = %q, want OPEN after verifier late fail", sourceQueue.Status)
	}
	if !strings.Contains(sourceQueue.Summary, winnerSummary) {
		t.Fatalf("source queue summary should retain winner-owned manual edit, got %q", sourceQueue.Summary)
	}
	if !strings.Contains(sourceQueue.Details, winnerDetails) {
		t.Fatalf("source queue details should retain winner-owned manual edit, got %q", sourceQueue.Details)
	}
	if !strings.Contains(sourceQueue.Details, "Rollback reason: execution_run_verifier_failed") {
		t.Fatalf("source queue details should record retry-needed failed state, got %q", sourceQueue.Details)
	}
	if sourceQueue.Urgency != "CRITICAL" || derefString(sourceQueue.DueAt) != winnerDueAt {
		t.Fatalf("source queue should preserve winner-owned urgency/due_at, got urgency=%q due_at=%q", sourceQueue.Urgency, derefString(sourceQueue.DueAt))
	}

	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", actionQueueBefore.QueueID, err)
	}
	if actionQueue.Status != "RESOLVED" {
		t.Fatalf("action queue status = %q, want RESOLVED", actionQueue.Status)
	}
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo {
		t.Fatalf("action queue assigned_to = %q, want %q", actionQueue.AssignedTo, actionQueueBefore.AssignedTo)
	}
	if actionQueue.Summary != actionQueueBefore.Summary || actionQueue.Details != actionQueueBefore.Details {
		t.Fatalf("action queue should preserve pre-existing manual content on resolve, before=%+v after=%+v", actionQueueBefore, actionQueue)
	}
	if !strings.Contains(strings.ToLower(actionQueue.Resolution), "execution verifier late fail") {
		t.Fatalf("action queue resolution should reflect verifier late fail, got %q", actionQueue.Resolution)
	}
	if derefString(actionQueue.ResolvedBy) != "system:execution_verifier" {
		t.Fatalf("action queue resolved_by = %q, want system:execution_verifier", derefString(actionQueue.ResolvedBy))
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after verifier late fail rehydrated the current started carrier")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("expected exactly one verifier action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+2 {
		t.Fatalf("expected winner manual edit plus verifier failure source updates, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("expected no source queue resolved rows after verifier failed path, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("expected no action queue updated rows from source-queue manual winner, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("expected exactly one action queue resolved row from verifier late fail, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunSuppressesRollbackFailureWhenConcurrentWinnerPausesAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-concurrent-winner-pause"
		taskID      = "task-execution-run-concurrent-winner-pause"
		agentID     = "agent-execution-run-concurrent-winner-pause"
		repairID    = "tens-repair-execution-run-concurrent-winner-pause"
		runID       = "run-execution-run-concurrent-winner-pause"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		pauseRaw, err := json.Marshal(actionPauseParams{
			ActionID: actionID,
			PausedBy: "reviewer-a",
			Comment:  "Concurrent winner paused before run-level verifier rollback applied.",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent winner actionPause params: %w", err)
			return
		}
		if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner actionPause rpc error: %+v", rpcErr)
		}
	}

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run arrived after concurrent pause",
		Summary:     "late failed run must not manufacture rollback recovery once a pause winner already rewound the action",
		Status:      "FAILED",
		Outcome:     "run-level verifier arrived after concurrent pause",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner pause hook: %v", hookErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite response type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionRunWrite run payload type %T", runPayload["run"])
	}
	if strings.ToUpper(strings.TrimSpace(runRecord.Status)) != "FAILED" {
		t.Fatalf("run status = %q, want FAILED record preserved", runRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.AssignedTo != "reviewer-a" {
		t.Fatalf("action truth after concurrent pause = %+v, want pending reviewer-a", action)
	}
	if strings.Contains(strings.ToLower(action.ResolutionComment), "execution verifier") {
		t.Fatalf("action should not gain verifier resolution comment after concurrent pause, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent pause winner")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("expected no action.resolved rows after concurrent pause winner, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused)+1 {
		t.Fatalf("expected exactly one winning action.paused row, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("expected exactly one winning source queue update row, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunSourceQueueOnlyLinkageFailsClosedAfterRetryStarts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-source-queue-ambiguous-retry"
		taskID      = "task-execution-run-source-queue-ambiguous-retry"
		agentID     = "agent-execution-run-source-queue-ambiguous-retry"
		repairID    = "tens-repair-execution-run-source-queue-ambiguous-retry"
		runID       = "run-execution-run-source-queue-ambiguous-retry"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	initialFailRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed and reopened the retry path",
		Summary:     "initial rollback opens the same source queue for a retry",
		Status:      "FAILED",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal initial failed run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, initialFailRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite initial rollback rpc error: %+v", rpcErr)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, _ := retryCreateResp["action_id"].(string)
	if retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry action id %q after rollback from %q", retryActionID, failedActionID)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Start the retry before an old source-queue-only failed run repeats.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	duplicateFailRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Old source-queue failed run repeated after retry started",
		Summary:     "queue-only run linkage must not roll back the new retry once the old attempt is already in failure lineage",
		Status:      "FAILED",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal duplicate failed run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, duplicateFailRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite duplicate rollback rpc error: %+v", rpcErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected duplicate run response type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected duplicate run payload type %T", runPayload["run"])
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	})
	if recoveryQueue.Status != "OPEN" || recoveryQueue.TaskID != taskID {
		t.Fatalf("unexpected recovery queue %+v", recoveryQueue)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if payload.FailureScope != "execution_run" || payload.FailureTrigger != "execution_run_verifier_failed" {
		t.Fatalf("unexpected recovery payload %+v", payload)
	}
	if payload.RunID != runID || payload.SourceQueueID != sourceQueueID {
		t.Fatalf("unexpected recovery linkage %+v", payload)
	}
	if runRecord.RunID != runID {
		t.Fatalf("unexpected duplicate run record %+v", runRecord)
	}
	if !strings.Contains(payload.FailureMessage, "action_id is required") {
		t.Fatalf("expected ambiguous source-queue linkage guidance in failure message, got %+v", payload)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunRepairOnlyLinkageFailsClosedAfterRetryStarts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-repair-only-ambiguous-retry"
		taskID      = "task-execution-run-repair-only-ambiguous-retry"
		agentID     = "agent-execution-run-repair-only-ambiguous-retry"
		repairID    = "tens-repair-execution-run-repair-only-ambiguous-retry"
		runID       = "run-execution-run-repair-only-ambiguous-retry"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	initialFailRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Repair-only failed run reopened the retry path",
		Summary:     "initial rollback should still work from repair_tension_id-only linkage",
		Status:      "FAILED",
		Verification: map[string]any{
			"repair_tension_id": repairID,
		},
	})
	if err != nil {
		t.Fatalf("marshal initial repair-only failed run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, initialFailRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite initial repair-only rollback rpc error: %+v", rpcErr)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, _ := retryCreateResp["action_id"].(string)
	if retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry action id %q after rollback from %q", retryActionID, failedActionID)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Start the retry before an old repair-only failed run repeats.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	duplicateFailRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Old repair-only failed run repeated after retry started",
		Summary:     "repair-only run linkage must not roll back the new retry once the old attempt is already in failure lineage",
		Status:      "FAILED",
		Verification: map[string]any{
			"repair_tension_id": repairID,
		},
	})
	if err != nil {
		t.Fatalf("marshal duplicate repair-only failed run params: %v", err)
	}
	runAny, rpcErr := h.workspaceExecutionRunWrite(ctx, duplicateFailRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite duplicate repair-only rollback rpc error: %+v", rpcErr)
	}
	runPayload, ok := runAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected duplicate repair-only run response type %T", runAny)
	}
	runRecord, ok := runPayload["run"].(sqlite.ExecutionRunRecord)
	if !ok {
		t.Fatalf("unexpected duplicate repair-only run payload type %T", runPayload["run"])
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "execution_run",
		RepairTensionID: repairID,
	})
	if recoveryQueue.Status != "OPEN" || recoveryQueue.TaskID != taskID {
		t.Fatalf("unexpected repair-only recovery queue %+v", recoveryQueue)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode repair-only recovery queue payload: %v", err)
	}
	if payload.FailureScope != "execution_run" || payload.FailureTrigger != "execution_run_verifier_failed" {
		t.Fatalf("unexpected repair-only recovery payload %+v", payload)
	}
	if payload.RunID != runID || payload.RepairTensionID != repairID {
		t.Fatalf("unexpected repair-only recovery linkage %+v", payload)
	}
	if runRecord.RunID != runID {
		t.Fatalf("unexpected duplicate repair-only run record %+v", runRecord)
	}
	if !strings.Contains(payload.FailureMessage, "action_id is required") {
		t.Fatalf("expected ambiguous repair-only linkage guidance in failure message, got %+v", payload)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunSupportsRetryPromotionAndCompletionWithPersistedVerificationLinkage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-blackbox-retry-complete"
		taskID      = "task-execution-run-blackbox-retry-complete"
		agentID     = "agent-execution-run-blackbox-retry-complete"
		repairID    = "tens-repair-execution-run-blackbox-retry-complete"
		runID       = "run-execution-run-blackbox-retry-complete"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	firstRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run active",
		Summary:     "seed persisted rebase linkage on the execution run before late fail",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal first execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, firstRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite first rpc error: %+v", rpcErr)
	}

	seenRunEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "execution_run.written", Limit: 50})
	currentRollbackCarrierEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       1,
	})
	seenRollbackQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50})
	seenFailedResolveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: failedActionID, Limit: 50})

	secondRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed after persisted linkage",
		Summary:     "repeat failed run should reuse stored linkage and keep retry path healthy",
		Status:      "FAILED",
		Outcome:     "run-level verification mismatch after overlap trim",
	})
	if err != nil {
		t.Fatalf("marshal second execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, secondRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite second rpc error: %+v", rpcErr)
	}

	runEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "execution_run.written", Limit: 50}, seenRunEvents)
	rollbackQueueEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50}, seenRollbackQueueEvents)
	failedResolveEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: failedActionID, Limit: 50}, seenFailedResolveEvents)
	if rollbackQueueEvent.RootCauseID != runEvent.EventID || rollbackQueueEvent.ProvenanceGroupID != runEvent.EventID {
		t.Fatalf("rollback source queue lineage = (%q,%q), want run event %q", rollbackQueueEvent.RootCauseID, rollbackQueueEvent.ProvenanceGroupID, runEvent.EventID)
	}
	assertRuntimeEventParentRefsContain(t, rollbackQueueEvent, currentRollbackCarrierEvent.EventID)
	if failedResolveEvent.RootCauseID != rollbackQueueEvent.RootCauseID || failedResolveEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("failed action resolve lineage %+v does not match rollback queue event %+v", failedResolveEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, failedResolveEvent, rollbackQueueEvent.EventID)

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	failedAction, err := store.GetHumanAction(ctx, failedActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", failedActionID, err)
	}
	if failedAction.ResolvedBy != "system:execution_verifier" {
		t.Fatalf("failed action resolved_by = %q, want system:execution_verifier", failedAction.ResolvedBy)
	}

	failedActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", failedActionID)
	if failedActionQueue.Status != "RESOLVED" {
		t.Fatalf("failed action queue status = %q, want RESOLVED", failedActionQueue.Status)
	}

	reopenedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if reopenedSourceQueue.Status != "OPEN" {
		t.Fatalf("reopened source queue status = %q, want OPEN", reopenedSourceQueue.Status)
	}
	reopenedPayload, err := actionCreateDecodeQueuePayload(reopenedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode reopened source queue payload: %v", err)
	}
	if reopenedPayload.ActionID != "" || reopenedPayload.ActionQueueKey != "" || reopenedPayload.ActionStatus != "" {
		t.Fatalf("reopened source queue should clear active action linkage, got %+v", reopenedPayload)
	}
	if reopenedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("last_failed_action_id = %q, want %q", reopenedPayload.LastFailedActionID, failedActionID)
	}
	if reopenedPayload.LastFailedStatus != humanActionStatusFailed {
		t.Fatalf("last_failed_status = %q, want %q", reopenedPayload.LastFailedStatus, humanActionStatusFailed)
	}
	if reopenedPayload.RollbackReason != "execution_run_verifier_failed" {
		t.Fatalf("rollback_reason = %q, want execution_run_verifier_failed", reopenedPayload.RollbackReason)
	}

	runDetail, err := store.GetExecutionRun(ctx, workspaceID, runID)
	if err != nil {
		t.Fatalf("GetExecutionRun(%s): %v", runID, err)
	}
	if runDetail.Run.VerificationJSON["source_queue_id"] != sourceQueueID {
		t.Fatalf("expected persisted execution run verification linkage, got %+v", runDetail.Run.VerificationJSON)
	}

	var blockedClaimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&blockedClaimStatus); err != nil {
		t.Fatalf("query blocked task claim after failed run rollback: %v", err)
	}
	if blockedClaimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED while retry remains open", blockedClaimStatus)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected canonical failed-run rollback path to avoid rollback-failure recovery queue")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	seenRetryCreateEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.created", Limit: 50})
	seenRetrySourceUpdates := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50})

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}
	if got, _ := retryCreateResp["source_queue_id"].(string); got != sourceQueueID {
		t.Fatalf("retry actionCreate source_queue_id = %q, want %q", got, sourceQueueID)
	}

	retrySourceUpdateEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50}, seenRetrySourceUpdates)
	retryCreatedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.created", EntityID: retryActionID, Limit: 50}, seenRetryCreateEvents)
	if retrySourceUpdateEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retrySourceUpdateEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry source queue lineage %+v does not match rollback queue event %+v", retrySourceUpdateEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retrySourceUpdateEvent, rollbackQueueEvent.EventID)
	assertRuntimeEventParentRefsContain(t, retrySourceUpdateEvent, failedResolveEvent.EventID)
	if retryCreatedEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retryCreatedEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry action created lineage %+v does not match rollback queue event %+v", retryCreatedEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retryCreatedEvent, retrySourceUpdateEvent.EventID)

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	retryLinkedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after retry create: %v", sourceQueueID, err)
	}
	retryLinkedPayload, err := actionCreateDecodeQueuePayload(retryLinkedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode retry-linked source queue payload: %v", err)
	}
	if retryLinkedPayload.ActionID != retryActionID {
		t.Fatalf("active retry action_id = %q, want %q", retryLinkedPayload.ActionID, retryActionID)
	}
	if retryLinkedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("retry payload should preserve failed attempt lineage, got %+v", retryLinkedPayload)
	}
	if retryLinkedPayload.RollbackReason != "execution_run_verifier_failed" {
		t.Fatalf("retry payload rollback_reason = %q, want execution_run_verifier_failed", retryLinkedPayload.RollbackReason)
	}

	failedAction, err = store.GetHumanAction(ctx, failedActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s) after retry create: %v", failedActionID, err)
	}
	if failedAction.Status != humanActionStatusFailed {
		t.Fatalf("failed action status after retry create = %q, want %q", failedAction.Status, humanActionStatusFailed)
	}

	seenRetryStartEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.started", EntityID: retryActionID, Limit: 50})
	seenRetryStartedSourceUpdates := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50})

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Retry the rebase after persisted run-level verifier rollback.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	retryStartedSourceUpdateEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50}, seenRetryStartedSourceUpdates)
	retryStartedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.started", EntityID: retryActionID, Limit: 50}, seenRetryStartEvents)
	if retryStartedSourceUpdateEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retryStartedSourceUpdateEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry started source queue lineage %+v does not match rollback queue event %+v", retryStartedSourceUpdateEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retryStartedSourceUpdateEvent, retrySourceUpdateEvent.EventID)
	assertRuntimeEventParentRefsContain(t, retryStartedSourceUpdateEvent, failedResolveEvent.EventID)
	if retryStartedEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retryStartedEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry action started lineage %+v does not match rollback queue event %+v", retryStartedEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retryStartedEvent, retryStartedSourceUpdateEvent.EventID)

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	seenRetryResolveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: retryActionID, Limit: 50})
	seenSourceResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.resolved", EntityID: sourceQueueID, Limit: 50})

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Retry landed cleanly after persisted run-level verifier late fail.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	retryResolveAny, rpcErr := h.actionResolve(ctx, retryResolveRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}
	retryResolveResp, ok := retryResolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionResolve response type %T", retryResolveAny)
	}
	if got, _ := retryResolveResp["status"].(string); got != humanActionStatusCompleted {
		t.Fatalf("retry actionResolve status = %q, want %q", got, humanActionStatusCompleted)
	}
	if got, _ := retryResolveResp["source_queue_id"].(string); got != sourceQueueID {
		t.Fatalf("retry actionResolve source_queue_id = %q, want %q", got, sourceQueueID)
	}

	sourceResolvedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.resolved", EntityID: sourceQueueID, Limit: 50}, seenSourceResolvedEvents)
	retryResolvedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: retryActionID, Limit: 50}, seenRetryResolveEvents)
	if sourceResolvedEvent.RootCauseID != rollbackQueueEvent.RootCauseID || sourceResolvedEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("completed source queue lineage %+v does not match rollback queue event %+v", sourceResolvedEvent, rollbackQueueEvent)
	}
	if retryResolvedEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retryResolvedEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry action resolved lineage %+v does not match rollback queue event %+v", retryResolvedEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retryResolvedEvent, sourceResolvedEvent.EventID)

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)

	completedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after retry completion: %v", sourceQueueID, err)
	}
	if completedSourceQueue.Status != "RESOLVED" {
		t.Fatalf("completed source queue status = %q, want RESOLVED", completedSourceQueue.Status)
	}
	if completedSourceQueue.Resolution != "linked_action_completed:"+retryActionID {
		t.Fatalf("completed source queue resolution = %q, want linked_action_completed:%s", completedSourceQueue.Resolution, retryActionID)
	}
	completedPayload, err := actionCreateDecodeQueuePayload(completedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode completed source queue payload: %v", err)
	}
	if completedPayload.ActionID != retryActionID {
		t.Fatalf("completed payload action_id = %q, want %q", completedPayload.ActionID, retryActionID)
	}
	if completedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("completed payload should preserve failed attempt lineage, got %+v", completedPayload)
	}
	if completedPayload.RollbackReason != "execution_run_verifier_failed" {
		t.Fatalf("completed payload rollback_reason = %q, want execution_run_verifier_failed", completedPayload.RollbackReason)
	}

	retryActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", retryActionID)
	if retryActionQueue.Status != "RESOLVED" {
		t.Fatalf("retry action queue status = %q, want RESOLVED", retryActionQueue.Status)
	}

	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&blockedClaimStatus); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected task claim blocker snapshot to be cleared after successful run-level retry, got status=%q err=%v", blockedClaimStatus, err)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunSupportsEscalatedRetryPromotionToNewHolder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-blackbox-retry-handoff"
		taskID      = "task-execution-run-blackbox-retry-handoff"
		agentID     = "agent-execution-run-blackbox-retry-handoff"
		repairID    = "tens-repair-execution-run-blackbox-retry-handoff"
		runID       = "run-execution-run-blackbox-retry-handoff"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	firstRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run active",
		Summary:     "seed persisted rebase linkage before handoff retry test",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal first execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, firstRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite first rpc error: %+v", rpcErr)
	}

	secondRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed after persisted linkage",
		Summary:     "the reopened retry queue should be handoff-able before retry promotion",
		Status:      "FAILED",
		Outcome:     "run-level verification mismatch after overlap trim",
	})
	if err != nil {
		t.Fatalf("marshal second execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, secondRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite second rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueueID, "")
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-b",
		Reason:           "route failed-run retry to a different reviewer",
		AssignedTo:       "reviewer-b",
		Urgency:          "CRITICAL",
		DueAt:            "2099-07-01T00:00:00Z",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}

	reopenedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after handoff: %v", sourceQueueID, err)
	}
	if reopenedSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("reopened source queue assigned_to = %q, want reviewer-b", reopenedSourceQueue.AssignedTo)
	}
	reopenedPayload, err := actionCreateDecodeQueuePayload(reopenedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode reopened source queue payload after handoff: %v", err)
	}
	if reopenedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("reopened payload action_assigned_to = %q, want reviewer-b", reopenedPayload.ActionAssignedTo)
	}
	if reopenedPayload.LastFailedActionID != failedActionID || reopenedPayload.RollbackReason != "execution_run_verifier_failed" {
		t.Fatalf("reopened payload should preserve failed lineage through run-level handoff, got %+v", reopenedPayload)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}

	retryAction, err := store.GetHumanAction(ctx, retryActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", retryActionID, err)
	}
	if retryAction.AssignedTo != "reviewer-b" {
		t.Fatalf("retry action assigned_to = %q, want reviewer-b", retryAction.AssignedTo)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	staleStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "old holder should not start handed-off failed-run retry",
	})
	if err != nil {
		t.Fatalf("marshal stale retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, staleStartRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected holder mismatch on retry actionStart after failed-run handoff, got %+v", rpcErr)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-b",
		Comment:   "new holder starts the handed-off failed-run retry",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	staleResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "old holder should not resolve handed-off failed-run retry",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal stale retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, staleResolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected holder mismatch on retry actionResolve after failed-run handoff, got %+v", rpcErr)
	}

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "new holder completed the handed-off failed-run retry",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, retryResolveRaw); rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)

	completedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after handed-off retry completion: %v", sourceQueueID, err)
	}
	if completedSourceQueue.Status != "RESOLVED" || completedSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("completed source queue after handed-off retry = %+v", completedSourceQueue)
	}
	completedPayload, err := actionCreateDecodeQueuePayload(completedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode completed source queue payload: %v", err)
	}
	if completedPayload.ActionID != retryActionID || completedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("completed payload should mirror winning handed-off retry, got %+v", completedPayload)
	}
	if completedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("completed payload should preserve original failed-run lineage, got %+v", completedPayload)
	}
	if completedPayload.RollbackReason != "execution_run_verifier_failed" {
		t.Fatalf("completed payload rollback_reason = %q, want execution_run_verifier_failed", completedPayload.RollbackReason)
	}
}

func TestWorkspaceExecutionRunWriteFailedRunRejectsSecondRetryPromotionAfterRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-blackbox-retry-single-authority"
		taskID      = "task-execution-run-blackbox-retry-single-authority"
		agentID     = "agent-execution-run-blackbox-retry-single-authority"
		repairID    = "tens-repair-execution-run-blackbox-retry-single-authority"
		runID       = "run-execution-run-blackbox-retry-single-authority"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	firstRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run active",
		Summary:     "seed persisted linkage before failed-run single-authority test",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal first execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, firstRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite first rpc error: %+v", rpcErr)
	}

	secondRunRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed after persisted linkage",
		Summary:     "only one retry action should win after failed-run rollback reopens the same source queue",
		Status:      "FAILED",
		Outcome:     "run-level verification mismatch after overlap trim",
	})
	if err != nil {
		t.Fatalf("marshal second execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, secondRunRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite second rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	assertSecondRetryPromotionFailsClosed(t, ctx, store, h, workspaceID, failedActionID, sourceQueueID)
}

func TestWorkspaceExecutionRunWriteFailedRunDoesNotRollbackPausedLinkedRebaseFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-paused-no-rollback"
		taskID      = "task-execution-run-paused-no-rollback"
		agentID     = "agent-execution-run-paused-no-rollback"
		repairID    = "tens-repair-execution-run-paused-no-rollback"
		runID       = "run-execution-run-paused-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Pause before failed run lands.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed after pause",
		Summary:     "paused rebase should not auto-rollback again on failed run",
		Status:      "FAILED",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestWorkspaceExecutionRunWriteFailedRunDoesNotRollbackResolvedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-resolved-no-rollback"
		taskID      = "task-execution-run-resolved-no-rollback"
		agentID     = "agent-execution-run-resolved-no-rollback"
		repairID    = "tens-repair-execution-run-resolved-no-rollback"
		runID       = "run-execution-run-resolved-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Resolved before stale failed run lands.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed after completion",
		Summary:     "resolved rebase should not be rolled back by a stale failed run",
		Status:      "FAILED",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
}

func TestWorkspaceExecutionRunWriteFailedRunDoesNotRollbackUnstartedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-unstarted-no-rollback"
		taskID      = "task-execution-run-unstarted-no-rollback"
		agentID     = "agent-execution-run-unstarted-no-rollback"
		repairID    = "tens-repair-execution-run-unstarted-no-rollback"
		runID       = "run-execution-run-unstarted-no-rollback"
	)

	actionID, sourceQueueID := createPendingRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed before operator start",
		Summary:     "unstarted rebase should not be rolled back by a stale failed run",
		Status:      "FAILED",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestWorkspaceExecutionRunWriteFailedRunRollsBackLinkedRebaseFollowupByRepairTensionID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-rebase-repair-link"
		taskID      = "task-execution-run-rebase-repair-link"
		agentID     = "agent-execution-run-rebase-repair-link"
		repairID    = "tens-repair-execution-run-rebase-repair-link"
		runID       = "run-execution-run-rebase-repair-link"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Repair verifier run failed",
		Summary:     "run-level verifier produced a bounded late fail",
		Status:      "FAILED",
		Verification: map[string]any{
			"repair_tension_id": repairID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestWorkspaceExecutionRunWriteFailedRunWithoutRebaseLinkageDoesNotRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-no-rebase-link"
		taskID      = "task-execution-run-no-rebase-link"
		agentID     = "agent-execution-run-no-rebase-link"
		repairID    = "tens-repair-execution-run-no-rebase-link"
		runID       = "run-execution-run-no-rebase-link"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:        runID,
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		AgentID:      agentID,
		Title:        "Verifier run failed without linkage",
		Summary:      "run-level verifier failed without explicit rebase linkage",
		Status:       "FAILED",
		Verification: map[string]any{"note": "no linked rebase metadata"},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestWorkspaceExecutionRunWriteFailedRunWithMismatchedSourceQueueKeyDoesNotRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-queue-key-mismatch-no-rollback"
		taskID      = "task-execution-run-queue-key-mismatch-no-rollback"
		agentID     = "agent-execution-run-queue-key-mismatch-no-rollback"
		repairID    = "tens-repair-execution-run-queue-key-mismatch-no-rollback"
		runID       = "run-execution-run-queue-key-mismatch-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed with wrong queue key",
		Summary:     "conflicting source_queue_id and source_queue_key should fail closed",
		Status:      "FAILED",
		Verification: map[string]any{
			"source_queue_id":  sourceQueueID,
			"source_queue_key": "tension_rebase_followup:some-other-repair",
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestWorkspaceExecutionRunWriteFailedRunWithMismatchedRepairLinkDoesNotRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-mismatch-no-rollback"
		taskID      = "task-execution-run-mismatch-no-rollback"
		agentID     = "agent-execution-run-mismatch-no-rollback"
		repairID    = "tens-repair-execution-run-mismatch-no-rollback"
		runID       = "run-execution-run-mismatch-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed with wrong repair link",
		Summary:     "mismatched repair_tension_id should fail closed on run writes",
		Status:      "FAILED",
		Verification: map[string]any{
			"action_id":         actionID,
			"repair_tension_id": "tens-repair-some-other-branch",
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestWorkspaceExecutionRunWriteQueuesRollbackFailureRecoveryWhenRollbackFails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-run-rollback-recovery-queue"
		taskID      = "task-execution-run-rollback-recovery-queue"
		agentID     = "agent-execution-run-rollback-recovery-queue"
		repairID    = "tens-repair-execution-run-rollback-recovery-queue"
		runID       = "run-execution-run-rollback-recovery-queue"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	corruptActionQueueSourceLinkForControlPlaneTest(t, ctx, store, workspaceID, actionID, sourceQueueID)

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Verifier run failed with broken rollback path",
		Summary:     "queue recovery when canonical run rollback path errors",
		Status:      "FAILED",
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
			"action_id":       actionID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execution run params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	})
	if recoveryQueue.Status != "OPEN" || recoveryQueue.TaskID != taskID {
		t.Fatalf("unexpected recovery queue %+v", recoveryQueue)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if payload.FailureScope != "execution_run" || payload.FailureTrigger != "execution_run_verifier_failed" {
		t.Fatalf("unexpected recovery payload %+v", payload)
	}
	if payload.RunID != runID || payload.ActionID != actionID || payload.SourceQueueID != sourceQueueID {
		t.Fatalf("unexpected recovery linkage %+v", payload)
	}
	if !strings.Contains(payload.FailureMessage, "operator queue item not found") {
		t.Fatalf("expected recovery payload to preserve rollback failure detail, got %+v", payload)
	}
}

func TestWorkspaceExecutionStepWriteQueuesRollbackFailureRecoveryWhenRollbackFails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-rollback-recovery-queue"
		taskID      = "task-execution-step-rollback-recovery-queue"
		agentID     = "agent-execution-step-rollback-recovery-queue"
		repairID    = "tens-repair-execution-step-rollback-recovery-queue"
		runID       = "run-execution-step-rollback-recovery-queue"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	corruptActionQueueSourceLinkForControlPlaneTest(t, ctx, store, workspaceID, actionID, sourceQueueID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier step failed with broken rollback path",
		Summary:     "queue recovery when canonical step rollback path errors",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
			"action_id":       actionID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected step payload type %T", stepPayload["step"])
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	})
	if recoveryQueue.Status != "OPEN" || recoveryQueue.TaskID != taskID {
		t.Fatalf("unexpected recovery queue %+v", recoveryQueue)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if payload.FailureScope != "execution_step" || payload.FailureTrigger != "execution_verifier_failed" {
		t.Fatalf("unexpected recovery payload %+v", payload)
	}
	if payload.RunID != runID || payload.StepID != stepRecord.StepID || payload.TaskID != taskID {
		t.Fatalf("unexpected recovery linkage %+v", payload)
	}
	if payload.ActionID != actionID || payload.SourceQueueID != sourceQueueID {
		t.Fatalf("unexpected recovery action linkage %+v", payload)
	}
}

func TestQueueRebaseRollbackFailureSkipsIdenticalOpenQueueReplay(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-replay-noop"
		taskID        = "task-rollback-failure-replay-noop"
		agentID       = "agent-rollback-failure-replay-noop"
		sourceQueueID = "opq-rollback-failure-replay-noop"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	input := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_step",
		FailureTrigger: "execution_verifier_failed",
		FailureMessage: "rollback path stayed broken",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		StepID:         "step-rollback-failure-replay-noop",
	}

	queue := createRebaseRollbackFailureQueueForTest(t, ctx, store, input)
	updatedAt := queue.UpdatedAt
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	})

	h.queueRebaseRollbackFailure(ctx, input)

	unchanged := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if unchanged.UpdatedAt != updatedAt {
		t.Fatalf("rollback failure queue updated_at changed on identical replay: before=%q after=%q", updatedAt, unchanged.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents) {
		t.Fatalf("rollback failure queue replay emitted extra runtime events: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureTreatsMissingRowIdenticalRaceAsNoop(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-missing-identical-race"
		taskID        = "task-rollback-failure-missing-identical-race"
		agentID       = "agent-rollback-failure-missing-identical-race"
		sourceQueueID = "opq-rollback-failure-missing-identical-race"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	input := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_step",
		FailureTrigger: "execution_verifier_failed",
		FailureMessage: "first create identical race should collapse",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		StepID:         "step-rollback-failure-missing-identical-race",
	}

	var created sqlite.OperatorQueueRecord
	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		created = createRebaseRollbackFailureQueueForTest(t, ctx, store, input)
	}

	h.queueRebaseRollbackFailure(ctx, input)

	current := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if created.QueueID == "" {
		t.Fatalf("expected create hook to capture the first queue creation")
	}
	if current.UpdatedAt != created.UpdatedAt {
		t.Fatalf("identical missing-row race changed queue updated_at: got %q want %q", current.UpdatedAt, created.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    current.QueueID,
		Limit:       20,
	}); len(got) != 1 {
		t.Fatalf("identical missing-row race should emit exactly one runtime event, got %d", len(got))
	}
}

func TestQueueRebaseRollbackFailureRejectsMissingRowNonIdenticalRaceWithoutOverwrite(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-missing-nonidentical-race"
		taskID        = "task-rollback-failure-missing-nonidentical-race"
		agentID       = "agent-rollback-failure-missing-nonidentical-race"
		sourceQueueID = "opq-rollback-failure-missing-nonidentical-race"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	stale := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_step",
		FailureTrigger: "execution_verifier_failed",
		FailureMessage: "stale first-create replay should lose",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		StepID:         "step-rollback-failure-missing-nonidentical-race",
	}
	fresh := stale
	fresh.FailureMessage = "fresh first-create wins"

	var created sqlite.OperatorQueueRecord
	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		created = createRebaseRollbackFailureQueueForTest(t, ctx, store, fresh)
	}

	h.queueRebaseRollbackFailure(ctx, stale)

	current := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  stale.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if created.QueueID == "" {
		t.Fatalf("expected create hook to capture the competing first queue creation")
	}
	if current.UpdatedAt != created.UpdatedAt {
		t.Fatalf("non-identical missing-row race changed queue updated_at: got %q want %q", current.UpdatedAt, created.UpdatedAt)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode current rollback failure payload: %v", err)
	}
	if strings.TrimSpace(payload.FailureMessage) != fresh.FailureMessage {
		t.Fatalf("failure_message = %q, want %q after missing-row race rejection", payload.FailureMessage, fresh.FailureMessage)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    current.QueueID,
		Limit:       20,
	}); len(got) != 1 {
		t.Fatalf("non-identical missing-row race should emit exactly one runtime event, got %d", len(got))
	}
}

func TestQueueRebaseRollbackFailureSkipsIdenticalReplayWithoutDroppingFollowupState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-followup-noop"
		taskID        = "task-rollback-failure-followup-noop"
		agentID       = "agent-rollback-failure-followup-noop"
		sourceQueueID = "opq-rollback-failure-followup-noop"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	input := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback path stayed broken during retry",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-followup-noop",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-rollback-followup-noop-root",
			ProvenanceGroupID: "evt-rollback-followup-noop-prov",
		},
	}

	sourceQueue := createRebaseRollbackFailureQueueForTest(t, ctx, store, input)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	linkedQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	updatedAt := linkedQueue.UpdatedAt
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    linkedQueue.QueueID,
		Limit:       20,
	})

	input.Lineage = rebaseRuntimeLineage{}
	h.queueRebaseRollbackFailure(ctx, input)

	unchanged := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if unchanged.UpdatedAt != updatedAt {
		t.Fatalf("linked rollback failure queue updated_at changed on identical replay: before=%q after=%q", updatedAt, unchanged.UpdatedAt)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(unchanged.PayloadJSON)
	if err != nil {
		t.Fatalf("decode unchanged rollback failure payload: %v", err)
	}
	if strings.TrimSpace(payload.FollowupActionID) != actionID {
		t.Fatalf("followup_action_id = %q, want %q", payload.FollowupActionID, actionID)
	}
	if strings.TrimSpace(payload.FollowupActionStatus) != humanActionStatusPending {
		t.Fatalf("followup_action_status = %q, want %q", payload.FollowupActionStatus, humanActionStatusPending)
	}
	if payload.RootCauseID != "evt-rollback-followup-noop-root" || payload.ProvenanceGroupID != "evt-rollback-followup-noop-prov" {
		t.Fatalf("rollback lineage should survive identical replay without new lineage, got %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    linkedQueue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents) {
		t.Fatalf("linked rollback failure replay emitted extra runtime events: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureCreateUsesCurrentLinkedSourceContextLineageInsteadOfCallerInput(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-current-lineage"
		taskID      = "task-rollback-failure-create-current-lineage"
		agentID     = "agent-rollback-failure-create-current-lineage"
		repairID    = "tens-repair-rollback-failure-create-current-lineage"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("get source queue: %v", err)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload: %v", err)
	}
	currentLineage := rebaseFollowupPayloadLineage(sourcePayload)
	if currentLineage.RootCauseID == "" && currentLineage.ProvenanceGroupID == "" && len(currentLineage.ParentRefsJSON) == 0 {
		var ok bool
		currentLineage, ok = h.latestHumanActionLineage(ctx, workspaceID, sourcePayload.ActionID)
		if !ok {
			t.Fatalf("expected started source context to provide canonical lineage")
		}
	}
	if currentLineage.RootCauseID == "" || currentLineage.ProvenanceGroupID == "" || len(currentLineage.ParentRefsJSON) == 0 {
		t.Fatalf("expected started source context to carry authoritative lineage, got %+v", currentLineage)
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback carrier create path should canonicalize lineage from current source queue",
		TaskID:         taskID,
		AgentID:        agentID,
		ActionID:       actionID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-create-current-lineage",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-create-root",
			ProvenanceGroupID: "evt-stale-create-prov",
			ParentRefsJSON:    []string{"evt-stale-create-parent"},
		},
	})

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	})
	recoveryPayload, err := actionCreateDecodeRollbackFailurePayload(recoveryQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	gotLineage := rebaseRollbackFailurePayloadLineage(recoveryPayload)
	if !reflect.DeepEqual(gotLineage, currentLineage) {
		t.Fatalf("rollback failure create path should use current linked source context lineage, got %+v want %+v", gotLineage, currentLineage)
	}
}

func TestQueueRebaseRollbackFailureCreateRecomputesCurrentSourceLineageInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-boundary-lineage"
		taskID      = "task-rollback-failure-create-boundary-lineage"
		agentID     = "agent-rollback-failure-create-boundary-lineage"
		repairID    = "tens-repair-rollback-failure-create-boundary-lineage"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("get source queue before boundary test: %v", err)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue before boundary test: %v", err)
	}
	currentSourceEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       1,
	})
	previousLineage := rebaseFollowupPayloadLineage(sourcePayload)
	if previousLineage.RootCauseID == "" && previousLineage.ProvenanceGroupID == "" && len(previousLineage.ParentRefsJSON) == 0 {
		var ok bool
		previousLineage, ok = h.latestHumanActionLineage(ctx, workspaceID, sourcePayload.ActionID)
		if !ok {
			t.Fatalf("expected pre-hook source context to provide lineage")
		}
	}

	freshLineage := rebaseRuntimeLineage{
		RootCauseID:       "evt-create-boundary-fresh-root",
		ProvenanceGroupID: "evt-create-boundary-fresh-prov",
		ParentRefsJSON:    []string{currentSourceEvent.EventID},
	}
	if reflect.DeepEqual(previousLineage, freshLineage) {
		t.Fatalf("expected boundary-test lineage to differ before hook, got %+v", previousLineage)
	}

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
		if err != nil {
			t.Fatalf("get source queue in create hook: %v", err)
		}
		currentPayload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
		if err != nil {
			t.Fatalf("decode source queue payload in create hook: %v", err)
		}
		currentPayload.RootCauseID = freshLineage.RootCauseID
		currentPayload.ProvenanceGroupID = freshLineage.ProvenanceGroupID
		currentPayload.ParentRefsJSON = append([]string(nil), freshLineage.ParentRefsJSON...)
		currentPayload.Normalize()
		currentPayloadJSON, err := json.Marshal(currentPayload)
		if err != nil {
			t.Fatalf("marshal current source queue payload in create hook: %v", err)
		}
		if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			QueueID:                 currentSourceQueue.QueueID,
			WorkspaceID:             currentSourceQueue.WorkspaceID,
			QueueKey:                currentSourceQueue.QueueKey,
			QueueType:               currentSourceQueue.QueueType,
			Title:                   currentSourceQueue.Title,
			Summary:                 currentSourceQueue.Summary,
			Details:                 currentSourceQueue.Details,
			PayloadJSON:             string(currentPayloadJSON),
			AssignedTo:              currentSourceQueue.AssignedTo,
			Urgency:                 currentSourceQueue.Urgency,
			SourceKind:              currentSourceQueue.SourceKind,
			SourceID:                currentSourceQueue.SourceID,
			TaskID:                  currentSourceQueue.TaskID,
			SessionID:               currentSourceQueue.SessionID,
			AgentID:                 currentSourceQueue.AgentID,
			KeepSessionActive:       currentSourceQueue.KeepSessionActive,
			DueAt:                   derefString(currentSourceQueue.DueAt),
			RequireCurrentUpdatedAt: currentSourceQueue.UpdatedAt,
		}); err != nil {
			t.Fatalf("refresh source queue lineage inside create hook: %v", err)
		}
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback carrier create path should derive lineage inside create boundary",
		TaskID:         taskID,
		AgentID:        agentID,
		ActionID:       actionID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-create-boundary-lineage",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-boundary-root",
			ProvenanceGroupID: "evt-stale-boundary-prov",
			ParentRefsJSON:    []string{"evt-stale-boundary-parent"},
		},
	})

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	})
	recoveryPayload, err := actionCreateDecodeRollbackFailurePayload(recoveryQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode boundary recovery queue payload: %v", err)
	}
	gotLineage := rebaseRollbackFailurePayloadLineage(recoveryPayload)
	if !reflect.DeepEqual(gotLineage, freshLineage) {
		t.Fatalf("rollback failure create path should derive fresh source lineage inside create boundary, got %+v want %+v", gotLineage, freshLineage)
	}
	if reflect.DeepEqual(gotLineage, previousLineage) {
		t.Fatalf("rollback failure create path reused stale pre-hook lineage %+v", gotLineage)
	}
}

func TestQueueRebaseRollbackFailureCreateRejectsLostExplicitSourceCarrierInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-explicit-source-lost-carrier"
		taskID      = "task-rollback-failure-create-explicit-source-lost-carrier"
		agentID     = "agent-rollback-failure-create-explicit-source-lost-carrier"
		repairID    = "tens-repair-rollback-failure-create-explicit-source-lost-carrier"
		runID       = "run-rollback-failure-create-explicit-source-lost-carrier"
	)

	_, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		corruptRebaseSourceQueuePayloadForControlPlaneTest(t, ctx, store, workspaceID, sourceQueueID)
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "explicit source-linked rollback carrier proof should fail closed inside create boundary",
		SourceQueueID:  sourceQueueID,
		RunID:          runID,
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-explicit-source-lost-carrier-root",
			ProvenanceGroupID: "evt-stale-explicit-source-lost-carrier-prov",
			ParentRefsJSON:    []string{"evt-stale-explicit-source-lost-carrier-parent"},
		},
	})

	queueKey := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope:  "execution_run",
		SourceQueueID: sourceQueueID,
	})
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected lost explicit source carrier inside create boundary to fail closed without recovery queue")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("GetOperatorQueueItem(lost explicit-source rollback queue): %v", err)
	}
}

func TestQueueRebaseRollbackFailureCreateCanonicalizesCurrentActionLinkedSourceLineageInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-action-lineage"
		taskID      = "task-rollback-failure-create-action-lineage"
		agentID     = "agent-rollback-failure-create-action-lineage"
		repairID    = "tens-repair-rollback-failure-create-action-lineage"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("get source queue before action-linked boundary test: %v", err)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue before action-linked boundary test: %v", err)
	}
	previousLineage := rebaseFollowupPayloadLineage(sourcePayload)
	if previousLineage.RootCauseID == "" && previousLineage.ProvenanceGroupID == "" && len(previousLineage.ParentRefsJSON) == 0 {
		var ok bool
		previousLineage, ok = h.latestHumanActionLineage(ctx, workspaceID, sourcePayload.ActionID)
		if !ok {
			t.Fatalf("expected action-linked source context to provide pre-hook lineage")
		}
	}

	currentSourceEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       1,
	})
	freshLineage := rebaseRuntimeLineage{
		RootCauseID:       "evt-action-create-boundary-fresh-root",
		ProvenanceGroupID: "evt-action-create-boundary-fresh-prov",
		ParentRefsJSON:    []string{currentSourceEvent.EventID},
	}
	if reflect.DeepEqual(previousLineage, freshLineage) {
		t.Fatalf("expected action-linked boundary-test lineage to differ before hook, got %+v", previousLineage)
	}

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
		if err != nil {
			t.Fatalf("get source queue in action-linked create hook: %v", err)
		}
		currentPayload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
		if err != nil {
			t.Fatalf("decode source queue payload in action-linked create hook: %v", err)
		}
		currentPayload.RootCauseID = freshLineage.RootCauseID
		currentPayload.ProvenanceGroupID = freshLineage.ProvenanceGroupID
		currentPayload.ParentRefsJSON = append([]string(nil), freshLineage.ParentRefsJSON...)
		currentPayload.Normalize()
		currentPayloadJSON, err := json.Marshal(currentPayload)
		if err != nil {
			t.Fatalf("marshal current source queue payload in action-linked create hook: %v", err)
		}
		if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			QueueID:                 currentSourceQueue.QueueID,
			WorkspaceID:             currentSourceQueue.WorkspaceID,
			QueueKey:                currentSourceQueue.QueueKey,
			QueueType:               currentSourceQueue.QueueType,
			Title:                   currentSourceQueue.Title,
			Summary:                 currentSourceQueue.Summary,
			Details:                 currentSourceQueue.Details,
			PayloadJSON:             string(currentPayloadJSON),
			AssignedTo:              currentSourceQueue.AssignedTo,
			Urgency:                 currentSourceQueue.Urgency,
			SourceKind:              currentSourceQueue.SourceKind,
			SourceID:                currentSourceQueue.SourceID,
			TaskID:                  currentSourceQueue.TaskID,
			SessionID:               currentSourceQueue.SessionID,
			AgentID:                 currentSourceQueue.AgentID,
			KeepSessionActive:       currentSourceQueue.KeepSessionActive,
			DueAt:                   derefString(currentSourceQueue.DueAt),
			RequireCurrentUpdatedAt: currentSourceQueue.UpdatedAt,
		}); err != nil {
			t.Fatalf("refresh source queue lineage inside action-linked create hook: %v", err)
		}
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "action-linked rollback carrier create path should derive current source lineage inside create boundary",
		TaskID:         taskID,
		AgentID:        agentID,
		ActionID:       actionID,
		RunID:          "run-rollback-failure-create-action-lineage",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-action-boundary-root",
			ProvenanceGroupID: "evt-stale-action-boundary-prov",
			ParentRefsJSON:    []string{"evt-stale-action-boundary-parent"},
		},
	})

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "execution_run",
		ActionID:     actionID,
	})
	recoveryPayload, err := actionCreateDecodeRollbackFailurePayload(recoveryQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode action-linked boundary recovery queue payload: %v", err)
	}
	gotLineage := rebaseRollbackFailurePayloadLineage(recoveryPayload)
	if !reflect.DeepEqual(gotLineage, freshLineage) {
		t.Fatalf("action-linked rollback failure create path should derive fresh source lineage inside create boundary, got %+v want %+v", gotLineage, freshLineage)
	}
	if reflect.DeepEqual(gotLineage, previousLineage) {
		t.Fatalf("action-linked rollback failure create path reused stale pre-hook lineage %+v", gotLineage)
	}
	if recoveryPayload.ActionID != actionID {
		t.Fatalf("expected action-linked recovery payload to retain action id %q, got %+v", actionID, recoveryPayload)
	}
	if recoveryPayload.SourceQueueID != sourceQueueID || recoveryPayload.SourceQueueKey != sourceQueue.QueueKey {
		t.Fatalf("expected action-linked recovery payload to canonicalize current source queue linkage, got %+v", recoveryPayload)
	}
}

func TestQueueRebaseRollbackFailureCreateRejectsLostActionLinkedCarrierInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-action-lost-carrier"
		taskID      = "task-rollback-failure-create-action-lost-carrier"
		agentID     = "agent-rollback-failure-create-action-lost-carrier"
		repairID    = "tens-repair-rollback-failure-create-action-lost-carrier"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		corruptActionQueueSourceLinkForControlPlaneTest(t, ctx, store, workspaceID, actionID, sourceQueueID)
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "action-linked rollback carrier loss should fail closed inside create boundary",
		TaskID:         taskID,
		AgentID:        agentID,
		ActionID:       actionID,
		RunID:          "run-rollback-failure-create-action-lost-carrier",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-action-lost-carrier-root",
			ProvenanceGroupID: "evt-stale-action-lost-carrier-prov",
			ParentRefsJSON:    []string{"evt-stale-action-lost-carrier-parent"},
		},
	})

	queueKey := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "execution_run",
		ActionID:     actionID,
	})
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected lost action-linked carrier inside create boundary to fail closed without recovery queue")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("GetOperatorQueueItem(lost action-linked rollback queue): %v", err)
	}
}

func TestQueueRebaseRollbackFailureCreateCanonicalizesCurrentRepairLinkedSourceLineageInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-repair-lineage"
		taskID      = "task-rollback-failure-create-repair-lineage"
		agentID     = "agent-rollback-failure-create-repair-lineage"
		repairID    = "tens-repair-rollback-failure-create-repair-lineage"
	)

	_, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("get source queue before repair-linked boundary test: %v", err)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue before repair-linked boundary test: %v", err)
	}
	previousLineage := rebaseFollowupPayloadLineage(sourcePayload)
	if previousLineage.RootCauseID == "" && previousLineage.ProvenanceGroupID == "" && len(previousLineage.ParentRefsJSON) == 0 {
		var ok bool
		previousLineage, ok = h.latestHumanActionLineage(ctx, workspaceID, sourcePayload.ActionID)
		if !ok {
			t.Fatalf("expected repair-linked source context to provide pre-hook lineage")
		}
	}

	currentSourceEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       1,
	})
	freshLineage := rebaseRuntimeLineage{
		RootCauseID:       "evt-repair-create-boundary-fresh-root",
		ProvenanceGroupID: "evt-repair-create-boundary-fresh-prov",
		ParentRefsJSON:    []string{currentSourceEvent.EventID},
	}
	if reflect.DeepEqual(previousLineage, freshLineage) {
		t.Fatalf("expected repair-linked boundary-test lineage to differ before hook, got %+v", previousLineage)
	}

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
		if err != nil {
			t.Fatalf("get source queue in repair-linked create hook: %v", err)
		}
		currentPayload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
		if err != nil {
			t.Fatalf("decode source queue payload in repair-linked create hook: %v", err)
		}
		currentPayload.RootCauseID = freshLineage.RootCauseID
		currentPayload.ProvenanceGroupID = freshLineage.ProvenanceGroupID
		currentPayload.ParentRefsJSON = append([]string(nil), freshLineage.ParentRefsJSON...)
		currentPayload.Normalize()
		currentPayloadJSON, err := json.Marshal(currentPayload)
		if err != nil {
			t.Fatalf("marshal current source queue payload in repair-linked create hook: %v", err)
		}
		if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			QueueID:                 currentSourceQueue.QueueID,
			WorkspaceID:             currentSourceQueue.WorkspaceID,
			QueueKey:                currentSourceQueue.QueueKey,
			QueueType:               currentSourceQueue.QueueType,
			Title:                   currentSourceQueue.Title,
			Summary:                 currentSourceQueue.Summary,
			Details:                 currentSourceQueue.Details,
			PayloadJSON:             string(currentPayloadJSON),
			AssignedTo:              currentSourceQueue.AssignedTo,
			Urgency:                 currentSourceQueue.Urgency,
			SourceKind:              currentSourceQueue.SourceKind,
			SourceID:                currentSourceQueue.SourceID,
			TaskID:                  currentSourceQueue.TaskID,
			SessionID:               currentSourceQueue.SessionID,
			AgentID:                 currentSourceQueue.AgentID,
			KeepSessionActive:       currentSourceQueue.KeepSessionActive,
			DueAt:                   derefString(currentSourceQueue.DueAt),
			RequireCurrentUpdatedAt: currentSourceQueue.UpdatedAt,
		}); err != nil {
			t.Fatalf("refresh source queue lineage inside repair-linked create hook: %v", err)
		}
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "execution_run",
		FailureTrigger:  "execution_run_verifier_failed",
		FailureMessage:  "repair-linked rollback carrier create path should derive current source lineage inside create boundary",
		TaskID:          taskID,
		AgentID:         agentID,
		RepairTensionID: repairID,
		RunID:           "run-rollback-failure-create-repair-lineage",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-repair-boundary-root",
			ProvenanceGroupID: "evt-stale-repair-boundary-prov",
			ParentRefsJSON:    []string{"evt-stale-repair-boundary-parent"},
		},
	})

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "execution_run",
		RepairTensionID: repairID,
	})
	recoveryPayload, err := actionCreateDecodeRollbackFailurePayload(recoveryQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode repair-linked boundary recovery queue payload: %v", err)
	}
	gotLineage := rebaseRollbackFailurePayloadLineage(recoveryPayload)
	if !reflect.DeepEqual(gotLineage, freshLineage) {
		t.Fatalf("repair-linked rollback failure create path should derive fresh source lineage inside create boundary, got %+v want %+v", gotLineage, freshLineage)
	}
	if reflect.DeepEqual(gotLineage, previousLineage) {
		t.Fatalf("repair-linked rollback failure create path reused stale pre-hook lineage %+v", gotLineage)
	}
	if recoveryPayload.RepairTensionID != repairID {
		t.Fatalf("expected repair-linked recovery payload to retain repair tension id %q, got %+v", repairID, recoveryPayload)
	}
	if recoveryPayload.SourceQueueID != sourceQueueID || recoveryPayload.SourceQueueKey != sourceQueue.QueueKey {
		t.Fatalf("expected repair-linked recovery payload to canonicalize current source queue linkage, got %+v", recoveryPayload)
	}
}

func TestQueueRebaseRollbackFailureCreateRejectsLostRepairLinkedCarrierInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-repair-lost-carrier"
		taskID      = "task-rollback-failure-create-repair-lost-carrier"
		agentID     = "agent-rollback-failure-create-repair-lost-carrier"
		repairID    = "tens-repair-rollback-failure-create-repair-lost-carrier"
	)

	_, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		corruptRebaseSourceQueuePayloadForControlPlaneTest(t, ctx, store, workspaceID, sourceQueueID)
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "execution_run",
		FailureTrigger:  "execution_run_verifier_failed",
		FailureMessage:  "repair-linked rollback carrier loss should fail closed inside create boundary",
		TaskID:          taskID,
		AgentID:         agentID,
		RepairTensionID: repairID,
		RunID:           "run-rollback-failure-create-repair-lost-carrier",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-repair-lost-carrier-root",
			ProvenanceGroupID: "evt-stale-repair-lost-carrier-prov",
			ParentRefsJSON:    []string{"evt-stale-repair-lost-carrier-parent"},
		},
	})

	queueKey := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope:    "execution_run",
		RepairTensionID: repairID,
	})
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected lost repair-linked carrier inside create boundary to fail closed without recovery queue")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("GetOperatorQueueItem(lost repair-linked rollback queue): %v", err)
	}
}

func TestQueueRebaseRollbackFailureCreateCanonicalizesCurrentTaskRunLinkedSourceLineageInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-task-run-lineage"
		taskID      = "task-rollback-failure-create-task-run-lineage"
		agentID     = "agent-rollback-failure-create-task-run-lineage"
		repairID    = "tens-repair-rollback-failure-create-task-run-lineage"
		runID       = "run-rollback-failure-create-task-run-lineage"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("get source queue before task/run-linked boundary test: %v", err)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue before task/run-linked boundary test: %v", err)
	}
	previousLineage := rebaseFollowupPayloadLineage(sourcePayload)
	if previousLineage.RootCauseID == "" && previousLineage.ProvenanceGroupID == "" && len(previousLineage.ParentRefsJSON) == 0 {
		var ok bool
		previousLineage, ok = h.latestHumanActionLineage(ctx, workspaceID, sourcePayload.ActionID)
		if !ok {
			t.Fatalf("expected task/run-linked source context to provide pre-hook lineage")
		}
	}

	currentSourceEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       1,
	})
	freshLineage := rebaseRuntimeLineage{
		RootCauseID:       "evt-task-run-create-boundary-fresh-root",
		ProvenanceGroupID: "evt-task-run-create-boundary-fresh-prov",
		ParentRefsJSON:    []string{currentSourceEvent.EventID},
	}
	if reflect.DeepEqual(previousLineage, freshLineage) {
		t.Fatalf("expected task/run-linked boundary-test lineage to differ before hook, got %+v", previousLineage)
	}

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
		if err != nil {
			t.Fatalf("get source queue in task/run-linked create hook: %v", err)
		}
		currentPayload, err := actionCreateDecodeQueuePayload(currentSourceQueue.PayloadJSON)
		if err != nil {
			t.Fatalf("decode source queue payload in task/run-linked create hook: %v", err)
		}
		currentPayload.RootCauseID = freshLineage.RootCauseID
		currentPayload.ProvenanceGroupID = freshLineage.ProvenanceGroupID
		currentPayload.ParentRefsJSON = append([]string(nil), freshLineage.ParentRefsJSON...)
		currentPayload.Normalize()
		currentPayloadJSON, err := json.Marshal(currentPayload)
		if err != nil {
			t.Fatalf("marshal current source queue payload in task/run-linked create hook: %v", err)
		}
		if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			QueueID:                 currentSourceQueue.QueueID,
			WorkspaceID:             currentSourceQueue.WorkspaceID,
			QueueKey:                currentSourceQueue.QueueKey,
			QueueType:               currentSourceQueue.QueueType,
			Title:                   currentSourceQueue.Title,
			Summary:                 currentSourceQueue.Summary,
			Details:                 currentSourceQueue.Details,
			PayloadJSON:             string(currentPayloadJSON),
			AssignedTo:              currentSourceQueue.AssignedTo,
			Urgency:                 currentSourceQueue.Urgency,
			SourceKind:              currentSourceQueue.SourceKind,
			SourceID:                currentSourceQueue.SourceID,
			TaskID:                  currentSourceQueue.TaskID,
			SessionID:               currentSourceQueue.SessionID,
			AgentID:                 currentSourceQueue.AgentID,
			KeepSessionActive:       currentSourceQueue.KeepSessionActive,
			DueAt:                   derefString(currentSourceQueue.DueAt),
			RequireCurrentUpdatedAt: currentSourceQueue.UpdatedAt,
		}); err != nil {
			t.Fatalf("refresh source queue lineage inside task/run-linked create hook: %v", err)
		}
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "task/run-linked rollback carrier create path should derive current source lineage inside create boundary",
		RunID:          runID,
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-task-run-boundary-root",
			ProvenanceGroupID: "evt-stale-task-run-boundary-prov",
			ParentRefsJSON:    []string{"evt-stale-task-run-boundary-parent"},
		},
	})

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "execution_run",
		RunID:        runID,
	})
	recoveryPayload, err := actionCreateDecodeRollbackFailurePayload(recoveryQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode task/run-linked boundary recovery queue payload: %v", err)
	}
	gotLineage := rebaseRollbackFailurePayloadLineage(recoveryPayload)
	if !reflect.DeepEqual(gotLineage, freshLineage) {
		t.Fatalf("task/run-linked rollback failure create path should derive fresh source lineage inside create boundary, got %+v want %+v", gotLineage, freshLineage)
	}
	if reflect.DeepEqual(gotLineage, previousLineage) {
		t.Fatalf("task/run-linked rollback failure create path reused stale pre-hook lineage %+v", gotLineage)
	}
	if recoveryPayload.ActionID != actionID || recoveryPayload.RepairTensionID != repairID {
		t.Fatalf("expected task/run-linked recovery payload to canonicalize action/repair linkage, got %+v", recoveryPayload)
	}
	if recoveryPayload.SourceQueueID != sourceQueueID || recoveryPayload.SourceQueueKey != sourceQueue.QueueKey {
		t.Fatalf("expected task/run-linked recovery payload to canonicalize current source queue linkage, got %+v", recoveryPayload)
	}
}

func TestQueueRebaseRollbackFailureCreateRejectsAmbiguousTaskRunLinkedCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-task-run-ambiguous"
		taskID      = "task-rollback-failure-create-task-run-ambiguous"
		agentID     = "agent-rollback-failure-create-task-run-ambiguous"
		runID       = "run-rollback-failure-create-task-run-ambiguous"
	)

	createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, "tens-repair-rollback-failure-create-task-run-ambiguous-a")
	createStartedRebaseFollowupActionOnExistingWorkspaceForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, "tens-repair-rollback-failure-create-task-run-ambiguous-b")
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "ambiguous task/run-linked rollback carrier should fail closed",
		RunID:          runID,
	})

	queueKey := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "execution_run",
		RunID:        runID,
	})
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected ambiguous task/run-linked rollback create to fail closed without recovery queue")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("GetOperatorQueueItem(ambiguous task/run rollback queue): %v", err)
	}
}

func TestQueueRebaseRollbackFailureCreateRejectsLostTaskRunLinkedCarrierInsideCreateBoundary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-task-run-lost-carrier"
		taskID      = "task-rollback-failure-create-task-run-lost-carrier"
		agentID     = "agent-rollback-failure-create-task-run-lost-carrier"
		repairID    = "tens-repair-rollback-failure-create-task-run-lost-carrier"
		runID       = "run-rollback-failure-create-task-run-lost-carrier"
	)

	_, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	h.beforeRebaseRollbackFailureCreateOverride = func(ctx context.Context, _ string) {
		h.beforeRebaseRollbackFailureCreateOverride = nil
		sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
		if err != nil {
			t.Fatalf("get source queue in lost-carrier create hook: %v", err)
		}
		if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
			WorkspaceID: workspaceID,
			QueueID:     sourceQueue.QueueID,
			Status:      "RESOLVED",
			ResolvedBy:  "tests",
			Resolution:  "simulate carrier loss before rollback-failure create commit",
		}); err != nil {
			t.Fatalf("resolve source queue in lost-carrier create hook: %v", err)
		}
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "task/run-linked rollback carrier loss should fail closed inside create boundary",
		RunID:          runID,
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-task-run-lost-carrier-root",
			ProvenanceGroupID: "evt-stale-task-run-lost-carrier-prov",
			ParentRefsJSON:    []string{"evt-stale-task-run-lost-carrier-parent"},
		},
	})

	queueKey := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "execution_run",
		RunID:        runID,
	})
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected lost task/run-linked carrier inside create boundary to fail closed without recovery queue")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("GetOperatorQueueItem(lost task/run rollback queue): %v", err)
	}
}

func TestQueueRebaseRollbackFailureCreateRejectsConflictingTaskRunCarrierIdentity(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-task-run-conflict"
		taskID      = "task-rollback-failure-create-task-run-conflict"
		runTaskID   = "task-rollback-failure-create-task-run-conflict-run"
		agentID     = "agent-rollback-failure-create-task-run-conflict"
		repairID    = "tens-repair-rollback-failure-create-task-run-conflict"
		runID       = "run-rollback-failure-create-task-run-conflict"
	)

	createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate conflicting run task graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      runTaskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create conflicting run task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      runTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach conflicting run task: %v", err)
	}
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, runTaskID, agentID, runID)

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "conflicting task/run carrier identity should fail closed",
		TaskID:         taskID,
		RunID:          runID,
	})

	queueKey := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "execution_run",
		RunID:        runID,
	})
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		t.Fatalf("expected conflicting task/run-linked rollback create to fail closed without recovery queue")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("GetOperatorQueueItem(conflicting task/run rollback queue): %v", err)
	}
}

func TestRebaseRollbackFailureQueueKeyUsesEventIDForQueueOnlyAnomalyListScope(t *testing.T) {
	t.Parallel()

	first := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-anomaly-a",
		EntityID:     "entity-shared",
	})
	second := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-anomaly-b",
		EntityID:     "entity-shared",
	})
	repeated := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-anomaly-a",
		EntityID:     "entity-shared",
	})
	fallback := rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		FailureScope: "rsp_anomaly_list",
		EntityID:     "entity-shared",
	})

	if first == "" || second == "" || repeated == "" || fallback == "" {
		t.Fatalf("expected non-empty queue keys, got first=%q second=%q repeated=%q fallback=%q", first, second, repeated, fallback)
	}
	if first == second {
		t.Fatalf("expected different anomaly-list queue keys for distinct event ids, got %q", first)
	}
	if first != repeated {
		t.Fatalf("expected same anomaly-list queue key for repeated event id, first=%q repeated=%q", first, repeated)
	}
	if fallback == first {
		t.Fatalf("expected event-aware anomaly-list queue key to differ from entity-only fallback, both=%q", first)
	}
}

func TestQueueRebaseRollbackFailureRefreshesFailureContextWithoutDroppingFollowupStateOrLineage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-followup-refresh"
		taskID        = "task-rollback-failure-followup-refresh"
		agentID       = "agent-rollback-failure-followup-refresh"
		sourceQueueID = "opq-rollback-failure-followup-refresh"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	input := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback path stayed broken during retry",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-followup-refresh",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-rollback-followup-refresh-root",
			ProvenanceGroupID: "evt-rollback-followup-refresh-prov",
		},
	}

	sourceQueue := createRebaseRollbackFailureQueueForTest(t, ctx, store, input)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	linkedQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	updatedAt := linkedQueue.UpdatedAt
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    linkedQueue.QueueID,
		Limit:       20,
	})

	input.FailureMessage = "rollback path is still broken after re-check"
	input.Lineage = rebaseRuntimeLineage{}
	h.queueRebaseRollbackFailure(ctx, input)

	updated := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if updated.UpdatedAt == updatedAt {
		t.Fatalf("rollback failure queue updated_at did not change after failure context refresh")
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(updated.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated rollback failure payload: %v", err)
	}
	if strings.TrimSpace(payload.FailureMessage) != input.FailureMessage {
		t.Fatalf("failure_message = %q, want %q", payload.FailureMessage, input.FailureMessage)
	}
	if strings.TrimSpace(payload.FollowupActionID) != actionID {
		t.Fatalf("followup_action_id = %q, want %q", payload.FollowupActionID, actionID)
	}
	if payload.RootCauseID != "evt-rollback-followup-refresh-root" || payload.ProvenanceGroupID != "evt-rollback-followup-refresh-prov" {
		t.Fatalf("rollback lineage should survive failure-context refresh without new lineage, got %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    linkedQueue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents)+1 {
		t.Fatalf("rollback failure refresh should emit one extra runtime event: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureRefreshPreservesExistingParentRefsOnPartialMatchingLineage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-partial-lineage-refresh"
		taskID        = "task-rollback-failure-partial-lineage-refresh"
		agentID       = "agent-rollback-failure-partial-lineage-refresh"
		sourceQueueID = "opq-rollback-failure-partial-lineage-refresh"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	input := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback path stayed broken during retry",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-partial-lineage-refresh",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-rollback-partial-root",
			ProvenanceGroupID: "evt-rollback-partial-prov",
		},
	}

	queue := createRebaseRollbackFailureQueueForTest(t, ctx, store, input)
	queueCreateEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	})

	authoritative := rebaseRollbackFailurePayload(input)
	authoritative.RootCauseID = input.Lineage.RootCauseID
	authoritative.ProvenanceGroupID = input.Lineage.ProvenanceGroupID
	authoritative.ParentRefsJSON = []string{queueCreateEvent.EventID}
	authoritative.Normalize()
	queue = overwriteRebaseRollbackFailureQueuePayloadForTest(t, ctx, store, queue, input, authoritative)
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	})

	partial := input
	partial.FailureMessage = "rollback path is still broken after fresh re-check"
	partial.Lineage = rebaseRuntimeLineage{
		RootCauseID:       authoritative.RootCauseID,
		ProvenanceGroupID: authoritative.ProvenanceGroupID,
	}

	h.queueRebaseRollbackFailure(ctx, partial)

	updated := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	payload, err := actionCreateDecodeRollbackFailurePayload(updated.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated rollback failure payload: %v", err)
	}
	if strings.TrimSpace(payload.FailureMessage) != partial.FailureMessage {
		t.Fatalf("failure_message = %q, want %q", payload.FailureMessage, partial.FailureMessage)
	}
	if payload.RootCauseID != authoritative.RootCauseID || payload.ProvenanceGroupID != authoritative.ProvenanceGroupID {
		t.Fatalf("partial matching lineage refresh should preserve current root/provenance, got %+v", payload)
	}
	if !reflect.DeepEqual(payload.ParentRefsJSON, authoritative.ParentRefsJSON) {
		t.Fatalf("partial matching lineage refresh should preserve existing parent refs, got %+v want %+v", payload.ParentRefsJSON, authoritative.ParentRefsJSON)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents)+1 {
		t.Fatalf("partial-lineage refresh should emit one extra runtime event: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureTreatsExplicitConflictingLineageReplayAsNoop(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-conflicting-lineage-noop"
		taskID        = "task-rollback-failure-conflicting-lineage-noop"
		agentID       = "agent-rollback-failure-conflicting-lineage-noop"
		sourceQueueID = "opq-rollback-failure-conflicting-lineage-noop"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	initial := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback path stayed broken during retry",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-conflicting-lineage-noop",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-rollback-conflict-old-root",
			ProvenanceGroupID: "evt-rollback-conflict-old-prov",
		},
	}

	queue := createRebaseRollbackFailureQueueForTest(t, ctx, store, initial)
	queueCreateEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	})

	authoritative := rebaseRollbackFailurePayload(initial)
	authoritative.RootCauseID = "evt-rollback-conflict-new-root"
	authoritative.ProvenanceGroupID = "evt-rollback-conflict-new-prov"
	authoritative.ParentRefsJSON = []string{queueCreateEvent.EventID}
	authoritative.Normalize()
	queue = overwriteRebaseRollbackFailureQueuePayloadForTest(t, ctx, store, queue, initial, authoritative)
	updatedAt := queue.UpdatedAt
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	})

	h.queueRebaseRollbackFailure(ctx, initial)

	current := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  initial.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if current.UpdatedAt != updatedAt {
		t.Fatalf("conflicting stale lineage replay changed rollback failure queue updated_at: before=%q after=%q", updatedAt, current.UpdatedAt)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode current rollback failure payload: %v", err)
	}
	if payload.RootCauseID != authoritative.RootCauseID || payload.ProvenanceGroupID != authoritative.ProvenanceGroupID {
		t.Fatalf("conflicting stale lineage replay should preserve current root/provenance, got %+v", payload)
	}
	if !reflect.DeepEqual(payload.ParentRefsJSON, authoritative.ParentRefsJSON) {
		t.Fatalf("conflicting stale lineage replay should preserve current parent refs, got %+v want %+v", payload.ParentRefsJSON, authoritative.ParentRefsJSON)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents) {
		t.Fatalf("conflicting stale lineage replay emitted extra runtime events: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureDoesNotReopenResolvedIdenticalReplay(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-resolved-no-reopen"
		taskID        = "task-rollback-failure-resolved-no-reopen"
		agentID       = "agent-rollback-failure-resolved-no-reopen"
		sourceQueueID = "opq-rollback-failure-resolved-no-reopen"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	input := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback path stayed broken during retry",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-resolved-no-reopen",
	}

	sourceQueue := createRebaseRollbackFailureQueueForTest(t, ctx, store, input)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Recovered the rollback failure.",
		ResolvedBy: "operator:rollback-no-reopen",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	resolvedQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if resolvedQueue.Status != "RESOLVED" {
		t.Fatalf("rollback failure queue status = %q, want RESOLVED", resolvedQueue.Status)
	}
	updatedAt := resolvedQueue.UpdatedAt
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedQueue.QueueID,
		Limit:       20,
	})

	h.queueRebaseRollbackFailure(ctx, input)

	unchanged := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if unchanged.Status != "RESOLVED" {
		t.Fatalf("rollback failure queue reopened on identical replay: status=%q", unchanged.Status)
	}
	if unchanged.UpdatedAt != updatedAt {
		t.Fatalf("resolved rollback failure queue updated_at changed on identical replay: before=%q after=%q", updatedAt, unchanged.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedQueue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents) {
		t.Fatalf("resolved rollback failure replay emitted extra runtime events: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureDoesNotReopenResolvedQueueOnChangedFailureContext(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-resolved-no-reopen-changed-context"
		taskID        = "task-rollback-failure-resolved-no-reopen-changed-context"
		agentID       = "agent-rollback-failure-resolved-no-reopen-changed-context"
		sourceQueueID = "opq-rollback-failure-resolved-no-reopen-changed-context"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	input := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "rollback path stayed broken during retry",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-resolved-no-reopen-changed-context",
	}

	sourceQueue := createRebaseRollbackFailureQueueForTest(t, ctx, store, input)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Recovered the rollback failure.",
		ResolvedBy: "operator:rollback-no-reopen-changed-context",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	resolvedQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if resolvedQueue.Status != "RESOLVED" {
		t.Fatalf("rollback failure queue status = %q, want RESOLVED", resolvedQueue.Status)
	}
	updatedAt := resolvedQueue.UpdatedAt
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedQueue.QueueID,
		Limit:       20,
	})

	input.FailureMessage = "a repeated late-fail should not reopen this resolved queue"
	h.queueRebaseRollbackFailure(ctx, input)

	unchanged := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  input.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if unchanged.Status != "RESOLVED" {
		t.Fatalf("rollback failure queue reopened on changed-context replay: status=%q", unchanged.Status)
	}
	if unchanged.UpdatedAt != updatedAt {
		t.Fatalf("resolved rollback failure queue updated_at changed on changed-context replay: before=%q after=%q", updatedAt, unchanged.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedQueue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents) {
		t.Fatalf("resolved rollback failure changed-context replay emitted extra runtime events: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureRejectsStaleOpenRefreshWithChangedFailureContext(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-stale-open-refresh"
		taskID        = "task-rollback-failure-stale-open-refresh"
		agentID       = "agent-rollback-failure-stale-open-refresh"
		sourceQueueID = "opq-rollback-failure-stale-open-refresh"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	initial := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_step",
		FailureTrigger: "execution_verifier_failed",
		FailureMessage: "initial rollback failure",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		StepID:         "step-rollback-failure-stale-open-refresh",
	}
	queue := createRebaseRollbackFailureQueueForTest(t, ctx, store, initial)
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	})

	stale := initial
	stale.FailureMessage = "stale replay should lose"
	fresh := initial
	fresh.FailureMessage = "fresh open refresh wins"

	var refreshed sqlite.OperatorQueueRecord
	h.beforeRebaseRollbackFailureUpsertOverride = func(ctx context.Context, existing sqlite.OperatorQueueRecord) {
		h.beforeRebaseRollbackFailureUpsertOverride = nil
		refreshed = refreshRebaseRollbackFailureQueueForTest(t, ctx, store, existing, fresh)
	}

	h.queueRebaseRollbackFailure(ctx, stale)

	current := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  initial.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if refreshed.QueueID == "" {
		t.Fatalf("expected refresh hook to capture a newer queue revision")
	}
	if current.UpdatedAt != refreshed.UpdatedAt {
		t.Fatalf("stale replay changed open queue updated_at: got %q want %q", current.UpdatedAt, refreshed.UpdatedAt)
	}
	if current.Revision != refreshed.Revision {
		t.Fatalf("stale replay changed open queue revision: got %d want %d", current.Revision, refreshed.Revision)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode current rollback failure payload: %v", err)
	}
	if strings.TrimSpace(payload.FailureMessage) != fresh.FailureMessage {
		t.Fatalf("failure_message = %q, want %q after stale replay rejection", payload.FailureMessage, fresh.FailureMessage)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents)+1 {
		t.Fatalf("stale open replay should not append a second event after refresh: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureTreatsRevisionConflictOnIdenticalReplayAsNoop(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-identical-race-noop"
		taskID        = "task-rollback-failure-identical-race-noop"
		agentID       = "agent-rollback-failure-identical-race-noop"
		sourceQueueID = "opq-rollback-failure-identical-race-noop"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	initial := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_step",
		FailureTrigger: "execution_verifier_failed",
		FailureMessage: "baseline rollback failure",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		StepID:         "step-rollback-failure-identical-race-noop",
	}
	queue := createRebaseRollbackFailureQueueForTest(t, ctx, store, initial)
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	})

	target := initial
	target.FailureMessage = "identical raced refresh target"

	var refreshed sqlite.OperatorQueueRecord
	h.beforeRebaseRollbackFailureUpsertOverride = func(ctx context.Context, existing sqlite.OperatorQueueRecord) {
		h.beforeRebaseRollbackFailureUpsertOverride = nil
		refreshed = refreshRebaseRollbackFailureQueueForTest(t, ctx, store, existing, target)
	}

	h.queueRebaseRollbackFailure(ctx, target)

	current := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  initial.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if refreshed.QueueID == "" {
		t.Fatalf("expected refresh hook to capture a newer identical queue revision")
	}
	if current.UpdatedAt != refreshed.UpdatedAt {
		t.Fatalf("identical replay changed queue updated_at after CAS conflict: got %q want %q", current.UpdatedAt, refreshed.UpdatedAt)
	}
	if current.Revision != refreshed.Revision {
		t.Fatalf("identical replay changed queue revision after CAS conflict: got %d want %d", current.Revision, refreshed.Revision)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode current rollback failure payload: %v", err)
	}
	if strings.TrimSpace(payload.FailureMessage) != target.FailureMessage {
		t.Fatalf("failure_message = %q, want %q after identical CAS-conflict noop", payload.FailureMessage, target.FailureMessage)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents)+1 {
		t.Fatalf("identical CAS-conflict replay should not emit extra runtime events: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestQueueRebaseRollbackFailureRejectsStaleResolvedRefreshWithoutReopen(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-rollback-failure-stale-resolved-refresh"
		taskID        = "task-rollback-failure-stale-resolved-refresh"
		agentID       = "agent-rollback-failure-stale-resolved-refresh"
		sourceQueueID = "opq-rollback-failure-stale-resolved-refresh"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	initial := rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "execution_run",
		FailureTrigger: "execution_run_verifier_failed",
		FailureMessage: "resolved rollback failure baseline",
		TaskID:         taskID,
		AgentID:        agentID,
		SourceQueueID:  sourceQueueID,
		RunID:          "run-rollback-failure-stale-resolved-refresh",
	}
	sourceQueue := createRebaseRollbackFailureQueueForTest(t, ctx, store, initial)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Recovered the rollback failure before stale replay.",
		ResolvedBy: "operator:stale-resolved-refresh",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	resolvedQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  initial.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	seenEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedQueue.QueueID,
		Limit:       20,
	})

	stale := initial
	stale.FailureMessage = "stale resolved replay should lose"
	fresh := initial
	fresh.FailureMessage = "resolved refresh should now stay terminal"

	var refreshed sqlite.OperatorQueueRecord
	h.beforeRebaseRollbackFailureUpsertOverride = func(ctx context.Context, existing sqlite.OperatorQueueRecord) {
		h.beforeRebaseRollbackFailureUpsertOverride = nil
		refreshed = refreshRebaseRollbackFailureQueueForTest(t, ctx, store, existing, fresh)
	}

	h.queueRebaseRollbackFailure(ctx, stale)

	current := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  initial.FailureScope,
		SourceQueueID: sourceQueueID,
	})
	if refreshed.QueueID != "" {
		t.Fatalf("resolved rollback-failure queue should stay terminal and skip refresh")
	}
	if current.Status != "RESOLVED" {
		t.Fatalf("stale resolved replay reopened queue: status=%q", current.Status)
	}
	if current.UpdatedAt != resolvedQueue.UpdatedAt {
		t.Fatalf("stale resolved replay changed queue updated_at: got %q want %q", current.UpdatedAt, resolvedQueue.UpdatedAt)
	}
	if current.Revision != resolvedQueue.Revision {
		t.Fatalf("stale resolved replay changed queue revision: got %d want %d", current.Revision, resolvedQueue.Revision)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode current resolved rollback failure payload: %v", err)
	}
	if strings.TrimSpace(payload.FailureMessage) != initial.FailureMessage {
		t.Fatalf("failure_message = %q, want %q after stale resolved replay rejection", payload.FailureMessage, initial.FailureMessage)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedQueue.QueueID,
		Limit:       20,
	}); len(got) != len(seenEvents) {
		t.Fatalf("stale resolved replay should not append any new event after terminal resolve: before=%d after=%d", len(seenEvents), len(got))
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifyRollsBackLinkedRebaseFollowupBySourceQueueID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-rebase-source-queue"
		taskID      = "task-execution-step-rebase-source-queue"
		agentID     = "agent-execution-step-rebase-source-queue"
		repairID    = "tens-repair-execution-step-rebase-source-queue"
		runID       = "run-execution-step-rebase-source-queue"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier gate failed",
		Summary:     "verification mismatch on rebase follow-up",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.ResolvedBy != "system:execution_verifier" {
		t.Fatalf("action resolved_by = %q, want system:execution_verifier", action.ResolvedBy)
	}
	if !strings.Contains(action.ResolutionComment, "verification mismatch on rebase follow-up") {
		t.Fatalf("action resolution comment = %q, want verifier late-fail context", action.ResolutionComment)
	}
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	payload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload: %v", err)
	}
	if payload.RollbackReason != "execution_verifier_failed" {
		t.Fatalf("rollback_reason = %q, want execution_verifier_failed", payload.RollbackReason)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifySupportsRetryPromotionAndCompletion(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-blackbox-retry-complete"
		taskID      = "task-execution-step-blackbox-retry-complete"
		agentID     = "agent-execution-step-blackbox-retry-complete"
		repairID    = "tens-repair-execution-step-blackbox-retry-complete"
		runID       = "run-execution-step-blackbox-retry-complete"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed but should keep retry path healthy",
		Summary:     "black-box late fail must reopen the same rebase source queue for retry",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	failedAction, err := store.GetHumanAction(ctx, failedActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", failedActionID, err)
	}
	if failedAction.ResolvedBy != "system:execution_verifier" {
		t.Fatalf("failed action resolved_by = %q, want system:execution_verifier", failedAction.ResolvedBy)
	}

	failedActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", failedActionID)
	if failedActionQueue.Status != "RESOLVED" {
		t.Fatalf("failed action queue status = %q, want RESOLVED", failedActionQueue.Status)
	}

	reopenedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if reopenedSourceQueue.Status != "OPEN" {
		t.Fatalf("reopened source queue status = %q, want OPEN", reopenedSourceQueue.Status)
	}
	reopenedPayload, err := actionCreateDecodeQueuePayload(reopenedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode reopened source queue payload: %v", err)
	}
	if reopenedPayload.ActionID != "" || reopenedPayload.ActionQueueKey != "" || reopenedPayload.ActionStatus != "" {
		t.Fatalf("reopened source queue should clear active action linkage, got %+v", reopenedPayload)
	}
	if reopenedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("last_failed_action_id = %q, want %q", reopenedPayload.LastFailedActionID, failedActionID)
	}
	if reopenedPayload.LastFailedStatus != humanActionStatusFailed {
		t.Fatalf("last_failed_status = %q, want %q", reopenedPayload.LastFailedStatus, humanActionStatusFailed)
	}
	if reopenedPayload.RollbackReason != "execution_verifier_failed" {
		t.Fatalf("rollback_reason = %q, want execution_verifier_failed", reopenedPayload.RollbackReason)
	}

	var blockedClaimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&blockedClaimStatus); err != nil {
		t.Fatalf("query blocked task claim after failed verify rollback: %v", err)
	}
	if blockedClaimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED while retry remains open", blockedClaimStatus)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected canonical failed-VERIFY rollback path to avoid rollback-failure recovery queue")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}
	if got, _ := retryCreateResp["source_queue_id"].(string); got != sourceQueueID {
		t.Fatalf("retry actionCreate source_queue_id = %q, want %q", got, sourceQueueID)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	retryLinkedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after retry create: %v", sourceQueueID, err)
	}
	retryLinkedPayload, err := actionCreateDecodeQueuePayload(retryLinkedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode retry-linked source queue payload: %v", err)
	}
	if retryLinkedPayload.ActionID != retryActionID {
		t.Fatalf("active retry action_id = %q, want %q", retryLinkedPayload.ActionID, retryActionID)
	}
	if retryLinkedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("retry payload should preserve failed attempt lineage, got %+v", retryLinkedPayload)
	}

	failedAction, err = store.GetHumanAction(ctx, failedActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s) after retry create: %v", failedActionID, err)
	}
	if failedAction.Status != humanActionStatusFailed {
		t.Fatalf("failed action status after retry create = %q, want %q", failedAction.Status, humanActionStatusFailed)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Retry the rebase after explicit verifier rollback.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Retry landed cleanly after verifier late fail.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	retryResolveAny, rpcErr := h.actionResolve(ctx, retryResolveRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}
	retryResolveResp, ok := retryResolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionResolve response type %T", retryResolveAny)
	}
	if got, _ := retryResolveResp["status"].(string); got != humanActionStatusCompleted {
		t.Fatalf("retry actionResolve status = %q, want %q", got, humanActionStatusCompleted)
	}
	if got, _ := retryResolveResp["source_queue_id"].(string); got != sourceQueueID {
		t.Fatalf("retry actionResolve source_queue_id = %q, want %q", got, sourceQueueID)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)

	completedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after retry completion: %v", sourceQueueID, err)
	}
	if completedSourceQueue.Status != "RESOLVED" {
		t.Fatalf("completed source queue status = %q, want RESOLVED", completedSourceQueue.Status)
	}
	if completedSourceQueue.Resolution != "linked_action_completed:"+retryActionID {
		t.Fatalf("completed source queue resolution = %q, want linked_action_completed:%s", completedSourceQueue.Resolution, retryActionID)
	}
	completedPayload, err := actionCreateDecodeQueuePayload(completedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode completed source queue payload: %v", err)
	}
	if completedPayload.ActionID != retryActionID {
		t.Fatalf("completed payload action_id = %q, want %q", completedPayload.ActionID, retryActionID)
	}
	if completedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("completed payload should preserve failed attempt lineage, got %+v", completedPayload)
	}
	if completedPayload.RollbackReason != "execution_verifier_failed" {
		t.Fatalf("completed payload rollback_reason = %q, want execution_verifier_failed", completedPayload.RollbackReason)
	}

	retryActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", retryActionID)
	if retryActionQueue.Status != "RESOLVED" {
		t.Fatalf("retry action queue status = %q, want RESOLVED", retryActionQueue.Status)
	}

	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&blockedClaimStatus); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected task claim blocker snapshot to be cleared after successful retry, got status=%q err=%v", blockedClaimStatus, err)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifySupportsEscalatedRetryPromotionToNewHolder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-blackbox-retry-handoff"
		taskID      = "task-execution-step-blackbox-retry-handoff"
		agentID     = "agent-execution-step-blackbox-retry-handoff"
		repairID    = "tens-repair-execution-step-blackbox-retry-handoff"
		runID       = "run-execution-step-blackbox-retry-handoff"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed and reopened the retry path",
		Summary:     "the retry should be handoff-able to a new holder before promotion",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueueID, "")
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-b",
		Reason:           "route retry to a different reviewer after late fail",
		AssignedTo:       "reviewer-b",
		Urgency:          "CRITICAL",
		DueAt:            "2099-06-01T00:00:00Z",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}

	reopenedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after handoff: %v", sourceQueueID, err)
	}
	if reopenedSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("reopened source queue assigned_to = %q, want reviewer-b", reopenedSourceQueue.AssignedTo)
	}
	reopenedPayload, err := actionCreateDecodeQueuePayload(reopenedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode reopened source queue payload after handoff: %v", err)
	}
	if reopenedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("reopened payload action_assigned_to = %q, want reviewer-b", reopenedPayload.ActionAssignedTo)
	}
	if reopenedPayload.LastFailedActionID != failedActionID || reopenedPayload.RollbackReason != "execution_verifier_failed" {
		t.Fatalf("reopened payload should preserve failed lineage through handoff, got %+v", reopenedPayload)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}

	retryAction, err := store.GetHumanAction(ctx, retryActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", retryActionID, err)
	}
	if retryAction.AssignedTo != "reviewer-b" {
		t.Fatalf("retry action assigned_to = %q, want reviewer-b", retryAction.AssignedTo)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	staleStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "old holder should not reclaim the handed-off retry",
	})
	if err != nil {
		t.Fatalf("marshal stale retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, staleStartRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected holder mismatch on retry actionStart after handoff, got %+v", rpcErr)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-b",
		Comment:   "new holder starts the handed-off retry",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	staleResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "old holder should not resolve the handed-off retry",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal stale retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, staleResolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected holder mismatch on retry actionResolve after handoff, got %+v", rpcErr)
	}

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "new holder completed the handed-off retry",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, retryResolveRaw); rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)

	completedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after handed-off retry completion: %v", sourceQueueID, err)
	}
	if completedSourceQueue.Status != "RESOLVED" || completedSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("completed source queue after handed-off retry = %+v", completedSourceQueue)
	}
	completedPayload, err := actionCreateDecodeQueuePayload(completedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode completed source queue payload: %v", err)
	}
	if completedPayload.ActionID != retryActionID || completedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("completed payload should mirror winning handed-off retry, got %+v", completedPayload)
	}
	if completedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("completed payload should preserve original failed attempt lineage, got %+v", completedPayload)
	}
	if completedPayload.RollbackReason != "execution_verifier_failed" {
		t.Fatalf("completed payload rollback_reason = %q, want execution_verifier_failed", completedPayload.RollbackReason)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifyCarriesCanonicalLineageAcrossRetryCompletion(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-blackbox-lineage"
		taskID      = "task-execution-step-blackbox-lineage"
		agentID     = "agent-execution-step-blackbox-lineage"
		repairID    = "tens-repair-execution-step-blackbox-lineage"
		runID       = "run-execution-step-blackbox-lineage"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	seenStepEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "execution_step.written", Limit: 50})
	currentRollbackCarrierEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       1,
	})
	seenRollbackQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50})
	seenFailedResolveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: failedActionID, Limit: 50})

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed and should preserve lineage",
		Summary:     "rollback lineage should survive retry completion",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	stepEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "execution_step.written", Limit: 50}, seenStepEvents)
	rollbackQueueEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50}, seenRollbackQueueEvents)
	failedResolveEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: failedActionID, Limit: 50}, seenFailedResolveEvents)

	if rollbackQueueEvent.RootCauseID != stepEvent.EventID || rollbackQueueEvent.ProvenanceGroupID != stepEvent.EventID {
		t.Fatalf("rollback source queue lineage = (%q,%q), want step event %q", rollbackQueueEvent.RootCauseID, rollbackQueueEvent.ProvenanceGroupID, stepEvent.EventID)
	}
	assertRuntimeEventParentRefsContain(t, rollbackQueueEvent, currentRollbackCarrierEvent.EventID)
	if failedResolveEvent.RootCauseID != rollbackQueueEvent.RootCauseID || failedResolveEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("failed action resolve lineage %+v does not match rollback queue event %+v", failedResolveEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, failedResolveEvent, rollbackQueueEvent.EventID)

	seenRetryCreateEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.created", Limit: 50})
	seenRetrySourceUpdates := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50})

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}

	retrySourceUpdateEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.updated", EntityID: sourceQueueID, Limit: 50}, seenRetrySourceUpdates)
	retryCreatedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.created", EntityID: retryActionID, Limit: 50}, seenRetryCreateEvents)
	if retrySourceUpdateEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retrySourceUpdateEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry source queue lineage %+v does not match rollback queue event %+v", retrySourceUpdateEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retrySourceUpdateEvent, rollbackQueueEvent.EventID)
	assertRuntimeEventParentRefsContain(t, retrySourceUpdateEvent, failedResolveEvent.EventID)
	if retryCreatedEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retryCreatedEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry action created lineage %+v does not match rollback queue event %+v", retryCreatedEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retryCreatedEvent, retrySourceUpdateEvent.EventID)

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Retry the rebase with preserved lineage.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	seenRetryResolveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: retryActionID, Limit: 50})
	seenSourceResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.resolved", EntityID: sourceQueueID, Limit: 50})

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Retry completed with canonical rollback lineage.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, retryResolveRaw); rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}

	sourceResolvedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "operator_queue.resolved", EntityID: sourceQueueID, Limit: 50}, seenSourceResolvedEvents)
	retryResolvedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{WorkspaceID: workspaceID, EventType: "action.resolved", EntityID: retryActionID, Limit: 50}, seenRetryResolveEvents)
	if sourceResolvedEvent.RootCauseID != rollbackQueueEvent.RootCauseID || sourceResolvedEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("completed source queue lineage %+v does not match rollback queue event %+v", sourceResolvedEvent, rollbackQueueEvent)
	}
	if retryResolvedEvent.RootCauseID != rollbackQueueEvent.RootCauseID || retryResolvedEvent.ProvenanceGroupID != rollbackQueueEvent.ProvenanceGroupID {
		t.Fatalf("retry action resolved lineage %+v does not match rollback queue event %+v", retryResolvedEvent, rollbackQueueEvent)
	}
	assertRuntimeEventParentRefsContain(t, retryResolvedEvent, sourceResolvedEvent.EventID)
}

func TestWorkspaceExecutionStepWriteFailedVerifyRejectsSecondRetryPromotionAfterRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-blackbox-retry-single-authority"
		taskID      = "task-execution-step-blackbox-retry-single-authority"
		agentID     = "agent-execution-step-blackbox-retry-single-authority"
		repairID    = "tens-repair-execution-step-blackbox-retry-single-authority"
		runID       = "run-execution-step-blackbox-retry-single-authority"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed and reopened the retry path",
		Summary:     "only one retry action should win after rollback reopens the same source queue",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	actions, err := store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("ListHumanActions before retry create: %v", err)
	}
	if len(actions) != 1 || actions[0].ActionID != failedActionID {
		t.Fatalf("expected only the failed first attempt before retry promotion, got %+v", actions)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("first retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected first retry actionCreate response %+v", retryCreateResp)
	}
	if got, _ := retryCreateResp["source_queue_id"].(string); got != sourceQueueID {
		t.Fatalf("first retry actionCreate source_queue_id = %q, want %q", got, sourceQueueID)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	actions, err = store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("ListHumanActions after first retry create: %v", err)
	}
	if len(actions) != 2 {
		t.Fatalf("expected exactly two human actions after first retry create, got %+v", actions)
	}

	if _, rpcErr := h.actionCreate(ctx, retryCreateRaw); rpcErr == nil {
		t.Fatal("expected second retry actionCreate to fail once the reopened source queue is relinked")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "already linked to action") {
		t.Fatalf("expected duplicate retry promotion to fail closed, got %+v", rpcErr)
	}

	actions, err = store.ListHumanActions(ctx, workspaceID, "")
	if err != nil {
		t.Fatalf("ListHumanActions after rejected second retry create: %v", err)
	}
	if len(actions) != 2 {
		t.Fatalf("rejected second retry create should not materialize a third action, got %+v", actions)
	}

	retryLinkedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after rejected second retry create: %v", sourceQueueID, err)
	}
	retryLinkedPayload, err := actionCreateDecodeQueuePayload(retryLinkedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload after rejected second retry create: %v", err)
	}
	if retryLinkedPayload.ActionID != retryActionID {
		t.Fatalf("source queue should stay linked to the winning retry action %q, got %+v", retryActionID, retryLinkedPayload)
	}
	if retryLinkedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("source queue should preserve failed-attempt lineage, got %+v", retryLinkedPayload)
	}
	if retryLinkedPayload.RollbackReason != "execution_verifier_failed" {
		t.Fatalf("rollback_reason = %q, want execution_verifier_failed", retryLinkedPayload.RollbackReason)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected happy rollback + retry promotion path to avoid rollback-failure recovery queue")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Winning retry promotion starts after duplicate create is rejected.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Winning retry landed cleanly after duplicate promotion was rejected.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, retryResolveRaw); rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}

	completedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s) after winning retry completion: %v", sourceQueueID, err)
	}
	if completedSourceQueue.Status != "RESOLVED" {
		t.Fatalf("completed source queue status = %q, want RESOLVED", completedSourceQueue.Status)
	}
	if completedSourceQueue.Resolution != "linked_action_completed:"+retryActionID {
		t.Fatalf("completed source queue resolution = %q, want linked_action_completed:%s", completedSourceQueue.Resolution, retryActionID)
	}
	completedPayload, err := actionCreateDecodeQueuePayload(completedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode completed source queue payload: %v", err)
	}
	if completedPayload.ActionID != retryActionID {
		t.Fatalf("completed payload action_id = %q, want %q", completedPayload.ActionID, retryActionID)
	}
	if completedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("completed payload should preserve failed-attempt lineage, got %+v", completedPayload)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifySuppressesRollbackFailureWhenConcurrentWinnerCompletesAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-concurrent-winner-complete"
		taskID      = "task-execution-step-concurrent-winner-complete"
		agentID     = "agent-execution-step-concurrent-winner-complete"
		repairID    = "tens-repair-execution-step-concurrent-winner-complete"
		runID       = "run-execution-step-concurrent-winner-complete"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusCompleted,
			Comment:    "Concurrent winner completed before verifier rollback applied.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent winner actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner actionResolve rpc error: %+v", rpcErr)
		}
	}

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier arrived after concurrent completion",
		Summary:     "late failed verify must not manufacture rollback recovery once a winner already completed the action",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner resolve hook: %v", hookErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite step payload type %T", stepPayload["step"])
	}
	if strings.ToUpper(strings.TrimSpace(stepRecord.Status)) != "FAILED" {
		t.Fatalf("step status = %q, want FAILED record preserved", stepRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusCompleted {
		t.Fatalf("action status = %q, want %q after concurrent winner", action.Status, humanActionStatusCompleted)
	}
	if action.ResolvedBy != "reviewer-a" {
		t.Fatalf("action resolved_by = %q, want reviewer-a", action.ResolvedBy)
	}
	if strings.Contains(action.ResolutionComment, "execution verifier") {
		t.Fatalf("action resolution comment should keep winner resolution, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.Status != "RESOLVED" {
		t.Fatalf("source queue status = %q, want RESOLVED", sourceQueue.Status)
	}
	if sourceQueue.Resolution != "linked_action_completed:"+actionID {
		t.Fatalf("source queue resolution = %q, want linked_action_completed:%s", sourceQueue.Resolution, actionID)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent winner completion")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("expected exactly one winning action.resolved event, before=%v after=%v", seenActionResolved, got)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifySuppressesRollbackFailureWhenConcurrentWinnerFailsAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-concurrent-winner-failed"
		taskID      = "task-execution-step-concurrent-winner-failed"
		agentID     = "agent-execution-step-concurrent-winner-failed"
		repairID    = "tens-repair-execution-step-concurrent-winner-failed"
		runID       = "run-execution-step-concurrent-winner-failed"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		resolveRaw, err := json.Marshal(actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "Concurrent winner failed before verifier rollback applied.",
			ResolvedBy: "reviewer-a",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent failed winner actionResolve params: %w", err)
			return
		}
		if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent failed winner actionResolve rpc error: %+v", rpcErr)
		}
	}

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier step arrived after concurrent failure",
		Summary:     "late failed verify step must not manufacture rollback recovery once a winner already failed the action",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent failed winner resolve hook: %v", hookErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite step payload type %T", stepPayload["step"])
	}
	if strings.ToUpper(strings.TrimSpace(stepRecord.Status)) != "FAILED" {
		t.Fatalf("step status = %q, want FAILED record preserved", stepRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q after concurrent failed winner", action.Status, humanActionStatusFailed)
	}
	if action.ResolvedBy != "reviewer-a" {
		t.Fatalf("action resolved_by = %q, want reviewer-a", action.ResolvedBy)
	}
	if strings.Contains(action.ResolutionComment, "execution verifier") {
		t.Fatalf("action resolution comment should keep failed winner resolution, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent failed winner")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("expected exactly one winning action.resolved event, before=%v after=%v", seenActionResolved, got)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifySuppressesRollbackFailureWhenConcurrentWinnerEscalatesAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-concurrent-winner-escalate"
		taskID      = "task-execution-step-concurrent-winner-escalate"
		agentID     = "agent-execution-step-concurrent-winner-escalate"
		repairID    = "tens-repair-execution-step-concurrent-winner-escalate"
		runID       = "run-execution-step-concurrent-winner-escalate"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		_, hookErr = interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueueID, "lead-b", "reviewer-b", "Concurrent winner escalated before verifier rollback applied.")
	}

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier step arrived after concurrent handoff",
		Summary:     "late failed verify must not manufacture rollback recovery once a handoff winner already reassigned the action",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner escalate hook: %v", hookErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite step payload type %T", stepPayload["step"])
	}
	if strings.ToUpper(strings.TrimSpace(stepRecord.Status)) != "FAILED" {
		t.Fatalf("step status = %q, want FAILED record preserved", stepRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.AssignedTo != "reviewer-b" {
		t.Fatalf("action truth after concurrent handoff = %+v, want pending reviewer-b", action)
	}
	if strings.Contains(strings.ToLower(action.ResolutionComment), "execution verifier") {
		t.Fatalf("action should not gain verifier resolution comment after concurrent handoff, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.Status != "OPEN" || sourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("source queue after concurrent handoff = %+v, want OPEN reviewer-b", sourceQueue)
	}
	sourcePayload, err := actionCreateDecodeQueuePayload(sourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode source queue payload: %v", err)
	}
	if sourcePayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("source queue payload action_assigned_to = %q, want reviewer-b", sourcePayload.ActionAssignedTo)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent handoff winner")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("expected no action.resolved rows after concurrent handoff winner, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated)+1 {
		t.Fatalf("expected exactly one winning operator_queue.escalated row, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("expected exactly one winning linked action queue update row, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifyReusesCurrentStartedCarrierAfterConcurrentUpsertWinner(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-execution-step-concurrent-winner-upsert"
		taskID        = "task-execution-step-concurrent-winner-upsert"
		agentID       = "agent-execution-step-concurrent-winner-upsert"
		repairID      = "tens-repair-execution-step-concurrent-winner-upsert"
		runID         = "run-execution-step-concurrent-winner-upsert"
		winnerSummary = "winner started-carrier note should survive verifier failed step"
		winnerDetails = "winner workspace.ops.upsert should not force false rollback-failure recovery after verifier failed step"
		winnerDueAt   = "2099-09-02T00:00:00Z"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving manual edit): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil

		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          sourceQueueBefore.QueueID,
			QueueKey:         sourceQueueBefore.QueueKey,
			QueueType:        sourceQueueBefore.QueueType,
			Title:            sourceQueueBefore.Title,
			Summary:          winnerSummary,
			Details:          winnerDetails,
			AssignedTo:       sourceQueueBefore.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       sourceQueueBefore.SourceKind,
			SourceID:         sourceQueueBefore.SourceID,
			TaskID:           sourceQueueBefore.TaskID,
			AgentID:          sourceQueueBefore.AgentID,
			DueAt:            winnerDueAt,
			CurrentRevision:  sourceQueueBefore.Revision,
			CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent winner workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier step arrived after concurrent manual edit",
		Summary:     "late failed verify should still land on the current started carrier after a concurrent manual edit",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner workspaceOpsUpsert hook: %v", hookErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite step payload type %T", stepPayload["step"])
	}
	if strings.ToUpper(strings.TrimSpace(stepRecord.Status)) != "FAILED" {
		t.Fatalf("step status = %q, want FAILED record preserved", stepRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q after verifier failed step on current carrier", action.Status, humanActionStatusFailed)
	}
	if action.ResolvedBy != "system:execution_verifier" {
		t.Fatalf("action resolved_by = %q, want system:execution_verifier", action.ResolvedBy)
	}
	if !strings.Contains(strings.ToLower(action.ResolutionComment), "execution verifier late fail") {
		t.Fatalf("action resolution comment should reflect verifier late fail, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.Status != "OPEN" {
		t.Fatalf("source queue status = %q, want OPEN after verifier failed step", sourceQueue.Status)
	}
	if !strings.Contains(sourceQueue.Summary, winnerSummary) {
		t.Fatalf("source queue summary should retain winner-owned manual edit, got %q", sourceQueue.Summary)
	}
	if !strings.Contains(sourceQueue.Details, winnerDetails) {
		t.Fatalf("source queue details should retain winner-owned manual edit, got %q", sourceQueue.Details)
	}
	if !strings.Contains(sourceQueue.Details, "Rollback reason: execution_verifier_failed") {
		t.Fatalf("source queue details should record retry-needed failed state, got %q", sourceQueue.Details)
	}
	if sourceQueue.Urgency != "CRITICAL" || derefString(sourceQueue.DueAt) != winnerDueAt {
		t.Fatalf("source queue should preserve winner-owned urgency/due_at, got urgency=%q due_at=%q", sourceQueue.Urgency, derefString(sourceQueue.DueAt))
	}

	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueueBefore.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", actionQueueBefore.QueueID, err)
	}
	if actionQueue.Status != "RESOLVED" {
		t.Fatalf("action queue status = %q, want RESOLVED", actionQueue.Status)
	}
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo {
		t.Fatalf("action queue assigned_to = %q, want %q", actionQueue.AssignedTo, actionQueueBefore.AssignedTo)
	}
	if actionQueue.Summary != actionQueueBefore.Summary || actionQueue.Details != actionQueueBefore.Details {
		t.Fatalf("action queue should preserve pre-existing manual content on resolve, before=%+v after=%+v", actionQueueBefore, actionQueue)
	}
	if !strings.Contains(strings.ToLower(actionQueue.Resolution), "execution verifier late fail") {
		t.Fatalf("action queue resolution should reflect verifier late fail, got %q", actionQueue.Resolution)
	}
	if derefString(actionQueue.ResolvedBy) != "system:execution_verifier" {
		t.Fatalf("action queue resolved_by = %q, want system:execution_verifier", derefString(actionQueue.ResolvedBy))
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after verifier failed step rehydrated the current started carrier")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved)+1 {
		t.Fatalf("expected exactly one verifier action.resolved row, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+2 {
		t.Fatalf("expected winner manual edit plus verifier failure source updates, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("expected no source queue resolved rows after verifier failed step path, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("expected no action queue updated rows from source-queue manual winner, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("expected exactly one action queue resolved row from verifier failed step, before=%v after=%v", seenActionQueueResolved, got)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifySuppressesRollbackFailureWhenConcurrentWinnerPausesAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-concurrent-winner-pause"
		taskID      = "task-execution-step-concurrent-winner-pause"
		agentID     = "agent-execution-step-concurrent-winner-pause"
		repairID    = "tens-repair-execution-step-concurrent-winner-pause"
		runID       = "run-execution-step-concurrent-winner-pause"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	actionResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	actionPausedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenActionResolved := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter)
	seenActionPaused := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		pauseRaw, err := json.Marshal(actionPauseParams{
			ActionID: actionID,
			PausedBy: "reviewer-a",
			Comment:  "Concurrent winner paused before verifier rollback applied.",
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal concurrent winner actionPause params: %w", err)
			return
		}
		if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner actionPause rpc error: %+v", rpcErr)
		}
	}

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier step arrived after concurrent pause",
		Summary:     "late failed verify must not manufacture rollback recovery once a pause winner already rewound the action",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("concurrent winner pause hook: %v", hookErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected workspaceExecutionStepWrite step payload type %T", stepPayload["step"])
	}
	if strings.ToUpper(strings.TrimSpace(stepRecord.Status)) != "FAILED" {
		t.Fatalf("step status = %q, want FAILED record preserved", stepRecord.Status)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending || action.AssignedTo != "reviewer-a" {
		t.Fatalf("action truth after concurrent pause = %+v, want pending reviewer-a", action)
	}
	if strings.Contains(strings.ToLower(action.ResolutionComment), "execution verifier") {
		t.Fatalf("action should not gain verifier resolution comment after concurrent pause, got %q", action.ResolutionComment)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	}))
	if err == nil {
		t.Fatalf("expected no rollback-failure recovery queue after concurrent pause winner")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionResolvedFilter); len(got) != len(seenActionResolved) {
		t.Fatalf("expected no action.resolved rows after concurrent pause winner, before=%v after=%v", seenActionResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionPausedFilter); len(got) != len(seenActionPaused)+1 {
		t.Fatalf("expected exactly one winning action.paused row, before=%v after=%v", seenActionPaused, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("expected exactly one winning source queue update row, before=%v after=%v", seenSourceUpdated, got)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifySourceQueueOnlyLinkageFailsClosedAfterRetryStarts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-source-queue-ambiguous-retry"
		taskID      = "task-execution-step-source-queue-ambiguous-retry"
		agentID     = "agent-execution-step-source-queue-ambiguous-retry"
		repairID    = "tens-repair-execution-step-source-queue-ambiguous-retry"
		runID       = "run-execution-step-source-queue-ambiguous-retry"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	initialFailRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed and reopened the retry path",
		Summary:     "initial rollback opens the same source queue for a retry",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal initial failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, initialFailRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite initial rollback rpc error: %+v", rpcErr)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, _ := retryCreateResp["action_id"].(string)
	if retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry action id %q after rollback from %q", retryActionID, failedActionID)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Start the retry before an old source-queue-only verifier signal repeats.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	duplicateFailRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Old source-queue verifier signal repeated after retry started",
		Summary:     "queue-only verifier linkage must not roll back the new retry once the old attempt is already in failure lineage",
		Status:      "FAILED",
		SortOrder:   2,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal duplicate failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, duplicateFailRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite duplicate rollback rpc error: %+v", rpcErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected duplicate step response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected duplicate step payload type %T", stepPayload["step"])
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "execution_step",
		SourceQueueID: sourceQueueID,
	})
	if recoveryQueue.Status != "OPEN" || recoveryQueue.TaskID != taskID {
		t.Fatalf("unexpected recovery queue %+v", recoveryQueue)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if payload.FailureScope != "execution_step" || payload.FailureTrigger != "execution_verifier_failed" {
		t.Fatalf("unexpected recovery payload %+v", payload)
	}
	if payload.RunID != runID || payload.StepID != stepRecord.StepID || payload.SourceQueueID != sourceQueueID {
		t.Fatalf("unexpected recovery linkage %+v", payload)
	}
	if !strings.Contains(payload.FailureMessage, "action_id is required") {
		t.Fatalf("expected ambiguous source-queue linkage guidance in failure message, got %+v", payload)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifyRepairOnlyLinkageFailsClosedAfterRetryStarts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-repair-only-ambiguous-retry"
		taskID      = "task-execution-step-repair-only-ambiguous-retry"
		agentID     = "agent-execution-step-repair-only-ambiguous-retry"
		repairID    = "tens-repair-execution-step-repair-only-ambiguous-retry"
		runID       = "run-execution-step-repair-only-ambiguous-retry"
	)

	failedActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	initialFailRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Repair-only verifier signal failed and reopened the retry path",
		Summary:     "initial rollback should still work from repair_tension_id-only linkage",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"repair_tension_id": repairID,
		},
	})
	if err != nil {
		t.Fatalf("marshal initial repair-only failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, initialFailRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite initial repair-only rollback rpc error: %+v", rpcErr)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, _ := retryCreateResp["action_id"].(string)
	if retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry action id %q after rollback from %q", retryActionID, failedActionID)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Start the retry before an old repair-only verifier signal repeats.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	duplicateFailRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Old repair-only verifier signal repeated after retry started",
		Summary:     "repair-only verifier linkage must not roll back the new retry once the old attempt is already in failure lineage",
		Status:      "FAILED",
		SortOrder:   2,
		Verification: map[string]any{
			"repair_tension_id": repairID,
		},
	})
	if err != nil {
		t.Fatalf("marshal duplicate repair-only failed verify step params: %v", err)
	}
	stepAny, rpcErr := h.workspaceExecutionStepWrite(ctx, duplicateFailRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite duplicate repair-only rollback rpc error: %+v", rpcErr)
	}
	stepPayload, ok := stepAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected duplicate repair-only step response type %T", stepAny)
	}
	stepRecord, ok := stepPayload["step"].(sqlite.ExecutionStepRecord)
	if !ok {
		t.Fatalf("unexpected duplicate repair-only step payload type %T", stepPayload["step"])
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "execution_step",
		RepairTensionID: repairID,
	})
	if recoveryQueue.Status != "OPEN" || recoveryQueue.TaskID != taskID {
		t.Fatalf("unexpected recovery queue %+v", recoveryQueue)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if payload.FailureScope != "execution_step" || payload.FailureTrigger != "execution_verifier_failed" {
		t.Fatalf("unexpected recovery payload %+v", payload)
	}
	if payload.RunID != runID || payload.StepID != stepRecord.StepID || payload.RepairTensionID != repairID {
		t.Fatalf("unexpected recovery linkage %+v", payload)
	}
	if !strings.Contains(payload.FailureMessage, "action_id is required") {
		t.Fatalf("expected ambiguous repair-only linkage guidance in failure message, got %+v", payload)
	}
}

func TestWorkspaceExecutionStepWriteFailedVerifyRollsBackLinkedRebaseFollowupByRepairTensionID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-rebase-repair-link"
		taskID      = "task-execution-step-rebase-repair-link"
		agentID     = "agent-execution-step-rebase-repair-link"
		repairID    = "tens-repair-execution-step-rebase-repair-link"
		runID       = "run-execution-step-rebase-repair-link"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Repair verifier failed",
		Summary:     "repair verifier produced a bounded late fail",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"repair_tension_id": repairID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestWorkspaceExecutionStepWriteFailedVerifyWithoutRebaseLinkageDoesNotRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-no-rebase-link"
		taskID      = "task-execution-step-no-rebase-link"
		agentID     = "agent-execution-step-no-rebase-link"
		repairID    = "tens-repair-execution-step-no-rebase-link"
		runID       = "run-execution-step-no-rebase-link"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:        runID,
		WorkspaceID:  workspaceID,
		Phase:        "VERIFY",
		Title:        "Verifier gate failed without linkage",
		Summary:      "verification mismatch without explicit rebase linkage",
		Status:       "FAILED",
		SortOrder:    1,
		Verification: map[string]any{"note": "no linked rebase metadata"},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestWorkspaceExecutionStepWriteFailedExecuteDoesNotRollbackLinkedRebaseFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-execute-no-rollback"
		taskID      = "task-execution-step-execute-no-rollback"
		agentID     = "agent-execution-step-execute-no-rollback"
		repairID    = "tens-repair-execution-step-execute-no-rollback"
		runID       = "run-execution-step-execute-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "EXECUTE",
		Title:       "Execution step failed before verify",
		Summary:     "execution step failed but verifier contract should not auto-rollback here",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed execute step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestWorkspaceExecutionStepWriteFailedVerifyDoesNotRollbackPausedLinkedRebaseFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-paused-no-rollback"
		taskID      = "task-execution-step-paused-no-rollback"
		agentID     = "agent-execution-step-paused-no-rollback"
		repairID    = "tens-repair-execution-step-paused-no-rollback"
		runID       = "run-execution-step-paused-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Pause before verifier failure lands.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed after pause",
		Summary:     "paused rebase should not auto-rollback again",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestWorkspaceExecutionStepWriteFailedVerifyDoesNotRollbackUnstartedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-unstarted-no-rollback"
		taskID      = "task-execution-step-unstarted-no-rollback"
		agentID     = "agent-execution-step-unstarted-no-rollback"
		repairID    = "tens-repair-execution-step-unstarted-no-rollback"
		runID       = "run-execution-step-unstarted-no-rollback"
	)

	actionID, sourceQueueID := createPendingRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed before operator start",
		Summary:     "unstarted rebase should not auto-rollback on failed verify",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestWorkspaceExecutionStepWriteFailedVerifyDoesNotRollbackResolvedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-resolved-no-rollback"
		taskID      = "task-execution-step-resolved-no-rollback"
		agentID     = "agent-execution-step-resolved-no-rollback"
		repairID    = "tens-repair-execution-step-resolved-no-rollback"
		runID       = "run-execution-step-resolved-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Resolved before stale failed verify lands.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed after completion",
		Summary:     "resolved rebase should not auto-rollback on stale failed verify",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
}

func TestWorkspaceExecutionStepWriteFailedVerifyWithMismatchedRepairLinkDoesNotRollback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-execution-step-mismatch-no-rollback"
		taskID      = "task-execution-step-mismatch-no-rollback"
		agentID     = "agent-execution-step-mismatch-no-rollback"
		repairID    = "tens-repair-execution-step-mismatch-no-rollback"
		runID       = "run-execution-step-mismatch-no-rollback"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier failed with wrong repair link",
		Summary:     "mismatched repair_tension_id should fail closed",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"action_id":         actionID,
			"repair_tension_id": "tens-repair-some-other-branch",
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func createStartedRebaseFollowupActionForControlPlaneTest(t *testing.T, ctx context.Context, store *sqlite.Store, h *Handler, workspaceID, taskID, agentID, repairID string) (string, string) {
	t.Helper()

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	return createStartedRebaseFollowupActionOnExistingWorkspaceForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
}

func createStartedRebaseFollowupActionOnExistingWorkspaceForControlPlaneTest(t *testing.T, ctx context.Context, store *sqlite.Store, h *Handler, workspaceID, taskID, agentID, repairID string) (string, string) {
	t.Helper()

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-" + repairID,
		"fork_tension_id":     "fork-" + repairID,
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "tension_rebase_followup:" + repairID,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for " + repairID,
		Details:           "Repair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Start bounded rebase before control-plane late fail.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}
	return actionID, sourceQueue.QueueID
}

func createPendingRebaseFollowupActionForControlPlaneTest(t *testing.T, ctx context.Context, store *sqlite.Store, h *Handler, workspaceID, taskID, agentID, repairID string) (string, string) {
	t.Helper()

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-" + repairID,
		"fork_tension_id":     "fork-" + repairID,
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "tension_rebase_followup:" + repairID,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for " + repairID,
		Details:           "Repair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	return actionID, sourceQueue.QueueID
}

func corruptActionQueueSourceLinkForControlPlaneTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actionID, sourceQueueID string) {
	t.Helper()

	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue): %v", err)
	}
	payload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	payload.SourceQueueID = "opq-missing-rebase-source-" + sourceQueueID
	payload.SourceQueueKey = ""
	payload.Normalize()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal corrupted action queue payload: %v", err)
	}
	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:          workspaceID,
		QueueKey:             actionQueue.QueueKey,
		QueueType:            actionQueue.QueueType,
		Title:                actionQueue.Title,
		Summary:              actionQueue.Summary,
		Details:              actionQueue.Details,
		PayloadJSON:          string(payloadJSON),
		AssignedTo:           actionQueue.AssignedTo,
		Urgency:              actionQueue.Urgency,
		SourceKind:           actionQueue.SourceKind,
		SourceID:             actionQueue.SourceID,
		TaskID:               actionQueue.TaskID,
		SessionID:            actionQueue.SessionID,
		AgentID:              actionQueue.AgentID,
		KeepSessionActive:    actionQueue.KeepSessionActive,
		RequireCurrentStatus: "OPEN",
	}); err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent(corrupt action queue): %v", err)
	}
}

func corruptRebaseSourceQueuePayloadForControlPlaneTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, sourceQueueID string) {
	t.Helper()

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue): %v", err)
	}
	payloadJSON, err := json.Marshal(map[string]any{"kind": "manual"})
	if err != nil {
		t.Fatalf("marshal corrupted source queue payload: %v", err)
	}
	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:                 sourceQueue.QueueID,
		WorkspaceID:             workspaceID,
		QueueKey:                sourceQueue.QueueKey,
		QueueType:               sourceQueue.QueueType,
		Title:                   sourceQueue.Title,
		Summary:                 sourceQueue.Summary,
		Details:                 sourceQueue.Details,
		PayloadJSON:             string(payloadJSON),
		AssignedTo:              sourceQueue.AssignedTo,
		Urgency:                 sourceQueue.Urgency,
		SourceKind:              sourceQueue.SourceKind,
		SourceID:                sourceQueue.SourceID,
		TaskID:                  sourceQueue.TaskID,
		SessionID:               sourceQueue.SessionID,
		AgentID:                 sourceQueue.AgentID,
		KeepSessionActive:       sourceQueue.KeepSessionActive,
		DueAt:                   derefString(sourceQueue.DueAt),
		RequireCurrentUpdatedAt: sourceQueue.UpdatedAt,
	}); err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent(corrupt source queue): %v", err)
	}
}

func mustRebaseRollbackFailureQueueForTest(t *testing.T, ctx context.Context, store *sqlite.Store, input rebaseRollbackFailureInput) sqlite.OperatorQueueRecord {
	t.Helper()

	queueKey := rebaseRollbackFailureQueueKey(input)
	if queueKey == "" {
		t.Fatalf("rebaseRollbackFailureQueueKey returned empty for %+v", input)
	}
	queue, err := store.GetOperatorQueueItem(ctx, input.WorkspaceID, "", queueKey)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", queueKey, err)
	}
	return queue
}

func createRebaseRollbackFailureQueueForTest(t *testing.T, ctx context.Context, store *sqlite.Store, input rebaseRollbackFailureInput) sqlite.OperatorQueueRecord {
	t.Helper()

	payload := rebaseRollbackFailurePayload(input)
	sourceID := firstNonEmpty(input.SourceID, input.RunID, input.StepID, input.EntityID, input.SourceQueueID, input.ActionID, input.RepairTensionID)
	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       strings.TrimSpace(input.WorkspaceID),
		QueueKey:          rebaseRollbackFailureQueueKey(input),
		QueueType:         "FOLLOW_UP",
		Title:             "Repair automatic rebase rollback failure",
		Summary:           rebaseRollbackFailureSummary(input),
		Details:           rebaseRollbackFailureDetails(input),
		PayloadJSON:       string(mustJSON(payload)),
		Urgency:           "HIGH",
		SourceKind:        strings.TrimSpace(input.FailureScope),
		SourceID:          sourceID,
		TaskID:            strings.TrimSpace(input.TaskID),
		SessionID:         strings.TrimSpace(input.SessionID),
		AgentID:           strings.TrimSpace(input.AgentID),
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent(create rollback failure queue): %v", err)
	}
	return queue
}

func refreshRebaseRollbackFailureQueueForTest(t *testing.T, ctx context.Context, store *sqlite.Store, existing sqlite.OperatorQueueRecord, input rebaseRollbackFailureInput) sqlite.OperatorQueueRecord {
	t.Helper()

	payload := rebaseRollbackFailurePayloadWithExistingFollowupState(existing, rebaseRollbackFailurePayload(input))
	payloadJSON := string(mustJSON(payload))
	if strings.EqualFold(strings.TrimSpace(existing.Status), "RESOLVED") {
		resolvedBy := "operator:test-refresh"
		if existing.ResolvedBy != nil && strings.TrimSpace(*existing.ResolvedBy) != "" {
			resolvedBy = strings.TrimSpace(*existing.ResolvedBy)
		}
		resolution := strings.TrimSpace(existing.Resolution)
		if resolution == "" {
			resolution = "followup_action_resolved"
		}
		queue, _, err := store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
			WorkspaceID:             existing.WorkspaceID,
			QueueID:                 existing.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              resolvedBy,
			Resolution:              resolution,
			Summary:                 rebaseRollbackFailureSummary(input),
			Details:                 rebaseRollbackFailureDetails(input),
			PayloadJSON:             payloadJSON,
			RequireCurrentStatus:    strings.TrimSpace(existing.Status),
			RequireCurrentRevision:  existing.Revision,
			RequireCurrentUpdatedAt: revisionFallbackUpdatedAt(existing),
		})
		if err != nil {
			t.Fatalf("ResolveOperatorQueueItemWithEvent(refresh rollback failure queue): %v", err)
		}
		return queue
	}

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             existing.WorkspaceID,
		QueueKey:                existing.QueueKey,
		QueueType:               existing.QueueType,
		Title:                   existing.Title,
		Summary:                 rebaseRollbackFailureSummary(input),
		Details:                 rebaseRollbackFailureDetails(input),
		PayloadJSON:             payloadJSON,
		AssignedTo:              existing.AssignedTo,
		Urgency:                 existing.Urgency,
		SourceKind:              existing.SourceKind,
		SourceID:                existing.SourceID,
		TaskID:                  existing.TaskID,
		SessionID:               existing.SessionID,
		AgentID:                 existing.AgentID,
		KeepSessionActive:       existing.KeepSessionActive,
		RequireCurrentStatus:    strings.TrimSpace(existing.Status),
		RequireCurrentRevision:  existing.Revision,
		RequireCurrentUpdatedAt: revisionFallbackUpdatedAt(existing),
	})
	if err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent(refresh rollback failure queue): %v", err)
	}
	return queue
}

func overwriteRebaseRollbackFailureQueuePayloadForTest(t *testing.T, ctx context.Context, store *sqlite.Store, existing sqlite.OperatorQueueRecord, input rebaseRollbackFailureInput, payload model.RebaseRollbackFailurePayload) sqlite.OperatorQueueRecord {
	t.Helper()

	payload.Normalize()
	payloadJSON := string(mustJSON(payload))
	if strings.EqualFold(strings.TrimSpace(existing.Status), "RESOLVED") {
		resolvedBy := "operator:test-refresh"
		if existing.ResolvedBy != nil && strings.TrimSpace(*existing.ResolvedBy) != "" {
			resolvedBy = strings.TrimSpace(*existing.ResolvedBy)
		}
		resolution := strings.TrimSpace(existing.Resolution)
		if resolution == "" {
			resolution = "followup_action_resolved"
		}
		queue, _, err := store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
			WorkspaceID:             existing.WorkspaceID,
			QueueID:                 existing.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              resolvedBy,
			Resolution:              resolution,
			Summary:                 rebaseRollbackFailureSummary(input),
			Details:                 rebaseRollbackFailureDetails(input),
			PayloadJSON:             payloadJSON,
			RequireCurrentStatus:    strings.TrimSpace(existing.Status),
			RequireCurrentRevision:  existing.Revision,
			RequireCurrentUpdatedAt: revisionFallbackUpdatedAt(existing),
		})
		if err != nil {
			t.Fatalf("ResolveOperatorQueueItemWithEvent(overwrite rollback failure queue payload): %v", err)
		}
		return queue
	}

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             existing.WorkspaceID,
		QueueKey:                existing.QueueKey,
		QueueType:               existing.QueueType,
		Title:                   existing.Title,
		Summary:                 rebaseRollbackFailureSummary(input),
		Details:                 rebaseRollbackFailureDetails(input),
		PayloadJSON:             payloadJSON,
		AssignedTo:              existing.AssignedTo,
		Urgency:                 existing.Urgency,
		SourceKind:              existing.SourceKind,
		SourceID:                existing.SourceID,
		TaskID:                  existing.TaskID,
		SessionID:               existing.SessionID,
		AgentID:                 existing.AgentID,
		KeepSessionActive:       existing.KeepSessionActive,
		RequireCurrentStatus:    strings.TrimSpace(existing.Status),
		RequireCurrentRevision:  existing.Revision,
		RequireCurrentUpdatedAt: revisionFallbackUpdatedAt(existing),
	})
	if err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent(overwrite rollback failure queue payload): %v", err)
	}
	return queue
}

func revisionFallbackUpdatedAt(record sqlite.OperatorQueueRecord) string {
	if record.Revision > 0 {
		return ""
	}
	return strings.TrimSpace(record.UpdatedAt)
}

func createExecutionRunForControlPlaneTest(t *testing.T, ctx context.Context, h *Handler, workspaceID, taskID, agentID, runID string) {
	t.Helper()

	runRaw, err := json.Marshal(workspaceExecutionRunWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Title:       "Control-plane verifier rollback run",
		Summary:     "Execution run for explicit verifier late-fail rollback",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("marshal workspaceExecutionRunWrite params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionRunWrite(ctx, runRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionRunWrite rpc error: %+v", rpcErr)
	}
}

func TestWorkspaceClaimWriteMirrorsNewPersistedRowForRepeatedClaimID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-write-repeat"
		claimID     = "claim-repeat"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Repeated Claim Mirror",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-repeat",
		OwnerUserID: "developer",
		DisplayName: "Claim Repeat Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	claimFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}
	firstRaw, err := json.Marshal(workspaceClaimWriteParams{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Runtime journal is canonical",
		Body:        "Use runtime events as the source of truth.",
		Summary:     "First claim state",
		Confidence:  0.55,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-claim-repeat",
	})
	if err != nil {
		t.Fatalf("marshal first claim write params: %v", err)
	}
	firstAny, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "system", "tests"), firstRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimWrite first rpc error: %+v", rpcErr)
	}
	firstPayload, ok := firstAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first claim write result type %T", firstAny)
	}
	firstClaim, ok := firstPayload["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok {
		t.Fatalf("unexpected first claim payload type %T", firstPayload["claim"])
	}
	firstLive := nextEventOfType(t, ch, "workspace.claim.written")
	firstPersisted := mustRuntimeEvent(t, ctx, store, claimFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstLive, firstPersisted, "workspace.claim.written")

	seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, claimFilter)
	secondRaw, err := json.Marshal(workspaceClaimWriteParams{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Runtime journal is canonical",
		Body:        "Prefer canonical runtime events over stale caches.",
		Summary:     "Second claim state",
		Confidence:  0.91,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-claim-repeat",
	})
	if err != nil {
		t.Fatalf("marshal second claim write params: %v", err)
	}
	secondAny, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "system", "tests"), secondRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimWrite second rpc error: %+v", rpcErr)
	}
	secondPayload, ok := secondAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second claim write result type %T", secondAny)
	}
	secondClaim, ok := secondPayload["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok {
		t.Fatalf("unexpected second claim payload type %T", secondPayload["claim"])
	}
	if secondClaim.ClaimID != firstClaim.ClaimID {
		t.Fatalf("expected repeated claim write to preserve claim_id %q, got %+v", firstClaim.ClaimID, secondClaim)
	}
	secondLive := nextEventOfType(t, ch, "workspace.claim.written")
	secondPersisted := mustNewRuntimeEvent(t, ctx, store, claimFilter, seenClaimEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondPersisted, "workspace.claim.written")
	if secondPersisted.EventID == firstPersisted.EventID || secondPersisted.IngestSeq <= firstPersisted.IngestSeq {
		t.Fatalf("expected second claim runtime event to advance beyond first, got first=%+v second=%+v", firstPersisted, secondPersisted)
	}
	var liveClaim sqlite.KnowledgeClaimRecord
	if err := json.Unmarshal([]byte(secondLive.PayloadJSON), &liveClaim); err != nil {
		t.Fatalf("decode second claim live payload: %v", err)
	}
	if liveClaim.ClaimID != claimID || liveClaim.Summary != "Second claim state" || liveClaim.Body != "Prefer canonical runtime events over stale caches." || liveClaim.Confidence != 0.91 {
		t.Fatalf("unexpected second claim live payload %+v", liveClaim)
	}
}

func TestWorkspaceClaimWriteSupportsAntiProcedureType(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-write-anti-procedure"
		agentID     = "agent-claim-write-anti-procedure"
		claimID     = "claim-anti-procedure"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Anti Procedure Claim Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Anti Procedure Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceClaimWriteParams{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "ANTI_PROCEDURE",
		Subject:     "Rollback bypass stays forbidden",
		Body:        "Do not bypass live doctor or rollback-gate checks during degraded telemetry.",
		Summary:     "Surface anti-procedure as first-class claim type.",
		Confidence:  0.73,
		SourceKind:  "manual",
		SourceID:    "dashboard",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("marshal anti procedure claim params: %v", err)
	}
	respAny, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimWrite rpc error: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected claim write response type %T", respAny)
	}
	claim, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok {
		t.Fatalf("unexpected claim payload type %T", resp["claim"])
	}
	if claim.ClaimType != "ANTI_PROCEDURE" || claim.ClaimID != claimID {
		t.Fatalf("expected anti procedure claim write response, got %+v", claim)
	}

	live := nextEventOfType(t, ch, "workspace.claim.written")
	var liveClaim sqlite.KnowledgeClaimRecord
	if err := json.Unmarshal([]byte(live.PayloadJSON), &liveClaim); err != nil {
		t.Fatalf("decode anti procedure claim live payload: %v", err)
	}
	if liveClaim.ClaimType != "ANTI_PROCEDURE" || liveClaim.ClaimID != claimID {
		t.Fatalf("expected anti procedure live payload, got %+v", liveClaim)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		ClaimType:   "ANTI_PROCEDURE",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list anti procedure claims: %v", err)
	}
	if len(claims) != 1 || claims[0].ClaimID != claimID || claims[0].ClaimType != "ANTI_PROCEDURE" {
		t.Fatalf("expected anti procedure claim filter to keep canonical type, got %+v", claims)
	}
}

func TestWorkspaceClaimWritePublishesRefChangeMemoryInvalidationEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-claim-write-invalidation-live"
		agentID     = "agent-claim-write-invalidation"
		claimID     = "claim-write-invalidation"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Write Invalidation Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	initialClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Invalidation should follow claim writes",
		Body:        "Original claim body.",
		Summary:     "Original claim state",
		Confidence:  0.61,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-claim-write-invalidation-live",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:claim-write",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claimID, VersionToken: initialClaim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed claim residency: %v", err)
	}

	seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceClaimWriteParams{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Invalidation should follow claim writes",
		Body:        "Updated claim body should trigger invalidation.",
		Summary:     "Updated claim state",
		Confidence:  0.84,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("marshal claim write params: %v", err)
	}
	result, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimWrite rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected claim write response type %T", result)
	}
	claimResp, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok || claimResp.ClaimID != claimID {
		t.Fatalf("unexpected claim write response %+v", resp)
	}

	claimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenClaimEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: claimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
	)
	if len(ordered) != 2 || !runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) {
		t.Fatalf("expected claim write and invalidation live mirrors to follow persisted chronology, got %+v", ordered)
	}
	var invalidationLive EventMessage
	for i, expectation := range ordered {
		if expectation.Type == "memory.invalidation_enqueued" {
			invalidationLive = liveEvents[i]
			break
		}
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "knowledge_claim.written" || payload["ref_kind"] != "knowledge_claim" || payload["ref_id"] != claimID {
		t.Fatalf("expected claim write invalidation payload, got %+v", payload)
	}
}

func TestWorkspaceClaimLifecycleRPCPublishesAndResolvesReviewQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-claim-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-rpc",
		OwnerUserID: "developer",
		DisplayName: "Claim RPC Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Runtime journal is canonical",
		Body:        "Use runtime events as the canonical source of truth.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-claim-rpc",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	reviewRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "needs operator confirmation",
		DueAt:       "2026-03-23T10:00:00Z",
		AssignedTo:  "reviewer-rpc",
	})
	if err != nil {
		t.Fatalf("marshal review params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "system", "tests"), reviewRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimReview rpc error: %+v", rpcErr)
	}
	reviewPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       1,
	})
	reviewQueuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		Limit:       1,
	})
	assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(
		t,
		ch,
		reviewPersisted,
		"workspace.claim.review_requested",
		reviewQueuePersisted,
		"workspace.ops.updated",
	)

	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues: %v", err)
	}
	if len(queues) != 1 || queues[0].Status != "OPEN" || queues[0].AssignedTo != "reviewer-rpc" {
		t.Fatalf("unexpected review queue items %+v", queues)
	}

	confirmRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "confirmed from live runtime",
	})
	if err != nil {
		t.Fatalf("marshal confirm params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimConfirm(testAuthContext(workspaceID, "system", "tests"), confirmRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimConfirm rpc error: %+v", rpcErr)
	}
	confirmPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.confirmed",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       1,
	})
	confirmQueuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		Limit:       1,
	})
	assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(
		t,
		ch,
		confirmPersisted,
		"workspace.claim.confirmed",
		confirmQueuePersisted,
		"workspace.ops.resolved",
	)

	updated, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("get confirmed claim: %v", err)
	}
	if updated.Status != "CONFIRMED" {
		t.Fatalf("expected confirmed claim, got %+v", updated)
	}
	queues, err = store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues after confirm: %v", err)
	}
	if len(queues) != 1 || queues[0].Status != "RESOLVED" {
		t.Fatalf("expected resolved follow-up queue, got %+v", queues)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list claim runtime events: %v", err)
	}
	seen := map[string]bool{}
	for _, event := range events {
		seen[event.EventType] = true
	}
	if !seen["knowledge_claim.review_requested"] || !seen["knowledge_claim.confirmed"] {
		t.Fatalf("expected claim lifecycle runtime events, got %+v", events)
	}
}

func TestWorkspaceClaimLifecyclePublishesReviewQueueMirrorsInChronologicalOrder(t *testing.T) {
	t.Run("review_requested", func(t *testing.T) {
		store := newServerTestStore(t)
		h := NewHandler(store)
		const (
			workspaceID = "ws-claim-review-queue-live"
			agentID     = "agent-claim-review-queue"
			claimID     = "claim-review-queue-live"
		)
		ctx := testAuthContext(workspaceID, "agent", agentID)
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "Claim Review Queue Live",
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent: %v", err)
		}
		claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
			ClaimID:     claimID,
			WorkspaceID: workspaceID,
			ClaimType:   "FACT",
			Subject:     "Review queue live parity",
			Body:        "Review should mirror queue open/update runtime rows.",
			Summary:     "Review queue baseline",
			Confidence:  0.67,
			SourceKind:  "manual",
			SourceID:    "developer",
			AgentID:     agentID,
		})
		if err != nil {
			t.Fatalf("seed claim: %v", err)
		}
		if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
			ReportID:    "memres-claim-review-queue-live",
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Replicas: []sqlite.MemoryReplicaStateInput{
				{
					ResidencyTier:  "P2",
					ReplicaKind:    "memory_node",
					CoherenceClass: "A",
					State:          "CURRENT",
					CacheKey:       "packet:claim-review-queue",
					VersionGuards: []sqlite.MemoryResidencyVersionGuard{
						{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
					},
				},
			},
		}); err != nil {
			t.Fatalf("seed claim residency: %v", err)
		}

		seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "knowledge_claim.review_requested",
			EntityType:  "knowledge_claim",
			EntityID:    claimID,
			Limit:       10,
		})
		seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "memory.invalidation_enqueued",
			EntityType:  "memory_invalidation",
			Limit:       10,
		})

		ch := h.GetEventBus().Subscribe(workspaceID)
		defer h.GetEventBus().Unsubscribe(workspaceID, ch)

		raw, err := json.Marshal(workspaceClaimLifecycleParams{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ActorID:     agentID,
			Reason:      "needs operator confirmation",
			DueAt:       "2099-01-01T00:00:00Z",
			AssignedTo:  "reviewer-rpc",
		})
		if err != nil {
			t.Fatalf("marshal review params: %v", err)
		}
		result, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "system", "tests"), raw)
		if rpcErr != nil {
			t.Fatalf("workspaceClaimReview rpc error: %+v", rpcErr)
		}
		resp, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("unexpected review response type %T", result)
		}
		claimResp, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
		if !ok || claimResp.ClaimID != claimID || claimResp.Status != "REVIEW" {
			t.Fatalf("unexpected review response %+v", resp)
		}

		queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
			WorkspaceID: workspaceID,
			QueueType:   "FOLLOW_UP",
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("list follow-up queues: %v", err)
		}
		if len(queues) != 1 {
			t.Fatalf("expected single follow-up queue, got %+v", queues)
		}
		queueID := queues[0].QueueID

		claimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "knowledge_claim.review_requested",
			EntityType:  "knowledge_claim",
			EntityID:    claimID,
			Limit:       10,
		}, seenClaimEvents)
		invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "memory.invalidation_enqueued",
			EntityType:  "memory_invalidation",
			Limit:       10,
		}, seenInvalidationEvents)
		queuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EntityType:  "operator_queue",
			EntityID:    queueID,
			Limit:       10,
		})
		ordered, _ := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
			runtimeEventExpectation{Event: claimPersisted, Type: "workspace.claim.review_requested"},
			runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
			runtimeEventExpectation{Event: queuePersisted, Type: "workspace.ops.updated"},
		)
		if len(ordered) != 3 ||
			!runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) ||
			!runtimeEventChronologicalLess(ordered[1].Event, ordered[2].Event) {
			t.Fatalf("expected review live mirrors to follow persisted chronology, got %+v", ordered)
		}
	})

	t.Run("confirmed", func(t *testing.T) {
		store := newServerTestStore(t)
		h := NewHandler(store)
		const (
			workspaceID = "ws-claim-confirm-queue-live"
			agentID     = "agent-claim-confirm-queue"
			claimID     = "claim-confirm-queue-live"
		)
		ctx := testAuthContext(workspaceID, "agent", agentID)
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "Claim Confirm Queue Live",
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent: %v", err)
		}
		claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
			ClaimID:     claimID,
			WorkspaceID: workspaceID,
			ClaimType:   "FACT",
			Subject:     "Confirm queue live parity",
			Body:        "Confirm should mirror queue resolution runtime rows.",
			Summary:     "Confirm queue baseline",
			Confidence:  0.67,
			SourceKind:  "manual",
			SourceID:    "developer",
			AgentID:     agentID,
		})
		if err != nil {
			t.Fatalf("seed claim: %v", err)
		}
		claim, err = store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ActorID:     agentID,
			Reason:      "seed review workflow",
			ReviewDueAt: "2099-01-01T00:00:00Z",
			AssignedTo:  "reviewer-seed",
		})
		if err != nil {
			t.Fatalf("seed review workflow: %v", err)
		}
		if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
			ReportID:    "memres-claim-confirm-queue-live",
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Replicas: []sqlite.MemoryReplicaStateInput{
				{
					ResidencyTier:  "P2",
					ReplicaKind:    "memory_node",
					CoherenceClass: "A",
					State:          "CURRENT",
					CacheKey:       "packet:claim-confirm-queue",
					VersionGuards: []sqlite.MemoryResidencyVersionGuard{
						{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
					},
				},
			},
		}); err != nil {
			t.Fatalf("seed claim residency: %v", err)
		}
		queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
			WorkspaceID: workspaceID,
			QueueType:   "FOLLOW_UP",
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("list follow-up queues: %v", err)
		}
		if len(queues) != 1 {
			t.Fatalf("expected single follow-up queue, got %+v", queues)
		}
		queueID := queues[0].QueueID

		seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "knowledge_claim.confirmed",
			EntityType:  "knowledge_claim",
			EntityID:    claimID,
			Limit:       10,
		})
		seenQueueResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "operator_queue.resolved",
			EntityType:  "operator_queue",
			EntityID:    queueID,
			Limit:       10,
		})
		seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "memory.invalidation_enqueued",
			EntityType:  "memory_invalidation",
			Limit:       10,
		})

		ch := h.GetEventBus().Subscribe(workspaceID)
		defer h.GetEventBus().Unsubscribe(workspaceID, ch)

		raw, err := json.Marshal(workspaceClaimLifecycleParams{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ActorID:     agentID,
			Reason:      "confirmed from live runtime",
		})
		if err != nil {
			t.Fatalf("marshal confirm params: %v", err)
		}
		result, rpcErr := h.workspaceClaimConfirm(testAuthContext(workspaceID, "system", "tests"), raw)
		if rpcErr != nil {
			t.Fatalf("workspaceClaimConfirm rpc error: %+v", rpcErr)
		}
		resp, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("unexpected confirm response type %T", result)
		}
		claimResp, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
		if !ok || claimResp.ClaimID != claimID || claimResp.Status != "CONFIRMED" {
			t.Fatalf("unexpected confirm response %+v", resp)
		}

		claimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "knowledge_claim.confirmed",
			EntityType:  "knowledge_claim",
			EntityID:    claimID,
			Limit:       10,
		}, seenClaimEvents)
		invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "memory.invalidation_enqueued",
			EntityType:  "memory_invalidation",
			Limit:       10,
		}, seenInvalidationEvents)
		queuePersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "operator_queue.resolved",
			EntityType:  "operator_queue",
			EntityID:    queueID,
			Limit:       10,
		}, seenQueueResolvedEvents)
		ordered, _ := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
			runtimeEventExpectation{Event: claimPersisted, Type: "workspace.claim.confirmed"},
			runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
			runtimeEventExpectation{Event: queuePersisted, Type: "workspace.ops.resolved"},
		)
		if len(ordered) != 3 ||
			!runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) ||
			!runtimeEventChronologicalLess(ordered[1].Event, ordered[2].Event) {
			t.Fatalf("expected confirm live mirrors to follow persisted chronology, got %+v", ordered)
		}
	})
}

func TestWorkspaceClaimEscalateRPCUpdatesReviewQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-claim-escalate-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Escalate RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-escalate-rpc",
		OwnerUserID: "developer",
		DisplayName: "Claim Escalate RPC Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Runtime journal is canonical",
		Body:        "Review escalation should update the queue SLA.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-claim-escalate-rpc",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "needs operator validation",
		ReviewDueAt: "2026-03-23T10:00:00Z",
		AssignedTo:  "reviewer-rpc-a",
	}); err != nil {
		t.Fatalf("request claim review: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	escalateRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "review is approaching SLA breach",
		DueAt:       "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-rpc-b",
		Urgency:     "CRITICAL",
	})
	if err != nil {
		t.Fatalf("marshal escalate params: %v", err)
	}
	result, rpcErr := h.workspaceClaimEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimEscalate rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected claim escalate result type %T", result)
	}
	queue, ok := payload["queue"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected escalated queue payload type %T", payload["queue"])
	}
	if queue.KeepSessionActive {
		t.Fatalf("expected escalated review queue to remain non-session-active, got %+v", queue)
	}
	if queue.LastEscalatedAt == nil || *queue.LastEscalatedAt == "" {
		t.Fatalf("expected escalated review queue to expose last_escalated_at, got %+v", queue)
	}
	if queue.LastEscalatedBy == nil || *queue.LastEscalatedBy != "dashboard" {
		t.Fatalf("expected escalated review queue to expose last_escalated_by, got %+v", queue)
	}

	escalatedClaimPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       1,
	})
	escalatedQueuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       1,
	})
	assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(
		t,
		ch,
		escalatedClaimPersisted,
		"workspace.claim.review_escalated",
		escalatedQueuePersisted,
		"workspace.ops.escalated",
	)

	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues after escalate: %v", err)
	}
	if len(queues) != 1 || queues[0].EscalationCount != 1 || queues[0].AssignedTo != "reviewer-rpc-b" || queues[0].Urgency != "CRITICAL" {
		t.Fatalf("unexpected escalated follow-up queue %+v", queues)
	}
	if queues[0].KeepSessionActive {
		t.Fatalf("expected escalated follow-up queue to remain non-session-active, got %+v", queues[0])
	}
	if queues[0].LastEscalatedAt == nil || *queues[0].LastEscalatedAt == "" {
		t.Fatalf("expected escalated follow-up queue to preserve last_escalated_at, got %+v", queues[0])
	}
	if queues[0].LastEscalatedBy == nil || *queues[0].LastEscalatedBy != "dashboard" {
		t.Fatalf("expected escalated follow-up queue to preserve last_escalated_by, got %+v", queues[0])
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	seen := map[string]bool{}
	for _, event := range events {
		seen[event.EventType] = true
	}
	for _, eventType := range []string{"knowledge_claim.review_requested", "knowledge_claim.review_escalated", "operator_queue.escalated"} {
		if !seen[eventType] {
			t.Fatalf("expected runtime event %s, got %+v", eventType, events)
		}
	}
}

func TestWorkspaceClaimReviewRPCRejectsWhenReviewerScarcitySaturated(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-claim-review-rpc-saturated"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Review RPC Saturated",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	liveAt := "2026-04-12T12:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-claim-review-saturated", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-claim-review-saturated", "coal-claim-review-saturated", "ACTIVE", 1, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-review-saturated", "agent-gen", "GENERATOR", 0.92, 0.40, 0, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-review-saturated", "reviewer-a", "NEAR_REVIEWER", 0.88, 0.35, 0, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-review-saturated", "reviewer-b", "FAR_REVIEWER", 0.79, 0.72, 0, liveAt)
	recordReviewerMeshDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "session-claim-review-saturated", "2026-04-12T12:01:00Z")

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-review-rpc-saturated",
		OwnerUserID: "developer",
		DisplayName: "Claim Review RPC Saturated Agent",
	}); err != nil {
		t.Fatalf("register claim agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Saturated reviewer scarcity should reject review admission",
		Body:        "Claim review admission must fail closed while reviewer capacity is saturated.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-claim-review-rpc-saturated",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}

	beforeClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("get claim before failed review: %v", err)
	}
	beforeQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues before failed review: %v", err)
	}
	if len(beforeQueues) != 0 {
		t.Fatalf("expected no follow-up queues before failed review, got %+v", beforeQueues)
	}
	beforeEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})

	reviewRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "needs operator confirmation",
		DueAt:       "2026-03-23T10:00:00Z",
		AssignedTo:  "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal saturated review params: %v", err)
	}
	result, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "system", "tests"), reviewRaw)
	if rpcErr == nil {
		t.Fatalf("expected reviewer scarcity saturation reject, got result %+v", result)
	}
	if result != nil {
		t.Fatalf("expected no result on reviewer scarcity saturation reject, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params reviewer scarcity reject, got %+v", rpcErr)
	}
	if !strings.Contains(rpcErr.Message, "reviewer scarcity is saturated") {
		t.Fatalf("expected reviewer scarcity saturation message, got %+v", rpcErr)
	}

	afterClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("get claim after failed review: %v", err)
	}
	if !reflect.DeepEqual(afterClaim, beforeClaim) {
		t.Fatalf("expected failed review to leave claim unchanged, before=%+v after=%+v", beforeClaim, afterClaim)
	}

	afterQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues after failed review: %v", err)
	}
	if len(afterQueues) != 0 {
		t.Fatalf("expected failed review to leave follow-up queues absent, got %+v", afterQueues)
	}

	reviewEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list failed claim review runtime events: %v", err)
	}
	if len(reviewEvents) != 0 {
		t.Fatalf("expected no knowledge_claim.review_requested events after failed review, got %+v", reviewEvents)
	}
	queueUpdates, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list failed queue update runtime events: %v", err)
	}
	if len(queueUpdates) != 0 {
		t.Fatalf("expected no operator_queue.updated events after failed review, got %+v", queueUpdates)
	}

	afterEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if !reflect.DeepEqual(afterEvents, beforeEvents) {
		t.Fatalf("expected failed review to append no runtime rows, before=%v after=%v", beforeEvents, afterEvents)
	}
}

func TestWorkspaceClaimDisputeRPCRejectsWhenReviewerScarcitySaturated(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-claim-dispute-rpc-saturated"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Dispute RPC Saturated",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-dispute-rpc-saturated",
		OwnerUserID: "developer",
		DisplayName: "Claim Dispute RPC Saturated Agent",
	}); err != nil {
		t.Fatalf("register claim agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Saturated reviewer scarcity should reject dispute admission",
		Body:        "Claim dispute admission must fail closed while reviewer capacity is saturated.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-claim-dispute-rpc-saturated",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "seed review before dispute",
		ReviewDueAt: "2026-04-20T10:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("seed review workflow: %v", err)
	}

	liveAt := time.Now().UTC().Format(time.RFC3339Nano)
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-claim-dispute-saturated", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-claim-dispute-saturated", "coal-claim-dispute-saturated", "ACTIVE", 1, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-dispute-saturated", "agent-gen", "GENERATOR", 0.92, 0.40, 0, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-dispute-saturated", "reviewer-a", "NEAR_REVIEWER", 0.88, 0.35, 0, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-dispute-saturated", "reviewer-b", "FAR_REVIEWER", 0.79, 0.72, 0, liveAt)
	recordReviewerMeshDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "session-claim-dispute-saturated", time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano))

	beforeClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("get claim before failed dispute: %v", err)
	}
	beforeQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues before failed dispute: %v", err)
	}
	if len(beforeQueues) != 1 {
		t.Fatalf("expected one seeded follow-up queue before failed dispute, got %+v", beforeQueues)
	}
	beforeEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})

	disputeRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "claim is disputed",
		DueAt:       "2026-04-22T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal saturated dispute params: %v", err)
	}
	result, rpcErr := h.workspaceClaimDispute(testAuthContext(workspaceID, "system", "tests"), disputeRaw)
	if rpcErr == nil {
		t.Fatalf("expected reviewer scarcity saturation reject, got result %+v", result)
	}
	if result != nil {
		t.Fatalf("expected no result on reviewer scarcity saturation reject, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params reviewer scarcity reject, got %+v", rpcErr)
	}
	if !strings.Contains(rpcErr.Message, "reviewer scarcity is saturated") {
		t.Fatalf("expected reviewer scarcity saturation message, got %+v", rpcErr)
	}

	afterClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("get claim after failed dispute: %v", err)
	}
	if !reflect.DeepEqual(afterClaim, beforeClaim) {
		t.Fatalf("expected failed dispute to leave claim unchanged, before=%+v after=%+v", beforeClaim, afterClaim)
	}

	afterQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues after failed dispute: %v", err)
	}
	if !reflect.DeepEqual(afterQueues, beforeQueues) {
		t.Fatalf("expected failed dispute to leave follow-up queues unchanged, before=%+v after=%+v", beforeQueues, afterQueues)
	}

	disputeEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.disputed",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list failed claim dispute runtime events: %v", err)
	}
	if len(disputeEvents) != 0 {
		t.Fatalf("expected no knowledge_claim.disputed events after failed dispute, got %+v", disputeEvents)
	}

	afterEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if !reflect.DeepEqual(afterEvents, beforeEvents) {
		t.Fatalf("expected failed dispute to append no runtime rows, before=%v after=%v", beforeEvents, afterEvents)
	}
}

func TestWorkspaceClaimEscalateRPCRejectsWhenReviewerScarcitySaturated(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-claim-escalate-rpc-saturated"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Escalate RPC Saturated",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-escalate-rpc-saturated",
		OwnerUserID: "developer",
		DisplayName: "Claim Escalate RPC Saturated Agent",
	}); err != nil {
		t.Fatalf("register claim agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Saturated reviewer scarcity should reject escalation",
		Body:        "Claim review escalation must fail closed while reviewer capacity is saturated.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-claim-escalate-rpc-saturated",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "needs operator validation",
		ReviewDueAt: "2026-03-23T10:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("request claim review: %v", err)
	}

	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	liveAt := "2026-04-12T12:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-claim-escalate-saturated", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-claim-escalate-saturated", "coal-claim-escalate-saturated", "ACTIVE", 1, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-escalate-saturated", "agent-gen", "GENERATOR", 0.92, 0.40, 0, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-escalate-saturated", "reviewer-a", "NEAR_REVIEWER", 0.88, 0.35, 0, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-claim-escalate-saturated", "reviewer-b", "FAR_REVIEWER", 0.79, 0.72, 0, liveAt)
	recordReviewerMeshDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "session-claim-escalate-saturated", "2026-04-12T12:01:00Z")

	beforeClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("get claim before failed escalation: %v", err)
	}
	beforeQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues before failed escalation: %v", err)
	}
	if len(beforeQueues) != 1 {
		t.Fatalf("expected exactly one review queue before failed escalation, got %+v", beforeQueues)
	}
	beforeQueue := beforeQueues[0]

	escalateRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "review is approaching SLA breach",
		DueAt:       "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-b",
		Urgency:     "CRITICAL",
	})
	if err != nil {
		t.Fatalf("marshal saturated escalate params: %v", err)
	}
	result, rpcErr := h.workspaceClaimEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw)
	if rpcErr == nil {
		t.Fatalf("expected reviewer scarcity saturation reject, got result %+v", result)
	}
	if result != nil {
		t.Fatalf("expected no result on reviewer scarcity saturation reject, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params reviewer scarcity reject, got %+v", rpcErr)
	}
	if !strings.Contains(rpcErr.Message, "reviewer scarcity is saturated") {
		t.Fatalf("expected reviewer scarcity saturation message, got %+v", rpcErr)
	}

	afterClaim, err := store.GetKnowledgeClaim(ctx, workspaceID, claim.ClaimID)
	if err != nil {
		t.Fatalf("get claim after failed escalation: %v", err)
	}
	if !reflect.DeepEqual(afterClaim, beforeClaim) {
		t.Fatalf("expected failed escalation to leave claim unchanged, before=%+v after=%+v", beforeClaim, afterClaim)
	}

	afterQueues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues after failed escalation: %v", err)
	}
	if len(afterQueues) != 1 {
		t.Fatalf("expected exactly one review queue after failed escalation, got %+v", afterQueues)
	}
	afterQueue := afterQueues[0]
	if afterQueue.QueueID != beforeQueue.QueueID ||
		afterQueue.AssignedTo != beforeQueue.AssignedTo ||
		afterQueue.Urgency != beforeQueue.Urgency ||
		derefString(afterQueue.DueAt) != derefString(beforeQueue.DueAt) ||
		afterQueue.EscalationCount != beforeQueue.EscalationCount ||
		strings.TrimSpace(afterQueue.UpdatedAt) != strings.TrimSpace(beforeQueue.UpdatedAt) {
		t.Fatalf("expected failed escalation to leave review queue unchanged, before=%+v after=%+v", beforeQueue, afterQueue)
	}

	claimEscalations, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list failed claim escalation runtime events: %v", err)
	}
	if len(claimEscalations) != 0 {
		t.Fatalf("expected no knowledge_claim.review_escalated events after failed escalation, got %+v", claimEscalations)
	}
	queueEscalations, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    beforeQueue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list failed queue escalation runtime events: %v", err)
	}
	if len(queueEscalations) != 0 {
		t.Fatalf("expected no operator_queue.escalated events after failed escalation, got %+v", queueEscalations)
	}
}

func TestWorkspaceOpsEscalateAndResolveMirrorNewPersistedRowsForQueueLifecycle(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-ops-lifecycle-repeat"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Ops Lifecycle Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:ops-lifecycle-repeat",
		QueueType:   "FOLLOW_UP",
		Title:       "Repeat lifecycle queue",
		Summary:     "Initial queue state",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	firstEscalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		EscalatedBy: "operator-a",
		Reason:      "first escalation",
		AssignedTo:  "reviewer-a",
		Urgency:     "HIGH",
	})
	if err != nil {
		t.Fatalf("marshal first escalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), firstEscalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate first rpc error: %+v", rpcErr)
	}
	firstEscalatedLive := nextEventOfType(t, ch, "workspace.ops.escalated")
	firstEscalatedPersisted := mustRuntimeEvent(t, ctx, store, escalatedFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstEscalatedLive, firstEscalatedPersisted, "workspace.ops.escalated")

	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, queue.QueueID, "")
	secondEscalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		EscalatedBy:      "operator-b",
		Reason:           "second escalation",
		AssignedTo:       "reviewer-b",
		Urgency:          "CRITICAL",
		DueAt:            "2099-01-01T00:00:00Z",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal second escalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), secondEscalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate second rpc error: %+v", rpcErr)
	}
	secondEscalatedLive := nextEventOfType(t, ch, "workspace.ops.escalated")
	secondEscalatedPersisted := mustNewRuntimeEvent(t, ctx, store, escalatedFilter, seenEscalated)
	assertLiveEventMirrorsRuntimeEvent(t, secondEscalatedLive, secondEscalatedPersisted, "workspace.ops.escalated")
	if secondEscalatedPersisted.EventID == firstEscalatedPersisted.EventID || secondEscalatedPersisted.IngestSeq <= firstEscalatedPersisted.IngestSeq {
		t.Fatalf("expected repeated queue escalation to mirror the newly appended runtime row, got first=%+v second=%+v", firstEscalatedPersisted, secondEscalatedPersisted)
	}
	var escalatedEnvelope sqlite.OperatorQueueRecord
	if err := json.Unmarshal([]byte(secondEscalatedLive.PayloadJSON), &escalatedEnvelope); err != nil {
		t.Fatalf("decode second escalated payload: %v", err)
	}
	if escalatedEnvelope.QueueID != queue.QueueID || escalatedEnvelope.AssignedTo != "reviewer-b" || escalatedEnvelope.Urgency != "CRITICAL" || escalatedEnvelope.EscalationCount != 2 {
		t.Fatalf("unexpected repeated queue escalation payload %+v", escalatedEnvelope)
	}

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	currentRevision, currentUpdatedAt = currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, queue.QueueID, "")
	firstResolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		ResolvedBy:       "operator-a",
		Resolution:       "first resolution",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal first resolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), firstResolveRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsResolve first rpc error: %+v", rpcErr)
	}
	firstResolvedLive := nextEventOfType(t, ch, "workspace.ops.resolved")
	firstResolvedPersisted := mustRuntimeEvent(t, ctx, store, resolvedFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstResolvedLive, firstResolvedPersisted, "workspace.ops.resolved")
}

func TestWorkspaceOpsEscalateReassignsLinkedRebaseActionAndRejectsStaleStart(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-rebase-handoff-start"
		taskID      = "task-ops-escalate-rebase-handoff-start"
		agentID     = "agent-ops-escalate-rebase-handoff-start"
		queueKey    = "tension_rebase_followup:tens-repair-ops-handoff-start"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-ops-handoff-start",
		"fork_tension_id":     "tens-fork-ops-handoff-start",
		"repair_tension_id":   "tens-repair-ops-handoff-start",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-ops-handoff-start",
		Details:           "Coalition ID: coal-ops-handoff-start\nRepair tension: tens-repair-ops-handoff-start\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-handoff-start",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue before handoff): %v", err)
	}
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "")
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		EscalatedBy:      "lead-b",
		Reason:           "handoff to reviewer-b",
		AssignedTo:       "reviewer-b",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}
	escalatedPersisted := mustNewRuntimeEvent(t, ctx, store, escalatedFilter, seenEscalated)
	actionQueueUpdatedPersisted := mustNewRuntimeEvent(t, ctx, store, actionQueueUpdatedFilter, seenActionQueueUpdated)
	ordered, _ := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: escalatedPersisted, Type: "workspace.ops.escalated"},
		runtimeEventExpectation{Event: actionQueueUpdatedPersisted, Type: "workspace.ops.updated"},
	)
	if len(ordered) != 2 || !runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) {
		t.Fatalf("expected queue handoff events in persisted chronological order, got %+v", ordered)
	}
	if escalatedPersisted.AuthorityHolderNodeID != authority.HolderAuthorityNodeID ||
		escalatedPersisted.AuthorityTerm != authority.Term ||
		escalatedPersisted.AuthorityLeaseTokenFingerprint == "" {
		t.Fatalf("expected escalated source queue event to carry authority metadata, got %+v", escalatedPersisted)
	}
	if actionQueueUpdatedPersisted.AuthorityHolderNodeID != authority.HolderAuthorityNodeID ||
		actionQueueUpdatedPersisted.AuthorityTerm != authority.Term ||
		actionQueueUpdatedPersisted.AuthorityLeaseTokenFingerprint == "" {
		t.Fatalf("expected linked action queue handoff event to carry authority metadata, got %+v", actionQueueUpdatedPersisted)
	}

	escalatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(escalated): %v", err)
	}
	if escalatedQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("escalated source queue assigned_to = %q, want reviewer-b", escalatedQueue.AssignedTo)
	}
	escalatedPayload, err := actionCreateDecodeQueuePayload(escalatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(escalated): %v", err)
	}
	if escalatedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("escalated source queue payload action_assigned_to = %q, want reviewer-b", escalatedPayload.ActionAssignedTo)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(escalated): %v", err)
	}
	if action.AssignedTo != "reviewer-b" {
		t.Fatalf("escalated action assigned_to = %q, want reviewer-b", action.AssignedTo)
	}
	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue): %v", err)
	}
	if actionQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("escalated action queue assigned_to = %q, want reviewer-b", actionQueue.AssignedTo)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("escalated action queue payload action_assigned_to = %q, want reviewer-b", actionQueuePayload.ActionAssignedTo)
	}

	staleStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "stale holder should fail after handoff",
	})
	if err != nil {
		t.Fatalf("marshal stale actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, staleStartRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected stale holder mismatch on actionStart after handoff, got %+v", rpcErr)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-b",
		Comment:   "new holder resumes work after handoff",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error after handoff: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
}

func TestWorkspaceOpsEscalateRejectsStaleSnapshotAfterLinkedRebaseActionStart(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-stale-after-rebase-start"
		taskID      = "task-ops-escalate-stale-after-rebase-start"
		agentID     = "agent-ops-escalate-stale-after-rebase-start"
		repairID    = "tens-repair-ops-escalate-stale-after-rebase-start"
	)

	actionID, sourceQueueID := createPendingRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before start): %v", err)
	}
	staleUpdatedAt := sourceQueueBefore.UpdatedAt
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "winner start should beat stale handoff",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	startedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after start): %v", err)
	}
	startedActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-b",
		Reason:           "stale handoff should fail after start",
		AssignedTo:       "reviewer-b",
		Urgency:          "LOW",
		DueAt:            "2099-03-01T00:00:00Z",
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to reject outdated queue revision after start")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale escalate after start, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale escalate): %v", err)
	}
	if currentQueue.AssignedTo != startedQueue.AssignedTo || currentQueue.Urgency != startedQueue.Urgency || currentQueue.UpdatedAt != startedQueue.UpdatedAt || derefString(currentQueue.DueAt) != derefString(startedQueue.DueAt) {
		t.Fatalf("stale escalate mutated started source queue state: got %+v want assigned_to=%q urgency=%q due_at=%q updated_at=%q", currentQueue, startedQueue.AssignedTo, startedQueue.Urgency, derefString(startedQueue.DueAt), startedQueue.UpdatedAt)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("current source queue payload action_assigned_to = %q, want reviewer-a", currentPayload.ActionAssignedTo)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("current source queue workflow after stale escalate = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" {
		t.Fatalf("action assigned_to after stale escalate = %q, want reviewer-a", action.AssignedTo)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != startedActionQueue.AssignedTo || actionQueue.UpdatedAt != startedActionQueue.UpdatedAt {
		t.Fatalf("stale escalate mutated action queue state: got %+v want assigned_to=%q updated_at=%q", actionQueue, startedActionQueue.AssignedTo, startedActionQueue.UpdatedAt)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("action queue payload action_assigned_to after stale escalate = %q, want reviewer-a", actionQueuePayload.ActionAssignedTo)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("stale escalate after start should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("stale escalate after start should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsStaleSnapshotAfterLinkedRebaseActionPause(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-stale-after-rebase-pause"
		taskID      = "task-ops-escalate-stale-after-rebase-pause"
		agentID     = "agent-ops-escalate-stale-after-rebase-pause"
		repairID    = "tens-repair-ops-escalate-stale-after-rebase-pause"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before pause): %v", err)
	}
	staleUpdatedAt := sourceQueueBefore.UpdatedAt
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "winner pause should beat stale handoff",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	pausedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after pause): %v", err)
	}
	pausedActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-b",
		Reason:           "stale handoff should fail after pause",
		AssignedTo:       "reviewer-b",
		Urgency:          "LOW",
		DueAt:            "2099-04-01T00:00:00Z",
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to reject outdated queue revision after pause")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale escalate after pause, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale escalate): %v", err)
	}
	if currentQueue.AssignedTo != pausedQueue.AssignedTo || currentQueue.Urgency != pausedQueue.Urgency || currentQueue.UpdatedAt != pausedQueue.UpdatedAt || derefString(currentQueue.DueAt) != derefString(pausedQueue.DueAt) {
		t.Fatalf("stale escalate mutated paused source queue state: got %+v want assigned_to=%q urgency=%q due_at=%q updated_at=%q", currentQueue, pausedQueue.AssignedTo, pausedQueue.Urgency, derefString(pausedQueue.DueAt), pausedQueue.UpdatedAt)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("current source queue payload action_assigned_to = %q, want reviewer-a", currentPayload.ActionAssignedTo)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateClaimed || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("current source queue workflow after stale escalate = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" {
		t.Fatalf("action assigned_to after stale escalate = %q, want reviewer-a", action.AssignedTo)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != pausedActionQueue.AssignedTo || actionQueue.UpdatedAt != pausedActionQueue.UpdatedAt {
		t.Fatalf("stale escalate mutated action queue state: got %+v want assigned_to=%q updated_at=%q", actionQueue, pausedActionQueue.AssignedTo, pausedActionQueue.UpdatedAt)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("action queue payload action_assigned_to after stale escalate = %q, want reviewer-a", actionQueuePayload.ActionAssignedTo)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("stale escalate after pause should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("stale escalate after pause should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsStaleSnapshotAfterLinkedRebaseFailedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-stale-after-rebase-failed-resolve"
		taskID      = "task-ops-escalate-stale-after-rebase-failed-resolve"
		agentID     = "agent-ops-escalate-stale-after-rebase-failed-resolve"
		repairID    = "tens-repair-ops-escalate-stale-after-rebase-failed-resolve"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before failed resolve): %v", err)
	}
	staleUpdatedAt := sourceQueueBefore.UpdatedAt

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "winner failed resolve should beat stale handoff",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	failedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after failed resolve): %v", err)
	}
	failedActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    failedActionQueue.QueueID,
		Limit:       10,
	}
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-b",
		Reason:           "stale handoff should fail after failed resolve",
		AssignedTo:       "reviewer-b",
		Urgency:          "LOW",
		DueAt:            "2099-05-01T00:00:00Z",
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to reject outdated queue revision after failed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale escalate after failed resolve, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale escalate): %v", err)
	}
	if currentQueue.AssignedTo != failedQueue.AssignedTo || currentQueue.Urgency != failedQueue.Urgency || currentQueue.UpdatedAt != failedQueue.UpdatedAt || derefString(currentQueue.DueAt) != derefString(failedQueue.DueAt) {
		t.Fatalf("stale escalate mutated failed-resolve source queue state: got %+v want assigned_to=%q urgency=%q due_at=%q updated_at=%q", currentQueue, failedQueue.AssignedTo, failedQueue.Urgency, derefString(failedQueue.DueAt), failedQueue.UpdatedAt)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionID != "" || currentPayload.ActionQueueKey != "" || currentPayload.ActionStatus != "" || currentPayload.ActionAssignedTo != "" {
		t.Fatalf("current source queue should clear active action linkage after failed resolve, got %+v", currentPayload)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateClaimed || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("current source queue workflow after stale escalate = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	}
	if currentPayload.LastFailedActionID != actionID || currentPayload.LastFailedStatus != humanActionStatusFailed {
		t.Fatalf("current source queue failed lineage after stale escalate = action=%q status=%q, want (%q,%q)", currentPayload.LastFailedActionID, currentPayload.LastFailedStatus, actionID, humanActionStatusFailed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusFailed {
		t.Fatalf("action mutated after stale escalate = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != failedActionQueue.AssignedTo || actionQueue.UpdatedAt != failedActionQueue.UpdatedAt || actionQueue.Status != failedActionQueue.Status {
		t.Fatalf("stale escalate mutated action queue state: got %+v want assigned_to=%q status=%q updated_at=%q", actionQueue, failedActionQueue.AssignedTo, failedActionQueue.Status, failedActionQueue.UpdatedAt)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != "reviewer-a" {
		t.Fatalf("action queue payload action_assigned_to after stale escalate = %q, want reviewer-a", actionQueuePayload.ActionAssignedTo)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("stale escalate after failed resolve should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated) {
		t.Fatalf("stale escalate after failed resolve should not append action queue updated rows, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsInterleavingWinnerOnStartedLinkedRebaseAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-interleaving-winner-started-rebase"
		taskID      = "task-ops-escalate-interleaving-winner-started-rebase"
		agentID     = "agent-ops-escalate-interleaving-winner-started-rebase"
		repairID    = "tens-repair-ops-escalate-interleaving-winner-started-rebase"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	staleRevision, staleUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueueID, "")
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       10,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       10,
	}
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)

	winnerQueue, err := interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueueID, "lead-b", "reviewer-b", "interleaving winner should own started linked rebase handoff")
	if err != nil {
		t.Fatalf("interleaving workspaceOpsEscalate winner: %v", err)
	}

	staleEscalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-c",
		Reason:           "stale outer handoff should fail after winner",
		AssignedTo:       "reviewer-c",
		Urgency:          "LOW",
		DueAt:            "2099-05-01T00:00:00Z",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), staleEscalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to reject after interleaving winner on started linked rebase")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale interleaving workspaceOpsEscalate, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale interleaving escalate): %v", err)
	}
	if currentQueue.AssignedTo != winnerQueue.AssignedTo || currentQueue.Urgency != winnerQueue.Urgency || currentQueue.UpdatedAt != winnerQueue.UpdatedAt || derefString(currentQueue.DueAt) != derefString(winnerQueue.DueAt) {
		t.Fatalf("stale interleaving escalate mutated winner source queue state: got %+v want assigned_to=%q urgency=%q due_at=%q updated_at=%q", currentQueue, winnerQueue.AssignedTo, winnerQueue.Urgency, derefString(winnerQueue.DueAt), winnerQueue.UpdatedAt)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-b" || action.Status != humanActionStatusPending {
		t.Fatalf("stale interleaving escalate mutated linked action truth = %+v, want assigned_to reviewer-b and pending status", action)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != "reviewer-b" || actionQueue.UpdatedAt == actionQueueBefore.UpdatedAt {
		t.Fatalf("expected exactly one winning action-queue reassignment to reviewer-b, got %+v (before updated_at=%q)", actionQueue, actionQueueBefore.UpdatedAt)
	}
	actionQueuePayload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action queue): %v", err)
	}
	if actionQueuePayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("action queue payload action_assigned_to after interleaving winner = %q, want reviewer-b", actionQueuePayload.ActionAssignedTo)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated)+1 {
		t.Fatalf("interleaving winner should append exactly one escalated runtime row, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("interleaving winner should append exactly one action queue updated row, before=%v after=%v", seenActionQueueUpdated, got)
	}
}

func TestWorkspaceOpsEscalateReassignsInProgressRebaseActionAndRejectsStalePause(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-rebase-handoff-pause"
		taskID      = "task-ops-escalate-rebase-handoff-pause"
		agentID     = "agent-ops-escalate-rebase-handoff-pause"
		queueKey    = "tension_rebase_followup:tens-repair-ops-handoff-pause"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-ops-handoff-pause",
		"fork_tension_id":     "tens-fork-ops-handoff-pause",
		"repair_tension_id":   "tens-repair-ops-handoff-pause",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-ops-handoff-pause",
		Details:           "Coalition ID: coal-ops-handoff-pause\nRepair tension: tens-repair-ops-handoff-pause\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-handoff-pause",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "start before handoff",
	})
	if err != nil {
		t.Fatalf("marshal initial actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("initial actionStart rpc error: %+v", rpcErr)
	}

	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "")
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		EscalatedBy:      "lead-b",
		Reason:           "handoff in progress work to reviewer-b",
		AssignedTo:       "reviewer-b",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(escalated): %v", err)
	}
	if action.AssignedTo != "reviewer-b" {
		t.Fatalf("escalated in-progress action assigned_to = %q, want reviewer-b", action.AssignedTo)
	}
	escalatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(escalated): %v", err)
	}
	escalatedPayload, err := actionCreateDecodeQueuePayload(escalatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(escalated): %v", err)
	}
	if escalatedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("escalated in-progress payload action_assigned_to = %q, want reviewer-b", escalatedPayload.ActionAssignedTo)
	}

	stalePauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "stale holder should fail after handoff",
	})
	if err != nil {
		t.Fatalf("marshal stale actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, stalePauseRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected stale holder mismatch on actionPause after handoff, got %+v", rpcErr)
	}

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-b",
		Comment:  "new holder pauses work after handoff",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
		t.Fatalf("actionPause rpc error after handoff: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestWorkspaceOpsEscalateReassignsLinkedRebaseActionAndRejectsStaleCompletedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-rebase-handoff-complete"
		taskID      = "task-ops-escalate-rebase-handoff-complete"
		agentID     = "agent-ops-escalate-rebase-handoff-complete"
		queueKey    = "tension_rebase_followup:tens-repair-ops-handoff-complete"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-ops-handoff-complete",
		"fork_tension_id":     "tens-fork-ops-handoff-complete",
		"repair_tension_id":   "tens-repair-ops-handoff-complete",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-ops-handoff-complete",
		Details:           "Coalition ID: coal-ops-handoff-complete\nRepair tension: tens-repair-ops-handoff-complete\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-handoff-complete",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "")
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		EscalatedBy:      "lead-b",
		Reason:           "handoff resolution to reviewer-b",
		AssignedTo:       "reviewer-b",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}

	staleResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "stale holder should fail after handoff",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal stale actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, staleResolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected stale holder mismatch on rebase actionResolve(COMPLETED) after handoff, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "new holder completes rebase after handoff",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error after handoff: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
}

func TestWorkspaceOpsEscalateReassignsInProgressRebaseActionAndRejectsStaleFailedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-rebase-handoff-failed-resolve"
		taskID      = "task-ops-escalate-rebase-handoff-failed-resolve"
		agentID     = "agent-ops-escalate-rebase-handoff-failed-resolve"
		queueKey    = "tension_rebase_followup:tens-repair-ops-handoff-failed-resolve"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-ops-handoff-failed-resolve",
		"fork_tension_id":     "tens-fork-ops-handoff-failed-resolve",
		"repair_tension_id":   "tens-repair-ops-handoff-failed-resolve",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for coalition coal-ops-handoff-failed-resolve",
		Details:           "Coalition ID: coal-ops-handoff-failed-resolve\nRepair tension: tens-repair-ops-handoff-failed-resolve\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-handoff-failed-resolve",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "start before failed handoff branch",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "")
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		EscalatedBy:      "lead-b",
		Reason:           "handoff failed-resolution branch to reviewer-b",
		AssignedTo:       "reviewer-b",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}

	staleResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "stale holder should fail after handoff",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal stale actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, staleResolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected stale holder mismatch on rebase actionResolve(FAILED) after handoff, got %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "new holder reopens rebase after handoff",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error after handoff: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestWorkspaceOpsEscalateReassignsLinkedRollbackFailureActionAndRejectsStaleResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-rollback-handoff-resolve"
		taskID      = "task-ops-escalate-rollback-handoff-resolve"
		agentID     = "agent-ops-escalate-rollback-handoff-resolve"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-ops-handoff-resolve"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-ops-handoff-resolve")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, sourceQueue.QueueID, "")
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		EscalatedBy:      "lead-b",
		Reason:           "handoff rollback recovery to reviewer-b",
		AssignedTo:       "reviewer-b",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(escalated): %v", err)
	}
	if action.AssignedTo != "reviewer-b" {
		t.Fatalf("escalated rollback-failure action assigned_to = %q, want reviewer-b", action.AssignedTo)
	}
	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue): %v", err)
	}
	if actionQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("escalated rollback-failure action queue assigned_to = %q, want reviewer-b", actionQueue.AssignedTo)
	}

	staleResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "stale holder should fail after handoff",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal stale actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, staleResolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected stale holder mismatch on rollback-failure actionResolve after handoff, got %+v", rpcErr)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "new holder completes rollback recovery after handoff",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error after handoff: %+v", rpcErr)
	}

	resolvedAction, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(resolved): %v", err)
	}
	if resolvedAction.Status != humanActionStatusCompleted {
		t.Fatalf("resolved rollback-failure action status = %q, want COMPLETED", resolvedAction.Status)
	}
}

func TestWorkspaceOpsUpsertRejectsInterleavingEscalateWinnerOnLinkedPendingRollbackFailureAction(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID   = "ws-ops-upsert-interleaving-escalate-winner-linked-pending-rollback"
		taskID        = "task-ops-upsert-interleaving-escalate-winner-linked-pending-rollback"
		agentID       = "agent-ops-upsert-interleaving-escalate-winner-linked-pending-rollback"
		queueKey      = model.RebaseRollbackFailureQueueKeyPrefix + "repair-ops-upsert-interleaving-escalate-winner-linked-pending-rollback"
		winnerSummary = "winner rollback handoff should beat stale manual edit"
		winnerDetails = "stale upsert should not survive interleaving rollback-failure handoff winner"
		winnerDueAt   = "2099-06-01T00:00:00Z"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-ops-upsert-interleaving-escalate-winner-linked-pending-rollback")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before interleaving handoff): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	sourceEscalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	sourceResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       20,
	}
	actionQueueUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenSourceEscalated := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter)
	seenSourceResolved := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter)
	seenActionQueueUpdated := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		QueueKey:         sourceQueueBefore.QueueKey,
		QueueType:        sourceQueueBefore.QueueType,
		Title:            sourceQueueBefore.Title,
		Summary:          winnerSummary,
		Details:          winnerDetails,
		AssignedTo:       sourceQueueBefore.AssignedTo,
		Urgency:          "CRITICAL",
		SourceKind:       sourceQueueBefore.SourceKind,
		SourceID:         sourceQueueBefore.SourceID,
		TaskID:           sourceQueueBefore.TaskID,
		AgentID:          sourceQueueBefore.AgentID,
		DueAt:            winnerDueAt,
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}

	var (
		hookRan     bool
		hookErr     error
		winnerQueue sqlite.OperatorQueueRecord
	)
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		hookRan = true
		winnerQueue, hookErr = interleaveWorkspaceOpsEscalateForTest(t, ctx, h, store, workspaceID, sourceQueue.QueueID, "lead-b", "reviewer-b", "rollback-failure handoff winner should beat stale manual edit")
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to fail after interleaving rollback-failure handoff winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale rollback-failure upsert after handoff winner, got %+v", rpcErr)
	} else {
		msg := strings.ToLower(rpcErr.Message)
		if strings.Contains(msg, "use workspace.ops.escalate") ||
			strings.Contains(msg, "operator queue item is not open") ||
			strings.Contains(msg, "assigned to") ||
			strings.Contains(msg, "human action was updated concurrently") {
			t.Fatalf("expected stale rollback-failure upsert loser, not adjacent guard path, got %+v", rpcErr)
		}
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsUpsertStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving rollback-failure handoff hook: %v", hookErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale rollback-failure upsert): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("rollback-failure source queue status after stale upsert loser = %q, want OPEN", currentQueue.Status)
	}
	if currentQueue.AssignedTo != winnerQueue.AssignedTo || currentQueue.Urgency != winnerQueue.Urgency || currentQueue.UpdatedAt != winnerQueue.UpdatedAt || derefString(currentQueue.DueAt) != derefString(winnerQueue.DueAt) {
		t.Fatalf("rollback-failure upsert loser mutated winner source queue state: got %+v want assigned_to=%q urgency=%q due_at=%q updated_at=%q", currentQueue, winnerQueue.AssignedTo, winnerQueue.Urgency, derefString(winnerQueue.DueAt), winnerQueue.UpdatedAt)
	}
	if strings.TrimSpace(currentQueue.Resolution) != "" || derefString(currentQueue.ResolvedBy) != "" {
		t.Fatalf("rollback-failure source queue should remain open after stale upsert loser, got resolution=%q resolved_by=%q", currentQueue.Resolution, derefString(currentQueue.ResolvedBy))
	}
	if currentQueue.Summary == winnerSummary || strings.Contains(currentQueue.Details, winnerDetails) {
		t.Fatalf("rollback-failure stale upsert should not smear manual text onto winner handoff truth = %+v", currentQueue)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale upsert loser: %v", err)
	}
	if payload.FollowupActionID != actionID || payload.FollowupActionQueueKey != "action:"+actionID || payload.FollowupActionStatus != humanActionStatusPending {
		t.Fatalf("rollback-failure payload after stale upsert loser = %+v, want active pending followup linkage for %q", payload, actionID)
	}
	if payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("rollback-failure stale upsert loser should not mint failed lineage, got %+v", payload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-b" || action.Status != humanActionStatusPending {
		t.Fatalf("rollback-failure stale upsert loser mutated linked action truth = %+v, want assigned_to reviewer-b and pending status", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != "reviewer-b" || actionQueue.Status != "OPEN" || actionQueue.UpdatedAt == actionQueueBefore.UpdatedAt {
		t.Fatalf("expected exactly one winning rollback-failure action-queue reassignment to reviewer-b, got %+v (before updated_at=%q)", actionQueue, actionQueueBefore.UpdatedAt)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("rollback-failure handoff winner should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceEscalatedFilter); len(got) != len(seenSourceEscalated)+1 {
		t.Fatalf("rollback-failure handoff winner should append exactly one source escalation row, before=%v after=%v", seenSourceEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceResolvedFilter); len(got) != len(seenSourceResolved) {
		t.Fatalf("rollback-failure stale upsert loser should not append source queue resolved rows, before=%v after=%v", seenSourceResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueUpdatedFilter); len(got) != len(seenActionQueueUpdated)+1 {
		t.Fatalf("rollback-failure handoff winner should append exactly one action queue updated row, before=%v after=%v", seenActionQueueUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("rollback-failure stale upsert loser should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
	assertLinkedRollbackFailureActionAuthorityHandoff(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, "reviewer-b")
}

func TestWorkspaceOpsResolveRejectsRepeatedTerminalResolution(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-ops-lifecycle-repeat-reject"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Ops Lifecycle Repeat Reject",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:ops-lifecycle-repeat-reject",
		QueueType:   "FOLLOW_UP",
		Title:       "Repeat lifecycle queue reject",
		Summary:     "Initial queue state",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}

	firstResolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "operator-a",
		Resolution:  "first resolution",
	})
	if err != nil {
		t.Fatalf("marshal first resolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), firstResolveRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsResolve first rpc error: %+v", rpcErr)
	}

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	firstResolvedPersisted := mustRuntimeEvent(t, ctx, store, resolvedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, queue.QueueID, "")

	secondResolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		ResolvedBy:       "operator-b",
		Resolution:       "second resolution",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal second resolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), secondResolveRaw); rpcErr == nil {
		t.Fatal("expected repeated workspaceOpsResolve to reject already-resolved queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "operator queue item is not open") {
		t.Fatalf("expected invalid params not-open error on repeated resolve, got %+v", rpcErr)
	}

	resolvedPersisted, err := store.ListRuntimeEvents(ctx, resolvedFilter)
	if err != nil {
		t.Fatalf("list resolved queue runtime events after rejected resolve: %v", err)
	}
	if len(resolvedPersisted) != 1 || resolvedPersisted[0].EventID != firstResolvedPersisted.EventID {
		t.Fatalf("expected rejected repeated resolve not to append runtime rows, got %+v", resolvedPersisted)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("expected rejected repeated resolve not to change resolved event set, before=%v after=%v", seenResolved, got)
	}
}

func currentQueueRevisionTokenForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueID, queueKey string) (int64, string) {
	t.Helper()
	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, queueID, queueKey)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s,%s): %v", queueID, queueKey, err)
	}
	return queue.Revision, queue.UpdatedAt
}

func seedQueueOnlyRollbackFailureQueueForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueKey, entityID string) sqlite.OperatorQueueRecord {
	t.Helper()
	payloadJSON, err := json.Marshal(model.RebaseRollbackFailurePayload{
		Kind:           model.RebaseRollbackFailureKind,
		FailureScope:   "rsp_anomaly_list",
		FailureTrigger: "verifier_late_fail_queue_list",
		FailureMessage: "Queue-only anomaly recovery needs operator review.",
		EntityID:       entityID,
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure payload: %v", err)
	}
	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Recover queue-only rollback failure",
		Summary:           "Investigate rollback recovery and resolve or escalate the queue directly",
		Details:           "Failure scope: rsp_anomaly_list\nFailure trigger: verifier_late_fail_queue_list\nEntity: " + entityID,
		PayloadJSON:       string(payloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "runtime_event",
		SourceID:          entityID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("seed rollback-failure queue: %v", err)
	}
	return queue
}

func TestWorkspaceOpsResolveClosesQueueOnlyRollbackFailureQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure"
		entityID    = "entity-ops-rollback-failure"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Resolve",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)

	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "operator-a",
		Resolution:  "reviewed queue-only rollback recovery",
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}
	respAny, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsResolve rpc error: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceOpsResolve response type %T", respAny)
	}
	if got, _ := resp["status"].(string); got != "RESOLVED" {
		t.Fatalf("workspaceOpsResolve status = %q, want RESOLVED", got)
	}

	resolved, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved rollback-failure queue: %v", err)
	}
	if resolved.Status != "RESOLVED" {
		t.Fatalf("queue status = %q, want RESOLVED", resolved.Status)
	}
	if resolved.Resolution != "reviewed queue-only rollback recovery" {
		t.Fatalf("queue resolution = %q, want %q", resolved.Resolution, "reviewed queue-only rollback recovery")
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(resolved.PayloadJSON)
	if err != nil {
		t.Fatalf("decode resolved rollback-failure payload: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("rollback-failure payload changed unexpectedly after queue-only resolve: %+v", payload)
	}
}

func TestWorkspaceOpsResolveRejectsStaleQueueOnlyRollbackFailureSnapshot(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-resolve-stale"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-resolve-stale"
		entityID    = "entity-ops-rollback-failure-resolve-stale"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Resolve Stale",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	escalated, _, _, err := store.EscalateOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		EscalatedBy: "operator-fresh",
		Reason:      "fresh escalation before stale resolve",
		AssignedTo:  "reviewer-fresh",
		Urgency:     "CRITICAL",
	})
	if err != nil {
		t.Fatalf("fresh escalate before stale resolve: %v", err)
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)

	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale resolve should fail",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale resolve params: %v", err)
	}
	blindResolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "operator-blind",
		Resolution:  "blind resolve should fail",
	})
	if err != nil {
		t.Fatalf("marshal blind resolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), blindResolveRaw); rpcErr == nil {
		t.Fatal("expected blind workspaceOpsResolve to reject missing queue base-version after revision advanced")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "current_revision") {
		t.Fatalf("expected invalid params current_revision guidance on blind resolve, got %+v", rpcErr)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to reject outdated queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale resolve, got %+v", rpcErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale resolve reject: %v", err)
	}
	if current.Status != "OPEN" {
		t.Fatalf("queue status after stale resolve = %q, want OPEN", current.Status)
	}
	if current.AssignedTo != escalated.AssignedTo || current.Urgency != escalated.Urgency || current.UpdatedAt != escalated.UpdatedAt {
		t.Fatalf("stale resolve mutated refreshed queue state: got %+v want assigned_to=%q urgency=%q updated_at=%q", current, escalated.AssignedTo, escalated.Urgency, escalated.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("stale resolve should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsInterleavingEscalateWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-resolve-interleaving-escalate-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-resolve-interleaving-escalate-winner"
		entityID    = "entity-ops-rollback-failure-resolve-interleaving-escalate-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Resolve Interleaving Escalate Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale queue-only resolve should fail after interleaving handoff",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale resolve params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsResolveStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsResolveStoreOverride = nil
		hookRan = true
		escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			EscalatedBy:      "operator-fresh",
			Reason:           "winner queue-only handoff should beat stale resolve",
			AssignedTo:       "reviewer-fresh",
			Urgency:          "CRITICAL",
			DueAt:            "2099-02-01T00:00:00Z",
			CurrentRevision:  staleRevision,
			CurrentUpdatedAt: staleUpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving escalate params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only escalate winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to fail after interleaving queue-only escalate winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale queue-only resolve after interleaving escalate winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsResolveStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only escalate hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving resolve reject: %v", err)
	}
	if current.Status != "OPEN" {
		t.Fatalf("queue status after stale interleaving resolve = %q, want OPEN", current.Status)
	}
	if current.AssignedTo != "reviewer-fresh" || current.Urgency != "CRITICAL" || derefString(current.DueAt) != "2099-02-01T00:00:00Z" {
		t.Fatalf("stale interleaving resolve mutated winner-owned handoff state: got %+v", current)
	}
	if current.EscalationCount != queue.EscalationCount+1 {
		t.Fatalf("queue escalation_count after stale interleaving resolve = %d, want %d", current.EscalationCount, queue.EscalationCount+1)
	}
	if strings.TrimSpace(current.Resolution) != "" || derefString(current.ResolvedBy) != "" {
		t.Fatalf("stale interleaving resolve smeared terminal fields onto open queue: got resolution=%q resolved_by=%q", current.Resolution, derefString(current.ResolvedBy))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving resolve reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving handoff winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving handoff winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated)+1 {
		t.Fatalf("interleaving queue-only escalate winner should append exactly one escalated runtime row, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("stale queue-only resolve loser should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsInterleavingUpsertWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-resolve-interleaving-upsert-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-resolve-interleaving-upsert-winner"
		entityID    = "entity-ops-rollback-failure-resolve-interleaving-upsert-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Resolve Interleaving Upsert Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	updatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenUpdated := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)

	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale queue-only resolve should fail after interleaving manual edit winner",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale resolve params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsResolveStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsResolveStoreOverride = nil
		hookRan = true
		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			QueueKey:         queue.QueueKey,
			QueueType:        queue.QueueType,
			Title:            queue.Title,
			Summary:          "winner queue-only note should beat stale resolve",
			Details:          "winner workspace.ops.upsert should block stale terminal close on queue-only rollback-failure carrier",
			AssignedTo:       queue.AssignedTo,
			Urgency:          queue.Urgency,
			SourceKind:       queue.SourceKind,
			SourceID:         queue.SourceID,
			TaskID:           queue.TaskID,
			AgentID:          queue.AgentID,
			CurrentRevision:  staleRevision,
			CurrentUpdatedAt: staleUpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving upsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only upsert winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to fail after interleaving queue-only upsert winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale queue-only resolve after interleaving upsert winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsResolveStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only upsert hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving resolve reject: %v", err)
	}
	if current.Status != "OPEN" {
		t.Fatalf("queue status after stale interleaving resolve = %q, want OPEN", current.Status)
	}
	if current.Summary != "winner queue-only note should beat stale resolve" || current.Details != "winner workspace.ops.upsert should block stale terminal close on queue-only rollback-failure carrier" {
		t.Fatalf("stale interleaving resolve did not preserve winner-owned manual text = %+v", current)
	}
	if current.AssignedTo != queue.AssignedTo || current.Urgency != queue.Urgency || derefString(current.DueAt) != derefString(queue.DueAt) {
		t.Fatalf("stale interleaving resolve mutated queue-only rollback-failure ownership fields: got %+v want assigned_to=%q urgency=%q due_at=%q", current, queue.AssignedTo, queue.Urgency, derefString(queue.DueAt))
	}
	if strings.TrimSpace(current.Resolution) != "" || derefString(current.ResolvedBy) != "" {
		t.Fatalf("stale interleaving resolve smeared terminal fields onto winner-owned open queue: resolution=%q resolved_by=%q", current.Resolution, derefString(current.ResolvedBy))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving resolve reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving upsert winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving upsert winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter); len(got) != len(seenUpdated)+1 {
		t.Fatalf("interleaving queue-only upsert winner should append exactly one updated runtime row, before=%v after=%v", seenUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("stale queue-only resolve loser should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsInterleavingResolveWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-resolve-interleaving-resolve-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-resolve-interleaving-resolve-winner"
		entityID    = "entity-ops-rollback-failure-resolve-interleaving-resolve-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Resolve Interleaving Resolve Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	updatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenUpdated := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale queue-only resolve should fail after interleaving resolve winner",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale resolve params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsResolveStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsResolveStoreOverride = nil
		hookRan = true
		winnerRaw, err := json.Marshal(workspaceOpsResolveParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			ResolvedBy:       "operator-fresh",
			Resolution:       "winner queue-only resolve should beat stale resolve",
			CurrentRevision:  staleRevision,
			CurrentUpdatedAt: staleUpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving resolve params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), winnerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only resolve winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to fail after interleaving queue-only resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "operator queue item is not open") {
		t.Fatalf("expected invalid params not-open error on stale queue-only resolve after interleaving resolve winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsResolveStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only resolve hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving resolve reject: %v", err)
	}
	if current.Status != "RESOLVED" {
		t.Fatalf("queue status after stale interleaving resolve = %q, want RESOLVED", current.Status)
	}
	if current.Resolution != "winner queue-only resolve should beat stale resolve" || derefString(current.ResolvedBy) != "operator-fresh" {
		t.Fatalf("stale interleaving resolve mutated winner-owned terminal truth = %+v", current)
	}
	if current.Summary != queue.Summary || current.Details != queue.Details {
		t.Fatalf("queue-only resolve self-race should not rewrite manual text: got summary=%q details=%q want summary=%q details=%q", current.Summary, current.Details, queue.Summary, queue.Details)
	}
	if current.AssignedTo != queue.AssignedTo || current.Urgency != queue.Urgency || derefString(current.DueAt) != derefString(queue.DueAt) {
		t.Fatalf("stale interleaving resolve mutated queue-only rollback-failure ownership fields: got %+v want assigned_to=%q urgency=%q due_at=%q", current, queue.AssignedTo, queue.Urgency, derefString(queue.DueAt))
	}
	if current.EscalationCount != queue.EscalationCount {
		t.Fatalf("queue escalation_count after stale interleaving resolve = %d, want %d", current.EscalationCount, queue.EscalationCount)
	}
	if current.Revision != queue.Revision+1 {
		t.Fatalf("queue revision after stale interleaving resolve = %d, want %d", current.Revision, queue.Revision+1)
	}
	if current.UpdatedAt == staleUpdatedAt {
		t.Fatalf("queue updated_at after stale interleaving resolve should advance beyond stale snapshot %q", staleUpdatedAt)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving resolve reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving resolve winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving resolve winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved)+1 {
		t.Fatalf("interleaving queue-only resolve winner should append exactly one resolved runtime row, before=%v after=%v", seenResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter); len(got) != len(seenUpdated) {
		t.Fatalf("queue-only resolve self-race should not append updated runtime rows, before=%v after=%v", seenUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("queue-only resolve self-race should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsInterleavingResolveWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-escalate-interleaving-resolve-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-escalate-interleaving-resolve-winner"
		entityID    = "entity-ops-rollback-failure-escalate-interleaving-resolve-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Escalate Interleaving Resolve Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		EscalatedBy:      "operator-stale",
		Reason:           "stale queue-only handoff should fail after interleaving terminal winner",
		AssignedTo:       "reviewer-stale",
		Urgency:          "CRITICAL",
		DueAt:            "2099-02-01T00:00:00Z",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale escalate params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsEscalateStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsEscalateStoreOverride = nil
		hookRan = true
		resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			ResolvedBy:       "operator-fresh",
			Resolution:       "winner queue-only resolve should beat stale escalate",
			CurrentRevision:  staleRevision,
			CurrentUpdatedAt: staleUpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving resolve params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only resolve winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to fail after interleaving queue-only resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "operator queue item is not open") {
		t.Fatalf("expected invalid params not-open error on stale queue-only escalate after interleaving resolve winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsEscalateStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only resolve hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving escalate reject: %v", err)
	}
	if current.Status != "RESOLVED" {
		t.Fatalf("queue status after stale interleaving escalate = %q, want RESOLVED", current.Status)
	}
	if current.Resolution != "winner queue-only resolve should beat stale escalate" || derefString(current.ResolvedBy) != "operator-fresh" {
		t.Fatalf("stale interleaving escalate mutated winner-owned terminal truth = %+v", current)
	}
	if current.AssignedTo != queue.AssignedTo || current.Urgency != queue.Urgency || derefString(current.DueAt) != derefString(queue.DueAt) {
		t.Fatalf("stale interleaving escalate smeared handoff fields over resolved queue truth: got %+v want assigned_to=%q urgency=%q due_at=%q", current, queue.AssignedTo, queue.Urgency, derefString(queue.DueAt))
	}
	if current.EscalationCount != queue.EscalationCount {
		t.Fatalf("queue escalation_count after stale interleaving escalate = %d, want %d", current.EscalationCount, queue.EscalationCount)
	}
	if current.Revision != queue.Revision+1 {
		t.Fatalf("queue revision after stale interleaving escalate = %d, want %d", current.Revision, queue.Revision+1)
	}
	if current.UpdatedAt == staleUpdatedAt {
		t.Fatalf("queue updated_at after stale interleaving escalate should advance beyond stale snapshot %q", staleUpdatedAt)
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving escalate reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving resolve winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving resolve winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved)+1 {
		t.Fatalf("interleaving queue-only resolve winner should append exactly one resolved runtime row, before=%v after=%v", seenResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("stale queue-only escalate loser should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsInterleavingUpsertWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-escalate-interleaving-upsert-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-escalate-interleaving-upsert-winner"
		entityID    = "entity-ops-rollback-failure-escalate-interleaving-upsert-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Escalate Interleaving Upsert Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	updatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenUpdated := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		EscalatedBy:      "operator-stale",
		Reason:           "stale queue-only handoff should fail after interleaving manual edit winner",
		AssignedTo:       "reviewer-stale",
		Urgency:          "CRITICAL",
		DueAt:            "2099-02-01T00:00:00Z",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale escalate params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsEscalateStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsEscalateStoreOverride = nil
		hookRan = true
		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			QueueKey:         queue.QueueKey,
			QueueType:        queue.QueueType,
			Title:            queue.Title,
			Summary:          "winner queue-only note should beat stale escalate",
			Details:          "winner workspace.ops.upsert should block stale handoff on queue-only rollback-failure carrier",
			AssignedTo:       queue.AssignedTo,
			Urgency:          queue.Urgency,
			SourceKind:       queue.SourceKind,
			SourceID:         queue.SourceID,
			TaskID:           queue.TaskID,
			AgentID:          queue.AgentID,
			CurrentRevision:  staleRevision,
			CurrentUpdatedAt: staleUpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving upsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only upsert winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to fail after interleaving queue-only upsert winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale queue-only escalate after interleaving upsert winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsEscalateStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only upsert hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving escalate reject: %v", err)
	}
	if current.Status != "OPEN" {
		t.Fatalf("queue status after stale interleaving escalate = %q, want OPEN", current.Status)
	}
	if current.Summary != "winner queue-only note should beat stale escalate" || current.Details != "winner workspace.ops.upsert should block stale handoff on queue-only rollback-failure carrier" {
		t.Fatalf("stale interleaving escalate did not preserve winner-owned manual text = %+v", current)
	}
	if current.AssignedTo != queue.AssignedTo || current.Urgency != queue.Urgency || derefString(current.DueAt) != derefString(queue.DueAt) {
		t.Fatalf("stale interleaving escalate mutated queue-only rollback-failure ownership fields: got %+v want assigned_to=%q urgency=%q due_at=%q", current, queue.AssignedTo, queue.Urgency, derefString(queue.DueAt))
	}
	if current.EscalationCount != queue.EscalationCount {
		t.Fatalf("queue escalation_count after stale interleaving escalate = %d, want %d", current.EscalationCount, queue.EscalationCount)
	}
	if current.Revision != queue.Revision+1 {
		t.Fatalf("queue revision after stale interleaving escalate = %d, want %d", current.Revision, queue.Revision+1)
	}
	if current.UpdatedAt == staleUpdatedAt {
		t.Fatalf("queue updated_at after stale interleaving escalate should advance beyond stale snapshot %q", staleUpdatedAt)
	}
	if strings.TrimSpace(current.Resolution) != "" || derefString(current.ResolvedBy) != "" {
		t.Fatalf("stale interleaving escalate smeared terminal fields onto winner-owned open queue: resolution=%q resolved_by=%q", current.Resolution, derefString(current.ResolvedBy))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving escalate reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving upsert winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving upsert winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter); len(got) != len(seenUpdated)+1 {
		t.Fatalf("interleaving queue-only upsert winner should append exactly one updated runtime row, before=%v after=%v", seenUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("stale queue-only escalate loser should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsInterleavingEscalateWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-escalate-interleaving-escalate-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-escalate-interleaving-escalate-winner"
		entityID    = "entity-ops-rollback-failure-escalate-interleaving-escalate-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Escalate Interleaving Escalate Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	updatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenUpdated := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		EscalatedBy:      "operator-stale",
		Reason:           "stale queue-only handoff should fail after interleaving handoff winner",
		AssignedTo:       "reviewer-stale",
		Urgency:          "LOW",
		DueAt:            "2099-03-01T00:00:00Z",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale escalate params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsEscalateStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsEscalateStoreOverride = nil
		hookRan = true
		winnerRaw, err := json.Marshal(workspaceOpsEscalateParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			EscalatedBy:      "operator-fresh",
			Reason:           "winner queue-only handoff should beat stale handoff",
			AssignedTo:       "reviewer-fresh",
			Urgency:          "CRITICAL",
			DueAt:            "2099-02-01T00:00:00Z",
			CurrentRevision:  staleRevision,
			CurrentUpdatedAt: staleUpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving escalate params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), winnerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only escalate winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to fail after interleaving queue-only escalate winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale queue-only escalate after interleaving escalate winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsEscalateStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only escalate hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving escalate reject: %v", err)
	}
	if current.Status != "OPEN" {
		t.Fatalf("queue status after stale interleaving escalate = %q, want OPEN", current.Status)
	}
	if current.Summary != queue.Summary || current.Details != queue.Details {
		t.Fatalf("queue-only escalate self-race should not rewrite manual text: got summary=%q details=%q want summary=%q details=%q", current.Summary, current.Details, queue.Summary, queue.Details)
	}
	if current.AssignedTo != "reviewer-fresh" || current.Urgency != "CRITICAL" || derefString(current.DueAt) != "2099-02-01T00:00:00Z" {
		t.Fatalf("stale interleaving escalate did not preserve winner-owned handoff fields = %+v", current)
	}
	if current.EscalationCount != queue.EscalationCount+1 {
		t.Fatalf("queue escalation_count after stale interleaving escalate = %d, want %d", current.EscalationCount, queue.EscalationCount+1)
	}
	if current.Revision != queue.Revision+1 {
		t.Fatalf("queue revision after stale interleaving escalate = %d, want %d", current.Revision, queue.Revision+1)
	}
	if current.UpdatedAt == staleUpdatedAt {
		t.Fatalf("queue updated_at after stale interleaving escalate should advance beyond stale snapshot %q", staleUpdatedAt)
	}
	if strings.TrimSpace(current.Resolution) != "" || derefString(current.ResolvedBy) != "" {
		t.Fatalf("stale interleaving escalate smeared terminal fields onto winner-owned open queue: resolution=%q resolved_by=%q", current.Resolution, derefString(current.ResolvedBy))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving escalate reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving escalate winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving escalate winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated)+1 {
		t.Fatalf("interleaving queue-only escalate winner should append exactly one escalated runtime row, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter); len(got) != len(seenUpdated) {
		t.Fatalf("queue-only escalate self-race should not append updated runtime rows, before=%v after=%v", seenUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("queue-only escalate self-race should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsUpsertRejectsInterleavingUpsertWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-upsert-interleaving-upsert-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-upsert-interleaving-upsert-winner"
		entityID    = "entity-ops-rollback-failure-upsert-interleaving-upsert-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Upsert Interleaving Upsert Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	updatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenUpdated := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		QueueKey:         queue.QueueKey,
		QueueType:        queue.QueueType,
		Title:            queue.Title,
		Summary:          "stale queue-only note should lose to interleaving upsert winner",
		Details:          "stale workspace.ops.upsert on queue-only rollback-failure carrier should fail after interleaving manual edit winner",
		AssignedTo:       queue.AssignedTo,
		Urgency:          "LOW",
		SourceKind:       queue.SourceKind,
		SourceID:         queue.SourceID,
		TaskID:           queue.TaskID,
		AgentID:          queue.AgentID,
		DueAt:            "2099-03-01T00:00:00Z",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale upsert params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		hookRan = true
		winnerRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			QueueKey:         queue.QueueKey,
			QueueType:        queue.QueueType,
			Title:            queue.Title,
			Summary:          "winner queue-only note should beat stale upsert",
			Details:          "winner workspace.ops.upsert should keep queue-only rollback-failure truth canonical",
			AssignedTo:       queue.AssignedTo,
			Urgency:          "CRITICAL",
			SourceKind:       queue.SourceKind,
			SourceID:         queue.SourceID,
			TaskID:           queue.TaskID,
			AgentID:          queue.AgentID,
			DueAt:            "2099-02-01T00:00:00Z",
			CurrentRevision:  staleRevision,
			CurrentUpdatedAt: staleUpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving upsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), winnerRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only upsert winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to fail after interleaving queue-only upsert winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale queue-only upsert after interleaving upsert winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsUpsertStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only upsert hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving upsert reject: %v", err)
	}
	if current.Status != "OPEN" {
		t.Fatalf("queue status after stale interleaving upsert = %q, want OPEN", current.Status)
	}
	if current.Summary != "winner queue-only note should beat stale upsert" || current.Details != "winner workspace.ops.upsert should keep queue-only rollback-failure truth canonical" {
		t.Fatalf("stale interleaving upsert did not preserve winner-owned manual text = %+v", current)
	}
	if current.AssignedTo != queue.AssignedTo || current.Urgency != "CRITICAL" || derefString(current.DueAt) != "2099-02-01T00:00:00Z" {
		t.Fatalf("stale interleaving upsert did not preserve winner-owned queue fields = %+v", current)
	}
	if current.Revision != queue.Revision+1 {
		t.Fatalf("queue revision after stale interleaving upsert = %d, want %d", current.Revision, queue.Revision+1)
	}
	if current.UpdatedAt == staleUpdatedAt {
		t.Fatalf("queue updated_at after stale interleaving upsert should advance beyond stale snapshot %q", staleUpdatedAt)
	}
	if strings.TrimSpace(current.Resolution) != "" || derefString(current.ResolvedBy) != "" {
		t.Fatalf("stale interleaving upsert smeared terminal fields onto winner-owned open queue: resolution=%q resolved_by=%q", current.Resolution, derefString(current.ResolvedBy))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving upsert reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving upsert winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving upsert winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter); len(got) != len(seenUpdated)+1 {
		t.Fatalf("interleaving queue-only upsert winner should append exactly one updated runtime row, before=%v after=%v", seenUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("queue-only upsert self-race should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("queue-only upsert self-race should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
}

func TestWorkspaceOpsUpsertRejectsInterleavingResolveWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-upsert-interleaving-resolve-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-upsert-interleaving-resolve-winner"
		entityID    = "entity-ops-rollback-failure-upsert-interleaving-resolve-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Upsert Interleaving Resolve Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)

	updatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenUpdated := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		QueueKey:         queue.QueueKey,
		QueueType:        queue.QueueType,
		Title:            queue.Title,
		Summary:          "stale note should lose to queue-only resolve winner",
		Details:          "stale upsert should not overwrite terminal queue-only rollback-failure truth",
		AssignedTo:       queue.AssignedTo,
		Urgency:          queue.Urgency,
		SourceKind:       queue.SourceKind,
		SourceID:         queue.SourceID,
		CurrentRevision:  queue.Revision,
		CurrentUpdatedAt: queue.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale upsert params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		hookRan = true
		resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			ResolvedBy:       "operator-fresh",
			Resolution:       "winner queue-only resolve should beat stale upsert",
			CurrentRevision:  queue.Revision,
			CurrentUpdatedAt: queue.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving resolve params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only resolve winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to fail after interleaving queue-only resolve winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale queue-only upsert after interleaving resolve winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsUpsertStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only resolve hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving upsert reject: %v", err)
	}
	if current.Status != "RESOLVED" {
		t.Fatalf("queue status after stale interleaving upsert = %q, want RESOLVED", current.Status)
	}
	if current.Resolution != "winner queue-only resolve should beat stale upsert" || derefString(current.ResolvedBy) != "operator-fresh" {
		t.Fatalf("stale interleaving upsert mutated winner-owned terminal truth = %+v", current)
	}
	if current.Summary != queue.Summary || current.Details != queue.Details {
		t.Fatalf("stale interleaving upsert smeared manual text onto resolved queue truth: got summary=%q details=%q want summary=%q details=%q", current.Summary, current.Details, queue.Summary, queue.Details)
	}
	if current.AssignedTo != queue.AssignedTo || current.Urgency != queue.Urgency || derefString(current.DueAt) != derefString(queue.DueAt) {
		t.Fatalf("stale interleaving upsert mutated queue-only rollback-failure ownership fields: got %+v want assigned_to=%q urgency=%q due_at=%q", current, queue.AssignedTo, queue.Urgency, derefString(queue.DueAt))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving upsert reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving resolve winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving resolve winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter); len(got) != len(seenUpdated) {
		t.Fatalf("stale queue-only upsert loser should not append updated runtime rows, before=%v after=%v", seenUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved)+1 {
		t.Fatalf("interleaving queue-only resolve winner should append exactly one resolved runtime row, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsUpsertRejectsInterleavingEscalateWinnerOnQueueOnlyRollbackFailureCarrier(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-upsert-interleaving-escalate-winner"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-upsert-interleaving-escalate-winner"
		entityID    = "entity-ops-rollback-failure-upsert-interleaving-escalate-winner"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Upsert Interleaving Escalate Winner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)

	updatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenUpdated := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		QueueKey:         queue.QueueKey,
		QueueType:        queue.QueueType,
		Title:            queue.Title,
		Summary:          "stale queue-only note should lose to interleaving handoff winner",
		Details:          "stale workspace.ops.upsert on queue-only rollback-failure carrier should fail after interleaving handoff",
		AssignedTo:       queue.AssignedTo,
		Urgency:          queue.Urgency,
		SourceKind:       queue.SourceKind,
		SourceID:         queue.SourceID,
		TaskID:           queue.TaskID,
		AgentID:          queue.AgentID,
		CurrentRevision:  queue.Revision,
		CurrentUpdatedAt: queue.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale upsert params: %v", err)
	}

	var (
		hookRan bool
		hookErr error
	)
	h.beforeWorkspaceOpsUpsertStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsUpsertStoreOverride = nil
		hookRan = true
		escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
			WorkspaceID:      workspaceID,
			QueueID:          queue.QueueID,
			EscalatedBy:      "operator-fresh",
			Reason:           "winner queue-only handoff should beat stale upsert",
			AssignedTo:       "reviewer-fresh",
			Urgency:          "CRITICAL",
			DueAt:            "2099-02-01T00:00:00Z",
			CurrentRevision:  queue.Revision,
			CurrentUpdatedAt: queue.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving escalate params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving queue-only escalate winner rpc error: %+v", rpcErr)
		}
	}

	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsUpsert to fail after interleaving queue-only escalate winner")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale queue-only upsert after interleaving escalate winner, got %+v", rpcErr)
	}
	if !hookRan {
		t.Fatal("expected beforeWorkspaceOpsUpsertStoreOverride hook to run")
	}
	if hookErr != nil {
		t.Fatalf("interleaving queue-only escalate hook: %v", hookErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale interleaving upsert reject: %v", err)
	}
	if current.Status != "OPEN" {
		t.Fatalf("queue status after stale interleaving upsert = %q, want OPEN", current.Status)
	}
	if current.AssignedTo != "reviewer-fresh" || current.Urgency != "CRITICAL" || derefString(current.DueAt) != "2099-02-01T00:00:00Z" {
		t.Fatalf("stale interleaving upsert mutated winner-owned handoff fields = %+v", current)
	}
	if current.EscalationCount != queue.EscalationCount+1 {
		t.Fatalf("queue escalation_count after stale interleaving upsert = %d, want %d", current.EscalationCount, queue.EscalationCount+1)
	}
	if current.Summary == "stale queue-only note should lose to interleaving handoff winner" || strings.Contains(current.Details, "stale workspace.ops.upsert on queue-only rollback-failure carrier should fail after interleaving handoff") {
		t.Fatalf("stale interleaving upsert smeared manual text onto winner-owned open queue truth = %+v", current)
	}
	if strings.TrimSpace(current.Resolution) != "" || derefString(current.ResolvedBy) != "" {
		t.Fatalf("stale interleaving upsert smeared terminal fields onto open queue truth: resolution=%q resolved_by=%q", current.Resolution, derefString(current.ResolvedBy))
	}
	payload, err := actionCreateDecodeRollbackFailurePayload(current.PayloadJSON)
	if err != nil {
		t.Fatalf("decode rollback-failure payload after stale interleaving upsert reject: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.EntityID != entityID {
		t.Fatalf("queue-only rollback-failure payload drifted after interleaving handoff winner: %+v", payload)
	}
	if payload.FollowupActionID != "" || payload.FollowupActionQueueKey != "" || payload.FollowupActionStatus != "" || payload.LastFailedFollowupActionID != "" || payload.LastFailedFollowupActionStatus != "" {
		t.Fatalf("queue-only rollback-failure payload should remain queue-only after interleaving handoff winner: %+v", payload)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated)+1 {
		t.Fatalf("interleaving queue-only escalate winner should append exactly one escalated runtime row, before=%v after=%v", seenEscalated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, updatedFilter); len(got) != len(seenUpdated) {
		t.Fatalf("stale queue-only upsert loser should not append updated runtime rows, before=%v after=%v", seenUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("stale queue-only upsert loser should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsEscalateRejectsStaleQueueOnlyRollbackFailureSnapshot(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-rollback-failure-queue-only-escalate-stale"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "rsp_anomaly_list:entity-ops-rollback-failure-escalate-stale"
		entityID    = "entity-ops-rollback-failure-escalate-stale"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Queue-only Rollback Failure Escalate Stale",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue := seedQueueOnlyRollbackFailureQueueForTest(t, ctx, store, workspaceID, queueKey, entityID)
	staleRevision := queue.Revision
	staleUpdatedAt := queue.UpdatedAt

	refreshed, _, _, err := store.EscalateOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		EscalatedBy: "operator-fresh",
		Reason:      "fresh escalation before stale escalate",
		AssignedTo:  "reviewer-fresh",
		Urgency:     "CRITICAL",
		DueAt:       "2099-02-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("fresh escalate before stale escalate: %v", err)
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	}
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		EscalatedBy:      "operator-stale",
		Reason:           "stale escalate should fail",
		AssignedTo:       "reviewer-stale",
		Urgency:          "LOW",
		DueAt:            "2099-03-01T00:00:00Z",
		CurrentRevision:  staleRevision,
		CurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal stale escalate params: %v", err)
	}
	blindEscalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		EscalatedBy: "operator-blind",
		Reason:      "blind escalate should fail",
		AssignedTo:  "reviewer-blind",
		Urgency:     "LOW",
		DueAt:       "2099-04-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal blind escalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), blindEscalateRaw); rpcErr == nil {
		t.Fatal("expected blind workspaceOpsEscalate to reject missing queue base-version after revision advanced")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "current_revision") {
		t.Fatalf("expected invalid params current_revision guidance on blind escalate, got %+v", rpcErr)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to reject outdated queue revision")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale escalate, got %+v", rpcErr)
	}

	current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("get queue after stale escalate reject: %v", err)
	}
	if current.AssignedTo != refreshed.AssignedTo || current.Urgency != refreshed.Urgency || current.UpdatedAt != refreshed.UpdatedAt || derefString(current.DueAt) != derefString(refreshed.DueAt) {
		t.Fatalf("stale escalate mutated refreshed queue state: got %+v want assigned_to=%q urgency=%q due_at=%q updated_at=%q", current, refreshed.AssignedTo, refreshed.Urgency, derefString(refreshed.DueAt), refreshed.UpdatedAt)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("stale escalate should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}
}

func TestWorkspaceOpsEscalateRejectsInterleavingRetryCreateWinnerOnReopenedFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-interleaving-retry-create-winner"
		taskID      = "task-ops-escalate-interleaving-retry-create-winner"
		agentID     = "agent-ops-escalate-interleaving-retry-create-winner"
		repairID    = "tens-repair-ops-escalate-interleaving-retry-create-winner"
		runID       = "run-ops-escalate-interleaving-retry-create-winner"
	)

	firstActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier late fail reopens retry path before stale handoff",
		Summary:     "seed reopened retry carrier before workspace.ops.escalate races a retry winner",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
			"action_id":       firstActionID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	sourceQueueBeforeCreate, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before retry create): %v", err)
	}

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	escalatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenEscalated := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter)

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		EscalatedBy:      "lead-stale",
		Reason:           "stale handoff should lose to retry winner",
		AssignedTo:       "reviewer-b",
		Urgency:          "LOW",
		DueAt:            "2099-06-01T00:00:00Z",
		CurrentRevision:  sourceQueueBeforeCreate.Revision,
		CurrentUpdatedAt: sourceQueueBeforeCreate.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}

	var (
		hookErr        error
		winnerActionID string
	)
	h.beforeWorkspaceOpsEscalateStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsEscalateStoreOverride = nil
		createAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
		if rpcErr != nil {
			hookErr = fmt.Errorf("interleaving retry actionCreate rpc error: %+v", rpcErr)
			return
		}
		createResp, ok := createAny.(map[string]any)
		if !ok {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response type %T", createAny)
			return
		}
		winnerActionID, _ = createResp["action_id"].(string)
		if strings.TrimSpace(winnerActionID) == "" {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response %+v", createResp)
		}
	}

	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsEscalate to fail after interleaving retry winner linked the reopened source queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale escalate after retry create winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving retry create hook: %v", hookErr)
	}
	if winnerActionID == "" || winnerActionID == firstActionID {
		t.Fatalf("unexpected winner action id %q after interleaving retry create", winnerActionID)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated)+1 {
		t.Fatalf("interleaving retry create winner should append exactly one action.created row, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving retry create winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, escalatedFilter); len(got) != len(seenEscalated) {
		t.Fatalf("stale interleaving escalate loser should not append escalated runtime rows, before=%v after=%v", seenEscalated, got)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale interleaving escalate): %v", err)
	}
	if currentQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("stale interleaving escalate mutated reopened retry holder to %q, want reviewer-a", currentQueue.AssignedTo)
	}
	action, err := store.GetHumanAction(ctx, winnerActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(winner): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending {
		t.Fatalf("stale interleaving escalate mutated retry winner action truth = %+v, want assigned_to reviewer-a and pending status", action)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, winnerActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestWorkspaceOpsResolveRejectsLinkedPendingRebaseActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-linked-rebase-pending"
		taskID      = "task-ops-resolve-linked-rebase-pending"
		agentID     = "agent-ops-resolve-linked-rebase-pending"
		queueKey    = "tension_rebase_followup:tens-repair-ops-resolve-linked-rebase-pending"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-ops-resolve-linked-rebase-pending",
		"fork_tension_id":     "tens-fork-ops-resolve-linked-rebase-pending",
		"repair_tension_id":   "tens-repair-ops-resolve-linked-rebase-pending",
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for manual resolve guard",
		Details:           "Pending rebase follow-up",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-resolve-linked-rebase-pending",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase source queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		ResolvedBy:  "operator-a",
		Resolution:  "manual resolve should be rejected while action pending",
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsResolve to reject linked pending rebase action queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use action.resolve or workspace.ops.escalate") {
		t.Fatalf("expected linked pending action guard on workspaceOpsResolve, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("rebase source queue status after rejected resolve = %q, want OPEN", currentQueue.Status)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(current): %v", err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("linked action status after rejected resolve = %q, want PENDING", action.Status)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("rejected linked rebase queue resolve should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsInterleavingRetryCreateWinnerOnReopenedFollowup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-interleaving-retry-create-winner"
		taskID      = "task-ops-resolve-interleaving-retry-create-winner"
		agentID     = "agent-ops-resolve-interleaving-retry-create-winner"
		repairID    = "tens-repair-ops-resolve-interleaving-retry-create-winner"
		runID       = "run-ops-resolve-interleaving-retry-create-winner"
	)

	firstActionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	createExecutionRunForControlPlaneTest(t, ctx, h, workspaceID, taskID, agentID, runID)

	stepRaw, err := json.Marshal(workspaceExecutionStepWriteParams{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Verifier late fail reopens retry path before stale resolve",
		Summary:     "seed reopened retry carrier before workspace.ops.resolve races a retry winner",
		Status:      "FAILED",
		SortOrder:   1,
		Verification: map[string]any{
			"source_queue_id": sourceQueueID,
			"action_id":       firstActionID,
		},
	})
	if err != nil {
		t.Fatalf("marshal failed verify step params: %v", err)
	}
	if _, rpcErr := h.workspaceExecutionStepWrite(ctx, stepRaw); rpcErr != nil {
		t.Fatalf("workspaceExecutionStepWrite rpc error: %+v", rpcErr)
	}

	sourceQueueBeforeCreate, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before retry create): %v", err)
	}

	actionCreatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       20,
	}
	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenActionCreated := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter)
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale resolve should lose to retry winner",
		CurrentRevision:  sourceQueueBeforeCreate.Revision,
		CurrentUpdatedAt: sourceQueueBeforeCreate.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}

	var (
		hookErr        error
		winnerActionID string
	)
	h.beforeWorkspaceOpsResolveStoreOverride = func(ctx context.Context) {
		h.beforeWorkspaceOpsResolveStoreOverride = nil
		createAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
		if rpcErr != nil {
			hookErr = fmt.Errorf("interleaving retry actionCreate rpc error: %+v", rpcErr)
			return
		}
		createResp, ok := createAny.(map[string]any)
		if !ok {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response type %T", createAny)
			return
		}
		winnerActionID, _ = createResp["action_id"].(string)
		if strings.TrimSpace(winnerActionID) == "" {
			hookErr = fmt.Errorf("unexpected interleaving retry actionCreate response %+v", createResp)
		}
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to fail after interleaving retry winner linked the reopened source queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale resolve after retry create winner, got %+v", rpcErr)
	}
	if hookErr != nil {
		t.Fatalf("interleaving retry create hook: %v", hookErr)
	}
	if winnerActionID == "" || winnerActionID == firstActionID {
		t.Fatalf("unexpected winner action id %q after interleaving retry create", winnerActionID)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, actionCreatedFilter); len(got) != len(seenActionCreated)+1 {
		t.Fatalf("interleaving retry create winner should append exactly one action.created row, before=%v after=%v", seenActionCreated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("interleaving retry create winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("stale resolve loser should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, winnerActionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)
}

func TestWorkspaceOpsResolveRejectsStaleSnapshotAfterLinkedRebaseFailedResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-interleaving-failed-resolve-winner"
		taskID      = "task-ops-resolve-interleaving-failed-resolve-winner"
		agentID     = "agent-ops-resolve-interleaving-failed-resolve-winner"
		repairID    = "tens-repair-ops-resolve-interleaving-failed-resolve-winner"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before failed resolve): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)

	resolveQueueRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale manual close should fail after failed resolve",
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusFailed,
		Comment:    "winner failed resolve should beat stale manual queue close",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveQueueRaw); rpcErr == nil {
		t.Fatal("expected stale workspaceOpsResolve to reject outdated queue revision after failed resolve")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "updated concurrently") {
		t.Fatalf("expected invalid params updated concurrently on stale resolve after failed resolve, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after stale resolve): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("stale resolve mutated source queue status to %q, want OPEN", currentQueue.Status)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.ActionID != "" || currentPayload.ActionQueueKey != "" || currentPayload.ActionStatus != "" || currentPayload.ActionAssignedTo != "" {
		t.Fatalf("current source queue should clear active action linkage after failed resolve, got %+v", currentPayload)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateClaimed || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("current source queue workflow after stale resolve = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	}
	if currentPayload.LastFailedActionID != actionID || currentPayload.LastFailedStatus != humanActionStatusFailed {
		t.Fatalf("current source queue failed lineage after stale resolve = action=%q status=%q, want (%q,%q)", currentPayload.LastFailedActionID, currentPayload.LastFailedStatus, actionID, humanActionStatusFailed)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusFailed {
		t.Fatalf("action mutated after stale resolve = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "RESOLVED" {
		t.Fatalf("stale resolve mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("failed resolve winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved)+1 {
		t.Fatalf("failed resolve winner should append exactly one action queue resolved row, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("stale resolve after failed resolve should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsLinkedPendingActionQueueAfterLinkedRebaseActionPause(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-stale-after-rebase-pause"
		taskID      = "task-ops-resolve-stale-after-rebase-pause"
		agentID     = "agent-ops-resolve-stale-after-rebase-pause"
		repairID    = "tens-repair-ops-resolve-stale-after-rebase-pause"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before pause): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)

	resolveQueueRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "stale manual close should fail after pause",
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}

	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "winner pause should beat stale manual queue close",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveQueueRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsResolve to reject linked pending action after pause")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "linked to pending action") {
		t.Fatalf("expected invalid params linked to pending action on resolve after pause, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after rejected resolve): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("rejected resolve mutated source queue status to %q, want OPEN", currentQueue.Status)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateClaimed || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("current source queue workflow after rejected resolve = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionQueueKey == "" || currentPayload.ActionStatus != humanActionStatusPending {
		t.Fatalf("current source queue active action linkage after rejected resolve = %+v", currentPayload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending {
		t.Fatalf("action mutated after rejected resolve = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "OPEN" {
		t.Fatalf("rejected resolve mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated)+1 {
		t.Fatalf("pause winner should append exactly one source queue updated row, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("rejected resolve after pause should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("rejected resolve after pause should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsLinkedPendingActionQueueAfterLinkedRebaseActionStart(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-linked-pending-after-rebase-start"
		taskID      = "task-ops-resolve-linked-pending-after-rebase-start"
		agentID     = "agent-ops-resolve-linked-pending-after-rebase-start"
		repairID    = "tens-repair-ops-resolve-linked-pending-after-rebase-start"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueueBefore, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue before start): %v", err)
	}
	actionQueueBefore := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	sourceUpdatedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	actionQueueResolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueueBefore.QueueID,
		Limit:       20,
	}
	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueueID,
		Limit:       20,
	}
	seenSourceUpdated := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter)
	seenActionQueueResolved := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter)
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)

	resolveQueueRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueueBefore.QueueID,
		ResolvedBy:       "operator-stale",
		Resolution:       "manual close should fail after action start",
		CurrentRevision:  sourceQueueBefore.Revision,
		CurrentUpdatedAt: sourceQueueBefore.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}

	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveQueueRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsResolve to reject linked pending action after start")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "linked to pending action") {
		t.Fatalf("expected invalid params linked to pending action on resolve after start, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue after rejected resolve): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("rejected resolve mutated source queue status to %q, want OPEN", currentQueue.Status)
	}
	currentPayload, err := actionCreateDecodeQueuePayload(currentQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(current source queue): %v", err)
	}
	if currentPayload.RebaseWorkflowState != rebaseWorkflowStateInProgress || currentPayload.RebaseWorkflowStep != rebaseWorkflowStepOperatorClaimed {
		t.Fatalf("current source queue workflow after rejected resolve = (%q,%q), want (%q,%q)", currentPayload.RebaseWorkflowState, currentPayload.RebaseWorkflowStep, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	}
	if currentPayload.ActionID != actionID || currentPayload.ActionQueueKey == "" || currentPayload.ActionStatus != humanActionStatusPending {
		t.Fatalf("current source queue active action linkage after rejected resolve = %+v", currentPayload)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(action): %v", err)
	}
	if action.AssignedTo != "reviewer-a" || action.Status != humanActionStatusPending {
		t.Fatalf("action mutated after rejected resolve = %+v", action)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.AssignedTo != actionQueueBefore.AssignedTo || actionQueue.Status != "OPEN" {
		t.Fatalf("rejected resolve mutated action queue state = %+v", actionQueue)
	}

	if got := snapshotRuntimeEventIDs(t, ctx, store, sourceUpdatedFilter); len(got) != len(seenSourceUpdated) {
		t.Fatalf("rejected resolve after start should not append source queue updated rows, before=%v after=%v", seenSourceUpdated, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, actionQueueResolvedFilter); len(got) != len(seenActionQueueResolved) {
		t.Fatalf("rejected resolve after start should not append action queue resolved rows, before=%v after=%v", seenActionQueueResolved, got)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("rejected resolve after start should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsLinkedPendingRollbackFailureActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-linked-rollback-pending"
		taskID      = "task-ops-resolve-linked-rollback-pending"
		agentID     = "agent-ops-resolve-linked-rollback-pending"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "repair-ops-resolve-linked-rollback-pending"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-ops-resolve-linked-rollback-pending")

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       10,
	}
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
		ResolvedBy:  "operator-a",
		Resolution:  "manual resolve should be rejected while rollback action pending",
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsResolve to reject linked pending rollback-failure action queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use action.resolve or workspace.ops.escalate") {
		t.Fatalf("expected linked pending rollback-failure guard on workspaceOpsResolve, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("rollback-failure source queue status after rejected resolve = %q, want OPEN", currentQueue.Status)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(current): %v", err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("rollback-failure linked action status after rejected resolve = %q, want PENDING", action.Status)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("rejected linked rollback-failure queue resolve should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsResolveRejectsPendingStandaloneHumanActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-resolve-standalone-action-pending"
		taskID      = "task-ops-resolve-standalone-action-pending"
		agentID     = "agent-ops-resolve-standalone-action-pending"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Review standalone operator action",
		Description: "Manual human action without rebase linkage.",
		Blocking:    boolPtr(true),
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	resolvedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    actionQueue.QueueID,
		Limit:       10,
	}
	seenResolved := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter)
	resolveRaw, err := json.Marshal(workspaceOpsResolveParams{
		WorkspaceID: workspaceID,
		QueueID:     actionQueue.QueueID,
		ResolvedBy:  "operator-a",
		Resolution:  "manual resolve should be rejected while action pending",
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsResolve params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsResolve(testAuthContext(workspaceID, "system", "tests"), resolveRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsResolve to reject pending standalone action queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use action.resolve") {
		t.Fatalf("expected standalone action queue guard on workspaceOpsResolve, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}
	if currentQueue.Status != "OPEN" {
		t.Fatalf("standalone action queue status after rejected resolve = %q, want OPEN", currentQueue.Status)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(current): %v", err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("standalone action status after rejected resolve = %q, want PENDING", action.Status)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, resolvedFilter); len(got) != len(seenResolved) {
		t.Fatalf("rejected standalone action queue resolve should not append resolved runtime rows, before=%v after=%v", seenResolved, got)
	}
}

func TestWorkspaceOpsEscalateReassignsStandaloneHumanActionQueueAndRejectsStaleResolve(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-standalone-action"
		taskID      = "task-ops-escalate-standalone-action"
		agentID     = "agent-ops-escalate-standalone-action"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Review standalone escalation action",
		Description: "Manual human action for direct queue handoff.",
		Blocking:    boolPtr(true),
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, actionQueue.QueueID, "")

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          actionQueue.QueueID,
		EscalatedBy:      "lead-reviewer",
		AssignedTo:       "reviewer-b",
		Reason:           "handoff standalone action",
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	escalateAny, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}
	escalateResp, ok := escalateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspaceOpsEscalate response type %T", escalateAny)
	}
	item, ok := escalateResp["item"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected workspaceOpsEscalate item %+v", escalateResp)
	}
	if item.AssignedTo != "reviewer-b" {
		t.Fatalf("escalated standalone action queue assigned_to = %q, want reviewer-b", item.AssignedTo)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.AssignedTo != "reviewer-b" {
		t.Fatalf("standalone action assigned_to after handoff = %q, want reviewer-b", action.AssignedTo)
	}
	escalatedQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(escalated): %v", err)
	}
	queuePayload, err := actionCreateDecodeQueuePayload(escalatedQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(escalated queue): %v", err)
	}
	if got := actionCreateQueuePayloadString(queuePayload, "action_assigned_to"); got != "reviewer-b" {
		t.Fatalf("standalone action queue payload action_assigned_to = %q, want reviewer-b", got)
	}

	staleResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "stale holder should not resolve after handoff",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal stale actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, staleResolveRaw); rpcErr == nil {
		t.Fatal("expected stale standalone action holder to be rejected after handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "human action is assigned to reviewer-b") {
		t.Fatalf("expected standalone action authority guard after handoff, got %+v", rpcErr)
	}

	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "new holder resolves after handoff",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error after standalone handoff: %+v", rpcErr)
	}
}

func TestWorkspaceOpsEscalateRejectsLinkedHumanActionQueueDirectHandoff(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-escalate-linked-action-queue"
		taskID      = "task-ops-escalate-linked-action-queue"
		agentID     = "agent-ops-escalate-linked-action-queue"
		queueKey    = "tension_rebase_followup:ops-escalate-linked-action-queue"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(model.RebaseFollowupPayload{
		CoalitionID:     "coal-ops-escalate-linked-action-queue",
		RepairTensionID: "tens-repair-ops-escalate-linked-action-queue",
		NextAction:      model.RebaseNextActionAttempt,
		RebasePlanClass: "trim_redundancy",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Direct action queue escalation should be rejected",
		Details:           "Coalition ID: coal-ops-escalate-linked-action-queue\nRepair tension: tens-repair-ops-escalate-linked-action-queue\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-escalate-linked-action-queue",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase source queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID: workspaceID,
		QueueID:     actionQueue.QueueID,
		EscalatedBy: "lead-reviewer",
		AssignedTo:  "reviewer-b",
		Reason:      "direct linked action queue handoff should fail closed",
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsEscalate to reject direct linked action queue handoff")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use workspace.ops.escalate on the source queue") {
		t.Fatalf("expected linked action queue handoff guidance, got %+v", rpcErr)
	}

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.AssignedTo != "reviewer-a" {
		t.Fatalf("linked action assigned_to after rejected direct handoff = %q, want reviewer-a", action.AssignedTo)
	}
	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue): %v", err)
	}
	if currentActionQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("linked action queue assigned_to after rejected direct handoff = %q, want reviewer-a", currentActionQueue.AssignedTo)
	}
	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue): %v", err)
	}
	if currentSourceQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("linked source queue assigned_to after rejected direct handoff = %q, want reviewer-a", currentSourceQueue.AssignedTo)
	}
}

func TestWorkspaceOpsUpsertRejectsAssignedToMutationForLinkedRebaseQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-linked-rebase-holder"
		taskID      = "task-ops-upsert-linked-rebase-holder"
		agentID     = "agent-ops-upsert-linked-rebase-holder"
		queueKey    = "tension_rebase_followup:ops-upsert-linked-rebase-holder"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(model.RebaseFollowupPayload{
		CoalitionID:     "coal-ops-upsert-linked-rebase-holder",
		RepairTensionID: "tens-repair-ops-upsert-linked-rebase-holder",
		NextAction:      model.RebaseNextActionAttempt,
		RebasePlanClass: "trim_redundancy",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Direct holder mutation via upsert should fail",
		Details:           "Coalition ID: coal-ops-upsert-linked-rebase-holder\nRepair tension: tens-repair-ops-upsert-linked-rebase-holder\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-upsert-linked-rebase-holder",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase source queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    sourceQueue.QueueKey,
		QueueType:   sourceQueue.QueueType,
		Title:       sourceQueue.Title,
		Summary:     sourceQueue.Summary,
		Details:     sourceQueue.Details,
		AssignedTo:  "reviewer-b",
		Urgency:     sourceQueue.Urgency,
		SourceKind:  sourceQueue.SourceKind,
		SourceID:    sourceQueue.SourceID,
		TaskID:      sourceQueue.TaskID,
		AgentID:     sourceQueue.AgentID,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject linked rebase holder mutation")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use workspace.ops.escalate") {
		t.Fatalf("expected linked rebase holder mutation guidance, got %+v", rpcErr)
	}

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue): %v", err)
	}
	if currentSourceQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("linked rebase source queue assigned_to after rejected upsert = %q, want reviewer-a", currentSourceQueue.AssignedTo)
	}
	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue): %v", err)
	}
	if currentActionQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("linked rebase action queue assigned_to after rejected upsert = %q, want reviewer-a", currentActionQueue.AssignedTo)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.AssignedTo != "reviewer-a" {
		t.Fatalf("linked rebase action assigned_to after rejected upsert = %q, want reviewer-a", action.AssignedTo)
	}
}

func TestWorkspaceOpsUpsertRejectsAssignedToMutationForRollbackFailureQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-rollback-holder"
		taskID      = "task-ops-upsert-rollback-holder"
		agentID     = "agent-ops-upsert-rollback-holder"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-upsert-rollback-holder"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-ops-upsert-rollback-holder")

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    sourceQueue.QueueKey,
		QueueType:   sourceQueue.QueueType,
		Title:       sourceQueue.Title,
		Summary:     sourceQueue.Summary,
		Details:     sourceQueue.Details,
		AssignedTo:  "reviewer-b",
		Urgency:     sourceQueue.Urgency,
		SourceKind:  sourceQueue.SourceKind,
		SourceID:    sourceQueue.SourceID,
		TaskID:      sourceQueue.TaskID,
		AgentID:     sourceQueue.AgentID,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject rollback-failure holder mutation")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use workspace.ops.escalate") {
		t.Fatalf("expected rollback-failure holder mutation guidance, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}
	if currentQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("rollback-failure queue assigned_to after rejected upsert = %q, want reviewer-a", currentQueue.AssignedTo)
	}
}

func TestWorkspaceOpsUpsertRejectsAssignedToMutationForStandaloneActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-standalone-action-holder"
		taskID      = "task-ops-upsert-standalone-action-holder"
		agentID     = "agent-ops-upsert-standalone-action-holder"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Standalone action upsert holder guard",
		Description: "Upsert should not reassign a pending standalone action.",
		Blocking:    boolPtr(true),
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    actionQueue.QueueKey,
		QueueType:   actionQueue.QueueType,
		Title:       actionQueue.Title,
		Summary:     actionQueue.Summary,
		Details:     actionQueue.Details,
		AssignedTo:  "reviewer-b",
		Urgency:     actionQueue.Urgency,
		SourceKind:  actionQueue.SourceKind,
		SourceID:    actionQueue.SourceID,
		TaskID:      actionQueue.TaskID,
		AgentID:     actionQueue.AgentID,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject standalone action holder mutation")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use workspace.ops.escalate") {
		t.Fatalf("expected standalone action holder mutation guidance, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}
	if currentQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("standalone action queue assigned_to after rejected upsert = %q, want reviewer-a", currentQueue.AssignedTo)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.AssignedTo != "reviewer-a" {
		t.Fatalf("standalone action assigned_to after rejected upsert = %q, want reviewer-a", action.AssignedTo)
	}
}

func TestWorkspaceOpsUpsertRejectsClearingAssignedToForLinkedRebaseQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-linked-rebase-clear-holder"
		taskID      = "task-ops-upsert-linked-rebase-clear-holder"
		agentID     = "agent-ops-upsert-linked-rebase-clear-holder"
		queueKey    = "tension_rebase_followup:ops-upsert-linked-rebase-clear-holder"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(model.RebaseFollowupPayload{
		CoalitionID:     "coal-ops-upsert-linked-rebase-clear-holder",
		RepairTensionID: "tens-repair-ops-upsert-linked-rebase-clear-holder",
		NextAction:      model.RebaseNextActionAttempt,
		RebasePlanClass: "trim_redundancy",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Direct holder clearing via upsert should fail",
		Details:           "Coalition ID: coal-ops-upsert-linked-rebase-clear-holder\nRepair tension: tens-repair-ops-upsert-linked-rebase-clear-holder\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-upsert-linked-rebase-clear-holder",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase source queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    sourceQueue.QueueKey,
		QueueType:   sourceQueue.QueueType,
		Title:       sourceQueue.Title,
		Summary:     sourceQueue.Summary,
		Details:     sourceQueue.Details,
		AssignedTo:  "",
		Urgency:     sourceQueue.Urgency,
		SourceKind:  sourceQueue.SourceKind,
		SourceID:    sourceQueue.SourceID,
		TaskID:      sourceQueue.TaskID,
		AgentID:     sourceQueue.AgentID,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject clearing linked rebase holder")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "use workspace.ops.escalate") {
		t.Fatalf("expected linked rebase holder clearing guidance, got %+v", rpcErr)
	}

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue): %v", err)
	}
	if currentSourceQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("linked rebase source queue assigned_to after rejected clear = %q, want reviewer-a", currentSourceQueue.AssignedTo)
	}
	currentActionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action queue): %v", err)
	}
	if currentActionQueue.AssignedTo != "reviewer-a" {
		t.Fatalf("linked rebase action queue assigned_to after rejected clear = %q, want reviewer-a", currentActionQueue.AssignedTo)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.AssignedTo != "reviewer-a" {
		t.Fatalf("linked rebase action assigned_to after rejected clear = %q, want reviewer-a", action.AssignedTo)
	}
}

func TestWorkspaceOpsUpsertRejectsSourceIdentityTamperForStandaloneActionQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-standalone-action-source-tamper"
		taskID      = "task-ops-upsert-standalone-action-source-tamper"
		agentID     = "agent-ops-upsert-standalone-action-source-tamper"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Standalone action source tamper guard",
		Description: "Upsert should not retag a pending standalone action queue.",
		Blocking:    boolPtr(true),
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    actionQueue.QueueKey,
		QueueType:   actionQueue.QueueType,
		Title:       actionQueue.Title,
		Summary:     actionQueue.Summary,
		Details:     actionQueue.Details,
		AssignedTo:  actionQueue.AssignedTo,
		Urgency:     actionQueue.Urgency,
		SourceKind:  "manual",
		SourceID:    "manual:tampered",
		TaskID:      actionQueue.TaskID,
		AgentID:     actionQueue.AgentID,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject standalone action source identity tamper")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "source identity is workflow-managed") {
		t.Fatalf("expected source-identity tamper guidance, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, actionQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}
	if currentQueue.SourceKind != "human_action" {
		t.Fatalf("standalone action queue source_kind after rejected tamper = %q, want human_action", currentQueue.SourceKind)
	}
	if currentQueue.SourceID != actionID {
		t.Fatalf("standalone action queue source_id after rejected tamper = %q, want %q", currentQueue.SourceID, actionID)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.Status != model.ActionStatusPending {
		t.Fatalf("standalone action status after rejected tamper = %q, want %q", action.Status, model.ActionStatusPending)
	}
}

func TestWorkspaceOpsUpsertRejectsTaskAgentContextTamperForLinkedRebaseQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-linked-rebase-context"
		taskID      = "task-ops-upsert-linked-rebase-context"
		agentID     = "agent-ops-upsert-linked-rebase-context"
		queueKey    = "tension_rebase_followup:ops-upsert-linked-rebase-context"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)

	queuePayload, err := json.Marshal(model.RebaseFollowupPayload{
		CoalitionID:     "coal-ops-upsert-linked-rebase-context",
		RepairTensionID: "tens-repair-ops-upsert-linked-rebase-context",
		NextAction:      model.RebaseNextActionAttempt,
		RebasePlanClass: "trim_redundancy",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Direct task/agent context tamper via upsert should fail",
		Details:           "Coalition ID: coal-ops-upsert-linked-rebase-context\nRepair tension: tens-repair-ops-upsert-linked-rebase-context\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          "tens-repair-ops-upsert-linked-rebase-context",
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase source queue: %v", err)
	}

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    sourceQueue.QueueKey,
		QueueType:   sourceQueue.QueueType,
		Title:       sourceQueue.Title,
		Summary:     sourceQueue.Summary,
		Details:     sourceQueue.Details,
		AssignedTo:  sourceQueue.AssignedTo,
		Urgency:     sourceQueue.Urgency,
		SourceKind:  sourceQueue.SourceKind,
		SourceID:    sourceQueue.SourceID,
		TaskID:      "task-tampered",
		AgentID:     "agent-tampered",
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject linked rebase task/agent context tamper")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "task/agent context is workflow-managed") {
		t.Fatalf("expected linked rebase task/agent guidance, got %+v", rpcErr)
	}

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(source queue): %v", err)
	}
	if currentSourceQueue.TaskID != taskID {
		t.Fatalf("linked rebase source queue task_id after rejected upsert = %q, want %q", currentSourceQueue.TaskID, taskID)
	}
	if currentSourceQueue.AgentID != agentID {
		t.Fatalf("linked rebase source queue agent_id after rejected upsert = %q, want %q", currentSourceQueue.AgentID, agentID)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.TaskID != taskID {
		t.Fatalf("linked rebase promoted action task_id = %q, want %q", action.TaskID, taskID)
	}
	if action.AgentID != agentID {
		t.Fatalf("linked rebase promoted action agent_id = %q, want %q", action.AgentID, agentID)
	}
}

func TestWorkspaceOpsUpsertRejectsTaskAgentContextTamperForRollbackFailureQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-ops-upsert-rollback-context"
		taskID      = "task-ops-upsert-rollback-context"
		agentID     = "agent-ops-upsert-rollback-context"
		queueKey    = model.RebaseRollbackFailureQueueKeyPrefix + "ops-upsert-rollback-context"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	sourceQueue := createRollbackFailureActionQueueFixture(t, ctx, store, workspaceID, taskID, agentID, queueKey, "tens-repair-ops-upsert-rollback-context")

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID: workspaceID,
		QueueKey:    sourceQueue.QueueKey,
		QueueType:   sourceQueue.QueueType,
		Title:       sourceQueue.Title,
		Summary:     sourceQueue.Summary,
		Details:     sourceQueue.Details,
		AssignedTo:  sourceQueue.AssignedTo,
		Urgency:     sourceQueue.Urgency,
		SourceKind:  sourceQueue.SourceKind,
		SourceID:    sourceQueue.SourceID,
		TaskID:      "task-tampered",
		AgentID:     "agent-tampered",
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsUpsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject rollback-failure task/agent context tamper")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "task/agent context is workflow-managed") {
		t.Fatalf("expected rollback-failure task/agent guidance, got %+v", rpcErr)
	}

	currentQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(current): %v", err)
	}
	if currentQueue.TaskID != taskID {
		t.Fatalf("rollback-failure queue task_id after rejected upsert = %q, want %q", currentQueue.TaskID, taskID)
	}
	if currentQueue.AgentID != agentID {
		t.Fatalf("rollback-failure queue agent_id after rejected upsert = %q, want %q", currentQueue.AgentID, agentID)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, _ := createResp["action_id"].(string)
	if strings.TrimSpace(actionID) == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction: %v", err)
	}
	if action.TaskID != taskID {
		t.Fatalf("rollback-failure promoted action task_id = %q, want %q", action.TaskID, taskID)
	}
	if action.AgentID != agentID {
		t.Fatalf("rollback-failure promoted action agent_id = %q, want %q", action.AgentID, agentID)
	}
}

func TestWorkspaceOpsUpsertRejectsImplicitReopenOfResolvedQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-ops-upsert-no-implicit-reopen"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Ops Upsert No Implicit Reopen",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:ops-no-implicit-reopen",
		QueueType:   "FOLLOW_UP",
		Title:       "No implicit reopen queue",
		Summary:     "Queue should not reopen through generic workspace.ops.upsert.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "done",
	}); err != nil {
		t.Fatalf("resolve queue: %v", err)
	}
	currentRevision, currentUpdatedAt := currentQueueRevisionTokenForTest(t, ctx, store, workspaceID, queue.QueueID, "")

	upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
		WorkspaceID:      workspaceID,
		QueueID:          queue.QueueID,
		QueueKey:         queue.QueueKey,
		QueueType:        queue.QueueType,
		Title:            queue.Title,
		Summary:          "Implicit reopen should fail",
		SourceKind:       queue.SourceKind,
		SourceID:         queue.SourceID,
		CurrentRevision:  currentRevision,
		CurrentUpdatedAt: currentUpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal upsert params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr == nil {
		t.Fatal("expected workspaceOpsUpsert to reject implicit reopen of resolved queue")
	} else if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "operator queue item is not open") {
		t.Fatalf("expected invalid params not-open error on implicit reopen, got %+v", rpcErr)
	}
}

func TestWorkspaceClaimReviewArchiveAndEscalateMirrorNewPersistedRowsForRepeatedClaimActions(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-claim-lifecycle-repeat"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Lifecycle Repeat",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Repeated claim actions should mirror exact rows",
		Body:        "Review/archive/escalate aliases must bind to the newly appended runtime row.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	reviewFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	}
	firstReviewRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "first review request",
		DueAt:       "2026-03-23T10:00:00Z",
		AssignedTo:  "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal first review params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "system", "tests"), firstReviewRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimReview first rpc error: %+v", rpcErr)
	}
	firstReviewPersisted := mustRuntimeEvent(t, ctx, store, reviewFilter)
	seenReview := snapshotRuntimeEventIDs(t, ctx, store, reviewFilter)
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list claim review queues after first review: %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("expected single review queue after first review, got %+v", queues)
	}
	queueID := queues[0].QueueID
	firstReviewQueuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       1,
	})
	assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(
		t,
		ch,
		firstReviewPersisted,
		"workspace.claim.review_requested",
		firstReviewQueuePersisted,
		"workspace.ops.updated",
	)

	seenReviewQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	})
	secondReviewRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "second review request",
		DueAt:       "2026-03-24T10:00:00Z",
		AssignedTo:  "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal second review params: %v", err)
	}
	secondReviewResult, rpcErr := h.workspaceClaimReview(testAuthContext(workspaceID, "system", "tests"), secondReviewRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimReview second rpc error: %+v", rpcErr)
	}
	secondReviewResp, ok := secondReviewResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second review response type %T", secondReviewResult)
	}
	secondReviewRecord, ok := secondReviewResp["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok {
		t.Fatalf("unexpected second review claim payload type %T", secondReviewResp["claim"])
	}
	secondReviewPersisted := mustNewRuntimeEvent(t, ctx, store, reviewFilter, seenReview)
	secondReviewQueuePersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	}, seenReviewQueueEvents)
	assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(
		t,
		ch,
		secondReviewPersisted,
		"workspace.claim.review_requested",
		secondReviewQueuePersisted,
		"workspace.ops.updated",
	)
	if secondReviewPersisted.EventID == firstReviewPersisted.EventID || secondReviewPersisted.IngestSeq <= firstReviewPersisted.IngestSeq {
		t.Fatalf("expected repeated claim review to mirror the newly appended runtime row, got first=%+v second=%+v", firstReviewPersisted, secondReviewPersisted)
	}
	if secondReviewRecord.ClaimID != claim.ClaimID || secondReviewRecord.LifecycleReason != "second review request" || secondReviewRecord.ReviewDueAt == nil || *secondReviewRecord.ReviewDueAt != "2026-03-24T10:00:00Z" {
		t.Fatalf("unexpected repeated claim review payload %+v", secondReviewRecord)
	}

	queues, err = store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list claim review queues: %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("expected single review queue, got %+v", queues)
	}
	escalatedClaimFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	}
	escalatedQueueFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	}
	firstEscalateRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "first review escalation",
		DueAt:       "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-c",
		Urgency:     "HIGH",
	})
	if err != nil {
		t.Fatalf("marshal first claim escalate params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimEscalate(testAuthContext(workspaceID, "system", "tests"), firstEscalateRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimEscalate first rpc error: %+v", rpcErr)
	}
	firstEscalatedClaimPersisted := mustRuntimeEvent(t, ctx, store, escalatedClaimFilter)
	firstEscalatedQueuePersisted := mustRuntimeEvent(t, ctx, store, escalatedQueueFilter)
	assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(
		t,
		ch,
		firstEscalatedClaimPersisted,
		"workspace.claim.review_escalated",
		firstEscalatedQueuePersisted,
		"workspace.ops.escalated",
	)

	seenEscalatedClaim := snapshotRuntimeEventIDs(t, ctx, store, escalatedClaimFilter)
	seenEscalatedQueue := snapshotRuntimeEventIDs(t, ctx, store, escalatedQueueFilter)
	secondEscalateRaw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "dashboard",
		Reason:      "second review escalation",
		DueAt:       "2099-01-02T00:00:00Z",
		AssignedTo:  "reviewer-d",
		Urgency:     "CRITICAL",
	})
	if err != nil {
		t.Fatalf("marshal second claim escalate params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimEscalate(testAuthContext(workspaceID, "system", "tests"), secondEscalateRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimEscalate second rpc error: %+v", rpcErr)
	}
	secondEscalatedClaimPersisted := mustNewRuntimeEvent(t, ctx, store, escalatedClaimFilter, seenEscalatedClaim)
	secondEscalatedQueuePersisted := mustNewRuntimeEvent(t, ctx, store, escalatedQueueFilter, seenEscalatedQueue)
	assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(
		t,
		ch,
		secondEscalatedClaimPersisted,
		"workspace.claim.review_escalated",
		secondEscalatedQueuePersisted,
		"workspace.ops.escalated",
	)
	if secondEscalatedClaimPersisted.EventID == firstEscalatedClaimPersisted.EventID || secondEscalatedClaimPersisted.IngestSeq <= firstEscalatedClaimPersisted.IngestSeq {
		t.Fatalf("expected repeated claim escalation to mirror the newly appended claim runtime row, got first=%+v second=%+v", firstEscalatedClaimPersisted, secondEscalatedClaimPersisted)
	}
	if secondEscalatedQueuePersisted.EventID == firstEscalatedQueuePersisted.EventID || secondEscalatedQueuePersisted.IngestSeq <= firstEscalatedQueuePersisted.IngestSeq {
		t.Fatalf("expected repeated claim escalation to mirror the newly appended queue runtime row, got first=%+v second=%+v", firstEscalatedQueuePersisted, secondEscalatedQueuePersisted)
	}

	archiveFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	}
	firstArchiveRaw, err := json.Marshal(workspaceClaimArchiveParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "dashboard",
		Reason:      "first archive pass",
	})
	if err != nil {
		t.Fatalf("marshal first archive params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimArchive(testAuthContext(workspaceID, "system", "tests"), firstArchiveRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimArchive first rpc error: %+v", rpcErr)
	}
	firstArchiveLive := nextEventOfType(t, ch, "workspace.claim.archived")
	firstArchivePersisted := mustRuntimeEvent(t, ctx, store, archiveFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstArchiveLive, firstArchivePersisted, "workspace.claim.archived")

	seenArchived := snapshotRuntimeEventIDs(t, ctx, store, archiveFilter)
	secondArchiveRaw, err := json.Marshal(workspaceClaimArchiveParams{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "dashboard",
		Reason:      "second archive pass",
	})
	if err != nil {
		t.Fatalf("marshal second archive params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimArchive(testAuthContext(workspaceID, "system", "tests"), secondArchiveRaw); rpcErr != nil {
		t.Fatalf("workspaceClaimArchive second rpc error: %+v", rpcErr)
	}
	secondArchiveLive := nextEventOfType(t, ch, "workspace.claim.archived")
	secondArchivePersisted := mustNewRuntimeEvent(t, ctx, store, archiveFilter, seenArchived)
	assertLiveEventMirrorsRuntimeEvent(t, secondArchiveLive, secondArchivePersisted, "workspace.claim.archived")
	if secondArchivePersisted.EventID == firstArchivePersisted.EventID || secondArchivePersisted.IngestSeq <= firstArchivePersisted.IngestSeq {
		t.Fatalf("expected repeated claim archive to mirror the newly appended runtime row, got first=%+v second=%+v", firstArchivePersisted, secondArchivePersisted)
	}
	var archiveEnvelope sqlite.KnowledgeClaimRecord
	if err := json.Unmarshal([]byte(secondArchiveLive.PayloadJSON), &archiveEnvelope); err != nil {
		t.Fatalf("decode second archive payload: %v", err)
	}
	if archiveEnvelope.ClaimID != claim.ClaimID || archiveEnvelope.Status != "ARCHIVED" || archiveEnvelope.LifecycleReason != "second archive pass" {
		t.Fatalf("unexpected repeated claim archive payload %+v", archiveEnvelope)
	}
}

func TestWorkspaceClaimArchivePublishesRefChangeMemoryInvalidationEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-claim-archive-invalidation-live"
		agentID     = "agent-claim-archive-invalidation"
		claimID     = "claim-archive-invalidation"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Archive Invalidation Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Archive should enqueue invalidation",
		Body:        "Archiving this claim should invalidate stale replicas.",
		Summary:     "Claim archive state",
		Confidence:  0.72,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-claim-archive-invalidation-live",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:claim-archive",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed claim residency: %v", err)
	}

	seenArchiveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceClaimArchiveParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ArchivedBy:  agentID,
		Reason:      "Claim is now obsolete",
	})
	if err != nil {
		t.Fatalf("marshal claim archive params: %v", err)
	}
	result, rpcErr := h.workspaceClaimArchive(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimArchive rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected claim archive response type %T", result)
	}
	claimResp, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok || claimResp.ClaimID != claimID || claimResp.Status != "ARCHIVED" {
		t.Fatalf("unexpected claim archive response %+v", resp)
	}

	archivePersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenArchiveEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: archivePersisted, Type: "workspace.claim.archived"},
		runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
	)
	if len(ordered) != 2 || !runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) {
		t.Fatalf("expected claim archive and invalidation live mirrors to follow persisted chronology, got %+v", ordered)
	}
	var invalidationLive EventMessage
	for i, expectation := range ordered {
		if expectation.Type == "memory.invalidation_enqueued" {
			invalidationLive = liveEvents[i]
			break
		}
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "knowledge_claim.archived" || payload["ref_kind"] != "knowledge_claim" || payload["ref_id"] != claimID {
		t.Fatalf("expected claim archive invalidation payload, got %+v", payload)
	}
}

func TestWorkspaceClaimArchivePublishesReviewQueueResolutionAndInvalidationInChronologicalOrder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-claim-archive-queue-live"
		agentID     = "agent-claim-archive-queue"
		claimID     = "claim-archive-queue-live"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Archive Queue Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Archive should resolve review queue",
		Body:        "Archiving a reviewed claim should resolve the open review queue and invalidate stale replicas.",
		Summary:     "Archive queue baseline",
		Confidence:  0.73,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	claim, err = store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     agentID,
		Reason:      "seed review queue before archive",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-archive",
	})
	if err != nil {
		t.Fatalf("seed review workflow: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-claim-archive-queue-live",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:claim-archive-queue",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed claim residency: %v", err)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queues: %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("expected single follow-up queue, got %+v", queues)
	}
	queueID := queues[0].QueueID

	seenArchiveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	seenQueueResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	})
	seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceClaimArchiveParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ArchivedBy:  agentID,
		Reason:      "Claim is obsolete after review",
	})
	if err != nil {
		t.Fatalf("marshal claim archive params: %v", err)
	}
	result, rpcErr := h.workspaceClaimArchive(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimArchive rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected claim archive response type %T", result)
	}
	claimResp, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok || claimResp.ClaimID != claimID || claimResp.Status != "ARCHIVED" {
		t.Fatalf("unexpected claim archive response %+v", resp)
	}

	archivePersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenArchiveEvents)
	queuePersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	}, seenQueueResolvedEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)

	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: archivePersisted, Type: "workspace.claim.archived"},
		runtimeEventExpectation{Event: queuePersisted, Type: "workspace.ops.resolved"},
		runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
	)
	if len(ordered) != 3 ||
		ordered[0].Type != "workspace.ops.resolved" ||
		ordered[1].Type != "workspace.claim.archived" ||
		ordered[2].Type != "memory.invalidation_enqueued" ||
		!runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) ||
		!runtimeEventChronologicalLess(ordered[1].Event, ordered[2].Event) {
		t.Fatalf("expected queue resolution -> claim archive -> invalidation live mirrors in persisted chronology, got %+v", ordered)
	}
	var invalidationLive EventMessage
	for i, expectation := range ordered {
		if expectation.Type == "memory.invalidation_enqueued" {
			invalidationLive = liveEvents[i]
			break
		}
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "knowledge_claim.archived" || payload["ref_kind"] != "knowledge_claim" || payload["ref_id"] != claimID {
		t.Fatalf("expected claim archive invalidation payload, got %+v", payload)
	}
}

func TestWorkspaceClaimLifecyclePublishesRefChangeMemoryInvalidationEvent(t *testing.T) {
	t.Run("review_requested", func(t *testing.T) {
		testWorkspaceClaimLifecyclePublishesRefChangeMemoryInvalidationEvent(t, "review_requested")
	})
	t.Run("stale", func(t *testing.T) {
		testWorkspaceClaimLifecyclePublishesRefChangeMemoryInvalidationEvent(t, "stale")
	})
}

func testWorkspaceClaimLifecyclePublishesRefChangeMemoryInvalidationEvent(t *testing.T, action string) {
	t.Helper()

	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-claim-lifecycle-invalidation-live"
		agentID     = "agent-claim-lifecycle-invalidation"
	)
	claimID := "claim-lifecycle-invalidation-" + action
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Lifecycle Invalidation Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Lifecycle should enqueue invalidation",
		Body:        "Lifecycle transitions should invalidate stale claim replicas.",
		Summary:     "Lifecycle invalidation baseline",
		Confidence:  0.66,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-claim-lifecycle-invalidation-live-" + action,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:claim-lifecycle:" + action,
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed claim residency: %v", err)
	}

	var (
		liveEventType string
		canonicalType string
		wantStatus    string
		raw           json.RawMessage
		call          func(context.Context, json.RawMessage) (any, *RPCError)
	)
	switch action {
	case "review_requested":
		liveEventType = "workspace.claim.review_requested"
		canonicalType = "knowledge_claim.review_requested"
		wantStatus = "REVIEW"
		call = h.workspaceClaimReview
		raw, err = json.Marshal(workspaceClaimLifecycleParams{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ActorID:     agentID,
			Reason:      "Need a fresh review pass",
			DueAt:       "2099-01-01T00:00:00Z",
			AssignedTo:  "reviewer-a",
		})
	case "stale":
		liveEventType = "workspace.claim.stale"
		canonicalType = "knowledge_claim.stale"
		wantStatus = "STALE"
		call = h.workspaceClaimStale
		raw, err = json.Marshal(workspaceClaimLifecycleParams{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ActorID:     agentID,
			Reason:      "Backing evidence expired",
			DueAt:       "2099-01-02T00:00:00Z",
		})
	default:
		t.Fatalf("unsupported action %q", action)
	}
	if err != nil {
		t.Fatalf("marshal %s params: %v", action, err)
	}

	seenPrimaryEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   canonicalType,
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	result, rpcErr := call(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("%s rpc error: %+v", action, rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected %s response type %T", action, result)
	}
	claimResp, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok || claimResp.ClaimID != claimID || claimResp.Status != wantStatus {
		t.Fatalf("unexpected %s response %+v", action, resp)
	}

	primaryPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   canonicalType,
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenPrimaryEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: primaryPersisted, Type: liveEventType},
		runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
	)
	if len(ordered) != 2 || !runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) {
		t.Fatalf("expected %s and invalidation live mirrors to follow persisted chronology, got %+v", action, ordered)
	}
	var invalidationLive EventMessage
	for i, expectation := range ordered {
		if expectation.Type == "memory.invalidation_enqueued" {
			invalidationLive = liveEvents[i]
			break
		}
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "knowledge_claim.written" || payload["ref_kind"] != "knowledge_claim" || payload["ref_id"] != claimID {
		t.Fatalf("expected %s invalidation payload, got %+v", action, payload)
	}
}

func TestWorkspaceClaimEscalatePublishesRefChangeMemoryInvalidationEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-claim-escalate-invalidation-live"
		agentID     = "agent-claim-escalate-invalidation"
		claimID     = "claim-escalate-invalidation"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Escalation Invalidation Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claimID,
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Escalation should invalidate stale claim replicas",
		Body:        "Escalating review should still emit claim ref-change invalidation.",
		Summary:     "Escalation invalidation baseline",
		Confidence:  0.71,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	claim, err = store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     agentID,
		Reason:      "Need reviewer attention",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-a",
	})
	if err != nil {
		t.Fatalf("seed review status: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-claim-escalate-invalidation-live",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:claim-escalate",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed claim residency: %v", err)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list review queue: %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("expected one review queue, got %+v", queues)
	}
	queueID := queues[0].QueueID

	seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	seenQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	})
	seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceClaimLifecycleParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     agentID,
		Reason:      "Escalate for final review",
		DueAt:       "2099-01-02T00:00:00Z",
		AssignedTo:  "reviewer-b",
		Urgency:     "HIGH",
	})
	if err != nil {
		t.Fatalf("marshal claim escalation params: %v", err)
	}
	result, rpcErr := h.workspaceClaimEscalate(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimEscalate rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected claim escalate response type %T", result)
	}
	claimResp, ok := resp["claim"].(sqlite.KnowledgeClaimRecord)
	if !ok || claimResp.ClaimID != claimID {
		t.Fatalf("unexpected claim escalate response %+v", resp)
	}
	queueResp, ok := resp["queue"].(sqlite.OperatorQueueRecord)
	if !ok || queueResp.QueueID != queueID {
		t.Fatalf("unexpected queue escalate response %+v", resp)
	}

	claimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenClaimEvents)
	queuePersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	}, seenQueueEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)

	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: claimPersisted, Type: "workspace.claim.review_escalated"},
		runtimeEventExpectation{Event: queuePersisted, Type: "workspace.ops.escalated"},
		runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
	)
	if len(ordered) != 3 || !runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) || !runtimeEventChronologicalLess(ordered[1].Event, ordered[2].Event) {
		t.Fatalf("expected claim escalation live mirrors to follow persisted chronology, got %+v", ordered)
	}
	var invalidationLive EventMessage
	for i, expectation := range ordered {
		if expectation.Type == "memory.invalidation_enqueued" {
			invalidationLive = liveEvents[i]
			break
		}
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "knowledge_claim.written" || payload["ref_kind"] != "knowledge_claim" || payload["ref_id"] != claimID {
		t.Fatalf("expected claim escalation invalidation payload, got %+v", payload)
	}
}

func TestWorkspaceClaimStateLifecycleMirrorsNewPersistedRowsForRepeatedClaimID(t *testing.T) {
	type lifecycleCase struct {
		name          string
		liveEventType string
		canonicalType string
		setup         func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, claim sqlite.KnowledgeClaimRecord) string
		firstParams   func(workspaceID, claimID, auxID string) workspaceClaimLifecycleParams
		secondParams  func(workspaceID, claimID, auxID string) workspaceClaimLifecycleParams
		call          func(h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError)
		assertPayload func(t *testing.T, live EventMessage, claimID string)
	}

	cases := []lifecycleCase{
		{
			name:          "confirmed",
			liveEventType: "workspace.claim.confirmed",
			canonicalType: "knowledge_claim.confirmed",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, claim sqlite.KnowledgeClaimRecord) string {
				t.Helper()
				if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: workspaceID,
					ClaimID:     claim.ClaimID,
					ActorID:     "seed-review",
					Reason:      "seed review",
					ReviewDueAt: "2099-02-01T10:00:00Z",
					AssignedTo:  "seed-reviewer",
				}); err != nil {
					t.Fatalf("seed review workflow: %v", err)
				}
				return ""
			},
			firstParams: func(workspaceID, claimID, _ string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-a", Reason: "first confirm"}
			},
			secondParams: func(workspaceID, claimID, _ string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-b", Reason: "second confirm"}
			},
			call: func(h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
				return h.workspaceClaimConfirm(ctx, raw)
			},
			assertPayload: func(t *testing.T, live EventMessage, claimID string) {
				t.Helper()
				var record sqlite.KnowledgeClaimRecord
				if err := json.Unmarshal([]byte(live.PayloadJSON), &record); err != nil {
					t.Fatalf("decode repeated confirm payload: %v", err)
				}
				if record.ClaimID != claimID || record.Status != "CONFIRMED" || record.LifecycleReason != "second confirm" {
					t.Fatalf("unexpected repeated confirm payload %+v", record)
				}
			},
		},
		{
			name:          "disputed",
			liveEventType: "workspace.claim.disputed",
			canonicalType: "knowledge_claim.disputed",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, claim sqlite.KnowledgeClaimRecord) string {
				t.Helper()
				if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: workspaceID,
					ClaimID:     claim.ClaimID,
					ActorID:     "seed-review",
					Reason:      "seed review",
					ReviewDueAt: "2099-02-01T10:00:00Z",
					AssignedTo:  "seed-reviewer",
				}); err != nil {
					t.Fatalf("seed review workflow: %v", err)
				}
				return ""
			},
			firstParams: func(workspaceID, claimID, _ string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-a", Reason: "first dispute", DueAt: "2099-03-03T10:00:00Z"}
			},
			secondParams: func(workspaceID, claimID, _ string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-b", Reason: "second dispute", DueAt: "2099-03-04T10:00:00Z"}
			},
			call: func(h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
				return h.workspaceClaimDispute(ctx, raw)
			},
			assertPayload: func(t *testing.T, live EventMessage, claimID string) {
				t.Helper()
				var record sqlite.KnowledgeClaimRecord
				if err := json.Unmarshal([]byte(live.PayloadJSON), &record); err != nil {
					t.Fatalf("decode repeated dispute payload: %v", err)
				}
				if record.ClaimID != claimID || record.Status != "DISPUTED" || record.LifecycleReason != "second dispute" || record.ReviewDueAt == nil || *record.ReviewDueAt != "2099-03-04T10:00:00Z" {
					t.Fatalf("unexpected repeated dispute payload %+v", record)
				}
			},
		},
		{
			name:          "superseded",
			liveEventType: "workspace.claim.superseded",
			canonicalType: "knowledge_claim.superseded",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, claim sqlite.KnowledgeClaimRecord) string {
				t.Helper()
				replacement, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					WorkspaceID: workspaceID,
					ClaimType:   "FACT",
					Subject:     "Replacement for " + claim.ClaimID,
					Body:        "Replacement claim for superseded lifecycle tests.",
					SourceKind:  "manual",
					SourceID:    "developer",
					AgentID:     claim.AgentID,
				})
				if err != nil {
					t.Fatalf("seed replacement claim: %v", err)
				}
				return replacement.ClaimID
			},
			firstParams: func(workspaceID, claimID, replacementID string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-a", Reason: "first supersede", SupersedingClaimID: replacementID}
			},
			secondParams: func(workspaceID, claimID, replacementID string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-b", Reason: "second supersede", SupersedingClaimID: replacementID}
			},
			call: func(h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
				return h.workspaceClaimSupersede(ctx, raw)
			},
			assertPayload: func(t *testing.T, live EventMessage, claimID string) {
				t.Helper()
				var record sqlite.KnowledgeClaimRecord
				if err := json.Unmarshal([]byte(live.PayloadJSON), &record); err != nil {
					t.Fatalf("decode repeated supersede payload: %v", err)
				}
				if record.ClaimID != claimID || record.Status != "SUPERSEDED" || record.LifecycleReason != "second supersede" || strings.TrimSpace(record.SupersededByClaimID) == "" {
					t.Fatalf("unexpected repeated supersede payload %+v", record)
				}
			},
		},
		{
			name:          "stale",
			liveEventType: "workspace.claim.stale",
			canonicalType: "knowledge_claim.stale",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, claim sqlite.KnowledgeClaimRecord) string {
				t.Helper()
				if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: workspaceID,
					ClaimID:     claim.ClaimID,
					ActorID:     "seed-review",
					Reason:      "seed review",
					ReviewDueAt: "2099-02-01T10:00:00Z",
					AssignedTo:  "seed-reviewer",
				}); err != nil {
					t.Fatalf("seed review workflow: %v", err)
				}
				return ""
			},
			firstParams: func(workspaceID, claimID, _ string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-a", Reason: "first stale", DueAt: "2099-03-05T10:00:00Z"}
			},
			secondParams: func(workspaceID, claimID, _ string) workspaceClaimLifecycleParams {
				return workspaceClaimLifecycleParams{WorkspaceID: workspaceID, ClaimID: claimID, ActorID: "dashboard-b", Reason: "second stale", DueAt: "2099-03-06T10:00:00Z"}
			},
			call: func(h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
				return h.workspaceClaimStale(ctx, raw)
			},
			assertPayload: func(t *testing.T, live EventMessage, claimID string) {
				t.Helper()
				var record sqlite.KnowledgeClaimRecord
				if err := json.Unmarshal([]byte(live.PayloadJSON), &record); err != nil {
					t.Fatalf("decode repeated stale payload: %v", err)
				}
				if record.ClaimID != claimID || record.Status != "STALE" || record.LifecycleReason != "second stale" || record.ReviewDueAt == nil || *record.ReviewDueAt != "2099-03-06T10:00:00Z" {
					t.Fatalf("unexpected repeated stale payload %+v", record)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newServerTestStore(t)
			h := NewHandler(store)
			ctx := context.Background()
			workspaceID := "ws-claim-repeat-state-" + strings.ReplaceAll(tc.canonicalType, ".", "-")

			if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
				WorkspaceID: workspaceID,
				Title:       "Repeated Claim State Lifecycle",
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
			if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
				WorkspaceID: workspaceID,
				AgentID:     "agent-claim-state-repeat",
				OwnerUserID: "developer",
				DisplayName: "Claim State Repeat Agent",
			}); err != nil {
				t.Fatalf("register agent: %v", err)
			}
			claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
				WorkspaceID: workspaceID,
				ClaimType:   "FACT",
				Subject:     "Repeated lifecycle " + tc.name,
				Body:        "Repeated lifecycle aliases must mirror the newly appended runtime row.",
				SourceKind:  "manual",
				SourceID:    "developer",
				AgentID:     "agent-claim-state-repeat",
			})
			if err != nil {
				t.Fatalf("record claim: %v", err)
			}
			auxID := ""
			if tc.setup != nil {
				auxID = tc.setup(t, ctx, store, workspaceID, claim)
			}

			ch := h.GetEventBus().Subscribe(workspaceID)
			defer h.GetEventBus().Unsubscribe(workspaceID, ch)

			claimFilter := sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   tc.canonicalType,
				EntityType:  "knowledge_claim",
				EntityID:    claim.ClaimID,
				Limit:       10,
			}

			firstRaw, err := json.Marshal(tc.firstParams(workspaceID, claim.ClaimID, auxID))
			if err != nil {
				t.Fatalf("marshal first params: %v", err)
			}
			rpcCtx := testAuthContext(workspaceID, "system", "tests")
			if _, rpcErr := tc.call(h, rpcCtx, firstRaw); rpcErr != nil {
				t.Fatalf("first lifecycle rpc error: %+v", rpcErr)
			}
			firstLive := nextEventOfType(t, ch, tc.liveEventType)
			firstPersisted := mustRuntimeEvent(t, ctx, store, claimFilter)
			assertLiveEventMirrorsRuntimeEvent(t, firstLive, firstPersisted, tc.liveEventType)

			seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, claimFilter)
			secondRaw, err := json.Marshal(tc.secondParams(workspaceID, claim.ClaimID, auxID))
			if err != nil {
				t.Fatalf("marshal second params: %v", err)
			}
			if _, rpcErr := tc.call(h, rpcCtx, secondRaw); rpcErr != nil {
				t.Fatalf("second lifecycle rpc error: %+v", rpcErr)
			}
			secondLive := nextEventOfType(t, ch, tc.liveEventType)
			secondPersisted := mustNewRuntimeEvent(t, ctx, store, claimFilter, seenClaimEvents)
			assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondPersisted, tc.liveEventType)
			if secondPersisted.EventID == firstPersisted.EventID || secondPersisted.IngestSeq <= firstPersisted.IngestSeq {
				t.Fatalf("expected repeated %s to mirror the newly appended runtime row, got first=%+v second=%+v", tc.liveEventType, firstPersisted, secondPersisted)
			}
			tc.assertPayload(t, secondLive, claim.ClaimID)
		})
	}
}

func TestWorkspaceEventsReplayAndEvaluateRPC(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-replay-rpc"
		agentID     = "agent-replay-rpc"
		sessionID   = "session-replay-rpc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replay RPC Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	startRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Start runtime replay coverage",
	})
	if err != nil {
		t.Fatalf("marshal session start params: %v", err)
	}
	if _, rpcErr := callAgentSessionStartRaw(t, h, ctx, startRaw); rpcErr != nil {
		t.Fatalf("agentSessionStart rpc error: %+v", rpcErr)
	}

	blockedRaw, err := json.Marshal(agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Waiting on operator reply",
		Status:      model.SessionStatusBlocked,
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "operator", Detail: "approve replay RPC flow"}},
	})
	if err != nil {
		t.Fatalf("marshal session blocked params: %v", err)
	}
	if _, rpcErr := callAgentSessionBlockedRaw(t, h, ctx, blockedRaw); rpcErr != nil {
		t.Fatalf("agentSessionBlocked rpc error: %+v", rpcErr)
	}

	replayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		Limit:         20,
		IncludeEvents: true,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayAny, rpcErr := h.workspaceEventsReplay(ctx, replayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayJSON, err := json.Marshal(replayAny)
	if err != nil {
		t.Fatalf("marshal replay response: %v", err)
	}
	var replayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(replayJSON, &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if len(replayResp.Report.Events) == 0 {
		t.Fatalf("expected replay events, got %+v", replayResp.Report)
	}
	if replayResp.Report.TimeAuthority.WorkspaceID != workspaceID || replayResp.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected replay report to expose workspace time authority, got %+v", replayResp.Report.TimeAuthority)
	}
	foundSession := false
	for _, session := range replayResp.Report.Sessions {
		if session.SessionID == sessionID && session.Status == model.SessionStatusBlocked {
			foundSession = true
			break
		}
	}
	if !foundSession {
		t.Fatalf("expected replay report to include blocked session %s, got %+v", sessionID, replayResp.Report.Sessions)
	}

	evaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal evaluate params: %v", err)
	}
	evaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, evaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	evaluateJSON, err := json.Marshal(evaluateAny)
	if err != nil {
		t.Fatalf("marshal evaluate response: %v", err)
	}
	var evaluateResp struct {
		TimeAuthority sqlite.WorkspaceTimeAuthority  `json:"time_authority"`
		Metrics       sqlite.RuntimeReplayMetrics    `json:"metrics"`
		Evaluation    sqlite.RuntimeReplayEvaluation `json:"evaluation"`
		Counts        map[string]int                 `json:"counts"`
	}
	if err := json.Unmarshal(evaluateJSON, &evaluateResp); err != nil {
		t.Fatalf("decode evaluate response: %v", err)
	}
	if evaluateResp.Metrics.TotalEvents == 0 {
		t.Fatalf("expected replay metrics to count events, got %+v", evaluateResp.Metrics)
	}
	if evaluateResp.TimeAuthority.WorkspaceID != workspaceID || evaluateResp.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected replay evaluate response to expose workspace time authority, got %+v", evaluateResp.TimeAuthority)
	}
	if evaluateResp.Counts["sessions"] == 0 {
		t.Fatalf("expected replay evaluate counts to include sessions, got %+v", evaluateResp.Counts)
	}
	if evaluateResp.Evaluation.Verdict == "" {
		t.Fatalf("expected replay evaluation verdict, got %+v", evaluateResp.Evaluation)
	}
	if evaluateResp.Evaluation.FindingSummary.TotalFindings != len(evaluateResp.Evaluation.Findings) {
		t.Fatalf("expected replay evaluate response to surface bounded finding summary, got %+v", evaluateResp.Evaluation)
	}
	if evaluateResp.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent > evaluateResp.Evaluation.FindingSummary.TotalFindings {
		t.Fatalf("expected replay evaluate response provenance summary to stay bounded by current findings, got %+v", evaluateResp.Evaluation)
	}
}

func TestWorkspaceEventsReplayAndEvaluateTruncatedReflectActualOverflow(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-truncated-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Truncated RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for idx, eventID := range []string{
		"rtev-rpc-truncated-1",
		"rtev-rpc-truncated-2",
		"rtev-rpc-truncated-3",
	} {
		toolID := fmt.Sprintf("tool-rpc-%d", idx+1)
		actorID := fmt.Sprintf("agent-rpc-%d", idx+1)
		payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, workspaceID, toolID, "agent", actorID, "tool.call.denied", "tool.call", "", nil)
		if rpcErr != nil {
			t.Fatalf("build runtime payload %s: %+v", eventID, rpcErr)
		}
		if _, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
			EventID:     eventID,
			WorkspaceID: workspaceID,
			EventType:   "tool.call.denied",
			EntityType:  "tool",
			EntityID:    toolID,
			ActorType:   "agent",
			ActorID:     actorID,
			PayloadJSON: payloadJSON,
			CreatedAt:   fmt.Sprintf("2026-03-22T17:0%d:00Z", idx),
		}); err != nil {
			t.Fatalf("record runtime event %s: %v", eventID, err)
		}
	}

	fullReplayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		Limit:         3,
		IncludeEvents: true,
	})
	if err != nil {
		t.Fatalf("marshal full replay params: %v", err)
	}
	fullReplayAny, rpcErr := h.workspaceEventsReplay(ctx, fullReplayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay full rpc error: %+v", rpcErr)
	}
	fullReplayJSON, err := json.Marshal(fullReplayAny)
	if err != nil {
		t.Fatalf("marshal full replay response: %v", err)
	}
	var fullReplayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(fullReplayJSON, &fullReplayResp); err != nil {
		t.Fatalf("decode full replay response: %v", err)
	}
	if fullReplayResp.Report.Truncated {
		t.Fatalf("expected exact-limit full replay RPC to stay untruncated, got %+v", fullReplayResp.Report)
	}
	if len(fullReplayResp.Report.Events) != 3 {
		t.Fatalf("expected exact-limit full replay RPC to keep all events, got %+v", fullReplayResp.Report.Events)
	}

	fullEvaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       3,
	})
	if err != nil {
		t.Fatalf("marshal full evaluate params: %v", err)
	}
	fullEvaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, fullEvaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate full rpc error: %+v", rpcErr)
	}
	fullEvaluateJSON, err := json.Marshal(fullEvaluateAny)
	if err != nil {
		t.Fatalf("marshal full evaluate response: %v", err)
	}
	var fullEvaluateResp struct {
		Scope      sqlite.RuntimeReplayScopeAssessment `json:"scope"`
		Evaluation sqlite.RuntimeReplayEvaluation      `json:"evaluation"`
	}
	if err := json.Unmarshal(fullEvaluateJSON, &fullEvaluateResp); err != nil {
		t.Fatalf("decode full evaluate response: %v", err)
	}
	if fullEvaluateResp.Evaluation.FindingSummary.ScopePartialCount != 0 {
		t.Fatalf("expected exact-limit full evaluate RPC to avoid partial-scope finding, got %+v", fullEvaluateResp.Evaluation)
	}
	if !fullEvaluateResp.Scope.Authoritative || fullEvaluateResp.Scope.IntegrityBand != "COMPLETE" || len(fullEvaluateResp.Scope.Reasons) != 0 {
		t.Fatalf("expected exact-limit full evaluate RPC to remain authoritative, got %+v", fullEvaluateResp.Scope)
	}

	partialReplayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		Limit:         2,
		IncludeEvents: true,
	})
	if err != nil {
		t.Fatalf("marshal partial replay params: %v", err)
	}
	partialReplayAny, rpcErr := h.workspaceEventsReplay(ctx, partialReplayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay partial rpc error: %+v", rpcErr)
	}
	partialReplayJSON, err := json.Marshal(partialReplayAny)
	if err != nil {
		t.Fatalf("marshal partial replay response: %v", err)
	}
	var partialReplayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(partialReplayJSON, &partialReplayResp); err != nil {
		t.Fatalf("decode partial replay response: %v", err)
	}
	if !partialReplayResp.Report.Truncated {
		t.Fatalf("expected overflow replay RPC to stay truncated, got %+v", partialReplayResp.Report)
	}
	if len(partialReplayResp.Report.Events) != 2 {
		t.Fatalf("expected overflow replay RPC to keep canonical limit, got %+v", partialReplayResp.Report.Events)
	}

	partialEvaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       2,
	})
	if err != nil {
		t.Fatalf("marshal partial evaluate params: %v", err)
	}
	partialEvaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, partialEvaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate partial rpc error: %+v", rpcErr)
	}
	partialEvaluateJSON, err := json.Marshal(partialEvaluateAny)
	if err != nil {
		t.Fatalf("marshal partial evaluate response: %v", err)
	}
	var partialEvaluateResp struct {
		Scope      sqlite.RuntimeReplayScopeAssessment `json:"scope"`
		Evaluation sqlite.RuntimeReplayEvaluation      `json:"evaluation"`
	}
	if err := json.Unmarshal(partialEvaluateJSON, &partialEvaluateResp); err != nil {
		t.Fatalf("decode partial evaluate response: %v", err)
	}
	if partialEvaluateResp.Evaluation.FindingSummary.ScopePartialCount != 1 {
		t.Fatalf("expected overflow evaluate RPC to surface partial-scope finding, got %+v", partialEvaluateResp.Evaluation)
	}
	if partialEvaluateResp.Scope.Authoritative || partialEvaluateResp.Scope.IntegrityBand != "PARTIAL" || !slices.Contains(partialEvaluateResp.Scope.Reasons, "truncated_window") {
		t.Fatalf("expected overflow evaluate RPC to stay partial because of truncation, got %+v", partialEvaluateResp.Scope)
	}
}

func TestWorkspaceEventsReplayAndEvaluateSurfaceRetentionRisk(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-retention-risk-rpc"
		agentID     = "agent-retention-risk-rpc"
		sessionID   = "session-retention-risk-rpc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Retention Risk RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replay Retention Risk Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-30T11:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:           sessionID,
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		TriggerKind:         "token_budget_exceeded",
		TokenBudget:         900,
		MessageCountBefore:  14,
		MessageCountAfter:   5,
		MessageTokensBefore: 2800,
		MessageTokensAfter:  900,
		TotalInputTokens:    2100,
		TotalOutputTokens:   700,
		SummaryText:         "Replay retention risk compacted session.",
	})
	if err != nil {
		t.Fatalf("record session compaction snapshot: %v", err)
	}
	if strings.TrimSpace(snapshot.EpisodePackID) == "" {
		t.Fatalf("expected compaction snapshot to carry episode pack lineage, got %+v", snapshot)
	}

	replayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		Limit:         10,
		IncludeEvents: true,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayAny, rpcErr := h.workspaceEventsReplay(ctx, replayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayJSON, err := json.Marshal(replayAny)
	if err != nil {
		t.Fatalf("marshal replay response: %v", err)
	}
	var replayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(replayJSON, &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayResp.Report.Evaluation.RetentionRisk.Band != "COMPACTED" {
		t.Fatalf("expected replay response to surface compacted retention risk, got %+v", replayResp.Report.Evaluation.RetentionRisk)
	}
	if replayResp.Report.Evaluation.FindingSummary.RetentionFindingCount != 1 || replayResp.Report.Evaluation.FindingSummary.InfoFindingCount != 1 {
		t.Fatalf("expected replay response to surface retention finding summary, got %+v", replayResp.Report.Evaluation.FindingSummary)
	}
	if replayResp.Report.Evaluation.FindingSummary.RetentionCompactionCandidateCount != 0 ||
		replayResp.Report.Evaluation.FindingSummary.RetentionCompactedSessionCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.RetentionSnapshotWithoutEpisodePackCount != 0 {
		t.Fatalf("expected replay response to surface compacted-session retention subcounts, got %+v", replayResp.Report.Evaluation.FindingSummary)
	}
	if replayResp.Report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 0 ||
		replayResp.Report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 0 {
		t.Fatalf("expected replay response retention findings to avoid source-lineage provenance counts, got %+v", replayResp.Report.Evaluation.ProvenanceSummary)
	}
	if replayResp.Report.Evaluation.RetentionRisk.CompactionSnapshotCount != 1 || replayResp.Report.Evaluation.RetentionRisk.EpisodePackCount != 1 {
		t.Fatalf("expected replay response to preserve compaction snapshot counts, got %+v", replayResp.Report.Evaluation.RetentionRisk)
	}
	compactedReplay := requireRuntimeReplayFinding(t, replayResp.Report.Evaluation.Findings, "runtime_event_retention_compacted_session")
	if compactedReplay.EntityType != "agent_session" || compactedReplay.EntityID != sessionID {
		t.Fatalf("expected replay response to surface compacted retention finding for %s, got %+v", sessionID, compactedReplay)
	}

	evaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal evaluate params: %v", err)
	}
	evaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, evaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	evaluateJSON, err := json.Marshal(evaluateAny)
	if err != nil {
		t.Fatalf("marshal evaluate response: %v", err)
	}
	var evaluateResp struct {
		TimeAuthority sqlite.WorkspaceTimeAuthority  `json:"time_authority"`
		Evaluation    sqlite.RuntimeReplayEvaluation `json:"evaluation"`
	}
	if err := json.Unmarshal(evaluateJSON, &evaluateResp); err != nil {
		t.Fatalf("decode evaluate response: %v", err)
	}
	if evaluateResp.TimeAuthority.WorkspaceID != workspaceID || evaluateResp.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected replay evaluate response to expose workspace time authority, got %+v", evaluateResp.TimeAuthority)
	}
	if evaluateResp.Evaluation.RetentionRisk.Band != "COMPACTED" {
		t.Fatalf("expected evaluate response to surface compacted retention risk, got %+v", evaluateResp.Evaluation.RetentionRisk)
	}
	if evaluateResp.Evaluation.FindingSummary.RetentionFindingCount != 1 || evaluateResp.Evaluation.FindingSummary.InfoFindingCount != 1 {
		t.Fatalf("expected evaluate response to surface retention finding summary, got %+v", evaluateResp.Evaluation.FindingSummary)
	}
	if evaluateResp.Evaluation.FindingSummary.RetentionCompactionCandidateCount != 0 ||
		evaluateResp.Evaluation.FindingSummary.RetentionCompactedSessionCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.RetentionSnapshotWithoutEpisodePackCount != 0 {
		t.Fatalf("expected evaluate response to surface compacted-session retention subcounts, got %+v", evaluateResp.Evaluation.FindingSummary)
	}
	if evaluateResp.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 0 ||
		evaluateResp.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 0 {
		t.Fatalf("expected evaluate response retention findings to avoid source-lineage provenance counts, got %+v", evaluateResp.Evaluation.ProvenanceSummary)
	}
	if evaluateResp.Evaluation.WarningCount != 1 || evaluateResp.Evaluation.Verdict != "warn" {
		t.Fatalf("expected compacted retention risk to add no warning severity beyond the existing partial-scope warning, got %+v", evaluateResp.Evaluation)
	}
	compactedEvaluate := requireRuntimeReplayFinding(t, evaluateResp.Evaluation.Findings, "runtime_event_retention_compacted_session")
	if compactedEvaluate.EntityType != "agent_session" || compactedEvaluate.EntityID != sessionID {
		t.Fatalf("expected evaluate response to surface compacted retention finding for %s, got %+v", sessionID, compactedEvaluate)
	}
}

func TestWorkspaceEventsReplayAndEvaluateSurfaceExecutionRunAndQueueIntegritySubcounts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-exec-integrity-rpc"
		agentID     = "agent-exec-integrity-rpc"
		sessionID   = "session-exec-integrity-rpc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Execution Integrity RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replay Execution Integrity Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	for _, input := range []sqlite.AgentSessionCoordinationInput{
		{
			EventType:   model.SessionEventStart,
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			AgentID:     agentID,
			Summary:     "Replay execution integrity seed",
			UpdatedAt:   "2026-03-22T12:00:00Z",
		},
		{
			EventType:   model.SessionEventBlocked,
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			AgentID:     agentID,
			Summary:     "Blocked before stale replay snapshot",
			BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
			UpdatedAt:   "2026-03-22T12:02:00Z",
		},
		{
			EventType:   model.SessionEventEnd,
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			AgentID:     agentID,
			Summary:     "Session closed but stale execution obligations remain",
			UpdatedAt:   "2026-03-22T12:05:00Z",
		},
	} {
		state, err := store.RecordAgentSessionCoordination(ctx, input)
		if err != nil {
			t.Fatalf("record session coordination: %v", err)
		}
		payloadJSON, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("marshal session replay payload: %v", err)
		}
		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:     "rtev:" + sessionID + ":" + state.UpdateType + ":" + state.UpdatedAt,
			WorkspaceID: workspaceID,
			EventType:   state.UpdateType,
			EntityType:  "agent_session",
			EntityID:    sessionID,
			ActorType:   "agent",
			ActorID:     agentID,
			AgentID:     agentID,
			SessionID:   sessionID,
			TaskID:      state.TaskID,
			PayloadJSON: string(payloadJSON),
			CreatedAt:   state.UpdatedAt,
		}); err != nil {
			t.Fatalf("record runtime session event: %v", err)
		}
		if model.NormalizeSessionEventType(state.UpdateType) == model.SessionEventBlocked {
			if _, err := store.SyncOperatorQueueFromSessionState(ctx, state); err != nil {
				t.Fatalf("sync stale queue fixture from blocked session state: %v", err)
			}
		}
	}

	staleQueueKey := "session:" + sessionID + ":blocker"
	result, err := store.DB().ExecContext(ctx, `
		UPDATE operator_queue_items
		   SET status = 'OPEN',
		       resolution = '',
		       resolved_at = NULL,
		       resolved_by = NULL,
		       updated_at = ?
		 WHERE workspace_id = ?
		   AND queue_key = ?`,
		"2026-03-22T12:06:00Z",
		workspaceID,
		staleQueueKey,
	)
	if err != nil {
		t.Fatalf("reopen stale queue fixture: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		t.Fatalf("rows affected for stale queue fixture: %v", err)
	} else if rows != 1 {
		t.Fatalf("expected to reopen exactly one stale queue row, got %d", rows)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO execution_runs(run_id, workspace_id, session_id, agent_id, title, summary, status, outcome, verification_json, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', '', '{}', ?, ?)`,
		"run-exec-integrity-rpc",
		workspaceID,
		sessionID,
		agentID,
		"Stale execution run",
		"Execution run stayed active after terminal session",
		"2026-03-22T12:06:00Z",
		"2026-03-22T12:06:00Z",
	); err != nil {
		t.Fatalf("insert stale execution run fixture: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev:run-exec-integrity-rpc:legacy-active",
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "run-exec-integrity-rpc",
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		SessionID:   sessionID,
		PayloadJSON: `{"status":"ACTIVE","title":"Stale execution run","summary":"Execution run stayed active after terminal session","agent_id":"agent-exec-integrity-rpc","session_id":"session-exec-integrity-rpc"}`,
		CreatedAt:   "2026-03-22T12:06:00Z",
	}); err != nil {
		t.Fatalf("record stale execution run event fixture: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-exec-integrity-rpc",
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Status:      "ACTIVE",
		Subject:     "Runtime journal is canonical",
		Body:        "But this claim intentionally omits memory linkage.",
		Summary:     "Intentional stale claim for replay regression.",
		SourceKind:  "workspace_memory",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("record stale knowledge claim: %v", err)
	}

	replayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayAny, rpcErr := h.workspaceEventsReplay(ctx, replayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayJSON, err := json.Marshal(replayAny)
	if err != nil {
		t.Fatalf("marshal replay response: %v", err)
	}
	var replayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(replayJSON, &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayResp.Report.Evaluation.Verdict != "warn" || replayResp.Report.Evaluation.WarningCount != 4 {
		t.Fatalf("expected replay response to surface stale execution integrity warnings, got %+v", replayResp.Report.Evaluation)
	}
	if replayResp.Report.Evaluation.FindingSummary.TotalFindings != 4 ||
		replayResp.Report.Evaluation.FindingSummary.ExecutionRunIntegrityCount != 2 ||
		replayResp.Report.Evaluation.FindingSummary.ExecutionRunOutOfSyncCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.ExecutionRunWithoutStepsCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.ClaimIntegrityCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.ClaimMissingMemoryLinkCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.OperatorQueueIntegrityCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.StaleOpenQueueCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected replay response to surface execution-run, claim, and operator-queue integrity subcounts, got %+v", replayResp.Report.Evaluation.FindingSummary)
	}

	evaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal evaluate params: %v", err)
	}
	evaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, evaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	evaluateJSON, err := json.Marshal(evaluateAny)
	if err != nil {
		t.Fatalf("marshal evaluate response: %v", err)
	}
	var evaluateResp struct {
		Evaluation sqlite.RuntimeReplayEvaluation `json:"evaluation"`
	}
	if err := json.Unmarshal(evaluateJSON, &evaluateResp); err != nil {
		t.Fatalf("decode evaluate response: %v", err)
	}
	if evaluateResp.Evaluation.Verdict != "warn" || evaluateResp.Evaluation.WarningCount != 4 {
		t.Fatalf("expected evaluate response to surface stale execution integrity warnings, got %+v", evaluateResp.Evaluation)
	}
	if evaluateResp.Evaluation.FindingSummary.TotalFindings != 4 ||
		evaluateResp.Evaluation.FindingSummary.ExecutionRunIntegrityCount != 2 ||
		evaluateResp.Evaluation.FindingSummary.ExecutionRunOutOfSyncCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.ExecutionRunWithoutStepsCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.ClaimIntegrityCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.ClaimMissingMemoryLinkCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.OperatorQueueIntegrityCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.StaleOpenQueueCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected evaluate response to surface execution-run, claim, and operator-queue integrity subcounts, got %+v", evaluateResp.Evaluation.FindingSummary)
	}
}

func TestWorkspaceEventsReplayAndEvaluateSurfaceMissingExecutionRunSubcounts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-missing-run-rpc"
		agentID     = "agent-missing-run-rpc"
		sessionID   = "session-missing-run-rpc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Missing Execution Run RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replay Missing Execution Run Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Replay missing execution run seed",
		UpdatedAt:   "2026-03-22T11:00:00Z",
	})
	if err != nil {
		t.Fatalf("record session coordination: %v", err)
	}
	payloadJSON, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal session replay payload: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev:" + sessionID + ":" + state.UpdateType + ":" + state.UpdatedAt,
		WorkspaceID: workspaceID,
		EventType:   state.UpdateType,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      state.TaskID,
		PayloadJSON: string(payloadJSON),
		CreatedAt:   state.UpdatedAt,
	}); err != nil {
		t.Fatalf("record runtime session event: %v", err)
	}

	replayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayAny, rpcErr := h.workspaceEventsReplay(ctx, replayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayJSON, err := json.Marshal(replayAny)
	if err != nil {
		t.Fatalf("marshal replay response: %v", err)
	}
	var replayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(replayJSON, &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayResp.Report.Evaluation.Verdict != "warn" || replayResp.Report.Evaluation.WarningCount != 1 {
		t.Fatalf("expected replay response to surface missing execution-run warning, got %+v", replayResp.Report.Evaluation)
	}
	if replayResp.Report.Evaluation.FindingSummary.TotalFindings != 1 ||
		replayResp.Report.Evaluation.FindingSummary.ExecutionRunIntegrityCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.MissingExecutionRunCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.ExecutionRunOutOfSyncCount != 0 ||
		replayResp.Report.Evaluation.FindingSummary.ExecutionRunWithoutStepsCount != 0 ||
		replayResp.Report.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected replay response to surface missing execution-run subcounts, got %+v", replayResp.Report.Evaluation.FindingSummary)
	}

	evaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal evaluate params: %v", err)
	}
	evaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, evaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	evaluateJSON, err := json.Marshal(evaluateAny)
	if err != nil {
		t.Fatalf("marshal evaluate response: %v", err)
	}
	var evaluateResp struct {
		Evaluation sqlite.RuntimeReplayEvaluation `json:"evaluation"`
	}
	if err := json.Unmarshal(evaluateJSON, &evaluateResp); err != nil {
		t.Fatalf("decode evaluate response: %v", err)
	}
	if evaluateResp.Evaluation.Verdict != "warn" || evaluateResp.Evaluation.WarningCount != 1 {
		t.Fatalf("expected evaluate response to surface missing execution-run warning, got %+v", evaluateResp.Evaluation)
	}
	if evaluateResp.Evaluation.FindingSummary.TotalFindings != 1 ||
		evaluateResp.Evaluation.FindingSummary.ExecutionRunIntegrityCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.MissingExecutionRunCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.ExecutionRunOutOfSyncCount != 0 ||
		evaluateResp.Evaluation.FindingSummary.ExecutionRunWithoutStepsCount != 0 ||
		evaluateResp.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected evaluate response to surface missing execution-run subcounts, got %+v", evaluateResp.Evaluation.FindingSummary)
	}
}

func TestWorkspaceEventsReplayAndEvaluateSurfaceClaimIntegritySubcounts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-claim-integrity-rpc"
		agentID     = "agent-claim-integrity-rpc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Claim Integrity RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replay Claim Integrity Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-review-missing-queue-rpc",
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Status:      "REVIEW",
		Subject:     "Review queue should exist",
		Body:        "This claim is intentionally left without a follow-up queue.",
		Summary:     "Missing review queue.",
		SourceKind:  "manual",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("record review warning claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-superseded-missing-link-rpc",
		WorkspaceID: workspaceID,
		ClaimType:   "PROCEDURE",
		Status:      "SUPERSEDED",
		Subject:     "Superseded claim should link successor",
		Body:        "This claim is intentionally missing superseded_by_claim_id.",
		Summary:     "Missing supersede link.",
		SourceKind:  "manual",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("record superseded warning claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-confirmed-stale-queue-rpc",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "CONFIRMED",
		Subject:     "Confirmed claim should not keep review queue",
		Body:        "This claim intentionally leaves a stale queue behind.",
		Summary:     "Stale claim review queue.",
		SourceKind:  "manual",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("record stale queue claim: %v", err)
	}
	if _, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "knowledge_claim:claim-confirmed-stale-queue-rpc:review",
		QueueType:   "FOLLOW_UP",
		Title:       "Stale claim review queue",
		Summary:     "This queue should have been resolved once the claim was confirmed.",
		SourceKind:  "knowledge_claim",
		SourceID:    "claim-confirmed-stale-queue-rpc",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("create stale claim review queue: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-missing-memory-link-rpc",
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Status:      "ACTIVE",
		Subject:     "Runtime journal is canonical",
		Body:        "But this claim intentionally omits memory linkage.",
		Summary:     "Intentional stale claim for replay regression.",
		SourceKind:  "workspace_memory",
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("record stale knowledge claim: %v", err)
	}
	memory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "memory-dup-rpc",
		MemoryType:  "DECISION",
		Title:       "Use runtime journal as canonical truth",
		Body:        "Operators should prefer runtime events over archived traces.",
		Summary:     "Runtime journal is the source of truth.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-manual-duplicate-rpc",
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Status:      "ACTIVE",
		Subject:     "Runtime journal is canonical",
		Body:        "Manual duplicate active claim for the same memory.",
		Summary:     "Intentional duplicate claim.",
		SourceKind:  "workspace_memory",
		SourceID:    memory.MemoryID,
		MemoryID:    memory.MemoryID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("record duplicate knowledge claim: %v", err)
	}

	replayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayAny, rpcErr := h.workspaceEventsReplay(ctx, replayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayJSON, err := json.Marshal(replayAny)
	if err != nil {
		t.Fatalf("marshal replay response: %v", err)
	}
	var replayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(replayJSON, &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayResp.Report.Evaluation.Verdict != "warn" || replayResp.Report.Evaluation.WarningCount != 5 {
		t.Fatalf("expected replay response to surface claim-integrity warnings, got %+v", replayResp.Report.Evaluation)
	}
	if replayResp.Report.Evaluation.FindingSummary.TotalFindings != 5 ||
		replayResp.Report.Evaluation.FindingSummary.ClaimIntegrityCount != 5 ||
		replayResp.Report.Evaluation.FindingSummary.ClaimMissingMemoryLinkCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.ClaimMissingReviewQueueCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.StaleClaimReviewQueueCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.SupersededClaimMissingLinkCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.DuplicateActiveMemoryClaimCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected replay response to surface claim-integrity subcounts, got %+v", replayResp.Report.Evaluation.FindingSummary)
	}

	evaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("marshal evaluate params: %v", err)
	}
	evaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, evaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	evaluateJSON, err := json.Marshal(evaluateAny)
	if err != nil {
		t.Fatalf("marshal evaluate response: %v", err)
	}
	var evaluateResp struct {
		Evaluation sqlite.RuntimeReplayEvaluation `json:"evaluation"`
	}
	if err := json.Unmarshal(evaluateJSON, &evaluateResp); err != nil {
		t.Fatalf("decode evaluate response: %v", err)
	}
	if evaluateResp.Evaluation.Verdict != "warn" || evaluateResp.Evaluation.WarningCount != 5 {
		t.Fatalf("expected evaluate response to surface claim-integrity warnings, got %+v", evaluateResp.Evaluation)
	}
	if evaluateResp.Evaluation.FindingSummary.TotalFindings != 5 ||
		evaluateResp.Evaluation.FindingSummary.ClaimIntegrityCount != 5 ||
		evaluateResp.Evaluation.FindingSummary.ClaimMissingMemoryLinkCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.ClaimMissingReviewQueueCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.StaleClaimReviewQueueCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.SupersededClaimMissingLinkCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.DuplicateActiveMemoryClaimCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected evaluate response to surface claim-integrity subcounts, got %+v", evaluateResp.Evaluation.FindingSummary)
	}
}

func TestWorkspaceEventsReplayAndEvaluateSurfacePayloadIntegritySubcounts(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-payload-integrity-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Payload Integrity RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-malformed-queue-rpc",
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.opened",
		EntityType:  "operator_queue",
		EntityID:    "queue-malformed-rpc",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"queue_key":`,
		CreatedAt:   "2026-03-22T16:00:00Z",
	}); err != nil {
		t.Fatalf("record malformed runtime event: %v", err)
	}

	replayRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayAny, rpcErr := h.workspaceEventsReplay(ctx, replayRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayJSON, err := json.Marshal(replayAny)
	if err != nil {
		t.Fatalf("marshal replay response: %v", err)
	}
	var replayResp struct {
		Report sqlite.RuntimeReplayReport `json:"report"`
	}
	if err := json.Unmarshal(replayJSON, &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayResp.Report.Evaluation.Verdict != "fail" || replayResp.Report.Evaluation.ErrorCount != 1 {
		t.Fatalf("expected replay response to surface payload-integrity error, got %+v", replayResp.Report.Evaluation)
	}
	if replayResp.Report.Evaluation.FindingSummary.TotalFindings != 1 ||
		replayResp.Report.Evaluation.FindingSummary.PayloadIntegrityCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.MalformedEventPayloadCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.ErrorFindingCount != 1 ||
		replayResp.Report.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected replay response to surface payload-integrity subcounts, got %+v", replayResp.Report.Evaluation.FindingSummary)
	}
	if replayResp.Report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 1 {
		t.Fatalf("expected replay response malformed finding to stay source-addressable, got %+v", replayResp.Report.Evaluation.ProvenanceSummary)
	}

	evaluateRaw, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal evaluate params: %v", err)
	}
	evaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, evaluateRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	evaluateJSON, err := json.Marshal(evaluateAny)
	if err != nil {
		t.Fatalf("marshal evaluate response: %v", err)
	}
	var evaluateResp struct {
		Evaluation sqlite.RuntimeReplayEvaluation `json:"evaluation"`
	}
	if err := json.Unmarshal(evaluateJSON, &evaluateResp); err != nil {
		t.Fatalf("decode evaluate response: %v", err)
	}
	if evaluateResp.Evaluation.Verdict != "fail" || evaluateResp.Evaluation.ErrorCount != 1 {
		t.Fatalf("expected evaluate response to surface payload-integrity error, got %+v", evaluateResp.Evaluation)
	}
	if evaluateResp.Evaluation.FindingSummary.TotalFindings != 1 ||
		evaluateResp.Evaluation.FindingSummary.PayloadIntegrityCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.MalformedEventPayloadCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.ErrorFindingCount != 1 ||
		evaluateResp.Evaluation.FindingSummary.OtherFindingCount != 0 {
		t.Fatalf("expected evaluate response to surface payload-integrity subcounts, got %+v", evaluateResp.Evaluation.FindingSummary)
	}
	if evaluateResp.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 1 {
		t.Fatalf("expected evaluate response malformed finding to stay source-addressable, got %+v", evaluateResp.Evaluation.ProvenanceSummary)
	}
}

func TestWorkspaceEventsReplayPreservesCanonicalEnvelopeAndIsIdempotent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-envelope-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Envelope RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	const createdAt = "2026-03-22T15:00:00Z"
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-a-legacy",
		WorkspaceID: workspaceID,
		EventType:   "bridge.signal",
		EntityType:  "bridge_signal",
		EntityID:    "bridge-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"message":"legacy envelope"}`,
		CreatedAt:   createdAt,
	}); err != nil {
		t.Fatalf("record legacy runtime event: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-b-canonical",
		DedupKey:          "D-bridge-1",
		WorkspaceID:       workspaceID,
		EventType:         "bridge.signal",
		EntityType:        "bridge_signal",
		EntityID:          "bridge-1",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "RC-bridge-1",
		ProvenanceGroupID: "PG-bridge-1",
		ParentRefsJSON:    `["rtev-a-legacy"]`,
		PayloadJSON:       `{"message":"canonical envelope","dedup_key":"D-bridge-1","root_cause_id":"RC-bridge-1","provenance_group_id":"PG-bridge-1"}`,
		CreatedAt:         createdAt,
	}); err != nil {
		t.Fatalf("record canonical runtime event: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result1, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayPayload1, ok := result1.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result1)
	}
	report1, ok := replayPayload1["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", replayPayload1["report"])
	}
	if len(report1.Events) != 2 || report1.Events[0].EventID != "rtev-b-canonical" || report1.Events[1].EventID != "rtev-a-legacy" {
		t.Fatalf("expected canonical event order in replay report, got %+v", report1.Events)
	}
	if report1.Events[0].DedupKey != "D-bridge-1" || report1.Events[0].RootCauseID != "RC-bridge-1" || report1.Events[0].ProvenanceGroupID != "PG-bridge-1" || report1.Events[0].ParentRefsJSON != `["rtev-a-legacy"]` {
		t.Fatalf("expected canonical runtime envelope fields in replay report, got %+v", report1.Events[0])
	}
	var canonicalPayload struct {
		Message           string `json:"message"`
		DedupKey          string `json:"dedup_key"`
		RootCauseID       string `json:"root_cause_id"`
		ProvenanceGroupID string `json:"provenance_group_id"`
	}
	if err := json.Unmarshal([]byte(report1.Events[0].PayloadJSON), &canonicalPayload); err != nil {
		t.Fatalf("decode canonical replay payload: %v", err)
	}
	if canonicalPayload.DedupKey != "D-bridge-1" || canonicalPayload.RootCauseID != "RC-bridge-1" || canonicalPayload.ProvenanceGroupID != "PG-bridge-1" {
		t.Fatalf("expected canonical envelope payload fields to survive replay, got %+v", canonicalPayload)
	}

	evaluateAny, rpcErr := h.workspaceEventsEvaluate(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsEvaluate rpc error: %+v", rpcErr)
	}
	evaluateJSON, err := json.Marshal(evaluateAny)
	if err != nil {
		t.Fatalf("marshal evaluate response: %v", err)
	}
	var evaluateResp struct {
		TimeAuthority sqlite.WorkspaceTimeAuthority  `json:"time_authority"`
		Metrics       sqlite.RuntimeReplayMetrics    `json:"metrics"`
		Evaluation    sqlite.RuntimeReplayEvaluation `json:"evaluation"`
		Counts        map[string]int                 `json:"counts"`
	}
	if err := json.Unmarshal(evaluateJSON, &evaluateResp); err != nil {
		t.Fatalf("decode evaluate response: %v", err)
	}
	if evaluateResp.Metrics.TotalEvents != 2 || evaluateResp.Evaluation.Verdict != "pass" {
		t.Fatalf("expected canonical replay to remain pass-clean, got %+v", evaluateResp)
	}
	if evaluateResp.Counts["events"] != 2 || evaluateResp.Counts["sessions"] != 0 || evaluateResp.Counts["queues"] != 0 || evaluateResp.Counts["claims"] != 0 || evaluateResp.Counts["execution_runs"] != 0 {
		t.Fatalf("expected evaluate counts to stay stable and backward compatible, got %+v", evaluateResp.Counts)
	}

	result2, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay second rpc error: %+v", rpcErr)
	}
	replayPayload2, ok := result2.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second replay result type %T", result2)
	}
	report2, ok := replayPayload2["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected second replay report type %T", replayPayload2["report"])
	}
	if len(report2.Events) != len(report1.Events) || report2.Events[0].EventID != report1.Events[0].EventID || report2.Events[1].EventID != report1.Events[1].EventID {
		t.Fatalf("expected idempotent replay ordering, first=%+v second=%+v", report1.Events, report2.Events)
	}
	if report2.Evaluation.Verdict != report1.Evaluation.Verdict || report2.Metrics.TotalEvents != report1.Metrics.TotalEvents {
		t.Fatalf("expected idempotent replay verdict and metrics, first=%+v second=%+v", report1, report2)
	}
}

func TestWorkspaceEventsReplaySuppressesEquivalentDedupKeyDuplicates(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-dedup-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Dedup RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	first, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-rpc-dedup-a",
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.opened",
		EntityType:  "operator_queue",
		EntityID:    "queue-rpc-dedup-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"queue_key":"queue:rpc-dedup","queue_type":"FOLLOW_UP","status":"OPEN","title":"RPC Dedup queue","summary":"Replay should apply once","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"queue:rpc-dedup:event-1","root_cause_id":"root-rpc-dedup-1","provenance_group_id":"prov-rpc-dedup-1"}`,
		CreatedAt:   "2026-03-22T18:00:00Z",
	})
	if err != nil {
		t.Fatalf("record replay dedup runtime event a: %v", err)
	}
	second, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-rpc-dedup-b",
		DedupKey:          "queue:rpc-dedup:event-1",
		WorkspaceID:       workspaceID,
		EventType:         "operator_queue.opened",
		EntityType:        "operator_queue",
		EntityID:          "queue-rpc-dedup-1",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "root-rpc-dedup-1",
		ProvenanceGroupID: "prov-rpc-dedup-1",
		PayloadJSON:       `{"summary":"Replay should apply once","source_id":"tests","assigned_to":"developer","source_kind":"manual","queue_type":"FOLLOW_UP","queue_key":"queue:rpc-dedup","urgency":"HIGH","keep_session_active":true,"status":"OPEN","title":"RPC Dedup queue"}`,
		CreatedAt:         "2026-03-22T18:01:00Z",
	})
	if err != nil {
		t.Fatalf("record replay dedup runtime event b: %v", err)
	}
	if second.EventID != first.EventID {
		t.Fatalf("expected equivalent dedup-key runtime event to reuse existing row, first=%+v second=%+v", first, second)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected dedup-key-equivalent replay to remain pass-clean, got %+v", report.Evaluation)
	}
	if report.Metrics.TotalEvents != 1 || report.Metrics.AppliedEvents != 1 || report.Metrics.SuppressedDuplicateEvents != 0 || report.Metrics.ConflictingDuplicateKeys != 0 {
		t.Fatalf("unexpected replay metrics %+v", report.Metrics)
	}
	if len(report.Queues) != 1 || report.Queues[0].QueueKey != "queue:rpc-dedup" || report.Queues[0].EventCount != 1 {
		t.Fatalf("expected replay queue to apply equivalent dedup-key event once, got %+v", report.Queues)
	}
}

func TestWorkspaceEventsReplaySuppressesConflictingDedupKeyEventsAfterWarning(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-dedup-conflict-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Dedup Conflict RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-rpc-conflict-a",
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.opened",
		EntityType:  "operator_queue",
		EntityID:    "queue-rpc-conflict-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"queue_key":"queue:rpc-conflict","queue_type":"FOLLOW_UP","status":"OPEN","title":"RPC Conflict queue","summary":"Replay should warn once","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"queue:rpc-conflict:event-1","root_cause_id":"root-rpc-conflict-1","provenance_group_id":"prov-rpc-conflict-1"}`,
		CreatedAt:   "2026-03-22T18:10:00Z",
	}); err != nil {
		t.Fatalf("record replay conflict runtime event a: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-rpc-conflict-b",
		DedupKey:          "queue:rpc-conflict:event-1",
		WorkspaceID:       workspaceID,
		EventType:         "operator_queue.resolved",
		EntityType:        "operator_queue",
		EntityID:          "queue-rpc-conflict-1",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "root-rpc-conflict-1",
		ProvenanceGroupID: "prov-rpc-conflict-1",
		PayloadJSON:       `{"queue_key":"queue:rpc-conflict","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"RPC Conflict queue","summary":"Replay should warn once","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"resolved manually","resolved_by":"operator-1"}`,
		CreatedAt:         "2026-03-22T18:11:00Z",
	}); err == nil {
		t.Fatal("expected conflicting dedup-key runtime event to fail")
	} else if !strings.Contains(err.Error(), "dedup_key") {
		t.Fatalf("expected dedup_key conflict, got %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected rejected dedup-key conflict replay to remain pass-clean, got %+v", report.Evaluation)
	}
	if report.Metrics.TotalEvents != 1 || report.Metrics.AppliedEvents != 1 || report.Metrics.SuppressedDuplicateEvents != 0 || report.Metrics.ConflictingDuplicateKeys != 0 {
		t.Fatalf("unexpected replay conflict metrics %+v", report.Metrics)
	}
	if len(report.Queues) != 1 || report.Queues[0].QueueKey != "queue:rpc-conflict" || report.Queues[0].EventCount != 1 || report.Queues[0].Status != "OPEN" {
		t.Fatalf("expected surviving canonical dedup-key event to remain visible, got %+v", report.Queues)
	}
	if len(report.Evaluation.Findings) != 0 {
		t.Fatalf("expected no replay findings after storage rejected the conflict, got %+v", report.Evaluation.Findings)
	}
}

func TestWorkspaceEventsReplaySurfacesSourceAddressableConflictFindingAndCausalOrder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-source-addressable-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Source Addressable RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-source-legacy",
		nil,
		workspaceID,
		"legacy.signal",
		"legacy_event",
		"legacy-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		nil,
		nil,
		`[]`,
		`{"message":"legacy event"}`,
		"2026-03-22T19:59:00Z",
		1,
	); err != nil {
		t.Fatalf("insert legacy runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-source-open",
		"D-source-rpc-1",
		workspaceID,
		"operator_queue.opened",
		"operator_queue",
		"queue-source-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-source-rpc-1",
		"prov-source-rpc-1",
		`[]`,
		`{"queue_key":"queue:source-rpc","queue_type":"FOLLOW_UP","status":"OPEN","title":"Source queue","summary":"Opened first","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"D-source-rpc-1","root_cause_id":"root-source-rpc-1","provenance_group_id":"prov-source-rpc-1"}`,
		"2026-03-22T20:00:00Z",
		2,
	); err != nil {
		t.Fatalf("insert source open runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-source-parent",
		nil,
		workspaceID,
		"legacy.signal",
		"legacy_event",
		"legacy-parent-rpc",
		"system",
		"tests",
		nil,
		nil,
		nil,
		nil,
		nil,
		`[]`,
		`{"message":"parent arrives later"}`,
		"2026-03-22T20:02:00Z",
		4,
	); err != nil {
		t.Fatalf("insert parent runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-source-close",
		"D-source-rpc-1",
		workspaceID,
		"operator_queue.resolved",
		"operator_queue",
		"queue-source-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-source-rpc-1",
		"prov-source-rpc-1",
		`["rtev-rpc-source-parent"]`,
		`{"queue_key":"queue:source-rpc","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Source queue","summary":"Resolved later","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1","root_cause_id":"root-source-rpc-1","provenance_group_id":"prov-source-rpc-1","parent_refs_json":["rtev-rpc-source-parent"]}`,
		"2026-03-22T20:01:00Z",
		3,
	); err != nil {
		t.Fatalf("insert source close runtime event: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if len(report.Events) != 4 || report.Events[0].EventID != "rtev-rpc-source-parent" || report.Events[1].EventID != "rtev-rpc-source-close" || report.Events[2].EventID != "rtev-rpc-source-open" || report.Events[3].EventID != "rtev-rpc-source-legacy" {
		t.Fatalf("expected replay report events to follow ingest sequence, got %+v", report.Events)
	}
	if report.Metrics.TotalEvents != 4 || report.Metrics.AppliedEvents != 3 || report.Metrics.SuppressedDuplicateEvents != 1 || report.Metrics.ConflictingDuplicateKeys != 1 {
		t.Fatalf("expected replay metrics to surface one conflicting dedup_key and preserve order, got %+v", report.Metrics)
	}
	if report.Evaluation.Verdict != "warn" || len(report.Evaluation.Findings) != 2 {
		t.Fatalf("expected two warning findings, got %+v", report.Evaluation)
	}
	conflict := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_dedup_conflict")
	if conflict.EntityType != "operator_queue" || conflict.EntityID != "queue-source-rpc-1" || conflict.SourceEventID != "rtev-rpc-source-close" || conflict.SourceEventType != "operator_queue.resolved" {
		t.Fatalf("expected source-addressable conflict finding, got %+v", conflict)
	}
	if conflict.SourceDedupKey != "D-source-rpc-1" || conflict.SourceRootCauseID != "root-source-rpc-1" || conflict.SourceProvenanceGroupID != "prov-source-rpc-1" || conflict.SourceParentRefsJSON != `["rtev-rpc-source-parent"]` {
		t.Fatalf("expected canonical lineage on rpc conflict finding, got %+v", conflict)
	}
	ordering := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_parent_ref_out_of_order")
	if ordering.SourceEventID != "rtev-rpc-source-close" || ordering.SourceParentRefsJSON != `["rtev-rpc-source-parent"]` {
		t.Fatalf("expected causal-order finding to stay source-addressable over rpc, got %+v", ordering)
	}
	if report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithRootCauseID != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithProvenanceGroupID != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithParentRefs != 2 ||
		report.Evaluation.ProvenanceSummary.FullLineageFieldFindingCount != 2 {
		t.Fatalf("expected rpc replay provenance summary to surface source dedup-key lineage on source-addressable findings, got %+v", report.Evaluation.ProvenanceSummary)
	}
}

func TestWorkspaceEventsReplayRequiresParentRefsForFullLineage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-partial-lineage-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Partial Lineage RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-partial-lineage-open",
		"D-partial-lineage-rpc-1",
		workspaceID,
		"operator_queue.opened",
		"operator_queue",
		"queue-partial-lineage-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-partial-lineage-rpc-1",
		"prov-partial-lineage-rpc-1",
		`[]`,
		`{"queue_key":"queue:partial-lineage-rpc","queue_type":"FOLLOW_UP","status":"OPEN","title":"Partial lineage rpc queue","summary":"Opened first","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"D-partial-lineage-rpc-1","root_cause_id":"root-partial-lineage-rpc-1","provenance_group_id":"prov-partial-lineage-rpc-1"}`,
		"2026-03-22T20:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert partial-lineage rpc open runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-partial-lineage-close",
		"D-partial-lineage-rpc-1",
		workspaceID,
		"operator_queue.resolved",
		"operator_queue",
		"queue-partial-lineage-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-partial-lineage-rpc-1",
		"prov-partial-lineage-rpc-1",
		`[]`,
		`{"queue_key":"queue:partial-lineage-rpc","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Partial lineage rpc queue","summary":"Resolved later","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1","dedup_key":"D-partial-lineage-rpc-1","root_cause_id":"root-partial-lineage-rpc-1","provenance_group_id":"prov-partial-lineage-rpc-1"}`,
		"2026-03-22T20:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert partial-lineage rpc close runtime event: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.Evaluation.FindingSummary.DedupConflictCount != 1 || report.Evaluation.FindingSummary.TotalFindings != 1 {
		t.Fatalf("expected rpc replay to surface one source-addressable dedup conflict, got %+v", report.Evaluation.FindingSummary)
	}
	if report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithRootCauseID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithProvenanceGroupID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithParentRefs != 0 ||
		report.Evaluation.ProvenanceSummary.FullLineageFieldFindingCount != 0 {
		t.Fatalf("expected rpc replay full-lineage summary to require non-empty parent refs, got %+v", report.Evaluation.ProvenanceSummary)
	}
	conflict := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_dedup_conflict")
	if conflict.SourceEventID != "rtev-rpc-partial-lineage-close" || conflict.SourceRootCauseID != "root-partial-lineage-rpc-1" || conflict.SourceProvenanceGroupID != "prov-partial-lineage-rpc-1" {
		t.Fatalf("expected rpc replay conflict finding to keep source/root/provenance fields, got %+v", conflict)
	}
	if conflict.SourceParentRefsJSON != `[]` {
		t.Fatalf("expected rpc replay conflict finding to preserve empty parent-ref lineage without counting it as full lineage, got %+v", conflict)
	}
}

func TestWorkspaceEventsReplayAppliesAvailableParentsBeforeChildrenButKeepsIngestOrderFinding(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-topological-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Topological RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-topo-child",
		nil,
		workspaceID,
		"operator_queue.resolved",
		"operator_queue",
		"queue-topo-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-topo-rpc-1",
		"prov-topo-rpc-1",
		`["rtev-rpc-topo-parent"]`,
		`{"queue_id":"queue-topo-rpc-1","queue_key":"queue:topo-rpc-1","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Topo rpc queue","summary":"Child should apply after parent","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1","parent_refs_json":["rtev-rpc-topo-parent"]}`,
		"2026-03-24T12:00:00Z",
		2,
	); err != nil {
		t.Fatalf("insert rpc topological child event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-topo-parent",
		nil,
		workspaceID,
		"operator_queue.opened",
		"operator_queue",
		"queue-topo-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-topo-rpc-1",
		"prov-topo-rpc-1",
		`[]`,
		`{"queue_id":"queue-topo-rpc-1","queue_key":"queue:topo-rpc-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Topo rpc queue","summary":"Parent arrives later in ingest order","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-24T12:01:00Z",
		3,
	); err != nil {
		t.Fatalf("insert rpc topological parent event: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.EventsOrder != "latest_first_ingest" {
		t.Fatalf("expected replay report to label ingest event order, got %+v", report)
	}
	if report.AppliedOrder != "causal_parent_before_child" {
		t.Fatalf("expected replay report to label causal apply order, got %+v", report)
	}
	if len(report.AppliedEventIDs) != 2 || report.AppliedEventIDs[0] != "rtev-rpc-topo-parent" || report.AppliedEventIDs[1] != "rtev-rpc-topo-child" {
		t.Fatalf("expected rpc replay to expose causal apply order, got %+v", report.AppliedEventIDs)
	}
	if len(report.Queues) != 1 || report.Queues[0].QueueID != "queue-topo-rpc-1" {
		t.Fatalf("expected replay report to surface topological queue state, got %+v", report.Queues)
	}
	queue := report.Queues[0]
	if queue.Status != "RESOLVED" || queue.Resolution != "done" || queue.ResolvedBy != "operator-1" {
		t.Fatalf("expected rpc replay to apply parent before child and keep resolved queue state, got %+v", queue)
	}
	ordering := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_parent_ref_out_of_order")
	if ordering.SourceEventID != "rtev-rpc-topo-child" || ordering.SourceParentRefsJSON != `["rtev-rpc-topo-parent"]` {
		t.Fatalf("expected rpc replay to keep ingest-order warning lineage, got %+v", ordering)
	}
	if !strings.Contains(ordering.Message, "ingest order") {
		t.Fatalf("expected rpc replay finding to mention ingest order, got %+v", ordering)
	}
	if report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 0 ||
		report.Evaluation.ProvenanceSummary.FindingsWithRootCauseID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithProvenanceGroupID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithParentRefs != 1 ||
		report.Evaluation.ProvenanceSummary.FullLineageFieldFindingCount != 1 {
		t.Fatalf("expected rpc replay to surface bounded provenance summary for ordering finding, got %+v", report.Evaluation.ProvenanceSummary)
	}
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == "runtime_event_parent_ref_missing" {
			t.Fatalf("expected all-available parent refs to avoid missing-parent findings, got %+v", report.Evaluation.Findings)
		}
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected rpc replay to stay warn while ingest-order anomaly remains, got %+v", report.Evaluation)
	}
}

func TestWorkspaceEventsReplaySurfacesMissingParentFinding(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-missing-parent-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Missing Parent RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-missing-parent-child",
		nil,
		workspaceID,
		"legacy.signal",
		"legacy_event",
		"legacy-child-rpc",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-rpc-missing-parent",
		"prov-rpc-missing-parent",
		`["rtev-rpc-missing-parent"]`,
		`{"message":"missing parent edge"}`,
		"2026-03-22T21:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert runtime event with missing parent: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected missing parent replay to warn, got %+v", report.Evaluation)
	}
	finding := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_parent_ref_missing")
	if finding.SourceEventID != "rtev-rpc-missing-parent-child" || finding.SourceParentRefsJSON != `["rtev-rpc-missing-parent"]` {
		t.Fatalf("expected rpc missing parent finding to stay source-addressable, got %+v", finding)
	}
}

func TestWorkspaceEventsReplayPrefersFirstClassParentRefsOrderOverPayloadLineageOrder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-parent-order-lineage-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Parent Order Lineage RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-parent-order-a",
		nil,
		workspaceID,
		"legacy.signal",
		"legacy_event",
		"legacy-parent-a-rpc",
		"system",
		"tests",
		nil,
		nil,
		nil,
		nil,
		nil,
		`[]`,
		`{"message":"existing parent a"}`,
		"2026-03-23T11:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert existing rpc parent a: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-parent-order-child",
		"D-parent-order-rpc-1",
		workspaceID,
		"legacy.signal",
		"legacy_event",
		"legacy-child-rpc",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-parent-order-rpc-1",
		"prov-parent-order-rpc-1",
		`["rtev-rpc-parent-order-a","rtev-rpc-parent-order-missing"]`,
		`{"message":"payload lineage has different parent order","dedup_key":"D-parent-order-rpc-1","root_cause_id":"root-parent-order-rpc-1","provenance_group_id":"prov-parent-order-rpc-1","parent_refs_json":["rtev-rpc-parent-order-missing","rtev-rpc-parent-order-a"]}`,
		"2026-03-23T11:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert lineage-order rpc child: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected replay warning for missing parent, got %+v", report.Evaluation)
	}
	finding := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_parent_ref_missing")
	if finding.SourceEventID != "rtev-rpc-parent-order-child" || finding.SourceParentRefsJSON != `["rtev-rpc-parent-order-a","rtev-rpc-parent-order-missing"]` {
		t.Fatalf("expected rpc lineage to prefer first-class parent_refs order, got %+v", finding)
	}
}

func TestWorkspaceEventsReplaySurfacesParentRefCycleFindingAndFallbackOrder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-parent-cycle-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Parent Cycle RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-cycle-a",
		nil,
		workspaceID,
		"operator_queue.opened",
		"operator_queue",
		"queue-cycle-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-cycle-rpc-1",
		"prov-cycle-rpc-1",
		`["rtev-rpc-cycle-b"]`,
		`{"queue_id":"queue-cycle-rpc-1","queue_key":"queue:cycle-rpc-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Cycle rpc queue","summary":"Cycle root event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-25T12:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert rpc cycle event a: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-cycle-b",
		nil,
		workspaceID,
		"operator_queue.resolved",
		"operator_queue",
		"queue-cycle-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-cycle-rpc-1",
		"prov-cycle-rpc-1",
		`["rtev-rpc-cycle-a"]`,
		`{"queue_id":"queue-cycle-rpc-1","queue_key":"queue:cycle-rpc-1","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Cycle rpc queue","summary":"Cycle leaf event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1"}`,
		"2026-03-25T12:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert rpc cycle event b: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.EventsOrder != "latest_first_ingest" || report.AppliedOrder != "causal_parent_before_child_with_cycle_ingest_fallback" {
		t.Fatalf("expected rpc replay order metadata, got %+v", report)
	}
	if len(report.AppliedEventIDs) != 2 || report.AppliedEventIDs[0] != "rtev-rpc-cycle-a" || report.AppliedEventIDs[1] != "rtev-rpc-cycle-b" {
		t.Fatalf("expected rpc cycle replay to expose ingest fallback order, got %+v", report.AppliedEventIDs)
	}
	if len(report.Queues) != 1 || report.Queues[0].QueueKey != "queue:cycle-rpc-1" || report.Queues[0].Status != "RESOLVED" || report.Queues[0].Resolution != "done" {
		t.Fatalf("expected rpc cycle replay to keep final queue state, got %+v", report.Queues)
	}
	finding := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_parent_ref_cycle")
	if finding.SourceEventID != "rtev-rpc-cycle-a" || finding.SourceParentRefsJSON != `["rtev-rpc-cycle-b"]` {
		t.Fatalf("expected rpc cycle finding to stay source-addressable, got %+v", finding)
	}
	if report.Evaluation.FindingSummary.CycleCount != 1 ||
		report.Evaluation.FindingSummary.CycleSelfParentCount != 0 ||
		report.Evaluation.FindingSummary.CycleParentComponentCount != 1 {
		t.Fatalf("expected rpc cycle replay to isolate parent-component cycle subcount, got %+v", report.Evaluation.FindingSummary)
	}
	for _, item := range report.Evaluation.Findings {
		if item.Code == "runtime_event_parent_ref_missing" {
			t.Fatalf("expected rpc cycle replay to avoid missing-parent false positives, got %+v", report.Evaluation.Findings)
		}
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected rpc cycle replay to warn, got %+v", report.Evaluation)
	}
}

func TestWorkspaceEventsReplaySurfacesSelfParentFindingAndCycleSubcount(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-events-replay-self-parent-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Self Parent RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-rpc-self-parent",
		nil,
		workspaceID,
		"operator_queue.opened",
		"operator_queue",
		"queue-self-parent-rpc-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-self-parent-rpc-1",
		"prov-self-parent-rpc-1",
		`["rtev-rpc-self-parent"]`,
		`{"queue_id":"queue-self-parent-rpc-1","queue_key":"queue:self-parent-rpc-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Self parent rpc queue","summary":"Self parent should stay source-addressable","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-25T12:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert rpc self-parent event: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if len(report.AppliedEventIDs) != 1 || report.AppliedEventIDs[0] != "rtev-rpc-self-parent" {
		t.Fatalf("expected rpc self-parent event to remain in applied set once, got %+v", report.AppliedEventIDs)
	}
	finding := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_self_parent_ref")
	if finding.SourceEventID != "rtev-rpc-self-parent" || finding.SourceParentRefsJSON != `["rtev-rpc-self-parent"]` {
		t.Fatalf("expected rpc self-parent finding to stay source-addressable, got %+v", finding)
	}
	if report.Evaluation.FindingSummary.CycleCount != 1 ||
		report.Evaluation.FindingSummary.CycleSelfParentCount != 1 ||
		report.Evaluation.FindingSummary.CycleParentComponentCount != 0 {
		t.Fatalf("expected rpc self-parent replay to isolate self-parent cycle subcount, got %+v", report.Evaluation.FindingSummary)
	}
	for _, item := range report.Evaluation.Findings {
		if item.Code == "runtime_event_parent_ref_cycle" || item.Code == "runtime_event_parent_ref_missing" {
			t.Fatalf("expected rpc self-parent replay to avoid cycle-component/missing-parent false positives, got %+v", report.Evaluation.Findings)
		}
	}
	if report.Evaluation.Verdict != "fail" {
		t.Fatalf("expected rpc self-parent replay to fail, got %+v", report.Evaluation)
	}
}

func TestWorkspaceEventsReplaySurfacesRetentionRiskForDetachedCompactionSnapshot(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-replay-retention-risk-rpc"
		agentID     = "agent-retention-risk-rpc"
		sessionID   = "sess-retention-risk-rpc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Replay Retention Risk RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Retention Risk Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Replay retention risk seed",
		UpdatedAt:   "2026-03-26T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("record session coordination: %v", err)
	}
	payloadJSON, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal session replay payload: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev:" + sessionID + ":" + state.UpdateType + ":" + state.UpdatedAt,
		WorkspaceID: workspaceID,
		EventType:   state.UpdateType,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      state.TaskID,
		PayloadJSON: string(payloadJSON),
		CreatedAt:   state.UpdatedAt,
	}); err != nil {
		t.Fatalf("record runtime session event: %v", err)
	}

	snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:           sessionID,
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		TriggerKind:         "token_budget_exceeded",
		PackMode:            "DETERMINISTIC_FALLBACK",
		SourceWindowDigest:  "digest-rpc-retention-risk",
		MessageCountBefore:  8,
		MessageCountAfter:   3,
		MessageTokensBefore: 2600,
		MessageTokensAfter:  900,
		TotalInputTokens:    3200,
		TotalOutputTokens:   1200,
		SummaryText:         "[Previous conversation history was truncated due to length. 5 messages were removed.]",
	})
	if err != nil {
		t.Fatalf("record compaction snapshot: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE episode_packs SET compaction_snapshot_id = NULL WHERE pack_id = ?`, snapshot.EpisodePackID); err != nil {
		t.Fatalf("detach episode pack from compaction snapshot: %v", err)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	report, ok := payload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", payload["report"])
	}
	if report.Evaluation.Verdict != "warn" || report.Evaluation.WarningCount != 1 {
		t.Fatalf("expected rpc replay retention risk to preserve only the existing non-retention warning severity, got %+v", report.Evaluation)
	}
	risk := report.Evaluation.RetentionRisk
	if risk.Band != "AT_RISK" || risk.CompactionCandidateCount != 0 || risk.CompactionSnapshotCount != 1 || risk.EpisodePackCount != 0 {
		t.Fatalf("expected detached compaction snapshot retention risk, got %+v", risk)
	}
	if report.Evaluation.FindingSummary.RetentionFindingCount != 2 ||
		report.Evaluation.FindingSummary.RetentionCompactionCandidateCount != 0 ||
		report.Evaluation.FindingSummary.RetentionCompactedSessionCount != 1 ||
		report.Evaluation.FindingSummary.RetentionSnapshotWithoutEpisodePackCount != 1 {
		t.Fatalf("expected detached compaction snapshot replay to surface retention subcounts, got %+v", report.Evaluation.FindingSummary)
	}
	if risk.LatestSnapshotAt != snapshot.CreatedAt {
		t.Fatalf("expected latest snapshot at %q, got %+v", snapshot.CreatedAt, risk)
	}
	if len(risk.SnapshotSessionIDs) != 1 || risk.SnapshotSessionIDs[0] != sessionID {
		t.Fatalf("expected snapshot session ids to keep %s, got %+v", sessionID, risk.SnapshotSessionIDs)
	}
	seenReasons := map[string]bool{}
	for _, reason := range risk.Reasons {
		seenReasons[reason] = true
	}
	for _, reason := range []string{"session_compaction_snapshot_present", "snapshot_without_episode_pack"} {
		if !seenReasons[reason] {
			t.Fatalf("expected rpc replay retention reason %s, got %+v", reason, risk)
		}
	}
	compacted := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_retention_compacted_session")
	if compacted.EntityType != "agent_session" || compacted.EntityID != sessionID {
		t.Fatalf("expected compacted-session finding to address session %s, got %+v", sessionID, compacted)
	}
	summaryGap := requireRuntimeReplayFinding(t, report.Evaluation.Findings, "runtime_event_retention_snapshot_without_episode_pack")
	if summaryGap.EntityType != "" || summaryGap.EntityID != "" {
		t.Fatalf("expected detached pack retention warning to stay aggregate, got %+v", summaryGap)
	}
}

func requireRuntimeReplayFinding(t *testing.T, findings []sqlite.RuntimeReplayFinding, code string) sqlite.RuntimeReplayFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Code == code {
			return finding
		}
	}
	t.Fatalf("expected replay finding %s, got %+v", code, findings)
	return sqlite.RuntimeReplayFinding{}
}

func TestWorkspaceEventsRPCUsesIngestSequenceOrderingWhenAvailable(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	requireRuntimeEventIngestSequenceSupportForServer(t, ctx, store)

	const workspaceID = "ws-events-ingest-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Ingest RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	for _, event := range []sqlite.RuntimeEventInput{
		{
			EventID:     "rtev-rpc-z-same-ts",
			WorkspaceID: workspaceID,
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-rpc",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"first ingest at equal timestamp"}`,
			CreatedAt:   "2026-03-22T23:00:00Z",
		},
		{
			EventID:     "rtev-rpc-a-same-ts",
			WorkspaceID: workspaceID,
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-rpc",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"second ingest at equal timestamp"}`,
			CreatedAt:   "2026-03-22T23:00:00Z",
		},
		{
			EventID:     "rtev-rpc-backfill-old",
			WorkspaceID: workspaceID,
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-rpc",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"backfilled legacy event ingested last"}`,
			CreatedAt:   "2026-03-20T23:00:00Z",
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, event); err != nil {
			t.Fatalf("record runtime event %s: %v", event.EventID, err)
		}
	}

	rawList, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal events list params: %v", err)
	}
	listResult, rpcErr := h.workspaceEventsList(ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected events list result type %T", listResult)
	}
	items, ok := listPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected events list payload type %T", listPayload["items"])
	}
	if len(items) != 3 || items[0].EventID != "rtev-rpc-backfill-old" || items[1].EventID != "rtev-rpc-a-same-ts" || items[2].EventID != "rtev-rpc-z-same-ts" {
		t.Fatalf("expected events list to follow ingest sequence, got %+v", items)
	}
	if items[0].IngestSeq != 3 || items[1].IngestSeq != 2 || items[2].IngestSeq != 1 {
		t.Fatalf("expected events list to expose monotonic ingest sequence, got %+v", items)
	}

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   workspaceID,
		IncludeEvents: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	replayResult, rpcErr := h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayPayload, ok := replayResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", replayResult)
	}
	report, ok := replayPayload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay report type %T", replayPayload["report"])
	}
	if len(report.Events) != 3 || report.Events[0].EventID != "rtev-rpc-backfill-old" || report.Events[1].EventID != "rtev-rpc-a-same-ts" || report.Events[2].EventID != "rtev-rpc-z-same-ts" {
		t.Fatalf("expected replay report events to follow ingest sequence, got %+v", report.Events)
	}
	if report.Events[0].IngestSeq != 3 || report.Events[1].IngestSeq != 2 || report.Events[2].IngestSeq != 1 {
		t.Fatalf("expected replay report to expose monotonic ingest sequence, got %+v", report.Events)
	}
}

func requireRuntimeEventIngestSequenceSupportForServer(t *testing.T, ctx context.Context, store *sqlite.Store) {
	t.Helper()

	rows, err := store.DB().QueryContext(ctx, `PRAGMA table_info(runtime_events)`)
	if err != nil {
		t.Fatalf("query runtime_events table info: %v", err)
	}
	defer rows.Close()

	var (
		cid       int
		name      string
		typ       string
		notNull   int
		defaultV  any
		primaryK  int
		hasColumn bool
	)
	for rows.Next() {
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultV, &primaryK); err != nil {
			t.Fatalf("scan runtime_events table info: %v", err)
		}
		if strings.EqualFold(strings.TrimSpace(name), "ingest_seq") {
			hasColumn = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate runtime_events table info: %v", err)
	}
	if !hasColumn {
		t.Fatalf("expected runtime_events.ingest_seq support to be landed")
	}
}

func TestPublishRuntimeEventRecordSurfacesCanonicalEnvelopeFields(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	ch := h.GetEventBus().Subscribe("ws-runtime-live-envelope")
	defer h.GetEventBus().Unsubscribe("ws-runtime-live-envelope", ch)

	h.publishRuntimeEventRecord(sqlite.RuntimeEventRecord{
		EventID:           "rtev-live-envelope",
		DedupKey:          "live:dedup:1",
		WorkspaceID:       "ws-runtime-live-envelope",
		EventType:         "operator_queue.opened",
		EntityType:        "operator_queue",
		EntityID:          "queue-live-1",
		AgentID:           "agent-live",
		RootCauseID:       "root-live-1",
		ProvenanceGroupID: "prov-live-1",
		ParentRefsJSON:    `["rtev-parent-live-1"]`,
		PayloadJSON:       `{"summary":"Live canonical envelope"}`,
		CreatedAt:         "2026-03-22T18:30:00Z",
		IngestSeq:         42,
	})

	live := nextEvent(t, ch)
	assertValidEventTimestamp(t, live.Timestamp)
	if live.CanonicalEventType != "operator_queue.opened" {
		t.Fatalf("expected live canonical event type, got %+v", live)
	}
	if live.EventID != "rtev-live-envelope" || live.DedupKey != "live:dedup:1" {
		t.Fatalf("expected live event identity fields, got %+v", live)
	}
	if live.IngestSeq != 42 {
		t.Fatalf("expected live event ingest sequence, got %+v", live)
	}
	if live.EntityType != "operator_queue" || live.EntityID != "queue-live-1" {
		t.Fatalf("expected live event entity fields, got %+v", live)
	}
	if live.RootCauseID != "root-live-1" || live.ProvenanceGroupID != "prov-live-1" || live.ParentRefsJSON != `["rtev-parent-live-1"]` {
		t.Fatalf("expected live event lineage fields, got %+v", live)
	}
}

func TestPublishRuntimeEventRecordUsesTensionIDSummaryFallback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	ch := h.GetEventBus().Subscribe("ws-runtime-live-tension-summary")
	defer h.GetEventBus().Unsubscribe("ws-runtime-live-tension-summary", ch)

	h.publishRuntimeEventRecord(sqlite.RuntimeEventRecord{
		EventID:     "rtev-live-tension-summary",
		WorkspaceID: "ws-runtime-live-tension-summary",
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    "tension-123",
		PayloadJSON: `{"event_kind":"tension.confirmed","tension_id":"tension-123"}`,
		CreatedAt:   "2026-03-22T18:31:00Z",
		IngestSeq:   43,
	})

	live := nextEvent(t, ch)
	if live.Summary != "tension-123" {
		t.Fatalf("expected tension_id summary fallback, got %+v", live)
	}
}

func TestPublishRuntimeEventRecordAliasCarriesCanonicalEventType(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-runtime-alias-canonical"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Alias Canonical Type",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "tests",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	record, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    "msg-alias-canonical",
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		PayloadJSON: `{"message_id":"msg-alias-canonical","from":"agent-a","to_agent_id":"agent-b","status":"SENT"}`,
		CreatedAt:   "2026-03-27T09:00:00Z",
	})
	if err != nil {
		t.Fatalf("record runtime event: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	liveAgentID := "agent-b"
	h.publishRuntimeEventRecordAlias(record, "agent.message", &liveAgentID, "", "Message from agent-a")

	live := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEventWithAgentID(t, live, record, "agent.message", "agent-b")
}

func TestInstrumentationSnapshotAndWorkspacePolicyRPCStaySeparated(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawPolicyPut, err := json.Marshal(workspacePolicyPutParams{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
		Effect:      "REQUIRE_APPROVAL",
		Reason:      "control read-side separation coverage",
		CreatedBy:   "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal policy put params: %v", err)
	}
	result, rpcErr := h.workspacePolicyPut(testAuthContext(scenario.workspaceID, "human", "dashboard"), rawPolicyPut)
	if rpcErr != nil {
		t.Fatalf("workspacePolicyPut rpc error: %+v", rpcErr)
	}
	policyPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected policy put result type %T", result)
	}
	policy, ok := policyPayload["policy"].(sqlite.CapabilityPolicyRecord)
	if !ok {
		t.Fatalf("unexpected policy payload type %T", policyPayload["policy"])
	}
	if policy.PolicyID == "" || policy.Effect != "REQUIRE_APPROVAL" {
		t.Fatalf("unexpected policy payload %+v", policy)
	}

	livePolicy := nextEvent(t, ch)
	if livePolicy.Type != "workspace.policy.put" {
		t.Fatalf("expected workspace.policy.put live event, got %+v", livePolicy)
	}
	var livePolicyPayload sqlite.CapabilityPolicyRecord
	if err := json.Unmarshal([]byte(livePolicy.PayloadJSON), &livePolicyPayload); err != nil {
		t.Fatalf("decode policy live payload: %v", err)
	}
	if livePolicyPayload.PolicyID != policy.PolicyID {
		t.Fatalf("expected live policy payload %s, got %+v", policy.PolicyID, livePolicyPayload)
	}
	policyEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    policy.PolicyID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, livePolicy, policyEvent, "workspace.policy.put")

	rawSnapshot, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.primaryTaskID,
		Limit:        100,
		ClusterLimit: 1,
		ActorID:      "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal snapshot params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	snapshotEvent, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected snapshot event payload type %T", snapshotPayload["event"])
	}
	if snapshotEvent.EventType != "cluster.metric_snapshot" {
		t.Fatalf("expected cluster.metric_snapshot event, got %+v", snapshotEvent)
	}

	liveSnapshot := nextEvent(t, ch)
	if liveSnapshot.Type != "cluster.metric_snapshot" {
		t.Fatalf("expected cluster.metric_snapshot live event, got %+v", liveSnapshot)
	}
	assertValidEventTimestamp(t, liveSnapshot.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveSnapshot, snapshotEvent, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveSnapshot.PayloadJSON), snapshotEvent.PayloadJSON)

	rawPolicyList, err := json.Marshal(workspacePolicyListParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal policy list params: %v", err)
	}
	result, rpcErr = h.workspacePolicyList(testAuthContext(scenario.workspaceID, "system", "tests"), rawPolicyList)
	if rpcErr != nil {
		t.Fatalf("workspacePolicyList rpc error: %+v", rpcErr)
	}
	listPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected policy list result type %T", result)
	}
	policies, ok := listPayload["items"].([]sqlite.CapabilityPolicyRecord)
	if !ok || len(policies) != 1 || policies[0].PolicyID != policy.PolicyID {
		t.Fatalf("expected one persisted capability policy, got %+v", listPayload)
	}

	rawPolicyCheck, err := json.Marshal(workspacePolicyCheckParams{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "deploy-tool",
	})
	if err != nil {
		t.Fatalf("marshal policy check params: %v", err)
	}
	result, rpcErr = h.workspacePolicyCheck(testAuthContext(scenario.workspaceID, "system", "tests"), rawPolicyCheck)
	if rpcErr != nil {
		t.Fatalf("workspacePolicyCheck rpc error: %+v", rpcErr)
	}
	checkPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected policy check result type %T", result)
	}
	check, ok := checkPayload["check"].(sqlite.CapabilityCheckResult)
	if !ok {
		t.Fatalf("unexpected policy check payload type %T", checkPayload["check"])
	}
	if check.Verdict != "REQUIRE_APPROVAL" || check.MatchedPolicy == nil || check.MatchedPolicy.PolicyID != policy.PolicyID {
		t.Fatalf("unexpected capability policy check %+v", check)
	}

	rawSnapshotEvents, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.metric_snapshot",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal snapshot events params: %v", err)
	}
	result, rpcErr = h.workspaceEventsList(ctx, rawSnapshotEvents)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList snapshot rpc error: %+v", rpcErr)
	}
	eventListPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot events list type %T", result)
	}
	snapshotEvents, ok := eventListPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok || len(snapshotEvents) != 1 || snapshotEvents[0].EventID != snapshotEvent.EventID {
		t.Fatalf("expected one persisted cluster.metric_snapshot event, got %+v", eventListPayload)
	}

	rawPolicyEvents, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: scenario.workspaceID,
		EventType:   "workspace.policy.put",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal policy events params: %v", err)
	}
	result, rpcErr = h.workspaceEventsList(ctx, rawPolicyEvents)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList policy rpc error: %+v", rpcErr)
	}
	eventListPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected policy events list type %T", result)
	}
	policyEvents, ok := eventListPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected policy events payload type %T", eventListPayload["items"])
	}
	if len(policyEvents) != 0 {
		t.Fatalf("expected workspace.policy.put alias events to stay out of runtime journal, got %+v", policyEvents)
	}

	rawCanonicalPolicyEvents, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: scenario.workspaceID,
		EventType:   "capability_policy.put",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal canonical policy events params: %v", err)
	}
	result, rpcErr = h.workspaceEventsList(ctx, rawCanonicalPolicyEvents)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList canonical policy rpc error: %+v", rpcErr)
	}
	eventListPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected canonical policy events list type %T", result)
	}
	policyEvents, ok = eventListPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok || len(policyEvents) != 1 || policyEvents[0].EventID != policyEvent.EventID {
		t.Fatalf("expected canonical capability policy runtime event, got %+v", eventListPayload)
	}
}

func TestWorkspaceControlCommandRequestRPCUsesCanonicalJournalPath(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawCommand, err := json.Marshal(workspaceControlCommandRequestParams{
		WorkspaceID: scenario.workspaceID,
		CommandType: sqlite.ControlCommandRefreshKernel,
		AgentID:     "agent-a",
		Reason:      "operator requested bounded refresh",
		RequestedBy: "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal control command params: %v", err)
	}
	result, rpcErr := h.workspaceControlCommandRequest(testAuthContext(scenario.workspaceID, "human", "dashboard"), rawCommand)
	if rpcErr != nil {
		t.Fatalf("workspaceControlCommandRequest rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected control command result type %T", result)
	}
	command, ok := payload["command"].(sqlite.ControlCommandRecord)
	if !ok {
		t.Fatalf("unexpected control command payload type %T", payload["command"])
	}
	if command.CommandID == "" || command.CommandType != sqlite.ControlCommandRefreshKernel || command.AppliedInline {
		t.Fatalf("unexpected control command payload %+v", command)
	}
	if command.Ownership.ActuatorOwner != "RMP" || command.Ownership.RMP != "actuator_owner" || command.Ownership.RSP != "advisory_only" || command.Ownership.ApplyMode != "journal_request_only" {
		t.Fatalf("expected control command ownership matrix in rpc payload, got %+v", command.Ownership)
	}

	live := nextEvent(t, ch)
	if live.Type != "workspace.control.command.request" {
		t.Fatalf("expected workspace.control.command.request live event, got %+v", live)
	}
	var livePayload sqlite.ControlCommandRecord
	if err := json.Unmarshal([]byte(live.PayloadJSON), &livePayload); err != nil {
		t.Fatalf("decode control command live payload: %v", err)
	}
	if livePayload.CommandID != command.CommandID {
		t.Fatalf("expected live command payload %s, got %+v", command.CommandID, livePayload)
	}
	if !reflect.DeepEqual(livePayload.Ownership, command.Ownership) {
		t.Fatalf("expected live command payload to preserve ownership matrix, got live=%+v command=%+v", livePayload.Ownership, command.Ownership)
	}

	commandEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "control.command.requested",
		EntityType:  "control_command",
		EntityID:    command.CommandID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, live, commandEvent, "workspace.control.command.request")

	rawAliasEvents, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: scenario.workspaceID,
		EventType:   "workspace.control.command.request",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal command alias events params: %v", err)
	}
	result, rpcErr = h.workspaceEventsList(ctx, rawAliasEvents)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList command alias rpc error: %+v", rpcErr)
	}
	eventListPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected command alias events list type %T", result)
	}
	aliasEvents, ok := eventListPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected command alias events payload type %T", eventListPayload["items"])
	}
	if len(aliasEvents) != 0 {
		t.Fatalf("expected workspace.control.command.request alias events to stay out of runtime journal, got %+v", aliasEvents)
	}

	rawCanonicalEvents, err := json.Marshal(workspaceEventsListParams{
		WorkspaceID: scenario.workspaceID,
		EventType:   "control.command.requested",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal canonical command events params: %v", err)
	}
	result, rpcErr = h.workspaceEventsList(ctx, rawCanonicalEvents)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsList canonical command rpc error: %+v", rpcErr)
	}
	eventListPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected canonical command events list type %T", result)
	}
	commandEvents, ok := eventListPayload["items"].([]sqlite.RuntimeEventRecord)
	if !ok || len(commandEvents) != 1 || commandEvents[0].EventID != commandEvent.EventID {
		t.Fatalf("expected canonical control command runtime event, got %+v", eventListPayload)
	}
}

func assertNextTwoLiveEventsMirrorRuntimeEventsInOrder(t *testing.T, ch <-chan EventMessage, first sqlite.RuntimeEventRecord, firstType string, second sqlite.RuntimeEventRecord, secondType string) {
	t.Helper()

	if runtimeEventChronologicalLess(second, first) {
		first, second = second, first
		firstType, secondType = secondType, firstType
	}

	firstLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, firstLive, first, firstType)

	secondLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, secondLive, second, secondType)
}

func TestOperatorQueueRuntimeEventLiveTypeMapsRebaseFollowupToWorkspaceOpsUpdated(t *testing.T) {
	if got := operatorQueueRuntimeEventLiveType("operator_queue.rebase_followup_created"); got != "workspace.ops.updated" {
		t.Fatalf("operatorQueueRuntimeEventLiveType(rebase_followup_created) = %q, want workspace.ops.updated", got)
	}
	if got := operatorQueueRuntimeEventLiveType("operator_queue.reopened"); got != "workspace.ops.updated" {
		t.Fatalf("operatorQueueRuntimeEventLiveType(reopened) = %q, want workspace.ops.updated", got)
	}
	if got := operatorQueueRuntimeEventLiveType("operator_queue.resolved"); got != "workspace.ops.resolved" {
		t.Fatalf("operatorQueueRuntimeEventLiveType(resolved) = %q, want workspace.ops.resolved", got)
	}
	if got := operatorQueueRuntimeEventLiveType("operator_queue.cancelled"); got != "workspace.ops.resolved" {
		t.Fatalf("operatorQueueRuntimeEventLiveType(cancelled) = %q, want workspace.ops.resolved", got)
	}
	if got := operatorQueueRuntimeEventLiveType("operator_queue.unknown"); got != "" {
		t.Fatalf("operatorQueueRuntimeEventLiveType(unknown) = %q, want empty", got)
	}
}
