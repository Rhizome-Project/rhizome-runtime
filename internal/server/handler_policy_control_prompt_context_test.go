package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestPolicyControlRPCPromptContextEnvelopesDurableEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const workspaceID = "ws-policy-control-rpc-prompt-context"
	ctx := testAuthContext(workspaceID, "human", "operator-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Policy Control RPC Prompt Context",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	policyResult, rpcErr := h.workspacePolicyPut(ctx, mustJSONRaw(workspacePolicyPutParams{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "REQUIRE_APPROVAL",
		Reason:      "operator required approval",
		CreatedBy:   "operator-a",
	}))
	if rpcErr != nil {
		t.Fatalf("workspacePolicyPut rpc error: %+v", rpcErr)
	}
	policyPayload := policyResult.(map[string]any)
	policy := policyPayload["policy"].(sqlite.CapabilityPolicyRecord)
	policyLive := nextEvent(t, ch)
	policyEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    policy.PolicyID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, policyLive, policyEvent, "workspace.policy.put")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, policyLive.PayloadJSON), policyEvent.PayloadJSON)
	assertServerPolicyControlPromptContext(t, policyEvent, "authority_bearing_capability_policy_write", "workspace.policy.put", workspaceID, "human", "operator-a", map[string]string{
		"policy_id":    policy.PolicyID,
		"subject_type": "agent",
		"subject_id":   "agent-a",
		"capability":   "tool.call",
		"tool_id":      "dangerous-tool",
		"effect":       "REQUIRE_APPROVAL",
		"created_by":   "operator-a",
		"event_type":   "capability_policy.put",
		"entity_type":  "capability_policy",
		"entity_id":    policy.PolicyID,
	})

	commandResult, rpcErr := h.workspaceControlCommandRequest(ctx, mustJSONRaw(workspaceControlCommandRequestParams{
		WorkspaceID: workspaceID,
		CommandType: sqlite.ControlCommandRefreshKernel,
		AgentID:     "agent-a",
		Reason:      "operator requested bounded refresh",
		RequestedBy: "operator-a",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceControlCommandRequest rpc error: %+v", rpcErr)
	}
	commandPayload := commandResult.(map[string]any)
	command := commandPayload["command"].(sqlite.ControlCommandRecord)
	commandLive := nextEvent(t, ch)
	commandEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "control.command.requested",
		EntityType:  "control_command",
		EntityID:    command.CommandID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, commandLive, commandEvent, "workspace.control.command.request")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, commandLive.PayloadJSON), commandEvent.PayloadJSON)
	assertServerPolicyControlPromptContext(t, commandEvent, "authority_bearing_control_command_write", "workspace.control.command.request", workspaceID, "human", "operator-a", map[string]string{
		"command_id":     command.CommandID,
		"command_type":   sqlite.ControlCommandRefreshKernel,
		"scope":          "agent",
		"requested_by":   "operator-a",
		"actor_type":     "operator",
		"agent_id":       "agent-a",
		"applied_inline": "false",
		"event_type":     "control.command.requested",
		"entity_type":    "control_command",
		"entity_id":      command.CommandID,
	})
}

func TestWorkspaceRSPCapabilityPutAddsPromptContextToGeneratedPolicies(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const workspaceID = "ws-rsp-capability-rpc-prompt-context"
	ctx := context.Background()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Capability RPC Prompt Context",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	enable := true
	if _, rpcErr := h.workspaceRSPCapabilityPut(testAuthContext(workspaceID, "human", "operator-a"), mustJSONRaw(workspaceRSPCapabilityPutParams{
		WorkspaceID:             workspaceID,
		GovernedHintsLive:       &enable,
		SafeLocalAutonomicsLive: &enable,
		UpdatedBy:               "operator-a",
		Reason:                  "enable controlled rsp rollout",
	})); rpcErr != nil {
		t.Fatalf("workspaceRSPCapabilityPut rpc error: %+v", rpcErr)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list generated rsp capability events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected two generated rsp capability events, got %+v", events)
	}
	for _, event := range events {
		payload := decodeEventPayloadMap(t, event.PayloadJSON)
		capability, _ := payload["capability"].(string)
		assertServerPolicyControlPromptContext(t, event, "authority_bearing_capability_policy_write", "workspace.rsp.capability.put", workspaceID, "human", "operator-a", map[string]string{
			"policy_id":    event.EntityID,
			"subject_type": "workspace",
			"subject_id":   workspaceID,
			"capability":   capability,
			"tool_id":      "*",
			"created_by":   "operator-a",
			"event_type":   "capability_policy.put",
			"entity_type":  "capability_policy",
			"entity_id":    event.EntityID,
		})
	}
}

func assertServerPolicyControlPromptContext(t *testing.T, event sqlite.RuntimeEventRecord, contextKind, surface, workspaceID, principalType, principalID string, extra map[string]string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       contextKind,
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		if got, _ := envelope[key].(string); got != want {
			t.Fatalf("expected prompt_context_envelope[%s]=%q, got %q in %+v", key, want, got, envelope)
		}
	}
}
