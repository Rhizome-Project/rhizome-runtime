package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPolicyControlPromptContextEnvelopeCarriesPrimaryRuntimeEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-policy-control-prompt-context"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Policy Control Prompt Context",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	policy, policyEvent, err := store.PutCapabilityPolicyWithEvent(ctx, CapabilityPolicyInput{
		PolicyID:              "policy-prompt-context",
		WorkspaceID:           workspaceID,
		SubjectType:           "agent",
		SubjectID:             "agent-a",
		Capability:            "tool.call",
		ToolID:                "dangerous-tool",
		Effect:                "REQUIRE_APPROVAL",
		Reason:                "operator required approval",
		CreatedBy:             "operator-a",
		PromptContextEnvelope: BuildCapabilityPolicyPromptContextEnvelope("workspace.policy.put", "server_rpc", workspaceID, "human", "operator-a"),
	})
	if err != nil {
		t.Fatalf("put capability policy with prompt context: %v", err)
	}
	assertPolicyControlPromptContextEnvelope(t, policyEvent.PayloadJSON, "authority_bearing_capability_policy_write", "workspace.policy.put", workspaceID, "human", "operator-a", map[string]string{
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
		"actor_type":   "operator",
		"actor_id":     "operator-a",
	})

	command, commandEvent, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		CommandID:             "ctrlcmd-prompt-context",
		WorkspaceID:           workspaceID,
		CommandType:           ControlCommandRefreshKernel,
		AgentID:               "agent-a",
		Reason:                "operator requested bounded refresh",
		RequestedBy:           "operator-a",
		PromptContextEnvelope: BuildControlCommandPromptContextEnvelope("workspace.control.command.request", "server_rpc", workspaceID, "human", "operator-a"),
	})
	if err != nil {
		t.Fatalf("request control command with prompt context: %v", err)
	}
	assertPolicyControlPromptContextEnvelope(t, commandEvent.PayloadJSON, "authority_bearing_control_command_write", "workspace.control.command.request", workspaceID, "human", "operator-a", map[string]string{
		"command_id":     command.CommandID,
		"command_type":   ControlCommandRefreshKernel,
		"scope":          "agent",
		"requested_by":   "operator-a",
		"actor_type":     "operator",
		"agent_id":       "agent-a",
		"applied_inline": "false",
		"event_type":     controlCommandEventType,
		"entity_type":    controlCommandEntityType,
		"entity_id":      command.CommandID,
	})
}

func TestRSPCapabilityFlagsPromptContextEnvelopeBindsEachGeneratedPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-rsp-policy-prompt-context"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Policy Prompt Context",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.SetRSPCapabilityFlags(ctx, SetRSPCapabilityFlagsInput{
		WorkspaceID:             workspaceID,
		GovernedHintsLive:       boolPtr(true),
		SafeLocalAutonomicsLive: boolPtr(false),
		UpdatedBy:               "operator-a",
		Reason:                  "roll forward governed hints and lock local autonomics",
		PromptContextEnvelope:   BuildCapabilityPolicyPromptContextEnvelope("workspace.rsp.capability.put", "server_rpc", workspaceID, "human", "operator-a"),
	}); err != nil {
		t.Fatalf("set rsp capability flags with prompt context: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list rsp capability policy events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected two rsp capability policy events, got %+v", events)
	}
	seen := map[string]bool{}
	for _, event := range events {
		payload := decodePolicyControlPromptPayload(t, event.PayloadJSON)
		capability, _ := payload["capability"].(string)
		seen[capability] = true
		assertPolicyControlPromptContextEnvelope(t, event.PayloadJSON, "authority_bearing_capability_policy_write", "workspace.rsp.capability.put", workspaceID, "human", "operator-a", map[string]string{
			"policy_id":    event.EntityID,
			"subject_type": "workspace",
			"subject_id":   workspaceID,
			"capability":   capability,
			"tool_id":      "*",
			"created_by":   "operator-a",
			"event_type":   "capability_policy.put",
			"entity_type":  "capability_policy",
			"entity_id":    event.EntityID,
			"actor_type":   "operator",
			"actor_id":     "operator-a",
		})
	}
	if !seen[rspCapabilityGovernedHintsLive] || !seen[rspCapabilitySafeLocalAutonomics] {
		t.Fatalf("expected prompt context on both generated rsp policy events, got capabilities %+v", seen)
	}
}

func TestPolicyPromptContextEnvelopeRejectsForgedBindingsAndRollsBack(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{
			name: "wrong surface",
			mutate: func(envelope map[string]any) {
				envelope["surface"] = "workspace.control.command.request"
			},
			want: "not valid for capability_policy",
		},
		{
			name: "wrong policy id",
			mutate: func(envelope map[string]any) {
				envelope["policy_id"] = "policy-other"
			},
			want: "policy_id",
		},
		{
			name: "wrong effect",
			mutate: func(envelope map[string]any) {
				envelope["effect"] = "ALLOW"
			},
			want: "effect",
		},
		{
			name: "wrong principal",
			mutate: func(envelope map[string]any) {
				envelope["principal_id"] = "mallory"
			},
			want: "principal_id",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			store := NewTestStore(t)
			const workspaceID = "ws-policy-prompt-forged"
			if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
				WorkspaceID: workspaceID,
				Title:       "Policy Prompt Forged",
				CreatedBy:   "tester",
			}); err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
			envelope := BuildCapabilityPolicyPromptContextEnvelope("workspace.policy.put", "server_rpc", workspaceID, "human", "operator-a")
			tc.mutate(envelope)
			_, _, err := store.PutCapabilityPolicyWithEvent(ctx, CapabilityPolicyInput{
				PolicyID:              "policy-forged",
				WorkspaceID:           workspaceID,
				SubjectType:           "agent",
				SubjectID:             "agent-a",
				Capability:            "tool.call",
				ToolID:                "dangerous-tool",
				Effect:                "DENY",
				CreatedBy:             "operator-a",
				PromptContextEnvelope: envelope,
			})
			if err == nil {
				t.Fatal("expected forged policy prompt context to fail")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected forged policy prompt context error: %v", err)
			}
			if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM workspace_capability_policies WHERE workspace_id = ?`, workspaceID); got != 0 {
				t.Fatalf("expected policy row rollback after forged prompt context reject, got %d", got)
			}
			if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = 'capability_policy.put'`, workspaceID); got != 0 {
				t.Fatalf("expected no capability policy event after forged prompt context reject, got %d", got)
			}
		})
	}
}

func TestRSPCapabilityFlagsPromptContextEnvelopeRejectsForgedPrincipalAndRollsBack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-rsp-policy-prompt-forged-principal"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Policy Prompt Forged Principal",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	envelope := BuildCapabilityPolicyPromptContextEnvelope("workspace.rsp.capability.put", "server_rpc", workspaceID, "human", "mallory")
	_, err := store.SetRSPCapabilityFlags(ctx, SetRSPCapabilityFlagsInput{
		WorkspaceID:             workspaceID,
		GovernedHintsLive:       boolPtr(true),
		SafeLocalAutonomicsLive: boolPtr(false),
		UpdatedBy:               "operator-a",
		Reason:                  "forged principal should not bind to generated policies",
		PromptContextEnvelope:   envelope,
	})
	if err == nil {
		t.Fatal("expected forged rsp capability prompt context to fail")
	}
	if !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("unexpected forged rsp capability error: %v", err)
	}
	if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM workspace_capability_policies WHERE workspace_id = ?`, workspaceID); got != 0 {
		t.Fatalf("expected no rsp policy rows after forged prompt context reject, got %d", got)
	}
	if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = 'capability_policy.put'`, workspaceID); got != 0 {
		t.Fatalf("expected no rsp capability policy events after forged prompt context reject, got %d", got)
	}
}

func TestControlCommandPromptContextEnvelopeRejectsForgedBindingsAndRollsBackInlineExclusion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-control-command-prompt-forged"
	tensionID := "tension:task:" + workspaceID + "/task-a"
	const agentID = "agent-alpha"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Prompt Forged",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	referenceAt := time.Now().UTC().Round(0)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       tensionID,
		WorkspaceID:     workspaceID,
		ProtoClusterID:  "task:" + workspaceID + "/task-a",
		TensionType:     "bottleneck",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Forged control command tension",
		Summary:         "Ensures forged prompt context rolls back inline exclusion",
		AnchorKind:      "entity_id",
		AnchorRef:       "task-a",
		TaskIDs:         []string{"task-a"},
		BaseScore:       1,
		SurfaceScore:    1,
		EvidenceCount:   1,
		LastSeenEventID: "evt-control-command-prompt-forged",
		LastSeenAt:      referenceAt.Format(time.RFC3339Nano),
		ConfirmedBy:     "tester",
		CreatedAt:       referenceAt.Format(time.RFC3339Nano),
		UpdatedAt:       referenceAt.Format(time.RFC3339Nano),
	})
	setWorkspaceControlEpochAnchor(t, store, workspaceID, referenceAt.Format(time.RFC3339Nano))

	envelope := BuildControlCommandPromptContextEnvelope("workspace.control.command.request", "server_rpc", workspaceID, "human", "operator-a")
	envelope["principal_id"] = "mallory"
	_, _, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		CommandID:             "ctrlcmd-forged",
		WorkspaceID:           workspaceID,
		CommandType:           ControlCommandExcludeAgentTension,
		TensionID:             tensionID,
		AgentID:               agentID,
		TTLSeconds:            60,
		Reason:                "thrash",
		RequestedBy:           "operator-a",
		PromptContextEnvelope: envelope,
	})
	if err == nil {
		t.Fatal("expected forged control command prompt context to fail")
	}
	if !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("unexpected forged control command error: %v", err)
	}
	if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM runtime_events WHERE workspace_id = ? AND event_type = 'control.command.requested'`, workspaceID); got != 0 {
		t.Fatalf("expected no control command event after forged prompt context reject, got %d", got)
	}
	if got := countPolicyControlRows(t, ctx, store, `SELECT COUNT(*) FROM workspace_tension_exclusions WHERE workspace_id = ?`, workspaceID); got != 0 {
		t.Fatalf("expected inline tension exclusion rollback after forged prompt context reject, got %d", got)
	}
}

func TestPolicyControlLegacyDirectWritesDoNotOverclaimPromptContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-policy-control-legacy-no-overclaim"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Policy Control Legacy No Overclaim",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	_, policyEvent, err := store.PutCapabilityPolicyWithEvent(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "DENY",
		CreatedBy:   "operator-a",
	})
	if err != nil {
		t.Fatalf("legacy put policy: %v", err)
	}
	assertPolicyControlNoPromptContextEnvelope(t, policyEvent.PayloadJSON)

	_, commandEvent, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandRefreshKernel,
		AgentID:     "agent-a",
		Reason:      "legacy direct command",
		RequestedBy: "operator-a",
	})
	if err != nil {
		t.Fatalf("legacy request control command: %v", err)
	}
	assertPolicyControlNoPromptContextEnvelope(t, commandEvent.PayloadJSON)
}

func assertPolicyControlPromptContextEnvelope(t *testing.T, payloadJSON, contextKind, surface, workspaceID, principalType, principalID string, extra map[string]string) {
	t.Helper()
	payload := decodePolicyControlPromptPayload(t, payloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload %+v", payload)
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
			t.Fatalf("expected envelope %s=%q, got %q in %+v", key, want, got, envelope)
		}
	}
}

func assertPolicyControlNoPromptContextEnvelope(t *testing.T, payloadJSON string) {
	t.Helper()
	payload := decodePolicyControlPromptPayload(t, payloadJSON)
	if _, ok := payload["prompt_context_envelope"]; ok {
		t.Fatalf("did not expect prompt_context_envelope in payload %+v", payload)
	}
}

func decodePolicyControlPromptPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode policy/control prompt payload: %v; payload=%q", err, payloadJSON)
	}
	return payload
}

func countPolicyControlRows(t *testing.T, ctx context.Context, store *Store, query, workspaceID string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx, query, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count rows for query %q: %v", query, err)
	}
	return count
}
