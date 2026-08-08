package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentProfileUpdateWithEventRecordsAuthorityBoundPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-profile-prompt-context"
		agentID     = "agent-profile-prompt-context"
		actorID     = "operator-profile"
	)
	seedAgentProfilePromptContextWorkspace(t, ctx, store, workspaceID, agentID, actorID)

	profile, event, err := store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		ActorType:      "human",
		Bio:            "Observes the system without direct participation.",
		Specialization: "meta-analysis reviewer",
		Tags:           []string{"observer", "runtime"},
		ToolsAccess:    []string{"agent.work.next"},
		Metadata: map[string]any{
			"default_work_mode": "observer",
			"cadence":           "slow",
		},
		PromptContextEnvelope: sqlite.BuildAgentProfilePromptContextEnvelope(
			"agent.profile.update",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "agent.profile.update",
	})
	if err != nil {
		t.Fatalf("upsert agent profile with event: %v", err)
	}
	if profile.WorkspaceID != workspaceID || profile.AgentID != agentID || profile.Specialization != "meta-analysis reviewer" {
		t.Fatalf("unexpected profile %+v", profile)
	}
	if event.EventType != "agent.profile.updated" || event.EntityType != "agent_profile" || event.EntityID != agentID || event.AgentID != agentID {
		t.Fatalf("unexpected profile runtime event %+v", event)
	}
	payload := decodeAgentProfilePromptPayload(t, event.PayloadJSON)
	assertAgentProfilePromptContext(t, payload, workspaceID, "human", actorID, agentID, actorID)
	if got, ok := payload["autonomous_execution_allowed_after"].(bool); !ok || got {
		t.Fatalf("expected observer profile to disable autonomous work selection, got %+v in %+v", payload["autonomous_execution_allowed_after"], payload)
	}
	keys, ok := payload["metadata_keys"].([]any)
	if !ok || len(keys) != 2 || keys[0] != "cadence" || keys[1] != "default_work_mode" {
		t.Fatalf("metadata_keys not sorted/minimal: %+v in %+v", payload["metadata_keys"], payload)
	}
	if got, ok := payload["profile_gate_basis_sha256"].(string); !ok || len(got) != 64 {
		t.Fatalf("expected stable profile gate basis hash, got %+v in %+v", payload["profile_gate_basis_sha256"], payload)
	}
	fields, ok := payload["profile_gate_basis_fields"].([]any)
	if !ok || len(fields) != 4 || fields[0] != "bio" || fields[3] != "metadata.default_work_mode" {
		t.Fatalf("expected explicit profile gate basis fields, got %+v in %+v", payload["profile_gate_basis_fields"], payload)
	}
}

func TestAgentProfileUpdateWithEventControlsWorkNextProfileGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-profile-event-work-gate"
		agentID     = "agent-profile-gated"
		actorID     = "developer"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createAgentWorkTask(t, ctx, store, workspaceID, "task-free", "high")

	_, _, err := store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		ActorType:      "human",
		Bio:            "Execute assigned tasks.",
		Specialization: "worker",
		Tags:           []string{"worker"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
		PromptContextEnvelope: sqlite.BuildAgentProfilePromptContextEnvelope(
			"agent.profile.update",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "agent.profile.update",
	})
	if err != nil {
		t.Fatalf("set worker profile with event: %v", err)
	}
	workerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("get worker work next: %v", err)
	}
	if !workerResult.HasWork || workerResult.Reason != "next_pending" || workerResult.Task == nil || workerResult.Task.TaskID != "task-free" {
		t.Fatalf("expected evented worker profile to allow pending work, got %+v", workerResult)
	}

	_, event, err := store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		ActorType:      "human",
		Bio:            "Analyze global system dynamics without direct participation.",
		Specialization: "meta-analysis",
		Tags:           []string{"observer"},
		Metadata: map[string]any{
			"default_work_mode": "observer",
		},
		PromptContextEnvelope: sqlite.BuildAgentProfilePromptContextEnvelope(
			"agent.profile.update",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "agent.profile.update",
	})
	if err != nil {
		t.Fatalf("set observer profile with event: %v", err)
	}
	payload := decodeAgentProfilePromptPayload(t, event.PayloadJSON)
	if got, ok := payload["autonomous_execution_allowed_after"].(bool); !ok || got {
		t.Fatalf("expected observer event payload to record scheduler gate closure, got %+v in %+v", payload["autonomous_execution_allowed_after"], payload)
	}
	observerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("get observer work next: %v", err)
	}
	if observerResult.HasWork || observerResult.Reason != "profile_gate_closed" {
		t.Fatalf("expected evented observer profile to suppress fresh autonomous work, got %+v", observerResult)
	}
	if !observerResult.ProfileGateBlockedWork || observerResult.AutonomousExecutionAllowed {
		t.Fatalf("expected evented observer profile to expose closed profile gate, got %+v", observerResult)
	}

	trustFirstResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		IncludeHydration: true,
		IncludeAllDocs:   true,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get trust-first observer work next: %v", err)
	}
	if !trustFirstResult.HasWork || trustFirstResult.Task == nil || trustFirstResult.Task.TaskID != "task-free" {
		t.Fatalf("trust-first should keep profile gate advisory and select work, got %+v", trustFirstResult)
	}
	if trustFirstResult.ProfileGateBlockedWork || !trustFirstResult.AutonomousExecutionAllowed {
		t.Fatalf("trust-first should expose autonomous execution as allowed, got %+v", trustFirstResult)
	}
}

func TestAgentProfileUpdateWithEventRejectsForgedPromptPrincipal(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-profile-forged-principal"
		agentID     = "agent-profile-forged"
		actorID     = "operator-profile"
	)
	seedAgentProfilePromptContextWorkspace(t, ctx, store, workspaceID, agentID, actorID)

	_, _, err := store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		ActorType:      "human",
		Specialization: "builder",
		PromptContextEnvelope: sqlite.BuildAgentProfilePromptContextEnvelope(
			"agent.profile.update",
			"server_rpc",
			workspaceID,
			"human",
			"other-operator",
		),
		PromptContextSurface: "agent.profile.update",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged principal rejection, got %v", err)
	}
	profile, getErr := store.GetAgentProfile(ctx, workspaceID, agentID)
	if getErr != nil {
		t.Fatalf("get profile after reject: %v", getErr)
	}
	if profile.Specialization != "" {
		t.Fatalf("forged profile update mutated storage: %+v", profile)
	}
	if got := countAgentProfileRuntimeEvents(t, ctx, store, workspaceID, agentID); got != 0 {
		t.Fatalf("forged profile update recorded runtime events: %d", got)
	}
}

func TestAgentProfileUpdateWithEventRequiresPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-profile-missing-context"
		agentID     = "agent-profile-missing-context"
		actorID     = "operator-profile"
	)
	seedAgentProfilePromptContextWorkspace(t, ctx, store, workspaceID, agentID, actorID)

	_, _, err := store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		ActorType:      "human",
		Specialization: "builder",
	})
	if err == nil || !strings.Contains(err.Error(), "prompt_context_envelope") {
		t.Fatalf("expected missing prompt context rejection, got %v", err)
	}
	if got := countAgentProfileRuntimeEvents(t, ctx, store, workspaceID, agentID); got != 0 {
		t.Fatalf("missing context profile update recorded runtime events: %d", got)
	}
}

func TestAgentProfileUpdateWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-profile-stale-authority"
		agentID     = "agent-profile-stale-authority"
		actorID     = "operator-profile"
	)
	seedAgentProfilePromptContextWorkspace(t, ctx, store, workspaceID, agentID, actorID)
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-4701")

	_, _, err := store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		ActorType:      "human",
		Specialization: "worker",
		PromptContextEnvelope: sqlite.BuildAgentProfilePromptContextEnvelope(
			"agent.profile.update",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "agent.profile.update",
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected stale authority reject, got %v", err)
	}
	profile, getErr := store.GetAgentProfile(ctx, workspaceID, agentID)
	if getErr != nil {
		t.Fatalf("get profile after stale authority reject: %v", getErr)
	}
	if profile.Specialization != "" {
		t.Fatalf("stale authority profile update mutated storage: %+v", profile)
	}
	if got := countAgentProfileRuntimeEvents(t, ctx, store, workspaceID, agentID); got != 0 {
		t.Fatalf("stale authority profile update recorded runtime events: %d", got)
	}
}

func TestAgentProfileUpdateWithEventRejectsAgentPrincipalTargetMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-profile-agent-target-mismatch"
		actorID     = "agent-profile-actor"
		targetID    = "agent-profile-target"
	)
	seedAgentProfilePromptContextWorkspace(t, ctx, store, workspaceID, actorID, "developer")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     targetID,
		OwnerUserID: "developer",
		DisplayName: targetID,
	}); err != nil {
		t.Fatalf("register target agent: %v", err)
	}

	for _, actorType := range []string{"agent", "Agent", "AGENT"} {
		_, _, err := store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
			WorkspaceID:    workspaceID,
			AgentID:        targetID,
			ActorID:        actorID,
			ActorType:      actorType,
			Specialization: "worker",
			PromptContextEnvelope: sqlite.BuildAgentProfilePromptContextEnvelope(
				"agent.profile.update",
				"server_rpc",
				workspaceID,
				actorType,
				actorID,
			),
			PromptContextSurface: "agent.profile.update",
		})
		if err == nil || !strings.Contains(err.Error(), "actor mismatch") {
			t.Fatalf("expected agent principal target mismatch rejection for actor_type %q, got %v", actorType, err)
		}
	}
	if got := countAgentProfileRuntimeEvents(t, ctx, store, workspaceID, targetID); got != 0 {
		t.Fatalf("agent principal target mismatch recorded runtime events: %d", got)
	}
}

func seedAgentProfilePromptContextWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, actorID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: actorID,
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent %s: %v", agentID, err)
	}
}

func decodeAgentProfilePromptPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode agent profile prompt payload: %v; payload=%q", err, payloadJSON)
	}
	return payload
}

func assertAgentProfilePromptContext(t *testing.T, payload map[string]any, workspaceID, principalType, principalID, agentID, actorID string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent profile prompt_context_envelope in payload, got %+v", payload)
	}
	for key, want := range map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_agent_profile_write",
		"surface":                            "agent.profile.update",
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"agent_id":                           agentID,
		"actor_id":                           actorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	} {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
}

func countAgentProfileRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) int {
	t.Helper()
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.profile.updated",
		EntityType:  "agent_profile",
		EntityID:    agentID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list agent profile runtime events: %v", err)
	}
	return len(events)
}
