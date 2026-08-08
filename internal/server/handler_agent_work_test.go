package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentTaskHydrateHandlerReturnsBundle(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-hydrate", "agent", "agent-hydrate")

	seedHandlerAgentWorkWorkspace(t, ctx, store, "ws-handler-hydrate", []string{"agent-hydrate"})
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-handler-hydrate",
		DocKey:      "current_context",
		Title:       "Current Context",
		Content:     "Hydrated handlers should use the existing bundle store.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	createHandlerAgentWorkTask(t, ctx, store, "ws-handler-hydrate", "task-hydrate", "normal")

	includeAllDocs := false
	raw, err := json.Marshal(agentTaskHydrateParams{
		WorkspaceID:    "ws-handler-hydrate",
		TaskID:         "task-hydrate",
		DocKeys:        []string{"current_context"},
		IncludeAllDocs: &includeAllDocs,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentTaskHydrate(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentTaskHydrate rpc error: %+v", rpcErr)
	}

	payload := result.(map[string]any)
	bundle, ok := payload["bundle"].(sqlite.TaskHydrationBundle)
	if !ok {
		t.Fatalf("unexpected bundle type %T", payload["bundle"])
	}
	if bundle.Workspace == nil || bundle.Workspace.WorkspaceID != "ws-handler-hydrate" {
		t.Fatalf("expected workspace in bundle, got %+v", bundle.Workspace)
	}
	if len(bundle.Docs) != 1 || bundle.Docs[0].DocKey != "current_context" {
		t.Fatalf("expected selected doc in bundle, got %+v", bundle.Docs)
	}
}

func TestAgentTaskHydrateHandlerMapsMissingTaskToInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-next", "agent", "agent-a")

	seedHandlerAgentWorkWorkspace(t, ctx, store, "ws-handler-next", []string{"agent-a"})

	raw, err := json.Marshal(agentTaskHydrateParams{
		WorkspaceID: "ws-handler-next",
		TaskID:      "missing-task",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	if _, rpcErr := h.agentTaskHydrate(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for missing task, got %+v", rpcErr)
	}
}

func TestAgentTaskHydrateRejectsMissingWorkspaceID(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-hydrate-required", "agent", "agent-a")

	raw, err := json.Marshal(agentTaskHydrateParams{TaskID: "task-any"})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentTaskHydrate(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected missing workspace_id to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result without workspace_id, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams || rpcErr.Message != "workspace_id is required" {
		t.Fatalf("unexpected missing workspace error %+v", rpcErr)
	}
}

func TestAgentWorkNextHandlerReturnsHydratedResumeSession(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-next", "agent", "agent-a")

	seedHandlerAgentWorkWorkspace(t, ctx, store, "ws-handler-next", []string{"agent-a", "agent-b"})
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-handler-next",
		DocKey:      "current_context",
		Title:       "Current Context",
		Content:     "Work.next should be able to inline hydration.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	createHandlerAgentWorkTask(t, ctx, store, "ws-handler-next", "task-session", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-handler-next",
		TaskID:      "task-session",
		AgentID:     "agent-a",
		Summary:     "resume me",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-a",
		AgentID:     "agent-a",
		WorkspaceID: "ws-handler-next",
		TaskID:      "task-session",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: "ws-handler-next",
		SessionID:   "sess-a",
		AgentID:     "agent-a",
		TaskID:      "task-session",
		Summary:     "Resume task-session",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record session coordination: %v", err)
	}

	includeHydration := true
	raw, err := json.Marshal(agentWorkNextParams{
		WorkspaceID:      "ws-handler-next",
		AgentID:          "agent-a",
		IncludeHydration: &includeHydration,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentWorkNext(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentWorkNext rpc error: %+v", rpcErr)
	}

	payload, ok := result.(sqlite.AgentWorkNextResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if !payload.HasWork || payload.Reason != "resume_session" {
		t.Fatalf("expected resume_session work result, got %+v", payload)
	}
	if payload.Task == nil || payload.Task.TaskID != "task-session" {
		t.Fatalf("expected task-session, got %+v", payload.Task)
	}
	if payload.Session == nil || payload.Session.SessionID != "sess-a" {
		t.Fatalf("expected sess-a, got %+v", payload.Session)
	}
	if payload.TimeAuthority.WorkspaceID != "ws-handler-next" || payload.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected workspace time authority on handler work.next result, got %+v", payload.TimeAuthority)
	}
	if payload.GeneratedAt != payload.TimeAuthority.ReferenceAt {
		t.Fatalf("expected handler work.next generated_at %q to mirror time authority reference_at %q", payload.GeneratedAt, payload.TimeAuthority.ReferenceAt)
	}
	if payload.Hydration == nil || len(payload.Hydration.Docs) != 1 {
		t.Fatalf("expected inline hydration docs, got %+v", payload.Hydration)
	}
}

func TestAgentWorkNextHandlerSurfacesProfileGateClosed(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-profile-gate", "agent", "observer")

	seedHandlerAgentWorkWorkspace(t, ctx, store, "ws-handler-profile-gate", []string{"observer"})
	createHandlerAgentWorkTask(t, ctx, store, "ws-handler-profile-gate", "task-free", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    "ws-handler-profile-gate",
		AgentID:        "observer",
		Bio:            "Analyze global system dynamics without direct participation.",
		Specialization: "meta-analysis",
		Tags:           []string{"observer"},
		Metadata: map[string]any{
			"default_work_mode": "observer",
		},
	}); err != nil {
		t.Fatalf("upsert observer profile: %v", err)
	}

	includePacket := true
	raw, err := json.Marshal(agentWorkNextParams{
		WorkspaceID:   "ws-handler-profile-gate",
		AgentID:       "observer",
		IncludePacket: &includePacket,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentWorkNext(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentWorkNext rpc error: %+v", rpcErr)
	}
	payload, ok := result.(sqlite.AgentWorkNextResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if payload.HasWork || payload.Reason != "profile_gate_closed" || payload.AutonomousExecutionAllowed {
		t.Fatalf("expected closed profile gate result, got %+v", payload)
	}
	if !payload.ProfileGateBlockedWork || payload.ProfileGateReason == "" || payload.ProfileGateSummary == "" {
		t.Fatalf("expected profile gate evidence, got %+v", payload)
	}
	if payload.Packet == nil || payload.Packet.Gate == nil || payload.Packet.Gate.GateType != "profile_autonomous_execution" {
		t.Fatalf("expected profile gate packet, got %+v", payload.Packet)
	}
}

func TestAgentWorkNextHandlerReturnsWakeResumePacket(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-trigger", "agent", "agent-a")

	seedHandlerAgentWorkWorkspace(t, ctx, store, "ws-handler-trigger", []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, "ws-handler-trigger", "task-waiting", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-handler-trigger",
		TaskID:      "task-waiting",
		AgentID:     "agent-a",
		Summary:     "waiting on inbound context",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-waiting",
		AgentID:     "agent-a",
		WorkspaceID: "ws-handler-trigger",
		TaskID:      "task-waiting",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.status",
		WorkspaceID: "ws-handler-trigger",
		SessionID:   "sess-waiting",
		AgentID:     "agent-a",
		TaskID:      "task-waiting",
		Summary:     "need human decision",
		Status:      "WAITING_DECISION",
	}); err != nil {
		t.Fatalf("record waiting session: %v", err)
	}

	raw, err := json.Marshal(agentWorkNextParams{
		WorkspaceID:     "ws-handler-trigger",
		AgentID:         "agent-a",
		Trigger:         "inbound_message",
		CandidateTaskID: "task-waiting",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentWorkNext(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentWorkNext rpc error: %+v", rpcErr)
	}

	payload, ok := result.(sqlite.AgentWorkNextResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if !payload.HasWork || payload.Trigger != "inbound_message" {
		t.Fatalf("expected triggered work packet, got %+v", payload)
	}
	if payload.ClaimAction != "reuse_claim" || payload.SessionAction != "resume_inactive" {
		t.Fatalf("expected explicit resume packet actions, got %+v", payload)
	}
	if payload.Session == nil || payload.Session.SessionID != "sess-waiting" || payload.Session.Status != "WAITING_DECISION" {
		t.Fatalf("expected waiting session in payload, got %+v", payload.Session)
	}
}

func TestAgentWorkNextHandlerReturnsTypedPacketAndAdvisory(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-packet", "agent", "agent-a")

	seedHandlerAgentWorkWorkspace(t, ctx, store, "ws-handler-packet", []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, "ws-handler-packet", "task-proof", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-handler-packet",
		TaskID:      "task-proof",
		AgentID:     "agent-a",
		Summary:     "waiting on approval",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-proof",
		AgentID:     "agent-a",
		WorkspaceID: "ws-handler-packet",
		TaskID:      "task-proof",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:          "session.decision_needed",
		WorkspaceID:        "ws-handler-packet",
		SessionID:          "sess-proof",
		AgentID:            "agent-a",
		TaskID:             "task-proof",
		Summary:            "need formal approval",
		Status:             "WAITING_DECISION",
		DecisionNeededFrom: "human",
		DecisionType:       "approval",
		RelatedDocKeys:     []string{"task.task-proof"},
	}); err != nil {
		t.Fatalf("record waiting session: %v", err)
	}

	includePacket := true
	includeAdvisory := true
	raw, err := json.Marshal(agentWorkNextParams{
		WorkspaceID:     "ws-handler-packet",
		AgentID:         "agent-a",
		IncludePacket:   &includePacket,
		IncludeAdvisory: &includeAdvisory,
		FrontierLimit:   2,
		Trigger:         "inbound_message",
		CandidateTaskID: "task-proof",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentWorkNext(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentWorkNext rpc error: %+v", rpcErr)
	}

	payload, ok := result.(sqlite.AgentWorkNextResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if payload.Packet == nil {
		t.Fatalf("expected typed packet, got %+v", payload)
	}
	if payload.Packet.WorkType != "resume_session" || payload.Packet.CoordinationState != "waiting_decision" {
		t.Fatalf("unexpected packet core: %+v", payload.Packet)
	}
	if payload.Packet.PreferredTransition != "await_decision" || payload.Packet.WhyNow != "inbound_message" {
		t.Fatalf("unexpected packet transition hints: %+v", payload.Packet)
	}
	if payload.Packet.Decision == nil || payload.Packet.Decision.DecisionType != "approval" {
		t.Fatalf("expected decision hint, got %+v", payload.Packet)
	}
	if payload.Packet.Gate == nil || payload.Packet.Gate.GateState != "open" || payload.Packet.Gate.GateType != "approval" {
		t.Fatalf("expected gate hint, got %+v", payload.Packet)
	}
	if payload.Packet.Advisory == nil || payload.Packet.Advisory.Control == nil || payload.Packet.Advisory.Corridor == nil {
		t.Fatalf("expected scoped advisory packet, got %+v", payload.Packet)
	}
}

func TestAgentWorkNextRejectsMismatchedAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-handler-next-mismatch", "agent", "agent-b")

	const workspaceID = "ws-handler-next-mismatch"
	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, "task-next-mismatch", "high")

	raw, err := json.Marshal(agentWorkNextParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentWorkNext(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected agent principal mismatch to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on agent mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied, got %+v", rpcErr)
	}
	if rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected agent mismatch error %+v", rpcErr)
	}
}

func TestAgentTaskHydrateRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-handler-hydrate-target"
	ctx := testAuthContext(workspaceID, "agent", "agent-hydrate")

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-hydrate"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, "task-hydrate-mismatch", "normal")

	raw, err := json.Marshal(agentTaskHydrateParams{
		WorkspaceID: workspaceID,
		TaskID:      "task-hydrate-mismatch",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	otherCtx := testAuthContext("ws-handler-hydrate-other", "agent", "agent-hydrate")
	result, rpcErr := h.agentTaskHydrate(otherCtx, raw)
	if rpcErr == nil {
		t.Fatal("expected workspace mismatch to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on workspace mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected workspace mismatch error %+v", rpcErr)
	}
}

func TestAgentTaskHydrateRejectsEmptyWorkspaceCrossWorkspaceLookup(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctxA := testAuthContext("ws-handler-hydrate-a", "agent", "agent-a")
	ctxB := testAuthContext("ws-handler-hydrate-b", "agent", "agent-b")

	seedHandlerAgentWorkWorkspace(t, ctxA, store, "ws-handler-hydrate-a", []string{"agent-a"})
	seedHandlerAgentWorkWorkspace(t, ctxB, store, "ws-handler-hydrate-b", []string{"agent-b"})
	createHandlerAgentWorkTask(t, ctxB, store, "ws-handler-hydrate-b", "task-secret", "high")

	raw, err := json.Marshal(agentTaskHydrateParams{
		TaskID: "task-secret",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.agentTaskHydrate(ctxA, raw)
	if rpcErr == nil {
		t.Fatal("expected empty workspace hydrate lookup to fail before store lookup")
	}
	if result != nil {
		t.Fatalf("expected no cross-workspace hydrate result, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams || rpcErr.Message != "workspace_id is required" {
		t.Fatalf("unexpected empty workspace error %+v", rpcErr)
	}
}

func seedHandlerAgentWorkWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs []string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}

func createHandlerAgentWorkTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, priority string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     priority,
		Title:        taskID,
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
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
