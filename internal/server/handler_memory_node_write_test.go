package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryNodeWriteRPCSurface(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-handler-memory-node-write"
		agentID     = "agent-handler-memory-node"
		taskID      = "task-handler-memory-node"
		sessionID   = "sess-handler-memory-node"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Node Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Node Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:      taskID,
		Title:       "Memory Node Task",
		Description: "Support memory node write tests.",
		OwnerUserID: "developer",
	})
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceMemoryNodeWriteParams{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:handler-node-memory",
		MemoryType:  "decision",
		Title:       "Use canonical node write",
		Body:        "Handlers should record memory nodes through workspace memory.",
		Summary:     "Node write RPC.",
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.77,
		Confidence:  0.88,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryNodeWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeWrite rpc error: %+v", rpcErr)
	}

	writeResult, ok := result.(sqlite.MemoryNodeWriteResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if writeResult.MemoryID != "handler-node-memory" || writeResult.NodeID == "" {
		t.Fatalf("unexpected write identity result: %+v", writeResult)
	}
	if writeResult.Event.EventID == "" {
		t.Fatalf("expected node write result to carry exact runtime event, got %+v", writeResult.Event)
	}
	if writeResult.Node.OriginKind != "workspace_memory" || writeResult.Node.TaskID != taskID {
		t.Fatalf("unexpected derived node projection: %+v", writeResult.Node)
	}

	memoryFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    writeResult.MemoryID,
		Limit:       10,
	}
	firstLive := nextEventOfType(t, ch, "workspace.memory.recorded")
	firstPersisted := mustRuntimeEvent(t, ctx, store, memoryFilter)
	assertLiveEventMirrorsRuntimeEvent(t, firstLive, firstPersisted, "workspace.memory.recorded")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, firstLive.PayloadJSON), firstPersisted.PayloadJSON)
	assertServerWorkspaceMemoryRuntimePromptContext(t, firstPersisted, "workspace.memory.node.write", workspaceID, writeResult.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer",
		"agent_id":    agentID,
		"session_id":  sessionID,
		"task_id":     taskID,
		"actor_type":  "agent",
		"actor_id":    agentID,
	})

	seenMemoryEvents := snapshotRuntimeEventIDs(t, ctx, store, memoryFilter)
	secondRaw, err := json.Marshal(workspaceMemoryNodeWriteParams{
		WorkspaceID: workspaceID,
		MemoryID:    writeResult.MemoryID,
		MemoryType:  "decision",
		Title:       "Use canonical node write",
		Body:        "Repeated memory node writes should mirror the exact runtime row.",
		Summary:     "Node write RPC repeated.",
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		SourceKind:  "manual",
		SourceID:    "developer-repeat",
		Importance:  0.81,
		Confidence:  0.91,
	})
	if err != nil {
		t.Fatalf("marshal second params: %v", err)
	}
	secondAny, rpcErr := h.workspaceMemoryNodeWrite(ctx, secondRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeWrite second rpc error: %+v", rpcErr)
	}
	secondResult, ok := secondAny.(sqlite.MemoryNodeWriteResult)
	if !ok {
		t.Fatalf("unexpected second result type %T", secondAny)
	}
	if secondResult.Event.EventID == "" {
		t.Fatalf("expected repeated node write result to carry exact runtime event, got %+v", secondResult.Event)
	}
	if secondResult.MemoryID != writeResult.MemoryID || secondResult.Record.SourceID != "developer-repeat" || secondResult.Record.Summary != "Node write RPC repeated." {
		t.Fatalf("unexpected repeated node write result %+v", secondResult)
	}

	secondLive := nextEventOfType(t, ch, "workspace.memory.recorded")
	secondPersisted := mustNewRuntimeEvent(t, ctx, store, memoryFilter, seenMemoryEvents)
	assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondPersisted, "workspace.memory.recorded")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondLive.PayloadJSON), secondPersisted.PayloadJSON)
	assertServerWorkspaceMemoryRuntimePromptContext(t, secondPersisted, "workspace.memory.node.write", workspaceID, writeResult.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer-repeat",
		"agent_id":    agentID,
		"session_id":  sessionID,
		"task_id":     taskID,
		"actor_type":  "agent",
		"actor_id":    agentID,
	})
	if secondPersisted.EventID == firstPersisted.EventID || secondPersisted.IngestSeq <= firstPersisted.IngestSeq {
		t.Fatalf("expected repeated node write to mirror the newly appended runtime row, got first=%+v second=%+v", firstPersisted, secondPersisted)
	}
}

func TestWorkspaceMemoryNodeWriteSupportsAntiProcedurePromotedClaimEffects(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-handler-memory-node-anti-procedure"
		agentID     = "agent-handler-memory-node-anti-procedure"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Node Anti Procedure",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Node Anti Procedure Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	seenRecordedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		Limit:       10,
	})
	seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		Limit:       10,
	})

	raw, err := json.Marshal(workspaceMemoryNodeWriteParams{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:handler-node-anti-procedure",
		MemoryType:  "anti_procedure",
		Title:       "Never bypass rollback gates",
		Body:        "Memory-node RPC writes should preserve anti-procedure type and promoted claim effects.",
		Summary:     "Anti-procedure node write parity.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "anti_procedure", "claim"},
	})
	if err != nil {
		t.Fatalf("marshal anti procedure node write params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryNodeWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeWrite anti procedure rpc error: %+v", rpcErr)
	}
	writeResult, ok := result.(sqlite.MemoryNodeWriteResult)
	if !ok {
		t.Fatalf("unexpected anti procedure node write result type %T", result)
	}
	if writeResult.Record.MemoryType != "ANTI_PROCEDURE" || writeResult.Node.MemoryType != "ANTI_PROCEDURE" {
		t.Fatalf("expected anti procedure node write to preserve canonical type, got %+v", writeResult)
	}
	if writeResult.PromotedClaimEffects == nil || writeResult.PromotedClaimEffects.Claim == nil {
		t.Fatalf("expected anti procedure node write to surface promoted claim effects, got %+v", writeResult)
	}
	if writeResult.PromotedClaimEffects.Claim.ClaimType != "ANTI_PROCEDURE" || writeResult.PromotedClaimEffects.Claim.MemoryID != writeResult.MemoryID {
		t.Fatalf("expected anti procedure promoted claim effect, got %+v", writeResult.PromotedClaimEffects.Claim)
	}

	claimID := "claim:memory:" + writeResult.MemoryID
	recordedPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    writeResult.MemoryID,
		Limit:       10,
	}, seenRecordedEvents)
	claimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenClaimEvents)
	assertServerWorkspaceMemoryRuntimePromptContext(t, recordedPersisted, "workspace.memory.node.write", workspaceID, writeResult.MemoryID, "human", "developer", map[string]string{
		"memory_type": "ANTI_PROCEDURE",
		"source_kind": "manual",
		"source_id":   "developer",
		"agent_id":    agentID,
		"actor_type":  "agent",
		"actor_id":    agentID,
	})
	assertRuntimeEventPayloadHasNoPromptContextEnvelope(t, claimPersisted)

	ordered, _ := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: recordedPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: claimPersisted, Type: "workspace.claim.written"},
	)
	if len(ordered) != 2 ||
		ordered[0].Type != "workspace.memory.recorded" ||
		ordered[1].Type != "workspace.claim.written" ||
		!runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) {
		t.Fatalf("expected anti-procedure node write live mirrors to follow persisted chronology, got %+v", ordered)
	}
}

func TestWorkspaceMemoryNodeWriteSupportsPolicyTraceType(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	const workspaceID = "ws-handler-memory-node-policy-trace"
	ctx := testAuthContext(workspaceID, "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Node Policy Trace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryNodeWriteParams{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:handler-node-policy-trace",
		MemoryType:  "policy_trace",
		Title:       "Policy trace",
		Body:        "Direct node-write RPC should preserve policy-trace identity memories on the existing boundary.",
		Summary:     "Policy-trace node write parity.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "policy_trace"},
	})
	if err != nil {
		t.Fatalf("marshal policy trace node write params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryNodeWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeWrite policy trace rpc error: %+v", rpcErr)
	}
	writeResult, ok := result.(sqlite.MemoryNodeWriteResult)
	if !ok {
		t.Fatalf("unexpected policy trace node write result type %T", result)
	}
	if writeResult.Record.MemoryType != "POLICY_TRACE" || writeResult.Node.MemoryType != "POLICY_TRACE" || writeResult.Node.MemoryLayer != "IDENTITY" {
		t.Fatalf("expected policy-trace node write to preserve identity memory type/layer, got %+v", writeResult)
	}
	if writeResult.PromotedClaimEffects != nil {
		t.Fatalf("did not expect policy-trace node write to surface promoted claim effects, got %+v", writeResult.PromotedClaimEffects)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    writeResult.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list policy-trace claims after node write rpc: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("did not expect policy-trace node write rpc to materialize claims yet, got %+v", claims)
	}
}

func TestWorkspaceMemoryNodeWriteSupportsGoalCommitmentType(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	const workspaceID = "ws-handler-memory-node-goal-commitment"
	ctx := testAuthContext(workspaceID, "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Node Goal Commitment",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryNodeWriteParams{
		WorkspaceID: workspaceID,
		NodeID:      "memnode:workspace_memory:handler-node-goal-commitment",
		MemoryType:  "goal_commitment",
		Title:       "Goal commitment",
		Body:        "Direct node-write RPC should preserve goal-commitment identity memories on the existing boundary.",
		Summary:     "Goal-commitment node write parity.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "goal_commitment"},
	})
	if err != nil {
		t.Fatalf("marshal goal commitment node write params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryNodeWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeWrite goal commitment rpc error: %+v", rpcErr)
	}
	writeResult, ok := result.(sqlite.MemoryNodeWriteResult)
	if !ok {
		t.Fatalf("unexpected goal commitment node write result type %T", result)
	}
	if writeResult.Record.MemoryType != "GOAL_COMMITMENT" || writeResult.Node.MemoryType != "GOAL_COMMITMENT" || writeResult.Node.MemoryLayer != "IDENTITY" {
		t.Fatalf("expected goal-commitment node write to preserve identity memory type/layer, got %+v", writeResult)
	}
	if writeResult.PromotedClaimEffects != nil {
		t.Fatalf("did not expect goal-commitment node write to surface promoted claim effects, got %+v", writeResult.PromotedClaimEffects)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    writeResult.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list goal-commitment claims after node write rpc: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("did not expect goal-commitment node write rpc to materialize claims yet, got %+v", claims)
	}
}

func TestWorkspaceMemoryNodeWriteRejectsAuthorityFields(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-memory-node-authority", "human", "developer")

	raw := []byte(`{
		"workspace_id":"ws-memory-node-authority",
		"body":"Attempted direct graph authority.",
		"memory_layer":"SEMANTIC",
		"origin_kind":"knowledge_claim",
		"refs":[{"ref_kind":"task","ref_id":"task-1"}]
	}`)
	if _, rpcErr := h.workspaceMemoryNodeWrite(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "direct graph authority fields") {
		t.Fatalf("expected authority-field rejection, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryNodeWriteRejectsInvalidAliases(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-memory-node-invalid-alias", "human", "developer")

	raw, err := json.Marshal(workspaceMemoryNodeWriteParams{
		WorkspaceID: "ws-memory-node-invalid-alias",
		NodeID:      "memnode:knowledge_claim:claim-1",
		Body:        "Invalid node id.",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryNodeWrite(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for foreign origin id, got %+v", rpcErr)
	}
}
