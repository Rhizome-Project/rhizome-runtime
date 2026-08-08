package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentNodeLifecycleAppendsRuntimeEventsAndPublishesSSE(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-node-runtime-events"
		taskID      = "task-node-runtime-events"
		agentID     = "agent-node-runtime-events"
		nodeID      = "node-1"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createNodeLifecycleFixture(t, ctx, store, workspaceID, taskID, agentID, nodeID)
	authority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawClaim, err := json.Marshal(agentNodeClaimParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		NodeID:      nodeID,
		Summary:     "claiming node",
	})
	if err != nil {
		t.Fatalf("marshal claim params: %v", err)
	}
	if _, rpcErr := h.agentNodeClaim(ctx, rawClaim); rpcErr != nil {
		t.Fatalf("agentNodeClaim rpc error: %+v", rpcErr)
	}

	claimEvent := nextEvent(t, ch)
	claimRuntimeEvent := assertNodeRuntimeEvent(t, ctx, store, workspaceID, taskID, nodeID, agentID, "node.claimed", map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   "CLAIMED",
		"summary":  "claiming node",
	})
	assertServerRuntimeEventAuthorityMetadata(t, claimRuntimeEvent, authority)
	assertLiveEventMirrorsRuntimeEvent(t, claimEvent, claimRuntimeEvent, "node.claimed")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, claimEvent.PayloadJSON), claimRuntimeEvent.PayloadJSON)
	assertAgentNodeRuntimePromptContext(t, claimRuntimeEvent, "agent.node.claim", workspaceID, taskID, nodeID, agentID)
	assertValidEventPayload(t, claimEvent.PayloadJSON, map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   "CLAIMED",
		"summary":  "claiming node",
	})

	rawRelease, err := json.Marshal(agentNodeReleaseParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		NodeID:      nodeID,
		Reason:      "need to retry",
	})
	if err != nil {
		t.Fatalf("marshal release params: %v", err)
	}
	if _, rpcErr := h.agentNodeRelease(ctx, rawRelease); rpcErr != nil {
		t.Fatalf("agentNodeRelease rpc error: %+v", rpcErr)
	}

	releaseEvent := nextEvent(t, ch)
	releaseRuntimeEvent := assertNodeRuntimeEvent(t, ctx, store, workspaceID, taskID, nodeID, agentID, "node.released", map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   "RELEASED",
		"reason":   "need to retry",
	})
	assertServerRuntimeEventAuthorityMetadata(t, releaseRuntimeEvent, authority)
	assertLiveEventMirrorsRuntimeEvent(t, releaseEvent, releaseRuntimeEvent, "node.released")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, releaseEvent.PayloadJSON), releaseRuntimeEvent.PayloadJSON)
	assertAgentNodeRuntimePromptContext(t, releaseRuntimeEvent, "agent.node.release", workspaceID, taskID, nodeID, agentID)
	assertValidEventPayload(t, releaseEvent.PayloadJSON, map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   "RELEASED",
		"reason":   "need to retry",
	})

	if _, rpcErr := h.agentNodeClaim(ctx, rawClaim); rpcErr != nil {
		t.Fatalf("agentNodeClaim second rpc error: %+v", rpcErr)
	}
	secondClaimEvent := nextEvent(t, ch)
	secondClaimRuntimeEvent := assertNodeRuntimeEvent(t, ctx, store, workspaceID, taskID, nodeID, agentID, "node.claimed", map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   "CLAIMED",
		"summary":  "claiming node",
	})
	assertServerRuntimeEventAuthorityMetadata(t, secondClaimRuntimeEvent, authority)
	assertLiveEventMirrorsRuntimeEvent(t, secondClaimEvent, secondClaimRuntimeEvent, "node.claimed")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondClaimEvent.PayloadJSON), secondClaimRuntimeEvent.PayloadJSON)
	assertAgentNodeRuntimePromptContext(t, secondClaimRuntimeEvent, "agent.node.claim", workspaceID, taskID, nodeID, agentID)

	rawComplete, err := json.Marshal(agentNodeCompleteParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		NodeID:      nodeID,
		Summary:     "completed node",
	})
	if err != nil {
		t.Fatalf("marshal complete params: %v", err)
	}
	if _, rpcErr := h.agentNodeComplete(ctx, rawComplete); rpcErr != nil {
		t.Fatalf("agentNodeComplete rpc error: %+v", rpcErr)
	}

	completeEvent := nextEvent(t, ch)
	completeRuntimeEvent := assertNodeRuntimeEvent(t, ctx, store, workspaceID, taskID, nodeID, agentID, "node.completed", map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   "COMPLETED",
		"summary":  "completed node",
	})
	assertServerRuntimeEventAuthorityMetadata(t, completeRuntimeEvent, authority)
	assertLiveEventMirrorsRuntimeEvent(t, completeEvent, completeRuntimeEvent, "node.completed")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, completeEvent.PayloadJSON), completeRuntimeEvent.PayloadJSON)
	assertAgentNodeRuntimePromptContext(t, completeRuntimeEvent, "agent.node.complete", workspaceID, taskID, nodeID, agentID)
	assertValidEventPayload(t, completeEvent.PayloadJSON, map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   "COMPLETED",
		"summary":  "completed node",
	})
}

func TestAgentNodeReleaseNoOpDoesNotAppendRuntimeEventOrPublishSSE(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-node-release-noop"
		taskID      = "task-node-release-noop"
		agentID     = "agent-node-release-noop"
		nodeID      = "node-1"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createNodeLifecycleFixture(t, ctx, store, workspaceID, taskID, agentID, nodeID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawRelease, err := json.Marshal(agentNodeReleaseParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		NodeID:      nodeID,
		Reason:      "nothing claimed yet",
	})
	if err != nil {
		t.Fatalf("marshal release params: %v", err)
	}
	if _, rpcErr := h.agentNodeRelease(ctx, rawRelease); rpcErr != nil {
		t.Fatalf("agentNodeRelease rpc error: %+v", rpcErr)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "node.released",
		EntityType:  "dag_node",
		EntityID:    nodeID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list node.released runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no node.released runtime events for noop release, got %+v", events)
	}
	assertNoEventWithin(t, ch, 150*time.Millisecond)
}

func createNodeLifecycleFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, nodeID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Node Runtime Events",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Node Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task with graph: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
}

func assertNodeRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID, agentID, eventType string, wantPayload map[string]string) sqlite.RuntimeEventRecord {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "dag_node",
		EntityID:    nodeID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events for %s: %v", eventType, err)
	}
	if len(events) == 0 {
		t.Fatalf("expected runtime event %s for node %s", eventType, nodeID)
	}
	event := events[0]
	if event.AgentID != agentID || event.ActorID != agentID {
		t.Fatalf("expected runtime event agent/actor ids to be %s, got %+v", agentID, event)
	}
	assertValidEventPayload(t, event.PayloadJSON, wantPayload)
	return event
}

func assertAgentNodeRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantTaskID, wantNodeID, wantAgentID string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected node prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	required := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_node_lifecycle_write",
		"surface":                            wantSurface,
		"origin":                             "server_rpc",
		"workspace_id":                       wantWorkspaceID,
		"principal_type":                     "agent",
		"principal_id":                       wantAgentID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
		"actor_agent_id":                     wantAgentID,
		"agent_id":                           wantAgentID,
		"task_id":                            wantTaskID,
		"node_id":                            wantNodeID,
		"status":                             agentNodeLifecycleStatusForSurface(wantSurface),
		"node_claim_status":                  agentNodeLifecycleStatusForSurface(wantSurface),
		"node_status_after":                  agentNodeLifecycleStatusAfterForSurface(wantSurface),
	}
	for key, want := range required {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("node prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("node prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
	if payload["status"] != required["status"] {
		t.Fatalf("node runtime payload status = %v, want %s in %+v", payload["status"], required["status"], payload)
	}
	if payload["node_claim_status"] != required["node_claim_status"] || payload["node_status_after"] != required["node_status_after"] {
		t.Fatalf("node runtime payload claim/status-after mismatch, want %+v got %+v", required, payload)
	}
}

func agentNodeLifecycleStatusForSurface(surface string) string {
	switch surface {
	case "agent.node.claim":
		return "CLAIMED"
	case "agent.node.release":
		return "RELEASED"
	case "agent.node.complete":
		return "COMPLETED"
	default:
		return ""
	}
}

func agentNodeLifecycleStatusAfterForSurface(surface string) string {
	switch surface {
	case "agent.node.claim":
		return "RUNNING"
	case "agent.node.release":
		return "PENDING"
	case "agent.node.complete":
		return "RESOLVED"
	default:
		return ""
	}
}
