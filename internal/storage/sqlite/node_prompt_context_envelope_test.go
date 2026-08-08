package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentNodePromptContextEnvelopeCarriesLifecycleEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-node-prompt-context"
		taskID      = "task-agent-node-prompt-context"
		agentID     = "agent-node-prompt-context"
		nodeID      = "node-1"
	)

	seedD2ANodeWorkspace(t, ctx, store, workspaceID, taskID, agentID, nodeID)
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claimEvent, err := store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		NodeID:                nodeID,
		AgentID:               agentID,
		Summary:               "claim node with context",
		PromptContextEnvelope: boundAgentNodePromptContextEnvelope("agent.node.claim", workspaceID, taskID, nodeID, agentID),
	})
	if err != nil {
		t.Fatalf("claim node with prompt context: %v", err)
	}
	assertAgentNodeRuntimePromptContext(t, claimEvent, "agent.node.claim", workspaceID, taskID, nodeID, agentID)

	releaseEvent, err := store.ReleaseNodeClaimWithEvent(ctx, sqlite.NodeReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		NodeID:                nodeID,
		AgentID:               agentID,
		Reason:                "release node with context",
		PromptContextEnvelope: boundAgentNodePromptContextEnvelope("agent.node.release", workspaceID, taskID, nodeID, agentID),
	})
	if err != nil {
		t.Fatalf("release node with prompt context: %v", err)
	}
	assertAgentNodeRuntimePromptContext(t, releaseEvent, "agent.node.release", workspaceID, taskID, nodeID, agentID)

	_, err = store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		NodeID:                nodeID,
		AgentID:               agentID,
		Summary:               "reclaim before completion",
		PromptContextEnvelope: boundAgentNodePromptContextEnvelope("agent.node.claim", workspaceID, taskID, nodeID, agentID),
	})
	if err != nil {
		t.Fatalf("reclaim node with prompt context: %v", err)
	}

	completeEvent, err := store.CompleteNodeClaimWithEvent(ctx, sqlite.NodeCompleteInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		NodeID:                nodeID,
		AgentID:               agentID,
		Summary:               "complete node with context",
		PromptContextEnvelope: boundAgentNodePromptContextEnvelope("agent.node.complete", workspaceID, taskID, nodeID, agentID),
	})
	if err != nil {
		t.Fatalf("complete node with prompt context: %v", err)
	}
	assertAgentNodeRuntimePromptContext(t, completeEvent, "agent.node.complete", workspaceID, taskID, nodeID, agentID)
}

func TestAgentNodePromptContextEnvelopeRejectsForgedBindings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{
			name: "wrong surface",
			mutate: func(envelope map[string]any) {
				envelope["surface"] = "agent.task.claim"
			},
			want: "not valid for node_lifecycle",
		},
		{
			name: "wrong workspace",
			mutate: func(envelope map[string]any) {
				envelope["workspace_id"] = "ws-other"
			},
			want: "workspace_id",
		},
		{
			name: "wrong principal",
			mutate: func(envelope map[string]any) {
				envelope["principal_id"] = "agent-forged"
			},
			want: "principal_id",
		},
		{
			name: "wrong agent",
			mutate: func(envelope map[string]any) {
				envelope["agent_id"] = "agent-forged"
			},
			want: "agent_id",
		},
		{
			name: "wrong task",
			mutate: func(envelope map[string]any) {
				envelope["task_id"] = "task-forged"
			},
			want: "task_id",
		},
		{
			name: "wrong node",
			mutate: func(envelope map[string]any) {
				envelope["node_id"] = "node-forged"
			},
			want: "node_id",
		},
		{
			name: "wrong status",
			mutate: func(envelope map[string]any) {
				envelope["status"] = "RUNNING"
			},
			want: "status",
		},
		{
			name: "wrong node status after",
			mutate: func(envelope map[string]any) {
				envelope["node_status_after"] = model.NodeStatusResolved
			},
			want: "node_status_after",
		},
		{
			name: "nested wrong node",
			mutate: func(envelope map[string]any) {
				nested := make(map[string]any, len(envelope))
				for key, value := range envelope {
					nested[key] = value
				}
				nested["node_id"] = "node-forged"
				envelope["nested_false_context"] = nested
			},
			want: "node_id",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			suffix := strings.ReplaceAll(tc.name, " ", "-")
			workspaceID := "ws-agent-node-forged-" + suffix
			taskID := "task-agent-node-forged-" + suffix
			agentID := "agent-node-forged-" + suffix
			nodeID := "node-1"
			seedD2ANodeWorkspace(t, ctx, store, workspaceID, taskID, agentID, nodeID)
			claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

			envelope := boundAgentNodePromptContextEnvelope("agent.node.claim", workspaceID, taskID, nodeID, agentID)
			tc.mutate(envelope)

			_, err := store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
				WorkspaceID:           workspaceID,
				TaskID:                taskID,
				NodeID:                nodeID,
				AgentID:               agentID,
				Summary:               "forged context should fail",
				PromptContextEnvelope: envelope,
			})
			if err == nil {
				t.Fatal("expected forged node prompt context to fail closed")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected forged prompt context error: %v", err)
			}
			assertNodeStayedPendingWithoutClaim(t, ctx, store, workspaceID, taskID, nodeID)
			if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "node.claimed",
				EntityType:  "dag_node",
				EntityID:    nodeID,
				TaskID:      taskID,
				Limit:       10,
			}); got != 0 {
				t.Fatalf("forged prompt context appended node.claimed events: %d", got)
			}
		})
	}
}

func TestAgentNodePromptContextEnvelopeRejectsForgedReleaseAndCompleteBindings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		surface   string
		eventType string
		mutate    func(map[string]any)
		call      func(context.Context, *sqlite.Store, string, string, string, string, map[string]any) (sqlite.RuntimeEventRecord, error)
	}{
		{
			name:      "release wrong node status after",
			surface:   "agent.node.release",
			eventType: "node.released",
			mutate: func(envelope map[string]any) {
				envelope["node_status_after"] = model.NodeStatusResolved
			},
			call: func(ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID, agentID string, envelope map[string]any) (sqlite.RuntimeEventRecord, error) {
				return store.ReleaseNodeClaimWithEvent(ctx, sqlite.NodeReleaseInput{
					WorkspaceID:           workspaceID,
					TaskID:                taskID,
					NodeID:                nodeID,
					AgentID:               agentID,
					Reason:                "forged release context should fail",
					PromptContextEnvelope: envelope,
				})
			},
		},
		{
			name:      "complete wrong claim status",
			surface:   "agent.node.complete",
			eventType: "node.completed",
			mutate: func(envelope map[string]any) {
				envelope["node_claim_status"] = "CLAIMED"
			},
			call: func(ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID, agentID string, envelope map[string]any) (sqlite.RuntimeEventRecord, error) {
				return store.CompleteNodeClaimWithEvent(ctx, sqlite.NodeCompleteInput{
					WorkspaceID:           workspaceID,
					TaskID:                taskID,
					NodeID:                nodeID,
					AgentID:               agentID,
					Summary:               "forged complete context should fail",
					PromptContextEnvelope: envelope,
				})
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			suffix := strings.ReplaceAll(tc.name, " ", "-")
			workspaceID := "ws-agent-node-forged-transition-" + suffix
			taskID := "task-agent-node-forged-transition-" + suffix
			agentID := "agent-node-forged-transition-" + suffix
			nodeID := "node-1"
			seedD2ANodeWorkspace(t, ctx, store, workspaceID, taskID, agentID, nodeID)
			claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
			if _, err := store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				NodeID:      nodeID,
				AgentID:     agentID,
				Summary:     "seed active claim",
			}); err != nil {
				t.Fatalf("seed active node claim: %v", err)
			}

			envelope := boundAgentNodePromptContextEnvelope(tc.surface, workspaceID, taskID, nodeID, agentID)
			tc.mutate(envelope)
			if _, err := tc.call(ctx, store, workspaceID, taskID, nodeID, agentID, envelope); err == nil {
				t.Fatal("expected forged transition node prompt context to fail closed")
			}
			if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   tc.eventType,
				EntityType:  "dag_node",
				EntityID:    nodeID,
				TaskID:      taskID,
				Limit:       10,
			}); got != 0 {
				t.Fatalf("forged prompt context appended %s events: %d", tc.eventType, got)
			}
			nodes, err := store.ListWorkspaceNodes(ctx, sqlite.WorkspaceNodeFilter{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				Limit:       10,
			})
			if err != nil {
				t.Fatalf("list nodes after forged transition: %v", err)
			}
			if len(nodes) != 1 || nodes[0].Status != model.NodeStatusRunning || nodes[0].ClaimStatus == nil || *nodes[0].ClaimStatus != model.TaskClaimStatusClaimed {
				t.Fatalf("expected forged transition rollback to keep node actively claimed, got %+v", nodes)
			}
		})
	}
}

func TestAgentNodeReleaseNoOpIgnoresPromptContextWithoutRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-node-prompt-noop"
		taskID      = "task-agent-node-prompt-noop"
		agentID     = "agent-node-prompt-noop"
		nodeID      = "node-1"
	)
	seedD2ANodeWorkspace(t, ctx, store, workspaceID, taskID, agentID, nodeID)
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	envelope := boundAgentNodePromptContextEnvelope("agent.node.release", workspaceID, taskID, nodeID, agentID)
	envelope["node_id"] = "node-forged-noop"
	event, err := store.ReleaseNodeClaimWithEvent(ctx, sqlite.NodeReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		NodeID:                nodeID,
		AgentID:               agentID,
		Reason:                "noop release should stay quiet",
		PromptContextEnvelope: envelope,
	})
	if err != nil {
		t.Fatalf("noop release should not validate or persist a prompt context: %v", err)
	}
	if event.EventID != "" {
		t.Fatalf("expected noop node release to return zero runtime event, got %+v", event)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "node.released",
		EntityType:  "dag_node",
		EntityID:    nodeID,
		TaskID:      taskID,
		Limit:       10,
	}); got != 0 {
		t.Fatalf("noop release appended node.released events: %d", got)
	}
}

func boundAgentNodePromptContextEnvelope(surface, workspaceID, taskID, nodeID, agentID string) map[string]any {
	envelope := sqlite.BuildNodeLifecyclePromptContextEnvelope(surface, "server_rpc", workspaceID, "agent", agentID)
	envelope["actor_agent_id"] = agentID
	envelope["agent_id"] = agentID
	envelope["task_id"] = taskID
	envelope["node_id"] = nodeID
	status := agentNodePromptStatusForTestSurface(surface)
	envelope["status"] = status
	envelope["node_claim_status"] = status
	envelope["node_status_after"] = agentNodePromptStatusAfterForTestSurface(surface)
	return envelope
}

func assertAgentNodeRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantTaskID, wantNodeID, wantAgentID string) {
	t.Helper()
	if event.EventID == "" {
		t.Fatal("expected runtime event")
	}
	payload := decodeAgentNodePromptPayload(t, event.PayloadJSON)
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
		"status":                             agentNodePromptStatusForTestSurface(wantSurface),
		"node_claim_status":                  agentNodePromptStatusForTestSurface(wantSurface),
		"node_status_after":                  agentNodePromptStatusAfterForTestSurface(wantSurface),
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

func agentNodePromptStatusForTestSurface(surface string) string {
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

func agentNodePromptStatusAfterForTestSurface(surface string) string {
	switch surface {
	case "agent.node.claim":
		return model.NodeStatusRunning
	case "agent.node.release":
		return model.NodeStatusPending
	case "agent.node.complete":
		return model.NodeStatusResolved
	default:
		return ""
	}
}

func assertNodeStayedPendingWithoutClaim(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID string) {
	t.Helper()
	nodes, err := store.ListWorkspaceNodes(ctx, sqlite.WorkspaceNodeFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list nodes after rejected prompt context: %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != nodeID {
		t.Fatalf("expected one node %s after rejected prompt context, got %+v", nodeID, nodes)
	}
	if nodes[0].Status != model.NodeStatusPending {
		t.Fatalf("expected node to stay PENDING after rejected prompt context, got %+v", nodes[0])
	}
	if nodes[0].ClaimStatus != nil {
		t.Fatalf("expected no node claim after rejected prompt context, got %+v", nodes[0])
	}
}

func decodeAgentNodePromptPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode node runtime payload: %v", err)
	}
	return payload
}
