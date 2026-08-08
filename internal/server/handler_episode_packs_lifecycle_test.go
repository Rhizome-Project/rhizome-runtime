package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestSessionHandlersExposeLifecycleEpisodePacks(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-episode-pack-lifecycle"
		taskID      = "task-handler-episode-pack-lifecycle"
		sessionID   = "sess-handler-episode-pack-lifecycle"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Episode Pack Lifecycle",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
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
		Summary:     "claim before blocked session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	for _, msg := range []sqlite.AgentSessionMessageInput{
		{SessionID: sessionID, Sequence: 0, Role: "user", ContentJSON: `{"text":"Need approval."}`, TokenCount: 8},
		{SessionID: sessionID, Sequence: 1, Role: "assistant", ContentJSON: `{"text":"Waiting on operator."}`, TokenCount: 10},
	} {
		if err := store.AppendAgentSessionMessage(ctx, msg); err != nil {
			t.Fatalf("append session message: %v", err)
		}
	}

	if _, rpcErr := callAgentSessionBlockedRaw(t, h, ctx, mustMarshalJSON(t, agentSessionEventParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Blocked pending approval",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve rollout"}},
		HandoffTo:   "agent-b",
	})); rpcErr != nil {
		t.Fatalf("agentSessionBlocked rpc error: %+v", rpcErr)
	}

	listRaw, err := json.Marshal(workspaceEpisodePackListParams{
		WorkspaceID: workspaceID,
		PackType:    "SESSION_BLOCKED",
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal blocked pack list params: %v", err)
	}
	listResult, rpcErr := h.workspaceEpisodePackList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEpisodePackList blocked rpc error: %+v", rpcErr)
	}
	blockedItems := listResult.(map[string]any)["items"].([]sqlite.EpisodePackRecord)
	if len(blockedItems) != 1 || blockedItems[0].LifecycleEventID == "" {
		t.Fatalf("unexpected blocked episode packs %+v", blockedItems)
	}

	if _, rpcErr := callAgentSessionTakeoverRaw(t, h, ctx, mustMarshalJSON(t, agentSessionTakeoverParams{
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		TakeoverAgentID: "agent-b",
		Summary:         "Switch to specialist",
	})); rpcErr != nil {
		t.Fatalf("agentSessionTakeover rpc error: %+v", rpcErr)
	}

	takeoverRaw, err := json.Marshal(workspaceEpisodePackListParams{
		WorkspaceID: workspaceID,
		PackType:    "SESSION_TAKEOVER",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal takeover pack list params: %v", err)
	}
	takeoverResult, rpcErr := h.workspaceEpisodePackList(ctx, takeoverRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEpisodePackList takeover rpc error: %+v", rpcErr)
	}
	takeoverItems := takeoverResult.(map[string]any)["items"].([]sqlite.EpisodePackRecord)
	if len(takeoverItems) != 1 || takeoverItems[0].LifecycleEventID == "" {
		t.Fatalf("unexpected takeover episode packs %+v", takeoverItems)
	}
}
