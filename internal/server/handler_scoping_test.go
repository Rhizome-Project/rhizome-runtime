package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentStateSetRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "agent", "agent-a")

	raw, err := json.Marshal(agentStateSetParams{
		WorkspaceID: "ws-other",
		AgentID:     "agent-a",
		Key:         "flag",
		Value:       "1",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.agentStateSet(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestWorkspaceMemoryListRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "human", "developer")

	raw, err := json.Marshal(workspaceMemoryListParams{
		WorkspaceID: "ws-other",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestWorkspaceSessionsListRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "agent", "agent-a")

	raw, err := json.Marshal(workspaceSessionsListParams{
		WorkspaceID: "ws-other",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.workspaceSessionsList(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

// ── P1A-002: tool.* workspace isolation ─────────────────────────────

func TestToolListRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "human", "developer")

	raw, err := json.Marshal(toolListParams{WorkspaceID: "ws-other"})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolList(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error for tool.list workspace mismatch")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestToolRegisterRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "human", "developer")

	raw, err := json.Marshal(toolRegisterParams{
		WorkspaceID: "ws-other",
		ToolID:      "evil-tool",
		DisplayName: "Evil",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolRegister(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error for tool.register workspace mismatch")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestToolStatusRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "human", "developer")

	raw, err := json.Marshal(toolStatusParams{
		WorkspaceID: "ws-other",
		ToolID:      "some-tool",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolStatus(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error for tool.status workspace mismatch")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestToolDeployRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "human", "developer")

	raw, err := json.Marshal(toolDeployParams{
		WorkspaceID: "ws-other",
		ToolID:      "evil-tool",
		Runtime:     "node",
		SourceCode:  "console.log('pwned')",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolDeploy(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error for tool.deploy workspace mismatch")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestToolCallRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "agent", "agent-a")

	raw, err := json.Marshal(toolCallParams{
		WorkspaceID: "ws-other",
		ToolID:      "some-tool",
		Arguments:   map[string]any{"query": "data"},
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolCall(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error for tool.call workspace mismatch")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestToolCallRejectsRemovedLegacyProviderBeforeDispatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "agent", "agent-a")

	raw, err := json.Marshal(toolCallParams{
		WorkspaceID: "ws-owned",
		ToolID:      sqlite.RemovedLegacyProviderToolID,
		Arguments:   map[string]any{"query": "data"},
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolCall(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected removed provider error")
	}
	if rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "has been removed from Rhizome") {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestToolUndeployRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "human", "developer")

	raw, err := json.Marshal(toolUndeployParams{
		WorkspaceID: "ws-other",
		ToolID:      "some-tool",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolUndeploy(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error for tool.undeploy workspace mismatch")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}

func TestToolRemoveRejectsWorkspaceMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-owned", "human", "developer")

	raw, err := json.Marshal(toolRemoveParams{
		WorkspaceID: "ws-other",
		ToolID:      "some-tool",
		RemovedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.toolRemove(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected permission error for tool.remove workspace mismatch")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error: %+v", rpcErr)
	}
}
