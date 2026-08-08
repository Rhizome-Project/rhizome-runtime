package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRPCContractTaskSubmitRequiresWorkspacePrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-rpc-contract-task-submit-auth"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RPC Contract Task Submit Auth",
		CreatedBy:   "test",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	params := map[string]any{
		"workspace_id":  workspaceID,
		"task_id":       "task-rpc-contract-unauth",
		"owner_user_id": "developer",
		"title":         "Unauthenticated task submit must fail",
	}
	resp := callHandlerServeHTTPRPC(t, h, context.Background(), "task.submit", params)
	if resp.Error == nil || resp.Error.Code != errCodePermissionDenied {
		t.Fatalf("expected task.submit permission denied through ServeHTTP, got %+v", resp)
	}

	params["task_id"] = "task-rpc-contract-auth"
	resp = callHandlerServeHTTPRPC(t, h, testAuthContext(workspaceID, "human", "developer"), "task.submit", params)
	if resp.Error != nil {
		t.Fatalf("expected authorized task.submit success through ServeHTTP, got %+v", resp.Error)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok || result["task_id"] != "task-rpc-contract-auth" {
		t.Fatalf("unexpected task.submit result through ServeHTTP: %+v", resp.Result)
	}
}

func TestRPCContractAgentTaskClaimAndCompleteUseServeHTTPAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-rpc-contract-agent-task"
		agentID     = "agent-rpc-contract"
		taskID      = "task-rpc-contract-agent-task"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)

	claimResp := callHandlerServeHTTPRPC(t, h, ctx, "agent.task.claim", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"task_id":      taskID,
		"summary":      "claim via ServeHTTP",
	})
	if claimResp.Error != nil {
		t.Fatalf("expected agent.task.claim ServeHTTP success, got %+v", claimResp.Error)
	}
	completeResp := callHandlerServeHTTPRPC(t, h, ctx, "agent.task.complete", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"task_id":      taskID,
		"summary":      "complete via ServeHTTP",
	})
	if completeResp.Error != nil {
		t.Fatalf("expected agent.task.complete ServeHTTP success, got %+v", completeResp.Error)
	}

	mismatchCtx := testAuthContext(workspaceID, "agent", "agent-other")
	mismatchResp := callHandlerServeHTTPRPC(t, h, mismatchCtx, "agent.task.claim", map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"task_id":      taskID,
	})
	if mismatchResp.Error == nil || mismatchResp.Error.Code != errCodePermissionDenied {
		t.Fatalf("expected mismatched agent.task.claim ServeHTTP permission denied, got %+v", mismatchResp)
	}
}

func callHandlerServeHTTPRPC(t *testing.T, h *Handler, ctx context.Context, method string, params map[string]any) RPCResponse {
	t.Helper()
	rawParams, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	body, err := json.Marshal(RPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  rawParams,
		ID:      json.RawMessage(`"test"`),
	})
	if err != nil {
		t.Fatalf("marshal rpc request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader(body))
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected HTTP status %d body=%s", rec.Code, rec.Body.String())
	}
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode rpc response: %v body=%s", err, rec.Body.String())
	}
	return resp
}
