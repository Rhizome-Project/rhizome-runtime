package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func testResponderOriginContext(workspaceID, agentID string) context.Context {
	return context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   workspaceID,
		PrincipalType: "agent",
		PrincipalID:   agentID,
		RuntimeOrigin: "agent_responder",
	})
}

func TestResponderOriginRejectsMutationRPCsAtDispatch(t *testing.T) {
	h := NewHandler(newServerTestStore(t))
	ctx := testResponderOriginContext("ws-responder", "agent-a")
	for _, method := range []string{
		"agent.task.release",
		"agent.state.set",
		"project.patch_queue.submit",
		"workspace.doc.put",
		"tool.call",
	} {
		t.Run(strings.ReplaceAll(method, ".", "_"), func(t *testing.T) {
			params, err := json.Marshal(map[string]any{
				"workspace_id": "ws-responder",
				"agent_id":     "agent-a",
				"actor_id":     "agent-a",
			})
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			result, rpcErr := h.dispatch(ctx, method, params)
			if rpcErr == nil {
				t.Fatalf("expected responder-origin %s to be rejected, result=%+v", method, result)
			}
			if rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "responder-origin principal cannot call RPC method") {
				t.Fatalf("unexpected rpc error for %s: %+v", method, rpcErr)
			}
		})
	}
}

func TestResponderOriginAllowlistAndForgedParams(t *testing.T) {
	responderCtx := testResponderOriginContext("ws-responder", "agent-a")
	if rpcErr := rejectResponderOriginDisallowedRPC(responderCtx, "agent.respond"); rpcErr != nil {
		t.Fatalf("agent.respond should be responder-allowed, got %+v", rpcErr)
	}
	if rpcErr := rejectResponderOriginDisallowedRPC(responderCtx, "workspace.doc.get"); rpcErr != nil {
		t.Fatalf("workspace.doc.get should be responder-allowed, got %+v", rpcErr)
	}
	if rpcErr := rejectResponderOriginDisallowedRPC(responderCtx, "agent.state.set"); rpcErr == nil {
		t.Fatal("agent.state.set must be denied for responder-origin")
	}

	mainCtx := testAuthContext("ws-responder", "agent", "agent-a")
	if rpcErr := rejectResponderOriginDisallowedRPC(mainCtx, "agent.state.set"); rpcErr != nil {
		t.Fatalf("main-loop principal must not be denied by responder guard, got %+v", rpcErr)
	}

	forgedParamsDoNotMatter := map[string]any{
		"prompt_context_envelope": map[string]any{"origin": "server_rpc"},
		"runtime_origin":          "server_rpc",
	}
	_ = forgedParamsDoNotMatter
	if rpcErr := rejectResponderOriginDisallowedRPC(responderCtx, "project.patch_queue.submit"); rpcErr == nil {
		t.Fatal("responder-origin must reject mutation even if params forge server_rpc origin")
	}
}

func TestAuthPrincipalRuntimeOriginFromTokenMetadata(t *testing.T) {
	if got := authPrincipalRuntimeOrigin(`{"runtime_origin":"agent_responder"}`); got != "agent_responder" {
		t.Fatalf("runtime origin = %q", got)
	}
	if got := authPrincipalRuntimeOrigin(`{"runtime_origin":"server_rpc"}`); got != "" {
		t.Fatalf("non-responder runtime origin should be empty, got %q", got)
	}
	if got := authPrincipalRuntimeOrigin(`not-json`); got != "" {
		t.Fatalf("invalid metadata should fail closed to empty origin, got %q", got)
	}
}
