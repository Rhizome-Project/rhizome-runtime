package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
)

func TestMCPWorkspaceIsolation(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	for _, input := range []mcp.RegisterInput{
		{
			ServerID:     "shared-server",
			WorkspaceID:  "ws-alpha",
			DisplayName:  "Alpha MCP",
			Transport:    "streamable-http",
			URL:          "http://alpha.example/mcp",
			RegisteredBy: "developer",
		},
		{
			ServerID:     "shared-server",
			WorkspaceID:  "ws-beta",
			DisplayName:  "Beta MCP",
			Transport:    "streamable-http",
			URL:          "http://beta.example/mcp",
			RegisteredBy: "developer",
		},
	} {
		if err := h.mcpStore.RegisterServer(ctx, input); err != nil {
			t.Fatalf("register server %s/%s: %v", input.WorkspaceID, input.ServerID, err)
		}
	}

	if err := h.mcpStore.SaveDiscoveredTools(ctx, "ws-alpha", "shared-server", []mcp.Tool{{
		Name:        "alpha-tool",
		Description: "alpha only",
	}}); err != nil {
		t.Fatalf("save ws-alpha tools: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, "ws-beta", "shared-server", []mcp.Tool{{
		Name:        "beta-tool",
		Description: "beta only",
	}}); err != nil {
		t.Fatalf("save ws-beta tools: %v", err)
	}

	alphaRaw, err := json.Marshal(mcpToolListParams{WorkspaceID: "ws-alpha"})
	if err != nil {
		t.Fatalf("marshal ws-alpha params: %v", err)
	}
	alphaAny, rpcErr := h.mcpToolList(testAuthContext("ws-alpha", "human", "developer"), alphaRaw)
	if rpcErr != nil {
		t.Fatalf("ws-alpha tool list rpc error: %+v", rpcErr)
	}
	alphaResp, ok := alphaAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ws-alpha response type %T", alphaAny)
	}
	alphaTools, ok := alphaResp["tools"].([]mcp.ServerToolRecord)
	if !ok {
		t.Fatalf("unexpected ws-alpha tools payload type %T", alphaResp["tools"])
	}
	if len(alphaTools) != 1 {
		t.Fatalf("expected 1 ws-alpha tool, got %+v", alphaTools)
	}
	if alphaTools[0].ToolName != "alpha-tool" || alphaTools[0].ServerID != "shared-server" {
		t.Fatalf("unexpected ws-alpha tool payload %+v", alphaTools[0])
	}

	betaRaw, err := json.Marshal(mcpToolListParams{WorkspaceID: "ws-beta"})
	if err != nil {
		t.Fatalf("marshal ws-beta params: %v", err)
	}
	betaAny, rpcErr := h.mcpToolList(testAuthContext("ws-beta", "human", "developer"), betaRaw)
	if rpcErr != nil {
		t.Fatalf("ws-beta tool list rpc error: %+v", rpcErr)
	}
	betaResp, ok := betaAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ws-beta response type %T", betaAny)
	}
	betaTools, ok := betaResp["tools"].([]mcp.ServerToolRecord)
	if !ok {
		t.Fatalf("unexpected ws-beta tools payload type %T", betaResp["tools"])
	}
	if len(betaTools) != 1 {
		t.Fatalf("expected 1 ws-beta tool, got %+v", betaTools)
	}
	if betaTools[0].ToolName != "beta-tool" || betaTools[0].ServerID != "shared-server" {
		t.Fatalf("unexpected ws-beta tool payload %+v", betaTools[0])
	}
}
