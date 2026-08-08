package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRhizomeMCPToolExecuteUsesMCPToolCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "mcp.tool.call" {
			t.Fatalf("expected mcp.tool.call, got %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"server_id": "notion",
			"tool_name": "search_docs",
			"is_error":  false,
			"content": []any{
				map[string]any{"type": "text", "text": "matching docs"},
			},
		})
	}))
	defer server.Close()

	record := MCPToolRecord{
		ServerID:    "notion",
		ToolName:    "search_docs",
		Description: "Search docs",
		InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
	}
	tool := NewRhizomeMCPTool(NewRhizomeClient(server.URL, "token"), "ws", record)
	result := tool.Execute(context.Background(), map[string]any{"query": "runtime"})
	if result == nil || result.IsError {
		t.Fatalf("expected successful mcp tool call, got %+v", result)
	}
	if result.Output != "matching docs" {
		t.Fatalf("unexpected output %q", result.Output)
	}
}

func TestRhizomeMCPToolExecuteRejectsMismatchedIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "mcp.tool.call" {
			t.Fatalf("expected mcp.tool.call, got %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"server_id": "notion",
			"tool_name": "other_docs",
			"is_error":  false,
			"content": []any{
				map[string]any{"type": "text", "text": "wrong routed payload"},
			},
		})
	}))
	defer server.Close()

	record := MCPToolRecord{
		ServerID:    "notion",
		ToolName:    "search_docs",
		Description: "Search docs",
		InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
	}
	tool := NewRhizomeMCPTool(NewRhizomeClient(server.URL, "token"), "ws", record)
	result := tool.Execute(context.Background(), map[string]any{"query": "runtime"})
	if result == nil || !result.IsError {
		t.Fatalf("expected mismatched mcp identity to surface as error, got %+v", result)
	}
	if !strings.Contains(result.Output, `mcp tool notion/search_docs returned mismatched identity "notion"/"other_docs"`) {
		t.Fatalf("expected explicit mismatched identity error, got %q", result.Output)
	}
}

func TestRefreshRhizomeMCPToolsRejectsSanitizedSiblingCollision(t *testing.T) {
	server := mcpToolListTestServer(t, []MCPToolRecord{
		{
			ServerID:    "notion-search",
			ToolName:    "docs",
			Description: "Search docs via dash server id",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
		{
			ServerID:    "notion/search",
			ToolName:    "docs",
			Description: "Search docs via slash server id",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
	})
	defer server.Close()

	agent := &Agent{Workdir: t.TempDir()}
	agent.Init()

	err := agent.RefreshRhizomeMCPTools(context.Background(), NewRhizomeClient(server.URL, "token"), "ws")
	if err == nil || !strings.Contains(err.Error(), `"notion-search/docs" and "notion/search/docs" collide after sanitization as "mcp__notion_search__docs"`) {
		t.Fatalf("expected mcp sibling collision error, got %v", err)
	}
	if _, ok := agent.registry.Get("mcp__notion_search__docs"); ok {
		t.Fatalf("expected colliding mcp alias to stay unregistered after reject")
	}
	if _, ok := agent.registry.Get("read_file"); !ok {
		t.Fatalf("expected built-in read_file to remain registered after mcp collision reject")
	}
}

func TestRefreshRhizomeMCPToolsRejectsUnsanitizableIdentifier(t *testing.T) {
	server := mcpToolListTestServer(t, []MCPToolRecord{
		{
			ServerID:    "!!!",
			ToolName:    "***",
			Description: "Hostile MCP inventory with no usable identifiers",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
	})
	defer server.Close()

	agent := &Agent{Workdir: t.TempDir()}
	agent.Init()

	err := agent.RefreshRhizomeMCPTools(context.Background(), NewRhizomeClient(server.URL, "token"), "ws")
	if err == nil || !strings.Contains(err.Error(), `mcp tool "!!!/***" has no usable identifier after sanitization`) {
		t.Fatalf("expected unsanitizable mcp identifier error, got %v", err)
	}
	if _, ok := agent.registry.Get(sanitizeMCPFunctionName("!!!", "***")); ok {
		t.Fatalf("expected garbage mcp alias to stay unregistered after reject")
	}
	if _, ok := agent.registry.Get("read_file"); !ok {
		t.Fatalf("expected built-in read_file to remain registered after unsanitizable reject")
	}
}

func TestRefreshRhizomeMCPToolsClearsStaleDynamicToolsAfterCollisionReject(t *testing.T) {
	validServer := mcpToolListTestServer(t, []MCPToolRecord{
		{
			ServerID:    "notion",
			ToolName:    "search_docs",
			Description: "Search docs",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
	})
	defer validServer.Close()

	invalidServer := mcpToolListTestServer(t, []MCPToolRecord{
		{
			ServerID:    "notion-search",
			ToolName:    "docs",
			Description: "Search docs via dash server id",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
		{
			ServerID:    "notion/search",
			ToolName:    "docs",
			Description: "Search docs via slash server id",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
	})
	defer invalidServer.Close()

	agent := &Agent{Workdir: t.TempDir()}
	agent.Init()

	if err := agent.RefreshRhizomeMCPTools(context.Background(), NewRhizomeClient(validServer.URL, "token"), "ws"); err != nil {
		t.Fatalf("expected valid mcp refresh to succeed, got %v", err)
	}
	if _, ok := agent.registry.Get("mcp__notion__search_docs"); !ok {
		t.Fatalf("expected valid dynamic mcp tool to be registered before reject")
	}

	err := agent.RefreshRhizomeMCPTools(context.Background(), NewRhizomeClient(invalidServer.URL, "token"), "ws")
	if err == nil || !strings.Contains(err.Error(), `"notion-search/docs" and "notion/search/docs" collide after sanitization as "mcp__notion_search__docs"`) {
		t.Fatalf("expected mcp sibling collision error, got %v", err)
	}
	if _, ok := agent.registry.Get("mcp__notion__search_docs"); ok {
		t.Fatalf("expected stale dynamic mcp tool to be cleared after collision reject")
	}
	if _, ok := agent.registry.Get("read_file"); !ok {
		t.Fatalf("expected built-in read_file to remain registered after mcp collision reject")
	}
}

func TestRefreshRhizomeMCPToolsClearsStaleDynamicToolsAfterUnsanitizableReject(t *testing.T) {
	validServer := mcpToolListTestServer(t, []MCPToolRecord{
		{
			ServerID:    "notion",
			ToolName:    "search_docs",
			Description: "Search docs",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
	})
	defer validServer.Close()

	invalidServer := mcpToolListTestServer(t, []MCPToolRecord{
		{
			ServerID:    "!!!",
			ToolName:    "***",
			Description: "Hostile MCP inventory with no usable identifiers",
			InputSchema: `{"type":"object","properties":{"query":{"type":"string"}}}`,
		},
	})
	defer invalidServer.Close()

	agent := &Agent{Workdir: t.TempDir()}
	agent.Init()

	if err := agent.RefreshRhizomeMCPTools(context.Background(), NewRhizomeClient(validServer.URL, "token"), "ws"); err != nil {
		t.Fatalf("expected valid mcp refresh to succeed, got %v", err)
	}
	if _, ok := agent.registry.Get("mcp__notion__search_docs"); !ok {
		t.Fatalf("expected valid dynamic mcp tool to be registered before reject")
	}

	err := agent.RefreshRhizomeMCPTools(context.Background(), NewRhizomeClient(invalidServer.URL, "token"), "ws")
	if err == nil || !strings.Contains(err.Error(), `mcp tool "!!!/***" has no usable identifier after sanitization`) {
		t.Fatalf("expected unsanitizable mcp identifier error, got %v", err)
	}
	if _, ok := agent.registry.Get("mcp__notion__search_docs"); ok {
		t.Fatalf("expected stale dynamic mcp tool to be cleared after unsanitizable reject")
	}
	if _, ok := agent.registry.Get("read_file"); !ok {
		t.Fatalf("expected built-in read_file to remain registered after unsanitizable reject")
	}
}

func mcpToolListTestServer(t *testing.T, records []MCPToolRecord) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "mcp.tool.list" {
			t.Fatalf("expected mcp.tool.list, got %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{
			"tools": records,
		})
	}))
}
