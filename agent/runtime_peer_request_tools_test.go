package main

import (
	"context"
	"strings"
	"testing"
)

func TestPeerRequestToolExecutorBlocksMutationTools(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws",
		AgentID:     "beta",
		OwnerUserID: "owner",
	}, &sequenceLLM{})
	t.Cleanup(func() { _ = runtime.Close() })

	result := runtime.peerRequestToolExecutor(context.Background(), NewToolRegistry(), ToolCall{
		ID:   "call-1",
		Type: "function",
		Function: FunctionCall{
			Name:      "project_checkout_materialize",
			Arguments: `{"project_id":"project-alpha","active_task_id":"task-impl"}`,
		},
	})
	if !result.IsError {
		t.Fatalf("expected project mutation tool to be blocked in model.ask context, got %+v", result)
	}
	for _, want := range []string{"model.ask", "read-only", "blocked tool project_checkout_materialize", "claim the relevant task"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
}

func TestPeerRequestReadOnlyToolAllowlist(t *testing.T) {
	for _, name := range []string{"workspace_doc_get", "read_file", "list_directory", "memory_search"} {
		if !peerRequestReadOnlyToolAllowed(name) {
			t.Fatalf("expected %s to be allowed in peer request context", name)
		}
	}
	for _, name := range []string{"workspace_doc_put", "write_file", "shell", "task_submit", "project_branch_commit", "agent_request"} {
		if peerRequestReadOnlyToolAllowed(name) {
			t.Fatalf("expected %s to be blocked in peer request context", name)
		}
	}
}
