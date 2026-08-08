package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// T-1: Spawn an agent with valid config, verify it implements Agent interface.
func TestDefaultSpawner_Spawn_Success(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	createAgentTestWorkspace(t, store, "ws-1")
	spawner := NewDefaultSpawner(store)

	agent, err := spawner.Spawn(AgentConfig{
		ID:          "parent",
		WorkspaceID: "ws-1",
	}, llm.ClientConfig{
		APIKey: "sk-test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify it implements Agent interface
	var _ Agent = agent

	// Verify the ID has the sub-agent suffix
	if !strings.HasPrefix(agent.ID(), "parent-sub-") {
		t.Fatalf("expected ID to start with 'parent-sub-', got %q", agent.ID())
	}
}

// T-2: Spawn two agents with same base ID, verify unique IDs.
func TestDefaultSpawner_Spawn_UniqueID(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	createAgentTestWorkspace(t, store, "ws-1")
	spawner := NewDefaultSpawner(store)

	cfg := AgentConfig{
		ID:          "base-agent",
		WorkspaceID: "ws-1",
	}
	llmCfg := llm.ClientConfig{APIKey: "sk-test"}

	agent1, err := spawner.Spawn(cfg, llmCfg)
	if err != nil {
		t.Fatalf("spawn 1: %v", err)
	}

	agent2, err := spawner.Spawn(cfg, llmCfg)
	if err != nil {
		t.Fatalf("spawn 2: %v", err)
	}

	if agent1.ID() == agent2.ID() {
		t.Fatalf("expected unique IDs, both got %q", agent1.ID())
	}
}

// T-3: Empty ID returns error.
func TestDefaultSpawner_Spawn_EmptyID(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	spawner := NewDefaultSpawner(store)

	_, err := spawner.Spawn(AgentConfig{
		WorkspaceID: "ws-1",
	}, llm.ClientConfig{
		APIKey: "sk-test",
	})
	if err == nil {
		t.Fatal("expected error for empty ID")
	}
}

// T-4: Spawned agent shares the same store and can write to it.
func TestDefaultSpawner_Spawn_SharedStore(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	createAgentTestWorkspace(t, store, "ws-1")
	spawner := NewDefaultSpawner(store)

	// Create a mock LLM server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := llm.Response{
			ID:    "msg_1",
			Model: "test-model",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Sub-agent done."},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 10, OutputTokens: 5},
		}
		w.Header().Set("Content-Type", "application/json")
		data, _ := json.Marshal(resp)
		w.Write(data)
	}))
	t.Cleanup(srv.Close)

	agent, err := spawner.Spawn(AgentConfig{
		ID:          "shared-store",
		WorkspaceID: "ws-1",
	}, llm.ClientConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Run the spawned agent — it should persist a session to the shared store
	result, err := agent.Run(context.Background(), "Do a sub-task")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.FinalResponse != "Sub-agent done." {
		t.Fatalf("expected final response %q, got %q", "Sub-agent done.", result.FinalResponse)
	}

	// Verify the session was written to the shared store
	sessions, err := store.ListAgentSessions(context.Background(), "ws-1", agent.ID(), 10)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session in shared store, got %d", len(sessions))
	}
	if sessions[0].Status != "COMPLETED" {
		t.Fatalf("expected COMPLETED, got %q", sessions[0].Status)
	}
}

// Verify SubAgentSpawner interface is satisfied at compile time.
var _ SubAgentSpawner = (*DefaultSpawner)(nil)
