package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/hooks"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/plugins"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// --- Helpers ---

func newAgentTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "agent-test.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		_ = store.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		_ = store.Close()
		t.Fatalf("ensure local authority node: %v", err)
	}
	authorityNodeIDLiteral := strings.ReplaceAll(node.AuthorityNodeID, `'`, `''`)
	triggerSQL := fmt.Sprintf(`
CREATE TRIGGER IF NOT EXISTS test_seed_workspace_authority_after_insert
AFTER INSERT ON workspaces
BEGIN
	INSERT INTO workspace_authority(
		workspace_id,
		scope,
		holder_authority_node_id,
		lease_token,
		term,
		lease_expires_at,
		commit_watermark,
		applied_watermark,
		status,
		updated_at
	) VALUES (
		NEW.workspace_id,
		'workspace',
		'%s',
		'lease-agent-test-auto-' || NEW.workspace_id,
		1,
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now','+1 hour'),
		1,
		1,
		'ACTIVE',
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now')
	)
	ON CONFLICT(workspace_id, scope) DO NOTHING;
END
`, authorityNodeIDLiteral)
	if _, err := store.DB().ExecContext(ctx, triggerSQL); err != nil {
		_ = store.Close()
		t.Fatalf("install workspace authority seed trigger: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func createAgentTestWorkspace(t *testing.T, store *sqlite.Store, workspaceID string) {
	t.Helper()
	if err := store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Test Workspace",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
}

func newMockLLMClient(t *testing.T, responses []llm.Response) (*llm.Client, *httptest.Server) {
	t.Helper()
	var callIdx atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(callIdx.Add(1)) - 1
		if idx >= len(responses) {
			idx = len(responses) - 1
		}
		w.Header().Set("Content-Type", "application/json")
		data, _ := json.Marshal(responses[idx])
		w.Write(data)
	}))
	t.Cleanup(srv.Close)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	return client, srv
}

// --- Tests ---

// T-1: Verifies R-5 — create agent with valid config, no error.
func TestNewInternalAgent_Success(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "test-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent == nil {
		t.Fatal("expected non-nil agent")
	}
	if agent.ID() != "test-agent" {
		t.Fatalf("expected ID %q, got %q", "test-agent", agent.ID())
	}
}

func TestNewInternalAgent_PropagatesRepoMutationPolicyToBuiltinWrite(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})
	workspaceDir := t.TempDir()
	var denied []tools.MutationDenyRecord

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:           "test-agent-mutation-policy",
		WorkspaceID:  "ws-1",
		WorkspaceDir: workspaceDir,
		RepoMutation: tools.RepoMutationPolicy{
			RequireAuthority: true,
			RecordDeny: func(record tools.MutationDenyRecord) {
				denied = append(denied, record)
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	writeTool, ok := agent.toolRegistry.Get("write")
	if !ok {
		t.Fatal("expected builtin write tool")
	}
	input, _ := json.Marshal(map[string]any{
		"file_path": "owned.txt",
		"content":   "bypass",
	})
	result, err := writeTool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected tool error: %v", err)
	}
	if !strings.Contains(result, tools.DirectRepoMutationDeniedReason) {
		t.Fatalf("expected repo mutation denial, got %q", result)
	}
	if _, err := os.Stat(filepath.Join(workspaceDir, "owned.txt")); !os.IsNotExist(err) {
		t.Fatalf("denied write should not create file; stat err=%v", err)
	}
	if len(denied) != 1 || denied[0].ToolName != "write" {
		t.Fatalf("expected one write deny record, got %+v", denied)
	}
}

// Verifies R-4 defaults — DisplayName defaults to ID, Role defaults to "internal-agent".
func TestNewInternalAgent_Defaults(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "my-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent.config.DisplayName != "my-agent" {
		t.Fatalf("expected DisplayName %q, got %q", "my-agent", agent.config.DisplayName)
	}
	if agent.config.Role != "internal-agent" {
		t.Fatalf("expected Role %q, got %q", "internal-agent", agent.config.Role)
	}
}

// T-2: Verifies EC-3 — empty ID returns error.
func TestNewInternalAgent_MissingID(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	_, err := NewInternalAgent(store, client, AgentConfig{
		WorkspaceID: "ws-1",
	})
	if err == nil {
		t.Fatal("expected error for missing ID")
	}
	if !strings.Contains(err.Error(), "ID") {
		t.Fatalf("expected error about ID, got: %v", err)
	}
}

// Verifies EC-3 variant — empty WorkspaceID returns error.
func TestNewInternalAgent_MissingWorkspaceID(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	_, err := NewInternalAgent(store, client, AgentConfig{
		ID: "agent-1",
	})
	if err == nil {
		t.Fatal("expected error for missing WorkspaceID")
	}
}

// T-3: Verifies EC-1 — nil store returns error.
func TestNewInternalAgent_NilStore(t *testing.T) {
	t.Parallel()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	_, err := NewInternalAgent(nil, client, AgentConfig{
		ID:          "test",
		WorkspaceID: "ws-1",
	})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
	if !strings.Contains(err.Error(), "store") {
		t.Fatalf("expected error about store, got: %v", err)
	}
}

// T-4: Verifies EC-2 — nil LLM client returns error.
func TestNewInternalAgent_NilLLM(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)

	_, err := NewInternalAgent(store, nil, AgentConfig{
		ID:          "test",
		WorkspaceID: "ws-1",
	})
	if err == nil {
		t.Fatal("expected error for nil LLM client")
	}
	if !strings.Contains(err.Error(), "llm") {
		t.Fatalf("expected error about llm client, got: %v", err)
	}
}

// T-5: Verifies R-6 — run with mock LLM, verify session exists in store after completion.
func TestInternalAgent_Run_PersistsSession(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	if err := store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-1",
		Title:       "Agent Test Workspace",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	client, _ := newMockLLMClient(t, []llm.Response{
		{
			ID:    "msg_1",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Task completed."},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 100, OutputTokens: 50},
		},
	})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "persist-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	result, err := agent.Run(context.Background(), "Do something")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.FinalResponse != "Task completed." {
		t.Fatalf("expected final response %q, got %q", "Task completed.", result.FinalResponse)
	}

	// Verify session was persisted
	sessions, err := store.ListAgentSessions(context.Background(), "ws-1", "persist-agent", 10)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].Status != "COMPLETED" {
		t.Fatalf("expected status COMPLETED, got %q", sessions[0].Status)
	}
	if sessions[0].Iterations != 1 {
		t.Fatalf("expected 1 iteration, got %d", sessions[0].Iterations)
	}

	sessionStates, err := store.ListWorkspaceSessionStates(context.Background(), "ws-1", false, 10)
	if err != nil {
		t.Fatalf("list workspace session states: %v", err)
	}
	if len(sessionStates) != 1 {
		t.Fatalf("expected 1 workspace session state, got %+v", sessionStates)
	}
	if sessionStates[0].Status != "ENDED" || sessionStates[0].SessionID != sessions[0].SessionID {
		t.Fatalf("unexpected session coordination state: %+v", sessionStates[0])
	}
}

// T-6: Verifies R-6 — run with mock LLM that produces 2 iterations, verify messages are persisted.
func TestInternalAgent_Run_DoesNotClobberExistingAgentMetadata(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	ctx := context.Background()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-preserve",
		Title:       "Internal Agent Preserve",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     "ws-agent-preserve",
		AgentID:         "preserve-agent",
		OwnerUserID:     "partner-owner",
		DisplayName:     "Partner Preserve Agent",
		Role:            "reviewer",
		Status:          "PAUSED",
		ProtocolVersion: "partner-runtime/v5",
		Capabilities:    []string{"review", "analysis"},
		Summary:         "partner summary",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-agent-preserve",
		AgentID:     "preserve-agent",
		Status:      "active",
		Summary:     "live partner summary",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	client, _ := newMockLLMClient(t, []llm.Response{
		{
			ID:    "msg_1",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Task completed."},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 100, OutputTokens: 50},
		},
	})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "preserve-agent",
		WorkspaceID: "ws-agent-preserve",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	if _, err := agent.Run(ctx, "Do something"); err != nil {
		t.Fatalf("run: %v", err)
	}

	record, err := store.GetAgent(ctx, "ws-agent-preserve", "preserve-agent")
	if err != nil {
		t.Fatalf("get agent after run: %v", err)
	}
	if record.OwnerUserID != "partner-owner" || record.DisplayName != "Partner Preserve Agent" {
		t.Fatalf("expected internal agent run not to clobber owner/display, got %+v", record)
	}
	if record.Role != "reviewer" || record.ProtocolVersion != "partner-runtime/v5" {
		t.Fatalf("expected internal agent run not to clobber role/protocol, got %+v", record)
	}
	if len(record.Capabilities) != 2 || record.Capabilities[0] != "review" || record.Capabilities[1] != "analysis" {
		t.Fatalf("expected internal agent run not to clobber capabilities, got %+v", record)
	}
	if record.Summary != "live partner summary" {
		t.Fatalf("expected internal agent run not to clobber summary, got %+v", record)
	}
}

func TestInternalAgent_Run_PersistsMessages(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	createAgentTestWorkspace(t, store, "ws-1")
	toolInput, _ := json.Marshal(map[string]any{"command": "echo test"})
	client, _ := newMockLLMClient(t, []llm.Response{
		{
			ID:    "msg_1",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "I'll run a command."},
				{Type: "tool_use", ID: "tu-1", Name: "bash", Input: json.RawMessage(toolInput)},
			},
			StopReason: llm.StopReasonToolUse,
			Usage:      llm.Usage{InputTokens: 100, OutputTokens: 50},
		},
		{
			ID:    "msg_2",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Done."},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 80, OutputTokens: 30},
		},
	})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "msg-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	result, err := agent.Run(context.Background(), "Run a command")
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Get session
	sessions, err := store.ListAgentSessions(context.Background(), "ws-1", "msg-agent", 10)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}

	// Verify messages were persisted
	msgs, err := store.ListAgentSessionMessages(context.Background(), sessions[0].SessionID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}

	// Expected: user(task), assistant(tool_use), user(tool_result), assistant(text) = 4 messages
	if len(msgs) != len(result.Messages) {
		t.Fatalf("expected %d persisted messages, got %d", len(result.Messages), len(msgs))
	}

	// Verify sequence order
	for i, msg := range msgs {
		if msg.Sequence != i {
			t.Fatalf("msg[%d]: expected sequence %d, got %d", i, i, msg.Sequence)
		}
	}
}

// T-8: Verifies R-8 — session ID matches pattern "sess-{agentID}-{digits}".
func TestInternalAgent_SessionID_Format(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	createAgentTestWorkspace(t, store, "ws-1")
	client, _ := newMockLLMClient(t, []llm.Response{
		{
			ID:    "msg_1",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "ok"},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 10, OutputTokens: 5},
		},
	})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "format-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	_, err = agent.Run(context.Background(), "Do something")
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	sessions, _ := store.ListAgentSessions(context.Background(), "ws-1", "format-agent", 10)
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}

	sessionID := sessions[0].SessionID
	if !strings.HasPrefix(sessionID, "sess-format-agent-") {
		t.Fatalf("session ID %q doesn't match pattern 'sess-format-agent-{digits}'", sessionID)
	}
	// The suffix should be numeric (unix nano)
	suffix := strings.TrimPrefix(sessionID, "sess-format-agent-")
	for _, c := range suffix {
		if c < '0' || c > '9' {
			t.Fatalf("session ID suffix %q should be numeric", suffix)
		}
	}
}

// Verifies R-2 — Agent interface is satisfied.
func TestInternalAgent_ImplementsAgent(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "interface-test",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	// Compile-time check
	var _ Agent = agent
}

// Verifies R-9 — RegisterPlugin adds tools and hooks.
func TestInternalAgent_RegisterPlugin(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "plugin-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	// Register a plugin with a custom tool
	plugin := &testPlugin{
		name: "test-plugin",
		tools: []tools.Tool{
			&testPluginTool{name: "custom_tool"},
		},
		hooks: []hooks.Hook{
			&testPluginHook{name: "custom_hook"},
		},
	}
	agent.RegisterPlugin(plugin)

	// Verify tool was registered
	_, ok := agent.toolRegistry.Get("custom_tool")
	if !ok {
		t.Fatal("expected custom_tool to be registered")
	}
}

// Verifies R-10 — AddHook registers a hook.
func TestInternalAgent_AddHook(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "hook-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	hook := &testPluginHook{name: "direct-hook"}
	agent.AddHook(hook)

	// No assertion beyond not panicking — hook runner doesn't expose list
}

// Verifies R-5 — built-in tools are registered (7 tools).
func TestInternalAgent_BuiltinToolsRegistered(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "builtin-test",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	toolList := agent.toolRegistry.List()
	if len(toolList) != 19 {
		t.Fatalf("expected 19 built-in tools, got %d", len(toolList))
	}
}

// NT-1: Run with LLM error — session should still be persisted as FAILED.
func TestInternalAgent_Run_LLMError_PersistsFailed(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	createAgentTestWorkspace(t, store, "ws-1")

	// Server that always returns 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"server down"}}`))
	}))
	t.Cleanup(srv.Close)

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "error-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	result, err := agent.Run(context.Background(), "Do something")
	if err != nil {
		t.Fatalf("Run should not return Go error, got: %v", err)
	}

	if result.Error == nil {
		t.Fatal("expected LoopResult.Error to be set")
	}

	sessions, _ := store.ListAgentSessions(context.Background(), "ws-1", "error-agent", 10)
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].Status != "FAILED" {
		t.Fatalf("expected status FAILED, got %q", sessions[0].Status)
	}
}

// NT-2: Multiple runs create separate sessions.
func TestInternalAgent_MultipleRuns(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	createAgentTestWorkspace(t, store, "ws-1")
	client, _ := newMockLLMClient(t, []llm.Response{
		{
			ID:    "msg_1",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "done"},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 10, OutputTokens: 5},
		},
	})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "multi-agent",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, err := agent.Run(context.Background(), "Task")
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	sessions, _ := store.ListAgentSessions(context.Background(), "ws-1", "multi-agent", 10)
	if len(sessions) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(sessions))
	}
}

func TestInternalAgent_Run_RecordsCompactionSnapshotAndCanonicalRuntimeState(t *testing.T) {
	t.Parallel()

	store := newAgentTestStore(t)
	if err := store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-compact",
		Title:       "Compaction Runtime State",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	toolInput, _ := json.Marshal(map[string]any{"input": "payload"})
	client, _ := newMockLLMClient(t, []llm.Response{
		{
			ID:    "msg_1",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Loop once."},
				{Type: "tool_use", ID: "tu-1", Name: "custom_tool", Input: json.RawMessage(toolInput)},
			},
			StopReason: llm.StopReasonToolUse,
			Usage:      llm.Usage{InputTokens: 80, OutputTokens: 20},
		},
		{
			ID:    "msg_2",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Loop twice."},
				{Type: "tool_use", ID: "tu-2", Name: "custom_tool", Input: json.RawMessage(toolInput)},
			},
			StopReason: llm.StopReasonToolUse,
			Usage:      llm.Usage{InputTokens: 90, OutputTokens: 20},
		},
		{
			ID:    "msg_3",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Loop three times."},
				{Type: "tool_use", ID: "tu-3", Name: "custom_tool", Input: json.RawMessage(toolInput)},
			},
			StopReason: llm.StopReasonToolUse,
			Usage:      llm.Usage{InputTokens: 90, OutputTokens: 20},
		},
		{
			ID:    "msg_4",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Compacted summary for the active runtime state."},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 60, OutputTokens: 30},
		},
		{
			ID:    "msg_5",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Done after compaction."},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 60, OutputTokens: 25},
		},
	})

	agent, err := NewInternalAgent(store, client, AgentConfig{
		ID:          "compact-agent",
		WorkspaceID: "ws-compact",
		LoopConfig: LoopConfig{
			TokenBudget:   300,
			MaxIterations: 6,
		},
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}
	agent.RegisterPlugin(&testPlugin{
		name: "compact-plugin",
		tools: []tools.Tool{
			&testPluginTool{name: "custom_tool", output: strings.Repeat("canonical-output ", 18)},
		},
	})

	result, err := agent.Run(context.Background(), strings.Repeat("seed context ", 30))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.FinalResponse != "Done after compaction." {
		t.Fatalf("unexpected final response: %q", result.FinalResponse)
	}

	sessions, err := store.ListAgentSessions(context.Background(), "ws-compact", "compact-agent", 10)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}

	msgs, err := store.ListAgentSessionMessages(context.Background(), sessions[0].SessionID)
	if err != nil {
		t.Fatalf("list session messages: %v", err)
	}
	if len(msgs) != len(result.Messages) {
		t.Fatalf("expected canonical message count %d, got %+v", len(result.Messages), msgs)
	}
	if len(msgs) >= 8 {
		t.Fatalf("expected compacted canonical runtime state, got %d messages", len(msgs))
	}
	for _, msg := range msgs {
		if msg.TokenCount <= 0 {
			t.Fatalf("expected token counts on canonical session messages, got %+v", msg)
		}
	}

	snapshots, err := store.ListSessionCompactionSnapshots(context.Background(), sqlite.SessionCompactionSnapshotFilter{
		WorkspaceID: "ws-compact",
		SessionID:   sessions[0].SessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list compaction snapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 compaction snapshot, got %+v", snapshots)
	}
	if snapshots[0].SummaryWorkspaceMemory == "" || snapshots[0].MessageTokensBefore <= snapshots[0].MessageTokensAfter {
		t.Fatalf("unexpected compaction snapshot: %+v", snapshots[0])
	}

	memory, err := store.ListWorkspaceMemory(context.Background(), sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-compact",
		MemoryType:  "SUMMARY",
		SessionID:   sessions[0].SessionID,
		SourceKind:  "compaction",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace memory: %v", err)
	}
	if len(memory) == 0 {
		t.Fatalf("expected compaction memory entry, got %+v", memory)
	}
	if memory[0].MemoryType != "SUMMARY" {
		t.Fatalf("expected compaction memory to use canonical SUMMARY type, got %+v", memory[0])
	}
}

// --- Mock plugin types ---

type testPlugin struct {
	name            string
	tools           []tools.Tool
	hooks           []hooks.Hook
	promptFragments []string
}

func (p *testPlugin) Name() string              { return p.name }
func (p *testPlugin) Description() string       { return "test plugin" }
func (p *testPlugin) Tools() []tools.Tool       { return p.tools }
func (p *testPlugin) Hooks() []hooks.Hook       { return p.hooks }
func (p *testPlugin) PromptFragments() []string { return p.promptFragments }

// Implements plugins.Plugin interface assertion
var _ plugins.Plugin = (*testPlugin)(nil)

type testPluginTool struct {
	name   string
	output string
}

func (t *testPluginTool) Name() string        { return t.name }
func (t *testPluginTool) Description() string { return "test plugin tool" }
func (t *testPluginTool) Schema() tools.Schema {
	return tools.Schema{Type: "object"}
}
func (t *testPluginTool) Execute(_ context.Context, _ json.RawMessage) (string, error) {
	if t.output != "" {
		return t.output, nil
	}
	return "ok", nil
}

type testPluginHook struct {
	name string
}

func (h *testPluginHook) Name() string          { return h.name }
func (h *testPluginHook) Points() []hooks.Point { return []hooks.Point{hooks.BeforeTool} }
func (h *testPluginHook) Run(_ context.Context, _ hooks.Context) (hooks.Result, error) {
	return hooks.Result{}, nil
}
