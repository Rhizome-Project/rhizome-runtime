package control

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTensionAttachToolWritesAgentPromptContextRuntimeEventAndDuplicateNoop(t *testing.T) {
	t.Parallel()

	store := newControlToolTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-attach-tool"
		tensionID   = "tension-attach-tool"
		taskID      = "task-attach-tool"
		agentID     = "agent-attach-tool"
	)
	seedTensionDetachWorkspace(t, ctx, store, workspaceID, tensionID, taskID, agentID)

	tool := NewTensionAttachTool(store, workspaceID, agentID)
	output, err := tool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`","role":"watcher","reason":"joining native tool test","actor_id":"forged","principal_id":"forged","agent_id":"agent-other","prompt_context_surface":"workspace.tension.confirm"}`))
	if err != nil {
		t.Fatalf("attach via native tension tool: %v", err)
	}
	if !strings.Contains(output, "successfully attached") {
		t.Fatalf("expected successful attach output, got %q", output)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition: %v", err)
	}
	if coalition == nil {
		t.Fatal("expected attach tool to create a live coalition")
	}
	if got := countControlToolCoalitionMembers(t, ctx, store, workspaceID, coalition.CoalitionID, agentID); got != 1 {
		t.Fatalf("expected attached coalition member, got %d", got)
	}

	events := listControlToolRuntimeEvents(t, ctx, store, workspaceID, "tension.agent.attached", tensionID)
	if len(events) != 1 {
		t.Fatalf("expected one attach runtime event, got %d", len(events))
	}
	if events[0].ActorType != "agent" || events[0].ActorID != agentID {
		t.Fatalf("expected agent actor on attach event, got actor_type=%q actor_id=%q", events[0].ActorType, events[0].ActorID)
	}
	assertControlToolPromptContextEnvelope(t, events[0].PayloadJSON, map[string]string{
		"surface":                "agent.tension.attach",
		"origin":                 "agent_tool",
		"workspace_id":           workspaceID,
		"principal_type":         "agent",
		"principal_id":           agentID,
		"tension_id":             tensionID,
		"event_kind":             "tension.agent.attached",
		"actor_type":             "agent",
		"actor_id":               agentID,
		"coalition_id":           coalition.CoalitionID,
		"coalition_agent_id":     agentID,
		"coalition_action":       "attached",
		"coalition_member_count": "1",
		"coalition_status":       "FORMING",
	})

	duplicateOutput, err := tool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`","role":"watcher"}`))
	if err != nil {
		t.Fatalf("duplicate attach via native tension tool: %v", err)
	}
	if !strings.Contains(duplicateOutput, "already attached") {
		t.Fatalf("expected duplicate attach no-op output, got %q", duplicateOutput)
	}
	if events := listControlToolRuntimeEvents(t, ctx, store, workspaceID, "tension.agent.attached", tensionID); len(events) != 1 {
		t.Fatalf("expected duplicate attach not to emit another runtime event, got %d", len(events))
	}
}

func TestTensionAttachToolRejectsUnknownAgentWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := newControlToolTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-tension-attach-unknown-agent"
		tensionID    = "tension-attach-unknown-agent"
		taskID       = "task-attach-unknown-agent"
		knownAgent   = "agent-known"
		unknownAgent = "agent-unknown"
	)
	seedTensionDetachWorkspace(t, ctx, store, workspaceID, tensionID, taskID, knownAgent)

	tool := NewTensionAttachTool(store, workspaceID, unknownAgent)
	_, err := tool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`","role":"watcher"}`))
	if err == nil {
		t.Fatal("expected unknown native tension tool agent to fail")
	}
	if got := countControlToolLiveCoalitions(t, ctx, store, workspaceID, tensionID); got != 0 {
		t.Fatalf("expected unknown-agent attach rollback to leave no live coalition, got %d", got)
	}
	if events := listControlToolRuntimeEvents(t, ctx, store, workspaceID, "tension.agent.attached", tensionID); len(events) != 0 {
		t.Fatalf("expected unknown-agent attach not to emit runtime events, got %d", len(events))
	}
}

func TestTensionDetachToolDoesNotCreatePhantomCoalition(t *testing.T) {
	t.Parallel()

	store := newControlToolTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-detach"
		tensionID   = "tension-detach"
		taskID      = "task-detach"
		agentID     = "agent-detach"
	)
	seedTensionDetachWorkspace(t, ctx, store, workspaceID, tensionID, taskID, agentID)

	tool := NewTensionDetachTool(store, workspaceID, agentID)
	_, err := tool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`"}`))
	if err == nil || !strings.Contains(err.Error(), "no live coalition") {
		t.Fatalf("expected no-live-coalition error, got %v", err)
	}

	var liveCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ? AND status IN ('FORMING','ACTIVE')`,
		workspaceID,
		tensionID,
	).Scan(&liveCount); err != nil {
		t.Fatalf("count live coalitions: %v", err)
	}
	if liveCount != 0 {
		t.Fatalf("expected tension_detach to avoid creating a phantom coalition, got %d", liveCount)
	}
}

func TestTensionDetachToolRemovesAgentFromExistingCoalition(t *testing.T) {
	t.Parallel()

	store := newControlToolTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-detach-live"
		tensionID   = "tension-detach-live"
		taskID      = "task-detach-live"
		agentID     = "agent-detach-live"
	)
	seedTensionDetachWorkspace(t, ctx, store, workspaceID, tensionID, taskID, agentID)

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "detach test coalition")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if _, err := store.AddCoalitionMemberWithHeuristicFactors(ctx, workspaceID, coalition.CoalitionID, agentID); err != nil {
		t.Fatalf("add coalition member: %v", err)
	}
	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	tool := NewTensionDetachTool(store, workspaceID, agentID)
	if _, err := tool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`"}`)); err != nil {
		t.Fatalf("detach from existing coalition: %v", err)
	}

	events := listControlToolRuntimeEvents(t, ctx, store, workspaceID, "tension.agent.detached", tensionID)
	if len(events) != 1 {
		t.Fatalf("expected one detach runtime event, got %d", len(events))
	}
	if events[0].ActorType != "agent" || events[0].ActorID != agentID {
		t.Fatalf("expected agent actor on detach event, got actor_type=%q actor_id=%q", events[0].ActorType, events[0].ActorID)
	}
	assertControlToolPromptContextEnvelope(t, events[0].PayloadJSON, map[string]string{
		"surface":                "agent.tension.detach",
		"origin":                 "agent_tool",
		"workspace_id":           workspaceID,
		"principal_type":         "agent",
		"principal_id":           agentID,
		"tension_id":             tensionID,
		"event_kind":             "tension.agent.detached",
		"actor_type":             "agent",
		"actor_id":               agentID,
		"coalition_id":           coalition.CoalitionID,
		"coalition_agent_id":     agentID,
		"coalition_action":       "detached",
		"coalition_member_count": "0",
		"coalition_status":       "DISBANDED",
	})

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition: %v", err)
	}
	if current != nil {
		t.Fatalf("expected detach of the final member to leave no live coalition, got %+v", current)
	}
}

func TestTensionDetachToolRejectsNonMemberWithoutRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := newControlToolTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-detach-non-member"
		tensionID   = "tension-detach-non-member"
		taskID      = "task-detach-non-member"
		agentID     = "agent-detach-non-member"
		otherAgent  = "agent-detach-member"
	)
	seedTensionDetachWorkspace(t, ctx, store, workspaceID, tensionID, taskID, agentID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     otherAgent,
		OwnerUserID: "developer",
		DisplayName: otherAgent,
		Role:        "generalist",
		Status:      "active",
	}); err != nil {
		t.Fatalf("register other agent: %v", err)
	}
	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "non-member detach test coalition")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if _, err := store.AddCoalitionMemberWithHeuristicFactors(ctx, workspaceID, coalition.CoalitionID, otherAgent); err != nil {
		t.Fatalf("add other coalition member: %v", err)
	}

	tool := NewTensionDetachTool(store, workspaceID, agentID)
	_, err = tool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`"}`))
	if err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Fatalf("expected non-member detach failure, got %v", err)
	}
	if events := listControlToolRuntimeEvents(t, ctx, store, workspaceID, "tension.agent.detached", tensionID); len(events) != 0 {
		t.Fatalf("expected non-member detach not to emit runtime event, got %d", len(events))
	}
	if got := countControlToolCoalitionMembers(t, ctx, store, workspaceID, coalition.CoalitionID, otherAgent); got != 1 {
		t.Fatalf("expected existing member to remain, got %d", got)
	}
}

func TestTensionDetachToolRespectsMinimumTenureWithoutRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := newControlToolTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-tension-detach-min-tenure"
		tensionID   = "tension-detach-min-tenure"
		taskID      = "task-detach-min-tenure"
		agentID     = "agent-detach-min-tenure"
	)
	seedTensionDetachWorkspace(t, ctx, store, workspaceID, tensionID, taskID, agentID)

	attachTool := NewTensionAttachTool(store, workspaceID, agentID)
	if _, err := attachTool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`","role":"watcher"}`)); err != nil {
		t.Fatalf("attach before minimum-tenure detach check: %v", err)
	}
	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition: %v", err)
	}
	if coalition == nil {
		t.Fatal("expected coalition after attach")
	}

	detachTool := NewTensionDetachTool(store, workspaceID, agentID)
	_, err = detachTool.Execute(ctx, []byte(`{"tension_id":"`+tensionID+`"}`))
	if err == nil || !strings.Contains(err.Error(), "minimum tenure") {
		t.Fatalf("expected minimum-tenure detach failure, got %v", err)
	}
	if events := listControlToolRuntimeEvents(t, ctx, store, workspaceID, "tension.agent.detached", tensionID); len(events) != 0 {
		t.Fatalf("expected minimum-tenure detach not to emit runtime event, got %d", len(events))
	}
	if got := countControlToolCoalitionMembers(t, ctx, store, workspaceID, coalition.CoalitionID, agentID); got != 1 {
		t.Fatalf("expected member to remain after minimum-tenure failure, got %d", got)
	}
}

func listControlToolRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType, tensionID string) []sqlite.RuntimeEventRecord {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "tension",
		EntityID:    tensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	return events
}

func assertControlToolPromptContextEnvelope(t *testing.T, payloadJSON string, want map[string]string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime payload: %v", err)
	}
	rawEnvelope, ok := payload["prompt_context_envelope"]
	if !ok {
		t.Fatalf("payload missing prompt_context_envelope: %s", payloadJSON)
	}
	envelope, ok := rawEnvelope.(map[string]any)
	if !ok {
		t.Fatalf("prompt_context_envelope has unexpected type %T", rawEnvelope)
	}
	for key, expected := range want {
		got, ok := envelope[key].(string)
		if !ok || got != expected {
			t.Fatalf("prompt_context_envelope[%s] = %#v, expected %q; payload=%s", key, envelope[key], expected, payloadJSON)
		}
	}
}

func countControlToolCoalitionMembers(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, coalitionID, agentID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_coalition_members WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
		workspaceID,
		coalitionID,
		agentID,
	).Scan(&count); err != nil {
		t.Fatalf("count coalition members: %v", err)
	}
	return count
}

func countControlToolLiveCoalitions(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ? AND status IN ('FORMING','ACTIVE')`,
		workspaceID,
		tensionID,
	).Scan(&count); err != nil {
		t.Fatalf("count live coalitions: %v", err)
	}
	return count
}

func seedTensionDetachWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, taskID, agentID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "control-tool-test",
	}); err != nil {
		t.Fatalf("ensure local workspace authority: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
		Role:        "generalist",
		Status:      "active",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "NORMAL",
		Title:       taskID,
	}, dag.Graph{}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task to workspace: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, segment_refs_json,
			agent_ids_json, constraint_refs_json, base_score, surface_score, evidence_count, created_at, updated_at
		) VALUES (?, ?, ?, 'gap', 'ACTIVE', 'PENDING', 'Detach Target', 'Detach target for coalition regression',
			'task_id', ?, ?, '[]', '[]', '[]', '[]', '[]', '[]', 50, 50, 1, ?, ?)`,
		tensionID,
		workspaceID,
		"task:"+workspaceID+"/"+taskID,
		taskID,
		`["`+taskID+`"]`,
		now,
		now,
	); err != nil {
		t.Fatalf("insert workspace tension: %v", err)
	}
}

func newControlToolTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	os.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")
	t.Cleanup(func() { os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") })

	dbPath := filepath.Join(t.TempDir(), "control-tools.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.ApplyMigrations(context.Background()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return store
}
