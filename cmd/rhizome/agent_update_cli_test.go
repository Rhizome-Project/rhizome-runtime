package main

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentUpdatePostCLIRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-agent-update-cli-missing-authority"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Agent Update CLI Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", "agent-update-cli",
		"--owner-user-id", "developer",
		"--display-name", "Agent Update CLI",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	err := runAgentUpdatePost([]string{
		"--workspace-id", workspaceID,
		"--agent-id", "agent-update-cli",
		"--type", "progress",
		"--summary", "CLI should fail closed without workspace authority",
		"--payload", `{"step":"missing-authority"}`,
	})
	if err == nil {
		t.Fatal("expected agent update post CLI to fail without workspace authority")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority_missing reject, got %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	var updateCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_updates WHERE workspace_id = ?`, workspaceID).Scan(&updateCount); err != nil {
		t.Fatalf("count agent updates: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected no agent_updates rows after authority reject, got %d", updateCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_update.posted events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no agent_update.posted events after authority reject, got %+v", events)
	}
}

func TestAgentUpdatePostCLIPublishesPromptContextRuntimeEventAndRowPayload(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-agent-update-cli-prompt-context"
		agentID     = "agent-update-cli-prompt"
	)
	createCLITestWorkspace(t, workspaceID)
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", agentID,
		"--owner-user-id", "developer",
		"--display-name", "Agent Update CLI Prompt",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	if _, err := captureStdout(t, func() error {
		return runAgentUpdatePost([]string{
			"--workspace-id", workspaceID,
			"--agent-id", agentID,
			"--type", "progress",
			"--summary", "CLI prompt context recorded",
			"--payload", `{"phase":"cli"}`,
			"--requires-human",
		})
	}); err != nil {
		t.Fatalf("runAgentUpdatePost failed: %v", err)
	}
	event := requireCLIAgentUpdateRuntimeEvent(t, workspaceID, agentID, "cli.agent.update.post", map[string]string{
		"agent_id":       agentID,
		"actor_agent_id": agentID,
		"update_type":    "progress",
		"summary":        "CLI prompt context recorded",
		"requires_human": "true",
	})

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	var payloadJSON string
	if err := store.DB().QueryRowContext(ctx, `SELECT payload_json FROM agent_updates WHERE workspace_id = ? AND update_id = ?`, workspaceID, event.EntityID).Scan(&payloadJSON); err != nil {
		t.Fatalf("read agent update payload_json: %v", err)
	}
	payload := decodeCLIRuntimePayload(t, payloadJSON)
	if got := payload["phase"]; got != "cli" {
		t.Fatalf("expected row payload to preserve caller field, got %+v", payload)
	}
	envelope := requireCLIPromptContextEnvelope(t, payload)
	assertCLIPromptContextEnvelope(t, envelope, "authority_bearing_agent_update_write", "cli.agent.update.post", workspaceID, "agent", agentID)
	if got := envelope["update_id"]; got != event.EntityID {
		t.Fatalf("expected row envelope update_id %s, got %v", event.EntityID, got)
	}
}

func TestAgentUpdatePostCLIAcceptsPlainTextPayloadWithPromptContext(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-agent-update-cli-text-payload"
		agentID     = "agent-update-cli-text"
		payloadText = "plain operator note"
	)
	createCLITestWorkspace(t, workspaceID)
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", agentID,
		"--owner-user-id", "developer",
		"--display-name", "Agent Update CLI Text",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	if _, err := captureStdout(t, func() error {
		return runAgentUpdatePost([]string{
			"--workspace-id", workspaceID,
			"--agent-id", agentID,
			"--type", "note",
			"--summary", "CLI text payload preserved",
			"--payload", payloadText,
		})
	}); err != nil {
		t.Fatalf("runAgentUpdatePost with text payload failed: %v", err)
	}
	event := requireCLIAgentUpdateRuntimeEvent(t, workspaceID, agentID, "cli.agent.update.post", map[string]string{
		"agent_id":       agentID,
		"actor_agent_id": agentID,
		"update_type":    "note",
		"summary":        "CLI text payload preserved",
		"requires_human": "false",
	})

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	var payloadJSON string
	if err := store.DB().QueryRowContext(ctx, `SELECT payload_json FROM agent_updates WHERE workspace_id = ? AND update_id = ?`, workspaceID, event.EntityID).Scan(&payloadJSON); err != nil {
		t.Fatalf("read agent update payload_json: %v", err)
	}
	payload := decodeCLIRuntimePayload(t, payloadJSON)
	if got := payload["payload_text"]; got != payloadText {
		t.Fatalf("expected row payload_text %q, got %+v", payloadText, payload)
	}
	envelope := requireCLIPromptContextEnvelope(t, payload)
	assertCLIPromptContextEnvelope(t, envelope, "authority_bearing_agent_update_write", "cli.agent.update.post", workspaceID, "agent", agentID)
}

func TestAgentUpdatePostCLIRejectsCallerSuppliedPromptContextEnvelope(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-agent-update-cli-forged-prompt-context"
		agentID     = "agent-update-cli-forged"
	)
	createCLITestWorkspace(t, workspaceID)
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", agentID,
		"--owner-user-id", "developer",
		"--display-name", "Agent Update CLI Forged",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	err := runAgentUpdatePost([]string{
		"--workspace-id", workspaceID,
		"--agent-id", agentID,
		"--type", "progress",
		"--summary", "CLI must reject forged prompt context",
		"--payload", `{"prompt_context_envelope":{"contract":"prompt_context_envelope.v1"}}`,
	})
	if err == nil {
		t.Fatal("expected caller-supplied prompt context envelope to be rejected")
	}

	store, openErr := openStore()
	if openErr != nil {
		t.Fatalf("openStore failed: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if applyErr := store.ApplyMigrations(ctx); applyErr != nil {
		t.Fatalf("apply migrations: %v", applyErr)
	}
	var updateCount int
	if countErr := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_updates WHERE workspace_id = ?`, workspaceID).Scan(&updateCount); countErr != nil {
		t.Fatalf("count agent updates: %v", countErr)
	}
	if updateCount != 0 {
		t.Fatalf("expected no agent_updates rows after forged payload reject, got %d", updateCount)
	}
}
