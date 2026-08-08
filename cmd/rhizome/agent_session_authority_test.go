package main

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentSessionStartCLIRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-agent-session-start-cli-missing-authority"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Agent Session Start CLI Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", "agent-session-cli",
		"--owner-user-id", "developer",
		"--display-name", "Agent Session CLI",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	err := runAgentSessionEvent([]string{
		"--workspace-id", workspaceID,
		"--session-id", "sess-agent-session-cli",
		"--agent-id", "agent-session-cli",
		"--summary", "CLI session start should fail closed without authority",
	}, "start")
	if err == nil {
		t.Fatal("expected agent session start CLI to fail without workspace authority")
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
	var sessionCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE workspace_id = ?`, workspaceID).Scan(&sessionCount); err != nil {
		t.Fatalf("count agent sessions: %v", err)
	}
	if sessionCount != 0 {
		t.Fatalf("expected no agent_sessions rows after authority reject, got %d", sessionCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "session.start",
		EntityType:  "agent_session",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session.start events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no session.start events after authority reject, got %+v", events)
	}
}
