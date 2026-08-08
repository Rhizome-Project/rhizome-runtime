package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRunToolRegisterRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-cli-tool-register-missing-authority"
		toolID      = "tool-cli-missing-authority"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "CLI Tool Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	err := runToolRegister([]string{
		"--workspace-id", workspaceID,
		"--tool-id", toolID,
		"--display-name", "CLI Missing Authority Tool",
		"--owner-user-id", "developer",
		"--kind", model.ToolKindOther,
		"--status", model.ToolStatusActive,
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
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
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, toolID); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected tool row to be absent after authority reject, got %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no workspace_tool.registered events after authority reject, got %+v", events)
	}
}

func TestRunToolLifecycleStampsAuthorityMetadata(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-cli-tool-authority-metadata"
		toolID      = "tool-cli-authority-metadata"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "CLI Tool Authority Metadata",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	authority := claimCLITestWorkspaceAuthority(t, workspaceID)

	if err := runToolRegister([]string{
		"--workspace-id", workspaceID,
		"--tool-id", toolID,
		"--display-name", "CLI Authority Metadata Tool",
		"--owner-user-id", "developer",
		"--kind", model.ToolKindIntegration,
		"--status", model.ToolStatusActive,
		"--endpoint", "mcp:notion",
	}); err != nil {
		t.Fatalf("runToolRegister failed: %v", err)
	}
	if err := runToolRemove([]string{
		"--workspace-id", workspaceID,
		"--tool-id", toolID,
		"--removed-by", "developer",
	}); err != nil {
		t.Fatalf("runToolRemove failed: %v", err)
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

	registeredEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_tool.registered events: %v", err)
	}
	if len(registeredEvents) != 1 {
		t.Fatalf("expected one workspace_tool.registered event, got %d", len(registeredEvents))
	}
	if registeredEvents[0].AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected registered authority holder %q, got %q", authority.HolderAuthorityNodeID, registeredEvents[0].AuthorityHolderNodeID)
	}
	if registeredEvents[0].AuthorityTerm != authority.Term {
		t.Fatalf("expected registered authority term %d, got %d", authority.Term, registeredEvents[0].AuthorityTerm)
	}
	if got, want := registeredEvents[0].AuthorityLeaseTokenFingerprint, cliTestAuthorityLeaseTokenFingerprint(authority.LeaseToken); got != want {
		t.Fatalf("expected registered authority lease fingerprint %q, got %q", want, got)
	}
	assertCLIToolRegistryPromptContextEnvelope(t, registeredEvents[0].PayloadJSON, map[string]string{
		"context_kind":   "authority_bearing_tool_registry_write",
		"surface":        "cli.tool.register",
		"origin":         "cli_local",
		"workspace_id":   workspaceID,
		"principal_type": "operator",
		"principal_id":   "developer",
		"tool_id":        toolID,
		"owner_user_id":  "developer",
		"event_type":     "workspace_tool.registered",
	})

	removedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_tool.removed events: %v", err)
	}
	if len(removedEvents) != 1 {
		t.Fatalf("expected one workspace_tool.removed event, got %d", len(removedEvents))
	}
	if removedEvents[0].AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected removed authority holder %q, got %q", authority.HolderAuthorityNodeID, removedEvents[0].AuthorityHolderNodeID)
	}
	if removedEvents[0].AuthorityTerm != authority.Term {
		t.Fatalf("expected removed authority term %d, got %d", authority.Term, removedEvents[0].AuthorityTerm)
	}
	if got, want := removedEvents[0].AuthorityLeaseTokenFingerprint, cliTestAuthorityLeaseTokenFingerprint(authority.LeaseToken); got != want {
		t.Fatalf("expected removed authority lease fingerprint %q, got %q", want, got)
	}
	assertCLIToolRegistryPromptContextEnvelope(t, removedEvents[0].PayloadJSON, map[string]string{
		"context_kind":   "authority_bearing_tool_registry_write",
		"surface":        "cli.tool.remove",
		"origin":         "cli_local",
		"workspace_id":   workspaceID,
		"principal_type": "operator",
		"principal_id":   "developer",
		"tool_id":        toolID,
		"removed_by":     "developer",
		"event_type":     "workspace_tool.removed",
	})
}

func assertCLIToolRegistryPromptContextEnvelope(t *testing.T, payloadJSON string, expected map[string]string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode cli tool-registry payload: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected cli tool-registry prompt_context_envelope in payload %+v", payload)
	}
	for key, want := range expected {
		if got, _ := envelope[key].(string); got != want {
			t.Fatalf("expected envelope %s=%q, got %q in %+v", key, want, got, envelope)
		}
	}
}
