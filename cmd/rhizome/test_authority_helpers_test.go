package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func createCLITestWorkspace(t *testing.T, workspaceID string) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("RHIZOME_WORKSPACE_PASSWORD")) == "" {
		t.Setenv("RHIZOME_WORKSPACE_PASSWORD", "cli-test-workspace-password")
	}

	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "CLI Test Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
}

func claimCLITestWorkspaceAuthority(t *testing.T, workspaceID string) sqlite.WorkspaceAuthorityRecord {
	t.Helper()

	dbPath := strings.TrimSpace(os.Getenv("RHIZOME_DB"))
	if dbPath == "" {
		t.Fatal("RHIZOME_DB is not set")
	}
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open cli test store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	now := time.Now().UTC()
	referenceAt := now.Format(time.RFC3339Nano)
	registeredAt := strings.TrimSpace(node.RegisteredAt)
	if registeredAt == "" {
		registeredAt = referenceAt
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status
`, node.AuthorityNodeID, node.NodeKind, node.HostLabel, node.BootInstanceID, registeredAt, referenceAt, string(sqlite.RuntimeNodeStatusOnline)); err != nil {
		t.Fatalf("seed runtime authority node for %s: %v", workspaceID, err)
	}
	if existing, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace"); err == nil {
		if existing.Status == sqlite.WorkspaceAuthorityStatusActive && existing.HolderAuthorityNodeID == node.AuthorityNodeID {
			return existing
		}
		term := existing.Term
		if existing.HolderAuthorityNodeID != node.AuthorityNodeID || existing.Status != sqlite.WorkspaceAuthorityStatusActive {
			term++
		}
		if term <= 0 {
			term = 1
		}
		commitWatermark := existing.CommitWatermark
		if commitWatermark <= 0 {
			commitWatermark = 1
		}
		appliedWatermark := existing.AppliedWatermark
		if appliedWatermark <= 0 {
			appliedWatermark = 1
		}
		leaseToken := strings.TrimSpace(existing.LeaseToken)
		if leaseToken == "" || existing.HolderAuthorityNodeID != node.AuthorityNodeID || existing.Status != sqlite.WorkspaceAuthorityStatusActive {
			leaseToken = "lease-cli-tests-" + workspaceID
		}
		if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_authority(
	workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at,
	commit_watermark, applied_watermark, status, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope) DO UPDATE SET
	holder_authority_node_id = excluded.holder_authority_node_id,
	lease_token = excluded.lease_token,
	term = excluded.term,
	lease_expires_at = excluded.lease_expires_at,
	commit_watermark = excluded.commit_watermark,
	applied_watermark = excluded.applied_watermark,
	status = excluded.status,
	updated_at = excluded.updated_at
`, workspaceID, "workspace", node.AuthorityNodeID, leaseToken, term, now.Add(time.Hour).Format(time.RFC3339Nano), commitWatermark, appliedWatermark, string(sqlite.WorkspaceAuthorityStatusActive), referenceAt); err != nil {
			t.Fatalf("seed workspace authority for %s: %v", workspaceID, err)
		}
		record, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
		if err != nil {
			t.Fatalf("reload seeded workspace authority for %s: %v", workspaceID, err)
		}
		return record
	}

	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_authority(
	workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at,
	commit_watermark, applied_watermark, status, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope) DO UPDATE SET
	holder_authority_node_id = excluded.holder_authority_node_id,
	lease_token = excluded.lease_token,
	term = excluded.term,
	lease_expires_at = excluded.lease_expires_at,
	commit_watermark = excluded.commit_watermark,
	applied_watermark = excluded.applied_watermark,
	status = excluded.status,
	updated_at = excluded.updated_at
`, workspaceID, "workspace", node.AuthorityNodeID, "lease-cli-tests-"+workspaceID, int64(1), now.Add(time.Hour).Format(time.RFC3339Nano), int64(1), int64(1), string(sqlite.WorkspaceAuthorityStatusActive), referenceAt); err != nil {
		t.Fatalf("seed workspace authority for %s: %v", workspaceID, err)
	}
	record, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("reload seeded workspace authority for %s: %v", workspaceID, err)
	}
	return record
}

func requireCLITaskRuntimeEvent(t *testing.T, workspaceID, taskID, eventType, surface string) sqlite.RuntimeEventRecord {
	t.Helper()

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list task runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one %s runtime event for %s/%s, got %+v", eventType, workspaceID, taskID, events)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode task runtime payload: %v; payload=%q", err, events[0].PayloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload, got %+v", payload)
	}
	if got := envelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected envelope contract: %v", got)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_task_write" {
		t.Fatalf("unexpected task context kind: %v", got)
	}
	if got := envelope["surface"]; got != surface {
		t.Fatalf("unexpected task surface: got %v want %s", got, surface)
	}
	if got := envelope["origin"]; got != "cli_local" {
		t.Fatalf("unexpected task origin: got %v want cli_local", got)
	}
	if got := envelope["workspace_id"]; got != workspaceID {
		t.Fatalf("unexpected task workspace_id: got %v want %s", got, workspaceID)
	}
	if got := envelope["principal_type"]; got != "operator" {
		t.Fatalf("unexpected task principal_type: got %v want operator", got)
	}
	principalID, ok := envelope["principal_id"].(string)
	if !ok || strings.TrimSpace(principalID) == "" {
		t.Fatalf("expected non-empty principal_id in prompt context envelope")
	}
	return events[0]
}

func requireCLIAgentSessionRuntimeEvent(t *testing.T, workspaceID, sessionID, eventType, surface, agentID string) sqlite.RuntimeEventRecord {
	t.Helper()

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one %s runtime event for %s/%s, got %+v", eventType, workspaceID, sessionID, events)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode session runtime payload: %v; payload=%q", err, events[0].PayloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in session payload, got %+v", payload)
	}
	if got := envelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected session envelope contract: %v", got)
	}
	if got := envelope["context_kind"]; got != "authority_bearing_session_write" {
		t.Fatalf("unexpected session context kind: %v", got)
	}
	if got := envelope["surface"]; got != surface {
		t.Fatalf("unexpected session surface: got %v want %s", got, surface)
	}
	if got := envelope["origin"]; got != "cli_local" {
		t.Fatalf("unexpected session origin: got %v want cli_local", got)
	}
	if got := envelope["workspace_id"]; got != workspaceID {
		t.Fatalf("unexpected session workspace_id: got %v want %s", got, workspaceID)
	}
	if got := envelope["principal_type"]; got != "agent" {
		t.Fatalf("unexpected session principal_type: got %v want agent", got)
	}
	if got := envelope["principal_id"]; got != agentID {
		t.Fatalf("unexpected session principal_id: got %v want %s", got, agentID)
	}
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("session envelope must not claim daemon convergence: %+v", envelope)
	}
	return events[0]
}

func requireCLIWorkspaceDocRuntimeEvent(t *testing.T, workspaceID, docKey, eventType, surface, principalID string, extra map[string]string) sqlite.RuntimeEventRecord {
	t.Helper()

	event := requireCLIRuntimeEvent(t, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "workspace_doc",
		EntityID:    docKey,
		Limit:       10,
	})
	payload := decodeCLIRuntimePayload(t, event.PayloadJSON)
	envelope := requireCLIPromptContextEnvelope(t, payload)
	assertCLIPromptContextEnvelope(t, envelope, "authority_bearing_workspace_doc_write", surface, workspaceID, "operator", principalID)
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("workspace doc envelope must not claim daemon convergence: %+v", envelope)
	}
	for key, want := range extra {
		if got := envelope[key]; got != want {
			t.Fatalf("unexpected workspace doc envelope %s: got %v want %s in %+v", key, got, want, envelope)
		}
	}
	return event
}

func requireCLIAgentUpdateRuntimeEvent(t *testing.T, workspaceID, agentID, surface string, extra map[string]string) sqlite.RuntimeEventRecord {
	t.Helper()

	event := requireCLIRuntimeEvent(t, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		AgentID:     agentID,
		Limit:       10,
	})
	payload := decodeCLIRuntimePayload(t, event.PayloadJSON)
	envelope := requireCLIPromptContextEnvelope(t, payload)
	assertCLIPromptContextEnvelope(t, envelope, "authority_bearing_agent_update_write", surface, workspaceID, "agent", agentID)
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("agent update envelope must not claim daemon convergence: %+v", envelope)
	}
	for key, want := range extra {
		if got := envelope[key]; got != want {
			t.Fatalf("unexpected agent update envelope %s: got %v want %s in %+v", key, got, want, envelope)
		}
	}
	return event
}

func requireCLIWorkspaceArtifactRuntimeEvent(t *testing.T, workspaceID, artifactID, surface, principalID string, extra map[string]string) sqlite.RuntimeEventRecord {
	t.Helper()

	event := requireCLIRuntimeEvent(t, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		EntityID:    artifactID,
		Limit:       10,
	})
	payload := decodeCLIRuntimePayload(t, event.PayloadJSON)
	envelope := requireCLIPromptContextEnvelope(t, payload)
	assertCLIPromptContextEnvelope(t, envelope, "authority_bearing_workspace_artifact_write", surface, workspaceID, "operator", principalID)
	if got := envelope["daemon_prompt_compiler_convergence"]; got != "not_claimed" {
		t.Fatalf("workspace artifact envelope must not claim daemon convergence: %+v", envelope)
	}
	if metadataHash, ok := envelope["metadata_sha256"].(string); !ok || strings.TrimSpace(metadataHash) == "" {
		t.Fatalf("workspace artifact envelope must bind metadata_sha256: %+v", envelope)
	}
	for key, want := range extra {
		if got := envelope[key]; got != want {
			t.Fatalf("unexpected workspace artifact envelope %s: got %v want %s in %+v", key, got, want, envelope)
		}
	}
	return event
}

func requireCLIRuntimeEvent(t *testing.T, filter sqlite.RuntimeEventFilter) sqlite.RuntimeEventRecord {
	t.Helper()

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one runtime event for filter %+v, got %+v", filter, events)
	}
	return events[0]
}

func decodeCLIRuntimePayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode CLI runtime payload: %v; payload=%q", err, payloadJSON)
	}
	return payload
}

func requireCLIPromptContextEnvelope(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in payload, got %+v", payload)
	}
	return envelope
}

func assertCLIPromptContextEnvelope(t *testing.T, envelope map[string]any, contextKind, surface, workspaceID, principalType, principalID string) {
	t.Helper()
	if got := envelope["contract"]; got != "prompt_context_envelope.v1" {
		t.Fatalf("unexpected envelope contract: %v", got)
	}
	if got := envelope["context_kind"]; got != contextKind {
		t.Fatalf("unexpected envelope context kind: got %v want %s", got, contextKind)
	}
	if got := envelope["surface"]; got != surface {
		t.Fatalf("unexpected envelope surface: got %v want %s", got, surface)
	}
	if got := envelope["origin"]; got != "cli_local" {
		t.Fatalf("unexpected envelope origin: got %v want cli_local", got)
	}
	if got := envelope["workspace_id"]; got != workspaceID {
		t.Fatalf("unexpected envelope workspace_id: got %v want %s", got, workspaceID)
	}
	if got := envelope["principal_type"]; got != principalType {
		t.Fatalf("unexpected envelope principal_type: got %v want %s", got, principalType)
	}
	if got := envelope["principal_id"]; got != principalID {
		t.Fatalf("unexpected envelope principal_id: got %v want %s", got, principalID)
	}
}
