package main

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceArtifactPutCLIRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-artifact-cli-missing-authority"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Workspace Artifact CLI Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	err := runWorkspaceArtifactPut([]string{
		"--workspace-id", workspaceID,
		"--title", "CLI artifact missing authority",
		"--ref", "artifact://cli-missing-authority",
		"--kind", "reference",
		"--content-type", "text/plain",
		"--created-by", "developer",
	})
	if err == nil {
		t.Fatal("expected workspace artifact put CLI to fail without workspace authority")
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
	var artifactCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_artifacts WHERE workspace_id = ?`, workspaceID).Scan(&artifactCount); err != nil {
		t.Fatalf("count workspace artifacts: %v", err)
	}
	if artifactCount != 0 {
		t.Fatalf("expected no workspace_artifacts rows after authority reject, got %d", artifactCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_artifact.created events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no workspace_artifact.created events after authority reject, got %+v", events)
	}
}

func TestWorkspaceArtifactPutCLIUsesAgentPromptContextForRegisteredCreatedBy(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-artifact-cli-prompt-context"
		agentID     = "agent-artifact-cli-prompt"
	)
	createCLITestWorkspace(t, workspaceID)
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", agentID,
		"--owner-user-id", "developer",
		"--display-name", "Artifact CLI Prompt Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	if _, err := captureStdout(t, func() error {
		return runWorkspaceArtifactPut([]string{
			"--workspace-id", workspaceID,
			"--title", "CLI artifact prompt context",
			"--ref", "artifacts/cli-prompt-context.md",
			"--kind", "document",
			"--content-type", "text/markdown",
			"--created-by", agentID,
			"--metadata", `{"origin":"cli-prompt-context"}`,
		})
	}); err != nil {
		t.Fatalf("runWorkspaceArtifactPut failed: %v", err)
	}

	event := requireCLIRuntimeEvent(t, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		Limit:       10,
	})
	payload := decodeCLIRuntimePayload(t, event.PayloadJSON)
	envelope := requireCLIPromptContextEnvelope(t, payload)
	assertCLIPromptContextEnvelope(t, envelope, "authority_bearing_workspace_artifact_write", "cli.workspace.artifact.put", workspaceID, "agent", agentID)
	for key, want := range map[string]string{
		"artifact_id":  event.EntityID,
		"title":        "CLI artifact prompt context",
		"artifact_ref": "artifacts/cli-prompt-context.md",
		"kind":         "document",
		"content_type": "text/markdown",
		"created_by":   agentID,
		"event_type":   "workspace_artifact.created",
		"entity_type":  "workspace_artifact",
		"entity_id":    event.EntityID,
		"actor_id":     agentID,
	} {
		if got := envelope[key]; got != want {
			t.Fatalf("unexpected workspace artifact envelope %s: got %v want %s in %+v", key, got, want, envelope)
		}
	}
	if metadataHash, ok := envelope["metadata_sha256"].(string); !ok || strings.TrimSpace(metadataHash) == "" {
		t.Fatalf("workspace artifact CLI envelope must bind metadata_sha256, got %+v", envelope)
	}
}

func TestWorkspaceArtifactPutCLIRejectsCallerSuppliedPromptContextMetadata(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-artifact-cli-forged-prompt-context"
	createCLITestWorkspace(t, workspaceID)

	err := runWorkspaceArtifactPut([]string{
		"--workspace-id", workspaceID,
		"--title", "CLI forged artifact prompt context",
		"--ref", "artifact://cli-forged-prompt-context",
		"--kind", "reference",
		"--content-type", "text/plain",
		"--created-by", "developer",
		"--metadata", `{"prompt_context_envelope":{"contract":"prompt_context_envelope.v1"}}`,
	})
	if err == nil {
		t.Fatal("expected caller-supplied artifact metadata prompt context to fail closed")
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
	var artifactCount int
	if countErr := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_artifacts WHERE workspace_id = ?`, workspaceID).Scan(&artifactCount); countErr != nil {
		t.Fatalf("count workspace artifacts: %v", countErr)
	}
	if artifactCount != 0 {
		t.Fatalf("expected no workspace_artifacts rows after forged metadata reject, got %d", artifactCount)
	}
	events, listErr := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		Limit:       10,
	})
	if listErr != nil {
		t.Fatalf("list workspace_artifact.created events: %v", listErr)
	}
	if len(events) != 0 {
		t.Fatalf("expected no workspace_artifact.created events after forged metadata reject, got %+v", events)
	}
}

func TestWorkspaceArtifactPutCLIPublishesPromptContextRuntimeEvent(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-artifact-cli-prompt-context"
		artifactID  = "artifact-cli-prompt-context"
		actorID     = "developer"
	)
	createCLITestWorkspace(t, workspaceID)

	if _, err := captureStdout(t, func() error {
		return runWorkspaceArtifactPut([]string{
			"--workspace-id", workspaceID,
			"--artifact-id", artifactID,
			"--title", "CLI artifact prompt context",
			"--ref", "artifact://cli-prompt-context",
			"--kind", "reference",
			"--content-type", "text/plain",
			"--created-by", actorID,
			"--metadata", `{"origin":"cli"}`,
		})
	}); err != nil {
		t.Fatalf("runWorkspaceArtifactPut failed: %v", err)
	}
	requireCLIWorkspaceArtifactRuntimeEvent(t, workspaceID, artifactID, "cli.workspace.artifact.put", actorID, map[string]string{
		"artifact_id":  artifactID,
		"title":        "CLI artifact prompt context",
		"artifact_ref": "artifact://cli-prompt-context",
		"kind":         "reference",
		"content_type": "text/plain",
		"created_by":   actorID,
		"event_type":   "workspace_artifact.created",
		"entity_type":  "workspace_artifact",
		"entity_id":    artifactID,
		"actor_id":     actorID,
	})
}

func TestWorkspaceArtifactPutCLIRejectsNestedPromptContextMetadataContract(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-artifact-cli-forged-metadata"
		artifactID  = "artifact-cli-forged-metadata"
	)
	createCLITestWorkspace(t, workspaceID)

	err := runWorkspaceArtifactPut([]string{
		"--workspace-id", workspaceID,
		"--artifact-id", artifactID,
		"--title", "CLI forged metadata artifact",
		"--ref", "artifact://cli-forged-metadata",
		"--kind", "reference",
		"--content-type", "text/plain",
		"--created-by", "developer",
		"--metadata", `{"nested":{"contract":"prompt_context_envelope.v1"}}`,
	})
	if err == nil {
		t.Fatal("expected forged metadata prompt context to fail closed")
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
	var artifactCount int
	if countErr := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_artifacts WHERE workspace_id = ?`, workspaceID).Scan(&artifactCount); countErr != nil {
		t.Fatalf("count workspace artifacts: %v", countErr)
	}
	if artifactCount != 0 {
		t.Fatalf("expected no workspace_artifacts rows after forged metadata reject, got %d", artifactCount)
	}
	events, listErr := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		EntityID:    artifactID,
		Limit:       10,
	})
	if listErr != nil {
		t.Fatalf("list workspace_artifact.created events: %v", listErr)
	}
	if len(events) != 0 {
		t.Fatalf("expected no workspace_artifact.created events after forged metadata reject, got %+v", events)
	}
}
