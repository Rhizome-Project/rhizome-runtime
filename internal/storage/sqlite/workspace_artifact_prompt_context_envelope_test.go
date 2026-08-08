package sqlite_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordWorkspaceArtifactWithEffectsCarriesPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-artifact-prompt-context"
		agentID     = "agent-artifact-prompt"
		taskID      = "task-artifact-prompt"
		artifactID  = "artifact-prompt-context"
		metadata    = `{"origin":"prompt-context-test"}`
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact Prompt Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)

	_, event, invalidationEvents, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:            artifactID,
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		Title:                 "Artifact Prompt Context Brief",
		ArtifactRef:           "artifacts/prompt-context.md",
		Kind:                  "document",
		ContentType:           "text/markdown",
		CreatedBy:             agentID,
		MetadataJSON:          metadata,
		PromptContextEnvelope: sqlite.BuildWorkspaceArtifactPromptContextEnvelope("workspace.artifact.write", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "workspace.artifact.write",
	})
	if err != nil {
		t.Fatalf("record workspace artifact with prompt context: %v", err)
	}
	if len(invalidationEvents) != 0 {
		for _, invalidation := range invalidationEvents {
			assertNoRuntimePromptContextEnvelope(t, invalidation.PayloadJSON)
		}
	}

	persisted := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		EntityID:    artifactID,
		TaskID:      taskID,
		Limit:       10,
	})
	if event.EventID != persisted.EventID || event.IngestSeq != persisted.IngestSeq || event.PayloadJSON != persisted.PayloadJSON {
		t.Fatalf("expected returned artifact event to match persisted row, returned=%+v persisted=%+v", event, persisted)
	}
	assertWorkspaceArtifactPromptContextEnvelope(t, persisted, map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_artifact_write",
		"surface":                            "workspace.artifact.write",
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     "agent",
		"principal_id":                       agentID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
		"artifact_id":                        artifactID,
		"task_id":                            taskID,
		"title":                              "Artifact Prompt Context Brief",
		"artifact_ref":                       "artifacts/prompt-context.md",
		"kind":                               "document",
		"content_type":                       "text/markdown",
		"created_by":                         agentID,
		"event_type":                         "workspace_artifact.created",
		"entity_type":                        "workspace_artifact",
		"entity_id":                          artifactID,
		"actor_id":                           agentID,
		"metadata_sha256":                    testSHA256Hex(metadata),
	})
}

func TestRecordWorkspaceArtifactWithEffectsRejectsForgedPromptContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-artifact-forged-prompt-context"
		artifactID  = "artifact-forged-prompt-context"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact Forged Prompt Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	_, _, _, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:            artifactID,
		WorkspaceID:           workspaceID,
		Title:                 "Forged Artifact Prompt Context",
		ArtifactRef:           "artifact://forged-prompt-context",
		Kind:                  "reference",
		ContentType:           "text/plain",
		CreatedBy:             "agent-artifact",
		PromptContextEnvelope: sqlite.BuildWorkspaceArtifactPromptContextEnvelope("workspace.artifact.write", "server_rpc", workspaceID, "agent", "agent-other"),
		PromptContextSurface:  "workspace.artifact.write",
	})
	if err == nil {
		t.Fatal("expected forged artifact prompt context to fail closed")
	}
	assertWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, artifactID, "workspace_artifact.created")
}

func TestRecordWorkspaceArtifactWithEffectsRejectsMetadataPromptContextMarkers(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-artifact-metadata-forged-prompt-context"
		artifactID  = "artifact-metadata-forged-prompt-context"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact Metadata Forged Prompt Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	_, _, _, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   artifactID,
		WorkspaceID:  workspaceID,
		Title:        "Metadata Forged Artifact Prompt Context",
		ArtifactRef:  "artifact://metadata-forged-prompt-context",
		Kind:         "reference",
		ContentType:  "text/plain",
		CreatedBy:    "agent-artifact",
		MetadataJSON: `{"prompt_context_envelope":{"contract":"prompt_context_envelope.v1"}}`,
	})
	if err == nil {
		t.Fatal("expected metadata prompt context marker to fail closed")
	}
	assertWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, artifactID, "workspace_artifact.created")
}

func TestRecordWorkspaceArtifactWithEffectsRejectsMalformedPromptContextMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-artifact-malformed-prompt-context-metadata"
		artifactID  = "artifact-malformed-prompt-context-metadata"
		createdBy   = "agent-artifact"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact Malformed Prompt Context Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	_, _, _, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:            artifactID,
		WorkspaceID:           workspaceID,
		Title:                 "Malformed Metadata Artifact Prompt Context",
		ArtifactRef:           "artifact://malformed-prompt-context-metadata",
		Kind:                  "reference",
		ContentType:           "text/plain",
		CreatedBy:             createdBy,
		MetadataJSON:          `{"broken":`,
		PromptContextEnvelope: sqlite.BuildWorkspaceArtifactPromptContextEnvelope("workspace.artifact.write", "server_rpc", workspaceID, "agent", createdBy),
		PromptContextSurface:  "workspace.artifact.write",
	})
	if err == nil {
		t.Fatal("expected malformed metadata with prompt context to fail closed")
	}
	assertWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, artifactID, "workspace_artifact.created")
}

func assertWorkspaceArtifactPromptContextEnvelope(t *testing.T, event sqlite.RuntimeEventRecord, expected map[string]string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode workspace artifact runtime payload: %v; payload=%q", err, event.PayloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected workspace artifact prompt_context_envelope in payload, got %+v", payload)
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("workspace artifact envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("workspace artifact envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}

func assertRuntimeEventPayloadHasNoPromptContextEnvelope(t *testing.T, event sqlite.RuntimeEventRecord) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime payload for prompt context absence check: %v; payload=%q", err, event.PayloadJSON)
	}
	if _, ok := payload["prompt_context_envelope"]; ok {
		t.Fatalf("expected no prompt_context_envelope in %s payload, got %+v", event.EventType, payload)
	}
}

func testSHA256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
