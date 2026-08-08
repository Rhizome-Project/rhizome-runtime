package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceArtifactWriteAndList(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-artifact-rpc", "agent", "agent-artifact")

	const workspaceID = "ws-artifact-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-artifact",
		OwnerUserID: "developer",
		DisplayName: "Artifact Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	writeRaw, err := json.Marshal(workspaceArtifactWriteParams{
		WorkspaceID:  workspaceID,
		Title:        "Primary brief",
		ArtifactRef:  "doc:deliverable.brief",
		Kind:         "workspace_doc",
		ContentType:  "text/markdown",
		CreatedBy:    "agent-artifact",
		MetadataJSON: `{"doc_key":"deliverable.brief","run_id":"run-1"}`,
	})
	if err != nil {
		t.Fatalf("marshal write params: %v", err)
	}
	writeRespAny, rpcErr := h.workspaceArtifactWrite(ctx, writeRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceArtifactWrite rpc error: %+v", rpcErr)
	}
	writeResp, ok := writeRespAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected write response type %T", writeRespAny)
	}
	record, ok := writeResp["artifact"].(sqlite.WorkspaceArtifactRecord)
	if !ok {
		t.Fatalf("unexpected artifact payload %#v", writeResp["artifact"])
	}
	if writeResp["status"] != "RECORDED" {
		t.Fatalf("expected RECORDED status, got %+v", writeResp)
	}
	if record.WorkspaceID != workspaceID || record.ArtifactRef != "doc:deliverable.brief" || record.Kind != "workspace_doc" {
		t.Fatalf("unexpected artifact record %+v", record)
	}
	liveEvent := nextEvent(t, ch)
	persisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		EntityID:    record.ArtifactID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, persisted, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), persisted.PayloadJSON)
	assertServerWorkspaceArtifactRuntimePromptContext(t, persisted, "workspace.artifact.write", workspaceID, "agent", "agent-artifact", map[string]string{
		"artifact_id":  record.ArtifactID,
		"title":        "Primary brief",
		"artifact_ref": "doc:deliverable.brief",
		"kind":         "workspace_doc",
		"content_type": "text/markdown",
		"created_by":   "agent-artifact",
		"event_type":   "workspace_artifact.created",
		"entity_type":  "workspace_artifact",
		"entity_id":    record.ArtifactID,
		"actor_id":     "agent-artifact",
	})

	listRaw, err := json.Marshal(workspaceArtifactListParams{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listRespAny, rpcErr := h.workspaceArtifactList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceArtifactList rpc error: %+v", rpcErr)
	}
	listResp, ok := listRespAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list response type %T", listRespAny)
	}
	items, ok := listResp["items"].([]sqlite.WorkspaceArtifactRecord)
	if !ok {
		t.Fatalf("unexpected items payload %#v", listResp["items"])
	}
	if len(items) != 1 || items[0].ArtifactID != record.ArtifactID {
		t.Fatalf("unexpected listed artifacts %+v", items)
	}
	if listResp["count"] != 1 {
		t.Fatalf("expected count=1, got %+v", listResp)
	}
}

func TestWorkspaceArtifactWritePublishesRefChangeMemoryInvalidationEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-artifact-invalidation-live", "agent", "agent-artifact")

	const (
		workspaceID = "ws-artifact-invalidation-live"
		agentID     = "agent-artifact"
		artifactRef = "doc:deliverable.brief"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact Invalidation Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	initialArtifact, err := store.RecordWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		Title:       "Primary brief",
		ArtifactRef: artifactRef,
		Kind:        "workspace_doc",
		ContentType: "text/markdown",
		CreatedBy:   agentID,
	})
	if err != nil {
		t.Fatalf("seed artifact: %v", err)
	}
	initialToken := initialArtifact.ArtifactID + "@" + initialArtifact.CreatedAt
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-artifact-invalidation-live",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "artifact:deliverable",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "artifact_ref", RefID: artifactRef, VersionToken: initialToken, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed residency: %v", err)
	}

	seenArtifactEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		Limit:       10,
	})
	seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	writeRaw, err := json.Marshal(workspaceArtifactWriteParams{
		WorkspaceID:  workspaceID,
		Title:        "Primary brief revised",
		ArtifactRef:  artifactRef,
		Kind:         "workspace_doc",
		ContentType:  "text/markdown",
		CreatedBy:    agentID,
		MetadataJSON: `{"doc_key":"deliverable.brief","run_id":"run-2"}`,
	})
	if err != nil {
		t.Fatalf("marshal write params: %v", err)
	}
	writeRespAny, rpcErr := h.workspaceArtifactWrite(ctx, writeRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceArtifactWrite rpc error: %+v", rpcErr)
	}
	writeResp, ok := writeRespAny.(map[string]any)
	if !ok || writeResp["status"] != "RECORDED" {
		t.Fatalf("unexpected write response %+v", writeRespAny)
	}

	artifactLive := nextEvent(t, ch)
	invalidationLive := nextEvent(t, ch)
	artifactPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		Limit:       10,
	}, seenArtifactEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)

	assertLiveEventMirrorsRuntimeEvent(t, artifactLive, artifactPersisted, "")
	assertLiveEventMirrorsRuntimeEvent(t, invalidationLive, invalidationPersisted, "")
	assertServerWorkspaceArtifactRuntimePromptContext(t, artifactPersisted, "workspace.artifact.write", workspaceID, "agent", agentID, map[string]string{
		"artifact_id":  writeResp["artifact"].(sqlite.WorkspaceArtifactRecord).ArtifactID,
		"title":        "Primary brief revised",
		"artifact_ref": artifactRef,
		"kind":         "workspace_doc",
		"content_type": "text/markdown",
		"created_by":   agentID,
		"event_type":   "workspace_artifact.created",
		"entity_type":  "workspace_artifact",
		"entity_id":    writeResp["artifact"].(sqlite.WorkspaceArtifactRecord).ArtifactID,
		"actor_id":     agentID,
	})
	assertRuntimeEventPayloadHasNoPromptContextEnvelope(t, invalidationPersisted)
	if artifactPersisted.IngestSeq >= invalidationPersisted.IngestSeq {
		t.Fatalf("expected artifact event before invalidation enqueue, got artifact=%+v invalidation=%+v", artifactPersisted, invalidationPersisted)
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "workspace_artifact.created" {
		t.Fatalf("expected artifact-triggered invalidation payload, got %+v", payload)
	}
}

func TestWorkspaceArtifactWriteRejectsCreatedByMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-artifact-actor-mismatch", "agent", "agent-artifact")

	const workspaceID = "ws-artifact-actor-mismatch"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact Actor Mismatch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceArtifactWriteParams{
		WorkspaceID: workspaceID,
		Title:       "Actor mismatch artifact",
		ArtifactRef: "artifact://actor-mismatch",
		Kind:        "reference",
		ContentType: "text/plain",
		CreatedBy:   "agent-other",
	})
	if err != nil {
		t.Fatalf("marshal write params: %v", err)
	}

	result, rpcErr := h.workspaceArtifactWrite(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected created_by mismatch to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on created_by mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match created_by" {
		t.Fatalf("unexpected created_by mismatch error %+v", rpcErr)
	}
}

func TestWorkspaceArtifactWriteRejectsCallerSuppliedPromptContextMetadata(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-artifact-rpc-forged-metadata", "agent", "agent-artifact")

	const workspaceID = "ws-artifact-rpc-forged-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Artifact RPC Forged Metadata",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-artifact",
		OwnerUserID: "developer",
		DisplayName: "Artifact Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(workspaceArtifactWriteParams{
		ArtifactID:   "artifact-rpc-forged-metadata",
		WorkspaceID:  workspaceID,
		Title:        "Forged metadata artifact",
		ArtifactRef:  "artifact://rpc-forged-metadata",
		Kind:         "reference",
		ContentType:  "text/plain",
		CreatedBy:    "agent-artifact",
		MetadataJSON: `{"nested":{"prompt_context_envelope":{"contract":"prompt_context_envelope.v1"}}}`,
	})
	if err != nil {
		t.Fatalf("marshal write params: %v", err)
	}

	result, rpcErr := h.workspaceArtifactWrite(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected forged metadata prompt context to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on forged metadata reject, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for forged metadata prompt context, got %+v", rpcErr)
	}
	assertServerWorkspaceArtifactCount(t, ctx, store, workspaceID, 0)
	assertNoServerWorkspaceArtifactRuntimeEvents(t, ctx, store, workspaceID, "artifact-rpc-forged-metadata", "workspace_artifact.created")
}

func assertServerWorkspaceArtifactRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantPrincipalType, wantPrincipalID string, extra map[string]string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected workspace artifact prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_artifact_write",
		"surface":                            wantSurface,
		"origin":                             "server_rpc",
		"workspace_id":                       wantWorkspaceID,
		"principal_type":                     wantPrincipalType,
		"principal_id":                       wantPrincipalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("workspace artifact prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("workspace artifact prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
	if metadataHash, ok := envelope["metadata_sha256"].(string); !ok || strings.TrimSpace(metadataHash) == "" {
		t.Fatalf("workspace artifact prompt_context_envelope must bind metadata_sha256, got %+v", envelope)
	}
}
