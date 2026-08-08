package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceDocRPCMirrorsDurableRuntimeEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const workspaceID = "ws-doc-rpc"
	ctx := testAuthContext(workspaceID, "agent", "agent-doc")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Doc RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawPut, err := json.Marshal(workspaceDocPutParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "hello world",
		UpdatedBy:   "agent-doc",
	})
	if err != nil {
		t.Fatalf("marshal put params: %v", err)
	}
	result, rpcErr := h.workspaceDocPut(ctx, rawPut)
	if rpcErr != nil {
		t.Fatalf("workspaceDocPut rpc error: %+v", rpcErr)
	}
	putPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected put response type %T", result)
	}
	if putPayload["status"] != "SAVED" || putPayload["doc_key"] != "runbook" {
		t.Fatalf("unexpected put payload %+v", putPayload)
	}
	putLive := nextEvent(t, ch)
	putPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    "runbook",
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, putLive, putPersisted, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, putLive.PayloadJSON), putPersisted.PayloadJSON)
	assertServerWorkspaceDocRuntimePromptContext(t, putPersisted, "workspace.doc.put", workspaceID, "agent", "agent-doc", map[string]string{
		"doc_key":    "runbook",
		"title":      "Runbook",
		"updated_by": "agent-doc",
	})

	seenUpserts := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    "runbook",
		Limit:       10,
	})
	rawPut, err = json.Marshal(workspaceDocPutParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook Revised",
		Content:     "hello again",
		UpdatedBy:   "agent-doc",
	})
	if err != nil {
		t.Fatalf("marshal second put params: %v", err)
	}
	result, rpcErr = h.workspaceDocPut(ctx, rawPut)
	if rpcErr != nil {
		t.Fatalf("second workspaceDocPut rpc error: %+v", rpcErr)
	}
	secondPutPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second put response type %T", result)
	}
	if secondPutPayload["status"] != "SAVED" || secondPutPayload["doc_key"] != "runbook" {
		t.Fatalf("unexpected second put payload %+v", secondPutPayload)
	}
	secondPutLive := nextEvent(t, ch)
	secondPutPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    "runbook",
		Limit:       10,
	}, seenUpserts)
	assertLiveEventMirrorsRuntimeEvent(t, secondPutLive, secondPutPersisted, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondPutLive.PayloadJSON), secondPutPersisted.PayloadJSON)
	assertServerWorkspaceDocRuntimePromptContext(t, secondPutPersisted, "workspace.doc.put", workspaceID, "agent", "agent-doc", map[string]string{
		"doc_key":    "runbook",
		"title":      "Runbook Revised",
		"updated_by": "agent-doc",
	})
	if secondPutPersisted.EventID == putPersisted.EventID || secondPutPersisted.IngestSeq <= putPersisted.IngestSeq {
		t.Fatalf("expected second doc upsert to append a newer runtime row, got first=%+v second=%+v", putPersisted, secondPutPersisted)
	}

	rawArchive, err := json.Marshal(workspaceDocArchiveParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		ArchivedBy:  "agent-doc",
	})
	if err != nil {
		t.Fatalf("marshal archive params: %v", err)
	}
	result, rpcErr = h.workspaceDocArchive(ctx, rawArchive)
	if rpcErr != nil {
		t.Fatalf("workspaceDocArchive rpc error: %+v", rpcErr)
	}
	archivePayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected archive response type %T", result)
	}
	if archivePayload["status"] != "ARCHIVED" {
		t.Fatalf("unexpected archive payload %+v", archivePayload)
	}
	archiveLive := nextEvent(t, ch)
	archivePersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.archived",
		EntityType:  "workspace_doc",
		EntityID:    "runbook",
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, archiveLive, archivePersisted, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, archiveLive.PayloadJSON), archivePersisted.PayloadJSON)
	assertServerWorkspaceDocRuntimePromptContext(t, archivePersisted, "workspace.doc.archive", workspaceID, "agent", "agent-doc", map[string]string{
		"doc_key":     "runbook",
		"archived_by": "agent-doc",
	})

	rawDelete, err := json.Marshal(workspaceDocDeleteParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		DeletedBy:   "agent-doc",
	})
	if err != nil {
		t.Fatalf("marshal delete params: %v", err)
	}
	result, rpcErr = h.workspaceDocDelete(ctx, rawDelete)
	if rpcErr != nil {
		t.Fatalf("workspaceDocDelete rpc error: %+v", rpcErr)
	}
	deletePayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected delete response type %T", result)
	}
	if deletePayload["status"] != "DELETED" {
		t.Fatalf("unexpected delete payload %+v", deletePayload)
	}
	deleteLive := nextEvent(t, ch)
	deletePersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.deleted",
		EntityType:  "workspace_doc",
		EntityID:    "runbook",
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, deleteLive, deletePersisted, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, deleteLive.PayloadJSON), deletePersisted.PayloadJSON)
	assertServerWorkspaceDocRuntimePromptContext(t, deletePersisted, "workspace.doc.delete", workspaceID, "agent", "agent-doc", map[string]string{
		"doc_key":    "runbook",
		"deleted_by": "agent-doc",
	})
}

func TestWorkspaceDocPutPublishesRefChangeMemoryInvalidationEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-doc-invalidation-live"
		agentID     = "agent-doc"
		docKey      = "runbook"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Doc Invalidation Live",
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
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "hello world",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("seed doc: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-doc-invalidation-live",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: sha256hex("hello world"), Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed residency: %v", err)
	}

	seenDocEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    docKey,
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

	rawPut, err := json.Marshal(workspaceDocPutParams{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook Revised",
		Content:     "hello again",
		UpdatedBy:   agentID,
	})
	if err != nil {
		t.Fatalf("marshal put params: %v", err)
	}
	result, rpcErr := h.workspaceDocPut(ctx, rawPut)
	if rpcErr != nil {
		t.Fatalf("workspaceDocPut rpc error: %+v", rpcErr)
	}
	putPayload, ok := result.(map[string]any)
	if !ok || putPayload["status"] != "SAVED" {
		t.Fatalf("unexpected put payload %+v", result)
	}

	docLive := nextEvent(t, ch)
	invalidationLive := nextEvent(t, ch)
	docPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    docKey,
		Limit:       10,
	}, seenDocEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)

	assertLiveEventMirrorsRuntimeEvent(t, docLive, docPersisted, "")
	assertLiveEventMirrorsRuntimeEvent(t, invalidationLive, invalidationPersisted, "")
	assertServerWorkspaceDocRuntimePromptContext(t, docPersisted, "workspace.doc.put", workspaceID, "agent", agentID, map[string]string{
		"doc_key":    docKey,
		"title":      "Runbook Revised",
		"updated_by": agentID,
	})
	assertRuntimeEventPayloadHasNoPromptContextEnvelope(t, invalidationPersisted)
	if docPersisted.IngestSeq >= invalidationPersisted.IngestSeq {
		t.Fatalf("expected doc event before invalidation enqueue, got doc=%+v invalidation=%+v", docPersisted, invalidationPersisted)
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "workspace_doc.upserted" {
		t.Fatalf("expected doc-triggered invalidation payload, got %+v", payload)
	}
}

func assertServerWorkspaceDocRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantPrincipalType, wantPrincipalID string, extra map[string]string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected workspace doc prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_doc_write",
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
			t.Fatalf("workspace doc prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("workspace doc prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}

func TestWorkspaceDocArchiveAndDeletePublishRefChangeMemoryInvalidationEvent(t *testing.T) {
	t.Run("archive", func(t *testing.T) {
		testWorkspaceDocLifecyclePublishesRefChangeMemoryInvalidationEvent(t, "archive")
	})
	t.Run("delete", func(t *testing.T) {
		testWorkspaceDocLifecyclePublishesRefChangeMemoryInvalidationEvent(t, "delete")
	})
}

func TestWorkspaceDocPutRejectsUpdatedByMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-doc-actor-mismatch", "agent", "agent-doc")

	const workspaceID = "ws-doc-actor-mismatch"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Doc Actor Mismatch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceDocPutParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "subject mismatch should fail closed",
		UpdatedBy:   "agent-other",
	})
	if err != nil {
		t.Fatalf("marshal put params: %v", err)
	}

	result, rpcErr := h.workspaceDocPut(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected updated_by mismatch to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no result on updated_by mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match updated_by" {
		t.Fatalf("unexpected updated_by mismatch error %+v", rpcErr)
	}
}

func TestWorkspaceDocRPCRejectsInvalidParamsAndNotFound(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-doc-invalid", "agent", "agent-doc")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-doc-invalid",
		Title:       "Doc Invalid",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, "ws-doc-invalid")

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params any
	}{
		{
			name: "put missing workspace",
			call: h.workspaceDocPut,
			params: workspaceDocPutParams{
				DocKey:    "runbook",
				Title:     "Runbook",
				Content:   "body",
				UpdatedBy: "agent-doc",
			},
		},
		{
			name: "get missing doc",
			call: h.workspaceDocGet,
			params: workspaceDocGetParams{
				WorkspaceID: "ws-doc-invalid",
				DocKey:      "missing",
			},
		},
		{
			name:   "list missing workspace",
			call:   h.workspaceDocList,
			params: workspaceDocListParams{},
		},
		{
			name: "archive missing doc",
			call: h.workspaceDocArchive,
			params: workspaceDocArchiveParams{
				WorkspaceID: "ws-doc-invalid",
				DocKey:      "missing",
				ArchivedBy:  "agent-doc",
			},
		},
		{
			name: "delete missing doc",
			call: h.workspaceDocDelete,
			params: workspaceDocDeleteParams{
				WorkspaceID: "ws-doc-invalid",
				DocKey:      "missing",
				DeletedBy:   "agent-doc",
			},
		},
		{
			name: "history missing doc key",
			call: h.workspaceDocHistory,
			params: workspaceDocHistoryParams{
				WorkspaceID: "ws-doc-invalid",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params rpc error, got %+v", rpcErr)
			}
		})
	}
}

func testWorkspaceDocLifecyclePublishesRefChangeMemoryInvalidationEvent(t *testing.T, action string) {
	t.Helper()

	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-doc-lifecycle-invalidation-live"
		agentID     = "agent-doc"
		docKey      = "runbook"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Doc Lifecycle Invalidation Live",
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
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "hello world",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("seed doc: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-doc-lifecycle-invalidation-live-" + action,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: sha256hex("hello world"), Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed residency: %v", err)
	}

	primaryEventType := map[string]string{
		"archive": "workspace_doc.archived",
		"delete":  "workspace_doc.deleted",
	}[action]
	seenPrimaryEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   primaryEventType,
		EntityType:  "workspace_doc",
		EntityID:    docKey,
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

	var (
		result any
		rpcErr *RPCError
	)
	switch action {
	case "archive":
		raw, err := json.Marshal(workspaceDocArchiveParams{WorkspaceID: workspaceID, DocKey: docKey, ArchivedBy: agentID})
		if err != nil {
			t.Fatalf("marshal archive params: %v", err)
		}
		result, rpcErr = h.workspaceDocArchive(ctx, raw)
	case "delete":
		raw, err := json.Marshal(workspaceDocDeleteParams{WorkspaceID: workspaceID, DocKey: docKey, DeletedBy: agentID})
		if err != nil {
			t.Fatalf("marshal delete params: %v", err)
		}
		result, rpcErr = h.workspaceDocDelete(ctx, raw)
	default:
		t.Fatalf("unsupported action %q", action)
	}
	if rpcErr != nil {
		t.Fatalf("workspace doc lifecycle rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected lifecycle response type %T", result)
	}
	if resp["doc_key"] != docKey {
		t.Fatalf("unexpected lifecycle response %+v", resp)
	}

	primaryLive := nextEvent(t, ch)
	invalidationLive := nextEvent(t, ch)
	primaryPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   primaryEventType,
		EntityType:  "workspace_doc",
		EntityID:    docKey,
		Limit:       10,
	}, seenPrimaryEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)

	assertLiveEventMirrorsRuntimeEvent(t, primaryLive, primaryPersisted, "")
	assertLiveEventMirrorsRuntimeEvent(t, invalidationLive, invalidationPersisted, "")
	surface := "workspace.doc.delete"
	extra := map[string]string{
		"doc_key":    docKey,
		"deleted_by": agentID,
	}
	if action == "archive" {
		surface = "workspace.doc.archive"
		extra = map[string]string{
			"doc_key":     docKey,
			"archived_by": agentID,
		}
	}
	assertServerWorkspaceDocRuntimePromptContext(t, primaryPersisted, surface, workspaceID, "agent", agentID, extra)
	assertRuntimeEventPayloadHasNoPromptContextEnvelope(t, invalidationPersisted)
	if primaryPersisted.IngestSeq >= invalidationPersisted.IngestSeq {
		t.Fatalf("expected %s event before invalidation enqueue, got primary=%+v invalidation=%+v", action, primaryPersisted, invalidationPersisted)
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	expectedCause := "workspace_doc." + action + "d"
	if action == "archive" {
		expectedCause = "workspace_doc.archived"
	}
	if payload["trigger_cause"] != expectedCause {
		t.Fatalf("expected %s-triggered invalidation payload, got %+v", action, payload)
	}
}

func mustListRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, filter sqlite.RuntimeEventFilter) sqlite.RuntimeEventRecord {
	t.Helper()
	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	if len(events) == 0 {
		t.Fatalf("expected runtime event for filter %+v", filter)
	}
	return events[0]
}
