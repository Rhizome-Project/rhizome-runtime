package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryInvalidationPollAndAck(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation"
		agentID     = "agent-handler-memory-invalidation"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Invalidation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawPoll, err := json.Marshal(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("marshal poll params: %v", err)
	}
	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, rawPoll)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	pollPayload := pollAny.(map[string]any)
	pollAuthority, ok := pollPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || pollAuthority.WorkspaceID != workspaceID || pollAuthority.ReferenceAt == "" {
		t.Fatalf("expected poll time authority, got %+v", pollPayload["time_authority"])
	}
	items, ok := pollPayload["items"].([]sqlite.MemoryInvalidationRecord)
	if !ok {
		t.Fatalf("unexpected poll items type %T", pollPayload["items"])
	}
	if len(items) != 1 || items[0].DeliveredAt == "" {
		t.Fatalf("unexpected poll payload %+v", pollPayload)
	}
	if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected poll item time authority, got %+v", items[0].TimeAuthority)
	}
	deliveredLive := expectMemoryInvalidationEvent(t, ch, "memory.invalidation_delivered")
	deliveredPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_delivered",
		EntityType:  "memory_invalidation",
		EntityID:    items[0].InvalidationID,
		Limit:       1,
	})
	if deliveredLive.EventID != deliveredPersisted.EventID || deliveredLive.IngestSeq != deliveredPersisted.IngestSeq {
		t.Fatalf("expected delivered live event to mirror persisted runtime envelope, live=%+v persisted=%+v", deliveredLive, deliveredPersisted)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, deliveredLive.PayloadJSON), deliveredPersisted.PayloadJSON)

	rawAck, err := json.Marshal(workspaceMemoryInvalidationAckParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{items[0].InvalidationID},
	})
	if err != nil {
		t.Fatalf("marshal ack params: %v", err)
	}
	ackAny, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, rawAck)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationAck rpc error: %+v", rpcErr)
	}
	ackPayload := ackAny.(map[string]any)
	ackAuthority, ok := ackPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || ackAuthority.WorkspaceID != workspaceID || ackAuthority.ReferenceAt == "" {
		t.Fatalf("expected ack time authority, got %+v", ackPayload["time_authority"])
	}
	ackedItems, ok := ackPayload["items"].([]sqlite.MemoryInvalidationRecord)
	if !ok {
		t.Fatalf("unexpected ack items type %T", ackPayload["items"])
	}
	if len(ackedItems) != 1 || ackedItems[0].State != "ACKED" {
		t.Fatalf("unexpected ack payload %+v", ackPayload)
	}
	if ackedItems[0].TimeAuthority.WorkspaceID != workspaceID || ackedItems[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected ack item time authority, got %+v", ackedItems[0].TimeAuthority)
	}
	ackedLive := expectMemoryInvalidationEvent(t, ch, "memory.invalidation_acked")
	ackedPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_acked",
		EntityType:  "memory_invalidation",
		EntityID:    ackedItems[0].InvalidationID,
		Limit:       1,
	})
	if ackedLive.EventID != ackedPersisted.EventID || ackedLive.IngestSeq != ackedPersisted.IngestSeq {
		t.Fatalf("expected ack live event to mirror persisted runtime envelope, live=%+v persisted=%+v", ackedLive, ackedPersisted)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, ackedLive.PayloadJSON), ackedPersisted.PayloadJSON)
}

func TestWorkspaceMemoryInvalidationListGetAndCursor(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-cursor"
		agentID     = "agent-handler-memory-invalidation-cursor"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Cursor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Cursor Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	pollPayload := pollAny.(map[string]any)
	items := pollPayload["items"].([]sqlite.MemoryInvalidationRecord)
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", pollPayload)
	}

	listAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		IncludeAcked:      true,
		IncludeDeadLetter: true,
		Limit:             10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList rpc error: %+v", rpcErr)
	}
	listPayload := listAny.(map[string]any)
	listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected list time authority, got %+v", listPayload["time_authority"])
	}
	listItems := listPayload["items"].([]sqlite.MemoryInvalidationRecord)
	if len(listItems) != 1 || listItems[0].InvalidationID != items[0].InvalidationID {
		t.Fatalf("unexpected list payload %+v", listPayload)
	}
	if listItems[0].TimeAuthority.WorkspaceID != workspaceID || listItems[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected list item time authority, got %+v", listItems[0].TimeAuthority)
	}
	if listItems[0].TriggerCause != "workspace_doc.upserted" || len(listItems[0].DependencyRevisionVector) != 1 || listItems[0].DependencyRevisionVector[0].RefKind != "workspace_doc" || listItems[0].DependencyRevisionVector[0].RefID != docKey {
		t.Fatalf("expected list item to surface dependency revision vector and trigger cause, got %+v", listItems[0])
	}

	getAny, rpcErr := callWorkspaceMemoryInvalidationGetRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationGetParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		InvalidationID: items[0].InvalidationID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationGet rpc error: %+v", rpcErr)
	}
	getItem := getAny.(sqlite.MemoryInvalidationRecord)
	if getItem.InvalidationID != items[0].InvalidationID {
		t.Fatalf("unexpected get item %+v", getItem)
	}
	if getItem.TimeAuthority.WorkspaceID != workspaceID || getItem.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected get item time authority, got %+v", getItem.TimeAuthority)
	}
	if getItem.TriggerCause != "workspace_doc.upserted" || len(getItem.DependencyRevisionVector) != 1 || getItem.DependencyRevisionVector[0].RefKind != "workspace_doc" || getItem.DependencyRevisionVector[0].RefID != docKey {
		t.Fatalf("expected get item to surface dependency revision vector and trigger cause, got %+v", getItem)
	}

	cursorAny, rpcErr := callWorkspaceMemoryInvalidationCursorGetRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationCursorGetParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationCursorGet rpc error: %+v", rpcErr)
	}
	cursor := cursorAny.(sqlite.MemoryInvalidationCursorRecord)
	if cursor.LastDeliveredInvalidationID != items[0].InvalidationID || cursor.LastPollCount != 1 {
		t.Fatalf("unexpected cursor %+v", cursor)
	}
	if cursor.TimeAuthority.WorkspaceID != workspaceID || cursor.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected cursor time authority, got %+v", cursor.TimeAuthority)
	}
}

func TestWorkspaceMemoryInvalidationListGetCanonicalizesStringEncodedDependencyVector(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-canonical-vector", "agent-handler-memory-invalidation-canonical-vector", "canonical-doc")

	rawMetadata, err := json.Marshal(map[string]any{
		"cause": "workspace_doc.upserted",
		"dependency_revision_vector": `[
			{"ref_kind":" workspace_doc ","ref_id":" canonical-doc ","version_token":"doc-v0","weight":0.25},
			{"ref_kind":"workspace_doc","ref_id":"canonical-doc","version_token":"doc-v1","weight":1}
		]`,
	})
	if err != nil {
		t.Fatalf("marshal string-encoded dependency vector metadata: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE memory_invalidation_queue SET metadata_json = ?, updated_at = ? WHERE invalidation_id = ?`,
		string(rawMetadata),
		time.Now().UTC().Format(time.RFC3339Nano),
		item.InvalidationID,
	); err != nil {
		t.Fatalf("update invalidation metadata: %v", err)
	}

	listAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		IncludeAcked:      true,
		IncludeDeadLetter: true,
		Limit:             10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList rpc error: %+v", rpcErr)
	}
	listItems := listAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(listItems) != 1 {
		t.Fatalf("expected one invalidation item, got %+v", listItems)
	}
	if len(listItems[0].DependencyRevisionVector) != 1 ||
		listItems[0].DependencyRevisionVector[0].RefKind != "workspace_doc" ||
		listItems[0].DependencyRevisionVector[0].RefID != "canonical-doc" ||
		listItems[0].DependencyRevisionVector[0].Weight != 1 {
		t.Fatalf("expected list RPC to canonicalize dependency vector, got %+v", listItems[0].DependencyRevisionVector)
	}
	if _, ok := listItems[0].Metadata["dependency_revision_vector"].(string); ok {
		t.Fatalf("expected list RPC metadata dependency vector to be canonicalized, got %+v", listItems[0].Metadata["dependency_revision_vector"])
	}

	getAny, rpcErr := callWorkspaceMemoryInvalidationGetRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationGetParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		InvalidationID: item.InvalidationID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationGet rpc error: %+v", rpcErr)
	}
	getItem := getAny.(sqlite.MemoryInvalidationRecord)
	if len(getItem.DependencyRevisionVector) != 1 ||
		getItem.DependencyRevisionVector[0].RefKind != "workspace_doc" ||
		getItem.DependencyRevisionVector[0].RefID != "canonical-doc" ||
		getItem.DependencyRevisionVector[0].Weight != 1 {
		t.Fatalf("expected get RPC to canonicalize dependency vector, got %+v", getItem.DependencyRevisionVector)
	}
	if _, ok := getItem.Metadata["dependency_revision_vector"].(string); ok {
		t.Fatalf("expected get RPC metadata dependency vector to be canonicalized, got %+v", getItem.Metadata["dependency_revision_vector"])
	}
}

func TestWorkspaceMemoryInvalidationListGetSurfacesMalformedDependencyVector(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-malformed-vector", "agent-handler-memory-invalidation-malformed-vector", "malformed-doc")

	rawMetadata, err := json.Marshal(map[string]any{
		"cause":                      "workspace_doc.upserted",
		"dependency_revision_vector": "not-json",
	})
	if err != nil {
		t.Fatalf("marshal malformed dependency vector metadata: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE memory_invalidation_queue SET metadata_json = ?, updated_at = ? WHERE invalidation_id = ?`,
		string(rawMetadata),
		time.Now().UTC().Format(time.RFC3339Nano),
		item.InvalidationID,
	); err != nil {
		t.Fatalf("update invalidation metadata: %v", err)
	}

	listAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		IncludeAcked:      true,
		IncludeDeadLetter: true,
		Limit:             10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList rpc error: %+v", rpcErr)
	}
	listItems := listAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(listItems) != 1 || len(listItems[0].DependencyRevisionVector) != 0 || !listItems[0].DependencyVectorMalformed {
		t.Fatalf("expected list RPC to surface malformed dependency vector flag, got %+v", listItems)
	}
	if malformed, _ := listItems[0].Metadata["dependency_revision_vector_malformed"].(bool); !malformed {
		t.Fatalf("expected list RPC metadata to retain malformed dependency vector warning, got %+v", listItems[0].Metadata)
	}

	getAny, rpcErr := callWorkspaceMemoryInvalidationGetRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationGetParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		InvalidationID: item.InvalidationID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationGet rpc error: %+v", rpcErr)
	}
	getItem := getAny.(sqlite.MemoryInvalidationRecord)
	if len(getItem.DependencyRevisionVector) != 0 || !getItem.DependencyVectorMalformed {
		t.Fatalf("expected get RPC to surface malformed dependency vector flag, got %+v", getItem)
	}
	if malformed, _ := getItem.Metadata["dependency_revision_vector_malformed"].(bool); !malformed {
		t.Fatalf("expected get RPC metadata to retain malformed dependency vector warning, got %+v", getItem.Metadata)
	}
}

func TestWorkspaceMemoryInvalidationFailDeadLettersAndMirrorsEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-fail"
		agentID     = "agent-handler-memory-invalidation-fail"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Fail",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Fail Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	items := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)
	for attempt := 1; attempt <= 3; attempt++ {
		failAny, rpcErr := callWorkspaceMemoryInvalidationFailRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{items[0].InvalidationID},
			FailureReason:   "APPLY_FAILED",
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationFail rpc error on attempt %d: %+v", attempt, rpcErr)
		}
		failedItems := failAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(failedItems) != 1 {
			t.Fatalf("expected one failed item on attempt %d, got %+v", attempt, failedItems)
		}
		expectedEvent := "memory.invalidation_failed"
		if attempt == 3 {
			if failedItems[0].State != "DEAD_LETTER" {
				t.Fatalf("expected dead-letter item on final attempt, got %+v", failedItems[0])
			}
			expectedEvent = "memory.invalidation_dead_lettered"
		}
		liveEvent := expectMemoryInvalidationEvent(t, ch, expectedEvent)
		persistedEvent := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   expectedEvent,
			EntityType:  "memory_invalidation",
			EntityID:    failedItems[0].InvalidationID,
			Limit:       1,
		})
		if liveEvent.EventID != persistedEvent.EventID || liveEvent.IngestSeq != persistedEvent.IngestSeq {
			t.Fatalf("expected fail live event to mirror persisted runtime envelope, live=%+v persisted=%+v", liveEvent, persistedEvent)
		}
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), persistedEvent.PayloadJSON)
		if attempt < 3 {
			redeliverHandlerMemoryInvalidationForTest(t, store, h, ctx, workspaceID, agentID, items[0].InvalidationID)
		}
	}

	listAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		IncludeDeadLetter: true,
		Limit:             10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList rpc error: %+v", rpcErr)
	}
	listItems := listAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(listItems) != 1 || listItems[0].State != "DEAD_LETTER" || listItems[0].FailureCount != 3 {
		t.Fatalf("unexpected dead-letter list payload %+v", listItems)
	}
}

func TestWorkspaceMemoryInvalidationPollLeaseSkipsImmediateRepollUntilExpired(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-lease", "agent-handler-memory-invalidation-lease", "lease-doc")

	firstAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("first workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	firstItems := firstAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(firstItems) != 1 || firstItems[0].InvalidationID != item.InvalidationID {
		t.Fatalf("unexpected first poll payload %+v", firstAny)
	}
	if firstItems[0].LeaseExpiresAt == "" {
		t.Fatalf("expected lease_expires_at in first poll item, got %+v", firstItems[0])
	}
	if lease := getHandlerMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at"); lease == "" {
		t.Fatalf("expected lease_expires_at persisted for %s", item.InvalidationID)
	}

	secondAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("second workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	secondItems := secondAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(secondItems) != 0 {
		t.Fatalf("expected immediate repoll to skip leased item, got %+v", secondItems)
	}

	setHandlerMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))

	thirdAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("third workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	thirdItems := thirdAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(thirdItems) != 1 || thirdItems[0].InvalidationID != item.InvalidationID {
		t.Fatalf("expected expired lease item to repoll, got %+v", thirdItems)
	}
	if thirdItems[0].LeaseExpiresAt == "" {
		t.Fatalf("expected refreshed lease_expires_at after repoll, got %+v", thirdItems[0])
	}
}

func TestWorkspaceMemoryInvalidationAckRequiresActiveDeliveryLease(t *testing.T) {
	t.Run("before delivery", func(t *testing.T) {
		store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-ack-guard-undelivered", "agent-handler-memory-invalidation-ack-guard-undelivered", "ack-guard-undelivered-doc")

		ackAny, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationAck rpc error before delivery: %+v", rpcErr)
		}
		ackItems := ackAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(ackItems) != 0 {
			t.Fatalf("expected undelivered invalidation ack to be ignored, got %+v", ackAny)
		}

		stored, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get invalidation after undelivered ack no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.AcknowledgedAt != "" || stored.DeliveredAt != "" {
			t.Fatalf("expected undelivered invalidation to stay open after ack no-op, got %+v", stored)
		}
	})

	t.Run("after lease expiry", func(t *testing.T) {
		store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-ack-guard-expired", "agent-handler-memory-invalidation-ack-guard-expired", "ack-guard-expired-doc")

		if _, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
			WorkspaceID:   workspaceID,
			AgentID:       agentID,
			Limit:         10,
			MarkDelivered: true,
		})); rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationPoll rpc error before ack expiry check: %+v", rpcErr)
		}
		setHandlerMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))

		ackAny, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationAck rpc error after lease expiry: %+v", rpcErr)
		}
		ackItems := ackAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(ackItems) != 0 {
			t.Fatalf("expected expired invalidation lease to reject ack, got %+v", ackAny)
		}

		stored, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get invalidation after expired ack no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.AcknowledgedAt != "" {
			t.Fatalf("expected expired invalidation lease to reject ack without state change, got %+v", stored)
		}
	})

	t.Run("during active delivery lease", func(t *testing.T) {
		_, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-ack-guard-active", "agent-handler-memory-invalidation-ack-guard-active", "ack-guard-active-doc")

		if _, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
			WorkspaceID:   workspaceID,
			AgentID:       agentID,
			Limit:         10,
			MarkDelivered: true,
		})); rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationPoll rpc error before active ack: %+v", rpcErr)
		}

		ackAny, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationAck rpc error during active lease: %+v", rpcErr)
		}
		ackItems := ackAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(ackItems) != 1 || ackItems[0].State != "ACKED" {
			t.Fatalf("expected ack during active lease, got %+v", ackItems)
		}
	})
}

func TestWorkspaceMemoryInvalidationFailBackoffSkipsImmediateRepollUntilExpired(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-backoff", "agent-handler-memory-invalidation-backoff", "backoff-doc")

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	pollItems := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(pollItems) != 1 || pollItems[0].InvalidationID != item.InvalidationID {
		t.Fatalf("unexpected initial poll payload %+v", pollAny)
	}

	failAny, rpcErr := callWorkspaceMemoryInvalidationFailRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{item.InvalidationID},
		FailureReason:   "APPLY_FAILED",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationFail rpc error: %+v", rpcErr)
	}
	failedItems := failAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(failedItems) != 1 || failedItems[0].InvalidationID != item.InvalidationID {
		t.Fatalf("unexpected fail payload %+v", failAny)
	}
	if failedItems[0].State != "OPEN" || failedItems[0].FailureCount != 1 {
		t.Fatalf("expected open item with failure_count=1 after below-threshold fail, got %+v", failedItems[0])
	}
	if failedItems[0].NextDeliveryAt == "" {
		t.Fatalf("expected next_delivery_at after below-threshold fail, got %+v", failedItems[0])
	}
	if nextDelivery := getHandlerMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "next_delivery_at"); nextDelivery == "" {
		t.Fatalf("expected next_delivery_at persisted for %s", item.InvalidationID)
	}

	skippedAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll after fail rpc error: %+v", rpcErr)
	}
	skippedItems := skippedAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(skippedItems) != 0 {
		t.Fatalf("expected immediate repoll to skip backed-off item, got %+v", skippedItems)
	}

	setHandlerMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "next_delivery_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))

	readyAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll after backoff expiry rpc error: %+v", rpcErr)
	}
	readyItems := readyAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(readyItems) != 1 || readyItems[0].InvalidationID != item.InvalidationID {
		t.Fatalf("expected expired next_delivery_at item to repoll, got %+v", readyItems)
	}
}

func TestWorkspaceMemoryInvalidationFailRequiresActiveDeliveryLease(t *testing.T) {
	t.Run("before delivery", func(t *testing.T) {
		store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-fail-guard-undelivered", "agent-handler-memory-invalidation-fail-guard-undelivered", "fail-guard-undelivered-doc")

		failAny, rpcErr := callWorkspaceMemoryInvalidationFailRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationFail rpc error before delivery: %+v", rpcErr)
		}
		failItems := failAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(failItems) != 0 {
			t.Fatalf("expected undelivered invalidation fail to be ignored, got %+v", failAny)
		}

		stored, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get invalidation after undelivered fail no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.FailureCount != 0 || stored.DeliveredAt != "" {
			t.Fatalf("expected undelivered invalidation to remain unchanged after fail no-op, got %+v", stored)
		}
	})

	t.Run("after lease expiry", func(t *testing.T) {
		store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-fail-guard-expired", "agent-handler-memory-invalidation-fail-guard-expired", "fail-guard-expired-doc")

		if _, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
			WorkspaceID:   workspaceID,
			AgentID:       agentID,
			Limit:         10,
			MarkDelivered: true,
		})); rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationPoll rpc error before fail expiry check: %+v", rpcErr)
		}
		setHandlerMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))

		failAny, rpcErr := callWorkspaceMemoryInvalidationFailRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationFail rpc error after lease expiry: %+v", rpcErr)
		}
		failItems := failAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(failItems) != 0 {
			t.Fatalf("expected expired invalidation lease to reject fail, got %+v", failAny)
		}

		stored, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get invalidation after expired fail no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.FailureCount != 0 {
			t.Fatalf("expected expired invalidation lease to reject fail without state change, got %+v", stored)
		}
	})

	t.Run("during active delivery lease", func(t *testing.T) {
		_, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-fail-guard-active", "agent-handler-memory-invalidation-fail-guard-active", "fail-guard-active-doc")

		if _, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
			WorkspaceID:   workspaceID,
			AgentID:       agentID,
			Limit:         10,
			MarkDelivered: true,
		})); rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationPoll rpc error before active fail: %+v", rpcErr)
		}

		failAny, rpcErr := callWorkspaceMemoryInvalidationFailRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationFail rpc error during active lease: %+v", rpcErr)
		}
		failItems := failAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(failItems) != 1 || failItems[0].FailureCount != 1 {
			t.Fatalf("expected fail during active lease, got %+v", failItems)
		}
	})
}

func TestWorkspaceMemoryInvalidationRequeueReopensDeadLetterAndMirrorsEvent(t *testing.T) {
	store, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-requeue", "agent-handler-memory-invalidation-requeue", "requeue-doc")

	if _, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error before dead-letter loop: %+v", rpcErr)
	}

	for attempt := 0; attempt < 3; attempt++ {
		failAny, rpcErr := callWorkspaceMemoryInvalidationFailRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationFail rpc error on attempt %d: %+v", attempt+1, rpcErr)
		}
		failedItems := failAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(failedItems) != 1 {
			t.Fatalf("expected dead-letter attempt %d to change the invalidation, got %+v", attempt+1, failAny)
		}
		if attempt < 2 {
			redeliverHandlerMemoryInvalidationForTest(t, store, h, ctx, workspaceID, agentID, item.InvalidationID)
		}
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	requeueAny, rpcErr := callWorkspaceMemoryInvalidationRequeueRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationRequeueParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{item.InvalidationID},
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationRequeue rpc error: %+v", rpcErr)
	}
	requeuedItems := requeueAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(requeuedItems) != 1 || requeuedItems[0].State != "OPEN" {
		t.Fatalf("unexpected requeue payload %+v", requeueAny)
	}
	if requeuedItems[0].InvalidationID == item.InvalidationID {
		t.Fatalf("expected requeue to clone into a new invalidation row, got %+v", requeuedItems[0])
	}
	if requeuedItems[0].RecoveredFromInvalidationID != item.InvalidationID || requeuedItems[0].RecoveryCause != "dead_letter_requeue" || len(requeuedItems[0].DependencyRevisionVector) == 0 {
		t.Fatalf("expected requeue payload to surface lineage and dependency vector, got %+v", requeuedItems[0])
	}
	if requeuedItems[0].FailureCount != 0 || requeuedItems[0].DeadLetteredAt != "" || requeuedItems[0].LeaseExpiresAt != "" || requeuedItems[0].NextDeliveryAt != "" {
		t.Fatalf("expected cleared requeue state, got %+v", requeuedItems[0])
	}
	liveEvent := expectMemoryInvalidationEvent(t, ch, "memory.invalidation_requeued")
	persistedEvent := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_requeued",
		EntityType:  "memory_invalidation",
		EntityID:    requeuedItems[0].InvalidationID,
		Limit:       1,
	})
	if liveEvent.EventID != persistedEvent.EventID || liveEvent.IngestSeq != persistedEvent.IngestSeq {
		t.Fatalf("expected requeue live event to mirror persisted runtime envelope, live=%+v persisted=%+v", liveEvent, persistedEvent)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), persistedEvent.PayloadJSON)

	readyAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll after requeue rpc error: %+v", rpcErr)
	}
	readyItems := readyAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(readyItems) != 1 || readyItems[0].InvalidationID != requeuedItems[0].InvalidationID {
		t.Fatalf("expected requeued item to become pollable again, got %+v", readyItems)
	}

	oldState := getHandlerMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "state")
	if oldState != "DEAD_LETTER" {
		t.Fatalf("expected original queue row to remain dead-lettered after requeue, got %s", oldState)
	}
	newState := getHandlerMemoryInvalidationQueueStringColumn(t, store, requeuedItems[0].InvalidationID, "state")
	if newState != "OPEN" {
		t.Fatalf("expected cloned queue row state OPEN after requeue, got %s", newState)
	}
}

func TestWorkspaceMemoryInvalidationRequeueSkipsNonDeadLetterItems(t *testing.T) {
	_, h, ctx, workspaceID, agentID, item := seedHandlerOpenMemoryInvalidation(t, "ws-handler-memory-invalidation-requeue-open", "agent-handler-memory-invalidation-requeue-open", "requeue-open-doc")

	requeueAny, rpcErr := callWorkspaceMemoryInvalidationRequeueRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationRequeueParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{item.InvalidationID},
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationRequeue rpc error: %+v", rpcErr)
	}
	requeuedItems := requeueAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(requeuedItems) != 0 {
		t.Fatalf("expected requeue on open item to be a no-op, got %+v", requeuedItems)
	}
}

func TestWorkspaceMemoryInvalidationAckSuppressesSameStaleCondition(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-ack-suppress"
		agentID     = "agent-handler-memory-invalidation-ack-suppress"
		docKey      = "runbook"
		reportID    = "memres-ack-suppress"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Ack Suppress",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Ack Suppress Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	items := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	if _, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{items[0].InvalidationID},
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationAck rpc error: %+v", rpcErr)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report same stale residency again: %v", err)
	}

	suppressedAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll suppression rpc error: %+v", rpcErr)
	}
	suppressedItems := suppressedAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(suppressedItems) != 0 {
		t.Fatalf("expected same stale invalidation to stay suppressed after ack, got %+v", suppressedItems)
	}
}

func TestWorkspaceMemoryInvalidationAckBypassesSuppressionWhenCanonicalDependencyVectorWidens(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-ack-vector-change"
		agentID     = "agent-handler-memory-invalidation-ack-vector-change"
		docKey      = "runbook"
		reportID    = "memres-ack-vector-change"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Ack Vector Change",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Ack Vector Change Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook evidence",
		Body:        "Current claim guard for widened dependency vector.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}
	docV2, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v2: %v", err)
	}

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	items := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(items) != 1 {
		t.Fatalf("expected one initial invalidation, got %+v", items)
	}
	if _, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{items[0].InvalidationID},
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationAck rpc error: %+v", rpcErr)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report widened dependency vector residency: %v", err)
	}

	readyAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll widened-vector rpc error: %+v", rpcErr)
	}
	readyItems := readyAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(readyItems) != 1 {
		t.Fatalf("expected widened dependency vector to bypass ack suppression, got %+v", readyItems)
	}
	if readyItems[0].InvalidationID == items[0].InvalidationID {
		t.Fatalf("expected widened dependency vector to yield a new invalidation row, got %+v", readyItems[0])
	}
	if readyItems[0].CurrentVersionToken != docV2.SHA {
		t.Fatalf("expected widened invalidation to retain current source version token %q, got %+v", docV2.SHA, readyItems[0])
	}
	assertHandlerMemoryInvalidationDependencyVector(t, readyItems[0].DependencyRevisionVector,
		sqlite.MemoryResidencyVersionGuard{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
		sqlite.MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
	)
}

func TestWorkspaceMemoryInvalidationAckAllowsWidenedDependencyVectorToReenqueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-ack-vector-change"
		agentID     = "agent-handler-memory-invalidation-ack-vector-change"
		docKey      = "runbook"
		reportID    = "memres-ack-vector-change"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Ack Vector Change",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Ack Vector Change Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook evidence",
		Body:        "Handler claim guard for widened dependency vector.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}
	docV2, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v2: %v", err)
	}

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	items := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	if _, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{items[0].InvalidationID},
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationAck rpc error: %+v", rpcErr)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report widened dependency vector residency: %v", err)
	}

	readyAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll widened-vector rpc error: %+v", rpcErr)
	}
	readyItems := readyAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(readyItems) != 1 {
		t.Fatalf("expected widened dependency vector to reenqueue, got %+v", readyItems)
	}
	if readyItems[0].InvalidationID == items[0].InvalidationID {
		t.Fatalf("expected widened lineage to create a new invalidation row, got %+v", readyItems[0])
	}
	if readyItems[0].CurrentVersionToken != docV2.SHA {
		t.Fatalf("expected widened invalidation to retain current source version token %q, got %+v", docV2.SHA, readyItems[0])
	}
	if len(readyItems[0].DependencyRevisionVector) != 2 {
		t.Fatalf("expected widened dependency vector on RPC poll payload, got %+v", readyItems[0].DependencyRevisionVector)
	}
	if readyItems[0].DependencyRevisionVector[0].RefKind != "knowledge_claim" ||
		readyItems[0].DependencyRevisionVector[0].RefID != claim.ClaimID ||
		readyItems[0].DependencyRevisionVector[1].RefKind != "workspace_doc" ||
		readyItems[0].DependencyRevisionVector[1].RefID != docKey {
		t.Fatalf("expected widened canonical dependency vector on RPC poll payload, got %+v", readyItems[0].DependencyRevisionVector)
	}
}

func TestWorkspaceMemoryInvalidationAckAllowsNewSourceVersionToReenqueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-ack-new-version"
		agentID     = "agent-handler-memory-invalidation-ack-new-version"
		docKey      = "runbook"
		reportID    = "memres-ack-new-version"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Ack New Version",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Ack New Version Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	items := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	if _, rpcErr := callWorkspaceMemoryInvalidationAckRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationAckParams{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{items[0].InvalidationID},
	})); rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationAck rpc error: %+v", rpcErr)
	}

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion C",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v3: %v", err)
	}

	readyAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll new-version rpc error: %+v", rpcErr)
	}
	readyItems := readyAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(readyItems) != 1 {
		t.Fatalf("expected invalidation after source version changed again, got %+v", readyItems)
	}
	if readyItems[0].CurrentVersionToken == items[0].CurrentVersionToken {
		t.Fatalf("expected a new current version token after second source change, got %+v", readyItems[0])
	}
}

func TestWorkspaceMemoryInvalidationListGetRefreshCanonicalVectorWhenSameOpenInvalidationWidens(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-open-vector-refresh"
		agentID     = "agent-handler-memory-invalidation-open-vector-refresh"
		docKey      = "runbook"
		reportID    = "memres-open-vector-refresh"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Open Vector Refresh",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Open Vector Refresh Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook side evidence",
		Body:        "Current claim guard for open invalidation vector refresh.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}
	docV2, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v2: %v", err)
	}

	initialListAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList initial rpc error: %+v", rpcErr)
	}
	initialItems := initialListAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(initialItems) != 1 {
		t.Fatalf("expected one initial invalidation, got %+v", initialItems)
	}
	initialID := initialItems[0].InvalidationID
	assertHandlerMemoryInvalidationDependencyVector(t, initialItems[0].DependencyRevisionVector,
		sqlite.MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
	)

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report widened dependency vector residency: %v", err)
	}

	refreshedListAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList refreshed rpc error: %+v", rpcErr)
	}
	refreshedItems := refreshedListAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(refreshedItems) == 0 {
		t.Fatalf("expected refreshed invalidation items, got %+v", refreshedItems)
	}

	var refreshed *sqlite.MemoryInvalidationRecord
	for idx := range refreshedItems {
		if refreshedItems[idx].InvalidationID == initialID {
			candidate := refreshedItems[idx]
			refreshed = &candidate
			break
		}
	}
	if refreshed == nil {
		t.Fatalf("expected same-key open invalidation to refresh in place, got %+v", refreshedItems)
	}
	if refreshed.CurrentVersionToken != docV2.SHA {
		t.Fatalf("expected refreshed invalidation to retain current source version token %q, got %+v", docV2.SHA, refreshed)
	}
	assertHandlerMemoryInvalidationDependencyVector(t, refreshed.DependencyRevisionVector,
		sqlite.MemoryResidencyVersionGuard{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
		sqlite.MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
	)

	getAny, rpcErr := callWorkspaceMemoryInvalidationGetRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationGetParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		InvalidationID: initialID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationGet refreshed rpc error: %+v", rpcErr)
	}
	getItem := getAny.(sqlite.MemoryInvalidationRecord)
	assertHandlerMemoryInvalidationDependencyVector(t, getItem.DependencyRevisionVector,
		sqlite.MemoryResidencyVersionGuard{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
		sqlite.MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
	)
}

func TestWorkspaceMemoryInvalidationListRefreshesOpenDependencyVectorWhenSameReportWidens(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-open-vector-refresh"
		agentID     = "agent-handler-memory-invalidation-open-vector-refresh"
		docKey      = "runbook"
		reportID    = "memres-open-vector-refresh"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Open Vector Refresh",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Open Vector Refresh Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook side evidence",
		Body:        "Handler claim guard for open invalidation vector refresh.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}
	docV2, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v2: %v", err)
	}

	initialAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList initial rpc error: %+v", rpcErr)
	}
	initialItems := initialAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(initialItems) != 1 {
		t.Fatalf("expected one initial invalidation, got %+v", initialItems)
	}
	initialID := initialItems[0].InvalidationID

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report widened dependency vector residency for same report: %v", err)
	}

	refreshedAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList widened-vector rpc error: %+v", rpcErr)
	}
	refreshedItems := refreshedAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(refreshedItems) != 1 {
		t.Fatalf("expected one refreshed open invalidation, got %+v", refreshedItems)
	}
	if refreshedItems[0].InvalidationID != initialID {
		t.Fatalf("expected widened lineage to refresh the same open invalidation row, got %+v", refreshedItems[0])
	}
	if refreshedItems[0].CurrentVersionToken != docV2.SHA {
		t.Fatalf("expected refreshed invalidation to retain current source version token %q, got %+v", docV2.SHA, refreshedItems[0])
	}
	if len(refreshedItems[0].DependencyRevisionVector) != 2 {
		t.Fatalf("expected widened dependency vector on RPC list payload, got %+v", refreshedItems[0].DependencyRevisionVector)
	}
	if refreshedItems[0].DependencyRevisionVector[0].RefKind != "knowledge_claim" ||
		refreshedItems[0].DependencyRevisionVector[0].RefID != claim.ClaimID ||
		refreshedItems[0].DependencyRevisionVector[1].RefKind != "workspace_doc" ||
		refreshedItems[0].DependencyRevisionVector[1].RefID != docKey {
		t.Fatalf("expected widened canonical dependency vector on RPC list payload, got %+v", refreshedItems[0].DependencyRevisionVector)
	}

	getAny, rpcErr := callWorkspaceMemoryInvalidationGetRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationGetParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		InvalidationID: initialID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationGet widened-vector rpc error: %+v", rpcErr)
	}
	getItem := getAny.(sqlite.MemoryInvalidationRecord)
	if len(getItem.DependencyRevisionVector) != 2 ||
		getItem.DependencyRevisionVector[0].RefKind != "knowledge_claim" ||
		getItem.DependencyRevisionVector[0].RefID != claim.ClaimID ||
		getItem.DependencyRevisionVector[1].RefKind != "workspace_doc" ||
		getItem.DependencyRevisionVector[1].RefID != docKey {
		t.Fatalf("expected widened canonical dependency vector on RPC get payload, got %+v", getItem.DependencyRevisionVector)
	}
}

func TestWorkspaceMemoryInvalidationDeadLetterStillAllowsSameStaleCondition(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-invalidation-dead-letter-same-stale"
		agentID     = "agent-handler-memory-invalidation-dead-letter-same-stale"
		docKey      = "runbook"
		reportID    = "memres-dead-letter-same-stale"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Invalidation Dead Letter Same Stale",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Dead Letter Same Stale Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll rpc error: %+v", rpcErr)
	}
	items := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	for attempt := 0; attempt < 3; attempt++ {
		failAny, rpcErr := callWorkspaceMemoryInvalidationFailRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationFailParams{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{items[0].InvalidationID},
			FailureReason:   "APPLY_FAILED",
		}))
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryInvalidationFail rpc error on attempt %d: %+v", attempt+1, rpcErr)
		}
		failedItems := failAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
		if len(failedItems) != 1 {
			t.Fatalf("expected dead-letter attempt %d to change the invalidation, got %+v", attempt+1, failAny)
		}
		if attempt < 2 {
			redeliverHandlerMemoryInvalidationForTest(t, store, h, ctx, workspaceID, agentID, items[0].InvalidationID)
		}
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
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
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report same stale residency after dead-letter: %v", err)
	}

	readyAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationPoll after dead-letter rpc error: %+v", rpcErr)
	}
	readyItems := readyAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(readyItems) != 1 {
		t.Fatalf("expected dead-letter path to remain reenqueuable, got %+v", readyItems)
	}
	if readyItems[0].InvalidationID == items[0].InvalidationID {
		t.Fatalf("expected a new invalidation row after dead-letter, got %+v", readyItems[0])
	}

	listAny, rpcErr := callWorkspaceMemoryInvalidationListRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationListParams{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		IncludeDeadLetter: true,
		Limit:             10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationList rpc error: %+v", rpcErr)
	}
	listItems := listAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	if len(listItems) != 2 {
		t.Fatalf("expected one dead-letter row plus one open row, got %+v", listItems)
	}
}

func TestWorkspaceMemoryInvalidationPollRequiresWorkspaceAndAgent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	raw, err := json.Marshal(workspaceMemoryInvalidationPollParams{})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, raw); rpcErr == nil {
		t.Fatal("expected missing workspace_id / agent_id error")
	}
}

func seedHandlerOpenMemoryInvalidation(t *testing.T, workspaceID, agentID, docKey string) (*sqlite.Store, *Handler, context.Context, string, string, sqlite.MemoryInvalidationRecord) {
	t.Helper()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       docKey,
		Content:     "# " + docKey + "\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:" + workspaceID,
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       docKey,
		Content:     "# " + docKey + "\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("seed poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one seeded invalidation, got %+v", items)
	}
	return store, h, ctx, workspaceID, agentID, items[0]
}

func assertHandlerMemoryInvalidationDependencyVector(t *testing.T, got []sqlite.MemoryResidencyVersionGuard, want ...sqlite.MemoryResidencyVersionGuard) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("expected dependency revision vector len %d, got %+v", len(want), got)
	}
	for idx := range want {
		if got[idx].RefKind != want[idx].RefKind ||
			got[idx].RefID != want[idx].RefID ||
			got[idx].VersionToken != want[idx].VersionToken ||
			got[idx].Weight != want[idx].Weight {
			t.Fatalf("expected dependency revision vector[%d] %+v, got %+v", idx, want[idx], got[idx])
		}
	}
}

func getHandlerMemoryInvalidationQueueStringColumn(t *testing.T, store *sqlite.Store, invalidationID, column string) string {
	t.Helper()

	query := fmt.Sprintf("SELECT %s FROM memory_invalidation_queue WHERE invalidation_id = ?", column)
	var value string
	if err := store.DB().QueryRowContext(context.Background(), query, invalidationID).Scan(&value); err != nil {
		t.Fatalf("query %s for invalidation %s: %v", column, invalidationID, err)
	}
	return value
}

func setHandlerMemoryInvalidationQueueStringColumn(t *testing.T, store *sqlite.Store, invalidationID, column, value string) {
	t.Helper()

	query := fmt.Sprintf("UPDATE memory_invalidation_queue SET %s = ?, updated_at = ? WHERE invalidation_id = ?", column)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := store.DB().ExecContext(context.Background(), query, value, now, invalidationID)
	if err != nil {
		t.Fatalf("update %s for invalidation %s: %v", column, invalidationID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected for %s on invalidation %s: %v", column, invalidationID, err)
	}
	if rowsAffected != 1 {
		t.Fatalf("expected one invalidation row updated for %s on %s, got %d", column, invalidationID, rowsAffected)
	}
}

func redeliverHandlerMemoryInvalidationForTest(t *testing.T, store *sqlite.Store, h *Handler, ctx context.Context, workspaceID, agentID, invalidationID string) sqlite.MemoryInvalidationRecord {
	t.Helper()

	setHandlerMemoryInvalidationQueueStringColumn(t, store, invalidationID, "next_delivery_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))
	pollAny, rpcErr := callWorkspaceMemoryInvalidationPollRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationPollParams{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}))
	if rpcErr != nil {
		t.Fatalf("redeliver invalidation %s rpc error: %+v", invalidationID, rpcErr)
	}
	items := pollAny.(map[string]any)["items"].([]sqlite.MemoryInvalidationRecord)
	for _, item := range items {
		if item.InvalidationID == invalidationID {
			return item
		}
	}
	t.Fatalf("expected invalidation %s to be redelivered, got %+v", invalidationID, items)
	return sqlite.MemoryInvalidationRecord{}
}

func expectMemoryInvalidationEvent(t *testing.T, ch chan EventMessage, eventType string) EventMessage {
	t.Helper()
	select {
	case msg := <-ch:
		if msg.Type != eventType {
			t.Fatalf("expected event %s, got %+v", eventType, msg)
		}
		return msg
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", eventType)
		return EventMessage{}
	}
}
