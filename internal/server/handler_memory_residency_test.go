package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryResidencyReportListAndGet(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-residency"
		agentID     = "agent-handler-memory-residency"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Residency",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Residency Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawReport, err := json.Marshal(sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "kernel:handler"},
			{ResidencyTier: "P2", ReplicaKind: "memory_node", CoherenceClass: "A", State: "STALE", CanonicalMemoryID: "memnode:workspace_memory:test"},
		},
	})
	if err != nil {
		t.Fatalf("marshal report params: %v", err)
	}
	reportAny, rpcErr := callWorkspaceMemoryResidencyReportRaw(t, h, ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryResidencyReport rpc error: %+v", rpcErr)
	}
	reportPayload := reportAny.(map[string]any)
	if reportPayload["status"] != "RECORDED" {
		t.Fatalf("unexpected report payload %+v", reportPayload)
	}
	detail, ok := reportPayload["report"].(sqlite.MemoryResidencyReportDetail)
	if !ok {
		t.Fatalf("unexpected report detail type %T", reportPayload["report"])
	}
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected report detail time authority, got %+v", detail.TimeAuthority)
	}
	if detail.Report.AgentID != agentID || detail.Report.ReplicaCount != 2 {
		t.Fatalf("unexpected stored report %+v", detail.Report)
	}
	liveEvent := expectEvent(t, ch, "memory.residency_reported")
	persisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.residency_reported",
		EntityType:  "memory_residency",
		EntityID:    detail.Report.ReportID,
		Limit:       1,
	})
	if liveEvent.EventID != persisted.EventID || liveEvent.IngestSeq != persisted.IngestSeq {
		t.Fatalf("expected live memory residency event to mirror persisted runtime envelope, live=%+v persisted=%+v", liveEvent, persisted)
	}
	if liveEvent.EntityType != persisted.EntityType || liveEvent.EntityID != persisted.EntityID {
		t.Fatalf("expected live memory residency entity envelope to match runtime journal, live=%+v persisted=%+v", liveEvent, persisted)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), persisted.PayloadJSON)

	rawList, err := json.Marshal(workspaceMemoryResidencyListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listAny, rpcErr := callWorkspaceMemoryResidencyListRaw(t, h, ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryResidencyList rpc error: %+v", rpcErr)
	}
	listPayload := listAny.(map[string]any)
	listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected residency list time authority, got %+v", listPayload["time_authority"])
	}
	items, ok := listPayload["items"].([]sqlite.MemoryResidencyReportRecord)
	if !ok {
		t.Fatalf("unexpected list items type %T", listPayload["items"])
	}
	if len(items) != 1 || items[0].ReportID != detail.Report.ReportID {
		t.Fatalf("unexpected list payload %+v", listPayload)
	}

	rawGet, err := json.Marshal(workspaceMemoryResidencyGetParams{
		WorkspaceID: workspaceID,
		ReportID:    detail.Report.ReportID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	getAny, rpcErr := callWorkspaceMemoryResidencyGetRaw(t, h, ctx, rawGet)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryResidencyGet rpc error: %+v", rpcErr)
	}
	getDetail := getAny.(sqlite.MemoryResidencyReportDetail)
	if getDetail.TimeAuthority.WorkspaceID != workspaceID || getDetail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected get detail time authority, got %+v", getDetail.TimeAuthority)
	}
	if len(getDetail.Replicas) != 2 || getDetail.Report.ReportID != detail.Report.ReportID {
		t.Fatalf("unexpected get detail %+v", getDetail)
	}
}

func TestWorkspaceMemoryResidencyListOrdersLatestReportFirst(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-residency-order"
		agentID     = "agent-handler-memory-residency-order"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Residency Order",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Order Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	write := func(reportID, summary string) {
		raw, err := json.Marshal(sqlite.MemoryResidencyReportInput{
			ReportID:    reportID,
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			ReportScope: "agent",
			Replicas: []sqlite.MemoryReplicaStateInput{
				{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: reportID},
			},
		})
		if err != nil {
			t.Fatalf("marshal residency report params: %v", err)
		}
		result, rpcErr := callWorkspaceMemoryResidencyReportRaw(t, h, ctx, raw)
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryResidencyReport rpc error: %+v", rpcErr)
		}
		payload, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("unexpected report result type %T", result)
		}
		if payload["status"] != "RECORDED" {
			t.Fatalf("unexpected report payload %+v", payload)
		}
		report, ok := payload["report"].(sqlite.MemoryResidencyReportDetail)
		if !ok {
			t.Fatalf("unexpected report detail type %T", payload["report"])
		}
		if report.Report.ReportID != reportID || report.Report.AgentID != agentID {
			t.Fatalf("unexpected report detail %+v", report)
		}
	}

	write("report-a", "first residency snapshot")
	write("report-b", "second residency snapshot")

	rawList, err := json.Marshal(workspaceMemoryResidencyListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listAny, rpcErr := callWorkspaceMemoryResidencyListRaw(t, h, ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryResidencyList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list result type %T", listAny)
	}
	items, ok := listPayload["items"].([]sqlite.MemoryResidencyReportRecord)
	if !ok {
		t.Fatalf("unexpected list items type %T", listPayload["items"])
	}
	if len(items) != 2 || items[0].ReportID != "report-b" || items[1].ReportID != "report-a" {
		t.Fatalf("expected latest residency report first, got %+v", items)
	}
}

func TestWorkspaceMemoryResidencyReportPublishesEnqueuedInvalidationsBeforeResidencyEvent(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-residency-invalidations"
		agentID     = "agent-handler-memory-residency-invalidations"
		docKey      = "memory-residency-live-doc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Residency Invalidations",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Residency Invalidations Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Residency Doc",
		Content:     "# Doc\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Residency Doc",
		Content:     "# Doc\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawReport, err := json.Marshal(sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
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
	})
	if err != nil {
		t.Fatalf("marshal report params: %v", err)
	}
	reportAny, rpcErr := callWorkspaceMemoryResidencyReportRaw(t, h, ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryResidencyReport rpc error: %+v", rpcErr)
	}
	reportPayload := reportAny.(map[string]any)
	detail := reportPayload["report"].(sqlite.MemoryResidencyReportDetail)

	invalidationLive := expectEvent(t, ch, "memory.invalidation_enqueued")
	invalidationPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       1,
	})
	if invalidationLive.EventID != invalidationPersisted.EventID || invalidationLive.IngestSeq != invalidationPersisted.IngestSeq {
		t.Fatalf("expected invalidation enqueue live event to mirror persisted runtime envelope, live=%+v persisted=%+v", invalidationLive, invalidationPersisted)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, invalidationLive.PayloadJSON), invalidationPersisted.PayloadJSON)

	residencyLive := expectEvent(t, ch, "memory.residency_reported")
	residencyPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.residency_reported",
		EntityType:  "memory_residency",
		EntityID:    detail.Report.ReportID,
		Limit:       1,
	})
	if residencyLive.EventID != residencyPersisted.EventID || residencyLive.IngestSeq != residencyPersisted.IngestSeq {
		t.Fatalf("expected residency live event to mirror persisted runtime envelope, live=%+v persisted=%+v", residencyLive, residencyPersisted)
	}
	if invalidationPersisted.IngestSeq >= residencyPersisted.IngestSeq {
		t.Fatalf("expected invalidation enqueue to precede residency report in runtime journal, invalidation=%+v residency=%+v", invalidationPersisted, residencyPersisted)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, residencyLive.PayloadJSON), residencyPersisted.PayloadJSON)
}

func TestWorkspaceMemoryResidencyReportPublishesRefreshEventBeforeResidencyEvent(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-residency-refresh"
		agentID     = "agent-handler-memory-residency-refresh"
		docKey      = "memory-residency-refresh-doc"
		reportID    = "memres-open-refresh"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Residency Refresh",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Residency Refresh Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Residency Refresh Doc",
		Content:     "# Doc\nVersion A",
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
		Subject:     "residency refresh evidence",
		Body:        "Current claim guard for refresh event coverage.",
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
				CacheKey:       "packet:refresh",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Residency Refresh Doc",
		Content:     "# Doc\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	initialItems, err := store.ListMemoryInvalidations(ctx, sqlite.MemoryInvalidationListFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list invalidations after initial report: %v", err)
	}
	if len(initialItems) != 1 {
		t.Fatalf("expected one open invalidation after initial report, got %+v", initialItems)
	}
	initialID := initialItems[0].InvalidationID

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	secondReport := mustJSONRaw(sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportID:    reportID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:refresh",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	})
	reportAny, rpcErr := callWorkspaceMemoryResidencyReportRaw(t, h, ctx, secondReport)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryResidencyReport refresh rpc error: %+v", rpcErr)
	}
	reportPayload := reportAny.(map[string]any)
	detail := reportPayload["report"].(sqlite.MemoryResidencyReportDetail)

	refreshLive := expectEvent(t, ch, "memory.invalidation_refreshed")
	refreshPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_refreshed",
		EntityType:  "memory_invalidation",
		EntityID:    initialID,
		Limit:       1,
	})
	if refreshLive.EventID != refreshPersisted.EventID || refreshLive.IngestSeq != refreshPersisted.IngestSeq {
		t.Fatalf("expected invalidation refresh live event to mirror persisted runtime envelope, live=%+v persisted=%+v", refreshLive, refreshPersisted)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, refreshLive.PayloadJSON), refreshPersisted.PayloadJSON)

	residencyLive := expectEvent(t, ch, "memory.residency_reported")
	residencyPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.residency_reported",
		EntityType:  "memory_residency",
		EntityID:    detail.Report.ReportID,
		Limit:       1,
	})
	if residencyLive.EventID != residencyPersisted.EventID || residencyLive.IngestSeq != residencyPersisted.IngestSeq {
		t.Fatalf("expected residency live event to mirror persisted runtime envelope, live=%+v persisted=%+v", residencyLive, residencyPersisted)
	}
	if refreshPersisted.IngestSeq >= residencyPersisted.IngestSeq {
		t.Fatalf("expected invalidation refresh to precede residency report in runtime journal, refresh=%+v residency=%+v", refreshPersisted, residencyPersisted)
	}

	getAny, rpcErr := callWorkspaceMemoryInvalidationGetRaw(t, h, ctx, mustJSONRaw(workspaceMemoryInvalidationGetParams{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		InvalidationID: initialID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryInvalidationGet refresh rpc error: %+v", rpcErr)
	}
	item := getAny.(sqlite.MemoryInvalidationRecord)
	if len(item.DependencyRevisionVector) != 2 ||
		item.DependencyRevisionVector[0].RefKind != "knowledge_claim" ||
		item.DependencyRevisionVector[0].RefID != claim.ClaimID ||
		item.DependencyRevisionVector[1].RefKind != "workspace_doc" ||
		item.DependencyRevisionVector[1].RefID != docKey {
		t.Fatalf("expected refreshed invalidation get payload to surface widened lineage, got %+v", item.DependencyRevisionVector)
	}
}

func TestWorkspaceMemoryResidencyReportRequiresAgentAndWorkspace(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	raw, err := json.Marshal(sqlite.MemoryResidencyReportInput{
		WorkspaceID: "",
		AgentID:     "",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryResidencyReportRaw(t, h, ctx, raw); rpcErr == nil {
		t.Fatal("expected missing workspace_id / agent_id error")
	}
}

func TestWorkspaceMemoryResidencyClassifiesValidationAndInternalErrors(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-residency-errors"
		agentID     = "agent-handler-memory-residency-errors"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Residency Errors",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Residency Error Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	rawInvalid, err := json.Marshal(sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "bogus",
	})
	if err != nil {
		t.Fatalf("marshal invalid params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryResidencyReportRaw(t, h, ctx, rawInvalid); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid_params for invalid scope, got %+v", rpcErr)
	}

	rawMissing, err := json.Marshal(workspaceMemoryResidencyGetParams{
		WorkspaceID: workspaceID,
		ReportID:    "missing",
	})
	if err != nil {
		t.Fatalf("marshal missing get params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryResidencyGetRaw(t, h, ctx, rawMissing); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid_params for missing report, got %+v", rpcErr)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	rawList, err := json.Marshal(workspaceMemoryResidencyListParams{
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryResidencyListRaw(t, h, ctx, rawList); rpcErr == nil || rpcErr.Code != errCodeInternal {
		t.Fatalf("expected internal error for closed store, got %+v", rpcErr)
	}
}

func expectEvent(t *testing.T, ch chan EventMessage, eventType string) EventMessage {
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
