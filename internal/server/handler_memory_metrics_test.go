package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryMetricsReportListAndGet(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-metrics"
		agentID     = "agent-handler-memory-metrics"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Metrics",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Metrics Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	rawReport, err := json.Marshal(sqlite.MemoryMetricsReportInput{
		WorkspaceID:             workspaceID,
		AgentID:                 agentID,
		ReportScope:             "agent",
		LookupCount:             12,
		L1HitCount:              6,
		L2HitCount:              3,
		P3HitCount:              1,
		StaleHitCount:           2,
		PromotionCount:          4,
		PromotionReuseCount:     3,
		FlushCount:              2,
		FlushPositiveCount:      1,
		LocalConsolidationCount: 2,
		PotentialSharedOpCount:  20,
	})
	if err != nil {
		t.Fatalf("marshal report params: %v", err)
	}
	reportAny, rpcErr := callWorkspaceMemoryMetricsReportRaw(t, h, ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryMetricsReport rpc error: %+v", rpcErr)
	}
	reportPayload := reportAny.(map[string]any)
	if reportPayload["status"] != "RECORDED" {
		t.Fatalf("unexpected report payload %+v", reportPayload)
	}
	record, ok := reportPayload["report"].(sqlite.MemoryMetricsReportRecord)
	if !ok {
		t.Fatalf("unexpected report record type %T", reportPayload["report"])
	}
	if record.AgentID != agentID || record.TotalHitCount != 10 {
		t.Fatalf("unexpected report record %+v", record)
	}
	if record.TimeAuthority.WorkspaceID != workspaceID || record.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected report time authority, got %+v", record.TimeAuthority)
	}
	liveEvent := expectEvent(t, ch, "memory.metrics_reported")
	persisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.metrics_reported",
		EntityType:  "memory_metrics",
		EntityID:    record.ReportID,
		Limit:       1,
	})
	if liveEvent.EventID != persisted.EventID || liveEvent.IngestSeq != persisted.IngestSeq {
		t.Fatalf("expected live memory metrics event to mirror persisted runtime envelope, live=%+v persisted=%+v", liveEvent, persisted)
	}
	if liveEvent.EntityType != persisted.EntityType || liveEvent.EntityID != persisted.EntityID {
		t.Fatalf("expected live memory metrics entity envelope to match runtime journal, live=%+v persisted=%+v", liveEvent, persisted)
	}
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), persisted.PayloadJSON)

	rawList, err := json.Marshal(workspaceMemoryMetricsListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listAny, rpcErr := callWorkspaceMemoryMetricsListRaw(t, h, ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryMetricsList rpc error: %+v", rpcErr)
	}
	listPayload := listAny.(map[string]any)
	items, ok := listPayload["items"].([]sqlite.MemoryMetricsReportRecord)
	if !ok {
		t.Fatalf("unexpected list items type %T", listPayload["items"])
	}
	if len(items) != 1 || items[0].ReportID != record.ReportID {
		t.Fatalf("unexpected list payload %+v", listPayload)
	}
	listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected list time authority, got %+v", listPayload["time_authority"])
	}
	if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected list item time authority, got %+v", items[0].TimeAuthority)
	}

	rawGet, err := json.Marshal(workspaceMemoryMetricsGetParams{
		WorkspaceID: workspaceID,
		ReportID:    record.ReportID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	getAny, rpcErr := callWorkspaceMemoryMetricsGetRaw(t, h, ctx, rawGet)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryMetricsGet rpc error: %+v", rpcErr)
	}
	getRecord := getAny.(sqlite.MemoryMetricsReportRecord)
	if getRecord.ReportID != record.ReportID || getRecord.PromotionPrecision <= 0 {
		t.Fatalf("unexpected get record %+v", getRecord)
	}
	if getRecord.TimeAuthority.WorkspaceID != workspaceID || getRecord.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected get time authority, got %+v", getRecord.TimeAuthority)
	}
}

func TestWorkspaceMemoryMetricsReportExposesWorkspaceTimeAuthority(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-metrics-time-authority"
		agentID     = "agent-handler-memory-metrics-time-authority"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Metrics Time Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Metrics Time Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	rawReport, err := json.Marshal(sqlite.MemoryMetricsReportInput{
		WorkspaceID:             workspaceID,
		AgentID:                 agentID,
		ReportScope:             "agent",
		LookupCount:             10,
		L1HitCount:              5,
		L2HitCount:              3,
		P3HitCount:              1,
		StaleHitCount:           1,
		PromotionCount:          2,
		PromotionReuseCount:     1,
		FlushCount:              1,
		FlushPositiveCount:      1,
		LocalConsolidationCount: 1,
		PotentialSharedOpCount:  12,
	})
	if err != nil {
		t.Fatalf("marshal report params: %v", err)
	}
	reportAny, rpcErr := callWorkspaceMemoryMetricsReportRaw(t, h, ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryMetricsReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := reportAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", reportAny)
	}
	reportRecord, ok := reportPayload["report"].(sqlite.MemoryMetricsReportRecord)
	if !ok {
		t.Fatalf("unexpected report record type %T", reportPayload["report"])
	}
	if reportRecord.TimeAuthority.WorkspaceID != workspaceID || reportRecord.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected report time authority, got %+v", reportRecord.TimeAuthority)
	}

	rawList, err := json.Marshal(workspaceMemoryMetricsListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listAny, rpcErr := callWorkspaceMemoryMetricsListRaw(t, h, ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryMetricsList rpc error: %+v", rpcErr)
	}
	listPayload, ok := listAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list result type %T", listAny)
	}
	listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected list time authority, got %+v", listPayload["time_authority"])
	}
	items, ok := listPayload["items"].([]sqlite.MemoryMetricsReportRecord)
	if !ok || len(items) != 1 {
		t.Fatalf("unexpected list payload %+v", listPayload)
	}
	if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected list item time authority, got %+v", items[0].TimeAuthority)
	}

	rawGet, err := json.Marshal(workspaceMemoryMetricsGetParams{
		WorkspaceID: workspaceID,
		ReportID:    reportRecord.ReportID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	getAny, rpcErr := callWorkspaceMemoryMetricsGetRaw(t, h, ctx, rawGet)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryMetricsGet rpc error: %+v", rpcErr)
	}
	getRecord, ok := getAny.(sqlite.MemoryMetricsReportRecord)
	if !ok {
		t.Fatalf("unexpected get result type %T", getAny)
	}
	if getRecord.TimeAuthority.WorkspaceID != workspaceID || getRecord.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected get time authority, got %+v", getRecord.TimeAuthority)
	}
}

func TestWorkspaceMemoryMetricsClassifiesValidationAndInternalErrors(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-metrics-errors"
		agentID     = "agent-handler-memory-metrics-errors"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Metrics Errors",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Metrics Error Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	rawInvalid, err := json.Marshal(sqlite.MemoryMetricsReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		LookupCount: 1,
		L1HitCount:  2,
	})
	if err != nil {
		t.Fatalf("marshal invalid params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryMetricsReportRaw(t, h, ctx, rawInvalid); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid_params for invalid counts, got %+v", rpcErr)
	}

	rawMissing, err := json.Marshal(workspaceMemoryMetricsGetParams{
		WorkspaceID: workspaceID,
		ReportID:    "missing",
	})
	if err != nil {
		t.Fatalf("marshal missing get params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryMetricsGetRaw(t, h, ctx, rawMissing); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid_params for missing report, got %+v", rpcErr)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	rawList, err := json.Marshal(workspaceMemoryMetricsListParams{
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	if _, rpcErr := callWorkspaceMemoryMetricsListRaw(t, h, ctx, rawList); rpcErr == nil || rpcErr.Code != errCodeInternal {
		t.Fatalf("expected internal error for closed store, got %+v", rpcErr)
	}
}
