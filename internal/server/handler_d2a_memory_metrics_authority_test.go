package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryMetricsReportRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID := seedHandlerMemoryMetricsAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-metrics-missing-authority", "agent-handler-d2a-memory-metrics-missing-authority")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	raw, err := json.Marshal(sqlite.MemoryMetricsReportInput{
		ReportID:               "memmet-handler-d2a-missing-authority",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            6,
		L1HitCount:             3,
		L2HitCount:             2,
		P3HitCount:             1,
		PotentialSharedOpCount: 8,
	})
	if err != nil {
		t.Fatalf("marshal memory metrics report params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryMetricsReport(testAuthContext(workspaceID, "agent", agentID), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.memory.metrics.report")

	assertNoHandlerMemoryMetricsReports(t, ctx, store, workspaceID, agentID)
	assertNoHandlerMemoryMetricsEvents(t, ctx, store, workspaceID, "memory.metrics_reported")
}

func TestWorkspaceMemoryMetricsReportRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID := seedHandlerMemoryMetricsAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-metrics-stale-authority", "agent-handler-d2a-memory-metrics-stale-authority")
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3901")

	raw, err := json.Marshal(sqlite.MemoryMetricsReportInput{
		ReportID:               "memmet-handler-d2a-stale-authority",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            7,
		L1HitCount:             4,
		L2HitCount:             2,
		P3HitCount:             1,
		PotentialSharedOpCount: 9,
	})
	if err != nil {
		t.Fatalf("marshal memory metrics report params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryMetricsReport(testAuthContext(workspaceID, "agent", agentID), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.metrics.report")

	assertNoHandlerMemoryMetricsReports(t, ctx, store, workspaceID, agentID)
	assertNoHandlerMemoryMetricsEvents(t, ctx, store, workspaceID, "memory.metrics_reported")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
}

func TestWorkspaceMemoryMetricsReportRejectsAgentPrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID := seedHandlerMemoryMetricsAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-metrics-agent-mismatch", "agent-handler-d2a-memory-metrics-agent-mismatch")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	result, rpcErr := h.workspaceMemoryMetricsReport(withTestAgentPrincipal(context.Background(), workspaceID, "agent-other"), mustJSONRaw(sqlite.MemoryMetricsReportInput{
		ReportID:               "memmet-handler-d2a-agent-mismatch",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            5,
		L1HitCount:             3,
		L2HitCount:             1,
		P3HitCount:             1,
		PotentialSharedOpCount: 7,
	}))
	if rpcErr == nil {
		t.Fatal("expected permission denied for agent principal mismatch")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}

	assertNoHandlerMemoryMetricsReports(t, ctx, store, workspaceID, agentID)
	assertNoHandlerMemoryMetricsEvents(t, ctx, store, workspaceID, "memory.metrics_reported")
}

func TestWorkspaceMemoryMetricsListRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID := seedHandlerMemoryMetricsAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-metrics-list-mismatch", "agent-handler-d2a-memory-metrics-list-mismatch")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		ReportID:               "memmet-handler-d2a-list-mismatch",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            6,
		L1HitCount:             3,
		L2HitCount:             2,
		P3HitCount:             1,
		PotentialSharedOpCount: 8,
	}); err != nil {
		t.Fatalf("seed memory metrics report: %v", err)
	}

	result, rpcErr := h.workspaceMemoryMetricsList(testAuthContext("ws-other-memory-metrics", "human", "developer"), mustJSONRaw(workspaceMemoryMetricsListParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceMemoryMetricsGetRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID := seedHandlerMemoryMetricsAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-metrics-get-mismatch", "agent-handler-d2a-memory-metrics-get-mismatch")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	report, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		ReportID:               "memmet-handler-d2a-get-mismatch",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            6,
		L1HitCount:             3,
		L2HitCount:             2,
		P3HitCount:             1,
		PotentialSharedOpCount: 8,
	})
	if err != nil {
		t.Fatalf("seed memory metrics report: %v", err)
	}

	result, rpcErr := h.workspaceMemoryMetricsGet(testAuthContext("ws-other-memory-metrics", "human", "developer"), mustJSONRaw(workspaceMemoryMetricsGetParams{
		WorkspaceID: workspaceID,
		ReportID:    report.Report.ReportID,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func seedHandlerMemoryMetricsAuthorityScenario(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) (string, string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Metrics Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Handler Memory Metrics Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	return workspaceID, agentID
}

func assertNoHandlerMemoryMetricsReports(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()

	items, err := store.ListMemoryMetricsReports(ctx, sqlite.MemoryMetricsReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list memory metrics reports: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no memory metrics reports, got %+v", items)
	}
}

func assertNoHandlerMemoryMetricsEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "memory_metrics",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory metrics runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events, got %+v", eventType, events)
	}
}
