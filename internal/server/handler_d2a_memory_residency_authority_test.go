package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryResidencyReportRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID, docKey, staleDocToken := seedHandlerMemoryResidencyAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-residency-missing-authority", "agent-handler-d2a-memory-residency-missing-authority", "handler-d2a-memory-residency-doc")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-handler-d2a-missing-authority",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:handler-d2a-missing-authority",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: staleDocToken, Weight: 1},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal memory residency report params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryResidencyReport(testAuthContext(workspaceID, "agent", agentID), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.memory.residency.report")

	assertNoHandlerMemoryResidencyReports(t, ctx, store, workspaceID, agentID)
	assertNoHandlerMemoryResidencyEvents(t, ctx, store, workspaceID, "memory.residency_reported")
	assertNoHandlerMemoryResidencyEvents(t, ctx, store, workspaceID, "memory.invalidation_enqueued")
	assertNoHandlerMemoryResidencyEvents(t, ctx, store, workspaceID, "memory.invalidation_refreshed")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceMemoryResidencyReportRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID, docKey, staleDocToken := seedHandlerMemoryResidencyAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-residency-stale-authority", "agent-handler-d2a-memory-residency-stale-authority", "handler-d2a-memory-residency-stale-doc")
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3701")

	raw, err := json.Marshal(sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-handler-d2a-stale-authority",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:handler-d2a-stale-authority",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: staleDocToken, Weight: 1},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal memory residency report params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryResidencyReport(testAuthContext(workspaceID, "agent", agentID), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.residency.report")

	assertNoHandlerMemoryResidencyReports(t, ctx, store, workspaceID, agentID)
	assertNoHandlerMemoryResidencyEvents(t, ctx, store, workspaceID, "memory.residency_reported")
	assertNoHandlerMemoryResidencyEvents(t, ctx, store, workspaceID, "memory.invalidation_enqueued")
	assertNoHandlerMemoryResidencyEvents(t, ctx, store, workspaceID, "memory.invalidation_refreshed")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale-authority reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceMemoryResidencyReportRejectsAgentPrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID, _, _ := seedHandlerMemoryResidencyAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-residency-agent-mismatch", "agent-handler-d2a-memory-residency-agent-mismatch", "handler-d2a-memory-residency-agent-mismatch-doc")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	result, rpcErr := h.workspaceMemoryResidencyReport(withTestAgentPrincipal(context.Background(), workspaceID, "agent-other"), mustJSONRaw(sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-handler-d2a-agent-mismatch",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "packet:agent-mismatch"},
		},
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

	assertNoHandlerMemoryResidencyReports(t, ctx, store, workspaceID, agentID)
	assertNoHandlerMemoryResidencyEvents(t, ctx, store, workspaceID, "memory.residency_reported")
}

func TestWorkspaceMemoryResidencyListRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID, _, _ := seedHandlerMemoryResidencyAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-residency-list-mismatch", "agent-handler-d2a-memory-residency-list-mismatch", "handler-d2a-memory-residency-list-doc")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-handler-d2a-list-mismatch",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "packet:list-mismatch"},
		},
	}); err != nil {
		t.Fatalf("seed memory residency report: %v", err)
	}

	result, rpcErr := h.workspaceMemoryResidencyList(testAuthContext("ws-other-memory-residency", "human", "developer"), mustJSONRaw(workspaceMemoryResidencyListParams{
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

func TestWorkspaceMemoryResidencyGetRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID, agentID, _, _ := seedHandlerMemoryResidencyAuthorityScenario(t, ctx, store, "ws-handler-d2a-memory-residency-get-mismatch", "agent-handler-d2a-memory-residency-get-mismatch", "handler-d2a-memory-residency-get-doc")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	report, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-handler-d2a-get-mismatch",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "packet:get-mismatch"},
		},
	})
	if err != nil {
		t.Fatalf("seed memory residency report: %v", err)
	}

	result, rpcErr := h.workspaceMemoryResidencyGet(testAuthContext("ws-other-memory-residency", "human", "developer"), mustJSONRaw(workspaceMemoryResidencyGetParams{
		WorkspaceID: workspaceID,
		ReportID:    report.Report.Report.ReportID,
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

func seedHandlerMemoryResidencyAuthorityScenario(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, docKey string) (string, string, string, string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Residency Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Handler Memory Residency Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Handler Memory Residency Authority Doc",
		Content:     "# Doc\nVersion A",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc v1: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Handler Memory Residency Authority Doc",
		Content:     "# Doc\nVersion B",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc v2: %v", err)
	}
	return workspaceID, agentID, docKey, docV1.SHA
}

func assertNoHandlerMemoryResidencyReports(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()
	items, err := store.ListMemoryResidencyReports(ctx, sqlite.MemoryResidencyReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list memory residency reports: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no memory residency reports, got %+v", items)
	}
}

func assertNoHandlerMemoryResidencyEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType string) {
	t.Helper()
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list runtime events for %s: %v", eventType, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s events, got %+v", eventType, events)
	}
}
