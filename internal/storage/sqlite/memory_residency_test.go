package sqlite

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
)

func TestReportMemoryResidencyPersistsReportReplicasAndRuntimeEvent(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-residency"
		agentID     = "agent-memory-residency"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Residency Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nCanonical memory grounding.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	memoryRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Prefer canonical memory",
		Body:        "Use grounded memory packets.",
		Summary:     "Prefer canonical memory.",
		AgentID:     agentID,
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := memoryGraphNodeID("workspace_memory", memoryRecord.MemoryID)
	doc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc: %v", err)
	}

	result, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		ReportScope:       "agent",
		HotHitRate:        0.71,
		PersistentHitRate: 0.54,
		OffloadRatio:      0.62,
		Notes: map[string]any{
			"kernel_packet": "warm",
		},
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P1",
				ReplicaKind:    "kernel_packet",
				CoherenceClass: "C",
				State:          "CURRENT",
				CacheKey:       "kernel:task-1",
				HitCount:       7,
				Metadata: map[string]any{
					"lane": "deterministic",
				},
			},
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "STALE",
				CanonicalMemoryID: nodeID,
				SourceKind:        "workspace_doc",
				SourceID:          docKey,
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: doc.SHA, Weight: 1, State: "CURRENT"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	if result.Report.Report.WorkspaceID != workspaceID || result.Report.Report.AgentID != agentID {
		t.Fatalf("unexpected report payload %+v", result.Report)
	}
	if result.Report.TimeAuthority.WorkspaceID != workspaceID || result.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected residency report time authority, got %+v", result.Report.TimeAuthority)
	}
	if result.Report.Report.P1EntryCount != 1 || result.Report.Report.P2EntryCount != 1 || result.Report.Report.ReplicaCount != 2 {
		t.Fatalf("expected tier counts derived from replicas, got %+v", result.Report.Report)
	}
	if len(result.Report.Replicas) != 2 {
		t.Fatalf("expected replicas in detail, got %+v", result.Report)
	}
	if result.Event.EventType != "memory.residency_reported" || result.Event.EntityType != "memory_residency" {
		t.Fatalf("unexpected runtime event %+v", result.Event)
	}

	stored, err := store.GetMemoryResidencyReport(ctx, workspaceID, result.Report.Report.ReportID)
	if err != nil {
		t.Fatalf("get memory residency report: %v", err)
	}
	if stored.Report.Notes["kernel_packet"] != "warm" {
		t.Fatalf("expected notes to round-trip, got %+v", stored.Report)
	}
	if stored.TimeAuthority.WorkspaceID != workspaceID || stored.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected stored residency time authority, got %+v", stored.TimeAuthority)
	}
	if !containsResidencyReplica(stored.Replicas, "P2", "MEMORY_NODE", nodeID) {
		t.Fatalf("expected stored canonical memory replica, got %+v", stored.Replicas)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.residency_reported",
		EntityType:  "memory_residency",
		EntityID:    result.Report.Report.ReportID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one residency runtime event, got %+v", events)
	}
	payload := decodeMemoryResidencyRuntimePayload(t, events[0].PayloadJSON)
	if payload["typed_event_type"] != "MEMORY_RESIDENCY_REPORT" || payload["replica_count"] != float64(2) {
		t.Fatalf("unexpected runtime payload %+v", payload)
	}
}

func TestListMemoryResidencyReportsOrdersLatestReportFirst(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-residency-order"
		agentID     = "agent-memory-order"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency Order",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Order Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	for _, reportID := range []string{"report-a", "report-b"} {
		if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
			ReportID:    reportID,
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			ReportScope: "agent",
			Replicas: []MemoryReplicaStateInput{
				{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "kernel:" + reportID},
			},
		}); err != nil {
			t.Fatalf("report memory residency %s: %v", reportID, err)
		}
	}

	items, err := store.ListMemoryResidencyReports(ctx, MemoryResidencyReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory residency reports: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected two residency reports, got %+v", items)
	}
	if items[0].ReportID != "report-b" || items[1].ReportID != "report-a" {
		t.Fatalf("expected latest residency report first, got %+v", items)
	}
}

func TestReportMemoryResidencyUpsertReplacesReplicaSnapshot(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-residency-replace"
		agentID     = "agent-memory-replace"
		reportID    = "memres-fixed"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency Replace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replace Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "kernel:first"},
			{ResidencyTier: "P2", ReplicaKind: "query_cache", CoherenceClass: "C", State: "CURRENT", CacheKey: "query:first"},
		},
	}); err != nil {
		t.Fatalf("first memory residency report: %v", err)
	}

	result, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{ResidencyTier: "P2", ReplicaKind: "memory_node", CoherenceClass: "A", State: "INVALIDATED", CanonicalMemoryID: "memnode:workspace_memory:replacement"},
		},
	})
	if err != nil {
		t.Fatalf("second memory residency report: %v", err)
	}
	if result.Report.Report.InvalidatedReplicaCount != 1 || result.Report.Report.ReplicaCount != 1 {
		t.Fatalf("expected replaced invalidated snapshot, got %+v", result.Report.Report)
	}

	stored, err := store.GetMemoryResidencyReport(ctx, workspaceID, reportID)
	if err != nil {
		t.Fatalf("get replaced report: %v", err)
	}
	if len(stored.Replicas) != 1 || stored.Replicas[0].ReplicaKind != "MEMORY_NODE" {
		t.Fatalf("expected replica snapshot replacement, got %+v", stored.Replicas)
	}
	if stored.TimeAuthority.WorkspaceID != workspaceID || stored.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected replacement report time authority, got %+v", stored.TimeAuthority)
	}
}

func TestListMemoryResidencyReportsFiltersByAgentAndScope(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-residency-list"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency List",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-b",
		AgentID:     "agent-b",
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-23T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session sess-b: %v", err)
	}
	for _, input := range []MemoryResidencyReportInput{
		{ReportID: "r1", WorkspaceID: workspaceID, AgentID: "agent-a", ReportScope: "agent", Replicas: []MemoryReplicaStateInput{{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT"}}},
		{ReportID: "r2", WorkspaceID: workspaceID, AgentID: "agent-b", ReportScope: "session", SessionID: "sess-b", Replicas: []MemoryReplicaStateInput{{ResidencyTier: "P2", ReplicaKind: "memory_node", CoherenceClass: "A", State: "CURRENT"}}},
	} {
		if _, err := store.ReportMemoryResidency(ctx, input); err != nil {
			t.Fatalf("report memory residency %s: %v", input.ReportID, err)
		}
	}

	items, err := store.ListMemoryResidencyReports(ctx, MemoryResidencyReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		ReportScope: "session",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory residency reports: %v", err)
	}
	if len(items) != 1 || items[0].ReportID != "r2" || items[0].SessionID != "sess-b" {
		t.Fatalf("unexpected filtered reports %+v", items)
	}
}

func TestReportMemoryResidencyRejectsInvalidScopeAndCrossOwnerReuse(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-residency-validation"
		agentA      = "agent-memory-residency-a"
		agentB      = "agent-memory-residency-b"
		reportID    = "memres-owner-lock"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{agentA, agentB} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentA,
		ReportScope: "bogus",
	}); err == nil {
		t.Fatal("expected invalid report_scope error")
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentA,
		ReportScope: "session",
	}); err == nil {
		t.Fatal("expected session_id requirement for SESSION report_scope")
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-agent-a",
		AgentID:     agentA,
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-23T10:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentB,
		SessionID:   "sess-agent-a",
		ReportScope: "session",
	}); err == nil {
		t.Fatal("expected session ownership validation to fail")
	}
	if _, err := store.ListMemoryResidencyReports(ctx, MemoryResidencyReportFilter{
		WorkspaceID: workspaceID,
		ReportScope: "bogus",
	}); err == nil {
		t.Fatal("expected invalid scope filter error")
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentA,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "bogus_ref", RefID: "doc", VersionToken: "v1", State: "CURRENT"},
				},
			},
		},
	}); err == nil {
		t.Fatal("expected invalid version_guard.ref_kind error")
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentA,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc", VersionToken: "v1", State: "mystery"},
				},
			},
		},
	}); err == nil {
		t.Fatal("expected invalid version_guard.state error")
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentA,
		Replicas: []MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "kernel:a"},
		},
	}); err != nil {
		t.Fatalf("seed memory residency report: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentB,
		Replicas: []MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "kernel:b"},
		},
	}); err == nil {
		t.Fatal("expected cross-owner report_id reuse to fail")
	}
}

func TestReportMemoryResidencyUpsertKeepsDeterministicReplicaStateIDs(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-residency-deterministic"
		agentID     = "agent-memory-residency-deterministic"
		reportID    = "memres-deterministic"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency Deterministic",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Deterministic Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	first, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "kernel:hot", HitCount: 2},
			{ResidencyTier: "P2", ReplicaKind: "memory_node", CoherenceClass: "A", State: "STALE", CanonicalMemoryID: "memnode:workspace_memory:123"},
		},
	})
	if err != nil {
		t.Fatalf("first memory residency report: %v", err)
	}
	second, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{ResidencyTier: "P2", ReplicaKind: "memory_node", CoherenceClass: "A", State: "STALE", CanonicalMemoryID: "memnode:workspace_memory:123"},
			{ResidencyTier: "P1", ReplicaKind: "kernel_packet", CoherenceClass: "C", State: "CURRENT", CacheKey: "kernel:hot", HitCount: 9},
		},
	})
	if err != nil {
		t.Fatalf("second memory residency report: %v", err)
	}

	firstIDs := replicaIdentityMap(first.Report.Replicas)
	secondIDs := replicaIdentityMap(second.Report.Replicas)
	if len(firstIDs) != len(secondIDs) {
		t.Fatalf("expected same replica cardinality, got %d vs %d", len(firstIDs), len(secondIDs))
	}
	for key, firstRecord := range firstIDs {
		secondRecord, ok := secondIDs[key]
		if !ok {
			t.Fatalf("expected replica key %s in second snapshot, got %+v", key, second.Report.Replicas)
		}
		if firstRecord.ReplicaStateID != secondRecord.ReplicaStateID {
			t.Fatalf("expected stable replica_state_id for %s, got %s then %s", key, firstRecord.ReplicaStateID, secondRecord.ReplicaStateID)
		}
		if firstRecord.CreatedAt != secondRecord.CreatedAt {
			t.Fatalf("expected created_at to stay stable for %s, got %s then %s", key, firstRecord.CreatedAt, secondRecord.CreatedAt)
		}
	}
}

func decodeMemoryResidencyRuntimePayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime payload: %v", err)
	}
	return payload
}

func containsResidencyReplica(items []MemoryReplicaStateRecord, tier, kind, canonicalMemoryID string) bool {
	for _, item := range items {
		if item.ResidencyTier == tier && item.ReplicaKind == kind && item.CanonicalMemoryID == canonicalMemoryID {
			return true
		}
	}
	return false
}

func replicaIdentityMap(items []MemoryReplicaStateRecord) map[string]MemoryReplicaStateRecord {
	keys := make([]string, 0, len(items))
	out := make(map[string]MemoryReplicaStateRecord, len(items))
	for _, item := range items {
		key := residencyReplicaIdentity(item)
		keys = append(keys, key)
		out[key] = item
	}
	sort.Strings(keys)
	return out
}

func residencyReplicaIdentity(item MemoryReplicaStateRecord) string {
	return item.ResidencyTier + "|" + item.ReplicaKind + "|" + item.CanonicalMemoryID + "|" + item.CacheKey + "|" + item.SourceKind + "|" + item.SourceID
}
