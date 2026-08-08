package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMemoryResidencyRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-memory-residency-authority-metadata"
		agentID     = "agent-d2a-memory-residency-authority-metadata"
		docKey      = "memres-authority-metadata-doc"
	)
	docV1 := seedMemoryResidencyAuthorityMetadataScenario(t, ctx, store, workspaceID, agentID, docKey)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	result, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-authority-metadata",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:memres-authority-metadata",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1, Weight: 1},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, result.Event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.residency_reported",
		EntityType:  "memory_residency",
		EntityID:    result.Report.Report.ReportID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list memory residency runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.residency_reported event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestMemoryResidencyEnqueueRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-memory-residency-enqueue-authority-metadata"
		agentID     = "agent-d2a-memory-residency-enqueue-authority-metadata"
		docKey      = "memres-enqueue-authority-doc"
	)
	docV1 := seedMemoryResidencyAuthorityMetadataScenario(t, ctx, store, workspaceID, agentID, docKey)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	result, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-enqueue-authority-metadata",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:memres-enqueue-authority",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1, Weight: 1},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	if len(result.InvalidationEvents) != 1 {
		t.Fatalf("expected one invalidation event, got %+v", result.InvalidationEvents)
	}
	if result.InvalidationEvents[0].EventType != "memory.invalidation_enqueued" {
		t.Fatalf("expected memory.invalidation_enqueued, got %+v", result.InvalidationEvents[0])
	}

	assertRuntimeEventAuthorityMetadata(t, result.InvalidationEvents[0], authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list memory invalidation runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.invalidation_enqueued event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestMemoryResidencyRefreshRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-memory-residency-refresh-authority-metadata"
		agentID     = "agent-d2a-memory-residency-refresh-authority-metadata"
		docKey      = "memres-refresh-authority-doc"
		reportID    = "memres-refresh-authority-metadata"
	)
	docV1 := seedMemoryResidencyAuthorityMetadataScenario(t, ctx, store, workspaceID, agentID, docKey)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "memory residency refresh authority metadata",
		Body:        "refresh should keep authority metadata on invalidation refresh events",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:memres-refresh-authority",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed initial memory residency: %v", err)
	}

	result, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:memres-refresh-authority",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("report refreshed memory residency: %v", err)
	}
	if len(result.InvalidationEvents) != 1 {
		t.Fatalf("expected one refresh invalidation event, got %+v", result.InvalidationEvents)
	}
	if result.InvalidationEvents[0].EventType != "memory.invalidation_refreshed" {
		t.Fatalf("expected memory.invalidation_refreshed, got %+v", result.InvalidationEvents[0])
	}

	assertRuntimeEventAuthorityMetadata(t, result.InvalidationEvents[0], authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_refreshed",
		EntityType:  "memory_invalidation",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list memory invalidation refresh runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.invalidation_refreshed event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func seedMemoryResidencyAuthorityMetadataScenario(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, docKey string) string {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Memory Residency Authority Metadata Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Memory Residency Authority Metadata Doc",
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
		Title:       "Memory Residency Authority Metadata Doc",
		Content:     "# Doc\nVersion B",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc v2: %v", err)
	}
	return docV1.SHA
}
