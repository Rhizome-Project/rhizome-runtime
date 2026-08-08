package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceDocRefChangeInvalidationEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-doc-ref-change-authority-metadata"
		agentID     = "agent-d2a-doc-ref-change-authority-metadata"
		docKey      = "d2a-doc-ref-change"
	)
	seedRefChangeInvalidationWorkspace(t, ctx, store, workspaceID, agentID)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, _, err := store.UpsertWorkspaceDocWithEffects(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "D2A Doc Ref Change",
		Content:     "# Doc\nVersion 1",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("seed workspace doc: %v", err)
	}
	doc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "d2a-doc-ref-change-report",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:d2a-doc-ref-change",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: doc.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed memory residency: %v", err)
	}

	_, invalidationEvents, err := store.UpsertWorkspaceDocWithEffects(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "D2A Doc Ref Change",
		Content:     "# Doc\nVersion 2",
		UpdatedBy:   "tests",
	})
	if err != nil {
		t.Fatalf("update workspace doc for invalidation enqueue: %v", err)
	}
	if len(invalidationEvents) != 1 || invalidationEvents[0].EventType != "memory.invalidation_enqueued" {
		t.Fatalf("expected one memory.invalidation_enqueued event, got %+v", invalidationEvents)
	}
	assertRuntimeEventAuthorityMetadata(t, invalidationEvents[0], authority)

	enqueued, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory.invalidation_enqueued events: %v", err)
	}
	if len(enqueued) != 1 {
		t.Fatalf("expected one persisted memory.invalidation_enqueued event, got %d", len(enqueued))
	}
	assertRuntimeEventAuthorityMetadata(t, enqueued[0], authority)

}

func TestWorkspaceArtifactRefChangeInvalidationEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-artifact-ref-change-authority-metadata"
		agentID     = "agent-d2a-artifact-ref-change-authority-metadata"
		artifactRef = "artifact://d2a/ref-change"
	)
	seedRefChangeInvalidationWorkspace(t, ctx, store, workspaceID, agentID)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	artifact, _, _, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		ArtifactRef: artifactRef,
		Title:       "D2A Artifact V1",
		Kind:        "reference",
		ContentType: "text/plain",
		CreatedBy:   "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace artifact: %v", err)
	}
	artifactToken := artifact.ArtifactID + "@" + artifact.CreatedAt
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "d2a-artifact-ref-change-report",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:d2a-artifact-ref-change",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "artifact_ref", RefID: artifactRef, VersionToken: artifactToken, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed artifact memory residency: %v", err)
	}

	_, _, invalidationEvents, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		ArtifactRef: artifactRef,
		Title:       "D2A Artifact V2",
		Kind:        "reference",
		ContentType: "text/plain",
		CreatedBy:   "tests",
	})
	if err != nil {
		t.Fatalf("create workspace artifact for invalidation enqueue: %v", err)
	}
	if len(invalidationEvents) != 1 || invalidationEvents[0].EventType != "memory.invalidation_enqueued" {
		t.Fatalf("expected one memory.invalidation_enqueued event, got %+v", invalidationEvents)
	}
	assertRuntimeEventAuthorityMetadata(t, invalidationEvents[0], authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory.invalidation_enqueued events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one persisted memory.invalidation_enqueued event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestKnowledgeClaimRefChangeInvalidationEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-claim-ref-change-authority-metadata"
		agentID     = "agent-d2a-claim-ref-change-authority-metadata"
	)
	seedRefChangeInvalidationWorkspace(t, ctx, store, workspaceID, agentID)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, _, _, err := store.RecordKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "D2A claim ref change",
		Body:        "version 1",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("seed knowledge claim: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "d2a-claim-ref-change-report",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:d2a-claim-ref-change",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed claim memory residency: %v", err)
	}

	updatedClaim, _, invalidationEvents, err := store.RecordKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     claim.ClaimID,
		WorkspaceID: workspaceID,
		ClaimType:   claim.ClaimType,
		Status:      claim.Status,
		Subject:     claim.Subject,
		Body:        "version 2",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("update knowledge claim for invalidation enqueue: %v", err)
	}
	if updatedClaim.UpdatedAt == claim.UpdatedAt {
		t.Fatalf("expected updated claim timestamp to change, got before=%s after=%s", claim.UpdatedAt, updatedClaim.UpdatedAt)
	}
	if len(invalidationEvents) != 1 || invalidationEvents[0].EventType != "memory.invalidation_enqueued" {
		t.Fatalf("expected one memory.invalidation_enqueued event, got %+v", invalidationEvents)
	}
	assertRuntimeEventAuthorityMetadata(t, invalidationEvents[0], authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory.invalidation_enqueued events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one persisted memory.invalidation_enqueued event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func seedRefChangeInvalidationWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Ref Change Invalidation Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D2A Ref Change Invalidation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
}
