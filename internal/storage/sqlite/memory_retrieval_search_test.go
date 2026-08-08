package sqlite

import (
	"context"
	"testing"
)

func TestSearchMemoryNodesBoundsRecoverableArchiveAndDissentVisibility(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-retrieval-bounds"
		dormantDoc  = "doc-retrieval-dormant"
		triggerDoc  = "doc-retrieval-triggered"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Retrieval Bounds",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, doc := range []string{dormantDoc, triggerDoc} {
		if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      doc,
			Title:       "Rollback Runbook",
			Content:     "# Rollback\nRollback guidance for retrieval bounds.",
			UpdatedBy:   "developer",
		}); err != nil {
			t.Fatalf("upsert workspace doc %s: %v", doc, err)
		}
	}

	dormantRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Dormant rollback archive",
		Body:        "Expired archived rollback memory should stay hidden until explicitly requested.",
		Summary:     "Dormant rollback archive.",
		SourceKind:  "workspace_doc",
		SourceID:    dormantDoc,
		Importance:  0.6,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record dormant memory: %v", err)
	}
	triggerRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Triggered rollback archive",
		Body:        "Expired archived rollback memory should become recoverable when the source doc drifts.",
		Summary:     "Triggered rollback archive.",
		SourceKind:  "workspace_doc",
		SourceID:    triggerDoc,
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record trigger memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	dissentMarkerClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-retrieval-dissent-marker",
		ClaimType:   "dissent_marker",
		Status:      "active",
		Subject:     "Rollback disagreement exists",
		Body:        "Rollback dissent marker should stay visible as a first-class retrieval signal.",
		Summary:     "Rollback dissent marker stays visible.",
		Confidence:  0.6,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent marker claim: %v", err)
	}
	dissentContentClaim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-retrieval-dissent-content",
		ClaimType:   "dissent_content",
		Status:      "active",
		Subject:     "Rollback counter-argument",
		Body:        "Rollback dissent content should stay visible as a first-class retrieval signal.",
		Summary:     "Rollback dissent content stays visible.",
		Confidence:  0.58,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent content claim: %v", err)
	}

	for _, memoryID := range []string{dormantRecord.MemoryID, triggerRecord.MemoryID} {
		if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
			WorkspaceID: workspaceID,
			MemoryID:    memoryID,
			ArchivedBy:  "rmp_pruner",
			Reason:      rmpArchivedReasonExpired,
		}); err != nil {
			t.Fatalf("archive expired memory %s: %v", memoryID, err)
		}
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      triggerDoc,
		Title:       "Rollback Runbook",
		Content:     "# Rollback\nUpdated rollback guidance triggers recoverable archive.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("update trigger workspace doc: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	defaultResult, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "rollback",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("default search memory nodes: %v", err)
	}
	if defaultResult.Count != 2 || len(defaultResult.Hits) != 2 {
		t.Fatalf("expected active dissent marker and content on default search, got %+v", defaultResult.Hits)
	}
	defaultHitsByID := make(map[string]MemoryNodeSearchHit, len(defaultResult.Hits))
	for _, hit := range defaultResult.Hits {
		defaultHitsByID[hit.MemoryID] = hit
	}
	dissentMarkerNodeID := memoryGraphNodeID("knowledge_claim", dissentMarkerClaim.ClaimID)
	dissentContentNodeID := memoryGraphNodeID("knowledge_claim", dissentContentClaim.ClaimID)
	if markerHit, ok := defaultHitsByID[dissentMarkerNodeID]; !ok || markerHit.MemoryType != "DISSENT_MARKER" || markerHit.RetentionPrunable || markerHit.RetentionGuardReason != "DISSENT_MARKER" || markerHit.RecoveryCandidate || markerHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected default search to keep dissent marker visible, got %+v", defaultResult.Hits)
	}
	if contentHit, ok := defaultHitsByID[dissentContentNodeID]; !ok || contentHit.MemoryType != "DISSENT_CONTENT" || contentHit.RetentionPrunable || contentHit.RetentionGuardReason != "DISSENT_CONTENT" || contentHit.RecoveryCandidate || contentHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected default search to keep dissent content visible, got %+v", defaultResult.Hits)
	}

	withArchived, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           "rollback",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("search memory nodes with archived: %v", err)
	}
	if withArchived.Count < 6 || len(withArchived.Hits) < 6 {
		t.Fatalf("expected dissent marker/content, archived rollback memories, and archived promoted-claim mirrors, got %+v", withArchived.Hits)
	}

	hitsByID := make(map[string]MemoryNodeSearchHit, len(withArchived.Hits))
	for _, hit := range withArchived.Hits {
		hitsByID[hit.MemoryID] = hit
	}

	dormantNodeID := memoryGraphNodeID("workspace_memory", dormantRecord.MemoryID)
	dormantHit, ok := hitsByID[dormantNodeID]
	if !ok {
		t.Fatalf("expected dormant archived rollback memory in include_archived results, got %+v", withArchived.Hits)
	}
	if dormantHit.LifecycleState != "ARCHIVED" || dormantHit.ArchivedReason != rmpArchivedReasonExpired {
		t.Fatalf("expected dormant archived rollback memory trace, got %+v", dormantHit)
	}
	if dormantHit.RecoveryCandidate || dormantHit.RecoveryGuardReason != "NO_TRIGGERED_LINKAGE" {
		t.Fatalf("expected dormant archived rollback memory to stay bounded by recovery guard, got %+v", dormantHit)
	}

	triggerNodeID := memoryGraphNodeID("workspace_memory", triggerRecord.MemoryID)
	triggerHit, ok := hitsByID[triggerNodeID]
	if !ok {
		t.Fatalf("expected triggered archived rollback memory in include_archived results, got %+v", withArchived.Hits)
	}
	if triggerHit.LifecycleState != "ARCHIVED" || triggerHit.ArchivedReason != rmpArchivedReasonExpired {
		t.Fatalf("expected triggered archived rollback memory trace, got %+v", triggerHit)
	}
	if !triggerHit.RecoveryCandidate || triggerHit.RecoveryGuardReason != "" || triggerHit.DriftState != "STALE" {
		t.Fatalf("expected triggered archived rollback memory to surface recovery candidate, got %+v", triggerHit)
	}
	if len(triggerHit.RecoveryTriggerKinds) != 1 || triggerHit.RecoveryTriggerKinds[0] != "workspace_doc" {
		t.Fatalf("expected workspace_doc recovery trigger for triggered archived rollback memory, got %+v", triggerHit)
	}

	dormantClaimNodeID := memoryGraphNodeID("knowledge_claim", "claim:memory:"+dormantRecord.MemoryID)
	dormantClaimHit, ok := hitsByID[dormantClaimNodeID]
	if !ok {
		t.Fatalf("expected archived promoted-claim mirror for dormant rollback memory, got %+v", withArchived.Hits)
	}
	if dormantClaimHit.LifecycleState != "ARCHIVED" || dormantClaimHit.ArchivedReason != rmpArchivedReasonExpired {
		t.Fatalf("expected dormant promoted-claim archive trace, got %+v", dormantClaimHit)
	}
	if dormantClaimHit.RecoveryCandidate {
		t.Fatalf("did not expect dormant promoted-claim archive to surface as recovery candidate, got %+v", dormantClaimHit)
	}

	triggerClaimNodeID := memoryGraphNodeID("knowledge_claim", "claim:memory:"+triggerRecord.MemoryID)
	triggerClaimHit, ok := hitsByID[triggerClaimNodeID]
	if !ok {
		t.Fatalf("expected archived promoted-claim mirror for triggered rollback memory, got %+v", withArchived.Hits)
	}
	if triggerClaimHit.LifecycleState != "ARCHIVED" || triggerClaimHit.ArchivedReason != rmpArchivedReasonExpired {
		t.Fatalf("expected triggered promoted-claim archive trace, got %+v", triggerClaimHit)
	}
	if triggerClaimHit.RecoveryCandidate {
		t.Fatalf("did not expect triggered promoted-claim archive to surface as recovery candidate, got %+v", triggerClaimHit)
	}

	dissentMarkerHit, ok := hitsByID[dissentMarkerNodeID]
	if !ok {
		t.Fatalf("expected dissent marker hit in include_archived results, got %+v", withArchived.Hits)
	}
	if dissentMarkerHit.MemoryType != "DISSENT_MARKER" || dissentMarkerHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected dissent marker hit to stay first-class when archived nodes are included, got %+v", dissentMarkerHit)
	}
	if dissentMarkerHit.RetentionPrunable || dissentMarkerHit.RetentionGuardReason != "DISSENT_MARKER" {
		t.Fatalf("expected dissent marker hit to keep its retention guard, got %+v", dissentMarkerHit)
	}
	if dissentMarkerHit.RecoveryCandidate || dissentMarkerHit.ArchivedReason != "" {
		t.Fatalf("did not expect dissent marker hit to collapse into archive recovery semantics, got %+v", dissentMarkerHit)
	}

	dissentContentHit, ok := hitsByID[dissentContentNodeID]
	if !ok {
		t.Fatalf("expected dissent content hit in include_archived results, got %+v", withArchived.Hits)
	}
	if dissentContentHit.MemoryType != "DISSENT_CONTENT" || dissentContentHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected dissent content hit to stay first-class when archived nodes are included, got %+v", dissentContentHit)
	}
	if dissentContentHit.RetentionPrunable || dissentContentHit.RetentionGuardReason != "DISSENT_CONTENT" {
		t.Fatalf("expected dissent content hit to keep its retention guard, got %+v", dissentContentHit)
	}
	if dissentContentHit.RecoveryCandidate || dissentContentHit.ArchivedReason != "" {
		t.Fatalf("did not expect dissent content hit to collapse into archive recovery semantics, got %+v", dissentContentHit)
	}
}
