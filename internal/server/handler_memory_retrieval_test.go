package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryNodeSearchRPCBoundsRecoverableArchiveAndDissentVisibility(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-retrieval-bounds"
		dormantDoc  = "doc-handler-retrieval-dormant"
		triggerDoc  = "doc-handler-retrieval-triggered"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Retrieval Bounds",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, doc := range []string{dormantDoc, triggerDoc} {
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      doc,
			Title:       "Rollback Runbook",
			Content:     "# Rollback\nRollback guidance for handler retrieval bounds.",
			UpdatedBy:   "developer",
		}); err != nil {
			t.Fatalf("upsert workspace doc %s: %v", doc, err)
		}
	}

	dormantRecord, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Dormant rollback archive",
		Body:        "Expired archived rollback memory should stay hidden from default RPC search.",
		Summary:     "Dormant rollback archive.",
		SourceKind:  "workspace_doc",
		SourceID:    dormantDoc,
		Importance:  0.6,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record dormant memory: %v", err)
	}
	triggerRecord, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Triggered rollback archive",
		Body:        "Expired archived rollback memory should surface recovery semantics once the source doc drifts.",
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
	dissentMarkerClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-retrieval-dissent-marker",
		ClaimType:   "dissent_marker",
		Status:      "active",
		Subject:     "Rollback disagreement exists",
		Body:        "Rollback dissent marker should stay visible as a first-class RPC retrieval signal.",
		Summary:     "Rollback dissent marker stays visible.",
		Confidence:  0.6,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent marker claim: %v", err)
	}
	dissentContentClaim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-retrieval-dissent-content",
		ClaimType:   "dissent_content",
		Status:      "active",
		Subject:     "Rollback counter-argument",
		Body:        "Rollback dissent content should stay visible as a first-class RPC retrieval signal.",
		Summary:     "Rollback dissent content stays visible.",
		Confidence:  0.58,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent content claim: %v", err)
	}

	for _, memoryID := range []string{dormantRecord.MemoryID, triggerRecord.MemoryID} {
		if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
			WorkspaceID: workspaceID,
			MemoryID:    memoryID,
			ArchivedBy:  "rmp_pruner",
			Reason:      "rmp_gc_expired",
		}); err != nil {
			t.Fatalf("archive expired memory %s: %v", memoryID, err)
		}
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      triggerDoc,
		Title:       "Rollback Runbook",
		Content:     "# Rollback\nUpdated rollback guidance triggers recoverable archive in RPC search.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("update trigger workspace doc: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	defaultRaw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID: workspaceID,
		Query:       "rollback",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal default search params: %v", err)
	}
	defaultResultRaw, rpcErr := h.workspaceMemoryNodeSearch(ctx, defaultRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch default rpc error: %+v", rpcErr)
	}
	defaultResult := defaultResultRaw.(sqlite.MemoryNodeSearchResult)
	if defaultResult.Count != 2 || len(defaultResult.Hits) != 2 {
		t.Fatalf("expected active dissent marker and content on default RPC search, got %+v", defaultResult.Hits)
	}
	defaultHitsByID := make(map[string]sqlite.MemoryNodeSearchHit, len(defaultResult.Hits))
	for _, hit := range defaultResult.Hits {
		defaultHitsByID[hit.MemoryID] = hit
	}
	dissentMarkerNodeID := "memnode:knowledge_claim:" + dissentMarkerClaim.ClaimID
	dissentContentNodeID := "memnode:knowledge_claim:" + dissentContentClaim.ClaimID
	if markerHit, ok := defaultHitsByID[dissentMarkerNodeID]; !ok || markerHit.MemoryType != "DISSENT_MARKER" || markerHit.RetentionPrunable || markerHit.RetentionGuardReason != "DISSENT_MARKER" || markerHit.RecoveryCandidate || markerHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected default RPC search to keep dissent marker visible, got %+v", defaultResult.Hits)
	}
	if contentHit, ok := defaultHitsByID[dissentContentNodeID]; !ok || contentHit.MemoryType != "DISSENT_CONTENT" || contentHit.RetentionPrunable || contentHit.RetentionGuardReason != "DISSENT_CONTENT" || contentHit.RecoveryCandidate || contentHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected default RPC search to keep dissent content visible, got %+v", defaultResult.Hits)
	}

	withArchivedRaw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID:     workspaceID,
		Query:           "rollback",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("marshal include_archived search params: %v", err)
	}
	withArchivedResultRaw, rpcErr := h.workspaceMemoryNodeSearch(ctx, withArchivedRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch include_archived rpc error: %+v", rpcErr)
	}
	withArchived := withArchivedResultRaw.(sqlite.MemoryNodeSearchResult)
	if withArchived.Count < 6 || len(withArchived.Hits) < 6 {
		t.Fatalf("expected dissent marker/content, archived rollback memories, and archived promoted-claim mirrors, got %+v", withArchived.Hits)
	}

	hitsByID := make(map[string]sqlite.MemoryNodeSearchHit, len(withArchived.Hits))
	for _, hit := range withArchived.Hits {
		hitsByID[hit.MemoryID] = hit
	}

	dormantNodeID := "memnode:workspace_memory:" + dormantRecord.MemoryID
	dormantHit, ok := hitsByID[dormantNodeID]
	if !ok {
		t.Fatalf("expected dormant archived rollback memory in include_archived RPC results, got %+v", withArchived.Hits)
	}
	if dormantHit.LifecycleState != "ARCHIVED" || dormantHit.ArchivedReason != "rmp_gc_expired" {
		t.Fatalf("expected dormant archived rollback trace, got %+v", dormantHit)
	}
	if dormantHit.RecoveryCandidate || dormantHit.RecoveryGuardReason != "NO_TRIGGERED_LINKAGE" {
		t.Fatalf("expected dormant archived rollback memory to stay bounded by recovery guard, got %+v", dormantHit)
	}

	triggerNodeID := "memnode:workspace_memory:" + triggerRecord.MemoryID
	triggerHit, ok := hitsByID[triggerNodeID]
	if !ok {
		t.Fatalf("expected triggered archived rollback memory in include_archived RPC results, got %+v", withArchived.Hits)
	}
	if triggerHit.LifecycleState != "ARCHIVED" || triggerHit.ArchivedReason != "rmp_gc_expired" {
		t.Fatalf("expected triggered archived rollback trace, got %+v", triggerHit)
	}
	if !triggerHit.RecoveryCandidate || triggerHit.RecoveryGuardReason != "" || triggerHit.DriftState != "STALE" {
		t.Fatalf("expected triggered archived rollback memory to surface recovery candidate, got %+v", triggerHit)
	}
	if len(triggerHit.RecoveryTriggerKinds) != 1 || triggerHit.RecoveryTriggerKinds[0] != "workspace_doc" {
		t.Fatalf("expected workspace_doc recovery trigger for triggered archived rollback memory, got %+v", triggerHit)
	}

	dormantClaimNodeID := "memnode:knowledge_claim:claim:memory:" + dormantRecord.MemoryID
	dormantClaimHit, ok := hitsByID[dormantClaimNodeID]
	if !ok {
		t.Fatalf("expected archived promoted-claim mirror for dormant rollback memory in include_archived RPC results, got %+v", withArchived.Hits)
	}
	if dormantClaimHit.LifecycleState != "ARCHIVED" || dormantClaimHit.ArchivedReason != "rmp_gc_expired" {
		t.Fatalf("expected dormant promoted-claim archive trace, got %+v", dormantClaimHit)
	}
	if dormantClaimHit.RecoveryCandidate {
		t.Fatalf("did not expect dormant promoted-claim archive to surface as recovery candidate, got %+v", dormantClaimHit)
	}

	triggerClaimNodeID := "memnode:knowledge_claim:claim:memory:" + triggerRecord.MemoryID
	triggerClaimHit, ok := hitsByID[triggerClaimNodeID]
	if !ok {
		t.Fatalf("expected archived promoted-claim mirror for triggered rollback memory in include_archived RPC results, got %+v", withArchived.Hits)
	}
	if triggerClaimHit.LifecycleState != "ARCHIVED" || triggerClaimHit.ArchivedReason != "rmp_gc_expired" {
		t.Fatalf("expected triggered promoted-claim archive trace, got %+v", triggerClaimHit)
	}
	if triggerClaimHit.RecoveryCandidate {
		t.Fatalf("did not expect triggered promoted-claim archive to surface as recovery candidate, got %+v", triggerClaimHit)
	}

	dissentMarkerHit, ok := hitsByID[dissentMarkerNodeID]
	if !ok {
		t.Fatalf("expected dissent marker hit in include_archived RPC results, got %+v", withArchived.Hits)
	}
	if dissentMarkerHit.MemoryType != "DISSENT_MARKER" || dissentMarkerHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected dissent marker hit to stay first-class when archived nodes are included, got %+v", dissentMarkerHit)
	}
	if dissentMarkerHit.RetentionPrunable || dissentMarkerHit.RetentionGuardReason != "DISSENT_MARKER" {
		t.Fatalf("expected RPC dissent marker hit to keep its retention guard, got %+v", dissentMarkerHit)
	}
	if dissentMarkerHit.RecoveryCandidate || dissentMarkerHit.ArchivedReason != "" {
		t.Fatalf("did not expect RPC dissent marker hit to collapse into archive recovery semantics, got %+v", dissentMarkerHit)
	}

	dissentContentHit, ok := hitsByID[dissentContentNodeID]
	if !ok {
		t.Fatalf("expected dissent content hit in include_archived RPC results, got %+v", withArchived.Hits)
	}
	if dissentContentHit.MemoryType != "DISSENT_CONTENT" || dissentContentHit.LifecycleState != "ACTIVE" {
		t.Fatalf("expected dissent content hit to stay first-class when archived nodes are included, got %+v", dissentContentHit)
	}
	if dissentContentHit.RetentionPrunable || dissentContentHit.RetentionGuardReason != "DISSENT_CONTENT" {
		t.Fatalf("expected RPC dissent content hit to keep its retention guard, got %+v", dissentContentHit)
	}
	if dissentContentHit.RecoveryCandidate || dissentContentHit.ArchivedReason != "" {
		t.Fatalf("did not expect RPC dissent content hit to collapse into archive recovery semantics, got %+v", dissentContentHit)
	}
}
