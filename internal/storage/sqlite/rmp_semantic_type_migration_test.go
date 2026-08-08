package sqlite

import (
	"context"
	"testing"
)

func TestRMPSemanticTypeMigrationGraphCanonicalizesDecisionAndBlockerAliases(t *testing.T) {
	t.Parallel()

	decisionRecord := KnowledgeClaimRecord{ClaimType: "DECISION"}
	decisionType := canonicalMemoryTypeFromKnowledgeClaim(decisionRecord)
	blockerRecord := KnowledgeClaimRecord{ClaimType: "BLOCKER"}
	blockerType := canonicalMemoryTypeFromKnowledgeClaim(blockerRecord)
	if decisionType != "DECISION_RECORD" || blockerType != "BLOCKER_SYMPTOM" {
		t.Fatalf("expected canonical semantic aliases, got decision=%q blocker=%q", decisionType, blockerType)
	}

	if gotPred := memoryGraphPredicateForType(decisionType); gotPred != "decides" {
		t.Fatalf("memoryGraphPredicateForType(%q) = %q, want decides", decisionType, gotPred)
	}
	if gotMod := memoryGraphClaimModality(decisionRecord, decisionType); gotMod != "decided" {
		t.Fatalf("memoryGraphClaimModality(decision, %q) = %q, want decided", decisionType, gotMod)
	}

	if gotPred := memoryGraphPredicateForType(blockerType); gotPred != "blocks" {
		t.Fatalf("memoryGraphPredicateForType(%q) = %q, want blocks", blockerType, gotPred)
	}
	if gotMod := memoryGraphClaimModality(blockerRecord, blockerType); gotMod != "observed" {
		t.Fatalf("memoryGraphClaimModality(blocker, %q) = %q, want observed", blockerType, gotMod)
	}

	if gotPred := memoryGraphPredicateForType("BLOCKER_HYPOTHESIS"); gotPred != "explains_blocker" {
		t.Fatalf("memoryGraphPredicateForType(BLOCKER_HYPOTHESIS) = %q, want explains_blocker", gotPred)
	}
	if gotMod := memoryGraphCanonicalModalityForType("BLOCKER_HYPOTHESIS"); gotMod != "proposed" {
		t.Fatalf("memoryGraphCanonicalModalityForType(BLOCKER_HYPOTHESIS) = %q, want proposed", gotMod)
	}
}

func TestRMPSemanticTypeMigrationSearchSurfacesDecisionRecordAndBlockerAliases(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rmp-semantic-type-search"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RMP Semantic Type Search",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	decision, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rmp-semantic-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Rollout decision record",
		Body:        "Legacy DECISION writes should surface as DECISION_RECORD on read-side search.",
		Summary:     "Decision record migration.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record decision claim: %v", err)
	}
	blocker, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rmp-semantic-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Deploy fails with missing credentials",
		Body:        "Legacy BLOCKER writes should surface as BLOCKER_SYMPTOM on read-side search.",
		Summary:     "Blocker symptom migration.",
		Confidence:  0.82,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record blocker claim: %v", err)
	}
	hypothesis, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rmp-semantic-hypothesis",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "Credential hypothesis",
		Body:        "Credential propagation lag likely explains the blocker.",
		Summary:     "Credential lag hypothesis.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record blocker hypothesis claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	decisionResult, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "decision record",
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search decision node: %v", err)
	}
	decisionHit := mustFindSearchHit(t, decisionResult.Hits, memoryGraphNodeID("knowledge_claim", decision.ClaimID))
	if decisionHit.MemoryType != "DECISION_RECORD" || decisionHit.CompatType != "DECISION" {
		t.Fatalf("expected canonical decision record with compat decision, got %+v", decisionHit)
	}
	if decisionHit.ClaimPredicate != "decides" {
		t.Fatalf("expected decision search hit predicate decides, got %+v", decisionHit)
	}

	blockerResult, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "fails with missing credentials",
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search blocker node: %v", err)
	}
	blockerHit := mustFindSearchHit(t, blockerResult.Hits, memoryGraphNodeID("knowledge_claim", blocker.ClaimID))
	if blockerHit.MemoryType != "BLOCKER_SYMPTOM" || blockerHit.CompatType != "BLOCKER" {
		t.Fatalf("expected canonical blocker symptom with compat blocker, got %+v", blockerHit)
	}
	if blockerHit.ClaimPredicate != "blocks" {
		t.Fatalf("expected blocker search hit predicate blocks, got %+v", blockerHit)
	}

	hypothesisResult, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "credential propagation lag",
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search hypothesis node: %v", err)
	}
	hypothesisHit := mustFindSearchHit(t, hypothesisResult.Hits, memoryGraphNodeID("knowledge_claim", hypothesis.ClaimID))
	if hypothesisHit.MemoryType != "BLOCKER_HYPOTHESIS" || hypothesisHit.CompatType != "BLOCKER_HYPOTHESIS" {
		t.Fatalf("expected canonical blocker hypothesis, got %+v", hypothesisHit)
	}
	if hypothesisHit.ClaimPredicate != "explains_blocker" {
		t.Fatalf("expected blocker hypothesis predicate explains_blocker, got %+v", hypothesisHit)
	}
}

func TestRMPSemanticTypeMigrationKernelPacketSurfacesCanonicalDecisionAndBlockerAliases(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rmp-semantic-packet")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rmp-packet-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Packet decision record",
		Body:        "Legacy DECISION writes should surface as DECISION_RECORD in kernel coordination.",
		Summary:     "Packet decision migration.",
		Confidence:  0.94,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rmp-packet-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Packet blocker symptom",
		Body:        "Legacy BLOCKER writes should surface as BLOCKER_SYMPTOM in kernel coordination.",
		Summary:     "Packet blocker migration.",
		Confidence:  0.88,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rmp-packet-hypothesis",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "Packet blocker hypothesis",
		Body:        "Credential propagation lag likely explains the blocker.",
		Summary:     "Packet blocker hypothesis migration.",
		Confidence:  0.77,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	packet, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 12},
				MemoryRetrievalLaneBridge:        {ItemLimit: 4},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory kernel packet: %v", err)
	}

	decisionClaim := mustFindKnowledgeClaim(t, packet.Coordination.AcceptedDecisions, "claim-rmp-packet-decision")
	if decisionClaim.ClaimType != "DECISION" {
		t.Fatalf("expected accepted_decisions to preserve legacy compat type, got %+v", decisionClaim)
	}
	if mirror := mustFindKnowledgeClaim(t, packet.Coordination.DecisionRecords, "claim-rmp-packet-decision"); mirror.ClaimType != "DECISION_RECORD" {
		t.Fatalf("expected decision_records alias to mirror canonical decision record, got %+v", mirror)
	}

	blockerClaim := mustFindKnowledgeClaim(t, packet.Coordination.ActiveBlockers, "claim-rmp-packet-blocker")
	if blockerClaim.ClaimType != "BLOCKER" {
		t.Fatalf("expected active_blockers to preserve legacy compat type, got %+v", blockerClaim)
	}
	if mirror := mustFindKnowledgeClaim(t, packet.Coordination.BlockerSymptoms, "claim-rmp-packet-blocker"); mirror.ClaimType != "BLOCKER_SYMPTOM" {
		t.Fatalf("expected blocker_symptoms alias to mirror canonical blocker symptom, got %+v", mirror)
	}
	if hasKnowledgeClaim(packet.Coordination.ActiveBlockers, "claim-rmp-packet-hypothesis") {
		t.Fatalf("expected blocker hypothesis to stay out of active_blockers compat bucket, got %+v", packet.Coordination.ActiveBlockers)
	}
	if mirror := mustFindKnowledgeClaim(t, packet.Coordination.BlockerHypotheses, "claim-rmp-packet-hypothesis"); mirror.ClaimType != "BLOCKER_HYPOTHESIS" {
		t.Fatalf("expected blocker_hypotheses alias to surface canonical blocker hypothesis, got %+v", mirror)
	}
}

func mustFindSearchHit(t *testing.T, hits []MemoryNodeSearchHit, memoryID string) MemoryNodeSearchHit {
	t.Helper()
	for _, hit := range hits {
		if hit.MemoryID == memoryID {
			return hit
		}
	}
	t.Fatalf("expected search hit for %s, got %+v", memoryID, hits)
	return MemoryNodeSearchHit{}
}

func mustFindKnowledgeClaim(t *testing.T, claims []KnowledgeClaimRecord, claimID string) KnowledgeClaimRecord {
	t.Helper()
	for _, claim := range claims {
		if claim.ClaimID == claimID {
			return claim
		}
	}
	t.Fatalf("expected knowledge claim %s, got %+v", claimID, claims)
	return KnowledgeClaimRecord{}
}
