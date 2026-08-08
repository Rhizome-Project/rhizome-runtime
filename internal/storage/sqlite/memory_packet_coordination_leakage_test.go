package sqlite

import (
	"context"
	"slices"
	"testing"
)

func TestBuildMemoryKernelPacketCoordinationBucketsAreIsolatedFromSemanticLaneNoise(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-kernel-coordination-leakage")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-leak-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint",
		Body:        "Coordination fetch should preserve this hard constraint under tight semantic budget.",
		Summary:     "Constraint leakage guard.",
		Confidence:  0.71,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-leak-decision",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision",
		Body:        "Coordination fetch should preserve this decision under tight semantic budget.",
		Summary:     "Decision leakage guard.",
		Confidence:  0.72,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-leak-blocker",
		ClaimType:   "BLOCKER_SYMPTOM",
		Status:      "ACTIVE",
		Subject:     "Blocker symptom",
		Body:        "Coordination fetch should preserve this blocker symptom under tight semantic budget.",
		Summary:     "Blocker leakage guard.",
		Confidence:  0.73,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-leak-hypothesis",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "Blocker hypothesis",
		Body:        "Coordination fetch should preserve this blocker hypothesis under tight semantic budget.",
		Summary:     "Blocker hypothesis leakage guard.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	// Flood the mixed semantic lane with higher-confidence non-coordination claims.
	for i, claimID := range []string{
		"claim-coord-leak-fact-a",
		"claim-coord-leak-fact-b",
		"claim-coord-leak-entity-a",
		"claim-coord-leak-hypothesis-a",
	} {
		recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     claimID,
			ClaimType:   []string{"FACT", "FACT", "ENTITY", "HYPOTHESIS"}[i],
			Status:      "ACTIVE",
			Subject:     "Semantic noise",
			Body:        "Mixed semantic lane saturation should not leak into dedicated coordination buckets.",
			Summary:     "Semantic noise.",
			Confidence:  0.95 - float64(i)*0.01,
			SourceKind:  "workspace_memory",
			SourceID:    "developer",
			TaskID:      scenario.taskID,
		})
	}

	tightPacket, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Budget: &MemoryPacketBudget{
			CoordinationFloor: 1,
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				MemoryRetrievalLaneBridge:        {ItemLimit: 2},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("build tight memory kernel packet: %v", err)
	}

	widePacket, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Budget: &MemoryPacketBudget{
			CoordinationFloor: 1,
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 12},
				MemoryRetrievalLaneBridge:        {ItemLimit: 2},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("build wide memory kernel packet: %v", err)
	}

	assertKnowledgeClaimIDsMatch(t, tightPacket.Coordination.HardConstraints, widePacket.Coordination.HardConstraints, "hard_constraints")
	assertKnowledgeClaimIDsMatch(t, tightPacket.Coordination.AcceptedDecisions, widePacket.Coordination.AcceptedDecisions, "accepted_decisions")
	assertKnowledgeClaimIDsMatch(t, tightPacket.Coordination.ActiveBlockers, widePacket.Coordination.ActiveBlockers, "active_blockers")
	assertKnowledgeClaimIDsMatch(t, tightPacket.Coordination.BlockerHypotheses, widePacket.Coordination.BlockerHypotheses, "blocker_hypotheses")
	assertKnowledgeClaimIDsMatch(t, tightPacket.Coordination.DecisionRecords, widePacket.Coordination.DecisionRecords, "decision_records")
	assertKnowledgeClaimIDsMatch(t, tightPacket.Coordination.BlockerSymptoms, widePacket.Coordination.BlockerSymptoms, "blocker_symptoms")

	if !hasKnowledgeClaim(tightPacket.Coordination.HardConstraints, "claim-coord-leak-constraint") {
		t.Fatalf("expected dedicated hard constraint under tight semantic budget, got %+v", tightPacket.Coordination.HardConstraints)
	}
	if !hasKnowledgeClaim(tightPacket.Coordination.AcceptedDecisions, "claim-coord-leak-decision") {
		t.Fatalf("expected dedicated decision under tight semantic budget, got %+v", tightPacket.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(tightPacket.Coordination.ActiveBlockers, "claim-coord-leak-blocker") {
		t.Fatalf("expected dedicated blocker under tight semantic budget, got %+v", tightPacket.Coordination.ActiveBlockers)
	}
	if !hasKnowledgeClaim(tightPacket.Coordination.BlockerHypotheses, "claim-coord-leak-hypothesis") {
		t.Fatalf("expected dedicated blocker hypothesis under tight semantic budget, got %+v", tightPacket.Coordination.BlockerHypotheses)
	}
}

func assertKnowledgeClaimIDsMatch(t *testing.T, got, want []KnowledgeClaimRecord, label string) {
	t.Helper()
	gotIDs := make([]string, 0, len(got))
	for _, record := range got {
		gotIDs = append(gotIDs, record.ClaimID)
	}
	wantIDs := make([]string, 0, len(want))
	for _, record := range want {
		wantIDs = append(wantIDs, record.ClaimID)
	}
	slices.Sort(gotIDs)
	slices.Sort(wantIDs)
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("%s ids mismatch: got=%v want=%v", label, gotIDs, wantIDs)
	}
}
