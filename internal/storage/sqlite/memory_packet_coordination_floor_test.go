package sqlite

import (
	"context"
	"testing"
)

func TestBuildMemoryKernelPacketCoordinationFloorPreservesBlockerHypothesisSeparatelyWhenAvailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-kernel-floor-hypothesis")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-floor-hyp-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint",
		Body:        "Coordination floor should preserve a hard constraint.",
		Summary:     "Constraint floor.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-floor-hyp-decision",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision",
		Body:        "Coordination floor should preserve a decision record.",
		Summary:     "Decision floor.",
		Confidence:  0.91,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-floor-hyp-blocker",
		ClaimType:   "BLOCKER_SYMPTOM",
		Status:      "ACTIVE",
		Subject:     "Blocker symptom",
		Body:        "Coordination floor should preserve a blocker symptom.",
		Summary:     "Blocker floor.",
		Confidence:  0.92,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-floor-hyp-hypothesis",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "Blocker hypothesis",
		Body:        "Coordination floor should preserve a blocker hypothesis in its own additive bucket.",
		Summary:     "Blocker hypothesis floor.",
		Confidence:  0.93,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	packet, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
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
		t.Fatalf("build memory kernel packet: %v", err)
	}

	if !hasKnowledgeClaim(packet.Coordination.HardConstraints, "claim-floor-hyp-constraint") {
		t.Fatalf("expected preserved hard constraint under tight semantic budget, got %+v", packet.Coordination.HardConstraints)
	}
	if !hasKnowledgeClaim(packet.Coordination.AcceptedDecisions, "claim-floor-hyp-decision") {
		t.Fatalf("expected preserved decision under tight semantic budget, got %+v", packet.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(packet.Coordination.ActiveBlockers, "claim-floor-hyp-blocker") {
		t.Fatalf("expected preserved blocker symptom under tight semantic budget, got %+v", packet.Coordination.ActiveBlockers)
	}
	if !hasKnowledgeClaim(packet.Coordination.BlockerHypotheses, "claim-floor-hyp-hypothesis") {
		t.Fatalf("expected preserved blocker hypothesis under tight semantic budget, got %+v", packet.Coordination.BlockerHypotheses)
	}
	if hasKnowledgeClaim(packet.Coordination.ActiveBlockers, "claim-floor-hyp-hypothesis") {
		t.Fatalf("expected blocker hypothesis to stay out of compat blocker bucket, got %+v", packet.Coordination.ActiveBlockers)
	}
	if !hasKnowledgeClaim(packet.Coordination.DecisionRecords, "claim-floor-hyp-decision") {
		t.Fatalf("expected decision_records alias to mirror preserved decision, got %+v", packet.Coordination.DecisionRecords)
	}
	if !hasKnowledgeClaim(packet.Coordination.BlockerSymptoms, "claim-floor-hyp-blocker") {
		t.Fatalf("expected blocker_symptoms alias to mirror preserved blocker, got %+v", packet.Coordination.BlockerSymptoms)
	}
}
