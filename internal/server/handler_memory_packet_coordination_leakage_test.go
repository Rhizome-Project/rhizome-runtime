package server

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPacketKernelRPCCoordinationBucketsAreIsolatedFromSemanticLaneNoise(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-coordination-leakage"
		taskID      = "task-memory-packet-coordination-leakage"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-leak-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint",
		Body:        "Dedicated coordination fetch should preserve this constraint under tight semantic budget.",
		Summary:     "Constraint leakage guard.",
		Confidence:  0.71,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-leak-decision",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision",
		Body:        "Dedicated coordination fetch should preserve this decision under tight semantic budget.",
		Summary:     "Decision leakage guard.",
		Confidence:  0.72,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-leak-blocker",
		ClaimType:   "BLOCKER_SYMPTOM",
		Status:      "ACTIVE",
		Subject:     "Blocker symptom",
		Body:        "Dedicated coordination fetch should preserve this blocker under tight semantic budget.",
		Summary:     "Blocker leakage guard.",
		Confidence:  0.73,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-leak-hypothesis",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "Blocker hypothesis",
		Body:        "Dedicated coordination fetch should preserve this blocker hypothesis under tight semantic budget.",
		Summary:     "Blocker hypothesis leakage guard.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	for i, claimID := range []string{
		"claim-rpc-coord-leak-fact-a",
		"claim-rpc-coord-leak-fact-b",
		"claim-rpc-coord-leak-entity-a",
		"claim-rpc-coord-leak-hypothesis-a",
	} {
		recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ClaimType:   []string{"FACT", "FACT", "ENTITY", "HYPOTHESIS"}[i],
			Status:      "ACTIVE",
			Subject:     "Semantic noise",
			Body:        "Mixed semantic lane saturation should not leak into dedicated coordination buckets.",
			Summary:     "Semantic noise.",
			Confidence:  0.95 - float64(i)*0.01,
			SourceKind:  "workspace_memory",
			SourceID:    "developer",
			TaskID:      taskID,
		})
	}

	tightPacket := buildKernelPacketViaRPCForBudget(t, ctx, h, workspaceID, taskID, &sqlite.MemoryPacketBudget{
		CoordinationFloor: 1,
		Lanes: map[string]sqlite.MemoryPacketLaneBudget{
			sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
			sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 2},
			sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
		},
	})
	widePacket := buildKernelPacketViaRPCForBudget(t, ctx, h, workspaceID, taskID, &sqlite.MemoryPacketBudget{
		CoordinationFloor: 1,
		Lanes: map[string]sqlite.MemoryPacketLaneBudget{
			sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 12},
			sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 2},
			sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
		},
	})

	assertServerKnowledgeClaimIDsMatch(t, tightPacket.Coordination.HardConstraints, widePacket.Coordination.HardConstraints, "hard_constraints")
	assertServerKnowledgeClaimIDsMatch(t, tightPacket.Coordination.AcceptedDecisions, widePacket.Coordination.AcceptedDecisions, "accepted_decisions")
	assertServerKnowledgeClaimIDsMatch(t, tightPacket.Coordination.ActiveBlockers, widePacket.Coordination.ActiveBlockers, "active_blockers")
	assertServerKnowledgeClaimIDsMatch(t, tightPacket.Coordination.BlockerHypotheses, widePacket.Coordination.BlockerHypotheses, "blocker_hypotheses")
	assertServerKnowledgeClaimIDsMatch(t, tightPacket.Coordination.DecisionRecords, widePacket.Coordination.DecisionRecords, "decision_records")
	assertServerKnowledgeClaimIDsMatch(t, tightPacket.Coordination.BlockerSymptoms, widePacket.Coordination.BlockerSymptoms, "blocker_symptoms")

	if !hasServerMemoryPacketClaim(tightPacket.Coordination.HardConstraints, "claim-rpc-coord-leak-constraint") {
		t.Fatalf("expected dedicated hard constraint under tight semantic budget, got %+v", tightPacket.Coordination.HardConstraints)
	}
	if !hasServerMemoryPacketClaim(tightPacket.Coordination.AcceptedDecisions, "claim-rpc-coord-leak-decision") {
		t.Fatalf("expected dedicated decision under tight semantic budget, got %+v", tightPacket.Coordination.AcceptedDecisions)
	}
	if !hasServerMemoryPacketClaim(tightPacket.Coordination.ActiveBlockers, "claim-rpc-coord-leak-blocker") {
		t.Fatalf("expected dedicated blocker under tight semantic budget, got %+v", tightPacket.Coordination.ActiveBlockers)
	}
	if !hasServerMemoryPacketClaim(tightPacket.Coordination.BlockerHypotheses, "claim-rpc-coord-leak-hypothesis") {
		t.Fatalf("expected dedicated blocker hypothesis under tight semantic budget, got %+v", tightPacket.Coordination.BlockerHypotheses)
	}
}

func buildKernelPacketViaRPCForBudget(t *testing.T, ctx context.Context, h *Handler, workspaceID, taskID string, budget *sqlite.MemoryPacketBudget) sqlite.MemoryKernelPacket {
	t.Helper()
	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Budget:      budget,
	})
	if err != nil {
		t.Fatalf("marshal kernel params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryPacketKernel(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketKernel rpc error: %+v", rpcErr)
	}
	return result.(map[string]any)["packet"].(sqlite.MemoryKernelPacket)
}

func assertServerKnowledgeClaimIDsMatch(t *testing.T, got, want []sqlite.KnowledgeClaimRecord, label string) {
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
