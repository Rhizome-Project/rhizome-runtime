package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryNodeSearchRPCSurfacesCanonicalDecisionAndBlockerAliasesForLegacyWrites(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-memory-semantic-type-search"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Semantic Type Search",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	decision, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-semantic-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "RPC decision record",
		Body:        "Legacy decision writes should surface as DECISION_RECORD in RPC search.",
		Summary:     "RPC decision migration.",
		Confidence:  0.91,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record decision claim: %v", err)
	}
	blocker, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-semantic-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "RPC blocker symptom",
		Body:        "Legacy blocker writes should surface as BLOCKER_SYMPTOM in RPC search.",
		Summary:     "RPC blocker migration.",
		Confidence:  0.83,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record blocker claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	decisionRaw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID: workspaceID,
		Query:       "decision migration",
		MemoryType:  "DECISION",
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal decision search params: %v", err)
	}
	decisionResultAny, rpcErr := h.workspaceMemoryNodeSearch(ctx, decisionRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch decision rpc error: %+v", rpcErr)
	}
	decisionResult := decisionResultAny.(sqlite.MemoryNodeSearchResult)
	decisionHit := mustFindServerSearchHit(t, decisionResult.Hits, sqliteMemoryNodeID("knowledge_claim", decision.ClaimID))
	if decisionHit.MemoryType != "DECISION_RECORD" || decisionHit.CompatType != "DECISION" {
		t.Fatalf("expected RPC decision search hit to surface DECISION_RECORD + compat DECISION, got %+v", decisionHit)
	}

	blockerRaw, err := json.Marshal(workspaceMemoryNodeSearchParams{
		WorkspaceID: workspaceID,
		Query:       "blocker migration",
		MemoryType:  "BLOCKER",
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal blocker search params: %v", err)
	}
	blockerResultAny, rpcErr := h.workspaceMemoryNodeSearch(ctx, blockerRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryNodeSearch blocker rpc error: %+v", rpcErr)
	}
	blockerResult := blockerResultAny.(sqlite.MemoryNodeSearchResult)
	blockerHit := mustFindServerSearchHit(t, blockerResult.Hits, sqliteMemoryNodeID("knowledge_claim", blocker.ClaimID))
	if blockerHit.MemoryType != "BLOCKER_SYMPTOM" || blockerHit.CompatType != "BLOCKER" {
		t.Fatalf("expected RPC blocker search hit to surface BLOCKER_SYMPTOM + compat BLOCKER, got %+v", blockerHit)
	}
}

func TestWorkspaceMemoryPacketKernelRPCSurfacesDecisionAndBlockerAliasFields(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-semantic-packet"
		taskID      = "task-handler-memory-semantic-packet"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerSemanticClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-packet-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "RPC packet decision",
		Body:        "Legacy decision writes should appear in decision_records alias field.",
		Summary:     "RPC packet decision migration.",
		Confidence:  0.94,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerSemanticClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-packet-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "RPC packet blocker",
		Body:        "Legacy blocker writes should appear in blocker_symptoms alias field.",
		Summary:     "RPC packet blocker migration.",
		Confidence:  0.86,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerSemanticClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-packet-hypothesis",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "RPC packet blocker hypothesis",
		Body:        "Legacy blocker hypotheses should appear in blocker_hypotheses alias field.",
		Summary:     "RPC packet blocker hypothesis migration.",
		Confidence:  0.77,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
	})
	if err != nil {
		t.Fatalf("marshal kernel params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryPacketKernel(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketKernel rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryKernelPacket)

	if claim := mustFindServerPacketClaim(t, packet.Coordination.AcceptedDecisions, "claim-handler-packet-decision"); claim.ClaimType != "DECISION" {
		t.Fatalf("expected legacy decision payload type in accepted_decisions, got %+v", claim)
	}
	if claim := mustFindServerPacketClaim(t, packet.Coordination.DecisionRecords, "claim-handler-packet-decision"); claim.ClaimType != "DECISION_RECORD" {
		t.Fatalf("expected decision_records alias field to surface canonical decision record, got %+v", claim)
	}
	if claim := mustFindServerPacketClaim(t, packet.Coordination.ActiveBlockers, "claim-handler-packet-blocker"); claim.ClaimType != "BLOCKER" {
		t.Fatalf("expected legacy blocker payload type in active_blockers, got %+v", claim)
	}
	if claim := mustFindServerPacketClaim(t, packet.Coordination.BlockerSymptoms, "claim-handler-packet-blocker"); claim.ClaimType != "BLOCKER_SYMPTOM" {
		t.Fatalf("expected blocker_symptoms alias field to surface canonical blocker symptom, got %+v", claim)
	}
	if hasServerMemoryPacketClaim(packet.Coordination.ActiveBlockers, "claim-handler-packet-hypothesis") {
		t.Fatalf("expected blocker hypothesis to stay out of active_blockers compat bucket, got %+v", packet.Coordination.ActiveBlockers)
	}
	if claim := mustFindServerPacketClaim(t, packet.Coordination.BlockerHypotheses, "claim-handler-packet-hypothesis"); claim.ClaimType != "BLOCKER_HYPOTHESIS" {
		t.Fatalf("expected blocker_hypotheses alias field to surface canonical blocker hypothesis, got %+v", claim)
	}
}

func mustFindServerSearchHit(t *testing.T, hits []sqlite.MemoryNodeSearchHit, memoryID string) sqlite.MemoryNodeSearchHit {
	t.Helper()
	for _, hit := range hits {
		if hit.MemoryID == memoryID {
			return hit
		}
	}
	t.Fatalf("expected search hit for %s, got %+v", memoryID, hits)
	return sqlite.MemoryNodeSearchHit{}
}

func mustFindServerPacketClaim(t *testing.T, claims []sqlite.KnowledgeClaimRecord, claimID string) sqlite.KnowledgeClaimRecord {
	t.Helper()
	for _, claim := range claims {
		if claim.ClaimID == claimID {
			return claim
		}
	}
	t.Fatalf("expected packet claim %s, got %+v", claimID, claims)
	return sqlite.KnowledgeClaimRecord{}
}

func recordHandlerSemanticClaim(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.KnowledgeClaimInput) {
	t.Helper()
	if _, err := store.RecordKnowledgeClaim(ctx, input); err != nil {
		t.Fatalf("record knowledge claim %s: %v", input.ClaimID, err)
	}
}

func sqliteMemoryNodeID(originKind, originID string) string {
	return "memnode:" + originKind + ":" + originID
}
