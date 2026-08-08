package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPacketShellRPCBoundsRecoverableContrastiveQuota(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-budget"
		taskID      = "task-memory-packet-budget"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-marker",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Marker",
		Body:        "Active dissent marker should keep a shell contrastive slot.",
		Summary:     "Active marker.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-content",
		ClaimType:   "DISSENT_CONTENT",
		Status:      "ACTIVE",
		Subject:     "Content",
		Body:        "Active dissent content should keep a shell contrastive slot.",
		Summary:     "Active content.",
		Confidence:  0.73,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-branch",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Branch",
		Body:        "Active alternative branch should compete for the remaining active slot.",
		Summary:     "Active branch.",
		Confidence:  0.72,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-archived-marker",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Archived marker",
		Body:        "Recoverable archived marker should be bounded by contrastive quota.",
		Summary:     "Archived marker.",
		Confidence:  0.64,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if _, err := store.ArchiveKnowledgeClaim(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-archived-marker",
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive recoverable marker: %v", err)
	}
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-archived-branch",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Archived branch",
		Body:        "Recoverable archived branch should not crowd out active shell claims.",
		Summary:     "Archived branch.",
		Confidence:  0.63,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      taskID,
		AgentID:     "agent-a",
	})
	if _, err := store.ArchiveKnowledgeClaim(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-archived-branch",
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive recoverable branch: %v", err)
	}
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-noise-a",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Noise A",
		Body:        "Semantic saturation should not crowd out active contrastive claims on RPC shell packets.",
		Summary:     "Noise A.",
		Confidence:  0.91,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-budget-noise-b",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Noise B",
		Body:        "A second semantic fact keeps the active semantic lane saturated on RPC shell packets.",
		Summary:     "Noise B.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneContrastive:   {ItemLimit: 3},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 6},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 6},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal shell params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)

	if got := len(packet.DifferentialClaims); got != 3 {
		t.Fatalf("expected bounded contrastive lane of 3 claims, got %d (%+v)", got, packet.DifferentialClaims)
	}

	activeCount := 0
	recoverableArchivedCount := 0
	for _, claim := range packet.DifferentialClaims {
		if claim.ClaimID == "claim-rpc-budget-archived-marker" || claim.ClaimID == "claim-rpc-budget-archived-branch" {
			if claim.ArchivedAt == nil || claim.LifecycleReason != "rmp_gc_expired" {
				t.Fatalf("expected recoverable archived metadata on shell differential claim, got %+v", claim)
			}
			recoverableArchivedCount++
			continue
		}
		activeCount++
	}
	if activeCount != 2 || recoverableArchivedCount != 1 {
		t.Fatalf("expected 2 active and 1 recoverable archived shell claims, got active=%d archived=%d (%+v)", activeCount, recoverableArchivedCount, packet.DifferentialClaims)
	}
}

func TestWorkspaceMemoryPacketKernelRPCKeepsBoundedCoordinationFloorWhenAvailable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-kernel-floor"
		taskID      = "task-memory-packet-kernel-floor"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-floor-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint",
		Body:        "Kernel packet floor should preserve at least one hard constraint.",
		Summary:     "Constraint floor.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-floor-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Decision",
		Body:        "Kernel packet floor should preserve at least one accepted decision.",
		Summary:     "Decision floor.",
		Confidence:  0.91,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-floor-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Blocker",
		Body:        "Kernel packet floor should preserve at least one blocker.",
		Summary:     "Blocker floor.",
		Confidence:  0.92,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Budget: &sqlite.MemoryPacketBudget{
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 2},
				sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal kernel params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryPacketKernel(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketKernel rpc error: %+v", rpcErr)
	}
	packet := result.(map[string]any)["packet"].(sqlite.MemoryKernelPacket)

	if len(packet.Coordination.HardConstraints) == 0 || len(packet.Coordination.AcceptedDecisions) == 0 || len(packet.Coordination.ActiveBlockers) == 0 {
		t.Fatalf("expected bounded coordination floor to preserve constraint/decision/blocker, got constraints=%d decisions=%d blockers=%d (%+v)", len(packet.Coordination.HardConstraints), len(packet.Coordination.AcceptedDecisions), len(packet.Coordination.ActiveBlockers), packet.Coordination)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.HardConstraints, "claim-rpc-floor-constraint") {
		t.Fatalf("expected preserved hard constraint under tight semantic budget, got %+v", packet.Coordination.HardConstraints)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.AcceptedDecisions, "claim-rpc-floor-decision") {
		t.Fatalf("expected preserved accepted decision under tight semantic budget, got %+v", packet.Coordination.AcceptedDecisions)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.ActiveBlockers, "claim-rpc-floor-blocker") {
		t.Fatalf("expected preserved blocker under tight semantic budget, got %+v", packet.Coordination.ActiveBlockers)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.DecisionRecords, "claim-rpc-floor-decision") {
		t.Fatalf("expected decision_records alias to mirror preserved accepted decision, got %+v", packet.Coordination.DecisionRecords)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.BlockerSymptoms, "claim-rpc-floor-blocker") {
		t.Fatalf("expected blocker_symptoms alias to mirror preserved blocker, got %+v", packet.Coordination.BlockerSymptoms)
	}
}

func recordHandlerMemoryPacketClaim(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.KnowledgeClaimInput) {
	t.Helper()
	if _, err := store.RecordKnowledgeClaim(ctx, input); err != nil {
		t.Fatalf("record knowledge claim %s: %v", input.ClaimID, err)
	}
}

func hasServerMemoryPacketClaim(records []sqlite.KnowledgeClaimRecord, claimID string) bool {
	for _, record := range records {
		if record.ClaimID == claimID {
			return true
		}
	}
	return false
}
