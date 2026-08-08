package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPacketKernelRPCCoordinationFloorPreservesBlockerHypothesisSeparatelyWhenAvailable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-floor-hypothesis"
		taskID      = "task-memory-packet-floor-hypothesis"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-floor-hyp-constraint",
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
		ClaimID:     "claim-rpc-floor-hyp-decision",
		ClaimType:   "DECISION_RECORD",
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
		ClaimID:     "claim-rpc-floor-hyp-blocker",
		ClaimType:   "BLOCKER_SYMPTOM",
		Status:      "ACTIVE",
		Subject:     "Blocker symptom",
		Body:        "Kernel packet floor should preserve at least one blocker symptom.",
		Summary:     "Blocker floor.",
		Confidence:  0.92,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-floor-hyp-hypothesis",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "Blocker hypothesis",
		Body:        "Kernel packet floor should preserve at least one blocker hypothesis in its additive bucket.",
		Summary:     "Blocker hypothesis floor.",
		Confidence:  0.93,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Budget: &sqlite.MemoryPacketBudget{
			CoordinationFloor: 1,
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

	if !hasServerMemoryPacketClaim(packet.Coordination.HardConstraints, "claim-rpc-floor-hyp-constraint") {
		t.Fatalf("expected preserved hard constraint under tight semantic budget, got %+v", packet.Coordination.HardConstraints)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.AcceptedDecisions, "claim-rpc-floor-hyp-decision") {
		t.Fatalf("expected preserved decision under tight semantic budget, got %+v", packet.Coordination.AcceptedDecisions)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.ActiveBlockers, "claim-rpc-floor-hyp-blocker") {
		t.Fatalf("expected preserved blocker symptom under tight semantic budget, got %+v", packet.Coordination.ActiveBlockers)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.BlockerHypotheses, "claim-rpc-floor-hyp-hypothesis") {
		t.Fatalf("expected preserved blocker hypothesis under tight semantic budget, got %+v", packet.Coordination.BlockerHypotheses)
	}
	if hasServerMemoryPacketClaim(packet.Coordination.ActiveBlockers, "claim-rpc-floor-hyp-hypothesis") {
		t.Fatalf("expected blocker hypothesis to stay out of active_blockers compat bucket, got %+v", packet.Coordination.ActiveBlockers)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.DecisionRecords, "claim-rpc-floor-hyp-decision") {
		t.Fatalf("expected decision_records alias to mirror preserved decision, got %+v", packet.Coordination.DecisionRecords)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.BlockerSymptoms, "claim-rpc-floor-hyp-blocker") {
		t.Fatalf("expected blocker_symptoms alias to mirror preserved blocker, got %+v", packet.Coordination.BlockerSymptoms)
	}
}

func TestWorkspaceMemoryPacketKernelRPCCoordinationLaneCanExceedSemanticLaneWithoutPolicyCutover(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-coordination-lane"
		taskID      = "task-memory-packet-coordination-lane"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-lane-constraint-a",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint A",
		Body:        "Explicit coordination lane should retain multiple coordination claims on RPC kernel packets.",
		Summary:     "Constraint A.",
		Confidence:  0.81,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-lane-constraint-b",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint B",
		Body:        "Second constraint should still fit through the dedicated coordination lane on RPC.",
		Summary:     "Constraint B.",
		Confidence:  0.80,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-lane-fact-a",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact A",
		Body:        "Mixed semantic lane stays tight for ordinary semantic claims on RPC.",
		Summary:     "Fact A.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coord-lane-fact-b",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact B",
		Body:        "Second fact competes for the same tight semantic slot on RPC.",
		Summary:     "Fact B.",
		Confidence:  0.94,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Budget: &sqlite.MemoryPacketBudget{
			CoordinationFloor: 1,
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneCoordination:  {ItemLimit: 2},
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

	if got := countServerKnowledgeClaimsByType(packet.Coordination.HardConstraints, "CONSTRAINT"); got != 2 {
		t.Fatalf("expected explicit coordination lane to preserve 2 constraints, got %d (%+v)", got, packet.Coordination.HardConstraints)
	}
}

func countServerKnowledgeClaimsByType(records []sqlite.KnowledgeClaimRecord, claimType string) int {
	count := 0
	for _, record := range records {
		if record.ClaimType == claimType {
			count++
		}
	}
	return count
}

func TestWorkspaceMemoryPacketKernelRPCUsesExplicitCoordinationLaneBudgetWhenProvided(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-coordination-budget"
		taskID      = "task-memory-packet-coordination-budget"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coordination-budget-decision-a",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision A",
		Body:        "Explicit coordination lane budget should preserve the first decision.",
		Summary:     "Decision A.",
		Confidence:  0.91,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coordination-budget-decision-b",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision B",
		Body:        "Explicit coordination lane budget should preserve the second decision.",
		Summary:     "Decision B.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-coordination-budget-fact",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact",
		Body:        "Semantic lane should stay tighter than explicit coordination lane.",
		Summary:     "Fact budget.",
		Confidence:  0.7,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Budget: &sqlite.MemoryPacketBudget{
			CoordinationFloor: 1,
			Lanes: map[string]sqlite.MemoryPacketLaneBudget{
				sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				sqlite.MemoryRetrievalLaneCoordination:  {ItemLimit: 2},
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

	if got := len(packet.Coordination.AcceptedDecisions); got < 2 {
		t.Fatalf("expected explicit coordination lane budget to preserve 2 accepted decisions, got %d (%+v)", got, packet.Coordination.AcceptedDecisions)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.AcceptedDecisions, "claim-rpc-coordination-budget-decision-a") || !hasServerMemoryPacketClaim(packet.Coordination.AcceptedDecisions, "claim-rpc-coordination-budget-decision-b") {
		t.Fatalf("expected explicit coordination lane budget to preserve both decisions, got %+v", packet.Coordination.AcceptedDecisions)
	}
	if got := len(packet.Coordination.DecisionRecords); got < 2 {
		t.Fatalf("expected decision_records alias to preserve 2 accepted decisions, got %d (%+v)", got, packet.Coordination.DecisionRecords)
	}
}

func TestWorkspaceMemoryPacketKernelRPCUsesIndependentDefaultCoordinationLaneWhenUnset(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-default-coordination"
		taskID      = "task-memory-packet-default-coordination"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-default-coord-constraint-a",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint A",
		Body:        "Default coordination lane should preserve multiple hard constraints over a tight semantic lane.",
		Summary:     "Constraint A.",
		Confidence:  0.85,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-default-coord-constraint-b",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint B",
		Body:        "Second hard constraint should still survive when coordination budget is omitted from RPC params.",
		Summary:     "Constraint B.",
		Confidence:  0.84,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-default-coord-decision-a",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision A",
		Body:        "Default coordination lane should preserve multiple accepted decisions over a tight semantic lane.",
		Summary:     "Decision A.",
		Confidence:  0.83,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-default-coord-decision-b",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision B",
		Body:        "Second accepted decision should still survive when coordination budget is omitted from RPC params.",
		Summary:     "Decision B.",
		Confidence:  0.82,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-default-coord-fact-a",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact A",
		Body:        "Semantic lane remains tight while default coordination lane stays independent.",
		Summary:     "Fact A.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})
	recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rpc-default-coord-fact-b",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact B",
		Body:        "Second fact competes only inside the semantic lane.",
		Summary:     "Fact B.",
		Confidence:  0.94,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      taskID,
	})

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Budget: &sqlite.MemoryPacketBudget{
			CoordinationFloor: 1,
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

	if got := countServerKnowledgeClaimsByType(packet.Coordination.HardConstraints, "CONSTRAINT"); got < 2 {
		t.Fatalf("expected default coordination lane to preserve 2 constraints, got %d (%+v)", got, packet.Coordination.HardConstraints)
	}
	if got := len(packet.Coordination.AcceptedDecisions); got < 2 {
		t.Fatalf("expected default coordination lane to preserve 2 accepted decisions, got %d (%+v)", got, packet.Coordination.AcceptedDecisions)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.HardConstraints, "claim-rpc-default-coord-constraint-a") || !hasServerMemoryPacketClaim(packet.Coordination.HardConstraints, "claim-rpc-default-coord-constraint-b") {
		t.Fatalf("expected default coordination lane to preserve both hard constraints, got %+v", packet.Coordination.HardConstraints)
	}
	if !hasServerMemoryPacketClaim(packet.Coordination.AcceptedDecisions, "claim-rpc-default-coord-decision-a") || !hasServerMemoryPacketClaim(packet.Coordination.AcceptedDecisions, "claim-rpc-default-coord-decision-b") {
		t.Fatalf("expected default coordination lane to preserve both accepted decisions, got %+v", packet.Coordination.AcceptedDecisions)
	}
}

func TestWorkspaceMemoryPacketKernelRPCAdaptiveCoordinationFallbackBumpsWhenPressureSignalsExist(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-adaptive-coordination"
		taskID      = "task-handler-memory-packet-adaptive-coordination"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	for i := 0; i < 7; i++ {
		recordHandlerMemoryPacketClaim(t, ctx, store, sqlite.KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     fmt.Sprintf("claim-rpc-adaptive-coord-constraint-%02d", i),
			ClaimType:   "CONSTRAINT",
			Status:      "ACTIVE",
			Subject:     fmt.Sprintf("Constraint %02d", i),
			Body:        "Adaptive coordination fallback should surface one extra constraint when pressure exists.",
			Summary:     "Adaptive coordination constraint.",
			Confidence:  0.9 - float64(i)*0.01,
			SourceKind:  "workspace_memory",
			SourceID:    "developer",
			TaskID:      taskID,
		})
	}

	raw, err := json.Marshal(workspaceMemoryPacketParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Budget: &sqlite.MemoryPacketBudget{
			CoordinationFloor: 1,
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

	if _, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "rpc-adaptive-coordination-pressure",
		QueueType:   "REVIEW",
		Title:       "Adaptive coordination pressure",
		Summary:     "Open queue should trigger bounded coordination pressure.",
		TaskID:      taskID,
		Urgency:     "high",
		SourceKind:  "workspace_task",
		SourceID:    taskID,
	}); err != nil {
		t.Fatalf("upsert operator queue pressure: %v", err)
	}

	afterAny, rpcErr := h.workspaceMemoryPacketKernel(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPacketKernel rpc error after pressure: %+v", rpcErr)
	}
	after := afterAny.(map[string]any)["packet"].(sqlite.MemoryKernelPacket)
	if got, want := len(after.Coordination.HardConstraints), 7; got != want {
		t.Fatalf("expected bounded adaptive coordination bump to surface %d hard constraints under pressure, got %d (%+v)", want, got, after.Coordination.HardConstraints)
	}
}

func TestWorkspaceMemoryPacketShellRPCAdaptiveBridgeFallbackBumpsWhenFreshBridgePressureExists(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-packet-adaptive-bridge"
		taskID      = "task-handler-memory-packet-adaptive-bridge"
		sessionID   = "sess-handler-memory-packet-adaptive-bridge"
		agentID     = "agent-a"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Start shell adaptive bridge packet test",
	}); err != nil {
		t.Fatalf("record start coordination: %v", err)
	}
	for i := 0; i < 7; i++ {
		if _, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
			SnapshotID:          fmt.Sprintf("snapshot-rpc-adaptive-bridge-%02d", i),
			SessionID:           sessionID,
			WorkspaceID:         workspaceID,
			AgentID:             agentID,
			TriggerKind:         "token_budget_exceeded",
			PackMode:            "DETERMINISTIC_FALLBACK",
			SourceWindowDigest:  fmt.Sprintf("digest-rpc-adaptive-bridge-%02d", i),
			MessageCountBefore:  10 + i,
			MessageCountAfter:   5 + i,
			MessageTokensBefore: 1000 + i,
			MessageTokensAfter:  500 + i,
			TotalInputTokens:    1400 + i,
			TotalOutputTokens:   300 + i,
			SummaryText:         fmt.Sprintf("RPC adaptive bridge compaction %02d", i),
		}); err != nil {
			t.Fatalf("record compaction snapshot %d: %v", i, err)
		}
	}

	buildShell := func(budget *sqlite.MemoryPacketBudget) sqlite.MemoryShellPacket {
		raw, err := json.Marshal(workspaceMemoryPacketParams{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			SessionID:   sessionID,
			AgentID:     agentID,
			Budget:      budget,
		})
		if err != nil {
			t.Fatalf("marshal shell params: %v", err)
		}
		result, rpcErr := h.workspaceMemoryPacketShell(ctx, raw)
		if rpcErr != nil {
			t.Fatalf("workspaceMemoryPacketShell rpc error: %+v", rpcErr)
		}
		return result.(map[string]any)["packet"].(sqlite.MemoryShellPacket)
	}

	omittedBudget := &sqlite.MemoryPacketBudget{
		CoordinationFloor: 2,
		Lanes: map[string]sqlite.MemoryPacketLaneBudget{
			sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
			sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
		},
	}
	before := buildShell(omittedBudget)
	if got, want := len(before.RecentEpisodePacks), 6; got != want {
		t.Fatalf("expected omitted bridge lane baseline=%d without fresh bridge pressure, got %d (%+v)", want, got, before.RecentEpisodePacks)
	}

	locus, err := store.BuildInstrumentationLocusBundle(ctx, sqlite.InstrumentationLocusFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		SessionID:   sessionID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("build instrumentation locus: %v", err)
	}
	if !locus.Resolved || locus.ProtoClusterID == "" {
		t.Fatalf("expected resolved locus for adaptive bridge RPC test, got %+v", locus)
	}
	insertHandlerBridgeTensionFixture(t, ctx, store, workspaceID, locus.ProtoClusterID, taskID, sessionID, agentID)

	after := buildShell(omittedBudget)
	if got, want := len(after.RecentEpisodePacks), 7; got != want {
		t.Fatalf("expected bounded omitted bridge uplift=%d under fresh bridge pressure, got %d (%+v)", want, got, after.RecentEpisodePacks)
	}

	explicitBudget := &sqlite.MemoryPacketBudget{
		CoordinationFloor: 2,
		Lanes: map[string]sqlite.MemoryPacketLaneBudget{
			sqlite.MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
			sqlite.MemoryRetrievalLaneBridge:        {ItemLimit: 2},
			sqlite.MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
		},
	}
	explicit := buildShell(explicitBudget)
	if got := len(explicit.RecentEpisodePacks); got != 2 {
		t.Fatalf("expected explicit bridge lane to override adaptive uplift and stay at 2, got %d (%+v)", got, explicit.RecentEpisodePacks)
	}
}

func insertHandlerBridgeTensionFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, protoClusterID, taskID, sessionID, agentID string) {
	t.Helper()

	now := time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
	taskIDs, err := json.Marshal([]string{taskID})
	if err != nil {
		t.Fatalf("marshal task ids: %v", err)
	}
	sessionIDs, err := json.Marshal([]string{sessionID})
	if err != nil {
		t.Fatalf("marshal session ids: %v", err)
	}
	agentIDs, err := json.Marshal([]string{agentID})
	if err != nil {
		t.Fatalf("marshal agent ids: %v", err)
	}
	emptyList, err := json.Marshal([]string{})
	if err != nil {
		t.Fatalf("marshal empty list: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status,
			title, summary, anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json,
			artifact_refs_json, segment_refs_json, agent_ids_json, constraint_refs_json, base_score,
			surface_score, evidence_count, last_seen_event_id, last_seen_at, confirmed_by, archived_by,
			dismissed_reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"tension-rpc-adaptive-bridge",
		workspaceID,
		protoClusterID,
		"bridge",
		"ACTIVE",
		"CONFIRMED",
		"Confirmed bridge pressure",
		"Fresh bridge tension should unlock one bounded omitted bridge slot.",
		"task",
		taskID,
		string(taskIDs),
		string(sessionIDs),
		string(emptyList),
		string(emptyList),
		string(emptyList),
		string(agentIDs),
		string(emptyList),
		70,
		55,
		1,
		"event-rpc-adaptive-bridge",
		now,
		"developer",
		"",
		"",
		now,
		now,
	); err != nil {
		t.Fatalf("insert handler bridge tension fixture: %v", err)
	}
}
