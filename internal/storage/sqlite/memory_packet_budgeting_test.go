package sqlite

import (
	"context"
	"fmt"
	"testing"
)

func TestBuildMemoryShellPacketBoundsRecoverableContrastiveQuota(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-shell-budget")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-marker",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Marker",
		Body:        "Active dissent marker should retain a contrastive slot.",
		Summary:     "Active marker.",
		Confidence:  0.74,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-content",
		ClaimType:   "DISSENT_CONTENT",
		Status:      "ACTIVE",
		Subject:     "Content",
		Body:        "Active dissent content should retain a contrastive slot.",
		Summary:     "Active content.",
		Confidence:  0.73,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-branch",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Branch",
		Body:        "Active alternative branch should compete for the remaining active slots.",
		Summary:     "Active branch.",
		Confidence:  0.72,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-archived-marker",
		ClaimType:   "DISSENT_MARKER",
		Status:      "ACTIVE",
		Subject:     "Archived marker",
		Body:        "Recoverable archived dissent marker should be capped by the contrastive quota.",
		Summary:     "Archived marker.",
		Confidence:  0.64,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if _, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-archived-marker",
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive recoverable marker: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-archived-branch",
		ClaimType:   "ALTERNATIVE_BRANCH",
		Status:      "ACTIVE",
		Subject:     "Archived branch",
		Body:        "Recoverable archived branch should not crowd out active contrastive claims.",
		Summary:     "Archived branch.",
		Confidence:  0.63,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if _, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-archived-branch",
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive recoverable branch: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-noise",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Noise",
		Body:        "Kernel-only claim should not enter the shell differential lane.",
		Summary:     "Noise.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-shell-budget-noise-b",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Noise B",
		Body:        "A second semantic fact keeps the active semantic lane saturated.",
		Summary:     "Noise B.",
		Confidence:  0.89,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{
			Lanes: map[string]MemoryPacketLaneBudget{
				MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
				MemoryRetrievalLaneContrastive:   {ItemLimit: 3},
				MemoryRetrievalLaneBridge:        {ItemLimit: 6},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 6},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}
	if got := len(packet.DifferentialClaims); got != 3 {
		t.Fatalf("expected bounded contrastive lane of 3 claims, got %d (%+v)", got, packet.DifferentialClaims)
	}

	activeCount := 0
	recoverableArchivedCount := 0
	for _, claim := range packet.DifferentialClaims {
		if hasKnowledgeClaim([]KnowledgeClaimRecord{claim}, "claim-shell-budget-noise") {
			t.Fatalf("did not expect kernel-only noise claim in differential lane: %+v", packet.DifferentialClaims)
		}
		if isMemoryPacketRecoverableArchivedContrastiveClaim(claim) {
			recoverableArchivedCount++
			continue
		}
		activeCount++
	}
	if activeCount != 2 || recoverableArchivedCount != 1 {
		t.Fatalf("expected 2 active and 1 recoverable archived contrastive claims, got active=%d archived=%d (%+v)", activeCount, recoverableArchivedCount, packet.DifferentialClaims)
	}
}

func TestMemoryPacketCoordinationLaneBudgetUsesIndependentDefaultAndFloorWhenUnset(t *testing.T) {
	t.Parallel()

	fallbackBudget := &MemoryPacketBudget{
		CoordinationFloor: 2,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic: {ItemLimit: 3},
		},
	}
	if got, want := memoryPacketCoordinationLimit(fallbackBudget), memoryPacketDefaultCoordination; got != want {
		t.Fatalf("expected coordination lane to use independent default=%d when explicit coordination lane is unset, got %d", want, got)
	}

	floorBudget := &MemoryPacketBudget{
		CoordinationFloor: memoryPacketDefaultCoordination + 1,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic: {ItemLimit: 1},
		},
	}
	if got, want := memoryPacketCoordinationLimit(floorBudget), memoryPacketDefaultCoordination+1; got != want {
		t.Fatalf("expected coordination lane to honor larger floor=%d when explicit coordination lane is unset, got %d", want, got)
	}

	explicitBudget := &MemoryPacketBudget{
		CoordinationFloor: 1,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic:     {ItemLimit: 2},
			MemoryRetrievalLaneCoordination: {ItemLimit: 5},
		},
	}
	if got, want := memoryPacketCoordinationLimit(explicitBudget), 5; got != want {
		t.Fatalf("expected explicit coordination lane budget=%d to override fallback behavior, got %d", want, got)
	}

	raisedExplicitBudget := &MemoryPacketBudget{
		CoordinationFloor: 4,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneCoordination: {ItemLimit: 2},
		},
	}
	if got, want := memoryPacketCoordinationLimit(raisedExplicitBudget), 4; got != want {
		t.Fatalf("expected coordination floor=%d to raise undersized explicit coordination lane, got %d", want, got)
	}
}

func TestBuildMemoryKernelPacketKeepsBoundedCoordinationFloorUnderTightSemanticBudgetWhenAvailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-kernel-floor")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-floor-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint",
		Body:        "Bounded coordination floor should preserve a hard constraint.",
		Summary:     "Constraint floor.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-floor-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Decision",
		Body:        "Bounded coordination floor should preserve an accepted decision.",
		Summary:     "Decision floor.",
		Confidence:  0.91,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-floor-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Blocker",
		Body:        "Bounded coordination floor should preserve an active blocker.",
		Summary:     "Blocker floor.",
		Confidence:  0.92,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	packet, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Budget: &MemoryPacketBudget{
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

	if len(packet.Coordination.HardConstraints) == 0 || len(packet.Coordination.AcceptedDecisions) == 0 || len(packet.Coordination.ActiveBlockers) == 0 {
		t.Fatalf("expected bounded coordination floor to preserve constraint/decision/blocker, got constraints=%d decisions=%d blockers=%d (%+v)", len(packet.Coordination.HardConstraints), len(packet.Coordination.AcceptedDecisions), len(packet.Coordination.ActiveBlockers), packet.Coordination)
	}
	if !hasKnowledgeClaim(packet.Coordination.HardConstraints, "claim-floor-constraint") {
		t.Fatalf("expected preserved hard constraint under tight semantic budget, got %+v", packet.Coordination.HardConstraints)
	}
	if !hasKnowledgeClaim(packet.Coordination.AcceptedDecisions, "claim-floor-decision") {
		t.Fatalf("expected preserved decision under tight semantic budget, got %+v", packet.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(packet.Coordination.ActiveBlockers, "claim-floor-blocker") {
		t.Fatalf("expected preserved blocker under tight semantic budget, got %+v", packet.Coordination.ActiveBlockers)
	}
	if !hasKnowledgeClaim(packet.Coordination.DecisionRecords, "claim-floor-decision") {
		t.Fatalf("expected decision_records alias to mirror preserved decision, got %+v", packet.Coordination.DecisionRecords)
	}
	if !hasKnowledgeClaim(packet.Coordination.BlockerSymptoms, "claim-floor-blocker") {
		t.Fatalf("expected blocker_symptoms alias to mirror preserved blocker, got %+v", packet.Coordination.BlockerSymptoms)
	}
}

func TestMemoryPacketCoordinationLimitPrefersExplicitLaneAndFallsBackToCurrentBehavior(t *testing.T) {
	t.Parallel()

	if got := memoryPacketCoordinationLimit(&MemoryPacketBudget{
		CoordinationFloor: 1,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic:     {ItemLimit: 1},
			MemoryRetrievalLaneCoordination: {ItemLimit: 4},
		},
	}); got != 4 {
		t.Fatalf("memoryPacketCoordinationLimit(explicit lane) = %d, want 4", got)
	}

	if got := memoryPacketCoordinationLimit(&MemoryPacketBudget{
		CoordinationFloor: 2,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic: {ItemLimit: 1},
		},
	}); got != memoryPacketDefaultCoordination {
		t.Fatalf("memoryPacketCoordinationLimit(independent default) = %d, want %d", got, memoryPacketDefaultCoordination)
	}

	if got := memoryPacketCoordinationLimit(&MemoryPacketBudget{
		CoordinationFloor: memoryPacketDefaultCoordination + 1,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic: {ItemLimit: 3},
		},
	}); got != memoryPacketDefaultCoordination+1 {
		t.Fatalf("memoryPacketCoordinationLimit(floor raises default) = %d, want %d", got, memoryPacketDefaultCoordination+1)
	}
}

func TestMemoryPacketAdaptiveCoordinationLimitBumpsOnlyForOmittedLanePressure(t *testing.T) {
	t.Parallel()

	omittedBudget := &MemoryPacketBudget{
		CoordinationFloor: 1,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic: {ItemLimit: 1},
		},
	}
	if got, want := memoryPacketAdaptiveCoordinationLimit(omittedBudget, InstrumentationLocusBundle{}, nil, false), memoryPacketDefaultCoordination; got != want {
		t.Fatalf("adaptive coordination limit without pressure = %d, want %d", got, want)
	}
	if got, want := memoryPacketAdaptiveCoordinationLimit(omittedBudget, InstrumentationLocusBundle{}, []OperatorQueueRecord{{QueueID: "queue-1"}}, false), memoryPacketDefaultCoordination+memoryPacketAdaptiveCoordBump; got != want {
		t.Fatalf("adaptive coordination limit with queue pressure = %d, want %d", got, want)
	}
	if got, want := memoryPacketAdaptiveCoordinationLimit(omittedBudget, InstrumentationLocusBundle{
		Control: &ControlClusterDetail{
			Cluster: ControlClusterReport{
				Signals: ControlSignalVector{CoordinationPressure: memoryPacketAdaptiveCoordSignal},
			},
		},
	}, nil, false), memoryPacketDefaultCoordination+memoryPacketAdaptiveCoordBump; got != want {
		t.Fatalf("adaptive coordination limit with fresh control pressure = %d, want %d", got, want)
	}
	if got, want := memoryPacketAdaptiveCoordinationLimit(&MemoryPacketBudget{
		CoordinationFloor: 1,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneCoordination: {ItemLimit: 3},
		},
	}, InstrumentationLocusBundle{}, []OperatorQueueRecord{{QueueID: "queue-1"}}, true), 3; got != want {
		t.Fatalf("adaptive coordination limit with explicit lane = %d, want %d", got, want)
	}
}

func TestBuildMemoryKernelPacketUsesIndependentDefaultCoordinationLaneWhenUnset(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-kernel-default-coordination-lane")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-default-coord-constraint-a",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint A",
		Body:        "Default coordination lane should keep multiple hard constraints independent of semantic budget.",
		Summary:     "Constraint A.",
		Confidence:  0.84,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-default-coord-constraint-b",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint B",
		Body:        "Second hard constraint should still survive when no explicit coordination lane is provided.",
		Summary:     "Constraint B.",
		Confidence:  0.83,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-default-coord-decision-a",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision A",
		Body:        "Default coordination lane should keep multiple accepted decisions independent of semantic budget.",
		Summary:     "Decision A.",
		Confidence:  0.82,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-default-coord-decision-b",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision B",
		Body:        "Second accepted decision should still survive when no explicit coordination lane is provided.",
		Summary:     "Decision B.",
		Confidence:  0.81,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-default-coord-fact-a",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact A",
		Body:        "Semantic lane remains tight while coordination lane is independent.",
		Summary:     "Fact A.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-default-coord-fact-b",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact B",
		Body:        "Second fact competes only in the semantic lane.",
		Summary:     "Fact B.",
		Confidence:  0.94,
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

	if got := len(packet.Coordination.HardConstraints); got < 2 {
		t.Fatalf("expected independent default coordination lane to preserve 2 hard constraints, got %d (%+v)", got, packet.Coordination.HardConstraints)
	}
	if got := len(packet.Coordination.AcceptedDecisions); got < 2 {
		t.Fatalf("expected independent default coordination lane to preserve 2 accepted decisions, got %d (%+v)", got, packet.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(packet.Coordination.HardConstraints, "claim-default-coord-constraint-a") || !hasKnowledgeClaim(packet.Coordination.HardConstraints, "claim-default-coord-constraint-b") {
		t.Fatalf("expected default coordination lane to preserve both hard constraints, got %+v", packet.Coordination.HardConstraints)
	}
	if !hasKnowledgeClaim(packet.Coordination.AcceptedDecisions, "claim-default-coord-decision-a") || !hasKnowledgeClaim(packet.Coordination.AcceptedDecisions, "claim-default-coord-decision-b") {
		t.Fatalf("expected default coordination lane to preserve both accepted decisions, got %+v", packet.Coordination.AcceptedDecisions)
	}

	referenceAt := memoryPacketTestReferenceAt(t, ctx, store, scenario.workspaceID)
	semanticClaims, err := store.listMemoryPacketClaims(ctx, scenario.workspaceID, scenario.taskID, scenario.sessionID, referenceAt, 1, 0, 0)
	if err != nil {
		t.Fatalf("list mixed semantic claims: %v", err)
	}
	if got := countKnowledgeClaimsByType(semanticClaims, "FACT"); got != 1 {
		t.Fatalf("expected semantic lane to remain tight at 1 fact claim, got %d (%+v)", got, semanticClaims)
	}
}

func TestBuildMemoryKernelPacketAdaptiveCoordinationFallbackBumpsWhenPressureSignalsExist(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-kernel-adaptive-coordination"
		taskID      = "task-memory-kernel-adaptive-coordination"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-memory-kernel-adaptive-coordination")

	for i := 0; i < memoryPacketDefaultCoordination+1; i++ {
		recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     fmt.Sprintf("claim-adaptive-coord-constraint-%02d", i),
			ClaimType:   "CONSTRAINT",
			Status:      "ACTIVE",
			Subject:     fmt.Sprintf("Constraint %02d", i),
			Body:        "Adaptive coordination fallback should surface one extra constraint when coordination pressure exists.",
			Summary:     "Adaptive coordination constraint.",
			Confidence:  0.9 - float64(i)*0.01,
			SourceKind:  "workspace_memory",
			SourceID:    "developer",
			TaskID:      taskID,
		})
	}

	buildPacket := func() MemoryKernelPacket {
		packet, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
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
		return packet
	}

	if _, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "adaptive-coordination-pressure",
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

	after := buildPacket()
	if got, want := len(after.Coordination.HardConstraints), memoryPacketDefaultCoordination+memoryPacketAdaptiveCoordBump; got != want {
		t.Fatalf("expected bounded adaptive coordination bump=%d under pressure, got %d (%+v)", want, got, after.Coordination.HardConstraints)
	}
}

func TestBuildMemoryShellPacketAdaptiveBridgeFallbackBumpsWhenFreshBridgePressureExists(t *testing.T) {
	t.Parallel()

	omittedBudget := &MemoryPacketBudget{
		CoordinationFloor: 2,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
			MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
		},
	}

	missingBridgePressure := InstrumentationLocusBundle{
		Control: &ControlClusterDetail{
			Cluster: ControlClusterReport{
				MetricsMissing:        false,
				BasisStale:            false,
				Signals:               ControlSignalVector{CoordinationPressure: memoryPacketAdaptiveCoordSignal},
				ConfirmedCountsByType: map[string]int{},
			},
		},
	}
	if got, want := memoryPacketAdaptiveBridgeLimit(omittedBudget, missingBridgePressure, false), memoryPacketDefaultPacks; got != want {
		t.Fatalf("expected omitted bridge lane baseline=%d without confirmed fresh bridge pressure, got %d", want, got)
	}

	freshBridgePressure := InstrumentationLocusBundle{
		Control: &ControlClusterDetail{
			Cluster: ControlClusterReport{
				MetricsMissing:        false,
				BasisStale:            false,
				Signals:               ControlSignalVector{CoordinationPressure: memoryPacketAdaptiveCoordSignal},
				ConfirmedCountsByType: map[string]int{"bridge": 1},
			},
		},
	}
	if got, want := memoryPacketAdaptiveBridgeLimit(omittedBudget, freshBridgePressure, false), memoryPacketDefaultPacks+memoryPacketAdaptiveCoordBump; got != want {
		t.Fatalf("expected bounded omitted bridge uplift=%d under fresh bridge pressure, got %d", want, got)
	}

	explicitBudget := &MemoryPacketBudget{
		CoordinationFloor: 2,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic:      {ItemLimit: 1},
			MemoryRetrievalLaneBridge:        {ItemLimit: 2},
			MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
		},
	}
	if got := memoryPacketAdaptiveBridgeLimit(explicitBudget, freshBridgePressure, true); got != 2 {
		t.Fatalf("expected explicit bridge lane to override adaptive uplift and stay at 2, got %d", got)
	}
}

func TestBuildMemoryKernelPacketCoordinationLaneCanExceedSemanticLaneWithoutPolicyCutover(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-kernel-coordination-lane")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-lane-constraint-a",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint A",
		Body:        "Explicit coordination lane should retain multiple coordination claims even under tight semantic budget.",
		Summary:     "Constraint A.",
		Confidence:  0.81,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-lane-constraint-b",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint B",
		Body:        "Second constraint should still fit through the dedicated coordination lane.",
		Summary:     "Constraint B.",
		Confidence:  0.80,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-lane-fact-a",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact A",
		Body:        "Mixed semantic lane stays tight for ordinary semantic claims.",
		Summary:     "Fact A.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coord-lane-fact-b",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact B",
		Body:        "Second fact competes for the same tight semantic slot.",
		Summary:     "Fact B.",
		Confidence:  0.94,
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
				MemoryRetrievalLaneCoordination:  {ItemLimit: 2},
				MemoryRetrievalLaneBridge:        {ItemLimit: 2},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory kernel packet: %v", err)
	}

	if got := countKnowledgeClaimsByType(packet.Coordination.HardConstraints, "CONSTRAINT"); got != 2 {
		t.Fatalf("expected explicit coordination lane to preserve 2 constraints, got %d (%+v)", got, packet.Coordination.HardConstraints)
	}

	referenceAt := memoryPacketTestReferenceAt(t, ctx, store, scenario.workspaceID)
	semanticClaims, err := store.listMemoryPacketClaims(ctx, scenario.workspaceID, scenario.taskID, scenario.sessionID, referenceAt, 1, 0, 0)
	if err != nil {
		t.Fatalf("list mixed semantic claims: %v", err)
	}
	if got := countKnowledgeClaimsByType(semanticClaims, "FACT"); got != 1 {
		t.Fatalf("expected mixed semantic lane to stay tight at 1 fact claim, got %d (%+v)", got, semanticClaims)
	}
}

func countKnowledgeClaimsByType(records []KnowledgeClaimRecord, claimType string) int {
	count := 0
	for _, record := range records {
		if record.ClaimType == claimType {
			count++
		}
	}
	return count
}

func TestBuildMemoryKernelPacketUsesExplicitCoordinationLaneBudgetWhenProvided(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-kernel-coordination-budget")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coordination-budget-decision-a",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision A",
		Body:        "First accepted decision should survive explicit coordination budgeting.",
		Summary:     "Decision A.",
		Confidence:  0.91,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coordination-budget-decision-b",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision B",
		Body:        "Second accepted decision should survive explicit coordination budgeting.",
		Summary:     "Decision B.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-coordination-budget-fact",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact",
		Body:        "Semantic lane stays tighter than coordination lane.",
		Summary:     "Fact budget.",
		Confidence:  0.7,
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
				MemoryRetrievalLaneCoordination:  {ItemLimit: 2},
				MemoryRetrievalLaneBridge:        {ItemLimit: 2},
				MemoryRetrievalLaneDeterministic: {ItemLimit: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("build memory kernel packet: %v", err)
	}

	if got := len(packet.Coordination.AcceptedDecisions); got < 2 {
		t.Fatalf("expected explicit coordination lane budget to preserve 2 accepted decisions, got %d (%+v)", got, packet.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(packet.Coordination.AcceptedDecisions, "claim-coordination-budget-decision-a") || !hasKnowledgeClaim(packet.Coordination.AcceptedDecisions, "claim-coordination-budget-decision-b") {
		t.Fatalf("expected explicit coordination lane budget to preserve both decisions, got %+v", packet.Coordination.AcceptedDecisions)
	}
	if got := len(packet.Coordination.DecisionRecords); got < 2 {
		t.Fatalf("expected decision_records alias to preserve 2 accepted decisions, got %d (%+v)", got, packet.Coordination.DecisionRecords)
	}
}

func TestMemoryPacketSemanticLaneExcludesCoordinationTypesOnceDedicatedCoordinationFetchExists(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-packet-semantic-vs-coordination")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-semantic-fact",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Fact",
		Body:        "Semantic lane should still surface ordinary fact claims.",
		Summary:     "Fact lane.",
		Confidence:  0.71,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-semantic-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Constraint",
		Body:        "Coordination claims should no longer ride the mixed semantic lane.",
		Summary:     "Constraint lane.",
		Confidence:  0.81,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-semantic-decision",
		ClaimType:   "DECISION_RECORD",
		Status:      "CONFIRMED",
		Subject:     "Decision",
		Body:        "Decisions should now arrive only through dedicated coordination fetch.",
		Summary:     "Decision lane.",
		Confidence:  0.82,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-semantic-blocker",
		ClaimType:   "BLOCKER_HYPOTHESIS",
		Status:      "ACTIVE",
		Subject:     "Blocker hypothesis",
		Body:        "Blocker hypotheses should now arrive only through dedicated coordination fetch.",
		Summary:     "Blocker lane.",
		Confidence:  0.83,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	referenceAt := memoryPacketTestReferenceAt(t, ctx, store, scenario.workspaceID)
	semanticClaims, err := store.listMemoryPacketClaims(ctx, scenario.workspaceID, scenario.taskID, scenario.sessionID, referenceAt, 8, 0, 0)
	if err != nil {
		t.Fatalf("list memory packet claims: %v", err)
	}
	if !hasKnowledgeClaim(semanticClaims, "claim-semantic-fact") {
		t.Fatalf("expected semantic lane to keep ordinary facts, got %+v", semanticClaims)
	}
	if hasKnowledgeClaim(semanticClaims, "claim-semantic-constraint") || hasKnowledgeClaim(semanticClaims, "claim-semantic-decision") || hasKnowledgeClaim(semanticClaims, "claim-semantic-blocker") {
		t.Fatalf("expected semantic lane to exclude coordination types once dedicated coordination fetch exists, got %+v", semanticClaims)
	}

	coordinationClaims, err := store.listMemoryPacketKernelCoordinationClaims(ctx, scenario.workspaceID, scenario.taskID, scenario.sessionID, referenceAt, 8)
	if err != nil {
		t.Fatalf("list memory packet coordination claims: %v", err)
	}
	if !hasKnowledgeClaim(coordinationClaims, "claim-semantic-constraint") || !hasKnowledgeClaim(coordinationClaims, "claim-semantic-decision") || !hasKnowledgeClaim(coordinationClaims, "claim-semantic-blocker") {
		t.Fatalf("expected dedicated coordination fetch to preserve coordination types, got %+v", coordinationClaims)
	}
}

func memoryPacketTestReferenceAt(t *testing.T, ctx context.Context, store *Store, workspaceID string) string {
	t.Helper()
	authority, err := store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	return generatedAtFromWorkspaceTimeAuthority(authority)
}
