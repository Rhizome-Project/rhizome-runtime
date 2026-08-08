package sqlite

import (
	"context"
	"fmt"
	"testing"
)

func TestMemoryPacketFreshnessExcludesNonAuthoritativeClaims(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-freshness")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-procedure-active",
		ClaimType:   "PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Active procedure",
		Body:        "Active procedure remains available.",
		Summary:     "Active procedure.",
		Confidence:  0.8,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		AgentID:     "agent-a",
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-procedure-stale",
		ClaimType:   "PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Stale procedure",
		Body:        "Stale procedure must not be used as current guidance.",
		Summary:     "Stale procedure.",
		Confidence:  0.99,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		AgentID:     "agent-a",
	})
	if _, err := store.MarkKnowledgeClaimStale(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-procedure-stale",
		ActorID:     "tests",
		Reason:      "procedure no longer matches runtime behavior",
	}); err != nil {
		t.Fatalf("mark procedure stale: %v", err)
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-procedure-weak",
		ClaimType:   "PROCEDURE",
		Status:      "ACTIVE",
		Subject:     "Weak procedure",
		Body:        "Weak provenance procedure must not be used as current guidance.",
		Summary:     "Weak procedure.",
		Confidence:  0.99,
		TaskID:      scenario.taskID,
		AgentID:     "agent-a",
	})

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-decision-a",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Old decision",
		Body:        "Old decision must be replaced.",
		Summary:     "Old decision.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-decision-b",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Replacement decision",
		Body:        "Replacement decision is the current accepted path.",
		Summary:     "Replacement decision.",
		Confidence:  0.88,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-decision-weak",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Weak decision",
		Body:        "Weak provenance decision must not be accepted.",
		Summary:     "Weak decision.",
		Confidence:  0.99,
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-constraint-weak",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Weak constraint",
		Body:        "Weak provenance constraint must not be hard context.",
		Summary:     "Weak constraint.",
		Confidence:  0.99,
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-blocker-weak",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Weak blocker",
		Body:        "Weak provenance blocker must not be active context.",
		Summary:     "Weak blocker.",
		Confidence:  0.99,
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-dissent-weak",
		ClaimType:   "DISSENT",
		Status:      "ACTIVE",
		Subject:     "Weak dissent",
		Body:        "Weak provenance dissent must not become contrastive context.",
		Summary:     "Weak dissent.",
		Confidence:  0.99,
		TaskID:      scenario.taskID,
	})
	if _, err := store.SupersedeKnowledgeClaim(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID:        scenario.workspaceID,
		ClaimID:            "claim-fresh-decision-a",
		ActorID:            "tests",
		Reason:             "replacement decision accepted",
		SupersedingClaimID: "claim-fresh-decision-b",
	}); err != nil {
		t.Fatalf("supersede old decision: %v", err)
	}

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-conflict-target",
		ClaimType:   "FACT",
		Status:      "CONFIRMED",
		Subject:     "Conflict target",
		Body:        "Conflict target anchors the dispute.",
		Summary:     "Conflict target.",
		Confidence:  0.7,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-blocker-disputed",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Disputed blocker",
		Body:        "Disputed blocker must not be promoted by higher confidence.",
		Summary:     "Disputed blocker.",
		Confidence:  0.99,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
	})
	if _, err := store.DisputeKnowledgeClaim(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID:      scenario.workspaceID,
		ClaimID:          "claim-fresh-blocker-disputed",
		ActorID:          "tests",
		Reason:           "conflicting claim requires review",
		ConflictsClaimID: "claim-fresh-conflict-target",
		ReviewDueAt:      "2099-01-01T00:00:00Z",
	}); err != nil {
		t.Fatalf("dispute blocker: %v", err)
	}

	kernel, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Budget: &MemoryPacketBudget{Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic:     {ItemLimit: 20},
			MemoryRetrievalLaneCoordination: {ItemLimit: 20},
		}},
	})
	if err != nil {
		t.Fatalf("build kernel packet: %v", err)
	}
	if kernel.Meta.Contract != memoryPacketContract {
		t.Fatalf("expected memory packet contract %q, got %+v", memoryPacketContract, kernel.Meta)
	}
	if hasKnowledgeClaim(kernel.Coordination.AcceptedDecisions, "claim-fresh-decision-a") {
		t.Fatalf("superseded decision leaked into accepted decisions: %+v", kernel.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(kernel.Coordination.AcceptedDecisions, "claim-fresh-decision-b") {
		t.Fatalf("replacement decision missing from accepted decisions: %+v", kernel.Coordination.AcceptedDecisions)
	}
	if hasKnowledgeClaim(kernel.Coordination.AcceptedDecisions, "claim-fresh-decision-weak") {
		t.Fatalf("weak provenance decision leaked into accepted decisions: %+v", kernel.Coordination.AcceptedDecisions)
	}
	if hasKnowledgeClaim(kernel.Coordination.HardConstraints, "claim-fresh-constraint-weak") {
		t.Fatalf("weak provenance constraint leaked into hard constraints: %+v", kernel.Coordination.HardConstraints)
	}
	if hasKnowledgeClaim(kernel.Coordination.ActiveBlockers, "claim-fresh-blocker-weak") {
		t.Fatalf("weak provenance blocker leaked into active blockers: %+v", kernel.Coordination.ActiveBlockers)
	}
	if hasKnowledgeClaim(kernel.Coordination.ActiveBlockers, "claim-fresh-blocker-disputed") {
		t.Fatalf("disputed blocker leaked into active blockers: %+v", kernel.Coordination.ActiveBlockers)
	}
	if kernel.ClaimFreshness == nil || kernel.ClaimFreshness.Contract != KnowledgeClaimFreshnessContract {
		t.Fatalf("expected claim freshness contract in kernel packet, got %+v", kernel.ClaimFreshness)
	}
	if kernel.ClaimFreshness.StaleClaimCount == 0 || kernel.ClaimFreshness.SupersededClaimCount == 0 || kernel.ClaimFreshness.DisputedClaimCount == 0 || kernel.ClaimFreshness.WeakProvenanceClaimCount == 0 || !kernel.ClaimFreshness.NeedsAttention {
		t.Fatalf("expected freshness summary to expose non-authoritative claims, got %+v", kernel.ClaimFreshness)
	}

	shell, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneSemantic:      {ItemLimit: 20},
			MemoryRetrievalLaneContrastive:   {ItemLimit: 20},
			MemoryRetrievalLaneProcedural:    {ItemLimit: 20},
			MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
		}},
	})
	if err != nil {
		t.Fatalf("build shell packet: %v", err)
	}
	if !hasKnowledgeClaim(shell.ProceduralClaims, "claim-fresh-procedure-active") {
		t.Fatalf("active procedure missing from shell procedural lane: %+v", shell.ProceduralClaims)
	}
	if hasKnowledgeClaim(shell.ProceduralClaims, "claim-fresh-procedure-stale") {
		t.Fatalf("stale procedure leaked into shell procedural lane: %+v", shell.ProceduralClaims)
	}
	if hasKnowledgeClaim(shell.ProceduralClaims, "claim-fresh-procedure-weak") {
		t.Fatalf("weak provenance procedure leaked into shell procedural lane: %+v", shell.ProceduralClaims)
	}
	if hasKnowledgeClaim(shell.DifferentialClaims, "claim-fresh-dissent-weak") {
		t.Fatalf("weak provenance dissent leaked into contrastive lane: %+v", shell.DifferentialClaims)
	}
}

func TestMemoryPacketFreshnessFilteringDoesNotUnderfillLaneWhenWeakClaimsCrowdLimit(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-freshness-crowded")
	records := []KnowledgeClaimInput{
		{ClaimID: "claim-crowded-decision-strong", ClaimType: "DECISION", Status: "CONFIRMED", Subject: "Strong decision", Body: "Strong decision survives weak crowding.", Summary: "Strong decision.", Confidence: 0.1, SourceKind: "workspace_memory", SourceID: "developer"},
		{ClaimID: "claim-crowded-decision-weak", ClaimType: "DECISION", Status: "CONFIRMED", Subject: "Weak decision", Body: "Weak decision must not consume lane.", Summary: "Weak decision.", Confidence: 0.99},
		{ClaimID: "claim-crowded-constraint-strong", ClaimType: "CONSTRAINT", Status: "ACTIVE", Subject: "Strong constraint", Body: "Strong constraint survives weak crowding.", Summary: "Strong constraint.", Confidence: 0.1, SourceKind: "workspace_memory", SourceID: "developer"},
		{ClaimID: "claim-crowded-constraint-weak", ClaimType: "CONSTRAINT", Status: "ACTIVE", Subject: "Weak constraint", Body: "Weak constraint must not consume lane.", Summary: "Weak constraint.", Confidence: 0.99},
		{ClaimID: "claim-crowded-blocker-strong", ClaimType: "BLOCKER", Status: "ACTIVE", Subject: "Strong blocker", Body: "Strong blocker survives weak crowding.", Summary: "Strong blocker.", Confidence: 0.1, SourceKind: "workspace_memory", SourceID: "developer"},
		{ClaimID: "claim-crowded-blocker-weak", ClaimType: "BLOCKER", Status: "ACTIVE", Subject: "Weak blocker", Body: "Weak blocker must not consume lane.", Summary: "Weak blocker.", Confidence: 0.99},
		{ClaimID: "claim-crowded-procedure-strong", ClaimType: "PROCEDURE", Status: "ACTIVE", Subject: "Strong procedure", Body: "Strong procedure survives weak crowding.", Summary: "Strong procedure.", Confidence: 0.1, SourceKind: "workspace_memory", SourceID: "developer"},
		{ClaimID: "claim-crowded-procedure-weak", ClaimType: "PROCEDURE", Status: "ACTIVE", Subject: "Weak procedure", Body: "Weak procedure must not consume lane.", Summary: "Weak procedure.", Confidence: 0.99},
		{ClaimID: "claim-crowded-dissent-strong", ClaimType: "DISSENT", Status: "ACTIVE", Subject: "Strong dissent", Body: "Strong dissent survives weak crowding.", Summary: "Strong dissent.", Confidence: 0.1, SourceKind: "workspace_memory", SourceID: "developer"},
		{ClaimID: "claim-crowded-dissent-weak", ClaimType: "DISSENT", Status: "ACTIVE", Subject: "Weak dissent", Body: "Weak dissent must not consume lane.", Summary: "Weak dissent.", Confidence: 0.99},
	}
	for _, input := range records {
		input.WorkspaceID = scenario.workspaceID
		input.TaskID = scenario.taskID
		recordMemoryPacketClaim(t, ctx, store, input)
	}

	budget := &MemoryPacketBudget{
		CoordinationFloor: 1,
		DissentQuota:      1,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneCoordination: {ItemLimit: 1},
			MemoryRetrievalLaneProcedural:   {ItemLimit: 1},
			MemoryRetrievalLaneContrastive:  {ItemLimit: 1},
		},
	}
	kernel, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		Budget:      budget,
	})
	if err != nil {
		t.Fatalf("build crowded kernel packet: %v", err)
	}
	if !hasKnowledgeClaim(kernel.Coordination.AcceptedDecisions, "claim-crowded-decision-strong") || hasKnowledgeClaim(kernel.Coordination.AcceptedDecisions, "claim-crowded-decision-weak") {
		t.Fatalf("expected weak decision not to starve strong decision, got %+v", kernel.Coordination.AcceptedDecisions)
	}
	if !hasKnowledgeClaim(kernel.Coordination.HardConstraints, "claim-crowded-constraint-strong") || hasKnowledgeClaim(kernel.Coordination.HardConstraints, "claim-crowded-constraint-weak") {
		t.Fatalf("expected weak constraint not to starve strong constraint, got %+v", kernel.Coordination.HardConstraints)
	}
	if !hasKnowledgeClaim(kernel.Coordination.ActiveBlockers, "claim-crowded-blocker-strong") || hasKnowledgeClaim(kernel.Coordination.ActiveBlockers, "claim-crowded-blocker-weak") {
		t.Fatalf("expected weak blocker not to starve strong blocker, got %+v", kernel.Coordination.ActiveBlockers)
	}

	shell, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget:      budget,
	})
	if err != nil {
		t.Fatalf("build crowded shell packet: %v", err)
	}
	if !hasKnowledgeClaim(shell.ProceduralClaims, "claim-crowded-procedure-strong") || hasKnowledgeClaim(shell.ProceduralClaims, "claim-crowded-procedure-weak") {
		t.Fatalf("expected weak procedure not to starve strong procedure, got %+v", shell.ProceduralClaims)
	}
	if !hasKnowledgeClaim(shell.DifferentialClaims, "claim-crowded-dissent-strong") || hasKnowledgeClaim(shell.DifferentialClaims, "claim-crowded-dissent-weak") {
		t.Fatalf("expected weak dissent not to starve strong dissent, got %+v", shell.DifferentialClaims)
	}
}

func TestMemoryPacketBasisDigestChangesOnClaimLifecycleStatusChange(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-freshness-digest")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-digest-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Digest decision",
		Body:        "Lifecycle changes must alter the packet basis.",
		Summary:     "Digest decision.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    "developer",
		TaskID:      scenario.taskID,
	})

	before, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Budget: &MemoryPacketBudget{Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneCoordination: {ItemLimit: 8},
		}},
	})
	if err != nil {
		t.Fatalf("build baseline kernel packet: %v", err)
	}
	if !hasKnowledgeClaim(before.Coordination.AcceptedDecisions, "claim-fresh-digest-decision") {
		t.Fatalf("baseline decision missing from packet: %+v", before.Coordination.AcceptedDecisions)
	}

	if _, err := store.MarkKnowledgeClaimStale(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-digest-decision",
		ActorID:     "tests",
		Reason:      "decision was invalidated by new evidence",
	}); err != nil {
		t.Fatalf("mark digest claim stale: %v", err)
	}

	after, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Budget: &MemoryPacketBudget{Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneCoordination: {ItemLimit: 8},
		}},
	})
	if err != nil {
		t.Fatalf("rebuild kernel packet: %v", err)
	}
	if before.Meta.BasisDigest == after.Meta.BasisDigest {
		t.Fatalf("expected lifecycle downgrade to change basis digest, got %s", after.Meta.BasisDigest)
	}
	if hasKnowledgeClaim(after.Coordination.AcceptedDecisions, "claim-fresh-digest-decision") {
		t.Fatalf("stale decision remained accepted after lifecycle change: %+v", after.Coordination.AcceptedDecisions)
	}
}

func TestRecoverableArchivedContrastiveClaimCarriesArchivedFreshness(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-freshness-archived")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-archived-dissent",
		ClaimType:   "DISSENT",
		Status:      "ACTIVE",
		Subject:     "Archived dissent",
		Body:        "Archived dissent may be recovered only as contrastive context.",
		Summary:     "Archived dissent.",
		Confidence:  0.65,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
	})
	if _, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-archived-dissent",
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive recoverable dissent: %v", err)
	}

	shell, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		Budget: &MemoryPacketBudget{Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneContrastive:   {ItemLimit: 4},
			MemoryRetrievalLaneDeterministic: {ItemLimit: 4},
		}},
	})
	if err != nil {
		t.Fatalf("build shell packet: %v", err)
	}
	claim := findKnowledgeClaim(shell.DifferentialClaims, "claim-fresh-archived-dissent")
	if claim == nil {
		t.Fatalf("expected recoverable archived dissent in shell differential lane, got %+v", shell.DifferentialClaims)
	}
	if claim.Freshness != "archived" || claim.DowngradeReason != "recoverable_archived_contrastive" || claim.ArchivedAt == nil {
		t.Fatalf("expected archived freshness metadata on recoverable claim, got %+v", claim)
	}
}

func TestMemoryCoherenceReportSurfacesClaimFreshnessAttention(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-freshness-coherence")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-coherence-stale",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Coherence stale fact",
		Body:        "Memory coherence must report stale claims.",
		Summary:     "Coherence stale fact.",
		Confidence:  0.92,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
	})
	if _, err := store.MarkKnowledgeClaimStale(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-fresh-coherence-stale",
		ActorID:     "tests",
		Reason:      "fact expired before long-running autonomy pass",
	}); err != nil {
		t.Fatalf("mark coherence claim stale: %v", err)
	}

	report, err := store.BuildMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("build memory coherence report: %v", err)
	}
	if report.ClaimFreshness == nil || report.ClaimFreshness.Contract != KnowledgeClaimFreshnessContract {
		t.Fatalf("expected coherence report claim freshness contract, got %+v", report.ClaimFreshness)
	}
	if !report.NeedsAttention || report.ClaimFreshness.StaleClaimCount == 0 || !containsString(report.AttentionReasons, "STALE_CLAIMS") {
		t.Fatalf("expected coherence report to surface stale claim attention, got report=%+v claim_freshness=%+v", report, report.ClaimFreshness)
	}
}

func TestKnowledgeClaimFreshnessSummaryAggregatesBeyondExampleLimit(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "memory-freshness-large")
	for idx := 0; idx < knowledgeClaimFreshnessMaxItems+1; idx++ {
		recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     fmt.Sprintf("claim-large-active-%03d", idx),
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     fmt.Sprintf("Active fact %d", idx),
			Body:        "Active fact remains fresh.",
			Summary:     "Active fact.",
			Confidence:  0.5,
			SourceKind:  "workspace_memory",
			SourceID:    "agent-a",
			TaskID:      scenario.taskID,
		})
	}
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-large-stale-tail",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Tail stale fact",
		Body:        "Tail stale fact must not disappear behind active claims.",
		Summary:     "Tail stale fact.",
		Confidence:  0.99,
		SourceKind:  "workspace_memory",
		SourceID:    "agent-a",
		TaskID:      scenario.taskID,
	})
	if _, err := store.MarkKnowledgeClaimStale(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-large-stale-tail",
		ActorID:     "tests",
		Reason:      "large workspace freshness regression",
	}); err != nil {
		t.Fatalf("mark large stale claim: %v", err)
	}

	summary, err := store.BuildKnowledgeClaimFreshnessSummary(ctx, scenario.workspaceID, "", 1)
	if err != nil {
		t.Fatalf("build knowledge claim freshness summary: %v", err)
	}
	if summary.TotalClaimCount != knowledgeClaimFreshnessMaxItems+2 || summary.StaleClaimCount != 1 || !summary.NeedsAttention {
		t.Fatalf("expected stale tail claim to be counted across full workspace, got %+v", summary)
	}
	if len(summary.Examples) != 1 || summary.Examples[0].ClaimID != "claim-large-stale-tail" {
		t.Fatalf("expected example limit to apply only to examples, got %+v", summary.Examples)
	}
}

func findKnowledgeClaim(records []KnowledgeClaimRecord, claimID string) *KnowledgeClaimRecord {
	for idx := range records {
		if records[idx].ClaimID == claimID {
			return &records[idx]
		}
	}
	return nil
}
