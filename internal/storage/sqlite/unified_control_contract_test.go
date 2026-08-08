package sqlite

import (
	"context"
	"reflect"
	"testing"
)

func TestBuildUnifiedControlReportKeepsDeterministicOrderAndDedupesOverlappingActions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "unified-control-contract")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for unified control contract test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-a",
		SessionID:   scenario.sessionID,
		ReportScope: "SESSION",
		ReportID:    "memres-unified-control-contract",
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:     "P1",
				ReplicaKind:       "kernel",
				CoherenceClass:    "A",
				State:             "CURRENT",
				CanonicalMemoryID: "mem:" + scenario.workspaceID,
				CacheKey:          "cache:" + scenario.workspaceID,
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: scenario.docKey, VersionToken: "outdated", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	records, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-a",
		SessionID:   scenario.sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory invalidations: %v", err)
	}
	if len(records) == 0 {
		t.Fatalf("expected memory residency to enqueue an invalidation")
	}
	redeliverMemoryInvalidationForTest(t, store, ctx, scenario.workspaceID, "agent-a", records[0].InvalidationID)
	for i := 0; i < 3; i++ {
		failed, err := store.FailMemoryInvalidations(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     scenario.workspaceID,
			AgentID:         "agent-a",
			InvalidationIDs: []string{records[0].InvalidationID},
			FailureReason:   "dead letter for unified control contract test",
		})
		if err != nil {
			t.Fatalf("fail memory invalidations %d: %v", i+1, err)
		}
		if len(failed) == 0 {
			t.Fatalf("expected fail attempt %d to change the invalidation, got none", i+1)
		}
		if i < 2 {
			redeliverMemoryInvalidationForTest(t, store, ctx, scenario.workspaceID, "agent-a", records[0].InvalidationID)
		}
	}

	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	for i := 0; i < 2; i++ {
		if _, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
			WorkspaceID:    scenario.workspaceID,
			ProtoClusterID: clusterID,
			ActorID:        "tests",
		}); err != nil {
			t.Fatalf("tick cluster control state %d: %v", i+1, err)
		}
	}

	first, err := store.BuildUnifiedControlReport(ctx, UnifiedControlReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build first unified control report: %v", err)
	}
	second, err := store.BuildUnifiedControlReport(ctx, UnifiedControlReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build second unified control report: %v", err)
	}

	if !first.Resolved || !second.Resolved {
		t.Fatalf("expected resolved unified control reports, got first=%+v second=%+v", first, second)
	}
	if !equalStringSlices(first.ControlOrder, []string{
		"event_time_ingest",
		"rsp_epoch_update",
		"rrp_coordination_update",
		"rmp_memory_update",
		"arbitration_and_saturation",
	}) {
		t.Fatalf("unexpected control order in first report: %+v", first.ControlOrder)
	}
	if !equalStringSlices(first.ControlOrder, second.ControlOrder) {
		t.Fatalf("expected deterministic control order, got first=%+v second=%+v", first.ControlOrder, second.ControlOrder)
	}
	if !first.CapabilityFlags.GovernedHintsLive || !second.CapabilityFlags.GovernedHintsLive {
		t.Fatalf("expected governed hints capability to be visible in both reports, got first=%+v second=%+v", first.CapabilityFlags, second.CapabilityFlags)
	}
	if first.MemoryCoherenceBand != "CRITICAL" || second.MemoryCoherenceBand != "CRITICAL" {
		t.Fatalf("expected critical memory coherence with dead-letter invalidations, got first=%+v second=%+v", first.MemoryCoherenceBand, second.MemoryCoherenceBand)
	}
	if first.EffectiveControls.ContextCap != 4 || second.EffectiveControls.ContextCap != 4 {
		t.Fatalf("expected critical memory coherence floor to clamp context cap to 4, got first=%+v second=%+v", first.EffectiveControls, second.EffectiveControls)
	}
	if len(first.GovernedHints) == 0 || len(second.GovernedHints) == 0 {
		t.Fatalf("expected governed hints to be materialized, got first=%+v second=%+v", first.GovernedHints, second.GovernedHints)
	}
	if len(first.AppliedActions) < 3 || first.AppliedActions[0] != "memory_coherence_floor" || first.AppliedActions[1] != "prefer_kernel_refresh" {
		t.Fatalf("expected unified arbitration to start with memory coherence floor and refresh, got %+v", first.AppliedActions)
	}
	if countStringSlice(first.AppliedActions, "prefer_kernel_refresh") != 1 {
		t.Fatalf("expected prefer_kernel_refresh to be deduped across overlapping paths, got %+v", first.AppliedActions)
	}
	if countAppliedActionAudit(first.AppliedActionAudit, "prefer_kernel_refresh") != 1 {
		t.Fatalf("expected prefer_kernel_refresh structured audit to be deduped too, got %+v", first.AppliedActionAudit)
	}
	if !equalStringSlices(first.AppliedActions, second.AppliedActions) {
		t.Fatalf("expected deterministic applied actions, got first=%+v second=%+v", first.AppliedActions, second.AppliedActions)
	}
	if !reflect.DeepEqual(first.AppliedActionAudit, second.AppliedActionAudit) {
		t.Fatalf("expected deterministic structured applied audit, got first=%+v second=%+v", first.AppliedActionAudit, second.AppliedActionAudit)
	}
	if !reflect.DeepEqual(first.AuditSummary, second.AuditSummary) {
		t.Fatalf("expected deterministic audit summary rollup, got first=%+v second=%+v", first.AuditSummary, second.AuditSummary)
	}
	if !reflect.DeepEqual(first.AuditCoverage, second.AuditCoverage) {
		t.Fatalf("expected deterministic audit coverage rollup, got first=%+v second=%+v", first.AuditCoverage, second.AuditCoverage)
	}
	if !reflect.DeepEqual(first.EffectiveControlBasis, second.EffectiveControlBasis) {
		t.Fatalf("expected deterministic effective-control basis rollup, got first=%+v second=%+v", first.EffectiveControlBasis, second.EffectiveControlBasis)
	}
	if !reflect.DeepEqual(first.EffectiveControlBasisSummary, second.EffectiveControlBasisSummary) {
		t.Fatalf("expected deterministic effective-control basis summary rollup, got first=%+v second=%+v", first.EffectiveControlBasisSummary, second.EffectiveControlBasisSummary)
	}
	if !reflect.DeepEqual(first.ContradictionSummary, second.ContradictionSummary) {
		t.Fatalf("expected deterministic contradiction summary rollup, got first=%+v second=%+v", first.ContradictionSummary, second.ContradictionSummary)
	}
	if !reflect.DeepEqual(first.CooldownBasis, second.CooldownBasis) {
		t.Fatalf("expected deterministic cooldown basis rollup, got first=%+v second=%+v", first.CooldownBasis, second.CooldownBasis)
	}
	if !equalStringSlices(first.SuppressedHints, second.SuppressedHints) || len(first.SuppressedHints) != 0 {
		t.Fatalf("expected no suppressed hints for supported actions, got first=%+v second=%+v", first.SuppressedHints, second.SuppressedHints)
	}
	if !reflect.DeepEqual(first.SuppressedHintAudit, second.SuppressedHintAudit) {
		t.Fatalf("expected deterministic structured suppressed audit, got first=%+v second=%+v", first.SuppressedHintAudit, second.SuppressedHintAudit)
	}
	if !reflect.DeepEqual(first.GovernedHintOutcomes, second.GovernedHintOutcomes) {
		t.Fatalf("expected deterministic governed hint outcomes, got first=%+v second=%+v", first.GovernedHintOutcomes, second.GovernedHintOutcomes)
	}
	if !reflect.DeepEqual(first.GovernedHintSummary, second.GovernedHintSummary) {
		t.Fatalf("expected deterministic governed-hint summary rollup, got first=%+v second=%+v", first.GovernedHintSummary, second.GovernedHintSummary)
	}
	if first.GovernedHintSummary == nil || first.GovernedHintSummary.TotalHints != len(first.GovernedHints) || first.GovernedHintSummary.OutcomeCount["ADVISORY_ROUTED"] == 0 {
		t.Fatalf("expected governed-hint summary rollup on deterministic report, got %+v", first.GovernedHintSummary)
	}
	preferKernelRefresh := findAppliedActionAudit(first.AppliedActionAudit, "prefer_kernel_refresh")
	if preferKernelRefresh == nil {
		t.Fatalf("expected structured audit entry for prefer_kernel_refresh, got %+v", first.AppliedActionAudit)
	}
	if !containsString(preferKernelRefresh.SourceKinds, "memory_coherence_floor") || !containsString(preferKernelRefresh.SourceKinds, "governed_hint") {
		t.Fatalf("expected overlapping prefer_kernel_refresh trace to keep both source kinds, got %+v", preferKernelRefresh)
	}
	if len(preferKernelRefresh.HintIDs) == 0 {
		t.Fatalf("expected overlapping prefer_kernel_refresh trace to retain governed hint ids, got %+v", preferKernelRefresh)
	}
	if first.AuditSummary == nil || first.AuditSummary.AppliedEntryCount != len(first.AppliedActionAudit) || first.AuditSummary.HintBackedActionCount == 0 || first.AuditSummary.DeltaBearingActionCount == 0 {
		t.Fatalf("expected deterministic report to surface audit summary rollup, got %+v", first.AuditSummary)
	}
	if first.AuditSummary.AppliedSourceKindCount["memory_coherence_floor"] == 0 || first.AuditSummary.AppliedSourceKindCount["governed_hint"] == 0 {
		t.Fatalf("expected audit summary to retain overlapping source-kind counts, got %+v", first.AuditSummary)
	}
	if first.AuditCoverage == nil || first.AuditCoverage.AppliedEntriesWithSourceKinds != len(first.AppliedActionAudit) || first.AuditCoverage.FullAppliedTraceEntryCount == 0 {
		t.Fatalf("expected deterministic report to surface audit coverage rollup, got %+v", first.AuditCoverage)
	}
	mergeThresholdBasis := findEffectiveControlBasis(first.EffectiveControlBasis, "merge_threshold")
	if mergeThresholdBasis == nil || !mergeThresholdBasis.Changed || !containsString(mergeThresholdBasis.AppliedActions, "memory_coherence_floor") {
		t.Fatalf("expected deterministic report to surface merge_threshold basis with memory floor provenance, got %+v", first.EffectiveControlBasis)
	}
	if first.EffectiveControlBasisSummary == nil || first.EffectiveControlBasisSummary.FieldCount != len(first.EffectiveControlBasis) || first.EffectiveControlBasisSummary.FieldsWithActionTraceCount == 0 {
		t.Fatalf("expected deterministic report to surface effective-control basis summary, got %+v", first.EffectiveControlBasisSummary)
	}
	if first.ContradictionSummary == nil || first.ContradictionSummary.TotalCount != len(first.Contradictions) || first.ContradictionSummary.HardSafetyClampCount == 0 {
		t.Fatalf("expected deterministic report to surface contradiction summary, got %+v contradictions=%+v", first.ContradictionSummary, first.Contradictions)
	}
	if first.CooldownBasis == nil || first.CooldownBasis.CooldownActive != first.CooldownActive || first.CooldownBasis.CurrentMode != first.ControlMode || first.CooldownBasis.CandidateMode != first.CandidateMode || first.CooldownBasis.Stage == "" || first.CooldownBasis.Reason == "" || first.CooldownBasis.Summary == "" {
		t.Fatalf("expected deterministic report to surface cooldown basis aligned with control state, got %+v report=%+v", first.CooldownBasis, first)
	}
	if first.CooldownBasis.AcceptanceReadiness == "" {
		t.Fatalf("expected deterministic report to surface bounded acceptance readiness, got %+v", first.CooldownBasis)
	}
	if first.CooldownBasis.AcceptanceGateReason == "" {
		t.Fatalf("expected deterministic report to surface bounded acceptance gate reason, got %+v", first.CooldownBasis)
	}
	if first.CooldownBasis.AcceptanceChecklist == nil {
		t.Fatalf("expected deterministic report to surface bounded acceptance checklist, got %+v", first.CooldownBasis)
	}
	if first.CooldownBasis.AcceptanceChecklist.CandidatePresent != (first.CooldownBasis.CandidateMode != "") || first.CooldownBasis.AcceptanceChecklist.CandidateDiverges != (first.CooldownBasis.CandidateMode != "" && first.CooldownBasis.CandidateMode != first.CooldownBasis.CurrentMode) {
		t.Fatalf("expected deterministic report to keep acceptance checklist aligned with candidate context, got %+v", first.CooldownBasis.AcceptanceChecklist)
	}
	if first.CooldownBasis.AcceptanceChecklist.HysteresisSatisfied != (first.CooldownBasis.CandidateMode != "" && first.CooldownBasis.CandidateMode != first.CooldownBasis.CurrentMode && first.CooldownBasis.ReadyToStabilize) {
		t.Fatalf("expected deterministic report to keep acceptance checklist hysteresis aligned, got %+v", first.CooldownBasis.AcceptanceChecklist)
	}
	if first.CooldownBasis.AcceptanceChecklist.CooldownClear != !first.CooldownBasis.CooldownActive || first.CooldownBasis.AcceptanceChecklist.ContradictionClear != (len(first.Contradictions) == 0) || first.CooldownBasis.AcceptanceChecklist.MemoryAttentionClear != !first.MemoryNeedsAttention {
		t.Fatalf("expected deterministic report to keep acceptance checklist aligned with report state, got checklist=%+v report=%+v", first.CooldownBasis.AcceptanceChecklist, first)
	}
	if expectedMissing := buildUnifiedControlAcceptanceMissingRequirements(first.CooldownBasis.AcceptanceReadiness, first.CooldownBasis.AcceptanceChecklist); !reflect.DeepEqual(first.CooldownBasis.AcceptanceMissingRequirements, expectedMissing) {
		t.Fatalf("expected deterministic report to keep acceptance missing requirements aligned, got actual=%+v expected=%+v", first.CooldownBasis.AcceptanceMissingRequirements, expectedMissing)
	}
	if expectedBand := buildUnifiedControlAcceptanceProgressBand(first.CooldownBasis.AcceptanceReadiness, first.CooldownBasis.AcceptanceChecklist, first.CooldownBasis.AcceptanceMissingRequirements); first.CooldownBasis.AcceptanceProgressBand != expectedBand {
		t.Fatalf("expected deterministic report to keep acceptance progress band aligned, got actual=%q expected=%q", first.CooldownBasis.AcceptanceProgressBand, expectedBand)
	}
	if first.CooldownBasis.RequiredStreak <= 0 || first.CooldownBasis.RemainingStreak < 0 {
		t.Fatalf("expected deterministic report to surface bounded cooldown transition window, got %+v", first.CooldownBasis)
	}
	if first.CooldownBasis.BlockingReasonCount != len(first.CooldownBasis.BlockingReasons) {
		t.Fatalf("expected deterministic report to keep cooldown blocking counts aligned, got %+v", first.CooldownBasis)
	}
	if first.CooldownBasis.CooldownActive && first.CooldownBasis.ReadyToStabilize {
		if first.CooldownBasis.AcceptanceReadiness != "READY_PENDING" || first.CooldownBasis.AcceptanceGateReason != "COOLDOWN_ACTIVE" || first.CooldownBasis.AcceptanceProgressBand == "FULLY_CLEAR" || !containsString(first.CooldownBasis.AcceptanceMissingRequirements, "cooldown_clear") {
			t.Fatalf("expected cooldown-active ready window to stay pending but not fully clear on deterministic report, got %+v", first.CooldownBasis)
		}
	}
}

func TestBuildUnifiedControlReportKeepsGovernedHintsShadowedUntilCapabilityEnabled(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "unified-control-contract-shadow")

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-a",
		SessionID:   scenario.sessionID,
		ReportScope: "SESSION",
		ReportID:    "memres-unified-control-contract-shadow",
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:     "P1",
				ReplicaKind:       "kernel",
				CoherenceClass:    "A",
				State:             "CURRENT",
				CanonicalMemoryID: "mem:" + scenario.workspaceID,
				CacheKey:          "cache:" + scenario.workspaceID,
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: scenario.docKey, VersionToken: "outdated", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	records, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-a",
		SessionID:   scenario.sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory invalidations: %v", err)
	}
	if len(records) == 0 {
		t.Fatalf("expected memory residency to enqueue an invalidation")
	}
	redeliverMemoryInvalidationForTest(t, store, ctx, scenario.workspaceID, "agent-a", records[0].InvalidationID)
	for i := 0; i < 3; i++ {
		if _, err := store.FailMemoryInvalidations(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     scenario.workspaceID,
			AgentID:         "agent-a",
			InvalidationIDs: []string{records[0].InvalidationID},
			FailureReason:   "dead letter for unified control contract shadow test",
		}); err != nil {
			t.Fatalf("fail memory invalidations %d: %v", i+1, err)
		}
		if i < 2 {
			redeliverMemoryInvalidationForTest(t, store, ctx, scenario.workspaceID, "agent-a", records[0].InvalidationID)
		}
	}

	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	for i := 0; i < 2; i++ {
		if _, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
			WorkspaceID:    scenario.workspaceID,
			ProtoClusterID: clusterID,
			ActorID:        "tests",
		}); err != nil {
			t.Fatalf("tick cluster control state %d: %v", i+1, err)
		}
	}

	report, err := store.BuildUnifiedControlReport(ctx, UnifiedControlReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build unified control report: %v", err)
	}
	if report.CapabilityFlags.GovernedHintsLive {
		t.Fatalf("expected governed hints to stay shadowed by default, got %+v", report.CapabilityFlags)
	}
	if len(report.GovernedHints) != 0 {
		t.Fatalf("expected no governed hints without capability flag, got %+v", report.GovernedHints)
	}
	if len(report.GovernedHintOutcomes) != 0 {
		t.Fatalf("expected no governed hint outcomes without capability flag, got %+v", report.GovernedHintOutcomes)
	}
	if report.GovernedHintSummary != nil {
		t.Fatalf("expected no governed-hint summary without capability flag, got %+v", report.GovernedHintSummary)
	}
	if report.MemoryCoherenceBand != "CRITICAL" || !report.MemoryNeedsAttention {
		t.Fatalf("expected critical memory coherence to still surface through the report, got %+v", report)
	}
	if !equalStringSlices(report.ControlOrder, []string{
		"event_time_ingest",
		"rsp_epoch_update",
		"rrp_coordination_update",
		"rmp_memory_update",
		"arbitration_and_saturation",
	}) {
		t.Fatalf("unexpected control order in shadowed report: %+v", report.ControlOrder)
	}
	if !equalStringSlices(report.AppliedActions, []string{
		"memory_coherence_floor",
		"prefer_kernel_refresh",
	}) {
		t.Fatalf("expected memory-coherence actions to start the shadowed report, got %+v", report.AppliedActions)
	}
	if countStringSlice(report.AppliedActions, "prefer_kernel_refresh") != 1 {
		t.Fatalf("expected prefer_kernel_refresh to be deduped in the shadow path, got %+v", report.AppliedActions)
	}
	if len(report.SuppressedHints) != 0 {
		t.Fatalf("expected no suppressed hints on the shadow path, got %+v", report.SuppressedHints)
	}
	if report.AuditSummary == nil || report.AuditSummary.AppliedEntryCount != len(report.AppliedActionAudit) || report.AuditSummary.SuppressedEntryCount != 0 {
		t.Fatalf("expected shadowed report to keep audit summary internally consistent, got %+v", report.AuditSummary)
	}
	if report.AuditCoverage == nil || report.AuditCoverage.AppliedEntriesWithSourceKinds != len(report.AppliedActionAudit) || report.AuditCoverage.SuppressedEntriesWithReason != 0 {
		t.Fatalf("expected shadowed report to keep audit coverage internally consistent, got %+v", report.AuditCoverage)
	}
}

func TestArbitrateUnifiedControlSurfacesStructuredSuppressedHintAudit(t *testing.T) {
	t.Parallel()

	result := arbitrateUnifiedControl(unifiedControlArbitrationInput{
		Controls: ControlSuggestedControls{
			FanoutCap:      5,
			ReviewDepth:    1,
			ContextCap:     8,
			MergeThreshold: 0.6,
			PriorityFocus:  "throughput",
		},
		CurrentMode:         "steady",
		CandidateMode:       "steady",
		CandidateStreak:     0,
		MemoryCoherenceBand: "STABLE",
		CapabilityFlags: RSPCapabilityFlags{
			GovernedHintsLive: true,
		},
		Hints: []RSPGovernedHint{
			{
				HintID:             "hint-refresh",
				ActuationClass:     "governed_hint",
				TTLEpochs:          2,
				RecommendedActions: []string{"prefer_kernel_refresh"},
			},
		},
	})

	if !equalStringSlices(result.Suppressed, []string{"hint-refresh:prefer_kernel_refresh_without_memory_pressure"}) {
		t.Fatalf("expected legacy suppressed hint string to stay stable, got %+v", result.Suppressed)
	}
	if len(result.SuppressedHintAudit) != 1 {
		t.Fatalf("expected one structured suppressed audit entry, got %+v", result.SuppressedHintAudit)
	}
	entry := result.SuppressedHintAudit[0]
	if entry.HintID != "hint-refresh" || entry.SourceKind != "governed_hint" || entry.Action != "prefer_kernel_refresh" || entry.Reason != "requires_memory_pressure" {
		t.Fatalf("unexpected structured suppressed audit entry %+v", entry)
	}
	if entry.Summary == "" {
		t.Fatalf("expected structured suppressed audit summary, got %+v", entry)
	}
}

func countAppliedActionAudit(items []UnifiedControlAppliedActionAudit, action string) int {
	total := 0
	for _, item := range items {
		if item.Action == action {
			total++
		}
	}
	return total
}

func findAppliedActionAudit(items []UnifiedControlAppliedActionAudit, action string) *UnifiedControlAppliedActionAudit {
	for i := range items {
		if items[i].Action == action {
			return &items[i]
		}
	}
	return nil
}

func findEffectiveControlBasis(items []UnifiedControlEffectiveControlBasis, field string) *UnifiedControlEffectiveControlBasis {
	for i := range items {
		if items[i].Field == field {
			return &items[i]
		}
	}
	return nil
}

func countStringSlice(values []string, target string) int {
	count := 0
	for _, value := range values {
		if value == target {
			count++
		}
	}
	return count
}
