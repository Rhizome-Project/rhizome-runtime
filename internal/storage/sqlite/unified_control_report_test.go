package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func seedBlockedTensionScenarioWithAuthority(t *testing.T, ctx context.Context, store *Store, suffix string) blockedTensionScenario {
	t.Helper()

	scenario := seedBlockedTensionScenario(t, ctx, store, suffix)
	claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	return scenario
}

func TestBuildUnifiedControlReportAppliesMemoryFloorBeforeHints(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenarioWithAuthority(t, ctx, store, "unified-control-report")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for unified report test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
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
	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-unified-control",
		LookupCount:        4,
		L1HitCount:         1,
		L2HitCount:         1,
		StaleHitCount:      2,
		PromotionCount:     1,
		FlushCount:         1,
		FlushPositiveCount: 1,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:           "rtev-unified-control-governed-lineage",
		WorkspaceID:       scenario.workspaceID,
		EventType:         "tests.unified_control.governed_hint_lineage",
		EntityType:        "test_scope",
		EntityID:          clusterID,
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-unified-control-governed",
		ProvenanceGroupID: "PG-unified-control-governed",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record governed hint lineage event: %v", err)
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
	if !report.Resolved || report.ProtoClusterID == "" {
		t.Fatalf("expected resolved unified control report, got %+v", report)
	}
	if len(report.ControlOrder) != 5 || report.ControlOrder[0] != "event_time_ingest" || report.ControlOrder[4] != "arbitration_and_saturation" {
		t.Fatalf("unexpected control order %+v", report.ControlOrder)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected unified control report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected unified control report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if !report.CapabilityFlags.GovernedHintsLive {
		t.Fatalf("expected governed hint capability flag to be visible, got %+v", report.CapabilityFlags)
	}
	if len(report.GovernedHints) == 0 {
		t.Fatalf("expected governed hints to feed unified arbitration, got %+v", report)
	}
	var sawHintInspectability bool
	for _, hint := range report.GovernedHints {
		if hint.RecommendationClass != "" && hint.EvidenceDiversity > 0 && hint.EvidenceDiversityBand != "" && hint.EvidenceSourceMix != "" && hint.RuntimeLineageBasis != "" && hint.TTLWindowState != "" && len(hint.EvidenceSourceKinds) > 0 && hint.Summary != "" {
			sawHintInspectability = true
			break
		}
	}
	if !sawHintInspectability {
		t.Fatalf("expected governed hints to expose recommendation class, evidence diversity, source kinds, lineage basis, ttl window state, and summary, got %+v", report.GovernedHints)
	}
	if len(report.GovernedHintOutcomes) == 0 {
		t.Fatalf("expected governed hint outcomes to be joined on unified report, got %+v", report)
	}
	if report.GovernedHintSummary == nil || report.GovernedHintSummary.TotalHints != len(report.GovernedHints) || report.GovernedHintSummary.OutcomeCount["ADVISORY_ROUTED"] == 0 {
		t.Fatalf("expected unified report to surface governed-hint summary rollup, got %+v", report.GovernedHintSummary)
	}
	var sawRoutedOutcome bool
	for _, outcome := range report.GovernedHintOutcomes {
		if outcome.ArbitrationOutcome == "ADVISORY_ROUTED" && len(outcome.AppliedActions) > 0 && outcome.Summary != "" {
			sawRoutedOutcome = true
			break
		}
	}
	if !sawRoutedOutcome {
		t.Fatalf("expected unified report to surface routed governed-hint outcome, got %+v", report.GovernedHintOutcomes)
	}
	if report.MemoryCoherenceBand == "" || !report.MemoryNeedsAttention {
		t.Fatalf("expected unified report to surface memory coherence pressure, got %+v", report)
	}
	if report.AdvisoryControls != report.SuggestedControls {
		t.Fatalf("expected advisory controls to mirror backward-compatible suggested controls, advisory=%+v suggested=%+v", report.AdvisoryControls, report.SuggestedControls)
	}
	if report.CandidateControls != report.EffectiveControls {
		t.Fatalf("expected candidate/effective controls to align while no persisted effective controls exist, candidate=%+v effective=%+v", report.CandidateControls, report.EffectiveControls)
	}
	if !report.AdvisoryOnly {
		t.Fatalf("expected unified report without persisted controls to remain advisory-only, got %+v", report)
	}
	if report.EffectiveControlsAudit == nil || report.EffectiveControlsAudit.Found || report.EffectiveControlsAudit.ScopeSource != "candidate_only" {
		t.Fatalf("expected unified report without persisted controls to expose candidate-only effective-controls audit, got %+v", report.EffectiveControlsAudit)
	}
	if report.EffectiveControlsAudit.TemporalContract != nil {
		t.Fatalf("expected candidate-only effective-controls audit not to fabricate a ttl temporal contract, got %+v", report.EffectiveControlsAudit.TemporalContract)
	}
	if report.EffectiveControls.ContextCap == 0 || report.EffectiveControls.ContextCap > report.SuggestedControls.ContextCap {
		t.Fatalf("expected memory floor to clamp effective context cap, suggested=%+v effective=%+v", report.SuggestedControls, report.EffectiveControls)
	}
	if len(report.AppliedActions) == 0 {
		t.Fatalf("expected applied arbitration actions, got %+v", report)
	}
	if len(report.AppliedActionAudit) == 0 {
		t.Fatalf("expected structured applied action audit, got %+v", report)
	}
	if report.AuditSummary == nil {
		t.Fatalf("expected unified report to surface audit summary, got %+v", report)
	}
	if report.AuditCoverage == nil {
		t.Fatalf("expected unified report to surface audit coverage, got %+v", report)
	}
	var appliedAuditActions []string
	var sawMemoryFloor bool
	var sawHintBackedAction bool
	var sawDeltaFields bool
	for _, entry := range report.AppliedActionAudit {
		appliedAuditActions = append(appliedAuditActions, entry.Action)
		if entry.Action == "memory_coherence_floor" && containsString(entry.SourceKinds, "memory_coherence_floor") {
			sawMemoryFloor = true
		}
		if len(entry.HintIDs) > 0 {
			sawHintBackedAction = true
		}
		if len(entry.DeltaFields) > 0 {
			sawDeltaFields = true
		}
		if entry.Summary == "" {
			t.Fatalf("expected structured applied action summary for %+v", entry)
		}
	}
	if !reflect.DeepEqual(appliedAuditActions, report.AppliedActions) {
		t.Fatalf("expected structured applied audit to mirror legacy applied actions order, audit=%+v legacy=%+v", appliedAuditActions, report.AppliedActions)
	}
	if report.AuditSummary.AppliedEntryCount != len(report.AppliedActionAudit) {
		t.Fatalf("expected applied audit summary count to match structured audit entries, summary=%+v audit=%+v", report.AuditSummary, report.AppliedActionAudit)
	}
	if report.AuditSummary.HintBackedActionCount == 0 || report.AuditSummary.DeltaBearingActionCount == 0 {
		t.Fatalf("expected audit summary to surface hint-backed and delta-bearing counts, got %+v", report.AuditSummary)
	}
	if report.AuditSummary.AppliedSourceKindCount["memory_coherence_floor"] == 0 || report.AuditSummary.AppliedSourceKindCount["governed_hint"] == 0 {
		t.Fatalf("expected audit summary to surface applied source-kind counts, got %+v", report.AuditSummary)
	}
	if report.AuditSummary.SuppressedEntryCount != len(report.SuppressedHintAudit) || report.AuditSummary.SuppressedEntriesWithActionRef != 0 {
		t.Fatalf("expected audit summary to keep empty suppressed trace counts aligned, got %+v", report.AuditSummary)
	}
	if report.AuditCoverage.AppliedEntriesWithSourceKinds != len(report.AppliedActionAudit) || report.AuditCoverage.AppliedEntriesWithSummary != len(report.AppliedActionAudit) {
		t.Fatalf("expected audit coverage to count full applied trace-field presence, got %+v", report.AuditCoverage)
	}
	if report.AuditCoverage.AppliedEntriesWithHintRefs == 0 || report.AuditCoverage.AppliedEntriesWithDeltaFields == 0 || report.AuditCoverage.FullAppliedTraceEntryCount == 0 {
		t.Fatalf("expected audit coverage to surface applied hint/delta/full-trace counts, got %+v", report.AuditCoverage)
	}
	if report.AuditCoverage.SuppressedEntriesWithSourceKind != 0 || report.AuditCoverage.SuppressedEntriesWithActionRef != 0 || report.AuditCoverage.FullSuppressedTraceEntryCount != 0 {
		t.Fatalf("expected audit coverage to keep empty suppressed trace-field counts aligned, got %+v", report.AuditCoverage)
	}
	if len(report.EffectiveControlBasis) != 6 {
		t.Fatalf("expected unified report to surface per-control basis for the current effective controls, got %+v", report.EffectiveControlBasis)
	}
	if report.EffectiveControlBasisSummary == nil || report.EffectiveControlBasisSummary.FieldCount != len(report.EffectiveControlBasis) || report.EffectiveControlBasisSummary.ChangedFieldCount == 0 || report.EffectiveControlBasisSummary.FieldsWithActionTraceCount == 0 {
		t.Fatalf("expected unified report to surface effective-control basis summary aligned with per-field basis, got %+v", report.EffectiveControlBasisSummary)
	}
	if len(report.Contradictions) == 0 {
		if report.ContradictionSummary != nil {
			t.Fatalf("expected no contradiction summary when contradictions are absent, got %+v", report.ContradictionSummary)
		}
	} else if report.ContradictionSummary == nil || report.ContradictionSummary.TotalCount != len(report.Contradictions) {
		t.Fatalf("expected unified report to surface contradiction summary aligned with contradictions, got %+v contradictions=%+v", report.ContradictionSummary, report.Contradictions)
	}
	if report.CooldownBasis == nil {
		t.Fatalf("expected unified report to surface cooldown basis, got %+v", report)
	}
	if report.CooldownBasis.CooldownActive != report.CooldownActive || report.CooldownBasis.CurrentMode != report.ControlMode || report.CooldownBasis.CandidateMode != report.CandidateMode {
		t.Fatalf("expected cooldown basis to stay aligned with report control state, got %+v report=%+v", report.CooldownBasis, report)
	}
	if report.CooldownBasis.Stage == "" || report.CooldownBasis.CandidateStreak < 0 || report.CooldownBasis.Reason == "" || report.CooldownBasis.Summary == "" {
		t.Fatalf("expected cooldown basis to expose bounded inspectability fields, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.AcceptanceReadiness == "" {
		t.Fatalf("expected cooldown basis to expose bounded acceptance readiness, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.AcceptanceGateReason == "" {
		t.Fatalf("expected cooldown basis to expose bounded acceptance gate reason, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.AcceptanceChecklist == nil {
		t.Fatalf("expected cooldown basis to expose bounded acceptance checklist, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.AcceptanceChecklist.CandidatePresent != (report.CooldownBasis.CandidateMode != "") {
		t.Fatalf("expected acceptance checklist candidate_present to align with candidate mode, got %+v", report.CooldownBasis.AcceptanceChecklist)
	}
	if report.CooldownBasis.AcceptanceChecklist.CandidateDiverges != (report.CooldownBasis.CandidateMode != "" && report.CooldownBasis.CandidateMode != report.CooldownBasis.CurrentMode) {
		t.Fatalf("expected acceptance checklist candidate_diverges to align with cooldown basis, got %+v", report.CooldownBasis.AcceptanceChecklist)
	}
	if report.CooldownBasis.AcceptanceChecklist.HysteresisSatisfied != (report.CooldownBasis.CandidateMode != "" && report.CooldownBasis.CandidateMode != report.CooldownBasis.CurrentMode && report.CooldownBasis.ReadyToStabilize) {
		t.Fatalf("expected acceptance checklist hysteresis_satisfied to align with ready_to_stabilize, got %+v", report.CooldownBasis.AcceptanceChecklist)
	}
	if report.CooldownBasis.AcceptanceChecklist.CooldownClear != !report.CooldownBasis.CooldownActive || report.CooldownBasis.AcceptanceChecklist.ContradictionClear != (len(report.Contradictions) == 0) || report.CooldownBasis.AcceptanceChecklist.MemoryAttentionClear != !report.MemoryNeedsAttention {
		t.Fatalf("expected acceptance checklist to align with current cooldown/contradiction/memory context, got checklist=%+v report=%+v", report.CooldownBasis.AcceptanceChecklist, report)
	}
	if expectedMissing := buildUnifiedControlAcceptanceMissingRequirements(report.CooldownBasis.AcceptanceReadiness, report.CooldownBasis.AcceptanceChecklist); !reflect.DeepEqual(report.CooldownBasis.AcceptanceMissingRequirements, expectedMissing) {
		t.Fatalf("expected acceptance missing requirements to align with acceptance checklist, got actual=%+v expected=%+v", report.CooldownBasis.AcceptanceMissingRequirements, expectedMissing)
	}
	if expectedBand := buildUnifiedControlAcceptanceProgressBand(report.CooldownBasis.AcceptanceReadiness, report.CooldownBasis.AcceptanceChecklist, report.CooldownBasis.AcceptanceMissingRequirements); report.CooldownBasis.AcceptanceProgressBand != expectedBand {
		t.Fatalf("expected acceptance progress band to align with acceptance context, got actual=%q expected=%q", report.CooldownBasis.AcceptanceProgressBand, expectedBand)
	}
	if report.CooldownBasis.RequiredStreak <= 0 || report.CooldownBasis.RemainingStreak < 0 {
		t.Fatalf("expected cooldown basis to expose bounded transition window fields, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.ReadyToStabilize && report.CooldownBasis.RemainingStreak != 0 {
		t.Fatalf("expected ready cooldown basis to have no remaining streak, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.BlockingReasonCount != len(report.CooldownBasis.BlockingReasons) || report.CooldownBasis.BlockingReasonCount < 0 {
		t.Fatalf("expected cooldown basis blocking count to stay aligned with blocking reasons, got %+v", report.CooldownBasis)
	}
	mergeThresholdBasis := findEffectiveControlBasis(report.EffectiveControlBasis, "merge_threshold")
	if mergeThresholdBasis == nil {
		t.Fatalf("expected merge_threshold effective-control basis entry, got %+v", report.EffectiveControlBasis)
	}
	if !mergeThresholdBasis.Changed || !containsString(mergeThresholdBasis.AppliedActions, "memory_coherence_floor") || !containsString(mergeThresholdBasis.SourceKinds, "memory_coherence_floor") || mergeThresholdBasis.Summary == "" {
		t.Fatalf("expected merge_threshold basis to retain current delta-bearing action provenance, got %+v", mergeThresholdBasis)
	}
	if !strings.Contains(mergeThresholdBasis.Summary, "merge threshold=") {
		t.Fatalf("expected merge_threshold basis summary to humanize field wording, got %q", mergeThresholdBasis.Summary)
	}
	if strings.Contains(mergeThresholdBasis.Summary, "merge_threshold=") {
		t.Fatalf("expected merge_threshold basis summary to avoid raw field key wording, got %q", mergeThresholdBasis.Summary)
	}
	if !sawMemoryFloor {
		t.Fatalf("expected structured audit to expose memory coherence floor provenance, got %+v", report.AppliedActionAudit)
	}
	if !sawHintBackedAction {
		t.Fatalf("expected structured audit to expose hint-backed action provenance, got %+v", report.AppliedActionAudit)
	}
	if !sawDeltaFields {
		t.Fatalf("expected structured audit to expose effective parameter delta fields, got %+v", report.AppliedActionAudit)
	}
}

func TestBuildUnifiedControlReportSurfacesPersistedEffectiveControlsSeparately(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenarioWithAuthority(t, ctx, store, "unified-control-report-persisted")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for persisted effective report test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
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
	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-unified-control-persisted",
		LookupCount:        4,
		L1HitCount:         1,
		L2HitCount:         1,
		StaleHitCount:      2,
		PromotionCount:     1,
		FlushCount:         1,
		FlushPositiveCount: 1,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}

	filter := UnifiedControlReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}
	first, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build baseline unified control report: %v", err)
	}
	if !first.Resolved {
		t.Fatalf("expected baseline report to resolve, got %+v", first)
	}

	persistedControls := ControlSuggestedControls{
		FanoutCap:      maxInt(first.CandidateControls.FanoutCap-1, 1),
		ReviewDepth:    first.CandidateControls.ReviewDepth + 1,
		ContextCap:     maxInt(first.CandidateControls.ContextCap-1, 1),
		BridgeQuota:    maxInt(first.CandidateControls.BridgeQuota-1, 0),
		MergeThreshold: first.CandidateControls.MergeThreshold + 1,
		PriorityFocus:  "persisted-effective",
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    first.ProtoClusterID,
		Epoch:             3,
		TTLSeconds:        600,
		ControlMode:       first.ControlMode,
		CandidateMode:     first.CandidateMode,
		CandidateControls: first.CandidateControls,
		AdvisoryControls:  first.AdvisoryControls,
		EffectiveControls: persistedControls,
		ResolvedFrom:      "persisted_test",
		MatchScore:        first.MatchScore,
		BasisSummary:      "persisted effective controls for unified report coverage",
		GeneratedAt:       first.GeneratedAt,
		ActorID:           "tests",
	}); err != nil {
		t.Fatalf("persist effective controls: %v", err)
	}

	second, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build persisted unified control report: %v", err)
	}
	if second.AdvisoryOnly {
		t.Fatalf("expected persisted effective controls to clear advisory-only posture, got %+v", second)
	}
	if second.AdvisoryControls != second.SuggestedControls {
		t.Fatalf("expected advisory controls to remain aligned with suggested controls, advisory=%+v suggested=%+v", second.AdvisoryControls, second.SuggestedControls)
	}
	if second.CandidateControls != first.CandidateControls {
		t.Fatalf("expected candidate controls to keep reflecting current arbitration output, got current=%+v baseline=%+v", second.CandidateControls, first.CandidateControls)
	}
	if second.EffectiveControls != persistedControls {
		t.Fatalf("expected effective controls to surface persisted state, got %+v want %+v", second.EffectiveControls, persistedControls)
	}
	if second.EffectiveControlsAudit == nil || !second.EffectiveControlsAudit.Found || !second.EffectiveControlsAudit.Live || second.EffectiveControlsAudit.ScopeSource != "proto_cluster" {
		t.Fatalf("expected persisted effective-controls audit to be surfaced, got %+v", second.EffectiveControlsAudit)
	}
	assertUnifiedEffectiveControlsTemporalContract(t, second.EffectiveControlsAudit.TemporalContract, "LIVE", "proto_cluster")
	if second.EffectiveControlsAudit.Pending || second.EffectiveControlsAudit.Expired {
		t.Fatalf("expected persisted effective-controls audit not to be pending/expired once live, got %+v", second.EffectiveControlsAudit)
	}
	if second.EffectiveControlsAudit.Epoch != 3 || second.EffectiveControlsAudit.TTLSeconds != 600 || second.EffectiveControlsAudit.ActorID != "tests" {
		t.Fatalf("expected persisted effective-controls audit metadata to round-trip, got %+v", second.EffectiveControlsAudit)
	}
	if second.CandidateControls == second.EffectiveControls {
		t.Fatalf("expected persisted effective controls to diverge from candidate controls for this test, got %+v", second)
	}
}

func TestBuildUnifiedControlReportSurfacesExpiredEffectiveControlsAudit(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenarioWithAuthority(t, ctx, store, "unified-control-report-expired")

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

	filter := UnifiedControlReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}
	baseline, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build baseline unified control report: %v", err)
	}

	expiredControls := ControlSuggestedControls{
		FanoutCap:      1,
		ReviewDepth:    1,
		ContextCap:     2,
		BridgeQuota:    0,
		MergeThreshold: 1,
		PriorityFocus:  "expired-effective",
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    baseline.ProtoClusterID,
		Epoch:             1,
		TTLSeconds:        1,
		ControlMode:       baseline.ControlMode,
		CandidateMode:     baseline.CandidateMode,
		CandidateControls: baseline.CandidateControls,
		AdvisoryControls:  baseline.AdvisoryControls,
		EffectiveControls: expiredControls,
		ResolvedFrom:      "expired_test",
		MatchScore:        baseline.MatchScore,
		BasisSummary:      "expired effective controls for audit coverage",
		GeneratedAt:       "2026-04-08T00:00:00Z",
		ActorID:           "tests",
	}); err != nil {
		t.Fatalf("persist expired effective controls: %v", err)
	}

	report, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build report with expired effective controls: %v", err)
	}
	if !report.AdvisoryOnly {
		t.Fatalf("expected expired effective controls not to clear advisory-only posture, got %+v", report)
	}
	if report.EffectiveControls != report.CandidateControls {
		t.Fatalf("expected expired effective controls not to override candidate controls, got candidate=%+v effective=%+v", report.CandidateControls, report.EffectiveControls)
	}
	if report.EffectiveControlsAudit == nil || !report.EffectiveControlsAudit.Found || report.EffectiveControlsAudit.Live || !report.EffectiveControlsAudit.Expired {
		t.Fatalf("expected expired effective-controls audit to remain visible, got %+v", report.EffectiveControlsAudit)
	}
	assertUnifiedEffectiveControlsTemporalContract(t, report.EffectiveControlsAudit.TemporalContract, "EXPIRED", "proto_cluster")
	if report.EffectiveControlsAudit.Pending {
		t.Fatalf("expected expired effective-controls audit not to stay pending, got %+v", report.EffectiveControlsAudit)
	}
	if report.EffectiveControlsAudit.ScopeSource != "proto_cluster" || report.EffectiveControlsAudit.ActorID != "tests" {
		t.Fatalf("expected expired effective-controls audit to preserve scope and actor, got %+v", report.EffectiveControlsAudit)
	}
}

func TestBuildUnifiedControlReportKeepsFutureGeneratedEffectiveControlsPending(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenarioWithAuthority(t, ctx, store, "unified-control-report-pending")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for pending effective-controls coverage",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
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
	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-unified-control-pending",
		LookupCount:        4,
		L1HitCount:         1,
		L2HitCount:         1,
		StaleHitCount:      2,
		PromotionCount:     1,
		FlushCount:         1,
		FlushPositiveCount: 1,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}

	filter := UnifiedControlReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}
	baseline, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build baseline unified control report: %v", err)
	}
	if reflect.DeepEqual(baseline.AdvisoryControls, baseline.CandidateControls) {
		t.Fatalf("expected baseline arbitration to diverge advisory and candidate controls for pending mismatch coverage, got advisory=%+v candidate=%+v", baseline.AdvisoryControls, baseline.CandidateControls)
	}

	pendingControls := ControlSuggestedControls{
		FanoutCap:      1,
		ReviewDepth:    baseline.CandidateControls.ReviewDepth + 1,
		ContextCap:     maxInt(baseline.CandidateControls.ContextCap-1, 1),
		BridgeQuota:    baseline.CandidateControls.BridgeQuota,
		MergeThreshold: baseline.CandidateControls.MergeThreshold + 1,
		PriorityFocus:  "pending-effective",
	}
	if reflect.DeepEqual(pendingControls, baseline.CandidateControls) {
		t.Fatalf("expected pending effective controls fixture to diverge from current candidate controls, got baseline=%+v pending=%+v", baseline.CandidateControls, pendingControls)
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    baseline.ProtoClusterID,
		Epoch:             2,
		TTLSeconds:        600,
		ControlMode:       baseline.ControlMode,
		CandidateMode:     baseline.CandidateMode,
		CandidateControls: baseline.CandidateControls,
		AdvisoryControls:  baseline.AdvisoryControls,
		EffectiveControls: pendingControls,
		ResolvedFrom:      "pending_test",
		MatchScore:        baseline.MatchScore,
		BasisSummary:      "future generated effective controls should stay pending",
		GeneratedAt:       "2099-01-01T00:00:00Z",
		ActorID:           "tests",
	}); err != nil {
		t.Fatalf("persist pending effective controls: %v", err)
	}

	beforeEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       200,
	})
	if err != nil {
		t.Fatalf("list runtime events before pending report read: %v", err)
	}
	beforeControlRequests := countRuntimeEventsByType(beforeEvents, controlCommandEventType)

	report, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build pending unified control report: %v", err)
	}
	if !report.AdvisoryOnly {
		t.Fatalf("expected future-generated effective controls to keep advisory-only posture, got %+v", report)
	}
	if report.EffectiveControls != report.CandidateControls {
		t.Fatalf("expected future-generated effective controls not to override candidate controls, got candidate=%+v effective=%+v", report.CandidateControls, report.EffectiveControls)
	}
	if reflect.DeepEqual(report.AdvisoryControls, report.EffectiveControls) {
		t.Fatalf("expected pending effective-controls report to keep advisory and effective surfaces distinct, got %+v", report)
	}
	if reflect.DeepEqual(report.EffectiveControls, pendingControls) {
		t.Fatalf("expected pending effective controls to remain non-live read-side evidence, got %+v", report)
	}
	if report.EffectiveControlsAudit == nil || !report.EffectiveControlsAudit.Found || report.EffectiveControlsAudit.Live || report.EffectiveControlsAudit.Expired || !report.EffectiveControlsAudit.Pending {
		t.Fatalf("expected future-generated effective-controls audit to remain pending, got %+v", report.EffectiveControlsAudit)
	}
	assertUnifiedEffectiveControlsTemporalContract(t, report.EffectiveControlsAudit.TemporalContract, "PENDING", "proto_cluster")
	if !strings.Contains(report.Summary, "advisory unified control report") || strings.Contains(report.Summary, "effective unified control report") {
		t.Fatalf("expected pending effective controls to keep advisory report wording, got %q", report.Summary)
	}

	afterEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       200,
	})
	if err != nil {
		t.Fatalf("list runtime events after pending report read: %v", err)
	}
	if len(afterEvents) != len(beforeEvents) {
		t.Fatalf("expected pending report read not to append runtime events, before=%d after=%d", len(beforeEvents), len(afterEvents))
	}
	if afterControlRequests := countRuntimeEventsByType(afterEvents, controlCommandEventType); afterControlRequests != beforeControlRequests {
		t.Fatalf("expected pending report read not to append control.command.requested events, before=%d after=%d", beforeControlRequests, afterControlRequests)
	}
}

func TestBuildUnifiedControlReportHandlesUnresolvedLocus(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-unified-control-unresolved"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Unified Control Unresolved",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	report, err := store.BuildUnifiedControlReport(ctx, UnifiedControlReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     "agent-missing",
	})
	if err != nil {
		t.Fatalf("build unresolved unified control report: %v", err)
	}
	if report.Resolved {
		t.Fatalf("expected unresolved report, got %+v", report)
	}
	if len(report.ControlOrder) != 5 {
		t.Fatalf("expected fixed control order even when unresolved, got %+v", report.ControlOrder)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected unresolved report to keep workspace time authority visible, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected unresolved unified control report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsReadyPendingWhileCooldownActive(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, clusterControlTickHysteresisEpoch, true, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.Stage != "READY_WINDOW" || !basis.ReadyToStabilize {
		t.Fatalf("expected ready-window cooldown basis, got %+v", basis)
	}
	if basis.AcceptanceReadiness != "READY_PENDING" {
		t.Fatalf("expected cooldown-active ready window to stay READY_PENDING, got %+v", basis)
	}
	if basis.AcceptanceGateReason != "COOLDOWN_ACTIVE" {
		t.Fatalf("expected cooldown-active ready window to keep cooldown gate reason, got %+v", basis)
	}
	if basis.Reason != "ready_window_pending_cooldown" {
		t.Fatalf("expected cooldown-active ready window to keep ready-window-aware reason, got %+v", basis)
	}
	if basis.AcceptanceProgressBand != "READY_WINDOW_PENDING" {
		t.Fatalf("expected cooldown-active ready window to keep ready-window-specific pending progress, got %+v", basis)
	}
	if basis.AcceptanceChecklist == nil || basis.AcceptanceChecklist.CooldownClear {
		t.Fatalf("expected cooldown-active ready window to keep cooldown_clear false, got %+v", basis.AcceptanceChecklist)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected cooldown-active ready window clear count to stay active-debt-scoped at 0, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 1 {
		t.Fatalf("expected cooldown-active ready window requirement count to stay active-debt-scoped at 1, got %d", got)
	}
	if !containsString(basis.AcceptanceMissingRequirements, "cooldown_clear") {
		t.Fatalf("expected cooldown-active ready window to keep cooldown_clear in missing requirements, got %+v", basis.AcceptanceMissingRequirements)
	}
	if !containsString(basis.BlockingReasons, "cooldown_active") {
		t.Fatalf("expected cooldown-active ready window to keep cooldown_active blocking reason, got %+v", basis.BlockingReasons)
	}
	if !strings.Contains(basis.Summary, "acceptance progress ready window pending") {
		t.Fatalf("expected cooldown-active ready window summary to reflect ready-window-specific pending progress, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance progress READY_WINDOW_PENDING") {
		t.Fatalf("expected cooldown-active ready window summary to avoid raw progress enum wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "reason ready window pending cooldown") {
		t.Fatalf("expected cooldown-active ready window summary to use ready-window-specific reason wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "reason ready_window_pending_cooldown") {
		t.Fatalf("expected cooldown-active ready window summary to avoid raw ready_window_pending_cooldown reason wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance gate cooldown active") {
		t.Fatalf("expected cooldown-active ready window summary to humanize acceptance gate wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance gate COOLDOWN_ACTIVE") {
		t.Fatalf("expected cooldown-active ready window summary to avoid raw COOLDOWN_ACTIVE gate wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance progress READY_WINDOW_PENDING") {
		t.Fatalf("expected cooldown-active ready window summary to avoid raw READY_WINDOW_PENDING progress wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "stage ready window") || !strings.Contains(basis.Summary, "acceptance readiness ready pending") {
		t.Fatalf("expected cooldown-active ready window summary to humanize stage/readiness wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "stage READY_WINDOW") || strings.Contains(basis.Summary, "acceptance readiness READY_PENDING") {
		t.Fatalf("expected cooldown-active ready window summary to avoid raw stage/readiness enums, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "current stabilize") || !strings.Contains(basis.Summary, "candidate synergy seeking") {
		t.Fatalf("expected cooldown-active ready window summary to humanize current/candidate mode wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "current STABILIZE") || strings.Contains(basis.Summary, "candidate SYNERGY_SEEKING") {
		t.Fatalf("expected cooldown-active ready window summary to avoid raw current/candidate mode enums, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "ready-window checklist 0/1") {
		t.Fatalf("expected cooldown-active ready window summary to keep active-debt-scoped checklist counts, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "blocking cooldown active") {
		t.Fatalf("expected cooldown-active ready window summary to humanize blocking reasons, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "missing requirements ready window cooldown clear") {
		t.Fatalf("expected cooldown-active ready window summary to humanize ready-window missing requirements, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "blocking cooldown_active") || strings.Contains(basis.Summary, "missing requirements cooldown_clear") {
		t.Fatalf("expected cooldown-active ready window summary to avoid raw blocking/missing labels, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsReadyPendingWhenReadyWindowClears(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, clusterControlTickHysteresisEpoch, false, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.Stage != "READY_WINDOW" || !basis.ReadyToStabilize {
		t.Fatalf("expected ready-window cooldown basis, got %+v", basis)
	}
	if basis.AcceptanceReadiness != "READY_PENDING" {
		t.Fatalf("expected clear ready window to stay READY_PENDING, got %+v", basis)
	}
	if basis.AcceptanceGateReason != "READY_WINDOW_OPEN" {
		t.Fatalf("expected clear ready window to keep ready-window gate reason, got %+v", basis)
	}
	if basis.Reason != "ready_window_open" {
		t.Fatalf("expected clear ready window to keep ready-window-aware reason, got %+v", basis)
	}
	if basis.AcceptanceProgressBand != "FULLY_CLEAR" {
		t.Fatalf("expected clear ready window to keep fully-clear progress band, got %+v", basis)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 1 {
		t.Fatalf("expected clear ready window clear count to stay active-debt-scoped at 1, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 1 {
		t.Fatalf("expected clear ready window requirement count to stay active-debt-scoped at 1, got %d", got)
	}
	if len(basis.AcceptanceMissingRequirements) != 0 {
		t.Fatalf("expected clear ready window to have no missing requirements, got %+v", basis.AcceptanceMissingRequirements)
	}
	if !strings.Contains(basis.Summary, "ready-window checklist 1/1") {
		t.Fatalf("expected clear ready window summary to keep active-debt-scoped checklist counts, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance progress ready window clear") {
		t.Fatalf("expected clear ready window summary to use ready-window-clear progress wording instead of raw FULLY_CLEAR, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance progress FULLY_CLEAR") {
		t.Fatalf("expected clear ready window summary to avoid raw FULLY_CLEAR progress wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "reason ready window open") {
		t.Fatalf("expected clear ready window summary to use ready-window-open reason wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "reason ready_window_open") {
		t.Fatalf("expected clear ready window summary to avoid raw ready_window_open reason wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance gate ready window open") {
		t.Fatalf("expected clear ready window summary to humanize acceptance gate wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance gate READY_WINDOW_OPEN") {
		t.Fatalf("expected clear ready window summary to avoid raw READY_WINDOW_OPEN gate wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "ready window clear") {
		t.Fatalf("expected clear ready window summary to use ready-window-clear wording instead of generic no-missing wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "no missing acceptance requirements") {
		t.Fatalf("expected clear ready window summary to avoid generic no-missing wording on a ready-window-clear path, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "current stabilize") || !strings.Contains(basis.Summary, "candidate synergy seeking") {
		t.Fatalf("expected clear ready window summary to humanize current/candidate mode wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "current STABILIZE") || strings.Contains(basis.Summary, "candidate SYNERGY_SEEKING") {
		t.Fatalf("expected clear ready window summary to avoid raw current/candidate mode enums, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsAcceptanceMissingRequirementsEmptyWithoutCandidate(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, "", 0, false, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "UNAVAILABLE" {
		t.Fatalf("expected no-candidate cooldown basis to stay unavailable, got %+v", basis)
	}
	if basis.AcceptanceProgressBand != "NONE" {
		t.Fatalf("expected no-candidate cooldown basis to stay in NONE progress band, got %+v", basis)
	}
	if len(basis.AcceptanceMissingRequirements) != 0 {
		t.Fatalf("expected no-candidate cooldown basis to keep acceptance missing requirements empty, got %+v", basis.AcceptanceMissingRequirements)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected no-candidate cooldown basis to keep acceptance clear count not-applicable, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected no-candidate cooldown basis to keep acceptance requirement count not-applicable, got %d", got)
	}
	if basis.AcceptanceChecklist == nil || basis.AcceptanceChecklist.CandidatePresent {
		t.Fatalf("expected no-candidate cooldown basis to keep checklist visible but candidate_present false, got %+v", basis.AcceptanceChecklist)
	}
	if !strings.Contains(basis.Summary, "acceptance path not active") {
		t.Fatalf("expected no-candidate cooldown basis summary to mark inactive acceptance path, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsAcceptanceMissingRequirementsEmptyWhenAligned(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeStabilize, clusterControlTickHysteresisEpoch, false, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "ALIGNED" {
		t.Fatalf("expected aligned cooldown basis to stay aligned, got %+v", basis)
	}
	if basis.AcceptanceProgressBand != "ALIGNED" {
		t.Fatalf("expected aligned cooldown basis to stay in ALIGNED progress band, got %+v", basis)
	}
	if len(basis.AcceptanceMissingRequirements) != 0 {
		t.Fatalf("expected aligned cooldown basis to keep acceptance missing requirements empty, got %+v", basis.AcceptanceMissingRequirements)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected aligned cooldown basis to keep acceptance clear count not-applicable, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected aligned cooldown basis to keep acceptance requirement count not-applicable, got %d", got)
	}
	if basis.AcceptanceChecklist == nil || !basis.AcceptanceChecklist.CandidatePresent || basis.AcceptanceChecklist.CandidateDiverges {
		t.Fatalf("expected aligned cooldown basis to keep checklist visible without candidate divergence, got %+v", basis.AcceptanceChecklist)
	}
	if !strings.Contains(basis.Summary, "acceptance path not active") {
		t.Fatalf("expected aligned cooldown basis summary to mark inactive acceptance path, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisCapsProgressBeforeHysteresisSatisfied(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, clusterControlTickHysteresisEpoch-1, false, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "WARMING" {
		t.Fatalf("expected pre-hysteresis cooldown basis to stay warming, got %+v", basis)
	}
	if basis.AcceptanceGateReason != "STREAK_BELOW_HYSTERESIS" {
		t.Fatalf("expected pre-hysteresis cooldown basis to keep hysteresis gate reason once streak has started, got %+v", basis)
	}
	if basis.Reason != "hysteresis_pending" {
		t.Fatalf("expected pre-hysteresis cooldown basis to keep hysteresis-aware reason, got %+v", basis)
	}
	if !strings.Contains(basis.Summary, "reason hysteresis pending") {
		t.Fatalf("expected pre-hysteresis cooldown basis summary to humanize hysteresis_pending reason wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "reason hysteresis_pending") {
		t.Fatalf("expected pre-hysteresis cooldown basis summary to avoid raw hysteresis_pending reason wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance gate streak below hysteresis") {
		t.Fatalf("expected pre-hysteresis cooldown basis summary to humanize acceptance gate wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance gate STREAK_BELOW_HYSTERESIS") {
		t.Fatalf("expected pre-hysteresis cooldown basis summary to avoid raw STREAK_BELOW_HYSTERESIS gate wording, got %q", basis.Summary)
	}
	if !containsString(basis.BlockingReasons, "streak_below_hysteresis") {
		t.Fatalf("expected pre-hysteresis cooldown basis to surface streak_below_hysteresis once streak has started, got %+v", basis.BlockingReasons)
	}
	if !containsString(basis.AcceptanceMissingRequirements, "hysteresis_satisfied") {
		t.Fatalf("expected pre-hysteresis cooldown basis to keep hysteresis requirement missing, got %+v", basis.AcceptanceMissingRequirements)
	}
	if basis.AcceptanceProgressBand != "PARTIAL" {
		t.Fatalf("expected pre-hysteresis cooldown basis to stay capped at partial progress, got %+v", basis)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsHysteresisGatePrimaryWhileWarmingUnderCooldown(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, clusterControlTickHysteresisEpoch-1, true, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "WARMING" {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to stay warming, got %+v", basis)
	}
	if basis.AcceptanceGateReason != "STREAK_BELOW_HYSTERESIS" {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to keep hysteresis as primary gate until ready window opens, got %+v", basis)
	}
	if basis.Reason != "hysteresis_pending" {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to keep hysteresis-aware reason, got %+v", basis)
	}
	if !containsString(basis.BlockingReasons, "streak_below_hysteresis") || !containsString(basis.BlockingReasons, "cooldown_active") {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to surface both hysteresis and cooldown blockers, got %+v", basis.BlockingReasons)
	}
	if containsString(basis.AcceptanceMissingRequirements, "cooldown_clear") {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to keep cooldown_clear out of active debt until the ready window opens, got %+v", basis.AcceptanceMissingRequirements)
	}
	if basis.AcceptanceProgressBand != "PARTIAL" {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to stay capped at partial progress, got %+v", basis)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 4 {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to keep acceptance clear count focused on current pre-ready debt, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 5 {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown to keep acceptance requirement count focused on current pre-ready debt, got %d", got)
	}
	if !strings.Contains(basis.Summary, "acceptance checklist 4/5") {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown summary to reflect warming-focused acceptance checklist counts, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "blocking cooldown active, streak below hysteresis") {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown summary to humanize blocking reasons, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "missing requirements hysteresis satisfied") {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown summary to humanize missing requirements, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "blocking cooldown_active, streak_below_hysteresis") || strings.Contains(basis.Summary, "missing requirements hysteresis_satisfied") {
		t.Fatalf("expected pre-hysteresis cooldown basis under cooldown summary to avoid raw blocking/missing labels, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsBlockedMissingRequirementsFocusedOnHardBlockers(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, clusterControlTickHysteresisEpoch-1, true, []string{"hard_safety_conditions_clamped_rsp_advice"}, true)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "BLOCKED" {
		t.Fatalf("expected contradiction/memory-blocked cooldown basis to stay blocked, got %+v", basis)
	}
	if basis.AcceptanceGateReason != "CONTRADICTIONS_AND_MEMORY_ATTENTION" {
		t.Fatalf("expected dual-blocked cooldown basis to keep composite gate reason, got %+v", basis)
	}
	if basis.Reason != "contradictions_and_memory_attention" {
		t.Fatalf("expected dual-blocked cooldown basis to keep composite blocker-aware reason, got %+v", basis)
	}
	if !reflect.DeepEqual(basis.AcceptanceMissingRequirements, []string{"contradiction_clear", "memory_attention_clear"}) {
		t.Fatalf("expected blocked cooldown basis to keep missing requirements focused on active hard blockers, got %+v", basis.AcceptanceMissingRequirements)
	}
	if !strings.Contains(basis.Summary, "reason contradictions and memory attention") {
		t.Fatalf("expected blocked cooldown basis summary to humanize composite contradiction/memory reason wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "reason contradictions_and_memory_attention") {
		t.Fatalf("expected blocked cooldown basis summary to avoid raw contradictions_and_memory_attention reason wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance gate contradictions and memory attention") {
		t.Fatalf("expected blocked cooldown basis summary to humanize acceptance gate wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance gate CONTRADICTIONS_AND_MEMORY_ATTENTION") {
		t.Fatalf("expected blocked cooldown basis summary to avoid raw CONTRADICTIONS_AND_MEMORY_ATTENTION gate wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance progress blocked") {
		t.Fatalf("expected blocked cooldown basis summary to humanize acceptance progress wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance progress BLOCKED") {
		t.Fatalf("expected blocked cooldown basis summary to avoid raw BLOCKED progress wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "stage warming") || !strings.Contains(basis.Summary, "acceptance readiness blocked") {
		t.Fatalf("expected blocked cooldown basis summary to humanize stage/readiness wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "stage WARMING") || strings.Contains(basis.Summary, "acceptance readiness BLOCKED") {
		t.Fatalf("expected blocked cooldown basis summary to avoid raw stage/readiness enums, got %q", basis.Summary)
	}
	if containsString(basis.AcceptanceMissingRequirements, "hysteresis_satisfied") || containsString(basis.AcceptanceMissingRequirements, "cooldown_clear") {
		t.Fatalf("expected blocked cooldown basis to avoid mixing transition debt into missing requirements while blocked, got %+v", basis.AcceptanceMissingRequirements)
	}
	if basis.AcceptanceProgressBand != "BLOCKED" {
		t.Fatalf("expected blocked cooldown basis to keep blocked progress band, got %+v", basis)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected blocked cooldown basis to keep acceptance clear count focused on hard blockers only, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 2 {
		t.Fatalf("expected blocked cooldown basis to keep acceptance requirement count focused on hard blockers only, got %d", got)
	}
	if !strings.Contains(basis.Summary, "active blocker checklist 0/2") {
		t.Fatalf("expected blocked cooldown basis summary to reflect blocker-focused checklist counts, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "reason contradictions and memory attention") {
		t.Fatalf("expected blocked cooldown basis summary to reflect blocker-aware reason, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "blocking cooldown active, streak below hysteresis, contradictions present, memory attention active") {
		t.Fatalf("expected blocked cooldown basis summary to humanize the full active blocker set, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "missing requirements contradiction clear, memory attention clear") {
		t.Fatalf("expected blocked cooldown basis summary to humanize missing requirements, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "blocking contradictions_present, memory_attention_active") || strings.Contains(basis.Summary, "missing requirements contradiction_clear, memory_attention_clear") {
		t.Fatalf("expected blocked cooldown basis summary to avoid raw blocking/missing labels, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsBlockedChecklistCountsFocusedOnActiveContradictionBlocker(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, clusterControlTickHysteresisEpoch-1, false, []string{"hard_safety_conditions_clamped_rsp_advice"}, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "BLOCKED" || basis.AcceptanceGateReason != "CONTRADICTIONS_PRESENT" || basis.Reason != "contradictions_present" {
		t.Fatalf("expected contradiction-only blocked cooldown basis, got %+v", basis)
	}
	if !reflect.DeepEqual(basis.AcceptanceMissingRequirements, []string{"contradiction_clear"}) {
		t.Fatalf("expected contradiction-only blocked cooldown basis to keep only contradiction debt active, got %+v", basis.AcceptanceMissingRequirements)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected contradiction-only blocked cooldown basis to keep blocker clear count at 0, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 1 {
		t.Fatalf("expected contradiction-only blocked cooldown basis to keep blocker requirement count at 1, got %d", got)
	}
	if !strings.Contains(basis.Summary, "active blocker checklist 0/1") {
		t.Fatalf("expected contradiction-only blocked cooldown basis summary to reflect active blocker-scoped checklist counts, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsBlockedChecklistCountsFocusedOnActiveMemoryBlocker(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, clusterControlTickHysteresisEpoch-1, false, nil, true)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "BLOCKED" || basis.AcceptanceGateReason != "MEMORY_ATTENTION_ACTIVE" || basis.Reason != "memory_attention_active" {
		t.Fatalf("expected memory-only blocked cooldown basis, got %+v", basis)
	}
	if !reflect.DeepEqual(basis.AcceptanceMissingRequirements, []string{"memory_attention_clear"}) {
		t.Fatalf("expected memory-only blocked cooldown basis to keep only memory debt active, got %+v", basis.AcceptanceMissingRequirements)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected memory-only blocked cooldown basis to keep blocker clear count at 0, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 1 {
		t.Fatalf("expected memory-only blocked cooldown basis to keep blocker requirement count at 1, got %d", got)
	}
	if !strings.Contains(basis.Summary, "active blocker checklist 0/1") {
		t.Fatalf("expected memory-only blocked cooldown basis summary to reflect active blocker-scoped checklist counts, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsObservingProgressEarlyBeforeStreakStarts(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, 0, false, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "OBSERVING" {
		t.Fatalf("expected zero-streak cooldown basis to stay observing, got %+v", basis)
	}
	if basis.AcceptanceGateReason != "OBSERVING_CANDIDATE" {
		t.Fatalf("expected zero-streak cooldown basis to keep observing gate reason, got %+v", basis)
	}
	if containsString(basis.BlockingReasons, "streak_below_hysteresis") {
		t.Fatalf("expected zero-streak cooldown basis to avoid pre-hysteresis blocker semantics before streak starts, got %+v", basis.BlockingReasons)
	}
	if basis.Reason != "candidate_streak_not_started" {
		t.Fatalf("expected zero-streak cooldown basis to keep not-started reason, got %+v", basis)
	}
	if len(basis.AcceptanceMissingRequirements) != 0 {
		t.Fatalf("expected zero-streak cooldown basis to keep hysteresis visible-but-not-active before the streak starts, got %+v", basis.AcceptanceMissingRequirements)
	}
	if basis.AcceptanceProgressBand != "EARLY" {
		t.Fatalf("expected zero-streak cooldown basis to keep acceptance progress early, got %+v", basis)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected zero-streak cooldown basis to keep observing checklist clear count deferred until a streak starts, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected zero-streak cooldown basis to keep observing checklist requirement count deferred until a streak starts, got %d", got)
	}
	if !strings.Contains(basis.Summary, "observing-deferred checklist 0/0") {
		t.Fatalf("expected zero-streak cooldown basis summary to reflect observing-deferred checklist counts before streak start, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "reason candidate streak not started") {
		t.Fatalf("expected zero-streak cooldown basis summary to humanize candidate_streak_not_started reason wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "reason candidate_streak_not_started") {
		t.Fatalf("expected zero-streak cooldown basis summary to avoid raw candidate_streak_not_started reason wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance gate observing candidate") {
		t.Fatalf("expected zero-streak cooldown basis summary to humanize acceptance gate wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance gate OBSERVING_CANDIDATE") {
		t.Fatalf("expected zero-streak cooldown basis summary to avoid raw OBSERVING_CANDIDATE gate wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "acceptance progress early") {
		t.Fatalf("expected zero-streak cooldown basis summary to humanize acceptance progress wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "acceptance progress EARLY") {
		t.Fatalf("expected zero-streak cooldown basis summary to avoid raw EARLY progress wording, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "stage observing") || !strings.Contains(basis.Summary, "acceptance readiness observing") {
		t.Fatalf("expected zero-streak cooldown basis summary to humanize stage/readiness wording, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "stage OBSERVING") || strings.Contains(basis.Summary, "acceptance readiness OBSERVING") {
		t.Fatalf("expected zero-streak cooldown basis summary to avoid raw stage/readiness enums, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "observing candidate not started") {
		t.Fatalf("expected zero-streak cooldown basis summary to use observing-not-started wording instead of a cleared-path summary, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "no missing acceptance requirements") {
		t.Fatalf("expected zero-streak cooldown basis summary to avoid generic no-missing wording before a streak starts, got %q", basis.Summary)
	}
}

func TestBuildUnifiedControlCooldownBasisKeepsObservingGatePrimaryWhileCooldownActiveBeforeStreakStarts(t *testing.T) {
	t.Parallel()

	basis := buildUnifiedControlCooldownBasis(clusterControlModeStabilize, clusterControlModeSynergySeeking, 0, true, nil, false)
	if basis == nil {
		t.Fatal("expected cooldown basis")
	}
	if basis.AcceptanceReadiness != "OBSERVING" {
		t.Fatalf("expected zero-streak cooldown-active basis to stay observing, got %+v", basis)
	}
	if basis.AcceptanceGateReason != "OBSERVING_CANDIDATE" {
		t.Fatalf("expected zero-streak cooldown-active basis to keep observing gate reason primary until streak starts, got %+v", basis)
	}
	if basis.Reason != "candidate_streak_not_started" {
		t.Fatalf("expected zero-streak cooldown-active basis to keep not-started reason, got %+v", basis)
	}
	if !containsString(basis.BlockingReasons, "cooldown_active") {
		t.Fatalf("expected zero-streak cooldown-active basis to keep cooldown_active visible as background blocker, got %+v", basis.BlockingReasons)
	}
	if containsString(basis.AcceptanceMissingRequirements, "cooldown_clear") {
		t.Fatalf("expected zero-streak cooldown-active basis to keep cooldown_clear out of active observing debt until a streak starts, got %+v", basis.AcceptanceMissingRequirements)
	}
	if containsString(basis.AcceptanceMissingRequirements, "hysteresis_satisfied") {
		t.Fatalf("expected zero-streak cooldown-active basis to keep hysteresis visible-but-not-active until a streak starts, got %+v", basis.AcceptanceMissingRequirements)
	}
	if basis.AcceptanceProgressBand != "EARLY" {
		t.Fatalf("expected zero-streak cooldown-active basis to keep acceptance progress early, got %+v", basis)
	}
	if got := unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected zero-streak cooldown-active basis to keep observing checklist clear count deferred until a streak starts, got %d", got)
	}
	if got := unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist); got != 0 {
		t.Fatalf("expected zero-streak cooldown-active basis to keep observing checklist requirement count deferred until a streak starts, got %d", got)
	}
	if !strings.Contains(basis.Summary, "observing-deferred checklist 0/0") {
		t.Fatalf("expected zero-streak cooldown-active basis summary to reflect observing-deferred checklist counts before streak start, got %q", basis.Summary)
	}
	if !strings.Contains(basis.Summary, "observing candidate not started") {
		t.Fatalf("expected zero-streak cooldown-active basis summary to use observing-not-started wording instead of a cleared-path summary, got %q", basis.Summary)
	}
	if strings.Contains(basis.Summary, "no missing acceptance requirements") {
		t.Fatalf("expected zero-streak cooldown-active basis summary to avoid generic no-missing wording before a streak starts, got %q", basis.Summary)
	}
}

func TestRecordUnifiedControlSnapshotAppendsRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenarioWithAuthority(t, ctx, store, "unified-control-snapshot")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for unified snapshot test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
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
	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-unified-control-snapshot",
		LookupCount:        4,
		L1HitCount:         1,
		L2HitCount:         1,
		StaleHitCount:      2,
		PromotionCount:     1,
		FlushCount:         1,
		FlushPositiveCount: 1,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:           "rtev-unified-control-snapshot-governed-lineage",
		WorkspaceID:       scenario.workspaceID,
		EventType:         "tests.unified_control.snapshot_lineage",
		EntityType:        "test_scope",
		EntityID:          clusterID,
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-unified-control-snapshot",
		ProvenanceGroupID: "PG-unified-control-snapshot",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record unified snapshot lineage event: %v", err)
	}

	filter := UnifiedControlReportFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		AgentID:        "agent-a",
		TaskID:         scenario.taskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.docKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
	}
	report, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build unified control report: %v", err)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected unified control snapshot source report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}

	event, err := store.RecordUnifiedControlSnapshot(ctx, report, filter, UnifiedControlSnapshotInput{
		ActorID: "dashboard",
	})
	if err != nil {
		t.Fatalf("record unified control snapshot: %v", err)
	}
	if event.EventType != "cluster.unified_control_advisory_snapshot" || event.EntityType != "instrumentation_unified_control" || event.EntityID != clusterID {
		t.Fatalf("unexpected unified control snapshot event %+v", event)
	}
	if event.TaskID != scenario.taskID || event.SessionID != scenario.sessionID {
		t.Fatalf("expected scoped unified snapshot to preserve task/session binding, got %+v", event)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode unified control snapshot payload: %v", err)
	}
	if payload["typed_event_type"] != "UNIFIED_CONTROL_ADVISORY_SNAPSHOT" {
		t.Fatalf("expected unified control snapshot typed_event_type, got %+v", payload)
	}
	if payload["event_kind"] != "cluster.unified_control_advisory_snapshot" {
		t.Fatalf("expected unified control snapshot event_kind, got %+v", payload)
	}
	reportPayload, ok := payload["report"].(map[string]any)
	if !ok {
		t.Fatalf("expected embedded unified report payload, got %+v", payload)
	}
	if reportPayload["workspace_id"] != scenario.workspaceID || reportPayload["proto_cluster_id"] != clusterID {
		t.Fatalf("expected embedded unified report scope, got %+v", reportPayload)
	}
	effectiveAudit, ok := reportPayload["effective_controls_audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected embedded effective-controls audit payload, got %+v", reportPayload["effective_controls_audit"])
	}
	if report.EffectiveControlsAudit == nil {
		t.Fatalf("expected report effective-controls audit to be populated, got %+v", report)
	}
	if effectiveAudit["found"] != report.EffectiveControlsAudit.Found || effectiveAudit["live"] != report.EffectiveControlsAudit.Live || effectiveAudit["expired"] != report.EffectiveControlsAudit.Expired {
		t.Fatalf("expected embedded effective-controls audit booleans to mirror report, payload=%+v report=%+v", effectiveAudit, report.EffectiveControlsAudit)
	}
	if payload["governed_hint_count"] == nil || payload["applied_action_count"] == nil {
		t.Fatalf("expected bounded unified snapshot count mirrors, got %+v", payload)
	}
	if payload["effective_controls_found"] != report.EffectiveControlsAudit.Found ||
		payload["effective_controls_live"] != report.EffectiveControlsAudit.Live ||
		payload["effective_controls_expired"] != report.EffectiveControlsAudit.Expired ||
		payload["effective_controls_pending"] != report.EffectiveControlsAudit.Pending ||
		payload["effective_controls_scope_source"] != report.EffectiveControlsAudit.ScopeSource {
		t.Fatalf("expected snapshot payload effective-controls audit fields to mirror report, payload=%+v report=%+v", payload, report.EffectiveControlsAudit)
	}
	if payload["effective_control_basis_field_count"] == nil || payload["effective_control_basis_changed_count"] == nil || payload["effective_control_basis_fields_with_action_trace_count"] == nil {
		t.Fatalf("expected bounded unified snapshot basis summary mirrors, got %+v", payload)
	}
	if payload["contradiction_count"] == nil || payload["hard_safety_contradiction_count"] == nil {
		t.Fatalf("expected bounded unified snapshot contradiction summary mirrors, got %+v", payload)
	}
	if payload["audit_applied_entry_count"] == nil || payload["audit_suppressed_entry_count"] == nil || payload["audit_coverage_full_applied_trace_entry_count"] == nil || payload["audit_coverage_full_suppressed_trace_entry_count"] == nil {
		t.Fatalf("expected bounded unified snapshot audit summary/coverage mirrors, got %+v", payload)
	}
	basisSummary := report.EffectiveControlBasisSummary
	if basisSummary == nil {
		t.Fatalf("expected unified control report basis summary, got nil")
	}
	if payload["effective_control_basis_field_count"] != float64(basisSummary.FieldCount) ||
		payload["effective_control_basis_changed_count"] != float64(basisSummary.ChangedFieldCount) ||
		payload["effective_control_basis_fields_with_action_trace_count"] != float64(basisSummary.FieldsWithActionTraceCount) ||
		payload["effective_control_basis_fields_with_hint_trace_count"] != float64(basisSummary.FieldsWithHintTraceCount) {
		t.Fatalf("expected unified control snapshot basis summary counts to mirror report, got payload=%+v summary=%+v", payload, basisSummary)
	}
	contradictionSummary := report.ContradictionSummary
	if contradictionSummary == nil {
		if payload["contradiction_count"] != float64(0) ||
			payload["hard_safety_contradiction_count"] != float64(0) ||
			payload["memory_safety_override_contradiction_count"] != float64(0) {
			t.Fatalf("expected unified control snapshot contradiction summary counts to stay zero when report summary is nil, got payload=%+v", payload)
		}
	} else if payload["contradiction_count"] != float64(contradictionSummary.TotalCount) ||
		payload["hard_safety_contradiction_count"] != float64(contradictionSummary.HardSafetyClampCount) ||
		payload["memory_safety_override_contradiction_count"] != float64(contradictionSummary.MemorySafetyOverrideCount) {
		t.Fatalf("expected unified control snapshot contradiction summary counts to mirror report, got payload=%+v summary=%+v", payload, contradictionSummary)
	}
	auditSummary := report.AuditSummary
	if auditSummary == nil {
		if payload["audit_applied_entry_count"] != float64(0) ||
			payload["audit_hint_backed_action_count"] != float64(0) ||
			payload["audit_delta_bearing_action_count"] != float64(0) ||
			payload["audit_suppressed_entry_count"] != float64(0) ||
			payload["audit_suppressed_entries_with_action_ref_count"] != float64(0) {
			t.Fatalf("expected unified control snapshot audit summary counts to stay zero when report summary is nil, got payload=%+v", payload)
		}
	} else if payload["audit_applied_entry_count"] != float64(auditSummary.AppliedEntryCount) ||
		payload["audit_hint_backed_action_count"] != float64(auditSummary.HintBackedActionCount) ||
		payload["audit_delta_bearing_action_count"] != float64(auditSummary.DeltaBearingActionCount) ||
		payload["audit_suppressed_entry_count"] != float64(auditSummary.SuppressedEntryCount) ||
		payload["audit_suppressed_entries_with_action_ref_count"] != float64(auditSummary.SuppressedEntriesWithActionRef) {
		t.Fatalf("expected unified control snapshot audit summary counts to mirror report, got payload=%+v summary=%+v", payload, auditSummary)
	}
	auditCoverage := report.AuditCoverage
	if auditCoverage == nil {
		if payload["audit_coverage_applied_entries_with_source_kinds"] != float64(0) ||
			payload["audit_coverage_applied_entries_with_hint_refs"] != float64(0) ||
			payload["audit_coverage_applied_entries_with_delta_fields"] != float64(0) ||
			payload["audit_coverage_applied_entries_with_summary"] != float64(0) ||
			payload["audit_coverage_full_applied_trace_entry_count"] != float64(0) ||
			payload["audit_coverage_suppressed_entries_with_source_kind"] != float64(0) ||
			payload["audit_coverage_suppressed_entries_with_action_ref"] != float64(0) ||
			payload["audit_coverage_suppressed_entries_with_reason"] != float64(0) ||
			payload["audit_coverage_suppressed_entries_with_summary"] != float64(0) ||
			payload["audit_coverage_full_suppressed_trace_entry_count"] != float64(0) {
			t.Fatalf("expected unified control snapshot audit coverage counts to stay zero when report coverage is nil, got payload=%+v", payload)
		}
	} else if payload["audit_coverage_applied_entries_with_source_kinds"] != float64(auditCoverage.AppliedEntriesWithSourceKinds) ||
		payload["audit_coverage_applied_entries_with_hint_refs"] != float64(auditCoverage.AppliedEntriesWithHintRefs) ||
		payload["audit_coverage_applied_entries_with_delta_fields"] != float64(auditCoverage.AppliedEntriesWithDeltaFields) ||
		payload["audit_coverage_applied_entries_with_summary"] != float64(auditCoverage.AppliedEntriesWithSummary) ||
		payload["audit_coverage_full_applied_trace_entry_count"] != float64(auditCoverage.FullAppliedTraceEntryCount) ||
		payload["audit_coverage_suppressed_entries_with_source_kind"] != float64(auditCoverage.SuppressedEntriesWithSourceKind) ||
		payload["audit_coverage_suppressed_entries_with_action_ref"] != float64(auditCoverage.SuppressedEntriesWithActionRef) ||
		payload["audit_coverage_suppressed_entries_with_reason"] != float64(auditCoverage.SuppressedEntriesWithReason) ||
		payload["audit_coverage_suppressed_entries_with_summary"] != float64(auditCoverage.SuppressedEntriesWithSummary) ||
		payload["audit_coverage_full_suppressed_trace_entry_count"] != float64(auditCoverage.FullSuppressedTraceEntryCount) {
		t.Fatalf("expected unified control snapshot audit coverage counts to mirror report, got payload=%+v coverage=%+v", payload, auditCoverage)
	}
	if payload["cooldown_stage"] == nil || payload["cooldown_acceptance_readiness"] == nil || payload["cooldown_acceptance_gate_reason"] == nil || payload["cooldown_acceptance_clear_count"] == nil || payload["cooldown_acceptance_requirement_count"] == nil || payload["cooldown_acceptance_missing_requirement_count"] == nil || payload["cooldown_acceptance_progress_band"] == nil || payload["cooldown_candidate_streak"] == nil || payload["cooldown_required_streak"] == nil || payload["cooldown_remaining_streak"] == nil || payload["cooldown_ready_to_stabilize"] == nil || payload["cooldown_blocking_reason_count"] == nil || payload["cooldown_reason"] == nil {
		t.Fatalf("expected bounded unified snapshot cooldown mirrors, got %+v", payload)
	}
	if _, ok := payload["cooldown_acceptance_missing_requirements"]; !ok {
		t.Fatalf("expected bounded unified snapshot missing-requirements array mirror key, got %+v", payload)
	}
	if _, ok := payload["cooldown_blocking_reasons"]; !ok {
		t.Fatalf("expected bounded unified snapshot blocking-reasons array mirror key, got %+v", payload)
	}
	cooldownBasis := report.CooldownBasis
	if cooldownBasis == nil {
		t.Fatalf("expected unified control report cooldown basis, got nil")
	}
	if payload["cooldown_current_mode"] != cooldownBasis.CurrentMode ||
		payload["cooldown_candidate_mode"] != cooldownBasis.CandidateMode ||
		payload["cooldown_stage"] != cooldownBasis.Stage ||
		payload["cooldown_acceptance_readiness"] != cooldownBasis.AcceptanceReadiness ||
		payload["cooldown_acceptance_gate_reason"] != cooldownBasis.AcceptanceGateReason ||
		payload["cooldown_acceptance_progress_band"] != cooldownBasis.AcceptanceProgressBand ||
		payload["cooldown_reason"] != cooldownBasis.Reason {
		t.Fatalf("expected unified control snapshot cooldown strings to mirror report basis, got payload=%+v basis=%+v", payload, cooldownBasis)
	}
	if payload["cooldown_acceptance_clear_count"] != float64(unifiedControlAcceptanceChecklistClearCount(cooldownBasis.AcceptanceReadiness, cooldownBasis.AcceptanceChecklist)) ||
		payload["cooldown_acceptance_requirement_count"] != float64(unifiedControlAcceptanceChecklistRequirementCount(cooldownBasis.AcceptanceReadiness, cooldownBasis.AcceptanceChecklist)) ||
		payload["cooldown_acceptance_missing_requirement_count"] != float64(len(cooldownBasis.AcceptanceMissingRequirements)) ||
		payload["cooldown_candidate_streak"] != float64(cooldownBasis.CandidateStreak) ||
		payload["cooldown_required_streak"] != float64(cooldownBasis.RequiredStreak) ||
		payload["cooldown_remaining_streak"] != float64(cooldownBasis.RemainingStreak) ||
		payload["cooldown_blocking_reason_count"] != float64(cooldownBasis.BlockingReasonCount) {
		t.Fatalf("expected unified control snapshot cooldown counts to mirror report basis, got payload=%+v basis=%+v", payload, cooldownBasis)
	}
	gotMissingRequirements := []string(nil)
	if payload["cooldown_acceptance_missing_requirements"] != nil {
		missingRequirements, ok := payload["cooldown_acceptance_missing_requirements"].([]any)
		if !ok {
			t.Fatalf("expected cooldown_acceptance_missing_requirements array mirror, got %+v", payload["cooldown_acceptance_missing_requirements"])
		}
		gotMissingRequirements = make([]string, 0, len(missingRequirements))
		for _, item := range missingRequirements {
			text, ok := item.(string)
			if !ok {
				t.Fatalf("expected cooldown_acceptance_missing_requirements entries to stay strings, got %+v", missingRequirements)
			}
			gotMissingRequirements = append(gotMissingRequirements, text)
		}
	}
	if !reflect.DeepEqual(gotMissingRequirements, cooldownBasis.AcceptanceMissingRequirements) {
		t.Fatalf("expected unified control snapshot missing-requirement array to mirror report basis, got payload=%+v basis=%+v", gotMissingRequirements, cooldownBasis.AcceptanceMissingRequirements)
	}
	gotBlockingReasons := []string(nil)
	if payload["cooldown_blocking_reasons"] != nil {
		blockingReasons, ok := payload["cooldown_blocking_reasons"].([]any)
		if !ok {
			t.Fatalf("expected cooldown_blocking_reasons array mirror, got %+v", payload["cooldown_blocking_reasons"])
		}
		gotBlockingReasons = make([]string, 0, len(blockingReasons))
		for _, item := range blockingReasons {
			text, ok := item.(string)
			if !ok {
				t.Fatalf("expected cooldown_blocking_reasons entries to stay strings, got %+v", blockingReasons)
			}
			gotBlockingReasons = append(gotBlockingReasons, text)
		}
	}
	if !reflect.DeepEqual(gotBlockingReasons, cooldownBasis.BlockingReasons) {
		t.Fatalf("expected unified control snapshot blocking-reason array to mirror report basis, got payload=%+v basis=%+v", gotBlockingReasons, cooldownBasis.BlockingReasons)
	}
	if payload["cooldown_ready_to_stabilize"] != cooldownBasis.ReadyToStabilize || payload["cooldown_transitioning"] != cooldownBasis.Transitioning {
		t.Fatalf("expected unified control snapshot cooldown booleans to mirror report basis, got payload=%+v basis=%+v", payload, cooldownBasis)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.unified_control_advisory_snapshot",
		EntityType:  "instrumentation_unified_control",
		EntityID:    clusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list unified control snapshot runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted unified control snapshot event, got %+v", events)
	}
}

func TestRecordUnifiedControlSnapshotUsesEffectiveEventTypeWhenPersistedControlsLive(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenarioWithAuthority(t, ctx, store, "unified-control-effective-snapshot")

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

	filter := UnifiedControlReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}
	baseline, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build baseline unified control report: %v", err)
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    baseline.ProtoClusterID,
		Epoch:             4,
		TTLSeconds:        600,
		ControlMode:       baseline.ControlMode,
		CandidateMode:     baseline.CandidateMode,
		CandidateControls: baseline.CandidateControls,
		AdvisoryControls:  baseline.AdvisoryControls,
		EffectiveControls: ControlSuggestedControls{
			FanoutCap:      maxInt(baseline.CandidateControls.FanoutCap-1, 1),
			ReviewDepth:    baseline.CandidateControls.ReviewDepth + 1,
			ContextCap:     maxInt(baseline.CandidateControls.ContextCap-1, 1),
			BridgeQuota:    baseline.CandidateControls.BridgeQuota,
			MergeThreshold: baseline.CandidateControls.MergeThreshold + 1,
			PriorityFocus:  "effective-snapshot",
		},
		ResolvedFrom: "snapshot_live_test",
		MatchScore:   baseline.MatchScore,
		BasisSummary: "live effective snapshot should classify correctly",
		GeneratedAt:  baseline.GeneratedAt,
		ActorID:      "tests",
	}); err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}

	report, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build live unified control report: %v", err)
	}
	if report.AdvisoryOnly {
		t.Fatalf("expected persisted live effective controls to clear advisory-only posture, got %+v", report)
	}

	event, err := store.RecordUnifiedControlSnapshot(ctx, report, filter, UnifiedControlSnapshotInput{ActorID: "dashboard"})
	if err != nil {
		t.Fatalf("record effective unified control snapshot: %v", err)
	}
	if event.EventType != "cluster.unified_control_effective_snapshot" {
		t.Fatalf("expected effective unified control snapshot event type, got %+v", event)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode effective unified control snapshot payload: %v", err)
	}
	if payload["typed_event_type"] != "UNIFIED_CONTROL_EFFECTIVE_SNAPSHOT" {
		t.Fatalf("expected effective unified control snapshot typed_event_type, got %+v", payload)
	}
	if payload["event_kind"] != "cluster.unified_control_effective_snapshot" {
		t.Fatalf("expected effective unified control snapshot event_kind, got %+v", payload)
	}
	if summary, ok := payload["summary"].(string); !ok || !strings.Contains(summary, "Unified control effective snapshot") || strings.Contains(summary, "advisory snapshot") {
		t.Fatalf("expected effective unified control snapshot summary to name the effective path, got %+v", payload["summary"])
	}
	if payload["effective_controls_live"] != true || payload["effective_controls_pending"] != false {
		t.Fatalf("expected effective unified control snapshot payload to reflect live non-pending controls, got %+v", payload)
	}
}

func assertUnifiedEffectiveControlsTemporalContract(t *testing.T, contract *TemporalHorizonContract, state, scopeSource string) {
	t.Helper()
	if contract == nil {
		t.Fatalf("expected unified effective-controls temporal contract, got nil")
	}
	if contract.SchemaVersion != temporalContractSchemaVersion ||
		contract.Domain != "effective_controls" ||
		contract.HorizonKind != "ttl_window" ||
		contract.Basis != temporalBasisWallClock ||
		contract.Mapping != temporalMappingExactWallClock ||
		!contract.WallClockComparable {
		t.Fatalf("expected unified effective-controls wall-clock contract, got %+v", contract)
	}
	if contract.State != state || contract.ScopeSource != scopeSource {
		t.Fatalf("expected unified effective-controls temporal contract state=%s scope=%s, got %+v", state, scopeSource, contract)
	}
}

func countRuntimeEventsByType(events []RuntimeEventRecord, eventType string) int {
	count := 0
	for _, event := range events {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}
