package server

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationUnifiedControlReport(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}
	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-handler-unified-control",
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
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-handler-unified-governed-lineage",
		WorkspaceID:       scenario.workspaceID,
		EventType:         "tests.handler.unified.governed_hint_lineage",
		EntityType:        "test_scope",
		EntityID:          clusterID,
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-handler-unified-governed",
		ProvenanceGroupID: "PG-handler-unified-governed",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record governed hint lineage event: %v", err)
	}

	raw, err := json.Marshal(workspaceInstrumentationUnifiedControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		AgentID:        "agent-a",
		TaskID:         scenario.primaryTaskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.runbookDocKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
	})
	if err != nil {
		t.Fatalf("marshal unified control params: %v", err)
	}

	result, rpcErr := h.workspaceInstrumentationUnifiedControlReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationUnifiedControlReport rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected unified control result type %T", result)
	}
	report, ok := payload["report"].(sqlite.UnifiedControlReport)
	if !ok {
		t.Fatalf("unexpected unified control payload type %T", payload["report"])
	}
	if payload["workspace_id"] != scenario.workspaceID || payload["summary"] != report.Summary {
		t.Fatalf("expected handler envelope to mirror workspace and summary, got %+v", payload)
	}
	if payload["advisory_only"] != report.AdvisoryOnly {
		t.Fatalf("expected handler envelope advisory_only mirror, got %+v report=%+v", payload, report)
	}
	if capabilityFlags, ok := payload["capability_flags"].(sqlite.RSPCapabilityFlags); !ok || capabilityFlags != report.CapabilityFlags {
		t.Fatalf("expected handler envelope capability_flags mirror, got %+v report=%+v", payload["capability_flags"], report.CapabilityFlags)
	}
	if advisoryControls, ok := payload["advisory_controls"].(sqlite.ControlSuggestedControls); !ok || advisoryControls != report.AdvisoryControls {
		t.Fatalf("expected handler envelope advisory_controls mirror, got %+v report=%+v", payload["advisory_controls"], report.AdvisoryControls)
	}
	if candidateControls, ok := payload["candidate_controls"].(sqlite.ControlSuggestedControls); !ok || candidateControls != report.CandidateControls {
		t.Fatalf("expected handler envelope candidate_controls mirror, got %+v report=%+v", payload["candidate_controls"], report.CandidateControls)
	}
	if effectiveControls, ok := payload["effective_controls"].(sqlite.ControlSuggestedControls); !ok || effectiveControls != report.EffectiveControls {
		t.Fatalf("expected handler envelope effective_controls mirror, got %+v report=%+v", payload["effective_controls"], report.EffectiveControls)
	}
	if audit, ok := payload["effective_controls_audit"].(*sqlite.UnifiedControlEffectiveControlsAudit); !ok || !reflect.DeepEqual(audit, report.EffectiveControlsAudit) {
		t.Fatalf("expected handler envelope effective_controls_audit mirror, got %+v report=%+v", payload["effective_controls_audit"], report.EffectiveControlsAudit)
	}
	if basis, ok := payload["effective_control_basis"].([]sqlite.UnifiedControlEffectiveControlBasis); !ok || !reflect.DeepEqual(basis, report.EffectiveControlBasis) {
		t.Fatalf("expected handler envelope effective_control_basis mirror, got %+v report=%+v", payload["effective_control_basis"], report.EffectiveControlBasis)
	}
	if basisSummary, ok := payload["effective_control_basis_summary"].(*sqlite.UnifiedControlEffectiveControlBasisSummary); !ok || !reflect.DeepEqual(basisSummary, report.EffectiveControlBasisSummary) {
		t.Fatalf("expected handler envelope effective_control_basis_summary mirror, got %+v report=%+v", payload["effective_control_basis_summary"], report.EffectiveControlBasisSummary)
	}
	if contradictionSummary, ok := payload["contradiction_summary"].(*sqlite.UnifiedControlContradictionSummary); !ok || !reflect.DeepEqual(contradictionSummary, report.ContradictionSummary) {
		t.Fatalf("expected handler envelope contradiction_summary mirror, got %+v report=%+v", payload["contradiction_summary"], report.ContradictionSummary)
	}
	if !report.Resolved || report.ProtoClusterID != clusterID {
		t.Fatalf("expected resolved unified control report for %s, got %+v", clusterID, report)
	}
	if len(report.ControlOrder) != 5 || report.ControlOrder[0] != "event_time_ingest" {
		t.Fatalf("unexpected unified control order %+v", report.ControlOrder)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected handler response to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected unified control handler report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if !report.CapabilityFlags.GovernedHintsLive {
		t.Fatalf("expected governed hint capability flag in handler response, got %+v", report.CapabilityFlags)
	}
	var sawHintInspectability bool
	for _, hint := range report.GovernedHints {
		if hint.RecommendationClass != "" && hint.EvidenceDiversity > 0 && hint.EvidenceDiversityBand != "" && hint.EvidenceSourceMix != "" && hint.RuntimeLineageBasis != "" && hint.TTLWindowState != "" && len(hint.EvidenceSourceKinds) > 0 && hint.Summary != "" {
			sawHintInspectability = true
			break
		}
	}
	if !sawHintInspectability {
		t.Fatalf("expected handler response to expose governed-hint inspectability fields, got %+v", report.GovernedHints)
	}
	if report.RSPHiddenState == "" || report.RSPRiskBand == "" {
		t.Fatalf("expected handler response to expose rsp arbitration state, got %+v", report)
	}
	if len(report.AppliedActions) == 0 {
		t.Fatalf("expected handler response to expose applied arbitration actions, got %+v", report)
	}
	if len(report.AppliedActionAudit) == 0 {
		t.Fatalf("expected handler response to expose structured applied action audit, got %+v", report)
	}
	if len(report.GovernedHintOutcomes) == 0 {
		t.Fatalf("expected handler response to expose governed-hint outcomes, got %+v", report)
	}
	if report.GovernedHintSummary == nil || report.GovernedHintSummary.TotalHints != len(report.GovernedHints) || report.GovernedHintSummary.OutcomeCount["ADVISORY_ROUTED"] == 0 {
		t.Fatalf("expected handler response to expose governed-hint summary rollup, got %+v", report.GovernedHintSummary)
	}
	if len(report.AppliedActionAudit) != len(report.AppliedActions) {
		t.Fatalf("expected structured applied action audit to mirror legacy action count, actions=%+v audit=%+v", report.AppliedActions, report.AppliedActionAudit)
	}
	if report.AuditSummary == nil {
		t.Fatalf("expected handler response to expose audit summary, got %+v", report)
	}
	if report.AuditSummary.AppliedEntryCount != len(report.AppliedActionAudit) || report.AuditSummary.HintBackedActionCount == 0 || report.AuditSummary.DeltaBearingActionCount == 0 {
		t.Fatalf("expected handler audit summary to stay aligned with structured traces, got %+v", report.AuditSummary)
	}
	if report.AuditCoverage == nil || report.AuditCoverage.AppliedEntriesWithSourceKinds != len(report.AppliedActionAudit) || report.AuditCoverage.FullAppliedTraceEntryCount == 0 {
		t.Fatalf("expected handler audit coverage to stay aligned with structured traces, got %+v", report.AuditCoverage)
	}
	if len(report.EffectiveControlBasis) != 6 {
		t.Fatalf("expected handler response to expose per-control effective basis, got %+v", report.EffectiveControlBasis)
	}
	if report.EffectiveControlBasisSummary == nil || report.EffectiveControlBasisSummary.FieldCount != len(report.EffectiveControlBasis) || report.EffectiveControlBasisSummary.FieldsWithActionTraceCount == 0 {
		t.Fatalf("expected handler response to expose effective-control basis summary, got %+v", report.EffectiveControlBasisSummary)
	}
	if report.CooldownBasis == nil || report.CooldownBasis.CooldownActive != report.CooldownActive || report.CooldownBasis.CurrentMode != report.ControlMode || report.CooldownBasis.CandidateMode != report.CandidateMode || report.CooldownBasis.Stage == "" || report.CooldownBasis.Reason == "" || report.CooldownBasis.Summary == "" {
		t.Fatalf("expected handler response to expose cooldown basis aligned with control state, got %+v report=%+v", report.CooldownBasis, report)
	}
	if expectedReason := expectedHandlerCooldownReason(report.CooldownBasis); report.CooldownBasis.Reason != expectedReason {
		t.Fatalf("expected handler response to keep cooldown reason aligned, got actual=%q expected=%q basis=%+v", report.CooldownBasis.Reason, expectedReason, report.CooldownBasis)
	}
	if report.CooldownBasis.AcceptanceReadiness == "" {
		t.Fatalf("expected handler response to expose bounded acceptance readiness, got %+v", report.CooldownBasis)
	}
	if expectedGateReason := expectedHandlerAcceptanceGateReason(report.CooldownBasis); report.CooldownBasis.AcceptanceGateReason != expectedGateReason {
		t.Fatalf("expected handler response to keep acceptance gate reason aligned, got actual=%q expected=%q basis=%+v", report.CooldownBasis.AcceptanceGateReason, expectedGateReason, report.CooldownBasis)
	}
	if report.CooldownBasis.AcceptanceChecklist == nil {
		t.Fatalf("expected handler response to expose bounded acceptance checklist, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.AcceptanceChecklist.CandidatePresent != (report.CooldownBasis.CandidateMode != "") || report.CooldownBasis.AcceptanceChecklist.CandidateDiverges != (report.CooldownBasis.CandidateMode != "" && report.CooldownBasis.CandidateMode != report.CooldownBasis.CurrentMode) {
		t.Fatalf("expected handler response to keep acceptance checklist aligned with candidate context, got %+v", report.CooldownBasis.AcceptanceChecklist)
	}
	if report.CooldownBasis.AcceptanceChecklist.HysteresisSatisfied != (report.CooldownBasis.CandidateMode != "" && report.CooldownBasis.CandidateMode != report.CooldownBasis.CurrentMode && report.CooldownBasis.ReadyToStabilize) {
		t.Fatalf("expected handler response to keep acceptance checklist hysteresis aligned, got %+v", report.CooldownBasis.AcceptanceChecklist)
	}
	if report.CooldownBasis.AcceptanceChecklist.CooldownClear != !report.CooldownBasis.CooldownActive || report.CooldownBasis.AcceptanceChecklist.ContradictionClear != (len(report.Contradictions) == 0) || report.CooldownBasis.AcceptanceChecklist.MemoryAttentionClear != !report.MemoryNeedsAttention {
		t.Fatalf("expected handler response to keep acceptance checklist aligned with report state, got checklist=%+v report=%+v", report.CooldownBasis.AcceptanceChecklist, report)
	}
	if expectedMissing := expectedHandlerAcceptanceMissingRequirements(report.CooldownBasis); !reflect.DeepEqual(report.CooldownBasis.AcceptanceMissingRequirements, expectedMissing) {
		t.Fatalf("expected handler response to keep acceptance missing requirements aligned, got actual=%+v expected=%+v", report.CooldownBasis.AcceptanceMissingRequirements, expectedMissing)
	}
	if expectedBand := expectedHandlerAcceptanceProgressBand(report.CooldownBasis); report.CooldownBasis.AcceptanceProgressBand != expectedBand {
		t.Fatalf("expected handler response to keep acceptance progress band aligned, got actual=%q expected=%q", report.CooldownBasis.AcceptanceProgressBand, expectedBand)
	}
	if report.CooldownBasis.RequiredStreak <= 0 || report.CooldownBasis.RemainingStreak < 0 {
		t.Fatalf("expected handler response to expose cooldown transition window, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.BlockingReasonCount != len(report.CooldownBasis.BlockingReasons) {
		t.Fatalf("expected handler response to keep cooldown blocking counts aligned, got %+v", report.CooldownBasis)
	}
	if report.CooldownBasis.CooldownActive && report.CooldownBasis.ReadyToStabilize {
		if report.CooldownBasis.AcceptanceReadiness != "READY_PENDING" || report.CooldownBasis.AcceptanceGateReason != "COOLDOWN_ACTIVE" || report.CooldownBasis.AcceptanceProgressBand != "READY_WINDOW_PENDING" || !containsString(report.CooldownBasis.AcceptanceMissingRequirements, "cooldown_clear") {
			t.Fatalf("expected handler response to keep cooldown-active ready window pending but not fully clear, got %+v", report.CooldownBasis)
		}
	}
	if len(report.Contradictions) == 0 {
		if report.ContradictionSummary != nil {
			t.Fatalf("expected no contradiction summary when handler report has no contradictions, got %+v", report.ContradictionSummary)
		}
	} else if report.ContradictionSummary == nil || report.ContradictionSummary.TotalCount != len(report.Contradictions) {
		t.Fatalf("expected handler response to expose contradiction summary, got %+v contradictions=%+v", report.ContradictionSummary, report.Contradictions)
	}
	mergeThresholdBasis := findUnifiedControlBasis(report.EffectiveControlBasis, "merge_threshold")
	if mergeThresholdBasis == nil || !mergeThresholdBasis.Changed || !containsString(mergeThresholdBasis.AppliedActions, "memory_coherence_floor") || mergeThresholdBasis.Summary == "" {
		t.Fatalf("expected handler response to preserve merge_threshold basis provenance, got %+v", report.EffectiveControlBasis)
	}
	var auditActions []string
	for _, entry := range report.AppliedActionAudit {
		auditActions = append(auditActions, entry.Action)
		if entry.Summary == "" {
			t.Fatalf("expected structured applied action summary, got %+v", entry)
		}
	}
	if !reflect.DeepEqual(auditActions, report.AppliedActions) {
		t.Fatalf("expected handler structured audit to preserve action order, audit=%+v legacy=%+v", auditActions, report.AppliedActions)
	}
	if report.MemoryCoherenceBand == "" {
		t.Fatalf("expected memory coherence band in handler response, got %+v", report)
	}
}

func findUnifiedControlBasis(items []sqlite.UnifiedControlEffectiveControlBasis, field string) *sqlite.UnifiedControlEffectiveControlBasis {
	for i := range items {
		if items[i].Field == field {
			return &items[i]
		}
	}
	return nil
}

func expectedHandlerAcceptanceMissingRequirements(basis *sqlite.UnifiedControlCooldownBasis) []string {
	if basis == nil || basis.AcceptanceChecklist == nil {
		return nil
	}
	switch strings.TrimSpace(basis.AcceptanceReadiness) {
	case "UNAVAILABLE", "ALIGNED":
		return nil
	case "BLOCKED":
		missing := make([]string, 0, 2)
		if !basis.AcceptanceChecklist.ContradictionClear {
			missing = append(missing, "contradiction_clear")
		}
		if !basis.AcceptanceChecklist.MemoryAttentionClear {
			missing = append(missing, "memory_attention_clear")
		}
		if len(missing) == 0 {
			return nil
		}
		return missing
	case "OBSERVING":
		return nil
	case "WARMING":
		missing := make([]string, 0, 5)
		if !basis.AcceptanceChecklist.CandidatePresent {
			missing = append(missing, "candidate_present")
		} else if !basis.AcceptanceChecklist.CandidateDiverges {
			missing = append(missing, "candidate_diverges")
		}
		if basis.AcceptanceChecklist.CandidatePresent && basis.AcceptanceChecklist.CandidateDiverges && !basis.AcceptanceChecklist.HysteresisSatisfied {
			missing = append(missing, "hysteresis_satisfied")
		}
		if !basis.AcceptanceChecklist.ContradictionClear {
			missing = append(missing, "contradiction_clear")
		}
		if !basis.AcceptanceChecklist.MemoryAttentionClear {
			missing = append(missing, "memory_attention_clear")
		}
		if len(missing) == 0 {
			return nil
		}
		return missing
	}
	missing := make([]string, 0, 6)
	if !basis.AcceptanceChecklist.CandidatePresent {
		missing = append(missing, "candidate_present")
	} else if !basis.AcceptanceChecklist.CandidateDiverges {
		missing = append(missing, "candidate_diverges")
	}
	if basis.AcceptanceChecklist.CandidatePresent && basis.AcceptanceChecklist.CandidateDiverges && !basis.AcceptanceChecklist.HysteresisSatisfied {
		missing = append(missing, "hysteresis_satisfied")
	}
	if !basis.AcceptanceChecklist.CooldownClear {
		missing = append(missing, "cooldown_clear")
	}
	if !basis.AcceptanceChecklist.ContradictionClear {
		missing = append(missing, "contradiction_clear")
	}
	if !basis.AcceptanceChecklist.MemoryAttentionClear {
		missing = append(missing, "memory_attention_clear")
	}
	if len(missing) == 0 {
		return nil
	}
	return missing
}

func expectedHandlerAcceptanceGateReason(basis *sqlite.UnifiedControlCooldownBasis) string {
	if basis == nil {
		return ""
	}
	switch {
	case strings.TrimSpace(basis.CandidateMode) == "":
		return "NO_CANDIDATE"
	case strings.TrimSpace(basis.CandidateMode) == strings.TrimSpace(basis.CurrentMode):
		return "ALREADY_ALIGNED"
	case containsString(basis.BlockingReasons, "contradictions_present") && containsString(basis.BlockingReasons, "memory_attention_active"):
		return "CONTRADICTIONS_AND_MEMORY_ATTENTION"
	case containsString(basis.BlockingReasons, "contradictions_present"):
		return "CONTRADICTIONS_PRESENT"
	case containsString(basis.BlockingReasons, "memory_attention_active"):
		return "MEMORY_ATTENTION_ACTIVE"
	case containsString(basis.BlockingReasons, "streak_below_hysteresis"):
		return "STREAK_BELOW_HYSTERESIS"
	case basis.CandidateStreak <= 0:
		return "OBSERVING_CANDIDATE"
	case containsString(basis.BlockingReasons, "cooldown_active"):
		return "COOLDOWN_ACTIVE"
	case basis.ReadyToStabilize:
		return "READY_WINDOW_OPEN"
	default:
		return "OBSERVING_CANDIDATE"
	}
}

func expectedHandlerCooldownReason(basis *sqlite.UnifiedControlCooldownBasis) string {
	if basis == nil {
		return ""
	}
	switch {
	case strings.TrimSpace(basis.CandidateMode) == "":
		return "no_candidate_mode"
	case strings.TrimSpace(basis.CandidateMode) == strings.TrimSpace(basis.CurrentMode):
		return "candidate_aligned"
	case containsString(basis.BlockingReasons, "contradictions_present") && containsString(basis.BlockingReasons, "memory_attention_active"):
		return "contradictions_and_memory_attention"
	case containsString(basis.BlockingReasons, "contradictions_present"):
		return "contradictions_present"
	case containsString(basis.BlockingReasons, "memory_attention_active"):
		return "memory_attention_active"
	case basis.ReadyToStabilize && basis.CooldownActive:
		return "ready_window_pending_cooldown"
	case basis.ReadyToStabilize:
		return "ready_window_open"
	case containsString(basis.BlockingReasons, "streak_below_hysteresis"):
		return "hysteresis_pending"
	case basis.CandidateStreak <= 0:
		return "candidate_streak_not_started"
	case basis.CooldownActive:
		return "candidate_transition_pending"
	default:
		return "candidate_transition_observing"
	}
}

func expectedHandlerAcceptanceProgressBand(basis *sqlite.UnifiedControlCooldownBasis) string {
	if basis == nil {
		return ""
	}
	switch strings.TrimSpace(basis.AcceptanceReadiness) {
	case "UNAVAILABLE":
		return "NONE"
	case "ALIGNED":
		return "ALIGNED"
	case "BLOCKED":
		return "BLOCKED"
	case "OBSERVING":
		return "EARLY"
	}
	clearCount := cooldownAcceptanceChecklistClearCountForTest(basis)
	requirementCount := cooldownAcceptanceChecklistRequirementCountForTest(basis)
	switch {
	case strings.TrimSpace(basis.AcceptanceReadiness) == "READY_PENDING" && requirementCount > 0 && clearCount >= requirementCount && len(basis.AcceptanceMissingRequirements) == 0:
		return "FULLY_CLEAR"
	case strings.TrimSpace(basis.AcceptanceReadiness) == "READY_PENDING" && containsString(basis.AcceptanceMissingRequirements, "cooldown_clear"):
		return "READY_WINDOW_PENDING"
	case containsString(basis.AcceptanceMissingRequirements, "hysteresis_satisfied"):
		return "PARTIAL"
	case requirementCount > 0 && clearCount >= requirementCount:
		return "NEARLY_READY"
	case clearCount >= 3:
		return "PARTIAL"
	default:
		return "EARLY"
	}
}

func cooldownAcceptanceChecklistClearCountForTest(basis *sqlite.UnifiedControlCooldownBasis) int {
	if basis == nil || basis.AcceptanceChecklist == nil {
		return 0
	}
	switch strings.TrimSpace(basis.AcceptanceReadiness) {
	case "UNAVAILABLE", "ALIGNED":
		return 0
	case "READY_PENDING":
		if basis.AcceptanceChecklist.CooldownClear {
			return 1
		}
		return 0
	case "BLOCKED":
		return 0
	case "OBSERVING":
		return 0
	case "WARMING":
		count := 0
		if basis.AcceptanceChecklist.CandidatePresent {
			count++
		}
		if basis.AcceptanceChecklist.CandidateDiverges {
			count++
		}
		if basis.AcceptanceChecklist.HysteresisSatisfied {
			count++
		}
		if basis.AcceptanceChecklist.ContradictionClear {
			count++
		}
		if basis.AcceptanceChecklist.MemoryAttentionClear {
			count++
		}
		return count
	default:
		count := 0
		if basis.AcceptanceChecklist.CandidatePresent {
			count++
		}
		if basis.AcceptanceChecklist.CandidateDiverges {
			count++
		}
		if basis.AcceptanceChecklist.HysteresisSatisfied {
			count++
		}
		if basis.AcceptanceChecklist.CooldownClear {
			count++
		}
		if basis.AcceptanceChecklist.ContradictionClear {
			count++
		}
		if basis.AcceptanceChecklist.MemoryAttentionClear {
			count++
		}
		return count
	}
}

func cooldownAcceptanceChecklistRequirementCountForTest(basis *sqlite.UnifiedControlCooldownBasis) int {
	if basis == nil || basis.AcceptanceChecklist == nil {
		return 0
	}
	switch strings.TrimSpace(basis.AcceptanceReadiness) {
	case "UNAVAILABLE", "ALIGNED":
		return 0
	case "READY_PENDING":
		return 1
	case "BLOCKED":
		count := 0
		if !basis.AcceptanceChecklist.ContradictionClear {
			count++
		}
		if !basis.AcceptanceChecklist.MemoryAttentionClear {
			count++
		}
		return count
	case "OBSERVING":
		return 0
	case "WARMING":
		return 5
	default:
		return 6
	}
}

func TestExpectedHandlerAcceptanceGateReasonKeepsObservingCandidatePrimaryWhileCooldownActiveBeforeStreakStarts(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		CurrentMode:          "STABILIZE",
		CandidateMode:        "SYNERGY_SEEKING",
		CandidateStreak:      0,
		CooldownActive:       true,
		BlockingReasons:      []string{"cooldown_active"},
		AcceptanceReadiness:  "OBSERVING",
		AcceptanceGateReason: "OBSERVING_CANDIDATE",
	}
	if got := expectedHandlerAcceptanceGateReason(basis); got != "OBSERVING_CANDIDATE" {
		t.Fatalf("expected handler cooldown gate helper to keep observing candidate primary before streak starts even under cooldown, got %q", got)
	}
}

func TestExpectedHandlerCompositeBlockerReasonsStayComposite(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		CurrentMode:         "STABILIZE",
		CandidateMode:       "SYNERGY_SEEKING",
		CandidateStreak:     2,
		CooldownActive:      true,
		BlockingReasons:     []string{"contradictions_present", "memory_attention_active"},
		AcceptanceReadiness: "BLOCKED",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{ContradictionClear: false, MemoryAttentionClear: false},
	}
	if got := expectedHandlerAcceptanceGateReason(basis); got != "CONTRADICTIONS_AND_MEMORY_ATTENTION" {
		t.Fatalf("expected handler acceptance gate helper to keep dual blockers composite, got %q", got)
	}
	if got := expectedHandlerCooldownReason(basis); got != "contradictions_and_memory_attention" {
		t.Fatalf("expected handler cooldown reason helper to keep dual blockers composite, got %q", got)
	}
}

func TestExpectedHandlerAcceptanceMissingRequirementsSuppressesCooldownDebtWhileObservingBeforeStreakStarts(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness: "OBSERVING",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			CandidatePresent:     true,
			CandidateDiverges:    true,
			HysteresisSatisfied:  false,
			CooldownClear:        false,
			ContradictionClear:   true,
			MemoryAttentionClear: true,
		},
	}
	if got := expectedHandlerAcceptanceMissingRequirements(basis); got != nil {
		t.Fatalf("expected handler missing-requirements helper to keep hysteresis visible-but-not-active while observing before streak starts, got %+v", got)
	}
}

func TestExpectedHandlerAcceptanceMissingRequirementsSuppressesCooldownDebtWhileWarmingBeforeReadyWindow(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness: "WARMING",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			CandidatePresent:     true,
			CandidateDiverges:    true,
			HysteresisSatisfied:  false,
			CooldownClear:        false,
			ContradictionClear:   true,
			MemoryAttentionClear: true,
		},
	}
	if got := expectedHandlerAcceptanceMissingRequirements(basis); !reflect.DeepEqual(got, []string{"hysteresis_satisfied"}) {
		t.Fatalf("expected handler missing-requirements helper to suppress cooldown debt while warming before ready window opens, got %+v", got)
	}
}

func TestCooldownAcceptanceChecklistHelpersKeepObservingCountsDeferredBeforeStreakStarts(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness: "OBSERVING",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			CandidatePresent:     true,
			CandidateDiverges:    true,
			HysteresisSatisfied:  false,
			CooldownClear:        false,
			ContradictionClear:   true,
			MemoryAttentionClear: true,
		},
	}
	if got := cooldownAcceptanceChecklistClearCountForTest(basis); got != 0 {
		t.Fatalf("expected handler observing checklist clear count to stay deferred before streak start, got %d", got)
	}
	if got := cooldownAcceptanceChecklistRequirementCountForTest(basis); got != 0 {
		t.Fatalf("expected handler observing checklist requirement count to stay deferred before streak start, got %d", got)
	}
}

func TestCooldownAcceptanceChecklistHelpersKeepBlockedCountsFocusedOnActiveContradictionBlocker(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness: "BLOCKED",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			ContradictionClear:   false,
			MemoryAttentionClear: true,
		},
	}
	if got := cooldownAcceptanceChecklistClearCountForTest(basis); got != 0 {
		t.Fatalf("expected handler blocked checklist clear count to stay at 0 for contradiction-only blocker, got %d", got)
	}
	if got := cooldownAcceptanceChecklistRequirementCountForTest(basis); got != 1 {
		t.Fatalf("expected handler blocked checklist requirement count to stay at 1 for contradiction-only blocker, got %d", got)
	}
}

func TestCooldownAcceptanceChecklistHelpersKeepBlockedCountsFocusedOnActiveMemoryBlocker(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness: "BLOCKED",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			ContradictionClear:   true,
			MemoryAttentionClear: false,
		},
	}
	if got := cooldownAcceptanceChecklistClearCountForTest(basis); got != 0 {
		t.Fatalf("expected handler blocked checklist clear count to stay at 0 for memory-only blocker, got %d", got)
	}
	if got := cooldownAcceptanceChecklistRequirementCountForTest(basis); got != 1 {
		t.Fatalf("expected handler blocked checklist requirement count to stay at 1 for memory-only blocker, got %d", got)
	}
}

func TestCooldownAcceptanceChecklistHelpersKeepReadyPendingCountsFocusedOnCooldownDebt(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness: "READY_PENDING",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			CandidatePresent:     true,
			CandidateDiverges:    true,
			HysteresisSatisfied:  true,
			CooldownClear:        false,
			ContradictionClear:   true,
			MemoryAttentionClear: true,
		},
	}
	if got := cooldownAcceptanceChecklistClearCountForTest(basis); got != 0 {
		t.Fatalf("expected handler ready-window checklist clear count to stay active-debt-scoped at 0, got %d", got)
	}
	if got := cooldownAcceptanceChecklistRequirementCountForTest(basis); got != 1 {
		t.Fatalf("expected handler ready-window checklist requirement count to stay active-debt-scoped at 1, got %d", got)
	}
}

func TestCooldownAcceptanceChecklistHelpersKeepReadyWindowClearCountsFocusedOnCooldownDebt(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness: "READY_PENDING",
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			CandidatePresent:     true,
			CandidateDiverges:    true,
			HysteresisSatisfied:  true,
			CooldownClear:        true,
			ContradictionClear:   true,
			MemoryAttentionClear: true,
		},
	}
	if got := cooldownAcceptanceChecklistClearCountForTest(basis); got != 1 {
		t.Fatalf("expected handler ready-window checklist clear count to stay active-debt-scoped at 1, got %d", got)
	}
	if got := cooldownAcceptanceChecklistRequirementCountForTest(basis); got != 1 {
		t.Fatalf("expected handler ready-window checklist requirement count to stay active-debt-scoped at 1, got %d", got)
	}
}

func TestExpectedHandlerAcceptanceProgressBandUsesReadinessScopedReadyWindowCounts(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness:           "READY_PENDING",
		AcceptanceMissingRequirements: []string{"cooldown_clear"},
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			CandidatePresent:     true,
			CandidateDiverges:    true,
			HysteresisSatisfied:  true,
			CooldownClear:        false,
			ContradictionClear:   true,
			MemoryAttentionClear: true,
		},
	}
	if got := expectedHandlerAcceptanceProgressBand(basis); got != "READY_WINDOW_PENDING" {
		t.Fatalf("expected handler acceptance progress helper to stay on readiness-scoped ready-window pending semantics, got %q", got)
	}
}

func TestExpectedHandlerAcceptanceProgressBandUsesReadinessScopedWarmingCounts(t *testing.T) {
	t.Parallel()

	basis := &sqlite.UnifiedControlCooldownBasis{
		AcceptanceReadiness:           "WARMING",
		AcceptanceMissingRequirements: []string{"hysteresis_satisfied"},
		AcceptanceChecklist: &sqlite.UnifiedControlAcceptanceChecklist{
			CandidatePresent:     true,
			CandidateDiverges:    true,
			HysteresisSatisfied:  false,
			CooldownClear:        false,
			ContradictionClear:   true,
			MemoryAttentionClear: true,
		},
	}
	if got := expectedHandlerAcceptanceProgressBand(basis); got != "PARTIAL" {
		t.Fatalf("expected handler acceptance progress helper to stay aligned with readiness-scoped warming semantics, got %q", got)
	}
}

func TestWorkspaceInstrumentationUnifiedControlSnapshotRPCSurfaceAndReplay(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)

	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for unified snapshot handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}
	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-handler-unified-control-snapshot",
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
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-handler-unified-snapshot-lineage",
		WorkspaceID:       scenario.workspaceID,
		EventType:         "tests.handler.unified.snapshot_lineage",
		EntityType:        "test_scope",
		EntityID:          clusterID,
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-handler-unified-snapshot",
		ProvenanceGroupID: "PG-handler-unified-snapshot",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record governed hint lineage event: %v", err)
	}

	rawReport, err := json.Marshal(workspaceInstrumentationUnifiedControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		AgentID:        "agent-a",
		TaskID:         scenario.primaryTaskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.runbookDocKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
	})
	if err != nil {
		t.Fatalf("marshal unified control report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationUnifiedControlReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationUnifiedControlReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected unified control report result type %T", result)
	}
	baselineReport, ok := reportPayload["report"].(sqlite.UnifiedControlReport)
	if !ok {
		t.Fatalf("unexpected unified control report payload type %T", reportPayload["report"])
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawSnapshot, err := json.Marshal(workspaceInstrumentationUnifiedControlSnapshotParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		AgentID:        "agent-a",
		TaskID:         scenario.primaryTaskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.runbookDocKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
		ActorID:        "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal unified control snapshot params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationUnifiedControlSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationUnifiedControlSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected unified control snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.UnifiedControlReport)
	if !ok {
		t.Fatalf("unexpected unified control snapshot report payload type %T", snapshotPayload["report"])
	}
	if snapshotPayload["workspace_id"] != scenario.workspaceID || snapshotPayload["summary"] != snapshotReport.Summary {
		t.Fatalf("expected unified control snapshot envelope to mirror workspace and summary, got %+v", snapshotPayload)
	}
	if advisoryControls, ok := snapshotPayload["advisory_controls"].(sqlite.ControlSuggestedControls); !ok || advisoryControls != snapshotReport.AdvisoryControls {
		t.Fatalf("expected unified control snapshot envelope advisory_controls mirror, got %+v report=%+v", snapshotPayload["advisory_controls"], snapshotReport.AdvisoryControls)
	}
	if candidateControls, ok := snapshotPayload["candidate_controls"].(sqlite.ControlSuggestedControls); !ok || candidateControls != snapshotReport.CandidateControls {
		t.Fatalf("expected unified control snapshot envelope candidate_controls mirror, got %+v report=%+v", snapshotPayload["candidate_controls"], snapshotReport.CandidateControls)
	}
	if effectiveControls, ok := snapshotPayload["effective_controls"].(sqlite.ControlSuggestedControls); !ok || effectiveControls != snapshotReport.EffectiveControls {
		t.Fatalf("expected unified control snapshot envelope effective_controls mirror, got %+v report=%+v", snapshotPayload["effective_controls"], snapshotReport.EffectiveControls)
	}
	if audit, ok := snapshotPayload["effective_controls_audit"].(*sqlite.UnifiedControlEffectiveControlsAudit); !ok || !reflect.DeepEqual(audit, snapshotReport.EffectiveControlsAudit) {
		t.Fatalf("expected unified control snapshot envelope effective_controls_audit mirror, got %+v report=%+v", snapshotPayload["effective_controls_audit"], snapshotReport.EffectiveControlsAudit)
	}
	if !snapshotReport.Resolved || snapshotReport.ProtoClusterID != clusterID {
		t.Fatalf("expected resolved unified control snapshot report for %s, got %+v", clusterID, snapshotReport)
	}
	if snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected unified control snapshot report generated_at %q to mirror authority reference_at %q", snapshotReport.GeneratedAt, snapshotReport.TimeAuthority.ReferenceAt)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected unified control snapshot event payload type %T", snapshotPayload["event"])
	}
	if event.EventType != "cluster.unified_control_advisory_snapshot" || event.EntityType != "instrumentation_unified_control" || event.EntityID != clusterID {
		t.Fatalf("unexpected unified control snapshot event %+v", event)
	}
	var eventPayload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &eventPayload); err != nil {
		t.Fatalf("decode unified control snapshot event payload: %v", err)
	}
	if eventPayload["effective_control_basis_field_count"] == nil || eventPayload["effective_control_basis_changed_count"] == nil {
		t.Fatalf("expected unified control snapshot event payload to mirror basis summary counts, got %+v", eventPayload)
	}
	if eventPayload["contradiction_count"] == nil || eventPayload["hard_safety_contradiction_count"] == nil {
		t.Fatalf("expected unified control snapshot event payload to mirror contradiction summary counts, got %+v", eventPayload)
	}
	if eventPayload["audit_applied_entry_count"] == nil || eventPayload["audit_suppressed_entry_count"] == nil || eventPayload["audit_coverage_full_applied_trace_entry_count"] == nil || eventPayload["audit_coverage_full_suppressed_trace_entry_count"] == nil {
		t.Fatalf("expected unified control snapshot event payload to mirror audit summary/coverage counts, got %+v", eventPayload)
	}
	basisSummary := snapshotReport.EffectiveControlBasisSummary
	if basisSummary == nil {
		t.Fatalf("expected snapshot report basis summary, got nil")
	}
	if eventPayload["effective_control_basis_field_count"] != float64(basisSummary.FieldCount) ||
		eventPayload["effective_control_basis_changed_count"] != float64(basisSummary.ChangedFieldCount) ||
		eventPayload["effective_control_basis_fields_with_action_trace_count"] != float64(basisSummary.FieldsWithActionTraceCount) ||
		eventPayload["effective_control_basis_fields_with_hint_trace_count"] != float64(basisSummary.FieldsWithHintTraceCount) {
		t.Fatalf("expected unified control snapshot event payload basis summary counts to mirror snapshot report, got payload=%+v summary=%+v", eventPayload, basisSummary)
	}
	contradictionSummary := snapshotReport.ContradictionSummary
	if contradictionSummary == nil {
		if eventPayload["contradiction_count"] != float64(0) ||
			eventPayload["hard_safety_contradiction_count"] != float64(0) ||
			eventPayload["memory_safety_override_contradiction_count"] != float64(0) {
			t.Fatalf("expected unified control snapshot event payload contradiction summary counts to stay zero when snapshot report summary is nil, got payload=%+v", eventPayload)
		}
	} else if eventPayload["contradiction_count"] != float64(contradictionSummary.TotalCount) ||
		eventPayload["hard_safety_contradiction_count"] != float64(contradictionSummary.HardSafetyClampCount) ||
		eventPayload["memory_safety_override_contradiction_count"] != float64(contradictionSummary.MemorySafetyOverrideCount) {
		t.Fatalf("expected unified control snapshot event payload contradiction summary counts to mirror snapshot report, got payload=%+v summary=%+v", eventPayload, contradictionSummary)
	}
	auditSummary := snapshotReport.AuditSummary
	if auditSummary == nil {
		if eventPayload["audit_applied_entry_count"] != float64(0) ||
			eventPayload["audit_hint_backed_action_count"] != float64(0) ||
			eventPayload["audit_delta_bearing_action_count"] != float64(0) ||
			eventPayload["audit_suppressed_entry_count"] != float64(0) ||
			eventPayload["audit_suppressed_entries_with_action_ref_count"] != float64(0) {
			t.Fatalf("expected unified control snapshot event payload audit summary counts to stay zero when snapshot report summary is nil, got payload=%+v", eventPayload)
		}
	} else if eventPayload["audit_applied_entry_count"] != float64(auditSummary.AppliedEntryCount) ||
		eventPayload["audit_hint_backed_action_count"] != float64(auditSummary.HintBackedActionCount) ||
		eventPayload["audit_delta_bearing_action_count"] != float64(auditSummary.DeltaBearingActionCount) ||
		eventPayload["audit_suppressed_entry_count"] != float64(auditSummary.SuppressedEntryCount) ||
		eventPayload["audit_suppressed_entries_with_action_ref_count"] != float64(auditSummary.SuppressedEntriesWithActionRef) {
		t.Fatalf("expected unified control snapshot event payload audit summary counts to mirror snapshot report, got payload=%+v summary=%+v", eventPayload, auditSummary)
	}
	auditCoverage := snapshotReport.AuditCoverage
	if auditCoverage == nil {
		if eventPayload["audit_coverage_applied_entries_with_source_kinds"] != float64(0) ||
			eventPayload["audit_coverage_applied_entries_with_hint_refs"] != float64(0) ||
			eventPayload["audit_coverage_applied_entries_with_delta_fields"] != float64(0) ||
			eventPayload["audit_coverage_applied_entries_with_summary"] != float64(0) ||
			eventPayload["audit_coverage_full_applied_trace_entry_count"] != float64(0) ||
			eventPayload["audit_coverage_suppressed_entries_with_source_kind"] != float64(0) ||
			eventPayload["audit_coverage_suppressed_entries_with_action_ref"] != float64(0) ||
			eventPayload["audit_coverage_suppressed_entries_with_reason"] != float64(0) ||
			eventPayload["audit_coverage_suppressed_entries_with_summary"] != float64(0) ||
			eventPayload["audit_coverage_full_suppressed_trace_entry_count"] != float64(0) {
			t.Fatalf("expected unified control snapshot event payload audit coverage counts to stay zero when snapshot report coverage is nil, got payload=%+v", eventPayload)
		}
	} else if eventPayload["audit_coverage_applied_entries_with_source_kinds"] != float64(auditCoverage.AppliedEntriesWithSourceKinds) ||
		eventPayload["audit_coverage_applied_entries_with_hint_refs"] != float64(auditCoverage.AppliedEntriesWithHintRefs) ||
		eventPayload["audit_coverage_applied_entries_with_delta_fields"] != float64(auditCoverage.AppliedEntriesWithDeltaFields) ||
		eventPayload["audit_coverage_applied_entries_with_summary"] != float64(auditCoverage.AppliedEntriesWithSummary) ||
		eventPayload["audit_coverage_full_applied_trace_entry_count"] != float64(auditCoverage.FullAppliedTraceEntryCount) ||
		eventPayload["audit_coverage_suppressed_entries_with_source_kind"] != float64(auditCoverage.SuppressedEntriesWithSourceKind) ||
		eventPayload["audit_coverage_suppressed_entries_with_action_ref"] != float64(auditCoverage.SuppressedEntriesWithActionRef) ||
		eventPayload["audit_coverage_suppressed_entries_with_reason"] != float64(auditCoverage.SuppressedEntriesWithReason) ||
		eventPayload["audit_coverage_suppressed_entries_with_summary"] != float64(auditCoverage.SuppressedEntriesWithSummary) ||
		eventPayload["audit_coverage_full_suppressed_trace_entry_count"] != float64(auditCoverage.FullSuppressedTraceEntryCount) {
		t.Fatalf("expected unified control snapshot event payload audit coverage counts to mirror snapshot report, got payload=%+v coverage=%+v", eventPayload, auditCoverage)
	}
	if eventPayload["cooldown_stage"] == nil || eventPayload["cooldown_acceptance_readiness"] == nil || eventPayload["cooldown_acceptance_gate_reason"] == nil || eventPayload["cooldown_acceptance_clear_count"] == nil || eventPayload["cooldown_acceptance_requirement_count"] == nil || eventPayload["cooldown_acceptance_missing_requirement_count"] == nil || eventPayload["cooldown_acceptance_progress_band"] == nil || eventPayload["cooldown_candidate_streak"] == nil || eventPayload["cooldown_required_streak"] == nil || eventPayload["cooldown_remaining_streak"] == nil || eventPayload["cooldown_ready_to_stabilize"] == nil || eventPayload["cooldown_blocking_reason_count"] == nil || eventPayload["cooldown_reason"] == nil {
		t.Fatalf("expected unified control snapshot event payload to mirror cooldown basis fields, got %+v", eventPayload)
	}
	if _, ok := eventPayload["cooldown_acceptance_missing_requirements"]; !ok {
		t.Fatalf("expected unified control snapshot event payload missing-requirements array mirror key, got %+v", eventPayload)
	}
	if _, ok := eventPayload["cooldown_blocking_reasons"]; !ok {
		t.Fatalf("expected unified control snapshot event payload blocking-reasons array mirror key, got %+v", eventPayload)
	}
	cooldownBasis := snapshotReport.CooldownBasis
	if cooldownBasis == nil {
		t.Fatalf("expected snapshot report cooldown basis, got nil")
	}
	if eventPayload["cooldown_current_mode"] != cooldownBasis.CurrentMode ||
		eventPayload["cooldown_candidate_mode"] != cooldownBasis.CandidateMode ||
		eventPayload["cooldown_stage"] != cooldownBasis.Stage ||
		eventPayload["cooldown_acceptance_readiness"] != cooldownBasis.AcceptanceReadiness ||
		eventPayload["cooldown_acceptance_gate_reason"] != cooldownBasis.AcceptanceGateReason ||
		eventPayload["cooldown_acceptance_progress_band"] != cooldownBasis.AcceptanceProgressBand ||
		eventPayload["cooldown_reason"] != cooldownBasis.Reason {
		t.Fatalf("expected unified control snapshot event payload strings to mirror snapshot report cooldown basis, got payload=%+v basis=%+v", eventPayload, cooldownBasis)
	}
	if eventPayload["cooldown_acceptance_clear_count"] != float64(cooldownAcceptanceChecklistClearCountForTest(cooldownBasis)) ||
		eventPayload["cooldown_acceptance_requirement_count"] != float64(cooldownAcceptanceChecklistRequirementCountForTest(cooldownBasis)) ||
		eventPayload["cooldown_acceptance_missing_requirement_count"] != float64(len(cooldownBasis.AcceptanceMissingRequirements)) ||
		eventPayload["cooldown_candidate_streak"] != float64(cooldownBasis.CandidateStreak) ||
		eventPayload["cooldown_required_streak"] != float64(cooldownBasis.RequiredStreak) ||
		eventPayload["cooldown_remaining_streak"] != float64(cooldownBasis.RemainingStreak) ||
		eventPayload["cooldown_blocking_reason_count"] != float64(cooldownBasis.BlockingReasonCount) {
		t.Fatalf("expected unified control snapshot event payload counts to mirror snapshot report cooldown basis, got payload=%+v basis=%+v", eventPayload, cooldownBasis)
	}
	gotMissingRequirements := []string(nil)
	if eventPayload["cooldown_acceptance_missing_requirements"] != nil {
		missingRequirements, ok := eventPayload["cooldown_acceptance_missing_requirements"].([]any)
		if !ok {
			t.Fatalf("expected unified control snapshot event payload missing-requirements array mirror, got %+v", eventPayload["cooldown_acceptance_missing_requirements"])
		}
		gotMissingRequirements = make([]string, 0, len(missingRequirements))
		for _, item := range missingRequirements {
			text, ok := item.(string)
			if !ok {
				t.Fatalf("expected unified control snapshot event payload missing-requirements entries to stay strings, got %+v", missingRequirements)
			}
			gotMissingRequirements = append(gotMissingRequirements, text)
		}
	}
	if !reflect.DeepEqual(gotMissingRequirements, cooldownBasis.AcceptanceMissingRequirements) {
		t.Fatalf("expected unified control snapshot event payload missing-requirements array to mirror snapshot report cooldown basis, got payload=%+v basis=%+v", gotMissingRequirements, cooldownBasis.AcceptanceMissingRequirements)
	}
	gotBlockingReasons := []string(nil)
	if eventPayload["cooldown_blocking_reasons"] != nil {
		blockingReasons, ok := eventPayload["cooldown_blocking_reasons"].([]any)
		if !ok {
			t.Fatalf("expected unified control snapshot event payload blocking-reasons array mirror, got %+v", eventPayload["cooldown_blocking_reasons"])
		}
		gotBlockingReasons = make([]string, 0, len(blockingReasons))
		for _, item := range blockingReasons {
			text, ok := item.(string)
			if !ok {
				t.Fatalf("expected unified control snapshot event payload blocking-reasons entries to stay strings, got %+v", blockingReasons)
			}
			gotBlockingReasons = append(gotBlockingReasons, text)
		}
	}
	if !reflect.DeepEqual(gotBlockingReasons, cooldownBasis.BlockingReasons) {
		t.Fatalf("expected unified control snapshot event payload blocking-reasons array to mirror snapshot report cooldown basis, got payload=%+v basis=%+v", gotBlockingReasons, cooldownBasis.BlockingReasons)
	}
	if eventPayload["cooldown_ready_to_stabilize"] != cooldownBasis.ReadyToStabilize || eventPayload["cooldown_transitioning"] != cooldownBasis.Transitioning {
		t.Fatalf("expected unified control snapshot event payload booleans to mirror snapshot report cooldown basis, got payload=%+v basis=%+v", eventPayload, cooldownBasis)
	}

	liveEvent := nextEvent(t, ch)
	if liveEvent.Type != "cluster.unified_control_advisory_snapshot" {
		t.Fatalf("expected cluster.unified_control_advisory_snapshot live event, got %+v", liveEvent)
	}
	assertValidEventTimestamp(t, liveEvent.Timestamp)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
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

	rawReplay, err := json.Marshal(workspaceEventsReplayParams{
		WorkspaceID:   scenario.workspaceID,
		IncludeEvents: true,
		Limit:         50,
	})
	if err != nil {
		t.Fatalf("marshal replay params: %v", err)
	}
	result, rpcErr = h.workspaceEventsReplay(ctx, rawReplay)
	if rpcErr != nil {
		t.Fatalf("workspaceEventsReplay rpc error: %+v", rpcErr)
	}
	replayPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected replay result type %T", result)
	}
	replay, ok := replayPayload["report"].(sqlite.RuntimeReplayReport)
	if !ok {
		t.Fatalf("unexpected replay payload type %T", replayPayload["report"])
	}
	snapshotCount := 0
	for _, item := range replay.Events {
		if item.EventType == "cluster.unified_control_advisory_snapshot" {
			snapshotCount++
		}
	}
	if snapshotCount != 1 {
		t.Fatalf("expected replay to include one unified control snapshot event, got %d from %+v", snapshotCount, replay.Events)
	}

	result, rpcErr = h.workspaceInstrumentationUnifiedControlReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationUnifiedControlReport after snapshot rpc error: %+v", rpcErr)
	}
	reportPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected post-snapshot unified control report result type %T", result)
	}
	afterReport, ok := reportPayload["report"].(sqlite.UnifiedControlReport)
	if !ok {
		t.Fatalf("unexpected post-snapshot unified control report payload type %T", reportPayload["report"])
	}
	if !reflect.DeepEqual(afterReport.EffectiveControls, baselineReport.EffectiveControls) {
		t.Fatalf("expected unified control snapshot replay not to drift effective controls, baseline=%+v after=%+v", baselineReport.EffectiveControls, afterReport.EffectiveControls)
	}
	if !reflect.DeepEqual(afterReport.EffectiveControlBasis, baselineReport.EffectiveControlBasis) {
		t.Fatalf("expected unified control snapshot replay not to drift effective-control basis, baseline=%+v after=%+v", baselineReport.EffectiveControlBasis, afterReport.EffectiveControlBasis)
	}
	if !reflect.DeepEqual(afterReport.EffectiveControlBasisSummary, baselineReport.EffectiveControlBasisSummary) {
		t.Fatalf("expected unified control snapshot replay not to drift effective-control basis summary, baseline=%+v after=%+v", baselineReport.EffectiveControlBasisSummary, afterReport.EffectiveControlBasisSummary)
	}
	if !reflect.DeepEqual(afterReport.ContradictionSummary, baselineReport.ContradictionSummary) {
		t.Fatalf("expected unified control snapshot replay not to drift contradiction summary, baseline=%+v after=%+v", baselineReport.ContradictionSummary, afterReport.ContradictionSummary)
	}
	if !reflect.DeepEqual(afterReport.CooldownBasis, baselineReport.CooldownBasis) {
		t.Fatalf("expected unified control snapshot replay not to drift cooldown basis, baseline=%+v after=%+v", baselineReport.CooldownBasis, afterReport.CooldownBasis)
	}
	if !reflect.DeepEqual(afterReport.AppliedActions, baselineReport.AppliedActions) {
		t.Fatalf("expected unified control snapshot replay not to drift applied actions, baseline=%+v after=%+v", baselineReport.AppliedActions, afterReport.AppliedActions)
	}
	if afterReport.MemoryCoherenceBand != baselineReport.MemoryCoherenceBand || afterReport.RSPRiskBand != baselineReport.RSPRiskBand || afterReport.CooldownActive != baselineReport.CooldownActive {
		t.Fatalf("expected unified control snapshot replay not to drift bounded risk/coherence state, baseline=%+v after=%+v", baselineReport, afterReport)
	}
}

func TestWorkspaceInstrumentationUnifiedControlReportKeepsPendingEffectiveControlsInspectabilityOnly(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	clusterID := seedConfirmedControlStateRPCScenario(t, ctx, store, scenario)

	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for pending effective-controls handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}
	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-handler-unified-control-pending",
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

	filter := sqlite.UnifiedControlReportFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		AgentID:        "agent-a",
		TaskID:         scenario.primaryTaskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.runbookDocKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
	}
	baseline, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build baseline unified control report: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	if reflect.DeepEqual(baseline.AdvisoryControls, baseline.CandidateControls) {
		t.Fatalf("expected baseline arbitration to diverge advisory and candidate controls for pending mismatch coverage, got advisory=%+v candidate=%+v", baseline.AdvisoryControls, baseline.CandidateControls)
	}
	pendingFanout := baseline.CandidateControls.FanoutCap - 1
	if pendingFanout < 1 {
		pendingFanout = 1
	}
	pendingContextCap := baseline.CandidateControls.ContextCap - 1
	if pendingContextCap < 1 {
		pendingContextCap = 1
	}

	pendingControls := sqlite.ControlSuggestedControls{
		FanoutCap:      pendingFanout,
		ReviewDepth:    baseline.CandidateControls.ReviewDepth + 1,
		ContextCap:     pendingContextCap,
		BridgeQuota:    baseline.CandidateControls.BridgeQuota,
		MergeThreshold: baseline.CandidateControls.MergeThreshold + 1,
		PriorityFocus:  "pending-effective-handler",
	}
	if reflect.DeepEqual(pendingControls, baseline.CandidateControls) {
		t.Fatalf("expected pending effective-controls fixture to diverge from candidate controls, got baseline=%+v pending=%+v", baseline.CandidateControls, pendingControls)
	}
	if _, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             3,
		TTLSeconds:        600,
		ControlMode:       baseline.ControlMode,
		CandidateMode:     baseline.CandidateMode,
		CandidateControls: baseline.CandidateControls,
		AdvisoryControls:  baseline.AdvisoryControls,
		EffectiveControls: pendingControls,
		ResolvedFrom:      "handler_pending_test",
		MatchScore:        baseline.MatchScore,
		BasisSummary:      "pending effective-controls must remain inspectability-only on handler read path",
		GeneratedAt:       "2099-01-01T00:00:00Z",
		ActorID:           "tests",
	}); err != nil {
		t.Fatalf("persist pending effective controls: %v", err)
	}

	beforeEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       200,
	})
	if err != nil {
		t.Fatalf("list runtime events before pending handler read: %v", err)
	}
	beforeControlRequests := countServerRuntimeEventsByType(beforeEvents, "control.command.requested")

	raw, err := json.Marshal(workspaceInstrumentationUnifiedControlParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		AgentID:        "agent-a",
		TaskID:         scenario.primaryTaskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.runbookDocKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
	})
	if err != nil {
		t.Fatalf("marshal unified control params: %v", err)
	}

	result, rpcErr := h.workspaceInstrumentationUnifiedControlReport(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationUnifiedControlReport rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected unified control result type %T", result)
	}
	report, ok := payload["report"].(sqlite.UnifiedControlReport)
	if !ok {
		t.Fatalf("unexpected unified control payload type %T", payload["report"])
	}
	if !report.AdvisoryOnly || payload["advisory_only"] != true {
		t.Fatalf("expected pending effective-controls read path to remain advisory-only, payload=%+v report=%+v", payload, report)
	}
	if !strings.Contains(report.Summary, "advisory unified control report") || strings.Contains(report.Summary, "effective unified control report") {
		t.Fatalf("expected pending effective-controls handler report to keep advisory wording, got %q", report.Summary)
	}
	if report.EffectiveControlsAudit == nil || !report.EffectiveControlsAudit.Found || report.EffectiveControlsAudit.Live || report.EffectiveControlsAudit.Expired || !report.EffectiveControlsAudit.Pending {
		t.Fatalf("expected pending effective-controls audit to stay visible and pending, got %+v", report.EffectiveControlsAudit)
	}
	if report.EffectiveControlsAudit.ScopeSource != "proto_cluster" || report.EffectiveControlsAudit.TemporalContract == nil || report.EffectiveControlsAudit.TemporalContract.State != "PENDING" {
		t.Fatalf("expected pending effective-controls audit to preserve proto-cluster pending contract, got %+v", report.EffectiveControlsAudit)
	}
	if report.EffectiveControls != report.CandidateControls {
		t.Fatalf("expected pending effective controls not to present as live-applied authority, got candidate=%+v effective=%+v", report.CandidateControls, report.EffectiveControls)
	}
	if reflect.DeepEqual(report.AdvisoryControls, report.EffectiveControls) {
		t.Fatalf("expected pending effective-controls handler read path to keep advisory and effective surfaces distinct, got %+v", report)
	}
	if reflect.DeepEqual(report.EffectiveControls, pendingControls) {
		t.Fatalf("expected pending effective-controls record not to surface as live effective controls, got %+v", report)
	}
	if report.CooldownBasis == nil || report.CooldownBasis.AcceptanceReadiness == "" || report.CooldownBasis.AcceptanceGateReason == "" {
		t.Fatalf("expected pending effective-controls handler report to keep bounded acceptance inspectability, got %+v", report.CooldownBasis)
	}

	afterEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       200,
	})
	if err != nil {
		t.Fatalf("list runtime events after pending handler read: %v", err)
	}
	if len(afterEvents) != len(beforeEvents) {
		t.Fatalf("expected unified control report read not to append runtime events, before=%d after=%d", len(beforeEvents), len(afterEvents))
	}
	if afterControlRequests := countServerRuntimeEventsByType(afterEvents, "control.command.requested"); afterControlRequests != beforeControlRequests {
		t.Fatalf("expected unified control report read not to append control.command.requested events, before=%d after=%d", beforeControlRequests, afterControlRequests)
	}
}

func countServerRuntimeEventsByType(events []sqlite.RuntimeEventRecord, eventType string) int {
	count := 0
	for _, event := range events {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}
