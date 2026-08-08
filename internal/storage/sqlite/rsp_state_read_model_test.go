package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestBuildRSPStateReportFavorsFocusedStableState(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "rsp-state-stable")

	report, err := store.BuildRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build rsp state report: %v", err)
	}
	if !report.Resolved || report.SignalType != rspStateSignalType || !report.ShadowMode {
		t.Fatalf("expected resolved shadow-only rsp state report, got %+v", report)
	}
	if report.Calibration.SchemaVersion != rspCalibrationSchemaVersion ||
		report.Calibration.Status != rspCalibrationStatusShadowOnly ||
		report.Calibration.CalibrationVersion != "state-read-model-v2" {
		t.Fatalf("expected state report to expose versioned shadow-only calibration contract, got %+v", report.Calibration)
	}
	if !containsString(report.Calibration.Unsupported, "global_root_cause_coverage") {
		t.Fatalf("expected state report calibration to keep bounded root-cause coverage explicit, got %+v", report.Calibration)
	}
	if containsString(report.Calibration.Unsupported, "root_cause_independence") {
		t.Fatalf("expected state report calibration contract to stop advertising blanket root-cause independence absence, got %+v", report.Calibration)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp state report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp state report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.HiddenState != "FOCUSED" || report.RiskBand != "LOW" {
		t.Fatalf("expected focused low-risk stable state, got %+v", report)
	}
	if report.BasisState != "FRESH" {
		t.Fatalf("expected fresh basis for healthy locus, got %+v", report)
	}
	if report.AnomalyScore >= 0.30 || len(report.HardGuards) != 0 {
		t.Fatalf("expected no strong anomalies or hard guards, got %+v", report)
	}
	if focused, ok := findRSPStatePosterior(report.StatePosterior, "FOCUSED"); !ok || focused <= 0.50 {
		t.Fatalf("expected focused posterior dominance, got %+v", report.StatePosterior)
	}
	if report.StateRationale == "" || !hasRSPStateDriverHint(report.StateDriverHints, "stable_scope") {
		t.Fatalf("expected focused state rationale and stable-scope driver hints, got %+v", report)
	}
}

func TestBuildRSPStateReportFlagsCacheDriftAndElevatedRisk(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-state-risky")

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
		ReportID:           "memmet-rsp-state-risky",
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
		EventID:           "rtev-rsp-governed-hints-lineage",
		WorkspaceID:       scenario.workspaceID,
		EventType:         "tests.rsp.governed_hint_lineage",
		EntityType:        "test_scope",
		EntityID:          clusterID,
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-rsp-governed-hints",
		ProvenanceGroupID: "PG-rsp-governed-hints",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record governed hint lineage event: %v", err)
	}

	report, err := store.BuildRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build risky rsp state report: %v", err)
	}
	if report.ControlMode == clusterControlModeSteady || report.AnomalyScore < 0.60 {
		t.Fatalf("expected non-steady risky state with strong anomalies, got %+v", report)
	}
	if report.RiskBand == "LOW" || report.HiddenState == "FOCUSED" {
		t.Fatalf("expected elevated risk posture, got %+v", report)
	}
	if !containsLocusString(report.HardGuards, "CACHE") {
		t.Fatalf("expected cache hard guard, got %+v", report.HardGuards)
	}
	if !hasRSPStateAnomaly(report.Anomalies, "cache_drift") {
		t.Fatalf("expected cache drift anomaly family, got %+v", report.Anomalies)
	}
	if report.StateRationale == "" || (!hasRSPStateDriverHint(report.StateDriverHints, "pressure") &&
		!hasRSPStateDriverHint(report.StateDriverHints, "saturation") &&
		!hasRSPStateDriverHint(report.StateDriverHints, "cache_drift") &&
		!hasRSPStateDriverHint(report.StateDriverHints, "cache_guard")) {
		t.Fatalf("expected risky state rationale to surface dominant-state drivers, got %+v", report)
	}
	flushCandidate, ok := findRSPStateLocalCandidate(report.LocalAutonomicsCandidates, "agent.control.flush_cache")
	if !ok {
		t.Fatalf("expected flush-cache local autonomics candidate, got %+v", report.LocalAutonomicsCandidates)
	}
	refreshCandidate, ok := findRSPStateLocalCandidate(report.LocalAutonomicsCandidates, "agent.control.refresh_kernel")
	if !ok {
		t.Fatalf("expected refresh-kernel local autonomics candidate, got %+v", report.LocalAutonomicsCandidates)
	}
	if flushCandidate.CapabilityEnabled || refreshCandidate.CapabilityEnabled {
		t.Fatalf("expected local autonomics to stay disabled by default, got %+v", report.LocalAutonomicsCandidates)
	}
	for _, candidate := range []RSPStateLocalAutonomicsCandidate{flushCandidate, refreshCandidate} {
		if candidate.GateOpen {
			if candidate.ObserveOnlyReason != "capability_disabled" {
				t.Fatalf("expected gate-open local autonomics candidates to surface capability_disabled without live capability, got %+v", report.LocalAutonomicsCandidates)
			}
			continue
		}
		if candidate.ObserveOnlyReason != "" {
			t.Fatalf("expected below-gate local autonomics candidates not to advertise an observe-only reason, got %+v", report.LocalAutonomicsCandidates)
		}
	}
	if !flushCandidate.BoundedLocal || !flushCandidate.Reversible || flushCandidate.SharedTruthMutation {
		t.Fatalf("expected bounded local actuator candidate contract, got %+v", flushCandidate)
	}
	if len(report.GovernedHints) != 0 {
		t.Fatalf("expected governed hints to stay disabled by default, got %+v", report.GovernedHints)
	}
}

func TestBuildRSPStateReportEmitsGovernedHintsWhenCapabilityEnabled(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-state-governed-hints")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for test",
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
		ReportID:           "memmet-rsp-state-governed",
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

	report, err := store.BuildRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build governed-hint rsp state report: %v", err)
	}
	if !report.CapabilityFlags.GovernedHintsLive {
		t.Fatalf("expected governed hint capability flag to be visible, got %+v", report.CapabilityFlags)
	}
	if len(report.GovernedHints) == 0 {
		t.Fatalf("expected governed hints when capability is enabled, got %+v", report)
	}
	routingHint, ok := findRSPGovernedHint(report.GovernedHints, "routing_risk")
	if !ok {
		t.Fatalf("expected routing_risk governed hint, got %+v", report.GovernedHints)
	}
	if routingHint.RecommendationClass != "coordination_review" {
		t.Fatalf("expected routing_risk hint to expose recommendation class, got %+v", routingHint)
	}
	if routingHint.EvidenceDiversity <= 0 || len(routingHint.EvidenceSourceKinds) == 0 {
		t.Fatalf("expected routing_risk hint to expose bounded evidence diversity and source kinds, got %+v", routingHint)
	}
	if routingHint.EvidenceDiversityBand == "" || routingHint.EvidenceSourceMix == "" || routingHint.RuntimeEventRefCount == 0 || routingHint.RuntimeLineageBasis == "" || routingHint.TTLWindowState == "" {
		t.Fatalf("expected routing_risk hint to expose diversity band, source mix, runtime-event ref count, lineage basis, and ttl window state, got %+v", routingHint)
	}
	if routingHint.Summary == "" {
		t.Fatalf("expected routing_risk hint to expose inspectable summary, got %+v", routingHint)
	}
	if report.GovernedHintSummary == nil || report.GovernedHintSummary.TotalHints != len(report.GovernedHints) || len(report.GovernedHintSummary.RecommendationClassCount) == 0 {
		t.Fatalf("expected governed hint summary on state report, got %+v", report.GovernedHintSummary)
	}
	if groundingHint, ok := findRSPGovernedHint(report.GovernedHints, "grounding_risk"); ok && groundingHint.RecommendationClass != "grounding_safety" {
		t.Fatalf("expected grounding_risk hint to expose recommendation class, got %+v", groundingHint)
	}
}

func TestBuildRSPStateReportEnablesLocalAutonomicsCandidatesWhenCapabilityEnabled(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-state-local-autonomics")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilitySafeLocalAutonomics,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable safe local autonomics for test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put safe local autonomics policy: %v", err)
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
		ReportID:           "memmet-rsp-state-local-autonomics",
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

	report, err := store.BuildRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build rsp state report: %v", err)
	}
	if !report.CapabilityFlags.SafeLocalAutonomicsLive {
		t.Fatalf("expected safe local autonomics capability to be visible, got %+v", report.CapabilityFlags)
	}
	flushCandidate, ok := findRSPStateLocalCandidate(report.LocalAutonomicsCandidates, "agent.control.flush_cache")
	if !ok {
		t.Fatalf("expected flush-cache candidate, got %+v", report.LocalAutonomicsCandidates)
	}
	refreshCandidate, ok := findRSPStateLocalCandidate(report.LocalAutonomicsCandidates, "agent.control.refresh_kernel")
	if !ok {
		t.Fatalf("expected refresh-kernel candidate, got %+v", report.LocalAutonomicsCandidates)
	}
	if !flushCandidate.CapabilityEnabled || !refreshCandidate.CapabilityEnabled {
		t.Fatalf("expected enabled safe local autonomics candidates, got %+v", report.LocalAutonomicsCandidates)
	}
	for _, candidate := range []RSPStateLocalAutonomicsCandidate{flushCandidate, refreshCandidate} {
		if candidate.GateOpen {
			if candidate.ObserveOnlyReason != "canonical_command_path_pending" {
				t.Fatalf("expected gate-open local autonomics candidates to stay observe-only until canonical command path exists, got %+v", report.LocalAutonomicsCandidates)
			}
			continue
		}
		if candidate.ObserveOnlyReason != "" {
			t.Fatalf("expected below-gate candidates not to advertise a command-path reason, got %+v", report.LocalAutonomicsCandidates)
		}
	}
	if !flushCandidate.GateOpen && !refreshCandidate.GateOpen {
		t.Fatalf("expected risky fixture to open at least one bounded local gate, got %+v", report.LocalAutonomicsCandidates)
	}
}

func TestSnapshotRSPStateReportAppendsSyntheticEvent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "rsp-state-snapshot")

	result, err := store.SnapshotRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("snapshot rsp state report: %v", err)
	}
	if result.Event.EventType != "rsp.state_snapshot" || result.Event.EntityType != "rsp_state" {
		t.Fatalf("unexpected rsp state snapshot event %+v", result.Event)
	}
	if !isSyntheticOperationalEvent(result.Event) {
		t.Fatalf("expected rsp state snapshot to stay synthetic %+v", result.Event)
	}
	if result.Report.SignalType != rspStateSignalType || result.Report.ShadowPhase != rspStateShadowPhase {
		t.Fatalf("unexpected rsp state snapshot report %+v", result.Report)
	}
	if result.Report.Calibration.SchemaVersion != rspCalibrationSchemaVersion ||
		result.Report.Calibration.CalibrationVersion != "state-read-model-v2" {
		t.Fatalf("expected state snapshot report to carry versioned calibration contract, got %+v", result.Report.Calibration)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Event.PayloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal state snapshot payload: %v", err)
	}
	calibration, ok := payload["calibration"].(map[string]any)
	if !ok {
		t.Fatalf("expected state snapshot payload to carry calibration contract, got %+v", payload)
	}
	if calibration["schema_version"] != rspCalibrationSchemaVersion ||
		calibration["calibration_version"] != "state-read-model-v2" ||
		calibration["status"] != rspCalibrationStatusShadowOnly {
		t.Fatalf("expected state snapshot payload calibration contract, got %+v", calibration)
	}
	if result.Report.TimeAuthority.WorkspaceID != scenario.workspaceID || result.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp state snapshot report to expose workspace time authority, got %+v", result.Report.TimeAuthority)
	}
	if result.Report.GeneratedAt != result.Report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp state snapshot report generated_at %q to mirror authority reference_at %q", result.Report.GeneratedAt, result.Report.TimeAuthority.ReferenceAt)
	}
}

func TestBuildMemoryShellPacketIncludesRSPStateEstimate(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "rsp-state-packet")

	packet, err := store.BuildMemoryShellPacket(ctx, MemoryPacketFilter{
		WorkspaceID:  scenario.workspaceID,
		AgentID:      scenario.agentID,
		TaskID:       scenario.taskID,
		SessionID:    scenario.sessionID,
		DocKeys:      []string{scenario.docKey},
		ArtifactRefs: []string{scenario.artifactRef},
	})
	if err != nil {
		t.Fatalf("build memory shell packet: %v", err)
	}
	if packet.StateEstimate == nil {
		t.Fatalf("expected rsp state estimate in shell packet, got %+v", packet)
	}
	if packet.StateEstimate.SignalType != rspStateSignalType || packet.StateEstimate.AgentID != scenario.agentID {
		t.Fatalf("unexpected shell packet state estimate %+v", packet.StateEstimate)
	}
	if packet.StateEstimate.HiddenState == "" {
		t.Fatalf("expected shell packet state estimate to include a hidden state %+v", packet.StateEstimate)
	}
	if packet.StateEstimate.StateRationale == "" || len(packet.StateEstimate.StateDriverHints) == 0 {
		t.Fatalf("expected shell packet state estimate to include bounded rationale hints %+v", packet.StateEstimate)
	}
}

func TestBuildRSPStateReportSurfacesHealthyExplorationViability(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "rsp-state-productive-exploration")

	upsertRSPStateControlModeFixture(t, ctx, store, scenario.workspaceID, scenario.clusterID, ClusterControlStateRecord{
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  scenario.clusterID,
		CorridorProfile: "exploration",
		CurrentMode:     clusterControlModeUnfreeze,
		CandidateMode:   clusterControlModeUnfreeze,
		CandidateStreak: 2,
		AttentionBand:   "WATCH",
		PressureScore:   6,
		TaskIDs:         []string{scenario.taskID},
		SessionIDs:      []string{scenario.sessionID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef},
		AgentIDs:        []string{scenario.agentID},
		Summary:         "bounded productive exploration fixture",
	})

	report, err := store.BuildRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build productive exploration rsp state report: %v", err)
	}
	if report.ExplorationViability != "HEALTHY" {
		t.Fatalf("expected healthy exploration viability hint, got %+v", report)
	}
	if len(report.ExplorationSuppressionReasons) != 0 {
		t.Fatalf("expected healthy exploration not to carry suppression reasons, got %+v", report)
	}
	if len(report.HardGuards) != 0 {
		t.Fatalf("expected healthy exploration to avoid hard guards, got %+v", report)
	}
}

func TestBuildRSPStateReportSuppressesExplorationViabilityUnderInstability(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-state-exploration-suppressed")

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
		ReportID:           "memmet-rsp-state-exploration-suppressed",
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
	upsertRSPStateControlModeFixture(t, ctx, store, scenario.workspaceID, clusterID, ClusterControlStateRecord{
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  clusterID,
		CorridorProfile: "exploration",
		CurrentMode:     clusterControlModeUnfreeze,
		CandidateMode:   clusterControlModeUnfreeze,
		CandidateStreak: 2,
		AttentionBand:   "HOT",
		PressureScore:   12,
		TaskIDs:         []string{scenario.taskID},
		SessionIDs:      []string{scenario.sessionID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef},
		AgentIDs:        []string{"agent-a"},
		Summary:         "exploration under unstable scope fixture",
	})

	report, err := store.BuildRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build exploration pressure rsp state report: %v", err)
	}
	if report.ExplorationViability != "SUPPRESSED" {
		t.Fatalf("expected unstable exploration to surface suppression hint, got %+v", report)
	}
	if len(report.ExplorationSuppressionReasons) == 0 {
		t.Fatalf("expected unstable exploration to surface suppression reasons, got %+v", report)
	}
	if !containsLocusString(report.ExplorationSuppressionReasons, "hard_guard:cache") &&
		!containsLocusString(report.ExplorationSuppressionReasons, "hard_guard:loop") &&
		!containsLocusString(report.ExplorationSuppressionReasons, "thrashing") &&
		!containsLocusString(report.ExplorationSuppressionReasons, "ungrounded") {
		t.Fatalf("expected unstable exploration to surface strong instability reasons, got %+v", report)
	}
}

func TestRSPGovernedHintRootCauseGroupsFallsBackToEntityScopedRuntimeLineage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-governed-root-cause"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Governed Hint Root Cause",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:           "rtev-rsp-governed-root-cause",
		WorkspaceID:       workspaceID,
		EventType:         "tests.rsp.governed_hint_lineage",
		EntityType:        "test_scope",
		EntityID:          "cluster:test-root-cause",
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-rsp-root-cause",
		ProvenanceGroupID: "PG-rsp-root-cause",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record runtime lineage event: %v", err)
	}

	groups, basis := rspGovernedHintRootCauseLineage(store, ctx, workspaceID, "cluster:test-root-cause", nil)
	if !containsLocusString(groups, "RC-rsp-root-cause") {
		t.Fatalf("expected helper to surface bounded root-cause groups from entity-scoped runtime lineage, got %+v", groups)
	}
	if basis != "ENTITY_SCOPE_FALLBACK" {
		t.Fatalf("expected entity-scope fallback lineage basis, got %s with groups %+v", basis, groups)
	}
}

func TestRSPGovernedHintEvidenceSupportLineageCollapsesSharedRootCauseFanout(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-governed-fanout-collapse"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Governed Hint Fanout Collapse",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, event := range []RuntimeEventInput{
		{
			EventID:           "rtev-rsp-fanout-cluster",
			WorkspaceID:       workspaceID,
			EventType:         "tests.rsp.fanout.cluster",
			EntityType:        "cluster_scope",
			EntityID:          "cluster:test-fanout",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-fanout-shared",
			ProvenanceGroupID: "PG-rsp-fanout-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
		{
			EventID:           "rtev-rsp-fanout-tension",
			WorkspaceID:       workspaceID,
			EventType:         "tests.rsp.fanout.tension",
			EntityType:        "tension",
			EntityID:          "tension-rsp-fanout",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-fanout-shared",
			ProvenanceGroupID: "PG-rsp-fanout-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
		{
			EventID:           "rtev-rsp-fanout-segment",
			WorkspaceID:       workspaceID,
			EventType:         "tests.rsp.fanout.segment",
			EntityType:        "segment",
			EntityID:          "segment-rsp-fanout",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-fanout-shared",
			ProvenanceGroupID: "PG-rsp-fanout-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
		{
			EventID:           "rtev-rsp-fanout-metrics",
			WorkspaceID:       workspaceID,
			EventType:         "tests.rsp.fanout.metrics",
			EntityType:        "memory_metrics_report",
			EntityID:          "memmet-rsp-fanout",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-fanout-shared",
			ProvenanceGroupID: "PG-rsp-fanout-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, event); err != nil {
			t.Fatalf("record fanout lineage event %s: %v", event.EventID, err)
		}
	}

	supportGroups, rootCauseGroups, basis := rspGovernedHintEvidenceSupportLineage(store, ctx, workspaceID, "cluster:test-fanout", []string{
		"cluster:cluster:test-fanout",
		"tension:tension-rsp-fanout",
		"segment:segment-rsp-fanout",
		"memory_metrics:memmet-rsp-fanout",
	})
	if basis != "EVIDENCE_REF_ENTITY_FALLBACK" {
		t.Fatalf("expected evidence-ref entity fallback basis, got %s with support=%+v root=%+v", basis, supportGroups, rootCauseGroups)
	}
	if len(rootCauseGroups) != 1 || !containsLocusString(rootCauseGroups, "RC-rsp-fanout-shared") {
		t.Fatalf("expected fanout evidence to collapse onto one root cause, got support=%+v root=%+v", supportGroups, rootCauseGroups)
	}
	if len(supportGroups) != 1 || !containsLocusString(supportGroups, "root_cause:RC-rsp-fanout-shared") {
		t.Fatalf("expected support groups to collapse onto the shared root cause, got %+v", supportGroups)
	}
	if diversity := rspGovernedHintEvidenceDiversity(supportGroups); diversity > 0.30 {
		t.Fatalf("expected shared-root fanout evidence diversity to stay low after collapse, got %.2f from %+v", diversity, supportGroups)
	}
}

func TestRSPGovernedHintEvidenceSupportLineageMarksMixedBasis(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-governed-mixed-lineage"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Governed Hint Mixed Lineage",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, event := range []RuntimeEventInput{
		{
			EventID:           "rtev-rsp-mixed-direct",
			WorkspaceID:       workspaceID,
			EventType:         "tests.rsp.mixed.direct",
			EntityType:        "runtime_event",
			EntityID:          "rtev-rsp-mixed-direct",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-mixed-shared",
			ProvenanceGroupID: "PG-rsp-mixed-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
		{
			EventID:           "rtev-rsp-mixed-cluster",
			WorkspaceID:       workspaceID,
			EventType:         "tests.rsp.mixed.cluster",
			EntityType:        "cluster_scope",
			EntityID:          "cluster:test-mixed",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-mixed-shared",
			ProvenanceGroupID: "PG-rsp-mixed-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, event); err != nil {
			t.Fatalf("record mixed-lineage event %s: %v", event.EventID, err)
		}
	}

	supportGroups, rootCauseGroups, basis := rspGovernedHintEvidenceSupportLineage(store, ctx, workspaceID, "cluster:test-mixed", []string{
		"runtime_event:rtev-rsp-mixed-direct",
		"cluster:cluster:test-mixed",
	})
	if basis != "MIXED_EVIDENCE_LINEAGE" {
		t.Fatalf("expected mixed evidence-lineage basis, got %s with support=%+v root=%+v", basis, supportGroups, rootCauseGroups)
	}
	if len(rootCauseGroups) != 1 || len(supportGroups) != 1 {
		t.Fatalf("expected mixed lineage to still collapse onto one shared root cause, got support=%+v root=%+v", supportGroups, rootCauseGroups)
	}
}

func TestRSPGovernedHintEvidenceMixAndBand(t *testing.T) {
	t.Parallel()

	if band := rspGovernedHintEvidenceDiversityBand(0); band != "UNKNOWN" {
		t.Fatalf("expected zero diversity to map to UNKNOWN, got %s", band)
	}
	if band := rspGovernedHintEvidenceDiversityBand(0.25); band != "LOW" {
		t.Fatalf("expected low diversity band, got %s", band)
	}
	if band := rspGovernedHintEvidenceDiversityBand(0.50); band != "MEDIUM" {
		t.Fatalf("expected medium diversity band, got %s", band)
	}
	if band := rspGovernedHintEvidenceDiversityBand(0.90); band != "HIGH" {
		t.Fatalf("expected high diversity band, got %s", band)
	}
	refs := []string{"runtime_event:rtev-1", "cluster:proto-1", "segment:doc-1"}
	if count := rspGovernedHintRuntimeEventRefCount(refs); count != 1 {
		t.Fatalf("expected one runtime-event ref, got %d", count)
	}
	if mix := rspGovernedHintEvidenceSourceMix(rspGovernedHintEvidenceSourceKinds(refs), 1); mix != "MIXED" {
		t.Fatalf("expected mixed source mix, got %s", mix)
	}
	if mix := rspGovernedHintEvidenceSourceMix([]string{"runtime_event"}, 2); mix != "RUNTIME_ONLY" {
		t.Fatalf("expected runtime-only source mix, got %s", mix)
	}
	if mix := rspGovernedHintEvidenceSourceMix([]string{"cluster", "segment"}, 0); mix != "CONTEXT_ONLY" {
		t.Fatalf("expected context-only source mix, got %s", mix)
	}
}

func TestRSPGovernedHintTTLWindowState(t *testing.T) {
	t.Parallel()

	if state := rspGovernedHintTTLWindowState(0, 2); state != "EARLY" {
		t.Fatalf("expected early ttl window state, got %s", state)
	}
	if state := rspGovernedHintTTLWindowState(1, 2); state != "ACTIVE" {
		t.Fatalf("expected active ttl window state, got %s", state)
	}
	if state := rspGovernedHintTTLWindowState(2, 2); state != "EDGE" {
		t.Fatalf("expected edge ttl window state, got %s", state)
	}
	if state := rspGovernedHintTTLWindowState(0, 0); state != "UNBOUNDED" {
		t.Fatalf("expected unbounded ttl window state, got %s", state)
	}
}

func TestRSPGovernedHintRootCauseLineagePrefersDirectRuntimeEventRefs(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-governed-lineage-direct"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Governed Hint Direct Lineage",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:           "rtev-rsp-governed-lineage-direct",
		WorkspaceID:       workspaceID,
		EventType:         "tests.rsp.governed_hint_direct_lineage",
		EntityType:        "test_scope",
		EntityID:          "cluster:test-direct-lineage",
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-rsp-direct-lineage",
		ProvenanceGroupID: "PG-rsp-direct-lineage",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record runtime lineage event: %v", err)
	}

	groups, basis := rspGovernedHintRootCauseLineage(store, ctx, workspaceID, "cluster:test-direct-lineage", []string{"runtime_event:rtev-rsp-governed-lineage-direct"})
	if !containsLocusString(groups, "RC-rsp-direct-lineage") {
		t.Fatalf("expected helper to surface direct runtime-event root-cause group, got %+v", groups)
	}
	if basis != "DIRECT_RUNTIME_EVENT_REFS" {
		t.Fatalf("expected direct runtime-event lineage basis, got %s with groups %+v", basis, groups)
	}
}

func TestBuildRSPGovernedHintSummary(t *testing.T) {
	t.Parallel()

	summary := buildRSPGovernedHintSummary([]RSPGovernedHint{
		{
			RecommendationClass: "coordination_review",
			EvidenceSourceMix:   "MIXED",
			TTLWindowState:      "ACTIVE",
			RuntimeLineageBasis: "DIRECT_RUNTIME_EVENT_REFS",
		},
		{
			RecommendationClass: "grounding_safety",
			EvidenceSourceMix:   "CONTEXT_ONLY",
			TTLWindowState:      "EDGE",
			RuntimeLineageBasis: "ENTITY_SCOPE_FALLBACK",
		},
	}, []UnifiedControlGovernedHintOutcome{
		{ArbitrationOutcome: "ADVISORY_ROUTED"},
		{ArbitrationOutcome: "OBSERVED_ONLY"},
	})
	if summary == nil || summary.TotalHints != 2 {
		t.Fatalf("expected governed-hint summary to count hints, got %+v", summary)
	}
	if summary.RecommendationClassCount["coordination_review"] != 1 || summary.RecommendationClassCount["grounding_safety"] != 1 {
		t.Fatalf("expected recommendation class counts, got %+v", summary.RecommendationClassCount)
	}
	if summary.EvidenceSourceMixCount["MIXED"] != 1 || summary.EvidenceSourceMixCount["CONTEXT_ONLY"] != 1 {
		t.Fatalf("expected source mix counts, got %+v", summary.EvidenceSourceMixCount)
	}
	if summary.TTLWindowStateCount["ACTIVE"] != 1 || summary.TTLWindowStateCount["EDGE"] != 1 {
		t.Fatalf("expected ttl window state counts, got %+v", summary.TTLWindowStateCount)
	}
	if summary.RuntimeLineageBasisCount["DIRECT_RUNTIME_EVENT_REFS"] != 1 || summary.RuntimeLineageBasisCount["ENTITY_SCOPE_FALLBACK"] != 1 {
		t.Fatalf("expected runtime lineage basis counts, got %+v", summary.RuntimeLineageBasisCount)
	}
	if summary.OutcomeCount["ADVISORY_ROUTED"] != 1 || summary.OutcomeCount["OBSERVED_ONLY"] != 1 {
		t.Fatalf("expected outcome counts, got %+v", summary.OutcomeCount)
	}
}

func findRSPStatePosterior(items []RSPStatePosterior, state string) (float64, bool) {
	for _, item := range items {
		if item.State == state {
			return item.Posterior, true
		}
	}
	return 0, false
}

func upsertRSPStateControlModeFixture(t *testing.T, ctx context.Context, store *Store, workspaceID, protoClusterID string, record ClusterControlStateRecord) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.WorkspaceID = workspaceID
	record.ProtoClusterID = protoClusterID
	record.ResolutionKind = firstNonEmpty(record.ResolutionKind, "proto_cluster")
	record.Epoch = maxInt(record.Epoch, 1)
	record.CreatedAt = firstNonEmpty(record.CreatedAt, now)
	record.UpdatedAt = firstNonEmpty(record.UpdatedAt, now)
	record.LastBasisAt = firstNonEmpty(record.LastBasisAt, now)
	record.LastTickAt = firstNonEmpty(record.LastTickAt, now)
	record.LastTransitionAt = firstNonEmpty(record.LastTransitionAt, now)

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin control-state fixture tx: %v", err)
	}
	if err := store.upsertClusterControlStateTx(ctx, tx, record); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert control-state fixture: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit control-state fixture: %v", err)
	}
}

func hasRSPStateAnomaly(items []RSPStateAnomaly, family string) bool {
	for _, item := range items {
		if item.Family == family {
			return true
		}
	}
	return false
}

func hasRSPStateDriverHint(items []RSPStateDriverHint, factor string) bool {
	for _, item := range items {
		if item.Factor == factor {
			return true
		}
	}
	return false
}

func findRSPStateLocalCandidate(items []RSPStateLocalAutonomicsCandidate, command string) (RSPStateLocalAutonomicsCandidate, bool) {
	for _, item := range items {
		if item.Command == command {
			return item, true
		}
	}
	return RSPStateLocalAutonomicsCandidate{}, false
}

func findRSPGovernedHint(items []RSPGovernedHint, hintType string) (RSPGovernedHint, bool) {
	for _, item := range items {
		if item.Type == hintType {
			return item, true
		}
	}
	return RSPGovernedHint{}, false
}
