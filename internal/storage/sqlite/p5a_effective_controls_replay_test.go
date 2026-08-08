package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestUnifiedControlSnapshotReplaysProposalApplicationAndSuppressionAudits(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-p5a-replay-proposal"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5A replay proposal",
		CreatedBy:   "codex",
		Status:      "active",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	arbitration := arbitrateUnifiedControl(unifiedControlArbitrationInput{
		Controls: ControlSuggestedControls{
			FanoutCap:      4,
			ReviewDepth:    1,
			ContextCap:     8,
			BridgeQuota:    1,
			MergeThreshold: 0.60,
			PriorityFocus:  "throughput",
		},
		CurrentMode:         clusterControlModeCoherence,
		CandidateMode:       clusterControlModeSynergySeeking,
		CandidateStreak:     2,
		MemoryCoherenceBand: "STABLE",
		Hints: []RSPGovernedHint{
			{
				HintID:              "hint-refresh",
				ActuationClass:      "governed_hint",
				RecommendedActions:  []string{"prefer_kernel_refresh", "tighten_context_cap"},
				TTLEpochs:           1,
				RecommendationClass: "refresh",
			},
			{
				HintID:              "hint-legacy",
				ActuationClass:      "legacy",
				RecommendedActions:  []string{"reduce_solver_fanout"},
				TTLEpochs:           1,
				RecommendationClass: "legacy",
			},
		},
		CapabilityFlags: RSPCapabilityFlags{GovernedHintsLive: true},
	})
	if len(arbitration.AppliedActions) == 0 {
		t.Fatalf("expected applied actions in arbitration trace, got %+v", arbitration)
	}
	if len(arbitration.Suppressed) == 0 || len(arbitration.SuppressedHintAudit) == 0 {
		t.Fatalf("expected suppressed hints in arbitration trace, got %+v", arbitration)
	}

	report := UnifiedControlReport{
		WorkspaceID:    workspaceID,
		ProtoClusterID: "cluster:p5a-proposal",
		Resolved:       true,
		ResolvedFrom:   "proposal_replay",
		MatchScore:     88,
		AdvisoryOnly:   true,
		ControlOrder: []string{
			"event_time_ingest",
			"rsp_epoch_update",
		},
		CapabilityFlags:   RSPCapabilityFlags{GovernedHintsLive: true},
		ControlMode:       clusterControlModeCoherence,
		CandidateMode:     clusterControlModeSynergySeeking,
		AttentionBand:     "WATCH",
		PressureScore:     47,
		SuggestedControls: arbitration.Controls,
		AdvisoryControls:  arbitration.Controls,
		CandidateControls: arbitration.Controls,
		EffectiveControls: arbitration.Controls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:       false,
			Live:        false,
			Expired:     false,
			ScopeSource: "candidate_only",
		},
		AppliedActions:      append([]string(nil), arbitration.AppliedActions...),
		AppliedActionAudit:  append([]UnifiedControlAppliedActionAudit(nil), arbitration.AppliedActionAudit...),
		SuppressedHints:     append([]string(nil), arbitration.Suppressed...),
		SuppressedHintAudit: append([]UnifiedControlSuppressedHintAudit(nil), arbitration.SuppressedHintAudit...),
		AuditSummary:        buildUnifiedControlAuditSummary(arbitration.AppliedActionAudit, arbitration.SuppressedHintAudit),
		AuditCoverage:       buildUnifiedControlAuditCoverage(arbitration.AppliedActionAudit, arbitration.SuppressedHintAudit),
		TimeAuthority: WorkspaceTimeAuthority{
			WorkspaceID:   workspaceID,
			ReferenceAt:   "2026-04-08T12:00:00Z",
			CurrentEpoch:  7,
			PolicyMode:    "steady",
			EpochAnchorAt: "2026-04-08T11:59:00Z",
		},
		GeneratedAt: "2026-04-08T12:00:00Z",
		Summary:     "proposal replay test",
	}

	event, err := store.RecordUnifiedControlSnapshot(ctx, report, UnifiedControlReportFilter{
		WorkspaceID: workspaceID,
	}, UnifiedControlSnapshotInput{
		ActorID: "auditor",
	})
	if err != nil {
		t.Fatalf("record unified control snapshot: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode snapshot payload: %v", err)
	}
	reportPayload, ok := payload["report"].(map[string]any)
	if !ok {
		t.Fatalf("expected embedded report payload, got %+v", payload["report"])
	}
	assertUnifiedControlSnapshotMirrorsReport(t, report, reportPayload, payload)
}

func TestBuildUnifiedControlReportReplaysEffectiveControlsLifecycle(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "p5a-replay-lifecycle")
	claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for p5a replay coverage",
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
		ReportID:           "memmet-p5a-replay-lifecycle",
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

	proposal, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build proposal report: %v", err)
	}
	if !proposal.AdvisoryOnly {
		t.Fatalf("expected proposal report to remain advisory-only, got %+v", proposal)
	}
	if proposal.EffectiveControlsAudit == nil || proposal.EffectiveControlsAudit.Found || proposal.EffectiveControlsAudit.ScopeSource != "candidate_only" {
		t.Fatalf("expected proposal report to expose candidate-only audit, got %+v", proposal.EffectiveControlsAudit)
	}
	if len(proposal.AppliedActionAudit) == 0 {
		t.Fatalf("expected proposal report to carry applied audits, got %+v", proposal)
	}
	proposalEvent, err := store.RecordUnifiedControlSnapshot(ctx, proposal, filter, UnifiedControlSnapshotInput{ActorID: "auditor"})
	if err != nil {
		t.Fatalf("record proposal snapshot: %v", err)
	}
	assertUnifiedControlSnapshotEventMirrorsReport(t, proposal, proposalEvent)

	persistedLiveControls := ControlSuggestedControls{
		FanoutCap:      maxInt(proposal.CandidateControls.FanoutCap-1, 1),
		ReviewDepth:    proposal.CandidateControls.ReviewDepth + 1,
		ContextCap:     maxInt(proposal.CandidateControls.ContextCap-1, 1),
		BridgeQuota:    maxInt(proposal.CandidateControls.BridgeQuota-1, 0),
		MergeThreshold: proposal.CandidateControls.MergeThreshold + 1,
		PriorityFocus:  "persisted-effective",
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    proposal.ProtoClusterID,
		Epoch:             3,
		TTLSeconds:        600,
		ControlMode:       proposal.ControlMode,
		CandidateMode:     proposal.CandidateMode,
		CandidateControls: proposal.CandidateControls,
		AdvisoryControls:  proposal.AdvisoryControls,
		EffectiveControls: persistedLiveControls,
		ResolvedFrom:      "p5a_replay_lifecycle",
		MatchScore:        proposal.MatchScore,
		BasisSummary:      "live effective-controls replay coverage",
		GeneratedAt:       proposal.GeneratedAt,
		ActorID:           "tests",
	}); err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}

	live, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build live report: %v", err)
	}
	if live.AdvisoryOnly {
		t.Fatalf("expected live report to adopt persisted effective controls, got %+v", live)
	}
	if live.EffectiveControls != persistedLiveControls {
		t.Fatalf("expected live report to surface persisted effective controls, got %+v want %+v", live.EffectiveControls, persistedLiveControls)
	}
	if live.EffectiveControlsAudit == nil || !live.EffectiveControlsAudit.Found || !live.EffectiveControlsAudit.Live || live.EffectiveControlsAudit.Expired {
		t.Fatalf("expected live audit to be found and live, got %+v", live.EffectiveControlsAudit)
	}
	if !reflect.DeepEqual(proposal.AppliedActionAudit, live.AppliedActionAudit) {
		t.Fatalf("expected proposal and live applied audits to stay replay-stable, proposal=%+v live=%+v", proposal.AppliedActionAudit, live.AppliedActionAudit)
	}
	if !reflect.DeepEqual(proposal.SuppressedHintAudit, live.SuppressedHintAudit) {
		t.Fatalf("expected proposal and live suppressed audits to stay replay-stable, proposal=%+v live=%+v", proposal.SuppressedHintAudit, live.SuppressedHintAudit)
	}
	liveEvent, err := store.RecordUnifiedControlSnapshot(ctx, live, filter, UnifiedControlSnapshotInput{ActorID: "auditor"})
	if err != nil {
		t.Fatalf("record live snapshot: %v", err)
	}
	assertUnifiedControlSnapshotEventMirrorsReport(t, live, liveEvent)

	expiredControls := ControlSuggestedControls{
		FanoutCap:      1,
		ReviewDepth:    1,
		ContextCap:     2,
		BridgeQuota:    0,
		MergeThreshold: 1,
		PriorityFocus:  "expired-effective",
	}
	expiredGeneratedAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    proposal.ProtoClusterID,
		Epoch:             4,
		TTLSeconds:        1,
		ControlMode:       proposal.ControlMode,
		CandidateMode:     proposal.CandidateMode,
		CandidateControls: proposal.CandidateControls,
		AdvisoryControls:  proposal.AdvisoryControls,
		EffectiveControls: expiredControls,
		ResolvedFrom:      "p5a_replay_lifecycle_expired",
		MatchScore:        proposal.MatchScore,
		BasisSummary:      "expired effective-controls replay coverage",
		GeneratedAt:       expiredGeneratedAt,
		ActorID:           "tests",
	}); err != nil {
		t.Fatalf("persist expired effective controls: %v", err)
	}

	expired, err := store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		t.Fatalf("build expired report: %v", err)
	}
	if !expired.AdvisoryOnly {
		t.Fatalf("expected expired report to fall back to advisory-only posture, got %+v", expired)
	}
	if expired.EffectiveControls != expired.CandidateControls {
		t.Fatalf("expected expired report to fall back to candidate controls, got candidate=%+v effective=%+v", expired.CandidateControls, expired.EffectiveControls)
	}
	if expired.EffectiveControlsAudit == nil || !expired.EffectiveControlsAudit.Found || expired.EffectiveControlsAudit.Live || !expired.EffectiveControlsAudit.Expired {
		t.Fatalf("expected expired audit to remain visible and expired, got %+v", expired.EffectiveControlsAudit)
	}
	if !reflect.DeepEqual(proposal.AppliedActionAudit, expired.AppliedActionAudit) {
		t.Fatalf("expected proposal and expired applied audits to stay replay-stable, proposal=%+v expired=%+v", proposal.AppliedActionAudit, expired.AppliedActionAudit)
	}
	if !reflect.DeepEqual(proposal.SuppressedHintAudit, expired.SuppressedHintAudit) {
		t.Fatalf("expected proposal and expired suppressed audits to stay replay-stable, proposal=%+v expired=%+v", proposal.SuppressedHintAudit, expired.SuppressedHintAudit)
	}
	expiredEvent, err := store.RecordUnifiedControlSnapshot(ctx, expired, filter, UnifiedControlSnapshotInput{ActorID: "auditor"})
	if err != nil {
		t.Fatalf("record expired snapshot: %v", err)
	}
	assertUnifiedControlSnapshotEventMirrorsReport(t, expired, expiredEvent)
}

func assertUnifiedControlSnapshotEventMirrorsReport(t *testing.T, report UnifiedControlReport, event RuntimeEventRecord) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode snapshot payload: %v", err)
	}
	reportPayload, ok := payload["report"].(map[string]any)
	if !ok {
		t.Fatalf("expected embedded report payload, got %+v", payload["report"])
	}
	assertUnifiedControlSnapshotMirrorsReport(t, report, reportPayload, payload)
}

func assertUnifiedControlSnapshotMirrorsReport(t *testing.T, report UnifiedControlReport, reportPayload map[string]any, payload map[string]any) {
	t.Helper()

	if payload["advisory_only"] != report.AdvisoryOnly {
		t.Fatalf("expected top-level advisory_only to mirror report, payload=%+v report=%+v", payload["advisory_only"], report.AdvisoryOnly)
	}
	if reportPayload["advisory_only"] != report.AdvisoryOnly {
		t.Fatalf("expected embedded report advisory_only to mirror report, payload=%+v report=%+v", reportPayload["advisory_only"], report.AdvisoryOnly)
	}
	if report.EffectiveControlsAudit == nil {
		t.Fatalf("expected report effective-controls audit to be present")
	}
	effectiveAudit, ok := reportPayload["effective_controls_audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected embedded effective-controls audit payload, got %+v", reportPayload["effective_controls_audit"])
	}
	if effectiveAudit["found"] != report.EffectiveControlsAudit.Found ||
		effectiveAudit["live"] != report.EffectiveControlsAudit.Live ||
		effectiveAudit["expired"] != report.EffectiveControlsAudit.Expired ||
		effectiveAudit["scope_source"] != report.EffectiveControlsAudit.ScopeSource {
		t.Fatalf("expected embedded effective-controls audit to mirror report, payload=%+v report=%+v", effectiveAudit, report.EffectiveControlsAudit)
	}
	if payload["effective_controls_found"] != report.EffectiveControlsAudit.Found ||
		payload["effective_controls_live"] != report.EffectiveControlsAudit.Live ||
		payload["effective_controls_expired"] != report.EffectiveControlsAudit.Expired ||
		payload["effective_controls_scope_source"] != report.EffectiveControlsAudit.ScopeSource {
		t.Fatalf("expected snapshot payload effective-controls audit fields to mirror report, payload=%+v report=%+v", payload, report.EffectiveControlsAudit)
	}
	if payload["audit_applied_entry_count"] != float64(len(report.AppliedActionAudit)) ||
		payload["audit_suppressed_entry_count"] != float64(len(report.SuppressedHintAudit)) {
		t.Fatalf("expected snapshot payload audit counters to mirror report, payload=%+v report=%+v", payload, report)
	}
	if payload["audit_coverage_full_applied_trace_entry_count"] != float64(report.AuditCoverage.FullAppliedTraceEntryCount) ||
		payload["audit_coverage_full_suppressed_trace_entry_count"] != float64(report.AuditCoverage.FullSuppressedTraceEntryCount) {
		t.Fatalf("expected snapshot payload audit coverage counters to mirror report, payload=%+v report=%+v", payload, report)
	}
	if report.AuditSummary != nil {
		if payload["audit_hint_backed_action_count"] != float64(report.AuditSummary.HintBackedActionCount) ||
			payload["audit_delta_bearing_action_count"] != float64(report.AuditSummary.DeltaBearingActionCount) {
			t.Fatalf("expected snapshot payload audit summary counters to mirror report, payload=%+v report=%+v", payload, report)
		}
	}
}
