package sqlite

import (
	"context"
	"testing"
)

func TestBuildControlReportAppliesGovernedHintOverlayFromRSPSnapshot(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedControlledTensionOnlyControlStateScenario(t, ctx, store, "control-rsp-hints")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for control report",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	_, err := store.db.ExecContext(ctx, `
		INSERT INTO runtime_events (
			event_id, workspace_id, event_type, entity_type, entity_id,
			actor_type, actor_id, payload_json, created_at
		) VALUES (
			'rtev-rsp-hints-1', ?, 'rsp.state_snapshot', 'rsp_state', ?,
			'system', 'rsp_state',
			'{"risk_score":0.81,"hidden_state":"THRASHING","coherence_band":"CRITICAL","governed_hints":[{"hint_id":"hint-routing","type":"routing_risk","scope":"cluster","entity_id":"`+scenario.clusterID+`","severity":0.88,"uncertainty":0.10,"persistence_epochs":2,"recommended_actions":["require_far_reviewer","tighten_context_cap","prefer_kernel_refresh"],"actuation_class":"governed_hint","ttl_epochs":2}]}',
			'2026-03-26T09:00:00Z'
		)`,
		scenario.workspaceID, scenario.clusterID,
	)
	if err != nil {
		t.Fatalf("insert rsp snapshot: %v", err)
	}

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report: %v", err)
	}

	cluster := requireControlClusterByID(t, report.Clusters, scenario.clusterID)
	if cluster.Signals.RSPRiskScore < 0.8 || cluster.Signals.RSPDominantState != "THRASHING" {
		t.Fatalf("expected cluster to surface latest rsp risk snapshot, got %+v", cluster.Signals)
	}
	if cluster.SuggestedControls.ReviewDepth != 3 {
		t.Fatalf("expected governed hints to raise review depth, got %+v", cluster.SuggestedControls)
	}
	if cluster.SuggestedControls.ContextCap != 4 {
		t.Fatalf("expected governed hints to tighten context cap, got %+v", cluster.SuggestedControls)
	}
	if cluster.SuggestedControls.PriorityFocus != "memory" {
		t.Fatalf("expected critical coherence hint to shift priority focus to memory, got %+v", cluster.SuggestedControls)
	}
}

func TestArbitrateUnifiedControlKeepsHardSafetyFloorsAboveHints(t *testing.T) {
	t.Parallel()

	result := arbitrateUnifiedControl(unifiedControlArbitrationInput{
		Controls: ControlSuggestedControls{
			FanoutCap:      4,
			ReviewDepth:    1,
			ContextCap:     8,
			BridgeQuota:    2,
			MergeThreshold: 0.60,
			PriorityFocus:  "coordination",
		},
		CurrentMode:         clusterControlModeSynergySeeking,
		CandidateMode:       clusterControlModeSteady,
		CandidateStreak:     1,
		MemoryCoherenceBand: "CRITICAL",
		CapabilityFlags: RSPCapabilityFlags{
			GovernedHintsLive: true,
		},
		Hints: []RSPGovernedHint{
			{
				HintID:             "hint-valid",
				ActuationClass:     "governed_hint",
				TTLEpochs:          2,
				RecommendedActions: []string{"require_far_reviewer", "prefer_kernel_refresh"},
			},
			{
				HintID:             "hint-unsupported",
				ActuationClass:     "strong_consequence",
				TTLEpochs:          2,
				RecommendedActions: []string{"tighten_context_cap"},
			},
		},
	})

	if !result.CooldownActive {
		t.Fatalf("expected non-steady reversing candidate to surface cooldown, got %+v", result)
	}
	if result.Controls.ContextCap != 4 {
		t.Fatalf("expected critical memory floor to clamp context cap, got %+v", result.Controls)
	}
	if result.Controls.ReviewDepth != 3 {
		t.Fatalf("expected far-reviewer hint to raise review depth, got %+v", result.Controls)
	}
	if result.Controls.PriorityFocus != "memory" {
		t.Fatalf("expected hard safety floor to keep memory focus, got %+v", result.Controls)
	}
	if !containsLocusString(result.Contradictions, "coherence_floor_overrides_synergy_seeking") {
		t.Fatalf("expected contradiction trace for synergy-seeking under critical coherence, got %+v", result.Contradictions)
	}
	if !containsLocusString(result.Suppressed, "hint-unsupported:unsupported_actuation_class") {
		t.Fatalf("expected unsupported actuation class to be suppressed, got %+v", result.Suppressed)
	}
	if !containsLocusString(result.AppliedActions, "memory_coherence_floor") || !containsLocusString(result.AppliedActions, "require_far_reviewer") {
		t.Fatalf("expected arbitration trace to include safety floor and valid hint action, got %+v", result.AppliedActions)
	}
}
