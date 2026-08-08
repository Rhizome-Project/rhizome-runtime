package sqlite

import (
	"context"
	"testing"
)

func TestTickClusterControlStateTransitionsToStabilizeOnHighRSPRisk(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedControlledTensionOnlyControlStateScenario(t, ctx, store, "rsp-integration")

	// Emit a synthetic RSP snapshot indicating THRASHING risk
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO runtime_events (
			event_id, workspace_id, event_type, entity_type, entity_id,
			actor_type, actor_id, payload_json, created_at
		) VALUES (
			'rtev-rsp-1', ?, 'rsp.state_snapshot', 'rsp_state', ?,
			'system', 'rsp_state',
			'{"risk_score": 0.82, "hidden_state": "THRASHING"}', '2026-03-24T02:00:00Z'
		)`,
		scenario.workspaceID, scenario.clusterID,
	)
	if err != nil {
		t.Fatalf("insert rsp snapshot: %v", err)
	}

	// First tick evaluates the state and sets candidate mode
	first, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("first tick control state: %v", err)
	}

	firstCluster := requireClusterControlStateCluster(t, first.Report.Clusters, scenario.clusterID)
	t.Logf("Tick 1: Mode=%s, Candidate=%s, Streak=%d, RSPRisk=%f", firstCluster.State.CurrentMode, firstCluster.State.CandidateMode, firstCluster.State.CandidateStreak, firstCluster.Signals.RSPRiskScore)

	// RSP should dominate
	if firstCluster.State.CandidateMode != clusterControlModeStabilize {
		t.Fatalf("expected candidate mode to be Stabilize due to RSP risk, got %s, RiskScore=%f DominantState=%s", firstCluster.State.CandidateMode, firstCluster.Signals.RSPRiskScore, firstCluster.Signals.RSPDominantState)
	}

	// Second tick transitions
	second, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	})
	if err != nil {
		t.Fatalf("second tick: %v", err)
	}

	if second.TransitionedCount != 1 {
		t.Fatalf("expected second tick to transition cluster")
	}

	row := requireStoredClusterControlStateRow(t, ctx, store, scenario.workspaceID, scenario.clusterID)
	if row.CurrentMode != clusterControlModeStabilize {
		t.Fatalf("expected transitioned mode to be Stabilize, got %s", row.CurrentMode)
	}
}
