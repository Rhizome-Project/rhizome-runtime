package sqlite

import (
	"context"
	"testing"
)

func TestSnapshotRSPStateReportLeavesClusterControlStateUntouched(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedConfirmedMetricsBackedControlStateScenario(t, ctx, store, "rsp-state-shadow-isolation")

	if _, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		ActorID:        "operator",
	}); err != nil {
		t.Fatalf("seed control state tick: %v", err)
	}

	before := requireStoredClusterControlStateRow(t, ctx, store, scenario.workspaceID, scenario.clusterID)

	result, err := store.SnapshotRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		TaskID:         scenario.taskID,
		SessionID:      scenario.sessionID,
		DocKeys:        []string{scenario.docKey},
		ArtifactRefs:   []string{scenario.artifactRef},
		FrontierLimit:  2,
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

	after := requireStoredClusterControlStateRow(t, ctx, store, scenario.workspaceID, scenario.clusterID)
	if before.CurrentMode != after.CurrentMode ||
		before.CandidateMode != after.CandidateMode ||
		before.CandidateStreak != after.CandidateStreak ||
		before.DominantViolationKind != after.DominantViolationKind ||
		before.DominantViolationScore != after.DominantViolationScore ||
		before.AttentionBand != after.AttentionBand ||
		before.PressureScore != after.PressureScore ||
		before.LastBasisAt != after.LastBasisAt ||
		before.LastTransitionAt != after.LastTransitionAt {
		t.Fatalf("expected rsp snapshot to leave cluster control state untouched, before=%+v after=%+v", before, after)
	}
}
