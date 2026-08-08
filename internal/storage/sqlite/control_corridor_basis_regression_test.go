package sqlite

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildControlReportBasisStaleDoesNotMovePolicyShapedSignals(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	freshAt := now.Format(time.RFC3339Nano)
	staleAt := now.Add(-96 * time.Hour).Format(time.RFC3339Nano)

	cluster := ProtoClusterReport{
		ProtoClusterID: "task:ws-control-basis-no-drift/task-a",
		ResolutionKind: "task",
		TaskIDs:        []string{"task-a"},
		SessionIDs:     []string{"sess-a"},
		AgentIDs:       []string{"agent-a", "agent-b"},
		Metrics: ProtoClusterMetrics{
			EventCount:                  8,
			OpenQueueCount:              1,
			BlockerSignalCount:          2,
			BlockerDensity:              0.30,
			CommunicationCentralization: 0.35,
			MaxAgentActivityShare:       0.40,
			DuplicationIndex:            0.20,
			LastEventAt:                 freshAt,
		},
	}
	freshTension := TensionRecord{
		TensionID:      "tension:ws-control-basis-no-drift/bottleneck/task-a",
		WorkspaceID:    "ws-control-basis-no-drift",
		ProtoClusterID: cluster.ProtoClusterID,
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewConfirmed,
		LastDetectedAt: freshAt,
		LastSeenAt:     freshAt,
		UpdatedAt:      freshAt,
		CreatedAt:      now.Add(-time.Hour).Format(time.RFC3339Nano),
	}
	staleTension := freshTension
	staleTension.LastDetectedAt = staleAt
	staleTension.LastSeenAt = staleAt
	staleTension.UpdatedAt = staleAt

	baselineCluster := buildControlClusterReport(cluster, []TensionRecord{freshTension}, "")
	if baselineCluster.BasisStale {
		t.Fatalf("expected baseline cluster to start fresh, got %+v", baselineCluster)
	}

	staleCluster := buildControlClusterReport(cluster, []TensionRecord{staleTension}, "")
	if !staleCluster.BasisStale {
		t.Fatalf("expected stale-basis cluster after aging confirmed tension timestamps, got %+v", staleCluster)
	}
	if staleCluster.LastTensionBasisAt == "" {
		t.Fatalf("expected stale cluster to retain last_tension_basis_at, got %+v", staleCluster)
	}
	if !reflect.DeepEqual(staleCluster.Signals, baselineCluster.Signals) {
		t.Fatalf("expected stale basis not to move advisory signal vector, baseline=%+v stale=%+v", baselineCluster.Signals, staleCluster.Signals)
	}
	if !reflect.DeepEqual(staleCluster.SuggestedControls, baselineCluster.SuggestedControls) {
		t.Fatalf("expected stale basis not to move policy-shaped controls, baseline=%+v stale=%+v", baselineCluster.SuggestedControls, staleCluster.SuggestedControls)
	}
	if !reflect.DeepEqual(staleCluster.ConfirmedCountsByType, baselineCluster.ConfirmedCountsByType) {
		t.Fatalf("expected stale basis not to rewrite confirmed tension counts, baseline=%+v stale=%+v", baselineCluster.ConfirmedCountsByType, staleCluster.ConfirmedCountsByType)
	}
}

func TestControlTimestampStaleUsesReferenceEventTime(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	basisAt := now.Add(-96 * time.Hour).Format(time.RFC3339Nano)
	sameReference := now.Add(-96 * time.Hour).Format(time.RFC3339Nano)
	newerReference := now.Format(time.RFC3339Nano)

	if controlTimestampStale(basisAt, "", controlReadBasisStaleAfter) {
		t.Fatalf("expected empty reference event time not to mark basis stale")
	}
	if controlTimestampStale(basisAt, sameReference, controlReadBasisStaleAfter) {
		t.Fatalf("expected equal reference event time not to mark basis stale")
	}
	if !controlTimestampStale(basisAt, newerReference, controlReadBasisStaleAfter) {
		t.Fatalf("expected newer reference event time to mark basis stale once threshold is exceeded")
	}
}

func TestCorridorBasisIsStaleUsesReferenceEventTime(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	basisAt := now.Add(-96 * time.Hour).Format(time.RFC3339Nano)
	sameReference := now.Add(-96 * time.Hour).Format(time.RFC3339Nano)
	newerReference := now.Format(time.RFC3339Nano)

	if corridorBasisIsStale(basisAt, "") {
		t.Fatalf("expected empty reference event time not to mark corridor basis stale")
	}
	if corridorBasisIsStale(basisAt, sameReference) {
		t.Fatalf("expected equal reference event time not to mark corridor basis stale")
	}
	if !corridorBasisIsStale(basisAt, newerReference) {
		t.Fatalf("expected newer reference event time to mark corridor basis stale once threshold is exceeded")
	}
}
