package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBuildControlReportUsesConfirmedTensionsAndProtoMetrics(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-throughput")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected tension refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirmed for control advisory",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report: %v", err)
	}
	if report.Workspace.TotalClusters == 0 || len(report.Clusters) == 0 {
		t.Fatalf("expected populated control report, got %+v", report)
	}
	cluster := requireControlClusterByID(t, report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	if cluster.ConfirmedTensionCount == 0 || cluster.ConfirmedCountsByType["bottleneck"] == 0 {
		t.Fatalf("expected confirmed bottleneck tension in control cluster, got %+v", cluster)
	}
	if cluster.Signals.AttentionBand != "HOT" {
		t.Fatalf("expected hot attention band for confirmed bottleneck, got %+v", cluster.Signals)
	}
	if cluster.Signals.ThroughputPressure < cluster.Signals.ReviewPressure || cluster.Signals.ThroughputPressure < cluster.Signals.CoordinationPressure {
		t.Fatalf("expected throughput pressure to dominate confirmed bottleneck cluster, got %+v", cluster.Signals)
	}
	if cluster.SuggestedControls.PriorityFocus != "throughput" || cluster.SuggestedControls.FanoutCap > 2 {
		t.Fatalf("expected throughput-focused suggested controls, got %+v", cluster.SuggestedControls)
	}
	if report.Workspace.HotClusterCount == 0 || report.Workspace.ConfirmedTensionCount == 0 {
		t.Fatalf("expected workspace summary to reflect hot confirmed cluster, got %+v", report.Workspace)
	}
}

func TestBuildControlReportIncludesTensionOnlyClusters(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-tension-only"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + workspaceID + "/contradiction/cluster-a",
		WorkspaceID:     workspaceID,
		ProtoClusterID:  "task:" + workspaceID + "/task-contradiction",
		TensionType:     "contradiction",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Confirmed contradiction",
		Summary:         "Conflict remains unresolved",
		AnchorKind:      "claim_subject",
		AnchorRef:       "deploy-sequence",
		TaskIDs:         []string{"task-contradiction"},
		AgentIDs:        []string{"agent-a", "agent-b"},
		ConstraintRefs:  []string{"claim:claim-1"},
		BaseScore:       70,
		SurfaceScore:    88,
		EvidenceCount:   2,
		LastSeenEventID: "rtev-test-1",
		LastSeenAt:      "2026-03-23T00:00:00Z",
		CreatedAt:       "2026-03-23T00:00:00Z",
		UpdatedAt:       "2026-03-23T00:00:00Z",
	})

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report for tension-only cluster: %v", err)
	}
	cluster := requireControlClusterByID(t, report.Clusters, "task:"+workspaceID+"/task-contradiction")
	if cluster.ConfirmedCountsByType["contradiction"] != 1 {
		t.Fatalf("expected confirmed contradiction to be surfaced, got %+v", cluster)
	}
	if !cluster.MetricsMissing || !cluster.BasisStale {
		t.Fatalf("expected tension-only control cluster to be flagged as missing metrics and stale basis, got %+v", cluster)
	}
	if cluster.LastTensionBasisAt == "" {
		t.Fatalf("expected tension-only control cluster to expose last_tension_basis_at, got %+v", cluster)
	}
	if cluster.Signals.AttentionBand != "STEADY" || cluster.Signals.ReviewPressure != 0 {
		t.Fatalf("expected stale/missing basis to suppress confirmed-tension-driven advisory pressure, got %+v", cluster.Signals)
	}
	if cluster.SuggestedControls.ReviewDepth != 1 {
		t.Fatalf("expected stale/missing basis to keep advisory controls conservative, got %+v", cluster.SuggestedControls)
	}
}

func TestBuildControlReportKeepsPendingTensionsVisibleWithoutDrivingBanding(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-pending-only"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + workspaceID + "/ambiguity/cluster-a",
		WorkspaceID:     workspaceID,
		ProtoClusterID:  "task:" + workspaceID + "/task-pending",
		TensionType:     "ambiguity",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewPending,
		Title:           "Pending ambiguity",
		Summary:         "Decision remains open but is not yet confirmed",
		AnchorKind:      "workspace_doc",
		AnchorRef:       "docs/pending.md",
		TaskIDs:         []string{"task-pending"},
		AgentIDs:        []string{"agent-a", "agent-b"},
		BaseScore:       55,
		SurfaceScore:    61,
		EvidenceCount:   2,
		LastSeenEventID: "rtev-test-2",
		LastSeenAt:      "2026-03-23T00:00:00Z",
		CreatedAt:       "2026-03-23T00:00:00Z",
		UpdatedAt:       "2026-03-23T00:00:00Z",
	})

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report for pending-only cluster: %v", err)
	}
	cluster := requireControlClusterByID(t, report.Clusters, "task:"+workspaceID+"/task-pending")
	if cluster.PendingCountsByType["ambiguity"] != 1 || cluster.PendingTensionCount != 1 {
		t.Fatalf("expected pending ambiguity tension to stay visible, got %+v", cluster)
	}
	if cluster.Signals.AttentionBand != "STEADY" {
		t.Fatalf("expected pending-only cluster to remain steady, got %+v", cluster.Signals)
	}
	if cluster.Signals.ReviewPressure != 0 || cluster.SuggestedControls.ReviewDepth != 1 {
		t.Fatalf("expected pending-only cluster not to elevate advisory controls, got signals=%+v controls=%+v", cluster.Signals, cluster.SuggestedControls)
	}
}

func TestBuildControlReportPendingTensionsDoNotChangeConfirmedDrivenSignals(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-pending-mixed")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for mixed pending/confirmed control test",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	baselineReport, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build baseline control report: %v", err)
	}
	baselineCluster := requireControlClusterByID(t, baselineReport.Clusters, clusterID)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + scenario.workspaceID + "/pending-ambiguity/" + scenario.taskID,
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  clusterID,
		TensionType:     "ambiguity",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewPending,
		Title:           "Pending ambiguity",
		Summary:         "Waiting on confirmation before advisory escalation",
		AnchorKind:      "workspace_doc",
		AnchorRef:       scenario.docKey,
		TaskIDs:         []string{scenario.taskID},
		SessionIDs:      []string{scenario.sessionID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef},
		AgentIDs:        []string{"agent-a", "agent-b"},
		EvidenceCount:   1,
		LastSeenEventID: "rtev-pending-ambiguity",
		LastSeenAt:      "2026-03-23T01:00:00Z",
		CreatedAt:       "2026-03-23T01:00:00Z",
		UpdatedAt:       "2026-03-23T01:00:00Z",
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension:" + scenario.workspaceID + "/pending-bridge/" + scenario.taskID,
		WorkspaceID:     scenario.workspaceID,
		ProtoClusterID:  clusterID,
		TensionType:     "bridge",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewPending,
		Title:           "Pending bridge",
		Summary:         "Coordination load is being reviewed",
		AnchorKind:      "workspace_artifact",
		AnchorRef:       scenario.artifactRef,
		TaskIDs:         []string{scenario.taskID},
		SessionIDs:      []string{scenario.sessionID},
		DocKeys:         []string{scenario.docKey},
		ArtifactRefs:    []string{scenario.artifactRef},
		AgentIDs:        []string{"agent-a", "agent-b"},
		EvidenceCount:   1,
		LastSeenEventID: "rtev-pending-bridge",
		LastSeenAt:      "2026-03-23T01:00:01Z",
		CreatedAt:       "2026-03-23T01:00:01Z",
		UpdatedAt:       "2026-03-23T01:00:01Z",
	})

	afterReport, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build mixed pending/confirmed control report: %v", err)
	}
	afterCluster := requireControlClusterByID(t, afterReport.Clusters, clusterID)
	if afterCluster.PendingTensionCount != 2 || afterCluster.PendingCountsByType["ambiguity"] != 1 || afterCluster.PendingCountsByType["bridge"] != 1 {
		t.Fatalf("expected pending tensions to stay visible, got %+v", afterCluster)
	}
	if afterCluster.Signals != baselineCluster.Signals {
		t.Fatalf("expected pending tensions not to change confirmed-driven signals, got baseline=%+v after=%+v", baselineCluster.Signals, afterCluster.Signals)
	}
	if afterCluster.SuggestedControls != baselineCluster.SuggestedControls {
		t.Fatalf("expected pending tensions not to change suggested controls, got baseline=%+v after=%+v", baselineCluster.SuggestedControls, afterCluster.SuggestedControls)
	}
	if afterCluster.ConfirmedTensionCount != baselineCluster.ConfirmedTensionCount || !reflect.DeepEqual(afterCluster.ConfirmedCountsByType, baselineCluster.ConfirmedCountsByType) {
		t.Fatalf("expected confirmed counts to remain unchanged, got baseline=%+v after=%+v", baselineCluster, afterCluster)
	}
	if !strings.Contains(afterCluster.Summary, "tracking 2 pending tensions") {
		t.Fatalf("expected summary to retain pending tension context, got %q", afterCluster.Summary)
	}
}

func TestBuildControlReportMarksBasisStaleWhenClusterMetricsOutrunConfirmedTensionRefresh(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-basis-stale")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected tension refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for basis staleness test",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}
	futureEventAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		WorkspaceID: scenario.workspaceID,
		EventType:   "custom.followup",
		EntityType:  "workspace_task",
		EntityID:    scenario.taskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		SessionID:   scenario.sessionID,
		TaskID:      scenario.taskID,
		PayloadJSON: `{"reason":"post-confirmation follow-up"}`,
		CreatedAt:   futureEventAt,
	}); err != nil {
		t.Fatalf("record follow-up runtime event: %v", err)
	}

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report after follow-up event: %v", err)
	}
	cluster := requireControlClusterByID(t, report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	if cluster.MetricsMissing {
		t.Fatalf("expected metric-backed cluster to keep metrics, got %+v", cluster)
	}
	if cluster.LastTensionBasisAt == "" {
		t.Fatalf("expected cluster to expose last_tension_basis_at, got %+v", cluster)
	}
	if !cluster.BasisStale {
		t.Fatalf("expected cluster to be marked stale after newer runtime event, got %+v", cluster)
	}
}

func TestBuildControlReportKeepsWorkspaceTimeAuthorityInspectable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-time-authority")

	if err := store.SetPolicyMode(ctx, scenario.workspaceID, "active"); err != nil {
		t.Fatalf("set active policy mode: %v", err)
	}
	if _, err := store.IncrementEpoch(ctx, scenario.workspaceID); err != nil {
		t.Fatalf("increment control epoch: %v", err)
	}

	referenceAt := time.Now().UTC().Add(45 * time.Minute).Round(0).Format(time.RFC3339Nano)
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		WorkspaceID: scenario.workspaceID,
		EventType:   "control.time_authority_probe",
		EntityType:  "workspace",
		EntityID:    scenario.workspaceID,
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{"probe":"control"}`,
		CreatedAt:   referenceAt,
	}); err != nil {
		t.Fatalf("record control time authority event: %v", err)
	}

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report for authority inspection: %v", err)
	}
	if len(report.Clusters) == 0 {
		t.Fatalf("expected control report to remain populated, got %+v", report)
	}

	authority, err := store.GetWorkspaceTimeAuthority(ctx, scenario.workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	if authority.WorkspaceID != scenario.workspaceID || authority.CurrentEpoch != 1 || authority.ReferenceAt != referenceAt {
		t.Fatalf("expected control surface to keep authority pair inspectable, got %+v", authority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected control report generated_at %q to mirror time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
}

func TestControlSuggestedControlsClampRateLimitsForHotSignals(t *testing.T) {
	t.Parallel()

	controls := controlSuggestedControlsForSignals(
		ProtoClusterMetrics{
			MaxAgentActivityShare: 0.82,
		},
		map[string]int{
			"bottleneck": 1,
			"bridge":     1,
		},
		ControlSignalVector{
			ThroughputPressure:   91,
			ReviewPressure:       18,
			CoordinationPressure: 84,
			PressureScore:        94,
			AttentionBand:        "HOT",
		},
	)
	if controls.PriorityFocus != "throughput" {
		t.Fatalf("expected throughput focus for hot throughput-dominant cluster, got %+v", controls)
	}
	if controls.FanoutCap != 1 {
		t.Fatalf("expected hottest cluster to clamp fanout cap to 1, got %+v", controls)
	}
	if controls.ContextCap != 4 {
		t.Fatalf("expected high coordination pressure to clamp context cap to 4, got %+v", controls)
	}
	if controls.BridgeQuota != 2 {
		t.Fatalf("expected bridge quota expansion under coordination load, got %+v", controls)
	}
	if controls.MergeThreshold != 0.75 {
		t.Fatalf("expected hot pressure to tighten merge threshold to 0.75, got %+v", controls)
	}
}

func TestBuildControlReportProtoClusterFilterIsStable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-filter-stable")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for proto-cluster filter stability",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	fullReport, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build full control report: %v", err)
	}
	fullCluster := requireControlClusterByID(t, fullReport.Clusters, "task:"+scenario.workspaceID+"/"+scenario.taskID)

	filteredFirst, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: fullCluster.ProtoClusterID,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("build filtered control report (first): %v", err)
	}
	filteredSecond, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: fullCluster.ProtoClusterID,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("build filtered control report (second): %v", err)
	}
	if filteredFirst.Filter.ProtoClusterID != fullCluster.ProtoClusterID || filteredSecond.Filter.ProtoClusterID != fullCluster.ProtoClusterID {
		t.Fatalf("expected proto-cluster filter %s to be preserved, got first=%+v second=%+v", fullCluster.ProtoClusterID, filteredFirst.Filter, filteredSecond.Filter)
	}
	if len(filteredFirst.Clusters) != 1 || len(filteredSecond.Clusters) != 1 {
		t.Fatalf("expected one filtered cluster, got first=%d second=%d", len(filteredFirst.Clusters), len(filteredSecond.Clusters))
	}
	if !reflect.DeepEqual(filteredFirst.Clusters, filteredSecond.Clusters) {
		t.Fatalf("expected repeated filtered control reads to stay stable, got first=%+v second=%+v", filteredFirst.Clusters, filteredSecond.Clusters)
	}
	if !reflect.DeepEqual(fullCluster, filteredFirst.Clusters[0]) {
		t.Fatalf("expected filtered cluster to match full report cluster, got full=%+v filtered=%+v", fullCluster, filteredFirst.Clusters[0])
	}
}

func TestRecordControlSignalSnapshotAppendsRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-snapshot")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create control snapshot seed tensions, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions before snapshot: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for control snapshot",
	}); err != nil {
		t.Fatalf("confirm tension before snapshot: %v", err)
	}

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report: %v", err)
	}
	event, err := store.RecordControlSignalSnapshot(ctx, report, ControlSnapshotInput{
		ActorID: "dashboard",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("record control signal snapshot: %v", err)
	}
	if event.EventType != "cluster.control_advisory_snapshot" || event.EntityType != "instrumentation_control" || event.EntityID != scenario.workspaceID {
		t.Fatalf("unexpected control snapshot event %+v", event)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode control snapshot payload: %v", err)
	}
	if payload["typed_event_type"] != "CONTROL_ADVISORY_SNAPSHOT" {
		t.Fatalf("expected control snapshot typed_event_type, got %+v", payload)
	}
	if payload["event_kind"] != "cluster.control_advisory_snapshot" {
		t.Fatalf("expected control snapshot event_kind, got %+v", payload)
	}
	clusters, ok := payload["clusters"].([]any)
	if !ok || len(clusters) == 0 {
		t.Fatalf("expected clusters in control snapshot payload, got %+v", payload)
	}
	if payload["captured_cluster_count"] != float64(len(clusters)) || payload["source_cluster_count"] != float64(len(report.Clusters)) {
		t.Fatalf("expected control snapshot payload to expose captured/source cluster counts, got %+v", payload)
	}
	if payload["snapshot_limit"] != float64(10) || payload["snapshot_truncated"] != (len(report.Clusters) > 10) {
		t.Fatalf("expected control snapshot payload to preserve limit/truncation metadata, got %+v", payload)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		EntityType:  "instrumentation_control",
		EntityID:    scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list control snapshot runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one control signal snapshot event, got %+v", events)
	}
}

func TestRecordControlSignalSnapshotUsesProtoClusterEntityIDWhenScoped(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-scoped-snapshot")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for scoped control snapshot",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("build scoped control report: %v", err)
	}
	if len(report.Clusters) != 1 || report.Clusters[0].ProtoClusterID != clusterID {
		t.Fatalf("expected one scoped cluster %s, got %+v", clusterID, report.Clusters)
	}

	event, err := store.RecordControlSignalSnapshot(ctx, report, ControlSnapshotInput{
		ActorID: "dashboard",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("record scoped control signal snapshot: %v", err)
	}
	if event.EntityID != clusterID {
		t.Fatalf("expected scoped snapshot entity_id %s, got %+v", clusterID, event)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode scoped control snapshot payload: %v", err)
	}
	filter, ok := payload["filter"].(map[string]any)
	if !ok {
		t.Fatalf("expected scoped snapshot filter in payload, got %+v", payload)
	}
	if filter["proto_cluster_id"] != clusterID {
		t.Fatalf("expected payload filter proto_cluster_id %s, got %+v", clusterID, filter)
	}
	clusters, ok := payload["clusters"].([]any)
	if !ok || len(clusters) != 1 {
		t.Fatalf("expected one scoped cluster in payload, got %+v", payload)
	}
	if payload["event_kind"] != "cluster.control_advisory_snapshot" {
		t.Fatalf("expected scoped control snapshot event_kind, got %+v", payload)
	}
	if payload["captured_cluster_count"] != float64(1) || payload["source_cluster_count"] != float64(1) {
		t.Fatalf("expected scoped control snapshot to expose captured/source counts, got %+v", payload)
	}
	if payload["snapshot_limit"] != float64(10) || payload["snapshot_truncated"] != false {
		t.Fatalf("expected scoped control snapshot limit/truncation metadata, got %+v", payload)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		EntityType:  "instrumentation_control",
		EntityID:    clusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list scoped control snapshot runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one scoped control snapshot event, got %+v", events)
	}
}

func TestRecordControlSignalSnapshotDoesNotBindWorkspaceScopedEventToArbitraryTaskOrSession(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-snapshot-unscoped-binding")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for unscoped control snapshot binding test",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report: %v", err)
	}
	event, err := store.RecordControlSignalSnapshot(ctx, report, ControlSnapshotInput{
		ActorID: "dashboard",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("record control signal snapshot: %v", err)
	}
	if event.TaskID != "" || event.SessionID != "" {
		t.Fatalf("expected workspace-scoped advisory snapshot to stay unbound from task/session, got %+v", event)
	}

	taskScopedEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		TaskID:      scenario.taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list task-scoped advisory snapshot events: %v", err)
	}
	if len(taskScopedEvents) != 0 {
		t.Fatalf("expected workspace-scoped advisory snapshot not to leak into task-scoped replay, got %+v", taskScopedEvents)
	}
}

func TestRecordControlSignalSnapshotRejectsMissingScopedCluster(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	_, err := store.RecordControlSignalSnapshot(ctx, ControlReport{
		WorkspaceID: "ws-control-scoped-missing",
		Filter: ControlReportFilter{
			WorkspaceID:    "ws-control-scoped-missing",
			ProtoClusterID: "task:ws-control-scoped-missing/task-missing",
			Limit:          1,
		},
	}, ControlSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
	if err == nil || !strings.Contains(err.Error(), "control cluster not found") {
		t.Fatalf("expected scoped control snapshot to reject missing proto cluster, got %v", err)
	}
}

func TestControlSignalSnapshotDoesNotContaminateInstrumentationOrTensionRefresh(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-contamination")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for contamination test",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	beforeInstrumentation, err := store.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		Limit:        200,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("build instrumentation report before advisory snapshot: %v", err)
	}
	controlReport, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report: %v", err)
	}
	if _, err := store.RecordControlSignalSnapshot(ctx, controlReport, ControlSnapshotInput{
		ActorID: "dashboard",
		Limit:   10,
	}); err != nil {
		t.Fatalf("record control advisory snapshot: %v", err)
	}

	afterInstrumentation, err := store.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		Limit:        200,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("build instrumentation report after advisory snapshot: %v", err)
	}
	beforeCluster := requireProtoClusterMetricsByID(t, beforeInstrumentation.Clusters, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	afterCluster := requireProtoClusterMetricsByID(t, afterInstrumentation.Clusters, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	if !reflect.DeepEqual(beforeCluster, afterCluster) {
		t.Fatalf("expected instrumentation cluster metrics to ignore advisory snapshot, got before=%+v after=%+v", beforeCluster, afterCluster)
	}

	secondRefresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions after advisory snapshot: %v", err)
	}
	if secondRefresh.CreatedCount != 0 || secondRefresh.UpdatedCount != 0 || secondRefresh.RecoveredCount != 0 {
		t.Fatalf("expected advisory snapshot to stay out of tension refresh evidence, got %+v", secondRefresh)
	}
}

func TestRecordControlSignalSnapshotDoesNotDriftControlReportAcrossReplayLoop(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "control-replay-loop")

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for replay-loop control test",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	baselineReport, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build baseline control report: %v", err)
	}
	baselineCluster := requireControlClusterByID(t, baselineReport.Clusters, clusterID)

	for _, actorID := range []string{"dashboard-a", "dashboard-b"} {
		event, err := store.RecordControlSignalSnapshot(ctx, baselineReport, ControlSnapshotInput{
			ActorID: actorID,
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("record control signal snapshot for %s: %v", actorID, err)
		}
		if event.EventType != "cluster.control_advisory_snapshot" {
			t.Fatalf("expected control advisory snapshot event, got %+v", event)
		}
	}

	replay, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	snapshotCount := 0
	for _, event := range replay.Events {
		if event.EventType == "cluster.control_advisory_snapshot" {
			snapshotCount++
		}
	}
	if snapshotCount != 0 {
		t.Fatalf("expected workspace-scoped advisory snapshots to stay out of task replay, got %d from %+v", snapshotCount, replay.Events)
	}

	snapshotEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		EntityType:  "instrumentation_control",
		EntityID:    scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace-scoped advisory snapshots: %v", err)
	}
	if len(snapshotEvents) != 2 {
		t.Fatalf("expected two persisted workspace-scoped advisory snapshots, got %+v", snapshotEvents)
	}

	afterReport, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report after replay loop: %v", err)
	}
	afterCluster := requireControlClusterByID(t, afterReport.Clusters, clusterID)
	if afterCluster.Signals != baselineCluster.Signals {
		t.Fatalf("expected replay loop snapshots not to drift control signals, got baseline=%+v after=%+v", baselineCluster.Signals, afterCluster.Signals)
	}
	if afterCluster.SuggestedControls != baselineCluster.SuggestedControls {
		t.Fatalf("expected replay loop snapshots not to drift suggested controls, got baseline=%+v after=%+v", baselineCluster.SuggestedControls, afterCluster.SuggestedControls)
	}
	if afterCluster.BasisStale != baselineCluster.BasisStale || afterCluster.LastTensionBasisAt != baselineCluster.LastTensionBasisAt {
		t.Fatalf("expected replay loop snapshots not to drift basis freshness, got baseline=%+v after=%+v", baselineCluster, afterCluster)
	}
	if afterCluster.ConfirmedTensionCount != baselineCluster.ConfirmedTensionCount || afterCluster.PendingTensionCount != baselineCluster.PendingTensionCount {
		t.Fatalf("expected replay loop snapshots not to change tension counts, got baseline=%+v after=%+v", baselineCluster, afterCluster)
	}
}

func requireControlClusterByID(t *testing.T, items []ControlClusterReport, protoClusterID string) ControlClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == protoClusterID {
			return item
		}
	}
	t.Fatalf("control cluster %s not found in %+v", protoClusterID, items)
	return ControlClusterReport{}
}

func requireProtoClusterMetricsByID(t *testing.T, items []ProtoClusterReport, protoClusterID string) ProtoClusterMetrics {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == protoClusterID {
			return item.Metrics
		}
	}
	t.Fatalf("proto cluster %s not found in %+v", protoClusterID, items)
	return ProtoClusterMetrics{}
}
