package sqlite

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestBuildInstrumentationLocusBundleResolvesScopedClusterAndDominantTension(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedTensionScenario(t, ctx, store, "locus-bundle")
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

	bundle, err := store.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build instrumentation locus bundle: %v", err)
	}
	if !bundle.Resolved || bundle.ProtoClusterID == "" {
		t.Fatalf("expected resolved bundle, got %+v", bundle)
	}
	if bundle.TimeAuthority.WorkspaceID != scenario.workspaceID || bundle.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected locus bundle to expose workspace time authority, got %+v", bundle.TimeAuthority)
	}
	if bundle.GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected locus bundle generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", bundle.GeneratedAt, bundle.TimeAuthority.ReferenceAt)
	}
	if bundle.ResolvedFrom != "task_id" {
		t.Fatalf("expected task_id resolution, got %+v", bundle)
	}
	if bundle.Control == nil || bundle.Control.Cluster.ProtoClusterID != bundle.ProtoClusterID {
		t.Fatalf("expected control detail for resolved cluster, got %+v", bundle.Control)
	}
	if bundle.ControlState == nil || bundle.ControlState.State.ProtoClusterID != bundle.ProtoClusterID {
		t.Fatalf("expected control-state detail, got %+v", bundle.ControlState)
	}
	if bundle.MemoryCoherence == nil || bundle.MemoryCoherence.AgentID != "agent-a" {
		t.Fatalf("expected memory coherence detail for scoped agent, got %+v", bundle.MemoryCoherence)
	}
	if bundle.Corridor == nil || bundle.Corridor.Cluster.ProtoClusterID != bundle.ProtoClusterID {
		t.Fatalf("expected corridor detail, got %+v", bundle.Corridor)
	}
	if bundle.CorridorOwnership == nil || bundle.CorridorOwnership.Cluster.ProtoClusterID != bundle.ProtoClusterID {
		t.Fatalf("expected corridor ownership detail, got %+v", bundle.CorridorOwnership)
	}
	if bundle.CorridorFit == nil || bundle.CorridorFit.Cluster.ProtoClusterID != bundle.ProtoClusterID {
		t.Fatalf("expected corridor fit detail, got %+v", bundle.CorridorFit)
	}
	if bundle.CorridorBoundary == nil || bundle.CorridorBoundary.Cluster.ProtoClusterID != bundle.ProtoClusterID {
		t.Fatalf("expected corridor boundary detail, got %+v", bundle.CorridorBoundary)
	}
	if bundle.CorridorAuthority == nil || bundle.CorridorAuthority.Task.TaskID != scenario.taskID {
		t.Fatalf("expected corridor authority detail for scoped task, got %+v", bundle.CorridorAuthority)
	}
	if bundle.SegmentReport == nil || len(bundle.SegmentReport.Segments) == 0 {
		t.Fatalf("expected segment report for scoped doc/artifact anchors, got %+v", bundle.SegmentReport)
	}
	if bundle.SegmentReport != nil && bundle.SegmentReport.TimeAuthority.WorkspaceID != scenario.workspaceID {
		t.Fatalf("expected scoped segment report to reuse workspace time authority, got %+v", bundle.SegmentReport.TimeAuthority)
	}
	if bundle.SegmentReport != nil && bundle.SegmentReport.GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected scoped segment report generated_at to mirror locus time authority reference_at, got generated_at=%q reference_at=%q", bundle.SegmentReport.GeneratedAt, bundle.TimeAuthority.ReferenceAt)
	}
	if bundle.SegmentReport != nil && len(bundle.SegmentReport.Segments) > 0 && bundle.SegmentReport.Segments[0].GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected scoped segment generated_at to mirror locus time authority reference_at, got %+v", bundle.SegmentReport.Segments[0])
	}
	if len(bundle.RelatedSegmentRefs) == 0 {
		t.Fatalf("expected related segment refs from dominant tension, got %+v", bundle)
	}
	if len(bundle.Frontier) == 0 {
		t.Fatalf("expected scoped frontier, got %+v", bundle)
	}
	if bundle.DominantTension == nil || bundle.DominantTension.Tension.TensionID == "" {
		t.Fatalf("expected dominant tension detail, got %+v", bundle.DominantTension)
	}
	if bundle.DominantTension.Tension.ProtoClusterID != bundle.ProtoClusterID {
		t.Fatalf("expected dominant tension to stay scoped to locus cluster, got %+v", bundle.DominantTension)
	}
}

func TestBuildInstrumentationLocusBundleKeepsAvailableSegmentsWhenOneAnchorGoesStale(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedTensionScenario(t, ctx, store, "locus-stale-anchor")
	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if err := store.DeleteWorkspaceDoc(ctx, scenario.workspaceID, scenario.docKey, "developer"); err != nil {
		t.Fatalf("delete workspace doc: %v", err)
	}

	bundle, err := store.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build instrumentation locus bundle: %v", err)
	}
	if bundle.SegmentReport == nil || len(bundle.SegmentReport.Segments) == 0 {
		t.Fatalf("expected remaining artifact anchor to keep segment report after stale doc removal, got %+v", bundle)
	}
	if bundle.GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected stale-anchor locus bundle generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", bundle.GeneratedAt, bundle.TimeAuthority.ReferenceAt)
	}
	if bundle.SegmentReport.GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected stale-anchor segment report generated_at to mirror locus time authority reference_at, got generated_at=%q reference_at=%q", bundle.SegmentReport.GeneratedAt, bundle.TimeAuthority.ReferenceAt)
	}
	foundArtifact := false
	for _, source := range bundle.SegmentReport.Sources {
		if source.SourceKind == "workspace_artifact" && source.SourceRef == scenario.artifactRef {
			foundArtifact = true
			break
		}
	}
	if !foundArtifact {
		t.Fatalf("expected segment report to retain artifact source %s, got %+v", scenario.artifactRef, bundle.SegmentReport.Sources)
	}
	if len(bundle.RelatedSegmentRefs) == 0 {
		t.Fatalf("expected dominant tension segment refs to survive stale doc anchor, got %+v", bundle)
	}
}

func TestBuildInstrumentationLocusBundleReturnsUnresolvedWhenNoAnchorMatches(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	setupInstrumentationInternalWorkspace(t, ctx, store, "ws-locus-empty", "agent-a")
	createInstrumentationInternalTask(t, ctx, store, "ws-locus-empty", "task-locus-empty", "node-locus-empty")

	bundle, err := store.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID: "ws-locus-empty",
		TaskID:      "missing-task",
	})
	if err != nil {
		t.Fatalf("build instrumentation locus bundle: %v", err)
	}
	if bundle.Resolved || bundle.ProtoClusterID != "" {
		t.Fatalf("expected unresolved bundle for missing anchor, got %+v", bundle)
	}
	if bundle.Control != nil || bundle.ControlState != nil || bundle.MemoryCoherence != nil || bundle.Corridor != nil || bundle.CorridorOwnership != nil || bundle.CorridorBoundary != nil || bundle.DominantTension != nil || bundle.CorridorAuthority != nil || bundle.SegmentReport != nil {
		t.Fatalf("expected empty unresolved bundle surfaces, got %+v", bundle)
	}
}

func TestBuildInstrumentationLocusBundleShadowOnlySidecarSignals(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	healthy := seedInstrumentationLocusHealthyScenario(t, ctx, store, "healthy")
	healthyBundle, err := store.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID:   healthy.workspaceID,
		AgentID:       healthy.agentID,
		TaskID:        healthy.taskID,
		SessionID:     healthy.sessionID,
		DocKeys:       []string{healthy.docKey},
		ArtifactRefs:  []string{healthy.artifactRef},
		FrontierLimit: 2,
		MemoryBudget:  nil,
	})
	if err != nil {
		t.Fatalf("build healthy locus bundle: %v", err)
	}
	if !healthyBundle.Resolved || healthyBundle.Control == nil || healthyBundle.ControlState == nil || healthyBundle.MemoryCoherence == nil {
		t.Fatalf("expected healthy locus bundle to resolve control, control-state, and memory-coherence surfaces, got %+v", healthyBundle)
	}
	if healthyBundle.ControlState.State.State.CurrentMode != "STEADY" || healthyBundle.ControlState.State.State.AttentionBand != "STEADY" {
		t.Fatalf("expected healthy locus bundle to stay steady, got %+v", healthyBundle.ControlState.State.State)
	}
	if healthyBundle.MemoryCoherence.CoherenceBandHint != "STABLE" || healthyBundle.MemoryCoherence.NeedsAttention {
		t.Fatalf("expected healthy locus bundle to stay stable, got %+v", healthyBundle.MemoryCoherence)
	}

	risky := seedBlockedTensionScenario(t, ctx, store, "risky")
	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: risky.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	clusterID := "task:" + risky.workspaceID + "/" + risky.taskID
	for i := 0; i < 2; i++ {
		if _, err := store.TickClusterControlState(ctx, ClusterControlTickInput{
			WorkspaceID:    risky.workspaceID,
			ProtoClusterID: clusterID,
			ActorID:        "tests",
		}); err != nil {
			t.Fatalf("tick cluster control state %d: %v", i+1, err)
		}
	}
	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:        risky.workspaceID,
		AgentID:            "agent-a",
		SessionID:          risky.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-risky-sidecar",
		LookupCount:        4,
		L1HitCount:         1,
		L2HitCount:         1,
		StaleHitCount:      2,
		PromotionCount:     1,
		FlushCount:         1,
		FlushPositiveCount: 1,
	}); err != nil {
		t.Fatalf("report risky memory metrics: %v", err)
	}

	riskyBundle, err := store.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID:   risky.workspaceID,
		AgentID:       "agent-a",
		TaskID:        risky.taskID,
		SessionID:     risky.sessionID,
		DocKeys:       []string{risky.docKey},
		ArtifactRefs:  []string{risky.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build risky locus bundle: %v", err)
	}
	if riskyBundle.ControlState == nil || riskyBundle.ControlState.State.State.CurrentMode == "STEADY" {
		t.Fatalf("expected risky locus bundle to leave steady control mode, got %+v", riskyBundle.ControlState)
	}
	if riskyBundle.ControlState.State.State.PressureScore == 0 || riskyBundle.ControlState.State.State.CurrentMode == "STEADY" {
		t.Fatalf("expected risky control state to leave steady mode and carry pressure, got %+v", riskyBundle.ControlState.State.State)
	}
	if riskyBundle.MemoryCoherence == nil || !riskyBundle.MemoryCoherence.NeedsAttention || riskyBundle.MemoryCoherence.CoherenceBandHint == "STABLE" {
		t.Fatalf("expected risky locus bundle to surface coherence attention, got %+v", riskyBundle.MemoryCoherence)
	}
}

func TestInstrumentationLocusSidecarSnapshotsStaySynthetic(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "snapshot")
	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		SessionID:     scenario.sessionID,
		ReportScope:   "SESSION",
		ReportID:      "memmet-snapshot-sidecar",
		LookupCount:   5,
		L1HitCount:    5,
		StaleHitCount: 0,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}

	controlReport, err := store.BuildClusterControlStateReport(ctx, ClusterControlStateFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("build control state report: %v", err)
	}
	controlEvent, err := store.RecordClusterControlStateSnapshot(ctx, controlReport, ClusterControlSnapshotInput{
		ActorID: "tests",
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("record control state snapshot: %v", err)
	}
	if controlEvent.EventType != "cluster.control_state_snapshot" || controlEvent.EntityType != "instrumentation_control_state" {
		t.Fatalf("unexpected control state snapshot event %+v", controlEvent)
	}
	if !isSyntheticOperationalEvent(controlEvent) {
		t.Fatalf("expected control state snapshot to stay synthetic %+v", controlEvent)
	}

	coherenceReport, err := store.BuildMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: scenario.workspaceID,
		AgentID:     scenario.agentID,
		SessionID:   scenario.sessionID,
		ReportScope: "SESSION",
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("build memory coherence report: %v", err)
	}
	coherenceSnapshot, err := store.SnapshotMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: scenario.workspaceID,
		AgentID:     scenario.agentID,
		SessionID:   scenario.sessionID,
		ReportScope: "SESSION",
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("snapshot memory coherence report: %v", err)
	}
	if coherenceSnapshot.Event.EventType != "memory.coherence_snapshot" || coherenceSnapshot.Event.EntityType != "memory_coherence" {
		t.Fatalf("unexpected memory coherence snapshot event %+v", coherenceSnapshot.Event)
	}
	if !isSyntheticOperationalEvent(coherenceSnapshot.Event) {
		t.Fatalf("expected memory coherence snapshot to stay synthetic %+v", coherenceSnapshot.Event)
	}
	if coherenceReport.ScopeCount == 0 || coherenceSnapshot.Report.ScopeCount == 0 {
		t.Fatalf("expected scoped memory coherence report before snapshot, got report=%+v snapshot=%+v", coherenceReport, coherenceSnapshot.Report)
	}
}

type locusSidecarScenario struct {
	workspaceID string
	taskID      string
	docKey      string
	artifactRef string
	sessionID   string
	agentID     string
	clusterID   string
}

func seedInstrumentationLocusHealthyScenario(t *testing.T, ctx context.Context, store *Store, suffix string) locusSidecarScenario {
	t.Helper()

	scenario := locusSidecarScenario{
		workspaceID: "ws-locus-sidecar-" + suffix,
		taskID:      "task-locus-sidecar-" + suffix,
		docKey:      "locus-sidecar-doc-" + suffix,
		artifactRef: "artifact://locus-sidecar-" + suffix,
		sessionID:   "sess-locus-sidecar-" + suffix,
		agentID:     "agent-a",
	}
	scenario.clusterID = "task:" + scenario.workspaceID + "/" + scenario.taskID

	setupInstrumentationInternalWorkspace(t, ctx, store, scenario.workspaceID, scenario.agentID, "agent-b")
	createInstrumentationInternalTask(t, ctx, store, scenario.workspaceID, scenario.taskID, "node-locus-sidecar-"+suffix)
	claimInternalTaskForSessionStart(t, ctx, store, scenario.workspaceID, scenario.taskID, scenario.agentID)
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.docKey,
		Title:       "Locus Sidecar Runbook",
		Content:     "Healthy locus sidecar scenario",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Title:       "Locus Sidecar Artifact",
		ArtifactRef: scenario.artifactRef,
		Kind:        "workspace_doc",
		ContentType: "text/markdown",
		CreatedBy:   scenario.agentID,
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   scenario.sessionID,
		AgentID:     scenario.agentID,
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     scenario.agentID,
		TaskID:      scenario.taskID,
		Summary:     "healthy locus sidecar session",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			scenario.docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
		},
	}); err != nil {
		t.Fatalf("record agent session coordination: %v", err)
	}
	return scenario
}
