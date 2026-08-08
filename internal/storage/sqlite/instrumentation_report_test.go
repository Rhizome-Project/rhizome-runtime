package sqlite_test

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type instrumentationScenario struct {
	workspaceID      string
	taskID           string
	runbookDocKey    string
	standaloneDocKey string
	artifactRef      string
	primarySession   string
	secondarySession string
}

func TestBuildInstrumentationReportResolvesStableProtoClustersAndMetrics(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-instrumentation-report", "task-instrumentation-report")

	report, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       200,
	})
	if err != nil {
		t.Fatalf("build instrumentation report: %v", err)
	}
	if report.Workspace.TotalClusters < 2 {
		t.Fatalf("expected at least task and standalone doc clusters, got %+v", report.Clusters)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected instrumentation report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected instrumentation report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.Workspace.TopAgentByActivity != "agent-a" {
		t.Fatalf("expected agent-a to lead activity, got %+v", report.Workspace)
	}
	if report.Workspace.WorkspaceCommunicationCentralization <= 0 {
		t.Fatalf("expected communication centralization > 0, got %+v", report.Workspace)
	}
	if report.Replay.EventTypeCounts["agent_message.sent"] == 0 {
		t.Fatalf("expected runtime journal replay to include messaging activity, got %+v", report.Replay.EventTypeCounts)
	}

	taskCluster := requireProtoCluster(t, report, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	if taskCluster.ResolutionKind != "task" {
		t.Fatalf("expected task proto cluster, got %+v", taskCluster)
	}
	if !containsString(taskCluster.DocKeys, scenario.runbookDocKey) {
		t.Fatalf("expected task cluster doc refs to include runbook, got %+v", taskCluster)
	}
	if !containsString(taskCluster.ArtifactRefs, scenario.artifactRef) {
		t.Fatalf("expected task cluster artifact refs to include deploy log, got %+v", taskCluster)
	}
	if !containsString(taskCluster.SessionIDs, scenario.primarySession) || !containsString(taskCluster.SessionIDs, scenario.secondarySession) {
		t.Fatalf("expected task cluster sessions to include both sessions, got %+v", taskCluster)
	}
	if taskCluster.Metrics.OpenQueueCount != 1 {
		t.Fatalf("expected one open queue on task cluster, got %+v", taskCluster.Metrics)
	}
	if taskCluster.Metrics.BlockerSignalCount == 0 || taskCluster.Metrics.BlockerDensity <= 0 {
		t.Fatalf("expected blocker signals on task cluster, got %+v", taskCluster.Metrics)
	}
	if taskCluster.Metrics.EventTypeCounts["agent_update.posted"] == 0 {
		t.Fatalf("expected agent update activity to stay attached to task cluster, got %+v", taskCluster.Metrics.EventTypeCounts)
	}
	if taskCluster.Metrics.DuplicationIndex <= 0 {
		t.Fatalf("expected duplication index > 0, got %+v", taskCluster.Metrics)
	}
	if taskCluster.Metrics.MaxAgentActivityShare <= 0 {
		t.Fatalf("expected activity share > 0, got %+v", taskCluster.Metrics)
	}
	if taskCluster.Metrics.RoleLock.Index <= 0 {
		t.Fatalf("expected role-lock index > 0, got %+v", taskCluster.Metrics.RoleLock)
	}
	if taskCluster.Metrics.RoleLock.Partial {
		t.Fatalf("expected role-lock metrics to stop being partial once motif_reuse_hhi is surfaced, got %+v", taskCluster.Metrics.RoleLock)
	}
	if len(taskCluster.Metrics.RoleLock.MissingComponents) != 0 {
		t.Fatalf("expected role-lock metrics to clear missing motif_reuse component, got %+v", taskCluster.Metrics.RoleLock)
	}
	if taskCluster.Metrics.RoleLock.StewardHHI <= 0 || taskCluster.Metrics.RoleLock.AcceptedBuilderHHI <= 0 || taskCluster.Metrics.RoleLock.DefaultReviewerHHI <= 0 {
		t.Fatalf("expected role-lock components to stay non-zero for steward/builder/reviewer concentration, got %+v", taskCluster.Metrics.RoleLock)
	}
	if taskCluster.Metrics.RoleLock.MotifReuseHHI <= 0 {
		t.Fatalf("expected role-lock metrics to surface motif reuse concentration, got %+v", taskCluster.Metrics.RoleLock)
	}
	if taskCluster.Metrics.RoleLock.ActiveStewardCount != 1 || taskCluster.Metrics.RoleLock.ActiveClaimCount != 1 || taskCluster.Metrics.RoleLock.BlockingReviewCount != 1 {
		t.Fatalf("expected role-lock counts to reflect active steward/claim/reviewer evidence, got %+v", taskCluster.Metrics.RoleLock)
	}

	docCluster := requireProtoCluster(t, report, "workspace_doc:"+scenario.workspaceID+"/"+scenario.standaloneDocKey)
	if docCluster.ResolutionKind != "doc" {
		t.Fatalf("expected standalone doc cluster, got %+v", docCluster)
	}

	filtered, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       200,
	})
	if err != nil {
		t.Fatalf("build filtered instrumentation report: %v", err)
	}
	filteredTaskCluster := requireProtoCluster(t, filtered, taskCluster.ProtoClusterID)
	if filteredTaskCluster.ProtoClusterID != taskCluster.ProtoClusterID || filteredTaskCluster.ResolutionKind != "task" {
		t.Fatalf("expected stable task proto cluster under filter, got %+v", filteredTaskCluster)
	}
	if hasProtoCluster(filtered.Clusters, "workspace_doc:"+scenario.workspaceID+"/"+scenario.standaloneDocKey) {
		t.Fatalf("expected task filter to exclude standalone doc cluster, got %+v", filtered.Clusters)
	}
}

func TestBuildInstrumentationReportRepeatedRefreshKeepsStableProtoClusterIDs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-instrumentation-repeat", "task-instrumentation-repeat")

	firstReport, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("build first instrumentation report: %v", err)
	}
	secondReport, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("build second instrumentation report: %v", err)
	}
	if firstReport.GeneratedAt != firstReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected first instrumentation report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", firstReport.GeneratedAt, firstReport.TimeAuthority.ReferenceAt)
	}
	if secondReport.GeneratedAt != secondReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected second instrumentation report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", secondReport.GeneratedAt, secondReport.TimeAuthority.ReferenceAt)
	}

	firstIDs := protoClusterIDs(firstReport.Clusters)
	secondIDs := protoClusterIDs(secondReport.Clusters)
	if !equalStringSlices(firstIDs, secondIDs) {
		t.Fatalf("expected repeated refresh to keep stable proto cluster ids, got %v then %v", firstIDs, secondIDs)
	}

	firstTaskCluster := requireProtoCluster(t, firstReport, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	secondTaskCluster := requireProtoCluster(t, secondReport, "task:"+scenario.workspaceID+"/"+scenario.taskID)
	if firstTaskCluster.Metrics.EventCount != secondTaskCluster.Metrics.EventCount || firstTaskCluster.Metrics.BlockerSignalCount != secondTaskCluster.Metrics.BlockerSignalCount {
		t.Fatalf("expected repeated refresh to preserve task cluster metrics, got %+v and %+v", firstTaskCluster.Metrics, secondTaskCluster.Metrics)
	}
}

func TestBuildInstrumentationReportCapturesTakeoverReplayStyleLifecycle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-instrumentation-takeover"
		taskID      = "task-instrumentation-takeover"
		docKey      = "handoff-runbook"
		artifactRef = "artifact://handoff-log"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Instrumentation Takeover",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{"agent-source", "agent-target"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Handoff Runbook",
		Content:     "Source session hands off to target session.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Title:       "Handoff Log",
		ArtifactRef: artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   "agent-source",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-source")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-source",
		AgentID:     "agent-source",
		TaskID:      taskID,
		Summary:     "Source agent starts takeover flow",
		OwnerScope:  "task/session",
		HandoffTo:   "agent-target",
		RelatedDocKeys: []string{
			docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: artifactRef},
		},
	}); err != nil {
		t.Fatalf("record source session: %v", err)
	}
	if _, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:        workspaceID,
		SessionID:          "sess-source",
		SuccessorSessionID: "sess-target",
		TakeoverAgentID:    "agent-target",
		Summary:            "Handoff to fresh owner",
		SuccessorSummary:   "Target agent resumes work",
	}); err != nil {
		t.Fatalf("take over session: %v", err)
	}

	report, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       200,
	})
	if err != nil {
		t.Fatalf("build instrumentation report: %v", err)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected takeover instrumentation report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}

	taskCluster := requireProtoCluster(t, report, "task:"+workspaceID+"/"+taskID)
	if !containsString(taskCluster.SessionIDs, "sess-source") || !containsString(taskCluster.SessionIDs, "sess-target") {
		t.Fatalf("expected takeover cluster to include both source and successor sessions, got %+v", taskCluster)
	}
	if !containsString(taskCluster.DocKeys, docKey) || !containsString(taskCluster.ArtifactRefs, artifactRef) {
		t.Fatalf("expected takeover cluster to keep doc/artifact linkage, got %+v", taskCluster)
	}
	if taskCluster.Metrics.EventTypeCounts["session.takeover"] == 0 {
		t.Fatalf("expected takeover event to appear in task cluster metrics, got %+v", taskCluster.Metrics.EventTypeCounts)
	}
}

func TestRecordInstrumentationMetricSnapshotProducesRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-instrumentation-snapshot", "task-instrumentation-snapshot")

	report, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		Limit:        200,
		ClusterLimit: 5,
	})
	if err != nil {
		t.Fatalf("build instrumentation report: %v", err)
	}

	event, err := store.RecordInstrumentationMetricSnapshot(ctx, report, sqlite.InstrumentationSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("record instrumentation metric snapshot: %v", err)
	}
	if event.EventType != "cluster.metric_snapshot" {
		t.Fatalf("expected cluster.metric_snapshot event, got %+v", event)
	}
	if event.EntityType != "workspace" || event.EntityID != scenario.workspaceID {
		t.Fatalf("expected workspace snapshot entity, got %+v", event)
	}
	if event.ActorID != "dashboard" {
		t.Fatalf("expected snapshot actor dashboard, got %+v", event)
	}

	var payload struct {
		WorkspaceID   string                                 `json:"workspace_id"`
		TimeAuthority sqlite.WorkspaceTimeAuthority          `json:"time_authority"`
		Filter        sqlite.InstrumentationReportFilter     `json:"filter"`
		Workspace     sqlite.InstrumentationWorkspaceMetrics `json:"workspace"`
		Clusters      []sqlite.ProtoClusterReport            `json:"clusters"`
	}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode snapshot payload: %v", err)
	}
	if payload.WorkspaceID != scenario.workspaceID {
		t.Fatalf("expected snapshot payload workspace %s, got %+v", scenario.workspaceID, payload)
	}
	if payload.Filter.TaskID != scenario.taskID {
		t.Fatalf("expected snapshot payload task filter %s, got %+v", scenario.taskID, payload.Filter)
	}
	if payload.TimeAuthority.WorkspaceID != scenario.workspaceID || payload.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected snapshot payload to expose workspace time authority, got %+v", payload.TimeAuthority)
	}
	if len(payload.Clusters) != 1 {
		t.Fatalf("expected snapshot payload cluster limit to trim to 1, got %+v", payload.Clusters)
	}
	if payload.Clusters[0].ProtoClusterID != "task:"+scenario.workspaceID+"/"+scenario.taskID {
		t.Fatalf("expected snapshot payload to keep task cluster, got %+v", payload.Clusters)
	}
	if payload.Workspace.TotalClusters == 0 {
		t.Fatalf("expected snapshot payload workspace metrics, got %+v", payload.Workspace)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.metric_snapshot",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list snapshot runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one cluster.metric_snapshot runtime event, got %+v", events)
	}
	if events[0].EventID != event.EventID {
		t.Fatalf("expected persisted snapshot event %s, got %+v", event.EventID, events)
	}
}

func TestConfirmedTensionReadSideStaysAlignedWithProtoClusterMetrics(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-instrumentation-confirmed", "task-instrumentation-confirmed")

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to project at least one tension, got %+v", refresh)
	}

	tensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	bottleneck := requireSQLiteTensionRecordByType(t, tensions, "bottleneck")

	confirmed, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   bottleneck.TensionID,
		ActorID:     "operator",
		Reason:      "carry into control read-side",
	})
	if err != nil {
		t.Fatalf("confirm tension: %v", err)
	}
	if confirmed.Tension.ReviewStatus != "CONFIRMED" {
		t.Fatalf("expected confirmed tension review status, got %+v", confirmed.Tension)
	}

	beforeReport, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		Limit:        200,
		ClusterLimit: 5,
	})
	if err != nil {
		t.Fatalf("build instrumentation report before snapshot: %v", err)
	}

	snapshotEvent, err := store.RecordInstrumentationMetricSnapshot(ctx, beforeReport, sqlite.InstrumentationSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("record instrumentation metric snapshot: %v", err)
	}
	if snapshotEvent.EventType != "cluster.metric_snapshot" {
		t.Fatalf("expected cluster.metric_snapshot, got %+v", snapshotEvent)
	}

	afterReport, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		Limit:        200,
		ClusterLimit: 5,
	})
	if err != nil {
		t.Fatalf("build instrumentation report after snapshot: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	beforeCluster := requireProtoCluster(t, beforeReport, clusterID)
	afterCluster := requireProtoCluster(t, afterReport, clusterID)
	if beforeCluster.ProtoClusterID != confirmed.Tension.ProtoClusterID {
		t.Fatalf("expected confirmed tension to stay anchored to task proto cluster, got tension=%+v cluster=%+v", confirmed.Tension, beforeCluster)
	}
	if beforeCluster.Metrics.EventCount != afterCluster.Metrics.EventCount ||
		beforeCluster.Metrics.BlockerSignalCount != afterCluster.Metrics.BlockerSignalCount ||
		beforeCluster.Metrics.OpenQueueCount != afterCluster.Metrics.OpenQueueCount ||
		beforeCluster.Metrics.DuplicationSignalCount != afterCluster.Metrics.DuplicationSignalCount {
		t.Fatalf("expected task cluster metrics to stay stable across snapshot event, got before=%+v after=%+v", beforeCluster.Metrics, afterCluster.Metrics)
	}

	confirmedItems, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		ReviewStatus: "CONFIRMED",
		Limit:        20,
	})
	if err != nil {
		t.Fatalf("list confirmed tensions: %v", err)
	}
	if len(confirmedItems) != 1 || confirmedItems[0].TensionID != confirmed.Tension.TensionID {
		t.Fatalf("expected one confirmed tension in task view, got %+v", confirmedItems)
	}
	if confirmedItems[0].ProtoClusterID != beforeCluster.ProtoClusterID {
		t.Fatalf("expected confirmed tension and task cluster to share proto_cluster_id, got %+v and %+v", confirmedItems[0], beforeCluster)
	}

	var payload struct {
		Clusters []sqlite.ProtoClusterReport `json:"clusters"`
	}
	if err := json.Unmarshal([]byte(snapshotEvent.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode snapshot payload: %v", err)
	}
	if len(payload.Clusters) != 1 || payload.Clusters[0].ProtoClusterID != beforeCluster.ProtoClusterID {
		t.Fatalf("expected snapshot payload to keep confirmed tension cluster, got %+v", payload.Clusters)
	}

	confirmedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    confirmed.Tension.TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension.confirmed runtime events: %v", err)
	}
	if len(confirmedEvents) != 1 {
		t.Fatalf("expected one tension.confirmed runtime event, got %+v", confirmedEvents)
	}
	snapshotEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.metric_snapshot",
		EntityType:  "workspace",
		EntityID:    scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list cluster.metric_snapshot runtime events: %v", err)
	}
	if len(snapshotEvents) != 1 || snapshotEvents[0].EventID != snapshotEvent.EventID {
		t.Fatalf("expected one persisted snapshot runtime event, got %+v", snapshotEvents)
	}
}

func TestBuildInstrumentationReportSurfacesRoleLockIndexAndMissingComponents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-instrumentation-role-lock"
		taskID      = "task-instrumentation-role-lock"
		clusterID   = "task:ws-instrumentation-role-lock/task-instrumentation-role-lock"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Instrumentation Role Lock",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{"agent-steward", "agent-builder"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Role Lock Metrics",
		Description: "Role-lock metrics contract fixture",
	}, createSingleNodeGraph(taskID)); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-builder",
		Summary:     "building role-lock fixture",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-builder",
		AssignedTo:  "reviewer-a",
		Title:       "Approve role-lock gate",
		Description: "Blocking reviewer evidence for role-lock metrics",
		Blocking:    true,
	}); err != nil {
		t.Fatalf("create blocking human action: %v", err)
	}
	if _, err := store.ElectClusterSteward(ctx, sqlite.ElectStewardInput{
		ClusterID:   clusterID,
		EpochID:     "epoch-role-lock",
		CandidateID: "agent-steward",
		TTLSeconds:  60,
	}); err != nil {
		t.Fatalf("elect steward: %v", err)
	}

	report, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		Limit:        100,
		ClusterLimit: 5,
	})
	if err != nil {
		t.Fatalf("build instrumentation report: %v", err)
	}
	taskCluster := requireProtoCluster(t, report, clusterID)
	roleLock := taskCluster.Metrics.RoleLock
	if math.Abs(roleLock.Index-(5.0/6.0)) > 1e-9 {
		t.Fatalf("expected role-lock index 5/6 after motif reuse lands, got %+v", roleLock)
	}
	if roleLock.Partial {
		t.Fatalf("expected role-lock metrics to stop being partial once motif reuse is surfaced, got %+v", roleLock)
	}
	if len(roleLock.MissingComponents) != 0 {
		t.Fatalf("expected motif_reuse to drop from missing components, got %+v", roleLock.MissingComponents)
	}
	if roleLock.ActiveStewardCount != 1 || roleLock.ActiveClaimCount != 1 || roleLock.BlockingReviewCount != 1 {
		t.Fatalf("expected one active steward/claim/review signal, got %+v", roleLock)
	}
	if roleLock.StewardHHI != 1 || roleLock.AcceptedBuilderHHI != 1 || roleLock.DefaultReviewerHHI != 1 {
		t.Fatalf("expected single-source HHIs to be 1, got %+v", roleLock)
	}
	if math.Abs(roleLock.MotifReuseHHI-(1.0/3.0)) > 1e-9 {
		t.Fatalf("expected motif reuse HHI to reflect merged role ownership concentration, got %+v", roleLock)
	}
}

func createSingleNodeGraph(taskID string) dag.Graph {
	return dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}},
	})
}

func seedInstrumentationScenario(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) instrumentationScenario {
	t.Helper()

	scenario := instrumentationScenario{
		workspaceID:      workspaceID,
		taskID:           taskID,
		runbookDocKey:    "runbook",
		standaloneDocKey: "global-notes",
		artifactRef:      "artifact://deploy-log",
		primarySession:   "sess-a",
		secondarySession: "sess-b",
	}

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: scenario.workspaceID,
		Title:       "Instrumentation Scenario",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	claimExternalWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	for _, agentID := range []string{"agent-a", "agent-b", "agent-c"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: scenario.workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	createSingleNodeTask(t, ctx, store, scenario.taskID, "node-"+scenario.taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     "agent-a",
		Summary:     "fixture claim before task-bound instrumentation sessions",
	}); err != nil {
		t.Fatalf("claim instrumentation task: %v", err)
	}

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.runbookDocKey,
		Title:       "Runbook",
		Content:     "Main deploy runbook",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put task doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.standaloneDocKey,
		Title:       "Global Notes",
		Content:     "Standalone workspace doc",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put global doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Title:       "Deploy log",
		ArtifactRef: scenario.artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-b",
		UpdateType:  "status",
		Summary:     "Working from the runbook",
		PayloadJSON: `{"task_ids":["` + taskID + `"],"doc_keys":["runbook"],"artifacts":[{"ref":"artifact://deploy-log"}]}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.primarySession,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "Starting rollout",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			scenario.runbookDocKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
		},
	}); err != nil {
		t.Fatalf("record first session start: %v", err)
	}

	keepFalse := false
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventBlocked,
		WorkspaceID:       scenario.workspaceID,
		SessionID:         scenario.primarySession,
		AgentID:           "agent-a",
		TaskID:            scenario.taskID,
		Summary:           "Blocked waiting on operator approval",
		KeepSessionActive: &keepFalse,
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve deploy"},
		},
		RelatedDocKeys: []string{
			scenario.runbookDocKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
		},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue: %v", err)
	}

	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.secondarySession,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "Parallel verification pass",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			scenario.runbookDocKey,
		},
	}); err != nil {
		t.Fatalf("record second session start: %v", err)
	}

	for _, send := range []sqlite.MessageSendInput{
		{
			WorkspaceID: scenario.workspaceID,
			FromAgentID: "agent-a",
			ToAgentID:   "agent-b",
			Channel:     "ops",
			Content:     "Need approval status",
		},
		{
			WorkspaceID: scenario.workspaceID,
			FromAgentID: "agent-a",
			ToAgentID:   "agent-c",
			Channel:     "ops",
			Content:     "Need health check",
		},
	} {
		if _, err := store.SendMessage(ctx, send); err != nil {
			t.Fatalf("send message %+v: %v", send, err)
		}
	}

	if _, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     "agent-a",
		AssignedTo:  "reviewer-a",
		Title:       "Review deploy gate",
		Description: "Blocking approval for rollout",
		Blocking:    true,
	}); err != nil {
		t.Fatalf("create blocking human action: %v", err)
	}
	if _, err := store.ElectClusterSteward(ctx, sqlite.ElectStewardInput{
		ClusterID:   "task:" + scenario.workspaceID + "/" + scenario.taskID,
		EpochID:     "epoch-instrumentation-role-lock",
		CandidateID: "agent-a",
		TTLSeconds:  300,
	}); err != nil {
		t.Fatalf("elect cluster steward: %v", err)
	}

	return scenario
}

func requireProtoCluster(t *testing.T, report sqlite.InstrumentationReport, clusterID string) sqlite.ProtoClusterReport {
	t.Helper()
	for _, cluster := range report.Clusters {
		if cluster.ProtoClusterID == clusterID {
			return cluster
		}
	}
	t.Fatalf("proto cluster %s not found in %+v", clusterID, report.Clusters)
	return sqlite.ProtoClusterReport{}
}

func hasProtoCluster(clusters []sqlite.ProtoClusterReport, clusterID string) bool {
	for _, cluster := range clusters {
		if cluster.ProtoClusterID == clusterID {
			return true
		}
	}
	return false
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func protoClusterIDs(clusters []sqlite.ProtoClusterReport) []string {
	ids := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		ids = append(ids, cluster.ProtoClusterID)
	}
	return ids
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func requireSQLiteTensionRecordByType(t *testing.T, items []sqlite.TensionRecord, tensionType string) sqlite.TensionRecord {
	t.Helper()
	for _, item := range items {
		if item.TensionType == tensionType {
			return item
		}
	}
	t.Fatalf("tension with type %s not found in %+v", tensionType, items)
	return sqlite.TensionRecord{}
}
