package server

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type instrumentationRPCScenario struct {
	workspaceID     string
	primaryTaskID   string
	secondaryTaskID string
	runbookDocKey   string
	artifactRef     string
	sessionID       string
}

func TestWorkspaceInstrumentationRPCSurfaceAndFilters(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	rawReport, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.InstrumentationReport)
	if !ok {
		t.Fatalf("unexpected report payload type %T", reportPayload["report"])
	}
	if report.WorkspaceID != scenario.workspaceID {
		t.Fatalf("expected report workspace %s, got %+v", scenario.workspaceID, report)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected instrumentation report rpc to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected instrumentation report rpc generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.Replay.EventTypeCounts["agent_message.sent"] == 0 {
		t.Fatalf("expected unfiltered instrumentation report to include message activity, got %+v", report.Replay.EventTypeCounts)
	}

	rawFilteredReport, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.primaryTaskID,
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal filtered report params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationReport(ctx, rawFilteredReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationReport filtered rpc error: %+v", rpcErr)
	}
	reportPayload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected filtered report result type %T", result)
	}
	report, ok = reportPayload["report"].(sqlite.InstrumentationReport)
	if !ok {
		t.Fatalf("unexpected filtered report payload type %T", reportPayload["report"])
	}
	if report.Filter.TaskID != scenario.primaryTaskID {
		t.Fatalf("expected report task filter %s, got %+v", scenario.primaryTaskID, report.Filter)
	}

	taskCluster := requireServerProtoCluster(t, report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.primaryTaskID)
	if taskCluster.ResolutionKind != "task" {
		t.Fatalf("expected task resolution cluster, got %+v", taskCluster)
	}
	if !containsServerString(taskCluster.SessionIDs, scenario.sessionID) {
		t.Fatalf("expected task cluster to include session %s, got %+v", scenario.sessionID, taskCluster)
	}
	if !containsServerString(taskCluster.DocKeys, scenario.runbookDocKey) {
		t.Fatalf("expected task cluster doc refs to include %s, got %+v", scenario.runbookDocKey, taskCluster)
	}
	if !containsServerString(taskCluster.ArtifactRefs, scenario.artifactRef) {
		t.Fatalf("expected task cluster artifact refs to include %s, got %+v", scenario.artifactRef, taskCluster)
	}
	if taskCluster.Metrics.BlockerSignalCount == 0 {
		t.Fatalf("expected blocker signals on filtered task cluster, got %+v", taskCluster.Metrics)
	}
	if hasServerProtoCluster(report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.secondaryTaskID) {
		t.Fatalf("expected task filter to exclude secondary task cluster, got %+v", report.Clusters)
	}

	rawClusters, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.primaryTaskID,
		Limit:        100,
		ClusterLimit: 1,
	})
	if err != nil {
		t.Fatalf("marshal clusters params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationClusters(ctx, rawClusters)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationClusters rpc error: %+v", rpcErr)
	}
	clusterPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected clusters result type %T", result)
	}
	clusters, ok := clusterPayload["clusters"].([]sqlite.ProtoClusterReport)
	if !ok {
		t.Fatalf("unexpected clusters payload type %T", clusterPayload["clusters"])
	}
	authority, ok := clusterPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok {
		t.Fatalf("unexpected clusters time_authority payload type %T", clusterPayload["time_authority"])
	}
	if authority.WorkspaceID != scenario.workspaceID || authority.ReferenceAt == "" {
		t.Fatalf("expected instrumentation clusters rpc to expose workspace time authority, got %+v", authority)
	}
	if count, ok := clusterPayload["count"].(int); !ok || count != 1 {
		t.Fatalf("expected cluster_limit to trim to 1 cluster, got %#v", clusterPayload["count"])
	}
	if len(clusters) != 1 || clusters[0].ProtoClusterID != taskCluster.ProtoClusterID {
		t.Fatalf("expected filtered cluster list to keep task cluster, got %+v", clusters)
	}

	rawSnapshot, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.primaryTaskID,
		Limit:        100,
		ClusterLimit: 1,
		ActorID:      "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal snapshot params: %v", err)
	}
	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)
	result, rpcErr = h.workspaceInstrumentationSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.InstrumentationReport)
	if !ok {
		t.Fatalf("unexpected snapshot report payload type %T", snapshotPayload["report"])
	}
	if snapshotReport.TimeAuthority.WorkspaceID != scenario.workspaceID || snapshotReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected instrumentation snapshot report to expose workspace time authority, got %+v", snapshotReport.TimeAuthority)
	}
	if snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected instrumentation snapshot report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", snapshotReport.GeneratedAt, snapshotReport.TimeAuthority.ReferenceAt)
	}
	if snapshotReport.Filter.TaskID != scenario.primaryTaskID {
		t.Fatalf("expected snapshot report task filter %s, got %+v", scenario.primaryTaskID, snapshotReport.Filter)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected snapshot event payload type %T", snapshotPayload["event"])
	}
	if event.EventType != "cluster.metric_snapshot" {
		t.Fatalf("expected cluster.metric_snapshot event, got %+v", event)
	}
	if event.ActorID != "dashboard" {
		t.Fatalf("expected snapshot actor dashboard, got %+v", event)
	}
	liveEvent := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "cluster.metric_snapshot")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.metric_snapshot",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one cluster.metric_snapshot event, got %+v", events)
	}
}

func TestWorkspaceInstrumentationRPCSurfacesRoleLockMetricsAndMissingComponents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRoleLockScenario(t, ctx, store)

	rawReport, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.primaryTaskID,
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.InstrumentationReport)
	if !ok {
		t.Fatalf("unexpected report payload type %T", reportPayload["report"])
	}
	cluster := requireServerProtoCluster(t, report.Clusters, "task:"+scenario.workspaceID+"/"+scenario.primaryTaskID)
	roleLock := cluster.Metrics.RoleLock
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

	rawClusters, err := json.Marshal(workspaceInstrumentationParams{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.primaryTaskID,
		Limit:        100,
		ClusterLimit: 1,
	})
	if err != nil {
		t.Fatalf("marshal clusters params: %v", err)
	}
	clusterResult, rpcErr := h.workspaceInstrumentationClusters(ctx, rawClusters)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationClusters rpc error: %+v", rpcErr)
	}
	clusterPayload, ok := clusterResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected clusters result type %T", clusterResult)
	}
	clusters, ok := clusterPayload["clusters"].([]sqlite.ProtoClusterReport)
	if !ok || len(clusters) != 1 {
		t.Fatalf("unexpected clusters payload %+v", clusterPayload["clusters"])
	}
	if math.Abs(clusters[0].Metrics.RoleLock.Index-(5.0/6.0)) > 1e-9 || len(clusters[0].Metrics.RoleLock.MissingComponents) != 0 {
		t.Fatalf("expected clusters RPC to surface role-lock metrics, got %+v", clusters[0].Metrics.RoleLock)
	}
}

func TestWorkspaceInstrumentationRPCContractsRejectMissingWorkspaceID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationParams
	}{
		{name: "report", call: h.workspaceInstrumentationReport, params: workspaceInstrumentationParams{Limit: 50}},
		{name: "clusters", call: h.workspaceInstrumentationClusters, params: workspaceInstrumentationParams{Limit: 50, ClusterLimit: 5}},
		{name: "snapshot", call: h.workspaceInstrumentationSnapshot, params: workspaceInstrumentationParams{ActorID: "dashboard", Limit: 50, ClusterLimit: 5}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params error, got %+v", rpcErr)
			}
		})
	}
}

func seedInstrumentationRPCScenario(t *testing.T, ctx context.Context, store *sqlite.Store) instrumentationRPCScenario {
	t.Helper()

	scenario := instrumentationRPCScenario{
		workspaceID:     "ws-instrumentation-rpc",
		primaryTaskID:   "task-instrumentation-rpc",
		secondaryTaskID: "task-instrumentation-rpc-secondary",
		runbookDocKey:   "runbook",
		artifactRef:     "artifact://instrumentation-rpc",
		sessionID:       "sess-instrumentation-rpc",
	}

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: scenario.workspaceID,
		Title:       "Instrumentation RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: scenario.workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	for _, taskID := range []string{scenario.primaryTaskID, scenario.secondaryTaskID} {
		graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
		if err := dag.ValidateGraph(graph); err != nil {
			t.Fatalf("validate graph for %s: %v", taskID, err)
		}
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			TaskID:      taskID,
			OwnerUserID: "developer",
			Priority:    "normal",
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: scenario.workspaceID,
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		AgentID:     "agent-a",
		Summary:     "fixture claim before task-bound session",
	}); err != nil {
		t.Fatalf("claim primary task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.secondaryTaskID,
		AgentID:     "agent-b",
		Summary:     "fixture claim before task-bound session",
	}); err != nil {
		t.Fatalf("claim secondary task: %v", err)
	}

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.runbookDocKey,
		Title:       "Runbook",
		Content:     "Instrumentation RPC runbook",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
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
		Summary:     "Working from instrumentation runbook",
		PayloadJSON: `{"task_ids":["` + scenario.primaryTaskID + `"],"doc_keys":["` + scenario.runbookDocKey + `"],"artifacts":[{"ref":"` + scenario.artifactRef + `"}]}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}

	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.primaryTaskID,
		Summary:     "Start task-backed instrumentation flow",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			scenario.runbookDocKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
		},
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.primaryTaskID,
		Summary:     "Blocked on operator confirmation",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve deploy"},
		},
		RelatedDocKeys:      []string{scenario.runbookDocKey},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: scenario.artifactRef}},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue: %v", err)
	}
	if _, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: scenario.workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Channel:     "ops",
		Content:     "Need operator confirmation status",
	}); err != nil {
		t.Fatalf("send instrumentation message: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   "sess-secondary-rpc",
		AgentID:     "agent-b",
		TaskID:      scenario.secondaryTaskID,
		Summary:     "Secondary task activity",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record secondary task session start: %v", err)
	}

	return scenario
}

func seedInstrumentationRoleLockScenario(t *testing.T, ctx context.Context, store *sqlite.Store) instrumentationRPCScenario {
	t.Helper()

	scenario := instrumentationRPCScenario{
		workspaceID:     "ws-instrumentation-role-lock-rpc",
		primaryTaskID:   "task-instrumentation-role-lock-rpc",
		secondaryTaskID: "task-instrumentation-role-lock-rpc-secondary",
		runbookDocKey:   "runbook-role-lock",
		artifactRef:     "artifact://role-lock-rpc",
		sessionID:       "sess-role-lock-rpc",
	}

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: scenario.workspaceID,
		Title:       "Instrumentation Role Lock RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	for _, agentID := range []string{"agent-steward", "agent-builder", "reviewer-a"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: scenario.workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	for _, taskID := range []string{scenario.primaryTaskID, scenario.secondaryTaskID} {
		graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
		if err := dag.ValidateGraph(graph); err != nil {
			t.Fatalf("validate graph for %s: %v", taskID, err)
		}
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			TaskID:      taskID,
			OwnerUserID: "developer",
			Priority:    "normal",
			Title:       taskID,
			Description: "role-lock fixture",
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: scenario.workspaceID,
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		AgentID:     "agent-builder",
		Summary:     "role-lock claim evidence",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		AgentID:     "agent-builder",
		AssignedTo:  "reviewer-a",
		Title:       "Approve role-lock gate",
		Description: "Blocking reviewer evidence for role-lock metrics",
		Blocking:    true,
	}); err != nil {
		t.Fatalf("create blocking human action: %v", err)
	}
	if _, err := store.ElectClusterSteward(ctx, sqlite.ElectStewardInput{
		ClusterID:   "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID,
		EpochID:     "epoch-role-lock-rpc",
		CandidateID: "agent-steward",
		TTLSeconds:  60,
	}); err != nil {
		t.Fatalf("elect steward: %v", err)
	}

	return scenario
}

func requireServerProtoCluster(t *testing.T, clusters []sqlite.ProtoClusterReport, clusterID string) sqlite.ProtoClusterReport {
	t.Helper()
	for _, cluster := range clusters {
		if cluster.ProtoClusterID == clusterID {
			return cluster
		}
	}
	t.Fatalf("proto cluster %s not found in %+v", clusterID, clusters)
	return sqlite.ProtoClusterReport{}
}

func hasServerProtoCluster(clusters []sqlite.ProtoClusterReport, clusterID string) bool {
	for _, cluster := range clusters {
		if cluster.ProtoClusterID == clusterID {
			return true
		}
	}
	return false
}

func containsServerString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
