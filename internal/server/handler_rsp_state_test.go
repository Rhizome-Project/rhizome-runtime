package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceRSPStateReportAndSnapshot(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "rsp-state")
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	reportAny, rpcErr := callWorkspaceRSPStateReportRaw(t, h, ctx, mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPStateReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.RSPStateReport)
	if !report.Resolved || report.SignalType != "AGENT_STATE_POSTERIOR" {
		t.Fatalf("unexpected rsp state report %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp state rpc to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp state rpc generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.StateRationale == "" || len(report.StateDriverHints) == 0 {
		t.Fatalf("expected rsp state rpc to surface bounded rationale hints, got %+v", report)
	}
	if !report.ShadowMode || report.Calibration.Status != "SHADOW_ONLY" {
		t.Fatalf("expected rsp state rpc to stay shadow-only, got %+v", report)
	}
	for _, needle := range []string{"authority", "eligible", "actuat"} {
		if strings.Contains(strings.ToLower(report.Summary), needle) || strings.Contains(strings.ToLower(report.StateRationale), needle) {
			t.Fatalf("expected rsp state rpc payload to stay inspectability-only, got summary=%q rationale=%q", report.Summary, report.StateRationale)
		}
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	snapshotAny, rpcErr := callWorkspaceRSPStateSnapshotRaw(t, h, ctx, mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPStateSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload := snapshotAny.(map[string]any)
	snapshotReport := snapshotPayload["report"].(sqlite.RSPStateReport)
	if snapshotReport.TimeAuthority.WorkspaceID != scenario.workspaceID || snapshotReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp state snapshot rpc to expose workspace time authority, got %+v", snapshotReport.TimeAuthority)
	}
	if snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp state snapshot rpc generated_at %q to mirror authority reference_at %q", snapshotReport.GeneratedAt, snapshotReport.TimeAuthority.ReferenceAt)
	}
	if snapshotReport.StateRationale == "" || len(snapshotReport.StateDriverHints) == 0 {
		t.Fatalf("expected rsp state snapshot rpc to surface bounded rationale hints, got %+v", snapshotReport)
	}
	if !snapshotReport.ShadowMode || snapshotReport.Calibration.Status != "SHADOW_ONLY" {
		t.Fatalf("expected rsp state snapshot rpc to stay shadow-only, got %+v", snapshotReport)
	}
	for _, needle := range []string{"authority", "eligible", "actuat"} {
		if strings.Contains(strings.ToLower(snapshotReport.Summary), needle) || strings.Contains(strings.ToLower(snapshotReport.StateRationale), needle) {
			t.Fatalf("expected rsp state snapshot rpc payload to stay inspectability-only, got summary=%q rationale=%q", snapshotReport.Summary, snapshotReport.StateRationale)
		}
	}
	event := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if event.EventType != "rsp.state_snapshot" || event.EntityType != "rsp_state" {
		t.Fatalf("unexpected rsp state snapshot event %+v", event)
	}
	expectMemoryInvalidationEvent(t, ch, "rsp.state_snapshot")
}

func TestWorkspaceRSPStateReportRequiresWorkspaceID(t *testing.T) {
	t.Parallel()

	h := NewHandler(newServerTestStore(t))
	if _, rpcErr := h.workspaceRSPStateReport(context.Background(), mustJSONRaw(workspaceRSPStateReportParams{
		AgentID: "agent-a",
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected missing workspace_id invalid params error, got %+v", rpcErr)
	}
}

func TestWorkspaceRSPStateReportSurfacesHealthyExplorationViability(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "rsp-state-productive-exploration")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO workspace_cluster_control_state(
			workspace_id, proto_cluster_id, resolution_kind, corridor_profile, epoch,
			current_mode, candidate_mode, candidate_streak, attention_band, pressure_score,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, agent_ids_json,
			summary, last_basis_at, last_tick_at, last_transition_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workspace_id, proto_cluster_id) DO UPDATE SET
			resolution_kind=excluded.resolution_kind,
			corridor_profile=excluded.corridor_profile,
			epoch=excluded.epoch,
			current_mode=excluded.current_mode,
			candidate_mode=excluded.candidate_mode,
			candidate_streak=excluded.candidate_streak,
			attention_band=excluded.attention_band,
			pressure_score=excluded.pressure_score,
			task_ids_json=excluded.task_ids_json,
			session_ids_json=excluded.session_ids_json,
			doc_keys_json=excluded.doc_keys_json,
			artifact_refs_json=excluded.artifact_refs_json,
			agent_ids_json=excluded.agent_ids_json,
			summary=excluded.summary,
			last_basis_at=excluded.last_basis_at,
			last_tick_at=excluded.last_tick_at,
			last_transition_at=excluded.last_transition_at,
			updated_at=excluded.updated_at`,
		scenario.workspaceID, clusterID, "proto_cluster", "exploration", 1,
		"UNFREEZE", "UNFREEZE", 2, "WATCH", 6,
		`["`+scenario.taskID+`"]`, `["`+scenario.sessionID+`"]`, `["`+scenario.docKey+`"]`, `["`+scenario.artifactRef+`"]`, `["`+scenario.agentID+`"]`,
		"bounded productive exploration fixture", now, now, now, now, now,
	); err != nil {
		t.Fatalf("insert control-state fixture: %v", err)
	}

	reportAny, rpcErr := callWorkspaceRSPStateReportRaw(t, h, ctx, mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPStateReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.RSPStateReport)
	if report.ExplorationViability != "HEALTHY" {
		t.Fatalf("expected productive exploration rpc path to surface healthy viability, got %+v", report)
	}
	if len(report.ExplorationSuppressionReasons) != 0 {
		t.Fatalf("expected productive exploration rpc path not to surface suppression reasons, got %+v", report)
	}
}

func TestWorkspaceRSPStateReportSurfacesLocalAutonomicsCandidates(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "rsp-state-local-autonomics")
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.safe_local_autonomics.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable safe local autonomics for handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable safe local autonomics capability: %v", err)
	}
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	for i := 0; i < 2; i++ {
		if _, err := store.TickClusterControlState(ctx, sqlite.ClusterControlTickInput{
			WorkspaceID:    scenario.workspaceID,
			ProtoClusterID: clusterID,
			ActorID:        "tests",
		}); err != nil {
			t.Fatalf("tick cluster control state %d: %v", i+1, err)
		}
	}
	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            scenario.agentID,
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-handler-rsp-state-local-autonomics",
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

	reportAny, rpcErr := callWorkspaceRSPStateReportRaw(t, h, ctx, mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPStateReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.RSPStateReport)
	if !report.CapabilityFlags.SafeLocalAutonomicsLive {
		t.Fatalf("expected rsp state rpc to expose safe local autonomics capability, got %+v", report.CapabilityFlags)
	}
	if !report.ShadowMode || report.Calibration.Status != "SHADOW_ONLY" {
		t.Fatalf("expected rsp state rpc to stay shadow-only even when local autonomics candidates appear, got %+v", report)
	}
	if len(report.LocalAutonomicsCandidates) < 2 {
		t.Fatalf("expected rsp state rpc to surface bounded local autonomics candidates, got %+v", report)
	}
	foundFlush := false
	foundRefresh := false
	openedGate := false
	for _, candidate := range report.LocalAutonomicsCandidates {
		if !candidate.BoundedLocal || !candidate.Reversible || candidate.SharedTruthMutation {
			t.Fatalf("expected rpc local autonomics candidate to stay bounded/local/reversible, got %+v", candidate)
		}
		if candidate.GateOpen {
			if candidate.ObserveOnlyReason != "canonical_command_path_pending" {
				t.Fatalf("expected gate-open rpc local autonomics candidates to stay observe-only until canonical command path exists, got %+v", report.LocalAutonomicsCandidates)
			}
		} else if candidate.ObserveOnlyReason != "" {
			t.Fatalf("expected below-gate rpc local autonomics candidates not to advertise a command-path reason, got %+v", report.LocalAutonomicsCandidates)
		}
		if strings.Contains(strings.ToLower(candidate.Summary), "authority") || strings.Contains(strings.ToLower(candidate.Summary), "eligible") || strings.Contains(strings.ToLower(candidate.Summary), "actuat") {
			t.Fatalf("expected local autonomics candidate summary to stay observe-only, got %+v", candidate)
		}
		if candidate.GateOpen {
			openedGate = true
		}
		switch candidate.Command {
		case "agent.control.flush_cache":
			foundFlush = true
		case "agent.control.refresh_kernel":
			foundRefresh = true
		}
	}
	if !foundFlush || !foundRefresh {
		t.Fatalf("expected both bounded local autonomics commands in rpc surface, got %+v", report.LocalAutonomicsCandidates)
	}
	if !openedGate {
		t.Fatalf("expected risky rpc fixture to open at least one bounded local gate, got %+v", report.LocalAutonomicsCandidates)
	}
}

func TestWorkspaceRSPStateReportSurfacesGovernedHintInspectability(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable governed hints capability: %v", err)
	}
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	for i := 0; i < 2; i++ {
		if _, err := store.TickClusterControlState(ctx, sqlite.ClusterControlTickInput{
			WorkspaceID:    scenario.workspaceID,
			ProtoClusterID: clusterID,
			ActorID:        "tests",
		}); err != nil {
			t.Fatalf("tick cluster control state %d: %v", i+1, err)
		}
	}
	if _, err := store.ReportMemoryMetrics(ctx, sqlite.MemoryMetricsReportInput{
		WorkspaceID:        scenario.workspaceID,
		AgentID:            "agent-a",
		SessionID:          scenario.sessionID,
		ReportScope:        "SESSION",
		ReportID:           "memmet-handler-rsp-state-governed-hints",
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
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-handler-rsp-governed-lineage",
		WorkspaceID:       scenario.workspaceID,
		EventType:         "tests.handler.rsp.governed_hint_lineage",
		EntityType:        "test_scope",
		EntityID:          clusterID,
		ActorType:         "tester",
		ActorID:           "tester",
		RootCauseID:       "RC-handler-rsp-governed",
		ProvenanceGroupID: "PG-handler-rsp-governed",
		PayloadJSON:       `{}`,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record governed hint lineage event: %v", err)
	}

	reportAny, rpcErr := callWorkspaceRSPStateReportRaw(t, h, ctx, mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       "agent-a",
		TaskID:        scenario.primaryTaskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.runbookDocKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPStateReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.RSPStateReport)
	if !report.CapabilityFlags.GovernedHintsLive {
		t.Fatalf("expected rsp state rpc to expose governed hint capability, got %+v", report.CapabilityFlags)
	}
	if len(report.GovernedHints) == 0 {
		t.Fatalf("expected rsp state rpc to surface governed hints, got %+v", report)
	}
	sawInspectableHint := false
	for _, hint := range report.GovernedHints {
		if hint.RecommendationClass != "" && hint.EvidenceDiversity > 0 && hint.EvidenceDiversityBand != "" && hint.EvidenceSourceMix != "" && hint.RuntimeLineageBasis != "" && hint.TTLWindowState != "" && len(hint.EvidenceSourceKinds) > 0 && hint.Summary != "" {
			sawInspectableHint = true
			break
		}
	}
	if !sawInspectableHint {
		t.Fatalf("expected rsp state rpc to expose governed-hint inspectability fields, got %+v", report.GovernedHints)
	}
	if report.GovernedHintSummary == nil || report.GovernedHintSummary.TotalHints != len(report.GovernedHints) || len(report.GovernedHintSummary.RecommendationClassCount) == 0 {
		t.Fatalf("expected rsp state rpc to expose governed-hint summary rollup, got %+v", report.GovernedHintSummary)
	}
}
