package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestSnapshotRSPForecastReportRejectsMissingWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "d2a-rsp-forecast-missing-authority")
	claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPForecastSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)

	_, err := store.SnapshotRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	assertNoRSPSnapshotRuntimeEvents(t, ctx, store, scenario.workspaceID, "rsp.forecast_snapshot", "rsp_forecast")
	if got := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID); got != beforeRejects {
		t.Fatalf("expected missing authority reject not to fabricate authority.rejected evidence without expected term, before=%d after=%d", beforeRejects, got)
	}
}

func TestSnapshotRSPForecastReportRejectsStaleWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "d2a-rsp-forecast-stale-authority")
	current := claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPForecastSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)
	beforeRejects := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)
	transferRSPSnapshotWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-4201")

	_, err := store.SnapshotRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	assertNoRSPSnapshotRuntimeEvents(t, ctx, store, scenario.workspaceID, "rsp.forecast_snapshot", "rsp_forecast")
	assertRSPSnapshotAuthorityRejectEventIncrement(t, ctx, store, scenario.workspaceID, beforeRejects, AuthorityRejectStale)
}

func TestSnapshotRSPForecastReportCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "d2a-rsp-forecast-authority-metadata")
	authority := claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPForecastSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)

	result, err := store.SnapshotRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("snapshot rsp forecast report: %v", err)
	}
	assertRSPSnapshotAuthorityMetadata(t, result.Event, authority)

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "rsp.forecast_snapshot",
		EntityType:  "rsp_forecast",
		EntityID:    result.Event.EntityID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list rsp forecast snapshot runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one rsp forecast snapshot runtime event, got %+v", events)
	}
	assertRSPSnapshotAuthorityMetadata(t, events[0], authority)
}

func TestSnapshotRSPBeliefReportRejectsMissingWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "d2a-rsp-belief-missing-authority")
	claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPBeliefSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)
	recordRSPBeliefAuthorityClaim(t, ctx, store, scenario.workspaceID, scenario.taskID, "claim-d2a-rsp-belief-missing-authority", scenario.docKey)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)

	_, err := store.SnapshotRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	assertNoRSPSnapshotRuntimeEvents(t, ctx, store, scenario.workspaceID, "rsp.belief_snapshot", "rsp_belief")
	if got := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID); got != beforeRejects {
		t.Fatalf("expected missing authority reject not to fabricate authority.rejected evidence without expected term, before=%d after=%d", beforeRejects, got)
	}
}

func TestSnapshotRSPBeliefReportRejectsStaleWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "d2a-rsp-belief-stale-authority")
	current := claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPBeliefSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)
	recordRSPBeliefAuthorityClaim(t, ctx, store, scenario.workspaceID, scenario.taskID, "claim-d2a-rsp-belief-stale-authority", scenario.docKey)
	beforeRejects := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)
	transferRSPSnapshotWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-4202")

	_, err := store.SnapshotRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	assertNoRSPSnapshotRuntimeEvents(t, ctx, store, scenario.workspaceID, "rsp.belief_snapshot", "rsp_belief")
	assertRSPSnapshotAuthorityRejectEventIncrement(t, ctx, store, scenario.workspaceID, beforeRejects, AuthorityRejectStale)
}

func TestSnapshotRSPBeliefReportCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "d2a-rsp-belief-authority-metadata")
	authority := claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPBeliefSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)
	recordRSPBeliefAuthorityClaim(t, ctx, store, scenario.workspaceID, scenario.taskID, "claim-d2a-rsp-belief-authority-metadata", scenario.docKey)

	result, err := store.SnapshotRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
	})
	if err != nil {
		t.Fatalf("snapshot rsp belief report: %v", err)
	}
	assertRSPSnapshotAuthorityMetadata(t, result.Event, authority)

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "rsp.belief_snapshot",
		EntityType:  "rsp_belief",
		EntityID:    result.Event.EntityID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list rsp belief snapshot runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one rsp belief snapshot runtime event, got %+v", events)
	}
	assertRSPSnapshotAuthorityMetadata(t, events[0], authority)
}

func TestSnapshotRSPStateReportRejectsMissingWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "d2a-rsp-state-missing-authority")
	claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPStateSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)

	_, err := store.SnapshotRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	assertNoRSPSnapshotRuntimeEvents(t, ctx, store, scenario.workspaceID, "rsp.state_snapshot", "rsp_state")
	if got := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID); got != beforeRejects {
		t.Fatalf("expected missing authority reject not to fabricate authority.rejected evidence without expected term, before=%d after=%d", beforeRejects, got)
	}
}

func TestSnapshotRSPStateReportRejectsStaleWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "d2a-rsp-state-stale-authority")
	current := claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPStateSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)
	beforeRejects := countRSPSnapshotAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)
	transferRSPSnapshotWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-4203")

	_, err := store.SnapshotRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	assertNoRSPSnapshotRuntimeEvents(t, ctx, store, scenario.workspaceID, "rsp.state_snapshot", "rsp_state")
	assertRSPSnapshotAuthorityRejectEventIncrement(t, ctx, store, scenario.workspaceID, beforeRejects, AuthorityRejectStale)
}

func TestSnapshotRSPStateReportCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "d2a-rsp-state-authority-metadata")
	authority := claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	enableRSPStateSnapshotAuthorityCapability(t, ctx, store, scenario.workspaceID)

	result, err := store.SnapshotRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("snapshot rsp state report: %v", err)
	}
	assertRSPSnapshotAuthorityMetadata(t, result.Event, authority)

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "rsp.state_snapshot",
		EntityType:  "rsp_state",
		EntityID:    result.Event.EntityID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list rsp state snapshot runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one rsp state snapshot runtime event, got %+v", events)
	}
	assertRSPSnapshotAuthorityMetadata(t, events[0], authority)
}

func enableRSPForecastSnapshotAuthorityCapability(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  rspCapabilityForecastShadow,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable forecast snapshot authority tests",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable forecast shadow capability: %v", err)
	}
}

func enableRSPBeliefSnapshotAuthorityCapability(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  rspCapabilityBeliefLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable belief snapshot authority tests",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable belief live capability: %v", err)
	}
}

func enableRSPStateSnapshotAuthorityCapability(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  rspCapabilityStateShadow,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable state snapshot authority tests",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable state shadow capability: %v", err)
	}
}

func recordRSPBeliefAuthorityClaim(t *testing.T, ctx context.Context, store *Store, workspaceID, taskID, claimID, docKey string) {
	t.Helper()

	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ClaimType:   "FACT",
		Status:      "CONFIRMED",
		Subject:     "RSP belief authority target",
		Body:        "This claim exists to exercise belief snapshot authority enforcement.",
		Summary:     "RSP belief authority target.",
		Confidence:  0.93,
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		TaskID:      taskID,
	}); err != nil {
		t.Fatalf("record belief authority claim: %v", err)
	}
}

func transferRSPSnapshotWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
	t.Helper()

	referenceAt := time.Now().UTC().Round(0)
	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head before transfer: %v", err)
	}
	commitWatermark := current.CommitWatermark + 1
	if journalHead > commitWatermark {
		commitWatermark = journalHead
	}
	appliedWatermark := current.AppliedWatermark + 1
	if appliedWatermark > commitWatermark {
		appliedWatermark = commitWatermark
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        authorityScopeWorkspace,
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    authorityActorTypeSystem,
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}

func countRSPSnapshotAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND entity_type = 'workspace_authority'`,
		workspaceID,
		AuthorityEventRejected,
	).Scan(&count); err != nil {
		t.Fatalf("count authority rejected runtime events: %v", err)
	}
	return count
}

func assertRSPSnapshotAuthorityRejectEventIncrement(t *testing.T, ctx context.Context, store *Store, workspaceID string, before int, wantRejectCode AuthorityRejectCode) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority rejected runtime events: %v", err)
	}
	if len(events) != before+1 {
		t.Fatalf("expected authority.rejected count to grow from %d to %d, got %+v", before, before+1, events)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority rejected payload: %v", err)
	}
	if got, _ := payload["reject_code"].(string); got != string(wantRejectCode) {
		t.Fatalf("expected authority reject code %q, got payload %+v", wantRejectCode, payload)
	}
}

func assertNoRSPSnapshotRuntimeEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, eventType, entityType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  entityType,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events: %v", eventType, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events, got %+v", eventType, events)
	}
}

func assertRSPSnapshotAuthorityMetadata(t *testing.T, event RuntimeEventRecord, authority WorkspaceAuthorityRecord) {
	t.Helper()

	if event.AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %+v", authority.HolderAuthorityNodeID, event)
	}
	if event.AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %+v", authority.Term, event)
	}
	if event.AuthorityLeaseTokenFingerprint != authorityLeaseTokenFingerprint(authority.LeaseToken) {
		t.Fatalf("expected authority lease fingerprint %q, got %+v", authorityLeaseTokenFingerprint(authority.LeaseToken), event)
	}
}
