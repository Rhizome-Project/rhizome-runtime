package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestEmitGlobalAnomalyAlertRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-anomaly-missing-authority"
	seedRSPAutonomousAuthorityFixture(t, ctx, store, workspaceID, "agent-a")
	beforeRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID)

	fh := NewRSPFirehose(store)
	fh.emitGlobalAnomalyAlert(ctx, workspaceID, "agent-a", "entity-a", "MOTIF_THRASH", "missing authority should fail closed")

	assertNoRSPRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, "ANOMALY_ALERT", "entity-a")
	if afterRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID); afterRejects != beforeRejects {
		t.Fatalf("expected missing-authority anomaly emit to avoid authority.rejected journaling, before=%d after=%d", beforeRejects, afterRejects)
	}
}

func TestEmitGlobalAnomalyAlertRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-anomaly-stale-authority"
	seedRSPAutonomousAuthorityFixture(t, ctx, store, workspaceID, "agent-a")
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2901")

	fh := NewRSPFirehose(store)
	fh.emitGlobalAnomalyAlert(ctx, workspaceID, "agent-a", "entity-a", "MOTIF_THRASH", "stale authority should fail closed")

	assertNoRSPRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, "ANOMALY_ALERT", "entity-a")
	if afterRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID); afterRejects != beforeRejects+1 {
		t.Fatalf("expected stale-authority anomaly emit to journal one authority.rejected event, before=%d after=%d", beforeRejects, afterRejects)
	}
}

func TestEmitLocalControlEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-local-control-metadata"
	seedRSPAutonomousAuthorityFixture(t, ctx, store, workspaceID, "agent-a")
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	actuator := NewRSPLocalActuator(store)
	actuator.emitLocalControlEvent(ctx, workspaceID, "agent-a", "agent.control.refresh_kernel", "authority metadata regression")

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.control.refresh_kernel",
		EntityType:  "AGENT",
		EntityID:    "agent-a",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list local control runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one local control runtime event, got %+v", events)
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestEmitLocalControlEventRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-local-control-missing-authority"
	seedRSPAutonomousAuthorityFixture(t, ctx, store, workspaceID, "agent-a")
	beforeRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID)

	actuator := NewRSPLocalActuator(store)
	actuator.emitLocalControlEvent(ctx, workspaceID, "agent-a", "agent.control.refresh_kernel", "missing authority should fail closed")

	assertNoRSPRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, "agent.control.refresh_kernel", "agent-a")
	if afterRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID); afterRejects != beforeRejects {
		t.Fatalf("expected missing-authority local control emit to avoid authority.rejected journaling, before=%d after=%d", beforeRejects, afterRejects)
	}
}

func TestEmitLocalControlEventRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-local-control-stale-authority"
	seedRSPAutonomousAuthorityFixture(t, ctx, store, workspaceID, "agent-a")
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2902")

	actuator := NewRSPLocalActuator(store)
	actuator.emitLocalControlEvent(ctx, workspaceID, "agent-a", "agent.control.refresh_kernel", "stale authority should fail closed")

	assertNoRSPRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, "agent.control.refresh_kernel", "agent-a")
	if afterRejects := countRSPAutonomousAuthorityRejectEvents(t, ctx, store, workspaceID); afterRejects != beforeRejects+1 {
		t.Fatalf("expected stale-authority local control emit to journal one authority.rejected event, before=%d after=%d", beforeRejects, afterRejects)
	}
}

func seedRSPAutonomousAuthorityFixture(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Autonomous Authority",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tester",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
}

func countRSPAutonomousAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority reject events: %v", err)
	}
	return len(events)
}

func assertNoRSPRuntimeEventsForAuthorityReject(t *testing.T, ctx context.Context, store *Store, workspaceID, eventType, entityID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityID:    entityID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events: %v", eventType, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events after authority reject, got %+v", eventType, events)
	}
}

func transferWorkspaceAuthorityToExternalPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
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
		Scope:                        "workspace",
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    "system",
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}

func assertRuntimeEventAuthorityMetadata(t *testing.T, event RuntimeEventRecord, authority WorkspaceAuthorityRecord) {
	t.Helper()

	if event.AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %q", authority.HolderAuthorityNodeID, event.AuthorityHolderNodeID)
	}
	if event.AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %d", authority.Term, event.AuthorityTerm)
	}
	if event.AuthorityLeaseTokenFingerprint == "" {
		t.Fatalf("expected authority lease fingerprint, got %+v", event)
	}
}
