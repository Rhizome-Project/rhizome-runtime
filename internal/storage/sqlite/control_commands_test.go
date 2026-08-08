package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRequestControlCommandRecordsRefreshRequestWithoutLegacyRuntimeAlias(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-control-command-refresh"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Refresh",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, event, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandRefreshKernel,
		AgentID:     "agent-refresh",
		Reason:      "operator requested bounded refresh",
		RequestedBy: "operator-a",
	})
	if err != nil {
		t.Fatalf("request refresh control command: %v", err)
	}
	if record.CommandType != ControlCommandRefreshKernel || record.AppliedInline {
		t.Fatalf("unexpected refresh command record %+v", record)
	}
	if record.Ownership.ActuatorOwner != "RMP" || record.Ownership.RMP != controlOwnershipRoleActuatorOwner || record.Ownership.RSP != controlOwnershipRoleAdvisoryOnly || record.Ownership.ApplyMode != controlApplyModeJournalOnly {
		t.Fatalf("expected refresh command ownership matrix, got %+v", record.Ownership)
	}
	if event.EventType != controlCommandEventType || event.EntityType != controlCommandEntityType || event.EntityID != record.CommandID {
		t.Fatalf("unexpected refresh runtime event %+v", event)
	}
	if legacy, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   ControlCommandRefreshKernel,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list legacy refresh events: %v", err)
	} else if len(legacy) != 0 {
		t.Fatalf("expected refresh requests to stay on canonical control.command.requested path, got %+v", legacy)
	}
}

func TestSetRSPCapabilityFlagsUsesCanonicalCapabilityPolicyJournal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-rsp-capability-flags-canonical"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Capability Canonical",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	flags, err := store.SetRSPCapabilityFlags(ctx, SetRSPCapabilityFlagsInput{
		WorkspaceID:       workspaceID,
		GovernedHintsLive: boolPtr(true),
		ForecastShadow:    boolPtr(true),
		UpdatedBy:         "operator-a",
		Reason:            "roll forward under canonical policy journal",
	})
	if err != nil {
		t.Fatalf("set rsp capability flags: %v", err)
	}
	if !flags.GovernedHintsLive || !flags.ForecastShadow {
		t.Fatalf("expected updated flags, got %+v", flags)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list capability policy runtime events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected exactly two canonical capability policy runtime events, got %+v", events)
	}
	for _, event := range events {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode capability policy payload: %v", err)
		}
		ownership, ok := payload["ownership"].(map[string]any)
		if !ok {
			t.Fatalf("expected ownership metadata in capability policy payload, got %+v", payload)
		}
		if ownership["actuator_owner"] != "RSP" || ownership["rsp"] != controlOwnershipRoleActuatorOwner || ownership["rrp"] != controlOwnershipRoleNone || ownership["rmp"] != controlOwnershipRoleNone {
			t.Fatalf("expected rsp capability policy ownership metadata, got %+v", ownership)
		}
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func TestRequestControlCommandRecordsParentRefsWhenProvided(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-control-command-parent-refs"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Parents",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	parentA, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-control-command-parent-a",
		WorkspaceID: workspaceID,
		EventType:   "legacy.signal",
		EntityType:  "legacy_event",
		EntityID:    "legacy-parent-a",
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{"message":"parent a"}`,
		CreatedAt:   "2026-04-08T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("record parent runtime event a: %v", err)
	}
	parentB, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-control-command-parent-b",
		WorkspaceID: workspaceID,
		EventType:   "legacy.signal",
		EntityType:  "legacy_event",
		EntityID:    "legacy-parent-b",
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{"message":"parent b"}`,
		CreatedAt:   "2026-04-08T00:01:00Z",
	})
	if err != nil {
		t.Fatalf("record parent runtime event b: %v", err)
	}

	record, event, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID:    workspaceID,
		CommandType:    ControlCommandClusterModeSwitch,
		ProtoClusterID: "cluster-a",
		TargetMode:     clusterControlModeUnfreeze,
		Reason:         "bounded operator escalation",
		RequestedBy:    "operator-a",
		ParentRefs:     []string{parentB.EventID, parentA.EventID, parentA.EventID},
	})
	if err != nil {
		t.Fatalf("request control command with parent refs: %v", err)
	}
	if len(record.ParentRefs) != 2 || record.ParentRefs[0] != parentA.EventID || record.ParentRefs[1] != parentB.EventID {
		t.Fatalf("expected unique sorted parent refs, got %+v", record.ParentRefs)
	}
	if event.ParentRefsJSON != `["rtev-control-command-parent-a","rtev-control-command-parent-b"]` {
		t.Fatalf("expected normalized parent refs json, got %+v", event)
	}
}

func TestRequestControlCommandRejectsMissingExcludeTTL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-control-command-invalid"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Invalid",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, _, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandExcludeAgentTension,
		TensionID:   "tension-a",
		AgentID:     "agent-a",
		RequestedBy: "tester",
	}); err == nil {
		t.Fatal("expected ttl validation error for tension exclusion command")
	}
}

func TestRequestControlCommandRejectsUnsupportedActorType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-control-command-actor-type"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Actor Type",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, _, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandRefreshKernel,
		AgentID:     "agent-a",
		Reason:      "bad actor type should fail closed",
		RequestedBy: "tester",
		ActorType:   "rsp",
	}); err == nil {
		t.Fatal("expected actor_type validation error for rsp control command request")
	}
}

func TestRequestControlCommandExclusionUsesWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-control-command-exclusion"
	tensionID := "tension:task:" + workspaceID + "/task-a"
	agentID := "agent-alpha"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Exclusion",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	referenceAt := time.Now().UTC().Add(2 * time.Minute).Round(0)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       tensionID,
		WorkspaceID:     workspaceID,
		ProtoClusterID:  "task:" + workspaceID + "/task-a",
		TensionType:     "bottleneck",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Control command exclusion tension",
		Summary:         "Ensures command exclusions follow workspace authority",
		AnchorKind:      "entity_id",
		AnchorRef:       "task-a",
		TaskIDs:         []string{"task-a"},
		BaseScore:       1,
		SurfaceScore:    1,
		EvidenceCount:   1,
		LastSeenEventID: "evt-control-command-exclusion",
		LastSeenAt:      referenceAt.Format(time.RFC3339Nano),
		ConfirmedBy:     "tester",
		CreatedAt:       referenceAt.Format(time.RFC3339Nano),
		UpdatedAt:       referenceAt.Format(time.RFC3339Nano),
	})
	setWorkspaceControlEpochAnchor(t, store, workspaceID, referenceAt.Format(time.RFC3339Nano))

	record, event, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandExcludeAgentTension,
		TensionID:   tensionID,
		AgentID:     agentID,
		TTLSeconds:  60,
		Reason:      "thrash",
		RequestedBy: "tester",
		ActorType:   "system",
	})
	if err != nil {
		t.Fatalf("request exclusion control command: %v", err)
	}
	wantExpiresAt := referenceAt.Add(time.Minute).Format(time.RFC3339Nano)
	if !record.AppliedInline || record.ExpiresAt != wantExpiresAt {
		t.Fatalf("unexpected exclusion command record %+v", record)
	}
	if record.Ownership.ActuatorOwner != "RRP" || record.Ownership.RRP != controlOwnershipRoleActuatorOwner || record.Ownership.RSP != controlOwnershipRoleSignalSource || record.Ownership.ApplyMode != controlApplyModeInlineJournaled {
		t.Fatalf("expected exclusion command ownership matrix, got %+v", record.Ownership)
	}
	if event.EventType != controlCommandEventType || event.EntityType != controlCommandEntityType || event.EntityID != record.CommandID {
		t.Fatalf("unexpected exclusion runtime event %+v", event)
	}
}

func TestRequestControlCommandRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-control-command-missing-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Missing Authority",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, _, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandRefreshKernel,
		AgentID:     "agent-refresh",
		Reason:      "should fail closed without authority",
		RequestedBy: "operator-a",
	}); err == nil {
		t.Fatal("expected missing workspace authority to fail")
	} else if reject, ok := AsAuthorityReject(err); !ok || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected authority missing reject, got %+v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   controlCommandEventType,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list control command runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no control.command.requested events on authority failure, got %+v", events)
	}
}

func TestRequestControlCommandExclusionRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)
	workspaceID := "ws-control-command-stale-authority"
	tensionID := "tension:task:" + workspaceID + "/task-a"
	agentID := "agent-alpha"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Command Stale Authority",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	referenceAt := time.Now().UTC().Round(0)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       tensionID,
		WorkspaceID:     workspaceID,
		ProtoClusterID:  "task:" + workspaceID + "/task-a",
		TensionType:     "bottleneck",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Stale authority exclusion tension",
		Summary:         "Ensures stale holders cannot mutate exclusions",
		AnchorKind:      "entity_id",
		AnchorRef:       "task-a",
		TaskIDs:         []string{"task-a"},
		BaseScore:       1,
		SurfaceScore:    1,
		EvidenceCount:   1,
		LastSeenEventID: "evt-control-command-stale-authority",
		LastSeenAt:      referenceAt.Format(time.RFC3339Nano),
		ConfirmedBy:     "tester",
		CreatedAt:       referenceAt.Format(time.RFC3339Nano),
		UpdatedAt:       referenceAt.Format(time.RFC3339Nano),
	})
	setWorkspaceControlEpochAnchor(t, store, workspaceID, referenceAt.Format(time.RFC3339Nano))

	peerNodeID := "authnode-999-2"
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
		"boot-peer-1",
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
		NewLeaseToken:                "lease-peer-1",
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    authorityActorTypeSystem,
		ActorID:                      "tester",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}

	if _, _, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandExcludeAgentTension,
		TensionID:   tensionID,
		AgentID:     agentID,
		TTLSeconds:  60,
		Reason:      "should fail closed on stale authority",
		RequestedBy: "tester",
		ActorType:   controlActorTypeSystem,
	}); err == nil {
		t.Fatal("expected stale workspace authority to fail")
	} else if reject, ok := AsAuthorityReject(err); !ok || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected authority stale reject, got %+v", err)
	}

	exclusions, err := store.GetAgentTensionExclusions(ctx, workspaceID, agentID)
	if err != nil {
		t.Fatalf("get tension exclusions: %v", err)
	}
	if len(exclusions) != 0 {
		t.Fatalf("expected no exclusion side effects on stale authority, got %+v", exclusions)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   controlCommandEventType,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list control command runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no control.command.requested events on stale authority, got %+v", events)
	}
}
