package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestControlEpochAnchorTracksPolicyModeAndEpochIncrements(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-control-epoch-anchor"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Epoch Anchor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	anchor, err := store.persistedControlEpochAnchor(ctx, workspaceID)
	if err != nil {
		t.Fatalf("initial control epoch anchor: %v", err)
	}
	if anchor != "" {
		t.Fatalf("expected empty anchor before first policy epoch row, got %q", anchor)
	}

	fallback, err := store.GetControlEpoch(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get virtual control epoch: %v", err)
	}
	if fallback.WorkspaceID != workspaceID || fallback.CurrentEpoch != 0 || fallback.PolicyMode != "shadow" || fallback.LastIncrementedAt == "" {
		t.Fatalf("expected virtual shadow epoch fallback, got %+v", fallback)
	}

	if err := store.SetPolicyMode(ctx, workspaceID, "active"); err != nil {
		t.Fatalf("set active policy mode: %v", err)
	}

	afterSet, err := store.GetControlEpoch(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get control epoch after policy set: %v", err)
	}
	if afterSet.CurrentEpoch != 0 || afterSet.PolicyMode != "active" || afterSet.LastIncrementedAt == "" {
		t.Fatalf("expected active policy row without epoch advance, got %+v", afterSet)
	}

	anchorAfterSet, err := store.persistedControlEpochAnchor(ctx, workspaceID)
	if err != nil {
		t.Fatalf("anchor after policy set: %v", err)
	}
	if anchorAfterSet != afterSet.LastIncrementedAt {
		t.Fatalf("expected persisted anchor to match policy row last increment time, got anchor=%q record=%+v", anchorAfterSet, afterSet)
	}

	time.Sleep(2 * time.Millisecond)

	currentEpoch, err := store.IncrementEpoch(ctx, workspaceID)
	if err != nil {
		t.Fatalf("increment control epoch: %v", err)
	}
	if currentEpoch != 1 {
		t.Fatalf("expected first increment to advance epoch to 1, got %d", currentEpoch)
	}

	afterIncrement, err := store.GetControlEpoch(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get control epoch after increment: %v", err)
	}
	if afterIncrement.CurrentEpoch != 1 || afterIncrement.PolicyMode != "active" || afterIncrement.LastIncrementedAt == "" {
		t.Fatalf("expected incremented active epoch row, got %+v", afterIncrement)
	}

	anchorAfterIncrement, err := store.persistedControlEpochAnchor(ctx, workspaceID)
	if err != nil {
		t.Fatalf("anchor after increment: %v", err)
	}
	if anchorAfterIncrement == anchorAfterSet {
		t.Fatalf("expected increment to advance persisted anchor, got %q", anchorAfterIncrement)
	}
}

func TestWorkspaceTimeAuthorityExposesEpochAndReferencePair(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-time-authority"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Time Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.SetPolicyMode(ctx, workspaceID, "active"); err != nil {
		t.Fatalf("set active policy mode: %v", err)
	}
	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	epochAnchorAt, err := store.persistedControlEpochAnchor(ctx, workspaceID)
	if err != nil {
		t.Fatalf("read persisted epoch anchor: %v", err)
	}
	runtimeAnchorAt := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "evt-workspace-time-authority",
		WorkspaceID: workspaceID,
		EventType:   "workspace.time_authority_probe",
		EntityType:  "workspace",
		EntityID:    workspaceID,
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{}`,
		CreatedAt:   runtimeAnchorAt,
	}); err != nil {
		t.Fatalf("record runtime event: %v", err)
	}

	authority, err := store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	if authority.WorkspaceID != workspaceID || authority.CurrentEpoch != 1 || authority.PolicyMode != "active" {
		t.Fatalf("unexpected workspace time authority identity fields: %+v", authority)
	}
	if authority.EpochAnchorAt != epochAnchorAt {
		t.Fatalf("expected epoch anchor %s, got %+v", epochAnchorAt, authority)
	}
	if authority.RuntimeEventAnchorAt != runtimeAnchorAt {
		t.Fatalf("expected runtime event anchor %s, got %+v", runtimeAnchorAt, authority)
	}
	if authority.ReferenceAt != runtimeAnchorAt {
		t.Fatalf("expected reference time to follow latest runtime event anchor %s, got %+v", runtimeAnchorAt, authority)
	}
	if authority.TemporalContract == nil {
		t.Fatalf("expected time authority temporal contract, got %+v", authority)
	}
	if authority.TemporalContract.Domain != "control_epoch" ||
		authority.TemporalContract.HorizonKind != "current_epoch" ||
		authority.TemporalContract.Basis != "control_epoch" ||
		authority.TemporalContract.Mapping != "explicit_phi_required" ||
		authority.TemporalContract.WallClockComparable ||
		authority.TemporalContract.State != "LIVE" {
		t.Fatalf("unexpected time authority temporal contract %+v", authority.TemporalContract)
	}
}

func TestWorkspaceReferenceTimestampIgnoresMalformedAnchorsWhenFallbackIsValid(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-time-authority-malformed-anchor"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Time Authority Malformed Anchor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	fallback := "2026-04-08T22:15:00Z"
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_control_epochs (workspace_id, current_epoch, policy_mode, last_incremented_at)
		VALUES (?, 3, 'active', ?)
		ON CONFLICT(workspace_id) DO UPDATE SET last_incremented_at = excluded.last_incremented_at
	`, workspaceID, "not-a-timestamp"); err != nil {
		t.Fatalf("seed malformed epoch anchor: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO runtime_events (event_id, workspace_id, event_type, entity_type, entity_id, actor_type, actor_id, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "evt-malformed-time-authority-anchor", workspaceID, "workspace.time_authority_probe", "workspace", workspaceID, "system", "tester", `{}`, "also-not-a-timestamp"); err != nil {
		t.Fatalf("seed malformed runtime anchor: %v", err)
	}

	referenceAt, err := store.workspaceReferenceTimestamp(ctx, workspaceID, fallback)
	if err != nil {
		t.Fatalf("workspaceReferenceTimestamp: %v", err)
	}
	if referenceAt != fallback {
		t.Fatalf("expected malformed anchors not to poison valid fallback %q, got %q", fallback, referenceAt)
	}
}
