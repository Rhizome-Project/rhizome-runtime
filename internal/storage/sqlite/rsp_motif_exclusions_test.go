package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestExcludeAgentFromTensionUsesWorkspaceReferenceTime(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	workspaceID := "ws-motif-exclusions"
	tensionID := "tension:task:" + workspaceID + "/task-a"
	agentID := "agent-alpha"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Motif Exclusions",
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
		Title:           "Motif exclusion test tension",
		Summary:         "Ensures exclusion TTLs follow workspace authority",
		AnchorKind:      "entity_id",
		AnchorRef:       "task-a",
		TaskIDs:         []string{"task-a"},
		BaseScore:       1,
		SurfaceScore:    1,
		EvidenceCount:   1,
		LastSeenEventID: "evt-motif-exclusion",
		LastSeenAt:      referenceAt.Format(time.RFC3339Nano),
		ConfirmedBy:     "tester",
		CreatedAt:       referenceAt.Format(time.RFC3339Nano),
		UpdatedAt:       referenceAt.Format(time.RFC3339Nano),
	})
	setWorkspaceControlEpochAnchor(t, store, workspaceID, referenceAt.Format(time.RFC3339Nano))

	if err := store.ExcludeAgentFromTension(ctx, workspaceID, tensionID, agentID, "thrash", time.Minute); err != nil {
		t.Fatalf("exclude agent from tension: %v", err)
	}

	var expiresAt string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT expires_at
		FROM workspace_tension_exclusions
		WHERE workspace_id = ? AND tension_id = ? AND agent_id = ?
	`, workspaceID, tensionID, agentID).Scan(&expiresAt); err != nil {
		t.Fatalf("query exclusion expiry: %v", err)
	}
	wantExpiresAt := referenceAt.Add(time.Minute).Format(time.RFC3339Nano)
	if expiresAt != wantExpiresAt {
		t.Fatalf("expected exclusion expiry %s, got %s", wantExpiresAt, expiresAt)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   controlCommandEventType,
		EntityType:  controlCommandEntityType,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list control command runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one canonical control command event, got %+v", events)
	}
	if events[0].EntityID == "" || events[0].EventType != controlCommandEventType {
		t.Fatalf("unexpected control command runtime event %+v", events[0])
	}

	exclusions, err := store.GetAgentTensionExclusions(ctx, workspaceID, agentID)
	if err != nil {
		t.Fatalf("get active exclusions: %v", err)
	}
	if len(exclusions) != 1 || exclusions[0] != tensionID {
		t.Fatalf("expected active exclusion for %s, got %+v", tensionID, exclusions)
	}

	expiredReferenceAt := referenceAt.Add(2 * time.Minute)
	setWorkspaceControlEpochAnchor(t, store, workspaceID, expiredReferenceAt.Format(time.RFC3339Nano))

	exclusions, err = store.GetAgentTensionExclusions(ctx, workspaceID, agentID)
	if err != nil {
		t.Fatalf("get expired exclusions: %v", err)
	}
	if len(exclusions) != 0 {
		t.Fatalf("expected exclusion to expire under advanced workspace authority, got %+v", exclusions)
	}
}
