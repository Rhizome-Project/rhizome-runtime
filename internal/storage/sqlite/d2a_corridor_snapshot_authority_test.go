package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type d2aCorridorSnapshotScenario struct {
	workspaceID string
	taskID      string
	clusterID   string
}

func TestCorridorSnapshotsRejectMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		peerNodeID string
		eventType  string
		record     func(context.Context, *Store, d2aCorridorSnapshotScenario) (RuntimeEventRecord, error)
	}{
		{
			name:       "readiness",
			peerNodeID: "authnode-9101-1",
			eventType:  "cluster.corridor_readiness_snapshot",
			record:     recordD2ACorridorReadinessSnapshot,
		},
		{
			name:       "ownership",
			peerNodeID: "authnode-9102-1",
			eventType:  "cluster.corridor_ownership_snapshot",
			record:     recordD2ACorridorOwnershipSnapshot,
		},
		{
			name:       "fit",
			peerNodeID: "authnode-9103-1",
			eventType:  "cluster.corridor_fit_snapshot",
			record:     recordD2ACorridorFitSnapshot,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := NewTestStore(t)
			ctx := context.Background()
			scenario := seedD2ACorridorSnapshotScenario(t, ctx, store, "missing-"+tc.name)

			if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
				t.Fatalf("remove workspace authority: %v", err)
			}
			beforeUpdatedAt := mustD2ACorridorSnapshotWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

			event, err := tc.record(ctx, store, scenario)
			if err == nil {
				t.Fatal("expected missing workspace authority reject")
			}
			if event.EventID != "" {
				t.Fatalf("expected no runtime event on authority reject, got %+v", event)
			}
			reject, ok := AsAuthorityReject(err)
			if !ok || reject == nil {
				t.Fatalf("expected authority reject, got %v", err)
			}
			if reject.RejectCode != AuthorityRejectMissing {
				t.Fatalf("expected missing workspace authority reject, got %+v", reject)
			}

			if got := countD2ACorridorSnapshotEvents(t, ctx, store, scenario.workspaceID, tc.eventType); got != 0 {
				t.Fatalf("expected no %s events after missing-authority reject, got %d", tc.eventType, got)
			}
			if afterUpdatedAt := mustD2ACorridorSnapshotWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
				t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
			}
		})
	}
}

func TestCorridorSnapshotsRejectStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		peerNodeID string
		eventType  string
		record     func(context.Context, *Store, d2aCorridorSnapshotScenario) (RuntimeEventRecord, error)
	}{
		{
			name:       "readiness",
			peerNodeID: "authnode-9101-1",
			eventType:  "cluster.corridor_readiness_snapshot",
			record:     recordD2ACorridorReadinessSnapshot,
		},
		{
			name:       "ownership",
			peerNodeID: "authnode-9102-1",
			eventType:  "cluster.corridor_ownership_snapshot",
			record:     recordD2ACorridorOwnershipSnapshot,
		},
		{
			name:       "fit",
			peerNodeID: "authnode-9103-1",
			eventType:  "cluster.corridor_fit_snapshot",
			record:     recordD2ACorridorFitSnapshot,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := NewTestStore(t)
			ctx := context.Background()
			scenario := seedD2ACorridorSnapshotScenario(t, ctx, store, "stale-"+tc.name)

			current, err := store.GetWorkspaceAuthority(ctx, scenario.workspaceID, authorityScopeWorkspace)
			if err != nil {
				t.Fatalf("get workspace authority: %v", err)
			}
			beforeUpdatedAt := mustD2ACorridorSnapshotWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
			beforeRejects := countD2ACorridorAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)
			transferWorkspaceAuthorityToExternalPeer(t, ctx, store, scenario.workspaceID, current, tc.peerNodeID)

			event, err := tc.record(ctx, store, scenario)
			if err == nil {
				t.Fatal("expected stale workspace authority reject")
			}
			if event.EventID != "" {
				t.Fatalf("expected no runtime event on stale authority reject, got %+v", event)
			}
			reject, ok := AsAuthorityReject(err)
			if !ok || reject == nil {
				t.Fatalf("expected authority reject, got %v", err)
			}
			if reject.RejectCode != AuthorityRejectStale {
				t.Fatalf("expected stale workspace authority reject, got %+v", reject)
			}

			if got := countD2ACorridorSnapshotEvents(t, ctx, store, scenario.workspaceID, tc.eventType); got != 0 {
				t.Fatalf("expected no %s events after stale-authority reject, got %d", tc.eventType, got)
			}
			assertD2ACorridorAuthorityRejectEventIncrement(t, ctx, store, scenario.workspaceID, beforeRejects, AuthorityRejectStale)
			if afterUpdatedAt := mustD2ACorridorSnapshotWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
				t.Fatalf("expected stale-authority reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
			}
		})
	}
}

func TestCorridorSnapshotsPersistAuthorityMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		record func(context.Context, *Store, d2aCorridorSnapshotScenario) (RuntimeEventRecord, error)
	}{
		{
			name:   "readiness",
			record: recordD2ACorridorReadinessSnapshot,
		},
		{
			name:   "ownership",
			record: recordD2ACorridorOwnershipSnapshot,
		},
		{
			name:   "fit",
			record: recordD2ACorridorFitSnapshot,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := NewTestStore(t)
			ctx := context.Background()
			scenario := seedD2ACorridorSnapshotScenario(t, ctx, store, "metadata-"+tc.name)

			authority, err := store.GetWorkspaceAuthority(ctx, scenario.workspaceID, authorityScopeWorkspace)
			if err != nil {
				t.Fatalf("get workspace authority: %v", err)
			}

			event, err := tc.record(ctx, store, scenario)
			if err != nil {
				t.Fatalf("record corridor snapshot: %v", err)
			}
			assertRuntimeEventAuthorityMetadata(t, event, authority)
		})
	}
}

func seedD2ACorridorSnapshotScenario(t *testing.T, ctx context.Context, store *Store, suffix string) d2aCorridorSnapshotScenario {
	t.Helper()

	workspaceID := "ws-d2a-corridor-" + suffix
	taskID := "task-d2a-corridor-" + suffix
	clusterID := "task:" + workspaceID + "/" + taskID

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-d2a-corridor-"+suffix)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks
		 SET task_kind = ?, task_template = ?, task_class = ?, task_class_source = ?, task_class_updated_at = ?, title = ?, description = ?, updated_at = ?
		 WHERE task_id = ?`,
		model.TaskKindCoordination,
		model.TaskTemplateResearch,
		model.TaskClassExploration,
		model.TaskClassSourceExplicit,
		now,
		"Explore corridor authority "+suffix,
		"Authority-backed corridor snapshots should fail closed on stale or missing workspace authority.",
		now,
		taskID,
	); err != nil {
		t.Fatalf("update corridor task metadata: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      taskID,
		PayloadJSON: `{"task_id":"` + taskID + `"}`,
	}); err != nil {
		t.Fatalf("record corridor runtime event: %v", err)
	}

	return d2aCorridorSnapshotScenario{
		workspaceID: workspaceID,
		taskID:      taskID,
		clusterID:   clusterID,
	}
}

func recordD2ACorridorReadinessSnapshot(ctx context.Context, store *Store, scenario d2aCorridorSnapshotScenario) (RuntimeEventRecord, error) {
	report, err := store.BuildCorridorReadinessReport(ctx, CorridorReadinessFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          1,
	})
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return store.RecordCorridorReadinessSnapshot(ctx, report, CorridorSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
}

func recordD2ACorridorOwnershipSnapshot(ctx context.Context, store *Store, scenario d2aCorridorSnapshotScenario) (RuntimeEventRecord, error) {
	report, err := store.BuildCorridorOwnershipReport(ctx, CorridorOwnershipFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          1,
	})
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return store.RecordCorridorOwnershipSnapshot(ctx, report, CorridorOwnershipSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
}

func recordD2ACorridorFitSnapshot(ctx context.Context, store *Store, scenario d2aCorridorSnapshotScenario) (RuntimeEventRecord, error) {
	report, err := store.BuildCorridorFitReport(ctx, CorridorFitFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          1,
	})
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return store.RecordCorridorFitSnapshot(ctx, report, CorridorFitSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
}

func countD2ACorridorSnapshotEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list %s runtime events: %v", eventType, err)
	}
	return len(events)
}

func mustD2ACorridorSnapshotWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		t.Fatalf("load workspace updated_at: %v", err)
	}
	return updatedAt
}

func countD2ACorridorAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "authority.rejected",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority.rejected runtime events: %v", err)
	}
	return len(events)
}

func assertD2ACorridorAuthorityRejectEventIncrement(t *testing.T, ctx context.Context, store *Store, workspaceID string, before int, wantCode AuthorityRejectCode) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "authority.rejected",
		Limit:       before + 5,
	})
	if err != nil {
		t.Fatalf("list authority.rejected runtime events: %v", err)
	}
	if len(events) != before+1 {
		t.Fatalf("expected authority.rejected count to grow from %d to %d, got %d", before, before+1, len(events))
	}
	latest := events[0]
	if latest.EventType != "authority.rejected" {
		t.Fatalf("expected latest runtime event to be authority.rejected, got %+v", latest)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(latest.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority.rejected payload: %v", err)
	}
	if payload["reject_code"] != string(wantCode) {
		t.Fatalf("expected latest authority reject code %s, got %+v", wantCode, payload)
	}
}
