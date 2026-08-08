package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationCorridorSnapshotsRejectMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	cases := []struct {
		name       string
		peerNodeID string
		eventType  string
		surface    string
		call       func(*Handler, context.Context, json.RawMessage) (any, *RPCError)
	}{
		{
			name:       "readiness",
			peerNodeID: "authnode-9201-1",
			eventType:  "cluster.corridor_readiness_snapshot",
			surface:    "workspace.instrumentation.corridor.snapshot",
			call:       (*Handler).workspaceInstrumentationCorridorSnapshot,
		},
		{
			name:       "ownership",
			peerNodeID: "authnode-9202-1",
			eventType:  "cluster.corridor_ownership_snapshot",
			surface:    "workspace.instrumentation.corridor.ownership.snapshot",
			call:       (*Handler).workspaceInstrumentationCorridorOwnershipSnapshot,
		},
		{
			name:       "fit",
			peerNodeID: "authnode-9203-1",
			eventType:  "cluster.corridor_fit_snapshot",
			surface:    "workspace.instrumentation.corridor.fit.snapshot",
			call:       (*Handler).workspaceInstrumentationCorridorFitSnapshot,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := newServerTestStore(t)
			h := NewHandler(store)
			ctx := context.Background()
			scenario := seedInstrumentationRPCScenario(t, ctx, store)

			if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
				t.Fatalf("remove workspace authority: %v", err)
			}
			beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

			raw := mustMarshalCorridorSnapshotParams(t, workspaceInstrumentationCorridorParams{
				WorkspaceID:    scenario.workspaceID,
				ProtoClusterID: "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID,
				ActorID:        "dashboard",
				Limit:          5,
			})
			result, rpcErr := tc.call(h, testAuthContext(scenario.workspaceID, "human", "developer"), raw)
			if rpcErr == nil {
				t.Fatal("expected typed authority reject for missing workspace authority")
			}
			if result != nil {
				t.Fatalf("expected no result on missing-authority reject, got %+v", result)
			}
			assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, tc.surface)
			assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, tc.eventType)
			if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
				t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
			}
		})
	}
}

func TestWorkspaceInstrumentationCorridorSnapshotsRejectStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	cases := []struct {
		name       string
		peerNodeID string
		eventType  string
		surface    string
		call       func(*Handler, context.Context, json.RawMessage) (any, *RPCError)
	}{
		{
			name:       "readiness",
			peerNodeID: "authnode-9201-1",
			eventType:  "cluster.corridor_readiness_snapshot",
			surface:    "workspace.instrumentation.corridor.snapshot",
			call:       (*Handler).workspaceInstrumentationCorridorSnapshot,
		},
		{
			name:       "ownership",
			peerNodeID: "authnode-9202-1",
			eventType:  "cluster.corridor_ownership_snapshot",
			surface:    "workspace.instrumentation.corridor.ownership.snapshot",
			call:       (*Handler).workspaceInstrumentationCorridorOwnershipSnapshot,
		},
		{
			name:       "fit",
			peerNodeID: "authnode-9203-1",
			eventType:  "cluster.corridor_fit_snapshot",
			surface:    "workspace.instrumentation.corridor.fit.snapshot",
			call:       (*Handler).workspaceInstrumentationCorridorFitSnapshot,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := newServerTestStore(t)
			h := NewHandler(store)
			ctx := context.Background()
			scenario := seedInstrumentationRPCScenario(t, ctx, store)

			current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
			beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
			transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, tc.peerNodeID)

			raw := mustMarshalCorridorSnapshotParams(t, workspaceInstrumentationCorridorParams{
				WorkspaceID:    scenario.workspaceID,
				ProtoClusterID: "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID,
				ActorID:        "dashboard",
				Limit:          5,
			})
			result, rpcErr := tc.call(h, testAuthContext(scenario.workspaceID, "human", "developer"), raw)
			if rpcErr == nil {
				t.Fatal("expected typed authority reject for stale workspace authority")
			}
			if result != nil {
				t.Fatalf("expected no result on stale-authority reject, got %+v", result)
			}
			assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, tc.surface)
			assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, tc.eventType)
			assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
			if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
				t.Fatalf("expected stale-authority reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
			}
		})
	}
}

func TestWorkspaceInstrumentationCorridorSnapshotsPersistAuthorityMetadataOnRuntimeEvents(t *testing.T) {
	cases := []struct {
		name      string
		eventType string
		call      func(*Handler, context.Context, json.RawMessage) (any, *RPCError)
	}{
		{
			name:      "readiness",
			eventType: "cluster.corridor_readiness_snapshot",
			call:      (*Handler).workspaceInstrumentationCorridorSnapshot,
		},
		{
			name:      "ownership",
			eventType: "cluster.corridor_ownership_snapshot",
			call:      (*Handler).workspaceInstrumentationCorridorOwnershipSnapshot,
		},
		{
			name:      "fit",
			eventType: "cluster.corridor_fit_snapshot",
			call:      (*Handler).workspaceInstrumentationCorridorFitSnapshot,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := newServerTestStore(t)
			h := NewHandler(store)
			ctx := context.Background()
			scenario := seedInstrumentationRPCScenario(t, ctx, store)
			authority := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

			raw := mustMarshalCorridorSnapshotParams(t, workspaceInstrumentationCorridorParams{
				WorkspaceID:    scenario.workspaceID,
				ProtoClusterID: "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID,
				ActorID:        "dashboard",
				Limit:          5,
			})
			result, rpcErr := tc.call(h, testAuthContext(scenario.workspaceID, "human", "developer"), raw)
			if rpcErr != nil {
				t.Fatalf("corridor snapshot rpc error: %+v", rpcErr)
			}
			event := mustCorridorSnapshotEventFromResult(t, result)
			if event.EventType != tc.eventType {
				t.Fatalf("expected event type %s, got %+v", tc.eventType, event)
			}
			assertServerRuntimeEventAuthorityMetadata(t, event, authority)
		})
	}
}

func mustMarshalCorridorSnapshotParams(t *testing.T, params workspaceInstrumentationCorridorParams) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal corridor snapshot params: %v", err)
	}
	return raw
}

func mustCorridorSnapshotEventFromResult(t *testing.T, result any) sqlite.RuntimeEventRecord {
	t.Helper()

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected corridor snapshot result map, got %T", result)
	}
	event, ok := payload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("expected corridor snapshot event, got %T", payload["event"])
	}
	return event
}
