package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordInstrumentationMetricSnapshotAppendsRuntimeEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-instrumentation-snapshot"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Instrumentation Snapshot",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "custom.seed",
		EntityType:  "custom",
		EntityID:    "seed",
		ActorType:   "operator",
		ActorID:     "developer",
	}); err != nil {
		t.Fatalf("record seed event: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	report, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("build instrumentation report: %v", err)
	}

	event, err := store.RecordInstrumentationMetricSnapshot(ctx, report, sqlite.InstrumentationSnapshotInput{
		ActorID: "instrumentation-test",
		Limit:   5,
	})
	if err != nil {
		t.Fatalf("record instrumentation metric snapshot: %v", err)
	}
	if event.EventType != "cluster.metric_snapshot" || event.EntityType != "workspace" || event.EntityID != workspaceID {
		t.Fatalf("unexpected snapshot event %+v", event)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal snapshot payload: %v", err)
	}
	if payload["workspace_id"] != workspaceID {
		t.Fatalf("expected workspace_id %q in snapshot payload, got %+v", workspaceID, payload)
	}
	clusters, ok := payload["clusters"].([]any)
	if !ok {
		t.Fatalf("expected clusters array in snapshot payload, got %+v", payload["clusters"])
	}
	if len(clusters) == 0 {
		t.Fatalf("expected at least one cluster in snapshot payload, got %+v", payload)
	}
	if payload["generated_at"] != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected snapshot payload generated_at %q to mirror time authority reference_at %q", payload["generated_at"], report.TimeAuthority.ReferenceAt)
	}
}
