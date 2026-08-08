package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRuntimeEventParentRefsWriteValidation(t *testing.T) {
	t.Parallel()

	t.Run("rejects missing parent in same workspace", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		mustCreateRuntimeEventValidationWorkspace(t, ctx, store, "ws-runtime-parent-missing")

		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:        "rtev-parent-missing-child",
			WorkspaceID:    "ws-runtime-parent-missing",
			EventType:      "runtime.envelope.child",
			EntityType:     "runtime_event",
			EntityID:       "runtime-child",
			ActorType:      "system",
			ActorID:        "tests",
			ParentRefsJSON: `["rtev-parent-missing"]`,
			PayloadJSON:    `{"message":"child event with missing parent"}`,
			CreatedAt:      "2026-03-27T10:01:00Z",
		}); err == nil {
			t.Fatal("expected missing same-workspace parent_ref write to fail")
		}

		events := listRuntimeEventsForWorkspace(t, ctx, store, "ws-runtime-parent-missing")
		if len(events) != 0 {
			t.Fatalf("expected rejected write to leave workspace empty, got %+v", events)
		}
	})

	t.Run("rejects self parent", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		mustCreateRuntimeEventValidationWorkspace(t, ctx, store, "ws-runtime-parent-self")

		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:        "rtev-parent-self",
			WorkspaceID:    "ws-runtime-parent-self",
			EventType:      "runtime.envelope.self",
			EntityType:     "runtime_event",
			EntityID:       "runtime-self",
			ActorType:      "system",
			ActorID:        "tests",
			ParentRefsJSON: `["rtev-parent-self"]`,
			PayloadJSON:    `{"message":"self parented event"}`,
			CreatedAt:      "2026-03-27T10:02:00Z",
		}); err == nil {
			t.Fatal("expected self-parent runtime event write to fail")
		}

		events := listRuntimeEventsForWorkspace(t, ctx, store, "ws-runtime-parent-self")
		if len(events) != 0 {
			t.Fatalf("expected rejected self-parent write to leave workspace empty, got %+v", events)
		}
	})

	t.Run("rejects cross workspace parent", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		mustCreateRuntimeEventValidationWorkspace(t, ctx, store, "ws-runtime-parent-source")
		mustCreateRuntimeEventValidationWorkspace(t, ctx, store, "ws-runtime-parent-target")

		parent, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:     "rtev-parent-cross-workspace",
			WorkspaceID: "ws-runtime-parent-source",
			EventType:   "runtime.envelope.parent",
			EntityType:  "runtime_event",
			EntityID:    "runtime-parent",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"source workspace parent"}`,
			CreatedAt:   "2026-03-27T10:03:00Z",
		})
		if err != nil {
			t.Fatalf("record source-workspace parent runtime event: %v", err)
		}

		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:        "rtev-parent-cross-workspace-child",
			WorkspaceID:    "ws-runtime-parent-target",
			EventType:      "runtime.envelope.child",
			EntityType:     "runtime_event",
			EntityID:       "runtime-child",
			ActorType:      "system",
			ActorID:        "tests",
			ParentRefsJSON: `["` + parent.EventID + `"]`,
			PayloadJSON:    `{"message":"cross workspace child"}`,
			CreatedAt:      "2026-03-27T10:04:00Z",
		}); err == nil {
			t.Fatal("expected cross-workspace parent_ref write to fail")
		}

		sourceEvents := listRuntimeEventsForWorkspace(t, ctx, store, "ws-runtime-parent-source")
		if len(sourceEvents) != 1 || sourceEvents[0].EventID != parent.EventID {
			t.Fatalf("expected source workspace parent to remain intact, got %+v", sourceEvents)
		}
		targetEvents := listRuntimeEventsForWorkspace(t, ctx, store, "ws-runtime-parent-target")
		if len(targetEvents) != 0 {
			t.Fatalf("expected rejected cross-workspace child write to leave target workspace empty, got %+v", targetEvents)
		}
	})

	t.Run("allows same workspace parent", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		mustCreateRuntimeEventValidationWorkspace(t, ctx, store, "ws-runtime-parent-valid")

		parent, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:     "rtev-parent-valid",
			WorkspaceID: "ws-runtime-parent-valid",
			EventType:   "runtime.envelope.parent",
			EntityType:  "runtime_event",
			EntityID:    "runtime-parent",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"valid parent event"}`,
			CreatedAt:   "2026-03-27T10:05:00Z",
		})
		if err != nil {
			t.Fatalf("record valid parent runtime event: %v", err)
		}

		child, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:        "rtev-parent-valid-child",
			WorkspaceID:    "ws-runtime-parent-valid",
			EventType:      "runtime.envelope.child",
			EntityType:     "runtime_event",
			EntityID:       "runtime-child",
			ActorType:      "system",
			ActorID:        "tests",
			ParentRefsJSON: `["` + parent.EventID + `"]`,
			PayloadJSON:    `{"message":"valid child event"}`,
			CreatedAt:      "2026-03-27T10:06:00Z",
		})
		if err != nil {
			t.Fatalf("record valid child runtime event: %v", err)
		}
		if child.ParentRefsJSON != `["`+parent.EventID+`"]` {
			t.Fatalf("expected same-workspace child to preserve parent refs, got %+v", child)
		}

		events := listRuntimeEventsForWorkspace(t, ctx, store, "ws-runtime-parent-valid")
		if len(events) != 2 {
			t.Fatalf("expected parent and child runtime events, got %+v", events)
		}

		report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
			WorkspaceID: "ws-runtime-parent-valid",
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("replay runtime journal: %v", err)
		}
		if report.Evaluation.Verdict != "pass" {
			t.Fatalf("expected valid same-workspace parent refs to replay cleanly, got %+v", report.Evaluation)
		}
	})
}

func mustCreateRuntimeEventValidationWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
}

func listRuntimeEventsForWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) []sqlite.RuntimeEventRecord {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list runtime events for workspace %s: %v", workspaceID, err)
	}
	return events
}
