package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/google/uuid"
)

func TestP4DReplayIncompletenessMarkers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatal(err)
	}

	wsID := uuid.NewString()
	err = store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Test Workspace",
		CreatedBy:   "test-owner",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Insert a parent
	parentEv, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: wsID,
		EventType:   "system.test",
		EntityType:  "test_entity",
		EntityID:    "parent-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Insert a child referencing the parent
	_, err = store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: wsID,
		EventType:   "system.test",
		EntityType:  "test_entity",
		EntityID:    "child-1",
		// Store it with canonical json
		ParentRefsJSON: `["` + parentEv.EventID + `"]`,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 3. Insert more events so we can test truncation independently of the missing parent edge
	for i := 0; i < 4; i++ {
		_, err = store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			WorkspaceID: wsID,
			EventType:   "system.test",
			EntityType:  "test_entity",
			EntityID:    uuid.NewString(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Case A: Full Window (No missing parents, not truncated)
	reportFull, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: wsID,
		Limit:       10, // Not truncated
	})
	if err != nil {
		t.Fatal(err)
	}

	if reportFull.Truncated {
		t.Errorf("expected Truncated=false")
	}
	if reportFull.WindowIncomplete {
		t.Errorf("expected WindowIncomplete=false for full replay")
	}
	if len(reportFull.MissingParentRefs) != 0 {
		t.Fatalf("expected 0 missing parents, got %d", len(reportFull.MissingParentRefs))
	}

	// Case B: Truncated Window triggers Incompleteness because the parent is naturally skipped
	reportPartial, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: wsID,
		Limit:       5, // 4 new events + the child = 5. The parent is the 6th, so it gets excluded.
	})
	if err != nil {
		t.Fatal(err)
	}

	if !reportPartial.Truncated {
		t.Errorf("expected Truncated=true")
	}
	if !reportPartial.WindowIncomplete {
		t.Errorf("expected WindowIncomplete=true due to truncation")
	}
	if len(reportPartial.MissingParentRefs) != 1 {
		t.Fatalf("expected 1 missing parent, got %d", len(reportPartial.MissingParentRefs))
	}
	if reportPartial.MissingParentRefs[0] != parentEv.EventID {
		t.Errorf("expected missing parent to be %s, got %s", parentEv.EventID, reportPartial.MissingParentRefs[0])
	}
}
