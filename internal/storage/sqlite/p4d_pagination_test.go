package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/google/uuid"
)

func TestP4DRuntimeEventCursorPagination(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "p4d.db")
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

	// 1. Insert 10 events
	var recorded []sqlite.RuntimeEventRecord
	for i := 0; i < 10; i++ {
		ev, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			WorkspaceID: wsID,
			EventType:   "test_type",
			EntityType:  "test_entity",
			EntityID:    fmt.Sprintf("ent-%d", i),
		})
		if err != nil {
			t.Fatal(err)
		}
		// Give a tiny microscopic delay to ensure created_at ordering if required implicitly, though we use ingest_seq now!
		time.Sleep(1 * time.Millisecond)
		recorded = append(recorded, ev)
	}

	// 2. Fetch first page of 3
	page1, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: wsID,
		Limit:       3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 {
		t.Fatalf("expected 3, got %d", len(page1))
	}
	// Since order is DESC, page1[0] should be recorded[9]
	if page1[0].EventID != recorded[9].EventID {
		t.Fatalf("expected event 9, got %s", page1[0].EventID)
	}

	// 3. Fetch second page using cursor
	cursorSeq := page1[2].IngestSeq
	cursorID := page1[2].EventID
	page2, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID:     wsID,
		Limit:           3,
		CursorIngestSeq: &cursorSeq,
		CursorEventID:   cursorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 3 {
		t.Fatalf("expected 3, got %d", len(page2))
	}
	// Last of page 1 was index 7 (recorded[9], recorded[8], recorded[7]).
	// First of page 2 should be recorded[6].
	if page2[0].EventID != recorded[6].EventID {
		t.Fatalf("expected event 6, got %s (%d)", page2[0].EventID, page2[0].IngestSeq)
	}

	// 4. Test ExcludeSynthetic with Cursor
	// Let's insert a couple synthetic events
	store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: wsID,
		EventType:   "controlplane.snapshot",
		EntityType:  "test_entity",
		EntityID:    "synth-1",
	})
	evNormal, _ := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: wsID,
		EventType:   "test_type",
		EntityType:  "test_entity",
		EntityID:    "normal-tail",
	})

	pageMixed, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID:      wsID,
		Limit:            2, // Should skip synthetic and find normal-tail + recorded[9]
		ExcludeSynthetic: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pageMixed) != 2 {
		t.Fatalf("expected 2, got %d", len(pageMixed))
	}
	if pageMixed[0].EventID != evNormal.EventID {
		t.Fatalf("expected normal tail event, got %s", pageMixed[0].EventID)
	}
	if pageMixed[1].EventID != recorded[9].EventID {
		t.Fatalf("expected recorded 9, got %s", pageMixed[1].EventID)
	}
}
