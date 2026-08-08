package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/google/uuid"
)

func TestListRuntimeEventsExcludeSyntheticAdvancesCursor(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "runtime-event-pagination-audit.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("synthetic_only_head", func(t *testing.T) {
		wsID := uuid.NewString()
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: wsID,
			Title:       "Synthetic head",
			CreatedBy:   "audit",
		}); err != nil {
			t.Fatal(err)
		}

		if err := recordRuntimeEvents(ctx, store, wsID, []string{
			"normal.a",
			"normal.b",
			"normal.c",
		}); err != nil {
			t.Fatal(err)
		}
		if err := recordRuntimeEvents(ctx, store, wsID, []string{
			"controlplane.snapshot",
			"controlplane.snapshot",
			"controlplane.snapshot",
		}); err != nil {
			t.Fatal(err)
		}

		events, err := collectRuntimeEventsExcludeSynthetic(ctx, store, wsID, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 3 {
			t.Fatalf("expected 3 non-synthetic events, got %d", len(events))
		}
		for _, ev := range events {
			if strings.HasPrefix(strings.TrimSpace(ev.EventType), "controlplane.") {
				t.Fatalf("synthetic event leaked into excluded page: %+v", ev)
			}
		}
	})

	t.Run("synthetic_tail", func(t *testing.T) {
		wsID := uuid.NewString()
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: wsID,
			Title:       "Synthetic tail",
			CreatedBy:   "audit",
		}); err != nil {
			t.Fatal(err)
		}

		if err := recordRuntimeEvents(ctx, store, wsID, []string{
			"controlplane.snapshot",
			"controlplane.snapshot",
			"controlplane.snapshot",
		}); err != nil {
			t.Fatal(err)
		}
		if err := recordRuntimeEvents(ctx, store, wsID, []string{
			"normal.x",
			"normal.y",
			"normal.z",
		}); err != nil {
			t.Fatal(err)
		}

		events, err := collectRuntimeEventsExcludeSynthetic(ctx, store, wsID, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 3 {
			t.Fatalf("expected 3 non-synthetic events, got %d", len(events))
		}
		for _, ev := range events {
			if strings.HasPrefix(strings.TrimSpace(ev.EventType), "controlplane.") {
				t.Fatalf("synthetic event leaked into excluded page: %+v", ev)
			}
		}
	})
}

func recordRuntimeEvents(ctx context.Context, store *sqlite.Store, workspaceID string, eventTypes []string) error {
	for idx, eventType := range eventTypes {
		entityID := fmt.Sprintf("%s-%d", strings.ReplaceAll(eventType, ".", "-"), idx)
		if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   eventType,
			EntityType:  "test_entity",
			EntityID:    entityID,
		}); err != nil {
			return err
		}
	}
	return nil
}

func collectRuntimeEventsExcludeSynthetic(ctx context.Context, store *sqlite.Store, workspaceID string, limit int) ([]sqlite.RuntimeEventRecord, error) {
	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var out []sqlite.RuntimeEventRecord
	var cursorEvent string
	var cursorSeq *int64

	for step := 0; step < 16; step++ {
		page, err := store.ListRuntimeEvents(tctx, sqlite.RuntimeEventFilter{
			WorkspaceID:      workspaceID,
			Limit:            limit,
			ExcludeSynthetic: true,
			CursorEventID:    cursorEvent,
			CursorIngestSeq:  cursorSeq,
		})
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		for _, ev := range page {
			if strings.HasPrefix(strings.TrimSpace(ev.EventType), "controlplane.") {
				return nil, fmt.Errorf("synthetic event leaked through excluded page: %+v", ev)
			}
		}
		out = append(out, page...)
		last := page[len(page)-1]
		cursorEvent = last.EventID
		seq := last.IngestSeq
		cursorSeq = &seq
	}

	return nil, fmt.Errorf("pagination did not terminate after 16 pages")
}
