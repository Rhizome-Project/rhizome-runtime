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

// P4D-004 Comprehensive Regression Suite
// Covers deterministic cursor pagination, synthetic exclusion behavior,
// incomplete-window semantics, and legacy query execution paths.
func TestP4DComprehensiveRegressionSuite(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "p4d.db")
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
	wsIDEmpty := uuid.NewString()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Regression Tests",
		CreatedBy:   "test",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsIDEmpty,
		Title:       "Empty Tests",
		CreatedBy:   "test",
	}); err != nil {
		t.Fatal(err)
	}

	// Setup Base Data Layer
	var recorded []sqlite.RuntimeEventRecord
	sessionID := "sess-regression-1"

	_, err = store.DB().ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, agent_id, workspace_id, status, started_at, created_at)
		VALUES (?, 'bot', ?, 'ACTIVE', ?, ?)`,
		sessionID, wsID, time.Now().Format(time.RFC3339), time.Now().Format(time.RFC3339),
	)
	if err != nil {
		t.Fatal(err)
	}

	// 1. A parent node
	parentEv, _ := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: wsID,
		EventType:   "system.test",
		EntityType:  "test_entity",
		EntityID:    "parent-1",
		SessionID:   sessionID,
	})
	recorded = append(recorded, parentEv)

	// 2. Insert 10 normal nodes and 5 synthetic nodes mixed in
	for i := 0; i < 15; i++ {
		eventType := "system.test"
		if i%3 == 0 {
			eventType = "controlplane.snapshot" // Synthetic
		}

		parentRefs := ""
		if i == 5 {
			// Deliberately establish casual edge to parent-1 to test missing parents on truncation
			parentRefs = `["` + parentEv.EventID + `"]`
		}

		ev, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			WorkspaceID:    wsID,
			EventType:      eventType,
			EntityType:     "test_entity",
			EntityID:       fmt.Sprintf("ent-%d", i),
			SessionID:      sessionID,
			ParentRefsJSON: parentRefs,
		})
		if err != nil {
			t.Fatal(err)
		}
		recorded = append(recorded, ev)
	}

	// 3. Insert an event that bypasses ingest_seq auto-gen using direct raw SQL mimicking legacy state
	legacyID := uuid.NewString()
	_, err = store.DB().ExecContext(ctx, `
		INSERT INTO runtime_events (
			event_id, workspace_id, event_type, entity_type, entity_id,
			payload_json, created_at, session_id
		) VALUES (
			?, ?, 'system.test.legacy', 'test_entity', 'legacy-1',
			'{}', '2020-01-01T00:00:00Z', ?
		)`,
		legacyID, wsID, sessionID,
	)
	if err != nil {
		t.Fatal(err)
	}

	// t.Run closures for modular testing

	t.Run("CursorPagination_NoDuplicatesOrGaps", func(t *testing.T) {
		var swept []sqlite.RuntimeEventRecord
		var cursorEvent string
		var cursorSeq *int64

		for {
			page, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID:     wsID,
				Limit:           4,
				CursorEventID:   cursorEvent,
				CursorIngestSeq: cursorSeq,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			swept = append(swept, page...)
			last := page[len(page)-1]
			cursorEvent = last.EventID

			seq := last.IngestSeq
			cursorSeq = &seq
		}

		if len(swept) != 17 {
			t.Fatalf("expected 17 total events (1 parent + 15 loop + 1 legacy), got %d", len(swept))
		}

		// Verify strict descending sequence map
		seen := map[string]bool{}
		for i, ev := range swept {
			if seen[ev.EventID] {
				t.Fatalf("duplicate event found in pagination: %s", ev.EventID)
			}
			seen[ev.EventID] = true
			if i > 0 {
				prevSeq := swept[i-1].IngestSeq
				currSeq := ev.IngestSeq

				if currSeq > prevSeq {
					t.Fatalf("out of order ingest_seq: prev %d, curr %d", prevSeq, currSeq)
				}
				if currSeq == prevSeq && ev.EventID >= swept[i-1].EventID {
					t.Fatalf("out of order event_id on same ingest_seq")
				}
			}
		}
	})

	t.Run("ExcludeSynthetic_NoCursorDrift", func(t *testing.T) {
		var swept []sqlite.RuntimeEventRecord
		var cursorEvent string
		var cursorSeq *int64

		for {
			page, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID:      wsID,
				Limit:            5,
				ExcludeSynthetic: true,
				CursorEventID:    cursorEvent,
				CursorIngestSeq:  cursorSeq,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			swept = append(swept, page...)
			last := page[len(page)-1]
			cursorEvent = last.EventID
			seq := last.IngestSeq
			cursorSeq = &seq

			// Verify page does not contain synthetic
			for _, e := range page {
				if strings.HasPrefix(strings.TrimSpace(e.EventType), "controlplane.") {
					t.Fatalf("found synthetic event despite exclusion: %v", e)
				}
			}
		}

		if len(swept) != 12 {
			t.Fatalf("expected 12 non-synthetic events, got %d", len(swept))
		}
	})

	t.Run("IncompleteWindow_ExplicitBounds", func(t *testing.T) {
		// Event 5 explicitly points to parent-1.
		// We'll read the journal with a limit of 5. Because we have 15 new events, limit 5
		// fetches events 10-14. parent-1 and event 5 are excluded.
		// Wait, we need an event within the window pointing outside the window.
		// We should insert a fresh parent and fresh child to ensure safe boundary testing.

		sysSess := "sess-bound-testing"
		_, err := store.DB().ExecContext(ctx, `
			INSERT INTO agent_sessions (session_id, agent_id, workspace_id, status, started_at, created_at)
			VALUES (?, 'bot', ?, 'ACTIVE', ?, ?)`,
			sysSess, wsID, time.Now().Format(time.RFC3339), time.Now().Format(time.RFC3339),
		)
		if err != nil {
			t.Fatal(err)
		}

		p, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			WorkspaceID: wsID,
			EventType:   "sys.p",
			EntityType:  "test",
			EntityID:    "bound-p1",
			SessionID:   sysSess,
		})
		if err != nil {
			t.Fatalf("failed to insert bound-p1: %v", err)
		}

		c, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			WorkspaceID:    wsID,
			EventType:      "sys.c",
			EntityType:     "test",
			EntityID:       "bound-c1",
			SessionID:      sysSess,
			ParentRefsJSON: `["` + p.EventID + `"]`,
		})
		if err != nil {
			t.Fatalf("failed to insert bound-c1: %v", err)
		}

		// Fill above child exactly 1 time
		_, err = store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			WorkspaceID: wsID,
			EventType:   "sys.pad",
			EntityType:  "test",
			EntityID:    "bound-pad",
			SessionID:   sysSess,
		})
		if err != nil {
			t.Fatalf("failed to insert bound-pad: %v", err)
		}

		// Fetch Limit 2 on THIS specific session to perfectly guarantee boundaries
		report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
			WorkspaceID: wsID,
			SessionID:   sysSess,
			Limit:       2,
		})
		if err != nil {
			t.Fatal(err)
		}

		if !report.Truncated {
			t.Errorf("expected report.Truncated=true")
		}
		if !report.WindowIncomplete {
			t.Errorf("expected report.WindowIncomplete=true")
		}

		var foundc1 bool
		for _, e := range report.Events {
			if e.EventID == c.EventID {
				foundc1 = true
			}
		}
		if !foundc1 {
			var ids []string
			for _, ev := range report.Events {
				ids = append(ids, ev.EventID+" ("+ev.EntityID+")")
			}
			t.Fatalf("test logic failure: bound-c1 not fetched. Fetched: %v", ids)
		}

		if len(report.MissingParentRefs) != 1 || report.MissingParentRefs[0] != p.EventID {
			t.Errorf("expected missing parent to be specifically %s, got %v", p.EventID, report.MissingParentRefs)
		}
	})

	t.Run("ForensicQueryShape_AndEmptyState", func(t *testing.T) {
		// Single row window
		page, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
			WorkspaceID: wsID,
			Limit:       1,
			SessionID:   sessionID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 1 {
			t.Fatalf("expected 1 row window limit to hold, got %d", len(page))
		}

		// Empty Workspace
		pageEmpty, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
			WorkspaceID: wsIDEmpty,
			Limit:       10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(pageEmpty) != 0 {
			t.Fatalf("expected empty result size, got %d", len(pageEmpty))
		}

		// Legacy records are fetched using same session scan
		var foundLegacy bool
		// sweep sessionID
		var cursorEvent string
		var cursorSeq *int64
		for {
			sessPage, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
				WorkspaceID:     wsID,
				SessionID:       sessionID,
				Limit:           100,
				CursorEventID:   cursorEvent,
				CursorIngestSeq: cursorSeq,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(sessPage) == 0 {
				break
			}
			for _, e := range sessPage {
				if e.EventID == legacyID {
					foundLegacy = true
				}
			}
			last := sessPage[len(sessPage)-1]
			cursorEvent = last.EventID
			seq := last.IngestSeq
			cursorSeq = &seq
		}

		if !foundLegacy {
			t.Fatalf("legacy event without ingest_seq was entirely dropped from session_id index sweeps")
		}
	})
}
