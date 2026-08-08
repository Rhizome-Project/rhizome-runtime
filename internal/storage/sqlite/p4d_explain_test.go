package sqlite_test

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/google/uuid"
)

func TestP4DExplainListRuntimeEvents(t *testing.T) {
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

	q1 := `EXPLAIN QUERY PLAN SELECT * FROM runtime_events WHERE workspace_id = ? AND session_id = ? ORDER BY ingest_seq DESC, event_id DESC LIMIT 500`

	rows, err := store.DB().QueryContext(ctx, q1, wsID, "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var output []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		output = append(output, detail)
	}
	t.Log("EXPLAIN QUERY PLAN for session_id + COALESCE(ingest_seq) + event_id:")
	t.Log("\n" + strings.Join(output, "\n"))
}

func TestRuntimeEventEntityEventLookupUsesCoveringIndex(t *testing.T) {
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

	q := `EXPLAIN QUERY PLAN
SELECT event_id
  FROM runtime_events
 WHERE workspace_id = ?
   AND entity_type = 'task'
   AND entity_id = ?
   AND event_type = 'task.created'
 ORDER BY created_at DESC, event_id DESC
 LIMIT 1`
	rows, err := store.DB().QueryContext(ctx, q, "ws-runtime-event-plan", "task-review-receipt")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var output []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		output = append(output, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(output, "\n")
	if !strings.Contains(plan, "idx_runtime_events_entity_event_created") {
		t.Fatalf("runtime event entity+event lookup should use covering index, plan:\n%s", plan)
	}
	if slices.ContainsFunc(output, func(detail string) bool {
		return strings.Contains(detail, "idx_runtime_events_workspace_created")
	}) {
		t.Fatalf("runtime event entity+event lookup must not scan workspace feed, plan:\n%s", plan)
	}
}
