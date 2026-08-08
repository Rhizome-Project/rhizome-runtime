package living

import (
	"context"
	"os"
	"testing"
	"time"
)

// newTestStore returns a MemoryStateStore for unit tests.
func newTestStore(t *testing.T) StateStore {
	t.Helper()
	return NewMemoryStateStore()
}

// newRedisTestStore returns a RedisStateStore if REDIS_URL is set, otherwise skips.
func newRedisTestStore(t *testing.T) StateStore {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set, skipping Redis integration test")
	}
	store, err := NewRedisStateStore(url, "test-brain")
	if err != nil {
		t.Fatalf("failed to create RedisStateStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func makeTestState(taskID string, status TaskStatus) *TaskState {
	return &TaskState{
		TaskID:         taskID,
		RhizomeTaskID:  "rhizome-" + taskID,
		Status:         status,
		IterationCount: 3,
		RetryCount:     1,
		MaxRetries:     5,
		CreatedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
	}
}

func runSaveAndLoadTest(t *testing.T, store StateStore) {
	ctx := context.Background()
	state := makeTestState("task-1", TaskStatusRunning)
	state.Context = "some context"
	state.ProgressSummary = "halfway there"

	if err := store.SaveTaskState(ctx, state); err != nil {
		t.Fatalf("SaveTaskState: %v", err)
	}

	loaded, err := store.LoadTaskState(ctx, "task-1")
	if err != nil {
		t.Fatalf("LoadTaskState: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadTaskState returned nil")
	}

	if loaded.TaskID != state.TaskID {
		t.Errorf("TaskID: got %q, want %q", loaded.TaskID, state.TaskID)
	}
	if loaded.RhizomeTaskID != state.RhizomeTaskID {
		t.Errorf("RhizomeTaskID: got %q, want %q", loaded.RhizomeTaskID, state.RhizomeTaskID)
	}
	if loaded.Status != state.Status {
		t.Errorf("Status: got %q, want %q", loaded.Status, state.Status)
	}
	if loaded.IterationCount != state.IterationCount {
		t.Errorf("IterationCount: got %d, want %d", loaded.IterationCount, state.IterationCount)
	}
	if loaded.Context != state.Context {
		t.Errorf("Context: got %q, want %q", loaded.Context, state.Context)
	}
	if loaded.ProgressSummary != state.ProgressSummary {
		t.Errorf("ProgressSummary: got %q, want %q", loaded.ProgressSummary, state.ProgressSummary)
	}
	if !loaded.CreatedAt.Equal(state.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", loaded.CreatedAt, state.CreatedAt)
	}
}

func runLoadNonExistentTest(t *testing.T, store StateStore) {
	ctx := context.Background()
	loaded, err := store.LoadTaskState(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("LoadTaskState: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil, got %+v", loaded)
	}
}

func runListActiveTaskStatesTest(t *testing.T, store StateStore) {
	ctx := context.Background()

	// Save states with various statuses
	states := []*TaskState{
		makeTestState("active-1", TaskStatusRunning),
		makeTestState("active-2", TaskStatusPending),
		makeTestState("active-3", TaskStatusWaiting),
		makeTestState("done-1", TaskStatusCompleted),
		makeTestState("done-2", TaskStatusFailed),
		makeTestState("done-3", TaskStatusAborted),
	}
	for _, s := range states {
		if err := store.SaveTaskState(ctx, s); err != nil {
			t.Fatalf("SaveTaskState(%s): %v", s.TaskID, err)
		}
	}

	active, err := store.ListActiveTaskStates(ctx)
	if err != nil {
		t.Fatalf("ListActiveTaskStates: %v", err)
	}

	if len(active) != 3 {
		t.Fatalf("expected 3 active states, got %d", len(active))
	}

	ids := map[string]bool{}
	for _, s := range active {
		ids[s.TaskID] = true
	}
	for _, expected := range []string{"active-1", "active-2", "active-3"} {
		if !ids[expected] {
			t.Errorf("expected active task %q in results", expected)
		}
	}
}

func runDeleteTaskStateTest(t *testing.T, store StateStore) {
	ctx := context.Background()
	state := makeTestState("to-delete", TaskStatusRunning)

	if err := store.SaveTaskState(ctx, state); err != nil {
		t.Fatalf("SaveTaskState: %v", err)
	}

	if err := store.DeleteTaskState(ctx, "to-delete"); err != nil {
		t.Fatalf("DeleteTaskState: %v", err)
	}

	loaded, err := store.LoadTaskState(ctx, "to-delete")
	if err != nil {
		t.Fatalf("LoadTaskState after delete: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil after delete, got %+v", loaded)
	}
}

func runCheckpointTest(t *testing.T, store StateStore) {
	ctx := context.Background()
	taskID := "cp-task"
	data := []byte(`{"step":42,"partial":"result"}`)

	if err := store.SaveCheckpoint(ctx, taskID, data); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	loaded, err := store.LoadCheckpoint(ctx, taskID)
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if string(loaded) != string(data) {
		t.Errorf("checkpoint data: got %q, want %q", loaded, data)
	}
}

func runLoadCheckpointNonExistentTest(t *testing.T, store StateStore) {
	ctx := context.Background()
	loaded, err := store.LoadCheckpoint(ctx, "no-checkpoint")
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil, got %v", loaded)
	}
}

func runEmptyListTest(t *testing.T, store StateStore) {
	ctx := context.Background()
	active, err := store.ListActiveTaskStates(ctx)
	if err != nil {
		t.Fatalf("ListActiveTaskStates: %v", err)
	}
	if active == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active states, got %d", len(active))
	}
}

// --- Memory tests (always run) ---

func TestMemory_SaveAndLoad(t *testing.T) {
	runSaveAndLoadTest(t, newTestStore(t))
}

func TestMemory_LoadNonExistent(t *testing.T) {
	runLoadNonExistentTest(t, newTestStore(t))
}

func TestMemory_ListActiveTaskStates(t *testing.T) {
	runListActiveTaskStatesTest(t, newTestStore(t))
}

func TestMemory_DeleteTaskState(t *testing.T) {
	runDeleteTaskStateTest(t, newTestStore(t))
}

func TestMemory_SaveAndLoadCheckpoint(t *testing.T) {
	runCheckpointTest(t, newTestStore(t))
}

func TestMemory_LoadCheckpointNonExistent(t *testing.T) {
	runLoadCheckpointNonExistentTest(t, newTestStore(t))
}

func TestMemory_EmptyList(t *testing.T) {
	runEmptyListTest(t, newTestStore(t))
}

// --- Redis integration tests (run when REDIS_URL is set) ---

func TestRedis_SaveAndLoad(t *testing.T) {
	runSaveAndLoadTest(t, newRedisTestStore(t))
}

func TestRedis_LoadNonExistent(t *testing.T) {
	runLoadNonExistentTest(t, newRedisTestStore(t))
}

func TestRedis_ListActiveTaskStates(t *testing.T) {
	runListActiveTaskStatesTest(t, newRedisTestStore(t))
}

func TestRedis_DeleteTaskState(t *testing.T) {
	runDeleteTaskStateTest(t, newRedisTestStore(t))
}

func TestRedis_SaveAndLoadCheckpoint(t *testing.T) {
	runCheckpointTest(t, newRedisTestStore(t))
}

func TestRedis_LoadCheckpointNonExistent(t *testing.T) {
	runLoadCheckpointNonExistentTest(t, newRedisTestStore(t))
}

func TestRedis_EmptyList(t *testing.T) {
	runEmptyListTest(t, newRedisTestStore(t))
}
