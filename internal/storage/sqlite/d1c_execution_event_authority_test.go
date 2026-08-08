package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestUpsertExecutionRunWithEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-execution-run-missing-authority"
		runID       = "run-d1c-execution-run-missing-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Execution Run Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	_, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "Missing authority execution run",
		Summary:     "should fail before execution run write",
		Status:      "ACTIVE",
	})
	if err == nil {
		t.Fatal("expected missing authority reject on execution run write")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	if got := countExecutionRuns(t, ctx, store, workspaceID, runID); got != 0 {
		t.Fatalf("expected no execution run rows after authority reject, got %d", got)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    runID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list execution run events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no execution_run.written events after authority reject, got %+v", events)
	}
}

func TestUpsertExecutionRunRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-execution-run-helper-missing-authority"
		runID       = "run-d1c-execution-run-helper-missing-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Execution Run Helper Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	_, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "Missing authority execution run helper",
		Summary:     "generic helper should fail before execution run write",
		Status:      "ACTIVE",
	})
	if err == nil {
		t.Fatal("expected missing authority reject on execution run helper write")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	if got := countExecutionRuns(t, ctx, store, workspaceID, runID); got != 0 {
		t.Fatalf("expected no execution run rows after authority reject, got %d", got)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    runID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list execution run events after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no execution_run.written events after authority reject, got %+v", events)
	}
}

func TestRecordExecutionStepWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-execution-step-stale-authority"
		runID       = "run-d1c-execution-step-stale-authority"
		stepID      = "step-d1c-execution-step-stale-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Execution Step Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "Seed execution run",
		Summary:     "seed execution run for stale authority step test",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("seed execution run with authority: %v", err)
	}
	transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2301")

	_, _, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		StepID:      stepID,
		Phase:       "VERIFY",
		Title:       "Stale authority execution step",
		Summary:     "should fail before execution step write",
		Status:      "BLOCKED",
		SortOrder:   20,
	})
	if err == nil {
		t.Fatal("expected stale authority reject on execution step write")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}

	if got := countExecutionSteps(t, ctx, store, workspaceID, stepID); got != 0 {
		t.Fatalf("expected no execution step rows after stale authority reject, got %d", got)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    stepID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list execution step events after stale authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no execution_step.written events after stale authority reject, got %+v", events)
	}
}

func TestRecordExecutionStepRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-execution-step-helper-stale-authority"
		runID       = "run-d1c-execution-step-helper-stale-authority"
		stepID      = "step-d1c-execution-step-helper-stale-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Execution Step Helper Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "Seed execution run helper",
		Summary:     "seed execution run for stale authority helper step test",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("seed execution run with authority: %v", err)
	}
	transferExternalWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2302")

	_, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		StepID:      stepID,
		Phase:       "VERIFY",
		Title:       "Stale authority execution step helper",
		Summary:     "generic helper should fail before execution step write",
		Status:      "BLOCKED",
		SortOrder:   20,
	})
	if err == nil {
		t.Fatal("expected stale authority reject on execution step helper write")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}

	if got := countExecutionSteps(t, ctx, store, workspaceID, stepID); got != 0 {
		t.Fatalf("expected no execution step rows after stale authority reject, got %d", got)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    stepID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list execution step events after stale authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no execution_step.written events after stale authority reject, got %+v", events)
	}
}

func countExecutionRuns(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, runID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_runs WHERE workspace_id = ? AND run_id = ?`, workspaceID, runID).Scan(&count); err != nil {
		t.Fatalf("count execution run rows: %v", err)
	}
	return count
}

func countExecutionSteps(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, stepID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_steps WHERE workspace_id = ? AND step_id = ?`, workspaceID, stepID).Scan(&count); err != nil {
		t.Fatalf("count execution step rows: %v", err)
	}
	return count
}
