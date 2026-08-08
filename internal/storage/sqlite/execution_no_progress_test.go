package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestCurrentExecutionNoProgressSnapshotMarksRepeatedStepsBlocked(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-execution-no-progress-blocked"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution No Progress Blocked",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	run, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       "run-no-progress-blocked",
		WorkspaceID: workspaceID,
		Title:       "Durable no-progress loop",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run: %v", err)
	}

	for idx := 1; idx <= 3; idx++ {
		if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
			RunID:       run.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       "Keep checking the same checkpoint",
			Summary:     "waiting on unchanged durable checkpoint",
			Status:      "ACTIVE",
			SortOrder:   idx,
			Verification: map[string]any{
				"checkpoint_id": "checkpoint-1",
			},
		}); err != nil {
			t.Fatalf("record execution step %d: %v", idx, err)
		}
	}

	snapshot := store.CurrentExecutionNoProgressSnapshot(ctx)
	if snapshot.State != "blocked" {
		t.Fatalf("expected blocked no-progress snapshot, got %+v", snapshot)
	}
	if snapshot.NoProgressRunCount != 1 {
		t.Fatalf("expected one no-progress run, got %+v", snapshot)
	}
	if snapshot.TriggeredRunID != run.RunID {
		t.Fatalf("expected triggered run id %q, got %+v", run.RunID, snapshot)
	}
	if snapshot.TriggeredConsecutiveRuns != 3 {
		t.Fatalf("expected three consecutive no-progress cycles, got %+v", snapshot)
	}
	if snapshot.RecommendedAction != "needs_operator" {
		t.Fatalf("expected needs_operator recommendation, got %+v", snapshot)
	}
	if snapshot.TriggeredStepPhase != "EXECUTE" || snapshot.TriggeredStepStatus != "ACTIVE" {
		t.Fatalf("expected triggered step details to reflect the durable loop, got %+v", snapshot)
	}
	if snapshot.Message == "" || !strings.Contains(snapshot.Message, "checkpoint_id=checkpoint-1") {
		t.Fatalf("expected message to mention the durable checkpoint token, got %+v", snapshot)
	}
}

func TestCurrentExecutionNoProgressSnapshotIgnoresAdvancingCheckpoints(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-execution-no-progress-healthy"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution No Progress Healthy",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	run, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       "run-no-progress-healthy",
		WorkspaceID: workspaceID,
		Title:       "Durable checkpoint progression",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run: %v", err)
	}

	for idx, checkpointID := range []string{"checkpoint-1", "checkpoint-2", "checkpoint-3"} {
		if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
			RunID:       run.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       "Advance the checkpoint",
			Summary:     "checkpoint moved forward",
			Status:      "ACTIVE",
			SortOrder:   idx + 1,
			Verification: map[string]any{
				"checkpoint_id": checkpointID,
			},
		}); err != nil {
			t.Fatalf("record execution step %d: %v", idx+1, err)
		}
	}

	snapshot := store.CurrentExecutionNoProgressSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected healthy snapshot when checkpoints advance, got %+v", snapshot)
	}
	if snapshot.NoProgressRunCount != 0 {
		t.Fatalf("expected zero no-progress runs, got %+v", snapshot)
	}
	if snapshot.RecommendedAction != "continue" {
		t.Fatalf("expected continue recommendation, got %+v", snapshot)
	}
}

func TestCurrentExecutionNoProgressSnapshotIgnoresAdvancingCoordinationRevision(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-execution-no-progress-coordination-revision"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution No Progress Coordination Revision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	run, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       "run-no-progress-coordination-revision",
		WorkspaceID: workspaceID,
		Title:       "Durable coordination progression",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run: %v", err)
	}

	for idx, revisionID := range []string{"coord-rev-1", "coord-rev-2", "coord-rev-3"} {
		if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
			RunID:       run.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       "Advance coordination receipt",
			Summary:     "same checkpoint with a new durable coordination receipt",
			Status:      "ACTIVE",
			SortOrder:   idx + 1,
			Verification: map[string]any{
				"checkpoint_id":            "checkpoint-shared",
				"coordination_revision_id": revisionID,
			},
		}); err != nil {
			t.Fatalf("record execution step %d: %v", idx+1, err)
		}
	}

	snapshot := store.CurrentExecutionNoProgressSnapshot(ctx)
	if snapshot.State != "ok" {
		t.Fatalf("expected healthy snapshot when coordination revisions advance, got %+v", snapshot)
	}
	if snapshot.NoProgressRunCount != 0 {
		t.Fatalf("expected zero no-progress runs, got %+v", snapshot)
	}
}

func TestCurrentExecutionNoProgressSnapshotIgnoresProseVariation(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-execution-no-progress-prose-variation"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution No Progress Prose Variation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	run, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       "run-no-progress-prose-variation",
		WorkspaceID: workspaceID,
		Title:       "Durable no-progress paraphrases",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run: %v", err)
	}

	for idx, prose := range []struct {
		title   string
		summary string
	}{
		{title: "Check blocker", summary: "same checkpoint, first wording"},
		{title: "Inspect blocker again", summary: "same checkpoint, paraphrased status"},
		{title: "Look once more", summary: "still only narration around the same checkpoint"},
	} {
		if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
			RunID:       run.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       prose.title,
			Summary:     prose.summary,
			Status:      "ACTIVE",
			SortOrder:   idx + 1,
			Verification: map[string]any{
				"checkpoint_id": "checkpoint-prose-stuck",
			},
		}); err != nil {
			t.Fatalf("record execution step %d: %v", idx+1, err)
		}
	}

	snapshot := store.CurrentExecutionNoProgressSnapshot(ctx)
	if snapshot.State != "blocked" {
		t.Fatalf("expected prose variation around same checkpoint to be blocked, got %+v", snapshot)
	}
	if snapshot.TriggeredConsecutiveRuns != 3 {
		t.Fatalf("expected three consecutive no-progress cycles, got %+v", snapshot)
	}
}

func TestCurrentExecutionNoProgressSnapshotUsesDurableStepChronologyAcrossPhases(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-execution-no-progress-phase-order"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution No Progress Phase Ordering",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	run, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       "run-no-progress-phase-order",
		WorkspaceID: workspaceID,
		Title:       "Durable phase ordering no-progress loop",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run: %v", err)
	}

	if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
		RunID:       run.RunID,
		WorkspaceID: workspaceID,
		Phase:       "PLAN",
		Title:       "Select work",
		Summary:     "initial planning should not hide later execute steps",
		Status:      "COMPLETED",
		SortOrder:   10,
		Verification: map[string]any{
			"checkpoint_id": "checkpoint-plan",
		},
	}); err != nil {
		t.Fatalf("record plan step: %v", err)
	}

	for idx := 1; idx <= 3; idx++ {
		if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
			RunID:       run.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       "Retry unchanged checkpoint",
			Summary:     "same checkpoint after plan phase",
			Status:      "ACTIVE",
			SortOrder:   20 + idx,
			Verification: map[string]any{
				"checkpoint_id": "checkpoint-execute",
			},
		}); err != nil {
			t.Fatalf("record execute step %d: %v", idx, err)
		}
	}

	detail, err := store.GetExecutionRun(ctx, workspaceID, run.RunID)
	if err != nil {
		t.Fatalf("get execution run: %v", err)
	}
	if len(detail.Steps) == 0 || detail.Steps[len(detail.Steps)-1].Phase != "PLAN" {
		t.Fatalf("test requires storage ordering to expose phase-order drift, got %+v", detail.Steps)
	}

	snapshot := store.CurrentExecutionNoProgressSnapshot(ctx)
	if snapshot.State != "blocked" {
		t.Fatalf("expected execute no-progress to be detected despite storage phase ordering, got %+v", snapshot)
	}
	if snapshot.TriggeredStepPhase != "EXECUTE" {
		t.Fatalf("expected triggered phase EXECUTE, got %+v", snapshot)
	}
	if snapshot.TriggeredConsecutiveRuns != 3 {
		t.Fatalf("expected three execute repeats, got %+v", snapshot)
	}
	if !strings.Contains(snapshot.Message, "checkpoint_id=checkpoint-execute") {
		t.Fatalf("expected execute checkpoint in message, got %+v", snapshot)
	}
}

func TestCurrentExecutionNoProgressSnapshotScansBeyondFormerWindow(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-execution-no-progress-beyond-window"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution No Progress Beyond Window",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	stuckRun, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       "run-no-progress-older-than-window",
		WorkspaceID: workspaceID,
		Title:       "Older no-progress run",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert stuck execution run: %v", err)
	}
	for idx := 1; idx <= 3; idx++ {
		if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
			RunID:       stuckRun.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       "Older unchanged checkpoint",
			Summary:     "still no durable movement",
			Status:      "ACTIVE",
			SortOrder:   idx,
			Verification: map[string]any{
				"checkpoint_id": "checkpoint-older-stuck",
			},
		}); err != nil {
			t.Fatalf("record stuck execution step %d: %v", idx, err)
		}
	}

	for runIdx := 0; runIdx < 70; runIdx++ {
		run, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
			RunID:       fmt.Sprintf("run-newer-active-%02d", runIdx),
			WorkspaceID: workspaceID,
			Title:       "Newer active run",
			Status:      "ACTIVE",
		})
		if err != nil {
			t.Fatalf("upsert newer active run %d: %v", runIdx, err)
		}
		if _, err := store.RecordExecutionStep(ctx, ExecutionStepInput{
			RunID:       run.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       "Advancing checkpoint",
			Summary:     "healthy single step",
			Status:      "ACTIVE",
			SortOrder:   1,
			Verification: map[string]any{
				"checkpoint_id": fmt.Sprintf("checkpoint-newer-%02d", runIdx),
			},
		}); err != nil {
			t.Fatalf("record newer execution step %d: %v", runIdx, err)
		}
	}

	snapshot := store.CurrentExecutionNoProgressSnapshot(ctx)
	if snapshot.State != "blocked" {
		t.Fatalf("expected older no-progress run beyond former scan window to be detected, got %+v", snapshot)
	}
	if snapshot.TriggeredRunID != stuckRun.RunID {
		t.Fatalf("expected older stuck run to trigger snapshot, got %+v", snapshot)
	}
	if snapshot.ActiveRunCount < 71 {
		t.Fatalf("expected detector to scan all active runs, got %+v", snapshot)
	}
}
