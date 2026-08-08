package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceExecutionRunWriteRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d1c-execution-run-write-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Execution Run Write Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceExecutionRunWriteParams{
		WorkspaceID: workspaceID,
		RunID:       "run-d1c-execution-run-write-missing-authority",
		Title:       "Missing authority execution run",
		Summary:     "should fail before execution run write",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("marshal execution run params: %v", err)
	}

	result, rpcErr := h.workspaceExecutionRunWrite(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "workspace.execution.run.write" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "run-d1c-execution-run-write-missing-authority",
		Limit:       10,
	}); err != nil {
		t.Fatalf("list execution run events after missing-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no execution_run.written events after missing-authority reject, got %+v", events)
	}
}

func TestWorkspaceExecutionStepWriteRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-execution-step-write-stale-authority"
		runID       = "run-d1c-execution-step-write-stale-authority"
		stepID      = "step-d1c-execution-step-write-stale-authority"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D1C Execution Step Write Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "Seed execution run for stale step",
		Summary:     "seed run",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("seed execution run: %v", err)
	}
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2302")

	raw, err := json.Marshal(workspaceExecutionStepWriteParams{
		WorkspaceID: workspaceID,
		RunID:       runID,
		StepID:      stepID,
		Phase:       "VERIFY",
		Title:       "Stale authority execution step",
		Summary:     "should fail before execution step write",
		Status:      "BLOCKED",
		SortOrder:   20,
	})
	if err != nil {
		t.Fatalf("marshal execution step params: %v", err)
	}

	result, rpcErr := h.workspaceExecutionStepWrite(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.execution.step.write" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    stepID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list execution step events after stale-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no execution_step.written events after stale-authority reject, got %+v", events)
	}
}
