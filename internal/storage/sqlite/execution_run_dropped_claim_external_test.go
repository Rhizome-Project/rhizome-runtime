package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// CT-08: a frontier "selected" decision only grants execution once a durable
// claim+session receipt confirms ownership. This proves the fail-closed read-back
// at the storage binding layer: once the task claim is released (dropped/terminal
// ownership), a session-bound execution run can no longer materialize as live,
// even though the session and its start receipt still exist. The agent therefore
// cannot proceed to execute against ownership it no longer holds.
func TestExecutionRunBindingRejectsReleasedClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-exec-dropped-claim"
		agentID     = "agent-dropped-claim"
		taskID      = "task-dropped-claim"
		sessionID   = "sess-dropped-claim"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Run Dropped Claim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: taskID, AgentID: agentID}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session with receipt: %v", err)
	}

	// While the claim is live and owned, the execution run binds.
	if _, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-while-claimed",
		AgentID:               agentID,
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Live owned run",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("tool.call", "server_operation_ledger", workspaceID, "agent", agentID),
	}); err != nil {
		t.Fatalf("expected run to bind while claim is live, got %v", err)
	}

	// Drop ownership: release the claim. The session/start receipt remain intact.
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "dropped ownership before execution",
	}); err != nil {
		t.Fatalf("release task claim: %v", err)
	}

	// A new session-bound run must now be rejected: released ownership is not live.
	_, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-after-release",
		AgentID:               agentID,
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Run after released claim",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("tool.call", "server_operation_ledger", workspaceID, "agent", agentID),
	})
	if !errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) || !strings.Contains(err.Error(), "claim is not live") {
		t.Fatalf("expected released-claim binding reject, got %v", err)
	}
}
