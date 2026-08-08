package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestRecordAgentWorkTaskFrontierFailsClosedWhenEvidenceIdentityMissing(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	frontier := AgentWorkTaskFrontier{
		GenerationID:  "frontier-missing-evidence",
		GeneratedAt:   "2026-05-26T00:00:00Z",
		SelectionMode: "agent_self_select",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{Task: WorkspaceTaskRecord{TaskID: "task-runnable"}},
		},
	}

	for _, tc := range []struct {
		name        string
		workspaceID string
		agentID     string
		generation  string
	}{
		{name: "missing workspace", workspaceID: "", agentID: "agent-a", generation: "frontier-missing-evidence"},
		{name: "missing agent", workspaceID: "ws-a", agentID: "", generation: "frontier-missing-evidence"},
		{name: "missing generation", workspaceID: "ws-a", agentID: "agent-a", generation: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frontier.GenerationID = tc.generation
			err := store.recordAgentWorkTaskFrontier(ctx, tc.workspaceID, tc.agentID, frontier)
			if !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "incomplete task frontier evidence") {
				t.Fatalf("expected incomplete frontier evidence to fail closed, got %v", err)
			}
		})
	}

	if err := store.recordAgentWorkTaskFrontier(ctx, "", "", AgentWorkTaskFrontier{
		GenerationID: "frontier-only-blocked",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{Task: WorkspaceTaskRecord{TaskID: "task-blocked"}, Blocked: true},
		},
	}); err != nil {
		t.Fatalf("blocked-only frontier should remain a no-op, got %v", err)
	}
}

func TestRecordAgentWorkTaskFrontierRejectsCrossAgentGenerationRebind(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-agent-immutable"
		generation  = "frontier-agent-immutable"
		taskID      = "task-frontier-agent-immutable"
	)
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-frontier-agent-immutable")

	frontier := AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		SelectionMode: "agent_self_select",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{Task: WorkspaceTaskRecord{TaskID: taskID}},
		},
	}
	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, "agent-a", frontier); err != nil {
		t.Fatalf("record first frontier: %v", err)
	}
	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, "agent-b", frontier); !errors.Is(err, ErrTaskClaimAdmissionInvalid) ||
		!strings.Contains(err.Error(), "already bound to another agent") {
		t.Fatalf("expected cross-agent generation rebind rejection, got %v", err)
	}

	var agentID string
	if err := store.db.QueryRowContext(ctx,
		`SELECT agent_id FROM agent_task_frontiers WHERE workspace_id = ? AND generation_id = ?`,
		workspaceID,
		generation,
	).Scan(&agentID); err != nil {
		t.Fatalf("load frontier agent: %v", err)
	}
	if agentID != "agent-a" {
		t.Fatalf("frontier generation was rebound to %q", agentID)
	}
}

func TestRecordAgentWorkTaskFrontierPersistsDiagnosticBlockedOnlyFrontier(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-diagnostic-blocked"
		agentID     = "agent-frontier"
		taskID      = "task-frontier-diagnostic-blocked"
		generation  = "frontier-diagnostic-blocked"
	)
	seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskID)

	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		SelectionMode: "agent_self_select",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{
				Task:         WorkspaceTaskRecord{TaskID: taskID},
				Blocked:      true,
				BlockReason:  "profile_task_mode_mismatch",
				BlockSummary: "Agent fresh-selection mode is not eligible for implementation work.",
			},
		},
	}); err != nil {
		t.Fatalf("record diagnostic blocked-only frontier: %v", err)
	}

	var rawClaimable, rawDiagnostic string
	var candidateCount int
	if err := store.db.QueryRowContext(ctx, `
SELECT candidate_task_ids_json, diagnostic_candidate_task_ids_json, candidate_count
FROM agent_task_frontiers
WHERE workspace_id = ? AND generation_id = ?`,
		workspaceID,
		generation,
	).Scan(&rawClaimable, &rawDiagnostic, &candidateCount); err != nil {
		t.Fatalf("load diagnostic frontier row: %v", err)
	}
	if rawClaimable != "[]" {
		t.Fatalf("blocked diagnostic frontier must not make the task claimable, got claimable evidence %s", rawClaimable)
	}
	if ok, err := taskFrontierEvidenceContainsTask(rawDiagnostic, taskID); err != nil || !ok {
		t.Fatalf("expected diagnostic evidence to contain %s, ok=%v err=%v raw=%s", taskID, ok, err, rawDiagnostic)
	}
	if candidateCount != 1 {
		t.Fatalf("candidate_count = %d, want 1 visible diagnostic candidate", candidateCount)
	}

	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "admission_failed",
		SelectedTaskID:       taskID,
		Summary:              "profile task mode mismatch",
	}); err != nil {
		t.Fatalf("record admission_failed diagnostic decision: %v", err)
	}
	assertTaskFrontierRow(t, ctx, store, workspaceID, generation, "admission_failed", taskID, "", "")

	_, err := store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: generation,
		SelfFitSummary:       "blocked diagnostic candidate must not be claimable",
		Summary:              "try claim diagnostic frontier task",
	})
	if !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires selected decision receipt") {
		t.Fatalf("expected diagnostic admission_failed frontier to reject claim, got %v", err)
	}
}

func TestTaskFrontierDecisionRejectsSelectedDiagnosticBlockedCandidate(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-selected-diagnostic-blocked"
		agentID     = "agent-frontier"
		taskID      = "task-frontier-selected-diagnostic-blocked"
		generation  = "frontier-selected-diagnostic-blocked"
	)
	seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskID)

	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		SelectionMode: "agent_self_select",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{
				Task:         WorkspaceTaskRecord{TaskID: taskID},
				Blocked:      true,
				BlockReason:  "project_claim_scope_busy",
				BlockSummary: "Project implementation write scope is busy.",
			},
		},
	}); err != nil {
		t.Fatalf("record diagnostic blocked-only frontier: %v", err)
	}

	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "selected",
		SelectedTaskID:       taskID,
		Summary:              "blocked task must not become selected",
	}); !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "was not in frontier generation") {
		t.Fatalf("expected selected decision for diagnostic-only task to be rejected, got %v", err)
	}
}

func TestTaskFrontierDecisionReceiptConsumesClaimEvidenceOnce(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-consume-once"
		agentID     = "agent-frontier"
		taskAID     = "task-frontier-a"
		taskBID     = "task-frontier-b"
		generation  = "frontier-consume-once"
	)
	seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskAID, taskBID)

	frontier := AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		SelectionMode: "agent_self_select",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{Task: WorkspaceTaskRecord{TaskID: taskAID}},
			{Task: WorkspaceTaskRecord{TaskID: taskBID}},
		},
	}
	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, frontier); err != nil {
		t.Fatalf("record frontier: %v", err)
	}
	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "selected",
		SelectedTaskID:       taskAID,
		Summary:              "task A fits this agent now",
	}); err != nil {
		t.Fatalf("record selected frontier decision: %v", err)
	}
	assertTaskFrontierRow(t, ctx, store, workspaceID, generation, "selected", taskAID, "", "")

	event, err := store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskAID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: generation,
		SelfFitSummary:       "task A fits this agent now",
		Summary:              "claim selected frontier task",
	})
	if err != nil {
		t.Fatalf("claim selected frontier task: %v", err)
	}
	assertTaskFrontierRow(t, ctx, store, workspaceID, generation, "consumed", taskAID, taskAID, event.EventID)

	_, err = store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskBID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: generation,
		SelfFitSummary:       "task B was also offered but the generation is consumed",
		Summary:              "try consumed frontier generation",
	})
	if !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("expected consumed frontier generation to reject second task claim, got %v", err)
	}
}

func TestTaskFrontierDecisionTransitionsAreFreshAndIdempotent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-decision-transition"
		agentID     = "agent-frontier"
		taskID      = "task-frontier-selected"
		generation  = "frontier-transition"
	)
	seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskID)
	frontier := AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		SelectionMode: "agent_self_select",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{Task: WorkspaceTaskRecord{TaskID: taskID}},
		},
	}
	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, frontier); err != nil {
		t.Fatalf("record frontier: %v", err)
	}

	envelope := BuildTaskPromptContextEnvelope("agent.task_frontier.decision", "server_rpc", workspaceID, "agent", agentID)
	envelope["actor_agent_id"] = agentID
	envelope["agent_id"] = agentID
	envelope["task_id"] = taskID
	firstEvent, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:           workspaceID,
		AgentID:               agentID,
		FrontierGenerationID:  generation,
		DecisionState:         "selected",
		SelectedTaskID:        taskID,
		Summary:               "task fits",
		PromptContextEnvelope: envelope,
	})
	if err != nil {
		t.Fatalf("record selected decision: %v", err)
	}
	replayEvent, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "selected",
		SelectedTaskID:       taskID,
		Summary:              "retry should not append another event",
	})
	if err != nil {
		t.Fatalf("replay selected decision: %v", err)
	}
	if replayEvent.EventID != firstEvent.EventID {
		t.Fatalf("expected idempotent replay to return original event %s, got %s", firstEvent.EventID, replayEvent.EventID)
	}
	if got := countTaskFrontierDecisionEvents(t, ctx, store, workspaceID, generation); got != 1 {
		t.Fatalf("expected one decision runtime event after replay, got %d", got)
	}
	if !strings.Contains(firstEvent.PayloadJSON, executionPromptContextEnvelopeKey) {
		t.Fatalf("expected prompt context envelope on decision event payload, got %s", firstEvent.PayloadJSON)
	}

	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "declined",
		Summary:              "too late to decline",
	}); !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "cannot transition") {
		t.Fatalf("expected selected -> declined to be rejected, got %v", err)
	}

	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "hydration_failed",
		SelectedTaskID:       taskID,
		Summary:              "hydration failed after selection",
	}); err != nil {
		t.Fatalf("selected -> hydration_failed should be accepted: %v", err)
	}
	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "selected",
		SelectedTaskID:       taskID,
		Summary:              "cannot resurrect failed hydration",
	}); !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("expected hydration_failed -> selected to be rejected, got %v", err)
	}
}

func TestTaskFrontierDecisionTerminalizesSelectedClaimAndAdmissionFailures(t *testing.T) {
	t.Parallel()

	for _, state := range []string{"claim_failed", "admission_failed"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()

			store := NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-frontier-" + state
			agentID := "agent-frontier-" + state
			taskID := "task-frontier-" + state
			generation := "frontier-" + state
			seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskID)
			if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, AgentWorkTaskFrontier{
				GenerationID:  generation,
				GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				SelectionMode: "agent_self_select",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{Task: WorkspaceTaskRecord{TaskID: taskID}},
				},
			}); err != nil {
				t.Fatalf("record frontier: %v", err)
			}
			if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
				WorkspaceID:          workspaceID,
				AgentID:              agentID,
				FrontierGenerationID: generation,
				DecisionState:        "selected",
				SelectedTaskID:       taskID,
				Summary:              "selected before claim/session failure",
			}); err != nil {
				t.Fatalf("record selected decision: %v", err)
			}
			firstEvent, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
				WorkspaceID:          workspaceID,
				AgentID:              agentID,
				FrontierGenerationID: generation,
				DecisionState:        state,
				SelectedTaskID:       taskID,
				Summary:              "terminal failure before executable session",
			})
			if err != nil {
				t.Fatalf("selected -> %s should be accepted: %v", state, err)
			}
			assertTaskFrontierRow(t, ctx, store, workspaceID, generation, state, taskID, "", "")
			if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, AgentWorkTaskFrontier{
				GenerationID:  generation,
				GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				SelectionMode: "agent_self_select",
				Candidates: []AgentWorkTaskFrontierCandidate{
					{Task: WorkspaceTaskRecord{TaskID: taskID}},
				},
			}); err != nil {
				t.Fatalf("re-record frontier after terminal %s: %v", state, err)
			}
			assertTaskFrontierRow(t, ctx, store, workspaceID, generation, state, taskID, "", "")

			replayEvent, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
				WorkspaceID:          workspaceID,
				AgentID:              agentID,
				FrontierGenerationID: generation,
				DecisionState:        state,
				SelectedTaskID:       taskID,
				Summary:              "idempotent retry",
			})
			if err != nil {
				t.Fatalf("replay %s decision: %v", state, err)
			}
			if replayEvent.EventID != firstEvent.EventID {
				t.Fatalf("expected %s replay to return original event %s, got %s", state, firstEvent.EventID, replayEvent.EventID)
			}
			if got := countTaskFrontierDecisionEvents(t, ctx, store, workspaceID, generation); got != 2 {
				t.Fatalf("expected selected plus terminal failure events, got %d", got)
			}
			if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
				WorkspaceID:          workspaceID,
				AgentID:              agentID,
				FrontierGenerationID: generation,
				DecisionState:        "selected",
				SelectedTaskID:       taskID,
				Summary:              "cannot resurrect terminal frontier failure",
			}); !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "terminal") {
				t.Fatalf("expected %s -> selected to be rejected, got %v", state, err)
			}
		})
	}
}

func TestTaskFrontierDecisionRejectsExpiredNonReplay(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-decision-expired"
		agentID     = "agent-frontier"
		taskID      = "task-frontier-expired"
		generation  = "frontier-expired"
	)
	seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskID)
	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   "2020-01-01T00:00:00Z",
		SelectionMode: "agent_self_select",
		Candidates: []AgentWorkTaskFrontierCandidate{
			{Task: WorkspaceTaskRecord{TaskID: taskID}},
		},
	}); err != nil {
		t.Fatalf("record expired frontier: %v", err)
	}
	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "selected",
		SelectedTaskID:       taskID,
		Summary:              "expired frontier should fail",
	}); !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired frontier decision to be rejected, got %v", err)
	}
}

// TestConsumeTaskFrontierRejectsExpiryAtConsumeTime is the CA-12 regression: the
// ensure-time expiry gate passes, but a long admission tx can cross the TTL before
// consume. consumeTaskFrontierClaimEvidenceTx must re-validate liveness against a
// fresh wall clock and refuse to consume an expired generation.
func TestConsumeTaskFrontierRejectsExpiryAtConsumeTime(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-consume-expired"
		agentID     = "agent-frontier"
		taskID      = "task-frontier-consume-expired"
		generation  = "frontier-consume-expired"
	)
	seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskID)

	// Record a live (non-expired) frontier + selected decision via the normal path.
	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		SelectionMode: "agent_self_select",
		Candidates:    []AgentWorkTaskFrontierCandidate{{Task: WorkspaceTaskRecord{TaskID: taskID}}},
	}); err != nil {
		t.Fatalf("record frontier: %v", err)
	}
	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "selected",
		SelectedTaskID:       taskID,
		Summary:              "task fits",
	}); err != nil {
		t.Fatalf("record selected decision: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Simulate the TTL crossing during the admission tx: force expires_at into the past.
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_task_frontiers SET expires_at = ?
		WHERE workspace_id = ? AND generation_id = ? AND agent_id = ?`,
		"2020-01-01T00:00:00Z", workspaceID, generation, agentID,
	); err != nil {
		t.Fatalf("force expiry: %v", err)
	}

	err = store.consumeTaskFrontierClaimEvidenceTx(ctx, tx, workspaceID, agentID, taskID, generation,
		"evt-consume-expired", "consume after expiry", time.Now().UTC().Format(time.RFC3339Nano))
	if !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected consume to reject an expired generation, got %v", err)
	}
}

// TestInvalidateSelectedTaskFrontierGenerationsRejectsStaleReplay is the CA-13/14
// regression: once a task's claim is released/reset, an un-consumed 'selected'
// generation must be invalidated so a later re-claim cannot replay it.
func TestInvalidateSelectedTaskFrontierGenerationsRejectsStaleReplay(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-frontier-invalidate"
		agentID     = "agent-frontier"
		taskID      = "task-frontier-invalidate"
		generation  = "frontier-invalidate"
	)
	seedFrontierTestWorkspace(t, ctx, store, workspaceID, agentID, taskID)
	if err := store.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, AgentWorkTaskFrontier{
		GenerationID:  generation,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		SelectionMode: "agent_self_select",
		Candidates:    []AgentWorkTaskFrontierCandidate{{Task: WorkspaceTaskRecord{TaskID: taskID}}},
	}); err != nil {
		t.Fatalf("record frontier: %v", err)
	}
	if _, err := store.RecordAgentTaskFrontierDecision(ctx, AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generation,
		DecisionState:        "selected",
		SelectedTaskID:       taskID,
		Summary:              "fits now",
	}); err != nil {
		t.Fatalf("record selected: %v", err)
	}
	assertTaskFrontierRow(t, ctx, store, workspaceID, generation, "selected", taskID, "", "")

	// Simulate the release/reaper-reset invalidation.
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := store.invalidateSelectedTaskFrontierGenerationsTx(ctx, tx, workspaceID, taskID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("invalidate selected generations: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit invalidation: %v", err)
	}
	assertTaskFrontierRow(t, ctx, store, workspaceID, generation, "claim_failed", taskID, "", "")

	// A re-claim replaying the now-stale generation must be rejected at the ensure gate.
	tx2, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback() }()
	if err := store.ensureTaskFrontierClaimEvidenceTx(ctx, tx2, workspaceID, agentID, taskID, generation); !errors.Is(err, ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected stale generation replay to be rejected, got %v", err)
	}
}

func seedFrontierTestWorkspace(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID string, taskIDs ...string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, taskID := range taskIDs {
		if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
			TaskID:       taskID,
			OwnerUserID:  "developer",
			Priority:     "high",
			Title:        taskID,
			TaskKind:     "COORDINATION",
			TaskTemplate: model.TaskTemplateIntegration,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
}

func assertTaskFrontierRow(t *testing.T, ctx context.Context, store *Store, workspaceID, generationID, wantState, wantSelectedTaskID, wantConsumedTaskID, wantConsumedEventID string) {
	t.Helper()
	var state, selectedTaskID, consumedTaskID, consumedEventID, consumedAt string
	if err := store.db.QueryRowContext(ctx, `
SELECT decision_state, selected_task_id, consumed_task_id, consumed_event_id, consumed_at
FROM agent_task_frontiers
WHERE workspace_id = ? AND generation_id = ?`,
		workspaceID, generationID,
	).Scan(&state, &selectedTaskID, &consumedTaskID, &consumedEventID, &consumedAt); err != nil {
		t.Fatalf("load task frontier row: %v", err)
	}
	if state != wantState || selectedTaskID != wantSelectedTaskID || consumedTaskID != wantConsumedTaskID || consumedEventID != wantConsumedEventID {
		t.Fatalf("frontier row = state=%q selected=%q consumed_task=%q consumed_event=%q, want state=%q selected=%q consumed_task=%q consumed_event=%q", state, selectedTaskID, consumedTaskID, consumedEventID, wantState, wantSelectedTaskID, wantConsumedTaskID, wantConsumedEventID)
	}
	if wantConsumedTaskID != "" && strings.TrimSpace(consumedAt) == "" {
		t.Fatalf("expected consumed_at to be recorded for consumed frontier")
	}
}

func countTaskFrontierDecisionEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, generationID string) int {
	t.Helper()
	var count int
	if err := store.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_events
WHERE workspace_id = ? AND event_type = 'task_frontier.decision' AND entity_type = 'agent_task_frontier' AND entity_id = ?`,
		workspaceID,
		generationID,
	).Scan(&count); err != nil {
		t.Fatalf("count frontier decision events: %v", err)
	}
	return count
}
