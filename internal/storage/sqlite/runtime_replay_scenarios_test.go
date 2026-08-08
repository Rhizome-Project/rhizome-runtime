package sqlite_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type runtimeReplayScenario struct {
	name        string
	workspaceID string
	setup       func(t *testing.T, ctx context.Context, store *sqlite.Store)
	assert      func(t *testing.T, report sqlite.RuntimeReplayReport)
}

func TestRuntimeReplayScenarioGate(t *testing.T) {
	t.Parallel()

	scenarios := []runtimeReplayScenario{
		{
			name:        "blocked_decision_resume_clean",
			workspaceID: "ws-replay-clean",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-clean", "agent-clean")

				start := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventStart,
					WorkspaceID: "ws-replay-clean",
					SessionID:   "sess-clean",
					AgentID:     "agent-clean",
					Summary:     "Taking ownership of transport recovery",
					OwnerScope:  "task/session",
					UpdatedAt:   "2026-03-22T09:00:00Z",
				})
				syncReplayExecutionFromSessionState(t, ctx, store, start)

				keepFalse := false
				blocked := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:         model.SessionEventBlocked,
					WorkspaceID:       "ws-replay-clean",
					SessionID:         "sess-clean",
					AgentID:           "agent-clean",
					Summary:           "Blocked waiting for bridge wake acknowledgement",
					BlockedOn:         []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake ack timeout"}},
					UpdatedAt:         "2026-03-22T09:05:00Z",
					KeepSessionActive: &keepFalse,
				})
				_, err := store.SyncOperatorQueueFromSessionState(ctx, blocked)
				if err != nil {
					t.Fatalf("sync blocked operator queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, blocked)

				decision := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:          model.SessionEventDecisionNeeded,
					WorkspaceID:        "ws-replay-clean",
					SessionID:          "sess-clean",
					AgentID:            "agent-clean",
					Summary:            "Need operator decision on retry policy",
					DecisionNeededFrom: "developer",
					DecisionType:       "retry_policy",
					UpdatedAt:          "2026-03-22T09:10:00Z",
					KeepSessionActive:  &keepFalse,
				})
				_, err = store.SyncOperatorQueueFromSessionState(ctx, decision)
				if err != nil {
					t.Fatalf("sync decision operator queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, decision)

				keepTrue := true
				resumed := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:         model.SessionEventKeepalive,
					WorkspaceID:       "ws-replay-clean",
					SessionID:         "sess-clean",
					AgentID:           "agent-clean",
					Status:            model.SessionStatusActive,
					Summary:           "Decision received and transport resumed",
					UpdatedAt:         "2026-03-22T09:15:00Z",
					KeepSessionActive: &keepTrue,
				})
				_, err = store.SyncOperatorQueueFromSessionState(ctx, resumed)
				if err != nil {
					t.Fatalf("sync keepalive operator queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, resumed)

				ended := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventEnd,
					WorkspaceID: "ws-replay-clean",
					SessionID:   "sess-clean",
					AgentID:     "agent-clean",
					Summary:     "Transport recovery completed cleanly",
					UpdatedAt:   "2026-03-22T09:20:00Z",
				})
				_, err = store.SyncOperatorQueueFromSessionState(ctx, ended)
				if err != nil {
					t.Fatalf("sync ended operator queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, ended)
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "pass")
				expectFindingCodes(t, report, nil)
				if report.Metrics.OpenQueueCount != 0 {
					t.Fatalf("expected no open queues, got %+v", report.Metrics)
				}
				session := requireReplaySession(t, report, "sess-clean")
				if session.Status != model.SessionStatusEnded {
					t.Fatalf("expected ended session, got %+v", session)
				}
				run := requireReplayExecutionRun(t, report, "session:sess-clean")
				if run.Status != "COMPLETED" || run.StepEventCount == 0 {
					t.Fatalf("expected completed run with steps, got %+v", run)
				}
			},
		},
		{
			name:        "reset_duplicate_keepalive_clean",
			workspaceID: "ws-replay-reset",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-reset", "agent-reset")

				start := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventStart,
					WorkspaceID: "ws-replay-reset",
					SessionID:   "sess-reset",
					AgentID:     "agent-reset",
					Summary:     "Recovered active session after workspace reset",
					OwnerScope:  "task/session",
					UpdatedAt:   "2026-03-22T10:00:00Z",
				})
				syncReplayExecutionFromSessionState(t, ctx, store, start)

				keepTrue := true
				for idx, ts := range []string{"2026-03-22T10:01:00Z", "2026-03-22T10:02:00Z"} {
					state := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
						EventType:         model.SessionEventKeepalive,
						WorkspaceID:       "ws-replay-reset",
						SessionID:         "sess-reset",
						AgentID:           "agent-reset",
						Status:            model.SessionStatusActive,
						Summary:           fmt.Sprintf("Duplicate wake recovery keepalive %d", idx+1),
						UpdatedAt:         ts,
						KeepSessionActive: &keepTrue,
					})
					_, err := store.SyncOperatorQueueFromSessionState(ctx, state)
					if err != nil {
						t.Fatalf("sync reset keepalive operator queue: %v", err)
					}
					syncReplayExecutionFromSessionState(t, ctx, store, state)
				}

				ended := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventEnd,
					WorkspaceID: "ws-replay-reset",
					SessionID:   "sess-reset",
					AgentID:     "agent-reset",
					Summary:     "Recovered run completed after reset",
					UpdatedAt:   "2026-03-22T10:05:00Z",
				})
				_, err := store.SyncOperatorQueueFromSessionState(ctx, ended)
				if err != nil {
					t.Fatalf("sync reset end operator queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, ended)
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "pass")
				session := requireReplaySession(t, report, "sess-reset")
				if session.EventCount < 4 {
					t.Fatalf("expected duplicated keepalive to be preserved in event count, got %+v", session)
				}
			},
		},
		{
			name:        "handoff_takeover_clean",
			workspaceID: "ws-replay-handoff",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-handoff", "agent-source", "agent-target")

				start := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventStart,
					WorkspaceID: "ws-replay-handoff",
					SessionID:   "sess-source",
					AgentID:     "agent-source",
					Summary:     "Owning bridge recovery rollout",
					OwnerScope:  "task/session",
					UpdatedAt:   "2026-03-22T11:00:00Z",
				})
				syncReplayExecutionFromSessionState(t, ctx, store, start)

				handoff := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventStatus,
					WorkspaceID: "ws-replay-handoff",
					SessionID:   "sess-source",
					AgentID:     "agent-source",
					Status:      model.SessionStatusHandoffPending,
					Summary:     "Need fresh owner after reset",
					HandoffTo:   "agent-target",
					UpdatedAt:   "2026-03-22T11:05:00Z",
				})
				_, err := store.SyncOperatorQueueFromSessionState(ctx, handoff)
				if err != nil {
					t.Fatalf("sync handoff queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, handoff)

				record := takeOverReplaySession(t, ctx, store, sqlite.AgentSessionTakeoverInput{
					WorkspaceID:        "ws-replay-handoff",
					SessionID:          "sess-source",
					SuccessorSessionID: "sess-target",
					TakeoverAgentID:    "agent-target",
					Summary:            "Source agent yields ownership after reset",
					SuccessorSummary:   "Target agent resumes active ownership",
					UpdatedAt:          "2026-03-22T11:10:00Z",
				})
				_, err = store.SyncOperatorQueueFromSessionState(ctx, record.SourceState)
				if err != nil {
					t.Fatalf("sync source takeover queue: %v", err)
				}
				_, err = store.SyncOperatorQueueFromSessionState(ctx, record.SuccessorState)
				if err != nil {
					t.Fatalf("sync successor takeover queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, record.SourceState)
				syncReplayExecutionFromSessionState(t, ctx, store, record.SuccessorState)

				successorEnded := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventEnd,
					WorkspaceID: "ws-replay-handoff",
					SessionID:   "sess-target",
					AgentID:     "agent-target",
					Summary:     "Takeover finished successfully",
					UpdatedAt:   "2026-03-22T11:20:00Z",
				})
				_, err = store.SyncOperatorQueueFromSessionState(ctx, successorEnded)
				if err != nil {
					t.Fatalf("sync successor end queue: %v", err)
				}
				syncReplayExecutionFromSessionState(t, ctx, store, successorEnded)
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "pass")
				expectFindingCodes(t, report, nil)
				requireReplaySession(t, report, "sess-source")
				target := requireReplaySession(t, report, "sess-target")
				if target.Status != model.SessionStatusEnded {
					t.Fatalf("expected successor session to end cleanly, got %+v", target)
				}
				if report.Metrics.OpenQueueCount != 0 {
					t.Fatalf("expected no open handoff queue after takeover, got %+v", report.Metrics)
				}
			},
		},
		{
			name:        "missing_execution_run_warn",
			workspaceID: "ws-replay-missing-run",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-missing-run", "agent-missing-run")

				recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventStart,
					WorkspaceID: "ws-replay-missing-run",
					SessionID:   "sess-missing-run",
					AgentID:     "agent-missing-run",
					Summary:     "Active session intentionally skips execution-run sync",
					UpdatedAt:   "2026-03-22T11:00:00Z",
				})
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "warn")
				expectFindingCodes(t, report, []string{"missing_execution_run"})
				summary := report.Evaluation.FindingSummary
				if summary.ExecutionRunIntegrityCount != 1 ||
					summary.MissingExecutionRunCount != 1 ||
					summary.ExecutionRunOutOfSyncCount != 0 ||
					summary.ExecutionRunWithoutStepsCount != 0 ||
					summary.OtherFindingCount != 0 {
					t.Fatalf("expected missing-run replay fixture to surface execution-run integrity subcounts, got %+v", summary)
				}
			},
		},
		{
			name:        "claim_review_confirm_clean",
			workspaceID: "ws-replay-claim-clean",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-claim-clean", "agent-claim")

				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-clean",
					WorkspaceID: "ws-replay-claim-clean",
					ClaimType:   "DECISION",
					Status:      "ACTIVE",
					Subject:     "Runtime journal is canonical",
					Body:        "Operators should trust runtime truth over archived traces.",
					Summary:     "Canonical runtime journal decision.",
					SourceKind:  "manual",
					SourceID:    "developer",
					AgentID:     "agent-claim",
				}); err != nil {
					t.Fatalf("record clean knowledge claim: %v", err)
				}
				if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: "ws-replay-claim-clean",
					ClaimID:     "claim-clean",
					ActorID:     "developer",
					Reason:      "operator review before rollout",
					ReviewDueAt: "2026-03-22T12:00:00Z",
					AssignedTo:  "developer",
				}); err != nil {
					t.Fatalf("request clean knowledge claim review: %v", err)
				}
				if _, err := store.ConfirmKnowledgeClaim(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: "ws-replay-claim-clean",
					ClaimID:     "claim-clean",
					ActorID:     "developer",
					Reason:      "review completed and confirmed",
				}); err != nil {
					t.Fatalf("confirm clean knowledge claim: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "pass")
				expectFindingCodes(t, report, nil)
				if report.Metrics.OpenQueueCount != 0 {
					t.Fatalf("expected no open claim review queue, got %+v", report.Metrics)
				}
				claim := requireReplayClaim(t, report, "claim-clean")
				if claim.Status != "CONFIRMED" || claim.ReviewedBy != "developer" {
					t.Fatalf("expected confirmed claim with reviewed_by developer, got %+v", claim)
				}
			},
		},
		{
			name:        "claim_lifecycle_warns",
			workspaceID: "ws-replay-claim-warn",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-claim-warn", "agent-claim-warn")

				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-review-missing-queue",
					WorkspaceID: "ws-replay-claim-warn",
					ClaimType:   "DECISION",
					Status:      "REVIEW",
					Subject:     "Review queue should exist",
					Body:        "This claim is intentionally left without a follow-up queue.",
					Summary:     "Missing review queue.",
					SourceKind:  "manual",
					AgentID:     "agent-claim-warn",
				}); err != nil {
					t.Fatalf("record review warning claim: %v", err)
				}
				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-superseded-missing-link",
					WorkspaceID: "ws-replay-claim-warn",
					ClaimType:   "PROCEDURE",
					Status:      "SUPERSEDED",
					Subject:     "Superseded claim should link successor",
					Body:        "This claim is intentionally missing superseded_by_claim_id.",
					Summary:     "Missing supersede link.",
					SourceKind:  "manual",
					AgentID:     "agent-claim-warn",
				}); err != nil {
					t.Fatalf("record superseded warning claim: %v", err)
				}
				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-confirmed-stale-queue",
					WorkspaceID: "ws-replay-claim-warn",
					ClaimType:   "FACT",
					Status:      "CONFIRMED",
					Subject:     "Confirmed claim should not keep review queue",
					Body:        "This claim intentionally leaves a stale queue behind.",
					Summary:     "Stale claim review queue.",
					SourceKind:  "manual",
					AgentID:     "agent-claim-warn",
				}); err != nil {
					t.Fatalf("record stale queue claim: %v", err)
				}
				if _, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
					WorkspaceID: "ws-replay-claim-warn",
					QueueKey:    "knowledge_claim:claim-confirmed-stale-queue:review",
					QueueType:   "FOLLOW_UP",
					Title:       "Stale claim review queue",
					Summary:     "This queue should have been resolved once the claim was confirmed.",
					SourceKind:  "knowledge_claim",
					SourceID:    "claim-confirmed-stale-queue",
					AgentID:     "agent-claim-warn",
				}); err != nil {
					t.Fatalf("create stale claim review queue: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "warn")
				expectFindingCodes(t, report, []string{
					"claim_missing_review_queue",
					"superseded_claim_missing_link",
					"stale_claim_review_queue",
				})
				summary := report.Evaluation.FindingSummary
				if summary.ClaimIntegrityCount != 3 ||
					summary.ClaimMissingReviewQueueCount != 1 ||
					summary.StaleClaimReviewQueueCount != 1 ||
					summary.SupersededClaimMissingLinkCount != 1 ||
					summary.ClaimMissingMemoryLinkCount != 0 ||
					summary.DuplicateActiveMemoryClaimCount != 0 ||
					summary.OtherFindingCount != 0 {
					t.Fatalf("expected claim warning replay fixture to surface claim-integrity subcounts, got %+v", summary)
				}
			},
		},
		{
			name:        "claim_review_escalation_clean",
			workspaceID: "ws-replay-claim-escalated",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-claim-escalated", "agent-claim-escalated")

				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-escalated",
					WorkspaceID: "ws-replay-claim-escalated",
					ClaimType:   "DECISION",
					Status:      "ACTIVE",
					Subject:     "Runtime journal is canonical",
					Body:        "Escalated review should keep the queue healthy.",
					Summary:     "Escalated review path.",
					SourceKind:  "manual",
					AgentID:     "agent-claim-escalated",
				}); err != nil {
					t.Fatalf("record escalated claim: %v", err)
				}
				if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: "ws-replay-claim-escalated",
					ClaimID:     "claim-escalated",
					ActorID:     "developer",
					Reason:      "needs operator review",
					ReviewDueAt: "2026-03-23T10:00:00Z",
					AssignedTo:  "reviewer-a",
				}); err != nil {
					t.Fatalf("request escalated review: %v", err)
				}
				if _, err := store.EscalateKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: "ws-replay-claim-escalated",
					ClaimID:     "claim-escalated",
					ActorID:     "developer",
					Reason:      "review is approaching SLA breach",
					ReviewDueAt: "2099-01-01T00:00:00Z",
					AssignedTo:  "reviewer-b",
					Urgency:     "CRITICAL",
				}); err != nil {
					t.Fatalf("escalate claim review: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "pass")
				expectFindingCodes(t, report, nil)
				queue := requireReplayQueue(t, report, "knowledge_claim:claim-escalated:review")
				if queue.EscalationCount != 1 || queue.Urgency != "CRITICAL" || queue.AssignedTo != "reviewer-b" {
					t.Fatalf("expected escalated healthy review queue, got %+v", queue)
				}
			},
		},
		{
			name:        "claim_review_overdue_warns",
			workspaceID: "ws-replay-claim-overdue",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-claim-overdue", "agent-claim-overdue")

				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-overdue",
					WorkspaceID: "ws-replay-claim-overdue",
					ClaimType:   "FACT",
					Status:      "ACTIVE",
					Subject:     "Overdue review should warn",
					Body:        "This review queue is intentionally overdue and unescalated.",
					Summary:     "Overdue review warning.",
					SourceKind:  "manual",
					AgentID:     "agent-claim-overdue",
				}); err != nil {
					t.Fatalf("record overdue claim: %v", err)
				}
				if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
					WorkspaceID: "ws-replay-claim-overdue",
					ClaimID:     "claim-overdue",
					ActorID:     "developer",
					Reason:      "left pending on purpose",
					ReviewDueAt: "2001-01-01T00:00:00Z",
					AssignedTo:  "reviewer-overdue",
				}); err != nil {
					t.Fatalf("request overdue review: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "warn")
				expectFindingCodes(t, report, []string{
					"overdue_operator_queue",
					"overdue_claim_review_unescalated",
				})
				summary := report.Evaluation.FindingSummary
				if summary.OperatorQueueIntegrityCount != 2 ||
					summary.OverdueOperatorQueueCount != 1 ||
					summary.OverdueClaimReviewUnescalatedCount != 1 ||
					summary.OtherFindingCount != 0 {
					t.Fatalf("expected overdue replay fixture to surface operator-queue integrity subcounts, got %+v", summary)
				}
			},
		},
		{
			name:        "stale_queue_and_run_warn",
			workspaceID: "ws-replay-stale",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-stale", "agent-stale")

				start := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventStart,
					WorkspaceID: "ws-replay-stale",
					SessionID:   "sess-stale",
					AgentID:     "agent-stale",
					Summary:     "Started stale control-plane scenario",
					UpdatedAt:   "2026-03-22T12:00:00Z",
				})
				_ = start
				blocked := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventBlocked,
					WorkspaceID: "ws-replay-stale",
					SessionID:   "sess-stale",
					AgentID:     "agent-stale",
					Summary:     "Blocked before stale replay snapshot",
					BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
					UpdatedAt:   "2026-03-22T12:02:00Z",
				})
				if _, err := store.SyncOperatorQueueFromSessionState(ctx, blocked); err != nil {
					t.Fatalf("sync stale queue fixture from blocked session state: %v", err)
				}
				ended := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
					EventType:   model.SessionEventEnd,
					WorkspaceID: "ws-replay-stale",
					SessionID:   "sess-stale",
					AgentID:     "agent-stale",
					Summary:     "Session closed but stale obligations remain",
					UpdatedAt:   "2026-03-22T12:05:00Z",
				})
				_ = ended

				result, err := store.DB().ExecContext(ctx, `
					UPDATE operator_queue_items
					   SET status = 'OPEN',
					       resolution = '',
					       resolved_at = NULL,
					       resolved_by = NULL,
					       updated_at = ?
					 WHERE workspace_id = ?
					   AND queue_key = ?`,
					"2026-03-22T12:06:00Z",
					"ws-replay-stale",
					"session:sess-stale:blocker",
				)
				if err != nil {
					t.Fatalf("reopen stale queue item fixture: %v", err)
				}
				if rows, err := result.RowsAffected(); err != nil {
					t.Fatalf("rows affected for stale queue item fixture: %v", err)
				} else if rows != 1 {
					t.Fatalf("expected exactly one stale queue row, got %d", rows)
				}
				if _, err := store.DB().ExecContext(ctx, `
INSERT INTO execution_runs(run_id, workspace_id, session_id, agent_id, title, summary, status, outcome, verification_json, created_at, updated_at, closed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
					"run-stale",
					"ws-replay-stale",
					"sess-stale",
					"agent-stale",
					"Stale execution run",
					"Execution run stayed active after terminal session",
					"ACTIVE",
					"",
					`{"legacy_invalid_fixture":"stale execution run intentionally bypasses public admission"}`,
					"2026-03-22T12:06:00Z",
					"2026-03-22T12:06:00Z",
				); err != nil {
					t.Fatalf("create stale execution run fixture: %v", err)
				}
				runPayload := map[string]any{
					"run_id":       "run-stale",
					"workspace_id": "ws-replay-stale",
					"session_id":   "sess-stale",
					"agent_id":     "agent-stale",
					"title":        "Stale execution run",
					"summary":      "Execution run stayed active after terminal session",
					"status":       "ACTIVE",
				}
				runPayloadJSON, err := json.Marshal(runPayload)
				if err != nil {
					t.Fatalf("marshal stale execution run payload: %v", err)
				}
				if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
					EventID:     "rtev:run-stale:execution_run.written:2026-03-22T12:06:00Z",
					WorkspaceID: "ws-replay-stale",
					EventType:   "execution_run.written",
					EntityType:  "execution_run",
					EntityID:    "run-stale",
					ActorType:   "agent",
					ActorID:     "agent-stale",
					AgentID:     "agent-stale",
					SessionID:   "sess-stale",
					PayloadJSON: string(runPayloadJSON),
					CreatedAt:   "2026-03-22T12:06:00Z",
				}); err != nil {
					t.Fatalf("record stale execution run event fixture: %v", err)
				}
				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-stale",
					WorkspaceID: "ws-replay-stale",
					ClaimType:   "DECISION",
					Status:      "ACTIVE",
					Subject:     "Runtime journal is canonical",
					Body:        "But this claim intentionally omits memory linkage.",
					Summary:     "Intentional stale claim for replay regression.",
					SourceKind:  "workspace_memory",
					AgentID:     "agent-stale",
				}); err != nil {
					t.Fatalf("record stale knowledge claim: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "warn")
				expectFindingCodes(t, report, []string{
					"stale_open_queue",
					"execution_run_out_of_sync",
					"execution_run_without_steps",
					"claim_missing_memory_link",
				})
				summary := report.Evaluation.FindingSummary
				if summary.ExecutionRunIntegrityCount != 2 ||
					summary.ExecutionRunOutOfSyncCount != 1 ||
					summary.ExecutionRunWithoutStepsCount != 1 ||
					summary.ClaimIntegrityCount != 1 ||
					summary.ClaimMissingMemoryLinkCount != 1 ||
					summary.OperatorQueueIntegrityCount != 1 ||
					summary.StaleOpenQueueCount != 1 ||
					summary.OtherFindingCount != 0 {
					t.Fatalf("expected stale replay fixture to surface execution-run, claim, and operator-queue integrity subcounts, got %+v", summary)
				}
			},
		},
		{
			name:        "duplicate_memory_claim_warns",
			workspaceID: "ws-replay-memory",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-memory", "agent-memory")

				memory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
					WorkspaceID: "ws-replay-memory",
					MemoryID:    "memory-dup",
					MemoryType:  "DECISION",
					Title:       "Use runtime journal as canonical truth",
					Body:        "Operators should prefer runtime events over archived traces.",
					Summary:     "Runtime journal is the source of truth.",
					AgentID:     "agent-memory",
					SourceKind:  "manual",
					SourceID:    "developer",
				})
				if err != nil {
					t.Fatalf("record workspace memory: %v", err)
				}
				if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
					ClaimID:     "claim-manual-duplicate",
					WorkspaceID: "ws-replay-memory",
					ClaimType:   "DECISION",
					Status:      "ACTIVE",
					Subject:     "Runtime journal is canonical",
					Body:        "Manual duplicate active claim for the same memory.",
					Summary:     "Intentional duplicate claim.",
					SourceKind:  "workspace_memory",
					SourceID:    memory.MemoryID,
					MemoryID:    memory.MemoryID,
					AgentID:     "agent-memory",
				}); err != nil {
					t.Fatalf("record duplicate knowledge claim: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "warn")
				expectFindingCodes(t, report, []string{"duplicate_active_memory_claim"})
				summary := report.Evaluation.FindingSummary
				if summary.ClaimIntegrityCount != 1 ||
					summary.DuplicateActiveMemoryClaimCount != 1 ||
					summary.OtherFindingCount != 0 {
					t.Fatalf("expected duplicate-memory replay fixture to surface claim-integrity subcounts, got %+v", summary)
				}
			},
		},
		{
			name:        "policy_deny_is_journaled",
			workspaceID: "ws-replay-policy",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-policy")
				authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-replay-policy")

				if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
					WorkspaceID: "ws-replay-policy",
					SubjectType: "agent",
					SubjectID:   "agent-governed",
					Capability:  "tool.call",
					ToolID:      "dangerous-tool",
					Effect:      "DENY",
					Reason:      "manual approval required",
					CreatedBy:   "developer",
				}); err != nil {
					t.Fatalf("put capability policy: %v", err)
				}
				_, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
					EventID:     "rtev-policy-denied",
					WorkspaceID: "ws-replay-policy",
					EventType:   "tool.call.denied",
					EntityType:  "tool",
					EntityID:    "dangerous-tool",
					ActorType:   "agent",
					ActorID:     "agent-governed",
					PayloadJSON: toolCallRuntimeEventPayloadJSONForTest(
						t,
						"ws-replay-policy",
						"dangerous-tool",
						"tool.call.denied",
						"agent",
						"agent-governed",
						"tool.call",
						"",
						map[string]any{
							"policy_check": map[string]any{
								"verdict":        "DENY",
								"matched_policy": map[string]any{"tool_id": "dangerous-tool"},
							},
						},
					),
					CreatedAt: "2026-03-22T13:00:00Z",
				})
				if err != nil {
					t.Fatalf("record tool deny runtime event: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "pass")
				if report.Metrics.EventTypeCounts["capability_policy.put"] != 1 || report.Metrics.EventTypeCounts["tool.call.denied"] != 1 {
					t.Fatalf("expected policy and tool denial journal events, got %+v", report.Metrics.EventTypeCounts)
				}
			},
		},
		{
			name:        "malformed_queue_payload_fails",
			workspaceID: "ws-replay-malformed-queue",
			setup: func(t *testing.T, ctx context.Context, store *sqlite.Store) {
				setupReplayWorkspace(t, ctx, store, "ws-replay-malformed-queue")
				if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
					EventID:     "rtev-malformed-queue",
					WorkspaceID: "ws-replay-malformed-queue",
					EventType:   "operator_queue.opened",
					EntityType:  "operator_queue",
					EntityID:    "queue-malformed",
					ActorType:   "system",
					ActorID:     "tests",
					PayloadJSON: `{"queue_key":`,
					CreatedAt:   "2026-03-22T15:00:00Z",
				}); err != nil {
					t.Fatalf("record malformed runtime event: %v", err)
				}
			},
			assert: func(t *testing.T, report sqlite.RuntimeReplayReport) {
				expectReplayVerdict(t, report, "fail")
				expectFindingCodes(t, report, []string{"malformed_event_payload"})
				summary := report.Evaluation.FindingSummary
				if summary.PayloadIntegrityCount != 1 ||
					summary.MalformedEventPayloadCount != 1 ||
					summary.ErrorFindingCount != 1 ||
					summary.OtherFindingCount != 0 {
					t.Fatalf("expected malformed replay fixture to surface payload-integrity subcounts, got %+v", summary)
				}
				if report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 1 {
					t.Fatalf("expected malformed replay finding to stay source-addressable, got %+v", report.Evaluation.ProvenanceSummary)
				}
			},
		},
	}

	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			scenario.setup(t, ctx, store)

			report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
				WorkspaceID: scenario.workspaceID,
				Limit:       256,
			})
			if err != nil {
				t.Fatalf("replay runtime journal: %v", err)
			}
			if report.Truncated {
				t.Fatalf("expected full replay report, got truncated %+v", report)
			}
			scenario.assert(t, report)
		})
	}
}

func TestRuntimeReplayTruncatedReflectsActualOverflow(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-runtime-replay-truncated"
	setupReplayWorkspace(t, ctx, store, workspaceID)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	for idx, eventID := range []string{
		"rtev-truncated-1",
		"rtev-truncated-2",
		"rtev-truncated-3",
	} {
		toolID := "tool-" + strconv.Itoa(idx+1)
		actorID := "agent-" + strconv.Itoa(idx+1)
		if _, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
			EventID:     eventID,
			WorkspaceID: workspaceID,
			EventType:   "tool.call.denied",
			EntityType:  "tool",
			EntityID:    toolID,
			ActorType:   "agent",
			ActorID:     actorID,
			PayloadJSON: toolCallRuntimeEventPayloadJSONForTest(
				t,
				workspaceID,
				toolID,
				"tool.call.denied",
				"agent",
				actorID,
				"tool.call",
				"",
				nil,
			),
			CreatedAt: fmt.Sprintf("2026-03-22T10:0%d:00Z", idx),
		}); err != nil {
			t.Fatalf("record runtime event %s: %v", eventID, err)
		}
	}

	fullReport, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       3,
	})
	if err != nil {
		t.Fatalf("replay runtime journal full limit: %v", err)
	}
	if fullReport.Truncated {
		t.Fatalf("expected exact-limit full replay to stay untruncated, got %+v", fullReport)
	}
	if !fullReport.Scope.Authoritative || fullReport.Scope.IntegrityBand != "COMPLETE" || len(fullReport.Scope.Reasons) != 0 {
		t.Fatalf("expected exact-limit full replay to remain authoritative, got %+v", fullReport.Scope)
	}
	if len(fullReport.Events) != 3 {
		t.Fatalf("expected exact-limit full replay to keep all events, got %+v", fullReport.Events)
	}
	if hasReplayFindingCode(fullReport, "replay_scope_partial") || fullReport.Evaluation.FindingSummary.ScopePartialCount != 0 {
		t.Fatalf("expected exact-limit full replay to avoid partial-scope finding, got %+v", fullReport.Evaluation)
	}

	partialReport, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       2,
	})
	if err != nil {
		t.Fatalf("replay runtime journal overflow limit: %v", err)
	}
	if !partialReport.Truncated {
		t.Fatalf("expected overflow replay to stay truncated, got %+v", partialReport)
	}
	if partialReport.Scope.Authoritative || partialReport.Scope.IntegrityBand != "PARTIAL" || !slices.Contains(partialReport.Scope.Reasons, "truncated_window") {
		t.Fatalf("expected overflow replay to become partial because of truncation, got %+v", partialReport.Scope)
	}
	if len(partialReport.Events) != 2 {
		t.Fatalf("expected overflow replay to keep canonical limit, got %+v", partialReport.Events)
	}
	if !hasReplayFindingCode(partialReport, "replay_scope_partial") || partialReport.Evaluation.FindingSummary.ScopePartialCount != 1 {
		t.Fatalf("expected overflow replay to surface partial-scope finding, got %+v", partialReport.Evaluation)
	}
}

func TestRuntimeReplayFilteredScopeStaysNonAuthoritativeWithoutWindowTruncation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-replay-filtered-scope"
	setupReplayWorkspace(t, ctx, store, workspaceID, "agent-filtered")

	recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-filtered",
		AgentID:     "agent-filtered",
		Summary:     "Filtered replay scope seed",
		OwnerScope:  "task/session",
		UpdatedAt:   "2026-03-22T10:00:00Z",
	})
	recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventKeepalive,
		WorkspaceID: workspaceID,
		SessionID:   "sess-filtered",
		AgentID:     "agent-filtered",
		Summary:     "Filtered replay scope keepalive",
		UpdatedAt:   "2026-03-22T10:05:00Z",
	})

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		AgentID:     "agent-filtered",
		Limit:       16,
	})
	if err != nil {
		t.Fatalf("replay filtered runtime journal: %v", err)
	}

	if report.Truncated || report.WindowIncomplete {
		t.Fatalf("expected filtered replay scope to stay structurally intact but non-authoritative, got %+v", report)
	}
	if report.Scope.Authoritative || report.Scope.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected filtered replay scope to remain partial, got %+v", report.Scope)
	}
	if !slices.Contains(report.Scope.Reasons, "agent_filtered_scope") {
		t.Fatalf("expected filtered replay scope reason, got %+v", report.Scope)
	}
	if !slices.Contains(report.Scope.SuppressedConclusions, "negative_absence_claims") || !slices.Contains(report.Scope.SuppressedConclusions, "rollback_trace_absence") {
		t.Fatalf("expected filtered replay scope to advertise suppressed negative conclusions, got %+v", report.Scope)
	}
	if !hasReplayFindingCode(report, "replay_scope_partial") || report.Evaluation.FindingSummary.ScopePartialCount != 1 {
		t.Fatalf("expected filtered replay scope to surface partial-scope finding, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayExcludeSyntheticStaysNonAuthoritative(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-replay-exclude-synthetic"
	setupReplayWorkspace(t, ctx, store, workspaceID, "agent-synthetic")

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-synthetic-snapshot",
		WorkspaceID: workspaceID,
		EventType:   "controlplane.snapshot",
		EntityType:  "instrumentation_control_plane",
		EntityID:    "snapshot-1",
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{"kind":"snapshot"}`,
		CreatedAt:   "2026-03-22T10:00:00Z",
	}); err != nil {
		t.Fatalf("record synthetic replay event: %v", err)
	}
	recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-synthetic",
		AgentID:     "agent-synthetic",
		Summary:     "Synthetic filtered replay seed",
		OwnerScope:  "task/session",
		UpdatedAt:   "2026-03-22T10:05:00Z",
	})

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID:      workspaceID,
		ExcludeSynthetic: true,
		Limit:            16,
	})
	if err != nil {
		t.Fatalf("replay synthetic-filtered runtime journal: %v", err)
	}

	if report.Truncated || report.WindowIncomplete {
		t.Fatalf("expected synthetic-filtered replay to stay structurally intact but non-authoritative, got %+v", report)
	}
	if report.Scope.Authoritative || report.Scope.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected synthetic-filtered replay to remain partial, got %+v", report.Scope)
	}
	if !slices.Contains(report.Scope.Reasons, "synthetic_events_excluded") {
		t.Fatalf("expected synthetic-filtered replay scope reason, got %+v", report.Scope)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected synthetic-filtered replay to degrade verdict to warn, got %+v", report.Evaluation)
	}
	if !hasReplayFindingCode(report, "replay_scope_partial") || report.Evaluation.FindingSummary.ScopePartialCount != 1 {
		t.Fatalf("expected synthetic-filtered replay to surface partial-scope finding, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayBlockedDecisionResumeScenarioStaysPartialUnderBoundedWindow(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-replay-clean-bounded-scenario"
	setupReplayWorkspace(t, ctx, store, workspaceID, "agent-clean")

	start := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-clean",
		AgentID:     "agent-clean",
		Summary:     "Taking ownership of transport recovery",
		OwnerScope:  "task/session",
		UpdatedAt:   "2026-03-22T09:00:00Z",
	})
	syncReplayExecutionFromSessionState(t, ctx, store, start)

	keepFalse := false
	blocked := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventBlocked,
		WorkspaceID:       workspaceID,
		SessionID:         "sess-clean",
		AgentID:           "agent-clean",
		Summary:           "Blocked waiting for bridge wake acknowledgement",
		BlockedOn:         []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake ack timeout"}},
		UpdatedAt:         "2026-03-22T09:05:00Z",
		KeepSessionActive: &keepFalse,
	})
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blocked); err != nil {
		t.Fatalf("sync blocked operator queue: %v", err)
	}
	syncReplayExecutionFromSessionState(t, ctx, store, blocked)

	decision := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:          model.SessionEventDecisionNeeded,
		WorkspaceID:        workspaceID,
		SessionID:          "sess-clean",
		AgentID:            "agent-clean",
		Summary:            "Need operator decision on retry policy",
		DecisionNeededFrom: "developer",
		DecisionType:       "retry_policy",
		UpdatedAt:          "2026-03-22T09:10:00Z",
		KeepSessionActive:  &keepFalse,
	})
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, decision); err != nil {
		t.Fatalf("sync decision operator queue: %v", err)
	}
	syncReplayExecutionFromSessionState(t, ctx, store, decision)

	keepTrue := true
	resumed := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventKeepalive,
		WorkspaceID:       workspaceID,
		SessionID:         "sess-clean",
		AgentID:           "agent-clean",
		Status:            model.SessionStatusActive,
		Summary:           "Decision received and transport resumed",
		UpdatedAt:         "2026-03-22T09:15:00Z",
		KeepSessionActive: &keepTrue,
	})
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, resumed); err != nil {
		t.Fatalf("sync keepalive operator queue: %v", err)
	}
	syncReplayExecutionFromSessionState(t, ctx, store, resumed)

	ended := recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   "sess-clean",
		AgentID:     "agent-clean",
		Summary:     "Transport recovery completed cleanly",
		UpdatedAt:   "2026-03-22T09:20:00Z",
	})
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, ended); err != nil {
		t.Fatalf("sync ended operator queue: %v", err)
	}
	syncReplayExecutionFromSessionState(t, ctx, store, ended)

	fullReport, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       64,
	})
	if err != nil {
		t.Fatalf("replay full runtime journal: %v", err)
	}
	expectReplayVerdict(t, fullReport, "pass")
	if fullReport.Truncated || hasReplayFindingCode(fullReport, "replay_scope_partial") {
		t.Fatalf("expected full replay to stay complete, got %+v", fullReport)
	}

	partialReport, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       3,
	})
	if err != nil {
		t.Fatalf("replay bounded runtime journal: %v", err)
	}
	if !partialReport.Truncated || !partialReport.WindowIncomplete {
		t.Fatalf("expected bounded replay scenario to remain incomplete, got %+v", partialReport)
	}
	expectReplayVerdict(t, partialReport, "warn")
	if !hasReplayFindingCode(partialReport, "replay_scope_partial") || partialReport.Evaluation.FindingSummary.ScopePartialCount != 1 {
		t.Fatalf("expected bounded replay scenario to surface partial-scope warning, got %+v", partialReport.Evaluation)
	}
	summary := partialReport.Evaluation.FindingSummary
	if summary.ExecutionRunIntegrityCount != 0 || summary.OperatorQueueIntegrityCount != 0 || summary.ClaimIntegrityCount != 0 || summary.MissingParentCount != 0 {
		t.Fatalf("expected bounded replay scenario to stay partial without fabricating integrity drift, got %+v", summary)
	}
}

func TestRuntimeReplaySurfacesRetentionRiskForCompactionArtifacts(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                   string
		seed                   func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, sessionID string) string
		wantBand               string
		wantCandidateCount     int
		wantSnapshotCount      int
		wantEpisodePackCount   int
		wantReasons            []string
		wantFindingCodes       []string
		wantAbsentFindingCodes []string
	}{
		{
			name: "watch_candidate",
			seed: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, sessionID string) string {
				if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
					SessionID:   sessionID,
					Sequence:    0,
					Role:        "user",
					ContentJSON: `[{"type":"text","text":"candidate trace"}]`,
					TokenCount:  128,
				}); err != nil {
					t.Fatalf("append candidate session message: %v", err)
				}
				return ""
			},
			wantBand:               "WATCH",
			wantCandidateCount:     1,
			wantSnapshotCount:      0,
			wantEpisodePackCount:   0,
			wantReasons:            []string{"session_compaction_candidate_present"},
			wantFindingCodes:       []string{"runtime_event_retention_compaction_candidate"},
			wantAbsentFindingCodes: []string{"runtime_event_retention_compacted_session", "runtime_event_retention_snapshot_without_episode_pack"},
		},
		{
			name: "compacted_with_episode_pack",
			seed: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, sessionID string) string {
				snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
					SessionID:           sessionID,
					WorkspaceID:         workspaceID,
					AgentID:             agentID,
					TriggerKind:         "token_budget_exceeded",
					PackMode:            "DETERMINISTIC_FALLBACK",
					SourceWindowDigest:  "digest-replay-compact",
					MessageCountBefore:  6,
					MessageCountAfter:   2,
					MessageTokensBefore: 1800,
					MessageTokensAfter:  640,
					TotalInputTokens:    2400,
					TotalOutputTokens:   900,
					SummaryText:         "[Previous conversation history was truncated due to length. 4 messages were removed.]",
				})
				if err != nil {
					t.Fatalf("record compaction snapshot: %v", err)
				}
				return snapshot.CreatedAt
			},
			wantBand:               "COMPACTED",
			wantCandidateCount:     0,
			wantSnapshotCount:      1,
			wantEpisodePackCount:   1,
			wantReasons:            []string{"session_compaction_snapshot_present"},
			wantFindingCodes:       []string{"runtime_event_retention_compacted_session"},
			wantAbsentFindingCodes: []string{"runtime_event_retention_snapshot_without_episode_pack"},
		},
		{
			name: "at_risk_snapshot_without_episode_pack",
			seed: func(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, sessionID string) string {
				snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
					SessionID:           sessionID,
					WorkspaceID:         workspaceID,
					AgentID:             agentID,
					TriggerKind:         "token_budget_exceeded",
					PackMode:            "DETERMINISTIC_FALLBACK",
					SourceWindowDigest:  "digest-replay-risk",
					MessageCountBefore:  7,
					MessageCountAfter:   3,
					MessageTokensBefore: 2100,
					MessageTokensAfter:  700,
					TotalInputTokens:    2800,
					TotalOutputTokens:   950,
					SummaryText:         "[Previous conversation history was truncated due to length. 4 messages were removed.]",
				})
				if err != nil {
					t.Fatalf("record compaction snapshot: %v", err)
				}
				if _, err := store.DB().ExecContext(ctx, `UPDATE episode_packs SET compaction_snapshot_id = NULL WHERE pack_id = ?`, snapshot.EpisodePackID); err != nil {
					t.Fatalf("detach episode pack from compaction snapshot: %v", err)
				}
				return snapshot.CreatedAt
			},
			wantBand:               "AT_RISK",
			wantCandidateCount:     0,
			wantSnapshotCount:      1,
			wantEpisodePackCount:   0,
			wantReasons:            []string{"session_compaction_snapshot_present", "snapshot_without_episode_pack"},
			wantFindingCodes:       []string{"runtime_event_retention_compacted_session", "runtime_event_retention_snapshot_without_episode_pack"},
			wantAbsentFindingCodes: []string{"runtime_event_retention_compaction_candidate"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()

			workspaceID := "ws-replay-retention-" + tc.name
			agentID := "agent-" + tc.name
			sessionID := "sess-" + tc.name

			setupReplayWorkspace(t, ctx, store, workspaceID, agentID)
			recordReplaySessionEvent(t, ctx, store, sqlite.AgentSessionCoordinationInput{
				EventType:   model.SessionEventStart,
				WorkspaceID: workspaceID,
				SessionID:   sessionID,
				AgentID:     agentID,
				Summary:     "Retention replay fixture",
				UpdatedAt:   "2026-03-26T09:00:00Z",
			})

			latestSnapshotAt := tc.seed(t, ctx, store, workspaceID, agentID, sessionID)

			report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
				WorkspaceID: workspaceID,
				Limit:       64,
			})
			if err != nil {
				t.Fatalf("replay runtime journal: %v", err)
			}
			if report.Evaluation.Verdict != "warn" || report.Evaluation.WarningCount != 1 {
				t.Fatalf("expected replay retention fixture to keep only the existing non-retention warning severity, got %+v", report.Evaluation)
			}
			if !hasReplaySession(report, sessionID) {
				t.Fatalf("expected replay retention fixture to keep session %s visible, got %+v", sessionID, report.Sessions)
			}

			risk := report.Evaluation.RetentionRisk
			if risk.Band != tc.wantBand {
				t.Fatalf("expected retention band %s, got %+v", tc.wantBand, risk)
			}
			summary := report.Evaluation.FindingSummary
			if summary.RetentionFindingCount != len(tc.wantFindingCodes) {
				t.Fatalf("expected retention finding summary to mirror retention findings, got %+v", summary)
			}
			wantCompactionCandidateCount := 0
			wantCompactedSessionCount := 0
			wantSnapshotWithoutEpisodePackCount := 0
			for _, code := range tc.wantFindingCodes {
				switch code {
				case "runtime_event_retention_compaction_candidate":
					wantCompactionCandidateCount++
				case "runtime_event_retention_compacted_session":
					wantCompactedSessionCount++
				case "runtime_event_retention_snapshot_without_episode_pack":
					wantSnapshotWithoutEpisodePackCount++
				}
			}
			if summary.RetentionCompactionCandidateCount != wantCompactionCandidateCount ||
				summary.RetentionCompactedSessionCount != wantCompactedSessionCount ||
				summary.RetentionSnapshotWithoutEpisodePackCount != wantSnapshotWithoutEpisodePackCount {
				t.Fatalf("expected retention finding subfamily counts to mirror retention findings, got %+v", summary)
			}
			if summary.InfoFindingCount != len(tc.wantFindingCodes) {
				t.Fatalf("expected retention findings to stay info-only in summary, got %+v", summary)
			}
			if report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 0 ||
				report.Evaluation.ProvenanceSummary.FindingsWithRootCauseID != 0 ||
				report.Evaluation.ProvenanceSummary.FindingsWithProvenanceGroupID != 0 ||
				report.Evaluation.ProvenanceSummary.FindingsWithParentRefs != 0 ||
				report.Evaluation.ProvenanceSummary.FullLineageFieldFindingCount != 0 {
				t.Fatalf("expected retention-only findings to avoid adding root/provenance/parent lineage counts, got %+v", report.Evaluation.ProvenanceSummary)
			}
			if risk.CompactionCandidateCount != tc.wantCandidateCount || risk.CompactionSnapshotCount != tc.wantSnapshotCount || risk.EpisodePackCount != tc.wantEpisodePackCount {
				t.Fatalf("unexpected replay retention counts %+v", risk)
			}
			if tc.wantSnapshotCount > 0 {
				if latestSnapshotAt == "" || risk.LatestSnapshotAt != latestSnapshotAt {
					t.Fatalf("expected latest snapshot at %q, got %+v", latestSnapshotAt, risk)
				}
				if len(risk.SnapshotSessionIDs) != 1 || risk.SnapshotSessionIDs[0] != sessionID {
					t.Fatalf("expected snapshot session ids to keep %s, got %+v", sessionID, risk.SnapshotSessionIDs)
				}
			}
			if tc.wantCandidateCount > 0 {
				if len(risk.CandidateSessionIDs) != 1 || risk.CandidateSessionIDs[0] != sessionID {
					t.Fatalf("expected candidate session ids to keep %s, got %+v", sessionID, risk.CandidateSessionIDs)
				}
			}
			for _, reason := range tc.wantReasons {
				if !hasRuntimeReplayRetentionReason(risk, reason) {
					t.Fatalf("expected retention reason %s, got %+v", reason, risk)
				}
			}
			for _, code := range tc.wantFindingCodes {
				requireReplayFinding(t, report, code)
			}
			for _, code := range tc.wantAbsentFindingCodes {
				if hasReplayFindingCode(report, code) {
					t.Fatalf("expected replay retention fixture to avoid finding %s, got %+v", code, report.Evaluation.Findings)
				}
			}
		})
	}
}

func TestRuntimeReplayDedupKeyEquivalentEventsRemainIdempotent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-replay-dedup-equivalent",
		Title:       "Replay Dedup Equivalent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-dedup-a",
		WorkspaceID: "ws-replay-dedup-equivalent",
		EventType:   "operator_queue.upserted",
		EntityType:  "operator_queue",
		EntityID:    "queue-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"queue_id":"queue-1","queue_key":"manual:queue-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Replay queue","summary":"Duplicate logical event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"D-bridge-42","root_cause_id":"RC-bridge-42","provenance_group_id":"PG-bridge-42"}`,
		CreatedAt:   "2026-03-22T16:00:00Z",
	}); err != nil {
		t.Fatalf("record replay dedup event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-dedup-b",
		"D-bridge-42",
		"ws-replay-dedup-equivalent",
		"operator_queue.upserted",
		"operator_queue",
		"queue-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"RC-bridge-42",
		"PG-bridge-42",
		`[]`,
		`{"queue_id":"queue-1","queue_key":"manual:queue-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Replay queue","summary":"Duplicate logical event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-22T16:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert replay dedup duplicate row: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-replay-dedup-equivalent",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Metrics.TotalEvents != 2 || report.Metrics.AppliedEvents != 1 || report.Metrics.SuppressedDuplicateEvents != 1 || report.Metrics.ConflictingDuplicateKeys != 0 {
		t.Fatalf("expected replay metrics to collapse equivalent dedup_key event once, got %+v", report.Metrics)
	}
	if len(report.Events) != 2 {
		t.Fatalf("expected raw persisted journal to remain visible during replay, got %+v", report.Events)
	}
	if len(report.Queues) != 1 || report.Queues[0].QueueID != "queue-1" || report.Queues[0].EventCount != 1 || report.Queues[0].Status != "OPEN" {
		t.Fatalf("expected replay reducers to apply equivalent dedup_key event once, got %+v", report.Queues)
	}
	if report.Evaluation.Verdict != "pass" || len(report.Evaluation.Findings) != 0 {
		t.Fatalf("expected clean replay for equivalent dedup_key duplicate, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayDedupKeyConflictsStayVisibleAsExplicitRisk(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-replay-dedup-conflict",
		Title:       "Replay Dedup Conflict",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-conflict-a",
		WorkspaceID: "ws-replay-dedup-conflict",
		EventType:   "operator_queue.upserted",
		EntityType:  "operator_queue",
		EntityID:    "queue-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"queue_id":"queue-1","queue_key":"manual:queue-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Replay queue","summary":"Conflicting logical event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"D-bridge-43","root_cause_id":"RC-bridge-43","provenance_group_id":"PG-bridge-43"}`,
		CreatedAt:   "2026-03-22T17:00:00Z",
	}); err != nil {
		t.Fatalf("record replay conflict event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-conflict-b",
		"D-bridge-43",
		"ws-replay-dedup-conflict",
		"operator_queue.resolved",
		"operator_queue",
		"queue-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"RC-bridge-43",
		"PG-bridge-43",
		`[]`,
		`{"queue_id":"queue-1","queue_key":"manual:queue-1","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Replay queue","summary":"Conflicting logical event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"resolved manually","resolved_by":"operator-1"}`,
		"2026-03-22T17:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert replay conflict duplicate row: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-replay-dedup-conflict",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Metrics.TotalEvents != 2 || report.Metrics.AppliedEvents != 1 || report.Metrics.SuppressedDuplicateEvents != 1 || report.Metrics.ConflictingDuplicateKeys != 1 {
		t.Fatalf("expected replay metrics to surface conflicting dedup_key without double-applying it, got %+v", report.Metrics)
	}
	if len(report.Events) != 2 {
		t.Fatalf("expected raw conflicting events to remain visible in replay report, got %+v", report.Events)
	}
	if len(report.Queues) != 1 || report.Queues[0].EventCount != 1 || report.Queues[0].Status != "OPEN" {
		t.Fatalf("expected conflicting dedup_key event to stay out of reducer state, got %+v", report.Queues)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected conflicting dedup_key to remain visible as replay warning, got %+v", report.Evaluation)
	}
	expectFindingCodes(t, report, []string{"runtime_event_dedup_conflict"})
}

func TestRuntimeReplayReportsSourceAddressableConflictFindingAndCausalOrder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-source-addressable",
		Title:       "Runtime Replay Source Addressable",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-source-legacy",
		nil,
		"ws-runtime-replay-source-addressable",
		"legacy.signal",
		"legacy_event",
		"legacy-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		nil,
		nil,
		`[]`,
		`{"message":"legacy event"}`,
		"2026-03-22T19:59:00Z",
		1,
	); err != nil {
		t.Fatalf("insert legacy runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-source-open",
		"D-source-1",
		"ws-runtime-replay-source-addressable",
		"operator_queue.opened",
		"operator_queue",
		"queue-source-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-source-1",
		"prov-source-1",
		`[]`,
		`{"queue_key":"queue:source","queue_type":"FOLLOW_UP","status":"OPEN","title":"Source queue","summary":"Opened first","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"D-source-1","root_cause_id":"root-source-1","provenance_group_id":"prov-source-1"}`,
		"2026-03-22T20:00:00Z",
		2,
	); err != nil {
		t.Fatalf("insert source open runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-source-parent",
		nil,
		"ws-runtime-replay-source-addressable",
		"legacy.signal",
		"legacy_event",
		"legacy-parent",
		"system",
		"tests",
		nil,
		nil,
		nil,
		nil,
		nil,
		`[]`,
		`{"message":"parent arrives later"}`,
		"2026-03-22T20:02:00Z",
		4,
	); err != nil {
		t.Fatalf("insert parent runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-source-close",
		"D-source-1",
		"ws-runtime-replay-source-addressable",
		"operator_queue.resolved",
		"operator_queue",
		"queue-source-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-source-1",
		"prov-source-1",
		`["rtev-source-parent"]`,
		`{"queue_key":"queue:source","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Source queue","summary":"Resolved later","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1","root_cause_id":"root-source-1","provenance_group_id":"prov-source-1","parent_refs_json":["rtev-source-parent"]}`,
		"2026-03-22T20:01:00Z",
		3,
	); err != nil {
		t.Fatalf("insert source close runtime event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-source-addressable",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if len(report.Events) != 4 || report.Events[0].EventID != "rtev-source-parent" || report.Events[1].EventID != "rtev-source-close" || report.Events[2].EventID != "rtev-source-open" || report.Events[3].EventID != "rtev-source-legacy" {
		t.Fatalf("expected replay events to follow ingest sequence, got %+v", report.Events)
	}
	if report.Metrics.TotalEvents != 4 || report.Metrics.AppliedEvents != 3 || report.Metrics.SuppressedDuplicateEvents != 1 || report.Metrics.ConflictingDuplicateKeys != 1 {
		t.Fatalf("expected replay metrics to surface one conflicting dedup_key and preserve order, got %+v", report.Metrics)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected conflicting duplicate replay to warn, got %+v", report.Evaluation)
	}
	if report.Evaluation.FindingSummary.TotalFindings != 2 ||
		report.Evaluation.FindingSummary.DedupConflictCount != 1 ||
		report.Evaluation.FindingSummary.CausalOrderCount != 1 ||
		report.Evaluation.FindingSummary.MissingParentCount != 0 ||
		report.Evaluation.FindingSummary.CycleCount != 0 ||
		report.Evaluation.FindingSummary.CycleSelfParentCount != 0 ||
		report.Evaluation.FindingSummary.CycleParentComponentCount != 0 ||
		report.Evaluation.FindingSummary.RetentionFindingCount != 0 ||
		report.Evaluation.FindingSummary.RetentionCompactionCandidateCount != 0 ||
		report.Evaluation.FindingSummary.RetentionCompactedSessionCount != 0 ||
		report.Evaluation.FindingSummary.RetentionSnapshotWithoutEpisodePackCount != 0 ||
		report.Evaluation.FindingSummary.ScopePartialCount != 0 {
		t.Fatalf("expected bounded replay finding summary for source-addressable conflict fixture, got %+v", report.Evaluation.FindingSummary)
	}
	if report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithRootCauseID != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithProvenanceGroupID != 2 ||
		report.Evaluation.ProvenanceSummary.FindingsWithParentRefs != 2 ||
		report.Evaluation.ProvenanceSummary.FullLineageFieldFindingCount != 2 {
		t.Fatalf("expected replay provenance summary to mirror source-addressable findings, got %+v", report.Evaluation.ProvenanceSummary)
	}
	conflict := requireReplayFinding(t, report, "runtime_event_dedup_conflict")
	if conflict.EntityType != "operator_queue" || conflict.EntityID != "queue-source-1" || conflict.SourceEventID != "rtev-source-close" || conflict.SourceEventType != "operator_queue.resolved" {
		t.Fatalf("expected source-addressable conflict finding, got %+v", conflict)
	}
	if conflict.SourceDedupKey != "D-source-1" || conflict.SourceRootCauseID != "root-source-1" || conflict.SourceProvenanceGroupID != "prov-source-1" || conflict.SourceParentRefsJSON != `["rtev-source-parent"]` {
		t.Fatalf("expected canonical lineage on conflict finding, got %+v", conflict)
	}
	ordering := requireReplayFinding(t, report, "runtime_event_parent_ref_out_of_order")
	if ordering.SourceEventID != "rtev-source-close" || ordering.SourceParentRefsJSON != `["rtev-source-parent"]` {
		t.Fatalf("expected causal-order finding to stay source-addressable, got %+v", ordering)
	}
}

func TestRuntimeReplayTopologicalOrderPrefersParentBeforeChildWhenParentsExist(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-topological-available",
		Title:       "Runtime Replay Topological Available",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-topo-child",
		nil,
		"ws-runtime-replay-topological-available",
		"operator_queue.resolved",
		"operator_queue",
		"queue-topo-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-topo-1",
		"prov-topo-1",
		`["rtev-topo-parent"]`,
		`{"queue_id":"queue-topo-1","queue_key":"queue:topo-1","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Topo queue","summary":"Child should apply after parent","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1","parent_refs_json":["rtev-topo-parent"]}`,
		"2026-03-24T10:00:00Z",
		2,
	); err != nil {
		t.Fatalf("insert topological child event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-topo-parent",
		nil,
		"ws-runtime-replay-topological-available",
		"operator_queue.opened",
		"operator_queue",
		"queue-topo-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-topo-1",
		"prov-topo-1",
		`[]`,
		`{"queue_id":"queue-topo-1","queue_key":"queue:topo-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Topo queue","summary":"Parent arrives later in ingest order","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-24T10:01:00Z",
		3,
	); err != nil {
		t.Fatalf("insert topological parent event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-topological-available",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.EventsOrder != "latest_first_ingest" {
		t.Fatalf("expected replay event list to be labeled latest_first_ingest, got %+v", report)
	}
	if report.AppliedOrder != "causal_parent_before_child" {
		t.Fatalf("expected replay apply order label, got %+v", report)
	}
	if len(report.AppliedEventIDs) != 2 || report.AppliedEventIDs[0] != "rtev-topo-parent" || report.AppliedEventIDs[1] != "rtev-topo-child" {
		t.Fatalf("expected replay to expose causal apply order, got %+v", report.AppliedEventIDs)
	}

	queue := requireReplayQueue(t, report, "queue:topo-1")
	if queue.Status != "RESOLVED" || queue.Resolution != "done" || queue.ResolvedBy != "operator-1" {
		t.Fatalf("expected topological replay to apply parent before child and keep resolved queue state, got %+v", queue)
	}
	ordering := requireReplayFinding(t, report, "runtime_event_parent_ref_out_of_order")
	if ordering.SourceEventID != "rtev-topo-child" || ordering.SourceParentRefsJSON != `["rtev-topo-parent"]` {
		t.Fatalf("expected topological replay to preserve ingest-order warning lineage, got %+v", ordering)
	}
	if !strings.Contains(ordering.Message, "ingest order") {
		t.Fatalf("expected ingest-order wording on finding, got %+v", ordering)
	}
	if report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 0 ||
		report.Evaluation.ProvenanceSummary.FindingsWithRootCauseID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithProvenanceGroupID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithParentRefs != 1 ||
		report.Evaluation.ProvenanceSummary.FullLineageFieldFindingCount != 1 {
		t.Fatalf("expected topological replay provenance summary to keep dedup-key lineage separate, got %+v", report.Evaluation.ProvenanceSummary)
	}
	if hasReplayFindingCode(report, "runtime_event_parent_ref_missing") {
		t.Fatalf("expected all-available parent refs to avoid missing-parent findings, got %+v", report.Evaluation.Findings)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected topological replay with ingest-order anomaly to stay warn, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayProvenanceSummaryRequiresParentRefsForFullLineage(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-runtime-replay-partial-lineage"

	setupReplayWorkspace(t, ctx, store, workspaceID, "agent-partial-lineage")

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-partial-lineage-open",
		"D-partial-lineage-1",
		workspaceID,
		"operator_queue.opened",
		"operator_queue",
		"queue-partial-lineage-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-partial-lineage-1",
		"prov-partial-lineage-1",
		`[]`,
		`{"queue_key":"queue:partial-lineage","queue_type":"FOLLOW_UP","status":"OPEN","title":"Partial lineage queue","summary":"Opened first","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"D-partial-lineage-1","root_cause_id":"root-partial-lineage-1","provenance_group_id":"prov-partial-lineage-1"}`,
		"2026-03-22T20:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert partial-lineage open runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-partial-lineage-close",
		"D-partial-lineage-1",
		workspaceID,
		"operator_queue.resolved",
		"operator_queue",
		"queue-partial-lineage-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-partial-lineage-1",
		"prov-partial-lineage-1",
		`[]`,
		`{"queue_key":"queue:partial-lineage","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Partial lineage queue","summary":"Resolved later","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1","dedup_key":"D-partial-lineage-1","root_cause_id":"root-partial-lineage-1","provenance_group_id":"prov-partial-lineage-1"}`,
		"2026-03-22T20:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert partial-lineage close runtime event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Evaluation.FindingSummary.DedupConflictCount != 1 || report.Evaluation.FindingSummary.TotalFindings != 1 {
		t.Fatalf("expected one source-addressable dedup conflict finding, got %+v", report.Evaluation.FindingSummary)
	}
	if report.Evaluation.ProvenanceSummary.TotalFindingsWithSourceEvent != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithSourceDedupKey != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithRootCauseID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithProvenanceGroupID != 1 ||
		report.Evaluation.ProvenanceSummary.FindingsWithParentRefs != 0 ||
		report.Evaluation.ProvenanceSummary.FullLineageFieldFindingCount != 0 {
		t.Fatalf("expected full-lineage summary to require non-empty parent refs, got %+v", report.Evaluation.ProvenanceSummary)
	}
	conflict := requireReplayFinding(t, report, "runtime_event_dedup_conflict")
	if conflict.SourceEventID != "rtev-partial-lineage-close" || conflict.SourceRootCauseID != "root-partial-lineage-1" || conflict.SourceProvenanceGroupID != "prov-partial-lineage-1" {
		t.Fatalf("expected conflict finding to keep source/root/provenance fields, got %+v", conflict)
	}
	if conflict.SourceParentRefsJSON != `[]` {
		t.Fatalf("expected conflict finding to preserve empty parent-ref lineage without counting it as full lineage, got %+v", conflict)
	}
}

func TestRuntimeReplayTopologicalOrderPreservesMissingParentFindings(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-topological-mixed",
		Title:       "Runtime Replay Topological Mixed",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-topo-mixed-child",
		nil,
		"ws-runtime-replay-topological-mixed",
		"operator_queue.resolved",
		"operator_queue",
		"queue-topo-mixed-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-topo-mixed-1",
		"prov-topo-mixed-1",
		`["rtev-topo-mixed-parent","rtev-topo-mixed-missing"]`,
		`{"queue_id":"queue-topo-mixed-1","queue_key":"queue:topo-mixed-1","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Topo mixed queue","summary":"Available parent should reorder; missing parent should still warn","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1","parent_refs_json":["rtev-topo-mixed-parent","rtev-topo-mixed-missing"]}`,
		"2026-03-24T11:00:00Z",
		2,
	); err != nil {
		t.Fatalf("insert mixed child event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-topo-mixed-parent",
		nil,
		"ws-runtime-replay-topological-mixed",
		"operator_queue.opened",
		"operator_queue",
		"queue-topo-mixed-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-topo-mixed-1",
		"prov-topo-mixed-1",
		`[]`,
		`{"queue_id":"queue-topo-mixed-1","queue_key":"queue:topo-mixed-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Topo mixed queue","summary":"Available parent arrives later in ingest order","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-24T11:01:00Z",
		3,
	); err != nil {
		t.Fatalf("insert mixed parent event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-topological-mixed",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	queue := requireReplayQueue(t, report, "queue:topo-mixed-1")
	if queue.Status != "RESOLVED" || queue.Resolution != "done" {
		t.Fatalf("expected topological replay to preserve resolved child state when available parents exist, got %+v", queue)
	}
	missing := requireReplayFinding(t, report, "runtime_event_parent_ref_missing")
	if missing.SourceEventID != "rtev-topo-mixed-child" {
		t.Fatalf("expected mixed topological replay to preserve source-addressable missing-parent lineage, got %+v", missing)
	}
	requireParentRefSetJSON(t, missing.SourceParentRefsJSON, "rtev-topo-mixed-parent", "rtev-topo-mixed-missing")
	ordering := requireReplayFinding(t, report, "runtime_event_parent_ref_out_of_order")
	if ordering.SourceEventID != "rtev-topo-mixed-child" {
		t.Fatalf("expected mixed topological replay to preserve ingest-order warning lineage, got %+v", ordering)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected mixed parent replay to warn for missing and ingest-order parent refs, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayFlagsSelfParentRefAsSourceAddressableFinding(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-self-parent",
		Title:       "Runtime Replay Self Parent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-self-parent",
		nil,
		"ws-runtime-replay-self-parent",
		"operator_queue.opened",
		"operator_queue",
		"queue-self-parent-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-self-parent-1",
		"prov-self-parent-1",
		`["rtev-self-parent"]`,
		`{"queue_id":"queue-self-parent-1","queue_key":"queue:self-parent-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Self parent queue","summary":"Self parent should stay source-addressable","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-25T09:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert self-parent runtime event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-self-parent",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	if report.EventsOrder != "latest_first_ingest" || report.AppliedOrder != "causal_parent_before_child" {
		t.Fatalf("expected replay order metadata, got %+v", report)
	}
	if len(report.AppliedEventIDs) != 1 || report.AppliedEventIDs[0] != "rtev-self-parent" {
		t.Fatalf("expected self-parent event to remain in applied set once, got %+v", report.AppliedEventIDs)
	}
	queue := requireReplayQueue(t, report, "queue:self-parent-1")
	if queue.Status != "OPEN" || queue.EventCount != 1 {
		t.Fatalf("expected self-parent event to keep deterministic queue state, got %+v", queue)
	}
	finding := requireReplayFinding(t, report, "runtime_event_self_parent_ref")
	if finding.SourceEventID != "rtev-self-parent" || finding.SourceEventType != "operator_queue.opened" || finding.SourceParentRefsJSON != `["rtev-self-parent"]` {
		t.Fatalf("expected self-parent finding to stay source-addressable, got %+v", finding)
	}
	if hasReplayFindingCode(report, "runtime_event_parent_ref_missing") || hasReplayFindingCode(report, "runtime_event_parent_ref_cycle") {
		t.Fatalf("expected self-parent scenario to avoid missing/cycle false positives, got %+v", report.Evaluation.Findings)
	}
	if report.Evaluation.FindingSummary.CycleCount != 1 ||
		report.Evaluation.FindingSummary.CycleSelfParentCount != 1 ||
		report.Evaluation.FindingSummary.CycleParentComponentCount != 0 {
		t.Fatalf("expected self-parent replay finding summary to isolate self-parent cycle subcount, got %+v", report.Evaluation.FindingSummary)
	}
	if report.Evaluation.Verdict != "fail" {
		t.Fatalf("expected self-parent replay to fail, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayFallsBackToIngestOrderForCycleAffectedParentRefs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-parent-cycle",
		Title:       "Runtime Replay Parent Cycle",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-cycle-a",
		nil,
		"ws-runtime-replay-parent-cycle",
		"operator_queue.opened",
		"operator_queue",
		"queue-cycle-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-cycle-1",
		"prov-cycle-1",
		`["rtev-cycle-b"]`,
		`{"queue_id":"queue-cycle-1","queue_key":"queue:cycle-1","queue_type":"FOLLOW_UP","status":"OPEN","title":"Cycle queue","summary":"Cycle root event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-25T10:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert cycle event a: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-cycle-b",
		nil,
		"ws-runtime-replay-parent-cycle",
		"operator_queue.resolved",
		"operator_queue",
		"queue-cycle-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-cycle-1",
		"prov-cycle-1",
		`["rtev-cycle-a"]`,
		`{"queue_id":"queue-cycle-1","queue_key":"queue:cycle-1","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Cycle queue","summary":"Cycle leaf event","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"operator-1"}`,
		"2026-03-25T10:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert cycle event b: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-parent-cycle",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	if report.EventsOrder != "latest_first_ingest" || report.AppliedOrder != "causal_parent_before_child_with_cycle_ingest_fallback" {
		t.Fatalf("expected replay order metadata, got %+v", report)
	}
	if len(report.AppliedEventIDs) != 2 || report.AppliedEventIDs[0] != "rtev-cycle-a" || report.AppliedEventIDs[1] != "rtev-cycle-b" {
		t.Fatalf("expected cycle fallback to preserve ingest-order apply within cycle component, got %+v", report.AppliedEventIDs)
	}
	queue := requireReplayQueue(t, report, "queue:cycle-1")
	if queue.Status != "RESOLVED" || queue.Resolution != "done" || queue.ResolvedBy != "operator-1" {
		t.Fatalf("expected cycle fallback to keep final ingest-applied queue state, got %+v", queue)
	}
	finding := requireReplayFinding(t, report, "runtime_event_parent_ref_cycle")
	if finding.SourceEventID != "rtev-cycle-a" || finding.SourceEventType != "operator_queue.opened" || finding.SourceParentRefsJSON != `["rtev-cycle-b"]` {
		t.Fatalf("expected cycle finding to stay source-addressable, got %+v", finding)
	}
	if hasReplayFindingCode(report, "runtime_event_parent_ref_missing") {
		t.Fatalf("expected cycle replay to avoid missing-parent false positives, got %+v", report.Evaluation.Findings)
	}
	if report.Evaluation.FindingSummary.CycleCount != 1 ||
		report.Evaluation.FindingSummary.CycleSelfParentCount != 0 ||
		report.Evaluation.FindingSummary.CycleParentComponentCount != 1 {
		t.Fatalf("expected cycle replay finding summary to isolate parent-component cycle subcount, got %+v", report.Evaluation.FindingSummary)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected cycle replay to warn, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayTreatsParentRefSetsAsEquivalentAcrossFieldAndPayloadLineage(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-parent-set",
		Title:       "Runtime Replay Parent Set",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, parent := range []struct {
		eventID   string
		entityID  string
		createdAt string
		ingestSeq int
	}{
		{eventID: "rtev-parent-a", entityID: "parent-a", createdAt: "2026-03-22T19:00:00Z", ingestSeq: 1},
		{eventID: "rtev-parent-b", entityID: "parent-b", createdAt: "2026-03-22T19:01:00Z", ingestSeq: 2},
	} {
		if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
			event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
			actor_type, actor_id, agent_id, session_id, task_id,
			root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			parent.eventID,
			nil,
			"ws-runtime-replay-parent-set",
			"legacy.signal",
			"legacy_event",
			parent.entityID,
			"system",
			"tests",
			nil,
			nil,
			nil,
			nil,
			nil,
			`[]`,
			fmt.Sprintf(`{"message":"%s"}`, parent.eventID),
			parent.createdAt,
			parent.ingestSeq,
		); err != nil {
			t.Fatalf("insert parent runtime event %s: %v", parent.eventID, err)
		}
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-parent-set-field",
		"D-parent-set-replay-1",
		"ws-runtime-replay-parent-set",
		"operator_queue.opened",
		"operator_queue",
		"queue-parent-set-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-parent-set-1",
		"prov-parent-set-1",
		`["rtev-parent-b","rtev-parent-a"]`,
		`{"queue_key":"queue:parent-set","queue_type":"FOLLOW_UP","status":"OPEN","title":"Parent set queue","summary":"First version","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
		"2026-03-22T19:02:00Z",
		3,
	); err != nil {
		t.Fatalf("insert field-lineage runtime event: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-parent-set-payload",
		"D-parent-set-replay-1",
		"ws-runtime-replay-parent-set",
		"operator_queue.opened",
		"operator_queue",
		"queue-parent-set-1",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-parent-set-1",
		"prov-parent-set-1",
		`[]`,
		`{"queue_key":"queue:parent-set","queue_type":"FOLLOW_UP","status":"OPEN","title":"Parent set queue","summary":"First version","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"parent_refs_json":["rtev-parent-a","rtev-parent-b","rtev-parent-a"]}`,
		"2026-03-22T19:03:00Z",
		4,
	); err != nil {
		t.Fatalf("insert payload-lineage runtime event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-parent-set",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Metrics.TotalEvents != 4 || report.Metrics.AppliedEvents != 3 || report.Metrics.SuppressedDuplicateEvents != 1 || report.Metrics.ConflictingDuplicateKeys != 0 {
		t.Fatalf("expected equivalent parent-ref sets to suppress duplicate without conflict, got %+v", report.Metrics)
	}
	if len(report.Queues) != 1 || report.Queues[0].QueueID != "queue-parent-set-1" || report.Queues[0].EventCount != 1 {
		t.Fatalf("expected equivalent parent-ref-set duplicate to stay out of reducer state, got %+v", report.Queues)
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected equivalent parent-ref-set replay to stay clean, got %+v", report.Evaluation)
	}
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == "runtime_event_dedup_conflict" {
			t.Fatalf("expected parent-ref-set equivalent replay to avoid dedup conflict, got %+v", report.Evaluation.Findings)
		}
	}
}

func TestRuntimeReplayLegacyEventsKeepStableOrderingWithoutDedupKey(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-replay-legacy-order",
		Title:       "Replay Legacy Order",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for _, rec := range []sqlite.RuntimeEventInput{
		{
			EventID:     "rtev-legacy-b",
			WorkspaceID: "ws-replay-legacy-order",
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-1",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"legacy b"}`,
			CreatedAt:   "2026-03-22T18:00:00Z",
		},
		{
			EventID:     "rtev-legacy-a",
			WorkspaceID: "ws-replay-legacy-order",
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-1",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"legacy a"}`,
			CreatedAt:   "2026-03-22T18:00:00Z",
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, rec); err != nil {
			t.Fatalf("record legacy runtime event %s: %v", rec.EventID, err)
		}
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-replay-legacy-order",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if len(report.Events) != 2 || report.Events[0].EventID != "rtev-legacy-a" || report.Events[1].EventID != "rtev-legacy-b" {
		t.Fatalf("expected legacy events to follow ingest order without dedup_key, got %+v", report.Events)
	}
	if report.Events[0].DedupKey != "" || report.Events[1].DedupKey != "" {
		t.Fatalf("expected legacy events to remain dedup_key-free, got %+v", report.Events)
	}
}

func TestRuntimeReplayFlagsMissingParentRefsAsSourceAddressableFinding(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-replay-missing-parent",
		Title:       "Replay Missing Parent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-missing-parent-child",
		nil,
		"ws-replay-missing-parent",
		"legacy.signal",
		"legacy_event",
		"legacy-child",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-missing-parent",
		"prov-missing-parent",
		`["rtev-missing-parent"]`,
		`{"message":"missing parent edge"}`,
		"2026-03-22T21:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert runtime event with missing parent: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-replay-missing-parent",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected missing parent replay to warn, got %+v", report.Evaluation)
	}
	finding := requireReplayFinding(t, report, "runtime_event_parent_ref_missing")
	if finding.SourceEventID != "rtev-missing-parent-child" || finding.SourceParentRefsJSON != `["rtev-missing-parent"]` {
		t.Fatalf("expected missing parent finding to stay source-addressable, got %+v", finding)
	}
}

func TestRuntimeReplayPrefersFirstClassParentRefsOrderOverPayloadLineageOrder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-parent-order-lineage",
		Title:       "Runtime Parent Order Lineage",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-parent-order-a",
		nil,
		"ws-runtime-parent-order-lineage",
		"legacy.signal",
		"legacy_event",
		"legacy-parent-a",
		"system",
		"tests",
		nil,
		nil,
		nil,
		nil,
		nil,
		`[]`,
		`{"message":"existing parent a"}`,
		"2026-03-23T11:00:00Z",
		1,
	); err != nil {
		t.Fatalf("insert existing parent a: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-parent-order-child",
		"D-parent-order-1",
		"ws-runtime-parent-order-lineage",
		"legacy.signal",
		"legacy_event",
		"legacy-child",
		"system",
		"tests",
		nil,
		nil,
		nil,
		"root-parent-order-1",
		"prov-parent-order-1",
		`["rtev-parent-order-a","rtev-parent-order-missing"]`,
		`{"message":"payload lineage has different parent order","dedup_key":"D-parent-order-1","root_cause_id":"root-parent-order-1","provenance_group_id":"prov-parent-order-1","parent_refs_json":["rtev-parent-order-missing","rtev-parent-order-a"]}`,
		"2026-03-23T11:01:00Z",
		2,
	); err != nil {
		t.Fatalf("insert lineage-order child: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-parent-order-lineage",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected replay warning for missing parent, got %+v", report.Evaluation)
	}
	finding := requireReplayFinding(t, report, "runtime_event_parent_ref_missing")
	if finding.SourceEventID != "rtev-parent-order-child" || finding.SourceParentRefsJSON != `["rtev-parent-order-a","rtev-parent-order-missing"]` {
		t.Fatalf("expected replay lineage to prefer first-class parent_refs order, got %+v", finding)
	}
}

func setupReplayWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}

func recordReplaySessionEvent(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.AgentSessionCoordinationInput) sqlite.AgentSessionStateRecord {
	t.Helper()
	state, err := store.RecordAgentSessionCoordination(ctx, input)
	if err != nil {
		t.Fatalf("record session coordination event %s: %v", input.EventType, err)
	}
	payloadJSON, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal session replay payload: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev:" + strings.TrimSpace(state.SessionID) + ":" + strings.TrimSpace(state.UpdateType) + ":" + strings.TrimSpace(state.UpdatedAt),
		WorkspaceID: state.WorkspaceID,
		EventType:   state.UpdateType,
		EntityType:  "agent_session",
		EntityID:    state.SessionID,
		ActorType:   "agent",
		ActorID:     state.AgentID,
		AgentID:     state.AgentID,
		SessionID:   state.SessionID,
		TaskID:      state.TaskID,
		PayloadJSON: string(payloadJSON),
		CreatedAt:   state.UpdatedAt,
	}); err != nil {
		t.Fatalf("record runtime session event: %v", err)
	}
	return state
}

func recordCanonicalRuntimeEventForTest(ctx context.Context, store *sqlite.Store, input sqlite.RuntimeEventInput) (sqlite.RuntimeEventRecord, error) {
	dedupKey := strings.TrimSpace(input.DedupKey)
	if dedupKey == "" {
		return store.RecordRuntimeEvent(ctx, input)
	}

	existing, err := findRuntimeEventByDedupKey(ctx, store, strings.TrimSpace(input.WorkspaceID), dedupKey)
	if err != nil {
		return sqlite.RuntimeEventRecord{}, err
	}
	if existing.EventID != "" {
		if runtimeEventEquivalentForTest(existing, input) {
			return existing, nil
		}
		return sqlite.RuntimeEventRecord{}, fmt.Errorf("dedup_key conflict: %s already maps to a different runtime event", dedupKey)
	}

	return store.RecordRuntimeEvent(ctx, input)
}

func findRuntimeEventByDedupKey(ctx context.Context, store *sqlite.Store, workspaceID, dedupKey string) (sqlite.RuntimeEventRecord, error) {
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       500,
	})
	if err != nil {
		return sqlite.RuntimeEventRecord{}, err
	}
	for _, event := range events {
		if strings.TrimSpace(event.DedupKey) == dedupKey {
			return event, nil
		}
	}
	return sqlite.RuntimeEventRecord{}, nil
}

func runtimeEventEquivalentForTest(existing sqlite.RuntimeEventRecord, candidate sqlite.RuntimeEventInput) bool {
	return strings.TrimSpace(existing.WorkspaceID) == strings.TrimSpace(candidate.WorkspaceID) &&
		strings.TrimSpace(existing.DedupKey) == strings.TrimSpace(candidate.DedupKey) &&
		strings.TrimSpace(existing.EventType) == strings.TrimSpace(candidate.EventType) &&
		strings.TrimSpace(existing.EntityType) == strings.TrimSpace(candidate.EntityType) &&
		strings.TrimSpace(existing.EntityID) == strings.TrimSpace(candidate.EntityID) &&
		strings.TrimSpace(existing.ActorType) == strings.TrimSpace(candidate.ActorType) &&
		strings.TrimSpace(existing.ActorID) == strings.TrimSpace(candidate.ActorID) &&
		strings.TrimSpace(existing.AgentID) == strings.TrimSpace(candidate.AgentID) &&
		strings.TrimSpace(existing.SessionID) == strings.TrimSpace(candidate.SessionID) &&
		strings.TrimSpace(existing.TaskID) == strings.TrimSpace(candidate.TaskID) &&
		strings.TrimSpace(existing.RootCauseID) == strings.TrimSpace(candidate.RootCauseID) &&
		strings.TrimSpace(existing.ProvenanceGroupID) == strings.TrimSpace(candidate.ProvenanceGroupID) &&
		canonicalRuntimeEventPayloadJSONForTest(existing.PayloadJSON) == canonicalRuntimeEventPayloadJSONForTest(candidate.PayloadJSON) &&
		canonicalRuntimeEventParentRefsJSONForTest(existing.ParentRefsJSON) == canonicalRuntimeEventParentRefsJSONForTest(candidate.ParentRefsJSON)
}

func canonicalRuntimeEventPayloadJSONForTest(raw string) string {
	return canonicalRuntimeEventJSONForTest(raw, "{}")
}

func canonicalRuntimeEventParentRefsJSONForTest(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "[]"
	}
	var refs []string
	if err := json.Unmarshal([]byte(trimmed), &refs); err != nil {
		return canonicalRuntimeEventJSONForTest(raw, "[]")
	}
	canonical := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if _, exists := seen[ref]; exists {
			continue
		}
		seen[ref] = struct{}{}
		canonical = append(canonical, ref)
	}
	sort.Strings(canonical)
	normalized, err := json.Marshal(canonical)
	if err != nil {
		return canonicalRuntimeEventJSONForTest(raw, "[]")
	}
	return string(normalized)
}

func canonicalRuntimeEventJSONForTest(raw, empty string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return empty
	}
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return trimmed
	}
	normalized, err := json.Marshal(decoded)
	if err != nil {
		return trimmed
	}
	return string(normalized)
}

func takeOverReplaySession(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.AgentSessionTakeoverInput) sqlite.AgentSessionTakeoverRecord {
	t.Helper()
	record, err := store.TakeOverAgentSession(ctx, input)
	if err != nil {
		t.Fatalf("take over session: %v", err)
	}
	payloadJSON, err := json.Marshal(map[string]any{
		"source_state":    record.SourceState,
		"successor_state": record.SuccessorState,
	})
	if err != nil {
		t.Fatalf("marshal takeover payload: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev:takeover:" + strings.TrimSpace(record.SourceState.SessionID) + ":" + strings.TrimSpace(record.SuccessorState.UpdatedAt),
		WorkspaceID: record.SourceState.WorkspaceID,
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    record.SourceState.SessionID,
		ActorType:   "agent",
		ActorID:     record.SuccessorState.AgentID,
		AgentID:     record.SuccessorState.AgentID,
		SessionID:   record.SourceState.SessionID,
		TaskID:      firstNonBlank(record.SourceState.TaskID, record.SuccessorState.TaskID),
		PayloadJSON: string(payloadJSON),
		CreatedAt:   record.SuccessorState.UpdatedAt,
	}); err != nil {
		t.Fatalf("record takeover runtime event: %v", err)
	}
	return record
}

func syncReplayExecutionFromSessionState(t *testing.T, ctx context.Context, store *sqlite.Store, state sqlite.AgentSessionStateRecord) {
	t.Helper()
	runID := "session:" + strings.TrimSpace(state.SessionID)
	if strings.TrimSpace(state.SessionID) == "" {
		t.Fatalf("sync execution requires session id")
	}
	runStatus := "ACTIVE"
	switch strings.TrimSpace(state.Status) {
	case model.SessionStatusBlocked, model.SessionStatusWaitingDecision, model.SessionStatusHandoffPending:
		runStatus = "BLOCKED"
	case model.SessionStatusEnded:
		runStatus = "COMPLETED"
	}
	outcome := ""
	if runStatus == "COMPLETED" {
		outcome = strings.TrimSpace(state.Summary)
	}
	run, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: state.WorkspaceID,
		TaskID:      state.TaskID,
		SessionID:   state.SessionID,
		AgentID:     state.AgentID,
		Title:       "Session execution " + state.SessionID,
		Summary:     strings.TrimSpace(state.Summary),
		Status:      runStatus,
		Outcome:     outcome,
	})
	if err != nil {
		t.Fatalf("upsert replay execution run: %v", err)
	}

	stepPhase := "EXECUTE"
	switch strings.TrimSpace(state.UpdateType) {
	case model.SessionEventStart:
		stepPhase = "PLAN"
	case model.SessionEventEnd:
		stepPhase = "VERIFY"
	}
	stepStatus := "ACTIVE"
	switch runStatus {
	case "COMPLETED":
		stepStatus = "COMPLETED"
	case "BLOCKED":
		stepStatus = "BLOCKED"
	}
	if _, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		RunID:       run.RunID,
		WorkspaceID: run.WorkspaceID,
		Phase:       stepPhase,
		Title:       strings.TrimSpace(state.UpdateType),
		Summary:     strings.TrimSpace(state.Summary),
		Status:      stepStatus,
		Evidence:    []string{"session:" + state.SessionID, "event:" + state.UpdateType},
	}); err != nil {
		t.Fatalf("record replay execution step: %v", err)
	}
}

func TestRuntimeReplayPreservesRecoveredAlternativeBranchAndActiveDissent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-replay-branch-recovery"
		agentID     = "agent-replay-branch-recovery"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Branch Recovery",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replay Branch Recovery Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	dissent, _, err := store.RecordKnowledgeClaimWithEvent(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-dissent-replay-marker",
		ClaimType:   "dissent_marker",
		Status:      "active",
		Subject:     "Rollback disagreement exists",
		Body:        "Keep the dissent marker visible while a recoverable branch cycles through archive and restore.",
		Summary:     "Replay keeps dissent first-class.",
		SourceKind:  "manual",
		SourceID:    "review",
		AgentID:     agentID,
		Confidence:  0.72,
	})
	if err != nil {
		t.Fatalf("record dissent marker: %v", err)
	}

	memory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "alternative_branch",
		Title:       "Delayed rollout branch",
		Body:        "Preserve a slower rollout branch as recoverable contrastive memory.",
		Summary:     "Recoverable delayed rollout branch.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
		AgentID:     agentID,
		Confidence:  0.81,
	})
	if err != nil {
		t.Fatalf("record alternative branch memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    memory.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "manual_archive_for_replay",
	}); err != nil {
		t.Fatalf("archive alternative branch memory: %v", err)
	}
	if _, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID:    workspaceID,
		MemoryID:       memory.MemoryID,
		RestoredBy:     "developer",
		RecoveryReason: "manual_reassessment",
	}); err != nil {
		t.Fatalf("restore alternative branch memory: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	expectReplayVerdict(t, report, "pass")

	recoveredMemory := requireReplayWorkspaceMemory(t, report, memory.MemoryID)
	if recoveredMemory.MemoryType != "ALTERNATIVE_BRANCH" || recoveredMemory.LastEventType != "workspace_memory.restored" {
		t.Fatalf("expected recovered alternative branch memory, got %+v", recoveredMemory)
	}
	if recoveredMemory.ArchivedAt != nil || recoveredMemory.ArchivedBy != "" || recoveredMemory.ArchivedReason != "" {
		t.Fatalf("expected replayed alternative branch to be active after restore, got %+v", recoveredMemory)
	}
	if recoveredMemory.RecoveryReason != "manual_reassessment" || recoveredMemory.EventCount != 3 {
		t.Fatalf("expected replayed alternative branch recovery lineage, got %+v", recoveredMemory)
	}

	branchClaim := requireReplayClaim(t, report, "claim:memory:"+memory.MemoryID)
	if branchClaim.ClaimType != "ALTERNATIVE_BRANCH" || branchClaim.Status != "ACTIVE" || branchClaim.ArchivedAt != nil {
		t.Fatalf("expected replayed promoted claim to stay active alternative branch, got %+v", branchClaim)
	}

	dissentClaim := requireReplayClaim(t, report, dissent.ClaimID)
	if dissentClaim.ClaimType != "DISSENT_MARKER" || dissentClaim.Status != "ACTIVE" || dissentClaim.ArchivedAt != nil {
		t.Fatalf("expected replayed dissent marker to remain first-class and active, got %+v", dissentClaim)
	}
}

func TestRuntimeReplayTracksAutoReactivatedArchivedAlternativeBranch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-replay-branch-reactivation"
		agentID     = "agent-replay-branch-reactivation"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Replay Branch Reactivation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Replay Branch Reactivation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	original, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "alternative_branch",
		Title:       "Reactivatable branch",
		Body:        "Preserve a recoverable branch and later reactivate it through duplicate recording.",
		Summary:     "Reactivatable branch.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
		AgentID:     agentID,
		Confidence:  0.78,
	})
	if err != nil {
		t.Fatalf("record original alternative branch: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    original.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive original branch: %v", err)
	}

	reactivated, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "alternative_branch",
		Title:       "Reactivatable branch",
		Body:        "Preserve a recoverable branch and later reactivate it through duplicate recording.",
		Summary:     "Reactivatable branch.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
		AgentID:     agentID,
		Confidence:  0.79,
	})
	if err != nil {
		t.Fatalf("reactivate archived branch through duplicate recording: %v", err)
	}
	if reactivated.MemoryID != original.MemoryID || reactivated.RecoveryReason != "rmp_gc_reactivated" {
		t.Fatalf("expected duplicate recording to reactivate archived branch in place, original=%+v reactivated=%+v", original, reactivated)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "workspace_memory",
		EntityID:    original.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace memory runtime events: %v", err)
	}
	if len(events) < 4 || events[0].EventType != "workspace_memory.recorded" || events[1].EventType != "workspace_memory.restored" {
		t.Fatalf("expected reactivation lineage recorded -> restored near the head, got %+v", events)
	}
	requireParentRefSetJSON(t, events[0].ParentRefsJSON, events[1].EventID)

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	expectReplayVerdict(t, report, "pass")

	replayed := requireReplayWorkspaceMemory(t, report, original.MemoryID)
	if replayed.MemoryType != "ALTERNATIVE_BRANCH" || replayed.LastEventType != "workspace_memory.recorded" {
		t.Fatalf("expected replayed reactivated branch to end on recorded alternative branch state, got %+v", replayed)
	}
	if replayed.ArchivedAt != nil || replayed.RecoveryReason != "rmp_gc_reactivated" || replayed.EventCount != 4 {
		t.Fatalf("expected replayed reactivation lineage to stay active and recoverable, got %+v", replayed)
	}

	claim := requireReplayClaim(t, report, "claim:memory:"+original.MemoryID)
	if claim.ClaimType != "ALTERNATIVE_BRANCH" || claim.Status != "ACTIVE" || claim.ArchivedAt != nil {
		t.Fatalf("expected replayed promoted branch claim to remain active, got %+v", claim)
	}
}

func expectReplayVerdict(t *testing.T, report sqlite.RuntimeReplayReport, verdict string) {
	t.Helper()
	if report.Evaluation.Verdict != verdict {
		t.Fatalf("expected replay verdict %s, got %+v", verdict, report.Evaluation)
	}
}

func expectFindingCodes(t *testing.T, report sqlite.RuntimeReplayReport, codes []string) {
	t.Helper()
	actual := map[string]bool{}
	for _, finding := range report.Evaluation.Findings {
		actual[finding.Code] = true
	}
	for _, code := range codes {
		if !actual[code] {
			t.Fatalf("expected replay finding %s, got %+v", code, report.Evaluation.Findings)
		}
	}
	if len(codes) == 0 && len(report.Evaluation.Findings) != 0 {
		t.Fatalf("expected no replay findings, got %+v", report.Evaluation.Findings)
	}
}

func requireReplayFinding(t *testing.T, report sqlite.RuntimeReplayReport, code string) sqlite.RuntimeReplayFinding {
	t.Helper()
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == code {
			return finding
		}
	}
	t.Fatalf("expected replay finding %s, got %+v", code, report.Evaluation.Findings)
	return sqlite.RuntimeReplayFinding{}
}

func requireReplaySession(t *testing.T, report sqlite.RuntimeReplayReport, sessionID string) sqlite.RuntimeReplaySession {
	t.Helper()
	for _, session := range report.Sessions {
		if session.SessionID == sessionID {
			return session
		}
	}
	t.Fatalf("session %s not found in replay report %+v", sessionID, report.Sessions)
	return sqlite.RuntimeReplaySession{}
}

func requireReplayExecutionRun(t *testing.T, report sqlite.RuntimeReplayReport, runID string) sqlite.RuntimeReplayExecutionRun {
	t.Helper()
	for _, run := range report.ExecutionRuns {
		if run.RunID == runID {
			return run
		}
	}
	t.Fatalf("execution run %s not found in replay report %+v", runID, report.ExecutionRuns)
	return sqlite.RuntimeReplayExecutionRun{}
}

func requireReplayClaim(t *testing.T, report sqlite.RuntimeReplayReport, claimID string) sqlite.RuntimeReplayClaim {
	t.Helper()
	for _, claim := range report.Claims {
		if claim.ClaimID == claimID {
			return claim
		}
	}
	t.Fatalf("claim %s not found in replay report %+v", claimID, report.Claims)
	return sqlite.RuntimeReplayClaim{}
}

func requireReplayWorkspaceMemory(t *testing.T, report sqlite.RuntimeReplayReport, memoryID string) sqlite.RuntimeReplayWorkspaceMemory {
	t.Helper()
	for _, memory := range report.WorkspaceMemory {
		if memory.MemoryID == memoryID {
			return memory
		}
	}
	t.Fatalf("workspace memory %s not found in replay report %+v", memoryID, report.WorkspaceMemory)
	return sqlite.RuntimeReplayWorkspaceMemory{}
}

func requireReplayQueue(t *testing.T, report sqlite.RuntimeReplayReport, queueKey string) sqlite.RuntimeReplayQueue {
	t.Helper()
	for _, queue := range report.Queues {
		if queue.QueueKey == queueKey {
			return queue
		}
	}
	t.Fatalf("queue %s not found in replay report %+v", queueKey, report.Queues)
	return sqlite.RuntimeReplayQueue{}
}

func hasReplaySession(report sqlite.RuntimeReplayReport, sessionID string) bool {
	for _, session := range report.Sessions {
		if session.SessionID == sessionID {
			return true
		}
	}
	return false
}

func hasReplayFindingCode(report sqlite.RuntimeReplayReport, code string) bool {
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}

func hasRuntimeReplayRetentionReason(risk sqlite.RuntimeReplayRetentionRisk, want string) bool {
	for _, reason := range risk.Reasons {
		if reason == want {
			return true
		}
	}
	return false
}

func requireParentRefSetJSON(t *testing.T, raw string, want ...string) {
	t.Helper()
	var got []string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("decode parent ref set %q: %v", raw, err)
	}
	sort.Strings(got)
	wantCopy := append([]string(nil), want...)
	sort.Strings(wantCopy)
	if len(got) != len(wantCopy) {
		t.Fatalf("expected parent ref set %v, got %v", wantCopy, got)
	}
	for i := range wantCopy {
		if got[i] != wantCopy[i] {
			t.Fatalf("expected parent ref set %v, got %v", wantCopy, got)
		}
	}
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
