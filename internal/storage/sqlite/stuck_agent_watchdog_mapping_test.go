package sqlite

import "testing"

func TestMapStuckAgentWatchdogVerdictToNextAction(t *testing.T) {
	t.Parallel()

	recoverableManifest := &StuckAgentRecoveryManifest{
		Checkpoint: &StuckAgentRecoveryCheckpoint{
			RunID:     "run-1",
			StepID:    "step-1",
			StepPhase: "execute",
		},
		OpenOps: []StuckAgentRecoveryOpenOperation{
			{
				RunID:   "run-1",
				Status:  "running",
				AgentID: "agent-1",
			},
		},
		BudgetRemainder: &StuckAgentRecoveryBudgetRemainder{
			RemainingTokens: 8,
		},
	}
	exhaustedManifest := &StuckAgentRecoveryManifest{
		BudgetRemainder: &StuckAgentRecoveryBudgetRemainder{
			RemainingTokens: 0,
		},
	}
	priorOwnerOnlyManifest := &StuckAgentRecoveryManifest{
		PriorOwner: StuckAgentRecoveryPriorOwner{
			WorkspaceID: "ws-1",
			SessionID:   "sess-1",
			AgentID:     "agent-1",
			TaskID:      "task-1",
		},
	}

	tests := []struct {
		name                 string
		verdict              StuckAgentWatchdogVerdict
		reason               string
		manifest             *StuckAgentRecoveryManifest
		wantAction           string
		wantRequiresOperator bool
	}{
		{
			name:                 "no progress resumes from checkpoint",
			verdict:              StuckAgentWatchdogVerdictNoProgress,
			reason:               "stalled after durable checkpoint",
			manifest:             recoverableManifest,
			wantAction:           string(StuckAgentRecoveryActionResumeFromCheckpoint),
			wantRequiresOperator: false,
		},
		{
			name:                 "timeout compacts when budget is exhausted",
			verdict:              StuckAgentWatchdogVerdictTimeout,
			reason:               "deadline exceeded and token budget spent",
			manifest:             exhaustedManifest,
			wantAction:           string(StuckAgentRecoveryActionCompactSession),
			wantRequiresOperator: false,
		},
		{
			name:                 "timeout retries from durable recovery state",
			verdict:              StuckAgentWatchdogVerdictTimeout,
			reason:               "timed out while waiting for a durable step",
			manifest:             recoverableManifest,
			wantAction:           string(StuckAgentRecoveryActionRetryOrTimeoutRecovery),
			wantRequiresOperator: false,
		},
		{
			name:                 "invalid output repairs structured output",
			verdict:              StuckAgentWatchdogVerdictInvalidOutput,
			reason:               "invalid structured output",
			manifest:             recoverableManifest,
			wantAction:           string(StuckAgentRecoveryActionRepairStructuredOutput),
			wantRequiresOperator: false,
		},
		{
			name:                 "heartbeat death resumes from checkpoint",
			verdict:              StuckAgentWatchdogVerdictHeartbeatDeath,
			reason:               "missing_agent heartbeat dropped",
			manifest:             recoverableManifest,
			wantAction:           string(StuckAgentRecoveryActionResumeFromCheckpoint),
			wantRequiresOperator: false,
		},
		{
			name:                 "heartbeat death claims successor when only prior owner is durable",
			verdict:              StuckAgentWatchdogVerdictHeartbeatDeath,
			reason:               "heartbeat died after the owner vanished",
			manifest:             priorOwnerOnlyManifest,
			wantAction:           string(StuckAgentRecoveryActionClaimSuccessorSession),
			wantRequiresOperator: false,
		},
		{
			name:                 "no progress without durable state needs operator",
			verdict:              StuckAgentWatchdogVerdictNoProgress,
			reason:               "stalled with no durable recovery path",
			manifest:             nil,
			wantAction:           string(StuckAgentRecoveryActionNeedsOperator),
			wantRequiresOperator: true,
		},
		{
			name:                 "invalid output without durable state needs operator",
			verdict:              StuckAgentWatchdogVerdictInvalidOutput,
			reason:               "structured output could not be repaired",
			manifest:             nil,
			wantAction:           string(StuckAgentRecoveryActionNeedsOperator),
			wantRequiresOperator: true,
		},
		{
			name:                 "unknown verdict without durable state needs operator",
			verdict:              StuckAgentWatchdogVerdict("unknown"),
			reason:               "unexpected watchdog verdict",
			manifest:             nil,
			wantAction:           string(StuckAgentRecoveryActionNeedsOperator),
			wantRequiresOperator: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			decision := MapStuckAgentWatchdogVerdictToNextAction(StuckAgentWatchdogMappingInput{
				Verdict:          tc.verdict,
				Reason:           tc.reason,
				RecoveryManifest: tc.manifest,
			})
			if decision.Action != tc.wantAction {
				t.Fatalf("unexpected action: got %q want %q decision=%+v", decision.Action, tc.wantAction, decision)
			}
			if decision.RequiresOperator != tc.wantRequiresOperator {
				t.Fatalf("unexpected operator requirement: got %v want %v decision=%+v", decision.RequiresOperator, tc.wantRequiresOperator, decision)
			}
			if decision.Action == "" {
				t.Fatalf("expected explicit action, got empty decision=%+v", decision)
			}
			if tc.wantRequiresOperator && decision.Action != string(StuckAgentRecoveryActionNeedsOperator) {
				t.Fatalf("expected explicit needs_operator fallback, got %+v", decision)
			}
		})
	}
}
