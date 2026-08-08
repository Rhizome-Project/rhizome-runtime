package sqlite

import (
	"context"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type StuckAgentRecoveryManifest struct {
	PriorOwner      StuckAgentRecoveryPriorOwner       `json:"prior_owner"`
	Checkpoint      *StuckAgentRecoveryCheckpoint      `json:"checkpoint,omitempty"`
	OpenOps         []StuckAgentRecoveryOpenOperation  `json:"open_ops,omitempty"`
	BudgetRemainder *StuckAgentRecoveryBudgetRemainder `json:"budget_remainder,omitempty"`
	NextAction      StuckAgentRecoveryNextAction       `json:"next_action"`
	DerivedAt       string                             `json:"derived_at"`
}

type StuckAgentRecoveryPriorOwner struct {
	WorkspaceID        string                        `json:"workspace_id,omitempty"`
	SessionID          string                        `json:"session_id,omitempty"`
	AgentID            string                        `json:"agent_id,omitempty"`
	TaskID             string                        `json:"task_id,omitempty"`
	Status             string                        `json:"status,omitempty"`
	UpdateType         string                        `json:"update_type,omitempty"`
	OwnerScope         string                        `json:"owner_scope,omitempty"`
	DecisionNeededFrom string                        `json:"decision_needed_from,omitempty"`
	DecisionType       string                        `json:"decision_type,omitempty"`
	HandoffTo          string                        `json:"handoff_to,omitempty"`
	BlockedOn          []model.AgentUpdateBlockedRef `json:"blocked_on,omitempty"`
	StartedAt          string                        `json:"started_at,omitempty"`
	LastObservedAt     string                        `json:"last_observed_at,omitempty"`
	StuckReason        string                        `json:"stuck_reason,omitempty"`
}

type StuckAgentRecoveryCheckpoint struct {
	RunID      string   `json:"run_id,omitempty"`
	RunStatus  string   `json:"run_status,omitempty"`
	RunOutcome string   `json:"run_outcome,omitempty"`
	StepID     string   `json:"step_id,omitempty"`
	StepPhase  string   `json:"step_phase,omitempty"`
	StepTitle  string   `json:"step_title,omitempty"`
	StepStatus string   `json:"step_status,omitempty"`
	UpdatedAt  string   `json:"updated_at,omitempty"`
	Summary    string   `json:"summary,omitempty"`
	Evidence   []string `json:"evidence,omitempty"`
}

type StuckAgentRecoveryOpenOperation struct {
	OperationID    string   `json:"operation_id,omitempty"`
	OperationName  string   `json:"operation_name,omitempty"`
	OperationKind  string   `json:"operation_kind,omitempty"`
	RunID          string   `json:"run_id"`
	TaskID         string   `json:"task_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	Status         string   `json:"status"`
	UpdatedAt      string   `json:"updated_at,omitempty"`
	LastStepID     string   `json:"last_step_id,omitempty"`
	LastStepPhase  string   `json:"last_step_phase,omitempty"`
	LastStepTitle  string   `json:"last_step_title,omitempty"`
	LastStepStatus string   `json:"last_step_status,omitempty"`
	LastStepAt     string   `json:"last_step_at,omitempty"`
	Evidence       []string `json:"evidence,omitempty"`
}

type StuckAgentRecoveryBudgetRemainder struct {
	SnapshotID         string `json:"snapshot_id,omitempty"`
	TokenBudget        int    `json:"token_budget"`
	MessageTokensAfter int    `json:"message_tokens_after"`
	RemainingTokens    int    `json:"remaining_tokens"`
	MessageCountAfter  int    `json:"message_count_after"`
	TotalInputTokens   int    `json:"total_input_tokens"`
	TotalOutputTokens  int    `json:"total_output_tokens"`
	CreatedAt          string `json:"created_at,omitempty"`
}

type StuckAgentRecoveryNextAction struct {
	Verdict          string `json:"verdict,omitempty"`
	Action           string `json:"action"`
	Reason           string `json:"reason,omitempty"`
	RequiresOperator bool   `json:"requires_operator"`
}

type StuckAgentWatchdogVerdict string

const (
	StuckAgentWatchdogVerdictNoProgress     StuckAgentWatchdogVerdict = "no_progress"
	StuckAgentWatchdogVerdictTimeout        StuckAgentWatchdogVerdict = "timeout"
	StuckAgentWatchdogVerdictInvalidOutput  StuckAgentWatchdogVerdict = "invalid_output"
	StuckAgentWatchdogVerdictHeartbeatDeath StuckAgentWatchdogVerdict = "heartbeat_death"
)

type StuckAgentRecoveryAction string

const (
	StuckAgentRecoveryActionResumeFromCheckpoint   StuckAgentRecoveryAction = "resume_from_checkpoint"
	StuckAgentRecoveryActionCompactSession         StuckAgentRecoveryAction = "compact_session"
	StuckAgentRecoveryActionClaimSuccessorSession  StuckAgentRecoveryAction = "claim_successor_session"
	StuckAgentRecoveryActionRepairStructuredOutput StuckAgentRecoveryAction = "repair_structured_output"
	StuckAgentRecoveryActionRetryOrTimeoutRecovery StuckAgentRecoveryAction = "retry_or_timeout_recovery"
	StuckAgentRecoveryActionNeedsOperator          StuckAgentRecoveryAction = "needs_operator"
)

type StuckAgentWatchdogMappingInput struct {
	Verdict          StuckAgentWatchdogVerdict
	Reason           string
	RecoveryManifest *StuckAgentRecoveryManifest
}

type stuckAgentRecoveryCandidate struct {
	WorkspaceID      string
	SessionID        string
	AgentID          string
	TaskID           string
	StartedAt        string
	LastSeenAt       string
	LatestActivityAt string
	MissingAgent     bool
	OfflineAgent     bool
	StaleActivity    bool
	StartupGrace     bool
	AffectedAt       time.Time
}

func (c *stuckAgentRecoveryCandidate) moreUrgent(other *stuckAgentRecoveryCandidate) bool {
	if c == nil {
		return false
	}
	if other == nil {
		return true
	}
	if c.AffectedAt.IsZero() && !other.AffectedAt.IsZero() {
		return false
	}
	if !c.AffectedAt.IsZero() && other.AffectedAt.IsZero() {
		return true
	}
	if !c.AffectedAt.IsZero() && !other.AffectedAt.IsZero() && !c.AffectedAt.Equal(other.AffectedAt) {
		return c.AffectedAt.Before(other.AffectedAt)
	}
	if c.StartedAt != other.StartedAt {
		return c.StartedAt < other.StartedAt
	}
	if c.SessionID != other.SessionID {
		return c.SessionID < other.SessionID
	}
	return c.AgentID < other.AgentID
}

func (s *Store) buildStuckAgentRecoveryManifest(ctx context.Context, referenceAt time.Time, candidate stuckAgentRecoveryCandidate) *StuckAgentRecoveryManifest {
	if s == nil {
		return nil
	}
	candidate.WorkspaceID = strings.TrimSpace(candidate.WorkspaceID)
	candidate.SessionID = strings.TrimSpace(candidate.SessionID)
	candidate.AgentID = strings.TrimSpace(candidate.AgentID)
	candidate.TaskID = strings.TrimSpace(candidate.TaskID)
	if candidate.WorkspaceID == "" || candidate.SessionID == "" {
		return nil
	}

	manifest := &StuckAgentRecoveryManifest{
		PriorOwner: StuckAgentRecoveryPriorOwner{
			WorkspaceID:    candidate.WorkspaceID,
			SessionID:      candidate.SessionID,
			AgentID:        candidate.AgentID,
			TaskID:         candidate.TaskID,
			StartedAt:      candidate.StartedAt,
			LastObservedAt: firstNonEmpty(candidate.LastSeenAt, candidate.LatestActivityAt, candidate.StartedAt),
		},
		DerivedAt: referenceAt.UTC().Format(time.RFC3339Nano),
	}

	if state, err := s.GetAgentSessionState(ctx, candidate.WorkspaceID, candidate.SessionID); err == nil {
		manifest.PriorOwner.AgentID = firstNonEmpty(manifest.PriorOwner.AgentID, state.AgentID)
		manifest.PriorOwner.TaskID = firstNonEmpty(manifest.PriorOwner.TaskID, state.TaskID)
		manifest.PriorOwner.Status = state.Status
		manifest.PriorOwner.UpdateType = state.UpdateType
		manifest.PriorOwner.OwnerScope = state.OwnerScope
		manifest.PriorOwner.DecisionNeededFrom = state.DecisionNeededFrom
		manifest.PriorOwner.DecisionType = state.DecisionType
		manifest.PriorOwner.HandoffTo = state.HandoffTo
		manifest.PriorOwner.BlockedOn = state.BlockedOn
		if observed := firstNonEmpty(state.UpdatedAt, state.StartedAt); observed != "" {
			manifest.PriorOwner.LastObservedAt = observed
		}
	}
	manifest.PriorOwner.StuckReason = stuckAgentRecoveryReason(candidate)

	if runs, err := s.listExecutionRunsForRecoverySession(ctx, candidate.WorkspaceID, candidate.SessionID); err == nil && len(runs) > 0 {
		manifest.Checkpoint = stuckAgentRecoveryCheckpointForRuns(ctx, s, candidate.WorkspaceID, runs)
		manifest.OpenOps = stuckAgentRecoveryOpenOperationsForRuns(ctx, s, candidate.WorkspaceID, runs)
	}

	if snapshots, err := s.ListSessionCompactionSnapshots(ctx, SessionCompactionSnapshotFilter{
		WorkspaceID: candidate.WorkspaceID,
		SessionID:   candidate.SessionID,
		AgentID:     candidate.AgentID,
		Limit:       1,
	}); err == nil && len(snapshots) > 0 {
		snapshot := snapshots[0]
		remaining := snapshot.TokenBudget - snapshot.MessageTokensAfter
		if remaining < 0 {
			remaining = 0
		}
		manifest.BudgetRemainder = &StuckAgentRecoveryBudgetRemainder{
			SnapshotID:         snapshot.SnapshotID,
			TokenBudget:        snapshot.TokenBudget,
			MessageTokensAfter: snapshot.MessageTokensAfter,
			RemainingTokens:    remaining,
			MessageCountAfter:  snapshot.MessageCountAfter,
			TotalInputTokens:   snapshot.TotalInputTokens,
			TotalOutputTokens:  snapshot.TotalOutputTokens,
			CreatedAt:          snapshot.CreatedAt,
		}
	}

	manifest.NextAction = stuckAgentRecoveryNextAction(candidate, manifest)
	return manifest
}

// ReadDurableStuckAgentRecoveryManifest rebuilds the recovery view strictly
// from sqlite rows. It is safe to call after a process restart because it does
// not consult any process-local supervisor or fanout state.
func (s *Store) ReadDurableStuckAgentRecoveryManifest(ctx context.Context, referenceAt time.Time, candidate stuckAgentRecoveryCandidate) *StuckAgentRecoveryManifest {
	return s.buildStuckAgentRecoveryManifest(ctx, referenceAt, candidate)
}

func stuckAgentRecoveryReason(candidate stuckAgentRecoveryCandidate) string {
	reasons := make([]string, 0, 3)
	if candidate.MissingAgent {
		reasons = append(reasons, "missing_agent")
	}
	if candidate.OfflineAgent {
		reasons = append(reasons, "offline_agent")
	}
	if candidate.StaleActivity {
		reasons = append(reasons, "stale_activity")
	}
	if len(reasons) == 0 {
		return "unknown"
	}
	return strings.Join(reasons, "+")
}

func (s *Store) listExecutionRunsForRecoverySession(ctx context.Context, workspaceID, sessionID string) ([]ExecutionRunRecord, error) {
	if s == nil {
		return nil, nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	sessionID = strings.TrimSpace(sessionID)
	if workspaceID == "" || sessionID == "" {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT run_id, workspace_id, COALESCE(task_id,''), COALESCE(session_id,''), COALESCE(agent_id,''),
	        title, summary, status, outcome, verification_json, created_at, updated_at, closed_at
	   FROM execution_runs
	  WHERE workspace_id = ? AND COALESCE(session_id,'') = ?
	  ORDER BY updated_at DESC, run_id DESC`, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ExecutionRunRecord{}
	for rows.Next() {
		record, err := scanExecutionRunRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func stuckAgentRecoveryCheckpointForRuns(ctx context.Context, store *Store, workspaceID string, runs []ExecutionRunRecord) *StuckAgentRecoveryCheckpoint {
	if store == nil || len(runs) == 0 {
		return nil
	}
	var fallback *StuckAgentRecoveryCheckpoint
	for _, run := range runs {
		checkpoint := stuckAgentRecoveryCheckpointForRun(ctx, store, workspaceID, run)
		if checkpoint == nil {
			continue
		}
		if fallback == nil {
			fallback = checkpoint
		}
		if strings.TrimSpace(checkpoint.StepID) != "" {
			return checkpoint
		}
	}
	return fallback
}

func stuckAgentRecoveryCheckpointForRun(ctx context.Context, store *Store, workspaceID string, run ExecutionRunRecord) *StuckAgentRecoveryCheckpoint {
	if store == nil {
		return nil
	}
	detail, err := store.GetExecutionRun(ctx, workspaceID, run.RunID)
	if err != nil {
		return &StuckAgentRecoveryCheckpoint{
			RunID:      run.RunID,
			RunStatus:  run.Status,
			RunOutcome: run.Outcome,
			Summary:    run.Summary,
		}
	}
	checkpoint := &StuckAgentRecoveryCheckpoint{
		RunID:      detail.Run.RunID,
		RunStatus:  detail.Run.Status,
		RunOutcome: detail.Run.Outcome,
		Summary:    detail.Run.Summary,
	}
	if len(detail.Steps) == 0 {
		return checkpoint
	}
	step := detail.Steps[0]
	for _, candidate := range detail.Steps[1:] {
		if recoveryStepMoreRecent(candidate, step) {
			step = candidate
		}
	}
	checkpoint.StepID = step.StepID
	checkpoint.StepPhase = step.Phase
	checkpoint.StepTitle = step.Title
	checkpoint.StepStatus = step.Status
	checkpoint.UpdatedAt = firstNonEmpty(step.UpdatedAt, step.CreatedAt)
	checkpoint.Evidence = append([]string(nil), step.Evidence...)
	return checkpoint
}

func stuckAgentRecoveryOpenOperationsForRuns(ctx context.Context, store *Store, workspaceID string, runs []ExecutionRunRecord) []StuckAgentRecoveryOpenOperation {
	if store == nil || len(runs) == 0 {
		return nil
	}
	operations := make([]StuckAgentRecoveryOpenOperation, 0, len(runs))
	for _, run := range runs {
		if ledgerOps := stuckAgentRecoveryOpenOperationsFromLedger(run); len(ledgerOps) > 0 {
			operations = append(operations, ledgerOps...)
			continue
		}
		if isExecutionRunTerminal(run.Status) {
			continue
		}
		op := StuckAgentRecoveryOpenOperation{
			RunID:     run.RunID,
			TaskID:    run.TaskID,
			SessionID: run.SessionID,
			AgentID:   run.AgentID,
			Status:    run.Status,
			UpdatedAt: run.UpdatedAt,
		}
		detail, err := store.GetExecutionRun(ctx, workspaceID, run.RunID)
		if err == nil && len(detail.Steps) > 0 {
			step := detail.Steps[0]
			for _, candidate := range detail.Steps[1:] {
				if recoveryStepMoreRecent(candidate, step) {
					step = candidate
				}
			}
			op.LastStepID = step.StepID
			op.LastStepPhase = step.Phase
			op.LastStepTitle = step.Title
			op.LastStepStatus = step.Status
			op.LastStepAt = firstNonEmpty(step.UpdatedAt, step.CreatedAt)
			op.Evidence = append([]string(nil), step.Evidence...)
		}
		operations = append(operations, op)
	}
	return operations
}

func stuckAgentRecoveryOpenOperationsFromLedger(run ExecutionRunRecord) []StuckAgentRecoveryOpenOperation {
	ledger, ok := run.VerificationJSON["operation_ledger"].(map[string]any)
	if !ok || len(ledger) == 0 {
		return nil
	}
	if recoveryMapBool(ledger, "terminal") || isExecutionRunTerminal(recoveryMapString(ledger, "status")) {
		return nil
	}
	operation := StuckAgentRecoveryOpenOperation{
		OperationID:   recoveryMapString(ledger, "operation_id"),
		OperationName: recoveryMapString(ledger, "operation_name"),
		OperationKind: recoveryMapString(ledger, "operation_kind"),
		RunID:         firstNonEmpty(run.RunID, recoveryNestedMapString(ledger, "binding", "run_id")),
		TaskID:        firstNonEmpty(run.TaskID, recoveryNestedMapString(ledger, "binding", "task_id")),
		SessionID:     firstNonEmpty(run.SessionID, recoveryNestedMapString(ledger, "binding", "session_id")),
		AgentID:       firstNonEmpty(run.AgentID, recoveryNestedMapString(ledger, "binding", "agent_id")),
		Status:        firstNonEmpty(recoveryMapString(ledger, "status"), run.Status),
		UpdatedAt:     firstNonEmpty(recoveryMapString(ledger, "updated_at"), run.UpdatedAt),
	}
	if operation.RunID == "" {
		operation.RunID = run.RunID
	}
	return []StuckAgentRecoveryOpenOperation{operation}
}

func stuckAgentRecoveryNextAction(candidate stuckAgentRecoveryCandidate, manifest *StuckAgentRecoveryManifest) StuckAgentRecoveryNextAction {
	if candidate.StartupGrace {
		return StuckAgentRecoveryNextAction{
			Action:           "wait_for_startup_grace",
			Reason:           "session is still within startup grace",
			RequiresOperator: false,
		}
	}
	verdict, reason := stuckAgentRecoveryVerdictForCandidate(candidate)
	return MapStuckAgentWatchdogVerdictToNextAction(StuckAgentWatchdogMappingInput{
		Verdict:          verdict,
		Reason:           reason,
		RecoveryManifest: manifest,
	})
}

func stuckAgentRecoveryVerdictForCandidate(candidate stuckAgentRecoveryCandidate) (StuckAgentWatchdogVerdict, string) {
	reason := stuckAgentRecoveryReason(candidate)
	switch {
	case candidate.MissingAgent || candidate.OfflineAgent:
		return StuckAgentWatchdogVerdictHeartbeatDeath, reason
	case candidate.StaleActivity:
		return StuckAgentWatchdogVerdictNoProgress, reason
	default:
		return StuckAgentWatchdogVerdictNoProgress, reason
	}
}

func MapStuckAgentWatchdogVerdictToNextAction(input StuckAgentWatchdogMappingInput) StuckAgentRecoveryNextAction {
	verdict := strings.TrimSpace(strings.ToLower(string(input.Verdict)))
	if verdict == "" {
		verdict = string(StuckAgentWatchdogVerdictNoProgress)
	}
	reason := strings.TrimSpace(input.Reason)
	normalizedReason := strings.ToLower(reason)
	if normalizedReason == "" {
		normalizedReason = verdict
	}

	if verdict == string(StuckAgentWatchdogVerdictInvalidOutput) ||
		strings.Contains(normalizedReason, "invalid output") ||
		strings.Contains(normalizedReason, "structured output") ||
		strings.Contains(normalizedReason, "schema") ||
		strings.Contains(normalizedReason, "parse") {
		if stuckAgentRecoveryHasRecoveryPath(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionRepairStructuredOutput),
				Reason:           "watchdog saw invalid structured output and durable recovery state is available",
				RequiresOperator: false,
			}
		}
		return stuckAgentRecoveryNeedsOperator(verdict, reason)
	}

	if verdict == string(StuckAgentWatchdogVerdictHeartbeatDeath) ||
		strings.Contains(normalizedReason, "heartbeat") ||
		strings.Contains(normalizedReason, "missing_agent") ||
		strings.Contains(normalizedReason, "offline_agent") {
		if stuckAgentRecoveryHasCheckpoint(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionResumeFromCheckpoint),
				Reason:           "watchdog saw a dead heartbeat and durable checkpoint state is available",
				RequiresOperator: false,
			}
		}
		if stuckAgentRecoveryHasPriorOwner(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionClaimSuccessorSession),
				Reason:           "watchdog saw a dead heartbeat but only the prior owner state is durable",
				RequiresOperator: false,
			}
		}
		if stuckAgentRecoveryHasRecoveryPath(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionRetryOrTimeoutRecovery),
				Reason:           "watchdog saw a dead heartbeat and only non-checkpoint durable recovery state is available",
				RequiresOperator: false,
			}
		}
		return stuckAgentRecoveryNeedsOperator(verdict, reason)
	}

	if verdict == string(StuckAgentWatchdogVerdictTimeout) ||
		strings.Contains(normalizedReason, "timeout") ||
		strings.Contains(normalizedReason, "timed out") ||
		strings.Contains(normalizedReason, "deadline") {
		if stuckAgentRecoveryBudgetExhausted(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionCompactSession),
				Reason:           "token budget remainder is exhausted",
				RequiresOperator: false,
			}
		}
		if stuckAgentRecoveryHasRecoveryPath(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionRetryOrTimeoutRecovery),
				Reason:           "watchdog timeout can be retried from durable recovery state",
				RequiresOperator: false,
			}
		}
		return stuckAgentRecoveryNeedsOperator(verdict, reason)
	}

	if verdict == string(StuckAgentWatchdogVerdictNoProgress) ||
		strings.Contains(normalizedReason, "no progress") ||
		strings.Contains(normalizedReason, "no_progress") ||
		strings.Contains(normalizedReason, "stalled") ||
		strings.Contains(normalizedReason, "stuck") {
		if stuckAgentRecoveryBudgetExhausted(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionCompactSession),
				Reason:           "token budget remainder is exhausted",
				RequiresOperator: false,
			}
		}
		if stuckAgentRecoveryHasCheckpoint(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionResumeFromCheckpoint),
				Reason:           "watchdog saw no progress and durable checkpoint state is available",
				RequiresOperator: false,
			}
		}
		if stuckAgentRecoveryHasPriorOwner(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionClaimSuccessorSession),
				Reason:           "watchdog saw no progress but only the prior owner state is durable",
				RequiresOperator: false,
			}
		}
		if stuckAgentRecoveryHasRecoveryPath(input.RecoveryManifest) {
			return StuckAgentRecoveryNextAction{
				Verdict:          verdict,
				Action:           string(StuckAgentRecoveryActionRetryOrTimeoutRecovery),
				Reason:           "watchdog saw no progress and only non-checkpoint durable recovery state is available",
				RequiresOperator: false,
			}
		}
		return stuckAgentRecoveryNeedsOperator(verdict, reason)
	}

	if stuckAgentRecoveryHasRecoveryPath(input.RecoveryManifest) {
		return StuckAgentRecoveryNextAction{
			Verdict:          verdict,
			Action:           string(StuckAgentRecoveryActionRetryOrTimeoutRecovery),
			Reason:           "watchdog verdict is recoverable from durable state",
			RequiresOperator: false,
		}
	}
	return stuckAgentRecoveryNeedsOperator(verdict, reason)
}

func stuckAgentRecoveryNeedsOperator(verdict, reason string) StuckAgentRecoveryNextAction {
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		trimmedReason = "no recovery path could be derived from durable state"
	} else {
		trimmedReason = "no recovery path could be derived from durable state: " + trimmedReason
	}
	return StuckAgentRecoveryNextAction{
		Verdict:          verdict,
		Action:           string(StuckAgentRecoveryActionNeedsOperator),
		Reason:           trimmedReason,
		RequiresOperator: true,
	}
}

func stuckAgentRecoveryHasRecoveryPath(manifest *StuckAgentRecoveryManifest) bool {
	return manifest != nil && (stuckAgentRecoveryHasCheckpoint(manifest) || len(manifest.OpenOps) > 0)
}

func stuckAgentRecoveryHasCheckpoint(manifest *StuckAgentRecoveryManifest) bool {
	return manifest != nil && manifest.Checkpoint != nil && strings.TrimSpace(manifest.Checkpoint.StepID) != ""
}

func stuckAgentRecoveryHasPriorOwner(manifest *StuckAgentRecoveryManifest) bool {
	if manifest == nil {
		return false
	}
	return strings.TrimSpace(manifest.PriorOwner.WorkspaceID) != "" ||
		strings.TrimSpace(manifest.PriorOwner.SessionID) != "" ||
		strings.TrimSpace(manifest.PriorOwner.AgentID) != "" ||
		strings.TrimSpace(manifest.PriorOwner.TaskID) != ""
}

func stuckAgentRecoveryBudgetExhausted(manifest *StuckAgentRecoveryManifest) bool {
	return manifest != nil && manifest.BudgetRemainder != nil && manifest.BudgetRemainder.RemainingTokens <= 0
}

func recoveryStepMoreRecent(candidate, current ExecutionStepRecord) bool {
	if candidate.UpdatedAt != current.UpdatedAt {
		return candidate.UpdatedAt > current.UpdatedAt
	}
	if candidate.CreatedAt != current.CreatedAt {
		return candidate.CreatedAt > current.CreatedAt
	}
	return candidate.StepID > current.StepID
}

func recoveryMapString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	switch value := values[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func recoveryNestedMapString(values map[string]any, key, nestedKey string) string {
	if values == nil {
		return ""
	}
	nested, ok := values[key].(map[string]any)
	if !ok {
		return ""
	}
	return recoveryMapString(nested, nestedKey)
}

func recoveryMapBool(values map[string]any, key string) bool {
	if values == nil {
		return false
	}
	value, _ := values[key].(bool)
	return value
}
