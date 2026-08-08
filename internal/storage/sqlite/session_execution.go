package sqlite

import (
	"context"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type SessionExecutionSyncResult struct {
	Run       ExecutionRunRecord  `json:"run"`
	RunEvent  RuntimeEventRecord  `json:"run_event"`
	Step      ExecutionStepRecord `json:"step"`
	StepEvent RuntimeEventRecord  `json:"step_event"`
}

// SyncExecutionRunFromSessionState projects a coordination state transition into
// the execution run/step journal. It is intentionally best-effort at call sites
// and does not change session semantics.
func (s *Store) SyncExecutionRunFromSessionState(ctx context.Context, state AgentSessionStateRecord) error {
	_, err := s.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	return err
}

// SyncExecutionRunFromSessionStateWithResult projects a coordination state
// transition into the execution run/step journal and returns the exact
// persisted rows and runtime events for optional live mirroring.
func (s *Store) SyncExecutionRunFromSessionStateWithResult(ctx context.Context, state AgentSessionStateRecord) (SessionExecutionSyncResult, error) {
	runID := sessionExecutionRunID(state.SessionID)
	if runID == "" || strings.TrimSpace(state.WorkspaceID) == "" {
		return SessionExecutionSyncResult{}, nil
	}

	sessionRecord, err := s.GetAgentSession(ctx, state.SessionID)
	if err != nil && !errors.Is(err, ErrSessionNotFound) {
		return SessionExecutionSyncResult{}, err
	}

	run, runEvent, err := s.UpsertExecutionRunWithEvent(ctx, ExecutionRunInput{
		RunID:                 runID,
		WorkspaceID:           state.WorkspaceID,
		TaskID:                state.TaskID,
		SessionID:             state.SessionID,
		AgentID:               state.AgentID,
		Title:                 executionRunTitleForSessionState(state),
		Summary:               strings.TrimSpace(state.Summary),
		Status:                executionRunStatusForSessionState(state, sessionRecord),
		Outcome:               executionRunOutcomeForSessionState(state, sessionRecord),
		PromptContextEnvelope: sessionExecutionPromptContextEnvelope(state, "agent.session.execution.run.sync"),
	})
	if err != nil {
		return SessionExecutionSyncResult{}, err
	}

	evidence := make([]string, 0, len(state.RelatedDocKeys)+len(state.RelatedArtifactRefs)+len(state.BlockedOn))
	for _, key := range state.RelatedDocKeys {
		if trimmed := strings.TrimSpace(key); trimmed != "" {
			evidence = append(evidence, "doc:"+trimmed)
		}
	}
	for _, artifact := range state.RelatedArtifactRefs {
		if trimmed := strings.TrimSpace(artifact.Ref); trimmed != "" {
			evidence = append(evidence, "artifact:"+trimmed)
		}
	}
	for _, blocked := range state.BlockedOn {
		kind := strings.TrimSpace(blocked.Kind)
		detail := strings.TrimSpace(blocked.Detail)
		if kind != "" || detail != "" {
			evidence = append(evidence, "blocked:"+firstNonEmpty(kind, "unknown")+":"+detail)
		}
	}

	step, stepEvent, err := s.RecordExecutionStepWithEvent(ctx, ExecutionStepInput{
		RunID:                 run.RunID,
		WorkspaceID:           run.WorkspaceID,
		Phase:                 executionStepPhaseForSessionState(state),
		Title:                 executionStepTitleForSessionState(state),
		Summary:               strings.TrimSpace(state.Summary),
		Status:                executionStepStatusForSessionState(state, sessionRecord),
		Evidence:              evidence,
		PromptContextEnvelope: sessionExecutionPromptContextEnvelope(state, "agent.session.execution.step.sync"),
		Verification: map[string]any{
			"session_status":       strings.TrimSpace(state.Status),
			"update_type":          strings.TrimSpace(state.UpdateType),
			"keep_session_active":  state.KeepSessionActive,
			"decision_needed_from": strings.TrimSpace(state.DecisionNeededFrom),
			"handoff_to":           strings.TrimSpace(state.HandoffTo),
		},
	})
	if err != nil {
		return SessionExecutionSyncResult{}, err
	}
	return SessionExecutionSyncResult{
		Run:       run,
		RunEvent:  runEvent,
		Step:      step,
		StepEvent: stepEvent,
	}, nil
}

func sessionExecutionPromptContextEnvelope(state AgentSessionStateRecord, surface string) map[string]any {
	principalType := "system"
	principalID := "session_projection"
	if agentID := strings.TrimSpace(state.AgentID); agentID != "" {
		principalType = "agent"
		principalID = agentID
	}
	return BuildExecutionPromptContextEnvelope(
		surface,
		"server_session_projection",
		state.WorkspaceID,
		principalType,
		principalID,
	)
}

func sessionExecutionRunID(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ""
	}
	return "session:" + sessionID
}

func executionRunTitleForSessionState(state AgentSessionStateRecord) string {
	if taskID := strings.TrimSpace(state.TaskID); taskID != "" {
		return "Session execution for task " + taskID
	}
	return "Session execution " + strings.TrimSpace(state.SessionID)
}

func executionRunStatusForSessionState(state AgentSessionStateRecord, sessionRecord *AgentSessionRecord) string {
	switch strings.ToUpper(strings.TrimSpace(state.Status)) {
	case model.SessionStatusBlocked, model.SessionStatusWaitingDecision, model.SessionStatusHandoffPending:
		return "BLOCKED"
	case model.SessionStatusEnded, "COMPLETED":
		if sessionRecord != nil {
			switch strings.ToUpper(strings.TrimSpace(sessionRecord.Status)) {
			case "FAILED", "ERROR":
				return "FAILED"
			case "CANCELLED":
				return "CANCELLED"
			}
		}
		return "COMPLETED"
	case "FAILED", "ERROR", "ABORTED":
		return "FAILED"
	case "CANCELLED":
		return "CANCELLED"
	default:
		return "ACTIVE"
	}
}

func executionRunOutcomeForSessionState(state AgentSessionStateRecord, sessionRecord *AgentSessionRecord) string {
	switch strings.ToUpper(strings.TrimSpace(state.Status)) {
	case model.SessionStatusEnded, "COMPLETED", "FAILED", "ERROR", "ABORTED", "CANCELLED":
	default:
		return ""
	}
	if sessionRecord != nil {
		if trimmed := strings.TrimSpace(sessionRecord.ErrorMessage); trimmed != "" {
			return trimmed
		}
		if trimmed := strings.TrimSpace(sessionRecord.StopReason); trimmed != "" {
			return trimmed
		}
	}
	return strings.TrimSpace(state.Summary)
}

func executionStepPhaseForSessionState(state AgentSessionStateRecord) string {
	switch strings.TrimSpace(state.UpdateType) {
	case model.SessionEventStart:
		return "PLAN"
	case model.SessionEventEnd:
		return "VERIFY"
	default:
		return "EXECUTE"
	}
}

func executionStepTitleForSessionState(state AgentSessionStateRecord) string {
	switch strings.TrimSpace(state.UpdateType) {
	case model.SessionEventStart:
		return "Session started"
	case model.SessionEventBlocked:
		return "Session blocked"
	case model.SessionEventDecisionNeeded:
		return "Decision needed"
	case model.SessionEventKeepalive:
		return "Session keepalive"
	case model.SessionEventEnd:
		return "Session ended"
	default:
		return "Session status updated"
	}
}

func executionStepStatusForSessionState(state AgentSessionStateRecord, sessionRecord *AgentSessionRecord) string {
	switch executionRunStatusForSessionState(state, sessionRecord) {
	case "COMPLETED":
		return "COMPLETED"
	case "FAILED", "CANCELLED":
		return "FAILED"
	case "BLOCKED":
		return "BLOCKED"
	default:
		return "ACTIVE"
	}
}
