package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TaskCycleDurablePhaseReadback struct {
	TaskCycleID     string
	TaskID          string
	SessionID       string
	RunID           string
	SelectionID     string
	PhaseName       string
	LastDurableStep string
	TerminalStatus  string
	StepPhase       string
	SourceStepID    string
	SourceStepPhase string
	SourceStepAt    string
	CheckpointID    string
	ClaimResult     string
	PhaseSequence   int
}

func (r *Runtime) readLastDurableTaskCyclePhaseFromExecutionSteps(taskCycleID string, steps []ExecutionStepRecord) (TaskCycleDurablePhaseReadback, bool) {
	return taskCycleLastDurablePhaseFromExecutionSteps(taskCycleID, steps)
}

func (r *Runtime) readLastDurableTaskCyclePhase(ctx context.Context, runID, taskCycleID string) (TaskCycleDurablePhaseReadback, bool, error) {
	if r == nil || r.client == nil {
		return TaskCycleDurablePhaseReadback{}, false, fmt.Errorf("runtime client is unavailable")
	}
	workspaceID := strings.TrimSpace(r.cfg.WorkspaceID)
	runID = strings.TrimSpace(runID)
	if workspaceID == "" {
		return TaskCycleDurablePhaseReadback{}, false, fmt.Errorf("workspace_id is required")
	}
	if runID == "" {
		return TaskCycleDurablePhaseReadback{}, false, fmt.Errorf("run_id is required")
	}
	detail, err := r.client.GetExecutionRun(ctx, workspaceID, runID)
	if err != nil {
		return TaskCycleDurablePhaseReadback{}, false, err
	}
	if got := strings.TrimSpace(detail.Run.WorkspaceID); got != "" && got != workspaceID {
		return TaskCycleDurablePhaseReadback{}, false, fmt.Errorf("durable run workspace mismatch: got %q want %q", got, workspaceID)
	}
	if got := strings.TrimSpace(detail.Run.RunID); got != "" && got != runID {
		return TaskCycleDurablePhaseReadback{}, false, fmt.Errorf("durable run identity mismatch: got %q want %q", got, runID)
	}
	steps, err := executionStepsForDurableRun(workspaceID, runID, detail.Steps)
	if err != nil {
		return TaskCycleDurablePhaseReadback{}, false, err
	}
	readback, ok := r.readLastDurableTaskCyclePhaseFromExecutionSteps(taskCycleID, steps)
	return readback, ok, nil
}

func executionStepsForDurableRun(workspaceID, runID string, steps []ExecutionStepRecord) ([]ExecutionStepRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	runID = strings.TrimSpace(runID)
	filtered := make([]ExecutionStepRecord, 0, len(steps))
	for _, step := range steps {
		stepWorkspaceID := strings.TrimSpace(step.WorkspaceID)
		stepRunID := strings.TrimSpace(step.RunID)
		if stepWorkspaceID == "" {
			return nil, fmt.Errorf("durable step workspace is missing: step %s", strings.TrimSpace(step.StepID))
		}
		if stepRunID == "" {
			return nil, fmt.Errorf("durable step run is missing: step %s", strings.TrimSpace(step.StepID))
		}
		if stepWorkspaceID != workspaceID {
			return nil, fmt.Errorf("durable step workspace mismatch: step %s has %q want %q", strings.TrimSpace(step.StepID), stepWorkspaceID, workspaceID)
		}
		if stepRunID != runID {
			return nil, fmt.Errorf("durable step run mismatch: step %s has %q want %q", strings.TrimSpace(step.StepID), stepRunID, runID)
		}
		filtered = append(filtered, step)
	}
	return filtered, nil
}

func taskCycleLastDurablePhaseFromExecutionSteps(taskCycleID string, steps []ExecutionStepRecord) (TaskCycleDurablePhaseReadback, bool) {
	filteredTaskCycleID := strings.TrimSpace(taskCycleID)
	bestSeq := -1
	best := TaskCycleDurablePhaseReadback{}

	for _, step := range steps {
		for _, candidate := range taskCyclePhaseRecordCandidates(step.VerificationJSON) {
			recordTaskCycleID := strings.TrimSpace(mapStringField(candidate, "task_cycle_id"))
			recordTaskID := strings.TrimSpace(mapStringField(candidate, "task_id"))
			recordRunID := strings.TrimSpace(mapStringField(mapNestedField(candidate, "run"), "run_id"))
			if filteredTaskCycleID != "" && recordTaskCycleID != filteredTaskCycleID && recordTaskID != filteredTaskCycleID && recordRunID != filteredTaskCycleID {
				continue
			}
			seq, ok := mapIntField(candidate, "phase_sequence")
			if !ok || seq < bestSeq {
				continue
			}

			phaseName := strings.TrimSpace(mapStringField(candidate, "phase_name"))
			if phaseName == "" {
				continue
			}

			candidateStepAt := firstNonEmpty(strings.TrimSpace(step.UpdatedAt), strings.TrimSpace(step.CreatedAt))
			// CA-28: on a phase_sequence tie, the previous code took whichever step the
			// server returned last, making SourceStepAt (which drives the durable-stall
			// comparison) order-dependent -> false stall or false freshness. Break ties
			// deterministically toward the newest step time so progress reflects the
			// freshest durable evidence.
			if seq == bestSeq && bestSeq >= 0 && !taskCycleStepAtNewer(candidateStepAt, best.SourceStepAt) {
				continue
			}

			bestSeq = seq
			best = TaskCycleDurablePhaseReadback{
				TaskCycleID:     recordTaskCycleID,
				TaskID:          strings.TrimSpace(mapStringField(candidate, "task_id")),
				SessionID:       strings.TrimSpace(mapStringField(mapNestedField(candidate, "session"), "session_id")),
				RunID:           strings.TrimSpace(mapStringField(mapNestedField(candidate, "run"), "run_id")),
				SelectionID:     strings.TrimSpace(mapStringField(candidate, "selection_id")),
				PhaseName:       phaseName,
				LastDurableStep: strings.TrimSpace(mapStringField(mapNestedField(candidate, "checkpoint"), "last_durable_step")),
				TerminalStatus:  strings.TrimSpace(mapStringField(mapNestedField(candidate, "terminal_transition"), "status")),
				StepPhase:       strings.TrimSpace(step.Phase),
				SourceStepID:    strings.TrimSpace(step.StepID),
				SourceStepPhase: strings.TrimSpace(mapStringField(candidate, "source_step_phase")),
				SourceStepAt:    candidateStepAt,
				CheckpointID:    strings.TrimSpace(mapStringField(mapNestedField(candidate, "checkpoint"), "checkpoint_id")),
				ClaimResult:     strings.TrimSpace(mapStringField(mapNestedField(candidate, "claim"), "claim_result")),
				PhaseSequence:   seq,
			}
		}
	}

	if bestSeq < 0 {
		return TaskCycleDurablePhaseReadback{}, false
	}
	return best, true
}

// taskCycleStepAtNewer reports whether candidate is strictly newer than current,
// used to break phase_sequence ties deterministically (CA-28). RFC3339Nano values
// are compared as instants; non-parseable values fall back to lexical order. An
// empty candidate never wins; a non-empty candidate beats an empty current.
func taskCycleStepAtNewer(candidate, current string) bool {
	candidate = strings.TrimSpace(candidate)
	current = strings.TrimSpace(current)
	if candidate == "" {
		return false
	}
	if current == "" {
		return true
	}
	candidateAt, errC := time.Parse(time.RFC3339Nano, candidate)
	currentAt, errP := time.Parse(time.RFC3339Nano, current)
	if errC == nil && errP == nil {
		return candidateAt.After(currentAt)
	}
	return candidate > current
}

func taskCyclePhaseRecordCandidates(verification map[string]any) []map[string]any {
	if len(verification) == 0 {
		return nil
	}
	candidates := make([]map[string]any, 0, 4)
	appendCandidate := func(raw any) {
		record, ok := raw.(map[string]any)
		if !ok {
			return
		}
		candidates = append(candidates, record)
	}

	switch raw := verification["task_cycle_phase_records"].(type) {
	case []map[string]any:
		candidates = append(candidates, raw...)
	case []any:
		for _, item := range raw {
			appendCandidate(item)
		}
	case map[string]any:
		candidates = append(candidates, raw)
	}

	appendCandidate(verification["task_cycle_phase_record"])
	return candidates
}

func mapStringField(values map[string]any, key string) string {
	if len(values) == 0 {
		return ""
	}
	raw, ok := values[key]
	if !ok || raw == nil {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	case fmt.Stringer:
		return value.String()
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func mapNestedField(values map[string]any, key string) map[string]any {
	if len(values) == 0 {
		return nil
	}
	raw, ok := values[key]
	if !ok || raw == nil {
		return nil
	}
	if nested, ok := raw.(map[string]any); ok {
		return nested
	}
	return nil
}

func mapIntField(values map[string]any, key string) (int, bool) {
	if len(values) == 0 {
		return 0, false
	}
	raw, ok := values[key]
	if !ok || raw == nil {
		return 0, false
	}
	switch value := raw.(type) {
	case int:
		return value, true
	case int8:
		return int(value), true
	case int16:
		return int(value), true
	case int32:
		return int(value), true
	case int64:
		return int(value), true
	case uint:
		return int(value), true
	case uint8:
		return int(value), true
	case uint16:
		return int(value), true
	case uint32:
		return int(value), true
	case uint64:
		return int(value), true
	case float32:
		return int(value), true
	case float64:
		return int(value), true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
