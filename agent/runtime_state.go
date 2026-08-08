package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const runtimeScratchStateKey = "rnar.runtime.v1"

type RuntimeScratchState struct {
	MessageCursor                    string             `json:"message_cursor,omitempty"`
	NewsCursorCreatedAt              string             `json:"news_cursor_created_at,omitempty"`
	NewsCursorID                     string             `json:"news_cursor_id,omitempty"`
	ActiveSessionID                  string             `json:"active_session_id,omitempty"`
	ActiveTaskID                     string             `json:"active_task_id,omitempty"`
	ActiveRunID                      string             `json:"active_run_id,omitempty"`
	ControlPaused                    bool               `json:"control_paused,omitempty"`
	ControlMode                      string             `json:"control_mode,omitempty"`
	ControlAction                    string             `json:"control_action,omitempty"`
	ControlActionReason              string             `json:"control_action_reason,omitempty"`
	ControlActionAt                  string             `json:"control_action_at,omitempty"`
	ControlTargetTaskID              string             `json:"control_target_task_id,omitempty"`
	ControlTargetSessionID           string             `json:"control_target_session_id,omitempty"`
	ControlTargetTensionID           string             `json:"control_target_tension_id,omitempty"`
	ControlTensionAction             string             `json:"control_tension_action,omitempty"`
	ControlTensionRole               string             `json:"control_tension_role,omitempty"`
	ControlTensionState              string             `json:"control_tension_state,omitempty"`
	PendingTrigger                   string             `json:"pending_trigger,omitempty"`
	PendingTriggerTask               string             `json:"pending_trigger_task,omitempty"`
	PendingTriggerSession            string             `json:"pending_trigger_session,omitempty"`
	PendingTriggerAt                 string             `json:"pending_trigger_at,omitempty"`
	LastWakeTrigger                  string             `json:"last_wake_trigger,omitempty"`
	LastWakeReason                   string             `json:"last_wake_reason,omitempty"`
	LastWakeSummary                  string             `json:"last_wake_summary,omitempty"`
	LastWakeTaskID                   string             `json:"last_wake_task_id,omitempty"`
	LastWakeSessionID                string             `json:"last_wake_session_id,omitempty"`
	LastWakeAt                       string             `json:"last_wake_at,omitempty"`
	LastBootstrapAt                  string             `json:"last_bootstrap_at,omitempty"`
	LastSummary                      string             `json:"last_summary,omitempty"`
	LastNewsID                       string             `json:"last_news_id,omitempty"`
	LastNewsAt                       string             `json:"last_news_at,omitempty"`
	LastNewsSummary                  string             `json:"last_news_summary,omitempty"`
	ContinuationHoldTaskID           string             `json:"continuation_hold_task_id,omitempty"`
	ContinuationHoldSessionID        string             `json:"continuation_hold_session_id,omitempty"`
	ContinuationHoldRunID            string             `json:"continuation_hold_run_id,omitempty"`
	ContinuationHoldUntil            string             `json:"continuation_hold_until,omitempty"`
	ContinuationHoldSummary          string             `json:"continuation_hold_summary,omitempty"`
	ContinuationHoldCount            int                `json:"continuation_hold_count,omitempty"`
	NoopContinueTaskID               string             `json:"noop_continue_task_id,omitempty"`
	NoopContinueSessionID            string             `json:"noop_continue_session_id,omitempty"`
	NoopContinueRunID                string             `json:"noop_continue_run_id,omitempty"`
	NoopContinueSignature            string             `json:"noop_continue_signature,omitempty"`
	NoopContinueSummary              string             `json:"noop_continue_summary,omitempty"`
	NoopContinueAt                   string             `json:"noop_continue_at,omitempty"`
	NoopContinueCount                int                `json:"noop_continue_count,omitempty"`
	TelicBaselineBuildOK             bool               `json:"telic_baseline_build_ok,omitempty"`
	TelicBaselinePass                int                `json:"telic_baseline_pass,omitempty"`
	TelicLastBuildOK                 bool               `json:"telic_last_build_ok,omitempty"`
	TelicLastPass                    int                `json:"telic_last_pass,omitempty"`
	TelicLastTotal                   int                `json:"telic_last_total,omitempty"`
	TelicLastReddest                 string             `json:"telic_last_reddest,omitempty"`
	TelicLastFrozenKnown             bool               `json:"telic_last_frozen_known,omitempty"`
	TelicLastFrozenGreen             bool               `json:"telic_last_frozen_green,omitempty"`
	RequiredTransitionTaskID         string             `json:"required_transition_task_id,omitempty"`
	RequiredTransitionSessionID      string             `json:"required_transition_session_id,omitempty"`
	RequiredTransitionRunID          string             `json:"required_transition_run_id,omitempty"`
	RequiredTransitionTool           string             `json:"required_transition_tool,omitempty"`
	RequiredTransitionSummary        string             `json:"required_transition_summary,omitempty"`
	RequiredTransitionAt             string             `json:"required_transition_at,omitempty"`
	RequiredTransitionCount          int                `json:"required_transition_count,omitempty"`
	StrategicRepairPollAt            string             `json:"strategic_repair_poll_at,omitempty"`
	ImmediateProjectResumeTaskID     string             `json:"immediate_project_resume_task_id,omitempty"`
	ImmediateProjectResumeSessionID  string             `json:"immediate_project_resume_session_id,omitempty"`
	ImmediateProjectResumeRunID      string             `json:"immediate_project_resume_run_id,omitempty"`
	ImmediateProjectResumeSignature  string             `json:"immediate_project_resume_signature,omitempty"`
	ImmediateProjectResumeAt         string             `json:"immediate_project_resume_at,omitempty"`
	ImmediateProjectResumeCount      int                `json:"immediate_project_resume_count,omitempty"`
	CompletionCoordinationTaskID     string             `json:"completion_coordination_task_id,omitempty"`
	CompletionCoordinationRunID      string             `json:"completion_coordination_run_id,omitempty"`
	CompletionCoordinationSessionID  string             `json:"completion_coordination_session_id,omitempty"`
	CompletionCoordinationPeerID     string             `json:"completion_coordination_peer_id,omitempty"`
	CompletionCoordinationReqID      string             `json:"completion_coordination_request_id,omitempty"`
	CompletionCoordinationState      string             `json:"completion_coordination_state,omitempty"`
	CompletionCoordinationAt         string             `json:"completion_coordination_at,omitempty"`
	CompletionCoordinationReply      string             `json:"completion_coordination_reply,omitempty"`
	CollaborationFanoutTaskID        string             `json:"collaboration_fanout_task_id,omitempty"`
	CollaborationFanoutRunID         string             `json:"collaboration_fanout_run_id,omitempty"`
	CollaborationFanoutAt            string             `json:"collaboration_fanout_at,omitempty"`
	CollaborationFanoutRequests      map[string]string  `json:"collaboration_fanout_requests,omitempty"`
	ProjectRoleRequestKey            string             `json:"project_role_request_key,omitempty"`
	ProjectRoleRequestID             string             `json:"project_role_request_id,omitempty"`
	ProjectRoleRequestAt             string             `json:"project_role_request_at,omitempty"`
	ProjectClaimHoldKind             string             `json:"project_claim_hold_kind,omitempty"`
	ProjectClaimHoldTaskID           string             `json:"project_claim_hold_task_id,omitempty"`
	ProjectClaimHoldProjectID        string             `json:"project_claim_hold_project_id,omitempty"`
	ProjectClaimHoldUntil            string             `json:"project_claim_hold_until,omitempty"`
	ProjectClaimHoldSummary          string             `json:"project_claim_hold_summary,omitempty"`
	ProjectClaimRepairKey            string             `json:"project_claim_repair_key,omitempty"`
	ProjectClaimRepairTaskID         string             `json:"project_claim_repair_task_id,omitempty"`
	ProjectClaimRepairRequestID      string             `json:"project_claim_repair_request_id,omitempty"`
	ProjectClaimRepairAt             string             `json:"project_claim_repair_at,omitempty"`
	StrategyBlockerTaskID            string             `json:"strategy_blocker_task_id,omitempty"`
	StrategyBlockerProjectID         string             `json:"strategy_blocker_project_id,omitempty"`
	StrategyBlockerResultFingerprint string             `json:"strategy_blocker_result_fingerprint,omitempty"`
	StrategyBlockerCoordFingerprint  string             `json:"strategy_blocker_coord_fingerprint,omitempty"`
	StrategyBlockerSummary           string             `json:"strategy_blocker_summary,omitempty"`
	StrategyBlockerRecordedAt        string             `json:"strategy_blocker_recorded_at,omitempty"`
	AmbientAutonomyKey               string             `json:"ambient_autonomy_key,omitempty"`
	AmbientAutonomyAt                string             `json:"ambient_autonomy_at,omitempty"`
	AmbientAutonomySummary           string             `json:"ambient_autonomy_summary,omitempty"`
	AmbientAutonomyOutcome           string             `json:"ambient_autonomy_outcome,omitempty"`
	AmbientAutonomyDocKey            string             `json:"ambient_autonomy_doc_key,omitempty"`
	AmbientAutonomyCount             int                `json:"ambient_autonomy_count,omitempty"`
	TimeoutResumeTaskID              string             `json:"timeout_resume_task_id,omitempty"`
	TimeoutResumeSessionID           string             `json:"timeout_resume_session_id,omitempty"`
	TimeoutResumeRunID               string             `json:"timeout_resume_run_id,omitempty"`
	TimeoutResumeSummary             string             `json:"timeout_resume_summary,omitempty"`
	TimeoutResumeAt                  string             `json:"timeout_resume_at,omitempty"`
	TimeoutResumeCount               int                `json:"timeout_resume_count,omitempty"`
	CapabilityBootSnapshotID         string             `json:"capability_boot_snapshot_id,omitempty"`
	CapabilityBootSnapshotPath       string             `json:"capability_boot_snapshot_path,omitempty"`
	ActiveCapabilitySnapshotID       string             `json:"active_capability_snapshot_id,omitempty"`
	ActiveCapabilitySnapshotPath     string             `json:"active_capability_snapshot_path,omitempty"`
	AdvisorySignals                  []string           `json:"advisory_signals,omitempty"`
	ReflectionSignals                []ReflectionSignal `json:"reflection_signals,omitempty"`
	DocSHAs                          map[string]string  `json:"doc_shas"`
	WorkspaceDocWakeVersions         map[string]string  `json:"workspace_doc_wake_versions,omitempty"`
}

// ReflectionSignal is one entry in the Layer B dedicated reflection channel: a
// prioritized, bounded buffer for self-authored heartbeat/reflection insight that
// is isolated from the 5-slot watchdog advisory ring. Writers are restricted to
// reflection-class heartbeats (metacognition / system_sensing / strategy_synthesis).
type ReflectionSignal struct {
	Source    string `json:"source,omitempty"` // heartbeat id
	Kind      string `json:"kind,omitempty"`   // observation | risk | opportunity | rule_candidate
	Text      string `json:"text"`
	Priority  int    `json:"priority,omitempty"`   // higher kept on eviction
	Count     int    `json:"count,omitempty"`      // dedupe occurrences
	CreatedAt string `json:"created_at,omitempty"` // RFC3339
	UpdatedAt string `json:"updated_at,omitempty"` // RFC3339; bumped on dedupe hit
}

func loadScratchState(ctx context.Context, client *RhizomeClient, workspaceID, agentID string) (RuntimeScratchState, error) {
	if client == nil {
		return RuntimeScratchState{DocSHAs: map[string]string{}}, nil
	}
	value, ok, err := client.StateGet(ctx, workspaceID, agentID, runtimeScratchStateKey)
	if err != nil {
		return RuntimeScratchState{}, err
	}
	if !ok || value == "" {
		return RuntimeScratchState{DocSHAs: map[string]string{}}, nil
	}

	var state RuntimeScratchState
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		return RuntimeScratchState{}, fmt.Errorf("decode scratch state: %w", err)
	}
	if state.DocSHAs == nil {
		state.DocSHAs = map[string]string{}
	}
	if state.WorkspaceDocWakeVersions == nil {
		state.WorkspaceDocWakeVersions = map[string]string{}
	}
	if state.CollaborationFanoutRequests == nil {
		state.CollaborationFanoutRequests = map[string]string{}
	}
	state.AdvisorySignals = normalizeRuntimeAdvisorySignals(state.AdvisorySignals)
	state.ReflectionSignals = normalizeReflectionSignals(state.ReflectionSignals)
	return state, nil
}

func saveScratchState(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, state RuntimeScratchState) error {
	if client == nil {
		return nil
	}
	state = clearConsumedPendingTriggerInState(state)
	state = normalizeScratchStateForPersist(state)
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode scratch state: %w", err)
	}
	return client.StateSet(ctx, workspaceID, agentID, runtimeScratchStateKey, string(raw))
}

func normalizeScratchStateForPersist(state RuntimeScratchState) RuntimeScratchState {
	if state.DocSHAs == nil {
		state.DocSHAs = map[string]string{}
	}
	if state.WorkspaceDocWakeVersions == nil {
		state.WorkspaceDocWakeVersions = map[string]string{}
	}
	if state.CollaborationFanoutRequests == nil {
		state.CollaborationFanoutRequests = map[string]string{}
	}
	state.AdvisorySignals = normalizeRuntimeAdvisorySignals(state.AdvisorySignals)
	state.ReflectionSignals = normalizeReflectionSignals(state.ReflectionSignals)
	return state
}

func runtimeShellFailureAdvisory() string {
	return "SYSTEM TOOL GUIDANCE: shell failed. Shell is trusted local execution when enabled; inspect the output, adjust the command or workdir/cwd, and retry when useful. Materialize a blocker or tension only when a concrete dependency, executable, permission, or environment capability is missing."
}

func normalizeRuntimeAdvisorySignals(signals []string) []string {
	if len(signals) == 0 {
		return signals
	}
	out := make([]string, 0, len(signals))
	for _, signal := range signals {
		signal = normalizeRuntimeAdvisorySignal(signal)
		if strings.TrimSpace(signal) != "" {
			out = append(out, signal)
		}
	}
	return out
}

// reflectionKindCanonical normalizes a reflection-signal kind to one of the four
// recognized classes, defaulting to "observation".
func reflectionKindCanonical(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "risk", "anti", "anti_procedure", "warning":
		return "risk"
	case "rule", "rule_candidate", "invariant":
		return "rule_candidate"
	case "opportunity", "prefer", "procedure":
		return "opportunity"
	default:
		return "observation"
	}
}

// reflectionKindPriority maps a kind to its eviction priority. Higher is kept when
// the buffer is full: a risk is never evicted by a routine observation.
func reflectionKindPriority(kind string) int {
	switch reflectionKindCanonical(kind) {
	case "risk":
		return 4
	case "rule_candidate":
		return 3
	case "opportunity":
		return 2
	default:
		return 1
	}
}

// classifyReflectionKind infers a reflection kind from headline text so routed
// heartbeat advisories carry a meaningful priority even without an explicit kind.
func classifyReflectionKind(text string) string {
	lowered := strings.ToLower(text)
	switch {
	case strings.Contains(lowered, "risk") || strings.Contains(lowered, "avoid") ||
		strings.Contains(lowered, "starv") || strings.Contains(lowered, "stall") ||
		strings.Contains(lowered, "anti-pattern") || strings.Contains(lowered, "do not") ||
		strings.Contains(lowered, "critical"):
		return "risk"
	case strings.Contains(lowered, "rule") || strings.Contains(lowered, "invariant") ||
		strings.Contains(lowered, "always") || strings.Contains(lowered, "never"):
		return "rule_candidate"
	case strings.Contains(lowered, "opportunity") || strings.Contains(lowered, "prefer") ||
		strings.Contains(lowered, "should") || strings.Contains(lowered, "could"):
		return "opportunity"
	default:
		return "observation"
	}
}

func normalizeReflectionSignals(signals []ReflectionSignal) []ReflectionSignal {
	if len(signals) == 0 {
		return signals
	}
	out := make([]ReflectionSignal, 0, len(signals))
	for _, sig := range signals {
		sig.Source = strings.TrimSpace(sig.Source)
		sig.Text = strings.TrimSpace(sig.Text)
		if sig.Text == "" {
			continue
		}
		sig.Kind = reflectionKindCanonical(sig.Kind)
		if sig.Priority <= 0 {
			sig.Priority = reflectionKindPriority(sig.Kind)
		}
		if sig.Count <= 0 {
			sig.Count = 1
		}
		out = append(out, sig)
	}
	return out
}

// appendReflectionSignal adds a signal to the Layer B reflection channel with
// dedupe-by-(source,text) and priority-aware eviction. cap<=0 means the channel
// is disabled and the caller must fall back to the advisory ring.
func appendReflectionSignal(state *RuntimeScratchState, signal ReflectionSignal, cap int) {
	if state == nil || cap <= 0 {
		return
	}
	signal.Source = strings.TrimSpace(signal.Source)
	signal.Text = strings.TrimSpace(signal.Text)
	if signal.Text == "" {
		return
	}
	signal.Kind = reflectionKindCanonical(signal.Kind)
	if signal.Priority <= 0 {
		signal.Priority = reflectionKindPriority(signal.Kind)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(signal.CreatedAt) == "" {
		signal.CreatedAt = now
	}
	signal.UpdatedAt = now
	signal.Count = 1

	// Dedupe by (source, text): refresh the existing entry instead of consuming a slot.
	for idx := range state.ReflectionSignals {
		existing := &state.ReflectionSignals[idx]
		if existing.Source == signal.Source && strings.EqualFold(existing.Text, signal.Text) {
			existing.Count++
			existing.UpdatedAt = now
			if signal.Priority > existing.Priority {
				existing.Priority = signal.Priority
				existing.Kind = signal.Kind
			}
			return
		}
	}

	state.ReflectionSignals = append(state.ReflectionSignals, signal)
	if len(state.ReflectionSignals) <= cap {
		return
	}
	// Over capacity: evict the lowest-priority entry (ties broken by oldest UpdatedAt).
	evictIdx := 0
	for idx := 1; idx < len(state.ReflectionSignals); idx++ {
		cand := state.ReflectionSignals[idx]
		low := state.ReflectionSignals[evictIdx]
		if cand.Priority < low.Priority ||
			(cand.Priority == low.Priority && cand.UpdatedAt < low.UpdatedAt) {
			evictIdx = idx
		}
	}
	state.ReflectionSignals = append(state.ReflectionSignals[:evictIdx], state.ReflectionSignals[evictIdx+1:]...)
}

func normalizeRuntimeAdvisorySignal(signal string) string {
	signal = strings.TrimSpace(signal)
	if signal == "" {
		return ""
	}
	if strings.Contains(signal, "SYSTEM TOOL GUIDANCE: shell failed") &&
		strings.Contains(signal, "Do not retry shell blindly") &&
		!strings.Contains(signal, "Corrective retry is allowed") {
		return runtimeShellFailureAdvisory()
	}
	return signal
}
