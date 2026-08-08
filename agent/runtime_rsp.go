package main

import (
	"fmt"
	"strings"
	"time"
)

type RSPRolloutPhase string

const (
	RSPRolloutObserveOnly RSPRolloutPhase = "observe_only"
	RSPRolloutAdvisory    RSPRolloutPhase = "advisory"
	RSPRolloutGated       RSPRolloutPhase = "gated"
	RSPRolloutLive        RSPRolloutPhase = "live"
)

type RuntimeRSPState struct {
	RolloutPhase         RSPRolloutPhase `json:"rollout_phase"`
	Verdict              string          `json:"verdict"`
	Reason               string          `json:"reason,omitempty"`
	RecommendedAction    string          `json:"recommended_action"`
	Evidence             []string        `json:"evidence,omitempty"`
	InterventionLevel    string          `json:"intervention_level,omitempty"`
	InterventionAction   string          `json:"intervention_action,omitempty"`
	LiveActuationAllowed bool            `json:"live_actuation_allowed"`
	AdvisoryOnly         bool            `json:"advisory_only"`
	ObservedAt           string          `json:"observed_at,omitempty"`
}

func normalizeRSPRolloutPhase(value string) RSPRolloutPhase {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(RSPRolloutAdvisory):
		return RSPRolloutAdvisory
	case string(RSPRolloutGated):
		return RSPRolloutGated
	case string(RSPRolloutLive):
		return RSPRolloutLive
	case "", string(RSPRolloutObserveOnly):
		return RSPRolloutObserveOnly
	default:
		return RSPRolloutObserveOnly
	}
}

func validateRSPRolloutPhase(value string) error {
	switch normalizeRSPRolloutPhase(value) {
	case RSPRolloutObserveOnly, RSPRolloutAdvisory, RSPRolloutGated, RSPRolloutLive:
		return nil
	default:
		return fmt.Errorf("unsupported RSP rollout phase: %s", strings.TrimSpace(value))
	}
}

func deriveRuntimeRSPState(cfg RuntimeConfig, snapshot RuntimeWatchdogSnapshot, now time.Time) RuntimeRSPState {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	phase := normalizeRSPRolloutPhase(cfg.RSPRolloutPhase)
	state := RuntimeRSPState{
		RolloutPhase:         phase,
		Verdict:              strings.TrimSpace(snapshot.MonitorVerdict),
		Reason:               strings.TrimSpace(snapshot.Reason),
		RecommendedAction:    strings.TrimSpace(snapshot.RecommendedAction),
		LiveActuationAllowed: phase == RSPRolloutLive,
		AdvisoryOnly:         phase != RSPRolloutLive,
		ObservedAt:           now.UTC().Format(time.RFC3339Nano),
	}

	evidence := make([]string, 0, 8)
	addEvidence := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		evidence = append(evidence, label+"="+value)
	}
	addEvidence("listener", snapshot.ListenerState)
	addEvidence("requests", snapshot.RequestState)
	addEvidence("planner", snapshot.PlannerState)
	addEvidence("memory", snapshot.Memory.State)
	addEvidence("task", snapshot.CurrentTaskID)
	addEvidence("session", snapshot.CurrentSessionID)
	addEvidence("pending_messages", fmt.Sprintf("%d", snapshot.PendingMessages))
	addEvidence("unacked_messages", fmt.Sprintf("%d", snapshot.UnackedMessages))
	if strings.TrimSpace(snapshot.Reason) != "" {
		evidence = append(evidence, "reason="+strings.TrimSpace(snapshot.Reason))
	}
	if strings.TrimSpace(snapshot.Memory.Reason) != "" {
		evidence = append(evidence, "memory_reason="+strings.TrimSpace(snapshot.Memory.Reason))
	}
	state.Evidence = evidence

	switch phase {
	case RSPRolloutObserveOnly:
		state.InterventionLevel = ""
		state.InterventionAction = "record_state_only"
	case RSPRolloutAdvisory:
		if snapshot.MonitorVerdict != "healthy" {
			state.InterventionLevel = "NUDGE"
			state.InterventionAction = firstNonEmpty(snapshot.RecommendedAction, "re-read the current task and keep making forward progress")
		}
	case RSPRolloutGated:
		if snapshot.MonitorVerdict == "stalled" {
			state.InterventionLevel = "FORCE_REPLAN"
			state.InterventionAction = firstNonEmpty(snapshot.RecommendedAction, "summarize the current state and replan before acting")
		} else if snapshot.MonitorVerdict == "degraded" {
			state.InterventionLevel = "DEMAND_SUMMARIZE"
			state.InterventionAction = firstNonEmpty(snapshot.RecommendedAction, "summarize the current state before continuing")
		}
	case RSPRolloutLive:
		if snapshot.MonitorVerdict == "stalled" {
			state.InterventionLevel = "PAUSE"
			state.InterventionAction = firstNonEmpty(snapshot.RecommendedAction, "pause the run and request human or peer help")
		} else if snapshot.MonitorVerdict == "degraded" {
			state.InterventionLevel = "FORCE_REPLAN"
			state.InterventionAction = firstNonEmpty(snapshot.RecommendedAction, "replan the task before continuing")
		}
	}

	return state
}

func (s RuntimeRSPState) hasIntervention() bool {
	return strings.TrimSpace(s.InterventionLevel) != ""
}

func (s RuntimeRSPState) intervention() *ShadowIntervention {
	if !s.hasIntervention() {
		return nil
	}
	reason := firstNonEmpty(s.Reason, s.Verdict)
	action := strings.TrimSpace(s.InterventionAction)
	if action == "" {
		action = "record the current state and adjust course"
	}
	return &ShadowIntervention{
		Level:  strings.TrimSpace(s.InterventionLevel),
		Reason: reason,
		Action: action,
	}
}

func (s RuntimeRSPState) advisorySignal() string {
	parts := []string{
		"RSP",
		"phase=" + string(s.RolloutPhase),
		"verdict=" + firstNonEmpty(s.Verdict, "unknown"),
	}
	if strings.TrimSpace(s.Reason) != "" {
		parts = append(parts, "reason="+strings.TrimSpace(s.Reason))
	}
	if strings.TrimSpace(s.InterventionLevel) != "" {
		parts = append(parts, "level="+strings.TrimSpace(s.InterventionLevel))
	}
	if strings.TrimSpace(s.InterventionAction) != "" {
		parts = append(parts, "action="+strings.TrimSpace(s.InterventionAction))
	}
	return strings.Join(parts, " ")
}
