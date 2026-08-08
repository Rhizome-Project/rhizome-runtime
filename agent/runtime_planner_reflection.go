package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	plannerRepeatedFailureThreshold = 3
	plannerRepeatedFailureCooldown  = 10 * time.Minute
)

func (r *Runtime) handlePlannerRepeatedFailure(ctx context.Context, err error, now time.Time) (bool, error) {
	if r == nil || err == nil {
		return false, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	signature := normalizePlannerFailureSignature(err)
	if signature == "" {
		return false, nil
	}

	r.mu.Lock()
	if r.plannerFailureSignature == signature {
		r.plannerFailureCount++
	} else {
		r.plannerFailureSignature = signature
		r.plannerFailureCount = 1
	}
	count := r.plannerFailureCount
	alreadyReflected := r.plannerFailureReflectedSig == signature && !r.plannerFailureReflectedAt.IsZero() && now.Sub(r.plannerFailureReflectedAt) < plannerRepeatedFailureCooldown
	r.mu.Unlock()

	if count < plannerRepeatedFailureThreshold {
		return false, nil
	}
	if alreadyReflected {
		return true, nil
	}
	if err := r.materializePlannerLoopReflection(ctx, signature, count, now); err != nil {
		return true, err
	}
	r.mu.Lock()
	r.plannerFailureReflectedSig = signature
	r.plannerFailureReflectedAt = now
	r.mu.Unlock()
	return true, nil
}

func (r *Runtime) resetPlannerFailureReflection() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.plannerFailureSignature = ""
	r.plannerFailureCount = 0
	r.mu.Unlock()
}

func normalizePlannerFailureSignature(err error) string {
	if err == nil {
		return ""
	}
	text := oneLine(err.Error())
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 1000 {
		text = strings.TrimSpace(text[:1000])
	}
	return text
}

func (r *Runtime) materializePlannerLoopReflection(ctx context.Context, signature string, count int, now time.Time) error {
	if r == nil {
		return nil
	}
	docKey := fmt.Sprintf("agent.%s.planner_loop_reflection", strings.TrimSpace(r.cfg.AgentID))
	summary := fmt.Sprintf("Planner self-paused after %d repeated identical failures: %s", count, truncate(signature, 180))
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.LastSummary = summary
		state.LastWakeTrigger = "planner_loop_reflection"
		state.LastWakeReason = "repeated_planner_failure"
		state.LastWakeSummary = summary
		state.LastWakeAt = now.Format(time.RFC3339Nano)
		appendRuntimeAdvisorySignal(state, "SYSTEM SELF-REFLECTION: planner detected a repeated identical failure. Stop retrying the same path, inspect the recorded planner_loop_reflection doc/update, publish or resolve the coordination blocker, and only resume after the cause changes.")
	}); err != nil {
		return err
	}
	if r.client != nil {
		content := plannerLoopReflectionDocContent(r.cfg.AgentID, signature, count, now)
		if err := r.putDocWithRetry(ctx, docKey, "Planner Loop Reflection", content); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{
			"status":            "SELF_PAUSED",
			"failure_signature": signature,
			"failure_count":     count,
			"doc_key":           docKey,
			"next_action":       "do not retry identical planner path until coordination/runtime state changes",
		})
		if err := r.client.PostUpdate(ctx, UpdatePostInput{
			WorkspaceID: r.cfg.WorkspaceID,
			AgentID:     r.cfg.AgentID,
			UpdateType:  "issue",
			Summary:     summary,
			PayloadJSON: string(payload),
		}); err != nil {
			return err
		}
	}
	_, err := r.pauseRuntime(ctx, runtimeControlRequestPayload{Reason: summary})
	return err
}

func plannerLoopReflectionDocContent(agentID, signature string, count int, now time.Time) string {
	var b strings.Builder
	b.WriteString("# Planner Loop Reflection\n\n")
	b.WriteString("- agent_id: " + strings.TrimSpace(agentID) + "\n")
	b.WriteString("- detected_at: " + now.Format(time.RFC3339Nano) + "\n")
	b.WriteString(fmt.Sprintf("- repeated_failures: %d\n", count))
	b.WriteString("- action: runtime self-paused\n")
	b.WriteString("- next_action: do not retry the identical planner path until coordination or runtime state changes\n\n")
	b.WriteString("## Failure Signature\n\n")
	b.WriteString("```text\n")
	b.WriteString(strings.TrimSpace(signature))
	b.WriteString("\n```\n")
	return b.String()
}
