package main

import (
	"context"
	"log"
	"strings"
	"time"
)

type transientBackoff struct {
	failures int
}

func (b *transientBackoff) Reset() {
	b.failures = 0
}

func (b *transientBackoff) Next(base, max time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	delay := base << b.failures
	if delay <= 0 || delay > max {
		delay = max
	}
	if b.failures < 16 {
		b.failures++
	}
	return delay
}

func (r *Runtime) invalidateBootstrap() {
	r.mu.Lock()
	r.lastBootstrap = time.Time{}
	r.mu.Unlock()
}

func (r *Runtime) bootstrapNeedsRefreshLocked() bool {
	if r.cfg.BootstrapEvery > 0 && !r.lastBootstrap.IsZero() && time.Since(r.lastBootstrap) < r.cfg.BootstrapEvery {
		if !r.bootstrapSnapshotLooksStaleLocked() {
			return false
		}
	}
	return true
}

func (r *Runtime) bootstrapSnapshotLooksStaleLocked() bool {
	if taskID := strings.TrimSpace(r.scratch.ActiveTaskID); taskID != "" {
		if _, ok := recoverableBootstrapTask(r.bootstrap.Snapshot.Tasks, taskID, r.cfg.AgentID); !ok {
			return true
		}
	}
	if sessionID := strings.TrimSpace(r.scratch.ActiveSessionID); sessionID != "" {
		if !findBootstrapSession(r.bootstrap.Snapshot.Sessions, sessionID, r.cfg.AgentID) {
			return true
		}
	}
	return false
}

func (r *Runtime) refreshBootstrapWithRecovery(ctx context.Context) (BootstrapResult, error) {
	bootstrap, err := r.client.Bootstrap(ctx, r.cfg.WorkspaceID, r.cfg.AgentID, r.cfg.UpdatesLimit)
	if err != nil {
		if !shouldRetryBootstrapWithFreshRegistration(err) || !r.canRegister() {
			return BootstrapResult{}, err
		}

		log.Printf("[runtime] bootstrap failed with auth error; refreshing registration")
		if _, regErr := r.registerAgent(ctx); regErr != nil {
			return BootstrapResult{}, regErr
		}
		bootstrap, err = r.client.Bootstrap(ctx, r.cfg.WorkspaceID, r.cfg.AgentID, r.cfg.UpdatesLimit)
		if err != nil {
			return BootstrapResult{}, err
		}
	}

	profile := LoadAgentProfile(r.cfg.Workdir)

	r.tokenMu.Lock()
	r.limitsGroupID = profile.GroupID
	r.tokenMu.Unlock()

	if limitGroup, _ := r.client.GetAgentLimits(ctx, r.cfg.WorkspaceID, r.cfg.AgentID); limitGroup != nil {
		r.tokenMu.Lock()
		if r.limitsGroupID == "" {
			r.limitsGroupID = limitGroup.GroupID
		}
		if !r.limitsLoaded {
			r.dailyRemaining = limitGroup.DailyRemaining
			r.weeklyRemaining = limitGroup.WeeklyRemaining
			r.lastReportedDailyRemaining = limitGroup.DailyRemaining
			r.limitsLoaded = true
		}
		r.tokenMu.Unlock()
	}
	return bootstrap, nil
}

func (r *Runtime) reconcileScratchAfterBootstrapLocked(now time.Time) bool {
	changed := false
	state := r.scratch
	if state.DocSHAs == nil {
		state.DocSHAs = map[string]string{}
		changed = true
	}

	stamp := now.UTC().Format(time.RFC3339Nano)
	if strings.TrimSpace(state.LastBootstrapAt) != stamp {
		state.LastBootstrapAt = stamp
		changed = true
	}

	if clearRuntimeSelfPauseAfterBootstrap(&state, stamp) {
		changed = true
	}

	if taskID := strings.TrimSpace(state.ActiveTaskID); taskID != "" {
		if _, ok := recoverableBootstrapTask(r.bootstrap.Snapshot.Tasks, taskID, r.cfg.AgentID); !ok {
			state.ActiveTaskID = ""
			state.ActiveSessionID = ""
			state.ActiveRunID = ""
			changed = true
		}
	}

	if pendingRequestResumeTriggerShouldClearAfterBootstrap(state, r.bootstrap.Snapshot.Tasks, r.cfg.AgentID) {
		state.PendingTrigger = ""
		state.PendingTriggerTask = ""
		state.PendingTriggerSession = ""
		state.PendingTriggerAt = ""
		changed = true
	}

	if sessionID := strings.TrimSpace(state.ActiveSessionID); sessionID != "" {
		if !findBootstrapSession(r.bootstrap.Snapshot.Sessions, sessionID, r.cfg.AgentID) {
			state.ActiveSessionID = ""
			state.ActiveRunID = ""
			changed = true
		}
	}

	if state.ActiveTaskID == "" && state.ActiveSessionID == "" && strings.TrimSpace(state.ActiveRunID) != "" {
		state.ActiveRunID = ""
		changed = true
	}

	if shouldQueueCompletionCoordinationResumeAfterBootstrap(state, r.bootstrap.Snapshot.Tasks, r.cfg.AgentID) {
		state.PendingTrigger = "request_resume"
		state.PendingTriggerTask = strings.TrimSpace(state.CompletionCoordinationTaskID)
		state.PendingTriggerSession = strings.TrimSpace(state.CompletionCoordinationSessionID)
		state.PendingTriggerAt = stamp
		changed = true
	}

	r.scratch = state
	return changed
}

func clearRuntimeSelfPauseAfterBootstrap(state *RuntimeScratchState, stamp string) bool {
	if state == nil || !state.ControlPaused || !runtimeControlPauseIsPlannerSelfPause(*state) {
		return false
	}
	state.ControlPaused = false
	state.ControlMode = "live"
	state.ControlAction = "resume"
	state.ControlActionReason = "auto-resume after daemon restart from planner self-pause"
	state.ControlActionAt = strings.TrimSpace(stamp)
	appendRuntimeAdvisorySignal(state, "SYSTEM RECOVERY: prior planner self-pause was cleared on daemon restart. Retry once with fresh bootstrap/context; if the same failure is still real, self-pause again with fresh evidence.")
	return true
}

func runtimeControlPauseIsPlannerSelfPause(state RuntimeScratchState) bool {
	if !state.ControlPaused {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		state.ControlAction,
		state.ControlActionReason,
		state.LastWakeTrigger,
		state.LastWakeReason,
		state.LastWakeSummary,
	}, "\n"))
	return strings.Contains(text, "planner self-paused") || strings.Contains(text, "planner_loop_reflection")
}

func shouldQueueCompletionCoordinationResumeAfterBootstrap(state RuntimeScratchState, tasks []WorkspaceTaskRecord, agentID string) bool {
	if normalizeWorkTrigger(state.PendingTrigger) != "" {
		return false
	}
	if strings.TrimSpace(state.CompletionCoordinationState) != completionCoordinationStateReviewReady {
		return false
	}
	taskID := strings.TrimSpace(state.CompletionCoordinationTaskID)
	if taskID == "" {
		return false
	}
	task, ok := findBootstrapTask(tasks, taskID)
	if !ok || !isClaimOwnedBy(task, agentID) {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(task.Status)) {
	case "DONE", "RESOLVED", "FAILED", "CANCELLED", "CANCELED":
		return false
	default:
		return true
	}
}

func pendingRequestResumeTriggerShouldClearAfterBootstrap(state RuntimeScratchState, tasks []WorkspaceTaskRecord, agentID string) bool {
	if normalizeWorkTrigger(state.PendingTrigger) != "request_resume" {
		return false
	}
	taskID := strings.TrimSpace(state.PendingTriggerTask)
	if taskID == "" {
		return false
	}
	task, ok := findBootstrapTask(tasks, taskID)
	if !ok {
		return true
	}
	return !requestResumeTaskRecoverableForAgent(task, agentID)
}

func (r *Runtime) recoverFromLoopFailure(ctx context.Context, scope string, err error) bool {
	if err == nil {
		return false
	}
	if !shouldRetryBootstrapWithFreshRegistration(err) {
		return false
	}
	cause := err
	if err := r.refreshBootstrap(ctx); err == nil {
		log.Printf("[%s] recovered after auth refresh", scope)
		r.logRecoveryMemory(scope, cause)
		return true
	}
	return false
}

func findBootstrapTask(tasks []WorkspaceTaskRecord, taskID string) (WorkspaceTaskRecord, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return WorkspaceTaskRecord{}, false
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == taskID {
			return task, true
		}
	}
	return WorkspaceTaskRecord{}, false
}

func findBootstrapSession(sessions []AgentSessionStateRecord, sessionID, agentID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	agentID = strings.TrimSpace(agentID)
	if sessionID == "" {
		return false
	}
	for _, session := range sessions {
		if strings.TrimSpace(session.SessionID) == sessionID && strings.TrimSpace(session.AgentID) == agentID {
			return true
		}
	}
	return false
}
