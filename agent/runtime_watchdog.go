package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

type loopHealth struct {
	LastStartedAt       time.Time
	LastAttemptAt       time.Time
	LastSuccessAt       time.Time
	LastFailureAt       time.Time
	ConsecutiveFailures int
	LastError           string
}

type runtimeHealthState struct {
	StartedAt              time.Time
	PreviousStartedAt      time.Time
	PreviousObservedAt     time.Time
	RestartCount           int
	Heartbeat              loopHealth
	InternalHeartbeat      loopHealth
	AmbientAutonomy        loopHealth
	Listener               loopHealth
	SSEEventLoop           loopHealth
	Requests               loopHealth
	Planner                loopHealth
	PlannerTimeout         loopHealth
	Watchdog               loopHealth
	ScratchReplay          loopHealth
	MemorySync             loopHealth
	LastTaskStartedAt      time.Time
	LastTaskProgressAt     time.Time
	LastTaskProgressSig    string
	LastTaskSummary        string
	LastWatchdogAt         time.Time
	LastWatchdogPublishAt  time.Time
	LastWatchdogPublished  string
	LastWatchdogReason     string
	LastMemoryRepairAt     time.Time
	LastMemoryRepairAction string
	LastMemoryRepairReason string
}

type RuntimeMemoryHealthSnapshot struct {
	State                  string `json:"state"`
	Reason                 string `json:"reason,omitempty"`
	RecommendedAction      string `json:"recommended_action"`
	PacketCacheEntries     int    `json:"packet_cache_entries"`
	StaleDigests           int    `json:"stale_digests"`
	PendingPromotions      int    `json:"pending_promotions"`
	PromotionFailures      int    `json:"promotion_failures"`
	ConsecutiveStaleReads  int    `json:"consecutive_stale_reads"`
	ConsecutiveP2Misses    int    `json:"consecutive_p2_misses"`
	LastStaleHitAt         string `json:"last_stale_hit_at,omitempty"`
	LastP2HitAt            string `json:"last_p2_hit_at,omitempty"`
	LastP2MissAt           string `json:"last_p2_miss_at,omitempty"`
	LastPacketBuiltAt      string `json:"last_packet_built_at,omitempty"`
	LastPromotionQueuedAt  string `json:"last_promotion_queued_at,omitempty"`
	LastPromotionAttemptAt string `json:"last_promotion_attempt_at,omitempty"`
	LastPromotionSyncedAt  string `json:"last_promotion_synced_at,omitempty"`
}

type RuntimeWatchdogSnapshot struct {
	MonitorVerdict            string                      `json:"monitor_verdict"`
	Reason                    string                      `json:"reason,omitempty"`
	RecommendedAction         string                      `json:"recommended_action"`
	RSP                       RuntimeRSPState             `json:"rsp,omitempty"`
	ListenerState             string                      `json:"listener_state"`
	HeartbeatState            string                      `json:"heartbeat_state"`
	InternalHeartbeatState    string                      `json:"internal_heartbeat_state"`
	AmbientAutonomyState      string                      `json:"ambient_autonomy_state"`
	SSEEventLoopState         string                      `json:"sse_event_loop_state"`
	RequestState              string                      `json:"request_state"`
	PlannerState              string                      `json:"planner_state"`
	PlannerTimeoutState       string                      `json:"planner_timeout_state"`
	Memory                    RuntimeMemoryHealthSnapshot `json:"memory,omitempty"`
	HeartbeatFailures         int                         `json:"heartbeat_failures"`
	InternalHeartbeatFailures int                         `json:"internal_heartbeat_failures"`
	AmbientAutonomyFailures   int                         `json:"ambient_autonomy_failures"`
	ListenerFailures          int                         `json:"listener_failures"`
	SSEEventLoopFailures      int                         `json:"sse_event_loop_failures"`
	RequestFailures           int                         `json:"request_failures"`
	PlannerFailures           int                         `json:"planner_failures"`
	PlannerTimeoutFailures    int                         `json:"planner_timeout_failures"`
	CurrentTaskID             string                      `json:"current_task_id,omitempty"`
	CurrentSessionID          string                      `json:"current_session_id,omitempty"`
	CurrentRunID              string                      `json:"current_run_id,omitempty"`
	LastTaskSummary           string                      `json:"last_task_summary,omitempty"`
	LastDurablePhase          string                      `json:"last_durable_phase,omitempty"`
	LastDurableStep           string                      `json:"last_durable_step,omitempty"`
	LastDurableStepID         string                      `json:"last_durable_step_id,omitempty"`
	LastDurableAt             string                      `json:"last_durable_at,omitempty"`
	DurableReadback           string                      `json:"durable_readback,omitempty"`
	LastListenerSuccess       string                      `json:"last_listener_success_at,omitempty"`
	LastRequestSuccess        string                      `json:"last_request_success_at,omitempty"`
	LastPlannerSuccess        string                      `json:"last_planner_success_at,omitempty"`
	LastTaskStartedAt         string                      `json:"last_task_started_at,omitempty"`
	LastTaskProgressAt        string                      `json:"last_task_progress_at,omitempty"`
	PendingMessages           int                         `json:"pending_messages"`
	UnreadMessages            int                         `json:"unread_messages"`
	UnackedMessages           int                         `json:"unacked_messages"`
	NeedsAttention            bool                        `json:"needs_attention"`
}

func newRuntimeHealthState(startedAt time.Time) runtimeHealthState {
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	return runtimeHealthState{
		StartedAt:         startedAt,
		Heartbeat:         loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		InternalHeartbeat: loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		AmbientAutonomy:   loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		Listener:          loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		SSEEventLoop:      loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		Requests:          loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		Planner:           loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		PlannerTimeout:    loopHealth{LastStartedAt: startedAt, LastSuccessAt: startedAt},
		Watchdog:          loopHealth{LastStartedAt: startedAt},
		ScratchReplay: loopHealth{
			LastStartedAt: startedAt,
		},
		MemorySync: loopHealth{LastStartedAt: startedAt},
	}
}

func (r *Runtime) recordLoopStarted(scope string, at time.Time) {
	if r == nil {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	changed := recordLoopStartedLocked(&r.health, scope, at)
	snapshot, ok := r.runtimeLoopHealthSnapshotLocked(at)
	r.mu.Unlock()
	if changed && ok {
		r.persistRuntimeLoopHealthSnapshotWithLog(snapshot)
	}
}

func (r *Runtime) recordLoopSuccess(scope string, at time.Time) {
	if r == nil {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	changed := recordLoopSuccessLocked(&r.health, scope, at)
	snapshot, ok := r.runtimeLoopHealthSnapshotLocked(at)
	r.mu.Unlock()
	if changed && ok {
		r.persistRuntimeLoopHealthSnapshotWithLog(snapshot)
	}
}

func (r *Runtime) recordLoopFailure(scope string, err error, at time.Time) {
	if r == nil {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	changed := recordLoopFailureLocked(&r.health, scope, err, at)
	snapshot, ok := r.runtimeLoopHealthSnapshotLocked(at)
	r.mu.Unlock()
	if changed && ok {
		r.persistRuntimeLoopHealthSnapshotWithLog(snapshot)
	}
}

func (r *Runtime) recordTaskCycleStart(task WorkspaceTaskRecord, at time.Time) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.LastTaskStartedAt = at
	if r.health.LastTaskProgressAt.IsZero() {
		r.health.LastTaskProgressAt = at
	}
	if summary := firstNonEmpty(task.Title, task.Description, task.TaskID); summary != "" {
		r.health.LastTaskSummary = summary
	}
}

func (r *Runtime) recordTaskProgress(summary string, at time.Time) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.LastTaskProgressAt = at
	if strings.TrimSpace(summary) != "" {
		r.health.LastTaskSummary = strings.TrimSpace(summary)
	}
}

// recordTaskCycleProgress refreshes the no-progress timer for a completed task
// cycle, but treats repeated identical blocked/failed cycles as churn (CA-07): a
// bare blocked/failed outcome with no other durable evidence advances the timer
// only when its blocker/summary signature differs from the previous cycle. A stuck
// agent emitting the same blocked/failed result every cycle therefore stops
// refreshing LastTaskProgressAt and eventually trips the no_progress detector,
// while a genuinely progressing agent (new blocker each cycle, materialized doc,
// memory write, durable tool receipt, or completion) keeps the timer fresh.
func (r *Runtime) recordTaskCycleProgress(result StructuredTaskResult, trace *TaskRunTrace, repairInfo *structuredOutputRepairInfo, at time.Time) {
	outcome := normalizeOutcome(result.Outcome)
	force := outcome == "completed" || taskCycleHasNonOutcomeDurableProgress(result, trace, repairInfo)
	if !force && outcome != "blocked" && outcome != "failed" {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	signature := taskCycleProgressSignature(result)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !force && signature != "" && signature == r.health.LastTaskProgressSig {
		// Same blocked/failed outcome as the previous cycle with no fresh durable
		// evidence: this is churn, not progress. Leave LastTaskProgressAt frozen.
		return
	}
	r.health.LastTaskProgressAt = at
	r.health.LastTaskProgressSig = signature
	if summary := strings.TrimSpace(result.Summary); summary != "" {
		r.health.LastTaskSummary = summary
	}
}

func (r *Runtime) runWatchdogLoop(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.WatchdogEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			now := time.Now().UTC()
			// CA-27: watchdog loop health is recorded inside handleWatchdogTick, gated
			// on whether the tick's critical operations actually succeeded, instead of
			// being stamped healthy unconditionally here.
			r.handleWatchdogTick(ctx, now)
		}
	}
}

func (r *Runtime) handleWatchdogTick(ctx context.Context, now time.Time) RuntimeWatchdogSnapshot {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	// CA-27: track whether the tick's critical self-liveness operation (the watchdog's
	// own server epoch heartbeat) succeeded, and record the watchdog loop's health
	// accordingly instead of always self-reporting healthy. A watchdog whose critical
	// step keeps failing now surfaces as a degraded Watchdog lane that the verdict
	// consumes on the next tick.
	var criticalErr error
	defer func() {
		if criticalErr != nil && ctx.Err() == nil {
			r.recordLoopFailure("watchdog", criticalErr, now)
		} else {
			r.recordLoopSuccess("watchdog", now)
		}
	}()

	if svc := r.memoryService(); svc != nil {
		if err := svc.PruneExpiredAndCold(DefaultLocalMemoryTTLConfig()); err != nil && ctx.Err() == nil {
			log.Printf("[watchdog] memory prune failed: %v", err)
		}
	}

	if err := r.client.PostEpochTick(ctx, r.cfg.WorkspaceID); err != nil && ctx.Err() == nil {
		log.Printf("[watchdog] policy epoch tick failed: %v", err)
		criticalErr = fmt.Errorf("policy epoch tick failed: %w", err)
	}

	snapshot := r.watchdogSnapshotWithContext(ctx, now)
	if snapshot.MonitorVerdict == "healthy" {
		r.recordWatchdogTick(snapshot, now)
		return snapshot
	}

	if snapshot.ListenerState == "degraded" {
		if err := r.reconcileLocalInbox(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[watchdog] inbox reconcile failed: %v", err)
		}
	}
	if snapshot.Memory.State == "degraded" {
		if err := r.actuateMemoryHealth(ctx, snapshot.Memory, now); err != nil && ctx.Err() == nil {
			log.Printf("[watchdog] memory repair failed: %v", err)
		}
	}
	if snapshot.ListenerState == "degraded" || snapshot.RequestState == "degraded" || snapshot.PlannerState == "degraded" || snapshot.MonitorVerdict == "stalled" {
		r.invalidateBootstrap()
		if err := r.refreshBootstrap(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[watchdog] bootstrap refresh failed: %v", err)
		}
	}

	if r.shouldPublishWatchdog(snapshot, now) {
		if err := r.publishWatchdogUpdate(ctx, snapshot); err != nil && ctx.Err() == nil {
			log.Printf("[watchdog] publish failed: %v", err)
		} else {
			r.recordWatchdogPublished(snapshot, now)
		}
	} else {
		r.recordWatchdogTick(snapshot, now)
	}
	return snapshot
}

func (r *Runtime) safeWatchdogSnapshot(now time.Time) (RuntimeWatchdogSnapshot, bool) {
	ch := make(chan RuntimeWatchdogSnapshot, 1)
	go func() {
		ch <- r.watchdogSnapshot(now)
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case snap := <-ch:
		return snap, true
	case <-timer.C:
		return RuntimeWatchdogSnapshot{}, false
	}
}

func (r *Runtime) watchdogSnapshot(now time.Time) RuntimeWatchdogSnapshot {
	return r.watchdogSnapshotCore(context.Background(), now, false)
}

func (r *Runtime) watchdogSnapshotWithContext(ctx context.Context, now time.Time) RuntimeWatchdogSnapshot {
	return r.watchdogSnapshotCore(ctx, now, true)
}

func (r *Runtime) watchdogSnapshotCore(ctx context.Context, now time.Time, includeDurableReadback bool) RuntimeWatchdogSnapshot {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	r.mu.Lock()
	health := r.health
	activeTaskID := ""
	activeSessionID := ""
	activeRunID := ""
	if r.activeTask != nil {
		activeTaskID = strings.TrimSpace(r.activeTask.TaskID)
	}
	if r.activeSession != nil {
		activeSessionID = strings.TrimSpace(r.activeSession.SessionID)
	}
	activeRunID = strings.TrimSpace(r.activeRunID)
	r.mu.Unlock()

	inboxStats := MessageInboxStats{}
	if inbox := r.messageInbox(); inbox != nil {
		inboxStats = inbox.Stats()
	}

	heartbeatState := evaluateLoopFailureState(health.Heartbeat)
	if heartbeatState == "healthy" {
		heartbeatState = evaluateLoopState(health.Heartbeat, health.StartedAt, now, r.loopHealthStaleAfter("heartbeat"))
	}
	internalHeartbeatState := evaluateLoopFailureState(health.InternalHeartbeat)
	ambientAutonomyState := evaluateLoopFailureState(health.AmbientAutonomy)
	listenerState := evaluateLoopState(health.Listener, health.StartedAt, now, r.cfg.ListenerStaleAfter)
	sseEventLoopState := evaluateLoopFailureState(health.SSEEventLoop)
	requestState := evaluateLoopState(health.Requests, health.StartedAt, now, r.cfg.RequestStaleAfter)
	plannerState := evaluateLoopState(health.Planner, health.StartedAt, now, r.cfg.PlannerStaleAfter)
	plannerTimeoutState := evaluateLoopFailureState(health.PlannerTimeout)
	watchdogLoopState := evaluateLoopFailureState(health.Watchdog)
	memoryState := r.memoryHealthSnapshot(now)
	durableReadback := TaskCycleDurablePhaseReadback{}
	durableReadbackState := ""
	durableReadbackErr := ""
	if includeDurableReadback {
		durableReadback, durableReadbackState, durableReadbackErr = r.watchdogDurableTaskReadback(ctx, activeRunID, activeTaskID)
		// CA-29: the readback RPC runs without the lock held, so the executor can
		// clear/rotate the active run while it is in flight. Re-validate that the
		// snapshot's run is still the live one before trusting the durable data;
		// otherwise we would judge a terminal/cleared run as the live task's
		// progress (false stall) or surface its readback error as degraded.
		r.mu.Lock()
		currentRunID := strings.TrimSpace(r.activeRunID)
		currentTaskID := ""
		if r.activeTask != nil {
			currentTaskID = strings.TrimSpace(r.activeTask.TaskID)
		}
		r.mu.Unlock()
		if currentRunID != activeRunID || currentTaskID != activeTaskID {
			durableReadback = TaskCycleDurablePhaseReadback{}
			durableReadbackState = ""
			durableReadbackErr = ""
		}
	}
	durableProgressAt := parseTaskCycleReadbackTime(durableReadback.SourceStepAt)

	verdict := "healthy"
	reason := ""
	recommendedAction := "continue"

	lastProgress := health.LastTaskProgressAt
	if activeTaskID != "" {
		if lastProgress.IsZero() {
			lastProgress = health.LastTaskStartedAt
		}
		if !durableProgressAt.IsZero() && (lastProgress.IsZero() || durableProgressAt.After(lastProgress)) {
			lastProgress = durableProgressAt
		}
		if !lastProgress.IsZero() && now.Sub(lastProgress) >= r.cfg.TaskStallAfter {
			verdict = "stalled"
			durablePhase := strings.TrimSpace(durableReadback.PhaseName)
			if durablePhase != "" {
				reason = fmt.Sprintf("active task %s made no durable progress since durable phase %s for %s", activeTaskID, durablePhase, now.Sub(lastProgress).Round(time.Second))
			} else {
				reason = fmt.Sprintf("active task %s made no durable progress for %s", activeTaskID, now.Sub(lastProgress).Round(time.Second))
			}
			recommendedAction = "summarize_and_replan"
		}
		// CT-03: independent durable-phase stall. The max(...) above lets a fresh
		// live progress timestamp (a durable-enough event such as a failTaskCycle
		// block, or any cycle the durable predicate accepts) mask a durable task-cycle
		// *phase* that has stopped advancing. When the durable readback phase itself is
		// older than DurableStallAfter, declare a stall regardless of LastTaskProgressAt.
		if verdict == "healthy" && !durableProgressAt.IsZero() && now.Sub(durableProgressAt) >= r.cfg.DurableStallAfter {
			verdict = "stalled"
			durablePhase := strings.TrimSpace(durableReadback.PhaseName)
			if durablePhase != "" {
				reason = fmt.Sprintf("active task %s durable phase %s has not advanced for %s", activeTaskID, durablePhase, now.Sub(durableProgressAt).Round(time.Second))
			} else {
				reason = fmt.Sprintf("active task %s durable phase has not advanced for %s", activeTaskID, now.Sub(durableProgressAt).Round(time.Second))
			}
			recommendedAction = "summarize_and_replan"
		}
	}

	if verdict == "healthy" && durableReadbackState == "error" {
		verdict = "degraded"
		reason = "durable progress readback failed: " + durableReadbackErr
		recommendedAction = "refresh_bootstrap"
	}

	if verdict == "healthy" && (heartbeatState != "healthy" || internalHeartbeatState != "healthy" || ambientAutonomyState != "healthy" || listenerState != "healthy" || sseEventLoopState != "healthy" || requestState != "healthy" || plannerState != "healthy" || plannerTimeoutState != "healthy" || watchdogLoopState != "healthy") {
		verdict = "degraded"
		reason = firstNonEmpty(
			degradedReason("heartbeat", heartbeatState),
			degradedReason("internal heartbeat", internalHeartbeatState),
			degradedReason("ambient autonomy", ambientAutonomyState),
			degradedReason("listener", listenerState),
			degradedReason("sse event", sseEventLoopState),
			degradedReason("request", requestState),
			degradedReason("planner", plannerState),
			degradedReason("planner timeout", plannerTimeoutState),
			degradedReason("watchdog", watchdogLoopState),
		)
		recommendedAction = "refresh_bootstrap"
	}

	if verdict == "healthy" && inboxStats.Unacked > 0 && inboxStats.Pending == 0 {
		verdict = "degraded"
		reason = fmt.Sprintf("%d handled messages still await remote ack", inboxStats.Unacked)
		recommendedAction = "retry_ack"
	}
	if verdict == "healthy" && memoryState.State == "degraded" {
		verdict = "degraded"
		reason = firstNonEmpty(memoryState.Reason, "memory control degraded")
		recommendedAction = firstNonEmpty(memoryState.RecommendedAction, "rebuild_memory")
	}

	return RuntimeWatchdogSnapshot{
		MonitorVerdict:            verdict,
		Reason:                    reason,
		RecommendedAction:         recommendedAction,
		ListenerState:             listenerState,
		HeartbeatState:            heartbeatState,
		InternalHeartbeatState:    internalHeartbeatState,
		AmbientAutonomyState:      ambientAutonomyState,
		SSEEventLoopState:         sseEventLoopState,
		RequestState:              requestState,
		PlannerState:              plannerState,
		PlannerTimeoutState:       plannerTimeoutState,
		Memory:                    memoryState,
		HeartbeatFailures:         health.Heartbeat.ConsecutiveFailures,
		InternalHeartbeatFailures: health.InternalHeartbeat.ConsecutiveFailures,
		AmbientAutonomyFailures:   health.AmbientAutonomy.ConsecutiveFailures,
		ListenerFailures:          health.Listener.ConsecutiveFailures,
		SSEEventLoopFailures:      health.SSEEventLoop.ConsecutiveFailures,
		RequestFailures:           health.Requests.ConsecutiveFailures,
		PlannerFailures:           health.Planner.ConsecutiveFailures,
		PlannerTimeoutFailures:    health.PlannerTimeout.ConsecutiveFailures,
		CurrentTaskID:             activeTaskID,
		CurrentSessionID:          activeSessionID,
		CurrentRunID:              activeRunID,
		LastTaskSummary:           strings.TrimSpace(health.LastTaskSummary),
		LastDurablePhase:          strings.TrimSpace(durableReadback.PhaseName),
		LastDurableStep:           strings.TrimSpace(durableReadback.LastDurableStep),
		LastDurableStepID:         strings.TrimSpace(durableReadback.SourceStepID),
		LastDurableAt:             formatWatchdogTime(durableProgressAt),
		DurableReadback:           durableReadbackState,
		LastListenerSuccess:       formatWatchdogTime(health.Listener.LastSuccessAt),
		LastRequestSuccess:        formatWatchdogTime(health.Requests.LastSuccessAt),
		LastPlannerSuccess:        formatWatchdogTime(health.Planner.LastSuccessAt),
		LastTaskStartedAt:         formatWatchdogTime(health.LastTaskStartedAt),
		LastTaskProgressAt:        formatWatchdogTime(lastProgress),
		PendingMessages:           inboxStats.Pending,
		UnreadMessages:            inboxStats.Unread,
		UnackedMessages:           inboxStats.Unacked,
		NeedsAttention:            verdict != "healthy",
		RSP: deriveRuntimeRSPState(r.cfg, RuntimeWatchdogSnapshot{
			MonitorVerdict:    verdict,
			Reason:            reason,
			RecommendedAction: recommendedAction,
			ListenerState:     listenerState,
			RequestState:      requestState,
			PlannerState:      plannerState,
			Memory:            memoryState,
			CurrentTaskID:     activeTaskID,
			CurrentSessionID:  activeSessionID,
			CurrentRunID:      activeRunID,
			PendingMessages:   inboxStats.Pending,
			UnreadMessages:    inboxStats.Unread,
			UnackedMessages:   inboxStats.Unacked,
			NeedsAttention:    verdict != "healthy",
		}, now),
	}
}

func (r *Runtime) memoryHealthSnapshot(now time.Time) RuntimeMemoryHealthSnapshot {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	control := r.memoryControlSnapshot()
	snapshot := RuntimeMemoryHealthSnapshot{
		State:                  "healthy",
		RecommendedAction:      "continue",
		PacketCacheEntries:     control.PacketCacheEntries,
		StaleDigests:           control.Store.StaleDigests,
		PendingPromotions:      control.Stats.PromotionQueue,
		PromotionFailures:      control.Stats.PromotionFailures,
		ConsecutiveStaleReads:  control.Stats.ConsecutiveStaleReads,
		ConsecutiveP2Misses:    control.Stats.ConsecutiveP2Misses,
		LastStaleHitAt:         strings.TrimSpace(control.Stats.LastStaleHitAt),
		LastP2HitAt:            strings.TrimSpace(control.Stats.LastP2HitAt),
		LastP2MissAt:           strings.TrimSpace(control.Stats.LastP2MissAt),
		LastPacketBuiltAt:      strings.TrimSpace(control.Stats.LastPacketBuiltAt),
		LastPromotionQueuedAt:  strings.TrimSpace(control.Stats.LastPromotionQueuedAt),
		LastPromotionAttemptAt: strings.TrimSpace(control.Stats.LastPromotionAttemptAt),
		LastPromotionSyncedAt:  strings.TrimSpace(control.Stats.LastPromotionSyncedAt),
	}
	if control.Stats.TotalEvents == 0 && control.Store.Episodes == 0 && control.Store.Digests == 0 && control.Stats.PromotionQueue == 0 {
		return snapshot
	}

	if control.Stats.PromotionQueue > 0 {
		oldestQueuedAt := oldestPendingPromotionAt(control.PendingPromotions)
		lastPromotionActivity := newestNonZeroTime(
			parseLocalMemoryTimestamp(control.Stats.LastPromotionAttemptAt),
			parseLocalMemoryTimestamp(control.Stats.LastPromotionSyncedAt),
			oldestQueuedAt,
		)
		if !lastPromotionActivity.IsZero() && now.Sub(lastPromotionActivity) >= r.cfg.MemoryPromotionStaleAfter {
			snapshot.State = "degraded"
			snapshot.Reason = fmt.Sprintf("%d pending memory promotions have not synced for %s", control.Stats.PromotionQueue, now.Sub(lastPromotionActivity).Round(time.Second))
			snapshot.RecommendedAction = "flush_promotions"
			return snapshot
		}
	}

	lastStaleHitAt := parseLocalMemoryTimestamp(control.Stats.LastStaleHitAt)
	if control.Store.StaleDigests > 0 &&
		control.Stats.ConsecutiveStaleReads >= r.cfg.MemoryStalePacketThreshold &&
		!lastStaleHitAt.IsZero() &&
		now.Sub(lastStaleHitAt) <= maxDuration(r.cfg.WatchdogEvery*3, 6*time.Minute) {
		snapshot.State = "degraded"
		snapshot.Reason = fmt.Sprintf(
			"memory packet hit stale digests %d consecutive times across %d stale entries",
			control.Stats.ConsecutiveStaleReads,
			control.Store.StaleDigests,
		)
		snapshot.RecommendedAction = "rebuild_memory"
	}
	return snapshot
}

func (r *Runtime) watchdogDurableTaskReadback(ctx context.Context, runID, taskID string) (TaskCycleDurablePhaseReadback, string, string) {
	runID = strings.TrimSpace(runID)
	taskID = strings.TrimSpace(taskID)
	if r == nil || r.client == nil || runID == "" || taskID == "" {
		return TaskCycleDurablePhaseReadback{}, "", ""
	}
	if ctx == nil {
		ctx = context.Background()
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	readback, ok, err := r.readLastDurableTaskCyclePhase(readCtx, runID, taskID)
	if err != nil {
		return TaskCycleDurablePhaseReadback{}, "error", strings.TrimSpace(err.Error())
	}
	if !ok {
		return TaskCycleDurablePhaseReadback{}, "missing", ""
	}
	return readback, "ok", ""
}

func parseTaskCycleReadbackTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func (r *Runtime) watchdogSummary(now time.Time) string {
	snapshot := r.watchdogSnapshot(now)
	if snapshot.MonitorVerdict == "healthy" {
		return ""
	}
	parts := []string{
		"watchdog",
		"rsp=" + string(snapshot.RSP.RolloutPhase),
		"verdict=" + snapshot.MonitorVerdict,
		"listener=" + snapshot.ListenerState,
		"requests=" + snapshot.RequestState,
		"planner=" + snapshot.PlannerState,
	}
	if snapshot.Memory.State != "" && snapshot.Memory.State != "healthy" {
		parts = append(parts, "memory="+snapshot.Memory.State)
	}
	parts = append(parts, "action="+snapshot.RecommendedAction)
	return strings.Join(parts, " ")
}

func oldestPendingPromotionAt(items []LocalMemoryPromotionSummary) time.Time {
	var oldest time.Time
	for _, item := range items {
		createdAt := parseLocalMemoryTimestamp(item.CreatedAt)
		if createdAt.IsZero() {
			continue
		}
		if oldest.IsZero() || createdAt.Before(oldest) {
			oldest = createdAt
		}
	}
	return oldest
}

func newestNonZeroTime(values ...time.Time) time.Time {
	var newest time.Time
	for _, value := range values {
		if value.IsZero() {
			continue
		}
		if newest.IsZero() || value.After(newest) {
			newest = value
		}
	}
	return newest
}

func (r *Runtime) shouldPublishWatchdog(snapshot RuntimeWatchdogSnapshot, now time.Time) bool {
	if snapshot.MonitorVerdict == "healthy" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cooldown := maxDuration(r.cfg.WatchdogEvery*2, 3*time.Minute)
	if r.health.LastWatchdogPublished != snapshot.MonitorVerdict {
		return true
	}
	if !r.health.LastWatchdogPublishAt.IsZero() && now.Sub(r.health.LastWatchdogPublishAt) < cooldown {
		return false
	}
	if strings.TrimSpace(r.health.LastWatchdogReason) != strings.TrimSpace(snapshot.Reason) {
		return true
	}
	return true
}

func (r *Runtime) shouldActuateMemory(snapshot RuntimeMemoryHealthSnapshot, now time.Time) bool {
	if snapshot.State != "degraded" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.health.LastMemoryRepairAt.IsZero() && now.Sub(r.health.LastMemoryRepairAt) < r.cfg.MemoryRepairCooldown {
		sameAction := strings.TrimSpace(r.health.LastMemoryRepairAction) == strings.TrimSpace(snapshot.RecommendedAction)
		sameReason := strings.TrimSpace(r.health.LastMemoryRepairReason) == strings.TrimSpace(snapshot.Reason)
		if sameAction && sameReason {
			return false
		}
	}
	return true
}

func (r *Runtime) actuateMemoryHealth(ctx context.Context, snapshot RuntimeMemoryHealthSnapshot, now time.Time) error {
	if snapshot.State != "degraded" {
		return nil
	}
	if !r.shouldActuateMemory(snapshot, now) {
		return nil
	}
	r.recordMemoryRepair(snapshot, now)
	switch strings.ToLower(strings.TrimSpace(snapshot.RecommendedAction)) {
	case "flush_promotions":
		return r.flushPendingMemoryPromotions(ctx, r.cfg.MaxPromotionSyncBatch)
	case "rebuild_memory":
		service := r.memoryService()
		if service == nil {
			return nil
		}
		return service.forceRebuild()
	default:
		return nil
	}
}

func (r *Runtime) publishWatchdogUpdate(ctx context.Context, snapshot RuntimeWatchdogSnapshot) error {
	payload := map[string]any{
		"monitor_verdict":    snapshot.MonitorVerdict,
		"reason":             snapshot.Reason,
		"recommended_action": snapshot.RecommendedAction,
		"listener_state":     snapshot.ListenerState,
		"request_state":      snapshot.RequestState,
		"planner_state":      snapshot.PlannerState,
		"listener_failures":  snapshot.ListenerFailures,
		"request_failures":   snapshot.RequestFailures,
		"planner_failures":   snapshot.PlannerFailures,
		"current_task_id":    snapshot.CurrentTaskID,
		"current_session_id": snapshot.CurrentSessionID,
		"current_run_id":     snapshot.CurrentRunID,
		"durable_readback":   snapshot.DurableReadback,
		"last_durable_phase": snapshot.LastDurablePhase,
		"last_durable_step":  snapshot.LastDurableStep,
		"last_durable_at":    snapshot.LastDurableAt,
		"pending_messages":   snapshot.PendingMessages,
		"unread_messages":    snapshot.UnreadMessages,
		"unacked_messages":   snapshot.UnackedMessages,
	}
	if snapshot.Memory.State != "" {
		payload["memory"] = snapshot.Memory
	}
	if snapshot.RSP.RolloutPhase != "" {
		payload["rsp"] = snapshot.RSP
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "issue",
		Summary:     "Watchdog: " + firstNonEmpty(snapshot.Reason, snapshot.MonitorVerdict),
		PayloadJSON: string(raw),
	})
}

func (r *Runtime) recordWatchdogPublished(snapshot RuntimeWatchdogSnapshot, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.LastWatchdogAt = now
	r.health.LastWatchdogPublishAt = now
	r.health.LastWatchdogPublished = snapshot.MonitorVerdict
	r.health.LastWatchdogReason = strings.TrimSpace(snapshot.Reason)
}

func (r *Runtime) recordWatchdogTick(snapshot RuntimeWatchdogSnapshot, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.LastWatchdogAt = now
	if snapshot.MonitorVerdict == "healthy" {
		r.health.LastWatchdogPublished = ""
		r.health.LastWatchdogReason = ""
	}
}

func (r *Runtime) recordMemoryRepair(snapshot RuntimeMemoryHealthSnapshot, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.LastMemoryRepairAt = now
	r.health.LastMemoryRepairAction = strings.TrimSpace(snapshot.RecommendedAction)
	r.health.LastMemoryRepairReason = strings.TrimSpace(snapshot.Reason)
}

func recordLoopStartedLocked(state *runtimeHealthState, scope string, at time.Time) bool {
	loop := selectLoopHealth(state, scope)
	if loop == nil {
		return false
	}
	loop.LastStartedAt = at
	return true
}

func recordLoopSuccessLocked(state *runtimeHealthState, scope string, at time.Time) bool {
	loop := selectLoopHealth(state, scope)
	if loop == nil {
		return false
	}
	if loop.LastStartedAt.IsZero() {
		loop.LastStartedAt = at
	}
	loop.LastAttemptAt = at
	loop.LastSuccessAt = at
	loop.ConsecutiveFailures = 0
	loop.LastError = ""
	return true
}

func recordLoopFailureLocked(state *runtimeHealthState, scope string, err error, at time.Time) bool {
	loop := selectLoopHealth(state, scope)
	if loop == nil {
		return false
	}
	if loop.LastStartedAt.IsZero() {
		loop.LastStartedAt = at
	}
	loop.LastAttemptAt = at
	loop.LastFailureAt = at
	loop.ConsecutiveFailures++
	if err != nil {
		loop.LastError = strings.TrimSpace(err.Error())
	}
	return true
}

func selectLoopHealth(state *runtimeHealthState, scope string) *loopHealth {
	if state == nil {
		return nil
	}
	switch canonicalLoopHealthScope(scope) {
	case "heartbeat":
		return &state.Heartbeat
	case "internal_heartbeat":
		return &state.InternalHeartbeat
	case "ambient_autonomy":
		return &state.AmbientAutonomy
	case "message_poll":
		return &state.Listener
	case "sse_event_loop":
		return &state.SSEEventLoop
	case "request_poll":
		return &state.Requests
	case "planner":
		return &state.Planner
	case "planner_timeout":
		return &state.PlannerTimeout
	case "watchdog":
		return &state.Watchdog
	case "scratch_replay":
		return &state.ScratchReplay
	case "memory_sync":
		return &state.MemorySync
	default:
		return nil
	}
}

func evaluateLoopState(loop loopHealth, startedAt, now time.Time, staleAfter time.Duration) string {
	if staleAfter <= 0 {
		staleAfter = 2 * time.Minute
	}
	if loop.ConsecutiveFailures >= 3 {
		return "degraded"
	}
	reference := loop.LastSuccessAt
	if reference.IsZero() {
		reference = startedAt
	}
	if reference.IsZero() {
		return "healthy"
	}
	if now.Sub(reference) >= staleAfter {
		return "degraded"
	}
	return "healthy"
}

func evaluateLoopFailureState(loop loopHealth) string {
	if loop.ConsecutiveFailures > 0 && (loop.LastSuccessAt.IsZero() || loop.LastFailureAt.After(loop.LastSuccessAt)) {
		return "degraded"
	}
	return "healthy"
}

func degradedReason(scope, state string) string {
	if strings.TrimSpace(state) == "degraded" {
		return scope + " loop degraded"
	}
	return ""
}

func formatWatchdogTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
