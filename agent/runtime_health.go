package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

const runtimeLoopHealthStateVersion = 1

var runtimeLoopHealthNames = []string{
	"heartbeat",
	"internal_heartbeat",
	"ambient_autonomy",
	"message_poll",
	"sse_event_loop",
	"request_poll",
	"planner",
	"planner_timeout",
	"watchdog",
	"scratch_replay",
	"memory_sync",
}

type RuntimeLoopHealthSnapshot struct {
	Version            int                               `json:"version"`
	WorkspaceID        string                            `json:"workspace_id"`
	AgentID            string                            `json:"agent_id"`
	StartedAt          string                            `json:"started_at,omitempty"`
	ObservedAt         string                            `json:"observed_at,omitempty"`
	PreviousStartedAt  string                            `json:"previous_started_at,omitempty"`
	PreviousObservedAt string                            `json:"previous_observed_at,omitempty"`
	RestartCount       int                               `json:"restart_count,omitempty"`
	ActiveTaskID       string                            `json:"active_task_id,omitempty"`
	ActiveSessionID    string                            `json:"active_session_id,omitempty"`
	ActiveRunID        string                            `json:"active_run_id,omitempty"`
	Loops              map[string]RuntimeLoopHealthEntry `json:"loops"`
}

type RuntimeLoopHealthEntry struct {
	State               string `json:"state"`
	LastStartedAt       string `json:"last_started_at,omitempty"`
	LastAttemptAt       string `json:"last_attempt_at,omitempty"`
	LastSuccessAt       string `json:"last_success_at,omitempty"`
	LastFailureAt       string `json:"last_failure_at,omitempty"`
	ConsecutiveFailures int    `json:"consecutive_failures,omitempty"`
	LastError           string `json:"last_error,omitempty"`
}

func newRuntimeHealthStateFromPrevious(startedAt time.Time, previous RuntimeLoopHealthSnapshot) runtimeHealthState {
	state := newRuntimeHealthState(startedAt)
	state.PreviousStartedAt = parseRuntimeLoopHealthTime(previous.StartedAt)
	state.PreviousObservedAt = parseRuntimeLoopHealthTime(previous.ObservedAt)
	state.RestartCount = previous.RestartCount
	if state.RestartCount < 0 {
		state.RestartCount = 0
	}
	if !state.PreviousStartedAt.IsZero() || !state.PreviousObservedAt.IsZero() || state.RestartCount > 0 {
		state.RestartCount++
	}
	return state
}

func runtimeLoopHealthPath(workspaceID, agentID string) string {
	workspacePart := sanitizePathComponent(firstNonEmpty(workspaceID, "workspace"))
	agentPart := sanitizePathComponent(firstNonEmpty(agentID, "agent"))
	return agentRuntimeConfigPath("runtime-health", workspacePart, agentPart+".json")
}

func loadRuntimeLoopHealthSnapshot(workspaceID, agentID string) (RuntimeLoopHealthSnapshot, error) {
	path := runtimeLoopHealthPath(workspaceID, agentID)
	if strings.TrimSpace(path) == "" {
		return RuntimeLoopHealthSnapshot{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return RuntimeLoopHealthSnapshot{}, nil
		}
		return RuntimeLoopHealthSnapshot{}, err
	}
	if strings.TrimSpace(string(data)) == "" {
		return RuntimeLoopHealthSnapshot{}, nil
	}
	var snapshot RuntimeLoopHealthSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return RuntimeLoopHealthSnapshot{}, fmt.Errorf("decode runtime loop health: %w", err)
	}
	if snapshot.Loops == nil {
		snapshot.Loops = map[string]RuntimeLoopHealthEntry{}
	}
	return snapshot, nil
}

func (r *Runtime) runtimeLoopHealthSnapshot(now time.Time) RuntimeLoopHealthSnapshot {
	snapshot, _ := r.currentRuntimeLoopHealthSnapshot(now)
	return snapshot
}

func (r *Runtime) currentRuntimeLoopHealthSnapshot(now time.Time) (RuntimeLoopHealthSnapshot, bool) {
	if r == nil {
		return RuntimeLoopHealthSnapshot{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runtimeLoopHealthSnapshotLocked(now)
}

func (r *Runtime) runtimeLoopHealthSnapshotLocked(now time.Time) (RuntimeLoopHealthSnapshot, bool) {
	if r == nil {
		return RuntimeLoopHealthSnapshot{}, false
	}
	workspaceID := strings.TrimSpace(r.cfg.WorkspaceID)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	if workspaceID == "" || agentID == "" {
		return RuntimeLoopHealthSnapshot{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	activeTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	activeSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	activeRunID := strings.TrimSpace(r.scratch.ActiveRunID)
	if r.activeTask != nil {
		activeTaskID = firstNonEmpty(strings.TrimSpace(r.activeTask.TaskID), activeTaskID)
	}
	if r.activeSession != nil {
		activeSessionID = firstNonEmpty(strings.TrimSpace(r.activeSession.SessionID), activeSessionID)
		activeTaskID = firstNonEmpty(strings.TrimSpace(r.activeSession.TaskID), activeTaskID)
	}
	activeRunID = firstNonEmpty(strings.TrimSpace(r.activeRunID), activeRunID)

	loops := make(map[string]RuntimeLoopHealthEntry, len(runtimeLoopHealthNames))
	for _, name := range runtimeLoopHealthNames {
		if loop := selectLoopHealth(&r.health, name); loop != nil {
			loops[name] = runtimeLoopHealthEntry(*loop, runtimeLoopSnapshotState(name, *loop, r.health.StartedAt, now, r.loopHealthStaleAfter(name)))
		}
	}

	return RuntimeLoopHealthSnapshot{
		Version:            runtimeLoopHealthStateVersion,
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		StartedAt:          formatRuntimeLoopHealthTime(r.health.StartedAt),
		ObservedAt:         formatRuntimeLoopHealthTime(now),
		PreviousStartedAt:  formatRuntimeLoopHealthTime(r.health.PreviousStartedAt),
		PreviousObservedAt: formatRuntimeLoopHealthTime(r.health.PreviousObservedAt),
		RestartCount:       r.health.RestartCount,
		ActiveTaskID:       activeTaskID,
		ActiveSessionID:    activeSessionID,
		ActiveRunID:        activeRunID,
		Loops:              loops,
	}, true
}

func runtimeLoopHealthEntry(loop loopHealth, state string) RuntimeLoopHealthEntry {
	return RuntimeLoopHealthEntry{
		State:               state,
		LastStartedAt:       formatRuntimeLoopHealthTime(loop.LastStartedAt),
		LastAttemptAt:       formatRuntimeLoopHealthTime(loop.LastAttemptAt),
		LastSuccessAt:       formatRuntimeLoopHealthTime(loop.LastSuccessAt),
		LastFailureAt:       formatRuntimeLoopHealthTime(loop.LastFailureAt),
		ConsecutiveFailures: loop.ConsecutiveFailures,
		LastError:           strings.TrimSpace(loop.LastError),
	}
}

func (r *Runtime) persistCurrentLoopHealthSnapshot(now time.Time) {
	snapshot, ok := r.currentRuntimeLoopHealthSnapshot(now)
	if !ok {
		return
	}
	r.persistRuntimeLoopHealthSnapshotWithLog(snapshot)
}

func (r *Runtime) persistRuntimeLoopHealthSnapshotWithLog(snapshot RuntimeLoopHealthSnapshot) {
	if err := persistRuntimeLoopHealthSnapshot(snapshot); err != nil {
		log.Printf("[runtime] warning: could not persist loop health: %v", err)
	}
}

func persistRuntimeLoopHealthSnapshot(snapshot RuntimeLoopHealthSnapshot) error {
	if strings.TrimSpace(snapshot.WorkspaceID) == "" || strings.TrimSpace(snapshot.AgentID) == "" {
		return nil
	}
	path := runtimeLoopHealthPath(snapshot.WorkspaceID, snapshot.AgentID)
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if snapshot.Version == 0 {
		snapshot.Version = runtimeLoopHealthStateVersion
	}
	if snapshot.Loops == nil {
		snapshot.Loops = map[string]RuntimeLoopHealthEntry{}
	}
	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode runtime loop health: %w", err)
	}
	raw = append(raw, '\n')
	return atomicWriteFile(path, raw, 0o600)
}

func canonicalLoopHealthScope(scope string) string {
	scope = strings.ToLower(strings.TrimSpace(scope))
	scope = strings.ReplaceAll(scope, "-", "_")
	switch scope {
	case "heartbeat":
		return "heartbeat"
	case "internal_heartbeat", "internal-heartbeat":
		return "internal_heartbeat"
	case "ambient", "ambient_autonomy", "ambient-autonomy":
		return "ambient_autonomy"
	case "listener", "message", "messages", "message_poll":
		return "message_poll"
	case "sse", "sse_event", "sse_event_loop", "event_loop", "event-loop":
		return "sse_event_loop"
	case "request", "requests", "request_poll":
		return "request_poll"
	case "planner":
		return "planner"
	case "planner_timeout", "planner-timeout":
		return "planner_timeout"
	case "watchdog":
		return "watchdog"
	case "scratch", "scratch_replay":
		return "scratch_replay"
	case "memory", "memory_sync":
		return "memory_sync"
	default:
		return scope
	}
}

func (r *Runtime) loopHealthStaleAfter(scope string) time.Duration {
	if r == nil {
		return 0
	}
	switch canonicalLoopHealthScope(scope) {
	case "heartbeat":
		return maxDuration(r.cfg.HeartbeatEvery*3, 2*time.Minute)
	case "internal_heartbeat", "ambient_autonomy":
		return maxDuration(runtimePlannerCycleTimeout(r.cfg)*2, 2*time.Minute)
	case "message_poll":
		return r.cfg.ListenerStaleAfter
	case "sse_event_loop":
		return r.cfg.ListenerStaleAfter
	case "request_poll":
		return r.cfg.RequestStaleAfter
	case "planner":
		return r.cfg.PlannerStaleAfter
	case "planner_timeout":
		return maxDuration(runtimePlannerCycleTimeout(r.cfg)*2, 2*time.Minute)
	case "watchdog":
		return maxDuration(r.cfg.WatchdogEvery*3, 3*time.Minute)
	case "memory_sync":
		return maxDuration(r.cfg.MemorySyncEvery*4, 2*time.Minute)
	case "scratch_replay":
		return 0
	default:
		return 2 * time.Minute
	}
}

func runtimeLoopSnapshotState(scope string, loop loopHealth, startedAt, now time.Time, staleAfter time.Duration) string {
	if loop.ConsecutiveFailures > 0 && (loop.LastSuccessAt.IsZero() || loop.LastFailureAt.After(loop.LastSuccessAt)) {
		return "degraded"
	}
	if canonicalLoopHealthScope(scope) == "scratch_replay" {
		if loop.LastSuccessAt.IsZero() {
			return "starting"
		}
		return "healthy"
	}
	reference := loop.LastSuccessAt
	if reference.IsZero() {
		reference = loop.LastStartedAt
	}
	if reference.IsZero() {
		reference = startedAt
	}
	if reference.IsZero() {
		return "starting"
	}
	if staleAfter > 0 && now.Sub(reference) >= staleAfter {
		return "stale"
	}
	return "healthy"
}

func parseRuntimeLoopHealthTime(value string) time.Time {
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

func formatRuntimeLoopHealthTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func (r *Runtime) runMemorySyncHealthLoop(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.MemorySyncEvery)
	defer ticker.Stop()

	var backoff transientBackoff
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			now := time.Now().UTC()
			if err := r.recordMemorySyncHealthTick(ctx, now); err != nil {
				log.Printf("[memory] sync loop degraded: %v", err)
				if !sleepContext(ctx, backoff.Next(2*time.Second, 30*time.Second)) {
					return nil
				}
				continue
			}
			backoff.Reset()
		}
	}
}

func (r *Runtime) recordMemorySyncHealthTick(ctx context.Context, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := r.flushPendingMemoryPromotions(ctx, r.cfg.MaxPromotionSyncBatch); err != nil {
		r.recordLoopFailure("memory_sync", err, now)
		return err
	}
	r.recordLoopSuccess("memory_sync", now)
	return nil
}
