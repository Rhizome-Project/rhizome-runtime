package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var errInternalHeartbeatLeaseLost = errors.New("internal heartbeat durable lease lost")

const internalHeartbeatLeaseAcquireRPCMethod = "agent.heartbeat_lease.acquire"

type internalHeartbeatSchedulerState struct {
	LastRun       map[string]time.Time
	Running       map[string]int
	HeldLocks     map[string]int
	PublicSummary map[string]internalHeartbeatPublicSummaryState
}

type internalHeartbeatPublicSummaryState struct {
	Status      string
	Outcome     string
	PublishedAt time.Time
}

type InternalHeartbeatDueItem struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"`
	Cadence     string   `json:"cadence,omitempty"`
	Priority    int      `json:"priority"`
	Enabled     bool     `json:"enabled"`
	Due         bool     `json:"due"`
	Reason      string   `json:"reason,omitempty"`
	LastRunAt   string   `json:"last_run_at,omitempty"`
	NextDueAt   string   `json:"next_due_at,omitempty"`
	Locks       []string `json:"locks,omitempty"`
	ToolSuites  []string `json:"tool_suites,omitempty"`
	MaxParallel int      `json:"max_parallel,omitempty"`
}

type InternalHeartbeatStatusSnapshot struct {
	Schema                      string                       `json:"schema,omitempty"`
	ProfileID                   string                       `json:"profile_id,omitempty"`
	Preset                      string                       `json:"preset,omitempty"`
	Digest                      string                       `json:"digest,omitempty"`
	ExecutionMode               string                       `json:"execution_mode,omitempty"`
	RemoteLease                 InternalHeartbeatLeaseStatus `json:"remote_lease"`
	Memory                      AgentInternalSessionStats    `json:"memory,omitempty"`
	EnabledCount                int                          `json:"enabled_count"`
	DueCount                    int                          `json:"due_count"`
	MaxParallelInternalSessions int                          `json:"max_parallel_internal_sessions,omitempty"`
	MaxLLMSessions              int                          `json:"max_llm_sessions,omitempty"`
	MaxBrowserSessions          int                          `json:"max_browser_sessions,omitempty"`
	Due                         []string                     `json:"due,omitempty"`
	Heartbeats                  []InternalHeartbeatDueItem   `json:"heartbeats,omitempty"`
}

type InternalHeartbeatLeaseStatus struct {
	Available                 bool   `json:"available"`
	Checked                   bool   `json:"checked"`
	Supported                 bool   `json:"supported"`
	State                     string `json:"state"`
	Source                    string `json:"source,omitempty"`
	Method                    string `json:"method,omitempty"`
	OwnerID                   string `json:"owner_id,omitempty"`
	FailClosedOnIndeterminate bool   `json:"fail_closed_on_indeterminate"`
}

func (r *Runtime) runInternalHeartbeatLoop(ctx context.Context) error {
	interval := runtimeInternalHeartbeatPollInterval(r.cfg)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			r.recordLoopStarted("internal_heartbeat", now.UTC())
			_, _ = r.executeDueInternalHeartbeatsLeased(ctx, now.UTC())
		}
	}
}

func runtimeInternalHeartbeatPollInterval(cfg RuntimeConfig) time.Duration {
	interval := cfg.PlannerEvery
	if interval <= 0 {
		interval = defaultPlannerInterval
	}
	if interval < 15*time.Second {
		return 15 * time.Second
	}
	if interval > time.Minute {
		return time.Minute
	}
	return interval
}

func newInternalHeartbeatSchedulerState() internalHeartbeatSchedulerState {
	return internalHeartbeatSchedulerState{
		LastRun:       map[string]time.Time{},
		Running:       map[string]int{},
		HeldLocks:     map[string]int{},
		PublicSummary: map[string]internalHeartbeatPublicSummaryState{},
	}
}

func runtimeProfileForAnatomy(cfg RuntimeConfig) AgentProfile {
	profile := loadAgentProfileRaw(cfg.Workdir)
	if strings.TrimSpace(profile.AgentID) == "" {
		profile.AgentID = cfg.AgentID
	}
	if strings.TrimSpace(profile.DisplayName) == "" {
		profile.DisplayName = cfg.DisplayName
	}
	if strings.TrimSpace(profile.GroupID) == "" {
		profile.GroupID = cfg.GroupID
	}
	if strings.TrimSpace(profile.Role) == "" {
		profile.Role = cfg.Role
	}
	return normalizeAgentProfile(profile)
}

func runtimeAnatomyConfig(cfg RuntimeConfig) AgentAnatomyConfig {
	return LoadAgentAnatomyConfig(cfg.Workdir, runtimeProfileForAnatomy(cfg))
}

func internalHeartbeatDueItems(anatomy AgentAnatomyConfig, now time.Time, lastRun map[string]time.Time, activeTask bool) []InternalHeartbeatDueItem {
	state := newInternalHeartbeatSchedulerState()
	for id, at := range lastRun {
		state.LastRun[id] = at
	}
	return internalHeartbeatDueItemsWithState(anatomy, now, state, activeTask, false)
}

func internalHeartbeatDueItemsWithState(anatomy AgentAnatomyConfig, now time.Time, state internalHeartbeatSchedulerState, activeTask bool, paused bool) []InternalHeartbeatDueItem {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	specs := append([]AgentHeartbeatSpec(nil), anatomy.Heartbeats...)
	sort.SliceStable(specs, func(i, j int) bool {
		if specs[i].Priority != specs[j].Priority {
			return specs[i].Priority > specs[j].Priority
		}
		return specs[i].ID < specs[j].ID
	})
	lastRun := state.LastRun
	if lastRun == nil {
		lastRun = map[string]time.Time{}
	}
	running := state.Running
	if running == nil {
		running = map[string]int{}
	}
	heldLocks := copyHeartbeatLockSet(state.HeldLocks)
	queuedLocks := map[string]int{}
	globalLimit := anatomy.Concurrency.MaxParallelInternalSessions
	if globalLimit <= 0 {
		globalLimit = len(specs)
	}
	activeInternalSessions := 0
	for _, count := range running {
		if count > 0 {
			activeInternalSessions += count
		}
	}
	items := make([]InternalHeartbeatDueItem, 0, len(specs))
	for _, spec := range specs {
		item := internalHeartbeatDueItem(spec, now, lastRun, activeTask)
		if item.Due && paused {
			item.Due = false
			item.Reason = "runtime_paused"
		}
		if item.Due && running[spec.ID] >= spec.MaxParallel {
			item.Due = false
			item.Reason = "concurrency_limit"
		}
		if item.Due && activeInternalSessions >= globalLimit {
			item.Due = false
			item.Reason = "global_concurrency_limit"
		}
		if item.Due && heartbeatLocksOverlap(spec.Locks, heldLocks) {
			item.Due = false
			item.Reason = "lock_held"
		}
		if item.Due && heartbeatLocksOverlap(spec.Locks, queuedLocks) {
			item.Due = false
			item.Reason = "queue_lock_conflict"
		}
		if item.Due {
			addHeartbeatLocks(queuedLocks, spec.Locks)
			activeInternalSessions++
		}
		items = append(items, item)
	}
	return items
}

func internalHeartbeatDueQueue(anatomy AgentAnatomyConfig, now time.Time, lastRun map[string]time.Time, activeTask bool) []InternalHeartbeatDueItem {
	items := internalHeartbeatDueItems(anatomy, now, lastRun, activeTask)
	out := make([]InternalHeartbeatDueItem, 0, len(items))
	for _, item := range items {
		if item.Due {
			out = append(out, item)
		}
	}
	return out
}

func (r *Runtime) tryAcquireInternalHeartbeatRun(anatomy AgentAnatomyConfig, spec AgentHeartbeatSpec) bool {
	if r == nil {
		return false
	}
	spec = normalizeAgentHeartbeatSpec(spec)
	if strings.TrimSpace(spec.ID) == "" {
		return false
	}
	globalLimit := anatomy.Concurrency.MaxParallelInternalSessions
	if globalLimit <= 0 {
		globalLimit = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.internalHeartbeatState.Running == nil {
		r.internalHeartbeatState.Running = map[string]int{}
	}
	if r.internalHeartbeatState.HeldLocks == nil {
		r.internalHeartbeatState.HeldLocks = map[string]int{}
	}
	active := 0
	for _, count := range r.internalHeartbeatState.Running {
		if count > 0 {
			active += count
		}
	}
	if active >= globalLimit {
		return false
	}
	if r.internalHeartbeatState.Running[spec.ID] >= spec.MaxParallel {
		return false
	}
	if heartbeatLocksOverlap(spec.Locks, r.internalHeartbeatState.HeldLocks) {
		return false
	}
	r.internalHeartbeatState.Running[spec.ID]++
	addHeartbeatLocks(r.internalHeartbeatState.HeldLocks, spec.Locks)
	return true
}

func (r *Runtime) releaseInternalHeartbeatRun(spec AgentHeartbeatSpec) {
	if r == nil {
		return
	}
	spec = normalizeAgentHeartbeatSpec(spec)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.internalHeartbeatState.Running != nil && r.internalHeartbeatState.Running[spec.ID] > 0 {
		r.internalHeartbeatState.Running[spec.ID]--
		if r.internalHeartbeatState.Running[spec.ID] <= 0 {
			delete(r.internalHeartbeatState.Running, spec.ID)
		}
	}
	if r.internalHeartbeatState.HeldLocks != nil {
		for _, lockName := range spec.Locks {
			lockName = strings.TrimSpace(lockName)
			if lockName == "" {
				continue
			}
			if r.internalHeartbeatState.HeldLocks[lockName] > 0 {
				r.internalHeartbeatState.HeldLocks[lockName]--
			}
			if r.internalHeartbeatState.HeldLocks[lockName] <= 0 {
				delete(r.internalHeartbeatState.HeldLocks, lockName)
			}
		}
	}
}

type internalHeartbeatRunLease struct {
	Spec       AgentHeartbeatSpec
	Local      bool
	Remote     bool
	OwnerID    string
	LeaseToken string
	TTLSeconds int
}

func (r *Runtime) tryAcquireInternalHeartbeatRunLease(ctx context.Context, anatomy AgentAnatomyConfig, spec AgentHeartbeatSpec, useRemote bool) (internalHeartbeatRunLease, bool) {
	lease := internalHeartbeatRunLease{Spec: normalizeAgentHeartbeatSpec(spec)}
	if !r.tryAcquireInternalHeartbeatRun(anatomy, spec) {
		return lease, false
	}
	lease.Local = true
	if !useRemote || !r.internalHeartbeatRemoteLeaseAvailable() {
		return lease, true
	}
	ownerID := strings.TrimSpace(r.internalHeartbeatLeaseOwnerID)
	if ownerID == "" {
		ownerID = newRuntimeID("ihb-owner")
		r.mu.Lock()
		if strings.TrimSpace(r.internalHeartbeatLeaseOwnerID) == "" {
			r.internalHeartbeatLeaseOwnerID = ownerID
		} else {
			ownerID = strings.TrimSpace(r.internalHeartbeatLeaseOwnerID)
		}
		r.mu.Unlock()
	}
	lease.OwnerID = ownerID
	lease.LeaseToken = newRuntimeID("ihb-lease")
	lease.TTLSeconds = r.internalHeartbeatLeaseTTLSeconds()
	result, err := r.client.AcquireHeartbeatLease(ctx, AgentHeartbeatLeaseAcquireInput{
		WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
		AgentID:     strings.TrimSpace(r.cfg.AgentID),
		HeartbeatID: strings.TrimSpace(lease.Spec.ID),
		OwnerID:     lease.OwnerID,
		LeaseToken:  lease.LeaseToken,
		Locks:       append([]string(nil), lease.Spec.Locks...),
		TTLSec:      lease.TTLSeconds,
	})
	if err != nil {
		if heartbeatLeaseMethodUnsupported(err) {
			r.markInternalHeartbeatRemoteLeaseSupport(false)
			return lease, true
		}
		r.releaseInternalHeartbeatRun(spec)
		lease.Local = false
		r.recordLoopFailure("internal_heartbeat", err, time.Now().UTC())
		return lease, false
	}
	r.markInternalHeartbeatRemoteLeaseSupport(true)
	if !result.Acquired {
		r.releaseInternalHeartbeatRun(spec)
		lease.Local = false
		return lease, false
	}
	lease.Remote = true
	return lease, true
}

func (r *Runtime) startInternalHeartbeatLeaseRefresh(lease internalHeartbeatRunLease, cancelRun context.CancelCauseFunc) func() {
	if r == nil || !lease.Remote || r.client == nil || strings.TrimSpace(lease.LeaseToken) == "" {
		return func() {}
	}
	if cancelRun == nil {
		cancelRun = func(error) {}
	}
	ttl := time.Duration(lease.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = time.Minute
	}
	interval := ttl / 3
	if interval < time.Second {
		interval = time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refreshCtx, refreshCancel := context.WithTimeout(context.Background(), 10*time.Second)
				result, err := r.client.RefreshHeartbeatLease(refreshCtx, AgentHeartbeatLeaseAcquireInput{
					WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
					AgentID:     strings.TrimSpace(r.cfg.AgentID),
					HeartbeatID: strings.TrimSpace(lease.Spec.ID),
					OwnerID:     strings.TrimSpace(lease.OwnerID),
					LeaseToken:  strings.TrimSpace(lease.LeaseToken),
					Locks:       append([]string(nil), lease.Spec.Locks...),
					TTLSec:      lease.TTLSeconds,
				})
				refreshCancel()
				if err != nil {
					r.recordLoopFailure("internal_heartbeat", err, time.Now().UTC())
					cancelRun(err)
					return
				}
				r.markInternalHeartbeatRemoteLeaseSupport(true)
				if !result.Acquired {
					r.recordLoopFailure("internal_heartbeat", errInternalHeartbeatLeaseLost, time.Now().UTC())
					cancelRun(errInternalHeartbeatLeaseLost)
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func (r *Runtime) releaseInternalHeartbeatRunLease(parent context.Context, lease internalHeartbeatRunLease) {
	if r == nil {
		return
	}
	if lease.Remote && r.client != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if parent != nil {
			if deadline, ok := parent.Deadline(); ok {
				if until := time.Until(deadline); until > 0 && until < 10*time.Second {
					cancel()
					releaseCtx, cancel = context.WithTimeout(context.Background(), until)
				}
			}
		}
		_, _ = r.client.ReleaseHeartbeatLease(releaseCtx, AgentHeartbeatLeaseReleaseInput{
			WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
			AgentID:     strings.TrimSpace(r.cfg.AgentID),
			HeartbeatID: strings.TrimSpace(lease.Spec.ID),
			LeaseToken:  strings.TrimSpace(lease.LeaseToken),
		})
		cancel()
	}
	if lease.Local {
		r.releaseInternalHeartbeatRun(lease.Spec)
	}
}

func (r *Runtime) internalHeartbeatRemoteLeaseAvailable() bool {
	return r != nil &&
		r.client != nil &&
		strings.TrimSpace(r.client.endpoint) != "" &&
		strings.TrimSpace(r.cfg.WorkspaceID) != "" &&
		strings.TrimSpace(r.cfg.AgentID) != ""
}

func (r *Runtime) markInternalHeartbeatRemoteLeaseSupport(supported bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.internalHeartbeatLeaseChecked = true
	r.internalHeartbeatLeaseOK = supported
	r.mu.Unlock()
}

func (r *Runtime) internalHeartbeatRemoteLeaseSupport() (checked bool, supported bool) {
	if r == nil {
		return false, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.internalHeartbeatLeaseChecked, r.internalHeartbeatLeaseOK
}

func (r *Runtime) internalHeartbeatRemoteLeaseStatusSnapshot() InternalHeartbeatLeaseStatus {
	status := InternalHeartbeatLeaseStatus{
		State:                     "unavailable",
		Source:                    "local_runtime_config",
		Method:                    internalHeartbeatLeaseAcquireRPCMethod,
		FailClosedOnIndeterminate: false,
	}
	if r == nil {
		return status
	}
	status.Available = r.internalHeartbeatRemoteLeaseAvailable()
	r.mu.Lock()
	status.Checked = r.internalHeartbeatLeaseChecked
	status.Supported = r.internalHeartbeatLeaseOK
	status.OwnerID = strings.TrimSpace(r.internalHeartbeatLeaseOwnerID)
	r.mu.Unlock()
	if !status.Available {
		status.Checked = false
		status.Supported = false
		return status
	}
	status.FailClosedOnIndeterminate = true
	switch {
	case status.Checked && status.Supported:
		status.State = "supported"
		status.Source = "rpc_describe_or_successful_acquire"
	case status.Checked:
		status.State = "unsupported"
		status.Source = "rpc_describe_or_method_not_found"
	default:
		status.State = "unconfirmed"
		status.Source = "pending_rpc_describe_or_acquire_probe"
	}
	return status
}

func (r *Runtime) probeInternalHeartbeatRemoteLeaseSupport(ctx context.Context) (checked bool, supported bool, err error) {
	if r == nil {
		return false, false, nil
	}
	checked, supported = r.internalHeartbeatRemoteLeaseSupport()
	if checked {
		return checked, supported, nil
	}
	if r.cfg.Mode != RuntimeModeDaemon || !r.internalHeartbeatRemoteLeaseAvailable() {
		return false, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	schema, ok, err := r.client.DescribeRPCMethod(probeCtx, internalHeartbeatLeaseAcquireRPCMethod)
	if err != nil {
		if rpcDescribeMethodUnsupported(err) {
			r.markInternalHeartbeatRemoteLeaseSupport(false)
			return true, false, nil
		}
		return false, false, err
	}
	if !ok {
		r.markInternalHeartbeatRemoteLeaseSupport(false)
		return true, false, nil
	}
	supported = strings.EqualFold(strings.TrimSpace(schema.Method), internalHeartbeatLeaseAcquireRPCMethod)
	r.markInternalHeartbeatRemoteLeaseSupport(supported)
	return true, supported, nil
}

func (r *Runtime) shouldUseRemoteLeaseForInternalHeartbeat(policy InternalHeartbeatExecutionPolicy, mode internalHeartbeatLeaseMode) bool {
	if r == nil || policy.LocalOnly {
		return false
	}
	switch mode {
	case internalHeartbeatLeaseModeProbeRemote:
		checked, supported := r.internalHeartbeatRemoteLeaseSupport()
		if checked && !supported {
			return false
		}
		return true
	case internalHeartbeatLeaseModeConfirmedRemote:
		_, supported := r.internalHeartbeatRemoteLeaseSupport()
		return supported
	default:
		return false
	}
}

func internalHeartbeatExecutionModeFromLeaseStatus(status InternalHeartbeatLeaseStatus) string {
	if !status.Available {
		return "local_scheduled_with_locks"
	}
	switch {
	case status.Checked && status.Supported:
		return "local_scheduled_with_durable_leases"
	case status.Checked:
		return "local_scheduled_with_locks_remote_lease_unsupported"
	default:
		return "local_scheduled_with_remote_lease_unconfirmed"
	}
}

func (r *Runtime) internalHeartbeatLeaseTTLSeconds() int {
	ttl := runtimePlannerCycleTimeout(r.cfg) + 30*time.Second
	if ttl < time.Minute {
		ttl = time.Minute
	}
	if ttl > 30*time.Minute {
		ttl = 30 * time.Minute
	}
	return int(ttl.Seconds())
}

func heartbeatLeaseMethodUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "method not found") ||
		strings.Contains(message, "unknown method") ||
		strings.Contains(message, "not implemented")
}

func internalHeartbeatDueItem(spec AgentHeartbeatSpec, now time.Time, lastRun map[string]time.Time, activeTask bool) InternalHeartbeatDueItem {
	spec = normalizeAgentHeartbeatSpec(spec)
	item := InternalHeartbeatDueItem{
		ID:          spec.ID,
		Kind:        spec.Kind,
		Cadence:     spec.Cadence,
		Priority:    spec.Priority,
		Enabled:     heartbeatEnabled(spec),
		Locks:       append([]string(nil), spec.Locks...),
		ToolSuites:  append([]string(nil), spec.ToolSuites...),
		MaxParallel: spec.MaxParallel,
	}
	if !item.Enabled {
		item.Reason = "disabled"
		return item
	}
	if last, ok := lastRun[spec.ID]; ok && !last.IsZero() {
		item.LastRunAt = last.UTC().Format(time.RFC3339Nano)
	}
	if strings.TrimSpace(spec.Cadence) == heartbeatCadenceWhileClaimed {
		if activeTask {
			item.Due = true
			item.Reason = "active_task_present"
		} else {
			item.Reason = "waiting_for_claimed_task"
		}
		return item
	}
	duration, ok := heartbeatCadenceDuration(spec)
	if !ok {
		item.Reason = "invalid_cadence"
		return item
	}
	last, hasLast := lastRun[spec.ID]
	if !hasLast || last.IsZero() {
		item.Due = true
		item.Reason = "never_ran"
		item.NextDueAt = now.UTC().Format(time.RFC3339Nano)
		return item
	}
	next := last.Add(duration)
	item.NextDueAt = next.UTC().Format(time.RFC3339Nano)
	if !now.Before(next) {
		item.Due = true
		item.Reason = "cadence_elapsed"
	} else {
		item.Reason = "cooldown"
	}
	return item
}

func (r *Runtime) recordInternalHeartbeatObservation(id string, at time.Time) {
	if r == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	cfg := r.cfg
	store := r.internalSessions
	r.mu.Unlock()

	anatomy := runtimeAnatomyConfig(cfg)
	heartbeat := AgentHeartbeatSpec{ID: id, Kind: "observation"}
	for _, spec := range anatomy.Heartbeats {
		if spec.ID == id {
			heartbeat = spec
			break
		}
	}
	if store == nil {
		var err error
		store, err = OpenAgentInternalSessionStore(cfg.WorkspaceID, cfg.AgentID)
		if err != nil {
			return
		}
	}
	session, err := store.BeginHeartbeatSession(heartbeat, AgentAnatomyDigest(anatomy), "observe_only_scheduler", at)
	if err != nil {
		return
	}
	session.PromotionBlocked = true
	session.Summary = "Observe-only internal heartbeat was marked as seen."
	if _, err := store.RecordSession(session); err != nil {
		return
	}
	if err := store.CompleteSession(session.SessionID, "observed", "observe_only", session.Summary, nil, nil, at); err != nil {
		return
	}
	r.mu.Lock()
	if r.internalSessions == nil {
		r.internalSessions = store
	}
	if r.internalHeartbeatState.LastRun == nil {
		r.internalHeartbeatState.LastRun = map[string]time.Time{}
	}
	r.internalHeartbeatState.LastRun[id] = at.UTC()
	r.mu.Unlock()
}

func (r *Runtime) internalHeartbeatStatusSnapshot(now time.Time) InternalHeartbeatStatusSnapshot {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	anatomy := runtimeAnatomyConfig(r.cfg)
	r.mu.Lock()
	lastRun := map[string]time.Time{}
	for id, at := range r.internalHeartbeatState.LastRun {
		lastRun[id] = at
	}
	state := r.internalHeartbeatState
	state.LastRun = lastRun
	activeTask := r.activeTask != nil || strings.TrimSpace(r.scratch.ActiveTaskID) != ""
	paused := r.scratch.ControlPaused
	r.mu.Unlock()

	items := internalHeartbeatDueItemsWithState(anatomy, now, state, activeTask, paused)
	due := make([]string, 0, len(items))
	enabled := 0
	for _, item := range items {
		if item.Enabled {
			enabled++
		}
		if item.Due {
			due = append(due, item.ID)
		}
	}
	remoteLease := r.internalHeartbeatRemoteLeaseStatusSnapshot()
	executionMode := internalHeartbeatExecutionModeFromLeaseStatus(remoteLease)
	return InternalHeartbeatStatusSnapshot{
		Schema:                      anatomy.Schema,
		ProfileID:                   anatomy.ProfileID,
		Preset:                      anatomy.Preset,
		Digest:                      AgentAnatomyDigest(anatomy),
		ExecutionMode:               executionMode,
		RemoteLease:                 remoteLease,
		Memory:                      r.agentInternalSessionStatsSnapshot(),
		EnabledCount:                enabled,
		DueCount:                    len(due),
		MaxParallelInternalSessions: anatomy.Concurrency.MaxParallelInternalSessions,
		MaxLLMSessions:              anatomy.Concurrency.MaxLLMSessions,
		MaxBrowserSessions:          anatomy.Concurrency.MaxBrowserSessions,
		Due:                         due,
		Heartbeats:                  items,
	}
}

func (r *Runtime) buildInternalHeartbeatRuntimePack(now time.Time, maxChars int) string {
	if r == nil || r.cfg.Mode != RuntimeModeDaemon {
		return ""
	}
	snapshot := r.internalHeartbeatStatusSnapshot(now)
	var b strings.Builder
	b.WriteString("## Internal Heartbeat Runtime\n")
	b.WriteString(fmt.Sprintf("- execution_mode: %s\n", firstNonEmpty(snapshot.ExecutionMode, "unknown")))
	b.WriteString(fmt.Sprintf("- anatomy: profile=%s preset=%s digest=%s\n",
		firstNonEmpty(snapshot.ProfileID, "-"),
		firstNonEmpty(snapshot.Preset, "-"),
		firstNonEmpty(snapshot.Digest, "-"),
	))
	b.WriteString(fmt.Sprintf("- enabled_count: %d\n", snapshot.EnabledCount))
	b.WriteString(fmt.Sprintf("- due_count: %d\n", snapshot.DueCount))
	if len(snapshot.Due) > 0 {
		b.WriteString(fmt.Sprintf("- due: %s\n", strings.Join(snapshot.Due[:min(len(snapshot.Due), 8)], ", ")))
	}
	b.WriteString(fmt.Sprintf("- concurrency: internal_sessions=%d llm=%d browser=%d\n",
		snapshot.MaxParallelInternalSessions,
		snapshot.MaxLLMSessions,
		snapshot.MaxBrowserSessions,
	))
	lease := snapshot.RemoteLease
	b.WriteString(fmt.Sprintf("- remote_lease.state: %s\n", firstNonEmpty(lease.State, "unknown")))
	b.WriteString(fmt.Sprintf("- remote_lease.available: %t\n", lease.Available))
	b.WriteString(fmt.Sprintf("- remote_lease.checked: %t\n", lease.Checked))
	b.WriteString(fmt.Sprintf("- remote_lease.supported: %t\n", lease.Supported))
	b.WriteString(fmt.Sprintf("- remote_lease.source: %s\n", firstNonEmpty(lease.Source, "-")))
	b.WriteString(fmt.Sprintf("- remote_lease.method: %s\n", firstNonEmpty(lease.Method, "-")))
	b.WriteString(fmt.Sprintf("- remote_lease.fail_closed_on_indeterminate: %t\n", lease.FailClosedOnIndeterminate))
	b.WriteString("- guidance: public/non-local heartbeat loops use durable remote leases only after confirmed support; indeterminate discovery failures fail closed instead of running locally.\n")
	return clipForPrompt(strings.TrimSpace(b.String()), maxChars)
}

func internalHeartbeatCooldownsFromSessions(state AgentInternalSessionState, anatomy AgentAnatomyConfig) map[string]time.Time {
	digest := AgentAnatomyDigest(anatomy)
	specs := map[string]AgentHeartbeatSpec{}
	for _, spec := range anatomy.Heartbeats {
		spec = normalizeAgentHeartbeatSpec(spec)
		if spec.ID != "" && heartbeatEnabled(spec) {
			specs[spec.ID] = spec
		}
	}
	lastRun := map[string]time.Time{}
	for _, session := range state.Sessions {
		heartbeatID := strings.TrimSpace(session.HeartbeatID)
		if heartbeatID == "" {
			continue
		}
		if _, ok := specs[heartbeatID]; !ok {
			continue
		}
		if strings.TrimSpace(session.AnatomyDigest) != "" && strings.TrimSpace(session.AnatomyDigest) != digest {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(session.Status)) {
		case "completed", "failed", "abandoned":
		default:
			continue
		}
		at := internalHeartbeatParseTime(firstNonEmpty(session.EndedAt, session.StartedAt))
		if at.IsZero() {
			continue
		}
		if previous, ok := lastRun[heartbeatID]; !ok || at.After(previous) {
			lastRun[heartbeatID] = at.UTC()
		}
	}
	return lastRun
}

func agentInternalSessionStatsSnapshot(workspaceID, agentID string) AgentInternalSessionStats {
	state := LoadAgentInternalSessionState(workspaceID, agentID)
	return agentInternalSessionStatsLocked(state, 0)
}

func (r *Runtime) agentInternalSessionStatsSnapshot() AgentInternalSessionStats {
	if r == nil {
		return AgentInternalSessionStats{}
	}
	r.mu.Lock()
	store := r.internalSessions
	cfg := r.cfg
	r.mu.Unlock()
	if store != nil {
		return store.Stats()
	}
	return agentInternalSessionStatsSnapshot(cfg.WorkspaceID, cfg.AgentID)
}

func copyHeartbeatLockSet(in map[string]int) map[string]int {
	out := map[string]int{}
	for lock, count := range in {
		lock = strings.TrimSpace(lock)
		if lock != "" && count > 0 {
			out[lock] = count
		}
	}
	return out
}

func heartbeatLocksOverlap(locks []string, held map[string]int) bool {
	for _, lock := range locks {
		lock = strings.TrimSpace(lock)
		if lock == "" {
			continue
		}
		if held[lock] > 0 {
			return true
		}
	}
	return false
}

func addHeartbeatLocks(held map[string]int, locks []string) {
	for _, lock := range locks {
		lock = strings.TrimSpace(lock)
		if lock == "" {
			continue
		}
		held[lock]++
	}
}
