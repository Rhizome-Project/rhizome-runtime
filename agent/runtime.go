package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type Runtime struct {
	cfg                  RuntimeConfig
	client               *RhizomeClient
	agent                *Agent
	startupIdentityLease *runtimeIdentityLease

	// telicMeasureFn, when non-nil, overrides the real build+differential-oracle measurement (tests inject
	// a deterministic measurement; production leaves it nil and shells RuntimeConfig.TelicOracleCmd).
	telicMeasureFn func(ctx context.Context) telicMeasurement

	mu                            sync.Mutex
	runMu                         sync.Mutex
	bootstrap                     BootstrapResult
	scratch                       RuntimeScratchState
	inbox                         *MessageInbox
	health                        runtimeHealthState
	activeSession                 *AgentSessionStateRecord
	activeTask                    *WorkspaceTaskRecord
	activeHydration               *TaskHydrationBundle
	memory                        *AgentMemoryService
	internalSessions              *AgentInternalSessionStore
	internalHeartbeatLeaseOwnerID string
	internalHeartbeatLeaseChecked bool
	internalHeartbeatLeaseOK      bool
	hydrationScope                string
	hydrationAt                   time.Time
	hydrationStale                bool
	activeWorkPacket              *AgentWorkPacket
	activeRunID                   string
	lastBootstrap                 time.Time
	busy                          bool
	policySignals                 []ToolPolicySignal
	internalHeartbeatState        internalHeartbeatSchedulerState
	focus                         *RuntimeFocusState
	focusKey                      string
	focusRefreshedAt              time.Time
	startupCapabilitySnapshotID   string
	startupCapabilitySnapshotPath string
	activeCapabilitySnapshotID    string
	activeCapabilitySnapshotPath  string
	plannerFailureSignature       string
	plannerFailureCount           int
	plannerFailureReflectedSig    string
	plannerFailureReflectedAt     time.Time
	idleNoWorkKey                 string
	idleNoWorkCount               int
	idleReflectionKey             string
	idleReflectionAt              time.Time
	lastTensionProjectionRefresh  time.Time
	patchQueueDuplicateTasks      map[string]time.Time
	runWG                         sync.WaitGroup
	closed                        bool
	runStarted                    bool
	runRunning                    bool
	runCtx                        context.Context
	runCancel                     context.CancelFunc

	eventWakeMessage chan struct{}
	eventWakeRequest chan struct{}
	eventWakePlanner chan struct{}

	tokenMu                    sync.Mutex
	limitsGroupID              string
	limitsLoaded               bool
	dailyRemaining             int
	weeklyRemaining            int
	lastReportedDailyRemaining int
	budgetMu                   sync.Mutex
	budgetAccountEnsured       bool
	soakMu                     sync.Mutex
	soakProviderCalls          int
}

var errRuntimePlannerWorkCycleTimeout = errors.New("runtime planner work cycle timed out")

func NewRuntime(cfg RuntimeConfig, llm ChatLLM) *Runtime {
	cfg.ApplyDefaults()
	agent := &Agent{
		LLM:                  llm,
		Workdir:              cfg.Workdir,
		DisableLocalShell:    !runtimeAllowsLocalShell(cfg),
		DisableLocalMutation: !runtimeAllowsLocalMutation(cfg),
		DisableLocalMemory:   cfg.Mode == RuntimeModeDaemon,
	}
	runtime := &Runtime{
		cfg:                           cfg,
		client:                        NewRhizomeClient(cfg.RhizomeRPC, cfg.RhizomeToken),
		agent:                         agent,
		internalHeartbeatState:        newInternalHeartbeatSchedulerState(),
		internalHeartbeatLeaseOwnerID: newRuntimeID("ihb-owner"),
		eventWakeMessage:              make(chan struct{}, 1),
		eventWakeRequest:              make(chan struct{}, 1),
		eventWakePlanner:              make(chan struct{}, 1),
	}
	runtime.agent.LLM = wrapRuntimeBudgetedLLM(llm, runtime)
	runtime.bindAgentRuntimeState()
	runtime.agent.Init()

	return runtime
}

func (r *Runtime) Run(ctx context.Context) error {
	return r.runDaemon(ctx)
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.markRuntimeClosed()
	if err := r.Stop(); err != nil {
		return err
	}
	r.mu.Lock()
	memory := r.memory
	r.memory = nil
	r.internalSessions = nil
	if r.agent != nil {
		r.agent.MemoryService = nil
	}
	r.mu.Unlock()
	var closeErr error
	if memory != nil {
		closeErr = memory.Close()
	}
	r.releaseStartupIdentityLease()
	return closeErr
}

func (r *Runtime) bindAgentRuntimeState() {
	if r == nil || r.agent == nil {
		return
	}
	r.agent.Client = r.client
	r.agent.WorkspaceID = r.cfg.WorkspaceID
	r.agent.AgentID = r.cfg.AgentID
	r.agent.OwnerUserID = r.cfg.OwnerUserID
	r.agent.CoordinationMode = coordinationModeLabel(r.cfg)
	r.agent.Anatomy = runtimeAnatomyConfig(r.cfg)
	r.agent.DisableLocalShell = !runtimeAllowsLocalShell(r.cfg)
	r.agent.DisableLocalMutation = !runtimeAllowsLocalMutation(r.cfg)
	r.agent.DisableLocalMemory = r.cfg.Mode == RuntimeModeDaemon
	r.agent.MemoryService = r.memory
	r.agent.WorkspaceDocDraftGate = r.workspaceDocDraftGateState
	r.agent.RuntimeBinding = r.currentAgentRuntimeBinding
	r.agent.DisabledToolNames = append([]string(nil), r.cfg.DisabledToolNames...)
}

func (r *Runtime) RecordTokenUsage(prompt, completion int) {
	if r == nil {
		return
	}
	r.tokenMu.Lock()
	if !r.limitsLoaded {
		r.tokenMu.Unlock()
		return
	}
	r.dailyRemaining -= (prompt + completion)
	r.weeklyRemaining -= (prompt + completion)
	r.tokenMu.Unlock()
}

func (r *Runtime) refreshToolPlane(ctx context.Context) error {
	if r == nil || r.agent == nil {
		return nil
	}
	r.bindAgentRuntimeState()
	if r.cfg.Mode == RuntimeModeDaemon && !r.cfg.DaemonWorkspaceTools {
		return nil
	}
	return r.agent.RefreshRhizomeWorkspaceTools(
		ctx,
		r.client,
		r.cfg.WorkspaceID,
		r.cfg.AgentID,
		WithWorkspaceToolExecutionContextProvider(r.currentToolExecutionContext),
	)
}

func (r *Runtime) currentToolExecutionContext() (string, string, string) {
	binding := r.currentAgentRuntimeBinding()
	return binding.TaskID, binding.SessionID, binding.RunID
}

func (r *Runtime) currentAgentRuntimeBinding() AgentRuntimeBinding {
	if r == nil {
		return AgentRuntimeBinding{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	taskID := ""
	projectID := ""
	sessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	runID := strings.TrimSpace(r.scratch.ActiveRunID)
	snapshotID := firstNonEmpty(strings.TrimSpace(r.scratch.ActiveCapabilitySnapshotID), strings.TrimSpace(r.activeCapabilitySnapshotID))
	if sessionID != "" {
		taskID = strings.TrimSpace(r.scratch.ActiveTaskID)
	}
	if r.activeSession != nil {
		sessionID = firstNonEmpty(strings.TrimSpace(r.activeSession.SessionID), sessionID)
		taskID = firstNonEmpty(strings.TrimSpace(r.activeSession.TaskID), taskID)
	}
	if r.activeTask != nil {
		activeTaskID := strings.TrimSpace(r.activeTask.TaskID)
		if sessionID != "" && (taskID == "" || taskID == activeTaskID) {
			taskID = firstNonEmpty(activeTaskID, taskID)
			projectID = strings.TrimSpace(r.activeTask.ProjectID)
		}
	}
	runID = firstNonEmpty(strings.TrimSpace(r.activeRunID), runID)
	if sessionID == "" {
		taskID = ""
		runID = ""
	}
	var claimRepoID, claimCheckoutID, claimBranchID, claimWriteScopeJSON string
	if r.activeTask != nil && taskID != "" && strings.TrimSpace(r.activeTask.TaskID) == taskID {
		claimRepoID = strings.TrimSpace(pointerValue(r.activeTask.ClaimRepoID))
		claimCheckoutID = strings.TrimSpace(pointerValue(r.activeTask.ClaimCheckoutID))
		claimBranchID = strings.TrimSpace(pointerValue(r.activeTask.ClaimBranchID))
		claimWriteScopeJSON = strings.TrimSpace(pointerValue(r.activeTask.ClaimWriteScopeJSON))
	}
	return AgentRuntimeBinding{
		TaskID:                   taskID,
		ProjectID:                projectID,
		ClaimRepoID:              claimRepoID,
		ClaimCheckoutID:          claimCheckoutID,
		ClaimBranchID:            claimBranchID,
		ClaimWriteScopeJSON:      claimWriteScopeJSON,
		SessionID:                sessionID,
		RunID:                    runID,
		CapabilitySnapshotID:     snapshotID,
		CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
	}
}

func (r *Runtime) beginRuntimeRun(ctx context.Context) (context.Context, error) {
	r.runMu.Lock()
	defer r.runMu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("runtime is closed")
	}
	if r.runStarted {
		return nil, fmt.Errorf("runtime already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	if r.soakGuardConfigured() && r.cfg.SoakRuntimeLimit > 0 {
		runCtx, cancel = context.WithTimeout(ctx, r.cfg.SoakRuntimeLimit)
	}
	r.runStarted = true
	r.runRunning = true
	r.runCtx = runCtx
	r.runCancel = cancel
	r.runWG.Add(1)
	if r.soakGuardConfigured() && strings.TrimSpace(r.cfg.SoakStopFile) != "" {
		r.runWG.Add(1)
		go r.watchSoakStopFile(runCtx, cancel)
	}
	return runCtx, nil
}

func (r *Runtime) stopRuntimeRun() {
	if r == nil {
		return
	}
	r.runMu.Lock()
	cancel := r.runCancel
	r.runMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *Runtime) waitRuntimeRun() {
	if r == nil {
		return
	}
	r.runWG.Wait()
}

func (r *Runtime) finishRuntimeRun() {
	if r == nil {
		return
	}
	r.runMu.Lock()
	r.runRunning = false
	r.runCtx = nil
	r.runCancel = nil
	r.runMu.Unlock()
}

func (r *Runtime) markRuntimeClosed() {
	if r == nil {
		return
	}
	r.runMu.Lock()
	r.closed = true
	r.runMu.Unlock()
}

func (r *Runtime) Stop() error {
	if r == nil {
		return nil
	}
	r.stopRuntimeRun()
	r.runMu.Lock()
	started := r.runStarted
	r.runMu.Unlock()
	if started {
		r.waitRuntimeRun()
	}
	return nil
}

func (r *Runtime) runDaemon(ctx context.Context) error {
	runCtx, err := r.beginRuntimeRun(ctx)
	if err != nil {
		return err
	}
	startupReleased := false
	defer r.finishRuntimeRun()
	defer func() {
		if !startupReleased {
			r.runWG.Done()
		}
	}()

	if err := r.initialize(runCtx); err != nil {
		r.stopRuntimeRun()
		return err
	}
	if runCtx.Err() != nil {
		return nil
	}

	errCh := make(chan error, 5)

	startLoop := func(name string, fn func(context.Context) error) {
		r.runWG.Add(1)
		go func() {
			defer r.runWG.Done()
			r.recordLoopStarted(name, time.Now().UTC())
			if err := fn(runCtx); err != nil && runCtx.Err() == nil {
				r.recordLoopFailure(name, err, time.Now().UTC())
				select {
				case errCh <- fmt.Errorf("%s: %w", name, err):
				default:
				}
				r.stopRuntimeRun()
			}
		}()
	}

	startLoop("heartbeat", r.runHeartbeatLoop)
	startLoop("events", r.runEventLoop)
	startLoop("messages", r.runMessageLoop)
	startLoop("requests", r.runRequestLoop)
	startLoop("internal_heartbeat", r.runInternalHeartbeatLoop)
	startLoop("planner", r.runPlannerLoop)
	startLoop("watchdog", r.runWatchdogLoop)
	startLoop("memory-sync", r.runMemorySyncHealthLoop)
	startupReleased = true
	r.runWG.Done()

	select {
	case err := <-errCh:
		r.stopRuntimeRun()
		r.waitRuntimeRun()
		return err
	case <-runCtx.Done():
		r.waitRuntimeRun()
		return nil
	}
}

func (r *Runtime) initialize(ctx context.Context) error {
	r.cfg.ApplyDefaults()
	if r.client == nil {
		r.client = NewRhizomeClient(r.cfg.RhizomeRPC, r.cfg.RhizomeToken)
	} else {
		if strings.TrimSpace(r.client.endpoint) == "" {
			r.client.endpoint = strings.TrimSpace(r.cfg.RhizomeRPC)
		}
		if strings.TrimSpace(r.cfg.RhizomeToken) != "" {
			r.client.SetToken(r.cfg.RhizomeToken)
		}
	}

	if err := r.acquireStartupIdentityLease(); err != nil {
		return err
	}

	if err := Bootstrap(r.cfg.Workdir); err != nil {
		return fmt.Errorf("local bootstrap: %w", err)
	}
	if err := r.syncAgentIdentityFiles(); err != nil {
		log.Printf("[runtime] warning: could not sync local agent identity files: %v", err)
	}

	registered, err := r.ensureRemoteAccess(ctx)
	if err != nil {
		return err
	}
	if err := r.syncRemoteAgentProfile(ctx); err != nil {
		log.Printf("[runtime] warning: could not sync remote agent profile: %v", err)
	}

	bootstrap, err := r.refreshBootstrapWithRecovery(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	previousHealth, err := loadRuntimeLoopHealthSnapshot(r.cfg.WorkspaceID, r.cfg.AgentID)
	if err != nil {
		log.Printf("[runtime] warning: could not load previous loop health: %v", err)
	}
	scratch, err := loadScratchState(ctx, r.client, r.cfg.WorkspaceID, r.cfg.AgentID)
	if err != nil {
		return fmt.Errorf("load scratch: %w", err)
	}
	needsStartupPresenceSync := scratchHasActivePresence(scratch)
	inbox, err := OpenMessageInbox(r.cfg.WorkspaceID, r.cfg.AgentID)
	if err != nil {
		return fmt.Errorf("open message inbox: %w", err)
	}
	if err := inbox.MarkRuntimeStarted(time.Now().UTC()); err != nil {
		return fmt.Errorf("mark inbox runtime start: %w", err)
	}
	memory, err := OpenAgentMemoryService(r.cfg.WorkspaceID, r.cfg.AgentID)
	if err != nil {
		return fmt.Errorf("open agent memory service: %w", err)
	}
	// Layer A: install the always-on constitution tier from agent anatomy.
	memory.SetConstitution(runtimeAnatomyConfig(r.cfg).Memory.Constitution)
	internalSessions, err := OpenAgentInternalSessionStore(r.cfg.WorkspaceID, r.cfg.AgentID)
	if err != nil {
		return fmt.Errorf("open internal session store: %w", err)
	}
	if _, err := internalSessions.AbandonRunningSessions("runtime initialized with previous running internal heartbeat session", time.Now().UTC()); err != nil {
		return fmt.Errorf("reconcile internal sessions: %w", err)
	}
	hydratedHeartbeatLastRun := internalHeartbeatCooldownsFromSessions(internalSessions.Snapshot(), runtimeAnatomyConfig(r.cfg))
	if cursor := inbox.LastSyncedCursor(); cursor != "" && cursor != scratch.MessageCursor {
		scratch.MessageCursor = cursor
	}
	if err := r.persistConnectionProfile(registered, &bootstrap); err != nil {
		log.Printf("[runtime] warning: could not save Rhizome profile after bootstrap: %v", err)
	}

	r.mu.Lock()
	r.bootstrap = bootstrap
	r.scratch = scratch
	r.inbox = inbox
	r.memory = memory
	r.internalSessions = internalSessions
	if r.internalHeartbeatState.LastRun == nil {
		r.internalHeartbeatState.LastRun = map[string]time.Time{}
	}
	for heartbeatID, at := range hydratedHeartbeatLastRun {
		r.internalHeartbeatState.LastRun[heartbeatID] = at
	}
	r.bindAgentRuntimeState()
	// Since dynamic tools were set before memory, re-init them here
	r.agent.SetDynamicTools(nil)
	r.lastBootstrap = time.Now()
	r.health = newRuntimeHealthStateFromPrevious(r.lastBootstrap, previousHealth)
	r.reconcileScratchAfterBootstrapLocked(r.lastBootstrap)
	r.recoverActiveStateLocked()
	startupPlannerWake := r.prepareStartupPlannerWakeLocked(r.lastBootstrap)
	recordLoopSuccessLocked(&r.health, "scratch_replay", r.lastBootstrap)
	r.mu.Unlock()
	if _, _, err := r.captureStartupCapabilitySnapshot(); err != nil {
		return fmt.Errorf("startup capability snapshot: %w", err)
	}
	r.mu.Lock()
	stateCopy := r.scratch
	r.mu.Unlock()
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		log.Printf("[runtime] warning: could not persist reconciled scratch after initialize: %v", err)
	}
	if startupPlannerWake {
		r.wakePlanner()
	}
	if needsStartupPresenceSync {
		if err := r.syncPresenceDocs(ctx); err != nil {
			log.Printf("[runtime] warning: could not sync startup presence docs: %v", err)
		}
	}
	r.persistCurrentLoopHealthSnapshot(r.lastBootstrap)

	startupStatus, startupSummary := r.logStartupIdentityAndHeartbeat()
	if err := r.client.Heartbeat(ctx, AgentHeartbeatInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		Status:      startupStatus,
		Summary:     startupSummary,
	}); err != nil {
		return fmt.Errorf("initial heartbeat: %w", err)
	}
	r.recordLoopSuccess("heartbeat", time.Now().UTC())

	return nil
}

func scratchHasActivePresence(state RuntimeScratchState) bool {
	return strings.TrimSpace(state.ActiveTaskID) != "" ||
		strings.TrimSpace(state.ActiveSessionID) != "" ||
		strings.TrimSpace(state.ActiveRunID) != ""
}

func (r *Runtime) syncAgentIdentityFiles() error {
	profile := agentProfileForRuntimeConfig(r.cfg)
	return WriteAgentIdentityFilesWithDisabledTools(r.cfg.Workdir, profile, runtimeDisabledToolNameItems(r.cfg.DisabledToolNames))
}

func (r *Runtime) syncRemoteAgentProfile(ctx context.Context) error {
	if r == nil || r.client == nil {
		return nil
	}
	profile := agentProfileForRuntimeConfig(r.cfg)
	policy := normalizeMetacognitionPolicy(profile)

	tags := uniqueTrimmedCSVStrings(append([]string{profile.Role, profile.DefaultWorkMode}, profile.SecondarySpecializations...))
	metadata := map[string]any{
		"primary_specialization":       strings.TrimSpace(profile.PrimarySpecialization),
		"default_work_mode":            strings.TrimSpace(profile.DefaultWorkMode),
		"mission":                      strings.TrimSpace(profile.Mission),
		"response_language":            strings.TrimSpace(profile.ResponseLanguage),
		"reflection_scope":             strings.TrimSpace(policy.ReflectionScope),
		"idle_action_policy":           strings.TrimSpace(policy.IdleActionPolicy),
		"can_open_reflection_tasks":    policy.CanOpenReflectionTasks,
		"max_new_tasks_per_idle_cycle": policy.MaxNewTasksPerIdleCycle,
		"reality_check_quality_bar":    strings.TrimSpace(policy.RealityCheckDescription),
		"secondary_specializations":    append([]string(nil), profile.SecondarySpecializations...),
		"domain_scope":                 append([]string(nil), profile.DomainScope...),
	}

	return r.client.UpdateAgentProfile(ctx, AgentProfileUpdateInput{
		WorkspaceID:    r.cfg.WorkspaceID,
		AgentID:        r.cfg.AgentID,
		ActorID:        r.cfg.AgentID,
		Bio:            strings.TrimSpace(profile.Mission),
		Specialization: strings.TrimSpace(profile.PrimarySpecialization),
		Tags:           tags,
		ToolsAccess:    append([]string(nil), profile.AllowedTools...),
		Metadata:       metadata,
	})
}

func (r *Runtime) startupIdentitySummary() string {
	if r == nil {
		return ""
	}
	parts := []string{"identity"}
	if mode := strings.TrimSpace(string(r.cfg.Mode)); mode != "" {
		parts = append(parts, "mode="+mode)
	}
	if workdir := strings.TrimSpace(r.cfg.Workdir); workdir != "" {
		parts = append(parts, "workdir="+workdir)
	}
	var profile RuntimeLaunchProfile
	hasProfile := false
	if deterministicProfile, ok := deterministicLaunchProfileForWorkdir(r.cfg.Workdir); ok {
		profile = deterministicProfile
		hasProfile = true
		parts = append(parts, "profile="+profile.Name)
	}
	if agentID := strings.TrimSpace(firstNonEmpty(r.cfg.AgentID, profile.AgentID)); agentID != "" {
		parts = append(parts, "agent="+agentID)
	}
	if displayName := strings.TrimSpace(firstNonEmpty(r.cfg.DisplayName, profile.DisplayName)); displayName != "" {
		parts = append(parts, "display="+displayName)
	}
	if providerID := strings.TrimSpace(firstNonEmpty(r.cfg.ProviderID, profile.ProviderID)); providerID != "" {
		parts = append(parts, "provider="+providerID)
	}
	if groupID := strings.TrimSpace(firstNonEmpty(r.cfg.GroupID, profile.GroupID)); groupID != "" {
		parts = append(parts, "group="+groupID)
	}
	if backend := strings.TrimSpace(firstNonEmpty(r.cfg.LLMBackend, profile.LLMBackend)); backend != "" {
		parts = append(parts, "backend="+backend)
	}
	if model := strings.TrimSpace(firstNonEmpty(r.cfg.Model, profile.Model)); model != "" {
		parts = append(parts, "model="+model)
	}
	if role := strings.TrimSpace(firstNonEmpty(r.cfg.Role, profile.Role)); role != "" {
		parts = append(parts, "role="+role)
	}
	if hasProfile && len(r.cfg.Capabilities) == 0 && len(profile.Capabilities) > 0 {
		parts = append(parts, "capabilities="+strings.Join(profile.Capabilities, ","))
	}
	if workspaceID := strings.TrimSpace(r.cfg.WorkspaceID); workspaceID != "" {
		parts = append(parts, "workspace="+workspaceID)
	}
	if ownerUserID := strings.TrimSpace(r.cfg.OwnerUserID); ownerUserID != "" {
		parts = append(parts, "owner="+ownerUserID)
	}
	return strings.Join(parts, " ")
}

func (r *Runtime) startupHeartbeatSnapshot() (string, string) {
	status, summary := r.heartbeatSnapshot()
	identity := strings.TrimSpace(r.startupIdentitySummary())
	if identity == "" {
		return status, summary
	}
	r.mu.Lock()
	hasRecoveredWork := r.activeTask != nil || r.activeSession != nil || r.busy
	r.mu.Unlock()
	if !hasRecoveredWork {
		return status, identity
	}
	if summary == "" {
		return status, identity
	}
	return status, identity + " | " + summary
}

func (r *Runtime) logStartupIdentityAndHeartbeat() (string, string) {
	status, summary := r.startupHeartbeatSnapshot()
	if identity := strings.TrimSpace(r.startupIdentitySummary()); identity != "" {
		log.Printf("[startup] effective_identity %s", identity)
	}
	log.Printf("[startup] runtime_heartbeat status=%s summary=%s", status, summary)
	return status, summary
}

func (r *Runtime) ensureRemoteAccess(ctx context.Context) (AgentRegisterResult, error) {
	if strings.TrimSpace(r.cfg.RhizomeToken) != "" {
		r.client.SetToken(r.cfg.RhizomeToken)
		if strings.TrimSpace(r.cfg.RhizomeRPC) == "" && strings.TrimSpace(r.cfg.RhizomeHost) != "" {
			r.cfg.RhizomeRPC = defaultRPCEndpoint(r.cfg.RhizomeHost)
			r.client.SetEndpoint(r.cfg.RhizomeRPC)
		}
		return AgentRegisterResult{
			AgentID:       r.cfg.AgentID,
			DisplayName:   r.cfg.DisplayName,
			Token:         r.cfg.RhizomeToken,
			WorkspaceID:   r.cfg.WorkspaceID,
			WorkspaceName: r.cfg.WorkspaceName,
			HostURL:       r.cfg.RhizomeHost,
		}, nil
	}
	return r.registerAgent(ctx)
}

func (r *Runtime) registerAgent(ctx context.Context) (AgentRegisterResult, error) {
	registered, err := r.client.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		WorkspaceName:     r.cfg.WorkspaceName,
		WorkspacePassword: r.cfg.WorkspacePassword,
		HostURL:           r.cfg.RhizomeHost,
		AgentID:           r.cfg.AgentID,
		GroupID:           r.cfg.GroupID,
		DisplayName:       r.cfg.DisplayName,
		Role:              r.cfg.Role,
		OwnerUserID:       r.cfg.OwnerUserID,
		Capabilities:      r.cfg.Capabilities,
		Status:            "REGISTERED",
		ProtocolVersion:   r.cfg.ProtocolVersion,
		Summary:           "RNAR daemon starting",
	})
	if err != nil {
		return AgentRegisterResult{}, fmt.Errorf("register agent: %w", err)
	}
	r.applyRegisterResult(registered)
	log.Printf("[runtime] registered agent=%s status=%s", firstNonEmpty(registered.Agent.AgentID, registered.AgentID, r.cfg.AgentID), firstNonEmpty(registered.Agent.Status, "REGISTERED"))
	if err := r.persistConnectionProfile(registered, nil); err != nil {
		log.Printf("[runtime] warning: could not save Rhizome profile after register: %v", err)
	}
	return registered, nil
}

func (r *Runtime) applyRegisterResult(registered AgentRegisterResult) {
	applyRegisterResultToConfig(&r.cfg, r.client, registered)
	r.bindAgentRuntimeState()
}

func (r *Runtime) canRegister() bool {
	return (strings.TrimSpace(r.cfg.RhizomeRPC) != "" || strings.TrimSpace(r.cfg.RhizomeHost) != "") &&
		strings.TrimSpace(r.cfg.WorkspaceID) != "" &&
		strings.TrimSpace(r.cfg.AgentID) != "" &&
		strings.TrimSpace(r.cfg.OwnerUserID) != ""
}

func shouldRetryBootstrapWithFreshRegistration(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "forbidden") ||
		strings.Contains(msg, "invalid access token") ||
		strings.Contains(msg, "http 401") ||
		strings.Contains(msg, "http 403")
}

func (r *Runtime) persistConnectionProfile(registered AgentRegisterResult, bootstrap *BootstrapResult) error {
	return persistRuntimeProfiles(r.cfg.Workdir, r.cfg, registered, bootstrap)
}

func (r *Runtime) runHeartbeatLoop(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.HeartbeatEvery)
	defer ticker.Stop()
	var backoff transientBackoff

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.refreshStartupIdentityLease(); err != nil {
				r.recordLoopFailure("heartbeat", err, time.Now().UTC())
				return fmt.Errorf("runtime identity lease lost: %w", err)
			}
			status, summary := r.heartbeatSnapshot()
			if err := r.client.Heartbeat(ctx, AgentHeartbeatInput{
				WorkspaceID: r.cfg.WorkspaceID,
				AgentID:     r.cfg.AgentID,
				Status:      status,
				Summary:     summary,
			}); err != nil {
				log.Printf("[heartbeat] failed: %v", err)
				r.recordLoopFailure("heartbeat", err, time.Now().UTC())
				if r.recoverFromLoopFailure(ctx, "heartbeat", err) {
					backoff.Reset()
					continue
				}
				if !sleepContext(ctx, backoff.Next(2*time.Second, 30*time.Second)) {
					return nil
				}
				continue
			}
			r.recordLoopSuccess("heartbeat", time.Now().UTC())
			backoff.Reset()

			// Report Limits if used
			r.tokenMu.Lock()
			groupID := r.limitsGroupID
			daily := r.dailyRemaining
			weekly := r.weeklyRemaining
			lastReported := r.lastReportedDailyRemaining
			r.tokenMu.Unlock()

			if groupID != "" && daily != lastReported {
				if err := r.client.ReportLimits(ctx, LimitReportInput{
					GroupID:         groupID,
					AgentID:         r.cfg.AgentID,
					DailyRemaining:  daily,
					WeeklyRemaining: weekly,
				}); err != nil {
					log.Printf("[heartbeat] failed to report limits: %v", err)
				} else {
					r.tokenMu.Lock()
					r.lastReportedDailyRemaining = daily
					r.tokenMu.Unlock()
				}
			}

			if session := r.currentSessionCopy(); session != nil && isSessionRunnable(session) {
				keepalive := true
				_, err := r.client.SessionEvent(ctx, "agent.session.keepalive", SessionEventInput{
					WorkspaceID:       r.cfg.WorkspaceID,
					SessionID:         session.SessionID,
					AgentID:           r.cfg.AgentID,
					TaskID:            session.TaskID,
					Summary:           summary,
					Status:            session.Status,
					KeepSessionActive: &keepalive,
				})
				if err != nil && ctx.Err() == nil {
					if r.handleInactiveSessionKeepalive(ctx, session, err) {
						backoff.Reset()
						continue
					}
					log.Printf("[heartbeat] keepalive failed: %v", err)
					r.recordLoopFailure("heartbeat", err, time.Now().UTC())
					if r.recoverFromLoopFailure(ctx, "heartbeat", err) {
						continue
					}
					if !sleepContext(ctx, backoff.Next(2*time.Second, 30*time.Second)) {
						return nil
					}
				}
			}
		}
	}
}

func (r *Runtime) handleInactiveSessionKeepalive(ctx context.Context, session *AgentSessionStateRecord, err error) bool {
	if !isInactiveSessionKeepaliveError(err) {
		return false
	}
	sessionID := ""
	if session != nil {
		sessionID = strings.TrimSpace(session.SessionID)
	}
	summary := fmt.Sprintf("Session keepalive stopped for inactive session %s: %v", firstNonEmpty(sessionID, "unknown"), err)
	log.Printf("[heartbeat] %s", summary)
	if clearErr := r.clearActiveState(ctx, summary); clearErr != nil {
		log.Printf("[heartbeat] failed to clear inactive session state: %v", clearErr)
		r.recordLoopFailure("heartbeat", clearErr, time.Now().UTC())
		return false
	}
	r.recordLoopSuccess("heartbeat", time.Now().UTC())
	return true
}

func isInactiveSessionKeepaliveError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "session is not active") ||
		strings.Contains(msg, "(ended)") ||
		strings.Contains(msg, "status ended")
}

func (r *Runtime) runEventLoop(ctx context.Context) error {
	var backoff transientBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}

		ch, err := r.client.SubscribeEvents(ctx, r.cfg.WorkspaceID)
		if err != nil {
			log.Printf("[events] subscribe init failed: %v", err)
			if !sleepContext(ctx, backoff.Next(2*time.Second, 30*time.Second)) {
				return nil
			}
			continue
		}

		backoff.Reset()
		log.Printf("[events] connected to Server-Sent Events stream")

		for evt := range ch {
			switch evt.Type {
			case "agent.message", "agent.response", "workspace.tension.update", "news.publish", "news.published":
				if evt.Type == "agent.response" {
					r.handleAgentResponseRuntimeEvent(ctx, evt)
				}
				select {
				case r.eventWakeMessage <- struct{}{}:
				default:
				}
			case "task.project_fields.updated":
				if err := r.handleTaskProjectFieldsRuntimeEvent(ctx, evt); err != nil && ctx.Err() == nil {
					log.Printf("[events] task project fields wake failed: %v", err)
				}
			case "agent.request":
				select {
				case r.eventWakeRequest <- struct{}{}:
				default:
				}
			case "workspace_doc.upserted":
				if err := r.handleWorkspaceDocRuntimeEvent(ctx, evt); err != nil && ctx.Err() == nil {
					log.Printf("[events] workspace doc wake failed: %v", err)
				}
			case "project.updated",
				"project.lead.claimed",
				"project.lead.renewed",
				"project.lead.released",
				"project.lead.transferred",
				"project.lead.changed",
				"project.role.assigned",
				"project.role.released",
				"project.role.changed",
				"project.repository.upserted",
				"project.repository.changed",
				"project.checkout.registered",
				"project.checkout.changed",
				"project.branch.registered",
				"project.branch.changed",
				"workspace.ops.updated",
				"workspace.ops.resolved",
				"workspace.ops.escalated":
				if err := r.handleProjectCoordinationRuntimeEvent(ctx, evt); err != nil && ctx.Err() == nil {
					log.Printf("[events] project coordination wake failed: %v", err)
				}
			default:
				if isProjectCoordinationWakeEvent(evt.Type) {
					if err := r.handleProjectCoordinationRuntimeEvent(ctx, evt); err != nil && ctx.Err() == nil {
						log.Printf("[events] project coordination wake failed: %v", err)
					}
				}
			}
		}

		if !sleepContext(ctx, backoff.Next(2*time.Second, 15*time.Second)) {
			return nil
		}
	}
}

func (r *Runtime) runMessageLoop(ctx context.Context) error {
	var backoff transientBackoff
	var nextPoll time.Duration
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.eventWakeMessage:
			nextPoll = 0
		case <-time.After(nextPoll):
		}

		if err := r.reconcileLocalInbox(ctx); err != nil {
			log.Printf("[listener] inbox reconcile failed: %v", err)
			r.recordLoopFailure("listener", err, time.Now().UTC())
			if r.recoverFromLoopFailure(ctx, "listener", err) {
				backoff.Reset()
				nextPoll = 0
				continue
			}
			nextPoll = backoff.Next(2*time.Second, 30*time.Second)
			continue
		}

		pollInput := MessagePollInput{
			WorkspaceID:    r.cfg.WorkspaceID,
			AgentID:        r.cfg.AgentID,
			AfterCreatedAt: r.messageCursor(),
			Limit:          r.cfg.PollLimit,
			TimeoutSec:     0, // Disable HTTP long-polling on backend, rely on true Push-Streaming (SSE)
			LookbackHours:  r.cfg.LookbackHours,
		}
		result, err := r.client.PollMessages(ctx, pollInput)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if messagePollInvalidCursorError(err) {
				log.Printf("[listener] clearing stale message cursor after poll rejection: %v", err)
				if clearErr := r.clearMessageCursor(ctx); clearErr != nil {
					r.recordLoopFailure("listener", clearErr, time.Now().UTC())
					if r.recoverFromLoopFailure(ctx, "listener", clearErr) {
						backoff.Reset()
						nextPoll = 0
						continue
					}
					nextPoll = backoff.Next(2*time.Second, 30*time.Second)
					continue
				}
				backoff.Reset()
				nextPoll = 0
				continue
			}
			r.recordLoopFailure("listener", err, time.Now().UTC())
			if r.recoverFromLoopFailure(ctx, "listener", err) {
				backoff.Reset()
				nextPoll = 0
				continue
			}
			nextPoll = backoff.Next(2*time.Second, 30*time.Second)
			continue
		}
		r.recordLoopSuccess("listener", time.Now().UTC())
		backoff.Reset()

		messagesProcessed := false
		if len(result.Messages) == 0 {
			if cursor := strings.TrimSpace(result.NextCursor); cursor != "" {
				if err := r.syncMessageCursor(ctx, cursor); err != nil {
					return err
				}
			}
		} else {
			outcome, err := r.processMessageBatch(ctx, result)
			if outcome.nextCursor != "" {
				if err := r.syncMessageCursor(ctx, outcome.nextCursor); err != nil {
					return err
				}
			}
			if err != nil {
				log.Printf("[listener] message batch failed: %v", err)
				r.recordLoopFailure("listener", err, time.Now().UTC())
				if r.recoverFromLoopFailure(ctx, "listener", err) {
					nextPoll = 0
					continue
				}
				nextPoll = backoff.Next(2*time.Second, 30*time.Second)
				continue
			}
			messagesProcessed = true
		}

		if err := r.pollNews(ctx); err != nil {
			log.Printf("[listener] news poll failed: %v", err)
			r.recordLoopFailure("listener", err, time.Now().UTC())
			if r.recoverFromLoopFailure(ctx, "listener", err) {
				backoff.Reset()
				nextPoll = 0
				continue
			}
			nextPoll = backoff.Next(2*time.Second, 30*time.Second)
			continue
		}

		if messagesProcessed || len(result.Messages) > 0 {
			nextPoll = 0 // Drain inbox quickly if there are more messages
		} else {
			nextPoll = 2 * time.Minute // Fallback background sync interval
		}
	}
}

func (r *Runtime) runRequestLoop(ctx context.Context) error {
	var backoff transientBackoff
	var nextPoll time.Duration

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.eventWakeRequest:
			nextPoll = 0
		case <-time.After(nextPoll):
		}

		requests, err := r.client.ListPendingRequests(ctx, r.cfg.WorkspaceID, r.cfg.AgentID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[requests] list failed: %v", err)
			r.recordLoopFailure("requests", err, time.Now().UTC())
			if r.recoverFromLoopFailure(ctx, "requests", err) {
				backoff.Reset()
				nextPoll = 0
				continue
			}
			nextPoll = backoff.Next(2*time.Second, 30*time.Second)
			continue
		}
		backoff.Reset()

		if err := r.processRequestBatch(ctx, requests); err != nil {
			log.Printf("[requests] batch failed: %v", err)
			r.recordLoopFailure("requests", err, time.Now().UTC())
			if r.recoverFromLoopFailure(ctx, "requests", err) {
				nextPoll = 0
				continue
			}
			nextPoll = backoff.Next(2*time.Second, 30*time.Second)
			continue
		}
		r.recordLoopSuccess("requests", time.Now().UTC())

		if len(requests) > 0 {
			nextPoll = 0 // Drain pending requests
		} else {
			nextPoll = 2 * time.Minute // Fallback background sync interval
		}
	}
}

func (r *Runtime) runPlannerLoop(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PlannerEvery)
	defer ticker.Stop()
	var backoff transientBackoff

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.eventWakePlanner:
			r.drainPlannerWake()
		}
		if err := r.plannerTick(ctx); err != nil {
			log.Printf("[planner] tick failed: %v", err)
			now := time.Now().UTC()
			r.recordLoopFailure("planner", err, now)
			if errors.Is(err, errRuntimePlannerWorkCycleTimeout) {
				r.recordLoopFailure("planner_timeout", err, now)
			}
			if handled, handleErr := r.handlePlannerRepeatedFailure(ctx, err, now); handled {
				if handleErr != nil {
					return handleErr
				}
				backoff.Reset()
				continue
			}
			if r.recoverFromLoopFailure(ctx, "planner", err) {
				backoff.Reset()
				continue
			}
			r.invalidateBootstrap()
			if !sleepContext(ctx, backoff.Next(2*time.Second, 30*time.Second)) {
				return nil
			}
			continue
		}
		r.recordLoopSuccess("planner", time.Now().UTC())
		r.recordLoopSuccess("planner_timeout", time.Now().UTC())
		r.resetPlannerFailureReflection()
	}
}

func (r *Runtime) plannerTick(ctx context.Context) error {
	if r.runtimePaused() {
		return nil
	}
	if r.shouldRefreshBootstrap() {
		if err := r.refreshBootstrap(ctx); err != nil {
			return err
		}
	}
	if r.runtimePaused() {
		return nil
	}

	r.mu.Lock()
	if r.busy {
		r.mu.Unlock()
		return nil
	}
	r.busy = true
	r.mu.Unlock()
	defer r.maybeRefreshTensionProjectionOnPlannerCadence(ctx)
	defer r.finishPlannerTick()

	workCtx, cancelWorkCycle := r.plannerWorkCycleContext(ctx)
	defer cancelWorkCycle()

	task, err := r.ensureRunnableTask(workCtx)
	if err != nil || task == nil {
		return r.wrapPlannerWorkCycleError(workCtx, "ensure runnable task", err)
	}
	if err := r.ensureStrategicLeadLeaseForTask(workCtx, *task); err != nil {
		log.Printf("[project] strategic lead lease refresh skipped: %v", err)
	}
	err = r.executeTaskCycle(workCtx, *task)
	return r.wrapPlannerWorkCycleError(workCtx, "execute task cycle", err)
}

func (r *Runtime) plannerWorkCycleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := runtimePlannerCycleTimeout(r.cfg)
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func runtimePlannerCycleTimeout(cfg RuntimeConfig) time.Duration {
	if cfg.PlannerCycleTimeout > 0 {
		return cfg.PlannerCycleTimeout
	}
	if cfg.ProviderCallTimeout > 0 {
		return cfg.ProviderCallTimeout
	}
	return defaultPlannerCycleTimeout
}

func (r *Runtime) wrapPlannerWorkCycleError(ctx context.Context, phase string, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		timeout := runtimePlannerCycleTimeout(r.cfg)
		if timeout > 0 {
			return fmt.Errorf("%w after %s during %s: %v", errRuntimePlannerWorkCycleTimeout, timeout, phase, err)
		}
		return fmt.Errorf("%w during %s: %v", errRuntimePlannerWorkCycleTimeout, phase, err)
	}
	return err
}

func (r *Runtime) finishPlannerTick() {
	r.mu.Lock()
	r.busy = false
	shouldReplayWake := normalizeWorkTrigger(r.scratch.PendingTrigger) != ""
	r.mu.Unlock()

	if shouldReplayWake {
		r.wakePlanner()
	}
}

func (r *Runtime) shouldRefreshBootstrap() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bootstrapNeedsRefreshLocked()
}

func (r *Runtime) refreshBootstrap(ctx context.Context) error {
	bootstrap, err := r.refreshBootstrapWithRecovery(ctx)
	if err != nil {
		return err
	}

	now := time.Now()
	r.mu.Lock()
	beforePresence := r.visiblePresenceIdentityLocked()
	r.bootstrap = bootstrap
	r.lastBootstrap = now
	reconciled := r.reconcileScratchAfterBootstrapLocked(now)
	r.recoverActiveStateLocked()
	r.invalidateFocusLocked()
	afterPresence := r.visiblePresenceIdentityLocked()
	shouldSyncPresence := beforePresence.shouldSyncAfter(afterPresence)
	stateCopy := r.scratch
	r.mu.Unlock()
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		if reconciled {
			log.Printf("[runtime] warning: could not persist reconciled scratch after bootstrap: %v", err)
		}
		return err
	}
	if shouldSyncPresence {
		if err := r.syncPresenceDocs(ctx); err != nil {
			return err
		}
	}
	return nil
}

type runtimePresenceIdentity struct {
	TaskID    string
	SessionID string
	RunID     string
}

func (r *Runtime) visiblePresenceIdentityLocked() runtimePresenceIdentity {
	identity := runtimePresenceIdentity{
		TaskID:    strings.TrimSpace(r.scratch.ActiveTaskID),
		SessionID: strings.TrimSpace(r.scratch.ActiveSessionID),
		RunID:     strings.TrimSpace(r.scratch.ActiveRunID),
	}
	if r.activeTask != nil {
		identity.TaskID = strings.TrimSpace(r.activeTask.TaskID)
	}
	if r.activeSession != nil {
		identity.SessionID = strings.TrimSpace(r.activeSession.SessionID)
	}
	if strings.TrimSpace(r.activeRunID) != "" {
		identity.RunID = strings.TrimSpace(r.activeRunID)
	}
	return identity
}

func (id runtimePresenceIdentity) hasActive() bool {
	return id.TaskID != "" || id.SessionID != "" || id.RunID != ""
}

func (id runtimePresenceIdentity) shouldSyncAfter(next runtimePresenceIdentity) bool {
	if !id.hasActive() {
		return false
	}
	return id != next
}

func (r *Runtime) handleInboundMessage(ctx context.Context, message MessageRecord) error {
	channel := strings.TrimSpace(message.Channel)
	if strings.EqualFold(channel, "system.instruction") {
		content := strings.TrimSpace(message.Content)
		if strings.Contains(content, "FLUSH_P1_CACHE") {
			if svc := r.memoryService(); svc != nil {
				_ = svc.forceRebuild()
			}
			if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
				state.LastSummary = "Reflexive P1 cache flush triggered by system instruction"
				// Inject an advisory signal so the LLM gets explicit prompt context about the intervention
				state.AdvisorySignals = append(state.AdvisorySignals, "SYSTEM INTERVENTION: Server-side Motif Anomaly Detectors caught you in a Thrash loop. Your memory has been flushed. You MUST fundamentally change your approach, stop repeating previous actions, or explicitly ask for human/peer help.")
				if len(state.AdvisorySignals) > 5 {
					state.AdvisorySignals = state.AdvisorySignals[len(state.AdvisorySignals)-5:]
				}
			}); err != nil {
				return err
			}
			r.invalidateFocus()
			r.markBootstrapStale()
		}
		return nil
	}

	if strings.EqualFold(channel, "news") {
		if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
			state.LastSummary = "News broadcast received"
		}); err != nil {
			return err
		}
		r.invalidateFocus()
		r.markBootstrapStale()
		return nil
	}

	taskID := extractTaskIDFromMessage(message)
	r.logInboundMessageMemory(message, taskID)
	if err := r.maybeQueueResumeTrigger(ctx, "inbound_message", taskID); err != nil {
		return err
	}

	updatePayload := map[string]any{
		"status":      "INBOUND_MESSAGE",
		"task_ids":    []string{},
		"links":       []string{},
		"notes":       oneLine(message.Content),
		"next_action": "replan after inbound message",
	}
	if taskID != "" {
		updatePayload["task_ids"] = []string{taskID}
		if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
			state.LastWakeTaskID = strings.TrimSpace(taskID)
			state.LastSummary = fmt.Sprintf("Inbound message from %s", firstNonEmpty(message.FromAgentID, "unknown"))
		}); err != nil {
			return err
		}
	} else if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.LastSummary = fmt.Sprintf("Inbound message from %s", firstNonEmpty(message.FromAgentID, "unknown"))
	}); err != nil {
		return err
	}
	r.invalidateFocus()
	r.markBootstrapStale()
	rawPayload, _ := json.Marshal(updatePayload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "progress",
		Summary:     fmt.Sprintf("Inbound message from %s", message.FromAgentID),
		PayloadJSON: string(rawPayload),
	})
}

func (r *Runtime) handleRequest(ctx context.Context, request AgentRequestRecord) error {
	r.logRequestMemory(request)
	status := r.runtimeStatusPayload(time.Now().UTC())
	status["method"] = request.Method

	switch strings.ToLower(strings.TrimSpace(request.Method)) {
	case "runtime.ping", "runtime.status":
	case "runtime.pause":
		var payload runtimeControlRequestPayload
		if err := decodeRuntimeRequestPayload(request, &payload); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		control, err := r.pauseRuntime(ctx, payload)
		if err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		status["summary"] = "pause requested"
		status["control"] = control
		status["paused"] = control.Paused
		status["attachable"] = control.Attachable
	case "runtime.switch_task":
		var payload runtimeControlRequestPayload
		if err := decodeRuntimeRequestPayload(request, &payload); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		control, err := r.switchTaskRuntime(ctx, payload)
		if err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		status["summary"] = "task switch requested"
		status["control"] = control
		status["paused"] = control.Paused
		status["attachable"] = control.Attachable
	case "runtime.switch_tension":
		var payload runtimeControlRequestPayload
		if err := decodeRuntimeRequestPayload(request, &payload); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		control, mutation, err := r.switchTensionRuntime(ctx, payload)
		if mutation != nil {
			status["tension_mutation"] = mutation
		}
		if err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		status["summary"] = "tension switch requested"
		status["control"] = control
		status["paused"] = control.Paused
		status["attachable"] = control.Attachable
	case "runtime.memory.status", "runtime.memory.packet", "runtime.memory.rebuild", "runtime.memory.invalidate":
		if handled := r.handleRuntimeMemoryRequest(ctx, request, status); handled {
			break
		}
	case "runtime.refresh":
		if err := r.refreshBootstrap(ctx); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		status["summary"] = "bootstrap refreshed"
	case "runtime.resume":
		var payload runtimeControlRequestPayload
		if err := decodeRuntimeRequestPayload(request, &payload); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		control, err := r.resumeRuntime(ctx, payload)
		if err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		status["summary"] = "resume requested"
		status["control"] = control
		status["paused"] = control.Paused
		status["attachable"] = control.Attachable
	case projectRoleRequestMethod:
		if err := r.handleProjectRoleRequest(ctx, request, status); err != nil {
			return err
		}
	case "model.ask":
		if delegated, ok := delegatedAgentTaskRequestFromRecord(request); ok {
			if err := r.handleDelegatedAgentTaskRequest(ctx, request, delegated); err != nil {
				status["status"] = "error"
				status["details"] = err.Error()
				break
			}
			return nil
		}
		response, err := r.answerAgentRequest(ctx, request)
		if err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			break
		}
		if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
			return err
		}
		if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
			log.Printf("[requests] public response evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		return nil
	default:
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(request.Method)), "runtime.") {
			status["status"] = "unsupported"
			status["details"] = "runtime request method is not implemented by " + appCommandName + " RNAR"
			break
		}
		status["status"] = "unsupported"
		status["details"] = "request method is not implemented; use model.ask for live prompt requests"
	}

	raw, _ := json.Marshal(status)
	return r.client.RespondRequest(ctx, request.RequestID, runtimeStatusResponseText(status, raw))
}

func (r *Runtime) heartbeatSnapshot() (string, string) {
	r.mu.Lock()
	summary := firstNonEmpty(r.scratch.LastSummary, "idle")
	activeSession := r.activeSession
	activeTask := r.activeTask
	busy := r.busy
	inbox := r.inbox
	r.mu.Unlock()

	if inbox != nil {
		if suffix := inbox.Summary(); suffix != "" {
			summary = summary + " | " + suffix
		}
	}
	if suffix := r.memorySummary(); suffix != "" {
		summary = summary + " | " + suffix
	}
	if suffix := r.watchdogSummary(time.Now().UTC()); suffix != "" {
		summary = summary + " | " + suffix
	}
	if suffix := r.currentFocusSummary(); suffix != "" {
		summary = summary + " | " + suffix
	}
	if r.runtimePaused() {
		return "PAUSED", summary + " | paused"
	}

	if activeSession != nil {
		switch strings.ToUpper(strings.TrimSpace(activeSession.Status)) {
		case "BLOCKED", "WAITING_DECISION", "HANDOFF_PENDING":
			return "BLOCKED", summary
		default:
			return "ACTIVE", summary
		}
	}
	if busy || activeTask != nil {
		return "ACTIVE", summary
	}
	return "ACTIVE", summary
}

func (r *Runtime) currentSummary() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return firstNonEmpty(r.scratch.LastSummary, "idle")
}

func (r *Runtime) currentSessionCopy() *AgentSessionStateRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeSession == nil {
		return nil
	}
	session := *r.activeSession
	return &session
}

func (r *Runtime) currentTaskCopy() *WorkspaceTaskRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeTask == nil {
		return nil
	}
	task := *r.activeTask
	return &task
}

func (r *Runtime) currentHydrationCopy() *TaskHydrationBundle {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeHydration == nil {
		return nil
	}
	return cloneTaskHydrationBundle(r.activeHydration)
}

func (r *Runtime) currentWorkPacketCopy() *AgentWorkPacket {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneAgentWorkPacket(r.activeWorkPacket)
}

func (r *Runtime) messageCursor() string {
	r.mu.Lock()
	inbox := r.inbox
	scratchCursor := r.scratch.MessageCursor
	r.mu.Unlock()
	if inbox != nil {
		if cursor := inbox.LastSyncedCursor(); cursor != "" {
			return cursor
		}
	}
	return scratchCursor
}

func (r *Runtime) messageInbox() *MessageInbox {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inbox
}

func (r *Runtime) updateScratch(ctx context.Context, update func(state *RuntimeScratchState)) error {
	r.mu.Lock()
	state := r.scratch
	if state.DocSHAs == nil {
		state.DocSHAs = map[string]string{}
	}
	if state.WorkspaceDocWakeVersions == nil {
		state.WorkspaceDocWakeVersions = map[string]string{}
	}
	update(&state)
	r.mu.Unlock()

	state = normalizeScratchStateForPersist(clearConsumedPendingTriggerInState(state))
	if err := saveScratchState(ctx, r.client, r.cfg.WorkspaceID, r.cfg.AgentID, state); err != nil {
		return err
	}
	r.mu.Lock()
	r.scratch = state
	r.mu.Unlock()
	return nil
}

func (r *Runtime) saveScratchState(ctx context.Context, state RuntimeScratchState) error {
	state = clearConsumedPendingTriggerInState(state)
	if r != nil {
		state = r.mergeCurrentPendingTriggerIntoScratchState(state)
	}
	state = normalizeScratchStateForPersist(state)
	return saveScratchState(ctx, r.client, r.cfg.WorkspaceID, r.cfg.AgentID, state)
}

func (r *Runtime) mergeCurrentPendingTriggerIntoScratchState(state RuntimeScratchState) RuntimeScratchState {
	if r == nil {
		return state
	}
	r.mu.Lock()
	if scratchStatePendingTriggerConsumed(r.scratch) {
		clearPendingTriggerFields(&r.scratch)
	}
	current := RuntimeScratchState{
		PendingTrigger:        r.scratch.PendingTrigger,
		PendingTriggerTask:    r.scratch.PendingTriggerTask,
		PendingTriggerSession: r.scratch.PendingTriggerSession,
		PendingTriggerAt:      r.scratch.PendingTriggerAt,
	}
	if pendingTriggerConsumedByScratchState(state, current) {
		clearPendingTriggerFields(&r.scratch)
		clearPendingTriggerFields(&state)
		r.mu.Unlock()
		return state
	}
	r.mu.Unlock()
	if normalizeWorkTrigger(current.PendingTrigger) == "" {
		return state
	}
	if normalizeWorkTrigger(state.PendingTrigger) == "" || pendingTriggerTimestampAfter(current.PendingTriggerAt, state.PendingTriggerAt) {
		state.PendingTrigger = current.PendingTrigger
		state.PendingTriggerTask = current.PendingTriggerTask
		state.PendingTriggerSession = current.PendingTriggerSession
		state.PendingTriggerAt = current.PendingTriggerAt
	}
	state = clearConsumedPendingTriggerInState(state)
	return state
}

func pendingTriggerConsumedByScratchState(state RuntimeScratchState, current RuntimeScratchState) bool {
	if normalizeWorkTrigger(current.PendingTrigger) == "" {
		return false
	}
	candidate := state
	candidate.PendingTrigger = current.PendingTrigger
	candidate.PendingTriggerTask = current.PendingTriggerTask
	candidate.PendingTriggerSession = current.PendingTriggerSession
	candidate.PendingTriggerAt = current.PendingTriggerAt
	return scratchStatePendingTriggerConsumed(candidate)
}

func pendingTriggerTimestampAfter(candidate, existing string) bool {
	candidate = strings.TrimSpace(candidate)
	existing = strings.TrimSpace(existing)
	if candidate == "" {
		return existing == ""
	}
	if existing == "" {
		return true
	}
	candidateAt, candidateErr := time.Parse(time.RFC3339Nano, candidate)
	existingAt, existingErr := time.Parse(time.RFC3339Nano, existing)
	if candidateErr != nil || existingErr != nil {
		return candidate > existing
	}
	return candidateAt.After(existingAt)
}

func (r *Runtime) recoverActiveStateLocked() {
	var recoveredTask *WorkspaceTaskRecord
	var recoveredSession *AgentSessionStateRecord
	previousTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	previousSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	previousRunID := strings.TrimSpace(r.scratch.ActiveRunID)

	if taskID := strings.TrimSpace(r.scratch.ActiveTaskID); taskID != "" {
		if taskCopy, ok := recoverableBootstrapTask(r.bootstrap.Snapshot.Tasks, taskID, r.cfg.AgentID); ok {
			recoveredTask = &taskCopy
		}
	}
	if recoveredTask == nil {
		for i := range r.bootstrap.Snapshot.Tasks {
			task := r.bootstrap.Snapshot.Tasks[i]
			if recoverableBootstrapTaskRecord(task, r.cfg.AgentID) {
				taskCopy := task
				recoveredTask = &taskCopy
				break
			}
		}
	}

	if sessionID := strings.TrimSpace(r.scratch.ActiveSessionID); sessionID != "" {
		for i := range r.bootstrap.Snapshot.Sessions {
			session := r.bootstrap.Snapshot.Sessions[i]
			if session.SessionID == sessionID && session.AgentID == r.cfg.AgentID && recoverableSessionTask(session, r.bootstrap.Snapshot.Tasks, r.cfg.AgentID) {
				sessionCopy := session
				recoveredSession = &sessionCopy
				break
			}
		}
	}
	if recoveredSession == nil {
		for i := range r.bootstrap.Snapshot.Sessions {
			session := r.bootstrap.Snapshot.Sessions[i]
			if session.AgentID != r.cfg.AgentID {
				continue
			}
			if strings.ToUpper(strings.TrimSpace(session.Status)) == "ENDED" {
				continue
			}
			if !recoverableSessionTask(session, r.bootstrap.Snapshot.Tasks, r.cfg.AgentID) {
				continue
			}
			sessionCopy := session
			recoveredSession = &sessionCopy
			break
		}
	}
	if recoveredSession != nil && strings.TrimSpace(recoveredSession.TaskID) != "" {
		if taskCopy, ok := recoverableBootstrapTask(r.bootstrap.Snapshot.Tasks, recoveredSession.TaskID, r.cfg.AgentID); ok {
			recoveredTask = &taskCopy
		} else {
			recoveredSession = nil
		}
	}
	if recoveredSession != nil && strings.TrimSpace(recoveredSession.TaskID) != "" && recoveredTask == nil {
		recoveredSession = nil
	}

	if recoveredTask == nil && recoveredSession == nil {
		r.activeTask = nil
		r.activeSession = nil
		r.clearHydrationLocked()
		r.activeWorkPacket = nil
		r.activeRunID = ""
		r.scratch.ActiveTaskID = ""
		r.scratch.ActiveSessionID = ""
		r.scratch.ActiveRunID = ""
		if previousTaskID != "" || previousSessionID != "" || previousRunID != "" {
			r.scratch.LastSummary = "idle"
		}
		r.invalidateFocusLocked()
		return
	}

	r.activeTask = recoveredTask
	r.activeSession = recoveredSession
	r.clearHydrationLocked()
	r.activeWorkPacket = nil
	recoveredSessionID := ""
	if recoveredSession != nil {
		recoveredSessionID = strings.TrimSpace(recoveredSession.SessionID)
	}
	recoveredTaskID := ""
	if recoveredTask != nil {
		recoveredTaskID = strings.TrimSpace(recoveredTask.TaskID)
	}
	recoveredRunID := strings.TrimSpace(r.scratch.ActiveRunID)
	if recoveredRunID != "" && (previousTaskID != recoveredTaskID || previousSessionID != recoveredSessionID) {
		recoveredRunID = ""
		r.scratch.ActiveRunID = ""
	}
	r.activeRunID = recoveredRunID
	if recoveredTask != nil {
		r.scratch.ActiveTaskID = recoveredTask.TaskID
		if recoveredSession == nil {
			r.scratch.ActiveSessionID = ""
			r.scratch.ActiveRunID = ""
			r.activeRunID = ""
		}
		if shouldRefreshActivationSummary(previousTaskID, previousSessionID, previousRunID, recoveredTask.TaskID, recoveredSessionID, r.activeRunID, r.scratch.LastSummary) {
			r.scratch.LastSummary = taskActivationSummary(*recoveredTask, "")
		}
	}
	if recoveredSession != nil {
		r.scratch.ActiveSessionID = recoveredSession.SessionID
		r.scratch.ActiveTaskID = firstNonEmpty(strings.TrimSpace(recoveredSession.TaskID), r.scratch.ActiveTaskID)
		if shouldRefreshActivationSummary(previousTaskID, previousSessionID, previousRunID, r.scratch.ActiveTaskID, recoveredSession.SessionID, r.activeRunID, r.scratch.LastSummary) && recoveredTask != nil {
			r.scratch.LastSummary = taskActivationSummary(*recoveredTask, "")
		}
	}
	r.invalidateFocusLocked()
}

func recoverableBootstrapTask(tasks []WorkspaceTaskRecord, taskID, agentID string) (WorkspaceTaskRecord, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return WorkspaceTaskRecord{}, false
	}
	for i := range tasks {
		task := tasks[i]
		if strings.TrimSpace(task.TaskID) == taskID && recoverableBootstrapTaskRecord(task, agentID) {
			return task, true
		}
	}
	return WorkspaceTaskRecord{}, false
}

func recoverableBootstrapTaskRecord(task WorkspaceTaskRecord, agentID string) bool {
	if bootstrapTaskStatusIsTerminal(task.Status) {
		return false
	}
	return hasClaimedOwnership(task, agentID)
}

func bootstrapTaskStatusIsTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "DONE", "RESOLVED", "FAILED", "CANCELLED", "CANCELED":
		return true
	default:
		return false
	}
}

func recoverableSessionTask(session AgentSessionStateRecord, tasks []WorkspaceTaskRecord, agentID string) bool {
	if strings.TrimSpace(session.AgentID) != strings.TrimSpace(agentID) {
		return false
	}
	taskID := strings.TrimSpace(session.TaskID)
	if taskID == "" {
		return false
	}
	_, ok := recoverableBootstrapTask(tasks, taskID, agentID)
	return ok
}

func (r *Runtime) pinnedTaskID() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.cfg.PinnedTaskID)
}

func (r *Runtime) pinnedTaskAllows(taskID string) bool {
	pinned := r.pinnedTaskID()
	return pinned == "" || strings.TrimSpace(taskID) == pinned
}

func (r *Runtime) ensureRunnableTask(ctx context.Context) (*WorkspaceTaskRecord, error) {
	return r.ensureRunnableTaskAttempt(ctx, true)
}

func (r *Runtime) ensureRunnableTaskAttempt(ctx context.Context, allowFrontierClaimConflictRetry bool) (*WorkspaceTaskRecord, error) {
	exitIfManagedAgentStopRequested(r.cfg.Workdir)
	if r.runtimePaused() {
		return nil, nil
	}
	trigger := r.currentPendingWorkTrigger()
	now := time.Now().UTC()
	r.mu.Lock()
	if r.activeTask != nil && isTaskCandidate(*r.activeTask, r.cfg.AgentID) && isSessionRunnable(r.activeSession) {
		taskCopy := *r.activeTask
		var sessionCopy *AgentSessionStateRecord
		if r.activeSession != nil {
			copy := *r.activeSession
			sessionCopy = &copy
		}
		if !r.pinnedTaskAllows(taskCopy.TaskID) {
			log.Printf("[runtime] pinned task %s ignoring active task %s", r.pinnedTaskID(), strings.TrimSpace(taskCopy.TaskID))
			r.activeTask = nil
			r.activeSession = nil
			r.activeRunID = ""
			r.activeWorkPacket = nil
			r.scratch.ActiveTaskID = ""
			r.scratch.ActiveSessionID = ""
			r.scratch.ActiveRunID = ""
			r.invalidateFocusLocked()
			stateCopy := r.scratch
			r.mu.Unlock()
			if err := r.saveScratchState(ctx, stateCopy); err != nil {
				return nil, err
			}
			if err := r.syncPresenceDocs(ctx); err != nil {
				return nil, err
			}
		} else if taskClaimStatus(taskCopy) == "RELEASED" {
			r.activeTask = nil
			r.activeSession = nil
			r.activeRunID = ""
			r.activeWorkPacket = nil
			r.scratch.ActiveTaskID = ""
			r.scratch.ActiveSessionID = ""
			r.scratch.ActiveRunID = ""
			r.invalidateFocusLocked()
			stateCopy := r.scratch
			r.mu.Unlock()
			if err := r.saveScratchState(ctx, stateCopy); err != nil {
				return nil, err
			}
			if err := r.syncPresenceDocs(ctx); err != nil {
				return nil, err
			}
		} else {
			r.mu.Unlock()
			if yielded, err := r.maybeYieldOwnerBoundActiveTask(ctx, taskCopy, sessionCopy); err != nil {
				return nil, err
			} else if yielded {
				return nil, nil
			}
			if trigger.Trigger == "" {
				r.mu.Lock()
				if r.continuationHoldActiveLocked(now) {
					r.mu.Unlock()
					return nil, nil
				}
				if activeRootShouldPollStrategicRepair(taskCopy, r.scratch, now) {
					r.scratch.StrategicRepairPollAt = now.Format(time.RFC3339Nano)
					stateCopy := r.scratch
					r.mu.Unlock()
					if err := r.saveScratchState(ctx, stateCopy); err != nil {
						return nil, err
					}
				} else {
					r.mu.Unlock()
					r.resetIdleNoWorkReflection()
					return &taskCopy, nil
				}
			}
		}
	} else {
		r.mu.Unlock()
	}
	r.mu.Lock()
	if trigger.Trigger == "" && r.continuationHoldActiveLocked(time.Now().UTC()) {
		r.mu.Unlock()
		return nil, nil
	}
	r.mu.Unlock()
	if trigger.Trigger == "" {
		if err := r.reconcileSatisfiedWorkspaceDocBlockers(ctx); err != nil {
			return nil, err
		}
		trigger = r.currentPendingWorkTrigger()
	}
	if pinned := r.pinnedTaskID(); pinned != "" && strings.TrimSpace(trigger.TaskID) != pinned {
		trigger = pendingWorkTrigger{Trigger: "request_resume", TaskID: pinned}
	}

	includeHydration := boolPtr(true)
	includePacket := boolPtr(true)
	includeAdvisory := boolPtr(true)
	includeAllDocs := boolPtr(false)
	enableTaskFrontier := boolPtr(runtimeTrustFirst(r.cfg))
	work, err := r.client.WorkNext(ctx, WorkNextInput{
		WorkspaceID:        r.cfg.WorkspaceID,
		AgentID:            r.cfg.AgentID,
		IncludeHydration:   includeHydration,
		IncludePacket:      includePacket,
		IncludeAdvisory:    includeAdvisory,
		EnableTaskFrontier: enableTaskFrontier,
		FrontierLimit:      3,
		DocKeys:            defaultHydrationDocKeys(""),
		IncludeAllDocs:     includeAllDocs,
		UpdatesLimit:       10,
		ArtifactLimit:      10,
		RelatedTaskLimit:   10,
		SessionLimit:       100,
		Trigger:            trigger.Trigger,
		CandidateTaskID:    trigger.TaskID,
		CandidateSessionID: trigger.SessionID,
		CoordinationMode:   coordinationModeLabel(r.cfg),
	})
	if err != nil {
		if !shouldFallbackNativeAgentWork(err) {
			return nil, err
		}
		if r.bootstrapWorkFallbackDisabled() {
			return r.markWorkNextDegraded(ctx, trigger, err)
		}
		if trigger.Trigger != "" {
			if clearErr := r.clearPendingWorkTrigger(ctx); clearErr != nil {
				return nil, clearErr
			}
		}
		return r.ensureRunnableTaskFromBootstrap(ctx)
	}
	if pinned := r.pinnedTaskID(); pinned != "" && isTaskFrontierWorkNext(work) {
		selected, err := r.selectPinnedTaskFromFrontier(ctx, &work, pinned)
		if err != nil {
			return nil, err
		}
		if selected {
			log.Printf("[task-frontier] selected pinned task %s from frontier", strings.TrimSpace(work.Task.TaskID))
		}
	} else if isTaskFrontierWorkNext(work) {
		selected, err := r.selectTaskFromFrontier(ctx, &work)
		if err != nil {
			return nil, err
		}
		if selected {
			log.Printf("[task-frontier] selected task %s from frontier", strings.TrimSpace(work.Task.TaskID))
		}
	}
	if work.HasWork && work.Task != nil && !r.pinnedTaskAllows(work.Task.TaskID) {
		summary := fmt.Sprintf("Pinned runtime ignored non-pinned task %s; waiting for %s.", strings.TrimSpace(work.Task.TaskID), r.pinnedTaskID())
		log.Printf("[runtime] %s", summary)
		r.cacheHydration(nil, TaskHydrationInput{})
		r.mu.Lock()
		r.activeTask = nil
		r.activeSession = nil
		r.activeRunID = ""
		r.activeWorkPacket = cloneAgentWorkPacket(work.Packet)
		r.scratch.ActiveTaskID = ""
		r.scratch.ActiveSessionID = ""
		r.scratch.ActiveRunID = ""
		r.scratch.LastWakeTrigger = firstNonEmpty(work.Trigger, trigger.Trigger)
		r.scratch.LastWakeReason = "pinned_task_mismatch"
		r.scratch.LastWakeSummary = summary
		r.scratch.LastWakeTaskID = strings.TrimSpace(work.Task.TaskID)
		r.scratch.LastWakeSessionID = ""
		r.scratch.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
		r.scratch.LastSummary = summary
		r.invalidateFocusLocked()
		stateCopy := r.scratch
		r.mu.Unlock()
		if err := r.saveScratchState(ctx, stateCopy); err != nil {
			return nil, err
		}
		if err := r.syncPresenceDocs(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if !work.HasWork || work.Task == nil {
		r.cacheHydration(nil, TaskHydrationInput{})
		var stateToSave *RuntimeScratchState
		r.mu.Lock()
		if isClosedGateWorkNext(work) {
			packet := closedGateWorkPacket(work)
			r.activeWorkPacket = packet
			r.activeTask = nil
			r.activeSession = nil
			r.activeRunID = ""
			r.scratch.ActiveTaskID = ""
			r.scratch.ActiveSessionID = ""
			r.scratch.ActiveRunID = ""
			r.scratch.LastWakeTrigger = firstNonEmpty(work.Trigger, trigger.Trigger)
			r.scratch.LastWakeReason = closedGateWorkReason(work, packet)
			r.scratch.LastWakeSummary = closedGateWorkSummary(work, packet)
			r.scratch.LastWakeTaskID = ""
			r.scratch.LastWakeSessionID = ""
			r.scratch.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
			r.scratch.LastSummary = firstNonEmpty(r.scratch.LastWakeSummary, r.scratch.LastWakeReason, r.scratch.LastSummary)
			r.scratch.PendingTrigger = ""
			r.scratch.PendingTriggerTask = ""
			r.scratch.PendingTriggerSession = ""
			r.scratch.PendingTriggerAt = ""
			r.invalidateFocusLocked()
			stateCopy := r.scratch
			stateToSave = &stateCopy
		} else {
			r.activeWorkPacket = cloneAgentWorkPacket(work.Packet)
		}
		r.mu.Unlock()
		if stateToSave != nil {
			if err := r.saveScratchState(ctx, *stateToSave); err != nil {
				return nil, err
			}
			if err := r.syncPresenceDocs(ctx); err != nil {
				return nil, err
			}
		}
		if err := r.maybePostDelegatedSwitchBlockedUpdate(ctx, work, trigger); err != nil {
			log.Printf("[requests] delegated runtime_switch_task blocker evidence failed for %s: %v", strings.TrimSpace(trigger.TaskID), err)
		}
		if delegatedSwitchNoWorkShouldClearPending(work, trigger) {
			if clearErr := r.clearPendingWorkTrigger(ctx); clearErr != nil {
				return nil, clearErr
			}
			trigger = pendingWorkTrigger{}
		}
		if r.campaignBuildBreakActive() {
			r.resetIdleNoWorkReflection()
			return nil, nil
		}
		if handled, err := r.maybeMaterializePatchQueueSupersedeTask(ctx, work); err != nil {
			log.Printf("[patch-queue] supersede stewardship materialization failed: %v", err)
		} else if handled {
			r.resetIdleNoWorkReflection()
			return nil, nil
		}
		if handled, err := r.maybeMaterializePatchQueueClaimStewardshipTask(ctx, work); err != nil {
			log.Printf("[patch-queue] claim stewardship materialization failed: %v", err)
		} else if handled {
			r.resetIdleNoWorkReflection()
			return nil, nil
		}
		if handled, err := r.maybeMaterializePatchQueueSubmitHandoffTask(ctx, work); err != nil {
			log.Printf("[patch-queue] submit handoff materialization failed: %v", err)
		} else if handled {
			r.resetIdleNoWorkReflection()
			return nil, nil
		}
		if handled, err := r.maybeRequestProjectRoleScopeFromWork(ctx, work); err != nil {
			log.Printf("[project-role-request] enqueue from work packet failed: %v", err)
		} else if handled {
			r.resetIdleNoWorkReflection()
			return nil, nil
		}
		if handled, err := r.maybeEnqueueProjectClaimRepairFromWork(ctx, work); err != nil {
			log.Printf("[project-claim-repair] enqueue from work packet failed: %v", err)
		} else if handled {
			r.resetIdleNoWorkReflection()
			return nil, nil
		}
		if err := r.maybeMaterializeIdleReflection(ctx, work, trigger); err != nil {
			return nil, err
		}
		return nil, nil
	}
	r.resetIdleNoWorkReflection()
	if work.Session != nil {
		r.mu.Lock()
		sessionCopy := *work.Session
		r.activeSession = &sessionCopy
		r.invalidateFocusLocked()
		r.mu.Unlock()
	}
	if work.Hydration != nil {
		r.cacheHydration(work.Hydration, TaskHydrationInput{
			WorkspaceID:      r.cfg.WorkspaceID,
			TaskID:           strings.TrimSpace(work.Task.TaskID),
			DocKeys:          defaultHydrationDocKeys(""),
			IncludeAllDocs:   includeAllDocs,
			UpdatesLimit:     10,
			ArtifactLimit:    10,
			RelatedTaskLimit: 10,
		})
	} else {
		r.cacheHydration(nil, TaskHydrationInput{})
	}

	task := work.Task
	if req, ok, err := r.ownerBoundRequirementForTask(ctx, *task); err != nil {
		return nil, err
	} else if ok && (req.RepairNeeded || strings.TrimSpace(req.RequiredAgentID) == "" || strings.TrimSpace(req.RequiredAgentID) != strings.TrimSpace(r.cfg.AgentID)) {
		summary := fmt.Sprintf("Skipping owner-bound task %s for %s; required_agent=%s branch_id=%s repair_needed=%t.", strings.TrimSpace(task.TaskID), strings.TrimSpace(r.cfg.AgentID), strings.TrimSpace(req.RequiredAgentID), strings.TrimSpace(req.BranchID), req.RepairNeeded)
		raw, _ := json.Marshal(map[string]any{
			"handoff_state":         "owner_bound_not_claimed",
			"task_id":               strings.TrimSpace(task.TaskID),
			"agent_id":              strings.TrimSpace(r.cfg.AgentID),
			"required_agent_id":     strings.TrimSpace(req.RequiredAgentID),
			"branch_id":             strings.TrimSpace(req.BranchID),
			"branch_name":           strings.TrimSpace(req.BranchName),
			"repair_needed":         req.RepairNeeded,
			"reason":                firstNonEmpty(strings.TrimSpace(req.Reason), "non-owner selection blocked locally"),
			"preferred_transition":  "delegate_to_branch_owner",
			"branch_owner_agent_id": strings.TrimSpace(req.RequiredAgentID),
		})
		if err := r.client.PostUpdate(ctx, UpdatePostInput{
			WorkspaceID: strings.TrimSpace(r.cfg.WorkspaceID),
			AgentID:     strings.TrimSpace(r.cfg.AgentID),
			UpdateType:  "coordination",
			Summary:     summary,
			PayloadJSON: string(raw),
		}); err != nil {
			log.Printf("[owner-bound] local selection block evidence failed for %s: %v", strings.TrimSpace(task.TaskID), err)
		}
		r.cacheHydration(nil, TaskHydrationInput{})
		return nil, nil
	}
	if !strategyBlockerTriggerBypasses(trigger) {
		hold, err := r.shouldHoldRepeatedStrategyBlocker(ctx, *task)
		if err != nil {
			return nil, err
		}
		if hold {
			if trigger.Trigger != "" {
				if clearErr := r.clearPendingWorkTrigger(ctx); clearErr != nil {
					return nil, clearErr
				}
			}
			r.cacheHydration(nil, TaskHydrationInput{})
			return nil, nil
		}
	}
	if trigger.Trigger == "" && r.projectClaimHoldActiveForTask(*task, time.Now().UTC()) {
		log.Printf("[runtime] deferring task %s while project claim overlap hold is active", task.TaskID)
		r.cacheHydration(nil, TaskHydrationInput{})
		return nil, nil
	}
	claimIsLive := hasClaimedOwnership(*task, r.cfg.AgentID)
	reuseSelectedClaim := claimShouldBeReused(*task, work, r.cfg.AgentID)
	if !claimIsLive && !reuseSelectedClaim && nonRunnableClaimRequiresWake(*task, r.cfg.AgentID) && !workHasExplicitResumeWake(work, trigger) {
		log.Printf("[runtime] skipping non-runnable owned claim for task %s status=%s without explicit wake", task.TaskID, taskClaimStatus(*task))
		r.mu.Lock()
		r.activeTask = nil
		r.activeSession = nil
		r.activeRunID = ""
		r.activeWorkPacket = nil
		r.scratch.ActiveTaskID = ""
		r.scratch.ActiveSessionID = ""
		r.scratch.ActiveRunID = ""
		r.invalidateFocusLocked()
		stateCopy := r.scratch
		r.mu.Unlock()
		if err := r.saveScratchState(ctx, stateCopy); err != nil {
			return nil, err
		}
		if err := r.syncPresenceDocs(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if !claimIsLive && !reuseSelectedClaim && !r.allowsAutonomousExecutionClaim(*task) {
		r.mu.Lock()
		r.activeWorkPacket = nil
		r.invalidateFocusLocked()
		r.mu.Unlock()
		return nil, nil
	}
	var admission projectClaimAdmission
	claimMaterialized := false
	if !claimIsLive && !reuseSelectedClaim {
		if err := r.claimTaskWithAdmission(ctx, *task, "RNAR planner claimed task", &work, &admission); err != nil {
			if frontierErr := r.recordSelectedTaskFrontierFailure(ctx, work, *task, taskFrontierFailureState(err), err); frontierErr != nil {
				return nil, frontierErr
			}
			if handled, handleErr := r.handleProjectClaimAdmissionError(ctx, *task, err); handled {
				return nil, handleErr
			}
			if isClaimConflictError(err) {
				if holdErr := r.recordTaskClaimConflictHoldScratch(ctx, *task, err, time.Now().UTC()); holdErr != nil {
					return nil, holdErr
				}
				if refreshErr := r.refreshBootstrap(ctx); refreshErr != nil {
					return nil, refreshErr
				}
				if runtimeSwitchClaimConflictShouldSurface(trigger, *task) {
					return nil, fmt.Errorf("runtime_switch_task claim_admission rejected for task %s: %w", strings.TrimSpace(task.TaskID), err)
				}
				if allowFrontierClaimConflictRetry && taskFrontierClaimConflictRetryAllowed(trigger, work, *task) {
					return r.ensureRunnableTaskAttempt(ctx, false)
				}
				return nil, nil
			}
			return nil, err
		}
		claimMaterialized = true
	}

	session, runID, err := r.prepareSessionForSelectedWork(ctx, *task, work)
	if err != nil {
		if claimMaterialized || claimIsLive || reuseSelectedClaim {
			if repairErr := r.repairPostClaimSessionFailure(ctx, *task, work, err); repairErr != nil {
				return nil, fmt.Errorf("prepare session for selected work: %w; post-claim ownership repair failed: %v", err, repairErr)
			}
		}
		return nil, err
	}

	taskCopy := *task
	taskCopy.ClaimAgentID = &r.cfg.AgentID
	taskCopy.ClaimStatus = stringPtr("CLAIMED")
	admission.applyTask(&taskCopy)
	activationSummary := taskActivationSummary(taskCopy, firstNonEmpty(work.ResumeSummary, work.Reason))

	r.mu.Lock()
	previousTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	previousSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	previousRunID := strings.TrimSpace(r.scratch.ActiveRunID)
	r.activeTask = &taskCopy
	r.activeSession = session
	if work.Packet != nil {
		packetCopy := *work.Packet
		packetCopy.ClaimAction = firstNonEmpty(packetCopy.ClaimAction, work.ClaimAction)
		packetCopy.SessionAction = firstNonEmpty(packetCopy.SessionAction, work.SessionAction)
		r.activeWorkPacket = &packetCopy
	} else {
		r.activeWorkPacket = nil
	}
	r.activeRunID = runID
	r.scratch.ActiveTaskID = taskCopy.TaskID
	r.scratch.ActiveSessionID = session.SessionID
	r.scratch.ActiveRunID = runID
	if (trigger.Trigger != "" && (strings.TrimSpace(trigger.TaskID) == taskCopy.TaskID || scratchPendingTriggerMatches(r.scratch, trigger))) ||
		(normalizeWorkTrigger(work.Trigger) != "" && strings.TrimSpace(r.scratch.PendingTriggerTask) == taskCopy.TaskID) {
		clearPendingTriggerFields(&r.scratch)
	}
	if trigger.Trigger != "" ||
		strings.TrimSpace(r.scratch.ContinuationHoldTaskID) != taskCopy.TaskID ||
		strings.TrimSpace(r.scratch.ContinuationHoldSessionID) != session.SessionID {
		r.clearContinuationHoldLocked()
	}
	r.scratch.LastWakeTrigger = firstNonEmpty(work.Trigger, trigger.Trigger)
	r.scratch.LastWakeReason = work.Reason
	r.scratch.LastWakeSummary = firstNonEmpty(work.ResumeSummary, work.Reason)
	r.scratch.LastWakeTaskID = taskCopy.TaskID
	r.scratch.LastWakeSessionID = session.SessionID
	r.scratch.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
	if shouldRefreshActivationSummary(previousTaskID, previousSessionID, previousRunID, taskCopy.TaskID, session.SessionID, runID, r.scratch.LastSummary) {
		r.scratch.LastSummary = activationSummary
	}
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()

	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		return nil, err
	}
	if err := r.syncCurrentContextDoc(ctx, &taskCopy, session, nil); err != nil {
		return nil, err
	}
	if err := r.syncClaimedWorkDoc(ctx, &taskCopy, session, runID, nil); err != nil {
		return nil, err
	}
	if err := r.ensureCollaborationFanout(ctx, taskCopy, session, runID); err != nil {
		log.Printf("[runtime] collaboration fanout degraded for task %s: %v", taskCopy.TaskID, err)
	}
	if err := r.clearConsumedPendingWorkTrigger(ctx, trigger, taskCopy.TaskID); err != nil {
		return nil, err
	}
	return &taskCopy, nil
}

func activeRootShouldPollStrategicRepair(task WorkspaceTaskRecord, scratch RuntimeScratchState, now time.Time) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "strategy" && lane != "planning" && lane != "coordination" {
		return false
	}
	id := strings.ToLower(strings.TrimSpace(task.TaskID))
	template := strings.ToLower(strings.TrimSpace(task.TaskTemplate))
	title := strings.ToLower(strings.TrimSpace(task.Title))
	if !strings.HasPrefix(id, "root-") &&
		!strings.Contains(id, "-root-") &&
		!strings.HasSuffix(id, "-root") &&
		!strings.Contains(template, "root") &&
		!strings.Contains(title, "root") {
		return false
	}
	last, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(scratch.StrategicRepairPollAt))
	if err != nil || last.IsZero() {
		return true
	}
	return now.Sub(last) >= 2*time.Minute
}

func (r *Runtime) bootstrapWorkFallbackDisabled() bool {
	if r == nil {
		return false
	}
	mode := r.cfg.Mode
	if mode == "" {
		mode = RuntimeModeDaemon
	}
	return mode == RuntimeModeDaemon
}

func (r *Runtime) markWorkNextDegraded(ctx context.Context, trigger pendingWorkTrigger, cause error) (*WorkspaceTaskRecord, error) {
	summary := "agent.work.next unavailable; bootstrap compatibility fallback disabled in daemon mode"
	if cause != nil {
		summary = summary + ": " + strings.TrimSpace(cause.Error())
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	packet := &AgentWorkPacket{
		WorkType:            "work_next_degraded",
		CoordinationState:   "degraded_no_work",
		PreferredTransition: "wait_for_scheduler",
		WhyNow:              "bootstrap compatibility fallback disabled in daemon mode",
		Gate: &AgentWorkGate{
			GateState:  "closed",
			GateType:   "scheduler_contract",
			NeededFrom: "agent.work.next",
			Summary:    summary,
		},
	}

	r.cacheHydration(nil, TaskHydrationInput{})
	r.mu.Lock()
	r.activeTask = nil
	r.activeSession = nil
	r.activeRunID = ""
	r.activeWorkPacket = packet
	r.scratch.ActiveTaskID = ""
	r.scratch.ActiveSessionID = ""
	r.scratch.ActiveRunID = ""
	r.scratch.LastWakeTrigger = firstNonEmpty(trigger.Trigger, "agent.work.next")
	r.scratch.LastWakeReason = "work_next_degraded"
	r.scratch.LastWakeSummary = summary
	r.scratch.LastWakeTaskID = ""
	r.scratch.LastWakeSessionID = ""
	r.scratch.LastWakeAt = now
	r.scratch.LastSummary = summary
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()

	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		log.Printf("[planner] warning: could not persist work-next degraded state: %v", err)
	}
	if err := r.syncPresenceDocs(ctx); err != nil {
		log.Printf("[planner] warning: could not sync work-next degraded presence docs: %v", err)
	}
	return nil, nil
}

func (r *Runtime) ensureRunnableTaskFromBootstrap(ctx context.Context) (*WorkspaceTaskRecord, error) {
	r.mu.Lock()
	snapshotTasks := append([]WorkspaceTaskRecord(nil), r.bootstrap.Snapshot.Tasks...)
	snapshotSessions := append([]AgentSessionStateRecord(nil), r.bootstrap.Snapshot.Sessions...)
	r.mu.Unlock()

	task := chooseNextRunnableTask(snapshotTasks, snapshotSessions, r.cfg.AgentID)
	if task == nil {
		return nil, nil
	}
	if r.projectClaimHoldActiveForTask(*task, time.Now().UTC()) {
		log.Printf("[runtime] deferring bootstrap-selected task %s while project claim overlap hold is active", task.TaskID)
		return nil, nil
	}
	claimIsLive := hasClaimedOwnership(*task, r.cfg.AgentID)
	if !claimIsLive && !r.allowsAutonomousExecutionClaim(*task) {
		return nil, nil
	}

	var admission projectClaimAdmission
	claimMaterialized := false
	if !claimIsLive {
		if err := r.claimTaskWithAdmission(ctx, *task, "RNAR planner claimed task", nil, &admission); err != nil {
			if handled, handleErr := r.handleProjectClaimAdmissionError(ctx, *task, err); handled {
				return nil, handleErr
			}
			if isClaimConflictError(err) {
				if holdErr := r.recordTaskClaimConflictHoldScratch(ctx, *task, err, time.Now().UTC()); holdErr != nil {
					return nil, holdErr
				}
				return nil, r.refreshBootstrap(ctx)
			}
			return nil, err
		}
		claimMaterialized = true
	}

	r.mu.Lock()
	willReuseActiveSession := r.activeSession != nil &&
		isSessionRunnable(r.activeSession) &&
		r.activeSession.TaskID == task.TaskID &&
		strings.TrimSpace(r.activeSession.SessionID) != ""
	r.mu.Unlock()
	session, runID, err := r.ensureSessionForTask(ctx, *task)
	if err != nil {
		if claimMaterialized || claimIsLive {
			if repairErr := r.repairPostClaimSessionFailure(ctx, *task, AgentWorkNextResult{}, err); repairErr != nil {
				return nil, fmt.Errorf("ensure session for bootstrap work: %w; post-claim ownership repair failed: %v", err, repairErr)
			}
		}
		return nil, err
	}

	taskCopy := *task
	taskCopy.ClaimAgentID = &r.cfg.AgentID
	taskCopy.ClaimStatus = stringPtr("CLAIMED")
	admission.applyTask(&taskCopy)
	activationSummary := taskActivationSummary(taskCopy)
	claimAction := "claim_required"
	sessionAction := "start_new"
	if claimIsLive {
		claimAction = "reuse_claim"
	}
	if willReuseActiveSession {
		sessionAction = "reuse_active"
	}

	r.mu.Lock()
	previousTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	previousSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	previousRunID := strings.TrimSpace(r.scratch.ActiveRunID)
	r.activeTask = &taskCopy
	r.clearHydrationLocked()
	r.activeSession = session
	r.activeWorkPacket = &AgentWorkPacket{
		WorkType:            "bootstrap_scan",
		ClaimAction:         claimAction,
		SessionAction:       sessionAction,
		PreferredTransition: "start_new",
		WhyNow:              "bootstrap compatibility path",
	}
	r.activeRunID = runID
	r.scratch.ActiveTaskID = taskCopy.TaskID
	r.scratch.ActiveSessionID = session.SessionID
	r.scratch.ActiveRunID = runID
	r.scratch.LastWakeTrigger = "bootstrap_scan"
	r.scratch.LastWakeReason = "bootstrap_scan"
	r.scratch.LastWakeSummary = "selected work from bootstrap compatibility path"
	r.scratch.LastWakeTaskID = taskCopy.TaskID
	r.scratch.LastWakeSessionID = session.SessionID
	r.scratch.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
	if shouldRefreshActivationSummary(previousTaskID, previousSessionID, previousRunID, taskCopy.TaskID, session.SessionID, runID, r.scratch.LastSummary) {
		r.scratch.LastSummary = activationSummary
	}
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()

	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		return nil, err
	}
	if err := r.syncCurrentContextDoc(ctx, &taskCopy, session, nil); err != nil {
		return nil, err
	}
	if err := r.syncClaimedWorkDoc(ctx, &taskCopy, session, runID, nil); err != nil {
		return nil, err
	}
	return &taskCopy, nil
}

func (r *Runtime) ensureSessionForTask(ctx context.Context, task WorkspaceTaskRecord) (*AgentSessionStateRecord, string, error) {
	r.mu.Lock()
	if r.activeSession != nil && isSessionRunnable(r.activeSession) && r.activeSession.TaskID == task.TaskID && strings.TrimSpace(r.activeSession.SessionID) != "" {
		sessionCopy := *r.activeSession
		runID := r.activeRunID
		r.mu.Unlock()
		if runID == "" {
			run, err := r.ensureExecutionRunForMaterializedSession(ctx, task, sessionCopy)
			return &sessionCopy, run, err
		}
		return &sessionCopy, runID, nil
	}
	r.mu.Unlock()

	sessionID := newRuntimeID("session")
	sessionState, err := r.client.SessionEvent(ctx, "agent.session.start", SessionEventInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		SessionID:         sessionID,
		AgentID:           r.cfg.AgentID,
		TaskID:            task.TaskID,
		Summary:           firstNonEmpty(task.Title, task.Description, "task claimed"),
		Status:            "ACTIVE",
		OwnerScope:        "task/session",
		KeepSessionActive: boolPtr(true),
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, "", err
	}

	runID, err := r.ensureExecutionRunForMaterializedSession(ctx, task, sessionState)
	if err != nil {
		return nil, "", err
	}
	r.logSessionTransitionMemory("session_start", task.TaskID, sessionState.SessionID, sessionState.Status, sessionState.Summary, "", "", nil)

	return &sessionState, runID, nil
}

func (r *Runtime) ensureExecutionRun(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord) (string, error) {
	runID := strings.TrimSpace(r.activeRunID)
	activeTaskID := ""
	activeSessionID := ""
	if r.activeTask != nil {
		activeTaskID = strings.TrimSpace(r.activeTask.TaskID)
	}
	if r.activeSession != nil {
		activeSessionID = strings.TrimSpace(r.activeSession.SessionID)
	}
	if runID != "" && (activeTaskID != strings.TrimSpace(task.TaskID) || activeSessionID != strings.TrimSpace(session.SessionID)) {
		runID = ""
	}
	if runID == "" {
		runID = newRuntimeID("run")
	}
	run, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     firstNonEmpty(task.Description, "RNAR execution run"),
		Status:      "ACTIVE",
	})
	if err != nil {
		return "", err
	}
	return run.RunID, nil
}

func (r *Runtime) allowsAutonomousExecutionClaim(task WorkspaceTaskRecord) bool {
	profile := LoadAgentProfile(r.cfg.Workdir)
	if profileAllowsAutonomousExecutionClaim(profile) {
		return true
	}
	if isClaimOwnedBy(task, r.cfg.AgentID) && taskClaimStatus(task) == "CLAIMED" {
		return true
	}
	return false
}

func extractTaskIDFromMessage(message MessageRecord) string {
	type messageMetadata struct {
		TaskID string `json:"task_id"`
	}
	var metadata messageMetadata
	if strings.TrimSpace(message.MetadataJSON) != "" && json.Unmarshal([]byte(message.MetadataJSON), &metadata) == nil {
		return strings.TrimSpace(metadata.TaskID)
	}
	return ""
}

func stringPtr(value string) *string {
	return &value
}

func (r *Runtime) maybeResumeSessionFromMessage(ctx context.Context, taskID string) error {
	r.mu.Lock()
	activeSession := r.activeSession
	activeTask := r.activeTask
	r.mu.Unlock()

	if activeSession == nil || isSessionRunnable(activeSession) {
		return nil
	}
	if taskID != "" && strings.TrimSpace(activeSession.TaskID) != "" && strings.TrimSpace(activeSession.TaskID) != strings.TrimSpace(taskID) {
		return nil
	}
	if taskID == "" && activeTask == nil {
		return nil
	}

	resumed, err := r.client.SessionEvent(ctx, "agent.session.status", SessionEventInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		SessionID:         activeSession.SessionID,
		AgentID:           r.cfg.AgentID,
		TaskID:            firstNonEmpty(taskID, activeSession.TaskID),
		Summary:           "Inbound message received; resuming session",
		Status:            "ACTIVE",
		KeepSessionActive: boolPtr(true),
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.activeSession = &resumed
	r.mu.Unlock()
	r.logSessionTransitionMemory("session_resume", firstNonEmpty(taskID, resumed.TaskID), resumed.SessionID, resumed.Status, resumed.Summary, "", "", nil)
	return nil
}

func isClaimConflictError(err error) bool {
	if err == nil {
		return false
	}
	if isProjectClaimOverlapAdmissionError(err) {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "already") ||
		strings.Contains(msg, "claimed") ||
		strings.Contains(msg, "claim conflict")
}

func taskFrontierClaimConflictRetryAllowed(trigger pendingWorkTrigger, work AgentWorkNextResult, task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(trigger.Trigger) != "" || work.Packet == nil || work.Packet.Frontier == nil {
		return false
	}
	taskID := strings.TrimSpace(task.TaskID)
	return taskID != "" && strings.TrimSpace(work.Packet.Frontier.SelectedTaskID) == taskID
}

func runtimeSwitchClaimConflictShouldSurface(trigger pendingWorkTrigger, task WorkspaceTaskRecord) bool {
	taskID := strings.TrimSpace(task.TaskID)
	return taskID != "" &&
		normalizeWorkTrigger(trigger.Trigger) == "runtime_switch_task" &&
		strings.TrimSpace(trigger.TaskID) == taskID
}

func (r *Runtime) answerAgentRequest(ctx context.Context, request AgentRequestRecord) (string, error) {
	if err := r.refreshBootstrap(ctx); err != nil {
		return "", err
	}
	if err := r.refreshToolPlane(ctx); err != nil {
		log.Printf("[tool-plane] refresh routed tool plane degraded: %v", err)
	}

	task := r.currentTaskCopy()
	hydration := r.taskHydration(ctx, task)
	specPack := r.buildDaemonSpecPack(ctx, task)
	specPack = appendSpecPack(r.activeCapabilitySnapshotPromptProjectionForRequest(), specPack)
	taskCtx := AgentTaskContext{
		Mode:              "daemon",
		WorkspaceID:       r.cfg.WorkspaceID,
		AgentID:           r.cfg.AgentID,
		Summary:           r.currentSummary(),
		SpecPack:          specPack,
		Hydration:         hydration,
		Task:              task,
		AdvisorySignals:   r.scratch.AdvisorySignals,
		ReflectionSignals: r.scratch.ReflectionSignals,
	}
	if hydration == nil {
		taskCtx.Snapshot = &r.bootstrap.Snapshot
	}
	if session := r.currentSessionCopy(); session != nil {
		taskCtx.SessionID = session.SessionID
	}

	objective := buildAgentRequestObjective(request)
	response, err := r.agent.ExecuteTaskWithHooks(ctx, objective, taskCtx, r.peerRequestToolExecutor, nil)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(response), nil
}

func (r *Runtime) activeCapabilitySnapshotPromptProjectionForRequest() string {
	if r == nil {
		return ""
	}
	if snapshot, ok := r.activeCapabilitySnapshotFromScratch(); ok && r.requestCanReuseCapabilitySnapshot(snapshot) {
		return renderDaemonCapabilityPromptProjection(snapshot)
	}
	return renderDaemonCapabilityPromptProjection(r.requestCapabilitySnapshotFallback())
}

func (r *Runtime) requestCapabilitySnapshotFallback() DaemonCapabilitySnapshot {
	if r == nil {
		return DaemonCapabilitySnapshot{}
	}
	r.mu.Lock()
	bootID := strings.TrimSpace(r.startupCapabilitySnapshotID)
	r.mu.Unlock()
	snapshot := buildDaemonBootCapabilitySnapshot(r.cfg, r.agent, time.Now().UTC())
	if bootID != "" {
		snapshot.SnapshotID = bootID
		snapshot.PromptContract.SnapshotID = bootID
	}
	return snapshot
}

func (r *Runtime) requestCanReuseCapabilitySnapshot(snapshot DaemonCapabilitySnapshot) bool {
	if r == nil || strings.TrimSpace(snapshot.SnapshotID) == "" {
		return false
	}
	if !r.requestCapabilitySnapshotContractIsSafe(snapshot) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(snapshot.SnapshotKind), "run") {
		return true
	}
	r.mu.Lock()
	taskID := ""
	if r.activeTask != nil {
		taskID = strings.TrimSpace(r.activeTask.TaskID)
	}
	sessionID := ""
	if r.activeSession != nil {
		sessionID = strings.TrimSpace(r.activeSession.SessionID)
	}
	runID := strings.TrimSpace(r.activeRunID)
	r.mu.Unlock()
	binding := snapshot.Binding
	bindingTaskID := strings.TrimSpace(binding.TaskID)
	bindingSessionID := strings.TrimSpace(binding.SessionID)
	bindingRunID := strings.TrimSpace(binding.RunID)
	if bindingTaskID == "" || bindingSessionID == "" || bindingRunID == "" {
		return false
	}
	if taskID == "" || sessionID == "" || runID == "" {
		return false
	}
	return bindingTaskID == taskID && bindingSessionID == sessionID && bindingRunID == runID
}

func (r *Runtime) requestCapabilitySnapshotContractIsSafe(snapshot DaemonCapabilitySnapshot) bool {
	if r == nil {
		return false
	}
	cfg := r.cfg
	cfg.ApplyDefaults()
	if cfg.Mode != RuntimeModeDaemon {
		return true
	}
	disabled := capabilityDisabledToolNames(cfg, nil)
	for _, name := range snapshot.PromptContract.EnabledToolNames {
		if _, ok := capabilityDisabledToolNameMatch(name, disabled); ok {
			return false
		}
	}
	requiredSurfaceStates := map[string]string{
		"workspace_tools": "inspection_only",
		"mcp":             "disabled",
		"bridges":         "inspection_only",
		"executor":        "disabled",
		"memory":          "disabled",
		"ui":              "inspection_only",
	}
	for surfaceID, wantStatus := range requiredSurfaceStates {
		surface, ok := snapshot.Surfaces[surfaceID]
		if !ok || !strings.EqualFold(strings.TrimSpace(surface.Status), wantStatus) {
			return false
		}
	}
	return true
}

func (r *Runtime) buildDaemonSpecPack(ctx context.Context, task *WorkspaceTaskRecord) string {
	hydration := r.taskHydration(ctx, task)
	var snapshot *WorkspaceSnapshot
	if hydration == nil {
		snapshot = &r.bootstrap.Snapshot
	}
	specPack := buildSpecPack(r.cfg, snapshot, hydration, task)
	specPack = appendSpecPack(specPack, trustFirstAutonomySpecPack(r.cfg))
	specPack = appendSpecPack(specPack, daemonCapabilityPosturePack(r.cfg))
	specPack = appendSpecPack(specPack, r.buildInternalHeartbeatRuntimePack(time.Now().UTC(), max(1000, r.cfg.MaxPromptSpecChars/5)))
	specPack = appendSpecPack(specPack, buildWorkPacketPack(r.currentWorkPacketCopy(), max(1000, r.cfg.MaxPromptSpecChars/4)))
	r.mu.Lock()
	roster := append([]AgentRecord(nil), r.bootstrap.Snapshot.Agents...)
	r.mu.Unlock()
	specPack = appendSpecPack(specPack, buildAgentRosterPack(roster, r.cfg.AgentID, max(1000, r.cfg.MaxPromptSpecChars/4)))
	focus := r.ensureFocus(ctx, task)
	specPack = appendSpecPack(specPack, buildFocusPack(focus, max(1200, r.cfg.MaxPromptSpecChars/3)))
	specPack = appendSpecPack(specPack, r.buildMemoryPacket(task, hydration, focus))
	if r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" {
		return specPack
	}
	if focusHasResolvedLocus(focus) {
		return specPack
	}

	var report *ControlReport
	controlReport, err := r.client.GetControlReport(ctx, r.cfg.WorkspaceID, 3)
	if err != nil {
		log.Printf("[coordination] control report degraded: %v", err)
	} else {
		report = &controlReport
	}

	frontier, err := r.client.GetTensionFrontier(ctx, r.cfg.WorkspaceID, 5)
	if err != nil {
		log.Printf("[coordination] tension frontier degraded: %v", err)
	}

	return appendSpecPack(specPack, buildCoordinationPack(report, frontier, max(1200, r.cfg.MaxPromptSpecChars/3)))
}

func buildAgentRequestObjective(request AgentRequestRecord) string {
	var b strings.Builder
	b.WriteString("Respond to an incoming Rhizome agent request.\n")
	b.WriteString("Return a short direct response suitable for agent.respond.\n")
	b.WriteString("Do not include markdown fences unless the payload explicitly needs them.\n\n")
	b.WriteString("This request context is read-only. Do not run shell commands, write files, submit tasks, mutate project state, materialize checkouts, commit branches, or create workspace docs while answering model.ask.\n")
	b.WriteString("If the sender asks you to implement work, answer with the concrete task/project you need to claim through your normal planner/work loop before doing any checkout or mutation.\n\n")
	b.WriteString("Artifact access rule: your local workdir is not the sender's workdir. If the request only names a local file path without inline content, excerpt, artifact_ref, or workspace_doc doc_key, say that you cannot verify it and ask the sender to share the artifact through workspace docs.\n\n")
	b.WriteString("request_id=" + strings.TrimSpace(request.RequestID) + "\n")
	b.WriteString("from_agent_id=" + strings.TrimSpace(request.FromAgentID) + "\n")
	b.WriteString("method=" + strings.TrimSpace(request.Method) + "\n")
	if strings.TrimSpace(request.Payload) != "" {
		b.WriteString("payload:\n")
		b.WriteString(strings.TrimSpace(request.Payload))
		b.WriteString("\n")
	}
	return b.String()
}
