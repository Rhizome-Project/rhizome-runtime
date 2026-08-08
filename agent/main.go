package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var runtimeFlagOutput io.Writer = os.Stderr

func main() {
	if err := repairManagerStateOnStartup(); err != nil {
		fatal(err.Error())
	}
	if code := runAgentCLI(os.Args[1:], os.Stdout, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

func runAgentCLI(args []string, stdout, stderr io.Writer) int {
	err := dispatchAgentCLI(args, stdout)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	fmt.Fprintf(stderr, "error: %s\n", err)
	return 1
}

func dispatchAgentCLI(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return runManager(nil)
	}

	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "help", "-h", "--help":
		printManagerUsage(stdout)
		return nil
	case "onboard":
		return runOnboard(args[1:])
	case "refresh-credentials":
		return runRefreshCredentials(args[1:])
	case "sync-roster":
		return runSyncRoster(args[1:])
	case "manager", "visor":
		return runManager(args[1:])
	case "list":
		return runListAgents()
	case "show":
		return runShowAgent(args[1:])
	case "start":
		return runStartAgent(args[1:])
	case "stop":
		return runStopAgent(args[1:])
	case "restart":
		return runRestartAgent(args[1:])
	case "status":
		return runStatusAgent(args[1:])
	case "logs":
		return runLogsAgent(args[1:])
	case "chat":
		return runChatAgent(args[1:])
	case "attach":
		return runAttachAgent(args[1:])
	case "install":
		return runInstall(args[1:])
	case "web":
		return runWeb(args[1:])
	case "defaults":
		return runDefaults(args[1:])
	case "set-default":
		if len(args) < 3 {
			return fmt.Errorf("usage: %s set-default <field> <value>", appCommandName)
		}
		return SetManagerDefault(args[1], strings.Join(args[2:], " "))
	case "clear-default":
		if len(args) < 2 {
			return fmt.Errorf("usage: %s clear-default <field>", appCommandName)
		}
		return ClearManagerDefault(args[1])
	case "daemon":
		args = append([]string{"--mode", string(RuntimeModeDaemon)}, args[1:]...)
	case "tui":
		args = append([]string{"--mode", string(RuntimeModeTUI)}, args[1:]...)
	}

	cfg, err := loadRuntimeConfigFromArgs(args)
	if err != nil {
		return err
	}
	return runWithConfig(cfg)
}

func loadRuntimeConfigFromArgs(args []string) (RuntimeConfig, error) {
	if err := rejectSecretCLIArgs(args); err != nil {
		return RuntimeConfig{}, err
	}
	workdir, err := resolveInitialWorkdir(args)
	if err != nil {
		return RuntimeConfig{}, err
	}
	folderName := defaultFolderAgentName(workdir)
	launchProfile, _ := deterministicLaunchProfileForWorkdir(workdir)
	registryDefaults := LoadBotRegistry().Defaults
	if isManagedAgentProcess() {
		registryDefaults = BotManagerDefaults{}
	}
	globalProfile := LoadRhizomeProfile()
	localProfile := LoadLocalRuntimeProfile(workdir)

	flags := flag.NewFlagSet(runtimeFlagSetName(args), flag.ContinueOnError)
	flags.SetOutput(runtimeFlagOutput)

	defaultHost := firstNonEmpty(
		os.Getenv("RHIZOME_HOST"),
		localProfile.HostURL,
		registryDefaults.HostURL,
		globalProfile.HostURL,
		hostURLForRPC(firstNonEmpty(os.Getenv("RHIZOME_RPC"), localProfile.RPCEndpoint, globalProfile.RPCEndpoint)),
		defaultRhizomeHostURL,
	)
	defaultRPC := firstNonEmpty(
		os.Getenv("RHIZOME_RPC"),
		localProfile.RPCEndpoint,
		defaultRPCEndpoint(defaultHost),
		globalProfile.RPCEndpoint,
	)

	mode := flags.String("mode", firstNonEmpty(os.Getenv("RHIZOME_AGENT_MODE"), localProfile.Mode, "auto"), "Runtime mode: auto, daemon, or tui")
	providerID := flags.String("provider-id", firstNonEmpty(os.Getenv("RHIZOME_AGENT_PROVIDER_ID"), localProfile.ProviderID, launchProfile.ProviderID, registryDefaults.DefaultProviderID), "Provider ID")
	modelOverride := flags.String("model-override", firstNonEmpty(os.Getenv("RHIZOME_AGENT_MODEL_OVERRIDE"), localProfile.ModelOverride, launchProfile.ModelOverride, registryDefaults.ModelOverride), "Model override")
	groupID := flags.String("group-id", firstNonEmpty(os.Getenv("RHIZOME_AGENT_GROUP_ID"), os.Getenv("RHIZOME_AGENT_GROUP"), localProfile.GroupID, launchProfile.GroupID, registryDefaults.GroupID), "Rhizome agent group ID")
	llmBackend := flags.String("llm-backend", firstNonEmpty(os.Getenv("RHIZOME_AGENT_LLM_BACKEND"), localProfile.LLMBackend, launchProfile.LLMBackend, registryDefaults.LLMBackend, llmBackendAuto), "LLM backend: auto, openai, codex, qwen, or fake (deterministic local evaluation)")
	model := flags.String("model", firstNonEmpty(os.Getenv("RHIZOME_AGENT_MODEL"), localProfile.Model, launchProfile.Model, registryDefaults.Model, defaultModel), "Provider model name or deterministic fake scenario")
	realLLMPilotDefault := localProfile.RealLLMPilot
	if raw := strings.TrimSpace(os.Getenv("RHIZOME_REAL_LLM_PILOT")); raw != "" {
		realLLMPilotDefault = parseRuntimeBool(raw)
	}
	realLLMPilot := flags.Bool("real-llm-pilot", realLLMPilotDefault, "Enable bounded-profile validation and soak guards for real-provider daemon execution")
	coordinationMode := flags.String("coordination-mode", firstNonEmpty(os.Getenv("RHIZOME_COORDINATION_MODE"), registryDefaults.CoordinationMode, localProfile.CoordinationMode, defaultCoordinationMode), "Coordination mode: strict or trust_first")
	workdirFlag := flags.String("workdir", workdir, "Agent working directory")

	rhizomeRPC := flags.String("rhizome-rpc", defaultRPC, "Rhizome JSON-RPC endpoint")
	rhizomeHost := flags.String("rhizome-host", defaultHost, "Rhizome workspace host URL")
	rhizomeToken := firstNonEmpty(os.Getenv("RHIZOME_TOKEN"), localProfile.AgentToken, globalProfile.AgentToken)
	workspaceID := flags.String("workspace-id", firstNonEmpty(os.Getenv("RHIZOME_WORKSPACE_ID"), localProfile.effectiveWorkspaceID(), registryDefaults.WorkspaceID, globalProfile.WorkspaceID, defaultWorkspaceID), "Rhizome workspace ID")
	workspaceName := flags.String("workspace-name", firstNonEmpty(os.Getenv("RHIZOME_WORKSPACE_NAME"), localProfile.WorkspaceName, globalProfile.WorkspaceName), "Rhizome workspace name")
	workspacePassword := firstNonEmpty(os.Getenv("RHIZOME_WORKSPACE_PASSWORD"), localProfile.WorkspacePassword, registryDefaults.WorkspacePassword, globalProfile.WorkspacePassword)
	agentID := flags.String("agent-id", firstNonEmpty(os.Getenv("RHIZOME_AGENT_ID"), localProfile.effectiveAgentID(), launchProfile.AgentID, suggestAgentID(workdir), globalProfile.AgentID), "Rhizome agent ID")
	displayName := flags.String("display-name", firstNonEmpty(os.Getenv("RHIZOME_AGENT_NAME"), localProfile.effectiveDisplayName(), launchProfile.DisplayName, folderName, firstNonEmpty(localProfile.effectiveAgentID(), globalProfile.AgentID)), "Rhizome display name")
	ownerUserID := flags.String("owner-user-id", defaultOwnerUserIDWithDefaults(localProfile, registryDefaults, globalProfile), "Owner user ID for registration")
	role := flags.String("role", firstNonEmpty(os.Getenv("RHIZOME_AGENT_ROLE"), localProfile.effectiveRole(), launchProfile.Role, registryDefaults.Role, "generalist"), "Agent role")
	capabilities := flags.String("capabilities", firstNonEmpty(os.Getenv("RHIZOME_AGENT_CAPABILITIES"), strings.Join(firstCapabilities(localProfile.effectiveCapabilities(), launchProfile.Capabilities, registryDefaults.Capabilities), ",")), "Comma-separated capability list")

	heartbeatSec := flags.Int("heartbeat-sec", firstPositiveInt(localProfile.HeartbeatSec, durationSeconds(defaultHeartbeatInterval)), "Heartbeat interval in seconds")
	plannerSec := flags.Int("planner-sec", firstPositiveInt(localProfile.PlannerSec, durationSeconds(defaultPlannerInterval)), "Planner/control-loop interval in seconds")
	bootstrapSec := flags.Int("bootstrap-sec", firstPositiveInt(localProfile.BootstrapSec, durationSeconds(defaultBootstrapInterval)), "Bootstrap refresh interval in seconds")
	watchdogSec := flags.Int("watchdog-sec", firstPositiveInt(localProfile.WatchdogSec, durationSeconds(defaultWatchdogInterval)), "Watchdog/autonomy health interval in seconds")
	listenerTimeoutSec := flags.Int("listener-timeout-sec", firstPositiveInt(localProfile.ListenerTimeoutSec, durationSeconds(defaultPollTimeout)), "Long-poll timeout in seconds")
	listenerLookbackHours := flags.Int("listener-lookback-hours", firstPositiveInt(localProfile.ListenerLookbackHours, defaultLookbackHours), "Listener history lookback in hours when no cursor is stored")
	listenerBatch := flags.Int("listener-batch", firstPositiveInt(localProfile.ListenerBatch, defaultPollLimit), "Maximum messages per listener poll")
	updatesLimit := flags.Int("updates-limit", firstPositiveInt(localProfile.UpdatesLimit, defaultUpdatesLimit), "Bootstrap updates limit")
	protocolVersion := flags.String("protocol-version", firstNonEmpty(localProfile.effectiveProtocolVersion(), registryDefaults.ProtocolVersion, defaultProtocolVersion), "Rhizome protocol version")
	daemonWorkspaceTools := flags.Bool("daemon-workspace-tools", parseRuntimeBool(os.Getenv("RHIZOME_DAEMON_WORKSPACE_TOOLS")), "Enable workspace tool discovery in daemon mode")
	inboxDrainAdvisory := flags.Bool("inbox-drain-advisory", parseRuntimeBool(os.Getenv("RHIZOME_INBOX_DRAIN_ADVISORY")), "Check pending peer requests before daemon task cycles")
	budgetAccountID := flags.String("budget-account-id", firstNonEmpty(os.Getenv("RHIZOME_BUDGET_ACCOUNT_ID"), localProfile.BudgetAccountID), "Rhizome budget account ID for runtime LLM provider calls")
	budgetHardLimitMicros := flags.Int64("budget-hard-limit-micros", firstPositiveInt64(envInt64("RHIZOME_BUDGET_HARD_LIMIT_MICROS"), localProfile.BudgetHardLimitMicros), "Hard runtime LLM budget account limit in micros")
	budgetReserveMicros := flags.Int64("budget-reserve-micros", firstPositiveInt64(envInt64("RHIZOME_BUDGET_RESERVE_MICROS"), localProfile.BudgetReserveMicros), "Per-call runtime LLM budget reservation in micros")
	budgetMicrosPerToken := flags.Int64("budget-micros-per-token", firstPositiveInt64(envInt64("RHIZOME_BUDGET_MICROS_PER_TOKEN"), localProfile.BudgetMicrosPerToken), "Runtime budget micros charged per reported LLM token")
	maxToolLoopIterations := flags.Int("max-tool-loop-iterations", firstPositiveInt(int(envInt64("RHIZOME_MAX_TOOL_LOOP_ITERATIONS")), localProfile.MaxToolLoopIterations, defaultToolLoopIterations), "Maximum tool-loop iterations per daemon task")
	toolLoopCompactionDefault := localProfile.ToolLoopCompaction
	if raw := strings.TrimSpace(os.Getenv("RHIZOME_TOOL_LOOP_COMPACTION")); raw != "" {
		toolLoopCompactionDefault = parseRuntimeBool(raw)
	}
	toolLoopCompaction := flags.Bool("tool-loop-compaction", toolLoopCompactionDefault, "Enable bounded prompt-view compaction for repeated tool iterations")
	maxProviderRetryAttempts := flags.Int("max-provider-retry-attempts", firstPositiveInt(int(envInt64("RHIZOME_MAX_PROVIDER_RETRY_ATTEMPTS")), localProfile.MaxProviderRetryAttempts, defaultProviderRetryLimit), "Maximum provider retry attempts per daemon task")
	providerCallTimeoutSec := flags.Int("provider-call-timeout-sec", firstPositiveInt(int(envInt64("RHIZOME_PROVIDER_CALL_TIMEOUT_SEC")), localProfile.ProviderCallTimeoutSec, durationSeconds(defaultProviderCallTimeout)), "Maximum wall-clock seconds for one LLM provider call")
	plannerCycleTimeoutSec := flags.Int("planner-cycle-timeout-sec", firstPositiveInt(int(envInt64("RHIZOME_PLANNER_CYCLE_TIMEOUT_SEC"))), "Maximum wall-clock seconds for one daemon planning/task cycle; defaults to provider timeout when omitted")
	disabledTools := flags.String("disabled-tools", firstNonEmpty(os.Getenv("RHIZOME_DISABLED_TOOLS"), os.Getenv("RHIZOME_AGENT_DISABLED_TOOLS")), "Comma-separated tool names to disable for this runtime")
	pinnedTaskID := flags.String("pinned-task-id", firstNonEmpty(os.Getenv("RHIZOME_PINNED_TASK_ID"), os.Getenv("RHIZOME_TASK_PIN")), "Restrict daemon work selection to one task ID")
	telicLoop := flags.Bool("telic-loop", os.Getenv("RHIZOME_TELIC_LOOP") == "1", "Enable an optional oracle-driven progress gate for a pinned task")
	telicTargetPath := flags.String("telic-target-path", os.Getenv("RHIZOME_TELIC_TARGET_PATH"), "Deliverable path, relative to the checkout, monitored by the progress gate")
	telicCheckoutDir := flags.String("telic-checkout-dir", os.Getenv("RHIZOME_TELIC_CHECKOUT_DIR"), "Checkout directory evaluated by the progress oracle")
	telicOracleCmd := flags.String("telic-oracle-cmd", os.Getenv("RHIZOME_TELIC_ORACLE_CMD"), "Command used to evaluate deliverable progress")
	telicRequireFrozenAuthority := flags.Bool("telic-require-frozen-authority", parseRuntimeBool(os.Getenv("RHIZOME_TELIC_REQUIRE_FROZEN_AUTHORITY")), "Require an authority-stability signal before the progress gate can converge")
	soakStopFile := flags.String("soak-stop-file", firstNonEmpty(os.Getenv("RHIZOME_SOAK_STOP_FILE"), localProfile.SoakStopFile), "Stop-file path for a bounded real-provider evaluation")
	soakRuntimeLimitSec := flags.Int("soak-runtime-limit-sec", firstPositiveInt(int(envInt64("RHIZOME_SOAK_RUNTIME_LIMIT_SEC")), localProfile.SoakRuntimeLimitSec), "Hard wall-clock limit in seconds for a bounded real-provider evaluation")
	soakMaxProviderCalls := flags.Int("soak-max-provider-calls", firstPositiveInt(int(envInt64("RHIZOME_SOAK_MAX_PROVIDER_CALLS")), localProfile.SoakMaxProviderCalls), "Maximum provider calls in a bounded real-provider evaluation")

	// String defaults can contain local paths, identities, and profile values.
	// Keep them out of generated help text while preserving useful numeric and
	// boolean defaults.
	hideRuntimeStringDefaults(flags)
	if err := flags.Parse(args); err != nil {
		return RuntimeConfig{}, err
	}
	explicitFlags := make(map[string]bool)
	flags.Visit(func(f *flag.Flag) {
		explicitFlags[f.Name] = true
	})
	if explicitFlags["rhizome-host"] && !explicitFlags["rhizome-rpc"] {
		*rhizomeRPC = defaultRPCEndpoint(*rhizomeHost)
	}

	cfg := RuntimeConfig{
		ProtocolVersion:             *protocolVersion,
		OpenAIKey:                   "",
		ProviderID:                  *providerID,
		ModelOverride:               *modelOverride,
		LLMBackend:                  *llmBackend,
		Model:                       *model,
		RealLLMPilot:                *realLLMPilot,
		CoordinationMode:            *coordinationMode,
		Workdir:                     *workdirFlag,
		RhizomeRPC:                  *rhizomeRPC,
		RhizomeHost:                 *rhizomeHost,
		RhizomeToken:                rhizomeToken,
		WorkspaceID:                 *workspaceID,
		WorkspaceName:               *workspaceName,
		WorkspacePassword:           workspacePassword,
		AgentID:                     *agentID,
		DisplayName:                 *displayName,
		OwnerUserID:                 *ownerUserID,
		GroupID:                     *groupID,
		Role:                        *role,
		Capabilities:                parseCapabilitiesCSV(*capabilities),
		HeartbeatEvery:              time.Duration(*heartbeatSec) * time.Second,
		PlannerEvery:                time.Duration(*plannerSec) * time.Second,
		BootstrapEvery:              time.Duration(*bootstrapSec) * time.Second,
		WatchdogEvery:               time.Duration(*watchdogSec) * time.Second,
		PollTimeout:                 time.Duration(*listenerTimeoutSec) * time.Second,
		PollLimit:                   *listenerBatch,
		LookbackHours:               *listenerLookbackHours,
		UpdatesLimit:                *updatesLimit,
		MemorySyncEvery:             secondsDuration(localProfile.MemorySyncSec),
		MaxPromotionSyncBatch:       localProfile.PromotionSyncBatch,
		RSPRolloutPhase:             localProfile.RSPRolloutPhase,
		ListenerStaleAfter:          secondsDuration(localProfile.ListenerStaleAfterSec),
		RequestStaleAfter:           secondsDuration(localProfile.RequestStaleAfterSec),
		PlannerStaleAfter:           secondsDuration(localProfile.PlannerStaleAfterSec),
		TaskStallAfter:              secondsDuration(localProfile.TaskStallAfterSec),
		MemoryRepairCooldown:        secondsDuration(localProfile.MemoryRepairCooldownSec),
		MemoryPromotionStaleAfter:   secondsDuration(localProfile.MemoryPromotionStaleSec),
		MemoryStalePacketThreshold:  localProfile.MemoryStalePacketThreshold,
		MaxPromptDocChars:           localProfile.MaxPromptDocChars,
		MaxPromptSpecChars:          localProfile.MaxPromptSpecChars,
		MaxResultMemoryBody:         localProfile.MaxResultMemoryBodyChars,
		MaxToolLoopIterations:       *maxToolLoopIterations,
		ToolLoopCompaction:          *toolLoopCompaction,
		MaxProviderRetryAttempts:    *maxProviderRetryAttempts,
		ProviderCallTimeout:         time.Duration(*providerCallTimeoutSec) * time.Second,
		PlannerCycleTimeout:         time.Duration(*plannerCycleTimeoutSec) * time.Second,
		DisabledToolNames:           uniqueTrimmedCSVStrings(strings.Split(*disabledTools, ",")),
		PinnedTaskID:                strings.TrimSpace(*pinnedTaskID),
		TelicLoopEnabled:            *telicLoop,
		TelicTargetPath:             strings.TrimSpace(*telicTargetPath),
		TelicCheckoutDir:            strings.TrimSpace(*telicCheckoutDir),
		TelicOracleCmd:              strings.TrimSpace(*telicOracleCmd),
		TelicRequireFrozenAuthority: *telicRequireFrozenAuthority,
		SoakStopFile:                *soakStopFile,
		SoakRuntimeLimit:            time.Duration(*soakRuntimeLimitSec) * time.Second,
		SoakMaxProviderCalls:        *soakMaxProviderCalls,
		DaemonWorkspaceTools:        *daemonWorkspaceTools,
		InboxDrainAdvisory:          *inboxDrainAdvisory,
		BudgetAccountID:             *budgetAccountID,
		BudgetHardLimitMicros:       *budgetHardLimitMicros,
		BudgetReserveMicros:         *budgetReserveMicros,
		BudgetMicrosPerToken:        *budgetMicrosPerToken,
	}
	cfg.ApplyDefaults()
	cfg.Mode = runtimeModeFromInputs(*mode, cfg.RhizomeRPC, cfg.RhizomeHost, cfg.RhizomeToken, cfg.WorkspaceID, cfg.AgentID, cfg.OwnerUserID)
	if isManagedAgentProcess() {
		cfg = managedAgentEffectiveRuntimeConfig(ManagedAgentRecord{
			AgentID:     cfg.AgentID,
			Workdir:     cfg.Workdir,
			WorkspaceID: cfg.WorkspaceID,
		}, cfg)
		cfg.Mode = runtimeModeFromInputs(*mode, cfg.RhizomeRPC, cfg.RhizomeHost, cfg.RhizomeToken, cfg.WorkspaceID, cfg.AgentID, cfg.OwnerUserID)
	}
	return cfg, nil
}

func runtimeFlagSetName(args []string) string {
	for i, arg := range args {
		value := strings.TrimSpace(arg)
		if strings.HasPrefix(value, "--mode=") {
			value = strings.TrimSpace(strings.TrimPrefix(value, "--mode="))
		} else if value == "--mode" && i+1 < len(args) {
			value = strings.TrimSpace(args[i+1])
		} else {
			continue
		}
		if value == string(RuntimeModeDaemon) || value == string(RuntimeModeTUI) {
			return appCommandName + " " + value
		}
	}
	return appCommandName
}

func hideRuntimeStringDefaults(flags *flag.FlagSet) {
	privateDefaults := map[string]struct{}{
		"agent-id": {}, "budget-account-id": {}, "capabilities": {},
		"coordination-mode": {}, "disabled-tools": {}, "display-name": {},
		"group-id": {}, "llm-backend": {}, "mode": {}, "model": {},
		"model-override": {}, "owner-user-id": {}, "pinned-task-id": {},
		"protocol-version": {}, "provider-id": {}, "rhizome-host": {},
		"rhizome-rpc": {}, "role": {}, "soak-stop-file": {},
		"telic-checkout-dir": {}, "telic-oracle-cmd": {},
		"telic-target-path": {}, "workdir": {}, "workspace-id": {},
		"workspace-name": {},
	}
	flags.VisitAll(func(f *flag.Flag) {
		if _, ok := privateDefaults[f.Name]; ok {
			f.DefValue = ""
		}
	})
}

func rejectSecretCLIArgs(args []string) error {
	secretFlags := map[string]string{
		"--openai-key":         "OPENAI_API_KEY or the provider-specific environment variable",
		"--rhizome-token":      "RHIZOME_TOKEN",
		"--workspace-password": "RHIZOME_WORKSPACE_PASSWORD",
	}
	for _, arg := range args {
		name := strings.SplitN(strings.TrimSpace(arg), "=", 2)[0]
		if replacement, ok := secretFlags[name]; ok {
			return fmt.Errorf("%s is not supported because command-line secrets can leak through shell history and process listings; use %s", name, replacement)
		}
	}
	return nil
}

// applyToolLoopCompactionSetting materializes the resolved runtime
// tool-loop-compaction switch into the process-wide compaction default (TE-12).
// It runs once at startup, before any agent loop starts, so tool loops snapshot
// the correct view config per call. Other fields come from the TE-10 defaults;
// turning the flag off restores stock behavior (Enabled=false) with no
// persistent state.
func applyToolLoopCompactionSetting(cfg RuntimeConfig) {
	config := DefaultToolLoopCompactionConfig()
	config.Enabled = cfg.ToolLoopCompaction
	SetDefaultToolLoopCompaction(config)
}

func runWithConfig(cfg RuntimeConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	applyToolLoopCompactionSetting(cfg)
	if err := os.MkdirAll(cfg.Workdir, 0o755); err != nil {
		return fmt.Errorf("create workdir: %w", err)
	}
	if err := Bootstrap(cfg.Workdir); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	llm, err := NewLLM(cfg)
	if err != nil {
		return err
	}

	log.Printf("[init] mode=%s workdir=%s", cfg.Mode, cfg.Workdir)
	if cfg.Mode == RuntimeModeTUI {
		return RunTUI(context.Background(), cfg, llm, os.Stdin, os.Stdout)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runtime := NewRuntime(cfg, llm)
	defer func() {
		if runtime != nil {
			_ = runtime.Close()
		}
	}()
	if cfg.Mode == RuntimeModeDaemon {
		log.Printf("[init] rhizome=%s workspace=%s agent=%s", cfg.RhizomeRPC, cfg.WorkspaceID, cfg.AgentID)
		if err := runtime.acquireStartupIdentityLease(); err != nil {
			return err
		}

		shadow := NewShadowMonitor(cfg, llm)
		go func() {
			err := shadow.Run(sigCtx, func() RuntimeWatchdogSnapshot {
				snap, ok := runtime.safeWatchdogSnapshot(time.Now().UTC())
				if !ok {
					return RuntimeWatchdogSnapshot{
						MonitorVerdict: "stalled",
						Reason:         "primary executor mutex deadlock",
					}
				}
				return snap
			})
			if err != nil {
				log.Printf("[shadow_monitor] exited with error: %v", err)
			}
		}()
	}

	return runtime.Run(sigCtx)
}

func fatal(msg string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", msg)
	os.Exit(1)
}

func defaultOwnerUserID(local LocalRuntimeProfile, global RhizomeConnectionProfile) string {
	return defaultOwnerUserIDWithDefaults(local, BotManagerDefaults{}, global)
}

func defaultOwnerUserIDWithDefaults(local LocalRuntimeProfile, defaults BotManagerDefaults, global RhizomeConnectionProfile) string {
	return firstNonEmpty(os.Getenv("RHIZOME_OWNER_USER_ID"), local.effectiveOwnerUserID(), defaults.OwnerUserID, global.OwnerUserID)
}

func firstCapabilities(groups ...[]string) []string {
	for _, group := range groups {
		normalized := normalizeCapabilityList(group)
		if len(normalized) == 0 {
			continue
		}
		return normalized
	}
	return nil
}
