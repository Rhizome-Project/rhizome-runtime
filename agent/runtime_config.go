package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type RuntimeMode string

const (
	RuntimeModeDaemon RuntimeMode = "daemon"
	RuntimeModeTUI    RuntimeMode = "tui"
)

const (
	CoordinationModeStrict     = "strict"
	CoordinationModeTrustFirst = "trust_first"
	defaultCoordinationMode    = CoordinationModeStrict
)

const (
	defaultModel               = "gpt-5.4-mini"
	defaultProtocolVersion     = "rnar/v1"
	defaultRhizomeHostURL      = "http://127.0.0.1:8420"
	defaultWorkspaceID         = "rhizome-main"
	defaultHeartbeatInterval   = 2 * time.Minute
	defaultPlannerInterval     = 45 * time.Second
	defaultBootstrapInterval   = 5 * time.Minute
	defaultWatchdogInterval    = 90 * time.Second
	defaultMemoryRepairAfter   = 4 * time.Minute
	defaultPollTimeout         = 30 * time.Second
	defaultPollLimit           = 20
	defaultLookbackHours       = 24
	defaultUpdatesLimit        = 20
	defaultPromptDocChars      = 14000
	defaultPromptSpecChars     = 10000
	defaultMemoryBodyChars     = 4000
	defaultMemorySyncEvery     = 30 * time.Second
	defaultMemoryStalePackets  = 3
	defaultPromotionSyncBatch  = 4
	defaultSmokeCyclesPerAgent = 3
	defaultSmokeCyclesPerTask  = 6
	defaultToolLoopIterations  = 20
	defaultToolLoopCompaction  = false
	defaultProviderRetryLimit  = 2
	defaultProviderCallTimeout = 0
	defaultPlannerCycleTimeout = 10 * time.Minute
	defaultRSPRolloutPhase     = string(RSPRolloutObserveOnly)
)

type RuntimeConfig struct {
	Mode             RuntimeMode
	ProtocolVersion  string
	OpenAIKey        string
	ProviderID       string
	ModelOverride    string
	LLMBackend       string
	Model            string
	RealLLMPilot     bool
	CoordinationMode string
	Workdir          string

	RhizomeRPC                 string
	RhizomeHost                string
	RhizomeToken               string
	WorkspaceID                string
	WorkspaceName              string
	WorkspacePassword          string
	AgentID                    string
	DisplayName                string
	OwnerUserID                string
	GroupID                    string
	Role                       string
	Capabilities               []string
	HeartbeatEvery             time.Duration
	PlannerEvery               time.Duration
	BootstrapEvery             time.Duration
	WatchdogEvery              time.Duration
	PollTimeout                time.Duration
	PollLimit                  int
	LookbackHours              int
	UpdatesLimit               int
	MemorySyncEvery            time.Duration
	MaxPromotionSyncBatch      int
	RSPRolloutPhase            string
	ListenerStaleAfter         time.Duration
	RequestStaleAfter          time.Duration
	PlannerStaleAfter          time.Duration
	TaskStallAfter             time.Duration
	DurableStallAfter          time.Duration
	MemoryRepairCooldown       time.Duration
	MemoryPromotionStaleAfter  time.Duration
	MemoryStalePacketThreshold int
	MaxPromptDocChars          int
	MaxPromptSpecChars         int
	MaxResultMemoryBody        int
	MaxSmokeCyclesPerAgent     int
	MaxSmokeCyclesPerTask      int
	MaxToolLoopIterations      int
	ToolLoopCompaction         bool
	MaxProviderRetryAttempts   int
	ProviderCallTimeout        time.Duration
	PlannerCycleTimeout        time.Duration
	DisabledToolNames          []string
	PinnedTaskID               string
	// Telic Oracle Loop (experiment; see runtime_telic_loop.go). All inert unless TelicLoopEnabled.
	TelicLoopEnabled            bool
	TelicTargetPath             string // deliverable file (relative to checkout) os.Stat'd / named in the cold-start objective
	TelicCheckoutDir            string // working tree where edits land and the oracle measures
	TelicOracleCmd              string // command that builds the product + runs the oracle and prints TELIC_BUILD/PASS/TOTAL/REDDEST
	TelicRequireFrozenAuthority bool   // require TELIC_FROZEN=green before a telic lane may converge
	SoakStopFile                string
	SoakRuntimeLimit            time.Duration
	SoakMaxProviderCalls        int
	DaemonWorkspaceTools        bool
	InboxDrainAdvisory          bool
	BudgetAccountID             string
	BudgetHardLimitMicros       int64
	BudgetReserveMicros         int64
	BudgetMicrosPerToken        int64
}

func (c *RuntimeConfig) ApplyDefaults() {
	if c.Mode == "" {
		c.Mode = RuntimeModeDaemon
	}
	if c.Mode == RuntimeModeDaemon && c.RealLLMPilot {
		c.DaemonWorkspaceTools = true
	}
	c.CoordinationMode = normalizeCoordinationMode(c.CoordinationMode)
	if strings.TrimSpace(c.ProtocolVersion) == "" {
		c.ProtocolVersion = defaultProtocolVersion
	}
	c.ProviderID = strings.TrimSpace(c.ProviderID)
	c.ModelOverride = strings.TrimSpace(c.ModelOverride)
	c.BudgetAccountID = strings.TrimSpace(c.BudgetAccountID)
	c.SoakStopFile = strings.TrimSpace(c.SoakStopFile)
	c.GroupID, c.LLMBackend, c.Model = applyProviderBinding(c.ProviderID, c.ModelOverride, c.GroupID, c.LLMBackend, c.Model)
	if normalizeLLMBackend(c.LLMBackend) == "" {
		c.LLMBackend = llmBackendAuto
	} else {
		c.LLMBackend = normalizeLLMBackend(c.LLMBackend)
	}
	if strings.TrimSpace(c.Model) == "" {
		c.Model = defaultModel
	}
	if strings.TrimSpace(c.Role) == "" {
		c.Role = "generalist"
	}
	c.Capabilities = normalizeCapabilityList(c.Capabilities)
	if len(c.Capabilities) == 0 {
		c.Capabilities = []string{
			"tool.call",
			"local.shell",
			"local.fs.read",
			"local.fs.write",
			"workspace.docs",
			"workspace.memory",
			"workspace.execution",
			"workspace.messaging",
		}
	}
	if strings.TrimSpace(c.RhizomeRPC) == "" {
		c.RhizomeRPC = defaultRPCEndpoint(c.RhizomeHost)
	}
	if strings.TrimSpace(c.RhizomeHost) == "" {
		c.RhizomeHost = hostURLForRPC(c.RhizomeRPC)
	}
	if c.HeartbeatEvery <= 0 {
		c.HeartbeatEvery = defaultHeartbeatInterval
	}
	if c.PlannerEvery <= 0 {
		c.PlannerEvery = defaultPlannerInterval
	}
	if c.BootstrapEvery <= 0 {
		c.BootstrapEvery = defaultBootstrapInterval
	}
	if c.WatchdogEvery <= 0 {
		c.WatchdogEvery = defaultWatchdogInterval
	}
	if c.PollTimeout <= 0 {
		c.PollTimeout = defaultPollTimeout
	}
	if c.PollLimit <= 0 {
		c.PollLimit = defaultPollLimit
	}
	if c.LookbackHours <= 0 {
		c.LookbackHours = defaultLookbackHours
	}
	if c.UpdatesLimit <= 0 {
		c.UpdatesLimit = defaultUpdatesLimit
	}
	if c.MemorySyncEvery <= 0 {
		c.MemorySyncEvery = defaultMemorySyncEvery
	}
	if c.MaxPromotionSyncBatch <= 0 {
		c.MaxPromotionSyncBatch = defaultPromotionSyncBatch
	}
	c.RSPRolloutPhase = string(normalizeRSPRolloutPhase(c.RSPRolloutPhase))
	if c.ListenerStaleAfter <= 0 {
		c.ListenerStaleAfter = maxDuration(c.PollTimeout*3, 2*time.Minute)
	}
	if c.RequestStaleAfter <= 0 {
		c.RequestStaleAfter = maxDuration(3*20*time.Second, 2*time.Minute)
	}
	if c.PlannerStaleAfter <= 0 {
		c.PlannerStaleAfter = maxDuration(c.PlannerEvery*4, 4*time.Minute)
	}
	if c.TaskStallAfter <= 0 {
		c.TaskStallAfter = 20 * time.Minute
	}
	if c.DurableStallAfter <= 0 {
		c.DurableStallAfter = maxDuration(2*c.TaskStallAfter, 40*time.Minute)
	}
	if c.MemoryRepairCooldown <= 0 {
		c.MemoryRepairCooldown = defaultMemoryRepairAfter
	}
	if c.MemoryPromotionStaleAfter <= 0 {
		c.MemoryPromotionStaleAfter = maxDuration(c.MemorySyncEvery*3, 5*time.Minute)
	}
	if c.MemoryStalePacketThreshold <= 0 {
		c.MemoryStalePacketThreshold = defaultMemoryStalePackets
	}
	if c.MaxPromptDocChars <= 0 {
		c.MaxPromptDocChars = defaultPromptDocChars
	}
	if c.MaxPromptSpecChars <= 0 {
		c.MaxPromptSpecChars = defaultPromptSpecChars
	}
	if c.MaxResultMemoryBody <= 0 {
		c.MaxResultMemoryBody = defaultMemoryBodyChars
	}
	if c.MaxSmokeCyclesPerAgent <= 0 {
		c.MaxSmokeCyclesPerAgent = defaultSmokeCyclesPerAgent
	}
	if c.MaxSmokeCyclesPerTask <= 0 {
		c.MaxSmokeCyclesPerTask = defaultSmokeCyclesPerTask
	}
	if c.MaxToolLoopIterations <= 0 {
		c.MaxToolLoopIterations = defaultToolLoopIterations
	}
	if c.MaxProviderRetryAttempts <= 0 {
		c.MaxProviderRetryAttempts = defaultProviderRetryLimit
	}
	if c.ProviderCallTimeout < 0 {
		c.ProviderCallTimeout = defaultProviderCallTimeout
	}
	if c.PlannerCycleTimeout < 0 {
		c.PlannerCycleTimeout = 0
	}
	if c.PlannerCycleTimeout == 0 {
		if c.ProviderCallTimeout > 0 {
			c.PlannerCycleTimeout = c.ProviderCallTimeout
		} else {
			c.PlannerCycleTimeout = defaultPlannerCycleTimeout
		}
	}
	c.DisabledToolNames = uniqueTrimmedCSVStrings(c.DisabledToolNames)
	c.PinnedTaskID = strings.TrimSpace(c.PinnedTaskID)
	// WR-03 (static-audit 2026-06-02): a planner-cycle or provider-call timeout at/above the
	// task-stall window lets a single legitimately-long cycle self-trigger the stall watchdog.
	// Keep them strictly below TaskStallAfter so one slow-but-healthy cycle never reads as a
	// stall. Defaults (10m cycle < 20m stall) are unaffected; this only clamps a misconfiguration.
	if c.TaskStallAfter > 0 {
		maxCycle := c.TaskStallAfter - time.Minute
		if maxCycle <= 0 {
			maxCycle = c.TaskStallAfter / 2
		}
		if c.PlannerCycleTimeout > maxCycle {
			c.PlannerCycleTimeout = maxCycle
		}
		if c.ProviderCallTimeout > maxCycle {
			c.ProviderCallTimeout = maxCycle
		}
	}
	if runtimeBudgetLedgerConfigured(*c) {
		if c.BudgetAccountID == "" {
			c.BudgetAccountID = defaultRuntimeBudgetAccountID(c.WorkspaceID, c.AgentID)
		}
		if c.BudgetReserveMicros <= 0 {
			c.BudgetReserveMicros = defaultRuntimeBudgetReserveMicros
			if c.BudgetHardLimitMicros > 0 && c.BudgetHardLimitMicros < c.BudgetReserveMicros {
				c.BudgetReserveMicros = c.BudgetHardLimitMicros
			}
		}
		if c.BudgetMicrosPerToken <= 0 {
			c.BudgetMicrosPerToken = defaultRuntimeBudgetMicrosPerToken
		}
	}
	if strings.TrimSpace(c.DisplayName) == "" {
		c.DisplayName = strings.TrimSpace(c.AgentID)
	}
}

func (c RuntimeConfig) Validate() error {
	if strings.TrimSpace(c.Workdir) == "" {
		return errors.New("workdir is required")
	}
	if c.BudgetHardLimitMicros < 0 {
		return errors.New("budget hard limit micros cannot be negative")
	}
	if c.BudgetReserveMicros < 0 {
		return errors.New("budget reserve micros cannot be negative")
	}
	if runtimeBudgetLedgerConfigured(c) {
		if strings.TrimSpace(c.BudgetAccountID) == "" {
			return errors.New("budget account id is required when runtime budget ledger is enabled")
		}
		if c.BudgetReserveMicros <= 0 {
			return errors.New("budget reserve micros must be positive when runtime budget ledger is enabled")
		}
		if c.BudgetMicrosPerToken <= 0 {
			return errors.New("budget micros per token must be positive when runtime budget ledger is enabled")
		}
	}
	if err := validateRuntimeSoakGuardConfig(c); err != nil {
		return err
	}
	if err := validateRSPRolloutPhase(c.RSPRolloutPhase); err != nil {
		return err
	}
	if err := validateCoordinationMode(c.CoordinationMode); err != nil {
		return err
	}
	if err := validateRealLLMPilotConfig(c); err != nil {
		return err
	}

	switch c.Mode {
	case RuntimeModeDaemon:
		required := map[string]string{
			"RHIZOME_WORKSPACE_ID": c.WorkspaceID,
			"RHIZOME_AGENT_ID":     c.AgentID,
		}
		if strings.TrimSpace(c.RhizomeRPC) == "" && strings.TrimSpace(c.RhizomeHost) == "" {
			return errors.New("RHIZOME_RPC or RHIZOME_HOST is required in daemon mode")
		}
		for name, value := range required {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s is required in daemon mode", name)
			}
		}
		if strings.TrimSpace(c.RhizomeToken) == "" && strings.TrimSpace(c.OwnerUserID) == "" {
			return errors.New("RHIZOME_OWNER_USER_ID is required in daemon mode when RHIZOME_TOKEN is not set")
		}
		if c.HeartbeatEvery > runtimeIdentityLeaseMaxHeartbeat {
			return fmt.Errorf("HEARTBEAT_SEC must be <= %s in daemon mode to refresh the runtime identity lease before stale window %s", runtimeIdentityLeaseMaxHeartbeat, runtimeIdentityLeaseStaleAfter)
		}
	case RuntimeModeTUI:
		return nil
	default:
		return fmt.Errorf("unsupported mode: %s", c.Mode)
	}

	return nil
}

func normalizeCoordinationMode(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case "", CoordinationModeStrict:
		return CoordinationModeStrict
	case CoordinationModeTrustFirst, "trustfirst", "autonomy", "autonomous":
		return CoordinationModeTrustFirst
	default:
		return normalized
	}
}

func normalizeCoordinationModeOptional(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return normalizeCoordinationMode(value)
}

func validateCoordinationMode(value string) error {
	switch normalizeCoordinationMode(value) {
	case CoordinationModeStrict, CoordinationModeTrustFirst:
		return nil
	default:
		return fmt.Errorf("unsupported coordination mode: %s", strings.TrimSpace(value))
	}
}

func runtimeTrustFirst(cfg RuntimeConfig) bool {
	return normalizeCoordinationMode(cfg.CoordinationMode) == CoordinationModeTrustFirst
}

func coordinationModeLabel(cfg RuntimeConfig) string {
	return normalizeCoordinationMode(cfg.CoordinationMode)
}

func runtimeSoakGuardConfigured(c RuntimeConfig) bool {
	return strings.TrimSpace(c.SoakStopFile) != "" || c.SoakRuntimeLimit != 0 || c.SoakMaxProviderCalls != 0
}

func validateRuntimeSoakGuardConfig(c RuntimeConfig) error {
	if !runtimeSoakGuardConfigured(c) {
		return nil
	}
	reasons := make([]string, 0, 8)
	if c.Mode != RuntimeModeDaemon {
		reasons = append(reasons, "real LLM soak guard requires daemon mode")
	}
	if !c.RealLLMPilot {
		reasons = append(reasons, "real LLM soak guard requires --real-llm-pilot")
	}
	if strings.TrimSpace(c.SoakStopFile) == "" {
		reasons = append(reasons, "real LLM soak guard requires --soak-stop-file")
	}
	if c.SoakRuntimeLimit <= 0 {
		reasons = append(reasons, "real LLM soak guard requires positive --soak-runtime-limit-sec")
	}
	if c.SoakMaxProviderCalls <= 0 {
		reasons = append(reasons, "real LLM soak guard requires positive --soak-max-provider-calls")
	}
	if c.BudgetHardLimitMicros > 0 && c.BudgetReserveMicros > 0 && c.SoakMaxProviderCalls > 0 {
		if c.BudgetHardLimitMicros > int64(c.SoakMaxProviderCalls)*c.BudgetReserveMicros {
			reasons = append(reasons, "real LLM soak guard budget hard limit must not exceed max provider calls times per-call reserve")
		}
	}
	if len(reasons) > 0 {
		return fmt.Errorf("real LLM soak guard invalid: %s", strings.Join(reasons, "; "))
	}
	return nil
}

func runtimeModeFromInputs(modeFlag, rhizomeRPC, rhizomeHost, rhizomeToken, workspaceID, agentID, ownerUserID string) RuntimeMode {
	switch strings.ToLower(strings.TrimSpace(modeFlag)) {
	case "", "auto", string(RuntimeModeDaemon):
		return RuntimeModeDaemon
	case string(RuntimeModeTUI), "chat", "interactive":
		return RuntimeModeTUI
	default:
		return RuntimeMode(strings.TrimSpace(modeFlag))
	}
}

func parseCapabilitiesCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return normalizeCapabilityList(out)
}

func normalizeCapabilityList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed == "" || isPlaceholderCapability(trimmed) {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			out = append(out, trimmed)
		}
	}
	return out
}

func isPlaceholderCapability(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "-", "--", "---", "n/a", "na", "none", "null", "(none)":
		return true
	}
	if strings.Trim(normalized, "-_. ") == "" {
		return true
	}
	return strings.Contains(normalized, "\u2014") ||
		strings.Contains(normalized, "\u2013") ||
		strings.Contains(normalized, "\u00e2\u20ac") ||
		strings.Contains(normalized, "\u00c3\u00a2")
}

func maxDuration(values ...time.Duration) time.Duration {
	var best time.Duration
	for _, value := range values {
		if value > best {
			best = value
		}
	}
	return best
}
