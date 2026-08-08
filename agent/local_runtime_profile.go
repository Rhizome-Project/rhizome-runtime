package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const localRuntimeProfileFilename = "agent.runtime.json"

type RegisteredExecutorIdentity struct {
	AgentID         string   `json:"agent_id,omitempty"`
	WorkspaceID     string   `json:"workspace_id,omitempty"`
	DisplayName     string   `json:"display_name,omitempty"`
	OwnerUserID     string   `json:"owner_user_id,omitempty"`
	Role            string   `json:"role,omitempty"`
	Status          string   `json:"status,omitempty"`
	ProtocolVersion string   `json:"protocol_version,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
	ConfirmedAt     string   `json:"confirmed_at,omitempty"`
}

type LocalRuntimeProfile struct {
	Mode                       string                     `json:"mode,omitempty"`
	ProtocolVersion            string                     `json:"protocol_version,omitempty"`
	ProviderID                 string                     `json:"provider_id,omitempty"`
	ModelOverride              string                     `json:"model_override,omitempty"`
	LLMBackend                 string                     `json:"llm_backend,omitempty"`
	Model                      string                     `json:"model,omitempty"`
	RealLLMPilot               bool                       `json:"real_llm_pilot,omitempty"`
	CoordinationMode           string                     `json:"coordination_mode,omitempty"`
	RPCEndpoint                string                     `json:"rpc_endpoint,omitempty"`
	HostURL                    string                     `json:"host_url,omitempty"`
	WorkspaceID                string                     `json:"workspace_id,omitempty"`
	WorkspaceName              string                     `json:"workspace_name,omitempty"`
	WorkspacePassword          string                     `json:"workspace_password,omitempty"`
	AgentID                    string                     `json:"agent_id,omitempty"`
	DisplayName                string                     `json:"display_name,omitempty"`
	AgentToken                 string                     `json:"agent_token,omitempty"`
	OwnerUserID                string                     `json:"owner_user_id,omitempty"`
	GroupID                    string                     `json:"group_id,omitempty"`
	Role                       string                     `json:"role,omitempty"`
	Capabilities               []string                   `json:"capabilities,omitempty"`
	RegisteredExecutor         RegisteredExecutorIdentity `json:"registered_executor,omitempty"`
	HeartbeatSec               int                        `json:"heartbeat_sec,omitempty"`
	PlannerSec                 int                        `json:"planner_sec,omitempty"`
	BootstrapSec               int                        `json:"bootstrap_sec,omitempty"`
	WatchdogSec                int                        `json:"watchdog_sec,omitempty"`
	ListenerTimeoutSec         int                        `json:"listener_timeout_sec,omitempty"`
	ListenerBatch              int                        `json:"listener_batch,omitempty"`
	ListenerLookbackHours      int                        `json:"listener_lookback_hours,omitempty"`
	UpdatesLimit               int                        `json:"updates_limit,omitempty"`
	MemorySyncSec              int                        `json:"memory_sync_sec,omitempty"`
	PromotionSyncBatch         int                        `json:"promotion_sync_batch,omitempty"`
	RSPRolloutPhase            string                     `json:"rsp_rollout_phase,omitempty"`
	ListenerStaleAfterSec      int                        `json:"listener_stale_after_sec,omitempty"`
	RequestStaleAfterSec       int                        `json:"request_stale_after_sec,omitempty"`
	PlannerStaleAfterSec       int                        `json:"planner_stale_after_sec,omitempty"`
	TaskStallAfterSec          int                        `json:"task_stall_after_sec,omitempty"`
	MemoryRepairCooldownSec    int                        `json:"memory_repair_cooldown_sec,omitempty"`
	MemoryPromotionStaleSec    int                        `json:"memory_promotion_stale_sec,omitempty"`
	MemoryStalePacketThreshold int                        `json:"memory_stale_packet_threshold,omitempty"`
	MaxPromptDocChars          int                        `json:"max_prompt_doc_chars,omitempty"`
	MaxPromptSpecChars         int                        `json:"max_prompt_spec_chars,omitempty"`
	MaxResultMemoryBodyChars   int                        `json:"max_result_memory_body_chars,omitempty"`
	MaxToolLoopIterations      int                        `json:"max_tool_loop_iterations,omitempty"`
	ToolLoopCompaction         bool                       `json:"tool_loop_compaction,omitempty"`
	MaxProviderRetryAttempts   int                        `json:"max_provider_retry_attempts,omitempty"`
	ProviderCallTimeoutSec     int                        `json:"provider_call_timeout_sec,omitempty"`
	SoakStopFile               string                     `json:"soak_stop_file,omitempty"`
	SoakRuntimeLimitSec        int                        `json:"soak_runtime_limit_sec,omitempty"`
	SoakMaxProviderCalls       int                        `json:"soak_max_provider_calls,omitempty"`
	BudgetAccountID            string                     `json:"budget_account_id,omitempty"`
	BudgetHardLimitMicros      int64                      `json:"budget_hard_limit_micros,omitempty"`
	BudgetReserveMicros        int64                      `json:"budget_reserve_micros,omitempty"`
	BudgetMicrosPerToken       int64                      `json:"budget_micros_per_token,omitempty"`
	UpdatedAt                  string                     `json:"updated_at,omitempty"`
}

func localRuntimeProfilePath(workdir string) string {
	if strings.TrimSpace(workdir) == "" {
		return ""
	}
	return filepath.Join(workdir, localRuntimeProfileFilename)
}

func LoadLocalRuntimeProfile(workdir string) LocalRuntimeProfile {
	path := localRuntimeProfilePath(workdir)
	if path == "" {
		return LocalRuntimeProfile{}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return LocalRuntimeProfile{}
	}
	var profile LocalRuntimeProfile
	if err := json.Unmarshal(data, &profile); err != nil {
		return LocalRuntimeProfile{}
	}
	return normalizeLocalRuntimeProfile(profile)
}

func SaveLocalRuntimeProfile(workdir string, profile LocalRuntimeProfile) error {
	path := localRuntimeProfilePath(workdir)
	if path == "" {
		return nil
	}
	if !localRuntimeProfileRequiresManagedWriter(workdir, path) {
		return saveLocalRuntimeProfileUnmanaged(path, profile)
	}

	root := agentRuntimeConfigRoot()
	if root == "" {
		return os.ErrInvalid
	}
	return withManagerStateLock(root, true, func() error {
		raw, _, err := marshalLocalRuntimeProfileForWrite(profile)
		if err != nil {
			return err
		}
		payloadPath, err := prepareManagerStatePayloadFile(root, "mat-local-runtime-", raw, 0o600)
		if err != nil {
			return err
		}
		if err := writeLocalRuntimeMaterializationMarker(path, raw); err != nil {
			return err
		}
		return materializeManagerStateEntriesLocked(root, "save_local_runtime_profile", []managerStateMaterializeEntry{{
			TargetPath:  path,
			PayloadPath: payloadPath,
			Perm:        0o600,
		}})
	})
}

func localRuntimeProfileRequiresManagedWriter(workdir, path string) bool {
	path = cleanManagedRuntimeRoot(path)
	if path == "" {
		return false
	}
	root := agentRuntimeConfigRoot()
	if managerStatePathWithinRoot(root, path) {
		return true
	}
	registry, err := loadBotRegistryFromDisk(botRegistryPath())
	if err != nil {
		return true
	}
	return botRegistryContainsManagedWorkdir(normalizeBotRegistry(registry), workdir)
}

func saveLocalRuntimeProfileUnmanaged(path string, profile LocalRuntimeProfile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, _, err := marshalLocalRuntimeProfileForWrite(profile)
	if err != nil {
		return err
	}
	return atomicWriteFile(path, raw, 0o600)
}

func marshalLocalRuntimeProfileForWrite(profile LocalRuntimeProfile) ([]byte, LocalRuntimeProfile, error) {
	profile = normalizeLocalRuntimeProfile(profile)
	profile.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return nil, LocalRuntimeProfile{}, err
	}
	return raw, profile, nil
}

func normalizeLocalRuntimeProfile(profile LocalRuntimeProfile) LocalRuntimeProfile {
	updatedAt := strings.TrimSpace(profile.UpdatedAt)
	registeredExecutor := normalizeRegisteredExecutorIdentity(profile.RegisteredExecutor)
	protocolVersion := strings.TrimSpace(profile.ProtocolVersion)
	workspaceID := strings.TrimSpace(profile.WorkspaceID)
	agentID := strings.TrimSpace(profile.AgentID)
	displayName := strings.TrimSpace(profile.DisplayName)
	ownerUserID := strings.TrimSpace(profile.OwnerUserID)
	role := strings.TrimSpace(profile.Role)
	capabilities := uniqueTrimmedCSVStrings(profile.Capabilities)
	profile = localRuntimeProfileFromConfig(runtimeConfigFromLocalRuntimeProfile(profile))
	profile.ProtocolVersion = protocolVersion
	profile.WorkspaceID = workspaceID
	profile.AgentID = agentID
	profile.DisplayName = displayName
	profile.OwnerUserID = ownerUserID
	profile.Role = role
	profile.Capabilities = capabilities
	profile.UpdatedAt = updatedAt
	profile.RegisteredExecutor = registeredExecutor
	return profile
}

func runtimeConfigFromLocalRuntimeProfile(profile LocalRuntimeProfile) RuntimeConfig {
	return RuntimeConfig{
		Mode:                       RuntimeMode(strings.TrimSpace(profile.Mode)),
		ProtocolVersion:            profile.effectiveProtocolVersion(),
		ProviderID:                 strings.TrimSpace(profile.ProviderID),
		ModelOverride:              strings.TrimSpace(profile.ModelOverride),
		LLMBackend:                 strings.TrimSpace(profile.LLMBackend),
		Model:                      strings.TrimSpace(profile.Model),
		RealLLMPilot:               profile.RealLLMPilot,
		CoordinationMode:           normalizeCoordinationModeOptional(profile.CoordinationMode),
		RhizomeRPC:                 strings.TrimSpace(profile.RPCEndpoint),
		RhizomeHost:                strings.TrimSpace(profile.HostURL),
		WorkspaceID:                profile.effectiveWorkspaceID(),
		WorkspaceName:              strings.TrimSpace(profile.WorkspaceName),
		WorkspacePassword:          strings.TrimSpace(profile.WorkspacePassword),
		AgentID:                    profile.effectiveAgentID(),
		DisplayName:                profile.effectiveDisplayName(),
		RhizomeToken:               strings.TrimSpace(profile.AgentToken),
		OwnerUserID:                profile.effectiveOwnerUserID(),
		GroupID:                    strings.TrimSpace(profile.GroupID),
		Role:                       profile.effectiveRole(),
		Capabilities:               profile.effectiveCapabilities(),
		HeartbeatEvery:             secondsDuration(profile.HeartbeatSec),
		PlannerEvery:               secondsDuration(profile.PlannerSec),
		BootstrapEvery:             secondsDuration(profile.BootstrapSec),
		WatchdogEvery:              secondsDuration(profile.WatchdogSec),
		PollTimeout:                secondsDuration(profile.ListenerTimeoutSec),
		PollLimit:                  profile.ListenerBatch,
		LookbackHours:              profile.ListenerLookbackHours,
		UpdatesLimit:               profile.UpdatesLimit,
		MemorySyncEvery:            secondsDuration(profile.MemorySyncSec),
		MaxPromotionSyncBatch:      profile.PromotionSyncBatch,
		RSPRolloutPhase:            strings.TrimSpace(profile.RSPRolloutPhase),
		ListenerStaleAfter:         secondsDuration(profile.ListenerStaleAfterSec),
		RequestStaleAfter:          secondsDuration(profile.RequestStaleAfterSec),
		PlannerStaleAfter:          secondsDuration(profile.PlannerStaleAfterSec),
		TaskStallAfter:             secondsDuration(profile.TaskStallAfterSec),
		MemoryRepairCooldown:       secondsDuration(profile.MemoryRepairCooldownSec),
		MemoryPromotionStaleAfter:  secondsDuration(profile.MemoryPromotionStaleSec),
		MemoryStalePacketThreshold: profile.MemoryStalePacketThreshold,
		MaxPromptDocChars:          profile.MaxPromptDocChars,
		MaxPromptSpecChars:         profile.MaxPromptSpecChars,
		MaxResultMemoryBody:        profile.MaxResultMemoryBodyChars,
		MaxToolLoopIterations:      profile.MaxToolLoopIterations,
		ToolLoopCompaction:         profile.ToolLoopCompaction,
		MaxProviderRetryAttempts:   profile.MaxProviderRetryAttempts,
		ProviderCallTimeout:        secondsDuration(profile.ProviderCallTimeoutSec),
		SoakStopFile:               strings.TrimSpace(profile.SoakStopFile),
		SoakRuntimeLimit:           secondsDuration(profile.SoakRuntimeLimitSec),
		SoakMaxProviderCalls:       profile.SoakMaxProviderCalls,
		BudgetAccountID:            strings.TrimSpace(profile.BudgetAccountID),
		BudgetHardLimitMicros:      profile.BudgetHardLimitMicros,
		BudgetReserveMicros:        profile.BudgetReserveMicros,
		BudgetMicrosPerToken:       profile.BudgetMicrosPerToken,
	}
}

func localRuntimeProfileFromConfig(cfg RuntimeConfig) LocalRuntimeProfile {
	cfg.ApplyDefaults()
	return LocalRuntimeProfile{
		Mode:                       string(cfg.Mode),
		ProtocolVersion:            cfg.ProtocolVersion,
		ProviderID:                 cfg.ProviderID,
		ModelOverride:              cfg.ModelOverride,
		LLMBackend:                 cfg.LLMBackend,
		Model:                      cfg.Model,
		RealLLMPilot:               cfg.RealLLMPilot,
		CoordinationMode:           cfg.CoordinationMode,
		RPCEndpoint:                cfg.RhizomeRPC,
		HostURL:                    cfg.RhizomeHost,
		WorkspaceID:                cfg.WorkspaceID,
		WorkspaceName:              cfg.WorkspaceName,
		WorkspacePassword:          cfg.WorkspacePassword,
		AgentID:                    cfg.AgentID,
		DisplayName:                cfg.DisplayName,
		AgentToken:                 cfg.RhizomeToken,
		OwnerUserID:                cfg.OwnerUserID,
		GroupID:                    cfg.GroupID,
		Role:                       cfg.Role,
		Capabilities:               append([]string(nil), cfg.Capabilities...),
		HeartbeatSec:               durationSeconds(cfg.HeartbeatEvery),
		PlannerSec:                 durationSeconds(cfg.PlannerEvery),
		BootstrapSec:               durationSeconds(cfg.BootstrapEvery),
		WatchdogSec:                durationSeconds(cfg.WatchdogEvery),
		ListenerTimeoutSec:         durationSeconds(cfg.PollTimeout),
		ListenerBatch:              cfg.PollLimit,
		ListenerLookbackHours:      cfg.LookbackHours,
		UpdatesLimit:               cfg.UpdatesLimit,
		MemorySyncSec:              durationSeconds(cfg.MemorySyncEvery),
		PromotionSyncBatch:         cfg.MaxPromotionSyncBatch,
		RSPRolloutPhase:            cfg.RSPRolloutPhase,
		ListenerStaleAfterSec:      durationSeconds(cfg.ListenerStaleAfter),
		RequestStaleAfterSec:       durationSeconds(cfg.RequestStaleAfter),
		PlannerStaleAfterSec:       durationSeconds(cfg.PlannerStaleAfter),
		TaskStallAfterSec:          durationSeconds(cfg.TaskStallAfter),
		MemoryRepairCooldownSec:    durationSeconds(cfg.MemoryRepairCooldown),
		MemoryPromotionStaleSec:    durationSeconds(cfg.MemoryPromotionStaleAfter),
		MemoryStalePacketThreshold: cfg.MemoryStalePacketThreshold,
		MaxPromptDocChars:          cfg.MaxPromptDocChars,
		MaxPromptSpecChars:         cfg.MaxPromptSpecChars,
		MaxResultMemoryBodyChars:   cfg.MaxResultMemoryBody,
		MaxToolLoopIterations:      cfg.MaxToolLoopIterations,
		ToolLoopCompaction:         cfg.ToolLoopCompaction,
		MaxProviderRetryAttempts:   cfg.MaxProviderRetryAttempts,
		ProviderCallTimeoutSec:     durationSeconds(cfg.ProviderCallTimeout),
		SoakStopFile:               cfg.SoakStopFile,
		SoakRuntimeLimitSec:        durationSeconds(cfg.SoakRuntimeLimit),
		SoakMaxProviderCalls:       cfg.SoakMaxProviderCalls,
		BudgetAccountID:            cfg.BudgetAccountID,
		BudgetHardLimitMicros:      cfg.BudgetHardLimitMicros,
		BudgetReserveMicros:        cfg.BudgetReserveMicros,
		BudgetMicrosPerToken:       cfg.BudgetMicrosPerToken,
	}
}

func normalizeRegisteredExecutorIdentity(identity RegisteredExecutorIdentity) RegisteredExecutorIdentity {
	identity.AgentID = strings.TrimSpace(identity.AgentID)
	identity.WorkspaceID = strings.TrimSpace(identity.WorkspaceID)
	identity.DisplayName = strings.TrimSpace(identity.DisplayName)
	identity.OwnerUserID = strings.TrimSpace(identity.OwnerUserID)
	identity.Role = strings.TrimSpace(identity.Role)
	identity.Status = strings.TrimSpace(identity.Status)
	identity.ProtocolVersion = strings.TrimSpace(identity.ProtocolVersion)
	identity.ConfirmedAt = strings.TrimSpace(identity.ConfirmedAt)
	identity.Capabilities = uniqueTrimmedCSVStrings(identity.Capabilities)
	return identity
}

func (profile LocalRuntimeProfile) effectiveOwnerUserID() string {
	return firstNonEmpty(profile.RegisteredExecutor.OwnerUserID, profile.OwnerUserID)
}

func (profile LocalRuntimeProfile) effectiveWorkspaceID() string {
	return firstNonEmpty(profile.RegisteredExecutor.WorkspaceID, profile.WorkspaceID)
}

func (profile LocalRuntimeProfile) effectiveAgentID() string {
	return firstNonEmpty(profile.RegisteredExecutor.AgentID, profile.AgentID)
}

func (profile LocalRuntimeProfile) effectiveDisplayName() string {
	return firstNonEmpty(profile.RegisteredExecutor.DisplayName, profile.DisplayName)
}

func (profile LocalRuntimeProfile) effectiveProtocolVersion() string {
	return firstNonEmpty(profile.RegisteredExecutor.ProtocolVersion, profile.ProtocolVersion)
}

func (profile LocalRuntimeProfile) effectiveRole() string {
	return firstNonEmpty(profile.RegisteredExecutor.Role, profile.Role)
}

func (profile LocalRuntimeProfile) effectiveCapabilities() []string {
	return firstCapabilities(profile.RegisteredExecutor.Capabilities, profile.Capabilities)
}

func registeredExecutorFromAgent(agent AgentRecord) RegisteredExecutorIdentity {
	if strings.TrimSpace(agent.AgentID) == "" {
		return RegisteredExecutorIdentity{}
	}
	return normalizeRegisteredExecutorIdentity(RegisteredExecutorIdentity{
		AgentID:         agent.AgentID,
		WorkspaceID:     agent.WorkspaceID,
		DisplayName:     agent.DisplayName,
		OwnerUserID:     agent.OwnerUserID,
		Role:            agent.Role,
		Status:          agent.Status,
		ProtocolVersion: agent.ProtocolVersion,
		Capabilities:    append([]string(nil), agent.Capabilities...),
		ConfirmedAt:     firstNonEmpty(agent.UpdatedAt, agent.CreatedAt),
	})
}

func durationSeconds(value time.Duration) int {
	if value <= 0 {
		return 0
	}
	return int(value / time.Second)
}

func secondsDuration(value int) time.Duration {
	if value <= 0 {
		return 0
	}
	return time.Duration(value) * time.Second
}

func applyRegisterResultToConfig(cfg *RuntimeConfig, client *RhizomeClient, registered AgentRegisterResult) {
	if cfg == nil {
		return
	}
	if workspaceID := strings.TrimSpace(registered.WorkspaceID); workspaceID != "" {
		cfg.WorkspaceID = workspaceID
	}
	if workspaceName := strings.TrimSpace(registered.WorkspaceName); workspaceName != "" {
		cfg.WorkspaceName = workspaceName
	}
	if hostURL := strings.TrimSpace(registered.HostURL); hostURL != "" {
		cfg.RhizomeHost = hostURL
		cfg.RhizomeRPC = defaultRPCEndpoint(hostURL)
	} else if strings.TrimSpace(cfg.RhizomeRPC) == "" && strings.TrimSpace(cfg.RhizomeHost) != "" {
		cfg.RhizomeRPC = defaultRPCEndpoint(cfg.RhizomeHost)
	}
	if token := strings.TrimSpace(registered.Token); token != "" {
		cfg.RhizomeToken = token
	}
	if client != nil {
		client.SetEndpoint(cfg.RhizomeRPC)
		client.SetToken(cfg.RhizomeToken)
	}
}

func persistRuntimeProfiles(workdir string, cfg RuntimeConfig, registered AgentRegisterResult, bootstrap *BootstrapResult) error {
	cfg.ApplyDefaults()
	if isManagedAgentProcess() {
		cfg = managedAgentEffectiveRuntimeConfig(ManagedAgentRecord{
			AgentID:     cfg.AgentID,
			Workdir:     workdir,
			WorkspaceID: cfg.WorkspaceID,
		}, cfg)
	}
	workspaceName := firstNonEmpty(cfg.WorkspaceName, registered.WorkspaceName)
	if bootstrap != nil {
		workspaceName = firstNonEmpty(workspaceName, bootstrap.Snapshot.Workspace.Title)
	}

	root := agentRuntimeConfigRoot()
	if root == "" {
		return os.ErrInvalid
	}
	return withManagerStateLock(root, true, func() error {
		existing := LoadLocalRuntimeProfile(workdir)
		localProfile := localRuntimeProfileFromConfig(cfg)
		localProfile.WorkspaceName = workspaceName
		localProfile.AgentID = firstNonEmpty(cfg.AgentID, registered.AgentID, registered.Agent.AgentID)
		localProfile.DisplayName = firstNonEmpty(cfg.DisplayName, registered.DisplayName, registered.Agent.DisplayName)
		localProfile.AgentToken = firstNonEmpty(cfg.RhizomeToken, registered.Token)
		localProfile.WorkspaceID = firstNonEmpty(cfg.WorkspaceID, registered.WorkspaceID)
		localProfile.HostURL = firstNonEmpty(cfg.RhizomeHost, registered.HostURL, hostURLForRPC(cfg.RhizomeRPC))
		localProfile.RPCEndpoint = firstNonEmpty(cfg.RhizomeRPC, defaultRPCEndpoint(localProfile.HostURL))
		localProfile.OwnerUserID = firstNonEmpty(cfg.OwnerUserID, registered.Agent.OwnerUserID)
		localProfile.RegisteredExecutor = existing.RegisteredExecutor
		if registeredExecutor := registeredExecutorFromAgent(registered.Agent); strings.TrimSpace(registeredExecutor.AgentID) != "" {
			localProfile.RegisteredExecutor = registeredExecutor
		}
		rhizomeProfile := RhizomeConnectionProfile{
			RPCEndpoint:       localProfile.RPCEndpoint,
			HostURL:           localProfile.HostURL,
			WorkspaceID:       localProfile.WorkspaceID,
			WorkspaceName:     localProfile.WorkspaceName,
			WorkspacePassword: cfg.WorkspacePassword,
			AgentID:           localProfile.AgentID,
			AgentToken:        localProfile.AgentToken,
			OwnerUserID:       localProfile.effectiveOwnerUserID(),
		}
		record := managedAgentRecordFromConfig(cfg)

		localRaw, _, err := marshalLocalRuntimeProfileForWrite(localProfile)
		if err != nil {
			return err
		}
		rhizomeRaw, _, err := marshalRhizomeProfileForWrite(rhizomeProfile)
		if err != nil {
			return err
		}

		entries := make([]managerStateMaterializeEntry, 0, 3)
		rhizomePayload, err := prepareManagerStatePayloadFile(root, "mat-rhizome-", rhizomeRaw, 0o600)
		if err != nil {
			return err
		}
		entries = append(entries, managerStateMaterializeEntry{
			TargetPath:  rhizomeProfilePath(),
			PayloadPath: rhizomePayload,
			Perm:        0o600,
		})

		if strings.TrimSpace(record.AgentID) != "" && strings.TrimSpace(record.Workdir) != "" {
			registry, err := loadBotRegistryFromDisk(botRegistryPath())
			if err != nil {
				return err
			}
			registry = normalizeBotRegistry(registry)
			upsertManagedAgentInRegistry(&registry, record)
			registryRaw, _, err := marshalBotRegistryForWrite(registry)
			if err != nil {
				return err
			}
			registryPayload, err := prepareManagerStatePayloadFile(root, "mat-registry-", registryRaw, 0o600)
			if err != nil {
				return err
			}
			entries = append(entries, managerStateMaterializeEntry{
				TargetPath:  botRegistryPath(),
				PayloadPath: registryPayload,
				Perm:        0o600,
			})
		}

		localPayload, err := prepareManagerStatePayloadFile(root, "mat-local-runtime-", localRaw, 0o600)
		if err != nil {
			return err
		}
		if err := writeLocalRuntimeMaterializationMarker(localRuntimeProfilePath(workdir), localRaw); err != nil {
			return err
		}
		entries = append(entries, managerStateMaterializeEntry{
			TargetPath:  localRuntimeProfilePath(workdir),
			PayloadPath: localPayload,
			Perm:        0o600,
		})

		return materializeManagerStateEntriesLocked(root, "persist_runtime_profiles", entries)
	})
}
