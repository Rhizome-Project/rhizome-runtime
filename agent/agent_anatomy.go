package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	agentAnatomySchemaV1 = "rhizome.agent_anatomy.v1"
	agentAnatomyFilename = "agent.anatomy.json"

	heartbeatCadenceWhileClaimed = "while_claimed"

	// Layer A — constitution defaults / clamps.
	constitutionDefaultMaxRules    = 7
	constitutionMaxRulesCeiling    = 12
	constitutionDefaultMinEvidence = 3
	constitutionMinEvidenceCeiling = 10
	constitutionDefaultPromotion   = "evidence_or_seed"

	// Layer B — reflection channel cap clamp.
	reflectionChannelCapCeiling = 12

	// Layer C — system_sensor (workspace-awareness) heartbeat.
	heartbeatKindSystemSensing = "system_sensing"
	systemSensingMemoryLane    = "system_sensing"
	systemObservationContract  = "system_observation_v1"

	// Layer D — strategy_synthesis (rule consolidation) heartbeat.
	heartbeatKindStrategySynthesis    = "strategy_synthesis"
	strategySynthesisContract         = "strategy_synthesis_v1"
	heartbeatKindGlobalProgressReview = "global_progress_review"
	globalProgressReviewMemoryLane    = "global_progress_review"
	globalProgressReviewContract      = "global_progress_review_v1"
)

// constitutionPromotionModes is the validated whitelist for
// memory.constitution.promotion.
var constitutionPromotionModes = map[string]struct{}{
	"disabled":         {},
	"seed_only":        {},
	"evidence_or_seed": {},
}

// constitutionRuleKinds is the validated whitelist for seed rule kinds.
var constitutionRuleKinds = map[string]struct{}{
	"procedure":      {},
	"anti_procedure": {},
	"invariant":      {},
}

func normalizeConstitutionPromotion(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if _, ok := constitutionPromotionModes[mode]; ok {
		return mode
	}
	return ""
}

func normalizeConstitutionRuleKind(kind string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if _, ok := constitutionRuleKinds[kind]; ok {
		return kind
	}
	return ""
}

type AgentAnatomyConfig struct {
	Schema      string                  `json:"schema,omitempty"`
	ProfileID   string                  `json:"profile_id,omitempty"`
	Preset      string                  `json:"preset,omitempty"`
	Concurrency AgentAnatomyConcurrency `json:"concurrency,omitempty"`
	Memory      AgentAnatomyMemory      `json:"memory,omitempty"`
	Heartbeats  []AgentHeartbeatSpec    `json:"heartbeats,omitempty"`
	UpdatedAt   string                  `json:"updated_at,omitempty"`
}

type AgentAnatomyConcurrency struct {
	MaxParallelInternalSessions int `json:"max_parallel_internal_sessions,omitempty"`
	MaxLLMSessions              int `json:"max_llm_sessions,omitempty"`
	MaxBrowserSessions          int `json:"max_browser_sessions,omitempty"`
}

type AgentAnatomyMemory struct {
	Enabled              *bool                    `json:"enabled,omitempty"`
	SharedScope          string                   `json:"shared_scope,omitempty"`
	Lanes                []string                 `json:"lanes,omitempty"`
	PromotionPolicy      string                   `json:"promotion_policy,omitempty"`
	Constitution         AgentAnatomyConstitution `json:"constitution,omitempty"`
	ReflectionChannelCap int                      `json:"reflection_channel_cap,omitempty"`
}

// AgentAnatomyConstitution configures the always-on, unanchored behavioral rule
// tier (Layer A of the autonomy harness). When disabled the memory packet is
// byte-identical to legacy behavior.
type AgentAnatomyConstitution struct {
	Enabled     *bool                          `json:"enabled,omitempty"`
	MaxRules    int                            `json:"max_rules,omitempty"`
	MinEvidence int                            `json:"min_evidence,omitempty"`
	Promotion   string                         `json:"promotion,omitempty"`
	SeedRules   []AgentAnatomyConstitutionRule `json:"seed_rules,omitempty"`
}

// AgentAnatomyConstitutionRule is an operator-authored invariant that is always
// surfaced (subject to the max_rules cap), ranked ahead of derived rules.
type AgentAnatomyConstitutionRule struct {
	ID       string `json:"id,omitempty"`
	Rule     string `json:"rule,omitempty"`
	Kind     string `json:"kind,omitempty"`
	Priority int    `json:"priority,omitempty"`
}

type AgentHeartbeatSpec struct {
	ID                string                              `json:"id,omitempty"`
	Kind              string                              `json:"kind,omitempty"`
	Cadence           string                              `json:"cadence,omitempty"`
	CadenceSec        int                                 `json:"cadence_sec,omitempty"`
	Priority          int                                 `json:"priority,omitempty"`
	Enabled           *bool                               `json:"enabled,omitempty"`
	Triggers          []string                            `json:"triggers,omitempty"`
	Locks             []string                            `json:"locks,omitempty"`
	ToolSuites        []string                            `json:"tool_suites,omitempty"`
	ContextSelectors  []string                            `json:"context_selectors,omitempty"`
	OutputContracts   []string                            `json:"output_contracts,omitempty"`
	PromotionSignals  []string                            `json:"promotion_signals,omitempty"`
	MaxParallel       int                                 `json:"max_parallel,omitempty"`
	MaxToolIterations int                                 `json:"max_tool_iterations,omitempty"`
	Objective         string                              `json:"objective,omitempty"`
	Instructions      []string                            `json:"instructions,omitempty"`
	MemoryLanes       []string                            `json:"memory_lanes,omitempty"`
	ActiveMemory      *AgentHeartbeatActiveMemorySpec     `json:"active_memory,omitempty"`
	WillPolicy        *AgentHeartbeatWillPolicySpec       `json:"will_policy,omitempty"`
	Notes             []string                            `json:"notes,omitempty"`
	EvidenceContract  *AgentHeartbeatEvidenceContractSpec `json:"evidence_contract,omitempty"`
	VisualAudit       *AgentHeartbeatVisualAuditSpec      `json:"visual_audit,omitempty"`
}

type AgentHeartbeatActiveMemorySpec struct {
	Lane                    string `json:"lane,omitempty"`
	MaxEntries              int    `json:"max_entries,omitempty"`
	IncludeSessionSummaries *bool  `json:"include_session_summaries,omitempty"`
	IncludeBacklog          *bool  `json:"include_backlog,omitempty"`
}

type AgentHeartbeatWillPolicySpec struct {
	Enabled           *bool    `json:"enabled,omitempty"`
	AllowedActions    []string `json:"allowed_actions,omitempty"`
	MaxDirectives     int      `json:"max_directives,omitempty"`
	RequiresEvidence  *bool    `json:"requires_evidence,omitempty"`
	PublishVisibility string   `json:"publish_visibility,omitempty"`
}

type AgentHeartbeatEvidenceContractSpec struct {
	Dimensions            []AgentHeartbeatEvidenceDimensionSpec    `json:"dimensions,omitempty"`
	States                []AgentHeartbeatEvidenceStateSpec        `json:"states,omitempty"`
	Checks                []string                                 `json:"checks,omitempty"`
	ArtifactRequirements  []string                                 `json:"artifact_requirements,omitempty"`
	RequiredToolArtifacts []AgentHeartbeatRequiredToolArtifactSpec `json:"required_tool_artifacts,omitempty"`
}

type AgentHeartbeatRequiredToolArtifactSpec struct {
	Tool            string `json:"tool,omitempty"`
	ContractVersion string `json:"contract_version,omitempty"`
	Capability      string `json:"capability,omitempty"`
	ToolSuite       string `json:"tool_suite,omitempty"`
	When            string `json:"when,omitempty"`
	Purpose         string `json:"purpose,omitempty"`
	BlockerGuidance string `json:"blocker_guidance,omitempty"`
}

type AgentHeartbeatEvidenceDimensionSpec struct {
	ID      string `json:"id,omitempty"`
	Kind    string `json:"kind,omitempty"`
	Label   string `json:"label,omitempty"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
	Purpose string `json:"purpose,omitempty"`
}

type AgentHeartbeatEvidenceStateSpec struct {
	ID                   string `json:"id,omitempty"`
	Label                string `json:"label,omitempty"`
	RequiredState        string `json:"required_state,omitempty"`
	EvidenceRequired     *bool  `json:"evidence_required,omitempty"`
	RealUserQuestion     string `json:"real_user_question,omitempty"`
	ExpectedEvidenceKind string `json:"expected_evidence_kind,omitempty"`
}

type AgentHeartbeatVisualAuditSpec struct {
	Viewports            []AgentHeartbeatVisualAuditViewportSpec `json:"viewports,omitempty"`
	Scenarios            []AgentHeartbeatVisualAuditScenarioSpec `json:"scenarios,omitempty"`
	Checks               []string                                `json:"checks,omitempty"`
	ArtifactRequirements []string                                `json:"artifact_requirements,omitempty"`
}

type AgentHeartbeatVisualAuditViewportSpec struct {
	ID      string `json:"id,omitempty"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
	Purpose string `json:"purpose,omitempty"`
}

type AgentHeartbeatVisualAuditScenarioSpec struct {
	ID                   string `json:"id,omitempty"`
	Label                string `json:"label,omitempty"`
	RequiredState        string `json:"required_state,omitempty"`
	ScreenshotRequired   *bool  `json:"screenshot_required,omitempty"`
	RealUserQuestion     string `json:"real_user_question,omitempty"`
	ExpectedEvidenceKind string `json:"expected_evidence_kind,omitempty"`
}

func agentAnatomyPath(workdir string) string {
	if strings.TrimSpace(workdir) == "" {
		return ""
	}
	return filepath.Join(workdir, agentAnatomyFilename)
}

func LoadAgentAnatomyConfig(workdir string, profile AgentProfile) AgentAnatomyConfig {
	config, err := ReadAgentAnatomyConfig(workdir, profile)
	if err != nil {
		return DefaultAgentAnatomyConfig(profile)
	}
	return config
}

func ReadAgentAnatomyConfig(workdir string, profile AgentProfile) (AgentAnatomyConfig, error) {
	path := agentAnatomyPath(workdir)
	if path == "" {
		return DefaultAgentAnatomyConfig(profile), nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return DefaultAgentAnatomyConfig(profile), nil
		}
		return AgentAnatomyConfig{}, err
	}
	return ReadAgentAnatomyConfigFile(path, profile)
}

func ReadAgentAnatomyConfigFile(path string, profile AgentProfile) (AgentAnatomyConfig, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return DefaultAgentAnatomyConfig(profile), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return AgentAnatomyConfig{}, err
	}
	var config AgentAnatomyConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return AgentAnatomyConfig{}, fmt.Errorf("read agent anatomy: %w", err)
	}
	if err := validateAgentAnatomyRaw(config); err != nil {
		return AgentAnatomyConfig{}, err
	}
	config = NormalizeAgentAnatomyConfig(config, profile)
	if err := config.Validate(); err != nil {
		return AgentAnatomyConfig{}, err
	}
	return config, nil
}

func SaveAgentAnatomyConfig(workdir string, config AgentAnatomyConfig, profile AgentProfile) error {
	path := agentAnatomyPath(workdir)
	if path == "" {
		return nil
	}
	raw, _, err := marshalAgentAnatomyConfigForWrite(config, profile)
	if err != nil {
		return err
	}
	if !agentAnatomyRequiresManagedWriter(workdir, path) {
		return saveAgentAnatomyRaw(path, raw)
	}

	root := agentRuntimeConfigRoot()
	if root == "" {
		return os.ErrInvalid
	}
	return withManagerStateLock(root, true, func() error {
		payloadPath, err := prepareManagerStatePayloadFile(root, "mat-agent-anatomy-", raw, 0o600)
		if err != nil {
			return err
		}
		if err := writeLocalRuntimeMaterializationMarker(path, raw); err != nil {
			return err
		}
		if err := materializeManagerStateEntriesLocked(root, "save_agent_anatomy", []managerStateMaterializeEntry{{
			TargetPath:  path,
			PayloadPath: payloadPath,
			Perm:        0o600,
		}}); err != nil {
			return err
		}
		return nil
	})
}

func agentAnatomyRequiresManagedWriter(workdir, path string) bool {
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

func marshalAgentAnatomyConfigForWrite(config AgentAnatomyConfig, profile AgentProfile) ([]byte, AgentAnatomyConfig, error) {
	config = NormalizeAgentAnatomyConfig(config, profile)
	if err := config.Validate(); err != nil {
		return nil, AgentAnatomyConfig{}, err
	}
	config.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, AgentAnatomyConfig{}, err
	}
	return raw, config, nil
}

func saveAgentAnatomyRaw(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return atomicWriteFile(path, raw, 0o600)
}

func SaveAgentAnatomyConfigFile(path string, config AgentAnatomyConfig, profile AgentProfile) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	raw, _, err := marshalAgentAnatomyConfigForWrite(config, profile)
	if err != nil {
		return err
	}
	return saveAgentAnatomyRaw(path, raw)
}

func DefaultAgentAnatomyConfig(profile AgentProfile) AgentAnatomyConfig {
	return NormalizeAgentAnatomyConfig(defaultAgentAnatomyBase(profile), profile)
}

func DefaultAgentAnatomyConfigForPreset(profile AgentProfile, preset string) AgentAnatomyConfig {
	preset = normalizeAgentAnatomyPreset(preset)
	if preset == "" {
		return DefaultAgentAnatomyConfig(profile)
	}
	return NormalizeAgentAnatomyConfig(defaultAgentAnatomyBaseForPreset(profile, preset), profile)
}

func NormalizeAgentAnatomyConfig(config AgentAnatomyConfig, profile AgentProfile) AgentAnatomyConfig {
	defaults := defaultAgentAnatomyBase(profile)
	if preset := normalizeAgentAnatomyPreset(config.Preset); preset != "" {
		defaults = defaultAgentAnatomyBaseForPreset(profile, preset)
		config.Preset = preset
	}
	config.Schema = firstNonEmpty(strings.TrimSpace(config.Schema), defaults.Schema)
	config.ProfileID = firstNonEmpty(strings.TrimSpace(config.ProfileID), defaults.ProfileID)
	config.Preset = firstNonEmpty(strings.TrimSpace(config.Preset), defaults.Preset)
	config.UpdatedAt = strings.TrimSpace(config.UpdatedAt)

	if config.Concurrency.MaxParallelInternalSessions <= 0 {
		config.Concurrency.MaxParallelInternalSessions = defaults.Concurrency.MaxParallelInternalSessions
	}
	if config.Concurrency.MaxLLMSessions <= 0 {
		config.Concurrency.MaxLLMSessions = defaults.Concurrency.MaxLLMSessions
	}
	if config.Concurrency.MaxBrowserSessions <= 0 {
		config.Concurrency.MaxBrowserSessions = defaults.Concurrency.MaxBrowserSessions
	}
	if config.Memory.Enabled == nil {
		config.Memory.Enabled = defaults.Memory.Enabled
	}
	config.Memory.SharedScope = firstNonEmpty(strings.TrimSpace(config.Memory.SharedScope), defaults.Memory.SharedScope)
	config.Memory.Lanes = uniqueTrimmedCSVStrings(append(defaults.Memory.Lanes, config.Memory.Lanes...))
	config.Memory.PromotionPolicy = firstNonEmpty(strings.TrimSpace(config.Memory.PromotionPolicy), defaults.Memory.PromotionPolicy)
	config.Memory.Constitution = normalizeAgentAnatomyConstitution(config.Memory.Constitution, defaults.Memory.Constitution)
	config.Memory.ReflectionChannelCap = clampInt(config.Memory.ReflectionChannelCap, 0, reflectionChannelCapCeiling)
	config.Heartbeats = mergeAgentHeartbeatSpecs(defaults.Heartbeats, config.Heartbeats)
	return config
}

// normalizeAgentAnatomyConstitution fills defaults/clamps for the constitution
// block. When the block is disabled (the default) it is left structurally inert.
func normalizeAgentAnatomyConstitution(config, defaults AgentAnatomyConstitution) AgentAnatomyConstitution {
	if config.Enabled == nil {
		config.Enabled = defaults.Enabled
	}
	if config.MaxRules <= 0 {
		config.MaxRules = firstPositiveInt(defaults.MaxRules, constitutionDefaultMaxRules)
	}
	config.MaxRules = clampInt(config.MaxRules, 0, constitutionMaxRulesCeiling)
	if config.MinEvidence <= 0 {
		config.MinEvidence = firstPositiveInt(defaults.MinEvidence, constitutionDefaultMinEvidence)
	}
	config.MinEvidence = clampInt(config.MinEvidence, 1, constitutionMinEvidenceCeiling)
	config.Promotion = firstNonEmpty(normalizeConstitutionPromotion(config.Promotion), normalizeConstitutionPromotion(defaults.Promotion), constitutionDefaultPromotion)
	config.SeedRules = normalizeConstitutionSeedRules(config.SeedRules)
	return config
}

// normalizeConstitutionSeedRules trims, drops empty rules, normalizes kinds, and
// dedupes by ID (falling back to rule text when ID is empty).
func normalizeConstitutionSeedRules(rules []AgentAnatomyConstitutionRule) []AgentAnatomyConstitutionRule {
	if len(rules) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]AgentAnatomyConstitutionRule, 0, len(rules))
	for _, rule := range rules {
		rule.ID = strings.TrimSpace(rule.ID)
		rule.Rule = strings.TrimSpace(rule.Rule)
		if rule.Rule == "" {
			continue
		}
		rule.Kind = firstNonEmpty(normalizeConstitutionRuleKind(rule.Kind), "invariant")
		dedupeKey := rule.ID
		if dedupeKey == "" {
			dedupeKey = "text:" + rule.Rule
		}
		if _, ok := seen[dedupeKey]; ok {
			continue
		}
		seen[dedupeKey] = struct{}{}
		out = append(out, rule)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func defaultAgentAnatomyBase(profile AgentProfile) AgentAnatomyConfig {
	preset := defaultAgentAnatomyPresetForProfile(profile)
	return defaultAgentAnatomyBaseForPreset(profile, preset)
}

func defaultAgentAnatomyBaseForPreset(profile AgentProfile, preset string) AgentAnatomyConfig {
	preset = normalizeAgentAnatomyPreset(preset)
	if preset == "" {
		preset = "generalist"
	}
	browserSessions := 0
	maxLLMSessions := 1
	if preset == "ui_ux_reality_critic" {
		browserSessions = 1
	}
	if preset == "strategist" || preset == "service_factory_operator" {
		maxLLMSessions = 2
	}
	lanes := []string{"working_notes", "self_check", "role_backlog", "promoted_refs"}
	if preset == "ui_ux_reality_critic" {
		lanes = append(lanes, "visual_findings", "design_sensemaking")
	}
	if preset == "strategist" || preset == "service_factory_operator" {
		lanes = append(lanes, "opportunity_map", "project_sensemaking")
	}
	heartbeats := []AgentHeartbeatSpec{
		defaultActiveTaskHeartbeat(),
		defaultLoopSelfCheckHeartbeat(),
		defaultPersonalBacklogArbiterHeartbeat(),
		defaultActionRequestPromoterHeartbeat(),
	}
	if preset == "ui_ux_reality_critic" {
		heartbeats = append(heartbeats, defaultGlobalProgressReviewHeartbeat(preset))
		heartbeats = append(heartbeats, defaultUIDesignGlobalReflectionHeartbeat())
		heartbeats = append(heartbeats, defaultVisualProductAuditHeartbeat())
	}
	if preset == "strategist" || preset == "service_factory_operator" {
		heartbeats = append(heartbeats, defaultGlobalProgressReviewHeartbeat(preset))
		heartbeats = append(heartbeats, defaultProjectRoleInitiativeHeartbeat(preset))
		// Layer D consolidation is default-on for the rule-stewarding presets.
		synthesis := defaultStrategySynthesisHeartbeat()
		synthesisEnabled := true
		synthesis.Enabled = &synthesisEnabled
		heartbeats = append(heartbeats, synthesis)
	}
	if preset == "service_factory_operator" {
		heartbeats = append(heartbeats, defaultPortfolioScoutHeartbeat())
		heartbeats = append(heartbeats, defaultDeployMonetizationVigilanceHeartbeat())
	}
	if preset == "integrator" || preset == "reviewer_qa" {
		if preset == "reviewer_qa" {
			heartbeats = append(heartbeats, defaultGlobalProgressReviewHeartbeat(preset))
		}
		heartbeats = append(heartbeats, defaultPatchQueueVigilanceHeartbeat(preset))
	}
	return AgentAnatomyConfig{
		Schema:    agentAnatomySchemaV1,
		ProfileID: defaultAgentAnatomyProfileID(profile),
		Preset:    preset,
		Concurrency: AgentAnatomyConcurrency{
			MaxParallelInternalSessions: 3,
			MaxLLMSessions:              maxLLMSessions,
			MaxBrowserSessions:          browserSessions,
		},
		Memory: AgentAnatomyMemory{
			Enabled:         boolPtr(true),
			SharedScope:     "agent_local_shared_memory",
			Lanes:           uniqueTrimmedCSVStrings(lanes),
			PromotionPolicy: "promote_only_actionable_public_artifacts",
		},
		Heartbeats: heartbeats,
	}
}

func (config AgentAnatomyConfig) Validate() error {
	if strings.TrimSpace(config.Schema) != agentAnatomySchemaV1 {
		return fmt.Errorf("agent anatomy schema must be %q", agentAnatomySchemaV1)
	}
	if config.Concurrency.MaxParallelInternalSessions <= 0 {
		return fmt.Errorf("agent anatomy max_parallel_internal_sessions must be positive")
	}
	if config.Concurrency.MaxLLMSessions <= 0 {
		return fmt.Errorf("agent anatomy max_llm_sessions must be positive")
	}
	if config.Concurrency.MaxBrowserSessions < 0 {
		return fmt.Errorf("agent anatomy max_browser_sessions cannot be negative")
	}
	if err := validateAgentAnatomyConstitution(config.Memory.Constitution); err != nil {
		return err
	}
	if config.Memory.ReflectionChannelCap < 0 || config.Memory.ReflectionChannelCap > reflectionChannelCapCeiling {
		return fmt.Errorf("agent anatomy reflection_channel_cap must be within [0,%d]", reflectionChannelCapCeiling)
	}
	if len(config.Heartbeats) == 0 {
		return fmt.Errorf("agent anatomy requires at least one heartbeat")
	}
	seen := map[string]struct{}{}
	for _, heartbeat := range config.Heartbeats {
		id := strings.TrimSpace(heartbeat.ID)
		if id == "" {
			return fmt.Errorf("agent anatomy heartbeat id is required")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate heartbeat id %q", id)
		}
		seen[id] = struct{}{}
		if strings.TrimSpace(heartbeat.Kind) == "" {
			return fmt.Errorf("agent anatomy heartbeat %q kind is required", id)
		}
		if heartbeat.Priority <= 0 {
			return fmt.Errorf("agent anatomy heartbeat %q priority must be positive", id)
		}
		if _, ok := heartbeatCadenceDuration(heartbeat); !ok && strings.TrimSpace(heartbeat.Cadence) != heartbeatCadenceWhileClaimed {
			return fmt.Errorf("agent anatomy heartbeat %q has invalid cadence %q", id, heartbeat.Cadence)
		}
		if heartbeat.MaxParallel <= 0 {
			return fmt.Errorf("agent anatomy heartbeat %q max_parallel must be positive", id)
		}
		if heartbeat.MaxToolIterations < 0 {
			return fmt.Errorf("agent anatomy heartbeat %q max_tool_iterations cannot be negative", id)
		}
		if err := validateAgentHeartbeatEnums(heartbeat); err != nil {
			return err
		}
		if err := validateAgentHeartbeatActiveMemorySpec(id, heartbeat.ActiveMemory); err != nil {
			return err
		}
		if err := validateAgentHeartbeatWillPolicySpec(id, heartbeat.WillPolicy); err != nil {
			return err
		}
		if err := validateAgentHeartbeatEvidenceContractSpec(id, heartbeat.EvidenceContract); err != nil {
			return err
		}
		if err := validateAgentHeartbeatVisualAuditSpec(id, heartbeat.VisualAudit); err != nil {
			return err
		}
	}
	return nil
}

// validateAgentAnatomyConstitution validates the constitution block. It is
// lenient about zero values (treated as "unset" and defaulted during
// normalization) but strict about negatives, ceilings, and enum membership so
// the same function guards both raw input and normalized config.
func validateAgentAnatomyConstitution(c AgentAnatomyConstitution) error {
	if c.MaxRules < 0 || c.MaxRules > constitutionMaxRulesCeiling {
		return fmt.Errorf("agent anatomy constitution max_rules must be within [0,%d]", constitutionMaxRulesCeiling)
	}
	if c.MinEvidence < 0 || c.MinEvidence > constitutionMinEvidenceCeiling {
		return fmt.Errorf("agent anatomy constitution min_evidence must be within [0,%d]", constitutionMinEvidenceCeiling)
	}
	if promotion := strings.TrimSpace(c.Promotion); promotion != "" && normalizeConstitutionPromotion(promotion) == "" {
		return fmt.Errorf("agent anatomy constitution has invalid promotion %q", promotion)
	}
	for _, rule := range c.SeedRules {
		if kind := strings.TrimSpace(rule.Kind); kind != "" && normalizeConstitutionRuleKind(kind) == "" {
			return fmt.Errorf("agent anatomy constitution seed rule has invalid kind %q", kind)
		}
	}
	return nil
}

func validateAgentAnatomyRaw(config AgentAnatomyConfig) error {
	if schema := strings.TrimSpace(config.Schema); schema != "" && schema != agentAnatomySchemaV1 {
		return fmt.Errorf("agent anatomy schema must be %q", agentAnatomySchemaV1)
	}
	if err := validateAgentAnatomyConstitution(config.Memory.Constitution); err != nil {
		return err
	}
	if config.Memory.ReflectionChannelCap < 0 || config.Memory.ReflectionChannelCap > reflectionChannelCapCeiling {
		return fmt.Errorf("agent anatomy reflection_channel_cap must be within [0,%d]", reflectionChannelCapCeiling)
	}
	seen := map[string]struct{}{}
	for _, heartbeat := range config.Heartbeats {
		id := strings.TrimSpace(heartbeat.ID)
		if id == "" {
			return fmt.Errorf("agent anatomy heartbeat id is required")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate heartbeat id %q", id)
		}
		seen[id] = struct{}{}
		if cadence := strings.TrimSpace(heartbeat.Cadence); cadence != "" {
			if _, ok := heartbeatCadenceDuration(heartbeat); !ok && cadence != heartbeatCadenceWhileClaimed {
				return fmt.Errorf("agent anatomy heartbeat %q has invalid cadence %q", id, cadence)
			}
		}
		if err := validateAgentHeartbeatEnums(heartbeat); err != nil {
			return err
		}
		if heartbeat.MaxToolIterations < 0 {
			return fmt.Errorf("agent anatomy heartbeat %q max_tool_iterations cannot be negative", id)
		}
		if err := validateAgentHeartbeatActiveMemorySpec(id, heartbeat.ActiveMemory); err != nil {
			return err
		}
		if err := validateAgentHeartbeatWillPolicySpec(id, heartbeat.WillPolicy); err != nil {
			return err
		}
		if err := validateAgentHeartbeatEvidenceContractSpec(id, heartbeat.EvidenceContract); err != nil {
			return err
		}
		if err := validateAgentHeartbeatVisualAuditSpec(id, heartbeat.VisualAudit); err != nil {
			return err
		}
	}
	return nil
}

func validateAgentHeartbeatActiveMemorySpec(heartbeatID string, spec *AgentHeartbeatActiveMemorySpec) error {
	if spec == nil {
		return nil
	}
	if spec.MaxEntries < 0 {
		return fmt.Errorf("agent anatomy heartbeat %q active_memory max_entries cannot be negative", heartbeatID)
	}
	if spec.MaxEntries > 50 {
		return fmt.Errorf("agent anatomy heartbeat %q active_memory max_entries is too large", heartbeatID)
	}
	return nil
}

func validateAgentHeartbeatWillPolicySpec(heartbeatID string, spec *AgentHeartbeatWillPolicySpec) error {
	if spec == nil {
		return nil
	}
	if spec.MaxDirectives < 0 {
		return fmt.Errorf("agent anatomy heartbeat %q will_policy max_directives cannot be negative", heartbeatID)
	}
	if spec.MaxDirectives > 20 {
		return fmt.Errorf("agent anatomy heartbeat %q will_policy max_directives is too large", heartbeatID)
	}
	for _, action := range spec.AllowedActions {
		if strings.TrimSpace(action) != "" && normalizeAgentHeartbeatWillAction(action) == "" {
			return fmt.Errorf("agent anatomy heartbeat %q will_policy has invalid action %q", heartbeatID, action)
		}
	}
	if visibility := strings.TrimSpace(spec.PublishVisibility); visibility != "" {
		switch strings.ToLower(visibility) {
		case "private", "rhizome", "public", "workspace":
		default:
			return fmt.Errorf("agent anatomy heartbeat %q will_policy has invalid publish_visibility %q", heartbeatID, visibility)
		}
	}
	return nil
}

func validateAgentHeartbeatEvidenceContractSpec(heartbeatID string, spec *AgentHeartbeatEvidenceContractSpec) error {
	if spec == nil {
		return nil
	}
	seenDimensions := map[string]struct{}{}
	for _, dimension := range spec.Dimensions {
		id := strings.TrimSpace(dimension.ID)
		if id == "" {
			return fmt.Errorf("agent anatomy heartbeat %q evidence_contract dimension id is required", heartbeatID)
		}
		if _, ok := seenDimensions[id]; ok {
			return fmt.Errorf("agent anatomy heartbeat %q duplicate evidence_contract dimension id %q", heartbeatID, id)
		}
		seenDimensions[id] = struct{}{}
		if (dimension.Width < 0 || dimension.Height < 0) || (dimension.Width == 0 && dimension.Height > 0) || (dimension.Width > 0 && dimension.Height == 0) {
			return fmt.Errorf("agent anatomy heartbeat %q evidence_contract dimension %q width and height must be both absent or both positive", heartbeatID, id)
		}
	}
	seenStates := map[string]struct{}{}
	for _, state := range spec.States {
		id := strings.TrimSpace(state.ID)
		if id == "" {
			return fmt.Errorf("agent anatomy heartbeat %q evidence_contract state id is required", heartbeatID)
		}
		if _, ok := seenStates[id]; ok {
			return fmt.Errorf("agent anatomy heartbeat %q duplicate evidence_contract state id %q", heartbeatID, id)
		}
		seenStates[id] = struct{}{}
		if strings.TrimSpace(state.Label) == "" {
			return fmt.Errorf("agent anatomy heartbeat %q evidence_contract state %q label is required", heartbeatID, id)
		}
	}
	seenRequiredTools := map[string]struct{}{}
	for _, artifact := range spec.RequiredToolArtifacts {
		tool := strings.TrimSpace(artifact.Tool)
		if tool == "" {
			return fmt.Errorf("agent anatomy heartbeat %q evidence_contract required_tool_artifacts tool is required", heartbeatID)
		}
		key := strings.ToLower(strings.Join([]string{tool, strings.TrimSpace(artifact.ContractVersion), strings.TrimSpace(artifact.When)}, "\x00"))
		if _, ok := seenRequiredTools[key]; ok {
			return fmt.Errorf("agent anatomy heartbeat %q duplicate evidence_contract required_tool_artifact %q", heartbeatID, tool)
		}
		seenRequiredTools[key] = struct{}{}
		if strings.TrimSpace(artifact.ToolSuite) != "" && !isKnownAgentHeartbeatToolSuite(artifact.ToolSuite) {
			return fmt.Errorf("agent anatomy heartbeat %q evidence_contract required_tool_artifact %q has invalid tool_suite %q", heartbeatID, tool, artifact.ToolSuite)
		}
	}
	return nil
}

func validateAgentHeartbeatVisualAuditSpec(heartbeatID string, spec *AgentHeartbeatVisualAuditSpec) error {
	if spec == nil {
		return nil
	}
	seenViewports := map[string]struct{}{}
	for _, viewport := range spec.Viewports {
		id := strings.TrimSpace(viewport.ID)
		if id == "" {
			return fmt.Errorf("agent anatomy heartbeat %q visual_audit viewport id is required", heartbeatID)
		}
		if _, ok := seenViewports[id]; ok {
			return fmt.Errorf("agent anatomy heartbeat %q duplicate visual_audit viewport id %q", heartbeatID, id)
		}
		seenViewports[id] = struct{}{}
		if viewport.Width <= 0 || viewport.Height <= 0 {
			return fmt.Errorf("agent anatomy heartbeat %q visual_audit viewport %q width and height must be positive", heartbeatID, id)
		}
	}
	seenScenarios := map[string]struct{}{}
	for _, scenario := range spec.Scenarios {
		id := strings.TrimSpace(scenario.ID)
		if id == "" {
			return fmt.Errorf("agent anatomy heartbeat %q visual_audit scenario id is required", heartbeatID)
		}
		if _, ok := seenScenarios[id]; ok {
			return fmt.Errorf("agent anatomy heartbeat %q duplicate visual_audit scenario id %q", heartbeatID, id)
		}
		seenScenarios[id] = struct{}{}
		if strings.TrimSpace(scenario.Label) == "" {
			return fmt.Errorf("agent anatomy heartbeat %q visual_audit scenario %q label is required", heartbeatID, id)
		}
	}
	return nil
}

func validateAgentHeartbeatEnums(heartbeat AgentHeartbeatSpec) error {
	for _, lock := range heartbeat.Locks {
		if !isKnownAgentHeartbeatLock(lock) {
			return fmt.Errorf("agent anatomy heartbeat %q has invalid lock %q", heartbeat.ID, lock)
		}
	}
	for _, suite := range heartbeat.ToolSuites {
		if !isKnownAgentHeartbeatToolSuite(suite) {
			return fmt.Errorf("agent anatomy heartbeat %q has invalid tool suite %q", heartbeat.ID, suite)
		}
	}
	if err := validateSystemSensingHeartbeat(heartbeat); err != nil {
		return err
	}
	if err := validateStrategySynthesisHeartbeat(heartbeat); err != nil {
		return err
	}
	return nil
}

// validateSystemSensingHeartbeat enforces the Layer C structural guarantees on a
// system_sensing (workspace-awareness) heartbeat: it is read-only and cannot
// preempt the agent's active work. These are validate-time invariants so a
// misconfigured sensor can never be loaded — not merely soft-clamped at runtime.
func validateSystemSensingHeartbeat(heartbeat AgentHeartbeatSpec) error {
	if !strings.EqualFold(strings.TrimSpace(heartbeat.Kind), heartbeatKindSystemSensing) {
		return nil
	}
	// Read-only invariant: every declared tool suite must be in the read-only
	// allow-set. A write/authority suite on a sensor is rejected, not stripped.
	for _, suite := range heartbeat.ToolSuites {
		if !isReadOnlyHeartbeatToolSuite(suite) {
			return fmt.Errorf("agent anatomy heartbeat %q (system_sensing) cannot hold non-read-only tool suite %q", heartbeat.ID, strings.TrimSpace(suite))
		}
	}
	// Non-preemption invariant: a sensor observes and advises; it can never
	// resume, switch, or replan the agent's active work.
	//
	// CA-33: publish_rhizome_update is intentionally NOT in the forbidden set. The
	// read-only suite restriction enforced above guarantees the heartbeat holds no
	// public-authority suite, which deterministically derives policy.LocalOnly=true
	// at runtime and unconditionally blocks the public write (see
	// applyInternalHeartbeatWillDirective's publish_rhizome_update branch). The
	// read-only guarantee is therefore structural at load time via the suite
	// allow-set, not a runtime coincidence; a declared publish action is a
	// neutralized no-op, so rejecting it here would only break the legitimate
	// default sensor/synthesis specs without closing any realizable write path.
	if heartbeat.WillPolicy != nil {
		for _, action := range heartbeat.WillPolicy.AllowedActions {
			switch normalizeAgentHeartbeatWillAction(action) {
			case "request_resume", "runtime_switch_task", "replan_active_work":
				return fmt.Errorf("agent anatomy heartbeat %q (system_sensing) cannot hold preemptive will action %q", heartbeat.ID, strings.TrimSpace(action))
			}
		}
	}
	return nil
}

// validateStrategySynthesisHeartbeat enforces the Layer D structural guarantees on
// a strategy_synthesis (rule-consolidation) heartbeat: it is read-only by
// construction (it reconciles rules by appending superseding evidence, never by
// mutating runtime/workspace state) and is non-preemptive (advisory + publish
// only, never resume/switch/replan). Validate-time invariants, like the sensor.
func validateStrategySynthesisHeartbeat(heartbeat AgentHeartbeatSpec) error {
	if !strings.EqualFold(strings.TrimSpace(heartbeat.Kind), heartbeatKindStrategySynthesis) {
		return nil
	}
	for _, suite := range heartbeat.ToolSuites {
		if !isReadOnlyHeartbeatToolSuite(suite) {
			return fmt.Errorf("agent anatomy heartbeat %q (strategy_synthesis) cannot hold non-read-only tool suite %q", heartbeat.ID, strings.TrimSpace(suite))
		}
	}
	// CA-33: as with system_sensing, publish_rhizome_update is permitted because the
	// read-only suite restriction above forces policy.LocalOnly=true at runtime,
	// neutralizing the public write. The read-only guarantee is structural via the
	// suite allow-set; the publish action is a no-op rather than a realizable write.
	if heartbeat.WillPolicy != nil {
		for _, action := range heartbeat.WillPolicy.AllowedActions {
			switch normalizeAgentHeartbeatWillAction(action) {
			case "request_resume", "runtime_switch_task", "replan_active_work":
				return fmt.Errorf("agent anatomy heartbeat %q (strategy_synthesis) cannot hold preemptive will action %q", heartbeat.ID, strings.TrimSpace(action))
			}
		}
	}
	return nil
}

// isReadOnlyHeartbeatToolSuite reports whether a tool suite reads state without
// the capacity to mutate runtime/workspace state. It is the allow-set for the
// system_sensing read-only invariant. custom: suites are intentionally excluded
// because their capability cannot be statically guaranteed.
func isReadOnlyHeartbeatToolSuite(suite string) bool {
	switch strings.TrimSpace(suite) {
	case "memory_and_docs_read",
		"local_log_read",
		"rhizome_read",
		"workspace_docs_read",
		"patch_queue_read",
		"local_tests_read",
		"browser_read_only",
		"screenshot_capture",
		"console_read":
		return true
	default:
		return false
	}
}

func isKnownAgentHeartbeatLock(lock string) bool {
	lock = strings.TrimSpace(lock)
	if strings.HasPrefix(lock, "custom:") {
		return true
	}
	switch lock {
	case "exclusive_task_mutation",
		"local_only",
		"read_only_artifact",
		"trusted_local_browser",
		"non_mutating_coordination",
		"patch_queue_read",
		"bounded_integration_claim":
		return true
	default:
		return false
	}
}

func isKnownAgentHeartbeatToolSuite(suite string) bool {
	suite = strings.TrimSpace(suite)
	if strings.HasPrefix(suite, "custom:") {
		return true
	}
	switch suite {
	case "task_authority",
		"workspace_tools",
		"local_execution",
		"memory_and_docs_read",
		"local_log_read",
		"browser_unrestricted",
		"browser_interactive",
		"browser_read_only",
		"screenshot_capture",
		"console_read",
		"rhizome_read",
		"workspace_docs_read",
		"bounded_task_submit",
		"project_governance_review",
		"patch_queue_read",
		"local_tests_read":
		return true
	default:
		return false
	}
}

func AgentAnatomyDigest(config AgentAnatomyConfig) string {
	config = stableAgentAnatomyForDigest(config)
	raw, err := json.Marshal(config)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}

func stableAgentAnatomyForDigest(config AgentAnatomyConfig) AgentAnatomyConfig {
	config.UpdatedAt = ""
	config.Memory.Lanes = uniqueTrimmedCSVStrings(config.Memory.Lanes)
	sort.Strings(config.Memory.Lanes)
	if seeds := config.Memory.Constitution.SeedRules; len(seeds) > 1 {
		sorted := append([]AgentAnatomyConstitutionRule(nil), seeds...)
		sort.SliceStable(sorted, func(i, j int) bool {
			if sorted[i].ID != sorted[j].ID {
				return sorted[i].ID < sorted[j].ID
			}
			return sorted[i].Rule < sorted[j].Rule
		})
		config.Memory.Constitution.SeedRules = sorted
	}
	for idx := range config.Heartbeats {
		config.Heartbeats[idx] = normalizeAgentHeartbeatSpec(config.Heartbeats[idx])
	}
	sort.Slice(config.Heartbeats, func(i, j int) bool {
		return config.Heartbeats[i].ID < config.Heartbeats[j].ID
	})
	return config
}

func mergeAgentHeartbeatSpecs(defaults, explicit []AgentHeartbeatSpec) []AgentHeartbeatSpec {
	merged := make(map[string]AgentHeartbeatSpec, len(defaults)+len(explicit))
	order := make([]string, 0, len(defaults)+len(explicit))
	for _, heartbeat := range defaults {
		heartbeat = normalizeAgentHeartbeatSpec(heartbeat)
		if heartbeat.ID == "" {
			continue
		}
		if _, ok := merged[heartbeat.ID]; !ok {
			order = append(order, heartbeat.ID)
		}
		merged[heartbeat.ID] = heartbeat
	}
	for _, heartbeat := range explicit {
		heartbeat.ID = strings.TrimSpace(heartbeat.ID)
		if heartbeat.ID == "" {
			continue
		}
		if base, ok := merged[heartbeat.ID]; ok {
			heartbeat = overlayAgentHeartbeatSpec(base, heartbeat)
		}
		heartbeat = normalizeAgentHeartbeatSpec(heartbeat)
		if _, ok := merged[heartbeat.ID]; !ok {
			order = append(order, heartbeat.ID)
		}
		merged[heartbeat.ID] = heartbeat
	}
	out := make([]AgentHeartbeatSpec, 0, len(order))
	for _, id := range order {
		out = append(out, merged[id])
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func overlayAgentHeartbeatSpec(base, override AgentHeartbeatSpec) AgentHeartbeatSpec {
	out := base
	out.ID = firstNonEmpty(strings.TrimSpace(override.ID), out.ID)
	out.Kind = firstNonEmpty(strings.TrimSpace(override.Kind), out.Kind)
	out.Cadence = firstNonEmpty(strings.TrimSpace(override.Cadence), out.Cadence)
	if override.CadenceSec > 0 {
		out.CadenceSec = override.CadenceSec
	}
	if override.Priority > 0 {
		out.Priority = override.Priority
	}
	if override.Enabled != nil {
		out.Enabled = override.Enabled
	}
	if len(override.Triggers) > 0 {
		out.Triggers = override.Triggers
	}
	if len(override.Locks) > 0 {
		out.Locks = override.Locks
	}
	if len(override.ToolSuites) > 0 {
		out.ToolSuites = override.ToolSuites
	}
	if len(override.ContextSelectors) > 0 {
		out.ContextSelectors = override.ContextSelectors
	}
	if len(override.OutputContracts) > 0 {
		out.OutputContracts = override.OutputContracts
	}
	if len(override.PromotionSignals) > 0 {
		out.PromotionSignals = override.PromotionSignals
	}
	if override.MaxParallel > 0 {
		out.MaxParallel = override.MaxParallel
	}
	if override.MaxToolIterations > 0 {
		out.MaxToolIterations = override.MaxToolIterations
	}
	if strings.TrimSpace(override.Objective) != "" {
		out.Objective = override.Objective
	}
	if len(override.Instructions) > 0 {
		out.Instructions = override.Instructions
	}
	if len(override.MemoryLanes) > 0 {
		out.MemoryLanes = override.MemoryLanes
	}
	if override.ActiveMemory != nil {
		out.ActiveMemory = overlayAgentHeartbeatActiveMemorySpec(out.ActiveMemory, override.ActiveMemory)
	}
	if override.WillPolicy != nil {
		out.WillPolicy = overlayAgentHeartbeatWillPolicySpec(out.WillPolicy, override.WillPolicy)
	}
	if len(override.Notes) > 0 {
		out.Notes = override.Notes
	}
	if override.EvidenceContract != nil {
		out.EvidenceContract = overlayAgentHeartbeatEvidenceContractSpec(out.EvidenceContract, override.EvidenceContract)
	}
	if override.VisualAudit != nil {
		out.VisualAudit = override.VisualAudit
	}
	return out
}

func overlayAgentHeartbeatEvidenceContractSpec(base, override *AgentHeartbeatEvidenceContractSpec) *AgentHeartbeatEvidenceContractSpec {
	if override == nil {
		return base
	}
	if base == nil {
		return override
	}
	out := *base
	if len(override.Dimensions) > 0 {
		out.Dimensions = override.Dimensions
	}
	if len(override.States) > 0 {
		out.States = override.States
	}
	if len(override.Checks) > 0 {
		out.Checks = override.Checks
	}
	if len(override.ArtifactRequirements) > 0 {
		out.ArtifactRequirements = override.ArtifactRequirements
	}
	if len(override.RequiredToolArtifacts) > 0 {
		out.RequiredToolArtifacts = override.RequiredToolArtifacts
	}
	return &out
}

func overlayAgentHeartbeatActiveMemorySpec(base, override *AgentHeartbeatActiveMemorySpec) *AgentHeartbeatActiveMemorySpec {
	if override == nil {
		return base
	}
	if base == nil {
		return override
	}
	out := *base
	if strings.TrimSpace(override.Lane) != "" {
		out.Lane = override.Lane
	}
	if override.MaxEntries > 0 {
		out.MaxEntries = override.MaxEntries
	}
	if override.IncludeSessionSummaries != nil {
		out.IncludeSessionSummaries = override.IncludeSessionSummaries
	}
	if override.IncludeBacklog != nil {
		out.IncludeBacklog = override.IncludeBacklog
	}
	return &out
}

func overlayAgentHeartbeatWillPolicySpec(base, override *AgentHeartbeatWillPolicySpec) *AgentHeartbeatWillPolicySpec {
	if override == nil {
		return base
	}
	if base == nil {
		return override
	}
	out := *base
	if override.Enabled != nil {
		out.Enabled = override.Enabled
	}
	if len(override.AllowedActions) > 0 {
		out.AllowedActions = override.AllowedActions
	}
	if override.MaxDirectives > 0 {
		out.MaxDirectives = override.MaxDirectives
	}
	if override.RequiresEvidence != nil {
		out.RequiresEvidence = override.RequiresEvidence
	}
	if strings.TrimSpace(override.PublishVisibility) != "" {
		out.PublishVisibility = override.PublishVisibility
	}
	return &out
}

func normalizeAgentHeartbeatSpec(heartbeat AgentHeartbeatSpec) AgentHeartbeatSpec {
	heartbeat.ID = strings.TrimSpace(heartbeat.ID)
	heartbeat.Kind = strings.TrimSpace(heartbeat.Kind)
	heartbeat.Cadence = strings.TrimSpace(heartbeat.Cadence)
	if heartbeat.Cadence == "" && heartbeat.CadenceSec <= 0 {
		heartbeat.Cadence = "every_15m"
	}
	if heartbeat.Priority <= 0 {
		heartbeat.Priority = 10
	}
	if heartbeat.Enabled == nil {
		heartbeat.Enabled = boolPtr(true)
	}
	heartbeat.Triggers = uniqueTrimmedCSVStrings(heartbeat.Triggers)
	heartbeat.Locks = uniqueTrimmedCSVStrings(heartbeat.Locks)
	heartbeat.ToolSuites = uniqueTrimmedCSVStrings(heartbeat.ToolSuites)
	heartbeat.ContextSelectors = uniqueTrimmedCSVStrings(heartbeat.ContextSelectors)
	heartbeat.OutputContracts = uniqueTrimmedCSVStrings(heartbeat.OutputContracts)
	heartbeat.PromotionSignals = uniqueTrimmedCSVStrings(heartbeat.PromotionSignals)
	heartbeat.Objective = strings.TrimSpace(heartbeat.Objective)
	heartbeat.Instructions = uniqueTrimmedCSVStrings(heartbeat.Instructions)
	heartbeat.MemoryLanes = uniqueTrimmedCSVStrings(heartbeat.MemoryLanes)
	heartbeat.ActiveMemory = normalizeAgentHeartbeatActiveMemorySpec(heartbeat.ActiveMemory, heartbeat)
	heartbeat.WillPolicy = normalizeAgentHeartbeatWillPolicySpec(heartbeat.WillPolicy)
	heartbeat.Notes = uniqueTrimmedCSVStrings(heartbeat.Notes)
	heartbeat.EvidenceContract = normalizeAgentHeartbeatEvidenceContractSpec(heartbeat.EvidenceContract)
	heartbeat.VisualAudit = normalizeAgentHeartbeatVisualAuditSpec(heartbeat.VisualAudit)
	if heartbeat.MaxParallel <= 0 {
		heartbeat.MaxParallel = 1
	}
	if heartbeat.MaxToolIterations < 0 {
		heartbeat.MaxToolIterations = 0
	}
	return heartbeat
}

func normalizeAgentHeartbeatActiveMemorySpec(spec *AgentHeartbeatActiveMemorySpec, heartbeat AgentHeartbeatSpec) *AgentHeartbeatActiveMemorySpec {
	if spec == nil {
		return nil
	}
	out := *spec
	out.Lane = strings.TrimSpace(out.Lane)
	if out.Lane == "" {
		if len(heartbeat.MemoryLanes) > 0 {
			out.Lane = firstNonEmpty(heartbeat.MemoryLanes[0], heartbeat.ID)
		} else {
			out.Lane = heartbeat.ID
		}
	}
	if out.MaxEntries <= 0 {
		out.MaxEntries = 6
	}
	if out.MaxEntries > 20 {
		out.MaxEntries = 20
	}
	if out.IncludeSessionSummaries == nil {
		out.IncludeSessionSummaries = boolPtr(true)
	}
	if out.IncludeBacklog == nil {
		out.IncludeBacklog = boolPtr(true)
	}
	return &out
}

func normalizeAgentHeartbeatWillPolicySpec(spec *AgentHeartbeatWillPolicySpec) *AgentHeartbeatWillPolicySpec {
	if spec == nil {
		return nil
	}
	out := *spec
	if out.Enabled == nil {
		out.Enabled = boolPtr(true)
	}
	if out.MaxDirectives <= 0 {
		out.MaxDirectives = 2
	}
	if out.MaxDirectives > 10 {
		out.MaxDirectives = 10
	}
	if out.RequiresEvidence == nil {
		out.RequiresEvidence = boolPtr(false)
	}
	actions := make([]string, 0, len(out.AllowedActions))
	for _, action := range out.AllowedActions {
		if normalized := normalizeAgentHeartbeatWillAction(action); normalized != "" {
			actions = append(actions, normalized)
		}
	}
	out.AllowedActions = uniqueTrimmedCSVStrings(actions)
	out.PublishVisibility = normalizeAgentHeartbeatWillPublishVisibility(out.PublishVisibility)
	return &out
}

func normalizeAgentHeartbeatWillAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "", "none":
		return ""
	case "advisory", "advisory_signal", "signal":
		return "advisory_signal"
	case "request_resume", "resume", "nudge_planner":
		return "request_resume"
	case "runtime_switch_task", "switch_task":
		return "runtime_switch_task"
	case "replan", "replan_active_work", "abandon_current_contour", "change_course":
		return "replan_active_work"
	case "publish_update", "publish_rhizome_update", "rhizome_update":
		return "publish_rhizome_update"
	default:
		return ""
	}
}

func normalizeAgentHeartbeatWillPublishVisibility(visibility string) string {
	switch strings.ToLower(strings.TrimSpace(visibility)) {
	case "", "private":
		return "private"
	case "rhizome", "public", "workspace":
		return "rhizome"
	default:
		return "private"
	}
}

func normalizeAgentHeartbeatEvidenceContractSpec(spec *AgentHeartbeatEvidenceContractSpec) *AgentHeartbeatEvidenceContractSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	out.Checks = uniqueTrimmedCSVStrings(out.Checks)
	out.ArtifactRequirements = uniqueTrimmedCSVStrings(out.ArtifactRequirements)
	requiredToolArtifacts := make([]AgentHeartbeatRequiredToolArtifactSpec, 0, len(out.RequiredToolArtifacts))
	seenRequiredToolArtifacts := map[string]struct{}{}
	for _, artifact := range out.RequiredToolArtifacts {
		artifact.Tool = strings.TrimSpace(artifact.Tool)
		artifact.ContractVersion = strings.TrimSpace(artifact.ContractVersion)
		artifact.Capability = strings.TrimSpace(artifact.Capability)
		artifact.ToolSuite = strings.TrimSpace(artifact.ToolSuite)
		artifact.When = strings.TrimSpace(artifact.When)
		artifact.Purpose = strings.TrimSpace(artifact.Purpose)
		artifact.BlockerGuidance = strings.TrimSpace(artifact.BlockerGuidance)
		if artifact.Tool == "" && artifact.ContractVersion == "" && artifact.Capability == "" && artifact.ToolSuite == "" && artifact.When == "" && artifact.Purpose == "" && artifact.BlockerGuidance == "" {
			continue
		}
		key := strings.ToLower(strings.Join([]string{artifact.Tool, artifact.ContractVersion, artifact.When}, "\x00"))
		if _, ok := seenRequiredToolArtifacts[key]; ok {
			continue
		}
		seenRequiredToolArtifacts[key] = struct{}{}
		requiredToolArtifacts = append(requiredToolArtifacts, artifact)
	}
	out.RequiredToolArtifacts = requiredToolArtifacts
	dimensions := make([]AgentHeartbeatEvidenceDimensionSpec, 0, len(out.Dimensions))
	for _, dimension := range out.Dimensions {
		dimension.ID = strings.TrimSpace(dimension.ID)
		dimension.Kind = strings.TrimSpace(dimension.Kind)
		dimension.Label = strings.TrimSpace(dimension.Label)
		dimension.Purpose = strings.TrimSpace(dimension.Purpose)
		if dimension.ID == "" && dimension.Kind == "" && dimension.Label == "" && dimension.Width <= 0 && dimension.Height <= 0 && dimension.Purpose == "" {
			continue
		}
		dimensions = append(dimensions, dimension)
	}
	out.Dimensions = dimensions
	states := make([]AgentHeartbeatEvidenceStateSpec, 0, len(out.States))
	for _, state := range out.States {
		state.ID = strings.TrimSpace(state.ID)
		state.Label = strings.TrimSpace(state.Label)
		state.RequiredState = strings.TrimSpace(state.RequiredState)
		state.RealUserQuestion = strings.TrimSpace(state.RealUserQuestion)
		state.ExpectedEvidenceKind = strings.TrimSpace(state.ExpectedEvidenceKind)
		if state.ID == "" && state.Label == "" && state.RequiredState == "" && state.RealUserQuestion == "" && state.ExpectedEvidenceKind == "" && state.EvidenceRequired == nil {
			continue
		}
		states = append(states, state)
	}
	out.States = states
	if len(out.Dimensions) == 0 && len(out.States) == 0 && len(out.Checks) == 0 && len(out.ArtifactRequirements) == 0 && len(out.RequiredToolArtifacts) == 0 {
		return nil
	}
	return &out
}

func normalizeAgentHeartbeatVisualAuditSpec(spec *AgentHeartbeatVisualAuditSpec) *AgentHeartbeatVisualAuditSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	out.Checks = uniqueTrimmedCSVStrings(out.Checks)
	out.ArtifactRequirements = uniqueTrimmedCSVStrings(out.ArtifactRequirements)
	viewports := make([]AgentHeartbeatVisualAuditViewportSpec, 0, len(out.Viewports))
	for _, viewport := range out.Viewports {
		viewport.ID = strings.TrimSpace(viewport.ID)
		viewport.Purpose = strings.TrimSpace(viewport.Purpose)
		if viewport.ID == "" && viewport.Width <= 0 && viewport.Height <= 0 && viewport.Purpose == "" {
			continue
		}
		viewports = append(viewports, viewport)
	}
	out.Viewports = viewports
	scenarios := make([]AgentHeartbeatVisualAuditScenarioSpec, 0, len(out.Scenarios))
	for _, scenario := range out.Scenarios {
		scenario.ID = strings.TrimSpace(scenario.ID)
		scenario.Label = strings.TrimSpace(scenario.Label)
		scenario.RequiredState = strings.TrimSpace(scenario.RequiredState)
		scenario.RealUserQuestion = strings.TrimSpace(scenario.RealUserQuestion)
		scenario.ExpectedEvidenceKind = strings.TrimSpace(scenario.ExpectedEvidenceKind)
		if scenario.ID == "" && scenario.Label == "" && scenario.RequiredState == "" && scenario.RealUserQuestion == "" && scenario.ExpectedEvidenceKind == "" && scenario.ScreenshotRequired == nil {
			continue
		}
		scenarios = append(scenarios, scenario)
	}
	out.Scenarios = scenarios
	if len(out.Viewports) == 0 && len(out.Scenarios) == 0 && len(out.Checks) == 0 && len(out.ArtifactRequirements) == 0 {
		return nil
	}
	return &out
}

func heartbeatEnabled(heartbeat AgentHeartbeatSpec) bool {
	return heartbeat.Enabled == nil || *heartbeat.Enabled
}

func heartbeatCadenceDuration(heartbeat AgentHeartbeatSpec) (time.Duration, bool) {
	if heartbeat.CadenceSec > 0 {
		return time.Duration(heartbeat.CadenceSec) * time.Second, true
	}
	cadence := strings.TrimSpace(heartbeat.Cadence)
	if cadence == "" || cadence == heartbeatCadenceWhileClaimed {
		return 0, false
	}
	if strings.HasPrefix(cadence, "every_") {
		cadence = strings.TrimPrefix(cadence, "every_")
	}
	duration, err := time.ParseDuration(cadence)
	if err != nil || duration <= 0 {
		return 0, false
	}
	return duration, true
}

func defaultActiveTaskHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:         "active_task_execution",
		Kind:       "task_execution",
		Cadence:    heartbeatCadenceWhileClaimed,
		Priority:   100,
		Locks:      []string{"exclusive_task_mutation"},
		ToolSuites: []string{"task_authority", "workspace_tools", "local_execution"},
		ContextSelectors: []string{
			"hydrated_task",
			"project_coordination",
			"patch_queue",
			"recent_memory",
		},
		OutputContracts:  []string{"task_result", "workspace_doc_when_material", "patch_or_evidence"},
		PromotionSignals: []string{"task_contract"},
		Objective:        "Close the currently claimed task by producing durable implementation, evidence, or a truthful blocked state.",
		Instructions: []string{
			"stay inside the claimed task contract and project write scope",
			"publish material evidence when implementation, review, or QA state changes",
			"avoid creating broad follow-up work while a concrete claimed task is runnable",
		},
		MemoryLanes: []string{"working_notes", "promoted_refs"},
		Notes:       []string{"Primary task-closing loop. It is intentionally not a generic idle reflector."},
	})
}

func defaultLoopSelfCheckHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:                "loop_self_check",
		Kind:              "metacognition",
		Cadence:           "every_15m",
		Priority:          50,
		MaxToolIterations: 2,
		Triggers:          []string{"repeated_no_work", "same_blocker_repeated", "dirty_checkout_without_progress", "long_session_no_evidence"},
		Locks:             []string{"local_only"},
		ToolSuites:        []string{"memory_and_docs_read", "local_log_read"},
		ContextSelectors: []string{
			"recent_internal_sessions",
			"local_memory",
			"active_task_state",
			"recent_runtime_events",
		},
		OutputContracts:  []string{"local_memory", "active_memory", "self_repair_action", "will_directive", "escalation_if_stuck"},
		PromotionSignals: []string{"stuck_or_repeated_failure", "systemic_runtime_bug"},
		Objective:        "Privately inspect whether this agent is looping, stuck, or failing to turn recent work into evidence.",
		Instructions: []string{
			"compare the last few internal sessions and runtime updates before deciding there is no action",
			"record local backlog for repeated blockers, repeated no-work outcomes, or missing evidence",
			"when the current plan must change, emit a will_directive instead of only describing the problem",
			"keep public task creation disabled; only future bounded heartbeats may promote the finding",
		},
		MemoryLanes: []string{"self_check", "working_notes"},
		ActiveMemory: &AgentHeartbeatActiveMemorySpec{
			Lane:       "self_check",
			MaxEntries: 6,
		},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions: []string{"advisory_signal", "request_resume", "replan_active_work"},
			MaxDirectives:  2,
		},
		Notes: []string{"Asks whether the agent is looping, blocked, or failing to turn thought into evidence."},
	})
}

func defaultPersonalBacklogArbiterHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "personal_backlog_arbiter",
		Kind:     "backlog_arbiter",
		Cadence:  "every_10m",
		Priority: 45,
		Triggers: []string{
			"personal_backlog_open",
			"action_request_pending",
			"capability_request_unrouted",
			"local_memory_needs_decision",
		},
		Locks:      []string{"local_only"},
		ToolSuites: []string{"memory_and_docs_read", "local_log_read"},
		ContextSelectors: []string{
			"recent_internal_sessions",
			"local_memory",
			"recent_runtime_events",
			"workspace_state",
		},
		OutputContracts:  []string{"local_memory", "backlog_triage", "action_route"},
		PromotionSignals: []string{"stale_action_request", "missing_capability", "unresolved_local_backlog"},
		Objective:        "Privately triage this agent's personal backlog and route unresolved action requests into the next safest local decision.",
		Instructions: []string{
			"inspect open personal backlog candidates before deciding there is no action",
			"route action_requests by capability, required tool suite, task-loop need, and human-input need",
			"keep this heartbeat local-only; do not promote public work or claim that unavailable capability work has been completed",
		},
		MemoryLanes: []string{"role_backlog", "self_check", "working_notes"},
		Notes:       []string{"Routes the agent's own unresolved local findings so action_requests do not remain inert private notes."},
	})
}

func defaultActionRequestPromoterHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "action_request_promoter",
		Kind:     "action_request_promoter",
		Cadence:  "every_20m",
		Priority: 35,
		Triggers: []string{
			"personal_backlog_action_route_stale",
			"capability_request_ready_for_public_owner",
			"private_finding_needs_public_work",
		},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"memory_and_docs_read", "rhizome_read", "bounded_task_submit"},
		ContextSelectors: []string{
			"recent_internal_sessions",
			"local_memory",
			"workspace_state",
			"project_coordination",
			"runnable_surface",
		},
		OutputContracts:  []string{"local_memory", "bounded_public_task_from_private_route"},
		PromotionSignals: []string{"stale_action_request", "missing_capability", "actionable_private_route"},
		Objective:        "Promote an unresolved private action route into one bounded public task only when it has project scope and enough evidence to be useful to other agents.",
		Instructions: []string{
			"inspect routed personal backlog items before creating public work",
			"promote at most one high-score project-scoped action route per heartbeat",
			"do not promote human-input, secret, budget, paid-resource, or credential requests",
			"mark unsupported or low-confidence routes local instead of inventing completed evidence",
		},
		MemoryLanes: []string{"role_backlog", "promoted_refs", "working_notes"},
		Notes:       []string{"This is the bridge from private agent initiative to public Rhizome work; it must stay narrow and deterministic."},
	})
}

func defaultUIDesignGlobalReflectionHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:                "design_global_reflection",
		Kind:              "global_metacognition",
		Cadence:           "every_30m",
		Priority:          60,
		MaxToolIterations: 2,
		Triggers: []string{
			"project_design_drift",
			"multiple_ui_agents_active",
			"visual_evidence_missing",
			"frontend_quality_goal_ambiguous",
		},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"memory_and_docs_read", "rhizome_read", "bounded_task_submit"},
		ContextSelectors: []string{
			"workspace_state",
			"project_coordination",
			"project_reflection",
			"design_plan",
			"recent_ui_findings",
			"visual_evidence",
		},
		OutputContracts:  []string{"local_memory", "active_memory", "project_reflection_note", "will_directive", "bounded_task_if_unowned"},
		PromotionSignals: []string{"design_drift", "missing_visual_evidence", "project_goal_drift", "coordination_gap"},
		Objective:        "Globally reflect on whether this UI/UX agent's design-quality work still matches the project goal, peer design contour, and available evidence.",
		Instructions: []string{
			"look beyond the current task and compare active UI/UX work with project contracts, design plans, visual evidence, and peer coordination",
			"record a compact active-memory observation when the design contour changes or remains healthy",
			"use will_directives when the agent should abandon a stale local plan, resume with a corrected plan, or switch toward a more important existing task",
			"promote at most one bounded public task only when the gap is project-scoped, unowned, and supported by concrete evidence",
		},
		MemoryLanes: []string{"design_sensemaking", "visual_findings", "role_backlog"},
		ActiveMemory: &AgentHeartbeatActiveMemorySpec{
			Lane:       "design_sensemaking",
			MaxEntries: 8,
		},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions:    []string{"advisory_signal", "request_resume", "replan_active_work", "runtime_switch_task", "publish_rhizome_update"},
			MaxDirectives:     3,
			RequiresEvidence:  boolPtr(true),
			PublishVisibility: "rhizome",
		},
		Notes: []string{"Global design-quality sensemaking loop for UI/UX agents; it can steer the planner but still uses bounded public promotion."},
	})
}

func defaultVisualProductAuditHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:                "visual_product_audit",
		Kind:              "browser_critic",
		Cadence:           "every_10m",
		Priority:          65,
		MaxToolIterations: 4,
		Triggers: []string{
			"project_has_ui_surface",
			"ui_patch_queue_candidate",
			"frontend_task_running_too_long",
			"post_mvp_no_visual_evidence",
		},
		Locks:      []string{"trusted_local_browser"},
		ToolSuites: []string{"browser_unrestricted", "browser_read_only", "screenshot_capture", "console_read", "local_log_read", "bounded_task_submit"},
		ContextSelectors: []string{
			"product_contract",
			"design_plan",
			"runnable_surface",
			"visual_evidence",
			"recent_ui_findings",
		},
		OutputContracts:  []string{"local_memory", "active_memory", "visual_finding_doc", "will_directive", "revision_task_if_actionable"},
		PromotionSignals: []string{"observed_user_harm", "layout_overlap", "unusable_flow", "visual_regression", "bad_primary_surface_geometry"},
		Objective:        "Act as a harsh real-user product critic and require durable evidence before accepting visual or interaction quality.",
		Instructions: []string{
			"treat page-load and product-marker checks as navigation evidence only",
			"when a runnable surface exists, use the browser_visual_probe tool to capture bounded desktop and narrow screenshots before judging visual quality; the tool may start the app and inspect local, file, or external URLs when the task needs it",
			"if only a dirty/local/provisional candidate is visible, inspect it as a provisional non-canonical candidate when exact provenance or a runnable URL is available; missing patch queue blocks visual pass, not critique or repair findings",
			"after screenshot capture, do semantic visual judgment yourself; low generic layout-risk scores and visible product markers do not prove the primary surface looks usable",
			"for boards, grids, canvases, charts, editors, maps, and game surfaces, check aspect ratio, cell/object size, wrapping, density, empty-space balance, and every visible mode/preset/difficulty that changes the primary surface",
			"request state-specific evidence according to the generic evidence_contract",
			"promote at most one bounded QA task only from typed sensor evidence, never from vibe-only claims",
		},
		MemoryLanes: []string{"visual_findings", "role_backlog"},
		ActiveMemory: &AgentHeartbeatActiveMemorySpec{
			Lane:       "visual_findings",
			MaxEntries: 8,
		},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			AllowedActions:    []string{"advisory_signal", "request_resume", "replan_active_work", "runtime_switch_task", "publish_rhizome_update"},
			MaxDirectives:     2,
			RequiresEvidence:  boolPtr(true),
			PublishVisibility: "rhizome",
		},
		Notes:            []string{"A deliberately harsh user-reality critic for real browser scenarios, not a design prompt stylist."},
		EvidenceContract: defaultVisualEvidenceContract(),
	})
}

func defaultVisualEvidenceContract() *AgentHeartbeatEvidenceContractSpec {
	return &AgentHeartbeatEvidenceContractSpec{
		Dimensions: []AgentHeartbeatEvidenceDimensionSpec{
			{ID: "desktop", Kind: "viewport", Label: "Desktop", Width: 1365, Height: 900, Purpose: "first viewport and main workflow at a common laptop/desktop size"},
			{ID: "narrow", Kind: "viewport", Label: "Narrow", Width: 390, Height: 844, Purpose: "mobile/narrow layout, text fit, controls, and horizontal overflow"},
		},
		States: []AgentHeartbeatEvidenceStateSpec{
			{
				ID:                   "initial_state",
				Label:                "First viewport / empty state",
				RequiredState:        "before user input",
				EvidenceRequired:     boolPtr(true),
				RealUserQuestion:     "Can a new user understand what to do without layout glitches?",
				ExpectedEvidenceKind: "state-specific screenshot",
			},
			{
				ID:                   "primary_flow",
				Label:                "Primary happy path",
				RequiredState:        "after performing the core action",
				EvidenceRequired:     boolPtr(true),
				RealUserQuestion:     "Can the user complete the main workflow without awkward controls or lag?",
				ExpectedEvidenceKind: "state-specific screenshot plus observed behavior",
			},
			{
				ID:                   "result_state",
				Label:                "Output / export / post-action state",
				RequiredState:        "after the product produces its result",
				EvidenceRequired:     boolPtr(true),
				RealUserQuestion:     "Is the final result visible, correctly sized, and ready to use?",
				ExpectedEvidenceKind: "state-specific screenshot or explicit not-applicable note",
			},
		},
		Checks: []string{
			"overlap",
			"clipping",
			"contrast",
			"readability",
			"responsive_fit",
			"typography_hierarchy",
			"spacing",
			"primary_surface_geometry",
			"state_or_mode_specific_density",
			"primary_action_visibility",
			"loading_error_empty_states",
			"performance_symptoms",
		},
		ArtifactRequirements: []string{
			"distinct screenshots for initial_state, primary_flow, and result_state",
			"at least one desktop screenshot and one narrow/mobile screenshot",
			"locally decodable screenshot files or durable workspace artifact refs",
			"branch/head/url/checkout provenance",
			"primary-surface geometry/density judgment for boards, grids, canvases, charts, editors, maps, or game surfaces",
			"visible modes/presets/difficulties that change the primary surface are checked or explicitly marked not applicable",
			"provisional non-canonical findings must be labeled as critique evidence and cannot satisfy acceptance",
			"visual_verdict: pass only when no blocking findings remain",
		},
		RequiredToolArtifacts: []AgentHeartbeatRequiredToolArtifactSpec{
			{
				Tool:            "browser_visual_probe",
				ContractVersion: "browser_visual_probe_result_v1",
				Capability:      "browser_screenshot",
				ToolSuite:       "screenshot_capture",
				When:            "runnable_surface_present",
				Purpose:         "Capture real browser screenshot/DOM evidence before judging visual quality; build/source/doc review cannot satisfy this artifact.",
				BlockerGuidance: "If no runnable surface or installed probe is available, emit an action_request/capability blocker instead of a visual pass.",
			},
		},
	}
}

func defaultVisualAuditContract() *AgentHeartbeatVisualAuditSpec {
	return &AgentHeartbeatVisualAuditSpec{
		Viewports: []AgentHeartbeatVisualAuditViewportSpec{
			{ID: "desktop", Width: 1365, Height: 900, Purpose: "first viewport and main workflow at a common laptop/desktop size"},
			{ID: "narrow", Width: 390, Height: 844, Purpose: "mobile/narrow layout, text fit, controls, and horizontal overflow"},
		},
		Scenarios: []AgentHeartbeatVisualAuditScenarioSpec{
			{
				ID:                   "initial_state",
				Label:                "First viewport / empty state",
				RequiredState:        "before user input",
				ScreenshotRequired:   boolPtr(true),
				RealUserQuestion:     "Can a new user understand what to do without layout glitches?",
				ExpectedEvidenceKind: "state-specific screenshot",
			},
			{
				ID:                   "primary_flow",
				Label:                "Primary happy path",
				RequiredState:        "after performing the core action",
				ScreenshotRequired:   boolPtr(true),
				RealUserQuestion:     "Can the user complete the main workflow without awkward controls or lag?",
				ExpectedEvidenceKind: "state-specific screenshot plus observed behavior",
			},
			{
				ID:                   "result_state",
				Label:                "Output / export / post-action state",
				RequiredState:        "after the product produces its result",
				ScreenshotRequired:   boolPtr(true),
				RealUserQuestion:     "Is the final result visible, correctly sized, and ready to use?",
				ExpectedEvidenceKind: "state-specific screenshot or explicit not-applicable note",
			},
		},
		Checks: []string{
			"overlap",
			"clipping",
			"contrast",
			"readability",
			"responsive_fit",
			"typography_hierarchy",
			"spacing",
			"primary_surface_geometry",
			"state_or_mode_specific_density",
			"primary_action_visibility",
			"loading_error_empty_states",
			"performance_symptoms",
		},
		ArtifactRequirements: []string{
			"distinct screenshots for initial_state, primary_flow, and result_state",
			"at least one desktop screenshot and one narrow/mobile screenshot",
			"locally decodable screenshot files or durable workspace artifact refs",
			"branch/head/url/checkout provenance",
			"primary-surface geometry/density judgment for boards, grids, canvases, charts, editors, maps, or game surfaces",
			"visible modes/presets/difficulties that change the primary surface are checked or explicitly marked not applicable",
			"visual_verdict: pass only when no blocking findings remain",
		},
	}
}

func defaultProjectRoleInitiativeHeartbeat(preset string) AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "project_role_initiative",
		Kind:     "global_metacognition",
		Cadence:  "every_30m",
		Priority: 40,
		Triggers: []string{
			"no_active_task",
			"all_public_tasks_closed",
			"missing_role_coverage",
			"post_mvp_quality_gap",
		},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"rhizome_read", "workspace_docs_read", "bounded_task_submit"},
		ContextSelectors: []string{
			"workspace_state",
			"project_contracts",
			"role_coverage",
			"role_memory",
			"reflection_boards",
			"service_pipeline",
			"recent_decisions",
			"open_quality_gaps",
		},
		OutputContracts:  []string{"local_memory", "initiative_proposal", "bounded_task_if_unowned"},
		PromotionSignals: []string{"unowned_high_value_gap", "service_candidate_with_evidence", "stalled_project_without_owner"},
		Objective:        "Maintain global project sensemaking and create one bounded public follow-up only when a high-value gap is unowned.",
		Instructions: []string{
			"read project state as context, not as authority to spam tasks",
			"prefer local sensemaking when active owners already cover the gap",
			"promote only one concrete task when the gap is current, actionable, and lacks an owner",
		},
		MemoryLanes: []string{"project_sensemaking", "opportunity_map", "role_backlog"},
		Notes:       []string{"Lets strategic roles create their own next useful public work only after local sensemaking."},
	})
}

func defaultGlobalProgressReviewHeartbeat(preset string) AgentHeartbeatSpec {
	requiresEvidence := true
	willEnabled := true
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:                "global_progress_review",
		Kind:              heartbeatKindGlobalProgressReview,
		Cadence:           "every_10m",
		Priority:          37,
		MaxParallel:       1,
		MaxToolIterations: 3,
		Triggers: []string{
			"fanout_absent",
			"idle_roster",
			"leader_no_product_fanout",
			"review_mesh_starved",
			"project_stall_suspected",
		},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"rhizome_read", "workspace_docs_read", "project_governance_review"},
		ContextSelectors: []string{
			"workspace_state",
			"project_coordination",
			"peer_roster",
			"patch_queue",
			"throughput_window",
			"recent_runtime_events",
		},
		OutputContracts:  []string{"local_memory", globalProgressReviewContract, "will_directive"},
		PromotionSignals: []string{"objective_fanout_stall", "idle_roster_with_no_product_lanes", "review_layer_starved"},
		Objective:        "Review global project progress for objective fanout/review stalls without taking over implementation work.",
		Instructions: []string{
			"treat this heartbeat as a sensing and governance-review loop, not as implementation authority",
			"use project_governance_challenge only when the tool is available and only after action=check reports all strict predicates true",
			"raise at most one challenge per heartbeat and include durable evidence refs or workspace doc keys; never raise from subjective disagreement",
			"do not create product tasks here; a healthy strategic lead or active implementer should own fanout and execution",
			"if predicates fail or evidence is ambiguous, publish at most an advisory observation and keep the incumbent authority intact",
		},
		MemoryLanes: []string{globalProgressReviewMemoryLane, "project_sensemaking", "role_backlog"},
		ActiveMemory: &AgentHeartbeatActiveMemorySpec{
			Lane:       globalProgressReviewMemoryLane,
			MaxEntries: 6,
		},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			Enabled:           &willEnabled,
			AllowedActions:    []string{"advisory_signal", "publish_rhizome_update"},
			MaxDirectives:     2,
			RequiresEvidence:  &requiresEvidence,
			PublishVisibility: "rhizome",
		},
		Notes: []string{"Default-on for strategy/review/critic presets only; inert for implementers unless explicitly configured."},
	})
}

// defaultSystemSensorHeartbeat is the Layer C workspace-awareness sensor. It is
// returned DISABLED (opt-in per agent) and is intentionally NOT injected into any
// preset, so an agent without explicit config behaves byte-identically to legacy.
// Operators add it to agent.anatomy.json (or a preset) to switch it on. The spec
// is read-only and non-preemptive by construction; validateSystemSensingHeartbeat
// rejects any config that violates those invariants.
func defaultSystemSensorHeartbeat() AgentHeartbeatSpec {
	enabled := false
	requiresEvidence := true
	willEnabled := true
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:         "system_sensor",
		Kind:       heartbeatKindSystemSensing,
		Cadence:    "every_10m",
		Priority:   40,
		Enabled:    &enabled,
		Triggers:   []string{"workspace_capacity_available", "queue_health_unknown"},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"rhizome_read"},
		ContextSelectors: []string{
			"workspace_queue",
			"peer_roster",
			"throughput_window",
		},
		OutputContracts:   []string{"local_memory", systemObservationContract},
		PromotionSignals:  []string{"recurring_system_tension"},
		MaxParallel:       1,
		MaxToolIterations: 3,
		Objective:         "Sense workspace health and surface system-level opportunities/risks; observe and advise without preempting active work.",
		Instructions: []string{
			"read workspace queue, peer roster, and throughput as a read-only snapshot",
			"record a one-line system tension to shared memory; never mutate runtime state",
			"emit at most advisory or publish directives; never resume, switch, or replan the agent's task",
		},
		MemoryLanes: []string{systemSensingMemoryLane},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			Enabled:           &willEnabled,
			AllowedActions:    []string{"advisory_signal", "publish_rhizome_update"},
			MaxDirectives:     2,
			RequiresEvidence:  &requiresEvidence,
			PublishVisibility: "workspace",
		},
		Notes: []string{"Layer C workspace-awareness sensor; opt-in, read-only, non-preemptive."},
	})
}

// defaultStrategySynthesisHeartbeat is the Layer D low-frequency, higher-budget
// consolidation pass: it reads the memory tail + constitution + reflection channel,
// reconciles contradictory rules (by appending superseding evidence — never
// mutating stored rules in place), prunes stale ones, and rewrites the strategy
// doc. Opt-in by default; presets that want it enable it explicitly.
func defaultStrategySynthesisHeartbeat() AgentHeartbeatSpec {
	enabled := false
	requiresEvidence := true
	willEnabled := true
	includeSummaries := true
	includeBacklog := true
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:         "strategy_synthesis",
		Kind:       heartbeatKindStrategySynthesis,
		Cadence:    "every_2h",
		Priority:   30,
		Enabled:    &enabled,
		Triggers:   []string{"rule_set_drift_suspected", "reflection_channel_backlog"},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"rhizome_read", "memory_and_docs_read", "workspace_docs_read"},
		ContextSelectors: []string{
			"workspace_queue",
			"throughput_window",
		},
		OutputContracts:   []string{"local_memory", strategySynthesisContract},
		PromotionSignals:  []string{"resolved_rule_conflict"},
		MaxParallel:       1,
		MaxToolIterations: 8,
		Objective:         "Consolidate memory + constitution, resolve rule conflicts, prune stale rules, and rewrite the strategy doc; observe and advise without preempting active work.",
		Instructions: []string{
			"read the recent memory tail, the constitution view, and the reflection channel",
			"detect contradictory rules (procedure vs anti_procedure on colliding action bases) and record an explicit superseding memory node; never mutate stored rules in place",
			"flag rules whose evidence has not recurred past the staleness horizon for de-globalization",
			"rewrite agent.<id>.strategy_synthesis to a stable representation of the consolidated rule set; re-running with no new evidence must not produce a meaningful diff",
			"emit at most advisory or publish directives; never resume, switch, or replan the agent's task",
		},
		MemoryLanes: []string{"project_sensemaking", "self_check", systemSensingMemoryLane},
		ActiveMemory: &AgentHeartbeatActiveMemorySpec{
			// Read wide: the normalizer clamps active-memory to 20 entries (the
			// system ceiling), well above self_check's default of 6.
			Lane:                    "project_sensemaking",
			MaxEntries:              20,
			IncludeSessionSummaries: &includeSummaries,
			IncludeBacklog:          &includeBacklog,
		},
		WillPolicy: &AgentHeartbeatWillPolicySpec{
			Enabled:           &willEnabled,
			AllowedActions:    []string{"advisory_signal", "publish_rhizome_update"},
			MaxDirectives:     3,
			RequiresEvidence:  &requiresEvidence,
			PublishVisibility: "workspace",
		},
		Notes: []string{"Layer D rule-consolidation pass; opt-in, read-only, non-preemptive, append-only."},
	})
}

func defaultPortfolioScoutHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "portfolio_scout",
		Kind:     "global_metacognition",
		Cadence:  "every_45m",
		Priority: 38,
		Triggers: []string{
			"portfolio_capacity_available",
			"service_pipeline_idle",
			"opportunity_notes_requested",
			"no_active_build_direction",
		},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"rhizome_read", "workspace_docs_read", "bounded_task_submit"},
		ContextSelectors: []string{
			"workspace_state",
			"service_pipeline",
			"reflection_boards",
			"recent_decisions",
			"recent_verification",
		},
		OutputContracts:  []string{"local_memory", "opportunity_note", "bounded_task_if_unowned"},
		PromotionSignals: []string{"service_candidate_with_evidence", "portfolio_gap", "validation_plan_ready"},
		Objective:        "Scout one small, evidence-backed service opportunity and keep it private until the opportunity, validation path, and expected scope are concrete.",
		Instructions: []string{
			"compare service candidates against current portfolio capacity, build size, distribution, and validation signal",
			"record local scored opportunity notes before promoting public build work",
			"promote at most one bounded task only when the candidate has a target user, concrete pain, validation plan, and no active owner",
		},
		MemoryLanes: []string{"opportunity_map", "project_sensemaking", "role_backlog"},
		Notes:       []string{"Service-factory scout loop. It is configuration, not a hard-coded product type."},
	})
}

func defaultDeployMonetizationVigilanceHeartbeat() AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "deploy_monetization_vigilance",
		Kind:     "global_metacognition",
		Cadence:  "every_30m",
		Priority: 44,
		Triggers: []string{
			"local_mvp_without_deploy",
			"deployed_service_without_smoke",
			"monetization_blocker_untracked",
			"ad_policy_risk_unreviewed",
		},
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"rhizome_read", "workspace_docs_read", "bounded_task_submit"},
		ContextSelectors: []string{
			"workspace_state",
			"project_contracts",
			"service_pipeline",
			"recent_verification",
			"open_quality_gaps",
		},
		OutputContracts:  []string{"local_memory", "deploy_readiness_note", "bounded_task_if_unowned"},
		PromotionSignals: []string{"deploy_smoke_gap", "monetization_readiness_gap", "policy_review_gap"},
		Objective:        "Keep service MVPs from being called done until deploy, smoke, public-readiness, and monetization or policy blockers are explicit.",
		Instructions: []string{
			"separate local product completion from public service readiness",
			"treat credentials, paid actions, domain purchase, and ad-network approval as operator-gated blockers, not autonomous assumptions",
			"promote one bounded follow-up only when a readiness gap is current, actionable, and unowned",
		},
		MemoryLanes: []string{"project_sensemaking", "role_backlog", "promoted_refs"},
		Notes:       []string{"Deploy and monetization vigilance for week-scale service-factory runs."},
	})
}

func defaultPatchQueueVigilanceHeartbeat(preset string) AgentHeartbeatSpec {
	return normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{
		ID:       "patch_queue_vigilance",
		Kind:     "integration_vigilance",
		Cadence:  "every_12m",
		Priority: 55,
		Triggers: []string{
			"accepted_patch_waiting",
			"review_ready_without_reviewer",
			"integration_candidate_without_smoke",
			"owner_bound_queue_stale",
		},
		Locks:      []string{"patch_queue_read", "bounded_integration_claim"},
		ToolSuites: []string{"rhizome_read", "patch_queue_read", "local_tests_read", "bounded_task_submit"},
		ContextSelectors: []string{
			"patch_queue",
			"review_packets",
			"integration_state",
			"recent_verification",
		},
		OutputContracts:  []string{"local_memory", "queue_health_note", "bounded_integration_task_if_safe"},
		PromotionSignals: []string{"accepted_queue_stale", "missing_integration_owner", "verification_gap"},
		Objective:        "Keep accepted, review-ready, and verification-sensitive patch queue work from silently dying.",
		Instructions: []string{
			"inspect queue state and recent verification before proposing integration action",
			"record local queue health findings for stale or under-evidenced candidates",
			"avoid duplicate integration tasks when an active owner or fresh follow-up already exists",
		},
		MemoryLanes: []string{"role_backlog", "promoted_refs", "working_notes"},
		Notes:       []string{"Prevents accepted or review-ready work from silently dying in coordination state."},
	})
}

func defaultAgentAnatomyPresetForProfile(profile AgentProfile) string {
	coreText := agentAnatomyCoreProfileText(profile)
	fullText := agentAnatomyProfileText(profile)
	if preset := defaultAgentAnatomyPresetFromText(coreText); preset != "" {
		return preset
	}
	return firstNonEmpty(defaultAgentAnatomyPresetFromText(fullText), "generalist")
}

func defaultAgentAnatomyPresetFromText(profileText string) string {
	hasEvaluationSignal := agentAnatomyTextContainsAny(profileText, "review", "qa", "tester", "critic", "verifier", "verification", "acceptance", "audit", "smoke")
	hasImplementerSignal := agentAnatomyTextContainsAny(profileText, "implementer", "implementation", "frontend", "algorithm", "data implementer", "worker")
	hasStrongUISignal := agentAnatomyTextContainsAny(profileText, "ui/ux", " ux ", "visual", "browser critic", "design critic")
	hasVisualQASignal := agentAnatomyTextContainsAny(profileText, "browser smoke", "browser-smoke", "accessibility", "responsive", "responsiveness")
	switch {
	case strings.Contains(profileText, "service factory") ||
		strings.Contains(profileText, "service scout") ||
		strings.Contains(profileText, "market scout") ||
		strings.Contains(profileText, "portfolio") ||
		strings.Contains(profileText, "opportunity") ||
		strings.Contains(profileText, "growth") ||
		strings.Contains(profileText, "monetization") ||
		strings.Contains(profileText, "deploy") ||
		strings.Contains(profileText, "revenue") ||
		strings.Contains(profileText, "advertising") ||
		strings.Contains(profileText, "ad policy") ||
		strings.Contains(profileText, "ad-policy") ||
		strings.Contains(profileText, "cloudflare") ||
		strings.Contains(profileText, "vercel"):
		return "service_factory_operator"
	case strings.Contains(profileText, "integrator") ||
		strings.Contains(profileText, "integration"):
		return "integrator"
	case strings.Contains(profileText, "strateg") ||
		strings.Contains(profileText, "planner") ||
		strings.Contains(profileText, "product"):
		return "strategist"
	case hasImplementerSignal && !hasEvaluationSignal:
		return "generalist"
	case hasStrongUISignal || (hasVisualQASignal && hasEvaluationSignal):
		return "ui_ux_reality_critic"
	case hasEvaluationSignal:
		return "reviewer_qa"
	default:
		return ""
	}
}

func agentAnatomyTextContainsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func normalizeAgentAnatomyPreset(preset string) string {
	preset = strings.ToLower(strings.TrimSpace(preset))
	preset = strings.ReplaceAll(preset, "-", "_")
	preset = strings.ReplaceAll(preset, " ", "_")
	switch preset {
	case "", "auto", "default":
		return ""
	case "ui", "ux", "ui_ux", "ui_critic", "ux_critic", "visual_critic", "browser_critic", "ui_ux_critic":
		return "ui_ux_reality_critic"
	case "reviewer", "review", "qa", "tester", "critic":
		return "reviewer_qa"
	case "strategy", "strategic", "planner", "product", "product_planner":
		return "strategist"
	case "integration", "patch_integrator":
		return "integrator"
	case "service_factory", "service_scout", "portfolio_scout", "portfolio_steward", "deployment_operator", "monetization_operator", "deploy_operator", "service_operator", "growth_scout", "revenue_operator":
		return "service_factory_operator"
	case "general", "generalist", "implementer", "worker":
		return "generalist"
	default:
		return preset
	}
}

func defaultAgentAnatomyProfileID(profile AgentProfile) string {
	return firstNonEmpty(
		strings.TrimSpace(profile.AgentID),
		strings.TrimSpace(profile.DisplayName),
		strings.TrimSpace(profile.Role),
		"default",
	)
}

func agentAnatomyProfileText(profile AgentProfile) string {
	parts := []string{
		profile.AgentID,
		profile.DisplayName,
		profile.Role,
		profile.PrimarySpecialization,
		profile.DefaultWorkMode,
		profile.Mission,
	}
	parts = append(parts, profile.SecondarySpecializations...)
	return strings.ToLower(strings.Join(parts, " "))
}

func agentAnatomyCoreProfileText(profile AgentProfile) string {
	parts := []string{
		profile.AgentID,
		profile.DisplayName,
		profile.Role,
		profile.PrimarySpecialization,
		profile.DefaultWorkMode,
	}
	return strings.ToLower(strings.Join(parts, " "))
}
