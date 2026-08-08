package living

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the living agent configuration loaded from a YAML file.
type Config struct {
	ID                  string        `yaml:"id"`
	Role                string        `yaml:"role"`
	Mode                string        `yaml:"mode"`
	WorkspaceID         string        `yaml:"workspace_id"`
	TaskTypes           []string      `yaml:"task_types"`
	HeartbeatInterval   time.Duration `yaml:"-"`
	RawHeartbeat        string        `yaml:"heartbeat_interval"`
	SituationCheckEvery int           `yaml:"situation_check_every"`
	MaxConcurrentTasks  int           `yaml:"max_concurrent_tasks"`
	MaxRetries          int           `yaml:"max_retries"`
	ReflectEvery        int           `yaml:"reflect_every"`
	ContextThreshold    float64       `yaml:"context_threshold"`
	Models              ModelsConfig  `yaml:"models"`
	WorkerTools         []string      `yaml:"worker_tools"`
	Tools               []string      `yaml:"tools"`
	Plugins             []string      `yaml:"plugins"`
	Skills              []string      `yaml:"skills"`
	RhizomeURL          string        `yaml:"rhizome_url"`
	RedisURL            string        `yaml:"redis_url"`
	Memory              MemoryConfig  `yaml:"memory"`
	LLM                 LLMConfig     `yaml:"llm"`
}

// ModelsConfig specifies which models to use for different roles.
type ModelsConfig struct {
	Primary string `yaml:"primary"`
	Worker  string `yaml:"worker"`
	Cheap   string `yaml:"cheap"`
}

// MemoryConfig holds memory/storage settings.
type MemoryConfig struct {
	DBPath     string `yaml:"db_path"`
	MDPath     string `yaml:"md_path"`
	MaxResults int    `yaml:"max_results"`
}

// LLMConfig holds LLM provider settings.
type LLMConfig struct {
	APIKey     string        `yaml:"api_key"`
	Provider   string        `yaml:"provider"`
	BaseURL    string        `yaml:"base_url"`
	MaxTokens  int           `yaml:"max_tokens"`
	Timeout    time.Duration `yaml:"-"`
	RawTimeout string        `yaml:"timeout"`
}

var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// LoadConfig reads a YAML file at path, expands ${ENV_VAR} references,
// applies defaults, and validates required fields.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config YAML: %w", err)
	}

	expandEnvVars(&cfg)
	applyDefaults(&cfg)

	if err := parseDurations(&cfg); err != nil {
		return nil, err
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// expandEnvVars replaces ${ENV_VAR} placeholders in string fields with
// os.Getenv values. If the env var is empty, the placeholder is left intact.
func expandEnvVars(cfg *Config) {
	expand := func(s string) string {
		return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
			varName := envVarPattern.FindStringSubmatch(match)[1]
			val := os.Getenv(varName)
			if val == "" {
				return match // leave placeholder intact
			}
			return val
		})
	}

	cfg.ID = expand(cfg.ID)
	cfg.Role = expand(cfg.Role)
	cfg.Mode = expand(cfg.Mode)
	cfg.WorkspaceID = expand(cfg.WorkspaceID)
	cfg.RhizomeURL = expand(cfg.RhizomeURL)
	cfg.RedisURL = expand(cfg.RedisURL)
	cfg.Memory.DBPath = expand(cfg.Memory.DBPath)
	cfg.Memory.MDPath = expand(cfg.Memory.MDPath)
	cfg.LLM.APIKey = expand(cfg.LLM.APIKey)
	cfg.LLM.Provider = expand(cfg.LLM.Provider)
	cfg.LLM.BaseURL = expand(cfg.LLM.BaseURL)
	cfg.Models.Primary = expand(cfg.Models.Primary)
	cfg.Models.Worker = expand(cfg.Models.Worker)
	cfg.Models.Cheap = expand(cfg.Models.Cheap)

	for i, t := range cfg.TaskTypes {
		cfg.TaskTypes[i] = expand(t)
	}
	for i, t := range cfg.WorkerTools {
		cfg.WorkerTools[i] = expand(t)
	}
	for i, t := range cfg.Tools {
		cfg.Tools[i] = expand(t)
	}
	for i, t := range cfg.Plugins {
		cfg.Plugins[i] = expand(t)
	}
	for i, t := range cfg.Skills {
		cfg.Skills[i] = expand(t)
	}
}

func applyDefaults(cfg *Config) {
	if cfg.RawHeartbeat == "" {
		cfg.RawHeartbeat = "10s"
	}
	if strings.TrimSpace(cfg.Mode) == "" {
		cfg.Mode = string(RuntimeModeObserveOnly)
	}
	if cfg.SituationCheckEvery == 0 {
		cfg.SituationCheckEvery = 6
	}
	if cfg.MaxConcurrentTasks == 0 {
		cfg.MaxConcurrentTasks = 3
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 2
	}
	if cfg.ReflectEvery == 0 {
		cfg.ReflectEvery = 5
	}
	if cfg.ContextThreshold == 0 {
		cfg.ContextThreshold = 0.5
	}
	if cfg.WorkerTools == nil {
		cfg.WorkerTools = []string{"bash", "read", "write", "edit", "glob", "grep"}
	}
	if cfg.RedisURL == "" {
		cfg.RedisURL = "localhost:6379"
	}
	if cfg.Memory.DBPath == "" && cfg.ID != "" {
		cfg.Memory.DBPath = fmt.Sprintf("data/living-%s-memory.db", cfg.ID)
	}
	if cfg.Memory.MaxResults == 0 {
		cfg.Memory.MaxResults = 10
	}
	if cfg.LLM.Provider == "" {
		cfg.LLM.Provider = "claude"
	}
	if cfg.LLM.MaxTokens == 0 {
		cfg.LLM.MaxTokens = 8192
	}
	if cfg.LLM.RawTimeout == "" {
		cfg.LLM.RawTimeout = "120s"
	}

	// EC-5: clamp negative concurrency to 1 (after zero-default above)
	if cfg.MaxConcurrentTasks < 0 {
		cfg.MaxConcurrentTasks = 1
	}
}

func parseDurations(cfg *Config) error {
	var errs []error

	d, err := time.ParseDuration(cfg.RawHeartbeat)
	if err != nil {
		errs = append(errs, fmt.Errorf("invalid heartbeat_interval %q: %w", cfg.RawHeartbeat, err))
	} else {
		cfg.HeartbeatInterval = d
	}

	d, err = time.ParseDuration(cfg.LLM.RawTimeout)
	if err != nil {
		errs = append(errs, fmt.Errorf("invalid llm.timeout %q: %w", cfg.LLM.RawTimeout, err))
	} else {
		cfg.LLM.Timeout = d
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func validate(cfg *Config) error {
	var errs []error

	if cfg.ID == "" {
		errs = append(errs, fmt.Errorf("id is required"))
	}
	if cfg.Role == "" {
		errs = append(errs, fmt.Errorf("role is required"))
	}
	if !validRuntimeMode(cfg.Mode) {
		errs = append(errs, fmt.Errorf("mode must be one of %q, %q, %q", RuntimeModeObserveOnly, RuntimeModeLocalExecution, RuntimeModeRhizomeNative))
	}
	if cfg.WorkspaceID == "" {
		errs = append(errs, fmt.Errorf("workspace_id is required"))
	}
	if len(cfg.TaskTypes) == 0 {
		errs = append(errs, fmt.Errorf("task_types is required"))
	}
	if cfg.Models.Primary == "" {
		errs = append(errs, fmt.Errorf("models.primary is required"))
	}
	if cfg.Models.Worker == "" {
		errs = append(errs, fmt.Errorf("models.worker is required"))
	}
	if cfg.Models.Cheap == "" {
		errs = append(errs, fmt.Errorf("models.cheap is required"))
	}
	if cfg.RhizomeURL == "" {
		errs = append(errs, fmt.Errorf("rhizome_url is required"))
	}
	if cfg.LLM.APIKey == "" || strings.Contains(cfg.LLM.APIKey, "${") {
		errs = append(errs, fmt.Errorf("llm.api_key is required"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (cfg Config) RuntimeMode() RuntimeMode {
	return normalizeRuntimeMode(cfg.Mode)
}
