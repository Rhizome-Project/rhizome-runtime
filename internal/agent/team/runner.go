package team

import (
	"context"
	"fmt"
	"os"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent"
	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/auth"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// RunTeamOpts holds the options for running a team.
type RunTeamOpts struct {
	ConfigPath string
	Task       string
	Store      *sqlite.Store
	AppConfig  app.Config
}

// RunTeam loads a team config, resolves API keys, creates a coordinator, and
// runs the team. This is the top-level orchestration entry point between the
// CLI and the coordinator.
func RunTeam(ctx context.Context, opts RunTeamOpts) (*TeamResult, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if opts.Task == "" {
		return nil, fmt.Errorf("task is required")
	}

	cfg, err := LoadTeamConfig(opts.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("loading team config: %w", err)
	}

	if err := ValidateTeamConfig(cfg); err != nil {
		return nil, fmt.Errorf("validating team config: %w", err)
	}

	apiKeys, err := resolveAPIKeys(cfg, opts.AppConfig)
	if err != nil {
		return nil, err
	}

	spawner := agent.NewDefaultSpawner(opts.Store)
	coordinator := NewCoordinator(*cfg, spawner, apiKeys)

	return coordinator.Run(ctx, opts.Task)
}

// RunTeamFromConfig runs a team from an already-loaded TeamConfig, skipping
// file loading. Validation is still performed.
func RunTeamFromConfig(ctx context.Context, cfg TeamConfig, task string, store *sqlite.Store, appConfig app.Config) (*TeamResult, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if task == "" {
		return nil, fmt.Errorf("task is required")
	}

	if err := ValidateTeamConfig(&cfg); err != nil {
		return nil, fmt.Errorf("validating team config: %w", err)
	}

	apiKeys, err := resolveAPIKeys(&cfg, appConfig)
	if err != nil {
		return nil, err
	}

	spawner := agent.NewDefaultSpawner(store)
	coordinator := NewCoordinator(cfg, spawner, apiKeys)

	return coordinator.Run(ctx, task)
}

// resolveAPIKeys collects all unique providers from the team config and resolves
// an API key for each. Resolution priority:
//  1. AppConfig.LLMAPIKey if AppConfig.LLMProvider matches
//  2. Provider-specific env vars (OPENAI_API_KEY, ANTHROPIC_API_KEY)
//  3. auth.LoadCredentials from ~/.rhizome/auth.json (for openai provider)
//  4. Error if no key found
func resolveAPIKeys(cfg *TeamConfig, appConfig app.Config) (map[string]string, error) {
	// Collect unique providers.
	providers := make(map[string]bool)
	for _, a := range cfg.Agents {
		providers[a.Provider] = true
	}

	apiKeys := make(map[string]string)
	for provider := range providers {
		key, err := resolveKeyForProvider(provider, appConfig)
		if err != nil {
			return nil, err
		}
		apiKeys[provider] = key
	}

	return apiKeys, nil
}

// resolveKeyForProvider resolves an API key for a single provider.
func resolveKeyForProvider(provider string, appConfig app.Config) (string, error) {
	// 1. AppConfig.LLMAPIKey if provider matches.
	if appConfig.LLMProvider == provider && appConfig.LLMAPIKey != "" {
		return appConfig.LLMAPIKey, nil
	}

	// 2. Provider-specific env vars.
	switch provider {
	case "openai":
		if key := os.Getenv("OPENAI_API_KEY"); key != "" {
			return key, nil
		}
	case "claude":
		if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
			return key, nil
		}
	}

	// 3. auth.LoadCredentials fallback.
	authPath := auth.DefaultAuthFilePath()
	if authPath != "" {
		creds, err := auth.LoadCredentials(authPath)
		if err == nil && creds.APIKey != "" && creds.Provider == provider {
			return creds.APIKey, nil
		}
	}

	return "", fmt.Errorf("no API key found for provider %q: checked app config, environment variables, and %s",
		provider, auth.DefaultAuthFilePath())
}
