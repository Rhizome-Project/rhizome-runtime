package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const defaultOpenAIBaseURL = "https://api.openai.com/v1"
const defaultOpenRouterBaseURL = "https://openrouter.ai/api/v1"

const (
	providerDriverOpenAICompatible = "openai_compatible"
	providerDriverOpenRouter       = "openrouter"
)

func providerRecordForRuntime(cfg RuntimeConfig) (ProviderRecord, bool) {
	providerID := strings.TrimSpace(cfg.ProviderID)
	if providerID == "" {
		return ProviderRecord{}, false
	}
	return FindProviderRecord(providerID)
}

func runtimeProviderRecord(cfg RuntimeConfig) (ProviderRecord, error) {
	providerID := strings.TrimSpace(cfg.ProviderID)
	if providerID == "" {
		return ProviderRecord{}, nil
	}
	return loadEnabledProviderRecord(providerID)
}

func providerOpenAIBaseURL(provider ProviderRecord) string {
	if provider.ProviderID == "" || provider.ChannelType != providerChannelAPI {
		return ""
	}
	if provider.Driver == providerDriverOpenRouter && strings.TrimSpace(provider.API.BaseURL) == "" {
		return defaultOpenRouterBaseURL
	}
	return strings.TrimSpace(provider.API.BaseURL)
}

func providerOpenAIPublicHeaders(provider ProviderRecord) map[string]string {
	if provider.ProviderID == "" || provider.ChannelType != providerChannelAPI || len(provider.API.PublicHeaders) == 0 {
		return nil
	}
	headers := make(map[string]string, len(provider.API.PublicHeaders))
	for key, value := range provider.API.PublicHeaders {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		headers[key] = value
	}
	if len(headers) == 0 {
		return nil
	}
	return headers
}

func providerCodexExecutable(provider ProviderRecord, fallback string) string {
	spec, err := providerCodexLaunchSpec(provider, fallback)
	if err != nil {
		return ""
	}
	return spec.Executable
}

type providerCodexSpec struct {
	Executable     string
	Args           []string
	UseManagedHome bool
}

func providerCodexLaunchSpec(provider ProviderRecord, fallback string) (providerCodexSpec, error) {
	spec := providerCodexSpec{
		Executable: strings.TrimSpace(fallback),
	}
	if provider.ProviderID != "" && provider.ChannelType == providerChannelBridge && provider.Driver == llmBackendCodex {
		spec.UseManagedHome = provider.Bridge.UseManagedHome
		if executable := strings.TrimSpace(provider.Bridge.Executable); executable != "" {
			spec.Executable = executable
		}
		if command := strings.TrimSpace(provider.Bridge.Command); command != "" {
			parts, err := splitManagerCommand(command)
			if err != nil {
				return providerCodexSpec{}, err
			}
			if len(parts) > 0 {
				spec.Executable = strings.TrimSpace(parts[0])
				spec.Args = append([]string(nil), parts[1:]...)
			}
		}
	}
	return spec, nil
}

type providerQwenSpec struct {
	Executable string
	Args       []string
}

func providerQwenLaunchSpec(provider ProviderRecord, fallback string) (providerQwenSpec, error) {
	spec := providerQwenSpec{
		Executable: strings.TrimSpace(fallback),
	}
	if provider.ProviderID != "" && provider.ChannelType == providerChannelBridge && provider.Driver == llmBackendQwen {
		if executable := strings.TrimSpace(provider.Bridge.Executable); executable != "" {
			spec.Executable = executable
		}
		if command := strings.TrimSpace(provider.Bridge.Command); command != "" {
			parts, err := splitManagerCommand(command)
			if err != nil {
				return providerQwenSpec{}, err
			}
			if len(parts) > 0 {
				spec.Executable = strings.TrimSpace(parts[0])
				spec.Args = append([]string(nil), parts[1:]...)
			}
		}
	}
	return spec, nil
}

func findQwenExecutable() string {
	return sharedQwenExecutablePath()
}

func sharedQwenExecutablePath() string {
	if env := strings.TrimSpace(os.Getenv("QWEN_CLI_PATH")); env != "" {
		if executableFileExists(env) {
			return env
		}
	}

	names := []string{"qwen"}
	if runtime.GOOS == "windows" {
		names = []string{"qwen.cmd", "qwen.exe", "qwen"}
	}

	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		candidates := make([]string, 0, len(names)+2)
		if runtime.GOOS == "windows" {
			appData := strings.TrimSpace(os.Getenv("APPDATA"))
			if appData == "" {
				appData = filepath.Join(home, "AppData", "Roaming")
			}
			for _, name := range names {
				candidates = append(candidates, filepath.Join(appData, "npm", name))
			}
		}
		for _, name := range names {
			candidates = append(candidates, filepath.Join(home, ".local", "bin", name))
		}
		for _, candidate := range candidates {
			if executableFileExists(candidate) {
				return candidate
			}
		}
	}

	for _, name := range names {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	return ""
}

func executableFileExists(path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func providerUsesManagedCodexHome(provider ProviderRecord) bool {
	provider = normalizeProviderRecord(provider)
	return provider.ProviderID != "" &&
		provider.ChannelType == providerChannelBridge &&
		provider.Driver == llmBackendCodex &&
		provider.Bridge.UseManagedHome
}
