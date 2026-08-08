package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/x/term"
)

type onboardState struct {
	Runtime      RuntimeConfig
	AgentProfile AgentProfile
}

type onboardWizard struct {
	in            *bufio.Reader
	terminalInput *os.File
	out           io.Writer
}

func runOnboard(args []string) error {
	return runOnboardWithIO(args, os.Stdin, os.Stdout)
}

func runOnboardWithIO(args []string, input io.Reader, output io.Writer) error {
	if len(args) > 0 {
		switch strings.ToLower(strings.TrimSpace(args[0])) {
		case "-h", "--help", "help":
			printOnboardUsage(output)
			return nil
		}
	}

	workdir, err := resolveInitialWorkdir(args)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		return fmt.Errorf("create workdir: %w", err)
	}

	globalProfile := LoadRhizomeProfile()
	registry := LoadBotRegistry()
	localRuntime := LoadLocalRuntimeProfile(workdir)
	agentProfile := LoadAgentProfile(workdir)
	state := buildOnboardState(workdir, registry.Defaults, globalProfile, localRuntime, agentProfile)

	wizard := &onboardWizard{in: bufio.NewReader(input), out: output}
	if terminalInput, ok := input.(*os.File); ok && term.IsTerminal(terminalInput.Fd()) {
		wizard.terminalInput = terminalInput
	}
	state, err = wizard.Run(state)
	if err != nil {
		return err
	}

	fmt.Fprintln(wizard.out)
	fmt.Fprintln(wizard.out, "Registering agent in Rhizome...")
	registeredCfg, err := persistAndRegisterOnboardState(state)
	if err != nil {
		return err
	}

	fmt.Fprintln(wizard.out, "Registration succeeded. Launching TUI...")
	registeredCfg.Mode = RuntimeModeTUI
	return runWithConfig(registeredCfg)
}

func persistAndRegisterOnboardState(state onboardState) (RuntimeConfig, error) {
	workdir := strings.TrimSpace(state.Runtime.Workdir)
	if workdir == "" {
		return RuntimeConfig{}, fmt.Errorf("workdir is required")
	}
	if err := Bootstrap(workdir); err != nil {
		return RuntimeConfig{}, fmt.Errorf("bootstrap: %w", err)
	}
	if err := SaveAgentProfile(workdir, state.AgentProfile); err != nil {
		return RuntimeConfig{}, fmt.Errorf("save agent profile: %w", err)
	}
	if err := WriteAgentIdentityFiles(workdir, state.AgentProfile); err != nil {
		return RuntimeConfig{}, fmt.Errorf("write identity files: %w", err)
	}
	persistedCfg := state.Runtime
	persistedCfg.Mode = RuntimeModeDaemon
	if err := SaveLocalRuntimeProfile(workdir, localRuntimeProfileFromConfig(persistedCfg)); err != nil {
		return RuntimeConfig{}, fmt.Errorf("save local runtime profile: %w", err)
	}
	registeredCfg, err := registerOnboardAgent(persistedCfg)
	if err != nil {
		return RuntimeConfig{}, err
	}
	return registeredCfg, nil
}

func printOnboardUsage(w io.Writer) {
	fmt.Fprintf(w, "usage: %s onboard [--workdir PATH]\n", appCommandName)
	fmt.Fprintln(w, "runs interactive onboarding in the target agent folder and then starts TUI")
}

func buildOnboardState(workdir string, defaults BotManagerDefaults, global RhizomeConnectionProfile, local LocalRuntimeProfile, existing AgentProfile) onboardState {
	folderName := defaultFolderAgentName(workdir)
	registeredAgentID := strings.TrimSpace(local.RegisteredExecutor.AgentID)
	registeredDisplayName := strings.TrimSpace(local.RegisteredExecutor.DisplayName)
	registeredProtocolVersion := strings.TrimSpace(local.RegisteredExecutor.ProtocolVersion)
	defaultHost := firstNonEmpty(
		os.Getenv("RHIZOME_HOST"),
		defaults.HostURL,
		local.HostURL,
		global.HostURL,
		hostURLForRPC(firstNonEmpty(os.Getenv("RHIZOME_RPC"), local.RPCEndpoint, global.RPCEndpoint)),
		defaultRhizomeHostURL,
	)
	defaultRPC := firstNonEmpty(
		os.Getenv("RHIZOME_RPC"),
		defaultRPCEndpoint(defaultHost),
		local.RPCEndpoint,
		global.RPCEndpoint,
	)
	providerID := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_PROVIDER_ID"),
		defaults.DefaultProviderID,
		local.ProviderID,
	)
	modelOverride := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_MODEL"),
		defaults.ModelOverride,
		local.ModelOverride,
	)
	groupID := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_GROUP"),
		existing.GroupID,
		local.GroupID,
		defaults.GroupID,
	)
	agentID := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_ID"),
		registeredAgentID,
		suggestAgentID(workdir),
		existing.AgentID,
		local.AgentID,
		global.AgentID,
	)
	displayName := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_NAME"),
		registeredDisplayName,
		folderName,
		existing.DisplayName,
		local.DisplayName,
		agentID,
	)
	role := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_ROLE"),
		local.effectiveRole(),
		existing.Role,
		defaults.Role,
		"generalist",
	)
	backend := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_LLM_BACKEND"),
		defaults.LLMBackend,
		local.LLMBackend,
		defaultOnboardLLMBackend(),
	)
	model := firstNonEmpty(
		os.Getenv("RHIZOME_AGENT_MODEL"),
		defaults.Model,
		local.Model,
		defaultModel,
	)
	coordinationMode := firstNonEmpty(
		defaults.CoordinationMode,
		os.Getenv("RHIZOME_COORDINATION_MODE"),
		local.CoordinationMode,
		defaultCoordinationMode,
	)
	groupID, backend, model = applyProviderBinding(providerID, modelOverride, groupID, backend, model)

	cfg := RuntimeConfig{
		Mode:              RuntimeModeTUI,
		ProtocolVersion:   firstNonEmpty(registeredProtocolVersion, defaults.ProtocolVersion, defaultProtocolVersion),
		ProviderID:        providerID,
		ModelOverride:     modelOverride,
		LLMBackend:        backend,
		Model:             model,
		CoordinationMode:  coordinationMode,
		Workdir:           workdir,
		RhizomeRPC:        defaultRPC,
		RhizomeHost:       defaultHost,
		RhizomeToken:      firstNonEmpty(os.Getenv("RHIZOME_TOKEN"), local.AgentToken, global.AgentToken),
		WorkspaceID:       firstNonEmpty(os.Getenv("RHIZOME_WORKSPACE_ID"), defaults.WorkspaceID, defaultWorkspaceID, local.WorkspaceID, global.WorkspaceID),
		WorkspaceName:     firstNonEmpty(os.Getenv("RHIZOME_WORKSPACE_NAME"), local.WorkspaceName, global.WorkspaceName),
		WorkspacePassword: firstNonEmpty(os.Getenv("RHIZOME_WORKSPACE_PASSWORD"), defaults.WorkspacePassword, local.WorkspacePassword, global.WorkspacePassword),
		AgentID:           agentID,
		DisplayName:       displayName,
		OwnerUserID:       firstNonEmpty(os.Getenv("RHIZOME_OWNER_USER_ID"), defaults.OwnerUserID, local.effectiveOwnerUserID(), global.OwnerUserID),
		GroupID:           groupID,
		Role:              role,
		Capabilities:      firstCapabilities(defaults.Capabilities, local.effectiveCapabilities()),
	}
	cfg.ApplyDefaults()

	profile := normalizeAgentProfile(existing)
	if strings.TrimSpace(profile.AgentID) == "" {
		profile = DefaultAgentProfile(agentID, displayName, role)
	}
	profile.AgentID = agentID
	profile.DisplayName = displayName
	profile.Role = role
	profile.GroupID = groupID
	profile.PrimarySpecialization = firstNonEmpty(profile.PrimarySpecialization, role, "generalist builder-reviewer")

	return onboardState{
		Runtime:      cfg,
		AgentProfile: profile,
	}
}

func (w *onboardWizard) Run(state onboardState) (onboardState, error) {
	fmt.Fprintf(w.out, "%s onboard\n", appCommandName)
	fmt.Fprintf(w.out, "workdir: %s\n", state.Runtime.Workdir)
	fmt.Fprintln(w.out, "Press Enter to accept a default shown in parentheses.")
	fmt.Fprintln(w.out)

	var err error
	state.Runtime.AgentID, err = w.promptRequired("agent id", state.Runtime.AgentID)
	if err != nil {
		return state, err
	}
	state.Runtime.DisplayName, err = w.promptRequired("display name", firstNonEmpty(state.Runtime.DisplayName, humanizeAgentID(state.Runtime.AgentID)))
	if err != nil {
		return state, err
	}
	state.Runtime.OwnerUserID, err = w.promptRequired("owner user id", state.Runtime.OwnerUserID)
	if err != nil {
		return state, err
	}
	state.Runtime.RhizomeHost, err = w.promptRequired("rhizome host url", state.Runtime.RhizomeHost)
	if err != nil {
		return state, err
	}
	state.Runtime.WorkspaceID, err = w.promptRequired("workspace id", state.Runtime.WorkspaceID)
	if err != nil {
		return state, err
	}
	state.Runtime.WorkspacePassword, err = w.promptSecretRequired("workspace password", state.Runtime.WorkspacePassword)
	if err != nil {
		return state, err
	}
	state.Runtime.Role, err = w.promptRequired("role", firstNonEmpty(state.Runtime.Role, "generalist"))
	if err != nil {
		return state, err
	}

	state.AgentProfile.AgentID = state.Runtime.AgentID
	state.AgentProfile.DisplayName = state.Runtime.DisplayName
	state.AgentProfile.Role = state.Runtime.Role
	state.AgentProfile.PrimarySpecialization, err = w.promptRequired("primary specialization", firstNonEmpty(state.AgentProfile.PrimarySpecialization, state.Runtime.Role))
	if err != nil {
		return state, err
	}
	state.AgentProfile.SecondarySpecializations, err = w.promptCSV("secondary specializations", state.AgentProfile.SecondarySpecializations)
	if err != nil {
		return state, err
	}
	state.AgentProfile.DomainScope, err = w.promptCSV("domain scope", state.AgentProfile.DomainScope)
	if err != nil {
		return state, err
	}
	state.AgentProfile.Mission, err = w.prompt("mission", state.AgentProfile.Mission)
	if err != nil {
		return state, err
	}

	state.Runtime.LLMBackend, err = w.promptRequired("llm backend", firstNonEmpty(state.Runtime.LLMBackend, defaultOnboardLLMBackend()))
	if err != nil {
		return state, err
	}
	state.Runtime.Model, err = w.promptRequired("model", firstNonEmpty(state.Runtime.Model, defaultModel))
	if err != nil {
		return state, err
	}

	state.Runtime.RhizomeRPC = defaultRPCEndpoint(state.Runtime.RhizomeHost)
	state.Runtime.Mode = RuntimeModeTUI
	state.Runtime.Workdir = strings.TrimSpace(state.Runtime.Workdir)
	state.Runtime.DisplayName = firstNonEmpty(state.Runtime.DisplayName, state.Runtime.AgentID)
	state.AgentProfile = normalizeAgentProfile(state.AgentProfile)
	state.Runtime.ApplyDefaults()

	fmt.Fprintln(w.out)
	fmt.Fprintln(w.out, "Onboarding summary:")
	fmt.Fprintf(w.out, "- agent: %s (%s)\n", state.Runtime.DisplayName, state.Runtime.AgentID)
	fmt.Fprintf(w.out, "- owner user id: %s\n", state.Runtime.OwnerUserID)
	fmt.Fprintf(w.out, "- workspace: %s @ %s\n", state.Runtime.WorkspaceID, state.Runtime.RhizomeHost)
	fmt.Fprintf(w.out, "- role: %s\n", state.Runtime.Role)
	fmt.Fprintf(w.out, "- primary specialization: %s\n", state.AgentProfile.PrimarySpecialization)
	fmt.Fprintf(w.out, "- llm backend: %s\n", state.Runtime.LLMBackend)
	fmt.Fprintf(w.out, "- model: %s\n", state.Runtime.Model)
	fmt.Fprintln(w.out)

	return state, nil
}

func (w *onboardWizard) prompt(label, defaultValue string) (string, error) {
	if strings.TrimSpace(defaultValue) != "" {
		fmt.Fprintf(w.out, "%s (default: %s): ", label, defaultValue)
	} else {
		fmt.Fprintf(w.out, "%s: ", label)
	}
	line, err := w.in.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	value := strings.TrimSpace(line)
	if value == "" {
		value = strings.TrimSpace(defaultValue)
	}
	return value, nil
}

func (w *onboardWizard) promptRequired(label, defaultValue string) (string, error) {
	for {
		value, err := w.prompt(label, defaultValue)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(value) != "" {
			return value, nil
		}
		fmt.Fprintf(w.out, "%s is required.\n", label)
	}
}

func (w *onboardWizard) promptSecretRequired(label, configuredValue string) (string, error) {
	for {
		if strings.TrimSpace(configuredValue) != "" {
			fmt.Fprintf(w.out, "%s ([configured], Enter to keep): ", label)
		} else {
			fmt.Fprintf(w.out, "%s: ", label)
		}
		var value string
		if w.terminalInput != nil {
			secret, err := term.ReadPassword(w.terminalInput.Fd())
			fmt.Fprintln(w.out)
			if err != nil {
				return "", fmt.Errorf("read %s: %w", label, err)
			}
			value = strings.TrimSpace(string(secret))
		} else {
			line, err := w.in.ReadString('\n')
			if err != nil && err != io.EOF {
				return "", err
			}
			value = strings.TrimSpace(line)
		}
		if value == "" {
			value = strings.TrimSpace(configuredValue)
		}
		if value != "" {
			return value, nil
		}
		fmt.Fprintf(w.out, "%s is required.\n", label)
	}
}

func (w *onboardWizard) promptCSV(label string, defaults []string) ([]string, error) {
	value, err := w.prompt(label, strings.Join(defaults, ", "))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	return uniqueTrimmedCSVStrings([]string{value}), nil
}

func defaultOnboardLLMBackend() string {
	if hasChatGPTCodexSession() && findCodexExecutable() != "" {
		return llmBackendCodex
	}
	return llmBackendAuto
}

func suggestAgentID(workdir string) string {
	base := defaultFolderAgentName(workdir)
	if strings.TrimSpace(base) == "" {
		return "agent-01"
	}
	return sanitizePathComponent(base)
}

func defaultFolderAgentName(workdir string) string {
	base := filepath.Base(strings.TrimSpace(workdir))
	if strings.TrimSpace(base) == "" || base == "." || base == string(filepath.Separator) {
		return "agent-01"
	}
	return strings.TrimSpace(base)
}

func humanizeAgentID(agentID string) string {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "Agent"
	}
	parts := strings.FieldsFunc(agentID, func(r rune) bool {
		return r == '-' || r == '_' || r == '.'
	})
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}
	if len(parts) == 0 {
		return "Agent"
	}
	return strings.Join(parts, " ")
}

func registerOnboardAgent(cfg RuntimeConfig) (RuntimeConfig, error) {
	cfg.ApplyDefaults()
	client := NewRhizomeClient(cfg.RhizomeRPC, cfg.RhizomeToken)
	registered, err := client.RegisterAgent(context.Background(), AgentRegisterInput{
		WorkspaceID:       cfg.WorkspaceID,
		WorkspaceName:     cfg.WorkspaceName,
		WorkspacePassword: cfg.WorkspacePassword,
		HostURL:           cfg.RhizomeHost,
		AgentID:           cfg.AgentID,
		GroupID:           cfg.GroupID,
		DisplayName:       cfg.DisplayName,
		Role:              cfg.Role,
		OwnerUserID:       cfg.OwnerUserID,
		Capabilities:      cfg.Capabilities,
		Status:            "REGISTERED",
		ProtocolVersion:   cfg.ProtocolVersion,
		Summary:           appCommandName + " onboard",
	})
	if err != nil {
		return cfg, fmt.Errorf("register agent: %w", err)
	}
	applyRegisterResultToConfig(&cfg, client, registered)
	if err := persistRuntimeProfiles(cfg.Workdir, cfg, registered, nil); err != nil {
		return cfg, fmt.Errorf("persist runtime profiles: %w", err)
	}
	return cfg, nil
}

func resolveInitialWorkdir(args []string) (string, error) {
	if fromFlag := extractFlagValue(args, "workdir"); strings.TrimSpace(fromFlag) != "" {
		return filepath.Abs(strings.TrimSpace(fromFlag))
	}
	if fromEnv := strings.TrimSpace(os.Getenv("RHIZOME_AGENT_WORKDIR")); fromEnv != "" {
		return filepath.Abs(fromEnv)
	}
	return os.Getwd()
}

func extractFlagValue(args []string, name string) string {
	longFlag := "--" + strings.TrimLeft(strings.TrimSpace(name), "-")
	shortFlag := "-" + strings.TrimLeft(strings.TrimSpace(name), "-")
	for idx := 0; idx < len(args); idx++ {
		current := strings.TrimSpace(args[idx])
		switch {
		case current == longFlag || current == shortFlag:
			if idx+1 < len(args) {
				return strings.TrimSpace(args[idx+1])
			}
		case strings.HasPrefix(current, longFlag+"="):
			return strings.TrimSpace(strings.TrimPrefix(current, longFlag+"="))
		case strings.HasPrefix(current, shortFlag+"="):
			return strings.TrimSpace(strings.TrimPrefix(current, shortFlag+"="))
		}
	}
	return ""
}
