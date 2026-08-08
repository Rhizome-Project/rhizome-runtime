package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type managedAgentCredentialRefreshResult struct {
	AgentID      string
	WorkspaceID  string
	HostURL      string
	TokenPrefix  string
	RegisteredAt string
}

type managedAgentCredentialRefreshOptions struct {
	RegistrationRoles map[string]string
}

func runRefreshCredentials(args []string) error {
	flags := flag.NewFlagSet(appCommandName+" refresh-credentials", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	agentRef := flags.String("agent", "", "Managed agent reference to refresh; defaults to all managed agents")
	rosterJSON := flags.String("roster-json", "", "Run roster JSON; preset_target values are used as canonical registered roles")
	timeoutSec := flags.Int("timeout-sec", 30, "Per-agent registration timeout in seconds")
	if err := flags.Parse(args); err != nil {
		return err
	}

	records, err := managedAgentCredentialRefreshRecords(*agentRef)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return fmt.Errorf("no managed agents are registered locally")
	}
	registrationRoles, err := loadManagedCredentialRefreshRoleMap(*rosterJSON)
	if err != nil {
		return err
	}

	timeout := time.Duration(*timeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	failures := []string{}
	for _, record := range records {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		result, err := refreshManagedAgentCredentialWithOptions(ctx, record, managedAgentCredentialRefreshOptions{
			RegistrationRoles: registrationRoles,
		})
		cancel()
		if err != nil {
			failures = append(failures, record.AgentID)
			fmt.Fprintf(os.Stdout, "%s\tfail\t%s\n", record.AgentID, err)
			continue
		}
		fmt.Fprintf(os.Stdout, "%s\tpass\tworkspace=%s\thost=%s\ttoken_prefix=%s\n", result.AgentID, result.WorkspaceID, result.HostURL, result.TokenPrefix)
	}
	if len(failures) > 0 {
		return fmt.Errorf("credential refresh failed for: %s", strings.Join(failures, ", "))
	}
	return nil
}

func managedAgentCredentialRefreshRecords(agentRef string) ([]ManagedAgentRecord, error) {
	agentRef = strings.TrimSpace(agentRef)
	if agentRef != "" {
		record, err := ResolveManagedAgentReference(agentRef)
		if err != nil {
			return nil, err
		}
		return []ManagedAgentRecord{record}, nil
	}

	registry := LoadBotRegistry()
	records := make([]ManagedAgentRecord, 0, len(registry.Agents))
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		if strings.TrimSpace(record.AgentID) == "" || strings.TrimSpace(record.Workdir) == "" {
			continue
		}
		records = append(records, record)
	}
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].AgentID < records[j].AgentID
	})
	return records, nil
}

func refreshManagedAgentCredential(ctx context.Context, record ManagedAgentRecord) (managedAgentCredentialRefreshResult, error) {
	return refreshManagedAgentCredentialWithOptions(ctx, record, managedAgentCredentialRefreshOptions{})
}

func refreshManagedAgentCredentialWithOptions(ctx context.Context, record ManagedAgentRecord, opts managedAgentCredentialRefreshOptions) (managedAgentCredentialRefreshResult, error) {
	record = normalizeManagedAgentRecord(record)
	if strings.TrimSpace(record.AgentID) == "" || strings.TrimSpace(record.Workdir) == "" {
		return managedAgentCredentialRefreshResult{}, fmt.Errorf("managed agent requires agent_id and workdir")
	}

	cfg := managedAgentStartRuntimeConfig(record)
	defaults := LoadBotRegistry().Defaults
	cfg.RhizomeHost = firstNonEmpty(record.HostURL, cfg.RhizomeHost, defaults.HostURL)
	cfg.WorkspaceID = firstNonEmpty(record.WorkspaceID, cfg.WorkspaceID, defaults.WorkspaceID)
	cfg.WorkspacePassword = firstNonEmpty(cfg.WorkspacePassword, defaults.WorkspacePassword)
	cfg.OwnerUserID = firstNonEmpty(record.OwnerUserID, cfg.OwnerUserID, defaults.OwnerUserID)
	cfg.AgentID = firstNonEmpty(record.AgentID, cfg.AgentID)
	cfg.DisplayName = firstNonEmpty(record.DisplayName, cfg.DisplayName, cfg.AgentID)
	cfg.RhizomeToken = ""
	if strings.TrimSpace(record.HostURL) != "" {
		cfg.RhizomeRPC = defaultRPCEndpoint(record.HostURL)
	} else if strings.TrimSpace(cfg.RhizomeRPC) == "" && strings.TrimSpace(cfg.RhizomeHost) != "" {
		cfg.RhizomeRPC = defaultRPCEndpoint(cfg.RhizomeHost)
	}
	cfg.ApplyDefaults()
	runtimeRole := strings.TrimSpace(cfg.Role)
	registrationRole, err := managedCredentialRefreshRegistrationRole(cfg.AgentID, runtimeRole, opts.RegistrationRoles)
	if err != nil {
		return managedAgentCredentialRefreshResult{}, err
	}

	client := NewRhizomeClient(cfg.RhizomeRPC, "")
	registered, err := client.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID:       cfg.WorkspaceID,
		WorkspaceName:     cfg.WorkspaceName,
		WorkspacePassword: cfg.WorkspacePassword,
		HostURL:           cfg.RhizomeHost,
		AgentID:           cfg.AgentID,
		GroupID:           cfg.GroupID,
		DisplayName:       cfg.DisplayName,
		Role:              registrationRole,
		OwnerUserID:       cfg.OwnerUserID,
		Capabilities:      cfg.Capabilities,
		Status:            "REGISTERED",
		ProtocolVersion:   cfg.ProtocolVersion,
		Summary:           appCommandName + " refresh-credentials",
	})
	if err != nil {
		return managedAgentCredentialRefreshResult{}, fmt.Errorf("register agent: %w", err)
	}

	applyRegisterResultToConfig(&cfg, client, registered)
	if opts.RegistrationRoles != nil && runtimeRole != "" {
		cfg.Role = runtimeRole
	}
	if err := persistRuntimeProfiles(cfg.Workdir, cfg, registered, nil); err != nil {
		return managedAgentCredentialRefreshResult{}, fmt.Errorf("persist runtime profiles: %w", err)
	}

	return managedAgentCredentialRefreshResult{
		AgentID:      firstNonEmpty(registered.AgentID, registered.Agent.AgentID, cfg.AgentID),
		WorkspaceID:  firstNonEmpty(registered.WorkspaceID, registered.Agent.WorkspaceID, cfg.WorkspaceID),
		HostURL:      firstNonEmpty(registered.HostURL, cfg.RhizomeHost, hostURLForRPC(cfg.RhizomeRPC)),
		TokenPrefix:  safeTokenPrefix(firstNonEmpty(registered.Token, cfg.RhizomeToken)),
		RegisteredAt: firstNonEmpty(registered.Agent.UpdatedAt, registered.Agent.CreatedAt),
	}, nil
}

func loadManagedCredentialRefreshRoleMap(path string) (map[string]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read roster json: %w", err)
	}
	var roster map[string]struct {
		PresetTarget string `json:"preset_target"`
	}
	if err := json.Unmarshal(raw, &roster); err != nil {
		return nil, fmt.Errorf("decode roster json: %w", err)
	}
	if len(roster) == 0 {
		return nil, fmt.Errorf("roster json has no agents")
	}
	roles := make(map[string]string, len(roster))
	for agentID, entry := range roster {
		agentID = strings.TrimSpace(agentID)
		role := strings.TrimSpace(entry.PresetTarget)
		if agentID == "" {
			return nil, fmt.Errorf("roster json contains an empty agent id")
		}
		if role == "" {
			return nil, fmt.Errorf("roster json agent %s is missing preset_target", agentID)
		}
		roles[agentID] = role
	}
	return roles, nil
}

func managedCredentialRefreshRegistrationRole(agentID, runtimeRole string, registrationRoles map[string]string) (string, error) {
	if registrationRoles == nil {
		return strings.TrimSpace(runtimeRole), nil
	}
	agentID = strings.TrimSpace(agentID)
	role := strings.TrimSpace(registrationRoles[agentID])
	if role == "" {
		return "", fmt.Errorf("roster json does not provide preset_target for managed agent %s", agentID)
	}
	return role, nil
}

func safeTokenPrefix(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if len(token) < 12 {
		return "<redacted>"
	}
	return token[:8]
}
