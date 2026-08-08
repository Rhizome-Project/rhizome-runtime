package main

import (
	"fmt"
	"os"
	"strings"
)

const managedAgentAllowLocalShellFlag = "RHIZOME_AGENT_ALLOW_LOCAL_SHELL"
const managedAgentAllowLocalMutationFlag = "RHIZOME_AGENT_ALLOW_LOCAL_MUTATION"
const managedAgentTrustedOwnerUserIDsFlag = "RHIZOME_AGENT_TRUSTED_OWNER_USER_IDS"

func isManagedAgentProcess() bool {
	value := strings.TrimSpace(os.Getenv(managedAgentEnvFlag))
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func trustedManagedShellOwnerUserID() string {
	owners := trustedManagedShellOwnerUserIDs()
	if len(owners) == 0 {
		return ""
	}
	return owners[0]
}

func trustedManagedShellOwnerUserIDs() []string {
	registry := LoadBotRegistry()
	global := LoadRhizomeProfile()
	return uniqueTrimmedCSVStrings([]string{
		os.Getenv(managedAgentTrustedOwnerUserIDsFlag),
		os.Getenv("RHIZOME_OWNER_USER_ID"),
		global.OwnerUserID,
		registry.Defaults.OwnerUserID,
	})
}

func managedOwnerAllowsLocalShell(ownerUserID string) bool {
	ownerUserID = strings.TrimSpace(ownerUserID)
	if ownerUserID == "" {
		return false
	}
	for _, trustedOwner := range trustedManagedShellOwnerUserIDs() {
		if strings.EqualFold(ownerUserID, trustedOwner) {
			return true
		}
	}
	return false
}

func runtimeAllowsLocalShell(cfg RuntimeConfig) bool {
	if runtimeTrustFirst(cfg) {
		return true
	}
	if cfg.Mode == RuntimeModeDaemon {
		if !isManagedAgentProcess() || !managedOwnerAllowsLocalShell(cfg.OwnerUserID) {
			return false
		}
		value := strings.TrimSpace(os.Getenv(managedAgentAllowLocalShellFlag))
		switch strings.ToLower(value) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	}
	if !isManagedAgentProcess() {
		return true
	}
	value := strings.TrimSpace(os.Getenv(managedAgentAllowLocalShellFlag))
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func managedRecordAllowsLocalShell(record ManagedAgentRecord) bool {
	return managedOwnerAllowsLocalShell(record.OwnerUserID)
}

func managedOwnerAllowsLocalMutation(ownerUserID string) bool {
	return managedOwnerAllowsLocalShell(ownerUserID)
}

func runtimeAllowsLocalMutation(cfg RuntimeConfig) bool {
	if runtimeTrustFirst(cfg) {
		return true
	}
	if cfg.Mode == RuntimeModeDaemon {
		if !isManagedAgentProcess() || !managedOwnerAllowsLocalMutation(cfg.OwnerUserID) {
			return false
		}
		value := strings.TrimSpace(os.Getenv(managedAgentAllowLocalMutationFlag))
		switch strings.ToLower(value) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	}
	if !isManagedAgentProcess() {
		return true
	}
	value := strings.TrimSpace(os.Getenv(managedAgentAllowLocalMutationFlag))
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func managedRecordAllowsLocalMutation(record ManagedAgentRecord) bool {
	return managedOwnerAllowsLocalMutation(record.OwnerUserID)
}

func managedOwnerAllowsInlineLocalChat(ownerUserID string) bool {
	return managedOwnerAllowsLocalShell(ownerUserID)
}

func managedRecordAllowsInlineLocalChat(record ManagedAgentRecord) bool {
	return managedOwnerAllowsInlineLocalChat(record.OwnerUserID)
}

func ensureManagedRecordAllowsInlineLocalChat(record ManagedAgentRecord) error {
	if managedRecordAllowsInlineLocalChat(record) {
		return nil
	}
	return fmt.Errorf("inline local chat is disabled for partner-managed agents; use the managed runtime surfaces instead")
}
