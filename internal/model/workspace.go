package model

import "strings"

const (
	WorkspaceStatusActive   = "ACTIVE"
	WorkspaceStatusPaused   = "PAUSED"
	WorkspaceStatusArchived = "ARCHIVED"
)

const (
	AgentStatusRegistered = "REGISTERED"
	AgentStatusActive     = "ACTIVE"
	AgentStatusPaused     = "PAUSED"
	AgentStatusBlocked    = "BLOCKED"
	AgentStatusOffline    = "OFFLINE"
)

const (
	TaskClaimStatusClaimed   = "CLAIMED"
	TaskClaimStatusReleased  = "RELEASED"
	TaskClaimStatusCompleted = "COMPLETED"
	TaskClaimStatusBlocked   = "BLOCKED"
	TaskClaimStatusFailed    = "FAILED"
	TaskClaimStatusCancelled = "CANCELLED"
)

var validWorkspaceStatuses = map[string]struct{}{
	WorkspaceStatusActive:   {},
	WorkspaceStatusPaused:   {},
	WorkspaceStatusArchived: {},
}

var validAgentStatuses = map[string]struct{}{
	AgentStatusRegistered: {},
	AgentStatusActive:     {},
	AgentStatusPaused:     {},
	AgentStatusBlocked:    {},
	AgentStatusOffline:    {},
}

var validTaskClaimStatuses = map[string]struct{}{
	TaskClaimStatusClaimed:   {},
	TaskClaimStatusReleased:  {},
	TaskClaimStatusCompleted: {},
	TaskClaimStatusBlocked:   {},
	TaskClaimStatusFailed:    {},
	TaskClaimStatusCancelled: {},
}

// NormalizeStatus uppercases and trims a status string for case-insensitive matching.
func NormalizeStatus(status string) string {
	return strings.ToUpper(strings.TrimSpace(status))
}

func ValidWorkspaceStatus(status string) bool {
	_, ok := validWorkspaceStatuses[NormalizeStatus(status)]
	return ok
}

func ValidAgentStatus(status string) bool {
	_, ok := validAgentStatuses[NormalizeStatus(status)]
	return ok
}

func ValidTaskClaimStatus(status string) bool {
	_, ok := validTaskClaimStatuses[NormalizeStatus(status)]
	return ok
}
