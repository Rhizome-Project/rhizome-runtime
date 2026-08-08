package model

const (
	ToolStatusActive     = "ACTIVE"
	ToolStatusPlanned    = "PLANNED"
	ToolStatusBlocked    = "BLOCKED"
	ToolStatusDeprecated = "DEPRECATED"
)

const (
	ToolKindCLI         = "CLI"
	ToolKindService     = "SERVICE"
	ToolKindBot         = "BOT"
	ToolKindBridge      = "BRIDGE"
	ToolKindIntegration = "INTEGRATION"
	ToolKindSandbox     = "SANDBOX"
	ToolKindOther       = "OTHER"
)

const (
	ToolAccessWorkspace = "WORKSPACE"
	ToolAccessOwner     = "OWNER"
	ToolAccessHumanGate = "HUMAN_GATED"
	ToolAccessAgentOnly = "AGENT_ONLY"
)

var validToolStatuses = map[string]struct{}{
	ToolStatusActive:     {},
	ToolStatusPlanned:    {},
	ToolStatusBlocked:    {},
	ToolStatusDeprecated: {},
}

var validToolKinds = map[string]struct{}{
	ToolKindCLI:         {},
	ToolKindService:     {},
	ToolKindBot:         {},
	ToolKindBridge:      {},
	ToolKindIntegration: {},
	ToolKindSandbox:     {},
	ToolKindOther:       {},
}

var validToolAccessLevels = map[string]struct{}{
	ToolAccessWorkspace: {},
	ToolAccessOwner:     {},
	ToolAccessHumanGate: {},
	ToolAccessAgentOnly: {},
}

func ValidToolStatus(status string) bool {
	_, ok := validToolStatuses[status]
	return ok
}

func ValidToolKind(kind string) bool {
	_, ok := validToolKinds[kind]
	return ok
}

func ValidToolAccessLevel(level string) bool {
	_, ok := validToolAccessLevels[level]
	return ok
}
