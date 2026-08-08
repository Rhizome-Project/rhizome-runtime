package agent

import (
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// Re-export types from llm package for convenience.
type (
	Role         = llm.Role
	StopReason   = llm.StopReason
	ContentBlock = llm.ContentBlock
	Message      = llm.Message
	ToolResult   = llm.ToolResult
	Usage        = llm.Usage
	Response     = llm.Response
)

// Re-export constants.
const (
	RoleUser      = llm.RoleUser
	RoleAssistant = llm.RoleAssistant

	StopReasonEndTurn      = llm.StopReasonEndTurn
	StopReasonToolUse      = llm.StopReasonToolUse
	StopReasonMaxTokens    = llm.StopReasonMaxTokens
	StopReasonStopSequence = llm.StopReasonStopSequence
)

// Re-export constructors.
var (
	NewUserMessage       = llm.NewUserMessage
	NewToolResultMessage = llm.NewToolResultMessage
	NewAssistantMessage  = llm.NewAssistantMessage
)
