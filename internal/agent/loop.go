package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/hooks"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
)

const StopReasonMaxIterations StopReason = "max_iterations"

// CompactFunc summarizes conversation history to reduce token usage.
type CompactFunc func(ctx context.Context, messages []Message, systemPrompt string, llmClient *llm.Client) ([]Message, error)

// LoopStateSnapshot captures the current canonical runtime state of a loop.
type LoopStateSnapshot struct {
	Messages          []Message
	Iterations        int
	TotalInputTokens  int
	TotalOutputTokens int
	ToolCalls         int
	PromptTokens      int
	StopReason        StopReason
}

// LoopCompactionSnapshot captures a single compaction event and its before/after state.
type LoopCompactionSnapshot struct {
	Trigger         string
	TokenBudget     int
	Before          LoopStateSnapshot
	After           LoopStateSnapshot
	SummaryText     string
	CompactionError error
}

// LoopObserver receives best-effort runtime state notifications from the loop.
type LoopObserver interface {
	OnState(ctx context.Context, snapshot LoopStateSnapshot) error
	OnCompaction(ctx context.Context, snapshot LoopCompactionSnapshot) error
}

// LoopConfig configures the agent loop.
type LoopConfig struct {
	MaxIterations       int
	MaxTokensPerRequest int
	TokenBudget         int
	ParallelToolCalls   bool
}

func (c LoopConfig) withDefaults() LoopConfig {
	if c.MaxIterations <= 0 {
		c.MaxIterations = 50
	}
	if c.MaxTokensPerRequest <= 0 {
		c.MaxTokensPerRequest = 8192
	}
	if c.TokenBudget <= 0 {
		c.TokenBudget = 100000
	}
	return c
}

// Loop is the core agent execution engine.
type Loop struct {
	llm          *llm.Client
	tools        *tools.Registry
	hooks        *hooks.Runner
	config       LoopConfig
	systemPrompt string
	compactFn    CompactFunc
	observer     LoopObserver
}

// NewLoop creates a new agent loop.
func NewLoop(llmClient *llm.Client, toolReg *tools.Registry, hookRunner *hooks.Runner, systemPrompt string, config LoopConfig) *Loop {
	return &Loop{
		llm:          llmClient,
		tools:        toolReg,
		hooks:        hookRunner,
		config:       config.withDefaults(),
		systemPrompt: systemPrompt,
	}
}

// SetCompactFunc sets the compaction function.
func (l *Loop) SetCompactFunc(fn CompactFunc) {
	l.compactFn = fn
}

// SetObserver registers an optional observer that receives best-effort runtime snapshots.
func (l *Loop) SetObserver(observer LoopObserver) {
	l.observer = observer
}

// LoopResult holds the outcome of an agent loop execution.
type LoopResult struct {
	FinalResponse     string
	Messages          []Message
	Iterations        int
	TotalInputTokens  int
	TotalOutputTokens int
	ToolCalls         int
	StopReason        StopReason
	Error             error
}

// Run executes the agent loop for the given task without dynamic context.
func (l *Loop) Run(ctx context.Context, task string) (*LoopResult, error) {
	return l.RunWithContext(ctx, task, "")
}

// RunWithContext executes the agent loop with an optional dynamic system context string.
func (l *Loop) RunWithContext(ctx context.Context, task string, dynamicContext string) (*LoopResult, error) {
	if task == "" {
		return nil, errors.New("task is required")
	}

	runSystemPrompt := l.systemPrompt
	if dynamicContext != "" {
		runSystemPrompt += "\n\n" + dynamicContext
	}

	// Build tool schemas for Claude API
	toolSchemas := l.buildToolSchemas()

	messages := []Message{NewUserMessage(task)}
	result := &LoopResult{}
	l.notifyState(ctx, messages, result)

	for i := 0; i < l.config.MaxIterations; i++ {
		result.Iterations = i + 1

		// Check token budget and compact if needed
		promptTokens := EstimateTokens(messages)
		if promptTokens > l.config.TokenBudget && l.compactFn != nil {
			beforeMessages := cloneMessages(messages)
			l.fireBeforeCompact(ctx, messages, promptTokens)

			compacted, err := l.compactFn(ctx, messages, runSystemPrompt, l.llm)
			if err != nil {
				log.Printf("[agent-loop] compaction failed: %v", err)
				l.notifyCompaction(ctx, LoopCompactionSnapshot{
					Trigger:         "token_budget_exceeded",
					TokenBudget:     l.config.TokenBudget,
					Before:          l.buildSnapshot(beforeMessages, result, ""),
					After:           l.buildSnapshot(beforeMessages, result, ""),
					SummaryText:     "",
					CompactionError: err,
				})
			} else if reflect.DeepEqual(messages, compacted) {
				log.Printf("[agent-loop] compaction returned unchanged state (%d messages)", len(messages))
			} else {
				log.Printf("[agent-loop] compacted %d messages to %d", len(messages), len(compacted))
				messages = compacted
				l.notifyCompaction(ctx, LoopCompactionSnapshot{
					Trigger:     "token_budget_exceeded",
					TokenBudget: l.config.TokenBudget,
					Before:      l.buildSnapshot(beforeMessages, result, ""),
					After:       l.buildSnapshot(messages, result, ""),
					SummaryText: firstMessageText(messages),
				})
				l.notifyState(ctx, messages, result)
			}
		} else if promptTokens > l.config.TokenBudget {
			log.Printf("[agent-loop] token budget exceeded (%d/%d) but no compact function set", promptTokens, l.config.TokenBudget)
		}

		// Hard constraint: evaluate tokens again and apply deterministic fallback if budget is still broken
		// This prevents API crashes when compactFn fails or if there's no compactFn
		if EstimateTokens(messages) > l.config.TokenBudget {
			log.Printf("[agent-loop] enforcing deterministic truncation fallback as budget %d is exceeded", l.config.TokenBudget)
			messages = deterministicFallback(messages, l.config.TokenBudget)
			l.notifyState(ctx, messages, result)
		}

		// Call LLM
		resp, err := l.llm.Send(ctx, llm.SendRequest{
			System:   runSystemPrompt,
			Messages: messages,
			Tools:    toolSchemas,
		})
		if err != nil {
			result.Error = err
			result.Messages = messages
			l.fireSessionEnd(ctx, messages, result, err)
			return result, nil
		}

		// Track tokens
		result.TotalInputTokens += resp.Usage.InputTokens
		result.TotalOutputTokens += resp.Usage.OutputTokens

		// Append assistant message
		assistantMsg := NewAssistantMessage(resp.Content)
		messages = append(messages, assistantMsg)
		l.notifyState(ctx, messages, result)

		log.Printf("[agent-loop] iteration %d: stop_reason=%s, tools=%d, tokens_in=%d, tokens_out=%d",
			i+1, resp.StopReason, len(assistantMsg.ToolUseBlocks()), resp.Usage.InputTokens, resp.Usage.OutputTokens)

		// No tool use — check if we should stop
		if !assistantMsg.HasToolUse() {
			hookResult := l.fireOnStop(ctx, messages, resp.StopReason)
			if hookResult.PreventStop {
				continueMsg := "Please continue."
				if hookResult.InjectSystemMessage != "" {
					continueMsg = hookResult.InjectSystemMessage
				}
				messages = append(messages, NewUserMessage(continueMsg))
				l.notifyState(ctx, messages, result)
				continue
			}
			result.StopReason = resp.StopReason
			result.FinalResponse = assistantMsg.TextContent()
			l.notifyState(ctx, messages, result)
			break
		}

		// Execute tools
		toolResults := l.executeTools(ctx, assistantMsg.ToolUseBlocks())
		result.ToolCalls += len(toolResults)
		messages = append(messages, NewToolResultMessage(toolResults))
		l.notifyState(ctx, messages, result)

		// Check if this was the last iteration
		if i == l.config.MaxIterations-1 {
			result.StopReason = StopReasonMaxIterations
			result.FinalResponse = assistantMsg.TextContent()
			l.notifyState(ctx, messages, result)
		}
	}

	result.Messages = messages
	l.notifyState(ctx, messages, result)
	l.fireSessionEnd(ctx, messages, result, nil)
	return result, nil
}

func (l *Loop) buildToolSchemas() []map[string]any {
	registered := l.tools.List()
	if len(registered) == 0 {
		return nil
	}
	schemas := make([]map[string]any, len(registered))
	for i, t := range registered {
		schemas[i] = tools.SchemaToClaudeFormat(t.Name(), t.Description(), t.Schema())
	}
	return schemas
}

func (l *Loop) executeTools(ctx context.Context, toolUseBlocks []ContentBlock) []ToolResult {
	results := make([]ToolResult, len(toolUseBlocks))

	if l.config.ParallelToolCalls && len(toolUseBlocks) > 1 {
		var wg sync.WaitGroup
		wg.Add(len(toolUseBlocks))
		for i, block := range toolUseBlocks {
			go func(idx int, b ContentBlock) {
				defer wg.Done()
				results[idx] = l.executeSingleTool(ctx, b)
			}(i, block)
		}
		wg.Wait()
	} else {
		for i, block := range toolUseBlocks {
			results[i] = l.executeSingleTool(ctx, block)
		}
	}

	return results
}

func (l *Loop) executeSingleTool(ctx context.Context, block ContentBlock) ToolResult {
	toolName := block.Name
	toolInput := block.Input

	// Fire BeforeTool hook
	hookResult, _ := l.hooks.Run(ctx, hooks.BeforeTool, hooks.Context{
		ToolName:  toolName,
		ToolInput: toolInput,
	})
	if hookResult.ModifiedInput != nil {
		toolInput = json.RawMessage(hookResult.ModifiedInput)
	}

	// Look up tool
	tool, ok := l.tools.Get(toolName)
	if !ok {
		result := ToolResult{
			ToolUseID: block.ID,
			Content:   fmt.Sprintf("Unknown tool: %s", toolName),
			IsError:   true,
		}
		l.hooks.Run(ctx, hooks.AfterTool, hooks.Context{
			ToolName:  toolName,
			ToolInput: toolInput,
			ToolError: fmt.Errorf("unknown tool: %s", toolName),
		})
		return result
	}

	// Execute tool
	output, err := tool.Execute(ctx, toolInput)

	// Fire AfterTool hook
	hookCtx := hooks.Context{
		ToolName:   toolName,
		ToolInput:  toolInput,
		ToolOutput: output,
		ToolError:  err,
	}
	afterResult, _ := l.hooks.Run(ctx, hooks.AfterTool, hookCtx)

	if afterResult.InjectSystemMessage != "" {
		output += "\n[SYSTEM: " + afterResult.InjectSystemMessage + "]"
	}

	if err != nil {
		return ToolResult{
			ToolUseID: block.ID,
			Content:   err.Error(),
			IsError:   true,
		}
	}

	return ToolResult{
		ToolUseID: block.ID,
		Content:   output,
	}
}

func (l *Loop) fireOnStop(ctx context.Context, messages []Message, stopReason StopReason) hooks.Result {
	result, _ := l.hooks.Run(ctx, hooks.OnStop, hooks.Context{
		StopReason:   string(stopReason),
		MessageCount: len(messages),
	})
	return result
}

func (l *Loop) fireBeforeCompact(ctx context.Context, messages []Message, totalTokens int) {
	l.hooks.Run(ctx, hooks.BeforeCompact, hooks.Context{
		MessageCount: len(messages),
		TotalTokens:  totalTokens,
	})
}

func (l *Loop) fireSessionEnd(ctx context.Context, messages []Message, result *LoopResult, err error) {
	l.hooks.Run(ctx, hooks.SessionEnd, hooks.Context{
		MessageCount: len(messages),
		TotalTokens:  result.TotalInputTokens + result.TotalOutputTokens,
		Error:        err,
	})
}

func (l *Loop) buildSnapshot(messages []Message, result *LoopResult, stopReason StopReason) LoopStateSnapshot {
	snapshot := LoopStateSnapshot{
		Messages:          cloneMessages(messages),
		Iterations:        result.Iterations,
		TotalInputTokens:  result.TotalInputTokens,
		TotalOutputTokens: result.TotalOutputTokens,
		ToolCalls:         result.ToolCalls,
		PromptTokens:      EstimateTokens(messages),
		StopReason:        stopReason,
	}
	if snapshot.StopReason == "" {
		snapshot.StopReason = result.StopReason
	}
	return snapshot
}

func (l *Loop) notifyState(ctx context.Context, messages []Message, result *LoopResult) {
	if l.observer == nil {
		return
	}
	if err := l.observer.OnState(ctx, l.buildSnapshot(messages, result, "")); err != nil {
		log.Printf("[agent-loop] state observer error: %v", err)
	}
}

func (l *Loop) notifyCompaction(ctx context.Context, snapshot LoopCompactionSnapshot) {
	if l.observer == nil {
		return
	}
	if err := l.observer.OnCompaction(ctx, snapshot); err != nil {
		log.Printf("[agent-loop] compaction observer error: %v", err)
	}
}

func cloneMessages(messages []Message) []Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]Message, len(messages))
	copy(cloned, messages)
	return cloned
}

func firstMessageText(messages []Message) string {
	if len(messages) == 0 {
		return ""
	}
	return messages[0].TextContent()
}

func deterministicFallback(messages []Message, budget int) []Message {
	if len(messages) <= 3 {
		return messages
	}
	first := messages[0]
	keepCount := (len(messages) - 1) / 2

	for keepCount > 1 {
		splitAt := len(messages) - keepCount
		truncMsg := NewUserMessage(fmt.Sprintf("[System: Hard truncation applied. %d messages were removed to satisfy token limits.]", splitAt-1))

		candidate := make([]Message, 0, 2+keepCount)
		candidate = append(candidate, first)
		candidate = append(candidate, truncMsg)
		candidate = append(candidate, messages[splitAt:]...)

		if EstimateTokens(candidate) <= budget {
			return candidate
		}
		keepCount /= 2
	}

	truncMsg := NewUserMessage("[System: Maximum truncation applied. Only original task and last message remain.]")
	return []Message{first, truncMsg, messages[len(messages)-1]}
}
