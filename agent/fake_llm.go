package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

const defaultFakeLLMScenario = "normal_complete"

type ScriptedFakeLLM struct {
	mu       sync.Mutex
	scenario string
	turn     int
}

type scriptedFakeTurn struct {
	content   string
	toolName  string
	arguments map[string]any
}

var scriptedFakeScenarios = map[string][]scriptedFakeTurn{
	"normal_complete": {
		{
			content: `{"outcome":"completed","summary":"deterministic fake evaluation completed","details":"The credential-free fake backend completed the task without contacting a model provider.","blocked_on":[]}`,
		},
	},
	"blocked": {
		{
			content: `{"outcome":"blocked","summary":"blocked live proof requires operator input","details":"scenario.blocked intentionally stops with durable blocked state","blocked_on":[{"kind":"operator","detail":"scenario.blocked deterministic blocker"}]}`,
		},
	},
	"timeout": {
		{
			content:  "fake LLM timeout: request deterministic timed-out workspace tool call",
			toolName: "fake_timeout_tool",
			arguments: map[string]any{
				"_timeout_sec": 1,
				"scenario":     "timeout",
				"sleep_sec":    5,
			},
		},
		{
			content: `{"outcome":"blocked","summary":"timeout live proof needs_operator after bounded tool timeout","details":"scenario.timeout observed fake_timeout_tool timed_out=true and stopped in durable BLOCKED needs_operator state","blocked_on":[{"kind":"tool_timeout","detail":"scenario.timeout fake_timeout_tool timed out"},{"kind":"operator","detail":"scenario.timeout needs_operator after bounded tool timeout"}]}`,
		},
	},
	"profile_gate_closed": {
		{
			content: "fake LLM profile_gate_closed: no daemon work should be claimed",
		},
	},
}

func NewScriptedFakeLLM(scenario string) *ScriptedFakeLLM {
	scenario = normalizeFakeLLMScenario(scenario)
	return &ScriptedFakeLLM{scenario: scenario}
}

func normalizeFakeLLMScenario(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == defaultModel {
		return defaultFakeLLMScenario
	}
	value = strings.TrimPrefix(value, "fake:")
	if _, ok := scriptedFakeScenarios[value]; ok {
		return value
	}
	return defaultFakeLLMScenario
}

func (l *ScriptedFakeLLM) Chat(_ context.Context, messages []Message, _ []ToolDef) (*LLMResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	turns := scriptedFakeScenarios[l.scenario]
	if len(turns) == 0 {
		return nil, fmt.Errorf("fake llm scenario %q has no scripted turns", l.scenario)
	}
	index := l.turn
	if index >= len(turns) {
		index = len(turns) - 1
	}
	l.turn++

	turn := turns[index]
	if l.scenario == "timeout" && index > 0 && !timeoutFakeToolTimedOut(messages) {
		turn = scriptedFakeTurn{
			content: `{"outcome":"failed","summary":"timeout fake_timeout_tool did not time out as expected","details":"fake_timeout_tool result did not contain timed_out=true and scenario.timeout markers","blocked_on":[{"kind":"tool_call","detail":"fake_timeout_tool did not time out as expected"}]}`,
		}
	}
	resp := &LLMResponse{
		Content: turn.content,
		Usage: TokenUsage{
			PromptTokens:     1,
			CompletionTokens: 1,
			TotalTokens:      2,
		},
	}
	if strings.TrimSpace(turn.toolName) != "" {
		raw, err := json.Marshal(turn.arguments)
		if err != nil {
			return nil, fmt.Errorf("marshal fake llm tool args: %w", err)
		}
		resp.ToolCalls = []ToolCall{
			{
				ID:   fmt.Sprintf("fake-%s-%d", l.scenario, l.turn),
				Type: "function",
				Function: FunctionCall{
					Name:      turn.toolName,
					Arguments: string(raw),
				},
			},
		}
	}
	return resp, nil
}

func timeoutFakeToolTimedOut(messages []Message) bool {
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		if !strings.EqualFold(strings.TrimSpace(msg.Role), "tool") {
			continue
		}
		if strings.TrimSpace(msg.ToolCallID) != "" && !strings.Contains(msg.ToolCallID, "timeout") {
			continue
		}
		content := strings.TrimSpace(msg.Content)
		lower := strings.ToLower(content)
		return strings.Contains(content, "scenario.timeout") &&
			(strings.Contains(lower, `"timed_out":true`) || strings.Contains(lower, `"timed_out": true`) || strings.Contains(lower, "timed out"))
	}
	return false
}
