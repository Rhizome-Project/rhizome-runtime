package main

import (
	"context"
	"fmt"
	"strings"
)

type ChatToolEvent struct {
	Name      string
	Arguments string
	Output    string
	IsError   bool
}

type ChatTurn struct {
	Input      string
	Response   string
	ToolEvents []ChatToolEvent
}

type ChatSession struct {
	agent   *Agent
	taskCtx AgentTaskContext
	system  string
	history []Message
}

func NewChatSession(agent *Agent, taskCtx AgentTaskContext) *ChatSession {
	if agent == nil {
		panic("chat session requires an agent")
	}
	taskCtx.Mode = firstNonEmpty(taskCtx.Mode, "tui")
	system := agent.buildSystemPrompt(taskCtx)
	return &ChatSession{
		agent:   agent,
		taskCtx: taskCtx,
		system:  system,
		history: []Message{{Role: "system", Content: system}},
	}
}

func (a *Agent) NewChatSession(taskCtx AgentTaskContext) *ChatSession {
	return NewChatSession(a, taskCtx)
}

func (s *ChatSession) Reset() {
	s.history = []Message{{Role: "system", Content: s.system}}
}

func (s *ChatSession) History() []Message {
	return append([]Message(nil), s.history...)
}

func (s *ChatSession) Send(ctx context.Context, input string) (*ChatTurn, error) {
	if s == nil || s.agent == nil {
		return nil, fmt.Errorf("chat session is not initialized")
	}
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, fmt.Errorf("message is empty")
	}

	recorder := &chatTurnRecorder{}
	messages := append(append([]Message(nil), s.history...), Message{
		Role:    "user",
		Content: trimmed,
	})

	run, err := RunToolLoopDetailed(ctx, s.agent.LLM, s.agent.registry, messages, nil, recorder)
	if err != nil {
		return nil, err
	}

	s.history = run.Messages
	return &ChatTurn{
		Input:      trimmed,
		Response:   run.Content,
		ToolEvents: recorder.events,
	}, nil
}

type chatTurnRecorder struct {
	events []ChatToolEvent
}

func (r *chatTurnRecorder) OnLLMResponse(_ int, _ *LLMResponse) {}

func (r *chatTurnRecorder) OnToolResult(_ int, call ToolCall, result ToolResult) {
	r.events = append(r.events, ChatToolEvent{
		Name:      call.Function.Name,
		Arguments: call.Function.Arguments,
		Output:    result.Output,
		IsError:   result.IsError,
	})
}
