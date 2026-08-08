package main

import (
	"context"
	"testing"
)

type sequenceLLM struct {
	responses []*LLMResponse
	calls     [][]Message
	tools     [][]ToolDef
}

func (l *sequenceLLM) Chat(_ context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	l.calls = append(l.calls, append([]Message(nil), messages...))
	l.tools = append(l.tools, append([]ToolDef(nil), tools...))
	if len(l.responses) == 0 {
		return &LLMResponse{}, nil
	}
	resp := l.responses[0]
	l.responses = l.responses[1:]
	return resp, nil
}

func TestRunToolLoopDetailedIncludesFinalAssistantMessage(t *testing.T) {
	llm := &sequenceLLM{
		responses: []*LLMResponse{{Content: "done"}},
	}

	run, err := RunToolLoopDetailed(context.Background(), llm, NewToolRegistry(), []Message{{Role: "user", Content: "hello"}}, nil, nil)
	if err != nil {
		t.Fatalf("RunToolLoopDetailed() error = %v", err)
	}
	if run.Content != "done" {
		t.Fatalf("expected final content to be recorded, got %q", run.Content)
	}
	if len(run.Messages) != 2 {
		t.Fatalf("expected transcript length 2, got %d", len(run.Messages))
	}
	if run.Messages[1].Role != "assistant" || run.Messages[1].Content != "done" {
		t.Fatalf("unexpected final assistant message: %+v", run.Messages[1])
	}
}

func TestChatSessionPreservesConversationHistory(t *testing.T) {
	llm := &sequenceLLM{
		responses: []*LLMResponse{
			{Content: "first reply"},
			{Content: "second reply"},
		},
	}

	agent := &Agent{
		LLM:     llm,
		Workdir: t.TempDir(),
	}
	agent.Init()

	session := agent.NewChatSession(AgentTaskContext{Mode: "tui"})
	if _, err := session.Send(context.Background(), "hello"); err != nil {
		t.Fatalf("first send error = %v", err)
	}
	if _, err := session.Send(context.Background(), "follow up"); err != nil {
		t.Fatalf("second send error = %v", err)
	}

	if len(llm.calls) != 2 {
		t.Fatalf("expected 2 llm calls, got %d", len(llm.calls))
	}
	second := llm.calls[1]
	if len(second) < 4 {
		t.Fatalf("expected prior conversation in second turn, got %+v", second)
	}
	if second[1].Role != "user" || second[1].Content != "hello" {
		t.Fatalf("missing first user turn in second call: %+v", second)
	}
	if second[2].Role != "assistant" || second[2].Content != "first reply" {
		t.Fatalf("missing first assistant turn in second call: %+v", second)
	}
	if second[3].Role != "user" || second[3].Content != "follow up" {
		t.Fatalf("missing follow-up user turn in second call: %+v", second)
	}
}
