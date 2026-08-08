package main

import (
	"context"
	"encoding/json"
	"testing"
)

func TestScriptedFakeLLMNormalCompleteReturnsStructuredJSONWithoutExternalTools(t *testing.T) {
	llm := NewScriptedFakeLLM("normal_complete")

	resp, err := llm.Chat(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("fake Chat() error = %v", err)
	}
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("credential-free fake path requested unavailable tools: %+v", resp.ToolCalls)
	}
	var result StructuredTaskResult
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("expected structured JSON result, got %q: %v", resp.Content, err)
	}
	if result.Outcome != "completed" || result.Summary == "" {
		t.Fatalf("unexpected structured result: %+v", result)
	}
}

func TestScriptedFakeLLMBlockedReturnsStructuredBlockedResult(t *testing.T) {
	llm := NewScriptedFakeLLM("blocked")

	resp, err := llm.Chat(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("blocked fake Chat() error = %v", err)
	}
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected blocked fake turn to avoid tool calls, got %+v", resp.ToolCalls)
	}
	var result StructuredTaskResult
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("expected structured JSON result, got %q: %v", resp.Content, err)
	}
	if result.Outcome != "blocked" || result.Summary == "" || len(result.BlockedOn) == 0 {
		t.Fatalf("unexpected blocked structured result: %+v", result)
	}
	if result.BlockedOn[0].Kind != "operator" || result.BlockedOn[0].Detail == "" {
		t.Fatalf("expected operator blocker detail, got %+v", result.BlockedOn)
	}
}

func TestScriptedFakeLLMTimeoutCallsToolThenBlocksAfterTimeout(t *testing.T) {
	llm := NewScriptedFakeLLM("timeout")

	first, err := llm.Chat(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("first fake Chat() error = %v", err)
	}
	if len(first.ToolCalls) != 1 {
		t.Fatalf("expected timeout fake turn to request one tool call, got %+v", first.ToolCalls)
	}
	if got := first.ToolCalls[0].Function.Name; got != "fake_timeout_tool" {
		t.Fatalf("expected fake_timeout_tool call, got %q", got)
	}
	transcript := []Message{
		{Role: "assistant", Content: first.Content, ToolCalls: first.ToolCalls},
		{Role: "tool", ToolCallID: first.ToolCalls[0].ID, Content: `workspace tool fake_timeout_tool timed out

{"tool_id":"fake_timeout_tool","stdout":"{\"marker\":\"scenario.timeout\"}","stderr":"","exit_code":-1,"timed_out":true}`},
	}
	second, err := llm.Chat(context.Background(), transcript, nil)
	if err != nil {
		t.Fatalf("second fake Chat() error = %v", err)
	}
	if len(second.ToolCalls) != 0 {
		t.Fatalf("expected second fake turn to stop tool calls, got %+v", second.ToolCalls)
	}
	var result StructuredTaskResult
	if err := json.Unmarshal([]byte(second.Content), &result); err != nil {
		t.Fatalf("expected structured JSON result, got %q: %v", second.Content, err)
	}
	if result.Outcome != "blocked" || len(result.BlockedOn) < 2 {
		t.Fatalf("expected blocked timeout result with tool/operator blockers, got %+v", result)
	}
}

func TestScriptedFakeLLMTimeoutFailsWithoutTimedOutToolResult(t *testing.T) {
	llm := NewScriptedFakeLLM("timeout")

	first, err := llm.Chat(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("first fake Chat() error = %v", err)
	}
	transcript := []Message{
		{Role: "assistant", Content: first.Content, ToolCalls: first.ToolCalls},
		{Role: "tool", ToolCallID: first.ToolCalls[0].ID, Content: `{"ok":true,"marker":"scenario.timeout","timed_out":false}`},
	}
	second, err := llm.Chat(context.Background(), transcript, nil)
	if err != nil {
		t.Fatalf("second fake Chat() error = %v", err)
	}
	var result StructuredTaskResult
	if err := json.Unmarshal([]byte(second.Content), &result); err != nil {
		t.Fatalf("expected structured JSON result, got %q: %v", second.Content, err)
	}
	if result.Outcome != "failed" {
		t.Fatalf("expected failed outcome after non-timeout tool result, got %+v", result)
	}
}
