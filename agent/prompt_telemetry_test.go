package main

import (
	"context"
	"encoding/json"
	"testing"
)

// fixtureTelemetryMessages builds a representative transcript: a system message,
// a first user task+work-packet message, an assistant turn with a tool call, a
// tool result, and a trailing assistant turn.
func fixtureTelemetryMessages() []Message {
	return []Message{
		{Role: "system", Content: "SYSTEM-INSTRUCTIONS"},
		{Role: "user", Content: "TASK-AND-WORK-PACKET-BODY"},
		{
			Role:    "assistant",
			Content: "ASSISTANT-THINKING",
			ToolCalls: []ToolCall{
				{
					ID:   "call-1",
					Type: "function",
					Function: FunctionCall{
						Name:      "read_file",
						Arguments: `{"path":"README.md"}`,
					},
				},
			},
		},
		{Role: "tool", Content: "TOOL-OUTPUT-CONTENT-BODY", ToolCallID: "call-1"},
		{Role: "assistant", Content: "FOLLOWUP-ASSISTANT-TURN"},
	}
}

func fixtureTelemetryTools() []ToolDef {
	return []ToolDef{
		{
			Type: "function",
			Function: ToolFunctionDef{
				Name:        "read_file",
				Description: "Read a file from disk",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
}

func TestComputePromptSectionBytesAttribution(t *testing.T) {
	messages := fixtureTelemetryMessages()
	toolsJSON := marshalToolsForTelemetry(fixtureTelemetryTools())
	const staticPreamble = 100

	sections := computePromptSectionBytes(messages, toolsJSON, staticPreamble, 0)

	if sections.StaticPreamble != staticPreamble {
		t.Fatalf("static_preamble = %d, want %d", sections.StaticPreamble, staticPreamble)
	}
	if want := len("SYSTEM-INSTRUCTIONS"); sections.System != want {
		t.Fatalf("system = %d, want %d", sections.System, want)
	}
	if want := len("TASK-AND-WORK-PACKET-BODY"); sections.TaskPacket != want {
		t.Fatalf("task_packet = %d, want %d", sections.TaskPacket, want)
	}
	if want := len("TOOL-OUTPUT-CONTENT-BODY"); sections.ToolOutputHistory != want {
		t.Fatalf("tool_output_history = %d, want %d", sections.ToolOutputHistory, want)
	}

	// history = assistant-with-toolcall content + tool-call name + tool-call
	// args + trailing assistant content. The first user message is the task
	// packet, not history.
	wantHistory := len("ASSISTANT-THINKING") + len("read_file") + len(`{"path":"README.md"}`) + len("FOLLOWUP-ASSISTANT-TURN")
	if sections.History != wantHistory {
		t.Fatalf("history = %d, want %d", sections.History, wantHistory)
	}

	if sections.ToolsJSON != len(toolsJSON) {
		t.Fatalf("tools_json = %d, want %d", sections.ToolsJSON, len(toolsJSON))
	}

	sum := sections.StaticPreamble + sections.System + sections.TaskPacket +
		sections.History + sections.ToolOutputHistory + sections.ToolsJSON
	if sections.Total < sum {
		t.Fatalf("total %d must be >= sum of parts %d", sections.Total, sum)
	}
	// With total=0 passed in, the computed total is exactly the section sum.
	if sections.Total != sum {
		t.Fatalf("derived total = %d, want section sum %d", sections.Total, sum)
	}
}

func TestComputePromptSectionBytesExactTotalWins(t *testing.T) {
	messages := fixtureTelemetryMessages()
	toolsJSON := marshalToolsForTelemetry(fixtureTelemetryTools())

	sections := computePromptSectionBytes(messages, toolsJSON, 10, 0)
	sum := sections.StaticPreamble + sections.System + sections.TaskPacket +
		sections.History + sections.ToolOutputHistory + sections.ToolsJSON

	exact := sum + 250
	withExact := computePromptSectionBytes(messages, toolsJSON, 10, exact)
	if withExact.Total != exact {
		t.Fatalf("exact total = %d, want %d", withExact.Total, exact)
	}
	if withExact.Total < sum {
		t.Fatalf("exact total %d must be >= sum %d", withExact.Total, sum)
	}
}

func TestPromptSectionJSONTags(t *testing.T) {
	raw, err := json.Marshal(PromptSectionBytes{
		StaticPreamble:    1,
		System:            2,
		TaskPacket:        3,
		History:           4,
		ToolOutputHistory: 5,
		ToolsJSON:         6,
		Total:             7,
	})
	if err != nil {
		t.Fatalf("marshal PromptSectionBytes: %v", err)
	}
	got := string(raw)
	for _, key := range []string{
		`"static_preamble":1`,
		`"system":2`,
		`"task_packet":3`,
		`"history":4`,
		`"tool_output_history":5`,
		`"tools_json":6`,
		`"total":7`,
	} {
		if !contains(got, key) {
			t.Fatalf("marshaled JSON %q missing %q", got, key)
		}
	}
}

func TestBuildCodexExecPromptSections(t *testing.T) {
	messages := fixtureTelemetryMessages()
	tools := fixtureTelemetryTools()

	prompt, sections, err := buildCodexExecPromptWithSections(messages, tools)
	if err != nil {
		t.Fatalf("buildCodexExecPromptWithSections: %v", err)
	}
	if sections.StaticPreamble == 0 {
		t.Fatal("codex static_preamble must be nonzero")
	}
	if sections.StaticPreamble != len(codexStaticPreamble()) {
		t.Fatalf("codex static_preamble = %d, want %d", sections.StaticPreamble, len(codexStaticPreamble()))
	}
	if sections.History == 0 {
		t.Fatal("codex history must be nonzero for fixture transcript")
	}
	if sections.ToolsJSON == 0 {
		t.Fatal("codex tools_json must be nonzero for fixture tools")
	}
	if sections.Total != len(prompt) {
		t.Fatalf("codex total = %d, want len(prompt) = %d", sections.Total, len(prompt))
	}
	sum := sections.StaticPreamble + sections.System + sections.TaskPacket +
		sections.History + sections.ToolOutputHistory + sections.ToolsJSON
	if sections.Total < sum {
		t.Fatalf("codex total %d must be >= section sum %d", sections.Total, sum)
	}
}

func TestBuildQwenExecPromptSections(t *testing.T) {
	messages := fixtureTelemetryMessages()
	tools := fixtureTelemetryTools()

	prompt, sections, err := buildQwenExecPromptWithSections(messages, tools)
	if err != nil {
		t.Fatalf("buildQwenExecPromptWithSections: %v", err)
	}
	if sections.StaticPreamble == 0 {
		t.Fatal("qwen static_preamble must be nonzero")
	}
	if sections.StaticPreamble != len(qwenStaticPreamble()) {
		t.Fatalf("qwen static_preamble = %d, want %d", sections.StaticPreamble, len(qwenStaticPreamble()))
	}
	if sections.History == 0 {
		t.Fatal("qwen history must be nonzero for fixture transcript")
	}
	if sections.ToolsJSON == 0 {
		t.Fatal("qwen tools_json must be nonzero for fixture tools")
	}
	if sections.Total != len(prompt) {
		t.Fatalf("qwen total = %d, want len(prompt) = %d", sections.Total, len(prompt))
	}
}

// promptSectionCapture is a tool-loop observer that opts into the optional
// promptSectionObserver hook to capture per-iteration section rows.
type promptSectionCapture struct {
	rows []PromptSectionBytes
}

func (c *promptSectionCapture) OnLLMResponse(_ int, _ *LLMResponse) {}

func (c *promptSectionCapture) OnToolResult(_ int, _ ToolCall, _ ToolResult) {}

func (c *promptSectionCapture) OnPromptSectionBytes(_ int, sections PromptSectionBytes) {
	c.rows = append(c.rows, sections)
}

// TestToolLoopEmitsPromptSectionRows is the fake-LLM smoke: an OpenAI-shaped
// stub backend (no static preamble) driven through the shared tool loop
// produces nonzero section rows via the optional observer hook.
func TestToolLoopEmitsPromptSectionRows(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&registryTestTool{
		name:        "read_file",
		description: "reads a file",
		output:      "file body",
	})

	llm := &sequenceLLM{responses: []*LLMResponse{
		{
			ToolCalls: []ToolCall{{
				ID:   "call-1",
				Type: "function",
				Function: FunctionCall{
					Name:      "read_file",
					Arguments: `{"path":"README.md"}`,
				},
			}},
		},
		{Content: "done"},
	}}
	capture := &promptSectionCapture{}

	messages := []Message{
		{Role: "system", Content: "SYSTEM-INSTRUCTIONS-FOR-SMOKE"},
		{Role: "user", Content: "TASK-AND-PACKET-FOR-SMOKE"},
	}

	run, err := RunToolLoopDetailed(context.Background(), llm, registry, messages, nil, capture)
	if err != nil {
		t.Fatalf("RunToolLoopDetailed: %v", err)
	}
	if run == nil {
		t.Fatal("expected non-nil run")
	}
	if len(capture.rows) == 0 {
		t.Fatal("expected at least one prompt section row from the tool loop")
	}
	// A second iteration (after the tool call) must see tool_output_history grow.
	if len(capture.rows) < 2 {
		t.Fatalf("expected at least two prompt section rows, got %d", len(capture.rows))
	}
	if capture.rows[1].ToolOutputHistory == 0 {
		t.Fatalf("expected nonzero tool_output_history on second iteration, got row %+v", capture.rows[1])
	}
	first := capture.rows[0]
	if first.System == 0 {
		t.Fatalf("expected nonzero system bytes, got row %+v", first)
	}
	if first.TaskPacket == 0 {
		t.Fatalf("expected nonzero task_packet bytes, got row %+v", first)
	}
	// ScriptedFakeLLM is an OpenAI-shaped backend: static_preamble is 0.
	if first.StaticPreamble != 0 {
		t.Fatalf("expected zero static_preamble for fake/openai-shaped backend, got %d", first.StaticPreamble)
	}
	if first.Total == 0 {
		t.Fatalf("expected nonzero total, got row %+v", first)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
