package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func compactionTestConfig() ToolLoopCompactionConfig {
	config := DefaultToolLoopCompactionConfig()
	config.Enabled = true
	return config
}

// buildCompactionFixtureTranscript simulates k tool-loop iterations with one
// large tool output per iteration, mirroring runToolLoopDetailed's append
// pattern.
func buildCompactionFixtureTranscript(iterations int, outputBytes int) []Message {
	messages := []Message{
		{Role: "system", Content: "system prompt body"},
		{Role: "user", Content: "task and work packet body"},
	}
	for i := 0; i < iterations; i++ {
		callID := fmt.Sprintf("call-%d", i+1)
		messages = append(messages, Message{
			Role:    "assistant",
			Content: fmt.Sprintf("thinking %d", i+1),
			ToolCalls: []ToolCall{{
				ID:   callID,
				Type: "function",
				Function: FunctionCall{
					Name:      "read_file",
					Arguments: fmt.Sprintf(`{"path":"file-%d.txt"}`, i+1),
				},
			}},
		})
		messages = append(messages, Message{
			Role:       "tool",
			ToolCallID: callID,
			Content: fmt.Sprintf("output %d header line\n%s\noutput %d trailer line",
				i+1, strings.Repeat("x", outputBytes), i+1),
		})
	}
	return messages
}

func messagesByteSize(t *testing.T, messages []Message) int {
	t.Helper()
	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	return len(raw)
}

func cloneMessagesJSON(t *testing.T, messages []Message) string {
	t.Helper()
	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	return string(raw)
}

func TestCompactionDisabledReturnsCanonicalView(t *testing.T) {
	transcript := buildCompactionFixtureTranscript(6, 4000)
	config := DefaultToolLoopCompactionConfig()
	if config.Enabled {
		t.Fatalf("compaction must default to off")
	}
	view := CompactToolLoopPromptView(transcript, config)
	if messagesByteSize(t, view) != messagesByteSize(t, transcript) {
		t.Fatalf("disabled compaction altered the prompt view")
	}
}

func TestCompactionPreservesCanonicalTranscript(t *testing.T) {
	transcript := buildCompactionFixtureTranscript(10, 4000)
	before := cloneMessagesJSON(t, transcript)
	view := CompactToolLoopPromptView(transcript, compactionTestConfig())
	if after := cloneMessagesJSON(t, transcript); after != before {
		t.Fatalf("compaction mutated the canonical transcript")
	}
	if messagesByteSize(t, view) >= messagesByteSize(t, transcript) {
		t.Fatalf("compacted view is not smaller than canonical transcript")
	}
}

func TestCompactionShrinksPromptAfterRawTail(t *testing.T) {
	config := compactionTestConfig()
	small := buildCompactionFixtureTranscript(config.RawTailExchanges, 4000)
	if got := CompactToolLoopPromptView(small, config); messagesByteSize(t, got) != messagesByteSize(t, small) {
		t.Fatalf("transcript within raw tail window must stay verbatim")
	}

	// Past the raw tail the view must grow far slower than the canonical
	// transcript: canonical grows ~4KB per iteration; the view should grow by
	// at most the excerpt+summary overhead.
	sizeAt := func(iterations int) int {
		return messagesByteSize(t, CompactToolLoopPromptView(
			buildCompactionFixtureTranscript(iterations, 4000), config))
	}
	canonicalAt := func(iterations int) int {
		return messagesByteSize(t, buildCompactionFixtureTranscript(iterations, 4000))
	}
	viewGrowth := sizeAt(20) - sizeAt(10)
	canonicalGrowth := canonicalAt(20) - canonicalAt(10)
	if viewGrowth*5 > canonicalGrowth {
		t.Fatalf("view growth %d should be <20%% of canonical growth %d", viewGrowth, canonicalGrowth)
	}
	if sizeAt(20)*2 > canonicalAt(20) {
		t.Fatalf("compacted view %d should be under half of canonical %d at 20 iterations",
			sizeAt(20), canonicalAt(20))
	}
}

func TestCompactionKeepsPrefixAndRawTailVerbatim(t *testing.T) {
	config := compactionTestConfig()
	transcript := buildCompactionFixtureTranscript(12, 3000)
	view := CompactToolLoopPromptView(transcript, config)

	if view[0].Role != "system" || view[0].Content != transcript[0].Content {
		t.Fatalf("system prefix changed")
	}
	if view[1].Role != "user" || view[1].Content != transcript[1].Content {
		t.Fatalf("task/packet message changed")
	}
	// Raw tail: the last 2*RawTailExchanges messages must be byte-identical.
	tail := 2 * config.RawTailExchanges
	for offset := 1; offset <= tail; offset++ {
		got := view[len(view)-offset]
		want := transcript[len(transcript)-offset]
		if got.Role != want.Role || got.Content != want.Content {
			t.Fatalf("raw tail message %d changed", offset)
		}
	}
}

func TestCompactionSummaryAndExcerptCarryRereadRefs(t *testing.T) {
	config := compactionTestConfig()
	transcript := buildCompactionFixtureTranscript(12, 3000)
	view := CompactToolLoopPromptView(transcript, config)

	var summary string
	excerpts := 0
	for _, msg := range view {
		if msg.Role == "user" && strings.HasPrefix(msg.Content, "[compacted-history]") {
			summary = msg.Content
		}
		if msg.Role == "tool" && strings.Contains(msg.Content, "compacted for prompt view") {
			excerpts++
			if !strings.Contains(msg.Content, "ref="+msg.ToolCallID) {
				t.Fatalf("excerpt missing stable reread ref: %s", truncate(msg.Content, 200))
			}
			if !strings.Contains(msg.Content, "sha256=") {
				t.Fatalf("excerpt missing digest")
			}
			if !strings.Contains(msg.Content, "re-read via the same tool") {
				t.Fatalf("excerpt missing reread guidance")
			}
		}
	}
	if summary == "" {
		t.Fatalf("compacted view missing running summary message")
	}
	if !strings.Contains(summary, "re-read") {
		t.Fatalf("summary missing reread guidance: %s", truncate(summary, 200))
	}
	if !strings.Contains(summary, "tool read_file args_digest=") {
		t.Fatalf("summary missing mechanical tool lines: %s", truncate(summary, 400))
	}
	if !strings.Contains(summary, "ref=call-1") {
		t.Fatalf("summary missing stable call refs: %s", truncate(summary, 400))
	}
	if excerpts != config.ExcerptExchanges {
		t.Fatalf("expected %d excerpted tool outputs, got %d", config.ExcerptExchanges, excerpts)
	}
}

func TestCompactionPreservesOpenAIAdjacency(t *testing.T) {
	config := compactionTestConfig()
	for _, iterations := range []int{1, 3, 4, 7, 12, 25} {
		transcript := buildCompactionFixtureTranscript(iterations, 2000)
		if err := validateOpenAIToolAdjacency(transcript); err != nil {
			t.Fatalf("fixture transcript invalid at %d iterations: %v", iterations, err)
		}
		view := CompactToolLoopPromptView(transcript, config)
		if err := validateOpenAIToolAdjacency(view); err != nil {
			t.Fatalf("compacted view broke adjacency at %d iterations: %v", iterations, err)
		}
	}
}

func TestCompactionHandlesMultiToolCallsAndUserMessages(t *testing.T) {
	config := compactionTestConfig()
	messages := []Message{
		{Role: "system", Content: "system"},
		{Role: "user", Content: "task"},
	}
	// One exchange with two parallel tool calls.
	messages = append(messages, Message{
		Role: "assistant",
		ToolCalls: []ToolCall{
			{ID: "a-1", Type: "function", Function: FunctionCall{Name: "shell", Arguments: `{"cmd":"ls"}`}},
			{ID: "a-2", Type: "function", Function: FunctionCall{Name: "read_file", Arguments: `{"path":"x"}`}},
		},
	})
	messages = append(messages,
		Message{Role: "tool", ToolCallID: "a-1", Content: strings.Repeat("a", 3000)},
		Message{Role: "tool", ToolCallID: "a-2", Content: strings.Repeat("b", 3000)},
		Message{Role: "user", Content: "operator interjection"},
	)
	for i := 0; i < 6; i++ {
		callID := fmt.Sprintf("b-%d", i)
		messages = append(messages,
			Message{Role: "assistant", ToolCalls: []ToolCall{{ID: callID, Type: "function",
				Function: FunctionCall{Name: "shell", Arguments: "{}"}}}},
			Message{Role: "tool", ToolCallID: callID, Content: strings.Repeat("c", 3000)},
		)
	}

	view := CompactToolLoopPromptView(messages, config)
	if err := validateOpenAIToolAdjacency(view); err != nil {
		t.Fatalf("adjacency broken: %v", err)
	}
	var summary string
	for _, msg := range view {
		if msg.Role == "user" && strings.HasPrefix(msg.Content, "[compacted-history]") {
			summary = msg.Content
		}
	}
	if summary == "" {
		t.Fatalf("expected summary for compacted multi-call exchange")
	}
	if !strings.Contains(summary, "ref=a-1") || !strings.Contains(summary, "ref=a-2") {
		t.Fatalf("summary must cover both parallel tool calls: %s", truncate(summary, 400))
	}
	if !strings.Contains(summary, "operator interjection") {
		t.Fatalf("summary must record standalone user messages: %s", truncate(summary, 400))
	}
}

func TestSetDefaultToolLoopCompaction(t *testing.T) {
	original := CurrentToolLoopCompaction()
	defer SetDefaultToolLoopCompaction(original)

	if original.Enabled {
		t.Fatalf("process default must start disabled")
	}
	SetDefaultToolLoopCompaction(ToolLoopCompactionConfig{Enabled: true})
	updated := CurrentToolLoopCompaction()
	if !updated.Enabled {
		t.Fatalf("enable did not stick")
	}
	if updated.RawTailExchanges != defaultRawTailExchanges {
		t.Fatalf("normalization must fill zero fields, got %+v", updated)
	}
}

// compactionRecordingLLM is an inline fake ChatLLM that asks for one big tool
// call per iteration, recording the byte size and protocol shape of every
// prompt view it receives.
type compactionRecordingLLM struct {
	iterations    int
	calls         int
	promptBytes   []int
	adjacencyErrs []error
}

func (l *compactionRecordingLLM) Chat(_ context.Context, messages []Message, _ []ToolDef) (*LLMResponse, error) {
	raw, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	l.promptBytes = append(l.promptBytes, len(raw))
	if err := validateOpenAIToolAdjacency(messages); err != nil {
		l.adjacencyErrs = append(l.adjacencyErrs, err)
	}
	l.calls++
	if l.calls <= l.iterations {
		return &LLMResponse{
			Content: fmt.Sprintf("iteration %d", l.calls),
			ToolCalls: []ToolCall{{
				ID:   fmt.Sprintf("loop-call-%d", l.calls),
				Type: "function",
				Function: FunctionCall{
					Name:      "big_tool",
					Arguments: fmt.Sprintf(`{"step":%d}`, l.calls),
				},
			}},
		}, nil
	}
	return &LLMResponse{Content: "final answer"}, nil
}

// TestToolLoopCompactionEndToEnd drives the real tool loop with compaction on:
// the prompt views sent to the backend must stay near-flat while the canonical
// ToolLoopRun.Messages keeps every full tool output (TE-10 acceptance).
func TestToolLoopCompactionEndToEnd(t *testing.T) {
	original := CurrentToolLoopCompaction()
	defer SetDefaultToolLoopCompaction(original)

	bigOutput := func(step int) string {
		return fmt.Sprintf("full output %d\n%s\nend %d", step, strings.Repeat("z", 4000), step)
	}
	executor := func(_ context.Context, _ *ToolRegistry, call ToolCall) ToolResult {
		var args struct {
			Step int `json:"step"`
		}
		_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
		return ToolResult{Output: bigOutput(args.Step)}
	}
	seed := []Message{
		{Role: "system", Content: "system prompt"},
		{Role: "user", Content: "task packet"},
	}
	const iterations = 12

	runWith := func(enabled bool) (*ToolLoopRun, *compactionRecordingLLM) {
		config := DefaultToolLoopCompactionConfig()
		config.Enabled = enabled
		SetDefaultToolLoopCompaction(config)
		llm := &compactionRecordingLLM{iterations: iterations}
		run, err := RunToolLoopDetailedWithLimit(
			context.Background(), llm, NewToolRegistry(), seed, executor, nil, iterations+1)
		if err != nil {
			t.Fatalf("tool loop failed (enabled=%v): %v", enabled, err)
		}
		return run, llm
	}

	runOff, llmOff := runWith(false)
	runOn, llmOn := runWith(true)

	if len(llmOn.adjacencyErrs) > 0 {
		t.Fatalf("compacted prompt views broke OpenAI adjacency: %v", llmOn.adjacencyErrs[0])
	}
	if runOn.Content != "final answer" {
		t.Fatalf("unexpected final content: %q", runOn.Content)
	}

	// Canonical transcript must keep every full tool output even with
	// compaction on, and match the compaction-off transcript shape.
	if len(runOn.Messages) != len(runOff.Messages) {
		t.Fatalf("canonical transcript length changed: on=%d off=%d",
			len(runOn.Messages), len(runOff.Messages))
	}
	for step := 1; step <= iterations; step++ {
		want := bigOutput(step)
		found := false
		for _, msg := range runOn.Messages {
			if msg.Role == "tool" && msg.Content == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("canonical transcript lost full tool output for step %d", step)
		}
	}

	// The last prompt view with compaction on must be a small fraction of the
	// uncompacted one (12 iterations x ~4KB outputs).
	lastOff := llmOff.promptBytes[len(llmOff.promptBytes)-1]
	lastOn := llmOn.promptBytes[len(llmOn.promptBytes)-1]
	if lastOn*2 > lastOff {
		t.Fatalf("compacted final prompt view %d should be under half of uncompacted %d", lastOn, lastOff)
	}
	// Growth after the raw tail fills must be near-flat: compare growth across
	// the last 6 iterations.
	n := len(llmOn.promptBytes)
	onGrowth := llmOn.promptBytes[n-1] - llmOn.promptBytes[n-7]
	offGrowth := llmOff.promptBytes[n-1] - llmOff.promptBytes[n-7]
	if onGrowth*5 > offGrowth {
		t.Fatalf("compacted view growth %d should be <20%% of uncompacted growth %d", onGrowth, offGrowth)
	}
}

// TestCodexQwenPromptShrinkUnderCompaction is the TE-11 backend check: the
// same compacted view flows through both prompt builders and shrinks the
// actual stdin prompt, with exact section totals.
func TestCodexQwenPromptShrinkUnderCompaction(t *testing.T) {
	config := compactionTestConfig()
	full := buildCompactionFixtureTranscript(12, 3000)
	view := CompactToolLoopPromptView(full, config)
	tools := []ToolDef{{
		Type: "function",
		Function: ToolFunctionDef{
			Name:        "read_file",
			Description: "read a file",
			Parameters:  map[string]any{"type": "object"},
		},
	}}

	type builder func([]Message, []ToolDef) (string, PromptSectionBytes, error)
	builders := map[string]builder{
		"codex": buildCodexExecPromptWithSections,
		"qwen":  buildQwenExecPromptWithSections,
	}
	for name, build := range builders {
		fullPrompt, fullSections, err := build(full, tools)
		if err != nil {
			t.Fatalf("%s full build failed: %v", name, err)
		}
		viewPrompt, viewSections, err := build(view, tools)
		if err != nil {
			t.Fatalf("%s view build failed: %v", name, err)
		}
		if fullSections.Total != len(fullPrompt) || viewSections.Total != len(viewPrompt) {
			t.Fatalf("%s section totals are not exact: full=%d/%d view=%d/%d",
				name, fullSections.Total, len(fullPrompt), viewSections.Total, len(viewPrompt))
		}
		if len(viewPrompt)*2 > len(fullPrompt) {
			t.Fatalf("%s compacted prompt %d should be under half of full prompt %d",
				name, len(viewPrompt), len(fullPrompt))
		}
		if viewSections.ToolOutputHistory >= fullSections.ToolOutputHistory {
			t.Fatalf("%s tool_output_history did not shrink: view=%d full=%d",
				name, viewSections.ToolOutputHistory, fullSections.ToolOutputHistory)
		}
	}
}

func TestValidateOpenAIToolAdjacencyRejectsBrokenShapes(t *testing.T) {
	broken := []Message{
		{Role: "system", Content: "s"},
		{Role: "tool", ToolCallID: "x", Content: "orphan"},
	}
	if err := validateOpenAIToolAdjacency(broken); err == nil {
		t.Fatalf("orphan tool message must be rejected")
	}
	missing := []Message{
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "m-1", Type: "function",
			Function: FunctionCall{Name: "shell", Arguments: "{}"}}}},
		{Role: "assistant", Content: "skipped result"},
	}
	if err := validateOpenAIToolAdjacency(missing); err == nil {
		t.Fatalf("missing tool result must be rejected")
	}
}
