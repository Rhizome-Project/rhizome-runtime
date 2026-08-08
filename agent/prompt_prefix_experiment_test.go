package main

import (
	"fmt"
	"strings"
	"testing"
)

// Lane TE-20 "Prefix Experiment" (plans/01-token-economy-tracker-2026-06-11.md).
//
// MEASUREMENT-ONLY. This NEW test file does not touch any prompt-builder source
// (codex_llm.go / qwen_llm.go / tool_loop_compaction.go). It measures the
// byte-identical common prefix length between the prompt built at tool-loop
// iteration k and the prompt built at iteration k+1 for the same session.
//
// The hypothesis under test (evidence appendix section 6): because the tool
// loop only ever APPENDS messages to the canonical transcript (assistant turn +
// tool results), and buildCodexExecPrompt marshals the messages array with a
// deterministic json.MarshalIndent, the prompt for iteration k+1 is the
// iteration-k prompt with new bytes inserted ONLY at the tail of the
// "Conversation JSON:" array (and a constant tools-JSON trailer). The static
// preamble is constant, and every earlier message marshals to the identical
// bytes on both iterations. So the common prefix should cover essentially the
// entire iteration-k message region up to the point where the array is extended.

// buildLoopFixtureMessages returns a transcript representing the state of an
// assistant-led tool loop after `iterations` completed exchanges. Earlier
// exchanges are NEVER rewritten between iterations, mirroring runToolLoopDetailed
// (it appends the assistant turn and the tool results, then re-sends the whole
// slice). The content is deterministic so the test is reproducible offline.
func buildLoopFixtureMessages(iterations int) []Message {
	msgs := []Message{
		{Role: "system", Content: "You are an outer coding agent operating under the Rhizome runtime."},
		{Role: "user", Content: "TASK: Implement the requested change and verify it. WORK PACKET: ticket TE-20, branch main, scope measurement-only."},
	}
	for k := 0; k < iterations; k++ {
		callID := fmt.Sprintf("call-%d", k+1)
		// Assistant turn requesting a tool call (appended once, never rewritten).
		msgs = append(msgs, Message{
			Role:    "assistant",
			Content: fmt.Sprintf("Iteration %d: reading a file to gather evidence.", k+1),
			ToolCalls: []ToolCall{
				{
					ID:   callID,
					Type: "function",
					Function: FunctionCall{
						Name:      "shell",
						Arguments: fmt.Sprintf(`{"command":"sed -n '1,200p' file_%d.go"}`, k+1),
					},
				},
			},
		})
		// Tool result (a chunk of deterministic ~3KB output).
		msgs = append(msgs, Message{
			Role:       "tool",
			ToolCallID: callID,
			Content:    deterministicToolOutput(k+1, 3000),
		})
	}
	return msgs
}

// deterministicToolOutput builds a deterministic ~size-byte tool output for
// iteration k. Byte content depends only on k, so iteration k's message is
// byte-identical whether the transcript has k or k+1 exchanges.
func deterministicToolOutput(k, size int) string {
	var b strings.Builder
	line := 0
	for b.Len() < size {
		fmt.Fprintf(&b, "iter=%d line=%04d data=%s\n", k, line, strings.Repeat("x", 24))
		line++
	}
	return b.String()[:size]
}

func fixtureTools() []ToolDef {
	return []ToolDef{
		{Type: "function", Function: ToolFunctionDef{
			Name:        "shell",
			Description: "Run a shell command in the workspace.",
			Parameters: map[string]any{
				"type":       "object",
				"properties": map[string]any{"command": map[string]any{"type": "string"}},
				"required":   []any{"command"},
			},
		}},
		{Type: "function", Function: ToolFunctionDef{
			Name:        "write_file",
			Description: "Write content to a file in the workspace.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":    map[string]any{"type": "string"},
					"content": map[string]any{"type": "string"},
				},
				"required": []any{"path", "content"},
			},
		}},
	}
}

// commonPrefixLen returns the number of leading bytes that are byte-identical
// between a and b.
func commonPrefixLen(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

type prefixResult struct {
	backend     string
	iterK       int
	promptK     int
	promptK1    int
	commonBytes int
	pctOfK      float64
}

func measurePrefixStability(t *testing.T, backend string, build func([]Message, []ToolDef) (string, error), iterK int) prefixResult {
	t.Helper()
	tools := fixtureTools()

	promptK, err := build(buildLoopFixtureMessages(iterK), tools)
	if err != nil {
		t.Fatalf("%s build iter %d: %v", backend, iterK, err)
	}
	promptK1, err := build(buildLoopFixtureMessages(iterK+1), tools)
	if err != nil {
		t.Fatalf("%s build iter %d: %v", backend, iterK+1, err)
	}

	cp := commonPrefixLen(promptK, promptK1)
	pct := 100 * float64(cp) / float64(len(promptK))

	t.Logf("[prefix-experiment] backend=%s iter_k=%d prompt_k_bytes=%d prompt_k1_bytes=%d common_prefix_bytes=%d common_prefix_pct_of_k=%.2f%%",
		backend, iterK, len(promptK), len(promptK1), cp, pct)
	t.Logf("[prefix-experiment] backend=%s iter_k=%d sha256_prompt_k=%s sha256_prefix_common=%s",
		backend, iterK, sha256Hex(promptK), sha256Hex(promptK[:cp]))

	return prefixResult{
		backend:     backend,
		iterK:       iterK,
		promptK:     len(promptK),
		promptK1:    len(promptK1),
		commonBytes: cp,
		pctOfK:      pct,
	}
}

// TestPrefixExperimentStability is the TE-20 measurement. It verifies and
// quantifies that the prompt prefix is byte-stable across iterations of the
// same session for both the codex and qwen exec backends.
func TestPrefixExperimentStability(t *testing.T) {
	cases := []struct {
		backend string
		build   func([]Message, []ToolDef) (string, error)
	}{
		{"codex", buildCodexExecPrompt},
		{"qwen", buildQwenExecPrompt},
	}

	for _, c := range cases {
		for _, iterK := range []int{1, 6, 12} {
			res := measurePrefixStability(t, c.backend, c.build, iterK)

			// (a) The prompt for iteration k+1 must be strictly longer (history
			// only grows) and the common prefix must be a strict prefix of it.
			if res.promptK1 <= res.promptK {
				t.Errorf("%s iter %d: expected k+1 prompt to grow (k=%d k1=%d)", c.backend, iterK, res.promptK, res.promptK1)
			}
			// (b) Scale-invariant stability invariant: the ONLY bytes of prompt(k)
			// that may differ from prompt(k+1) are in the tail where the messages
			// array is extended by the newly appended exchange. So the divergence
			// region (promptK bytes after the common prefix) must be bounded by the
			// size of a single appended assistant+tool exchange (~3.2KB on this
			// fixture: ~3KB tool output + assistant turn + JSON punctuation) plus a
			// small slack for the closing array/tools trailer. We assert the
			// divergence is < 4KB regardless of how large the prompt grows -- this
			// is the hard regression check: if an earlier message were ever
			// rewritten, the divergence would jump to thousands of bytes earlier in
			// the prompt and blow this bound.
			const maxDivergenceBytes = 4096
			divergence := res.promptK - res.commonBytes
			if divergence > maxDivergenceBytes {
				t.Errorf("%s iter %d: divergence region %d bytes exceeds one-exchange bound %d (common=%d/%d, %.2f%%); a non-tail byte changed -> prefix not stable",
					c.backend, iterK, divergence, maxDivergenceBytes, res.commonBytes, res.promptK, res.pctOfK)
			}
			// The percentage-of-prompt stability is reported for the appendix and
			// rises monotonically toward 100% as the transcript grows (the fixed
			// divergence becomes a vanishing fraction of an ever-larger prompt).
			// (c) The static preamble (a known constant) must be inside the common
			// prefix verbatim.
			var preambleLen int
			if c.backend == "codex" {
				preambleLen = len(codexStaticPreamble())
			} else {
				preambleLen = len(qwenStaticPreamble())
			}
			if res.commonBytes < preambleLen {
				t.Errorf("%s iter %d: common prefix %d bytes is shorter than static preamble %d bytes",
					c.backend, iterK, res.commonBytes, preambleLen)
			}
		}
	}
}

// TestPrefixExperimentIdenticalCallSameSession confirms (a) from the lane goal:
// building the prompt twice from the SAME message slice yields a byte-identical
// prompt (same sha256). This isolates determinism of buildCodexExecPrompt /
// buildQwenExecPrompt from the loop-growth question.
func TestPrefixExperimentIdenticalCallSameSession(t *testing.T) {
	tools := fixtureTools()
	for _, c := range []struct {
		backend string
		build   func([]Message, []ToolDef) (string, error)
	}{
		{"codex", buildCodexExecPrompt},
		{"qwen", buildQwenExecPrompt},
	} {
		msgs := buildLoopFixtureMessages(8)
		p1, err := c.build(msgs, tools)
		if err != nil {
			t.Fatalf("%s build1: %v", c.backend, err)
		}
		p2, err := c.build(msgs, tools)
		if err != nil {
			t.Fatalf("%s build2: %v", c.backend, err)
		}
		h1, h2 := sha256Hex(p1), sha256Hex(p2)
		t.Logf("[prefix-experiment] backend=%s same_session_repeat_call sha256_1=%s sha256_2=%s identical=%v",
			c.backend, h1, h2, h1 == h2)
		if h1 != h2 {
			t.Errorf("%s: repeated build of identical messages produced different prompts", c.backend)
		}
	}
}
