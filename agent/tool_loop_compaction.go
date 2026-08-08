package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

// Tool-loop prompt-view compaction.
//
// The tool loop re-sends the entire transcript to the backend on every
// iteration, which makes per-cycle token cost grow O(k^2) with iteration
// count. Compaction builds a provider-safe PROMPT VIEW of the canonical
// transcript for the LLM call only: ToolLoopRun.Messages, chat history, and
// session history always keep the full canonical messages.
//
// View shape (three zones over assistant-led exchanges):
//   - prefix: leading system messages + first user (task/packet) message,
//     verbatim, so the stable prompt prefix stays byte-identical across
//     iterations (E3 prefix caching).
//   - zone A (oldest exchanges): collapsed entirely into one mechanical
//     running-summary message (tool name + args digest + outcome line).
//     Whole exchanges are removed, so OpenAI assistant/tool adjacency is
//     preserved by construction.
//   - zone B (middle exchanges): messages kept, but tool outputs replaced by
//     head+tail excerpt + sha256 digest + stable reread ref (tool_call_id).
//   - zone C (raw tail, last RawTailExchanges exchanges): verbatim, so the
//     model always sees the freshest tool outputs in full.

type ToolLoopCompactionConfig struct {
	Enabled bool
	// RawTailExchanges is the number of most recent assistant-led exchanges
	// kept verbatim (zone C).
	RawTailExchanges int
	// ExcerptExchanges is the number of exchanges before the raw tail that
	// keep message structure with excerpted tool outputs (zone B).
	ExcerptExchanges int
	// ToolOutputHeadBytes/ToolOutputTailBytes bound the excerpt kept from
	// each zone-B tool output.
	ToolOutputHeadBytes int
	ToolOutputTailBytes int
	// SummaryOutcomeBytes bounds the per-call outcome line in the zone-A
	// running summary.
	SummaryOutcomeBytes int
}

const (
	defaultRawTailExchanges    = 3
	defaultExcerptExchanges    = 3
	defaultToolOutputHeadBytes = 600
	defaultToolOutputTailBytes = 400
	defaultSummaryOutcomeBytes = 160

	compactionSummaryHeader = "[compacted-history] Older tool exchanges were compacted out of this prompt view." +
		" The canonical transcript is preserved outside the prompt. Summary lines are mechanical" +
		" (tool name, args digest, outcome). If you need any full output again, re-read it with the" +
		" same tool and arguments instead of guessing."
	compactionExcerptNote = "re-read via the same tool with the same arguments if the full output is needed"
)

func DefaultToolLoopCompactionConfig() ToolLoopCompactionConfig {
	return ToolLoopCompactionConfig{
		Enabled:             false,
		RawTailExchanges:    defaultRawTailExchanges,
		ExcerptExchanges:    defaultExcerptExchanges,
		ToolOutputHeadBytes: defaultToolOutputHeadBytes,
		ToolOutputTailBytes: defaultToolOutputTailBytes,
		SummaryOutcomeBytes: defaultSummaryOutcomeBytes,
	}
}

func (c ToolLoopCompactionConfig) normalized() ToolLoopCompactionConfig {
	d := DefaultToolLoopCompactionConfig()
	if c.RawTailExchanges <= 0 {
		c.RawTailExchanges = d.RawTailExchanges
	}
	if c.ExcerptExchanges < 0 {
		c.ExcerptExchanges = d.ExcerptExchanges
	}
	if c.ToolOutputHeadBytes <= 0 {
		c.ToolOutputHeadBytes = d.ToolOutputHeadBytes
	}
	if c.ToolOutputTailBytes <= 0 {
		c.ToolOutputTailBytes = d.ToolOutputTailBytes
	}
	if c.SummaryOutcomeBytes <= 0 {
		c.SummaryOutcomeBytes = d.SummaryOutcomeBytes
	}
	return c
}

// Process-wide default, set once at startup by the runtime flag wiring
// before any agent loop starts. Tool loops snapshot it per call.
var (
	toolLoopCompactionMu      sync.RWMutex
	toolLoopCompactionDefault = DefaultToolLoopCompactionConfig()
)

func SetDefaultToolLoopCompaction(config ToolLoopCompactionConfig) {
	toolLoopCompactionMu.Lock()
	defer toolLoopCompactionMu.Unlock()
	toolLoopCompactionDefault = config.normalized()
}

func CurrentToolLoopCompaction() ToolLoopCompactionConfig {
	toolLoopCompactionMu.RLock()
	defer toolLoopCompactionMu.RUnlock()
	return toolLoopCompactionDefault
}

// promptExchange is one assistant-led exchange (assistant message plus the
// tool-result messages answering its tool calls) or a standalone
// user/assistant message inside the loop history.
type promptExchange struct {
	messages []Message
}

func (e promptExchange) assistantLed() bool {
	return len(e.messages) > 0 && e.messages[0].Role == "assistant"
}

// splitPromptTranscript separates the stable prefix (leading system messages
// plus the first non-system message, normally the task/packet user message)
// from the exchange list.
func splitPromptTranscript(messages []Message) (prefix []Message, exchanges []promptExchange) {
	i := 0
	for i < len(messages) && messages[i].Role == "system" {
		i++
	}
	if i < len(messages) && messages[i].Role != "assistant" && messages[i].Role != "tool" {
		i++ // first user/task message stays in the prefix
	}
	prefix = messages[:i]

	for i < len(messages) {
		msg := messages[i]
		if msg.Role == "assistant" {
			exchange := promptExchange{messages: []Message{msg}}
			j := i + 1
			for j < len(messages) && messages[j].Role == "tool" {
				exchange.messages = append(exchange.messages, messages[j])
				j++
			}
			exchanges = append(exchanges, exchange)
			i = j
			continue
		}
		// Standalone user (or stray tool) message: its own exchange so zone
		// boundaries never split an assistant/tool pair.
		exchanges = append(exchanges, promptExchange{messages: []Message{msg}})
		i++
	}
	return prefix, exchanges
}

// CompactToolLoopPromptView builds the prompt view for one LLM call. The
// returned slice shares Message values with the canonical transcript but the
// slice itself (and any rewritten tool messages) are fresh; the canonical
// transcript is never mutated.
func CompactToolLoopPromptView(messages []Message, config ToolLoopCompactionConfig) []Message {
	if !config.Enabled {
		return messages
	}
	config = config.normalized()
	prefix, exchanges := splitPromptTranscript(messages)
	if len(exchanges) <= config.RawTailExchanges {
		return messages
	}

	rawTailStart := len(exchanges) - config.RawTailExchanges
	excerptStart := rawTailStart - config.ExcerptExchanges
	if excerptStart < 0 {
		excerptStart = 0
	}

	view := make([]Message, 0, len(messages))
	view = append(view, prefix...)

	if excerptStart > 0 {
		summary := buildCompactionSummary(exchanges[:excerptStart], config)
		view = append(view, Message{Role: "user", Content: summary})
	}
	for _, exchange := range exchanges[excerptStart:rawTailStart] {
		for _, msg := range exchange.messages {
			if msg.Role == "tool" {
				view = append(view, excerptToolMessage(msg, config))
				continue
			}
			view = append(view, msg)
		}
	}
	for _, exchange := range exchanges[rawTailStart:] {
		view = append(view, exchange.messages...)
	}
	return view
}

func buildCompactionSummary(exchanges []promptExchange, config ToolLoopCompactionConfig) string {
	var b strings.Builder
	b.WriteString(compactionSummaryHeader)
	b.WriteString("\n")
	for i, exchange := range exchanges {
		if !exchange.assistantLed() {
			for _, msg := range exchange.messages {
				fmt.Fprintf(&b, "- step %d %s: %s\n", i+1, msg.Role,
					truncate(strings.TrimSpace(msg.Content), config.SummaryOutcomeBytes))
			}
			continue
		}
		assistant := exchange.messages[0]
		toolOutputs := map[string]Message{}
		for _, msg := range exchange.messages[1:] {
			toolOutputs[msg.ToolCallID] = msg
		}
		if len(assistant.ToolCalls) == 0 {
			fmt.Fprintf(&b, "- step %d assistant: %s\n", i+1,
				truncate(strings.TrimSpace(assistant.Content), config.SummaryOutcomeBytes))
			continue
		}
		for _, call := range assistant.ToolCalls {
			outcome := "(no tool result recorded)"
			if result, ok := toolOutputs[call.ID]; ok {
				outcome = firstSummaryLine(result.Content, config.SummaryOutcomeBytes)
			}
			fmt.Fprintf(&b, "- step %d tool %s args_digest=%s ref=%s -> %s\n",
				i+1, call.Function.Name, shortDigest(call.Function.Arguments), call.ID, outcome)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func firstSummaryLine(content string, limit int) string {
	content = strings.TrimSpace(content)
	if idx := strings.IndexByte(content, '\n'); idx >= 0 {
		content = content[:idx]
	}
	if content == "" {
		return "(empty output)"
	}
	return truncate(content, limit)
}

func excerptToolMessage(msg Message, config ToolLoopCompactionConfig) Message {
	content := msg.Content
	budget := config.ToolOutputHeadBytes + config.ToolOutputTailBytes
	if len(content) <= budget {
		return msg
	}
	head := content[:config.ToolOutputHeadBytes]
	tail := content[len(content)-config.ToolOutputTailBytes:]
	compacted := fmt.Sprintf(
		"%s\n[...tool output compacted for prompt view: %d bytes total, sha256=%s, ref=%s; %s...]\n%s",
		head, len(content), shortDigest(content), msg.ToolCallID, compactionExcerptNote, tail)
	out := msg
	out.Content = compacted
	return out
}

func shortDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:12]
}

// validateOpenAIToolAdjacency checks the OpenAI chat protocol shape: every
// assistant message that declares tool calls is immediately followed by
// exactly one tool message per declared call id (in order), and every tool
// message answers the immediately preceding assistant's calls. Used by
// compaction tests and available as a guard for prompt builders.
func validateOpenAIToolAdjacency(messages []Message) error {
	i := 0
	for i < len(messages) {
		msg := messages[i]
		if msg.Role == "tool" {
			return fmt.Errorf("message %d: tool message without preceding assistant tool_calls", i)
		}
		if msg.Role != "assistant" || len(msg.ToolCalls) == 0 {
			i++
			continue
		}
		expected := map[string]bool{}
		for _, call := range msg.ToolCalls {
			expected[call.ID] = true
		}
		j := i + 1
		for j < len(messages) && messages[j].Role == "tool" {
			id := messages[j].ToolCallID
			if !expected[id] {
				return fmt.Errorf("message %d: tool result for unknown call id %q", j, id)
			}
			delete(expected, id)
			j++
		}
		if len(expected) > 0 {
			missing := make([]string, 0, len(expected))
			for id := range expected {
				missing = append(missing, id)
			}
			return fmt.Errorf("message %d: assistant tool calls missing results: %s", i, strings.Join(missing, ","))
		}
		i = j
	}
	return nil
}
