package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

const (
	compactionMinMessages = 6
	maxMessageReprLen     = 5000
	summaryMaxTokens      = 2048
)

// CompactMessages summarizes older messages to reduce conversation size.
func CompactMessages(ctx context.Context, messages []Message, systemPrompt string, llmClient *llm.Client) ([]Message, error) {
	if len(messages) == 0 {
		return messages, nil
	}
	if len(messages) < compactionMinMessages {
		log.Printf("[agent-compact] skipping compaction: only %d messages (minimum %d)", len(messages), compactionMinMessages)
		return messages, nil
	}

	// Determine split point: keep last N messages
	keepCount := len(messages) / 2
	if keepCount > 10 {
		keepCount = 10
	}
	splitAt := len(messages) - keepCount
	toSummarize := messages[:splitAt]
	toKeep := messages[splitAt:]

	// Format messages for summarization
	formatted := formatMessagesForSummary(toSummarize)

	prompt := fmt.Sprintf(`Summarize the following conversation between a user and an assistant. Focus on:
1. What task was being worked on
2. Key decisions made
3. Files modified or created
4. Current state of progress
5. Any errors encountered and how they were resolved

Be concise but preserve important context needed to continue the work.

Conversation:
%s`, formatted)

	// Call LLM for summarization
	resp, err := llmClient.Send(ctx, llm.SendRequest{
		System:   "You are a conversation summarizer.",
		Messages: []Message{NewUserMessage(prompt)},
	})

	if err != nil || resp == nil || extractText(resp.Content) == "" {
		if err != nil {
			log.Printf("[agent-compact] LLM summarization failed: %v, using truncation fallback", err)
		} else {
			log.Printf("[agent-compact] LLM returned empty summary, using truncation fallback")
		}
		// Fallback: truncation
		truncMsg := NewUserMessage(fmt.Sprintf("[Previous conversation history was truncated due to length. %d messages were removed.]", splitAt))
		result := make([]Message, 0, 1+len(toKeep))
		result = append(result, truncMsg)
		result = append(result, toKeep...)
		log.Printf("[agent-compact] truncated %d messages to %d (fallback)", len(messages), len(result))
		return result, nil
	}

	summary := extractText(resp.Content)
	summaryMsg := NewUserMessage(fmt.Sprintf("[Conversation summary: %s]", summary))
	result := make([]Message, 0, 1+len(toKeep))
	result = append(result, summaryMsg)
	result = append(result, toKeep...)

	log.Printf("[agent-compact] compacted %d messages to %d", len(messages), len(result))
	return result, nil
}

// EstimateTokens provides a rough token count: ~1 token per 4 characters.
func EstimateTokens(messages []Message) int {
	total := 0
	for _, m := range messages {
		for _, b := range m.Content {
			switch b.Type {
			case "text", "thinking":
				total += len(b.Text)
			case "tool_use":
				total += len(b.Name) + len(b.Input)
			case "tool_result":
				total += len(b.Content)
			}
		}
	}
	return total / 4
}

func formatMessagesForSummary(messages []Message) string {
	var sb strings.Builder
	for _, m := range messages {
		sb.WriteString(fmt.Sprintf("[%s]: ", m.Role))
		for _, b := range m.Content {
			repr := formatBlock(b)
			if len(repr) > maxMessageReprLen {
				repr = repr[:maxMessageReprLen] + "..."
			}
			sb.WriteString(repr)
			sb.WriteString(" ")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func formatBlock(b ContentBlock) string {
	switch b.Type {
	case "text", "thinking":
		return b.Text
	case "tool_use":
		inputStr := string(b.Input)
		if len(inputStr) > 200 {
			inputStr = inputStr[:200] + "..."
		}
		return fmt.Sprintf("[tool_use: %s(%s)]", b.Name, inputStr)
	case "tool_result":
		content := b.Content
		if len(content) > 200 {
			content = content[:200] + "..."
		}
		return fmt.Sprintf("[tool_result: %s]", content)
	default:
		return ""
	}
}

func extractText(blocks []ContentBlock) string {
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			return b.Text
		}
	}
	return ""
}

// FormatMessagesForSummary is exported for testing.
func FormatMessagesForSummary(messages []Message) string {
	return formatMessagesForSummary(messages)
}

// marshalInput is a helper to create json.RawMessage from a map.
func marshalInput(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
