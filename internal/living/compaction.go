package living

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

// CompactLLM abstracts the LLM calls for compaction (testable).
type CompactLLM interface {
	Extract(ctx context.Context, formattedConversation string) ([]ExtractionEntry, error)
	Compress(ctx context.Context, formattedConversation string) (string, error)
}

// ExtractionEntry represents a single piece of knowledge extracted during compaction.
type ExtractionEntry struct {
	Type    string `json:"type"`
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

// NewBrainCompactFunc creates a CompactFunc that extracts knowledge into memory
// before compressing the conversation.
func NewBrainCompactFunc(compactLLM CompactLLM, memoryStore MemoryBackend) func(ctx context.Context, messages []llm.Message, systemPrompt string, llmClient *llm.Client) ([]llm.Message, error) {
	return func(ctx context.Context, messages []llm.Message, systemPrompt string, llmClient *llm.Client) ([]llm.Message, error) {
		// Format messages for LLM
		formatted := FormatMessages(messages)

		// Phase 1: Extract (if memory store available)
		if memoryStore != nil {
			entries, err := compactLLM.Extract(ctx, formatted)
			if err != nil {
				log.Printf("[compaction] extraction failed, skipping: %v", err)
			} else {
				for _, e := range entries {
					entry, ok := promoteMemoryEntry(memory.MemoryEntry{
						Type:    e.Type,
						Source:  "compaction",
						Topic:   e.Topic,
						Content: e.Content,
					}, "compaction")
					if !ok {
						continue
					}
					if _, saveErr := memoryStore.Save(ctx, entry); saveErr != nil {
						log.Printf("[compaction] failed to persist extracted memory: %v", saveErr)
					}
				}
			}
		}

		// Phase 2: Compress
		summary, err := compactLLM.Compress(ctx, formatted)
		if err != nil {
			log.Printf("[compaction] compression failed, returning original messages: %v", err)
			return messages, fmt.Errorf("compression failed: %w", err)
		}

		compressed := []llm.Message{
			llm.NewUserMessage("[COMPRESSED CONTEXT]\n" + summary),
		}
		return compressed, nil
	}
}

// FormatMessages formats conversation messages into a human-readable string
// suitable for sending to an LLM for extraction or compression.
func FormatMessages(messages []llm.Message) string {
	var b strings.Builder
	for _, m := range messages {
		b.WriteString(fmt.Sprintf("[%s]: ", m.Role))
		for _, block := range m.Content {
			switch block.Type {
			case "text":
				b.WriteString(block.Text)
			case "tool_use":
				fmt.Fprintf(&b, "[used tool: %s]", block.Name)
			case "tool_result":
				b.WriteString("[tool result]")
			}
			b.WriteString(" ")
		}
		b.WriteString("\n")
	}
	return b.String()
}
