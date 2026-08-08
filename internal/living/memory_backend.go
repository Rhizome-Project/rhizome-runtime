package living

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

// MemoryBackend is the minimal memory surface used by living runtime components.
// Canonical workspace memory implements this via Rhizome RPC/store; the legacy
// per-agent MemoryStore remains a fallback backend.
type MemoryBackend interface {
	Save(ctx context.Context, entry memory.MemoryEntry) (int64, error)
	Search(ctx context.Context, query string, opts memory.SearchOpts) ([]memory.MemoryEntry, error)
	GetRecent(ctx context.Context, opts memory.RecentOpts) ([]memory.MemoryEntry, error)
}

type canonicalMemoryBackend struct {
	client      WorkspaceMemoryAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewCanonicalMemoryBackend(client WorkspaceMemoryAwareRhizomeClient, workspaceID, agentID string) MemoryBackend {
	if client == nil {
		return nil
	}
	return &canonicalMemoryBackend{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (b *canonicalMemoryBackend) Save(ctx context.Context, entry memory.MemoryEntry) (int64, error) {
	promoted, ok := promoteMemoryEntry(entry, entry.Source)
	if !ok {
		return 0, fmt.Errorf("memory: canonical save requires non-empty type and content")
	}
	memoryType, summary, tags, importance, confidence := canonicalMemoryDefaults(promoted)
	record, err := b.client.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: b.workspaceID,
		MemoryType:  memoryType,
		Title:       strings.TrimSpace(promoted.Topic),
		Body:        strings.TrimSpace(promoted.Content),
		Summary:     summary,
		AgentID:     b.agentID,
		TaskID:      strings.TrimSpace(promoted.TaskID),
		SourceKind:  strings.TrimSpace(promoted.Source),
		SourceID:    b.agentID,
		Tags:        tags,
		Importance:  importance,
		Confidence:  confidence,
	})
	if err != nil {
		return 0, err
	}
	return stableMemoryNumericID(record.MemoryID), nil
}

func (b *canonicalMemoryBackend) Search(ctx context.Context, query string, opts memory.SearchOpts) ([]memory.MemoryEntry, error) {
	items, err := b.client.SearchWorkspaceMemory(ctx, WorkspaceMemorySearchFilter{
		WorkspaceID: b.workspaceID,
		Query:       strings.TrimSpace(query),
		MemoryType:  legacyMemoryTypeToCanonical(opts.TypeFilter),
		Limit:       opts.Limit,
	})
	if err != nil {
		return nil, err
	}
	return convertCanonicalMemoryRecords(items), nil
}

func (b *canonicalMemoryBackend) GetRecent(ctx context.Context, opts memory.RecentOpts) ([]memory.MemoryEntry, error) {
	items, err := b.client.ListWorkspaceMemory(ctx, WorkspaceMemorySearchFilter{
		WorkspaceID: b.workspaceID,
		MemoryType:  legacyMemoryTypeToCanonical(opts.TypeFilter),
		TaskID:      strings.TrimSpace(opts.TaskID),
		Limit:       opts.Limit,
	})
	if err != nil {
		return nil, err
	}
	return convertCanonicalMemoryRecords(items), nil
}

func convertCanonicalMemoryRecords(items []WorkspaceMemoryRecord) []memory.MemoryEntry {
	out := make([]memory.MemoryEntry, 0, len(items))
	for _, item := range items {
		out = append(out, memory.MemoryEntry{
			ID:        stableMemoryNumericID(item.MemoryID),
			Timestamp: parseCanonicalMemoryTime(item.UpdatedAt, item.CreatedAt),
			Type:      canonicalMemoryTypeToLegacy(item.MemoryType),
			Source:    item.SourceKind,
			Topic:     firstNonEmptyNonBlank(item.Title, item.Summary),
			Content:   item.Body,
			TaskID:    item.TaskID,
			Rank:      item.Importance,
		})
	}
	return out
}

func parseCanonicalMemoryTime(values ...string) time.Time {
	for _, value := range values {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func stableMemoryNumericID(memoryID string) int64 {
	memoryID = strings.TrimSpace(memoryID)
	if memoryID == "" {
		return 0
	}
	if idx := strings.LastIndex(memoryID, "-"); idx >= 0 && idx+1 < len(memoryID) {
		if n, err := strconv.ParseInt(memoryID[idx+1:], 10, 64); err == nil {
			return n
		}
	}
	var sum int64
	for _, r := range memoryID {
		sum = (sum * 131) + int64(r)
	}
	if sum < 0 {
		return -sum
	}
	return sum
}

func legacyMemoryTypeToCanonical(raw string) string {
	switch normalizeLivingMemoryType(raw) {
	case "", "note":
		return ""
	case memory.TypeExperience:
		return "EXPERIENCE"
	case memory.TypeProcedure:
		return "PROCEDURE"
	case memory.TypeEntity:
		return "ENTITY"
	case memory.TypeError, memory.TypeIncident:
		return "INCIDENT"
	case memory.TypeReflection, memory.TypeLesson:
		return "LESSON"
	case memory.TypeDecision:
		return "DECISION"
	case memory.TypeUpdateDigest:
		return "UPDATE_DIGEST"
	case memory.TypeSummary:
		return "SUMMARY"
	default:
		return strings.ToUpper(strings.TrimSpace(normalizeLivingMemoryType(raw)))
	}
}

func canonicalMemoryTypeToLegacy(raw string) string {
	switch strings.TrimSpace(strings.ToUpper(raw)) {
	case "PROCEDURE":
		return memory.TypeProcedure
	case "ENTITY":
		return memory.TypeEntity
	case "INCIDENT":
		return memory.TypeIncident
	case "LESSON":
		return memory.TypeLesson
	case "DECISION":
		return memory.TypeDecision
	case "UPDATE_DIGEST":
		return memory.TypeUpdateDigest
	case "SUMMARY":
		return memory.TypeSummary
	case "EXPERIENCE":
		return memory.TypeExperience
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}
