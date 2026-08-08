package living

import (
	"strings"
	"unicode"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

func normalizeLivingMemoryType(raw string) string {
	switch normalized := strings.TrimSpace(strings.ToLower(raw)); normalized {
	case "", "note", "notes":
		return ""
	case memory.TypeExperience:
		return memory.TypeExperience
	case memory.TypeProcedure, "runbook", "playbook", "howto", "how-to", "instruction", "workflow":
		return memory.TypeProcedure
	case memory.TypeEntity, "component", "service", "system":
		return memory.TypeEntity
	case memory.TypeError, memory.TypeIncident, "failure", "bug", "regression":
		return memory.TypeIncident
	case memory.TypeReflection, memory.TypeLesson, "learning", "learnings", "takeaway":
		return memory.TypeLesson
	case memory.TypeDecision, "policy":
		return memory.TypeDecision
	case memory.TypeUpdateDigest, "update", "digest", "status", "status_update":
		return memory.TypeUpdateDigest
	case memory.TypeSummary, "context_summary":
		return memory.TypeSummary
	default:
		return normalized
	}
}

func promoteMemoryEntry(entry memory.MemoryEntry, defaultSource string) (memory.MemoryEntry, bool) {
	promoted := entry
	promoted.Type = normalizeLivingMemoryType(entry.Type)
	promoted.Source = strings.TrimSpace(firstNonEmptyNonBlank(entry.Source, defaultSource))
	promoted.Topic = strings.TrimSpace(entry.Topic)
	promoted.Content = strings.TrimSpace(entry.Content)
	promoted.TaskID = strings.TrimSpace(entry.TaskID)
	if promoted.Type == "" || promoted.Content == "" {
		return memory.MemoryEntry{}, false
	}
	if promoted.Topic == "" {
		promoted.Topic = defaultMemoryTopic(promoted.Type, promoted.Content)
	}
	return promoted, true
}

func memorySearchAliases(raw string) []string {
	switch normalized := normalizeLivingMemoryType(raw); normalized {
	case "":
		return nil
	case memory.TypeLesson:
		return []string{memory.TypeLesson, memory.TypeReflection}
	case memory.TypeIncident:
		return []string{memory.TypeIncident, memory.TypeError}
	case memory.TypeUpdateDigest:
		return []string{memory.TypeUpdateDigest, memory.TypeSummary}
	default:
		return []string{normalized}
	}
}

func canonicalMemoryDefaults(entry memory.MemoryEntry) (memoryType, summary string, tags []string, importance, confidence float64) {
	normalizedType := normalizeLivingMemoryType(entry.Type)
	return legacyMemoryTypeToCanonical(normalizedType),
		clipTaskSessionSummary(strings.TrimSpace(entry.Content), 240),
		buildPromotedMemoryTags(normalizedType, entry.Source, entry.Topic),
		memoryImportance(normalizedType, entry.Source),
		memoryConfidence(normalizedType, entry.Source)
}

func defaultMemoryTopic(entryType, content string) string {
	if clipped := clipTaskSessionSummary(strings.TrimSpace(content), 72); clipped != "" {
		return clipped
	}
	switch normalizeLivingMemoryType(entryType) {
	case memory.TypeDecision:
		return "Decision"
	case memory.TypeProcedure:
		return "Procedure"
	case memory.TypeIncident:
		return "Incident"
	case memory.TypeLesson:
		return "Lesson"
	case memory.TypeUpdateDigest:
		return "Update Digest"
	case memory.TypeSummary:
		return "Summary"
	case memory.TypeEntity:
		return "Entity"
	case memory.TypeExperience:
		return "Experience"
	default:
		return "Memory"
	}
}

func buildPromotedMemoryTags(entryType, source, topic string) []string {
	tags := make([]string, 0, 5)
	if normalizedType := normalizeLivingMemoryType(entryType); normalizedType != "" {
		tags = append(tags, normalizedType)
	}
	if normalizedSource := strings.ToLower(strings.TrimSpace(source)); normalizedSource != "" {
		tags = append(tags, normalizedSource)
	}
	for _, keyword := range extractMemoryTopicKeywords(topic, 3) {
		tags = append(tags, keyword)
	}
	return uniqueLowercaseValues(tags)
}

func extractMemoryTopicKeywords(raw string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	fields := strings.FieldsFunc(strings.ToLower(raw), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' && r != '-'
	})
	out := make([]string, 0, limit)
	for _, field := range fields {
		if len(field) <= 3 {
			continue
		}
		out = append(out, field)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func uniqueLowercaseValues(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func memoryImportance(entryType, source string) float64 {
	score, _ := memoryScores(entryType, source)
	return score
}

func memoryConfidence(entryType, source string) float64 {
	_, score := memoryScores(entryType, source)
	return score
}

func memoryScores(entryType, source string) (importance, confidence float64) {
	switch normalizeLivingMemoryType(entryType) {
	case memory.TypeDecision:
		importance, confidence = 0.95, 0.84
	case memory.TypeProcedure:
		importance, confidence = 0.90, 0.82
	case memory.TypeIncident:
		importance, confidence = 0.88, 0.78
	case memory.TypeLesson:
		importance, confidence = 0.80, 0.76
	case memory.TypeEntity:
		importance, confidence = 0.72, 0.78
	case memory.TypeExperience:
		importance, confidence = 0.68, 0.66
	case memory.TypeUpdateDigest:
		importance, confidence = 0.62, 0.72
	case memory.TypeSummary:
		importance, confidence = 0.55, 0.60
	default:
		importance, confidence = 0.50, 0.60
	}

	switch strings.ToLower(strings.TrimSpace(source)) {
	case "manual":
		importance += 0.05
		confidence += 0.08
	case "reflection":
		confidence += 0.04
	case "session_event":
		importance += 0.04
		confidence += 0.03
	case "compaction":
		confidence -= 0.03
	}

	return clampMemoryUnitInterval(importance), clampMemoryUnitInterval(confidence)
}

func clampMemoryUnitInterval(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}
