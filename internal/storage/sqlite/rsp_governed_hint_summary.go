package sqlite

import "strings"

type RSPGovernedHintSummary struct {
	TotalHints               int            `json:"total_hints"`
	RecommendationClassCount map[string]int `json:"recommendation_class_count,omitempty"`
	EvidenceSourceMixCount   map[string]int `json:"evidence_source_mix_count,omitempty"`
	TTLWindowStateCount      map[string]int `json:"ttl_window_state_count,omitempty"`
	RuntimeLineageBasisCount map[string]int `json:"runtime_lineage_basis_count,omitempty"`
	OutcomeCount             map[string]int `json:"outcome_count,omitempty"`
}

func buildRSPGovernedHintSummary(hints []RSPGovernedHint, outcomes []UnifiedControlGovernedHintOutcome) *RSPGovernedHintSummary {
	if len(hints) == 0 && len(outcomes) == 0 {
		return nil
	}
	summary := &RSPGovernedHintSummary{
		RecommendationClassCount: map[string]int{},
		EvidenceSourceMixCount:   map[string]int{},
		TTLWindowStateCount:      map[string]int{},
		RuntimeLineageBasisCount: map[string]int{},
		OutcomeCount:             map[string]int{},
	}
	for _, hint := range hints {
		summary.TotalHints++
		incrementRSPGovernedHintSummaryCount(summary.RecommendationClassCount, strings.TrimSpace(hint.RecommendationClass))
		incrementRSPGovernedHintSummaryCount(summary.EvidenceSourceMixCount, strings.TrimSpace(hint.EvidenceSourceMix))
		incrementRSPGovernedHintSummaryCount(summary.TTLWindowStateCount, strings.TrimSpace(hint.TTLWindowState))
		incrementRSPGovernedHintSummaryCount(summary.RuntimeLineageBasisCount, strings.TrimSpace(hint.RuntimeLineageBasis))
	}
	for _, outcome := range outcomes {
		incrementRSPGovernedHintSummaryCount(summary.OutcomeCount, strings.TrimSpace(outcome.ArbitrationOutcome))
	}
	if len(summary.RecommendationClassCount) == 0 {
		summary.RecommendationClassCount = nil
	}
	if len(summary.EvidenceSourceMixCount) == 0 {
		summary.EvidenceSourceMixCount = nil
	}
	if len(summary.TTLWindowStateCount) == 0 {
		summary.TTLWindowStateCount = nil
	}
	if len(summary.RuntimeLineageBasisCount) == 0 {
		summary.RuntimeLineageBasisCount = nil
	}
	if len(summary.OutcomeCount) == 0 {
		summary.OutcomeCount = nil
	}
	return summary
}

func incrementRSPGovernedHintSummaryCount(counts map[string]int, key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "UNSPECIFIED"
	}
	counts[key]++
}
