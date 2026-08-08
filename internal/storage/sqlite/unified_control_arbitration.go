package sqlite

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
)

type unifiedControlRSPSnapshot struct {
	RiskScore       float64
	HiddenState     string
	CoherenceBand   string
	CapabilityFlags RSPCapabilityFlags
	GovernedHints   []RSPGovernedHint
}

type unifiedHintOverlayResult struct {
	Controls            ControlSuggestedControls
	AppliedActions      []string
	AppliedActionAudit  []UnifiedControlAppliedActionAudit
	AppliedHints        []string
	Suppressed          []string
	SuppressedHintAudit []UnifiedControlSuppressedHintAudit
}

type unifiedControlArbitrationResult struct {
	Controls            ControlSuggestedControls
	AppliedActions      []string
	AppliedActionAudit  []UnifiedControlAppliedActionAudit
	Suppressed          []string
	SuppressedHintAudit []UnifiedControlSuppressedHintAudit
	Contradictions      []string
	CooldownActive      bool
}

type unifiedControlArbitrationInput struct {
	Controls            ControlSuggestedControls
	CurrentMode         string
	CandidateMode       string
	CandidateStreak     int
	MemoryCoherenceBand string
	Hints               []RSPGovernedHint
	CapabilityFlags     RSPCapabilityFlags
}

func (s *Store) getLatestClusterRSPSnapshot(ctx context.Context, workspaceID, clusterID string) unifiedControlRSPSnapshot {
	snapshot := unifiedControlRSPSnapshot{
		CapabilityFlags: s.GetRSPCapabilityFlags(ctx, workspaceID),
	}
	events, err := s.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "rsp.state_snapshot",
		EntityType:  "rsp_state",
		EntityID:    clusterID,
		Limit:       1,
	})
	if err != nil || len(events) == 0 {
		return snapshot
	}
	var payload struct {
		RiskScore       float64            `json:"risk_score"`
		HiddenState     string             `json:"hidden_state"`
		CoherenceBand   string             `json:"coherence_band"`
		CapabilityFlags RSPCapabilityFlags `json:"capability_flags"`
		GovernedHints   []RSPGovernedHint  `json:"governed_hints"`
	}
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		return snapshot
	}
	snapshot.RiskScore = payload.RiskScore
	snapshot.HiddenState = strings.TrimSpace(payload.HiddenState)
	snapshot.CoherenceBand = strings.TrimSpace(payload.CoherenceBand)
	snapshot.GovernedHints = append([]RSPGovernedHint(nil), payload.GovernedHints...)
	if payload.CapabilityFlags != (RSPCapabilityFlags{}) {
		snapshot.CapabilityFlags = payload.CapabilityFlags
	}
	return snapshot
}

func (s *Store) getLatestClusterRSPRisk(ctx context.Context, workspaceID, clusterID string) (float64, string) {
	snapshot := s.getLatestClusterRSPSnapshot(ctx, workspaceID, clusterID)
	return snapshot.RiskScore, snapshot.HiddenState
}

func applyUnifiedHintOverlay(base ControlSuggestedControls, flags RSPCapabilityFlags, hints []RSPGovernedHint, coherenceBand string) unifiedHintOverlayResult {
	result := unifiedHintOverlayResult{
		Controls: base,
	}
	if !flags.GovernedHintsLive || len(hints) == 0 {
		return result
	}
	for _, hint := range normalizeUnifiedGovernedHints(hints) {
		hintID := strings.TrimSpace(hint.HintID)
		if hintID == "" {
			hintID = "rsp_hint"
		}
		switch {
		case !strings.EqualFold(strings.TrimSpace(hint.ActuationClass), "governed_hint"):
			result.Suppressed = appendOrderedUnique(result.Suppressed, hintID+":unsupported_actuation_class")
			result.SuppressedHintAudit = appendUnifiedSuppressedHintAudit(
				result.SuppressedHintAudit,
				UnifiedControlSuppressedHintAudit{
					HintID:     hintID,
					SourceKind: "governed_hint",
					Reason:     "unsupported_actuation_class",
				},
			)
			continue
		case hint.TTLEpochs <= 0:
			result.Suppressed = appendOrderedUnique(result.Suppressed, hintID+":expired")
			result.SuppressedHintAudit = appendUnifiedSuppressedHintAudit(
				result.SuppressedHintAudit,
				UnifiedControlSuppressedHintAudit{
					HintID:     hintID,
					SourceKind: "governed_hint",
					Reason:     "expired",
				},
			)
			continue
		}
		actions := normalizeUnifiedHintActions(hint.RecommendedActions)
		if len(actions) == 0 {
			result.Suppressed = appendOrderedUnique(result.Suppressed, hintID+":no_actions")
			result.SuppressedHintAudit = appendUnifiedSuppressedHintAudit(
				result.SuppressedHintAudit,
				UnifiedControlSuppressedHintAudit{
					HintID:     hintID,
					SourceKind: "governed_hint",
					Reason:     "no_actions",
				},
			)
			continue
		}
		for _, action := range actions {
			before := result.Controls
			switch action {
			case "require_far_reviewer":
				result.Controls.ReviewDepth = maxInt(result.Controls.ReviewDepth, 3)
				result.Controls.MergeThreshold = maxFloat(result.Controls.MergeThreshold, 0.85)
				result.Controls.PriorityFocus = "review"
				result.AppliedActions = appendOrderedUnique(result.AppliedActions, action)
				result.AppliedHints = appendOrderedUnique(result.AppliedHints, hintID)
				result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
					result.AppliedActionAudit,
					action,
					"governed_hint",
					hintID,
					before,
					result.Controls,
				)
			case "raise_reviewer_diversity":
				result.Controls.ReviewDepth = maxInt(result.Controls.ReviewDepth, 2)
				if result.Controls.PriorityFocus == "" || result.Controls.PriorityFocus == "coordination" {
					result.Controls.PriorityFocus = "review"
				}
				result.AppliedActions = appendOrderedUnique(result.AppliedActions, action)
				result.AppliedHints = appendOrderedUnique(result.AppliedHints, hintID)
				result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
					result.AppliedActionAudit,
					action,
					"governed_hint",
					hintID,
					before,
					result.Controls,
				)
			case "reduce_solver_fanout":
				result.Controls.FanoutCap = clampUnifiedPositiveCap(result.Controls.FanoutCap, 2)
				result.AppliedActions = appendOrderedUnique(result.AppliedActions, action)
				result.AppliedHints = appendOrderedUnique(result.AppliedHints, hintID)
				result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
					result.AppliedActionAudit,
					action,
					"governed_hint",
					hintID,
					before,
					result.Controls,
				)
			case "tighten_context_cap":
				result.Controls.ContextCap = clampUnifiedPositiveCap(result.Controls.ContextCap, 4)
				result.AppliedActions = appendOrderedUnique(result.AppliedActions, action)
				result.AppliedHints = appendOrderedUnique(result.AppliedHints, hintID)
				result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
					result.AppliedActionAudit,
					action,
					"governed_hint",
					hintID,
					before,
					result.Controls,
				)
			case "prefer_kernel_refresh":
				if strings.EqualFold(strings.TrimSpace(coherenceBand), "STABLE") {
					result.Suppressed = appendOrderedUnique(result.Suppressed, hintID+":prefer_kernel_refresh_without_memory_pressure")
					result.SuppressedHintAudit = appendUnifiedSuppressedHintAudit(
						result.SuppressedHintAudit,
						UnifiedControlSuppressedHintAudit{
							HintID:     hintID,
							SourceKind: "governed_hint",
							Action:     action,
							Reason:     "requires_memory_pressure",
						},
					)
					continue
				}
				result.Controls.PriorityFocus = "memory"
				result.AppliedActions = appendOrderedUnique(result.AppliedActions, action)
				result.AppliedHints = appendOrderedUnique(result.AppliedHints, hintID)
				result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
					result.AppliedActionAudit,
					action,
					"governed_hint",
					hintID,
					before,
					result.Controls,
				)
			default:
				result.Suppressed = appendOrderedUnique(result.Suppressed, hintID+":"+action)
				result.SuppressedHintAudit = appendUnifiedSuppressedHintAudit(
					result.SuppressedHintAudit,
					UnifiedControlSuppressedHintAudit{
						HintID:     hintID,
						SourceKind: "governed_hint",
						Action:     action,
						Reason:     "unsupported_action",
					},
				)
			}
		}
	}
	return result
}

func arbitrateUnifiedControl(input unifiedControlArbitrationInput) unifiedControlArbitrationResult {
	result := unifiedControlArbitrationResult{
		Controls: input.Controls,
	}
	currentMode := normalizeClusterControlMode(input.CurrentMode)
	candidateMode := normalizeClusterControlMode(input.CandidateMode)
	coherenceBand := strings.ToUpper(strings.TrimSpace(input.MemoryCoherenceBand))

	switch currentMode {
	case clusterControlModeStabilize, clusterControlModeAntiCollapse:
		result.Controls.FanoutCap = clampUnifiedPositiveCap(result.Controls.FanoutCap, 2)
	case clusterControlModeCoherence:
		result.Controls.ReviewDepth = maxInt(result.Controls.ReviewDepth, 3)
		result.Controls.MergeThreshold = maxFloat(result.Controls.MergeThreshold, 0.85)
		result.Controls.PriorityFocus = "review"
	case clusterControlModeDecentralize:
		result.Controls.ContextCap = clampUnifiedPositiveCap(result.Controls.ContextCap, 6)
	}

	switch coherenceBand {
	case "CRITICAL":
		preHint := result.Controls
		result.Controls.ContextCap = clampUnifiedPositiveCap(result.Controls.ContextCap, 4)
		result.Controls.ReviewDepth = maxInt(result.Controls.ReviewDepth, 2)
		result.Controls.MergeThreshold = maxFloat(result.Controls.MergeThreshold, 0.85)
		result.Controls.PriorityFocus = "memory"
		result.AppliedActions = appendOrderedUnique(result.AppliedActions, "memory_coherence_floor")
		result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
			result.AppliedActionAudit,
			"memory_coherence_floor",
			"memory_coherence_floor",
			"",
			preHint,
			result.Controls,
		)
		result.AppliedActions = appendOrderedUnique(result.AppliedActions, "prefer_kernel_refresh")
		result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
			result.AppliedActionAudit,
			"prefer_kernel_refresh",
			"memory_coherence_floor",
			"",
			result.Controls,
			result.Controls,
		)
		if currentMode == clusterControlModeSynergySeeking || candidateMode == clusterControlModeSynergySeeking {
			result.Contradictions = appendOrderedUnique(result.Contradictions, "coherence_floor_overrides_synergy_seeking")
		}
		if preHint.PriorityFocus != "" && preHint.PriorityFocus != result.Controls.PriorityFocus {
			result.Contradictions = appendOrderedUnique(result.Contradictions, "freshness_safety_overrides_non_memory_focus")
		}
	case "DEGRADED":
		preHint := result.Controls
		result.Controls.ContextCap = clampUnifiedPositiveCap(result.Controls.ContextCap, 6)
		result.Controls.MergeThreshold = maxFloat(result.Controls.MergeThreshold, 0.80)
		result.AppliedActions = appendOrderedUnique(result.AppliedActions, "memory_coherence_floor")
		result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
			result.AppliedActionAudit,
			"memory_coherence_floor",
			"memory_coherence_floor",
			"",
			preHint,
			result.Controls,
		)
	}

	if currentMode != clusterControlModeSteady && candidateMode != currentMode && candidateMode != "" && input.CandidateStreak > 0 {
		result.CooldownActive = true
		result.AppliedActions = appendOrderedUnique(result.AppliedActions, "mode_cooldown_active")
		result.AppliedActionAudit = appendUnifiedAppliedActionAudit(
			result.AppliedActionAudit,
			"mode_cooldown_active",
			"mode_cooldown",
			"",
			result.Controls,
			result.Controls,
		)
	}

	hintOverlay := applyUnifiedHintOverlay(result.Controls, input.CapabilityFlags, input.Hints, coherenceBand)
	result.Controls = hintOverlay.Controls
	result.AppliedActions = appendOrderedUniqueAll(result.AppliedActions, hintOverlay.AppliedActions)
	result.AppliedActionAudit = appendUnifiedAppliedActionAuditAll(result.AppliedActionAudit, hintOverlay.AppliedActionAudit)
	result.Suppressed = appendOrderedUniqueAll(result.Suppressed, hintOverlay.Suppressed)
	result.SuppressedHintAudit = appendUnifiedSuppressedHintAuditAll(result.SuppressedHintAudit, hintOverlay.SuppressedHintAudit)

	// Re-apply hard safety floors after hints so RSP remains advisory.
	postHint := result.Controls
	switch coherenceBand {
	case "CRITICAL":
		result.Controls.ContextCap = clampUnifiedPositiveCap(result.Controls.ContextCap, 4)
		result.Controls.ReviewDepth = maxInt(result.Controls.ReviewDepth, 2)
		result.Controls.MergeThreshold = maxFloat(result.Controls.MergeThreshold, 0.85)
		result.Controls.PriorityFocus = "memory"
	case "DEGRADED":
		result.Controls.ContextCap = clampUnifiedPositiveCap(result.Controls.ContextCap, 6)
		result.Controls.MergeThreshold = maxFloat(result.Controls.MergeThreshold, 0.80)
	}
	if currentMode == clusterControlModeCoherence {
		result.Controls.ReviewDepth = maxInt(result.Controls.ReviewDepth, 3)
		result.Controls.MergeThreshold = maxFloat(result.Controls.MergeThreshold, 0.85)
	}
	if postHint != result.Controls {
		result.Contradictions = appendOrderedUnique(result.Contradictions, "hard_safety_conditions_clamped_rsp_advice")
	}
	return result
}

func normalizeUnifiedGovernedHints(hints []RSPGovernedHint) []RSPGovernedHint {
	out := append([]RSPGovernedHint(nil), hints...)
	sort.Slice(out, func(i, j int) bool {
		left := strings.TrimSpace(out[i].HintID)
		right := strings.TrimSpace(out[j].HintID)
		if out[i].Severity != out[j].Severity {
			return out[i].Severity > out[j].Severity
		}
		return left < right
	})
	return out
}

func normalizeUnifiedHintActions(actions []string) []string {
	priority := map[string]int{
		"require_far_reviewer":     0,
		"raise_reviewer_diversity": 1,
		"reduce_solver_fanout":     2,
		"tighten_context_cap":      3,
		"prefer_kernel_refresh":    4,
	}
	out := uniqueTrimmedLocusStrings(actions)
	sort.Slice(out, func(i, j int) bool {
		left := priority[out[i]]
		right := priority[out[j]]
		if left != right {
			return left < right
		}
		return out[i] < out[j]
	})
	return out
}

func appendOrderedUniqueAll(values, next []string) []string {
	out := append([]string(nil), values...)
	for _, item := range next {
		out = appendOrderedUnique(out, item)
	}
	return out
}

func controlSuggestedDeltaFields(before, after ControlSuggestedControls) []string {
	var fields []string
	if before.PriorityFocus != after.PriorityFocus {
		fields = append(fields, "priority_focus")
	}
	if before.FanoutCap != after.FanoutCap {
		fields = append(fields, "fanout_cap")
	}
	if before.ReviewDepth != after.ReviewDepth {
		fields = append(fields, "review_depth")
	}
	if before.ContextCap != after.ContextCap {
		fields = append(fields, "context_cap")
	}
	if before.BridgeQuota != after.BridgeQuota {
		fields = append(fields, "bridge_quota")
	}
	if before.MergeThreshold != after.MergeThreshold {
		fields = append(fields, "merge_threshold")
	}
	return fields
}

func appendUnifiedAppliedActionAudit(audits []UnifiedControlAppliedActionAudit, action, sourceKind, hintID string, before, after ControlSuggestedControls) []UnifiedControlAppliedActionAudit {
	action = strings.TrimSpace(action)
	sourceKind = strings.TrimSpace(sourceKind)
	hintID = strings.TrimSpace(hintID)
	if action == "" {
		return audits
	}
	out := append([]UnifiedControlAppliedActionAudit(nil), audits...)
	deltaFields := controlSuggestedDeltaFields(before, after)
	for i := range out {
		if out[i].Action != action {
			continue
		}
		if sourceKind != "" {
			out[i].SourceKinds = appendOrderedUnique(out[i].SourceKinds, sourceKind)
		}
		if hintID != "" {
			out[i].HintIDs = appendOrderedUnique(out[i].HintIDs, hintID)
		}
		out[i].DeltaFields = appendOrderedUniqueAll(out[i].DeltaFields, deltaFields)
		out[i].Summary = summarizeUnifiedAppliedActionAudit(out[i])
		return out
	}
	entry := UnifiedControlAppliedActionAudit{
		Action:      action,
		DeltaFields: append([]string(nil), deltaFields...),
	}
	if sourceKind != "" {
		entry.SourceKinds = []string{sourceKind}
	}
	if hintID != "" {
		entry.HintIDs = []string{hintID}
	}
	entry.Summary = summarizeUnifiedAppliedActionAudit(entry)
	return append(out, entry)
}

func appendUnifiedAppliedActionAuditAll(audits, next []UnifiedControlAppliedActionAudit) []UnifiedControlAppliedActionAudit {
	out := append([]UnifiedControlAppliedActionAudit(nil), audits...)
	for _, entry := range next {
		out = mergeUnifiedAppliedActionAuditEntry(out, entry)
	}
	return out
}

func mergeUnifiedAppliedActionAuditEntry(audits []UnifiedControlAppliedActionAudit, next UnifiedControlAppliedActionAudit) []UnifiedControlAppliedActionAudit {
	next.Action = strings.TrimSpace(next.Action)
	if next.Action == "" {
		return audits
	}
	out := append([]UnifiedControlAppliedActionAudit(nil), audits...)
	for i := range out {
		if out[i].Action != next.Action {
			continue
		}
		out[i].SourceKinds = appendOrderedUniqueAll(out[i].SourceKinds, next.SourceKinds)
		out[i].HintIDs = appendOrderedUniqueAll(out[i].HintIDs, next.HintIDs)
		out[i].DeltaFields = appendOrderedUniqueAll(out[i].DeltaFields, next.DeltaFields)
		out[i].Summary = summarizeUnifiedAppliedActionAudit(out[i])
		return out
	}
	next.SourceKinds = uniqueTrimmedLocusStrings(next.SourceKinds)
	next.HintIDs = uniqueTrimmedLocusStrings(next.HintIDs)
	next.DeltaFields = uniqueTrimmedLocusStrings(next.DeltaFields)
	next.Summary = summarizeUnifiedAppliedActionAudit(next)
	return append(out, next)
}

func summarizeUnifiedAppliedActionAudit(entry UnifiedControlAppliedActionAudit) string {
	parts := []string{entry.Action}
	if len(entry.SourceKinds) > 0 {
		parts = append(parts, "sources "+strings.Join(entry.SourceKinds, ", "))
	}
	if len(entry.HintIDs) > 0 {
		parts = append(parts, "hints "+strings.Join(entry.HintIDs, ", "))
	}
	if len(entry.DeltaFields) > 0 {
		parts = append(parts, "changed "+strings.Join(entry.DeltaFields, ", "))
	} else {
		parts = append(parts, "no effective parameter delta")
	}
	return strings.Join(parts, " | ")
}

func appendUnifiedSuppressedHintAudit(audits []UnifiedControlSuppressedHintAudit, next UnifiedControlSuppressedHintAudit) []UnifiedControlSuppressedHintAudit {
	next.HintID = strings.TrimSpace(next.HintID)
	next.SourceKind = strings.TrimSpace(next.SourceKind)
	next.Action = strings.TrimSpace(next.Action)
	next.Reason = strings.TrimSpace(next.Reason)
	if next.HintID == "" || next.Reason == "" {
		return audits
	}
	next.Summary = summarizeUnifiedSuppressedHintAudit(next)
	for _, existing := range audits {
		if existing.HintID == next.HintID && existing.SourceKind == next.SourceKind && existing.Action == next.Action && existing.Reason == next.Reason {
			return audits
		}
	}
	out := append([]UnifiedControlSuppressedHintAudit(nil), audits...)
	return append(out, next)
}

func appendUnifiedSuppressedHintAuditAll(audits, next []UnifiedControlSuppressedHintAudit) []UnifiedControlSuppressedHintAudit {
	out := append([]UnifiedControlSuppressedHintAudit(nil), audits...)
	for _, entry := range next {
		out = appendUnifiedSuppressedHintAudit(out, entry)
	}
	return out
}

func summarizeUnifiedSuppressedHintAudit(entry UnifiedControlSuppressedHintAudit) string {
	parts := []string{entry.HintID}
	if entry.SourceKind != "" {
		parts = append(parts, entry.SourceKind)
	}
	if entry.Action != "" {
		parts = append(parts, "action "+entry.Action)
	}
	parts = append(parts, "suppressed "+entry.Reason)
	return strings.Join(parts, " | ")
}
