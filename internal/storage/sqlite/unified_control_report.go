package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type UnifiedControlReportFilter struct {
	WorkspaceID    string
	ProtoClusterID string
	AgentID        string
	TaskID         string
	SessionID      string
	DocKeys        []string
	ArtifactRefs   []string
	FrontierLimit  int
}

type UnifiedControlSnapshotInput struct {
	ActorID string
}

type UnifiedControlAppliedActionAudit struct {
	Action      string   `json:"action"`
	SourceKinds []string `json:"source_kinds,omitempty"`
	HintIDs     []string `json:"hint_ids,omitempty"`
	DeltaFields []string `json:"delta_fields,omitempty"`
	Summary     string   `json:"summary,omitempty"`
}

type UnifiedControlSuppressedHintAudit struct {
	HintID     string `json:"hint_id"`
	SourceKind string `json:"source_kind,omitempty"`
	Action     string `json:"action,omitempty"`
	Reason     string `json:"reason"`
	Summary    string `json:"summary,omitempty"`
}

type UnifiedControlAuditSummary struct {
	AppliedEntryCount              int            `json:"applied_entry_count"`
	AppliedSourceKindCount         map[string]int `json:"applied_source_kind_count,omitempty"`
	HintBackedActionCount          int            `json:"hint_backed_action_count"`
	DeltaBearingActionCount        int            `json:"delta_bearing_action_count"`
	SuppressedEntryCount           int            `json:"suppressed_entry_count"`
	SuppressedSourceKindCount      map[string]int `json:"suppressed_source_kind_count,omitempty"`
	SuppressedEntriesWithActionRef int            `json:"suppressed_entries_with_action_ref_count"`
	SuppressionReasonCount         map[string]int `json:"suppression_reason_count,omitempty"`
}

type UnifiedControlAuditCoverage struct {
	AppliedEntriesWithSourceKinds   int `json:"applied_entries_with_source_kinds"`
	AppliedEntriesWithHintRefs      int `json:"applied_entries_with_hint_refs"`
	AppliedEntriesWithDeltaFields   int `json:"applied_entries_with_delta_fields"`
	AppliedEntriesWithSummary       int `json:"applied_entries_with_summary"`
	FullAppliedTraceEntryCount      int `json:"full_applied_trace_entry_count"`
	SuppressedEntriesWithSourceKind int `json:"suppressed_entries_with_source_kind"`
	SuppressedEntriesWithActionRef  int `json:"suppressed_entries_with_action_ref"`
	SuppressedEntriesWithReason     int `json:"suppressed_entries_with_reason"`
	SuppressedEntriesWithSummary    int `json:"suppressed_entries_with_summary"`
	FullSuppressedTraceEntryCount   int `json:"full_suppressed_trace_entry_count"`
}

type UnifiedControlEffectiveControlBasis struct {
	Field          string   `json:"field"`
	SuggestedValue string   `json:"suggested_value,omitempty"`
	EffectiveValue string   `json:"effective_value,omitempty"`
	Changed        bool     `json:"changed"`
	AppliedActions []string `json:"applied_actions,omitempty"`
	SourceKinds    []string `json:"source_kinds,omitempty"`
	HintIDs        []string `json:"hint_ids,omitempty"`
	Summary        string   `json:"summary,omitempty"`
}

type UnifiedControlEffectiveControlBasisSummary struct {
	FieldCount                 int `json:"field_count"`
	ChangedFieldCount          int `json:"changed_field_count"`
	FieldsWithActionTraceCount int `json:"fields_with_action_trace_count"`
	FieldsWithHintTraceCount   int `json:"fields_with_hint_trace_count"`
	FieldsWithMultiSourceCount int `json:"fields_with_multi_source_count"`
}

type UnifiedControlContradictionSummary struct {
	TotalCount                int            `json:"total_count"`
	FamilyCount               map[string]int `json:"family_count,omitempty"`
	HardSafetyClampCount      int            `json:"hard_safety_clamp_count"`
	MemorySafetyOverrideCount int            `json:"memory_safety_override_count"`
	OtherCount                int            `json:"other_count"`
}

type UnifiedControlCooldownBasis struct {
	CurrentMode                   string                             `json:"current_mode,omitempty"`
	CandidateMode                 string                             `json:"candidate_mode,omitempty"`
	Stage                         string                             `json:"stage,omitempty"`
	AcceptanceReadiness           string                             `json:"acceptance_readiness,omitempty"`
	AcceptanceGateReason          string                             `json:"acceptance_gate_reason,omitempty"`
	AcceptanceChecklist           *UnifiedControlAcceptanceChecklist `json:"acceptance_checklist,omitempty"`
	AcceptanceMissingRequirements []string                           `json:"acceptance_missing_requirements,omitempty"`
	AcceptanceProgressBand        string                             `json:"acceptance_progress_band,omitempty"`
	CandidateStreak               int                                `json:"candidate_streak"`
	RequiredStreak                int                                `json:"required_streak"`
	RemainingStreak               int                                `json:"remaining_streak"`
	ReadyToStabilize              bool                               `json:"ready_to_stabilize"`
	Transitioning                 bool                               `json:"transitioning"`
	CooldownActive                bool                               `json:"cooldown_active"`
	BlockingReasons               []string                           `json:"blocking_reasons,omitempty"`
	BlockingReasonCount           int                                `json:"blocking_reason_count"`
	Reason                        string                             `json:"reason,omitempty"`
	Summary                       string                             `json:"summary,omitempty"`
}

type UnifiedControlAcceptanceChecklist struct {
	CandidatePresent     bool `json:"candidate_present"`
	CandidateDiverges    bool `json:"candidate_diverges"`
	HysteresisSatisfied  bool `json:"hysteresis_satisfied"`
	CooldownClear        bool `json:"cooldown_clear"`
	ContradictionClear   bool `json:"contradiction_clear"`
	MemoryAttentionClear bool `json:"memory_attention_clear"`
}

type UnifiedControlGovernedHintOutcome struct {
	HintID              string   `json:"hint_id"`
	Type                string   `json:"type,omitempty"`
	RecommendationClass string   `json:"recommendation_class,omitempty"`
	Severity            float64  `json:"severity"`
	EvidenceDiversity   float64  `json:"evidence_diversity"`
	ArbitrationOutcome  string   `json:"arbitration_outcome"`
	AppliedActions      []string `json:"applied_actions,omitempty"`
	SuppressedActions   []string `json:"suppressed_actions,omitempty"`
	SuppressionReasons  []string `json:"suppression_reasons,omitempty"`
	Summary             string   `json:"summary,omitempty"`
}

type UnifiedControlEffectiveControlsAudit struct {
	Found            bool                     `json:"found"`
	Live             bool                     `json:"live"`
	Expired          bool                     `json:"expired"`
	Pending          bool                     `json:"pending"`
	ScopeSource      string                   `json:"scope_source,omitempty"`
	Epoch            int                      `json:"epoch,omitempty"`
	TTLSeconds       int                      `json:"ttl_seconds,omitempty"`
	ExpiresAt        string                   `json:"expires_at,omitempty"`
	GeneratedAt      string                   `json:"generated_at,omitempty"`
	ActorID          string                   `json:"actor_id,omitempty"`
	ResolvedFrom     string                   `json:"resolved_from,omitempty"`
	MatchScore       int                      `json:"match_score,omitempty"`
	BasisSummary     string                   `json:"basis_summary,omitempty"`
	TemporalContract *TemporalHorizonContract `json:"temporal_contract,omitempty"`
}

type UnifiedControlReport struct {
	WorkspaceID                  string                                      `json:"workspace_id"`
	ProtoClusterID               string                                      `json:"proto_cluster_id,omitempty"`
	Resolved                     bool                                        `json:"resolved"`
	ResolvedFrom                 string                                      `json:"resolved_from,omitempty"`
	MatchScore                   int                                         `json:"match_score,omitempty"`
	AdvisoryOnly                 bool                                        `json:"advisory_only"`
	ControlOrder                 []string                                    `json:"control_order"`
	CapabilityFlags              RSPCapabilityFlags                          `json:"capability_flags"`
	ControlMode                  string                                      `json:"control_mode,omitempty"`
	CandidateMode                string                                      `json:"candidate_mode,omitempty"`
	AttentionBand                string                                      `json:"attention_band,omitempty"`
	PressureScore                int                                         `json:"pressure_score"`
	SuggestedControls            ControlSuggestedControls                    `json:"suggested_controls"`
	AdvisoryControls             ControlSuggestedControls                    `json:"advisory_controls"`
	CandidateControls            ControlSuggestedControls                    `json:"candidate_controls"`
	EffectiveControls            ControlSuggestedControls                    `json:"effective_controls"`
	EffectiveControlsAudit       *UnifiedControlEffectiveControlsAudit       `json:"effective_controls_audit,omitempty"`
	EffectiveControlBasis        []UnifiedControlEffectiveControlBasis       `json:"effective_control_basis,omitempty"`
	EffectiveControlBasisSummary *UnifiedControlEffectiveControlBasisSummary `json:"effective_control_basis_summary,omitempty"`
	MemoryCoherenceBand          string                                      `json:"memory_coherence_band,omitempty"`
	MemoryNeedsAttention         bool                                        `json:"memory_needs_attention"`
	MemoryAttentionReasons       []string                                    `json:"memory_attention_reasons,omitempty"`
	RSPHiddenState               string                                      `json:"rsp_hidden_state,omitempty"`
	RSPRiskBand                  string                                      `json:"rsp_risk_band,omitempty"`
	RSPRiskScore                 float64                                     `json:"rsp_risk_score"`
	GovernedHints                []RSPGovernedHint                           `json:"governed_hints,omitempty"`
	GovernedHintSummary          *RSPGovernedHintSummary                     `json:"governed_hint_summary,omitempty"`
	CooldownActive               bool                                        `json:"cooldown_active"`
	CooldownBasis                *UnifiedControlCooldownBasis                `json:"cooldown_basis,omitempty"`
	AppliedActions               []string                                    `json:"applied_actions,omitempty"`
	AppliedActionAudit           []UnifiedControlAppliedActionAudit          `json:"applied_action_audit,omitempty"`
	SuppressedHints              []string                                    `json:"suppressed_hints,omitempty"`
	SuppressedHintAudit          []UnifiedControlSuppressedHintAudit         `json:"suppressed_hint_audit,omitempty"`
	AuditSummary                 *UnifiedControlAuditSummary                 `json:"audit_summary,omitempty"`
	AuditCoverage                *UnifiedControlAuditCoverage                `json:"audit_coverage,omitempty"`
	GovernedHintOutcomes         []UnifiedControlGovernedHintOutcome         `json:"governed_hint_outcomes,omitempty"`
	Contradictions               []string                                    `json:"contradictions,omitempty"`
	ContradictionSummary         *UnifiedControlContradictionSummary         `json:"contradiction_summary,omitempty"`
	TimeAuthority                WorkspaceTimeAuthority                      `json:"time_authority"`
	GeneratedAt                  string                                      `json:"generated_at"`
	Summary                      string                                      `json:"summary"`
	readSurfacePolicy            ReadSurfacePolicy                           `json:"-"`
}

func normalizeUnifiedControlReportFilter(filter UnifiedControlReportFilter) UnifiedControlReportFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.DocKeys = uniqueTrimmedLocusStrings(filter.DocKeys)
	filter.ArtifactRefs = uniqueTrimmedLocusStrings(filter.ArtifactRefs)
	filter.FrontierLimit = clampReadSurfaceLimit(filter.FrontierLimit, readSurfaceFrontierLimitDefault, readSurfaceFrontierLimitMax)
	return filter
}

func (s *Store) BuildUnifiedControlReport(ctx context.Context, filter UnifiedControlReportFilter) (UnifiedControlReport, error) {
	filter = normalizeUnifiedControlReportFilter(filter)
	if filter.WorkspaceID == "" {
		return UnifiedControlReport{}, errors.New("workspace_id is required")
	}

	bundle, err := s.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		AgentID:        filter.AgentID,
		TaskID:         filter.TaskID,
		SessionID:      filter.SessionID,
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	})
	if err != nil {
		return UnifiedControlReport{}, err
	}

	report := UnifiedControlReport{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: strings.TrimSpace(bundle.ProtoClusterID),
		Resolved:       bundle.Resolved,
		ResolvedFrom:   strings.TrimSpace(bundle.ResolvedFrom),
		MatchScore:     bundle.MatchScore,
		AdvisoryOnly:   true,
		ControlOrder: []string{
			"event_time_ingest",
			"rsp_epoch_update",
			"rrp_coordination_update",
			"rmp_memory_update",
			"arbitration_and_saturation",
		},
		CapabilityFlags:   s.GetRSPCapabilityFlags(ctx, filter.WorkspaceID),
		readSurfacePolicy: unifiedControlReadSurfacePolicy(filter),
	}
	report.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return UnifiedControlReport{}, err
	}
	report.GeneratedAt = generatedAtFromWorkspaceTimeAuthority(report.TimeAuthority)
	if !report.Resolved {
		report.Summary = fmt.Sprintf("unified control report for %s unresolved: no matching locus", report.WorkspaceID)
		return report, nil
	}

	stateReport := buildRSPStateReportFromBundle(s, ctx, RSPStateReportFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		AgentID:        filter.AgentID,
		TaskID:         filter.TaskID,
		SessionID:      filter.SessionID,
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	}, bundle)

	report.RSPHiddenState = stateReport.HiddenState
	report.RSPRiskBand = stateReport.RiskBand
	report.RSPRiskScore = stateReport.RiskScore
	report.GovernedHints = append([]RSPGovernedHint(nil), stateReport.GovernedHints...)

	if bundle.ControlState != nil {
		report.ControlMode = strings.TrimSpace(bundle.ControlState.State.State.CurrentMode)
		report.CandidateMode = strings.TrimSpace(bundle.ControlState.State.State.CandidateMode)
		report.AttentionBand = strings.TrimSpace(bundle.ControlState.State.State.AttentionBand)
		report.PressureScore = bundle.ControlState.State.State.PressureScore
	}
	if bundle.Control != nil {
		report.AttentionBand = strings.TrimSpace(bundle.Control.Cluster.Signals.AttentionBand)
		report.PressureScore = bundle.Control.Cluster.Signals.PressureScore
		report.SuggestedControls = bundle.Control.Cluster.SuggestedControls
	} else if bundle.ControlState != nil {
		report.SuggestedControls = bundle.ControlState.State.State.OperatorHints
	}
	report.AdvisoryControls = report.SuggestedControls
	report.EffectiveControlsAudit = &UnifiedControlEffectiveControlsAudit{
		Found:       false,
		Live:        false,
		Expired:     false,
		ScopeSource: "candidate_only",
	}
	if bundle.MemoryCoherence != nil {
		report.MemoryCoherenceBand = strings.TrimSpace(bundle.MemoryCoherence.CoherenceBandHint)
		report.MemoryNeedsAttention = bundle.MemoryCoherence.NeedsAttention
		report.MemoryAttentionReasons = append([]string(nil), bundle.MemoryCoherence.AttentionReasons...)
	}
	candidateStreak := unifiedCandidateStreak(bundle)
	arbitration := arbitrateUnifiedControl(unifiedControlArbitrationInput{
		Controls:            report.AdvisoryControls,
		CurrentMode:         report.ControlMode,
		CandidateMode:       report.CandidateMode,
		CandidateStreak:     candidateStreak,
		MemoryCoherenceBand: report.MemoryCoherenceBand,
		Hints:               report.GovernedHints,
		CapabilityFlags:     report.CapabilityFlags,
	})
	report.CandidateControls = arbitration.Controls
	report.EffectiveControls = report.CandidateControls
	if resolution, err := s.ResolveEffectiveControlsScope(ctx, report.WorkspaceID, report.ProtoClusterID, report.GeneratedAt); err != nil {
		return UnifiedControlReport{}, fmt.Errorf("load effective controls for unified report: %w", err)
	} else if resolution.Found {
		report.EffectiveControlsAudit = &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         resolution.Live,
			Expired:      resolution.Record.Expired,
			Pending:      resolution.Record.Pending,
			ScopeSource:  resolution.ScopeSource,
			Epoch:        resolution.Record.Epoch,
			TTLSeconds:   resolution.Record.TTLSeconds,
			ExpiresAt:    resolution.Record.ExpiresAt,
			GeneratedAt:  resolution.Record.GeneratedAt,
			ActorID:      resolution.Record.ActorID,
			ResolvedFrom: resolution.Record.ResolvedFrom,
			MatchScore:   resolution.Record.MatchScore,
			BasisSummary: resolution.Record.BasisSummary,
		}
		applyUnifiedEffectiveControlsTemporalContract(report.EffectiveControlsAudit, report.GeneratedAt)
		if resolution.Live {
			report.AdvisoryOnly = false
			report.EffectiveControls = resolution.Record.EffectiveControls
		}
	}
	report.AppliedActions = appendOrderedUniqueAll(report.AppliedActions, arbitration.AppliedActions)
	report.AppliedActionAudit = appendUnifiedAppliedActionAuditAll(report.AppliedActionAudit, arbitration.AppliedActionAudit)
	report.SuppressedHints = appendOrderedUniqueAll(report.SuppressedHints, arbitration.Suppressed)
	report.SuppressedHintAudit = appendUnifiedSuppressedHintAuditAll(report.SuppressedHintAudit, arbitration.SuppressedHintAudit)
	report.EffectiveControlBasis = buildUnifiedControlEffectiveControlBasis(report.AdvisoryControls, report.EffectiveControls, report.AppliedActionAudit)
	report.EffectiveControlBasisSummary = buildUnifiedControlEffectiveControlBasisSummary(report.EffectiveControlBasis)
	report.AuditSummary = buildUnifiedControlAuditSummary(report.AppliedActionAudit, report.SuppressedHintAudit)
	report.AuditCoverage = buildUnifiedControlAuditCoverage(report.AppliedActionAudit, report.SuppressedHintAudit)
	report.GovernedHintOutcomes = buildUnifiedGovernedHintOutcomes(report.GovernedHints, report.AppliedActionAudit, report.SuppressedHintAudit)
	report.GovernedHintSummary = buildRSPGovernedHintSummary(report.GovernedHints, report.GovernedHintOutcomes)
	report.Contradictions = appendOrderedUniqueAll(report.Contradictions, arbitration.Contradictions)
	report.ContradictionSummary = buildUnifiedControlContradictionSummary(report.Contradictions)
	report.CooldownActive = arbitration.CooldownActive
	report.CooldownBasis = buildUnifiedControlCooldownBasis(report.ControlMode, report.CandidateMode, candidateStreak, report.CooldownActive, report.Contradictions, report.MemoryNeedsAttention)

	reportKind := "advisory"
	if !report.AdvisoryOnly {
		reportKind = "effective"
	}
	report.Summary = fmt.Sprintf(
		"%s unified control report for %s/%s: mode=%s risk=%s coherence=%s actions=%d contradictions=%d",
		reportKind,
		firstNonEmpty(report.WorkspaceID, "workspace"),
		firstNonEmpty(report.ProtoClusterID, "scope"),
		firstNonEmpty(report.ControlMode, "n/a"),
		firstNonEmpty(report.RSPRiskBand, "n/a"),
		firstNonEmpty(report.MemoryCoherenceBand, "STABLE"),
		len(report.AppliedActions),
		len(report.Contradictions),
	)
	return report, nil
}

func (s *Store) RecordUnifiedControlSnapshot(ctx context.Context, report UnifiedControlReport, filter UnifiedControlReportFilter, input UnifiedControlSnapshotInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(report.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	filter = normalizeUnifiedControlReportFilter(filter)
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		if !report.Resolved || strings.TrimSpace(report.ProtoClusterID) != clusterID {
			return RuntimeEventRecord{}, fmt.Errorf("unified control cluster not found: %s/%s", workspaceID, clusterID)
		}
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "unified.control.snapshot"
	}
	summary := unifiedControlSnapshotSummary(report, filter)
	payload := map[string]any{
		"workspace_id":                          report.WorkspaceID,
		"filter":                                filter,
		"report":                                report,
		"summary":                               summary,
		"typed_event_type":                      unifiedControlSnapshotTypedEventType(report),
		"event_kind":                            unifiedControlSnapshotEventType(report),
		"governed_hint_count":                   len(report.GovernedHints),
		"applied_action_count":                  len(report.AppliedActions),
		"suppressed_hint_count":                 len(report.SuppressedHints),
		"governed_hint_outcome_count":           len(report.GovernedHintOutcomes),
		"effective_control_basis_field_count":   unifiedControlBasisSummaryCount(report.EffectiveControlBasisSummary, func(s UnifiedControlEffectiveControlBasisSummary) int { return s.FieldCount }),
		"effective_control_basis_changed_count": unifiedControlBasisSummaryCount(report.EffectiveControlBasisSummary, func(s UnifiedControlEffectiveControlBasisSummary) int { return s.ChangedFieldCount }),
		"effective_control_basis_fields_with_action_trace_count": unifiedControlBasisSummaryCount(report.EffectiveControlBasisSummary, func(s UnifiedControlEffectiveControlBasisSummary) int { return s.FieldsWithActionTraceCount }),
		"effective_control_basis_fields_with_hint_trace_count":   unifiedControlBasisSummaryCount(report.EffectiveControlBasisSummary, func(s UnifiedControlEffectiveControlBasisSummary) int { return s.FieldsWithHintTraceCount }),
		"contradiction_count":                        unifiedControlContradictionSummaryCount(report.ContradictionSummary, func(s UnifiedControlContradictionSummary) int { return s.TotalCount }),
		"hard_safety_contradiction_count":            unifiedControlContradictionSummaryCount(report.ContradictionSummary, func(s UnifiedControlContradictionSummary) int { return s.HardSafetyClampCount }),
		"memory_safety_override_contradiction_count": unifiedControlContradictionSummaryCount(report.ContradictionSummary, func(s UnifiedControlContradictionSummary) int { return s.MemorySafetyOverrideCount }),
		"audit_applied_entry_count":                  unifiedControlAuditSummaryCount(report.AuditSummary, func(s UnifiedControlAuditSummary) int { return s.AppliedEntryCount }),
		"audit_hint_backed_action_count":             unifiedControlAuditSummaryCount(report.AuditSummary, func(s UnifiedControlAuditSummary) int { return s.HintBackedActionCount }),
		"audit_delta_bearing_action_count":           unifiedControlAuditSummaryCount(report.AuditSummary, func(s UnifiedControlAuditSummary) int { return s.DeltaBearingActionCount }),
		"audit_suppressed_entry_count":               unifiedControlAuditSummaryCount(report.AuditSummary, func(s UnifiedControlAuditSummary) int { return s.SuppressedEntryCount }),
		"audit_suppressed_entries_with_action_ref_count": unifiedControlAuditSummaryCount(report.AuditSummary, func(s UnifiedControlAuditSummary) int {
			return s.SuppressedEntriesWithActionRef
		}),
		"audit_coverage_applied_entries_with_source_kinds": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.AppliedEntriesWithSourceKinds
		}),
		"audit_coverage_applied_entries_with_hint_refs": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.AppliedEntriesWithHintRefs
		}),
		"audit_coverage_applied_entries_with_delta_fields": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.AppliedEntriesWithDeltaFields
		}),
		"audit_coverage_applied_entries_with_summary": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.AppliedEntriesWithSummary
		}),
		"audit_coverage_full_applied_trace_entry_count": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.FullAppliedTraceEntryCount
		}),
		"audit_coverage_suppressed_entries_with_source_kind": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.SuppressedEntriesWithSourceKind
		}),
		"audit_coverage_suppressed_entries_with_action_ref": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.SuppressedEntriesWithActionRef
		}),
		"audit_coverage_suppressed_entries_with_reason": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.SuppressedEntriesWithReason
		}),
		"audit_coverage_suppressed_entries_with_summary": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.SuppressedEntriesWithSummary
		}),
		"audit_coverage_full_suppressed_trace_entry_count": unifiedControlAuditCoverageCount(report.AuditCoverage, func(s UnifiedControlAuditCoverage) int {
			return s.FullSuppressedTraceEntryCount
		}),
		"resolved":        report.Resolved,
		"resolved_from":   report.ResolvedFrom,
		"advisory_only":   report.AdvisoryOnly,
		"cooldown_active": report.CooldownActive,
	}
	if report.EffectiveControlsAudit != nil {
		payload["effective_controls_found"] = report.EffectiveControlsAudit.Found
		payload["effective_controls_live"] = report.EffectiveControlsAudit.Live
		payload["effective_controls_expired"] = report.EffectiveControlsAudit.Expired
		payload["effective_controls_pending"] = report.EffectiveControlsAudit.Pending
		payload["effective_controls_scope_source"] = report.EffectiveControlsAudit.ScopeSource
		payload["effective_controls_epoch"] = report.EffectiveControlsAudit.Epoch
		payload["effective_controls_ttl_seconds"] = report.EffectiveControlsAudit.TTLSeconds
		payload["effective_controls_expires_at"] = report.EffectiveControlsAudit.ExpiresAt
		payload["effective_controls_generated_at"] = report.EffectiveControlsAudit.GeneratedAt
		payload["effective_controls_actor_id"] = report.EffectiveControlsAudit.ActorID
		payload["effective_controls_resolved_from"] = report.EffectiveControlsAudit.ResolvedFrom
		payload["effective_controls_match_score"] = report.EffectiveControlsAudit.MatchScore
		payload["effective_controls_basis_summary"] = report.EffectiveControlsAudit.BasisSummary
	}
	if report.CooldownBasis != nil {
		payload["cooldown_current_mode"] = report.CooldownBasis.CurrentMode
		payload["cooldown_candidate_mode"] = report.CooldownBasis.CandidateMode
		payload["cooldown_stage"] = report.CooldownBasis.Stage
		payload["cooldown_acceptance_readiness"] = report.CooldownBasis.AcceptanceReadiness
		payload["cooldown_acceptance_gate_reason"] = report.CooldownBasis.AcceptanceGateReason
		payload["cooldown_acceptance_clear_count"] = unifiedControlAcceptanceChecklistClearCount(report.CooldownBasis.AcceptanceReadiness, report.CooldownBasis.AcceptanceChecklist)
		payload["cooldown_acceptance_requirement_count"] = unifiedControlAcceptanceChecklistRequirementCount(report.CooldownBasis.AcceptanceReadiness, report.CooldownBasis.AcceptanceChecklist)
		payload["cooldown_acceptance_missing_requirement_count"] = len(report.CooldownBasis.AcceptanceMissingRequirements)
		payload["cooldown_acceptance_missing_requirements"] = append([]string(nil), report.CooldownBasis.AcceptanceMissingRequirements...)
		payload["cooldown_acceptance_progress_band"] = report.CooldownBasis.AcceptanceProgressBand
		payload["cooldown_candidate_streak"] = report.CooldownBasis.CandidateStreak
		payload["cooldown_required_streak"] = report.CooldownBasis.RequiredStreak
		payload["cooldown_remaining_streak"] = report.CooldownBasis.RemainingStreak
		payload["cooldown_ready_to_stabilize"] = report.CooldownBasis.ReadyToStabilize
		payload["cooldown_transitioning"] = report.CooldownBasis.Transitioning
		payload["cooldown_blocking_reason_count"] = report.CooldownBasis.BlockingReasonCount
		payload["cooldown_blocking_reasons"] = append([]string(nil), report.CooldownBasis.BlockingReasons...)
		payload["cooldown_reason"] = report.CooldownBasis.Reason
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin unified control snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: workspaceID,
			EventType:   unifiedControlSnapshotEventType(report),
			EntityType:  "instrumentation_unified_control",
			EntityID:    unifiedControlSnapshotEntityID(filter),
			ActorType:   "operator",
			ActorID:     actorID,
			SessionID:   unifiedControlSnapshotSessionID(report, filter),
			TaskID:      unifiedControlSnapshotTaskID(report, filter),
			PayloadJSON: mustJSON(payload),
			CreatedAt:   referenceAt,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit unified control snapshot tx: %w", err)
	}
	return record, nil
}

func unifiedControlSnapshotEventType(report UnifiedControlReport) string {
	if report.AdvisoryOnly {
		return "cluster.unified_control_advisory_snapshot"
	}
	return "cluster.unified_control_effective_snapshot"
}

func unifiedControlSnapshotTypedEventType(report UnifiedControlReport) string {
	if report.AdvisoryOnly {
		return "UNIFIED_CONTROL_ADVISORY_SNAPSHOT"
	}
	return "UNIFIED_CONTROL_EFFECTIVE_SNAPSHOT"
}

func unifiedControlSnapshotEntityID(filter UnifiedControlReportFilter) string {
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		return clusterID
	}
	return strings.TrimSpace(filter.WorkspaceID)
}

func unifiedControlSnapshotTaskID(report UnifiedControlReport, filter UnifiedControlReportFilter) string {
	if strings.TrimSpace(filter.ProtoClusterID) == "" {
		return ""
	}
	return strings.TrimSpace(filter.TaskID)
}

func unifiedControlSnapshotSessionID(report UnifiedControlReport, filter UnifiedControlReportFilter) string {
	if strings.TrimSpace(filter.ProtoClusterID) == "" {
		return ""
	}
	return strings.TrimSpace(filter.SessionID)
}

func unifiedControlSnapshotSummary(report UnifiedControlReport, filter UnifiedControlReportFilter) string {
	snapshotLabel := "advisory"
	if !report.AdvisoryOnly {
		snapshotLabel = "effective"
	}
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		return fmt.Sprintf(
			"Unified control %s snapshot: %s mode=%s risk=%s actions=%d",
			snapshotLabel,
			clusterID,
			firstNonEmpty(strings.TrimSpace(report.ControlMode), "n/a"),
			firstNonEmpty(strings.TrimSpace(report.RSPRiskBand), "n/a"),
			len(report.AppliedActions),
		)
	}
	if !report.Resolved {
		return fmt.Sprintf("Unified control %s snapshot for %s unresolved", snapshotLabel, firstNonEmpty(strings.TrimSpace(report.WorkspaceID), "workspace"))
	}
	return fmt.Sprintf(
		"Unified control %s snapshot: %s mode=%s hints=%d actions=%d",
		snapshotLabel,
		firstNonEmpty(strings.TrimSpace(report.WorkspaceID), "workspace"),
		firstNonEmpty(strings.TrimSpace(report.ControlMode), "n/a"),
		len(report.GovernedHints),
		len(report.AppliedActions),
	)
}

func unifiedCandidateStreak(bundle InstrumentationLocusBundle) int {
	if bundle.ControlState == nil {
		return 0
	}
	return maxInt(bundle.ControlState.State.State.CandidateStreak, 0)
}

func buildUnifiedControlCooldownBasis(currentMode, candidateMode string, candidateStreak int, cooldownActive bool, contradictions []string, memoryNeedsAttention bool) *UnifiedControlCooldownBasis {
	currentMode = normalizeClusterControlMode(currentMode)
	candidateMode = strings.TrimSpace(candidateMode)
	if candidateMode != "" {
		candidateMode = normalizeClusterControlMode(candidateMode)
	}
	candidateStreak = maxInt(candidateStreak, 0)
	requiredStreak := maxInt(clusterControlTickHysteresisEpoch, 1)
	transitioning := candidateMode != "" && candidateMode != currentMode && candidateStreak > 0
	basis := &UnifiedControlCooldownBasis{
		CurrentMode:      currentMode,
		CandidateMode:    candidateMode,
		CandidateStreak:  candidateStreak,
		RequiredStreak:   requiredStreak,
		RemainingStreak:  0,
		ReadyToStabilize: false,
		Transitioning:    transitioning,
		CooldownActive:   cooldownActive,
	}
	if transitioning {
		if candidateStreak >= requiredStreak {
			basis.ReadyToStabilize = true
		} else {
			basis.RemainingStreak = requiredStreak - candidateStreak
		}
	}
	switch {
	case candidateMode == "":
		basis.Stage = "NO_CANDIDATE"
	case candidateMode == currentMode:
		basis.Stage = "ALIGNED"
	case candidateStreak <= 0:
		basis.Stage = "OBSERVING"
	case basis.ReadyToStabilize:
		basis.Stage = "READY_WINDOW"
	default:
		basis.Stage = "WARMING"
	}
	if cooldownActive {
		basis.BlockingReasons = appendOrderedUnique(basis.BlockingReasons, "cooldown_active")
	}
	if transitioning && candidateStreak > 0 && !basis.ReadyToStabilize {
		basis.BlockingReasons = appendOrderedUnique(basis.BlockingReasons, "streak_below_hysteresis")
	}
	if len(uniqueTrimmedLocusStrings(contradictions)) > 0 {
		basis.BlockingReasons = appendOrderedUnique(basis.BlockingReasons, "contradictions_present")
	}
	if memoryNeedsAttention {
		basis.BlockingReasons = appendOrderedUnique(basis.BlockingReasons, "memory_attention_active")
	}
	basis.BlockingReasonCount = len(basis.BlockingReasons)
	contradictionsBlocked := containsString(basis.BlockingReasons, "contradictions_present")
	memoryAttentionBlocked := containsString(basis.BlockingReasons, "memory_attention_active")
	switch {
	case candidateMode == "":
		basis.AcceptanceReadiness = "UNAVAILABLE"
	case candidateMode == currentMode:
		basis.AcceptanceReadiness = "ALIGNED"
	case contradictionsBlocked || memoryAttentionBlocked:
		basis.AcceptanceReadiness = "BLOCKED"
	case basis.ReadyToStabilize:
		basis.AcceptanceReadiness = "READY_PENDING"
	case candidateStreak <= 0:
		basis.AcceptanceReadiness = "OBSERVING"
	default:
		basis.AcceptanceReadiness = "WARMING"
	}
	switch {
	case candidateMode == "":
		basis.AcceptanceGateReason = "NO_CANDIDATE"
	case candidateMode == currentMode:
		basis.AcceptanceGateReason = "ALREADY_ALIGNED"
	case contradictionsBlocked && memoryAttentionBlocked:
		basis.AcceptanceGateReason = "CONTRADICTIONS_AND_MEMORY_ATTENTION"
	case contradictionsBlocked:
		basis.AcceptanceGateReason = "CONTRADICTIONS_PRESENT"
	case memoryAttentionBlocked:
		basis.AcceptanceGateReason = "MEMORY_ATTENTION_ACTIVE"
	case containsString(basis.BlockingReasons, "streak_below_hysteresis"):
		basis.AcceptanceGateReason = "STREAK_BELOW_HYSTERESIS"
	case candidateStreak <= 0:
		basis.AcceptanceGateReason = "OBSERVING_CANDIDATE"
	case containsString(basis.BlockingReasons, "cooldown_active"):
		basis.AcceptanceGateReason = "COOLDOWN_ACTIVE"
	case basis.ReadyToStabilize:
		basis.AcceptanceGateReason = "READY_WINDOW_OPEN"
	default:
		basis.AcceptanceGateReason = "OBSERVING_CANDIDATE"
	}
	basis.AcceptanceChecklist = &UnifiedControlAcceptanceChecklist{
		CandidatePresent:     candidateMode != "",
		CandidateDiverges:    candidateMode != "" && candidateMode != currentMode,
		HysteresisSatisfied:  candidateMode != "" && candidateMode != currentMode && basis.ReadyToStabilize,
		CooldownClear:        !cooldownActive,
		ContradictionClear:   len(uniqueTrimmedLocusStrings(contradictions)) == 0,
		MemoryAttentionClear: !memoryNeedsAttention,
	}
	basis.AcceptanceMissingRequirements = buildUnifiedControlAcceptanceMissingRequirements(basis.AcceptanceReadiness, basis.AcceptanceChecklist)
	basis.AcceptanceProgressBand = buildUnifiedControlAcceptanceProgressBand(basis.AcceptanceReadiness, basis.AcceptanceChecklist, basis.AcceptanceMissingRequirements)
	switch {
	case candidateMode == "":
		basis.Reason = "no_candidate_mode"
	case candidateMode == currentMode:
		basis.Reason = "candidate_aligned"
	case contradictionsBlocked && memoryAttentionBlocked:
		basis.Reason = "contradictions_and_memory_attention"
	case contradictionsBlocked:
		basis.Reason = "contradictions_present"
	case memoryAttentionBlocked:
		basis.Reason = "memory_attention_active"
	case basis.ReadyToStabilize && cooldownActive:
		basis.Reason = "ready_window_pending_cooldown"
	case basis.ReadyToStabilize:
		basis.Reason = "ready_window_open"
	case containsString(basis.BlockingReasons, "streak_below_hysteresis"):
		basis.Reason = "hysteresis_pending"
	case candidateStreak <= 0:
		basis.Reason = "candidate_streak_not_started"
	case cooldownActive:
		basis.Reason = "candidate_transition_pending"
	default:
		basis.Reason = "candidate_transition_observing"
	}
	basis.Summary = summarizeUnifiedControlCooldownBasis(*basis)
	return basis
}

func clampUnifiedPositiveCap(current, ceiling int) int {
	if current <= 0 {
		return ceiling
	}
	if current > ceiling {
		return ceiling
	}
	return current
}

func appendOrderedUnique(values []string, next string) []string {
	next = strings.TrimSpace(next)
	if next == "" {
		return values
	}
	for _, existing := range values {
		if existing == next {
			return values
		}
	}
	return append(values, next)
}

func buildUnifiedGovernedHintOutcomes(hints []RSPGovernedHint, applied []UnifiedControlAppliedActionAudit, suppressed []UnifiedControlSuppressedHintAudit) []UnifiedControlGovernedHintOutcome {
	if len(hints) == 0 {
		return nil
	}
	order := make([]string, 0, len(hints))
	outcomes := make(map[string]UnifiedControlGovernedHintOutcome, len(hints))
	for _, hint := range hints {
		hintID := strings.TrimSpace(hint.HintID)
		if hintID == "" {
			continue
		}
		if _, exists := outcomes[hintID]; exists {
			continue
		}
		order = append(order, hintID)
		outcomes[hintID] = UnifiedControlGovernedHintOutcome{
			HintID:              hintID,
			Type:                strings.TrimSpace(hint.Type),
			RecommendationClass: strings.TrimSpace(hint.RecommendationClass),
			Severity:            hint.Severity,
			EvidenceDiversity:   hint.EvidenceDiversity,
		}
	}
	for _, audit := range applied {
		for _, hintID := range audit.HintIDs {
			hintID = strings.TrimSpace(hintID)
			entry, exists := outcomes[hintID]
			if !exists {
				continue
			}
			entry.AppliedActions = appendOrderedUnique(entry.AppliedActions, audit.Action)
			outcomes[hintID] = entry
		}
	}
	for _, audit := range suppressed {
		hintID := strings.TrimSpace(audit.HintID)
		entry, exists := outcomes[hintID]
		if !exists {
			continue
		}
		if action := strings.TrimSpace(audit.Action); action != "" {
			entry.SuppressedActions = appendOrderedUnique(entry.SuppressedActions, action)
		}
		entry.SuppressionReasons = appendOrderedUnique(entry.SuppressionReasons, audit.Reason)
		outcomes[hintID] = entry
	}
	result := make([]UnifiedControlGovernedHintOutcome, 0, len(order))
	for _, hintID := range order {
		entry := outcomes[hintID]
		switch {
		case len(entry.AppliedActions) > 0 && len(entry.SuppressionReasons) > 0:
			entry.ArbitrationOutcome = "ADVISORY_PARTIAL"
		case len(entry.AppliedActions) > 0:
			entry.ArbitrationOutcome = "ADVISORY_ROUTED"
		case len(entry.SuppressionReasons) > 0:
			entry.ArbitrationOutcome = "ADVISORY_SUPPRESSED"
		default:
			entry.ArbitrationOutcome = "OBSERVED_ONLY"
		}
		entry.Summary = summarizeUnifiedGovernedHintOutcome(entry)
		result = append(result, entry)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Severity != result[j].Severity {
			return result[i].Severity > result[j].Severity
		}
		return result[i].HintID < result[j].HintID
	})
	return result
}

func buildUnifiedControlAuditSummary(applied []UnifiedControlAppliedActionAudit, suppressed []UnifiedControlSuppressedHintAudit) *UnifiedControlAuditSummary {
	if len(applied) == 0 && len(suppressed) == 0 {
		return nil
	}
	summary := &UnifiedControlAuditSummary{
		AppliedEntryCount:      len(applied),
		AppliedSourceKindCount: make(map[string]int),
		SuppressedEntryCount:   len(suppressed),
		SuppressionReasonCount: make(map[string]int),
	}
	for _, entry := range applied {
		if len(entry.HintIDs) > 0 {
			summary.HintBackedActionCount++
		}
		if len(entry.DeltaFields) > 0 {
			summary.DeltaBearingActionCount++
		}
		for _, sourceKind := range uniqueTrimmedLocusStrings(entry.SourceKinds) {
			summary.AppliedSourceKindCount[sourceKind]++
		}
	}
	for _, entry := range suppressed {
		if action := strings.TrimSpace(entry.Action); action != "" {
			summary.SuppressedEntriesWithActionRef++
		}
		if sourceKind := strings.TrimSpace(entry.SourceKind); sourceKind != "" {
			if summary.SuppressedSourceKindCount == nil {
				summary.SuppressedSourceKindCount = make(map[string]int)
			}
			summary.SuppressedSourceKindCount[sourceKind]++
		}
		if reason := strings.TrimSpace(entry.Reason); reason != "" {
			summary.SuppressionReasonCount[reason]++
		}
	}
	if len(summary.AppliedSourceKindCount) == 0 {
		summary.AppliedSourceKindCount = nil
	}
	if len(summary.SuppressedSourceKindCount) == 0 {
		summary.SuppressedSourceKindCount = nil
	}
	if len(summary.SuppressionReasonCount) == 0 {
		summary.SuppressionReasonCount = nil
	}
	return summary
}

func buildUnifiedControlAuditCoverage(applied []UnifiedControlAppliedActionAudit, suppressed []UnifiedControlSuppressedHintAudit) *UnifiedControlAuditCoverage {
	if len(applied) == 0 && len(suppressed) == 0 {
		return nil
	}
	coverage := &UnifiedControlAuditCoverage{}
	for _, entry := range applied {
		hasSourceKinds := len(uniqueTrimmedLocusStrings(entry.SourceKinds)) > 0
		hasHintRefs := len(uniqueTrimmedLocusStrings(entry.HintIDs)) > 0
		hasDeltaFields := len(uniqueTrimmedLocusStrings(entry.DeltaFields)) > 0
		hasSummary := strings.TrimSpace(entry.Summary) != ""
		if hasSourceKinds {
			coverage.AppliedEntriesWithSourceKinds++
		}
		if hasHintRefs {
			coverage.AppliedEntriesWithHintRefs++
		}
		if hasDeltaFields {
			coverage.AppliedEntriesWithDeltaFields++
		}
		if hasSummary {
			coverage.AppliedEntriesWithSummary++
		}
		if hasSourceKinds && hasHintRefs && hasDeltaFields && hasSummary {
			coverage.FullAppliedTraceEntryCount++
		}
	}
	for _, entry := range suppressed {
		hasSourceKind := strings.TrimSpace(entry.SourceKind) != ""
		hasActionRef := strings.TrimSpace(entry.Action) != ""
		hasReason := strings.TrimSpace(entry.Reason) != ""
		hasSummary := strings.TrimSpace(entry.Summary) != ""
		if hasSourceKind {
			coverage.SuppressedEntriesWithSourceKind++
		}
		if hasActionRef {
			coverage.SuppressedEntriesWithActionRef++
		}
		if hasReason {
			coverage.SuppressedEntriesWithReason++
		}
		if hasSummary {
			coverage.SuppressedEntriesWithSummary++
		}
		if hasSourceKind && hasActionRef && hasReason && hasSummary {
			coverage.FullSuppressedTraceEntryCount++
		}
	}
	return coverage
}

func buildUnifiedControlEffectiveControlBasis(suggested, effective ControlSuggestedControls, applied []UnifiedControlAppliedActionAudit) []UnifiedControlEffectiveControlBasis {
	fields := []string{
		"priority_focus",
		"fanout_cap",
		"review_depth",
		"context_cap",
		"bridge_quota",
		"merge_threshold",
	}
	entries := make([]UnifiedControlEffectiveControlBasis, 0, len(fields))
	for _, field := range fields {
		entry := UnifiedControlEffectiveControlBasis{
			Field:          field,
			SuggestedValue: unifiedControlBasisValueString(suggested, field),
			EffectiveValue: unifiedControlBasisValueString(effective, field),
		}
		entry.Changed = entry.SuggestedValue != entry.EffectiveValue
		for _, audit := range applied {
			if !containsString(audit.DeltaFields, field) {
				continue
			}
			entry.AppliedActions = appendOrderedUnique(entry.AppliedActions, audit.Action)
			entry.SourceKinds = appendOrderedUniqueAll(entry.SourceKinds, audit.SourceKinds)
			entry.HintIDs = appendOrderedUniqueAll(entry.HintIDs, audit.HintIDs)
		}
		entry.Summary = summarizeUnifiedControlEffectiveControlBasis(entry)
		entries = append(entries, entry)
	}
	return entries
}

func buildUnifiedControlEffectiveControlBasisSummary(entries []UnifiedControlEffectiveControlBasis) *UnifiedControlEffectiveControlBasisSummary {
	if len(entries) == 0 {
		return nil
	}
	summary := &UnifiedControlEffectiveControlBasisSummary{
		FieldCount: len(entries),
	}
	for _, entry := range entries {
		if entry.Changed {
			summary.ChangedFieldCount++
		}
		if len(entry.AppliedActions) > 0 {
			summary.FieldsWithActionTraceCount++
		}
		if len(entry.HintIDs) > 0 {
			summary.FieldsWithHintTraceCount++
		}
		if len(uniqueTrimmedLocusStrings(entry.SourceKinds)) > 1 {
			summary.FieldsWithMultiSourceCount++
		}
	}
	return summary
}

func unifiedControlBasisSummaryCount(summary *UnifiedControlEffectiveControlBasisSummary, selector func(UnifiedControlEffectiveControlBasisSummary) int) int {
	if summary == nil {
		return 0
	}
	return selector(*summary)
}

func buildUnifiedControlContradictionSummary(items []string) *UnifiedControlContradictionSummary {
	contradictions := uniqueTrimmedLocusStrings(items)
	if len(contradictions) == 0 {
		return nil
	}
	summary := &UnifiedControlContradictionSummary{
		TotalCount:  len(contradictions),
		FamilyCount: make(map[string]int),
	}
	for _, item := range contradictions {
		family := unifiedControlContradictionFamily(item)
		summary.FamilyCount[family]++
		switch family {
		case "hard_safety_clamp":
			summary.HardSafetyClampCount++
		case "memory_safety_override":
			summary.MemorySafetyOverrideCount++
		default:
			summary.OtherCount++
		}
	}
	if len(summary.FamilyCount) == 0 {
		summary.FamilyCount = nil
	}
	return summary
}

func unifiedControlContradictionFamily(item string) string {
	switch strings.TrimSpace(item) {
	case "hard_safety_conditions_clamped_rsp_advice":
		return "hard_safety_clamp"
	case "coherence_floor_overrides_synergy_seeking", "freshness_safety_overrides_non_memory_focus":
		return "memory_safety_override"
	default:
		return "other"
	}
}

func unifiedControlContradictionSummaryCount(summary *UnifiedControlContradictionSummary, selector func(UnifiedControlContradictionSummary) int) int {
	if summary == nil {
		return 0
	}
	return selector(*summary)
}

func unifiedControlAuditSummaryCount(summary *UnifiedControlAuditSummary, selector func(UnifiedControlAuditSummary) int) int {
	if summary == nil {
		return 0
	}
	return selector(*summary)
}

func unifiedControlAuditCoverageCount(coverage *UnifiedControlAuditCoverage, selector func(UnifiedControlAuditCoverage) int) int {
	if coverage == nil {
		return 0
	}
	return selector(*coverage)
}

func summarizeUnifiedControlCooldownBasis(basis UnifiedControlCooldownBasis) string {
	checklistLabel := "acceptance checklist"
	switch strings.TrimSpace(basis.AcceptanceReadiness) {
	case "BLOCKED":
		checklistLabel = "active blocker checklist"
	case "OBSERVING":
		checklistLabel = "observing-deferred checklist"
	case "READY_PENDING":
		checklistLabel = "ready-window checklist"
	}
	acceptanceProgress := firstNonEmpty(strings.TrimSpace(basis.AcceptanceProgressBand), "n/a")
	acceptanceProgress = firstNonEmpty(unifiedControlAcceptanceProgressLabel(acceptanceProgress), "n/a")
	reasonLabel := unifiedControlCooldownReasonLabel(basis.Reason)
	acceptanceGateLabel := unifiedControlAcceptanceGateLabel(basis.AcceptanceGateReason)
	stageLabel := unifiedControlStageLabel(basis.Stage)
	acceptanceReadinessLabel := unifiedControlAcceptanceReadinessLabel(basis.AcceptanceReadiness)
	currentModeLabel := unifiedControlModeLabel(basis.CurrentMode)
	candidateModeLabel := unifiedControlModeLabel(basis.CandidateMode)
	parts := []string{
		"current " + firstNonEmpty(currentModeLabel, "n/a"),
		"candidate " + firstNonEmpty(candidateModeLabel, "n/a"),
		"stage " + firstNonEmpty(stageLabel, "n/a"),
		"acceptance readiness " + firstNonEmpty(acceptanceReadinessLabel, "n/a"),
		"acceptance gate " + firstNonEmpty(acceptanceGateLabel, "n/a"),
		"acceptance progress " + acceptanceProgress,
		fmt.Sprintf("%s %d/%d", checklistLabel, unifiedControlAcceptanceChecklistClearCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist), unifiedControlAcceptanceChecklistRequirementCount(basis.AcceptanceReadiness, basis.AcceptanceChecklist)),
		fmt.Sprintf("candidate streak %d", basis.CandidateStreak),
		fmt.Sprintf("required streak %d", basis.RequiredStreak),
		fmt.Sprintf("remaining streak %d", basis.RemainingStreak),
	}
	if basis.ReadyToStabilize {
		parts = append(parts, "ready to stabilize")
	} else {
		parts = append(parts, "not ready to stabilize")
	}
	if basis.CooldownActive {
		parts = append(parts, "cooldown active")
	} else {
		parts = append(parts, "cooldown inactive")
	}
	if basis.Transitioning {
		parts = append(parts, "transitioning")
	} else {
		parts = append(parts, "not transitioning")
	}
	if len(basis.BlockingReasons) > 0 {
		parts = append(parts, "blocking "+strings.Join(unifiedControlBlockingReasonLabels(basis.BlockingReasons), ", "))
	} else {
		parts = append(parts, "no blocking reasons")
	}
	if len(basis.AcceptanceMissingRequirements) > 0 {
		parts = append(parts, "missing requirements "+strings.Join(unifiedControlAcceptanceRequirementLabels(basis.AcceptanceReadiness, basis.AcceptanceMissingRequirements), ", "))
	} else if unifiedControlAcceptanceChecklistNotApplicable(basis.AcceptanceReadiness) {
		parts = append(parts, "acceptance path not active")
	} else if strings.TrimSpace(basis.AcceptanceReadiness) == "READY_PENDING" && strings.TrimSpace(basis.Reason) == "ready_window_open" {
		parts = append(parts, "ready window clear")
	} else if strings.TrimSpace(basis.AcceptanceReadiness) == "OBSERVING" && strings.TrimSpace(basis.Reason) == "candidate_streak_not_started" {
		parts = append(parts, "observing candidate not started")
	} else {
		parts = append(parts, "no missing acceptance requirements")
	}
	if reasonLabel != "" {
		parts = append(parts, "reason "+reasonLabel)
	}
	return strings.Join(parts, " | ")
}

func unifiedControlAcceptanceProgressLabel(progress string) string {
	switch strings.TrimSpace(progress) {
	case "NONE":
		return "none"
	case "ALIGNED":
		return "aligned"
	case "BLOCKED":
		return "blocked"
	case "EARLY":
		return "early"
	case "PARTIAL":
		return "partial"
	case "NEARLY_READY":
		return "nearly ready"
	case "READY_WINDOW_PENDING":
		return "ready window pending"
	case "FULLY_CLEAR":
		return "ready window clear"
	default:
		return strings.TrimSpace(progress)
	}
}

func unifiedControlModeLabel(mode string) string {
	switch strings.TrimSpace(mode) {
	case clusterControlModeSteady:
		return "steady"
	case clusterControlModeAntiCollapse:
		return "anti-collapse"
	case clusterControlModeCoherence:
		return "coherence"
	case clusterControlModeDecentralize:
		return "decentralize"
	case clusterControlModeSynergySeeking:
		return "synergy seeking"
	case clusterControlModeUnfreeze:
		return "unfreeze"
	case clusterControlModeStabilize:
		return "stabilize"
	default:
		return strings.TrimSpace(mode)
	}
}

func unifiedControlStageLabel(stage string) string {
	switch strings.TrimSpace(stage) {
	case "NO_CANDIDATE":
		return "no candidate"
	case "ALIGNED":
		return "aligned"
	case "OBSERVING":
		return "observing"
	case "WARMING":
		return "warming"
	case "READY_WINDOW":
		return "ready window"
	default:
		return strings.TrimSpace(stage)
	}
}

func unifiedControlAcceptanceReadinessLabel(readiness string) string {
	switch strings.TrimSpace(readiness) {
	case "UNAVAILABLE":
		return "unavailable"
	case "ALIGNED":
		return "aligned"
	case "BLOCKED":
		return "blocked"
	case "OBSERVING":
		return "observing"
	case "WARMING":
		return "warming"
	case "READY_PENDING":
		return "ready pending"
	default:
		return strings.TrimSpace(readiness)
	}
}

func unifiedControlBlockingReasonLabels(reasons []string) []string {
	labels := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		label := unifiedControlBlockingReasonLabel(reason)
		if label == "" {
			continue
		}
		labels = append(labels, label)
	}
	return labels
}

func unifiedControlBlockingReasonLabel(reason string) string {
	switch strings.TrimSpace(reason) {
	case "cooldown_active":
		return "cooldown active"
	case "streak_below_hysteresis":
		return "streak below hysteresis"
	case "contradictions_present":
		return "contradictions present"
	case "memory_attention_active":
		return "memory attention active"
	default:
		return strings.TrimSpace(reason)
	}
}

func unifiedControlAcceptanceRequirementLabels(readiness string, requirements []string) []string {
	labels := make([]string, 0, len(requirements))
	for _, requirement := range requirements {
		label := unifiedControlAcceptanceRequirementLabel(readiness, requirement)
		if label == "" {
			continue
		}
		labels = append(labels, label)
	}
	return labels
}

func unifiedControlAcceptanceRequirementLabel(readiness, requirement string) string {
	switch strings.TrimSpace(requirement) {
	case "candidate_present":
		return "candidate present"
	case "candidate_diverges":
		return "candidate diverges"
	case "hysteresis_satisfied":
		return "hysteresis satisfied"
	case "cooldown_clear":
		if strings.TrimSpace(readiness) == "READY_PENDING" {
			return "ready window cooldown clear"
		}
		return "cooldown clear"
	case "contradiction_clear":
		return "contradiction clear"
	case "memory_attention_clear":
		return "memory attention clear"
	default:
		return strings.TrimSpace(requirement)
	}
}

func unifiedControlAcceptanceGateLabel(reason string) string {
	switch strings.TrimSpace(reason) {
	case "NO_CANDIDATE":
		return "no candidate"
	case "ALREADY_ALIGNED":
		return "already aligned"
	case "CONTRADICTIONS_AND_MEMORY_ATTENTION":
		return "contradictions and memory attention"
	case "CONTRADICTIONS_PRESENT":
		return "contradictions present"
	case "COOLDOWN_ACTIVE":
		return "cooldown active"
	case "MEMORY_ATTENTION_ACTIVE":
		return "memory attention active"
	case "OBSERVING_CANDIDATE":
		return "observing candidate"
	case "READY_WINDOW_OPEN":
		return "ready window open"
	case "STREAK_BELOW_HYSTERESIS":
		return "streak below hysteresis"
	default:
		return strings.TrimSpace(reason)
	}
}

func unifiedControlCooldownReasonLabel(reason string) string {
	switch strings.TrimSpace(reason) {
	case "no_candidate_mode":
		return "no candidate mode"
	case "candidate_aligned":
		return "candidate aligned"
	case "contradictions_and_memory_attention":
		return "contradictions and memory attention"
	case "contradictions_present":
		return "contradictions present"
	case "memory_attention_active":
		return "memory attention active"
	case "ready_window_pending_cooldown":
		return "ready window pending cooldown"
	case "ready_window_open":
		return "ready window open"
	case "hysteresis_pending":
		return "hysteresis pending"
	case "candidate_streak_not_started":
		return "candidate streak not started"
	case "candidate_transition_pending":
		return "candidate transition pending"
	case "candidate_transition_observing":
		return "candidate transition observing"
	default:
		return strings.TrimSpace(reason)
	}
}

func buildUnifiedControlAcceptanceMissingRequirements(readiness string, item *UnifiedControlAcceptanceChecklist) []string {
	if unifiedControlAcceptanceChecklistNotApplicable(readiness) {
		return nil
	}
	if item == nil {
		return nil
	}
	switch strings.TrimSpace(readiness) {
	case "BLOCKED":
		blocking := make([]string, 0, 2)
		if !item.ContradictionClear {
			blocking = append(blocking, "contradiction_clear")
		}
		if !item.MemoryAttentionClear {
			blocking = append(blocking, "memory_attention_clear")
		}
		if len(blocking) == 0 {
			return nil
		}
		return blocking
	case "OBSERVING":
		return nil
	case "WARMING":
		missing := make([]string, 0, 5)
		if !item.CandidatePresent {
			missing = append(missing, "candidate_present")
		} else if !item.CandidateDiverges {
			missing = append(missing, "candidate_diverges")
		}
		if item.CandidatePresent && item.CandidateDiverges && !item.HysteresisSatisfied {
			missing = append(missing, "hysteresis_satisfied")
		}
		if !item.ContradictionClear {
			missing = append(missing, "contradiction_clear")
		}
		if !item.MemoryAttentionClear {
			missing = append(missing, "memory_attention_clear")
		}
		if len(missing) == 0 {
			return nil
		}
		return missing
	}
	missing := make([]string, 0, 6)
	if !item.CandidatePresent {
		missing = append(missing, "candidate_present")
	} else if !item.CandidateDiverges {
		missing = append(missing, "candidate_diverges")
	}
	if item.CandidatePresent && item.CandidateDiverges && !item.HysteresisSatisfied {
		missing = append(missing, "hysteresis_satisfied")
	}
	if !item.CooldownClear {
		missing = append(missing, "cooldown_clear")
	}
	if !item.ContradictionClear {
		missing = append(missing, "contradiction_clear")
	}
	if !item.MemoryAttentionClear {
		missing = append(missing, "memory_attention_clear")
	}
	if len(missing) == 0 {
		return nil
	}
	return missing
}

func buildUnifiedControlAcceptanceProgressBand(readiness string, checklist *UnifiedControlAcceptanceChecklist, missing []string) string {
	switch strings.TrimSpace(readiness) {
	case "UNAVAILABLE":
		return "NONE"
	case "ALIGNED":
		return "ALIGNED"
	case "BLOCKED":
		return "BLOCKED"
	case "OBSERVING":
		return "EARLY"
	}
	clearCount := unifiedControlAcceptanceChecklistClearCount(readiness, checklist)
	switch {
	case strings.TrimSpace(readiness) == "READY_PENDING" && len(missing) == 0:
		return "FULLY_CLEAR"
	case strings.TrimSpace(readiness) == "READY_PENDING" && containsString(missing, "cooldown_clear"):
		return "READY_WINDOW_PENDING"
	case containsString(missing, "hysteresis_satisfied"):
		return "PARTIAL"
	case clearCount >= 5:
		return "NEARLY_READY"
	case clearCount >= 3:
		return "PARTIAL"
	default:
		return "EARLY"
	}
}

func unifiedControlActiveBlockedRequirementCount(item *UnifiedControlAcceptanceChecklist) int {
	if item == nil {
		return 0
	}
	count := 0
	if !item.ContradictionClear {
		count++
	}
	if !item.MemoryAttentionClear {
		count++
	}
	return count
}

func unifiedControlAcceptanceChecklistClearCount(readiness string, item *UnifiedControlAcceptanceChecklist) int {
	if unifiedControlAcceptanceChecklistNotApplicable(readiness) {
		return 0
	}
	if item == nil {
		return 0
	}
	switch strings.TrimSpace(readiness) {
	case "READY_PENDING":
		if item.CooldownClear {
			return 1
		}
		return 0
	case "BLOCKED":
		return 0
	case "OBSERVING":
		return 0
	case "WARMING":
		count := 0
		if item.CandidatePresent {
			count++
		}
		if item.CandidateDiverges {
			count++
		}
		if item.HysteresisSatisfied {
			count++
		}
		if item.ContradictionClear {
			count++
		}
		if item.MemoryAttentionClear {
			count++
		}
		return count
	}
	count := 0
	if item.CandidatePresent {
		count++
	}
	if item.CandidateDiverges {
		count++
	}
	if item.HysteresisSatisfied {
		count++
	}
	if item.CooldownClear {
		count++
	}
	if item.ContradictionClear {
		count++
	}
	if item.MemoryAttentionClear {
		count++
	}
	return count
}

func unifiedControlAcceptanceChecklistRequirementCount(readiness string, item *UnifiedControlAcceptanceChecklist) int {
	if unifiedControlAcceptanceChecklistNotApplicable(readiness) || item == nil {
		return 0
	}
	switch strings.TrimSpace(readiness) {
	case "READY_PENDING":
		return 1
	case "BLOCKED":
		return unifiedControlActiveBlockedRequirementCount(item)
	case "OBSERVING":
		return 0
	case "WARMING":
		return 5
	}
	return 6
}

func unifiedControlAcceptanceChecklistNotApplicable(readiness string) bool {
	switch strings.TrimSpace(readiness) {
	case "UNAVAILABLE", "ALIGNED":
		return true
	default:
		return false
	}
}

func unifiedControlBasisValueString(controls ControlSuggestedControls, field string) string {
	switch field {
	case "priority_focus":
		return firstNonEmpty(strings.TrimSpace(controls.PriorityFocus), "throughput")
	case "fanout_cap":
		return fmt.Sprintf("%d", controls.FanoutCap)
	case "review_depth":
		return fmt.Sprintf("%d", controls.ReviewDepth)
	case "context_cap":
		return fmt.Sprintf("%d", controls.ContextCap)
	case "bridge_quota":
		return fmt.Sprintf("%d", controls.BridgeQuota)
	case "merge_threshold":
		return fmt.Sprintf("%g", controls.MergeThreshold)
	default:
		return ""
	}
}

func unifiedControlBasisFieldLabel(field string) string {
	switch strings.TrimSpace(field) {
	case "priority_focus":
		return "priority focus"
	case "fanout_cap":
		return "fanout cap"
	case "review_depth":
		return "review depth"
	case "context_cap":
		return "context cap"
	case "bridge_quota":
		return "bridge quota"
	case "merge_threshold":
		return "merge threshold"
	default:
		return strings.TrimSpace(field)
	}
}

func summarizeUnifiedControlEffectiveControlBasis(entry UnifiedControlEffectiveControlBasis) string {
	parts := []string{unifiedControlBasisFieldLabel(entry.Field) + "=" + firstNonEmpty(strings.TrimSpace(entry.EffectiveValue), "n/a")}
	if entry.Changed {
		parts = append(parts, "suggested "+firstNonEmpty(strings.TrimSpace(entry.SuggestedValue), "n/a"))
	} else {
		parts = append(parts, "inherits suggested control")
	}
	if len(entry.AppliedActions) > 0 {
		parts = append(parts, "actions "+strings.Join(entry.AppliedActions, ", "))
	} else if entry.Changed {
		parts = append(parts, "no current delta-bearing action trace")
	}
	if len(entry.SourceKinds) > 0 {
		parts = append(parts, "sources "+strings.Join(entry.SourceKinds, ", "))
	}
	if len(entry.HintIDs) > 0 {
		parts = append(parts, "hints "+strings.Join(entry.HintIDs, ", "))
	}
	return strings.Join(parts, " | ")
}

func summarizeUnifiedGovernedHintOutcome(entry UnifiedControlGovernedHintOutcome) string {
	parts := []string{entry.HintID, firstNonEmpty(strings.TrimSpace(entry.ArbitrationOutcome), "OBSERVED_ONLY")}
	if entry.RecommendationClass != "" {
		parts = append(parts, "class "+entry.RecommendationClass)
	}
	if len(entry.AppliedActions) > 0 {
		parts = append(parts, "applied "+strings.Join(entry.AppliedActions, ", "))
	}
	if len(entry.SuppressionReasons) > 0 {
		parts = append(parts, "suppressed "+strings.Join(entry.SuppressionReasons, ", "))
	}
	return strings.Join(parts, " | ")
}
