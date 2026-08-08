package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	rspCapabilityBeliefLive             = "rsp.belief.live"
	rspCapabilityAnomalyShadow          = "rsp.anomaly.shadow"
	rspCapabilityStateShadow            = "rsp.state.shadow"
	rspCapabilityForecastShadow         = "rsp.forecast.shadow"
	rspCapabilitySafeLocalAutonomics    = "rsp.safe_local_autonomics.live"
	rspCapabilityGovernedHintsLive      = "rsp.governed_hints.live"
	rspCapabilityStrongConsequencesLive = "rsp.strong_consequences.live"
)

type RSPCapabilityFlags struct {
	BeliefLive              bool `json:"belief_live"`
	AnomalyShadow           bool `json:"anomaly_shadow"`
	StateShadow             bool `json:"state_shadow"`
	ForecastShadow          bool `json:"forecast_shadow"`
	SafeLocalAutonomicsLive bool `json:"safe_local_autonomics_live"`
	GovernedHintsLive       bool `json:"governed_hints_live"`
	StrongConsequencesLive  bool `json:"strong_consequences_live"`
}

type RSPGovernedHint struct {
	HintID                string   `json:"hint_id"`
	Type                  string   `json:"type"`
	Scope                 string   `json:"scope"`
	EntityID              string   `json:"entity_id"`
	Severity              float64  `json:"severity"`
	Uncertainty           float64  `json:"uncertainty"`
	PersistenceEpochs     int      `json:"persistence_epochs"`
	EvidenceDiversity     float64  `json:"evidence_diversity"`
	EvidenceDiversityBand string   `json:"evidence_diversity_band,omitempty"`
	EvidenceSourceMix     string   `json:"evidence_source_mix,omitempty"`
	RuntimeEventRefCount  int      `json:"runtime_event_ref_count,omitempty"`
	EvidenceRefs          []string `json:"evidence_refs,omitempty"`
	EvidenceSourceKinds   []string `json:"evidence_source_kinds,omitempty"`
	RootCauseGroups       []string `json:"root_cause_groups,omitempty"`
	RuntimeLineageBasis   string   `json:"runtime_lineage_basis,omitempty"`
	TTLWindowState        string   `json:"ttl_window_state,omitempty"`
	RecommendedActions    []string `json:"recommended_actions,omitempty"`
	RecommendationClass   string   `json:"recommendation_class,omitempty"`
	ActuationClass        string   `json:"actuation_class"`
	TTLEpochs             int      `json:"ttl_epochs"`
	Summary               string   `json:"summary,omitempty"`
}

type SetRSPCapabilityFlagsInput struct {
	WorkspaceID             string
	BeliefLive              *bool
	AnomalyShadow           *bool
	StateShadow             *bool
	ForecastShadow          *bool
	SafeLocalAutonomicsLive *bool
	GovernedHintsLive       *bool
	StrongConsequencesLive  *bool
	UpdatedBy               string
	Reason                  string
	PromptContextEnvelope   map[string]any
	PromptContextSurface    string
}

func rspLegacyLiveActuationEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_RSP_LIVE_ACTUATION"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func defaultRSPCapabilityFlags() RSPCapabilityFlags {
	legacyLive := rspLegacyLiveActuationEnabled()
	return RSPCapabilityFlags{
		BeliefLive:              false,
		AnomalyShadow:           true,
		StateShadow:             true,
		ForecastShadow:          false,
		SafeLocalAutonomicsLive: legacyLive,
		GovernedHintsLive:       false,
		StrongConsequencesLive:  false,
	}
}

func (s *Store) GetRSPCapabilityFlags(ctx context.Context, workspaceID string) RSPCapabilityFlags {
	flags := defaultRSPCapabilityFlags()
	workspaceID = strings.TrimSpace(workspaceID)
	if s == nil || workspaceID == "" {
		return flags
	}
	flags.BeliefLive = s.rspCapabilityEnabled(ctx, workspaceID, rspCapabilityBeliefLive, flags.BeliefLive)
	flags.AnomalyShadow = s.rspCapabilityEnabled(ctx, workspaceID, rspCapabilityAnomalyShadow, flags.AnomalyShadow)
	flags.StateShadow = s.rspCapabilityEnabled(ctx, workspaceID, rspCapabilityStateShadow, flags.StateShadow)
	flags.ForecastShadow = s.rspCapabilityEnabled(ctx, workspaceID, rspCapabilityForecastShadow, flags.ForecastShadow)
	flags.SafeLocalAutonomicsLive = s.rspCapabilityEnabled(ctx, workspaceID, rspCapabilitySafeLocalAutonomics, flags.SafeLocalAutonomicsLive)
	flags.GovernedHintsLive = s.rspCapabilityEnabled(ctx, workspaceID, rspCapabilityGovernedHintsLive, flags.GovernedHintsLive)
	flags.StrongConsequencesLive = s.rspCapabilityEnabled(ctx, workspaceID, rspCapabilityStrongConsequencesLive, flags.StrongConsequencesLive)
	return flags
}

func (s *Store) rspCapabilityEnabled(ctx context.Context, workspaceID, capability string, fallback bool) bool {
	result, err := s.CheckCapabilityPolicy(ctx, CapabilityCheckInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  capability,
		ToolID:      "*",
	})
	if err != nil || result.MatchedPolicy == nil {
		return fallback
	}
	return strings.EqualFold(strings.TrimSpace(result.Verdict), "ALLOW")
}

func (s *Store) ensureRSPCapabilityEnabled(ctx context.Context, workspaceID, capability string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return fmt.Errorf("workspace_id is required")
	}
	flags := s.GetRSPCapabilityFlags(ctx, workspaceID)
	enabled := false
	switch capability {
	case rspCapabilityBeliefLive:
		enabled = flags.BeliefLive
	case rspCapabilityAnomalyShadow:
		enabled = flags.AnomalyShadow
	case rspCapabilityStateShadow:
		enabled = flags.StateShadow
	case rspCapabilityForecastShadow:
		enabled = flags.ForecastShadow
	case rspCapabilitySafeLocalAutonomics:
		enabled = flags.SafeLocalAutonomicsLive
	case rspCapabilityGovernedHintsLive:
		enabled = flags.GovernedHintsLive
	case rspCapabilityStrongConsequencesLive:
		enabled = flags.StrongConsequencesLive
	default:
		return fmt.Errorf("unknown rsp capability %q", capability)
	}
	if enabled {
		return nil
	}
	return fmt.Errorf("%s disabled by rollout policy", capability)
}

func IsRSPRolloutDisabledError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(err.Error())), "disabled by rollout policy")
}

func (s *Store) SetRSPCapabilityFlags(ctx context.Context, input SetRSPCapabilityFlagsInput) (RSPCapabilityFlags, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RSPCapabilityFlags{}, fmt.Errorf("workspace_id is required")
	}
	updatedBy := strings.TrimSpace(input.UpdatedBy)
	if updatedBy == "" {
		return RSPCapabilityFlags{}, fmt.Errorf("updated_by is required")
	}
	updates := []CapabilityPolicyRecord{}
	for _, update := range []struct {
		capability string
		value      *bool
	}{
		{capability: rspCapabilityBeliefLive, value: input.BeliefLive},
		{capability: rspCapabilityAnomalyShadow, value: input.AnomalyShadow},
		{capability: rspCapabilityStateShadow, value: input.StateShadow},
		{capability: rspCapabilityForecastShadow, value: input.ForecastShadow},
		{capability: rspCapabilitySafeLocalAutonomics, value: input.SafeLocalAutonomicsLive},
		{capability: rspCapabilityGovernedHintsLive, value: input.GovernedHintsLive},
		{capability: rspCapabilityStrongConsequencesLive, value: input.StrongConsequencesLive},
	} {
		if update.value == nil {
			continue
		}
		effect := "DENY"
		if *update.value {
			effect = "ALLOW"
		}
		record, err := normalizeCapabilityPolicyInput(CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			Capability:  update.capability,
			ToolID:      "*",
			Effect:      effect,
			Reason:      strings.TrimSpace(input.Reason),
			CreatedBy:   updatedBy,
		})
		if err != nil {
			return RSPCapabilityFlags{}, err
		}
		updates = append(updates, record)
	}
	if len(updates) == 0 {
		return s.GetRSPCapabilityFlags(ctx, workspaceID), nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RSPCapabilityFlags{}, err
	}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		for _, record := range updates {
			if _, _, err := s.putCapabilityPolicyTx(ctx, tx, authority, record, now, input.PromptContextEnvelope, rspCapabilityPromptContextSurface(input.PromptContextSurface)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return RSPCapabilityFlags{}, err
	}
	return s.GetRSPCapabilityFlags(ctx, workspaceID), nil
}

func rspCapabilityPromptContextSurface(surface string) string {
	if surface = strings.TrimSpace(surface); surface != "" {
		return surface
	}
	return "workspace.rsp.capability.put"
}
