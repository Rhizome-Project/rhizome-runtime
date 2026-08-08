package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

const defaultShadowMonitorInterval = 3 * time.Minute

type ShadowIntervention struct {
	Level  string `json:"level"` // "NUDGE", "DEMAND_SUMMARIZE", "FORCE_REPLAN", "PAUSE"
	Reason string `json:"reason"`
	Action string `json:"action"`
}

type ShadowMonitor struct {
	cfg                      RuntimeConfig
	client                   *RhizomeClient
	pokeChan                 chan struct{}
	consecutiveInterventions int
}

func NewShadowMonitor(cfg RuntimeConfig, llm ChatLLM) *ShadowMonitor {
	cfg.ApplyDefaults()
	_ = llm
	return &ShadowMonitor{
		cfg:      cfg,
		client:   NewRhizomeClient(cfg.RhizomeRPC, cfg.RhizomeToken),
		pokeChan: make(chan struct{}, 1),
	}
}

func (s *ShadowMonitor) Poke() {
	if s == nil {
		return
	}
	select {
	case s.pokeChan <- struct{}{}:
	default:
	}
}

func (s *ShadowMonitor) Run(ctx context.Context, getSnapshot func() RuntimeWatchdogSnapshot) error {
	if s == nil {
		return nil
	}
	log.Printf("[shadow_monitor] started phase=%s agent=%s", normalizeRSPRolloutPhase(s.cfg.RSPRolloutPhase), strings.TrimSpace(s.cfg.AgentID))

	ticker := time.NewTicker(defaultShadowMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-s.pokeChan:
		}

		if getSnapshot == nil {
			continue
		}
		snapshot := getSnapshot()
		if err := s.handleSnapshot(ctx, snapshot, time.Now().UTC()); err != nil && ctx.Err() == nil {
			log.Printf("[shadow_monitor] snapshot handling failed: %v", err)
		}
	}
}

func (s *ShadowMonitor) handleSnapshot(ctx context.Context, snapshot RuntimeWatchdogSnapshot, now time.Time) error {
	if s == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	rsp := snapshot.RSP
	if rsp.RolloutPhase == "" {
		rsp = deriveRuntimeRSPState(s.cfg, snapshot, now)
	}

	if err := s.recordAdvisorySignal(ctx, rsp.advisorySignal()); err != nil {
		return err
	}

	intervention := rsp.intervention()
	if intervention == nil || rsp.RolloutPhase == RSPRolloutObserveOnly {
		s.consecutiveInterventions = 0
		return nil
	}

	s.consecutiveInterventions++
	if err := s.postIntervention(ctx, snapshot, rsp, *intervention, now); err != nil {
		return err
	}
	if s.consecutiveInterventions >= 3 && rsp.RolloutPhase != RSPRolloutAdvisory {
		return s.escalateIntervention(ctx, snapshot, rsp, *intervention, now)
	}
	return nil
}

func (s *ShadowMonitor) recordAdvisorySignal(ctx context.Context, signal string) error {
	if s == nil || strings.TrimSpace(signal) == "" || s.client == nil {
		return nil
	}
	s.refreshClientFromLocalRuntimeProfile()
	if strings.TrimSpace(s.cfg.RhizomeToken) == "" {
		return nil
	}

	state, err := loadScratchState(ctx, s.client, s.cfg.WorkspaceID, s.cfg.AgentID)
	if err != nil {
		return err
	}

	signal = strings.TrimSpace(signal)
	if len(state.AdvisorySignals) == 0 || state.AdvisorySignals[len(state.AdvisorySignals)-1] != signal {
		state.AdvisorySignals = append(state.AdvisorySignals, signal)
	}
	if len(state.AdvisorySignals) > 5 {
		state.AdvisorySignals = state.AdvisorySignals[len(state.AdvisorySignals)-5:]
	}

	return saveScratchState(ctx, s.client, s.cfg.WorkspaceID, s.cfg.AgentID, state)
}

func (s *ShadowMonitor) refreshClientFromLocalRuntimeProfile() {
	if s == nil || s.client == nil || strings.TrimSpace(s.cfg.RhizomeToken) != "" {
		return
	}
	profile := LoadLocalRuntimeProfile(s.cfg.Workdir)
	token := strings.TrimSpace(profile.AgentToken)
	if token == "" {
		return
	}
	s.cfg.RhizomeToken = token
	s.client.SetToken(token)
	if endpoint := strings.TrimSpace(profile.RPCEndpoint); endpoint != "" {
		s.cfg.RhizomeRPC = endpoint
		s.client.SetEndpoint(endpoint)
	}
}

func (s *ShadowMonitor) postIntervention(ctx context.Context, snapshot RuntimeWatchdogSnapshot, rsp RuntimeRSPState, intervention ShadowIntervention, now time.Time) error {
	if s == nil {
		return nil
	}

	signal := strings.TrimSpace(interventionSignal(rsp, intervention, now))
	if signal != "" {
		if err := s.recordAdvisorySignal(ctx, signal); err != nil {
			return err
		}
	}
	if s.client == nil {
		return nil
	}

	payload := map[string]any{
		"rsp":      rsp,
		"snapshot": snapshot,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode shadow intervention payload: %w", err)
	}

	return s.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   s.cfg.WorkspaceID,
		AgentID:       s.cfg.AgentID,
		UpdateType:    "watchdog_intervention",
		Summary:       fmt.Sprintf("Shadow Monitor Intervention (%s)", intervention.Level),
		PayloadJSON:   string(raw),
		RequiresHuman: intervention.Level == "PAUSE",
	})
}

func (s *ShadowMonitor) escalateIntervention(ctx context.Context, snapshot RuntimeWatchdogSnapshot, rsp RuntimeRSPState, intervention ShadowIntervention, now time.Time) error {
	if s == nil || s.client == nil {
		return nil
	}

	payload := map[string]any{
		"rsp":                       rsp,
		"snapshot":                  snapshot,
		"consecutive_interventions": s.consecutiveInterventions,
		"escalated_at":              now.UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode shadow escalation payload: %w", err)
	}

	return s.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   s.cfg.WorkspaceID,
		AgentID:       s.cfg.AgentID,
		UpdateType:    "issue",
		Summary:       fmt.Sprintf("RSP escalation (%s)", intervention.Level),
		PayloadJSON:   string(raw),
		RequiresHuman: true,
	})
}

func interventionSignal(rsp RuntimeRSPState, intervention ShadowIntervention, now time.Time) string {
	parts := []string{
		"RSP",
		"phase=" + string(rsp.RolloutPhase),
		"verdict=" + firstNonEmpty(rsp.Verdict, "unknown"),
		"level=" + strings.TrimSpace(intervention.Level),
		"action=" + strings.TrimSpace(intervention.Action),
		"observed_at=" + now.UTC().Format(time.RFC3339Nano),
	}
	if strings.TrimSpace(rsp.Reason) != "" {
		parts = append(parts, "reason="+strings.TrimSpace(rsp.Reason))
	}
	return strings.Join(parts, " ")
}
