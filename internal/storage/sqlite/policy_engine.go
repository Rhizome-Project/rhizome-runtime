package sqlite

import (
	"context"
	"fmt"
	"math"
	"strings"
)

// PolicyEngineParams holds the mathematical constants for the PI-controller and Hysteresis.
type PolicyEngineParams struct {
	Kp                 float64 // Proportional gain for damping Local Variables
	Ki                 float64 // Integral gain (future use)
	HMode              int     // Hysteresis threshold in epochs the signal must persist to switch
	MaxFanoutHardLimit int     // Hard constraint: Maximum fanout allowed (fork bomb prevention)
	MinFanoutHardLimit int     // Hard constraint: Minimum fanout to prevent total freeze
}

var DefaultPolicyParams = PolicyEngineParams{
	Kp:                 0.3,
	Ki:                 0.05,
	HMode:              3,
	MaxFanoutHardLimit: 10,
	MinFanoutHardLimit: 1,
}

// ComputeLocalControlUpdate applies a PI-like damping function to adjust a control parameter θ_c.
func ComputeLocalControlUpdate(currentTheta float64, deviation float64, params PolicyEngineParams) float64 {
	// Simple P-controller: adjust the parameter opposite to the deviation.
	newTheta := currentTheta - (params.Kp * deviation)
	return newTheta
}

// ApplyHysteresisModeSwitch determines if a candidate mode should become the stabilized mode,
// requiring it to persist for H_mode epochs.
func ApplyHysteresisModeSwitch(currentMode, candidateMode string, currentStreak, hMode int) (string, int) {
	if currentMode == candidateMode {
		return currentMode, currentStreak + 1
	}

	if currentStreak+1 >= hMode {
		return candidateMode, 0 // Switch applied, streak resets
	}

	return currentMode, currentStreak + 1
}

type ControlPolicyPreview struct {
	WorkspaceID      string `json:"workspace_id"`
	PolicyMode       string `json:"policy_mode"`
	TargetFanout     int    `json:"target_fanout"`
	Capability       string `json:"capability"`
	Effect           string `json:"effect"`
	Reason           string `json:"reason"`
	LiveApplied      bool   `json:"live_applied"`
	SuppressedReason string `json:"suppressed_reason,omitempty"`
}

// PreviewPolicyActuation computes the current control proposal but intentionally does not
// mutate runtime capability policy. Canonical command-path actuation is deferred to P5B-002.
func (s *Store) PreviewPolicyActuation(ctx context.Context, workspaceID string, targetFanout int) (ControlPolicyPreview, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return ControlPolicyPreview{}, fmt.Errorf("workspace_id is required")
	}

	epochRec, err := s.GetControlEpoch(ctx, workspaceID)
	if err != nil {
		return ControlPolicyPreview{}, fmt.Errorf("read epoch for policy preview: %w", err)
	}

	if targetFanout > DefaultPolicyParams.MaxFanoutHardLimit {
		targetFanout = DefaultPolicyParams.MaxFanoutHardLimit
	}
	if targetFanout < DefaultPolicyParams.MinFanoutHardLimit {
		targetFanout = DefaultPolicyParams.MinFanoutHardLimit
	}

	effect := "ALLOW"
	reason := "Policy Engine: Normal Operations"
	if targetFanout <= 1 {
		effect = "DENY"
		reason = "Policy Engine: Fork Bomb Prevention (High Stress/Deviation)"
	}

	return ControlPolicyPreview{
		WorkspaceID:      workspaceID,
		PolicyMode:       epochRec.PolicyMode,
		TargetFanout:     targetFanout,
		Capability:       "agent.fork",
		Effect:           effect,
		Reason:           reason,
		LiveApplied:      false,
		SuppressedReason: "live control actuation removed pending canonical journal commands",
	}, nil
}

// ActuatePolicies is kept as a compatibility wrapper while live control writes remain removed.
func (s *Store) ActuatePolicies(ctx context.Context, workspaceID string, targetFanout int) error {
	_, err := s.PreviewPolicyActuation(ctx, workspaceID, targetFanout)
	return err
}

// EvaluateClusterPolicy is the main loop function called per epoch to recalculate
// the states mathematically and surface a preview without mutating runtime policy.
func (s *Store) EvaluateClusterPolicy(ctx context.Context, workspaceID string, gaps []CorridorFitMetricGap) (ControlPolicyPreview, error) {
	centDev := 0.0
	for _, gap := range gaps {
		if gap.Metric == "centralization" {
			centDev = gap.Delta
			break
		}
	}

	baselineFanout := 5.0

	// Compute damping
	targetFanoutF := ComputeLocalControlUpdate(baselineFanout, centDev*10, DefaultPolicyParams)
	targetFanout := int(math.Round(targetFanoutF))

	return s.PreviewPolicyActuation(ctx, workspaceID, targetFanout)
}
