package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"
)

type LatentState string

const (
	StateFocused    LatentState = "FOCUSED"
	StateExploring  LatentState = "EXPLORING"
	StateSaturated  LatentState = "SATURATED"
	StateThrashing  LatentState = "THRASHING"
	StateUngrounded LatentState = "UNGROUNDED"
	StateIdle       LatentState = "IDLE"
	StateRecovering LatentState = "RECOVERING"
)

// HMMObservation represents the observable vector $\psi_i(\tau)$
type HMMObservation struct {
	PatchDelta    float64 // 0.0 to 1.0 magnitude
	VerifierFails int     // count of failed mesh checks
	StaleHits     int     // count of ungrounded memory reads
	Repetitions   bool    // true if acting on same surface iteratively
}

// LatentStateEstimate captures the posterior state probabilities $p(z | \psi)$.
type LatentStateEstimate struct {
	WorkspaceID        string
	AgentID            string
	StateProbabilities map[LatentState]float64
	DominantState      LatentState
	RiskScore          float64
}

// UpdateAgentLatentState computes the recursive posterior HMM probability map
// for the given agent using the provided observation metrics.
func (s *Store) UpdateAgentLatentState(ctx context.Context, workspaceID, agentID string, obs HMMObservation) (LatentStateEstimate, error) {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return LatentStateEstimate{}, err
	}
	defer tx.Rollback()

	// 1. Fetch Prior probabilities pi(z)
	probs := map[LatentState]float64{
		StateFocused:    0.0,
		StateExploring:  0.0,
		StateSaturated:  0.0,
		StateThrashing:  0.0,
		StateUngrounded: 0.0,
		StateIdle:       1.0, // Default prior if missing
		StateRecovering: 0.0,
	}

	var pF, pE, pS, pT, pU, pI, pR float64

	err = tx.QueryRowContext(ctx, `
		SELECT state_focused, state_exploring, state_saturated, state_thrashing, state_ungrounded, state_idle, state_recovering
		FROM rsp_agent_latent_states
		WHERE workspace_id = ? AND agent_id = ?
	`, workspaceID, agentID).Scan(&pF, &pE, &pS, &pT, &pU, &pI, &pR)

	if err == nil {
		probs[StateFocused] = pF
		probs[StateExploring] = pE
		probs[StateSaturated] = pS
		probs[StateThrashing] = pT
		probs[StateUngrounded] = pU
		probs[StateIdle] = pI
		probs[StateRecovering] = pR
	} else if !errors.Is(err, sql.ErrNoRows) {
		return LatentStateEstimate{}, fmt.Errorf("read hmm prior: %w", err)
	}

	// 2. Emission Models (heuristic unnormalized likelihoods p(\psi | z))
	emissions := make(map[LatentState]float64)

	// High delta, zero fails -> Focused/Exploring
	if obs.PatchDelta > 0.0 && obs.VerifierFails == 0 && obs.StaleHits == 0 {
		emissions[StateFocused] = 0.8
		emissions[StateExploring] = 0.6
		emissions[StateRecovering] = 0.4
	}

	// High verifier fails + repetitions -> Thrashing
	if obs.VerifierFails > 0 || obs.Repetitions {
		emissions[StateThrashing] = float64(obs.VerifierFails)*0.5 + 0.5
		emissions[StateSaturated] = 0.5
		emissions[StateFocused] = 0.1
	}

	// Stale hits -> Ungrounded
	if obs.StaleHits > 0 {
		emissions[StateUngrounded] = float64(obs.StaleHits) * 0.8
		emissions[StateThrashing] = 0.4
	}

	// Low delta, no fails -> Idle / Saturated
	if obs.PatchDelta == 0.0 && obs.VerifierFails == 0 {
		emissions[StateIdle] = 0.9
		emissions[StateSaturated] = 0.6
	}

	// Apply minimum emission to avoid 0 probability crashes
	for k := range probs {
		if emissions[k] == 0 {
			emissions[k] = 0.01 // epsilon smoothing
		}
	}

	// 3. Simple posterior approximation (Prior * Emission)
	var sum float64
	for k := range probs {
		// Damping factor to prevent instantaneous lock-in (acts as diagonal transition matrix)
		posteriorRaw := (probs[k]*0.7 + 0.3) * emissions[k]
		probs[k] = posteriorRaw
		sum += posteriorRaw
	}

	// Normalize
	var dominant LatentState
	var maxProb float64
	for k := range probs {
		probs[k] = probs[k] / sum
		if probs[k] > maxProb {
			maxProb = probs[k]
			dominant = k
		}
	}

	// 4. Calculate Risk Score based on pathological states
	riskScore := probs[StateThrashing]*0.8 + probs[StateUngrounded]*0.9 + probs[StateSaturated]*0.4
	if riskScore > 1.0 {
		riskScore = 1.0
	}

	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO rsp_agent_latent_states (
			workspace_id, agent_id,
			state_focused, state_exploring, state_saturated, state_thrashing,
			state_ungrounded, state_idle, state_recovering,
			last_updated, risk_score
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workspace_id, agent_id) DO UPDATE SET
			state_focused = excluded.state_focused,
			state_exploring = excluded.state_exploring,
			state_saturated = excluded.state_saturated,
			state_thrashing = excluded.state_thrashing,
			state_ungrounded = excluded.state_ungrounded,
			state_idle = excluded.state_idle,
			state_recovering = excluded.state_recovering,
			last_updated = excluded.last_updated,
			risk_score = excluded.risk_score
	`, workspaceID, agentID,
		math.Round(probs[StateFocused]*1000)/1000,
		math.Round(probs[StateExploring]*1000)/1000,
		math.Round(probs[StateSaturated]*1000)/1000,
		math.Round(probs[StateThrashing]*1000)/1000,
		math.Round(probs[StateUngrounded]*1000)/1000,
		math.Round(probs[StateIdle]*1000)/1000,
		math.Round(probs[StateRecovering]*1000)/1000,
		now, math.Round(riskScore*1000)/1000)

	if err != nil {
		return LatentStateEstimate{}, fmt.Errorf("upsert hmm state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return LatentStateEstimate{}, err
	}

	return LatentStateEstimate{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		StateProbabilities: probs,
		DominantState:      dominant,
		RiskScore:          riskScore,
	}, nil
}
