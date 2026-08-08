package sqlite

import (
	"context"
	"testing"
)

func TestUpdateAgentLatentState_ThrashingConvergence(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	workspaceID := "ws-hmm-1"
	agentID := "agent-x"

	// Initial observation: Normal patching -> FOCUSED
	est, err := store.UpdateAgentLatentState(ctx, workspaceID, agentID, HMMObservation{
		PatchDelta:    0.8,
		VerifierFails: 0,
		StaleHits:     0,
		Repetitions:   false,
	})
	if err != nil {
		t.Fatalf("first update failed: %v", err)
	}

	if est.DominantState != StateFocused && est.DominantState != StateExploring {
		t.Errorf("expected FOCUSED or EXPLORING, got %v", est.DominantState)
	}
	if est.RiskScore > 0.3 {
		t.Errorf("risk score too high for normal behavior: %f", est.RiskScore)
	}

	// 2. Introduce repetitive verifier failure stream -> Should shift to THRASHING
	for i := 0; i < 3; i++ {
		est, err = store.UpdateAgentLatentState(ctx, workspaceID, agentID, HMMObservation{
			PatchDelta:    0.1,
			VerifierFails: 2,
			StaleHits:     0,
			Repetitions:   true,
		})
		if err != nil {
			t.Fatalf("update cycle %d failed: %v", i, err)
		}
	}

	if est.DominantState != StateThrashing {
		t.Errorf("HMM failed to converge on THRASHING, stuck at %v", est.DominantState)
	}
	if est.RiskScore < 0.6 {
		t.Errorf("Risk score should be acutely elevated, got %f", est.RiskScore)
	}
}

func TestUpdateAgentLatentState_UngroundedSpike(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	est, err := store.UpdateAgentLatentState(ctx, "ws-hmm-2", "agent-y", HMMObservation{
		PatchDelta:    0.5,
		VerifierFails: 0,
		StaleHits:     5, // Massive stale hit anomaly
		Repetitions:   false,
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if est.DominantState != StateUngrounded {
		t.Errorf("HMM failed to detect UNGROUNDED state, got %v", est.DominantState)
	}
}
