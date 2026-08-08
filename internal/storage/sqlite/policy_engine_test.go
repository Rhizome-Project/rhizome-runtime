package sqlite

import (
	"context"
	"math"
	"testing"
)

func TestPolicyEngineMaths(t *testing.T) {
	// Test ComputeLocalControlUpdate
	params := DefaultPolicyParams

	// Baseline fanout 5. Centralization deviation +0.2 (too high)
	deviation := 0.2 * 10
	newFanoutF := ComputeLocalControlUpdate(5.0, deviation, params)
	expectedF := 5.0 - (0.3 * 2.0) // 5.0 - 0.6 = 4.4

	if math.Abs(newFanoutF-expectedF) > 0.0001 {
		t.Errorf("expected 4.4, got %f", newFanoutF)
	}

	// Test Hysteresis Switch
	mode, streak := ApplyHysteresisModeSwitch("active", "shadow", 0, 3)
	if mode != "active" || streak != 1 {
		t.Errorf("expected active/1, got %s/%d", mode, streak)
	}

	mode, streak = ApplyHysteresisModeSwitch("active", "shadow", 2, 3)
	if mode != "shadow" || streak != 0 {
		t.Errorf("expected shadow/0, got %s/%d", mode, streak)
	}
}

func TestPolicyEngineActuation(t *testing.T) {
	t.Run("observe only by default", func(t *testing.T) {
		store := NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-policy-observe-only"
		setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "test-agent")

		if err := store.SetPolicyMode(ctx, workspaceID, "active"); err != nil {
			t.Fatalf("set active mode: %v", err)
		}
		if err := store.ActuatePolicies(ctx, workspaceID, 1); err != nil {
			t.Fatalf("actuate observe-only failed: %v", err)
		}

		res, _ := store.CheckCapabilityPolicy(ctx, CapabilityCheckInput{
			WorkspaceID: workspaceID,
			SubjectType: "agent",
			SubjectID:   "test-agent",
			Capability:  "agent.fork",
		})
		if res.Verdict == "DENY" {
			t.Fatalf("expected observe-only mode to skip live DENY actuation, got %s", res.Verdict)
		}
	})

	t.Run("preview remains side-effect free even when policy mode is active", func(t *testing.T) {
		store := NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-policy-live"
		setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "test-agent")

		if err := store.SetPolicyMode(ctx, workspaceID, "active"); err != nil {
			t.Fatalf("set active mode: %v", err)
		}

		preview, err := store.PreviewPolicyActuation(ctx, workspaceID, 1)
		if err != nil {
			t.Fatalf("preview policy actuation: %v", err)
		}
		if preview.LiveApplied {
			t.Fatalf("expected preview-only path to avoid live application, got %+v", preview)
		}
		if preview.Effect != "DENY" || preview.Capability != "agent.fork" {
			t.Fatalf("expected deny preview for agent.fork, got %+v", preview)
		}
		if preview.SuppressedReason == "" {
			t.Fatalf("expected preview to surface suppression reason, got %+v", preview)
		}

		res, _ := store.CheckCapabilityPolicy(ctx, CapabilityCheckInput{
			WorkspaceID: workspaceID,
			SubjectType: "agent",
			SubjectID:   "test-agent",
			Capability:  "agent.fork",
		})
		if res.Verdict != "ALLOW" {
			t.Fatalf("expected preview-only path to avoid live DENY actuation, got %s", res.Verdict)
		}
	})
}
